"""YouTube Data API v3 fetcher.

For an arbitrary company name, there's no clean historical "videos per week"
firehose. Strategy:

1. search.list for the query, collect top ~200 most-recent results in the date
   range (publishedAfter / publishedBefore).
2. videos.list on those IDs to get viewCount.
3. Bucket by publish week, sum views and count videos.

Note: the search endpoint is relevance-ordered by default and capped at 500
results per query. For long timeframes we paginate monthly to reduce bias.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Iterable

import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

YT_KEY = os.environ.get("YOUTUBE_API_KEY", "")


def _client():
    if not YT_KEY:
        raise RuntimeError("YOUTUBE_API_KEY not set")
    return build("youtube", "v3", developerKey=YT_KEY, cache_discovery=False)


def _iso_month_windows(start: str, end: str) -> Iterable[tuple[str, str]]:
    s = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    e = datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    cur = s
    while cur < e:
        nxt = (cur + timedelta(days=32)).replace(day=1)
        yield cur.strftime("%Y-%m-%dT%H:%M:%SZ"), min(nxt, e).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        cur = nxt


def fetch_youtube_weekly(
    queries: str | list[str],
    start: str,
    end: str,
    max_per_month: int = 50,
) -> pd.DataFrame:
    """Weekly aggregate: videos published + total views for videos matching ANY query.

    `queries` can be a string or a list of strings. Results are unioned by
    video ID. Returns columns: date, videos, views.
    """
    if isinstance(queries, str):
        query_list = [queries]
    else:
        query_list = list(queries)
    # dedupe case-insensitively
    seen: set[str] = set()
    clean_queries: list[str] = []
    for q in query_list:
        q = (q or "").strip()
        if not q or q.lower() in seen:
            continue
        seen.add(q.lower())
        clean_queries.append(q)
    if not clean_queries:
        return pd.DataFrame(columns=["date", "videos", "views"])

    if not YT_KEY:
        print("[youtube] YOUTUBE_API_KEY not set; skipping")
        return pd.DataFrame(columns=["date", "videos", "views"])

    yt = _client()
    published_map: dict[str, str] = {}  # id -> publishedAt iso

    # Split quota across queries: if we'd normally pull 50/month per term,
    # split evenly so total quota stays roughly the same as single-term.
    per_query_month_cap = max(10, max_per_month // len(clean_queries))

    for query in clean_queries:
        for win_start, win_end in _iso_month_windows(start, end):
            page_token = None
            collected_this_window = 0
            while collected_this_window < per_query_month_cap:
                try:
                    resp = (
                        yt.search()
                        .list(
                            part="id,snippet",
                            q=query,
                            type="video",
                            order="viewCount",
                            publishedAfter=win_start,
                            publishedBefore=win_end,
                            maxResults=min(
                                50, per_query_month_cap - collected_this_window
                            ),
                            pageToken=page_token,
                        )
                        .execute()
                    )
                except HttpError as exc:
                    print(f"[youtube] search error for {query!r}: {exc}")
                    break

                for item in resp.get("items", []):
                    vid = item["id"].get("videoId")
                    if not vid:
                        continue
                    # first-seen wins (stable publish date across terms)
                    if vid not in published_map:
                        published_map[vid] = item["snippet"]["publishedAt"]
                collected_this_window += len(resp.get("items", []))
                page_token = resp.get("nextPageToken")
                if not page_token:
                    break

    video_ids = list(published_map.keys())

    if not video_ids:
        return pd.DataFrame(columns=["date", "videos", "views"])

    # videos.list in batches of 50 for viewCount
    view_counts: dict[str, int] = {}
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i : i + 50]
        try:
            resp = (
                yt.videos()
                .list(part="statistics", id=",".join(batch), maxResults=50)
                .execute()
            )
        except HttpError as exc:
            print(f"[youtube] videos.list error: {exc}")
            continue
        for item in resp.get("items", []):
            stats = item.get("statistics", {})
            view_counts[item["id"]] = int(stats.get("viewCount", 0))

    rows = []
    for vid, pub in published_map.items():
        rows.append(
            {
                "published": pub,
                "views": view_counts.get(vid, 0),
            }
        )
    df = pd.DataFrame(rows)
    df["dt"] = pd.to_datetime(df["published"], utc=True)
    df["week"] = df["dt"].dt.to_period("W-SUN").dt.end_time.dt.strftime("%Y-%m-%d")
    weekly = (
        df.groupby("week")
        .agg(videos=("views", "size"), views=("views", "sum"))
        .reset_index()
    )
    weekly.columns = ["date", "videos", "views"]
    return weekly.sort_values("date").reset_index(drop=True)
