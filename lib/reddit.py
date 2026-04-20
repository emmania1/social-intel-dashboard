"""Reddit post-count fetcher.

Strategy: use pushshift-like Arctic Shift mirror for historical coverage; fall
back to reddit.com/.json for recent data. Arctic Shift exposes a public API at
https://arctic-shift.photon-reddit.com/api/ that supports date filtering.

Output: weekly post-count time series across a set of subreddits mentioning
the company name / search term.
"""
from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Iterable

import pandas as pd
import requests

ARCTIC_BASE = "https://arctic-shift.photon-reddit.com/api/posts/search"
USER_AGENT = os.environ.get(
    "REDDIT_USER_AGENT", "social-intel-dashboard/1.0 (+local)"
)


def _iso_to_epoch(d: str) -> int:
    return int(datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())


DEFAULT_SUBREDDITS = (
    "stocks",
    "investing",
    "wallstreetbets",
    "pennystocks",
    "smallstreetbets",
    "SPACs",
    "shortsqueeze",
)

# Minimum subscribers for an auto-detected company sub to count — weeds out
# typo-squatted dead subreddits while keeping niche real ones (r/crocs has
# thousands of subs, whereas a squatted r/someticker might have zero).
_MIN_SUBSCRIBERS = 500


def _slug_candidates(ticker: str, company: str) -> list[str]:
    """Generate plausible subreddit names from a ticker and company name."""
    import re as _re

    out: list[str] = []
    t = (ticker or "").strip().lower()
    c = (company or "").strip().lower()
    if t:
        out.append(t)
        out.append(f"{t}stock")
    if c:
        # Strip non-alnum for Reddit's URL charset; try whole name first.
        slug = _re.sub(r"[^a-z0-9]", "", c)
        if slug and slug != t:
            out.append(slug)
        # First word only (e.g. "Chili's" -> "chilis", "Brinker International" -> "brinker")
        first = _re.sub(r"[^a-z0-9]", "", c.split()[0]) if c.split() else ""
        if first and first not in out:
            out.append(first)
    # dedupe keeping order
    seen: set[str] = set()
    return [s for s in out if not (s in seen or seen.add(s))]


def discover_company_subreddits(ticker: str, company: str) -> list[dict]:
    """Probe Reddit for plausible company-specific subreddits.

    Returns a list of {"name": str, "subscribers": int} for each candidate
    that exists AND has at least `_MIN_SUBSCRIBERS` subscribers (filters out
    typo-squatted dead subs). Capped at 3 results to bound request volume.
    """
    results: list[dict] = []
    for cand in _slug_candidates(ticker, company):
        try:
            r = requests.get(
                f"https://www.reddit.com/r/{cand}/about.json",
                headers={"User-Agent": USER_AGENT},
                timeout=8,
            )
        except requests.RequestException:
            continue
        if r.status_code != 200:
            continue
        try:
            data = r.json().get("data", {})
        except ValueError:
            continue
        if not isinstance(data, dict) or data.get("subreddit_type") == "private":
            continue
        subs = int(data.get("subscribers") or 0)
        if subs < _MIN_SUBSCRIBERS:
            continue
        # Reddit returns the canonical-case name in data["display_name"]
        name = data.get("display_name") or cand
        results.append({"name": name, "subscribers": subs})
        if len(results) >= 3:
            break
        time.sleep(0.2)
    return results


def _fetch_one(sub: str, query: str, start_epoch: int, end_epoch: int) -> list[dict]:
    rows: list[dict] = []
    after = start_epoch
    page = 0
    while after < end_epoch and page < 40:
        params = {
            "subreddit": sub,
            "title": query,
            "limit": 100,
            "after": after,
            "sort": "asc",
        }
        try:
            r = requests.get(
                ARCTIC_BASE, params=params, headers={"User-Agent": USER_AGENT}, timeout=30
            )
            r.raise_for_status()
        except requests.RequestException as exc:
            print(f"[reddit] {sub!r} q={query!r} page {page}: {exc}")
            break
        data = r.json().get("data", [])
        if not data:
            break
        newest_ts = after
        for row in data:
            ts = int(row.get("created_utc") or row.get("created", 0))
            if ts >= end_epoch:
                continue
            rows.append(
                {
                    "post_id": row.get("id") or f"{sub}_{ts}",
                    "created_utc": ts,
                    "subreddit": row.get("subreddit", sub),
                }
            )
            newest_ts = max(newest_ts, ts)
        if len(data) < 100 or newest_ts <= after:
            break
        after = newest_ts + 1
        page += 1
        time.sleep(0.25)
    return rows


def fetch_reddit_weekly(
    queries: str | Iterable[str],
    start: str,
    end: str,
    subreddits: Iterable[str] = DEFAULT_SUBREDDITS,
    company_subs: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Return weekly post counts matching ANY of the queries across subreddits.

    Accepts either a single string or an iterable of query strings. Results are
    unioned and deduped by (subreddit, post_id). Returns columns: date, count.
    """
    if isinstance(queries, str):
        query_list = [queries]
    else:
        query_list = list(queries)
    # Dedupe, drop empties, cap length to keep request volume reasonable
    seen: set[str] = set()
    clean_queries: list[str] = []
    for q in query_list:
        q = (q or "").strip()
        if not q or q.lower() in seen:
            continue
        seen.add(q.lower())
        clean_queries.append(q)
    if not clean_queries:
        return pd.DataFrame(columns=["date", "count"])

    subs = list(subreddits)
    lowered = {s.lower() for s in subs}
    for extra in (company_subs or []):
        if extra and extra.lower() not in lowered:
            subs.append(extra)
            lowered.add(extra.lower())

    start_epoch = _iso_to_epoch(start)
    end_epoch = _iso_to_epoch(end)

    all_rows: list[dict] = []
    for sub in subs:
        for q in clean_queries:
            all_rows.extend(_fetch_one(sub, q, start_epoch, end_epoch))

    if not all_rows:
        return pd.DataFrame(columns=["date", "count"])

    df = pd.DataFrame(all_rows).drop_duplicates(subset=["subreddit", "post_id"])
    df["dt"] = pd.to_datetime(df["created_utc"], unit="s", utc=True)
    df["week"] = df["dt"].dt.to_period("W-SUN").dt.end_time.dt.strftime("%Y-%m-%d")
    weekly = df.groupby("week").size().reset_index(name="count")
    weekly.columns = ["date", "count"]
    return weekly.sort_values("date").reset_index(drop=True)
