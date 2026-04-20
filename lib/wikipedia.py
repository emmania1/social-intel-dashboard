"""Wikipedia pageviews fetcher.

Daily pageview counts for a company's Wikipedia article via Wikimedia's
public REST API. Works for every public company that has a Wikipedia page
(nearly all do). Free, no auth, goes back to 2015-07.

Resolution flow:
  1. Try article title = company name verbatim
  2. If 404, use opensearch API to find the closest article
  3. Fetch daily pageviews between start and end, user-access only
"""
from __future__ import annotations

import time
from datetime import datetime

import pandas as pd
import requests

SEARCH_URL = "https://en.wikipedia.org/w/api.php"
PAGEVIEWS_URL = (
    "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
    "en.wikipedia/all-access/user/{article}/daily/{start}/{end}"
)
UA = "social-intel-dashboard/1.0 (research tool; contact via github)"


def _yyyymmdd(d: str) -> str:
    # Wikimedia wants YYYYMMDD with optional HH (zero-pad)
    return datetime.strptime(d, "%Y-%m-%d").strftime("%Y%m%d")


def _search_title(query: str) -> str | None:
    """Use OpenSearch to find the best matching article title."""
    try:
        r = requests.get(
            SEARCH_URL,
            params={
                "action": "opensearch",
                "search": query,
                "limit": 1,
                "namespace": 0,
                "format": "json",
            },
            headers={"User-Agent": UA},
            timeout=15,
        )
        r.raise_for_status()
        results = r.json()
        titles = results[1] if len(results) > 1 else []
        return titles[0] if titles else None
    except Exception as exc:  # noqa: BLE001
        print(f"[wikipedia] search {query!r}: {exc}")
        return None


def _fetch_pageviews(article: str, start: str, end: str) -> pd.DataFrame:
    url = PAGEVIEWS_URL.format(
        article=article.replace(" ", "_"),
        start=_yyyymmdd(start),
        end=_yyyymmdd(end),
    )
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=30)
        if r.status_code == 404:
            return pd.DataFrame(columns=["date", "views"])
        r.raise_for_status()
    except requests.RequestException as exc:
        print(f"[wikipedia] pageviews {article!r}: {exc}")
        return pd.DataFrame(columns=["date", "views"])

    items = r.json().get("items", [])
    if not items:
        return pd.DataFrame(columns=["date", "views"])
    rows = []
    for item in items:
        ts = item.get("timestamp", "")[:8]  # YYYYMMDD
        try:
            d = datetime.strptime(ts, "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            continue
        rows.append({"date": d, "views": int(item.get("views", 0))})
    return pd.DataFrame(rows)


def fetch_wikipedia_daily(company: str, start: str, end: str) -> tuple[pd.DataFrame, str | None]:
    """Return (daily_views_df, resolved_article_title).

    Tries verbatim title first; falls back to OpenSearch.
    """
    company = (company or "").strip()
    if not company:
        return pd.DataFrame(columns=["date", "views"]), None

    # Try verbatim first
    df = _fetch_pageviews(company, start, end)
    if not df.empty:
        return df, company

    # Fallback: search for the best article title
    title = _search_title(company)
    time.sleep(0.3)
    if not title:
        return pd.DataFrame(columns=["date", "views"]), None
    df = _fetch_pageviews(title, start, end)
    return df, title
