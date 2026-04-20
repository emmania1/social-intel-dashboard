"""StockTwits fetcher — messages per day for `$TICKER`.

StockTwits' unauthenticated stream endpoint returns the most recent ~30
messages per page. We paginate backwards via the `max=<message_id>` cursor
until we cross the start date or hit a hard request cap (~30 pages = ~900
messages). For quiet tickers this reaches back months; for busy ones it only
covers the last few days — the result is ALWAYS recent-weighted, not a full
historical series. Treat this as a complementary signal to Reddit/YouTube.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone

import pandas as pd
import requests

STREAM_URL = "https://api.stocktwits.com/api/2/streams/symbol/{symbol}.json"
UA = "social-intel-dashboard/1.0"
MAX_PAGES = 30


def fetch_stocktwits_daily(ticker: str, start: str, end: str) -> pd.DataFrame:
    """Daily message counts for a ticker. Columns: date, count."""
    ticker = ticker.strip().upper()
    if not ticker:
        return pd.DataFrame(columns=["date", "count"])
    start_dt = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt = datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    rows: list[dict] = []
    cursor: int | None = None
    for page in range(MAX_PAGES):
        params = {"limit": 30}
        if cursor is not None:
            params["max"] = cursor
        try:
            r = requests.get(
                STREAM_URL.format(symbol=ticker),
                params=params,
                headers={"User-Agent": UA},
                timeout=20,
            )
        except requests.RequestException as exc:
            print(f"[stocktwits] {ticker}: {exc}")
            break
        if r.status_code == 404:
            print(f"[stocktwits] {ticker}: symbol not found")
            break
        if r.status_code == 429:
            print(f"[stocktwits] rate limited, pausing 30s")
            time.sleep(30)
            continue
        if not r.ok:
            print(f"[stocktwits] {ticker}: HTTP {r.status_code}")
            break
        data = r.json()
        messages = data.get("messages", [])
        if not messages:
            break
        oldest_seen: int | None = None
        for m in messages:
            mid = int(m["id"])
            created = m.get("created_at")
            try:
                ts = datetime.strptime(created, "%Y-%m-%dT%H:%M:%SZ").replace(
                    tzinfo=timezone.utc
                )
            except (TypeError, ValueError):
                continue
            rows.append({"id": mid, "ts": ts})
            oldest_seen = min(oldest_seen, mid) if oldest_seen else mid
        # stop once the oldest message in this page is before our start window
        if oldest_seen is None:
            break
        oldest_ts = min(r["ts"] for r in rows[-len(messages):])
        if oldest_ts < start_dt:
            break
        cursor = oldest_seen - 1
        time.sleep(0.4)

    if not rows:
        return pd.DataFrame(columns=["date", "count"])

    df = pd.DataFrame(rows).drop_duplicates(subset=["id"])
    df = df[(df["ts"] >= start_dt) & (df["ts"] <= end_dt)]
    if df.empty:
        return pd.DataFrame(columns=["date", "count"])
    df["date"] = df["ts"].dt.strftime("%Y-%m-%d")
    daily = df.groupby("date").size().reset_index(name="count")
    return daily.sort_values("date").reset_index(drop=True)
