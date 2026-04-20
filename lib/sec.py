"""SEC EDGAR filing-density fetcher.

Counts filings (8-K, 10-Q, 10-K, S-1, Form 4, etc.) per week for a ticker.
High filing density = more corporate events/activity = more news.

Flow:
  1. Look up the 10-digit CIK from company_tickers.json
  2. Fetch all recent filings from submissions/CIK{cik}.json
  3. Bucket by week
"""
from __future__ import annotations

from datetime import datetime

import pandas as pd
import requests

TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
UA = "social-intel-dashboard research-tool contact@example.com"  # SEC requires UA

_TICKER_TO_CIK: dict[str, str] | None = None


def _load_ticker_map() -> dict[str, str]:
    """Fetch-and-cache the ticker→CIK map once per process."""
    global _TICKER_TO_CIK
    if _TICKER_TO_CIK is not None:
        return _TICKER_TO_CIK
    try:
        r = requests.get(TICKERS_URL, headers={"User-Agent": UA}, timeout=30)
        r.raise_for_status()
        data = r.json()
    except requests.RequestException as exc:
        print(f"[sec] ticker map fetch failed: {exc}")
        _TICKER_TO_CIK = {}
        return _TICKER_TO_CIK
    # The map is a dict keyed by index. Each entry has cik_str + ticker.
    _TICKER_TO_CIK = {
        entry["ticker"].upper(): str(entry["cik_str"]).zfill(10)
        for entry in data.values()
    }
    return _TICKER_TO_CIK


def fetch_sec_filings_weekly(ticker: str, start: str, end: str) -> pd.DataFrame:
    """Return weekly filing counts for a ticker.

    Columns: date (week-ending YYYY-MM-DD), count (int), forms (list of strs).
    """
    ticker = (ticker or "").strip().upper()
    if not ticker:
        return pd.DataFrame(columns=["date", "count"])

    cik = _load_ticker_map().get(ticker)
    if not cik:
        print(f"[sec] no CIK for {ticker}")
        return pd.DataFrame(columns=["date", "count"])

    try:
        r = requests.get(
            SUBMISSIONS_URL.format(cik=cik),
            headers={"User-Agent": UA},
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
    except requests.RequestException as exc:
        print(f"[sec] submissions fetch {ticker}: {exc}")
        return pd.DataFrame(columns=["date", "count"])

    recent = (data.get("filings", {}) or {}).get("recent", {}) or {}
    dates = recent.get("filingDate", [])
    forms = recent.get("form", [])
    if not dates:
        return pd.DataFrame(columns=["date", "count"])

    start_dt = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")
    rows = []
    for d, f in zip(dates, forms):
        try:
            dt = datetime.strptime(d, "%Y-%m-%d")
        except ValueError:
            continue
        if dt < start_dt or dt > end_dt:
            continue
        rows.append({"date": d, "form": f})
    if not rows:
        return pd.DataFrame(columns=["date", "count"])

    df = pd.DataFrame(rows)
    df["dt"] = pd.to_datetime(df["date"])
    df["week"] = df["dt"].dt.to_period("W-SUN").dt.end_time.dt.strftime("%Y-%m-%d")
    weekly = (
        df.groupby("week")
        .agg(count=("form", "size"), forms=("form", lambda s: ",".join(sorted(set(s)))))
        .reset_index()
    )
    weekly.columns = ["date", "count", "forms"]
    return weekly.sort_values("date").reset_index(drop=True)
