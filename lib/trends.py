"""Google Trends fetcher using pytrends."""
from __future__ import annotations

import time

import pandas as pd
from pytrends.request import TrendReq


def fetch_trends(term: str, start: str, end: str, geo: str = "US") -> pd.DataFrame:
    """Weekly Google Trends interest for a term.

    Returns DataFrame with columns: date (weekly, ISO string), value (0-100).
    """
    pytrends = TrendReq(hl="en-US", tz=360, retries=3, backoff_factor=0.5)
    timeframe = f"{start} {end}"
    try:
        pytrends.build_payload([term], timeframe=timeframe, geo=geo)
        df = pytrends.interest_over_time()
    except Exception as exc:  # pragma: no cover - network/pytrends errors
        print(f"[trends] failed for {term!r}: {exc}")
        return pd.DataFrame(columns=["date", "value"])
    if df.empty:
        return pd.DataFrame(columns=["date", "value"])
    if "isPartial" in df.columns:
        df = df[~df["isPartial"]]
    df = df.reset_index()[["date", term]]
    df.columns = ["date", "value"]
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df["value"] = df["value"].astype(float)
    # polite spacing between calls when fetcher is reused
    time.sleep(0.5)
    return df
