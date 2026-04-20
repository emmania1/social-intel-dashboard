"""Stock price fetcher using yfinance."""
from __future__ import annotations

import pandas as pd
import yfinance as yf


def fetch_stock(ticker: str, start: str, end: str) -> pd.DataFrame:
    """Return daily OHLCV for ticker between start and end (YYYY-MM-DD).

    Output columns: date (str), close (float).
    """
    t = yf.Ticker(ticker)
    df = t.history(start=start, end=end, auto_adjust=True, actions=False)
    if df.empty:
        return pd.DataFrame(columns=["date", "close"])
    df = df.reset_index()[["Date", "Close"]].copy()
    df.columns = ["date", "close"]
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df["close"] = df["close"].astype(float).round(4)
    return df


def resolve_ticker(ticker: str) -> dict:
    """Look up a human-readable name and default search term from yfinance.

    Returns {"name": str, "short": str, "ok": bool}. Falls back to the ticker
    itself if yfinance has no metadata.
    """
    try:
        t = yf.Ticker(ticker)
        info = t.info or {}
    except Exception as exc:  # noqa: BLE001
        print(f"[resolve] {ticker}: {exc}")
        info = {}
    long_name = info.get("longName") or info.get("shortName") or ""
    short_name = info.get("shortName") or long_name or ticker.upper()
    # Strip common corporate suffixes so the Google/Reddit/YouTube search
    # uses the brand, not the legal entity (e.g. "Brinker International Inc"
    # -> "Brinker International").
    clean = long_name
    for suffix in (
        ", Inc.", " Inc.", " Inc", " Corporation", " Corp.", " Corp",
        " Company", " Co.", " Ltd.", " Ltd", " plc", " PLC", " S.A.",
        " N.V.", " A.G.", " AG", ", LLC", " LLC",
    ):
        if clean.endswith(suffix):
            clean = clean[: -len(suffix)].rstrip(",").strip()
            break
    return {
        "name": clean or ticker.upper(),
        "short": short_name,
        "ok": bool(long_name),
    }
