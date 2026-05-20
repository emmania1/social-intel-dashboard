"""Stock price fetcher using yfinance.

Yahoo Finance blocks many cloud/datacenter IPs when yfinance makes requests
with its default session. We use `curl_cffi` to impersonate a real Chrome
browser, which reliably works from Render/Fly/etc.
"""
from __future__ import annotations

import pandas as pd
import yfinance as yf

try:
    from curl_cffi import requests as curl_requests

    _SESSION = curl_requests.Session(impersonate="chrome")
except Exception:  # noqa: BLE001
    _SESSION = None


def _ticker(symbol: str) -> yf.Ticker:
    if _SESSION is not None:
        return yf.Ticker(symbol, session=_SESSION)
    return yf.Ticker(symbol)


def get_info(ticker: str) -> dict:
    """Return a yfinance-shaped info dict for `ticker`. Tries yfinance first
    (free, full data when it works). If yfinance returns an empty dict — which
    happens whenever Yahoo blocks the IP (common from Render's datacenter) —
    falls back to Financial Modeling Prep (configured via FMP_API_KEY).

    Returns an empty dict if both sources fail, so callers can still degrade.
    """
    # Try yfinance first. Require BOTH an identifier AND a real numeric field
    # before accepting — Yahoo sometimes returns just the company name with
    # every price/marketCap field as None when it half-blocks us. In that case
    # we want to fall through to FMP, not return a useless partial dict.
    try:
        info = _ticker(ticker).info or {}
        has_name = info.get("longName") or info.get("shortName")
        has_numbers = info.get("currentPrice") or info.get("marketCap") or info.get("previousClose")
        if has_name and has_numbers:
            return info
        if info:
            print(f"[stock] yfinance returned partial info for {ticker} (name={bool(has_name)}, numbers={bool(has_numbers)}) — falling through to FMP")
    except Exception as exc:  # noqa: BLE001
        print(f"[stock] yfinance failed for {ticker}: {exc}")

    # Fall back to FMP
    try:
        from lib import fmp
        if fmp.is_available():
            fmp_info = fmp.get_info(ticker)
            if fmp_info.get("longName") or fmp_info.get("currentPrice"):
                print(f"[stock] using FMP fallback for {ticker}")
                return fmp_info
    except Exception as exc:  # noqa: BLE001
        print(f"[stock] FMP fallback failed for {ticker}: {exc}")

    return {}


def fetch_stock(ticker: str, start: str, end: str) -> pd.DataFrame:
    """Return daily OHLCV for ticker between start and end (YYYY-MM-DD).

    Output columns: date (str), close (float). Tries yfinance first; falls
    back to FMP if yfinance returns an empty frame (Yahoo IP block).
    """
    # Try yfinance
    try:
        t = _ticker(ticker)
        df = t.history(start=start, end=end, auto_adjust=True, actions=False)
        if not df.empty:
            df = df.reset_index()[["Date", "Close"]].copy()
            df.columns = ["date", "close"]
            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
            df["close"] = df["close"].astype(float).round(4)
            return df
    except Exception as exc:  # noqa: BLE001
        print(f"[stock] yfinance history failed for {ticker}: {exc}")

    # Fall back to FMP
    try:
        from lib import fmp
        if fmp.is_available():
            print(f"[stock] using FMP fallback for {ticker} history")
            return fmp.get_history(ticker, start, end)
    except Exception as exc:  # noqa: BLE001
        print(f"[stock] FMP history fallback failed for {ticker}: {exc}")

    return pd.DataFrame(columns=["date", "close"])


def resolve_ticker(ticker: str) -> dict:
    """Look up a human-readable name and default search term.

    Returns {"name": str, "short": str, "ok": bool}. Uses the unified
    get_info() so it benefits from the FMP fallback automatically.
    """
    info = get_info(ticker)
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
