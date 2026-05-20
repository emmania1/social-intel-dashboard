"""Financial Modeling Prep adapter — used as a fallback when yfinance gets
blocked by Yahoo (which happens routinely from Render's datacenter IPs).

Returns data shaped like yfinance's `Ticker.info` dict so the rest of the
codebase doesn't need to know which source it came from.
"""
from __future__ import annotations

import os
from typing import Any

import pandas as pd
import requests

API_KEY = os.environ.get("FMP_API_KEY", "")
BASE = "https://financialmodelingprep.com/stable"


def is_available() -> bool:
    return bool(API_KEY)


def _get(path: str, params: dict | None = None) -> Any:
    """GET an FMP endpoint and return parsed JSON. Returns None on any
    failure — callers degrade gracefully rather than raising."""
    if not API_KEY:
        return None
    p = dict(params or {})
    p["apikey"] = API_KEY
    try:
        r = requests.get(f"{BASE}/{path}", params=p, timeout=30)
        if not r.ok:
            print(f"[fmp] {path}: HTTP {r.status_code}: {r.text[:120]}")
            return None
        return r.json()
    except requests.RequestException as exc:
        print(f"[fmp] {path}: {exc}")
        return None


def _first(arr):
    """FMP returns single-row results as a 1-element array."""
    if isinstance(arr, list) and arr:
        return arr[0] or {}
    if isinstance(arr, dict):
        return arr
    return {}


def get_info(symbol: str) -> dict:
    """Return a yfinance-shaped info dict for `symbol`, sourced from FMP.

    Combines /quote (price/volume), /profile (company info), and
    /key-metrics-ttm (ratios) into the same shape yfinance.info returns.
    Missing fields come back as None — same convention as yfinance.
    """
    if not API_KEY:
        return {}

    quote = _first(_get("quote", {"symbol": symbol}))
    profile = _first(_get("profile", {"symbol": symbol}))
    metrics = _first(_get("key-metrics-ttm", {"symbol": symbol}))

    if not quote and not profile:
        return {}

    # FMP marketCap on quote can be 0 for some tickers; fall back to profile.
    mkt_cap = quote.get("marketCap") or profile.get("mktCap")
    long_name = profile.get("companyName") or quote.get("name")
    short_name = profile.get("symbol") or long_name

    return {
        # Identifying fields
        "longName": long_name,
        "shortName": short_name,
        "sector": profile.get("sector"),
        "industry": profile.get("industry"),
        "exchange": profile.get("exchangeShortName") or profile.get("exchange") or quote.get("exchange"),
        "description": profile.get("description"),
        # Price
        "currentPrice": quote.get("price"),
        "previousClose": quote.get("previousClose"),
        "open": quote.get("open"),
        "dayLow": quote.get("dayLow"),
        "dayHigh": quote.get("dayHigh"),
        "fiftyTwoWeekLow": quote.get("yearLow"),
        "fiftyTwoWeekHigh": quote.get("yearHigh"),
        # Size
        "marketCap": mkt_cap,
        # Valuation
        "trailingPE": quote.get("pe") or metrics.get("peRatioTTM"),
        "forwardPE": metrics.get("forwardPE"),
        "priceToBook": metrics.get("pbRatioTTM"),
        "enterpriseToEbitda": metrics.get("evToEBITDA"),
        "enterpriseToRevenue": metrics.get("evToSalesTTM"),
        # Profitability (FMP returns these as decimals already)
        "grossMargins": metrics.get("grossProfitMarginTTM"),
        "operatingMargins": metrics.get("operatingProfitMarginTTM"),
        "netMargins": metrics.get("netProfitMarginTTM"),
        # EPS / cash
        "eps": quote.get("eps") or metrics.get("netIncomePerShareTTM"),
        "trailingEps": quote.get("eps") or metrics.get("netIncomePerShareTTM"),
        "freeCashflow": metrics.get("freeCashFlowPerShareTTM"),  # per-share; rough proxy
        # Revenue: FMP exposes revenuePerShareTTM; multiply by share count if available.
        "totalRevenue": (
            metrics.get("revenuePerShareTTM") * (mkt_cap / quote.get("price"))
            if metrics.get("revenuePerShareTTM") and mkt_cap and quote.get("price")
            else None
        ),
        "revenueGrowth": None,  # not directly available; would need income-statement YoY
    }


def get_history(symbol: str, start: str, end: str) -> pd.DataFrame:
    """Return daily close-price history with columns ['date', 'close']."""
    if not API_KEY:
        return pd.DataFrame(columns=["date", "close"])
    data = _get("historical-price-eod/light", {"symbol": symbol, "from": start, "to": end})
    if not isinstance(data, list) or not data:
        return pd.DataFrame(columns=["date", "close"])
    df = pd.DataFrame(data)
    if "date" not in df.columns or ("price" not in df.columns and "close" not in df.columns):
        return pd.DataFrame(columns=["date", "close"])
    close_col = "price" if "price" in df.columns else "close"
    df = df[["date", close_col]].copy()
    df.columns = ["date", "close"]
    df["close"] = pd.to_numeric(df["close"], errors="coerce").round(4)
    df = df.dropna(subset=["close"]).sort_values("date").reset_index(drop=True)
    return df


def get_earnings_surprises(symbol: str, limit: int = 8) -> list[dict]:
    """Return the last `limit` quarters of earnings surprises in the shape
    /api/full's frontend expects: date / estimate / actual / surprise_pct /
    beat."""
    if not API_KEY:
        return []
    data = _get("earnings-surprises", {"symbol": symbol})
    if not isinstance(data, list):
        return []
    out = []
    # FMP returns reverse-chrono; sort chronologically and take last `limit`
    data_sorted = sorted(data, key=lambda r: r.get("date") or "")
    for row in data_sorted[-limit:]:
        est = row.get("epsEstimated")
        act = row.get("epsActual")
        if est is None and act is None:
            continue
        beat = (act > est) if (est is not None and act is not None) else None
        surprise_pct = None
        if est is not None and act is not None and est != 0:
            surprise_pct = (act - est) / abs(est) * 100
        out.append({
            "date": (row.get("date") or "")[:10],
            "estimate": float(est) if est is not None else None,
            "actual": float(act) if act is not None else None,
            "surprise_pct": float(surprise_pct) if surprise_pct is not None else None,
            "beat": beat,
        })
    return out


def get_revenue_history(symbol: str, limit: int = 10) -> list[dict]:
    """Annual revenue history, chronological. Output: [{year, value}]."""
    if not API_KEY:
        return []
    data = _get("income-statement", {"symbol": symbol, "period": "annual", "limit": limit})
    if not isinstance(data, list):
        return []
    out = []
    for row in data:
        date_str = row.get("date") or row.get("fiscalYear") or ""
        try:
            year = int(str(date_str)[:4])
        except (ValueError, TypeError):
            continue
        rev = row.get("revenue")
        if rev is None:
            continue
        out.append({"year": year, "value": float(rev)})
    out.sort(key=lambda r: r["year"])
    return out


def get_next_earnings_date(symbol: str) -> str | None:
    """Get the next scheduled earnings date for `symbol`."""
    if not API_KEY:
        return None
    data = _get(f"earnings-calendar", {"symbol": symbol})
    if not isinstance(data, list):
        return None
    # Filter to future dates
    from datetime import date
    today = date.today().isoformat()
    future = [r for r in data if (r.get("date") or "") > today]
    if not future:
        return None
    future.sort(key=lambda r: r.get("date") or "")
    return (future[0].get("date") or "")[:10] or None
