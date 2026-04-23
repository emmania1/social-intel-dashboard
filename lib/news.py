"""News article volume.

Primary: GDELT Project Doc 2.0 timeline (global news, deep history, free).
Fallback: yfinance.Ticker(symbol).news — recent Yahoo Finance headlines
only (~30 most recent), used when GDELT rate-limits Render's cloud IPs.

For tickers where neither works we return an empty frame; the UI hides the
chart cleanly.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone

import pandas as pd
import requests

GDELT_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
UA = "social-intel-dashboard/1.0 (research)"


def _gdelt_time(d: str) -> str:
    return datetime.strptime(d, "%Y-%m-%d").strftime("%Y%m%d") + "000000"


def fetch_news_weekly(ticker: str, company: str, start: str, end: str) -> pd.DataFrame:
    """Return weekly global news-article counts matching the query.

    Uses an OR query across ticker + company name, quoted for phrase match.
    Returns columns: date, count.
    """
    ticker = (ticker or "").strip().upper()
    company = (company or "").strip()
    # GDELT rejects short/ambiguous phrases ("CROX" = "too short"), and
    # complex OR queries get rate-limited more aggressively. Strategy: prefer
    # the distinctive company name (quoted for phrase match). Only fall back
    # to the ticker if we have nothing else. This means extremely short
    # companies with no name still won't match but that's a rare edge case.
    if company and len(company) >= 5 and company.upper() != ticker:
        query = f'"{company}"'
    elif ticker and len(ticker) >= 5:
        query = f'"{ticker}"'
    else:
        return pd.DataFrame(columns=["date", "count"])

    params = {
        "query": query,
        "mode": "TimelineVolRaw",
        "format": "json",
        "timespan": "5years",
        "startdatetime": _gdelt_time(start),
        "enddatetime": _gdelt_time(end),
        # "TIMELINEVOLRAW" gives actual article counts per day in the window.
        "timelinesmooth": 7,  # 7-day smoothing
    }
    # Polite delay — GDELT's free endpoint asks for <1 req/5s per requester
    time.sleep(1.0)
    try:
        r = requests.get(GDELT_URL, params=params, headers={"User-Agent": UA}, timeout=45)
        r.raise_for_status()
    except requests.RequestException as exc:
        print(f"[news/gdelt] {query!r}: {exc}")
        return pd.DataFrame(columns=["date", "count"])
    # GDELT sometimes returns a text rate-limit message with HTTP 200
    if "limit requests" in r.text[:200].lower() or "too short" in r.text[:200].lower():
        print(f"[news/gdelt] rate/query rejected: {r.text[:150]}")
        return pd.DataFrame(columns=["date", "count"])

    # GDELT sometimes returns HTML when the query errors; guard the json parse
    try:
        data = r.json()
    except ValueError:
        print(f"[news/gdelt] non-JSON response (first 120 chars): {r.text[:120]}")
        return pd.DataFrame(columns=["date", "count"])

    timeline = data.get("timeline") or []
    rows = []
    for series in timeline:
        for p in series.get("data", []):
            # GDELT timestamps look like "20230816T000000Z"
            ts = p.get("date", "")
            try:
                d = datetime.strptime(ts[:8], "%Y%m%d").strftime("%Y-%m-%d")
            except (TypeError, ValueError):
                continue
            rows.append({"date": d, "count": float(p.get("value", 0))})
    if not rows:
        return pd.DataFrame(columns=["date", "count"])
    df = pd.DataFrame(rows)
    # Bucket to weekly (week-ending Sunday) and sum
    df["dt"] = pd.to_datetime(df["date"])
    df["week"] = df["dt"].dt.to_period("W-SUN").dt.end_time.dt.strftime("%Y-%m-%d")
    weekly = df.groupby("week")["count"].sum().reset_index()
    weekly.columns = ["date", "count"]
    # GDELT returns float; cast to int to keep the JSON clean
    weekly["count"] = weekly["count"].round().astype(int)
    return weekly.sort_values("date").reset_index(drop=True)


def _yahoo_news_weekly(ticker: str, start: str, end: str) -> pd.DataFrame:
    """Fallback: count recent Yahoo Finance news items per week via yfinance.

    Yahoo caps the feed at ~30 most recent items, so this only covers the
    last 2-6 weeks of activity for most tickers. Still better than 0 when
    GDELT is blocked.
    """
    try:
        from lib.stock import _ticker as _make_ticker
    except ImportError:
        return pd.DataFrame(columns=["date", "count"])
    try:
        items = _make_ticker(ticker).news or []
    except Exception as exc:  # noqa: BLE001
        print(f"[news/yahoo] {ticker}: {exc}")
        return pd.DataFrame(columns=["date", "count"])
    if not items:
        return pd.DataFrame(columns=["date", "count"])
    start_dt = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt = datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    rows = []
    for item in items:
        # yfinance schema varies: top-level providerPublishTime OR content.pubDate
        ts = None
        if item.get("providerPublishTime"):
            try:
                ts = datetime.fromtimestamp(int(item["providerPublishTime"]), tz=timezone.utc)
            except (TypeError, ValueError):
                pass
        if ts is None and isinstance(item.get("content"), dict):
            pub = item["content"].get("pubDate")
            if pub:
                try:
                    ts = datetime.fromisoformat(pub.replace("Z", "+00:00"))
                except ValueError:
                    pass
        if ts is None:
            continue
        if not (start_dt <= ts <= end_dt):
            continue
        rows.append({"ts": ts})
    if not rows:
        return pd.DataFrame(columns=["date", "count"])
    df = pd.DataFrame(rows)
    df["week"] = df["ts"].dt.to_period("W-SUN").dt.end_time.dt.strftime("%Y-%m-%d")
    weekly = df.groupby("week").size().reset_index(name="count")
    weekly.columns = ["date", "count"]
    return weekly.sort_values("date").reset_index(drop=True)


def fetch_news_with_fallback(ticker: str, company: str, start: str, end: str) -> tuple[pd.DataFrame, str]:
    """Try GDELT first; if empty (rate-limited, too-short phrase, etc) fall
    back to Yahoo Finance. Returns (df, source_label) so the UI can show
    which source was used.
    """
    df = fetch_news_weekly(ticker, company, start, end)
    if not df.empty:
        return df, "GDELT global news"
    df = _yahoo_news_weekly(ticker, start, end)
    if not df.empty:
        return df, "Yahoo Finance news (recent only)"
    return pd.DataFrame(columns=["date", "count"]), "no news source available"
