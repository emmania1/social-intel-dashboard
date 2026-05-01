"""Social Intelligence Dashboard — Flask app.

Run:
    source venv/bin/activate
    python app.py
    open http://localhost:5050
"""
from __future__ import annotations

import datetime
import io
import json
import os
import socket
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta

import pandas as pd
from dotenv import load_dotenv
from flask import Flask, Response, jsonify, render_template, request  # noqa: F401

from lib.analysis import (
    align_weekly,
    build_narrative,
    pick_hero_signal,
    social_health_score,
    summarise_series,
)
from lib.news import fetch_news_with_fallback
from lib.reddit import fetch_reddit_weekly
from lib.sec import fetch_sec_filings_weekly
from lib.snapshots import list_snapshots, load_snapshot, save_snapshot
from lib.stock import fetch_stock, resolve_ticker
from lib.stocktwits import fetch_stocktwits_daily
from lib.trends import fetch_trends
from lib.wikipedia import fetch_wikipedia_daily
from lib.youtube import fetch_youtube_weekly

load_dotenv()

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False
# Disable static-file caching during dev so JS/CSS edits show up on refresh.
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0
# Reject NaN/Infinity in JSON responses — browsers can't parse them. pandas
# sometimes leaks NaN through to_dict; this is a belt-and-braces safeguard.
try:
    app.json.allow_nan = False  # Flask 2.3+
except AttributeError:  # pragma: no cover
    pass

# 2-year default (was 3) — shorter window = faster fetches = fewer timeouts on
# Render's free tier. Users can extend via the Advanced panel.
DEFAULT_WINDOW_DAYS = 2 * 365

# Belt-and-braces: set a global socket default timeout so any library that
# doesn't expose an explicit timeout (yfinance, googleapiclient) still can't
# hang forever. Set high enough to not break legit slow responses, low enough
# to save the request. Units: seconds.
socket.setdefaulttimeout(60)

# Global deadline for /api/generate. Render's free tier HTTP timeout is ~100s.
# We target 85s so there's headroom to serialise the response and flush.
GENERATE_BUDGET_SECONDS = 85.0


def _resolve_dates(start: str | None, end: str | None) -> tuple[str, str]:
    today = date.today()
    end_d = date.fromisoformat(end) if end else today
    start_d = date.fromisoformat(start) if start else end_d - timedelta(days=DEFAULT_WINDOW_DAYS)
    return start_d.isoformat(), end_d.isoformat()


@app.route("/")
def index():
    # Pass a per-request cache-buster so browsers never serve stale JS/CSS
    # from previous versions of the app.
    import time as _t
    return render_template("index.html", cb=str(int(_t.time())))


@app.route("/api/health")
def health():
    """Fast endpoint with no external calls — used by the UI to detect
    cold-start wake-ups. Answering within 100ms once the container is warm.
    """
    return jsonify({"ok": True, "time": datetime.datetime.utcnow().isoformat() + "Z"})


@app.route("/api/resolve")
def resolve():
    """Look up company name for a ticker. Used to pre-fill the UI."""
    ticker = (request.args.get("ticker") or "").strip().upper()
    if not ticker:
        return jsonify({"error": "ticker required"}), 400
    info = resolve_ticker(ticker)
    return jsonify({"ticker": ticker, **info})


@app.route("/api/generate", methods=["POST"])
def generate():
    payload = request.get_json(force=True, silent=True) or request.form.to_dict()
    ticker = (payload.get("ticker") or "").strip().upper()
    company = (payload.get("company") or "").strip()
    custom_term = (payload.get("custom_term") or "").strip()
    start, end = _resolve_dates(payload.get("start"), payload.get("end"))

    if not ticker:
        return jsonify({"error": "ticker is required"}), 400

    # Auto-resolve company name from yfinance if not provided. If the lookup
    # fails (Yahoo sometimes blocks cloud IPs or the symbol is unusual), we
    # fall back to the ticker itself and keep going — all other sources still
    # work without the company name.
    if not company:
        info = resolve_ticker(ticker)
        company = info["name"] if info.get("ok") else ticker

    search_term = custom_term or company

    # Build a list of search queries: [company/brand, TICKER, "TICKER stock"]
    # Arctic Shift and YouTube both handle these; case-insensitive dedup
    # happens inside the fetchers.
    reddit_queries = [search_term, ticker, f"${ticker}"]
    youtube_queries = [search_term, f"{ticker} stock"]

    from lib.reddit import DEFAULT_SUBREDDITS, discover_company_subreddits

    # Auto-detect company-specific subreddit(s) from ticker + company name —
    # e.g. CROX → r/crocs, SBUX → r/starbucks. Dead or typo-squatted subs
    # with < 500 subscribers are filtered out.
    discovered = discover_company_subreddits(ticker, company)
    company_sub_names = [d["name"] for d in discovered]

    # Global deadline — every fetcher gets whatever's left of this budget,
    # and any that don't finish in time are skipped (empty result) so the
    # response still ships rather than timing out the whole request.
    deadline = time.time() + GENERATE_BUDGET_SECONDS

    def budget() -> float:
        """Seconds remaining before the global deadline."""
        return max(1.0, deadline - time.time())

    # Run all fetchers in parallel (all I/O bound)
    with ThreadPoolExecutor(max_workers=9) as pool:
        f_stock = pool.submit(fetch_stock, ticker, start, end)
        f_trends = pool.submit(fetch_trends, search_term, start, end)
        f_reddit = pool.submit(
            fetch_reddit_weekly,
            reddit_queries,
            start,
            end,
            DEFAULT_SUBREDDITS,
            company_sub_names,
        )
        f_yt = pool.submit(fetch_youtube_weekly, youtube_queries, start, end)
        f_st = pool.submit(fetch_stocktwits_daily, ticker, start, end)
        f_wiki = pool.submit(fetch_wikipedia_daily, company, start, end)
        f_sec = pool.submit(fetch_sec_filings_weekly, ticker, start, end)
        f_news = pool.submit(fetch_news_with_fallback, ticker, company, start, end)

        # Each fetcher waits up to `budget()` seconds. If the deadline is hit
        # we return an empty frame and the response ships with partial data.
        stock_df = _safe(f_stock, "stock", timeout=budget())
        trends_df = _safe(f_trends, "trends", timeout=budget())
        reddit_df = _safe(f_reddit, "reddit", timeout=budget())
        yt_df = _safe(f_yt, "youtube", timeout=budget())
        st_df = _safe(f_st, "stocktwits", timeout=budget())
        wiki_result = _safe(f_wiki, "wikipedia", default=(pd.DataFrame(), None), timeout=budget())
        sec_df = _safe(f_sec, "sec", timeout=budget())
        news_result = _safe(f_news, "news", default=(pd.DataFrame(), "unavailable"), timeout=budget())

    news_df, news_source = news_result if isinstance(news_result, tuple) else (news_result, "unknown")

    wiki_df, wiki_title = wiki_result if isinstance(wiki_result, tuple) else (wiki_result, None)

    # Roll daily sources into weekly buckets so they align with the rest.
    # StockTwits: sum count/bullish/bearish; recompute ratio on weekly.
    st_weekly = _stocktwits_to_weekly(st_df)
    wiki_weekly = _daily_to_weekly(wiki_df, "views")

    summaries = [
        summarise_series("Stock price", stock_df, "close"),
        summarise_series("Google Trends", trends_df, "value"),
        summarise_series("Reddit mentions/wk", reddit_df, "count"),
        summarise_series("YouTube views/wk", yt_df, "views"),
        summarise_series("YouTube videos/wk", yt_df, "videos"),
        summarise_series("StockTwits msgs/wk", st_weekly, "count"),
        summarise_series("Wikipedia views/wk", wiki_weekly, "views"),
        summarise_series("SEC filings/wk", sec_df, "count"),
        summarise_series("News articles/wk", news_df, "count"),
    ]

    health = social_health_score(summaries)

    # Pick the single best non-stock source to pair with stock on the master chart
    hero = pick_hero_signal(
        {
            "trends": trends_df,
            "reddit": reddit_df,
            "youtube_views": yt_df,
            "stocktwits": st_weekly,
            "wikipedia": wiki_weekly,
            "sec": sec_df,
            "news": news_df,
        },
        {
            "trends": "value",
            "reddit": "count",
            "youtube_views": "views",
            "stocktwits": "count",
            "wikipedia": "views",
            "sec": "count",
            "news": "count",
        },
    )
    hero_key = hero[0] if hero else None
    _narrative_series = {
        "trends": trends_df,
        "reddit": reddit_df,
        "youtube": yt_df,
        "stocktwits": st_weekly,
        "wikipedia": wiki_weekly,
        "sec": sec_df,
        "news": news_df,
    }
    _narrative_cols = {
        "trends": "value",
        "reddit": "count",
        "youtube": "views",
        "stocktwits": "count",
        "wikipedia": "views",
        "sec": "count",
        "news": "count",
    }
    narrative = build_narrative(summaries, hero_key, _narrative_series, _narrative_cols)

    aligned = align_weekly(
        {
            "stock": (stock_df, "close"),
            "trends": (trends_df, "value"),
            "reddit": (reddit_df, "count"),
            "youtube_views": (yt_df, "views"),
            "youtube_videos": (yt_df, "videos"),
            "stocktwits": (st_weekly, "count"),
            "stocktwits_bullish_ratio": (st_weekly, "bullish_ratio"),
            "wikipedia": (wiki_weekly, "views"),
            "sec": (sec_df, "count"),
            "news": (news_df, "count"),
        }
    )

    payload = {
        "inputs": {
            "ticker": ticker,
            "company": company,
            "search_term": search_term,
            "reddit_queries": reddit_queries,
            "youtube_queries": youtube_queries,
            "wikipedia_title": wiki_title,
            "news_source": news_source,
            "start": start,
            "end": end,
            "discovered_subreddits": discovered,
            "subreddits_searched": list(DEFAULT_SUBREDDITS) + company_sub_names,
        },
        "series": {
            "stock": _clean_records(stock_df),
            "trends": _clean_records(trends_df),
            "reddit": _clean_records(reddit_df),
            "youtube": _clean_records(yt_df),
            "stocktwits": _clean_records(st_weekly),
            "wikipedia": _clean_records(wiki_weekly),
            "sec": _clean_records(sec_df),
            "news": _clean_records(news_df),
        },
        "aligned_weekly": _clean_records(aligned),
        "summaries": [s.to_dict() for s in summaries],
        "health_score": health,
        "hero": {"key": hero_key, "col": hero[1] if hero else None} if hero else None,
        "narrative": narrative,
    }

    # Persist snapshot so the user builds their own longitudinal dataset
    try:
        payload["snapshot_path"] = save_snapshot(ticker, payload)
    except Exception as exc:  # noqa: BLE001
        print(f"[snapshot] save failed: {exc}")

    # Safety net: serialize once up front so any remaining NaN/Inf or
    # un-encodable value raises a clean 500 BEFORE Flask flushes headers,
    # rather than truncating mid-body and producing a partial-JSON error in
    # the client.
    try:
        body = json.dumps(payload, allow_nan=False)
    except (ValueError, TypeError) as exc:
        print(f"[serialize] failed cleanly: {exc}")
        return jsonify({
            "error": f"Internal serialization error: {exc}. "
                     "This is a bug — please report the ticker you used."
        }), 500
    return Response(body, mimetype="application/json")


def _clean_records(df: pd.DataFrame) -> list[dict]:
    """Convert a DataFrame to records with NaN/Infinity replaced by None.

    Browsers' strict JSON parsers reject NaN and Infinity tokens; pandas
    emits them by default via to_dict(). This helper guarantees the output
    is always strict-JSON-safe, no matter what the upstream calculation did
    (e.g. dividing by zero in bullish_ratio when no messages are tagged).
    """
    if df is None or df.empty:
        return []
    import numpy as np
    out = df.copy()
    # Replace inf/-inf with NaN first, then NaN with None.
    out = out.replace([np.inf, -np.inf], np.nan)
    return out.astype(object).where(out.notna(), None).to_dict(orient="records")


def _daily_to_weekly(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if df is None or df.empty or col not in df.columns:
        return pd.DataFrame(columns=["date", col])
    d = df.copy()
    d["date"] = pd.to_datetime(d["date"])
    d = d.set_index("date").resample("W-SUN").sum(numeric_only=True)
    d.index = d.index.strftime("%Y-%m-%d")
    return d.reset_index()


def _stocktwits_to_weekly(df: pd.DataFrame) -> pd.DataFrame:
    """StockTwits needs special weekly rollup: sum counts, recompute ratio."""
    if df is None or df.empty:
        return pd.DataFrame(columns=["date", "count", "bullish", "bearish", "bullish_ratio"])
    d = df.copy()
    d["date"] = pd.to_datetime(d["date"])
    agg = d.set_index("date")[["count", "bullish", "bearish"]].resample("W-SUN").sum()
    tagged = agg["bullish"] + agg["bearish"]
    agg["bullish_ratio"] = (agg["bullish"] / tagged.where(tagged > 0)).round(3)
    agg.index = agg.index.strftime("%Y-%m-%d")
    return agg.reset_index()


def _safe(future, label, default=None, timeout: float | None = None):
    """Resolve a future with optional wall-clock timeout.

    If the future doesn't complete within `timeout` seconds we log it and
    return `default`. The underlying thread keeps running but the response
    no longer waits on it — the next request starts fresh.
    """
    try:
        return future.result(timeout=timeout)
    except Exception as exc:  # noqa: BLE001 — intentionally broad
        kind = "timed out" if "TimeoutError" in type(exc).__name__ else "failed"
        print(f"[{label}] {kind}: {exc}")
        return default if default is not None else pd.DataFrame()


@app.route("/api/snapshots")
def snapshots():
    """List previous runs for a ticker."""
    ticker = (request.args.get("ticker") or "").strip().upper()
    if not ticker:
        return jsonify({"error": "ticker required"}), 400
    return jsonify({"ticker": ticker, "snapshots": list_snapshots(ticker)})


@app.route("/api/snapshots/<ticker>/<filename>")
def get_snapshot(ticker, filename):
    """Fetch one cached snapshot by filename."""
    data = load_snapshot(ticker, filename)
    if data is None:
        return jsonify({"error": "not found"}), 404
    return jsonify(data)


@app.route("/api/export.csv", methods=["POST"])
def export_csv():
    payload = request.get_json(force=True, silent=True) or {}
    rows = payload.get("rows") or []
    if not rows:
        return Response("no data", status=400)
    df = pd.DataFrame(rows)
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    filename = payload.get("filename", "export.csv")
    return Response(
        buf.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5050"))
    app.run(host="127.0.0.1", port=port, debug=True)
