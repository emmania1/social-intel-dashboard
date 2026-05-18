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
from flask import Flask, Response, jsonify, render_template, request, send_from_directory  # noqa: F401
from flask_cors import CORS

from lib.analysis import (
    align_weekly,
    build_narrative,
    pick_hero_signal,
    social_health_score,
    summarise_series,
)
from lib.docs import delete_doc, get_all_text, list_docs, save_doc
from lib.drive_reader import COVERAGE_FILES, DriveAuthError, read_coverage_data
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
# Allow cross-origin requests from any client. This is a public read-only
# data API with no auth/session cookies, so `*` is safe.
CORS(app, origins="*")
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
    # Serves dashboard.html from the project root as a raw static file.
    # SEND_FILE_MAX_AGE_DEFAULT=0 above keeps the browser from caching it.
    return send_from_directory(app.root_path, "dashboard.html")


@app.route("/dashboards/<name>")
def company_dashboard(name):
    """Serve a per-ticker dashboard HTML file from /dashboards.

    Used by the iframes inside dashboard.html's company tabs (BBW/EAT/CROX/GAW).
    send_from_directory normalises the path and refuses traversal, so a
    request like /dashboards/../app.py 404s.
    """
    return send_from_directory(
        os.path.join(app.root_path, "dashboards"),
        f"{name}.html",
    )


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


@app.route("/api/financials")
def financials():
    """Fast per-ticker yfinance snapshot for the dashboard's price tiles.

    Returns {"ticker": "BBW", "info": {...}}. Wrapping in "info" matches
    the frontend's existing contract (d.info.currentPrice etc.) and keeps
    this endpoint distinct from /api/full, which bundles social + alerts
    and takes ~60s. This one is ~2-5s.
    """
    ticker = (request.args.get("ticker") or "").strip().upper()
    if not ticker:
        return jsonify({"error": "ticker required"}), 400

    # Reuse the curl_cffi-impersonating session so Yahoo doesn't block
    # Render's datacenter IPs.
    from lib.stock import _ticker as _yf_ticker
    try:
        t = _yf_ticker(ticker)
        info = t.info or {}
    except Exception as exc:  # noqa: BLE001
        print(f"[financials] {ticker}: {exc}")
        return jsonify({"ticker": ticker, "info": {}, "error": str(exc)}), 502

    # yfinance's info dict can contain NaN/Inf for fields that don't apply
    # (e.g. forwardPE on a loss-making name). Strip them so the response
    # serialises under app.json.allow_nan=False.
    import math
    clean = {
        k: (None if isinstance(v, float) and (math.isnan(v) or math.isinf(v)) else v)
        for k, v in info.items()
    }
    return jsonify({"ticker": ticker, "info": clean})


@app.route("/api/docs/upload", methods=["POST"])
def docs_upload():
    """Accept a multipart file upload for a ticker, parse it to text, store
    both the original and the extracted text on the persistent disk."""
    ticker = (request.args.get("ticker") or "").strip().upper()
    if not ticker:
        return jsonify({"error": "ticker query param required"}), 400
    if "file" not in request.files:
        return jsonify({"error": "no file in request body"}), 400
    f = request.files["file"]
    if not f.filename:
        return jsonify({"error": "no filename"}), 400
    try:
        meta = save_doc(ticker, f.filename, f.read())
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    return jsonify({"ticker": ticker, **meta})


@app.route("/api/docs", methods=["GET"])
def docs_list():
    """List every uploaded doc for a ticker."""
    ticker = (request.args.get("ticker") or "").strip().upper()
    if not ticker:
        return jsonify({"error": "ticker required"}), 400
    return jsonify({"ticker": ticker, "docs": list_docs(ticker)})


@app.route("/api/docs/text", methods=["GET"])
def docs_text():
    """Concatenated text of every doc for a ticker, capped. Used by the
    chat bubble to ground answers in the user's uploaded research."""
    ticker = (request.args.get("ticker") or "").strip().upper()
    if not ticker:
        return jsonify({"error": "ticker required"}), 400
    try:
        max_chars = max(1000, min(int(request.args.get("max_chars", 80000)), 200000))
    except (TypeError, ValueError):
        max_chars = 80000
    return jsonify({"ticker": ticker, **get_all_text(ticker, max_chars=max_chars)})


@app.route("/api/docs/<ticker>/<path:filename>", methods=["DELETE"])
def docs_delete(ticker, filename):
    try:
        ok = delete_doc(ticker, filename)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    if not ok:
        return jsonify({"error": "not found"}), 404
    return jsonify({"ok": True})


@app.route("/api/claude", methods=["POST"])
def claude_proxy():
    """Proxy to Anthropic's Messages API using a server-side API key.

    The frontend dashboard.html calls callClaude() throughout — search,
    morning notes, screeners, thesis builder. The Anthropic API can't be
    hit directly from the browser without exposing the key, so we forward
    the request body verbatim and inject the auth headers here.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return jsonify({
            "error": "ANTHROPIC_API_KEY not set on the server. "
                     "Add it in Render's Environment tab."
        }), 503

    body = request.get_json(force=True, silent=True) or {}
    body.setdefault("model", "claude-sonnet-4-6")
    body.setdefault("max_tokens", 1500)

    import requests as http
    try:
        r = http.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json=body,
            timeout=120,
        )
    except http.exceptions.Timeout:
        return jsonify({"error": "Anthropic API timeout (>120s)"}), 504
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": f"Proxy failed: {exc}"}), 502

    return Response(r.content, status=r.status_code, mimetype="application/json")


def _run_generate(
    ticker: str,
    company: str = "",
    custom_term: str = "",
    start: str | None = None,
    end: str | None = None,
) -> dict:
    """Core social-data fetch. Returns the payload dict that /api/generate
    serialises. Extracted so /api/full can reuse the same logic without
    going through Flask's routing layer.
    """
    ticker = ticker.strip().upper()
    if not ticker:
        raise ValueError("ticker is required")

    start, end = _resolve_dates(start, end)
    company = (company or "").strip()
    custom_term = (custom_term or "").strip()

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

    return {
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


@app.route("/api/generate", methods=["POST"])
def generate():
    payload_in = request.get_json(force=True, silent=True) or request.form.to_dict()
    ticker = (payload_in.get("ticker") or "").strip().upper()
    if not ticker:
        return jsonify({"error": "ticker is required"}), 400

    try:
        payload = _run_generate(
            ticker,
            company=(payload_in.get("company") or "").strip(),
            custom_term=(payload_in.get("custom_term") or "").strip(),
            start=payload_in.get("start"),
            end=payload_in.get("end"),
        )
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

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


@app.route("/api/coverage")
def coverage():
    """Return all CSVs registered for `ticker` from Google Drive as JSON.

    Response shape:
        {
            "ticker": "EAT",
            "datasets": {
                "reddit_chilis_monthly_unique": [{...row}, ...],
                "youtube_chilis_cumulative":    [{...row}, ...],
                ...
            }
        }

    Datasets that fail to download are omitted (errors logged server-side)
    so the dashboard can still render with partial data — same philosophy
    as /api/generate.
    """
    ticker = (request.args.get("ticker") or "").strip().upper()
    if not ticker:
        return jsonify({"error": "ticker required"}), 400
    if ticker not in COVERAGE_FILES:
        return jsonify({
            "error": f"no coverage mapping for {ticker}",
            "known_tickers": sorted(COVERAGE_FILES),
        }), 404
    try:
        frames = read_coverage_data(ticker)
    except DriveAuthError as exc:
        # Config problem on the server, not a client error.
        return jsonify({"error": str(exc)}), 503
    datasets = {label: _clean_records(df) for label, df in frames.items()}
    return jsonify({"ticker": ticker, "datasets": datasets})


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


COVERAGE_TICKERS = frozenset({"BBW", "GAW", "CROX", "EAT"})


def _fetch_yfinance_financials(ticker: str) -> dict:
    """Fetch yfinance-derived snapshot for /api/full.

    Returns a flat dict of valuation/margin/price fields plus earningsHistory
    (last 8 quarters) and revenueHistory (annual). Individual field failures
    fall back to None so a single broken source doesn't sink the whole call.
    """
    # Reuse the curl_cffi-impersonating session from lib.stock so Yahoo
    # doesn't block us when called from Render's datacenter IPs.
    from lib.stock import _ticker as _yf_ticker

    try:
        t = _yf_ticker(ticker)
        info = t.info or {}
    except Exception as exc:  # noqa: BLE001
        print(f"[financials] info failed for {ticker}: {exc}")
        return {}

    # earnings_dates returns 20+ quarters of historical EPS data, vs the 4
    # served by earnings_history. Surprise(%) here is already in percent
    # form (3.28 = 3.28%), unlike earnings_history.surprisePercent (decimal).
    earnings_history: list[dict] = []
    try:
        ed = t.earnings_dates
        if ed is not None and not ed.empty:
            now = pd.Timestamp.now(tz=ed.index.tz) if ed.index.tz is not None else pd.Timestamp.now()
            past = ed[ed.index < now].sort_index()
            for idx, row in past.tail(8).iterrows():
                est = row.get("EPS Estimate")
                act = row.get("Reported EPS")
                surprise = row.get("Surprise(%)")
                est_f = float(est) if pd.notna(est) else None
                act_f = float(act) if pd.notna(act) else None
                surp_f = float(surprise) if pd.notna(surprise) else None
                beat = act_f > est_f if (est_f is not None and act_f is not None) else None
                earnings_history.append({
                    "date": str(idx)[:10],
                    "estimate": est_f,
                    "actual": act_f,
                    "surprise_pct": surp_f,
                    "beat": beat,
                })
    except Exception as exc:  # noqa: BLE001
        print(f"[financials] earnings_dates failed for {ticker}: {exc}")

    revenue_history: list[dict] = []
    try:
        fin = t.income_stmt
        if fin is not None and not fin.empty and "Total Revenue" in fin.index:
            row = fin.loc["Total Revenue"]
            for col_date, val in row.items():
                if pd.isna(val):
                    continue
                try:
                    year = col_date.year if hasattr(col_date, "year") else int(str(col_date)[:4])
                except Exception:  # noqa: BLE001
                    continue
                revenue_history.append({"year": year, "value": float(val)})
            revenue_history.sort(key=lambda x: x["year"])
    except Exception as exc:  # noqa: BLE001
        print(f"[financials] revenue_history failed for {ticker}: {exc}")

    next_earnings_date = None
    try:
        cal = t.calendar
        if isinstance(cal, dict):
            ed = cal.get("Earnings Date")
            if ed:
                first = ed[0] if isinstance(ed, list) and ed else ed
                next_earnings_date = str(first)[:10]
        elif cal is not None and hasattr(cal, "loc") and "Earnings Date" in getattr(cal, "index", []):
            val = cal.loc["Earnings Date"]
            next_earnings_date = str(val.iloc[0])[:10] if hasattr(val, "iloc") else str(val)[:10]
    except Exception as exc:  # noqa: BLE001
        print(f"[financials] calendar failed for {ticker}: {exc}")

    return {
        "currentPrice": info.get("currentPrice"),
        "previousClose": info.get("previousClose"),
        "marketCap": info.get("marketCap"),
        "revenue": info.get("totalRevenue"),
        "revenueGrowth": info.get("revenueGrowth"),
        "grossMargin": info.get("grossMargins"),
        "operatingMargin": info.get("operatingMargins"),
        "netMargin": info.get("profitMargins"),
        "forwardPE": info.get("forwardPE"),
        "trailingPE": info.get("trailingPE"),
        "priceToBook": info.get("priceToBook"),
        "eps": info.get("trailingEps"),
        "fiftyTwoWeekLow": info.get("fiftyTwoWeekLow"),
        "fiftyTwoWeekHigh": info.get("fiftyTwoWeekHigh"),
        "enterpriseToEbitda": info.get("enterpriseToEbitda"),
        "enterpriseToRevenue": info.get("enterpriseToRevenue"),
        "freeCashflow": info.get("freeCashflow"),
        "nextEarningsDate": next_earnings_date,
        "longName": info.get("longName"),
        "shortName": info.get("shortName"),
        "sector": info.get("sector"),
        "exchange": info.get("exchange"),
        "description": info.get("longBusinessSummary"),
        "earningsHistory": earnings_history,
        "revenueHistory": revenue_history,
    }


def _compute_alerts(ticker: str, financials: dict, social: dict) -> list[dict]:
    """Threshold-driven alerts derived from financials + social data.

    Severity is 'info' for positive signals (beats, surges, near-highs) and
    'warn' for negative ones (misses, collapses, near-lows). Front-end
    decides colour/icon by severity, type is for filtering/grouping.
    """
    alerts: list[dict] = []

    price = financials.get("currentPrice")
    hi = financials.get("fiftyTwoWeekHigh")
    lo = financials.get("fiftyTwoWeekLow")
    if isinstance(price, (int, float)) and isinstance(hi, (int, float)) and hi > 0 and price >= hi * 0.95:
        alerts.append({
            "severity": "info",
            "type": "near_52w_high",
            "message": f"${price:.2f} within 5% of 52-week high (${hi:.2f})",
        })
    if isinstance(price, (int, float)) and isinstance(lo, (int, float)) and lo > 0 and price <= lo * 1.05:
        alerts.append({
            "severity": "warn",
            "type": "near_52w_low",
            "message": f"${price:.2f} within 5% of 52-week low (${lo:.2f})",
        })

    eh = financials.get("earningsHistory") or []
    if eh:
        latest = eh[-1]
        beat = latest.get("beat")
        surp = latest.get("surprise_pct")
        if beat is True:
            msg = "Last quarter beat estimates"
            if isinstance(surp, (int, float)):
                msg += f" by {surp:.1f}%"
            alerts.append({"severity": "info", "type": "earnings_beat", "message": msg})
        elif beat is False:
            msg = "Last quarter missed estimates"
            if isinstance(surp, (int, float)):
                msg += f" by {abs(surp):.1f}%"
            alerts.append({"severity": "warn", "type": "earnings_miss", "message": msg})

    for s in social.get("summaries", []) or []:
        metric = s.get("metric")
        if metric == "Stock price":
            continue
        trend = s.get("trend_12w")
        pct = s.get("pct_from_peak")
        if trend == "rising" and isinstance(pct, (int, float)) and pct >= -10:
            alerts.append({
                "severity": "info",
                "type": "social_rising",
                "message": f"{metric}: rising 12-wk trend, within 10% of peak",
            })
        elif trend == "falling" and isinstance(pct, (int, float)) and pct <= -50:
            alerts.append({
                "severity": "warn",
                "type": "social_falling",
                "message": f"{metric}: falling trend, {abs(pct):.0f}% below peak",
            })

    health = social.get("health_score")
    if isinstance(health, (int, float)):
        if health >= 80:
            alerts.append({
                "severity": "info",
                "type": "strong_health",
                "message": f"Social health {health:.0f}/100 — near peak engagement",
            })
        elif health <= 25:
            alerts.append({
                "severity": "warn",
                "type": "weak_health",
                "message": f"Social health {health:.0f}/100 — well below peak",
            })

    return alerts


@app.route("/api/full")
def api_full():
    """Master endpoint: /api/generate output + yfinance financials + alerts
    in a single round-trip.

    Runs social and financial fetches in parallel so wall-clock is bounded
    by max(social, financials) rather than the sum.
    """
    ticker = (request.args.get("ticker") or "").strip().upper()
    if not ticker:
        return jsonify({"error": "ticker required"}), 400

    company_arg = (request.args.get("company") or "").strip()
    custom_term = (request.args.get("custom_term") or "").strip()
    start = request.args.get("start")
    end = request.args.get("end")

    with ThreadPoolExecutor(max_workers=2) as pool:
        f_social = pool.submit(_run_generate, ticker, company_arg, custom_term, start, end)
        f_fin = pool.submit(_fetch_yfinance_financials, ticker)

        try:
            social = f_social.result()
        except Exception as exc:  # noqa: BLE001
            print(f"[full] social fetch failed: {exc}")
            social = {"error": str(exc), "series": {}, "summaries": [], "health_score": None}
        try:
            financials = f_fin.result()
        except Exception as exc:  # noqa: BLE001
            print(f"[full] financials fetch failed: {exc}")
            financials = {}

    company_name = (
        social.get("inputs", {}).get("company")
        or financials.get("longName")
        or financials.get("shortName")
        or ticker
    )
    alerts = _compute_alerts(ticker, financials, social)

    payload = {
        "ticker": ticker,
        "company": company_name,
        "isCoverage": ticker in COVERAGE_TICKERS,
        "financials": financials,
        "social": social,
        "alerts": alerts,
    }

    try:
        body = json.dumps(payload, allow_nan=False)
    except (ValueError, TypeError) as exc:
        print(f"[full] serialize failed: {exc}")
        return jsonify({"error": f"Internal serialization error: {exc}"}), 500
    return Response(body, mimetype="application/json")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5050"))
    app.run(host="127.0.0.1", port=port, debug=True)
