"""Google Drive CSV reader for coverage datasets.

Reads CSV files stored in Google Drive and returns them as pandas DataFrames.
Used by /api/coverage so the dashboard can pull live data from Drive instead
of bundling stale local copies.

Auth: NONE. Each Drive file must be shared as
    "Anyone with the link → Viewer"
We hit Drive's public download endpoint directly; no service account, no
OAuth, no env vars. The trade-off is that the file IDs in COVERAGE_FILES
are effectively world-readable to anyone who reads the repo. The data was
judged non-sensitive (public coverage/research datasets), so this is fine.

If you ever need to make these private again, replace _download_csv with
a service-account path — see git history for the previous implementation.
"""
from __future__ import annotations

import io
from typing import Dict

import pandas as pd
import requests

PUBLIC_URL = "https://drive.google.com/uc?export=download&id={file_id}"
TIMEOUT_SECONDS = 30

# Hardcoded mapping: ticker -> {dataset_label: drive_file_id}.
# Dataset labels are stable keys the frontend can switch on; file IDs come
# from the May 2026 Pillsbury Lake Drive migration. Add new tickers/datasets
# here — no other code change required.
COVERAGE_FILES: Dict[str, Dict[str, str]] = {
    "EAT": {
        "reddit_chilis_monthly_unique": "1J7rE44U8PEq3mJFf8GKPjmq_YbQXU17A",
        "youtube_chilis_cumulative":    "1_EsSu_a56rd7gaJsG-61vae9-Qam3aZv",
        "youtube_chilis_creators":      "1llRCHXwCQHXDRZeiaTUNwj_Rd0CFRTY5",
        "analytics_peaks":              "1lykHg3O-Y7-xT-YSkpY7MXM6Hys8Z54d",
        "eat_key_events":               "1IJGzkMSjC7oDmMoX6Led-o1UZWchKGqt",
        "eat_earnings":                 "1WvB-HzoMDncHOU9vOy6AzheasUJbugRr",
    },
    "CROX": {
        "silhouettes":            "1PJsoIMroUuY9rrowJaKMzeDcc6wKpXE0",
        "crox_stock_monthly":     "1qQCZT25tO_rXbOnbk9tcH9xeD26Byf2v",
        "youtube_yoy":            "1Wq_outD5ANHllUtsNtQP1Q0HuJZHW8qk",
        "sneaker_news_raw":       "19ek2v8-kwSkSZlvadfW5D22T3YXDHpN9",
        "google_trends_relative": "1-JTxgkkW21YD6r--65R72HZQB_PI7IoJ",
    },
    "GAW": {
        "youtube_evergreen_anchors": "1ZKH3-lEm9cQE5Z9gTKn6v3ERuOPGGzVW",
        "beginner_funnel_history":   "1GsK0MQhJpggQetGU38pDy8veg6TSQT3n",
    },
}


class DriveAuthError(RuntimeError):
    """Kept for backward compatibility with callers (app.py imports it).

    The public-URL path doesn't authenticate, so this is never raised today,
    but we leave the symbol exported so a future swap back to service-account
    auth doesn't require changes in the caller.
    """


class DriveFetchError(RuntimeError):
    """Raised when a Drive file can't be downloaded or parsed as CSV."""


def _download_csv(file_id: str) -> pd.DataFrame:
    """Fetch one CSV from Drive via the public download URL.

    Drive sometimes interposes a "virus scan" HTML interstitial for files
    above ~50MB; our coverage files are all <100KB so we never hit it. If
    that ever changes, the HTML response will fail pd.read_csv with a clear
    parser error and DriveFetchError below will surface it.
    """
    url = PUBLIC_URL.format(file_id=file_id)
    try:
        r = requests.get(url, timeout=TIMEOUT_SECONDS, allow_redirects=True)
        r.raise_for_status()
    except requests.RequestException as exc:
        raise DriveFetchError(f"download failed for {file_id}: {exc}") from exc

    # Drive returns HTML if the file is private — easiest tell is that the
    # body is HTML, not CSV. Surface this as a clear error so the user knows
    # to flip the file's sharing setting.
    if r.text.lstrip().lower().startswith("<!doctype html") or "<html" in r.text[:200].lower():
        raise DriveFetchError(
            f"{file_id} returned HTML instead of CSV — file is not public. "
            "Share it as 'Anyone with the link → Viewer' in Drive."
        )

    try:
        return pd.read_csv(io.BytesIO(r.content))
    except Exception as exc:  # pandas ParserError / EmptyDataError / etc.
        raise DriveFetchError(f"{file_id} parse failed: {exc}") from exc


# Public name kept for backward compatibility.
def read_csv_from_drive(file_id: str) -> pd.DataFrame:
    """Download one Drive file by ID and return it as a DataFrame."""
    return _download_csv(file_id)


def read_coverage_data(ticker: str) -> Dict[str, pd.DataFrame]:
    """Load every CSV registered for `ticker` and return a dict of DataFrames.

    Keys match COVERAGE_FILES[ticker]. Unknown tickers raise KeyError so
    the caller can return a clean 404. A failure on any single file is
    logged and that key is omitted — partial coverage beats nothing when
    the dashboard is rendering.
    """
    ticker = ticker.upper()
    if ticker not in COVERAGE_FILES:
        raise KeyError(
            f"No coverage mapping for {ticker!r}. "
            f"Known tickers: {sorted(COVERAGE_FILES)}"
        )
    out: Dict[str, pd.DataFrame] = {}
    for label, file_id in COVERAGE_FILES[ticker].items():
        try:
            out[label] = _download_csv(file_id)
        except DriveFetchError as exc:
            print(f"[drive_reader] {ticker} {label}: {exc}")
        except Exception as exc:  # noqa: BLE001 — last-resort net
            print(f"[drive_reader] {ticker} {label} unexpected: {exc}")
    return out
