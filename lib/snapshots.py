"""Snapshot caching — build your own longitudinal dataset over time.

Each time a user generates a dashboard, we save the full response JSON to
`data/{TICKER}/{YYYY-MM-DD_HHMMSS}.json`. Over weeks/months of use you
accumulate a time-series of dashboards per ticker that no single API gives
you — particularly valuable for the "recent-weighted" sources (StockTwits,
YouTube view counts, current-state metrics) whose historical depth is
truncated.
"""
from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent / "data"


def save_snapshot(ticker: str, payload: dict) -> str:
    """Persist a response payload. Returns the path written."""
    ticker = (ticker or "UNKNOWN").upper()
    outdir = ROOT / ticker
    outdir.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y-%m-%d_%H%M%S")
    path = outdir / f"{ts}.json"
    path.write_text(json.dumps(payload, default=str))
    return str(path)


def list_snapshots(ticker: str) -> list[dict]:
    """List previous snapshots for a ticker (newest first).

    Returns lightweight metadata — timestamp, health_score, filename — so the
    UI can offer a "previous runs" dropdown without loading every file.
    """
    ticker = (ticker or "").upper()
    outdir = ROOT / ticker
    if not outdir.exists():
        return []
    out = []
    for f in sorted(outdir.glob("*.json"), reverse=True):
        try:
            data = json.loads(f.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        out.append(
            {
                "filename": f.name,
                "path": str(f.relative_to(ROOT)),
                "captured_at": f.stem,  # "YYYY-MM-DD_HHMMSS"
                "ticker": data.get("inputs", {}).get("ticker", ticker),
                "health_score": data.get("health_score"),
                "company": data.get("inputs", {}).get("company"),
                "size_bytes": f.stat().st_size,
            }
        )
    return out


def load_snapshot(ticker: str, filename: str) -> dict | None:
    """Load a specific snapshot by filename."""
    ticker = (ticker or "").upper()
    # Strict path containment to prevent traversal
    path = ROOT / ticker / filename
    try:
        path = path.resolve()
        root_resolved = ROOT.resolve()
    except OSError:
        return None
    if os.path.commonpath([str(path), str(root_resolved)]) != str(root_resolved):
        return None
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None
