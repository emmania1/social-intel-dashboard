"""Per-ticker document storage + text extraction.

Uploaded files live at $DOCS_ROOT/<TICKER>/<filename>. Plain-text extracts
are cached alongside as <filename>.txt so the chat bubble can inject the
content into prompts without re-parsing on every question.

On Render, $DOCS_ROOT defaults to /data/docs (the persistent-disk mount).
Locally, falls back to <repo>/data/docs which is gitignored.
"""
from __future__ import annotations

import os
import re
from datetime import datetime
from pathlib import Path

from werkzeug.utils import secure_filename

ALLOWED_EXT = {".pdf", ".txt", ".md", ".csv"}
MAX_FILE_BYTES = 25 * 1024 * 1024  # 25 MB cap per upload


def _resolve_docs_root() -> Path:
    """Find a writable directory for uploaded docs. Render's persistent disk
    mounts at /data/docs; locally we fall back to <repo>/data/docs."""
    candidates = [
        os.environ.get("DOCS_ROOT"),
        "/data/docs",
        str(Path(__file__).parent.parent / "data" / "docs"),
    ]
    for c in candidates:
        if not c:
            continue
        try:
            Path(c).mkdir(parents=True, exist_ok=True)
            probe = Path(c) / ".write_test"
            probe.write_text("x")
            probe.unlink()
            return Path(c)
        except (PermissionError, OSError):
            continue
    raise RuntimeError("no writable docs directory found")


DOCS_ROOT = _resolve_docs_root()


def _valid_ticker(ticker: str) -> str:
    t = (ticker or "").strip().upper()
    if not re.fullmatch(r"[A-Z0-9.\-]{1,15}", t):
        raise ValueError(f"invalid ticker: {ticker!r}")
    return t


def _ticker_dir(ticker: str) -> Path:
    p = DOCS_ROOT / _valid_ticker(ticker)
    p.mkdir(parents=True, exist_ok=True)
    return p


def _extract_text(file_path: Path) -> str:
    """Best-effort text extraction. Returns empty string for image-only PDFs
    (no OCR yet) or unparseable inputs — caller treats missing text as a
    soft failure rather than an error."""
    ext = file_path.suffix.lower()
    if ext == ".pdf":
        try:
            from pypdf import PdfReader
            reader = PdfReader(str(file_path))
            return "\n\n".join((page.extract_text() or "") for page in reader.pages)
        except Exception as exc:  # noqa: BLE001
            print(f"[docs] pypdf failed for {file_path.name}: {exc}")
            return ""
    if ext in {".txt", ".md", ".csv"}:
        try:
            return file_path.read_text(errors="replace")
        except OSError as exc:
            print(f"[docs] read failed for {file_path.name}: {exc}")
            return ""
    return ""


def save_doc(ticker: str, filename: str, content_bytes: bytes) -> dict:
    safe_name = secure_filename(filename)
    if not safe_name:
        raise ValueError("filename was empty after sanitisation")
    ext = Path(safe_name).suffix.lower()
    if ext not in ALLOWED_EXT:
        raise ValueError(f"unsupported file type {ext!r} (allowed: {sorted(ALLOWED_EXT)})")
    if len(content_bytes) > MAX_FILE_BYTES:
        raise ValueError(
            f"file too large: {len(content_bytes):,} bytes (cap {MAX_FILE_BYTES:,})"
        )

    tdir = _ticker_dir(ticker)
    src_path = tdir / safe_name
    src_path.write_bytes(content_bytes)

    text = _extract_text(src_path)
    (tdir / (safe_name + ".txt")).write_text(text)

    return {
        "filename": safe_name,
        "size": src_path.stat().st_size,
        "char_count": len(text),
        "uploaded": datetime.utcnow().isoformat() + "Z",
    }


def _is_cache_file(f: Path) -> bool:
    """A cache file is <original>.txt where <original> also exists in the
    directory (e.g. report.pdf.txt next to report.pdf). A bare upload of
    notes.txt is NOT a cache file even though it ends in .txt."""
    if f.suffix != ".txt":
        return False
    partner = f.with_suffix("")  # foo.pdf.txt -> foo.pdf
    return partner.is_file() and partner != f


def list_docs(ticker: str) -> list[dict]:
    try:
        tdir = _ticker_dir(ticker)
    except (ValueError, FileNotFoundError):
        return []
    out: list[dict] = []
    for f in tdir.iterdir():
        if not f.is_file() or f.name.startswith(".") or _is_cache_file(f):
            continue
        txt = tdir / (f.name + ".txt")
        char_count = len(txt.read_text(errors="replace")) if txt.exists() else 0
        out.append({
            "filename": f.name,
            "size": f.stat().st_size,
            "char_count": char_count,
            "uploaded": datetime.utcfromtimestamp(f.stat().st_mtime).isoformat() + "Z",
        })
    out.sort(key=lambda d: d["uploaded"], reverse=True)
    return out


def get_all_text(ticker: str, max_chars: int = 80_000) -> dict:
    """Concatenate every doc's text for a ticker, capped at max_chars. Used
    by the chat bubble — keeps the full library in context up to the cap,
    truncates the oldest beyond that and flags it."""
    docs = list_docs(ticker)
    parts: list[str] = []
    total = 0
    truncated = False
    for d in docs:
        txt_path = _ticker_dir(ticker) / (d["filename"] + ".txt")
        if not txt_path.exists():
            continue
        text = txt_path.read_text(errors="replace")
        if not text.strip():
            continue
        header = f"\n\n=== {d['filename']} ({d['char_count']:,} chars) ===\n"
        budget_left = max_chars - total - len(header)
        if budget_left <= 0:
            truncated = True
            break
        if len(text) > budget_left:
            parts.append(header + text[:budget_left] + "\n[truncated]")
            total = max_chars
            truncated = True
            break
        parts.append(header + text)
        total += len(header) + len(text)
    return {
        "text": "".join(parts),
        "doc_count": len(docs),
        "truncated": truncated,
        "total_chars": total,
    }


def get_doc_text(ticker: str, filename: str) -> str | None:
    """Return extracted text for one specific file, or None if not found.
    Used by the doc viewer modal."""
    safe = secure_filename(filename)
    if not safe:
        return None
    try:
        tdir = _ticker_dir(ticker)
    except ValueError:
        return None
    txt_path = tdir / (safe + ".txt")
    if not txt_path.exists():
        return None
    return txt_path.read_text(errors="replace")


def delete_doc(ticker: str, filename: str) -> bool:
    safe_name = secure_filename(filename)
    if not safe_name:
        raise ValueError("empty filename")
    tdir = _ticker_dir(ticker)
    removed = False
    for p in (tdir / safe_name, tdir / (safe_name + ".txt")):
        if p.exists():
            p.unlink()
            removed = True
    return removed
