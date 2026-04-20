"""Analysis engine: peak detection, trend classification, social health score."""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any

import numpy as np
import pandas as pd


@dataclass
class MetricSummary:
    metric: str
    peak_date: str | None
    peak_value: float | None
    current_date: str | None
    current_value: float | None
    pct_from_peak: float | None  # negative = below peak
    trend_12w: str  # rising | flat | falling | insufficient-data
    slope_12w: float | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def classify_trend(values: list[float], threshold: float = 0.01) -> tuple[str, float | None]:
    """Linear regression slope on the last 12 observations, normalised by mean.

    Returns (label, normalised_slope). Labels: rising | flat | falling |
    insufficient-data.
    """
    if len(values) < 4:
        return "insufficient-data", None
    window = values[-12:]
    x = np.arange(len(window), dtype=float)
    y = np.asarray(window, dtype=float)
    if np.all(y == 0):
        return "flat", 0.0
    slope, _ = np.polyfit(x, y, 1)
    mean = float(np.mean(y)) if np.mean(y) != 0 else 1.0
    norm = float(slope / abs(mean))
    if norm > threshold:
        return "rising", norm
    if norm < -threshold:
        return "falling", norm
    return "flat", norm


def summarise_series(metric: str, df: pd.DataFrame, value_col: str) -> MetricSummary:
    """Compute peak, current, pct-from-peak, trend classification."""
    if df is None or df.empty or value_col not in df.columns:
        return MetricSummary(metric, None, None, None, None, None, "insufficient-data", None)
    df = df.dropna(subset=[value_col]).sort_values("date").reset_index(drop=True)
    if df.empty:
        return MetricSummary(metric, None, None, None, None, None, "insufficient-data", None)

    peak_idx = df[value_col].idxmax()
    peak_date = str(df.loc[peak_idx, "date"])
    peak_val = float(df.loc[peak_idx, value_col])

    current_date = str(df["date"].iloc[-1])
    current_val = float(df[value_col].iloc[-1])

    pct = None
    if peak_val > 0:
        pct = round(((current_val - peak_val) / peak_val) * 100.0, 2)

    label, slope = classify_trend(df[value_col].tolist())
    return MetricSummary(
        metric=metric,
        peak_date=peak_date,
        peak_value=round(peak_val, 4),
        current_date=current_date,
        current_value=round(current_val, 4),
        pct_from_peak=pct,
        trend_12w=label,
        slope_12w=round(slope, 6) if slope is not None else None,
    )


def social_health_score(summaries: list[MetricSummary], exclude: tuple[str, ...] = ("Stock price",)) -> float | None:
    """Average % distance from peak across social metrics (not stock).

    Score is expressed as a positive number where 100 = at peak, 0 = zero value.
    """
    pcts = [s.pct_from_peak for s in summaries if s.metric not in exclude and s.pct_from_peak is not None]
    if not pcts:
        return None
    # pct_from_peak is <= 0; convert so 0 decline => 100, -100 => 0
    score = 100.0 + float(np.mean(pcts))
    return round(max(0.0, min(100.0, score)), 1)


def align_weekly(frames: dict[str, tuple[pd.DataFrame, str]]) -> pd.DataFrame:
    """Align multiple (df, value_col) pairs onto a weekly index (W-SUN end).

    Returns a wide DataFrame indexed by date with one column per metric key.
    """
    aligned = None
    for key, (df, col) in frames.items():
        if df is None or df.empty:
            continue
        d = df[["date", col]].copy()
        d["date"] = pd.to_datetime(d["date"])
        d = d.set_index("date").resample("W-SUN").last()
        d.columns = [key]
        aligned = d if aligned is None else aligned.join(d, how="outer")
    if aligned is None:
        return pd.DataFrame()
    aligned = aligned.sort_index()
    aligned.index = aligned.index.strftime("%Y-%m-%d")
    out = aligned.reset_index().rename(columns={"index": "date"})
    # Replace NaN with None so the result is valid JSON (browsers reject NaN).
    return out.astype(object).where(out.notna(), None)
