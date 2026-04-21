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


def signal_quality(df: pd.DataFrame, col: str) -> float:
    """How informative is this series? Higher = richer signal.

    Combines density (non-zero rows) and dynamic range (peak/mean ratio). A
    series with 50 rows where most are ~1 scores lower than 50 rows with a
    clear peak of 1000 and a mean of 50.
    """
    if df is None or df.empty or col not in df.columns:
        return 0.0
    values = pd.to_numeric(df[col], errors="coerce").dropna()
    if values.empty:
        return 0.0
    nonzero = (values > 0).sum()
    if nonzero < 4:
        return 0.0
    mean = float(values.mean()) or 1.0
    peak = float(values.max())
    range_score = min(peak / mean, 10.0)  # cap so crazy outliers don't dominate
    return float(nonzero) * range_score


def pick_hero_signal(
    series: dict[str, pd.DataFrame],
    columns: dict[str, str],
) -> tuple[str, str] | None:
    """Pick the single best non-stock source to pair with stock on the main chart.

    Returns (source_key, value_col) or None if nothing qualifies.
    """
    candidates = [
        k for k in series
        if k != "stock" and not series[k].empty
    ]
    if not candidates:
        return None
    scored = [(k, signal_quality(series[k], columns[k])) for k in candidates]
    scored = [(k, s) for k, s in scored if s > 0]
    if not scored:
        return None
    scored.sort(key=lambda x: x[1], reverse=True)
    best = scored[0][0]
    return best, columns[best]


_LABELS = {
    "Stock price": "stock price",
    "Google Trends": "Google search interest",
    "Reddit posts/wk": "Reddit chatter",
    "YouTube views/wk": "YouTube views",
    "YouTube videos/wk": "YouTube publishing",
    "StockTwits msgs/wk": "StockTwits chatter",
    "Wikipedia views/wk": "Wikipedia pageviews",
    "SEC filings/wk": "SEC filing activity",
}


def build_narrative(
    summaries: list[MetricSummary],
    hero_key: str | None,
    series: dict[str, pd.DataFrame] | None = None,
    columns: dict[str, str] | None = None,
) -> dict:
    """Human-readable synthesis of what the data shows.

    Only counts a source as "strong" if it has meaningful density — at least
    4 weeks of non-zero data AND a signal_quality score above a noise floor.
    This prevents a single pageview or a bit of pytrends noise on a garbage
    query from being declared as a "demand signal".

    Returns dict with:
      headline, paragraph, strong (metric names), weak (metric names),
      direction ("declining" | "rising" | "mixed" | "flat" | "unclear")
    """
    # Map metric → (series_key, value_col) for quality lookup
    series = series or {}
    columns = columns or {}
    metric_to_series = {
        "Google Trends": ("trends", "value"),
        "Reddit posts/wk": ("reddit", "count"),
        "YouTube views/wk": ("youtube", "views"),
        "YouTube videos/wk": ("youtube", "videos"),
        "StockTwits msgs/wk": ("stocktwits", "count"),
        "Wikipedia views/wk": ("wikipedia", "views"),
        "SEC filings/wk": ("sec", "count"),
    }
    QUALITY_FLOOR = 10.0  # empirical: noise/near-empty series score < ~5

    strong, weak = [], []
    pct_changes = []
    trends = []
    for s in summaries:
        if s.metric == "Stock price":
            continue
        if s.peak_value is None or s.current_value is None:
            weak.append(s.metric)
            continue
        # Signal-quality gate — rejects thin/noisy series from "strong"
        skey = metric_to_series.get(s.metric)
        if skey and skey[0] in series:
            q = signal_quality(series[skey[0]], skey[1])
            if q < QUALITY_FLOOR:
                weak.append(s.metric)
                continue
        strong.append(s)
        if s.pct_from_peak is not None:
            pct_changes.append(s.pct_from_peak)
        trends.append(s.trend_12w)

    if not strong:
        return {
            "headline": "Not enough demand signal to draw conclusions.",
            "paragraph": (
                "All social, news, and filings sources returned sparse or noise-level data for "
                "this ticker. This usually means the company is too obscure, newly listed, "
                "delisted, or the ticker doesn't exist. If you typed a real brand, try overriding "
                "the company name in Advanced — small holding-co tickers often need a manual override."
            ),
            "strong": [],
            "weak": weak,
            "direction": "unclear",
        }

    if len(strong) == 1:
        # Only one signal above the noise floor — acknowledge the uncertainty
        s = strong[0]
        name = _LABELS.get(s.metric, s.metric)
        return {
            "headline": f"Only one substantial signal ({name}) — interpret with caution.",
            "paragraph": (
                f"{name} peaked on {s.peak_date} at {_fmt(s.peak_value)} and is now "
                f"{_fmt(s.current_value)} ({s.pct_from_peak:+.0f}%). Every other source "
                "returned sparse/noise-level data, so treat this as a single-source read rather "
                "than cross-platform consensus."
            ),
            "strong": [s.metric],
            "weak": weak,
            "direction": s.trend_12w if s.trend_12w != "insufficient-data" else "unclear",
        }

    avg_pct = sum(pct_changes) / len(pct_changes) if pct_changes else None
    rising = trends.count("rising")
    falling = trends.count("falling")
    flat = trends.count("flat")

    if rising > falling and rising > flat:
        direction = "rising"
    elif falling > rising and falling > flat:
        direction = "declining"
    elif rising and falling and abs(rising - falling) <= 1:
        direction = "mixed"
    else:
        direction = "flat"

    stock_s = next((s for s in summaries if s.metric == "Stock price"), None)
    stock_pct = stock_s.pct_from_peak if stock_s else None

    # Headline
    avg_txt = f"{avg_pct:+.0f}%" if avg_pct is not None else "unclear"
    strong_names = [_LABELS.get(s.metric, s.metric) for s in strong]
    if len(strong_names) <= 2:
        names_txt = " and ".join(strong_names)
    else:
        names_txt = ", ".join(strong_names[:-1]) + f", and {strong_names[-1]}"

    if direction == "declining":
        headline = f"Demand signals across {len(strong)} sources are trending down — on average {avg_txt} from peak."
    elif direction == "rising":
        headline = f"Demand signals across {len(strong)} sources are rebounding — averaging {avg_txt} from peak with upward 12-week trends."
    elif direction == "mixed":
        headline = f"Mixed picture: {rising} source(s) rising, {falling} falling across {len(strong)} usable signals."
    else:
        headline = f"{len(strong)} usable signals, mostly flat — averaging {avg_txt} from peak."

    # Body paragraph: name the strongest sources, quote specific peaks
    strong_sorted = sorted(strong, key=lambda s: abs(s.pct_from_peak or 0), reverse=True)
    top = strong_sorted[0]
    top_name = _LABELS.get(top.metric, top.metric)
    body_parts = [
        f"The most informative source is {top_name}: peaked on {top.peak_date} at {_fmt(top.peak_value)} and is now {_fmt(top.current_value)} ({top.pct_from_peak:+.0f}%)."
    ]
    if stock_s and stock_pct is not None:
        body_parts.append(
            f"Stock is {stock_pct:+.0f}% vs its {stock_s.peak_date} peak of ${stock_s.peak_value}."
        )
    if weak:
        weak_labels = [_LABELS.get(m, m) for m in weak]
        body_parts.append(
            f"Low-signal sources for this ticker: {', '.join(weak_labels)}."
        )
    paragraph = " ".join(body_parts)

    return {
        "headline": headline,
        "paragraph": paragraph,
        "strong": [s.metric for s in strong],
        "weak": weak,
        "direction": direction,
    }


def _fmt(n) -> str:
    if n is None:
        return "—"
    if isinstance(n, (int, float)):
        if abs(n) >= 1_000_000:
            return f"{n/1_000_000:.2f}M"
        if abs(n) >= 1_000:
            return f"{n/1_000:.1f}K"
        if abs(n) < 10:
            return f"{n:.2f}"
        return f"{int(round(n)):,}"
    return str(n)


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
