# Social Intelligence Dashboard

Single-page Flask tool that pulls cross-platform demand signals for **any public
company** and compares them against the stock price on one timeline.

## Data sources
- **Stock price** — `yfinance` daily close (auto-adjusted)
- **Google Trends** — `pytrends` weekly interest (0–100)
- **Reddit** — Arctic Shift archive (`arctic-shift.photon-reddit.com`) for
  `r/stocks`, `r/investing`, `r/wallstreetbets` + optional company subreddit,
  weekly post counts matching the query in the title
- **YouTube** — Data API v3, top 50 most-viewed videos per month matching the
  query, bucketed to weekly video counts and view sums

## Analysis
- Peak detection (date + value) per metric
- % vs peak for current value
- 12-week trend classification via OLS slope (rising / flat / falling)
- **Social health score** = 100 + average(%-from-peak across non-stock metrics),
  clamped to [0, 100]. 100 means every social metric is at its peak.

## Setup

```bash
cd ~/Desktop/social_intel_dashboard
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# edit .env — set YOUTUBE_API_KEY at minimum
```

## Run

```bash
source venv/bin/activate
python app.py
# → http://127.0.0.1:5050
```

Enter a ticker (e.g. `EAT`), company name (e.g. `Chili's`), optional custom
search term (e.g. `Triple Dipper`), and optional subreddit (e.g. `chilis`).
Date range defaults to the last 3 years.

A full run typically takes 30–90s depending on history length (YouTube is the
slowest — one `search.list` call per month of history).

## Exports
- **PNG** per chart (master, Trends, Reddit, YouTube) — client-side
  `chart.toBase64Image()`
- **CSV** per series and a combined weekly-aligned CSV — server-side
  `/api/export.csv`

## File map
```
app.py                  Flask routes (/ + /api/generate + /api/export.csv)
lib/stock.py            yfinance wrapper
lib/trends.py           pytrends wrapper
lib/reddit.py           Arctic Shift weekly counts
lib/youtube.py          YouTube Data API v3 weekly aggregates
lib/analysis.py         peak detection, trend slope, health score, alignment
templates/index.html    single-page UI
static/style.css        dark theme
static/dashboard.js     fetch, render, exports (Chart.js 4 + moment adapter)
```

## Known limitations
- YouTube `search.list` is relevance-ordered and capped at 500 results per
  query — we paginate monthly and take the top 50 per month to reduce bias,
  but this is not a firehose.
- Reddit Arctic Shift title-only matching is intentional (avoids body-text
  false positives like "where do I store my winning streak"). Recall is lower
  than broader search.
- Google Trends is anonymised; very low-volume terms return zeros.
- pytrends requires `urllib3<2` (pinned in requirements).

## Reference
- Analogous static-HTML dashboards: `~/Desktop/warhammer_demand/` and
  `~/Desktop/crocs_demand/` (Python-generator pattern; this tool is the
  live/interactive equivalent).
