/* Social Intelligence Dashboard — client-side rendering. */
(function () {
  const $ = (sel) => document.querySelector(sel);
  const form = $("#inputs");
  const runBtn = $("#run");
  const results = $("#results");
  const statusEl = $("#status");

  const charts = {}; // keyed by canvas id

  // Default date range: last 2 years (shorter = less chance of timeout on Render free tier)
  (function initDates() {
    const end = new Date();
    const start = new Date();
    start.setFullYear(end.getFullYear() - 2);
    $("#end").value = end.toISOString().slice(0, 10);
    $("#start").value = start.toISOString().slice(0, 10);
  })();

  // On page load, ping /api/health. If it takes more than 2s, we're probably
  // cold-starting — show a warming banner with a progress indicator so users
  // know the first generate will be slow but not broken.
  (async function prewarm() {
    const banner = $("#wakeup-banner");
    const bar = banner.querySelector(".wakeup-bar");
    const ctrl = new AbortController();
    const slowTimer = setTimeout(() => {
      banner.classList.remove("hidden");
      // Animate a fake progress bar up to 90% over 40s so it feels responsive
      let pct = 0;
      const tick = setInterval(() => {
        pct = Math.min(pct + 2, 90);
        bar.style.width = pct + "%";
        if (!banner.classList.contains("hidden") && pct >= 90) clearInterval(tick);
      }, 900);
      banner._tick = tick;
    }, 2000);
    try {
      const t0 = performance.now();
      await fetch("/api/health", { signal: ctrl.signal });
      const ms = Math.round(performance.now() - t0);
      console.log("[prewarm] health in", ms, "ms");
    } catch (e) {
      console.warn("[prewarm] health check failed", e);
    } finally {
      clearTimeout(slowTimer);
      if (banner._tick) clearInterval(banner._tick);
      bar.style.width = "100%";
      setTimeout(() => banner.classList.add("hidden"), 400);
    }
  })();

  // Fetch with timeout + one retry on network/abort errors. Returns Response.
  async function fetchWithRetry(url, options = {}, timeoutMs = 220000) {
    const attempt = async () => {
      const ctrl = new AbortController();
      const timer = setTimeout(() => ctrl.abort(), timeoutMs);
      try {
        return await fetch(url, { ...options, signal: ctrl.signal });
      } finally {
        clearTimeout(timer);
      }
    };
    try {
      return await attempt();
    } catch (e) {
      console.warn("fetch failed, retrying once:", e);
      return await attempt();
    }
  }

  // "Advanced" toggle
  $("#toggle-advanced").addEventListener("click", () => {
    $("#advanced").classList.toggle("hidden");
  });

  // Auto-resolve company name whenever the ticker field changes
  let resolveTimer = null;
  $("#ticker").addEventListener("input", () => {
    clearTimeout(resolveTimer);
    const t = $("#ticker").value.trim().toUpperCase();
    $("#resolved").textContent = "";
    if (!t) return;
    resolveTimer = setTimeout(async () => {
      try {
        const res = await fetch(`/api/resolve?ticker=${encodeURIComponent(t)}`);
        const info = await res.json();
        if (info.ok) {
          const holdingHints = /(international|holdings?|corporation|group|brands?|enterprises)/i;
          const looksLikeHolding = holdingHints.test(info.name);
          $("#resolved").innerHTML = looksLikeHolding
            ? `→ ${info.name} <span style="color:var(--warn)">⚠ looks like a holding/parent — open Advanced and type the consumer brand (e.g. "Chili's") for better demand signal</span>`
            : `→ ${info.name}`;
          if (!$("#company").value) $("#company").placeholder = info.name;
        } else {
          $("#resolved").textContent = "ticker not found (you can still try)";
        }
      } catch (e) { /* ignore */ }
    }, 350);
  });

  form.addEventListener("submit", async (ev) => {
    ev.preventDefault();
    const payload = {
      ticker: $("#ticker").value.trim(),
      company: $("#company").value.trim(),
      custom_term: $("#custom_term").value.trim(),
      start: $("#start").value,
      end: $("#end").value,
    };
    if (!payload.ticker) {
      setStatus("Ticker is required.", "error");
      return;
    }
    runBtn.disabled = true;
    setStatus("Fetching 7 data sources in parallel — typically 30–90s...", "loading");
    results.classList.add("hidden");
    const t0 = performance.now();
    try {
      const res = await fetchWithRetry("/api/generate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const elapsed = Math.round((performance.now() - t0) / 1000);
      const rawText = await res.text();
      let data;
      try {
        data = JSON.parse(rawText);
      } catch (parseErr) {
        const preview = rawText.slice(0, 200).replace(/\n/g, " ");
        throw new Error(`Server returned non-JSON (HTTP ${res.status} after ${elapsed}s): ${preview}`);
      }
      if (!res.ok) throw new Error(data.error || `HTTP ${res.status} after ${elapsed}s`);
      window.__lastData = data;
      render(data);
      setStatus("", "");
      results.classList.remove("hidden");
    } catch (e) {
      console.error(e);
      const elapsed = Math.round((performance.now() - t0) / 1000);
      let friendly = e.message || String(e);
      if (friendly.includes("aborted") || friendly.includes("timed out")) {
        friendly = `Request timed out after ${elapsed}s. Render's free tier has a ~100s request limit — try a shorter date range (e.g. 1 year) in Advanced, or try again in a few seconds.`;
      } else if (friendly.includes("Failed to fetch") || friendly.includes("NetworkError")) {
        friendly = `Network error after ${elapsed}s. The service may have gone back to sleep — click Generate again, it'll wake up.`;
      }
      setStatus(`Error: ${friendly}`, "error");
    } finally {
      runBtn.disabled = false;
    }
  });

  function setStatus(msg, cls) {
    statusEl.textContent = msg;
    statusEl.className = cls || "";
  }

  function fmt(n) {
    if (n === null || n === undefined || Number.isNaN(n)) return "—";
    if (Math.abs(n) >= 1_000_000) return (n / 1_000_000).toFixed(2) + "M";
    if (Math.abs(n) >= 1_000) return (n / 1_000).toFixed(1) + "K";
    if (Math.abs(n) < 10) return Number(n).toFixed(2);
    return Math.round(n).toLocaleString();
  }

  function pctFmt(n) {
    if (n === null || n === undefined) return "—";
    const sign = n > 0 ? "+" : "";
    return `${sign}${n.toFixed(1)}%`;
  }

  function render(data) {
    renderNarrative(data);
    renderKPIs(data);
    renderSummaryTable(data.summaries);
    maybeWarnLowSignal(data);
    renderMaster(data);
    renderIndividual("trendsChart", data.series.trends, "date", "value", "Google Trends", "#f3b84a");
    renderIndividual("redditChart", data.series.reddit, "date", "count", "Reddit posts/week", "#ff6b6b");
    renderYoutube(data.series.youtube);
    renderIndividual("stocktwitsChart", data.series.stocktwits, "date", "count", "StockTwits msgs/week", "#4ea1ff");
    renderIndividual("wikipediaChart", data.series.wikipedia, "date", "views", "Wikipedia pageviews/week", "#b489ff");
    renderIndividual("secChart", data.series.sec, "date", "count", "SEC filings/week", "#49c774");
    renderIndividual("newsChart", data.series.news, "date", "count", "News articles/week", "#f37a4a");
    const nsrc = $("#news-source");
    if (nsrc) nsrc.textContent = `source: ${data.inputs.news_source || "unknown"}`;
    renderSentiment(data.series.stocktwits);
    // Show which queries were used
    const rq = $("#reddit-queries");
    if (rq) rq.textContent = (data.inputs.reddit_queries || []).join(", ");
    const subs = $("#subreddits-searched");
    if (subs) {
      const all = data.inputs.subreddits_searched || [];
      const discovered = (data.inputs.discovered_subreddits || []).map(d => d.name.toLowerCase());
      subs.innerHTML = all.map(s => {
        const auto = discovered.includes(s.toLowerCase());
        return auto
          ? `<span style="color:var(--good)">r/${s}</span>`
          : `r/${s}`;
      }).join(", ");
    }
    const yq = $("#youtube-queries");
    if (yq) yq.textContent = (data.inputs.youtube_queries || []).join(", ");
    const sym = $("#st-symbol");
    if (sym) sym.textContent = data.inputs.ticker;
    const wt = $("#wiki-title");
    if (wt) wt.textContent = data.inputs.wikipedia_title || "(not found)";
    wireExports(data);
    loadSnapshots(data.inputs.ticker);
  }

  function renderKPIs(data) {
    const summaries = data.summaries;
    const byName = Object.fromEntries(summaries.map(s => [s.metric, s]));
    const setKPI = (id, label, valTxt, subTxt) => {
      const el = document.getElementById(id);
      el.querySelector(".label").textContent = label;
      el.querySelector(".value").textContent = valTxt;
      el.querySelector(".sub").textContent = subTxt;
    };
    const score = data.health_score;
    setKPI("kpi-score", "Social Health Score",
      score === null ? "—" : `${score}/100`,
      score === null ? "insufficient data"
        : score >= 70 ? "strong"
        : score >= 40 ? "mixed"
        : "weak");

    const stock = byName["Stock price"] || {};
    setKPI("kpi-stock", "Stock",
      stock.current_value !== null ? `$${fmt(stock.current_value)}` : "—",
      `peak ${stock.peak_date || "—"} • ${pctFmt(stock.pct_from_peak)}`);

    const trends = byName["Google Trends"] || {};
    setKPI("kpi-trends", "Google Trends",
      trends.current_value !== null ? fmt(trends.current_value) : "—",
      `peak ${trends.peak_date || "—"} • ${pctFmt(trends.pct_from_peak)}`);

    const reddit = byName["Reddit posts/wk"] || {};
    setKPI("kpi-reddit", "Reddit posts/wk",
      reddit.current_value !== null ? fmt(reddit.current_value) : "—",
      `peak ${reddit.peak_date || "—"} • ${pctFmt(reddit.pct_from_peak)}`);
  }

  function renderSummaryTable(summaries) {
    const tbody = $("#summaryTable tbody");
    tbody.innerHTML = "";
    summaries.forEach(s => {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${s.metric}</td>
        <td>${s.peak_date || "—"}</td>
        <td class="num">${fmt(s.peak_value)}</td>
        <td>${s.current_date || "—"}</td>
        <td class="num">${fmt(s.current_value)}</td>
        <td class="num">${pctFmt(s.pct_from_peak)}</td>
        <td><span class="pill ${s.trend_12w}">${s.trend_12w}</span></td>
      `;
      tbody.appendChild(tr);
    });
  }

  function renderNarrative(data) {
    const n = data.narrative || {};
    const head = $("#narrative-headline");
    const para = $("#narrative-paragraph");
    if (!head || !para) return;
    head.textContent = n.headline || "";
    para.textContent = n.paragraph || "";
    // Color the left border to reflect direction
    const card = $("#narrative-card");
    if (card) {
      const colorMap = { declining: "var(--bad)", rising: "var(--good)", mixed: "var(--warn)", flat: "var(--muted)", unclear: "var(--muted)" };
      card.style.borderLeftColor = colorMap[n.direction] || "var(--accent)";
    }
  }

  function maybeWarnLowSignal(data) {
    const redditRows = (data.series.reddit || []).length;
    const ytRows = (data.series.youtube || []).length;
    const el = $("#low-signal");
    if (!el) return;
    if (redditRows < 10 && ytRows < 10) {
      el.classList.remove("hidden");
      el.textContent =
        `Reddit & YouTube returned almost nothing for "${data.inputs.search_term}". ` +
        `This usually means the company name resolved to a holding entity. ` +
        `Open Advanced and set "Company name" to the consumer brand (e.g. "Chili's" instead of "Brinker International"), then regenerate.`;
    } else {
      el.classList.add("hidden");
    }
  }

  function dataset(label, points, color, yAxisID = "y") {
    return {
      label,
      data: points.map(p => ({ x: p.x, y: p.y })),
      borderColor: color,
      backgroundColor: color + "33",
      borderWidth: 1.8,
      pointRadius: 0,
      pointHoverRadius: 4,
      tension: 0.2,
      yAxisID,
      spanGaps: true,
    };
  }

  // Map hero key → human label, series path, value column, color
  const HERO_CONFIG = {
    trends: { label: "Google Trends (0-100)", series: "trends", col: "value", color: "#f3b84a" },
    reddit: { label: "Reddit mentions/week", series: "reddit", col: "count", color: "#ff6b6b" },
    youtube_views: { label: "YouTube views/week", series: "youtube", col: "views", color: "#49c774" },
    stocktwits: { label: "StockTwits msgs/week", series: "stocktwits", col: "count", color: "#4ea1ff" },
    wikipedia: { label: "Wikipedia views/week", series: "wikipedia", col: "views", color: "#b489ff" },
    sec: { label: "SEC filings/week", series: "sec", col: "count", color: "#49c774" },
    news: { label: "News articles/week", series: "news", col: "count", color: "#f37a4a" },
  };

  function renderMaster(data) {
    const card = $("#master-card");
    const title = $("#master-title");
    const hero = data.hero;
    if (!hero || !hero.key || !HERO_CONFIG[hero.key]) {
      // No signal rich enough — hide the master chart entirely
      if (card) card.classList.add("hidden");
      return;
    }
    if (card) card.classList.remove("hidden");
    const cfg = HERO_CONFIG[hero.key];
    if (title) title.textContent = `Master timeline — Stock × ${cfg.label}`;

    const stock = (data.series.stock || []).map(r => ({ x: r.date, y: r.close }));
    const heroPts = (data.series[cfg.series] || []).map(r => ({ x: r.date, y: r[cfg.col] }));
    mkChart("masterChart", {
      type: "line",
      data: {
        datasets: [
          dataset("Stock price", stock, "#4ea1ff", "y"),
          dataset(cfg.label, heroPts, cfg.color, "y2"),
        ],
      },
      options: masterOptions("Stock ($)", cfg.label),
    });
  }

  function renderIndividual(id, rows, xKey, yKey, label, color) {
    const canvas = document.getElementById(id);
    if (!canvas) return;
    const card = canvas.closest(".card");
    const rowsArr = rows || [];
    const hasData = rowsArr.length > 0 && rowsArr.some(r => Number(r[yKey]) > 0);
    if (!hasData) {
      if (card) card.classList.add("hidden");
      return;
    }
    if (card) card.classList.remove("hidden");
    const points = rowsArr.map(r => ({ x: r[xKey], y: r[yKey] }));
    mkChart(id, {
      type: "line",
      data: { datasets: [dataset(label, points, color)] },
      options: singleAxisOptions(label),
    });
  }

  function renderSentiment(rows) {
    const canvas = document.getElementById("sentimentChart");
    const card = canvas ? canvas.closest(".card") : null;
    const rowsArr = rows || [];
    // Only render if we actually have bullish/bearish-tagged messages
    const tagged = rowsArr.filter(r => (Number(r.bullish) || 0) + (Number(r.bearish) || 0) > 0);
    if (tagged.length < 4) {
      if (card) card.classList.add("hidden");
      return;
    }
    if (card) card.classList.remove("hidden");
    const ratio = tagged.map(r => ({ x: r.date, y: r.bullish_ratio }));
    const vol = tagged.map(r => ({ x: r.date, y: (Number(r.bullish) || 0) + (Number(r.bearish) || 0) }));
    mkChart("sentimentChart", {
      type: "line",
      data: {
        datasets: [
          { ...dataset("Bullish ratio (0-1)", ratio, "#49c774", "y"), fill: false },
          { ...dataset("Tagged messages/wk", vol, "#8b93a7", "y2"), borderDash: [3, 3] },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
        plugins: { legend: { labels: { color: "#e7ecf3" } } },
        scales: {
          ...baseScales("Bullish ratio"),
          y: { ...baseScales("Bullish ratio").y, min: 0, max: 1 },
          y2: {
            position: "right", grid: { drawOnChartArea: false },
            ticks: { color: "#8b93a7" },
            title: { display: true, text: "Tagged volume", color: "#8b93a7" },
          },
        },
      },
    });
  }

  function renderYoutube(rows) {
    const canvas = document.getElementById("youtubeChart");
    const card = canvas ? canvas.closest(".card") : null;
    const rowsArr = rows || [];
    const hasData = rowsArr.length > 0 && rowsArr.some(r => Number(r.views) > 0);
    if (!hasData) {
      if (card) card.classList.add("hidden");
      return;
    }
    if (card) card.classList.remove("hidden");
    const views = rowsArr.map(r => ({ x: r.date, y: r.views }));
    const videos = rowsArr.map(r => ({ x: r.date, y: r.videos }));
    mkChart("youtubeChart", {
      type: "line",
      data: {
        datasets: [
          dataset("Views (sum/week)", views, "#49c774", "y"),
          dataset("Videos published/week", videos, "#b489ff", "y2"),
        ],
      },
      options: masterOptions("Views", "Videos"),
    });
  }

  function mkChart(id, config) {
    const ctx = document.getElementById(id);
    if (!ctx) return;
    if (charts[id]) charts[id].destroy();
    charts[id] = new Chart(ctx, config);
  }

  function baseScales(leftTitle = "") {
    return {
      x: {
        type: "time",
        time: { unit: "month" },
        grid: { color: "#262a35" },
        ticks: { color: "#8b93a7", maxRotation: 0 },
      },
      y: {
        position: "left",
        grid: { color: "#262a35" },
        ticks: { color: "#8b93a7" },
        title: { display: !!leftTitle, text: leftTitle, color: "#8b93a7" },
      },
    };
  }

  function masterOptions(leftTitle = "Stock ($)", rightTitle = "Trends") {
    return {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      plugins: { legend: { labels: { color: "#e7ecf3" } } },
      scales: {
        ...baseScales(leftTitle),
        y2: {
          position: "right",
          grid: { drawOnChartArea: false },
          ticks: { color: "#8b93a7" },
          title: { display: true, text: rightTitle, color: "#8b93a7" },
        },
      },
    };
  }

  function singleAxisOptions(leftTitle) {
    return {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      plugins: { legend: { labels: { color: "#e7ecf3" } } },
      scales: baseScales(leftTitle),
    };
  }

  async function loadSnapshots(ticker) {
    const empty = $("#snapshots-empty");
    const tbl = $("#snapshots-table");
    const tbody = tbl.querySelector("tbody");
    tbody.innerHTML = "";
    try {
      const res = await fetch(`/api/snapshots?ticker=${encodeURIComponent(ticker)}`);
      const info = await res.json();
      const rows = info.snapshots || [];
      if (!rows.length) { empty.classList.remove("hidden"); tbl.classList.add("hidden"); return; }
      empty.classList.add("hidden");
      tbl.classList.remove("hidden");
      rows.forEach(r => {
        const tr = document.createElement("tr");
        const sizeKB = Math.round(r.size_bytes / 102.4) / 10;
        tr.innerHTML = `
          <td>${r.captured_at}</td>
          <td>${r.company || "—"}</td>
          <td class="num">${r.health_score ?? "—"}</td>
          <td class="num">${sizeKB} KB</td>
          <td><button class="secondary" data-fn="${r.filename}">Load</button></td>
        `;
        tbody.appendChild(tr);
      });
      tbody.querySelectorAll("button[data-fn]").forEach(b => {
        b.onclick = async () => {
          const fn = b.getAttribute("data-fn");
          const r = await fetch(`/api/snapshots/${encodeURIComponent(ticker)}/${encodeURIComponent(fn)}`);
          if (!r.ok) { alert("load failed"); return; }
          const data = await r.json();
          window.__lastData = data;
          render(data);
          setStatus(`Loaded snapshot from ${fn}`, "");
        };
      });
    } catch (e) { console.error(e); }
  }

  function wireExports(data) {
    const bindPng = (btnId, chartId, filename) => {
      const b = document.getElementById(btnId);
      if (!b) return;
      b.onclick = () => {
        const chart = charts[chartId];
        if (!chart) return;
        const url = chart.toBase64Image("image/png", 1);
        const a = document.createElement("a");
        a.href = url; a.download = filename; a.click();
      };
    };
    bindPng("dl-master", "masterChart", `${data.inputs.ticker}_master.png`);
    bindPng("dl-trends", "trendsChart", `${data.inputs.ticker}_trends.png`);
    bindPng("dl-reddit", "redditChart", `${data.inputs.ticker}_reddit.png`);
    bindPng("dl-youtube", "youtubeChart", `${data.inputs.ticker}_youtube.png`);
    bindPng("dl-stocktwits", "stocktwitsChart", `${data.inputs.ticker}_stocktwits.png`);
    bindPng("dl-wikipedia", "wikipediaChart", `${data.inputs.ticker}_wikipedia.png`);
    bindPng("dl-sec", "secChart", `${data.inputs.ticker}_sec.png`);
    bindPng("dl-news", "newsChart", `${data.inputs.ticker}_news.png`);
    bindPng("dl-sentiment", "sentimentChart", `${data.inputs.ticker}_sentiment.png`);

    const bindCsv = (btnId, rows, filename) => {
      const b = document.getElementById(btnId);
      if (!b) return;
      b.onclick = async () => {
        const res = await fetch("/api/export.csv", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ rows, filename }),
        });
        if (!res.ok) { alert("export failed"); return; }
        const blob = await res.blob();
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url; a.download = filename; a.click();
        URL.revokeObjectURL(url);
      };
    };
    bindCsv("csv-stock", data.series.stock, `${data.inputs.ticker}_stock.csv`);
    bindCsv("csv-trends", data.series.trends, `${data.inputs.ticker}_trends.csv`);
    bindCsv("csv-reddit", data.series.reddit, `${data.inputs.ticker}_reddit.csv`);
    bindCsv("csv-youtube", data.series.youtube, `${data.inputs.ticker}_youtube.csv`);
    bindCsv("csv-stocktwits", data.series.stocktwits, `${data.inputs.ticker}_stocktwits.csv`);
    bindCsv("csv-wikipedia", data.series.wikipedia, `${data.inputs.ticker}_wikipedia.csv`);
    bindCsv("csv-sec", data.series.sec, `${data.inputs.ticker}_sec.csv`);
    bindCsv("csv-news", data.series.news, `${data.inputs.ticker}_news.csv`);
    bindCsv("csv-sentiment", data.series.stocktwits, `${data.inputs.ticker}_sentiment.csv`);
    bindCsv("csv-aligned", data.aligned_weekly, `${data.inputs.ticker}_aligned_weekly.csv`);

    // Snapshot download: save entire response as JSON to user's machine
    const dlSnap = document.getElementById("dl-snapshot");
    if (dlSnap) {
      dlSnap.onclick = () => {
        const blob = new Blob([JSON.stringify(data, null, 2)], { type: "application/json" });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        const ts = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
        a.href = url;
        a.download = `${data.inputs.ticker}_snapshot_${ts}.json`;
        a.click();
        URL.revokeObjectURL(url);
      };
    }
    // Snapshot upload: reload a previously downloaded JSON
    const upBtn = document.getElementById("upload-snapshot-btn");
    const upInput = document.getElementById("upload-snapshot-input");
    if (upBtn && upInput) {
      upBtn.onclick = () => upInput.click();
      upInput.onchange = async () => {
        const file = upInput.files && upInput.files[0];
        if (!file) return;
        try {
          const text = await file.text();
          const loaded = JSON.parse(text);
          if (!loaded || !loaded.series) throw new Error("Not a valid snapshot file");
          window.__lastData = loaded;
          render(loaded);
          setStatus(`Loaded snapshot: ${file.name}`, "");
          results.classList.remove("hidden");
        } catch (e) {
          setStatus(`Upload failed: ${e.message}`, "error");
        } finally {
          upInput.value = "";
        }
      };
    }
  }
})();
