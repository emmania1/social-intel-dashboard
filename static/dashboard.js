/* Social Intelligence Dashboard — client-side rendering. */
(function () {
  const $ = (sel) => document.querySelector(sel);
  const form = $("#inputs");
  const runBtn = $("#run");
  const results = $("#results");
  const statusEl = $("#status");

  const charts = {}; // keyed by canvas id

  // Default date range: last 3 years
  (function initDates() {
    const end = new Date();
    const start = new Date();
    start.setFullYear(end.getFullYear() - 3);
    $("#end").value = end.toISOString().slice(0, 10);
    $("#start").value = start.toISOString().slice(0, 10);
  })();

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
    setStatus("Fetching data — stock, trends, reddit, youtube (this can take 30-90s)...", "loading");
    results.classList.add("hidden");
    try {
      const res = await fetch("/api/generate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const rawText = await res.text();
      let data;
      try {
        data = JSON.parse(rawText);
      } catch (parseErr) {
        const preview = rawText.slice(0, 200).replace(/\n/g, " ");
        throw new Error(`Server returned non-JSON (HTTP ${res.status}): ${preview}`);
      }
      if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
      window.__lastData = data;
      render(data);
      setStatus("", "");
      results.classList.remove("hidden");
    } catch (e) {
      console.error(e);
      setStatus(`Error: ${e.message}`, "error");
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
    renderSummaryLine(data);
    maybeWarnLowSignal(data);
    renderMaster(data);
    renderIndividual("trendsChart", data.series.trends, "date", "value", "Google Trends", "#f3b84a");
    renderIndividual("redditChart", data.series.reddit, "date", "count", "Reddit posts/week", "#ff6b6b");
    renderYoutube(data.series.youtube);
    renderIndividual("stocktwitsChart", data.series.stocktwits, "date", "count", "StockTwits msgs/week", "#4ea1ff");
    renderIndividual("wikipediaChart", data.series.wikipedia, "date", "views", "Wikipedia pageviews/week", "#b489ff");
    renderIndividual("secChart", data.series.sec, "date", "count", "SEC filings/week", "#49c774");
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

  function renderSummaryLine(data) {
    const byName = Object.fromEntries(data.summaries.map(s => [s.metric, s]));
    const score = data.health_score;
    const socialPcts = data.summaries
      .filter(s => s.metric !== "Stock price" && s.pct_from_peak !== null)
      .map(s => s.pct_from_peak);
    const avgPct = socialPcts.length
      ? socialPcts.reduce((a, b) => a + b, 0) / socialPcts.length
      : null;

    // Majority trend direction across social metrics
    const trends = data.summaries.filter(s => s.metric !== "Stock price").map(s => s.trend_12w);
    const counts = trends.reduce((m, t) => ((m[t] = (m[t] || 0) + 1), m), {});
    const primary = Object.entries(counts).sort((a, b) => b[1] - a[1])[0]?.[0] || "unclear";

    $("#summaryLine").innerHTML =
      `Social metrics average <strong>${pctFmt(avgPct)}</strong> from peak. ` +
      `Trend direction (last 12 weeks): <strong>${primary}</strong>. ` +
      `Social Health Score: <strong>${score ?? "—"}/100</strong>.`;
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
    reddit: { label: "Reddit posts/week", series: "reddit", col: "count", color: "#ff6b6b" },
    youtube_views: { label: "YouTube views/week", series: "youtube", col: "views", color: "#49c774" },
    stocktwits: { label: "StockTwits msgs/week", series: "stocktwits", col: "count", color: "#4ea1ff" },
    wikipedia: { label: "Wikipedia views/week", series: "wikipedia", col: "views", color: "#b489ff" },
    sec: { label: "SEC filings/week", series: "sec", col: "count", color: "#49c774" },
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
    bindCsv("csv-aligned", data.aligned_weekly, `${data.inputs.ticker}_aligned_weekly.csv`);
  }
})();
