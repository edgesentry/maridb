/**
 * indago metrics dashboard
 *
 * Data flow:
 *   1. Fetch metrics/index.json from R2
 *   2. For each entry: check OPFS cache (< 24h) → else fetch from R2
 *   3. Render time-series charts with Observable Plot (CDN)
 *   4. Render summary table
 */

import * as Plot from "https://cdn.jsdelivr.net/npm/@observablehq/plot@0.6/+esm";

const R2_BASE = "https://pub-e088008b61ee432b906ef710d52af28c.r2.dev";
const INDEX_URL = `${R2_BASE}/metrics/index.json`;
const OPFS_CACHE_TTL_MS = 24 * 60 * 60 * 1000; // 24 hours

// ---------------------------------------------------------------------------
// OPFS helpers
// ---------------------------------------------------------------------------

async function opfsGet(key) {
  try {
    const root = await navigator.storage.getDirectory();
    const dir = await root.getDirectoryHandle("indago-metrics", { create: true });
    const file = await dir.getFileHandle(key);
    const f = await file.getFile();
    const text = await f.text();
    const { ts, data } = JSON.parse(text);
    if (Date.now() - ts < OPFS_CACHE_TTL_MS) return data;
  } catch (_) { /* cache miss */ }
  return null;
}

async function opfsSet(key, data) {
  try {
    const root = await navigator.storage.getDirectory();
    const dir = await root.getDirectoryHandle("indago-metrics", { create: true });
    const file = await dir.getFileHandle(key, { create: true });
    const writable = await file.createWritable();
    await writable.write(JSON.stringify({ ts: Date.now(), data }));
    await writable.close();
  } catch (_) { /* best-effort */ }
}

// ---------------------------------------------------------------------------
// Fetch with OPFS cache
// ---------------------------------------------------------------------------

async function fetchWithCache(url, cacheKey) {
  const cached = await opfsGet(cacheKey);
  if (cached) return cached;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status}: ${url}`);
  const data = await res.json();
  await opfsSet(cacheKey, data);
  return data;
}

// ---------------------------------------------------------------------------
// Chart helpers
// ---------------------------------------------------------------------------

function sparkline(snapshots, field, { color = "#3fb950", label = field, fmt = (v) => v.toFixed(3) } = {}) {
  const data = snapshots
    .filter((s) => s[field] != null)
    .map((s) => ({ date: new Date(s.date), value: +s[field] }));

  if (data.length === 0) return null;

  return Plot.plot({
    width: 320,
    height: 100,
    marginLeft: 40,
    marginRight: 8,
    marginTop: 8,
    marginBottom: 24,
    style: { background: "transparent", color: "#8b949e", fontSize: "11px" },
    x: { type: "time", label: null, tickFormat: "%m/%d" },
    y: { label: null, tickFormat: fmt, ticks: 3 },
    marks: [
      Plot.lineY(data, { x: "date", y: "value", stroke: color, strokeWidth: 2 }),
      Plot.dotY(data, { x: "date", y: "value", fill: color, r: 3 }),
    ],
  });
}

// ---------------------------------------------------------------------------
// DOM helpers
// ---------------------------------------------------------------------------

function setStatus(msg, isError = false) {
  const el = document.getElementById("status");
  el.textContent = msg;
  el.className = isError ? "error" : "";
}

function setMetric(id, value, fmt = (v) => v) {
  const el = document.getElementById(id);
  if (el) el.textContent = value != null ? fmt(value) : "—";
}

function colorClass(value, { floor, ceiling } = {}) {
  if (value == null) return "";
  if (floor != null && value < floor) return "bad";
  if (ceiling != null && value > ceiling) return "warn";
  return "good";
}

function renderTable(snapshots) {
  const tbody = document.getElementById("history-body");
  tbody.innerHTML = "";
  for (const s of snapshots) {
    const p50 = s.precision_at_50;
    const recall = s.recall_at_200;
    const lead = s.median_lead_days;
    const uu = s.unknown_unknown_candidates;
    const known = s.known_positives;

    const row = document.createElement("tr");
    row.innerHTML = `
      <td>${s.date}</td>
      <td class="${colorClass(p50, { floor: 0.25, ceiling: 0.95 })}">${p50 != null ? p50.toFixed(4) : "—"}</td>
      <td class="${colorClass(recall, { floor: 0.5 })}">${recall != null ? recall.toFixed(4) : "—"}</td>
      <td>${lead != null ? lead + "d" : "—"}</td>
      <td>${uu != null ? uu : "—"}</td>
      <td>${known != null ? known : "—"}</td>
      <td style="color:#484f58;font-size:0.7rem">${(s.regions || []).join(", ") || "—"}</td>
    `;
    tbody.appendChild(row);
  }
}

function mountChart(containerId, chart) {
  const el = document.getElementById(containerId);
  if (!el || !chart) return;
  el.innerHTML = "";
  el.appendChild(chart);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  setStatus("Fetching index from R2…");

  let index;
  try {
    index = await fetchWithCache(INDEX_URL, "index.json");
  } catch (err) {
    setStatus(`Failed to load index: ${err.message}`, true);
    return;
  }

  const entries = index.entries || [];
  if (entries.length === 0) {
    setStatus("No snapshots found in index.", true);
    return;
  }

  setStatus(`Loading ${entries.length} snapshot(s)…`);

  const snapshots = [];
  for (const dateKey of entries) {
    const url = `${R2_BASE}/metrics/${dateKey}.json`;
    try {
      const snap = await fetchWithCache(url, `snapshot-${dateKey}.json`);
      snapshots.push(snap);
    } catch (err) {
      console.warn(`Failed to load snapshot ${dateKey}:`, err);
    }
  }

  if (snapshots.length === 0) {
    setStatus("No snapshots could be loaded.", true);
    return;
  }

  // Sort oldest-first for charts
  snapshots.sort((a, b) => a.date.localeCompare(b.date));
  const latest = snapshots.at(-1);

  // Latest metric values
  setMetric("val-p50", latest.precision_at_50, (v) => v.toFixed(4));
  if (latest.precision_at_50_ci_low != null && latest.precision_at_50_ci_high != null) {
    document.getElementById("sub-p50").textContent =
      `CI 95%: ${latest.precision_at_50_ci_low.toFixed(3)}–${latest.precision_at_50_ci_high.toFixed(3)} · gate ≥ 0.60`;
  }
  setMetric("val-recall", latest.recall_at_200, (v) => v.toFixed(4));
  setMetric("val-lead", latest.median_lead_days, (v) => `${v}d`);
  if (latest.pre_designation_count != null) {
    document.getElementById("sub-lead").textContent =
      `median · ${latest.pre_designation_count} vessel(s) detected pre-designation`;
  }
  setMetric("val-uu", latest.unknown_unknown_candidates);

  // Charts
  mountChart("chart-p50", sparkline(snapshots, "precision_at_50", {
    color: latest.precision_at_50 >= 0.25 ? "#3fb950" : "#f85149",
    fmt: (v) => v.toFixed(2),
  }));
  mountChart("chart-recall", sparkline(snapshots, "recall_at_200", {
    color: "#58a6ff",
    fmt: (v) => v.toFixed(2),
  }));
  mountChart("chart-lead", sparkline(snapshots, "median_lead_days", {
    color: "#d2a679",
    fmt: (v) => `${v}d`,
  }));
  mountChart("chart-uu", sparkline(snapshots, "unknown_unknown_candidates", {
    color: "#bc8cff",
    fmt: (v) => String(Math.round(v)),
  }));

  // Table (newest first)
  renderTable([...snapshots].reverse());

  const cached = await navigator.storage.estimate();
  const updatedAt = index.updated_at_utc?.slice(0, 16).replace("T", " ") ?? "unknown";
  const cacheKB = cached.usage ? Math.round(cached.usage / 1024) : "?";
  document.getElementById("footer").textContent =
    `Index updated: ${updatedAt} UTC · OPFS cache: ~${cacheKB} KB · Source: maridb-public R2`;

  setStatus(`Showing ${snapshots.length} snapshot(s) · latest: ${latest.date}`);
}

main().catch((err) => setStatus(`Unexpected error: ${err.message}`, true));
