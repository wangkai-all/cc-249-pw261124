/* Operations dashboard.  Everything shown here is read from the API, which
 * reads committed SQLite state; the page never invents success. */
"use strict";

const $ = (id) => document.getElementById(id);
const state = {
  auto: true,
  timer: null,
  status: null,
  settingsInit: false,
  busy: false,
};

function fmtBytes(n) {
  if (n === null || n === undefined) return "—";
  const v = Number(n);
  if (!Number.isFinite(v)) return "—";
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let i = 0;
  let x = Math.abs(v);
  while (x >= 1024 && i < units.length - 1) { x /= 1024; i += 1; }
  return i === 0 ? `${Math.round(v)} B` : `${x.toFixed(1)} ${units[i]}`;
}

function fmtNum(n) {
  if (n === null || n === undefined) return "—";
  return Number(n).toLocaleString();
}

function fmtAge(seconds) {
  if (seconds === null || seconds === undefined) return "—";
  const s = Math.max(0, Number(seconds));
  if (s < 1) return `${(s * 1000).toFixed(0)} ms`;
  if (s < 60) return `${s.toFixed(1)} s`;
  if (s < 3600) return `${Math.floor(s / 60)}m ${Math.floor(s % 60)}s`;
  return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
}

function ago(epochSeconds) {
  if (!epochSeconds) return "—";
  return fmtAge(Date.now() / 1000 - epochSeconds);
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  let body = null;
  try { body = await response.json(); } catch { body = null; }
  if (!response.ok && !(body && body.error && response.status === 429)) {
    const message = body?.error?.message || `${response.status} ${response.statusText}`;
    throw new Error(message);
  }
  return body;
}

function toast(message, isError = false) {
  let el = document.querySelector(".toast");
  if (!el) {
    el = document.createElement("div");
    el.className = "toast";
    document.body.appendChild(el);
  }
  el.textContent = message;
  el.classList.toggle("err", isError);
  el.classList.add("show");
  clearTimeout(el._t);
  el._t = setTimeout(() => el.classList.remove("show"), 3200);
}

/* ---------------------------------------------------------------- status */
function render(status) {
  state.status = status;
  const worker = status.worker;
  $("health-dot").className = "dot " + (worker.running ? "live" : "down");
  $("worker-pill").textContent = worker.running
    ? `worker: pid ${worker.pid}`
    : "worker: stopped";
  $("worker-pill").className = "pill " + (worker.running ? "ok" : "danger");
  const pressure = status.spool.pressure;
  $("pressure-pill").textContent = "pressure: " + pressure.replace(/_/g, " ");
  $("pressure-pill").className =
    "pill " + (pressure === "normal" ? "ok" : pressure === "soft_limit" ? "warn" : "danger");

  // KPIs
  const live = status.spool.live_batches;
  const q = status.spool.quarantine_batches;
  $("kpi-backlog").textContent = fmtNum(live) + (q ? ` (+${q} held)` : "");
  $("kpi-backlog-sub").textContent =
    `${fmtBytes(status.spool.used_bytes)} of ${fmtBytes(status.spool.cap_bytes)} cap · ` +
    `${status.spool.percent.toFixed(1)}% full`;
  const meter = $("meter-spool");
  meter.style.width = Math.min(100, status.spool.percent) + "%";
  meter.className = status.spool.percent >= 95 ? "full" : status.spool.percent >= 70 ? "hot" : "";

  const indexed = status.totals.indexed_batches;
  $("kpi-indexed").textContent = fmtNum(indexed);
  $("kpi-indexed-sub").textContent =
    `${fmtNum(status.totals.indexed_records)} records · ` +
    `${status.totals.storage.hot_batches} hot / ${status.totals.storage.cold_batches} cold`;

  const oldest = status.backlog.oldest_live_epoch;
  const lag = oldest ? Date.now() / 1000 - oldest : null;
  $("kpi-lag").textContent = lag === null ? "0 s" : fmtAge(lag);
  $("kpi-lag-sub").textContent = oldest
    ? `${status.spool.live_batches} batch(es) not yet durable in the index`
    : "no unindexed work";

  $("kpi-worker").textContent = worker.running ? "running" : "stopped";
  $("kpi-worker-sub").textContent = worker.running
    ? `pid ${worker.pid} · API is independent`
    : "API still accepting within credit";

  renderChart(status.samples);
  renderResources(status);
  renderSources(status);
  renderQueue(status);
  renderTiers(status);
  renderIntegrity(status.integrity);
  renderEvents(status.events);
  syncControls(status.settings);
}

/* ----------------------------------------------------------------- chart */
function renderChart(samples) {
  const canvas = $("chart");
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  const width = Math.max(320, rect.width);
  const height = 260;
  canvas.width = width * dpr;
  canvas.height = height * dpr;
  const ctx = canvas.getContext("2d");
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, width, height);

  const pad = { l: 46, r: 12, t: 12, b: 22 };
  const w = width - pad.l - pad.r;
  const h = height - pad.t - pad.b;
  ctx.strokeStyle = "#232b35";
  ctx.lineWidth = 1;
  ctx.strokeRect(pad.l, pad.t, w, h);

  if (!samples || samples.length < 2) {
    ctx.fillStyle = "#93a0b1";
    ctx.font = "12px ui-sans-serif";
    ctx.fillText("collecting samples…", pad.l + 10, pad.t + 20);
    $("sample-note").textContent = "waiting for worker samples";
    return;
  }
  $("sample-note").textContent = `${samples.length} samples · last ${ago(samples[samples.length - 1].epoch)}`;

  const maxSpool = Math.max(1, ...samples.map((s) => s.live_bytes || 0));
  const maxRecords = Math.max(1, ...samples.map((s) => s.indexed_records || 0));
  const maxLag = Math.max(1, ...samples.map((s) => s.lag_secs || 0));
  const firstT = samples[0].epoch;
  const lastT = samples[samples.length - 1].epoch;
  const span = Math.max(1, lastT - firstT);
  const x = (epoch) => pad.l + ((epoch - firstT) / span) * w;

  const line = (getter, max, color, fill) => {
    ctx.beginPath();
    samples.forEach((s, i) => {
      const px = x(s.epoch);
      const py = pad.t + h - Math.min(1, getter(s) / max) * h;
      if (i === 0) ctx.moveTo(px, py);
      else ctx.lineTo(px, py);
    });
    ctx.strokeStyle = color;
    ctx.lineWidth = 2;
    ctx.stroke();
    if (fill) {
      ctx.lineTo(x(samples[samples.length - 1].epoch), pad.t + h);
      ctx.lineTo(x(samples[0].epoch), pad.t + h);
      ctx.closePath();
      ctx.fillStyle = fill;
      ctx.fill();
    }
  };
  line((s) => s.live_bytes || 0, maxSpool, "#5fb3ff", "rgba(95,179,255,0.12)");
  line((s) => s.indexed_records || 0, maxRecords, "#4fd18b", null);
  ctx.setLineDash([4, 4]);
  line((s) => s.lag_secs || 0, maxLag, "#f2b84b", null);
  ctx.setLineDash([]);

  ctx.fillStyle = "#93a0b1";
  ctx.font = "11px ui-sans-serif";
  ctx.fillText(fmtBytes(maxSpool), 4, pad.t + 10);
  ctx.fillText("0", 4, pad.t + h);
  ctx.fillText("-" + fmtAge(span), pad.l, height - 6);
}

/* ------------------------------------------------------------- resources */
function renderResources(status) {
  const rows = [
    ["spool used", fmtBytes(status.spool.used_bytes)],
    ["spool hard cap", fmtBytes(status.spool.cap_bytes)],
    ["soft waterline", fmtBytes(status.spool.soft_limit_bytes)],
    ["live / max batches", `${status.spool.live_batches} / ${status.spool.max_inflight_batches}`],
    ["quarantine held", `${status.spool.quarantine_batches} · ${fmtBytes(status.spool.quarantine_bytes)}`],
    ["free disk", fmtBytes(status.disk.free_bytes)],
    ["free disk floor", fmtBytes(status.disk.min_free_bytes)],
    ["metadata DB", `${fmtBytes(status.db.db_bytes)} (+${fmtBytes(status.db.wal_bytes)} WAL)`],
  ];
  $("resource-rows").innerHTML = rows
    .map(([k, v]) => `<dt>${k}</dt><dd>${v}</dd>`)
    .join("");
  const cap = status.spool.cap_bytes || 1;
  $("meter-cap").style.width = Math.min(100, (100 * status.spool.used_bytes) / cap) + "%";
  const soft = (100 * status.spool.soft_limit_bytes) / cap;
  $("cap-marker").style.left = soft + "%";
  const disk = status.disk;
  const used = disk.total_bytes ? 1 - disk.free_bytes / disk.total_bytes : 0;
  $("meter-disk").style.width = (Math.max(0, Math.min(1, used)) * 100) + "%";
  $("resource-note").textContent =
    status.spool.pressure === "normal"
      ? "Both gates open: senders may borrow shared slack."
      : status.spool.pressure === "soft_limit"
      ? status.spool.lag_reduced
        ? "Index lag gate: the oldest live batch is old enough that borrowed credit is withdrawn; reservations still work."
        : "Soft gate: senders are limited to their reservations until the index catches up."
      : "Hard gate: /ingest answers 429 and writes nothing.";
}

/* --------------------------------------------------------------- sources */
function renderSources(status) {
  $("sources").innerHTML = status.sources
    .map((s) => {
      const c = s.credit;
      const st = s.states || {};
      const live = (st.staged?.count || 0) + (st.indexing?.count || 0);
      const indexed = st.indexed?.count || 0;
      const held = st.quarantine?.count || 0;
      const creditClass = c.credit_batches > 0 ? "ok" : "danger";
      return `
      <div class="source ${s.active ? "" : "paused"}">
        <div class="source-head">
          <div>
            <div class="source-name">${s.source_id} <span class="source-meta">${s.label || ""}</span></div>
            <div class="source-meta">
              ${s.active ? "active" : "paused"} · reserved ${fmtBytes(s.reserved_bytes)} / ${s.reserved_batches} batches
              · max batch ${fmtBytes(s.max_batch_bytes)}
            </div>
          </div>
          <span class="pill ${creditClass}">${c.reason.replace(/_/g, " ")}</span>
        </div>
        <div class="credit-grid">
          <div class="credit-cell"><b>${fmtBytes(c.credit_bytes)}</b><span>credit bytes</span></div>
          <div class="credit-cell"><b>${fmtNum(c.credit_batches)}</b><span>credit batches</span></div>
          <div class="credit-cell"><b>${fmtNum(c.used_batches)}</b><span>in flight</span></div>
          <div class="credit-cell"><b>${fmtNum(s.indexed_hot_batches)}/${fmtNum(s.indexed_cold_batches)}</b><span>hot / cold</span></div>
        </div>
        <div class="source-meta" style="margin-top:8px">
          ${fmtNum(live)} live · ${fmtNum(indexed)} indexed${held ? ` · <span style="color:var(--danger)">${held} quarantined</span>` : ""}
          ${c.global_limited ? " · limited by global spool" : ""}
          ${c.borrowing ? " · using shared slack" : ""}
        </div>
      </div>`;
    })
    .join("");
}

/* ----------------------------------------------------------------- queue */
function renderQueue(status) {
  const pending = status.backlog.pending || [];
  const recent = status.backlog.recent || [];
  const seen = new Set(pending.map((b) => b.seq));
  const rows = [...pending, ...recent.filter((b) => !seen.has(b.seq))].slice(0, 24);
  $("queue").querySelector("tbody").innerHTML = rows.length
    ? rows
        .map((b) => {
          const note = b.error
            ? escapeHtml(b.error)
            : b.indexed_at
            ? `indexed ${ago(new Date(b.indexed_at).getTime() / 1000)} ago`
            : `received ${ago(new Date(b.received_at).getTime() / 1000)} ago`;
          const action =
            b.state === "quarantine"
              ? `<button class="tiny" data-requeue="${b.seq}">requeue</button>`
              : "";
          return `<tr>
            <td>${b.seq}</td>
            <td>${escapeHtml(b.source_id)}</td>
            <td>${escapeHtml(b.batch_id)}</td>
            <td><span class="state ${b.state}">${b.state}</span>
              <span class="storage ${b.storage_level || ""}">${b.storage_level && b.storage_level !== "none" ? b.storage_level : ""}</span></td>
            <td>${fmtNum(b.record_count)}</td>
            <td>${fmtNum(b.attempts)}</td>
            <td class="note">${note}</td>
            <td>${action}</td>
          </tr>`;
        })
        .join("")
    : `<tr><td colspan="8" class="muted">queue is empty</td></tr>`;
  $("queue").querySelectorAll("[data-requeue]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      try {
        await api(`/api/batches/${btn.dataset.requeue}/requeue`, { method: "POST", body: "{}" });
        toast(`batch ${btn.dataset.requeue} requeued`);
        refresh();
      } catch (err) { toast(err.message, true); }
    });
  });
}

/* ----------------------------------------------------------------- tiers */
function renderTiers(status) {
  const t = status.totals.storage;
  const rows = [
    ["hot batches", fmtNum(t.hot_batches)],
    ["hot records", fmtNum(t.hot_records)],
    ["cold batches", fmtNum(t.cold_batches)],
    ["cold records", fmtNum(t.cold_records)],
    ["segment files", fmtNum(t.segment_files)],
    ["segment bytes", fmtBytes(t.segment_bytes)],
    ["state: staged", fmtNum(status.totals.states.staged?.count || 0)],
    ["state: indexing", fmtNum(status.totals.states.indexing?.count || 0)],
    ["state: quarantine", fmtNum(status.totals.states.quarantine?.count || 0)],
  ];
  $("tier-rows").innerHTML = rows.map(([k, v]) => `<dt>${k}</dt><dd>${v}</dd>`).join("");
}

/* ------------------------------------------------------------- integrity */
function renderIntegrity(integrity) {
  const box = $("integrity");
  if (integrity.running) {
    box.innerHTML = `<div class="stats">checking payloads and digests…</div>`;
    return;
  }
  const report = integrity.report;
  if (!report) {
    box.innerHTML = `<div class="stats">No check has run yet. This verifies every indexed payload's
      digest against metadata, record counts, and the shared index row count.</div>`;
    return;
  }
  const issues = report.issues || [];
  box.innerHTML = `
    <div class="verdict ${report.ok ? "ok" : "bad"}">
      ${report.ok ? "✓ all payloads verified" : `✗ ${issues.length} issue(s)`}
    </div>
    <div class="stats">
      ${fmtNum(report.checked_batches)} batches · ${fmtNum(report.indexed_records)} records in files ·
      ${fmtNum(report.record_index_count)} index rows · ${fmtNum(report.segment_files)} segment files
      · ${ago(report.finished_epoch)}
    </div>
    ${issues.length
      ? `<ul>${issues.slice(0, 8).map((i) => `<li>${escapeHtml(JSON.stringify(i))}</li>`).join("")}</ul>`
      : ""}`;
}

/* ---------------------------------------------------------------- events */
function renderEvents(events) {
  $("events").innerHTML = (events || [])
    .slice(0, 30)
    .map((e) => {
      const time = new Date(e.epoch * 1000).toLocaleTimeString();
      return `<li class="lvl-${e.level}">
        <time>${time}</time>
        <div><span class="kind">${escapeHtml(e.kind)}</span>
        <span class="message">${escapeHtml(e.message)}</span></div>
      </li>`;
    })
    .join("");
}

/* -------------------------------------------------------------- controls */
function syncControls(settings) {
  if (state.settingsInit) return;
  state.settingsInit = true;
  $("index-delay").value = settings.index_delay_ms;
  $("storage-delay").value = settings.storage_delay_ms;
  $("spool-cap").value = Math.min(8388608, Math.max(262144, settings.spool_cap_bytes));
  updateDelayLabels();
  updateCapLabel();
}

function updateDelayLabels() {
  $("index-delay-val").textContent = `${$("index-delay").value} ms`;
  $("storage-delay-val").textContent = `${$("storage-delay").value} ms`;
}
function updateCapLabel() {
  $("spool-cap-val").textContent = fmtBytes(Number($("spool-cap").value));
}

async function saveSettings(patch) {
  try {
    await api("/api/settings", { method: "POST", body: JSON.stringify(patch) });
    toast(`settings updated: ${Object.keys(patch).join(", ")}`);
    refresh();
  } catch (err) { toast(err.message, true); }
}

function bindControls() {
  $("refresh-toggle").addEventListener("click", () => {
    state.auto = !state.auto;
    $("refresh-toggle").textContent = `Auto-refresh: ${state.auto ? "on" : "off"}`;
    $("refresh-toggle").setAttribute("aria-pressed", String(state.auto));
    schedule();
  });
  $("index-delay").addEventListener("input", updateDelayLabels);
  $("index-delay").addEventListener("change", () =>
    saveSettings({ index_delay_ms: Number($("index-delay").value) }));
  $("storage-delay").addEventListener("input", updateDelayLabels);
  $("storage-delay").addEventListener("change", () =>
    saveSettings({ storage_delay_ms: Number($("storage-delay").value) }));
  $("spool-cap").addEventListener("input", updateCapLabel);
  $("spool-cap").addEventListener("change", () =>
    saveSettings({ spool_cap_bytes: Number($("spool-cap").value) }));

  $("btn-worker-stop").addEventListener("click", async () => {
    try {
      await api("/api/control/worker", { method: "POST", body: JSON.stringify({ action: "kill" }) });
      toast("worker killed — backlog will grow until restart");
      refresh();
    } catch (err) { toast(err.message, true); }
  });
  $("btn-worker-start").addEventListener("click", async () => {
    try {
      const r = await api("/api/control/worker", { method: "POST", body: JSON.stringify({ action: "start" }) });
      toast(r.already ? "worker already running" : `worker started (pid ${r.pid})`);
      refresh();
    } catch (err) { toast(err.message, true); }
  });
  $("btn-bad").addEventListener("click", async () => {
    try {
      const r = await api("/api/control/inject-bad", {
        method: "POST",
        body: JSON.stringify({ mode: "invalid-record", source_id: "alpha" }),
      });
      toast(r.ok ? "bad batch was accepted (unexpected)" : "bad batch quarantined", !r.ok ? false : true);
      refresh();
    } catch (err) { toast(err.message, true); }
  });
  $("btn-integrity").addEventListener("click", async () => {
    $("control-note").textContent = "integrity check running…";
    try {
      const r = await api("/api/control/integrity", { method: "POST", body: "{}" });
      $("control-note").textContent = r.report?.ok
        ? `integrity verified ${r.report.checked_batches} batches`
        : `integrity found ${r.report?.issues?.length || "?"} issue(s)`;
      refresh();
    } catch (err) {
      $("control-note").textContent = err.message;
      toast(err.message, true);
    }
  });
  $("btn-pause-alpha").addEventListener("click", () => setSourceActive("alpha", false));
  $("btn-resume-alpha").addEventListener("click", () => setSourceActive("alpha", true));
}

async function setSourceActive(source, active) {
  try {
    await api(`/api/sources/${source}`, {
      method: "POST",
      body: JSON.stringify({ active }),
    });
    toast(`${source} ${active ? "resumed" : "paused"}`);
    refresh();
  } catch (err) { toast(err.message, true); }
}

/* ---------------------------------------------------------------- records */
async function loadRecords() {
  try {
    const data = await api("/api/records?limit=14");
    $("records-total").textContent = `${fmtNum(data.total_indexed)} records indexed`;
    $("records").querySelector("tbody").innerHTML = data.records.length
      ? data.records
          .map(
            (r) => `<tr>
              <td>${escapeHtml(r.source_id)}</td>
              <td>${escapeHtml(r.doc_id)}</td>
              <td>${escapeHtml(JSON.stringify(r.value))}</td>
              <td>${escapeHtml(r.batch_id)}</td>
            </tr>`
          )
          .join("")
      : `<tr><td colspan="4" class="muted">nothing indexed yet</td></tr>`;
  } catch (err) {
    /* non-fatal */
  }
}

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, (ch) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  })[ch]);
}

/* ------------------------------------------------------------------ loop */
async function refresh() {
  if (state.busy) return;
  state.busy = true;
  try {
    const status = await api("/api/status");
    render(status);
    if (state.auto) await loadRecords();
  } catch (err) {
    $("health-dot").className = "dot down";
    $("worker-pill").textContent = "API unreachable";
    $("worker-pill").className = "pill danger";
  } finally {
    state.busy = false;
  }
}

function schedule() {
  clearInterval(state.timer);
  if (state.auto) state.timer = setInterval(refresh, 1500);
}

bindControls();
refresh();
schedule();
