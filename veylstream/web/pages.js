"use strict";
/* Veylstream pages. Every control calls the real API; failures surface as toasts or inline errors. */
(function () {
const { esc, $, api, fmt, pct } = Object.assign({ $: window.$ || ((s, e = document) => e.querySelector(s)), api: V.api, fmt: V.fmt, pct: V.pct, esc: V.esc });
const Q = (s, el = document) => el.querySelector(s);
const P = V.pages;
const kpi = (l, v, s = "") => `<div class="panel kpi"><div class="l">${esc(l)}</div><div class="v">${v}</div><div class="s">${s}</div></div>`;
const debounce = (fn, ms) => { let t; return (...a) => { clearTimeout(t); t = setTimeout(() => fn(...a), ms); }; };
const SEVS = ["critical", "high", "medium", "low"];
const pts = (rows, m) => rows.map((p) => [p.t, p[m]]);

/* ---------- landing + login ---------- */
const loginBase = V.loginView;
V.loginView = function (msg) {
  loginBase(msg);
  const lg = Q(".login"); lg.style.maxWidth = "1100px"; lg.style.margin = "0 auto"; lg.style.gridTemplateColumns = "1.1fr 400px"; lg.style.gap = "40px"; lg.style.display = "grid"; lg.style.alignItems = "center";
  const hero = document.createElement("div");
  hero.innerHTML = `<h1 style="font-size:38px;line-height:1.1;letter-spacing:-.03em;max-width:15ch">See the signal. Detect the unexpected.</h1>
  <p class="muted" style="max-width:52ch;font-size:15px">Veylstream ingests event streams, aggregates them in event time, runs rule, statistical and machine-learning detectors side by side, and turns what they find into alerts you can investigate, replay and benchmark.</p>
  <svg viewBox="0 0 560 120" width="100%" style="max-width:560px;margin:10px 0" role="img" aria-label="Data flow: producers to ingestion to broker log to processor to detectors to alerts and dashboard">
   ${["Producers", "Ingest", "Log", "Process", "Detect", "Alert"].map((t, i) => `<g transform="translate(${i * 92},34)"><rect width="78" height="44" rx="8" fill="var(--bg2)" stroke="var(--line2)"/><text x="39" y="26" text-anchor="middle" font-size="12" fill="var(--text)">${t}</text></g>${i < 5 ? `<path d="M${i * 92 + 80},56h10" stroke="var(--cyan)" stroke-width="2"/>` : ""}`).join("")}
   <path d="M0 104h110l12-18 14 30 14-24 12 12h400" fill="none" stroke="var(--cyan)" stroke-width="1.6" opacity=".7"/><circle cx="262" cy="110" r="4" fill="var(--red)"/></svg>
  <div class="grid g2 small muted" style="max-width:560px"><div>Windowed analytics with exact percentiles, late-event and duplicate handling</div><div>Seven detector families with explainable, evidence-backed findings</div><div>Alert lifecycle with grouping, cooldown, audit trail and webhooks</div><div>Replay and a reproducible research lab with ground-truth scoring</div></div>`;
  lg.prepend(hero);
};

/* ---------- overview ---------- */
P.overview = async (el) => {
  el.innerHTML = `<div class="page-head"><h1>Overview</h1><span id="simb"></span><div class="grow"></div><span class="muted small" id="winfo"></span></div>
  <div class="grid g6" id="kpis"></div><div class="grid g21" style="margin-top:14px"><div class="panel"><h2>Throughput <span class="muted small">events per second, 30s windows, global</span></h2><div id="c1"></div></div><div class="panel"><h2>Open alerts</h2><div id="al"></div></div></div>
  <div class="grid g2" style="margin-top:14px"><div class="panel"><h2>Transaction failure rate</h2><div id="c2"></div></div><div class="panel"><h2>Latency p95 (ms)</h2><div id="c3"></div></div></div>
  <div class="grid g2" style="margin-top:14px"><div class="panel"><h2>Recent anomalies</h2><div id="an"></div></div><div class="panel" id="demo"></div></div>`;
  const load = async () => {
    const [o, w] = [await api("/overview"), V.rangeWindow()];
    const q = (m) => api(`/analytics/series?${V.qs({ entity: "global", metrics: m, from: w.from, to: w.to })}`, { full: true });
    const [s1, s2, s3] = await Promise.all([q("event_rate"), q("txn_failure_rate"), q("latency_p95_ms")]);
    Q("#simb", el).innerHTML = o.simulated_data_present ? V.simBadge() + ` <span class="muted small">this workspace contains simulated sources</span>` : "";
    Q("#winfo", el).textContent = `${o.mode} processor · ${o.window_s}s windows`;
    Q("#kpis", el).innerHTML = kpi("Events processed", fmt(o.total_events, 0), "since workspace start") + kpi("Events/s (10s)", fmt(o.events_per_s), `processed ${fmt(o.processed_per_s)}/s`) + kpi("Failure rate", pct(o.failure_rate), "last 3 windows") +
      kpi("Active anomalies", o.active_anomalies, "distinct series, 5 min") + kpi("Critical alerts", o.critical_alerts, `${o.active_alerts} open total`) + kpi("Pipeline", V.st(o.pipeline_status), `delay p95 ${fmt(o.processing_delay_ms.p95, 0)} ms`);
    const mk = (s, m) => s.data.anomalies.filter((a) => a.metric === m).map((a) => ({ t: a.window_start, v: a.observed, sev: a.severity, label: `${a.detector} ${a.severity}` }));
    V.lineChart(Q("#c1", el), { series: [{ name: "events/s", points: s1.data.points.map((p) => [p.t, p.event_rate]) }], markers: mk(s1, "event_rate"), label: "throughput" });
    V.lineChart(Q("#c2", el), { series: [{ name: "failure rate", points: s2.data.points.map((p) => [p.t, p.txn_failure_rate]), color: "#f87171" }], markers: mk(s2, "txn_failure_rate"), fmt: pct, height: 150 });
    V.lineChart(Q("#c3", el), { series: [{ name: "p95", points: s3.data.points.map((p) => [p.t, p.latency_p95_ms]), color: "#a78bfa" }], markers: mk(s3, "latency_p95_ms"), height: 150 });
    Q("#al", el).innerHTML = o.recent_alerts.length ? o.recent_alerts.map((a) => `<div style="padding:6px 0;border-bottom:1px solid var(--line)"><a href="#/alerts">${esc(a.title)}</a><div>${V.sev(a.severity)} ${V.st(a.status)} ${a.simulated ? V.simBadge() : ""}</div></div>`).join("") : '<div class="empty">No alerts. Start the simulated stream and trigger a scenario to see one.</div>';
    Q("#an", el).innerHTML = V.table([{ h: "Time", f: (r) => V.time(r.window_start) }, { h: "Severity", f: (r) => V.sev(r.severity) }, { h: "Series", f: (r) => `${esc(r.entity)} · ${esc(r.metric)}` }, { h: "Detector", f: (r) => esc(r.detector) }], o.recent_anomalies.slice(0, 8), { empty: "No anomalies in the last 30 minutes." });
  };
  const refresh = debounce(() => load().catch(() => {}), 1500);
  await load(); V.live.handlers.add((m) => { if (["window", "anomaly", "alert"].includes(m.type)) refresh(); });
  demoPanel(Q("#demo", el));
};
async function demoPanel(el) {
  if (!V.can("operator")) { el.innerHTML = `<h2>Simulated traffic</h2><div class="muted">Operators and admins can drive the simulated stream.</div>`; return; }
  const d = await api("/demo");
  el.innerHTML = `<h2>Simulated traffic ${V.simBadge()}</h2><p class="muted small" style="margin-top:0">Everything below is generated data. It enters through the same ingestion, processing and detection path as real events and is flagged simulated.</p>
  <div class="pill-row"><button id="d-seed">1. Seed 30 min history</button><button id="d-start" class="primary" ${d.running ? "disabled" : ""}>2. Start stream</button><button id="d-stop" ${d.running ? "" : "disabled"}>Stop</button></div>
  <div class="bar" style="margin-top:12px"><label class="f">3. Trigger scenario<select id="d-kind">${d.scenario_kinds.map((k) => `<option>${k}</option>`).join("")}</select></label><label class="f">Duration (s)<input id="d-dur" type="number" value="180" min="10" max="1800" style="width:90px"></label><button id="d-trig" ${d.running ? "" : "disabled"}>Trigger</button></div>
  <div class="small muted">${d.running ? `Streaming ~${fmt(d.rate, 0)} events/s (${fmt(d.sent, 0)} sent).` : "Stream is stopped."} ${d.scenarios.length ? "Last: " + esc(d.scenarios.at(-1).kind) : ""}</div>
  ${V.can("admin") ? '<div style="margin-top:10px"><button id="d-reset" class="danger">Reset demo data</button></div>' : ""}`;
  const act = (id, path, body, msg) => (Q(id, el).onclick = (e) => V.guard(e.target, async () => { const r = await api(path, { method: "POST", body }); V.toast(msg(r), "good"); demoPanel(el); }));
  act("#d-seed", "/demo/seed", { minutes: 30, rate: 20 }, (r) => `Seeded ${r.events} simulated events${r.drained ? "" : " (processing still catching up)"}`);
  act("#d-start", "/demo/start", { rate: 20 }, () => "Simulated stream started");
  act("#d-stop", "/demo/stop", {}, () => "Simulated stream stopped");
  Q("#d-trig", el).onclick = (e) => V.guard(e.target, async () => { const r = await api("/demo/trigger", { method: "POST", body: { kind: Q("#d-kind", el).value, duration_s: +Q("#d-dur", el).value } }); V.toast(`Scenario ${r.kind} running for ${Math.round(r.end - r.start)}s. Detection takes a few windows.`, "good"); demoPanel(el); });
  if (Q("#d-reset", el)) Q("#d-reset", el).onclick = async (e) => { if (await V.confirm("Reset demo data", "Deletes simulated events, windows, anomalies, alerts and notifications from this demo workspace. Sources and users are kept.", "Reset", true)) V.guard(e.target, async () => { await api("/demo/reset", { method: "POST", body: {} }); V.toast("Demo data reset", "good"); V.render(); }); };
}

/* ---------- live streams ---------- */
P.streams = async (el) => {
  const load = async () => {
    const s = await api("/streams"), p = s.processor;
    el.innerHTML = `<div class="page-head"><h1>Live streams</h1><div class="grow"></div>${V.can("operator") ? `<button id="pp">${p.paused ? "Resume" : "Pause"} processor</button>` : ""}</div>
    <div class="grid g4">${kpi("Processing mode", esc(p.mode), p.distributed ? "distributed" : "single process, no partitioning")}${kpi("Consumer lag", fmt(p.lag, 0), "records behind the log")}${kpi("Watermark", p.watermark ? V.time(p.watermark) : "n/a", `lateness ${s.config.allowed_lateness_s}s`)}${kpi("Open windows", p.open_windows, `${s.config.window_s}s windows`)}</div>
    <div class="grid g21" style="margin-top:14px"><div class="panel"><h2>Sources</h2>${V.table([{ h: "Source", f: (r) => `${esc(r.name)} ${r.simulated ? V.simBadge() : ""}` }, { h: "Status", f: (r) => V.st(r.status) }, { h: "Events/s (60s)", cls: "num", f: (r) => fmt(r.events_per_s_60s) }, { h: "Total", cls: "num", f: (r) => fmt(r.event_count, 0) }, { h: "Last seen", f: (r) => V.ago(r.last_seen_wall) }], s.sources, { empty: "No sources registered. Add one under Data sources." })}</div>
    <div class="panel"><h2>Event types (15 min)</h2><div id="et"></div></div></div>
    <div class="panel" style="margin-top:14px"><h2>Recent events</h2>${V.table([{ h: "Event time", f: (r) => V.time(r.ts) }, { h: "Type", f: (r) => esc(r.event_type) }, { h: "Service", f: (r) => esc(r.service) }, { h: "Region", f: (r) => esc(r.region) }, { h: "Status", f: (r) => esc(r.status) }, { h: "Latency", cls: "num", f: (r) => fmt(r.latency_ms, 0) }, { h: "", f: (r) => (r.simulated ? V.simBadge() : "") }], s.recent_events, { empty: "No events yet." })}</div>`;
    V.bars(Q("#et", el), s.event_types_15m.map((t) => ({ l: t.event_type, v: t.n })), (v) => v.toLocaleString());
    if (Q("#pp", el)) Q("#pp", el).onclick = async (e) => { if (await V.confirm(p.paused ? "Resume processing" : "Pause processing", p.paused ? "Consumption resumes from the log; lag will drain." : "Ingestion keeps accepting events into the durable log, but detection stops until you resume. Lag will grow.", p.paused ? "Resume" : "Pause")) V.guard(e.target, async () => { await api(`/streams/processor/${p.paused ? "resume" : "pause"}`, { method: "POST", body: {} }); load(); }); };
  };
  await load(); const t = setInterval(() => load().catch(() => {}), 5000); V.cleanup = () => clearInterval(t);
};

/* ---------- analytics ---------- */
P.analytics = async (el) => {
  const ents = await api("/analytics/entities");
  const METRICS = ["event_rate", "txn_failure_rate", "http_error_rate", "latency_p95_ms", "latency_mean_ms", "latency_p50_ms", "unique_entities"];
  el.innerHTML = `<div class="page-head"><h1>Analytics</h1></div><div class="panel"><div class="bar"><label class="f">Series<select id="ent">${ents.map((e) => `<option>${esc(e)}</option>`).join("")}</select></label>
  <label class="f">Metric<select id="met">${METRICS.map((m) => `<option>${m}</option>`).join("")}</select></label><label class="f">Compare with<select id="met2"><option value="">none</option>${METRICS.map((m) => `<option>${m}</option>`).join("")}</select></label><button id="exp">Export CSV</button></div><div id="ch"></div></div>
  <div class="grid g2" style="margin-top:14px"><div class="panel"><h2>By service</h2><div id="bs"></div></div><div class="panel"><h2>By region</h2><div id="br"></div></div></div>
  <div class="panel" style="margin-top:14px"><h2>Latency distribution <span class="muted small">log-scale bins</span></h2><div class="bar"><label class="f">Service<select id="ls"><option value="">all</option>${ents.filter((e) => e.startsWith("service:")).map((e) => `<option value="${esc(e.slice(8))}">${esc(e.slice(8))}</option>`).join("")}</select></label><label class="f">Region<select id="lr"><option value="">all</option>${ents.filter((e) => e.startsWith("region:")).map((e) => `<option value="${esc(e.slice(7))}">${esc(e.slice(7))}</option>`).join("")}</select></label></div><div id="hist"></div></div>`;
  if (!ents.length) { Q("#ch", el).innerHTML = '<div class="empty">No windows yet. Seed demo history or send events to see analytics.</div>'; }
  const draw = async () => {
    const w = V.rangeWindow(), m = Q("#met", el).value, m2 = Q("#met2", el).value, ent = Q("#ent", el).value;
    const r = await api(`/analytics/series?${V.qs({ entity: ent, metrics: [m, m2].filter(Boolean).join(","), from: w.from, to: w.to })}`, { full: true });
    const series = [{ name: m, points: r.data.points.map((p) => [p.t, p[m]]) }]; if (m2) series.push({ name: m2, points: r.data.points.map((p) => [p.t, p[m2]]) });
    V.lineChart(Q("#ch", el), { series, markers: r.data.anomalies.filter((a) => a.metric === m).map((a) => ({ t: a.window_start, v: a.observed, sev: a.severity, label: `${a.detector}: ${a.severity}` })), fmt: m.includes("rate") && m !== "event_rate" ? pct : fmt });
    for (const [by, id] of [["service", "#bs"], ["region", "#br"]]) { const b = await api(`/analytics/breakdown?${V.qs({ by, from: w.from, to: w.to })}`); Q(id, el).innerHTML = V.table([{ h: by, f: (x) => esc(x[by]) }, { h: "Events", cls: "num", f: (x) => fmt(x.events, 0) }, { h: "Txn fail", cls: "num", f: (x) => pct(x.txn_failure_rate) }, { h: "HTTP 5xx", cls: "num", f: (x) => pct(x.http_error_rate) }, { h: "p95 (mean)", cls: "num", f: (x) => fmt(x.latency_p95_ms_mean_of_windows, 0) }], b); }
    const h = await api(`/analytics/latency-distribution?${V.qs({ from: w.from, to: w.to, service: Q("#ls", el).value, region: Q("#lr", el).value })}`);
    V.bars(Q("#hist", el), h.bins.map((b) => ({ l: `${fmt(b.lo, 0)}–${fmt(b.hi, 0)} ms`, v: b.count, c: "var(--violet)" })), (v) => v.toLocaleString());
    if (h.n) Q("#hist", el).insertAdjacentHTML("afterbegin", `<div class="muted small">n=${h.n.toLocaleString()}${h.sampled_latest ? " (latest 20,000)" : ""} · p50 ${fmt(h.percentiles.p50, 0)} · p95 ${fmt(h.percentiles.p95, 0)} · p99 ${fmt(h.percentiles.p99, 0)} ms</div>`);
  };
  ["#ent", "#met", "#met2", "#ls", "#lr"].forEach((s) => (Q(s, el).onchange = () => draw().catch((e) => V.toast(e.message, "bad"))));
  Q("#exp", el).onclick = (e) => V.guard(e.target, () => { const w = V.rangeWindow(); return V.download(`/analytics/series?${V.qs({ entity: Q("#ent", el).value, metrics: Q("#met", el).value, from: w.from, to: w.to, format: "csv" })}`, "series.csv"); });
  if (ents.length) await draw();
};

/* ---------- anomalies ---------- */
P.anomalies = async (el) => {
  const f = { page: 1, severity: "", detector: "", q: V.searchQ || "", sort: "-time" }; V.searchQ = "";
  el.innerHTML = `<div class="page-head"><h1>Anomaly explorer</h1></div><div class="panel"><div class="bar"><label class="f">Severity<select id="fs"><option value="">any</option>${SEVS.map((s) => `<option>${s}</option>`).join("")}</select></label>
  <label class="f">Detector<select id="fd"><option value="">any</option>${["rule", "zscore", "mad", "iqr", "isolation_forest", "seasonal", "baseline_deviation", "rate_of_change", "ensemble"].map((s) => `<option>${s}</option>`).join("")}</select></label>
  <label class="f">Search<input id="fq" value="${esc(f.q)}" placeholder="entity, metric, text"></label><label class="f">Sort<select id="fo"><option value="-time">newest</option><option value="-severity">highest score</option><option value="entity">entity</option></select></label></div><div id="tb"></div></div>`;
  const load = async () => {
    const w = V.rangeWindow(), j = await api(`/anomalies?${V.qs({ ...f, from: w.from, to: w.to, page_size: 25 })}`, { full: true });
    Q("#tb", el).innerHTML = V.table([{ h: "Window", f: (r) => V.time(r.window_start, true) }, { h: "Severity", f: (r) => V.sev(r.severity) }, { h: "Series", f: (r) => `${esc(r.entity)} · ${esc(r.metric)}` }, { h: "Observed", cls: "num", f: (r) => fmt(r.observed) }, { h: "Expected", cls: "num", f: (r) => fmt(r.expected) }, { h: "Score", cls: "num", f: (r) => fmt(r.score) }, { h: "Detector", f: (r) => esc(r.detector) }, { h: "Alert", f: (r) => (r.alert_id ? `#${r.alert_id}` : "") }], j.data, { click: true, empty: "No anomalies match these filters in the selected range." }) + V.pager(j.meta);
    V.bindRows(Q("#tb", el), j.data, (r) => anomalyDrawer(r.id)); V.bindPager(Q("#tb", el), (p) => { f.page = p; load(); });
  };
  [["#fs", "severity"], ["#fd", "detector"], ["#fo", "sort"]].forEach(([s, k]) => (Q(s, el).onchange = (e) => { f[k] = e.target.value; f.page = 1; load(); }));
  Q("#fq", el).oninput = debounce((e) => { f.q = e.target.value; f.page = 1; load(); }, 300); await load();
};
async function anomalyDrawer(id) {
  const a = await api(`/anomalies/${id}`);
  const d = V.drawer(`<h1>${V.sev(a.severity)} ${esc(a.metric)} on ${esc(a.entity)}</h1><p>${esc(a.explanation)}</p><div id="ac"></div>
  <dl class="kv"><dt>Detector</dt><dd>${esc(a.detector)} v${esc(a.version)}</dd><dt>Window</dt><dd class="num">${esc(V.time(a.window_start, true))} → ${esc(V.time(a.window_end))}</dd><dt>Deviation began</dt><dd class="num">${esc(V.time(a.deviation_started, true))}</dd>
  <dt>Observed vs expected</dt><dd class="num">${fmt(a.observed, 3)} vs ${fmt(a.expected, 3)} ${a.lower != null || a.upper != null ? `(range ${fmt(a.lower, 3)} to ${fmt(a.upper, 3)})` : ""}</dd><dt>Direction</dt><dd>${esc(a.direction)}</dd><dt>Score</dt><dd class="num">${fmt(a.score)} <span class="muted small">(deviation ÷ detector threshold; ≥1 fires)</span></dd>
  <dt>Supporting volume</dt><dd class="num">${esc(JSON.stringify(a.support))}</dd><dt>Severity policy</dt><dd class="muted">Severity reflects how far past the threshold the value is. It is not a probability.</dd></dl>
  <h2 style="margin-top:14px">Evidence</h2><pre>${esc(JSON.stringify(a.evidence, null, 2))}</pre><h2>Suggested investigation steps</h2><ul>${(a.steps || []).map((s) => `<li>${esc(s)}</li>`).join("") || "<li>None for this metric.</li>"}</ul>
  <h2>Related anomalies</h2><p class="muted small">${esc(a.caveat)}</p>${V.table([{ h: "Time", f: (r) => V.time(r.window_start) }, { h: "Series", f: (r) => `${esc(r.entity)} · ${esc(r.metric)}` }, { h: "Detector", f: (r) => esc(r.detector) }, { h: "Sev", f: (r) => V.sev(r.severity) }], a.related, { empty: "No nearby anomalies." })}
  ${a.alert_id ? `<p><a href="#/alerts" id="toal">Open alert #${a.alert_id}</a></p>` : '<p class="muted">Not linked to an alert (below policy or still building a streak).</p>'}`);
  const hist = a.history.filter((h) => h.value != null).map((h) => [h.t, h.value]);
  V.lineChart(Q("#ac", d), { series: [{ name: a.metric, points: hist }], band: a.lower != null && a.upper != null ? hist.map((h) => [h[0], a.lower, a.upper]) : null, markers: [{ t: a.window_start, v: a.observed, sev: a.severity, label: "this detection" }], height: 160, empty: "Multivariate detections have no single-metric history." });
}

/* ---------- alerts ---------- */
P.alerts = async (el) => {
  const f = { page: 1, status: "", severity: "", q: "" }, sel = new Set();
  el.innerHTML = `<div class="page-head"><h1>Alerts and incidents</h1><div class="grow"></div>${V.can("operator") ? '<button id="bulk">Acknowledge selected</button>' : ""}</div><div class="grid g4" id="sevd"></div>
  <div class="panel" style="margin-top:14px"><div class="bar"><label class="f">Status<select id="as"><option value="">any</option>${["open", "acknowledged", "investigating", "resolved", "suppressed"].map((s) => `<option>${s}</option>`).join("")}</select></label>
  <label class="f">Severity<select id="av"><option value="">any</option>${SEVS.map((s) => `<option>${s}</option>`).join("")}</select></label><label class="f">Search<input id="aq" placeholder="title or summary"></label></div><div id="tb"></div></div>`;
  const load = async () => {
    const j = await api(`/alerts?${V.qs({ ...f, page_size: 25 })}`, { full: true }), d = j.meta.open_by_severity;
    Q("#sevd", el).innerHTML = SEVS.map((s) => kpi(`${s} (not resolved)`, d[s] || 0)).join("");
    Q("#tb", el).innerHTML = V.table([{ h: "", f: (r) => `<input type="checkbox" data-s="${r.id}" aria-label="select alert ${r.id}" ${sel.has(r.id) ? "checked" : ""}>` }, { h: "Alert", f: (r) => `<b>${esc(r.title)}</b> ${r.simulated ? V.simBadge() : ""}<div class="muted small">${esc(r.summary || "").slice(0, 110)}</div>` }, { h: "Severity", f: (r) => V.sev(r.severity) }, { h: "Status", f: (r) => V.st(r.status) }, { h: "Detections", cls: "num", f: (r) => r.detection_count }, { h: "Last seen", f: (r) => V.time(r.last_seen, true) }], j.data, { click: true, empty: "No alerts match. Trigger a scenario from the Overview to generate one." }) + V.pager(j.meta);
    Q("#tb", el).querySelectorAll("input[data-s]").forEach((c) => { c.onclick = (e) => { e.stopPropagation(); c.checked ? sel.add(+c.dataset.s) : sel.delete(+c.dataset.s); }; });
    V.bindRows(Q("#tb", el), j.data, (r) => alertDrawer(r.id, load)); V.bindPager(Q("#tb", el), (p) => { f.page = p; load(); });
  };
  [["#as", "status"], ["#av", "severity"]].forEach(([s, k]) => (Q(s, el).onchange = (e) => { f[k] = e.target.value; f.page = 1; load(); }));
  Q("#aq", el).oninput = debounce((e) => { f.q = e.target.value; f.page = 1; load(); }, 300);
  if (Q("#bulk", el)) Q("#bulk", el).onclick = (e) => V.guard(e.target, async () => { if (!sel.size) return V.toast("Select at least one alert first"); const r = await api("/alerts/bulk-status", { method: "POST", body: { ids: [...sel], status: "acknowledged" } }); V.toast(`${r.updated.length} acknowledged, ${r.failed.length} skipped (invalid transition)`, r.failed.length ? "" : "good"); sel.clear(); load(); });
  await load(); V.live.handlers.add((m) => { if (m.type === "alert") load().catch(() => {}); });
};
const NEXT = { open: ["acknowledged", "investigating", "resolved"], acknowledged: ["investigating", "resolved", "open"], investigating: ["resolved", "acknowledged"], resolved: ["open"], suppressed: ["open", "resolved"] };
async function alertDrawer(id, reload) {
  const a = await api(`/alerts/${id}`);
  const d = V.drawer(`<h1>${V.sev(a.severity)} ${esc(a.title)}</h1><div>${V.st(a.status)} ${a.simulated ? V.simBadge() : ""} <span class="muted small">${a.detection_count} detections · first ${esc(V.time(a.first_seen, true))} · last ${esc(V.time(a.last_seen, true))}</span></div><p>${esc(a.summary || "")}</p>
  ${V.can("operator") ? `<div class="pill-row">${NEXT[a.status].map((s) => `<button data-to="${s}" class="${s === "resolved" ? "primary" : ""}">${s === "open" ? "Reopen" : s[0].toUpperCase() + s.slice(1)}</button>`).join("")}<button id="asg">${a.assignee_user_id === V.user.id ? "Unassign me" : "Assign to me"}</button></div>
  <div class="bar" style="margin-top:10px"><label class="f" style="flex:1">Note<input id="note" placeholder="What did you find or do?"></label><button id="addn">Add note</button></div>` : '<p class="muted">Viewer and analyst roles can read but not change alerts.</p>'}
  <h2 style="margin-top:12px">Timeline</h2><div class="tl">${a.audit.map((x) => `<div><b>${esc(x.action.replace("_", " "))}</b> ${x.from_status ? `${esc(x.from_status)} → ${esc(x.to_status)}` : ""} <span class="muted small">by ${esc(x.actor)} · ${esc(V.time(x.ts, true))}</span>${x.note ? `<div class="muted">${esc(x.note)}</div>` : ""}</div>`).join("")}</div>
  <h2>Related anomalies</h2>${V.table([{ h: "Time", f: (r) => V.time(r.window_start) }, { h: "Detector", f: (r) => esc(r.detector) }, { h: "Sev", f: (r) => V.sev(r.severity) }, { h: "Observed", cls: "num", f: (r) => fmt(r.observed) }], a.anomalies.slice(-12), { click: true, empty: "None linked." })}
  <h2 style="margin-top:12px">Notification delivery</h2>${V.table([{ h: "Destination", f: (r) => esc(r.destination) }, { h: "Status", f: (r) => V.st(r.status) }, { h: "Attempts", f: (r) => r.attempts }, { h: "Detail", f: (r) => esc(r.error || (r.http_status ? "HTTP " + r.http_status : "")) }], a.deliveries, { empty: "No webhook or e-mail deliveries. In-app notifications are always created." })}`);
  d.querySelectorAll("[data-to]").forEach((b) => (b.onclick = () => V.guard(b, async () => { await api(`/alerts/${id}/status`, { method: "POST", body: { status: b.dataset.to, note: Q("#note", d)?.value || null } }); V.toast(`Alert is now ${b.dataset.to}`, "good"); V.closeDrawer(); reload(); alertDrawer(id, reload); })));
  if (Q("#asg", d)) Q("#asg", d).onclick = (e) => V.guard(e.target, async () => { await api(`/alerts/${id}/assign`, { method: "POST", body: { user_id: a.assignee_user_id === V.user.id ? null : V.user.id } }); V.closeDrawer(); reload(); alertDrawer(id, reload); });
  if (Q("#addn", d)) Q("#addn", d).onclick = (e) => V.guard(e.target, async () => { if (!Q("#note", d).value.trim()) return V.toast("Write a note first"); await api(`/alerts/${id}/notes`, { method: "POST", body: { note: Q("#note", d).value } }); V.closeDrawer(); alertDrawer(id, reload); });
  V.bindRows(d, a.anomalies.slice(-12), (r) => anomalyDrawer(r.id));
}

/* ---------- event explorer ---------- */
P.events = async (el) => {
  const f = { page: 1, event_type: "", service: "", region: "", status: "", correlation_id: "", event_id: "", sort: "-time" };
  el.innerHTML = `<div class="page-head"><h1>Event explorer</h1></div><div class="tabs" role="tablist"><button role="tab" aria-selected="true" id="t1">Events</button><button role="tab" aria-selected="false" id="t2">Rejected and late</button></div><div class="panel"><div id="flt" class="bar"></div><div id="tb"></div></div>`;
  const inp = (k, l, opts) => `<label class="f">${l}${opts ? `<select data-k="${k}"><option value="">any</option>${opts.map((o) => `<option>${o}</option>`).join("")}</select>` : `<input data-k="${k}" placeholder="exact match" style="width:150px">`}</label>`;
  const events = async () => {
    Q("#flt", el).innerHTML = inp("event_type", "Type", ["transaction", "telemetry"]) + inp("service", "Service") + inp("region", "Region") + inp("status", "Status") + inp("correlation_id", "Correlation ID") + inp("event_id", "Event ID");
    Q("#flt", el).querySelectorAll("[data-k]").forEach((i) => { i.value = f[i.dataset.k]; i.onchange = (e) => { f[i.dataset.k] = e.target.value.trim(); f.page = 1; list(); }; });
    await list();
  };
  const list = async () => {
    const w = V.rangeWindow(), j = await api(`/events?${V.qs({ ...f, from: w.from, to: w.to, page_size: 30 })}`, { full: true });
    Q("#tb", el).innerHTML = V.table([{ h: "Event time", f: (r) => V.time(r.ts, true) }, { h: "ID", cls: "num", f: (r) => esc(r.event_id) }, { h: "Type", f: (r) => esc(r.event_type) }, { h: "Service", f: (r) => esc(r.service) }, { h: "Region", f: (r) => esc(r.region) }, { h: "Status", f: (r) => esc(r.status) }, { h: "Latency", cls: "num", f: (r) => fmt(r.latency_ms, 0) }, { h: "", f: (r) => (r.simulated ? V.simBadge() : "") }], j.data, { click: true, empty: "No events match in this time range." }) + V.pager(j.meta);
    V.bindRows(Q("#tb", el), j.data, async (r) => { const e = await api(`/events/${encodeURIComponent(r.event_id)}`); V.drawer(`<h1>Event ${esc(r.event_id)}</h1><dl class="kv"><dt>Event time</dt><dd class="num">${esc(e.processing.event_time)}</dd><dt>Ingestion time</dt><dd class="num">${esc(e.processing.ingestion_time)}</dd><dt>Window</dt><dd class="num">${e.processing.window_start ? esc(V.time(e.processing.window_start, true)) : "n/a"}</dd><dt>Validation</dt><dd>${esc(e.processing.validation)}</dd></dl><h2 style="margin-top:12px">Normalized event</h2><pre>${esc(JSON.stringify(e.event, null, 2))}</pre>${V.role === "viewer" ? '<p class="muted small">Entity identifiers are masked for the viewer role.</p>' : ""}`); });
    V.bindPager(Q("#tb", el), (p) => { f.page = p; list(); });
  };
  Q("#t1", el).onclick = () => { Q("#t1", el).setAttribute("aria-selected", true); Q("#t2", el).setAttribute("aria-selected", false); events(); };
  Q("#t2", el).onclick = async () => {
    Q("#t2", el).setAttribute("aria-selected", true); Q("#t1", el).setAttribute("aria-selected", false); Q("#flt", el).innerHTML = "";
    if (!V.can("analyst")) { Q("#tb", el).innerHTML = '<div class="empty">Dead letters can contain raw payloads, so they need the analyst role.</div>'; return; }
    const j = await api("/dead-letters?page_size=50", { full: true });
    Q("#flt", el).innerHTML = Object.entries(j.meta.by_reason).map(([k, v]) => V.badge(`${k}: ${v}`)).join(" ") || '<span class="muted">No rejected or late events recorded.</span>';
    Q("#tb", el).innerHTML = V.table([{ h: "Time", f: (r) => V.time(r.ts, true) }, { h: "Source", f: (r) => esc(r.source_id) }, { h: "Reason", f: (r) => V.badge(r.reason_code, "sev-medium") }, { h: "Detail", f: (r) => `<code>${esc((r.detail || "").slice(0, 160))}</code>` }], j.data, { empty: "Nothing rejected." });
  };
  await events();
};

/* ---------- pipeline ---------- */
P.pipeline = async (el) => {
  const load = async () => {
    const h = await api("/pipeline/status"), m = h.metrics, c = m.counters, tp = h.throughput;
    el.innerHTML = `<div class="page-head"><h1>Pipelines</h1>${V.st(h.status)}<span class="muted small">computed from live component checks, not static</span></div>
    <div class="grid g3">${h.components.map((x) => `<div class="panel"><div class="pill-row" style="justify-content:space-between"><b>${esc(x.name.replace("_", " "))}</b>${V.st(x.status)}</div><div class="muted small" style="margin-top:6px">${esc(x.detail)}</div>${x.saturation != null ? `<div class="hbar" style="margin-top:8px"><i style="width:${Math.min(100, x.saturation * 100)}%"></i></div><div class="small muted">buffer saturation ${pct(x.saturation)}</div>` : ""}${x.latency_ms != null ? `<div class="small muted">query ${x.latency_ms} ms</div>` : ""}</div>`).join("")}</div>
    <div class="grid g4" style="margin-top:14px">${kpi("Accepted", fmt(c.accepted || 0, 0), `${fmt(m.ingest_rate_10s)}/s`)}${kpi("Rejected", fmt(c.rejected || 0, 0), "validation failures")}${kpi("Duplicates", fmt(c.duplicates || 0, 0), "idempotent drops")}${kpi("Late dropped", fmt(c.late_dropped || 0, 0), "beyond allowed lateness")}
    ${kpi("Processing delay p50/p95", `${fmt(m.processing_delay_ms.p50, 0)}/${fmt(m.processing_delay_ms.p95, 0)} ms`, "ingest → processed")}${kpi("Detection latency p95", `${fmt(m.detection_latency_ms.p95, 0)} ms`, "window end → detection")}${kpi("Rate-limited batches", fmt(c.rate_limited_batches || 0, 0), "429 responses")}${kpi("Backpressure rejects", fmt(c.backpressure_rejections || 0, 0), "503 responses")}</div>
    <div class="panel" style="margin-top:14px"><h2>Ingest vs processed (events per second, last 2 minutes)</h2><div id="tc"></div></div>`;
    V.lineChart(Q("#tc", el), { series: [{ name: "accepted", points: tp.map((p) => [p.t, p.accepted]) }, { name: "processed", points: tp.map((p) => [p.t, p.processed]), color: "#a78bfa" }, { name: "rejected", points: tp.map((p) => [p.t, p.rejected]), color: "#f87171" }], height: 150 });
  };
  await load(); const t = setInterval(() => load().catch(() => {}), 4000); V.cleanup = () => clearInterval(t);
};

/* ---------- data quality ---------- */
P.quality = async (el) => {
  const load = async () => {
    const [rules, f] = await Promise.all([api("/dq/rules"), api("/dq/findings?page_size=20")]);
    el.innerHTML = `<div class="page-head"><h1>Data quality</h1><div class="grow"></div>${V.can("operator") ? '<button id="ev">Evaluate now</button>' : ""}</div>
    <div class="panel"><h2>Rules</h2>${V.table([{ h: "Rule", f: (r) => `<b>${esc(r.name)}</b><div class="muted small">${esc(r.kind)} · owner ${esc(r.owner || "n/a")}</div>` }, { h: "Severity", f: (r) => V.sev(r.severity) }, { h: "State", f: (r) => (r.open_findings ? V.st("failing") .replace("st-failing", "st-down") : V.st("ok")) }, { h: "Parameters", f: (r) => `<code>${esc(JSON.stringify(r.params))}</code>` }, { h: "Enabled", f: (r) => `<input type="checkbox" data-r="${r.id}" ${r.enabled ? "checked" : ""} ${V.can("admin") ? "" : "disabled"} aria-label="enable ${esc(r.name)}">` }], rules)}</div>
    <div class="panel" style="margin-top:14px"><h2>Findings</h2>${V.table([{ h: "Time", f: (r) => V.time(r.ts, true) }, { h: "Rule", f: (r) => esc(r.rule) }, { h: "Severity", f: (r) => V.sev(r.severity) }, { h: "Status", f: (r) => V.st(r.status === "open" ? "open" : "resolved").replace(">resolved<", `>${esc(r.status)}<`) }, { h: "Details", f: (r) => `<code>${esc(JSON.stringify(r.details))}</code>` }], f, { empty: "No findings. All enabled rules are passing." })}</div>`;
    el.querySelectorAll("input[data-r]").forEach((c) => (c.onchange = () => V.guard(null, async () => { await api(`/dq/rules/${c.dataset.r}`, { method: "PATCH", body: { enabled: c.checked } }); V.toast("Rule updated", "good"); })));
    if (Q("#ev", el)) Q("#ev", el).onclick = (e) => V.guard(e.target, async () => { const r = await api("/dq/evaluate", { method: "POST", body: {} }); V.toast(`${r.filter((x) => x.failing).length} of ${r.length} checks failing`); load(); });
  };
  await load(); V.live.handlers.add((m) => { if (m.type === "alert") load().catch(() => {}); });
};

/* ---------- data sources ---------- */
P.sources = async (el) => {
  const load = async () => {
    const s = await api("/sources");
    el.innerHTML = `<div class="page-head"><h1>Data sources</h1><div class="grow"></div>${V.can("admin") ? '<button class="primary" id="ns">Register source</button>' : ""}</div>
    <div class="panel">${V.table([{ h: "Source", f: (r) => `<b>${esc(r.name)}</b> ${r.simulated ? V.simBadge() : ""}<div class="muted small num">${esc(r.id)} · key ${esc(r.key_prefix)}…</div>` }, { h: "Type", f: (r) => esc(r.source_type) }, { h: "State", f: (r) => V.st(r.revoked_at ? "revoked" : r.last_seen_wall ? "active" : "queued").replace(">queued<", ">never seen<") }, { h: "Events", cls: "num", f: (r) => fmt(r.event_count, 0) }, { h: "Last event", f: (r) => V.ago(r.last_seen_wall) }, { h: "Heartbeat", f: (r) => (r.heartbeat_interval_s ? `${r.heartbeat_interval_s}s` : "none") }, { h: "", f: (r) => (V.can("admin") && !r.simulated ? `<button data-rot="${r.id}">Rotate key</button> <button class="danger" data-rev="${r.id}">Revoke</button>` : "") }], s, { empty: "No sources yet." })}</div>
    <div class="panel" style="margin-top:14px"><h2>Send events</h2><p class="muted small" style="margin-top:0">Contract version 1.0. Required: event_id, event_type, event_timestamp (ISO-8601 or epoch). Transactions also need service_name, region, status, amount, currency.</p>
    <pre>curl -X POST ${esc(location.origin)}/api/v1/ingest/batch \\
  -H "X-API-Key: vsk_…" -H "Content-Type: application/json" \\
  -d '{"events":[{"event_id":"tx-1","event_type":"transaction","event_timestamp":"${new Date().toISOString()}",
       "service_name":"payments","region":"eu-west","status":"failed","amount":42.5,"currency":"USD","latency_ms":310}]}'</pre>
    <pre>from veylstream.sdk import Producer, transaction_event
p = Producer("${esc(location.origin)}", "vsk_…")
print(p.send_batch([transaction_event("payments", "eu-west", "success", 19.9, latency_ms=120)]))</pre><a href="/api/v1/ingest/schema" target="_blank" rel="noopener">View the event schema</a></div>`;
    if (Q("#ns", el)) Q("#ns", el).onclick = newSource;
    el.querySelectorAll("[data-rot]").forEach((b) => (b.onclick = async () => { if (await V.confirm("Rotate key", "The old key stops working immediately. Update your producers.", "Rotate", true)) V.guard(b, async () => { showKey(await api(`/sources/${b.dataset.rot}/rotate`, { method: "POST", body: {} })); load(); }); }));
    el.querySelectorAll("[data-rev]").forEach((b) => (b.onclick = async () => { if (await V.confirm("Revoke source", "Producers using this key will get 401 and ingestion from it stops.", "Revoke", true)) V.guard(b, async () => { await api(`/sources/${b.dataset.rev}/revoke`, { method: "POST", body: {} }); load(); }); }));
  };
  function newSource() {
    const d = V.dialog(`<h2>Register source</h2><form id="nf" style="display:grid;gap:10px"><label class="f">Name<input name="name" required maxlength="80"></label><label class="f">Type<select name="source_type"><option>application</option><option>sensor</option><option>service</option></select></label><label class="f">Expected heartbeat interval in seconds (optional)<input name="hb" type="number" min="1"></label><label class="f">Rate limit (events/s)<input name="rl" type="number" value="2000" min="1"></label><div class="pill-row" style="justify-content:flex-end"><button type="button" id="x">Cancel</button><button class="primary">Create</button></div></form>`);
    Q("#x", d).onclick = () => d.close();
    Q("#nf", d).onsubmit = (e) => { e.preventDefault(); const f = new FormData(e.target); V.guard(Q(".primary", d), async () => { const r = await api("/sources", { method: "POST", body: { name: f.get("name"), source_type: f.get("source_type"), heartbeat_interval_s: f.get("hb") ? +f.get("hb") : null, rate_limit_per_s: +f.get("rl") } }); showKey(r); load(); }); };
  }
  function showKey(r) {
    const d = V.dialog(`<h2>API key</h2><p class="muted">Copy it now. Only a hash is stored, so it cannot be shown again.</p><pre id="k">${esc(r.api_key)}</pre><div class="pill-row"><button id="cp">Copy</button><button id="tst">Send test event</button></div><div id="tr" class="small" style="margin-top:8px"></div><div class="pill-row" style="justify-content:flex-end;margin-top:10px"><button id="cl" class="primary">Done</button></div>`);
    Q("#cp", d).onclick = () => navigator.clipboard?.writeText(r.api_key).then(() => V.toast("Copied", "good"), () => V.toast("Copy failed; select the key manually", "bad"));
    Q("#tst", d).onclick = (e) => V.guard(e.target, async () => { const res = await fetch("/api/v1/ingest/events", { method: "POST", headers: { "X-API-Key": r.api_key, "Content-Type": "application/json" }, body: JSON.stringify({ event_id: "test-" + Math.random().toString(36).slice(2, 10), event_type: "telemetry", event_timestamp: new Date().toISOString(), service_name: "connection-test", region: "local", http_status: 200, latency_ms: 12 }) }); const j = await res.json(); Q("#tr", d).innerHTML = `HTTP ${res.status}: ${esc(JSON.stringify(j.data || j.error))}`; });
    Q("#cl", d).onclick = () => d.close();
  }
  await load();
};

/* ---------- replay ---------- */
P.replay = async (el) => {
  let timer;
  const load = async () => {
    const [ds, jobs] = await Promise.all([api("/datasets"), api("/replays")]);
    el.innerHTML = `<div class="page-head"><h1>Historical replay</h1></div>
    <div class="grid g2"><div class="panel"><h2>Datasets</h2>${V.table([{ h: "Name", f: (r) => `<b>${esc(r.name)}</b> ${JSON.parse(r.meta_json).simulated ? V.simBadge() : ""}<div class="muted small num">sha256 ${esc(r.sha256.slice(0, 12))}…</div>` }, { h: "Rows", cls: "num", f: (r) => `${fmt(r.rows_valid, 0)}${r.rows_rejected ? ` <span class="muted">(+${r.rows_rejected} rejected)</span>` : ""}` }, { h: "Range", f: (r) => `${V.time(r.ts_min, true)}` }, { h: "Labels", f: (r) => (r.has_labels ? "yes" : "no") }, { h: "", f: (r) => (r.rows_rejected ? `<button data-rj="${r.id}">Rejected rows</button>` : "") }], ds, { empty: "No datasets. Import a file or generate a synthetic one." })}
    ${V.can("analyst") ? `<div class="bar" style="margin-top:10px"><button id="gen">Generate labeled synthetic dataset</button><label class="f">Import CSV / JSONL<input type="file" id="imp" accept=".csv,.jsonl,.json"></label></div>` : ""}</div>
    <div class="panel"><h2>Start a replay</h2>${V.can("analyst") ? `<div class="bar"><label class="f">Dataset<select id="rds">${ds.map((d) => `<option value="${d.id}">${esc(d.name)}</option>`).join("")}</select></label><label class="f">Speed (0 = as fast as possible)<input id="rsp" type="number" value="0" min="0" style="width:90px"></label><label class="f">Skip first N rows<input id="rof" type="number" value="0" min="0" style="width:90px"></label><button class="primary" id="rgo" ${ds.length ? "" : "disabled"}>Start replay</button></div><p class="muted small">Replays run the real aggregation and detectors in an isolated scope. They never open live alerts.</p>` : '<div class="muted">Analyst role or higher is required.</div>'}</div></div>
    <div class="panel" style="margin-top:14px"><h2>Replay jobs</h2>${V.table([{ h: "Job", f: (r) => `<span class="num">${esc(r.id)}</span>` }, { h: "Status", f: (r) => V.st(r.status) }, { h: "Progress", f: (r) => `<div class="hbar" style="width:120px"><i style="width:${r.progress * 100}%"></i></div>` }, { h: "Events", cls: "num", f: (r) => fmt(r.processed, 0) }, { h: "Controls", f: (r) => (["running", "paused"].includes(r.status) && V.can("analyst") ? `<button data-c="${r.id}:${r.status === "paused" ? "resume" : "pause"}">${r.status === "paused" ? "Resume" : "Pause"}</button> <button class="danger" data-c="${r.id}:stop">Stop</button>` : "") }], jobs, { click: true, empty: "No replays yet." })}</div><div id="rres"></div>`;
    el.querySelectorAll("[data-c]").forEach((b) => (b.onclick = (e) => { e.stopPropagation(); const [id, a] = b.dataset.c.split(":"); V.guard(b, async () => { await api(`/replays/${id}/${a}`, { method: "POST", body: {} }); load(); }); }));
    el.querySelectorAll("[data-rj]").forEach((b) => (b.onclick = () => { const d = ds.find((x) => x.id === b.dataset.rj); V.drawer(`<h1>Rejected rows</h1><p class="muted">First ${JSON.parse(d.rejects_json).length} of ${d.rows_rejected}.</p>${V.table([{ h: "Row", f: (r) => r.row }, { h: "Code", f: (r) => esc(r.code) }, { h: "Message", f: (r) => esc(r.message) }], JSON.parse(d.rejects_json))}`); }));
    V.bindRows(el, jobs, (r) => result(r.id));
    if (Q("#rgo", el)) Q("#rgo", el).onclick = (e) => V.guard(e.target, async () => { const r = await api("/replays", { method: "POST", body: { dataset_id: Q("#rds", el).value, speed: +Q("#rsp", el).value, start_offset: +Q("#rof", el).value } }); V.toast("Replay started", "good"); load(); });
    if (Q("#gen", el)) Q("#gen", el).onclick = (e) => V.guard(e.target, async () => { V.toast("Generating 65 minutes of simulated data…"); await api("/datasets/synthetic", { method: "POST", body: { name: "incidents-" + Date.now().toString(36), seed: 11, duration_s: 3900, base_rate: 25, scenarios: [{ kind: "failure_spike", start: 2000, end: 2300, service: "payments", region: "eu-west", magnitude: 0.4 }, { kind: "latency_degradation", start: 2800, end: 3100, service: "checkout", magnitude: 4 }, { kind: "traffic_drop", start: 3300, end: 3500, magnitude: 0.2 }] } }); V.toast("Dataset ready", "good"); load(); });
    if (Q("#imp", el)) Q("#imp", el).onchange = async (e) => { const f = e.target.files[0]; if (!f) return; const fmt_ = f.name.toLowerCase().endsWith(".csv") ? "csv" : "jsonl"; await V.guard(null, async () => { const r = await api(`/datasets/import?${V.qs({ format: fmt_, name: f.name.replace(/\.[^.]+$/, "") })}`, { method: "POST", body: f }); V.toast(`Imported ${r.rows_valid} rows (${r.rows_rejected} rejected)`, r.rows_rejected ? "" : "good"); load(); }); };
    clearTimeout(timer); if (jobs.some((j) => ["running", "queued", "paused"].includes(j.status))) timer = setTimeout(() => load().catch(() => {}), 1500);
  };
  async function result(id) {
    const j = await api(`/replays/${id}`); const r = j.result;
    if (!r) return V.toast(j.error ? "Replay failed: " + j.error : `Replay is ${j.status}`);
    const cmp = await api(`/replays/compare?${V.qs({ a: "live", b: id })}`).catch(() => null);
    V.drawer(`<h1>Replay ${esc(id)} ${r.simulated_data ? V.simBadge() : ""}</h1><div class="muted small">dataset sha256 ${esc(r.provenance.dataset_sha256.slice(0, 16))}… · ${fmt(r.events_processed, 0)} events in ${r.wall_seconds}s · ${r.rows_rejected} rows rejected · ${r.late_dropped} late dropped</div>
    <h2 style="margin-top:12px">Detections</h2><div class="pill-row">${Object.entries(r.summary.by_detector).map(([k, v]) => V.badge(`${k}: ${v}`)).join("")}</div>
    <h2 style="margin-top:12px">First detection per series${r.ground_truth ? " vs injected incidents" : ""}</h2>${V.table([{ h: "Series", f: (x) => esc(x[0]) }, { h: "First flagged", f: (x) => V.time(x[1], true) }], Object.entries(r.summary.first_detection).slice(0, 25), { empty: "No detections." })}
    ${r.ground_truth ? `<h2 style="margin-top:12px">Injected incidents (ground truth)</h2>${V.table([{ h: "Kind", f: (g) => esc(g.kind) }, { h: "Start", f: (g) => V.time(g.start, true) }, { h: "End", f: (g) => V.time(g.end) }, { h: "Targets", f: (g) => esc(g.entities.slice(0, 3).join(", ")) }], r.ground_truth)}` : ""}
    ${cmp ? `<h2 style="margin-top:12px">Compared with live timeline</h2><p class="muted small">Live: ${cmp.a.anomalies} anomalies · replay: ${cmp.b.anomalies}. Replays use their own timestamps, so overlap with live data only appears when the same period was observed live.</p>` : ""}
    <h2 style="margin-top:12px">Rejected rows (sample)</h2>${V.table([{ h: "Row", f: (x) => x.row }, { h: "Code", f: (x) => esc(x.code) }, { h: "Message", f: (x) => esc(x.message) }], r.rejected_sample, { empty: "None." })}`);
  }
  await load(); V.cleanup = () => clearTimeout(timer);
};

/* ---------- research lab ---------- */
P.lab = async (el) => {
  let timer; const cmpSel = new Set();
  const load = async () => {
    const [ds, ex, defs] = await Promise.all([api("/datasets"), api("/experiments"), api("/experiments/defaults")]);
    el.innerHTML = `<div class="page-head"><h1>Research lab</h1></div>
    <div class="panel"><h2>New experiment</h2>${V.can("analyst") ? `<div class="bar"><label class="f">Dataset<select id="xds">${ds.map((d) => `<option value="${d.id}">${esc(d.name)} ${d.has_labels ? "(labeled)" : "(no labels)"}</option>`).join("")}</select></label>
    <label class="f">Window (s)<select id="xw"><option>10</option><option selected>30</option><option>60</option></select></label><label class="f">Score after first<select id="xe"><option value="0.2">20%</option><option value="0.3" selected>30%</option><option value="0.5">50%</option></select></label><label class="f">Seed<input id="xs" type="number" value="7" style="width:80px"></label></div>
    <div class="pill-row" id="xd">${defs.detectors.map((d) => `<label><input type="checkbox" value="${d.name}" checked> ${d.name}</label>`).join(" ")}</div><div style="margin-top:10px"><button class="primary" id="xgo" ${ds.length ? "" : "disabled"}>Run experiment</button> <span class="muted small">${ds.length ? "Same windows go to every detector. Scoring starts after the warm-up/training prefix." : "Create a dataset under Historical replay first."}</span></div>` : '<div class="muted">Analyst role or higher required.</div>'}</div>
    <div class="panel" style="margin-top:14px"><div class="pill-row" style="justify-content:space-between"><h2>Experiment history</h2><button id="cmp" ${cmpSel.size >= 2 ? "" : "disabled"}>Compare selected (${cmpSel.size})</button></div>${V.table([{ h: "", f: (r) => `<input type="checkbox" data-x="${r.id}" ${r.status === "completed" ? "" : "disabled"} ${cmpSel.has(r.id) ? "checked" : ""} aria-label="compare ${r.id}">` }, { h: "Experiment", f: (r) => `<b>${esc(r.name)}</b><div class="muted small num">${esc(r.id)} · seed ${r.seed} · v${esc(r.software_version)}</div>` }, { h: "Status", f: (r) => V.st(r.status) + (r.error ? `<div class="small muted">${esc(r.error)}</div>` : "") }, { h: "Finished", f: (r) => V.time(r.finished_at, true) }, { h: "Export", f: (r) => (r.status === "completed" ? ["json", "csv", "md"].map((f) => `<button data-e="${r.id}:${f}">${f}</button>`).join(" ") : "") }], ex, { click: true, empty: "No experiments yet." })}</div><div id="xr"></div>`;
    el.querySelectorAll("[data-e]").forEach((b) => (b.onclick = (e) => { e.stopPropagation(); const [id, f] = b.dataset.e.split(":"); V.guard(b, () => V.download(`/experiments/${id}/export?format=${f}`, `${id}.${f}`)); }));
    el.querySelectorAll("input[data-x]").forEach((c) => (c.onclick = (e) => { e.stopPropagation(); c.checked ? cmpSel.add(c.dataset.x) : cmpSel.delete(c.dataset.x); Q("#cmp", el).disabled = cmpSel.size < 2; Q("#cmp", el).textContent = `Compare selected (${cmpSel.size})`; }));
    Q("#cmp", el).onclick = () => compare([...cmpSel]);
    V.bindRows(el, ex, (r) => r.status === "completed" && show(r.id));
    if (Q("#xgo", el)) Q("#xgo", el).onclick = (e) => V.guard(e.target, async () => { const names = [...el.querySelectorAll("#xd input:checked")].map((c) => c.value); if (!names.length) return V.toast("Select at least one detector"); const dets = defs.detectors.filter((d) => names.includes(d.name)); await api("/experiments", { method: "POST", body: { dataset_id: Q("#xds", el).value, name: "UI experiment " + new Date().toISOString().slice(0, 16), window_s: +Q("#xw", el).value, eval_start_frac: +Q("#xe", el).value, seed: +Q("#xs", el).value, detectors: dets } }); V.toast("Experiment started", "good"); load(); });
    clearTimeout(timer); if (ex.some((x) => ["running", "queued"].includes(x.status))) timer = setTimeout(() => load().catch(() => {}), 2500);
  };
  const metricRow = (n, r) => `<td>${esc(n)}</td><td class="num">${fmt(r.flagged_units, 0)}</td><td class="num">${fmt(r.precision, 3)}</td><td class="num">${fmt(r.recall, 3)}</td><td class="num">${fmt(r.f1, 3)}</td><td class="num">${fmt(r.fpr, 4)}</td><td class="num">${r.incidents_total != null ? `${r.incidents_detected}/${r.incidents_total}` : "n/a"}</td><td class="num">${fmt(r.detection_delay_s?.median, 0)}</td>`;
  async function show(id) {
    const x = await api(`/experiments/${id}`), r = x.results, ev = r.evaluation;
    const d = V.drawer(`<h1>${esc(x.name)}</h1><div class="muted small">dataset ${esc(r.dataset.name)} · sha256 ${esc(r.dataset.sha256.slice(0, 16))}… · ${fmt(r.dataset.events, 0)} events · simulated ${r.dataset.simulated} · config ${esc(r.reproducibility.config_sha256.slice(0, 12))}</div>
    ${ev.labels_available ? "" : '<p class="sev-medium badge">No ground truth: precision, recall and F1 are unavailable and shown as n/a.</p>'}
    <div class="tablewrap"><table><thead><tr><th>Detector</th><th>Flagged</th><th>Precision</th><th>Recall</th><th>F1</th><th>FPR</th><th>Incidents</th><th>Median delay (s)</th></tr></thead><tbody>${Object.entries(ev.detectors).map(([n, v]) => `<tr>${metricRow(n, v)}</tr>`).join("")}</tbody></table></div>
    <h2 style="margin-top:14px">F1 by detector</h2><div id="f1"></div><h2 style="margin-top:14px">Confusion matrices [[TN, FP], [FN, TP]]</h2>${Object.entries(ev.detectors).map(([n, v]) => `<div class="small"><b>${esc(n)}</b> <code>${esc(JSON.stringify(v.confusion_matrix || "n/a"))}</code></div>`).join("")}
    <h2 style="margin-top:14px">Performance (this run)</h2><div class="muted small">aggregation ${fmt(r.performance.aggregation_events_per_s, 0)} events/s on ${esc(r.performance.environment.machine)}, ${r.performance.environment.cpus} CPU, Python ${esc(r.performance.environment.python)}. Per-window detector cost (ms): <code>${esc(JSON.stringify(Object.fromEntries(Object.entries(r.performance.detector_ms_per_window).map(([k, v]) => [k, +v.toFixed(3)]))))}</code></div>
    <h2 style="margin-top:14px">Known limitations</h2><ul>${r.limitations.map((l) => `<li>${esc(l)}</li>`).join("")}</ul>`);
    V.bars(Q("#f1", d), Object.entries(ev.detectors).map(([n, v]) => ({ l: n, v: v.f1 ?? 0, c: "var(--teal)" })), (v) => fmt(v, 3));
  }
  async function compare(ids) {
    const xs = await Promise.all(ids.map((i) => api(`/experiments/${i}`))); const names = [...new Set(xs.flatMap((x) => Object.keys(x.results.evaluation.detectors)))];
    V.drawer(`<h1>Compare experiments</h1><div class="tablewrap"><table><thead><tr><th>Detector</th>${xs.map((x) => `<th>${esc(x.id)}<div class="muted small">${x.config.window_s}s · seed ${x.seed}</div></th>`).join("")}</tr></thead><tbody>${names.map((n) => `<tr><td>${esc(n)}</td>${xs.map((x) => { const v = x.results.evaluation.detectors[n]; return `<td class="num">${v ? `F1 ${fmt(v.f1, 3)}<div class="muted small">P ${fmt(v.precision, 2)} · R ${fmt(v.recall, 2)}</div>` : "n/a"}</td>`; }).join("")}</tr>`).join("")}</tbody></table></div><p class="muted small">Only compare runs on the same dataset hash and scoring settings. Datasets: ${xs.map((x) => esc(x.results.dataset.sha256.slice(0, 8))).join(", ")}.</p>`);
  }
  await load(); V.cleanup = () => clearTimeout(timer);
};

/* ---------- settings ---------- */
P.settings = async (el) => {
  const s = await api("/settings"), admin = V.can("admin"), dis = admin ? "" : "disabled";
  const dets = ["rule", "mad", "zscore", "iqr", "baseline_deviation", "rate_of_change", "isolation_forest"], cur = s.detectors.map((d) => d.name);
  el.innerHTML = `<div class="page-head"><h1>Settings</h1>${admin ? "" : '<span class="muted">Read-only: admin role required to change settings.</span>'}</div>
  <div class="grid g2"><div class="panel"><h2>Workspace</h2><form id="sf" style="display:grid;gap:10px"><label class="f">Name<input name="display_name" value="${esc(s.workspace.name)}" ${dis}></label>
  <label class="f">Default time range<select name="default_time_range" ${dis}>${["15m", "1h", "6h", "24h"].map((r) => `<option ${r === s.default_time_range ? "selected" : ""}>${r}</option>`).join("")}</select></label>
  <label class="f">Time display<select name="timezone" ${dis}><option value="UTC" ${s.timezone === "UTC" ? "selected" : ""}>UTC</option><option value="local" ${s.timezone === "local" ? "selected" : ""}>Browser local time</option></select></label>
  <label class="f">Event retention (days)<input name="retention_days" type="number" min="1" max="365" value="${s.retention_days}" ${dis}></label><button class="primary" ${dis}>Save workspace settings</button></form></div>
  <div class="panel"><h2>Detection and alert policy</h2><form id="df" style="display:grid;gap:10px"><label class="f">Window size<select name="window_s" ${dis}>${[10, 15, 30, 60].map((w) => `<option ${w === s.window_s ? "selected" : ""}>${w}</option>`).join("")}</select></label>
  <div class="f">Active detectors<div class="pill-row">${dets.map((d) => `<label><input type="checkbox" value="${d}" ${cur.includes(d) ? "checked" : ""} ${dis}> ${d}</label>`).join(" ")}</div></div>
  <label class="f">Minimum severity to open an alert<select name="min_severity" ${dis}>${["low", "medium", "high", "critical"].map((x) => `<option ${x === s.alert_policy.min_severity ? "selected" : ""}>${x}</option>`).join("")}</select></label>
  <label class="f">Consecutive windows required (below high severity)<input name="consecutive_required" type="number" min="1" max="10" value="${s.alert_policy.consecutive_required}" ${dis}></label><label class="f">Cooldown after resolve (s)<input name="cooldown_s" type="number" min="0" value="${s.alert_policy.cooldown_s}" ${dis}></label>
  <button class="primary" ${dis}>Save and restart processor</button><span class="muted small">Restarting rebuilds detector baselines from stored windows.</span></form></div></div>
  <div class="panel" style="margin-top:14px" id="dest"></div><div class="panel" style="margin-top:14px" id="aud"></div>`;
  Q("#sf", el).onsubmit = (e) => { e.preventDefault(); const f = new FormData(e.target); V.guard(Q("button", e.target), async () => { const r = await api("/settings", { method: "PATCH", body: { display_name: f.get("display_name"), default_time_range: f.get("default_time_range"), timezone: f.get("timezone"), retention_days: +f.get("retention_days") } }); V.tz = r.timezone === "local" ? "local" : "UTC"; V.wsName = f.get("display_name"); V.toast("Saved", "good"); V.shell(); }); };
  Q("#df", el).onsubmit = (e) => { e.preventDefault(); const f = new FormData(e.target), names = [...e.target.querySelectorAll("input[type=checkbox]:checked")].map((c) => c.value); if (!names.length) return V.toast("Select at least one detector", "bad"); V.guard(Q("button", e.target), async () => { await api("/settings", { method: "PATCH", body: { window_s: +f.get("window_s"), detectors: names.map((n) => (n === "isolation_forest" ? { name: n, config: { min_train: 40 } } : { name: n })), alert_policy: { min_severity: f.get("min_severity"), consecutive_required: +f.get("consecutive_required"), cooldown_s: +f.get("cooldown_s") } } }); V.toast("Saved. Processor restarted.", "good"); }); };
  if (admin) {
    const dl = async () => {
      const [ds, dv] = await Promise.all([api("/destinations"), api("/deliveries")]);
      Q("#dest", el).innerHTML = `<div class="pill-row" style="justify-content:space-between"><h2>Notification destinations</h2><button id="nd">Add webhook</button></div>${V.table([{ h: "Name", f: (r) => esc(r.name) }, { h: "Kind", f: (r) => esc(r.kind) }, { h: "Min severity", f: (r) => V.sev(r.min_severity) }, { h: "Target", f: (r) => `<code>${esc(r.config.url || r.config.to || "")}</code>` }, { h: "", f: (r) => `<button data-t="${r.id}">Send test</button> <button class="danger" data-d="${r.id}">Delete</button>` }], ds, { empty: "No destinations. In-app notifications are always on." })}<h3 style="margin-top:12px">Recent deliveries</h3>${V.table([{ h: "Time", f: (r) => V.time(r.ts, true) }, { h: "Destination", f: (r) => esc(r.destination) }, { h: "Status", f: (r) => V.st(r.status) }, { h: "Attempts", f: (r) => r.attempts }, { h: "Detail", f: (r) => esc(r.error || (r.http_status ? "HTTP " + r.http_status : "")) }], dv.slice(0, 8), { empty: "None yet." })}`;
      Q("#nd", el).onclick = () => { const d = V.dialog(`<h2>Add webhook</h2><form id="wf" style="display:grid;gap:10px"><label class="f">Name<input name="name" required></label><label class="f">URL (http or https)<input name="url" type="url" required></label><label class="f">Minimum severity<select name="min_severity"><option>medium</option><option selected>high</option><option>critical</option></select></label><p class="muted small">Private and loopback addresses are blocked unless the server enables VEYLSTREAM_ALLOW_PRIVATE_WEBHOOKS.</p><div class="pill-row" style="justify-content:flex-end"><button type="button" id="x">Cancel</button><button class="primary">Add</button></div></form>`); Q("#x", d).onclick = () => d.close(); Q("#wf", d).onsubmit = (e) => { e.preventDefault(); const f = new FormData(e.target); V.guard(Q(".primary", d), async () => { await api("/destinations", { method: "POST", body: { kind: "webhook", name: f.get("name"), min_severity: f.get("min_severity"), config: { url: f.get("url") } } }); d.close(); dl(); }); }; };
      el.querySelectorAll("[data-t]").forEach((b) => (b.onclick = () => V.guard(b, async () => { const r = await api(`/destinations/${b.dataset.t}/test`, { method: "POST", body: {} }); V.toast(`Test ${r.status}${r.error ? ": " + r.error : ""}${r.http_status ? " (HTTP " + r.http_status + ")" : ""}`, r.status === "delivered" ? "good" : "bad"); dl(); })));
      el.querySelectorAll("[data-d]").forEach((b) => (b.onclick = async () => { if (await V.confirm("Delete destination", "Alerts will stop being sent there.", "Delete", true)) V.guard(b, async () => { await api(`/destinations/${b.dataset.d}`, { method: "DELETE" }); dl(); }); }));
    };
    dl().catch((e) => (Q("#dest", el).innerHTML = `<div class="err">${esc(e.message)}</div>`));
    const au = await api("/audit"); Q("#aud", el).innerHTML = `<h2>Audit log</h2>${V.table([{ h: "Time", f: (r) => V.time(r.ts, true) }, { h: "Actor", f: (r) => esc(r.actor) }, { h: "Action", f: (r) => esc(r.action) }, { h: "Target", f: (r) => `<code>${esc(r.target || "")}</code>` }], au.slice(0, 30), { empty: "No audit events." })}`;
  } else { Q("#dest", el).innerHTML = '<h2>Notification destinations</h2><div class="muted">Admin role required.</div>'; Q("#aud", el).innerHTML = ""; }
};
})();
