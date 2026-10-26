"use strict";
/* Veylstream UI core. No build step, no external requests. All dynamic text goes through esc(). */
const V = {
  token: sessionStorage.getItem("vs_token"), ws: sessionStorage.getItem("vs_ws"), user: null, role: "viewer", wsName: "",
  range: sessionStorage.getItem("vs_range") || "1h", tz: "UTC", pages: {}, live: { status: "connecting", handlers: new Set(), lastStats: null },
  RANGES: { "15m": 900, "1h": 3600, "6h": 21600, "24h": 86400 },
  ROLES: { viewer: 0, analyst: 1, operator: 2, admin: 3 },
};
const $ = (s, el = document) => el.querySelector(s);
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const can = (role) => V.ROLES[V.role] >= V.ROLES[role];
V.esc = esc; V.can = can;

/* ---------- formatting ---------- */
V.fmt = (v, d = 2) => (v == null || Number.isNaN(v) ? "n/a" : Math.abs(v) >= 1000 ? Math.round(v).toLocaleString() : Number(v).toFixed(Math.abs(v) < 10 ? d : 1));
V.pct = (v) => (v == null ? "n/a" : (v * 100).toFixed(v < 0.1 ? 2 : 1) + "%");
V.time = (t, full) => {
  if (t == null) return "n/a";
  const d = new Date(t * 1000), u = V.tz === "UTC";
  const p = (n) => String(n).padStart(2, "0");
  const hh = u ? d.getUTCHours() : d.getHours(), mm = u ? d.getUTCMinutes() : d.getMinutes(), ss = u ? d.getUTCSeconds() : d.getSeconds();
  const day = u ? d.toISOString().slice(0, 10) : `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}`;
  return (full ? day + " " : "") + `${p(hh)}:${p(mm)}:${p(ss)}` + (full && u ? " UTC" : "");
};
V.ago = (t) => { if (!t) return "never"; const s = Date.now() / 1000 - t; return s < 90 ? `${Math.round(s)}s ago` : s < 5400 ? `${Math.round(s / 60)}m ago` : `${Math.round(s / 3600)}h ago`; };
V.badge = (txt, cls) => `<span class="badge ${cls || ""}">${esc(txt)}</span>`;
V.sev = (s) => V.badge(s, "sev-" + s);
V.st = (s) => V.badge(s, "st-" + s);
V.simBadge = () => V.badge("simulated", "sim");

/* ---------- api ---------- */
V.api = async function (path, opts = {}) {
  const url = path.startsWith("/api") ? path : `/api/v1/workspaces/${V.ws}${path}`;
  const headers = { ...(opts.headers || {}) };
  if (V.token) headers.Authorization = "Bearer " + V.token;
  let body = opts.body;
  if (body && typeof body === "object" && !(body instanceof Blob) && typeof body !== "string") { headers["Content-Type"] = "application/json"; body = JSON.stringify(body); }
  let res;
  try { res = await fetch(url, { method: opts.method || (body ? "POST" : "GET"), headers, body }); }
  catch (e) { throw new Error("Cannot reach the Veylstream API. Is the server running?"); }
  if (opts.raw) return res;
  const j = await res.json().catch(() => ({}));
  if (res.status === 401 && !path.includes("/auth/login")) { V.logout(true); throw new Error("Session expired"); }
  if (!res.ok) { const e = new Error(j.error?.message || `HTTP ${res.status}`); e.code = j.error?.code; e.status = res.status; throw e; }
  return opts.full ? j : j.data;
};
V.toast = (msg, kind) => { const t = document.createElement("div"); t.className = "toast " + (kind || ""); t.textContent = msg; $("#toasts").append(t); setTimeout(() => t.remove(), kind === "bad" ? 7000 : 3500); };
V.guard = async (btn, fn) => {
  if (btn) { btn.disabled = true; btn.classList.add("busy"); }
  try { return await fn(); } catch (e) { V.toast(e.message, "bad"); } finally { if (btn) { btn.disabled = false; btn.classList.remove("busy"); } }
};
V.confirm = (title, text, okLabel = "Confirm", danger = false) => new Promise((res) => {
  const d = $("#dlg");
  d.innerHTML = `<h2>${esc(title)}</h2><p class="muted">${esc(text)}</p><div class="pill-row" style="justify-content:flex-end;margin-top:14px"><button id="c-no">Cancel</button><button id="c-yes" class="${danger ? "danger" : "primary"}">${esc(okLabel)}</button></div>`;
  d.showModal();
  $("#c-no").onclick = () => { d.close(); res(false); }; $("#c-yes").onclick = () => { d.close(); res(true); }; d.oncancel = () => res(false);
});
V.dialog = (html, onMount) => { const d = $("#dlg"); d.innerHTML = html; d.showModal(); onMount && onMount(d); return d; };
V.drawer = (html, onClose) => {
  V.closeDrawer();
  const s = document.createElement("div"); s.className = "scrim"; s.id = "scrim";
  const d = document.createElement("div"); d.className = "drawer"; d.id = "drawer"; d.setAttribute("role", "dialog"); d.setAttribute("aria-modal", "true");
  d.innerHTML = `<div class="pill-row" style="justify-content:flex-end"><button class="ghost" id="dclose" aria-label="Close panel">Close ✕</button></div>${html}`;
  document.body.append(s, d); V._dclose = onClose;
  const close = () => V.closeDrawer(); s.onclick = close; $("#dclose").onclick = close;
  d.tabIndex = -1; d.focus();
  return d;
};
V.closeDrawer = () => { $("#drawer")?.remove(); $("#scrim")?.remove(); if (V._dclose) { V._dclose(); V._dclose = null; } };
document.addEventListener("keydown", (e) => { if (e.key === "Escape") V.closeDrawer(); });

/* ---------- table / pager helpers ---------- */
V.table = (cols, rows, opts = {}) => {
  if (!rows.length) return `<div class="empty">${esc(opts.empty || "Nothing here yet.")}</div>`;
  return `<div class="tablewrap"><table><thead><tr>${cols.map((c) => `<th>${esc(c.h)}</th>`).join("")}</tr></thead><tbody>${rows.map((r, i) =>
    `<tr class="${opts.click ? "click" : ""}" data-i="${i}" ${opts.click ? 'tabindex="0"' : ""}>${cols.map((c) => `<td class="${c.cls || ""}">${c.f(r)}</td>`).join("")}</tr>`).join("")}</tbody></table></div>`;
};
V.bindRows = (el, rows, fn) => el.querySelectorAll("tr.click").forEach((tr) => { const go = () => fn(rows[+tr.dataset.i], tr); tr.onclick = go; tr.onkeydown = (e) => { if (e.key === "Enter") go(); }; });
V.pager = (meta, onPage) => {
  const pages = Math.max(1, Math.ceil((meta.total || 0) / meta.page_size));
  return `<div class="pill-row" style="justify-content:flex-end;align-items:center;margin-top:8px"><span class="muted small">${(meta.total || 0).toLocaleString()} results · page ${meta.page} of ${pages}</span>
  <button data-p="${meta.page - 1}" ${meta.page <= 1 ? "disabled" : ""}>Prev</button><button data-p="${meta.page + 1}" ${meta.page >= pages ? "disabled" : ""}>Next</button></div>`;
};
V.bindPager = (el, onPage) => el.querySelectorAll("[data-p]").forEach((b) => (b.onclick = () => onPage(+b.dataset.p)));
V.qs = (o) => Object.entries(o).filter(([, v]) => v !== "" && v != null).map(([k, v]) => `${k}=${encodeURIComponent(v)}`).join("&");
V.loading = (el, n = 3) => (el.innerHTML = `<div class="panel muted">Loading…</div>`);
V.download = async (path, name) => {
  const res = await V.api(path, { raw: true });
  if (!res.ok) throw new Error("Export failed: HTTP " + res.status);
  const url = URL.createObjectURL(await res.blob()); const a = document.createElement("a"); a.href = url; a.download = name; a.click(); URL.revokeObjectURL(url);
};

/* ---------- charts (SVG) ---------- */
const COLORS = ["#22d3ee", "#a78bfa", "#34d399", "#fbbf24", "#f87171", "#60a5fa"];
V.COLORS = COLORS;
/** lineChart(el, {series:[{name,color,points:[[t,v]]}], band:[[t,lo,hi]], markers:[{t,v,sev,label}], height, unit, fmt}) */
V.lineChart = (el, o) => {
  const W = 760, H = o.height || 190, m = { l: 46, r: 10, t: 8, b: 22 };
  const all = o.series.flatMap((s) => s.points.filter((p) => p[1] != null));
  if (!all.length) { el.innerHTML = `<div class="empty">${esc(o.empty || "No data in this range yet.")}</div>`; return; }
  const ts = all.map((p) => p[0]).concat(o.markers?.map((x) => x.t) || []);
  let t0 = Math.min(...ts), t1 = Math.max(...ts); if (t1 === t0) t1 = t0 + 1;
  let vals = all.map((p) => p[1]); (o.band || []).forEach((b) => b[1] != null && vals.push(b[1], b[2]));
  let lo = o.min ?? Math.min(0, ...vals), hi = Math.max(...vals) * 1.08 || 1; if (hi === lo) hi = lo + 1;
  const x = (t) => m.l + ((t - t0) / (t1 - t0)) * (W - m.l - m.r), y = (v) => H - m.b - ((v - lo) / (hi - lo)) * (H - m.t - m.b);
  const f = o.fmt || V.fmt;
  let g = "";
  for (let i = 0; i <= 4; i++) { const v = lo + ((hi - lo) * i) / 4; g += `<line x1="${m.l}" x2="${W - m.r}" y1="${y(v)}" y2="${y(v)}" stroke="var(--line)"/><text x="${m.l - 6}" y="${y(v) + 4}" text-anchor="end" fill="var(--faint)" font-size="10" font-family="var(--mono)">${esc(f(v))}</text>`; }
  for (let i = 0; i <= 4; i++) { const t = t0 + ((t1 - t0) * i) / 4; g += `<text x="${x(t)}" y="${H - 6}" text-anchor="middle" fill="var(--faint)" font-size="10" font-family="var(--mono)">${esc(V.time(t))}</text>`; }
  let band = "";
  if (o.band?.length) { const b = o.band.filter((p) => p[1] != null); if (b.length) band = `<path d="M${b.map((p) => `${x(p[0])},${y(p[2])}`).join("L")}L${b.slice().reverse().map((p) => `${x(p[0])},${y(p[1])}`).join("L")}Z" fill="var(--violet)" opacity=".14"/>`; }
  const lines = o.series.map((s, i) => { const c = s.color || COLORS[i % 6]; let d = "", pen = false; s.points.forEach((p) => { if (p[1] == null) { pen = false; return; } d += `${pen ? "L" : "M"}${x(p[0]).toFixed(1)},${y(p[1]).toFixed(1)}`; pen = true; }); return `<path d="${d}" fill="none" stroke="${c}" stroke-width="1.8" stroke-linejoin="round"/>`; }).join("");
  const sevc = { critical: "#f87171", high: "#fb923c", medium: "#fbbf24", low: "#60a5fa" };
  const marks = (o.markers || []).map((k) => `<circle cx="${x(k.t)}" cy="${y(k.v ?? lo)}" r="4.5" fill="${sevc[k.sev] || "#fbbf24"}" stroke="var(--bg1)" stroke-width="1.5"><title>${esc(k.label || "")}</title></circle>`).join("");
  el.innerHTML = `<div class="chart"><svg viewBox="0 0 ${W} ${H}" role="img" aria-label="${esc(o.label || "time series chart")}">${g}${band}${lines}${marks}<line class="xh" y1="${m.t}" y2="${H - m.b}" stroke="var(--line2)" visibility="hidden"/><rect class="hit" x="${m.l}" y="0" width="${W - m.l - m.r}" height="${H}" fill="transparent"/></svg><div class="tip" hidden></div></div>` +
    (o.series.length > 1 || o.band ? `<div class="legend">${o.series.map((s, i) => `<span><i style="background:${s.color || COLORS[i % 6]}"></i>${esc(s.name)}</span>`).join("")}${o.band ? '<span><i style="background:var(--violet);opacity:.5"></i>expected range</span>' : ""}</div>` : "");
  const svg = $("svg", el), tip = $(".tip", el), xh = $(".xh", el);
  $(".hit", el).onmousemove = (e) => {
    const r = svg.getBoundingClientRect(), px = ((e.clientX - r.left) / r.width) * W, t = t0 + ((px - m.l) / (W - m.l - m.r)) * (t1 - t0);
    let best = null; o.series[0].points.forEach((p) => { if (!best || Math.abs(p[0] - t) < Math.abs(best[0] - t)) best = p; });
    if (!best) return;
    xh.setAttribute("x1", x(best[0])); xh.setAttribute("x2", x(best[0])); xh.setAttribute("visibility", "visible");
    tip.hidden = false; tip.style.left = Math.min(r.width - 150, Math.max(0, (x(best[0]) / W) * r.width + 10)) + "px"; tip.style.top = "4px";
    tip.innerHTML = `<b>${esc(V.time(best[0], true))}</b>` + o.series.map((s, i) => { const p = s.points.find((q) => q[0] === best[0]); return `<div><span style="color:${s.color || COLORS[i % 6]}">●</span> ${esc(s.name)}: ${esc(f(p?.[1]))}</div>`; }).join("");
  };
  $(".hit", el).onmouseleave = () => { tip.hidden = true; xh.setAttribute("visibility", "hidden"); };
};
V.bars = (el, items, fmt = V.fmt) => {
  if (!items.length) { el.innerHTML = '<div class="empty">No data.</div>'; return; }
  const mx = Math.max(...items.map((i) => i.v || 0)) || 1;
  el.innerHTML = items.map((i) => `<div style="margin:7px 0"><div class="pill-row" style="justify-content:space-between"><span>${esc(i.l)}</span><span class="num muted">${esc(fmt(i.v))}</span></div><div class="hbar"><i style="width:${Math.max(1, ((i.v || 0) / mx) * 100)}%;background:${i.c || "var(--cyan)"}"></i></div></div>`).join("");
};

/* ---------- live stream (fetch-based SSE so the bearer token never goes in a URL) ---------- */
V.connectLive = async function () {
  if (V._liveAbort) V._liveAbort.abort();
  const ac = (V._liveAbort = new AbortController()); let delay = 1000;
  while (!ac.signal.aborted && V.token && V.ws) {
    try {
      setLive("connecting");
      const res = await fetch(`/api/v1/workspaces/${V.ws}/live`, { headers: { Authorization: "Bearer " + V.token }, signal: ac.signal });
      if (!res.ok) throw new Error("HTTP " + res.status);
      setLive("live"); delay = 1000;
      const rd = res.body.getReader(), dec = new TextDecoder(); let buf = "";
      for (;;) {
        const { done, value } = await rd.read(); if (done) break;
        buf += dec.decode(value, { stream: true }); let i;
        while ((i = buf.indexOf("\n\n")) >= 0) { const chunk = buf.slice(0, i); buf = buf.slice(i + 2); const line = chunk.split("\n").find((l) => l.startsWith("data: ")); if (line) { try { const m = JSON.parse(line.slice(6)); if (m.type === "stats") V.live.lastStats = m.data; V.live.handlers.forEach((h) => h(m)); } catch (_) { /* ignore malformed frame */ } } }
      }
    } catch (e) { if (ac.signal.aborted) return; }
    setLive("disconnected"); await new Promise((r) => setTimeout(r, delay)); delay = Math.min(delay * 2, 15000);
  }
};
function setLive(s) { V.live.status = s; const el = $("#livei"); if (el) el.innerHTML = `<span class="dot ${s === "live" ? "on" : s === "connecting" ? "wait" : "off"}"></span> ${s === "live" ? "Live" : s === "connecting" ? "Connecting" : "Disconnected, retrying"}`; }

/* ---------- shell / router ---------- */
const NAV = [
  ["Monitor", [["overview", "Overview", "M3 12l4-8 4 14 3-9 2 3h5"], ["streams", "Live streams", "M3 6h18M3 12h12M3 18h7"], ["analytics", "Analytics", "M4 20V10M10 20V4M16 20v-8M22 20H2"]]],
  ["Investigate", [["anomalies", "Anomalies", "M12 3l9 16H3z"], ["alerts", "Alerts and incidents", "M6 9a6 6 0 0112 0v5l2 3H4l2-3zM10 20h4"], ["events", "Event explorer", "M4 5h16M4 10h16M4 15h10M4 20h6"]]],
  ["Platform", [["pipeline", "Pipelines", "M3 12h4l2-5 4 10 2-5h6"], ["quality", "Data quality", "M5 12l4 4 10-10"], ["sources", "Data sources", "M4 7c0-2 16-2 16 0s-16 2-16 0v10c0 2 16 2 16 0V7"]]],
  ["Lab", [["replay", "Historical replay", "M4 4v6h6M4 10a8 8 0 112 6"], ["lab", "Research lab", "M9 3h6M10 3v6l-5 9a2 2 0 002 3h10a2 2 0 002-3l-5-9V3"]]],
  ["Workspace", [["settings", "Settings", "M12 8a4 4 0 100 8 4 4 0 000-8zM3 12h2m14 0h2M12 3v2m0 14v2"]]],
];
V.route = () => (location.hash.replace(/^#\/?/, "").split("?")[0] || "overview");
V.shell = function () {
  const r = V.route(), label = NAV.flatMap((g) => g[1]).find((n) => n[0] === r)?.[1] || "Overview";
  $("#app").innerHTML = `<div class="shell ${V.collapsed ? "collapsed" : ""}" id="shell"><aside aria-label="Primary"><div class="brand"><svg width="24" height="24" viewBox="0 0 32 32" aria-hidden="true"><path d="M2 18h6l3-8 4 16 3-12 2 4h10" fill="none" stroke="#22d3ee" stroke-width="2.6" stroke-linecap="round" stroke-linejoin="round"/><circle cx="15" cy="26" r="2.6" fill="#f87171"/></svg><span>veylstream</span></div>
  <nav>${NAV.map(([g, items]) => `<div class="navgrp">${g}</div>` + items.map(([id, t, p]) => `<a href="#/${id}" ${id === r ? 'aria-current="page"' : ""} title="${esc(t)}"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="${p}"/></svg><span>${esc(t)}</span></a>`).join("")).join("")}</nav>
  <div class="grow"></div><div style="padding:12px 16px" class="faint small"><span>See the signal. Detect the unexpected.</span></div></aside>
  <main id="main"><header class="top"><button class="ghost" id="navtoggle" aria-label="Toggle navigation">☰</button>
   <div class="crumb"><span>${esc(V.wsName)}</span> / <b>${esc(label)}</b></div><div class="grow"></div>
   <input id="gsearch" type="search" placeholder="Search anomalies…" aria-label="Search anomalies" style="width:200px">
   <select id="range" aria-label="Time range">${Object.keys(V.RANGES).map((k) => `<option ${k === V.range ? "selected" : ""} value="${k}">Last ${k}</option>`).join("")}</select>
   <span id="livei" class="small muted" style="min-width:110px"></span>
   <button class="ghost" id="bell" aria-label="Notifications">🔔<span id="bellc"></span></button>
   <button class="ghost" id="theme" aria-label="Toggle light or dark theme">◐</button>
   <button class="ghost" id="usermenu" title="Sign out">${esc(V.user?.display_name || "")} (${esc(V.role)}) ⎋</button></header>
   <div class="content" id="page"></div></main></div>`;
  setLive(V.live.status);
  $("#navtoggle").onclick = () => { if (window.innerWidth < 1000) $("#shell").classList.toggle("navopen"); else { V.collapsed = !V.collapsed; $("#shell").classList.toggle("collapsed"); } };
  $("#range").onchange = (e) => { V.range = e.target.value; sessionStorage.setItem("vs_range", V.range); V.render(); };
  $("#gsearch").onkeydown = (e) => { if (e.key === "Enter") { V.searchQ = e.target.value; location.hash = "#/anomalies"; V.render(); } };
  $("#usermenu").onclick = async () => { if (await V.confirm("Sign out", "End this session?", "Sign out")) V.logout(); };
  $("#theme").onclick = () => { const t = document.documentElement.dataset.theme === "dark" ? "light" : "dark"; document.documentElement.dataset.theme = t; localStorage.setItem("vs_theme", t); };
  $("#bell").onclick = notifications; refreshBell();
};
async function refreshBell() { try { const j = await V.api("/notifications", { full: true }); const n = j.meta.unread; $("#bellc") && ($("#bellc").textContent = n ? ` ${n}` : ""); V._notes = j.data; } catch (_) { /* bell is best-effort */ } }
async function notifications() {
  const rows = V._notes || [];
  V.drawer(`<h1>Notifications</h1>${rows.length ? rows.map((n) => `<div class="panel" style="margin:8px 0"><div>${V.sev(n.severity)} <b>${esc(n.title)}</b></div><div class="muted small">${esc(V.time(n.ts, true))}</div><div>${esc(n.body || "")}</div>${n.alert_id ? `<a href="#/alerts" data-a="${n.alert_id}">Open alerts</a>` : ""}</div>`).join("") : '<div class="empty">No notifications yet. They appear when alerts open or escalate.</div>'}<button id="markread">Mark all read</button>`);
  $("#markread").onclick = async () => { await V.api("/notifications/read", { method: "POST", body: {} }); V.closeDrawer(); refreshBell(); };
}
V.render = async function () {
  if (!V.token) return V.loginView();
  if (!$("#shell")) V.shell(); else { V.shell(); }
  const id = V.route(), page = V.pages[id] || V.pages.overview, el = $("#page");
  V.cleanup && V.cleanup(); V.cleanup = null; V.live.handlers.clear();
  try { await page(el); } catch (e) { el.innerHTML = `<div class="panel err">${esc(e.message)}<br><button id="retry" style="margin-top:10px">Retry</button></div>`; $("#retry").onclick = V.render; }
};
V.logout = (expired) => { V._liveAbort?.abort(); if (V.token && !expired) fetch("/api/v1/auth/logout", { method: "POST", headers: { Authorization: "Bearer " + V.token } }).catch(() => {}); sessionStorage.clear(); V.token = null; V.ws = null; V.loginView(expired ? "Your session expired. Sign in again." : ""); };
V.loginView = function (msg) {
  $("#app").innerHTML = `<div class="login"><div class="panel"><div class="brand" style="padding:0 0 14px"><svg width="28" height="28" viewBox="0 0 32 32"><path d="M2 18h6l3-8 4 16 3-12 2 4h10" fill="none" stroke="#22d3ee" stroke-width="2.6" stroke-linecap="round" stroke-linejoin="round"/><circle cx="15" cy="26" r="2.6" fill="#f87171"/></svg><span style="font-size:20px">veylstream</span></div>
  <p class="muted" style="margin-top:0">Real-time intelligence and anomaly detection. See the signal. Detect the unexpected.</p>
  <form id="lf" style="display:grid;gap:12px"><label class="f">Email<input name="email" type="email" required autocomplete="username"></label><label class="f">Password<input name="password" type="password" required autocomplete="current-password"></label>
  <div class="err" id="lerr" style="padding:0;text-align:left">${esc(msg || "")}</div><button class="primary" type="submit">Sign in</button></form>
  <p class="faint small">There is no default account. Create one with <code>python -m veylstream setup-demo</code>.</p></div></div>`;
  $("#lf").onsubmit = async (e) => {
    e.preventDefault(); const f = new FormData(e.target), b = $("button", e.target);
    await V.guard(b, async () => {
      try { const d = await V.api("/api/v1/auth/login", { method: "POST", body: { email: f.get("email"), password: f.get("password") } }); V.token = d.token; V.user = d.user; const w = d.workspaces[0]; if (!w) throw new Error("This account has no workspace membership."); V.ws = w.id; sessionStorage.setItem("vs_token", V.token); sessionStorage.setItem("vs_ws", V.ws); await V.boot(); }
      catch (err) { $("#lerr").textContent = err.message; }
    });
  };
};
V.boot = async function () {
  document.documentElement.dataset.theme = localStorage.getItem("vs_theme") || "dark";
  if (!V.token) return V.loginView();
  try {
    const me = await V.api("/api/v1/auth/me"); V.user = me.user; const w = me.workspaces.find((x) => x.id === V.ws) || me.workspaces[0];
    if (!w) throw new Error("no workspace"); V.ws = w.id; V.role = w.role; V.wsName = w.name;
    const s = await V.api("/settings"); V.tz = s.timezone === "local" ? "local" : "UTC"; if (!sessionStorage.getItem("vs_range") && s.default_time_range) V.range = s.default_time_range; V.settings = s;
  } catch (e) { return V.loginView(e.message === "Session expired" ? "" : e.message); }
  window.onhashchange = () => V.render(); V.connectLive(); V.render();
};
V.rangeWindow = () => { const to = Date.now() / 1000 + 60; return { from: to - V.RANGES[V.range], to }; };
