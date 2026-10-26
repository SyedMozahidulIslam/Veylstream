"""REST API (Flask) under /api/v1. Envelope: {"data":..., "meta":..., "request_id":...}; errors: {"error":{code,message},"request_id":...}.

Auth: user sessions use ``Authorization: Bearer <token>``; producers use ``X-API-Key``. Every workspace route
checks membership (non-members get 404, so IDs cannot be probed) and a minimum role.
"""
from __future__ import annotations

import csv
import io
import json
import os
import queue
import secrets
import time
from collections import defaultdict, deque
from functools import wraps
from typing import Any, Callable

from werkzeug.exceptions import HTTPException
from flask import Flask, Response, g, jsonify, request, send_from_directory

from . import __version__
from .alerts import InvalidTransition
from .contracts import parse_ts, schema_description
from .datasets import create_synthetic_dataset, import_dataset
from .experiments import DEFAULT_SPECS, research_summary, to_csv
from .generator import Scenario
from .pipeline import Backpressure, RateLimited
from .replay import compare_scopes
from .runtime import DEFAULT_SETTINGS, ROLE_RANK, SETTING_KEYS, Runtime
from .detectors import SEVERITIES, SEV_RANK

WEB_DIR = os.path.join(os.path.dirname(__file__), "web")


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str, details: Any = None) -> None:
        super().__init__(message)
        self.status, self.code, self.message, self.details = status, code, message, details


def _num(name: str, default: float | None = None) -> float | None:
    v = request.args.get(name)
    if v is None or v == "":
        return default
    t = parse_ts(v)
    if t is None:
        raise ApiError(400, "invalid_parameter", f"{name} must be epoch seconds or ISO-8601")
    return t


def _page() -> tuple[int, int]:
    try:
        page = max(1, int(request.args.get("page", 1)))
        size = min(200, max(1, int(request.args.get("page_size", 50))))
    except ValueError:
        raise ApiError(400, "invalid_parameter", "page and page_size must be integers")
    return page, size


def _sort(allowed: dict[str, str], default: str) -> str:
    key = request.args.get("sort", default)
    desc = key.startswith("-")
    col = allowed.get(key.lstrip("-"))
    if col is None:
        raise ApiError(400, "invalid_parameter", f"sort must be one of {sorted(allowed)} (prefix '-' for descending)")
    return f"{col} {'DESC' if desc else 'ASC'}"


def _body() -> dict[str, Any]:
    b = request.get_json(silent=True)
    if not isinstance(b, dict):
        raise ApiError(400, "invalid_body", "JSON object body required")
    return b


def create_app(rt: Runtime) -> Flask:
    app = Flask(__name__, static_folder=None)
    app.config["MAX_CONTENT_LENGTH"] = rt.cfg.max_body_bytes
    store = rt.store
    login_attempts: dict[str, deque] = defaultdict(lambda: deque(maxlen=20))

    # ---- plumbing -------------------------------------------------------------------------------
    @app.before_request
    def _before() -> None:
        g.rid = request.headers.get("X-Request-ID", "")[:64] or secrets.token_hex(8)
        g.t0 = time.perf_counter()

    @app.after_request
    def _after(resp: Response) -> Response:
        resp.headers["X-Request-ID"] = g.get("rid", "")
        resp.headers["X-Content-Type-Options"] = "nosniff"
        resp.headers["X-Frame-Options"] = "DENY"
        resp.headers["Referrer-Policy"] = "no-referrer"
        if not request.path.startswith("/api/"):
            resp.headers["Content-Security-Policy"] = "default-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'"
        origin = request.headers.get("Origin")
        if origin and origin in rt.cfg.cors_origins:
            resp.headers["Access-Control-Allow-Origin"] = origin
            resp.headers["Access-Control-Allow-Headers"] = "Authorization, Content-Type, X-API-Key, X-Request-ID"
            resp.headers["Access-Control-Allow-Methods"] = "GET, POST, PATCH, DELETE, OPTIONS"
            resp.headers["Vary"] = "Origin"
        return resp

    @app.errorhandler(ApiError)
    def _api_error(e: ApiError):
        r = jsonify({"error": {"code": e.code, "message": e.message, "details": e.details}, "request_id": g.get("rid")})
        r.status_code = e.status
        return r

    @app.errorhandler(HTTPException)
    def _http(e: HTTPException):
        code = {404: "not_found", 405: "method_not_allowed", 413: "payload_too_large", 400: "bad_request", 415: "unsupported_media_type"}.get(e.code or 500, "http_error")
        msg = f"request body exceeds {rt.cfg.max_body_bytes} bytes" if e.code == 413 else (e.description or code)
        return _api_error(ApiError(e.code or 500, code, msg))

    @app.errorhandler(Exception)
    def _500(e: Exception):
        app.logger.exception("unhandled error rid=%s", g.get("rid"))
        return _api_error(ApiError(500, "internal_error", "internal server error"))

    def ok(data: Any, meta: dict | None = None, status: int = 200):
        r = jsonify({"data": data, "meta": meta or {}, "request_id": g.get("rid")})
        r.status_code = status
        return r

    def bearer() -> str | None:
        h = request.headers.get("Authorization", "")
        return h[7:] if h.startswith("Bearer ") else None

    def secured(role: str = "viewer") -> Callable:
        def deco(fn: Callable) -> Callable:
            @wraps(fn)
            def wrapper(ws: str, *a: Any, **kw: Any):
                tok = bearer()
                user = rt.user_for_token(tok) if tok else None
                if user is None:
                    raise ApiError(401, "unauthenticated", "valid bearer token required")
                r = rt.role(user["id"], ws)
                if r is None:
                    raise ApiError(404, "not_found", "workspace not found")
                if ROLE_RANK[r] < ROLE_RANK[role]:
                    raise ApiError(403, "forbidden", f"requires role {role} or higher")
                g.user, g.role = user, r
                return fn(ws, *a, **kw)
            return wrapper
        return deco

    def row_out(r: dict[str, Any] | None, drop: tuple[str, ...] = ()) -> dict[str, Any] | None:
        if r is None:
            return None
        return {k: v for k, v in r.items() if k not in drop and not k.endswith("_hash")}

    def need(rows: dict | None, what: str = "resource") -> dict:
        if rows is None:
            raise ApiError(404, "not_found", f"{what} not found")
        return rows

    # ---- UI + health -------------------------------------------------------------------------------
    @app.get("/")
    def index():
        return send_from_directory(WEB_DIR, "index.html")

    @app.get("/static/<path:name>")
    def static_files(name: str):
        return send_from_directory(WEB_DIR, name)

    @app.get("/api/v1/health/live")
    def live():
        return ok({"status": "alive", "version": __version__})

    @app.get("/api/v1/health/ready")
    def ready():
        try:
            v = store.schema_version()
            return ok({"status": "ready", "schema_version": v})
        except Exception as e:  # noqa: BLE001
            raise ApiError(503, "not_ready", f"database unavailable: {e}")

    @app.get("/api/v1/ingest/schema")
    def schema():
        return ok(schema_description())

    # ---- auth ---------------------------------------------------------------------------------------------
    @app.post("/api/v1/auth/login")
    def login():
        b = _body()
        ip = request.remote_addr or "?"
        q = login_attempts[ip]
        now = time.time()
        while q and now - q[0] > 60:
            q.popleft()
        if len(q) >= 10:
            raise ApiError(429, "rate_limited", "too many login attempts; wait a minute")
        q.append(now)
        res = rt.login(str(b.get("email", "")), str(b.get("password", "")))
        if res is None:
            raise ApiError(401, "invalid_credentials", "email or password is incorrect")
        res["workspaces"] = store.q("SELECT w.id,w.name,m.role FROM memberships m JOIN workspaces w ON w.id=m.workspace_id WHERE m.user_id=?", (res["user"]["id"],))
        return ok(res)

    @app.post("/api/v1/auth/logout")
    def logout():
        tok = bearer()
        if tok:
            rt.logout(tok)
        return ok({"logged_out": True})

    @app.get("/api/v1/auth/me")
    def me():
        tok = bearer()
        user = rt.user_for_token(tok) if tok else None
        if not user:
            raise ApiError(401, "unauthenticated", "valid bearer token required")
        return ok({"user": user, "workspaces": store.q("SELECT w.id,w.name,m.role FROM memberships m JOIN workspaces w ON w.id=m.workspace_id WHERE m.user_id=?", (user["id"],))})

    # ---- ingestion (machine auth) ----------------------------------------------------------------------------
    def producer() -> dict[str, Any]:
        key = request.headers.get("X-API-Key", "")
        src = rt.source_for_key(key) if key else None
        if src is None:
            raise ApiError(401, "invalid_api_key", "missing, unknown or revoked X-API-Key")
        return src

    def do_ingest(src: dict[str, Any], events: list[Any]):
        if not events:
            raise ApiError(400, "empty_batch", "no events supplied")
        if len(events) > rt.cfg.max_batch:
            raise ApiError(413, "batch_too_large", f"max {rt.cfg.max_batch} events per request")
        pipe = rt.pipeline(src["workspace_id"])
        try:
            res = pipe.ingest(src, events)
        except Backpressure as e:
            r = ApiError(503, "backpressure", f"processing lag {e.lag} too high; retry later")
            raise r
        except RateLimited as e:
            resp = jsonify({"error": {"code": "rate_limited", "message": "per-source rate limit exceeded"}, "request_id": g.rid})
            resp.status_code = 429
            resp.headers["Retry-After"] = str(max(1, int(e.retry_after + 0.999)))
            return resp
        rt.processor(src["workspace_id"]).notify()
        status = 202 if res.accepted or res.heartbeats or not res.rejected else 422
        if res.rejected and (res.accepted or res.duplicates):
            status = 207
        return ok(res.to_dict(), {"durability": "fsync" if res.durable else "none (non-durable broker)"}, status)

    @app.post("/api/v1/ingest/events")
    def ingest_one():
        return do_ingest(producer(), [request.get_json(silent=True)])

    @app.post("/api/v1/ingest/batch")
    def ingest_batch():
        b = request.get_json(silent=True)
        if not isinstance(b, dict) or not isinstance(b.get("events"), list):
            raise ApiError(400, "invalid_body", 'body must be {"events": [...]}')
        return do_ingest(producer(), b["events"])

    @app.post("/api/v1/ingest/heartbeat")
    def ingest_hb():
        src = producer()
        body = request.get_json(silent=True) or {}
        body = {**{"event_id": f"hb-{secrets.token_hex(6)}", "event_type": "heartbeat", "event_timestamp": time.time()}, **(body if isinstance(body, dict) else {})}
        body["event_type"] = "heartbeat"
        return do_ingest(src, [body])

    # ---- overview / streams / pipeline --------------------------------------------------------------------------
    def window_s(ws: str) -> float:
        return float(rt.settings(ws)["window_s"])

    def series_rows(ws: str, entity: str, t0: float, t1: float, scope: str = "live", limit: int = 4000):
        return store.q("SELECT window_start,n_events,metrics_json FROM windows WHERE workspace_id=? AND scope=? AND window_s=? AND entity=? AND window_start>=? AND window_start<? "
                       "ORDER BY window_start LIMIT ?", (ws, scope, window_s(ws), entity, t0, t1, limit))

    @app.get("/api/v1/workspaces/<ws>/overview")
    @secured("viewer")
    def overview(ws: str):
        now = time.time()
        pipe, proc, dq, alerts, metrics = rt._bundle(ws)
        snap = metrics.snapshot()
        total = store.q1("SELECT COALESCE(SUM(event_count),0) c FROM sources WHERE workspace_id=?", (ws,))["c"]  # type: ignore[index]
        rows = series_rows(ws, "global", now - 3600, now + 60)
        pts = [{"t": r["window_start"], **json.loads(r["metrics_json"])} for r in rows]
        recent_anoms = store.q("SELECT id,detector,severity,entity,metric,window_start,observed,expected,explanation FROM anomalies WHERE workspace_id=? AND scope='live' AND window_start>=? ORDER BY window_start DESC LIMIT 15", (ws, now - 1800))
        crit = store.q1("SELECT COUNT(*) c FROM alerts WHERE workspace_id=? AND status IN ('open','acknowledged','investigating') AND severity='critical'", (ws,))["c"]  # type: ignore[index]
        active_alerts = store.q1("SELECT COUNT(*) c FROM alerts WHERE workspace_id=? AND status IN ('open','acknowledged','investigating')", (ws,))["c"]  # type: ignore[index]
        active_anoms = store.q1("SELECT COUNT(DISTINCT group_key) c FROM anomalies WHERE workspace_id=? AND scope='live' AND window_start>=?", (ws, now - 300))["c"]  # type: ignore[index]
        last = pts[-1] if pts else {}
        fail = [p.get("txn_failure_rate") for p in pts[-3:] if p.get("txn_failure_rate") is not None]
        sim = bool(store.q1("SELECT 1 FROM sources WHERE workspace_id=? AND simulated=1 LIMIT 1", (ws,)))
        health = _health(ws)
        return ok({"total_events": total, "events_per_s": snap["ingest_rate_10s"], "processed_per_s": snap["processed_rate_10s"],
                   "failure_rate": (sum(fail) / len(fail)) if fail else None, "active_anomalies": active_anoms, "active_alerts": active_alerts, "critical_alerts": crit,
                   "pipeline_status": health["status"], "processing_delay_ms": snap["processing_delay_ms"], "detection_latency_ms": snap["detection_latency_ms"],
                   "series": pts, "recent_anomalies": recent_anoms, "mode": proc.status()["mode"], "simulated_data_present": sim,
                   "recent_alerts": store.q("SELECT id,title,severity,status,entity,last_seen,simulated FROM alerts WHERE workspace_id=? ORDER BY updated_at DESC LIMIT 6", (ws,)),
                   "dq_open": store.q1("SELECT COUNT(*) c FROM dq_findings WHERE workspace_id=? AND status='open'", (ws,))["c"],  # type: ignore[index]
                   "window_s": window_s(ws), "last_window": last})

    def _health(ws: str) -> dict[str, Any]:
        pipe, proc, dq, alerts, metrics = rt._bundle(ws)
        now = time.time()
        comps = []

        def add(name: str, status: str, detail: str, **extra: Any) -> None:
            comps.append({"name": name, "status": status, "detail": detail, **extra})

        t = time.perf_counter()
        try:
            store.q1("SELECT 1")
            add("database", "ok", f"SQLite schema v{store.schema_version()}", latency_ms=round((time.perf_counter() - t) * 1000, 2))
        except Exception as e:  # noqa: BLE001
            add("database", "down", str(e))
        add("api", "ok", f"veylstream {__version__}")
        lag = pipe.lag()
        add("broker", "ok" if lag < pipe.cfg.max_lag * 0.5 else "degraded" if lag < pipe.cfg.max_lag else "down",
            f"file log, durable={pipe.broker.durable}, end offset {pipe.broker.end_offset()}, lag {lag}", lag=lag, saturation=round(lag / pipe.cfg.max_lag, 4))  # type: ignore[union-attr]
        ps = proc.status()
        age = (now - pipe.last_processed) if pipe.last_processed else None
        if not ps["running"]:
            st, d = "down", "processor thread is not running"
        elif pipe.last_error:
            st, d = "degraded", f"last error: {pipe.last_error}"
        elif ps["paused"]:
            st, d = "degraded", "paused by operator"
        else:
            st, d = "ok", f"local processor, watermark {pipe.agg.watermark if pipe.agg.watermark != float('-inf') else None}"
        add("processor", st, d, mode=ps["mode"], distributed=False, seconds_since_last_batch=None if age is None else round(age, 1))
        snap = metrics.snapshot()
        add("detection", "ok", f"{len(pipe.detectors)} detectors; {int(snap['counters'].get('windows_closed', 0))} windows evaluated, {int(snap['counters'].get('anomalies', 0))} detections",
            detectors=[d.name for d in pipe.detectors])
        fails = store.q1("SELECT COUNT(*) c FROM deliveries WHERE workspace_id=? AND status IN ('failed','dropped') AND ts>?", (ws, now - 3600))["c"]  # type: ignore[index]
        add("notifier", "ok" if not fails else "degraded", f"{fails} failed deliveries in the last hour")
        dead = store.q1("SELECT COUNT(*) c FROM dead_letters WHERE workspace_id=? AND scope='live' AND ts>?", (ws, now - 300))["c"]  # type: ignore[index]
        add("dead_letters", "ok" if dead < 10 else "degraded", f"{dead} rejected/late events in the last 5 minutes", count_5m=dead)
        silent = []
        for s in store.q("SELECT id,heartbeat_interval_s,last_seen_wall FROM sources WHERE workspace_id=? AND revoked_at IS NULL AND heartbeat_interval_s IS NOT NULL AND last_seen_wall IS NOT NULL", (ws,)):
            if now - s["last_seen_wall"] > 3 * s["heartbeat_interval_s"]:
                silent.append(s["id"])
        add("producers", "ok" if not silent else "degraded", "all heartbeating producers are active" if not silent else f"silent producers: {', '.join(silent)}", silent=silent)
        order = {"ok": 0, "degraded": 1, "down": 2}
        worst = max(comps, key=lambda c: order[c["status"]])["status"]
        return {"status": worst, "components": comps, "metrics": snap, "processor": ps, "throughput": metrics.series(120)}

    @app.get("/api/v1/workspaces/<ws>/pipeline/status")
    @secured("viewer")
    def pipeline_status(ws: str):
        return ok(_health(ws))

    @app.get("/api/v1/workspaces/<ws>/streams")
    @secured("viewer")
    def streams(ws: str):
        now = time.time()
        pipe, proc, *_ = rt._bundle(ws)
        srcs = store.q("SELECT id,name,source_type,simulated,event_count,last_event_ts,last_seen_wall,heartbeat_interval_s,revoked_at FROM sources WHERE workspace_id=?", (ws,))
        types = store.q("SELECT event_type, COUNT(*) n FROM events WHERE workspace_id=? AND scope='live' AND ts>? GROUP BY event_type", (ws, now - 900))
        per_src = {r["source_id"]: r["n"] for r in store.q("SELECT source_id, COUNT(*) n FROM events WHERE workspace_id=? AND scope='live' AND ts>? GROUP BY source_id", (ws, now - 60))}
        for s in srcs:
            s["events_per_s_60s"] = round(per_src.get(s["id"], 0) / 60, 2)
            s["status"] = "revoked" if s["revoked_at"] else ("never_seen" if s["last_seen_wall"] is None else "active" if now - s["last_seen_wall"] < max(30, 3 * (s["heartbeat_interval_s"] or 10)) else "silent")
        return ok({"processor": proc.status(), "sources": srcs, "event_types_15m": types,
                   "config": {"window_s": pipe.cfg.window_s, "allowed_lateness_s": pipe.cfg.allowed_lateness_s, "max_lag": pipe.cfg.max_lag, "dedupe_entries": pipe.cfg.dedupe_entries},
                   "recent_events": store.q("SELECT event_id,event_type,ts,source_id,service,region,status,latency_ms,simulated FROM events WHERE workspace_id=? AND scope='live' ORDER BY ts DESC LIMIT 20", (ws,))})

    @app.post("/api/v1/workspaces/<ws>/streams/processor/<action>")
    @secured("operator")
    def processor_action(ws: str, action: str):
        proc = rt.processor(ws)
        if action == "pause":
            proc.paused = True
        elif action == "resume":
            proc.paused = False
            proc.notify()
        else:
            raise ApiError(400, "invalid_action", "action must be pause or resume")
        store.audit(ws, g.user["email"], f"processor_{action}", ws)
        return ok(proc.status())

    # ---- analytics ---------------------------------------------------------------------------------------------------
    @app.get("/api/v1/workspaces/<ws>/analytics/series")
    @secured("viewer")
    def analytics_series(ws: str):
        now = time.time()
        t1 = _num("to", now + 60)
        t0 = _num("from", t1 - 3600)
        entity = request.args.get("entity", "global")
        metrics = [m for m in request.args.get("metrics", "event_rate").split(",") if m]
        scope = request.args.get("scope", "live")
        rows = series_rows(ws, entity, t0, t1, scope)  # type: ignore[arg-type]
        data = [{"t": r["window_start"], "n": r["n_events"], **{m: json.loads(r["metrics_json"]).get(m) for m in metrics}} for r in rows]
        anoms = store.q("SELECT id,detector,severity,metric,window_start,observed,expected,lower,upper FROM anomalies WHERE workspace_id=? AND scope=? AND entity=? AND window_start>=? AND window_start<? ORDER BY window_start",
                        (ws, scope, entity, t0, t1))
        if request.args.get("format") == "csv":
            buf = io.StringIO()
            w = csv.writer(buf)
            w.writerow(["window_start", "n_events", *metrics])
            for d in data:
                w.writerow([d["t"], d["n"], *[d[m] for m in metrics]])
            return Response(buf.getvalue(), mimetype="text/csv", headers={"Content-Disposition": "attachment; filename=series.csv"})
        return ok({"points": data, "anomalies": [a for a in anoms if a["metric"] in metrics or a["metric"] == "multivariate"]}, {"entity": entity, "window_s": window_s(ws), "from": t0, "to": t1})

    @app.get("/api/v1/workspaces/<ws>/analytics/entities")
    @secured("viewer")
    def analytics_entities(ws: str):
        return ok([r["entity"] for r in store.q("SELECT DISTINCT entity FROM windows WHERE workspace_id=? AND scope='live' ORDER BY entity", (ws,))])

    @app.get("/api/v1/workspaces/<ws>/analytics/breakdown")
    @secured("viewer")
    def analytics_breakdown(ws: str):
        by = request.args.get("by", "service")
        if by not in ("service", "region"):
            raise ApiError(400, "invalid_parameter", "by must be service or region")
        t1 = _num("to", time.time() + 60)
        t0 = _num("from", t1 - 3600)
        rows = store.q("SELECT entity,n_events,metrics_json FROM windows WHERE workspace_id=? AND scope='live' AND window_s=? AND entity LIKE ? AND window_start>=? AND window_start<?",
                       (ws, window_s(ws), f"{by}:%", t0, t1))
        agg: dict[str, dict[str, float]] = {}
        for r in rows:
            m = json.loads(r["metrics_json"])
            a = agg.setdefault(r["entity"].split(":", 1)[1], {"events": 0, "txn": 0, "failed": 0, "p95_sum": 0, "p95_n": 0, "req": 0, "req_err": 0})
            a["events"] += r["n_events"]
            a["txn"] += m.get("txn_count") or 0
            a["failed"] += (m.get("txn_failure_rate") or 0) * (m.get("txn_count") or 0) if m.get("txn_failure_rate") is not None else 0
            a["req"] += m.get("req_count") or 0
            a["req_err"] += (m.get("http_error_rate") or 0) * (m.get("req_count") or 0) if m.get("http_error_rate") is not None else 0
            if m.get("latency_p95_ms") is not None and r["n_events"]:
                a["p95_sum"] += m["latency_p95_ms"]
                a["p95_n"] += 1
        out = [{by: k, "events": int(v["events"]), "txn_failure_rate": v["failed"] / v["txn"] if v["txn"] >= 20 else None, "http_error_rate": v["req_err"] / v["req"] if v["req"] >= 20 else None,
                "latency_p95_ms_mean_of_windows": v["p95_sum"] / v["p95_n"] if v["p95_n"] else None} for k, v in sorted(agg.items())]
        return ok(out, {"note": "failure/error rates are volume-weighted over windows; latency is the mean of per-window p95 (not a global percentile)"})

    @app.get("/api/v1/workspaces/<ws>/analytics/latency-distribution")
    @secured("viewer")
    def latency_dist(ws: str):
        t1 = _num("to", time.time() + 60)
        t0 = _num("from", t1 - 900)
        q, a = "SELECT latency_ms FROM events WHERE workspace_id=? AND scope='live' AND latency_ms IS NOT NULL AND ts>=? AND ts<?", [ws, t0, t1]
        for col in ("service", "region"):
            if request.args.get(col):
                q += f" AND {col}=?"
                a.append(request.args[col])
        vals = [r["latency_ms"] for r in store.q(q + " ORDER BY ts DESC LIMIT 20000", a)]
        if not vals:
            return ok({"bins": [], "n": 0})
        import numpy as np
        hist, edges = np.histogram(np.log10(np.maximum(vals, 1)), bins=24)
        return ok({"bins": [{"lo": float(10 ** edges[i]), "hi": float(10 ** edges[i + 1]), "count": int(hist[i])} for i in range(len(hist))], "n": len(vals),
                   "percentiles": dict(zip(("p50", "p95", "p99"), map(float, np.percentile(vals, [50, 95, 99])))), "sampled_latest": len(vals) == 20000})

    # ---- anomalies ----------------------------------------------------------------------------------------------------
    @app.get("/api/v1/workspaces/<ws>/anomalies")
    @secured("viewer")
    def anomalies(ws: str):
        page, size = _page()
        order = _sort({"time": "window_start", "severity": "score", "score": "score", "entity": "entity", "detector": "detector"}, "-time")
        where, args = ["workspace_id=?", "scope=?"], [ws, request.args.get("scope", "live")]
        for p, col in (("severity", "severity"), ("detector", "detector"), ("entity", "entity"), ("metric", "metric")):
            if request.args.get(p):
                vals = request.args[p].split(",")
                where.append(f"{col} IN ({','.join('?' * len(vals))})")
                args += vals
        if request.args.get("min_severity") in SEV_RANK:
            ok_sev = [s for s in SEVERITIES if SEV_RANK[s] >= SEV_RANK[request.args["min_severity"]]]
            where.append(f"severity IN ({','.join('?' * len(ok_sev))})")
            args += ok_sev
        if request.args.get("q"):
            where.append("(explanation LIKE ? OR entity LIKE ? OR metric LIKE ?)")
            args += [f"%{request.args['q'][:100]}%"] * 3
        for p, op in (("from", ">="), ("to", "<")):
            v = _num(p)
            if v is not None:
                where.append(f"window_start {op} ?")
                args.append(v)
        w = " AND ".join(where)
        total = store.q1(f"SELECT COUNT(*) c FROM anomalies WHERE {w}", args)["c"]  # type: ignore[index]
        rows = store.q(f"SELECT id,detector,version,entity,metric,window_start,window_end,observed,expected,lower,upper,score,severity,direction,explanation,alert_id,detected_at "
                       f"FROM anomalies WHERE {w} ORDER BY {order} LIMIT ? OFFSET ?", [*args, size, (page - 1) * size])
        return ok(rows, {"page": page, "page_size": size, "total": total})

    @app.get("/api/v1/workspaces/<ws>/anomalies/<int:aid>")
    @secured("viewer")
    def anomaly_detail(ws: str, aid: int):
        a = need(store.q1("SELECT * FROM anomalies WHERE id=? AND workspace_id=?", (aid, ws)), "anomaly")
        detail = json.loads(a.pop("detail_json"))
        step = a["window_end"] - a["window_start"]
        hist = series_rows(ws, a["entity"], a["window_start"] - 40 * step, a["window_end"] + 10 * step, a["scope"])
        related = store.q("SELECT id,detector,entity,metric,severity,window_start FROM anomalies WHERE workspace_id=? AND scope=? AND id!=? AND ABS(window_start-?)<=? AND (entity=? OR metric=?) ORDER BY window_start LIMIT 25",
                          (ws, a["scope"], aid, a["window_start"], 3 * step, a["entity"], a["metric"]))
        first = store.q1("SELECT MIN(window_start) m FROM anomalies WHERE workspace_id=? AND scope=? AND group_key=? AND window_start>=? AND window_start<=?",
                         (ws, a["scope"], a["group_key"], a["window_start"] - 10 * step, a["window_start"]))
        return ok({**a, **detail, "deviation_started": first["m"] if first else a["window_start"],
                   "history": [{"t": r["window_start"], "value": json.loads(r["metrics_json"]).get(a["metric"]) if a["metric"] != "multivariate" else None} for r in hist],
                   "related": related, "caveat": "Related anomalies are correlated in time/entity/metric; correlation is not proof of causation."})

    # ---- alerts ---------------------------------------------------------------------------------------------------------
    @app.get("/api/v1/workspaces/<ws>/alerts")
    @secured("viewer")
    def alerts_list(ws: str):
        page, size = _page()
        order = _sort({"updated": "updated_at", "severity": "severity", "first_seen": "first_seen", "last_seen": "last_seen", "status": "status"}, "-updated")
        where, args = ["workspace_id=?"], [ws]
        for p in ("status", "severity", "entity"):
            if request.args.get(p):
                vals = request.args[p].split(",")
                where.append(f"{p} IN ({','.join('?' * len(vals))})")
                args += vals
        if request.args.get("q"):
            where.append("(title LIKE ? OR summary LIKE ?)")
            args += [f"%{request.args['q'][:100]}%"] * 2
        if request.args.get("assignee"):
            where.append("assignee_user_id=?")
            args.append(request.args["assignee"])
        w = " AND ".join(where)
        total = store.q1(f"SELECT COUNT(*) c FROM alerts WHERE {w}", args)["c"]  # type: ignore[index]
        rows = store.q(f"SELECT * FROM alerts WHERE {w} ORDER BY {order} LIMIT ? OFFSET ?", [*args, size, (page - 1) * size])
        dist = store.q("SELECT severity, COUNT(*) n FROM alerts WHERE workspace_id=? AND status!='resolved' GROUP BY severity", (ws,))
        return ok(rows, {"page": page, "page_size": size, "total": total, "open_by_severity": {d["severity"]: d["n"] for d in dist}})

    @app.get("/api/v1/workspaces/<ws>/alerts/<int:aid>")
    @secured("viewer")
    def alert_detail(ws: str, aid: int):
        a = need(store.q1("SELECT * FROM alerts WHERE id=? AND workspace_id=?", (aid, ws)), "alert")
        return ok({**a, "audit": store.q("SELECT ts,actor,action,from_status,to_status,note FROM alert_audit WHERE alert_id=? ORDER BY id", (aid,)),
                   "anomalies": store.q("SELECT id,detector,severity,metric,window_start,observed,expected,explanation FROM anomalies WHERE alert_id=? ORDER BY window_start LIMIT 200", (aid,)),
                   "deliveries": store.q("SELECT d.ts,d.status,d.attempts,d.http_status,d.error,x.name destination FROM deliveries d JOIN destinations x ON x.id=d.destination_id WHERE d.alert_id=? ORDER BY d.id DESC LIMIT 50", (aid,))})

    @app.post("/api/v1/workspaces/<ws>/alerts/<int:aid>/status")
    @secured("operator")
    def alert_status(ws: str, aid: int):
        b = _body()
        try:
            a = rt.alerts(ws).transition(ws, aid, str(b.get("status")), g.user["email"], (b.get("note") or None))
        except KeyError:
            raise ApiError(404, "not_found", "alert not found")
        except InvalidTransition as e:
            raise ApiError(409, "invalid_transition", str(e))
        return ok(a)

    @app.post("/api/v1/workspaces/<ws>/alerts/bulk-status")
    @secured("operator")
    def alert_bulk(ws: str):
        b = _body()
        ids = b.get("ids")
        if not isinstance(ids, list) or not ids or len(ids) > 100:
            raise ApiError(400, "invalid_body", "ids must be a list of 1-100 alert ids")
        done, failed = [], []
        for i in ids:
            try:
                rt.alerts(ws).transition(ws, int(i), str(b.get("status")), g.user["email"], b.get("note"))
                done.append(i)
            except (KeyError, InvalidTransition, ValueError) as e:
                failed.append({"id": i, "error": str(e)})
        return ok({"updated": done, "failed": failed})

    @app.post("/api/v1/workspaces/<ws>/alerts/<int:aid>/assign")
    @secured("operator")
    def alert_assign(ws: str, aid: int):
        try:
            rt.alerts(ws).assign(ws, aid, _body().get("user_id") or None, g.user["email"])
        except KeyError:
            raise ApiError(404, "not_found", "alert not found")
        except ValueError as e:
            raise ApiError(400, "invalid_assignee", str(e))
        return ok(store.q1("SELECT * FROM alerts WHERE id=?", (aid,)))

    @app.post("/api/v1/workspaces/<ws>/alerts/<int:aid>/notes")
    @secured("operator")
    def alert_note(ws: str, aid: int):
        note = str(_body().get("note", "")).strip()
        if not note:
            raise ApiError(400, "invalid_body", "note is required")
        try:
            rt.alerts(ws).add_note(ws, aid, g.user["email"], note)
        except KeyError:
            raise ApiError(404, "not_found", "alert not found")
        return ok({"added": True}, status=201)

    @app.post("/api/v1/workspaces/<ws>/suppressions")
    @secured("operator")
    def suppress(ws: str):
        b = _body()
        mins = float(b.get("minutes", 60))
        if not b.get("entity_prefix") or not 1 <= mins <= 7 * 24 * 60:
            raise ApiError(400, "invalid_body", "entity_prefix and minutes (1..10080) required")
        sid = rt.alerts(ws).suppress(ws, str(b["entity_prefix"]), time.time() + mins * 60, g.user["email"], b.get("metric"), str(b.get("reason", ""))[:200])
        return ok({"id": sid}, status=201)

    @app.get("/api/v1/workspaces/<ws>/members")
    @secured("operator")
    def members(ws: str):
        return ok(store.q("SELECT u.id,u.email,u.display_name,m.role FROM memberships m JOIN users u ON u.id=m.user_id WHERE m.workspace_id=?", (ws,)))

    # ---- data quality ------------------------------------------------------------------------------------------------------
    @app.get("/api/v1/workspaces/<ws>/dq/rules")
    @secured("viewer")
    def dq_rules(ws: str):
        return ok(rt.dq(ws).status())

    @app.post("/api/v1/workspaces/<ws>/dq/rules")
    @secured("admin")
    def dq_rule_create(ws: str):
        b = _body()
        if b.get("kind") not in ("reject_rate", "duplicate_rate", "late_rate", "reason_count", "producer_inactivity", "freshness") or b.get("severity") not in SEVERITIES or not b.get("name"):
            raise ApiError(400, "invalid_body", "name, kind and severity are required and must be valid")
        try:
            i = store.x("INSERT INTO dq_rules(workspace_id,name,kind,scope,params_json,severity,owner) VALUES(?,?,?,?,?,?,?)",
                        (ws, str(b["name"])[:80], b["kind"], b.get("scope", "*"), json.dumps(b.get("params") or {}), b["severity"], str(b.get("owner", g.user["email"]))[:80]))
        except Exception:  # noqa: BLE001
            raise ApiError(409, "conflict", "a rule with this name already exists")
        return ok({"id": i}, status=201)

    @app.patch("/api/v1/workspaces/<ws>/dq/rules/<int:rid>")
    @secured("admin")
    def dq_rule_patch(ws: str, rid: int):
        b = _body()
        need(store.q1("SELECT 1 x FROM dq_rules WHERE id=? AND workspace_id=?", (rid, ws)), "rule")
        if "enabled" in b:
            store.x("UPDATE dq_rules SET enabled=? WHERE id=?", (int(bool(b["enabled"])), rid))
        if isinstance(b.get("params"), dict):
            store.x("UPDATE dq_rules SET params_json=? WHERE id=?", (json.dumps(b["params"]), rid))
        return ok({"updated": True})

    @app.post("/api/v1/workspaces/<ws>/dq/evaluate")
    @secured("operator")
    def dq_eval(ws: str):
        return ok(rt.dq(ws).evaluate())

    @app.get("/api/v1/workspaces/<ws>/dq/findings")
    @secured("viewer")
    def dq_findings(ws: str):
        page, size = _page()
        rows = store.q("SELECT f.id,f.ts,f.severity,f.status,f.details_json,r.name rule FROM dq_findings f JOIN dq_rules r ON r.id=f.rule_id WHERE f.workspace_id=? ORDER BY f.id DESC LIMIT ? OFFSET ?", (ws, size, (page - 1) * size))
        for r in rows:
            r["details"] = json.loads(r.pop("details_json"))
        return ok(rows, {"page": page, "page_size": size})

    # ---- events ------------------------------------------------------------------------------------------------------------------
    @app.get("/api/v1/workspaces/<ws>/events")
    @secured("viewer")
    def events(ws: str):
        page, size = _page()
        order = _sort({"time": "ts", "latency": "latency_ms", "ingested": "ingest_ts"}, "-time")
        where, args = ["workspace_id=?", "scope=?"], [ws, request.args.get("scope", "live")]
        for p, col in (("event_type", "event_type"), ("source_id", "source_id"), ("service", "service"), ("region", "region"), ("status", "status"), ("correlation_id", "correlation_id"), ("event_id", "event_id")):
            if request.args.get(p):
                where.append(f"{col}=?")
                args.append(request.args[p])
        for p, op in (("from", ">="), ("to", "<")):
            v = _num(p)
            if v is not None:
                where.append(f"ts {op} ?")
                args.append(v)
        w = " AND ".join(where)
        total = store.q1(f"SELECT COUNT(*) c FROM events WHERE {w}", args)["c"]  # type: ignore[index]
        rows = store.q(f"SELECT event_id,event_type,ts,ingest_ts,source_id,service,region,entity_id,status,latency_ms,correlation_id,simulated FROM events WHERE {w} ORDER BY {order} LIMIT ? OFFSET ?", [*args, size, (page - 1) * size])
        return ok(rows, {"page": page, "page_size": size, "total": total})

    @app.get("/api/v1/workspaces/<ws>/events/<event_id>")
    @secured("viewer")
    def event_detail(ws: str, event_id: str):
        e = need(store.q1("SELECT payload_json,scope FROM events WHERE workspace_id=? AND event_id=? ORDER BY scope='live' DESC LIMIT 1", (ws, event_id)), "event")
        p = json.loads(e["payload_json"])
        p["entity_id"] = (p.get("entity_id") or "")[:2] + "***" if p.get("entity_id") and g.role == "viewer" else p.get("entity_id")   # viewers see masked entity ids
        w = store.q1("SELECT window_start FROM windows WHERE workspace_id=? AND scope='live' AND entity='global' AND window_start<=? ORDER BY window_start DESC LIMIT 1", (ws, parse_ts(p["event_timestamp"])))
        return ok({"event": p, "processing": {"scope": e["scope"], "event_time": p["event_timestamp"], "ingestion_time": p["ingestion_timestamp"],
                                              "window_start": w["window_start"] if w else None, "validation": "passed (rejected events are in dead letters)"}})

    @app.get("/api/v1/workspaces/<ws>/dead-letters")
    @secured("analyst")
    def dead_letters(ws: str):
        page, size = _page()
        rows = store.q("SELECT id,ts,source_id,reason_code,detail FROM dead_letters WHERE workspace_id=? AND scope=? ORDER BY id DESC LIMIT ? OFFSET ?", (ws, request.args.get("scope", "live"), size, (page - 1) * size))
        by = store.q("SELECT reason_code, COUNT(*) n FROM dead_letters WHERE workspace_id=? AND scope='live' GROUP BY reason_code", (ws,))
        return ok(rows, {"page": page, "page_size": size, "by_reason": {r["reason_code"]: r["n"] for r in by}})

    # ---- sources / destinations / notifications -----------------------------------------------------------------------------------------------
    @app.get("/api/v1/workspaces/<ws>/sources")
    @secured("viewer")
    def sources_list(ws: str):
        return ok([row_out(r) for r in store.q("SELECT * FROM sources WHERE workspace_id=? ORDER BY created_at", (ws,))])

    @app.post("/api/v1/workspaces/<ws>/sources")
    @secured("admin")
    def sources_create(ws: str):
        b = _body()
        if not b.get("name"):
            raise ApiError(400, "invalid_body", "name is required")
        hb = b.get("heartbeat_interval_s")
        res = rt.create_source(ws, str(b["name"]), str(b.get("source_type", "application")), bool(b.get("simulated", False)), float(hb) if hb else None, float(b.get("rate_limit_per_s", 2000)))
        store.audit(ws, g.user["email"], "source_created", res["id"])
        return ok({**res, "endpoint": "/api/v1/ingest/batch", "example": 'curl -X POST $HOST/api/v1/ingest/batch -H "X-API-Key: <key>" -H "Content-Type: application/json" -d \'{"events":[...]}\''}, status=201)

    @app.post("/api/v1/workspaces/<ws>/sources/<sid>/rotate")
    @secured("admin")
    def sources_rotate(ws: str, sid: str):
        try:
            res = rt.rotate_key(ws, sid)
        except KeyError:
            raise ApiError(404, "not_found", "source not found")
        store.audit(ws, g.user["email"], "source_key_rotated", sid)
        return ok(res)

    @app.post("/api/v1/workspaces/<ws>/sources/<sid>/revoke")
    @secured("admin")
    def sources_revoke(ws: str, sid: str):
        if not store.x("UPDATE sources SET revoked_at=? WHERE id=? AND workspace_id=?", (time.time(), sid, ws)):
            raise ApiError(404, "not_found", "source not found")
        store.audit(ws, g.user["email"], "source_revoked", sid)
        return ok({"revoked": True})

    @app.get("/api/v1/workspaces/<ws>/destinations")
    @secured("admin")
    def dest_list(ws: str):
        out = []
        for d in store.q("SELECT * FROM destinations WHERE workspace_id=?", (ws,)):
            cfg = json.loads(d.pop("config_json"))
            d["config"] = {k: ("***" if k in ("password", "token", "secret") else v) for k, v in cfg.items()}
            out.append(d)
        return ok(out)

    @app.post("/api/v1/workspaces/<ws>/destinations")
    @secured("admin")
    def dest_create(ws: str):
        b = _body()
        if b.get("kind") not in ("webhook", "email") or not b.get("name") or not isinstance(b.get("config"), dict):
            raise ApiError(400, "invalid_body", "kind (webhook|email), name and config are required")
        if b["kind"] == "webhook":
            from .alerts import _safe_webhook_url
            bad = _safe_webhook_url(str(b["config"].get("url", "")), rt.cfg.allow_private_webhooks)
            if bad:
                raise ApiError(400, "unsafe_webhook", bad)
        i = store.x("INSERT INTO destinations(workspace_id,kind,name,config_json,min_severity) VALUES(?,?,?,?,?)",
                    (ws, b["kind"], str(b["name"])[:80], json.dumps(b["config"]), b.get("min_severity", "high") if b.get("min_severity") in SEVERITIES else "high"))
        store.audit(ws, g.user["email"], "destination_created", str(i))
        return ok({"id": i}, status=201)

    @app.delete("/api/v1/workspaces/<ws>/destinations/<int:did>")
    @secured("admin")
    def dest_delete(ws: str, did: int):
        if not store.x("DELETE FROM destinations WHERE id=? AND workspace_id=?", (did, ws)):
            raise ApiError(404, "not_found", "destination not found")
        return ok({"deleted": True})

    @app.post("/api/v1/workspaces/<ws>/destinations/<int:did>/test")
    @secured("admin")
    def dest_test(ws: str, did: int):
        d = need(store.q1("SELECT * FROM destinations WHERE id=? AND workspace_id=?", (did, ws)), "destination")
        res = rt.notifier.deliver(ws, d, None, {"event": "test", "title": "Veylstream test notification", "alert": None, "source": "veylstream"})
        return ok(res)

    @app.get("/api/v1/workspaces/<ws>/deliveries")
    @secured("admin")
    def deliveries(ws: str):
        return ok(store.q("SELECT d.ts,d.status,d.attempts,d.http_status,d.error,d.alert_id,x.name destination FROM deliveries d JOIN destinations x ON x.id=d.destination_id WHERE d.workspace_id=? ORDER BY d.id DESC LIMIT 100", (ws,)))

    @app.get("/api/v1/workspaces/<ws>/notifications")
    @secured("viewer")
    def notifications(ws: str):
        rows = store.q("SELECT * FROM notifications WHERE workspace_id=? ORDER BY id DESC LIMIT 50", (ws,))
        return ok(rows, {"unread": sum(1 for r in rows if not r["read"])})

    @app.post("/api/v1/workspaces/<ws>/notifications/read")
    @secured("viewer")
    def notifications_read(ws: str):
        store.x("UPDATE notifications SET read=1 WHERE workspace_id=?", (ws,))
        return ok({"read": True})

    # ---- datasets / replay / experiments ----------------------------------------------------------------------------------------------------------
    @app.get("/api/v1/workspaces/<ws>/datasets")
    @secured("viewer")
    def datasets(ws: str):
        return ok([row_out(r, ("path",)) for r in store.q("SELECT * FROM datasets WHERE workspace_id=? ORDER BY created_at DESC", (ws,))])

    @app.post("/api/v1/workspaces/<ws>/datasets/import")
    @secured("analyst")
    def dataset_import(ws: str):
        fmt = request.args.get("format", "jsonl")
        name = request.args.get("name", "imported")
        try:
            ds = import_dataset(store, ws, name, request.get_data(), fmt, os.path.join(rt.cfg.data_dir))
        except ValueError as e:
            raise ApiError(422, "invalid_dataset", str(e))
        store.audit(ws, g.user["email"], "dataset_imported", ds["id"])
        return ok(row_out(ds, ("path",)), status=201)

    @app.post("/api/v1/workspaces/<ws>/datasets/synthetic")
    @secured("analyst")
    def dataset_synth(ws: str):
        b = _body()
        dur = float(b.get("duration_s", 3600))
        rate = float(b.get("base_rate", 20))
        if not 300 <= dur <= 6 * 3600 or not 1 <= rate <= 60:
            raise ApiError(400, "invalid_body", "duration_s must be 300..21600 and base_rate 1..60 (resource limit)")
        scen = []
        for s in b.get("scenarios", []):
            scen.append(Scenario(s["kind"], float(s["start"]), float(s["end"]), s.get("service"), s.get("region"), float(s.get("magnitude", 1.0))))
        try:
            ds = create_synthetic_dataset(store, ws, str(b.get("name", "synthetic")), rt.cfg.data_dir, int(b.get("seed", 42)), dur, rate, scen)
        except (ValueError, KeyError) as e:
            raise ApiError(400, "invalid_body", str(e))
        return ok(row_out(ds, ("path",)), status=201)

    @app.get("/api/v1/workspaces/<ws>/replays")
    @secured("viewer")
    def replays(ws: str):
        rows = store.q("SELECT id,dataset_id,config_json,status,progress,processed,started_at,finished_at,error FROM replay_jobs WHERE workspace_id=? ORDER BY rowid DESC LIMIT 50", (ws,))
        for r in rows:
            r["config"] = json.loads(r.pop("config_json"))
        return ok(rows)

    @app.post("/api/v1/workspaces/<ws>/replays")
    @secured("analyst")
    def replay_start(ws: str):
        b = _body()
        cfg = {k: b[k] for k in ("speed", "start_ts", "end_ts", "start_offset", "window_s", "allowed_lateness_s", "detectors") if k in b}
        try:
            jid = rt.replays.start(ws, str(b.get("dataset_id")), cfg)
        except KeyError as e:
            raise ApiError(404, "not_found", str(e.args[0]))
        except (ValueError, RuntimeError) as e:
            raise ApiError(400 if isinstance(e, ValueError) else 429, "replay_rejected", str(e))
        store.audit(ws, g.user["email"], "replay_started", jid)
        return ok({"id": jid}, status=202)

    @app.get("/api/v1/workspaces/<ws>/replays/compare")
    @secured("viewer")
    def replay_compare(ws: str):
        a, b = request.args.get("a", "live"), request.args.get("b")
        if not b:
            raise ApiError(400, "invalid_parameter", "b (replay id) is required; a defaults to live")
        for s in (a, b):
            if s != "live" and not store.q1("SELECT 1 x FROM replay_jobs WHERE id=? AND workspace_id=?", (s, ws)):
                raise ApiError(404, "not_found", f"replay {s} not found")
        rng = (_num("from"), _num("to"))
        return ok(compare_scopes(store, ws, a, b, rng if a == "live" else None, rng if b == "live" else None))

    @app.get("/api/v1/workspaces/<ws>/replays/<jid>")
    @secured("viewer")
    def replay_get(ws: str, jid: str):
        r = need(store.q1("SELECT * FROM replay_jobs WHERE id=? AND workspace_id=?", (jid, ws)), "replay")
        r["config"] = json.loads(r.pop("config_json"))
        r["result"] = json.loads(r.pop("result_json")) if r["result_json"] else None
        return ok(r)

    @app.post("/api/v1/workspaces/<ws>/replays/<jid>/<action>")
    @secured("analyst")
    def replay_control(ws: str, jid: str, action: str):
        try:
            rt.replays.control(ws, jid, action)
        except KeyError:
            raise ApiError(404, "not_found", "replay not found")
        except ValueError as e:
            raise ApiError(409, "invalid_action", str(e))
        return ok({"id": jid, "action": action})

    @app.get("/api/v1/workspaces/<ws>/experiments")
    @secured("viewer")
    def experiments(ws: str):
        rows = store.q("SELECT id,name,status,dataset_hash,started_at,finished_at,software_version,seed,error FROM experiments WHERE workspace_id=? ORDER BY rowid DESC LIMIT 100", (ws,))
        return ok(rows)

    @app.get("/api/v1/workspaces/<ws>/experiments/defaults")
    @secured("viewer")
    def experiments_defaults(ws: str):
        return ok({"detectors": DEFAULT_SPECS, "window_s": 30, "allowed_lateness_s": 20, "eval_start_frac": 0.3, "seed": 7})

    @app.post("/api/v1/workspaces/<ws>/experiments")
    @secured("analyst")
    def experiment_start(ws: str):
        b = _body()
        try:
            eid = rt.experiments.start(ws, b)
        except KeyError as e:
            raise ApiError(404, "not_found", str(e.args[0]))
        except ValueError as e:
            raise ApiError(400, "invalid_body", str(e))
        store.audit(ws, g.user["email"], "experiment_started", eid)
        return ok({"id": eid}, status=202)

    @app.get("/api/v1/workspaces/<ws>/experiments/<eid>")
    @secured("viewer")
    def experiment_get(ws: str, eid: str):
        r = need(store.q1("SELECT * FROM experiments WHERE id=? AND workspace_id=?", (eid, ws)), "experiment")
        r["config"] = json.loads(r.pop("config_json"))
        r["results"] = json.loads(r.pop("results_json")) if r["results_json"] else None
        return ok(r)

    @app.get("/api/v1/workspaces/<ws>/experiments/<eid>/export")
    @secured("viewer")
    def experiment_export(ws: str, eid: str):
        r = need(store.q1("SELECT * FROM experiments WHERE id=? AND workspace_id=?", (eid, ws)), "experiment")
        if r["status"] != "completed":
            raise ApiError(409, "not_ready", f"experiment is {r['status']}")
        r["config"] = json.loads(r.pop("config_json"))
        r["results"] = json.loads(r.pop("results_json"))
        fmt = request.args.get("format", "json")
        if fmt == "csv":
            return Response(to_csv(r["results"]), mimetype="text/csv", headers={"Content-Disposition": f"attachment; filename={eid}.csv"})
        if fmt == "md":
            return Response(research_summary(r), mimetype="text/markdown", headers={"Content-Disposition": f"attachment; filename={eid}.md"})
        if fmt == "json":
            return Response(json.dumps(r, indent=2, default=str), mimetype="application/json", headers={"Content-Disposition": f"attachment; filename={eid}.json"})
        raise ApiError(400, "invalid_parameter", "format must be json, csv or md")

    # ---- settings / audit ---------------------------------------------------------------------------------------------------------------------------------
    @app.get("/api/v1/workspaces/<ws>/settings")
    @secured("viewer")
    def settings_get(ws: str):
        w = need(store.q1("SELECT id,name FROM workspaces WHERE id=?", (ws,)))
        return ok({**rt.settings(ws), "workspace": w, "role": g.role})

    @app.patch("/api/v1/workspaces/<ws>/settings")
    @secured("admin")
    def settings_patch(ws: str):
        b = _body()
        bad = set(b) - (SETTING_KEYS - {"demo"})
        if bad:
            raise ApiError(400, "invalid_setting", f"unknown or read-only settings: {sorted(bad)}")
        upd = {}
        if "retention_days" in b:
            if not isinstance(b["retention_days"], (int, float)) or not 1 <= b["retention_days"] <= 365:
                raise ApiError(400, "invalid_setting", "retention_days must be 1..365")
            upd["retention_days"] = b["retention_days"]
        if "default_time_range" in b:
            if b["default_time_range"] not in ("15m", "1h", "6h", "24h"):
                raise ApiError(400, "invalid_setting", "default_time_range must be 15m, 1h, 6h or 24h")
            upd["default_time_range"] = b["default_time_range"]
        if "timezone" in b:
            upd["timezone"] = "UTC" if b["timezone"] not in ("UTC", "local") else b["timezone"]
        if "display_name" in b:
            store.x("UPDATE workspaces SET name=? WHERE id=?", (str(b["display_name"])[:80], ws))
        restart = False
        if "alert_policy" in b:
            ap = b["alert_policy"]
            if not isinstance(ap, dict) or set(ap) - {"min_severity", "consecutive_required", "cooldown_s"} or ap.get("min_severity", "medium") not in SEVERITIES:
                raise ApiError(400, "invalid_setting", "alert_policy accepts min_severity, consecutive_required, cooldown_s")
            upd["alert_policy"] = {**DEFAULT_SETTINGS["alert_policy"], **ap}
            restart = True
        if "detectors" in b:
            from .replay import build_detectors
            try:
                build_detectors(b["detectors"])
            except (ValueError, TypeError, KeyError) as e:
                raise ApiError(400, "invalid_setting", f"detectors: {e}")
            upd["detectors"] = b["detectors"]
            restart = True
        if "allowed_lateness_s" in b:
            if not isinstance(b["allowed_lateness_s"], (int, float)) or not 0 <= b["allowed_lateness_s"] <= 300:
                raise ApiError(400, "invalid_setting", "allowed_lateness_s must be 0..300")
            upd["allowed_lateness_s"] = b["allowed_lateness_s"]
            restart = True
        if "window_s" in b:
            if b["window_s"] not in (10, 15, 30, 60):
                raise ApiError(400, "invalid_setting", "window_s must be 10, 15, 30 or 60")
            upd["window_s"] = b["window_s"]
            restart = True
        if upd:
            store.set_settings(ws, upd)
        if restart:
            rt.restart_pipeline(ws)
        store.audit(ws, g.user["email"], "settings_updated", ws, json.dumps(list(b)))
        return ok({**rt.settings(ws), "restarted_pipeline": restart})

    @app.get("/api/v1/workspaces/<ws>/audit")
    @secured("admin")
    def audit(ws: str):
        return ok(store.q("SELECT ts,actor,action,target,detail FROM audit_events WHERE workspace_id=? ORDER BY id DESC LIMIT 200", (ws,)))

    # ---- demo ------------------------------------------------------------------------------------------------------------------------------------------------
    @app.get("/api/v1/workspaces/<ws>/demo")
    @secured("viewer")
    def demo_status(ws: str):
        d = rt.demo(ws)
        return ok({"running": d.running, "rate": d.rate, "sent": d.sent, "scenarios": d.scenarios_log[-10:], "simulated": True,
                   "is_demo_workspace": bool(rt.settings(ws).get("demo")), "scenario_kinds": sorted(__import__("veylstream.generator", fromlist=["x"]).SCENARIO_KINDS)})

    @app.post("/api/v1/workspaces/<ws>/demo/<action>")
    @secured("operator")
    def demo_action(ws: str, action: str):
        d = rt.demo(ws)
        b = request.get_json(silent=True) or {}
        try:
            if action == "seed":
                res = d.seed(float(b.get("minutes", 30)), float(b.get("rate", 20)))
            elif action == "start":
                d.start(float(b["rate"]) if b.get("rate") else None)
                res = {"running": True, "rate": d.rate}
            elif action == "stop":
                d.stop()
                res = {"running": False}
            elif action == "trigger":
                res = d.trigger(str(b.get("kind")), float(b.get("duration_s", 180)), b.get("service"), b.get("region"), b.get("magnitude"))
            elif action == "reset":
                if g.role != "admin":
                    raise ApiError(403, "forbidden", "reset requires admin")
                res = rt.reset_demo(ws)
            else:
                raise ApiError(404, "not_found", "unknown demo action")
        except PermissionError as e:
            raise ApiError(409, "refused", str(e))
        except (ValueError, RuntimeError) as e:
            raise ApiError(400, "invalid_request", str(e))
        store.audit(ws, g.user["email"], f"demo_{action}", ws)
        return ok(res)

    # ---- live stream (SSE) and Prometheus metrics ------------------------------------------------------------------------------------------------------------------
    @app.get("/api/v1/workspaces/<ws>/live")
    @secured("viewer")
    def live_stream(ws: str):
        rt._bundle(ws)
        sid, q = rt.bus.subscribe(ws)

        def gen():
            try:
                yield "retry: 3000\n\n"
                yield f"data: {json.dumps({'type': 'hello', 'data': {'server_time': time.time()}})}\n\n"
                last_stats = 0.0
                while True:
                    try:
                        yield f"data: {q.get(timeout=1.0)}\n\n"
                    except queue.Empty:
                        pass
                    now = time.time()
                    if now - last_stats >= 2.0:
                        last_stats = now
                        m = rt.metrics(ws).snapshot()
                        yield f"data: {json.dumps({'type': 'stats', 'data': {'t': now, 'ingest_rate': m['ingest_rate_10s'], 'processed_rate': m['processed_rate_10s'], 'lag': rt.pipeline(ws).lag(), 'delay_ms': m['processing_delay_ms']}})}\n\n"
            finally:
                rt.bus.unsubscribe(sid)

        return Response(gen(), mimetype="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    @app.get("/api/v1/workspaces/<ws>/metrics")
    @secured("viewer")
    def prom(ws: str):
        m = rt.metrics(ws).snapshot()
        lines = []
        for k, v in sorted(m["counters"].items()):
            lines += [f"# TYPE veylstream_{k}_total counter", f"veylstream_{k}_total {v}"]
        lines += ["# TYPE veylstream_processing_lag gauge", f"veylstream_processing_lag {rt.pipeline(ws).lag()}",
                  "# TYPE veylstream_ingest_rate gauge", f"veylstream_ingest_rate {m['ingest_rate_10s']}"]
        return Response("\n".join(lines) + "\n", mimetype="text/plain; version=0.0.4")

    return app
