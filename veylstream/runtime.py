"""Application runtime: owns the store, per-workspace pipelines, auth, demo control, background maintenance."""
from __future__ import annotations

import heapq
import json
import os
import secrets
import shutil
import threading
import time
from dataclasses import dataclass, field
from typing import Any

from .alerts import AlertManager, AlertPolicy, Notifier
from .broker import Broker, FileLogBroker, MemoryBroker
from .contracts import validate_event
from .dq import DQEngine
from .experiments import ExperimentService
from .generator import GeneratorConfig, Scenario, SyntheticGenerator, SCENARIO_KINDS, SERVICES
from .pipeline import LocalProcessor, Pipeline, PipelineConfig
from .replay import ReplayService, build_detectors
from .store import Store, hash_password, new_id, sha256, verify_password
from .telemetry import EventBus, Metrics

ROLE_RANK = {"viewer": 0, "analyst": 1, "operator": 2, "admin": 3}
DEFAULT_SETTINGS = {
    "default_time_range": "1h", "timezone": "UTC", "retention_days": 3,
    "alert_policy": {"min_severity": "medium", "consecutive_required": 2, "cooldown_s": 300},
    "detectors": [{"name": "rule"}, {"name": "mad"}, {"name": "zscore"}, {"name": "isolation_forest", "config": {"min_train": 40}}],
    "window_s": 30, "allowed_lateness_s": 20,
}
SETTING_KEYS = set(DEFAULT_SETTINGS) | {"display_name", "demo"}


@dataclass
class Config:
    data_dir: str = "./data/runtime"
    db_path: str | None = None
    fsync: bool = True
    allow_private_webhooks: bool = False
    session_ttl_s: float = 12 * 3600
    max_batch: int = 1000
    max_body_bytes: int = 2 * 1024 * 1024
    cors_origins: list[str] = field(default_factory=list)

    @classmethod
    def from_env(cls) -> "Config":
        e = os.environ.get
        return cls(data_dir=e("VEYLSTREAM_DATA_DIR", "./data/runtime"), db_path=e("VEYLSTREAM_DB"), fsync=e("VEYLSTREAM_FSYNC", "1") != "0",
                   allow_private_webhooks=e("VEYLSTREAM_ALLOW_PRIVATE_WEBHOOKS", "0") == "1",
                   cors_origins=[o for o in e("VEYLSTREAM_CORS_ORIGINS", "").split(",") if o])


class Demo:
    """Simulated traffic for one workspace. Everything it produces is flagged simulated and enters through the real ingestion path."""

    def __init__(self, rt: "Runtime", ws: str) -> None:
        self.rt, self.ws = rt, ws
        self.gen: SyntheticGenerator | None = None
        self.rate = 20.0
        self._stop = threading.Event()
        self._t: threading.Thread | None = None
        self._heap: list[tuple[float, int, str, Any]] = []
        self._n = 0
        self.sent = 0
        self.scenarios_log: list[dict[str, Any]] = []

    @property
    def running(self) -> bool:
        return bool(self._t and self._t.is_alive())

    def ensure_sources(self) -> dict[str, dict[str, Any]]:
        out = {}
        for svc in SERVICES:
            sid = SyntheticGenerator.source_id(svc)
            row = self.rt.store.q1("SELECT * FROM sources WHERE id=? AND workspace_id=?", (sid, self.ws))
            if row is None:
                self.rt.store.x("INSERT INTO sources(id,workspace_id,name,source_type,simulated,key_hash,key_prefix,created_at,heartbeat_interval_s,rate_limit_per_s) VALUES(?,?,?,?,?,?,?,?,?,?)",
                                (sid, self.ws, f"{svc} (simulated)", "application", 1, sha256(secrets.token_hex(16)), "demo", time.time(), 10.0, 5000))
                row = self.rt.store.q1("SELECT * FROM sources WHERE id=?", (sid,))
            out[sid] = row  # type: ignore[assignment]
        return out

    def _ingest_ordered(self, items: list[tuple[float, str, Any]], unlimited: bool = False, max_skew_s: float = 2.0) -> None:
        """Send (arrival, source, payload) items in arrival order. Items are flushed per source every ``max_skew_s`` of
        arrival time so that cross-source event-time skew stays far below the allowed lateness."""
        srcs = self.ensure_sources()
        p = self.rt.pipeline(self.ws)
        buf: dict[str, list] = {}
        start = None

        def flush() -> None:
            for src, payloads in buf.items():
                for i in range(0, len(payloads), 500):
                    p.ingest({**srcs[src], "rate_limit_per_s": 1e9} if unlimited else srcs[src], payloads[i:i + 500])
            buf.clear()

        for arr, src, payload in items:
            if start is None:
                start = arr
            if arr - start >= max_skew_s:
                flush()
                start = arr
            buf.setdefault(src, []).append(payload)
        flush()

    def _send(self, items: list[tuple[float, str, Any]]) -> None:
        self._ingest_ordered(items)
        self.sent += len(items)

    def seed(self, minutes: float = 30, rate: float = 20.0, seed: int = 42) -> dict[str, Any]:
        """Backfill normal history through the real pipeline, then wait for the processor to drain it."""
        self.rt.store.set_settings(self.ws, {"demo": True})
        self.rt.dq(self.ws).ensure_default_rules()
        self.rate = rate
        self.gen = SyntheticGenerator(GeneratorConfig(seed=seed, base_rate=rate))
        end = time.time() - 2.0
        start = end - minutes * 60
        raw = self.gen.generate(start, end)
        self.rt.pipeline(self.ws)
        self._ingest_ordered(raw, unlimited=True)
        ok = self.rt.processor(self.ws).drain(120)
        self._cursor = end
        return {"events": len(raw), "minutes": minutes, "drained": ok}

    def start(self, rate: float | None = None) -> None:
        if self.running:
            return
        if self.gen is None:
            self.gen = SyntheticGenerator(GeneratorConfig(seed=42, base_rate=rate or self.rate))
            self._cursor = time.time()
        if rate:
            self.rate = max(1.0, min(rate, 200.0))
        self.gen.cfg.base_rate = self.rate
        self._stop.clear()
        self._t = threading.Thread(target=self._loop, name=f"demo-{self.ws}", daemon=True)
        self._t.start()

    def stop(self) -> None:
        self._stop.set()
        if self._t:
            self._t.join(3)

    def _loop(self) -> None:
        cursor = getattr(self, "_cursor", time.time())
        while not self._stop.is_set():
            now = time.time()
            if now - cursor >= 1.0:
                assert self.gen is not None
                for arr, src, payload in self.gen.generate(cursor, cursor + 1.0):
                    self._n += 1
                    heapq.heappush(self._heap, (arr, self._n, src, payload))
                cursor += 1.0
            due = []
            while self._heap and self._heap[0][0] <= now:
                a, _, s, pl = heapq.heappop(self._heap)
                due.append((a, s, pl))
            if due:
                try:
                    self._send(due)
                except Exception:      # backpressure / rate limit: drop the simulated batch, never crash the loop
                    self.rt.metrics(self.ws).inc("demo_send_errors")
            self._stop.wait(0.2)

    def trigger(self, kind: str, duration_s: float = 180.0, service: str | None = None, region: str | None = None, magnitude: float | None = None) -> dict[str, Any]:
        if kind not in SCENARIO_KINDS:
            raise ValueError(f"unknown scenario {kind!r}; choose from {sorted(SCENARIO_KINDS)}")
        if not self.running:
            raise RuntimeError("start the synthetic stream first")
        defaults = {"failure_spike": ("payments", "eu-west", 0.4), "latency_degradation": ("checkout", None, 4.0), "traffic_spike": (None, None, 3.0),
                    "traffic_drop": (None, None, 0.2), "producer_outage": ("payments", None, 1.0), "gradual_drift": ("checkout", None, 4.0),
                    "duplicates": (None, None, 1.0), "late_events": (None, None, 1.0), "invalid_payloads": (None, None, 1.0)}
        ds, dr, dm = defaults[kind]
        now = time.time()
        s = Scenario(kind, now, now + max(10.0, min(duration_s, 1800.0)), service if service is not None else ds, region if region is not None else dr,
                     magnitude if magnitude is not None else dm, label=kind in ("failure_spike", "latency_degradation", "traffic_spike", "traffic_drop", "producer_outage", "gradual_drift"))
        self.gen.add_scenario(s)  # type: ignore[union-attr]
        rec = {"kind": kind, "start": s.start, "end": s.end, "service": s.service, "region": s.region, "magnitude": s.magnitude, "simulated": True}
        self.scenarios_log.append(rec)
        return rec


class Runtime:
    def __init__(self, cfg: Config | None = None, store: Store | None = None) -> None:
        self.cfg = cfg or Config()
        os.makedirs(self.cfg.data_dir, exist_ok=True)
        self.store = store or Store(self.cfg.db_path or os.path.join(self.cfg.data_dir, "veylstream.sqlite"))
        self.bus = EventBus()
        self.notifier = Notifier(self.store, allow_private=self.cfg.allow_private_webhooks)
        self.replays = ReplayService(self.store)
        self.experiments = ExperimentService(self.store)
        self._lock = threading.RLock()
        self._pipes: dict[str, tuple[Pipeline, LocalProcessor, DQEngine, AlertManager, Metrics]] = {}
        self._demos: dict[str, Demo] = {}
        self._stop = threading.Event()
        self._maint = threading.Thread(target=self._maintenance, name="maintenance", daemon=True)
        self._maint.start()

    # ---- per-workspace components ------------------------------------------------------------
    def settings(self, ws: str) -> dict[str, Any]:
        r = self.store.q1("SELECT settings_json FROM workspaces WHERE id=?", (ws,))
        if r is None:
            raise KeyError("workspace not found")
        return {**DEFAULT_SETTINGS, **json.loads(r["settings_json"])}

    def _bundle(self, ws: str):
        with self._lock:
            b = self._pipes.get(ws)
            if b is None:
                st = self.settings(ws)
                broker: Broker = FileLogBroker(os.path.join(self.cfg.data_dir, "broker", ws), fsync=self.cfg.fsync)
                metrics = Metrics()
                pol = AlertPolicy(**{k: v for k, v in st["alert_policy"].items() if k in AlertPolicy.__dataclass_fields__})
                alerts = AlertManager(self.store, self.notifier, self.bus, pol)
                pcfg = PipelineConfig(window_s=float(st["window_s"]), allowed_lateness_s=float(st["allowed_lateness_s"]))
                dq = DQEngine(self.store, ws, metrics, alerts)
                pipe = Pipeline(self.store, ws, broker, pcfg, build_detectors(st["detectors"]), alerts, self.bus, metrics, dq=dq)
                dq.pipeline = pipe
                proc = LocalProcessor(pipe)
                proc.start()
                b = self._pipes[ws] = (pipe, proc, dq, alerts, metrics)
            return b

    def pipeline(self, ws: str) -> Pipeline: return self._bundle(ws)[0]
    def processor(self, ws: str) -> LocalProcessor: return self._bundle(ws)[1]
    def dq(self, ws: str) -> DQEngine: return self._bundle(ws)[2]
    def alerts(self, ws: str) -> AlertManager: return self._bundle(ws)[3]
    def metrics(self, ws: str) -> Metrics: return self._bundle(ws)[4]

    def demo(self, ws: str) -> Demo:
        with self._lock:
            return self._demos.setdefault(ws, Demo(self, ws))

    def restart_pipeline(self, ws: str) -> None:
        with self._lock:
            b = self._pipes.pop(ws, None)
        if b:
            b[1].stop()
            b[0].broker.close()  # type: ignore[union-attr]
        self._bundle(ws)

    def _maintenance(self) -> None:
        last_ret = 0.0
        while not self._stop.wait(5.0):
            if time.time() - last_ret > 3600:
                last_ret = time.time()
                for w in self.store.q("SELECT id, settings_json FROM workspaces"):
                    d = int({**DEFAULT_SETTINGS, **json.loads(w["settings_json"])}["retention_days"])
                    self.store.apply_retention(w["id"], d, d * 2)

    # ---- accounts / auth --------------------------------------------------------------------------
    def create_workspace(self, name: str) -> str:
        wid = new_id("ws")
        self.store.x("INSERT INTO workspaces(id,name,created_at,settings_json) VALUES(?,?,?,?)", (wid, name[:80], time.time(), "{}"))
        return wid

    def create_user(self, email: str, password: str, display_name: str, ws: str | None = None, role: str = "viewer") -> str:
        if len(password) < 10:
            raise ValueError("password must be at least 10 characters")
        if role not in ROLE_RANK:
            raise ValueError("invalid role")
        uid = new_id("u")
        self.store.x("INSERT INTO users(id,email,display_name,pw_hash,created_at) VALUES(?,?,?,?,?)", (uid, email.lower().strip(), display_name[:80], hash_password(password), time.time()))
        if ws:
            self.store.x("INSERT INTO memberships(user_id,workspace_id,role) VALUES(?,?,?)", (uid, ws, role))
        return uid

    def login(self, email: str, password: str) -> dict[str, Any] | None:
        u = self.store.q1("SELECT * FROM users WHERE email=?", (email.lower().strip(),))
        if u is None:
            verify_password(password, "scrypt$16384$8$1$00$00")      # equalise timing
            return None
        if not verify_password(password, u["pw_hash"]):
            return None
        tok = secrets.token_urlsafe(32)
        self.store.x("INSERT INTO sessions(token_hash,user_id,created_at,expires_at) VALUES(?,?,?,?)", (sha256(tok), u["id"], time.time(), time.time() + self.cfg.session_ttl_s))
        self.store.audit(None, u["email"], "login", u["id"])
        return {"token": tok, "user": {"id": u["id"], "email": u["email"], "display_name": u["display_name"]}}

    def user_for_token(self, tok: str) -> dict[str, Any] | None:
        s = self.store.q1("SELECT u.id,u.email,u.display_name FROM sessions s JOIN users u ON u.id=s.user_id WHERE s.token_hash=? AND s.expires_at>?", (sha256(tok), time.time()))
        return s

    def logout(self, tok: str) -> None:
        self.store.x("DELETE FROM sessions WHERE token_hash=?", (sha256(tok),))

    def role(self, user_id: str, ws: str) -> str | None:
        r = self.store.q1("SELECT role FROM memberships WHERE user_id=? AND workspace_id=?", (user_id, ws))
        return r["role"] if r else None

    def create_source(self, ws: str, name: str, source_type: str = "application", simulated: bool = False, heartbeat_interval_s: float | None = None,
                      rate_limit_per_s: float = 2000) -> dict[str, Any]:
        key = "vsk_" + secrets.token_urlsafe(24)
        sid = new_id("src")
        self.store.x("INSERT INTO sources(id,workspace_id,name,source_type,simulated,key_hash,key_prefix,created_at,heartbeat_interval_s,rate_limit_per_s) VALUES(?,?,?,?,?,?,?,?,?,?)",
                     (sid, ws, name[:80], source_type, int(simulated), sha256(key), key[:8], time.time(), heartbeat_interval_s, rate_limit_per_s))
        return {"id": sid, "api_key": key, "note": "store this key now; it is shown once and only a hash is kept"}

    def rotate_key(self, ws: str, sid: str) -> dict[str, Any]:
        if not self.store.q1("SELECT 1 FROM sources WHERE id=? AND workspace_id=?", (sid, ws)):
            raise KeyError("source not found")
        key = "vsk_" + secrets.token_urlsafe(24)
        self.store.x("UPDATE sources SET key_hash=?, key_prefix=?, revoked_at=NULL WHERE id=?", (sha256(key), key[:8], sid))
        return {"id": sid, "api_key": key}

    def source_for_key(self, key: str) -> dict[str, Any] | None:
        return self.store.q1("SELECT * FROM sources WHERE key_hash=? AND revoked_at IS NULL", (sha256(key),))

    def reset_demo(self, ws: str) -> dict[str, int]:
        """Delete live-scope derived data of a demo workspace. Refuses if any non-simulated event exists."""
        if not self.settings(ws).get("demo"):
            raise PermissionError("workspace is not a demo workspace")
        real = self.store.q1("SELECT COUNT(*) c FROM events WHERE workspace_id=? AND scope='live' AND simulated=0", (ws,))["c"]  # type: ignore[index]
        if real:
            raise PermissionError("workspace contains non-simulated events; refusing to reset")
        self.demo(ws).stop()
        with self._lock:
            b = self._pipes.pop(ws, None)
        if b:
            b[1].stop()
            b[0].broker.close()  # type: ignore[union-attr]
        shutil.rmtree(os.path.join(self.cfg.data_dir, "broker", ws), ignore_errors=True)
        n = {}
        for t in ("events", "windows", "anomalies", "dead_letters", "alerts", "notifications", "deliveries", "dq_findings", "suppressions"):
            n[t] = self.store.x(f"DELETE FROM {t} WHERE workspace_id=?" + (" AND scope='live'" if t in ("events", "windows", "anomalies", "dead_letters") else ""), (ws,))
        self.store.x("DELETE FROM alert_audit WHERE alert_id NOT IN (SELECT id FROM alerts)")
        self.store.x("DELETE FROM checkpoints WHERE name LIKE ?", (f"{ws}:%",))
        self.store.x("UPDATE sources SET event_count=0, last_event_ts=NULL, last_seen_wall=NULL WHERE workspace_id=?", (ws,))
        self.store.x("UPDATE dq_rules SET state='ok' WHERE workspace_id=?", (ws,))
        self._demos.pop(ws, None)
        self.store.audit(ws, "system", "demo_reset", ws)
        return n

    def close(self) -> None:
        self._stop.set()
        for d in list(self._demos.values()):
            d.stop()
        with self._lock:
            for pipe, proc, *_ in self._pipes.values():
                proc.stop()
                pipe.broker.close()  # type: ignore[union-attr]
            self._pipes.clear()
        self.notifier.close()
        self.store.close()
