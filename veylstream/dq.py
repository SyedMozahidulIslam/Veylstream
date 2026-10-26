"""Data-quality rule engine.

Rules are rows in ``dq_rules`` (name, kind, scope, params, severity, owner, enabled). Checks are evaluated
on ingestion counters and the dead-letter table, so they measure what Veylstream actually observed at its edge.
Validation itself (required fields, allowed values, numeric ranges, schema version) happens at ingestion; the
``reason_count`` rule kind turns those rejections into monitored, alertable checks.
"""
from __future__ import annotations

import json
import time
from collections import deque
from typing import Any

from .alerts import AlertManager
from .store import Store
from .telemetry import Metrics

DEFAULT_RULES = [
    ("reject-rate", "reject_rate", "*", {"window_s": 300, "max_rate": 0.05, "min_events": 50}, "high", "platform"),
    ("duplicate-rate", "duplicate_rate", "*", {"window_s": 300, "max_rate": 0.05, "min_events": 50}, "medium", "platform"),
    ("late-event-rate", "late_rate", "*", {"window_s": 300, "max_rate": 0.03, "min_events": 50}, "medium", "platform"),
    ("missing-required-fields", "reason_count", "*", {"codes": ["missing_field"], "window_s": 300, "max_count": 10}, "high", "platform"),
    ("allowed-values-and-ranges", "reason_count", "*", {"codes": ["invalid_value", "out_of_range", "invalid_type"], "window_s": 300, "max_count": 10}, "medium", "platform"),
    ("schema-consistency", "reason_count", "*", {"codes": ["schema_mismatch"], "window_s": 300, "max_count": 0}, "high", "platform"),
    ("producer-inactivity", "producer_inactivity", "*", {"missed_heartbeats": 3}, "critical", "platform"),
    ("stream-freshness", "freshness", "*", {"max_silence_s": 120}, "high", "platform"),
]


class DQEngine:
    def __init__(self, store: Store, ws: str, metrics: Metrics, alerts: AlertManager | None = None, pipeline: Any = None) -> None:
        self.store, self.ws, self.metrics, self.alerts, self.pipeline = store, ws, metrics, alerts, pipeline
        self._snaps: deque[tuple[float, dict[str, float]]] = deque(maxlen=2000)
        self._snaps.append((time.time(), {"accepted": 0.0, "rejected": 0.0, "duplicates": 0.0, "late_dropped": 0.0}))

    def ensure_default_rules(self) -> None:
        for name, kind, scope, params, sev, owner in DEFAULT_RULES:
            self.store.x("INSERT OR IGNORE INTO dq_rules(workspace_id,name,kind,scope,params_json,severity,owner) VALUES(?,?,?,?,?,?,?)",
                         (self.ws, name, kind, scope, json.dumps(params), sev, owner))

    def _delta(self, now: float, window_s: float) -> dict[str, float]:
        cur = self._snaps[-1][1]
        base = next((s for t, s in self._snaps if t >= now - window_s), self._snaps[0][1])
        return {k: cur.get(k, 0) - base.get(k, 0) for k in ("accepted", "rejected", "duplicates", "late_dropped")}

    def evaluate(self, now: float | None = None) -> list[dict[str, Any]]:
        now = now or time.time()
        c = self.metrics.snapshot()["counters"]
        self._snaps.append((now, {k: c.get(k, 0.0) for k in ("accepted", "rejected", "duplicates", "late_dropped")}))
        results = []
        for r in self.store.q("SELECT * FROM dq_rules WHERE workspace_id=? AND enabled=1", (self.ws,)):
            p = json.loads(r["params_json"])
            for key, failing, details in self._check(r, p, now):
                results.append(self._transition(r, key, failing, details, now))
        return results

    def _check(self, r: dict, p: dict, now: float):
        kind = r["kind"]
        if kind in ("reject_rate", "duplicate_rate", "late_rate"):
            d = self._delta(now, p["window_s"])
            total = d["accepted"] + d["rejected"] + d["duplicates"] + d["late_dropped"]
            num = {"reject_rate": d["rejected"], "duplicate_rate": d["duplicates"], "late_rate": d["late_dropped"]}[kind]
            rate = num / total if total else 0.0
            yield "", total >= p["min_events"] and rate > p["max_rate"], {"rate": round(rate, 4), "count": num, "total_seen": total, "limit": p["max_rate"], "window_s": p["window_s"]}
        elif kind == "reason_count":
            codes = p["codes"]
            q = ",".join("?" * len(codes))
            n = self.store.q1(f"SELECT COUNT(*) c FROM dead_letters WHERE workspace_id=? AND scope='live' AND ts>? AND reason_code IN ({q})",
                              (self.ws, now - p["window_s"], *codes))["c"]  # type: ignore[index]
            yield "", n > p["max_count"], {"count": n, "limit": p["max_count"], "codes": codes, "window_s": p["window_s"]}
        elif kind == "producer_inactivity":
            for s in self.store.q("SELECT id,heartbeat_interval_s,last_seen_wall FROM sources WHERE workspace_id=? AND revoked_at IS NULL AND heartbeat_interval_s IS NOT NULL", (self.ws,)):
                if s["last_seen_wall"] is None:
                    continue                       # never seen: nothing to be "inactive" relative to
                silent = now - s["last_seen_wall"]
                limit = p["missed_heartbeats"] * s["heartbeat_interval_s"]
                yield s["id"], silent > limit, {"source_id": s["id"], "silent_s": round(silent, 1), "limit_s": limit}
        elif kind == "freshness":
            last = self.store.q1("SELECT MAX(last_seen_wall) m FROM sources WHERE workspace_id=?", (self.ws,))["m"]  # type: ignore[index]
            if last is not None:
                yield "", now - last > p["max_silence_s"], {"silent_s": round(now - last, 1), "limit_s": p["max_silence_s"]}

    def _transition(self, r: dict, key: str, failing: bool, details: dict, now: float) -> dict[str, Any]:
        fp_name = f"{r['name']}:{key}" if key else r["name"]
        last = self.store.q1("SELECT * FROM dq_findings WHERE workspace_id=? AND rule_id=? AND json_extract(details_json,'$.key')=? ORDER BY id DESC LIMIT 1",
                             (self.ws, r["id"], key))
        was_open = bool(last and last["status"] == "open")
        details = {**details, "key": key}
        if failing and not was_open:
            self.store.x("INSERT INTO dq_findings(workspace_id,rule_id,ts,severity,status,details_json) VALUES(?,?,?,?,?,?)",
                         (self.ws, r["id"], now, r["severity"], "open", json.dumps(details)))
            if self.alerts:
                self.alerts.raise_signal(self.ws, f"dq|{fp_name}", f"Data quality: {r['name']}" + (f" ({key})" if key else ""), r["severity"],
                                         f"source:{key}" if key else "pipeline", r["kind"], f"{r['name']} failing: {json.dumps(details)}", now)
        elif not failing and was_open:
            self.store.x("UPDATE dq_findings SET status='recovered' WHERE id=?", (last["id"],))
        return {"rule": r["name"], "key": key, "failing": failing, "details": details}

    def status(self) -> list[dict[str, Any]]:
        out = []
        for r in self.store.q("SELECT * FROM dq_rules WHERE workspace_id=? ORDER BY id", (self.ws,)):
            f = self.store.q1("SELECT * FROM dq_findings WHERE workspace_id=? AND rule_id=? ORDER BY id DESC LIMIT 1", (self.ws, r["id"]))
            open_n = self.store.q1("SELECT COUNT(*) c FROM dq_findings WHERE workspace_id=? AND rule_id=? AND status='open'", (self.ws, r["id"]))["c"]  # type: ignore[index]
            out.append({"id": r["id"], "name": r["name"], "kind": r["kind"], "scope": r["scope"], "severity": r["severity"], "enabled": bool(r["enabled"]),
                        "owner": r["owner"], "params": json.loads(r["params_json"]), "open_findings": open_n,
                        "last_finding": ({"ts": f["ts"], "status": f["status"], "details": json.loads(f["details_json"])} if f else None)})
        return out
