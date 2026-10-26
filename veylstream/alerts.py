"""Alert lifecycle: policy-based opening, grouping, cooldown/reopen, suppression, audit, notifications."""
from __future__ import annotations

import ipaddress
import json
import os
import queue
import smtplib
import socket
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from email.message import EmailMessage
from typing import Any

from .detectors import SEV_RANK, Detection
from .store import Store
from .telemetry import EventBus

TRANSITIONS = {
    "open": {"acknowledged", "investigating", "resolved", "suppressed"},
    "acknowledged": {"investigating", "resolved", "open", "suppressed"},
    "investigating": {"resolved", "acknowledged", "open", "suppressed"},
    "resolved": {"open"},
    "suppressed": {"open", "resolved"},
}


class InvalidTransition(ValueError):
    pass


@dataclass
class AlertPolicy:
    min_severity: str = "medium"        # detections below this never open alerts (still stored as anomalies)
    immediate_severity: str = "high"    # at/above this a single window opens an alert
    consecutive_required: int = 2       # otherwise this many consecutive windows are required
    gap_windows: float = 2.0            # a gap larger than this many windows resets the streak
    cooldown_s: float = 300.0           # after resolution, detections inside this quiet period do not reopen


def _safe_webhook_url(url: str, allow_private: bool) -> str | None:
    """Return an error string if the URL is unsafe (SSRF guard), else None."""
    p = urllib.parse.urlparse(url)
    if p.scheme not in ("http", "https") or not p.hostname:
        return "webhook URL must be http(s) with a host"
    if allow_private:
        return None
    try:
        for fam, _, _, _, sa in socket.getaddrinfo(p.hostname, p.port or (443 if p.scheme == "https" else 80)):
            ip = ipaddress.ip_address(sa[0])
            if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_multicast:
                return f"webhook host resolves to a non-public address ({ip})"
    except socket.gaierror as e:
        return f"cannot resolve webhook host: {e}"
    return None


class Notifier:
    """Webhook / e-mail delivery with timeouts, bounded retries and a delivery log."""

    def __init__(self, store: Store, allow_private: bool = False, timeout_s: float = 3.0, max_attempts: int = 3,
                 backoff_s: float = 0.5, async_delivery: bool = True) -> None:
        self.store, self.allow_private = store, allow_private
        self.timeout_s, self.max_attempts, self.backoff_s = timeout_s, max_attempts, backoff_s
        self._q: queue.Queue = queue.Queue(maxsize=1000)
        self._stop = threading.Event()
        self._t = None
        if async_delivery:
            self._t = threading.Thread(target=self._loop, name="notifier", daemon=True)
            self._t.start()

    def submit(self, ws: str, dest: dict[str, Any], alert: dict[str, Any] | None, payload: dict[str, Any]) -> None:
        try:
            if self._t is None:
                self.deliver(ws, dest, alert["id"] if alert else None, payload)
            else:
                self._q.put_nowait((ws, dest, alert["id"] if alert else None, payload))
        except queue.Full:
            self.store.x("INSERT INTO deliveries(workspace_id,destination_id,alert_id,ts,status,attempts,error) VALUES(?,?,?,?,?,?,?)",
                         (ws, dest["id"], alert["id"] if alert else None, time.time(), "dropped", 0, "delivery queue full"))

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                item = self._q.get(timeout=0.3)
            except queue.Empty:
                continue
            try:
                self.deliver(*item)
            except Exception as e:  # never kill the worker; the failure is recorded
                self.store.x("INSERT INTO deliveries(workspace_id,destination_id,alert_id,ts,status,attempts,error) VALUES(?,?,?,?,?,?,?)",
                             (item[0], item[1]["id"], item[2], time.time(), "failed", 0, f"internal error: {e}"[:300]))

    def deliver(self, ws: str, dest: dict[str, Any], alert_id: int | None, payload: dict[str, Any]) -> dict[str, Any]:
        cfg = json.loads(dest["config_json"]) if "config_json" in dest else dest.get("config", {})
        kind, status, http, err, attempts = dest["kind"], "failed", None, None, 0
        if kind == "webhook":
            bad = _safe_webhook_url(cfg.get("url", ""), self.allow_private)
            if bad:
                err, attempts = bad, 0
            else:
                body = json.dumps(payload, default=str).encode()
                for attempts in range(1, self.max_attempts + 1):
                    try:
                        req = urllib.request.Request(cfg["url"], data=body, method="POST",
                                                     headers={"Content-Type": "application/json", "User-Agent": "veylstream/0.1"})
                        with urllib.request.urlopen(req, timeout=self.timeout_s) as r:
                            http = r.status
                        status, err = "delivered", None
                        break
                    except urllib.error.HTTPError as e:
                        http, err = e.code, f"HTTP {e.code}"
                        if 400 <= e.code < 500 and e.code != 429:
                            break                      # permanent client error: do not retry
                    except (urllib.error.URLError, TimeoutError, OSError) as e:
                        err = str(getattr(e, "reason", e))[:200]
                    if attempts < self.max_attempts:
                        time.sleep(self.backoff_s * 2 ** (attempts - 1))
        elif kind == "email":
            host = cfg.get("host") or os.environ.get("VEYLSTREAM_SMTP_HOST")
            if not host:
                status, err = "skipped", "SMTP is not configured (set destination host or VEYLSTREAM_SMTP_HOST)"
            else:
                attempts = 1
                try:
                    msg = EmailMessage()
                    msg["Subject"] = payload.get("title", "Veylstream notification")
                    msg["From"], msg["To"] = cfg.get("from", "veylstream@localhost"), cfg["to"]
                    msg.set_content(json.dumps(payload, indent=2, default=str))
                    with smtplib.SMTP(host, int(cfg.get("port", 25)), timeout=self.timeout_s) as s:
                        if cfg.get("starttls"):
                            s.starttls()
                        if cfg.get("user"):
                            s.login(cfg["user"], os.environ.get(cfg.get("password_env", ""), ""))
                        s.send_message(msg)
                    status = "delivered"
                except Exception as e:
                    err = str(e)[:200]
        else:
            status, err = "skipped", f"unsupported destination kind {kind!r}"
        self.store.x("INSERT INTO deliveries(workspace_id,destination_id,alert_id,ts,status,attempts,http_status,error) VALUES(?,?,?,?,?,?,?,?)",
                     (ws, dest["id"], alert_id, time.time(), status, attempts, http, err))
        return {"status": status, "attempts": attempts, "http_status": http, "error": err}

    def close(self) -> None:
        self._stop.set()
        if self._t:
            self._t.join(timeout=2)


class AlertManager:
    def __init__(self, store: Store, notifier: Notifier | None = None, bus: EventBus | None = None,
                 policy: AlertPolicy | None = None) -> None:
        self.store, self.notifier, self.bus = store, notifier, bus
        self.policy = policy or AlertPolicy()
        self._streak: dict[tuple[str, str], dict[str, Any]] = {}
        self._lock = threading.RLock()

    # ---- audit ------------------------------------------------------------------------
    def _audit(self, alert_id: int, actor: str, action: str, frm: str | None, to: str | None, note: str | None = None) -> None:
        self.store.x("INSERT INTO alert_audit(alert_id,ts,actor,action,from_status,to_status,note) VALUES(?,?,?,?,?,?,?)",
                     (alert_id, time.time(), actor, action, frm, to, note))

    # ---- detections -> alerts ---------------------------------------------------------
    def handle(self, ws: str, items: list[tuple[Detection, int]], simulated: bool = False) -> list[int]:
        """items: (detection, anomaly_row_id). Returns ids of alerts created / updated."""
        touched: list[int] = []
        pol = self.policy
        with self._lock:
            for det, aid in sorted(items, key=lambda x: x[0].window_start):
                if SEV_RANK[det.severity] < SEV_RANK[pol.min_severity]:
                    continue
                key = (ws, det.group_key)
                st = self._streak.get(key)
                step = det.window_end - det.window_start
                if st is None or det.window_start - st["last_ws"] > pol.gap_windows * step + 1e-6:
                    st = self._streak[key] = {"count": 0, "last_ws": -1.0, "items": []}
                if det.window_start > st["last_ws"]:
                    st["count"] += 1
                    st["last_ws"] = det.window_start
                st["items"].append((det, aid))
                immediate = SEV_RANK[det.severity] >= SEV_RANK[pol.immediate_severity]
                if st["count"] >= pol.consecutive_required or immediate:
                    aid_alert = self._attach(ws, det.group_key, st["items"], simulated)
                    st["items"] = []
                    if aid_alert:
                        touched.append(aid_alert)
        return touched

    def _suppressed(self, ws: str, entity: str, metric: str, now: float) -> bool:
        for s in self.store.q("SELECT * FROM suppressions WHERE workspace_id=? AND until>?", (ws, now)):
            if entity.startswith(s["entity_prefix"]) and (not s["metric"] or s["metric"] == metric):
                return True
        return False

    def _attach(self, ws: str, fp: str, items: list[tuple[Detection, int]], simulated: bool) -> int | None:
        best = max((d for d, _ in items), key=lambda d: (SEV_RANK[d.severity], d.score))
        last = max(d.window_end for d, _ in items)
        first = min(d.window_start for d, _ in items)
        now = time.time()
        row = self.store.q1("SELECT * FROM alerts WHERE workspace_id=? AND fingerprint=? AND status!='resolved' ORDER BY id DESC LIMIT 1", (ws, fp))
        event = None
        if row is None:
            prev = self.store.q1("SELECT * FROM alerts WHERE workspace_id=? AND fingerprint=? AND status='resolved' ORDER BY id DESC LIMIT 1", (ws, fp))
            if prev is not None:
                quiet = first - (prev["resolved_event_ts"] or prev["last_seen"])
                if quiet < self.policy.cooldown_s:
                    self.store.xmany("UPDATE anomalies SET alert_id=? WHERE id=? AND alert_id IS NULL", [(prev["id"], a) for _, a in items])
                    return None                      # tail of the incident that was just resolved
                self.store.x("UPDATE alerts SET status='open',severity=?,last_seen=?,detection_count=detection_count+?,updated_at=?,resolved_at=NULL,summary=? WHERE id=?",
                             (best.severity, last, len(items), now, best.explanation, prev["id"]))
                self._audit(prev["id"], "system", "reopened", "resolved", "open", f"new qualifying incident after {quiet:.0f}s quiet")
                row, event = prev["id"], "reopened"
            else:
                if self._suppressed(ws, best.entity, best.metric, now):
                    return None
                row = self.store.x(
                    "INSERT INTO alerts(workspace_id,fingerprint,title,severity,status,entity,metric,first_seen,last_seen,detection_count,created_at,updated_at,simulated,summary) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (ws, fp, f"{best.metric} anomaly on {best.entity}", best.severity, "open", best.entity, best.metric, first, last,
                     len(items), now, now, int(simulated), best.explanation))
                self._audit(row, "system", "created", None, "open", best.explanation)
                event = "created"
        else:
            cur = self.store.q1("SELECT severity FROM alerts WHERE id=?", (row["id"],))
            sev = best.severity if SEV_RANK[best.severity] > SEV_RANK[cur["severity"]] else cur["severity"]
            self.store.x("UPDATE alerts SET severity=?,last_seen=MAX(last_seen,?),detection_count=detection_count+?,updated_at=?,summary=? WHERE id=?",
                         (sev, last, len(items), now, best.explanation if sev == best.severity else row["summary"], row["id"]))
            if sev != cur["severity"]:
                self._audit(row["id"], "system", "escalated", row["status"], row["status"], f"{cur['severity']} -> {sev}")
                event = "escalated" if sev == "critical" else None
            row = row["id"]
        self.store.xmany("UPDATE anomalies SET alert_id=? WHERE id=?", [(row, a) for _, a in items])
        if event:
            self._notify(ws, row, event)
        elif self.bus:
            self.bus.publish(ws, "alert", {"id": row, "event": "updated"})
        return row

    def raise_signal(self, ws: str, fingerprint: str, title: str, severity: str, entity: str, metric: str, summary: str,
                     event_ts: float, simulated: bool = False) -> int | None:
        """Non-detector alert source (data-quality findings, producer silence). Immediate, no streak."""
        d = Detection(detector="data_quality", version="1", entity=entity, metric=metric, window_start=event_ts, window_end=event_ts + 1,
                      observed=0, expected=None, lower=None, upper=None, score=1.0, severity=severity, direction="n/a", explanation=summary)
        with self._lock:
            return self._attach(ws, fingerprint, [(d, 0)], simulated) if not self._suppressed(ws, entity, metric, time.time()) else None

    # ---- manual lifecycle -----------------------------------------------------------------
    def transition(self, ws: str, alert_id: int, to: str, actor: str, note: str | None = None) -> dict[str, Any]:
        a = self.store.q1("SELECT * FROM alerts WHERE id=? AND workspace_id=?", (alert_id, ws))
        if a is None:
            raise KeyError("alert not found")
        if to not in TRANSITIONS.get(a["status"], set()):
            raise InvalidTransition(f"cannot move alert from {a['status']} to {to}")
        now = time.time()
        if to == "resolved":
            self.store.x("UPDATE alerts SET status=?,resolved_at=?,resolved_event_ts=?,updated_at=? WHERE id=?", (to, now, a["last_seen"], now, alert_id))
        else:
            self.store.x("UPDATE alerts SET status=?,updated_at=? WHERE id=?", (to, now, alert_id))
        self._audit(alert_id, actor, "status_change", a["status"], to, note)
        if self.bus:
            self.bus.publish(ws, "alert", {"id": alert_id, "event": f"status:{to}"})
        return self.store.q1("SELECT * FROM alerts WHERE id=?", (alert_id,))  # type: ignore[return-value]

    def assign(self, ws: str, alert_id: int, user_id: str | None, actor: str) -> None:
        a = self.store.q1("SELECT * FROM alerts WHERE id=? AND workspace_id=?", (alert_id, ws))
        if a is None:
            raise KeyError("alert not found")
        if user_id and not self.store.q1("SELECT 1 FROM memberships WHERE user_id=? AND workspace_id=?", (user_id, ws)):
            raise ValueError("assignee is not a member of this workspace")
        self.store.x("UPDATE alerts SET assignee_user_id=?,updated_at=? WHERE id=?", (user_id, time.time(), alert_id))
        self._audit(alert_id, actor, "assigned", a["status"], a["status"], f"assignee={user_id}")

    def add_note(self, ws: str, alert_id: int, actor: str, note: str) -> None:
        if not self.store.q1("SELECT 1 FROM alerts WHERE id=? AND workspace_id=?", (alert_id, ws)):
            raise KeyError("alert not found")
        self._audit(alert_id, actor, "note", None, None, note[:2000])

    def suppress(self, ws: str, entity_prefix: str, until: float, actor: str, metric: str | None = None, reason: str = "") -> int:
        return self.store.x("INSERT INTO suppressions(workspace_id,entity_prefix,metric,until,reason,created_by,created_at) VALUES(?,?,?,?,?,?,?)",
                            (ws, entity_prefix, metric, until, reason, actor, time.time()))

    # ---- notifications ------------------------------------------------------------------------
    def _notify(self, ws: str, alert_id: int, event: str) -> None:
        a = self.store.q1("SELECT * FROM alerts WHERE id=?", (alert_id,))
        if a is None:
            return
        self.store.x("INSERT INTO notifications(workspace_id,ts,title,body,severity,alert_id) VALUES(?,?,?,?,?,?)",
                     (ws, time.time(), f"Alert {event}: {a['title']}", a["summary"], a["severity"], alert_id))
        if self.bus:
            self.bus.publish(ws, "alert", {"id": alert_id, "event": event, "severity": a["severity"], "title": a["title"]})
        if self.notifier is None:
            return
        payload = {"event": f"alert.{event}", "alert": {k: a[k] for k in ("id", "title", "severity", "status", "entity", "metric", "summary", "simulated")},
                   "source": "veylstream"}
        for d in self.store.q("SELECT * FROM destinations WHERE workspace_id=? AND enabled=1", (ws,)):
            if SEV_RANK[a["severity"]] >= SEV_RANK.get(d["min_severity"], 3):
                self.notifier.submit(ws, d, a, payload)
