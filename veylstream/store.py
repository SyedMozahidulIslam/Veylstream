"""SQLite persistence with versioned migrations.

SQLite is the zero-dependency local profile. The schema is plain SQL and uses only portable types so
it maps to PostgreSQL (partition ``events``/``windows`` by time or use TimescaleDB for volume).
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
import sqlite3
import threading
import time
from typing import Any, Iterable, Sequence

MIGRATIONS: list[tuple[int, str]] = [(1, """
CREATE TABLE workspaces(id TEXT PRIMARY KEY, name TEXT NOT NULL, created_at REAL NOT NULL, settings_json TEXT NOT NULL DEFAULT '{}');
CREATE TABLE users(id TEXT PRIMARY KEY, email TEXT NOT NULL UNIQUE, display_name TEXT NOT NULL, pw_hash TEXT NOT NULL, created_at REAL NOT NULL);
CREATE TABLE memberships(user_id TEXT NOT NULL, workspace_id TEXT NOT NULL, role TEXT NOT NULL CHECK(role IN ('admin','operator','analyst','viewer')), PRIMARY KEY(user_id, workspace_id));
CREATE TABLE sessions(token_hash TEXT PRIMARY KEY, user_id TEXT NOT NULL, created_at REAL NOT NULL, expires_at REAL NOT NULL);
CREATE TABLE sources(id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL, name TEXT NOT NULL, source_type TEXT NOT NULL, simulated INTEGER NOT NULL DEFAULT 0,
  key_hash TEXT NOT NULL, key_prefix TEXT NOT NULL, created_at REAL NOT NULL, revoked_at REAL, last_event_ts REAL, last_seen_wall REAL, event_count INTEGER NOT NULL DEFAULT 0,
  heartbeat_interval_s REAL, rate_limit_per_s REAL NOT NULL DEFAULT 2000);
CREATE INDEX ix_sources_ws ON sources(workspace_id);
CREATE UNIQUE INDEX ix_sources_key ON sources(key_hash);
CREATE TABLE events(workspace_id TEXT NOT NULL, scope TEXT NOT NULL DEFAULT 'live', event_id TEXT NOT NULL, event_type TEXT NOT NULL, ts REAL NOT NULL, ingest_ts REAL NOT NULL,
  source_id TEXT NOT NULL, service TEXT, region TEXT, entity_id TEXT, status TEXT, latency_ms REAL, correlation_id TEXT, trace_id TEXT, simulated INTEGER NOT NULL DEFAULT 0,
  payload_json TEXT NOT NULL, PRIMARY KEY(workspace_id, scope, event_id));
CREATE INDEX ix_events_ts ON events(workspace_id, scope, ts);
CREATE INDEX ix_events_svc ON events(workspace_id, scope, service, region, ts);
CREATE INDEX ix_events_corr ON events(workspace_id, correlation_id);
CREATE TABLE windows(workspace_id TEXT NOT NULL, scope TEXT NOT NULL DEFAULT 'live', window_start REAL NOT NULL, window_s REAL NOT NULL, entity TEXT NOT NULL,
  n_events INTEGER NOT NULL, metrics_json TEXT NOT NULL, PRIMARY KEY(workspace_id, scope, window_s, window_start, entity));
CREATE TABLE anomalies(id INTEGER PRIMARY KEY AUTOINCREMENT, workspace_id TEXT NOT NULL, scope TEXT NOT NULL DEFAULT 'live', dedup_key TEXT NOT NULL, group_key TEXT NOT NULL,
  detector TEXT NOT NULL, version TEXT NOT NULL, entity TEXT NOT NULL, metric TEXT NOT NULL, window_start REAL NOT NULL, window_end REAL NOT NULL, observed REAL, expected REAL,
  lower REAL, upper REAL, score REAL NOT NULL, severity TEXT NOT NULL, direction TEXT NOT NULL, explanation TEXT NOT NULL, detail_json TEXT NOT NULL, detected_at REAL NOT NULL,
  alert_id INTEGER, UNIQUE(workspace_id, scope, dedup_key));
CREATE INDEX ix_anom_ts ON anomalies(workspace_id, scope, window_start);
CREATE INDEX ix_anom_alert ON anomalies(alert_id);
CREATE TABLE alerts(id INTEGER PRIMARY KEY AUTOINCREMENT, workspace_id TEXT NOT NULL, fingerprint TEXT NOT NULL, title TEXT NOT NULL, severity TEXT NOT NULL, status TEXT NOT NULL,
  entity TEXT NOT NULL, metric TEXT NOT NULL, first_seen REAL NOT NULL, last_seen REAL NOT NULL, detection_count INTEGER NOT NULL DEFAULT 1, assignee_user_id TEXT,
  created_at REAL NOT NULL, updated_at REAL NOT NULL, resolved_at REAL, resolved_event_ts REAL, simulated INTEGER NOT NULL DEFAULT 0, summary TEXT);
CREATE INDEX ix_alerts_ws ON alerts(workspace_id, status, severity);
CREATE INDEX ix_alerts_fp ON alerts(workspace_id, fingerprint);
CREATE TABLE alert_audit(id INTEGER PRIMARY KEY AUTOINCREMENT, alert_id INTEGER NOT NULL, ts REAL NOT NULL, actor TEXT NOT NULL, action TEXT NOT NULL, from_status TEXT, to_status TEXT, note TEXT);
CREATE INDEX ix_audit_alert ON alert_audit(alert_id);
CREATE TABLE suppressions(id INTEGER PRIMARY KEY AUTOINCREMENT, workspace_id TEXT NOT NULL, entity_prefix TEXT NOT NULL, metric TEXT, until REAL NOT NULL, reason TEXT, created_by TEXT, created_at REAL NOT NULL);
CREATE TABLE dead_letters(id INTEGER PRIMARY KEY AUTOINCREMENT, workspace_id TEXT NOT NULL, scope TEXT NOT NULL DEFAULT 'live', ts REAL NOT NULL, source_id TEXT, reason_code TEXT NOT NULL, detail TEXT, raw TEXT);
CREATE INDEX ix_dl ON dead_letters(workspace_id, scope, ts);
CREATE TABLE dq_rules(id INTEGER PRIMARY KEY AUTOINCREMENT, workspace_id TEXT NOT NULL, name TEXT NOT NULL, kind TEXT NOT NULL, scope TEXT NOT NULL DEFAULT '*', params_json TEXT NOT NULL,
  severity TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1, owner TEXT, state TEXT NOT NULL DEFAULT 'ok', UNIQUE(workspace_id, name));
CREATE TABLE dq_findings(id INTEGER PRIMARY KEY AUTOINCREMENT, workspace_id TEXT NOT NULL, rule_id INTEGER NOT NULL, ts REAL NOT NULL, severity TEXT NOT NULL, status TEXT NOT NULL, details_json TEXT NOT NULL);
CREATE TABLE destinations(id INTEGER PRIMARY KEY AUTOINCREMENT, workspace_id TEXT NOT NULL, kind TEXT NOT NULL, name TEXT NOT NULL, config_json TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1, min_severity TEXT NOT NULL DEFAULT 'high');
CREATE TABLE deliveries(id INTEGER PRIMARY KEY AUTOINCREMENT, workspace_id TEXT NOT NULL, destination_id INTEGER NOT NULL, alert_id INTEGER, ts REAL NOT NULL, status TEXT NOT NULL, attempts INTEGER NOT NULL, http_status INTEGER, error TEXT);
CREATE TABLE notifications(id INTEGER PRIMARY KEY AUTOINCREMENT, workspace_id TEXT NOT NULL, ts REAL NOT NULL, title TEXT NOT NULL, body TEXT, severity TEXT NOT NULL, alert_id INTEGER, read INTEGER NOT NULL DEFAULT 0);
CREATE TABLE datasets(id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL, name TEXT NOT NULL, path TEXT NOT NULL, format TEXT NOT NULL, rows_valid INTEGER NOT NULL, rows_rejected INTEGER NOT NULL,
  sha256 TEXT NOT NULL, ts_min REAL, ts_max REAL, has_labels INTEGER NOT NULL DEFAULT 0, created_at REAL NOT NULL, rejects_json TEXT NOT NULL DEFAULT '[]', meta_json TEXT NOT NULL DEFAULT '{}');
CREATE TABLE replay_jobs(id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL, dataset_id TEXT NOT NULL, config_json TEXT NOT NULL, status TEXT NOT NULL, progress REAL NOT NULL DEFAULT 0,
  processed INTEGER NOT NULL DEFAULT 0, started_at REAL, finished_at REAL, result_json TEXT, error TEXT);
CREATE TABLE experiments(id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL, name TEXT NOT NULL, config_json TEXT NOT NULL, dataset_hash TEXT, status TEXT NOT NULL,
  started_at REAL, finished_at REAL, results_json TEXT, software_version TEXT NOT NULL, seed INTEGER, error TEXT);
CREATE TABLE audit_events(id INTEGER PRIMARY KEY AUTOINCREMENT, workspace_id TEXT, ts REAL NOT NULL, actor TEXT NOT NULL, action TEXT NOT NULL, target TEXT, detail TEXT);
CREATE TABLE checkpoints(name TEXT PRIMARY KEY, next_offset INTEGER NOT NULL, processed_upto INTEGER NOT NULL DEFAULT 0, watermark REAL, max_event_ts REAL, updated_at REAL NOT NULL);
""")]


def hash_password(pw: str) -> str:
    salt = secrets.token_bytes(16)
    h = hashlib.scrypt(pw.encode(), salt=salt, n=2 ** 14, r=8, p=1, dklen=32)
    return f"scrypt$16384$8$1${salt.hex()}${h.hex()}"


def verify_password(pw: str, stored: str) -> bool:
    try:
        _, n, r, p, salt, h = stored.split("$")
        calc = hashlib.scrypt(pw.encode(), salt=bytes.fromhex(salt), n=int(n), r=int(r), p=int(p), dklen=32)
        return hmac.compare_digest(calc.hex(), h)
    except (ValueError, TypeError):
        return False


def sha256(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()


def new_id(prefix: str) -> str:
    return f"{prefix}_{secrets.token_hex(6)}"


class Store:
    def __init__(self, path: str = ":memory:") -> None:
        self.path = path
        if path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
        self.migrate()

    def migrate(self) -> int:
        with self._lock:
            self._conn.execute("CREATE TABLE IF NOT EXISTS schema_migrations(version INTEGER PRIMARY KEY, applied_at REAL NOT NULL)")
            done = {r[0] for r in self._conn.execute("SELECT version FROM schema_migrations")}
            for v, sql in MIGRATIONS:
                if v in done:
                    continue
                self._conn.execute("BEGIN")
                try:
                    for stmt in [s for s in sql.split(";\n") if s.strip()]:
                        self._conn.execute(stmt)
                    self._conn.execute("INSERT INTO schema_migrations VALUES(?,?)", (v, time.time()))
                    self._conn.execute("COMMIT")
                except Exception:
                    self._conn.execute("ROLLBACK")
                    raise
            return max(v for v, _ in MIGRATIONS)

    def schema_version(self) -> int:
        return self.q1("SELECT MAX(version) v FROM schema_migrations")["v"]

    def q(self, sql: str, args: Sequence[Any] = ()) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(r) for r in self._conn.execute(sql, args).fetchall()]

    def q1(self, sql: str, args: Sequence[Any] = ()) -> dict[str, Any] | None:
        rows = self.q(sql, args)
        return rows[0] if rows else None

    def x(self, sql: str, args: Sequence[Any] = ()) -> int:
        with self._lock:
            cur = self._conn.execute(sql, args)
            return (cur.lastrowid or 0) if sql.lstrip().upper().startswith("INSERT") else cur.rowcount

    def xmany(self, sql: str, rows: Iterable[Sequence[Any]]) -> None:
        with self._lock:
            self._conn.execute("BEGIN")
            try:
                self._conn.executemany(sql, rows)
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ---- domain helpers shared across modules -------------------------------------
    def audit(self, ws: str | None, actor: str, action: str, target: str = "", detail: str = "") -> None:
        self.x("INSERT INTO audit_events(workspace_id,ts,actor,action,target,detail) VALUES(?,?,?,?,?,?)",
               (ws, time.time(), actor, action, target, detail[:500]))

    def setting(self, ws: str, key: str, default: Any = None) -> Any:
        r = self.q1("SELECT settings_json FROM workspaces WHERE id=?", (ws,))
        return json.loads(r["settings_json"]).get(key, default) if r else default

    def set_settings(self, ws: str, updates: dict[str, Any]) -> dict[str, Any]:
        r = self.q1("SELECT settings_json FROM workspaces WHERE id=?", (ws,))
        cur = json.loads(r["settings_json"]) if r else {}
        cur.update(updates)
        self.x("UPDATE workspaces SET settings_json=? WHERE id=?", (json.dumps(cur), ws))
        return cur

    def apply_retention(self, ws: str, event_days: float, window_days: float, now: float | None = None) -> dict[str, int]:
        """Delete live events / windows / dead-letters older than the retention horizon (replay scopes excluded)."""
        now = now or time.time()
        n1 = self.x("DELETE FROM events WHERE workspace_id=? AND scope='live' AND ts<?", (ws, now - event_days * 86400))
        n2 = self.x("DELETE FROM windows WHERE workspace_id=? AND scope='live' AND window_start<?", (ws, now - window_days * 86400))
        n3 = self.x("DELETE FROM dead_letters WHERE workspace_id=? AND scope='live' AND ts<?", (ws, now - event_days * 86400))
        return {"events": n1, "windows": n2, "dead_letters": n3}
