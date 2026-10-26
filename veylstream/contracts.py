"""Versioned event contracts, validation and normalization.

Timestamp convention: every timestamp is stored internally as UTC epoch seconds
(float). Inputs may be ISO-8601 strings (naive values are treated as UTC) or
numeric epoch seconds / milliseconds. Output uses ISO-8601 with a trailing 'Z'.
"""
from __future__ import annotations

import math
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

SCHEMA_VERSION = "1.0"
SUPPORTED_SCHEMA_VERSIONS = {"1.0"}
EVENT_TYPES = {"transaction", "telemetry", "heartbeat"}
TXN_STATUSES = {"success", "failed", "pending"}
PAYMENT_METHODS = {"card", "wallet", "bank_transfer", "bnpl", "other"}
MAX_FUTURE_SKEW_S = 300.0
MAX_EXTRA_KEYS = 20
_ID_RE = re.compile(r"^[A-Za-z0-9_.:\-]{1,128}$")
_CCY_RE = re.compile(r"^[A-Z]{3}$")

KNOWN_FIELDS = {
    "event_id", "schema_version", "event_type", "event_timestamp", "source_type", "environment",
    "service_name", "region", "entity_id", "trace_id", "correlation_id", "transaction_id", "amount",
    "currency", "status", "payment_method", "latency_ms", "failure_reason", "http_status", "endpoint",
    "cpu_pct", "mem_pct", "request_count", "payload_metadata",
}


@dataclass(slots=True)
class Event:
    event_id: str
    event_type: str
    ts: float
    ingest_ts: float
    source_id: str
    source_type: str = "application"
    environment: str = "demo"
    service: str = ""
    region: str = ""
    entity_id: str = ""
    trace_id: str | None = None
    correlation_id: str | None = None
    status: str = ""           # transaction: success|failed|pending; telemetry: ok|client_error|server_error
    latency_ms: float | None = None
    measures: dict[str, float] = field(default_factory=dict)
    attrs: dict[str, Any] = field(default_factory=dict)
    schema_version: str = SCHEMA_VERSION
    simulated: bool = False
    replay_id: str | None = None

    @property
    def is_failure(self) -> bool:
        return self.status in ("failed", "server_error")

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id, "schema_version": self.schema_version, "event_type": self.event_type,
            "event_timestamp": iso(self.ts), "ingestion_timestamp": iso(self.ingest_ts),
            "source_id": self.source_id, "source_type": self.source_type, "environment": self.environment,
            "service_name": self.service, "region": self.region, "entity_id": self.entity_id,
            "trace_id": self.trace_id, "correlation_id": self.correlation_id, "status": self.status,
            "latency_ms": self.latency_ms, "measures": self.measures, "attributes": self.attrs,
            "simulated": self.simulated, "replay_id": self.replay_id,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Event":
        return cls(
            event_id=d["event_id"], event_type=d["event_type"], ts=parse_ts(d["event_timestamp"]) or 0.0,
            ingest_ts=parse_ts(d["ingestion_timestamp"]) or 0.0, source_id=d["source_id"],
            source_type=d.get("source_type", "application"), environment=d.get("environment", "demo"),
            service=d.get("service_name", ""), region=d.get("region", ""), entity_id=d.get("entity_id", ""),
            trace_id=d.get("trace_id"), correlation_id=d.get("correlation_id"), status=d.get("status", ""),
            latency_ms=d.get("latency_ms"), measures=d.get("measures") or {}, attrs=d.get("attributes") or {},
            schema_version=d.get("schema_version", SCHEMA_VERSION), simulated=bool(d.get("simulated")),
            replay_id=d.get("replay_id"),
        )


@dataclass(slots=True)
class Problem:
    code: str
    field: str
    message: str

    def to_dict(self) -> dict[str, str]:
        return {"code": self.code, "field": self.field, "message": self.message}


@dataclass(slots=True)
class ValidationResult:
    event: Event | None
    problems: list[Problem]

    @property
    def ok(self) -> bool:
        return self.event is not None and not self.problems


def iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def parse_ts(value: Any) -> float | None:
    """Parse ISO-8601 / epoch seconds / epoch millis into UTC epoch seconds. None if invalid."""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        v = float(value)
        if not math.isfinite(v) or v <= 0:
            return None
        return v / 1000.0 if v > 1e11 else v
    if isinstance(value, str) and value.strip():
        s = value.strip()
        if re.fullmatch(r"\d+(\.\d+)?", s):
            return parse_ts(float(s))
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        except ValueError:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    return None


def _num(raw: dict, key: str, probs: list[Problem], lo: float | None = None, hi: float | None = None,
         required: bool = False) -> float | None:
    if key not in raw or raw[key] is None:
        if required:
            probs.append(Problem("missing_field", key, f"{key} is required"))
        return None
    v = raw[key]
    if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(float(v)):
        probs.append(Problem("invalid_type", key, f"{key} must be a finite number"))
        return None
    v = float(v)
    if (lo is not None and v < lo) or (hi is not None and v > hi):
        probs.append(Problem("out_of_range", key, f"{key}={v} outside [{lo}, {hi}]"))
        return None
    return v


def _str(raw: dict, key: str, probs: list[Problem], required: bool = False, ident: bool = False,
         maxlen: int = 128) -> str | None:
    v = raw.get(key)
    if v is None or v == "":
        if required:
            probs.append(Problem("missing_field", key, f"{key} is required"))
        return None
    if not isinstance(v, str):
        probs.append(Problem("invalid_type", key, f"{key} must be a string"))
        return None
    if len(v) > maxlen or (ident and not _ID_RE.match(v)):
        probs.append(Problem("invalid_value", key, f"{key} has invalid format or length"))
        return None
    return v


def validate_event(raw: Any, *, source_id: str, source_type: str = "application", simulated: bool = False,
                   now: float | None = None, replay_id: str | None = None,
                   allow_future: bool = False) -> ValidationResult:
    """Validate a raw payload and normalize it into an Event. Never raises on bad input."""
    now = time.time() if now is None else now
    probs: list[Problem] = []
    if not isinstance(raw, dict):
        return ValidationResult(None, [Problem("invalid_type", "$", "event must be a JSON object")])

    version = raw.get("schema_version", SCHEMA_VERSION)
    if version not in SUPPORTED_SCHEMA_VERSIONS:
        probs.append(Problem("schema_mismatch", "schema_version",
                             f"unsupported schema_version {version!r}; supported: {sorted(SUPPORTED_SCHEMA_VERSIONS)}"))
    event_id = _str(raw, "event_id", probs, required=True, ident=True)
    etype = raw.get("event_type")
    if etype is None:
        probs.append(Problem("missing_field", "event_type", "event_type is required"))
    elif etype not in EVENT_TYPES:
        probs.append(Problem("invalid_value", "event_type", f"event_type must be one of {sorted(EVENT_TYPES)}"))
    ts = None
    if raw.get("event_timestamp") is None:
        probs.append(Problem("missing_field", "event_timestamp", "event_timestamp is required"))
    else:
        ts = parse_ts(raw["event_timestamp"])
        if ts is None:
            probs.append(Problem("invalid_timestamp", "event_timestamp", "unparseable event_timestamp"))
        elif ts > now + MAX_FUTURE_SKEW_S and not allow_future:
            probs.append(Problem("timestamp_in_future", "event_timestamp",
                                 f"timestamp more than {int(MAX_FUTURE_SKEW_S)}s in the future"))

    service = _str(raw, "service_name", probs, required=etype in ("transaction", "telemetry"), ident=True)
    region = _str(raw, "region", probs, required=etype in ("transaction", "telemetry"), ident=True)
    entity = _str(raw, "entity_id", probs, ident=True)
    trace = _str(raw, "trace_id", probs, ident=True)
    corr = _str(raw, "correlation_id", probs, ident=True)
    env = _str(raw, "environment", probs, ident=True) or "demo"
    latency = _num(raw, "latency_ms", probs, 0, 600_000)
    measures: dict[str, float] = {}
    attrs: dict[str, Any] = {}
    status = ""

    if etype == "transaction":
        status_in = raw.get("status")
        if status_in not in TXN_STATUSES:
            probs.append(Problem("invalid_value" if status_in is not None else "missing_field", "status",
                                 f"status must be one of {sorted(TXN_STATUSES)}"))
        else:
            status = status_in
        amt = _num(raw, "amount", probs, 0, 1e9, required=True)
        if amt is not None:
            measures["amount"] = amt
        ccy = raw.get("currency")
        if ccy is None:
            probs.append(Problem("missing_field", "currency", "currency is required"))
        elif not (isinstance(ccy, str) and _CCY_RE.match(ccy)):
            probs.append(Problem("invalid_value", "currency", "currency must be a 3-letter uppercase code"))
        else:
            attrs["currency"] = ccy
        pm = raw.get("payment_method", "other")
        if pm not in PAYMENT_METHODS:
            probs.append(Problem("invalid_value", "payment_method", f"payment_method must be one of {sorted(PAYMENT_METHODS)}"))
        else:
            attrs["payment_method"] = pm
        tid = _str(raw, "transaction_id", probs, ident=True)
        attrs["transaction_id"] = tid or event_id
        fr = _str(raw, "failure_reason", probs)
        if fr:
            if status and status != "failed":
                probs.append(Problem("invalid_value", "failure_reason", "failure_reason only allowed when status=failed"))
            else:
                attrs["failure_reason"] = fr
    elif etype == "telemetry":
        hs = _num(raw, "http_status", probs, 100, 599, required=True)
        if hs is not None:
            h = int(hs)
            attrs["http_status"] = h
            status = "server_error" if h >= 500 else "client_error" if h >= 400 else "ok"
        ep = _str(raw, "endpoint", probs, maxlen=128)
        if ep:
            attrs["endpoint"] = ep
        for k in ("cpu_pct", "mem_pct"):
            v = _num(raw, k, probs, 0, 100)
            if v is not None:
                measures[k] = v
        rc = _num(raw, "request_count", probs, 1, 1e9)
        measures["request_count"] = rc if rc is not None else 1.0
    elif etype == "heartbeat":
        status = "ok"

    extra = {k: v for k, v in raw.items() if k not in KNOWN_FIELDS}
    if extra:
        safe = {}
        for k, v in list(extra.items())[:MAX_EXTRA_KEYS]:
            if isinstance(k, str) and len(k) <= 64 and isinstance(v, (str, int, float, bool)) \
                    and (not isinstance(v, str) or len(v) <= 256):
                safe[k] = v
        if safe:
            attrs["_unknown"] = safe
    meta = raw.get("payload_metadata")
    if isinstance(meta, dict) and len(str(meta)) <= 1024:
        attrs["payload_metadata"] = meta

    if probs:
        return ValidationResult(None, probs)
    ev = Event(event_id=event_id, event_type=etype, ts=ts, ingest_ts=now, source_id=source_id,  # type: ignore[arg-type]
               source_type=_str(raw, "source_type", [], ident=True) or source_type, environment=env,
               service=service or "", region=region or "", entity_id=entity or "", trace_id=trace,
               correlation_id=corr, status=status, latency_ms=latency, measures=measures, attrs=attrs,
               schema_version=version, simulated=simulated, replay_id=replay_id)
    return ValidationResult(ev, [])


def schema_description() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION, "supported_versions": sorted(SUPPORTED_SCHEMA_VERSIONS),
        "event_types": sorted(EVENT_TYPES),
        "common_required": ["event_id", "event_type", "event_timestamp"],
        "required_by_type": {
            "transaction": ["service_name", "region", "status", "amount", "currency"],
            "telemetry": ["service_name", "region", "http_status"],
            "heartbeat": [],
        },
        "enums": {"transaction.status": sorted(TXN_STATUSES), "payment_method": sorted(PAYMENT_METHODS)},
        "timestamps": "ISO-8601 or epoch seconds/millis; naive values are UTC; max 300s in the future",
        "unknown_fields": "kept (scalar values only, max 20 keys) under attributes._unknown",
    }
