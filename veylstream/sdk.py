"""Small producer SDK (stdlib only): build valid events, send single/batch with retries.

Retry policy: transient failures (network errors, timeouts, HTTP 408/429/5xx) are retried with exponential
backoff + full jitter, honouring Retry-After. Permanent failures (other 4xx, per-event validation rejections) are
never retried; rejected events are returned to the caller with the server's reason.
"""
from __future__ import annotations

import json
import random
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from .contracts import iso


@dataclass
class DeliveryResult:
    accepted: int = 0
    duplicates: int = 0
    rejected: list[dict[str, Any]] = field(default_factory=list)
    failed: int = 0                 # events not delivered after all retries
    retries: int = 0
    durable: bool | None = None
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.failed == 0 and not self.rejected and self.error is None


def transaction_event(service: str, region: str, status: str, amount: float, currency: str = "USD", latency_ms: float | None = None,
                      failure_reason: str | None = None, payment_method: str = "card", ts: float | None = None, **extra: Any) -> dict[str, Any]:
    e = {"event_id": f"tx-{uuid.uuid4().hex[:16]}", "event_type": "transaction", "event_timestamp": iso(ts or time.time()), "service_name": service,
         "region": region, "status": status, "amount": amount, "currency": currency, "payment_method": payment_method}
    if latency_ms is not None:
        e["latency_ms"] = latency_ms
    if failure_reason:
        e["failure_reason"] = failure_reason
    return {**e, **extra}


def telemetry_event(service: str, region: str, http_status: int, latency_ms: float | None = None, endpoint: str | None = None,
                    ts: float | None = None, **extra: Any) -> dict[str, Any]:
    e = {"event_id": f"tm-{uuid.uuid4().hex[:16]}", "event_type": "telemetry", "event_timestamp": iso(ts or time.time()), "service_name": service,
         "region": region, "http_status": http_status}
    if latency_ms is not None:
        e["latency_ms"] = latency_ms
    if endpoint:
        e["endpoint"] = endpoint
    return {**e, **extra}


class Producer:
    def __init__(self, endpoint: str, api_key: str, timeout_s: float = 5.0, max_retries: int = 4, backoff_base_s: float = 0.25,
                 backoff_cap_s: float = 8.0, batch_size: int = 500) -> None:
        self.endpoint, self.api_key = endpoint.rstrip("/"), api_key
        self.timeout_s, self.max_retries = timeout_s, max_retries
        self.backoff_base_s, self.backoff_cap_s, self.batch_size = backoff_base_s, backoff_cap_s, batch_size

    def _post(self, path: str, body: Any) -> tuple[int, dict[str, Any], dict[str, str]]:
        req = urllib.request.Request(self.endpoint + path, data=json.dumps(body).encode(), method="POST",
                                     headers={"Content-Type": "application/json", "X-API-Key": self.api_key})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_s) as r:
                return r.status, json.loads(r.read() or b"{}"), dict(r.headers)
        except urllib.error.HTTPError as e:
            try:
                payload = json.loads(e.read() or b"{}")
            except json.JSONDecodeError:
                payload = {}
            return e.code, payload, dict(e.headers)

    def _send(self, path: str, body: Any, n_events: int) -> DeliveryResult:
        res = DeliveryResult()
        for attempt in range(self.max_retries + 1):
            try:
                code, payload, hdr = self._post(path, body)
            except (urllib.error.URLError, TimeoutError, OSError) as e:
                code, payload, hdr = 0, {}, {}
                res.error = f"network error: {getattr(e, 'reason', e)}"
            if code in (200, 202, 207):
                d = payload.get("data", payload)
                res.accepted += d.get("accepted", 0)
                res.duplicates += d.get("duplicates", 0)
                res.rejected += d.get("rejected", [])
                res.durable = d.get("durable")
                res.error = None
                return res
            transient = code in (0, 408, 429, 500, 502, 503, 504)
            if code:
                res.error = f"HTTP {code}: {payload.get('error', {}).get('message', '')}"
            if not transient or attempt == self.max_retries:
                break
            retry_after = float(hdr.get("Retry-After", 0) or 0)
            delay = max(retry_after, random.uniform(0, min(self.backoff_cap_s, self.backoff_base_s * 2 ** attempt)))
            res.retries += 1
            time.sleep(delay)
        res.failed = n_events
        return res

    def send(self, event: dict[str, Any]) -> DeliveryResult:
        return self._send("/api/v1/ingest/events", event, 1)

    def send_batch(self, events: list[dict[str, Any]]) -> DeliveryResult:
        total = DeliveryResult()
        for i in range(0, len(events), self.batch_size):
            chunk = events[i:i + self.batch_size]
            r = self._send("/api/v1/ingest/batch", {"events": chunk}, len(chunk))
            total.accepted += r.accepted
            total.duplicates += r.duplicates
            total.failed += r.failed
            total.retries += r.retries
            total.durable = r.durable
            total.error = r.error or total.error
            total.rejected += [{**x, "index": x.get("index", 0) + i} for x in r.rejected]
        return total

    def heartbeat(self) -> DeliveryResult:
        return self._send("/api/v1/ingest/heartbeat", {"event_id": f"hb-{uuid.uuid4().hex[:12]}", "event_type": "heartbeat",
                                                       "event_timestamp": datetime.now(timezone.utc).isoformat()}, 1)
