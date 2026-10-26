"""In-process metrics registry and a bounded pub/sub bus used for live (SSE) updates."""
from __future__ import annotations

import json
import queue
import threading
import time
from collections import defaultdict, deque
from typing import Any


class Metrics:
    """Thread-safe counters plus a per-second throughput ring (last ``horizon_s`` seconds)."""

    def __init__(self, horizon_s: int = 600) -> None:
        self._lock = threading.Lock()
        self.counters: dict[str, float] = defaultdict(float)
        self._ring: deque[list[float]] = deque(maxlen=horizon_s)   # [sec, accepted, rejected, processed]
        self._delay_ms: deque[float] = deque(maxlen=2000)
        self._detect_ms: deque[float] = deque(maxlen=500)
        self.started = time.time()

    def inc(self, name: str, n: float = 1.0) -> None:
        with self._lock:
            self.counters[name] += n

    def tick(self, col: int, n: int = 1) -> None:
        sec = int(time.time())
        with self._lock:
            if not self._ring or self._ring[-1][0] != sec:
                self._ring.append([sec, 0, 0, 0])
            self._ring[-1][col] += n

    def observe_delay(self, ms: float) -> None:
        with self._lock:
            self._delay_ms.append(ms)

    def observe_detect(self, ms: float) -> None:
        with self._lock:
            self._detect_ms.append(ms)

    def rate(self, col: int, span_s: int = 10) -> float:
        now = int(time.time())
        with self._lock:
            tot = sum(r[col] for r in self._ring if now - span_s < r[0] <= now)
        return tot / span_s

    def series(self, span_s: int = 120) -> list[dict[str, float]]:
        now = int(time.time())
        with self._lock:
            m = {int(r[0]): r for r in self._ring}
        return [{"t": s, "accepted": m[s][1] if s in m else 0, "rejected": m[s][2] if s in m else 0,
                 "processed": m[s][3] if s in m else 0} for s in range(now - span_s + 1, now + 1)]

    @staticmethod
    def _pct(vals: list[float], q: float) -> float | None:
        if not vals:
            return None
        v = sorted(vals)
        return v[min(len(v) - 1, int(q * len(v)))]

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            d, dd = list(self._delay_ms), list(self._detect_ms)
            c = dict(self.counters)
        return {"counters": c, "ingest_rate_10s": round(self.rate(1), 2), "processed_rate_10s": round(self.rate(3), 2),
                "processing_delay_ms": {"p50": self._pct(d, .5), "p95": self._pct(d, .95), "samples": len(d)},
                "detection_latency_ms": {"p50": self._pct(dd, .5), "p95": self._pct(dd, .95), "samples": len(dd)},
                "uptime_s": round(time.time() - self.started, 1)}


class EventBus:
    """Fan-out bus. Each subscriber has a bounded queue; slow consumers drop the oldest message."""

    def __init__(self, maxsize: int = 500) -> None:
        self._subs: dict[int, tuple[str, queue.Queue]] = {}
        self._lock = threading.Lock()
        self._next = 0
        self.maxsize = maxsize
        self.dropped = 0

    def subscribe(self, workspace_id: str) -> tuple[int, queue.Queue]:
        q: queue.Queue = queue.Queue(self.maxsize)
        with self._lock:
            self._next += 1
            self._subs[self._next] = (workspace_id, q)
            return self._next, q

    def unsubscribe(self, sid: int) -> None:
        with self._lock:
            self._subs.pop(sid, None)

    def publish(self, workspace_id: str, kind: str, data: Any) -> None:
        msg = json.dumps({"type": kind, "data": data}, default=str)
        with self._lock:
            targets = [q for ws, q in self._subs.values() if ws == workspace_id]
        for q in targets:
            try:
                q.put_nowait(msg)
            except queue.Full:
                try:
                    q.get_nowait()
                    q.put_nowait(msg)
                    self.dropped += 1
                except (queue.Empty, queue.Full):
                    pass

    def subscriber_count(self) -> int:
        with self._lock:
            return len(self._subs)
