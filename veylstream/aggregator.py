"""Bounded deduplication and event-time windowed aggregation.

Semantics (identical for live and replay processing):
  * Windows are aligned to multiples of ``slide_s`` (tumbling when slide_s == size_s).
  * watermark = max(event_time seen) - allowed_lateness_s. A window closes (is emitted)
    when its end <= watermark.
  * An event whose every window has already closed is *late*: it is not aggregated, it is
    reported to the caller (dead-lettered with reason ``late_event``) and counted.
  * Empty windows are emitted (count 0) for every scope that has been seen before, so a
    producer that goes silent shows up as a throughput drop instead of a gap.
  * Percentiles are EXACT (numpy linear-interpolation percentile over all latency values in
    the window); memory per open window is O(events in window).
"""
from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Iterable

import numpy as np

from .contracts import Event


class Deduper:
    """Bounded LRU set of event keys. Memory is capped by ``max_entries``."""

    def __init__(self, max_entries: int = 200_000) -> None:
        self.max_entries = max_entries
        self._seen: OrderedDict[str, None] = OrderedDict()
        self.duplicates = 0

    def seen(self, key: str) -> bool:
        """Return True if key was already seen (and count a duplicate); otherwise remember it."""
        if key in self._seen:
            self._seen.move_to_end(key)
            self.duplicates += 1
            return True
        self._seen[key] = None
        if len(self._seen) > self.max_entries:
            self._seen.popitem(last=False)
        return False

    def __len__(self) -> int:
        return len(self._seen)


@dataclass
class WindowMetrics:
    window_start: float
    window_end: float
    entity: str                      # "global" | "service:<x>" | "region:<x>"
    metrics: dict[str, float | None]
    n_events: int

    def to_dict(self) -> dict:
        return {"window_start": self.window_start, "window_end": self.window_end, "entity": self.entity,
                "metrics": self.metrics, "n_events": self.n_events}


@dataclass
class _Acc:
    n: int = 0
    txn: int = 0
    txn_failed: int = 0
    req: float = 0.0
    req_err: float = 0.0
    lat: list[float] = field(default_factory=list)
    entities: set[str] = field(default_factory=set)
    min_offset: int | None = None

    def add(self, e: Event, offset: int | None) -> None:
        self.n += 1
        if e.event_type == "transaction":
            if e.status in ("success", "failed"):
                self.txn += 1
                self.txn_failed += e.status == "failed"
        elif e.event_type == "telemetry":
            rc = e.measures.get("request_count", 1.0)
            self.req += rc
            if e.status == "server_error":
                self.req_err += rc
        if e.latency_ms is not None:
            self.lat.append(e.latency_ms)
        if e.entity_id:
            self.entities.add(e.entity_id)
        if offset is not None and (self.min_offset is None or offset < self.min_offset):
            self.min_offset = offset


def _scopes(e: Event) -> list[str]:
    s = ["global"]
    if e.service:
        s.append(f"service:{e.service}")
    if e.region:
        s.append(f"region:{e.region}")
    return s


class WindowAggregator:
    def __init__(self, size_s: float = 10.0, slide_s: float | None = None, allowed_lateness_s: float = 5.0,
                 min_txn_for_rate: int = 5, min_req_for_rate: int = 5) -> None:
        self.size_s = float(size_s)
        self.slide_s = float(slide_s or size_s)
        if self.slide_s > self.size_s:
            raise ValueError("slide_s must be <= size_s")
        self.allowed_lateness_s = float(allowed_lateness_s)
        self.min_txn = min_txn_for_rate
        self.min_req = min_req_for_rate
        self.max_event_ts: float | None = None
        self.watermark: float = float("-inf")
        self._open: dict[tuple[float, str], _Acc] = {}
        self._known_scopes: set[str] = set()
        self._next_empty_start: float | None = None
        self.late_dropped = 0
        self.accepted = 0

    # -- window geometry -------------------------------------------------
    def windows_for(self, ts: float) -> list[float]:
        """Start times of all windows containing ts (one for tumbling)."""
        last = np.floor(ts / self.slide_s) * self.slide_s
        starts = []
        s = last
        while s > ts - self.size_s:
            starts.append(float(s))
            s -= self.slide_s
        return starts

    def restore_watermark(self, watermark: float, max_event_ts: float | None) -> None:
        self.watermark = watermark
        self.max_event_ts = max_event_ts
        if watermark != float("-inf"):
            self._next_empty_start = float(np.floor(watermark / self.slide_s) * self.slide_s)

    def seed_scopes(self, scopes: Iterable[str]) -> None:
        self._known_scopes.update(scopes)

    def advance_clock(self, now: float) -> list[WindowMetrics]:
        """Processing-time advance for idle live streams: lets silent producers produce empty windows."""
        if self.max_event_ts is None or now <= self.max_event_ts:
            return []
        self.max_event_ts = now
        return self._advance()

    # -- ingestion -------------------------------------------------------
    def add(self, e: Event, offset: int | None = None, silent_late: bool = False) -> tuple[bool, list[WindowMetrics]]:
        """Add one event. Returns (accepted, windows_closed_as_a_result)."""
        starts = [s for s in self.windows_for(e.ts) if s + self.size_s > self.watermark]
        if not starts:
            if not silent_late:
                self.late_dropped += 1
            return False, []
        for s in starts:
            for scope in _scopes(e):
                acc = self._open.get((s, scope))
                if acc is None:
                    acc = self._open[(s, scope)] = _Acc()
                acc.add(e, offset)
                self._known_scopes.add(scope)
        self.accepted += 1
        if self.max_event_ts is None or e.ts > self.max_event_ts:
            self.max_event_ts = e.ts
            return True, self._advance()
        return True, []

    def _advance(self) -> list[WindowMetrics]:
        wm = (self.max_event_ts or 0.0) - self.allowed_lateness_s
        if wm <= self.watermark:
            return []
        self.watermark = wm
        return self._close_through(wm)

    def flush(self) -> list[WindowMetrics]:
        """Close every open window regardless of watermark (end of replay / shutdown)."""
        if not self._open:
            return []
        far = max(s for s, _ in self._open) + self.size_s
        self.watermark = max(self.watermark, far)
        return self._close_through(far)

    def _close_through(self, wm: float) -> list[WindowMetrics]:
        out: list[WindowMetrics] = []
        closing = sorted({s for s, _ in self._open if s + self.size_s <= wm})
        if self._next_empty_start is None and closing:
            self._next_empty_start = closing[0]
        start = self._next_empty_start
        if start is not None:
            # iterate every slide-aligned window that is now complete, emitting empties for known scopes
            s = start
            while s + self.size_s <= wm:
                for scope in sorted(self._known_scopes):
                    acc = self._open.pop((s, scope), None)
                    out.append(self._finalize(s, scope, acc or _Acc()))
                s += self.slide_s
            self._next_empty_start = s
        return out

    def _finalize(self, start: float, scope: str, a: _Acc) -> WindowMetrics:
        m: dict[str, float | None] = {
            "event_rate": a.n / self.size_s, "event_count": float(a.n),
            "txn_count": float(a.txn), "req_count": float(a.req),
            "txn_failure_rate": (a.txn_failed / a.txn) if a.txn >= self.min_txn else None,
            "http_error_rate": (a.req_err / a.req) if a.req >= self.min_req else None,
            "unique_entities": float(len(a.entities)),
        }
        if a.lat:
            arr = np.asarray(a.lat, dtype=float)
            p50, p95, p99 = np.percentile(arr, [50, 95, 99])
            m.update(latency_mean_ms=float(arr.mean()), latency_p50_ms=float(p50), latency_p95_ms=float(p95),
                     latency_p99_ms=float(p99), latency_min_ms=float(arr.min()), latency_max_ms=float(arr.max()))
        else:
            m.update(latency_mean_ms=None, latency_p50_ms=None, latency_p95_ms=None, latency_p99_ms=None,
                     latency_min_ms=None, latency_max_ms=None)
        return WindowMetrics(start, start + self.size_s, scope, m, a.n)

    # -- state introspection --------------------------------------------
    def min_open_offset(self) -> int | None:
        offs = [a.min_offset for a in self._open.values() if a.min_offset is not None]
        return min(offs) if offs else None

    def open_windows(self) -> int:
        return len({s for s, _ in self._open})

    def known_scopes(self) -> Iterable[str]:
        return sorted(self._known_scopes)
