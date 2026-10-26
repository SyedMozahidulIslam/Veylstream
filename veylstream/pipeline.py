"""Ingestion + stream processing.

Durability policy: an event is acknowledged as *accepted* only after the broker append returned.
With ``FileLogBroker(fsync=True)`` that means the batch was flushed and fsync'd to disk
(ack field ``durable: true``). With a non-durable broker the ack says ``durable: false``.

Processing semantics: at-least-once from the broker with idempotent sinks (events are INSERT OR
IGNORE by event id; windows/anomalies are keyed). Checkpoints store the oldest offset still needed by
an open window, so a restart rebuilds identical aggregates (see tests/test_pipeline.py).
"""
from __future__ import annotations

import json
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from .aggregator import Deduper, WindowAggregator, WindowMetrics
from .alerts import AlertManager
from .broker import Broker
from .contracts import Event, validate_event
from .detectors import Detection
from .store import Store
from .telemetry import EventBus, Metrics


class Backpressure(Exception):
    def __init__(self, lag: int) -> None:
        super().__init__(f"processing lag {lag} exceeds limit")
        self.lag = lag


class RateLimited(Exception):
    def __init__(self, retry_after: float) -> None:
        super().__init__("rate limit exceeded")
        self.retry_after = retry_after


@dataclass
class PipelineConfig:
    window_s: float = 30.0
    slide_s: float | None = None
    allowed_lateness_s: float = 20.0
    dedupe_entries: int = 200_000
    max_lag: int = 200_000
    batch_size: int = 1000
    persist_events: bool = True
    idle_clock_after_s: float = 2.0       # advance watermark from wall clock if the stream is idle...
    max_clock_gap_s: float = 180.0        # ...but only while the stream is "live" (not historical backfill)
    warm_windows: int = 6000
    min_txn_for_rate: int = 5
    min_req_for_rate: int = 5


@dataclass
class IngestResult:
    accepted: int = 0
    duplicates: int = 0
    heartbeats: int = 0
    rejected: list[dict[str, Any]] = field(default_factory=list)
    durable: bool = False
    first_offset: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {"accepted": self.accepted, "duplicates": self.duplicates, "heartbeats": self.heartbeats,
                "rejected": self.rejected, "rejected_count": len(self.rejected), "durable": self.durable,
                "first_offset": self.first_offset}


class _Bucket:
    def __init__(self, rate: float) -> None:
        self.rate, self.tokens, self.t = rate, rate * 2, time.monotonic()

    def take(self, n: int) -> float:
        now = time.monotonic()
        self.tokens = min(self.rate * 2, self.tokens + (now - self.t) * self.rate)
        self.t = now
        if self.tokens >= n:
            self.tokens -= n
            return 0.0
        return (n - self.tokens) / self.rate


class StreamProcessor(ABC):
    """Processing-engine adapter. ``LocalProcessor`` is implemented; a Spark Structured Streaming
    adapter would implement this interface but is not part of this repository."""
    mode = "abstract"

    @abstractmethod
    def start(self) -> None: ...
    @abstractmethod
    def stop(self) -> None: ...
    @abstractmethod
    def status(self) -> dict[str, Any]: ...


class Pipeline:
    def __init__(self, store: Store, workspace_id: str, broker: Broker | None, cfg: PipelineConfig, detectors: list,
                 alerts: AlertManager | None = None, bus: EventBus | None = None, metrics: Metrics | None = None,
                 scope: str = "live", name: str = "live", dq: Any = None) -> None:
        self.store, self.ws, self.broker, self.cfg = store, workspace_id, broker, cfg
        self.detectors, self.alerts, self.bus = detectors, alerts, bus
        self.metrics = metrics or Metrics()
        self.scope, self.name, self.dq = scope, name, dq
        self.agg = WindowAggregator(cfg.window_s, cfg.slide_s, cfg.allowed_lateness_s, cfg.min_txn_for_rate, cfg.min_req_for_rate)
        self.dedupe = Deduper(cfg.dedupe_entries)
        self._buckets: dict[str, _Bucket] = {}
        self._lock = threading.Lock()
        self._sources: dict[str, dict[str, Any]] = {}
        self.next_offset = 0          # where reading starts (after recovery: oldest offset an open window still needs)
        self.processed_upto = 0       # high-water mark: every offset below this has been processed at least once
        self.checkpoint_offset = 0
        self._recover_until = 0
        self.last_processed: float | None = None
        self.last_error: str | None = None
        if scope == "live" and broker is not None:
            self._recover()

    # ---- recovery -------------------------------------------------------------------------
    def _recover(self) -> None:
        cp = self.store.q1("SELECT * FROM checkpoints WHERE name=?", (f"{self.ws}:{self.name}",))
        if cp:
            self.next_offset = self.checkpoint_offset = cp["next_offset"]
            self.processed_upto = self._recover_until = cp["processed_upto"]
            self.agg.restore_watermark(cp["watermark"] if cp["watermark"] is not None else float("-inf"), cp["max_event_ts"])
        rows = self.store.q("SELECT window_start, entity, n_events, metrics_json FROM windows WHERE workspace_id=? AND scope='live' AND window_s=? "
                            "ORDER BY window_start DESC LIMIT ?", (self.ws, self.cfg.window_s, self.cfg.warm_windows))
        if rows:
            self.agg.seed_scopes({r["entity"] for r in rows})
            for r in sorted(rows, key=lambda r: r["window_start"]):          # rebuild detector baselines from history
                wm = WindowMetrics(r["window_start"], r["window_start"] + self.cfg.window_s, r["entity"], json.loads(r["metrics_json"]), r["n_events"])
                for d in self.detectors:
                    d.observe_window(wm)
        for r in self.store.q("SELECT event_id FROM events WHERE workspace_id=? AND scope='live' ORDER BY ts DESC LIMIT ?", (self.ws, self.cfg.dedupe_entries)):
            self.dedupe.seen(r["event_id"])
        for _, rec in self.broker.read(self.next_offset, 200_000):          # type: ignore[union-attr]
            self.dedupe.seen(rec["event_id"])
        self.dedupe.duplicates = 0

    def lag(self) -> int:
        return (self.broker.end_offset() - max(self.next_offset, self.processed_upto)) if self.broker else 0

    # ---- ingestion --------------------------------------------------------------------------
    def ingest(self, source: dict[str, Any], raws: list[Any], now: float | None = None) -> IngestResult:
        if self.broker is None:
            raise RuntimeError("this pipeline has no broker")
        sid = source["id"]
        lag = self.lag()
        if lag > self.cfg.max_lag:
            self.metrics.inc("backpressure_rejections")
            raise Backpressure(lag)
        b = self._buckets.setdefault(sid, _Bucket(float(source.get("rate_limit_per_s") or 2000)))
        wait = b.take(len(raws))
        if wait > 0:
            self.metrics.inc("rate_limited_batches")
            raise RateLimited(wait)
        res = IngestResult(durable=self.broker.durable)
        recs: list[dict[str, Any]] = []
        dead: list[tuple] = []
        now = now or time.time()
        last_ts = None
        for i, raw in enumerate(raws):
            v = validate_event(raw, source_id=sid, source_type=source.get("source_type", "application"),
                               simulated=bool(source.get("simulated")), now=now)
            if not v.ok:
                p = v.problems[0]
                res.rejected.append({"index": i, "code": p.code, "field": p.field, "message": p.message,
                                     "problems": [x.to_dict() for x in v.problems][:5]})
                dead.append((self.ws, "live", now, sid, p.code, json.dumps([x.to_dict() for x in v.problems][:5]), json.dumps(raw, default=str)[:2000]))
                continue
            ev = v.event
            assert ev is not None
            if ev.event_type == "heartbeat":
                res.heartbeats += 1
                last_ts = max(last_ts or 0, ev.ts)
                continue
            if self.dedupe.seen(ev.event_id):
                res.duplicates += 1
                continue
            recs.append(ev.to_dict())
            last_ts = max(last_ts or 0, ev.ts)
        if dead:
            self.store.xmany("INSERT INTO dead_letters(workspace_id,scope,ts,source_id,reason_code,detail,raw) VALUES(?,?,?,?,?,?,?)", dead)
        if recs:
            res.first_offset = self.broker.append(recs)         # returns after the broker's durability level is met
        res.accepted = len(recs)
        if res.accepted or res.heartbeats:
            self.store.x("UPDATE sources SET event_count=event_count+?, last_event_ts=COALESCE(?,last_event_ts), last_seen_wall=? WHERE id=?",
                         (res.accepted, last_ts, now, sid))
        self.metrics.inc("accepted", res.accepted)
        self.metrics.inc("rejected", len(res.rejected))
        self.metrics.inc("duplicates", res.duplicates)
        self.metrics.inc("heartbeats", res.heartbeats)
        self.metrics.tick(1, res.accepted)
        self.metrics.tick(2, len(res.rejected))
        return res

    # ---- processing ---------------------------------------------------------------------------
    def process_records(self, recs: list[tuple[int, dict[str, Any]]], now_wall: float | None = None) -> int:
        """Process (offset, event-dict) records synchronously. Returns number processed."""
        if not recs:
            return 0
        now_wall = now_wall or time.time()
        closed: list[WindowMetrics] = []
        rows: list[tuple] = []
        dead: list[tuple] = []
        for off, d in recs:
            ev = Event.from_dict(d)
            if self.scope != "live":
                ev.replay_id = self.scope
            self.metrics.observe_delay(max(0.0, (now_wall - ev.ingest_ts) * 1000.0)) if self.scope == "live" else None
            silent = off < self._recover_until
            ok, c = self.agg.add(ev, off, silent_late=silent)
            if not ok and not silent:
                self.metrics.inc("late_dropped")
                dead.append((self.ws, self.scope, now_wall, ev.source_id, "late_event",
                             f"event_time {ev.ts:.0f} older than watermark {self.agg.watermark:.0f}", json.dumps(d)[:2000]))
            elif ok and self.cfg.persist_events:
                rows.append((self.ws, self.scope, ev.event_id, ev.event_type, ev.ts, ev.ingest_ts, ev.source_id, ev.service, ev.region,
                             ev.entity_id, ev.status, ev.latency_ms, ev.correlation_id, ev.trace_id, int(ev.simulated), json.dumps(d)))
            closed.extend(c)
        last_off = recs[-1][0]
        if rows:
            self.store.xmany("INSERT OR IGNORE INTO events(workspace_id,scope,event_id,event_type,ts,ingest_ts,source_id,service,region,entity_id,status,latency_ms,"
                             "correlation_id,trace_id,simulated,payload_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
        if dead:
            self.store.xmany("INSERT INTO dead_letters(workspace_id,scope,ts,source_id,reason_code,detail,raw) VALUES(?,?,?,?,?,?,?)", dead)
        self._emit(closed, now_wall)
        self._checkpoint(last_off)
        self.metrics.inc("processed", len(recs))
        self.metrics.tick(3, len(recs))
        self.last_processed = now_wall
        return len(recs)

    def idle_tick(self, now: float | None = None) -> None:
        """Wall-clock watermark advance while the live stream is idle (lets a silent producer show as zero-volume windows)."""
        now = now or time.time()
        mx = self.agg.max_event_ts
        if mx is None or self.scope != "live":
            return
        if now - mx > self.cfg.max_clock_gap_s or (self.last_processed and now - self.last_processed < self.cfg.idle_clock_after_s):
            return
        closed = self.agg.advance_clock(now)
        if closed:
            self._emit(closed, now)
            self._checkpoint(None)

    def finish(self) -> None:
        """Flush all open windows (end of replay / experiment / shutdown of a bounded run)."""
        self._emit(self.agg.flush(), time.time())

    def _checkpoint(self, last_off: int | None) -> None:
        if self.broker is None or self.scope != "live":
            return
        if last_off is not None:
            self.processed_upto = max(self.processed_upto, last_off + 1)
        mo = self.agg.min_open_offset()
        keep = min(mo, self.processed_upto) if mo is not None else self.processed_upto
        self.checkpoint_offset = keep
        self.store.x("INSERT INTO checkpoints(name,next_offset,processed_upto,watermark,max_event_ts,updated_at) VALUES(?,?,?,?,?,?) "
                     "ON CONFLICT(name) DO UPDATE SET next_offset=excluded.next_offset, processed_upto=excluded.processed_upto, watermark=excluded.watermark, "
                     "max_event_ts=excluded.max_event_ts, updated_at=excluded.updated_at",
                     (f"{self.ws}:{self.name}", keep, self.processed_upto, None if self.agg.watermark == float("-inf") else self.agg.watermark,
                      self.agg.max_event_ts, time.time()))

    def _emit(self, closed: list[WindowMetrics], now_wall: float) -> None:
        if not closed:
            return
        self.store.xmany("INSERT OR REPLACE INTO windows(workspace_id,scope,window_start,window_s,entity,n_events,metrics_json) VALUES(?,?,?,?,?,?,?)",
                         [(self.ws, self.scope, w.window_start, self.cfg.window_s, w.entity, w.n_events, json.dumps(w.metrics)) for w in closed])
        live = self.scope == "live"
        for w in closed:
            if live and self.bus and (w.entity == "global" or w.n_events):
                self.bus.publish(self.ws, "window", w.to_dict())
        dets: list[Detection] = []
        t0 = time.perf_counter()
        for w in closed:
            for d in self.detectors:
                dets.extend(d.observe_window(w))
        if dets:
            self._persist_detections(dets, now_wall)
        self.metrics.inc("windows_closed", len(closed))
        self.metrics.inc("detection_seconds", time.perf_counter() - t0)

    def _persist_detections(self, dets: list[Detection], now_wall: float) -> None:
        pairs: list[tuple[Detection, int]] = []
        for d in dets:
            detail = {"support": d.support, "config": d.config, "evidence": d.evidence, "steps": d.steps}
            self.store.x("INSERT OR IGNORE INTO anomalies(workspace_id,scope,dedup_key,group_key,detector,version,entity,metric,window_start,window_end,observed,expected,lower,upper,"
                         "score,severity,direction,explanation,detail_json,detected_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                         (self.ws, self.scope, d.dedup_key, d.group_key, d.detector, d.version, d.entity, d.metric, d.window_start, d.window_end, d.observed,
                          d.expected, d.lower, d.upper, d.score, d.severity, d.direction, d.explanation, json.dumps(detail, default=str), now_wall))
            row = self.store.q1("SELECT id FROM anomalies WHERE workspace_id=? AND scope=? AND dedup_key=?", (self.ws, self.scope, d.dedup_key))
            pairs.append((d, row["id"]))  # type: ignore[index]
            if self.scope == "live":
                self.metrics.observe_detect(max(0.0, (now_wall - d.window_end) * 1000.0))
            self.metrics.inc("anomalies")
        if self.scope == "live":
            if self.bus:
                for d, i in pairs:
                    self.bus.publish(self.ws, "anomaly", {"id": i, **d.to_dict()})
            if self.alerts:
                src_sim = bool(self.store.q1("SELECT 1 FROM sources WHERE workspace_id=? AND simulated=1 LIMIT 1", (self.ws,)))
                self.alerts.handle(self.ws, pairs, simulated=src_sim)

    def status(self) -> dict[str, Any]:
        return {"scope": self.scope, "lag": self.lag(), "checkpoint_offset": self.checkpoint_offset, "processed_upto": self.processed_upto,
                "watermark": None if self.agg.watermark == float("-inf") else self.agg.watermark,
                "max_event_ts": self.agg.max_event_ts, "open_windows": self.agg.open_windows(), "late_dropped": self.agg.late_dropped,
                "dedupe_entries": len(self.dedupe), "window_s": self.cfg.window_s, "allowed_lateness_s": self.cfg.allowed_lateness_s,
                "last_processed": self.last_processed, "last_error": self.last_error, "durable_broker": bool(self.broker and self.broker.durable)}


class LocalProcessor(StreamProcessor):
    """Single-threaded consumer: broker -> Pipeline.process_records. Not distributed; no partitioning."""
    mode = "local"

    def __init__(self, pipeline: Pipeline, dq_interval_s: float = 10.0) -> None:
        self.p = pipeline
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._t: threading.Thread | None = None
        self._read = pipeline.next_offset
        self.dq_interval_s = dq_interval_s
        self._last_dq = 0.0
        self.paused = False
        self.restarts = 0

    def notify(self) -> None:
        self._wake.set()

    def start(self) -> None:
        if self._t and self._t.is_alive():
            return
        self._stop.clear()
        self._t = threading.Thread(target=self._run, name=f"processor-{self.p.ws}", daemon=True)
        self._t.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        self._wake.set()
        if self._t:
            self._t.join(timeout)

    def _run(self) -> None:
        p = self.p
        while not self._stop.is_set():
            try:
                if self.paused:
                    self._wake.wait(0.2)
                    continue
                recs = p.broker.read(self._read, p.cfg.batch_size)      # type: ignore[union-attr]
                if recs:
                    p.process_records(recs)
                    self._read = recs[-1][0] + 1
                    p.broker.truncate_before(p.checkpoint_offset - 1_000)    # type: ignore[union-attr]
                else:
                    self._wake.wait(0.25)
                    self._wake.clear()
                    p.idle_tick()
                if self.dq_interval_s and p.dq and time.time() - self._last_dq >= self.dq_interval_s:
                    self._last_dq = time.time()
                    p.dq.evaluate()
                p.last_error = None
            except Exception as e:   # keep the processor alive; surface the failure in status
                p.last_error = f"{type(e).__name__}: {e}"
                p.metrics.inc("processing_errors")
                time.sleep(0.5)

    def drain(self, timeout: float = 10.0) -> bool:
        """Block until the processor has consumed everything currently in the broker (used by tests/benchmarks)."""
        end = time.time() + timeout
        while time.time() < end:
            if self._read >= self.p.broker.end_offset() and self.p.last_error is None:      # type: ignore[union-attr]
                return True
            self._wake.set()
            time.sleep(0.01)
        return False

    def status(self) -> dict[str, Any]:
        return {"mode": self.mode, "running": bool(self._t and self._t.is_alive()), "paused": self.paused,
                "distributed": False, "note": "single-process local processor; no partitioning or horizontal scale", **self.p.status()}
