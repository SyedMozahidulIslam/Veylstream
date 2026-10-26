"""Historical replay through the real processing + detection pipeline.

A replay runs a dedicated ``Pipeline`` whose scope is the replay id: its windows and anomalies are stored
with ``scope=<replay_id>`` so they can never mix with live results, and it never opens live alerts or sends
notifications. Aggregation and detection code is identical to the live path.
"""
from __future__ import annotations

import json
import threading
import time
from collections import Counter
from typing import Any

from .contracts import validate_event
from .datasets import iter_dataset
from .detectors import build_detector, default_detector_set
from .pipeline import Pipeline, PipelineConfig
from .store import Store, new_id

BATCH = 500


def build_detectors(specs: list[dict[str, Any]] | None) -> list:
    if not specs:
        return default_detector_set()
    return [build_detector(s["name"], **(s.get("config") or {})) for s in specs]


def summarize_scope(store: Store, ws: str, scope: str, t0: float | None = None, t1: float | None = None) -> dict[str, Any]:
    q = "SELECT detector,severity,entity,metric,window_start FROM anomalies WHERE workspace_id=? AND scope=?"
    args: list[Any] = [ws, scope]
    if t0 is not None:
        q += " AND window_start>=?"
        args.append(t0)
    if t1 is not None:
        q += " AND window_start<?"
        args.append(t1)
    rows = store.q(q + " ORDER BY window_start", args)
    first: dict[str, float] = {}
    for r in rows:
        first.setdefault(f"{r['entity']}|{r['metric']}", r["window_start"])
    return {"scope": scope, "anomalies": len(rows), "by_detector": dict(Counter(r["detector"] for r in rows)),
            "by_severity": dict(Counter(r["severity"] for r in rows)), "first_detection": first}


def compare_scopes(store: Store, ws: str, a: str, b: str, a_range: tuple | None = None, b_range: tuple | None = None) -> dict[str, Any]:
    sa = summarize_scope(store, ws, a, *(a_range or (None, None)))
    sb = summarize_scope(store, ws, b, *(b_range or (None, None)))
    keys = sorted(set(sa["first_detection"]) | set(sb["first_detection"]))
    rows = []
    for k in keys:
        ta, tb = sa["first_detection"].get(k), sb["first_detection"].get(k)
        rows.append({"series": k, "first_a": ta, "first_b": tb, "delta_s": (tb - ta) if ta is not None and tb is not None else None,
                     "only_in": "a" if tb is None else "b" if ta is None else None})
    return {"a": sa, "b": sb, "series": rows}


class ReplayJob:
    def __init__(self, store: Store, ws: str, job_id: str, ds: dict[str, Any], cfg: dict[str, Any]) -> None:
        self.store, self.ws, self.id, self.ds, self.cfg = store, ws, job_id, ds, cfg
        self._pause = threading.Event()
        self._pause.set()                      # set == running
        self._stop = threading.Event()
        self.thread = threading.Thread(target=self._run, name=f"replay-{job_id}", daemon=True)

    def control(self, action: str) -> None:
        if action == "pause":
            self._pause.clear()
            self.store.x("UPDATE replay_jobs SET status='paused' WHERE id=? AND status='running'", (self.id,))
        elif action == "resume":
            self._pause.set()
            self.store.x("UPDATE replay_jobs SET status='running' WHERE id=? AND status='paused'", (self.id,))
        elif action == "stop":
            self._stop.set()
            self._pause.set()
        else:
            raise ValueError("action must be pause, resume or stop")

    def _run(self) -> None:
        cfg, ds = self.cfg, self.ds
        meta = json.loads(ds["meta_json"])
        total = ds["rows_valid"]
        self.store.x("UPDATE replay_jobs SET status='running', started_at=? WHERE id=?", (time.time(), self.id))
        try:
            pcfg = PipelineConfig(window_s=cfg.get("window_s", 30.0), allowed_lateness_s=cfg.get("allowed_lateness_s", 20.0), persist_events=False)
            pipe = Pipeline(self.store, self.ws, None, pcfg, build_detectors(cfg.get("detectors")), alerts=None, scope=self.id, name=self.id)
            speed = float(cfg.get("speed", 0))                       # 0 => as fast as possible
            start_ts, end_ts = cfg.get("start_ts"), cfg.get("end_ts")
            skip = int(cfg.get("start_offset", 0))
            rejected: list[dict[str, Any]] = []
            n_rej = n_proc = 0
            batch: list[tuple[int, dict[str, Any]]] = []
            wall0, t_first = time.time(), None
            for idx, row in enumerate(iter_dataset(ds["path"])):
                if self._stop.is_set():
                    break
                if idx < skip:
                    continue
                self._pause.wait()
                v = validate_event(row["e"], source_id=row.get("src", "replay"), simulated=bool(meta.get("simulated")), replay_id=self.id, allow_future=True)
                if not v.ok:
                    n_rej += 1
                    if len(rejected) < 100:
                        rejected.append({"row": idx + 1, "code": v.problems[0].code, "message": v.problems[0].message})
                    continue
                ev = v.event
                assert ev is not None
                if (start_ts and ev.ts < start_ts) or (end_ts and ev.ts >= end_ts):
                    continue
                if speed > 0:
                    t_first = row["t"] if t_first is None else t_first
                    delay = (row["t"] - t_first) / speed - (time.time() - wall0)
                    if delay > 0:
                        time.sleep(min(delay, 1.0))
                batch.append((idx, ev.to_dict()))
                if len(batch) >= BATCH:
                    n_proc += pipe.process_records(batch)
                    batch = []
                    self.store.x("UPDATE replay_jobs SET progress=?, processed=? WHERE id=?", (min(1.0, (idx + 1) / max(total, 1)), n_proc, self.id))
            if batch:
                n_proc += pipe.process_records(batch)
            pipe.finish()
            result = {"events_processed": n_proc, "rows_rejected": n_rej, "rejected_sample": rejected, "late_dropped": pipe.agg.late_dropped,
                      "summary": summarize_scope(self.store, self.ws, self.id), "provenance": {"replay_id": self.id, "dataset_id": ds["id"], "dataset_sha256": ds["sha256"]},
                      "simulated_data": bool(meta.get("simulated")), "wall_seconds": round(time.time() - wall0, 3)}
            if meta.get("ground_truth"):
                result["ground_truth"] = meta["ground_truth"]
            status = "stopped" if self._stop.is_set() else "completed"
            self.store.x("UPDATE replay_jobs SET status=?, progress=?, processed=?, finished_at=?, result_json=? WHERE id=?",
                         (status, 1.0 if status == "completed" else self._prog(), n_proc, time.time(), json.dumps(result, default=str), self.id))
        except Exception as e:
            self.store.x("UPDATE replay_jobs SET status='failed', finished_at=?, error=? WHERE id=?", (time.time(), f"{type(e).__name__}: {e}", self.id))

    def _prog(self) -> float:
        r = self.store.q1("SELECT progress FROM replay_jobs WHERE id=?", (self.id,))
        return r["progress"] if r else 0.0


class ReplayService:
    def __init__(self, store: Store) -> None:
        self.store = store
        self.jobs: dict[str, ReplayJob] = {}
        self._lock = threading.Lock()

    def start(self, ws: str, dataset_id: str, cfg: dict[str, Any]) -> str:
        ds = self.store.q1("SELECT * FROM datasets WHERE id=? AND workspace_id=?", (dataset_id, ws))
        if ds is None:
            raise KeyError("dataset not found")
        with self._lock:
            if sum(1 for j in self.jobs.values() if j.thread.is_alive()) >= 2:
                raise RuntimeError("at most 2 concurrent replays")
            jid = new_id("rp")
            build_detectors(cfg.get("detectors"))                      # validate config before accepting the job
            self.store.x("INSERT INTO replay_jobs(id,workspace_id,dataset_id,config_json,status) VALUES(?,?,?,?,?)", (jid, ws, dataset_id, json.dumps(cfg), "queued"))
            job = ReplayJob(self.store, ws, jid, ds, cfg)
            self.jobs[jid] = job
            job.thread.start()
            return jid

    def control(self, ws: str, jid: str, action: str) -> None:
        if not self.store.q1("SELECT 1 FROM replay_jobs WHERE id=? AND workspace_id=?", (jid, ws)):
            raise KeyError("replay not found")
        job = self.jobs.get(jid)
        if job is None or not job.thread.is_alive():
            raise ValueError("replay is not running")
        job.control(action)

    def wait(self, jid: str, timeout: float = 60.0) -> None:
        job = self.jobs.get(jid)
        if job:
            job.thread.join(timeout)
