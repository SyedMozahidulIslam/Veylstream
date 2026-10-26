#!/usr/bin/env python3
"""Reproducible benchmarks. Writes machine-readable JSON to experiments/results/.

  python scripts/benchmark.py detectors   # labeled detector comparison (seeded)
  python scripts/benchmark.py pipeline    # ingest ack latency + processing throughput + detection latency
  python scripts/benchmark.py all

Numbers describe THIS machine and THIS single-process local profile only; the output records the environment.
"""
import json, os, platform, sys, tempfile, time
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import numpy as np
from veylstream import __version__
from veylstream.broker import FileLogBroker, MemoryBroker
from veylstream.datasets import create_synthetic_dataset
from veylstream.detectors import default_detector_set
from veylstream.experiments import run_experiment
from veylstream.generator import GeneratorConfig, Scenario, SyntheticGenerator
from veylstream.pipeline import LocalProcessor, Pipeline, PipelineConfig
from veylstream.store import Store

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "experiments", "results")
T0 = 1_790_000_000.0
ENV = {"python": platform.python_version(), "numpy": np.__version__, "cpus": os.cpu_count(), "machine": platform.machine(), "system": platform.platform(), "veylstream": __version__}


def detectors(seed=11, repeats=3):
    runs = []
    for k in range(repeats):
        d = tempfile.mkdtemp(); st = Store(d + "/db")
        sc = [Scenario("failure_spike", 2000, 2300, "payments", "eu-west", 0.4), Scenario("latency_degradation", 2800, 3100, "checkout", None, 4.0), Scenario("traffic_drop", 3300, 3500, None, None, 0.2)]
        ds = create_synthetic_dataset(st, "ws", "bench", d, seed=seed + k, duration_s=3900, base_rate=25, scenarios=sc)
        eid = run_experiment(st, "ws", {"dataset_id": ds["id"], "name": f"benchmark seed {seed + k}", "seed": seed + k})
        runs.append({"seed": seed + k, "dataset_sha256": ds["sha256"], "results": json.loads(st.q1("select results_json r from experiments where id=?", (eid,))["r"])})
    names = list(runs[0]["results"]["evaluation"]["detectors"])
    summary = {}
    for n in names:
        f = lambda key: [r["results"]["evaluation"]["detectors"][n].get(key) for r in runs]
        vals = {m: [x for x in f(m) if x is not None] for m in ("precision", "recall", "f1", "fpr")}
        summary[n] = {m: {"mean": float(np.mean(v)) if v else None, "min": float(np.min(v)) if v else None, "max": float(np.max(v)) if v else None} for m, v in vals.items()}
        summary[n]["incidents_detected"] = [r["results"]["evaluation"]["detectors"][n].get("incidents_detected") for r in runs]
    out = {"benchmark": "detectors", "repeats": repeats, "scenarios": "failure_spike(payments,eu-west) + latency_degradation(checkout) + traffic_drop(global)", "environment": ENV, "summary": summary, "runs": runs}
    path = os.path.join(OUT, "detectors.json"); json.dump(out, open(path, "w"), indent=1, default=str)
    print(f"{'detector':20}{'precision':>12}{'recall':>10}{'F1':>8}{'FPR':>9}  incidents")
    for n, s in summary.items():
        print(f"{n:20}{s['precision']['mean'] or 0:12.3f}{s['recall']['mean'] or 0:10.3f}{s['f1']['mean'] or 0:8.3f}{s['fpr']['mean'] or 0:9.4f}  {s['incidents_detected']}")
    print("wrote", path)


def pipeline(n_events=60_000):
    g = SyntheticGenerator(GeneratorConfig(seed=1, base_rate=60)); raw = g.generate(T0, T0 + n_events / 60 + 5)
    payloads = [p for _, _, p in raw if p["event_type"] != "heartbeat"][:n_events]
    src = {"id": "bench", "rate_limit_per_s": 1e12}
    res = {"benchmark": "pipeline", "events": len(payloads), "environment": ENV, "notes": "single-process Python, SQLite, 1 broker, 1 processor thread; no network hop"}
    for label, fsync in (("fsync_per_batch_500", True), ("no_fsync_per_batch_500", False)):
        d = tempfile.mkdtemp(); st = Store(d + "/db"); b = FileLogBroker(d + "/log", fsync=fsync)
        p = Pipeline(st, "ws", b, PipelineConfig(window_s=30, allowed_lateness_s=20), default_detector_set())
        acks = []; t = time.perf_counter()
        for i in range(0, len(payloads), 500):
            t1 = time.perf_counter(); p.ingest(src, payloads[i:i + 500], now=T0 + 99999); acks.append((time.perf_counter() - t1) * 1000)
        ingest_s = time.perf_counter() - t
        proc = LocalProcessor(p, dq_interval_s=0); t = time.perf_counter(); proc.start(); drained = proc.drain(600); proc_s = time.perf_counter() - t; proc.stop(); p.finish()
        res[label] = {"ingest_events_per_s": len(payloads) / ingest_s, "batch_ack_ms_p50": float(np.percentile(acks, 50)), "batch_ack_ms_p95": float(np.percentile(acks, 95)),
                      "processing_events_per_s": len(payloads) / proc_s, "drained": drained, "late_dropped": p.agg.late_dropped,
                      "detection_ms_per_window_total": 1000 * p.metrics.counters["detection_seconds"] / max(1, p.metrics.counters["windows_closed"])}
        b.close()
    path = os.path.join(OUT, "pipeline.json"); json.dump(res, open(path, "w"), indent=1)
    print(json.dumps(res, indent=1)); print("wrote", path)


if __name__ == "__main__":
    os.makedirs(OUT, exist_ok=True)
    w = sys.argv[1] if len(sys.argv) > 1 else "all"
    if w in ("detectors", "all"): detectors()
    if w in ("pipeline", "all"): pipeline()
