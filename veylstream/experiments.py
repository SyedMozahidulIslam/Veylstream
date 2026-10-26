"""Reproducible detector experiments.

Harness: dataset -> validation -> WindowAggregator (same class as live) -> every detector sees the SAME closed
windows (detectors are independent, stateful instances) -> window-level evaluation.

Evaluation unit = (entity, metric, window). A unit is *positive* when a ground-truth interval that targets that
entity and metric overlaps the window. ``global`` is excluded from scoring unless a scenario targets it
(a scoped incident is diluted at global level, so scoring it would be misleading). Only windows after the first
``eval_start_frac`` of the stream are scored, identically for all detectors: this removes warm-up and keeps the
ML detector's training prefix out of the test period (no temporal leakage). Multivariate detections
(metric='multivariate') count for any metric of the same entity.

When no ground truth exists, precision/recall/F1 are reported as unavailable (never fabricated) and
unsupervised diagnostics are returned instead.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import platform
import threading
import time
from typing import Any

import numpy as np

from . import __version__
from .aggregator import WindowAggregator, WindowMetrics
from .contracts import validate_event
from .datasets import iter_dataset
from .detectors import DEFAULT_METRICS
from .replay import build_detectors
from .store import Store, new_id

DEFAULT_SPECS = [
    {"name": "rule"}, {"name": "zscore"}, {"name": "mad"}, {"name": "iqr"},
    {"name": "baseline_deviation"}, {"name": "rate_of_change"},
    {"name": "isolation_forest", "config": {"min_train": 40}},
]


def confusion(pred: list[bool], truth: list[bool]) -> dict[str, Any]:
    tp = sum(p and t for p, t in zip(pred, truth))
    fp = sum(p and not t for p, t in zip(pred, truth))
    fn = sum((not p) and t for p, t in zip(pred, truth))
    tn = len(truth) - tp - fp - fn
    prec = tp / (tp + fp) if tp + fp else None
    rec = tp / (tp + fn) if tp + fn else None
    f1 = 2 * prec * rec / (prec + rec) if prec is not None and rec is not None and prec + rec > 0 else (0.0 if prec is not None and rec is not None else None)
    fpr = fp / (fp + tn) if fp + tn else None
    return {"tp": tp, "fp": fp, "fn": fn, "tn": tn, "precision": prec, "recall": rec, "f1": f1, "tpr": rec, "fpr": fpr,
            "confusion_matrix": [[tn, fp], [fn, tp]]}


def run_harness(events: list[dict[str, Any]], window_s: float, lateness_s: float, detectors: list) -> dict[str, Any]:
    """Aggregate once, run each detector over identical windows. Returns windows, detections, timings."""
    agg = WindowAggregator(window_s, None, lateness_s)
    windows: list[WindowMetrics] = []
    t0 = time.perf_counter()
    n = 0
    for e in events:
        v = validate_event(e, source_id="exp", allow_future=True)
        if not v.ok:
            continue
        n += 1
        windows.extend(agg.add(v.event)[1])  # type: ignore[arg-type]
    windows.extend(agg.flush())
    agg_s = time.perf_counter() - t0
    dets: dict[str, list] = {d.name: [] for d in detectors}
    det_s: dict[str, float] = {d.name: 0.0 for d in detectors}
    for w in windows:
        for d in detectors:
            t1 = time.perf_counter()
            dets[d.name].extend(d.observe_window(w))
            det_s[d.name] += time.perf_counter() - t1
    return {"windows": windows, "detections": dets, "events": n, "late_dropped": agg.late_dropped, "agg_seconds": agg_s, "detector_seconds": det_s}


def _positive(ws: float, we: float, entity: str, metric: str, gt: list[dict[str, Any]]) -> bool:
    return any(g["start"] < we and g["end"] > ws and entity in g["entities"] and metric in g["metrics"] for g in gt)


def evaluate(windows: list[WindowMetrics], dets: dict[str, list], gt: list[dict[str, Any]], metrics: list[str], eval_start_frac: float,
             window_s: float) -> dict[str, Any]:
    starts = sorted({w.window_start for w in windows})
    if not starts:
        return {"error": "no windows produced"}
    cut = starts[int(len(starts) * eval_start_frac)] if starts else 0
    gt_entities = {e for g in gt for e in g["entities"]}
    scored_entities = {w.entity for w in windows if w.entity != "global" or "global" in gt_entities}
    def ignored(e: str, m: str, ws_: float, we_: float) -> bool:   # diluted/propagated impact: neither positive nor negative
        return any(g["start"] < we_ and g["end"] > ws_ and m in g["metrics"] and e not in g["entities"]
                   and any(e.startswith(px) for px in g.get("ignore_prefixes", [])) for g in gt)

    units = [(w.entity, m, w.window_start, w.window_end) for w in windows
             if w.window_start >= cut and w.entity in scored_entities and w.n_events > 0 for m in metrics
             if w.metrics.get(m) is not None and not ignored(w.entity, m, w.window_start, w.window_end)]
    out: dict[str, Any] = {"scored_units": len(units), "eval_start": cut, "windows_total": len(starts), "windows_scored": len([s for s in starts if s >= cut])}
    labeled = bool(gt)
    truth = [_positive(u[2], u[3], u[0], u[1], gt) for u in units] if labeled else []
    per: dict[str, Any] = {}
    flagged_all: dict[str, set] = {}
    for name, ds in dets.items():
        keys = {(d.entity, d.metric if d.metric != "multivariate" else None, d.window_start) for d in ds}
        flagged = {(e, m, ws_) for (e, m, ws_) in ((u[0], u[1], u[2]) for u in units)
                   if (e, m, ws_) in keys or (e, None, ws_) in keys}
        flagged_all[name] = flagged
        pred = [(u[0], u[1], u[2]) in flagged for u in units]
        r: dict[str, Any] = {"detections_total": len(ds), "flagged_units": sum(pred), "flag_rate": (sum(pred) / len(units)) if units else None,
                             "mean_score": float(np.mean([d.score for d in ds])) if ds else None,
                             "by_severity": {s: sum(1 for d in ds if d.severity == s) for s in ("low", "medium", "high", "critical")}}
        if labeled:
            r.update(confusion(pred, truth))
            delays, missed = [], []
            for g in gt:
                if g["end"] <= cut:
                    continue
                hits = [u[2] for u, p in zip(units, pred) if p and u[0] in g["entities"] and u[1] in g["metrics"] and g["start"] < u[3] and g["end"] > u[2]]
                if hits:
                    delays.append(max(0.0, min(hits) + window_s - g["start"]))   # time from onset until the end of the first flagged window
                else:
                    missed.append({"kind": g["kind"], "start": g["start"], "entities": g["entities"]})
            r["incidents_total"] = len([g for g in gt if g["end"] > cut])
            r["incidents_detected"] = len(delays)
            r["detection_delay_s"] = {"mean": float(np.mean(delays)) if delays else None, "median": float(np.median(delays)) if delays else None, "per_incident": delays}
            r["missed_incidents"] = missed
        else:
            for k in ("precision", "recall", "f1", "tpr", "fpr", "tp", "fp", "fn", "tn"):
                r[k] = None
            r["note"] = "unavailable: dataset has no ground-truth labels; see unsupervised diagnostics"
        per[name] = r
    out["detectors"] = per
    names = list(flagged_all)
    out["agreement_jaccard"] = {a: {b: (len(flagged_all[a] & flagged_all[b]) / len(flagged_all[a] | flagged_all[b]) if flagged_all[a] | flagged_all[b] else None)
                                    for b in names} for a in names}
    out["labels_available"] = labeled
    return out


def to_csv(results: dict[str, Any]) -> str:
    cols = ["detector", "detections_total", "flagged_units", "precision", "recall", "f1", "fpr", "tp", "fp", "fn", "tn", "incidents_detected", "incidents_total", "median_delay_s"]
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(cols)
    for name, r in results["evaluation"]["detectors"].items():
        w.writerow([name, r["detections_total"], r["flagged_units"], r.get("precision"), r.get("recall"), r.get("f1"), r.get("fpr"), r.get("tp"), r.get("fp"),
                    r.get("fn"), r.get("tn"), r.get("incidents_detected"), r.get("incidents_total"), (r.get("detection_delay_s") or {}).get("median")])
    return buf.getvalue()


def research_summary(exp: dict[str, Any]) -> str:
    cfg, res = exp["config"], exp["results"]
    lines = [f"# Experiment {exp['id']}: {exp['name']}", "", f"- Status: {exp['status']}", f"- Software: veylstream {exp['software_version']}",
             f"- Dataset: {res['dataset']['name']} (sha256 `{res['dataset']['sha256'][:16]}…`, {res['dataset']['events']} events, simulated={res['dataset'].get('simulated')})",
             f"- Window: {cfg['window_s']}s, lateness {cfg['allowed_lateness_s']}s, eval starts at {cfg['eval_start_frac']:.0%} of the stream, seed {cfg.get('seed')}",
             f"- Ground truth available: {res['evaluation']['labels_available']}",
             "- Scoring: window-level per (entity, metric); scopes that only see a diluted version of a scoped incident are excluded, not counted negative", "", "| detector | flagged | precision | recall | F1 | FPR | median delay (s) |", "|---|---|---|---|---|---|---|"]
    f = lambda v: "n/a" if v is None else f"{v:.3f}"  # noqa: E731
    for n, r in res["evaluation"]["detectors"].items():
        lines.append(f"| {n} | {r['flagged_units']} | {f(r.get('precision'))} | {f(r.get('recall'))} | {f(r.get('f1'))} | {f(r.get('fpr'))} | {f((r.get('detection_delay_s') or {}).get('median'))} |")
    lines += ["", "## Known limitations", *[f"- {x}" for x in res["limitations"]]]
    return "\n".join(lines)


def run_experiment(store: Store, ws: str, config: dict[str, Any], exp_id: str | None = None) -> str:
    eid = exp_id or new_id("ex")
    ds = store.q1("SELECT * FROM datasets WHERE id=? AND workspace_id=?", (config["dataset_id"], ws))
    if ds is None:
        raise KeyError("dataset not found")
    cfg = {"window_s": 30.0, "allowed_lateness_s": 20.0, "eval_start_frac": 0.3, "metrics": DEFAULT_METRICS, "detectors": DEFAULT_SPECS, "seed": 7, **config}
    if not store.q1("SELECT 1 FROM experiments WHERE id=?", (eid,)):
        store.x("INSERT INTO experiments(id,workspace_id,name,config_json,dataset_hash,status,software_version,seed) VALUES(?,?,?,?,?,?,?,?)",
                (eid, ws, cfg.get("name", f"experiment {eid}"), json.dumps(cfg), ds["sha256"], "running", __version__, cfg["seed"]))
    store.x("UPDATE experiments SET status='running', started_at=?, config_json=? WHERE id=?", (time.time(), json.dumps(cfg), eid))
    try:
        specs = [{**s, "config": {**(s.get("config") or {}), **({"random_state": cfg["seed"]} if s["name"] == "isolation_forest" else {})}} for s in cfg["detectors"]]
        detectors = build_detectors(specs)
        meta = json.loads(ds["meta_json"])
        events, labels = [], []
        for row in iter_dataset(ds["path"]):
            events.append(row["e"])
            labels.append(row.get("label"))
        t_lo, t_hi = cfg.get("start_ts"), cfg.get("end_ts")
        if t_lo or t_hi:
            events = [e for e in events if (not t_lo or (validate_event(e, source_id="x", allow_future=True).event or _Z).ts >= t_lo) and (not t_hi or (validate_event(e, source_id="x", allow_future=True).event or _Z).ts < t_hi)]
        h = run_harness(events, cfg["window_s"], cfg["allowed_lateness_s"], detectors)
        gt = meta.get("ground_truth") or []
        if not gt and any(l is not None for l in labels):
            gt = _labels_to_intervals(events, labels, cfg["window_s"])
        ev = evaluate(h["windows"], h["detections"], gt, cfg["metrics"], cfg["eval_start_frac"], cfg["window_s"])
        n = max(h["events"], 1)
        results = {
            "dataset": {"id": ds["id"], "name": ds["name"], "sha256": ds["sha256"], "events": h["events"], "ts_min": ds["ts_min"], "ts_max": ds["ts_max"], "simulated": bool(meta.get("simulated"))},
            "detectors": [{"name": d.name, "version": d.version, "config": {k: v for k, v in d.config.items() if k != "rules"}} for d in detectors],
            "feature_definitions": {"isolation_forest": getattr(next((d for d in detectors if d.name == "isolation_forest"), None), "features", None),
                                    "univariate_metrics": cfg["metrics"]},
            "evaluation": ev,
            "performance": {"aggregation_events_per_s": n / h["agg_seconds"] if h["agg_seconds"] else None, "detector_ms_per_window": {k: 1000 * v / max(len(h["windows"]), 1) for k, v in h["detector_seconds"].items()},
                            "windows": len(h["windows"]), "late_dropped": h["late_dropped"], "environment": {"python": platform.python_version(), "numpy": np.__version__, "machine": platform.machine(), "cpus": __import__("os").cpu_count()}},
            "limitations": ["Window-level scoring on synthetic data: results do not transfer automatically to real traffic.",
                            "Detector thresholds are defaults, not tuned per dataset; no hyper-parameter search was performed.",
                            "Performance numbers come from a single run on the stated machine and include Python overhead.",
                            "Ground truth comes from injected scenarios; scenario boundaries are idealised." if gt else "No ground truth: supervised metrics are unavailable."],
            "reproducibility": {"experiment_id": eid, "config_sha256": hashlib.sha256(json.dumps(cfg, sort_keys=True, default=str).encode()).hexdigest(), "seed": cfg["seed"],
                                "software_version": __version__, "library_versions": {"numpy": np.__version__, "sklearn": __import__("sklearn").__version__}},
        }
        store.x("UPDATE experiments SET status='completed', finished_at=?, results_json=?, dataset_hash=? WHERE id=?", (time.time(), json.dumps(results, default=str), ds["sha256"], eid))
    except Exception as e:
        store.x("UPDATE experiments SET status='failed', finished_at=?, error=? WHERE id=?", (time.time(), f"{type(e).__name__}: {e}", eid))
    return eid


class _Z:
    ts = 0.0


def _labels_to_intervals(events: list[dict], labels: list, window_s: float) -> list[dict[str, Any]]:
    """Derive global-scope intervals from event-level labels: maximal runs of windows where >=20% of events are labeled anomalous."""
    from .contracts import parse_ts
    buckets: dict[int, list[int]] = {}
    for e, l in zip(events, labels):
        ts = parse_ts(e.get("event_timestamp"))
        if ts is not None and l is not None:
            b = buckets.setdefault(int(ts // window_s), [0, 0])
            b[0] += 1
            b[1] += l
    runs, cur = [], None
    for k in sorted(buckets):
        bad = buckets[k][1] / buckets[k][0] >= 0.2
        if bad and cur and k == cur[1] + 1:
            cur[1] = k
        elif bad:
            cur = [k, k]
            runs.append(cur)
        else:
            cur = None
    return [{"kind": "labeled", "start": a * window_s, "end": (b + 1) * window_s, "entities": ["global"], "metrics": DEFAULT_METRICS} for a, b in runs]


class ExperimentService:
    def __init__(self, store: Store) -> None:
        self.store = store
        self._threads: dict[str, threading.Thread] = {}

    def start(self, ws: str, config: dict[str, Any]) -> str:
        if not self.store.q1("SELECT 1 FROM datasets WHERE id=? AND workspace_id=?", (config.get("dataset_id"), ws)):
            raise KeyError("dataset not found")
        build_detectors(config.get("detectors") or DEFAULT_SPECS)       # validate before queueing
        eid = new_id("ex")
        self.store.x("INSERT INTO experiments(id,workspace_id,name,config_json,status,software_version,seed) VALUES(?,?,?,?,?,?,?)",
                     (eid, ws, config.get("name", f"experiment {eid}"), json.dumps(config), "queued", __version__, config.get("seed", 7)))
        t = threading.Thread(target=run_experiment, args=(self.store, ws, config, eid), daemon=True, name=f"exp-{eid}")
        self._threads[eid] = t
        t.start()
        return eid

    def wait(self, eid: str, timeout: float = 120) -> None:
        t = self._threads.get(eid)
        if t:
            t.join(timeout)
