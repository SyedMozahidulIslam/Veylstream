"""Unsupervised multivariate detector (Isolation Forest). scikit-learn is imported lazily.

Pipeline per entity: window metrics -> fixed feature vector -> impute missing values with the
training median -> robust scaling (median / MAD from the training set only) -> IsolationForest.

Leakage control: the model is trained only on windows that precede the window being scored. It is
(re)trained at ``min_train`` windows and then every ``retrain_every`` windows using a trailing
history of at most ``max_history`` windows. Windows flagged as anomalous are kept out of later
training sets so incidents do not become the new normal (until ``max_excluded`` consecutive flags).
"""
from __future__ import annotations

import hashlib
import json
import time
import warnings
from collections import deque
from typing import Any

import numpy as np

from .aggregator import WindowMetrics
from .detectors import INVESTIGATION, Detection, Detector

FEATURES = ["event_rate", "txn_failure_rate", "http_error_rate", "latency_p95_ms", "latency_mean_ms", "unique_entities"]


class IsolationForestDetector(Detector):
    name = "isolation_forest"
    version = "1.0.0"
    defaults = {"features": FEATURES, "n_estimators": 150, "contamination": 0.01, "min_train": 120,
                "retrain_every": 240, "max_history": 1500, "random_state": 7, "max_excluded": 30,
                "threshold_margin": 1.05}

    def __init__(self, **config: Any) -> None:
        super().__init__(**config)
        self.features: list[str] = list(self.config["features"])
        self._hist: dict[str, deque[tuple[float, np.ndarray]]] = {}
        self._models: dict[str, dict[str, Any]] = {}
        self._since_train: dict[str, int] = {}
        self._excluded: dict[str, int] = {}

    # -- features ---------------------------------------------------------
    def vectorize(self, metrics: dict[str, float | None]) -> np.ndarray:
        return np.array([np.nan if metrics.get(f) is None else float(metrics[f]) for f in self.features], dtype=float)

    # -- training ---------------------------------------------------------
    def fit(self, X: np.ndarray, t_start: float, t_end: float) -> dict[str, Any]:
        """Fit on chronologically ordered rows (NaN allowed). Returns the model record."""
        from sklearn.ensemble import IsolationForest
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)   # all-NaN columns are imputed with 0 below
            med = np.nanmedian(X, axis=0)
        med = np.where(np.isnan(med), 0.0, med)
        Xi = np.where(np.isnan(X), med, X)
        mad = np.median(np.abs(Xi - med), axis=0) * 1.4826
        scale = np.maximum(mad, np.maximum(np.abs(med) * 0.05, 1e-3))
        Z = (Xi - med) / scale
        model = IsolationForest(n_estimators=int(self.config["n_estimators"]), contamination="auto",
                                random_state=int(self.config["random_state"]), n_jobs=1).fit(Z)
        s = -model.score_samples(Z)
        s_med = float(np.median(s))
        thr = float(np.quantile(s, 1 - float(self.config["contamination"]))) * float(self.config["threshold_margin"])
        spread = max(thr - s_med, 1e-6)
        cfg_hash = hashlib.sha1(json.dumps({"c": {k: self.config[k] for k in ("n_estimators", "contamination", "random_state")},
                                            "f": self.features, "n": len(X), "t0": t_start, "t1": t_end},
                                           sort_keys=True).encode()).hexdigest()[:12]
        return {"model": model, "median": med, "scale": scale, "threshold": thr, "score_median": s_med, "spread": spread,
                "info": {"model_version": cfg_hash, "features": self.features, "n_train": int(len(X)),
                         "train_start": t_start, "train_end": t_end, "trained_at": time.time(),
                         "params": {k: self.config[k] for k in ("n_estimators", "contamination", "random_state", "threshold_margin")},
                         "score_threshold": thr, "library": "scikit-learn IsolationForest"}}

    def score(self, rec: dict[str, Any], x: np.ndarray) -> tuple[float, np.ndarray]:
        xi = np.where(np.isnan(x), rec["median"], x)
        z = (xi - rec["median"]) / rec["scale"]
        return float(-rec["model"].score_samples(z.reshape(1, -1))[0]), z

    # -- streaming --------------------------------------------------------
    def observe_window(self, wm: WindowMetrics) -> list[Detection]:
        if not self.applies(wm.entity) or wm.n_events == 0:
            return []
        e = wm.entity
        x = self.vectorize(wm.metrics)
        h = self._hist.setdefault(e, deque(maxlen=int(self.config["max_history"])))
        det = None
        rec = self._models.get(e)
        if rec is not None:
            s, z = self.score(rec, x)
            if s >= rec["threshold"]:
                ratio = 1.0 + (s - rec["threshold"]) / rec["spread"]
                top = np.argsort(-np.abs(z))[:3]
                contrib = [{"feature": self.features[i], "value": None if np.isnan(x[i]) else float(x[i]),
                            "robust_z": round(float(z[i]), 2), "training_median": float(rec["median"][i])} for i in top]
                lead = contrib[0]
                det = Detection(
                    detector=self.name, version=self.version, entity=e, metric="multivariate", window_start=wm.window_start,
                    window_end=wm.window_end, observed=round(s, 4), expected=round(rec["score_median"], 4), lower=None,
                    upper=round(rec["threshold"], 4), score=round(ratio, 4), severity=_ml_severity(ratio), direction="multi",
                    explanation=f"Isolation Forest flagged {e}: anomaly score {s:.3f} ≥ threshold {rec['threshold']:.3f}. "
                                f"Most unusual feature: {lead['feature']} (robust z={lead['robust_z']:+.1f}).",
                    support={"event_count": wm.metrics.get("event_count"), "txn_count": wm.metrics.get("txn_count"),
                             "req_count": wm.metrics.get("req_count")},
                    config={k: self.config[k] for k in ("n_estimators", "contamination", "random_state", "min_train")},
                    evidence={"model": {k: v for k, v in rec["info"].items()}, "top_features": contrib},
                    steps=INVESTIGATION["multivariate"])
        if det is None:
            h.append((wm.window_start, x))
            self._excluded[e] = 0
        else:
            self._excluded[e] = self._excluded.get(e, 0) + 1
            if self._excluded[e] >= self.config["max_excluded"]:
                h.append((wm.window_start, x))
        self._since_train[e] = self._since_train.get(e, 0) + 1
        due = (rec is None and len(h) >= self.config["min_train"]) or \
              (rec is not None and self._since_train[e] >= self.config["retrain_every"])
        if due:
            X = np.vstack([v for _, v in h])
            self._models[e] = self.fit(X, h[0][0], h[-1][0])
            self._since_train[e] = 0
        return [det] if det else []

    def evaluate(self, wm, metric, value):  # pragma: no cover
        return None

    def model_info(self) -> dict[str, dict[str, Any]]:
        return {e: r["info"] for e, r in self._models.items()}


def _ml_severity(r: float) -> str:
    # ratio is 1.0 at the threshold; 2.0 when as far above it as the threshold is above the training median
    return "critical" if r >= 3.0 else "high" if r >= 2.0 else "medium" if r >= 1.4 else "low"
