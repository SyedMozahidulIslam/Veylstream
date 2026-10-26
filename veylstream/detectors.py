"""Anomaly detector framework.

Every detector consumes closed window metrics (``WindowMetrics``) and returns zero or more
``Detection`` objects. ``score`` is *deviation magnitude divided by the detector's own
threshold* (>= 1.0 means fired). Scores are therefore comparable within a detector family
only; the ensemble combines detectors by voting, never by averaging raw scores.

Severity is a documented function of that ratio (see ``severity_from_ratio``). It expresses
how far past the threshold an observation is. It is NOT a probability or statistical confidence.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field, asdict
from typing import Any

import numpy as np

from .aggregator import WindowMetrics

SEVERITIES = ["info", "low", "medium", "high", "critical"]
SEV_RANK = {s: i for i, s in enumerate(SEVERITIES)}
DEFAULT_METRICS = ["event_rate", "txn_failure_rate", "http_error_rate", "latency_p95_ms", "latency_mean_ms"]
DIRECTIONS = {"txn_failure_rate": "up", "http_error_rate": "up", "latency_p95_ms": "up",
              "latency_mean_ms": "up", "event_rate": "both"}
ABS_FLOOR = {"txn_failure_rate": 0.01, "http_error_rate": 0.01, "latency_p95_ms": 10.0,
             "latency_mean_ms": 5.0, "event_rate": 0.5}


def severity_from_ratio(r: float) -> str:
    """ratio<1 -> none (caller must not call); 1-1.5 low; 1.5-2.5 medium; 2.5-4 high; >=4 critical."""
    return "critical" if r >= 4 else "high" if r >= 2.5 else "medium" if r >= 1.5 else "low"


INVESTIGATION = {
    "txn_failure_rate": ["Break failures down by failure_reason, payment_method and region in Event Explorer.",
                         "Compare against deploys or config changes near the deviation start time.",
                         "Check whether one downstream processor accounts for most failures."],
    "http_error_rate": ["Inspect 5xx events by endpoint and service.", "Check dependency health for the affected service.",
                        "Look for a correlated latency increase."],
    "latency_p95_ms": ["Check whether latency rose for one region/service or everywhere.",
                       "Compare with request volume (saturation) and CPU/memory telemetry.",
                       "Inspect traces via trace_id for the slowest events."],
    "latency_mean_ms": ["Compare with p95: a mean-only shift suggests broad degradation.", "Check volume and CPU/memory."],
    "event_rate": ["Check producer heartbeats for the affected source (stopped producer vs. real traffic drop).",
                   "For a spike, check for retries or duplicate events and downstream saturation.",
                   "Compare with the Data Quality page for validation failures or late events."],
    "multivariate": ["Open the feature breakdown in the detection evidence to see which features are most unusual.",
                     "Cross-check with the univariate detectors on the same entity and window."],
}


@dataclass
class Detection:
    detector: str
    version: str
    entity: str
    metric: str
    window_start: float
    window_end: float
    observed: float
    expected: float | None
    lower: float | None
    upper: float | None
    score: float
    severity: str
    direction: str
    explanation: str
    support: dict[str, float | None] = field(default_factory=dict)
    config: dict[str, Any] = field(default_factory=dict)
    evidence: dict[str, Any] = field(default_factory=dict)
    steps: list[str] = field(default_factory=list)

    @property
    def dedup_key(self) -> str:
        """Unique per detector+series+window: makes persistence idempotent across replays/restarts."""
        return f"{self.detector}|{self.entity}|{self.metric}|{int(self.window_start)}"

    @property
    def group_key(self) -> str:
        """Detector-independent key used to group detections into one alert."""
        return f"{self.entity}|{self.metric}|{self.direction}"

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["dedup_key"] = self.dedup_key
        d["group_key"] = self.group_key
        return d


class _Series:
    __slots__ = ("vals", "recent", "excluded")

    def __init__(self, maxlen: int) -> None:
        self.vals: deque[float] = deque(maxlen=maxlen)     # baseline (anomalies excluded)
        self.recent: deque[float] = deque(maxlen=8)        # raw recent values (for rate-of-change)
        self.excluded = 0


class Detector(ABC):
    name = "base"
    version = "1.0.0"
    defaults: dict[str, Any] = {}

    def __init__(self, **config: Any) -> None:
        self.config: dict[str, Any] = {"metrics": DEFAULT_METRICS, "scopes": None, **self.defaults, **config}
        self.metrics: list[str] = list(self.config["metrics"])

    def applies(self, entity: str) -> bool:
        scopes = self.config.get("scopes")
        return not scopes or any(entity == s or (s.endswith(":") and entity.startswith(s)) for s in scopes)

    def observe_window(self, wm: WindowMetrics) -> list[Detection]:
        if not self.applies(wm.entity):
            return []
        out = []
        for metric in self.metrics:
            v = wm.metrics.get(metric)
            if v is None:
                continue
            d = self.evaluate(wm, metric, float(v))
            if d is not None:
                out.append(d)
        return out

    @abstractmethod
    def evaluate(self, wm: WindowMetrics, metric: str, value: float) -> Detection | None: ...

    def _make(self, wm: WindowMetrics, metric: str, observed: float, expected: float | None, lower: float | None,
              upper: float | None, ratio: float, explanation: str, evidence: dict, direction: str,
              severity: str | None = None) -> Detection:
        return Detection(
            detector=self.name, version=self.version, entity=wm.entity, metric=metric, window_start=wm.window_start,
            window_end=wm.window_end, observed=observed, expected=expected, lower=lower, upper=upper,
            score=round(float(ratio), 4), severity=severity or severity_from_ratio(ratio), direction=direction,
            explanation=explanation, support={"event_count": wm.metrics.get("event_count"),
                                              "txn_count": wm.metrics.get("txn_count"),
                                              "req_count": wm.metrics.get("req_count")},
            config={k: v for k, v in self.config.items() if k != "rules"} if self.name != "rule" else dict(self.config),
            evidence=evidence, steps=INVESTIGATION.get(metric, []))


def _fmt(v: float | None) -> str:
    return "n/a" if v is None else (f"{v:.3f}" if abs(v) < 10 else f"{v:.1f}")


class _BaselineDetector(Detector):
    """Shared rolling-baseline bookkeeping. Anomalous points are kept out of the baseline so a
    sustained incident does not normalise itself; after ``max_excluded`` consecutive exclusions the
    detector adapts (the new level is accepted as the baseline)."""

    defaults = {"baseline_windows": 60, "min_samples": 20, "max_excluded": 30, "rel_floor": 0.05}

    def __init__(self, **config: Any) -> None:
        super().__init__(**config)
        self._series: dict[tuple[str, str], _Series] = {}

    def _s(self, entity: str, metric: str) -> _Series:
        k = (entity, metric)
        if k not in self._series:
            self._series[k] = _Series(int(self.config["baseline_windows"]))
        return self._series[k]

    def _dirs(self, metric: str) -> str:
        return self.config.get("directions", {}).get(metric) or DIRECTIONS.get(metric, "both")

    def _floor(self, metric: str, center: float) -> float:
        return max(abs(center) * self.config["rel_floor"], ABS_FLOOR.get(metric, 1e-6))

    def evaluate(self, wm: WindowMetrics, metric: str, value: float) -> Detection | None:
        s = self._s(wm.entity, metric)
        det = None
        if len(s.vals) >= self.config["min_samples"]:
            det = self.judge(wm, metric, value, np.asarray(s.vals, dtype=float), s)
        # baseline update
        if det is None:
            s.vals.append(value)
            s.excluded = 0
        else:
            s.excluded += 1
            if s.excluded >= self.config["max_excluded"]:
                s.vals.append(value)
        s.recent.append(value)
        return det

    @abstractmethod
    def judge(self, wm: WindowMetrics, metric: str, value: float, base: np.ndarray, s: _Series) -> Detection | None: ...

    def _dir_ok(self, metric: str, delta: float) -> str | None:
        d = "up" if delta > 0 else "down"
        allowed = self._dirs(metric)
        return d if allowed in ("both", d) else None


class ZScoreDetector(_BaselineDetector):
    """Rolling mean / standard deviation z-score. Zero variance is handled with a floor."""
    name = "zscore"
    defaults = {**_BaselineDetector.defaults, "threshold": 4.0}

    def judge(self, wm, metric, value, base, s):
        mean, std = float(base.mean()), float(base.std(ddof=1))
        eff = max(std, self._floor(metric, mean))
        z = (value - mean) / eff
        d = self._dir_ok(metric, z)
        if d is None or abs(z) < self.config["threshold"]:
            return None
        ratio = abs(z) / self.config["threshold"]
        lo, hi = mean - self.config["threshold"] * eff, mean + self.config["threshold"] * eff
        return self._make(wm, metric, value, mean, lo, hi, ratio,
                          f"{metric} on {wm.entity} was {_fmt(value)} vs rolling mean {_fmt(mean)} "
                          f"(z={z:+.1f}, threshold {self.config['threshold']}, {len(base)} baseline windows).",
                          {"z": round(z, 3), "mean": mean, "std": std, "effective_std": eff, "n_baseline": len(base)}, d)


class MadDetector(_BaselineDetector):
    """Robust z-score from median and median absolute deviation (MAD*1.4826 ~ sigma for normal data)."""
    name = "mad"
    defaults = {**_BaselineDetector.defaults, "threshold": 5.0}

    def judge(self, wm, metric, value, base, s):
        med = float(np.median(base))
        sigma = 1.4826 * float(np.median(np.abs(base - med)))
        eff = max(sigma, self._floor(metric, med))
        z = (value - med) / eff
        d = self._dir_ok(metric, z)
        if d is None or abs(z) < self.config["threshold"]:
            return None
        ratio = abs(z) / self.config["threshold"]
        return self._make(wm, metric, value, med, med - self.config["threshold"] * eff, med + self.config["threshold"] * eff,
                          ratio, f"{metric} on {wm.entity} was {_fmt(value)} vs rolling median {_fmt(med)} "
                          f"(robust z={z:+.1f}, threshold {self.config['threshold']}).",
                          {"robust_z": round(z, 3), "median": med, "mad_sigma": sigma, "effective_sigma": eff,
                           "n_baseline": len(base)}, d)


class IqrDetector(_BaselineDetector):
    """Tukey fences: fires when the value is more than k*IQR beyond Q1/Q3 (k=3 'far out')."""
    name = "iqr"
    defaults = {**_BaselineDetector.defaults, "k": 3.0}

    def judge(self, wm, metric, value, base, s):
        q1, med, q3 = (float(x) for x in np.percentile(base, [25, 50, 75]))
        iqr = max(q3 - q1, self._floor(metric, med))
        k = self.config["k"]
        lo, hi = q1 - k * iqr, q3 + k * iqr
        if value > hi:
            dev, d = (value - q3) / iqr, "up"
        elif value < lo:
            dev, d = (q1 - value) / iqr, "down"
        else:
            return None
        if self._dirs(metric) not in ("both", d):
            return None
        return self._make(wm, metric, value, med, lo, hi, dev / k,
                          f"{metric} on {wm.entity} was {_fmt(value)}, outside Tukey fences [{_fmt(lo)}, {_fmt(hi)}] "
                          f"(k={k}, IQR={_fmt(iqr)}).", {"q1": q1, "median": med, "q3": q3, "iqr": iqr, "n_baseline": len(base)}, d)


class BaselineDeviationDetector(_BaselineDetector):
    """Percentage deviation from the rolling median baseline."""
    name = "baseline_deviation"
    defaults = {**_BaselineDetector.defaults, "tolerance": 0.5}

    def judge(self, wm, metric, value, base, s):
        med = float(np.median(base))
        denom = max(abs(med), ABS_FLOOR.get(metric, 1e-6))
        pct = (value - med) / denom
        d = self._dir_ok(metric, pct)
        tol = self.config["tolerance"]
        if d is None or abs(pct) < tol or abs(value - med) < ABS_FLOOR.get(metric, 1e-6) * 2:
            return None
        return self._make(wm, metric, value, med, med * (1 - tol), med * (1 + tol), abs(pct) / tol,
                          f"{metric} on {wm.entity} deviates {pct:+.0%} from its rolling median {_fmt(med)} "
                          f"(tolerance ±{tol:.0%}).", {"pct_deviation": round(pct, 4), "median": med, "n_baseline": len(base)}, d)


class RateOfChangeDetector(_BaselineDetector):
    """Sudden shift: change of the current value relative to the mean of the last ``lookback`` raw values."""
    name = "rate_of_change"
    defaults = {**_BaselineDetector.defaults, "threshold": 0.6, "lookback": 3, "min_samples": 5}

    def judge(self, wm, metric, value, base, s):
        lb = int(self.config["lookback"])
        if len(s.recent) < lb:
            return None
        prev = float(np.mean(list(s.recent)[-lb:]))
        floor = ABS_FLOOR.get(metric, 1e-6)
        change = (value - prev) / max(abs(prev), floor)
        d = self._dir_ok(metric, change)
        if d is None or abs(change) < self.config["threshold"] or abs(value - prev) < 2 * floor:
            return None
        return self._make(wm, metric, value, prev, None, None, abs(change) / self.config["threshold"],
                          f"{metric} on {wm.entity} shifted {change:+.0%} versus the mean of the previous {lb} windows ({_fmt(prev)}).",
                          {"relative_change": round(change, 4), "previous_mean": prev}, d)


class SeasonalBaselineDetector(Detector):
    """Compares a value with the same phase of prior periods (``period_s`` apart).

    This models seasonality ONLY at the configured period and only once ``min_periods`` comparable
    periods exist; it does not discover seasonality. Spread is the MAD-based sigma across prior periods
    (floored), so a tight history yields a tight band.
    """
    name = "seasonal"
    defaults = {"period_s": 86400.0, "min_periods": 3, "max_periods": 14, "threshold": 4.0, "rel_floor": 0.08}

    def __init__(self, **config: Any) -> None:
        super().__init__(**config)
        self._hist: dict[tuple[str, str], dict[int, float]] = {}

    def evaluate(self, wm, metric, value):
        period = float(self.config["period_s"])
        step = wm.window_end - wm.window_start
        key = (wm.entity, metric)
        h = self._hist.setdefault(key, {})
        slot = int(round(wm.window_start / step))
        per_slots = int(round(period / step))
        prior = [h[slot - k * per_slots] for k in range(1, int(self.config["max_periods"]) + 1) if (slot - k * per_slots) in h]
        h[slot] = value
        for old in [s for s in h if s < slot - per_slots * (self.config["max_periods"] + 1)]:
            del h[old]
        if len(prior) < self.config["min_periods"]:
            return None
        arr = np.asarray(prior)
        med = float(np.median(arr))
        sigma = max(1.4826 * float(np.median(np.abs(arr - med))), abs(med) * self.config["rel_floor"], ABS_FLOOR.get(metric, 1e-6))
        z = (value - med) / sigma
        d = "up" if z > 0 else "down"
        allowed = DIRECTIONS.get(metric, "both")
        if allowed not in ("both", d) or abs(z) < self.config["threshold"]:
            return None
        th = self.config["threshold"]
        return self._make(wm, metric, value, med, med - th * sigma, med + th * sigma, abs(z) / th,
                          f"{metric} on {wm.entity} was {_fmt(value)} vs {_fmt(med)} at the same phase of the previous "
                          f"{len(arr)} periods of {period:.0f}s (z={z:+.1f}).",
                          {"seasonal_z": round(z, 3), "n_periods": len(arr), "period_s": period}, d)


class RuleDetector(Detector):
    """Configurable threshold / relative-change rules with warning and critical levels and minimum support."""
    name = "rule"
    version = "1.1.0"
    DEFAULT_RULES = [
        {"id": "failure-rate", "metric": "txn_failure_rate", "kind": "threshold", "op": ">", "warn": 0.10, "crit": 0.25,
         "min_support": {"metric": "txn_count", "min": 20}},
        {"id": "http-errors", "metric": "http_error_rate", "kind": "threshold", "op": ">", "warn": 0.05, "crit": 0.15,
         "min_support": {"metric": "req_count", "min": 30}},
        {"id": "latency-p95", "metric": "latency_p95_ms", "kind": "threshold", "op": ">", "warn": 800.0, "crit": 1500.0,
         "min_support": {"metric": "event_count", "min": 20}},
        {"id": "throughput-drop", "metric": "event_rate", "kind": "relative", "op": "<", "warn": 0.5, "crit": 0.8,
         "lookback": 12, "min_baseline": 1.0},
        {"id": "throughput-spike", "metric": "event_rate", "kind": "relative", "op": ">", "warn": 1.0, "crit": 3.0,
         "lookback": 12, "min_baseline": 1.0},
    ]

    def __init__(self, **config: Any) -> None:
        config.setdefault("rules", [dict(r) for r in self.DEFAULT_RULES])
        super().__init__(**config)
        self.config["metrics"] = sorted({r["metric"] for r in self.config["rules"]})
        self.metrics = self.config["metrics"]
        self._hist: dict[tuple[str, str], deque[float]] = {}

    def observe_window(self, wm):
        out = []
        for r in self.config["rules"]:
            if r.get("enabled", True) is False:
                continue
            scope = r.get("scope")
            if scope and not (wm.entity == scope or (scope.endswith(":") and wm.entity.startswith(scope))):
                continue
            v = wm.metrics.get(r["metric"])
            if v is None:
                continue
            d = self._rule(wm, r, float(v))
            if d is not None:
                out.append(d)
        return out

    def evaluate(self, wm, metric, value):  # pragma: no cover - rules use observe_window
        return None

    def _rule(self, wm, r, value):
        metric = r["metric"]
        ms = r.get("min_support")
        if ms and (wm.metrics.get(ms["metric"]) or 0) < ms["min"]:
            return None
        if r["kind"] == "threshold":
            op, warn, crit = r["op"], r["warn"], r["crit"]
            hit_c = value > crit if op == ">" else value < crit
            hit_w = value > warn if op == ">" else value < warn
            if not hit_w:
                return None
            ratio = value / warn if op == ">" else (warn / value if value > 0 else 10.0)
            sev = "critical" if hit_c else "medium"
            return self._make(wm, metric, value, warn, None, warn if op == ">" else None, max(ratio, 1.0),
                              f"{metric} on {wm.entity} is {_fmt(value)}, {'above' if op == '>' else 'below'} the "
                              f"{'critical' if hit_c else 'warning'} threshold {_fmt(crit if hit_c else warn)} "
                              f"(rule {r['id']}).", {"rule_id": r["id"], "level": "critical" if hit_c else "warning",
                                                     "min_support": ms}, "up" if op == ">" else "down", severity=sev)
        # relative rule against the mean of previous windows
        key = (wm.entity, r["id"])
        h = self._hist.setdefault(key, deque(maxlen=int(r.get("lookback", 12))))
        res = None
        if len(h) >= max(3, int(r.get("lookback", 12)) // 2):
            base = float(np.mean(h))
            if base >= r.get("min_baseline", 0.0):
                chg = (value - base) / base
                up = r["op"] == ">"
                mag = chg if up else -chg
                if mag >= r["warn"]:
                    crit = mag >= r["crit"]
                    res = self._make(wm, metric, value, base, None, None, max(mag / r["warn"], 1.0),
                                     f"{metric} on {wm.entity} {'rose' if up else 'fell'} {abs(chg):.0%} versus the mean of the "
                                     f"previous {len(h)} windows ({_fmt(base)}) (rule {r['id']}).",
                                     {"rule_id": r["id"], "level": "critical" if crit else "warning", "baseline": base,
                                      "relative_change": round(chg, 4)}, "up" if up else "down",
                                     severity="critical" if crit else "medium")
        if res is None:
            h.append(value)
        return res


class EnsembleDetector:
    """Combines child detections for the same (entity, metric, window).

    strategy 'min_agree': at least ``k`` distinct detectors fire.
    strategy 'weighted_vote': sum of weights of firing detectors / total weight >= ``vote_threshold``.
    Raw scores are never averaged. Severity is the highest child severity; the child list is kept as evidence.
    """
    name = "ensemble"
    version = "1.0.0"

    def __init__(self, children: list[Detector], strategy: str = "min_agree", k: int = 2,
                 weights: dict[str, float] | None = None, vote_threshold: float = 0.5) -> None:
        if strategy not in ("min_agree", "weighted_vote"):
            raise ValueError("strategy must be min_agree or weighted_vote")
        self.children, self.strategy, self.k = children, strategy, k
        self.weights = weights or {}
        self.vote_threshold = vote_threshold
        self.config = {"strategy": strategy, "k": k, "weights": self.weights, "vote_threshold": vote_threshold,
                       "children": [c.name for c in children]}

    def observe_window(self, wm: WindowMetrics) -> list[Detection]:
        groups: dict[str, list[Detection]] = {}
        for c in self.children:
            for d in c.observe_window(wm):
                groups.setdefault(d.metric, []).append(d)
        out = []
        total_w = sum(self.weights.get(c.name, 1.0) for c in self.children)
        for metric, ds in groups.items():
            names = {d.detector for d in ds}
            fired_w = sum(self.weights.get(n, 1.0) for n in names)
            ok = len(names) >= self.k if self.strategy == "min_agree" else fired_w / total_w >= self.vote_threshold
            if not ok:
                continue
            best = max(ds, key=lambda d: (SEV_RANK[d.severity], d.score))
            if len({d.direction for d in ds}) > 1:
                continue  # contradictory directions: do not combine
            how = f"{len(names)}/{len(self.children)} detectors agree ({', '.join(sorted(names))})" \
                if self.strategy == "min_agree" else f"weighted vote {fired_w / total_w:.0%} ≥ {self.vote_threshold:.0%} ({', '.join(sorted(names))})"
            out.append(Detection(
                detector=self.name, version=self.version, entity=wm.entity, metric=metric, window_start=wm.window_start,
                window_end=wm.window_end, observed=best.observed, expected=best.expected, lower=best.lower, upper=best.upper,
                score=round(fired_w / total_w, 4), severity=best.severity, direction=best.direction,
                explanation=f"Ensemble ({self.strategy}): {how}. Most severe finding: {best.explanation}",
                support=best.support, config=self.config,
                evidence={"children": [{"detector": d.detector, "severity": d.severity, "score": d.score} for d in ds],
                          "strategy": self.strategy}, steps=best.steps))
        return out


REGISTRY: dict[str, type] = {c.name: c for c in (ZScoreDetector, MadDetector, IqrDetector, BaselineDeviationDetector,
                                                  RateOfChangeDetector, SeasonalBaselineDetector, RuleDetector)}


def build_detector(name: str, **config: Any):
    if name == "isolation_forest":
        from .ml import IsolationForestDetector
        return IsolationForestDetector(**config)
    if name not in REGISTRY:
        raise ValueError(f"unknown detector {name!r}; available: {sorted(REGISTRY) + ['isolation_forest']}")
    return REGISTRY[name](**config)


def default_detector_set() -> list:
    """Default live set: rules + two robust statistical detectors (low false-positive posture)."""
    return [RuleDetector(), MadDetector(), ZScoreDetector()]
