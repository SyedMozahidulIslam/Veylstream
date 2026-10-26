"""Deterministic synthetic e-commerce / telemetry stream generator (simulated data only).

Same seed + same sequence of ``generate`` calls => identical output. Scenarios inject anomalies and
double as ground-truth labels for the research module.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field, asdict
from typing import Any

import numpy as np

from .contracts import iso

SERVICES = {"checkout": 0.30, "payments": 0.30, "catalog": 0.25, "search": 0.15}
REGIONS = {"us-east": 0.45, "eu-west": 0.35, "ap-south": 0.20}
TXN_SERVICES = {"checkout", "payments"}
LATENCY_MEDIAN_MS = {"checkout": 140.0, "payments": 180.0, "catalog": 45.0, "search": 70.0}
BASE_FAILURE = {"checkout": 0.02, "payments": 0.03}
FAIL_REASONS = ["card_declined", "insufficient_funds", "processor_timeout", "fraud_check", "network_error"]
SCENARIO_KINDS = {"failure_spike", "latency_degradation", "traffic_spike", "traffic_drop", "producer_outage",
                  "gradual_drift", "duplicates", "late_events", "invalid_payloads"}


@dataclass
class Scenario:
    kind: str
    start: float
    end: float
    service: str | None = None
    region: str | None = None
    magnitude: float = 1.0       # meaning depends on kind (see _apply docs below)
    label: bool = True           # counts as ground-truth anomaly (duplicates/late/invalid are data-quality, not metric anomalies)

    def active(self, t: float) -> bool:
        return self.start <= t < self.end

    def matches(self, service: str, region: str) -> bool:
        return (self.service in (None, service)) and (self.region in (None, region))


@dataclass
class GeneratorConfig:
    seed: int = 42
    base_rate: float = 20.0                # events per second, all sources
    day_length_s: float | None = None      # compressed "day" for a sinusoidal volume cycle
    heartbeat_s: float = 10.0
    noise: float = 0.06
    environment: str = "demo"


class SyntheticGenerator:
    def __init__(self, config: GeneratorConfig | None = None, scenarios: list[Scenario] | None = None) -> None:
        self.cfg = config or GeneratorConfig()
        self.rng = np.random.default_rng(self.cfg.seed)
        self.scenarios: list[Scenario] = list(scenarios or [])
        self._n = 0
        self._next_hb: dict[str, float] = {}
        self._svc = list(SERVICES)
        self._svc_p = np.array(list(SERVICES.values())) / sum(SERVICES.values())
        self._reg = list(REGIONS)
        self._reg_p = np.array(list(REGIONS.values())) / sum(REGIONS.values())

    @staticmethod
    def source_id(service: str) -> str:
        return f"src-{service}"

    def add_scenario(self, s: Scenario) -> None:
        if s.kind not in SCENARIO_KINDS:
            raise ValueError(f"unknown scenario kind {s.kind!r}; choose from {sorted(SCENARIO_KINDS)}")
        self.scenarios.append(s)

    def _rate(self, t: float) -> float:
        r = self.cfg.base_rate
        if self.cfg.day_length_s:
            r *= 1.0 + 0.4 * math.sin(2 * math.pi * (t % self.cfg.day_length_s) / self.cfg.day_length_s)
        return r

    def _active(self, t: float, kind: str, service: str, region: str) -> list[Scenario]:
        return [s for s in self.scenarios if s.kind == kind and s.active(t) and s.matches(service, region)]

    def _new_id(self) -> str:
        self._n += 1
        return f"e{self.cfg.seed}-{self._n:09d}"

    def generate(self, t0: float, t1: float) -> list[tuple[float, str, Any]]:
        """Return (arrival_time, source_id, raw_payload) tuples for event times in [t0, t1), ordered by arrival."""
        out: list[tuple[float, str, Any]] = []
        t = float(np.floor(t0))
        while t < t1:
            out.extend(self._second(t, max(t, t0), min(t + 1.0, t1)))
            t += 1.0
        out.sort(key=lambda x: x[0])
        return out

    def _second(self, sec: float, lo: float, hi: float) -> list[tuple[float, str, Any]]:
        cfg, rng = self.cfg, self.rng
        res: list[tuple[float, str, Any]] = []
        noise = float(np.exp(rng.normal(0, cfg.noise)))
        mult_global = 1.0
        for s in self.scenarios:
            if s.active(sec) and s.service is None and s.region is None:
                if s.kind == "traffic_spike":
                    mult_global *= s.magnitude
                elif s.kind == "traffic_drop":
                    mult_global *= s.magnitude
        n = rng.poisson(self._rate(sec) * noise * mult_global * (hi - lo))
        for _ in range(int(n)):
            service = self._svc[rng.choice(len(self._svc), p=self._svc_p)]
            region = self._reg[rng.choice(len(self._reg), p=self._reg_p)]
            ts = lo + float(rng.random()) * (hi - lo)
            # scoped traffic scenarios thin or amplify through accept / duplicate-in-place sampling
            keep = 1.0
            for s in self._active(ts, "traffic_drop", service, region):
                if s.service or s.region:
                    keep *= s.magnitude
            if keep < 1.0 and rng.random() > keep:
                continue
            if self._active(ts, "producer_outage", service, region):
                continue
            ev = self._event(ts, service, region)
            arrival = ts
            if self._active(ts, "late_events", service, region) and rng.random() < 0.15:
                delay = float(rng.uniform(15, 90))
                arrival = ts + delay
            src = self.source_id(service)
            if self._active(ts, "invalid_payloads", service, region) and rng.random() < 0.10:
                bad = dict(ev)
                kind = rng.integers(0, 3)
                if kind == 0:
                    bad.pop("event_timestamp", None)
                elif kind == 1:
                    bad["latency_ms"] = "not-a-number"
                else:
                    bad["schema_version"] = "9.9"
                res.append((arrival, src, bad))
                continue
            res.append((arrival, src, ev))
            if self._active(ts, "duplicates", service, region) and rng.random() < 0.10:
                res.append((arrival + float(rng.uniform(0.01, 2.0)), src, dict(ev)))
            if self._active(ts, "traffic_spike", service, region) and (service or region):
                extra = int(max(s.magnitude for s in self._active(ts, "traffic_spike", service, region)) - 1)
                for _ in range(extra):
                    res.append((arrival, src, self._event(ts + float(rng.random()) * 0.5, service, region)))
        # heartbeats
        for service in self._svc:
            src = self.source_id(service)
            nxt = self._next_hb.setdefault(src, lo)
            while nxt < hi:
                if nxt >= lo and not self._active(nxt, "producer_outage", service, ""):
                    res.append((nxt, src, {"event_id": self._new_id(), "event_type": "heartbeat",
                                           "event_timestamp": iso(nxt), "source_type": "application"}))
                nxt += cfg.heartbeat_s
            self._next_hb[src] = nxt
        return res

    def _event(self, ts: float, service: str, region: str) -> dict[str, Any]:
        rng = self.rng
        lat_mult = 1.0
        for s in self._active(ts, "latency_degradation", service, region):
            lat_mult *= s.magnitude
        for s in self._active(ts, "gradual_drift", service, region):
            lat_mult *= 1.0 + (s.magnitude - 1.0) * (ts - s.start) / max(s.end - s.start, 1.0)
        latency = float(max(1.0, rng.lognormal(math.log(LATENCY_MEDIAN_MS[service] * lat_mult), 0.35)))
        base = {"event_id": self._new_id(), "schema_version": "1.0", "event_timestamp": iso(ts), "service_name": service,
                "region": region, "environment": self.cfg.environment, "latency_ms": round(latency, 2),
                "entity_id": f"u{int(rng.integers(1, 5000))}", "correlation_id": f"c{int(rng.integers(1, 10**8))}"}
        if service in TXN_SERVICES and rng.random() < 0.7:
            p = BASE_FAILURE[service]
            for s in self._active(ts, "failure_spike", service, region):
                p = max(p, s.magnitude)
            for s in self._active(ts, "gradual_drift", service, region):
                p += (s.magnitude - 1.0) * 0.02 * (ts - s.start) / max(s.end - s.start, 1.0)
            failed = rng.random() < p
            base.update(event_type="transaction", status="failed" if failed else "success",
                        amount=round(float(rng.lognormal(3.4, 0.8)), 2), currency="USD",
                        payment_method=str(rng.choice(["card", "wallet", "bank_transfer", "bnpl"], p=[.6, .25, .1, .05])))
            if failed:
                base["failure_reason"] = str(rng.choice(FAIL_REASONS))
        else:
            p5 = 0.005
            for s in self._active(ts, "failure_spike", service, region):
                p5 = max(p5, s.magnitude * 0.6)
            r = rng.random()
            status = 503 if r < p5 else 404 if r < p5 + 0.02 else 200
            base.update(event_type="telemetry", http_status=status, endpoint=str(rng.choice(["/cart", "/pay", "/items", "/q"])),
                        cpu_pct=round(float(np.clip(rng.normal(35 * lat_mult ** 0.5, 6), 0, 100)), 1),
                        mem_pct=round(float(np.clip(rng.normal(55, 4), 0, 100)), 1))
        return base

    def ground_truth(self) -> list[dict[str, Any]]:
        """Labeled metric anomalies implied by the scenarios (data-quality scenarios excluded)."""
        metric_for = {"failure_spike": ["txn_failure_rate", "http_error_rate"], "latency_degradation": ["latency_p95_ms", "latency_mean_ms"],
                      "gradual_drift": ["latency_p95_ms", "latency_mean_ms", "txn_failure_rate"],
                      "traffic_spike": ["event_rate"], "traffic_drop": ["event_rate"], "producer_outage": ["event_rate"]}
        out = []
        for s in self.scenarios:
            if s.kind not in metric_for:
                continue
            ents = [f"service:{s.service}"] if s.service else []
            ents += [f"region:{s.region}"] if s.region else []
            if not ents:                       # platform-wide scenario: every scope is genuinely affected
                ents = ["global"] + [f"service:{x}" for x in SERVICES] + [f"region:{x}" for x in REGIONS]
            # scopes along the other dimension see a diluted version of a scoped incident: ignore them when scoring
            out.append({**asdict(s), "entities": ents, "metrics": metric_for[s.kind], "ignore_prefixes": ["service:", "region:"]})
        return out
