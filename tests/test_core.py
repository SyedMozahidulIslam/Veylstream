import unittest
from helpers import *
from veylstream.contracts import validate_event, parse_ts, iso
from veylstream.aggregator import WindowAggregator, Deduper, WindowMetrics
from veylstream.detectors import *
from veylstream.generator import SyntheticGenerator, GeneratorConfig, Scenario


class Contracts(unittest.TestCase):
    def v(self, raw, **kw):
        return validate_event(raw, source_id="s", now=T0 + 10, **kw)

    def test_valid_transaction_and_unknown_fields(self):
        r = self.v({**raw_tx(1, T0), "mystery": 5, "nested": {"a": 1}})
        self.assertTrue(r.ok)
        self.assertEqual(r.event.attrs["_unknown"], {"mystery": 5})      # scalars kept, nested dropped
        self.assertEqual(r.event.attrs["transaction_id"], "t1")

    def test_missing_and_invalid_fields_reported_not_raised(self):
        r = self.v({"event_type": "transaction"})
        codes = {p.code for p in r.problems}
        self.assertIn("missing_field", codes)
        self.assertFalse(r.ok)
        self.assertEqual(self.v("nope").problems[0].code, "invalid_type")
        self.assertEqual(self.v({**raw_tx(1, T0), "latency_ms": "fast"}).problems[0].code, "invalid_type")
        self.assertEqual(self.v({**raw_tx(1, T0), "latency_ms": -5}).problems[0].code, "out_of_range")
        self.assertEqual(self.v({**raw_tx(1, T0), "amount": float("nan")}).problems[0].code, "invalid_type")

    def test_schema_version(self):
        self.assertEqual(self.v({**raw_tx(1, T0), "schema_version": "2.0"}).problems[0].code, "schema_mismatch")
        self.assertTrue(self.v({**raw_tx(1, T0), "schema_version": "1.0"}).ok)

    def test_timestamps(self):
        self.assertEqual(parse_ts("2026-10-25T10:00:00Z"), parse_ts("2026-10-25T10:00:00"))   # naive == UTC
        self.assertEqual(parse_ts(1_790_000_000_000), 1_790_000_000.0)                       # millis
        self.assertIsNone(parse_ts("yesterday"))
        self.assertIsNone(parse_ts(True))
        self.assertEqual(self.v({**raw_tx(1, T0 + 10_000)}).problems[0].code, "timestamp_in_future")
        self.assertTrue(self.v({**raw_tx(1, T0 + 10_000)}, allow_future=True).ok)
        self.assertTrue(iso(T0).endswith("Z"))

    def test_telemetry_status_derivation_and_failure_reason_rule(self):
        t = self.v({"event_id": "a", "event_type": "telemetry", "event_timestamp": T0, "service_name": "s", "region": "r", "http_status": 503})
        self.assertEqual(t.event.status, "server_error")
        bad = self.v({**raw_tx(1, T0), "failure_reason": "x"})            # reason on a successful txn
        self.assertEqual(bad.problems[0].code, "invalid_value")

    def test_id_injection_rejected(self):
        self.assertFalse(self.v({**raw_tx(1, T0), "event_id": "a b; DROP TABLE"}).ok)


class Aggregation(unittest.TestCase):
    def test_out_of_order_within_lateness_is_counted(self):
        a = WindowAggregator(10, allowed_lateness_s=5)
        out = []
        for i, t in enumerate([1, 2, 12, 4, 25]):
            ok, c = a.add(ev(i, T0 - T0 % 10 + t)); out += c
        g = {w.window_start: w for w in out if w.entity == "global"}
        first = g[T0 - T0 % 10]
        self.assertEqual(first.n_events, 3)                        # 1, 2 and the out-of-order 4

    def test_too_late_is_dropped_and_reported(self):
        a = WindowAggregator(10, allowed_lateness_s=5)
        base = T0 - T0 % 10
        a.add(ev(1, base + 1)); a.add(ev(2, base + 40))
        ok, _ = a.add(ev(3, base + 2))
        self.assertFalse(ok); self.assertEqual(a.late_dropped, 1)

    def test_empty_windows_emitted_for_known_scopes(self):
        a = WindowAggregator(10, allowed_lateness_s=0)
        base = T0 - T0 % 10
        out = []
        for i, t in enumerate([1, 2, 55]):
            out += a.add(ev(i, base + t))[1]
        empties = [w for w in out if w.entity == "global" and w.n_events == 0]
        self.assertEqual(sorted(w.window_start - base for w in empties), [10, 20, 30, 40])   # watermark 55 closes four empty windows
        self.assertEqual(empties[0].metrics["event_rate"], 0.0)

    def test_exact_percentiles_and_rates(self):
        a = WindowAggregator(10, allowed_lateness_s=0, min_txn_for_rate=5)
        base = T0 - T0 % 10
        for i in range(100):
            a.add(ev(i, base + 1 + i * 0.05, status="failed" if i < 20 else "success", lat=float(i + 1)))
        w = [x for x in a.flush() if x.entity == "global"][0]
        self.assertAlmostEqual(w.metrics["txn_failure_rate"], 0.2)
        self.assertAlmostEqual(w.metrics["latency_p50_ms"], 50.5)       # numpy linear interpolation of 1..100
        self.assertAlmostEqual(w.metrics["latency_p95_ms"], 95.05)
        self.assertEqual(w.metrics["latency_max_ms"], 100.0)
        self.assertEqual(w.metrics["unique_entities"], 3.0)

    def test_rate_withheld_below_min_support(self):
        a = WindowAggregator(10, min_txn_for_rate=5)
        base = T0 - T0 % 10
        for i in range(3):
            a.add(ev(i, base + 1, status="failed"))
        w = [x for x in a.flush() if x.entity == "global"][0]
        self.assertIsNone(w.metrics["txn_failure_rate"])

    def test_sliding_windows_count_event_in_each_window(self):
        a = WindowAggregator(10, slide_s=5, allowed_lateness_s=0)
        self.assertEqual(len(a.windows_for(T0 - T0 % 10 + 7)), 2)

    def test_deduper_bounded(self):
        d = Deduper(3)
        self.assertFalse(d.seen("a")); self.assertTrue(d.seen("a")); self.assertEqual(d.duplicates, 1)
        for k in "bcde":
            d.seen(k)
        self.assertEqual(len(d), 3)


def wm(entity, start, **m):
    base = {"event_count": 100.0, "txn_count": 100.0, "req_count": 100.0, "event_rate": 10.0}
    base.update(m)
    return WindowMetrics(start, start + 30, entity, base, 100)


class Detectors(unittest.TestCase):
    def feed(self, det, values, metric="latency_p95_ms", entity="global"):
        out = []
        for i, v in enumerate(values):
            out += det.observe_window(wm(entity, T0 + i * 30, **{metric: v}))
        return out

    def test_insufficient_history_is_silent(self):
        self.assertEqual(self.feed(ZScoreDetector(), [100, 100, 5000]), [])

    def test_zero_variance_does_not_crash_or_overfire(self):
        d = self.feed(ZScoreDetector(), [100.0] * 30 + [101.0])
        self.assertEqual(d, [])                                     # variance floor prevents infinite z
        d = self.feed(ZScoreDetector(), [100.0] * 30 + [400.0])
        self.assertEqual(len(d), 1)

    def test_zscore_mad_iqr_fire_on_spike_and_not_on_noise(self):
        import random
        rnd = random.Random(1)
        base = [100 + rnd.gauss(0, 3) for _ in range(40)]
        for cls in (ZScoreDetector, MadDetector, IqrDetector):
            self.assertEqual(self.feed(cls(), base), [], cls.__name__)
            r = self.feed(cls(), base + [300])
            self.assertEqual(len(r), 1, cls.__name__)
            self.assertEqual(r[0].direction, "up")
            self.assertIn(r[0].severity, SEVERITIES)
            self.assertGreaterEqual(r[0].score, 1.0)

    def test_direction_filter_ignores_improvements(self):
        # latency dropping is not an anomaly by default (direction 'up' only)
        self.assertEqual(self.feed(MadDetector(), [100.0] * 30 + [10.0]), [])

    def test_baseline_not_contaminated_by_sustained_incident(self):
        det = ZScoreDetector()
        out = self.feed(det, [100.0] * 30 + [500.0] * 10)
        self.assertEqual(len(out), 10)                              # still flagged on the 10th window

    def test_rule_threshold_levels_and_min_support(self):
        r = RuleDetector()
        d = r.observe_window(wm("global", T0, txn_failure_rate=0.3))
        self.assertEqual((d[0].severity, d[0].evidence["level"]), ("critical", "critical"))
        d = r.observe_window(wm("global", T0 + 30, txn_failure_rate=0.12))
        self.assertEqual(d[0].severity, "medium")
        self.assertEqual(r.observe_window(wm("global", T0 + 60, txn_failure_rate=0.9, txn_count=5.0)), [])   # too few txns
        self.assertEqual(r.observe_window(wm("global", T0 + 90, txn_failure_rate=0.02)), [])

    def test_rule_relative_drop_and_service_scope(self):
        r = RuleDetector(rules=[{"id": "drop", "metric": "event_rate", "kind": "relative", "op": "<", "warn": 0.5, "crit": 0.8, "lookback": 12, "min_baseline": 1.0}])
        for i in range(12):
            self.assertEqual(r.observe_window(wm("global", T0 + i * 30, event_rate=10.0)), [])
        d = r.observe_window(wm("global", T0 + 400, event_rate=1.0))
        self.assertEqual(d[0].severity, "critical"); self.assertEqual(d[0].direction, "down")
        r2 = RuleDetector(rules=[{"id": "s", "metric": "latency_p95_ms", "kind": "threshold", "op": ">", "warn": 100, "crit": 200, "scope": "service:payments"}])
        self.assertEqual(r2.observe_window(wm("service:search", T0, latency_p95_ms=500.0)), [])
        self.assertEqual(len(r2.observe_window(wm("service:payments", T0, latency_p95_ms=500.0))), 1)

    def test_none_metrics_skipped(self):
        self.assertEqual(MadDetector().observe_window(wm("global", T0, latency_p95_ms=None, txn_failure_rate=None)), [])

    def test_ensemble_requires_agreement_and_never_averages_scores(self):
        vals = [100.0] * 30 + [400.0]
        e = EnsembleDetector([ZScoreDetector(), MadDetector(), IqrDetector()], "min_agree", k=3)
        out = []
        for i, v in enumerate(vals):
            out += e.observe_window(wm("global", T0 + i * 30, latency_p95_ms=v))
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0].detector, "ensemble"); self.assertEqual(out[0].score, 1.0)    # fraction of detectors, not a score mix
        e2 = EnsembleDetector([ZScoreDetector(), RuleDetector(rules=[])], "min_agree", k=2)
        out = []
        for i, v in enumerate(vals):
            out += e2.observe_window(wm("global", T0 + i * 30, latency_p95_ms=v))
        self.assertEqual(out, [])
        with self.assertRaises(ValueError):
            EnsembleDetector([], "average")

    def test_seasonal_uses_same_phase_of_prior_periods(self):
        det = SeasonalBaselineDetector(period_s=300, min_periods=3)
        out = []
        for i in range(60):                                         # 10 periods of 10 windows, strong in-period shape
            v = 100.0 if i % 10 != 5 else 300.0
            if i == 55:
                v = 1000.0                                          # phase-5 value far above the usual 300
            out += det.observe_window(wm("global", T0 + i * 30, latency_p95_ms=v))
        self.assertEqual([d.window_start for d in out], [T0 + 55 * 30])
        # a flat 300 at phase 5 is NOT anomalous even though 300 >> overall mean: that is what seasonality buys
        self.assertNotIn(T0 + 45 * 30, [d.window_start for d in out])

    def test_detection_keys(self):
        d = RuleDetector().observe_window(wm("service:x", T0, txn_failure_rate=0.5))[0]
        self.assertEqual(d.group_key, "service:x|txn_failure_rate|up")
        self.assertTrue(d.dedup_key.startswith("rule|service:x|txn_failure_rate|"))
        self.assertIn("steps", d.to_dict())


class Generator(unittest.TestCase):
    def test_deterministic_with_seed(self):
        a = SyntheticGenerator(GeneratorConfig(seed=3, base_rate=10)).generate(T0, T0 + 20)
        b = SyntheticGenerator(GeneratorConfig(seed=3, base_rate=10)).generate(T0, T0 + 20)
        c = SyntheticGenerator(GeneratorConfig(seed=4, base_rate=10)).generate(T0, T0 + 20)
        self.assertEqual(a, b); self.assertNotEqual(a, c)

    def test_scenarios_change_the_data(self):
        g = SyntheticGenerator(GeneratorConfig(seed=1, base_rate=30), [Scenario("failure_spike", T0, T0 + 60, "payments", "eu-west", 0.9)])
        ev_ = [p for _, _, p in g.generate(T0, T0 + 60) if p.get("service_name") == "payments" and p.get("region") == "eu-west" and p.get("event_type") == "transaction"]
        self.assertGreater(sum(e["status"] == "failed" for e in ev_) / len(ev_), 0.7)
        out = SyntheticGenerator(GeneratorConfig(seed=1, base_rate=30), [Scenario("producer_outage", T0, T0 + 60, "payments")]).generate(T0, T0 + 60)
        self.assertFalse([1 for _, s, _ in out if s == "src-payments"])
        with self.assertRaises(ValueError):
            SyntheticGenerator().add_scenario(Scenario("nope", 0, 1))


class MLDetector(unittest.TestCase):
    def test_trains_only_on_past_and_records_metadata(self):
        from veylstream.ml import IsolationForestDetector
        import random
        rnd = random.Random(2)
        d = IsolationForestDetector(min_train=50, contamination=0.01)
        flagged = []
        for i in range(120):
            m = dict(event_rate=10 + rnd.gauss(0, .3), txn_failure_rate=0.03 + rnd.gauss(0, .005), http_error_rate=0.01, latency_p95_ms=200 + rnd.gauss(0, 8),
                     latency_mean_ms=120 + rnd.gauss(0, 4), unique_entities=50.0)
            if i == 100:
                m.update(latency_p95_ms=900.0, txn_failure_rate=0.4)
            flagged += d.observe_window(WindowMetrics(T0 + i * 30, T0 + i * 30 + 30, "global", m, 100))
        self.assertEqual([f.window_start for f in flagged], [T0 + 100 * 30])
        info = d.model_info()["global"]
        self.assertEqual(info["n_train"], 50)
        self.assertLess(info["train_end"], T0 + 100 * 30)                 # training window precedes the anomaly
        self.assertIn("features", info); self.assertIn("model_version", info)
        self.assertEqual(flagged[0].metric, "multivariate")
        self.assertTrue(flagged[0].evidence["top_features"])


if __name__ == "__main__":
    unittest.main()
