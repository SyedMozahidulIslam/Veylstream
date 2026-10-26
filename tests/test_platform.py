import json, os, threading, time, unittest, http.server
from helpers import *
from veylstream.alerts import AlertManager, AlertPolicy, InvalidTransition, Notifier
from veylstream.broker import FileLogBroker, MemoryBroker
from veylstream.detectors import Detection, default_detector_set
from veylstream.pipeline import Backpressure, LocalProcessor, Pipeline, PipelineConfig, RateLimited
from veylstream.store import Store, verify_password, hash_password
from veylstream.generator import GeneratorConfig, SyntheticGenerator, Scenario

SRC = {"id": "src-x", "rate_limit_per_s": 1e9, "simulated": 1}


def det(entity="service:p", metric="txn_failure_rate", start=T0, sev="high", detector="rule"):
    return Detection(detector, "1", entity, metric, start, start + 30, 0.4, 0.03, None, None, 2.0, sev, "up", "x")


class Persistence(unittest.TestCase):
    def test_migrations_idempotent_and_password_hashing(self):
        d = tmpdir(); a = Store(d + "/a.db"); v = a.schema_version(); a.close()
        b = Store(d + "/a.db"); self.assertEqual(b.schema_version(), v)
        h = hash_password("hunter2-hunter2"); self.assertNotIn("hunter2", h)
        self.assertTrue(verify_password("hunter2-hunter2", h)); self.assertFalse(verify_password("wrong", h)); self.assertFalse(verify_password("x", "garbage"))

    def test_file_broker_durable_reopen_and_torn_write_recovery(self):
        d = tmpdir(); b = FileLogBroker(d, segment_records=3)
        self.assertEqual(b.append([{"i": i} for i in range(7)]), 0); b.close()
        with open(os.path.join(d, sorted(os.listdir(d))[-1]), "ab") as f:
            f.write(b'{"torn":')                                    # crash mid-write
        b = FileLogBroker(d, segment_records=3)
        self.assertEqual(b.end_offset(), 7)
        self.assertEqual([r["i"] for _, r in b.read(5, 10)], [5, 6])
        self.assertTrue(b.durable)
        self.assertEqual(b.append([{"i": 7}]), 7)


class PipelineTests(unittest.TestCase):
    def make(self, d=None, **cfg):
        d = d or tmpdir(); st = Store(d + "/db"); b = FileLogBroker(d + "/log")
        p = Pipeline(st, "ws", b, PipelineConfig(window_s=30, allowed_lateness_s=20, **cfg), default_detector_set())
        return d, st, b, p

    def test_ingest_ack_semantics(self):
        d, st, b, p = self.make()
        res = p.ingest(SRC, [raw_tx(1, T0), raw_tx(1, T0), {"event_type": "x"}, raw_tx(2, T0 + 1)], now=T0 + 5)
        self.assertEqual((res.accepted, res.duplicates, len(res.rejected)), (2, 1, 1))
        self.assertTrue(res.durable)                                # file broker with fsync
        self.assertEqual(b.end_offset(), 2)                         # accepted events are in the durable log before the ack
        self.assertEqual(st.q1("select reason_code r from dead_letters")["r"], "missing_field")   # rejection is recorded, not discarded
        self.assertFalse(Pipeline(st, "w2", MemoryBroker(), PipelineConfig(), [], scope="x").ingest(SRC, [raw_tx(9, T0)], now=T0).durable)

    def test_backpressure_and_rate_limit(self):
        d, st, b, p = self.make(max_lag=3)
        p.ingest(SRC, [raw_tx(i, T0 + i) for i in range(5)], now=T0 + 9)
        with self.assertRaises(Backpressure):
            p.ingest(SRC, [raw_tx(99, T0)], now=T0)
        d, st, b, p = self.make()
        with self.assertRaises(RateLimited):
            p.ingest({"id": "slow", "rate_limit_per_s": 2}, [raw_tx(i, T0 + i) for i in range(50)], now=T0 + 99)

    def test_end_to_end_processing_persists_windows_and_events(self):
        d, st, b, p = self.make()
        g = SyntheticGenerator(GeneratorConfig(seed=2, base_rate=15)); raw = g.generate(T0, T0 + 300)
        for i in range(0, len(raw), 400):
            p.ingest(SRC, [x for _, _, x in raw[i:i + 400]], now=T0 + 999)
        proc = LocalProcessor(p, dq_interval_s=0); proc.start(); self.assertTrue(proc.drain(20)); proc.stop(); p.finish()
        n_tx = sum(1 for _, _, x in raw if x["event_type"] != "heartbeat")
        self.assertEqual(st.q1("select count(*) c from events")["c"] + p.agg.late_dropped, n_tx)
        w = st.q1("select sum(n_events) s from windows where entity='global'")["s"]
        self.assertEqual(w, st.q1("select count(*) c from events")["c"])    # every stored event is aggregated exactly once
        self.assertEqual(p.lag(), 0)

    def test_restart_yields_identical_aggregates(self):
        raw = SyntheticGenerator(GeneratorConfig(seed=5, base_rate=15)).generate(T0, T0 + 900)
        payloads = [x for _, _, x in raw]

        def run(split):
            d = tmpdir(); st = Store(d + "/db"); b = FileLogBroker(d + "/log")
            p = Pipeline(st, "ws", b, PipelineConfig(window_s=30, allowed_lateness_s=20), default_detector_set()); pr = LocalProcessor(p, dq_interval_s=0)
            n = len(payloads) if split is None else split
            for i in range(0, n, 500):
                p.ingest(SRC, payloads[i:i + 500], now=T0 + 5000)
            pr.start(); self.assertTrue(pr.drain(30)); pr.stop()
            if split is not None:                                   # simulated crash: new objects over the same db + log
                b.close(); b = FileLogBroker(d + "/log")
                p = Pipeline(st, "ws", b, PipelineConfig(window_s=30, allowed_lateness_s=20), default_detector_set()); pr = LocalProcessor(p, dq_interval_s=0)
                for i in range(split, len(payloads), 500):
                    p.ingest(SRC, payloads[i:i + 500], now=T0 + 5000)
                pr.start(); self.assertTrue(pr.drain(30)); pr.stop()
            p.finish()
            return ({(r["window_start"], r["entity"]): r["metrics_json"] for r in st.q("select * from windows")}, st.q1("select count(*) c from events")["c"], p.agg.late_dropped)

        a, b = run(None), run(5000)
        self.assertEqual(a[0], b[0]); self.assertEqual(a[1], b[1]); self.assertEqual(b[2], 0)

    def test_late_event_goes_to_dead_letter(self):
        d, st, b, p = self.make()
        p.ingest(SRC, [raw_tx(1, T0), raw_tx(2, T0 + 200)], now=T0 + 300)
        p.ingest(SRC, [raw_tx(3, T0 + 1)], now=T0 + 301)
        proc = LocalProcessor(p, dq_interval_s=0); proc.start(); proc.drain(5); proc.stop()
        self.assertEqual(st.q1("select reason_code r from dead_letters where reason_code='late_event'")["r"], "late_event")

    def test_replay_scope_is_isolated_from_live(self):
        from veylstream.datasets import create_synthetic_dataset
        from veylstream.replay import ReplayService
        d = tmpdir(); st = Store(d + "/db")
        ds = create_synthetic_dataset(st, "ws", "t", d, seed=3, duration_s=1500, base_rate=15, scenarios=[Scenario("failure_spike", 900, 1200, "payments", "eu-west", 0.5)])
        rs = ReplayService(st); jid = rs.start("ws", ds["id"], {"speed": 0}); rs.wait(jid)
        job = st.q1("select * from replay_jobs where id=?", (jid,)); self.assertEqual(job["status"], "completed", job["error"])
        self.assertGreater(st.q1("select count(*) c from anomalies where scope=?", (jid,))["c"], 0)
        self.assertEqual(st.q1("select count(*) c from anomalies where scope='live'")["c"], 0)
        self.assertEqual(st.q1("select count(*) c from alerts")["c"], 0)          # replays never open live alerts
        self.assertEqual(json.loads(job["result_json"])["provenance"]["dataset_sha256"], ds["sha256"])


class Alerts(unittest.TestCase):
    def make(self, **pol):
        st = Store(); st.x("INSERT INTO workspaces VALUES('ws','w',0,'{}')")
        return st, AlertManager(st, None, None, AlertPolicy(**pol))

    def add(self, st, d):
        st.x("INSERT INTO anomalies(workspace_id,scope,dedup_key,group_key,detector,version,entity,metric,window_start,window_end,score,severity,direction,explanation,detail_json,detected_at) "
             "VALUES('ws','live',?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (d.dedup_key, d.group_key, d.detector, "1", d.entity, d.metric, d.window_start, d.window_end, 1.0, d.severity, d.direction, "x", "{}", 0))
        return d, st.q1("select id from anomalies where dedup_key=?", (d.dedup_key,))["id"]

    def test_streak_required_for_medium_but_not_for_high(self):
        st, am = self.make()
        self.assertEqual(am.handle("ws", [self.add(st, det(sev="medium", start=T0))]), [])
        self.assertEqual(len(am.handle("ws", [self.add(st, det(sev="medium", start=T0 + 30))])), 1)     # second consecutive window
        self.assertEqual(len(am.handle("ws", [self.add(st, det("service:q", sev="high"))])), 1)        # high opens immediately

    def test_streak_resets_after_gap_and_below_min_severity_ignored(self):
        st, am = self.make()
        am.handle("ws", [self.add(st, det(sev="medium", start=T0))])
        self.assertEqual(am.handle("ws", [self.add(st, det(sev="medium", start=T0 + 600))]), [])        # gap > 2 windows
        self.assertEqual(am.handle("ws", [self.add(st, det("service:z", sev="low"))]), [])

    def test_grouping_sustained_incident_is_one_alert_and_escalates(self):
        st, am = self.make()
        for i in range(10):
            am.handle("ws", [self.add(st, det(sev="high" if i < 5 else "critical", start=T0 + 30 * i, detector=["rule", "mad"][i % 2]))])
        a = st.q("select * from alerts"); self.assertEqual(len(a), 1)
        self.assertEqual((a[0]["severity"], a[0]["detection_count"]), ("critical", 10))
        self.assertIn("escalated", [r["action"] for r in st.q("select action from alert_audit")])

    def test_lifecycle_audit_cooldown_and_reopen(self):
        st, am = self.make(cooldown_s=300)
        am.handle("ws", [self.add(st, det(start=T0))]); aid = st.q1("select id from alerts")["id"]
        am.transition("ws", aid, "acknowledged", "bob"); am.transition("ws", aid, "investigating", "bob"); am.transition("ws", aid, "resolved", "bob", "fixed")
        with self.assertRaises(InvalidTransition):
            am.transition("ws", aid, "acknowledged", "bob")
        am.handle("ws", [self.add(st, det(start=T0 + 60))])                 # inside cooldown: tail of the same incident
        self.assertEqual(st.q1("select status s from alerts")["s"], "resolved")
        am.handle("ws", [self.add(st, det(start=T0 + 2000))])               # genuinely new incident
        self.assertEqual((st.q1("select status s from alerts")["s"], st.q1("select count(*) c from alerts")["c"]), ("open", 1))
        actions = [r["action"] for r in st.q("select action from alert_audit order by id")]
        self.assertEqual(actions, ["created", "status_change", "status_change", "status_change", "reopened"])
        with self.assertRaises(KeyError):
            am.transition("other-ws", aid, "resolved", "x")                 # workspace scoping

    def test_suppression_blocks_new_alerts(self):
        st, am = self.make()
        am.suppress("ws", "service:p", time.time() + 600, "bob", reason="maintenance")
        self.assertEqual(am.handle("ws", [self.add(st, det())]), [])
        self.assertEqual(st.q1("select count(*) c from alerts")["c"], 0)

    def test_assign_requires_membership(self):
        st, am = self.make(); am.handle("ws", [self.add(st, det())]); aid = st.q1("select id from alerts")["id"]
        with self.assertRaises(ValueError):
            am.assign("ws", aid, "nobody", "bob")


class Webhooks(unittest.TestCase):
    def serve(self, codes):
        got = []

        class H(http.server.BaseHTTPRequestHandler):
            def do_POST(s):
                n = int(s.headers["Content-Length"]); got.append(json.loads(s.rfile.read(n)))
                s.send_response(codes.pop(0) if codes else 200); s.end_headers()
            def log_message(s, *a): pass
        srv = http.server.HTTPServer(("127.0.0.1", 0), H); threading.Thread(target=srv.serve_forever, daemon=True).start()
        self.addCleanup(srv.shutdown)
        return srv, got

    def dest(self, st, url):
        st.x("INSERT INTO destinations(workspace_id,kind,name,config_json) VALUES('ws','webhook','h',?)", (json.dumps({"url": url}),))
        return st.q1("select * from destinations")

    def test_retries_transient_failures_then_succeeds_and_logs(self):
        st = Store(); srv, got = self.serve([503, 503, 200])
        n = Notifier(st, allow_private=True, backoff_s=0.01, async_delivery=False)
        r = n.deliver("ws", self.dest(st, f"http://127.0.0.1:{srv.server_port}/"), None, {"x": 1})
        self.assertEqual((r["status"], r["attempts"]), ("delivered", 3)); self.assertEqual(len(got), 3)
        self.assertEqual(st.q1("select status s from deliveries")["s"], "delivered")

    def test_permanent_4xx_not_retried_and_bounded_retries(self):
        st = Store(); srv, got = self.serve([400])
        n = Notifier(st, allow_private=True, backoff_s=0.01, async_delivery=False)
        self.assertEqual(n.deliver("ws", self.dest(st, f"http://127.0.0.1:{srv.server_port}/"), None, {})["attempts"], 1)
        st2 = Store(); srv2, got2 = self.serve([500] * 10)
        r = Notifier(st2, allow_private=True, backoff_s=0.01, max_attempts=3, async_delivery=False).deliver("ws", self.dest(st2, f"http://127.0.0.1:{srv2.server_port}/"), None, {})
        self.assertEqual((r["status"], r["attempts"]), ("failed", 3))

    def test_ssrf_guard_blocks_private_targets_by_default(self):
        st = Store(); n = Notifier(st, allow_private=False, async_delivery=False)
        r = n.deliver("ws", self.dest(st, "http://127.0.0.1:9/x"), None, {})
        self.assertEqual(r["status"], "failed"); self.assertIn("non-public", r["error"])
        self.assertIn("http(s)", n.deliver("ws", {"id": 1, "kind": "webhook", "config_json": json.dumps({"url": "file:///etc/passwd"})}, None, {})["error"])

    def test_email_without_smtp_is_reported_as_skipped(self):
        st = Store(); n = Notifier(st, async_delivery=False)
        r = n.deliver("ws", {"id": 1, "kind": "email", "config_json": json.dumps({"to": "a@b.c"})}, None, {})
        self.assertEqual(r["status"], "skipped")


if __name__ == "__main__":
    unittest.main()
