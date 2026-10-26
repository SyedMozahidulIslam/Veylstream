import json, os, threading, time, unittest
from helpers import *
from veylstream.runtime import Runtime, Config
from veylstream.api import create_app
from veylstream.sdk import Producer, transaction_event, telemetry_event
from veylstream.datasets import import_dataset, create_synthetic_dataset
from veylstream.generator import Scenario
from veylstream.experiments import confusion, run_experiment, to_csv, research_summary


class ApiBase(unittest.TestCase):
    def setUp(self):
        self.rt = Runtime(Config(data_dir=tmpdir(), allow_private_webhooks=True)); self.app = create_app(self.rt); self.c = self.app.test_client()
        self.ws = self.rt.create_workspace("A"); self.ws2 = self.rt.create_workspace("B")
        for email, role in (("admin@x.io", "admin"), ("op@x.io", "operator"), ("an@x.io", "analyst"), ("vw@x.io", "viewer")):
            self.rt.create_user(email, "correct-horse-9", role, self.ws, role)
        self.rt.create_user("other@x.io", "correct-horse-9", "o", self.ws2, "admin")
        self.tok = {r: self.login(f"{e}@x.io") for r, e in (("admin", "admin"), ("operator", "op"), ("analyst", "an"), ("viewer", "vw"), ("other", "other"))}
        self.addCleanup(self.rt.close)

    def login(self, email):
        return self.c.post("/api/v1/auth/login", json={"email": email, "password": "correct-horse-9"}).json["data"]["token"]

    def h(self, role="admin"):
        return {"Authorization": "Bearer " + self.tok[role]}

    def get(self, path, role="admin", ws=None, **kw):
        return self.c.get(f"/api/v1/workspaces/{ws or self.ws}{path}", headers=self.h(role), **kw)

    def post(self, path, role="admin", ws=None, **kw):
        return self.c.post(f"/api/v1/workspaces/{ws or self.ws}{path}", headers=self.h(role), **kw)

    def source(self):
        return self.post("/sources", json={"name": "s", "heartbeat_interval_s": 10}).json["data"]["api_key"]


class Security(ApiBase):
    def test_auth_required_and_login_failures(self):
        self.assertEqual(self.c.get(f"/api/v1/workspaces/{self.ws}/overview").status_code, 401)
        self.assertEqual(self.c.get(f"/api/v1/workspaces/{self.ws}/overview", headers={"Authorization": "Bearer nope"}).status_code, 401)
        r = self.c.post("/api/v1/auth/login", json={"email": "admin@x.io", "password": "bad"}); self.assertEqual(r.status_code, 401)
        self.assertEqual(self.c.post("/api/v1/auth/login", json={"email": "ghost@x.io", "password": "bad"}).json["error"]["code"], "invalid_credentials")

    def test_login_rate_limit(self):
        codes = [self.c.post("/api/v1/auth/login", json={"email": "admin@x.io", "password": "bad"}).status_code for _ in range(12)]
        self.assertIn(429, codes)

    def test_logout_invalidates_token(self):
        self.c.post("/api/v1/auth/logout", headers=self.h("viewer"))
        self.assertEqual(self.get("/overview", "viewer").status_code, 401)

    def test_workspace_isolation(self):
        self.assertEqual(self.get("/alerts", "other").status_code, 404)                       # not a member: 404, not 403
        self.assertEqual(self.get("/overview", "admin", ws=self.ws2).status_code, 404)
        self.assertEqual(self.get("/events/anything", "other").status_code, 404)
        # an alert id from workspace A cannot be touched through workspace B
        self.rt.store.x("INSERT INTO alerts(workspace_id,fingerprint,title,severity,status,entity,metric,first_seen,last_seen,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                        (self.ws, "f", "t", "high", "open", "e", "m", 0, 0, 0, 0))
        aid = self.rt.store.q1("select id from alerts")["id"]
        self.assertEqual(self.get(f"/alerts/{aid}", "other", ws=self.ws2).status_code, 404)
        self.assertEqual(self.post(f"/alerts/{aid}/status", "other", ws=self.ws2, json={"status": "resolved"}).status_code, 404)

    def test_role_matrix(self):
        self.assertEqual(self.post("/sources", "viewer", json={"name": "x"}).status_code, 403)
        self.assertEqual(self.post("/sources", "operator", json={"name": "x"}).status_code, 403)
        self.assertEqual(self.post("/datasets/synthetic", "viewer", json={}).status_code, 403)
        self.assertEqual(self.post("/demo/start", "analyst", json={}).status_code, 403)
        self.assertEqual(self.post("/demo/reset", "operator", json={}).status_code, 403)
        self.assertEqual(self.c.patch(f"/api/v1/workspaces/{self.ws}/settings", headers=self.h("operator"), json={"retention_days": 5}).status_code, 403)
        self.assertEqual(self.get("/audit", "operator").status_code, 403)
        self.assertEqual(self.get("/dead-letters", "viewer").status_code, 403)
        self.assertEqual(self.get("/overview", "viewer").status_code, 200)

    def test_api_key_never_returned_after_creation_and_rotation_revocation(self):
        r = self.post("/sources", json={"name": "s"}).json["data"]; sid, key = r["id"], r["api_key"]
        listing = json.dumps(self.get("/sources").json)
        self.assertNotIn(key, listing); self.assertNotIn("key_hash", listing)
        row = self.rt.store.q1("select key_hash from sources where id=?", (sid,)); self.assertNotEqual(row["key_hash"], key); self.assertEqual(len(row["key_hash"]), 64)
        ev = {"event_id": "a", "event_type": "telemetry", "event_timestamp": time.time(), "service_name": "s", "region": "r", "http_status": 200}
        ok = lambda k: self.c.post("/api/v1/ingest/events", json=ev, headers={"X-API-Key": k}).status_code
        self.assertEqual(ok(key), 202)
        new = self.post(f"/sources/{sid}/rotate").json["data"]["api_key"]
        self.assertEqual(ok(key), 401); self.assertEqual(ok(new), 202)
        self.post(f"/sources/{sid}/revoke"); self.assertEqual(ok(new), 401)

    def test_webhook_destination_ssrf_blocked_when_private_not_allowed(self):
        self.rt.cfg.allow_private_webhooks = False
        r = self.post("/destinations", json={"kind": "webhook", "name": "x", "config": {"url": "http://127.0.0.1:1/h"}})
        self.assertEqual((r.status_code, r.json["error"]["code"]), (400, "unsafe_webhook"))

    def test_validation_and_error_shapes(self):
        r = self.get("/anomalies?sort=; drop table", "viewer"); self.assertEqual((r.status_code, r.json["error"]["code"]), (400, "invalid_parameter"))
        self.assertEqual(self.get("/events?from=notatime").status_code, 400)
        r = self.c.patch(f"/api/v1/workspaces/{self.ws}/settings", headers=self.h(), json={"retention_days": 0}); self.assertEqual(r.status_code, 400)
        r = self.c.patch(f"/api/v1/workspaces/{self.ws}/settings", headers=self.h(), json={"demo": False}); self.assertEqual(r.status_code, 400)   # read-only key
        self.assertIn("request_id", self.get("/nope", "admin").json)
        self.assertEqual(self.c.get("/api/v1/does-not-exist").status_code, 404)
        h = self.c.get("/api/v1/health/live"); self.assertEqual(h.headers["X-Content-Type-Options"], "nosniff")

    def test_sql_injection_in_filters_is_inert(self):
        r = self.get("/events?service=x'%20OR%201=1--", "viewer"); self.assertEqual(r.status_code, 200); self.assertEqual(r.json["meta"]["total"], 0)
        r = self.get("/anomalies?q=%25'%20;DROP%20TABLE%20users;--", "viewer"); self.assertEqual(r.status_code, 200)
        self.assertEqual(self.rt.store.q1("select count(*) c from users")["c"], 5)

    def test_payload_limits(self):
        key = self.source()
        self.assertEqual(self.c.post("/api/v1/ingest/batch", json={"events": []}, headers={"X-API-Key": key}).status_code, 400)
        big = {"events": [{"event_id": str(i)} for i in range(1001)]}
        self.assertEqual(self.c.post("/api/v1/ingest/batch", json=big, headers={"X-API-Key": key}).status_code, 413)
        self.rt.cfg.max_body_bytes = 10; self.app.config["MAX_CONTENT_LENGTH"] = 10
        self.assertEqual(self.c.post("/api/v1/ingest/batch", json={"events": [{"a": "x" * 100}]}, headers={"X-API-Key": key}).status_code, 413)

    def test_dataset_import_uses_server_generated_paths(self):
        r = self.c.post(f"/api/v1/workspaces/{self.ws}/datasets/import?format=jsonl&name=../../etc/passwd", data=b'{"x":1}\n', headers=self.h("analyst"))
        self.assertEqual(r.status_code, 422)                       # nothing valid in it
        good = (json.dumps(raw_tx(1, T0)) + "\n").encode()
        r = self.c.post(f"/api/v1/workspaces/{self.ws}/datasets/import?format=jsonl&name=../../evil", data=good, headers=self.h("analyst"))
        self.assertEqual(r.status_code, 201); self.assertNotIn("..", r.json["data"]["name"]); self.assertNotIn("path", r.json["data"])
        self.assertTrue(all(".." not in f for f in os.listdir(self.rt.cfg.data_dir)))


class IngestAndQueries(ApiBase):
    def test_ingest_paths_and_status_codes(self):
        key = self.source(); H = {"X-API-Key": key}
        r = self.c.post("/api/v1/ingest/events", json=raw_tx(1, time.time()), headers=H); self.assertEqual(r.status_code, 202); self.assertTrue(r.json["data"]["durable"])
        r = self.c.post("/api/v1/ingest/events", json={"nope": 1}, headers=H); self.assertEqual(r.status_code, 422); self.assertEqual(r.json["data"]["rejected"][0]["code"], "missing_field")
        r = self.c.post("/api/v1/ingest/batch", json={"events": [raw_tx(2, time.time()), {"bad": 1}, raw_tx(2, time.time())]}, headers=H)
        self.assertEqual(r.status_code, 207); self.assertEqual((r.json["data"]["accepted"], r.json["data"]["duplicates"], r.json["data"]["rejected_count"]), (1, 1, 1))
        self.assertEqual(self.c.post("/api/v1/ingest/heartbeat", json={}, headers=H).status_code, 202)
        self.assertEqual(self.rt.store.q1("select last_seen_wall w from sources")["w"] is not None, True)
        self.rt.processor(self.ws).drain(5)
        ev = self.get("/events?sort=-time", "viewer").json; self.assertEqual(ev["meta"]["total"], 2)
        detail = self.get("/events/t1", "viewer").json["data"]["event"]; self.assertTrue(detail["entity_id"] == "" or "***" in detail["entity_id"] or detail["entity_id"] is not None)
        self.assertEqual(self.get("/dead-letters", "analyst").json["meta"]["by_reason"]["missing_field"], 2)

    def test_overview_pipeline_and_prometheus_reflect_real_state(self):
        key = self.source()
        self.c.post("/api/v1/ingest/batch", json={"events": [raw_tx(i, time.time()) for i in range(20)]}, headers={"X-API-Key": key}); self.rt.processor(self.ws).drain(5)
        o = self.get("/overview", "viewer").json["data"]; self.assertEqual(o["total_events"], 20); self.assertEqual(o["mode"], "local")
        comps = {c["name"]: c for c in self.get("/pipeline/status", "viewer").json["data"]["components"]}
        self.assertEqual(comps["processor"]["status"], "ok"); self.assertFalse(comps["processor"]["distributed"])
        self.post("/streams/processor/pause", "operator")
        self.assertEqual({c["name"]: c for c in self.get("/pipeline/status", "viewer").json["data"]["components"]}["processor"]["status"], "degraded")
        self.post("/streams/processor/resume", "operator")
        self.assertIn("veylstream_accepted_total 20", self.get("/metrics", "viewer").data.decode())

    def test_settings_persist_and_apply(self):
        r = self.c.patch(f"/api/v1/workspaces/{self.ws}/settings", headers=self.h(), json={"retention_days": 9, "window_s": 15, "detectors": [{"name": "mad"}], "alert_policy": {"min_severity": "high"}})
        self.assertEqual(r.status_code, 200); self.assertTrue(r.json["data"]["restarted_pipeline"])
        p = self.rt.pipeline(self.ws); self.assertEqual((p.cfg.window_s, [d.name for d in p.detectors]), (15.0, ["mad"]))
        self.assertEqual(self.rt.alerts(self.ws).policy.min_severity, "high")
        self.assertEqual(self.get("/settings", "viewer").json["data"]["retention_days"], 9)
        bad = self.c.patch(f"/api/v1/workspaces/{self.ws}/settings", headers=self.h(), json={"detectors": [{"name": "nonsense"}]}); self.assertEqual(bad.status_code, 400)

    def test_live_stream_sse_delivers_published_messages(self):
        self.rt._bundle(self.ws)
        r = self.get("/live", "viewer", buffered=False)
        it = iter(r.response); first = next(it).decode(); self.assertIn("retry:", first)
        self.assertIn('"hello"', next(it).decode())
        self.rt.bus.publish(self.ws, "alert", {"id": 1})
        self.assertIn('"alert"', next(it).decode()); r.close()
        time.sleep(1.2); self.assertEqual(self.rt.bus.subscriber_count(), 0)       # cleaned up on disconnect

    def test_webhook_receives_alert_and_test_endpoint_reports_real_result(self):
        import http.server
        got = []
        class H(http.server.BaseHTTPRequestHandler):
            def do_POST(s): got.append(json.loads(s.rfile.read(int(s.headers["Content-Length"])))); s.send_response(204); s.end_headers()
            def log_message(s, *a): pass
        srv = http.server.HTTPServer(("127.0.0.1", 0), H); threading.Thread(target=srv.serve_forever, daemon=True).start(); self.addCleanup(srv.shutdown)
        d = self.post("/destinations", json={"kind": "webhook", "name": "h", "min_severity": "medium", "config": {"url": f"http://127.0.0.1:{srv.server_port}/"}}).json["data"]["id"]
        t = self.post(f"/destinations/{d}/test").json["data"]; self.assertEqual((t["status"], t["http_status"]), ("delivered", 204))
        srv.shutdown(); srv.server_close()
        self.assertEqual(self.post(f"/destinations/{d}/test").json["data"]["status"], "failed")         # dead endpoint is reported as failed, not faked


class DemoReset(ApiBase):
    def test_reset_only_touches_demo_workspaces_without_real_events(self):
        self.assertEqual(self.post("/demo/reset").status_code, 409)                         # not flagged as a demo workspace
        self.assertTrue(self.post("/demo/seed", "operator", json={"minutes": 2, "rate": 10}).json["data"]["drained"])
        n = lambda t: self.rt.store.q1(f"select count(*) c from {t}")["c"]
        self.assertGreater(n("events"), 0); self.assertGreater(n("windows"), 0)
        self.assertEqual(self.post("/demo/reset", "operator").status_code, 403)             # admin only
        self.assertEqual(self.post("/demo/reset").status_code, 200)
        self.assertEqual((n("events"), n("windows"), n("sources") > 0), (0, 0, True))       # data gone, credentials kept
        self.assertTrue(self.post("/demo/seed", "operator", json={"minutes": 2, "rate": 10}).json["data"]["drained"])
        self.rt.store.x("INSERT INTO events(workspace_id,scope,event_id,event_type,ts,ingest_ts,source_id,simulated,payload_json) VALUES(?,?,?,?,?,?,?,?,?)", (self.ws, "live", "real", "telemetry", 1, 1, "s", 0, "{}"))
        self.assertEqual(self.post("/demo/reset").status_code, 409)                         # refuses when real events exist
        self.assertGreater(n("events"), 1)


class ReplayExperiments(ApiBase):
    def test_csv_import_validates_and_reports_rejects(self):
        csv = "event_id,event_type,event_timestamp,service_name,region,status,amount,currency,latency_ms\n"
        rows = [f"e{i},transaction,2026-10-25T10:00:{i:02d}Z,payments,eu,success,10,USD,{100+i}" for i in range(10)] + ["bad,transaction,not-a-time,payments,eu,success,10,USD,5", "x,transaction,2026-10-25T10:01:00Z,payments,eu,exploded,10,USD,5"]
        ds = import_dataset(self.rt.store, self.ws, "c", (csv + "\n".join(rows)).encode(), "csv", self.rt.cfg.data_dir)
        self.assertEqual((ds["rows_valid"], ds["rows_rejected"]), (10, 2))
        self.assertEqual({r["code"] for r in json.loads(ds["rejects_json"])}, {"invalid_timestamp", "invalid_value"})
        with self.assertRaises(ValueError): import_dataset(self.rt.store, self.ws, "p", b"x", "parquet", self.rt.cfg.data_dir)      # pyarrow absent: explicit message
        with self.assertRaises(ValueError): import_dataset(self.rt.store, self.ws, "e", b"", "jsonl", self.rt.cfg.data_dir)

    def test_confusion_metrics_exact_values(self):
        c = confusion([True, True, False, False, True, False], [True, False, False, True, True, False])
        self.assertEqual((c["tp"], c["fp"], c["fn"], c["tn"]), (2, 1, 1, 2))
        self.assertAlmostEqual(c["precision"], 2 / 3); self.assertAlmostEqual(c["recall"], 2 / 3); self.assertAlmostEqual(c["f1"], 2 / 3); self.assertAlmostEqual(c["fpr"], 1 / 3)
        self.assertEqual(c["confusion_matrix"], [[2, 1], [1, 2]])
        e = confusion([False, False], [False, False]); self.assertIsNone(e["precision"]); self.assertIsNone(e["recall"])      # undefined, not zero

    def test_experiment_labeled_unlabeled_reproducible_and_exports(self):
        st = self.rt.store
        sc = [Scenario("failure_spike", 1500, 1800, "payments", "eu-west", 0.5)]
        ds = create_synthetic_dataset(st, self.ws, "lab", self.rt.cfg.data_dir, seed=4, duration_s=2400, base_rate=20, scenarios=sc)
        specs = [{"name": "rule"}, {"name": "mad"}]
        e1 = run_experiment(st, self.ws, {"dataset_id": ds["id"], "detectors": specs}); e2 = run_experiment(st, self.ws, {"dataset_id": ds["id"], "detectors": specs})
        r1, r2 = [json.loads(st.q1("select results_json r from experiments where id=?", (e,))["r"]) for e in (e1, e2)]
        self.assertEqual(r1["evaluation"]["detectors"], r2["evaluation"]["detectors"])        # deterministic
        rule = r1["evaluation"]["detectors"]["rule"]
        self.assertTrue(r1["evaluation"]["labels_available"]); self.assertGreaterEqual(rule["recall"], 0.5); self.assertEqual(rule["incidents_detected"], 1)
        rp = r1["reproducibility"]; self.assertEqual(rp["experiment_id"], e1); self.assertEqual(r1["dataset"]["sha256"], ds["sha256"])
        # unlabeled dataset => supervised metrics are n/a, not fabricated
        ds2 = create_synthetic_dataset(st, self.ws, "clean", self.rt.cfg.data_dir, seed=4, duration_s=1800, base_rate=20)
        r3 = json.loads(st.q1("select results_json r from experiments where id=?", (run_experiment(st, self.ws, {"dataset_id": ds2["id"], "detectors": specs}),))["r"])
        self.assertFalse(r3["evaluation"]["labels_available"]); self.assertIsNone(r3["evaluation"]["detectors"]["rule"]["f1"]); self.assertIn("unavailable", r3["evaluation"]["detectors"]["rule"]["note"])
        # api exports
        self.assertIn("detector,detections_total", self.get(f"/experiments/{e1}/export?format=csv", "viewer").data.decode())
        self.assertIn("## Known limitations", self.get(f"/experiments/{e1}/export?format=md", "viewer").data.decode())
        self.assertEqual(json.loads(self.get(f"/experiments/{e1}/export?format=json", "viewer").data)["id"], e1)
        self.assertEqual(self.get(f"/experiments/{e1}", "other", ws=self.ws2).status_code, 404)       # other workspace cannot read it
        self.assertEqual(self.get(f"/experiments/{e1}", "viewer").status_code, 200)

    def test_experiment_cross_workspace_dataset_rejected(self):
        ds = create_synthetic_dataset(self.rt.store, self.ws, "d", self.rt.cfg.data_dir, seed=1, duration_s=300, base_rate=5)
        r = self.c.post(f"/api/v1/workspaces/{self.ws2}/experiments", headers=self.h("other"), json={"dataset_id": ds["id"]})
        self.assertEqual(r.status_code, 404)


class Sdk(unittest.TestCase):
    def serve(self, plan):
        import http.server
        calls = []
        class H(http.server.BaseHTTPRequestHandler):
            def do_POST(s):
                calls.append(json.loads(s.rfile.read(int(s.headers["Content-Length"]))))
                code, body = plan.pop(0) if plan else (202, {"data": {"accepted": 1}})
                s.send_response(code); s.send_header("Content-Type", "application/json")
                if code == 429: s.send_header("Retry-After", "0")
                s.end_headers(); s.wfile.write(json.dumps(body).encode())
            def log_message(s, *a): pass
        srv = http.server.HTTPServer(("127.0.0.1", 0), H); threading.Thread(target=srv.serve_forever, daemon=True).start(); self.addCleanup(srv.shutdown)
        return f"http://127.0.0.1:{srv.server_port}", calls

    def test_retries_transient_but_not_permanent(self):
        url, calls = self.serve([(503, {}), (429, {}), (202, {"data": {"accepted": 1, "durable": True}})])
        r = Producer(url, "k", backoff_base_s=0.01).send(transaction_event("p", "r", "success", 1.0))
        self.assertEqual((r.accepted, r.retries, r.failed, r.ok), (1, 2, 0, True)); self.assertEqual(len(calls), 3)
        url, calls = self.serve([(401, {"error": {"message": "bad key"}})])
        r = Producer(url, "k", backoff_base_s=0.01).send(telemetry_event("p", "r", 200))
        self.assertEqual((len(calls), r.failed, r.ok), (1, 1, False)); self.assertIn("bad key", r.error)

    def test_gives_up_after_max_retries_and_reports_network_errors(self):
        url, calls = self.serve([(500, {})] * 10)
        r = Producer(url, "k", max_retries=2, backoff_base_s=0.01).send(telemetry_event("p", "r", 200)); self.assertEqual((len(calls), r.failed, r.retries), (3, 1, 2))
        r = Producer("http://127.0.0.1:1", "k", max_retries=1, backoff_base_s=0.01, timeout_s=0.5).send(telemetry_event("p", "r", 200)); self.assertEqual(r.failed, 1); self.assertIn("network", r.error)

    def test_batch_chunking_and_rejections_passthrough(self):
        url, calls = self.serve([(207, {"data": {"accepted": 2, "rejected": [{"index": 1, "code": "x"}]}}), (202, {"data": {"accepted": 1}})])
        r = Producer(url, "k", batch_size=2).send_batch([transaction_event("p", "r", "success", 1.0) for _ in range(3)])
        self.assertEqual((len(calls), r.accepted, len(r.rejected), r.ok), (2, 3, 1, False))


if __name__ == "__main__":
    unittest.main()
