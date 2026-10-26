"""Opt-in end-to-end test in real time (~2-3 min): VEYLSTREAM_SLOW=1 python -m unittest discover -s tests -p test_e2e_slow.py"""
import os, time, unittest
from helpers import *
from veylstream.runtime import Runtime, Config
from veylstream.api import create_app


@unittest.skipUnless(os.environ.get("VEYLSTREAM_SLOW") == "1", "set VEYLSTREAM_SLOW=1 to run (takes minutes)")
class DemoJourney(unittest.TestCase):
    def test_seed_stream_spike_alert_ack_resolve_replay_experiment(self):
        rt = Runtime(Config(data_dir=tmpdir())); self.addCleanup(rt.close); c = create_app(rt).test_client()
        ws = rt.create_workspace("Demo"); rt.create_user("a@x.io", "correct-horse-1", "A", ws, "admin")
        H = {"Authorization": "Bearer " + c.post("/api/v1/auth/login", json={"email": "a@x.io", "password": "correct-horse-1"}).json["data"]["token"]}
        P = lambda p, **k: c.post(f"/api/v1/workspaces/{ws}{p}", headers=H, **k); G = lambda p: c.get(f"/api/v1/workspaces/{ws}{p}", headers=H)
        self.assertTrue(P("/demo/seed", json={"minutes": 30, "rate": 20}).json["data"]["drained"])
        self.assertEqual(G("/anomalies").json["meta"]["total"], 0, "clean seeded history must not raise anomalies")
        self.assertEqual(G("/alerts").json["meta"]["total"], 0)
        P("/demo/start", json={"rate": 20}); time.sleep(10)
        P("/demo/trigger", json={"kind": "failure_spike", "duration_s": 120})
        deadline, alert = time.time() + 200, None
        while time.time() < deadline and alert is None:
            rows = G("/alerts").json["data"]; alert = next((a for a in rows if a["metric"] == "txn_failure_rate"), None); time.sleep(2)
        self.assertIsNotNone(alert, "failure spike should open a txn_failure_rate alert")
        self.assertTrue(alert["simulated"]); self.assertIn(alert["entity"], ("service:payments", "region:eu-west"))
        d = G(f"/alerts/{alert['id']}").json["data"]; self.assertGreater(len(d["anomalies"]), 0)
        self.assertEqual(P(f"/alerts/{alert['id']}/status", json={"status": "acknowledged"}).status_code, 200)
        self.assertEqual(P(f"/alerts/{alert['id']}/status", json={"status": "resolved", "note": "e2e"}).status_code, 200)
        P("/demo/stop", json={})
        ds = P("/datasets/synthetic", json={"name": "e2e", "seed": 3, "duration_s": 2400, "base_rate": 15, "scenarios": [{"kind": "latency_degradation", "start": 1500, "end": 1800, "service": "checkout", "magnitude": 4}]}).json["data"]
        jid = P("/replays", json={"dataset_id": ds["id"], "speed": 0}).json["data"]["id"]; rt.replays.wait(jid)
        self.assertEqual(G(f"/replays/{jid}").json["data"]["status"], "completed")
        eid = P("/experiments", json={"dataset_id": ds["id"], "detectors": [{"name": "rule"}, {"name": "mad"}]}).json["data"]["id"]; rt.experiments.wait(eid)
        self.assertEqual(G(f"/experiments/{eid}").json["data"]["status"], "completed")


if __name__ == "__main__":
    unittest.main()
