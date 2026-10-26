import os, sys, tempfile
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from veylstream.contracts import Event

T0 = 1_790_000_000.0


def ev(i, ts, status="success", lat=100.0, svc="payments", reg="eu-west", etype="transaction", ent=None):
    return Event(event_id=str(i), event_type=etype, ts=ts, ingest_ts=ts, source_id="s", service=svc, region=reg, status=status,
                 latency_ms=lat, entity_id=ent if ent is not None else f"u{i % 3}")


def raw_tx(i, ts, status="success", lat=100.0, svc="payments", reg="eu-west"):
    from veylstream.contracts import iso
    return {"event_id": f"t{i}", "event_type": "transaction", "event_timestamp": iso(ts), "service_name": svc, "region": reg,
            "status": status, "amount": 10.0, "currency": "USD", "latency_ms": lat}


def tmpdir():
    return tempfile.mkdtemp(prefix="vs-test-")
