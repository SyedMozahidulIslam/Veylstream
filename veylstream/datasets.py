"""Historical dataset import (CSV / JSONL), validation, canonicalization and synthetic dataset creation.

Canonical dataset file = JSONL, one object per line: {"t": <arrival offset seconds>, "e": <raw event payload>, "label": 0|1|null}.
Parquet is supported only when ``pyarrow`` is installed; otherwise the import is rejected with a clear message.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
import time
from typing import Any, Iterator

from .contracts import iso, parse_ts, validate_event
from .generator import GeneratorConfig, Scenario, SyntheticGenerator
from .store import Store, new_id

NUMERIC = {"amount", "latency_ms", "http_status", "cpu_pct", "mem_pct", "request_count"}
MAX_IMPORT_BYTES = 200 * 1024 * 1024
_SAFE = re.compile(r"[^A-Za-z0-9_.-]")


def _clean(name: str) -> str:
    """Display-name sanitiser (the on-disk path is always server-generated, never derived from this)."""
    return _SAFE.sub("_", name).replace("..", "_")[:80] or "dataset"


def _coerce_row(row: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in row.items():
        if v is None or v == "":
            continue
        if isinstance(v, str) and k in NUMERIC:
            try:
                v = float(v)
            except ValueError:
                pass
        out[k] = v
    return out


def _truthy(v: Any) -> int | None:
    if v is None or v == "":
        return None
    return 1 if str(v).strip().lower() in ("1", "true", "yes", "y", "anomaly") else 0


def _read_rows(data: bytes, fmt: str) -> Iterator[dict[str, Any]]:
    if fmt == "jsonl":
        for line in data.decode("utf-8", "replace").splitlines():
            if line.strip():
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    obj = {"__bad_json__": line[:200]}
                yield obj if isinstance(obj, dict) else {"__bad_json__": line[:200]}
    elif fmt == "csv":
        for row in csv.DictReader(io.StringIO(data.decode("utf-8", "replace"))):
            yield _coerce_row(row)
    elif fmt == "parquet":
        try:
            import pyarrow.parquet as pq  # type: ignore
        except ImportError as e:
            raise ValueError("Parquet import needs the optional 'pyarrow' package, which is not installed here; use CSV or JSONL") from e
        for row in pq.read_table(io.BytesIO(data)).to_pylist():
            yield _coerce_row(row)
    else:
        raise ValueError("format must be csv, jsonl or parquet")


def import_dataset(store: Store, ws: str, name: str, data: bytes, fmt: str, data_dir: str, source_id: str = "import") -> dict[str, Any]:
    if len(data) > MAX_IMPORT_BYTES:
        raise ValueError("file too large")
    fmt = fmt.lower()
    sha = hashlib.sha256(data).hexdigest()
    rows: list[tuple[float, dict, int | None]] = []
    rejects: list[dict[str, Any]] = []
    n_rej = 0
    for i, raw in enumerate(_read_rows(data, fmt)):
        label = _truthy(raw.pop("is_anomaly", raw.pop("label", None)))
        if "__bad_json__" in raw:
            n_rej += 1
            rejects.append({"row": i + 1, "code": "invalid_json", "message": raw["__bad_json__"]}) if len(rejects) < 50 else None
            continue
        v = validate_event(raw, source_id=source_id, allow_future=True)
        if not v.ok:
            n_rej += 1
            if len(rejects) < 50:
                rejects.append({"row": i + 1, "code": v.problems[0].code, "field": v.problems[0].field, "message": v.problems[0].message})
            continue
        rows.append((v.event.ts, raw, label))  # type: ignore[union-attr]
    if not rows:
        raise ValueError(f"no valid rows; first problems: {rejects[:3]}")
    rows.sort(key=lambda r: r[0])
    t0 = rows[0][0]
    did = new_id("ds")
    os.makedirs(os.path.join(data_dir, "datasets"), exist_ok=True)
    path = os.path.join(data_dir, "datasets", f"{did}.jsonl")          # server-generated name: no user-controlled path
    with open(path, "w") as f:
        for ts, raw, label in rows:
            f.write(json.dumps({"t": round(ts - t0, 3), "e": raw, "label": label}, separators=(",", ":")) + "\n")
    has_labels = any(r[2] is not None for r in rows)
    store.x("INSERT INTO datasets(id,workspace_id,name,path,format,rows_valid,rows_rejected,sha256,ts_min,ts_max,has_labels,created_at,rejects_json,meta_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (did, ws, _clean(name), path, fmt, len(rows), n_rej, sha, rows[0][0], rows[-1][0], int(has_labels), time.time(),
             json.dumps(rejects), json.dumps({"origin": "import", "ordering": "event-time"})))
    return store.q1("SELECT * FROM datasets WHERE id=?", (did,))  # type: ignore[return-value]


def create_synthetic_dataset(store: Store, ws: str, name: str, data_dir: str, seed: int = 42, duration_s: float = 3600, base_rate: float = 20.0,
                             scenarios: list[Scenario] | None = None, t_start: float = 1_790_000_000.0, day_length_s: float | None = None) -> dict[str, Any]:
    """Generate a deterministic labeled synthetic dataset (SIMULATED DATA). Scenario times are offsets from the dataset start."""
    gen = SyntheticGenerator(GeneratorConfig(seed=seed, base_rate=base_rate, day_length_s=day_length_s))
    for s in scenarios or []:
        gen.add_scenario(Scenario(s.kind, t_start + s.start, t_start + s.end, s.service, s.region, s.magnitude, s.label))
    raw = gen.generate(t_start, t_start + duration_s)
    did = new_id("ds")
    os.makedirs(os.path.join(data_dir, "datasets"), exist_ok=True)
    path = os.path.join(data_dir, "datasets", f"{did}.jsonl")
    h = hashlib.sha256()
    with open(path, "w") as f:
        for arrival, src, payload in raw:
            line = json.dumps({"t": round(arrival - t_start, 3), "e": payload, "src": src, "label": None}, separators=(",", ":")) + "\n"
            f.write(line)
            h.update(line.encode())
    gt = gen.ground_truth()
    ts = [parse_ts(p.get("event_timestamp")) for _, _, p in raw if isinstance(p, dict) and parse_ts(p.get("event_timestamp"))]
    meta = {"origin": "synthetic", "simulated": True, "seed": seed, "base_rate": base_rate, "duration_s": duration_s, "day_length_s": day_length_s,
            "ground_truth": gt, "generator": "veylstream.generator"}
    store.x("INSERT INTO datasets(id,workspace_id,name,path,format,rows_valid,rows_rejected,sha256,ts_min,ts_max,has_labels,created_at,rejects_json,meta_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (did, ws, _clean(name), path, "jsonl", len(raw), 0, h.hexdigest(), min(ts), max(ts), int(bool(gt)), time.time(), "[]", json.dumps(meta)))
    return store.q1("SELECT * FROM datasets WHERE id=?", (did,))  # type: ignore[return-value]


def iter_dataset(path: str) -> Iterator[dict[str, Any]]:
    with open(path) as f:
        for line in f:
            if line.strip():
                yield json.loads(line)
