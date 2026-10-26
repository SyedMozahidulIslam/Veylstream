"""Durable append-only log broker abstraction.

``FileLogBroker`` is the local, genuinely durable implementation (segmented JSONL; ``fsync`` before
acknowledging when ``fsync=True``). ``MemoryBroker`` is non-durable and used for tests / replay.
A Kafka-compatible adapter (Redpanda) would implement the same ``Broker`` interface; it is NOT
implemented or verified in this repository (see docs/LIMITATIONS.md).
"""
from __future__ import annotations

import json
import os
import threading
from abc import ABC, abstractmethod
from typing import Any


class Broker(ABC):
    durable: bool = False

    @abstractmethod
    def append(self, records: list[dict[str, Any]]) -> int:
        """Append records; return the offset of the first appended record. Returns only after the
        durability level of this broker has been satisfied."""

    @abstractmethod
    def read(self, from_offset: int, max_n: int) -> list[tuple[int, dict[str, Any]]]: ...

    @abstractmethod
    def end_offset(self) -> int:
        """Offset that the next appended record will receive."""

    def truncate_before(self, offset: int) -> int:
        return 0

    def close(self) -> None:
        pass


class MemoryBroker(Broker):
    durable = False

    def __init__(self) -> None:
        self._recs: list[dict[str, Any]] = []
        self._lock = threading.Lock()

    def append(self, records):
        with self._lock:
            first = len(self._recs)
            self._recs.extend(records)
            return first

    def read(self, from_offset, max_n):
        with self._lock:
            return [(i, self._recs[i]) for i in range(from_offset, min(len(self._recs), from_offset + max_n))]

    def end_offset(self):
        with self._lock:
            return len(self._recs)


class FileLogBroker(Broker):
    def __init__(self, directory: str, fsync: bool = True, segment_records: int = 50_000) -> None:
        self.dir = directory
        self.fsync = fsync
        self.durable = fsync
        self.segment_records = segment_records
        os.makedirs(directory, exist_ok=True)
        self._lock = threading.Lock()
        self._segs: list[list] = []          # [base_offset, path, count]
        for fn in sorted(os.listdir(directory)):
            if fn.startswith("seg-") and fn.endswith(".jsonl"):
                base = int(fn[4:-6])
                path = os.path.join(directory, fn)
                with open(path, "rb") as f:
                    data = f.read()
                good = data.rfind(b"\n") + 1            # drop a torn trailing write after a crash
                if good != len(data):
                    with open(path, "r+b") as f:
                        f.truncate(good)
                self._segs.append([base, path, data[:good].count(b"\n")])
        self._fh = None
        if not self._segs:
            self._new_segment(0)
        self._open_tail()

    def _new_segment(self, base: int) -> None:
        self._segs.append([base, os.path.join(self.dir, f"seg-{base:012d}.jsonl"), 0])

    def _open_tail(self) -> None:
        if self._fh:
            self._fh.close()
        self._fh = open(self._segs[-1][1], "ab")

    def end_offset(self):
        with self._lock:
            b, _, n = self._segs[-1]
            return b + n

    def append(self, records):
        if not records:
            return self.end_offset()
        with self._lock:
            first = self._segs[-1][0] + self._segs[-1][2]
            for r in records:
                seg = self._segs[-1]
                if seg[2] >= self.segment_records:
                    self._fh.flush()
                    if self.fsync:
                        os.fsync(self._fh.fileno())
                    self._new_segment(seg[0] + seg[2])
                    self._open_tail()
                    seg = self._segs[-1]
                self._fh.write(json.dumps(r, separators=(",", ":"), default=str).encode() + b"\n")
                seg[2] += 1
            self._fh.flush()
            if self.fsync:
                os.fsync(self._fh.fileno())
            return first

    def read(self, from_offset, max_n):
        with self._lock:
            segs = [list(s) for s in self._segs]
        out: list[tuple[int, dict[str, Any]]] = []
        for base, path, count in segs:
            if base + count <= from_offset or not os.path.exists(path):
                continue
            with open(path, "rb") as f:
                for i, line in enumerate(f):
                    off = base + i
                    if off < from_offset:
                        continue
                    if i >= count:
                        break
                    out.append((off, json.loads(line)))
                    if len(out) >= max_n:
                        return out
        return out

    def truncate_before(self, offset: int) -> int:
        """Delete whole segments that lie entirely before ``offset`` (already consumed and checkpointed)."""
        removed = 0
        with self._lock:
            while len(self._segs) > 1 and self._segs[0][0] + self._segs[0][2] <= offset:
                _, path, _ = self._segs.pop(0)
                try:
                    os.remove(path)
                    removed += 1
                except OSError:
                    pass
        return removed

    def close(self) -> None:
        with self._lock:
            if self._fh:
                self._fh.close()
                self._fh = None
