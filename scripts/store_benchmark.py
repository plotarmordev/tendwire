#!/usr/bin/env python3
"""Small public-API benchmark for the fresh Tendwire store."""

from __future__ import annotations

import argparse
import json
import statistics
import tempfile
import time
from pathlib import Path

from tendwire.core.models import BackendHealth, Snapshot, Worker
from tendwire.store.projection import latest_snapshot, save_snapshot
from tendwire.store.retention import RetentionPolicy, run_retention_cycle
from tendwire.store.schema import STORE_SCHEMA_VERSION, init_store


def _snapshot(index: int) -> Snapshot:
    observed_at = f"2026-08-05T00:00:{index % 60:02d}.000000Z"
    return Snapshot(
        host_id="benchmark-host",
        updated_at=observed_at,
        workers=[Worker(id="worker-a", name=f"worker-{index}", status="active")],
        backend_health=[
            BackendHealth(
                name="herdr",
                status="healthy",
                outcome="healthy_non_empty",
                observed_at=observed_at,
            )
        ],
    )


def _milliseconds(samples: list[int]) -> dict[str, float]:
    ordered = sorted(samples)
    return {
        "median": statistics.median(ordered) / 1_000_000,
        "p95": ordered[max(0, int(len(ordered) * 0.95) - 1)] / 1_000_000,
        "max": ordered[-1] / 1_000_000,
    }


def benchmark(db_path: Path, iterations: int) -> dict[str, object]:
    init_samples: list[int] = []
    save_samples: list[int] = []
    read_samples: list[int] = []
    for index in range(iterations):
        started = time.perf_counter_ns()
        init_store(db_path)
        init_samples.append(time.perf_counter_ns() - started)

        started = time.perf_counter_ns()
        persisted = save_snapshot(db_path, _snapshot(index))
        save_samples.append(time.perf_counter_ns() - started)
        if persisted.host_id != "benchmark-host":
            raise RuntimeError("snapshot_persistence_failed")

        started = time.perf_counter_ns()
        latest = latest_snapshot(db_path, "benchmark-host")
        read_samples.append(time.perf_counter_ns() - started)
        if latest is None:
            raise RuntimeError("snapshot_read_failed")

    retention = run_retention_cycle(
        db_path,
        policy=RetentionPolicy(batch_size=max(1, iterations)),
        now="2026-08-05T01:00:00.000000Z",
    )
    return {
        "schema_version": STORE_SCHEMA_VERSION,
        "iterations": iterations,
        "database_bytes": db_path.stat().st_size,
        "init_ms": _milliseconds(init_samples),
        "save_ms": _milliseconds(save_samples),
        "latest_ms": _milliseconds(read_samples),
        "retention": retention,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path)
    parser.add_argument("--iterations", type=int, default=100)
    args = parser.parse_args()
    if args.iterations < 1:
        parser.error("--iterations must be positive")
    if args.database is None:
        with tempfile.TemporaryDirectory(prefix="tendwire-store-benchmark-") as root:
            print(json.dumps(benchmark(Path(root) / "store.db", args.iterations), sort_keys=True))
    else:
        print(json.dumps(benchmark(args.database.resolve(), args.iterations), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
