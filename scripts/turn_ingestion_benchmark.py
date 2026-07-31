#!/usr/bin/env python3
"""Deterministic synthetic benchmark for background turn ingestion.

Run from a source checkout with ``PYTHONPATH=src``. The benchmark creates only
private temporary fixtures, exercises a real Unix-domain socket daemon, and
prints one aggregate JSON object. Documented-host latency budgets are evidence
gates for this benchmark run, not generic service-level guarantees.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import platform
import sqlite3
import stat
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from time import perf_counter_ns
from typing import Any

from tendwire.backends.herdr_turns import TurnIngestionScheduler
from tendwire.config import Config
from tendwire.core.commands import (
    DISPOSITION_NO_RECEIPT,
    STATUS_NOOP,
    CommandEnvelope,
    CommandRequest,
)
from tendwire.core.models import BackendHealth, Snapshot, Worker, WorkerBinding
from tendwire.core.turns import recompute_pending_content_fingerprint
from tendwire.daemon import DaemonHooks, TendwireDaemon
from tendwire.daemon_api import (
    MAX_REQUEST_BYTES,
    MAX_RESPONSE_BYTES,
    DaemonAPIClient,
)
from tendwire.store import sqlite as store

REPORT_SCHEMA_VERSION = 1
FIXTURE_HOST = "synthetic-turn-benchmark-host"
FIXTURE_TIMESTAMP = "2026-07-01T00:00:00+00:00"
SCHEDULER_REFRESH_SECONDS = 2.0
SCHEDULER_WORKERS = 4
SCHEDULER_QUEUE_CAPACITY = 64
API_REQUEST_WORKERS = 8
API_ADMISSION_CAPACITY = 32
LIST_BUDGET_NS = 350_000_000
HEALTH_BUDGET_NS = 350_000_000
COMMAND_BUDGET_NS = 250_000_000
SHUTDOWN_BOUND_NS = 2_000_000_000
POLL_SECONDS = 0.005


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _nearest_rank(samples: list[int], percentile: float) -> int:
    if not samples:
        raise ValueError("samples_required")
    ordered = sorted(samples)
    rank = max(1, math.ceil(percentile * len(ordered)))
    return ordered[rank - 1]


def _metric(
    samples: list[int],
    *,
    warmups: int,
    response_bytes: list[int],
    budget_ns: int,
) -> dict[str, Any]:
    p95_ns = _nearest_rank(samples, 0.95)
    return {
        "samples": len(samples),
        "warmups": warmups,
        "min_ns": min(samples),
        "p50_ns": _nearest_rank(samples, 0.50),
        "p95_ns": p95_ns,
        "max_ns": max(samples),
        "response_bytes_max": max(response_bytes),
        "documented_host_budget_ns": budget_ns,
        "documented_host_budget_met": p95_ns <= budget_ns,
    }


def _wait_until(predicate: Callable[[], bool], timeout_seconds: float, code: str) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(POLL_SECONDS)
    if not predicate():
        raise RuntimeError(code)


def _thread_ids(prefixes: tuple[str, ...]) -> set[int]:
    return {
        int(thread.ident)
        for thread in threading.enumerate()
        if thread.ident is not None and thread.name.startswith(prefixes)
    }


def _process_alive(process_id: int) -> bool:
    try:
        os.kill(process_id, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _marker_records(marker_dir: Path) -> list[dict[str, int]]:
    records: list[dict[str, int]] = []
    for marker in marker_dir.glob("done-*.json"):
        try:
            value = json.loads(marker.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            continue
        if not isinstance(value, Mapping):
            continue
        try:
            ordinal = int(value["ordinal"])
            process_id = int(value["process_id"])
            started_ns = int(value["started_ns"])
            finished_ns = int(value["finished_ns"])
        except (KeyError, TypeError, ValueError):
            continue
        if ordinal < 0 or process_id <= 0 or finished_ns < started_ns:
            continue
        records.append(
            {
                "ordinal": ordinal,
                "process_id": process_id,
                "started_ns": started_ns,
                "finished_ns": finished_ns,
            }
        )
    return records


def _active_markers(marker_dir: Path) -> list[Path]:
    return list(marker_dir.glob("active-*.json"))


def _source_call_count(marker_dir: Path) -> int:
    return len(_active_markers(marker_dir)) + len(_marker_records(marker_dir))


def _interval_maximum(records: list[dict[str, int]]) -> int:
    events: list[tuple[int, int]] = []
    for record in records:
        events.append((record["started_ns"], 1))
        events.append((record["finished_ns"], -1))
    active = 0
    maximum = 0
    for _at, delta in sorted(events, key=lambda item: (item[0], item[1])):
        active += delta
        maximum = max(maximum, active)
    return maximum


def _first_call_overlap_ns(
    records: list[dict[str, int]],
    blocked_workers: int,
) -> int:
    first: list[dict[str, int]] = []
    for ordinal in range(blocked_workers):
        matching = sorted(
            (record for record in records if record["ordinal"] == ordinal),
            key=lambda record: record["started_ns"],
        )
        if not matching:
            return 0
        first.append(matching[0])
    return max(
        0,
        min(record["finished_ns"] for record in first)
        - max(record["started_ns"] for record in first),
    )


def _write_adapter(
    adapter_path: Path,
    marker_dir: Path,
    release_path: Path,
    state_path: Path,
) -> None:
    source = f'''#!/usr/bin/env python3
import json
import os
import pathlib
import sys
import time

marker_dir = pathlib.Path({str(marker_dir)!r})
release_path = pathlib.Path({str(release_path)!r})
state_path = pathlib.Path({str(state_path)!r})
target = sys.argv[3] if len(sys.argv) > 3 else ""
try:
    ordinal = int(target.rsplit("-", 1)[-1])
except ValueError:
    raise SystemExit(2)
process_id = os.getpid()
started_ns = time.monotonic_ns()
active = marker_dir / f"active-{{ordinal}}-{{process_id}}.json"
done = marker_dir / f"done-{{ordinal}}-{{process_id}}.json"
active.write_text(json.dumps({{"ordinal": ordinal, "process_id": process_id, "started_ns": started_ns}}, sort_keys=True), encoding="utf-8")
os.chmod(active, 0o600)
with open(release_path, "rb", buffering=0) as release:
    if release.read(1) != b"R":
        raise SystemExit(3)
finished_ns = time.monotonic_ns()
done.write_text(json.dumps({{"ordinal": ordinal, "process_id": process_id, "started_ns": started_ns, "finished_ns": finished_ns}}, sort_keys=True), encoding="utf-8")
os.chmod(done, 0o600)
try:
    active.unlink()
except FileNotFoundError:
    pass
turn = {{"available": True, "user_text": "generated request", "assistant_final_text": "generated response", "complete": True, "has_open_turn": False, "model": "synthetic"}}
try:
    pending_state = state_path.read_text(encoding="utf-8").strip()
except OSError:
    pending_state = "none"
if pending_state == "open":
    turn["pending_decision"] = {{
        "id": "synthetic-private-decision",
        "prompt": "generated pending prompt",
        "options": [
            {{"id": "approve", "label": "Approve generated choice"}},
            {{"id": "reject", "label": "Reject generated choice"}},
        ],
    }}
print(json.dumps({{"result": {{"turn": turn}}}}, sort_keys=True, separators=(",", ":")))
'''
    adapter_path.write_text(source, encoding="utf-8")
    adapter_path.chmod(0o700)


def _fixture(
    blocked_workers: int,
) -> tuple[list[Worker], list[WorkerBinding], dict[str, Any]]:
    workers: list[Worker] = []
    bindings: list[WorkerBinding] = []
    for ordinal in range(blocked_workers):
        worker = Worker(
            id=f"worker-benchmark-{ordinal}",
            name=f"Generated Worker {ordinal + 1}",
            status="active",
        )
        workers.append(worker)
        bindings.append(
            WorkerBinding(
                host_id=FIXTURE_HOST,
                worker_id=worker.id,
                worker_fingerprint=worker.fingerprint,
                backend="herdr",
                target_kind="agent_id",
                target_value=f"synthetic-agent-{ordinal}",
                turn_target_kind="pane_id",
                turn_target_value=f"synthetic-pane-{ordinal}",
                sendable=True,
                reason=None,
                observed_at=FIXTURE_TIMESTAMP,
                private_fingerprint=f"synthetic-private-binding-{ordinal}",
            )
        )
    content = {
        "user_text": "generated request",
        "assistant_final_text": "generated response",
        "complete": True,
        "has_open_turn": False,
        "model": "synthetic",
    }
    return workers, bindings, content


def _revision_state(db_path: Path) -> dict[str, int]:
    with sqlite3.connect(str(db_path)) as conn:
        rows = int(
            conn.execute("SELECT COUNT(*) FROM turn_content_revisions").fetchone()[0]
        )
        current = int(
            conn.execute(
                "SELECT COUNT(*) FROM turn_content_revisions WHERE is_current = 1"
            ).fetchone()[0]
        )
        duplicate_groups = int(
            conn.execute(
                """
                SELECT COUNT(*) FROM (
                    SELECT host_id, turn_id, content_revision
                    FROM turn_content_revisions
                    GROUP BY host_id, turn_id, content_revision
                    HAVING COUNT(*) > 1
                )
                """
            ).fetchone()[0]
        )
    return {
        "rows": rows,
        "current_rows": current,
        "duplicate_groups": duplicate_groups,
    }


def _pending_row_state(db_path: Path) -> dict[str, int]:
    with sqlite3.connect(str(db_path)) as conn:
        rows = int(conn.execute("SELECT COUNT(*) FROM backend_pending").fetchone()[0])
        open_rows = int(
            conn.execute(
                "SELECT COUNT(*) FROM backend_pending WHERE observation_state = 'open'"
            ).fetchone()[0]
        )
        duplicate_groups = int(
            conn.execute(
                """
                SELECT COUNT(*) FROM (
                    SELECT host_id, worker_id
                    FROM backend_pending
                    GROUP BY host_id, worker_id
                    HAVING COUNT(*) > 1
                )
                """
            ).fetchone()[0]
        )
    return {
        "rows": rows,
        "open_rows": open_rows,
        "duplicate_groups": duplicate_groups,
    }


def _outbox_rows(db_path: Path) -> list[tuple[Any, ...]]:
    with sqlite3.connect(str(db_path)) as conn:
        return conn.execute(
            "SELECT * FROM connector_outbox ORDER BY id"
        ).fetchall()


def _seed_store(
    db_path: Path,
    workers: list[Worker],
    bindings: list[WorkerBinding],
    content: Mapping[str, Any],
) -> None:
    snapshot = Snapshot(
        host_id=FIXTURE_HOST,
        updated_at=FIXTURE_TIMESTAMP,
        workers=workers,
        backend_health=[
            BackendHealth(
                name="herdr",
                status="healthy",
                outcome="healthy_non_empty",
                observed_at=FIXTURE_TIMESTAMP,
                counts={"workers": len(workers)},
            )
        ],
    )
    store.save_snapshot(db_path, snapshot)
    if store.upsert_worker_bindings(db_path, bindings) != len(bindings):
        raise RuntimeError("binding_seed_failed")
    for ordinal, binding in enumerate(bindings):
        applied = store.apply_turn_refresh(
            db_path,
            FIXTURE_HOST,
            binding.worker_id,
            {
                **content,
                "source_turn_id": f"synthetic-source-turn-{ordinal}",
            },
            expected_binding=binding,
            observed_at=FIXTURE_TIMESTAMP,
        )
        if applied.updated != 1:
            raise RuntimeError("revision_seed_failed")
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                FIXTURE_HOST,
                "attention",
                "synthetic-delivery-key",
                "queued",
                '{"generated":true}',
                '{"opaque":"generated"}',
                FIXTURE_TIMESTAMP,
                FIXTURE_TIMESTAMP,
            ),
        )


class _FixtureEventBackend:
    def __init__(
        self,
        db_path: Path,
        workers: list[Worker],
        bindings: list[WorkerBinding],
        content: Mapping[str, Any],
    ) -> None:
        self._db_path = db_path
        self._workers = workers
        self._bindings = bindings
        self._content = content
        self._callback: Callable[[], None] | None = None
        self.started = False
        self.stopped = False
        self.callback_detached = False
        self.flush_calls = 0
        self.committed_events = 0
        self.event_rows_after = 0
        self.callback_notifications = 0

    def start(self, *, wait_for_reconcile: bool = True) -> None:
        if not wait_for_reconcile:
            raise RuntimeError("event_reconcile_not_requested")
        _seed_store(
            self._db_path,
            self._workers,
            self._bindings,
            self._content,
        )
        with sqlite3.connect(str(self._db_path)) as conn:
            before = int(
                conn.execute(
                    "SELECT COUNT(*) FROM events WHERE host_id = ?",
                    (FIXTURE_HOST,),
                ).fetchone()[0]
            )
        for ordinal in range(2):
            store.append_event(
                self._db_path,
                FIXTURE_HOST,
                "pane.output_matched",
                {
                    "schema_version": 1,
                    "generated": True,
                    "ordinal": ordinal,
                },
                observed_at=FIXTURE_TIMESTAMP,
            )
        with sqlite3.connect(str(self._db_path)) as conn:
            self.event_rows_after = int(
                conn.execute(
                    "SELECT COUNT(*) FROM events WHERE host_id = ?",
                    (FIXTURE_HOST,),
                ).fetchone()[0]
            )
        self.committed_events = self.event_rows_after - before
        if self.committed_events != 2:
            raise RuntimeError("event_commit_count_mismatch")
        self.started = True

    def set_turn_refresh_callback(
        self,
        callback: Callable[[], None] | None,
    ) -> None:
        self._callback = callback
        if callback is None:
            self.callback_detached = True

    def emit_committed_burst(self, count: int) -> None:
        if not self.started or self.stopped or count <= 0:
            raise RuntimeError("event_backend_not_ready")
        with sqlite3.connect(str(self._db_path)) as conn:
            event_rows = int(
                conn.execute(
                    "SELECT COUNT(*) FROM events WHERE host_id = ?",
                    (FIXTURE_HOST,),
                ).fetchone()[0]
            )
        if count != self.committed_events or event_rows != self.event_rows_after:
            raise RuntimeError("event_commit_count_mismatch")
        callback = self._callback
        if callback is None:
            raise RuntimeError("event_callback_missing")
        self.callback_notifications += 1
        callback()

    def flush(self) -> None:
        self.flush_calls += 1

    def stop(self) -> None:
        self.stopped = True

    @property
    def operational_status(self) -> Mapping[str, Any]:
        return {
            "status": "healthy",
            "outcome": "healthy_non_empty",
            "ready": self.started and not self.stopped,
            "running": self.started and not self.stopped,
            "reconcile_enabled": False,
            "last_event_at": FIXTURE_TIMESTAMP if self.committed_events else None,
            "last_snapshot_at": FIXTURE_TIMESTAMP,
            "last_reconcile_at": FIXTURE_TIMESTAMP,
        }


class _APIConcurrency:
    def __init__(self, workers: int) -> None:
        self.workers = workers
        self.lock = threading.Lock()
        self.release = threading.Event()
        self.active = 0
        self.maximum = 0
        self.probe_entered = 0
        self.dispatches = 0
        self.method_dispatches: dict[str, int] = {}

    def wrap(
        self,
        dispatcher: Callable[[Any], Mapping[str, Any]],
    ) -> Callable[[Any], Mapping[str, Any]]:
        def instrumented(request: Any) -> Mapping[str, Any]:
            probe = bool(
                isinstance(request, Mapping)
                and request.get("method") == "ping"
                and isinstance(request.get("params"), Mapping)
                and request["params"].get("concurrency_probe") is True
            )
            with self.lock:
                self.active += 1
                self.maximum = max(self.maximum, self.active)
                self.dispatches += 1
                method = (
                    str(request.get("method"))
                    if isinstance(request, Mapping)
                    and isinstance(request.get("method"), str)
                    else "invalid"
                )
                self.method_dispatches[method] = self.method_dispatches.get(method, 0) + 1
                if probe:
                    self.probe_entered += 1
                    if self.probe_entered == self.workers:
                        self.release.set()
            try:
                if probe and not self.release.wait(timeout=2.0):
                    raise RuntimeError("api_probe_barrier_timeout")
                return dispatcher(request)
            finally:
                with self.lock:
                    self.active -= 1

        return instrumented


def _run_api_probe(
    socket_path: Path,
    workers: int,
) -> tuple[int, bool]:
    ready = threading.Barrier(workers + 1)
    results: list[dict[str, Any]] = []
    failures: list[str] = []
    lock = threading.Lock()

    def request() -> None:
        try:
            ready.wait(timeout=2.0)
            response = DaemonAPIClient(
                socket_path,
                timeout_seconds=3.0,
            ).request("ping", {"concurrency_probe": True})
            with lock:
                results.append(response)
        except BaseException as exc:  # noqa: BLE001
            with lock:
                failures.append(type(exc).__name__)

    clients = [
        threading.Thread(
            target=request,
            name=f"tendwire-benchmark-api-probe-{index}",
        )
        for index in range(workers)
    ]
    started = perf_counter_ns()
    for client in clients:
        client.start()
    ready.wait(timeout=2.0)
    for client in clients:
        client.join(timeout=4.0)
    elapsed = perf_counter_ns() - started
    clean = all(not client.is_alive() for client in clients)
    ok = (
        clean
        and not failures
        and len(results) == workers
        and all(response.get("ok") is True for response in results)
    )
    return elapsed, ok


def _validate_list(response: Mapping[str, Any], blocked_workers: int) -> None:
    result = response.get("result")
    turns = result.get("turns") if isinstance(result, Mapping) else None
    if (
        response.get("ok") is not True
        or not isinstance(result, Mapping)
        or result.get("schema_version") != 2
        or not isinstance(turns, list)
        or sum(
            isinstance(turn, Mapping)
            and isinstance(turn.get("content"), Mapping)
            and bool(turn["content"].get("content_revision"))
            for turn in turns
        )
        != blocked_workers
    ):
        raise RuntimeError("turn_list_contract_failed")


def _validate_pending(response: Mapping[str, Any], _blocked_workers: int) -> None:
    result = response.get("result")
    interactions = (
        result.get("pending_interactions") if isinstance(result, Mapping) else None
    )
    health = result.get("pending_health") if isinstance(result, Mapping) else None
    counts = health.get("counts") if isinstance(health, Mapping) else None
    fingerprint = (
        result.get("content_fingerprint") if isinstance(result, Mapping) else None
    )
    if (
        response.get("ok") is not True
        or not isinstance(result, Mapping)
        or result.get("schema_version") != 1
        or not isinstance(interactions, list)
        or not all(isinstance(item, Mapping) for item in interactions)
        or not isinstance(health, Mapping)
        or health.get("status") not in {"healthy", "degraded"}
        or not isinstance(counts, Mapping)
        or set(counts) != {"fresh", "stale", "total"}
        or any(
            isinstance(counts.get(key), bool)
            or not isinstance(counts.get(key), int)
            or counts[key] < 0
            for key in ("fresh", "stale", "total")
        )
        or counts["total"] != counts["fresh"] + counts["stale"]
        or not isinstance(fingerprint, str)
        or len(fingerprint) != 24
        or any(character not in "0123456789abcdef" for character in fingerprint)
        or recompute_pending_content_fingerprint(result) != fingerprint
    ):
        raise RuntimeError("pending_list_contract_failed")


def _wait_for_pending_count(
    socket_path: Path,
    *,
    expected_count: int,
    blocked_workers: int,
    timeout_seconds: float,
    code: str,
) -> tuple[dict[str, Any], int]:
    deadline = time.monotonic() + timeout_seconds
    polls = 0
    while True:
        response = DaemonAPIClient(
            socket_path,
            timeout_seconds=1.0,
        ).request("pending.list")
        polls += 1
        _validate_pending(response, blocked_workers)
        result = response["result"]
        if len(result["pending_interactions"]) == expected_count:
            return response, polls
        if time.monotonic() >= deadline:
            raise RuntimeError(code)
        time.sleep(POLL_SECONDS)


def _validate_health(response: Mapping[str, Any], blocked_workers: int) -> None:
    result = response.get("result")
    ingestion = result.get("turn_ingestion") if isinstance(result, Mapping) else None
    if (
        response.get("ok") is not True
        or not isinstance(ingestion, Mapping)
        or int(ingestion.get("active") or 0) < blocked_workers
    ):
        raise RuntimeError("health_contract_failed")


def _validate_command(response: Mapping[str, Any], _blocked_workers: int) -> None:
    result = response.get("result")
    try:
        envelope = (
            CommandEnvelope.from_dict(dict(result))
            if isinstance(result, Mapping)
            else None
        )
    except (TypeError, ValueError):
        envelope = None
    if (
        response.get("ok") is not True
        or envelope is None
        or envelope.ok is not True
        or envelope.status != STATUS_NOOP
        or envelope.dry_run is not True
        or envelope.disposition != DISPOSITION_NO_RECEIPT
    ):
        raise RuntimeError("command_contract_failed")


def _measure_requests(
    socket_path: Path,
    marker_dir: Path,
    scheduler: TurnIngestionScheduler,
    *,
    method: str,
    params: Mapping[str, Any],
    validator: Callable[[Mapping[str, Any], int], None],
    blocked_workers: int,
    warmups: int,
    samples: int,
    budget_ns: int,
) -> dict[str, Any]:
    timings: list[int] = []
    response_bytes: list[int] = []
    for index in range(warmups + samples):
        if (
            int(scheduler.operational_status().get("active") or 0) < blocked_workers
            or len(_active_markers(marker_dir)) < blocked_workers
        ):
            raise RuntimeError("adapters_not_blocked_during_request")
        started = perf_counter_ns()
        response = DaemonAPIClient(
            socket_path,
            timeout_seconds=1.0,
        ).request(method, params)
        elapsed = perf_counter_ns() - started
        validator(response, blocked_workers)
        if (
            int(scheduler.operational_status().get("active") or 0) < blocked_workers
            or len(_active_markers(marker_dir)) < blocked_workers
        ):
            raise RuntimeError("adapter_block_ended_during_request")
        if index >= warmups:
            timings.append(elapsed)
            response_bytes.append(len(_canonical_json(response).encode("utf-8")) + 1)
    return _metric(
        timings,
        warmups=warmups,
        response_bytes=response_bytes,
        budget_ns=budget_ns,
    )


def _privacy_scan(report: Mapping[str, Any], forbidden_values: list[str]) -> bool:
    encoded = _canonical_json(report)
    return all(not value or value not in encoded for value in forbidden_values)


def _contains_raw_error_field(value: Any) -> bool:
    if isinstance(value, Mapping):
        return any(
            str(key) in {"error", "error_type", "errors"}
            or _contains_raw_error_field(item)
            for key, item in value.items()
        )
    if isinstance(value, list):
        return any(_contains_raw_error_field(item) for item in value)
    return False


def _command_text(args: argparse.Namespace) -> str:
    return (
        "PYTHONPATH=src python3 scripts/turn_ingestion_benchmark.py "
        f"--workers {args.workers} --blocked-workers {args.blocked_workers} "
        f"--blocked-seconds {args.blocked_seconds:g} --warmups {args.warmups} "
        f"--samples {args.samples} --json"
    )


def _benchmark(args: argparse.Namespace) -> dict[str, Any]:
    temporary_path: Path | None = None
    forbidden_values: list[str] = [
        FIXTURE_HOST,
        "generated request",
        "generated response",
        "synthetic-delivery-key",
        '{"opaque":"generated"}',
        "generated pending prompt",
        "Approve generated choice",
        "Reject generated choice",
        "synthetic-private-decision",
    ]
    report: dict[str, Any] | None = None
    with tempfile.TemporaryDirectory(
        prefix="tendwire-turn-benchmark-",
        dir="/dev/shm",
    ) as raw_root:
        root = Path(raw_root)
        temporary_path = root
        db_path = root / "benchmark.db"
        socket_path = root / "benchmark.sock"
        marker_dir = root / "adapter-markers"
        release_path = root / "release-adapters"
        adapter_path = root / "generated-herdr"
        state_path = root / "pending-state"
        marker_dir.mkdir(mode=0o700)
        os.mkfifo(release_path, mode=0o600)
        _write_adapter(adapter_path, marker_dir, release_path, state_path)
        state_path.write_text("none", encoding="utf-8")
        state_path.chmod(0o600)
        forbidden_values.extend(
            [
                str(root),
                str(db_path),
                str(socket_path),
                str(marker_dir),
                str(release_path),
                str(adapter_path),
                str(state_path),
            ]
        )
        workers, bindings, content = _fixture(args.blocked_workers)
        forbidden_values.extend(
            [
                worker.id
                for worker in workers
            ]
        )
        forbidden_values.extend(
            value
            for binding in bindings
            for value in (
                binding.target_value,
                str(binding.turn_target_value or ""),
                binding.private_fingerprint,
            )
        )

        config = Config(
            host_id=FIXTURE_HOST,
            herdr_bin=str(adapter_path),
            data_dir=root,
            db_path=db_path,
            socket_path=socket_path,
            herdr_timeout_seconds=max(60.0, float(args.blocked_seconds) + 30.0),
            herdr_backend="socket",
            reconcile_interval_seconds=0.0,
            turn_refresh_interval_seconds=SCHEDULER_REFRESH_SECONDS,
            turn_refresh_workers=SCHEDULER_WORKERS,
        )
        event_backend = _FixtureEventBackend(
            db_path,
            workers,
            bindings,
            content,
        )
        scheduler: TurnIngestionScheduler | None = None

        def scheduler_factory(current: Config) -> TurnIngestionScheduler:
            nonlocal scheduler
            scheduler = TurnIngestionScheduler(current)
            return scheduler

        command_calls = 0
        command_lock = threading.Lock()

        def submit_command(_config: Config, payload: str) -> Mapping[str, Any]:
            nonlocal command_calls
            parsed = json.loads(payload)
            if (
                not isinstance(parsed, dict)
                or parsed.get("schema_version") != 1
                or parsed.get("action") != "noop"
                or parsed.get("dry_run") is not True
            ):
                raise RuntimeError("synthetic_command_invalid")
            request = CommandRequest.from_dict(parsed)
            with command_lock:
                command_calls += 1
            return CommandEnvelope.from_result(
                request,
                ok=True,
                status=STATUS_NOOP,
                result={},
            ).to_dict()

        baseline_threads = _thread_ids(
            (
                "tendwire-turn-",
                "tendwire-daemon-api",
                "tendwire-benchmark-",
            )
        )
        daemon = TendwireDaemon(
            config,
            hooks=DaemonHooks(
                event_backend_factory=lambda _config, _stop: event_backend,
                turn_scheduler_factory=scheduler_factory,
                submit_command=submit_command,
            ),
        )
        server_thread: threading.Thread | None = None
        shutdown_ns = 0
        api_probe_elapsed_ns = 0
        api_probe_ok = False
        api_concurrency = _APIConcurrency(args.workers)
        initial_revisions: dict[str, int] = {}
        initial_outbox: list[tuple[Any, ...]] = []
        during_block_health: dict[str, Any] = {}
        final_health: dict[str, Any] = {}
        latency: dict[str, Any] = {}
        source_calls_before_requests = 0
        source_calls_after_requests = 0
        blocked_observation_started = 0
        release_fd: int | None = None
        production_handlers_measured = False
        production_pending_handler_measured = False
        turn_list_calls_during_pending_measurement = 0
        production_event_callback_bound = False
        pending_source_calls_before_measurement = 0
        pending_source_calls_after_measurement = 0
        pending_rows_before_requests: dict[str, int] = {}
        pending_rows_after_requests: dict[str, int] = {}
        final_pending_rows: dict[str, int] = {}
        independent_pending_polls = 0
        independent_turn_list_calls = 0
        independent_prompt_count = 0
        independent_clear_count = -1
        independent_discovery_fingerprint_changed = False
        independent_unchanged_fingerprint_stable = False
        independent_clear_fingerprint_changed = False
        independent_clear_restored_baseline = False
        try:
            daemon.start()
            if scheduler is None or daemon.server is None:
                raise RuntimeError("daemon_components_missing")
            original_dispatcher = daemon.server.dispatcher
            daemon.server.dispatcher = api_concurrency.wrap(original_dispatcher)
            api_dispatch = getattr(original_dispatcher, "__self__", None)
            production_handlers_measured = bool(
                getattr(api_dispatch, "_get_turns", None) == daemon.get_turns
                and getattr(api_dispatch, "_get_health", None) == daemon.get_health
            )
            production_pending_handler_measured = bool(
                getattr(api_dispatch, "_get_pending", None) == daemon.get_pending
            )
            production_event_callback_bound = (
                event_backend._callback == scheduler.request_refresh
            )
            if (
                not production_handlers_measured
                or not production_pending_handler_measured
                or not production_event_callback_bound
            ):
                raise RuntimeError("production_handlers_not_bound")
            server_thread = threading.Thread(
                target=daemon.serve_forever,
                name="tendwire-benchmark-daemon",
            )
            server_thread.start()
            _wait_until(
                lambda: len(_active_markers(marker_dir)) >= args.blocked_workers,
                5.0,
                "blocked_adapters_not_entered",
            )
            blocked_observation_started = perf_counter_ns()
            initial_revisions = _revision_state(db_path)
            initial_outbox = _outbox_rows(db_path)
            source_calls_before_requests = _source_call_count(marker_dir)
            pending_rows_before_requests = _pending_row_state(db_path)
            if source_calls_before_requests != args.blocked_workers:
                raise RuntimeError("unexpected_initial_source_calls")

            api_probe_elapsed_ns, api_probe_ok = _run_api_probe(
                socket_path,
                args.workers,
            )
            event_backend.emit_committed_burst(2)
            _wait_until(
                lambda: int(scheduler.operational_status().get("coalesced") or 0)
                >= args.blocked_workers,
                SCHEDULER_REFRESH_SECONDS + 2.0,
                "scheduler_coalescing_not_observed",
            )
            during_response = DaemonAPIClient(
                socket_path,
                timeout_seconds=1.0,
            ).request("health.get")
            _validate_health(during_response, args.blocked_workers)
            during_block_health = dict(
                during_response["result"]["turn_ingestion"]
            )

            latency["turn_list"] = _measure_requests(
                socket_path,
                marker_dir,
                scheduler,
                method="turn.list",
                params={
                    "schema_version": 2,
                    "limit": 100,
                    "cursor": None,
                    "since": None,
                },
                validator=_validate_list,
                blocked_workers=args.blocked_workers,
                warmups=args.warmups,
                samples=args.samples,
                budget_ns=LIST_BUDGET_NS,
            )
            pending_source_calls_before_measurement = _source_call_count(marker_dir)
            turn_list_calls_before_pending = api_concurrency.method_dispatches.get(
                "turn.list",
                0,
            )
            latency["pending_list"] = _measure_requests(
                socket_path,
                marker_dir,
                scheduler,
                method="pending.list",
                params={},
                validator=_validate_pending,
                blocked_workers=args.blocked_workers,
                warmups=args.warmups,
                samples=args.samples,
                budget_ns=LIST_BUDGET_NS,
            )
            pending_source_calls_after_measurement = _source_call_count(marker_dir)
            if (
                pending_source_calls_after_measurement
                != pending_source_calls_before_measurement
            ):
                raise RuntimeError("pending_list_started_source_reads")
            turn_list_calls_during_pending_measurement = (
                api_concurrency.method_dispatches.get("turn.list", 0)
                - turn_list_calls_before_pending
            )
            latency["health_get"] = _measure_requests(
                socket_path,
                marker_dir,
                scheduler,
                method="health.get",
                params={},
                validator=_validate_health,
                blocked_workers=args.blocked_workers,
                warmups=args.warmups,
                samples=args.samples,
                budget_ns=HEALTH_BUDGET_NS,
            )
            latency["command_submit"] = _measure_requests(
                socket_path,
                marker_dir,
                scheduler,
                method="command.submit",
                params={
                    "schema_version": 1,
                    "action": "noop",
                    "dry_run": True,
                },
                validator=_validate_command,
                blocked_workers=args.blocked_workers,
                warmups=args.warmups,
                samples=args.samples,
                budget_ns=COMMAND_BUDGET_NS,
            )
            source_calls_after_requests = _source_call_count(marker_dir)
            if source_calls_after_requests != source_calls_before_requests:
                raise RuntimeError("request_path_started_source_reads")
            pending_rows_after_requests = _pending_row_state(db_path)

            remaining_ns = int(args.blocked_seconds * 1_000_000_000) - (
                perf_counter_ns() - blocked_observation_started
            )
            if remaining_ns > 0:
                time.sleep(remaining_ns / 1_000_000_000)
            if len(_active_markers(marker_dir)) < args.blocked_workers:
                raise RuntimeError("configured_block_not_held")
            release_fd = os.open(release_path, os.O_WRONLY | os.O_NONBLOCK)
            os.write(release_fd, b"R" * max(64, args.blocked_workers * 4))
            _wait_until(
                lambda: (
                    _source_call_count(marker_dir) >= args.blocked_workers * 2
                    and int(scheduler.operational_status().get("active") or 0) == 0
                    and int(scheduler.operational_status().get("queue_depth") or 0) == 0
                ),
                3.0,
                "scheduler_did_not_drain",
            )
            turn_calls_before_independent = api_concurrency.method_dispatches.get(
                "turn.list",
                0,
            )
            baseline_pending_response = DaemonAPIClient(
                socket_path,
                timeout_seconds=1.0,
            ).request("pending.list")
            independent_pending_polls += 1
            _validate_pending(baseline_pending_response, args.blocked_workers)
            baseline_pending = baseline_pending_response["result"]
            baseline_fingerprint = str(baseline_pending["content_fingerprint"])

            state_path.write_text("open", encoding="utf-8")
            discovery_source_calls = _source_call_count(marker_dir)
            event_backend.emit_committed_burst(2)
            _wait_until(
                lambda: (
                    _source_call_count(marker_dir)
                    >= discovery_source_calls + args.blocked_workers
                    and int(scheduler.operational_status().get("active") or 0) == 0
                    and int(scheduler.operational_status().get("queue_depth") or 0) == 0
                ),
                3.0,
                "independent_pending_discovery_did_not_drain",
            )
            open_pending_response, discovery_polls = _wait_for_pending_count(
                socket_path,
                expected_count=args.blocked_workers,
                blocked_workers=args.blocked_workers,
                timeout_seconds=3.0,
                code="independent_pending_not_discovered",
            )
            independent_pending_polls += discovery_polls
            open_pending = open_pending_response["result"]
            independent_prompt_count = len(open_pending["pending_interactions"])
            open_fingerprint = str(open_pending["content_fingerprint"])
            independent_discovery_fingerprint_changed = (
                open_fingerprint != baseline_fingerprint
            )

            unchanged_pending_response = DaemonAPIClient(
                socket_path,
                timeout_seconds=1.0,
            ).request("pending.list")
            independent_pending_polls += 1
            _validate_pending(unchanged_pending_response, args.blocked_workers)
            unchanged_fingerprint = str(
                unchanged_pending_response["result"]["content_fingerprint"]
            )
            independent_unchanged_fingerprint_stable = (
                unchanged_fingerprint == open_fingerprint
            )

            state_path.write_text("none", encoding="utf-8")
            clearing_source_calls = _source_call_count(marker_dir)
            event_backend.emit_committed_burst(2)
            _wait_until(
                lambda: (
                    _source_call_count(marker_dir)
                    >= clearing_source_calls + args.blocked_workers
                    and int(scheduler.operational_status().get("active") or 0) == 0
                    and int(scheduler.operational_status().get("queue_depth") or 0) == 0
                ),
                3.0,
                "independent_pending_clear_did_not_drain",
            )
            cleared_pending_response, clear_polls = _wait_for_pending_count(
                socket_path,
                expected_count=0,
                blocked_workers=args.blocked_workers,
                timeout_seconds=3.0,
                code="independent_pending_not_cleared",
            )
            independent_pending_polls += clear_polls
            cleared_pending = cleared_pending_response["result"]
            independent_clear_count = len(cleared_pending["pending_interactions"])
            cleared_fingerprint = str(cleared_pending["content_fingerprint"])
            independent_clear_fingerprint_changed = (
                cleared_fingerprint != open_fingerprint
            )
            independent_clear_restored_baseline = (
                cleared_fingerprint == baseline_fingerprint
            )
            independent_turn_list_calls = (
                api_concurrency.method_dispatches.get("turn.list", 0)
                - turn_calls_before_independent
            )
            final_pending_rows = _pending_row_state(db_path)

            daemon.server.dispatcher = api_concurrency.wrap(original_dispatcher)
            final_response = DaemonAPIClient(
                socket_path,
                timeout_seconds=1.0,
            ).request("health.get")
            if final_response.get("ok") is not True:
                raise RuntimeError("final_health_failed")
            final_health = dict(final_response["result"]["turn_ingestion"])
        finally:
            if release_fd is not None:
                try:
                    os.close(release_fd)
                except OSError:
                    pass
            shutdown_started = perf_counter_ns()
            daemon.stop()
            if server_thread is not None:
                server_thread.join(timeout=2.0)
            shutdown_ns = perf_counter_ns() - shutdown_started

        if scheduler is None or server_thread is None:
            raise RuntimeError("daemon_lifecycle_incomplete")
        final_revisions = _revision_state(db_path)
        final_outbox = _outbox_rows(db_path)
        adapter_records = _marker_records(marker_dir)
        process_ids = {record["process_id"] for record in adapter_records}
        _wait_until(
            lambda: all(not _process_alive(process_id) for process_id in process_ids),
            1.0,
            "adapter_child_not_reaped",
        )
        _wait_until(
            lambda: not (
                _thread_ids(
                    (
                        "tendwire-turn-",
                        "tendwire-daemon-api",
                        "tendwire-benchmark-",
                    )
                )
                - baseline_threads
            ),
            2.0,
            "benchmark_thread_not_reaped",
        )
        remaining_threads = _thread_ids(
            (
                "tendwire-turn-",
                "tendwire-daemon-api",
                "tendwire-benchmark-",
            )
        ) - baseline_threads
        source_calls_final = len(adapter_records)
        forbidden_values.extend(
            marker
            for process_id in process_ids
            for marker in (
                f'"process_id":{process_id}',
                f'"process_id":"{process_id}"',
                f'"pid":{process_id}',
                f'"pid":"{process_id}"',
            )
        )
        overlap_ns = _first_call_overlap_ns(adapter_records, args.blocked_workers)
        response_bytes_max = max(
            metric["response_bytes_max"] for metric in latency.values()
        )
        scheduler_bounds = {
            "refresh_interval_seconds": config.turn_refresh_interval_seconds,
            "max_workers": config.turn_refresh_workers,
            "queue_capacity": SCHEDULER_QUEUE_CAPACITY,
            "adapter_timeout_seconds": config.herdr_timeout_seconds,
        }
        checks = {
            "private_temporary_directory": stat.S_IMODE(root.stat().st_mode) == 0o700,
            "private_marker_directory": stat.S_IMODE(marker_dir.stat().st_mode) == 0o700,
            "private_adapter_executable": stat.S_IMODE(adapter_path.stat().st_mode) == 0o700,
            "private_database_mode": stat.S_IMODE(db_path.stat().st_mode) & 0o077 == 0,
            "real_unix_socket_removed": not os.path.lexists(socket_path),
            "blocked_adapters_overlapped": overlap_ns >= int(args.blocked_seconds * 1_000_000_000),
            "cached_requests_started_no_source_reads": source_calls_after_requests
            == source_calls_before_requests,
            "api_probe_completed": api_probe_ok,
            "api_worker_bound_observed": api_concurrency.maximum == args.workers,
            "production_list_health_handlers_measured": production_handlers_measured,
            "production_pending_handler_measured": production_pending_handler_measured,
            "production_event_callback_bound": production_event_callback_bound,
            "pending_list_started_no_turn_reads": turn_list_calls_during_pending_measurement == 0,
            "pending_list_started_no_source_reads": (
                pending_source_calls_after_measurement
                == pending_source_calls_before_measurement
            ),
            "pending_list_store_rows_unchanged": (
                pending_rows_after_requests == pending_rows_before_requests
            ),
            "independent_pending_discovered": (
                independent_prompt_count == args.blocked_workers
            ),
            "independent_pending_cleared": independent_clear_count == 0,
            "independent_pending_zero_turn_calls": independent_turn_list_calls == 0,
            "independent_pending_discovery_fingerprint_changed": (
                independent_discovery_fingerprint_changed
            ),
            "independent_pending_unchanged_fingerprint_stable": (
                independent_unchanged_fingerprint_stable
            ),
            "independent_pending_clear_fingerprint_changed": (
                independent_clear_fingerprint_changed
            ),
            "independent_pending_clear_restored_baseline": (
                independent_clear_restored_baseline
            ),
            "no_duplicate_pending_rows": (
                final_pending_rows.get("duplicate_groups") == 0
            ),
            "independent_pending_health_coherent": (
                open_pending["pending_health"]
                == {
                    "status": "healthy",
                    "counts": {
                        "fresh": args.blocked_workers,
                        "stale": 0,
                        "total": args.blocked_workers,
                    },
                }
                and cleared_pending["pending_health"]
                == {
                    "status": "healthy",
                    "counts": {"fresh": 0, "stale": 0, "total": 0},
                }
            ),
            "independent_pending_rows_coherent_after_clear": (
                final_pending_rows.get("rows") == args.blocked_workers
                and final_pending_rows.get("open_rows") == 0
            ),
            "adapter_worker_bound_observed": _interval_maximum(adapter_records)
            == args.blocked_workers,
            "event_burst_committed_before_notification": event_backend.committed_events == 2
            and event_backend.callback_notifications == 3,
            "scheduler_queue_drained": int(final_health.get("queue") or 0) == 0
            and int(final_health.get("active") or 0) == 0,
            "scheduler_coalescing_observed": int(final_health.get("coalesced") or 0)
            >= args.blocked_workers,
            "scheduler_no_timeouts_or_queue_full": int(
                final_health.get("timed_out") or 0
            )
            == 0
            and int(final_health.get("queue_full") or 0) == 0,
            "revision_rows_unchanged": final_revisions == initial_revisions,
            "no_duplicate_revisions": final_revisions.get("duplicate_groups") == 0,
            "outbox_rows_unchanged": final_outbox == initial_outbox,
            "expected_outbox_rows_preserved": (
                len(final_outbox) == args.blocked_workers + 1
            ),
            "expected_command_calls": command_calls == args.warmups + args.samples,
            "list_budget_met": bool(latency["turn_list"]["documented_host_budget_met"]),
            "pending_list_budget_met": bool(
                latency["pending_list"]["documented_host_budget_met"]
            ),
            "health_budget_met": bool(latency["health_get"]["documented_host_budget_met"]),
            "command_budget_met": bool(latency["command_submit"]["documented_host_budget_met"]),
            "shutdown_bounded": shutdown_ns <= SHUTDOWN_BOUND_NS,
            "daemon_thread_reaped": not server_thread.is_alive(),
            "adapter_children_reaped": all(
                not _process_alive(process_id) for process_id in process_ids
            ),
            "benchmark_threads_reaped": not remaining_threads,
            "event_callback_detached": event_backend.callback_detached,
            "event_backend_stopped": event_backend.stopped,
        }
        report = {
            "schema_version": REPORT_SCHEMA_VERSION,
            "ok": False,
            "status": "validating",
            "command": _command_text(args),
            "parameters": {
                "api_probe_workers": args.workers,
                "blocked_adapter_workers": args.blocked_workers,
                "blocked_seconds": args.blocked_seconds,
                "warmups_per_operation": args.warmups,
                "samples_per_operation": args.samples,
            },
            "environment": {
                "python_version": platform.python_version(),
                "sqlite_version": sqlite3.sqlite_version,
                "operating_system": platform.system(),
                "platform_release": platform.release(),
                "platform": platform.platform(),
                "architecture": platform.machine(),
                "timer": "perf_counter_ns",
                "percentiles": "nearest_rank",
                "source_checkout_pythonpath": "src",
                "fixture_storage": "memory_backed_tmpfs",
            },
            "transport": {
                "kind": "unix_stream_socket",
                "request_workers": API_REQUEST_WORKERS,
                "admission_capacity": API_ADMISSION_CAPACITY,
                "request_frame_max_bytes": MAX_REQUEST_BYTES,
                "response_frame_max_bytes": MAX_RESPONSE_BYTES,
                "observed_max_api_concurrency": api_concurrency.maximum,
                "probe_elapsed_ns": api_probe_elapsed_ns,
                "dispatches": api_concurrency.dispatches,
                "measured_response_bytes_max": response_bytes_max,
                "method_dispatches": dict(sorted(api_concurrency.method_dispatches.items())),
                "handler_mode": "production_store_backed",
            },
            "ingestion": {
                "scheduler_bounds": scheduler_bounds,
                "source_calls_before_requests": source_calls_before_requests,
                "source_calls_after_requests": source_calls_after_requests,
                "source_calls_final": source_calls_final,
                "turn_list_calls_during_pending_measurement": turn_list_calls_during_pending_measurement,
                "pending_source_calls_before_measurement": pending_source_calls_before_measurement,
                "pending_source_calls_after_measurement": pending_source_calls_after_measurement,
                "independent_pending_polls": independent_pending_polls,
                "independent_turn_list_calls": independent_turn_list_calls,
                "independent_prompt_count": independent_prompt_count,
                "independent_clear_count": independent_clear_count,
                "observed_max_adapter_concurrency": _interval_maximum(adapter_records),
                "first_call_overlap_ns": overlap_ns,
                "event_committed_count": event_backend.committed_events,
                "event_callback_notifications": event_backend.callback_notifications,
                "during_block": {
                    "status": during_block_health.get("status"),
                    "queue": during_block_health.get("queue"),
                    "active": during_block_health.get("active"),
                    "refreshed": during_block_health.get("refreshed"),
                    "failed": during_block_health.get("failed"),
                    "timed_out": during_block_health.get("timed_out"),
                    "coalesced": during_block_health.get("coalesced"),
                    "queue_full": during_block_health.get("queue_full"),
                },
                "final": {
                    "status": final_health.get("status"),
                    "queue": final_health.get("queue"),
                    "active": final_health.get("active"),
                    "refreshed": final_health.get("refreshed"),
                    "failed": final_health.get("failed"),
                    "timed_out": final_health.get("timed_out"),
                    "coalesced": final_health.get("coalesced"),
                    "queue_full": final_health.get("queue_full"),
                },
            },
            "latency_ns": latency,
            "store": {
                "schema_version": store.STORE_SCHEMA_VERSION,
                "event_rows_after": event_backend.event_rows_after,
                "generated_event_rows": event_backend.committed_events,
                "bindings": len(bindings),
                "revision_rows_before": initial_revisions["rows"],
                "revision_rows_after": final_revisions["rows"],
                "current_revision_rows_after": final_revisions["current_rows"],
                "duplicate_revision_groups_after": final_revisions["duplicate_groups"],
                "pending_rows_before_requests": pending_rows_before_requests["rows"],
                "pending_rows_after_requests": pending_rows_after_requests["rows"],
                "pending_rows_after_independent_clear": final_pending_rows["rows"],
                "pending_open_rows_after_independent_clear": final_pending_rows["open_rows"],
                "duplicate_pending_groups_after": final_pending_rows["duplicate_groups"],
                "outbox_rows_before": len(initial_outbox),
                "outbox_rows_after": len(final_outbox),
            },
            "cleanup": {
                "shutdown_ns": shutdown_ns,
                "shutdown_bound_ns": SHUTDOWN_BOUND_NS,
                "adapter_child_count": len(process_ids),
                "adapter_children_alive": sum(
                    _process_alive(process_id) for process_id in process_ids
                ),
                "benchmark_threads_alive": len(remaining_threads),
                "socket_present_after_shutdown": os.path.lexists(socket_path),
                "event_flush_calls": event_backend.flush_calls,
            },
            "checks": checks,
        }

    if report is None:
        raise RuntimeError("report_not_created")
    report["checks"]["temporary_artifacts_removed"] = bool(
        temporary_path is not None and not temporary_path.exists()
    )
    report["checks"]["raw_errors_absent"] = not _contains_raw_error_field(report)
    if not _privacy_scan(report, forbidden_values):
        raise RuntimeError("privacy_scan_failed")
    report["checks"]["privacy_scan_passed"] = True
    failed = sorted(
        name
        for name, passed in report["checks"].items()
        if isinstance(passed, bool) and not passed
    )
    if failed:
        raise RuntimeError("benchmark_invariants_failed")
    report["ok"] = True
    report["status"] = "completed"
    return report


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the deterministic synthetic turn-ingestion benchmark."
    )
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--blocked-workers", type=int, default=2)
    parser.add_argument("--blocked-seconds", type=float, default=5.0)
    parser.add_argument("--warmups", type=int, default=3)
    parser.add_argument("--samples", type=int, default=21)
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit the aggregate report as one compact JSON object.",
    )
    return parser


def main() -> int:
    benchmark_started = perf_counter_ns()
    args = _parser().parse_args()
    if (
        not 1 <= args.workers <= API_REQUEST_WORKERS
        or not 2 <= args.blocked_workers <= SCHEDULER_WORKERS
        or not math.isfinite(args.blocked_seconds)
        or args.blocked_seconds <= 0
        or args.warmups < 0
        or args.samples <= 0
    ):
        print(
            _canonical_json(
                {
                    "schema_version": REPORT_SCHEMA_VERSION,
                    "ok": False,
                    "status": "invalid_arguments",
                }
            )
        )
        return 2
    try:
        report = _benchmark(args)
        report["wall_time_ns"] = perf_counter_ns() - benchmark_started
    except Exception as exc:
        print(
            _canonical_json(
                {
                    "schema_version": REPORT_SCHEMA_VERSION,
                    "ok": False,
                    "status": "benchmark_failed",
                    "error_type": type(exc).__name__,
                }
            )
        )
        return 1
    print(_canonical_json(report))
    return 0


if __name__ == "__main__":
    sys.exit(main())
