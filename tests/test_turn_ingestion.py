"""Deterministic concurrency tests for daemon-owned turn ingestion."""

from __future__ import annotations

import json
import multiprocessing
import os
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

from tendwire.backends import herdr_turns
from tendwire.backends.herdr_turns import (
    TurnIngestionScheduler,
    TurnRefreshResult,
)
from tendwire.config import Config
from tendwire.core.models import Snapshot, Worker, WorkerBinding
from tendwire.core.turns import PendingObservation, PendingObservedChoice
from tendwire.core.projector import project_from_raw
from tendwire.store.sqlite import (
    apply_backend_pending_observation,
    init_store,
    merge_turn_content,
    pending_payload_from_store,
    save_snapshot,
    turns_payload_from_store,
    upsert_worker_bindings,
)


def _blocked_codex_child(channel) -> None:
    herdr_turns._blocking_recv_frame(
        channel,
        herdr_turns._CODEX_STATE_IPC_MAX_BYTES,
    )
    threading.Event().wait(30)


def _wrong_source_codex_child(channel) -> None:
    try:
        request = json.loads(
            herdr_turns._blocking_recv_frame(
                channel,
                herdr_turns._CODEX_STATE_IPC_MAX_BYTES,
            ).decode("utf-8")
        )
        response = {
            "protocol": 1,
            "nonce": request["nonce"],
            "disposition": "ok",
            "content": {
                "user_text": "must not publish",
                "assistant_final_text": "must not publish",
                "complete": True,
                "has_open_turn": False,
                "source_turn_id": "wrong-source",
            },
            "parser_state": {"source": "omp", "state": None},
            "bytes_read": 0,
        }
        herdr_turns._blocking_send_frame(
            channel,
            json.dumps(response, separators=(",", ":")).encode("utf-8"),
        )
    finally:
        channel.close()


def _oversized_direct_omp_child(channel) -> None:
    try:
        herdr_turns._blocking_recv_frame(
            channel,
            herdr_turns._OMP_REQUEST_MAX_BYTES,
        )
        herdr_turns._blocking_send_frame(
            channel,
            b"x" * (herdr_turns._OMP_IPC_RESPONSE_CHUNK_BYTES + 1),
        )
    finally:
        channel.close()


def _large_direct_codex_child(channel) -> None:
    try:
        request = json.loads(
            herdr_turns._blocking_recv_frame(
                channel,
                herdr_turns._CODEX_STATE_IPC_MAX_BYTES,
            ).decode("utf-8")
        )
        final = "codex-frame-" + (
            "c" * (herdr_turns._OMP_IPC_RESPONSE_CHUNK_BYTES + 1024)
        )
        response = {
            "protocol": 1,
            "nonce": request["nonce"],
            "disposition": "ok",
            "content": {
                "assistant_final_text": final,
                "complete": True,
                "has_open_turn": False,
                "source_turn_id": "codex-large-frame",
            },
            "parser_state": request["parser_state"],
            "bytes_read": 0,
        }
        herdr_turns._blocking_send_frame(
            channel,
            json.dumps(response, separators=(",", ":")).encode("utf-8"),
        )
    finally:
        channel.close()


def _wait_until(predicate, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise AssertionError("condition did not become true")
        threading.Event().wait(min(0.01, remaining))


def _binding(config: Config, worker: Any, ordinal: int, *, target: str | None = None) -> WorkerBinding:
    return WorkerBinding(
        host_id=config.host_id,
        worker_id=worker.id,
        worker_fingerprint=worker.fingerprint,
        backend="herdr",
        target_kind="agent_id",
        target_value=f"agent-{ordinal}",
        turn_target_kind="pane_id",
        turn_target_value=target or f"pane-{ordinal}",
        sendable=True,
        observed_at="2026-07-12T00:00:00+00:00",
        expires_at="9999-12-31T23:59:59+00:00",
        private_fingerprint=f"private-{ordinal}",
    )


def _scheduler_store(tmp_path: Path, count: int) -> tuple[Config, Any, list[WorkerBinding]]:
    config = Config(
        host_id="ingestion-host",
        db_path=tmp_path / "ingestion.db",
        herdr_timeout_seconds=0.5,
        turn_refresh_interval_seconds=100.0,
        turn_refresh_workers=4,
    )
    snapshot = project_from_raw(
        config,
        workers=[
            {"id": f"worker-{index}", "name": f"worker {index}", "status": "active"}
            for index in range(count)
        ],
    )
    init_store(config.db_path)
    save_snapshot(config.db_path, snapshot)
    bindings = [_binding(config, worker, index) for index, worker in enumerate(snapshot.workers)]
    upsert_worker_bindings(config.db_path, bindings)
    return config, snapshot, bindings


def test_background_scheduler_discovers_and_clears_pending_without_turn_list(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config, _snapshot, bindings = _scheduler_store(tmp_path, 1)
    current = {
        "observation": PendingObservation(
            "open_prompt",
            question="Background choice?",
            pending_kind="question",
            choices=(
                PendingObservedChoice(
                    "choice-0123456789abcdef01234567",
                    "Continue",
                    1,
                ),
            ),
            revision_digest="revision-background",
        )
    }
    utc_now = ["2026-07-13T00:00:00+00:00"]

    def read_pending(*_args, **_kwargs):
        return {"_backend_pending_observation": current["observation"]}

    monkeypatch.setattr(herdr_turns, "_read_turn_for_binding", read_pending)
    scheduler = TurnIngestionScheduler(
        config,
        refresh_interval_seconds=100,
        max_workers=1,
        utc_clock=lambda: utc_now[0],
    )
    scheduler.start()
    try:
        _wait_until(
            lambda: any(
                row["question"] == "Background choice?"
                for row in pending_payload_from_store(
                    config.db_path,
                    config.host_id,
                )["pending_interactions"]
            )
        )
        with sqlite3.connect(config.db_path) as conn:
            assert conn.execute(
                """
                SELECT observation_state, binding_private_fingerprint
                FROM backend_pending
                WHERE host_id = ? AND worker_id = ?
                """,
                (config.host_id, bindings[0].worker_id),
            ).fetchone() == ("open", bindings[0].private_fingerprint)
        current["observation"] = PendingObservation("read_succeeded_no_prompt")
        utc_now[0] = "2026-07-13T00:00:01+00:00"
        scheduler.request_refresh()
        _wait_until(
            lambda: not pending_payload_from_store(
                config.db_path,
                config.host_id,
            )["pending_interactions"]
        )
        with sqlite3.connect(config.db_path) as conn:
            assert conn.execute(
                """
                SELECT observation_state, freshness, binding_private_fingerprint
                FROM backend_pending
                WHERE host_id = ? AND worker_id = ?
                """,
                (config.host_id, bindings[0].worker_id),
            ).fetchone() == ("none", "fresh", bindings[0].private_fingerprint)
    finally:
        scheduler.stop()


def test_background_authoritative_scan_reaps_removed_pane_pending(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config, snapshot, bindings = _scheduler_store(tmp_path, 1)
    observation = PendingObservation(
        "open_prompt",
        question="Will be removed?",
        pending_kind="question",
        revision_digest="revision-removal",
    )
    def read_pending(_config, binding, **_kwargs):
        selected = (
            observation
            if binding.private_fingerprint == bindings[0].private_fingerprint
            else PendingObservation("read_succeeded_no_prompt")
        )
        return {"_backend_pending_observation": selected}

    monkeypatch.setattr(herdr_turns, "_read_turn_for_binding", read_pending)
    scheduler = TurnIngestionScheduler(
        config,
        refresh_interval_seconds=100,
        max_workers=1,
        utc_clock=lambda: "2026-07-13T00:00:00+00:00",
    )
    scheduler.start()
    try:
        _wait_until(
            lambda: bool(
                pending_payload_from_store(
                    config.db_path,
                    config.host_id,
                )["pending_interactions"]
            )
        )
        decoy = _binding(config, snapshot.workers[0], 99)
        upsert_worker_bindings(config.db_path, [decoy])
        with sqlite3.connect(config.db_path) as conn:
            conn.execute(
                """
                DELETE FROM worker_bindings
                WHERE host_id = ? AND private_fingerprint = ?
                """,
                (config.host_id, bindings[0].private_fingerprint),
            )
        scheduler.request_refresh()
        _wait_until(
            lambda: not pending_payload_from_store(
                config.db_path,
                config.host_id,
            )["pending_interactions"]
        )
        with sqlite3.connect(config.db_path) as conn:
            assert conn.execute(
                """
                SELECT private_fingerprint FROM worker_bindings
                WHERE host_id = ?
                """,
                (config.host_id,),
            ).fetchall() == [(decoy.private_fingerprint,)]
    finally:
        scheduler.stop()


def test_same_key_never_overlaps_and_burst_causes_one_rerun(tmp_path: Path, monkeypatch) -> None:
    config, _snapshot, _bindings = _scheduler_store(tmp_path, 1)
    first_entered = threading.Event()
    release_first = threading.Event()
    second_entered = threading.Event()
    calls = 0
    active = 0
    maximum_active = 0
    lock = threading.Lock()

    def reader(_config, _binding, *, adapter_timeout_seconds):
        nonlocal calls, active, maximum_active
        with lock:
            calls += 1
            call = calls
            active += 1
            maximum_active = max(maximum_active, active)
        if call == 1:
            first_entered.set()
            assert release_first.wait(2)
        else:
            second_entered.set()
        with lock:
            active -= 1
        return TurnRefreshResult("unchanged", 0)

    original_list = herdr_turns.list_worker_bindings
    scan_after_burst = threading.Event()
    list_calls = 0

    def observed_list(*args, **kwargs):
        nonlocal list_calls
        result = original_list(*args, **kwargs)
        list_calls += 1
        if first_entered.is_set() and list_calls >= 3:
            scan_after_burst.set()
        return result

    monkeypatch.setattr(herdr_turns, "list_worker_bindings", observed_list)
    scheduler = TurnIngestionScheduler(config, refresh_interval_seconds=100, reader=reader)
    scheduler.start()
    assert first_entered.wait(2)
    for _ in range(20):
        scheduler.request_refresh()
    assert scan_after_burst.wait(2)
    release_first.set()
    assert second_entered.wait(2)
    _wait_until(lambda: scheduler.operational_status()["active"] == 0)
    scheduler.stop()

    assert calls == 2
    assert maximum_active == 1
    assert scheduler.operational_status()["coalesced"] >= 19


def test_scan_dispatches_reader_before_blocked_orphan_prune_completes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config, _snapshot, _bindings = _scheduler_store(tmp_path, 1)
    prune_entered = threading.Event()
    reader_entered = threading.Event()
    original_prune = herdr_turns.prune_backend_pending

    def observed_prune(*args, **kwargs):
        prune_entered.set()
        return original_prune(*args, **kwargs)

    def reader(_config, _binding, *, adapter_timeout_seconds):
        reader_entered.set()
        return TurnRefreshResult("unchanged", 0)

    monkeypatch.setattr(herdr_turns, "prune_backend_pending", observed_prune)
    writer = sqlite3.connect(config.db_path, isolation_level=None, timeout=1)
    writer.execute("BEGIN IMMEDIATE")
    scheduler = TurnIngestionScheduler(
        config,
        refresh_interval_seconds=100,
        max_workers=1,
        reader=reader,
    )
    scheduler.start()
    try:
        assert prune_entered.wait(2)
        assert reader_entered.wait(2)
    finally:
        writer.rollback()
        writer.close()
    _wait_until(lambda: scheduler.operational_status()["refreshed"] == 1)
    scheduler.stop()



def test_transient_initial_binding_scan_retries_once_and_dispatches(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config, _snapshot, _bindings = _scheduler_store(tmp_path, 1)
    original_list = herdr_turns.list_worker_bindings
    reader_entered = threading.Event()
    calls = 0
    lock = threading.Lock()

    def transient_list(*args, **kwargs):
        nonlocal calls
        with lock:
            calls += 1
            call = calls
        if call == 1:
            raise sqlite3.OperationalError("transient initial read")
        return original_list(*args, **kwargs)

    def reader(_config, _binding, *, adapter_timeout_seconds):
        reader_entered.set()
        return TurnRefreshResult("unchanged", 0)

    monkeypatch.setattr(herdr_turns, "list_worker_bindings", transient_list)
    scheduler = TurnIngestionScheduler(
        config,
        refresh_interval_seconds=100,
        max_workers=1,
        reader=reader,
    )
    scheduler.start()
    assert reader_entered.wait(2)
    _wait_until(lambda: scheduler.operational_status()["refreshed"] == 1)
    status = scheduler.operational_status()
    assert status["failed"] == 1
    assert status["status"] == "healthy"
    scheduler.stop()


def test_persistent_initial_binding_scan_failure_retries_once_without_spin(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config, _snapshot, bindings = _scheduler_store(tmp_path, 1)
    second_call = threading.Event()
    calls = 0
    lock = threading.Lock()

    prune_calls = 0

    apply_backend_pending_observation(
        config.db_path,
        config.host_id,
        bindings[0].worker_id,
        PendingObservation(
            "open_prompt",
            question="Must survive failed scan?",
            pending_kind="question",
            revision_digest="failed-scan-revision",
        ),
        binding_private_fingerprint=bindings[0].private_fingerprint,
        observed_turn_target_value=bindings[0].turn_target_value,
    )
    def failed_list(*_args, **_kwargs):
        nonlocal calls
        with lock:
            calls += 1
            if calls == 2:
                second_call.set()
        raise sqlite3.OperationalError("persistent read failure")

    def observed_prune(*_args, **_kwargs):
        nonlocal prune_calls
        prune_calls += 1
        return 0

    monkeypatch.setattr(herdr_turns, "list_worker_bindings", failed_list)
    monkeypatch.setattr(herdr_turns, "prune_backend_pending", observed_prune)
    scheduler = TurnIngestionScheduler(config, refresh_interval_seconds=100)
    scheduler.start()
    assert second_call.wait(2)
    _wait_until(lambda: scheduler.operational_status()["failed"] == 2)
    with scheduler._condition:
        assert calls == 2
        assert scheduler._scan_retry_remaining == 0
        assert scheduler._rescan_requested is False
    assert scheduler.operational_status()["status"] == "degraded"
    assert prune_calls == 0
    assert any(
        row["question"] == "Must survive failed scan?"
        for row in pending_payload_from_store(
            config.db_path,
            config.host_id,
        )["pending_interactions"]
    )
    scheduler.stop()


def test_transient_binding_revalidation_failure_retries_after_prune(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config, _snapshot, _bindings = _scheduler_store(tmp_path, 1)
    original_list = herdr_turns.list_worker_bindings
    original_prune = herdr_turns.prune_backend_pending
    validation_failed = threading.Event()
    prune_entered = threading.Event()
    release_prune = threading.Event()
    reader_entered = threading.Event()
    validation_calls = 0
    lock = threading.Lock()

    def transient_validation(*args, **kwargs):
        nonlocal validation_calls
        if threading.current_thread().name.startswith("tendwire-turn-ingestion"):
            with lock:
                validation_calls += 1
                call = validation_calls
            if call == 1:
                validation_failed.set()
                raise sqlite3.OperationalError("transient validation read")
        return original_list(*args, **kwargs)

    def blocked_prune(*args, **kwargs):
        prune_entered.set()
        assert release_prune.wait(5)
        return original_prune(*args, **kwargs)

    def reader(_config, _binding, *, adapter_timeout_seconds):
        reader_entered.set()
        return TurnRefreshResult("unchanged", 0)

    monkeypatch.setattr(herdr_turns, "list_worker_bindings", transient_validation)
    monkeypatch.setattr(herdr_turns, "prune_backend_pending", blocked_prune)
    scheduler = TurnIngestionScheduler(
        config,
        refresh_interval_seconds=100,
        max_workers=1,
        reader=reader,
    )
    scheduler.start()
    try:
        assert validation_failed.wait(2)
        assert prune_entered.wait(2)
        assert not reader_entered.is_set()
        release_prune.set()
        assert reader_entered.wait(2)
        _wait_until(lambda: scheduler.operational_status()["refreshed"] == 1)
    finally:
        release_prune.set()
        scheduler.stop()
    status = scheduler.operational_status()
    assert validation_calls == 2
    assert status["failed"] == 1


def test_persistent_binding_revalidation_failure_is_bounded_and_skips_reader(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config, _snapshot, _bindings = _scheduler_store(tmp_path, 1)
    original_list = herdr_turns.list_worker_bindings
    second_failure = threading.Event()
    reader_entered = threading.Event()
    validation_calls = 0
    lock = threading.Lock()

    def failed_validation(*args, **kwargs):
        nonlocal validation_calls
        if threading.current_thread().name.startswith("tendwire-turn-ingestion"):
            with lock:
                validation_calls += 1
                if validation_calls == 2:
                    second_failure.set()
            raise sqlite3.OperationalError("persistent validation read")
        return original_list(*args, **kwargs)

    def reader(_config, _binding, *, adapter_timeout_seconds):
        reader_entered.set()
        return TurnRefreshResult("unchanged", 0)

    monkeypatch.setattr(herdr_turns, "list_worker_bindings", failed_validation)
    scheduler = TurnIngestionScheduler(
        config,
        refresh_interval_seconds=100,
        max_workers=1,
        reader=reader,
    )
    scheduler.start()
    assert second_failure.wait(2)
    _wait_until(lambda: scheduler.operational_status()["failed"] == 2)
    with scheduler._condition:
        key = next(iter(scheduler._binding_retry_remaining))
        assert scheduler._binding_retry_remaining[key] == 0
        assert key not in scheduler._binding_retry_due
        assert validation_calls == 2
    assert not reader_entered.is_set()
    assert scheduler.operational_status()["status"] == "degraded"
    scheduler.stop()


def test_dirty_rerun_survives_full_queue_at_completion(tmp_path: Path, monkeypatch) -> None:
    config, _snapshot, _bindings = _scheduler_store(tmp_path, 3)
    a_entered = threading.Event()
    release_a = threading.Event()
    a_finished = threading.Event()
    b_entered = threading.Event()
    release_b = threading.Event()
    c_entered = threading.Event()
    release_c = threading.Event()
    third_prune_entered = threading.Event()
    release_third_prune = threading.Event()
    rerun_a_entered = threading.Event()
    calls: list[str] = []
    first_target: str | None = None
    b_target: str | None = None
    target_calls: dict[str, int] = {}
    lock = threading.Lock()

    def reader(_config, binding, *, adapter_timeout_seconds):
        nonlocal first_target, b_target
        target = str(binding.turn_target_value)
        with lock:
            calls.append(target)
            if first_target is None:
                first_target = target
            target_calls[target] = target_calls.get(target, 0) + 1
            current_target_call = target_calls[target]
            if target != first_target and b_target is None:
                b_target = target
            role = "a" if target == first_target else ("b" if target == b_target else "c")
        if role == "a" and current_target_call == 1:
            a_entered.set()
            assert release_a.wait(5)
            a_finished.set()
        elif role == "a":
            rerun_a_entered.set()
        elif role == "b":
            b_entered.set()
            assert release_b.wait(5)
        else:
            c_entered.set()
            assert release_c.wait(5)
        return TurnRefreshResult("unchanged", 0)

    prune_calls = 0
    prune_lock = threading.Lock()

    def block_third_prune(*_args, **_kwargs):
        nonlocal prune_calls
        with prune_lock:
            prune_calls += 1
            call = prune_calls
        if call == 3:
            third_prune_entered.set()
            assert release_third_prune.wait(5)
        return 0

    monkeypatch.setattr(herdr_turns, "prune_backend_pending", block_third_prune)
    scheduler = TurnIngestionScheduler(
        config,
        refresh_interval_seconds=100,
        max_workers=2,
        queue_capacity=1,
        reader=reader,
    )
    scheduler.start()
    try:
        assert a_entered.wait(2)
        scheduler.request_refresh()
        assert b_entered.wait(2)
        assert b_target is not None
        with sqlite3.connect(config.db_path) as conn:
            conn.execute(
                "DELETE FROM worker_bindings WHERE host_id = ? AND turn_target_value = ?",
                (config.host_id, b_target),
            )
        scheduler.request_refresh()
        assert third_prune_entered.wait(2)
        assert not c_entered.is_set()

        release_a.set()
        assert a_finished.wait(2)
        _wait_until(
            lambda: any(
                item.turn_target_value == first_target and future.done()
                for item, future, _started_at in scheduler._running.values()
            )
        )
        release_third_prune.set()
        assert c_entered.wait(2)
        release_b.set()
        release_c.set()
        assert rerun_a_entered.wait(2)
        _wait_until(lambda: scheduler.operational_status()["active"] == 0)
    finally:
        release_a.set()
        release_b.set()
        release_c.set()
        release_third_prune.set()
        scheduler.stop()

    assert first_target is not None
    assert calls.count(first_target) == 2
    assert len(calls) == 4
    assert scheduler.operational_status()["queue_full"] >= 2


def test_distinct_keys_use_four_workers_and_fifth_waits(tmp_path: Path) -> None:
    config, _snapshot, _bindings = _scheduler_store(tmp_path, 5)
    release = threading.Event()
    four_entered = threading.Event()
    fifth_entered = threading.Event()
    lock = threading.Lock()
    active = 0
    maximum_active = 0
    calls = 0

    def reader(_config, _binding, *, adapter_timeout_seconds):
        nonlocal active, maximum_active, calls
        with lock:
            calls += 1
            active += 1
            maximum_active = max(maximum_active, active)
            if calls == 4:
                four_entered.set()
            elif calls == 5:
                fifth_entered.set()
        assert release.wait(2)
        with lock:
            active -= 1
        return TurnRefreshResult("unchanged", 0)

    scheduler = TurnIngestionScheduler(
        config,
        refresh_interval_seconds=100,
        max_workers=4,
        reader=reader,
    )
    scheduler.start()
    assert four_entered.wait(2)
    assert not fifth_entered.is_set()
    status = scheduler.operational_status()
    assert status["active"] == 4
    assert status["queue_depth"] == 1
    release.set()
    assert fifth_entered.wait(2)
    _wait_until(lambda: scheduler.operational_status()["active"] == 0)
    scheduler.stop()
    assert maximum_active == 4


def test_queue_saturation_is_recovered_by_next_cadence_scan(tmp_path: Path) -> None:
    config, _snapshot, bindings = _scheduler_store(tmp_path, 4)

    class Clock:
        def __init__(self) -> None:
            self.value = 0.0
            self.lock = threading.Lock()

        def __call__(self) -> float:
            with self.lock:
                return self.value

        def advance(self, seconds: float) -> None:
            with self.lock:
                self.value += seconds

    clock = Clock()
    first_entered = threading.Event()
    release = threading.Event()
    all_seen = threading.Event()
    seen: set[str] = set()
    lock = threading.Lock()

    def reader(_config, binding, *, adapter_timeout_seconds):
        with lock:
            seen.add(binding.private_fingerprint)
            if len(seen) == len(bindings):
                all_seen.set()
        if not first_entered.is_set():
            first_entered.set()
            assert release.wait(2)
        return TurnRefreshResult("unchanged", 0)

    scheduler = TurnIngestionScheduler(
        config,
        refresh_interval_seconds=2,
        max_workers=1,
        queue_capacity=2,
        clock=clock,
        reader=reader,
    )
    scheduler.start()
    assert first_entered.wait(2)
    _wait_until(lambda: scheduler.operational_status()["queue_full"] >= 1)
    release.set()
    _wait_until(lambda: scheduler.operational_status()["active"] == 0)
    assert len(seen) == 2
    clock.advance(2.1)
    with scheduler._condition:
        scheduler._condition.notify_all()
    assert all_seen.wait(2)
    scheduler.stop()
    assert scheduler.operational_status()["queue_full"] >= 1


def test_target_change_discards_old_result_and_runs_latest_binding(tmp_path: Path, monkeypatch) -> None:
    config, snapshot, bindings = _scheduler_store(tmp_path, 1)
    old_binding = bindings[0]
    old_entered = threading.Event()
    release_old = threading.Event()
    new_entered = threading.Event()
    reads: list[str] = []

    def read_binding(_config, binding, *, timeout_seconds, cancel_event=None):
        target = str(binding.turn_target_value)
        reads.append(target)
        if target == "pane-0":
            old_entered.set()
            assert release_old.wait(2)
        else:
            new_entered.set()
        return {
            "source_turn_id": "source-stable",
            "user_text": "question",
            "assistant_final_text": target,
            "complete": True,
            "has_open_turn": False,
        }

    monkeypatch.setattr(herdr_turns, "_read_turn_for_binding", read_binding)
    original_list = herdr_turns.list_worker_bindings
    latest_scan = threading.Event()

    def observed_list(*args, **kwargs):
        result = original_list(*args, **kwargs)
        if old_entered.is_set() and any(item.turn_target_value == "pane-new" for item in result):
            latest_scan.set()
        return result

    monkeypatch.setattr(herdr_turns, "list_worker_bindings", observed_list)
    scheduler = TurnIngestionScheduler(config, refresh_interval_seconds=100, max_workers=1)
    scheduler.start()
    assert old_entered.wait(2)
    replacement = WorkerBinding(
        **{
            **old_binding.__dict__,
            "turn_target_value": "pane-new",
            "observed_at": "2026-07-12T00:00:01+00:00",
        }
    )
    upsert_worker_bindings(config.db_path, [replacement])
    scheduler.request_refresh()
    assert latest_scan.wait(2)
    release_old.set()
    assert new_entered.wait(2)
    _wait_until(lambda: scheduler.operational_status()["active"] == 0)
    scheduler.stop()


    turns = turns_payload_from_store(config.db_path, config.host_id, snapshot=snapshot)["turns"]
    assert any(turn.get("assistant_final_text") == "pane-new" for turn in turns)
    assert reads == ["pane-0", "pane-new"]
    assert scheduler.operational_status()["failed"] == 1


def test_atomic_binding_guard_closes_revalidation_commit_race(tmp_path: Path, monkeypatch) -> None:
    config, _snapshot, bindings = _scheduler_store(tmp_path, 1)
    original = bindings[0]
    with sqlite3.connect(config.db_path) as conn:
        baseline_revisions = conn.execute(
            "SELECT COUNT(*) FROM turn_content_revisions"
        ).fetchone()[0]
    replacement = WorkerBinding(
        **{
            **original.__dict__,
            "turn_target_value": "pane-replaced-after-check",
            "observed_at": "2026-07-12T00:00:02+00:00",
        }
    )
    monkeypatch.setattr(
        herdr_turns,
        "_read_turn_for_binding",
        lambda _config, _binding, *, timeout_seconds, cancel_event=None: {
            "source_turn_id": "must-not-commit",
            "user_text": "stale question",
            "assistant_final_text": "stale final",
            "complete": True,
            "has_open_turn": False,
        },
    )

    def race_after_revalidation(_config, _item):
        upsert_worker_bindings(config.db_path, [replacement])
        return True

    monkeypatch.setattr(herdr_turns, "_binding_still_matches", race_after_revalidation)
    result = herdr_turns.refresh_turn_binding(config, original)

    assert result == TurnRefreshResult("stale_binding", 0)
    with sqlite3.connect(config.db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM turn_content_revisions").fetchone()[0] == baseline_revisions
        assert conn.execute(
            "SELECT COUNT(*) FROM turns WHERE payload_json LIKE '%must-not-commit%'"
        ).fetchone()[0] == 0


def test_omp_compact_checkpoint_publishes_only_after_validated_durable_apply(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config, _snapshot, bindings = _scheduler_store(tmp_path, 1)
    root = tmp_path / "omp-sessions"
    session_dir = root / "-retry"
    session_dir.mkdir(parents=True)
    path = session_dir / "retry.jsonl"
    path.write_text(
        "\n".join(
            json.dumps(line, separators=(",", ":"))
            for line in (
                {
                    "type": "message",
                    "id": "retry-user",
                    "message": {
                        "role": "user",
                        "attribution": "user",
                        "content": [{"type": "text", "text": "retry prompt"}],
                    },
                },
                {
                    "type": "message",
                    "id": "retry-final",
                    "message": {
                        "role": "assistant",
                        "stopReason": "stop",
                        "content": [{"type": "text", "text": "durable final"}],
                    },
                },
            )
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("OMP_SESSIONS_DIR", str(root))
    original = bindings[0]
    omp_binding = WorkerBinding(
        **{
            **original.__dict__,
            "turn_target_kind": "omp_session_path",
            "turn_target_value": str(path),
            "private_fingerprint": "omp-retry-private",
        }
    )
    upsert_worker_bindings(config.db_path, [omp_binding])
    cache_key = herdr_turns._omp_cache_key(str(path))
    assert cache_key is not None
    with herdr_turns._OMP_SESSION_CACHE_LOCK:
        herdr_turns._OMP_SESSION_CACHE.clear()
        herdr_turns._OMP_SESSION_CACHE_LIVE_KEYS = None

    original_matches = herdr_turns._binding_still_matches
    monkeypatch.setattr(herdr_turns, "_binding_still_matches", lambda *_args: False)
    stale = herdr_turns._refresh_turn_binding(
        config,
        omp_binding,
        adapter_timeout_seconds=10,
    )
    assert stale == TurnRefreshResult("stale_binding", 0)
    with herdr_turns._OMP_SESSION_CACHE_LOCK:
        assert cache_key not in herdr_turns._OMP_SESSION_CACHE

    monkeypatch.setattr(herdr_turns, "_binding_still_matches", original_matches)
    original_apply = herdr_turns.apply_turn_refresh

    def fail_apply(*_args, **_kwargs):
        raise sqlite3.OperationalError("injected apply failure")

    monkeypatch.setattr(herdr_turns, "apply_turn_refresh", fail_apply)
    failed = herdr_turns._refresh_turn_binding(
        config,
        omp_binding,
        adapter_timeout_seconds=10,
    )
    assert failed == TurnRefreshResult("failed", 0)
    with herdr_turns._OMP_SESSION_CACHE_LOCK:
        assert cache_key not in herdr_turns._OMP_SESSION_CACHE

    monkeypatch.setattr(herdr_turns, "apply_turn_refresh", original_apply)
    applied = herdr_turns._refresh_turn_binding(
        config,
        omp_binding,
        adapter_timeout_seconds=10,
    )
    assert applied.status == "updated"
    with herdr_turns._OMP_SESSION_CACHE_LOCK:
        checkpoint = herdr_turns._serialize_omp_state(
            herdr_turns._OMP_SESSION_CACHE[cache_key]
        )
    assert checkpoint["turn_open"] is False
    assert "retry prompt" not in json.dumps(checkpoint)
    assert "durable final" not in json.dumps(checkpoint)

    unchanged = herdr_turns._refresh_turn_binding(
        config,
        omp_binding,
        adapter_timeout_seconds=10,
    )
    assert unchanged == TurnRefreshResult("unchanged", 0)


def test_codex_checkpoint_publication_requires_validated_durable_apply_and_cas(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config, snapshot, bindings = _scheduler_store(tmp_path, 1)
    session_id = "019f5590-3333-7333-8333-333333333333"
    home = tmp_path / "codex-publication"
    path = (
        home
        / "sessions"
        / "2026"
        / "07"
        / "12"
        / f"rollout-2026-07-12T00-00-00-{session_id}.jsonl"
    )
    path.parent.mkdir(parents=True)
    turn_id = "codex-durable-turn"
    records = (
        {
            "type": "event_msg",
            "payload": {"type": "task_started", "turn_id": turn_id},
        },
        {
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": "durable prompt"}],
                "internal_chat_message_metadata_passthrough": {"turn_id": turn_id},
            },
        },
        {
            "type": "event_msg",
            "payload": {
                "type": "task_complete",
                "turn_id": turn_id,
                "last_agent_message": "durable Codex final",
            },
        },
    )
    path.write_text(
        "\n".join(json.dumps(record, separators=(",", ":")) for record in records)
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("CODEX_HOME", str(home))
    original = bindings[0]
    binding = WorkerBinding(
        **{
            **original.__dict__,
            "turn_target_kind": "codex_session_id",
            "turn_target_value": session_id,
            "private_fingerprint": "codex-publication-private",
        }
    )
    upsert_worker_bindings(config.db_path, [binding])
    cache_key = (str((home / "sessions").resolve()), session_id)
    with herdr_turns._CODEX_PATH_CACHE_LOCK:
        herdr_turns._CODEX_PATH_CACHE.clear()
        herdr_turns._CODEX_INDEX_GENERATION = None
    with herdr_turns._CODEX_SESSION_CACHE_LOCK:
        herdr_turns._CODEX_SESSION_CACHE.clear()
        herdr_turns._CODEX_SESSION_CACHE_LIVE_KEYS = None

    original_matches = herdr_turns._binding_still_matches
    monkeypatch.setattr(herdr_turns, "_binding_still_matches", lambda *_args: False)
    assert herdr_turns._refresh_turn_binding(
        config,
        binding,
        adapter_timeout_seconds=10,
    ) == TurnRefreshResult("stale_binding", 0)
    with herdr_turns._CODEX_SESSION_CACHE_LOCK:
        assert cache_key not in herdr_turns._CODEX_SESSION_CACHE

    monkeypatch.setattr(herdr_turns, "_binding_still_matches", original_matches)
    original_apply = herdr_turns.apply_turn_refresh
    monkeypatch.setattr(
        herdr_turns,
        "apply_turn_refresh",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            sqlite3.OperationalError("injected Codex apply failure")
        ),
    )
    assert herdr_turns._refresh_turn_binding(
        config,
        binding,
        adapter_timeout_seconds=10,
    ) == TurnRefreshResult("failed", 0)
    with herdr_turns._CODEX_SESSION_CACHE_LOCK:
        assert cache_key not in herdr_turns._CODEX_SESSION_CACHE

    monkeypatch.setattr(herdr_turns, "apply_turn_refresh", original_apply)
    real_child = herdr_turns._file_turn_child
    monkeypatch.setattr(herdr_turns, "_file_turn_child", _wrong_source_codex_child)
    assert herdr_turns._refresh_turn_binding(
        config,
        binding,
        adapter_timeout_seconds=10,
    ) == TurnRefreshResult("failed", 0)
    with herdr_turns._CODEX_SESSION_CACHE_LOCK:
        assert cache_key not in herdr_turns._CODEX_SESSION_CACHE

    monkeypatch.setattr(herdr_turns, "_file_turn_child", _blocked_codex_child)
    before_children = {child.pid for child in multiprocessing.active_children()}
    assert herdr_turns._refresh_turn_binding(
        config,
        binding,
        adapter_timeout_seconds=0.05,
    ) == TurnRefreshResult("timeout", 0)
    assert {child.pid for child in multiprocessing.active_children()} == before_children
    with herdr_turns._CODEX_SESSION_CACHE_LOCK:
        assert cache_key not in herdr_turns._CODEX_SESSION_CACHE

    monkeypatch.setattr(herdr_turns, "_file_turn_child", real_child)
    real_commit = herdr_turns._file_publication_commit
    monkeypatch.setattr(
        herdr_turns,
        "_file_publication_commit",
        lambda _publication, content: content,
    )
    applied_without_checkpoint = herdr_turns._refresh_turn_binding(
        config,
        binding,
        adapter_timeout_seconds=10,
    )
    assert applied_without_checkpoint.status == "updated"
    with herdr_turns._CODEX_SESSION_CACHE_LOCK:
        assert cache_key not in herdr_turns._CODEX_SESSION_CACHE
    payload = turns_payload_from_store(
        config.db_path,
        config.host_id,
        snapshot=snapshot,
    )
    assert payload["turns"][0]["assistant_final_text"] == "durable Codex final"

    monkeypatch.setattr(herdr_turns, "_file_publication_commit", real_commit)
    retry = herdr_turns._refresh_turn_binding(
        config,
        binding,
        adapter_timeout_seconds=10,
    )
    assert retry.status == "unchanged"
    with herdr_turns._CODEX_SESSION_CACHE_LOCK:
        checkpoint = herdr_turns._serialize_codex_state(
            herdr_turns._CODEX_SESSION_CACHE[cache_key]
        )
    assert "durable prompt" not in json.dumps(checkpoint)
    assert "durable Codex final" not in json.dumps(checkpoint)
    assert herdr_turns._refresh_turn_binding(
        config,
        binding,
        adapter_timeout_seconds=10,
    ) == TurnRefreshResult("unchanged", 0)


def _codex_lifecycle_file(tmp_path: Path, monkeypatch, session_id: str) -> None:
    home = tmp_path / "codex-lifecycle"
    path = (
        home
        / "sessions"
        / "2026"
        / "07"
        / "12"
        / f"rollout-2026-07-12T00-00-00-{session_id}.jsonl"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"")
    monkeypatch.setenv("CODEX_HOME", str(home))
    with herdr_turns._CODEX_PATH_CACHE_LOCK:
        herdr_turns._CODEX_PATH_CACHE.clear()
        herdr_turns._CODEX_INDEX_GENERATION = None
def test_direct_omp_first_frame_over_chunk_bound_is_rejected_and_reaped(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "omp-direct-frame"
    path = root / "-session" / "session.jsonl"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"")
    monkeypatch.setenv("OMP_SESSIONS_DIR", str(root))
    monkeypatch.setattr(
        herdr_turns,
        "_file_turn_child",
        _oversized_direct_omp_child,
    )
    before_children = {child.pid for child in multiprocessing.active_children()}

    try:
        herdr_turns._read_file_turn_isolated(
            "omp_session_path",
            str(path),
            timeout_seconds=5,
        )
    except herdr_turns._TurnReadFailed:
        pass
    else:
        raise AssertionError("oversized direct OMP frame was accepted")
    assert {child.pid for child in multiprocessing.active_children()} == before_children


def test_codex_direct_frame_can_exceed_omp_chunk_bound(
    tmp_path: Path,
    monkeypatch,
) -> None:
    session_id = "019f5590-4444-7444-8444-444444444444"
    _codex_lifecycle_file(tmp_path, monkeypatch, session_id)
    monkeypatch.setattr(
        herdr_turns,
        "_file_turn_child",
        _large_direct_codex_child,
    )

    observed = herdr_turns._read_file_turn_isolated(
        "codex_session_id",
        session_id,
        timeout_seconds=5,
        defer_cache=True,
    )

    assert isinstance(observed, herdr_turns._ObservedFileTurn)
    final = observed.content["assistant_final_text"]
    assert len(final) > herdr_turns._OMP_IPC_RESPONSE_CHUNK_BYTES
    assert len(final) < herdr_turns._CODEX_IPC_FRAME_MAX_BYTES




def test_oversized_isolated_request_is_rejected_without_process_or_helper(
    monkeypatch,
) -> None:
    before_children = {child.pid for child in multiprocessing.active_children()}
    before_threads = {thread.ident for thread in threading.enumerate()}
    started = time.monotonic()
    try:
        herdr_turns._read_file_turn_isolated(
            "codex_session_id",
            "x" * (8 * 1024 * 1024),
            timeout_seconds=0.05,
        )
    except herdr_turns._TurnReadFailed:
        pass
    else:
        raise AssertionError("oversized private request was accepted")
    elapsed = time.monotonic() - started

    assert elapsed < 0.2
    assert {child.pid for child in multiprocessing.active_children()} == before_children
    assert {thread.ident for thread in threading.enumerate()} == before_threads


def test_isolated_process_construction_failure_closes_both_socket_fds(
    tmp_path: Path,
    monkeypatch,
) -> None:
    session_id = "019f5590-1111-7111-8111-111111111111"
    _codex_lifecycle_file(tmp_path, monkeypatch, session_id)
    real_socketpair = herdr_turns.socket.socketpair
    opened = []

    def tracked_socketpair():
        pair = real_socketpair()
        opened.extend(pair)
        return pair

    real_context = multiprocessing.get_context("spawn")

    class FailingContext:
        def Process(self, **_kwargs):
            raise RuntimeError("injected process construction failure")

    monkeypatch.setattr(herdr_turns.socket, "socketpair", tracked_socketpair)
    monkeypatch.setattr(
        herdr_turns.multiprocessing,
        "get_context",
        lambda _method: FailingContext(),
    )
    try:
        herdr_turns._read_file_turn_isolated(
            "codex_session_id",
            session_id,
            timeout_seconds=0.1,
        )
    except RuntimeError:
        pass
    else:
        raise AssertionError("process construction failure was hidden")

    assert len(opened) == 2
    assert all(channel.fileno() == -1 for channel in opened)
    assert not real_context.active_children()


def test_isolated_start_delay_times_out_then_reaps_with_bounded_grace(
    tmp_path: Path,
    monkeypatch,
) -> None:
    session_id = "019f5590-2222-7222-8222-222222222222"
    _codex_lifecycle_file(tmp_path, monkeypatch, session_id)
    context = multiprocessing.get_context("spawn")
    process_type = type(context.Process())
    original_start = process_type.start
    original_deadline_check = herdr_turns._check_ipc_deadline
    start_entered = False

    def delayed_start(process):
        nonlocal start_entered
        start_entered = True
        time.sleep(0.08)
        return original_start(process)

    def deadline_after_start(deadline, cancel_event):
        if start_entered:
            original_deadline_check(deadline, cancel_event)

    monkeypatch.setattr(process_type, "start", delayed_start)
    monkeypatch.setattr(
        herdr_turns,
        "_check_ipc_deadline",
        deadline_after_start,
    )
    before_children = {child.pid for child in multiprocessing.active_children()}
    started = time.monotonic()
    try:
        herdr_turns._read_file_turn_isolated(
            "codex_session_id",
            session_id,
            timeout_seconds=0.02,
        )
    except herdr_turns._TurnReadTimeout:
        pass
    else:
        raise AssertionError("delayed process start ignored the request deadline")
    elapsed = time.monotonic() - started

    assert elapsed >= 0.08
    assert elapsed < 0.08 + herdr_turns._OMP_TEARDOWN_GRACE_SECONDS + 0.25
    assert {child.pid for child in multiprocessing.active_children()} == before_children
    assert not any(
        thread.name == "tendwire-turn-ipc"
        for thread in threading.enumerate()
    )


def test_pathological_reap_never_uses_unbounded_join() -> None:
    class NeverReaped:
        pid = 123

        def __init__(self):
            self.joins = []
            self.kills = 0
            self.terminates = 0

        def is_alive(self):
            return True

        def join(self, timeout=None):
            self.joins.append(timeout)

        def terminate(self):
            self.terminates += 1

        def kill(self):
            self.kills += 1

    process = NeverReaped()
    started = time.monotonic()
    herdr_turns._terminate_and_reap(process)
    elapsed = time.monotonic() - started

    assert process.terminates == 1
    assert process.kills == 2
    assert process.joins
    assert all(timeout is not None and timeout >= 0 for timeout in process.joins)
    assert elapsed < herdr_turns._OMP_TEARDOWN_GRACE_SECONDS




def test_back_to_back_frames_are_received_without_trailing_byte_loss() -> None:
    sender, receiver = herdr_turns.socket.socketpair()
    try:
        payloads = (b"first-frame", b"second-frame")
        sender.sendall(
            b"".join(
                herdr_turns._OMP_FRAME_HEADER.pack(len(payload)) + payload
                for payload in payloads
            )
        )
        receiver.setblocking(False)
        deadline = time.monotonic() + 1
        assert herdr_turns._recv_frame_until(receiver, deadline, None, 1024) == payloads[0]
        assert herdr_turns._recv_frame_until(receiver, deadline, None, 1024) == payloads[1]
    finally:
        sender.close()
        receiver.close()


def test_streamed_omp_response_reassembles_small_coalesced_chunks_without_leaks() -> None:
    sender, receiver = herdr_turns.socket.socketpair()
    receiver.setblocking(False)
    payload = (b"chunked-private-response-" * 257) + b"end"
    nonce = "stream-nonce"
    failures = []

    def send() -> None:
        try:
            herdr_turns._blocking_send_streamed_omp_response(
                sender,
                payload,
                nonce,
                chunk_bytes=7,
            )
        except BaseException as exc:
            failures.append(exc)
        finally:
            sender.close()

    thread = threading.Thread(target=send, name="test-omp-stream-sender")
    thread.start()
    try:
        deadline = time.monotonic() + 5
        first = herdr_turns._recv_frame_until(receiver, deadline, None, 1024)
        assembled = herdr_turns._recv_streamed_omp_response_until(
            receiver,
            first,
            nonce,
            "omp_session_path",
            deadline,
            None,
        )
    finally:
        receiver.close()
        thread.join(5)
    assert assembled == payload
    assert failures == []
    assert thread.is_alive() is False

def test_stream_manifest_rejects_chunk_bound_above_one_mib() -> None:
    sender, receiver = herdr_turns.socket.socketpair()
    receiver.setblocking(False)
    nonce = "oversized-chunk-manifest"
    chunk_bytes = herdr_turns._OMP_IPC_RESPONSE_CHUNK_BYTES + 1
    try:
        herdr_turns._blocking_send_streamed_omp_response(
            sender,
            b"x",
            nonce,
            chunk_bytes=chunk_bytes,
        )
    except ValueError:
        pass
    else:
        raise AssertionError("oversized OMP sender chunk bound was accepted")
    manifest = json.dumps(
        {
            "protocol": 1,
            "nonce": nonce,
            "stream": "omp_response",
            "chunks": 1,
            "total_bytes": 1,
            "chunk_bytes": chunk_bytes,
        },
        separators=(",", ":"),
    ).encode("utf-8")
    herdr_turns._blocking_send_frame(sender, manifest)
    sender.close()
    try:
        deadline = time.monotonic() + 1
        first = herdr_turns._recv_frame_until(
            receiver,
            deadline,
            None,
            herdr_turns._OMP_IPC_RESPONSE_CHUNK_BYTES,
        )
        try:
            herdr_turns._recv_streamed_omp_response_until(
                receiver,
                first,
                nonce,
                "omp_session_path",
                deadline,
                None,
            )
        except herdr_turns._TurnReadFailed:
            pass
        else:
            raise AssertionError("oversized OMP chunk manifest was accepted")
    finally:
        receiver.close()


def test_streamed_omp_response_rejects_extra_frame_after_terminator() -> None:
    sender, receiver = herdr_turns.socket.socketpair()
    receiver.setblocking(False)
    nonce = "extra-frame-nonce"
    manifest = json.dumps(
        {
            "protocol": 1,
            "nonce": nonce,
            "stream": "omp_response",
            "chunks": 1,
            "total_bytes": 3,
            "chunk_bytes": 3,
        },
        separators=(",", ":"),
    ).encode("utf-8")
    end = json.dumps(
        {"protocol": 1, "nonce": nonce, "stream": "omp_response_end"},
        separators=(",", ":"),
    ).encode("utf-8")
    frames = (manifest, b"abc", end, b"unexpected")
    sender.sendall(
        b"".join(
            herdr_turns._OMP_FRAME_HEADER.pack(len(payload)) + payload
            for payload in frames
        )
    )
    sender.close()
    try:
        deadline = time.monotonic() + 1
        first = herdr_turns._recv_frame_until(receiver, deadline, None, 1024)
        try:
            herdr_turns._recv_streamed_omp_response_until(
                receiver,
                first,
                nonce,
                "omp_session_path",
                deadline,
                None,
            )
        except herdr_turns._TurnReadFailed:
            pass
        else:
            raise AssertionError("extra streamed IPC frame was accepted")
    finally:
        receiver.close()



def test_file_adapter_timeout_kills_reaps_and_leaves_no_ipc_threads(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "sessions"
    root.mkdir()
    fifo = root / "blocked.jsonl"
    os.mkfifo(fifo)
    monkeypatch.setenv("OMP_SESSIONS_DIR", str(root))
    before_children = {child.pid for child in multiprocessing.active_children()}
    before_threads = {thread.ident for thread in threading.enumerate() if thread.name == "tendwire-turn-ipc"}
    started = time.monotonic()

    for _ in range(3):
        try:
            herdr_turns._read_file_turn_isolated(
                "omp_session_path",
                str(fifo),
                timeout_seconds=0.1,
            )
        except herdr_turns._TurnReadTimeout:
            pass
        else:
            raise AssertionError("blocked file reader did not time out")
    elapsed = time.monotonic() - started

    assert {child.pid for child in multiprocessing.active_children()} == before_children
    assert {thread.ident for thread in threading.enumerate() if thread.name == "tendwire-turn-ipc"} == before_threads
    assert elapsed < 2.0


def test_stop_terminates_and_reaps_active_pane_adapter(tmp_path: Path, monkeypatch) -> None:
    base_config, _snapshot, _bindings = _scheduler_store(tmp_path, 1)
    pid_file = tmp_path / "adapter.pid"
    adapter = tmp_path / "blocked_adapter.py"
    adapter.write_text(
        "#!/usr/bin/env python3\n"
        "import os, time\n"
        "with open(os.environ['TENDWIRE_TEST_ADAPTER_PID'], 'w') as handle:\n"
        "    handle.write(str(os.getpid()))\n"
        "time.sleep(30)\n",
        encoding="utf-8",
    )
    adapter.chmod(0o700)
    monkeypatch.setenv("TENDWIRE_TEST_ADAPTER_PID", str(pid_file))
    config = Config(
        host_id=base_config.host_id,
        db_path=base_config.db_path,
        herdr_bin=str(adapter),
        herdr_timeout_seconds=10,
        turn_refresh_interval_seconds=100,
        turn_refresh_workers=1,
    )
    scheduler = TurnIngestionScheduler(config)
    scheduler.start()
    _wait_until(
        lambda: pid_file.exists()
        and bool(pid_file.read_text(encoding="utf-8").strip())
    )
    pid = int(pid_file.read_text(encoding="utf-8"))
    started = time.monotonic()
    scheduler.stop(flush_timeout_seconds=1)
    elapsed = time.monotonic() - started

    assert elapsed < 1
    assert scheduler.operational_status()["active"] == 0
    assert not any(
        thread.name.startswith("tendwire-turn-ingestion")
        for thread in threading.enumerate()
    )
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        pass
    else:
        raise AssertionError("pane adapter child was not reaped")


def test_stop_rejects_new_refresh_and_boundedly_drains_started_work(tmp_path: Path) -> None:
    config, _snapshot, _bindings = _scheduler_store(tmp_path, 1)
    entered = threading.Event()
    release = threading.Event()
    finished = threading.Event()
    calls = 0

    def reader(_config, _binding, *, adapter_timeout_seconds):
        nonlocal calls
        calls += 1
        entered.set()
        assert release.wait(2)
        finished.set()
        return TurnRefreshResult("unchanged", 0)

    scheduler = TurnIngestionScheduler(config, refresh_interval_seconds=100, reader=reader)
    scheduler.start()
    assert entered.wait(2)
    stopper = threading.Thread(
        target=lambda: scheduler.stop(flush_timeout_seconds=0.5),
        name="test-scheduler-stopper",
    )
    started = time.monotonic()
    stopper.start()
    _wait_until(lambda: scheduler.operational_status()["status"] == "stopping")
    scheduler.request_refresh()
    release.set()
    stopper.join(1)
    assert not stopper.is_alive()
    assert time.monotonic() - started < 1
    assert finished.wait(1)
    assert calls == 1
    assert scheduler.operational_status()["queue_depth"] == 0


def test_direct_fallback_actively_feeds_only_worker_bound(tmp_path: Path, monkeypatch) -> None:
    config, _snapshot, bindings = _scheduler_store(tmp_path, 5)
    release = threading.Event()
    two_entered = threading.Event()
    lock = threading.Lock()
    calls: list[str] = []
    active = 0
    maximum_active = 0

    def reader(
        _config,
        binding,
        *,
        adapter_timeout_seconds,
        cancel_event=None,
        apply_deadline_monotonic=None,
    ):
        nonlocal active, maximum_active
        with lock:
            calls.append(binding.private_fingerprint)
            active += 1
            maximum_active = max(maximum_active, active)
            if len(calls) == 2:
                two_entered.set()
        assert release.wait(2)
        with lock:
            active -= 1
        return TurnRefreshResult("updated", 1)

    monkeypatch.setattr(herdr_turns, "_refresh_turn_binding", reader)
    returned: list[dict[str, Any]] = []
    fallback = threading.Thread(
        target=lambda: returned.append(
            herdr_turns.refresh_structured_turn_content(
                config,
                max_workers=2,
                total_timeout_seconds=2,
            )
        ),
        name="test-turn-fallback",
    )
    fallback.start()
    assert two_entered.wait(2)
    assert len(calls) == 2
    release.set()
    fallback.join(2)
    assert not fallback.is_alive()
    assert returned == [{"ok": True, "status": "ok", "updated": 5, "attempted": 5}]
    assert len(calls) == len(bindings)
    assert len(set(calls)) == len(bindings)
    assert maximum_active == 2


def test_direct_fallback_total_deadline_stops_feeding_new_work(tmp_path: Path, monkeypatch) -> None:
    config, _snapshot, _bindings = _scheduler_store(tmp_path, 5)
    calls = 0
    lock = threading.Lock()

    def reader(
        _config,
        _binding,
        *,
        adapter_timeout_seconds,
        cancel_event=None,
        apply_deadline_monotonic=None,
    ):
        nonlocal calls
        with lock:
            calls += 1
        threading.Event().wait(adapter_timeout_seconds)
        return TurnRefreshResult("timeout", 0)

    monkeypatch.setattr(herdr_turns, "_refresh_turn_binding", reader)
    started = time.monotonic()
    result = herdr_turns.refresh_structured_turn_content(
        config,
        max_workers=2,
        total_timeout_seconds=1,
    )
    assert result == {
        "ok": False,
        "status": "deadline_exceeded",
        "updated": 0,
        "attempted": 2,
    }
    assert calls == 2
    assert time.monotonic() - started < 1
    _wait_until(
        lambda: not any(
            thread.name.startswith("tendwire-turn-fallback")
            for thread in threading.enumerate()
        )
    )


def test_direct_fallback_deadline_reaps_real_pane_child(tmp_path: Path, monkeypatch) -> None:
    base_config, _snapshot, _bindings = _scheduler_store(tmp_path, 3)
    pid_file = tmp_path / "fallback-adapter.pid"
    adapter = tmp_path / "fallback_blocked_adapter.py"
    adapter.write_text(
        "#!/usr/bin/env python3\n"
        "import os, time\n"
        "with open(os.environ['TENDWIRE_TEST_FALLBACK_PID'], 'w') as handle:\n"
        "    handle.write(str(os.getpid()))\n"
        "time.sleep(30)\n",
        encoding="utf-8",
    )
    adapter.chmod(0o700)
    monkeypatch.setenv("TENDWIRE_TEST_FALLBACK_PID", str(pid_file))
    config = Config(
        host_id=base_config.host_id,
        db_path=base_config.db_path,
        herdr_bin=str(adapter),
        herdr_timeout_seconds=10,
        turn_refresh_interval_seconds=100,
        turn_refresh_workers=1,
    )

    started = time.monotonic()
    result = herdr_turns.refresh_structured_turn_content(
        config,
        max_workers=1,
        total_timeout_seconds=1,
    )
    elapsed = time.monotonic() - started

    assert result == {
        "ok": False,
        "status": "deadline_exceeded",
        "updated": 0,
        "attempted": 1,
    }
    assert elapsed < 1
    assert pid_file.exists()
    pid = int(pid_file.read_text(encoding="utf-8"))
    _wait_until(
        lambda: not any(
            thread.name.startswith("tendwire-turn-fallback")
            for thread in threading.enumerate()
        )
    )
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        pass
    else:
        raise AssertionError("fallback pane child was not reaped")


def test_fallback_deadline_cancels_blocked_store_apply_without_late_commit(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config, _snapshot, _bindings = _scheduler_store(tmp_path, 3)
    content = {
        "source_turn_id": "blocked-fallback-source",
        "user_text": "must not commit",
        "assistant_final_text": "must not commit",
        "complete": True,
        "has_open_turn": False,
    }
    read_done = threading.Event()

    def read_now(_config, _binding, *, timeout_seconds, cancel_event=None):
        read_done.set()
        return content

    monkeypatch.setattr(herdr_turns, "_read_turn_for_binding", read_now)
    with sqlite3.connect(config.db_path) as conn:
        baseline = conn.execute("SELECT COUNT(*) FROM turn_content_revisions").fetchone()[0]
    blocker = sqlite3.connect(config.db_path, isolation_level=None)
    blocker.execute("BEGIN IMMEDIATE")
    returned: list[dict[str, Any]] = []
    fallback = threading.Thread(
        target=lambda: returned.append(
            herdr_turns.refresh_structured_turn_content(
                config,
                max_workers=1,
                total_timeout_seconds=1,
            )
        ),
        name="test-locked-fallback",
    )
    started = time.monotonic()
    fallback.start()
    assert read_done.wait(1)
    fallback.join(1.1)
    elapsed = time.monotonic() - started
    assert not fallback.is_alive()
    assert elapsed < 1.1
    assert returned[0]["status"] == "deadline_exceeded"
    blocker.rollback()
    blocker.close()
    _wait_until(
        lambda: not any(
            thread.name.startswith("tendwire-turn-fallback")
            for thread in threading.enumerate()
        )
    )
    with sqlite3.connect(config.db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM turn_content_revisions").fetchone()[0] == baseline
        assert conn.execute(
            "SELECT COUNT(*) FROM turns WHERE payload_json LIKE '%blocked-fallback-source%'"
        ).fetchone()[0] == 0




def test_fallback_prune_obeys_total_deadline_without_late_delete(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config, _snapshot, _bindings = _scheduler_store(tmp_path, 1)
    with sqlite3.connect(config.db_path) as conn:
        conn.execute(
            """
            INSERT INTO backend_pending (host_id, worker_id, payload_json, observed_at)
            VALUES (?, ?, ?, ?)
            """,
            (
                config.host_id,
                "orphan-worker",
                '{"prompt":"must survive"}',
                "2026-07-12T00:00:00+00:00",
            ),
        )
    job_finished = threading.Event()

    def reader(
        _config,
        _binding,
        *,
        adapter_timeout_seconds,
        cancel_event=None,
        apply_deadline_monotonic=None,
    ):
        job_finished.set()
        return TurnRefreshResult("unchanged", 0)

    monkeypatch.setattr(herdr_turns, "_refresh_turn_binding", reader)
    blocker = sqlite3.connect(config.db_path, isolation_level=None)
    blocker.execute("BEGIN IMMEDIATE")
    started = time.monotonic()
    result = herdr_turns.refresh_structured_turn_content(
        config,
        max_workers=1,
        total_timeout_seconds=0.4,
    )
    elapsed = time.monotonic() - started

    assert job_finished.is_set()
    assert result == {
        "ok": False,
        "status": "deadline_exceeded",
        "updated": 0,
        "attempted": 1,
    }
    assert elapsed < 0.7
    blocker.rollback()
    blocker.close()
    threading.Event().wait(0.2)
    with sqlite3.connect(config.db_path) as conn:
        assert conn.execute(
            """
            SELECT COUNT(*) FROM backend_pending
            WHERE host_id = ? AND worker_id = ?
            """,
            (config.host_id, "orphan-worker"),
        ).fetchone()[0] == 1
    _wait_until(
        lambda: not any(
            thread.name.startswith("tendwire-turn-fallback")
            for thread in threading.enumerate()
        )
    )


def test_scheduler_stop_cancels_blocked_store_apply_without_late_commit(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config, _snapshot, _bindings = _scheduler_store(tmp_path, 1)
    read_done = threading.Event()

    def read_now(_config, _binding, *, timeout_seconds, cancel_event=None):
        read_done.set()
        return {
            "source_turn_id": "blocked-scheduler-source",
            "user_text": "must not commit",
            "assistant_final_text": "must not commit",
            "complete": True,
            "has_open_turn": False,
        }

    monkeypatch.setattr(herdr_turns, "_read_turn_for_binding", read_now)
    monkeypatch.setattr(herdr_turns, "prune_backend_pending", lambda *args, **kwargs: 0)
    with sqlite3.connect(config.db_path) as conn:
        baseline = conn.execute("SELECT COUNT(*) FROM turn_content_revisions").fetchone()[0]
    blocker = sqlite3.connect(config.db_path, isolation_level=None)
    blocker.execute("BEGIN IMMEDIATE")
    scheduler = TurnIngestionScheduler(config, refresh_interval_seconds=100, max_workers=1)
    scheduler.start()
    assert read_done.wait(1)
    started = time.monotonic()
    scheduler.stop(flush_timeout_seconds=0.5)
    elapsed = time.monotonic() - started
    assert elapsed < 0.5
    assert scheduler.operational_status()["active"] == 0
    blocker.rollback()
    blocker.close()
    with sqlite3.connect(config.db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM turn_content_revisions").fetchone()[0] == baseline
        assert conn.execute(
            "SELECT COUNT(*) FROM turns WHERE payload_json LIKE '%blocked-scheduler-source%'"
        ).fetchone()[0] == 0


def test_scheduler_restart_with_identical_final_is_revision_noop(tmp_path: Path, monkeypatch) -> None:
    config, _snapshot, _bindings = _scheduler_store(tmp_path, 1)
    content = {
        "source_turn_id": "restart-source",
        "user_text": "same prompt",
        "assistant_final_text": "same final",
        "complete": True,
        "has_open_turn": False,
    }
    monkeypatch.setattr(
        herdr_turns,
        "_read_turn_for_binding",
        lambda _config, _binding, *, timeout_seconds, cancel_event=None: content,
    )

    def run_once() -> None:
        scheduler = TurnIngestionScheduler(config, refresh_interval_seconds=100, max_workers=1)
        scheduler.start()
        _wait_until(lambda: scheduler.operational_status()["refreshed"] == 1)
        scheduler.stop()

    run_once()
    with sqlite3.connect(config.db_path) as conn:
        first = conn.execute("SELECT COUNT(*) FROM turn_content_revisions").fetchone()[0]
    run_once()
    with sqlite3.connect(config.db_path) as conn:
        second = conn.execute("SELECT COUNT(*) FROM turn_content_revisions").fetchone()[0]
    assert second == first


def test_operational_status_recovers_after_successful_empty_binding_scan(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config, _snapshot, _bindings = _scheduler_store(tmp_path, 0)
    stale_cache_key = "disappeared-omp-binding"
    with herdr_turns._OMP_SESSION_CACHE_LOCK:
        herdr_turns._OMP_SESSION_CACHE.clear()
        herdr_turns._omp_cache_store_locked(
            stale_cache_key,
            herdr_turns._OmpSessionState(offset=1, file_id=(1, 1)),
        )
    original_list = herdr_turns.list_worker_bindings
    allow_success = threading.Event()
    successful_scan = threading.Event()

    def flaky_list(*args, **kwargs):
        if not allow_success.is_set():
            raise sqlite3.OperationalError("deterministic scan failure")
        bindings = original_list(*args, **kwargs)
        successful_scan.set()
        return bindings

    monkeypatch.setattr(herdr_turns, "list_worker_bindings", flaky_list)
    scheduler = TurnIngestionScheduler(config, refresh_interval_seconds=100)
    scheduler.start()
    _wait_until(lambda: scheduler.operational_status()["failed"] == 1)
    assert scheduler.operational_status()["status"] == "degraded"
    with herdr_turns._OMP_SESSION_CACHE_LOCK:
        assert stale_cache_key in herdr_turns._OMP_SESSION_CACHE

    allow_success.set()
    scheduler.request_refresh()
    assert successful_scan.wait(2)
    _wait_until(lambda: scheduler.operational_status()["status"] == "stale")
    recovered = scheduler.operational_status()
    assert recovered["failed"] == 1
    assert recovered["refreshed"] == 0
    assert recovered["timed_out"] == 0
    with herdr_turns._OMP_SESSION_CACHE_LOCK:
        assert stale_cache_key not in herdr_turns._OMP_SESSION_CACHE
    scheduler.stop()


def test_fallback_successful_empty_scan_prunes_disappeared_omp_cache(
    tmp_path: Path,
) -> None:
    config, _snapshot, _bindings = _scheduler_store(tmp_path, 0)
    with herdr_turns._OMP_SESSION_CACHE_LOCK:
        herdr_turns._OMP_SESSION_CACHE.clear()
        herdr_turns._omp_cache_store_locked(
            "fallback-disappeared-omp",
            herdr_turns._OmpSessionState(offset=1, file_id=(1, 1)),
        )

    result = herdr_turns.refresh_structured_turn_content(config)

    assert result == {"ok": True, "status": "ok", "updated": 0, "attempted": 0}
    with herdr_turns._OMP_SESSION_CACHE_LOCK:
        assert not herdr_turns._OMP_SESSION_CACHE


def test_operational_status_recovers_from_completed_failures_and_stale_churn(
    tmp_path: Path,
) -> None:
    config, _snapshot, _bindings = _scheduler_store(tmp_path, 1)
    outcomes = [
        "failed",
        "unchanged",
        "stale_binding",
        "updated",
        "timeout",
        "unchanged",
        "blocked",
    ]
    blocked_entered = threading.Event()
    release_blocked = threading.Event()
    calls = 0
    lock = threading.Lock()

    def reader(_config, _binding, *, adapter_timeout_seconds):
        nonlocal calls
        with lock:
            outcome = outcomes[calls]
            calls += 1
        if outcome == "blocked":
            blocked_entered.set()
            assert release_blocked.wait(2)
            outcome = "unchanged"
        return TurnRefreshResult(outcome, 1 if outcome == "updated" else 0)

    scheduler = TurnIngestionScheduler(
        config,
        refresh_interval_seconds=100,
        max_workers=1,
        reader=reader,
    )
    scheduler.start()
    _wait_until(lambda: scheduler.operational_status()["failed"] == 1)
    failed_now = scheduler.operational_status()
    assert failed_now["status"] == "degraded"
    assert failed_now["refreshed"] == 0
    assert failed_now["timed_out"] == 0

    scheduler.request_refresh()
    _wait_until(lambda: scheduler.operational_status()["refreshed"] == 1)
    recovered = scheduler.operational_status()
    assert recovered["status"] == "healthy"
    assert recovered["failed"] == 1

    scheduler.request_refresh()
    _wait_until(lambda: scheduler.operational_status()["failed"] == 2)
    stale_churn = scheduler.operational_status()
    assert stale_churn["status"] == "healthy"
    assert stale_churn["refreshed"] == 1

    scheduler.request_refresh()
    _wait_until(lambda: scheduler.operational_status()["refreshed"] == 2)
    assert scheduler.operational_status()["status"] == "healthy"

    scheduler.request_refresh()
    _wait_until(lambda: scheduler.operational_status()["timed_out"] == 1)
    current_timeout = scheduler.operational_status()
    assert current_timeout["status"] == "degraded"
    assert current_timeout["failed"] == 2
    assert current_timeout["refreshed"] == 2

    scheduler.request_refresh()
    _wait_until(lambda: scheduler.operational_status()["refreshed"] == 3)
    assert scheduler.operational_status()["status"] == "healthy"

    scheduler.request_refresh()
    assert blocked_entered.wait(2)
    active_fresh = scheduler.operational_status()
    assert active_fresh["active"] == 1
    assert active_fresh["status"] == "healthy"
    release_blocked.set()
    _wait_until(lambda: scheduler.operational_status()["refreshed"] == 4)
    final = scheduler.operational_status()
    assert final["status"] == "healthy"
    assert final["failed"] == 2
    assert final["timed_out"] == 1
    assert final["refreshed"] == 4
    scheduler.stop()


def test_operational_status_has_only_fixed_aggregate_fields(tmp_path: Path) -> None:
    config, _snapshot, _bindings = _scheduler_store(tmp_path, 0)
    scheduler = TurnIngestionScheduler(config)
    assert set(scheduler.operational_status()) == {
        "status",
        "queue_depth",
        "active",
        "refreshed",
        "failed",
        "timed_out",
        "coalesced",
        "queue_full",
        "last_success",
        "last_duration_ms",
        "stale_age_seconds",
        "max_workers",
        "queue_capacity",
        "refresh_interval_seconds",
        "adapter_timeout_seconds",
    }


def test_structured_refresh_rebinds_same_owner_after_stale_a_rejection_and_current_b_retry(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = Config(
        host_id="ingestion-owner-host",
        db_path=tmp_path / "ingestion-owner.db",
        herdr_timeout_seconds=0.5,
        turn_refresh_interval_seconds=100.0,
        turn_refresh_workers=1,
    )
    assert config.db_path is not None
    stable_key = "wsk1_" + ("7" * 64)
    worker_a = Worker(
        id="structured-worker-a",
        name="Structured Worker A",
        status="active",
        space_id="structured-space-a",
        fingerprint="structured-fingerprint-a",
        meta={"stable_key": stable_key, "stable_key_version": 1},
    )
    worker_b = Worker(
        id="structured-worker-b",
        name="Structured Worker B",
        status="waiting",
        space_id="structured-space-b",
        fingerprint="structured-fingerprint-b",
        meta={"stable_key": stable_key, "stable_key_version": 1},
    )
    snapshot_a = Snapshot(
        host_id=config.host_id,
        updated_at="2026-07-13T05:00:00+00:00",
        workers=[worker_a],
    )
    snapshot_b = Snapshot(
        host_id=config.host_id,
        updated_at="2026-07-13T05:01:00+00:00",
        workers=[worker_b],
    )
    binding_a = _binding(config, worker_a, 70, target="structured-pane-a-private")
    binding_b = _binding(config, worker_b, 71, target="structured-pane-b-private")
    raw_source = "019f5590-4444-7444-8444-444444444444"

    init_store(config.db_path)
    save_snapshot(config.db_path, snapshot_a)
    upsert_worker_bindings(config.db_path, [binding_a])
    assert merge_turn_content(
        config.db_path,
        config.host_id,
        worker_a.id,
        {
            "source_turn_id": raw_source,
            "user_text": "stable owner prompt",
            "assistant_final_text": "initial A final",
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-07-13T05:00:01+00:00",
    ) == 1
    with sqlite3.connect(str(config.db_path)) as conn:
        source_before = conn.execute(
            """
            SELECT turn_id, list_sequence,
                   json_extract(payload_json, '$.source_turn_id')
            FROM turns
            WHERE host_id = ?
              AND json_extract(payload_json, '$.source_turn_id') IS NOT NULL
            """,
            (config.host_id,),
        ).fetchone()
        list_state_before = conn.execute(
            """
            SELECT next_sequence, traversal_generation
            FROM turn_list_hosts
            WHERE host_id = ?
            """,
            (config.host_id,),
        ).fetchone()
    assert source_before is not None

    old_entered = threading.Event()
    release_old = threading.Event()
    current_entered = threading.Event()
    reads: list[tuple[str, str]] = []

    def read_binding(_config, binding, *, timeout_seconds, cancel_event=None):
        reads.append((str(binding.worker_id), str(binding.turn_target_value)))
        if binding.worker_id == worker_a.id:
            old_entered.set()
            assert release_old.wait(2)
            final_text = "stale A final must not commit"
        else:
            current_entered.set()
            final_text = "current B final"
        return {
            "source_turn_id": raw_source,
            "user_text": "stable owner prompt",
            "assistant_final_text": final_text,
            "complete": True,
            "has_open_turn": False,
        }

    monkeypatch.setattr(herdr_turns, "_read_turn_for_binding", read_binding)
    scheduler = TurnIngestionScheduler(
        config,
        refresh_interval_seconds=100,
        max_workers=1,
    )
    scheduler.start()
    try:
        assert old_entered.wait(2)
        save_snapshot(config.db_path, snapshot_b)
        upsert_worker_bindings(config.db_path, [binding_b])
        with sqlite3.connect(str(config.db_path)) as conn:
            conn.execute(
                """
                DELETE FROM worker_bindings
                WHERE host_id = ? AND private_fingerprint = ?
                """,
                (config.host_id, binding_a.private_fingerprint),
            )
        scheduler.request_refresh()
        release_old.set()
        assert current_entered.wait(2)
        _wait_until(lambda: scheduler.operational_status()["active"] == 0)
    finally:
        release_old.set()
        scheduler.stop()

    public_payload = turns_payload_from_store(
        config.db_path,
        config.host_id,
        snapshot=snapshot_b,
        schema_version=2,
    )
    source_turns = [
        turn for turn in public_payload["turns"] if turn.get("source_turn_id")
    ]
    assert len(source_turns) == 1
    source_turn = source_turns[0]
    assert source_turn["id"] == source_before[0]
    assert source_turn["source_turn_id"] == source_before[2]
    assert source_turn["source_turn_id"] != raw_source
    assert source_turn["worker_id"] == worker_b.id
    assert source_turn["worker_fingerprint"] == worker_b.fingerprint
    assert source_turn["space_id"] == worker_b.space_id
    assert source_turn["assistant_final_text"] == "current B final"
    assert source_turn["complete"] is True
    assert source_turn["has_open_turn"] is False
    assert reads == [
        (worker_a.id, "structured-pane-a-private"),
        (worker_b.id, "structured-pane-b-private"),
    ]
    assert scheduler.operational_status()["failed"] == 1
    with sqlite3.connect(str(config.db_path)) as conn:
        persisted_source = conn.execute(
            """
            SELECT turn_id, list_sequence,
                   json_extract(payload_json, '$.source_turn_id')
            FROM turns
            WHERE host_id = ?
              AND json_extract(payload_json, '$.source_turn_id') IS NOT NULL
            """,
            (config.host_id,),
        ).fetchone()
        list_state_after = conn.execute(
            """
            SELECT next_sequence, traversal_generation
            FROM turn_list_hosts
            WHERE host_id = ?
            """,
            (config.host_id,),
        ).fetchone()
        current_revisions = conn.execute(
            """
            SELECT COUNT(*)
            FROM turn_content_revisions
            WHERE host_id = ? AND turn_id = ? AND is_current = 1
            """,
            (config.host_id, source_turn["id"]),
        ).fetchone()[0]
        foreign_keys = conn.execute("PRAGMA foreign_key_check").fetchall()
    assert persisted_source == source_before
    assert persisted_source[1] == source_before[1]
    assert list_state_after == list_state_before
    assert current_revisions == 1
    assert foreign_keys == []
    encoded = json.dumps(
        [public_payload, scheduler.operational_status()],
        sort_keys=True,
    )
    for private_value in (
        raw_source,
        "structured-pane-a-private",
        "structured-pane-b-private",
        binding_a.private_fingerprint,
        binding_b.private_fingerprint,
        "stale A final must not commit",
    ):
        assert private_value not in encoded
