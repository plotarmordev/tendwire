"""Public sanitizer and transaction performance bounds without compatibility seams."""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from tendwire.core.models import Snapshot, Worker, WorkerBinding
from tendwire.store.projection import latest_snapshot, save_snapshot
from tendwire.store.schema import init_store
from tendwire.store.turns import apply_turn_refresh


def _workers(count: int) -> list[Worker]:
    return [
        Worker(
            id=f"worker-{index}",
            name=f"worker-{index}",
            meta={"stable_key": "wsk1_" + f"{index:064x}", "stable_key_version": 1},
        )
        for index in range(count)
    ]


def _bindings(workers: list[Worker]) -> list[WorkerBinding]:
    return [
        WorkerBinding(
            host_id="host-a",
            worker_id=worker.id,
            worker_fingerprint=worker.fingerprint,
            backend="herdr",
            target_kind="agent_id",
            target_value=f"private-{worker.id}",
            private_fingerprint=f"private-{worker.id}",
        )
        for worker in workers
    ]


def test_snapshot_route_enrichment_is_bounded_for_large_observation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = tmp_path / "performance.db"
    init_store(db_path)
    # This valid opaque token used to match the generic secret heuristic and
    # disappear nondeterministically from the public worker projection.
    route_tokens = iter(
        ["TAbzoDWCgnT8p-t1hNIqP6ozdIpQeBYHIu2CIuBzKAU"]
        + [f"{index:043d}" for index in range(1, 200)]
    )
    monkeypatch.setattr(
        "tendwire.store.projection.secrets.token_urlsafe",
        lambda _size: next(route_tokens),
    )
    workers = _workers(200)
    started = time.perf_counter()
    persisted = save_snapshot(
        db_path,
        Snapshot(host_id="host-a", updated_at="2026-08-05T00:00:00Z", workers=workers),
        worker_bindings=_bindings(workers),
        binding_backend="herdr",
    )
    assert time.perf_counter() - started < 10
    assert len(persisted.workers) == 200
    assert all(worker.meta.get("route_generation") for worker in persisted.workers)


def test_unchanged_snapshot_round_trip_stays_bounded(tmp_path: Path) -> None:
    db_path = tmp_path / "performance.db"
    init_store(db_path)
    workers = _workers(100)
    bindings = _bindings(workers)
    first = save_snapshot(db_path, Snapshot(host_id="host-a", updated_at="2026-08-05T00:00:00Z", workers=workers), worker_bindings=bindings, binding_backend="herdr")
    started = time.perf_counter()
    second = save_snapshot(db_path, first, worker_bindings=bindings, binding_backend="herdr")
    assert time.perf_counter() - started < 5
    assert second.content_fingerprint == first.content_fingerprint
    assert latest_snapshot(db_path, "host-a").content_fingerprint == first.content_fingerprint


def test_large_turn_sanitization_and_paging_transaction_is_bounded(tmp_path: Path) -> None:
    db_path = tmp_path / "performance.db"
    init_store(db_path)
    workers = _workers(1)
    save_snapshot(db_path, Snapshot(host_id="host-a", updated_at="2026-08-05T00:00:00Z", workers=workers), worker_bindings=_bindings(workers), binding_backend="herdr")
    started = time.perf_counter()
    result = apply_turn_refresh(
        db_path,
        "host-a",
        workers[0].id,
        {
            "source_turn_id": "turn-a",
            "user_text": "question",
            "assistant_final_text": "safe " * 200_000,
            "complete": True,
        },
        observed_at="2026-08-05T00:00:00Z",
    )
    assert time.perf_counter() - started < 10
    assert result.updated == 1
