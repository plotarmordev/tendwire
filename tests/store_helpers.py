"""Test adapters onto supported store entry points."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
import hashlib
from itertools import count
from pathlib import Path
from typing import Any

from tendwire.core.agent_events import agent_event
from tendwire.core.models import Snapshot, Worker, WorkerBinding, sanitize_public_mapping, stable_fingerprint
from tendwire.core.turns import PendingObservation, PendingObservedChoice
from tendwire.store.events import list_agent_events
from tendwire.store.pending import apply_backend_pending_observation
from tendwire.store.projection import (
    attention_payload_from_store,
    latest_snapshot,
    list_worker_bindings,
    save_snapshot,
    upsert_worker_bindings as _upsert_worker_bindings,
)
from tendwire.store.schema import init_store
from tendwire.store.turns import (
    TurnRefreshApplyResult,
    append_agent_event_and_apply_turn_for_binding,
)


_TEST_TURN_EVENT_SEQUENCE = count(1)


def _current_test_binding(
    db_path: Path | str,
    host_id: str,
    worker_id: str,
) -> WorkerBinding:
    matches = [
        binding
        for binding in list_worker_bindings(Path(db_path), host_id)
        if binding.worker_id == worker_id
    ]
    assert len(matches) == 1
    return matches[0]


def append_test_turn(
    db_path: Path | str,
    host_id: str,
    worker_id: str,
    content: Mapping[str, Any],
    *,
    observed_at: str | None = None,
    expected_updated: int = 1,
) -> TurnRefreshApplyResult:
    binding = _current_test_binding(db_path, host_id, worker_id)
    source_turn_id = content.get("source_turn_id")
    assert isinstance(source_turn_id, str) and source_turn_id
    sequence = next(_TEST_TURN_EVENT_SEQUENCE)
    event = agent_event(
        kind="agent_message",
        source=binding.backend,
        worker_id=worker_id,
        payload={"fixture": "turn_projection", "sequence": sequence},
        source_session_id=binding.turn_target_value or binding.target_value,
        source_turn_id=source_turn_id,
        source_event_id=f"test-turn-event-{sequence}",
        observed_at=observed_at,
    )
    result = append_agent_event_and_apply_turn_for_binding(
        db_path,
        host_id,
        event,
        expected_binding=binding,
        content=content,
    )
    assert result.event.status == "inserted"
    assert result.turn is not None
    assert result.turn.updated == expected_updated
    return result.turn


def apply_test_backend_pending(
    db_path: Path | str,
    host_id: str,
    worker_id: str,
    pending: Mapping[str, Any] | None,
    *,
    expected_binding: WorkerBinding | None = None,
    observed_at: str | None = None,
) -> bool:
    binding = expected_binding or _current_test_binding(db_path, host_id, worker_id)
    if pending is None:
        observation = PendingObservation("read_succeeded_no_prompt")
    else:
        clean = sanitize_public_mapping(pending)
        choices = tuple(
            PendingObservedChoice(
                choice_id=(
                    "choice-"
                    + stable_fingerprint(
                        {
                            "domain": "test.pending-choice.v1",
                            "ordinal": ordinal,
                            "choice": choice,
                        },
                        length=24,
                    )
                ),
                label=str(choice.get("label") or "Option"),
                picker_ordinal=ordinal,
            )
            for ordinal, choice in enumerate(clean.get("choices", ()), 1)
            if isinstance(choice, Mapping)
        )
        observation = PendingObservation(
            "open_prompt",
            question=str(clean.get("question") or "Pending action"),
            pending_kind=str(clean.get("kind") or "question"),
            choices=choices,
            revision_digest=stable_fingerprint(
                {"domain": "test.pending-observation.v1", "payload": clean}
            ),
        )
    return apply_backend_pending_observation(
        db_path,
        host_id,
        worker_id,
        observation,
        observed_at=observed_at,
        binding_private_fingerprint=binding.private_fingerprint,
        observed_turn_target_value=binding.turn_target_value,
        binding_authoritative=True,
    )


def read_test_attention_items(
    db_path: Path,
    host_id: str,
    *,
    include_resolved: bool = False,
) -> list[dict[str, Any]]:
    payload = attention_payload_from_store(
        db_path,
        host_id,
        include_resolved=include_resolved,
    )
    return [] if payload is None else list(payload["attention"])


def read_public_test_agent_events(
    db_path: Path | str,
    host_id: str,
) -> tuple[dict[str, Any], ...]:
    return tuple(
        item.public_dict()
        for item in list_agent_events(db_path, host_id, visibility="public")
    )


def upsert_test_worker_bindings(
    db_path: Path,
    bindings: list[WorkerBinding] | tuple[WorkerBinding, ...],
) -> int:
    """Persist test routes through the supported snapshot/binding transaction."""
    init_store(db_path)
    for binding in bindings:
        snapshot = latest_snapshot(db_path, binding.host_id)
        workers = list(snapshot.workers) if snapshot is not None else []
        worker = next((item for item in workers if item.id == binding.worker_id), None)
        if worker is None:
            stable_key = "wsk1_" + hashlib.sha256(binding.worker_id.encode()).hexdigest()
            worker = Worker(
                id=binding.worker_id,
                name=binding.worker_id,
                fingerprint=binding.worker_fingerprint,
                meta={"stable_key": stable_key, "stable_key_version": 1},
            )
            workers.append(worker)
        elif not isinstance(worker.meta.get("stable_key"), str):
            meta = dict(worker.meta)
            meta.update(
                {
                    "stable_key": "wsk1_"
                    + hashlib.sha256(binding.worker_id.encode()).hexdigest(),
                    "stable_key_version": 1,
                }
            )
            worker = replace(worker, meta=meta, fingerprint="")
            workers = [worker if item.id == worker.id else item for item in workers]
        save_snapshot(
            db_path,
            Snapshot(
                host_id=binding.host_id,
                updated_at=(snapshot.updated_at if snapshot is not None else binding.observed_at),
                spaces=(() if snapshot is None else snapshot.spaces),
                workers=workers,
                attention=(() if snapshot is None else snapshot.attention),
                backend_health=(() if snapshot is None else snapshot.backend_health),
            ),
        )
        _upsert_worker_bindings(db_path, [binding])
    return len(bindings)
