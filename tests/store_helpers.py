"""Test adapters onto supported store entry points."""

from __future__ import annotations

from collections.abc import Mapping
import hashlib
from dataclasses import replace
from pathlib import Path
from typing import Any

from tendwire.core.agent_events import AgentEvent, AppendAgentEventResult
from tendwire.core.models import Snapshot, Worker, WorkerBinding, sanitize_public_mapping, stable_fingerprint
from tendwire.core.turns import PendingObservation, PendingObservedChoice
from tendwire.store.events import list_agent_events, record_agent_event
from tendwire.store.projection import (
    attention_payload_from_store,
    latest_snapshot,
    save_snapshot,
    upsert_worker_bindings as _upsert_worker_bindings,
)
from tendwire.store.schema import init_store
from tendwire.store.turns import apply_turn_refresh


def apply_test_turn_refresh(
    db_path: Path | str,
    host_id: str,
    worker_id: str,
    content: Mapping[str, Any],
    *,
    observed_at: str | None = None,
) -> int:
    return apply_turn_refresh(
        db_path,
        host_id,
        worker_id,
        content,
        observed_at=observed_at,
    ).updated


def apply_test_backend_pending(
    db_path: Path | str,
    host_id: str,
    worker_id: str,
    pending: Mapping[str, Any] | None,
    *,
    expected_binding: WorkerBinding | None = None,
    observed_at: str | None = None,
) -> bool:
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
    return apply_turn_refresh(
        db_path,
        host_id,
        worker_id,
        {},
        backend_pending_observation=observation,
        expected_binding=expected_binding,
        observed_at=observed_at,
    ).pending_changed


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


def record_test_agent_event(
    db_path: Path | str,
    host_id: str,
    event: AgentEvent,
) -> AppendAgentEventResult:
    return record_agent_event(
        db_path,
        host_id,
        kind=event.kind,
        source=event.source,
        worker_id=event.worker_id,
        payload=event.payload,
        source_session_id=event.source_session_id,
        source_turn_id=event.source_turn_id,
        source_item_id=event.source_item_id,
        source_message_id=event.source_message_id,
        source_event_id=event.source_event_id,
        source_sequence=event.source_sequence,
        visibility=event.visibility,
        observed_at=event.observed_at,
    )


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
