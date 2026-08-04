"""Test adapters onto supported store entry points."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from tendwire.core.agent_events import AgentEvent, AppendAgentEventResult
from tendwire.core.models import sanitize_public_mapping, stable_fingerprint
from tendwire.core.turns import PendingObservation, PendingObservedChoice
from tendwire.store.sqlite import (
    apply_turn_refresh,
    attention_payload_from_store,
    list_agent_events,
    record_agent_event,
)


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
