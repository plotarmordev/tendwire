"""Integration-boundary tests for durable ACP event ingestion."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from tendwire.backends.acp_ingestion import AcpSessionIngestor
from tendwire.config import Config
from tendwire.core.agent_events import AgentEvent, AppendBoundAgentEventResult
from tendwire.core.models import Snapshot, Worker, WorkerBinding
from tendwire.backends.acp_protocol import StopReason
from tendwire.store.events import list_agent_events
from tendwire.store.projection import save_snapshot
from tendwire.store.turns import (
    AppendProjectedAgentEventResult,
    TurnRefreshApplyResult,
)
from .store_helpers import (
    read_public_test_agent_events,
    upsert_test_worker_bindings as upsert_worker_bindings,
)


def _binding() -> WorkerBinding:
    return WorkerBinding(
        host_id="host-a",
        worker_id="worker-a",
        worker_fingerprint="worker-fingerprint",
        backend="herdr",
        target_kind="pane_id",
        target_value="private-pane",
        turn_target_kind="acp_session_id",
        turn_target_value="session-a",
        private_fingerprint="binding-fingerprint",
    )


def _update(kind: str, **fields: object) -> dict[str, object]:
    return {
        "method": "session/update",
        "params": {
            "sessionId": "session-a",
            "update": {"sessionUpdate": kind, **fields},
        },
    }


def _appended(sequence: int, event: AgentEvent) -> AppendBoundAgentEventResult:
    return AppendBoundAgentEventResult("inserted", event.event_id, sequence)


def _persist(
    append,
    apply=None,
):
    def persist(
        path: Path | str,
        host_id: str,
        event: AgentEvent,
        *,
        expected_binding: WorkerBinding,
        content=None,
        **_kwargs,
    ) -> AppendProjectedAgentEventResult:
        appended = append(
            path,
            host_id,
            event,
            expected_binding=expected_binding,
        )
        turn = None
        if content is not None and appended.status != "binding_changed":
            turn = (
                apply(path, host_id, event.worker_id, content)
                if apply is not None
                else TurnRefreshApplyResult(0, False)
            )
        return AppendProjectedAgentEventResult(appended, turn)

    return persist


def _config(db_path: Path, **kwargs: object) -> Config:
    return Config(
        host_id="host-a",
        db_path=db_path,
        **kwargs,
    )


def test_messages_are_journaled_privately_and_projected_without_thoughts(
    tmp_path: Path,
) -> None:
    events: list[AgentEvent] = []
    turns: list[dict[str, object]] = []

    def append(
        _path: Path | str,
        _host: str,
        event: AgentEvent,
        *,
        expected_binding: WorkerBinding,
    ) -> AppendBoundAgentEventResult:
        assert expected_binding.worker_id == "worker-a"
        assert expected_binding.private_fingerprint == "binding-fingerprint"
        events.append(event)
        return _appended(len(events), event)

    def apply(_path: Path | str, _host: str, _worker: str, content, **_kwargs):
        turns.append(dict(content))
        return TurnRefreshApplyResult(1, False)

    ingestor = AcpSessionIngestor(
        _config(tmp_path / "events.db"),
        session_id="session-a",
        stream_generation="generation-a",
        binding=_binding(),
        persist_event=_persist(append, apply),
    )
    turn_id = ingestor.start_turn(producer_turn_id="private-turn")
    ingestor.ingest_update(
        _update(
            "user_message_chunk",
            messageId="user-1",
            content={"type": "text", "text": "question"},
        )
    )
    ingestor.ingest_update(
        _update(
            "agent_thought_chunk",
            messageId="reasoning-1",
            content={"type": "text", "text": "private reasoning"},
            _meta={"tendwire.dev/thought_kind": "summary"},
        )
    )
    ingestor.ingest_update(
        _update(
            "agent_message_chunk",
            messageId="assistant-1",
            content={"type": "text", "text": "answer"},
        )
    )
    ingestor.mark_prompt_complete()

    assert turn_id.startswith("acpt_")
    assert [event.kind for event in events] == [
        "user_message",
        "agent_message",
        "extension",
    ]
    assert all(event.visibility == "private" for event in events)
    assert turns[-1]["assistant_final_text"] == "answer"
    assert turns[-1]["user_text"] == "question"
    assert "private reasoning" not in repr(turns)
    assert turns[-1]["source_turn_id"] == turn_id


def test_disabled_thought_policy_discards_before_persistence(tmp_path: Path) -> None:
    def unexpected_append(*_args, **_kwargs):
        raise AssertionError("disabled thoughts must not be persisted")

    ingestor = AcpSessionIngestor(
        _config(tmp_path / "events.db", acp_thought_policy="disabled"),
        session_id="session-a",
        stream_generation="generation-a",
        binding=_binding(),
        persist_event=_persist(unexpected_append),
    )
    result = ingestor.ingest_update(
        _update(
            "agent_thought_chunk",
            content={"type": "text", "text": "discard me"},
        )
    )

    assert result.ignored_reason == "thought_policy_disabled"


def test_synthetic_event_identity_is_scoped_to_stream_generation(tmp_path: Path) -> None:
    seen: list[str] = []

    def append(
        _path: Path | str,
        _host: str,
        event: AgentEvent,
        **_kwargs,
    ) -> AppendBoundAgentEventResult:
        assert event.source_event_id is not None
        seen.append(event.source_event_id)
        return _appended(1, event)

    for generation in ("generation-a", "generation-b"):
        ingestor = AcpSessionIngestor(
            _config(tmp_path / "events.db"),
            session_id="session-a",
            stream_generation=generation,
            binding=_binding(),
            persist_event=_persist(append),
        )
        ingestor.ingest_update(
            _update(
                "agent_message_chunk",
                content={"type": "text", "text": "same chunk"},
            )
        )

    assert seen == ["stream:generation-a:1", "stream:generation-b:1"]


def test_constructor_rejects_binding_for_another_acp_session(tmp_path: Path) -> None:
    binding = _binding()
    mismatched = WorkerBinding(
        **{**binding.__dict__, "turn_target_value": "another-session"}
    )

    with pytest.raises(ValueError, match="does not match"):
        AcpSessionIngestor(
            _config(tmp_path / "events.db"),
            session_id="session-a",
            stream_generation="generation-a",
            binding=mismatched,
        )


def test_notification_session_mismatch_is_rejected_before_state_or_persistence(
    tmp_path: Path,
) -> None:
    def unexpected(*_args, **_kwargs):
        raise AssertionError("mismatched session must not cross the authority boundary")

    ingestor = AcpSessionIngestor(
        _config(tmp_path / "events.db"),
        session_id="session-a",
        stream_generation="generation-a",
        binding=_binding(),
        persist_event=_persist(unexpected, unexpected),
    )
    notification = _update(
        "agent_message_chunk",
        content={"type": "text", "text": "wrong worker"},
    )
    params = notification["params"]
    assert isinstance(params, dict)
    params["sessionId"] = "session-b"

    result = ingestor.ingest_update(notification)

    assert result.ignored_reason == "session_mismatch"
    assert ingestor.source_turn_id is None


def test_required_mode_fails_closed_when_durable_binding_is_stale(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "events.db"
    binding = _binding()
    save_snapshot(
        db_path,
        Snapshot(
            host_id="host-a",
            updated_at="2026-01-01T00:00:00+00:00",
            workers=[
                Worker(
                    id=binding.worker_id,
                    name="Worker A",
                    fingerprint=binding.worker_fingerprint,
                )
            ],
        ),
    )
    upsert_worker_bindings(db_path, [binding])
    replacement = replace(
        binding,
        worker_id="replacement-worker",
        worker_fingerprint="replacement-fingerprint",
    )
    upsert_worker_bindings(db_path, [replacement])

    ingestor = AcpSessionIngestor(
        _config(db_path),
        session_id="session-a",
        stream_generation="generation-a",
        binding=binding,
    )

    result = ingestor.ingest_update(
        _update(
            "agent_message_chunk",
            content={"type": "text", "text": "must not publish"},
        )
    )

    assert result.ignored_reason == "stale_binding"
    assert result.event is not None
    assert result.event.status == "binding_changed"
    assert result.event.sequence is None
    assert result.turn is None
    assert list_agent_events(db_path, "host-a") == ()
    assert ingestor.source_turn_id is None


def test_default_authority_check_accepts_the_current_durable_binding(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "events.db"
    binding = _binding()
    upsert_worker_bindings(db_path, [binding])
    ingestor = AcpSessionIngestor(
        _config(db_path),
        session_id="session-a",
        stream_generation="generation-a",
        binding=binding,
    )

    result = ingestor.ingest_update(_update("usage_update", used=1, size=100))

    assert result.event is not None
    assert result.event.inserted
    assert result.ignored_reason is None



@pytest.mark.parametrize(
    ("stop_reason", "outcome", "notice"),
    (
        (StopReason.END_TURN, "completed", None),
        (StopReason.MAX_TOKENS, "truncated_max_tokens", "token limit"),
        (
            StopReason.MAX_TURN_REQUESTS,
            "truncated_max_turn_requests",
            "request limit",
        ),
        (StopReason.REFUSAL, "refused", "refused"),
        (StopReason.CANCELLED, "cancelled", "cancelled"),
    ),
)
def test_outgoing_prompt_is_durable_before_no_echo_completion_stop_reason(
    tmp_path: Path,
    stop_reason: StopReason,
    outcome: str,
    notice: str | None,
) -> None:
    events: list[AgentEvent] = []
    turns: list[dict[str, object]] = []

    def append(_path, _host, event, **_kwargs):
        events.append(event)
        return _appended(len(events), event)

    def apply(_path, _host, _worker, content, **_kwargs):
        turns.append(dict(content))
        return TurnRefreshApplyResult(len(turns), False)

    ingestor = AcpSessionIngestor(
        _config(tmp_path / "events.db"),
        session_id="session-a",
        stream_generation="generation-a",
        binding=_binding(),
        persist_event=_persist(append, apply),
    )
    begun = ingestor.begin_prompt(
        ({"type": "text", "text": "question not echoed"},),
        producer_turn_id="turn-a",
    )
    completed = ingestor.mark_prompt_complete(stop_reason)

    assert begun.event is not None and begun.event.status == "inserted"
    assert events[0].kind == "user_message"
    assert events[0].payload["outgoing"] is True
    assert turns[0]["user_text"] == "question not echoed"
    assert completed.event is not None
    assert events[-1].payload["stop_reason"] == stop_reason.value
    assert events[-1].payload["outcome"] == outcome
    final_text = str(turns[-1]["assistant_final_text"])
    assert (notice is not None and notice in final_text) or (
        notice is None and final_text == ""
    )




def test_steering_prompt_appends_to_active_turn_without_resetting_identity(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "events.db"
    events: list[AgentEvent] = []
    turns: list[dict[str, object]] = []

    def append(
        _path: Path | str,
        _host: str,
        event: AgentEvent,
        **_kwargs,
    ) -> AppendBoundAgentEventResult:
        events.append(event)
        return _appended(len(events), event)

    def apply(_path: Path | str, _host: str, _worker: str, content, **_kwargs):
        turns.append(dict(content))
        return TurnRefreshApplyResult(len(turns), False)

    binding = _binding()
    ingestor = AcpSessionIngestor(
        _config(db_path),
        session_id="session-a",
        stream_generation="generation-a",
        binding=binding,
        persist_event=_persist(append, apply),
    )
    begun = ingestor.begin_prompt(
        ({"type": "text", "text": "initial"},),
        producer_turn_id="producer-initial",
    )
    source_turn_id = ingestor.source_turn_id
    steered = ingestor.append_prompt(
        ({"type": "text", "text": "live follow-up"},),
        producer_turn_id="producer-steer",
    )

    assert begun.event is not None
    assert steered.event is not None
    assert events[1].payload["steering"] is True
    assert ingestor.source_turn_id == source_turn_id
    assert turns[-1]["user_text"] == "initial\n\nlive follow-up"

    ingestor.mark_prompt_complete()
    assert not ingestor.can_append_prompt()
    with pytest.raises(RuntimeError, match="active turn"):
        ingestor.append_prompt(
            ({"type": "text", "text": "too late"},),
            producer_turn_id="producer-late",
        )


def test_append_exception_rolls_back_turn_identity_sequence_and_message(
    tmp_path: Path,
) -> None:
    attempts = 0

    def append(
        _path: Path | str,
        _host: str,
        event: AgentEvent,
        **_kwargs,
    ) -> AppendBoundAgentEventResult:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("durable append failed")
        return _appended(1, event)

    ingestor = AcpSessionIngestor(
        _config(tmp_path / "events.db"),
        session_id="session-a",
        stream_generation="generation-a",
        binding=_binding(),
        persist_event=_persist(
            append,
            lambda *_args, **_kwargs: TurnRefreshApplyResult(1, False),
        ),
    )
    notification = _update(
        "agent_message_chunk",
        content={"type": "text", "text": "exactly once"},
    )

    with pytest.raises(RuntimeError, match="durable append failed"):
        ingestor.ingest_update(notification)
    assert ingestor.source_turn_id is None

    accepted = ingestor.ingest_update(notification)
    assert accepted.event is not None and accepted.event.status == "inserted"
    assert accepted.event.sequence == 1
    assert accepted.turn is not None


def test_oversized_first_chunk_does_not_leave_an_implicit_turn(tmp_path: Path) -> None:
    from tendwire.backends.acp_projection import AcpEventProjector, AcpProjectionError

    ingestor = AcpSessionIngestor(
        _config(tmp_path / "events.db"),
        session_id="session-a",
        stream_generation="generation-a",
        binding=_binding(),
        projector=AcpEventProjector(max_event_bytes=128),
    )

    with pytest.raises(AcpProjectionError, match="size limit"):
        ingestor.ingest_update(
            _update(
                "agent_message_chunk",
                content={"type": "text", "text": "x" * 300},
            )
        )
    assert ingestor.source_turn_id is None




def test_producer_turn_identity_survives_transport_recreation(tmp_path: Path) -> None:
    identities: list[str] = []
    for generation in ("generation-a", "generation-b"):
        ingestor = AcpSessionIngestor(
            _config(tmp_path / "events.db"),
            session_id="session-a",
            stream_generation=generation,
            binding=_binding(),
        )
        identities.append(ingestor.start_turn(producer_turn_id="producer-turn-7"))

    assert identities[0] == identities[1]


def test_private_summary_requires_exact_trusted_marker_and_rejects_conflicts(
    tmp_path: Path,
) -> None:
    events: list[AgentEvent] = []

    def append(
        _path: Path | str,
        _host: str,
        event: AgentEvent,
        **_kwargs,
    ) -> AppendBoundAgentEventResult:
        events.append(event)
        return _appended(len(events), event)

    ingestor = AcpSessionIngestor(
        _config(tmp_path / "events.db", acp_thought_policy="private_summary"),
        session_id="session-a",
        stream_generation="generation-a",
        binding=_binding(),
        persist_event=_persist(append),
    )
    unclassified = ingestor.ingest_update(
        _update(
            "agent_thought_chunk",
            content={"type": "text", "text": "adapter display summary"},
        )
    )
    raw = ingestor.ingest_update(
        _update(
            "agent_thought_chunk",
            content={"type": "text", "text": "raw secret"},
            _meta={"tendwire": {"thought_kind": "raw"}},
        )
    )
    unknown = ingestor.ingest_update(
        _update(
            "agent_thought_chunk",
            content={"type": "text", "text": "unknown secret"},
            _meta={"tendwire.dev/thought_kind": "SUMMARY"},
        )
    )
    conflicting = ingestor.ingest_update(
        _update(
            "agent_thought_chunk",
            content={
                "type": "text",
                "text": "conflicting secret",
                "_meta": {"reasoning_kind": "raw"},
            },
            _meta={"tendwire.dev/thought_kind": "summary"},
        )
    )
    summary = ingestor.ingest_update(
        _update(
            "agent_thought_chunk",
            messageId="summary-1",
            content={"type": "text", "text": "readable summary"},
            _meta={"tendwire.dev/thought_kind": "summary"},
        )
    )

    assert unclassified.ignored_reason == "thought_policy_requires_summary"
    assert raw.ignored_reason == "thought_policy_requires_summary"
    assert unknown.ignored_reason == "thought_policy_requires_summary"
    assert conflicting.ignored_reason == "thought_policy_requires_summary"
    assert summary.event is not None
    assert len(events) == 1
    assert all(event.visibility == "private" for event in events)
    assert all(event.public_payload == {} for event in events)
    assert "raw secret" not in repr(events)


def test_private_all_policy_retains_marked_raw_thought_privately(tmp_path: Path) -> None:
    events: list[AgentEvent] = []

    def append(
        _path: Path | str,
        _host: str,
        event: AgentEvent,
        **_kwargs,
    ) -> AppendBoundAgentEventResult:
        events.append(event)
        return _appended(1, event)

    ingestor = AcpSessionIngestor(
        _config(tmp_path / "events.db", acp_thought_policy="private_all"),
        session_id="session-a",
        stream_generation="generation-a",
        binding=_binding(),
        persist_event=_persist(append),
    )
    result = ingestor.ingest_update(
        _update(
            "agent_thought_chunk",
            content={"type": "text", "text": "raw local diagnostic"},
            _meta={"tendwire": {"thought_kind": "raw"}},
        )
    )

    assert result.event is not None
    assert len(events) == 1
    assert events[0].payload["text_delta"] == "raw local diagnostic"
    assert events[0].public_payload == {}
