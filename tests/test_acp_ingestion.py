"""Integration-boundary tests for durable ACP event ingestion."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from tendwire.backends.acp_ingestion import AcpSessionIngestor
from tendwire.config import Config
from tendwire.core.agent_events import AgentEvent, AppendBoundAgentEventResult
from tendwire.core.models import WorkerBinding
from tendwire.store.sqlite import (
    TurnRefreshApplyResult,
    list_agent_events,
    upsert_worker_bindings,
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
        Config(host_id="host-a", db_path=tmp_path / "events.db"),
        session_id="session-a",
        stream_generation="generation-a",
        binding=_binding(),
        append_event=append,
        apply_turn=apply,
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
            _meta={"tendwire": {"thought_kind": "summary"}},
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
        "thought",
        "agent_message",
    ]
    assert all(event.visibility == "private" for event in events)
    assert turns[-1]["assistant_final_text"] == "answer"
    assert turns[-1]["user_text"] == "question"
    assert "private reasoning" not in repr(turns)
    assert turns[-1]["source_turn_id"] == turn_id


def test_shadow_mode_journals_without_turn_projection(tmp_path: Path) -> None:
    events: list[AgentEvent] = []

    def append(
        _path: Path | str,
        _host: str,
        event: AgentEvent,
        **_kwargs,
    ) -> AppendBoundAgentEventResult:
        events.append(event)
        return _appended(1, event)

    def unexpected_turn(*_args, **_kwargs):
        raise AssertionError("shadow mode must not project turns")

    ingestor = AcpSessionIngestor(
        Config(
            host_id="host-a",
            db_path=tmp_path / "events.db",
            agent_event_source="acp_shadow",
        ),
        session_id="session-a",
        stream_generation="generation-a",
        binding=_binding(),
        append_event=append,
        apply_turn=unexpected_turn,
    )
    result = ingestor.ingest_update(
        _update(
            "agent_message_chunk",
            content={"type": "text", "text": "shadow"},
        )
    )

    assert result.event is not None
    assert result.turn is None
    assert len(events) == 1


def test_disabled_thought_policy_discards_before_persistence(tmp_path: Path) -> None:
    def unexpected_append(*_args, **_kwargs):
        raise AssertionError("disabled thoughts must not be persisted")

    ingestor = AcpSessionIngestor(
        Config(
            host_id="host-a",
            db_path=tmp_path / "events.db",
            acp_thought_policy="disabled",
        ),
        session_id="session-a",
        stream_generation="generation-a",
        binding=_binding(),
        append_event=unexpected_append,
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
            Config(host_id="host-a", db_path=tmp_path / "events.db"),
            session_id="session-a",
            stream_generation=generation,
            binding=_binding(),
            append_event=append,
            apply_turn=lambda *_args, **_kwargs: TurnRefreshApplyResult(0, False),
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
            Config(host_id="host-a", db_path=tmp_path / "events.db"),
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
        Config(host_id="host-a", db_path=tmp_path / "events.db"),
        session_id="session-a",
        stream_generation="generation-a",
        binding=_binding(),
        append_event=unexpected,
        apply_turn=unexpected,
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
    assert ingestor.projector.session_snapshot("session-b") is None


def test_required_mode_fails_closed_when_durable_binding_is_stale(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "events.db"
    binding = _binding()
    upsert_worker_bindings(db_path, [binding])
    replacement = replace(
        binding,
        worker_id="replacement-worker",
        worker_fingerprint="replacement-fingerprint",
    )
    upsert_worker_bindings(db_path, [replacement])

    def unexpected_projection(*_args, **_kwargs):
        raise AssertionError("stale ACP events must not be projected")

    ingestor = AcpSessionIngestor(
        Config(
            host_id="host-a",
            db_path=db_path,
            agent_event_source="acp_required",
        ),
        session_id="session-a",
        stream_generation="generation-a",
        binding=binding,
        apply_turn=unexpected_projection,
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


def test_default_authority_check_accepts_the_current_durable_binding(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "events.db"
    binding = _binding()
    upsert_worker_bindings(db_path, [binding])
    ingestor = AcpSessionIngestor(
        Config(host_id="host-a", db_path=db_path, agent_event_source="acp_required"),
        session_id="session-a",
        stream_generation="generation-a",
        binding=binding,
    )

    result = ingestor.ingest_update(_update("usage_update", used=1, size=100))

    assert result.event is not None
    assert result.event.inserted
    assert result.ignored_reason is None


def test_shadow_completion_never_projects_and_finality_is_idempotent(
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

    def unexpected_turn(*_args, **_kwargs):
        raise AssertionError("shadow mode must never project, including completion")

    ingestor = AcpSessionIngestor(
        Config(
            host_id="host-a",
            db_path=tmp_path / "events.db",
            agent_event_source="acp_shadow",
        ),
        session_id="session-a",
        stream_generation="generation-a",
        binding=_binding(),
        append_event=append,
        apply_turn=unexpected_turn,
    )
    ingestor.start_turn(producer_turn_id="turn-1")
    ingestor.ingest_update(
        _update(
            "agent_message_chunk",
            content={"type": "text", "text": "shadow final"},
        )
    )

    completed = ingestor.mark_prompt_complete()
    repeated = ingestor.mark_prompt_complete()
    late = ingestor.ingest_update(
        _update(
            "agent_message_chunk",
            content={"type": "text", "text": "late mutation"},
        )
    )

    assert completed.turn is None
    assert repeated.ignored_reason == "turn_already_complete"
    assert late.ignored_reason == "turn_already_complete"
    assert len(events) == 1


def test_required_mode_projects_messages_and_final_exactly_once(tmp_path: Path) -> None:
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
        return TurnRefreshApplyResult(1, False)

    ingestor = AcpSessionIngestor(
        Config(
            host_id="host-a",
            db_path=tmp_path / "events.db",
            agent_event_source="acp_required",
        ),
        session_id="session-a",
        stream_generation="generation-a",
        binding=_binding(),
        append_event=append,
        apply_turn=apply,
    )
    ingestor.start_turn(producer_turn_id="turn-1")
    streamed = ingestor.ingest_update(
        _update(
            "agent_message_chunk",
            content={"type": "text", "text": "answer"},
        )
    )
    completed = ingestor.mark_prompt_complete()

    assert streamed.turn is not None
    assert completed.turn is not None
    assert [turn["complete"] for turn in turns] == [False, True]
    assert turns[-1]["assistant_final_text"] == "answer"
    assert turns[-1]["assistant_stream_text"] == ""


def test_duplicate_durable_event_is_not_reprojected(tmp_path: Path) -> None:
    projected = False

    def append(
        _path: Path | str,
        _host: str,
        event: AgentEvent,
        **_kwargs,
    ) -> AppendBoundAgentEventResult:
        return AppendBoundAgentEventResult("replayed", event.event_id, 9)

    def apply(*_args, **_kwargs):
        nonlocal projected
        projected = True
        return TurnRefreshApplyResult(1, False)

    ingestor = AcpSessionIngestor(
        Config(host_id="host-a", db_path=tmp_path / "events.db"),
        session_id="session-a",
        stream_generation="generation-a",
        binding=_binding(),
        append_event=append,
        apply_turn=apply,
    )
    result = ingestor.ingest_update(
        _update(
            "agent_message_chunk",
            content={"type": "text", "text": "replayed"},
        ),
        source_event_id="event-1",
        replay=True,
    )

    assert result.ignored_reason == "duplicate_event"
    assert not projected


def test_atomic_durable_replay_is_reported_without_second_projection(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "events.db"
    binding = _binding()
    upsert_worker_bindings(db_path, [binding])
    turns: list[dict[str, object]] = []

    def apply(_path: Path | str, _host: str, _worker: str, content, **_kwargs):
        turns.append(dict(content))
        return TurnRefreshApplyResult(len(turns), False)

    def ingestor() -> AcpSessionIngestor:
        return AcpSessionIngestor(
            Config(host_id="host-a", db_path=db_path),
            session_id="session-a",
            stream_generation="generation-a",
            binding=binding,
            apply_turn=apply,
        )

    notification = _update(
        "agent_message_chunk",
        messageId="assistant-1",
        content={"type": "text", "text": "durable once"},
    )
    inserted = ingestor().ingest_update(
        notification,
        source_event_id="event-1",
    )
    replayed = ingestor().ingest_update(
        notification,
        source_event_id="event-1",
        replay=True,
    )

    assert inserted.event is not None
    assert inserted.event.status == "inserted"
    assert inserted.turn is not None
    assert replayed.event is not None
    assert replayed.event.status == "replayed"
    assert replayed.ignored_reason == "duplicate_event"
    assert replayed.turn is None
    assert len(turns) == 1
    assert len(list_agent_events(db_path, "host-a")) == 1


def test_producer_turn_identity_survives_transport_recreation(tmp_path: Path) -> None:
    identities: list[str] = []
    for generation in ("generation-a", "generation-b"):
        ingestor = AcpSessionIngestor(
            Config(host_id="host-a", db_path=tmp_path / "events.db"),
            session_id="session-a",
            stream_generation=generation,
            binding=_binding(),
        )
        identities.append(ingestor.start_turn(producer_turn_id="producer-turn-7"))

    assert identities[0] == identities[1]


def test_private_summary_policy_retains_display_chunks_but_rejects_marked_raw_thoughts(
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
        Config(host_id="host-a", db_path=tmp_path / "events.db"),
        session_id="session-a",
        stream_generation="generation-a",
        binding=_binding(),
        append_event=append,
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
    summary = ingestor.ingest_update(
        _update(
            "agent_thought_chunk",
            messageId="summary-1",
            content={"type": "text", "text": "readable summary"},
            _meta={"tendwire": {"thought_kind": "summary"}},
        )
    )

    assert unclassified.event is not None
    assert raw.ignored_reason == "thought_policy_requires_summary"
    assert summary.event is not None
    assert len(events) == 2
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
        Config(
            host_id="host-a",
            db_path=tmp_path / "events.db",
            acp_thought_policy="private_all",
        ),
        session_id="session-a",
        stream_generation="generation-a",
        binding=_binding(),
        append_event=append,
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
