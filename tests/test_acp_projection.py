from __future__ import annotations

import pytest

from tendwire.backends.acp_projection import AcpEventProjector, AcpProjectionError


def _update(session_update: str, **fields: object) -> dict[str, object]:
    return {
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {
            "sessionId": "session-1",
            "update": {"sessionUpdate": session_update, **fields},
        },
    }


def test_message_chunks_are_assembled_by_session_kind_and_message_id() -> None:
    projector = AcpEventProjector()

    first = projector.normalize_session_update(
        _update(
            "agent_message_chunk",
            messageId="answer-1",
            content={"type": "text", "text": "hello "},
        )
    )
    second = projector.normalize_session_update(
        _update(
            "agent_message_chunk",
            messageId="answer-1",
            content={"type": "text", "text": "world"},
        )
    )
    third = projector.normalize_session_update(
        _update(
            "agent_message_chunk",
            messageId="answer-2",
            content={"type": "text", "text": "follow-up"},
        )
    )

    assert first is not None and first["payload"]["assembled_text"] == "hello "
    assert second is not None and second["payload"]["assembled_text"] == "hello world"
    assert third is not None and third["payload"]["message_index"] == 1
    assert [first["sequence"], second["sequence"], third["sequence"]] == [1, 2, 3]
    assert projector.project_turn_content("session-1") == {
        "user_text": "",
        "assistant_stream_text": "hello world\n\nfollow-up",
        "assistant_final_text": "",
        "complete": False,
        "has_open_turn": True,
    }


def test_thoughts_are_private_and_never_enter_legacy_turn_content() -> None:
    projector = AcpEventProjector()
    thought = projector.normalize_session_update(
        _update(
            "agent_thought_chunk",
            messageId="reasoning-1",
            content={"type": "text", "text": "private reasoning"},
        )
    )
    projector.normalize_session_update(
        _update(
            "agent_message_chunk",
            messageId="answer-1",
            content={"type": "text", "text": "safe answer"},
        )
    )

    assert thought is not None
    assert thought["kind"] == "thought"
    assert thought["privacy"] == "private"
    assert thought["private_fields"] == ["payload"]
    legacy = projector.mark_turn_complete("session-1")
    assert legacy["assistant_final_text"] == "safe answer"
    assert legacy["assistant_stream_text"] == ""
    assert "reasoning" not in repr(legacy)


def test_non_text_content_is_preserved_without_becoming_turn_text() -> None:
    projector = AcpEventProjector()
    event = projector.normalize_session_update(
        _update(
            "agent_message_chunk",
            messageId="image-1",
            content={"type": "image", "data": "sensitive-base64", "mimeType": "image/png"},
        )
    )

    assert event is not None
    assert event["payload"]["content"]["type"] == "image"
    assert event["payload"]["text_delta"] == ""
    assert projector.project_turn_content("session-1")["assistant_stream_text"] == ""


def test_user_and_agent_text_remain_separate_and_reset_starts_new_turn() -> None:
    projector = AcpEventProjector()
    projector.normalize_session_update(
        _update(
            "user_message_chunk",
            messageId="prompt-1",
            content={"type": "text", "text": "question"},
        )
    )
    projector.normalize_session_update(
        _update(
            "agent_message_chunk",
            messageId="answer-1",
            content={"type": "text", "text": "answer"},
        )
    )
    assert projector.mark_turn_complete("session-1") == {
        "user_text": "question",
        "assistant_stream_text": "",
        "assistant_final_text": "answer",
        "complete": True,
        "has_open_turn": False,
    }

    projector.reset_turn("session-1")
    assert projector.project_turn_content("session-1")["user_text"] == ""
    assert projector.session_snapshot("session-1")["sequence"] == 2


def test_tool_lifecycle_merges_partial_updates_and_marks_raw_fields_private() -> None:
    projector = AcpEventProjector()
    started = projector.normalize_session_update(
        _update(
            "tool_call",
            toolCallId="tool-1",
            title="Read configuration",
            kind="read",
            status="pending",
            rawInput={"path": "/private/file"},
        )
    )
    updated = projector.normalize_session_update(
        _update(
            "tool_call_update",
            toolCallId="tool-1",
            status="completed",
            content=[{"type": "content", "content": {"type": "text", "text": "done"}}],
            rawOutput={"secret": "value"},
        )
    )

    assert started is not None and started["kind"] == "tool_call"
    assert updated is not None and updated["kind"] == "tool_call_update"
    snapshot = updated["payload"]["snapshot"]
    assert snapshot["title"] == "Read configuration"
    assert snapshot["status"] == "completed"
    assert snapshot["rawInput"] == {"path": "/private/file"}
    assert snapshot["rawOutput"] == {"secret": "value"}
    assert updated["privacy"] == "mixed"
    assert "payload.snapshot.rawInput" in updated["private_fields"]
    assert "payload.snapshot.rawOutput" in updated["private_fields"]
    assert projector.project_turn_content("session-1")["assistant_stream_text"] == ""


def test_permission_request_updates_tool_and_keeps_options() -> None:
    projector = AcpEventProjector()
    event = projector.normalize_permission_request(
        {
            "jsonrpc": "2.0",
            "id": 42,
            "method": "session/request_permission",
            "params": {
                "sessionId": "session-1",
                "toolCall": {"toolCallId": "tool-9", "status": "pending"},
                "options": [
                    {"optionId": "yes", "name": "Allow", "kind": "allow_once"},
                    {"optionId": "no", "name": "Reject", "kind": "reject_once"},
                ],
            },
        },
    )

    assert event is not None
    assert event["kind"] == "tool_call_update"
    assert event["payload"]["permission"]["required"] is True
    assert event["payload"]["permission"]["options"][1]["optionId"] == "no"
    assert event["source_event_id"] == "42"
    assert event["event_id"] == "acp:session-1:42"

    assert (
        projector.normalize_permission_request(
            {
                "jsonrpc": "2.0",
                "id": 42,
                "method": "session/request_permission",
                "params": {
                    "sessionId": "session-1",
                    "toolCall": {"toolCallId": "tool-9", "status": "pending"},
                    "options": [],
                },
            }
        )
        is None
    )


def test_plan_usage_and_session_info_are_full_or_merged_snapshots() -> None:
    projector = AcpEventProjector()
    plan = projector.normalize_session_update(
        _update(
            "plan",
            entries=[
                {"content": "Implement", "priority": "high", "status": "in_progress"}
            ],
        )
    )
    usage = projector.normalize_session_update(
        _update("usage_update", used=25, size=100, cost={"amount": 0.1, "currency": "USD"})
    )
    title = projector.normalize_session_update(
        _update("session_info_update", title="ACP migration")
    )
    cleared = projector.normalize_session_update(
        _update("session_info_update", title=None, updatedAt="2026-07-31T12:00:00Z")
    )

    assert plan is not None and plan["payload"]["snapshot"] is True
    assert usage is not None and usage["payload"]["used"] == 25
    assert title is not None and title["payload"]["title"] == "ACP migration"
    assert cleared is not None and cleared["payload"]["title"] is None
    assert cleared["payload"]["updatedAt"] == "2026-07-31T12:00:00Z"


def test_explicit_event_ids_dedupe_replay_but_identical_unidentified_chunks_do_not() -> None:
    projector = AcpEventProjector()
    notification = _update(
        "agent_message_chunk",
        messageId="answer-1",
        content={"type": "text", "text": "ha"},
    )

    first = projector.normalize_session_update(
        notification, source_event_id="transport-sequence-7", replay=True
    )
    duplicate = projector.normalize_session_update(
        notification, source_event_id="transport-sequence-7", replay=True
    )
    repeated_text = projector.normalize_session_update(notification)

    assert first is not None and first["replay"] is True
    assert duplicate is None
    assert repeated_text is not None
    assert projector.project_turn_content("session-1")["assistant_stream_text"] == "haha"
    assert len(first["dedupe_hint"]) == 64


def test_meta_event_id_is_used_as_replay_key() -> None:
    projector = AcpEventProjector()
    notification = _update(
        "agent_message_chunk",
        messageId="answer-1",
        content={"type": "text", "text": "once"},
        _meta={"eventId": "adapter-99"},
    )
    assert projector.normalize_session_update(notification) is not None
    assert projector.normalize_session_update(notification) is None


def test_unknown_update_is_forward_compatible_and_malformed_input_is_rejected() -> None:
    projector = AcpEventProjector()
    assert projector.normalize_session_update(_update("future_update", value=1)) is None
    with pytest.raises(AcpProjectionError, match="sessionId"):
        projector.normalize_session_update(
            {"update": {"sessionUpdate": "agent_message_chunk", "content": {}}}
        )
    with pytest.raises(AcpProjectionError, match="toolCallId"):
        projector.normalize_permission_request(
            {"sessionId": "session-1", "toolCall": {}, "options": []}
        )


def test_failed_normalization_does_not_consume_sequence_or_replay_id() -> None:
    projector = AcpEventProjector()
    malformed = _update(
        "agent_message_chunk", messageId="answer-1", content="not-an-object"
    )
    with pytest.raises(AcpProjectionError, match="missing content"):
        projector.normalize_session_update(malformed, source_event_id="event-1")

    valid = _update(
        "agent_message_chunk",
        messageId="answer-1",
        content={"type": "text", "text": "recovered"},
    )
    event = projector.normalize_session_update(valid, source_event_id="event-1")
    assert event is not None
    assert event["sequence"] == 1
    assert event["dedupe_safe"] is True


def test_sessions_have_independent_ordering_and_defensive_snapshots() -> None:
    projector = AcpEventProjector()
    first = projector.normalize_session_update(
        _update("usage_update", used=1, size=10)
    )
    other = projector.normalize_session_update(
        {
            "sessionId": "session-2",
            "update": {"sessionUpdate": "usage_update", "used": 2, "size": 20},
        }
    )
    assert first is not None and first["sequence"] == 1
    assert other is not None and other["sequence"] == 1

    snapshot = projector.session_snapshot("session-1")
    assert snapshot is not None
    snapshot["usage"]["used"] = 999
    assert projector.session_snapshot("session-1")["usage"]["used"] == 1
