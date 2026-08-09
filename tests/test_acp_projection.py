"""Contract tests for ACP event normalization before durable ingestion."""

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


@pytest.mark.parametrize(
    ("update_kind", "fields"),
    [
        (
            "available_commands_update",
            {"availableCommands": [{"name": "review", "description": "Review"}]},
        ),
        ("current_mode_update", {"currentModeId": "agent"}),
        (
            "config_option_update",
            {"configOptions": [{"id": "model", "currentValue": "safe"}]},
        ),
    ],
)
def test_stable_control_updates_normalize_as_extensions(
    update_kind: str, fields: dict[str, object]
) -> None:
    projector = AcpEventProjector()
    event = projector.normalize_session_update(
        _update(update_kind, **fields), source_event_id="stable-update-1"
    )

    assert event == {
        "kind": "extension",
        "payload": {
            "schema_version": 1,
            "extension": f"acp.session_update.{update_kind}",
            "update": fields,
        },
        "sequence": 1,
        "source_event_id": "stable-update-1",
    }
    assert projector.normalize_session_update(
        _update(update_kind, **fields), source_event_id="stable-update-1"
    ) is None


def test_message_chunks_are_assembled_by_kind_and_message_id() -> None:
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


def test_non_text_and_assistant_only_content_do_not_become_public_text() -> None:
    projector = AcpEventProjector()
    image = projector.normalize_session_update(
        _update(
            "agent_message_chunk",
            messageId="image-1",
            content={"type": "image", "data": "base64", "mimeType": "image/png"},
        )
    )
    assistant_only = projector.normalize_session_update(
        _update(
            "agent_message_chunk",
            messageId="private-1",
            content={
                "type": "text",
                "text": "assistant-only context",
                "annotations": {"audience": ["assistant"]},
            },
        )
    )

    assert image is not None and image["payload"]["text_delta"] == ""
    assert assistant_only is not None
    assert assistant_only["payload"]["assembled_text"] == ""


def test_turn_content_is_ordered_read_only_and_excludes_thoughts() -> None:
    projector = AcpEventProjector()
    assert projector.turn_content("session-1") == {
        "user_text": "",
        "assistant_stream_text": "",
        "assistant_final_text": "",
        "complete": False,
        "has_open_turn": False,
    }
    for update in (
        _update(
            "user_message_chunk",
            messageId="user-1",
            content={"type": "text", "text": "first"},
        ),
        _update(
            "agent_thought_chunk",
            messageId="thought-1",
            content={"type": "text", "text": "private reasoning"},
        ),
        _update(
            "user_message_chunk",
            messageId="user-2",
            content={"type": "text", "text": "second"},
        ),
        _update(
            "agent_message_chunk",
            messageId="answer-1",
            content={"type": "text", "text": "answer"},
        ),
    ):
        assert projector.normalize_session_update(update) is not None

    assert projector.turn_content("session-1") == {
        "user_text": "first\n\nsecond",
        "assistant_stream_text": "answer",
        "assistant_final_text": "",
        "complete": False,
        "has_open_turn": True,
    }
    assert projector.turn_content("session-1", complete=True) == {
        "user_text": "first\n\nsecond",
        "assistant_stream_text": "",
        "assistant_final_text": "answer",
        "complete": True,
        "has_open_turn": False,
    }
    with pytest.raises(AcpProjectionError, match="another session"):
        projector.turn_content("session-2")


def test_tool_updates_merge_and_permission_attaches_options() -> None:
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
        _update("tool_call_update", toolCallId="tool-1", status="completed")
    )
    permission = projector.normalize_permission_request(
        {
            "jsonrpc": "2.0",
            "id": 7,
            "method": "session/request_permission",
            "params": {
                "sessionId": "session-1",
                "toolCall": {"toolCallId": "tool-1"},
                "options": [
                    {"optionId": "allow", "name": "Allow", "kind": "allow_once"}
                ],
            },
        }
    )

    assert started is not None and started["kind"] == "tool_call"
    assert updated is not None and updated["payload"]["snapshot"]["status"] == "completed"
    assert permission is not None and permission["kind"] == "tool_call_update"
    assert permission["payload"]["permission"]["options"][0]["optionId"] == "allow"


def test_permission_replaces_snapshot_and_preserves_limits_and_replay() -> None:
    projector = AcpEventProjector()

    def request(option_id: str) -> dict[str, object]:
        return {
            "sessionId": "session-1",
            "toolCall": {"toolCallId": "tool-1"},
            "options": [
                {
                    "optionId": option_id,
                    "name": option_id.title(),
                    "kind": "allow_once",
                }
            ],
        }

    first = projector.normalize_permission_request(
        request("allow"), source_event_id="permission-1"
    )
    assert first is not None
    assert projector.normalize_permission_request(
        request("allow"), source_event_id="permission-1"
    ) is None
    with pytest.raises(AcpProjectionError, match="reused"):
        projector.normalize_permission_request(
            request("different"), source_event_id="permission-1"
        )
    replacement = projector.normalize_permission_request(
        request("reject"), source_event_id="permission-2"
    )
    assert replacement is not None
    assert replacement["payload"]["snapshot"]["permission"] == {
        "required": True,
        "options": [
            {"optionId": "reject", "name": "Reject", "kind": "allow_once"}
        ],
    }
    assert replacement["payload"]["permission"] == replacement["payload"]["snapshot"][
        "permission"
    ]

    with pytest.raises(AcpProjectionError, match="field limit"):
        AcpEventProjector(max_state_fields=1).normalize_permission_request(
            request("allow")
        )
    with pytest.raises(AcpProjectionError, match="retained session state limit"):
        AcpEventProjector(max_session_state_bytes=1).normalize_permission_request(
            request("allow")
        )


def test_plan_usage_and_session_info_are_normalized() -> None:
    projector = AcpEventProjector()
    plan = projector.normalize_session_update(
        _update(
            "plan",
            entries=[{"content": "Ship", "priority": "high", "status": "pending"}],
        )
    )
    usage = projector.normalize_session_update(
        _update("usage_update", size=100, used=25, cost={"amount": 1, "currency": "USD"})
    )
    info = projector.normalize_session_update(
        _update("session_info_update", title="Current work", updatedAt="2026-01-01T00:00:00Z")
    )

    assert plan is not None and plan["payload"]["entries"][0]["content"] == "Ship"
    assert usage is not None and usage["payload"]["used"] == 25
    assert info is not None and info["payload"]["title"] == "Current work"


def test_malformed_supported_update_does_not_consume_sequence() -> None:
    projector = AcpEventProjector()
    with pytest.raises(AcpProjectionError):
        projector.normalize_session_update(
            _update("agent_message_chunk", content={"type": "text", "text": 7})
        )
    event = projector.normalize_session_update(
        _update(
            "agent_message_chunk",
            content={"type": "text", "text": "valid"},
        )
    )
    assert event is not None and event["sequence"] == 1


def test_unknown_update_is_forward_compatible() -> None:
    projector = AcpEventProjector()
    assert projector.normalize_session_update(_update("future_update", value=1)) is None


def test_explicit_source_identity_replay_is_idempotent_and_collision_fails() -> None:
    projector = AcpEventProjector()
    first = _update(
        "agent_message_chunk", content={"type": "text", "text": "first"}
    )
    assert projector.normalize_session_update(first, source_event_id="event-1") is not None
    assert projector.normalize_session_update(first, source_event_id="event-1") is None
    with pytest.raises(AcpProjectionError, match="reused"):
        projector.normalize_session_update(
            _update(
                "agent_message_chunk", content={"type": "text", "text": "different"}
            ),
            source_event_id="event-1",
        )


def test_size_and_depth_limits_fail_closed() -> None:
    with pytest.raises(AcpProjectionError, match="size limit"):
        AcpEventProjector(max_event_bytes=128).normalize_session_update(
            _update(
                "agent_message_chunk",
                content={"type": "text", "text": "x" * 300},
            )
        )

    nested: object = "leaf"
    for _ in range(10):
        nested = {"nested": nested}
    with pytest.raises(AcpProjectionError, match="nesting limit"):
        AcpEventProjector(max_json_depth=6).normalize_session_update(
            _update("session_info_update", title="ok", _meta=nested)
        )
