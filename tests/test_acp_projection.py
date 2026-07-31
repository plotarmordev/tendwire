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
    request = {
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
    }
    event = projector.normalize_permission_request(request, source_event_id="permission-42")

    assert event is not None
    assert event["kind"] == "tool_call_update"
    assert event["payload"]["permission"]["required"] is True
    assert event["payload"]["permission"]["options"][1]["optionId"] == "no"
    assert event["source_event_id"] == "permission-42"
    assert event["event_id"] == "acp:session-1:permission-42"
    assert "payload.permission" in event["private_fields"]

    assert (
        projector.normalize_permission_request(
            request, source_event_id="permission-42"
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


@pytest.mark.parametrize(
    "content",
    [
        {"type": "text", "text": "hello", "_meta": {"traceparent": "00-abc"}},
        {"type": "image", "data": "aW1hZ2U=", "mimeType": "image/png"},
        {"type": "audio", "data": "YXVkaW8=", "mimeType": "audio/wav"},
        {
            "type": "resource_link",
            "name": "source.py",
            "uri": "file:///workspace/source.py",
            "mimeType": "text/x-python",
            "size": 42,
        },
        {
            "type": "resource",
            "resource": {
                "uri": "file:///workspace/source.py",
                "mimeType": "text/x-python",
                "text": "print('ok')",
                "_meta": {"messageCount": 1},
            },
        },
    ],
)
def test_official_v1_content_block_shapes_are_accepted(content: dict[str, object]) -> None:
    projector = AcpEventProjector()
    event = projector.normalize_session_update(
        _update("agent_message_chunk", messageId="message-1", content=content)
    )

    assert event is not None
    assert event["payload"]["content"] == content


def test_official_v1_tool_shapes_are_validated_and_preserved() -> None:
    projector = AcpEventProjector()
    projector.normalize_session_update(
        _update(
            "tool_call",
            toolCallId="tool-1",
            title="Edit source",
            kind="edit",
            status="in_progress",
            locations=[{"path": "/workspace/source.py", "line": 7}],
        )
    )
    event = projector.normalize_session_update(
        _update(
            "tool_call_update",
            toolCallId="tool-1",
            status="completed",
            content=[
                {
                    "type": "diff",
                    "path": "/workspace/source.py",
                    "oldText": "old",
                    "newText": "new",
                    "_meta": {"traceparent": "00-tool"},
                },
                {"type": "terminal", "terminalId": "terminal-1"},
                {
                    "type": "content",
                    "content": {"type": "text", "text": "done"},
                },
            ],
        )
    )

    assert event is not None
    assert event["payload"]["snapshot"]["status"] == "completed"
    assert event["payload"]["snapshot"]["locations"] == [
        {"path": "/workspace/source.py", "line": 7}
    ]


@pytest.mark.parametrize(
    "notification,match",
    [
        (_update("agent_message_chunk", content={"type": "text"}), "string text"),
        (_update("agent_message_chunk", content={"type": "future"}), "unsupported type"),
        (_update("tool_call", toolCallId="tool-1"), "string title"),
        (
            _update(
                "tool_call",
                toolCallId="tool-1",
                title="Bad status",
                status="cancelled",
            ),
            "invalid status",
        ),
        (
            _update("plan", entries=[{"content": "missing fields"}]),
            "invalid priority",
        ),
        (_update("usage_update", used=1), "usage size"),
        (_update("usage_update", used=True, size=10), "usage used"),
        (_update("session_info_update", title=7), "title must be text"),
    ],
)
def test_malformed_supported_v1_updates_fail_without_allocating_state(
    notification: dict[str, object], match: str
) -> None:
    projector = AcpEventProjector()
    with pytest.raises(AcpProjectionError, match=match):
        projector.normalize_session_update(notification)
    assert projector.session_snapshot("session-1") is None


def test_usage_update_is_a_complete_snapshot_and_omission_clears_cost() -> None:
    projector = AcpEventProjector()
    projector.normalize_session_update(
        _update(
            "usage_update",
            used=5,
            size=100,
            cost={"amount": 0.25, "currency": "USD"},
        )
    )
    event = projector.normalize_session_update(
        _update("usage_update", used=7, size=100)
    )

    assert event is not None
    assert event["payload"] == {"used": 7, "size": 100}
    assert projector.session_snapshot("session-1")["usage"] == {
        "used": 7,
        "size": 100,
    }


def test_implicit_v1_message_is_split_after_an_update_type_boundary() -> None:
    projector = AcpEventProjector()
    first = projector.normalize_session_update(
        _update("agent_message_chunk", content={"type": "text", "text": "before"})
    )
    projector.normalize_session_update(
        _update("tool_call", toolCallId="tool-1", title="Boundary")
    )
    second = projector.normalize_session_update(
        _update("agent_message_chunk", content={"type": "text", "text": "after"})
    )

    assert first is not None and second is not None
    assert first["payload"]["message_id"] == "implicit-agent_message-1"
    assert second["payload"]["message_id"] == "implicit-agent_message-2"
    assert projector.project_turn_content("session-1")["assistant_stream_text"] == (
        "before\n\nafter"
    )


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


def test_meta_event_id_is_opaque_and_not_used_as_replay_key() -> None:
    projector = AcpEventProjector()
    notification = _update(
        "agent_message_chunk",
        messageId="answer-1",
        content={"type": "text", "text": "once"},
        _meta={"eventId": "adapter-99"},
    )
    first = projector.normalize_session_update(notification)
    second = projector.normalize_session_update(notification)
    assert first is not None and second is not None
    assert first["source_event_id"] is None
    assert first["payload"]["extensions"]["update"] == {"eventId": "adapter-99"}


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
    with pytest.raises(AcpProjectionError, match="invalid kind"):
        projector.normalize_permission_request(
            {
                "sessionId": "session-1",
                "toolCall": {"toolCallId": "tool-1"},
                "options": [
                    {"optionId": "maybe", "name": "Maybe", "kind": "sometimes"}
                ],
            }
        )


def test_failed_normalization_does_not_consume_sequence_or_replay_id() -> None:
    projector = AcpEventProjector()
    malformed = _update(
        "agent_message_chunk", messageId="answer-1", content="not-an-object"
    )
    with pytest.raises(AcpProjectionError, match="missing object content"):
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


def test_interleaved_explicit_messages_and_implicit_v1_chunks_do_not_alias() -> None:
    projector = AcpEventProjector()

    for message_id, text in (("a", "A1"), ("b", "B")):
        projector.normalize_session_update(
            _update(
                "agent_message_chunk",
                messageId=message_id,
                content={"type": "text", "text": text},
            )
        )
    with pytest.raises(AcpProjectionError, match="reused after a message boundary"):
        projector.normalize_session_update(
            _update(
                "agent_message_chunk",
                messageId="a",
                content={"type": "text", "text": "A2"},
            )
        )
    implicit = projector.normalize_session_update(
        _update(
            "agent_message_chunk",
            content={"type": "text", "text": "stable-v1"},
        )
    )

    assert implicit is not None
    assert implicit["payload"]["message_id"] == "implicit-agent_message-1"
    assert projector.project_turn_content("session-1")["assistant_stream_text"] == (
        "A1\n\nB\n\nstable-v1"
    )


def test_replay_identity_collision_is_rejected_and_sessions_are_isolated() -> None:
    projector = AcpEventProjector()
    first = _update(
        "agent_message_chunk",
        content={"type": "text", "text": "one"},
    )
    assert projector.normalize_session_update(first, source_event_id="event-7")
    assert projector.normalize_session_update(first, source_event_id="event-7") is None

    with pytest.raises(AcpProjectionError, match="reused for different content"):
        projector.normalize_session_update(
            _update(
                "agent_message_chunk",
                content={"type": "text", "text": "different"},
            ),
            source_event_id="event-7",
        )

    other = {
        "sessionId": "session-2",
        "update": first["params"]["update"],
    }
    isolated = projector.normalize_session_update(other, source_event_id="event-7")
    assert isolated is not None and isolated["sequence"] == 1


def test_permission_request_ids_do_not_collide_with_notification_event_ids() -> None:
    projector = AcpEventProjector()
    notification = _update(
        "tool_call",
        toolCallId="tool-1",
        title="Tracked tool",
        status="pending",
        _meta={"eventId": 42},
    )
    assert projector.normalize_session_update(
        notification, source_event_id="notification-42"
    ) is not None
    permission = projector.normalize_permission_request(
        {
            "jsonrpc": "2.0",
            "id": 42,
            "method": "session/request_permission",
            "params": {
                "sessionId": "session-1",
                "toolCall": {"toolCallId": "tool-1"},
                "options": [
                    {"optionId": "yes", "name": "Allow", "kind": "allow_once"}
                ],
            },
        }
    )
    assert permission is not None
    assert permission["source_event_id"] is None


def test_namespaced_extension_metadata_is_private_and_adapter_neutral() -> None:
    projector = AcpEventProjector(max_sessions=1)
    # Unknown variants neither allocate a session nor consume ordering state.
    assert projector.normalize_session_update(_update("vendor/future", value=1)) is None
    event = projector.normalize_session_update(
        {
            "params": {
                "sessionId": "session-2",
                "_meta": {"vendor.example/params": {"trace": "abc"}},
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "content": {
                        "type": "text",
                        "text": "safe",
                        "_meta": {
                            "vendor.example/content": {"revision": 1},
                            "unscoped": "discard",
                        },
                    },
                    "_meta": {
                        "vendor.example/update": {"opaque": True},
                        "adapterInternal": "discard",
                    },
                },
            },
        }
    )
    assert event is not None
    assert event["sequence"] == 1
    assert event["payload"]["extensions"] == {
        "params": {"vendor.example/params": {"trace": "abc"}},
        "update": {
            "vendor.example/update": {"opaque": True},
            "adapterInternal": "discard",
        },
    }
    assert event["payload"]["content"]["_meta"] == {
        "vendor.example/content": {"revision": 1},
        "unscoped": "discard",
    }
    assert "adapterInternal" in repr(event["payload"])
    assert "unscoped" in repr(event["payload"])


def test_plan_is_a_validated_full_replacement() -> None:
    projector = AcpEventProjector()
    projector.normalize_session_update(
        _update(
            "plan",
            entries=[{"content": "old", "priority": "medium", "status": "pending"}],
        )
    )
    replacement = projector.normalize_session_update(
        _update(
            "plan",
            entries=[{"content": "new", "priority": "high", "status": "completed"}],
        )
    )
    assert replacement is not None
    assert replacement["payload"]["entries"] == [
        {"content": "new", "priority": "high", "status": "completed"}
    ]
    with pytest.raises(AcpProjectionError, match="entries must be an array"):
        projector.normalize_session_update(_update("plan", entries="bad"))
    assert projector.session_snapshot("session-1")["plan"] == [
        {"content": "new", "priority": "high", "status": "completed"}
    ]


def test_completion_is_not_reopened_by_session_updates_and_requires_reset() -> None:
    projector = AcpEventProjector()
    projector.normalize_session_update(
        _update(
            "agent_message_chunk",
            content={"type": "text", "text": "final"},
        )
    )
    projector.mark_turn_complete("session-1")
    projector.normalize_session_update(_update("usage_update", used=9, size=10))
    projector.normalize_session_update(
        _update("session_info_update", title="still complete")
    )
    assert projector.project_turn_content("session-1")["complete"] is True
    assert projector.project_turn_content("session-1", complete=False)["complete"] is True
    with pytest.raises(AcpProjectionError, match="reset_turn"):
        projector.normalize_session_update(
            _update(
                "agent_message_chunk",
                content={"type": "text", "text": "late"},
            )
        )

    projector.reset_turn("session-1")
    projector.normalize_session_update(
        _update(
            "agent_message_chunk",
            content={"type": "text", "text": "next"},
        )
    )
    assert projector.project_turn_content("session-1")["assistant_stream_text"] == "next"


def test_bounded_state_fails_closed_and_drop_session_releases_capacity() -> None:
    projector = AcpEventProjector(
        max_sessions=1,
        max_source_events_per_session=1,
        max_messages_per_kind=1,
        max_tool_calls_per_session=1,
        max_state_fields=2,
        max_plan_entries=1,
        max_text_chars_per_message=3,
        max_event_bytes=1024,
    )
    assert projector.normalize_session_update(
        _update("usage_update", used=1, size=10), source_event_id="one"
    )
    with pytest.raises(AcpProjectionError, match="replay window"):
        projector.normalize_session_update(
            _update("usage_update", used=2, size=10), source_event_id="two"
        )
    with pytest.raises(AcpProjectionError, match="session limit"):
        projector.normalize_session_update(
            {
                "sessionId": "session-2",
                "update": {"sessionUpdate": "usage_update", "used": 1, "size": 10},
            }
        )
    assert projector.drop_session("session-1") is True
    assert projector.drop_session("session-1") is False
    assert projector.normalize_session_update(
        {
            "sessionId": "session-2",
            "update": {"sessionUpdate": "usage_update", "used": 1, "size": 10},
        }
    )


def test_failed_or_oversized_input_does_not_allocate_or_mutate_session() -> None:
    projector = AcpEventProjector(max_sessions=1, max_event_bytes=128)
    with pytest.raises(AcpProjectionError, match="bounded JSON"):
        projector.normalize_session_update(
            _update("session_info_update", invalid={1, 2, 3})
        )
    assert projector.session_snapshot("session-1") is None

    with pytest.raises(AcpProjectionError, match="size limit"):
        projector.normalize_session_update(
            _update("session_info_update", value="x" * 200)
        )
    assert projector.session_snapshot("session-1") is None
    accepted = projector.normalize_session_update(
        {
            "sessionId": "session-2",
            "update": {"sessionUpdate": "usage_update", "used": 1, "size": 10},
        }
    )
    assert accepted is not None and accepted["sequence"] == 1


def test_aggregate_retained_session_state_has_a_hard_budget() -> None:
    projector = AcpEventProjector(max_session_state_bytes=12)
    with pytest.raises(AcpProjectionError, match="retained session state"):
        projector.normalize_session_update(
            _update("session_info_update", title="far too large")
        )
    assert projector.session_snapshot("session-1") is None


def test_total_retained_state_is_bounded_across_isolated_sessions() -> None:
    projector = AcpEventProjector(
        max_session_state_bytes=100,
        max_total_state_bytes=18,
    )
    assert projector.normalize_session_update(
        {
            "sessionId": "session-a",
            "update": {"sessionUpdate": "session_info_update", "title": "1234"},
        }
    )
    with pytest.raises(AcpProjectionError, match="total retained state"):
        projector.normalize_session_update(
            {
                "sessionId": "session-b",
                "update": {
                    "sessionUpdate": "session_info_update",
                    "title": "1234",
                },
            }
        )
    assert projector.session_snapshot("session-b") is None


def test_all_non_message_events_remain_unreachable_from_legacy_turns() -> None:
    projector = AcpEventProjector()
    projector.normalize_session_update(
        _update(
            "agent_thought_chunk",
            content={"type": "text", "text": "do not leak thought"},
        )
    )
    projector.normalize_session_update(
        _update(
            "tool_call",
            toolCallId="tool-secret",
            title="Private tool",
            rawInput={"secret": "do not leak raw input"},
        )
    )
    projector.normalize_session_update(
        _update(
            "plan",
            entries=[
                {
                    "content": "do not leak plan",
                    "priority": "low",
                    "status": "pending",
                }
            ],
        )
    )
    projector.normalize_permission_request(
        {
            "jsonrpc": "2.0",
            "id": "permission-secret",
            "method": "session/request_permission",
            "params": {
                "sessionId": "session-1",
                "toolCall": {"toolCallId": "tool-secret"},
                "options": [
                    {"optionId": "secret", "name": "Private", "kind": "reject_once"}
                ],
            },
        }
    )
    legacy = projector.project_turn_content("session-1")
    assert legacy == {
        "user_text": "",
        "assistant_stream_text": "",
        "assistant_final_text": "",
        "complete": False,
        "has_open_turn": False,
    }
