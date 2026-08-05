"""Tests for public turn and pending-interaction contracts."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
import pytest

from tendwire.config import Config
from tendwire.core.models import (
    AttentionSignal,
    Snapshot,
    SuggestedAction,
    Worker,
    sanitize_canonical_turn_text,
)
from .model_helpers import project_from_raw
from tendwire.core.turns import (
    InteractionChoice,
    PendingInteraction,
    PendingObservation,
    PendingObservedChoice,
    TURN_CONTENT_PAGE_MAX_UTF8_BYTES,
    TURN_LIST_CURSOR_TTL_SECONDS,
    TURN_LIST_DEFAULT_LIMIT,
    TURN_LIST_MAX_LIMIT,
    build_turn_content_page,
    content_cursor,
    content_revision,
    content_segment_id,
    decode_content_cursor,
    decode_turn_list_cursor,
    decode_turn_since_token,
    project_turn_content,
    segment_canonical_text,
    turn_list_cursor,
    turn_since_token,
)


_FORBIDDEN_FIELDS = {
    "telegram",
    "chat_id",
    "chat_ids",
    "topic_id",
    "topic_ids",
    "message_id",
    "message_ids",
    "thread_id",
    "thread_ids",
    "token",
    "tokens",
    "bot_token",
    "bot_tokens",
    "auth",
    "auth_token",
    "auth_tokens",
    "authorization",
    "authorization_header",
    "authorization_headers",
    "bearer_token",
    "bearer_tokens",
    "cookie",
    "cookies",
    "credential",
    "credentials",
    "delivery",
    "deliveries",
    "route",
    "routes",
    "connector",
    "connectors",
    "herdres_delivery",
    "command",
    "command_arg",
    "command_args",
    "command_argv",
    "command_argvs",
    "command_line",
    "command_lines",
    "command_payload",
    "command_text",
    "command_texts",
    "backend_target",
    "backend_targets",
    "terminal_id",
    "terminal_ids",
    "pane_id",
    "pane_ids",
    "tab_id",
    "tab_ids",
    "window_id",
    "window_ids",
    "tty",
    "pty",
    "pid",
    "pids",
    "process_id",
    "process_ids",
    "process",
    "tmux",
    "tmux_session",
    "tmux_sessions",
    "tmux_window",
    "tmux_windows",
    "tmux_pane",
    "tmux_panes",
    "screen",
    "screen_session",
    "screen_sessions",
    "screen_window",
    "screen_windows",
    "agent_session",
    "agent_sessions",
    "session_id",
    "session_ids",
    "herdr_state",
    "herdres_state",
    "target_kind",
    "target_value",
    "turn_target_kind",
    "turn_target_value",
    "private",
    "private_binding",
    "private_bindings",
    "private_fingerprint",
    "private_fingerprints",
    "argv",
    "args",
    "env",
    "raw_arg",
    "raw_args",
    "raw_argv",
    "raw_argvs",
    "stderr",
    "stdout",
    "stdin",
    "secret",
    "secrets",
    "password",
    "passwords",
    "api_keys",
    "api_key",
    "raw_command",
    "raw_command_line",
    "raw_command_lines",
    "raw_payload",
    "raw_control",
    "shell_command",
    "shell_commands",
    "terminal_control",
    "control_sequence",
    "escape_sequence",
    "ansi_escape",
}
_FORBIDDEN_FIELD_COMPACT = {field.replace("_", "") for field in _FORBIDDEN_FIELDS}


def _is_forbidden_test_key(key: Any) -> bool:
    normalized = str(key).lower().replace("-", "_")
    return normalized in _FORBIDDEN_FIELDS or normalized.replace("_", "") in _FORBIDDEN_FIELD_COMPACT


def _assert_no_forbidden_fields(value: Any, path: str = "$") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            assert not _is_forbidden_test_key(key), f"forbidden field {path}.{key}"
            _assert_no_forbidden_fields(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _assert_no_forbidden_fields(item, f"{path}[{index}]")


def _assert_no_private_sentinels(value: Any) -> None:
    encoded = json.dumps(value, sort_keys=True)
    assert "sentinel-" not in encoded
    assert "private-" not in encoded






def test_pending_observation_models_explicit_outcomes_and_private_picker_routes() -> None:
    choice = PendingObservedChoice(
        choice_id="choice-" + ("a" * 24),
        label="Approve",
        picker_ordinal=1,
    )
    observation = PendingObservation(
        kind="open_prompt",
        question="Continue?",
        pending_kind="choice",
        choices=(choice,),
        revision_digest="private-revision-digest",
    )

    assert observation.kind == "open_prompt"
    assert observation.choices == (choice,)
    assert not hasattr(observation, "to_dict")
    for kind in (
        "read_succeeded_no_prompt",
        "read_succeeded_invalid_prompt",
        "read_succeeded_unsupported_decision",
        "read_failed",
        "worker_authoritatively_absent",
    ):
        assert PendingObservation(kind=kind).kind == kind

    with pytest.raises(ValueError, match="cannot carry prompt data"):
        PendingObservation(kind="read_failed", question="must not survive")
    with pytest.raises(ValueError, match="must be unique"):
        PendingObservation(
            kind="open_prompt",
            question="Continue?",
            choices=(choice, choice),
            revision_digest="private-revision-digest",
        )
    with pytest.raises(ValueError, match="picker ordinal"):
        PendingObservedChoice(
            choice_id="choice-" + ("b" * 24),
            label="Reject",
            picker_ordinal=0,
        )


def test_pending_interaction_preserves_only_valid_supplied_revision_bound_id() -> None:
    supplied_id = "pending-" + ("c" * 24)
    authoritative = PendingInteraction(
        id=supplied_id,
        host_id="pending-id-host",
        worker_id="worker-1",
        question="Continue?",
    )
    canonical = PendingInteraction(
        host_id="pending-id-host",
        worker_id="worker-1",
        question="Continue?",
    )
    invalid = PendingInteraction(
        id="sentinel-private-tool-decision",
        host_id="pending-id-host",
        worker_id="worker-1",
        question="Continue?",
    )

    assert authoritative.id == supplied_id
    assert authoritative.fingerprint == canonical.fingerprint
    assert invalid.id == canonical.id
    assert "sentinel-private" not in json.dumps(invalid.to_dict(), sort_keys=True)


def test_pending_interaction_serializes_current_choices_and_ignores_timestamps() -> None:
    choice = InteractionChoice(label="Approve")
    pending = PendingInteraction(
        host_id="pending-host",
        worker_id="worker-1",
        worker_fingerprint="worker-fp",
        space_id="space-1",
        kind="confirm_destructive_action",
        question="Delete generated files?",
        choices=[choice],
        status="open",
        created_at="2026-01-01T00:00:00+00:00",
        updated_at="2026-01-01T00:00:01+00:00",
        expires_at="2026-01-01T00:05:00+00:00",
        meta={"source": "attention", "message_id": 99},
    )
    same_logical_pending = PendingInteraction(
        host_id="pending-host",
        worker_id="worker-1",
        worker_fingerprint="worker-fp",
        space_id="space-1",
        kind="confirm_destructive_action",
        question="Delete generated files?",
        choices=[choice],
        status="open",
        created_at="2026-01-02T00:00:00+00:00",
        updated_at="2026-01-02T00:00:01+00:00",
        expires_at="2026-01-02T00:05:00+00:00",
        meta={"source": "attention"},
    )

    payload = pending.to_dict()

    assert payload["schema_version"] == 1
    assert payload["kind"] == "confirm_destructive_action"
    assert payload["status"] == "open"
    assert payload["choices"] == [{"choice_id": choice.choice_id, "label": "Approve"}]
    assert "decision" not in json.dumps(payload)
    assert pending.id == same_logical_pending.id
    assert pending.fingerprint == same_logical_pending.fingerprint
    _assert_no_forbidden_fields(payload)




























def test_large_multibyte_canonical_content_pages_reassemble_exactly() -> None:
    text = ("😀漢字e\u0301\r\n# heading\n- item\n```text\nx\n```\n" * 35_000)
    canonical = sanitize_canonical_turn_text(text)
    assert canonical is not None
    assert len(canonical.encode("utf-8")) > 1024 * 1024

    segments = segment_canonical_text(canonical)

    assert "".join(segment.text for segment in segments) == canonical
    assert sum(segment.char_length for segment in segments) == len(canonical)
    assert sum(segment.byte_length for segment in segments) == len(canonical.encode("utf-8"))
    assert all(
        segment.byte_length == len(segment.text.encode("utf-8"))
        and segment.byte_length <= TURN_CONTENT_PAGE_MAX_UTF8_BYTES
        and segment.start_char == (segments[index - 1].end_char if index else 0)
        for index, segment in enumerate(segments)
    )
    assert segments[-1].end_char == len(canonical)
    assert segments == segment_canonical_text(canonical)

@pytest.mark.parametrize(
    ("byte_length", "expected_page_bytes"),
    (
        (TURN_CONTENT_PAGE_MAX_UTF8_BYTES - 1, (TURN_CONTENT_PAGE_MAX_UTF8_BYTES - 1,)),
        (TURN_CONTENT_PAGE_MAX_UTF8_BYTES, (TURN_CONTENT_PAGE_MAX_UTF8_BYTES,)),
        (TURN_CONTENT_PAGE_MAX_UTF8_BYTES + 1, (TURN_CONTENT_PAGE_MAX_UTF8_BYTES - 3, 4)),
    ),
)
def test_multibyte_content_pages_honor_exact_utf8_byte_boundaries(
    byte_length: int,
    expected_page_bytes: tuple[int, ...],
) -> None:
    text = ("a" * (byte_length - 4)) + "😀"

    segments = segment_canonical_text(text)

    assert len(text.encode("utf-8")) == byte_length
    assert tuple(segment.byte_length for segment in segments) == expected_page_bytes
    assert "".join(segment.text for segment in segments) == text
    assert segments[-1].text.endswith("😀")



def test_content_identities_and_cursors_are_deterministic_and_revision_bound() -> None:
    turn_id = "turn-" + ("a" * 24)
    user_text = " prompt "
    final_text = "final😀" * 10_000
    revision = content_revision(
        turn_id,
        user_text,
        final_text,
        "complete",
        "complete",
    )
    same_revision = content_revision(
        turn_id,
        user_text,
        final_text,
        "complete",
        "complete",
    )
    changed_revision = content_revision(
        turn_id,
        user_text,
        final_text + "!",
        "complete",
        "complete",
    )
    cursor = content_cursor(
        revision,
        "assistant_final_text",
        1,
        start_char=6_144,
        start_byte=9_216,
    )

    assert revision == same_revision
    assert revision.startswith("twrev1.")
    assert changed_revision != revision
    assert content_segment_id(revision, "assistant_final_text", 1).startswith("twseg1.")
    assert content_segment_id(revision, "assistant_final_text", 1) == content_segment_id(
        revision,
        "assistant_final_text",
        1,
    )
    assert cursor.startswith("twcur1.")
    position = decode_content_cursor(
        cursor,
        revision=revision,
        field="assistant_final_text",
        count=3,
    )
    assert position.index == 1
    assert position.segment_id == content_segment_id(
        revision, "assistant_final_text", 1
    )
    assert position.start_char == 6_144
    assert position.start_byte == 9_216
    with pytest.raises(ValueError, match="invalid_cursor"):
        content_cursor(revision, "assistant_final_text", 1)
    with pytest.raises(ValueError, match="invalid_cursor"):
        decode_content_cursor(
            cursor,
            revision=changed_revision,
            field="assistant_final_text",
            count=3,
        )
    with pytest.raises(ValueError, match="invalid_cursor"):
        decode_content_cursor(
            cursor[:-1] + ("A" if cursor[-1] != "A" else "B"),
            revision=revision,
            field="assistant_final_text",
            count=3,
        )
    with pytest.raises(ValueError, match="invalid_cursor"):
        decode_content_cursor(
            cursor,
            revision=revision,
            field="user_text",
            count=3,
        )

@pytest.mark.parametrize(
    "cursor",
    (
        "",
        "not-a-cursor",
        "twcur1.",
        "twcur1.!!!!",
        "twcur1.e30",
    ),
)
def test_content_cursor_rejects_malformed_encodings(cursor: str) -> None:
    revision = "twrev1." + ("d" * 43)

    with pytest.raises(ValueError, match="invalid_cursor"):
        decode_content_cursor(
            cursor,
            revision=revision,
            field="assistant_final_text",
            count=2,
        )


def test_content_cursor_rejects_valid_integrity_at_out_of_range_index() -> None:
    revision = "twrev1." + ("e" * 43)
    cursor = content_cursor(
        revision,
        "assistant_final_text",
        2,
        start_char=20_000,
        start_byte=40_000,
    )

    with pytest.raises(ValueError, match="invalid_cursor"):
        decode_content_cursor(
            cursor,
            revision=revision,
            field="assistant_final_text",
            count=2,
        )


def test_v2_descriptor_and_page_payload_have_exact_lengths_and_cursor_progression() -> None:
    turn_id = "turn-" + ("b" * 24)
    user_text = "short"
    final_text = ("😀" * 20_000) + "\r\n "
    projection = project_turn_content(turn_id, user_text, final_text)
    descriptor = projection["content"]
    revision = descriptor["content_revision"]
    final_descriptor = descriptor["fields"]["assistant_final_text"]

    assert descriptor["schema_version"] == 1
    assert descriptor["known_incomplete"] is False
    assert descriptor["fields"]["user_text"]["inline"] is True
    assert final_descriptor["inline"] is False
    assert final_descriptor["char_length"] == len(final_text)
    assert final_descriptor["byte_length"] == len(final_text.encode("utf-8"))
    assert final_descriptor["first_cursor"] == content_cursor(
        revision,
        "assistant_final_text",
        0,
    )
    first = build_turn_content_page(
        turn_id,
        revision,
        "assistant_final_text",
        final_text,
    )
    second = build_turn_content_page(
        turn_id,
        revision,
        "assistant_final_text",
        final_text,
        cursor=first["next_cursor"],
    )
    assert first["index"] == 0
    assert second["index"] == 1
    assert first["text"] + second["text"] == final_text
    assert first["segment_byte_length"] <= TURN_CONTENT_PAGE_MAX_UTF8_BYTES
    assert second["segment_byte_length"] <= TURN_CONTENT_PAGE_MAX_UTF8_BYTES
    assert first["total_char_length"] == second["total_char_length"] == len(final_text)
    assert first["total_byte_length"] == second["total_byte_length"] == len(
        final_text.encode("utf-8")
    )


def test_known_incomplete_projection_is_explicit_and_never_pageable_or_inline() -> None:
    turn_id = "turn-" + ("c" * 24)
    fragment = "legacy fragment\n[truncated]"

    projection = project_turn_content(
        turn_id,
        None,
        fragment,
        final_state="known_incomplete",
    )
    descriptor = projection["content"]
    final_descriptor = descriptor["fields"]["assistant_final_text"]
    complete_revision = content_revision(
        turn_id,
        None,
        fragment,
        "absent",
        "complete",
    )

    assert descriptor["known_incomplete"] is True
    assert final_descriptor == {
        "availability": "known_incomplete",
        "inline": False,
        "char_length": len(fragment),
        "byte_length": len(fragment.encode("utf-8")),
        "page_count": 0,
        "first_cursor": None,
    }
    assert "assistant_final_text" not in projection
    assert projection["assistant_final_preview"] == fragment
    assert descriptor["content_revision"] != complete_revision




def test_turn_list_cursor_round_trip_binds_complete_request_and_expiry() -> None:
    cursor = turn_list_cursor(
        "host-public",
        schema_version=2,
        limit=37,
        since_sequence=4,
        watermark=91,
        floor_sequence=1,
        traversal_generation=7,
        worker_id="worker-public",
        list_sequence=77,
        turn_id="turn-public",
        store_epoch="epoch-public",
        expires_at=1_900,
    )

    position = decode_turn_list_cursor(
        cursor,
        host_id="host-public",
        schema_version=2,
        limit=37,
        now=1_000,
    )

    assert cursor.startswith("twlist1.")
    assert position.schema_version == 2
    assert position.limit == 37
    assert position.since_sequence == 4
    assert position.watermark == 91
    assert position.floor_sequence == 1
    assert position.traversal_generation == 7
    assert position.worker_id == "worker-public"
    assert position.list_sequence == 77
    assert position.turn_id == "turn-public"
    assert position.store_epoch == "epoch-public"
    assert position.expires_at == 1_900
    assert TURN_LIST_DEFAULT_LIMIT == 100
    assert TURN_LIST_MAX_LIMIT == 250
    assert TURN_LIST_CURSOR_TTL_SECONDS == 900


def test_turn_list_cursor_rejects_tamper_cross_binding_and_expiry_distinctly() -> None:
    cursor = turn_list_cursor(
        "host-a",
        schema_version=1,
        limit=1,
        since_sequence=0,
        watermark=2,
        floor_sequence=1,
        traversal_generation=1,
        worker_id="worker-a",
        list_sequence=2,
        turn_id="turn-a",
        store_epoch="epoch-a",
        expires_at=2_000,
    )
    tampered = cursor[:-1] + ("A" if cursor[-1] != "A" else "B")

    for candidate, host, schema, limit in (
        (tampered, "host-a", 1, 1),
        (cursor, "host-b", 1, 1),
        (cursor, "host-a", 2, 1),
        (cursor, "host-a", 1, 2),
        ("twlist1.!!!!", "host-a", 1, 1),
        ("twsince1.e30", "host-a", 1, 1),
    ):
        with pytest.raises(ValueError, match="invalid_cursor"):
            decode_turn_list_cursor(
                candidate,
                host_id=host,
                schema_version=schema,
                limit=limit,
                now=1_000,
            )
    with pytest.raises(ValueError, match="cursor_expired"):
        decode_turn_list_cursor(
            cursor,
            host_id="host-a",
            schema_version=1,
            limit=1,
            now=2_000,
        )


def test_turn_since_token_is_deterministic_strict_and_store_epoch_bound() -> None:
    token = turn_since_token(
        "host-a",
        schema_version=2,
        watermark=123,
        store_epoch="epoch-a",
    )
    same = turn_since_token(
        "host-a",
        schema_version=2,
        watermark=123,
        store_epoch="epoch-a",
    )

    position = decode_turn_since_token(
        token,
        host_id="host-a",
        schema_version=2,
    )

    assert token == same
    assert token.startswith("twsince1.")
    assert position.schema_version == 2
    assert position.watermark == 123
    assert position.store_epoch == "epoch-a"
    for candidate, host, schema in (
        (token[:-1] + ("A" if token[-1] != "A" else "B"), "host-a", 2),
        (token, "host-b", 2),
        (token, "host-a", 1),
        ("twsince1.!!!!", "host-a", 2),
        ("twlist1.e30", "host-a", 2),
    ):
        with pytest.raises(ValueError, match="invalid_cursor"):
            decode_turn_since_token(
                candidate,
                host_id=host,
                schema_version=schema,
            )
