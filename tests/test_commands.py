"""Tests for the neutral command request/result/envelope contract."""

from __future__ import annotations

import json
from typing import Any

import pytest

from tendwire.core.commands import (
    ALLOWED_ACTIONS,
    COMMAND_ENVELOPE_SCHEMA_VERSION,
    COMMAND_ENVELOPE_V3_SCHEMA_VERSION,
    COMMAND_REQUEST_SCHEMA_VERSION,
    DRY_RUN_MUTATION_NO_RECEIPT_REJECTION_STATUSES,
    DISPOSITION_IN_PROGRESS,
    DISPOSITION_NO_RECEIPT,
    DISPOSITION_TERMINAL_ACCEPTED,
    DISPOSITION_TERMINAL_REJECTED,
    DISPOSITION_TERMINAL_UNCERTAIN,
    LIVE_MUTATION_NO_RECEIPT_REJECTION_STATUSES,
    TERMINAL_MUTATION_REJECTION_STATUSES,
    VALID_DISPOSITIONS,
    STATUS_ACCEPTED,
    STATUS_ANSWER_IN_PROGRESS,
    STATUS_BACKEND_UNAVAILABLE,
    STATUS_PENDING,
    STATUS_REQUEST_STATE_UNCERTAIN,
    STATUS_DRY_RUN,
    CANONICAL_MUTATION_VERSION,
    STATUS_AMBIGUOUS_TARGET,
    STATUS_INVALID_REQUEST,
    STATUS_NOT_FOUND,
    STATUS_REJECTED,
    STATUS_RESOLVED,
    STATUS_STALE_TARGET,
    VALID_STATUSES,
    CommandEnvelope,
    CommandRequest,
    CanonicalMutation,
    MAX_INSTRUCTION_LENGTH,
    build_canonical_mutation,
    build_selector_proof,
    is_selector_proof,
    is_valid_request_id,
    parse_command_request,
    resolve_target,
    sanitize_command_result,
    turn_submission_id,
    validate_instruction_text,
    validate_request,
    worker_candidate,
)
from tendwire.core.models import Worker


_FORBIDDEN_COMMAND_FIELDS = {
    "telegram",
    "chat_id",
    "chat_ids",
    "topic_id",
    "topic_ids",
    "message_id",
    "message_ids",
    "thread_id",
    "thread_ids",
    "route",
    "routes",
    "delivery",
    "deliveries",
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
    "pane_id",
    "pane_ids",
    "terminal_id",
    "terminal_ids",
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
    "window_id",
    "window_ids",
    "tab_id",
    "tab_ids",
    "argv",
    "args",
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
    "raw_command",
    "raw_command_line",
    "raw_command_lines",
    "raw_arg",
    "raw_args",
    "raw_argv",
    "raw_argvs",
    "raw_payload",
    "raw_control",
    "shell_command",
    "shell_commands",
    "terminal_control",
    "control_sequence",
    "escape_sequence",
    "ansi_escape",
    "shell",
    "stdin",
    "stdout",
    "stderr",
    "env",
    "environment",
    "connector",
    "connectors",
    "backend_target",
    "backend_targets",
    "agent_session",
    "session_id",
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
    "passwords",
    "api_keys",
}
_FORBIDDEN_COMMAND_COMPACT = {
    field.replace("_", "") for field in _FORBIDDEN_COMMAND_FIELDS
}


def _is_forbidden_command_key(key: Any) -> bool:
    normalized = str(key).lower().replace("-", "_").replace(".", "_")
    return (
        normalized in _FORBIDDEN_COMMAND_FIELDS
        or normalized.replace("_", "") in _FORBIDDEN_COMMAND_COMPACT
    )


def _assert_no_forbidden_fields(value: Any, path: str = "$") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            assert not _is_forbidden_command_key(key), f"forbidden field {path}.{key}"
            _assert_no_forbidden_fields(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _assert_no_forbidden_fields(item, f"{path}[{index}]")


def test_allowed_actions_frozen() -> None:
    assert ALLOWED_ACTIONS == {
        "noop",
        "read_snapshot",
        "resolve_target",
        "send_instruction",
        "answer_pending",
        "answer_decision",
    }


def test_duplicate_instruction_is_not_a_command_status() -> None:
    assert "duplicate_instruction" not in VALID_STATUSES


def test_command_request_defaults_are_dry_run() -> None:
    request = CommandRequest(action="noop")
    assert request.dry_run is True
    assert request.schema_version == 1
    assert request.request_id is None


def test_parse_command_request_requires_schema_version() -> None:
    request, error = parse_command_request(json.dumps({"action": "noop"}))
    assert request is None
    assert error is not None
    assert error["code"] == STATUS_INVALID_REQUEST


@pytest.mark.parametrize("value", ["1", 1.0, True, False, None, [], {}, 2])
def test_parse_command_request_rejects_non_integer_one_schema_version(value: Any) -> None:
    request, error = parse_command_request(json.dumps({"schema_version": value, "action": "noop"}))
    assert request is None
    assert error is not None
    assert error["code"] == STATUS_INVALID_REQUEST


def test_parse_command_request_accepts_integer_one_schema_version() -> None:
    request, error = parse_command_request(json.dumps({"schema_version": 1, "action": "noop"}))
    assert error is None
    assert request is not None
    assert request.schema_version == 1


def test_command_request_negotiates_response_v3_explicitly() -> None:
    default, default_error = parse_command_request(
        json.dumps({"schema_version": 1, "action": "noop"})
    )
    opted_in, opted_error = parse_command_request(
        json.dumps(
            {
                "schema_version": 1,
                "action": "noop",
                "response_schema_version": 3,
            }
        )
    )

    assert default_error is opted_error is None
    assert default is not None and default.response_schema_version == 2
    assert "response_schema_version" not in default.to_dict()
    assert opted_in is not None and opted_in.response_schema_version == 3
    assert opted_in.to_dict()["response_schema_version"] == 3


@pytest.mark.parametrize("value", [1, 4, True, "3", None])
def test_command_request_rejects_unsupported_response_schema(value: Any) -> None:
    request, error = parse_command_request(
        json.dumps(
            {
                "schema_version": 1,
                "action": "noop",
                "response_schema_version": value,
            }
        )
    )
    assert request is None
    assert error is not None
    assert error["code"] == STATUS_INVALID_REQUEST


@pytest.mark.parametrize("value", [True, False])
def test_parse_command_request_accepts_literal_boolean_dry_run(value: bool) -> None:
    request, error = parse_command_request(json.dumps({"schema_version": 1, "action": "noop", "dry_run": value}))
    assert error is None
    assert request is not None
    assert request.dry_run is value


@pytest.mark.parametrize("value", ["false", "true", 0, 1, None, [], {}])
def test_parse_command_request_rejects_non_boolean_dry_run(value: Any) -> None:
    request, error = parse_command_request(json.dumps({"schema_version": 1, "action": "noop", "dry_run": value}))
    assert request is None
    assert error is not None
    assert error["code"] == STATUS_INVALID_REQUEST


@pytest.mark.parametrize("value", ["false", "true", 0, 1, None, [], {}])
def test_validate_request_rejects_non_boolean_dry_run(value: Any) -> None:
    request = CommandRequest(action="noop", dry_run=value)
    error = validate_request(request)
    assert error is not None
    assert error["code"] == STATUS_INVALID_REQUEST


def test_command_request_to_dict_roundtrip() -> None:
    request = CommandRequest(
        action="send_instruction",
        request_id="req-1",
        dry_run=False,
        target={"worker_id": "w-1"},
        instruction={"text": "hello"},
        params={"extra": True},
    )
    data = request.to_dict()
    restored = CommandRequest.from_dict(data)
    assert restored == request


def test_build_canonical_send_instruction_has_hard_coded_v1_identity() -> None:
    request = CommandRequest(
        action="send_instruction",
        request_id="request-is-not-canonical",
        dry_run=False,
        target={
            "name": "raw selector spelling",
            "space_id": "raw-space",
            "worker_fingerprint": "transient-observation",
        },
        instruction={"text": "Deploy α\nnow"},
        params={"origin": {"source": "arbitrary"}, "null_noise": None},
    )

    mutation = build_canonical_mutation(request, public_worker_id="worker-public-7")

    assert isinstance(mutation, CanonicalMutation)
    assert mutation.canonical_version == CANONICAL_MUTATION_VERSION == 1
    assert mutation.action == "send_instruction"
    assert mutation.public_worker_id == "worker-public-7"
    assert mutation.canonical_json == (
        '{"action":"send_instruction","canonical_version":1,'
        '"instruction":{"text":"Deploy α\\nnow"},"options":{},'
        '"target":{"worker_id":"worker-public-7"}}'
    )
    assert mutation.fingerprint == "92368719174890d8481f2a6d"


def test_canonical_send_instruction_equates_selectors_after_resolution() -> None:
    by_name = CommandRequest(
        action="send_instruction",
        request_id="request-by-name",
        dry_run=False,
        target={
            "name": "Alpha",
            "space_id": "space-one",
            "worker_fingerprint": "transient-one",
        },
        instruction={"text": "exact text"},
        params={"origin": {"source": "one"}, "ignored": None},
    )
    by_id = CommandRequest(
        action="send_instruction",
        request_id="request-by-id",
        dry_run=False,
        target={"worker_id": "selector-worker", "worker_fingerprint": "transient-two"},
        instruction={"text": "exact text"},
        params={"origin": "different"},
    )

    first = build_canonical_mutation(by_name, public_worker_id="resolved-worker")
    second = build_canonical_mutation(by_id, public_worker_id="resolved-worker")

    assert first.canonical_json == second.canonical_json
    assert first.fingerprint == second.fingerprint
    assert "request-by-name" not in first.canonical_json
    assert "transient-one" not in first.canonical_json
    assert "origin" not in first.canonical_json
    assert '"options":{}' in first.canonical_json


@pytest.mark.parametrize(
    ("instruction_text", "public_worker_id"),
    [
        ("exact text changed", "resolved-worker"),
        ("exact text", "different-worker"),
    ],
)
def test_canonical_send_instruction_fingerprint_changes_for_semantics(
    instruction_text: str,
    public_worker_id: str,
) -> None:
    baseline_request = CommandRequest(
        action="send_instruction",
        request_id="baseline",
        dry_run=False,
        target={"worker_id": "selector"},
        instruction={"text": "exact text"},
    )
    changed_request = CommandRequest(
        action="send_instruction",
        request_id="changed",
        dry_run=False,
        target={"worker_id": "selector"},
        instruction={"text": instruction_text},
    )
    baseline = build_canonical_mutation(
        baseline_request,
        public_worker_id="resolved-worker",
    )

    changed = build_canonical_mutation(
        changed_request,
        public_worker_id=public_worker_id,
    )

    assert changed.fingerprint != baseline.fingerprint


def test_build_canonical_answer_pending_has_hard_coded_v1_identity() -> None:
    request = _answer_pending_request(request_id="not-canonical")

    mutation = build_canonical_mutation(request, public_worker_id="worker-public-7")

    assert mutation.canonical_version == 1
    assert mutation.action == "answer_pending"
    assert mutation.public_worker_id == "worker-public-7"
    assert mutation.canonical_json == (
        '{"action":"answer_pending","canonical_version":1,"options":{},'
        '"pending":{"choice_id":"choice-public",'
        '"pending_fingerprint":"pending-revision","pending_id":"pending-public"},'
        '"target":{"worker_id":"worker-public-7"}}'
    )
    assert mutation.fingerprint == "1a88307fbc8afd0a1205eaca"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("pending_id", "pending-public-changed"),
        ("pending_fingerprint", "pending-revision-changed"),
        ("choice_id", " choice-public "),
    ],
)
def test_canonical_answer_pending_fingerprint_changes_for_exact_semantics(
    field: str,
    value: str,
) -> None:
    baseline = build_canonical_mutation(
        _answer_pending_request(request_id="baseline"),
        public_worker_id="worker-public-7",
    )
    params = {
        "pending_id": "pending-public",
        "pending_fingerprint": "pending-revision",
        "choice_id": "choice-public",
    }
    params[field] = value
    changed = build_canonical_mutation(
        _answer_pending_request(request_id="changed", params=params),
        public_worker_id="worker-public-7",
    )

    assert changed.fingerprint != baseline.fingerprint


def test_command_envelope_shape_matches_contract() -> None:
    request = CommandRequest(action="noop")
    envelope = CommandEnvelope.from_result(request, ok=True, status="noop")
    payload = envelope.to_dict()
    assert set(payload) == {
        "schema_version",
        "action",
        "request_id",
        "ok",
        "dry_run",
        "status",
        "disposition",
        "result",
        "error",
        "warnings",
    }
    assert COMMAND_REQUEST_SCHEMA_VERSION == 1
    assert payload["schema_version"] == COMMAND_ENVELOPE_SCHEMA_VERSION == 2
    assert payload["ok"] is True
    assert payload["status"] == "noop"
    assert payload["disposition"] == DISPOSITION_NO_RECEIPT
    _assert_no_forbidden_fields(payload)


def test_command_envelope_v3_roundtrips_only_accepted_submission_fields() -> None:
    request = CommandRequest(
        action="send_instruction",
        request_id="v3-request",
        dry_run=False,
        target={"worker_id": "w-1"},
        instruction={"text": "hello"},
        response_schema_version=3,
    )
    result = {
        "turn_id": "turn-public",
        "submission_id": turn_submission_id("host-a", "v3-request"),
    }
    envelope = CommandEnvelope.from_result(
        request,
        ok=True,
        status=STATUS_ACCEPTED,
        disposition=DISPOSITION_TERMINAL_ACCEPTED,
        result=result,
        schema_version=COMMAND_ENVELOPE_V3_SCHEMA_VERSION,
    )

    assert envelope.schema_version == 3
    assert CommandEnvelope.from_dict(envelope.to_dict()).to_dict() == envelope.to_dict()
    pending_link = CommandEnvelope.from_result(
        request,
        ok=True,
        status=STATUS_ACCEPTED,
        disposition=DISPOSITION_TERMINAL_ACCEPTED,
        result={
            "turn_id": None,
            "submission_id": turn_submission_id("host-a", "v3-request"),
        },
        schema_version=COMMAND_ENVELOPE_V3_SCHEMA_VERSION,
    )
    assert pending_link.result["turn_id"] is None
    with pytest.raises(ValueError, match="accepted instruction submission"):
        CommandEnvelope.from_result(
            request,
            ok=True,
            status=STATUS_ACCEPTED,
            disposition=DISPOSITION_TERMINAL_ACCEPTED,
            result={"turn_id": "turn-public"},
            schema_version=COMMAND_ENVELOPE_V3_SCHEMA_VERSION,
        )


@pytest.mark.parametrize(
    ("disposition", "action", "ok", "status", "error"),
    [
        (DISPOSITION_NO_RECEIPT, "noop", True, "noop", None),
        (
            DISPOSITION_IN_PROGRESS,
            "send_instruction",
            False,
            STATUS_PENDING,
            {"code": STATUS_PENDING, "message": "pending"},
        ),
        (
            DISPOSITION_TERMINAL_ACCEPTED,
            "send_instruction",
            True,
            STATUS_ACCEPTED,
            None,
        ),
        (
            DISPOSITION_TERMINAL_REJECTED,
            "send_instruction",
            False,
            STATUS_BACKEND_UNAVAILABLE,
            {"code": STATUS_BACKEND_UNAVAILABLE, "message": "unavailable"},
        ),
        (
            DISPOSITION_TERMINAL_UNCERTAIN,
            "send_instruction",
            False,
            STATUS_REQUEST_STATE_UNCERTAIN,
            {"code": STATUS_REQUEST_STATE_UNCERTAIN, "message": "uncertain"},
        ),
    ],
)
def test_command_envelope_strictly_roundtrips_every_disposition(
    disposition: str,
    action: str,
    ok: bool,
    status: str,
    error: dict[str, Any] | None,
) -> None:
    request = CommandRequest(
        action=action,
        request_id="roundtrip-1" if action == "send_instruction" else None,
        dry_run=action != "send_instruction",
        target={"worker_id": "w-1"} if action == "send_instruction" else None,
        instruction={"text": "hello"} if action == "send_instruction" else None,
    )
    envelope = CommandEnvelope.from_result(
        request,
        ok=ok,
        status=status,
        disposition=disposition,
        error=error,
    )
    payload = envelope.to_dict()

    assert payload["disposition"] == disposition
    assert set(VALID_DISPOSITIONS) == {
        "no_receipt",
        "in_progress",
        "terminal_accepted",
        "terminal_rejected",
        "terminal_uncertain",
    }
    assert CommandEnvelope.from_dict(payload).to_dict() == payload


@pytest.mark.parametrize(
    "mutation",
    [
        lambda payload: payload.pop("disposition"),
        lambda payload: payload.__setitem__("disposition", "unknown"),
        lambda payload: payload.__setitem__("private_receipt", "leak"),
        lambda payload: payload.__setitem__("schema_version", 1),
    ],
)
def test_command_envelope_rejects_malformed_wire_shape(mutation: Any) -> None:
    payload = CommandEnvelope.from_result(
        CommandRequest(action="noop"),
        ok=True,
        status="noop",
    ).to_dict()
    mutation(payload)

    with pytest.raises((TypeError, ValueError)):
        CommandEnvelope.from_dict(payload)


@pytest.mark.parametrize(
    ("disposition", "ok", "status"),
    [
        (DISPOSITION_IN_PROGRESS, True, STATUS_PENDING),
        (DISPOSITION_TERMINAL_ACCEPTED, False, STATUS_ACCEPTED),
        (DISPOSITION_TERMINAL_REJECTED, False, STATUS_REQUEST_STATE_UNCERTAIN),
        (DISPOSITION_TERMINAL_UNCERTAIN, False, STATUS_PENDING),
    ],
)
def test_command_envelope_rejects_inconsistent_receipt_tuples(
    disposition: str,
    ok: bool,
    status: str,
) -> None:
    request = CommandRequest(
        action="send_instruction",
        request_id="tuple-1",
        dry_run=False,
        target={"worker_id": "w-1"},
        instruction={"text": "hello"},
    )
    error = None if ok else {"code": status, "message": "failed"}

    with pytest.raises(ValueError):
        CommandEnvelope.from_result(
            request,
            ok=ok,
            status=status,
            disposition=disposition,
            error=error,
        )


@pytest.mark.parametrize(
    "action", ["send_instruction", "answer_pending", "answer_decision"]
)
@pytest.mark.parametrize("request_id", [None, "", "not canonical"])
def test_command_envelope_live_mutations_require_canonical_request_ids(
    action: str,
    request_id: str | None,
) -> None:
    request = CommandRequest(
        action=action,
        request_id=request_id,
        dry_run=False,
        target={"worker_id": "w-1"} if action != "answer_pending" else None,
        instruction={"text": "hello"} if action == "send_instruction" else None,
        params=(
            {
                "pending_id": "pending-1",
                "pending_fingerprint": "revision-1",
                "choice_id": "choice-1",
            }
            if action == "answer_pending"
            else (
                {
                    "decision_ref": "decision-1",
                    "selection": {"option_refs": ["1"]},
                }
                if action == "answer_decision"
                else None
            )
        ),
    )

    with pytest.raises(ValueError, match="valid request_id"):
        CommandEnvelope.from_result(
            request,
            ok=False,
            status=STATUS_BACKEND_UNAVAILABLE,
            error={"code": STATUS_BACKEND_UNAVAILABLE, "message": "unavailable"},
        )


@pytest.mark.parametrize("request_id", [None, "", "not canonical"])
@pytest.mark.parametrize(
    ("disposition", "ok", "status"),
    [
        (DISPOSITION_IN_PROGRESS, False, STATUS_PENDING),
        (DISPOSITION_TERMINAL_ACCEPTED, True, STATUS_ACCEPTED),
        (DISPOSITION_TERMINAL_REJECTED, False, STATUS_BACKEND_UNAVAILABLE),
        (
            DISPOSITION_TERMINAL_UNCERTAIN,
            False,
            STATUS_REQUEST_STATE_UNCERTAIN,
        ),
    ],
)
def test_command_envelope_strict_parser_rejects_receipts_without_canonical_request_ids(
    request_id: str | None,
    disposition: str,
    ok: bool,
    status: str,
) -> None:
    request = CommandRequest(
        action="send_instruction",
        request_id="valid-id",
        dry_run=False,
        target={"worker_id": "w-1"},
        instruction={"text": "hello"},
    )
    payload = CommandEnvelope.from_result(
        request,
        ok=ok,
        status=status,
        disposition=disposition,
        error=None if ok else {"code": status, "message": "failed"},
    ).to_dict()
    payload["request_id"] = request_id

    with pytest.raises(ValueError, match="valid request_id"):
        CommandEnvelope.from_dict(payload)


def test_mutation_disposition_status_sets_are_explicit_and_fail_closed() -> None:
    assert TERMINAL_MUTATION_REJECTION_STATUSES == {
        "rejected",
        "stale_target",
        "backend_unavailable",
        "backend_unsupported",
        "ambiguous_backend_target",
        "backend_failed",
        "duplicate_request",
        "decision_not_pending",
        "unknown_worker",
        "invalid_selection",
        "unsupported_decision",
    }
    assert LIVE_MUTATION_NO_RECEIPT_REJECTION_STATUSES == {
        "invalid_request",
        "rejected",
        "not_found",
        "ambiguous_target",
        "stale_target",
        "backend_unavailable",
        "backend_unsupported",
        "ambiguous_backend_target",
        "backend_failed",
        "answer_in_progress",
        "decision_not_pending",
        "unknown_worker",
        "invalid_selection",
        "unsupported_decision",
    }
    assert DRY_RUN_MUTATION_NO_RECEIPT_REJECTION_STATUSES == {
        "invalid_request",
        "invalid_selection",
        "rejected",
        "not_found",
        "ambiguous_target",
        "stale_target",
    }


def test_answer_in_progress_allows_only_retryable_dispositions() -> None:
    request = CommandRequest(
        action="answer_decision",
        request_id="answer-race",
        dry_run=False,
        target={"worker_id": "w-1"},
        params={
            "decision_ref": "decision-1",
            "selection": {"option_refs": ["1"]},
        },
    )
    for disposition in (DISPOSITION_NO_RECEIPT, DISPOSITION_IN_PROGRESS):
        envelope = CommandEnvelope.from_result(
            request,
            ok=False,
            status=STATUS_ANSWER_IN_PROGRESS,
            disposition=disposition,
            error={"code": STATUS_ANSWER_IN_PROGRESS, "message": "racing"},
        )
        assert envelope.disposition == disposition
    for disposition in (
        DISPOSITION_TERMINAL_ACCEPTED,
        DISPOSITION_TERMINAL_REJECTED,
        DISPOSITION_TERMINAL_UNCERTAIN,
    ):
        with pytest.raises(ValueError):
            CommandEnvelope.from_result(
                request,
                ok=False,
                status=STATUS_ANSWER_IN_PROGRESS,
                disposition=disposition,
                error={"code": STATUS_ANSWER_IN_PROGRESS, "message": "racing"},
            )


@pytest.mark.parametrize(
    "action", ["send_instruction", "answer_pending", "answer_decision"]
)
@pytest.mark.parametrize("ok", [False, True])
@pytest.mark.parametrize("status", sorted(VALID_STATUSES))
def test_command_envelope_from_dict_enforces_terminal_rejected_matrix(
    action: str,
    ok: bool,
    status: str,
) -> None:
    payload = {
        "schema_version": COMMAND_ENVELOPE_SCHEMA_VERSION,
        "action": action,
        "request_id": "terminal-rejected-matrix",
        "ok": ok,
        "dry_run": False,
        "status": status,
        "disposition": DISPOSITION_TERMINAL_REJECTED,
        "result": {} if ok else None,
        "error": None if ok else {"message": "rejected before send"},
        "warnings": [],
    }
    allowed = not ok and status in TERMINAL_MUTATION_REJECTION_STATUSES

    if allowed:
        assert CommandEnvelope.from_dict(payload).to_dict() == payload
    else:
        with pytest.raises(ValueError, match="terminal_rejected"):
            CommandEnvelope.from_dict(payload)


@pytest.mark.parametrize(
    "action", ["send_instruction", "answer_pending", "answer_decision"]
)
@pytest.mark.parametrize("dry_run", [False, True], ids=["live", "dry-run"])
@pytest.mark.parametrize("ok", [False, True])
@pytest.mark.parametrize("status", sorted(VALID_STATUSES))
def test_command_envelope_from_dict_enforces_mutation_no_receipt_matrix(
    action: str,
    dry_run: bool,
    ok: bool,
    status: str,
) -> None:
    payload = {
        "schema_version": COMMAND_ENVELOPE_SCHEMA_VERSION,
        "action": action,
        "request_id": "no-receipt-matrix",
        "ok": ok,
        "dry_run": dry_run,
        "status": status,
        "disposition": DISPOSITION_NO_RECEIPT,
        "result": {} if ok else None,
        "error": None if ok else {"message": "failed before receipt"},
        "warnings": [],
    }
    if dry_run:
        allowed = (
            ok and status == STATUS_DRY_RUN
        ) or (
            not ok
            and status in DRY_RUN_MUTATION_NO_RECEIPT_REJECTION_STATUSES
        )
    else:
        allowed = (
            not ok
            and status in LIVE_MUTATION_NO_RECEIPT_REJECTION_STATUSES
            and (
                status != STATUS_ANSWER_IN_PROGRESS
                or action == "answer_decision"
            )
        )

    if allowed:
        assert CommandEnvelope.from_dict(payload).to_dict() == payload
    else:
        with pytest.raises(ValueError, match="no_receipt"):
            CommandEnvelope.from_dict(payload)


def test_validate_request_rejects_bad_schema_version() -> None:
    request = CommandRequest(action="noop", schema_version=2)
    error = validate_request(request)
    assert error is not None
    assert error["code"] == STATUS_INVALID_REQUEST


@pytest.mark.parametrize("value", ["1", 1.0, True, False, None, [], {}])
def test_validate_request_rejects_malformed_schema_version(value: Any) -> None:
    request = CommandRequest(action="noop", schema_version=value)
    error = validate_request(request)
    assert error is not None
    assert error["code"] == STATUS_INVALID_REQUEST


def test_validate_request_rejects_unknown_action() -> None:
    request = CommandRequest(action="explode")
    error = validate_request(request)
    assert error is not None
    assert error["code"] == STATUS_REJECTED


def test_validate_request_rejects_missing_action() -> None:
    request = CommandRequest(action="")
    error = validate_request(request)
    assert error is not None
    assert error["code"] == STATUS_INVALID_REQUEST


@pytest.mark.parametrize("field", sorted(_FORBIDDEN_COMMAND_FIELDS))
def test_validate_request_rejects_forbidden_connector_and_terminal_fields(field: str) -> None:
    request = CommandRequest(action="noop", params={field: "leaked"})
    error = validate_request(request)
    assert error is not None, field
    assert error["code"] == STATUS_INVALID_REQUEST
    assert field in str(error.get("details", {}))


def test_validate_request_rejects_forbidden_nested_fields() -> None:
    request = CommandRequest(
        action="send_instruction",
        target={"worker_id": "w-1"},
        instruction={"text": "ok"},
        params={"nested": {"pane_id": "leaked"}},
    )
    error = validate_request(request)
    assert error is not None
    assert error["code"] == STATUS_INVALID_REQUEST


def test_validate_request_rejects_dot_separated_forbidden_fields() -> None:
    request = CommandRequest(
        action="send_instruction",
        target={"worker_id": "w-1", "pane.id": "leaked"},
        instruction={"text": "ok", "raw.command": "leaked"},
        params={"nested": [{"backend.target": "leaked", "bot.token": "leaked"}]},
    )
    error = validate_request(request)
    assert error is not None
    assert error["code"] == STATUS_INVALID_REQUEST
    details = str(error.get("details", {}))
    assert "$.target.pane.id" in details
    assert "$.instruction.raw.command" in details
    assert "$.params.nested[0].backend.target" in details
    assert "$.params.nested[0].bot.token" in details


@pytest.mark.parametrize("field", sorted(_FORBIDDEN_COMMAND_FIELDS))
def test_parse_command_request_rejects_raw_top_level_forbidden_field(field: str) -> None:
    """Raw decoded JSON is rejected before from_dict drops unknown top-level keys."""
    payload = json.dumps(
        {
            "schema_version": 1,
            "action": "send_instruction",
            "request_id": "raw-rej",
            "dry_run": False,
            "target": {"worker_id": "w-1"},
            "instruction": {"text": "hello"},
            field: "leaked",
        }
    )
    request, error = parse_command_request(payload)
    assert request is None, field
    assert error is not None, field
    assert error["code"] == STATUS_INVALID_REQUEST, field
    assert field in str(error.get("details", {}))


def test_parse_command_request_rejects_raw_dot_separated_forbidden_fields() -> None:
    payload = json.dumps(
        {
            "schema_version": 1,
            "action": "send_instruction",
            "request_id": "raw-dot-rej",
            "dry_run": False,
            "target": {"worker_id": "w-1"},
            "instruction": {"text": "hello"},
            "message.id": "leaked",
            "params": {"nested": {"backend.target": "leaked"}},
        }
    )
    request, error = parse_command_request(payload)
    assert request is None
    assert error is not None
    assert error["code"] == STATUS_INVALID_REQUEST
    details = str(error.get("details", {}))
    assert "$.message.id" in details
    assert "$.params.nested.backend.target" in details


def test_parse_command_request_rejects_unknown_top_level_fields() -> None:
    payload = json.dumps({"schema_version": 1, "action": "noop", "surprise": True})
    request, error = parse_command_request(payload)

    assert request is None
    assert error is not None
    assert error["code"] == STATUS_INVALID_REQUEST
    assert "$.surprise" in str(error.get("details", {}))


def test_parse_command_request_rejects_raw_nested_forbidden_field() -> None:
    payload = json.dumps(
        {
            "schema_version": 1,
            "action": "send_instruction",
            "request_id": "raw-nested-rej",
            "dry_run": False,
            "target": {"worker_id": "w-1"},
            "instruction": {"text": "hello"},
            "params": {"nested": [{"raw": {"shell": "bash"}}]},
        }
    )
    request, error = parse_command_request(payload)

    assert request is not None
    assert request.request_id == "raw-nested-rej"
    assert error is not None
    assert error["code"] == STATUS_INVALID_REQUEST
    assert "$.params.nested[0].raw.shell" in str(error.get("details", {}))


def test_command_request_from_dict_drops_unknown_top_level_keys() -> None:
    """Unknown top-level keys disappear from the canonical request shape."""
    request = CommandRequest.from_dict(
        {
            "schema_version": 1,
            "action": "noop",
            "telegram": "leaked",
            "pane_id": "p-1",
        }
    )
    assert "telegram" not in request.to_dict()
    assert "pane_id" not in request.to_dict()


@pytest.mark.parametrize("field", ["pane_id", "terminal_id", "argv", "shell"])
def test_validate_request_rejects_disallowed_target_fields(field: str) -> None:
    request = CommandRequest(
        action="resolve_target",
        target={"worker_id": "w-1", field: "leaked"},
    )
    error = validate_request(request)
    assert error is not None
    assert error["code"] == STATUS_INVALID_REQUEST
    assert field in str(error.get("details", {}))

@pytest.mark.parametrize("field", ["argv", "command", "shell"])
def test_validate_request_rejects_disallowed_instruction_fields(field: str) -> None:
    request = CommandRequest(
        action="send_instruction",
        target={"worker_id": "w-1"},
        instruction={"text": "ok", field: "leaked"},
    )
    error = validate_request(request)
    assert error is not None
    assert error["code"] == STATUS_INVALID_REQUEST
    assert field in str(error.get("details", {}))


def test_validate_send_instruction_requires_target_and_text() -> None:
    missing_target = CommandRequest(
        action="send_instruction",
        instruction={"text": "ok"},
    )
    error = validate_request(missing_target)
    assert error is not None
    assert error["code"] == STATUS_INVALID_REQUEST

    empty_target = CommandRequest(
        action="send_instruction",
        target={},
        instruction={"text": "ok"},
    )
    error = validate_request(empty_target)
    assert error is not None
    assert error["code"] == STATUS_INVALID_REQUEST
    assert "stable target selector" in error["message"]

    missing_text = CommandRequest(
        action="send_instruction",
        target={"worker_id": "w-1"},
        instruction={"text": ""},
    )
    error = validate_request(missing_text)
    assert error is not None
    assert error["code"] == STATUS_INVALID_REQUEST


@pytest.mark.parametrize("action", ["send_instruction", "resolve_target"])
def test_validate_rejects_a_fingerprint_only_target(action: str) -> None:
    """A fingerprint names no worker, so it can never be the whole target.

    It is a mutable observation precondition. Alone it would make two genuinely
    different targets look identical to every identity-based check.
    """
    request = CommandRequest(
        action=action,
        request_id="fingerprint-only",
        dry_run=False,
        target={"worker_fingerprint": "fingerprint-A"},
        instruction={"text": "hello"} if action == "send_instruction" else None,
    )

    error = validate_request(request)

    assert error is not None
    assert error["code"] == STATUS_INVALID_REQUEST
    assert "worker_fingerprint" in error["message"]
    assert error["details"]["allowed"] == ["name", "space_id", "worker_id"]


@pytest.mark.parametrize(
    "stable",
    [
        {"worker_id": "w-1"},
        {"name": "Alpha"},
        {"space_id": "space-1"},
        {"name": "Alpha", "space_id": "space-1"},
    ],
)
def test_validate_accepts_a_fingerprint_beside_a_stable_selector(
    stable: dict[str, Any],
) -> None:
    request = CommandRequest(
        action="send_instruction",
        request_id="fingerprint-beside",
        dry_run=False,
        target={**stable, "worker_fingerprint": "fingerprint-A"},
        instruction={"text": "hello"},
    )

    assert validate_request(request) is None


@pytest.mark.parametrize(
    "stable",
    [
        {"worker_id": "w-1"},
        {"name": "Alpha"},
        {"space_id": "space-1"},
        {"name": "Alpha", "space_id": "space-1"},
    ],
)
def test_selector_proof_ignores_a_refreshed_worker_fingerprint(
    stable: dict[str, Any],
) -> None:
    """A refreshed fingerprint beside a stable selector is the same command."""

    def proof(fingerprint: str | None) -> str:
        target = dict(stable)
        if fingerprint is not None:
            target["worker_fingerprint"] = fingerprint
        return build_selector_proof(
            CommandRequest(
                action="send_instruction",
                request_id="proof",
                dry_run=False,
                target=target,
                instruction={"text": "hello"},
            )
        )

    assert proof("fingerprint-A") == proof("fingerprint-B") == proof(None)


def test_selector_proof_distinguishes_every_stable_selector_shape() -> None:
    """Each shape and value must hash differently, or a retry could cross targets."""

    def proof(target: dict[str, Any]) -> str:
        return build_selector_proof(
            CommandRequest(
                action="send_instruction",
                request_id="proof",
                dry_run=False,
                target=target,
                instruction={"text": "hello"},
            )
        )

    proofs = [
        proof({"worker_id": "w-1"}),
        proof({"worker_id": "w-2"}),
        proof({"name": "Alpha"}),
        proof({"name": "Beta"}),
        proof({"space_id": "space-1"}),
        proof({"space_id": "space-2"}),
        proof({"name": "Alpha", "space_id": "space-1"}),
        # A name that happens to equal a space, and vice versa, must not collide.
        proof({"name": "space-1"}),
        proof({"space_id": "Alpha"}),
    ]

    assert len(set(proofs)) == len(proofs)
    assert all(is_selector_proof(value) for value in proofs)


def test_selector_proof_requires_a_valid_target() -> None:
    fingerprint_only = CommandRequest(
        action="send_instruction",
        request_id="fingerprint-only",
        dry_run=False,
        target={"worker_fingerprint": "fingerprint-A"},
        instruction={"text": "hello"},
    )

    with pytest.raises(ValueError):
        build_selector_proof(fingerprint_only)


@pytest.mark.parametrize(
    "request_id",
    [
        "A",
        "0",
        ".",
        "_",
        "-",
        "Az09._-",
        "hri1_0123456789abcdef",
        "x" * 128,
    ],
)
def test_mutation_request_id_accepts_exact_ascii_tokens_and_roundtrips(
    request_id: str,
) -> None:
    requests = [
        CommandRequest(
            action="send_instruction",
            request_id=request_id,
            dry_run=False,
            target={"worker_id": "w-1"},
            instruction={"text": "ok"},
        ),
        _answer_pending_request(request_id=request_id),
    ]

    assert is_valid_request_id(request_id)
    for request in requests:
        assert validate_request(request) is None
        assert request.to_dict()["request_id"].encode("ascii") == request_id.encode("ascii")

        restored_request = CommandRequest.from_dict(request.to_dict())
        assert restored_request.request_id == request_id

        parsed_request, parse_error = parse_command_request(
            json.dumps(request.to_dict(), ensure_ascii=False)
        )
        assert parse_error is None
        assert parsed_request is not None
        assert parsed_request.request_id.encode("ascii") == request_id.encode("ascii")
        assert validate_request(parsed_request) is None

        envelope = CommandEnvelope.from_result(
            request,
            ok=True,
            status="accepted",
            disposition=DISPOSITION_TERMINAL_ACCEPTED,
        )
        serialized_envelope = envelope.to_json()
        assert json.loads(serialized_envelope)["request_id"].encode("ascii") == request_id.encode(
            "ascii"
        )
        restored_envelope = CommandEnvelope.from_dict(json.loads(serialized_envelope))
        assert restored_envelope.request_id == request_id


@pytest.mark.parametrize(
    "request_id",
    [
        pytest.param(None, id="none"),
        pytest.param(123, id="non-string"),
        pytest.param("", id="empty"),
        pytest.param("x" * 129, id="max-plus-one"),
        pytest.param("x" * ((1024 * 1024) - 256), id="near-frame-size"),
        pytest.param(" leading", id="leading-space"),
        pytest.param("trailing ", id="trailing-space"),
        pytest.param("interior space", id="interior-space"),
        pytest.param("\t", id="tab"),
        pytest.param("\n", id="newline"),
        pytest.param("\r", id="carriage-return"),
        pytest.param("\0", id="nul"),
        pytest.param("\x1f", id="unit-separator"),
        pytest.param("\x7f", id="delete"),
        pytest.param("é", id="unicode-nfc"),
        pytest.param("e\u0301", id="unicode-nfd"),
        pytest.param("Ａ", id="unicode-fullwidth"),
        pytest.param("K", id="unicode-normalizes-to-ascii"),
        pytest.param("request:id", id="disallowed-ascii-punctuation"),
    ],
)
def test_mutation_request_id_rejects_everything_outside_exact_ascii_grammar(
    request_id: Any,
) -> None:
    requests = [
        CommandRequest(
            action="send_instruction",
            request_id=request_id,
            dry_run=False,
            target={"worker_id": "w-1"},
            instruction={"text": "ok"},
        ),
        _answer_pending_request(request_id=request_id),
    ]

    assert not is_valid_request_id(request_id)
    for request in requests:
        error = validate_request(request)
        assert error is not None
        assert error["code"] == STATUS_INVALID_REQUEST
        assert error["details"] == {"field": "request_id"}

        parsed_request, parse_error = parse_command_request(
            json.dumps(request.to_dict(), ensure_ascii=False)
        )
        assert parse_error is None
        assert parsed_request is not None
        parsed_error = validate_request(parsed_request)
        assert parsed_error is not None
        assert parsed_error["code"] == STATUS_INVALID_REQUEST


def _answer_pending_request(
    *,
    request_id: str | None = "answer-1",
    dry_run: bool = False,
    params: Any = None,
) -> CommandRequest:
    return CommandRequest(
        action="answer_pending",
        request_id=request_id,
        dry_run=dry_run,
        params=params
        if params is not None
        else {
            "pending_id": "pending-public",
            "pending_fingerprint": "pending-revision",
            "choice_id": "choice-public",
        },
    )


def test_parse_answer_pending_accepts_exact_opaque_params() -> None:
    payload = {
        "schema_version": 1,
        "action": "answer_pending",
        "request_id": "answer-1",
        "dry_run": False,
        "params": {
            "pending_id": " opaque pending ",
            "pending_fingerprint": " opaque revision ",
            "choice_id": " opaque choice ",
        },
    }

    request, parse_error = parse_command_request(json.dumps(payload))

    assert parse_error is None
    assert request is not None
    assert validate_request(request) is None
    assert request.params == payload["params"]


@pytest.mark.parametrize(
    "changes,field",
    [
        ({"target": {"worker_id": "w-1"}}, "target"),
        ({"instruction": {"text": "private"}}, "instruction"),
        ({"params": None}, "params"),
        ({"params": {}}, "params"),
        (
            {
                "params": {
                    "pending_id": "pending-public",
                    "pending_fingerprint": "pending-revision",
                    "choice_id": "choice-public",
                    "extra": "no",
                }
            },
            "params",
        ),
        (
            {
                "params": {
                    "pending_id": "",
                    "pending_fingerprint": "pending-revision",
                    "choice_id": "choice-public",
                }
            },
            "params.pending_id",
        ),
        (
            {
                "params": {
                    "pending_id": "pending-public",
                    "pending_fingerprint": " \t",
                    "choice_id": "choice-public",
                }
            },
            "params.pending_fingerprint",
        ),
        (
            {
                "params": {
                    "pending_id": "pending-public",
                    "pending_fingerprint": "pending-revision",
                    "choice_id": 1,
                }
            },
            "params.choice_id",
        ),
    ],
)
def test_validate_answer_pending_rejects_non_exact_shape(
    changes: dict[str, Any],
    field: str,
) -> None:
    request = _answer_pending_request()
    data = request.to_dict()
    data.update(changes)

    error = validate_request(CommandRequest.from_dict(data))

    assert error is not None
    assert error["code"] == STATUS_INVALID_REQUEST
    assert field in str(error)


@pytest.mark.parametrize(
    "request_id",
    [None, "", "   \t", " leading", "trailing ", "\twrapped\t"],
)
def test_validate_answer_pending_non_dry_run_requires_canonical_request_id(
    request_id: str | None,
) -> None:
    error = validate_request(_answer_pending_request(request_id=request_id))

    assert error is not None
    assert error["code"] == STATUS_INVALID_REQUEST
    assert "request_id" in error["message"]


def test_validate_answer_pending_dry_run_does_not_require_request_id() -> None:
    assert validate_request(
        _answer_pending_request(request_id=None, dry_run=True)
    ) is None


@pytest.mark.parametrize(
    "text,expected_code",
    [
        ("", STATUS_INVALID_REQUEST),
        ("a" * (MAX_INSTRUCTION_LENGTH + 1), STATUS_INVALID_REQUEST),
        ("hello\x00world", STATUS_INVALID_REQUEST),
        ("hello\x1b[31mworld", STATUS_INVALID_REQUEST),
        ("hello\x1b]0;title\x07world", STATUS_INVALID_REQUEST),
        ("hello\x9b31mworld", STATUS_INVALID_REQUEST),
        ("hello\x7fworld", STATUS_INVALID_REQUEST),
        ("\x1b[200~paste\x1b[201~", STATUS_INVALID_REQUEST),
        ("hello\rworld", STATUS_INVALID_REQUEST),
        ("hello\x07world", STATUS_INVALID_REQUEST),
        ("hello\bworld", STATUS_INVALID_REQUEST),
        ("hello\fworld", STATUS_INVALID_REQUEST),
        ("hello\vworld", STATUS_INVALID_REQUEST),
        ("hello\x01world", STATUS_INVALID_REQUEST),
        ("hello\tworld", None),
        ("hello\nworld", None),
        ("plain text", None),
        ("unicode 🚀", None),
    ],
)
def test_validate_instruction_text_rejects_unsafe_input(text: str, expected_code: str | None) -> None:
    error = validate_instruction_text(text)
    if expected_code is None:
        assert error is None
    else:
        assert error is not None
        assert error["code"] == expected_code


def test_worker_candidate_is_neutral() -> None:
    worker = Worker(
        id="w-1",
        name="Agent One",
        status="active",
        space_id="s-1",
        summary="working",
        meta={"raw_status": "executing"},
    )
    candidate = worker_candidate(worker)
    assert set(candidate.keys()) <= {
        "worker_id",
        "name",
        "space_id",
        "status",
        "worker_fingerprint",
        "summary",
    }
    assert candidate["worker_id"] == "w-1"
    assert candidate["worker_fingerprint"] == worker.fingerprint
    _assert_no_forbidden_fields(candidate)


def _workers() -> list[Worker]:
    return [
        Worker(id="w-1", name="Alpha", status="active", space_id="s-1"),
        Worker(id="w-2", name="Beta", status="idle", space_id="s-1"),
        Worker(id="w-3", name="Alpha", status="waiting", space_id="s-2"),
        Worker(id="w-4", name="Closed", status="closed", space_id="s-1"),
    ]


def test_resolve_target_by_exact_worker_id() -> None:
    resolved, candidates, status = resolve_target({"worker_id": "w-2"}, _workers())
    assert status == STATUS_RESOLVED
    assert resolved is not None
    assert resolved["worker_id"] == "w-2"
    assert len(candidates) == 1


def test_resolve_target_not_found() -> None:
    resolved, candidates, status = resolve_target({"worker_id": "missing"}, _workers())
    assert status == STATUS_NOT_FOUND
    assert resolved is None
    assert candidates == []


def test_resolve_target_ambiguous_by_name() -> None:
    resolved, candidates, status = resolve_target({"name": "Alpha"}, _workers())
    assert status == STATUS_AMBIGUOUS_TARGET
    assert resolved is None
    assert len(candidates) == 2


def test_resolve_target_name_unique_after_space_filter() -> None:
    resolved, candidates, status = resolve_target({"name": "Alpha", "space_id": "s-2"}, _workers())
    assert status == STATUS_RESOLVED
    assert resolved is not None
    assert resolved["worker_id"] == "w-3"


def test_resolve_target_stale_fingerprint() -> None:
    workers = _workers()
    stale_fp = "deadbeef"
    resolved, candidates, status = resolve_target(
        {"worker_id": "w-1", "worker_fingerprint": stale_fp},
        workers,
    )
    assert status == STATUS_STALE_TARGET
    assert resolved is None
    assert len(candidates) == 1
    assert candidates[0]["worker_id"] == "w-1"


def test_resolve_target_matching_fingerprint() -> None:
    workers = _workers()
    fp = workers[0].fingerprint
    resolved, candidates, status = resolve_target(
        {"worker_id": "w-1", "worker_fingerprint": fp},
        workers,
    )
    assert status == STATUS_RESOLVED
    assert resolved is not None
    assert resolved["worker_id"] == "w-1"


def test_resolve_target_rejects_disallowed_status() -> None:
    resolved, candidates, status = resolve_target({"worker_id": "w-4"}, _workers())
    assert status == STATUS_REJECTED
    assert resolved is None
    assert len(candidates) == 1
    assert candidates[0]["status"] == "closed"


def test_command_envelope_roundtrip_via_dict() -> None:
    request = CommandRequest(action="resolve_target", request_id="r-1", dry_run=False)
    envelope = CommandEnvelope.from_result(
        request,
        ok=True,
        status=STATUS_RESOLVED,
        result={"target": {"worker_id": "w-1"}},
        warnings=["one"],
    )
    restored = CommandEnvelope.from_dict(envelope.to_dict())
    assert restored.to_dict() == envelope.to_dict()


_COMMAND_RESULT_FORBIDDEN_KEYS = {
    "pane_id",
    "pane_ids",
    "terminal_id",
    "terminal_ids",
    "pid",
    "pids",
    "process_id",
    "process_ids",
    "process",
    "tty",
    "pty",
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
    "window_id",
    "window_ids",
    "tab_id",
    "tab_ids",
    "argv",
    "args",
    "shell",
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
    "raw_command",
    "raw_command_line",
    "raw_command_lines",
    "raw_arg",
    "raw_args",
    "raw_argv",
    "raw_argvs",
    "raw_payload",
    "raw_control",
    "shell_command",
    "shell_commands",
    "terminal_control",
    "control_sequence",
    "escape_sequence",
    "ansi_escape",
    "stdin",
    "stdout",
    "stderr",
    "env",
    "environment",
    "route",
    "routes",
    "delivery",
    "deliveries",
    "token",
    "tokens",
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
    "connector",
    "connectors",
    "telegram",
    "chat_id",
    "chat_ids",
    "topic_id",
    "topic_ids",
    "message_id",
    "message_ids",
    "thread_id",
    "thread_ids",
    "bot_token",
    "herdres_delivery",
    "backend_target",
    "backend_targets",
    "agent_session",
    "session_id",
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
    "passwords",
    "api_keys",
}


def test_sanitize_command_result_strips_plural_and_variant_forbidden_keys() -> None:
    raw = {
        "safe": "kept",
        "pane_id": "p-1",
        "terminal_id": "t-1",
        "pid": 123,
        "process_id": 456,
        "tty": "/dev/pts/0",
        "shell": "bash",
        "command": "python app.py",
        "commandLine": "python app.py --token secret",
        "command-text": "python app.py",
        "command_payload": {"argv": ["python"]},
        "routes": ["r1"],
        "deliveries": [{"id": 1}],
        "tokens": "secret",
        "credentials": "secret",
        "cookie": "secret",
        "authToken": "secret",
        "connectors": {"telegram": "leaked"},
        "backend_target": {"kind": "agent_id", "value": "agent-1"},
        "agent_session": {"value": "sess-1"},
        "session_id": "session-1",
        "messageIds": "message-secret",
        "terminalIds": "terminal-secret",
        "terminal": "terminal-object-secret",
        "telegramMessageId": "telegram-message-secret",
        "routeId": "route-id-secret",
        "connectorId": "connector-id-secret",
        "tmuxPaneId": "tmux-pane-id-secret",
        "screenWindowId": "screen-window-id-secret",
        "agentSessionId": "agent-session-id-secret",
        "session": "session-object-secret",
        "privateFingerprints": "fingerprint-secret",
        "passwords": "password-secret",
        "nested": {
            "safe": "kept",
            "window_id": "w-1",
            "tab_id": "t-1",
            "processId": "proc-1",
            "tmux-session": "tmux-1",
            "terminalid": "term-compact",
            "backendTarget": {"value": "camel"},
            "argv": ["-c"],
            "rawArgs": ["--token", "secret"],
            "shellCommand": "bash -lc secret",
            "backend_target": {"value": "nested"},
        },
        "list": [
            {"safe": "kept", "route": "r", "screenSession": "screen-1", "raw-command-line": "secret"},
        ],
    }
    sanitized = sanitize_command_result(raw)
    assert sanitized == {
        "safe": "kept",
        "nested": {"safe": "kept"},
        "list": [{"safe": "kept"}],
    }
    _assert_no_forbidden_fields(sanitized)


def test_sanitize_command_result_preserves_legitimate_envelope_and_target_fields() -> None:
    envelope = {
        "schema_version": 1,
        "action": "send_instruction",
        "request_id": "req-1",
        "ok": True,
        "dry_run": False,
        "status": "accepted",
        "result": {
            "target": {
                "worker_id": "w-1",
                "space_id": "s-1",
                "worker_fingerprint": "fp",
                "name": "Alpha",
            },
            "snapshot": {
                "schema_version": 2,
                "host_id": "host",
                "content_fingerprint": "fp",
            },
        },
        "error": None,
        "warnings": [],
    }
    sanitized = sanitize_command_result(envelope)
    assert sanitized == envelope
