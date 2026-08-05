"""Fixed ACP-only command protocol contract tests."""

from __future__ import annotations

import json
from typing import Any

import pytest

from tendwire.core.commands import (
    ALLOWED_ACTIONS,
    CANONICAL_MUTATION_VERSION,
    COMMAND_ENVELOPE_SCHEMA_VERSION,
    COMMAND_REQUEST_SCHEMA_VERSION,
    DISPOSITION_IN_PROGRESS,
    DISPOSITION_NO_RECEIPT,
    DISPOSITION_TERMINAL_ACCEPTED,
    DISPOSITION_TERMINAL_REJECTED,
    DISPOSITION_TERMINAL_UNCERTAIN,
    LIVE_MUTATION_NO_RECEIPT_REJECTION_STATUSES,
    STATUS_ACCEPTED,
    STATUS_AMBIGUOUS_TARGET,
    STATUS_ANSWER_IN_PROGRESS,
    STATUS_BACKEND_UNAVAILABLE,
    STATUS_DUPLICATE_REQUEST,
    STATUS_INVALID_REQUEST,
    STATUS_NOT_FOUND,
    STATUS_PENDING,
    STATUS_REJECTED,
    STATUS_REQUEST_STATE_UNCERTAIN,
    STATUS_STALE_TARGET,
    TERMINAL_MUTATION_REJECTION_STATUSES,
    CommandEnvelope,
    CommandRequest,
    build_canonical_mutation,
    build_selector_proof,
    instruction_fingerprint,
    is_selector_proof,
    is_valid_request_id,
    parse_command_request,
    resolve_target,
    turn_submission_id,
    validate_instruction_text,
    validate_public_command_envelope,
    validate_request,
    worker_candidate,
)
from tendwire.core.models import FORBIDDEN_FIELD_NAMES, Worker


def _send(**changes: Any) -> dict[str, Any]:
    value = {
        "schema_version": 1,
        "action": "send_instruction",
        "request_id": "request-1",
        "target": {"worker_id": "worker-1"},
        "instruction": {"text": "do it"},
    }
    value.update(changes)
    return value


def _answer(**changes: Any) -> dict[str, Any]:
    value = {
        "schema_version": 1,
        "action": "answer_decision",
        "request_id": "request-1",
        "target": {"worker_id": "worker-1"},
        "params": {
            "decision_ref": "decision-1",
            "selection": {"option_refs": ["1"]},
        },
    }
    value.update(changes)
    return value


def _request(value: dict[str, Any]) -> CommandRequest:
    request, error = parse_command_request(json.dumps(value))
    assert error is None and request is not None
    assert validate_request(request) is None
    return request


def _envelope(
    *,
    action: str = "send_instruction",
    status: str,
    disposition: str,
    ok: bool,
    request_id: str | None = "request-1",
) -> CommandEnvelope:
    return CommandEnvelope(
        action=action,
        status=status,
        disposition=disposition,
        ok=ok,
        request_id=request_id,
    )


def test_fixed_protocol_versions_actions_and_shapes() -> None:
    assert COMMAND_REQUEST_SCHEMA_VERSION == 1
    assert COMMAND_ENVELOPE_SCHEMA_VERSION == 2
    assert ALLOWED_ACTIONS == {"send_instruction", "answer_decision"}
    assert _request(_send()).to_dict() == _send()
    assert _request(_answer()).to_dict() == _answer()


@pytest.mark.parametrize("removed", ["dry_run", "response_schema_version"])
@pytest.mark.parametrize("factory", [_send, _answer])
def test_removed_request_modes_are_unknown(
    factory: Any,
    removed: str,
) -> None:
    request, error = parse_command_request(json.dumps(factory(**{removed: False})))
    assert request is None
    assert error is not None and error["code"] == STATUS_INVALID_REQUEST


@pytest.mark.parametrize(
    "value",
    [
        _send(params={}),
        {key: value for key, value in _send().items() if key != "instruction"},
        _answer(instruction={"text": "collision"}),
        {key: value for key, value in _answer().items() if key != "params"},
    ],
)
def test_action_exact_keys_prevent_uncanonicalized_cross_action_data(
    value: dict[str, Any],
) -> None:
    request, error = parse_command_request(json.dumps(value))
    assert request is None
    assert error is not None and error["code"] == STATUS_INVALID_REQUEST


@pytest.mark.parametrize("value", [None, True, False, 0, 2, 1.0, "1"])
def test_request_schema_is_exact_integer_one(value: Any) -> None:
    request, error = parse_command_request(json.dumps(_send(schema_version=value)))
    assert request is None
    assert error is not None and error["details"]["field"] == "schema_version"


@pytest.mark.parametrize("value", ["", " ", "a/b", "é", "a" * 129, True, 1, None])
def test_request_id_is_strict(value: Any) -> None:
    assert not is_valid_request_id(value)
    request = CommandRequest.from_dict(_send(request_id=value))
    assert validate_request(request)["details"]["field"] == "request_id"


@pytest.mark.parametrize("value", ["a", "A_Z-9.0", "x" * 128])
def test_request_id_accepts_exact_ascii_tokens(value: str) -> None:
    assert is_valid_request_id(value)
    assert validate_request(CommandRequest.from_dict(_send(request_id=value))) is None


@pytest.mark.parametrize("field", sorted(FORBIDDEN_FIELD_NAMES))
def test_forbidden_private_fields_fail_closed_at_every_depth(field: str) -> None:
    top, top_error = parse_command_request(json.dumps({**_send(), field: "private"}))
    nested, nested_error = parse_command_request(
        json.dumps(_send(instruction={"text": "do it", "nested": {field: "private"}}))
    )
    assert top is None and top_error is not None
    assert nested is None and nested_error is not None
    assert top_error["code"] == nested_error["code"] == STATUS_INVALID_REQUEST


@pytest.mark.parametrize(
    "target",
    [
        {"name": "removed"},
        {"worker_fingerprint": "mutable-only"},
        {"worker_id": 123},
        {"worker_id": " "},
        {"space_id": None},
        {"worker_id": "worker-1", "space_id": "space-1"},
        {"space_id": "space-1", "worker_fingerprint": "fp"},
        {"stable_key": "wsk1_" + "a" * 64},
        {"stable_key_version": 1},
        {"stable_key": "bad", "stable_key_version": 1},
        {"stable_key": "wsk1_" + "a" * 64, "stable_key_version": True},
    ],
)
def test_target_shape_rejects_removed_or_unstable_selectors(target: dict[str, Any]) -> None:
    request, error = parse_command_request(json.dumps(_send(target=target)))
    if request is None:
        assert error is not None
    else:
        assert validate_request(request) is not None


@pytest.mark.parametrize(
    "target",
    [
        {"worker_id": "worker-1"},
        {"worker_id": "worker-1", "worker_fingerprint": "fp"},
        {"space_id": "space-1"},
        {"stable_key": "wsk1_" + "a" * 64, "stable_key_version": 1},
    ],
)
def test_supported_selectors_validate(target: dict[str, Any]) -> None:
    assert validate_request(CommandRequest.from_dict(_send(target=target))) is None


def test_canonical_mutation_ignores_selector_spelling_and_fingerprint() -> None:
    worker_id = "worker-1"
    requests = [
        _request(_send(target={"worker_id": worker_id})),
        _request(_send(target={"worker_id": worker_id, "worker_fingerprint": "fresh"})),
        _request(_send(target={"space_id": "space-1"})),
        _request(_send(target={"stable_key": "wsk1_" + "a" * 64, "stable_key_version": 1})),
    ]
    canonical = [build_canonical_mutation(item, public_worker_id=worker_id) for item in requests]
    assert {item.canonical_version for item in canonical} == {CANONICAL_MUTATION_VERSION}
    assert len({item.canonical_json for item in canonical}) == 1
    assert len({item.fingerprint for item in canonical}) == 1
    assert "fresh" not in canonical[0].canonical_json


def test_canonical_mutation_changes_for_instruction_and_decision_semantics() -> None:
    first = build_canonical_mutation(_request(_send()), public_worker_id="worker-1")
    second = build_canonical_mutation(
        _request(_send(instruction={"text": "different"})),
        public_worker_id="worker-1",
    )
    answer_a = build_canonical_mutation(_request(_answer()), public_worker_id="worker-1")
    answer_b = build_canonical_mutation(
        _request(_answer(params={
            "decision_ref": "decision-1",
            "selection": {"option_refs": ["2"]},
        })),
        public_worker_id="worker-1",
    )
    assert len({first.fingerprint, second.fingerprint, answer_a.fingerprint, answer_b.fingerprint}) == 4


def test_answer_rejects_duplicate_option_refs() -> None:
    request = CommandRequest.from_dict(_answer(params={
        "decision_ref": "decision-1",
        "selection": {"option_refs": ["1", "1"]},
    }))
    error = validate_request(request)
    assert error is not None and error["code"] == "invalid_selection"


def test_selector_proof_preserves_frozen_v1_empty_name_slot() -> None:
    stable = _request(_send(target={
        "stable_key": "wsk1_" + "a" * 64,
        "stable_key_version": 1,
    }))
    space = _request(_send(target={"space_id": "space-a"}))
    worker = _request(_send(target={"worker_id": "worker-1"}))
    assert build_selector_proof(stable) == (
        "v1:aa215841f96e972a245123d4661f277b5d68c5c9338c13cfdb3a2617275a19f3"
    )
    assert build_selector_proof(space) == (
        "v1:a9e28528782c6d6ab91362cdbc5e1ff7a2fff0626cb8e010851f0102233fe03a"
    )
    assert build_selector_proof(worker) == (
        "v1:79178a9d2724ad80cefa2a5e3f90f56278a36fb896f04313f307a450cbba96fd"
    )
    assert all(is_selector_proof(build_selector_proof(item)) for item in (stable, space, worker))


def test_selector_proof_ignores_only_worker_fingerprint() -> None:
    plain = _request(_send(target={"worker_id": "worker-1"}))
    observed = _request(_send(target={
        "worker_id": "worker-1",
        "worker_fingerprint": "changed",
    }))
    assert build_selector_proof(plain) == build_selector_proof(observed)


@pytest.mark.parametrize(
    ("disposition", "status", "ok"),
    [
        (DISPOSITION_IN_PROGRESS, STATUS_PENDING, False),
        (DISPOSITION_IN_PROGRESS, STATUS_ANSWER_IN_PROGRESS, False),
        (DISPOSITION_TERMINAL_ACCEPTED, STATUS_ACCEPTED, True),
        (DISPOSITION_TERMINAL_REJECTED, STATUS_REJECTED, False),
        (DISPOSITION_TERMINAL_REJECTED, STATUS_DUPLICATE_REQUEST, False),
        (DISPOSITION_TERMINAL_UNCERTAIN, STATUS_REQUEST_STATE_UNCERTAIN, False),
        (DISPOSITION_NO_RECEIPT, STATUS_BACKEND_UNAVAILABLE, False),
    ],
)
def test_envelope_exact_schema2_roundtrips_valid_live_tuples(
    disposition: str,
    status: str,
    ok: bool,
) -> None:
    envelope = _envelope(
        action="answer_decision" if status == STATUS_ANSWER_IN_PROGRESS else "send_instruction",
        disposition=disposition,
        status=status,
        ok=ok,
    )
    data = envelope.to_dict()
    assert set(data) == {
        "schema_version", "action", "request_id", "ok", "dry_run",
        "status", "disposition", "result", "error", "warnings",
    }
    assert data["schema_version"] == 2 and data["dry_run"] is False
    assert CommandEnvelope.from_dict(data).to_dict() == data


@pytest.mark.parametrize(
    "mutation",
    [
        {"dry_run": True},
        {"dry_run": 0},
        {"schema_version": 3},
        {"schema_version": True},
        {"extra": None},
    ],
)
def test_envelope_rejects_nonfixed_wire_shape(mutation: dict[str, Any]) -> None:
    data = _envelope(
        disposition=DISPOSITION_TERMINAL_ACCEPTED,
        status=STATUS_ACCEPTED,
        ok=True,
    ).to_dict()
    data.update(mutation)
    with pytest.raises((TypeError, ValueError)):
        CommandEnvelope.from_dict(data)


@pytest.mark.parametrize(
    ("disposition", "status", "ok"),
    [
        (DISPOSITION_IN_PROGRESS, STATUS_ACCEPTED, True),
        (DISPOSITION_TERMINAL_ACCEPTED, STATUS_REJECTED, False),
        (DISPOSITION_TERMINAL_REJECTED, STATUS_PENDING, False),
        (DISPOSITION_TERMINAL_UNCERTAIN, STATUS_REJECTED, False),
        (DISPOSITION_NO_RECEIPT, STATUS_DUPLICATE_REQUEST, False),
    ],
)
def test_envelope_rejects_inconsistent_disposition_status_tuples(
    disposition: str,
    status: str,
    ok: bool,
) -> None:
    with pytest.raises(ValueError):
        _envelope(disposition=disposition, status=status, ok=ok)


def test_envelope_normalizes_unknown_or_malformed_request_failure() -> None:
    unknown = CommandRequest(action="old_action", request_id="request-1")
    envelope = CommandEnvelope.from_error(
        unknown,
        {"code": STATUS_REJECTED, "message": "unknown", "details": {}},
    )
    assert envelope.action == ""
    assert envelope.request_id is None
    assert envelope.status == STATUS_INVALID_REQUEST
    assert envelope.dry_run is False


def test_public_success_validator_enforces_fixed_send_and_answer_shapes() -> None:
    send_request = _request(_send())
    send = CommandEnvelope.from_result(
        send_request,
        ok=True,
        status=STATUS_ACCEPTED,
        disposition=DISPOSITION_TERMINAL_ACCEPTED,
        result={
            "target": {"worker_id": "worker-1"},
            "delivery_state": "submitted",
            "transport_state": "submitted",
            "target_state_at_send": "idle",
            "turn_id": None,
            "observed_turn_state": "pending_observation",
            "submission_verdict": "submitted",
            "submission_id": "twsub1." + "a" * 64,
        },
    )
    assert validate_public_command_envelope(send, send_request) is send
    answer_request = _request(_answer())
    answer = CommandEnvelope.from_result(
        answer_request,
        ok=True,
        status=STATUS_ACCEPTED,
        disposition=DISPOSITION_TERMINAL_ACCEPTED,
        result={
            "target": {"worker_id": "worker-1"},
            "decision": {"decision_ref": "decision-1"},
            "delivery_state": "submitted",
            "transport_state": "submitted",
            "observed_pending_state": "pending_observation",
        },
    )
    assert validate_public_command_envelope(answer, answer_request) is answer


@pytest.mark.parametrize(
    "mutation",
    [
        {"submission_id": "twsub1.BAD"},
        {"submission_verdict": "unknown"},
        {"turn_id": ""},
        {"target": {"worker_id": "worker-2"}},
    ],
)
def test_public_success_validator_rejects_malformed_send(
    mutation: dict[str, Any],
) -> None:
    request = _request(_send())
    result = {
        "target": {"worker_id": "worker-1"},
        "delivery_state": "submitted",
        "transport_state": "submitted",
        "target_state_at_send": "idle",
        "turn_id": None,
        "observed_turn_state": "pending_observation",
        "submission_verdict": "submitted",
        "submission_id": "twsub1." + "a" * 64,
    }
    result.update(mutation)
    envelope = CommandEnvelope.from_result(
        request,
        ok=True,
        status=STATUS_ACCEPTED,
        disposition=DISPOSITION_TERMINAL_ACCEPTED,
        result=result,
    )
    with pytest.raises(ValueError):
        validate_public_command_envelope(envelope, request)


def test_public_success_validator_rejects_malformed_answer() -> None:
    request = _request(_answer())
    base = {
        "target": {"worker_id": "worker-1"},
        "decision": {"decision_ref": "decision-1"},
        "delivery_state": "submitted",
        "transport_state": "submitted",
        "observed_pending_state": "pending_observation",
    }
    malformed = [
        {**base, "target": {"worker_id": "worker-2"}},
        {**base, "decision": {"decision_ref": "other"}},
        {**base, "observed_pending_state": "complete"},
        {**base, "extra": None},
    ]
    for result in malformed:
        envelope = CommandEnvelope.from_result(
            request,
            ok=True,
            status=STATUS_ACCEPTED,
            disposition=DISPOSITION_TERMINAL_ACCEPTED,
            result=result,
        )
        with pytest.raises(ValueError):
            validate_public_command_envelope(envelope, request)


@pytest.mark.parametrize(
    ("text", "valid"),
    [
        ("hello", True),
        ("white space\nallowed", True),
        ("", False),
        ("nul\x00", False),
        ("escape\x1b[A", False),
        ("control\x7f", False),
    ],
)
def test_instruction_validation_and_fingerprints(text: str, valid: bool) -> None:
    assert (validate_instruction_text(text) is None) is valid
    if valid:
        assert instruction_fingerprint(text).startswith("twins1.")


def _worker(
    worker_id: str,
    *,
    space_id: str = "",
    status: str = "idle",
    fingerprint: str = "",
    stable_key: str | None = None,
) -> Worker:
    meta = {}
    if stable_key is not None:
        meta = {"stable_key": stable_key, "stable_key_version": 1}
    return Worker(
        id=worker_id,
        name=f"name-{worker_id}",
        space_id=space_id,
        status=status,
        fingerprint=fingerprint,
        meta=meta,
    )


def test_resolve_target_supports_only_fixed_selectors_and_status_fencing() -> None:
    stable_key = "wsk1_" + "a" * 64
    open_worker = _worker(
        "worker-1",
        space_id="space-a",
        fingerprint="fp-a",
        stable_key=stable_key,
    )
    closed = _worker("worker-2", space_id="space-b", status="closed")
    workers = [open_worker, closed]

    resolved, candidates, status = resolve_target({"worker_id": "worker-1"}, workers)
    assert status == "resolved" and resolved == candidates[0]
    assert resolved["worker_id"] == "worker-1"
    assert resolve_target({"space_id": "missing"}, workers)[2] == STATUS_NOT_FOUND
    assert resolve_target(
        {"worker_id": "worker-1", "worker_fingerprint": "old"}, workers
    )[2] == STATUS_STALE_TARGET
    assert resolve_target({"worker_id": "worker-2"}, workers)[2] == STATUS_REJECTED
    assert resolve_target({"stable_key": stable_key, "stable_key_version": 1}, workers)[2] == "resolved"


def test_resolve_target_fails_closed_on_ambiguity() -> None:
    workers = [_worker("one", space_id="same"), _worker("two", space_id="same")]
    assert resolve_target({"space_id": "same"}, workers)[2] == STATUS_AMBIGUOUS_TARGET


def test_worker_candidate_and_public_ids_are_bounded() -> None:
    worker = _worker("worker-1", space_id="space-a", fingerprint="fp")
    candidate = worker_candidate(worker)
    assert candidate == {
        "worker_id": "worker-1",
        "name": "name-worker-1",
        "space_id": "space-a",
        "status": "idle",
        "worker_fingerprint": worker.fingerprint,
    }
    assert turn_submission_id("host-a", "request-1").startswith("twsub1.")
    with pytest.raises(ValueError):
        turn_submission_id("", "request-1")
