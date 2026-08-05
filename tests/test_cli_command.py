"""CLI boundary tests for the fixed ACP-only command protocol."""

from __future__ import annotations

import io
import json
from types import SimpleNamespace
from typing import Any

import pytest

from tendwire.cli import main
from tendwire.core.commands import (
    DISPOSITION_TERMINAL_ACCEPTED,
    STATUS_ACCEPTED,
    STATUS_BACKEND_UNAVAILABLE,
    STATUS_INVALID_REQUEST,
    STATUS_REJECTED,
    CommandEnvelope,
    CommandRequest,
    error_value,
)


def _send(request_id: str = "request-1") -> dict[str, Any]:
    return {
        "schema_version": 1,
        "action": "send_instruction",
        "request_id": request_id,
        "target": {"worker_id": "worker-1"},
        "instruction": {"text": "do it"},
    }


def _answer(request_id: str = "request-1") -> dict[str, Any]:
    return {
        "schema_version": 1,
        "action": "answer_decision",
        "request_id": request_id,
        "target": {"worker_id": "worker-1"},
        "params": {
            "decision_ref": "decision-1",
            "selection": {"option_refs": ["1"]},
        },
    }


def _send_result(**changes: Any) -> dict[str, Any]:
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
    result.update(changes)
    return result


def _run(monkeypatch, capsys, value: Any) -> tuple[int, dict[str, Any] | None]:
    raw = value if isinstance(value, str) else json.dumps(value)
    monkeypatch.setattr("sys.stdin", io.StringIO(raw))
    code = main(["command", "--json"])
    output = capsys.readouterr()
    return code, json.loads(output.out) if output.out else None


def _attempt(**changes: Any) -> SimpleNamespace:
    fields = {
        "result": None,
        "response_error": None,
        "error_kind": None,
        "request_started": True,
    }
    fields.update(changes)
    return SimpleNamespace(**fields)


@pytest.mark.parametrize("raw", ["{", "[]", "null", '"text"'])
def test_nonobject_command_is_rejected_locally_without_rpc(
    monkeypatch,
    capsys,
    raw: str,
) -> None:
    monkeypatch.setattr(
        "tendwire.cli._try_daemon_attempt",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("invalid JSON roots must not reach the daemon")
        ),
    )
    code, result = _run(monkeypatch, capsys, raw)
    assert code == 1
    assert result is not None
    assert result["status"] == STATUS_INVALID_REQUEST
    assert result["schema_version"] == 2 and result["dry_run"] is False


@pytest.mark.parametrize(
    "payload",
    [
        {"schema_version": 1, "action": "noop"},
        {**_send(), "dry_run": False},
        {**_send(), "response_schema_version": 3},
        {**_send(), "params": {"ignored": True}},
    ],
)
def test_semantic_invalid_object_is_forwarded_once_and_daemon_wins(
    monkeypatch,
    capsys,
    payload: dict[str, Any],
) -> None:
    expected = CommandEnvelope.from_error(
        None,
        error_value(STATUS_INVALID_REQUEST, "daemon rejected command semantics"),
    ).to_dict()
    calls: list[tuple[str, dict[str, Any]]] = []

    def submit(_config, method, params):
        calls.append((method, params))
        return _attempt(result=expected)

    monkeypatch.setattr("tendwire.cli._try_daemon_attempt", submit)
    code, result = _run(monkeypatch, capsys, payload)
    assert code == 1
    assert calls == [("command.submit", payload)]
    assert result == expected


@pytest.mark.parametrize("ok", [True, False])
def test_online_command_returns_exact_fixed_envelope(
    monkeypatch,
    capsys,
    ok: bool,
) -> None:
    payload = _send()
    request = CommandRequest.from_dict(payload)
    expected = (
        CommandEnvelope.from_result(
            request,
            ok=True,
            status=STATUS_ACCEPTED,
            disposition=DISPOSITION_TERMINAL_ACCEPTED,
            result=_send_result(),
        )
        if ok
        else CommandEnvelope.from_error(
            request,
            error_value(STATUS_REJECTED, "rejected"),
        )
    ).to_dict()
    monkeypatch.setattr(
        "tendwire.cli._try_daemon_attempt",
        lambda *_args, **_kwargs: _attempt(result=expected),
    )
    code, result = _run(monkeypatch, capsys, payload)
    assert code == (0 if ok else 1)
    assert result == expected


def test_valid_witness_rejects_daemon_identity_substitution(
    monkeypatch,
    capsys,
) -> None:
    payload = _send("expected")
    substituted = CommandEnvelope.from_result(
        CommandRequest.from_dict(_send("other")),
        ok=True,
        status=STATUS_ACCEPTED,
        disposition=DISPOSITION_TERMINAL_ACCEPTED,
        result=_send_result(),
    ).to_dict()
    monkeypatch.setattr(
        "tendwire.cli._try_daemon_attempt",
        lambda *_args, **_kwargs: _attempt(result=substituted),
    )
    code, result = _run(monkeypatch, capsys, payload)
    assert code == 2 and result is None


@pytest.mark.parametrize(
    "payload",
    [
        {**_send("invalid-shape"), "dry_run": False},
        {**_send("semantic-invalid"), "target": {"worker_id": 7}},
    ],
)
def test_invalid_request_rejects_daemon_success_substitution(
    monkeypatch,
    capsys,
    payload: dict[str, Any],
) -> None:
    substituted = CommandEnvelope.from_result(
        CommandRequest.from_dict(_send("other")),
        ok=True,
        status=STATUS_ACCEPTED,
        disposition=DISPOSITION_TERMINAL_ACCEPTED,
        result=_send_result(),
    ).to_dict()
    monkeypatch.setattr(
        "tendwire.cli._try_daemon_attempt",
        lambda *_args, **_kwargs: _attempt(result=substituted),
    )
    code, result = _run(monkeypatch, capsys, payload)
    assert code == 2 and result is None


def test_semantic_invalid_request_rejects_same_identity_daemon_success(
    monkeypatch,
    capsys,
) -> None:
    payload = {**_send("unsafe"), "instruction": {"text": "bad\x1b[A"}}
    accepted = CommandEnvelope.from_result(
        CommandRequest.from_dict(_send("unsafe")),
        ok=True,
        status=STATUS_ACCEPTED,
        disposition=DISPOSITION_TERMINAL_ACCEPTED,
        result=_send_result(),
    ).to_dict()
    monkeypatch.setattr(
        "tendwire.cli._try_daemon_attempt",
        lambda *_args, **_kwargs: _attempt(result=accepted),
    )
    code, result = _run(monkeypatch, capsys, payload)
    assert code == 2 and result is None


@pytest.mark.parametrize(
    "payload,result",
    [
        (_send(), _send_result(submission_verdict="unknown")),
        (_send(), _send_result(submission_id="twsub1.bad")),
        (_send(), _send_result(target={"worker_id": "worker-2"})),
        (
            _answer(),
            {
                "target": {"worker_id": "worker-1"},
                "decision": {"decision_ref": "other"},
                "delivery_state": "submitted",
                "transport_state": "submitted",
                "observed_pending_state": "pending_observation",
            },
        ),
    ],
)
def test_cli_rejects_semantically_malformed_accepted_result(
    monkeypatch,
    capsys,
    payload: dict[str, Any],
    result: dict[str, Any],
) -> None:
    request = CommandRequest.from_dict(payload)
    malformed = CommandEnvelope.from_result(
        request,
        ok=True,
        status=STATUS_ACCEPTED,
        disposition=DISPOSITION_TERMINAL_ACCEPTED,
        result=result,
    ).to_dict()
    monkeypatch.setattr(
        "tendwire.cli._try_daemon_attempt",
        lambda *_args, **_kwargs: _attempt(result=malformed),
    )
    code, output = _run(monkeypatch, capsys, payload)
    assert code == 2 and output is None


@pytest.mark.parametrize(
    "mutation",
    [{"dry_run": True}, {"schema_version": 3}, {"extra": None}],
)
def test_cli_rejects_malformed_daemon_envelope(
    monkeypatch,
    capsys,
    mutation: dict[str, Any],
) -> None:
    valid = CommandEnvelope.from_error(
        CommandRequest.from_dict(_send()),
        error_value(STATUS_REJECTED, "rejected"),
    ).to_dict()
    valid.update(mutation)
    monkeypatch.setattr(
        "tendwire.cli._try_daemon_attempt",
        lambda *_args, **_kwargs: _attempt(result=valid),
    )
    code, result = _run(monkeypatch, capsys, _send())
    assert code == 2 and result is None


def test_valid_command_offline_returns_fixed_backend_failure(
    monkeypatch,
    capsys,
    tmp_path,
) -> None:
    monkeypatch.setenv("TENDWIRE_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(
        "tendwire.cli._try_daemon_attempt",
        lambda *_args, **_kwargs: _attempt(
            error_kind="unavailable", request_started=False
        ),
    )
    code, result = _run(monkeypatch, capsys, _send())
    assert code == 1 and result is not None
    assert (result["action"], result["request_id"]) == (
        "send_instruction",
        "request-1",
    )
    assert result["status"] == STATUS_BACKEND_UNAVAILABLE
    assert result["dry_run"] is False
    assert not any(tmp_path.iterdir())


def test_started_request_without_response_is_never_fabricated(
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setattr(
        "tendwire.cli._try_daemon_attempt",
        lambda *_args, **_kwargs: _attempt(
            error_kind="unavailable", request_started=True
        ),
    )
    code, result = _run(monkeypatch, capsys, _send())
    assert code == 2 and result is None
