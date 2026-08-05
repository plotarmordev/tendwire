from __future__ import annotations

import io
import json
from types import SimpleNamespace
from typing import Any

import pytest

from tendwire.cli import main
from tendwire.core.commands import (
    STATUS_INVALID_REQUEST,
    CommandEnvelope,
    CommandRequest,
    error_value,
)


_APPROVED_DAEMON_SEMANTICS = pytest.mark.xfail(
    strict=True,
    reason="approved: semantic command validation belongs to command.submit",
)


def _run(monkeypatch, capsys, payload):
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(payload)))
    code = main(["command", "--json"])
    return code, json.loads(capsys.readouterr().out)


def _run_raw(monkeypatch, capsys, payload: str) -> tuple[int, dict[str, Any]]:
    monkeypatch.setattr("sys.stdin", io.StringIO(payload))
    code = main(["command", "--json"])
    return code, json.loads(capsys.readouterr().out)


@pytest.mark.parametrize("raw", ["{", "[]", "null", '"text"'])
def test_unparseable_or_nonobject_command_is_rejected_locally_without_rpc(
    monkeypatch,
    capsys,
    raw: str,
) -> None:
    monkeypatch.setattr(
        "tendwire.cli._try_daemon_attempt",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("syntax and root shape must be rejected before RPC")
        ),
    )
    code, payload = _run_raw(monkeypatch, capsys, raw)
    assert code == 1
    assert payload["ok"] is False
    assert payload["status"] == STATUS_INVALID_REQUEST


def _invalid_daemon_envelope(payload: dict[str, Any]) -> dict[str, Any]:
    request = None if "schema_version" not in payload else CommandRequest.from_dict(payload)
    return CommandEnvelope.from_error(
        request,
        error_value(STATUS_INVALID_REQUEST, "daemon rejected command semantics"),
    ).to_dict()


@_APPROVED_DAEMON_SEMANTICS
@pytest.mark.parametrize(
    "payload",
    [
        {"schema_version": 1, "action": "unknown"},
        {"action": "noop"},
        {
            "schema_version": 1,
            "action": "noop",
            "params": {"private_binding": "must-not-echo"},
        },
        {
            "schema_version": 1,
            "action": "answer_decision",
            "request_id": "invalid-decision",
            "dry_run": False,
            "target": {"worker_id": "worker"},
            "params": {"decision_ref": "decision", "selection": {}},
        },
    ],
)
def test_semantic_invalid_object_is_forwarded_once_and_daemon_identity_wins(
    monkeypatch,
    capsys,
    payload: dict[str, Any],
) -> None:
    expected = _invalid_daemon_envelope(payload)
    calls: list[tuple[str, dict[str, Any]]] = []

    def attempt(_config, method: str, params: dict[str, Any]):
        calls.append((method, params))
        return SimpleNamespace(
            result=expected,
            response_error=None,
            error_kind=None,
            request_started=True,
        )

    monkeypatch.setattr("tendwire.cli._try_daemon_attempt", attempt)
    code, result = _run(monkeypatch, capsys, payload)

    assert code == 1
    assert calls == [("command.submit", payload)]
    assert result == expected
    assert result["action"] == expected["action"]
    assert result["request_id"] == expected["request_id"]
    assert "must-not-echo" not in json.dumps(result)


@_APPROVED_DAEMON_SEMANTICS
def test_semantic_invalid_object_offline_is_backend_unavailable_not_invalid(
    monkeypatch,
    capsys,
) -> None:
    payload = {"schema_version": 1, "action": "unknown", "request_id": "opaque-id"}
    calls = 0

    def unavailable(_config, method: str, params: dict[str, Any]):
        nonlocal calls
        calls += 1
        assert method == "command.submit" and params == payload
        return SimpleNamespace(
            result=None,
            response_error=None,
            error_kind="unavailable",
            request_started=False,
        )

    monkeypatch.setattr("tendwire.cli._try_daemon_attempt", unavailable)
    code, result = _run(monkeypatch, capsys, payload)

    assert code == 1
    assert calls == 1
    assert result["status"] == "backend_unavailable"
    assert result["action"] == "unknown"
    assert result["request_id"] == "opaque-id"


def test_online_command_returns_exact_daemon_envelope_once(
    monkeypatch,
    capsys,
) -> None:
    payload = {"schema_version": 1, "action": "noop"}
    expected = CommandEnvelope.from_result(
        CommandRequest(action="noop"),
        ok=True,
        status="noop",
        result={},
    ).to_dict()
    calls = 0

    def attempt(_config, method: str, params: dict[str, Any]):
        nonlocal calls
        calls += 1
        assert method == "command.submit" and params == payload
        return SimpleNamespace(
            result=expected,
            response_error=None,
            error_kind=None,
            request_started=True,
        )

    monkeypatch.setattr("tendwire.cli._try_daemon_attempt", attempt)
    code, result = _run(monkeypatch, capsys, payload)

    assert code == 0
    assert calls == 1
    assert result == expected


def test_post_send_command_loss_is_unresolved_and_never_retried(
    monkeypatch,
    capsys,
) -> None:
    payload = {
        "schema_version": 1,
        "action": "send_instruction",
        "request_id": "ambiguous-command",
        "dry_run": False,
        "target": {"worker_id": "worker"},
        "instruction": {"text": "hello"},
    }
    calls = 0

    def ambiguous(_config, method: str, params: dict[str, Any]):
        nonlocal calls
        calls += 1
        assert method == "command.submit" and params == payload
        return SimpleNamespace(
            result=None,
            response_error=None,
            error_kind="timeout",
            request_started=True,
        )

    monkeypatch.setattr("tendwire.cli._try_daemon_attempt", ambiguous)
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(payload)))

    assert main(["command", "--json"]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "unresolved" in captured.err
    assert calls == 1


@pytest.mark.parametrize(
    "payload",
    [
        {"schema_version": 1, "action": "noop"},
        {
            "schema_version": 1,
            "action": "send_instruction",
            "dry_run": True,
            "target": {"worker_id": "worker"},
            "instruction": {"text": "hello"},
        },
    ],
)
def test_valid_command_always_uses_authoritative_daemon(
    monkeypatch, capsys, tmp_path, payload
) -> None:
    monkeypatch.setenv("TENDWIRE_DATA_DIR", str(tmp_path))
    code, response = _run(monkeypatch, capsys, payload)
    assert code == 1
    assert response["status"] == "backend_unavailable"
    assert not (tmp_path / "tendwire.db").exists()


def test_live_mutation_never_falls_back_from_daemon(monkeypatch, capsys, tmp_path) -> None:
    monkeypatch.setenv("TENDWIRE_DATA_DIR", str(tmp_path))
    code, payload = _run(
        monkeypatch,
        capsys,
        {
            "schema_version": 1,
            "action": "send_instruction",
            "request_id": "r1",
            "dry_run": False,
            "target": {"worker_id": "worker"},
            "instruction": {"text": "hello"},
        },
    )
    assert code == 1
    assert payload["status"] == "backend_unavailable"
    assert not (tmp_path / "tendwire.db").exists()
