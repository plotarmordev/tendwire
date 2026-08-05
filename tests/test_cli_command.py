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


def _run(monkeypatch, capsys, value: Any) -> tuple[int, dict[str, Any]]:
    raw = value if isinstance(value, str) else json.dumps(value)
    monkeypatch.setattr("sys.stdin", io.StringIO(raw))
    code = main(["command", "--json"])
    return code, json.loads(capsys.readouterr().out)


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
def test_nonobject_command_is_rejected_locally_without_rpc(monkeypatch, capsys, raw: str) -> None:
    def forbidden(*_args, **_kwargs):
        raise AssertionError("invalid JSON roots must not reach the daemon")
    monkeypatch.setattr("tendwire.cli._try_daemon_attempt", forbidden)
    code, result = _run(monkeypatch, capsys, raw)
    assert code == 1
    assert result["ok"] is False
    assert result["status"] == STATUS_INVALID_REQUEST


def _invalid_daemon_envelope(payload: dict[str, Any]) -> dict[str, Any]:
    try:
        request = CommandRequest.from_dict(payload)
    except (TypeError, ValueError):
        request = None
    return CommandEnvelope.from_error(
        request,
        error_value(STATUS_INVALID_REQUEST, "daemon rejected command semantics"),
    ).to_dict()


@pytest.mark.parametrize(
    "payload",
    [
        {"schema_version": 1, "action": "unknown"},
        {"action": "noop"},
        {"schema_version": 1, "action": "noop", "params": {"secret": "hidden"}},
        {
            "schema_version": 1,
            "action": "answer_decision",
            "request_id": "bad",
            "dry_run": False,
            "target": {"worker_id": "worker"},
            "params": {"decision_ref": "decision", "selection": {}},
        },
    ],
)
def test_semantic_invalid_object_is_forwarded_once_and_daemon_wins(
    monkeypatch, capsys, payload: dict[str, Any]
) -> None:
    expected = _invalid_daemon_envelope(payload)
    calls: list[tuple[str, dict[str, Any]]] = []
    def submit(_config, method, params):
        calls.append((method, params))
        return _attempt(result=expected)
    monkeypatch.setattr("tendwire.cli._try_daemon_attempt", submit)
    code, result = _run(monkeypatch, capsys, payload)
    assert code == 1
    assert calls == [("command.submit", payload)]
    assert result == expected
    assert "hidden" not in json.dumps(result)


@pytest.mark.parametrize(
    ("payload", "identity"),
    [
        (
            {
                "action": {"nested": "secret"},
                "request_id": ["secret"],
                "dry_run": 1,
                "target": {"secret": "hidden"},
            },
            ("", None, True),
        ),
        (
            {"action": "unknown", "request_id": "opaque", "dry_run": False},
            ("unknown", "opaque", False),
        ),
        (
            {"action": "send_instruction", "request_id": [], "dry_run": False},
            ("send_instruction", None, True),
        ),
        (
            {"action": "answer_decision", "dry_run": False},
            ("answer_decision", None, True),
        ),
    ],
)
def test_semantic_invalid_offline_uses_only_safe_scalar_identity(
    monkeypatch, capsys, payload: dict[str, Any], identity: tuple[Any, ...]
) -> None:
    calls = []
    def submit(_config, method, params):
        calls.append((method, params))
        return _attempt(error_kind="unavailable", request_started=False)
    monkeypatch.setattr("tendwire.cli._try_daemon_attempt", submit)
    code, result = _run(monkeypatch, capsys, payload)
    assert code == 1
    assert calls == [("command.submit", payload)]
    assert result["status"] == "backend_unavailable"
    assert (result["action"], result["request_id"], result["dry_run"]) == identity
    assert "secret" not in json.dumps(result)


@pytest.mark.parametrize("ok", [True, False])
def test_online_command_returns_exact_daemon_envelope(monkeypatch, capsys, ok: bool) -> None:
    payload = {"schema_version": 1, "action": "noop"}
    request = CommandRequest(action="noop")
    expected = (
        CommandEnvelope.from_result(request, ok=True, status="noop", result={})
        if ok
        else CommandEnvelope.from_error(
            request, error_value(STATUS_INVALID_REQUEST, "rejected")
        )
    ).to_dict()
    calls = []
    def submit(_config, method, params):
        calls.append((method, params))
        return _attempt(result=expected)
    monkeypatch.setattr("tendwire.cli._try_daemon_attempt", submit)
    code, result = _run(monkeypatch, capsys, payload)
    assert code == (0 if ok else 1)
    assert calls == [("command.submit", payload)]
    assert result == expected


def test_valid_witness_rejects_daemon_identity_substitution(monkeypatch, capsys) -> None:
    payload = {"schema_version": 1, "action": "noop", "request_id": "expected"}
    substituted = CommandEnvelope.from_result(
        CommandRequest(action="noop", request_id="other"),
        ok=True,
        status="noop",
        result={},
    ).to_dict()
    monkeypatch.setattr(
        "tendwire.cli._try_daemon_attempt", lambda *_args: _attempt(result=substituted)
    )
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(payload)))
    assert main(["command", "--json"]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "unresolved" in captured.err


@pytest.mark.parametrize(
    "attempt",
    [
        _attempt(error_kind="timeout", request_started=True),
        _attempt(error_kind="protocol", request_started=None),
        _attempt(response_error={"ok": False}, error_kind="daemon_error"),
        _attempt(result={"not": "an envelope"}),
    ],
)
def test_ambiguous_command_is_unresolved_and_never_retried(
    monkeypatch, capsys, attempt: SimpleNamespace
) -> None:
    payload = {"schema_version": 1, "action": "noop"}
    calls = []
    def submit(_config, method, params):
        calls.append((method, params))
        return attempt
    monkeypatch.setattr("tendwire.cli._try_daemon_attempt", submit)
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(payload)))
    assert main(["command", "--json"]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "unresolved" in captured.err
    assert calls == [("command.submit", payload)]


def test_valid_command_offline_has_no_local_store(monkeypatch, capsys, tmp_path) -> None:
    monkeypatch.setenv("TENDWIRE_DATA_DIR", str(tmp_path))
    code, result = _run(monkeypatch, capsys, {"schema_version": 1, "action": "noop"})
    assert code == 1
    assert result["status"] == "backend_unavailable"
    assert not (tmp_path / "tendwire.db").exists()
