"""Tests for the Herdr CLI backend adapter contract."""

from __future__ import annotations

import json
import os
import subprocess
from collections.abc import Sequence
from typing import Any

import pytest

from tendwire import cli as tendwire_cli
from tendwire.backends import herdr_cli, herdr_command
from tendwire.backends.herdr_cli import fetch_herdr_state
from tendwire.config import Config
from tendwire.core.commands import (
    STATUS_ACCEPTED,
    STATUS_AMBIGUOUS_BACKEND_TARGET,
    STATUS_BACKEND_FAILED,
    STATUS_BACKEND_UNAVAILABLE,
    STATUS_BACKEND_UNSUPPORTED,
    STATUS_REQUEST_STATE_UNCERTAIN,
    CommandEnvelope,
)
from tendwire.core.models import Worker, WorkerBinding, worker_binding_private_fingerprint
from tendwire.core.projector import project_from_observations
from tendwire.store.sqlite import init_store, list_worker_bindings


_FORBIDDEN_FIELDS = {
    "telegram",
    "chat_id",
    "topic_id",
    "message_id",
    "thread_id",
    "token",
    "bot_token",
    "delivery",
    "route",
    "herdres_delivery",
    "pane_id",
    "terminal_id",
    "backend_target",
    "agent_session",
    "session_id",
    "herdr_state",
    "herdres_state",
    "target_kind",
    "target_value",
    "turn_target_kind",
    "turn_target_value",
    "private_fingerprint",
    "argv",
    "command",
    "env",
    "stderr",
    "stdout",
    "secret",
    "secrets",
    "shell",
    "connector",
    "connectors",
}
_FORBIDDEN_FIELDS_COMPACT = {field.replace("_", "") for field in _FORBIDDEN_FIELDS}


def _completed(stdout: str, returncode: int = 0) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        args=["herdr", "workspace", "list", "--json"],
        returncode=returncode,
        stdout=stdout,
        stderr="",
    )


def _respond(args: Sequence[str], responses: dict[tuple[str, ...], Any]) -> subprocess.CompletedProcess[str] | None:
    """Return a canned response for a herdr command tuple."""
    key = tuple(args)
    if key not in responses:
        return _completed("", returncode=1)
    response = responses[key]
    if isinstance(response, subprocess.CompletedProcess):
        return response
    if isinstance(response, str):
        return _completed(response)
    return _completed(json.dumps(response))


def _assert_no_forbidden_fields(value: Any, path: str = "$") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            normalized = str(key).lower().replace("-", "_").replace(".", "_")
            compact = normalized.replace("_", "")
            segments = {part for part in normalized.split("_") if part}
            assert (
                normalized not in _FORBIDDEN_FIELDS and compact not in _FORBIDDEN_FIELDS_COMPACT
                and not (segments & _FORBIDDEN_FIELDS)
            ), f"forbidden field {path}.{key}"
            _assert_no_forbidden_fields(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _assert_no_forbidden_fields(item, f"{path}[{index}]")


def _send_completed(returncode: int = 0) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        args=["herdr", "agent", "send", "worker-1", "hello"],
        returncode=returncode,
        stdout="",
        stderr="",
    )


def test_send_instruction_uses_agent_send_argv(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    calls: list[tuple[list[str], dict[str, Any]]] = []
    instruction_text = "line one\nline two\tindented"

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append((args, kwargs))
        return _send_completed()

    monkeypatch.setattr(herdr_command.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_command.subprocess, "run", fake_run)

    envelope = herdr_command.send_instruction(
        config,
        {
            "worker_id": "public-worker-1",
            "backend_target": {"kind": "agent_id", "value": "agent-send-1", "sendable": True, "reason": None},
            "terminal_id": "ignored",
        },
        {"text": instruction_text},
    )

    assert envelope.ok is True
    assert envelope.status == STATUS_ACCEPTED
    assert envelope.result == {"target": {"worker_id": "public-worker-1"}}
    assert not isinstance(envelope, CommandEnvelope)
    assert set(envelope.to_dict()) == {"ok", "status", "result", "error"}
    assert calls == [
        (
            ["herdr", "agent", "send", "agent-send-1", instruction_text],
            {
                "capture_output": True,
                "text": True,
                "check": False,
                "timeout": config.herdr_timeout_seconds,
            },
        )
    ]
    assert "shell" not in calls[0][1]


def test_send_instruction_requires_private_backend_target(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    calls: list[Any] = []

    monkeypatch.setattr(herdr_command.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_command, "_run_agent_send", lambda *args: calls.append(args))

    envelope = herdr_command.send_instruction(
        config,
        {"worker_id": "public-worker-1", "agent_session": {"value": "sess-ignored"}},
        {"text": "hello"},
    )

    assert envelope.ok is False
    assert envelope.status == STATUS_BACKEND_UNSUPPORTED
    assert calls == []
    _assert_no_forbidden_fields(envelope.to_dict())


def test_send_instruction_maps_missing_binary_to_backend_unavailable(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="missing-herdr")
    monkeypatch.setattr(herdr_command.shutil, "which", lambda _: None)
    monkeypatch.setattr(
        herdr_command,
        "_run_agent_send",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should not run")),
    )

    envelope = herdr_command.send_instruction(
        config,
        {"worker_id": "worker-1", "backend_target": {"kind": "agent_id", "value": "agent-1", "sendable": True, "reason": None}},
        {"text": "hello"},
    )

    assert envelope.ok is False
    assert envelope.status == STATUS_BACKEND_UNAVAILABLE
    _assert_no_forbidden_fields(envelope.to_dict())


def test_send_instruction_maps_nonzero_exit_to_backend_failed(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    monkeypatch.setattr(herdr_command.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_command, "_run_agent_send", lambda *args: _send_completed(returncode=2))

    envelope = herdr_command.send_instruction(
        config,
        {"worker_id": "worker-1", "backend_target": {"kind": "agent_id", "value": "agent-1", "sendable": True, "reason": None}},
        {"text": "hello"},
    )

    assert envelope.ok is False
    assert envelope.status == STATUS_BACKEND_FAILED
    assert envelope.error["details"] == {"exit_code": 2}
    _assert_no_forbidden_fields(envelope.to_dict())


def test_send_instruction_maps_timeout_to_uncertain(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")

    def raise_timeout(*args: Any) -> subprocess.CompletedProcess[str]:
        raise subprocess.TimeoutExpired(cmd=["herdr", "agent", "send"], timeout=5.0)

    monkeypatch.setattr(herdr_command.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_command, "_run_agent_send", raise_timeout)

    envelope = herdr_command.send_instruction(
        config,
        {"worker_id": "worker-1", "backend_target": {"kind": "agent_id", "value": "agent-1", "sendable": True, "reason": None}},
        {"text": "hello"},
    )

    assert envelope.ok is False
    assert envelope.status == STATUS_REQUEST_STATE_UNCERTAIN
    _assert_no_forbidden_fields(envelope.to_dict())


def test_send_instruction_rejects_ambiguous_private_backend_target(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    calls: list[Any] = []

    monkeypatch.setattr(herdr_command.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_command, "_run_agent_send", lambda *args: calls.append(args))

    envelope = herdr_command.send_instruction(
        config,
        {
            "worker_id": "public-worker-1",
            "backend_target": {
                "kind": "agent_id",
                "value": "agent-1",
                "sendable": False,
                "reason": "duplicate_backend_target",
            },
        },
        {"text": "hello"},
    )

    assert envelope.ok is False
    assert envelope.status == STATUS_AMBIGUOUS_BACKEND_TARGET
    assert calls == []
    _assert_no_forbidden_fields(envelope.to_dict())


def test_send_instruction_rejects_unsupported_private_backend_target_kind(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    calls: list[Any] = []

    monkeypatch.setattr(herdr_command.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_command, "_run_agent_send", lambda *args: calls.append(args))

    envelope = herdr_command.send_instruction(
        config,
        {
            "worker_id": "public-worker-1",
            "backend_target": {
                "kind": "session_id",
                "value": "session-must-not-send",
                "sendable": True,
                "reason": None,
            },
        },
        {"text": "hello"},
    )

    assert envelope.ok is False
    assert envelope.status == STATUS_BACKEND_UNSUPPORTED
    assert calls == []
    serialized = json.dumps(envelope.to_dict())
    assert "session-must-not-send" not in serialized
    _assert_no_forbidden_fields(envelope.to_dict())


def test_fetch_herdr_state_returns_empty_when_binary_missing() -> None:
    config = Config(host_id="testhost", herdr_bin="definitely-not-a-real-herdr-binary")
    spaces, workers = fetch_herdr_state(config)
    assert spaces == []
    assert workers == []


def test_fetch_herdr_state_returns_empty_on_cli_failure(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(
        herdr_cli,
        "_run_herdr",
        lambda args, cfg: _completed('{"workers":[{"id":"leaked"}]}', returncode=2),
    )

    spaces, workers = fetch_herdr_state(config)

    assert spaces == []
    assert workers == []


def test_fetch_herdr_state_returns_empty_on_malformed_json(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", lambda args, cfg: _completed("not json"))

    spaces, workers = fetch_herdr_state(config)

    assert spaces == []
    assert workers == []


def test_sample_herdr_projection_is_neutral_and_fingerprinted(monkeypatch) -> None:
    config = Config(host_id="herdr-host", herdr_bin="herdr")
    sample_payload = {
        "spaces": [
            {
                "id": "space-1",
                "name": "Build",
                "status": "running",
                "status_line": "building package",
                "telegram": "forbidden",
                "chat_id": 111,
                "safe": "space-meta",
            }
        ],
        "workers": [
            {
                "id": "worker-1",
                "name": "Agent One",
                "status": "panic",
                "space_id": "space-1",
                "summary": "crashed",
                "topic_id": 222,
                "message_id": 333,
                "route": "telegram",
                "delivery": {"chat_id": 444},
                "safe": "worker-meta",
            }
        ],
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(
        herdr_cli,
        "_run_herdr",
        lambda args, cfg: _completed(json.dumps(sample_payload)),
    )

    spaces, workers = fetch_herdr_state(config)
    snapshot = project_from_observations(config, spaces=spaces, workers=workers)
    payload = json.loads(snapshot.to_json())

    assert payload["schema_version"] == 2
    assert len(payload["content_fingerprint"]) == 24
    assert payload["spaces"][0]["status"] == "active"
    assert payload["spaces"][0]["fingerprint"]
    assert payload["spaces"][0]["meta"]["safe"] == "space-meta"
    assert payload["workers"][0]["status"] == "failed"
    assert payload["workers"][0]["fingerprint"]
    assert payload["workers"][0]["meta"]["raw_status"] == "panic"
    assert payload["workers"][0]["meta"]["safe"] == "worker-meta"
    assert payload["attention"][0]["source"] == "worker:worker-1"
    assert payload["attention"][0]["fingerprint"]
    _assert_no_forbidden_fields(payload)


def test_herdr_cli_strip_connector_fields_drops_dot_separated_aliases() -> None:
    raw = {
        "safe": "kept",
        "backend.target": "sentinel-private-backend",
        "message.id": "sentinel-private-message",
        "bot.token": "sentinel-private-token",
        "herdres.delivery": {"message.id": "sentinel-private-delivery"},
        "delivery.route": "sentinel-private-route",
        "telegram.message.id": "sentinel-private-telegram",
        "children": [
            {
                "safe_child": "kept",
                "topic.id": "sentinel-private-topic",
            }
        ],
    }

    stripped = herdr_cli._strip_connector_fields(raw)

    assert stripped == {"safe": "kept", "children": [{"safe_child": "kept"}]}
    assert "sentinel-private" not in json.dumps(stripped, sort_keys=True)
    _assert_no_forbidden_fields(stripped)
    assert herdr_cli._safe_text_sample("failed backend.target=sentinel-private-backend") is None
    assert all(
        herdr_cli._safe_text_sample(sample) is None
        for sample in (
            "failed bot.token=sentinel-private-token",
            "failed bot_token=sentinel-private-token",
            "failed bot-token=sentinel-private-token",
            "failed botToken=sentinel-private-token",
        )
    )
    assert herdr_cli._safe_text_sample("plain diagnostic text") == "plain diagnostic text"


def test_herdr_agent_public_id_and_private_backend_target_are_separate(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list"): {"result": {"workspaces": []}},
        ("agent", "list"): {
            "result": {
                "agents": [
                    {
                        "worker_id": "public-worker",
                        "agent_id": "send-agent",
                        "agent": "Coder",
                        "agent_session": {"value": "sess-must-not-leak"},
                        "terminal_id": "term-must-not-leak",
                        "pane_id": "pane-must-not-leak",
                        "session_id": "session-must-not-leak",
                        "workspace_id": "ws-1",
                    }
                ]
            }
        },
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", lambda args, cfg: _respond(args, responses))

    spaces, workers = fetch_herdr_state(config)
    snapshot = project_from_observations(config, spaces=spaces, workers=workers)
    payload = json.loads(snapshot.to_json())

    assert len(workers) == 1
    assert workers[0].id == "public-worker"
    assert workers[0].backend_target is not None
    assert workers[0].backend_target["kind"] == "agent_id"
    assert workers[0].backend_target["value"] == "send-agent"
    assert workers[0].backend_target["sendable"] is True
    assert workers[0].backend_target["reason"] is None
    assert payload["workers"][0]["id"] == "public-worker"
    assert payload["workers"][0]["name"] == "Coder"
    assert "sess-must-not-leak" not in json.dumps(payload)
    assert "term-must-not-leak" not in json.dumps(payload)
    assert "pane-must-not-leak" not in json.dumps(payload)
    assert "session-must-not-leak" not in json.dumps(payload)
    _assert_no_forbidden_fields(payload)


def test_herdr_bindings_reuse_worker_id_by_private_fingerprint_and_update_moved_target(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    first_responses = {
        ("workspace", "list"): {"result": {"workspaces": []}},
        ("agent", "list"): {
            "result": {
                "agents": [
                    {
                        "worker_id": "public-before",
                        "agent": "Coder",
                        "agent_session": {"value": "sess-stable"},
                        "terminal_id": "term-before",
                        "pane_id": "pane-before",
                        "workspace_id": "ws-1",
                    }
                ]
            }
        },
    }
    second_responses = {
        ("workspace", "list"): {"result": {"workspaces": []}},
        ("agent", "list"): {
            "result": {
                "agents": [
                    {
                        "worker_id": "public-after",
                        "agent": "Coder",
                        "agent_session": {"value": "sess-stable"},
                        "terminal_id": "term-after",
                        "pane_id": "pane-after",
                        "workspace_id": "ws-1",
                    }
                ]
            }
        },
    }
    responses = [first_responses, second_responses]

    def fake_run(args: Sequence[str], cfg: Config) -> subprocess.CompletedProcess[str] | None:
        return _respond(args, responses[0])

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", fake_run)

    _spaces, workers, bindings = fetch_herdr_state(config, include_bindings=True)
    assert workers[0].id == "public-before"
    assert bindings[0].worker_id == "public-before"
    assert bindings[0].target_kind == "terminal_id"
    assert bindings[0].target_value == "term-before"

    responses.pop(0)
    _spaces2, workers2, bindings2 = fetch_herdr_state(
        config,
        stored_bindings=bindings,
        include_bindings=True,
    )

    assert workers2[0].id == "public-before"
    assert bindings2[0].worker_id == "public-before"
    assert bindings2[0].private_fingerprint == bindings[0].private_fingerprint
    assert bindings2[0].target_kind == "terminal_id"
    assert bindings2[0].target_value == "term-after"
    payload = json.loads(project_from_observations(config, workers=workers2).to_json())
    assert "term-after" not in json.dumps(payload)
    assert "pane-after" not in json.dumps(payload)
    _assert_no_forbidden_fields(payload)


def test_herdr_reuses_worker_id_by_unique_backend_target_fallback(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    stored = [
        WorkerBinding(
            host_id="testhost",
            worker_id="stored-public",
            worker_fingerprint="stored-fp",
            backend="herdr",
            target_kind="agent_id",
            target_value="agent-send",
            sendable=True,
            reason=None,
            observed_at="2026-01-01T00:00:00+00:00",
            expires_at="2026-01-02T00:00:00+00:00",
            private_fingerprint="old-private",
        )
    ]
    responses = {
        ("workspace", "list"): {"result": {"workspaces": []}},
        ("agent", "list"): {
            "result": {
                "agents": [
                    {
                        "worker_id": "new-public",
                        "agent_id": "agent-send",
                        "agent": "Coder",
                    }
                ]
            }
        },
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", lambda args, cfg: _respond(args, responses))

    _spaces, workers, bindings = fetch_herdr_state(
        config,
        stored_bindings=stored,
        include_bindings=True,
    )

    assert workers[0].id == "stored-public"
    assert bindings[0].worker_id == "stored-public"
    assert bindings[0].target_value == "agent-send"
    assert bindings[0].private_fingerprint != "old-private"


def test_herdr_backend_target_precedence_and_pane_fallback(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list"): {"result": {"workspaces": []}},
        ("agent", "list"): {
            "result": {
                "agents": [
                    {
                        "id": "public-id",
                        "agent_id": "agent-send",
                        "agent": "agent-name",
                        "label": "agent-label",
                        "terminal_id": "term-fallback",
                        "pane_id": "pane-fallback",
                    },
                    {
                        "slug": "pane-public",
                        "agent_session": {"value": "sess-not-sendable"},
                        "terminal_id": "term-send",
                        "pane_id": "pane-send",
                        "state_labels": ["agent"],
                    },
                ]
            }
        },
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", lambda args, cfg: _respond(args, responses))

    _, workers, bindings = fetch_herdr_state(config, include_bindings=True)
    by_id = {worker.id: worker for worker in workers}
    bindings_by_id = {binding.worker_id: binding for binding in bindings}

    assert by_id["public-id"].backend_target is not None
    assert by_id["public-id"].backend_target["kind"] == "agent_id"
    assert by_id["public-id"].backend_target["value"] == "agent-send"
    assert by_id["public-id"].backend_target["sendable"] is True
    assert by_id["pane-public"].backend_target is not None
    assert by_id["pane-public"].backend_target["kind"] == "terminal_id"
    assert by_id["pane-public"].backend_target["value"] == "term-send"
    assert by_id["pane-public"].backend_target["sendable"] is True
    assert bindings_by_id["public-id"].turn_target_kind is None
    assert bindings_by_id["public-id"].turn_target_value is None
    assert bindings_by_id["pane-public"].turn_target_kind is None
    assert bindings_by_id["pane-public"].turn_target_value is None
    assert all(
        (worker.backend_target or {}).get("value") != "sess-not-sendable"
        for worker in workers
    )


def test_duplicate_sendable_backend_targets_are_marked_not_sendable(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list"): {"result": {"workspaces": []}},
        ("agent", "list"): {
            "result": {
                "agents": [
                    {"worker_id": "public-a", "agent_id": "same-send", "agent": "A"},
                    {"worker_id": "public-b", "agent_id": "same-send", "agent": "B"},
                ]
            }
        },
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", lambda args, cfg: _respond(args, responses))

    _, workers = fetch_herdr_state(config)

    assert {worker.id for worker in workers} == {"public-a", "public-b"}
    assert all((worker.backend_target or {}).get("kind") == "agent_id" for worker in workers)
    assert all((worker.backend_target or {}).get("value") == "same-send" for worker in workers)
    assert all((worker.backend_target or {}).get("sendable") is False for worker in workers)
    assert all(
        (worker.backend_target or {}).get("reason") == "duplicate_backend_target"
        for worker in workers
    )
    assert herdr_cli.assert_unique_sendable_backend_targets(workers) is True


def test_duplicate_backend_targets_mark_bindings_unsendable(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list"): {"result": {"workspaces": []}},
        ("agent", "list"): {
            "result": {
                "agents": [
                    {"worker_id": "public-a", "agent_id": "same-send", "agent": "A"},
                    {"worker_id": "public-b", "agent_id": "same-send", "agent": "B"},
                ]
            }
        },
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", lambda args, cfg: _respond(args, responses))

    _spaces, workers, bindings = fetch_herdr_state(config, include_bindings=True)

    assert {worker.id for worker in workers} == {"public-a", "public-b"}
    assert len(bindings) == 2
    assert all(binding.target_kind == "agent_id" for binding in bindings)
    assert all(binding.target_value == "same-send" for binding in bindings)
    assert all(binding.sendable is False for binding in bindings)
    assert all(binding.reason == "duplicate_backend_target" for binding in bindings)
    assert len({binding.private_fingerprint for binding in bindings}) == 2


def test_bindings_from_workers_marks_duplicate_targets_unsendable() -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    workers = [
        Worker(
            id="public-a",
            name="A",
            status="active",
            backend_target={"kind": "agent_id", "value": "same-agent", "sendable": True, "reason": None},
        ),
        Worker(
            id="public-b",
            name="B",
            status="active",
            backend_target={"kind": "agent_id", "value": "same-agent", "sendable": True, "reason": None},
        ),
    ]

    bindings = herdr_cli.bindings_from_workers(config, workers, observed_at="2026-01-01T00:00:00+00:00")

    assert len(bindings) == 2
    assert {binding.worker_id for binding in bindings} == {"public-a", "public-b"}
    assert {binding.sendable for binding in bindings} == {False}
    assert {binding.reason for binding in bindings} == {"duplicate_backend_target"}


def test_duplicate_private_identity_bindings_stay_separate_across_reobserve(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list"): {"result": {"workspaces": []}},
        ("agent", "list"): {
            "result": {
                "agents": [
                    {
                        "worker_id": "public-a",
                        "agent_id": "same-agent",
                        "agent": "A",
                        "workspace_id": "ws-1",
                    },
                    {
                        "worker_id": "public-b",
                        "agent_id": "same-agent",
                        "agent": "B",
                        "workspace_id": "ws-1",
                    },
                ]
            }
        },
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", lambda args, cfg: _respond(args, responses))

    _spaces, workers, bindings = fetch_herdr_state(config, include_bindings=True)
    _spaces2, workers2, bindings2 = fetch_herdr_state(
        config,
        stored_bindings=bindings,
        include_bindings=True,
    )

    assert {worker.id for worker in workers} == {"public-a", "public-b"}
    assert {worker.id for worker in workers2} == {"public-a", "public-b"}
    assert len(bindings) == 2
    assert len(bindings2) == 2
    assert {binding.sendable for binding in bindings} == {False}
    assert {binding.reason for binding in bindings} == {"duplicate_backend_target"}
    assert len({binding.private_fingerprint for binding in bindings}) == 2
    assert {binding.private_fingerprint for binding in bindings2} == {
        binding.private_fingerprint for binding in bindings
    }


def test_legacy_collapsed_private_identity_does_not_rewrite_duplicate_public_ids(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    original_private = worker_binding_private_fingerprint(
        host_id="testhost",
        backend="herdr",
        identity_material={
            "agent_id": "same-agent",
            "agent_session": None,
            "session_id": None,
            "space_id": "ws-1",
        },
    )
    stored = [
        WorkerBinding(
            host_id="testhost",
            worker_id="collapsed-public",
            worker_fingerprint="collapsed-fp",
            backend="herdr",
            target_kind="agent_id",
            target_value="same-agent",
            sendable=True,
            reason=None,
            observed_at="2026-01-01T00:00:00+00:00",
            expires_at="2026-01-02T00:00:00+00:00",
            private_fingerprint=original_private,
        )
    ]
    responses = {
        ("workspace", "list"): {"result": {"workspaces": []}},
        ("agent", "list"): {
            "result": {
                "agents": [
                    {
                        "worker_id": "public-a",
                        "agent_id": "same-agent",
                        "agent": "A",
                        "workspace_id": "ws-1",
                    },
                    {
                        "worker_id": "public-b",
                        "agent_id": "same-agent",
                        "agent": "B",
                        "workspace_id": "ws-1",
                    },
                ]
            }
        },
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", lambda args, cfg: _respond(args, responses))

    _spaces, workers, bindings = fetch_herdr_state(
        config,
        stored_bindings=stored,
        include_bindings=True,
    )

    assert {worker.id for worker in workers} == {"public-a", "public-b"}
    assert "collapsed-public" not in {binding.worker_id for binding in bindings}
    assert len({binding.private_fingerprint for binding in bindings}) == 2


def test_duplicate_final_send_tokens_across_backend_kinds_are_not_sendable(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list"): {"result": {"workspaces": []}},
        ("agent", "list"): {
            "result": {
                "agents": [
                    {"worker_id": "agent-id-worker", "agent_id": "same-send", "agent": "A"},
                    {"worker_id": "name-worker", "name": "same-send"},
                ]
            }
        },
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", lambda args, cfg: _respond(args, responses))

    _, workers = fetch_herdr_state(config)
    by_id = {worker.id: worker for worker in workers}

    assert by_id["agent-id-worker"].backend_target["kind"] == "agent_id"
    assert by_id["name-worker"].backend_target["kind"] == "name"
    assert all((worker.backend_target or {}).get("value") == "same-send" for worker in workers)
    assert all((worker.backend_target or {}).get("sendable") is False for worker in workers)
    assert all(
        (worker.backend_target or {}).get("reason") == "duplicate_backend_target"
        for worker in workers
    )
    assert herdr_cli.assert_unique_sendable_backend_targets(workers) is True


def test_name_and_label_backend_fallbacks_are_sendable_only_when_unique(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list"): {"result": {"workspaces": []}},
        ("agent", "list"): {
            "result": {
                "agents": [
                    {"worker_id": "name-unique", "name": "NameUnique"},
                    {"worker_id": "label-unique", "label": "LabelUnique"},
                    {"worker_id": "name-dupe-a", "name": "DupText"},
                    {"worker_id": "label-dupe-b", "label": "DupText"},
                ]
            }
        },
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", lambda args, cfg: _respond(args, responses))

    _, workers = fetch_herdr_state(config)
    by_id = {worker.id: worker for worker in workers}

    assert by_id["name-unique"].backend_target == {
        "kind": "name",
        "value": "NameUnique",
        "sendable": True,
        "reason": None,
    }
    assert by_id["label-unique"].backend_target == {
        "kind": "label",
        "value": "LabelUnique",
        "sendable": True,
        "reason": None,
    }
    assert by_id["name-dupe-a"].backend_target["sendable"] is False
    assert by_id["name-dupe-a"].backend_target["reason"] == "duplicate_backend_target"
    assert by_id["label-dupe-b"].backend_target["sendable"] is False
    assert by_id["label-dupe-b"].backend_target["reason"] == "duplicate_backend_target"


def test_fetch_herdr_command_observation_reports_healthy_empty(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list"): {"result": {"workspaces": []}},
        ("agent", "list"): {"result": {"agents": []}},
        ("pane", "list"): {"result": {"panes": []}},
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_probe_herdr", lambda args, cfg: ("ok", _respond(args, responses).stdout and json.loads(_respond(args, responses).stdout)))

    observation = herdr_cli.fetch_herdr_command_observation(config)

    assert observation.healthy is True
    assert observation.outcome == "empty_healthy"
    assert observation.workers == []
    assert observation.backend_health[0].name == "herdr"
    assert observation.backend_health[0].status == "healthy"
    assert observation.backend_health[0].outcome == "empty_healthy"
    assert observation.backend_health[0].counts == {"spaces": 0, "workers": 0}


def test_fetch_herdr_command_observation_degrades_unmatched_agent(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list"): {
            "result": {"workspaces": [{"workspace_id": "wR9", "label": "Build"}]}
        },
        ("agent", "list"): {
            "result": {
                "agents": [
                    {"worker_id": "w-1", "agent_id": "agent-1", "agent": "Coder"}
                ]
            }
        },
        ("pane", "list"): {"result": {"panes": []}},
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(
        herdr_cli,
        "_run_herdr",
        lambda args, cfg: _respond(args, responses),
    )

    observation = herdr_cli.fetch_herdr_command_observation(config)

    assert observation.healthy is False
    assert observation.status == "degraded"
    assert observation.outcome == "continuity_unavailable"
    assert observation.workers == []
    assert observation.bindings == []


def test_fetch_herdr_snapshot_observation_reports_healthy_non_empty(monkeypatch, tmp_path) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr", data_dir=tmp_path / "state")
    responses = {
        ("workspace", "list"): {"result": {"workspaces": [{"workspace_id": "wR9", "label": "Build"}]}},
        ("agent", "list"): {
            "result": {
                "agents": [
                    {
                        "worker_id": "w-1",
                        "agent_id": "agent-1",
                        "terminal_id": "terminal-1",
                        "agent": "Coder",
                    }
                ]
            }
        },
        ("pane", "list"): {
            "result": {
                "panes": [
                    {
                        "workspace_id": "wR9",
                        "pane_id": "wR9:pA",
                        "terminal_id": "terminal-1",
                        "agent": "Coder",
                    }
                ]
            }
        },
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", lambda args, cfg: _respond(args, responses))

    observation = herdr_cli.fetch_herdr_snapshot_observation(config)
    health = observation.backend_health[0]

    assert [space.id for space in observation.spaces] == ["wR9"]
    assert [worker.id for worker in observation.workers] == ["w-1"]
    assert observation.workers[0].meta["stable_key"].startswith("wsk1_")
    assert health.to_dict() == {
        "name": "herdr",
        "status": "healthy",
        "outcome": "healthy_non_empty",
        "observed_at": health.observed_at,
        "message": "Herdr observation is healthy",
        "counts": {"spaces": 1, "workers": 1},
    }


def test_fetch_herdr_snapshot_observation_degrades_unmatched_agent(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list"): {
            "result": {"workspaces": [{"workspace_id": "wR9", "label": "Build"}]}
        },
        ("agent", "list"): {
            "result": {
                "agents": [
                    {"worker_id": "w-1", "agent_id": "agent-1", "agent": "Coder"}
                ]
            }
        },
        ("pane", "list"): {"result": {"panes": []}},
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(
        herdr_cli,
        "_run_herdr",
        lambda args, cfg: _respond(args, responses),
    )

    observation = herdr_cli.fetch_herdr_snapshot_observation(config)

    assert [space.id for space in observation.spaces] == ["wR9"]
    assert observation.workers == []
    assert observation.bindings == []
    assert observation.backend_health[0].status == "degraded"
    assert observation.backend_health[0].outcome == "continuity_unavailable"


def test_fetch_herdr_snapshot_observation_reports_healthy_empty(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list"): {"result": {"workspaces": []}},
        ("agent", "list"): {"result": {"agents": []}},
        ("pane", "list"): {"result": {"panes": []}},
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", lambda args, cfg: _respond(args, responses))

    observation = herdr_cli.fetch_herdr_snapshot_observation(config)
    health = observation.backend_health[0]

    assert observation.spaces == []
    assert observation.workers == []
    assert health.status == "healthy"
    assert health.outcome == "empty_healthy"
    assert health.counts == {"spaces": 0, "workers": 0}


def test_fetch_herdr_snapshot_observation_reports_missing_binary() -> None:
    config = Config(host_id="testhost", herdr_bin="definitely-not-a-real-herdr-binary")

    observation = herdr_cli.fetch_herdr_snapshot_observation(config)
    health = observation.backend_health[0]

    assert observation.spaces == []
    assert observation.workers == []
    assert health.status == "unavailable"
    assert health.outcome == "missing_binary"


@pytest.mark.parametrize(
    ("probe_outcome", "expected_status", "expected_outcome"),
    [
        ("launch_error", "unavailable", "launch_error"),
        ("timeout", "degraded", "timeout"),
        ("deadline_exhausted", "degraded", "deadline_exhausted"),
        ("malformed_json", "degraded", "malformed_json"),
        ("nonzero", "degraded", "nonzero"),
        ("unknown", "unknown", "unknown"),
    ],
)
def test_fetch_herdr_snapshot_observation_maps_failure_outcomes(
    monkeypatch,
    probe_outcome: str,
    expected_status: str,
    expected_outcome: str,
) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")

    def fake_probe(args: Sequence[str], cfg: Config) -> tuple[str, Any]:
        return probe_outcome, None

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_probe_herdr", fake_probe)

    observation = herdr_cli.fetch_herdr_snapshot_observation(config)
    health = observation.backend_health[0]

    assert observation.spaces == []
    assert observation.workers == []
    assert health.status == expected_status
    assert health.outcome == expected_outcome
    assert health.message


def test_herdr_health_mapping_includes_socket_disconnect() -> None:
    health = herdr_cli.herdr_backend_health("socket_disconnected")

    assert health.status == "unavailable"
    assert health.outcome == "socket_disconnected"


def test_cli_snapshot_retains_authenticated_worker_while_pane_probe_recovers(
    monkeypatch,
    tmp_path,
) -> None:
    config = Config(
        host_id="pane-recovery",
        herdr_bin="herdr",
        data_dir=tmp_path / "state",
        db_path=tmp_path / "pane-recovery.db",
    )
    init_store(config.db_path)
    responses = {
        ("workspace", "list"): {
            "result": {"workspaces": [{"workspace_id": "wR9", "label": "Build"}]}
        },
        ("agent", "list"): {
            "result": {
                "agents": [
                    {
                        "worker_id": "public-worker",
                        "agent_id": "private-agent",
                        "workspace_id": "wR9",
                        "pane_id": "wR9:pA",
                        "terminal_id": "private-terminal",
                        "agent": "Coder",
                    }
                ]
            }
        },
        ("pane", "list"): {
            "result": {
                "panes": [
                    {
                        "workspace_id": "wR9",
                        "pane_id": "wR9:pA",
                        "terminal_id": "private-terminal",
                        "agent": "Coder",
                    }
                ]
            }
        },
    }
    pane_available = True

    def fake_probe(args: Sequence[str], cfg: Config, *unused: Any) -> tuple[str, Any]:
        if tuple(args) == ("pane", "list") and not pane_available:
            return "timeout", None
        payload = responses.get(tuple(args))
        return ("ok", payload) if payload is not None else ("nonzero", None)

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_probe_herdr", fake_probe)

    first = tendwire_cli.observe_public_snapshot(config, store_snapshot=True)
    first_worker = first.workers[0]
    first_binding = list_worker_bindings(config.db_path, config.host_id, backend="herdr")
    assert first.backend_health[0].status == "healthy"
    assert first_worker.meta["stable_key"].startswith("wsk1_")

    pane_available = False
    degraded = tendwire_cli.observe_public_snapshot(config, store_snapshot=True)

    assert degraded.workers == first.workers
    assert degraded.spaces == first.spaces
    assert degraded.backend_health[0].status == "degraded"
    assert degraded.backend_health[0].outcome == "timeout"
    assert degraded.backend_health[0].counts == {"spaces": 1, "workers": 1}
    assert list_worker_bindings(config.db_path, config.host_id, backend="herdr") == first_binding

    pane_available = True
    recovered = tendwire_cli.observe_public_snapshot(config, store_snapshot=True)

    assert recovered.backend_health[0].status == "healthy"
    assert recovered.workers[0].meta["stable_key"] == first_worker.meta["stable_key"]
    recovered_binding = list_worker_bindings(
        config.db_path,
        config.host_id,
        backend="herdr",
    )

    healthy_panes = responses[("pane", "list")]
    responses[("pane", "list")] = {"result": {"panes": []}}
    unmatched = tendwire_cli.observe_public_snapshot(config, store_snapshot=True)

    assert unmatched.workers == first.workers
    assert unmatched.spaces == first.spaces
    assert unmatched.backend_health[0].status == "degraded"
    assert unmatched.backend_health[0].outcome == "continuity_unavailable"
    assert (
        list_worker_bindings(config.db_path, config.host_id, backend="herdr")
        == recovered_binding
    )

    responses[("pane", "list")] = healthy_panes
    recovered_again = tendwire_cli.observe_public_snapshot(config, store_snapshot=True)

    assert recovered_again.backend_health[0].status == "healthy"
    assert recovered_again.workers[0].meta["stable_key"] == first_worker.meta["stable_key"]


def test_cli_snapshot_retains_authenticated_worker_while_installation_key_recovers(
    monkeypatch,
    tmp_path,
) -> None:
    config = Config(
        host_id="key-recovery",
        herdr_bin="herdr",
        data_dir=tmp_path / "state",
        db_path=tmp_path / "key-recovery.db",
    )
    init_store(config.db_path)
    responses = {
        ("workspace", "list"): {
            "result": {"workspaces": [{"workspace_id": "wR9", "label": "Build"}]}
        },
        ("agent", "list"): {
            "result": {
                "agents": [
                    {
                        "worker_id": "public-worker",
                        "agent_id": "private-agent",
                        "workspace_id": "wR9",
                        "pane_id": "wR9:pA",
                        "terminal_id": "private-terminal",
                        "agent": "Coder",
                    }
                ]
            }
        },
        ("pane", "list"): {
            "result": {
                "panes": [
                    {
                        "workspace_id": "wR9",
                        "pane_id": "wR9:pA",
                        "terminal_id": "private-terminal",
                        "agent": "Coder",
                    }
                ]
            }
        },
    }

    def fake_probe(args: Sequence[str], cfg: Config, *unused: Any) -> tuple[str, Any]:
        payload = responses.get(tuple(args))
        return ("ok", payload) if payload is not None else ("nonzero", None)

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_probe_herdr", fake_probe)

    first = tendwire_cli.observe_public_snapshot(config, store_snapshot=True)
    first_worker = first.workers[0]
    first_binding = list_worker_bindings(config.db_path, config.host_id, backend="herdr")
    marker = config.installation_key_marker_path.read_bytes()
    config.installation_key_marker_path.unlink()

    degraded = tendwire_cli.observe_public_snapshot(config, store_snapshot=True)

    assert degraded.workers == first.workers
    assert degraded.backend_health[0].status == "degraded"
    assert degraded.backend_health[0].outcome == "continuity_unavailable"
    assert degraded.backend_health[0].message == "Herdr continuity identity is unavailable"
    assert degraded.backend_health[0].counts == {"spaces": 1, "workers": 1}
    assert json.loads(degraded.to_json())["backend_health"][0]["outcome"] == "continuity_unavailable"
    assert list_worker_bindings(config.db_path, config.host_id, backend="herdr") == first_binding

    degraded_again = tendwire_cli.observe_public_snapshot(config, store_snapshot=True)
    assert degraded_again.workers == first.workers
    assert degraded_again.spaces == first.spaces
    assert degraded_again.backend_health[0].outcome == "continuity_unavailable"
    assert degraded_again.backend_health[0].counts == {"spaces": 1, "workers": 1}

    def timeout_pane_probe(
        args: Sequence[str],
        cfg: Config,
        *unused: Any,
    ) -> tuple[str, Any]:
        if tuple(args) == ("pane", "list"):
            return "timeout", None
        return fake_probe(args, cfg, *unused)

    monkeypatch.setattr(herdr_cli, "_probe_herdr", timeout_pane_probe)
    alternate_failure = tendwire_cli.observe_public_snapshot(config, store_snapshot=True)
    assert alternate_failure.workers == first.workers
    assert alternate_failure.backend_health[0].outcome == "continuity_unavailable"
    monkeypatch.setattr(herdr_cli, "_probe_herdr", fake_probe)

    config.installation_key_marker_path.write_bytes(marker)
    os.chmod(config.installation_key_marker_path, 0o600)
    recovered = tendwire_cli.observe_public_snapshot(config, store_snapshot=True)

    assert recovered.backend_health[0].status == "healthy"
    assert recovered.workers[0].meta["stable_key"] == first_worker.meta["stable_key"]


def test_probe_payload_variants_stops_after_timeout(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    calls: list[tuple[str, ...]] = []

    def fake_probe(args: Sequence[str], cfg: Config) -> tuple[str, Any]:
        calls.append(tuple(args))
        return "timeout", None

    monkeypatch.setattr(herdr_cli, "_probe_herdr", fake_probe)

    outcome, payload = herdr_cli._probe_payload_variants(
        [["agent", "list"], ["agent", "list", "--json"]],
        config,
    )

    assert outcome == "timeout"
    assert payload is None
    assert calls == [("agent", "list")]


def test_fetch_herdr_command_observation_stops_fallbacks_after_timeout(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    calls: list[tuple[str, ...]] = []

    def fake_probe(args: Sequence[str], cfg: Config) -> tuple[str, Any]:
        calls.append(tuple(args))
        if tuple(args) == ("workspace", "list"):
            return "ok", {"result": {"workspaces": []}}
        return "timeout", None

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_probe_herdr", fake_probe)

    observation = herdr_cli.fetch_herdr_command_observation(config)

    assert observation.healthy is False
    assert observation.outcome == "timeout"
    assert observation.workers == []
    assert calls == [("workspace", "list"), ("agent", "list")]


def test_fetch_herdr_state_short_circuits_after_timeout(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    calls: list[tuple[str, ...]] = []

    def fake_run(args: Sequence[str], cfg: Config) -> subprocess.CompletedProcess[str]:
        calls.append(tuple(args))
        raise subprocess.TimeoutExpired(cmd=["herdr", *args], timeout=config.herdr_timeout_seconds)

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", fake_run)

    spaces, workers = fetch_herdr_state(config)

    assert spaces == []
    assert workers == []
    assert calls == [("workspace", "list")]


def test_fetch_herdr_state_returns_spaces_and_skips_worker_fallback_after_agent_timeout(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    calls: list[tuple[str, ...]] = []

    responses = {
        ("workspace", "list"): {"result": {"workspaces": [{"workspace_id": "ws-1", "label": "Build"}]}},
    }

    def fake_run(args: Sequence[str], cfg: Config) -> subprocess.CompletedProcess[str]:
        calls.append(tuple(args))
        if tuple(args) == ("workspace", "list"):
            return _respond(args, responses)
        raise subprocess.TimeoutExpired(cmd=["herdr", *args], timeout=config.herdr_timeout_seconds)

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", fake_run)

    spaces, workers = fetch_herdr_state(config)

    assert [space.id for space in spaces] == ["ws-1"]
    assert workers == []
    assert calls == [("workspace", "list"), ("agent", "list")]


def test_fetch_herdr_state_uses_aggregate_deadline_for_remaining_probe_timeout(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr", herdr_timeout_seconds=1.0)
    current_time = [0.0]
    calls: list[tuple[tuple[str, ...], float]] = []

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append((tuple(args[1:]), float(kwargs["timeout"])))
        if len(calls) == 1:
            current_time[0] += 4.75
        else:
            current_time[0] += 0.30
        return subprocess.CompletedProcess(args=args, returncode=2, stdout="", stderr="")

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli.time, "monotonic", lambda: current_time[0])
    monkeypatch.setattr(herdr_cli.subprocess, "run", fake_run)

    spaces, workers = fetch_herdr_state(config)

    assert spaces == []
    assert workers == []
    assert calls[0] == (("workspace", "list"), 1.0)
    assert calls[1][0] == ("workspace", "list", "--json")
    assert 0 < calls[1][1] <= 0.25
    assert len(calls) == 2


def test_fetch_herdr_command_observation_stops_after_aggregate_deadline(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr", herdr_timeout_seconds=1.0)
    current_time = [0.0]
    calls: list[tuple[str, ...]] = []

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append(tuple(args[1:]))
        current_time[0] += 5.1
        return subprocess.CompletedProcess(
            args=args,
            returncode=0,
            stdout=json.dumps({"result": {"workspaces": []}}),
            stderr="",
        )

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli.time, "monotonic", lambda: current_time[0])
    monkeypatch.setattr(herdr_cli.subprocess, "run", fake_run)

    observation = herdr_cli.fetch_herdr_command_observation(config)

    assert observation.healthy is False
    assert observation.status == "degraded"
    assert observation.outcome == "deadline_exhausted"
    assert calls == [("workspace", "list")]


@pytest.mark.parametrize("outcome", ["timeout", "malformed_json", "nonzero"])
def test_fetch_herdr_command_observation_degraded_agent_probe(monkeypatch, outcome: str) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")

    def fake_probe(args: Sequence[str], cfg: Config) -> tuple[str, Any]:
        if tuple(args) == ("workspace", "list"):
            return "ok", {"result": {"workspaces": []}}
        if tuple(args) == ("workspace", "list", "--json"):
            return "ok", {"result": {"workspaces": []}}
        return outcome, None

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_probe_herdr", fake_probe)

    observation = herdr_cli.fetch_herdr_command_observation(config)

    assert observation.healthy is False
    assert observation.status == "degraded"
    assert observation.outcome == outcome
    assert observation.workers == []


def test_no_flag_workspace_and_agent_lists_preferred_without_json_calls(monkeypatch) -> None:
    """Herdr 0.7.0 no-flag envelopes are used before compatibility --json fallbacks."""
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list"): {
            "result": {
                "workspaces": [
                    {
                        "workspace_id": "ws-1",
                        "label": "Build",
                        "agent_status": "working",
                        "active_tab_id": "tab-1",
                    }
                ]
            }
        },
        ("agent", "list"): {
            "result": {
                "agents": [
                    {
                        "agent_session": {"value": "sess-1"},
                        "agent": "Coder",
                        "workspace_id": "ws-1",
                        "agent_status": "done",
                        "cwd": "/home/dev",
                    }
                ]
            }
        },
        ("pane", "list"): {"result": {"panes": []}},
    }
    calls: list[tuple[str, ...]] = []

    def fake_run(args: Sequence[str], cfg: Config) -> subprocess.CompletedProcess[str] | None:
        calls.append(tuple(args))
        if "--json" in args:
            raise AssertionError("--json fallback should not be called after valid no-flag output")
        return _respond(args, responses)

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", fake_run)

    spaces, workers = fetch_herdr_state(config)

    assert calls == [("workspace", "list"), ("agent", "list"), ("pane", "list")]
    assert len(spaces) == 1
    assert spaces[0].id == "ws-1"
    assert spaces[0].name == "Build"
    assert spaces[0].status == "active"
    assert spaces[0].meta.get("active_tab_id") == "tab-1"
    assert spaces[0].meta.get("raw_status") == "working"
    assert len(workers) == 1
    assert workers[0].id == "Coder"
    assert workers[0].name == "Coder"
    assert workers[0].status == "done"
    assert workers[0].space_id == "ws-1"
    assert workers[0].backend_target is not None
    assert workers[0].backend_target["kind"] == "agent"
    assert workers[0].backend_target["value"] == "Coder"
    assert workers[0].backend_target["sendable"] is True
    assert "cwd" not in workers[0].meta
    assert "/home/dev" not in json.dumps(workers[0].to_dict())
    assert "raw_status" not in workers[0].meta


def test_json_workspace_list_fallback_when_no_flag_fails(monkeypatch) -> None:
    """Compatibility --json workspace list is tried when no-flag output fails."""
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list"): _completed("usage", returncode=2),
        ("workspace", "list", "--json"): {
            "result": {
                "workspaces": [{"workspace_id": "ws-json", "label": "Compat", "agent_status": "idle"}]
            }
        },
        ("agent", "list"): {"result": {"agents": []}},
        ("pane", "list"): {"result": {"panes": []}},
    }
    calls: list[tuple[str, ...]] = []

    def fake_run(args: Sequence[str], cfg: Config) -> subprocess.CompletedProcess[str] | None:
        calls.append(tuple(args))
        return _respond(args, responses)

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", fake_run)

    spaces, workers = fetch_herdr_state(config)

    assert calls[:2] == [("workspace", "list"), ("workspace", "list", "--json")]
    assert len(spaces) == 1
    assert spaces[0].id == "ws-json"
    assert spaces[0].name == "Compat"
    assert workers == []


def test_json_agent_list_fallback_when_no_flag_is_malformed(monkeypatch) -> None:
    """Compatibility --json agent list is tried when no-flag output is malformed."""
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list"): {"result": {"workspaces": []}},
        ("agent", "list"): _completed("not json"),
        ("agent", "list", "--json"): {
            "result": {
                "agents": [
                    {
                        "agent_session": {"value": "sess-json"},
                        "agent": "CompatAgent",
                        "workspace_id": "ws-1",
                    }
                ]
            }
        },
        ("pane", "list"): {"result": {"panes": []}},
    }
    calls: list[tuple[str, ...]] = []

    def fake_run(args: Sequence[str], cfg: Config) -> subprocess.CompletedProcess[str] | None:
        calls.append(tuple(args))
        return _respond(args, responses)

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", fake_run)

    spaces, workers = fetch_herdr_state(config)

    assert calls == [
        ("workspace", "list"),
        ("agent", "list"),
        ("agent", "list", "--json"),
        ("pane", "list"),
    ]
    assert spaces == []
    assert len(workers) == 1
    assert workers[0].id == "CompatAgent"
    assert workers[0].name == "CompatAgent"
    assert workers[0].space_id == "ws-1"


def test_result_envelopes_parse(monkeypatch) -> None:
    """result.workspaces, result.agents, and result.panes envelopes parse."""
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list", "--json"): {
            "result": {
                "workspaces": [{"workspace_id": "ws-result", "label": "ResultSpace", "agent_status": "idle"}]
            }
        },
        ("agent", "list", "--json"): {
            "result": {"agents": [{"agent_session": {"value": "sess-result"}, "agent": "Agent"}]}
        },
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", lambda args, cfg: _respond(args, responses))

    spaces, workers = fetch_herdr_state(config)

    assert len(spaces) == 1
    assert spaces[0].id == "ws-result"
    assert len(workers) == 1
    assert workers[0].id == "Agent"


def test_pane_fallback_only_when_agent_list_yields_none(monkeypatch) -> None:
    """Pane list fallback runs only when agents are empty and only for agent-bearing panes."""
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list", "--json"): _completed("", returncode=1),
        ("workspace", "list"): _completed("", returncode=1),
        ("agent", "list", "--json"): _completed("", returncode=1),
        ("agent", "list"): {"result": {"agents": []}},
        ("pane", "list"): {
            "result": {
                "panes": [
                    {"pane_id": "pane-agent", "agent": "Runner", "workspace_id": "ws-1"},
                    {"pane_id": "pane-plain", "workspace_id": "ws-1"},
                ]
            }
        },
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", lambda args, cfg: _respond(args, responses))

    spaces, workers = fetch_herdr_state(config)

    assert len(workers) == 1
    assert workers[0].id == "Runner"
    assert workers[0].name == "Runner"
    assert workers[0].backend_target is not None
    assert workers[0].backend_target["kind"] == "pane_id"
    assert workers[0].backend_target["value"] == "pane-agent"
    assert workers[0].backend_target["sendable"] is True


def test_pane_list_enriches_without_adding_unmatched_panes_when_agents_present(monkeypatch) -> None:
    """Pane list enriches matching agents without projecting unmatched fallback panes."""
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list", "--json"): _completed("", returncode=1),
        ("workspace", "list"): _completed("", returncode=1),
        ("agent", "list", "--json"): {
            "result": {"agents": [{"agent_session": {"value": "sess-1"}, "agent": "Agent"}]}
        },
        ("pane", "list"): {"result": {"panes": [{"pane_id": "sess-1", "agent": "Agent"}]}},
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", lambda args, cfg: _respond(args, responses))

    spaces, workers = fetch_herdr_state(config)

    assert len(workers) == 1
    assert workers[0].id == "Agent"


def test_agent_and_pane_duplicates_emit_one_worker(monkeypatch) -> None:
    """A worker described by both agent and pane payloads is deduplicated."""
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list", "--json"): _completed("", returncode=1),
        ("workspace", "list"): _completed("", returncode=1),
        ("agent", "list", "--json"): _completed("", returncode=1),
        ("agent", "list"): {"result": {"agents": []}},
        ("pane", "list"): {
            "result": {
                "panes": [
                    {"pane_id": "pane-1", "agent": "Runner", "workspace_id": "ws-1"},
                    {"pane_id": "pane-1", "agent": "Runner", "workspace_id": "ws-1"},
                ]
            }
        },
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", lambda args, cfg: _respond(args, responses))

    spaces, workers = fetch_herdr_state(config)

    assert len(workers) == 1
    assert workers[0].id == "Runner"


def test_repeated_agent_names_with_distinct_ids_remain_distinct(monkeypatch) -> None:
    """Workers with the same display name but different session ids are kept."""
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list", "--json"): _completed("", returncode=1),
        ("workspace", "list"): _completed("", returncode=1),
        ("agent", "list", "--json"): {
            "result": {
                "agents": [
                    {"agent_session": {"value": "sess-a"}, "agent": "Coder", "workspace_id": "ws-1"},
                    {"agent_session": {"value": "sess-b"}, "agent": "Coder", "workspace_id": "ws-1"},
                ]
            }
        },
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", lambda args, cfg: _respond(args, responses))

    spaces, workers = fetch_herdr_state(config)

    assert len(workers) == 2
    assert {w.id for w in workers} == {"Coder-1", "Coder-2"}
    assert workers[0].id < workers[1].id
    assert all((w.backend_target or {}).get("kind") == "agent" for w in workers)
    assert all((w.backend_target or {}).get("value") == "Coder" for w in workers)
    assert all((w.backend_target or {}).get("sendable") is False for w in workers)
    assert all((w.backend_target or {}).get("reason") == "duplicate_backend_target" for w in workers)


def test_status_aliases_for_live_herdr(monkeypatch) -> None:
    """Working maps to active, done maps to done, and raw_status is preserved."""
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list", "--json"): {
            "result": {
                "workspaces": [
                    {"workspace_id": "ws-1", "label": "Space", "agent_status": "working"},
                    {"workspace_id": "ws-2", "label": "Space2", "agent_status": "responding"},
                ]
            }
        },
        ("agent", "list", "--json"): {
            "result": {
                "agents": [
                    {"agent_session": {"value": "sess-1"}, "agent": "Agent", "workspace_id": "ws-1", "agent_status": "done"},
                    {"agent_session": {"value": "sess-2"}, "agent": "Agent", "workspace_id": "ws-2", "agent_status": "awaiting-input"},
                ]
            }
        },
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", lambda args, cfg: _respond(args, responses))

    spaces, workers = fetch_herdr_state(config)

    assert spaces[0].status == "active"
    assert spaces[0].meta.get("raw_status") == "working"
    assert spaces[1].status == "waiting"
    assert spaces[1].meta.get("raw_status") == "responding"
    by_space = {w.space_id: w for w in workers}
    assert by_space["ws-1"].status == "done"
    assert "raw_status" not in by_space["ws-1"].meta
    assert by_space["ws-2"].status == "waiting"
    assert by_space["ws-2"].meta.get("raw_status") == "awaiting-input"


def test_forbidden_connector_fields_stripped_in_herdr_070(monkeypatch) -> None:
    """Connector/delivery fields are stripped from live Herdr 0.7.0 payloads."""
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list", "--json"): {
            "result": {
                "workspaces": [
                    {
                        "workspace_id": "ws-1",
                        "label": "Space",
                        "agent_status": "idle",
                        "telegram": "leaked",
                        "chat_id": 123,
                        "bot.token": "leaked",
                        "message.id": "leaked",
                        "backend.target": "leaked",
                    }
                ]
            }
        },
        ("agent", "list", "--json"): {
            "result": {
                "agents": [
                    {
                        "agent_session": {"value": "sess-1"},
                        "agent": "Agent",
                        "workspace_id": "ws-1",
                        "agent_status": "active",
                        "route": "telegram",
                        "delivery": {"topic_id": 456},
                        "herdres_delivery": {"message_id": 789},
                        "herdres.delivery": {"message.id": 789},
                        "delivery.route": "telegram",
                        "telegram.message.id": "leaked",
                    }
                ]
            }
        },
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", lambda args, cfg: _respond(args, responses))

    spaces, workers = fetch_herdr_state(config)
    snapshot = project_from_observations(config, spaces=spaces, workers=workers)
    payload = json.loads(snapshot.to_json())

    _assert_no_forbidden_fields(payload)
    assert "telegram" not in payload["spaces"][0]["meta"]
    assert "route" not in payload["workers"][0]["meta"]
