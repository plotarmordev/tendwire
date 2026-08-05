"""Contract tests for neutral model serialization and fingerprints."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from tendwire.config import Config
from tendwire.core.models import (
    AttentionSignal,
    BackendHealth,
    Snapshot,
    Space,
    SuggestedAction,
    Worker,
    WorkerBinding,
    normalize_status,
    separate_duplicate_worker_bindings,
    sanitize_forbidden_fields,
    utc_timestamp,
    worker_binding_private_fingerprint,
)
from tendwire.core.projector import project_empty, project_from_raw


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
    normalized = str(key).lower().replace("-", "_").replace(".", "_")
    return normalized in _FORBIDDEN_FIELDS or normalized.replace("_", "") in _FORBIDDEN_FIELD_COMPACT


def _assert_no_forbidden_fields(value: Any, path: str = "$") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            assert not _is_forbidden_test_key(key), f"forbidden field {path}.{key}"
            _assert_no_forbidden_fields(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _assert_no_forbidden_fields(item, f"{path}[{index}]")


def test_sanitize_forbidden_fields_strips_pr5_nested_and_variant_keys() -> None:
    raw = {
        "schema_version": 1,
        "host_id": "host-public",
        "worker_id": "worker-public",
        "space_id": "space-public",
        "worker_fingerprint": "worker-fp",
        "id": "public-id",
        "fingerprint": "public-fp",
        "content_fingerprint": "content-fp",
        "source": "snapshot",
        "origin_command_id": "cmd-public",
        "backend_health": [{"name": "herdr", "status": "healthy"}],
        "safe": "kept",
        "commandLine": "sentinel-command-line",
        "command-text": "sentinel-command-text",
        "tty": "sentinel-tty",
        "pty": "sentinel-pty",
        "pid": "sentinel-pid",
        "process_id": "sentinel-process",
        "tmux": "sentinel-tmux",
        "screen_session": "sentinel-screen",
        "window_id": "sentinel-window",
        "tab_id": "sentinel-tab",
        "pane_id": "sentinel-pane",
        "terminal_id": "sentinel-terminal",
        "backend_target": "sentinel-backend",
        "backend.target": "sentinel-dot-backend",
        "session_id": "sentinel-session",
        "bot.token": "sentinel-dot-token",
        "messageIds": "sentinel-message-ids",
        "terminalIds": "sentinel-terminal-ids",
        "terminal": "sentinel-terminal-object",
        "telegramMessageId": "sentinel-telegram-message",
        "routeId": "sentinel-route-id",
        "connectorId": "sentinel-connector-id",
        "tmuxPaneId": "sentinel-tmux-pane-id",
        "screenWindowId": "sentinel-screen-window-id",
        "agentSessionId": "sentinel-agent-session-id",
        "session": "sentinel-session-object",
        "privateFingerprints": "sentinel-private-fingerprints",
        "passwords": "sentinel-passwords",
        "nested": [
            {
                "safe_nested": "kept",
                "processId": "sentinel-camel-process",
                "tmux-session": "sentinel-kebab-tmux",
                "terminalid": "sentinel-compact-terminal",
                "terminal.id": "sentinel-dot-terminal",
                "backendTarget": "sentinel-camel-backend",
                "screenSession": "sentinel-camel-screen",
                "privateBinding": "sentinel-private-binding",
                "private.binding": "sentinel-dot-private-binding",
                "telegramChatId": "sentinel-chat",
                "authToken": "sentinel-auth",
                "cookies": "sentinel-cookie",
                "shellCommand": "sentinel-shell-command",
                "rawArgs": "sentinel-raw-args",
            }
        ],
        "tuple": (
            {
                "pane-id": "sentinel-kebab-pane",
                "pane.id": "sentinel-dot-pane",
                "tabId": "sentinel-camel-tab",
                "raw-command-line": "sentinel-raw-command-line",
                "raw.command.line": "sentinel-dot-raw-command-line",
                "safe_tuple": "kept",
            },
        ),
    }

    sanitized = sanitize_forbidden_fields(raw)
    encoded = json.dumps(sanitized, sort_keys=True)

    assert sanitized["host_id"] == "host-public"
    assert sanitized["worker_id"] == "worker-public"
    assert sanitized["space_id"] == "space-public"
    assert sanitized["worker_fingerprint"] == "worker-fp"
    assert sanitized["id"] == "public-id"
    assert sanitized["fingerprint"] == "public-fp"
    assert sanitized["content_fingerprint"] == "content-fp"
    assert sanitized["source"] == "snapshot"
    assert sanitized["origin_command_id"] == "cmd-public"
    assert sanitized["backend_health"] == [{"name": "herdr", "status": "healthy"}]
    assert sanitized["safe"] == "kept"
    assert sanitized["nested"] == [{"safe_nested": "kept"}]
    assert sanitized["tuple"] == [{"safe_tuple": "kept"}]
    assert "sentinel-" not in encoded
    _assert_no_forbidden_fields(sanitized)


def test_suggested_action_from_dict_does_not_promote_forbidden_command_alias() -> None:
    raw_actions = [
        {
            "label": "Run raw command",
            "command": "tendwire snapshot --json --token sentinel-command-token",
            "terminal_id": "sentinel-terminal",
            "backend_target": "sentinel-backend",
            "session_id": "sentinel-session",
            "params": {
                "safe": "kept",
                "commandLine": "sentinel-command-line",
                "token": "sentinel-token",
                "secret": "sentinel-secret",
            },
        },
        {
            "label": "Run safe-looking alias",
            "command": "sentinel-safe-looking-command-alias",
            "terminalId": "sentinel-camel-terminal",
            "backendTarget": "sentinel-camel-backend",
            "session-id": "sentinel-kebab-session",
            "commandLine": "sentinel-top-level-command-line",
            "params": {
                "safe": "kept",
                "nested": {
                    "safe_nested": "kept",
                    "terminal_id": "sentinel-nested-terminal",
                    "commandLine": "sentinel-nested-command-line",
                },
            },
        },
    ]

    for raw_action in raw_actions:
        action = SuggestedAction.from_dict(raw_action)
        payload = action.to_dict()
        encoded = json.dumps(payload, sort_keys=True)

        assert action.tendwire_action == ""
        expected_label = "Action" if raw_action["label"] == "Run raw command" else raw_action["label"]
        assert payload["label"] == expected_label
        assert "tendwire_action" not in payload
        assert "safe" in payload["params"]
        assert "sentinel-" not in encoded
        _assert_no_forbidden_fields(payload)


def test_suggested_action_from_dict_prefers_safe_explicit_tendwire_action() -> None:
    action = SuggestedAction.from_dict(
        {
            "label": "Approve",
            "command": "sentinel-forbidden-command-alias",
            "tendwire_action": "approve",
            "terminal_id": "sentinel-terminal",
            "backend_target": "sentinel-backend",
            "session_id": "sentinel-session",
            "params": {"safe": "kept", "commandLine": "sentinel-command-line"},
        }
    )

    payload = action.to_dict()
    encoded = json.dumps(payload, sort_keys=True)

    assert action.tendwire_action == "approve"
    assert payload["tendwire_action"] == "approve"
    assert payload["params"] == {"safe": "kept"}
    assert "sentinel-" not in encoded
    _assert_no_forbidden_fields(payload)


def test_suggested_action_omits_raw_explicit_tendwire_action_publicly() -> None:
    action = SuggestedAction(
        label="Run diagnostic",
        tendwire_action="tendwire snapshot --json --token sentinel-action-token",
        params={"safe": "kept"},
    )

    payload = action.to_dict()

    assert "tendwire_action" not in payload
    assert "sentinel-" not in json.dumps(payload, sort_keys=True)
    _assert_no_forbidden_fields(payload)


def _snapshot_payload(snapshot: Snapshot) -> dict[str, Any]:
    return snapshot.to_dict()


def _stable_item_key(item: dict[str, Any]) -> str:
    stable_id = item.get("id") or item.get("fingerprint")
    if stable_id is not None:
        return str(stable_id)
    return json.dumps(item, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _strip_volatile_fingerprint_fields(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _strip_volatile_fingerprint_fields(item)
            for key, item in value.items()
            if key not in {"updated_at", "observed_at", "content_fingerprint"}
        }
    if isinstance(value, list):
        return [_strip_volatile_fingerprint_fields(item) for item in value]
    return value



def _expected_snapshot_fingerprint(payload: dict[str, Any]) -> str:
    content = _strip_volatile_fingerprint_fields(
        json.loads(json.dumps(payload, ensure_ascii=False))
    )
    for collection in ("spaces", "workers", "attention", "backend_health"):
        content[collection] = sorted(content.get(collection, []), key=_stable_item_key)
    encoded = json.dumps(
        content,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:24]


def test_space_worker_and_attention_serialization_include_contract_fields() -> None:
    space = Space.from_dict(
        {
            "id": "space-1",
            "name": "Alpha",
            "status": "ACTIVE",
            "updated_at": "2026-01-01T00:00:00+00:00",
            "status_line": "all green",
            "meta": {"safe": True},
        }
    )
    worker = Worker.from_dict(
        {
            "id": "worker-1",
            "name": "Agent One",
            "status": "waiting",
            "space_id": "space-1",
            "last_seen_at": "2026-01-01T00:00:01+00:00",
            "summary": "awaiting input",
            "meta": {"safe": "worker"},
        }
    )
    signal = AttentionSignal.from_dict(
        {
            "kind": "worker_status",
            "severity": "warning",
            "status": "blocked",
            "reason": "Worker is blocked",
            "source": "worker:worker-1",
            "updated_at": "2026-01-01T00:00:02+00:00",
            "suggested_actions": [
                {
                    "action_id": "inspect-worker",
                    "label": "Inspect worker",
                    "tendwire_action": "snapshot",
                    "params": {"worker_id": "worker-1"},
                }
            ],
            "meta": {"safe": "attention"},
        }
    )

    space_payload = space.to_dict()
    worker_payload = worker.to_dict()
    signal_payload = signal.to_dict()

    assert space_payload["status"] == "active"
    assert {"updated_at", "status_line", "fingerprint", "meta"} <= set(space_payload)
    assert {"last_seen_at", "summary", "fingerprint", "meta"} <= set(worker_payload)
    assert {
        "id",
        "fingerprint",
        "kind",
        "severity",
        "status",
        "reason",
        "source",
        "updated_at",
        "suggested_actions",
        "meta",
    } <= set(signal_payload)
    assert len(space_payload["fingerprint"]) == 24
    assert len(worker_payload["fingerprint"]) == 24
    assert len(signal_payload["fingerprint"]) == 24
    assert signal_payload["id"]
    assert signal_payload["suggested_actions"][0]["action_id"] == "inspect-worker"
    assert signal_payload["suggested_actions"][0]["tendwire_action"] == "snapshot"
    assert "command" not in signal_payload["suggested_actions"][0]
    assert Space.from_dict(space_payload).to_dict() == space_payload
    assert Worker.from_dict(worker_payload).to_dict() == worker_payload
    assert AttentionSignal.from_dict(signal_payload).to_dict() == signal_payload
    _assert_no_forbidden_fields(signal_payload)


def test_public_model_text_values_preserve_public_connector_words_before_fingerprints() -> None:
    dirty_space = Space.from_dict(
        {
            "id": "space-1",
            "name": "telegram delivery workspace",
            "status": "telegram delivery",
            "status_line": "backend target chat id",
            "meta": {
                "active_tab_id": "tab-1",
                "safe": "kept",
                "note": "terminal id",
                "delivery.route": "leaked route",
                "connector.outbox": "leaked connector",
                "private.label": "leaked private",
                "raw.status": "leaked raw",
                "telegram.delivery": "leaked route",
                "nested": {"safe_nested": "kept", "herdres.route": "leaked route"},
            },
        }
    )
    clean_space = Space(
        id="space-1",
        name="telegram delivery workspace",
        status="unknown",
        meta={
            "active_tab_id": "tab-1",
            "raw_status": "telegram delivery",
            "safe": "kept",
            "nested": {"safe_nested": "kept"},
        },
    )
    dirty_worker = Worker(
        id="worker-1",
        name="message id worker",
        status="bot token status",
        summary="backend target pane id",
        meta={
            "safe": "kept",
            "note": "session id",
            "delivery.route": "leaked route",
            "connector.outbox": "leaked connector",
            "private.label": "leaked private",
            "raw.status": "leaked raw",
            "telegram.delivery": "leaked route",
            "nested": {"safe_nested": "kept", "herdres.route": "leaked route"},
        },
    )
    clean_worker = Worker(
        id="worker-1",
        name="worker-1",
        status="unknown",
        meta={"safe": "kept", "nested": {"safe_nested": "kept"}},
    )
    dirty_action = SuggestedAction(
        action_id="telegram delivery action",
        label="chat id approve",
        tendwire_action="herdres route approve",
        params={
            "safe": "kept",
            "note": "backend target",
            "delivery.route": "leaked route",
            "connector.outbox": "leaked connector",
            "private.label": "leaked private",
            "raw.status": "leaked raw",
            "telegram.delivery": "leaked route",
            "nested": {"safe_nested": "kept", "herdres.route": "leaked route"},
        },
    )
    clean_action = SuggestedAction(
        action_id="telegram delivery action",
        label="Action",
        params={"safe": "kept", "nested": {"safe_nested": "kept"}},
    )
    dirty_signal = AttentionSignal(
        id="telegram delivery attention",
        kind="telegram delivery",
        severity="warning",
        status="waiting",
        reason="message id approval",
        source="herdres route",
        suggested_actions=[dirty_action],
        fingerprint="herdres route private fingerprint",
        meta={
            "safe": "kept",
            "note": "bot token",
            "delivery.route": "leaked route",
            "connector.outbox": "leaked connector",
            "private.label": "leaked private",
            "raw.status": "leaked raw",
            "telegram.delivery": "leaked route",
            "nested": {"safe_nested": "kept", "herdres.route": "leaked route"},
        },
        host_id="model-host",
    )
    clean_signal = AttentionSignal(
        id="telegram delivery attention",
        kind="telegram delivery",
        severity="warning",
        status="waiting",
        source="unknown",
        suggested_actions=[clean_action],
        meta={"safe": "kept", "nested": {"safe_nested": "kept"}},
        host_id="model-host",
    )
    fallback_space = Space.from_dict({"name": "telegram delivery workspace"})
    fallback_worker = Worker.from_dict(
        {
            "name": "telegram chat id worker",
            "space_id": "telegram delivery workspace",
        }
    )
    explicit_dirty_space = Space(
        id="telegram delivery workspace",
        name="Clean",
        fingerprint="telegram delivery private fingerprint",
    )
    explicit_dirty_worker = Worker(
        id="telegram chat id worker",
        name="Clean",
        space_id="telegram delivery workspace",
        fingerprint="backend target private fingerprint",
    )
    private_suffix_space = Space(
        id="target-private",
        name="Clean",
    )
    private_suffix_worker = Worker(
        id="pane-private",
        name="Clean",
        space_id="target-private",
    )
    standalone_backend_space = Space(
        id="space-2",
        name="herdres outbox",
        status_line="outbox",
    )
    standalone_backend_worker = Worker(
        id="worker-2",
        name="Clean",
        summary="herdres queue",
    )
    standalone_backend_action = SuggestedAction(
        label="herdres worker",
        params={"safe": "kept"},
    )

    payload = {
        "space": dirty_space.to_dict(),
        "worker": dirty_worker.to_dict(),
        "signal": dirty_signal.to_dict(),
        "fallback_space": fallback_space.to_dict(),
        "fallback_worker": fallback_worker.to_dict(),
        "explicit_dirty_space": explicit_dirty_space.to_dict(),
        "explicit_dirty_worker": explicit_dirty_worker.to_dict(),
        "redacted_suffix_space": private_suffix_space.to_dict(),
        "redacted_suffix_worker": private_suffix_worker.to_dict(),
        "standalone_backend_space": standalone_backend_space.to_dict(),
        "standalone_backend_worker": standalone_backend_worker.to_dict(),
        "standalone_backend_action": standalone_backend_action.to_dict(),
    }
    encoded = json.dumps(payload, sort_keys=True).lower()

    assert dirty_space.to_dict() == clean_space.to_dict()
    assert dirty_worker.to_dict() == clean_worker.to_dict()
    assert dirty_action.to_dict() == clean_action.to_dict()
    assert dirty_signal.to_dict() == clean_signal.to_dict()
    assert fallback_space.id == "unknown"
    assert fallback_space.name == "telegram delivery workspace"
    assert fallback_worker.id == "unknown"
    assert fallback_worker.name == fallback_worker.id
    assert fallback_worker.space_id == "telegram delivery workspace"
    assert explicit_dirty_space.id == "telegram delivery workspace"
    assert explicit_dirty_space.name == "Clean"
    assert explicit_dirty_worker.id.startswith("worker-")
    assert explicit_dirty_worker.name == "Clean"
    assert explicit_dirty_worker.space_id == "telegram delivery workspace"
    assert private_suffix_space.id.startswith("space-")
    assert private_suffix_space.name == "Clean"
    assert private_suffix_worker.id.startswith("worker-")
    assert private_suffix_worker.name == "Clean"
    assert private_suffix_worker.space_id is not None
    assert private_suffix_worker.space_id.startswith("space-")
    assert standalone_backend_space.name == "herdres outbox"
    assert standalone_backend_space.status_line == "outbox"
    assert standalone_backend_worker.summary == "herdres queue"
    assert standalone_backend_action.label == "herdres worker"
    assert "telegram delivery workspace" in encoded
    assert "telegram delivery action" in encoded
    assert "telegram delivery attention" in encoded
    assert "herdres outbox" in encoded
    assert "herdres queue" in encoded
    for forbidden in (
        "route",
        "private",
        "raw.status",
        "backend target",
        "pane id",
        "session id",
        "terminal id",
        "chat id",
        "message id",
        "bot token",
    ):
        assert forbidden not in encoded
    _assert_no_forbidden_fields(payload)


def test_worker_private_backend_target_and_raw_backend_meta_do_not_serialize() -> None:
    worker = Worker(
        id="public-worker",
        name="Agent One",
        status="active",
        meta={
            "safe": "kept",
            "terminal_id": "term-1",
            "pane_id": "pane-1",
            "agent_session": {"value": "sess-1"},
            "session_id": "session-1",
            "backend_target": {"kind": "agent_id", "value": "agent-1", "sendable": True, "reason": None},
        },
        backend_target={"kind": "agent_id", "value": "agent-1", "sendable": True, "reason": None},
    )

    payload = worker.to_dict()

    assert payload["id"] == "public-worker"
    assert payload["meta"] == {"safe": "kept"}
    assert worker.backend_target == {"kind": "agent_id", "value": "agent-1", "sendable": True, "reason": None}
    _assert_no_forbidden_fields(payload)


def test_worker_binding_is_private_and_not_snapshot_serialized() -> None:
    private_fingerprint = worker_binding_private_fingerprint(
        host_id="host-a",
        backend="herdr",
        identity_material={"agent_session": "sess-private", "pane_id": "pane-private"},
    )
    assert private_fingerprint != worker_binding_private_fingerprint(
        host_id="host-b",
        backend="herdr",
        identity_material={"agent_session": "sess-private", "pane_id": "pane-private"},
    )
    binding = WorkerBinding(
        host_id="host-a",
        worker_id="worker-public",
        worker_fingerprint="worker-fp",
        backend="herdr",
        target_kind="pane_id",
        target_value="pane-private",
        turn_target_kind="terminal_id",
        turn_target_value="term-private",
        sendable=True,
        reason=None,
        observed_at="2026-01-01T00:00:00+00:00",
        expires_at="2026-01-02T00:00:00+00:00",
        private_fingerprint=private_fingerprint,
    )
    snapshot = Snapshot(
        host_id="host-a",
        updated_at="2026-01-01T00:00:00+00:00",
        workers=[
            Worker(
                id="worker-public",
                name="Worker",
                status="active",
                meta={
                    "safe": "kept",
                    "target_kind": "pane_id",
                    "target_value": "pane-private",
                    "turn_target_kind": "terminal_id",
                    "turn_target_value": "term-private",
                    "private_fingerprint": private_fingerprint,
                },
                backend_target=binding.backend_target(),
            )
        ],
    )

    payload = snapshot.to_dict()
    encoded = json.dumps(payload, sort_keys=True)

    assert binding.private_fingerprint == private_fingerprint
    assert binding.backend_target() == {
        "kind": "pane_id",
        "value": "pane-private",
        "sendable": True,
        "reason": None,
    }
    assert payload["workers"][0]["meta"] == {"safe": "kept"}
    assert "pane-private" not in encoded
    assert private_fingerprint not in encoded
    _assert_no_forbidden_fields(payload)


def test_separate_duplicate_worker_bindings_splits_colliding_private_identities() -> None:
    binding_a = WorkerBinding(
        host_id="host-a",
        worker_id="public-a",
        worker_fingerprint="worker-fp-a",
        backend="herdr",
        target_kind="agent_id",
        target_value="same-agent",
        sendable=True,
        reason=None,
        observed_at="2026-01-01T00:00:00+00:00",
        private_fingerprint="colliding-private",
    )
    binding_b = WorkerBinding(
        host_id="host-a",
        worker_id="public-b",
        worker_fingerprint="worker-fp-b",
        backend="herdr",
        target_kind="agent_id",
        target_value="same-agent",
        turn_target_kind="pane_id",
        turn_target_value="pane-b",
        sendable=True,
        reason=None,
        observed_at="2026-01-01T00:00:00+00:00",
        private_fingerprint="colliding-private",
    )

    separated = separate_duplicate_worker_bindings([binding_a, binding_b])
    separated_again = separate_duplicate_worker_bindings(separated)

    assert separated == separated_again
    assert {binding.worker_id for binding in separated} == {"public-a", "public-b"}
    assert {binding.sendable for binding in separated} == {False}
    assert {binding.reason for binding in separated} == {"duplicate_backend_target"}
    assert "colliding-private" not in {binding.private_fingerprint for binding in separated}
    assert len({binding.private_fingerprint for binding in separated}) == 2


def test_separate_duplicate_worker_bindings_leaves_single_private_identity_unchanged() -> None:
    binding = WorkerBinding(
        host_id="host-a",
        worker_id="public-a",
        worker_fingerprint="worker-fp-a",
        backend="herdr",
        target_kind="agent_id",
        target_value="agent-a",
        sendable=True,
        reason=None,
        observed_at="2026-01-01T00:00:00+00:00",
        private_fingerprint="private-a",
    )

    assert separate_duplicate_worker_bindings([binding]) == [binding]


def test_attention_signal_direct_identity_and_dataclass_actions_are_neutral() -> None:
    action = SuggestedAction(
        action_id="inspect-worker",
        label="Inspect worker",
        params={"worker_id": "worker-1", "route": "telegram", "safe": "kept"},
    )

    def signal(**overrides: Any) -> AttentionSignal:
        data = {
            "kind": "worker_status",
            "severity": "warning",
            "status": "blocked",
            "reason": "Worker is blocked",
            "source": "worker:worker-1",
            "updated_at": "2026-01-01T00:00:00+00:00",
            "suggested_actions": [action],
            "host_id": "attention-host",
        }
        data.update(overrides)
        return AttentionSignal(**data)

    base = signal()
    same = signal(updated_at="2026-01-02T00:00:00+00:00")
    changed_status = signal(status="failed")
    changed_reason = signal(reason="Worker is failed")
    changed_source = signal(source="worker:worker-2")
    payload = base.to_dict()

    assert same.id == base.id
    assert same.fingerprint == base.fingerprint
    assert changed_status.fingerprint != base.fingerprint
    assert changed_reason.fingerprint != base.fingerprint
    assert changed_source.fingerprint != base.fingerprint
    assert payload["suggested_actions"][0]["params"] == {
        "worker_id": "worker-1",
        "safe": "kept",
    }
    assert "tendwire_action" not in payload["suggested_actions"][0]
    assert "command" not in payload["suggested_actions"][0]
    _assert_no_forbidden_fields(payload)


def test_attention_signal_preserves_explicit_safe_tendwire_action_publicly() -> None:
    action = SuggestedAction(
        action_id="approve-worker",
        label="Approve",
        tendwire_action="approve_interaction",
        params={"safe": "kept", "telegram_message_id": "sentinel-message"},
    )
    signal = AttentionSignal(
        kind="worker_status",
        severity="warning",
        status="waiting",
        reason="Worker needs approval",
        source="worker:worker-1",
        updated_at="2026-01-01T00:00:00+00:00",
        suggested_actions=[action],
        host_id="attention-host",
    )

    payload = signal.to_dict()

    assert payload["suggested_actions"][0] == {
        "action_id": "approve-worker",
        "label": "Approve",
        "tendwire_action": "approve_interaction",
        "params": {"safe": "kept"},
    }
    _assert_no_forbidden_fields(payload)


def test_done_status_aliases_canonicalize_to_sendable_done() -> None:
    for raw_status in ("done", "complete", "completed", "success"):
        assert normalize_status(raw_status) == "done"


def test_snapshot_json_has_schema_version_content_fingerprint_and_legacy_keys() -> None:
    config = Config(host_id="testhost")
    snapshot = project_empty(config)
    payload = _snapshot_payload(snapshot)

    assert {
        "schema_version",
        "content_fingerprint",
        "host_id",
        "updated_at",
        "spaces",
        "workers",
        "attention",
        "backend_health",
    } <= set(payload)
    assert payload["schema_version"] == 2
    assert payload["host_id"] == "testhost"
    assert len(payload["content_fingerprint"]) == 24
    assert payload["content_fingerprint"] == _expected_snapshot_fingerprint(payload)
    assert isinstance(payload["updated_at"], str)
    assert isinstance(payload["spaces"], list)
    assert isinstance(payload["workers"], list)
    assert payload["attention"] == []
    assert payload["backend_health"] == []
    _assert_no_forbidden_fields(payload)


def test_backend_health_serialization_is_public_safe() -> None:
    health = BackendHealth(
        name="Herdr",
        status="HEALTHY",
        outcome="healthy_non_empty",
        observed_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        message="Herdr observation is healthy",
        counts={
            "spaces": 1,
            "workers": "2",
            "pane_id": 9,
            "private_fingerprint": 10,
            "other": 11,
        },
    )
    payload = health.to_dict()

    assert payload == {
        "name": "herdr",
        "status": "healthy",
        "outcome": "healthy_non_empty",
        "observed_at": "2026-01-01T00:00:00+00:00",
        "message": "Herdr observation is healthy",
        "counts": {"spaces": 1},
    }
    _assert_no_forbidden_fields(payload)

    redacted = BackendHealth.from_dict(
        {
            "name": "herdr",
            "status": "not-real",
            "outcome": "not-real",
            "message": "stderr token pane_id secret should not leak",
            "counts": {"workers": 1, "target_value": 1},
        }
    ).to_dict()

    assert redacted["status"] == "unknown"
    assert redacted["outcome"] == "unknown"
    assert redacted["message"] == "Backend health details redacted"
    assert redacted["counts"] == {"workers": 1}
    assert "pane_id" not in json.dumps(redacted)


def test_backend_health_message_redacts_secret_label_variants() -> None:
    unsafe_messages = [
        "apiKey=abc123 failed",
        "api-key=abc123 failed",
        "api_key=abc123 failed",
        "api key=abc123 failed",
        "botToken=abc123 failed",
        "bot-token=abc123 failed",
        "bot_token=abc123 failed",
        "bot token=abc123 failed",
        "token=abc123 failed",
        "secret=abc123 failed",
        "password=abc123 failed",
        "Password=abc123 failed",
        "PASSWORD=abc123 failed",
        "env=PROD failed",
        "stdout=abc123 failed",
        "stderr=abc123 failed",
        "stderr token pane_id secret",
        "backend.target leaked",
        "pane.id leaked",
        "message.id leaked",
        "raw.command.line leaked",
    ]

    for message in unsafe_messages:
        payload = BackendHealth.from_dict({"message": message}).to_dict()
        encoded = json.dumps(payload)
        assert payload["message"] == "Backend health details redacted"
        assert "abc123" not in encoded
        assert "PROD" not in encoded


def test_backend_health_name_preserves_public_connector_words() -> None:
    safe_names = [
        "herdres",
        "telegram",
        "telegram-connector",
        "connector-outbox",
    ]

    for name in safe_names:
        payload = BackendHealth.from_dict({"name": name}).to_dict()
        assert payload["name"] == name


def test_backend_health_name_buckets_private_names() -> None:
    unsafe_names = [
        "private-backend",
        "raw-status",
        "token-store",
        "pane-id",
        "terminal-id",
        "backend-target",
    ]

    for name in unsafe_names:
        payload = BackendHealth.from_dict({"name": name}).to_dict()
        encoded = json.dumps(payload).lower()
        assert payload["name"] == "unknown"
        assert "private" not in encoded
        assert "token" not in encoded
        assert "pane" not in encoded
        assert "terminal" not in encoded
        assert "backend-target" not in encoded

    assert BackendHealth.from_dict({"name": "herdr-socket"}).to_dict()["name"] == "herdr-socket"


def test_backend_health_message_keeps_safe_fixed_messages() -> None:
    safe_messages = [
        "Herdr observation is healthy",
        "Herdr observation is healthy but empty",
        "Herdr command returned nonzero status",
        "Herdr socket disconnected",
        "Herdr backend is unavailable",
        "Herdres Telegram connector degraded",
        "Telegram connector outbox drained",
    ]

    for message in safe_messages:
        assert BackendHealth.from_dict({"message": message}).to_dict()["message"] == message


def test_snapshot_backend_health_roundtrip_and_fingerprint_ignore_observed_at() -> None:
    config = Config(host_id="health-host")
    snapshot_a = project_from_raw(
        config,
        backend_health=[
            {
                "name": "herdr",
                "status": "healthy",
                "outcome": "empty_healthy",
                "observed_at": "2026-01-01T00:00:00+00:00",
                "message": "Herdr observation is healthy but empty",
                "counts": {"spaces": 0, "workers": 0},
            }
        ],
    )
    snapshot_b = project_from_raw(
        config,
        backend_health=[
            {
                "name": "herdr",
                "status": "healthy",
                "outcome": "empty_healthy",
                "observed_at": "2026-01-02T00:00:00+00:00",
                "message": "Herdr observation is healthy but empty",
                "counts": {"spaces": 0, "workers": 0},
            }
        ],
    )
    changed = project_from_raw(
        config,
        backend_health=[
            {
                "name": "herdr",
                "status": "degraded",
                "outcome": "malformed_json",
                "observed_at": "2026-01-02T00:00:00+00:00",
                "message": "Herdr command returned malformed JSON",
                "counts": {"spaces": 0, "workers": 0},
            }
        ],
    )

    assert Snapshot.from_dict(snapshot_a.to_dict()) == snapshot_a
    assert snapshot_a.content_fingerprint == snapshot_b.content_fingerprint
    assert changed.content_fingerprint != snapshot_a.content_fingerprint
    assert _snapshot_payload(snapshot_a)["content_fingerprint"] == _expected_snapshot_fingerprint(
        _snapshot_payload(snapshot_a)
    )


def test_snapshot_from_json_roundtrip_preserves_fingerprints() -> None:
    config = Config(host_id="testhost")
    snapshot = project_from_raw(
        config,
        spaces=[{"id": "space-1", "name": "Alpha", "status": "active"}],
        workers=[{"id": "worker-1", "name": "Agent One", "status": "blocked"}],
    )

    restored = Snapshot.from_dict(snapshot.to_dict())

    assert restored == snapshot
    assert restored.content_fingerprint == snapshot.content_fingerprint
    assert restored.workers[0].fingerprint == snapshot.workers[0].fingerprint
    assert restored.attention[0].fingerprint == snapshot.attention[0].fingerprint


def test_attention_ids_are_stable_and_fingerprints_track_logical_changes() -> None:
    config = Config(host_id="attention-host")
    timestamp_a = datetime(2026, 1, 1, tzinfo=timezone.utc)
    timestamp_b = datetime(2026, 1, 2, tzinfo=timezone.utc)

    def signal_for(worker: dict[str, Any], timestamp: datetime = timestamp_a) -> dict[str, Any]:
        snapshot = project_from_raw(config, workers=[worker], timestamp=timestamp)
        payload = _snapshot_payload(snapshot)
        assert len(payload["attention"]) == 1
        return payload["attention"][0]

    base = signal_for({"id": "worker-1", "name": "Agent One", "status": "blocked"})
    same = signal_for(
        {"id": "worker-1", "name": "Agent One", "status": "blocked"},
        timestamp=timestamp_b,
    )
    changed_status = signal_for(
        {"id": "worker-1", "name": "Agent One", "status": "failed"}
    )
    changed_reason = signal_for(
        {"id": "worker-1", "name": "Agent Two", "status": "blocked"}
    )
    changed_source = signal_for(
        {"id": "worker-2", "name": "Agent One", "status": "blocked"}
    )

    assert same["id"] == base["id"]
    assert same["fingerprint"] == base["fingerprint"]
    assert same["updated_at"] is None
    assert base["updated_at"] is None
    assert changed_status["fingerprint"] != base["fingerprint"]
    assert changed_reason["fingerprint"] != base["fingerprint"]
    assert changed_source["fingerprint"] != base["fingerprint"]


def test_attention_filters_status_feed_noise_and_empty_snapshots() -> None:
    config = Config(host_id="attention-host")
    snapshot = project_from_raw(
        config,
        workers=[
            {"id": "active", "name": "Active", "status": "running"},
            {"id": "idle", "name": "Idle", "status": "idle"},
            {"id": "done", "name": "Done", "status": "completed"},
            {"id": "closed", "name": "Closed", "status": "closed"},
            {"id": "waiting", "name": "Waiting", "status": "waiting", "summary": "waiting for response"},
            {"id": "pending", "name": "Pending", "status": "pending", "summary": "pending work"},
        ],
    )

    assert _snapshot_payload(project_empty(config))["attention"] == []
    assert _snapshot_payload(snapshot)["attention"] == []


def test_attention_emits_failed_blocked_warning_and_explicit_human_waiting() -> None:
    config = Config(host_id="attention-host")
    snapshot = project_from_raw(
        config,
        workers=[
            {"id": "failed", "name": "Failed", "status": "error"},
            {"id": "blocked", "name": "Blocked", "status": "blocked"},
            {"id": "warning", "name": "Warning", "status": "warning"},
            {
                "id": "needs-input",
                "name": "Needs Input",
                "status": "waiting",
                "space_id": "space-1",
                "meta": {"needs_human": True},
            },
            {
                "id": "approval",
                "name": "Approval",
                "status": "pending",
                "summary": "human approval required before continuing",
            },
        ],
    )
    attention = {item["source"]: item for item in _snapshot_payload(snapshot)["attention"]}

    assert attention["worker:failed"]["severity"] == "critical"
    assert attention["worker:failed"]["status"] == "failed"
    assert attention["worker:blocked"]["severity"] == "warning"
    assert attention["worker:warning"]["severity"] == "warning"
    assert attention["worker:needs-input"]["severity"] == "warning"
    assert attention["worker:needs-input"]["meta"]["worker_id"] == "needs-input"
    assert attention["worker:needs-input"]["meta"]["space_id"] == "space-1"
    assert attention["worker:needs-input"]["meta"]["needs_human"] is True
    assert attention["worker:approval"]["severity"] == "warning"


def test_attention_updated_at_uses_worker_source_time_without_snapshot_fallback() -> None:
    config = Config(host_id="attention-host")
    timestamp_a = datetime(2026, 1, 1, tzinfo=timezone.utc)
    timestamp_b = datetime(2026, 1, 2, tzinfo=timezone.utc)
    source_time = "2026-01-03T00:00:00+00:00"

    first = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "Agent One", "status": "blocked"}],
        timestamp=timestamp_a,
    )
    second = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "Agent One", "status": "blocked"}],
        timestamp=timestamp_b,
    )
    sourced = project_from_raw(
        config,
        workers=[
            {
                "id": "worker-1",
                "name": "Agent One",
                "status": "blocked",
                "last_seen_at": source_time,
            }
        ],
        timestamp=timestamp_b,
    )
    sourced_from_updated = project_from_raw(
        config,
        workers=[
            {
                "id": "worker-2",
                "name": "Agent Two",
                "status": "blocked",
                "updated_at": source_time,
            }
        ],
        timestamp=timestamp_b,
    )

    first_signal = _snapshot_payload(first)["attention"][0]
    second_signal = _snapshot_payload(second)["attention"][0]
    assert first_signal["id"] == second_signal["id"]
    assert first_signal["updated_at"] is None
    assert second_signal["updated_at"] is None
    assert _snapshot_payload(sourced)["attention"][0]["updated_at"] == source_time
    assert _snapshot_payload(sourced_from_updated)["attention"][0]["updated_at"] is None


def test_snapshot_content_fingerprint_ignores_updated_at_and_sorts_content() -> None:
    config = Config(host_id="fingerprint-host")
    timestamp_a = datetime(2026, 1, 1, tzinfo=timezone.utc)
    timestamp_b = datetime(2026, 1, 2, tzinfo=timezone.utc)
    spaces = [
        {"id": "space-b", "name": "Beta", "status": "active"},
        {"id": "space-a", "name": "Alpha", "status": "idle"},
    ]
    workers = [
        {"id": "worker-b", "name": "Bravo", "status": "active", "space_id": "space-b"},
        {"id": "worker-a", "name": "Alpha", "status": "active", "space_id": "space-a"},
    ]

    snapshot_a = project_from_raw(config, spaces=spaces, workers=workers, timestamp=timestamp_a)
    snapshot_b = project_from_raw(
        config,
        spaces=list(reversed(spaces)),
        workers=list(reversed(workers)),
        timestamp=timestamp_b,
    )
    changed = project_from_raw(
        config,
        spaces=spaces,
        workers=[
            {"id": "worker-b", "name": "Bravo", "status": "active", "space_id": "space-b"},
            {"id": "worker-a", "name": "Alpha", "status": "waiting", "space_id": "space-a"},
        ],
        timestamp=timestamp_b,
    )

    payload_a = _snapshot_payload(snapshot_a)
    payload_b = _snapshot_payload(snapshot_b)

    assert payload_a["updated_at"] != payload_b["updated_at"]
    assert payload_a["content_fingerprint"] == payload_b["content_fingerprint"]
    assert payload_a["content_fingerprint"] == _expected_snapshot_fingerprint(payload_a)
    assert changed.content_fingerprint != snapshot_a.content_fingerprint


def test_worker_attention_includes_local_target_metadata() -> None:
    config = Config(host_id="attention-host")
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "Agent One", "status": "blocked", "space_id": "space-1"}],
    )
    signal = _snapshot_payload(snapshot)["attention"][0]

    assert signal["meta"]["needs_human"] is True
    assert signal["meta"]["worker_id"] == "worker-1"
    assert signal["meta"]["space_id"] == "space-1"


def test_project_from_raw_normalizes_status_and_strips_connector_fields() -> None:
    config = Config(host_id="strip-host")
    snapshot = project_from_raw(
        config,
        spaces=[
            {
                "id": "space-1",
                "name": "Alpha",
                "status": "running",
                "telegram": "forbidden",
                "chat_id": 123,
                "meta": {"token": "secret", "safe": "kept"},
            }
        ],
        workers=[
            {
                "id": "worker-1",
                "name": "Agent One",
                "status": "panic",
                "space_id": "space-1",
                "topic_id": 456,
                "message_id": 789,
                "delivery": {"route": "telegram"},
                "meta": {"bot_token": "secret", "safe": "worker-kept"},
            }
        ],
    )
    payload = _snapshot_payload(snapshot)

    assert payload["spaces"][0]["status"] == "active"
    assert payload["spaces"][0]["meta"]["safe"] == "kept"
    assert payload["workers"][0]["status"] == "failed"
    assert payload["workers"][0]["meta"]["raw_status"] == "panic"
    assert payload["workers"][0]["meta"]["safe"] == "worker-kept"
    _assert_no_forbidden_fields(payload)


def test_utc_timestamp_is_iso8601_with_z_offset() -> None:
    ts = utc_timestamp()
    assert ts.endswith("+00:00")
    assert "T" in ts
