"""Tests for the PR7 Tendwire daemon skeleton and local JSON API."""

from __future__ import annotations

import errno
import gc
import io
import json
import os
import signal
import socket
import sqlite3
import stat
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from tendwire import __version__
from tendwire.cli import main
from tendwire.config import Config
from tendwire.core.commands import (
    DISPOSITION_IN_PROGRESS,
    DISPOSITION_NO_RECEIPT,
    DISPOSITION_TERMINAL_ACCEPTED,
    DISPOSITION_TERMINAL_REJECTED,
    DISPOSITION_TERMINAL_UNCERTAIN,
    STATUS_BACKEND_UNAVAILABLE,
    STATUS_REQUEST_STATE_UNCERTAIN,
    STATUS_ACCEPTED,
    STATUS_INVALID_REQUEST,
    STATUS_PENDING,
    CommandEnvelope,
    CommandRequest,
)
from tendwire.core.models import (
    AttentionSignal,
    BackendHealth,
    Snapshot,
    SuggestedAction,
    Worker,
    WorkerBinding,
)
from tendwire.core.projector import project_from_raw
from tendwire.core.turns import (
    pending_payload_from_snapshot,
    recompute_pending_content_fingerprint,
)
from tendwire.daemon import DaemonHooks, TendwireDaemon, run_daemon
from tendwire.daemon_api import (
    DaemonAPIClient,
    DaemonUnavailable,
    DaemonProtocolError,
    TendwireDaemonAPI,
    UnixSocketJSONServer,
    ensure_daemon_socket_not_active,
    MAX_RESPONSE_BYTES,
)
from tendwire.local_state import LocalStateError, LocalStateErrorCode, LocalStateKind
from tendwire.store.sqlite import (
    SnapshotObservationContext,
    attention_payload_from_store,
    get_command_request,
    init_store,
    latest_snapshot,
    merge_backend_pending,
    merge_turn_content,
    pending_payload_from_store,
    save_snapshot,
    upsert_worker_bindings,
)


_PUBLIC_JSON_FORBIDDEN_KEYS = {
    "tty",
    "pty",
    "pid",
    "process_id",
    "pane_id",
    "terminal_id",
    "backend_target",
    "session_id",
    "private",
    "private_binding",
    "private_fingerprint",
    "route",
    "delivery",
    "connector",
    "command",
    "raw_command",
    "chat_id",
    "topic_id",
    "message_id",
    "token",
    "secret",
    "password",
    "credentials",
}
_PUBLIC_JSON_FORBIDDEN_COMPACT = {key.replace("_", "") for key in _PUBLIC_JSON_FORBIDDEN_KEYS}


@pytest.fixture(autouse=True)
def _required_acp_supervisor_for_daemon_unit_tests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep non-ACP daemon tests focused on their own boundary."""

    class Supervisor:
        def __init__(self, config: Config) -> None:
            self.config = config

        def start(self) -> None:
            assert self.config.db_path is not None
            init_store(self.config.db_path)
            if latest_snapshot(self.config.db_path, self.config.host_id) is None:
                save_snapshot(
                    self.config.db_path,
                    Snapshot(
                        host_id=self.config.host_id,
                        updated_at="2026-08-04T00:00:00+00:00",
                        backend_health=[
                            BackendHealth(
                                name="herdr",
                                status="healthy",
                                outcome="empty_healthy",
                            )
                        ],
                    ),
                )
            return None

        def stop(self, *, timeout: float) -> None:
            del timeout

        def join(self, *, timeout: float) -> bool:
            del timeout
            return True

        def status(self) -> dict[str, Any]:
            return {"state": "running", "healthy": True}

        def prompt_route(self, _worker: Worker) -> None:
            return None

    original_init = TendwireDaemon.__init__

    def init_with_required_acp(self: TendwireDaemon, *args: Any, **kwargs: Any) -> None:
        original_init(self, *args, **kwargs)
        hooks = self.hooks
        if (
            hooks.acp_supervisor_factory is not None
            and getattr(hooks.acp_supervisor_factory, "__name__", "")
            == "_default_acp_supervisor_factory"
        ):
            object.__setattr__(
                hooks,
                "acp_supervisor_factory",
                lambda config, _stop: Supervisor(config),
            )
            self._acp_supervisor = Supervisor(self.config)

    monkeypatch.setattr(TendwireDaemon, "__init__", init_with_required_acp)


def _assert_no_public_json_forbidden(value: Any, path: str = "$") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            normalized = str(key).lower().replace("-", "_")
            daemon_pid = (
                path == "$.result"
                and normalized == "pid"
                and value.get("version") == __version__
            )
            assert (
                daemon_pid
                or normalized not in _PUBLIC_JSON_FORBIDDEN_KEYS
                and normalized.replace("_", "") not in _PUBLIC_JSON_FORBIDDEN_COMPACT
            ), f"forbidden field {path}.{key}"
            _assert_no_public_json_forbidden(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _assert_no_public_json_forbidden(item, f"{path}[{index}]")


def _public_snapshot() -> Snapshot:
    return Snapshot(
        host_id="daemon-host",
        updated_at="2026-01-01T00:00:00+00:00",
        workers=[
            Worker(
                id="worker-1",
                name="Worker One",
                status="waiting",
                summary="approval required before continuing",
                meta={
                    "safe": "kept",
                    "tty": "sentinel-private-tty",
                    "pane_id": "sentinel-private-pane",
                    "connectorId": "sentinel-private-connector",
                    "authToken": "sentinel-private-token",
                },
                backend_target={
                    "kind": "agent_id",
                    "value": "sentinel-private-target",
                    "sendable": True,
                },
            )
        ],
        attention=[
            AttentionSignal(
                kind="worker_status",
                severity="warning",
                status="waiting",
                reason="approval required before continuing",
                source="worker:worker-1",
                updated_at="2026-01-01T00:00:00+00:00",
                suggested_actions=[
                    SuggestedAction(
                        action_id="approve",
                        label="Approve",
                        tendwire_action="approve",
                        params={"safe": "kept", "message_id": "sentinel-private-message"},
                    )
                ],
                meta={"needs_human": True, "space_id": "space-1", "private": "sentinel-private-meta"},
            )
        ],
        backend_health=[
            BackendHealth(
                name="herdr",
                status="healthy",
                outcome="healthy_non_empty",
                observed_at="2026-01-01T00:00:00+00:00",
                message="healthy",
                counts={"workers": 1},
            )
        ],
    )


def test_daemon_api_required_methods_are_public_safe() -> None:
    snapshot = _public_snapshot()
    calls: list[dict[str, Any]] = []
    api = TendwireDaemonAPI(
        get_snapshot=lambda: snapshot,
        get_health=lambda: {
            "schema_version": 1,
            "status": "ok",
            "host_id": snapshot.host_id,
            "backend_health": [health.to_dict() for health in snapshot.backend_health],
        },
        submit_command=lambda params: calls.append(dict(params))
        or CommandEnvelope.from_error(
            None,
            {
                "code": STATUS_INVALID_REQUEST,
                "message": "bad command",
                "details": {"fields": ["$.tty"]},
            },
        ),
    )

    for method in ("ping", "health.get", "snapshot.get", "attention.list", "turn.list", "pending.list"):
        response = api.dispatch({"method": method})
        assert response["ok"] is True
        if method in {"ping", "health.get"}:
            assert response["result"]["version"] == __version__
            assert response["result"]["pid"] == os.getpid()
        encoded = json.dumps(response)
        assert "sentinel-private" not in encoded
        _assert_no_public_json_forbidden(response)
    default_pending = api.dispatch({"method": "pending.list"})["result"]
    assert default_pending == pending_payload_from_snapshot(snapshot)
    assert default_pending["pending_health"] == {
        "status": "healthy",
        "counts": {"fresh": 0, "stale": 0, "total": 0},
    }
    assert (
        default_pending["content_fingerprint"]
        == recompute_pending_content_fingerprint(default_pending)
    )

    command_response = api.dispatch(
        {
            "method": "command.submit",
            "params": {
                "schema_version": 1,
                "action": "noop",
                "tty": "sentinel-private-tty",
            },
        }
    )
    assert command_response["ok"] is True
    assert command_response["schema_version"] == 1
    assert command_response["result"]["ok"] is False
    assert command_response["result"]["schema_version"] == 2
    assert command_response["result"]["disposition"] == DISPOSITION_NO_RECEIPT
    assert calls[0]["tty"] == "sentinel-private-tty"
    assert "sentinel-private" not in json.dumps(command_response)
    _assert_no_public_json_forbidden(command_response)


def test_daemon_answer_pending_response_is_recursively_public_safe() -> None:
    snapshot = _public_snapshot()
    request = CommandRequest(
        action="answer_pending",
        request_id="answer-public",
        dry_run=False,
        params={
            "pending_id": "pending-" + ("a" * 24),
            "pending_fingerprint": "b" * 24,
            "choice_id": "choice-" + ("c" * 24),
        },
    )
    api = TendwireDaemonAPI(
        get_snapshot=lambda: snapshot,
        get_health=lambda: {"schema_version": 1, "status": "ok"},
        submit_command=lambda _params: CommandEnvelope.from_result(
            request,
            ok=True,
            status=STATUS_ACCEPTED,
            disposition=DISPOSITION_TERMINAL_ACCEPTED,
            result={
                "target": {
                    "worker_id": "worker-public",
                    "pane_id": "sentinel-private-pane",
                    "private_binding": "sentinel-private-binding",
                },
                "pending": {
                    "id": "pending-" + ("a" * 24),
                    "fingerprint": "b" * 24,
                    "decision_id": "sentinel-private-decision",
                },
                "choice": {
                    "choice_id": "choice-" + ("c" * 24),
                    "tool_id": "sentinel-private-tool",
                    "raw_payload": "sentinel-private-option",
                },
                "delivery_state": "submitted",
                "transport_state": "submitted",
                "observed_pending_state": "pending_observation",
            },
        ),
    )

    response = api.dispatch(
        {
            "method": "command.submit",
            "params": request.to_dict(),
        }
    )
    result = response["result"]["result"]

    assert response["schema_version"] == 1
    assert response["ok"] is True
    assert response["result"]["schema_version"] == 2
    assert response["result"]["disposition"] == DISPOSITION_TERMINAL_ACCEPTED
    assert result == {
        "target": {"worker_id": "worker-public"},
        "pending": {
            "id": "pending-" + ("a" * 24),
            "fingerprint": "b" * 24,
        },
        "choice": {"choice_id": "choice-" + ("c" * 24)},
        "delivery_state": "submitted",
        "transport_state": "submitted",
        "observed_pending_state": "pending_observation",
    }
    assert "sentinel-private" not in json.dumps(response, sort_keys=True)
    _assert_no_public_json_forbidden(response)


@pytest.mark.parametrize(
    ("disposition", "ok", "status", "error"),
    [
        (DISPOSITION_NO_RECEIPT, False, STATUS_BACKEND_UNAVAILABLE, {
            "code": STATUS_BACKEND_UNAVAILABLE,
            "message": "unavailable",
        }),
        (DISPOSITION_IN_PROGRESS, False, STATUS_PENDING, {
            "code": STATUS_PENDING,
            "message": "pending",
        }),
        (DISPOSITION_TERMINAL_ACCEPTED, True, STATUS_ACCEPTED, None),
        (DISPOSITION_TERMINAL_REJECTED, False, STATUS_BACKEND_UNAVAILABLE, {
            "code": STATUS_BACKEND_UNAVAILABLE,
            "message": "rejected",
        }),
        (
            DISPOSITION_TERMINAL_UNCERTAIN,
            False,
            STATUS_REQUEST_STATE_UNCERTAIN,
            {
                "code": STATUS_REQUEST_STATE_UNCERTAIN,
                "message": "uncertain",
            },
        ),
    ],
)
def test_daemon_command_submit_preserves_exact_disposition(
    disposition: str,
    ok: bool,
    status: str,
    error: dict[str, Any] | None,
) -> None:
    request = CommandRequest(
        action="send_instruction",
        request_id=f"daemon-{disposition}",
        dry_run=False,
        target={"worker_id": "w-1"},
        instruction={"text": "hello"},
    )
    envelope = CommandEnvelope.from_result(
        request,
        ok=ok,
        status=status,
        disposition=disposition,
        error=error,
    )
    api = TendwireDaemonAPI(
        get_snapshot=_public_snapshot,
        get_health=lambda: {"schema_version": 1, "status": "ok"},
        submit_command=lambda _params: envelope,
    )

    response = api.dispatch(
        {"method": "command.submit", "params": request.to_dict()}
    )

    assert response["schema_version"] == 1
    assert response["ok"] is True
    assert response["result"] == envelope.to_dict()
    assert response["result"]["schema_version"] == 2
    assert response["result"]["disposition"] == disposition


def test_daemon_command_submit_rejects_malformed_inner_envelope() -> None:
    api = TendwireDaemonAPI(
        get_snapshot=_public_snapshot,
        get_health=lambda: {"schema_version": 1, "status": "ok"},
        submit_command=lambda _params: {
            "schema_version": 2,
            "action": "send_instruction",
            "request_id": "malformed-inner",
            "ok": True,
            "dry_run": False,
            "status": STATUS_ACCEPTED,
            "result": {},
            "error": None,
            "warnings": [],
        },
    )

    response = api.dispatch(
        {
            "method": "command.submit",
            "params": {
                "schema_version": 1,
                "action": "send_instruction",
                "request_id": "malformed-inner",
                "dry_run": False,
                "target": {"worker_id": "w-1"},
                "instruction": {"text": "hello"},
            },
        }
    )

    assert response["schema_version"] == 1
    assert response["ok"] is False
    assert response["result"] is None
    assert response["error"]["code"] == "internal_error"
    assert "disposition" not in response
    _assert_no_public_json_forbidden(response)


def test_daemon_connector_pending_projection_is_recursively_public_safe() -> None:
    snapshot = _public_snapshot()
    api = TendwireDaemonAPI(
        get_snapshot=lambda: snapshot,
        get_health=lambda: {"schema_version": 1, "status": "ok"},
        submit_command=lambda _params: {},
        connector_call=lambda _method, _params: {
            "schema_version": 1,
            "ok": True,
            "status": "ok",
            "items": [
                {
                    "pending_id": "pending-" + ("d" * 24),
                    "choice_id": "choice-" + ("e" * 24),
                    "pane_id": "sentinel-private-pane",
                    "decision_id": "sentinel-private-decision",
                    "tool_id": "sentinel-private-tool",
                    "raw_payload": "sentinel-private-option",
                }
            ],
        },
    )

    response = api.dispatch(
        {
            "method": "connector.poll",
            "params": {"name": "pending-public"},
        }
    )

    assert response["result"]["items"] == [
        {"choice_id": "choice-" + ("e" * 24)}
    ]
    assert "sentinel-private" not in json.dumps(response, sort_keys=True)
    _assert_no_public_json_forbidden(response)


def test_daemon_pending_matches_shared_durable_projection_and_fingerprint(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "pending-parity.db"
    snapshot = _public_snapshot()
    config = Config(host_id=snapshot.host_id, db_path=db_path)
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    baseline = pending_payload_from_snapshot(snapshot)
    assert baseline["pending_health"] == {
        "status": "healthy",
        "counts": {"fresh": 0, "stale": 0, "total": 0},
    }
    degraded = dict(baseline)
    degraded["pending_health"] = {
        "status": "degraded",
        "counts": {"fresh": 0, "stale": 1, "total": 1},
    }
    assert (
        recompute_pending_content_fingerprint(degraded)
        != baseline["content_fingerprint"]
    )
    merge_backend_pending(
        db_path,
        snapshot.host_id,
        "worker-1",
        {
            "question": "Choose the durable option?",
            "kind": "choice",
            "choices": [
                {"choice_id": "safe", "label": "Safe"},
                {
                    "choice_id": "private",
                    "label": "sentinel-private-pane",
                    "value": "sentinel-private-command",
                },
            ],
            "meta": {
                "source": "backend",
                "pane_id": "sentinel-private-pane",
            },
        },
    )

    daemon_payload = TendwireDaemon(config).get_pending()
    shared_payload = pending_payload_from_store(db_path, snapshot.host_id)

    assert daemon_payload == shared_payload
    assert daemon_payload["content_fingerprint"] != baseline["content_fingerprint"]
    assert daemon_payload["pending_interactions"][0]["question"] == "Choose the durable option?"
    assert "sentinel-private" not in json.dumps(daemon_payload, sort_keys=True)
    _assert_no_public_json_forbidden(daemon_payload)


@pytest.mark.parametrize(
    "stored_payload",
    [
        "not-json sentinel-private-invalid",
        json.dumps("sentinel-private-scalar"),
        json.dumps(["sentinel-private-list"]),
        json.dumps(
            {
                "updated_at": "2026-01-01T00:00:00+00:00",
                "workers": [],
                "sentinel": "sentinel-private-missing-host",
            }
        ),
        json.dumps(
            {
                "host_id": "malformed-pending",
                "workers": [],
                "sentinel": "sentinel-private-missing-updated",
            }
        ),
        json.dumps(
            {
                "host_id": "sentinel-private-cross-host",
                "updated_at": "2026-01-01T00:00:00+00:00",
                "workers": [],
            }
        ),
        json.dumps(
            {
                "host_id": "malformed-pending",
                "updated_at": "2026-01-01T00:00:00+00:00",
                "workers": [["sentinel-private-nested"]],
            }
        ),
        json.dumps(
            {
                "host_id": "malformed-pending",
                "updated_at": "2026-01-01T00:00:00+00:00",
                "workers": [
                    {
                        "id": "worker-1",
                        "name": "Worker One",
                        "meta": ["sentinel-private-nested-meta"],
                    }
                ],
            }
        ),
    ],
    ids=[
        "invalid-json",
        "scalar-json",
        "list-json",
        "missing-host",
        "missing-updated-at",
        "cross-host",
        "malformed-nested",
        "malformed-nested-meta",
    ],
)
def test_malformed_durable_snapshot_is_fixed_unavailable_for_daemon_and_cli(
    tmp_path: Path,
    capsys,
    stored_payload: str,
) -> None:
    db_path = tmp_path / "malformed-pending.db"
    host_id = "malformed-pending"
    config = Config(host_id=host_id, data_dir=tmp_path, db_path=db_path)
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO snapshots (
                host_id, created_at, content_fingerprint, payload
            ) VALUES (?, ?, ?, ?)
            """,
            (
                host_id,
                "2026-01-01T00:00:00+00:00",
                "sentinel-private-fingerprint",
                stored_payload,
            ),
        )

    daemon_payload = TendwireDaemon(config).get_pending()
    cli_code = main(
        [
            "--host-id",
            host_id,
            "--socket-path",
            str(tmp_path / "missing.sock"),
            "pending",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    cli_payload = json.loads(capsys.readouterr().out)
    expected = {
        "schema_version": 1,
        "host_id": host_id,
        "ok": False,
        "status": "store_unavailable",
        "pending_interactions": [],
        "backend_health": [],
        "pending_health": {
            "status": "store_unavailable",
            "counts": {"fresh": 0, "stale": 0, "total": 0},
        },
    }

    assert cli_code == 1
    assert daemon_payload == expected
    assert cli_payload["ok"] is False
    assert cli_payload["status"] == "daemon_unavailable"
    assert "sentinel-private" not in json.dumps(
        {"daemon": daemon_payload, "cli": cli_payload},
        sort_keys=True,
    )
    _assert_no_public_json_forbidden(daemon_payload)
    _assert_no_public_json_forbidden(cli_payload)


def test_pending_store_projection_reads_snapshot_and_overlay_atomically(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from contextlib import contextmanager

    import tendwire.store.sqlite as sqlite_store

    db_path = tmp_path / "pending-atomic.db"
    config = Config(host_id="atomic-pending", db_path=db_path)
    snapshot_a = project_from_raw(
        config,
        workers=[
            {
                "id": "worker-1",
                "name": "Worker One",
                "status": "blocked",
                "summary": "snapshot-a",
            }
        ],
    )
    snapshot_b = project_from_raw(
        config,
        workers=[
            {
                "id": "worker-1",
                "name": "Worker One",
                "status": "waiting",
                "summary": "snapshot-b",
            }
        ],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot_a)
    merge_backend_pending(
        db_path,
        config.host_id,
        "worker-1",
        {"question": "Backend A?", "kind": "question", "meta": {"source": "backend"}},
    )

    allow_writer = threading.Event()
    writer_done = threading.Event()
    writer_errors: list[BaseException] = []
    reader_thread_id = threading.get_ident()
    original_connect = sqlite_store._connect

    @contextmanager
    def traced_connect(*args: Any, **kwargs: Any) -> Any:
        with original_connect(*args, **kwargs) as conn:
            if threading.get_ident() == reader_thread_id:
                def trace(statement: str) -> None:
                    normalized = " ".join(statement.lower().split())
                    if normalized.startswith(
                        "select worker_id, payload_json, choice_routes_json"
                    ):
                        allow_writer.set()
                        writer_done.wait(timeout=5)

                conn.set_trace_callback(trace)
            yield conn

    def publish_new_view() -> None:
        try:
            assert allow_writer.wait(timeout=5)
            save_snapshot(db_path, snapshot_b)
            merge_backend_pending(
                db_path,
                config.host_id,
                "worker-1",
                {
                    "question": "Backend B?",
                    "kind": "question",
                    "meta": {"source": "backend"},
                },
            )
        except BaseException as exc:
            writer_errors.append(exc)
        finally:
            writer_done.set()

    monkeypatch.setattr(sqlite_store, "_connect", traced_connect)
    writer = threading.Thread(target=publish_new_view)
    writer.start()
    first = pending_payload_from_store(db_path, config.host_id)
    writer.join(timeout=5)

    assert not writer.is_alive()
    assert writer_errors == []
    assert first["pending_interactions"][0]["question"] == "Backend A?"
    assert (
        first["pending_interactions"][0]["worker_fingerprint"]
        == snapshot_a.workers[0].fingerprint
    )

    second = pending_payload_from_store(db_path, config.host_id)
    assert second["pending_interactions"][0]["question"] == "Backend B?"
    assert (
        second["pending_interactions"][0]["worker_fingerprint"]
        == snapshot_b.workers[0].fingerprint
    )
    assert TendwireDaemon(config).get_pending() == second


def test_daemon_api_versions_turn_list_and_preserves_exact_content_page() -> None:
    turn_calls: list[dict[str, Any]] = []
    page_calls: list[dict[str, Any]] = []
    page_text = "\n  " + ("α" * 20_000) + "  \r\n"
    revision = "twrev1.ueLJtVatFOQxa1UePvWId8C01qdrb05FpW_ipSSPHMM"

    def get_turns(**params: Any) -> dict[str, Any]:
        turn_calls.append(dict(params))
        cursor = params["cursor"]
        since = params["since"]
        if cursor in {"invalid", "expired"}:
            status = "invalid_cursor" if cursor == "invalid" else "cursor_expired"
            return {
                "schema_version": params["schema_version"],
                "ok": False,
                "status": status,
                "error": {"code": status, "message": "turn list cursor is unavailable"},
            }
        if since == "expired":
            return {
                "schema_version": params["schema_version"],
                "ok": False,
                "status": "since_expired",
                "error": {
                    "code": "since_expired",
                    "message": "turn list watermark is unavailable",
                },
            }
        return {
            "schema_version": params["schema_version"],
            "turns": [
                {
                    "id": "turn-public",
                    "assistant_final_text": "\n exact inline  ",
                    "content": {
                        "schema_version": 1,
                        "content_revision": "twrev1.public",
                        "known_incomplete": False,
                        "fields": {
                            "assistant_final_text": {
                                "availability": "complete",
                                "inline": True,
                            }
                        },
                    },
                }
            ],
        }

    api = TendwireDaemonAPI(
        get_snapshot=lambda: Snapshot(host_id="daemon-host"),
        get_health=lambda: {"schema_version": 1, "status": "ok"},
        submit_command=lambda _params: {},
        get_turns=get_turns,
        get_turn_content=lambda params: page_calls.append(dict(params))
        or {
            "schema_version": 1,
            "ok": True,
            "status": "ok",
            "turn_id": "turn-public",
            "content_revision": revision,
            "field": "assistant_final_text",
            "availability": "complete",
            "segment_id": "twseg1.public",
            "index": 0,
            "count": 1,
            "text": page_text,
            "segment_char_length": len(page_text),
            "segment_byte_length": len(page_text.encode("utf-8")),
            "total_char_length": len(page_text),
            "total_byte_length": len(page_text.encode("utf-8")),
            "next_cursor": None,
        },
    )

    listed = api.dispatch(
        {
            "method": "turn.list",
            "params": {
                "schema_version": 2,
                "limit": 17,
                "cursor": "twlist1.valid",
            },
        }
    )
    page = api.dispatch(
        {
            "method": "turn.content.get",
            "params": {
                "schema_version": 1,
                "turn_id": "turn-public",
                "content_revision": revision,
                "field": "assistant_final_text",
            },
        }
    )
    invalid_cursor = api.dispatch(
        {"method": "turn.list", "params": {"schema_version": 2, "cursor": "invalid"}}
    )
    expired_cursor = api.dispatch(
        {"method": "turn.list", "params": {"schema_version": 2, "cursor": "expired"}}
    )
    expired_since = api.dispatch(
        {"method": "turn.list", "params": {"schema_version": 2, "since": "expired"}}
    )
    calls_before_rejections = len(turn_calls)
    rejected = [
        api.dispatch({"method": "turn.list", "params": {"schema_version": 3}}),
        api.dispatch({"method": "turn.list", "params": {"limit": True}}),
        api.dispatch({"method": "turn.list", "params": {"limit": 0}}),
        api.dispatch({"method": "turn.list", "params": {"limit": 251}}),
        api.dispatch({"method": "turn.list", "params": {"cursor": ""}}),
        api.dispatch({"method": "turn.list", "params": {"since": 7}}),
        api.dispatch(
            {
                "method": "turn.list",
                "params": {"cursor": "twlist1.valid", "since": "twsince1.valid"},
            }
        ),
        api.dispatch({"method": "turn.list", "params": {"private": "sentinel"}}),
    ]

    assert listed["result"]["schema_version"] == 2
    assert listed["result"]["turns"][0]["assistant_final_text"] == "\n exact inline  "
    assert turn_calls[0] == {
        "schema_version": 2,
        "limit": 17,
        "cursor": "twlist1.valid",
        "since": None,
    }
    assert page["result"]["text"] == page_text
    assert page["result"]["content_revision"] == revision
    assert page_calls == [
        {
            "schema_version": 1,
            "turn_id": "turn-public",
            "content_revision": revision,
            "field": "assistant_final_text",
        }
    ]
    assert invalid_cursor["ok"] is True
    assert invalid_cursor["result"]["status"] == "invalid_cursor"
    assert expired_cursor["ok"] is True
    assert expired_cursor["result"]["status"] == "cursor_expired"
    assert expired_since["ok"] is True
    assert expired_since["result"]["status"] == "since_expired"
    assert len(turn_calls) == calls_before_rejections
    assert rejected[0]["error"]["code"] == "unsupported_schema"
    assert all(response["ok"] is False for response in rejected)
    assert all(
        response["error"]["code"] in {"unsupported_schema", "invalid_params"}
        for response in rejected
    )
    assert "sentinel" not in json.dumps(rejected, sort_keys=True)


def test_daemon_turn_list_is_store_projection_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "projection.db"
    config = Config(host_id="projection-host", db_path=db_path)
    snapshot = Snapshot(
        host_id=config.host_id,
        updated_at="2026-01-01T00:00:00+00:00",
    )
    save_snapshot(db_path, snapshot)
    projection_calls: list[dict[str, Any]] = []

    def project(
        path: Path,
        host_id: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        projection_calls.append({"path": path, "host_id": host_id, **kwargs})
        return {
            "schema_version": kwargs["schema_version"],
            "host_id": host_id,
            "ok": True,
            "status": "ok",
            "turns": [],
        }

    monkeypatch.setattr("tendwire.store.sqlite.turns_payload_from_store", project)
    daemon = TendwireDaemon(config)

    for _ in range(3):
        result = daemon.get_turns(
            schema_version=2,
            limit=17,
            cursor="twlist1.public",
            since=None,
        )
        assert result["status"] == "ok"

    assert len(projection_calls) == 3
    assert all(
        call == {
            "path": db_path,
            "host_id": config.host_id,
            "snapshot": snapshot,
            "schema_version": 2,
            "limit": 17,
            "cursor": "twlist1.public",
            "since": None,
            "turn_model": "observed",
        }
        for call in projection_calls
    )


def test_daemon_api_protocol_errors_do_not_echo_private_request_names() -> None:
    api = TendwireDaemonAPI(
        get_snapshot=lambda: Snapshot(host_id="daemon-host"),
        get_health=lambda: {"schema_version": 1, "status": "ok", "host_id": "daemon-host"},
        submit_command=lambda params: CommandEnvelope.from_error(
            None,
            {
                "code": STATUS_INVALID_REQUEST,
                "message": "bad command",
                "details": {},
            },
        ),
    )

    unknown_field = api.dispatch(
        {
            "method": "ping",
            "telegram.bot.token": "sentinel-private-field",
            "backend.target": "sentinel-private-target",
        }
    )
    unknown_method = api.dispatch({"method": "telegram.bot.token"})
    unsafe_id = api.dispatch({"id": "telegram.bot.token", "method": "telegram.bot.token"})
    unsafe_object_id = api.dispatch(
        {
            "id": {"backend.target": "sentinel-private-id"},
            "method": "telegram.bot.token",
        }
    )
    unsafe_prefixed_ids = {
        private_id: api.dispatch({"id": private_id, "method": "telegram.bot.token"})
        for private_id in ("x-api_key", "my-api-key", "credentials", "my-credentials")
    }
    safe_id = api.dispatch({"id": "req-123_ok.1", "method": "ping"})

    unknown_field_encoded = json.dumps(unknown_field, sort_keys=True).lower()
    unknown_method_encoded = json.dumps(unknown_method, sort_keys=True).lower()
    unsafe_id_encoded = json.dumps(unsafe_id, sort_keys=True).lower()
    unsafe_object_id_encoded = json.dumps(unsafe_object_id, sort_keys=True).lower()

    assert unknown_field["ok"] is False
    assert unknown_field["error"]["message"] == "request contains unknown top-level fields"
    assert unknown_field["error"]["details"] == {"field_count": 2}
    assert "sentinel-private" not in unknown_field_encoded
    assert "telegram" not in unknown_field_encoded
    assert "bot.token" not in unknown_field_encoded
    assert "backend.target" not in unknown_field_encoded

    assert unknown_method["ok"] is False
    assert unknown_method["error"]["message"] == "unknown method"
    assert "telegram" not in unknown_method_encoded
    assert "bot.token" not in unknown_method_encoded
    assert unsafe_id["ok"] is False
    assert "id" not in unsafe_id
    assert "telegram" not in unsafe_id_encoded
    assert "bot.token" not in unsafe_id_encoded
    assert unsafe_object_id["ok"] is False
    assert "id" not in unsafe_object_id
    assert "sentinel-private" not in unsafe_object_id_encoded
    assert "backend.target" not in unsafe_object_id_encoded
    for private_id, response in unsafe_prefixed_ids.items():
        encoded = json.dumps(response, sort_keys=True).lower()
        assert response["ok"] is False
        assert "id" not in response
        assert private_id.lower() not in encoded
    assert safe_id["ok"] is True
    assert safe_id["id"] == "req-123_ok.1"
    _assert_no_public_json_forbidden(unknown_field)
    _assert_no_public_json_forbidden(unknown_method)
    _assert_no_public_json_forbidden(unsafe_id)
    _assert_no_public_json_forbidden(unsafe_object_id)
    for response in unsafe_prefixed_ids.values():
        _assert_no_public_json_forbidden(response)
    _assert_no_public_json_forbidden(safe_id)


def test_daemon_api_attention_list_uses_store_lifecycle_payload(tmp_path: Path) -> None:
    db_path = tmp_path / "attention-api.db"
    config = Config(host_id="daemon-host", db_path=db_path)
    observed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    snapshot = project_from_raw(
        config,
        workers=[
            {
                "id": "worker-1",
                "name": "Worker One",
                "status": "blocked",
                "meta": {
                    "safe": "kept",
                    "pane_id": "sentinel-private-pane",
                    "backendTarget": "sentinel-private-backend",
                    "authToken": "sentinel-private-token",
                },
            }
        ],
        backend_health=[
            {
                "name": "herdr",
                "status": "healthy",
                "outcome": "healthy_non_empty",
                "observed_at": observed_at.isoformat(),
                "counts": {"workers": 1},
            }
        ],
        timestamp=observed_at,
    )
    save_snapshot(
        db_path,
        snapshot,
        observation=SnapshotObservationContext(
            authority="complete",
            observed_at=observed_at.isoformat(),
        ),
    )
    escalated_at = observed_at + timedelta(seconds=1)
    save_snapshot(
        db_path,
        project_from_raw(
            config,
            workers=[
                {
                    "id": "worker-1",
                    "name": "Worker One",
                    "status": "failed",
                    "meta": {"safe": "kept"},
                }
            ],
            backend_health=[
                {
                    "name": "herdr",
                    "status": "healthy",
                    "outcome": "healthy_non_empty",
                    "observed_at": escalated_at.isoformat(),
                    "counts": {"workers": 1},
                }
            ],
            timestamp=escalated_at,
        ),
        observation=SnapshotObservationContext(
            authority="complete",
            observed_at=escalated_at.isoformat(),
        ),
    )
    daemon = TendwireDaemon(config)
    api = TendwireDaemonAPI(
        get_snapshot=daemon.get_snapshot,
        get_health=daemon.get_health,
        submit_command=daemon.submit_command,
        get_attention=daemon.get_attention,
    )

    response = api.dispatch({"method": "attention.list"})
    payload = response["result"]

    assert response["ok"] is True
    assert payload["host_id"] == "daemon-host"
    assert len(payload["attention"]) == 1
    assert payload["attention"][0]["lifecycle_status"] == "open"
    assert payload["attention"][0]["first_seen_at"] == observed_at.isoformat()
    assert payload["attention"][0]["last_seen_at"] == escalated_at.isoformat()
    assert payload["attention"][0]["signal_count"] == 2
    assert payload["attention"][0]["severity"] == "critical"
    assert not {
        "family_key",
        "generation",
        "first_missing_at",
        "missing_observation_count",
        "last_accepted_at",
        "last_observation_key",
        "max_notified_severity_rank",
    }.intersection(payload["attention"][0])
    assert attention_payload_from_store(db_path, "daemon-host") == payload
    assert "sentinel-private" not in json.dumps(response, sort_keys=True)
    _assert_no_public_json_forbidden(response)


def _blocked_worker(status: str) -> list[dict[str, Any]]:
    return [{"id": "worker-1", "name": "Worker One", "status": status}]


# Complete observations are the sole absence authority. Resolution requires
# two distinct misses and 120 seconds elapsed from the first accepted miss.
_HEALTHY_BACKEND = [
    {
        "name": "herdr",
        "status": "healthy",
        "outcome": "healthy_non_empty",
        "observed_at": "2026-01-01T00:00:00+00:00",
        "counts": {"workers": 1},
    }
]


def _attention_outbox_count(db_path: Path, host_id: str) -> int:
    with sqlite3.connect(str(db_path)) as conn:
        return int(
            conn.execute(
                "SELECT COUNT(*) FROM connector_outbox WHERE host_id = ? AND connector = 'attention'",
                (host_id,),
            ).fetchone()[0]
        )

def _complete_observation(observed_at: datetime) -> SnapshotObservationContext:
    return SnapshotObservationContext(
        authority="complete",
        observed_at=observed_at.isoformat(),
    )



def test_attention_positive_after_two_early_complete_misses_does_not_re_notify(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "attention-flap.db"
    config = Config(host_id="flap-host", db_path=db_path)
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)

    save_snapshot(
        db_path,
        project_from_raw(
            config,
            workers=_blocked_worker("blocked"),
            backend_health=_HEALTHY_BACKEND,
            timestamp=base,
        ),
        observation=_complete_observation(base),
    )
    assert _attention_outbox_count(db_path, "flap-host") == 1

    for offset in (30, 90):
        observed_at = base + timedelta(seconds=offset)
        save_snapshot(
            db_path,
            project_from_raw(
                config,
                workers=_blocked_worker("idle"),
                backend_health=_HEALTHY_BACKEND,
                timestamp=observed_at,
            ),
            observation=_complete_observation(observed_at),
        )
    payload = attention_payload_from_store(db_path, "flap-host")
    assert len(payload["attention"]) == 1
    assert payload["attention"][0]["lifecycle_status"] == "open"

    recurrence_at = base + timedelta(seconds=100)
    save_snapshot(
        db_path,
        project_from_raw(
            config,
            workers=_blocked_worker("blocked"),
            backend_health=_HEALTHY_BACKEND,
            timestamp=recurrence_at,
        ),
        observation=_complete_observation(recurrence_at),
    )
    assert _attention_outbox_count(db_path, "flap-host") == 1
    assert len(attention_payload_from_store(db_path, "flap-host")["attention"]) == 1
    with sqlite3.connect(str(db_path)) as conn:
        generation, missing_count = conn.execute(
            """
            SELECT generation, missing_observation_count
            FROM attention_lifecycles
            WHERE host_id = ?
            """,
            ("flap-host",),
        ).fetchone()
    assert (generation, missing_count) == (1, 0)


def test_attention_recurrence_after_two_complete_misses_re_notifies(tmp_path: Path) -> None:
    db_path = tmp_path / "attention-genuine-reopen.db"
    config = Config(host_id="reopen-host", db_path=db_path)
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)

    save_snapshot(
        db_path,
        project_from_raw(
            config,
            workers=_blocked_worker("blocked"),
            backend_health=_HEALTHY_BACKEND,
            timestamp=base,
        ),
        observation=_complete_observation(base),
    )
    assert _attention_outbox_count(db_path, "reopen-host") == 1

    first_miss_at = base + timedelta(seconds=10)
    save_snapshot(
        db_path,
        project_from_raw(
            config,
            workers=_blocked_worker("idle"),
            backend_health=_HEALTHY_BACKEND,
            timestamp=first_miss_at,
        ),
        observation=_complete_observation(first_miss_at),
    )
    assert len(attention_payload_from_store(db_path, "reopen-host")["attention"]) == 1

    second_miss_at = first_miss_at + timedelta(seconds=120)
    save_snapshot(
        db_path,
        project_from_raw(
            config,
            workers=_blocked_worker("idle"),
            backend_health=_HEALTHY_BACKEND,
            timestamp=second_miss_at,
        ),
        observation=_complete_observation(second_miss_at),
    )
    assert attention_payload_from_store(db_path, "reopen-host")["attention"] == []

    recurrence_at = second_miss_at + timedelta(seconds=1)
    save_snapshot(
        db_path,
        project_from_raw(
            config,
            workers=_blocked_worker("blocked"),
            backend_health=_HEALTHY_BACKEND,
            timestamp=recurrence_at,
        ),
        observation=_complete_observation(recurrence_at),
    )
    assert _attention_outbox_count(db_path, "reopen-host") == 2
    assert len(attention_payload_from_store(db_path, "reopen-host")["attention"]) == 1
    with sqlite3.connect(str(db_path)) as conn:
        generation = conn.execute(
            "SELECT generation FROM attention_lifecycles WHERE host_id = ?",
            ("reopen-host",),
        ).fetchone()[0]
    assert generation == 2


def test_daemon_health_exposes_public_operational_status_without_private_values(tmp_path: Path) -> None:
    db_path = tmp_path / "health.db"
    config = Config(
        host_id="health-host",
        db_path=db_path,
        reconcile_interval_seconds=2,
        event_retention_days=3,
        max_workers=8,
        max_outbox_attempts=4,
        connector_claim_ttl_seconds=33,
        connector_max_claim_ttl_seconds=222,
        connector_ack_ttl_seconds=111,
        acknowledged_final_retention_days=40,
        acknowledged_final_retention_count=500,
        snapshot_retention_days=9,
        snapshot_retention_count=70,
        snapshot_maintenance_batch_size=6,
        store_maintenance_cadence_seconds=44,
        pending_stale_grace_seconds=31,
        command_retry_horizon_seconds=120,
        command_receipt_retention_seconds=691_200,
        command_receipt_retention_count=77,
    )
    snapshot = project_from_raw(
        config,
        workers=[
            {
                "id": "worker-1",
                "name": "Worker One",
                "backend_target": {"pane_id": "sentinel-private-pane"},
            }
        ],
        backend_health=[
            {
                "name": "herdr",
                "status": "healthy",
                "outcome": "healthy_non_empty",
                "observed_at": "2026-01-01T00:00:00+00:00",
                "counts": {"workers": 1},
            }
        ],
    )
    save_snapshot(db_path, snapshot)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "health-host",
                "attention",
                "job-1",
                "queued",
                '{"safe":"kept"}',
                '{"token":"sentinel-private-token"}',
                "9999-01-01T00:00:00+00:00",
                "9999-01-01T00:00:00+00:00",
            ),
        )

    daemon = TendwireDaemon(config)
    health = daemon.get_health()
    encoded = json.dumps(health)

    assert health["status"] == "ok"
    assert health["acp"]["required"] is True
    assert health["acp"]["healthy"] is True
    assert health["daemon"]["started_at"]
    assert health["store"]["counts"]["snapshots"] == 1
    assert health["store"]["outbox"]["pending"] == 1
    assert health["store"]["final_retention"] == {
        "acknowledged": 0,
        "unresolved": 0,
        "queued": 0,
        "leased": 0,
        "deferred": 0,
        "retry": 0,
        "dead_letter": 0,
        "awaiting_ack": 0,
        "eligible": 0,
        "acknowledged_final_retention_days": 40,
        "acknowledged_final_retention_count": 500,
        "storage_pressure": False,
    }
    assert health["store"]["command_requests"] == {
        "total": 0,
        "states": {
            "reserved": 0,
            "send_started": 0,
            "accepted": 0,
            "rejected": 0,
            "uncertain": 0,
        },
        "stale_active": 0,
        "eligible": 0,
        "retry_horizon_seconds": 120,
        "retention_seconds": 691_200,
        "retention_count": 77,
        "storage_pressure": False,
    }
    assert health["store"]["maintenance"] == {
        "last_completed_at": None,
        "status": "never",
        "snapshot_count": 1,
        "snapshot_retention_days": 9,
        "snapshot_retention_count": 70,
        "maintenance_batch_size": 6,
        "maintenance_cadence_seconds": 44,
        "backlog": False,
    }
    assert health["limits"] == {
        "reconcile_interval_seconds": 2,
        "event_retention_days": 3,
        "max_workers": 8,
        "max_outbox_attempts": 4,
        "outbox_claim_ttl_seconds": 33,
        "outbox_max_claim_ttl_seconds": 222,
        "outbox_ack_ttl_seconds": 111,
        "acknowledged_final_retention_days": 40,
        "acknowledged_final_retention_count": 500,
        "command_retry_horizon_seconds": 120,
        "command_receipt_retention_seconds": 691_200,
        "command_receipt_retention_count": 77,
        "snapshot_retention_days": 9,
        "snapshot_retention_count": 70,
        "snapshot_maintenance_batch_size": 6,
        "store_maintenance_cadence_seconds": 44,
    }
    assert health["pending_ingestion"] == {
        "status": "healthy",
        "counts": {"fresh": 0, "stale": 0, "total": 0},
        "bounds": {"stale_grace_seconds": 31.0},
    }
    assert health["backend"]["reconcile_enabled"] is True
    assert "health.db" not in encoded
    assert str(tmp_path) not in encoded
    assert "sentinel-private" not in encoded
    _assert_no_public_json_forbidden(health)


def test_daemon_health_accepts_valid_command_request_aggregate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tendwire.store import sqlite as store_sqlite

    db_path = tmp_path / "valid-command-health.db"
    config = Config(
        host_id="command-health-host",
        db_path=db_path,
        command_retry_horizon_seconds=120,
        command_receipt_retention_seconds=691_200,
        command_receipt_retention_count=7,
    )
    init_store(db_path)
    save_snapshot(db_path, project_from_raw(config, workers=[]))
    real_status = store_sqlite.store_status
    expected = {
        "total": 15,
        "states": {
            "reserved": 1,
            "send_started": 2,
            "accepted": 3,
            "rejected": 4,
            "uncertain": 5,
        },
        "stale_active": 0,
        "eligible": 0,
        "retry_horizon_seconds": 120,
        "retention_seconds": 691_200,
        "retention_count": 7,
        "storage_pressure": False,
    }

    def valid_status(*args: Any, **kwargs: Any) -> dict[str, Any]:
        payload = real_status(*args, **kwargs)
        payload["command_requests"] = expected
        return payload

    monkeypatch.setattr(store_sqlite, "store_status", valid_status)
    health = TendwireDaemon(config).get_health()

    assert health["status"] == "ok"
    assert health["store"]["status"] == "healthy"
    assert health["store"]["command_requests"] == expected


@pytest.mark.parametrize(
    "case",
    [
        "retry_policy",
        "retention_policy",
        "count_policy",
        "state_type",
        "total",
        "stale_active",
        "eligible",
        "pressure",
        "shape",
    ],
)
def test_daemon_health_rejects_invalid_command_request_aggregate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    from tendwire.store import sqlite as store_sqlite

    db_path = tmp_path / f"invalid-command-health-{case}.db"
    config = Config(
        host_id="command-health-host",
        db_path=db_path,
        command_retry_horizon_seconds=120,
        command_receipt_retention_seconds=691_200,
        command_receipt_retention_count=7,
    )
    init_store(db_path)
    save_snapshot(db_path, project_from_raw(config, workers=[]))
    real_status = store_sqlite.store_status

    def invalid_status(*args: Any, **kwargs: Any) -> dict[str, Any]:
        payload = real_status(*args, **kwargs)
        aggregate = payload["command_requests"]
        if case == "retry_policy":
            aggregate["retry_horizon_seconds"] = 121
        elif case == "retention_policy":
            aggregate["retention_seconds"] = 691_201
        elif case == "count_policy":
            aggregate["retention_count"] = 8
        elif case == "state_type":
            aggregate["states"]["reserved"] = True
        elif case == "total":
            aggregate["total"] = 1
        elif case == "stale_active":
            aggregate["states"]["reserved"] = 1
            aggregate["total"] = 1
            aggregate["stale_active"] = 1
            aggregate["storage_pressure"] = True
        elif case == "eligible":
            aggregate["eligible"] = 1
            aggregate["storage_pressure"] = True
        elif case == "pressure":
            aggregate["storage_pressure"] = True
        else:
            del aggregate["eligible"]
        return payload

    monkeypatch.setattr(store_sqlite, "store_status", invalid_status)
    health = TendwireDaemon(config).get_health()

    assert health["status"] == "degraded"
    assert health["store"]["status"] == "unavailable"
    assert health["store"]["command_requests"] == {
        "total": 0,
        "states": {
            "reserved": 0,
            "send_started": 0,
            "accepted": 0,
            "rejected": 0,
            "uncertain": 0,
        },
        "stale_active": 0,
        "eligible": 0,
        "retry_horizon_seconds": 120,
        "retention_seconds": 691_200,
        "retention_count": 7,
        "storage_pressure": False,
    }


def test_daemon_health_degrades_on_command_request_storage_pressure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tendwire.store import sqlite as store_sqlite

    db_path = tmp_path / "command-pressure.db"
    config = Config(
        host_id="command-pressure-host",
        db_path=db_path,
        command_retry_horizon_seconds=120,
        command_receipt_retention_seconds=691_200,
        command_receipt_retention_count=2,
    )
    init_store(db_path)
    save_snapshot(db_path, project_from_raw(config, workers=[]))
    real_status = store_sqlite.store_status

    def pressured_status(*args: Any, **kwargs: Any) -> dict[str, Any]:
        payload = real_status(*args, **kwargs)
        payload["command_requests"] = {
            "total": 4,
            "states": {
                "reserved": 1,
                "send_started": 0,
                "accepted": 0,
                "rejected": 0,
                "uncertain": 3,
            },
            "stale_active": 0,
            "eligible": 1,
            "retry_horizon_seconds": 120,
            "retention_seconds": 691_200,
            "retention_count": 2,
            "storage_pressure": True,
        }
        return payload

    monkeypatch.setattr(store_sqlite, "store_status", pressured_status)
    health = TendwireDaemon(config).get_health()

    assert health["status"] == "degraded"
    assert health["store"]["status"] == "degraded"
    assert health["store"]["command_requests"]["eligible"] == 1
    assert health["store"]["command_requests"]["storage_pressure"] is True


def test_daemon_health_command_request_aggregate_never_exposes_row_data(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tendwire.store import sqlite as store_sqlite

    db_path = tmp_path / "private-command-health.db"
    config = Config(host_id="command-health-host", db_path=db_path)
    init_store(db_path)
    save_snapshot(db_path, project_from_raw(config, workers=[]))
    real_status = store_sqlite.store_status
    private_fields = {
        "id": "sentinel-private-id",
        "request_id": "sentinel-private-request",
        "action": "sentinel-private-action",
        "canonical_request_json": "sentinel-private-canonical-json",
        "canonical_fingerprint": "sentinel-private-canonical-fingerprint",
        "result": "sentinel-private-result",
        "worker": "sentinel-private-worker",
        "binding": "sentinel-private-binding",
    }

    def private_status(*args: Any, **kwargs: Any) -> dict[str, Any]:
        payload = real_status(*args, **kwargs)
        payload["command_requests"].update(private_fields)
        return payload

    monkeypatch.setattr(store_sqlite, "store_status", private_status)
    health = TendwireDaemon(config).get_health()
    command_requests = health["store"]["command_requests"]
    encoded = json.dumps(health, sort_keys=True)

    assert health["status"] == "degraded"
    assert health["store"]["status"] == "unavailable"
    assert set(command_requests) == {
        "total",
        "states",
        "stale_active",
        "eligible",
        "retry_horizon_seconds",
        "retention_seconds",
        "retention_count",
        "storage_pressure",
    }
    assert not set(private_fields).intersection(command_requests)
    assert "sentinel-private" not in encoded
    _assert_no_public_json_forbidden(health)


def test_daemon_health_pending_aggregate_is_fail_closed_and_public_safe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tendwire.store import sqlite as store_sqlite

    db_path = tmp_path / "pending-health.db"
    config = Config(
        host_id="health-host",
        db_path=db_path,
        pending_stale_grace_seconds=19,
    )
    init_store(db_path)
    save_snapshot(db_path, project_from_raw(config, workers=[]))

    def degraded_health(_db_path: Path, _host_id: str) -> dict[str, Any]:
        return {
            "status": "degraded",
            "counts": {"fresh": 4, "stale": 2, "total": 6},
            "pane_id": "sentinel-private-pane",
            "source_path": str(tmp_path / "sentinel-private-source"),
            "tool_id": "sentinel-private-tool",
            "error": "sentinel-private-error",
        }

    monkeypatch.setattr(store_sqlite, "backend_pending_health", degraded_health)

    health = TendwireDaemon(config).get_health()
    encoded = json.dumps(health, sort_keys=True)

    assert health["status"] == "degraded"
    assert health["store"]["status"] == "healthy"
    assert health["pending_ingestion"] == {
        "status": "degraded",
        "counts": {"fresh": 4, "stale": 2, "total": 6},
        "bounds": {"stale_grace_seconds": 19.0},
    }
    assert "sentinel-private" not in encoded
    assert str(tmp_path) not in encoded
    _assert_no_public_json_forbidden(health)
    monkeypatch.setattr(
        store_sqlite,
        "backend_pending_health",
        lambda *_args: {
            "status": "healthy",
            "counts": {"fresh": 1, "stale": 1, "total": 2},
        },
    )
    fail_closed = TendwireDaemon(config).get_health()
    assert fail_closed["pending_ingestion"] == {
        "status": "store_unavailable",
        "counts": {"fresh": 0, "stale": 0, "total": 0},
        "bounds": {"stale_grace_seconds": 19.0},
    }
    monkeypatch.setattr(
        store_sqlite,
        "backend_pending_health",
        lambda *_args: {
            "status": "healthy",
            "counts": {"fresh": 1, "stale": 0, "total": 1},
        },
    )
    recovered = TendwireDaemon(config).get_health()
    assert recovered["status"] == "ok"
    assert recovered["pending_ingestion"] == {
        "status": "healthy",
        "counts": {"fresh": 1, "stale": 0, "total": 1},
        "bounds": {"stale_grace_seconds": 19.0},
    }


def test_daemon_health_degrades_on_public_safe_final_storage_pressure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tendwire.store import sqlite as store_sqlite

    db_path = tmp_path / "final-pressure.db"
    config = Config(
        host_id="pressure-host",
        db_path=db_path,
        acknowledged_final_retention_days=30,
        acknowledged_final_retention_count=2,
    )
    init_store(db_path)
    save_snapshot(db_path, project_from_raw(config, workers=[]))

    def pressured_status(*args: Any, **kwargs: Any) -> dict[str, Any]:
        payload = {
            "schema_version": 1,
            "ok": True,
            "status": "ok",
            "host_id": config.host_id,
            "counts": {
                "snapshots": 1,
                "events": 0,
                "spaces": 0,
                "workers": 0,
                "turns": 0,
                "pending_interactions": 0,
                "attention_items": 0,
                "commands": 0,
                "command_receipts": 0,
                "backend_health": 0,
            },
            "outbox": {
                "pending": 0,
                "leased": 0,
                "completed": 0,
                "by_status": {},
                "due": 0,
                "oldest_due_at": None,
                "overdue_awaiting_ack": 0,
                "drain_target_seconds": 30,
                "starved": False,
            },
            "maintenance": {
                "last_completed_at": None,
                "status": "never",
                "snapshot_count": 1,
                "snapshot_retention_days": config.snapshot_retention_days,
                "snapshot_retention_count": config.snapshot_retention_count,
                "maintenance_batch_size": config.snapshot_maintenance_batch_size,
                "maintenance_cadence_seconds": config.store_maintenance_cadence_seconds,
                "backlog": False,
            },
            "final_retention": {
                "acknowledged": 4,
                "unresolved": 3,
                "queued": 1,
                "leased": 0,
                "deferred": 0,
                "retry": 0,
                "dead_letter": 1,
                "awaiting_ack": 1,
                "eligible": 2,
                "acknowledged_final_retention_days": 30,
                "acknowledged_final_retention_count": 2,
                "storage_pressure": True,
                "row_id": 987,
                "private_state_json": "sentinel-private-state",
                "source_path": str(tmp_path / "sentinel-private-source"),
            },
            "command_requests": {
                "total": 0,
                "states": {
                    "reserved": 0,
                    "send_started": 0,
                    "accepted": 0,
                    "rejected": 0,
                    "uncertain": 0,
                },
                "stale_active": 0,
                "eligible": 0,
                "retry_horizon_seconds": config.command_retry_horizon_seconds,
                "retention_seconds": config.command_receipt_retention_seconds,
                "retention_count": config.command_receipt_retention_count,
                "storage_pressure": False,
            },
        }
        return payload

    monkeypatch.setattr(store_sqlite, "store_status", pressured_status)

    health = TendwireDaemon(config).get_health()
    encoded = json.dumps(health, sort_keys=True)

    assert health["status"] == "degraded"
    assert health["store"]["status"] == "degraded"
    assert health["store"]["final_retention"] == {
        "acknowledged": 4,
        "unresolved": 3,
        "queued": 1,
        "leased": 0,
        "deferred": 0,
        "retry": 0,
        "dead_letter": 1,
        "awaiting_ack": 1,
        "eligible": 2,
        "acknowledged_final_retention_days": 30,
        "acknowledged_final_retention_count": 2,
        "storage_pressure": True,
    }
    assert "sentinel-private" not in encoded
    assert str(tmp_path) not in encoded
    assert "row_id" not in encoded
    _assert_no_public_json_forbidden(health)


def test_daemon_health_degrades_on_valid_snapshot_maintenance_backlog(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tendwire.store import sqlite as store_sqlite

    db_path = tmp_path / "snapshot-pressure.db"
    config = Config(host_id="snapshot-pressure-host", db_path=db_path)
    init_store(db_path)
    save_snapshot(db_path, project_from_raw(config, workers=[]))
    real_status = store_sqlite.store_status

    def backlogged_status(*args: Any, **kwargs: Any) -> dict[str, Any]:
        payload = real_status(*args, **kwargs)
        payload["maintenance"]["backlog"] = True
        return payload

    monkeypatch.setattr(store_sqlite, "store_status", backlogged_status)
    health = TendwireDaemon(config).get_health()

    assert health["status"] == "degraded"
    assert health["store"]["status"] == "degraded"
    assert health["store"]["maintenance"]["backlog"] is True


def test_daemon_health_rejects_cross_host_store_aggregate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tendwire.store import sqlite as store_sqlite

    db_path = tmp_path / "cross-host-health.db"
    config = Config(host_id="expected-host", db_path=db_path)
    init_store(db_path)
    save_snapshot(db_path, project_from_raw(config, workers=[]))
    real_status = store_sqlite.store_status

    def cross_host_status(*args: Any, **kwargs: Any) -> dict[str, Any]:
        payload = real_status(*args, **kwargs)
        payload["host_id"] = "foreign-host"
        payload["counts"]["snapshots"] = 999
        return payload

    monkeypatch.setattr(store_sqlite, "store_status", cross_host_status)
    health = TendwireDaemon(config).get_health()

    assert health["status"] == "degraded"
    assert health["store"]["status"] == "unavailable"
    assert set(health["store"]["counts"].values()) == {0}
    assert health["store"]["maintenance"]["snapshot_count"] == 0


def test_daemon_health_rejects_malformed_aggregate_fields_without_leaking(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tendwire.store import sqlite as store_sqlite

    db_path = tmp_path / "malformed-health.db"
    config = Config(host_id="malformed-host", db_path=db_path)
    init_store(db_path)
    save_snapshot(db_path, project_from_raw(config, workers=[]))
    private_marker = "XYZZY-private-health-marker"
    real_status = store_sqlite.store_status

    def malformed_status(*args: Any, **kwargs: Any) -> dict[str, Any]:
        payload = real_status(*args, **kwargs)
        payload["counts"]["diagnostic"] = private_marker
        payload["outbox"] = {
            "pending": -1,
            "leased": 0,
            "by_status": {},
            "diagnostic": private_marker,
        }
        payload["maintenance"]["last_completed_at"] = private_marker
        payload["final_retention"]["unresolved"] = 0
        payload["final_retention"]["queued"] = 1
        return payload

    monkeypatch.setattr(store_sqlite, "store_status", malformed_status)
    health = TendwireDaemon(config).get_health()
    encoded = json.dumps(health, sort_keys=True)

    assert health["status"] == "degraded"
    assert health["store"]["status"] == "unavailable"
    assert set(health["store"]["counts"].values()) == {0}
    assert health["store"]["outbox"] == {
        "pending": 0,
        "leased": 0,
        "completed": 0,
        "by_status": {},
        "due": 0,
        "oldest_due_at": None,
        "overdue_awaiting_ack": 0,
        "drain_target_seconds": 30,
        "starved": False,
    }
    assert health["store"]["maintenance"]["last_completed_at"] is None
    assert health["store"]["final_retention"]["queued"] == 0
    assert private_marker not in encoded


_UNIX_SOCKET_TEST = pytest.mark.skipif(
    os.name != "posix"
    or not sys.platform.startswith("linux")
    or not hasattr(socket, "AF_UNIX"),
    reason="Linux/POSIX Unix-socket lifecycle contract",
)


def test_sigterm_handler_only_requests_stop_before_ordered_teardown(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tendwire import daemon as daemon_module

    calls: list[str] = []
    installed: dict[int, Any] = {}

    class FakeDaemon:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            pass

        def start(self) -> None:
            calls.append("start")

        def serve_forever(self) -> None:
            calls.append("serve")
            installed[signal.SIGTERM](signal.SIGTERM, None)
            installed[signal.SIGTERM](signal.SIGTERM, None)

        def request_stop(self) -> None:
            calls.append("request_stop")

        def stop(self) -> None:
            calls.append("stop")

    previous = object()
    monkeypatch.setattr(daemon_module, "TendwireDaemon", FakeDaemon)
    monkeypatch.setattr(signal, "getsignal", lambda _signum: previous)

    def capture_signal(signum: int, handler: Any) -> Any:
        if handler is not previous:
            installed[signum] = handler
        return previous

    monkeypatch.setattr(signal, "signal", capture_signal)

    assert run_daemon(Config(data_dir=tmp_path, db_path=tmp_path / "daemon.db")) == 0
    assert calls == [
        "start",
        "serve",
        "request_stop",
        "request_stop",
        "stop",
    ]


def _socket_mode(path: Path) -> int:
    return stat.S_IMODE(os.lstat(path).st_mode)


def _socket_identity(path: Path) -> tuple[int, int]:
    current = os.lstat(path)
    return (int(current.st_dev), int(current.st_ino))


def _bind_unix_listener(path: Path) -> socket.socket:
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        listener.bind(os.fspath(path))
        listener.listen()
    except Exception:
        listener.close()
        raise
    return listener


def _assert_unix_socket_connects(path: Path) -> None:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.settimeout(0.5)
        connection.connect(os.fspath(path))


def _assert_private_daemon_failure(
    error: BaseException,
    *paths: Path,
    forbidden: tuple[str, ...] = (),
) -> None:
    rendered = f"{error!s}\n{error!r}"
    for path in paths:
        assert os.fspath(path) not in rendered
    for value in forbidden:
        assert value not in rendered


def test_snapshot_maintenance_wires_agent_event_retention_without_socket(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "maintenance-wiring.db"
    config = Config(
        host_id="daemon-host",
        data_dir=tmp_path,
        db_path=db_path,
        event_retention_days=9,
        snapshot_maintenance_batch_size=13,
    )
    init_store(db_path)
    captured: dict[str, Any] = {}

    def maintenance(path: Path, **kwargs: Any) -> dict[str, Any]:
        captured.update({"path": path, **kwargs})
        return {
            "schema_version": 1,
            "ok": True,
            "status": "ok",
            "due": False,
            "snapshot": {
                "examined": 0,
                "deleted": 0,
                "remaining_candidates": False,
            },
            "agent_events": {
                "examined": 5,
                "deleted": 4,
                "remaining_candidates": True,
            },
        }

    monkeypatch.setattr(
        "tendwire.store.sqlite.maybe_run_automatic_store_maintenance",
        maintenance,
    )
    daemon = TendwireDaemon(config)
    daemon._after_snapshot_saved()

    assert captured["path"] == db_path
    assert captured["agent_event_host_id"] == "daemon-host"
    assert captured["agent_event_retention_days"] == 9
    assert captured["policy"].batch_size == 13
    assert daemon._automatic_maintenance_status == {
        "ok": True,
        "status": "ok",
        "due": False,
        "examined": 0,
        "deleted": 0,
        "remaining_candidates": False,
        "agent_events_examined": 5,
        "agent_events_deleted": 4,
        "agent_events_remaining_candidates": True,
    }


@_UNIX_SOCKET_TEST
def test_cli_snapshot_barrier_checks_maintenance_once_and_reads_do_not(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = tmp_path / "cli-maintenance"
    db_path = data_dir / "daemon.db"
    config = Config(
        host_id="daemon-host",
        data_dir=data_dir,
        db_path=db_path,
        snapshot_retention_days=21,
        snapshot_retention_count=123,
        snapshot_maintenance_batch_size=17,
        store_maintenance_cadence_seconds=91,
        acknowledged_final_retention_days=33,
        acknowledged_final_retention_count=456,
        command_retry_horizon_seconds=120,
        command_receipt_retention_seconds=691_200,
        command_receipt_retention_count=77,
    )
    calls: list[
        tuple[Path, Any, str | None, int | None, int, int, int, int, int, int]
    ] = []

    def initialize(path: Path) -> None:
        init_store(path)
        snapshot = _public_snapshot()
        save_snapshot(db_path, snapshot)

    def maintenance(
        path: Path,
        *,
        policy: Any,
        agent_event_host_id: str | None = None,
        agent_event_retention_days: int | None = None,
        turn_model: str = "legacy",
        acknowledged_final_retention_days: int = 30,
        acknowledged_final_retention_count: int = 4096,
        command_retry_horizon_seconds: int = 604_800,
        command_receipt_retention_seconds: int = 2_592_000,
        command_receipt_retention_count: int = 4096,
        cadence_seconds: int = 3600,
        now: str | None = None,
    ) -> dict[str, Any]:
        assert now is None
        assert turn_model == "observed"
        calls.append(
            (
                path,
                policy,
                agent_event_host_id,
                agent_event_retention_days,
                acknowledged_final_retention_days,
                acknowledged_final_retention_count,
                command_retry_horizon_seconds,
                command_receipt_retention_seconds,
                command_receipt_retention_count,
                cadence_seconds,
            )
        )
        return {
            "schema_version": 1,
            "ok": True,
            "status": "not_due",
            "due": False,
            "last_completed_at": "2026-01-01T00:00:00Z",
            "next_due_at": "2026-01-01T00:01:31Z",
            "snapshot": {
                "examined": 0,
                "deleted": 0,
                "remaining_candidates": False,
            },
            "agent_events": {
                "examined": 2,
                "deleted": 1,
                "remaining_candidates": True,
            },
            "batch_size": policy.batch_size,
        }

    monkeypatch.setattr(
        "tendwire.store.sqlite.maybe_run_automatic_store_maintenance",
        maintenance,
    )
    daemon = TendwireDaemon(
        config,
        hooks=DaemonHooks(init_store=initialize),
    )
    try:
        daemon.start()
        daemon.get_snapshot()
        daemon.get_snapshot()
        daemon.get_attention()
        health = daemon.get_health()
        daemon.get_health()
    finally:
        daemon.stop()

    assert len(calls) == 1
    (
        path,
        policy,
        agent_host,
        agent_days,
        final_days,
        final_count,
        retry_horizon,
        retention_seconds,
        retention_count,
        cadence,
    ) = calls[0]
    assert path == db_path
    assert (
        policy.retention_days,
        policy.retention_count,
        policy.batch_size,
        agent_host,
        agent_days,
        final_days,
        final_count,
        retry_horizon,
        retention_seconds,
        retention_count,
        cadence,
    ) == (
        21,
        123,
        17,
        "daemon-host",
        config.event_retention_days,
        33,
        456,
        120,
        691_200,
        77,
        91,
    )
    assert health["store"]["maintenance"]["last_check"] == {
        "ok": True,
        "status": "not_due",
        "due": False,
        "examined": 0,
        "deleted": 0,
        "remaining_candidates": False,
        "agent_events_examined": 2,
        "agent_events_deleted": 1,
        "agent_events_remaining_candidates": True,
    }


@_UNIX_SOCKET_TEST
def test_cli_snapshot_persists_when_automatic_maintenance_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = tmp_path / "cli-maintenance-failure"
    db_path = data_dir / "daemon.db"
    config = Config(host_id="daemon-host", data_dir=data_dir, db_path=db_path)
    calls = 0

    def initialize(path: Path) -> None:
        init_store(path)
        snapshot = _public_snapshot()
        save_snapshot(db_path, snapshot)

    def maintenance_failure(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        raise RuntimeError(f"sentinel-private failure at {tmp_path}/secret.db")

    monkeypatch.setattr(
        "tendwire.store.sqlite.maybe_run_automatic_store_maintenance",
        maintenance_failure,
    )
    daemon = TendwireDaemon(
        config,
        hooks=DaemonHooks(init_store=initialize),
    )
    try:
        daemon.start()
        persisted = latest_snapshot(db_path, config.host_id)
        health = daemon.get_health()
    finally:
        daemon.stop()

    encoded = json.dumps(health, sort_keys=True)
    assert calls == 1
    assert persisted is not None
    assert persisted.content_fingerprint == _public_snapshot().content_fingerprint
    assert health["status"] == "degraded"
    assert health["store"]["status"] == "degraded"
    assert health["store"]["maintenance"]["last_check"] == {
        "ok": False,
        "status": "failed",
        "due": False,
        "examined": 0,
        "deleted": 0,
        "remaining_candidates": False,
        "agent_events_examined": 0,
        "agent_events_deleted": 0,
        "agent_events_remaining_candidates": False,
    }
    assert str(tmp_path) not in encoded
    assert "secret.db" not in encoded
    assert "sentinel-private" not in encoded
    _assert_no_public_json_forbidden(health)


@_UNIX_SOCKET_TEST
@pytest.mark.parametrize(
    "existing_mode",
    [None, 0o777],
    ids=["creates-private-parent", "repairs-permissive-parent"],
)
def test_daemon_default_socket_parent_and_endpoint_are_private_under_umask_zero(
    tmp_path: Path,
    existing_mode: int | None,
) -> None:
    data_dir = tmp_path / "default-state"
    if existing_mode is not None:
        data_dir.mkdir()
        os.chmod(data_dir, existing_mode)
    socket_path = data_dir / "tendwire.sock"
    config = Config(
        host_id="daemon-host",
        data_dir=data_dir,
        db_path=data_dir / "daemon.db",
    )
    daemon = TendwireDaemon(
        config,
        hooks=DaemonHooks(init_store=lambda _path: None),
    )

    try:
        previous_umask = os.umask(0)
        try:
            daemon.start()
        finally:
            os.umask(previous_umask)

        assert _socket_mode(data_dir) == 0o700
        assert stat.S_ISSOCK(os.lstat(socket_path).st_mode)
        assert _socket_mode(socket_path) == 0o600
        _assert_unix_socket_connects(socket_path)
    finally:
        daemon.stop()

    assert not os.path.lexists(socket_path)


@_UNIX_SOCKET_TEST
def test_daemon_startup_repairs_all_existing_state_before_empty_observation(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "startup-state"
    data_dir.mkdir()
    os.chmod(data_dir, 0o755)
    db_path = data_dir / "daemon.db"
    init_store(db_path)
    os.chmod(db_path, 0o644)
    config = Config(host_id="daemon-host", data_dir=data_dir, db_path=db_path)
    identity_paths = (
        config.installation_key_path,
        config.installation_key_marker_path,
        config.installation_key_sentinel_path,
    )
    for path in identity_paths:
        path.write_bytes(b"existing-identity")
        os.chmod(path, 0o644)
    def initialize_store(path: Path) -> None:
        assert path == db_path
        assert _socket_mode(data_dir) == 0o700
        assert _socket_mode(db_path) == 0o600
        assert all(_socket_mode(identity_path) == 0o600 for identity_path in identity_paths)

    for _attempt in range(2):
        daemon = TendwireDaemon(
            config,
            hooks=DaemonHooks(init_store=initialize_store),
        )
        try:
            daemon.start()
            assert daemon.snapshot is not None
            assert daemon.snapshot.workers == []
            assert _socket_mode(data_dir) == 0o700
            assert _socket_mode(db_path) == 0o600
            assert all(
                _socket_mode(identity_path) == 0o600
                for identity_path in identity_paths
            )
        finally:
            daemon.stop()

    assert not os.path.lexists(data_dir / "tendwire.sock")


@_UNIX_SOCKET_TEST
def test_daemon_rejects_identity_defect_before_socket_or_hook_work(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "defective-startup-state"
    data_dir.mkdir()
    os.chmod(data_dir, 0o755)
    db_path = data_dir / "daemon.db"
    db_path.write_bytes(b"existing-database")
    os.chmod(db_path, 0o644)
    protected_target = data_dir / "protected-target"
    protected_target.write_bytes(b"unchanged")
    os.chmod(protected_target, 0o600)
    identity_path = data_dir / "installation.key"
    identity_path.symlink_to(protected_target)
    socket_path = data_dir / "tendwire.sock"
    hook_calls: list[str] = []

    def initialize_store(_path: Path) -> None:
        hook_calls.append("init_store")
        raise AssertionError("store hook must not run")

    daemon = TendwireDaemon(
        Config(host_id="daemon-host", data_dir=data_dir, db_path=db_path),
        hooks=DaemonHooks(init_store=initialize_store),
    )

    with pytest.raises(LocalStateError) as caught:
        daemon.start()

    assert caught.value.code is LocalStateErrorCode.WRONG_TYPE
    assert hook_calls == []
    assert daemon.server is None
    assert not os.path.lexists(socket_path)
    assert _socket_mode(data_dir) == 0o755
    assert _socket_mode(db_path) == 0o644
    assert identity_path.is_symlink()
    assert protected_target.read_bytes() == b"unchanged"
    _assert_private_daemon_failure(
        caught.value,
        data_dir,
        db_path,
        identity_path,
        protected_target,
        socket_path,
    )


def test_one_shot_cli_repairs_existing_database_without_initializing_identity(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = tmp_path / "one-shot-state"
    db_path = data_dir / "one-shot.db"
    init_store(db_path)
    os.chmod(data_dir, 0o755)
    os.chmod(db_path, 0o644)
    identity_paths = (
        data_dir / "installation.key",
        data_dir / "installation.key.sha256",
        data_dir / "installation.key.initialized",
    )
    monkeypatch.setenv("TENDWIRE_DATA_DIR", str(data_dir))
    monkeypatch.delenv("TENDWIRE_DB_PATH", raising=False)

    exit_code = main(
        [
            "--host-id",
            "one-shot-host",
            "store",
            "status",
            "--db-path",
            str(db_path),
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["ok"] is True
    assert _socket_mode(data_dir) == 0o700
    assert _socket_mode(db_path) == 0o600
    assert all(not path.exists() for path in identity_paths)


@_UNIX_SOCKET_TEST
def test_daemon_group_socket_and_client_use_exact_shared_mode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import grp

    parent = tmp_path / "shared-socket-parent"
    parent.mkdir()
    target_gid = next(
        (group_id for group_id in os.getgroups() if group_id != os.getegid()),
        os.getegid(),
    )
    try:
        group_name = grp.getgrgid(target_gid).gr_name
    except KeyError:
        target_gid = os.getegid()
        group_name = grp.getgrgid(target_gid).gr_name
    os.chown(parent, -1, target_gid)
    os.chmod(parent, 0o710)
    socket_path = parent / "daemon.sock"
    config = Config(
        host_id="daemon-host",
        data_dir=tmp_path / "private-state",
        db_path=tmp_path / "daemon.db",
        socket_path=socket_path,
        socket_group=group_name,
    )
    daemon = TendwireDaemon(
        config,
        hooks=DaemonHooks(init_store=lambda _path: None),
    )
    thread: threading.Thread | None = None

    try:
        previous_umask = os.umask(0)
        try:
            daemon.start()
        finally:
            os.umask(previous_umask)
        thread = threading.Thread(target=daemon.serve_forever)
        thread.start()

        socket_owner = os.lstat(socket_path).st_uid
        with monkeypatch.context() as client_process:
            client_process.setattr(
                "tendwire.local_state.os.geteuid",
                lambda: socket_owner + 100_000,
            )
            response = DaemonAPIClient(
                socket_path,
                socket_group=group_name,
                timeout_seconds=1,
            ).request("ping")

        assert response["ok"] is True
        assert response["result"]["pong"] is True
        assert _socket_mode(socket_path) == 0o660
        assert os.lstat(socket_path).st_gid == target_gid
    finally:
        daemon.stop()
        if thread is not None:
            thread.join(timeout=2)

    assert thread is not None and not thread.is_alive()
    assert not os.path.lexists(socket_path)


@_UNIX_SOCKET_TEST
def test_group_chown_failure_rolls_back_bound_socket_without_leaking_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import grp

    supplementary = [
        group_id for group_id in os.getgroups() if group_id != os.getegid()
    ]
    if not supplementary:
        pytest.skip("no supplementary group available for chgrp failure coverage")
    target_gid = supplementary[0]
    try:
        group_name = grp.getgrgid(target_gid).gr_name
    except KeyError:
        pytest.skip("supplementary group has no local name")
    parent = tmp_path / "shared-socket-parent"
    parent.mkdir()
    os.chown(parent, -1, target_gid)
    os.chmod(parent, 0o710)
    socket_path = parent / "daemon.sock"
    raw_error_path = os.fspath(socket_path)

    def fail_chown(*_args: Any, **_kwargs: Any) -> None:
        raise OSError(errno.EPERM, "sentinel chown failure", raw_error_path)

    monkeypatch.setattr("tendwire.local_state.os.chown", fail_chown)
    server = UnixSocketJSONServer(
        socket_path,
        lambda _request: {"ok": True},
        socket_group=group_name,
        prepare_parent=False,
    )

    with pytest.raises(DaemonUnavailable) as caught:
        server.start()

    _assert_private_daemon_failure(
        caught.value,
        socket_path,
        forbidden=("sentinel chown failure",),
    )
    assert not os.path.lexists(socket_path)
    server.close()


@_UNIX_SOCKET_TEST
def test_explicit_private_socket_securely_creates_missing_parent(
    tmp_path: Path,
) -> None:
    parent = tmp_path / "explicit-private-parent"
    socket_path = parent / "daemon.sock"
    server = UnixSocketJSONServer(socket_path, lambda _request: {"ok": True})

    try:
        server.start()

        assert _socket_mode(parent) == 0o700
        assert _socket_mode(socket_path) == 0o600
        _assert_unix_socket_connects(socket_path)
    finally:
        server.close()

    assert parent.is_dir()
    assert _socket_mode(parent) == 0o700
    assert not os.path.lexists(socket_path)


@_UNIX_SOCKET_TEST
def test_explicit_private_socket_rejects_writable_parent_before_stale_cleanup(
    tmp_path: Path,
) -> None:
    parent = tmp_path / "unsafe-explicit-parent"
    parent.mkdir()
    os.chmod(parent, 0o1777)
    socket_path = parent / "daemon.sock"
    stale_listener = _bind_unix_listener(socket_path)
    stale_identity = _socket_identity(socket_path)
    stale_listener.close()
    server = UnixSocketJSONServer(socket_path, lambda _request: {"ok": True})

    try:
        with pytest.raises(DaemonUnavailable) as caught:
            server.start()

        assert caught.value.code is LocalStateErrorCode.INSECURE_SOCKET_PARENT
        _assert_private_daemon_failure(caught.value, parent, socket_path)
        assert _socket_mode(parent) == 0o1777
        assert _socket_identity(socket_path) == stale_identity
    finally:
        server.close()
        socket_path.unlink(missing_ok=True)


@_UNIX_SOCKET_TEST
def test_post_bind_pin_failure_rolls_back_exact_bound_endpoint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import tendwire.daemon_api as daemon_api_module

    socket_path = tmp_path / "pin-failure.sock"
    original_pin = daemon_api_module.pin_owned_socket_at

    def fail_post_bind_pin(parent_fd: int, leaf: str) -> Any:
        if os.path.lexists(socket_path):
            raise LocalStateError(
                LocalStateErrorCode.OPERATION_FAILED,
                "secure local-state operation failed",
            )
        return original_pin(parent_fd, leaf)

    monkeypatch.setattr(daemon_api_module, "pin_owned_socket_at", fail_post_bind_pin)
    server = UnixSocketJSONServer(socket_path, lambda _request: {"ok": True})

    with pytest.raises(DaemonUnavailable) as caught:
        server.start()

    assert caught.value.code is LocalStateErrorCode.OPERATION_FAILED
    _assert_private_daemon_failure(caught.value, socket_path)
    assert not os.path.lexists(socket_path)
    server.close()


@_UNIX_SOCKET_TEST
def test_post_bind_pin_failure_never_unlinks_replacement_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import tendwire.daemon_api as daemon_api_module

    socket_path = tmp_path / "pin-substitution.sock"
    original_pin = daemon_api_module.pin_owned_socket_at
    replacement_listener: socket.socket | None = None

    def substitute_before_pin_failure(parent_fd: int, leaf: str) -> Any:
        nonlocal replacement_listener
        if not os.path.lexists(socket_path):
            return original_pin(parent_fd, leaf)
        socket_path.unlink()
        replacement_listener = _bind_unix_listener(socket_path)
        raise LocalStateError(
            LocalStateErrorCode.OPERATION_FAILED,
            "secure local-state operation failed",
        )

    monkeypatch.setattr(
        daemon_api_module,
        "pin_owned_socket_at",
        substitute_before_pin_failure,
    )
    server = UnixSocketJSONServer(socket_path, lambda _request: {"ok": True})

    try:
        with pytest.raises(DaemonUnavailable) as caught:
            server.start()

        replacement_identity = _socket_identity(socket_path)
        assert caught.value.code is LocalStateErrorCode.OPERATION_FAILED
        _assert_private_daemon_failure(caught.value, socket_path)
        assert replacement_listener is not None
        _assert_unix_socket_connects(socket_path)

        server.close()

        assert _socket_identity(socket_path) == replacement_identity
        _assert_unix_socket_connects(socket_path)
    finally:
        server.close()
        if replacement_listener is not None:
            replacement_listener.close()
        socket_path.unlink(missing_ok=True)


@_UNIX_SOCKET_TEST
def test_startup_cleanup_failure_preserves_primary_error_and_pending_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import tendwire.daemon_api as daemon_api_module

    socket_path = tmp_path / "pending-cleanup.sock"
    original_unlink = daemon_api_module.unlink_verified_socket_at
    unlink_calls = 0

    def fail_permissions(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("primary startup failure")

    def fail_unlink_once(parent_fd: int, leaf: str, expected: Any) -> None:
        nonlocal unlink_calls
        unlink_calls += 1
        if unlink_calls == 1:
            raise LocalStateError(
                LocalStateErrorCode.OPERATION_FAILED,
                "secure local-state operation failed",
            )
        original_unlink(parent_fd, leaf, expected)

    monkeypatch.setattr(
        daemon_api_module,
        "enforce_bound_socket_permissions_at",
        fail_permissions,
    )
    monkeypatch.setattr(
        daemon_api_module,
        "unlink_verified_socket_at",
        fail_unlink_once,
    )
    server = UnixSocketJSONServer(socket_path, lambda _request: {"ok": True})

    with pytest.raises(RuntimeError, match="primary startup failure"):
        server.start()

    assert os.path.lexists(socket_path)
    with pytest.raises(DaemonUnavailable, match="cleanup is pending"):
        server.start()
    server.close()
    assert unlink_calls == 2
    assert not os.path.lexists(socket_path)


@_UNIX_SOCKET_TEST
def test_daemon_rejects_group_sharing_on_implicit_private_parent_before_mutation(
    tmp_path: Path,
) -> None:
    import grp

    data_dir = tmp_path / "default-state"
    group_name = grp.getgrgid(os.getegid()).gr_name
    daemon = TendwireDaemon(
        Config(
            host_id="daemon-host",
            data_dir=data_dir,
            db_path=tmp_path / "daemon.db",
            socket_group=group_name,
        ),
        hooks=DaemonHooks(init_store=lambda _path: None),
    )

    with pytest.raises(DaemonUnavailable) as caught:
        daemon.start()

    assert not data_dir.exists()
    _assert_private_daemon_failure(caught.value, data_dir)


@_UNIX_SOCKET_TEST
def test_nonmember_socket_group_is_rejected_before_parent_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import grp
    from types import SimpleNamespace

    memberships = {os.getegid(), *os.getgroups()}
    nonmember_gid = max(memberships, default=0) + 100_000
    group_name = "tendwire-nonmember-group"
    original_getgrnam = grp.getgrnam

    def fake_getgrnam(name: str) -> object:
        if name == group_name:
            return SimpleNamespace(gr_gid=nonmember_gid)
        return original_getgrnam(name)

    monkeypatch.setattr(grp, "getgrnam", fake_getgrnam)
    missing_parent = tmp_path / "missing-shared-parent"
    server = UnixSocketJSONServer(
        missing_parent / "daemon.sock",
        lambda _request: {"ok": True},
        socket_group=group_name,
        prepare_parent=False,
    )

    with pytest.raises(DaemonUnavailable) as caught:
        server.start()

    assert not missing_parent.exists()
    _assert_private_daemon_failure(caught.value, missing_parent)


@_UNIX_SOCKET_TEST
def test_unix_socket_server_start_is_idempotent(tmp_path: Path) -> None:
    socket_path = tmp_path / "idempotent.sock"
    server = UnixSocketJSONServer(
        socket_path,
        lambda _request: {"ok": True},
        socket_group=None,
        prepare_parent=False,
    )

    try:
        server.start()
        first_identity = _socket_identity(socket_path)
        server.start()

        assert server.listening is True
        assert _socket_identity(socket_path) == first_identity
        _assert_unix_socket_connects(socket_path)
    finally:
        server.close()
        socket_path.unlink(missing_ok=True)

    assert not os.path.lexists(socket_path)




@_UNIX_SOCKET_TEST
def test_concurrent_startup_cannot_unlink_socket_before_first_listener_is_ready(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import tendwire.daemon_api as daemon_api_module

    socket_path = tmp_path / "concurrent.sock"
    permission_started = threading.Event()
    allow_permission = threading.Event()
    first_call_lock = threading.Lock()
    first_call = True
    original_enforce = daemon_api_module.enforce_bound_socket_permissions_at

    def delayed_enforce(*args: Any, **kwargs: Any) -> Any:
        nonlocal first_call
        with first_call_lock:
            should_wait = first_call
            first_call = False
        if should_wait:
            permission_started.set()
            assert allow_permission.wait(timeout=2)
        return original_enforce(*args, **kwargs)

    monkeypatch.setattr(
        daemon_api_module,
        "enforce_bound_socket_permissions_at",
        delayed_enforce,
    )
    first = UnixSocketJSONServer(socket_path, lambda _request: {"ok": True})
    second = UnixSocketJSONServer(socket_path, lambda _request: {"ok": True})
    first_errors: list[Exception] = []
    second_errors: list[Exception] = []

    def start_server(
        server: UnixSocketJSONServer,
        errors: list[Exception],
    ) -> None:
        try:
            server.start()
        except Exception as exc:
            errors.append(exc)

    first_thread = threading.Thread(
        target=start_server,
        args=(first, first_errors),
    )
    second_thread = threading.Thread(
        target=start_server,
        args=(second, second_errors),
    )
    try:
        first_thread.start()
        assert permission_started.wait(timeout=2)
        bound_identity = _socket_identity(socket_path)
        second_thread.start()
        time.sleep(0.05)

        assert second_thread.is_alive()
        assert _socket_identity(socket_path) == bound_identity
        allow_permission.set()
        first_thread.join(timeout=2)
        second_thread.join(timeout=2)

        assert not first_thread.is_alive()
        assert not second_thread.is_alive()
        assert first_errors == []
        assert len(second_errors) == 1
        assert isinstance(second_errors[0], DaemonUnavailable)
        assert str(second_errors[0]) == "daemon socket is already active"
        assert first.listening is True
        assert _socket_identity(socket_path) == bound_identity
        _assert_unix_socket_connects(socket_path)
    finally:
        allow_permission.set()
        first.close()
        second.close()
        first_thread.join(timeout=2)
        second_thread.join(timeout=2)
        socket_path.unlink(missing_ok=True)


@_UNIX_SOCKET_TEST
def test_unix_socket_server_replaces_owned_stale_socket_only_after_connection_refused(
    tmp_path: Path,
) -> None:
    socket_path = tmp_path / "stale.sock"
    stale_listener = _bind_unix_listener(socket_path)
    stale_listener.close()

    with pytest.raises(OSError) as refused:
        _assert_unix_socket_connects(socket_path)
    assert refused.value.errno == errno.ECONNREFUSED

    server = UnixSocketJSONServer(
        socket_path,
        lambda _request: {"ok": True, "result": {"pong": True}},
        socket_group=None,
        prepare_parent=False,
    )
    thread: threading.Thread | None = None
    try:
        server.start()
        thread = threading.Thread(target=server.serve_forever)
        thread.start()

        response = DaemonAPIClient(
            socket_path,
            socket_group=None,
            timeout_seconds=1,
        ).request("ping")
        assert response == {"ok": True, "result": {"pong": True}}
    finally:
        server.close()
        if thread is not None:
            thread.join(timeout=2)
        socket_path.unlink(missing_ok=True)

    assert thread is not None
    assert not thread.is_alive()
    assert not os.path.lexists(socket_path)


@_UNIX_SOCKET_TEST
def test_client_treats_disconnect_after_request_delivery_as_uncertain_protocol(
    tmp_path: Path,
) -> None:
    socket_path = tmp_path / "disconnect-after-request.sock"
    listener = _bind_unix_listener(socket_path)
    os.chmod(socket_path, 0o600)
    request_received = threading.Event()

    def receive_then_disconnect() -> None:
        connection, _address = listener.accept()
        with connection:
            frame = bytearray()
            while b"\n" not in frame:
                chunk = connection.recv(4096)
                if not chunk:
                    break
                frame.extend(chunk)
            request_received.set()

    thread = threading.Thread(target=receive_then_disconnect)
    thread.start()
    try:
        with pytest.raises(DaemonProtocolError) as caught:
            DaemonAPIClient(socket_path, timeout_seconds=1).request("ping")

        assert request_received.wait(timeout=1)
        assert str(caught.value) == "empty daemon response"
        _assert_private_daemon_failure(caught.value, socket_path)
    finally:
        listener.close()
        thread.join(timeout=2)
        socket_path.unlink(missing_ok=True)

    assert not thread.is_alive()


@_UNIX_SOCKET_TEST
def test_client_transport_phase_is_false_before_send_and_true_after_send(
    tmp_path: Path,
) -> None:
    missing_socket = tmp_path / "missing.sock"
    with pytest.raises(DaemonUnavailable) as pre_send:
        DaemonAPIClient(missing_socket, timeout_seconds=0.1).request("ping")

    assert pre_send.value.request_started is False

    socket_path = tmp_path / "timeout-after-send.sock"
    listener = _bind_unix_listener(socket_path)
    os.chmod(socket_path, 0o600)
    request_received = threading.Event()
    release = threading.Event()

    def hold_after_request() -> None:
        connection, _address = listener.accept()
        with connection:
            _read_request_frame(connection)
            request_received.set()
            release.wait(timeout=2)

    thread = threading.Thread(target=hold_after_request)
    thread.start()
    try:
        with pytest.raises(DaemonUnavailable) as post_send:
            DaemonAPIClient(socket_path, timeout_seconds=0.05).request("ping")

        assert request_received.wait(timeout=1)
        assert post_send.value.timed_out is True
        assert post_send.value.request_started is True
    finally:
        release.set()
        listener.close()
        thread.join(timeout=2)
        socket_path.unlink(missing_ok=True)

    assert not thread.is_alive()


@_UNIX_SOCKET_TEST
@pytest.mark.parametrize(
    ("response_frame", "max_response_bytes", "expected_message"),
    [
        (b"", MAX_RESPONSE_BYTES, "empty daemon response"),
        (b"not-json\n", MAX_RESPONSE_BYTES, "invalid daemon response JSON"),
        (b"[]\n", MAX_RESPONSE_BYTES, "daemon response must be a JSON object"),
        (b'{"ok":true,"padding":"' + (b"x" * 128) + b'"}\n', 32, "maximum frame size"),
    ],
    ids=["empty", "malformed", "non-object", "oversized"],
)
def test_client_protocol_distrust_after_send_records_started_phase(
    tmp_path: Path,
    response_frame: bytes,
    max_response_bytes: int,
    expected_message: str,
) -> None:
    socket_path = tmp_path / "protocol-distrust.sock"
    listener = _bind_unix_listener(socket_path)
    os.chmod(socket_path, 0o600)

    def serve_untrusted_response() -> None:
        connection, _address = listener.accept()
        with connection:
            _read_request_frame(connection)
            if response_frame:
                connection.sendall(response_frame)

    thread = threading.Thread(target=serve_untrusted_response)
    thread.start()
    try:
        with pytest.raises(DaemonProtocolError) as caught:
            DaemonAPIClient(
                socket_path,
                timeout_seconds=1,
                max_response_bytes=max_response_bytes,
            ).request("ping")

        assert expected_message in str(caught.value)
        assert caught.value.request_started is True
    finally:
        listener.close()
        thread.join(timeout=2)
        socket_path.unlink(missing_ok=True)

    assert not thread.is_alive()


@_UNIX_SOCKET_TEST
def test_unix_socket_server_rejects_and_preserves_active_listener(tmp_path: Path) -> None:
    socket_path = tmp_path / "active.sock"
    active_listener = _bind_unix_listener(socket_path)
    active_identity = _socket_identity(socket_path)
    server = UnixSocketJSONServer(
        socket_path,
        lambda _request: {"ok": True},
        socket_group=None,
        prepare_parent=False,
    )

    try:
        with pytest.raises(DaemonUnavailable) as caught:
            server.start()

        _assert_private_daemon_failure(caught.value, socket_path)
        assert _socket_identity(socket_path) == active_identity
        _assert_unix_socket_connects(socket_path)
    finally:
        server.close()
        active_listener.close()
        socket_path.unlink(missing_ok=True)


def test_peer_validation_failure_is_typed_and_fails_startup_guard_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import tendwire.daemon_api as daemon_api_module

    class BrokenPeerSocket:
        def getsockopt(self, *_args: Any) -> bytes:
            raise OSError("peer credentials unavailable")

    with pytest.raises(DaemonUnavailable) as validation:
        daemon_api_module._validate_connected_peer(BrokenPeerSocket(), os.geteuid())
    assert validation.value.code is LocalStateErrorCode.PEER_VALIDATION_FAILED

    def fail_peer_validation(
        _client: DaemonAPIClient,
        _method: str,
        _params: Any = None,
    ) -> dict[str, Any]:
        raise DaemonUnavailable(
            "daemon peer validation failed",
            code=LocalStateErrorCode.PEER_VALIDATION_FAILED,
        )

    monkeypatch.setattr(DaemonAPIClient, "request", fail_peer_validation)
    with pytest.raises(DaemonUnavailable) as guarded:
        ensure_daemon_socket_not_active(tmp_path / "peer-validation.sock")

    assert guarded.value.code is LocalStateErrorCode.PEER_VALIDATION_FAILED
    assert "holder version is unknown" in str(guarded.value)
    assert f"refusing to start tendwire {__version__}" in str(guarded.value)


@_UNIX_SOCKET_TEST
def test_daemon_active_socket_fails_before_store_or_backend_work(tmp_path: Path) -> None:
    socket_path = tmp_path / "fail-fast-active.sock"
    active = UnixSocketJSONServer(
        socket_path,
        lambda _request: {
            "ok": True,
            "result": {
                "pong": True,
                "version": __version__,
                "pid": os.getpid(),
            },
        },
    )
    active_thread = threading.Thread(target=active.serve_forever)
    active_thread.start()
    deadline = time.monotonic() + 2
    while not active.listening and time.monotonic() < deadline:
        time.sleep(0.005)

    calls: list[str] = []

    def forbidden_store(_path: Path) -> None:
        calls.append("init_store")

    daemon = TendwireDaemon(
        Config(
            host_id="fail-fast-host",
            data_dir=tmp_path,
            db_path=tmp_path / "fail-fast.db",
            socket_path=socket_path,
        ),
        hooks=DaemonHooks(init_store=forbidden_store),
    )
    try:
        with pytest.raises(DaemonUnavailable) as caught:
            daemon.start()

        assert str(caught.value) == (
            "daemon socket is already active: "
            f"holder is tendwire {__version__} (PID {os.getpid()}); "
            f"refusing to start tendwire {__version__}"
        )
        assert calls == []
        _assert_unix_socket_connects(socket_path)
    finally:
        daemon.stop()
        active.close()
        active_thread.join(timeout=2)
        socket_path.unlink(missing_ok=True)

    assert not active_thread.is_alive()


@_UNIX_SOCKET_TEST
def test_blocked_periodic_callback_does_not_block_stop(tmp_path: Path) -> None:
    callback_started = threading.Event()
    release_callback = threading.Event()
    stop_event = threading.Event()

    def blocked_periodic_callback() -> None:
        callback_started.set()
        release_callback.wait(timeout=2)

    server = UnixSocketJSONServer(
        tmp_path / "blocked-periodic.sock",
        lambda _request: {"ok": True},
        stop_event=stop_event,
        accept_timeout_seconds=0.01,
        periodic_callback=blocked_periodic_callback,
        periodic_interval_seconds=0.01,
    )
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        assert callback_started.wait(timeout=1)
        started = time.monotonic()
        stop_event.set()
        thread.join(timeout=0.5)
        assert not thread.is_alive()
        assert time.monotonic() - started < 0.5
    finally:
        stop_event.set()
        release_callback.set()
        server.close()
        thread.join(timeout=2)

    deadline = time.monotonic() + 1
    while (
        any(item.name.startswith("tendwire-daemon-periodic") for item in threading.enumerate())
        and time.monotonic() < deadline
    ):
        time.sleep(0.005)
    assert not any(
        item.name.startswith("tendwire-daemon-periodic")
        for item in threading.enumerate()
    )


@_UNIX_SOCKET_TEST
@pytest.mark.parametrize("entry_kind", ["regular-file", "symlink"])
def test_unix_socket_server_rejects_wrong_type_without_mutating_entry_or_target(
    tmp_path: Path,
    entry_kind: str,
) -> None:
    protected_contents = b"sentinel-daemon-socket-target-contents"
    socket_path = tmp_path / "unsafe.sock"
    if entry_kind == "regular-file":
        protected_path = socket_path
        protected_path.write_bytes(protected_contents)
    else:
        protected_path = tmp_path / "protected-target"
        protected_path.write_bytes(protected_contents)
        socket_path.symlink_to(protected_path)
    original_identity = _socket_identity(socket_path)
    server = UnixSocketJSONServer(
        socket_path,
        lambda _request: {"ok": True},
        socket_group=None,
        prepare_parent=False,
    )

    try:
        with pytest.raises(DaemonUnavailable) as caught:
            server.start()

        _assert_private_daemon_failure(
            caught.value,
            socket_path,
            protected_path,
            forbidden=(protected_contents.decode("ascii"),),
        )
        assert _socket_identity(socket_path) == original_identity
        assert protected_path.read_bytes() == protected_contents
        if entry_kind == "symlink":
            assert socket_path.is_symlink()
    finally:
        server.close()


@_UNIX_SOCKET_TEST
def test_unix_socket_server_rejects_wrong_owner_without_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    socket_path = tmp_path / "wrong-owner.sock"
    stale_listener = _bind_unix_listener(socket_path)
    stale_listener.close()
    original_identity = _socket_identity(socket_path)
    actual_euid = os.geteuid()
    monkeypatch.setattr("tendwire.local_state.os.geteuid", lambda: actual_euid + 1)
    server = UnixSocketJSONServer(
        socket_path,
        lambda _request: {"ok": True},
        socket_group=None,
        prepare_parent=False,
    )

    try:
        with pytest.raises(DaemonUnavailable) as caught:
            server.start()

        _assert_private_daemon_failure(caught.value, socket_path)
        assert _socket_identity(socket_path) == original_identity
    finally:
        server.close()
        socket_path.unlink(missing_ok=True)


@_UNIX_SOCKET_TEST
def test_unix_socket_server_preserves_stale_socket_when_probe_error_is_ambiguous(
    tmp_path: Path,
    monkeypatch,
) -> None:
    socket_path = tmp_path / "ambiguous.sock"
    stale_listener = _bind_unix_listener(socket_path)
    stale_listener.close()
    stale_identity = _socket_identity(socket_path)
    original_connect = socket.socket.connect

    def ambiguous_connect(connection: socket.socket, address: Any) -> Any:
        if str(address).endswith(f"/{socket_path.name}"):
            raise OSError(
                errno.EACCES,
                "sentinel ambiguous socket probe",
                os.fspath(socket_path),
            )
        return original_connect(connection, address)

    monkeypatch.setattr(socket.socket, "connect", ambiguous_connect)
    server = UnixSocketJSONServer(
        socket_path,
        lambda _request: {"ok": True},
        socket_group=None,
        prepare_parent=False,
    )

    try:
        with pytest.raises(DaemonUnavailable) as caught:
            server.start()

        _assert_private_daemon_failure(caught.value, socket_path)
        assert _socket_identity(socket_path) == stale_identity
    finally:
        server.close()
        socket_path.unlink(missing_ok=True)


@_UNIX_SOCKET_TEST
def test_unix_socket_server_refuses_substitution_before_stale_unlink(
    tmp_path: Path,
    monkeypatch,
) -> None:
    socket_path = tmp_path / "stale-substitution.sock"
    stale_listener = _bind_unix_listener(socket_path)
    stale_listener.close()
    stale_identity = _socket_identity(socket_path)
    stale_fd = os.open(socket_path, os.O_PATH | os.O_NOFOLLOW)
    original_connect = socket.socket.connect
    replacement_listener: socket.socket | None = None

    def substitute_after_refusal(connection: socket.socket, address: Any) -> Any:
        nonlocal replacement_listener
        if not str(address).endswith(f"/{socket_path.name}"):
            return original_connect(connection, address)
        try:
            return original_connect(connection, address)
        except OSError as exc:
            if exc.errno != errno.ECONNREFUSED:
                raise
            socket_path.unlink()
            replacement_listener = _bind_unix_listener(socket_path)
            raise

    monkeypatch.setattr(socket.socket, "connect", substitute_after_refusal)
    server = UnixSocketJSONServer(
        socket_path,
        lambda _request: {"ok": True},
        socket_group=None,
        prepare_parent=False,
    )

    try:
        with pytest.raises(DaemonUnavailable) as caught:
            server.start()

        _assert_private_daemon_failure(caught.value, socket_path)
        assert replacement_listener is not None
        replacement_identity = _socket_identity(socket_path)
        assert replacement_identity != stale_identity
        _assert_unix_socket_connects(socket_path)
        assert _socket_identity(socket_path) == replacement_identity
    finally:
        server.close()
        if replacement_listener is not None:
            replacement_listener.close()
        os.close(stale_fd)
        socket_path.unlink(missing_ok=True)


@_UNIX_SOCKET_TEST
def test_unix_socket_server_close_preserves_substituted_socket(tmp_path: Path) -> None:
    socket_path = tmp_path / "close-substitution.sock"
    server = UnixSocketJSONServer(
        socket_path,
        lambda _request: {"ok": True},
        socket_group=None,
        prepare_parent=False,
    )
    replacement_listener: socket.socket | None = None

    try:
        server.start()
        original_identity = _socket_identity(socket_path)
        socket_path.unlink()
        replacement_listener = _bind_unix_listener(socket_path)
        replacement_identity = _socket_identity(socket_path)
        assert replacement_identity != original_identity

        server.close()

        assert _socket_identity(socket_path) == replacement_identity
        _assert_unix_socket_connects(socket_path)
    finally:
        server.close()
        if replacement_listener is not None:
            replacement_listener.close()
        socket_path.unlink(missing_ok=True)


@_UNIX_SOCKET_TEST
def test_daemon_store_startup_failure_never_publishes_socket(tmp_path: Path) -> None:
    socket_path = tmp_path / "init-store.sock"

    def assert_unpublished() -> None:
        assert not os.path.lexists(socket_path)

    def initialize_store(path: Path) -> None:
        assert_unpublished()
        del path
        raise RuntimeError("sentinel startup failure")

    config = Config(
        host_id="daemon-host",
        data_dir=tmp_path,
        db_path=tmp_path / "init-store.db",
        socket_path=socket_path,
    )
    daemon = TendwireDaemon(
        config,
        hooks=DaemonHooks(init_store=initialize_store),
    )

    try:
        with pytest.raises(RuntimeError, match="sentinel startup failure") as caught:
            daemon.start()

        _assert_private_daemon_failure(caught.value, socket_path)
        assert not os.path.lexists(socket_path)
        with pytest.raises(RuntimeError, match="cannot start after shutdown"):
            daemon.start()
        assert not os.path.lexists(socket_path)
    finally:
        daemon.stop()


@_UNIX_SOCKET_TEST
def test_daemon_starts_persists_serves_and_removes_socket(tmp_path: Path) -> None:
    db_path = tmp_path / "daemon.db"
    socket_path = tmp_path / "daemon.sock"
    config = Config(host_id="daemon-host", data_dir=tmp_path, db_path=db_path, socket_path=socket_path)

    def initialize(path: Path) -> None:
        init_store(path)
        snapshot = project_from_raw(
            config,
            workers=[{"id": "worker-1", "name": "Worker One", "status": "active"}],
            backend_health=[
                {
                    "name": "herdr",
                    "status": "healthy",
                    "outcome": "healthy_non_empty",
                    "observed_at": "2026-01-01T00:00:00+00:00",
                    "counts": {"workers": 1},
                }
            ],
        )
        save_snapshot(db_path, snapshot)

    daemon = TendwireDaemon(
        config,
        hooks=DaemonHooks(init_store=initialize),
    )
    daemon.start()
    thread = threading.Thread(target=daemon.serve_forever)
    thread.start()
    try:
        assert latest_snapshot(db_path, "daemon-host") is not None
        ping = DaemonAPIClient(socket_path).request("ping")
        snapshot_response = DaemonAPIClient(socket_path).request("snapshot.get")
        health_response = DaemonAPIClient(socket_path).request("health.get")

        assert ping["ok"] is True
        assert ping["result"]["pong"] is True
        assert ping["result"]["version"] == __version__
        assert ping["result"]["pid"] == os.getpid()
        assert snapshot_response["result"]["host_id"] == "daemon-host"
        assert snapshot_response["result"]["workers"][0]["id"] == "worker-1"
        assert health_response["result"]["store"]["status"] == "healthy"
        assert health_response["result"]["version"] == __version__
        assert health_response["result"]["pid"] == os.getpid()
    finally:
        daemon.stop()
        thread.join(timeout=2)

    assert not thread.is_alive()
    assert not socket_path.exists()


@_UNIX_SOCKET_TEST
def test_daemon_server_survives_client_disconnect_during_response(tmp_path: Path) -> None:
    socket_path = tmp_path / "daemon.sock"
    request_seen = threading.Event()
    allow_response = threading.Event()

    def dispatch(request: dict[str, Any]) -> dict[str, Any]:
        if request.get("method") == "large.response":
            request_seen.set()
            allow_response.wait(timeout=2)
            return {"ok": True, "result": {"payload": "x" * 5_000_000}}
        return {"ok": True, "result": {"pong": True}}

    server = UnixSocketJSONServer(
        socket_path,
        dispatch,
        accept_timeout_seconds=0.05,
        client_timeout_seconds=2,
    )
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        deadline = time.monotonic() + 2
        while not server.listening and time.monotonic() < deadline:
            time.sleep(0.01)
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
            conn.connect(str(socket_path))
            conn.sendall(b'{"method":"large.response"}\n')
            assert request_seen.wait(timeout=2)
        allow_response.set()

        deadline = time.monotonic() + 2
        response: dict[str, Any] | None = None
        while time.monotonic() < deadline:
            try:
                response = DaemonAPIClient(socket_path, timeout_seconds=1).request("ping")
                break
            except Exception:
                time.sleep(0.01)

        assert response is not None
        assert response["ok"] is True
        assert response["result"]["pong"] is True
        assert thread.is_alive()
    finally:
        server.close()
        thread.join(timeout=2)

    assert not thread.is_alive()


def test_daemon_bounds_oversized_response_and_keeps_serving(tmp_path: Path) -> None:
    socket_path = tmp_path / "bounded-response.sock"

    def dispatch(request: dict[str, Any]) -> dict[str, Any]:
        if request.get("method") == "oversized":
            # Numeric JSON avoids making the guard test depend on text-redaction cost.
            return {
                "ok": True,
                "result": {"items": list(range(250_000))},
            }
        return {"ok": True, "result": {"pong": True}}

    server = UnixSocketJSONServer(
        socket_path,
        dispatch,
        accept_timeout_seconds=0.05,
    )
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        deadline = time.monotonic() + 2
        while not server.listening and time.monotonic() < deadline:
            time.sleep(0.01)
        oversized = DaemonAPIClient(socket_path, timeout_seconds=10).request("oversized")
        ping = DaemonAPIClient(socket_path, timeout_seconds=2).request("ping")

        assert oversized["ok"] is False
        assert oversized["error"]["code"] == "response_too_large"
        assert oversized["error"]["details"] == {"max_response_bytes": MAX_RESPONSE_BYTES}
        assert ping["ok"] is True
        assert ping["result"]["pong"] is True
        assert thread.is_alive()
    finally:
        server.close()
        thread.join(timeout=2)

    assert not thread.is_alive()


@_UNIX_SOCKET_TEST
def test_daemon_request_executor_enforces_worker_and_admission_bounds_and_recovers(
    tmp_path: Path,
) -> None:
    socket_path = tmp_path / "bounded-concurrency.sock"
    release = threading.Event()
    eight_running = threading.Event()
    state_lock = threading.Lock()
    active = 0
    maximum_active = 0
    dispatched = 0

    def dispatch(request: dict[str, Any]) -> dict[str, Any]:
        nonlocal active, maximum_active, dispatched
        if request.get("method") != "block":
            return {"ok": True, "result": {"pong": True}}
        with state_lock:
            active += 1
            dispatched += 1
            maximum_active = max(maximum_active, active)
            if active == 8:
                eight_running.set()
        try:
            assert release.wait(timeout=10)
            return {"ok": True, "result": {"accepted": True}}
        finally:
            with state_lock:
                active -= 1

    server = UnixSocketJSONServer(
        socket_path,
        dispatch,
        accept_timeout_seconds=0.01,
        client_timeout_seconds=10,
    )
    server_thread = threading.Thread(target=server.serve_forever)
    baseline_executor_threads = {
        thread.ident
        for thread in threading.enumerate()
        if thread.name.startswith("tendwire-daemon-api")
    }
    server_thread.start()
    results: list[dict[str, Any]] = []
    failures: list[BaseException] = []
    result_lock = threading.Lock()

    def request_block() -> None:
        try:
            response = DaemonAPIClient(
                socket_path,
                timeout_seconds=10,
            ).request("block")
            with result_lock:
                results.append(response)
        except BaseException as exc:  # noqa: BLE001
            with result_lock:
                failures.append(exc)

    admitted_clients = [threading.Thread(target=request_block) for _index in range(32)]
    overflow_clients = [threading.Thread(target=request_block) for _index in range(8)]
    clients = admitted_clients + overflow_clients
    try:
        deadline = time.monotonic() + 2
        while not server.listening and time.monotonic() < deadline:
            time.sleep(0.005)
        assert server.listening is True
        for client in admitted_clients:
            client.start()
        assert eight_running.wait(timeout=3)
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            with server._tracking_lock:
                admitted = len(server._futures)
            if admitted == 32:
                break
            time.sleep(0.005)
        assert admitted == 32
        for client in overflow_clients:
            client.start()

        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            with result_lock:
                busy_count = sum(
                    result.get("error", {}).get("code") == "server_busy"
                    for result in results
                    if isinstance(result.get("error"), dict)
                )
            if busy_count == 8:
                break
            time.sleep(0.005)

        assert busy_count == 8
        with state_lock:
            assert active == 8
            assert maximum_active == 8
        busy_results = [
            result
            for result in results
            if isinstance(result.get("error"), dict)
            and result["error"].get("code") == "server_busy"
        ]
        assert all(
            result
            == {
                "schema_version": 1,
                "ok": False,
                "status": "error",
                "result": None,
                "error": {
                    "code": "server_busy",
                    "message": "daemon request capacity is full",
                    "details": {"retryable": True},
                },
            }
            for result in busy_results
        )
        for result in busy_results:
            _assert_no_public_json_forbidden(result)

        release.set()
        for client in clients:
            client.join(timeout=10)
        assert all(not client.is_alive() for client in clients)
        assert failures == []
        successful = [result for result in results if result.get("ok") is True]
        assert len(successful) == 32
        assert len(busy_results) == 8
        with state_lock:
            assert dispatched == 32
            assert maximum_active == 8

        recovered = DaemonAPIClient(socket_path, timeout_seconds=1).request("ping")
        assert recovered == {"ok": True, "result": {"pong": True}}
    finally:
        release.set()
        server.close()
        server_thread.join(timeout=2)
        for client in clients:
            client.join(timeout=2)

    assert not server_thread.is_alive()
    assert not os.path.lexists(socket_path)
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        remaining = {
            thread.ident
            for thread in threading.enumerate()
            if thread.name.startswith("tendwire-daemon-api")
        } - baseline_executor_threads
        if not remaining:
            break
        time.sleep(0.005)
    assert remaining == set()


@_UNIX_SOCKET_TEST
def test_blocked_handler_does_not_block_health_or_command_requests(
    tmp_path: Path,
) -> None:
    socket_path = tmp_path / "independent-workers.sock"
    blocked = threading.Event()
    release = threading.Event()

    def dispatch(request: dict[str, Any]) -> dict[str, Any]:
        method = request.get("method")
        if method == "blocked.adapter":
            blocked.set()
            assert release.wait(timeout=3)
            return {"ok": True, "result": {"released": True}}
        if method == "health.get":
            return {"ok": True, "result": {"status": "ok"}}
        if method == "command.submit":
            return {"ok": True, "result": {"status": "accepted"}}
        return {"ok": True, "result": {}}

    server = UnixSocketJSONServer(
        socket_path,
        dispatch,
        accept_timeout_seconds=0.01,
        client_timeout_seconds=3,
    )
    server_thread = threading.Thread(target=server.serve_forever)
    blocked_result: list[dict[str, Any]] = []
    blocked_client = threading.Thread(
        target=lambda: blocked_result.append(
            DaemonAPIClient(socket_path, timeout_seconds=3).request(
                "blocked.adapter"
            )
        )
    )
    server_thread.start()
    try:
        deadline = time.monotonic() + 2
        while not server.listening and time.monotonic() < deadline:
            time.sleep(0.005)
        blocked_client.start()
        assert blocked.wait(timeout=2)

        health = DaemonAPIClient(socket_path, timeout_seconds=0.5).request(
            "health.get"
        )
        command = DaemonAPIClient(socket_path, timeout_seconds=0.5).request(
            "command.submit"
        )

        assert health == {"ok": True, "result": {"status": "ok"}}
        assert command == {"ok": True, "result": {"status": "accepted"}}
        assert blocked_client.is_alive()
    finally:
        release.set()
        blocked_client.join(timeout=2)
        server.close()
        server_thread.join(timeout=2)

    assert blocked_result == [{"ok": True, "result": {"released": True}}]
    assert not blocked_client.is_alive()
    assert not server_thread.is_alive()


@_UNIX_SOCKET_TEST
def test_daemon_shutdown_is_bounded_closes_active_socket_and_reaps_executor(
    tmp_path: Path,
) -> None:
    socket_path = tmp_path / "bounded-shutdown.sock"
    handler_started = threading.Event()
    release_handler = threading.Event()

    def dispatch(_request: dict[str, Any]) -> dict[str, Any]:
        handler_started.set()
        release_handler.wait()
        return {"ok": True, "result": {"released": True}}

    server = UnixSocketJSONServer(
        socket_path,
        dispatch,
        accept_timeout_seconds=0.01,
        client_timeout_seconds=3,
        request_workers=1,
        max_in_flight_requests=2,
        shutdown_grace_seconds=0.05,
    )
    baseline_executor_threads = {
        thread.ident
        for thread in threading.enumerate()
        if thread.name.startswith("tendwire-daemon-api")
    }
    server_thread = threading.Thread(target=server.serve_forever)
    first_outcome: list[BaseException | dict[str, Any]] = []
    second_outcome: list[BaseException | dict[str, Any]] = []

    def request_into(target: list[BaseException | dict[str, Any]]) -> None:
        try:
            target.append(
                DaemonAPIClient(socket_path, timeout_seconds=3).request("blocked")
            )
        except BaseException as exc:  # noqa: BLE001
            target.append(exc)

    first_client = threading.Thread(target=request_into, args=(first_outcome,))
    second_client = threading.Thread(target=request_into, args=(second_outcome,))
    server_thread.start()
    try:
        deadline = time.monotonic() + 2
        while not server.listening and time.monotonic() < deadline:
            time.sleep(0.005)
        first_client.start()
        assert handler_started.wait(timeout=2)
        second_client.start()
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            with server._tracking_lock:
                admitted = len(server._futures)
            if admitted == 2:
                break
            time.sleep(0.005)
        assert admitted == 2

        started_at = time.monotonic()
        server.close()
        close_duration = time.monotonic() - started_at

        assert close_duration < 0.5
        blocked_workers = [
            thread
            for thread in threading.enumerate()
            if thread.name.startswith("tendwire-daemon-api")
            and thread.ident not in baseline_executor_threads
        ]
        assert blocked_workers
        assert all(thread.daemon for thread in blocked_workers)
        second_client.join(timeout=2)
        assert second_outcome == [
            {
                "schema_version": 1,
                "ok": False,
                "status": "error",
                "result": None,
                "error": {
                    "code": "daemon_stopping",
                    "message": "daemon is stopping",
                    "details": {"retryable": True},
                },
            }
        ]
        _assert_no_public_json_forbidden(second_outcome[0])
    finally:
        release_handler.set()
        first_client.join(timeout=2)
        second_client.join(timeout=2)
        server.close()
        server_thread.join(timeout=2)

    assert len(first_outcome) == 1
    assert isinstance(first_outcome[0], DaemonProtocolError)
    assert first_outcome[0].request_started is True
    assert not first_client.is_alive()
    assert not second_client.is_alive()
    assert not server_thread.is_alive()
    assert not os.path.lexists(socket_path)
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        remaining = {
            thread.ident
            for thread in threading.enumerate()
            if thread.name.startswith("tendwire-daemon-api")
        } - baseline_executor_threads
        if not remaining:
            break
        time.sleep(0.005)
    assert remaining == set()


def test_daemon_command_submit_rejects_blank_request_id_before_mutation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "invalid-request-id.db"
    config = Config(
        host_id="cmd-host",
        data_dir=tmp_path,
        db_path=db_path,
    )
    init_store(db_path)
    calls: list[str] = []

    daemon = TendwireDaemon(config)
    request = {
        "schema_version": 1,
        "action": "send_instruction",
        "request_id": "   \t",
        "dry_run": False,
        "target": {"worker_id": "w-1"},
        "instruction": {"text": "hello"},
    }

    direct = daemon.submit_command(request)
    api = TendwireDaemonAPI(
        get_snapshot=_public_snapshot,
        get_health=lambda: {"schema_version": 1, "status": "ok"},
        submit_command=daemon.submit_command,
    )
    response = api.dispatch({"method": "command.submit", "params": request})

    assert isinstance(direct, CommandEnvelope)
    assert direct.status == STATUS_INVALID_REQUEST
    assert response["ok"] is True
    assert response["result"]["status"] == STATUS_INVALID_REQUEST
    assert calls == []
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM commands").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0

def test_cli_snapshot_requires_daemon_when_configured_socket_is_absent(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    socket_path = tmp_path / "absent.sock"
    data_dir = tmp_path / "private-state"
    data_dir.mkdir(mode=0o700)
    monkeypatch.setenv("TENDWIRE_DATA_DIR", os.fspath(data_dir))
    monkeypatch.delenv("TENDWIRE_DB_PATH", raising=False)

    code = main(
        [
            "--host-id",
            "fallback-host",
            "--socket-path",
            str(socket_path),
            "snapshot",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 1
    assert captured.err == ""
    assert payload["ok"] is False
    assert payload["status"] == "daemon_unavailable"
    assert not (data_dir / "tendwire.db").exists()


def test_cli_command_falls_back_when_configured_socket_is_stale(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    socket_path = tmp_path / "stale.sock"
    stale = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    stale.bind(str(socket_path))
    stale.close()

    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps({"schema_version": 1, "action": "noop"})))

    code = main(["--socket-path", str(socket_path), "command", "--json"])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert payload["ok"] is True
    assert payload["status"] == "noop"


def _current_socket_group() -> tuple[str, int]:
    import grp

    group_id = os.getegid()
    try:
        return grp.getgrgid(group_id).gr_name, group_id
    except KeyError:
        pytest.skip("effective group has no local name")


def _prepare_socket_test_parent(
    parent: Path,
    *,
    group_id: int | None,
) -> None:
    parent.mkdir(parents=True)
    if group_id is None:
        os.chmod(parent, 0o700)
    else:
        os.chown(parent, -1, group_id)
        os.chmod(parent, 0o710)


def _prepare_socket_test_endpoint(
    path: Path,
    *,
    group_id: int | None,
) -> socket.socket:
    listener = _bind_unix_listener(path)
    if group_id is None:
        os.chmod(path, 0o600)
    else:
        os.chown(path, -1, group_id)
        os.chmod(path, 0o660)
    return listener


def _read_request_frame(connection: socket.socket) -> bytes:
    frame = bytearray()
    while b"\n" not in frame:
        chunk = connection.recv(4096)
        if not chunk:
            break
        frame.extend(chunk)
    return bytes(frame)


def _configured_path_variant(
    path: Path,
    root: Path,
    variant: str,
    monkeypatch: pytest.MonkeyPatch,
) -> Path:
    if variant == "absolute":
        return path
    monkeypatch.chdir(root)
    return path.relative_to(root)


@_UNIX_SOCKET_TEST
@pytest.mark.parametrize(
    "server_mode",
    ["default-private", "explicit-private", "group"],
)
@pytest.mark.parametrize("path_variant", ["absolute", "relative"])
def test_socket_server_rejects_intermediate_symlink_without_touching_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    server_mode: str,
    path_variant: str,
) -> None:
    group_name: str | None = None
    group_id: int | None = None
    if server_mode == "group":
        group_name, group_id = _current_socket_group()
    target_parent = tmp_path / "protected-target" / "socket-parent"
    _prepare_socket_test_parent(target_parent, group_id=group_id)
    target_socket = target_parent / "daemon.sock"
    target_listener = _prepare_socket_test_endpoint(
        target_socket,
        group_id=group_id,
    )
    target_identity = _socket_identity(target_socket)
    target_parent_mode = _socket_mode(target_parent)
    configured_root = tmp_path / "configured-root"
    configured_root.mkdir()
    intermediate = configured_root / "intermediate"
    intermediate.symlink_to(target_parent.parent, target_is_directory=True)
    absolute_configured = intermediate / target_parent.name / target_socket.name
    configured = _configured_path_variant(
        absolute_configured,
        tmp_path,
        path_variant,
        monkeypatch,
    )
    server = UnixSocketJSONServer(
        configured,
        lambda _request: {"ok": True},
        socket_group=group_name,
        prepare_parent=server_mode == "default-private",
    )
    assert server.socket_path == configured

    try:
        with pytest.raises(DaemonUnavailable) as caught:
            server.start()

        _assert_private_daemon_failure(
            caught.value,
            configured,
            target_parent,
            target_socket,
        )
        assert intermediate.is_symlink()
        assert _socket_mode(target_parent) == target_parent_mode
        assert _socket_identity(target_socket) == target_identity
        _assert_unix_socket_connects(target_socket)
    finally:
        server.close()
        target_listener.close()
        target_socket.unlink(missing_ok=True)


@_UNIX_SOCKET_TEST
@pytest.mark.parametrize("client_mode", ["private", "group"])
@pytest.mark.parametrize("path_variant", ["absolute", "relative"])
def test_socket_client_rejects_intermediate_symlink_without_touching_listener(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    client_mode: str,
    path_variant: str,
) -> None:
    group_name: str | None = None
    group_id: int | None = None
    if client_mode == "group":
        group_name, group_id = _current_socket_group()
    target_parent = tmp_path / "ct" / "p"
    _prepare_socket_test_parent(target_parent, group_id=group_id)
    target_socket = target_parent / "s"
    target_listener = _prepare_socket_test_endpoint(
        target_socket,
        group_id=group_id,
    )
    target_identity = _socket_identity(target_socket)
    configured_root = tmp_path / "cc"
    configured_root.mkdir()
    intermediate = configured_root / "i"
    intermediate.symlink_to(target_parent.parent, target_is_directory=True)
    absolute_configured = intermediate / target_parent.name / target_socket.name
    configured = _configured_path_variant(
        absolute_configured,
        tmp_path,
        path_variant,
        monkeypatch,
    )

    client = DaemonAPIClient(
        configured,
        socket_group=group_name,
        timeout_seconds=0.2,
    )
    assert client.socket_path == configured

    try:
        with pytest.raises(DaemonUnavailable) as caught:
            client.request("ping")

        _assert_private_daemon_failure(
            caught.value,
            configured,
            target_parent,
            target_socket,
        )
        assert intermediate.is_symlink()
        assert _socket_identity(target_socket) == target_identity
        _assert_unix_socket_connects(target_socket)
    finally:
        target_listener.close()
        target_socket.unlink(missing_ok=True)


@_UNIX_SOCKET_TEST
@pytest.mark.parametrize("socket_mode", ["private", "group"])
@pytest.mark.parametrize("path_variant", ["absolute", "relative"])
def test_socket_server_keeps_resolved_parent_pinned_when_ancestor_is_replaced(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    socket_mode: str,
    path_variant: str,
) -> None:
    group_name: str | None = None
    group_id: int | None = None
    if socket_mode == "group":
        group_name, group_id = _current_socket_group()
    configured_parent = tmp_path / "server-configured-parent"
    _prepare_socket_test_parent(configured_parent, group_id=group_id)
    absolute_socket = configured_parent / "daemon.sock"
    configured_socket = _configured_path_variant(
        absolute_socket,
        tmp_path,
        path_variant,
        monkeypatch,
    )
    pinned_parent = tmp_path / "server-pinned-parent"
    original_bind = socket.socket.bind
    replacement_listener: socket.socket | None = None
    replacement_identity: tuple[int, int] | None = None
    substituted = False

    def substitute_before_bind(connection: socket.socket, address: Any) -> Any:
        nonlocal replacement_listener, replacement_identity, substituted
        if (
            not substituted
            and str(address).startswith("/proc/self/fd/")
            and str(address).endswith(f"/{absolute_socket.name}")
        ):
            substituted = True
            configured_parent.rename(pinned_parent)
            _prepare_socket_test_parent(configured_parent, group_id=group_id)
            replacement_listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            original_bind(replacement_listener, os.fspath(absolute_socket))
            replacement_listener.listen()
            if group_id is None:
                os.chmod(absolute_socket, 0o600)
            else:
                os.chown(absolute_socket, -1, group_id)
                os.chmod(absolute_socket, 0o660)
            replacement_identity = _socket_identity(absolute_socket)
        return original_bind(connection, address)

    monkeypatch.setattr(socket.socket, "bind", substitute_before_bind)
    server = UnixSocketJSONServer(
        configured_socket,
        lambda _request: {"ok": True},
        socket_group=group_name,
        prepare_parent=False,
    )

    try:
        server.start()
        pinned_socket = pinned_parent / absolute_socket.name
        assert substituted is True
        assert replacement_listener is not None
        assert replacement_identity is not None
        assert _socket_identity(absolute_socket) == replacement_identity
        _assert_unix_socket_connects(absolute_socket)
        _assert_unix_socket_connects(pinned_socket)

        server.close()

        assert not os.path.lexists(pinned_socket)
        assert _socket_identity(absolute_socket) == replacement_identity
        _assert_unix_socket_connects(absolute_socket)
    finally:
        server.close()
        if replacement_listener is not None:
            replacement_listener.close()
        absolute_socket.unlink(missing_ok=True)


@_UNIX_SOCKET_TEST
@pytest.mark.parametrize("socket_mode", ["private", "group"])
@pytest.mark.parametrize("path_variant", ["absolute", "relative"])
def test_socket_client_keeps_resolved_parent_pinned_when_ancestor_is_replaced(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    socket_mode: str,
    path_variant: str,
) -> None:
    group_name: str | None = None
    group_id: int | None = None
    if socket_mode == "group":
        group_name, group_id = _current_socket_group()
    configured_parent = tmp_path / "client-configured-parent"
    _prepare_socket_test_parent(configured_parent, group_id=group_id)
    absolute_socket = configured_parent / "daemon.sock"
    original_listener = _prepare_socket_test_endpoint(
        absolute_socket,
        group_id=group_id,
    )
    configured_socket = _configured_path_variant(
        absolute_socket,
        tmp_path,
        path_variant,
        monkeypatch,
    )
    pinned_parent = tmp_path / "client-pinned-parent"
    original_connect = socket.socket.connect
    replacement_listener: socket.socket | None = None
    replacement_identity: tuple[int, int] | None = None
    substituted = False

    def serve_original() -> None:
        connection, _address = original_listener.accept()
        with connection:
            _read_request_frame(connection)
            connection.sendall(b'{"ok":true,"result":{"source":"original"}}\n')

    def substitute_before_connect(connection: socket.socket, address: Any) -> Any:
        nonlocal replacement_listener, replacement_identity, substituted
        if (
            not substituted
            and str(address).startswith("/proc/self/fd/")
            and str(address).endswith(f"/{absolute_socket.name}")
        ):
            substituted = True
            configured_parent.rename(pinned_parent)
            _prepare_socket_test_parent(configured_parent, group_id=group_id)
            replacement_listener = _prepare_socket_test_endpoint(
                absolute_socket,
                group_id=group_id,
            )
            replacement_identity = _socket_identity(absolute_socket)
        return original_connect(connection, address)

    monkeypatch.setattr(socket.socket, "connect", substitute_before_connect)
    thread = threading.Thread(target=serve_original)
    thread.start()
    try:
        response = DaemonAPIClient(
            configured_socket,
            socket_group=group_name,
            timeout_seconds=1,
        ).request("ping")

        pinned_socket = pinned_parent / absolute_socket.name
        assert response == {"ok": True, "result": {"source": "original"}}
        assert substituted is True
        assert replacement_listener is not None
        assert replacement_identity is not None
        assert _socket_identity(absolute_socket) == replacement_identity
        _assert_unix_socket_connects(absolute_socket)
        _assert_unix_socket_connects(pinned_socket)
    finally:
        original_listener.close()
        if replacement_listener is not None:
            replacement_listener.close()
        thread.join(timeout=2)
        absolute_socket.unlink(missing_ok=True)
        (pinned_parent / absolute_socket.name).unlink(missing_ok=True)

    assert not thread.is_alive()


@_UNIX_SOCKET_TEST
@pytest.mark.parametrize("socket_mode", ["private", "group"])
def test_socket_client_rejects_leaf_replacement_after_anchored_connect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    socket_mode: str,
) -> None:
    group_name: str | None = None
    group_id: int | None = None
    if socket_mode == "group":
        group_name, group_id = _current_socket_group()
    parent = tmp_path / "post-connect-parent"
    _prepare_socket_test_parent(parent, group_id=group_id)
    socket_path = parent / "daemon.sock"
    original_listener = _prepare_socket_test_endpoint(
        socket_path,
        group_id=group_id,
    )
    original_connect = socket.socket.connect
    replacement_listener: socket.socket | None = None
    replacement_identity: tuple[int, int] | None = None

    def replace_after_connect(connection: socket.socket, address: Any) -> Any:
        nonlocal replacement_listener, replacement_identity
        result = original_connect(connection, address)
        if (
            replacement_listener is None
            and str(address).startswith("/proc/self/fd/")
            and str(address).endswith(f"/{socket_path.name}")
        ):
            socket_path.unlink()
            replacement_listener = _prepare_socket_test_endpoint(
                socket_path,
                group_id=group_id,
            )
            replacement_identity = _socket_identity(socket_path)
        return result

    monkeypatch.setattr(socket.socket, "connect", replace_after_connect)
    try:
        with pytest.raises(DaemonUnavailable) as caught:
            DaemonAPIClient(
                socket_path,
                socket_group=group_name,
                timeout_seconds=1,
            ).request("ping")

        assert caught.value.code is LocalStateErrorCode.ENTRY_CHANGED
        assert caught.value.request_started is False
        _assert_private_daemon_failure(caught.value, socket_path)
        assert replacement_listener is not None
        assert replacement_identity is not None
        assert _socket_identity(socket_path) == replacement_identity
        _assert_unix_socket_connects(socket_path)
    finally:
        original_listener.close()
        if replacement_listener is not None:
            replacement_listener.close()
        socket_path.unlink(missing_ok=True)


@_UNIX_SOCKET_TEST
@pytest.mark.parametrize("socket_mode", ["private", "group"])
def test_socket_startup_lock_contention_is_bounded_and_closes_parent_fd(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    socket_mode: str,
) -> None:
    import fcntl
    import tendwire.daemon_api as daemon_api_module

    group_name: str | None = None
    group_id: int | None = None
    if socket_mode == "group":
        group_name, group_id = _current_socket_group()
    parent = tmp_path / "locked"
    _prepare_socket_test_parent(parent, group_id=group_id)
    socket_path = parent / "daemon.sock"
    holder_fd = os.open(
        parent,
        os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
    )
    fcntl.flock(holder_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    original_open = daemon_api_module.open_resolved_parent
    opened_parent_fds: list[int] = []

    def track_open_parent(*args: Any, **kwargs: Any) -> tuple[int, str]:
        parent_fd, leaf = original_open(*args, **kwargs)
        opened_parent_fds.append(parent_fd)
        return parent_fd, leaf

    monkeypatch.setattr(daemon_api_module, "open_resolved_parent", track_open_parent)
    monkeypatch.setattr(
        daemon_api_module,
        "_SOCKET_STARTUP_LOCK_TIMEOUT_SECONDS",
        0.02,
    )
    monkeypatch.setattr(
        daemon_api_module,
        "_SOCKET_STARTUP_LOCK_RETRY_SECONDS",
        0.001,
    )
    server = UnixSocketJSONServer(
        socket_path,
        lambda _request: {"ok": True},
        socket_group=group_name,
        prepare_parent=False,
    )
    errors: list[BaseException] = []

    def start_server() -> None:
        try:
            server.start()
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=start_server)
    thread.start()
    thread.join(timeout=0.5)
    completed_while_contended = not thread.is_alive()
    fcntl.flock(holder_fd, fcntl.LOCK_UN)
    os.close(holder_fd)
    thread.join(timeout=1)
    try:
        assert completed_while_contended
        assert not thread.is_alive()
        assert len(errors) == 1
        assert isinstance(errors[0], DaemonUnavailable)
        assert str(errors[0]) == "daemon socket startup lock timed out"
        assert errors[0].code is LocalStateErrorCode.OPERATION_FAILED
        _assert_private_daemon_failure(errors[0], parent, socket_path)
        assert opened_parent_fds
        with pytest.raises(OSError) as closed:
            os.fstat(opened_parent_fds[0])
        assert closed.value.errno == errno.EBADF
        assert not os.path.lexists(socket_path)
    finally:
        server.close()


@_UNIX_SOCKET_TEST
def test_socket_startup_lock_retries_interrupted_nonblocking_flock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import fcntl

    parent = tmp_path / "eintr"
    _prepare_socket_test_parent(parent, group_id=None)
    socket_path = parent / "daemon.sock"
    original_flock = fcntl.flock
    interrupted = False

    def interrupt_once(fd: int, operation: int) -> Any:
        nonlocal interrupted
        if operation & fcntl.LOCK_NB and not interrupted:
            interrupted = True
            raise OSError(errno.EINTR, "sentinel interrupted flock")
        return original_flock(fd, operation)

    monkeypatch.setattr(fcntl, "flock", interrupt_once)
    server = UnixSocketJSONServer(
        socket_path,
        lambda _request: {"ok": True},
        prepare_parent=False,
    )
    try:
        server.start()
        assert interrupted
        assert server.listening
        _assert_unix_socket_connects(socket_path)
    finally:
        server.close()

    assert not os.path.lexists(socket_path)


@_UNIX_SOCKET_TEST
def test_isolated_daemon_survives_deterministic_real_wal_retirement_without_resources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "private-daemon-race.db"
    socket_path = tmp_path / "private-daemon-race.sock"
    config = Config(
        host_id="daemon-race-host",
        data_dir=tmp_path,
        db_path=db_path,
        socket_path=socket_path,
        acknowledged_final_retention_days=36500,
    )
    worker = Worker(id="worker-race", name="Worker Race", status="active")
    snapshot = Snapshot(
        host_id=config.host_id,
        updated_at="2026-01-01T00:00:00+00:00",
        workers=[worker],
        backend_health=[
            BackendHealth(
                name="herdr",
                status="healthy",
                outcome="healthy_non_empty",
                observed_at="2026-01-01T00:00:00+00:00",
            )
        ],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    upsert_worker_bindings(
        db_path,
        [
            WorkerBinding(
                host_id=config.host_id,
                worker_id=worker.id,
                worker_fingerprint=worker.fingerprint,
                backend="herdr",
                target_kind="agent_id",
                target_value="private-race-agent",
                turn_target_kind="pane_id",
                turn_target_value="private-race-pane",
                sendable=True,
                reason=None,
                observed_at="2026-01-01T00:00:00+00:00",
                private_fingerprint="private-race-binding",
            )
        ],
    )
    assert merge_turn_content(
        db_path,
        config.host_id,
        worker.id,
        {
            "source_turn_id": "source-turn-race",
            "assistant_final_text": "durable race final",
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-01-01T00:01:00+00:00",
    ) == 1
    setup_connection = sqlite3.connect(str(db_path))
    try:
        assert setup_connection.execute(
            "PRAGMA journal_mode=WAL"
        ).fetchone()[0] == "wal"
        setup_connection.execute(
            "CREATE TABLE IF NOT EXISTS daemon_race_churn "
            "(cycle INTEGER PRIMARY KEY, value TEXT NOT NULL)"
        )
        setup_connection.commit()
        assert setup_connection.execute(
            "PRAGMA wal_checkpoint(TRUNCATE)"
        ).fetchone()[0] == 0
    finally:
        setup_connection.close()

    cycle_count = 4
    wal_path = Path(f"{db_path}-wal")
    shm_path = Path(f"{db_path}-shm")
    ready = threading.Barrier(2, timeout=10)
    captured = threading.Barrier(2, timeout=10)
    retired = threading.Barrier(2, timeout=10)
    consumed = threading.Barrier(2, timeout=10)
    race_enabled = threading.Event()
    phase_calls: list[tuple[str, LocalStateKind]] = []
    writer_errors: list[BaseException] = []

    def phase_hook(phase: str, kind: LocalStateKind) -> None:
        if (
            phase == "captured"
            and kind is LocalStateKind.DATABASE_WAL
            and race_enabled.is_set()
        ):
            race_enabled.clear()
            phase_calls.append((phase, kind))
            captured.wait()
            retired.wait()

    monkeypatch.setattr(
        "tendwire.local_state._sqlite_family_test_phase",
        phase_hook,
    )

    def churn_wal() -> None:
        try:
            for cycle in range(cycle_count):
                connection = sqlite3.connect(str(db_path), timeout=5)
                try:
                    assert connection.execute(
                        "PRAGMA journal_mode=WAL"
                    ).fetchone()[0] == "wal"
                    connection.execute(
                        "INSERT INTO daemon_race_churn (cycle, value) VALUES (?, ?)",
                        (cycle, f"value-{cycle}"),
                    )
                    connection.commit()
                    if not os.path.lexists(wal_path):
                        raise AssertionError("WAL was not live before capture")
                    ready.wait()
                    captured.wait()
                    assert connection.execute(
                        "PRAGMA wal_checkpoint(TRUNCATE)"
                    ).fetchone()[0] == 0
                finally:
                    connection.close()
                wal_path.unlink(missing_ok=True)
                shm_path.unlink(missing_ok=True)
                assert not os.path.lexists(wal_path)
                assert not os.path.lexists(shm_path)
                retired.wait()
                consumed.wait()
        except BaseException as exc:  # noqa: BLE001
            writer_errors.append(exc)
            for barrier in (ready, captured, retired, consumed):
                try:
                    barrier.abort()
                except threading.BrokenBarrierError:
                    pass

    def direct_child_processes() -> set[int]:
        children: set[int] = set()
        for task in (Path("/proc/self/task")).iterdir():
            try:
                values = (task / "children").read_text(encoding="ascii").split()
            except FileNotFoundError:
                continue
            children.update(int(value) for value in values)
        return children

    def fd_targets() -> dict[str, tuple[str, int, int, int]]:
        targets: dict[str, tuple[str, int, int, int]] = {}
        for fd in os.listdir("/proc/self/fd"):
            try:
                target = os.readlink(f"/proc/self/fd/{fd}")
                fd_stat = os.fstat(int(fd))
            except (FileNotFoundError, OSError):
                continue
            targets[fd] = (
                target,
                int(fd_stat.st_dev),
                int(fd_stat.st_ino),
                stat.S_IFMT(fd_stat.st_mode),
            )
        return targets

    # Earlier SQLite fixtures may leave unreachable cyclic connections pending
    # collection. Stabilize the process-wide descriptor baseline before this
    # test attributes later changes to the daemon under test.
    gc.collect()
    baseline_fds = fd_targets()
    baseline_threads = {id(thread) for thread in threading.enumerate()}
    baseline_children = direct_child_processes()
    main_identity = (db_path.stat().st_dev, db_path.stat().st_ino)
    daemon = TendwireDaemon(config)
    server_thread: threading.Thread | None = None
    writer_thread = threading.Thread(target=churn_wal)
    writer_started = False
    requests_completed = False
    responses: list[dict[str, Any]] = []
    try:
        daemon.start()
        assert daemon.server is not None
        api = daemon.server.dispatcher.__self__
        assert isinstance(api, TendwireDaemonAPI)
        for callback_name, method_name in (
            ("_get_snapshot", "get_snapshot"),
            ("_get_turns", "get_turns"),
            ("_get_health", "get_health"),
            ("_get_pending", "get_pending"),
        ):
            callback = getattr(api, callback_name)
            assert callback.__self__ is daemon
            assert callback.__func__ is getattr(TendwireDaemon, method_name)
        server_thread = threading.Thread(target=daemon.serve_forever)
        server_thread.start()
        writer_thread.start()
        writer_started = True
        client = DaemonAPIClient(socket_path, timeout_seconds=5)
        for _cycle in range(cycle_count):
            ready.wait()
            race_enabled.set()
            snapshot_response = client.request("snapshot.get")
            turn_response = client.request(
                "turn.list",
                {
                    "schema_version": 2,
                    "limit": 10,
                    "cursor": None,
                    "since": None,
                },
            )
            health_response = client.request("health.get")
            responses.extend(
                (snapshot_response, turn_response, health_response)
            )
            assert snapshot_response["ok"] is True, (
                snapshot_response,
                writer_errors,
            )
            assert snapshot_response["result"]["host_id"] == config.host_id
            assert turn_response["ok"] is True, turn_response
            assert turn_response["result"]["schema_version"] == 2
            assert any(
                turn.get("assistant_final_text") == "durable race final"
                for turn in turn_response["result"]["turns"]
            )
            assert health_response["ok"] is True, health_response
            assert health_response["result"]["status"] == "ok"
            assert health_response["result"]["store"]["status"] == "healthy"
            consumed.wait()
        requests_completed = True
    finally:
        race_enabled.clear()
        if not requests_completed:
            for barrier in (ready, captured, retired, consumed):
                try:
                    barrier.abort()
                except threading.BrokenBarrierError:
                    pass
        daemon.stop()
        if writer_started:
            writer_thread.join(timeout=10)
        if server_thread is not None:
            server_thread.join(timeout=10)

    assert not writer_thread.is_alive()
    assert server_thread is not None and not server_thread.is_alive()
    assert writer_errors == []
    assert phase_calls == [
        ("captured", LocalStateKind.DATABASE_WAL)
    ] * cycle_count
    assert len(responses) == cycle_count * 3
    assert (db_path.stat().st_dev, db_path.stat().st_ino) == main_identity
    assert not os.path.lexists(socket_path)
    current_fds = fd_targets()
    changed_fds = {
        fd: (baseline_fds.get(fd), current_fds.get(fd))
        for fd in set(baseline_fds) | set(current_fds)
        if baseline_fds.get(fd) != current_fds.get(fd)
    }
    assert current_fds == baseline_fds, changed_fds
    assert {id(thread) for thread in threading.enumerate()} == baseline_threads
    assert direct_child_processes() == baseline_children
    for response in responses:
        _assert_no_public_json_forbidden(response)
        serialized = json.dumps(response, sort_keys=True)
        for private_value in (
            str(tmp_path),
            str(db_path),
            db_path.name,
            str(socket_path),
            socket_path.name,
            "private-race-agent",
            "private-race-pane",
            "private-race-binding",
            "-wal",
            "-shm",
            "-journal",
            '"uid"',
            '"gid"',
            '"inode"',
        ):
            assert private_value not in serialized
