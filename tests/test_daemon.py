"""Tests for the PR7 Tendwire daemon skeleton and local JSON API."""

from __future__ import annotations

import errno
import base64
import hashlib
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
    stable_fingerprint,
)
from tendwire.core.projector import project_from_raw
from tendwire.daemon import DaemonHooks, TendwireDaemon, run_daemon
from tendwire.daemon_api import (
    DaemonAPIClient,
    DaemonUnavailable,
    DaemonProtocolError,
    TendwireDaemonAPI as _ProductionTendwireDaemonAPI,
    UnixSocketJSONServer,
    ensure_daemon_socket_not_active,
    MAX_RESPONSE_BYTES,
)


def _connector_digest(value: Any) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False,
    ).encode("utf-8")
    return base64.urlsafe_b64encode(hashlib.sha256(encoded).digest()).rstrip(b"=").decode()


def _working_connector_key(payload: dict[str, Any], host_id: str = "host-a") -> str:
    turn, worker = payload["turn"], payload["worker"]
    return "turn-final:working:twwork1." + _connector_digest([
        "tendwire.working.v1",
        [host_id, turn["turn_id"], turn["content_revision"], worker["route_generation"]],
    ])


def _decision_connector_identity(
    payload: dict[str, Any], host_id: str = "host-a",
) -> tuple[str, str]:
    worker, decision = payload["worker"], payload["decision"]
    decision_ref = "pending-" + hashlib.sha256(json.dumps(
        [host_id, worker["worker_id"], decision["revision_digest"]],
        ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False,
    ).encode()).hexdigest()[:24]
    key = "turn-final:decision:twdecision1." + _connector_digest({
        "domain": "tendwire.decision.v1", "host_id": host_id,
        "decision_ref": decision_ref, "revision_digest": decision["revision_digest"],
        "route_generation": worker["route_generation"],
    })
    return decision_ref, key
from tendwire.local_state import LocalStateError, LocalStateErrorCode
from tendwire.store.pending import pending_payload_from_store
from tendwire.store.projection import (
    SnapshotObservationContext,
    attention_payload_from_store,
    latest_snapshot,
    save_snapshot,
    upsert_worker_bindings,
)
from tendwire.store.receipts import get_command_request
from tendwire.store.schema import init_store
from .store_helpers import (
    apply_test_backend_pending,
    upsert_test_worker_bindings,
)


class TendwireDaemonAPI(_ProductionTendwireDaemonAPI):
    """Test constructor that makes the always-injected store callbacks explicit."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        empty = lambda *_args, **_kwargs: {}
        kwargs.setdefault("get_attention", empty)
        kwargs.setdefault("get_turns", empty)
        kwargs.setdefault("get_turn_delta", empty)
        kwargs.setdefault("get_turn_content", empty)
        kwargs.setdefault("get_pending", empty)
        kwargs.setdefault("connector_call", empty)
        super().__init__(*args, **kwargs)


def _required_daemon_callbacks() -> dict[str, Any]:
    empty = lambda *_args, **_kwargs: {}
    return {
        "get_snapshot": lambda: None,
        "get_health": empty,
        "submit_command": empty,
        "get_attention": empty,
        "get_turns": empty,
        "get_turn_delta": empty,
        "get_turn_content": empty,
        "get_pending": empty,
        "connector_call": empty,
    }


def test_daemon_api_requires_every_store_callback() -> None:
    callbacks = _required_daemon_callbacks()
    callbacks.pop("connector_call")
    with pytest.raises(TypeError, match="connector_call"):
        _ProductionTendwireDaemonAPI(**callbacks)


def test_daemon_api_rejects_explicit_noncallable_callback_by_name() -> None:
    callbacks = _required_daemon_callbacks()
    callbacks["get_pending"] = None
    with pytest.raises(TypeError, match="get_pending"):
        _ProductionTendwireDaemonAPI(**callbacks)


class _ConnectorMemoryConnection:
    def __init__(self, request: bytes) -> None:
        self.request = request
        self.response = b""

    def __enter__(self) -> "_ConnectorMemoryConnection":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def settimeout(self, _value: float) -> None:
        return None

    def recv(self, _size: int) -> bytes:
        value, self.request = self.request, b""
        return value

    def sendall(self, value: bytes) -> None:
        self.response += value

    def shutdown(self, _how: int) -> None:
        return None


@pytest.mark.parametrize(
    "kind", ["working", "final_ready", "final_part", "retire", "decision"]
)
def test_connector_payload_survives_connection_framing_exactly(
    tmp_path: Path, kind: str
) -> None:
    payload: dict[str, Any] = {
        "schema_version": {"working": 1, "final_ready": 3, "final_part": 2, "retire": 1, "decision": 1}[kind],
        "kind": kind,
        "created_at": "2026-08-05T01:02:03.000000Z",
        "worker": {
            "worker_id": "worker-a",
            "stable_key": "wsk1_" + "a" * 64,
            "stable_key_version": 1,
            "route_generation": "twroute1." + "A" * 43,
        },
        "route": {
            "partition_key": "twpart1_" + "b" * 64,
            "partition_sequence": 7,
        },
    }
    if kind == "working":
        text = "/home/smith/x /tmp/example token=example must remain exact"
        payload["turn"] = {
            "turn_id": "turn-a", "content_revision": "twrev1." + "C" * 43,
            "replaces_key": None,
            "text": {"assistant_stream_text": text, "char_length": len(text), "byte_length": len(text.encode())},
        }
    elif kind == "final_ready":
        exact = "/home/smith/x /tmp/example token=example must remain exact"
        descriptor = {"availability": "complete", "inline": exact, "char_length": len(exact), "byte_length": len(exact.encode()), "page_count": 0, "first_cursor": None}
        payload["turn"] = {
            "turn_id": "turn-a", "final_identity": "twfinal1." + "D" * 43,
            "content_revision": "twrev1." + "C" * 43, "replaces_key": None,
            "content": {"schema_version": 1, "content_revision": "twrev1." + "C" * 43, "known_incomplete": False, "fields": {"user_text": descriptor, "assistant_final_text": descriptor}},
        }
    elif kind == "final_part":
        payload.update({
            "turn": {"turn_id": "turn-a", "final_identity": "twfinal1." + "D" * 43, "content_revision": "twrev1." + "C" * 43},
            "plan": {"plan_token": "twplan1." + "E" * 43, "generation": 1, "presentation_version": "v1", "ordinal": 0, "part_count": 1, "spans": [{"field": "assistant_final_text", "start_char": 0, "end_char": 1}]},
            "lineage": {"recovered_from_plan_token": None, "predecessor_key": None, "replaces_key": None},
        })
    elif kind == "retire":
        payload.update({
            "turn": {"turn_id": "turn-a", "final_identity": "twfinal1." + "D" * 43, "content_revision": "twrev1." + "C" * 43},
            "retire": {"target_key": "turn-final:twplan1." + "T" * 43 + ":000003", "target_kind": "final_part", "target_ordinal": 2, "predecessor_key": "turn-final:twplan1." + "E" * 43 + ":000004", "plan_token": "twplan1." + "E" * 43, "generation": 1, "reason": "excess_part"},
        })
    else:
        payload["decision"] = {
            "decision_ref": "", "revision_digest": "revision-a", "mode": "single",
            "title": "/home/smith/x", "body": "/tmp/example token=example",
            "choices": [{"ordinal": 0, "option_ref": "1", "label": "code token=example"}],
        }
        payload["decision"]["decision_ref"], decision_key = _decision_connector_identity(payload)
    working_key = _working_connector_key(payload) if kind == "working" else ""
    connector_result = {
        "schema_version": 1,
        "ok": True,
        "status": "ok",
        "host_id": "host-a",
        "name": "turn-final",
        "items": [{
            "key": {
                "working": working_key,
                "final_ready": "turn-final:revision:twfinal1." + "D" * 43,
                "final_part": "turn-final:twplan1." + "E" * 43 + ":000001",
                "retire": "turn-final:twplan1." + "E" * 43 + ":000005",
                "decision": decision_key if kind == "decision" else "",
            }[kind],
            "ref": "twref1." + "R" * 43,
            "attempt": 1,
            "leased_until": "2026-08-05T01:03:03.000000Z",
            "available_at": "2026-08-05T01:02:03.000000Z",
            "created_at": "2026-08-05T01:02:03.000000Z",
            "payload": payload,
        }],
    }
    api = TendwireDaemonAPI(
        get_snapshot=lambda: None,  # type: ignore[arg-type]
        get_health=lambda: {},
        submit_command=lambda _params: {},
        connector_call=lambda _method, _params: connector_result,
    )
    server = UnixSocketJSONServer(
        tmp_path / "unused.sock",
        api.dispatch,
        request_workers=1,
        max_in_flight_requests=1,
    )
    request = {"method": "connector.poll", "params": {"name": "turn-final"}}
    connection = _ConnectorMemoryConnection(
        json.dumps(request).encode("utf-8") + b"\n"
    )
    server._handle_connection(connection)  # type: ignore[arg-type]
    response = json.loads(connection.response.split(b"\n", 1)[0])
    assert response["result"] == connector_result
    assert response["result"]["items"][0]["payload"] == payload
    if kind == "retire":
        swapped = json.loads(json.dumps(connector_result))
        swapped["items"][0]["payload"]["retire"]["target_ordinal"] = 999
    else:
        swapped = json.loads(json.dumps(connector_result))
        swapped["items"][0]["key"] = {
            "working": "turn-final:working:twwork1." + "Z" * 43,
            "final_ready": "turn-final:revision:twfinal1." + "Z" * 43,
            "final_part": "turn-final:twplan1." + "E" * 43 + ":000002",
            "decision": "turn-final:decision:twdecision1." + "Z" * 43,
        }[kind]
    invalid_api = TendwireDaemonAPI(
        get_snapshot=lambda: None,  # type: ignore[arg-type]
        get_health=lambda: {}, submit_command=lambda _params: {},
        connector_call=lambda _method, _params: swapped,
    )
    invalid = invalid_api.dispatch(request)
    assert invalid["ok"] is False
    assert invalid["error"]["code"] == "internal_error"
    if kind in {"working", "final_ready", "final_part"}:
        fields = ("replaces_key", "predecessor_key") if kind == "final_part" else ("replaces_key",)
        for field in fields:
            for replacement in (
                "turn-final:decision:twdecision1." + "Z" * 43,
                "turn-final:retire:twretire1." + "Z" * 43,
            ):
                bad_lineage = json.loads(json.dumps(connector_result))
                container = "lineage" if kind == "final_part" else "turn"
                bad_lineage["items"][0]["payload"][container][field] = replacement
                lineage_api = TendwireDaemonAPI(
                    get_snapshot=lambda: None,  # type: ignore[arg-type]
                    get_health=lambda: {}, submit_command=lambda _params: {},
                    connector_call=lambda _method, _params: bad_lineage,
                )
                assert lineage_api.dispatch(request)["ok"] is False
    if kind == "retire":
        for field, malformed in (
            ("target_ordinal", None), ("target_ordinal", True),
            ("generation", None), ("generation", 0), ("generation", "1"),
        ):
            bad_retire = json.loads(json.dumps(connector_result))
            bad_retire["items"][0]["payload"]["retire"][field] = malformed
            retire_api = TendwireDaemonAPI(
                get_snapshot=lambda: None,  # type: ignore[arg-type]
                get_health=lambda: {}, submit_command=lambda _params: {},
                connector_call=lambda _method, _params: bad_retire,
            )
            rejected = retire_api.dispatch(request)
            assert rejected["ok"] is False
            assert rejected["error"]["code"] == "internal_error"


@pytest.mark.parametrize(
    "mutation",
    [
        lambda result: result.update({"unexpected": "field"}),
        lambda result: result["items"][0]["payload"].update(
            {"private_binding": {"chat_id": 42}}
        ),
        lambda result: result["items"][0]["payload"]["turn"]["text"].update(
            {"chatId": 42}
        ),
        lambda result: result["items"][0]["payload"]["worker"].update(
            {"provider_token": "private"}
        ),
        lambda result: result["items"][0]["payload"]["route"].update(
            {"private_binding_json": "private"}
        ),
        lambda result: result["items"][0].update({"attempt": float("nan")}),
    ],
)
def test_connector_exact_response_rejects_unvalidated_or_private_data(
    tmp_path: Path, mutation: Any
) -> None:
    payload = {
        "schema_version": 1,
        "kind": "working",
        "created_at": "2026-08-05T01:02:03.000000Z",
        "worker": {"worker_id": "worker-a", "stable_key": "wsk1_" + "a" * 64, "stable_key_version": 1, "route_generation": "twroute1." + "A" * 43},
        "route": {"partition_key": "twpart1_" + "b" * 64, "partition_sequence": 1},
        "turn": {"turn_id": "turn-a", "content_revision": "twrev1." + "C" * 43, "replaces_key": None, "text": {"assistant_stream_text": "ok", "char_length": 2, "byte_length": 2}},
    }
    result = {"schema_version": 1, "ok": True, "status": "ok", "host_id": "host-a", "name": "turn-final", "items": [{"key": "turn-final:working:twwork1." + "W" * 43, "ref": "twref1." + "R" * 43, "attempt": 1, "leased_until": "2026-08-05T01:03:03.000000Z", "available_at": "2026-08-05T01:02:03.000000Z", "created_at": "2026-08-05T01:02:03.000000Z", "payload": payload}]}
    result["items"][0]["key"] = _working_connector_key(payload)
    mutation(result)
    api = TendwireDaemonAPI(get_snapshot=lambda: None, get_health=lambda: {}, submit_command=lambda _params: {}, connector_call=lambda _method, _params: result)  # type: ignore[arg-type]
    response = api.dispatch({"method": "connector.poll", "params": {"name": "turn-final"}})
    assert response["ok"] is False
    assert response["error"]["code"] == "internal_error"


def test_generic_connector_payload_and_requeued_status_survive_exact_framing(
    tmp_path: Path,
) -> None:
    generic_result = {
        "schema_version": 1,
        "ok": True,
        "status": "ok",
        "host_id": "host-a",
        "name": "notice",
        "items": [{
            "key": "notice-key",
            "ref": "twref1." + "R" * 43,
            "attempt": 1,
            "leased_until": "2026-08-05T01:03:03.000000Z",
            "available_at": "2026-08-05T01:02:03.000000Z",
            "payload": {"schema_version": 1, "event_type": "notice", "body": "exact"},
        }],
    }
    api = TendwireDaemonAPI(
        get_snapshot=lambda: None,  # type: ignore[arg-type]
        get_health=lambda: {},
        submit_command=lambda _params: {},
        connector_call=lambda _method, _params: generic_result,
    )
    response = api.dispatch({"method": "connector.poll", "params": {"name": "notice"}})
    assert response["ok"] is True
    assert response["result"] == generic_result

    requeued = {
        "schema_version": 1,
        "ok": True,
        "status": "requeued",
        "host_id": "host-a",
        "name": "turn-final",
        "key": "turn-final:decision:twdecision1." + "D" * 43,
        "retry_generation": 2,
        "prior_attempt_count": 1,
        "warning": "provider_acceptance_may_have_occurred",
    }
    retry_api = TendwireDaemonAPI(
        get_snapshot=lambda: None,  # type: ignore[arg-type]
        get_health=lambda: {},
        submit_command=lambda _params: {},
        connector_call=lambda _method, _params: requeued,
    )
    retry_response = retry_api.dispatch(
        {"method": "connector.retry", "params": {"name": "turn-final"}}
    )
    assert retry_response["ok"] is True
    assert retry_response["result"] == requeued


@pytest.mark.parametrize("private_key", ["chatId", "provider_token", "private_binding_json"])
def test_generic_connector_payload_rejects_nested_private_keys(
    private_key: str,
) -> None:
    generic_result = {
        "schema_version": 1,
        "ok": True,
        "status": "ok",
        "host_id": "host-a",
        "name": "notice",
        "items": [
            {
                "key": "notice-key",
                "ref": "twref1." + "R" * 43,
                "attempt": 1,
                "leased_until": "2026-08-05T01:03:03.000000Z",
                "available_at": "2026-08-05T01:02:03.000000Z",
                "payload": {
                    "schema_version": 1,
                    "event_type": "notice",
                    "body": "exact",
                    "nested": {private_key: "private"},
                },
            }
        ],
    }
    api = TendwireDaemonAPI(
        get_snapshot=lambda: None,  # type: ignore[arg-type]
        get_health=lambda: {},
        submit_command=lambda _params: {},
        connector_call=lambda _method, _params: generic_result,
    )
    response = api.dispatch(
        {"method": "connector.poll", "params": {"name": "notice"}}
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "internal_error"


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
    assert default_pending == {}

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

    assert response["ok"] is False
    assert response["error"]["code"] == "internal_error"
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
    baseline = pending_payload_from_store(db_path, snapshot.host_id)
    assert baseline["pending_health"] == {
        "status": "healthy",
        "counts": {"fresh": 0, "stale": 0, "total": 0},
    }
    upsert_test_worker_bindings(
        db_path,
        [
            WorkerBinding(
                host_id=snapshot.host_id,
                worker_id="worker-1",
                worker_fingerprint=snapshot.workers[0].fingerprint,
                backend="herdr",
                target_kind="agent_id",
                target_value="private",
                private_fingerprint="pending-parity-private",
            )
        ],
    )
    apply_test_backend_pending(
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
def test_pending_projection_is_independent_of_malformed_snapshot_payload(
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
                host_id, observed_at, authority_fingerprint,
                content_fingerprint, payload_json
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                host_id,
                "2026-01-01T00:00:00+00:00",
                "test-authority",
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
        ]
    )
    cli_payload = json.loads(capsys.readouterr().out)
    expected = {
        "schema_version": 1,
        "host_id": host_id,
        "ok": True,
        "status": "ok",
        "pending_interactions": [],
        "backend_health": [],
        "pending_health": {
            "status": "healthy",
            "counts": {"fresh": 0, "stale": 0, "total": 0},
        },
    }
    expected["content_fingerprint"] = stable_fingerprint(
        {
            key: expected[key]
            for key in (
                "schema_version",
                "host_id",
                "pending_interactions",
                "backend_health",
                "pending_health",
            )
        }
    )

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


def test_pending_store_projection_uses_durable_binding_not_snapshot_churn(
    tmp_path: Path,
) -> None:
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
    upsert_test_worker_bindings(
        db_path,
        [
            WorkerBinding(
                host_id=config.host_id,
                worker_id="worker-1",
                worker_fingerprint=snapshot_a.workers[0].fingerprint,
                backend="herdr",
                target_kind="agent_id",
                target_value="private",
                private_fingerprint="pending-atomic-private",
            )
        ],
    )
    apply_test_backend_pending(
        db_path,
        config.host_id,
        "worker-1",
        {"question": "Backend A?", "kind": "question", "meta": {"source": "backend"}},
    )

    first = pending_payload_from_store(db_path, config.host_id)
    assert first["pending_interactions"][0]["question"] == "Backend A?"
    assert (
        first["pending_interactions"][0]["worker_fingerprint"]
        == snapshot_a.workers[0].fingerprint
    )

    save_snapshot(db_path, snapshot_b)
    apply_test_backend_pending(
        db_path,
        config.host_id,
        "worker-1",
        {
            "question": "Backend B?",
            "kind": "question",
            "meta": {"source": "backend"},
        },
    )
    second = pending_payload_from_store(db_path, config.host_id)
    assert second["pending_interactions"][0]["question"] == "Backend B?"
    assert (
        second["pending_interactions"][0]["worker_fingerprint"]
        == snapshot_a.workers[0].fingerprint
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

    monkeypatch.setattr("tendwire.store.turns.turns_payload_from_store", project)
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


def test_daemon_api_attention_list_uses_current_store_projection(tmp_path: Path) -> None:
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
    assert payload["attention"][0]["severity"] == "critical"
    assert payload["attention"][0]["status"] == "failed"
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



def test_attention_projection_replaces_prior_snapshot_without_outbox(
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
    assert _attention_outbox_count(db_path, "flap-host") == 0

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
    assert payload["attention"] == []

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
    assert _attention_outbox_count(db_path, "flap-host") == 0
    assert len(attention_payload_from_store(db_path, "flap-host")["attention"]) == 1


def test_attention_recurrence_is_projection_only_and_never_enqueues(tmp_path: Path) -> None:
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
    assert _attention_outbox_count(db_path, "reopen-host") == 0

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
    assert attention_payload_from_store(db_path, "reopen-host")["attention"] == []

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
    assert _attention_outbox_count(db_path, "reopen-host") == 0
    assert len(attention_payload_from_store(db_path, "reopen-host")["attention"]) == 1


def test_daemon_health_uses_current_concern_owned_store_counts(tmp_path: Path) -> None:
    db_path = tmp_path / "current-health.db"
    config = Config(host_id="health-host", db_path=db_path)
    save_snapshot(db_path, project_from_raw(config, workers=[]))

    health = TendwireDaemon(config).get_health()

    assert health["status"] == "ok"
    assert health["store"]["status"] == "healthy"
    assert health["store"]["counts"] == {
        "turns": 0,
        "agent_events": 0,
        "outbox": 0,
    }
    assert health["store"]["maintenance"] is None
    assert set(health["limits"]) == {
        "reconcile_interval_seconds",
        "event_retention_days",
        "max_outbox_attempts",
    }


def test_daemon_health_rejects_malformed_current_store_shape_without_leaking(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tendwire.store import db as store_db

    db_path = tmp_path / "malformed-current-health.db"
    config = Config(host_id="health-host", db_path=db_path)
    save_snapshot(db_path, project_from_raw(config, workers=[]))
    private_marker = "sentinel-private-store-diagnostic"

    monkeypatch.setattr(
        store_db,
        "store_status",
        lambda *_args, **_kwargs: {
            "ok": True,
            "status": "ok",
            "host_id": config.host_id,
            "store_schema_version": 30,
            "counts": {
                "turns": 0,
                "agent_events": 0,
                "connector_outbox": 0,
                "diagnostic": private_marker,
            },
        },
    )

    health = TendwireDaemon(config).get_health()
    assert health["status"] == "degraded"
    assert health["store"] == {
        "status": "unavailable",
        "schema_version": None,
        "counts": {"turns": 0, "agent_events": 0, "outbox": 0},
        "maintenance": None,
    }
    assert private_marker not in json.dumps(health)


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
        acknowledged_final_retention_days=36_500,
        command_receipt_retention_seconds=691_200,
    )
    init_store(db_path)
    captured: dict[str, Any] = {}

    def maintenance(path: Path, *, policy: Any) -> dict[str, Any]:
        captured.update({"path": path, "policy": policy})
        return {"agent_events": 4, "snapshots": 0, "checkpoint": {}}

    monkeypatch.setattr(
        "tendwire.store.retention.run_retention_cycle",
        maintenance,
    )
    daemon = TendwireDaemon(config)
    daemon._after_snapshot_saved()

    assert captured["path"] == db_path
    assert captured["policy"].event_retention_days == 9
    assert captured["policy"].batch_size == 13
    assert captured["policy"].targetable_retention_days == 36_500
    assert captured["policy"].route_content_retention_days == 36_500
    assert captured["policy"].command_retention_days == 8
    assert daemon._automatic_maintenance_status == {
        "ok": True,
        "status": "ok",
        "due": True,
        "result": {"agent_events": 4, "snapshots": 0, "checkpoint": {}},
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
        snapshot_maintenance_batch_size=17,
        store_maintenance_cadence_seconds=91,
        acknowledged_final_retention_days=33,
        command_receipt_retention_seconds=691_200,
    )
    calls: list[tuple[Path, Any]] = []

    def initialize(path: Path) -> None:
        init_store(path)
        snapshot = _public_snapshot()
        save_snapshot(db_path, snapshot)

    def maintenance(path: Path, *, policy: Any) -> dict[str, Any]:
        calls.append((path, policy))
        return {"agent_events": 1, "snapshots": 0, "checkpoint": {}}

    monkeypatch.setattr(
        "tendwire.store.retention.run_retention_cycle",
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
    path, policy = calls[0]
    assert path == db_path
    assert (
        policy.snapshot_retention_days,
        policy.batch_size,
    ) == (
        21,
        17,
    )
    assert health["store"]["maintenance"] == {
        "ok": True,
        "status": "ok",
        "due": True,
        "result": {"agent_events": 1, "snapshots": 0, "checkpoint": {}},
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
        "tendwire.store.retention.run_retention_cycle",
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
    assert health["store"]["maintenance"] == {
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
            raise LocalStateError(LocalStateErrorCode.OPERATION_FAILED)
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
        raise LocalStateError(LocalStateErrorCode.OPERATION_FAILED)

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
            raise LocalStateError(LocalStateErrorCode.OPERATION_FAILED)
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
        assert conn.execute("SELECT COUNT(*) FROM command_receipts").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM agent_events").fetchone()[0] == 0

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
