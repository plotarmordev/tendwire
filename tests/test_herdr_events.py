"""Tests for the opt-in Herdr socket event backend."""

from __future__ import annotations

import builtins
import importlib
import json
import os
import sqlite3
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Mapping
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from tendwire.backends import herdr_cli, herdr_events
from tendwire.backends.herdr_events import (
    DEFAULT_SUBSCRIBE_METHOD,
    HerdrEventBackend,
    HerdrEventBackendError,
    HerdrEventId,
    HerdrProducerSequence,
    normalize_event,
)
from tendwire.backends.herdr_cli import HerdrContinuityUnavailableError
from tendwire.backends.herdr_socket import (
    HerdrSocketClient,
    HerdrSocketDisconnectedError,
    HerdrSocketTimeoutError,
)
from tendwire.backends.herdr_protocol import (
    HERDR_EVENTS_SUBSCRIBE_METHOD,
    HERDR_OFFICIAL_EVENT_NAMES,
    HerdrEnvelopeError,
    HerdrErrorResponse,
    build_events_subscribe_params,
)
from tendwire.config import Config
from tendwire.core.models import BackendHealth, Snapshot, Worker, WorkerBinding
from tendwire.core.projector import project_from_observations
from tendwire.core.turns import PendingObservation
from tendwire.daemon import DaemonHooks, TendwireDaemon
from tendwire.local_state import LocalStateErrorCode, local_state_error
from tendwire.store.sqlite import (
    SnapshotObservationContext,
    SnapshotRetentionPolicy,
    apply_backend_pending_observation,
    init_store,
    latest_snapshot,
    list_attention_items,
    list_backend_pending,
    list_worker_bindings,
    maybe_run_automatic_store_maintenance,
    merge_turn_content,
    pending_payload_from_store,
    save_snapshot,
    turns_payload_from_store,
)


_PUBLIC_JSON_FORBIDDEN_KEYS = {
    "pane_id",
    "terminal_id",
    "backend_target",
    "chat_id",
    "topic_id",
    "message_id",
    "connector",
    "argv",
    "args",
    "env",
    "environment",
    "stdin",
    "stdout",
    "stderr",
    "token",
    "tokens",
    "secret",
    "secrets",
    "raw_payload",
    "socket_path",
    "target_kind",
    "target_value",
    "private_fingerprint",
}
_PUBLIC_JSON_FORBIDDEN_COMPACT = {key.replace("_", "") for key in _PUBLIC_JSON_FORBIDDEN_KEYS}


def _assert_no_public_json_forbidden(value: Any, path: str = "$") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            normalized = str(key).lower().replace("-", "_")
            assert (
                normalized not in _PUBLIC_JSON_FORBIDDEN_KEYS
                and normalized.replace("_", "") not in _PUBLIC_JSON_FORBIDDEN_COMPACT
            ), f"forbidden field {path}.{key}"
            _assert_no_public_json_forbidden(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _assert_no_public_json_forbidden(item, f"{path}[{index}]")



_NO_OP_TABLES = (
    "commands",
    "command_receipts",
    "turns",
    "attention_items",
    "connector_outbox",
    "connector_deliveries",
)

_PERSISTED_EVENT_EFFECT_TABLES = (
    "snapshots",
    "events",
    "workers",
    "worker_bindings",
    "attention_items",
    "connector_outbox",
)


def _table_count(db_path: Path, host_id: str, table: str) -> int:
    with closing(sqlite3.connect(str(db_path))) as conn, conn:
        return int(conn.execute(f"SELECT COUNT(*) FROM {table} WHERE host_id = ?", (host_id,)).fetchone()[0])

def _persisted_event_effect_counts(backend: HerdrEventBackend) -> dict[str, int]:
    return {
        table: _table_count(backend.db_path, backend.config.host_id, table)
        for table in _PERSISTED_EVENT_EFFECT_TABLES
    }


def _attention_lifecycle_rows(backend: HerdrEventBackend) -> tuple[tuple[Any, ...], ...]:
    with closing(sqlite3.connect(str(backend.db_path))) as conn, conn:
        return tuple(
            conn.execute(
                """
                SELECT
                    generation,
                    lifecycle_status,
                    current_attention_id,
                    first_seen_at,
                    last_positive_at,
                    first_missing_at,
                    missing_observation_count,
                    last_accepted_at,
                    last_observation_key,
                    max_notified_severity_rank
                FROM attention_lifecycles
                WHERE host_id = ?
                ORDER BY family_key
                """,
                (backend.config.host_id,),
            ).fetchall()
        )


def _attention_event_types(backend: HerdrEventBackend) -> list[str]:
    with closing(sqlite3.connect(str(backend.db_path))) as conn, conn:
        rows = conn.execute(
            """
            SELECT payload_json
            FROM connector_outbox
            WHERE host_id = ? AND connector = 'attention'
            ORDER BY id
            """,
            (backend.config.host_id,),
        ).fetchall()
    return [str(json.loads(row[0])["event_type"]) for row in rows]


def _set_observation_time(monkeypatch: Any, value: str) -> None:
    monkeypatch.setattr("tendwire.backends.herdr_cli.utc_timestamp", lambda: value)
    monkeypatch.setattr("tendwire.backends.herdr_events.utc_timestamp", lambda: value)

def _no_op_state(backend: HerdrEventBackend) -> tuple[str, tuple[tuple[Any, ...], ...], dict[str, int]]:
    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    assert snapshot is not None
    bindings = tuple(
        sorted(
            (
                binding.worker_id,
                binding.private_fingerprint,
                binding.target_kind,
                binding.target_value,
                binding.sendable,
                binding.reason,
                binding.expires_at,
            )
            for binding in list_worker_bindings(
                backend.db_path,
                backend.config.host_id,
                backend="herdr",
                include_expired=True,
            )
        )
    )
    counts = {table: _table_count(backend.db_path, backend.config.host_id, table) for table in _NO_OP_TABLES}
    return snapshot.to_json(), bindings, counts

class _SocketConnection:
    def __init__(self, conn: socket.socket, requests: list[dict[str, Any]]) -> None:
        self.conn = conn
        self.requests = requests
        self._buffer = bytearray()

    def read_request(self) -> dict[str, Any]:
        while b"\n" not in self._buffer:
            chunk = self.conn.recv(4096)
            if not chunk:
                raise ConnectionError("client disconnected before request")
            self._buffer.extend(chunk)
        index = self._buffer.index(b"\n")
        line = bytes(self._buffer[: index + 1])
        del self._buffer[: index + 1]
        request = json.loads(line.decode("utf-8"))
        self.requests.append(request)
        return request

    def send_json(self, payload: Mapping[str, Any]) -> None:
        self.conn.sendall(json.dumps(dict(payload), separators=(",", ":")).encode("utf-8") + b"\n")


class _FakeHerdrSocketServer:
    def __init__(
        self,
        tmp_path: Path,
        handler: Callable[[_SocketConnection], None],
        *,
        connections: int = 1,
    ) -> None:
        self.path = tmp_path / f"herdr-events-{time.monotonic_ns()}.sock"
        self.handler = handler
        self.connections = connections
        self.requests: list[dict[str, Any]] = []
        self.errors: list[BaseException] = []
        self._ready = threading.Event()
        self._listener: socket.socket | None = None
        self._thread: threading.Thread | None = None

    def __enter__(self) -> "_FakeHerdrSocketServer":
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(self.path))
        listener.listen(self.connections)
        listener.settimeout(0.2)
        self._listener = listener
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        assert self._ready.wait(1)
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        if self._listener is not None:
            self._listener.close()
        if self._thread is not None:
            self._thread.join(timeout=1)
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass
        if exc_type is None and self.errors:
            raise AssertionError(f"fake Herdr socket failed: {self.errors!r}")

    def _run(self) -> None:
        self._ready.set()
        try:
            assert self._listener is not None
            for _index in range(self.connections):
                conn, _addr = self._listener.accept()
                with conn:
                    self.handler(_SocketConnection(conn, self.requests))
        except OSError:
            pass
        except BaseException as exc:
            self.errors.append(exc)


class _StaticClient:
    def __init__(
        self,
        *,
        workspaces: Any | None = None,
        tabs: Any | None = None,
        panes: Any | None = None,
        agents: Any | None = None,
    ) -> None:
        self.workspaces = {"workspaces": list(workspaces or [])}
        self.tabs = {"tabs": list(tabs or [])}
        self.panes = {"panes": list(panes or [])}
        self.agents = {"agents": list(agents or [])}
        self.calls: list[str] = []

    def workspace_list(self) -> Any:
        self.calls.append("workspace.list")
        return self.workspaces

    def tab_list(self) -> Any:
        self.calls.append("tab.list")
        return self.tabs

    def pane_list(self) -> Any:
        self.calls.append("pane.list")
        return self.panes

    def agent_list(self) -> Any:
        self.calls.append("agent.list")
        return self.agents


def _config(tmp_path: Path, host_id: str = "events-host") -> Config:
    return Config(
        host_id=host_id,
        data_dir=tmp_path,
        db_path=tmp_path / f"{host_id}.db",
        herdr_backend="socket",
        herdr_timeout_seconds=0.5,
    )


def _backend(tmp_path: Path, host_id: str = "events-host", *, debounce_seconds: float = 0) -> HerdrEventBackend:
    config = _config(tmp_path, host_id)
    init_store(Path(config.db_path))
    return HerdrEventBackend(
        config,
        debounce_seconds=debounce_seconds,
        reconnect_delay_seconds=0,
    )


def _initial_pane_client() -> _StaticClient:
    return _StaticClient(
        workspaces=[{"id": "space-1", "name": "Build", "status": "active"}],
        panes=[
            {
                "pane_id": "pane-1",
                "agent": "Agent One",
                "workspace_id": "space-1",
                "status": "running",
            }
        ],
        agents=[],
    )

def _status_event(status: str) -> dict[str, Any]:
    """Return the confirmed idless Herdr EventEnvelope shape."""
    return {
        "event": "pane_agent_status_changed",
        "data": {
            "pane_id": "pane-1",
            "agent": "Agent One",
            "status": status,
        },
    }


def _write_decision_adapter(tmp_path: Path) -> Path:
    adapter = tmp_path / "fake-herdr-turn-adapter"
    adapter.write_text(
        "#!/usr/bin/env python3\n"
        "import json\n"
        "print(json.dumps({'result': {'turn': {"
        "'available': True, 'complete': False, 'awaiting_input': True, "
        "'user_text': 'Choose a rollout.', 'source_turn_id': 'producer-decision-turn', "
        "'pending_decision': {'prompt': 'Choose a rollout.', 'mode': 'buttons', "
        "'options': [{'id': 'alpha', 'label': 'Alpha', 'send_text': 'Alpha'}, "
        "{'id': 'beta', 'label': 'Beta', 'send_text': 'Beta'}]}}}}))\n",
        encoding="utf-8",
    )
    adapter.chmod(0o700)
    return adapter


def _decision_backend(
    tmp_path: Path,
    turn_model: str,
) -> tuple[HerdrEventBackend, WorkerBinding]:
    adapter = _write_decision_adapter(tmp_path)
    config = Config(
        host_id=f"decision-{turn_model}",
        data_dir=tmp_path,
        db_path=tmp_path / f"decision-{turn_model}.db",
        herdr_backend="socket",
        herdr_bin=str(adapter),
        herdr_timeout_seconds=1,
        turn_model=turn_model,
    )
    init_store(Path(config.db_path))
    backend = HerdrEventBackend(config, debounce_seconds=0, reconnect_delay_seconds=0)
    snapshot = backend.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "w123456789abcde", "name": "Build"}],
            panes=[
                {
                    "pane_id": "w123456789abcde:pA",
                    "terminal_id": "terminal-decision",
                    "agent": "claude",
                    "workspace_id": "w123456789abcde",
                    "agent_status": "working",
                }
            ],
            agents=[],
        )
    )
    binding = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
    )[0]
    # Reproduce the production wedge: the stable worker still has a durable
    # no-prompt row owned by an expired pane binding.
    assert apply_backend_pending_observation(
        backend.db_path,
        backend.config.host_id,
        snapshot.workers[0].id,
        PendingObservation("read_succeeded_no_prompt"),
        binding_private_fingerprint="expired-pane-binding",
        observed_turn_target_value="old-pane",
    )
    return backend, binding


def _assert_decision_persisted(backend: HerdrEventBackend) -> None:
    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    assert snapshot is not None
    worker = snapshot.workers[0]
    backend_pending = list_backend_pending(backend.db_path, backend.config.host_id)
    assert list(backend_pending) == [worker.id]
    assert [choice["label"] for choice in backend_pending[worker.id]["choices"]] == [
        "Alpha",
        "Beta",
    ]
    pending = pending_payload_from_store(backend.db_path, backend.config.host_id)
    interaction = next(
        item for item in pending["pending_interactions"] if item["worker_id"] == worker.id
    )
    assert [choice["label"] for choice in interaction["choices"]] == ["Alpha", "Beta"]
    turns = turns_payload_from_store(
        backend.db_path,
        backend.config.host_id,
        snapshot=snapshot,
    )["turns"]
    turn = next(item for item in turns if item.get("awaiting_input") is True)
    assert turn["complete"] is False
    assert turn["pending_decision"] == {
        "prompt": "Choose a rollout.",
        "mode": "buttons",
        "options": [
            {"id": "1", "label": "Alpha"},
            {"id": "2", "label": "Beta"},
        ],
        "multi_select": False,
        "question_count": 1,
    }


def test_startup_reconcile_uses_socket_client_persists_projection_and_private_bindings(tmp_path: Path) -> None:
    def handler(conn: _SocketConnection) -> None:
        results = {
            "workspace.list": {
                "workspaces": [
                    {
                        "id": "space-1",
                        "name": "Build",
                        "status": "active",
                        "pane_id": "private-pane",
                    }
                ]
            },
            "tab.list": {"tabs": [{"id": "tab-private", "workspace_id": "space-1"}]},
            "pane.list": {
                "panes": [
                    {
                        "pane_id": "pane-1",
                        "terminal_id": "terminal-private",
                        "agent": "Agent One",
                        "workspace_id": "space-1",
                        "status": "running",
                    }
                ]
            },
            "agent.list": {
                "agents": [
                    {
                        "agent_id": "agent-private",
                        "name": "Agent One",
                        "workspace_id": "space-1",
                        "status": "waiting",
                        "pane_id": "pane-1",
                    }
                ]
            },
        }
        request = conn.read_request()
        conn.send_json({"id": request["id"], "result": results[request["method"]]})

    config = _config(tmp_path, "socket-reconcile")
    init_store(Path(config.db_path))
    with _FakeHerdrSocketServer(tmp_path, handler, connections=4) as server:
        backend = HerdrEventBackend(config, debounce_seconds=0)
        client = HerdrSocketClient(str(server.path), timeout=1)
        snapshot = backend.reconcile_once(client=client)
        client.close()

    assert [request["method"] for request in server.requests] == [
        "workspace.list",
        "tab.list",
        "pane.list",
        "agent.list",
    ]
    assert snapshot.backend_health[0].status == "healthy"
    assert "agent-private" not in {worker.id for worker in snapshot.workers}
    assert all("private" not in worker.id.lower() for worker in snapshot.workers)
    bindings = list_worker_bindings(Path(config.db_path), config.host_id, backend="herdr")
    assert bindings
    assert bindings[0].target_value == "agent-private"
    encoded = snapshot.to_json()
    assert "agent-private" not in encoded
    assert "private-pane" not in encoded
    assert "terminal-private" not in encoded
    _assert_no_public_json_forbidden(json.loads(encoded))


@pytest.mark.parametrize("event_name", HERDR_OFFICIAL_EVENT_NAMES)
def test_normalize_event_accepts_each_official_event_name(event_name: str) -> None:
    event = normalize_event({"event": event_name, "data": {}})

    assert event is not None
    assert event.name == event_name
    assert event.producer_identity is None


@pytest.mark.parametrize(
    ("raw_name", "canonical_name"),
    [
        ("agent.status_changed", "pane.agent_status_changed"),
        ("agent_status_changed", "pane.agent_status_changed"),
        ("agent.detected", "pane.agent_detected"),
        ("pane_output_changed", "pane.updated"),
        ("pane.observed", "pane.created"),
        ("workspace.observed", "workspace.updated"),
        ("worktree.updated", "worktree.opened"),
        ("worktree.closed", "worktree.removed"),
    ],
)
def test_normalize_event_tolerates_legacy_inbound_aliases_only_after_receive(
    raw_name: str,
    canonical_name: str,
) -> None:
    event = normalize_event({"event": raw_name, "data": {}})

    assert event is not None
    assert event.name == canonical_name


def test_normalize_event_accepts_confirmed_live_idless_event_data_shape() -> None:
    event = normalize_event(
        {
            "event": "pane_agent_status_changed",
            "data": {"agent": "Agent One", "status": "blocked"},
        }
    )

    assert event is not None
    assert event.name == "pane.agent_status_changed"
    assert event.payload == {"agent": "Agent One", "status": "blocked"}
    assert event.producer_identity is None


def test_normalize_event_prefers_confirmed_data_over_legacy_payload() -> None:
    event = normalize_event(
        {
            "event": "pane_agent_status_changed",
            "data": {"status": "working"},
            "payload": {"status": "idle"},
        }
    )

    assert event is not None
    assert event.payload == {"status": "working"}
    assert event.producer_identity is None


def test_normalize_event_keeps_receive_only_legacy_payload_compatibility() -> None:
    event = normalize_event(
        {
            "event": "pane_agent_status_changed",
            "payload": {"status": "idle"},
        }
    )

    assert event is not None
    assert event.payload == {"status": "idle"}
    assert event.producer_identity is None


def test_normalize_event_exposes_forward_compatible_producer_identity_types() -> None:
    by_id = normalize_event(
        {"event": "pane_agent_status_changed", "data": {}, "event_id": "event-1"}
    )
    by_sequence = normalize_event(
        {
            "event": "pane_agent_status_changed",
            "data": {},
            "server_id": "server-1",
            "sequence": 7,
        }
    )

    assert by_id is not None
    assert by_id.producer_identity == HerdrEventId("event-1")
    assert by_sequence is not None
    assert by_sequence.producer_identity == HerdrProducerSequence("server-1", "7")


def test_normalize_event_never_uses_entity_data_as_producer_identity() -> None:
    event = normalize_event(
        {
            "event": "pane_agent_status_changed",
            "data": {
                "event_id": "entity-event",
                "server_id": "entity-server",
                "sequence": 9,
                "revision": 10,
            },
        }
    )

    assert event is not None
    assert event.producer_identity is None


@pytest.mark.parametrize(
    "metadata",
    [
        {"event_id": True},
        {"event_id": 1},
        {"event_id": 1.5},
        {"event_id": "event id"},
        {"event_id": {"id": "nested-event"}},
        {"event_id": ["nested-event"]},
        {"server_id": True, "sequence": 1},
        {"server_id": "server id", "sequence": 1},
        {"server_id": {"id": "nested-server"}, "sequence": 1},
        {"server_id": "server-1", "sequence": True},
        {"server_id": "server-1", "sequence": 1.5},
        {"server_id": "server-1", "sequence": "1"},
        {"server_id": "server-1", "sequence": {"value": 1}},
        {"server_id": "server-1"},
        {"sequence": 1},
        {"event_id": True, "server_id": "server-1", "sequence": 1},
    ],
)
def test_malformed_producer_metadata_is_idless_and_preserves_transitions(
    tmp_path: Path,
    metadata: dict[str, Any],
) -> None:
    backend = _backend(tmp_path, f"malformed-producer-{len(str(metadata))}")
    backend.reconcile_once(client=_initial_pane_client())
    accepted: list[bool] = []

    for status in ("working", "idle", "working"):
        envelope = {**_status_event(status), **metadata}
        normalized = normalize_event(envelope)
        assert normalized is not None
        assert normalized.producer_identity is None
        accepted.append(backend.queue_event_envelope(envelope))

    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    assert snapshot is not None
    assert accepted == [True, True, True]
    assert snapshot.workers[0].status == "active"


def test_backend_default_subscription_uses_official_shape_without_legacy_defaults(tmp_path: Path) -> None:
    config = _config(tmp_path, "default-subscribe")
    init_store(Path(config.db_path))
    backend = HerdrEventBackend(config, debounce_seconds=0, reconnect_delay_seconds=0)

    class SubscribeClient(_StaticClient):
        def __init__(self) -> None:
            super().__init__()
            self.subscriptions: list[tuple[str, dict[str, Any]]] = []

        def connect(self) -> None:
            return None

        def close(self) -> None:
            return None

        def subscribe(
            self,
            method: str,
            params: Mapping[str, Any],
            *,
            timeout: float | None = None,
            event_timeout: float | None = None,
        ) -> Any:
            self.subscriptions.append((method, dict(params)))
            backend.stop_event.set()
            return SimpleNamespace(subscription_id="sub-default")

    client = SubscribeClient()
    backend.client_factory = lambda _config: client

    backend.run_forever()

    expected_params = {
        "subscriptions": [
            {"type": name}
            for name in HERDR_OFFICIAL_EVENT_NAMES
            if name not in {"pane.agent_status_changed", "pane.output_matched"}
        ]
    }
    assert DEFAULT_SUBSCRIBE_METHOD == HERDR_EVENTS_SUBSCRIBE_METHOD
    assert client.subscriptions == [(HERDR_EVENTS_SUBSCRIBE_METHOD, expected_params)]
    subscribed_names = {subscription["type"] for subscription in expected_params["subscriptions"]}
    assert {
        "pane.observed",
        "workspace.observed",
        "agent.status_changed",
        "worktree.updated",
    }.isdisjoint(subscribed_names)


def test_backend_falls_back_to_herdr_074_pane_scoped_event_subscriptions(tmp_path: Path) -> None:
    config = _config(tmp_path, "pane-scoped-subscribe")
    init_store(Path(config.db_path))
    backend = HerdrEventBackend(config, debounce_seconds=0, reconnect_delay_seconds=0)

    class PaneScopedClient(_StaticClient):
        def __init__(self) -> None:
            super().__init__(
                workspaces=[{"id": "space-1", "name": "Build"}],
                panes=[
                    {
                        "pane_id": "pane-private",
                        "agent": "Agent One",
                        "workspace_id": "space-1",
                        "status": "running",
                    }
                ],
            )
            self.mixed_attempts = 0
            self.subscriptions: list[tuple[str, dict[str, Any]]] = []
            self.closed = 0
            self.connected = 0

        def connect(self) -> None:
            self.connected += 1

        def close(self) -> None:
            self.closed += 1

        def events_subscribe(
            self,
            event_names: Any,
            *,
            timeout: float | None = None,
            event_timeout: float | None = None,
        ) -> Any:
            raise AssertionError("0.7.4 fallback must retain the pane-scoped request")

        def subscribe(
            self,
            method: str,
            params: Mapping[str, Any],
            *,
            timeout: float | None = None,
            event_timeout: float | None = None,
        ) -> Any:
            self.subscriptions.append((method, dict(params)))
            if any(
                item.get("type") == "pane.updated"
                for item in params.get("subscriptions", [])
            ):
                self.mixed_attempts += 1
                raise HerdrErrorResponse(
                    {
                        "code": "invalid_request",
                        "message": "invalid request: unknown variant pane.updated",
                    },
                    "subscribe-1",
                    uncorrelated=True,
                )
            backend.stop_event.set()
            return SimpleNamespace(subscription_id="pane-scoped-sub")

    client = PaneScopedClient()
    backend.client_factory = lambda _config: client

    backend.run_forever()

    assert client.mixed_attempts == 1
    assert client.closed >= 1
    assert client.connected >= 1
    assert len(client.subscriptions) == 2
    method, params = client.subscriptions[1]
    assert method == HERDR_EVENTS_SUBSCRIBE_METHOD
    subscriptions = params["subscriptions"]
    fallback_names = set(HERDR_OFFICIAL_EVENT_NAMES) - {
        "workspace.focused",
        "pane.updated",
        "pane.focused",
        "pane.agent_detected",
        "pane.output_matched",
    }
    assert len(subscriptions) == len(fallback_names)
    assert {item["type"] for item in subscriptions} == fallback_names
    assert {item["pane_id"] for item in subscriptions} == {"pane-private"}


def test_backend_empty_installation_falls_back_to_herdr_074_global_subscription(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, "empty-global-fallback")
    init_store(Path(config.db_path))
    backend = HerdrEventBackend(config, debounce_seconds=0, reconnect_delay_seconds=0)

    class EmptyInstallationClient(_StaticClient):
        def __init__(self) -> None:
            super().__init__()
            self.subscriptions: list[tuple[str, dict[str, Any]]] = []
            self.closed = 0
            self.connected = 0

        def connect(self) -> None:
            self.connected += 1

        def close(self) -> None:
            self.closed += 1

        def subscribe(
            self,
            method: str,
            params: Mapping[str, Any],
            *,
            timeout: float | None = None,
            event_timeout: float | None = None,
        ) -> Any:
            copied = (method, {"subscriptions": [dict(item) for item in params["subscriptions"]]})
            self.subscriptions.append(copied)
            if any(item.get("type") == "pane.updated" for item in params["subscriptions"]):
                raise HerdrErrorResponse(
                    {
                        "code": "invalid_request",
                        "message": "invalid request: unknown variant pane.updated",
                    },
                    "subscribe-1",
                    uncorrelated=True,
                )
            backend.stop_event.set()
            return SimpleNamespace(subscription_id="empty-global-sub")

    client = EmptyInstallationClient()
    backend.client_factory = lambda _config: client

    backend.run_forever()

    assert client.closed >= 1
    assert client.connected >= 1
    assert len(client.subscriptions) == 2
    method, params = client.subscriptions[1]
    assert method == HERDR_EVENTS_SUBSCRIBE_METHOD
    subscriptions = params["subscriptions"]
    assert subscriptions
    assert all(set(item) == {"type"} for item in subscriptions)
    assert {item["type"] for item in subscriptions} == set(HERDR_OFFICIAL_EVENT_NAMES) - {
        "pane.updated"
    }


@pytest.mark.parametrize(
    "failure",
    [
        HerdrErrorResponse(
            {"code": "permission_denied", "message": "subscription denied"},
            "subscribe-1",
        ),
        HerdrErrorResponse(
            {"code": "invalid_request", "message": "invalid request: correlated"},
            "subscribe-1",
        ),
        HerdrEnvelopeError("malformed subscription response"),
    ],
)
def test_backend_reconnects_instead_of_downgrading_unrelated_subscription_failures(
    tmp_path: Path,
    failure: Exception,
) -> None:
    backend = _backend(tmp_path, "no-unrelated-subscription-downgrade")

    class RejectingClient:
        def __init__(self) -> None:
            self.calls = 0
            self.closed = 0
            self.connected = 0

        def subscribe(self, method, params, **kwargs):
            self.calls += 1
            raise failure

        def close(self) -> None:
            self.closed += 1

        def connect(self) -> None:
            self.connected += 1

    client = RejectingClient()
    with pytest.raises(type(failure)):
        backend._subscribe_event_stream(client)
    assert client.calls == 1
    assert client.closed == 0
    assert client.connected == 0












def test_backend_rejects_non_official_subscribe_method(tmp_path: Path) -> None:
    config = _config(tmp_path, "custom-subscribe")
    init_store(Path(config.db_path))

    with pytest.raises(HerdrEventBackendError):
        HerdrEventBackend(config, subscribe_method="custom.subscribe")


def test_pane_agent_detected_official_event_updates_worker_and_private_binding(tmp_path: Path) -> None:
    backend = _backend(tmp_path, "agent-detected")
    backend.reconcile_once(client=_StaticClient(workspaces=[{"id": "space-1", "name": "Build"}]))

    backend.queue_event_envelope(
        {"event": "pane.agent_detected", "data": {
            "agent": {
                "agent_id": "agent-2",
                "name": "Agent Two",
                "workspace_id": "space-1",
                "pane_id": "pane-2",
                "status": "running",
            }
        }}
    )

    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    bindings = list_worker_bindings(backend.db_path, backend.config.host_id, backend="herdr")
    assert snapshot is not None
    assert {worker.id for worker in snapshot.workers} == {"agent-2"}
    assert bindings[0].target_kind == "agent_id"
    assert bindings[0].target_value == "agent-2"
    _assert_no_public_json_forbidden(json.loads(snapshot.to_json()))


@pytest.mark.parametrize(
    "event_name",
    [
        "pane.created",
        "pane.focused",
        "pane.agent_detected",
        "pane.agent_status_changed",
    ],
)
@pytest.mark.parametrize("entity_name", ["agent", "worker"])
def test_supported_nested_agent_or_worker_canonical_fields_cannot_mint_continuity(
    tmp_path: Path,
    event_name: str,
    entity_name: str,
) -> None:
    backend = _backend(
        tmp_path,
        f"nested-no-mint-{event_name}-{entity_name}",
    )
    backend.reconcile_once(
        client=_StaticClient(workspaces=[{"id": "wR9", "name": "Build"}])
    )
    entity = {
        "worker_id": f"public-{entity_name}",
        "agent_id": f"{entity_name}-target-secret",
        "name": "codex",
        "agent": "codex",
        "workspace_id": "wR9",
        "pane_id": "wR9:pA",
        "status": "running",
        "agent_session": {
            "source": "compatibility-secret",
            "agent": "codex",
            "kind": "id",
            "value": "compatibility-session-secret",
        },
    }

    assert backend.queue_event_envelope(
        {"event": event_name, "data": {entity_name: entity}}
    )

    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    assert snapshot is not None
    assert len(snapshot.workers) == 1
    worker = snapshot.workers[0]
    assert "stable_key" not in worker.meta
    assert "stable_key_version" not in worker.meta
    assert not backend.config.installation_key_path.exists()
    _assert_no_public_json_forbidden(json.loads(snapshot.to_json()))




@pytest.mark.parametrize("entity_source", ["top_level", "pane"])
def test_official_pane_tuple_provenance_mints_continuity(
    tmp_path: Path,
    entity_source: str,
) -> None:
    backend = _backend(tmp_path, f"official-pane-{entity_source}")
    backend.reconcile_once(
        client=_StaticClient(workspaces=[{"id": "wR9", "name": "Build"}])
    )
    pane = {
        "agent": "codex",
        "workspace_id": "wR9",
        "pane_id": "wR9:pA",
        "terminal_id": "official-terminal-secret",
        "status": "running",
        "agent_session": {
            "source": "official-source-secret",
            "agent": "codex",
            "kind": "id",
            "value": "official-session-secret",
        },
    }
    payload = pane if entity_source == "top_level" else {"pane": pane}

    assert backend.queue_event_envelope(
        {"event": "pane.created", "data": payload}
    )

    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    assert snapshot is not None
    assert len(snapshot.workers) == 1
    assert snapshot.workers[0].meta["stable_key"].startswith("wsk1_")
    assert snapshot.workers[0].meta["stable_key_version"] == 1
    assert backend.config.installation_key_path.exists()








def test_key_failure_precedes_move_conflict_mutation(tmp_path: Path) -> None:
    backend = _backend(tmp_path, "event-move-conflict-key-failure")
    panes = [
        {
            "workspace_id": "wR9",
            "pane_id": "wR9:pA",
            "agent": "Agent A",
            "status": "running",
        },
        {
            "workspace_id": "wR9",
            "pane_id": "wR9:pB",
            "agent": "Agent B",
            "status": "running",
        },
    ]
    before = backend.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "wR9", "name": "Build"}],
            panes=panes,
        )
    )
    before_bindings = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
    )
    backend.config.installation_key_marker_path.unlink()

    assert backend.queue_event_envelope(
        {"event": "pane.moved", "data": {
            "previous_pane_id": "wR9:pB",
            "pane": {
                "workspace_id": "wR9",
                "pane_id": "wR9:pA",
                "agent": "Agent B",
                "status": "running",
            },
        }}
    )

    after = latest_snapshot(backend.db_path, backend.config.host_id)
    assert after is not None
    assert after.workers == before.workers
    assert after.spaces == before.spaces
    assert after.backend_health[0].status == "degraded"
    assert after.backend_health[0].outcome == "continuity_unavailable"
    assert (
        list_worker_bindings(
            backend.db_path,
            backend.config.host_id,
            backend="herdr",
        )
        == before_bindings
    )




def test_reconcile_retains_authenticated_snapshot_until_installation_key_recovers(
    tmp_path: Path,
) -> None:
    backend = _backend(tmp_path, "reconcile-key-recovery")
    client = _StaticClient(
        workspaces=[{"id": "wR9", "name": "Build"}],
        panes=[
            {
                "workspace_id": "wR9",
                "pane_id": "wR9:pA",
                "terminal_id": "terminal-secret",
                "agent": "codex",
                "status": "running",
            }
        ],
        agents=[
            {
                "worker_id": "public-worker",
                "agent_id": "agent-secret",
                "workspace_id": "wR9",
                "pane_id": "wR9:pA",
                "terminal_id": "terminal-secret",
                "agent": "codex",
                "status": "running",
            }
        ],
    )
    first = backend.reconcile_once(client=client)
    first_bindings = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
    )
    stable_key = first.workers[0].meta["stable_key"]
    marker = backend.config.installation_key_marker_path.read_bytes()
    backend.config.installation_key_marker_path.unlink()

    degraded = backend.reconcile_once(client=client)

    assert degraded.workers == first.workers
    assert degraded.spaces == first.spaces
    assert degraded.backend_health[0].status == "degraded"
    assert degraded.backend_health[0].outcome == "continuity_unavailable"
    assert degraded.backend_health[0].counts == {"spaces": 1, "workers": 1}
    assert (
        list_worker_bindings(
            backend.db_path,
            backend.config.host_id,
            backend="herdr",
        )
        == first_bindings
    )

    previous_max_workers = backend.max_workers
    backend.max_workers = 1
    capped = backend.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "wR9", "name": "Build"}],
            agents=[
                {"worker_id": "agent-a", "agent": "Agent A"},
                {"worker_id": "agent-b", "agent": "Agent B"},
            ],
        )
    )
    assert capped.backend_health[0].outcome == "continuity_unavailable"
    backend.max_workers = previous_max_workers

    backend.config.installation_key_marker_path.write_bytes(marker)
    os.chmod(backend.config.installation_key_marker_path, 0o600)
    recovered = backend.reconcile_once(client=client)

    assert recovered.backend_health[0].status == "healthy"
    assert recovered.workers[0].meta["stable_key"] == stable_key
    recovered_bindings = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
    )

    unmatched = backend.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "wR9", "name": "Build"}],
            panes=[],
            agents=[
                {
                    "worker_id": "public-worker",
                    "agent_id": "agent-secret",
                    "workspace_id": "wR9",
                    "pane_id": "wR9:pA",
                    "terminal_id": "terminal-secret",
                    "agent": "codex",
                    "status": "running",
                }
            ],
        )
    )

    assert unmatched.workers == recovered.workers
    assert unmatched.spaces == recovered.spaces
    assert unmatched.backend_health[0].status == "degraded"
    assert unmatched.backend_health[0].outcome == "continuity_unavailable"
    assert (
        list_worker_bindings(
            backend.db_path,
            backend.config.host_id,
            backend="herdr",
        )
        == recovered_bindings
    )

    recovered_again = backend.reconcile_once(client=client)
    assert recovered_again.backend_health[0].status == "healthy"
    assert recovered_again.workers[0].meta["stable_key"] == stable_key


def test_incomplete_pane_event_does_not_clear_continuity_failure(
    tmp_path: Path,
) -> None:
    backend = _backend(tmp_path, "incomplete-pane-key-recovery")
    pane = {
        "workspace_id": "wR9",
        "pane_id": "wR9:pA",
        "terminal_id": "terminal-secret",
        "agent": "codex",
        "status": "running",
    }
    backend.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "wR9", "name": "Build"}],
            panes=[pane],
        )
    )
    marker = backend.config.installation_key_marker_path.read_bytes()
    backend.config.installation_key_marker_path.unlink()
    assert backend.queue_event_envelope(
        {"event": "pane.focused", "data": {"pane": pane}}
    )
    failed = latest_snapshot(backend.db_path, backend.config.host_id)
    assert failed is not None
    assert failed.backend_health[0].outcome == "continuity_unavailable"

    backend.config.installation_key_marker_path.write_bytes(marker)
    os.chmod(backend.config.installation_key_marker_path, 0o600)
    assert backend.queue_event_envelope(
        {"event": "pane.closed", "data": {"pane": {"pane_id": "wR9:pA"}}}
    )

    incomplete = latest_snapshot(backend.db_path, backend.config.host_id)
    assert incomplete is not None
    assert incomplete.backend_health[0].status == "degraded"
    assert incomplete.backend_health[0].outcome == "continuity_unavailable"


def test_over_cap_authenticated_retry_does_not_clear_continuity_failure(
    tmp_path: Path,
) -> None:
    backend = _backend(tmp_path, "over-cap-key-recovery")
    pane_a = {
        "workspace_id": "wR9",
        "pane_id": "wR9:pA",
        "agent": "Agent A",
        "status": "running",
    }
    pane_b = {
        "workspace_id": "wR9",
        "pane_id": "wR9:pB",
        "agent": "Agent B",
        "status": "running",
    }
    first = backend.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "wR9", "name": "Build"}],
            panes=[pane_a],
        )
    )
    first_bindings = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
    )
    backend.max_workers = 1
    marker = backend.config.installation_key_marker_path.read_bytes()
    backend.config.installation_key_marker_path.unlink()
    assert backend.queue_event_envelope(
        {"event": "pane.created", "data": {"pane": pane_b}}
    )

    backend.config.installation_key_marker_path.write_bytes(marker)
    os.chmod(backend.config.installation_key_marker_path, 0o600)
    assert backend.queue_event_envelope(
        {"event": "pane.created", "data": {"pane": pane_b}}
    )

    after = latest_snapshot(backend.db_path, backend.config.host_id)
    assert after is not None
    assert after.workers == first.workers
    assert after.backend_health[0].outcome == "continuity_unavailable"
    assert (
        list_worker_bindings(
            backend.db_path,
            backend.config.host_id,
            backend="herdr",
        )
        == first_bindings
    )


def test_conflicting_close_does_not_revalidate_continuity(
    tmp_path: Path,
) -> None:
    backend = _backend(tmp_path, "conflicting-close-key-recovery")
    pane_a = {
        "workspace_id": "wR9",
        "pane_id": "wR9:pA",
        "terminal_id": "terminal-a-secret",
        "agent": "Agent A",
        "status": "running",
    }
    pane_b = {
        "workspace_id": "wR9",
        "pane_id": "wR9:pB",
        "terminal_id": "terminal-b-secret",
        "agent": "Agent B",
        "status": "running",
    }
    first = backend.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "wR9", "name": "Build"}],
            panes=[pane_a, pane_b],
        )
    )
    first_bindings = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
    )
    marker = backend.config.installation_key_marker_path.read_bytes()
    backend.config.installation_key_marker_path.unlink()
    assert backend.queue_event_envelope(
        {"event": "pane.focused", "data": {"pane": pane_a}}
    )

    backend.config.installation_key_marker_path.write_bytes(marker)
    os.chmod(backend.config.installation_key_marker_path, 0o600)
    conflicting_close = {
        **pane_a,
        "terminal_id": pane_b["terminal_id"],
    }
    assert backend.queue_event_envelope(
        {"event": "pane.closed", "data": {"pane": conflicting_close}}
    )

    after = latest_snapshot(backend.db_path, backend.config.host_id)
    assert after is not None
    assert after.workers == first.workers
    assert after.backend_health[0].outcome == "continuity_unavailable"
    assert (
        list_worker_bindings(
            backend.db_path,
            backend.config.host_id,
            backend="herdr",
        )
        == first_bindings
    )


def test_conflicting_upsert_preserves_latched_authenticated_state(
    tmp_path: Path,
) -> None:
    backend = _backend(tmp_path, "conflicting-upsert-key-recovery")
    pane_a = {
        "workspace_id": "wR9",
        "pane_id": "wR9:pA",
        "terminal_id": "terminal-a-secret",
        "agent": "Agent A",
        "status": "running",
    }
    pane_b = {
        "workspace_id": "wR9",
        "pane_id": "wR9:pB",
        "terminal_id": "terminal-b-secret",
        "agent": "Agent B",
        "status": "running",
    }
    first = backend.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "wR9", "name": "Build"}],
            panes=[pane_a, pane_b],
        )
    )
    first_bindings = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
    )
    marker = backend.config.installation_key_marker_path.read_bytes()
    backend.config.installation_key_marker_path.unlink()
    assert backend.queue_event_envelope(
        {"event": "pane.focused", "data": {"pane": pane_a}}
    )

    backend.config.installation_key_marker_path.write_bytes(marker)
    os.chmod(backend.config.installation_key_marker_path, 0o600)
    conflicting_pane = {
        **pane_a,
        "terminal_id": pane_b["terminal_id"],
        "status": "blocked",
    }
    assert backend.queue_event_envelope(
        {"event": "pane.focused", "data": {"pane": conflicting_pane}}
    )

    after = latest_snapshot(backend.db_path, backend.config.host_id)
    assert after is not None
    assert after.workers == first.workers
    assert after.backend_health[0].status == "degraded"
    assert after.backend_health[0].outcome == "continuity_unavailable"
    assert (
        list_worker_bindings(
            backend.db_path,
            backend.config.host_id,
            backend="herdr",
        )
        == first_bindings
    )


def test_official_pane_event_generic_id_remains_private_binding_only(tmp_path: Path) -> None:
    backend = _backend(tmp_path, "pane-id-private")
    backend.reconcile_once(client=_StaticClient(workspaces=[{"id": "space-1", "name": "Build"}]))

    assert (
        backend.queue_event_envelope(
            {"event": "pane.created", "data": {
                "id": "pane-secret",
                "agent": "Agent Two",
                "workspace_id": "space-1",
                "status": "running",
            }}
        )
        is True
    )

    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    bindings = list_worker_bindings(backend.db_path, backend.config.host_id, backend="herdr")
    assert snapshot is not None
    public_json = snapshot.to_json()
    assert "pane-secret" not in public_json
    assert {worker.id for worker in snapshot.workers} == {"Agent Two"}
    assert bindings[0].target_kind == "pane_id"
    assert bindings[0].target_value == "pane-secret"
    _assert_no_public_json_forbidden(json.loads(public_json))


def test_unknown_and_malformed_known_events_do_not_mutate_any_public_or_private_state(
    tmp_path: Path,
) -> None:
    backend = _backend(tmp_path, "event-noop")
    backend.reconcile_once(client=_initial_pane_client())
    before = _no_op_state(backend)

    for envelope in (
        {"event": "unknown.future", "data": {"pane_id": "pane-secret", "stdout": "secret"}},
        {"event": "workspace.created", "data": {"pane_id": "pane-secret", "stdout": "secret"}},
        {"event": "workspace.renamed", "data": {"new_name": "secret"}},
        {"event": "worktree.created", "data": {"worktree_id": "worktree-secret", "stderr": "secret"}},
        {"event": "pane.created", "data": {"labels": ["agent"], "argv": ["secret"]}},
        {"event": "pane.agent_detected", "data": []},
        {"event": "pane.agent_status_changed", "data": {"status": "failed", "stderr": "secret"}},
        {"event": "pane.output_matched", "data": {
            "pane_id": "pane-secret",
            "terminal_id": "terminal-secret",
            "stdout": "secret",
            "stderr": "secret",
            "token": "secret",
        }},
    ):
        backend.queue_event_envelope(envelope)

    after = _no_op_state(backend)
    assert after == before
    _assert_no_public_json_forbidden(json.loads(after[0]))
    assert "pane-secret" not in after[0]
    assert "terminal-secret" not in after[0]
    assert "secret" not in after[0]


def test_worktree_events_only_update_existing_workspace_observations(tmp_path: Path) -> None:
    backend = _backend(tmp_path, "worktree-adjacent")
    backend.reconcile_once(client=_StaticClient(workspaces=[{"id": "space-1", "name": "Build"}]))
    before = latest_snapshot(backend.db_path, backend.config.host_id)
    assert before is not None

    assert (
        backend.queue_event_envelope(
            {"event": "worktree.created", "data": {"workspace_id": "new-space", "name": "Should Not Appear"}}
        )
        is True
    )
    unchanged = latest_snapshot(backend.db_path, backend.config.host_id)
    assert unchanged is not None
    assert [space.id for space in unchanged.spaces] == ["space-1"]
    assert unchanged.spaces[0].name == "Build"

    assert (
        backend.queue_event_envelope(
            {"event": "worktree.opened", "data": {
                "workspace_id": "space-1",
                "name": "Build Worktree",
                "status": "active",
            }}
        )
        is True
    )
    updated = latest_snapshot(backend.db_path, backend.config.host_id)
    assert updated is not None
    assert [space.id for space in updated.spaces] == ["space-1"]
    assert updated.spaces[0].name == "Build Worktree"
    _assert_no_public_json_forbidden(json.loads(updated.to_json()))

def test_run_forever_reconnect_accepts_identical_idless_event_again(tmp_path: Path) -> None:
    config = _config(tmp_path, "reconnect-resubscribe")
    init_store(Path(config.db_path))

    class SequenceClient(_StaticClient):
        def __init__(self, label: str, events: list[Any], *, pane_status: str) -> None:
            super().__init__(
                workspaces=[{"id": "space-1", "name": "Build"}],
                panes=[
                    {
                        "pane_id": "pane-1",
                        "agent": "Agent One",
                        "workspace_id": "space-1",
                        "status": pane_status,
                    }
                ],
            )
            self.label = label
            self.events = list(events)
            self.subscriptions: list[tuple[str, dict[str, Any]]] = []
            self.read_calls = 0
            self.closed = False

        def connect(self) -> None:
            return None

        def close(self) -> None:
            self.closed = True

        def subscribe(
            self,
            method: str,
            params: Mapping[str, Any],
            *,
            timeout: float | None = None,
            event_timeout: float | None = None,
        ) -> Any:
            self.subscriptions.append((method, dict(params)))
            return SimpleNamespace(subscription_id=f"{self.label}-sub")

        def read_event(self, subscription_id: str, *, timeout: float | None = None) -> dict[str, Any]:
            self.read_calls += 1
            if not self.events:
                backend.stop_event.set()
                raise HerdrSocketTimeoutError("idle")
            event = self.events.pop(0)
            if event == "disconnect":
                raise HerdrSocketDisconnectedError("disconnect")
            return dict(event)

    working = _status_event("working")
    first = SequenceClient("first", [working, "disconnect"], pane_status="idle")
    second = SequenceClient("second", [working], pane_status="idle")
    clients = [first, second]
    backend = HerdrEventBackend(
        config,
        client_factory=lambda _config: clients.pop(0),
        debounce_seconds=0,
        reconnect_delay_seconds=0,
    )

    backend.run_forever()

    expected = {
        "subscriptions": [
            {"type": name}
            for name in HERDR_OFFICIAL_EVENT_NAMES
            if name not in {"pane.agent_status_changed", "pane.output_matched"}
        ]
        + [{"type": "pane.agent_status_changed", "pane_id": "pane-1"}]
    }
    assert first.subscriptions == [(HERDR_EVENTS_SUBSCRIBE_METHOD, expected)]
    assert second.subscriptions == [(HERDR_EVENTS_SUBSCRIBE_METHOD, expected)]
    assert first.read_calls == 2
    assert second.read_calls == 2
    assert first.closed is True
    assert second.closed is True
    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    assert snapshot is not None
    assert snapshot.workers[0].status == "active"


def test_run_forever_retains_complete_reconcile_when_event_stream_disconnects(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, "reconcile-before-stream-disconnect")
    init_store(Path(config.db_path))

    class DisconnectingClient(_StaticClient):
        def __init__(self) -> None:
            super().__init__(workspaces=[{"id": "space-1", "name": "Build"}])

        def connect(self) -> None:
            return None

        def close(self) -> None:
            return None

        def subscribe(
            self,
            method: str,
            params: Mapping[str, Any],
            *,
            timeout: float | None = None,
            event_timeout: float | None = None,
        ) -> Any:
            return SimpleNamespace(subscription_id="disconnect-sub")

        def read_event(
            self,
            subscription_id: str,
            *,
            timeout: float | None = None,
        ) -> dict[str, Any]:
            backend.stop_event.set()
            raise HerdrSocketDisconnectedError("event stream closed")

    backend = HerdrEventBackend(
        config,
        client_factory=lambda _config: DisconnectingClient(),
        debounce_seconds=0,
        reconnect_delay_seconds=0,
    )

    backend.run_forever()

    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    assert snapshot is not None
    assert snapshot.backend_health[0].status == "healthy"
    assert snapshot.backend_health[0].outcome == "healthy_non_empty"


def test_start_stop_are_idempotent_and_bounded_for_idle_subscription(tmp_path: Path) -> None:
    config = Config(
        host_id="bounded-stop",
        data_dir=tmp_path,
        db_path=tmp_path / "bounded-stop.db",
        herdr_backend="socket",
        herdr_timeout_seconds=0.05,
    )
    init_store(Path(config.db_path))

    class IdleClient(_StaticClient):
        def __init__(self) -> None:
            super().__init__()
            self.subscriptions = 0

        def connect(self) -> None:
            return None

        def close(self) -> None:
            return None

        def subscribe(
            self,
            method: str,
            params: Mapping[str, Any],
            *,
            timeout: float | None = None,
            event_timeout: float | None = None,
        ) -> Any:
            self.subscriptions += 1
            return SimpleNamespace(subscription_id="idle-sub")

        def read_event(self, subscription_id: str, *, timeout: float | None = None) -> dict[str, Any]:
            raise HerdrSocketTimeoutError("idle")

    client = IdleClient()
    backend = HerdrEventBackend(
        config,
        client_factory=lambda _config: client,
        debounce_seconds=0,
        reconnect_delay_seconds=0,
    )

    started = time.monotonic()
    backend.start(wait_for_reconcile=True, timeout_seconds=0.2)
    deadline = time.monotonic() + 0.5
    while client.subscriptions < 1 and time.monotonic() < deadline:
        time.sleep(0.01)
    assert backend.ready is True
    assert client.subscriptions >= 1

    backend.stop()
    backend.stop()

    assert backend.running is False
    assert time.monotonic() - started < 2.0


def test_start_raises_instead_of_proceeding_with_stale_state_when_not_ready(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    config = _config(tmp_path, "initial-reconcile-timeout")
    init_store(Path(config.db_path))
    save_snapshot(
        Path(config.db_path),
        project_from_observations(
            config,
            workers=[Worker(id="stale-worker", name="Stale Worker", status="waiting")],
        ),
    )
    backend = HerdrEventBackend(
        config,
        debounce_seconds=0,
        reconnect_delay_seconds=0,
    )
    assert latest_snapshot(backend.db_path, backend.config.host_id) is not None

    def wait_until_stopped() -> None:
        backend.stop_event.wait()

    monkeypatch.setattr(backend, "run_forever", wait_until_stopped)

    try:
        with pytest.raises(
            HerdrSocketTimeoutError,
            match="initial Herdr reconciliation timed out",
        ):
            backend.start(wait_for_reconcile=True, timeout_seconds=0.01)

        assert backend.ready is False
        assert backend.running is False
        assert backend._thread is None
    finally:
        backend.stop()


def test_start_defaults_to_dedicated_initial_reconcile_timeout(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    config = Config(
        host_id="initial-reconcile-budget",
        data_dir=tmp_path,
        db_path=tmp_path / "initial-reconcile-budget.db",
        herdr_backend="socket",
        herdr_timeout_seconds=0.25,
        herdr_initial_reconcile_timeout_seconds=42,
    )
    backend = HerdrEventBackend(config, debounce_seconds=0, reconnect_delay_seconds=0)
    waited: list[float] = []

    class NeverReady:
        def clear(self) -> None:
            return None

        def is_set(self) -> bool:
            return False

        def wait(self, timeout: float | None = None) -> bool:
            assert timeout is not None
            waited.append(timeout)
            return False

    backend._ready = NeverReady()  # type: ignore[assignment]
    monkeypatch.setattr(backend, "run_forever", backend.stop_event.wait)

    try:
        with pytest.raises(HerdrSocketTimeoutError):
            backend.start(wait_for_reconcile=True)
        assert waited == [42.0]
        assert config.herdr_timeout_seconds == 0.25
    finally:
        backend.stop()




@pytest.mark.parametrize("batched", [False, True], ids=["one-flush-per-event", "one-batch"])
def test_real_idless_working_idle_working_preserves_every_transition(
    tmp_path: Path,
    monkeypatch: Any,
    batched: bool,
) -> None:
    backend = _backend(
        tmp_path,
        f"real-idless-transitions-{batched}",
        debounce_seconds=60 if batched else 0,
    )
    backend.reconcile_once(client=_initial_pane_client())
    bindings_before = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
    )
    applied: list[str] = []
    original_apply = backend._apply_event

    def recording_apply(event: Any) -> bool:
        applied.append(str(event.payload["status"]))
        return original_apply(event)

    monkeypatch.setattr(backend, "_apply_event", recording_apply)
    observed_statuses: list[str] = []
    accepted = []
    for status in ("working", "idle", "working"):
        accepted.append(backend.queue_event_envelope(_status_event(status), flush=not batched))
        if not batched:
            snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
            assert snapshot is not None
            observed_statuses.append(snapshot.workers[0].status)
    if batched:
        backend.flush()

    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    bindings_after = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
    )
    assert snapshot is not None
    assert accepted == [True, True, True]
    assert applied == ["working", "idle", "working"]
    if not batched:
        assert observed_statuses == ["active", "idle", "active"]
    assert snapshot.workers[0].status == "active"
    assert len(snapshot.workers) == 1
    assert bindings_after == bindings_before


@pytest.mark.parametrize("batched", [False, True], ids=["separate-flushes", "one-batch"])
def test_adjacent_idless_duplicates_do_not_duplicate_persisted_effects(
    tmp_path: Path,
    batched: bool,
) -> None:
    backend = _backend(
        tmp_path,
        f"idless-repeat-effects-{batched}",
        debounce_seconds=60 if batched else 0,
    )
    backend.reconcile_once(client=_initial_pane_client())
    before = _persisted_event_effect_counts(backend)
    blocked = _status_event("blocked")

    assert backend.queue_event_envelope(blocked, flush=not batched) is True
    if batched:
        assert backend.queue_event_envelope(blocked, flush=False) is True
        backend.flush()
        after = _persisted_event_effect_counts(backend)
    else:
        after_first = _persisted_event_effect_counts(backend)
        assert backend.queue_event_envelope(blocked, flush=True) is True
        after = _persisted_event_effect_counts(backend)
        assert after == after_first

    assert after["snapshots"] == before["snapshots"] + 1
    assert after["events"] == before["events"] + 1
    assert after["workers"] == before["workers"] == 1
    assert after["worker_bindings"] == before["worker_bindings"] == 1
    assert after["attention_items"] == before["attention_items"] + 1
    assert after["connector_outbox"] == before["connector_outbox"] + 1


def test_snapshot_observation_context_matches_each_herdr_persistence_barrier(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    calls: list[SnapshotObservationContext] = []
    binding_calls: list[tuple[Any, str | None, bool, bool]] = []
    original_save_snapshot = save_snapshot

    def recording_save_snapshot(
        db_path: Path,
        snapshot: Any,
        *,
        turn_model: str,
        observation: SnapshotObservationContext | None = None,
        worker_bindings: Any = None,
        binding_backend: str | None = None,
        binding_observation_authoritative: bool = False,
        binding_workers_present: bool = True,
    ) -> bool:
        assert observation is not None
        calls.append(observation)
        binding_calls.append(
            (
                worker_bindings,
                binding_backend,
                binding_observation_authoritative,
                binding_workers_present,
            )
        )
        return original_save_snapshot(
            db_path,
            snapshot,
            turn_model=turn_model,
            observation=observation,
            worker_bindings=worker_bindings,
            binding_backend=binding_backend,
            binding_observation_authoritative=binding_observation_authoritative,
            binding_workers_present=binding_workers_present,
        )

    monkeypatch.setattr(
        "tendwire.backends.herdr_events.save_snapshot",
        recording_save_snapshot,
    )
    _set_observation_time(monkeypatch, "2026-01-01T00:00:00Z")
    backend = _backend(tmp_path, "herdr-observation-context")

    backend.reconcile_once(client=_initial_pane_client())
    _set_observation_time(monkeypatch, "2026-01-01T00:00:10Z")
    backend.queue_event_envelope(_status_event("blocked"))
    _set_observation_time(monkeypatch, "2026-01-01T00:00:20Z")
    backend._mark_worker_cap_exceeded_locked(999)
    _set_observation_time(monkeypatch, "2026-01-01T00:00:30Z")
    backend._mark_unhealthy("socket_disconnected")

    assert [(call.authority, call.observed_at) for call in calls] == [
        ("complete", "2026-01-01T00:00:00Z"),
        ("positive", "2026-01-01T00:00:10Z"),
        ("none", "2026-01-01T00:00:20Z"),
        ("none", "2026-01-01T00:00:30Z"),
    ]
    assert binding_calls[0][0]
    assert binding_calls[0][1:] == ("herdr", True, True)
    assert all(call[0] is None for call in binding_calls[1:])

def test_event_snapshot_expiry_serializes_delayed_older_observer(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    from tendwire.store import sqlite as store_sqlite

    backend = _backend(tmp_path, "herdr-atomic-expiry")
    worker = Worker(id="worker-expiry", name="Worker Expiry", status="active")

    def binding(target: str, observed_at: str, fingerprint: str) -> WorkerBinding:
        return WorkerBinding(
            host_id=backend.config.host_id,
            worker_id=worker.id,
            worker_fingerprint=worker.fingerprint,
            backend="herdr",
            target_kind="terminal_id",
            target_value=target,
            sendable=True,
            observed_at=observed_at,
            private_fingerprint=fingerprint,
        )

    initial = Snapshot(
        host_id=backend.config.host_id,
        updated_at="2026-01-01T00:00:00+00:00",
        workers=[worker],
    )
    init_store(backend.db_path)
    assert save_snapshot(
        backend.db_path,
        initial,
        worker_bindings=[
            binding(
                "initial-private-target",
                "2026-01-01T00:00:00+00:00",
                "initial-private-owner",
            )
        ],
        binding_backend="herdr",
        binding_observation_authoritative=True,
    ) is True
    older = Snapshot(
        host_id=backend.config.host_id,
        updated_at="2026-01-01T00:00:01+00:00",
        workers=[],
    )
    newer = Snapshot(
        host_id=backend.config.host_id,
        updated_at="2026-01-01T00:00:02+00:00",
        workers=[worker],
    )
    newer_binding = binding(
        "newer-private-target",
        "2026-01-01T00:00:02+00:00",
        "newer-private-owner",
    )
    original_expire = store_sqlite._expire_stale_worker_bindings_conn
    older_inside_transaction = threading.Event()
    release_older = threading.Event()

    def delayed_expire(conn: sqlite3.Connection, host_id: str, **kwargs: Any) -> int:
        if kwargs.get("now") == "2026-01-01T00:00:01+00:00":
            older_inside_transaction.set()
            assert release_older.wait(timeout=10)
        return original_expire(conn, host_id, **kwargs)

    monkeypatch.setattr(
        store_sqlite,
        "_expire_stale_worker_bindings_conn",
        delayed_expire,
    )
    errors: list[BaseException] = []

    def save_older() -> None:
        try:
            backend._save_snapshot(
                older,
                observation=SnapshotObservationContext(
                    authority="complete",
                    observed_at=older.updated_at,
                ),
                worker_bindings=[],
                binding_observation_authoritative=True,
                binding_workers_present=False,
            )
        except BaseException as exc:
            errors.append(exc)

    def save_newer() -> None:
        try:
            backend._save_snapshot(
                newer,
                observation=SnapshotObservationContext(
                    authority="complete",
                    observed_at=newer.updated_at,
                ),
                worker_bindings=[newer_binding],
                binding_observation_authoritative=True,
                binding_workers_present=True,
            )
        except BaseException as exc:
            errors.append(exc)

    older_thread = threading.Thread(target=save_older)
    newer_thread = threading.Thread(target=save_newer)
    older_thread.start()
    assert older_inside_transaction.wait(timeout=10)
    newer_thread.start()
    time.sleep(0.05)
    release_older.set()
    older_thread.join(timeout=10)
    newer_thread.join(timeout=10)

    assert not errors
    assert not older_thread.is_alive() and not newer_thread.is_alive()
    active = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
        now="2026-01-01T00:00:03+00:00",
    )
    assert len(active) == 1
    assert active[0].target_value == "newer-private-target"


def test_same_fingerprint_observations_refresh_attention_and_run_bounded_cadence(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    config = Config(
        host_id="same-fingerprint-cadence",
        data_dir=tmp_path,
        db_path=tmp_path / "same-fingerprint-cadence.db",
        herdr_backend="socket",
        snapshot_retention_days=14,
        snapshot_retention_count=1,
        snapshot_maintenance_batch_size=100,
        store_maintenance_cadence_seconds=3600,
        acknowledged_final_retention_days=33,
        acknowledged_final_retention_count=456,
        command_retry_horizon_seconds=120,
        command_receipt_retention_seconds=691_200,
        command_receipt_retention_count=77,
    )
    init_store(Path(config.db_path))
    maintenance_times = iter(
        (
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:00:10Z",
            "2026-01-01T00:00:20Z",
            "2026-01-01T01:00:10Z",
        )
    )
    maintenance_calls: list[
        tuple[SnapshotRetentionPolicy, int, int, int, int, int, int, dict[str, Any]]
    ] = []

    def fixed_clock_maintenance(
        db_path: Path,
        *,
        policy: SnapshotRetentionPolicy,
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
        result = maybe_run_automatic_store_maintenance(
            db_path,
            policy=policy,
            turn_model=turn_model,
            cadence_seconds=cadence_seconds,
            acknowledged_final_retention_days=acknowledged_final_retention_days,
            acknowledged_final_retention_count=acknowledged_final_retention_count,
            command_retry_horizon_seconds=command_retry_horizon_seconds,
            command_receipt_retention_seconds=command_receipt_retention_seconds,
            command_receipt_retention_count=command_receipt_retention_count,
            now=next(maintenance_times),
        )
        maintenance_calls.append(
            (
                policy,
                acknowledged_final_retention_days,
                acknowledged_final_retention_count,
                command_retry_horizon_seconds,
                command_receipt_retention_seconds,
                command_receipt_retention_count,
                cadence_seconds,
                result,
            )
        )
        return result

    monkeypatch.setattr(
        "tendwire.backends.herdr_events.maybe_run_automatic_store_maintenance",
        fixed_clock_maintenance,
    )
    saved_fingerprints: list[str] = []

    def recording_save(
        db_path: Path,
        snapshot: Any,
        *,
        turn_model: str,
        observation: SnapshotObservationContext | None = None,
        worker_bindings: Any = None,
        binding_backend: str | None = None,
        binding_observation_authoritative: bool = False,
        binding_workers_present: bool = True,
    ) -> bool:
        saved_fingerprints.append(snapshot.content_fingerprint)
        return save_snapshot(
            db_path,
            snapshot,
            turn_model=turn_model,
            observation=observation,
            worker_bindings=worker_bindings,
            binding_backend=binding_backend,
            binding_observation_authoritative=binding_observation_authoritative,
            binding_workers_present=binding_workers_present,
        )

    monkeypatch.setattr("tendwire.backends.herdr_events.save_snapshot", recording_save)
    backend = HerdrEventBackend(config, debounce_seconds=0)

    _set_observation_time(monkeypatch, "2026-01-01T00:00:00Z")
    backend.reconcile_once(client=_initial_pane_client())
    _set_observation_time(monkeypatch, "2026-01-01T00:00:10Z")
    assert backend.queue_event_envelope(_status_event("blocked")) is True
    assert _attention_lifecycle_rows(backend)[0][4] == "2026-01-01T00:00:10+00:00"

    _set_observation_time(monkeypatch, "2026-01-01T00:00:20Z")
    backend._persist_current_state(observed_at="2026-01-01T00:00:20Z")
    assert _attention_lifecycle_rows(backend)[0][4] == "2026-01-01T00:00:20+00:00"

    _set_observation_time(monkeypatch, "2026-01-01T01:00:10Z")
    backend._persist_current_state(observed_at="2026-01-01T01:00:10Z")

    assert len(maintenance_calls) == 4
    assert len(saved_fingerprints) == 4
    assert len(set(saved_fingerprints[1:])) == 1
    assert [result["status"] for *_, result in maintenance_calls] == [
        "ok",
        "not_due",
        "not_due",
        "ok",
    ]
    assert sum(bool(result["due"]) for *_, result in maintenance_calls) == 2
    assert {
        (
            policy.retention_days,
            policy.retention_count,
            policy.batch_size,
            final_days,
            final_count,
            retry_horizon,
            retention_seconds,
            retention_count,
            cadence,
        )
        for (
            policy,
            final_days,
            final_count,
            retry_horizon,
            retention_seconds,
            retention_count,
            cadence,
            _,
        ) in maintenance_calls
    } == {(14, 1, 100, 33, 456, 120, 691_200, 77, 3600)}
    assert _table_count(backend.db_path, config.host_id, "snapshots") == 1
    assert _attention_lifecycle_rows(backend)[0][4] == "2026-01-01T01:00:10+00:00"
    assert backend.operational_status["automatic_maintenance"] == {
        "ok": True,
        "status": "ok",
        "due": True,
        "examined": 1,
        "deleted": 1,
        "remaining_candidates": False,
    }


def test_maintenance_failure_cannot_undo_committed_backend_snapshot(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    backend = _backend(tmp_path, "maintenance-failure-contained")

    def fail_maintenance(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        raise RuntimeError(f"private maintenance failure at {tmp_path}/secret.db")

    monkeypatch.setattr(
        "tendwire.backends.herdr_events.maybe_run_automatic_store_maintenance",
        fail_maintenance,
    )

    snapshot = backend.reconcile_once(client=_initial_pane_client())
    persisted = latest_snapshot(backend.db_path, backend.config.host_id)
    status = backend.operational_status
    encoded = json.dumps(status, sort_keys=True)

    assert persisted is not None
    assert persisted.content_fingerprint == snapshot.content_fingerprint
    assert status["automatic_maintenance"] == {
        "ok": False,
        "status": "failed",
        "due": False,
        "examined": 0,
        "deleted": 0,
        "remaining_candidates": False,
    }
    assert str(tmp_path) not in encoded
    assert "secret.db" not in encoded
    _assert_no_public_json_forbidden(status)


def test_incremental_positive_lifecycle_and_complete_absence_are_separate(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    _set_observation_time(monkeypatch, "2026-01-01T00:00:00Z")
    backend = _backend(tmp_path, "incremental-lifecycle")
    backend.reconcile_once(client=_initial_pane_client())
    assert _attention_lifecycle_rows(backend) == ()

    _set_observation_time(monkeypatch, "2026-01-01T00:00:10Z")
    backend.queue_event_envelope(_status_event("blocked"))
    opened = _attention_lifecycle_rows(backend)
    assert len(opened) == 1
    assert opened[0][0:2] == (1, "open")
    assert opened[0][5:7] == (None, 0)
    initial_rank = int(opened[0][9])
    assert _attention_event_types(backend) == ["attention_created"]

    _set_observation_time(monkeypatch, "2026-01-01T00:00:20Z")
    backend.queue_event_envelope(_status_event("failed"))
    escalated = _attention_lifecycle_rows(backend)
    assert len(escalated) == 1
    assert escalated[0][0:2] == (1, "open")
    assert int(escalated[0][9]) > initial_rank
    assert _attention_event_types(backend) == [
        "attention_created",
        "attention_escalated",
    ]
    current = list_attention_items(backend.db_path, backend.config.host_id)
    assert len(current) == 1
    assert current[0]["severity"] == "critical"

    _set_observation_time(monkeypatch, "2026-01-01T00:00:30Z")
    backend.reconcile_once(client=_initial_pane_client())
    first_missing = _attention_lifecycle_rows(backend)
    assert first_missing[0][0:2] == (1, "open")
    assert first_missing[0][5] is not None
    assert first_missing[0][6] == 1

    _set_observation_time(monkeypatch, "2026-01-01T00:00:40Z")
    backend.queue_event_envelope(_status_event("blocked"))
    cleared = _attention_lifecycle_rows(backend)
    assert cleared[0][0:2] == (1, "open")
    assert cleared[0][5:7] == (None, 0)
    assert _attention_event_types(backend) == [
        "attention_created",
        "attention_escalated",
    ]

    _set_observation_time(monkeypatch, "2026-01-01T00:00:50Z")
    backend.queue_event_envelope(_status_event("idle"))
    assert _attention_lifecycle_rows(backend) == cleared

    _set_observation_time(monkeypatch, "2026-01-01T00:01:40Z")
    backend.reconcile_once(client=_initial_pane_client())
    pending = _attention_lifecycle_rows(backend)
    assert pending[0][0:2] == (1, "open")
    assert pending[0][5] is not None
    assert pending[0][6] == 1
    pending_since = pending[0][5]

    _set_observation_time(monkeypatch, "2026-01-01T00:03:40Z")
    backend.reconcile_once(client=_initial_pane_client())
    resolved = _attention_lifecycle_rows(backend)
    assert resolved[0][0:3] == (1, "resolved", None)
    assert resolved[0][5:7] == (pending_since, 2)
    assert list_attention_items(backend.db_path, backend.config.host_id) == []
    assert _attention_event_types(backend) == [
        "attention_created",
        "attention_escalated",
    ]


def test_non_authoritative_herdr_saves_do_not_advance_pending_absence(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    _set_observation_time(monkeypatch, "2026-01-01T00:00:00Z")
    backend = _backend(tmp_path, "non-authoritative-lifecycle")
    backend.reconcile_once(client=_initial_pane_client())
    _set_observation_time(monkeypatch, "2026-01-01T00:00:10Z")
    backend.queue_event_envelope(_status_event("blocked"))
    _set_observation_time(monkeypatch, "2026-01-01T00:01:40Z")
    backend.reconcile_once(client=_initial_pane_client())

    pending = _attention_lifecycle_rows(backend)
    outbox = _attention_event_types(backend)
    assert pending[0][5] is not None
    assert pending[0][6] == 1

    for timestamp, outcome in (
        ("2026-01-01T00:03:40Z", "socket_disconnected"),
        ("2026-01-01T00:04:40Z", "protocol_error"),
        ("2026-01-01T00:05:40Z", "continuity_unavailable"),
    ):
        _set_observation_time(monkeypatch, timestamp)
        backend._mark_unhealthy(outcome)
        assert _attention_lifecycle_rows(backend) == pending
        assert _attention_event_types(backend) == outbox

    failed_worker = backend._workers[next(iter(backend._workers))]
    backend._workers[failed_worker.id] = Worker(
        id=failed_worker.id,
        name=failed_worker.name,
        status="failed",
        space_id=failed_worker.space_id,
        meta=failed_worker.meta,
        last_seen_at=failed_worker.last_seen_at,
        summary=failed_worker.summary,
        backend_target=failed_worker.backend_target,
    )
    _set_observation_time(monkeypatch, "2026-01-01T00:06:40Z")
    backend._persist_current_state(observed_at="2026-01-01T00:06:40Z")
    assert _attention_lifecycle_rows(backend) == pending
    assert _attention_event_types(backend) == outbox

    _set_observation_time(monkeypatch, "2026-01-01T00:07:40Z")
    backend._mark_worker_cap_exceeded_locked(999)
    assert _attention_lifecycle_rows(backend) == pending
    assert _attention_event_types(backend) == outbox


def test_event_flush_and_direct_persistence_use_the_same_lifecycle_executor(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    def exercise(host_id: str, *, direct: bool) -> tuple[Any, ...]:
        _set_observation_time(monkeypatch, "2026-01-01T00:00:00Z")
        backend = _backend(tmp_path, host_id)
        backend.reconcile_once(client=_initial_pane_client())
        for timestamp, status in (
            ("2026-01-01T00:00:10Z", "blocked"),
            ("2026-01-01T00:00:20Z", "failed"),
        ):
            _set_observation_time(monkeypatch, timestamp)
            envelope = _status_event(status)
            if direct:
                event = normalize_event(envelope)
                assert event is not None
                assert backend._apply_event(event) is True
                backend._persist_current_state(observed_at=timestamp)
            else:
                assert backend.queue_event_envelope(envelope) is True
        lifecycle = _attention_lifecycle_rows(backend)
        public = list_attention_items(backend.db_path, backend.config.host_id)
        return (
            lifecycle[0][0],
            lifecycle[0][1],
            lifecycle[0][5],
            lifecycle[0][6],
            lifecycle[0][9],
            _attention_event_types(backend),
            public[0]["severity"],
            public[0]["status"],
            public[0]["lifecycle_status"],
        )

    assert exercise("event-lifecycle-path", direct=False) == exercise(
        "direct-lifecycle-path",
        direct=True,
    )


def test_restart_preserves_pending_absence_until_complete_confirmation(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    _set_observation_time(monkeypatch, "2026-01-01T00:00:00Z")
    backend = _backend(tmp_path, "restart-pending-lifecycle")
    backend.reconcile_once(client=_initial_pane_client())
    _set_observation_time(monkeypatch, "2026-01-01T00:00:10Z")
    backend.queue_event_envelope(_status_event("blocked"))
    _set_observation_time(monkeypatch, "2026-01-01T00:01:40Z")
    backend.reconcile_once(client=_initial_pane_client())
    pending = _attention_lifecycle_rows(backend)
    assert pending[0][0:2] == (1, "open")
    assert pending[0][5] is not None
    assert pending[0][6] == 1
    pending_since = pending[0][5]

    restarted = HerdrEventBackend(
        backend.config,
        debounce_seconds=0,
        reconnect_delay_seconds=0,
    )
    assert _attention_lifecycle_rows(restarted) == pending

    _set_observation_time(monkeypatch, "2026-01-01T00:03:40Z")
    restarted.reconcile_once(client=_initial_pane_client())
    resolved = _attention_lifecycle_rows(restarted)
    assert resolved[0][0:3] == (1, "resolved", None)
    assert resolved[0][5:7] == (pending_since, 2)
    assert _attention_event_types(restarted) == ["attention_created"]



@pytest.mark.parametrize("identity_kind", ["event_id", "producer_sequence"])
def test_forward_compatible_producer_identity_dedupes_retries_with_bounded_lru(
    tmp_path: Path,
    identity_kind: str,
) -> None:
    config = _config(tmp_path, f"producer-dedupe-{identity_kind}")
    init_store(Path(config.db_path))
    backend = HerdrEventBackend(
        config,
        debounce_seconds=0,
        reconnect_delay_seconds=0,
        dedupe_size=2,
    )
    backend.reconcile_once(client=_initial_pane_client())

    def identity(value: int) -> dict[str, Any]:
        if identity_kind == "event_id":
            return {"event_id": f"event-{value}"}
        return {"server_id": "server-1", "sequence": value}

    first = {**_status_event("blocked"), **identity(1)}
    retry = {**_status_event("failed"), **identity(1)}
    assert backend.queue_event_envelope(first) is True
    assert backend.queue_event_envelope(retry) is False
    assert backend.queue_event_envelope({**_status_event("idle"), **identity(2)}) is True
    assert backend.queue_event_envelope({**_status_event("working"), **identity(3)}) is True
    assert len(backend._producer_dedupe) == 2
    assert backend.queue_event_envelope({**_status_event("waiting"), **identity(1)}) is True
    assert len(backend._producer_dedupe) == 2

    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    assert snapshot is not None
    assert snapshot.workers[0].status == "waiting"


def test_pending_producer_identity_is_not_committed_before_successful_flush(
    tmp_path: Path,
) -> None:
    backend = _backend(tmp_path, "pending-producer-dedupe", debounce_seconds=60)
    backend.reconcile_once(client=_initial_pane_client())
    identity = HerdrEventId("pending-event")
    first = {**_status_event("blocked"), "event_id": identity.value}
    duplicate = {**_status_event("failed"), "event_id": identity.value}

    assert backend.queue_event_envelope(first, flush=False) is True
    assert backend._producer_dedupe == {}
    assert len(backend._pending_events) == 1
    assert backend.queue_event_envelope(duplicate, flush=False) is False
    assert backend._producer_dedupe == {}
    assert len(backend._pending_events) == 1

    backend.flush()

    assert list(backend._producer_dedupe) == [identity]
    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    assert snapshot is not None
    assert snapshot.workers[0].status == "blocked"


def test_continuity_failure_leaves_producer_identity_retryable(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    backend = _backend(tmp_path, "continuity-producer-retry")
    backend.reconcile_once(client=_initial_pane_client())
    identity = HerdrEventId("continuity-retry")
    envelope = {**_status_event("blocked"), "event_id": identity.value}
    original_apply = backend._apply_event
    fail_next = True

    def fail_once(event: Any) -> bool:
        nonlocal fail_next
        if fail_next:
            fail_next = False
            raise HerdrContinuityUnavailableError("continuity unavailable")
        return original_apply(event)

    monkeypatch.setattr(backend, "_apply_event", fail_once)

    assert backend.queue_event_envelope(envelope) is True
    assert identity not in backend._producer_dedupe
    failed = latest_snapshot(backend.db_path, backend.config.host_id)
    assert failed is not None
    assert failed.workers[0].status == "active"
    assert failed.backend_health[0].outcome == "continuity_unavailable"

    assert backend.queue_event_envelope(envelope) is True
    recovered = latest_snapshot(backend.db_path, backend.config.host_id)
    assert recovered is not None
    assert recovered.workers[0].status == "blocked"
    assert identity in backend._producer_dedupe
    assert backend.queue_event_envelope(envelope) is False


def test_snapshot_failure_leaves_identity_retryable_and_retry_persists_dirty_state(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    backend = _backend(tmp_path, "snapshot-producer-retry")
    backend.reconcile_once(client=_initial_pane_client())
    identity = HerdrEventId("snapshot-retry")
    envelope = {**_status_event("blocked"), "event_id": identity.value}
    original_persist = backend._persist_current_state
    fail_next = True

    def fail_once(*, observed_at: str | None = None) -> Any:
        nonlocal fail_next
        if fail_next:
            fail_next = False
            raise RuntimeError("snapshot unavailable")
        return original_persist(observed_at=observed_at)

    monkeypatch.setattr(backend, "_persist_current_state", fail_once)

    with pytest.raises(RuntimeError, match="snapshot unavailable"):
        backend.queue_event_envelope(envelope)
    assert identity not in backend._producer_dedupe
    stale = latest_snapshot(backend.db_path, backend.config.host_id)
    assert stale is not None
    assert stale.workers[0].status == "active"
    assert backend._workers[stale.workers[0].id].status == "blocked"

    assert backend.queue_event_envelope(envelope) is True
    persisted = latest_snapshot(backend.db_path, backend.config.host_id)
    assert persisted is not None
    assert persisted.workers[0].status == "blocked"
    assert identity in backend._producer_dedupe
    assert backend.queue_event_envelope(envelope) is False


def test_concurrent_queueing_applies_events_in_backend_lock_order(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    backend = _backend(tmp_path, "concurrent-event-order", debounce_seconds=60)
    backend.reconcile_once(client=_initial_pane_client())
    first_queued = threading.Event()
    idle_queued = threading.Event()
    accepted: dict[str, bool] = {}
    applied: list[str] = []
    original_apply = backend._apply_event

    def recording_apply(event: Any) -> bool:
        applied.append(str(event.payload["status"]))
        return original_apply(event)

    monkeypatch.setattr(backend, "_apply_event", recording_apply)

    def queue_working_events() -> None:
        accepted["first"] = backend.queue_event_envelope(_status_event("working"), flush=False)
        first_queued.set()
        if idle_queued.wait(1):
            accepted["last"] = backend.queue_event_envelope(_status_event("working"), flush=False)

    def queue_idle_event() -> None:
        if first_queued.wait(1):
            accepted["middle"] = backend.queue_event_envelope(_status_event("idle"), flush=False)
            idle_queued.set()

    working_thread = threading.Thread(target=queue_working_events)
    idle_thread = threading.Thread(target=queue_idle_event)
    working_thread.start()
    idle_thread.start()
    working_thread.join(timeout=1)
    idle_thread.join(timeout=1)

    assert working_thread.is_alive() is False
    assert idle_thread.is_alive() is False
    assert accepted == {"first": True, "middle": True, "last": True}
    backend.flush()

    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    assert snapshot is not None
    assert applied == ["working", "idle", "working"]
    assert snapshot.workers[0].status == "active"
    assert len(snapshot.workers) == 1
    assert len(list_worker_bindings(backend.db_path, backend.config.host_id, backend="herdr")) == 1


















def test_pane_moved_preserves_public_worker_and_updates_private_binding(tmp_path: Path) -> None:
    backend = _backend(tmp_path, "pane-moved")
    backend.reconcile_once(client=_initial_pane_client())
    before = latest_snapshot(backend.db_path, backend.config.host_id)
    assert before is not None
    worker_id = before.workers[0].id
    binding = list_worker_bindings(backend.db_path, backend.config.host_id, backend="herdr")[0]

    backend.queue_event_envelope(
        {"event": "pane.moved", "data": {
            "old_pane_id": "pane-1",
            "pane_id": "pane-2",
            "agent": "Agent One",
            "workspace_id": "space-1",
        }}
    )

    after = latest_snapshot(backend.db_path, backend.config.host_id)
    moved_binding = list_worker_bindings(backend.db_path, backend.config.host_id, backend="herdr")[0]
    assert after is not None
    assert after.workers[0].id == worker_id
    assert moved_binding.private_fingerprint == binding.private_fingerprint
    assert moved_binding.target_kind == "pane_id"
    assert moved_binding.target_value == "pane-2"


def test_pane_closed_closes_worker_and_expires_matching_binding(tmp_path: Path) -> None:
    backend = _backend(tmp_path, "pane-closed")
    backend.reconcile_once(client=_initial_pane_client())

    backend.queue_event_envelope({"event": "pane.closed", "data": {"pane_id": "pane-1"}})

    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    assert snapshot is not None
    assert snapshot.workers[0].status == "closed"
    assert list_worker_bindings(backend.db_path, backend.config.host_id, backend="herdr") == []
    expired = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
        include_expired=True,
    )
    assert expired[0].sendable is False
    assert expired[0].reason == "pane_closed"

def test_pane_exited_closes_worker_and_expires_matching_binding(tmp_path: Path) -> None:
    backend = _backend(tmp_path, "pane-exited")
    backend.reconcile_once(client=_initial_pane_client())

    backend.queue_event_envelope({"event": "pane.exited", "data": {"pane_id": "pane-1"}})

    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    assert snapshot is not None
    assert snapshot.workers[0].status == "closed"
    assert list_worker_bindings(backend.db_path, backend.config.host_id, backend="herdr") == []
    expired = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
        include_expired=True,
    )
    assert expired[0].sendable is False
    assert expired[0].reason == "pane_exited"


def test_disconnect_degraded_state_preserves_workers_and_bindings(tmp_path: Path) -> None:
    backend = _backend(tmp_path, "degraded")
    backend.reconcile_once(client=_initial_pane_client())
    binding_before = list_worker_bindings(backend.db_path, backend.config.host_id, backend="herdr")[0]

    backend._mark_unhealthy("socket_disconnected")

    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    binding_after = list_worker_bindings(backend.db_path, backend.config.host_id, backend="herdr")[0]
    assert snapshot is not None
    assert snapshot.workers[0].status == "active"
    assert snapshot.backend_health[0].status == "unavailable"
    assert binding_after.private_fingerprint == binding_before.private_fingerprint
    assert binding_after.sendable is True


def test_healthy_empty_reconnect_closes_missing_workers_and_expires_bindings(tmp_path: Path) -> None:
    backend = _backend(tmp_path, "healthy-empty")
    backend.reconcile_once(client=_initial_pane_client())

    backend.reconcile_once(client=_StaticClient(workspaces=[], tabs=[], panes=[], agents=[]))

    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    assert snapshot is not None
    assert snapshot.backend_health[0].status == "healthy"
    assert snapshot.backend_health[0].outcome == "empty_healthy"
    assert snapshot.workers[0].status == "closed"
    assert list_worker_bindings(backend.db_path, backend.config.host_id, backend="herdr") == []




def test_output_excerpt_limit_bounds_public_worker_summary(tmp_path: Path) -> None:
    config = Config(
        host_id="output-excerpt",
        data_dir=tmp_path,
        db_path=tmp_path / "output-excerpt.db",
        herdr_backend="socket",
        herdr_timeout_seconds=0.5,
        output_excerpt_chars=12,
    )
    init_store(Path(config.db_path))
    backend = HerdrEventBackend(config, debounce_seconds=0)
    long_summary = "x" * 40

    snapshot = backend.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "space-1", "name": "Build"}],
            panes=[
                {
                    "pane_id": "pane-1",
                    "agent": "Agent One",
                    "workspace_id": "space-1",
                    "description": long_summary,
                }
            ],
        )
    )
    binding = list_worker_bindings(backend.db_path, backend.config.host_id, backend="herdr")[0]

    assert snapshot.workers[0].summary == "xxxxxxxxx..."
    assert len(snapshot.workers[0].summary or "") == 12
    assert latest_snapshot(backend.db_path, backend.config.host_id).workers[0].summary == "xxxxxxxxx..."
    assert binding.worker_fingerprint == snapshot.workers[0].fingerprint
    assert long_summary not in snapshot.to_json()








def test_periodic_reconcile_uses_config_and_zero_disables_it(tmp_path: Path) -> None:
    disabled_config = Config(
        host_id="periodic-disabled",
        data_dir=tmp_path,
        db_path=tmp_path / "periodic-disabled.db",
        herdr_backend="socket",
        reconcile_interval_seconds=0,
    )
    init_store(Path(disabled_config.db_path))
    disabled = HerdrEventBackend(disabled_config, debounce_seconds=0)
    disabled_client = _initial_pane_client()
    disabled._next_reconcile_monotonic = time.monotonic() - 1
    disabled._run_periodic_reconcile_if_due(disabled_client)

    enabled_config = Config(
        host_id="periodic-enabled",
        data_dir=tmp_path,
        db_path=tmp_path / "periodic-enabled.db",
        herdr_backend="socket",
        reconcile_interval_seconds=0.001,
    )
    init_store(Path(enabled_config.db_path))
    enabled = HerdrEventBackend(enabled_config, debounce_seconds=0)
    enabled_client = _initial_pane_client()
    enabled._next_reconcile_monotonic = time.monotonic() - 1
    enabled._run_periodic_reconcile_if_due(enabled_client)

    assert disabled_client.calls == []
    assert enabled_client.calls == ["workspace.list", "tab.list", "pane.list", "agent.list"]
    assert enabled.operational_status["last_reconcile_at"] is not None








def test_debounce_batches_until_flush_and_shutdown_flushes(tmp_path: Path) -> None:
    backend = _backend(tmp_path, "debounce", debounce_seconds=60)
    backend.reconcile_once(client=_initial_pane_client())

    backend.queue_event_envelope(
        {"event": "pane.agent_status_changed", "data": {"agent": "Agent One", "status": "blocked"}}
    )
    pending = latest_snapshot(backend.db_path, backend.config.host_id)
    assert pending is not None
    assert pending.workers[0].status == "active"

    backend.stop()

    flushed = latest_snapshot(backend.db_path, backend.config.host_id)
    assert flushed is not None
    assert flushed.workers[0].status == "blocked"


def test_idle_event_timeout_keeps_polling_without_marking_backend_unhealthy(tmp_path: Path) -> None:
    backend = _backend(tmp_path, "idle-timeout")
    backend.reconcile_once(client=_initial_pane_client())

    class IdleThenEventClient:
        def __init__(self) -> None:
            self.calls = 0

        def read_event(self, subscription_id: str, *, timeout: float | None = None) -> dict[str, Any]:
            self.calls += 1
            if self.calls == 1:
                raise HerdrSocketTimeoutError("idle")
            backend.stop_event.set()
            return {
                "id": subscription_id,
                "event": "pane.agent_status_changed",
                "data": {"agent": "Agent One", "status": "blocked"},
            }

    client = IdleThenEventClient()
    backend._read_event_stream(client, "sub-1")

    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    assert snapshot is not None
    assert snapshot.workers[0].status == "blocked"
    assert snapshot.backend_health[0].status == "healthy"
    assert client.calls == 2


def test_mark_unhealthy_safe_sets_ready_even_when_persist_fails(tmp_path: Path, monkeypatch: Any) -> None:
    backend = _backend(tmp_path, "ready-on-error")

    def boom(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("store unavailable")

    monkeypatch.setattr("tendwire.backends.herdr_events.save_snapshot", boom)

    assert backend._mark_unhealthy_safe("protocol_error") is None
    assert backend.ready is True


def test_run_forever_retries_when_unhealthy_persistence_fails(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    backend = _backend(tmp_path, "retry-after-health-persist-error")
    reconcile_calls = 0
    clients_created = 0

    def client_factory(_config: Config) -> object:
        nonlocal clients_created
        clients_created += 1
        return object()

    def reconcile_once(*, client: object) -> None:
        nonlocal reconcile_calls
        reconcile_calls += 1
        if reconcile_calls == 1:
            raise RuntimeError("observation failed")
        backend.stop_event.set()

    def persist_failure(*_args: Any, **_kwargs: Any) -> None:
        raise local_state_error(LocalStateErrorCode.OPERATION_FAILED)

    backend.client_factory = client_factory
    monkeypatch.setattr(backend, "reconcile_once", reconcile_once)
    monkeypatch.setattr("tendwire.backends.herdr_events.save_snapshot", persist_failure)

    backend.run_forever()

    assert reconcile_calls == 2
    assert clients_created == 2
    assert backend.ready is True
    assert backend.health.outcome == "unknown"


def test_protocol_error_health_is_degraded_and_specific(tmp_path: Path) -> None:
    backend = _backend(tmp_path, "protocol-health")

    backend._mark_unhealthy("protocol_error")

    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    assert snapshot is not None
    assert snapshot.backend_health[0].status == "degraded"
    assert snapshot.backend_health[0].outcome == "protocol_error"


def test_daemon_starts_socket_backend_only_when_configured(tmp_path: Path) -> None:
    db_path = tmp_path / "daemon-socket.db"
    socket_path = tmp_path / "daemon.sock"
    config = Config(
        host_id="daemon-socket",
        data_dir=tmp_path,
        db_path=db_path,
        socket_path=socket_path,
        herdr_backend="socket",
    )
    calls: list[str] = []

    class FakeBackend:
        def __init__(self, config: Config, stop_event: threading.Event) -> None:
            self.config = config
            self.stop_event = stop_event

        def start(self, *, wait_for_reconcile: bool = True) -> None:
            calls.append(f"start:{wait_for_reconcile}")
            snapshot = project_from_observations(
                self.config,
                workers=[Worker(id="worker-1", name="Worker", status="active")],
                backend_health=[
                    BackendHealth(
                        name="herdr",
                        status="healthy",
                        outcome="healthy_non_empty",
                        counts={"workers": 1},
                    )
                ],
            )
            save_snapshot(Path(self.config.db_path), snapshot)

        def stop(self) -> None:
            calls.append("stop")

    def observe_cli(_config: Config) -> Any:
        raise AssertionError("CLI observation must not run in socket mode")

    daemon = TendwireDaemon(
        config,
        hooks=DaemonHooks(
            observe_initial_snapshot=observe_cli,
            event_backend_factory=lambda cfg, stop_event: FakeBackend(cfg, stop_event),
        ),
    )
    daemon.start()
    try:
        assert calls == ["start:True"]
        assert daemon.get_snapshot().workers[0].id == "worker-1"
        _assert_no_public_json_forbidden(daemon.get_health())
    finally:
        daemon.stop()

    assert "stop" in calls


def test_daemon_socket_fallback_uses_backend_health_when_snapshot_missing(tmp_path: Path) -> None:
    db_path = tmp_path / "daemon-fallback.db"
    socket_path = tmp_path / "daemon-fallback.sock"
    config = Config(
        host_id="daemon-fallback",
        data_dir=tmp_path,
        db_path=db_path,
        socket_path=socket_path,
        herdr_backend="socket",
    )

    class FakeBackend:
        def __init__(self, config: Config, stop_event: threading.Event) -> None:
            self.config = config
            self.stop_event = stop_event
            self.health = HerdrEventBackend(config)._health_for("protocol_error")

        def start(self, *, wait_for_reconcile: bool = True) -> None:
            return None

        def stop(self) -> None:
            return None

    daemon = TendwireDaemon(
        config,
        hooks=DaemonHooks(event_backend_factory=lambda cfg, stop_event: FakeBackend(cfg, stop_event)),
    )
    daemon.start()
    try:
        snapshot = daemon.get_snapshot()
        assert snapshot.backend_health[0].status == "degraded"
        assert snapshot.backend_health[0].outcome == "protocol_error"
    finally:
        daemon.stop()


def test_status_event_with_pane_id_only_updates_bound_worker_not_a_phantom(tmp_path: Path) -> None:
    """Regression: status events that only carry a pane id must resolve through
    the binding turn target instead of inserting a duplicate re-lettered worker
    that freezes the real worker's status (the 'stuck working icon' bug)."""
    backend = _backend(tmp_path, "phantom-host")
    client = _StaticClient(
        workspaces=[{"id": "space-1", "name": "Build", "status": "active"}],
        panes=[
            {
                "pane_id": "w1:p1",
                "terminal_id": "term-1",
                "agent": "claude",
                "workspace_id": "space-1",
                "agent_status": "working",
            },
            {
                "pane_id": "w1:p2",
                "terminal_id": "term-2",
                "agent": "claude",
                "workspace_id": "space-1",
                "agent_status": "working",
            },
        ],
        agents=[],
    )
    backend.reconcile_once(client=client)
    snapshot = latest_snapshot(Path(backend.db_path), backend.config.host_id)
    assert snapshot is not None
    ids_before = sorted(worker.id for worker in snapshot.workers)
    assert ids_before == ["claude-1", "claude-2"]

    event = normalize_event(
        {"event": "pane.agent_status_changed", "data": {"pane": {"pane_id": "w1:p2", "agent": "claude", "status": "idle"}}}
    )
    assert event is not None
    assert backend._apply_event(event) is True
    backend._persist_projection_locked() if hasattr(backend, "_persist_projection_locked") else None
    workers = backend._workers
    assert sorted(workers) == ["claude-1", "claude-2"], f"phantom worker inserted: {sorted(workers)}"
    by_target = {}
    for binding in backend._bindings.values():
        by_target[binding.target_value] = binding.worker_id
    idle_worker_id = by_target["term-2"]
    assert workers[idle_worker_id].status in {"idle", "done"}




def test_reconcile_drops_unbound_missing_workers_but_keeps_bound_closed(tmp_path: Path) -> None:
    backend = _backend(tmp_path, "phantom-aging-host")
    worker_bound = Worker(id="codex-1", name="codex", status="active", space_id="wX8")
    worker_phantom = Worker(id="codex", name="codex", status="working", space_id="wX8")
    merged = backend._workers_with_closed_missing(
        [worker_bound, worker_phantom],
        [],
        bound_worker_ids={"codex-1"},
    )
    ids = {worker.id: worker.status for worker in merged}
    assert "codex" not in ids, "unbound phantom must be dropped, not carried as closed"
    assert ids.get("codex-1") == "closed"


def test_nested_agent_pane_claim_cannot_poison_terminal_close_matching(
    tmp_path: Path,
) -> None:
    backend = _backend(tmp_path, "pane-terminal-provenance")
    initial = backend.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "W1", "name": "Build", "status": "active"}],
            panes=[
                {
                    "pane_id": "P1",
                    "terminal_id": "T1",
                    "workspace_id": "W1",
                    "agent": "codex",
                    "agent_status": "working",
                }
            ],
        )
    )
    worker = initial.workers[0]
    binding = next(iter(backend._bindings.values()))
    assert backend._pane_terminals == {"P1": "T1"}

    assert backend.queue_event_envelope(
        {"event": "pane.agent_detected", "data": {
            "agent": {
                "worker_id": "nested-agent-claimant",
                "pane_id": "Pfake",
                "terminal_id": "T1",
                "agent": "codex",
                "status": "working",
            }
        }}
    )
    assert backend._pane_terminals == {"P1": "T1"}
    assert backend._pane_owners == {"P1": {worker.id}}

    assert backend.queue_event_envelope(
        {"event": "pane.closed", "data": {"pane_id": "Pfake"}}
    )

    current = backend._workers[worker.id]
    assert current.status == worker.status
    assert current.status != "closed"
    assert "Pfake" not in backend._pane_terminals
    assert backend._pane_owners == {"P1": {worker.id}}
    stored = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
    )
    assert len(stored) == 1
    assert stored[0].private_fingerprint == binding.private_fingerprint
    assert stored[0].sendable is True


def test_reconcile_maps_only_accepted_pane_info_rows(
    tmp_path: Path,
) -> None:
    backend = _backend(tmp_path, "reconcile-pane-map-provenance")
    initial = backend.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "W1", "name": "Build", "status": "active"}],
            panes=[
                {
                    "pane_id": "P1",
                    "terminal_id": "T1",
                    "workspace_id": "W1",
                    "agent": "codex",
                    "agent_status": "working",
                },
                {
                    "pane_id": "Pfake",
                    "terminal_id": "T1",
                    "workspace_id": "W1",
                    "agent_status": "working",
                },
            ],
        )
    )
    worker = initial.workers[0]
    binding = next(iter(backend._bindings.values()))
    assert backend._pane_terminals == {"P1": "T1"}
    assert backend._pane_owners == {"P1": {worker.id}}

    assert backend.queue_event_envelope(
        {"event": "pane.closed", "data": {"pane_id": "Pfake"}}
    )

    assert backend._workers[worker.id].status == worker.status
    assert backend._workers[worker.id].status != "closed"
    stored = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
    )
    assert len(stored) == 1
    assert stored[0].private_fingerprint == binding.private_fingerprint


@pytest.mark.parametrize(
    ("source_field", "source_value"),
    [
        ("previous_pane_id", "P1"),
        ("previous_terminal_id", "T1"),
    ],
)
def test_accepted_move_removes_source_pane_terminal_alias_before_stale_close(
    tmp_path: Path,
    source_field: str,
    source_value: str,
) -> None:
    backend = _backend(tmp_path, "moved-pane-map-provenance")
    initial = backend.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "W1", "name": "Build", "status": "active"}],
            panes=[
                {
                    "pane_id": "P1",
                    "terminal_id": "T1",
                    "workspace_id": "W1",
                    "agent": "codex",
                    "agent_status": "working",
                }
            ],
        )
    )
    worker = initial.workers[0]

    assert backend.queue_event_envelope(
        {"event": "pane.moved", "data": {
            source_field: source_value,
            "pane": {
                "pane_id": "P2",
                "terminal_id": "T1",
                "workspace_id": "W1",
                "agent": "codex",
                "agent_status": "working",
            },
        }}
    )
    assert backend._pane_terminals == {"P2": "T1"}
    assert backend._pane_owners == {"P2": {worker.id}}

    assert backend.queue_event_envelope(
        {"event": "pane.closed", "data": {"pane_id": "P1"}}
    )

    assert backend._workers[worker.id].status == worker.status
    assert backend._workers[worker.id].status != "closed"
    assert backend._pane_terminals == {"P2": "T1"}
