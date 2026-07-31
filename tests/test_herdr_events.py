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

from tendwire.backends import herdr_cli, herdr_events, herdr_turns
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
from tendwire.store.sqlite import (
    SnapshotObservationContext,
    SnapshotRetentionPolicy,
    apply_backend_pending_observation,
    get_herdr_turn_watermark,
    init_store,
    latest_snapshot,
    list_attention_items,
    list_backend_pending,
    list_worker_bindings,
    maybe_run_automatic_store_maintenance,
    merge_turn_content,
    pending_payload_from_store,
    save_snapshot,
    set_herdr_turn_watermark,
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


def test_herdr_075_observation_paths_preserve_474_identity_inputs_byte_for_byte(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Feed one pane through every event path without changing its identity bytes."""
    fixed_installation_key = b"tendwire-identity-regression-key"
    assert len(fixed_installation_key) == 32
    derivation_inputs: list[bytes] = []
    real_stable_worker_key = herdr_cli.stable_worker_key

    def capture_stable_worker_key(
        installation_key: bytes,
        *,
        backend: str,
        host_id: str,
        workspace_id: str,
        pane_id: str,
    ) -> str:
        derivation_inputs.append(
            installation_key
            + b"\0"
            + json.dumps(
                {
                    "backend": backend,
                    "host_id": host_id,
                    "pane_id": pane_id,
                    "workspace_id": workspace_id,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        return real_stable_worker_key(
            installation_key,
            backend=backend,
            host_id=host_id,
            workspace_id=workspace_id,
            pane_id=pane_id,
        )

    monkeypatch.setattr(
        herdr_cli,
        "load_or_create_installation_key",
        lambda _data_dir: fixed_installation_key,
    )
    monkeypatch.setattr(herdr_cli, "stable_worker_key", capture_stable_worker_key)

    pane = {
        "pane_id": "w123456789abcde:pA",
        "terminal_id": "terminal-identity",
        "agent": "claude",
        "workspace_id": "w123456789abcde",
        "agent_status": "working",
        "label": "identity-pane",
    }
    workspaces = [{"id": "w123456789abcde", "name": "Build"}]

    def reconciled_backend(name: str) -> HerdrEventBackend:
        data_dir = tmp_path / name
        config = Config(
            host_id="identity-host",
            data_dir=data_dir,
            db_path=data_dir / "tendwire.db",
            herdr_backend="socket",
        )
        init_store(Path(config.db_path))
        backend = HerdrEventBackend(config, debounce_seconds=0)
        backend.reconcile_once(
            client=_StaticClient(
                workspaces=workspaces,
                panes=[dict(pane)],
                agents=[],
            )
        )
        return backend

    def stable_key_bytes(backend: HerdrEventBackend) -> bytes:
        snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
        assert snapshot is not None
        return snapshot.workers[0].meta["stable_key"].encode("ascii")

    def worker_meta_bytes(backend: HerdrEventBackend) -> bytes:
        snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
        assert snapshot is not None
        return json.dumps(
            snapshot.workers[0].meta,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")

    reference_backend = reconciled_backend("474-reference-reconcile")
    assert len(derivation_inputs) == 1
    reference_input = derivation_inputs[0]
    reference_key = stable_key_bytes(reference_backend)
    reference_meta = worker_meta_bytes(reference_backend)

    legacy_backend = reconciled_backend("herdr-074-status-path")
    legacy_path_start = len(derivation_inputs) - 1
    assert legacy_backend.queue_event_envelope(
        {
            "event": "pane.agent_status_changed",
            "data": {**pane, "agent_status": "blocked"},
        }
    )
    # Scalar status payloads are not PaneInfo and must not re-derive identity.
    assert derivation_inputs[legacy_path_start:] == [reference_input]
    assert stable_key_bytes(legacy_backend) == reference_key
    assert worker_meta_bytes(legacy_backend) == reference_meta

    new_backend = reconciled_backend("herdr-075-pane-updated-path")
    new_path_start = len(derivation_inputs) - 1
    before_new_meta = worker_meta_bytes(new_backend)
    refreshes: list[None] = []
    new_backend.set_turn_refresh_callback(lambda: refreshes.append(None))

    # Herdr 0.7.5's pane.updated is the scalar PaneOutputChanged event. It may
    # trigger a turn refresh but must not rebuild worker identity.
    assert new_backend.queue_event_envelope(
        {
            "event": "pane.updated",
            "data": {
                "type": "pane_updated",
                "workspace_id": pane["workspace_id"],
                "pane_id": pane["pane_id"],
                "revision": 2,
            },
        }
    )
    assert refreshes == [None]
    assert derivation_inputs[new_path_start:] == [reference_input]
    assert stable_key_bytes(new_backend) == reference_key
    assert before_new_meta == worker_meta_bytes(new_backend) == reference_meta

    class Herdr074FallbackClient:
        def __init__(self) -> None:
            self.params: list[dict[str, Any]] = []
            self.closed = 0
            self.connected = 0

        def subscribe(self, _method: str, params: Mapping[str, Any], **_kwargs: Any) -> Any:
            copied = json.loads(json.dumps(params))
            self.params.append(copied)
            if any(
                item.get("type") == "pane.updated"
                for item in copied["subscriptions"]
            ):
                raise HerdrErrorResponse(
                    {
                        "code": "invalid_request",
                        "message": "invalid request: unknown variant pane.updated",
                    },
                    "subscribe-1",
                    uncorrelated=True,
                )
            return SimpleNamespace(subscription_id="herdr-074-compatible")

        def close(self) -> None:
            self.closed += 1

        def connect(self) -> None:
            self.connected += 1

    fallback = Herdr074FallbackClient()
    before_fallback_inputs = list(derivation_inputs)
    before_fallback = latest_snapshot(new_backend.db_path, new_backend.config.host_id)
    stream = new_backend._subscribe_event_stream(fallback)
    after_fallback = latest_snapshot(new_backend.db_path, new_backend.config.host_id)
    assert stream.subscription_id == "herdr-074-compatible"
    assert fallback.closed == fallback.connected == 1
    assert len(fallback.params) == 2
    assert all(
        item["type"] != "pane.updated"
        for item in fallback.params[1]["subscriptions"]
    )
    assert {item["pane_id"] for item in fallback.params[1]["subscriptions"]} == {
        pane["pane_id"]
    }
    assert derivation_inputs == before_fallback_inputs
    assert before_fallback == after_fallback

    fallback_event_start = len(derivation_inputs)
    assert new_backend.queue_event_envelope(
        {
            "event": "pane.agent_status_changed",
            "data": {**pane, "agent_status": "blocked"},
        }
    )
    assert derivation_inputs[fallback_event_start:] == []
    assert stable_key_bytes(new_backend) == reference_key
    assert worker_meta_bytes(new_backend) == reference_meta

    # This models the observation-layer failure: a scalar refresh arrives with
    # a second identity-looking tuple. It must neither call the derivation
    # function nor replace any persisted worker metadata.
    before_dangerous_update_inputs = list(derivation_inputs)
    before_dangerous_update_meta = worker_meta_bytes(new_backend)
    assert new_backend.queue_event_envelope(
        {
            "event": "pane.updated",
            "data": {
                "type": "pane_updated",
                "workspace_id": "wD2",
                "pane_id": "wD2:p7",
                "revision": 3,
            },
        }
    )
    assert refreshes == [None, None, None]
    assert derivation_inputs == before_dangerous_update_inputs
    assert stable_key_bytes(new_backend) == reference_key
    assert worker_meta_bytes(new_backend) == before_dangerous_update_meta


def test_cross_path_and_cross_representation_observations_keep_one_stable_key(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = tmp_path / "identity-turn-adapter"
    adapter.write_text(
        "#!/usr/bin/env python3\n"
        "import json\n"
        "print(json.dumps({'result': {'turn': {"
        "'available': True, 'complete': True, 'user_text': 'identity read', "
        "'assistant_final_text': 'stable', 'source_turn_id': 'identity-turn', "
        "'workspace_id': 7, 'pane_id': 41}}}))\n",
        encoding="utf-8",
    )
    adapter.chmod(0o700)
    config = Config(
        host_id="cross-path-identity",
        data_dir=tmp_path,
        db_path=tmp_path / "cross-path.db",
        herdr_backend="socket",
        herdr_bin=str(adapter),
        herdr_timeout_seconds=1,
    )
    init_store(Path(config.db_path))
    backend = HerdrEventBackend(config, debounce_seconds=0)
    record_observations: list[
        tuple[str, bool, str | None, str | None, str | None, str | None]
    ] = []
    real_worker_record_from_item = herdr_cli._worker_record_from_item

    def capture_worker_record(
        item: Mapping[str, Any],
        record_config: Config | None = None,
        *,
        pane_info_observed: bool = False,
        identity_source: str = "unknown",
    ) -> Any:
        record = real_worker_record_from_item(
            item,
            record_config,
            pane_info_observed=pane_info_observed,
            identity_source=identity_source,
        )
        record_observations.append(
            (
                record.identity_source,
                record.pane_info_observed,
                record.observed_workspace_id,
                record.observed_pane_id,
                record.workspace_id,
                record.pane_id,
            )
        )
        return record

    monkeypatch.setattr(herdr_cli, "_worker_record_from_item", capture_worker_record)
    monkeypatch.setattr(herdr_events, "_worker_record_from_item", capture_worker_record)
    pane = {
        "workspace_id": "w65383a2e877513",
        "pane_id": "w65383a2e877513:pA",
        "terminal_id": "terminal-cross-path",
        "agent": "claude",
        "agent_status": "working",
        "label": "cross-path",
    }
    client = _StaticClient(
        workspaces=[{"id": pane["workspace_id"], "name": "Build"}],
        panes=[pane],
    )

    stable_keys: list[str] = []

    def capture_key() -> None:
        snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
        assert snapshot is not None
        assert len(snapshot.workers) == 1
        stable_keys.append(str(snapshot.workers[0].meta["stable_key"]))

    backend.reconcile_once(client=client)
    capture_key()
    assert (
        "pane.list",
        True,
        pane["workspace_id"],
        pane["pane_id"],
        pane["workspace_id"],
        pane["pane_id"],
    ) in record_observations
    records = backend._records_from_reconcile_payloads(
        {"agents": []},
        {"panes": [pane]},
    )
    assert len(records) == 1
    assert records[0].identity_source == "pane.list"
    assert (records[0].workspace_id, records[0].pane_id) == (
        pane["workspace_id"],
        pane["pane_id"],
    )

    # Full PaneInfo events may use aliases, but record construction still
    # stores the exact canonical public pair used by pane.list.
    assert backend.queue_event_envelope(
        {
            "event": "pane.created",
            "data": {
                "type": "pane_created",
                "pane": {
                    "workspaceId": pane["workspace_id"],
                    "paneId": pane["pane_id"],
                    "terminalId": pane["terminal_id"],
                    "agent": "claude",
                    "agentStatus": "idle",
                    "label": "cross-path",
                },
            },
        }
    )
    capture_key()
    assert (
        "event:pane.created",
        True,
        pane["workspace_id"],
        pane["pane_id"],
        pane["workspace_id"],
        pane["pane_id"],
    ) in record_observations

    # pane.updated is a scalar refresh notification in Herdr 0.7.5. Even a
    # different identity-looking representation cannot enter worker records.
    assert backend.queue_event_envelope(
        {
            "event": "pane.updated",
            "data": {
                "type": "pane_output_changed",
                "workspace_id": 7,
                "pane_id": 41,
                "revision": 2,
            },
        }
    )
    capture_key()
    assert all(source != "event:pane.updated" for source, *_rest in record_observations)

    # Scalar events may carry raw runtime representations. They can update a
    # matched worker but cannot become PaneInfo or feed stable-key derivation.
    assert backend.queue_event_envelope(
        {
            "event": "pane.agent_status_changed",
            "data": {
                "workspace_id": 7,
                "pane_id": 41,
                "terminal_id": pane["terminal_id"],
                "agent": "claude",
                "agent_status": "working",
            },
        }
    )
    capture_key()
    assert (
        "event:pane.agent_status_changed",
        False,
        "7",
        "41",
        None,
        None,
    ) in record_observations

    binding = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
    )[0]
    assert binding.turn_target_kind == "pane_id"
    assert herdr_turns.refresh_turn_binding(config, binding).status in {
        "updated",
        "unchanged",
    }
    capture_key()

    backend.reconcile_once(client=client)
    capture_key()
    assert len(set(stable_keys)) == 1


def test_one_hundred_interleaved_identity_observations_never_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_stable_worker_key = herdr_cli.stable_worker_key
    derived_keys: set[str] = set()

    def capture_stable_worker_key(*args: Any, **kwargs: Any) -> str:
        stable_key = real_stable_worker_key(*args, **kwargs)
        derived_keys.add(stable_key)
        return stable_key

    monkeypatch.setattr(herdr_cli, "stable_worker_key", capture_stable_worker_key)
    backend = _backend(tmp_path, "interleaved-identity")
    pane = {
        "workspace_id": "w65383a2e877513",
        "pane_id": "w65383a2e877513:pA",
        "terminal_id": "terminal-interleaved",
        "agent": "claude",
        "agent_status": "working",
    }
    client = _StaticClient(
        workspaces=[{"id": pane["workspace_id"], "name": "Build"}],
        panes=[pane],
    )
    backend.reconcile_once(client=client)
    binding = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
    )[0]
    monkeypatch.setattr(
        herdr_turns,
        "_read_turn_for_binding",
        lambda *_args, **_kwargs: {
            "complete": True,
            "user_text": "identity read",
            "assistant_final_text": "stable",
            "source_turn_id": "identity-turn",
            "workspace_id": 7,
            "pane_id": 41,
        },
    )

    observed_keys: set[str] = set()

    def remember_key() -> None:
        snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
        assert snapshot is not None
        observed_keys.add(str(snapshot.workers[0].meta["stable_key"]))

    remember_key()
    for index in range(100):
        path = index % 5
        if path == 0:
            assert backend.queue_event_envelope(
                {
                    "event": "pane.created",
                    "data": {"pane": {**pane, "agent_status": "idle"}},
                }
            )
        elif path == 1:
            assert backend.queue_event_envelope(
                {
                    "event": "pane.updated",
                    "data": {
                        "workspace_id": 7,
                        "pane_id": 41,
                        "revision": index,
                    },
                }
            )
        elif path == 2:
            assert backend.queue_event_envelope(
                {
                    "event": "pane.agent_status_changed",
                    "data": {
                        "workspace_id": 7,
                        "pane_id": 41,
                        "terminal_id": pane["terminal_id"],
                        "agent": "claude",
                        "agent_status": "working",
                    },
                }
            )
        elif path == 3:
            backend.reconcile_once(client=client)
            binding = list_worker_bindings(
                backend.db_path,
                backend.config.host_id,
                backend="herdr",
            )[0]
        else:
            assert herdr_turns.refresh_turn_binding(
                backend.config,
                binding,
            ).status in {"updated", "unchanged"}
        remember_key()

    assert len(observed_keys) == 1
    assert derived_keys == observed_keys


@pytest.mark.parametrize("turn_model", ["legacy", "observed"])
@pytest.mark.parametrize(
    "event_envelope",
    [
        pytest.param(
            {
                "event": "pane.agent_status_changed",
                "data": {
                    "pane_id": "w123456789abcde:pA",
                    "workspace_id": "w123456789abcde",
                    "agent": "claude",
                    "agent_status": "blocked",
                },
            },
            id="herdr-074-status-event",
        ),
        pytest.param(
            {
                "event": "pane_updated",
                "data": {
                    "type": "pane_updated",
                    "pane": {
                        "pane_id": "w123456789abcde:pA",
                        "workspace_id": "w123456789abcde",
                        "terminal_id": "terminal-decision",
                        "agent": "claude",
                        "agent_status": "blocked",
                    },
                },
            },
            id="herdr-075-pane-updated-event",
        ),
        pytest.param(
            {
                "event": "pane_agent_status_changed",
                "data": {
                    "type": "pane_agent_status_changed",
                    "pane_id": "w123456789abcde:pA",
                    "workspace_id": "w123456789abcde",
                    "agent": "claude",
                    "display_agent": "Claude",
                    "agent_status": "blocked",
                    "state_labels": {},
                    "title": None,
                },
            },
            id="herdr-075-agent-status-event",
        ),
    ],
)
def test_idless_blocked_event_persists_and_lists_decision(
    tmp_path: Path,
    turn_model: str,
    event_envelope: dict[str, Any],
) -> None:
    backend, _binding = _decision_backend(tmp_path, turn_model)
    refreshes: list[Any] = []

    def refresh_current() -> None:
        binding = next(iter(backend._bindings.values()))
        refreshes.append(
            herdr_turns.refresh_turn_binding(
                backend.config, binding, adapter_timeout_seconds=1
            )
        )

    backend.set_turn_refresh_callback(refresh_current)

    def handler(conn: _SocketConnection) -> None:
        request = conn.read_request()
        subscriptions = request["params"]["subscriptions"]
        assert {"type": "pane.updated"} in subscriptions
        assert {
            "type": "pane.agent_status_changed",
            "pane_id": "w123456789abcde:pA",
        } in subscriptions
        conn.send_json(
            {"id": request["id"], "result": {"type": "subscription_started"}}
        )
        conn.send_json(event_envelope)

    with _FakeHerdrSocketServer(tmp_path, handler) as server:
        client = HerdrSocketClient(str(server.path), timeout=1)
        stream = backend._subscribe_event_stream(client)
        envelope = client.read_event(stream.subscription_id, timeout=1)
        assert envelope.get("id") is None
        assert backend.queue_event_envelope(envelope) is True
        client.close()

    assert refreshes == [herdr_turns.TurnRefreshResult("updated", 1, True)]
    _assert_decision_persisted(backend)


@pytest.mark.parametrize("turn_model", ["legacy", "observed"])
def test_reconcile_fallback_refreshes_and_lists_decision_without_status_event(
    tmp_path: Path,
    turn_model: str,
) -> None:
    backend, _binding = _decision_backend(tmp_path, turn_model)
    refreshes: list[Any] = []

    def refresh_current() -> None:
        binding = next(iter(backend._bindings.values()))
        refreshes.append(
            herdr_turns.refresh_turn_binding(
                backend.config, binding, adapter_timeout_seconds=1
            )
        )

    backend.set_turn_refresh_callback(refresh_current)

    backend.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "w123456789abcde", "name": "Build"}],
            panes=[
                {
                    "pane_id": "w123456789abcde:pA",
                    "terminal_id": "terminal-decision",
                    "agent": "claude",
                    "workspace_id": "w123456789abcde",
                    "agent_status": "blocked",
                }
            ],
            agents=[],
        )
    )

    assert refreshes == [herdr_turns.TurnRefreshResult("updated", 1, True)]
    _assert_decision_persisted(backend)


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


def test_nested_compatibility_event_cannot_duplicate_authenticated_turn_owner(
    tmp_path: Path,
) -> None:
    backend = _backend(tmp_path, "nested-turn-owner-conflict")
    session = {
        "source": "old-source-secret",
        "agent": "codex",
        "kind": "id",
        "value": "shared-session-secret",
    }
    backend.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "wR9", "name": "Build"}],
            panes=[
                {
                    "workspace_id": "wR9",
                    "pane_id": "wR9:pA",
                    "terminal_id": "old-terminal-secret",
                    "agent": "codex",
                    "agent_session": session,
                    "status": "running",
                }
            ],
            agents=[
                {
                    "worker_id": "public-old-owner",
                    "agent_id": "old-agent-target-secret",
                    "workspace_id": "wR9",
                    "pane_id": "wR9:pA",
                    "terminal_id": "old-terminal-secret",
                    "agent": "codex",
                    "agent_session": session,
                    "status": "running",
                }
            ],
        )
    )

    assert backend.queue_event_envelope(
        {"event": "pane.agent_detected", "data": {
            "agent": {
                "worker_id": "public-compatibility-claimant",
                "agent_id": "new-agent-target-secret",
                "agent": "codex",
                "agent_session": {
                    "source": "new-source-secret",
                    "agent": "codex",
                    "kind": "id",
                    "value": "shared-session-secret",
                },
                "status": "running",
            }
        }}
    )

    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    bindings = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
    )
    assert snapshot is not None
    assert len(snapshot.workers) == len(bindings) == 1
    assert "stable_key" not in snapshot.workers[0].meta
    assert bindings[0].sendable is False
    assert bindings[0].reason == "ambiguous_pane_match"
    assert bindings[0].turn_target_kind is None
    assert bindings[0].turn_target_value is None


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


@pytest.mark.parametrize(
    "event_name",
    ["pane.agent_detected", "pane.agent_status_changed"],
)
@pytest.mark.parametrize("key_failure", [False, True])
def test_official_idless_event_reuses_single_authenticated_pane_owner(
    tmp_path: Path,
    key_failure: bool,
    event_name: str,
) -> None:
    backend = _backend(
        tmp_path,
        f"event-pane-owner-id-churn-{key_failure}-{event_name}",
    )
    old_session = {
        "source": "old-source-secret",
        "agent": "codex",
        "kind": "id",
        "value": "old-session-secret",
    }
    pane = {
        "workspace_id": "wR9",
        "pane_id": "wR9:pA",
        "terminal_id": "old-terminal-secret",
        "agent": "codex",
        "agent_session": old_session,
        "status": "running",
    }
    agent = {
        "worker_id": "public-old-owner",
        "agent_id": "old-agent-target-secret",
        "workspace_id": "wR9",
        "pane_id": "wR9:pA",
        "terminal_id": "old-terminal-secret",
        "agent": "codex",
        "agent_session": old_session,
        "status": "running",
    }
    backend.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "wR9", "name": "Build"}],
            panes=[pane],
            agents=[agent],
        )
    )
    before = latest_snapshot(backend.db_path, backend.config.host_id)
    before_bindings = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
    )
    assert before is not None
    assert len(before.workers) == len(before_bindings) == 1
    worker_id = before.workers[0].id
    stable_key = before.workers[0].meta["stable_key"]
    if key_failure:
        backend.config.installation_key_marker_path.unlink()

    event_payload = {
        "workspace_id": "wR9",
        "pane_id": "wR9:pA",
        "agent": {
            "worker_id": "public-new-owner",
            "agent_id": "new-agent-target-secret",
            "terminal_id": "new-terminal-secret",
            "agent": "codex",
            "agent_session": {
                "source": "new-source-secret",
                "agent": "codex",
                "kind": "id",
                "value": "new-session-secret",
            },
            "status": "working",
        },
    }
    assert backend.queue_event_envelope(
        {"event": event_name, "data": event_payload}
    )

    after = latest_snapshot(backend.db_path, backend.config.host_id)
    bindings = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
    )
    assert after is not None
    assert len(after.workers) == len(bindings) == 1
    assert after.workers[0].id == worker_id
    assert after.workers[0].meta["stable_key"] == stable_key
    assert after.backend_health[0].status == "healthy"
    if key_failure:
        assert not backend.config.installation_key_marker_path.exists()
    assert bindings[0].worker_id == worker_id
    assert bindings[0].private_fingerprint == before_bindings[0].private_fingerprint
    assert (bindings[0].target_kind, bindings[0].target_value) == (
        before_bindings[0].target_kind,
        before_bindings[0].target_value,
    )
    assert (bindings[0].turn_target_kind, bindings[0].turn_target_value) == (
        before_bindings[0].turn_target_kind,
        before_bindings[0].turn_target_value,
    )
    _assert_no_public_json_forbidden(json.loads(after.to_json()))


@pytest.mark.parametrize("shared_owner", ["terminal_id", "agent_session"])
@pytest.mark.parametrize("key_failure", [False, True])
def test_incremental_shared_private_owner_fails_closed_across_canonical_panes(
    tmp_path: Path,
    shared_owner: str,
    key_failure: bool,
) -> None:
    backend = _backend(
        tmp_path,
        f"event-shared-{shared_owner}-owner-{key_failure}",
    )
    backend.reconcile_once(
        client=_StaticClient(workspaces=[{"id": "wR9", "name": "Build"}])
    )

    for suffix, pane_id in (("a", "wR9:pA"), ("b", "wR9:pB")):
        assert backend.queue_event_envelope(
            {"event": "pane.created", "data": {
                "workspace_id": "wR9",
                "pane_id": pane_id,
                "terminal_id": (
                    "shared-terminal-secret"
                    if shared_owner == "terminal_id"
                    else f"terminal-{suffix}-secret"
                ),
                "agent_id": f"agent-{suffix}-secret",
                "agent": "codex",
                "agent_session": {
                    "source": f"source-{suffix}-secret",
                    "agent": "codex",
                    "kind": "id",
                    "value": (
                        "shared-session-secret"
                        if shared_owner == "agent_session"
                        else f"session-{suffix}-secret"
                    ),
                },
                "status": "running",
            }}
        )
        if suffix == "a" and key_failure:
            backend.config.installation_key_marker_path.unlink()
    assert backend.queue_event_envelope(
        {"event": "pane.created", "data": {
            "workspace_id": "wR9",
            "pane_id": "wR9:pB",
            "terminal_id": (
                "shared-terminal-secret"
                if shared_owner == "terminal_id"
                else "terminal-b-secret"
            ),
            "agent_id": "agent-b-secret",
            "agent": "codex",
            "agent_session": {
                "source": "source-b-secret",
                "agent": "codex",
                "kind": "id",
                "value": (
                    "shared-session-secret"
                    if shared_owner == "agent_session"
                    else "session-b-secret"
                ),
            },
            "status": "running",
        }}
    )

    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    bindings = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
    )
    assert snapshot is not None
    assert len(snapshot.workers) == len(bindings) == 1
    if key_failure:
        assert snapshot.workers[0].meta["stable_key"].startswith("wsk1_")
        assert snapshot.workers[0].meta["stable_key_version"] == 1
        assert snapshot.backend_health[0].status == "degraded"
        assert snapshot.backend_health[0].outcome == "continuity_unavailable"
        assert bindings[0].sendable is True
        assert bindings[0].reason is None
        assert bindings[0].turn_target_kind is not None
        assert bindings[0].turn_target_value is not None
    else:
        assert "stable_key" not in snapshot.workers[0].meta
        assert "stable_key_version" not in snapshot.workers[0].meta
        assert bindings[0].sendable is False
        assert bindings[0].reason == "ambiguous_pane_match"
        assert bindings[0].turn_target_kind is None
        assert bindings[0].turn_target_value is None


@pytest.mark.parametrize("complete_move", [False, True])
def test_move_into_owned_pane_fails_closed_for_both_owners(
    tmp_path: Path,
    complete_move: bool,
) -> None:
    backend = _backend(
        tmp_path,
        f"event-move-owned-destination-{complete_move}",
    )
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
    backend.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "wR9", "name": "Build"}],
            panes=panes,
        )
    )

    payload: dict[str, Any] = {
        "previous_pane_id": "wR9:pB",
        "new_pane_id": "wR9:pA",
    }
    if complete_move:
        payload["pane"] = {
            "workspace_id": "wR9",
            "pane_id": "wR9:pA",
            "agent": "Agent B",
            "status": "running",
        }
    assert backend.queue_event_envelope(
        {"event": "pane.moved", "data": payload}
    )

    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    bindings = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
    )
    assert snapshot is not None
    assert len(snapshot.workers) == len(bindings) == 2
    assert all("stable_key" not in worker.meta for worker in snapshot.workers)
    assert all("stable_key_version" not in worker.meta for worker in snapshot.workers)
    assert all(binding.sendable is False for binding in bindings)
    assert all(binding.reason == "ambiguous_pane_match" for binding in bindings)
    assert all(binding.turn_target_kind is None for binding in bindings)
    assert all(binding.turn_target_value is None for binding in bindings)


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


@pytest.mark.parametrize("key_failure", [False, True])
def test_authoritative_move_resolves_agent_targeted_source_by_previous_pane(
    tmp_path: Path,
    key_failure: bool,
) -> None:
    backend = _backend(
        tmp_path,
        f"event-move-agent-targeted-source-{key_failure}",
    )
    old_session = {
        "source": "old-source-secret",
        "agent": "codex",
        "kind": "id",
        "value": "old-session-secret",
    }
    backend.reconcile_once(
        client=_StaticClient(
            workspaces=[
                {"id": "wR9", "name": "Source"},
                {"id": "wD2", "name": "Destination"},
            ],
            panes=[
                {
                    "workspace_id": "wR9",
                    "pane_id": "wR9:pA",
                    "terminal_id": "old-terminal-secret",
                    "agent": "codex",
                    "agent_session": old_session,
                    "status": "running",
                }
            ],
            agents=[
                {
                    "worker_id": "public-source-owner",
                    "agent_id": "old-agent-target-secret",
                    "workspace_id": "wR9",
                    "pane_id": "wR9:pA",
                    "terminal_id": "old-terminal-secret",
                    "agent": "codex",
                    "agent_session": old_session,
                    "status": "running",
                }
            ],
        )
    )
    before = latest_snapshot(backend.db_path, backend.config.host_id)
    before_binding = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
    )[0]
    assert before is not None
    worker_id = before.workers[0].id
    original_key = before.workers[0].meta["stable_key"]
    if key_failure:
        key_bytes = backend.config.installation_key_path.read_bytes()
        backend.config.installation_key_path.write_bytes(
            bytes(byte ^ 0xFF for byte in key_bytes)
        )

    assert backend.queue_event_envelope(
        {"event": "pane.moved", "data": {
            "previous_pane_id": "wR9:pA",
            "pane": {
                "workspace_id": "wD2",
                "pane_id": "wD2:p7",
                "terminal_id": "new-terminal-secret",
                "agent": "codex",
                "agent_session": {
                    "source": "new-source-secret",
                    "agent": "codex",
                    "kind": "id",
                    "value": "new-session-secret",
                },
                "status": "running",
            },
        }}
    )

    after = latest_snapshot(backend.db_path, backend.config.host_id)
    bindings = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
    )
    assert after is not None
    assert len(after.workers) == len(bindings) == 1
    assert after.workers[0].id == worker_id
    if key_failure:
        assert after.workers == before.workers
        assert after.workers[0].meta["stable_key"] == original_key
        assert bindings == [before_binding]
        assert after.backend_health[0].status == "degraded"
        assert after.backend_health[0].outcome == "continuity_unavailable"
    else:
        assert after.workers[0].meta["stable_key"] != original_key
        assert after.workers[0].space_id == "wD2"
        assert bindings[0].private_fingerprint == before_binding.private_fingerprint
        assert bindings[0].target_kind == "terminal_id"
        assert bindings[0].target_value == "new-terminal-secret"
        assert bindings[0].turn_target_kind == "codex_session_id"
        assert bindings[0].turn_target_value == "new-session-secret"


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

def test_reconcile_turn_refresh_observes_durable_state_and_replaced_ownership_maps(
    tmp_path: Path,
) -> None:
    backend = _backend(tmp_path, "reconcile-turn-refresh")
    client = _StaticClient(
        workspaces=[{"id": "space-1", "name": "Build", "status": "active"}],
        panes=[
            {
                "pane_id": "pane-1",
                "terminal_id": "terminal-1",
                "agent": "Agent One",
                "workspace_id": "space-1",
                "status": "running",
            }
        ],
        agents=[],
    )
    observed: list[tuple[str, str]] = []

    def callback() -> None:
        assert not backend._lock._is_owned()
        snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
        bindings = list_worker_bindings(
            backend.db_path,
            backend.config.host_id,
            backend="herdr",
        )
        assert snapshot is not None
        assert bindings
        assert backend._pane_terminals == {"pane-1": "terminal-1"}
        observed.append((snapshot.to_json(), bindings[0].private_fingerprint))
        # The callback-state lock must also be released before invocation.
        backend.set_turn_refresh_callback(None)

    backend.set_turn_refresh_callback(callback)
    reconciled = backend.reconcile_once(client=client)

    assert observed == [
        (
            reconciled.to_json(),
            list_worker_bindings(
                backend.db_path,
                backend.config.host_id,
                backend="herdr",
            )[0].private_fingerprint,
        )
    ]


@pytest.mark.parametrize(
    ("event_name", "data"),
    [
        (
            "pane.created",
            {
                "pane_id": "pane-1",
                "agent": "Agent One",
                "workspace_id": "space-1",
                "status": "working",
            },
        ),
        (
            "pane.focused",
            {
                "pane_id": "pane-1",
                "agent": "Agent One",
                "workspace_id": "space-1",
                "status": "working",
            },
        ),
        (
            "pane.updated",
            {
                "pane": {
                    "pane_id": "pane-1",
                    "agent": "Agent One",
                    "workspace_id": "space-1",
                    "status": "working",
                },
            },
        ),
        (
            "pane.moved",
            {
                "old_pane_id": "pane-1",
                "pane_id": "pane-2",
                "agent": "Agent One",
                "workspace_id": "space-1",
            },
        ),
        ("pane.closed", {"pane_id": "pane-1"}),
        ("pane.exited", {"pane_id": "pane-1"}),
        (
            "pane.agent_detected",
            {
                "pane_id": "pane-1",
                "agent": "Agent One",
                "workspace_id": "space-1",
                "status": "working",
            },
        ),
        (
            "pane.agent_status_changed",
            {
                "pane_id": "pane-1",
                "agent": "Agent One",
                "status": "blocked",
            },
        ),
        ("pane.output_matched", {"pane_id": "pane-1"}),
    ],
)
def test_each_turn_relevant_event_notifies_once(
    tmp_path: Path,
    event_name: str,
    data: dict[str, Any],
) -> None:
    backend = _backend(tmp_path, f"turn-refresh-{event_name.replace('.', '-')}")
    backend.reconcile_once(client=_initial_pane_client())
    calls: list[None] = []
    backend.set_turn_refresh_callback(lambda: calls.append(None))

    assert backend.queue_event_envelope({"event": event_name, "data": data}) is True

    assert calls == [None]


def test_relevant_event_burst_notifies_once_after_batch_commit(tmp_path: Path) -> None:
    backend = _backend(tmp_path, "turn-refresh-burst", debounce_seconds=60)
    backend.reconcile_once(client=_initial_pane_client())
    calls: list[str] = []
    identity = HerdrEventId("turn-refresh-burst")

    def callback() -> None:
        assert not backend._lock._is_owned()
        snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
        assert snapshot is not None
        assert snapshot.workers[0].status == "blocked"
        assert identity in backend._producer_dedupe
        calls.append(snapshot.updated_at)

    backend.set_turn_refresh_callback(callback)
    assert backend.queue_event_envelope(
        {"event": "pane.focused", "data": {"pane_id": "pane-1", "agent": "Agent One"}},
        flush=False,
    )
    assert backend.queue_event_envelope(
        {**_status_event("blocked"), "event_id": identity.value},
        flush=False,
    )
    assert backend.queue_event_envelope(
        {"event": "pane.output_matched", "data": {"pane_id": "pane-1"}},
        flush=False,
    )

    backend.flush()

    assert len(calls) == 1


def test_projection_neutral_output_match_notifies_without_rewriting_snapshot(tmp_path: Path) -> None:
    backend = _backend(tmp_path, "turn-refresh-output")
    backend.reconcile_once(client=_initial_pane_client())
    before = latest_snapshot(backend.db_path, backend.config.host_id)
    assert before is not None
    calls: list[None] = []
    backend.set_turn_refresh_callback(lambda: calls.append(None))

    assert backend.queue_event_envelope(
        {"event": "pane.output_matched", "data": {"pane_id": "pane-1"}}
    )

    after = latest_snapshot(backend.db_path, backend.config.host_id)
    assert after is not None
    assert after.to_json() == before.to_json()
    assert calls == [None]


def test_irrelevant_duplicate_and_invalid_events_do_not_notify(tmp_path: Path) -> None:
    backend = _backend(tmp_path, "turn-refresh-noop")
    backend.reconcile_once(client=_initial_pane_client())
    calls: list[None] = []
    backend.set_turn_refresh_callback(lambda: calls.append(None))

    assert backend.queue_event_envelope(
        {
            "event": "workspace.updated",
            "data": {"workspace_id": "space-1", "name": "Renamed"},
        }
    )
    assert backend.queue_event_envelope(
        {
            "event": "worktree.created",
            "data": {"workspace_id": "missing-space", "name": "Ignored"},
        }
    )
    assert backend.queue_event_envelope({"event": "not.official", "data": {}}) is False
    assert backend.queue_event_envelope({"event": 7, "data": {}}) is False
    assert calls == []

    backend.set_turn_refresh_callback(None)
    duplicate = {
        "event": "pane.output_matched",
        "event_id": "already-applied",
        "data": {"pane_id": "pane-1"},
    }
    assert backend.queue_event_envelope(duplicate) is True
    backend.set_turn_refresh_callback(lambda: calls.append(None))
    assert backend.queue_event_envelope(duplicate) is False
    assert calls == []


def test_persistence_failure_does_not_notify_turn_refresh(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    backend = _backend(tmp_path, "turn-refresh-persist-failure")
    backend.reconcile_once(client=_initial_pane_client())
    calls: list[None] = []
    backend.set_turn_refresh_callback(lambda: calls.append(None))

    def fail_persistence(*, observed_at: str | None = None) -> Any:
        raise RuntimeError(f"snapshot unavailable at {observed_at}")

    monkeypatch.setattr(backend, "_persist_current_state", fail_persistence)

    with pytest.raises(RuntimeError, match="snapshot unavailable"):
        backend.queue_event_envelope(_status_event("blocked"))
    assert calls == []


def test_callback_failure_cannot_roll_back_durable_event_state(tmp_path: Path) -> None:
    backend = _backend(tmp_path, "turn-refresh-callback-failure")
    backend.reconcile_once(client=_initial_pane_client())
    identity = HerdrEventId("callback-failure")

    def fail_callback() -> None:
        raise RuntimeError("scheduler unavailable")

    backend.set_turn_refresh_callback(fail_callback)
    assert backend.queue_event_envelope(
        {**_status_event("blocked"), "event_id": identity.value}
    )

    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    assert snapshot is not None
    assert snapshot.workers[0].status == "blocked"
    assert identity in backend._producer_dedupe
    assert backend.queue_event_envelope(
        {**_status_event("blocked"), "event_id": identity.value}
    ) is False


@pytest.mark.parametrize("event_name", ["pane.closed", "pane.exited"])
def test_close_refresh_callback_observes_expired_binding(
    tmp_path: Path,
    event_name: str,
) -> None:
    backend = _backend(tmp_path, f"turn-refresh-{event_name.replace('.', '-')}-binding")
    backend.reconcile_once(client=_initial_pane_client())
    observed_reasons: list[str | None] = []

    def callback() -> None:
        assert list_worker_bindings(
            backend.db_path,
            backend.config.host_id,
            backend="herdr",
        ) == []
        expired = list_worker_bindings(
            backend.db_path,
            backend.config.host_id,
            backend="herdr",
            include_expired=True,
        )
        assert len(expired) == 1
        observed_reasons.append(expired[0].reason)

    backend.set_turn_refresh_callback(callback)
    assert backend.queue_event_envelope(
        {"event": event_name, "data": {"pane_id": "pane-1"}}
    )

    assert observed_reasons == [event_name.replace(".", "_")]



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


def test_worker_cap_exceeded_preserves_previous_authoritative_snapshot(tmp_path: Path) -> None:
    config = Config(
        host_id="worker-cap",
        data_dir=tmp_path,
        db_path=tmp_path / "worker-cap.db",
        herdr_backend="socket",
        herdr_timeout_seconds=0.5,
        max_workers=1,
        turn_refresh_workers=1,
    )
    init_store(Path(config.db_path))
    backend = HerdrEventBackend(config, debounce_seconds=0)
    backend.reconcile_once(client=_initial_pane_client())

    capped = backend.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "space-1", "name": "Build"}],
            panes=[
                {"pane_id": "pane-1", "agent": "Agent One", "workspace_id": "space-1"},
                {"pane_id": "pane-2", "agent": "Agent Two", "workspace_id": "space-1"},
            ],
        )
    )
    latest = latest_snapshot(backend.db_path, backend.config.host_id)

    assert latest is not None
    assert capped.content_fingerprint == latest.content_fingerprint
    assert [worker.name for worker in capped.workers] == ["Agent One"]
    assert capped.backend_health[0].status == "degraded"
    assert capped.backend_health[0].outcome == "worker_cap_exceeded"
    assert list_worker_bindings(backend.db_path, backend.config.host_id, backend="herdr")
    _assert_no_public_json_forbidden(json.loads(capped.to_json()))


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


def test_incremental_event_over_worker_cap_is_ignored_with_degraded_health(tmp_path: Path) -> None:
    config = Config(
        host_id="event-worker-cap",
        data_dir=tmp_path,
        db_path=tmp_path / "event-worker-cap.db",
        herdr_backend="socket",
        herdr_timeout_seconds=0.5,
        max_workers=1,
        turn_refresh_workers=1,
    )
    init_store(Path(config.db_path))
    backend = HerdrEventBackend(config, debounce_seconds=0)
    backend.reconcile_once(client=_initial_pane_client())

    backend.queue_event_envelope(
        {"event": "pane.agent_detected", "data": {
            "agent": {
                "agent_id": "agent-2",
                "name": "Agent Two",
                "workspace_id": "space-1",
                "pane_id": "pane-2",
            }
        }}
    )
    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)

    assert snapshot is not None
    assert [worker.name for worker in snapshot.workers] == ["Agent One"]
    assert snapshot.backend_health[0].status == "degraded"
    assert snapshot.backend_health[0].outcome == "worker_cap_exceeded"


def test_closed_worker_reactivation_over_worker_cap_is_ignored(tmp_path: Path) -> None:
    config = Config(
        host_id="event-worker-cap-reactivate",
        data_dir=tmp_path,
        db_path=tmp_path / "event-worker-cap-reactivate.db",
        herdr_backend="socket",
        herdr_timeout_seconds=0.5,
        max_workers=1,
        turn_refresh_workers=1,
    )
    init_store(Path(config.db_path))
    backend = HerdrEventBackend(config, debounce_seconds=0)
    backend.reconcile_once(client=_initial_pane_client())

    backend.queue_event_envelope(
        {"event": "pane.closed", "data": {"pane": {"pane_id": "pane-1", "agent": "Agent One", "workspace_id": "space-1"}}}
    )
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

    before_reactivation = latest_snapshot(backend.db_path, backend.config.host_id)
    assert before_reactivation is not None
    assert {worker.name: worker.status for worker in before_reactivation.workers} == {
        "Agent One": "closed",
        "Agent Two": "active",
    }

    backend.queue_event_envelope(
        {"event": "pane.agent_detected", "data": {
            "agent": {
                "agent": "Agent One",
                "workspace_id": "space-1",
                "pane_id": "pane-3",
                "status": "running",
            }
        }}
    )
    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)

    assert snapshot is not None
    assert {worker.name: worker.status for worker in snapshot.workers} == {
        "Agent One": "closed",
        "Agent Two": "active",
    }
    assert snapshot.backend_health[0].status == "degraded"
    assert snapshot.backend_health[0].outcome == "worker_cap_exceeded"


def test_pane_moved_reactivation_over_worker_cap_is_ignored(tmp_path: Path) -> None:
    config = Config(
        host_id="event-worker-cap-moved-reactivate",
        data_dir=tmp_path,
        db_path=tmp_path / "event-worker-cap-moved-reactivate.db",
        herdr_backend="socket",
        herdr_timeout_seconds=0.5,
        max_workers=1,
        turn_refresh_workers=1,
    )
    init_store(Path(config.db_path))
    backend = HerdrEventBackend(config, debounce_seconds=0)
    first = backend.reconcile_once(client=_initial_pane_client())
    first_worker = first.workers[0]
    closed_worker = Worker(
        id=first_worker.id,
        name=first_worker.name,
        status="closed",
        space_id=first_worker.space_id,
        meta=first_worker.meta,
        last_seen_at=first_worker.last_seen_at,
        summary=first_worker.summary,
        backend_target=first_worker.backend_target,
    )
    backend._workers[closed_worker.id] = closed_worker
    save_snapshot(
        backend.db_path,
        project_from_observations(
            backend.config,
            spaces=first.spaces,
            workers=[closed_worker],
            backend_health=first.backend_health,
        ),
    )

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
    before_move = latest_snapshot(backend.db_path, backend.config.host_id)
    assert before_move is not None
    assert {worker.name: worker.status for worker in before_move.workers} == {
        "Agent One": "closed",
        "Agent Two": "active",
    }

    backend.queue_event_envelope(
        {"event": "pane.moved", "data": {
            "old_pane_id": "pane-1",
            "pane_id": "pane-3",
            "agent": "Agent One",
            "workspace_id": "space-1",
            "status": "running",
        }}
    )
    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)

    assert snapshot is not None
    assert {worker.name: worker.status for worker in snapshot.workers} == {
        "Agent One": "closed",
        "Agent Two": "active",
    }
    assert snapshot.backend_health[0].status == "degraded"
    assert snapshot.backend_health[0].outcome == "worker_cap_exceeded"


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

    try:
        backend._mark_unhealthy_safe("protocol_error")
    except RuntimeError:
        pass

    assert backend.ready is True


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


def test_pane_id_only_status_event_resolves_codex_binding_via_pane_terminal_map(tmp_path: Path) -> None:
    """Regression: codex bindings' turn target is a session id, so pane-id-only
    status events must resolve through the pane->terminal map remembered from
    reconcile instead of inserting a phantom bare 'codex' worker."""
    backend = _backend(tmp_path, "codex-phantom-host")
    client = _StaticClient(
        workspaces=[{"id": "wX8", "name": "projectx", "status": "active"}],
        panes=[
            {
                "pane_id": "wX8:p1",
                "terminal_id": "term-ctx",
                "agent": "codex",
                "agent_session": {"agent": "codex", "kind": "id", "value": "019f-session"},
                "workspace_id": "wX8",
                "agent_status": "working",
            }
        ],
        agents=[],
    )
    backend.reconcile_once(client=client)
    assert sorted(backend._workers) == ["codex"]
    binding = next(iter(backend._bindings.values()))
    assert binding.turn_target_kind == "codex_session_id"
    assert backend._pane_terminals == {"wX8:p1": "term-ctx"}

    event = normalize_event(
        {"event": "pane.agent_status_changed", "data": {"pane_id": "wX8:p1", "workspace_id": "wX8", "agent_status": "idle"}}
    )
    assert event is not None
    assert backend._apply_event(event) is True
    assert sorted(backend._workers) == ["codex"], f"phantom inserted: {sorted(backend._workers)}"
    assert backend._workers["codex"].status in {"idle", "done"}
    updated_binding = next(iter(backend._bindings.values()))
    assert updated_binding.private_fingerprint == binding.private_fingerprint
    assert updated_binding.target_kind == binding.target_kind == "terminal_id"
    assert updated_binding.target_value == binding.target_value == "term-ctx"
    assert updated_binding.turn_target_kind == binding.turn_target_kind == "codex_session_id"
    assert updated_binding.turn_target_value == binding.turn_target_value == "019f-session"


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


def test_pane_only_status_preserves_unobserved_owner_aliases_for_later_conflict(
    tmp_path: Path,
) -> None:
    backend = _backend(tmp_path, "status-owner-alias-provenance")
    session = {
        "source": "old-source-secret",
        "agent": "codex",
        "kind": "id",
        "value": "old-session-secret",
    }
    initial = backend.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "wR9", "name": "Build", "status": "active"}],
            panes=[
                {
                    "pane_id": "wR9:pA",
                    "terminal_id": "shared-terminal-secret",
                    "workspace_id": "wR9",
                    "agent": "codex",
                    "agent_session": session,
                    "agent_status": "working",
                }
            ],
            agents=[
                {
                    "worker_id": "public-owner",
                    "agent_id": "old-agent-target-secret",
                    "pane_id": "wR9:pA",
                    "terminal_id": "shared-terminal-secret",
                    "workspace_id": "wR9",
                    "agent": "codex",
                    "agent_session": session,
                    "agent_status": "working",
                }
            ],
        )
    )
    worker = initial.workers[0]
    binding = next(iter(backend._bindings.values()))
    assert binding.target_kind == "agent_id"
    assert binding.turn_target_kind == "codex_session_id"

    assert backend.queue_event_envelope(
        {"event": "pane.agent_status_changed", "data": {
            "workspace_id": "wR9",
            "pane_id": "wR9:pA",
            "status": "idle",
        }}
    )
    status_binding = next(iter(backend._bindings.values()))
    assert status_binding.private_fingerprint == binding.private_fingerprint
    assert status_binding.target_kind == binding.target_kind
    assert status_binding.target_value == binding.target_value
    assert status_binding.turn_target_kind == binding.turn_target_kind
    assert status_binding.turn_target_value == binding.turn_target_value
    assert backend._terminal_owners == {
        "shared-terminal-secret": {worker.id},
    }
    assert backend._session_owners == {
        "old-session-secret": {worker.id},
    }

    assert backend.queue_event_envelope(
        {"event": "pane.created", "data": {
            "pane": {
                "workspace_id": "wR9",
                "pane_id": "wR9:pB",
                "terminal_id": "shared-terminal-secret",
                "agent_id": "new-agent-target-secret",
                "agent": "codex",
                "agent_session": {
                    "source": "new-source-secret",
                    "agent": "codex",
                    "kind": "id",
                    "value": "new-session-secret",
                },
                "status": "working",
            }
        }}
    )

    assert set(backend._workers) == {worker.id}
    conflicted = next(iter(backend._bindings.values()))
    assert conflicted.worker_id == worker.id
    assert conflicted.sendable is False
    assert conflicted.reason == "ambiguous_pane_match"
    assert "wR9:pB" not in backend._pane_terminals


def test_nested_compatibility_replay_cannot_rehabilitate_ambiguous_binding(
    tmp_path: Path,
) -> None:
    backend = _backend(tmp_path, "compatibility-ambiguity-provenance")
    session = {
        "source": "shared-source-secret",
        "agent": "codex",
        "kind": "id",
        "value": "shared-session-secret",
    }
    original_agent = {
        "worker_id": "public-owner-a",
        "agent_id": "agent-target-a-secret",
        "pane_id": "wR9:pA",
        "terminal_id": "terminal-a-secret",
        "workspace_id": "wR9",
        "agent": "codex",
        "agent_session": session,
        "status": "working",
    }
    initial = backend.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "wR9", "name": "Build", "status": "active"}],
            panes=[
                {
                    "pane_id": "wR9:pA",
                    "terminal_id": "terminal-a-secret",
                    "workspace_id": "wR9",
                    "agent": "codex",
                    "agent_session": session,
                    "status": "working",
                }
            ],
            agents=[original_agent],
        )
    )
    worker = initial.workers[0]

    assert backend.queue_event_envelope(
        {"event": "pane.created", "data": {
            "pane": {
                "workspace_id": "wR9",
                "pane_id": "wR9:pB",
                "terminal_id": "terminal-b-secret",
                "agent_id": "agent-target-b-secret",
                "agent": "codex",
                "agent_session": session,
                "status": "working",
            }
        }}
    )
    ambiguous = next(iter(backend._bindings.values()))
    assert ambiguous.worker_id == worker.id
    assert ambiguous.target_value == "agent-target-a-secret"
    assert ambiguous.sendable is False
    assert ambiguous.reason == "ambiguous_pane_match"
    assert ambiguous.turn_target_kind is None
    assert ambiguous.turn_target_value is None
    assert backend._workers[worker.id].backend_target == ambiguous.backend_target()

    assert backend.queue_event_envelope(
        {"event": "pane.agent_detected", "data": {"agent": original_agent}}
    )

    replayed = next(iter(backend._bindings.values()))
    assert replayed.private_fingerprint == ambiguous.private_fingerprint
    assert replayed.target_kind == ambiguous.target_kind
    assert replayed.target_value == ambiguous.target_value
    assert replayed.sendable is False
    assert replayed.reason == "ambiguous_pane_match"
    assert replayed.turn_target_kind is None
    assert replayed.turn_target_value is None
    assert backend._workers[worker.id].backend_target == replayed.backend_target()
    assert backend._pane_terminals == {
        "wR9:pA": "terminal-a-secret",
    }


def _run_authoritative_recovery_trace(
    tmp_path: Path,
    monkeypatch: Any,
) -> dict[str, Any]:
    backend = _backend(tmp_path, "authoritative-turn-recovery")
    session = {
        "source": "codex",
        "agent": "codex",
        "kind": "id",
        "value": "session-private-recovery",
    }
    agent = {
        "worker_id": "public-recovery-worker",
        "agent_id": "agent-private-recovery",
        "workspace_id": "wR9",
        "pane_id": "wR9:pA",
        "terminal_id": "terminal-private-a",
        "agent": "codex",
        "agent_session": session,
        "status": "working",
    }
    pane_a = {
        "workspace_id": "wR9",
        "pane_id": "wR9:pA",
        "terminal_id": "terminal-private-a",
        "agent": "codex",
        "agent_session": session,
        "status": "working",
    }
    workspace = [{"id": "wR9", "name": "Build", "status": "active"}]

    initial = backend.reconcile_once(
        client=_StaticClient(
            workspaces=workspace,
            panes=[pane_a],
            agents=[agent],
        )
    )
    worker = initial.workers[0]
    initial_binding = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
    )[0]
    assert initial_binding.worker_id == worker.id
    assert initial_binding.turn_target_kind == "codex_session_id"
    assert initial_binding.turn_target_value == "session-private-recovery"
    assert merge_turn_content(
        backend.db_path,
        backend.config.host_id,
        worker.id,
        {
            "user_text": "Recover the deterministic producer final.",
            "assistant_stream_text": "Still working.",
            "assistant_final_text": None,
            "complete": False,
            "has_open_turn": True,
            "source_turn_id": "producer-turn-42",
        },
    ) == 1
    seeded_payload = turns_payload_from_store(
        backend.db_path,
        backend.config.host_id,
        snapshot=initial,
    )
    seeded_turns = [
        turn
        for turn in seeded_payload["turns"]
        if turn.get("user_text") == "Recover the deterministic producer final."
    ]
    assert len(seeded_turns) == 1
    public_turn_id = seeded_turns[0]["id"]
    public_source_turn_id = seeded_turns[0]["source_turn_id"]
    assert public_source_turn_id != "producer-turn-42"
    assert seeded_turns[0]["worker_id"] == worker.id
    assert seeded_turns[0]["complete"] is False

    adapter_calls: list[tuple[str, str | None]] = []

    def refresh_existing_final(
        config: Config,
        binding: Any,
        *,
        adapter_timeout_seconds: float | None = None,
    ) -> Any:
        del adapter_timeout_seconds
        adapter_calls.append((binding.worker_id, binding.turn_target_value))
        applied = herdr_turns.apply_turn_refresh(
            config.db_path,
            config.host_id,
            binding.worker_id,
            {
                "user_text": "Recover the deterministic producer final.",
                "assistant_stream_text": None,
                "assistant_final_text": "The durable producer final.",
                "complete": True,
                "has_open_turn": False,
                "source_turn_id": "producer-turn-42",
            },
            expected_binding=binding,
        )
        assert applied.updated == 1
        return herdr_turns.TurnRefreshResult("updated", 1, applied.pending_changed)

    monkeypatch.setattr(herdr_turns, "refresh_turn_binding", refresh_existing_final)
    pane_b = {
        "workspace_id": "wR9",
        "pane_id": "wR9:pB",
        "terminal_id": "terminal-private-b",
        "agent": "codex",
        "agent_session": session,
        "status": "working",
    }
    quarantined = backend.reconcile_once(
        client=_StaticClient(
            workspaces=workspace,
            panes=[pane_a, pane_b],
            agents=[agent],
        )
    )
    quarantined_bindings = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
    )
    assert len(quarantined_bindings) == 1
    quarantined_binding = quarantined_bindings[0]
    assert quarantined_binding.worker_id == worker.id
    assert quarantined_binding.sendable is False
    assert quarantined_binding.reason == "ambiguous_pane_match"
    assert quarantined_binding.turn_target_kind is None
    assert quarantined_binding.turn_target_value is None
    assert herdr_turns.refresh_structured_turn_content(backend.config) == {
        "ok": True,
        "status": "ok",
        "updated": 0,
        "attempted": 0,
    }
    assert adapter_calls == []
    quarantined_payload = turns_payload_from_store(
        backend.db_path,
        backend.config.host_id,
        snapshot=quarantined,
    )
    quarantined_turn = next(
        turn for turn in quarantined_payload["turns"] if turn["id"] == public_turn_id
    )
    assert quarantined_turn["source_turn_id"] == public_source_turn_id
    assert quarantined_turn["assistant_final_text"] is None
    assert quarantined_turn["assistant_stream_text"] == "Still working."
    assert quarantined_turn["complete"] is False

    restarted = HerdrEventBackend(
        backend.config,
        debounce_seconds=0,
        reconnect_delay_seconds=0,
    )
    assert len(restarted._bindings) == 1
    restarted_binding = next(iter(restarted._bindings.values()))
    assert restarted_binding.worker_id == worker.id
    assert restarted_binding.reason == "ambiguous_pane_match"
    assert restarted_binding.turn_target_value is None

    recovered = restarted.reconcile_once(
        client=_StaticClient(
            workspaces=workspace,
            panes=[pane_a],
            agents=[agent],
        )
    )
    recovered_bindings = list_worker_bindings(
        restarted.db_path,
        restarted.config.host_id,
        backend="herdr",
    )
    assert len(recovered_bindings) == 1
    recovered_binding = recovered_bindings[0]
    assert recovered.workers[0].id == worker.id
    assert recovered_binding.worker_id == initial_binding.worker_id
    assert recovered_binding.private_fingerprint == initial_binding.private_fingerprint
    assert recovered_binding.sendable is True
    assert recovered_binding.reason is None
    assert recovered_binding.turn_target_kind == "codex_session_id"
    assert recovered_binding.turn_target_value == "session-private-recovery"

    assert herdr_turns.refresh_turn_binding(
        restarted.config,
        recovered_binding,
    ) == herdr_turns.TurnRefreshResult("updated", 1)
    assert adapter_calls == [(worker.id, "session-private-recovery")]

    final_payload = turns_payload_from_store(
        restarted.db_path,
        restarted.config.host_id,
        snapshot=recovered,
    )
    final_turns = [
        turn
        for turn in final_payload["turns"]
        if turn.get("assistant_final_text") == "The durable producer final."
    ]
    assert len(final_turns) == 1
    assert final_turns[0]["id"] == public_turn_id
    assert final_turns[0]["source_turn_id"] == public_source_turn_id
    assert final_turns[0]["worker_id"] == worker.id
    assert final_turns[0]["assistant_stream_text"] is None
    assert final_turns[0]["complete"] is True
    assert final_turns[0]["has_open_turn"] is False
    public_json = json.dumps(final_payload, sort_keys=True)
    for private_value in (
        "producer-turn-42",
        "agent-private-recovery",
        "session-private-recovery",
        "terminal-private-a",
        "terminal-private-b",
        "wR9:pA",
        "wR9:pB",
    ):
        assert private_value not in public_json
    _assert_no_public_json_forbidden(json.loads(public_json))
    snapshot_payload = json.loads(recovered.to_json())
    snapshot_payload["ok"] = True
    return {
        "turns_payload": final_payload,
        "snapshot_payload": snapshot_payload,
        "public_turn": final_turns[0],
    }


def test_authoritative_reconcile_quarantines_then_recovers_existing_final_once(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    _run_authoritative_recovery_trace(tmp_path, monkeypatch)


@pytest.mark.skipif(
    not os.environ.get("HERDRES_SOURCE_DIR"),
    reason="HERDRES_SOURCE_DIR is required for the cross-repository recovery contract",
)
def test_authoritative_recovery_payload_promotes_herdres_working_once(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    trace = _run_authoritative_recovery_trace(tmp_path, monkeypatch)
    turns_payload = trace["turns_payload"]
    snapshot_payload = trace["snapshot_payload"]
    public_turn = trace["public_turn"]
    assert turns_payload["schema_version"] == 1
    assert public_turn["id"]

    source_dir = Path(os.environ["HERDRES_SOURCE_DIR"]).expanduser().resolve()
    package_roots = [
        candidate
        for candidate in (source_dir, source_dir / "src")
        if (candidate / "herdres_connector" / "source_sync.py").is_file()
    ]
    assert len(package_roots) == 1, (
        "HERDRES_SOURCE_DIR must contain herdres_connector/source_sync.py "
        "either directly or under src/"
    )
    package_root = package_roots[0]
    monkeypatch.syspath_prepend(str(package_root))
    preloaded_herdres_modules = sorted(
        name
        for name in sys.modules
        if name == "herdres_connector" or name.startswith("herdres_connector.")
    )
    assert preloaded_herdres_modules == []
    direct_boundary_attempts: list[str] = []

    def reject_direct_boundary(*_args: Any, **_kwargs: Any) -> Any:
        direct_boundary_attempts.append("process_or_socket")
        raise AssertionError("Herdres source sync must not access Herdr outside Tendwire")

    class RejectDirectSocket(socket.socket):
        def __new__(cls, *_args: Any, **_kwargs: Any) -> Any:
            direct_boundary_attempts.append("socket")
            raise AssertionError("Herdres source sync must not open a direct socket")


    real_import = builtins.__import__

    def guarded_import(
        name: str,
        globals: Any = None,
        locals: Any = None,
        fromlist: Any = (),
        level: int = 0,
    ) -> Any:
        private_roots = {
            "herdr",
            "herdr_turn_adapter",
            "herdr_socket",
            "herdr_cli",
            "herdr_events",
        }
        if name.split(".", 1)[0] in private_roots or name.startswith(
            "tendwire.backends"
        ):
            direct_boundary_attempts.append(f"import:{name[:80]}")
            raise AssertionError("Herdres source sync must not import a direct Herdr client")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    monkeypatch.setattr(subprocess, "run", reject_direct_boundary)
    monkeypatch.setattr(subprocess, "Popen", reject_direct_boundary)
    monkeypatch.setattr(socket, "socket", RejectDirectSocket)
    monkeypatch.setattr(socket, "create_connection", reject_direct_boundary)
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    source_sync = importlib.import_module("herdres_connector.source_sync")
    herdres_state = importlib.import_module("herdres_connector.state")
    source_sync_path = Path(source_sync.__file__).resolve()
    loaded_herdres_files = {
        name: Path(module.__file__).resolve()
        for name, module in sys.modules.items()
        if (name == "herdres_connector" or name.startswith("herdres_connector."))
        and getattr(module, "__file__", None)
    }
    assert source_sync_path == package_root / "herdres_connector" / "source_sync.py"
    assert loaded_herdres_files
    assert all(path.is_relative_to(package_root) for path in loaded_herdres_files.values())

    worker_observation = next(
        worker
        for worker in snapshot_payload["workers"]
        if worker["id"] == public_turn["worker_id"]
    )
    space_observation = next(
        space
        for space in snapshot_payload["spaces"]
        if space["id"] == public_turn["space_id"]
    )
    store = {
        "enabled": True,
        "telegram": {"chat_id": "-100", "general_thread_id": "1"},
        "panes": {},
        "spaces": {},
        "tendwired_bootstrap_complete": True,
    }
    _worker_key, worker_entry, _created = herdres_state.upsert_worker_entry(
        store,
        worker_observation,
        topic_id="77",
    )
    herdres_state.upsert_space_entry(
        store,
        space_observation,
        topic_id="77",
    )
    worker_entry.update(
        {
            "last_stream_turn_id": public_turn["id"],
            "last_stream_hash": "persisted-working-hash",
            "last_stream_message_id": "555",
            "last_stream_bot_kind": "manager",
        }
    )
    herdres_state.bind_message_to_worker(
        store,
        "555",
        worker_entry,
        topic_id="77",
        kind="working",
        turn_id=public_turn["id"],
        bot_kind="manager",
    )

    class RecoveryTendwire:
        def __init__(self) -> None:
            self.calls: list[str] = []
            self.turn_payload_objects: list[dict[str, Any]] = []

        def snapshot(self) -> dict[str, Any]:
            self.calls.append("snapshot")
            return snapshot_payload

        def turns(self) -> dict[str, Any]:
            self.calls.append("turns")
            self.turn_payload_objects.append(turns_payload)
            return turns_payload

        def pending(self) -> dict[str, Any]:
            self.calls.append("pending")
            return {"ok": True, "pending_interactions": []}

    class RecoveryTelegram:
        dry_run = False

        def __init__(
            self,
            token: str = "fake",
            shared: dict[str, list[Any]] | None = None,
        ) -> None:
            self.token = token
            self.shared = shared or {
                "sent": [],
                "edited": [],
                "topics": [],
                "deleted_topics": [],
                "renamed_topics": [],
                "pins": [],
                "api_calls": [],
                "icon_edits": [],
            }
            self.sent = self.shared["sent"]
            self.edited = self.shared["edited"]
            self.topics = self.shared["topics"]
            self.deleted_topics = self.shared["deleted_topics"]
            self.renamed_topics = self.shared["renamed_topics"]
            self.pins = self.shared["pins"]
            self.api_calls = self.shared["api_calls"]
            self.icon_edits = self.shared["icon_edits"]

        def with_token(self, token: str) -> Any:
            return RecoveryTelegram(token=token, shared=self.shared)

        def api(self, method: str, payload: dict[str, Any]) -> dict[str, Any]:
            self.api_calls.append((method, dict(payload), self.token))
            if method == "editMessageText":
                rich_payload = payload.get("rich_message")
                rich = json.loads(rich_payload) if rich_payload else {}
                html = str(rich.get("html") or payload.get("text") or "")
                message_id = str(payload.get("message_id") or "")
                self.edited.append((str(payload.get("chat_id") or ""), message_id, html))
                return {"ok": True, "result": {"message_id": message_id}}
            if method == "sendRichMessage":
                message_id = str(100 + len(self.sent))
                rich = json.loads(payload.get("rich_message") or "{}")
                self.sent.append(
                    (
                        str(payload.get("chat_id") or ""),
                        str(rich.get("html") or ""),
                        {"thread_id": str(payload.get("message_thread_id") or "")},
                        message_id,
                    )
                )
                return {"ok": True, "result": {"message_id": message_id}}
            return {"ok": True, "result": {"message_id": "0"}}

        def create_topic(
            self,
            _chat_id: str,
            name: str,
            icon_color: int | None = None,
        ) -> dict[str, Any]:
            self.topics.append((name, icon_color))
            return {"ok": True, "topic_id": str(76 + len(self.topics))}

        def rename_topic(
            self,
            chat_id: str,
            thread_id: str,
            name: str,
        ) -> dict[str, Any]:
            self.renamed_topics.append((chat_id, thread_id, name))
            return {"ok": True}

        def edit_topic_icon(
            self,
            chat_id: str,
            thread_id: str,
            emoji_id: str,
        ) -> dict[str, Any]:
            self.icon_edits.append((chat_id, thread_id, emoji_id))
            return {"ok": True}

        def delete_topic(self, _chat_id: str, thread_id: str) -> dict[str, Any]:
            self.deleted_topics.append(thread_id)
            return {"ok": True}

        def send_message(
            self,
            chat_id: str,
            html: str,
            **kwargs: Any,
        ) -> dict[str, Any]:
            message_id = str(100 + len(self.sent))
            self.sent.append((chat_id, html, dict(kwargs), message_id))
            return {"ok": True, "message_id": message_id}

        def edit_message(
            self,
            chat_id: str,
            message_id: str,
            html: str,
        ) -> dict[str, Any]:
            self.edited.append((chat_id, str(message_id), html))
            return {"ok": True, "message_id": str(message_id)}

        def pin_message(self, chat_id: str, message_id: str) -> dict[str, Any]:
            self.pins.append((chat_id, str(message_id)))
            return {"ok": True}

    tendwire = RecoveryTendwire()
    telegram = RecoveryTelegram()
    runtime = source_sync.SyncRuntime(tendwire, telegram, with_outbox=False)

    first = source_sync.sync_once(store, runtime)
    ledger_after_first = json.loads(
        json.dumps(herdres_state.delivered_turns(store), sort_keys=True)
    )
    edits_after_first = list(telegram.edited)
    sends_after_first = list(telegram.sent)
    second = source_sync.sync_once(store, runtime)
    ledger_after_second = json.loads(
        json.dumps(herdres_state.delivered_turns(store), sort_keys=True)
    )
    third = source_sync.sync_once(store, runtime)

    binding = herdres_state.find_message_binding(store, "555", topic_id="77")
    ledger = herdres_state.delivered_turns(store)
    assert tendwire.calls == ["snapshot", "turns", "pending"] * 3
    assert tendwire.turn_payload_objects == [turns_payload] * 3
    assert all(payload is turns_payload for payload in tendwire.turn_payload_objects)
    assert first["feed_sent"] == first["sent"] == 1
    assert len(telegram.edited) == 1
    assert telegram.edited[0][1] == "555"
    assert public_turn["assistant_final_text"] in telegram.edited[0][2]
    assert telegram.sent == []
    assert second["feed_sent"] == second["sent"] == second["turn_updates"] == 0
    assert third["feed_sent"] == third["sent"] == third["turn_updates"] == 0
    assert telegram.edited == edits_after_first
    assert telegram.sent == sends_after_first
    assert ledger_after_second == ledger_after_first
    assert ledger == ledger_after_first
    assert len(ledger) == 1
    assert list(ledger.values())[0]["turn_id"] == public_turn["id"]
    assert binding is not None
    assert binding["kind"] == "final"
    assert binding["turn_id"] == public_turn["id"]
    assert binding["topic_id"] == "77"
    assert worker_entry["topic_id"] == "77"
    assert worker_entry["last_turn_id"] == public_turn["id"]
    assert worker_entry["last_clean_message_id"] == "555"
    assert worker_entry["last_clean_message_ids"] == ["555"]
    assert "last_stream_turn_id" not in worker_entry
    assert "last_stream_hash" not in worker_entry
    assert "last_stream_message_id" not in worker_entry
    assert "last_stream_bot_kind" not in worker_entry
    assert direct_boundary_attempts == []


def _turn_api_pane() -> dict[str, Any]:
    return {
        "pane_id": "w123456789abcde:pA",
        "terminal_id": "terminal-turn-api",
        "agent": "claude",
        "workspace_id": "w123456789abcde",
        "agent_status": "idle",
        "last_completed_turn": {
            "turn": 0,
            "turn_epoch": 7,
            "completed_unix_ms": 0,
        },
        "turn": 0,
        "turn_epoch": 7,
        "outcome": "completed",
    }


def _turn_record(
    turn: int,
    *,
    epoch: int = 7,
    outcome: str = "completed",
) -> dict[str, Any]:
    return {
        "turn": turn,
        "turn_epoch": epoch,
        "outcome": outcome,
        "completed_unix_ms": 1_700_000_000_000 + turn,
        "message": f"turn {turn}",
        "message_truncated": False,
        "agent_session_path": None,
    }


def _turn_api_backend(
    tmp_path: Path,
    host_id: str,
    processor: Callable[..., Any],
) -> HerdrEventBackend:
    backend = _backend(tmp_path, host_id)
    backend.turn_completion_processor = processor
    backend.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "w123456789abcde", "name": "Build"}],
            panes=[_turn_api_pane()],
        )
    )
    return backend


def test_turn_api_absent_preserves_old_subscription_negotiation(tmp_path: Path) -> None:
    backend = _turn_api_backend(
        tmp_path,
        "turn-api-absent",
        lambda *_args, **_kwargs: SimpleNamespace(status="updated"),
    )

    class OldClient:
        subscriptions: list[dict[str, Any]] = []

        def pane_turns(self, _params: Mapping[str, Any], **_kwargs: Any) -> Any:
            raise HerdrErrorResponse(
                {"code": "invalid_params", "message": "unknown method"},
                "probe",
            )

        def subscribe(
            self,
            _method: str,
            params: Mapping[str, Any],
            **_kwargs: Any,
        ) -> Any:
            self.subscriptions.append(dict(params))
            return SimpleNamespace(subscription_id="old")

    client = OldClient()
    backend._replay_turns_after_reconcile(client)
    backend._subscribe_event_stream(client)

    assert backend._turn_api_supported is False
    assert all(
        item["type"] != "pane.turn_completed"
        for item in client.subscriptions[0]["subscriptions"]
    )
    assert get_herdr_turn_watermark(
        backend.db_path,
        backend.config.host_id,
        _turn_api_pane()["pane_id"],
    ) is None


def test_turn_api_idless_unknown_variant_probe_is_inert(
    tmp_path: Path,
) -> None:
    backend = _turn_api_backend(
        tmp_path,
        "turn-api-idless-unknown-variant",
        lambda *_args, **_kwargs: SimpleNamespace(status="updated"),
    )
    # Exact envelope emitted by live stock Herdr 0.7.5 (reported upstream):
    # the id is present but EMPTY, and the method name is backticked.
    raw_probe_error = {
        "id": "",
        "error": {
            "code": "invalid_request",
            "message": "invalid request: unknown variant `pane.turns`, expected one of `pane.read`, `pane.list`, `agent.list`",
        },
    }

    def handler(conn: _SocketConnection) -> None:
        probe = conn.read_request()
        assert probe["method"] == "pane.turns"
        conn.send_json(raw_probe_error)

        ordinary = conn.read_request()
        assert ordinary["method"] == "workspace.list"
        conn.send_json({"id": ordinary["id"], "result": {"workspaces": []}})

        subscription = conn.read_request()
        assert subscription["method"] == HERDR_EVENTS_SUBSCRIBE_METHOD
        assert all(
            item["type"] != "pane.turn_completed"
            for item in subscription["params"]["subscriptions"]
        )
        conn.send_json(
            {
                "id": subscription["id"],
                "result": {"type": "subscription_started"},
            }
        )

    with _FakeHerdrSocketServer(tmp_path, handler) as server:
        client = HerdrSocketClient(str(server.path), timeout=1)
        backend._replay_turns_after_reconcile(client)
        assert client.workspace_list() == {"workspaces": []}
        backend._subscribe_event_stream(client)
        client.close()

    assert [request["method"] for request in server.requests] == [
        "pane.turns",
        "workspace.list",
        HERDR_EVENTS_SUBSCRIBE_METHOD,
    ]
    assert backend._turn_api_supported is False
    assert (
        _table_count(
            backend.db_path,
            backend.config.host_id,
            "herdr_turn_watermarks",
        )
        == 0
    )
    assert get_herdr_turn_watermark(
        backend.db_path,
        backend.config.host_id,
        _turn_api_pane()["pane_id"],
    ) is None


def test_turn_api_unknown_variant_classifier_requires_exact_code_and_prefix() -> None:
    assert HerdrEventBackend._turn_api_method_unsupported(
        HerdrErrorResponse(
            {
                "code": "invalid_request",
                "message": "invalid request: unknown variant pane.turns",
            },
            "probe",
        )
    )
    assert not HerdrEventBackend._turn_api_method_unsupported(
        HerdrErrorResponse(
            {
                "code": "invalid_params",
                "message": "invalid request: unknown variant pane.turns",
            },
            "probe",
        )
    )
    assert not HerdrEventBackend._turn_api_method_unsupported(
        HerdrErrorResponse(
            {
                "code": "invalid_request",
                "message": "pane.turns failed: unknown variant",
            },
            "probe",
        )
    )


def test_turn_api_first_connect_baselines_full_ring_without_processing(
    tmp_path: Path,
) -> None:
    processed: list[str] = []
    binding_hints: list[str | None] = []

    def process(_config: Config, pane_id: str, **kwargs: Any) -> Any:
        processed.append(pane_id)
        binding_hints.append(kwargs.get("binding_private_fingerprint"))
        return SimpleNamespace(
            status="updated",
            worker_id="claude",
            refreshed_turn_id=f"public-turn-{len(processed)}",
        )

    backend = _turn_api_backend(tmp_path, "turn-api-happy", process)

    class NewClient:
        subscriptions: list[dict[str, Any]] = []

        def pane_turns(self, params: Mapping[str, Any], **_kwargs: Any) -> Any:
            assert params == {
                "pane_id": _turn_api_pane()["pane_id"],
                "since": 0,
            }
            return {
                "type": "pane_turns",
                "turns": {
                    "pane_id": _turn_api_pane()["pane_id"],
                    "turn_epoch": 7,
                    "records": [_turn_record(turn) for turn in range(1, 65)],
                    "truncated": False,
                    "oldest_available": 1,
                },
            }

        def subscribe(
            self,
            _method: str,
            params: Mapping[str, Any],
            **_kwargs: Any,
        ) -> Any:
            self.subscriptions.append(dict(params))
            return SimpleNamespace(subscription_id="new")

    client = NewClient()
    backend._replay_turns_after_reconcile(client)
    backend._subscribe_event_stream(client)

    assert processed == []
    assert binding_hints == []
    watermark = get_herdr_turn_watermark(
        backend.db_path,
        backend.config.host_id,
        _turn_api_pane()["pane_id"],
    )
    assert watermark is not None
    assert (watermark.turn_epoch, watermark.last_turn) == (7, 64)
    assert watermark.completeness_break_count == 0
    assert {
        "type": "pane.turn_completed",
        "pane_id": _turn_api_pane()["pane_id"],
    } in client.subscriptions[0]["subscriptions"]
    with sqlite3.connect(str(backend.db_path)) as conn:
        assert conn.execute(
            """
            SELECT turn, outcome, refreshed_turn_id
            FROM herdr_turn_completions
            WHERE host_id = ?
            ORDER BY turn
            """,
            (backend.config.host_id,),
        ).fetchall() == []


def test_turn_completion_late_attaches_final_after_idle_event_wins_race(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    config = Config(
        host_id="turn-api-content-race",
        data_dir=tmp_path,
        db_path=tmp_path / "turn-api-content-race.db",
        herdr_backend="socket",
        herdr_bin="turn-adapter",
        herdr_timeout_seconds=0.5,
    )
    init_store(Path(config.db_path))
    backend = HerdrEventBackend(
        config,
        debounce_seconds=0,
        reconnect_delay_seconds=0,
    )
    pane_id = _turn_api_pane()["pane_id"]
    working_pane = {
        **_turn_api_pane(),
        "agent_status": "working",
    }
    backend.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "w123456789abcde", "name": "Build"}],
            panes=[working_pane],
        )
    )
    set_herdr_turn_watermark(
        backend.db_path,
        backend.config.host_id,
        pane_id,
        turn_epoch=7,
        last_turn=0,
    )

    # Reproduce the production ordering: the status stream closes the public
    # projection first, before any semantic adapter read has run.
    assert backend.queue_event_envelope(
        {
            "event": "pane.agent_status_changed",
            "data": {
                **working_pane,
                "agent_status": "idle",
            },
        }
    )
    idle_snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    assert idle_snapshot is not None
    idle_payload = turns_payload_from_store(
        backend.db_path,
        backend.config.host_id,
        snapshot=idle_snapshot,
    )
    assert idle_snapshot.workers[0].status == "idle"
    assert not any(
        turn.get("assistant_final_text") for turn in idle_payload["turns"]
    )

    adapter_calls: list[list[str]] = []
    final_payload = {
        "result": {
            "turn": {
                "available": True,
                "user_text": "reply with purple elephant 42",
                "assistant_final_text": "purple elephant 42",
                "complete": True,
                "has_open_turn": False,
                "source_turn_id": "event-close-race-turn",
            }
        }
    }

    def fake_run(args: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        adapter_calls.append(args)
        return subprocess.CompletedProcess(
            args=args,
            returncode=0,
            stdout=json.dumps(final_payload),
            stderr="",
        )

    monkeypatch.setattr(herdr_turns.subprocess, "run", fake_run)
    backend._turn_api_supported = True
    assert backend.queue_event_envelope(
        {
            "event": "pane.turn_completed",
            "data": {
                "pane": {
                    **working_pane,
                    "agent_status": "idle",
                },
                **_turn_record(1),
            },
        }
    )

    completed_snapshot = latest_snapshot(
        backend.db_path,
        backend.config.host_id,
    )
    assert completed_snapshot is not None
    captured_payload = turns_payload_from_store(
        backend.db_path,
        backend.config.host_id,
        snapshot=completed_snapshot,
    )
    captured = next(
        turn
        for turn in captured_payload["turns"]
        if turn.get("assistant_final_text") == "purple elephant 42"
    )
    assert captured["status"] == "idle"
    assert captured["complete"] is True
    assert adapter_calls == [
        [
            "turn-adapter",
            "pane",
            "turn",
            pane_id,
            "--last",
            "--format",
            "json",
        ]
    ]
    with closing(sqlite3.connect(str(backend.db_path))) as conn, conn:
        completion = conn.execute(
            """
            SELECT turn, refreshed_turn_id
            FROM herdr_turn_completions
            WHERE host_id = ? AND pane_id = ?
            """,
            (backend.config.host_id, pane_id),
        ).fetchone()
    assert completion == (1, captured["id"])


@pytest.mark.parametrize(
    "refresh_status",
    [
        "timeout",
        "failed",
        "stale_binding",
        "binding_missing",
        "binding_ambiguous",
        "store_unavailable",
    ],
)
def test_default_completion_processor_nonterminal_status_advances_with_diagnostic(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    refresh_status: str,
) -> None:
    backend = _backend(tmp_path, f"default-composition-{refresh_status}")
    backend.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "w123456789abcde", "name": "Build"}],
            panes=[_turn_api_pane()],
        )
    )
    pane_id = _turn_api_pane()["pane_id"]
    bindings = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
    )
    assert len(bindings) == 1
    set_herdr_turn_watermark(
        backend.db_path,
        backend.config.host_id,
        pane_id,
        turn_epoch=7,
        last_turn=0,
    )

    if refresh_status in {"timeout", "failed", "stale_binding"}:
        monkeypatch.setattr(
            herdr_turns,
            "_refresh_turn_binding",
            lambda *_args, **_kwargs: herdr_turns.TurnRefreshResult(
                refresh_status,
                0,
            ),
        )
    elif refresh_status == "binding_missing":
        monkeypatch.setattr(
            herdr_turns,
            "list_worker_bindings",
            lambda *_args, **_kwargs: [],
        )
    elif refresh_status == "binding_ambiguous":
        monkeypatch.setattr(
            herdr_turns,
            "list_worker_bindings",
            lambda *_args, **_kwargs: [bindings[0], bindings[0]],
        )
    else:
        def unavailable(*_args: Any, **_kwargs: Any) -> Any:
            raise RuntimeError("store unavailable")

        monkeypatch.setattr(herdr_turns, "list_worker_bindings", unavailable)

    assert backend.queue_event_envelope(
        {
            "event": "pane.turn_completed",
            "data": {"pane": _turn_api_pane(), **_turn_record(1)},
        }
    )

    watermark = get_herdr_turn_watermark(
        backend.db_path,
        backend.config.host_id,
        pane_id,
    )
    assert watermark is not None and watermark.last_turn == 1
    assert backend.operational_status["turn_completion_diagnostics"] == {
        f"herdr_turn_completion_refresh_skipped:{refresh_status}": 1
    }
    with closing(sqlite3.connect(str(backend.db_path))) as conn, conn:
        assert conn.execute(
            """
            SELECT turn, refreshed_turn_id
            FROM herdr_turn_completions
            WHERE host_id = ? AND pane_id = ?
            """,
            (backend.config.host_id, pane_id),
        ).fetchall() == [(1, None)]


def test_replay_timeout_on_one_pane_does_not_block_other_panes_or_subscribe(
    tmp_path: Path,
) -> None:
    pane_a = _turn_api_pane()
    pane_b = {
        **_turn_api_pane(),
        "pane_id": "w123456789abcde:pB",
        "terminal_id": "terminal-turn-api-b",
        "agent": "codex",
    }
    processed: list[str] = []

    def process(_config: Config, pane_id: str, **_kwargs: Any) -> Any:
        processed.append(pane_id)
        return SimpleNamespace(
            status="timeout" if pane_id == pane_a["pane_id"] else "updated",
            worker_id=pane_id,
            refreshed_turn_id=f"public-{pane_id}",
        )

    backend = _backend(tmp_path, "turn-api-pane-timeout")
    backend.turn_completion_processor = process
    backend.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "w123456789abcde", "name": "Build"}],
            panes=[pane_a, pane_b],
        )
    )
    for pane in (pane_a, pane_b):
        set_herdr_turn_watermark(
            backend.db_path,
            backend.config.host_id,
            pane["pane_id"],
            turn_epoch=7,
            last_turn=0,
        )

    class Client:
        subscriptions: list[dict[str, Any]] = []

        def pane_turns(self, params: Mapping[str, Any], **_kwargs: Any) -> Any:
            pane_id = str(params["pane_id"])
            return {
                "turns": {
                    "pane_id": pane_id,
                    "turn_epoch": 7,
                    "records": [{**_turn_record(1), "pane_id": pane_id}],
                    "truncated": False,
                    "oldest_available": 1,
                }
            }

        def subscribe(
            self,
            _method: str,
            params: Mapping[str, Any],
            **_kwargs: Any,
        ) -> Any:
            self.subscriptions.append(dict(params))
            return SimpleNamespace(subscription_id="healthy")

    client = Client()
    backend._replay_turns_after_reconcile(client)
    backend._subscribe_event_stream(client)

    assert set(processed) == {pane_a["pane_id"], pane_b["pane_id"]}
    assert len(client.subscriptions) == 1
    for pane in (pane_a, pane_b):
        watermark = get_herdr_turn_watermark(
            backend.db_path,
            backend.config.host_id,
            pane["pane_id"],
        )
        assert watermark is not None and watermark.last_turn == 1


def test_first_probe_pane_not_found_skips_only_that_pane(tmp_path: Path) -> None:
    pane_a = _turn_api_pane()
    pane_b = {
        **_turn_api_pane(),
        "pane_id": "w123456789abcde:pB",
        "terminal_id": "terminal-turn-api-b",
        "agent": "codex",
    }
    processed: list[str] = []
    backend = _backend(tmp_path, "turn-api-probe-pane-missing")
    backend.turn_completion_processor = (
        lambda _config, pane_id, **_kwargs: (
            processed.append(pane_id) or SimpleNamespace(status="unchanged")
        )
    )
    backend.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "w123456789abcde", "name": "Build"}],
            panes=[pane_a, pane_b],
        )
    )
    for pane in (pane_a, pane_b):
        set_herdr_turn_watermark(
            backend.db_path,
            backend.config.host_id,
            pane["pane_id"],
            turn_epoch=7,
            last_turn=0,
        )

    class Client:
        subscriptions: list[dict[str, Any]] = []

        def pane_turns(self, params: Mapping[str, Any], **_kwargs: Any) -> Any:
            pane_id = str(params["pane_id"])
            if pane_id == pane_a["pane_id"]:
                raise HerdrErrorResponse(
                    {"code": "pane_not_found", "message": "pane disappeared"},
                    "probe",
                )
            return {
                "turns": {
                    "pane_id": pane_id,
                    "turn_epoch": 7,
                    "records": [{**_turn_record(1), "pane_id": pane_id}],
                    "truncated": False,
                    "oldest_available": 1,
                }
            }

        def subscribe(
            self,
            _method: str,
            params: Mapping[str, Any],
            **_kwargs: Any,
        ) -> Any:
            self.subscriptions.append(dict(params))
            return SimpleNamespace(subscription_id="new")

    client = Client()
    backend._replay_turns_after_reconcile(client)
    backend._subscribe_event_stream(client)

    assert backend._turn_api_supported is True
    assert processed == [pane_b["pane_id"]]
    assert len(client.subscriptions) == 1
    assert {
        "type": "pane.turn_completed",
        "pane_id": pane_b["pane_id"],
    } in client.subscriptions[0]["subscriptions"]


def test_malformed_completion_is_quarantined_without_poisoning_batch(
    tmp_path: Path,
) -> None:
    backend = _backend(tmp_path, "turn-api-malformed-batch", debounce_seconds=1)
    backend.reconcile_once(client=_initial_pane_client())
    assert backend.queue_event_envelope(
        {
            "event": "pane.turn_completed",
            "data": {
                "pane_id": "pane-1",
                "turn": "not-an-integer",
                "turn_epoch": 7,
                "outcome": "completed",
                "completed_unix_ms": 1,
            },
        },
        flush=False,
    )
    assert backend.queue_event_envelope(_status_event("idle"), flush=False)

    backend.flush()

    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    assert snapshot is not None
    assert snapshot.workers[0].status == "idle"
    assert backend.health.outcome != "protocol_error"
    assert backend.operational_status["turn_completion_diagnostics"] == {
        "herdr_turn_completion_record_quarantined": 1
    }


def test_replay_gap_inside_records_records_break_without_processing(
    tmp_path: Path,
) -> None:
    processed: list[str] = []
    backend = _turn_api_backend(
        tmp_path,
        "turn-api-interior-gap",
        lambda _config, pane_id, **_kwargs: (
            processed.append(pane_id) or SimpleNamespace(status="updated")
        ),
    )
    pane_id = _turn_api_pane()["pane_id"]
    set_herdr_turn_watermark(
        backend.db_path,
        backend.config.host_id,
        pane_id,
        turn_epoch=7,
        last_turn=2,
    )

    class GapClient:
        def pane_turns(self, _params: Mapping[str, Any], **_kwargs: Any) -> Any:
            return {
                "turns": {
                    "pane_id": pane_id,
                    "turn_epoch": 7,
                    "records": [_turn_record(3), _turn_record(5)],
                    "truncated": False,
                    "oldest_available": 3,
                }
            }

    backend._replay_turns_after_reconcile(GapClient())

    assert processed == []
    watermark = get_herdr_turn_watermark(
        backend.db_path,
        backend.config.host_id,
        pane_id,
    )
    assert watermark is not None
    assert watermark.last_turn == 5
    assert watermark.last_completeness_break_reason == "replay_gap"


def test_optional_signature_fallback_never_double_invokes_typeerror(
    tmp_path: Path,
) -> None:
    calls = 0

    def broken_processor(*_args: Any, **_kwargs: Any) -> Any:
        nonlocal calls
        calls += 1
        raise TypeError("processor body failed")

    backend = _turn_api_backend(
        tmp_path,
        "turn-api-no-double-invoke",
        broken_processor,
    )
    with pytest.raises(TypeError, match="processor body failed"):
        backend._completion_processor_result(
            herdr_events._turn_completion_record(
                {"pane_id": _turn_api_pane()["pane_id"], **_turn_record(1)}
            )
        )
    assert calls == 1

    pane_calls = 0

    class BrokenClient:
        def pane_turns(self, _params: Mapping[str, Any], **_kwargs: Any) -> Any:
            nonlocal pane_calls
            pane_calls += 1
            raise TypeError("pane.turns body failed")

    with pytest.raises(TypeError, match="pane.turns body failed"):
        backend._call_pane_turns(
            BrokenClient(),
            _turn_api_pane()["pane_id"],
            since=0,
            expected_epoch=None,
        )
    assert pane_calls == 1


def test_turn_api_run_loop_closes_probe_to_subscribe_completion_race(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, "turn-api-subscribe-race")
    init_store(Path(config.db_path))
    pane_id = _turn_api_pane()["pane_id"]
    processed: list[str] = []
    operations: list[str] = []

    class RaceClient(_StaticClient):
        def __init__(self) -> None:
            super().__init__(
                workspaces=[{"id": "w123456789abcde", "name": "Build"}],
                panes=[_turn_api_pane()],
            )
            self.subscribed = False

        def connect(self) -> None:
            return None

        def close(self) -> None:
            return None

        def pane_turns(
            self,
            params: Mapping[str, Any],
            **_kwargs: Any,
        ) -> Any:
            operations.append("replay" if self.subscribed else "probe")
            records = [_turn_record(1)] if self.subscribed else []
            if self.subscribed:
                backend.stop_event.set()
            return {
                "turns": {
                    "pane_id": pane_id,
                    "turn_epoch": 7,
                    "records": records,
                    "truncated": False,
                    "oldest_available": 1 if records else None,
                }
            }

        def subscribe(
            self,
            _method: str,
            params: Mapping[str, Any],
            **_kwargs: Any,
        ) -> Any:
            operations.append("subscribe")
            assert {
                "type": "pane.turn_completed",
                "pane_id": pane_id,
            } in params["subscriptions"]
            self.subscribed = True
            return SimpleNamespace(subscription_id="race")

    client = RaceClient()
    backend = HerdrEventBackend(
        config,
        client_factory=lambda _config: client,
        debounce_seconds=0,
        reconnect_delay_seconds=0,
        turn_completion_processor=lambda _config, current_pane, **_kwargs: (
            processed.append(current_pane)
            or SimpleNamespace(status="unchanged")
        ),
    )

    backend.run_forever()

    assert operations == ["probe", "subscribe", "replay"]
    assert processed == [pane_id]
    watermark = get_herdr_turn_watermark(
        backend.db_path,
        backend.config.host_id,
        pane_id,
    )
    assert watermark is not None
    assert (watermark.turn_epoch, watermark.last_turn) == (7, 1)


def test_turn_api_restart_replays_exactly_the_missed_turn(tmp_path: Path) -> None:
    pane_id = _turn_api_pane()["pane_id"]
    first = _turn_api_backend(
        tmp_path,
        "turn-api-restart",
        lambda *_args, **_kwargs: SimpleNamespace(status="unchanged"),
    )
    set_herdr_turn_watermark(
        first.db_path,
        first.config.host_id,
        pane_id,
        turn_epoch=7,
        last_turn=2,
    )
    processed: list[str] = []
    restarted = HerdrEventBackend(
        first.config,
        debounce_seconds=0,
        reconnect_delay_seconds=0,
        turn_completion_processor=lambda _config, current_pane, **_kwargs: (
            processed.append(current_pane)
            or SimpleNamespace(status="unchanged")
        ),
    )
    restarted.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "w123456789abcde", "name": "Build"}],
            panes=[_turn_api_pane()],
        )
    )

    class ReplayClient:
        params: dict[str, Any] | None = None

        def pane_turns(self, params: Mapping[str, Any], **_kwargs: Any) -> Any:
            self.params = dict(params)
            return {
                "turns": {
                    "pane_id": pane_id,
                    "turn_epoch": 7,
                    "records": [_turn_record(3)],
                    "truncated": False,
                    "oldest_available": 1,
                }
            }

    client = ReplayClient()
    restarted._replay_turns_after_reconcile(client)

    assert client.params == {
        "pane_id": pane_id,
        "since": 2,
        "expected_epoch": 7,
    }
    assert processed == [pane_id]
    watermark = get_herdr_turn_watermark(
        restarted.db_path,
        restarted.config.host_id,
        pane_id,
    )
    assert watermark is not None and watermark.last_turn == 3


@pytest.mark.parametrize(
    ("mode", "expected_epoch", "expected_turn", "expected_reason"),
    [
        ("truncated", 7, 12, "replay_truncated"),
        ("epoch", 8, 1, "turn_epoch_mismatch"),
    ],
)
def test_turn_api_breaks_rebaseline_without_inferring_completion(
    tmp_path: Path,
    mode: str,
    expected_epoch: int,
    expected_turn: int,
    expected_reason: str,
) -> None:
    processed: list[str] = []
    backend = _turn_api_backend(
        tmp_path,
        f"turn-api-break-{mode}",
        lambda _config, pane_id, **_kwargs: (
            processed.append(pane_id)
            or SimpleNamespace(status="updated")
        ),
    )
    pane_id = _turn_api_pane()["pane_id"]
    set_herdr_turn_watermark(
        backend.db_path,
        backend.config.host_id,
        pane_id,
        turn_epoch=7,
        last_turn=2,
    )

    class BreakClient:
        calls = 0

        def pane_turns(self, _params: Mapping[str, Any], **_kwargs: Any) -> Any:
            self.calls += 1
            if mode == "epoch" and self.calls == 1:
                raise HerdrErrorResponse(
                    {
                        "code": "turn_epoch_mismatch",
                        "message": "epoch changed",
                    },
                    "epoch",
                )
            records = (
                [_turn_record(1, epoch=8)]
                if mode == "epoch"
                else [_turn_record(11), _turn_record(12)]
            )
            return {
                "turns": {
                    "pane_id": pane_id,
                    "turn_epoch": expected_epoch,
                    "records": records,
                    "truncated": mode == "truncated",
                    "oldest_available": records[0]["turn"],
                }
            }

    backend._replay_turns_after_reconcile(BreakClient())

    assert processed == []
    watermark = get_herdr_turn_watermark(
        backend.db_path,
        backend.config.host_id,
        pane_id,
    )
    assert watermark is not None
    assert (watermark.turn_epoch, watermark.last_turn) == (
        expected_epoch,
        expected_turn,
    )
    assert watermark.completeness_break_count == 1
    assert watermark.last_completeness_break_reason == expected_reason


def test_live_aborted_completion_refreshes_then_advances_with_provenance(
    tmp_path: Path,
) -> None:
    processed: list[str] = []
    backend = _turn_api_backend(
        tmp_path,
        "turn-api-live-abort",
        lambda _config, pane_id, **_kwargs: (
            processed.append(pane_id)
            or SimpleNamespace(
                status="updated",
                worker_id="claude",
                refreshed_turn_id="public-aborted-turn",
            )
        ),
    )
    pane_id = _turn_api_pane()["pane_id"]
    set_herdr_turn_watermark(
        backend.db_path,
        backend.config.host_id,
        pane_id,
        turn_epoch=7,
        last_turn=1,
    )
    backend._turn_api_supported = True

    assert backend.queue_event_envelope(
        {
            "event": "pane.turn_completed",
            "data": {
                "pane": _turn_api_pane(),
                **_turn_record(2, outcome="aborted"),
            },
        }
    )

    assert processed == [pane_id]
    watermark = get_herdr_turn_watermark(
        backend.db_path,
        backend.config.host_id,
        pane_id,
    )
    assert watermark is not None and watermark.last_turn == 2
    with sqlite3.connect(str(backend.db_path)) as conn:
        assert conn.execute(
            """
            SELECT outcome, refreshed_turn_id
            FROM herdr_turn_completions
            WHERE host_id = ? AND pane_id = ? AND turn = 2
            """,
            (backend.config.host_id, pane_id),
        ).fetchone() == ("aborted", "public-aborted-turn")


def test_status_turn_hints_never_advance_completion_watermarks(
    tmp_path: Path,
) -> None:
    processed: list[str] = []
    backend = _turn_api_backend(
        tmp_path,
        "turn-api-hints-only",
        lambda _config, pane_id, **_kwargs: (
            processed.append(pane_id)
            or SimpleNamespace(status="updated")
        ),
    )
    pane_id = _turn_api_pane()["pane_id"]

    assert backend.queue_event_envelope(
        {
            "event": "pane.agent_status_changed",
            "data": {
                "pane_id": pane_id,
                "workspace_id": "w123456789abcde",
                "agent": "claude",
                "agent_status": "idle",
                "turn": 19,
                "turn_epoch": 7,
            },
        }
    )

    assert processed == []
    assert get_herdr_turn_watermark(
        backend.db_path,
        backend.config.host_id,
        pane_id,
    ) is None
