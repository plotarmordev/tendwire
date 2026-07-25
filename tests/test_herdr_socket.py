"""Tests for the inactive Herdr Unix socket client."""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from tendwire.backends.herdr_protocol import (
    HerdrEnvelopeError,
    HerdrErrorResponse,
    HerdrMalformedLineError,
    HerdrRequestIdMismatchError,
    HERDR_EVENTS_SUBSCRIBE_METHOD,
    HERDR_OFFICIAL_EVENT_NAMES,
    build_events_subscribe_params,
)
from tendwire.backends.herdr_socket import (
    HerdrSocketClient,
    HerdrSocketDisconnectedError,
    HerdrSocketTimeoutError,
)


class _Connection:
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

    def send_json(self, payload: dict[str, Any]) -> None:
        self.conn.sendall(json.dumps(payload, separators=(",", ":")).encode("utf-8") + b"\n")

    def send_bytes(self, payload: bytes) -> None:
        self.conn.sendall(payload)


class _FakeHerdrServer:
    def __init__(self, tmp_path: Path, handler: Callable[[_Connection], None]) -> None:
        self.path = tmp_path / f"herdr-{time.monotonic_ns()}.sock"
        self.handler = handler
        self.requests: list[dict[str, Any]] = []
        self.errors: list[BaseException] = []
        self._ready = threading.Event()
        self._done = threading.Event()
        self._listener: socket.socket | None = None
        self._thread: threading.Thread | None = None

    def __enter__(self) -> "_FakeHerdrServer":
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(self.path))
        listener.listen(1)
        listener.settimeout(0.2)
        self._listener = listener
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        if not self._ready.wait(1):
            raise AssertionError("fake Herdr server did not start")
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
            raise AssertionError(f"fake Herdr server failed: {self.errors!r}")

    def _run(self) -> None:
        self._ready.set()
        try:
            assert self._listener is not None
            conn, _addr = self._listener.accept()
            with conn:
                self.handler(_Connection(conn, self.requests))
        except OSError:
            pass
        except BaseException as exc:
            self.errors.append(exc)
        finally:
            self._done.set()


class _FakeOneShotHerdrServer:
    def __init__(self, tmp_path: Path, handler: Callable[[_Connection], None], *, connections: int) -> None:
        self.path = tmp_path / f"herdr-oneshot-{time.monotonic_ns()}.sock"
        self.handler = handler
        self.connections = connections
        self.requests: list[dict[str, Any]] = []
        self.errors: list[BaseException] = []
        self._ready = threading.Event()
        self._listener: socket.socket | None = None
        self._thread: threading.Thread | None = None

    def __enter__(self) -> "_FakeOneShotHerdrServer":
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(self.path))
        listener.listen(self.connections)
        listener.settimeout(0.5)
        self._listener = listener
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        if not self._ready.wait(1):
            raise AssertionError("fake one-shot Herdr server did not start")
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
            raise AssertionError(f"fake one-shot Herdr server failed: {self.errors!r}")

    def _run(self) -> None:
        self._ready.set()
        try:
            assert self._listener is not None
            for _index in range(self.connections):
                conn, _addr = self._listener.accept()
                with conn:
                    self.handler(_Connection(conn, self.requests))
        except OSError:
            pass
        except BaseException as exc:
            self.errors.append(exc)


def _responding_handler(result: Any) -> Callable[[_Connection], None]:
    def handler(conn: _Connection) -> None:
        request = conn.read_request()
        conn.send_json({"id": request["id"], "result": result})

    return handler


def test_client_successful_request_matches_id_and_returns_raw_result(tmp_path: Path) -> None:
    result = {"items": [{"id": "w-1", "future": {"kept": True}}]}
    with _FakeHerdrServer(tmp_path, _responding_handler(result)) as server:
        client = HerdrSocketClient(str(server.path), timeout=1)

        assert client.request("workspace.list", {"scope": "all"}) == result
        client.close()

    assert server.requests[0]["method"] == "workspace.list"
    assert server.requests[0]["params"] == {"scope": "all"}


def test_client_pane_turns_wrapper_uses_additive_method(tmp_path: Path) -> None:
    result = {
        "type": "pane_turns",
        "turns": {
            "pane_id": "w1:p1",
            "turn_epoch": 3,
            "records": [],
            "truncated": False,
        },
    }
    with _FakeHerdrServer(tmp_path, _responding_handler(result)) as server:
        client = HerdrSocketClient(str(server.path), timeout=1)

        assert client.pane_turns(
            {"pane_id": "w1:p1", "since": 4, "expected_epoch": 3}
        ) == result
        client.close()

    assert server.requests[0]["method"] == "pane.turns"
    assert server.requests[0]["params"] == {
        "pane_id": "w1:p1",
        "since": 4,
        "expected_epoch": 3,
    }
    assert isinstance(server.requests[0]["id"], str)


def test_client_reconnects_after_one_shot_response_connection_closes(tmp_path: Path) -> None:
    results = {
        "workspace.list": {"workspaces": []},
        "agent.list": {"agents": []},
    }

    def handler(conn: _Connection) -> None:
        request = conn.read_request()
        conn.send_json({"id": request["id"], "result": results[request["method"]]})

    with _FakeOneShotHerdrServer(tmp_path, handler, connections=2) as server:
        client = HerdrSocketClient(str(server.path), timeout=1)

        assert client.request("workspace.list") == {"workspaces": []}
        assert client.request("agent.list") == {"agents": []}
        client.close()

    assert [request["method"] for request in server.requests] == ["workspace.list", "agent.list"]


def test_client_timeout_waiting_for_response(tmp_path: Path) -> None:
    def handler(conn: _Connection) -> None:
        conn.read_request()
        time.sleep(0.15)

    with _FakeHerdrServer(tmp_path, handler) as server:
        client = HerdrSocketClient(str(server.path), timeout=0.05)
        with pytest.raises(HerdrSocketTimeoutError):
            client.request("workspace.list")
        client.close()


def test_client_malformed_response_raises_protocol_error(tmp_path: Path) -> None:
    def handler(conn: _Connection) -> None:
        conn.read_request()
        conn.send_bytes(b"{not json}\n")

    with _FakeHerdrServer(tmp_path, handler) as server:
        client = HerdrSocketClient(str(server.path), timeout=1)
        with pytest.raises(HerdrMalformedLineError):
            client.request("workspace.list")
        client.close()


def test_client_malformed_envelope_shape_raises_protocol_error(tmp_path: Path) -> None:
    def handler(conn: _Connection) -> None:
        request = conn.read_request()
        conn.send_json({"id": request["id"], "not_result": True})

    with _FakeHerdrServer(tmp_path, handler) as server:
        client = HerdrSocketClient(str(server.path), timeout=1)
        with pytest.raises(HerdrEnvelopeError):
            client.request("workspace.list")
        client.close()


def test_ordinary_request_rejects_uncorrelated_empty_id_error(tmp_path: Path) -> None:
    def handler(conn: _Connection) -> None:
        conn.read_request()
        conn.send_json(
            {
                "id": "",
                "error": {
                    "code": "invalid_request",
                    "message": "uncorrelated ordinary request error",
                },
            }
        )

    with _FakeHerdrServer(tmp_path, handler) as server:
        client = HerdrSocketClient(str(server.path), timeout=1)
        with pytest.raises(HerdrEnvelopeError):
            client.request("workspace.list")
        client.close()


def test_ordinary_request_rejects_idless_unknown_variant_error(
    tmp_path: Path,
) -> None:
    def handler(conn: _Connection) -> None:
        conn.read_request()
        conn.send_json(
            {
                "id": "",
                "error": {
                    "code": "invalid_request",
                    "message": "invalid request: unknown variant `pane.turns`, expected one of `pane.read`, `pane.list`, `agent.list`",
                },
            }
        )

    with _FakeHerdrServer(tmp_path, handler) as server:
        client = HerdrSocketClient(str(server.path), timeout=1)
        with pytest.raises(HerdrEnvelopeError):
            client.pane_turns({"pane_id": "w1:p1", "since": 0})
        client.close()


def test_client_non_utf8_response_raises_protocol_error(tmp_path: Path) -> None:
    def handler(conn: _Connection) -> None:
        conn.read_request()
        conn.send_bytes(b"\xff\n")

    with _FakeHerdrServer(tmp_path, handler) as server:
        client = HerdrSocketClient(str(server.path), timeout=1)
        with pytest.raises(HerdrMalformedLineError):
            client.request("workspace.list")
        client.close()


def test_client_disconnect_before_response_raises(tmp_path: Path) -> None:
    def handler(conn: _Connection) -> None:
        conn.read_request()

    with _FakeHerdrServer(tmp_path, handler) as server:
        client = HerdrSocketClient(str(server.path), timeout=1)
        with pytest.raises(HerdrSocketDisconnectedError):
            client.request("workspace.list")
        client.close()


def test_client_handles_partial_reads_split_across_recv_boundaries(tmp_path: Path) -> None:
    result = {"text": "hello"}

    def handler(conn: _Connection) -> None:
        request = conn.read_request()
        response = json.dumps({"id": request["id"], "result": result}).encode("utf-8") + b"\n"
        conn.send_bytes(response[:7])
        time.sleep(0.01)
        conn.send_bytes(response[7:])

    with _FakeHerdrServer(tmp_path, handler) as server:
        client = HerdrSocketClient(str(server.path), timeout=1)
        assert client.request("pane.read", {"pane_id": "p-1"}) == result
        client.close()


def test_client_error_response_raises_with_raw_error_payload(tmp_path: Path) -> None:
    error = {"code": "not_found", "message": "missing", "extra": {"kept": True}}

    def handler(conn: _Connection) -> None:
        request = conn.read_request()
        conn.send_json({"id": request["id"], "error": error, "future": "ignored"})

    with _FakeHerdrServer(tmp_path, handler) as server:
        client = HerdrSocketClient(str(server.path), timeout=1)
        with pytest.raises(HerdrErrorResponse) as excinfo:
            client.request("pane.get", {"pane_id": "missing"})
        client.close()

    assert excinfo.value.error == error


def test_client_response_id_mismatch_raises(tmp_path: Path) -> None:
    def handler(conn: _Connection) -> None:
        conn.read_request()
        conn.send_json({"id": "wrong-id", "result": {"ok": True}})

    with _FakeHerdrServer(tmp_path, handler) as server:
        client = HerdrSocketClient(str(server.path), timeout=1)
        with pytest.raises(HerdrRequestIdMismatchError):
            client.request("workspace.list")
        client.close()


def test_subscription_ack_events_and_stream_termination(tmp_path: Path) -> None:
    events = [
        {"event": "pane.output", "payload": {"text": "one"}, "future": {"kept": 1}},
        {"event": "pane.output", "payload": {"text": "two"}, "future": {"kept": 2}},
    ]

    def handler(conn: _Connection) -> None:
        request = conn.read_request()
        conn.send_json({"id": request["id"], "result": {"subscribed": True, "raw": [1]}})
        for event in events:
            conn.send_json({"id": request["id"], **event})

    with _FakeHerdrServer(tmp_path, handler) as server:
        client = HerdrSocketClient(str(server.path), timeout=1)
        stream = client.subscribe("pane.watch", {"pane_id": "p-1"})

        assert stream.ack == {"subscribed": True, "raw": [1]}
        assert list(stream) == [
            {"id": server.requests[0]["id"], **events[0]},
            {"id": server.requests[0]["id"], **events[1]},
        ]
        client.close()


def test_subscription_accepts_uncorrelated_idless_event_data_frames(tmp_path: Path) -> None:
    events = [
        {"event": "pane.agent_status_changed", "data": {"status": "blocked"}},
        {"event": "pane.closed", "data": {"pane_id": "p-1"}},
    ]

    def handler(conn: _Connection) -> None:
        request = conn.read_request()
        conn.send_json({"id": request["id"], "result": {"subscribed": True}})
        for event in events:
            conn.send_json(event)

    with _FakeHerdrServer(tmp_path, handler) as server:
        client = HerdrSocketClient(str(server.path), timeout=1)
        stream = client.subscribe("events.subscribe", {"subscriptions": []})

        assert stream.ack == {"subscribed": True}
        assert list(stream) == events
        client.close()


def test_subscription_buffers_idless_event_before_correlated_ack(tmp_path: Path) -> None:
    event = {
        "event": "pane.output_matched",
        "data": {"pane_id": "p-1"},
    }

    def handler(conn: _Connection) -> None:
        request = conn.read_request()
        conn.send_json(event)
        conn.send_json({"id": request["id"], "result": {"subscribed": True}})

    with _FakeHerdrServer(tmp_path, handler) as server:
        client = HerdrSocketClient(str(server.path), timeout=1)
        stream = client.events_subscribe(["pane.output_matched"])

        assert stream.ack == {"subscribed": True}
        assert list(stream) == [event]
        client.close()


def test_subscription_surfaces_herdr_075_empty_id_schema_error(tmp_path: Path) -> None:
    def handler(conn: _Connection) -> None:
        conn.read_request()
        conn.send_json(
            {
                "id": "",
                "error": {
                    "code": "invalid_request",
                    "message": "invalid request: missing field pane_id",
                },
            }
        )

    with _FakeHerdrServer(tmp_path, handler) as server:
        client = HerdrSocketClient(str(server.path), timeout=1)
        with pytest.raises(HerdrErrorResponse, match="missing field pane_id"):
            client.events_subscribe(["pane.agent_status_changed"])
        client.close()


@pytest.mark.parametrize(
    "error",
    [
        {"code": "permission_denied", "message": "invalid request: denied"},
        {"code": "invalid_request", "message": "subscription unavailable"},
    ],
)
def test_subscription_rejects_other_empty_id_errors(
    tmp_path: Path,
    error: dict[str, str],
) -> None:
    def handler(conn: _Connection) -> None:
        conn.read_request()
        conn.send_json({"id": "", "error": error})

    with _FakeHerdrServer(tmp_path, handler) as server:
        client = HerdrSocketClient(str(server.path), timeout=1)
        with pytest.raises(HerdrEnvelopeError):
            client.events_subscribe(["pane.agent_status_changed"])
        client.close()


def test_ordinary_request_rejects_herdr_075_empty_id_error(tmp_path: Path) -> None:
    def handler(conn: _Connection) -> None:
        conn.read_request()
        conn.send_json(
            {
                "id": "",
                "error": {
                    "code": "invalid_request",
                    "message": "uncorrelated ordinary request error",
                },
            }
        )

    with _FakeHerdrServer(tmp_path, handler) as server:
        client = HerdrSocketClient(str(server.path), timeout=1)
        with pytest.raises(HerdrEnvelopeError):
            client.pane_list()
        client.close()


def test_events_subscribe_wrapper_sends_official_method_and_params(tmp_path: Path) -> None:
    event_names = ("workspace.created", "pane.agent_status_changed")

    def handler(conn: _Connection) -> None:
        request = conn.read_request()
        conn.send_json({"id": request["id"], "result": {"subscribed": True}})

    with _FakeHerdrServer(tmp_path, handler) as server:
        client = HerdrSocketClient(str(server.path), timeout=1)
        stream = client.events_subscribe(event_names)
        client.close()

    assert stream.ack == {"subscribed": True}
    assert server.requests[0]["method"] == HERDR_EVENTS_SUBSCRIBE_METHOD
    assert server.requests[0]["params"] == build_events_subscribe_params(event_names)


@pytest.mark.parametrize("event_name", HERDR_OFFICIAL_EVENT_NAMES)
def test_events_subscribe_wrapper_accepts_each_official_event_name(
    tmp_path: Path,
    event_name: str,
) -> None:
    def handler(conn: _Connection) -> None:
        request = conn.read_request()
        conn.send_json({"id": request["id"], "result": {"subscribed": event_name}})

    with _FakeHerdrServer(tmp_path, handler) as server:
        client = HerdrSocketClient(str(server.path), timeout=1)
        stream = client.events_subscribe([event_name])
        client.close()

    assert stream.ack == {"subscribed": event_name}
    assert server.requests[0]["method"] == HERDR_EVENTS_SUBSCRIBE_METHOD
    assert server.requests[0]["params"] == {"subscriptions": [{"type": event_name}]}


def test_context_manager_and_close_are_idempotent(tmp_path: Path) -> None:
    with _FakeHerdrServer(tmp_path, _responding_handler({"ok": True})) as server:
        with HerdrSocketClient(str(server.path), timeout=1) as client:
            assert client.request("agent.get", {"agent_id": "a-1"}) == {"ok": True}
            client.close()
            client.close()


@pytest.mark.parametrize(
    ("wrapper_name", "method", "params", "result"),
    [
        ("workspace_list", "workspace.list", {"all": True}, {"workspaces": [{"id": "w"}]}),
        ("tab_list", "tab.list", {"workspace_id": "w"}, {"tabs": [{"id": "t"}]}),
        ("pane_list", "pane.list", {"tab_id": "t"}, {"panes": [{"id": "p"}]}),
        ("agent_list", "agent.list", {"workspace_id": "w"}, {"agents": [{"id": "a"}]}),
        ("pane_get", "pane.get", {"pane_id": "p"}, {"id": "p", "raw": {"kept": True}}),
        ("agent_get", "agent.get", {"agent_id": "a"}, {"id": "a", "raw": {"kept": True}}),
        ("pane_read", "pane.read", {"pane_id": "p", "limit": 20}, {"text": "raw"}),
    ],
)
def test_allowed_read_wrappers_send_exact_method_and_params(
    tmp_path: Path,
    wrapper_name: str,
    method: str,
    params: dict[str, Any],
    result: dict[str, Any],
) -> None:
    with _FakeHerdrServer(tmp_path, _responding_handler(result)) as server:
        client = HerdrSocketClient(str(server.path), timeout=1)

        assert getattr(client, wrapper_name)(params) == result
        client.close()

    assert server.requests[0]["method"] == method
    assert server.requests[0]["params"] == params


def test_agent_send_is_the_only_exposed_mutate_wrapper_and_shape_is_exact(tmp_path: Path) -> None:
    result = {"accepted": True, "opaque": {"server": "kept"}}
    params = {"agent_id": "a-1", "text": "hello"}
    with _FakeHerdrServer(tmp_path, _responding_handler(result)) as server:
        client = HerdrSocketClient(str(server.path), timeout=1)

        assert client.agent_send(params) == result
        client.close()

    assert server.requests[0]["method"] == "agent.send"
    assert server.requests[0]["params"] == params

    excluded_public_api = {
        "pane_send_text",
        "pane_send_keys",
        "pane_run",
        "send_text",
        "send_keys",
        "run",
        "shell",
        "raw_terminal_control",
        "source_mode",
        "connector_polling",
        "poll_connectors",
        "event_backend_replacement",
    }
    for name in excluded_public_api:
        assert not hasattr(client, name), name


def test_cli_import_does_not_load_socket_client_by_default() -> None:
    code = """
import sys
before = set(sys.modules)
import tendwire.cli
loaded = set(sys.modules) - before
for name in sorted(loaded):
    if name in {"tendwire.backends.herdr_socket", "tendwire.backends.herdr_protocol"}:
        print(name)
"""
    env = os.environ.copy()
    env["PYTHONPATH"] = os.path.join(os.path.dirname(__file__), "..", "src")
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    assert result.returncode == 0
    assert result.stdout == ""


def test_existing_production_backend_files_do_not_import_socket_client() -> None:
    root = Path(__file__).resolve().parents[1]
    for relative in (
        "src/tendwire/cli.py",
        "src/tendwire/backends/herdr_cli.py",
        "src/tendwire/backends/herdr_command.py",
    ):
        text = (root / relative).read_text(encoding="utf-8")
        assert "herdr_socket" not in text
