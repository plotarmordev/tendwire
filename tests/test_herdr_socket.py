from __future__ import annotations

import json
import socket
import threading
import time

import pytest

from tendwire.backends.herdr_protocol import HerdrFrameTooLargeError, HerdrRequestIdMismatchError
from tendwire.backends.herdr_socket import (
    HerdrSocketClient,
    HerdrSocketConnectionError,
    HerdrSocketDisconnectedError,
    HerdrSocketTimeoutError,
    _MAX_FRAME_BYTES,
)


def _serve(path, responses, requests) -> threading.Thread:
    ready = threading.Event()

    def run() -> None:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(str(path))
            server.listen()
            ready.set()
            for result in responses:
                conn, _ = server.accept()
                with conn:
                    request = json.loads(conn.makefile("rb").readline())
                    requests.append(request)
                    response_id = result.pop("response_id", request["id"])
                    conn.sendall(json.dumps({"id": response_id, **result}).encode() + b"\n")

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    assert ready.wait(2)
    return thread


class _ScriptedSocket:
    def __init__(self, response: bytes) -> None:
        self.response = response
        self.sent = b""
        self.closed = False
        self.shutdown_calls = 0

    def settimeout(self, _value) -> None:
        return None

    def sendall(self, payload: bytes) -> None:
        self.sent += payload

    def recv(self, _size: int) -> bytes:
        response, self.response = self.response, b""
        return response

    def shutdown(self, _how: int) -> None:
        self.shutdown_calls += 1

    def close(self) -> None:
        self.closed = True


def test_exact_request_transcript_and_success_reap(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(
        "tendwire.backends.herdr_protocol.new_request_id",
        lambda: "request-fixed",
    )
    sock = _ScriptedSocket(
        b'{"id":"request-fixed","result":{"status":"ok"}}\n'
    )
    client = HerdrSocketClient(str(tmp_path / "unused.sock"), timeout=1)
    client._socket = sock

    assert client.request("pane.list", {"workspace_id": "workspace-a"}) == {
        "status": "ok"
    }
    assert sock.sent == (
        b'{"id":"request-fixed","method":"pane.list",'
        b'"params":{"workspace_id":"workspace-a"}}\n'
    )
    assert client._socket is None
    assert sock.shutdown_calls == 1
    assert sock.closed is True


@pytest.mark.parametrize(
    ("response", "error"),
    [
        (b"", HerdrSocketDisconnectedError),
        (b'{"id":"wrong","result":{}}\n', HerdrRequestIdMismatchError),
    ],
)
def test_eof_and_correlation_faults_reap_connection(
    monkeypatch,
    tmp_path,
    response: bytes,
    error: type[Exception],
) -> None:
    monkeypatch.setattr(
        "tendwire.backends.herdr_protocol.new_request_id",
        lambda: "request-fixed",
    )
    sock = _ScriptedSocket(response)
    client = HerdrSocketClient(str(tmp_path / "unused.sock"), timeout=1)
    client._socket = sock

    with pytest.raises(error):
        client.request("agent.list")
    assert client._socket is None
    assert sock.closed is True


def test_discovery_lists_use_one_request_per_connection(tmp_path) -> None:
    path = tmp_path / "herdr.sock"
    requests: list[dict] = []
    thread = _serve(
        path,
        [
            {"result": {"workspaces": []}},
            {"result": {"panes": []}},
            {"result": {"agents": []}},
        ],
        requests,
    )
    client = HerdrSocketClient(str(path), timeout=1)
    assert client.workspace_list() == {"workspaces": []}
    assert client._socket is None
    assert client.pane_list() == {"panes": []}
    assert client._socket is None
    assert client.agent_list() == {"agents": []}
    assert client._socket is None
    thread.join(2)
    assert [item["method"] for item in requests] == [
        "workspace.list",
        "pane.list",
        "agent.list",
    ]


def test_acp_methods_use_frozen_socket_shapes_without_connection_reuse(tmp_path) -> None:
    path = tmp_path / "herdr.sock"
    requests: list[dict] = []
    thread = _serve(
        path,
        [
            {"result": {"type": "agent_acp_status"}},
            {"result": {"type": "agent_acp_endpoint"}},
        ],
        requests,
    )
    client = HerdrSocketClient(str(path), timeout=1)
    assert client.agent_acp_status("term") == {"type": "agent_acp_status"}
    assert client._socket is None
    assert client.agent_acp_endpoint("term") == {"type": "agent_acp_endpoint"}
    assert client._socket is None
    thread.join(2)
    assert [(item["method"], item["params"]) for item in requests] == [
        ("agent.acp_status", {"target": "term"}),
        ("agent.acp_endpoint", {"target": "term"}),
    ]


def test_response_id_mismatch_fails_closed(tmp_path) -> None:
    path = tmp_path / "herdr.sock"
    thread = _serve(path, [{"response_id": "wrong", "result": {}}], [])
    with pytest.raises(HerdrRequestIdMismatchError):
        HerdrSocketClient(str(path), timeout=1).agent_acp_status("term")
    thread.join(2)


def test_connection_and_timeout_fail_closed(tmp_path) -> None:
    with pytest.raises(HerdrSocketConnectionError):
        HerdrSocketClient(str(tmp_path / "missing.sock"), timeout=0.01).pane_list()

    class TimedOutSocket:
        def settimeout(self, _value): return None
        def recv(self, _size): raise socket.timeout
        def shutdown(self, _how): return None
        def close(self): return None

    client = HerdrSocketClient(str(tmp_path / "unused.sock"), timeout=1)
    client._socket = TimedOutSocket()
    with pytest.raises(HerdrSocketTimeoutError):
        client._read_line(deadline=time.monotonic() + 1)


def test_request_and_response_frames_are_bounded(tmp_path) -> None:
    class UnusedSocket:
        def settimeout(self, _value): return None
        def sendall(self, _payload): raise AssertionError("oversize request must not be sent")
        def recv(self, _size): return b"x"
        def shutdown(self, _how): return None
        def close(self): return None

    outbound = HerdrSocketClient(str(tmp_path / "unused.sock"), timeout=1)
    outbound._socket = UnusedSocket()
    with pytest.raises(HerdrFrameTooLargeError):
        outbound.request("pane.list", {"padding": "x" * _MAX_FRAME_BYTES})

    inbound = HerdrSocketClient(str(tmp_path / "unused.sock"), timeout=1)
    inbound._socket = UnusedSocket()
    inbound._buffer.extend(b"x" * _MAX_FRAME_BYTES)
    with pytest.raises(HerdrFrameTooLargeError):
        inbound._read_line(deadline=time.monotonic() + 1)
