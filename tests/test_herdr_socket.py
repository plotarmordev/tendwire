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


def test_lifecycle_and_acp_methods_use_frozen_socket_shapes(tmp_path) -> None:
    path = tmp_path / "herdr.sock"
    requests: list[dict] = []
    thread = _serve(
        path,
        [
            {"result": {"panes": []}},
            {"result": {"type": "agent_acp_status"}},
            {"result": {"type": "agent_acp_endpoint"}},
        ],
        requests,
    )
    client = HerdrSocketClient(str(path), timeout=1)
    assert client.pane_list() == {"panes": []}
    client.close()
    assert client.agent_acp_status("term") == {"type": "agent_acp_status"}
    client.close()
    assert client.agent_acp_endpoint("term") == {"type": "agent_acp_endpoint"}
    client.close()
    thread.join(2)
    assert [(item["method"], item["params"]) for item in requests] == [
        ("pane.list", {}),
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
