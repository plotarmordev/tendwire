from __future__ import annotations

import importlib.util
import json
import os
import socket
import stat
import sys
from pathlib import Path

import pytest


DEPLOY = Path(__file__).resolve().parents[1]
CANARY = DEPLOY / "herdr-console-canary-r8.py"


def load_canary():
    spec = importlib.util.spec_from_file_location("herdr_console_canary_r8", CANARY)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_canary_source_has_no_live_control_plane_or_known_pane_dependency() -> None:
    source = CANARY.read_text(encoding="utf-8")
    for forbidden in (
        "systemctl",
        "w53:p8",
        "term_6589e193be915a",
        "019f96b6-3f4e-74a0-9ad9-6fbf68203f74",
        "/home/smith/.config/herdr/herdr.sock",
        "/home/smith/.local/share/tendwire/tendwire.sock",
    ):
        assert forbidden not in source
    assert 'tempfile.TemporaryDirectory(prefix="herdr-console-r8-")' in source
    assert '"HERDR_SOCKET_PATH": str(socket_path)' in source
    assert '"XDG_CONFIG_HOME": str(config_home)' in source
    assert '"XDG_STATE_HOME": str(state_home)' in source


def test_cli_requires_exact_identity_evidence_and_protected_path(tmp_path: Path) -> None:
    canary = load_canary()
    arguments = canary.parse_args(
        [
            "--herdr", str(tmp_path / "herdr"),
            "--adapter-bin-dir", str(tmp_path / "adapter"),
            "--expected-herdr-sha256", "a" * 64,
            "--expected-adapter-sha256", "b" * 64,
            "--expected-herdr-commit", "c" * 40,
            "--expected-herdr-tree", "d" * 40,
            "--expected-herdr-version", "0.7.5-acp.test",
            "--expected-protocol", "19",
            "--release-id", "candidate-r8",
            "--evidence", str(tmp_path / "evidence.json"),
            "--protect-path", str(tmp_path / "protected"),
        ]
    )
    assert arguments.expected_protocol == 19
    assert arguments.protect_path == [tmp_path / "protected"]


def test_protected_fingerprint_detects_content_metadata_and_symlink_changes(tmp_path: Path) -> None:
    canary = load_canary()
    protected = tmp_path / "protected"
    protected.mkdir()
    value = protected / "value"
    value.write_text("before\n", encoding="utf-8")
    link = protected / "link"
    link.symlink_to("value")
    before = canary.protected_fingerprint([protected])
    value.write_text("after\n", encoding="utf-8")
    after = canary.protected_fingerprint([protected])
    assert before["sha256"] != after["sha256"]
    link.unlink()
    link.symlink_to("missing")
    changed_link = canary.protected_fingerprint([protected])
    assert after["sha256"] != changed_link["sha256"]


def test_atomic_evidence_is_read_only_fsynced_json_and_never_overwritten(tmp_path: Path) -> None:
    canary = load_canary()
    evidence = tmp_path / "evidence.json"
    value = {"schema_version": 1, "valid": True}
    canary.atomic_evidence(evidence, value)
    assert json.loads(evidence.read_text(encoding="utf-8")) == value
    assert stat.S_IMODE(evidence.stat().st_mode) == 0o444
    with pytest.raises(canary.CanaryFailure, match="already exists"):
        canary.atomic_evidence(evidence, value)


def test_private_exchange_server_supports_only_ping_and_console_exchange(tmp_path: Path) -> None:
    canary = load_canary()
    socket_path = tmp_path / "private.sock"
    state = canary.ExchangeState(
        version="0.7.5-acp.test",
        protocol=19,
        expected_process_group=os.getpgrp(),
    )
    server = canary.PrivateExchangeServer(socket_path, state)
    try:
        server.__enter__()
    except canary.CanaryFailure as error:
        if "Operation not permitted" in str(error):
            pytest.skip("managed test sandbox forbids AF_UNIX listeners")
        raise
    try:
        def request(value: dict[str, object]) -> dict[str, object]:
            connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                connection.connect(str(socket_path))
                connection.sendall(json.dumps(value).encode() + b"\n")
                response = bytearray()
                while not response.endswith(b"\n"):
                    response.extend(connection.recv(65_536))
                return json.loads(response)
            finally:
                connection.close()

        pong = request({"id": "ping", "method": "ping", "params": {}})
        assert pong["result"] == {
            "type": "pong",
            "version": "0.7.5-acp.test",
            "protocol": 19,
            "capabilities": None,
        }
        first = request(
            {
                "id": "exchange",
                "method": "agent.acp_console_exchange",
                "params": {
                    "target": canary.PRIVATE_TARGET,
                    "generation": 1,
                    "lease": canary.PRIVATE_LEASE,
                    "role": "console",
                    "process_id": os.getpid(),
                    "input": "R8_NORMAL_fixture",
                    "output": [],
                    "after_input_sequence": 0,
                    "after_output_sequence": 0,
                },
            }
        )
        assert first["error"]["code"] == "acp_console_input_backpressure"
        exchange = request(
            {
                "id": "exchange-retry",
                "method": "agent.acp_console_exchange",
                "params": {
                    "target": canary.PRIVATE_TARGET,
                    "generation": 1,
                    "lease": canary.PRIVATE_LEASE,
                    "role": "console",
                    "process_id": os.getpid(),
                    "input": "R8_NORMAL_fixture",
                    "output": [],
                    "after_input_sequence": 0,
                    "after_output_sequence": 0,
                },
            }
        )
        outputs = exchange["result"]["outputs"]
        assert state.snapshot_inputs() == ["R8_NORMAL_fixture"]
        assert any(item["stream"] == "thought" for item in outputs)
        assert any(item["stream"] == "future_private" for item in outputs)
        assert any(item["text"] == "R8 normal prompt accepted" for item in outputs)
    finally:
        server.__exit__(None, None, None)
    assert not socket_path.exists()


def test_private_exchange_server_rejects_wrong_console_identity() -> None:
    canary = load_canary()
    state = canary.ExchangeState(
        version="0.7.5-acp.test",
        protocol=19,
        expected_process_group=os.getpgrp(),
    )

    class FakeConnection:
        def __init__(self, request: dict[str, object]):
            self.body = bytearray(json.dumps(request).encode() + b"\n")

        def settimeout(self, _timeout: float) -> None:
            pass

        def recv(self, _limit: int) -> bytes:
            body = bytes(self.body)
            self.body.clear()
            return body

        def sendall(self, _body: bytes) -> None:
            raise AssertionError("wrong identity unexpectedly received a response")

    request = {
        "id": "wrong-identity",
        "method": "agent.acp_console_exchange",
        "params": {
            "target": "not-the-private-target",
            "generation": canary.PRIVATE_GENERATION,
            "lease": canary.PRIVATE_LEASE,
            "role": "console",
            "process_id": os.getpid(),
            "input": None,
            "output": [],
            "after_input_sequence": 0,
            "after_output_sequence": 0,
        },
    }
    with pytest.raises(canary.CanaryFailure, match="identity is not exact"):
        canary.PrivateExchangeServer(Path("unused"), state)._handle(FakeConnection(request))


def test_private_environment_pins_all_state_and_adapter_discovery(tmp_path: Path) -> None:
    canary = load_canary()
    adapter = tmp_path / "adapter-bin"
    adapter.mkdir()
    root = tmp_path / "private"
    root.mkdir()
    environment = canary.private_environment(root, root / "private.sock", adapter)
    assert environment["HERDR_SOCKET_PATH"] == str(root / "private.sock")
    assert environment["HERDR_CONFIG_PATH"].startswith(str(root))
    assert environment["XDG_CONFIG_HOME"].startswith(str(root))
    assert environment["XDG_STATE_HOME"].startswith(str(root))
    assert environment["XDG_DATA_HOME"].startswith(str(root))
    assert environment["TMPDIR"].startswith(str(root))
    assert environment["PATH"].split(":", 1)[0] == str(adapter)
    assert "HERDR_CLIENT_SOCKET_PATH" not in environment
    assert "HOME" not in environment
    assert not any(key.startswith("TELEGRAM_") for key in environment)


def test_canary_covers_retry_stream_privacy_and_oversize_drain_contract() -> None:
    source = CANARY.read_text(encoding="utf-8")
    assert 'diagnostic = b"[status] pane input was not sent; retrying safely"' in source
    assert 'bytes(captured).count(diagnostic) != 1' in source
    assert 'with PrivateExchangeServer(socket_path, state):' in source
    assert 'stdin=subprocess.PIPE' in source
    assert 'stdout=slave' in source
    assert 'stderr=slave' in source
    assert 'b"x" * 16_385 + b"\\n"' in source
    assert 'state.snapshot_inputs() != [accepted]' in source
    assert '"known_agent_streams_visible": True' in source
    assert '"private_bookkeeping_hidden": True' in source
    assert '"oversize_input_rejected": True' in source
    assert '"post_oversize_input_exactly_once": True' in source
