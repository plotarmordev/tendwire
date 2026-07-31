from __future__ import annotations

import sys
import threading
import time
from pathlib import Path

import pytest

from tendwire.backends.acp_client import (
    AcpCapabilityError,
    AcpClient,
    AcpEventQueueFullError,
    AcpRequestTimeoutError,
    AcpTransportError,
    ClientState,
)
from tendwire.backends.acp_protocol import AcpProtocolError, SessionUpdateKind, StopReason


FAKE_AGENT = Path(__file__).parent / "fixtures" / "acp_fake_agent.py"


def client(mode: str = "normal", **kwargs: object) -> AcpClient:
    return AcpClient([sys.executable, "-u", str(FAKE_AGENT), mode], **kwargs)


def test_initialize_capabilities_and_session_lifecycle() -> None:
    with client() as acp:
        initialized = acp.initialize()
        assert initialized.protocol_version == 1
        assert initialized.agent_info == {"name": "fake", "version": "1.0"}
        assert initialized.capabilities.load_session
        assert initialized.capabilities.session_list
        assert initialized.capabilities.session_resume
        assert initialized.capabilities.session_close
        assert initialized.capabilities.session_delete
        assert initialized.capabilities.raw["_meta"] == {
            "vendor.example": {"level": 2}
        }
        assert acp.state is ClientState.INITIALIZED

        created = acp.new_session(
            "/tmp/project",
            additional_directories=["/tmp/other"],
        )
        assert created.session_id == "s-new"
        assert created.modes == {"currentModeId": "default"}
        streamed = acp.next_update(timeout=1)
        assert streamed.update_kind is SessionUpdateKind.AGENT_MESSAGE_CHUNK

        loaded = acp.load_session("s1", "/tmp/project")
        resumed = acp.resume_session("s1", "/tmp/project")
        assert loaded.session_id == "s1"
        assert resumed.config_options[0]["id"] == "model"

        first = acp.list_sessions(cwd="/tmp/project")
        assert first.sessions[0].session_id == "s1"
        assert first.next_cursor == "page-2"
        second = acp.list_sessions(cursor=first.next_cursor)
        assert second.sessions[0].title == "second"
        assert second.next_cursor is None

        assert acp.close_session("s1")["_meta"]["vendor.example"]["receipt"] == "session/close"
        assert acp.delete_session("s2")["_meta"]["vendor.example"]["receipt"] == "session/delete"

    assert acp.state is ClientState.CLOSED
    assert acp.exit is not None
    assert acp.exit.returncode == 0


def test_prompt_stream_and_permission_response_can_run_concurrently() -> None:
    with client() as acp:
        acp.initialize()
        acp.new_session("/tmp/project")
        # Drain the new-session message update.
        acp.next_update(timeout=1)

        outcome: list[object] = []
        failure: list[BaseException] = []

        def run_prompt() -> None:
            try:
                outcome.append(acp.prompt("s-new", "please inspect"))
            except BaseException as exc:  # pragma: no cover - diagnostic path
                failure.append(exc)

        thread = threading.Thread(target=run_prompt)
        thread.start()
        thought = acp.next_update(timeout=1)
        assert thought.update_kind is SessionUpdateKind.AGENT_THOUGHT_CHUNK
        permission = acp.next_permission_request(timeout=1)
        assert permission.options[0].option_id == "allow"
        acp.respond_permission(permission.request_id, option_id="allow")
        plan = acp.next_update(timeout=1)
        assert plan.update_kind is SessionUpdateKind.PLAN
        thread.join(timeout=2)

        assert not thread.is_alive()
        assert not failure
        assert outcome[0].stop_reason is StopReason.END_TURN


def test_ordered_session_event_api_preserves_cross_kind_reader_order() -> None:
    with client() as acp:
        acp.initialize()
        outcome: list[object] = []
        thread = threading.Thread(
            target=lambda: outcome.append(acp.prompt("s1", "inspect"))
        )
        thread.start()
        first = acp.next_session_event(timeout=1)
        second = acp.next_session_event(timeout=1)
        assert first.update_kind is SessionUpdateKind.AGENT_THOUGHT_CHUNK
        assert second.options[0].option_id == "allow"
        acp.respond_permission(second.request_id, option_id="allow")
        third = acp.next_session_event(timeout=1)
        assert third.update_kind is SessionUpdateKind.PLAN
        thread.join(timeout=2)
        assert not thread.is_alive()
        assert outcome[0].stop_reason is StopReason.END_TURN


def test_cancel_resolves_pending_permissions_as_cancelled() -> None:
    with client() as acp:
        acp.initialize()
        result: list[object] = []
        thread = threading.Thread(target=lambda: result.append(acp.prompt("s1", "wait")))
        thread.start()
        acp.next_update(timeout=1)
        acp.next_permission_request(timeout=1)
        acp.cancel("s1")
        acp.next_update(timeout=1)
        thread.join(timeout=2)
        assert not thread.is_alive()
        assert result[0].stop_reason is StopReason.CANCELLED


def test_cancel_also_resolves_permission_that_races_after_notification() -> None:
    with client("cancel_race") as acp:
        acp.initialize()
        result: list[object] = []
        thread = threading.Thread(target=lambda: result.append(acp.prompt("s1", "wait")))
        thread.start()
        acp.next_update(timeout=1)
        acp.next_permission_request(timeout=1)
        acp.cancel("s1")
        acp.next_update(timeout=1)
        thread.join(timeout=2)
        assert not thread.is_alive()
        assert result[0].stop_reason is StopReason.CANCELLED
        with pytest.raises(AcpRequestTimeoutError):
            acp.next_permission_request(timeout=0.05)


def test_optional_methods_require_advertised_capabilities() -> None:
    with client("baseline") as acp:
        acp.initialize()
        with pytest.raises(AcpCapabilityError):
            acp.list_sessions()
        with pytest.raises(AcpCapabilityError):
            acp.load_session("s1", "/tmp")
        with pytest.raises(AcpCapabilityError):
            acp.close_session("s1")
        with pytest.raises(AcpCapabilityError):
            acp.delete_session("s1")


def test_request_timeout_does_not_poison_transport() -> None:
    with client("slow", request_timeout=0.05) as acp:
        acp.initialize(timeout=1)
        with pytest.raises(AcpRequestTimeoutError):
            acp.list_sessions()
        assert acp.state is ClientState.INITIALIZED


@pytest.mark.parametrize("mode", ["malformed", "oversize", "partial_eof"])
def test_malformed_or_oversized_stdout_fails_connection(mode: str) -> None:
    with client(mode, max_frame_bytes=1024) as acp:
        with pytest.raises(AcpProtocolError):
            acp.initialize(timeout=1)
        assert acp.state is ClientState.FAILED


def test_absolute_session_paths_are_enforced_before_write() -> None:
    with client() as acp:
        acp.initialize()
        with pytest.raises(ValueError, match="absolute"):
            acp.new_session("relative/path")


def test_prompt_content_is_validated_and_gated_by_negotiated_capabilities() -> None:
    with client("baseline") as acp:
        acp.initialize()
        with pytest.raises(AcpCapabilityError, match="image"):
            acp.prompt(
                "s1",
                [{"type": "image", "data": "AA==", "mimeType": "image/png"}],
            )
        with pytest.raises(ValueError, match="text"):
            acp.prompt("s1", [{"type": "text"}])
        with pytest.raises(ValueError, match="outside"):
            acp.prompt("s1", [{"type": "text", "text": "ok", "vendor": True}])


def test_prompt_content_matches_stable_v1_generated_shapes() -> None:
    blocks = [
        {"type": "text", "text": "hello"},
        {"type": "image", "data": "AA==", "mimeType": "image/png"},
        {"type": "audio", "data": "AA==", "mimeType": "audio/wav"},
        {"type": "resource_link", "name": "main.py", "uri": "file:///tmp/main.py"},
        {
            "type": "resource",
            "resource": {
                "uri": "file:///tmp/main.py",
                "mimeType": "text/x-python",
                "text": "print('ok')",
            },
        },
    ]
    with client("echo_prompt") as acp:
        acp.initialize()
        result = acp.prompt("s1", blocks)
        assert result.stop_reason is StopReason.END_TURN


def test_mcp_servers_match_stable_v1_generated_shapes_and_capabilities() -> None:
    with client() as acp:
        acp.initialize()
        created = acp.new_session(
            "/tmp/project",
            mcp_servers=[
                {
                    "name": "stdio-tools",
                    "command": "/usr/bin/tools",
                    "args": ["--stdio"],
                    "env": [{"name": "MODE", "value": "test"}],
                },
                {
                    "type": "http",
                    "name": "http-tools",
                    "url": "https://example.invalid/mcp",
                    "headers": [{"name": "Authorization", "value": "opaque"}],
                },
                {
                    "type": "sse",
                    "name": "legacy-tools",
                    "url": "https://example.invalid/sse",
                    "headers": [],
                },
            ],
        )
        assert created.session_id == "s-new"

    with client("baseline") as acp:
        acp.initialize()
        with pytest.raises(AcpCapabilityError, match="HTTP"):
            acp.new_session(
                "/tmp/project",
                mcp_servers=[
                    {
                        "type": "http",
                        "name": "http-tools",
                        "url": "https://example.invalid/mcp",
                        "headers": [],
                    }
                ],
            )
        with pytest.raises(ValueError, match="absolute"):
            acp.new_session(
                "/tmp/project",
                mcp_servers=[
                    {"name": "stdio-tools", "command": "tools", "args": [], "env": []}
                ],
            )
        with pytest.raises(ValueError, match="stable ACP v1"):
            acp.new_session(
                "/tmp/project",
                mcp_servers=[
                    {
                        "type": "stdio",
                        "name": "stdio-tools",
                        "command": "/usr/bin/tools",
                        "args": [],
                        "env": [],
                    }
                ],
            )


def test_concurrent_initialize_is_exactly_once_and_returns_same_result() -> None:
    with client() as acp:
        results: list[object] = []
        failures: list[BaseException] = []

        def initialize() -> None:
            try:
                results.append(acp.initialize())
            except BaseException as exc:  # pragma: no cover - diagnostic path
                failures.append(exc)

        threads = [threading.Thread(target=initialize) for _ in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=2)
        assert not failures
        assert len(results) == 6
        assert all(result is results[0] for result in results)


def test_backpressure_failure_remains_visible_after_full_queue_drains() -> None:
    with client("flood", max_pending_events=1) as acp:
        acp.initialize()
        deadline = time.monotonic() + 1
        while acp.state is not ClientState.FAILED and time.monotonic() < deadline:
            time.sleep(0.01)
        assert acp.state is ClientState.FAILED
        assert isinstance(acp.failure, AcpEventQueueFullError)
        first = acp.next_update(timeout=1)
        assert first.update_kind is SessionUpdateKind.AGENT_MESSAGE_CHUNK
        with pytest.raises(AcpTransportError):
            acp.next_update(timeout=1)


def test_blocked_or_partial_stdin_write_is_bounded_and_terminal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_write = AcpClient._write_chunk
    select_calls = 0

    def partial_write(fd: int, data: memoryview) -> int:
        return real_write(fd, data[:8])

    def writable_once(fd: int, timeout: float) -> bool:
        nonlocal select_calls
        select_calls += 1
        return select_calls == 1

    monkeypatch.setattr(AcpClient, "_write_chunk", staticmethod(partial_write))
    monkeypatch.setattr(AcpClient, "_wait_writable", staticmethod(writable_once))
    acp = client("no_read", request_timeout=0.2, close_timeout=0.05)
    try:
        started = time.monotonic()
        with pytest.raises(AcpTransportError, match="partial write"):
            acp.initialize(
                client_capabilities={"vendor/padding": "small"},
                timeout=0.2,
            )
        assert time.monotonic() - started < 1
        assert acp.state is ClientState.FAILED
    finally:
        acp.close()


def test_close_escalates_to_kill_for_stubborn_adapter() -> None:
    acp = client("stubborn", close_timeout=0.05)
    acp.initialize()
    acp.close()
    assert acp.state is ClientState.CLOSED
    assert acp.exit is not None
    assert acp.exit.returncode != 0


def test_stderr_tail_is_bounded_and_keeps_suffix() -> None:
    acp = client("stderr_tail", stderr_limit_bytes=64)
    acp.initialize()
    acp.close()
    tail = acp.stderr_tail()
    assert len(tail.encode()) <= 64
    assert tail.endswith("-TAIL")


def test_unknown_adapter_extensions_remain_observable() -> None:
    method = "_vendor.example/future_notification"
    with client("extensions", supported_extension_notifications=(method,)) as acp:
        initialized = acp.initialize()
        assert initialized.capabilities.raw["_meta"] == {
            "vendor.example": {"level": 2}
        }
        notification = acp.next_notification(timeout=1)
        assert notification.method == method
        assert notification.params["opaque"] == {"revision": 9}


def test_unrecognized_extension_notifications_are_ignored_without_backpressure() -> None:
    with client("extension_flood", max_pending_events=1) as acp:
        acp.initialize()
        time.sleep(0.1)
        assert acp.state is ClientState.INITIALIZED
        with pytest.raises(AcpRequestTimeoutError):
            acp.next_notification(timeout=0.05)


def test_unsupported_inbound_request_gets_automatic_method_not_found() -> None:
    with client("unknown_request") as acp:
        acp.initialize()
        confirmation = acp.next_update(timeout=1)
        assert confirmation.session_id == "s-extension"
        assert confirmation.update["content"]["text"] == "method-not-found"


def test_explicitly_supported_extension_request_remains_observable() -> None:
    method = "_vendor.example/request"
    with client("supported_request", supported_extension_requests=(method,)) as acp:
        acp.initialize()
        request = acp.next_inbound_request(timeout=1)
        assert request.method == method
        acp.reject_inbound_request(request.request_id)
        confirmation = acp.next_update(timeout=1)
        assert confirmation.update["content"]["text"] == "method-not-found"


def test_uncorrelated_null_error_response_does_not_poison_transport() -> None:
    with client("null_response") as acp:
        acp.initialize()
        time.sleep(0.05)
        assert acp.state is ClientState.INITIALIZED


def test_unexpected_clean_stdout_eof_is_transport_failure() -> None:
    with client("exit_after_init") as acp:
        acp.initialize()
        deadline = time.monotonic() + 1
        while acp.state is not ClientState.FAILED and time.monotonic() < deadline:
            time.sleep(0.01)
        assert acp.state is ClientState.FAILED
        assert isinstance(acp.failure, AcpTransportError)


def test_boolean_protocol_version_is_not_accepted_as_integer_one() -> None:
    with client("bool_version") as acp:
        with pytest.raises(AcpProtocolError, match="protocol version"):
            acp.initialize()
        assert acp.state is ClientState.FAILED


def test_unsupported_protocol_version_is_reaped_during_initialize() -> None:
    acp = client("bool_version", close_timeout=0.1)
    with pytest.raises(AcpProtocolError, match="protocol version"):
        acp.initialize()
    assert acp.process is not None
    assert acp.process.poll() is not None


@pytest.mark.parametrize(
    "kwargs",
    [
        {"cwd": "relative"},
        {"cwd": "/tmp/bad\x00path"},
        {"env": {"BAD=NAME": "value"}},
        {"env": {"NAME": "bad\x00value"}},
    ],
)
def test_process_paths_and_environment_are_validated(
    kwargs: dict[str, object],
) -> None:
    with pytest.raises(ValueError):
        client(**kwargs)
