from __future__ import annotations

import sys
import threading
import time
from contextlib import contextmanager
from pathlib import Path

import pytest

from tendwire.backends.acp_client import (
    AcpCapabilityError,
    BoundedAcpConnection as AcpClient,
    AcpRequestTimeoutError,
    AcpTransportError,
    ClientState,
)
from tendwire.backends.acp_protocol import (
    AcpProtocolError,
    PermissionRequest,
    SessionUpdate,
    StopReason,
    SteeringOutcome,
)


FAKE_AGENT = Path(__file__).parent / "fixtures" / "acp_fake_agent.py"


def client(mode: str = "normal", **kwargs: object) -> AcpClient:
    return AcpClient([sys.executable, "-u", str(FAKE_AGENT), mode], **kwargs)


@contextmanager
def opened_client(mode: str = "normal", **kwargs: object):
    acp = client(mode, **kwargs)
    try:
        yield acp
    finally:
        acp.close()


def _typed_update(index: int) -> SessionUpdate:
    update = {
        "sessionUpdate": "agent_message_chunk",
        "messageId": f"message-{index}",
        "content": {"type": "text", "text": str(index)},
    }
    raw = {"sessionId": "s", "update": update}
    return SessionUpdate("s", raw)


def _typed_permission(index: int) -> PermissionRequest:
    raw = {
        "sessionId": "s",
        "toolCall": {"toolCallId": f"tool-{index}"},
        "options": [],
    }
    return PermissionRequest(
        index,
        "s",
        raw["toolCall"],
        (),
        None,
        raw,
    )


def _install_permission(acp: AcpClient, request: PermissionRequest) -> None:
    with acp._permission_lock:
        acp._pending_permissions[request.request_id] = request
    acp._put_session_event(request)


def _wait_for_condition_waiters(acp: AcpClient, count: int) -> None:
    deadline = time.monotonic() + 1
    while time.monotonic() < deadline:
        with acp._session_event_condition:
            if len(acp._session_event_condition._waiters) >= count:
                return
        time.sleep(0.001)
    raise AssertionError("session event consumers did not start waiting")


def _capture_failure(method, failures: list[BaseException]) -> None:
    try:
        method()
    except BaseException as exc:
        failures.append(exc)


def test_initialize_capabilities_and_session_lifecycle() -> None:
    with opened_client() as acp:
        initialized = acp.initialize()
        assert initialized["loadSession"] is True
        assert initialized["sessionCapabilities"]["resume"] == {}
        assert initialized["_meta"] == {
            "vendor.example": {"level": 2}
        }
        assert acp.state is ClientState.INITIALIZED

        created = acp.new_session(
            "/tmp/project",
            additional_directories=["/tmp/other"],
        )
        assert created == "s-new"
        streamed = acp.next_session_event(timeout=1)
        assert streamed.raw["update"]["sessionUpdate"] == "agent_message_chunk"

        loaded = acp.load_session("s1", "/tmp/project")
        resumed = acp.resume_session("s1", "/tmp/project")
        assert loaded == "s1"
        assert resumed == "s1"

    assert acp.state is ClientState.CLOSED


def test_load_session_accepts_standard_null_result(monkeypatch: pytest.MonkeyPatch) -> None:
    """ACP v1 completes load replay with a JSON-RPC null result."""

    with opened_client() as acp:
        acp.initialize()
        monkeypatch.setattr(acp, "request", lambda *_args, **_kwargs: None)
        loaded = acp.load_session("s1", "/tmp/project")

    assert loaded == "s1"


def test_prompt_stream_and_permission_response_can_run_concurrently() -> None:
    with opened_client() as acp:
        acp.initialize()
        acp.new_session("/tmp/project")
        # Drain the new-session message update.
        acp.next_session_event(timeout=1)

        outcome: list[object] = []
        failure: list[BaseException] = []

        def run_prompt() -> None:
            try:
                outcome.append(acp.prompt("s-new", "please inspect"))
            except BaseException as exc:  # pragma: no cover - diagnostic path
                failure.append(exc)

        thread = threading.Thread(target=run_prompt)
        thread.start()
        thought = acp.next_session_event(timeout=1)
        assert thought.raw["update"]["sessionUpdate"] == "agent_thought_chunk"
        permission = acp.next_session_event(timeout=1)
        assert permission.options[0].option_id == "allow"
        acp.respond_permission(permission.request_id, option_id="allow")
        plan = acp.next_session_event(timeout=1)
        assert plan.raw["update"]["sessionUpdate"] == "plan"
        thread.join(timeout=2)

        assert not thread.is_alive()
        assert not failure
        assert outcome[0] is StopReason.END_TURN


def test_advertised_steering_extension_is_capability_gated() -> None:
    with opened_client("steering") as acp:
        acp.initialize()
        acp.new_session("/tmp/project")
        acp.next_session_event(timeout=1)
        assert acp.steering_supported
        callbacks: list[str] = []
        result = acp.steer_session(
            "s-new",
            "live input",
            on_send_start=lambda: callbacks.append("started"),
            on_submitted=lambda: callbacks.append("submitted"),
        )
        assert result is SteeringOutcome.INJECTED
        assert callbacks == ["started", "submitted"]
        echoed = acp.next_session_event(timeout=1)
        assert echoed.raw["update"]["sessionUpdate"] == "user_message_chunk"

    with opened_client() as acp:
        acp.initialize()
        assert not acp.steering_supported
        with pytest.raises(AcpCapabilityError, match="steering"):
            acp.steer_session("s1", "no capability")


def test_ordered_session_event_api_preserves_cross_kind_reader_order() -> None:
    with opened_client() as acp:
        acp.initialize()
        outcome: list[object] = []
        thread = threading.Thread(
            target=lambda: outcome.append(acp.prompt("s1", "inspect"))
        )
        thread.start()
        first = acp.next_session_event(timeout=1)
        second = acp.next_session_event(timeout=1)
        assert first.raw["update"]["sessionUpdate"] == "agent_thought_chunk"
        assert second.options[0].option_id == "allow"
        acp.respond_permission(second.request_id, option_id="allow")
        third = acp.next_session_event(timeout=1)
        assert third.raw["update"]["sessionUpdate"] == "plan"
        thread.join(timeout=2)
        assert not thread.is_alive()
        assert outcome[0] is StopReason.END_TURN


def test_ordered_consumer_remains_exact_with_mixed_session_events() -> None:
    acp = client()
    expected: list[SessionUpdate | PermissionRequest] = []
    for index in range(20):
        event: SessionUpdate | PermissionRequest
        if index % 2:
            event = _typed_permission(index)
            _install_permission(acp, event)
        else:
            event = _typed_update(index)
            acp._put_session_event(event)
        expected.append(event)

    assert [acp.next_session_event() for _ in expected] == expected
    assert list(acp._session_events) == []


def test_close_and_failure_wake_all_session_event_waiters() -> None:
    acp = client()
    failures: list[BaseException] = []
    threads = [
        threading.Thread(target=lambda: _capture_failure(acp.next_session_event, failures))
        for _ in range(2)
    ]
    for thread in threads:
        thread.start()
    _wait_for_condition_waiters(acp, 2)
    acp.close()
    for thread in threads:
        thread.join(timeout=1)
        assert not thread.is_alive()
    assert len(failures) == 2

    failed = client()
    failure: list[BaseException] = []
    thread = threading.Thread(
        target=lambda: _capture_failure(failed.next_session_event, failure)
    )
    thread.start()
    _wait_for_condition_waiters(failed, 1)
    failed._set_failed(AcpTransportError("boom"))
    thread.join(timeout=1)
    assert not thread.is_alive()
    assert len(failure) == 1
    assert isinstance(failure[0], AcpTransportError)


def test_terminal_signal_and_racing_event_wake_capacity_one_waiters() -> None:
    acp = client(max_pending_events=1)
    outcomes: list[object] = []

    def consume() -> None:
        try:
            outcomes.append(acp.next_session_event())
        except BaseException as exc:
            outcomes.append(exc)

    waiters = [threading.Thread(target=consume) for _ in range(3)]
    for thread in waiters:
        thread.start()
    _wait_for_condition_waiters(acp, 3)

    with acp._session_event_condition:
        failure_thread = threading.Thread(
            target=acp._set_failed,
            args=(AcpTransportError("terminal"),),
        )
        failure_thread.start()
        deadline = time.monotonic() + 1
        while acp.state is not ClientState.FAILED and time.monotonic() < deadline:
            time.sleep(0.001)
        assert acp.state is ClientState.FAILED
        event_thread = threading.Thread(
            target=acp._put_session_event,
            args=(_typed_update(1),),
        )
        event_thread.start()

    failure_thread.join(timeout=1)
    event_thread.join(timeout=1)
    for thread in waiters:
        thread.join(timeout=1)
        assert not thread.is_alive()
    assert len(outcomes) == 3
    assert all(isinstance(item, SessionUpdate | AcpTransportError) for item in outcomes)


def test_ordered_consumer_stress_is_bounded_and_exactly_once() -> None:
    acp = client(max_pending_events=8)
    count = 200
    received: list[SessionUpdate | PermissionRequest] = []
    consumer = threading.Thread(
        target=lambda: received.extend(
            acp.next_session_event() for _ in range(count * 2)
        )
    )
    consumer.start()
    maximum_depth = 0
    for index in range(count):
        update = _typed_update(index)
        permission = _typed_permission(index)
        if index % 2:
            _install_permission(acp, permission)
            acp._put_session_event(update)
        else:
            acp._put_session_event(update)
            _install_permission(acp, permission)
        with acp._session_event_condition:
            maximum_depth = max(maximum_depth, len(acp._session_events))
    consumer.join(timeout=3)

    assert not consumer.is_alive()
    assert maximum_depth <= acp.max_pending_events
    assert sum(isinstance(item, SessionUpdate) for item in received) == count
    assert sum(isinstance(item, PermissionRequest) for item in received) == count
    assert list(acp._session_events) == []


def test_cancel_resolves_pending_permissions_as_cancelled() -> None:
    with opened_client() as acp:
        acp.initialize()
        result: list[object] = []
        thread = threading.Thread(target=lambda: result.append(acp.prompt("s1", "wait")))
        thread.start()
        acp.next_session_event(timeout=1)
        acp.next_session_event(timeout=1)
        acp.cancel("s1")
        acp.next_session_event(timeout=1)
        thread.join(timeout=2)
        assert not thread.is_alive()
        assert result[0] is StopReason.CANCELLED


def test_cancel_also_resolves_permission_that_races_after_notification() -> None:
    with opened_client("cancel_race") as acp:
        acp.initialize()
        result: list[object] = []
        thread = threading.Thread(target=lambda: result.append(acp.prompt("s1", "wait")))
        thread.start()
        acp.next_session_event(timeout=1)
        acp.next_session_event(timeout=1)
        acp.cancel("s1")
        acp.next_session_event(timeout=1)
        thread.join(timeout=2)
        assert not thread.is_alive()
        assert result[0] is StopReason.CANCELLED
        with pytest.raises(AcpRequestTimeoutError):
            acp.next_session_event(timeout=0.05)


def test_optional_methods_require_advertised_capabilities() -> None:
    with opened_client("baseline") as acp:
        acp.initialize()
        with pytest.raises(AcpCapabilityError):
            acp.load_session("s1", "/tmp")


def test_request_timeout_does_not_poison_transport() -> None:
    with opened_client("slow", request_timeout=0.05) as acp:
        acp.initialize(timeout=1)
        with pytest.raises(AcpRequestTimeoutError):
            acp.request("session/list", {})
        assert acp.state is ClientState.INITIALIZED


@pytest.mark.parametrize("mode", ["malformed", "oversize", "partial_eof"])
def test_malformed_or_oversized_stdout_fails_connection(mode: str) -> None:
    with opened_client(mode, max_frame_bytes=1024) as acp:
        with pytest.raises(AcpProtocolError):
            acp.initialize(timeout=1)
        assert acp.state is ClientState.FAILED


def test_absolute_session_paths_are_enforced_before_write() -> None:
    with opened_client() as acp:
        acp.initialize()
        with pytest.raises(ValueError, match="absolute"):
            acp.new_session("relative/path")


def test_prewrite_callback_failure_emits_no_acp_frame(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ReceiptUnavailable(RuntimeError):
        pass

    with opened_client() as acp:
        acp.initialize()
        writes: list[object] = []

        def forbidden_write(*args: object, **kwargs: object) -> int:
            writes.append((args, kwargs))
            return 0

        monkeypatch.setattr(acp, "_write_chunk", forbidden_write)
        with pytest.raises(ReceiptUnavailable):
            acp.request(
                "session/list",
                {},
                on_writing=lambda: (_ for _ in ()).throw(ReceiptUnavailable()),
            )

        assert writes == []
        assert acp._pending == {}


def test_submission_callback_timeout_keeps_its_own_taxonomy() -> None:
    with opened_client() as acp:
        acp.initialize()

        def fail_after_write() -> None:
            raise TimeoutError("receipt store timed out")

        with pytest.raises(TimeoutError, match="receipt store timed out"):
            acp.request("session/list", {}, on_written=fail_after_write)
        assert acp._pending == {}


def test_pending_failure_completion_is_atomic_with_detachment() -> None:
    acp = client()
    entered = threading.Event()
    release = threading.Event()

    class BlockingWaiter:
        def set_exception(self, _failure: BaseException) -> None:
            entered.set()
            assert release.wait(timeout=1)

    acp._pending[1] = BlockingWaiter()  # type: ignore[assignment]
    thread = threading.Thread(
        target=acp._fail_pending,
        args=(AcpTransportError("boom"),),
    )
    thread.start()
    assert entered.wait(timeout=1)
    acquired = acp._pending_lock.acquire(timeout=0.02)
    if acquired:
        acp._pending_lock.release()
    assert not acquired
    release.set()
    thread.join(timeout=1)
    assert not thread.is_alive()
    assert acp._pending == {}


def test_write_lock_timeout_does_not_cross_prewrite_boundary() -> None:
    with opened_client() as acp:
        acp.initialize()
        callbacks: list[str] = []
        assert acp._write_lock.acquire(timeout=0.1)
        try:
            with pytest.raises(AcpRequestTimeoutError, match="waiting to write"):
                acp.request(
                    "session/list",
                    {},
                    timeout=0.01,
                    on_writing=lambda: callbacks.append("started"),
                )
        finally:
            acp._write_lock.release()

        assert callbacks == []
        assert acp._pending == {}


def test_unwritable_transport_does_not_cross_prewrite_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with opened_client() as acp:
        acp.initialize()
        callbacks: list[str] = []
        monkeypatch.setattr(acp, "_wait_writable", lambda _fd, _timeout: False)
        with pytest.raises(AcpRequestTimeoutError, match="writing ACP frame"):
            acp.request(
                "session/list",
                {},
                timeout=0.01,
                on_writing=lambda: callbacks.append("started"),
            )

        assert callbacks == []
        assert acp._pending == {}


def test_prompt_content_is_validated_and_gated_by_negotiated_capabilities() -> None:
    with opened_client("baseline") as acp:
        acp.initialize()
        with pytest.raises(AcpCapabilityError, match="image"):
            acp.prompt(
                "s1",
                [{"type": "image", "data": "AA==", "mimeType": "image/png"}],
            )
        with pytest.raises(ValueError, match="text content"):
            acp.prepare_prompt([{"type": "text"}])
        assert acp.prepare_prompt(
            [{"type": "text", "text": "ok", "vendor": True}]
        ) == ({"type": "text", "text": "ok"},)


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
    with opened_client("echo_prompt") as acp:
        acp.initialize()
        result = acp.prompt("s1", blocks)
        assert result is StopReason.END_TURN


def test_mcp_servers_match_stable_v1_generated_shapes_and_capabilities() -> None:
    with opened_client() as acp:
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
        assert created == "s-new"

    with opened_client("baseline") as acp:
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
    with opened_client() as acp:
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
    with opened_client("flood", max_pending_events=1) as acp:
        acp.initialize()
        deadline = time.monotonic() + 1
        while acp.state is not ClientState.FAILED and time.monotonic() < deadline:
            time.sleep(0.01)
        assert acp.state is ClientState.FAILED
        first = acp.next_session_event(timeout=1)
        assert first.raw["update"]["sessionUpdate"] == "agent_message_chunk"
        with pytest.raises(AcpTransportError):
            acp.next_session_event(timeout=1)


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


def test_unsupported_inbound_request_gets_automatic_method_not_found() -> None:
    with opened_client("unknown_request") as acp:
        acp.initialize()
        confirmation = acp.next_session_event(timeout=1)
        assert confirmation.session_id == "s-extension"
        assert confirmation.raw["update"]["content"]["text"] == "method-not-found"


def test_uncorrelated_null_error_response_does_not_poison_transport() -> None:
    with opened_client("null_response") as acp:
        acp.initialize()
        time.sleep(0.05)
        assert acp.state is ClientState.INITIALIZED


def test_unexpected_clean_stdout_eof_is_transport_failure() -> None:
    with opened_client("exit_after_init") as acp:
        acp.initialize()
        deadline = time.monotonic() + 1
        while acp.state is not ClientState.FAILED and time.monotonic() < deadline:
            time.sleep(0.01)
        assert acp.state is ClientState.FAILED


def test_boolean_protocol_version_is_not_accepted_as_integer_one() -> None:
    with opened_client("bool_version") as acp:
        with pytest.raises(AcpProtocolError, match="protocol version"):
            acp.initialize()
        assert acp.state is ClientState.FAILED


def test_unsupported_protocol_version_is_reaped_during_initialize() -> None:
    acp = client("bool_version", close_timeout=0.1)
    with pytest.raises(AcpProtocolError, match="protocol version"):
        acp.initialize()
    assert acp.state is ClientState.FAILED
    acp.close()


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
