from __future__ import annotations

import queue
import threading
import time
from pathlib import Path
from typing import Any

import pytest

from tendwire.backends.acp_client import AcpRequestTimeoutError
from tendwire.backends.acp_protocol import (
    PermissionOption,
    PermissionOptionKind,
    PermissionRequest,
    PromptResult,
    SessionResult,
    SessionUpdate,
    SessionUpdateKind,
    StopReason,
)
from tendwire.backends.acp_runtime import (
    AcpRuntime,
    AcpRuntimeProtocolError,
    AcpRuntimeStopTimeout,
    RuntimeState,
    SessionOpenMode,
)
from tendwire.config import Config
from tendwire.core.models import WorkerBinding


_END = object()


class FakeClient:
    def __init__(self) -> None:
        self.updates: queue.Queue[SessionUpdate | object] = queue.Queue()
        self.permissions: queue.Queue[PermissionRequest | object] = queue.Queue()
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []
        self.permission_responses: list[tuple[object, str | None, bool]] = []
        self.prompt_result: object = PromptResult(StopReason.END_TURN, {})
        self.prompt_failure: BaseException | None = None
        self.closed = False

    def initialize(self, **kwargs: Any) -> object:
        self.calls.append(("initialize", (), kwargs))
        return object()

    def new_session(self, cwd: Path, **kwargs: Any) -> SessionResult:
        self.calls.append(("new", (cwd,), kwargs))
        return SessionResult("session-private", None, (), {})

    def load_session(
        self, session_id: str, cwd: Path, **kwargs: Any
    ) -> SessionResult:
        self.calls.append(("load", (session_id, cwd), kwargs))
        return SessionResult(session_id, None, (), {})

    def resume_session(
        self, session_id: str, cwd: Path, **kwargs: Any
    ) -> SessionResult:
        self.calls.append(("resume", (session_id, cwd), kwargs))
        return SessionResult(session_id, None, (), {})

    def prompt(self, session_id: str, prompt: object, **kwargs: Any) -> object:
        self.calls.append(("prompt", (session_id, prompt), kwargs))
        if self.prompt_failure is not None:
            raise self.prompt_failure
        return self.prompt_result

    def cancel(self, session_id: str) -> None:
        self.calls.append(("cancel", (session_id,), {}))

    def next_update(self, *, timeout: float) -> SessionUpdate:
        try:
            value = self.updates.get(timeout=timeout)
        except queue.Empty as exc:
            raise AcpRequestTimeoutError("idle") from exc
        if value is _END:
            raise EOFError("closed")
        assert isinstance(value, SessionUpdate)
        return value

    def next_permission_request(self, *, timeout: float) -> PermissionRequest:
        try:
            value = self.permissions.get(timeout=timeout)
        except queue.Empty as exc:
            raise AcpRequestTimeoutError("idle") from exc
        if value is _END:
            raise EOFError("closed")
        assert isinstance(value, PermissionRequest)
        return value

    def respond_permission(
        self,
        request_id: object,
        *,
        option_id: str | None = None,
        cancelled: bool = False,
    ) -> None:
        self.permission_responses.append((request_id, option_id, cancelled))

    def close(self) -> None:
        self.closed = True
        self.updates.put(_END)
        self.permissions.put(_END)


class FakeIngestor:
    def __init__(self, session_id: str = "session-private") -> None:
        self.session_id = session_id
        self.started: list[str | None] = []
        self.updates: list[object] = []
        self.permissions: list[tuple[object, str | None]] = []
        self.completions = 0
        self.update_failure: BaseException | None = None

    def start_turn(self, *, producer_turn_id: str | None = None) -> str:
        self.started.append(producer_turn_id)
        return "opaque-turn"

    def ingest_update(self, raw: object) -> None:
        if self.update_failure is not None:
            raise self.update_failure
        self.updates.append(raw)

    def ingest_permission_request(
        self, raw: object, *, source_event_id: str | None = None
    ) -> None:
        self.permissions.append((raw, source_event_id))

    def mark_prompt_complete(self) -> None:
        self.completions += 1


def binding() -> WorkerBinding:
    return WorkerBinding(
        host_id="host-a",
        worker_id="worker-public",
        worker_fingerprint="worker-fingerprint",
        backend="herdr",
        target_kind="pane_id",
        target_value="pane-private-secret",
        turn_target_kind="acp_session_id",
        turn_target_value="session-private",
        private_fingerprint="binding-private-secret",
    )


def runtime(
    tmp_path: Path,
    client: FakeClient,
    ingestor: FakeIngestor | None = None,
    **kwargs: Any,
) -> AcpRuntime:
    return AcpRuntime(
        client,  # type: ignore[arg-type]
        config=Config(host_id="host-a", db_path=tmp_path / "events.db"),
        binding=binding(),
        cwd=tmp_path,
        stream_generation="generation-private-secret",
        ingestor=ingestor or FakeIngestor(),  # type: ignore[arg-type]
        poll_timeout=0.01,
        stop_timeout=0.5,
        **kwargs,
    )


def update(session_id: str = "session-private") -> SessionUpdate:
    raw = {
        "sessionId": session_id,
        "update": {
            "sessionUpdate": "agent_message_chunk",
            "content": {"type": "text", "text": "answer"},
        },
    }
    return SessionUpdate(
        session_id,
        SessionUpdateKind.AGENT_MESSAGE_CHUNK,
        raw["update"],
        None,
        raw,
    )


def permission(
    request_id: object = 7, session_id: str = "session-private"
) -> PermissionRequest:
    options = (
        PermissionOption(
            "allow-once",
            "Allow once",
            PermissionOptionKind.ALLOW_ONCE,
            {"optionId": "allow-once", "name": "Allow once", "kind": "allow_once"},
        ),
        PermissionOption(
            "reject-once",
            "Reject once",
            PermissionOptionKind.REJECT_ONCE,
            {"optionId": "reject-once", "name": "Reject once", "kind": "reject_once"},
        ),
    )
    raw = {
        "sessionId": session_id,
        "toolCall": {"toolCallId": "tool-private"},
        "options": [dict(option.raw) for option in options],
    }
    return PermissionRequest(
        request_id,
        session_id,
        raw["toolCall"],
        options,
        None,
        raw,
    )


def wait_until(predicate, timeout: float = 1.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.005)
    raise AssertionError("condition did not become true")


def test_start_negotiates_opens_one_session_and_binds_factory(tmp_path: Path) -> None:
    client = FakeClient()
    captured: dict[str, object] = {}
    ingestor = FakeIngestor()

    def factory(config: Config, **kwargs: object) -> FakeIngestor:
        captured.update(kwargs)
        captured["config"] = config
        return ingestor

    service = AcpRuntime(
        client,  # type: ignore[arg-type]
        config=Config(host_id="host-a", db_path=tmp_path / "events.db"),
        binding=binding(),
        cwd=tmp_path,
        stream_generation="generation-private-secret",
        client_capabilities={"fs": {"readTextFile": True}},
        ingestor_factory=factory,  # type: ignore[arg-type]
        poll_timeout=0.01,
        stop_timeout=0.5,
    ).start()
    try:
        assert [call[0] for call in client.calls[:2]] == ["initialize", "new"]
        assert client.calls[0][2]["client_capabilities"] == {
            "fs": {"readTextFile": True}
        }
        assert captured["session_id"] == "session-private"
        assert captured["binding"] is not None
        assert captured["stream_generation"] == "generation-private-secret"
        assert service.status().healthy
    finally:
        service.stop()


@pytest.mark.parametrize(
    ("mode", "method"),
    [(SessionOpenMode.LOAD, "load"), (SessionOpenMode.RESUME, "resume")],
)
def test_load_and_resume_use_requested_session(
    tmp_path: Path, mode: SessionOpenMode, method: str
) -> None:
    client = FakeClient()
    service = AcpRuntime(
        client,  # type: ignore[arg-type]
        config=Config(host_id="host-a", db_path=tmp_path / "events.db"),
        binding=binding(),
        cwd=tmp_path,
        session_mode=mode,
        session_id="existing-private",
        ingestor=FakeIngestor("existing-private"),  # type: ignore[arg-type]
        poll_timeout=0.01,
        stop_timeout=0.5,
    ).start()
    try:
        assert client.calls[1][0] == method
        assert client.calls[1][1][0] == "existing-private"
    finally:
        service.stop()


def test_background_consumers_ingest_losslessly_and_permissions_fail_closed(
    tmp_path: Path,
) -> None:
    client = FakeClient()
    ingestor = FakeIngestor()
    service = runtime(tmp_path, client, ingestor).start()
    try:
        client.updates.put(update())
        client.permissions.put(permission())
        wait_until(lambda: service.status().permissions_ingested == 1)
        wait_until(lambda: service.status().updates_ingested == 1)

        assert len(ingestor.updates) == 1
        assert ingestor.permissions[0][1] == "permission:7"
        assert client.permission_responses == [(7, None, True)]
        assert service.status().permissions_cancelled == 1
    finally:
        service.stop()


def test_callback_can_select_only_an_offered_permission(tmp_path: Path) -> None:
    client = FakeClient()
    decisions = iter(["not-offered", "allow-once"])
    service = runtime(
        tmp_path,
        client,
        permission_callback=lambda _request: next(decisions),
    ).start()
    try:
        client.permissions.put(permission(1))
        client.permissions.put(permission(2))
        wait_until(lambda: service.status().permissions_ingested == 2)
        wait_until(lambda: len(client.permission_responses) == 2)

        assert client.permission_responses == [
            (1, None, True),
            (2, "allow-once", False),
        ]
        status = service.status()
        assert status.invalid_permission_selections == 1
        assert status.permissions_cancelled == 1
        assert status.permissions_selected == 1
    finally:
        service.stop()


def test_callback_failure_cancels_permission_before_propagating(tmp_path: Path) -> None:
    client = FakeClient()
    callback_failure = LookupError("decision failed")

    def fail(_request: PermissionRequest) -> str:
        raise callback_failure

    service = runtime(tmp_path, client, permission_callback=fail).start()
    client.permissions.put(permission())
    wait_until(lambda: service.status().state is RuntimeState.FAILED)

    assert client.permission_responses == [(7, None, True)]
    assert service.status().permissions_cancelled == 1
    with pytest.raises(LookupError) as raised:
        service.stop()
    assert raised.value is callback_failure


def test_prompt_finalizes_only_after_valid_response_and_update_drain(
    tmp_path: Path,
) -> None:
    client = FakeClient()
    ingestor = FakeIngestor()
    service = runtime(tmp_path, client, ingestor).start()
    try:
        client.updates.put(update())
        result = service.prompt("question", producer_turn_id="producer-private")

        assert result.stop_reason is StopReason.END_TURN
        assert ingestor.started == ["producer-private"]
        assert len(ingestor.updates) == 1
        assert ingestor.completions == 1
        status = service.status()
        assert status.prompts_started == 1
        assert status.prompts_completed == 1
        assert status.prompts_failed == 0
    finally:
        service.stop()


def test_invalid_prompt_response_never_marks_complete_and_propagates(
    tmp_path: Path,
) -> None:
    client = FakeClient()
    client.prompt_result = {"stopReason": "end_turn"}
    ingestor = FakeIngestor()
    service = runtime(tmp_path, client, ingestor).start()

    with pytest.raises(AcpRuntimeProtocolError, match="invalid response"):
        service.prompt("question")
    assert ingestor.completions == 0
    assert service.status().state is RuntimeState.FAILED
    with pytest.raises(AcpRuntimeProtocolError):
        service.stop()


def test_background_ingestion_failure_is_propagated_and_status_is_redacted(
    tmp_path: Path,
) -> None:
    client = FakeClient()
    ingestor = FakeIngestor()
    failure = OSError("session-private pane-private-secret")
    ingestor.update_failure = failure
    service = runtime(tmp_path, client, ingestor).start()
    client.updates.put(update())
    wait_until(lambda: service.status().state is RuntimeState.FAILED)

    status = service.status()
    assert not status.healthy
    assert status.failure_type == "OSError"
    rendered = repr(status)
    assert "session-private" not in rendered
    assert "pane-private-secret" not in rendered
    assert "generation-private-secret" not in rendered
    with pytest.raises(OSError) as raised:
        service.raise_if_failed()
    assert raised.value is failure
    with pytest.raises(OSError):
        service.stop()


def test_cancel_targets_bound_session_and_stop_joins_consumers(tmp_path: Path) -> None:
    client = FakeClient()
    service = runtime(tmp_path, client).start()
    service.cancel()
    assert ("cancel", ("session-private",), {}) in client.calls
    assert service.status().cancellation_requests == 1

    service.stop()
    assert client.closed
    assert service.join(timeout=0.1)
    assert service.status().state is RuntimeState.STOPPED


def test_stop_deadline_is_bounded_even_when_client_close_hangs(tmp_path: Path) -> None:
    client = FakeClient()
    release_close = threading.Event()

    def hanging_close() -> None:
        release_close.wait(timeout=1)

    client.close = hanging_close  # type: ignore[method-assign]
    service = runtime(tmp_path, client).start()
    started = time.monotonic()
    try:
        with pytest.raises(AcpRuntimeStopTimeout):
            service.stop(timeout=0.05)
        assert time.monotonic() - started < 0.25
    finally:
        release_close.set()
