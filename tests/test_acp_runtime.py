from __future__ import annotations

import queue
import sqlite3
import threading
import time
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
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
    AcpRuntimeBindingError,
    AcpRuntimeProtocolError,
    AcpRuntimeStopTimeout,
    RuntimeState,
    SessionOpenMode,
)
from tendwire.config import Config
from tendwire.core.models import WorkerBinding
from tendwire.store.sqlite import list_agent_events, upsert_worker_bindings


_END = object()


class FakeClient:
    def __init__(self) -> None:
        self.updates: queue.Queue[SessionUpdate | object] = queue.Queue()
        self.permissions: queue.Queue[PermissionRequest | object] = queue.Queue()
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []
        self.permission_responses: list[tuple[object, str | None, bool]] = []
        self.prompt_result: object = PromptResult(StopReason.END_TURN, {})
        self.prompt_failure: BaseException | None = None
        self.initialize_failure: BaseException | None = None
        self.new_session_result: SessionResult | None = None
        self.closed = False
        self.close_calls = 0

    def initialize(self, **kwargs: Any) -> object:
        self.calls.append(("initialize", (), kwargs))
        if self.initialize_failure is not None:
            raise self.initialize_failure
        return object()

    def new_session(self, cwd: Path, **kwargs: Any) -> SessionResult:
        self.calls.append(("new", (cwd,), kwargs))
        return self.new_session_result or SessionResult(
            "session-private", None, (), {}
        )

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
        self.close_calls += 1
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
        self.permission_failure: BaseException | None = None
        self.update_result: object = None
        self.permission_result: object = None
        self.completion_result: object = None

    def start_turn(self, *, producer_turn_id: str | None = None) -> str:
        self.started.append(producer_turn_id)
        return "opaque-turn"

    def ingest_update(self, raw: object) -> object:
        if self.update_failure is not None:
            raise self.update_failure
        self.updates.append(raw)
        return self.update_result

    def ingest_permission_request(
        self, raw: object, *, source_event_id: str | None = None
    ) -> object:
        if self.permission_failure is not None:
            raise self.permission_failure
        self.permissions.append((raw, source_event_id))
        return self.permission_result

    def mark_prompt_complete(self) -> object:
        self.completions += 1
        return self.completion_result


def binding(session_id: str = "session-private") -> WorkerBinding:
    return WorkerBinding(
        host_id="host-a",
        worker_id="worker-public",
        worker_fingerprint="worker-fingerprint",
        backend="herdr",
        target_kind="pane_id",
        target_value="pane-private-secret",
        turn_target_kind="acp_session_id",
        turn_target_value=session_id,
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


def bound_runtime(
    tmp_path: Path,
    client: FakeClient,
    current_binding: WorkerBinding,
    **kwargs: Any,
) -> AcpRuntime:
    db_path = tmp_path / "bound-events.db"
    upsert_worker_bindings(db_path, [current_binding])
    return AcpRuntime(
        client,  # type: ignore[arg-type]
        config=Config(
            host_id="host-a",
            db_path=db_path,
            agent_event_source="acp_required",
        ),
        binding=current_binding,
        cwd=tmp_path,
        stream_generation="generation-private-secret",
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
        assert client.calls[0][2]["client_capabilities"] == {}
        assert captured["session_id"] == "session-private"
        assert captured["binding"] is not None
        assert captured["stream_generation"] == "generation-private-secret"
        assert service.status().healthy
    finally:
        service.stop()


@pytest.mark.parametrize(
    "claims",
    [
        {"fs": {"readTextFile": True, "writeTextFile": True}},
        {"terminal": True},
        {"elicitation": {"form": {}, "url": {}}},
        {"session": {"configOptions": {"boolean": {}}}},
        {"_meta": {"example.test/capability": True}},
    ],
)
def test_runtime_strips_client_capabilities_it_cannot_serve(
    tmp_path: Path,
    claims: dict[str, object],
) -> None:
    client = FakeClient()
    service = runtime(tmp_path, client, client_capabilities=claims).start()
    try:
        assert client.calls[0][2]["client_capabilities"] == {}
    finally:
        service.stop()


def test_runtime_rejects_unknown_extension_capability_claims(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="unsupported client capabilities"):
        runtime(
            tmp_path,
            FakeClient(),
            client_capabilities={"example.test/custom": {}},
        )


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
        binding=binding("existing-private"),
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


def test_start_rejects_unbound_session_and_closes_adapter(tmp_path: Path) -> None:
    client = FakeClient()
    client.new_session_result = SessionResult("other-private", None, (), {})
    service = runtime(tmp_path, client)

    with pytest.raises(AcpRuntimeProtocolError, match="bound session"):
        service.start()

    assert client.closed
    assert client.close_calls == 1
    assert service.join(timeout=0.1)
    assert service.status().state is RuntimeState.FAILED


def test_initialize_failure_is_cleaned_up_without_caller_stop(tmp_path: Path) -> None:
    client = FakeClient()
    failure = OSError("initialize failed")
    client.initialize_failure = failure
    service = runtime(tmp_path, client)

    with pytest.raises(OSError) as raised:
        service.start()

    assert raised.value is failure
    assert client.closed
    assert client.close_calls == 1
    assert service.status().failure_type == "OSError"


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
        permission_event_id = ingestor.permissions[0][1]
        assert permission_event_id is not None
        assert permission_event_id.startswith("permission:")
        assert permission_event_id != "permission:7"
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


def test_permission_ingestion_failure_cancels_before_runtime_fails(
    tmp_path: Path,
) -> None:
    client = FakeClient()
    ingestor = FakeIngestor()
    failure = OSError("journal unavailable")
    ingestor.permission_failure = failure
    service = runtime(tmp_path, client, ingestor).start()
    client.permissions.put(permission())
    wait_until(lambda: service.status().state is RuntimeState.FAILED)

    assert client.permission_responses == [(7, None, True)]
    assert service.status().permissions_cancelled == 1
    assert service.status().permissions_ingested == 0
    with pytest.raises(OSError) as raised:
        service.stop()
    assert raised.value is failure


def test_replaced_binding_cancels_permission_before_callback_and_is_terminal(
    tmp_path: Path,
) -> None:
    client = FakeClient()
    current = binding()
    callback_called = threading.Event()

    def unsafe_allow(_request: PermissionRequest) -> str:
        callback_called.set()
        return "allow-once"

    service = bound_runtime(
        tmp_path,
        client,
        current,
        permission_callback=unsafe_allow,
    ).start()
    replacement = replace(
        current,
        worker_id="replacement-worker-private",
        worker_fingerprint="replacement-fingerprint-private",
        observed_at="2026-08-01T00:00:00+00:00",
    )
    upsert_worker_bindings(tmp_path / "bound-events.db", [replacement])
    client.permissions.put(permission())
    wait_until(lambda: service.status().state is RuntimeState.FAILED)

    status = service.status()
    assert callback_called.is_set() is False
    assert client.permission_responses == [(7, None, True)]
    assert status.permissions_ingested == 0
    assert status.permissions_selected == 0
    assert status.permissions_cancelled == 1
    assert status.failure_type == "AcpRuntimeBindingError"
    assert list_agent_events(tmp_path / "bound-events.db", "host-a") == ()
    rendered = repr(status)
    assert "replacement-worker-private" not in rendered
    assert "binding-private-secret" not in rendered
    with pytest.raises(AcpRuntimeBindingError):
        service.stop()


def test_binding_expiry_after_start_rejects_update_and_is_terminal(
    tmp_path: Path,
) -> None:
    client = FakeClient()
    current = binding()
    service = bound_runtime(tmp_path, client, current).start()
    with sqlite3.connect(tmp_path / "bound-events.db") as conn:
        conn.execute(
            "UPDATE worker_bindings SET expires_at = ? "
            "WHERE host_id = ? AND private_fingerprint = ?",
            (
                "2000-01-01T00:00:00+00:00",
                current.host_id,
                current.private_fingerprint,
            ),
        )
    client.updates.put(update())
    wait_until(lambda: service.status().state is RuntimeState.FAILED)

    status = service.status()
    assert status.updates_ingested == 0
    assert status.failure_type == "AcpRuntimeBindingError"
    assert list_agent_events(tmp_path / "bound-events.db", "host-a") == ()
    rendered = repr(status)
    assert "session-private" not in rendered
    assert "binding-private-secret" not in rendered
    with pytest.raises(AcpRuntimeBindingError):
        service.stop()


def test_permission_source_identity_distinguishes_jsonrpc_id_types(
    tmp_path: Path,
) -> None:
    client = FakeClient()
    ingestor = FakeIngestor()
    service = runtime(tmp_path, client, ingestor).start()
    try:
        client.permissions.put(permission(1))
        client.permissions.put(permission("1"))
        wait_until(lambda: service.status().permissions_ingested == 2)

        source_ids = [source_id for _raw, source_id in ingestor.permissions]
        assert len(set(source_ids)) == 2
        assert all(
            source_id is not None and source_id.startswith("permission:")
            for source_id in source_ids
        )
    finally:
        service.stop()


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


def test_stale_binding_completion_is_terminal_and_not_counted_complete(
    tmp_path: Path,
) -> None:
    client = FakeClient()
    ingestor = FakeIngestor()
    ingestor.completion_result = SimpleNamespace(
        ignored_reason="stale_binding",
        event=None,
        turn=None,
    )
    service = runtime(tmp_path, client, ingestor).start()

    with pytest.raises(AcpRuntimeBindingError):
        service.prompt("question")

    status = service.status()
    assert status.state is RuntimeState.FAILED
    assert status.failure_type == "AcpRuntimeBindingError"
    assert status.prompts_started == 1
    assert status.prompts_completed == 0
    assert status.prompts_failed == 1
    with pytest.raises(AcpRuntimeBindingError):
        service.stop()


def test_prompt_finality_waits_for_permission_resolution(tmp_path: Path) -> None:
    client = FakeClient()
    ingestor = FakeIngestor()
    callback_entered = threading.Event()
    release_callback = threading.Event()

    def decide(_request: PermissionRequest) -> str:
        callback_entered.set()
        assert release_callback.wait(timeout=1)
        return "allow-once"

    service = runtime(
        tmp_path,
        client,
        ingestor,
        permission_callback=decide,
    ).start()
    result: list[PromptResult] = []
    failure: list[BaseException] = []

    def run_prompt() -> None:
        try:
            result.append(service.prompt("question", drain_timeout=0.5))
        except BaseException as exc:
            failure.append(exc)

    try:
        client.permissions.put(permission())
        assert callback_entered.wait(timeout=1)
        prompt_thread = threading.Thread(target=run_prompt)
        prompt_thread.start()
        time.sleep(0.04)
        assert prompt_thread.is_alive()
        assert ingestor.completions == 0

        release_callback.set()
        prompt_thread.join(timeout=1)
        assert not prompt_thread.is_alive()
        assert failure == []
        assert len(result) == 1
        assert ingestor.completions == 1
        assert client.permission_responses == [(7, "allow-once", False)]
    finally:
        release_callback.set()
        service.stop()


def test_cross_kind_ingestion_cannot_overtake_an_active_update(
    tmp_path: Path,
) -> None:
    client = FakeClient()
    update_entered = threading.Event()
    release_update = threading.Event()
    order: list[str] = []

    class OrderedIngestor(FakeIngestor):
        def ingest_update(self, raw: object) -> None:
            order.append("update-start")
            update_entered.set()
            assert release_update.wait(timeout=1)
            super().ingest_update(raw)
            order.append("update-end")

        def ingest_permission_request(
            self, raw: object, *, source_event_id: str | None = None
        ) -> None:
            order.append("permission")
            super().ingest_permission_request(
                raw,
                source_event_id=source_event_id,
            )

    service = runtime(tmp_path, client, OrderedIngestor()).start()
    try:
        client.updates.put(update())
        assert update_entered.wait(timeout=1)
        client.permissions.put(permission())
        time.sleep(0.04)
        assert order == ["update-start"]

        release_update.set()
        wait_until(lambda: service.status().permissions_ingested == 1)
        assert order == ["update-start", "update-end", "permission"]
    finally:
        release_update.set()
        service.stop()


def test_prompt_transport_failure_cancels_and_makes_runtime_terminal(
    tmp_path: Path,
) -> None:
    client = FakeClient()
    failure = AcpRequestTimeoutError("prompt timed out")
    client.prompt_failure = failure
    ingestor = FakeIngestor()
    service = runtime(tmp_path, client, ingestor).start()

    with pytest.raises(AcpRequestTimeoutError) as raised:
        service.prompt("question")

    assert raised.value is failure
    assert ingestor.started == [None]
    assert ingestor.completions == 0
    assert ("cancel", ("session-private",), {}) in client.calls
    assert service.status().cancellation_requests == 1
    assert service.status().state is RuntimeState.FAILED
    with pytest.raises(AcpRequestTimeoutError):
        service.prompt("unsafe retry")
    with pytest.raises(AcpRequestTimeoutError):
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


def test_concurrent_stop_closes_adapter_exactly_once(tmp_path: Path) -> None:
    client = FakeClient()
    service = runtime(tmp_path, client).start()
    failures: list[BaseException] = []

    def stop() -> None:
        try:
            service.stop()
        except BaseException as exc:
            failures.append(exc)

    callers = [threading.Thread(target=stop) for _ in range(2)]
    for caller in callers:
        caller.start()
    for caller in callers:
        caller.join(timeout=1)

    assert failures == []
    assert all(not caller.is_alive() for caller in callers)
    assert client.close_calls == 1
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


def test_concurrent_stop_wait_for_lifecycle_is_also_deadline_bounded(
    tmp_path: Path,
) -> None:
    client = FakeClient()
    close_entered = threading.Event()
    release_close = threading.Event()
    original_close = client.close

    def hanging_close() -> None:
        close_entered.set()
        release_close.wait(timeout=1)
        original_close()

    client.close = hanging_close  # type: ignore[method-assign]
    service = runtime(tmp_path, client).start()
    first_failures: list[BaseException] = []

    def first_stop() -> None:
        try:
            service.stop(timeout=0.2)
        except BaseException as exc:
            first_failures.append(exc)

    caller = threading.Thread(target=first_stop)
    caller.start()
    assert close_entered.wait(timeout=1)
    started = time.monotonic()
    try:
        with pytest.raises(AcpRuntimeStopTimeout, match="lifecycle"):
            service.stop(timeout=0.03)
        assert time.monotonic() - started < 0.15
    finally:
        release_close.set()
        caller.join(timeout=1)

    assert len(first_failures) == 1
    assert isinstance(first_failures[0], AcpRuntimeStopTimeout)
    assert client.close_calls == 1
