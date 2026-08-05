from __future__ import annotations

import queue
import sqlite3
import sys
import threading
import time
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from tendwire.backends.acp_client import (
    AcpRequestTimeoutError,
    BoundedAcpConnection as AcpClient,
)
from tendwire.backends.acp_protocol import (
    PermissionOption,
    PermissionRequest,
    SessionUpdate,
    StopReason,
    SteeringOutcome,
)
from tendwire.backends.acp_runtime import (
    AcpWorkerSession as AcpRuntime,
    AcpRuntimeBindingError,
    AcpRuntimeProtocolError,
    AcpRuntimeStateError,
    AcpRuntimeStopTimeout,
    RuntimeState,
    SessionOpenMode,
)
from tendwire.config import Config
from tendwire.core.models import Snapshot, Worker, WorkerBinding, utc_timestamp
from tendwire.store.events import list_agent_events
from tendwire.store.projection import (
    expire_worker_bindings,
    list_worker_bindings,
    save_snapshot,
)
from .store_helpers import upsert_test_worker_bindings as upsert_worker_bindings


_END = object()
FAKE_AGENT = Path(__file__).parent / "fixtures" / "acp_fake_agent.py"


class FakeClient:
    def __init__(self) -> None:
        self.events: queue.Queue[SessionUpdate | PermissionRequest | object] = queue.Queue()
        # Existing tests enqueue through the typed names; both feed the one
        # reader-ordered stream used by the runtime.
        self.updates = self.events
        self.permissions = self.events
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []
        self.permission_responses: list[tuple[object, str | None, bool]] = []
        self.prompt_result: object = StopReason.END_TURN
        self.prompt_failure: BaseException | None = None
        self.initialize_failure: BaseException | None = None
        self.new_session_result: str | None = None
        self.restored_session_result: str | None = None
        self.closed = False
        self.close_calls = 0
        self.steering_supported = False
        self.steering_result = SteeringOutcome.INJECTED

    def initialize(self, **kwargs: Any) -> object:
        self.calls.append(("initialize", (), kwargs))
        if self.initialize_failure is not None:
            raise self.initialize_failure
        return object()

    def new_session(self, cwd: Path, **kwargs: Any) -> str:
        self.calls.append(("new", (cwd,), kwargs))
        return self.new_session_result or "session-private"

    def load_session(
        self, session_id: str, cwd: Path, **kwargs: Any
    ) -> str:
        self.calls.append(("load", (session_id, cwd), kwargs))
        return self.restored_session_result or session_id

    def resume_session(
        self, session_id: str, cwd: Path, **kwargs: Any
    ) -> str:
        self.calls.append(("resume", (session_id, cwd), kwargs))
        return self.restored_session_result or session_id

    def prompt(self, session_id: str, prompt: object, **kwargs: Any) -> object:
        self.calls.append(("prompt", (session_id, prompt), kwargs))
        if self.prompt_failure is not None:
            raise self.prompt_failure
        on_submitted = kwargs.get("on_submitted")
        if callable(on_submitted):
            on_submitted()
        return self.prompt_result

    def prepare_prompt(self, prompt: object) -> tuple[dict[str, Any], ...]:
        if isinstance(prompt, str):
            return ({"type": "text", "text": prompt},)
        return tuple(dict(block) for block in prompt)  # type: ignore[arg-type]

    def steer_session(self, session_id: str, prompt: object, **kwargs: Any) -> object:
        self.calls.append(("steer", (session_id, prompt), kwargs))
        on_send_start = kwargs.get("on_send_start")
        if callable(on_send_start):
            on_send_start()
        on_submitted = kwargs.get("on_submitted")
        if callable(on_submitted):
            on_submitted()
        return self.steering_result

    def cancel(self, session_id: str) -> None:
        self.calls.append(("cancel", (session_id,), {}))

    def next_session_event(
        self,
        *,
        timeout: float,
    ) -> SessionUpdate | PermissionRequest:
        try:
            value = self.events.get(timeout=timeout)
        except queue.Empty as exc:
            raise AcpRequestTimeoutError("idle") from exc
        if value is _END:
            raise EOFError("closed")
        assert isinstance(value, SessionUpdate | PermissionRequest)
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
        self.events.put(_END)


class FakeIngestor:
    def __init__(self, session_id: str = "session-private") -> None:
        self.session_id = session_id
        self.started: list[str | None] = []
        self.updates: list[object] = []
        self.permissions: list[tuple[object, str | None]] = []
        self.completions = 0
        self.completion_reasons: list[StopReason] = []
        self.update_failure: BaseException | None = None
        self.permission_failure: BaseException | None = None
        self.completion_failure: BaseException | None = None
        self.appended: list[tuple[object, str]] = []
        self.appendable = True
        persisted = SimpleNamespace(status="inserted")
        self.update_result: object = SimpleNamespace(
            event=persisted,
            turn=None,
            ignored_reason=None,
        )
        self.permission_result: object = SimpleNamespace(
            event=persisted,
            turn=None,
            ignored_reason=None,
        )
        self.completion_result: object = None

    def start_turn(self, *, producer_turn_id: str | None = None) -> str:
        self.started.append(producer_turn_id)
        return "opaque-turn"

    def ingest_update(self, raw: object, **_kwargs: Any) -> object:
        if self.update_failure is not None:
            raise self.update_failure
        self.updates.append(raw)
        return self.update_result

    def ingest_permission_request(
        self,
        raw: object,
        *,
        source_event_id: str | None = None,
        **_kwargs: Any,
    ) -> object:
        if self.permission_failure is not None:
            raise self.permission_failure
        self.permissions.append((raw, source_event_id))
        return self.permission_result

    def begin_prompt(
        self,
        prompt: object,
        *,
        producer_turn_id: str | None = None,
    ) -> object:
        return self.start_turn(producer_turn_id=producer_turn_id)

    def can_append_prompt(self) -> bool:
        return self.appendable

    def append_prompt(self, prompt: object, *, producer_turn_id: str) -> object:
        self.appended.append((prompt, producer_turn_id))
        return self.update_result

    def mark_prompt_complete(
        self,
        stop_reason: StopReason = StopReason.END_TURN,
    ) -> object:
        self.completions += 1
        self.completion_reasons.append(stop_reason)
        if self.completion_failure is not None:
            raise self.completion_failure
        return self.completion_result


def binding(session_id: str = "session-private") -> WorkerBinding:
    return WorkerBinding(
        host_id="host-a",
        worker_id="worker-public",
        worker_fingerprint="worker-fingerprint",
        backend="acp",
        target_kind="pane_id",
        target_value="pane-private-secret",
        turn_target_kind="acp_session_id",
        turn_target_value=session_id,
        private_fingerprint="binding-private-secret",
    )


def continuity_binding() -> WorkerBinding:
    return WorkerBinding(
        host_id="host-a",
        worker_id="worker-public",
        worker_fingerprint="worker-fingerprint",
        backend="herdr",
        target_kind="pane_id",
        target_value="pane-private-secret",
        turn_target_kind="pane_id",
        turn_target_value="pane-private-secret",
        private_fingerprint="continuity-binding-private-secret",
    )


def binding_callback(db_path: Path):
    def establish(
        session_id: str,
        continuity: WorkerBinding,
    ) -> WorkerBinding:
        bound = replace(
            continuity,
            backend="acp",
            turn_target_kind="acp_session_id",
            turn_target_value=session_id,
            private_fingerprint="",
        )
        upsert_worker_bindings(db_path, [bound])
        return bound

    return establish


def runtime(
    tmp_path: Path,
    client: FakeClient,
    ingestor: FakeIngestor | None = None,
    **kwargs: Any,
) -> AcpRuntime:
    db_path = tmp_path / "events.db"
    continuity = continuity_binding()
    upsert_worker_bindings(db_path, [continuity])
    return AcpRuntime(
        client,  # type: ignore[arg-type]
        config=Config(
            host_id="host-a",
            db_path=db_path,
        ),
        binding=continuity,
        cwd=tmp_path,
        stream_generation="generation-private-secret",
        session_binding_callback=binding_callback(db_path),
        ingestor_factory=lambda *_args, **_kwargs: ingestor or FakeIngestor(),
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
        ),
        binding=current_binding,
        cwd=tmp_path,
        session_mode=SessionOpenMode.LOAD,
        session_id=current_binding.turn_target_value,
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
    return SessionUpdate(session_id, raw)


def permission(
    request_id: object = 7, session_id: str = "session-private"
) -> PermissionRequest:
    options = (
        PermissionOption(
            "allow-once",
            "Allow once",
            "allow_once",
        ),
        PermissionOption(
            "reject-once",
            "Reject once",
            "reject_once",
        ),
    )
    raw = {
        "sessionId": session_id,
        "toolCall": {"toolCallId": "tool-private"},
        "options": [
            {"optionId": option.option_id, "name": option.name, "kind": option.kind}
            for option in options
        ],
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
    db_path = tmp_path / "events.db"
    continuity = continuity_binding()
    upsert_worker_bindings(db_path, [continuity])

    def factory(config: Config, **kwargs: object) -> FakeIngestor:
        captured.update(kwargs)
        captured["config"] = config
        return ingestor

    service = AcpRuntime(
        client,  # type: ignore[arg-type]
        config=Config(host_id="host-a", db_path=db_path),
        binding=continuity,
        cwd=tmp_path,
        stream_generation="generation-private-secret",
        session_binding_callback=binding_callback(db_path),
        ingestor_factory=factory,  # type: ignore[arg-type]
        poll_timeout=0.01,
        stop_timeout=0.5,
    ).start()
    try:
        assert [call[0] for call in client.calls[:2]] == ["initialize", "new"]
        assert captured["session_id"] == "session-private"
        assert captured["binding"] is not None
        assert captured["stream_generation"] == "generation-private-secret"
        assert service.status().healthy
    finally:
        service.stop()
    assert len(list_worker_bindings(db_path, "host-a", backend="acp")) == 1

@pytest.mark.parametrize(
    ("mode", "method"),
    [(SessionOpenMode.LOAD, "load"), (SessionOpenMode.RESUME, "resume")],
)
def test_load_and_resume_use_requested_session(
    tmp_path: Path, mode: SessionOpenMode, method: str
) -> None:
    client = FakeClient()
    db_path = tmp_path / "events.db"
    existing = binding("existing-private")
    ingestor = FakeIngestor("existing-private")
    upsert_worker_bindings(db_path, [existing])
    service = AcpRuntime(
        client,  # type: ignore[arg-type]
        config=Config(
            host_id="host-a",
            db_path=db_path,
        ),
        binding=existing,
        cwd=tmp_path,
        session_mode=mode,
        session_id="existing-private",
        ingestor_factory=lambda *_args, **_kwargs: ingestor,
        poll_timeout=0.01,
        stop_timeout=0.5,
    ).start()
    try:
        assert client.calls[1][0] == method
        assert client.calls[1][1][0] == "existing-private"
    finally:
        service.stop()
    assert list_worker_bindings(db_path, "host-a", backend="acp") == [existing]


@pytest.mark.parametrize("mode", [SessionOpenMode.LOAD, SessionOpenMode.RESUME])
def test_load_and_resume_reject_agent_session_mismatch_and_close(
    tmp_path: Path,
    mode: SessionOpenMode,
) -> None:
    client = FakeClient()
    client.restored_session_result = "attacker-session-private"
    db_path = tmp_path / "events.db"
    existing = binding("existing-private")
    upsert_worker_bindings(db_path, [existing])
    service = AcpRuntime(
        client,  # type: ignore[arg-type]
        config=Config(
            host_id="host-a",
            db_path=db_path,
        ),
        binding=existing,
        cwd=tmp_path,
        session_mode=mode,
        session_id="existing-private",
        poll_timeout=0.01,
        stop_timeout=0.5,
    )

    with pytest.raises(AcpRuntimeProtocolError, match="bound session"):
        service.start()

    assert client.closed
    assert client.close_calls == 1
    assert service.status().state is RuntimeState.FAILED
    assert list_worker_bindings(db_path, "host-a", backend="acp") == [existing]


@pytest.mark.parametrize("mode", [SessionOpenMode.LOAD, SessionOpenMode.RESUME])
def test_load_and_resume_reject_non_acp_binding_before_transport(
    tmp_path: Path,
    mode: SessionOpenMode,
) -> None:
    client = FakeClient()
    legacy = replace(binding("existing-private"), backend="herdr")

    with pytest.raises(ValueError, match="ACP backend binding"):
        AcpRuntime(
            client,  # type: ignore[arg-type]
            config=Config(
                host_id="host-a",
                db_path=tmp_path / "events.db",
            ),
            binding=legacy,
            cwd=tmp_path,
            session_mode=mode,
            session_id="existing-private",
        )

    assert client.calls == []


def test_new_accepts_unpredictable_agent_generated_session_id(tmp_path: Path) -> None:
    client = FakeClient()
    generated = "agent-generated-unpredictable-7f94"
    client.new_session_result = generated
    db_path = tmp_path / "events.db"
    continuity = continuity_binding()
    upsert_worker_bindings(db_path, [continuity])
    seen: list[tuple[str, WorkerBinding]] = []

    def establish(session_id: str, anchor: WorkerBinding) -> WorkerBinding:
        seen.append((session_id, anchor))
        return binding_callback(db_path)(session_id, anchor)

    service = AcpRuntime(
        client,  # type: ignore[arg-type]
        config=Config(host_id="host-a", db_path=db_path),
        binding=continuity,
        cwd=tmp_path,
        session_binding_callback=establish,
        ingestor_factory=lambda *_args, **_kwargs: FakeIngestor(generated),
        poll_timeout=0.01,
        stop_timeout=0.5,
    ).start()
    try:
        assert seen == [(generated, continuity)]
        assert service.status().healthy
    finally:
        service.stop()


def test_new_acp_binding_survives_herdr_refresh_and_normal_stop(
    tmp_path: Path,
) -> None:
    client = FakeClient()
    db_path = tmp_path / "events.db"
    continuity = continuity_binding()
    upsert_worker_bindings(db_path, [continuity])
    service = AcpRuntime(
        client,  # type: ignore[arg-type]
        config=Config(host_id="host-a", db_path=db_path),
        binding=continuity,
        cwd=tmp_path,
        session_binding_callback=binding_callback(db_path),
        ingestor_factory=lambda *_args, **_kwargs: FakeIngestor(),
        poll_timeout=0.01,
        stop_timeout=0.5,
    ).start()

    derived = list_worker_bindings(db_path, "host-a", backend="acp")
    assert len(derived) == 1
    assert derived[0].turn_target_value == "session-private"
    expire_worker_bindings(
        db_path,
        "host-a",
        backend="herdr",
        private_fingerprints=["unrelated-private-binding"],
        reason="stale_observation",
    )
    assert list_worker_bindings(db_path, "host-a", backend="acp") == derived
    service.stop()
    assert list_worker_bindings(db_path, "host-a", backend="acp") == derived


def test_new_session_binding_accepts_concurrent_herdr_lease_refresh(
    tmp_path: Path,
) -> None:
    client = FakeClient()
    db_path = tmp_path / "events.db"
    continuity = continuity_binding()
    upsert_worker_bindings(db_path, [continuity])

    def bind_after_refresh(session_id: str, anchor: WorkerBinding) -> WorkerBinding:
        refreshed = replace(
            anchor,
            observed_at="2098-01-01T00:00:00+00:00",
            expires_at="9999-12-31T23:59:59+00:00",
        )
        upsert_worker_bindings(db_path, [refreshed])
        return binding_callback(db_path)(session_id, anchor)

    service = AcpRuntime(
        client,  # type: ignore[arg-type]
        config=Config(host_id="host-a", db_path=db_path),
        binding=continuity,
        cwd=tmp_path,
        session_binding_callback=bind_after_refresh,
        ingestor_factory=lambda *_args, **_kwargs: FakeIngestor(),
        poll_timeout=0.01,
        stop_timeout=0.5,
    ).start()
    try:
        assert service.status().healthy
        current = list_worker_bindings(db_path, "host-a", backend="herdr")
        assert current == [
            replace(
                continuity,
                observed_at="2098-01-01T00:00:00+00:00",
                expires_at="9999-12-31T23:59:59+00:00",
            )
        ]
    finally:
        service.stop()


def test_new_leaves_persisted_binding_for_coordinator_after_startup_failure(
    tmp_path: Path,
) -> None:
    client = FakeClient()
    db_path = tmp_path / "events.db"
    continuity = continuity_binding()
    upsert_worker_bindings(db_path, [continuity])

    def fail_factory(*_args: object, **_kwargs: object) -> FakeIngestor:
        raise RuntimeError("factory failed")

    service = AcpRuntime(
        client,  # type: ignore[arg-type]
        config=Config(host_id="host-a", db_path=db_path),
        binding=continuity,
        cwd=tmp_path,
        session_binding_callback=binding_callback(db_path),
        ingestor_factory=fail_factory,  # type: ignore[arg-type]
        poll_timeout=0.01,
        stop_timeout=0.5,
    )
    with pytest.raises(RuntimeError, match="factory failed"):
        service.start()
    assert len(list_worker_bindings(db_path, "host-a", backend="acp")) == 1


def test_new_callback_can_stop_runtime_without_lifecycle_deadlock(
    tmp_path: Path,
) -> None:
    client = FakeClient()
    db_path = tmp_path / "events.db"
    continuity = continuity_binding()
    upsert_worker_bindings(db_path, [continuity])
    service: AcpRuntime

    def stop_during_bind(session_id: str, anchor: WorkerBinding) -> WorkerBinding:
        bound = binding_callback(db_path)(session_id, anchor)
        service.stop(timeout=0.2)
        return bound

    service = AcpRuntime(
        client,  # type: ignore[arg-type]
        config=Config(host_id="host-a", db_path=db_path),
        binding=continuity,
        cwd=tmp_path,
        session_binding_callback=stop_during_bind,
        poll_timeout=0.01,
        stop_timeout=0.5,
    )
    started = time.monotonic()
    with pytest.raises(AcpRuntimeStateError, match="stopped during"):
        service.start()
    assert time.monotonic() - started < 0.5
    assert len(list_worker_bindings(db_path, "host-a", backend="acp")) == 1


def test_prompt_rechecks_binding_before_remote_send(tmp_path: Path) -> None:
    client = FakeClient()
    ingestor = FakeIngestor()
    db_path = tmp_path / "events.db"
    continuity = continuity_binding()
    upsert_worker_bindings(db_path, [continuity])
    service = AcpRuntime(
        client,  # type: ignore[arg-type]
        config=Config(host_id="host-a", db_path=db_path),
        binding=continuity,
        cwd=tmp_path,
        session_binding_callback=binding_callback(db_path),
        ingestor_factory=lambda *_args, **_kwargs: ingestor,
        poll_timeout=0.01,
        stop_timeout=0.5,
    ).start()
    derived = list_worker_bindings(db_path, "host-a", backend="acp")[0]
    expire_worker_bindings(
        db_path,
        derived.host_id,
        backend="acp",
        private_fingerprints=[derived.private_fingerprint],
        reason="test_expiry",
    )

    with pytest.raises(AcpRuntimeBindingError):
        service.prompt("must not send", producer_turn_id="producer-private")
    assert [call[0] for call in client.calls].count("prompt") == 0
    assert ingestor.started == []
    assert ingestor.completions == 0


def test_new_requires_explicit_session_binder_before_launch(tmp_path: Path) -> None:
    client = FakeClient()

    with pytest.raises(ValueError, match="session_binding_callback is required"):
        AcpRuntime(
            client,  # type: ignore[arg-type]
            config=Config(host_id="host-a", db_path=tmp_path / "events.db"),
            binding=continuity_binding(),
            cwd=tmp_path,
        )

    assert client.calls == []
    assert client.close_calls == 0


@pytest.mark.parametrize("mismatch", ["session", "worker"])
def test_new_rejects_malicious_binder_return_and_closes_adapter(
    tmp_path: Path,
    mismatch: str,
) -> None:
    client = FakeClient()
    db_path = tmp_path / "events.db"
    continuity = continuity_binding()
    upsert_worker_bindings(db_path, [continuity])

    def malicious(session_id: str, anchor: WorkerBinding) -> WorkerBinding:
        return replace(
            anchor,
            worker_id=(
                "attacker-worker-private"
                if mismatch == "worker"
                else anchor.worker_id
            ),
            turn_target_kind="acp_session_id",
            turn_target_value=(
                "attacker-session-private"
                if mismatch == "session"
                else session_id
            ),
            private_fingerprint="",
        )

    service = AcpRuntime(
        client,  # type: ignore[arg-type]
        config=Config(host_id="host-a", db_path=db_path),
        binding=continuity,
        cwd=tmp_path,
        session_binding_callback=malicious,
        poll_timeout=0.01,
        stop_timeout=0.5,
    )

    with pytest.raises(AcpRuntimeBindingError):
        service.start()

    assert client.closed
    assert client.close_calls == 1
    assert service.status().state is RuntimeState.FAILED
    assert service.status().failure_type == "AcpRuntimeBindingError"
    assert list_worker_bindings(db_path, "host-a", backend="herdr") == [
        continuity
    ]


def test_new_binder_exception_closes_adapter_and_fails_terminally(
    tmp_path: Path,
) -> None:
    client = FakeClient()
    db_path = tmp_path / "events.db"
    continuity = continuity_binding()
    upsert_worker_bindings(db_path, [continuity])
    failure = RuntimeError("binder-private-failure")

    def fail(_session_id: str, _anchor: WorkerBinding) -> WorkerBinding:
        raise failure

    service = AcpRuntime(
        client,  # type: ignore[arg-type]
        config=Config(host_id="host-a", db_path=db_path),
        binding=continuity,
        cwd=tmp_path,
        session_binding_callback=fail,
        poll_timeout=0.01,
        stop_timeout=0.5,
    )

    with pytest.raises(RuntimeError) as raised:
        service.start()

    assert raised.value is failure
    assert client.closed
    assert client.close_calls == 1
    status = service.status()
    assert status.state is RuntimeState.FAILED
    assert status.failure_type == "RuntimeError"
    assert "binder-private-failure" not in repr(status)


def test_new_rejects_valid_shaped_binding_that_was_not_persisted(
    tmp_path: Path,
) -> None:
    client = FakeClient()
    db_path = tmp_path / "events.db"
    continuity = continuity_binding()
    upsert_worker_bindings(db_path, [continuity])

    def dishonest(session_id: str, anchor: WorkerBinding) -> WorkerBinding:
        return replace(
            anchor,
            backend="acp",
            turn_target_kind="acp_session_id",
            turn_target_value=session_id,
            private_fingerprint="",
        )

    service = AcpRuntime(
        client,  # type: ignore[arg-type]
        config=Config(host_id="host-a", db_path=db_path),
        binding=continuity,
        cwd=tmp_path,
        session_binding_callback=dishonest,
        poll_timeout=0.01,
        stop_timeout=0.5,
    )

    with pytest.raises(AcpRuntimeBindingError, match="not current"):
        service.start()

    assert client.closed
    assert client.close_calls == 1
    assert service.status().state is RuntimeState.FAILED


def test_new_rejects_binder_that_overwrites_herdr_continuity(
    tmp_path: Path,
) -> None:
    client = FakeClient()
    db_path = tmp_path / "events.db"
    continuity = continuity_binding()
    upsert_worker_bindings(db_path, [continuity])

    def destructive(session_id: str, anchor: WorkerBinding) -> WorkerBinding:
        upsert_worker_bindings(
            db_path,
            [
                replace(
                    anchor,
                    worker_id="replacement-worker-private",
                    worker_fingerprint="replacement-fingerprint-private",
                    observed_at=utc_timestamp(),
                )
            ],
        )
        bound = replace(
            anchor,
            backend="acp",
            turn_target_kind="acp_session_id",
            turn_target_value=session_id,
            private_fingerprint="",
        )
        upsert_worker_bindings(db_path, [bound])
        return bound

    service = AcpRuntime(
        client,  # type: ignore[arg-type]
        config=Config(host_id="host-a", db_path=db_path),
        binding=continuity,
        cwd=tmp_path,
        session_binding_callback=destructive,
        poll_timeout=0.01,
        stop_timeout=0.5,
    )

    with pytest.raises(AcpRuntimeBindingError, match="not current"):
        service.start()

    assert client.closed
    assert client.close_calls == 1
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
        observed_at=utc_timestamp(),
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

        assert result is StopReason.END_TURN
        assert ingestor.started == ["producer-private"]
        assert len(ingestor.updates) == 1
        assert ingestor.completions == 1
        status = service.status()
        assert status.prompts_started == 1
        assert status.prompts_completed == 1
        assert status.prompts_failed == 0
    finally:
        service.stop()


def test_prompt_requires_crash_stable_producer_identity_before_remote_send(
    tmp_path: Path,
) -> None:
    client = FakeClient()
    ingestor = FakeIngestor()
    service = runtime(tmp_path, client, ingestor).start()
    try:
        with pytest.raises(ValueError, match="producer_turn_id"):
            service.prompt("question")
        with pytest.raises(ValueError, match="producer_turn_id"):
            service.prompt("question", producer_turn_id="  ")

        assert [call[0] for call in client.calls].count("prompt") == 0
        assert ingestor.started == []
        assert service.status().prompts_started == 0
        assert service.status().state is RuntimeState.RUNNING
    finally:
        service.stop()


def test_submit_prompt_acknowledges_frame_before_end_of_turn(tmp_path: Path) -> None:
    class BlockingPromptClient(FakeClient):
        def __init__(self) -> None:
            super().__init__()
            self.release = threading.Event()

        def prompt(self, session_id: str, prompt: object, **kwargs: Any) -> object:
            self.calls.append(("prompt", (session_id, prompt), kwargs))
            on_submitted = kwargs.get("on_submitted")
            assert callable(on_submitted)
            on_submitted()
            assert self.release.wait(1.0)
            return self.prompt_result

    client = BlockingPromptClient()
    service = runtime(tmp_path, client).start()
    started = time.monotonic()
    service.submit_prompt(
        "long turn",
        producer_turn_id="producer-turn-ack",
        acknowledgement_timeout=0.25,
    )
    assert time.monotonic() - started < 0.2
    assert service.status().prompts_completed == 0
    client.release.set()
    wait_until(lambda: service.status().prompts_completed == 1)
    service.stop()


def test_submit_steering_records_input_at_transport_boundary(tmp_path: Path) -> None:
    client = FakeClient()
    client.steering_supported = True
    ingestor = FakeIngestor()
    service = runtime(tmp_path, client, ingestor).start()
    try:
        callbacks: list[str] = []
        result = service.submit_steering(
            "live follow-up",
            producer_turn_id="producer-steer",
            acknowledgement_timeout=0.25,
            on_send_start=lambda: callbacks.append("started"),
        )
        assert result is SteeringOutcome.INJECTED
        assert callbacks == ["started"]
        assert len(ingestor.appended) == 1
        assert ingestor.appended[0][1] == "producer-steer"
        assert [call[0] for call in client.calls].count("prompt") == 0
        assert [call[0] for call in client.calls].count("steer") == 1
    finally:
        service.stop()


def test_submit_steering_retries_definite_non_application_once(tmp_path: Path) -> None:
    class FailOnceSteeringClient(FakeClient):
        def __init__(self) -> None:
            super().__init__()
            self.steering_supported = True
            self.steering_attempts = 0

        def steer_session(
            self, session_id: str, prompt: object, **kwargs: Any
        ) -> SteeringOutcome:
            self.calls.append(("steer", (session_id, prompt), kwargs))
            self.steering_attempts += 1
            on_send_start = kwargs.get("on_send_start")
            if callable(on_send_start):
                on_send_start()
            outcome = (
                SteeringOutcome.FAILED
                if self.steering_attempts == 1
                else SteeringOutcome.INJECTED
            )
            return outcome

    client = FailOnceSteeringClient()
    ingestor = FakeIngestor()
    service = runtime(tmp_path, client, ingestor).start()
    try:
        callbacks: list[str] = []
        result = service.submit_steering(
            "live retry follow-up",
            producer_turn_id="producer-steer-retry",
            acknowledgement_timeout=0.25,
            on_send_start=lambda: callbacks.append("started"),
        )
        assert result is SteeringOutcome.INJECTED
        assert callbacks == ["started"]
        assert len(ingestor.appended) == 1
        assert ingestor.appended[0][1] == "producer-steer-retry"
        assert [call[0] for call in client.calls].count("steer") == 2
        assert client.calls[-1][2].get("on_send_start") is None
        assert 0 < client.calls[-1][2]["timeout"] <= 0.25
    finally:
        service.stop()


def test_submit_steering_returns_second_definite_failure_without_third_attempt(
    tmp_path: Path,
) -> None:
    client = FakeClient()
    client.steering_supported = True
    client.steering_result = SteeringOutcome.FAILED
    ingestor = FakeIngestor()
    service = runtime(tmp_path, client, ingestor).start()
    try:
        callbacks: list[str] = []
        result = service.submit_steering(
            "live failed follow-up",
            producer_turn_id="producer-steer-failed",
            acknowledgement_timeout=0.25,
            on_send_start=lambda: callbacks.append("started"),
        )
        assert result is SteeringOutcome.FAILED
        assert callbacks == ["started"]
        assert len(ingestor.appended) == 1
        assert [call[0] for call in client.calls].count("steer") == 2
    finally:
        service.stop()


def test_ignored_update_does_not_increment_persisted_counter(tmp_path: Path) -> None:
    client = FakeClient()
    ingestor = FakeIngestor()
    ingestor.update_result = SimpleNamespace(
        event=None,
        turn=None,
        ignored_reason="prompt_echo",
    )
    service = runtime(tmp_path, client, ingestor).start()
    try:
        client.updates.put(update())
        wait_until(lambda: len(ingestor.updates) == 1)
        assert service.status().updates_ingested == 0
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
        service.prompt("question", producer_turn_id="producer-private")

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
    result: list[StopReason] = []
    failure: list[BaseException] = []

    def run_prompt() -> None:
        try:
            result.append(
                service.prompt(
                    "question",
                    producer_turn_id="producer-private",
                    drain_timeout=0.5,
                )
            )
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
        def ingest_update(self, raw: object, **kwargs: Any) -> object:
            order.append("update-start")
            update_entered.set()
            assert release_update.wait(timeout=1)
            result = super().ingest_update(raw, **kwargs)
            order.append("update-end")
            return result

        def ingest_permission_request(
            self,
            raw: object,
            *,
            source_event_id: str | None = None,
            **kwargs: Any,
        ) -> object:
            order.append("permission")
            return super().ingest_permission_request(
                raw,
                source_event_id=source_event_id,
                **kwargs,
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


def test_load_drains_and_discards_setup_replay_before_live_ingestion(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "load.db"
    current = binding("s-load")
    upsert_worker_bindings(db_path, [current])
    client = AcpClient(
        [sys.executable, "-u", str(FAKE_AGENT), "load_replay"],
        max_pending_events=4,
    )
    service = AcpRuntime(
        client,
        config=Config(
            host_id="host-a",
            db_path=db_path,
        ),
        binding=current,
        cwd=tmp_path,
        session_mode=SessionOpenMode.LOAD,
        session_id="s-load",
        poll_timeout=0.005,
        stop_timeout=2,
    ).start()
    try:
        assert service.status().updates_ingested == 0
        assert list_agent_events(db_path, "host-a") == ()
        with sqlite3.connect(db_path) as conn:
            assert conn.execute("SELECT COUNT(*) FROM connector_outbox").fetchone()[0] == 0
    finally:
        service.stop(timeout=2)


@pytest.mark.parametrize("stop_reason", tuple(StopReason))
def test_runtime_carries_every_prompt_stop_reason_to_completion(
    tmp_path: Path,
    stop_reason: StopReason,
) -> None:
    client = FakeClient()
    client.prompt_result = stop_reason
    ingestor = FakeIngestor()
    service = runtime(tmp_path, client, ingestor).start()
    try:
        result = service.prompt("no echo", producer_turn_id="turn-a")
        assert result is stop_reason
        assert ingestor.started == ["turn-a"]
        assert ingestor.completion_reasons == [stop_reason]
    finally:
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
        service.prompt("question", producer_turn_id="producer-private")

    assert raised.value is failure
    assert ingestor.started == ["producer-private"]
    assert ingestor.completions == 1
    assert ingestor.completion_reasons == [StopReason.CANCELLED]
    assert ("cancel", ("session-private",), {}) in client.calls
    assert service.status().cancellation_requests == 1
    assert service.status().state is RuntimeState.FAILED
    with pytest.raises(AcpRequestTimeoutError):
        service.prompt("unsafe retry", producer_turn_id="producer-private-2")
    with pytest.raises(AcpRequestTimeoutError):
        service.stop()


def test_prompt_failure_preserves_original_when_cancel_finalization_fails(
    tmp_path: Path,
) -> None:
    client = FakeClient()
    original = AcpRequestTimeoutError("prompt timed out")
    client.prompt_failure = original
    ingestor = FakeIngestor()
    ingestor.completion_failure = OSError("completion unavailable")
    service = runtime(tmp_path, client, ingestor).start()

    with pytest.raises(AcpRequestTimeoutError) as raised:
        service.prompt("question", producer_turn_id="producer-private")

    assert raised.value is original
    assert ingestor.completions == 1
    assert ingestor.completion_reasons == [StopReason.CANCELLED]
    assert service.status().failure_type == "AcpRequestTimeoutError"


def test_prompt_failure_drains_queued_updates_before_cancelled_completion(
    tmp_path: Path,
) -> None:
    actions: list[str] = []

    class UpdatingFailureClient(FakeClient):
        def prompt(self, session_id: str, prompt: object, **kwargs: Any) -> object:
            self.calls.append(("prompt", (session_id, prompt), kwargs))
            self.events.put(update())
            raise AcpRequestTimeoutError("prompt timed out")

    class OrderedIngestor(FakeIngestor):
        def ingest_update(self, raw: object, **kwargs: Any) -> object:
            outcome = super().ingest_update(raw, **kwargs)
            actions.append("update")
            return outcome

        def mark_prompt_complete(
            self,
            stop_reason: StopReason = StopReason.END_TURN,
        ) -> object:
            actions.append("complete")
            return super().mark_prompt_complete(stop_reason)

    client = UpdatingFailureClient()
    ingestor = OrderedIngestor()
    service = runtime(tmp_path, client, ingestor).start()

    with pytest.raises(AcpRequestTimeoutError):
        service.prompt("question", producer_turn_id="producer-private")

    assert actions == ["update", "complete"]
    assert ingestor.completion_reasons == [StopReason.CANCELLED]


def test_prompt_transport_failure_materializes_one_cancelled_final_projection(
    tmp_path: Path,
) -> None:
    client = FakeClient()
    failure = AcpRequestTimeoutError("prompt timed out")
    client.prompt_failure = failure
    current = binding()
    service = bound_runtime(tmp_path, client, current)
    save_snapshot(
        tmp_path / "bound-events.db",
        Snapshot(
            host_id="host-a",
            updated_at=utc_timestamp(),
            workers=[
                Worker(
                    id=current.worker_id,
                    name="worker-public",
                    status="active",
                    fingerprint=current.worker_fingerprint,
                )
            ],
        ),
    )
    service.start()

    with pytest.raises(AcpRequestTimeoutError) as raised:
        service.prompt("question", producer_turn_id="producer-private")

    assert raised.value is failure
    with sqlite3.connect(tmp_path / "bound-events.db") as conn:
        turns = conn.execute(
            "SELECT payload_json FROM turns WHERE host_id = ?",
            ("host-a",),
        ).fetchall()
        revisions = conn.execute(
            "SELECT known_incomplete, assistant_final_text "
            "FROM turn_content_revisions WHERE host_id = ? AND is_current = 1",
            ("host-a",),
        ).fetchall()
    assert len(turns) == 1
    assert revisions == [(0, "[ACP prompt cancelled]")]


def test_invalid_prompt_response_cancels_and_finalizes_open_turn(
    tmp_path: Path,
) -> None:
    client = FakeClient()
    client.prompt_result = {"stopReason": "end_turn"}
    ingestor = FakeIngestor()
    service = runtime(tmp_path, client, ingestor).start()

    with pytest.raises(AcpRuntimeProtocolError, match="invalid response"):
        service.prompt("question", producer_turn_id="producer-private")
    assert ("cancel", ("session-private",), {}) in client.calls
    assert ingestor.completions == 1
    assert ingestor.completion_reasons == [StopReason.CANCELLED]
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
