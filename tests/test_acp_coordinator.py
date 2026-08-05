"""Production ACP coordinator and command-path contract tests."""

from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from tendwire.backends.acp_coordinator import (
    AcpConsoleInputGap,
    AcpCoordinatorError,
    AcpSupervisor as AcpRuntimeCoordinator,
    HerdrAcpConsoleEndpoint,
    _SessionSlot as _RuntimeSlot,
    _CONSOLE_BRIDGE_INTERVAL_SECONDS,
    _derived_binding,
    _expire_derived_binding,
    _console_event_output,
    _bounded_console_output,
    _console_output_wire_bytes,
    _console_permission_selection,
    _latest_console_event_sequence,
    _parse_console_exchange,
    _parse_endpoint,
    _parse_status,
    production_acp_supervisor_factory as production_acp_runtime_factory,
)
from tendwire.backends.acp_runtime import RuntimeState, SessionOpenMode
from tendwire.backends.herdr_protocol import HerdrErrorResponse
from tendwire.command_submission import submit_command
from tendwire.config import Config
from tendwire.core.models import (
    BackendHealth,
    Snapshot,
    Worker,
    WorkerBinding,
)
from tendwire.daemon import DaemonHooks, TendwireDaemon
from tendwire.store.events import list_agent_events, record_agent_event
from tendwire.store.projection import (
    latest_snapshot,
    list_worker_bindings,
    save_snapshot,
)
from tendwire.store.receipts import get_command_request
from tendwire.store.schema import init_store
from .store_helpers import upsert_test_worker_bindings as upsert_worker_bindings


def _config(tmp_path: Path) -> Config:
    return Config(
        host_id="acp-host",
        data_dir=tmp_path,
        db_path=tmp_path / "tendwire.db",
        herdr_bin="herdr",
    )


def _binding() -> WorkerBinding:
    return WorkerBinding(
        host_id="acp-host",
        worker_id="worker-1",
        worker_fingerprint="worker-fingerprint",
        backend="herdr",
        target_kind="pane_id",
        target_value="pane-private",
        turn_target_kind="pane_id",
        turn_target_value="pane-private",
        sendable=True,
        private_fingerprint="herdr-private-binding",
    )


@pytest.fixture
def console_executor_factory():
    executors: list[ThreadPoolExecutor] = []

    def create() -> ThreadPoolExecutor:
        executor = ThreadPoolExecutor(max_workers=1)
        executors.append(executor)
        return executor

    yield create
    for executor in executors:
        executor.shutdown(wait=True, cancel_futures=True)


def _endpoint(*, generation: int = 42, lifecycle: str = "acp_owned_ready") -> dict[str, Any]:
    return {
        "type": "agent_acp_endpoint",
        "endpoint": {
            "transport": "stdio",
            "command": "herdr",
            "args": [
                "agent",
                "acp-attach",
                "pane-private",
                "--generation",
                str(generation),
                "--ticket",
                "one-shot-private-ticket",
            ],
            "protocol_version": 1,
        },
        "console": {
            "generation": generation,
            "lease": "console-coordinator-private-lease",
        },
        "worker": {
            "terminal_id": "pane-private",
            "workspace_id": "workspace-private",
            "tab_id": "tab-private",
            "pane_id": "pane-private",
            "name": "agent-name",
            "agent": "codex",
            "generation": generation,
        },
        "adapter": {"name": "codex-acp", "version": "1.2.3"},
        "session": {"mode": "new"},
        "cwd": "/tmp/project",
        "lifecycle": lifecycle,
    }


def _status(*, generation: int = 42, lifecycle: str = "acp_owned_attached") -> dict[str, Any]:
    endpoint = _endpoint(generation=generation)
    return {
        "type": "agent_acp_status",
        "worker": endpoint["worker"],
        "adapter": endpoint["adapter"],
        "session": endpoint["session"],
        "cwd": endpoint["cwd"],
        "lifecycle": lifecycle,
        "console_lifecycle": "attached",
    }


def test_endpoint_requires_explicit_acp_ownership_and_strict_attach_shape(tmp_path: Path) -> None:
    config = _config(tmp_path)
    parsed = _parse_endpoint(config, _binding(), _endpoint())
    assert parsed.generation == "42"
    assert parsed.command[0:3] == ("herdr", "agent", "acp-attach")

    with pytest.raises(AcpCoordinatorError, match="not ready"):
        _parse_endpoint(config, _binding(), _endpoint(lifecycle="ready"))
    injected = _endpoint()
    injected["endpoint"]["command"] = "/tmp/evil"
    with pytest.raises(AcpCoordinatorError, match="configured Herdr"):
        _parse_endpoint(config, _binding(), injected)
    replayed = _endpoint(generation=43)
    replayed["endpoint"]["args"][4] = "42"
    with pytest.raises(AcpCoordinatorError, match="inconsistent"):
        _parse_endpoint(config, _binding(), replayed)
    wrong_terminal = _endpoint()
    wrong_terminal["worker"]["terminal_id"] = "other-terminal"
    terminal_binding = replace(
        _binding(),
        target_kind="terminal_id",
        target_value="pane-private",
    )
    with pytest.raises(AcpCoordinatorError, match="target changed"):
        _parse_endpoint(config, terminal_binding, wrong_terminal)

    wrong_status = _status()
    wrong_status["worker"]["terminal_id"] = "other-terminal"
    with pytest.raises(AcpCoordinatorError, match="target changed"):
        _parse_status(terminal_binding, wrong_status)


def test_console_exchange_requires_floor_and_next_sequence_contract() -> None:
    result = {
        "type": "agent_acp_console_exchange",
        "inputs": [{"sequence": 3, "text": "continue"}],
        "outputs": [],
        "input_floor_sequence": 3,
        "output_floor_sequence": 1,
        "next_input_sequence": 4,
        "next_output_sequence": 1,
    }
    assert _parse_console_exchange(result, 2) == ((3, "continue"),)
    missing_next = dict(result)
    missing_next.pop("next_input_sequence")
    with pytest.raises(AcpCoordinatorError, match="shape"):
        _parse_console_exchange(missing_next, 2)
    lost = dict(result, input_floor_sequence=4)
    with pytest.raises(AcpConsoleInputGap, match="gap") as raised:
        _parse_console_exchange(lost, 2)
    assert raised.value.recovery_after_sequence == 3

    incomplete = dict(result, inputs=[], next_input_sequence=4)
    with pytest.raises(AcpCoordinatorError, match="incomplete"):
        _parse_console_exchange(incomplete, 2)

def test_live_only_console_policy_skips_lost_backlog_to_current_tail(
    tmp_path: Path,
    console_executor_factory,
) -> None:
    config = _config(tmp_path)
    assert config.db_path is not None
    init_store(config.db_path)
    exchanges: list[int] = []

    class EndpointClient:
        def agent_acp_console_exchange(
            self,
            _target: Any,
            *,
            generation: int,
            lease: str,
            after_input_sequence: int,
            output: Any,
            timeout: float,
        ) -> dict[str, Any]:
            exchanges.append(after_input_sequence)
            return {
                "type": "agent_acp_console_exchange",
                "inputs": (
                    [
                        {"sequence": 3, "text": "historical one"},
                        {"sequence": 4, "text": "historical two"},
                    ]
                    if after_input_sequence == 0
                    else []
                ),
                "outputs": [
                    {
                        "sequence": 1,
                        "event_id": "already-retained",
                        "stream": "status",
                        "text": "existing pane output",
                    }
                ],
                "input_floor_sequence": 3,
                "output_floor_sequence": 1,
                "next_input_sequence": 5,
                "next_output_sequence": 1,
            }

        def close(self) -> None:
            return None

    coordinator = AcpRuntimeCoordinator(
        config,
        threading.Event(),
        endpoint_client_factory=lambda _config: EndpointClient(),
        reconcile_interval=60.0,
    )
    binding = replace(
        _binding(),
        backend="acp",
        turn_target_kind="acp_session_id",
        turn_target_value="session-a",
        private_fingerprint="",
    )
    slot = _RuntimeSlot(
        continuity=_binding(),
        generation="42",
        runtime=SimpleNamespace(_binding=binding),
        console=HerdrAcpConsoleEndpoint(42, "coordinator-lease"),
        console_executor=console_executor_factory(),
    )

    coordinator._bridge_console_slot(slot)

    assert exchanges == [0]
    assert slot.console_input_sequence == 4
    assert slot.console_retained_output_bytes == _console_output_wire_bytes(
        [
            {
                "sequence": 1,
                "event_id": "already-retained",
                "stream": "status",
                "text": "existing pane output",
            }
        ]
    )


def test_console_event_projection_covers_messages_thought_tools_and_plan() -> None:
    assert _console_event_output("agent_message", {"text_delta": "done"}) == (
        "assistant",
        "done",
    )
    assert _console_event_output("thought", {"text_delta": "reason"}) == (
        "thought",
        "reason",
    )
    assert _console_event_output(
        "tool_call_update", {"snapshot": {"title": "pytest", "status": "completed"}}
    ) == ("tool", "pytest [completed]")
    assert _console_event_output(
        "plan", {"entries": [{"content": "verify", "status": "in_progress"}]}
    ) == ("plan", "[in_progress] verify")
    chunks = [
        _console_event_output("agent_message", {"text_delta": "hello"}),
        _console_event_output("agent_message", {"text_delta": " world"}),
    ]
    assert "".join(item[1] for item in chunks if item is not None) == "hello world"


def test_console_permission_selection_is_explicit_and_fail_closed() -> None:
    options = (("1", "Allow once (allow_once)"), ("2", "Reject (reject_once)"))
    assert _console_permission_selection("1", options) == "1"
    assert _console_permission_selection("allow", options) == "1"
    assert _console_permission_selection("deny", options) == "2"
    assert _console_permission_selection("do something else", options) is None


def test_console_output_batch_is_utf8_bounded_and_replay_deterministic() -> None:
    first = _bounded_console_output("event-a", "tool", "😀" * 100_000)
    second = _bounded_console_output("event-b", "assistant", "β" * 100_000)
    assert first["text"].encode("utf-8").decode("utf-8") == first["text"]
    assert "[console output truncated]" in first["text"]
    budget = _console_output_wire_bytes([first])
    assert _console_output_wire_bytes([first]) <= budget


def test_new_console_slot_starts_at_live_tail_and_delivers_only_fresh_events(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    assert config.db_path is not None
    init_store(config.db_path)
    for source_id, text in (("old-1", "old one"), ("old-2", "old two")):
        record_agent_event(
            config.db_path,
            config.host_id,
            kind="agent_message",
            source="acp",
            worker_id="worker-1",
            payload={"text_delta": text},
            source_session_id="session-a",
            source_event_id=source_id,
            visibility="private",
        )
    baseline = _latest_console_event_sequence(
        config.db_path, config.host_id, "worker-1", "session-a"
    )
    record_agent_event(
        config.db_path,
        config.host_id,
        kind="agent_message",
        source="acp",
        worker_id="worker-1",
        payload={"text_delta": "fresh"},
        source_session_id="session-a",
        source_event_id="fresh-1",
        visibility="private",
    )

    class EndpointClient:
        published: list[dict[str, Any]] = []
        next_output = 1

        def agent_acp_console_exchange(self, _target: str, **params: Any) -> dict[str, Any]:
            for item in params["output"]:
                self.published.append({"sequence": self.next_output, **dict(item)})
                self.next_output += 1
            return {
                "type": "agent_acp_console_exchange",
                "inputs": [],
                "outputs": list(self.published),
                "input_floor_sequence": 1,
                "output_floor_sequence": 1,
                "next_input_sequence": 1,
                "next_output_sequence": self.next_output,
            }

        def close(self) -> None:
            return None

    runtime = SimpleNamespace(
        _binding=replace(
            _binding(),
            backend="acp",
            turn_target_kind="acp_session_id",
            turn_target_value="session-a",
        )
    )
    slot = _RuntimeSlot(
        continuity=_binding(),
        generation="42",
        runtime=runtime,
        console=HerdrAcpConsoleEndpoint(42, "coordinator-lease"),
        console_event_sequence=baseline,
        console_executor=ThreadPoolExecutor(max_workers=1),
        console_submissions={},
        console_local_turns=set(),
    )
    coordinator = AcpRuntimeCoordinator(
        config,
        threading.Event(),
        endpoint_client_factory=lambda _config: EndpointClient(),
        reconcile_interval=60.0,
    )
    try:
        coordinator._bridge_console_slot(slot)
        assert EndpointClient.published == []
        coordinator._bridge_console_slot(slot)
        assert [item["text"] for item in EndpointClient.published] == ["fresh"]
    finally:
        assert slot.console_executor is not None
        slot.console_executor.shutdown(wait=True, cancel_futures=True)


def test_console_bridge_byte_batches_and_advances_only_published_prefix(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    assert config.db_path is not None
    init_store(config.db_path)
    for index in range(10):
        record_agent_event(
            config.db_path,
            config.host_id,
            kind="agent_message",
            source="acp",
            worker_id="worker-1",
            payload={"text_delta": "😀" * 40_000},
            source_session_id="session-a",
            source_event_id=f"large-agent-chunk-{index}",
            visibility="private",
        )

    class EndpointClient:
        retained: list[dict[str, Any]] = []
        next_output = 1

        def connect(self) -> None:
            return None

        def close(self) -> None:
            return None

        def agent_acp_console_exchange(
            self, _target: str, **params: Any
        ) -> dict[str, Any]:
            for item in params["output"]:
                EndpointClient.retained.append(
                    {"sequence": EndpointClient.next_output, **dict(item)}
                )
                EndpointClient.next_output += 1
            floor = (
                EndpointClient.retained[0]["sequence"]
                if EndpointClient.retained
                else EndpointClient.next_output
            )
            return {
                "type": "agent_acp_console_exchange",
                "inputs": [],
                "outputs": list(EndpointClient.retained),
                "input_floor_sequence": 1,
                "output_floor_sequence": floor,
                "next_input_sequence": 1,
                "next_output_sequence": EndpointClient.next_output,
            }

    coordinator = AcpRuntimeCoordinator(
        config,
        threading.Event(),
        endpoint_client_factory=lambda _config: EndpointClient(),
        reconcile_interval=60.0,
    )
    runtime = SimpleNamespace(
        _binding=replace(
            _binding(),
            backend="acp",
            turn_target_kind="acp_session_id",
            turn_target_value="session-a",
        )
    )
    executor = ThreadPoolExecutor(max_workers=1)
    slot = _RuntimeSlot(
        continuity=_binding(),
        generation="42",
        runtime=runtime,
        console=HerdrAcpConsoleEndpoint(42, "coordinator-lease"),
        console_executor=executor,
        console_retained_output_bytes=0,
        console_submissions={},
        console_local_turns=set(),
    )
    try:
        coordinator._bridge_console_slot(slot)
        first_cursor = slot.console_event_sequence
        assert 0 < first_cursor
        assert len(EndpointClient.retained) < 10
        assert _console_output_wire_bytes(EndpointClient.retained) <= 512 * 1024
        # A full retained queue cannot make Tendwire acknowledge the remainder.
        coordinator._bridge_console_slot(slot)
        assert slot.console_event_sequence == first_cursor
        # Once the pane drains, the exact unacknowledged suffix is published.
        EndpointClient.retained.clear()
        coordinator._bridge_console_slot(slot)
        coordinator._bridge_console_slot(slot)
        assert slot.console_event_sequence > first_cursor
    finally:
        executor.shutdown(wait=True, cancel_futures=True)


def test_supervisor_polls_console_independently_of_slow_reconcile_interval(
    tmp_path: Path,
) -> None:
    coordinator = AcpRuntimeCoordinator(
        _config(tmp_path), threading.Event(), reconcile_interval=60.0
    )
    ticks: list[float] = []

    def tick() -> None:
        ticks.append(time.monotonic())
        if len(ticks) == 3:
            coordinator._stop.set()

    coordinator._bridge_console_slots = tick  # type: ignore[method-assign]
    started = time.monotonic()
    coordinator._run()
    assert len(ticks) == 3
    assert 0.25 <= _CONSOLE_BRIDGE_INTERVAL_SECONDS <= 0.5
    assert ticks[-1] - started < 3.5 * _CONSOLE_BRIDGE_INTERVAL_SECONDS


def test_console_bridge_dispatches_slow_workers_independently(
    tmp_path: Path,
    console_executor_factory,
) -> None:
    coordinator = AcpRuntimeCoordinator(
        _config(tmp_path), threading.Event(), reconcile_interval=60.0
    )
    slow_entered = threading.Event()
    fast_entered = threading.Event()
    release = threading.Event()

    slow_slot = _RuntimeSlot(
        _binding(),
        "42",
        SimpleNamespace(),
        console=HerdrAcpConsoleEndpoint(42, "slow-lease"),
        console_executor=console_executor_factory(),
    )
    fast_slot = _RuntimeSlot(
        replace(_binding(), worker_id="worker-2"),
        "43",
        SimpleNamespace(),
        console=HerdrAcpConsoleEndpoint(43, "fast-lease"),
        console_executor=console_executor_factory(),
    )
    coordinator._slots = {"worker-1": slow_slot, "worker-2": fast_slot}

    def bridge(slot: _RuntimeSlot) -> None:
        if slot is slow_slot:
            slow_entered.set()
            assert release.wait(2.0)
        else:
            fast_entered.set()

    coordinator._bridge_console_slot = bridge  # type: ignore[method-assign]
    coordinator._bridge_console_slots()
    try:
        assert slow_entered.wait(1.0)
        assert fast_entered.wait(1.0)
    finally:
        release.set()
        for slot in (slow_slot, fast_slot):
            thread = slot.console_bridge_thread
            if thread is not None:
                thread.join(timeout=1.0)


def test_console_failure_remains_degraded_after_slot_disappears(
    tmp_path: Path,
    console_executor_factory,
) -> None:
    coordinator = AcpRuntimeCoordinator(
        _config(tmp_path),
        threading.Event(),
        reconcile_interval=60.0,
    )
    coordinator._state = RuntimeState.RUNNING
    slot = _RuntimeSlot(
        _binding(),
        "42",
        SimpleNamespace(),
        console=HerdrAcpConsoleEndpoint(42, "console-lease"),
        console_executor=console_executor_factory(),
    )

    def fail(_slot: _RuntimeSlot) -> None:
        raise OSError("console unavailable")

    coordinator._bridge_console_slot = fail  # type: ignore[method-assign]
    coordinator._bridge_console_slot_supervised(slot)
    assert coordinator.status()["healthy"] is False
    coordinator._slots.clear()
    coordinator._bridge_console_slots()
    status = coordinator.status()
    assert status["healthy"] is False
    assert status["failure_type"] == "OSError"


def test_first_console_failure_immediately_fences_prompt_route_until_success(
    tmp_path: Path,
    console_executor_factory,
) -> None:
    coordinator = AcpRuntimeCoordinator(
        _config(tmp_path),
        threading.Event(),
        reconcile_interval=60.0,
    )
    coordinator._state = RuntimeState.RUNNING
    runtime = SimpleNamespace(
        status=lambda: SimpleNamespace(healthy=True, failure_type=None),
        _binding=_binding(),
    )
    slot = _RuntimeSlot(
        _binding(),
        "42",
        runtime,
        console=HerdrAcpConsoleEndpoint(42, "console-lease"),
        console_executor=console_executor_factory(),
    )
    coordinator._slots["worker-1"] = slot
    worker = Worker(
        id="worker-1",
        name="worker",
        status="idle",
        fingerprint="worker-fingerprint",
    )
    issued_before_loss = coordinator.prompt_route(worker)
    assert issued_before_loss is not None
    reconcile_calls = 0

    def unexpected_reconcile(_worker_id: str, *, strict: bool) -> None:
        nonlocal reconcile_calls
        reconcile_calls += 1

    coordinator._reconcile_worker = unexpected_reconcile  # type: ignore[method-assign]

    failure_entered = threading.Event()
    release_failure = threading.Event()

    def fail(_slot: _RuntimeSlot) -> None:
        failure_entered.set()
        assert release_failure.wait(2.0)
        raise OSError("console unavailable")

    coordinator._bridge_console_slot = fail  # type: ignore[method-assign]
    failed_pass = threading.Thread(
        target=coordinator._bridge_console_slot_supervised,
        args=(slot,),
    )
    failed_pass.start()
    assert failure_entered.wait(1.0)
    release_failure.set()
    failed_pass.join(timeout=1.0)
    assert not failed_pass.is_alive()

    assert coordinator.prompt_route(worker) is None
    assert reconcile_calls == 0
    with pytest.raises(AcpCoordinatorError, match="visible console"):
        _ = issued_before_loss.binding_fingerprint
    assert coordinator.status()["healthy"] is False

    success_entered = threading.Event()
    release_success = threading.Event()

    def succeed(_slot: _RuntimeSlot) -> None:
        success_entered.set()
        assert release_success.wait(2.0)

    coordinator._bridge_console_slot = succeed  # type: ignore[method-assign]
    successful_pass = threading.Thread(
        target=coordinator._bridge_console_slot_supervised,
        args=(slot,),
    )
    successful_pass.start()
    assert success_entered.wait(1.0)
    # Starting a recovery bridge is not recovery; it must complete one
    # visible-console exchange before either command ingress can route again.
    assert coordinator.prompt_route(worker) is None
    release_success.set()
    successful_pass.join(timeout=1.0)
    assert not successful_pass.is_alive()

    assert coordinator.prompt_route(worker) is not None
    assert reconcile_calls == 0
    assert coordinator.status()["healthy"] is True


def test_superseded_console_success_cannot_clear_replacement_fence(
    tmp_path: Path,
    console_executor_factory,
) -> None:
    coordinator = AcpRuntimeCoordinator(
        _config(tmp_path),
        threading.Event(),
        reconcile_interval=60.0,
    )
    coordinator._state = RuntimeState.RUNNING
    runtime = SimpleNamespace(
        status=lambda: SimpleNamespace(healthy=True, failure_type=None),
        _binding=_binding(),
    )
    old = _RuntimeSlot(
        _binding(),
        "42",
        runtime,
        console=HerdrAcpConsoleEndpoint(42, "old-lease"),
        console_executor=console_executor_factory(),
    )
    replacement = _RuntimeSlot(
        _binding(),
        "43",
        runtime,
        console=HerdrAcpConsoleEndpoint(43, "replacement-lease"),
        console_executor=console_executor_factory(),
    )
    coordinator._slots["worker-1"] = replacement
    coordinator._console_failed_claims["worker-1"] = "worker-fingerprint"
    coordinator._bridge_console_slot = lambda _slot: None  # type: ignore[method-assign]

    coordinator._bridge_console_slot_supervised(old)
    assert "worker-1" in coordinator._console_failed_claims
    assert coordinator.status()["healthy"] is False

    coordinator._bridge_console_slot_supervised(replacement)
    assert "worker-1" not in coordinator._console_failed_claims
    assert coordinator.status()["healthy"] is True


def test_console_submission_rejects_a_retired_generation_before_store_access(
    tmp_path: Path,
    console_executor_factory,
) -> None:
    coordinator = AcpRuntimeCoordinator(
        _config(tmp_path), threading.Event(), reconcile_interval=60.0
    )
    coordinator._state = RuntimeState.RUNNING
    stale = _RuntimeSlot(
        _binding(),
        "42",
        SimpleNamespace(),
        HerdrAcpConsoleEndpoint(42, "stale-lease"),
        console_executor_factory(),
    )
    replacement = _RuntimeSlot(
        _binding(),
        "43",
        SimpleNamespace(),
        HerdrAcpConsoleEndpoint(43, "replacement-lease"),
        console_executor_factory(),
    )
    coordinator._slots["worker-1"] = replacement
    with pytest.raises(AcpCoordinatorError, match="generation is stale"):
        coordinator._submit_console_input(stale, 1, "must not cross sessions")


def test_console_local_turn_is_suppressed_before_acp_submission_emits(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    console_executor_factory,
) -> None:
    config = _config(tmp_path)
    assert config.db_path is not None
    init_store(config.db_path)
    worker = Worker(
        id="worker-1",
        name="Agent",
        status="active",
        meta={"stable_key": "wsk1_" + "a" * 64, "stable_key_version": 1},
    )
    snapshot = Snapshot(
        host_id=config.host_id,
        updated_at="2026-01-01T00:00:00+00:00",
        workers=[worker],
    )
    monkeypatch.setattr(
        "tendwire.backends.acp_coordinator.latest_snapshot",
        lambda _path, _host: snapshot,
    )
    observed: list[set[str]] = []

    def submit_while_observing(*_args: Any, **_kwargs: Any) -> Any:
        observed.append(set(slot.console_local_turns or ()))
        return SimpleNamespace(status="accepted")

    monkeypatch.setattr(
        "tendwire.command_submission.submit_command", submit_while_observing
    )
    continuity = replace(_binding(), worker_fingerprint=worker.fingerprint)
    runtime = SimpleNamespace(_session_id="session-a")
    slot = _RuntimeSlot(
        continuity,
        "42",
        runtime,
        console=HerdrAcpConsoleEndpoint(42, "coordinator-lease"),
        console_executor=console_executor_factory(),
        console_local_turns=set(),
    )
    coordinator = AcpRuntimeCoordinator(
        config, threading.Event(), reconcile_interval=60.0
    )

    assert coordinator._submit_console_input_fenced(slot, 1, "hello") == "instruction"
    assert len(observed) == 1
    assert len(observed[0]) == 1
    assert slot.console_local_turns == observed[0]


def test_stop_reports_failed_while_console_submission_thread_is_still_running(
    tmp_path: Path,
) -> None:
    coordinator = AcpRuntimeCoordinator(
        _config(tmp_path), threading.Event(), reconcile_interval=60.0
    )
    coordinator._state = RuntimeState.RUNNING
    entered = threading.Event()
    release = threading.Event()
    executor = ThreadPoolExecutor(max_workers=1)

    def block() -> None:
        entered.set()
        assert release.wait(2.0)

    future = executor.submit(block)
    assert entered.wait(1.0)
    runtime = SimpleNamespace(stop=lambda *, timeout: None)
    slot = _RuntimeSlot(
        _binding(),
        "42",
        runtime,
        console=HerdrAcpConsoleEndpoint(42, "coordinator-lease"),
        console_executor=executor,
        console_submissions={1: future},
    )
    coordinator._slots["worker-1"] = slot
    try:
        coordinator.stop(timeout=0.05)
        status = coordinator.status()
        assert status["state"] == "failed"
        assert status["failure_type"] == "AcpRuntimeStopTimeout"
    finally:
        release.set()
        future.result(timeout=1.0)


def test_stop_accounts_for_submission_work_after_slot_retirement(tmp_path: Path) -> None:
    coordinator = AcpRuntimeCoordinator(
        _config(tmp_path), threading.Event(), reconcile_interval=60.0
    )
    coordinator._state = RuntimeState.RUNNING
    entered = threading.Event()
    release = threading.Event()
    executor = ThreadPoolExecutor(max_workers=1)

    def block() -> None:
        entered.set()
        assert release.wait(2.0)

    future = executor.submit(block)
    assert entered.wait(1.0)
    slot = _RuntimeSlot(
        _binding(),
        "42",
        SimpleNamespace(stop=lambda *, timeout: None),
        console=HerdrAcpConsoleEndpoint(42, "coordinator-lease"),
        console_executor=executor,
        console_submissions={1: future},
    )
    coordinator._slots["worker-1"] = slot
    coordinator._retire_worker("worker-1")
    try:
        coordinator.stop(timeout=0.05)
        assert coordinator.status()["state"] == "failed"
        assert coordinator.status()["failure_type"] == "AcpRuntimeStopTimeout"
    finally:
        release.set()
        future.result(timeout=1.0)


def test_terminal_retired_work_is_reaped_but_active_work_remains_accounted(
    tmp_path: Path,
) -> None:
    coordinator = AcpRuntimeCoordinator(
        _config(tmp_path), threading.Event(), reconcile_interval=60.0
    )
    coordinator._state = RuntimeState.RUNNING
    release = threading.Event()
    executor = ThreadPoolExecutor(max_workers=1)
    future = executor.submit(lambda: release.wait(2.0))
    slot = _RuntimeSlot(
        _binding(),
        "42",
        SimpleNamespace(stop=lambda *, timeout: None),
        console=HerdrAcpConsoleEndpoint(42, "coordinator-lease"),
        console_executor=executor,
        console_submissions={1: future},
    )
    coordinator._slots["worker-1"] = slot
    coordinator._retire_worker("worker-1")

    coordinator._reap_retired_slots()
    assert coordinator._retired_slots == {id(slot): slot}
    release.set()
    future.result(timeout=1.0)
    coordinator._reap_retired_slots()
    assert coordinator._retired_slots == {}
    with pytest.raises(RuntimeError, match="shutdown"):
        executor.submit(lambda: None)


def test_console_submission_error_is_visible_before_input_ack(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    assert config.db_path is not None
    init_store(config.db_path)
    exchanges: list[dict[str, Any]] = []

    class EndpointClient:
        def agent_acp_console_exchange(self, _target: str, **params: Any) -> Any:
            exchanges.append(params)
            retained = [
                {"sequence": index, **dict(item)}
                for index, item in enumerate(params["output"], start=1)
            ]
            return {
                "type": "agent_acp_console_exchange",
                "inputs": [],
                "outputs": retained,
                "input_floor_sequence": 1,
                "output_floor_sequence": 1,
                "next_input_sequence": params["after_input_sequence"] + 1,
                "next_output_sequence": len(retained) + 1,
            }

        def close(self) -> None:
            return None

    executor = ThreadPoolExecutor(max_workers=1)
    failed = executor.submit(
        lambda: (_ for _ in ()).throw(RuntimeError("private command detail"))
    )
    with pytest.raises(RuntimeError):
        failed.result(timeout=1.0)
    slot = _RuntimeSlot(
        _binding(),
        "42",
        SimpleNamespace(
            _binding=replace(
                _binding(),
                backend="acp",
                turn_target_kind="acp_session_id",
                turn_target_value="session-a",
            )
        ),
        console=HerdrAcpConsoleEndpoint(42, "coordinator-lease"),
        console_executor=executor,
        console_retained_output_bytes=0,
        console_submissions={1: failed},
    )
    coordinator = AcpRuntimeCoordinator(
        config,
        threading.Event(),
        endpoint_client_factory=lambda _config: EndpointClient(),
        reconcile_interval=60.0,
    )
    try:
        coordinator._bridge_console_slot(slot)
        assert exchanges[0]["after_input_sequence"] == 1
        assert exchanges[0]["output"] == [
            {
                "event_id": "console-input:42:1",
                "stream": "error",
                "text": "instruction failed",
            }
        ]
        assert "private command detail" not in json.dumps(exchanges)
        assert slot.console_input_sequence == 1
        assert slot.console_submissions == {}
    finally:
        executor.shutdown(wait=True, cancel_futures=True)


def test_console_input_submission_worker_does_not_block_event_exchange(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    assert config.db_path is not None
    init_store(config.db_path)
    entered = threading.Event()
    release = threading.Event()
    exchanges: list[int] = []

    class EndpointClient:
        def connect(self) -> None:
            return None

        def close(self) -> None:
            return None

        def agent_acp_console_exchange(self, _target: str, **params: Any) -> Any:
            exchanges.append(int(params["after_input_sequence"]))
            return {
                "type": "agent_acp_console_exchange",
                "inputs": [{"sequence": 1, "text": "stream while I run"}],
                "outputs": [],
                "input_floor_sequence": 1,
                "output_floor_sequence": 1,
                "next_input_sequence": 2,
                "next_output_sequence": 1,
            }

    coordinator = AcpRuntimeCoordinator(
        config,
        threading.Event(),
        endpoint_client_factory=lambda _config: EndpointClient(),
        reconcile_interval=60.0,
    )

    def blocking_submit(_slot: Any, _sequence: int, _text: str) -> str:
        entered.set()
        assert release.wait(2.0)
        return "instruction"

    coordinator._submit_console_input = blocking_submit  # type: ignore[method-assign]
    runtime = SimpleNamespace(
        _binding=replace(
            _binding(),
            backend="acp",
            turn_target_kind="acp_session_id",
            turn_target_value="session-a",
        )
    )
    executor = ThreadPoolExecutor(max_workers=1)
    slot = _RuntimeSlot(
        continuity=_binding(),
        generation="42",
        runtime=runtime,
        console=HerdrAcpConsoleEndpoint(42, "coordinator-lease"),
        console_executor=executor,
        console_submissions={},
        console_local_turns=set(),
    )
    try:
        coordinator._bridge_console_slot(slot)
        assert entered.wait(1.0)
        started = time.monotonic()
        coordinator._bridge_console_slot(slot)
        assert time.monotonic() - started < 0.5
        assert exchanges == [0, 0]
        assert slot.console_input_sequence == 0
    finally:
        release.set()
        executor.shutdown(wait=True, cancel_futures=True)


def test_canonical_herdr_acp_contract_fixture_executes_configured_binary(
    tmp_path: Path,
) -> None:
    fixture = json.loads(
        (Path(__file__).parent / "fixtures" / "herdr_acp_contract_v1.json").read_text(
            encoding="utf-8"
        )
    )
    assert fixture["request"] == {
        "id": "tw:acp:endpoint",
        "method": "agent.acp_endpoint",
        "params": {"target": "term_abc"},
    }
    config = replace(_config(tmp_path), herdr_bin="/opt/herdr/bin/herdr")
    continuity = replace(
        _binding(),
        target_kind="terminal_id",
        target_value="term_abc",
        turn_target_kind="terminal_id",
        turn_target_value="term_abc",
        private_fingerprint="fixture-binding",
    )
    parsed = _parse_endpoint(config, continuity, fixture["result"])
    assert parsed.command[0] == "/opt/herdr/bin/herdr"
    assert parsed.command[1:] == tuple(fixture["result"]["endpoint"]["args"])
    assert parsed.generation == "42"
    assert parsed.console == HerdrAcpConsoleEndpoint(
        42, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQ"
    )


class _Route:
    binding_fingerprint = "acp-private-binding"

    def __init__(self, failure: BaseException | None = None) -> None:
        self.failure = failure
        self.calls: list[tuple[str, str, float]] = []

    @property
    def supports_steering(self) -> bool:
        return False

    @contextmanager
    def prepare(self):
        yield self

    def prompt(
        self,
        text: str,
        *,
        producer_turn_id: str,
        timeout: float,
        on_send_start: Any = None,
    ) -> None:
        self.calls.append((text, producer_turn_id, timeout))
        if callable(on_send_start):
            on_send_start()
        if self.failure is not None:
            raise self.failure


class _PreparationFailureRoute(_Route):
    @contextmanager
    def prepare(self):
        raise AcpCoordinatorError("transient generation status failure")
        yield self


class _BeforeTransportFailureRoute(_Route):
    def prompt(
        self,
        text: str,
        *,
        producer_turn_id: str,
        timeout: float,
        on_send_start: Any = None,
    ) -> None:
        del on_send_start
        self.calls.append((text, producer_turn_id, timeout))
        raise AcpCoordinatorError("visible console route changed before write")


class _SteeringRoute(_Route):
    def __init__(self, outcome: str = "injected") -> None:
        super().__init__()
        self.outcome = outcome
        self.steering_calls: list[tuple[str, str, float]] = []

    @property
    def supports_steering(self) -> bool:
        return True

    def steer(
        self,
        text: str,
        *,
        producer_turn_id: str,
        timeout: float,
        on_send_start: Any = None,
    ) -> str:
        self.steering_calls.append((text, producer_turn_id, timeout))
        if callable(on_send_start):
            on_send_start()
        return self.outcome


def _seed(config: Config) -> Worker:
    assert config.db_path is not None
    init_store(config.db_path)
    worker = Worker(
        id="worker-1",
        name="worker",
        status="idle",
        fingerprint="worker-fingerprint",
        meta={
            "stable_key": "wsk1_" + ("a" * 64),
            "stable_key_version": 1,
        },
    )
    save_snapshot(
        config.db_path,
        Snapshot(
            host_id=config.host_id,
            updated_at="2026-07-31T00:00:00+00:00",
            workers=[worker],
            backend_health=[
                BackendHealth(
                    name="herdr",
                    status="healthy",
                    outcome="healthy_non_empty",
                )
            ],
        ),
    )
    continuity = _binding()
    acp_route = replace(
        continuity,
        backend="acp",
        target_kind="acp_session_id",
        target_value="session-private",
        turn_target_kind="acp_session_id",
        turn_target_value="session-private",
        private_fingerprint="acp-private-binding",
    )
    upsert_worker_bindings(config.db_path, [continuity, acp_route])
    stored = latest_snapshot(config.db_path, config.host_id)
    assert stored is not None
    enriched = save_snapshot(
        config.db_path,
        stored,
        worker_bindings=[acp_route],
        binding_backend="acp",
    )
    return enriched.workers[0]


def _request(request_id: str = "request-1") -> dict[str, Any]:
    return {
        "schema_version": 1,
        "action": "send_instruction",
        "request_id": request_id,
        "dry_run": False,
        "target": {"worker_id": "worker-1"},
        "instruction": {"text": "do the work"},
    }


def test_acp_command_uses_durable_receipt_and_duplicate_does_not_resend(tmp_path: Path) -> None:
    config = _config(tmp_path)
    worker = _seed(config)
    route = _Route()
    first = submit_command(
        config,
        _request(),
        acp_prompt_router=lambda routed: route if routed == worker else None,
    )
    second = submit_command(
        config,
        _request(),
        acp_prompt_router=lambda _worker: route,
    )
    assert first.status == "accepted"
    assert second.status == "accepted"
    assert len(route.calls) == 1
    receipt = get_command_request(config.db_path, config.host_id, "request-1")
    assert receipt is not None and receipt["state"] == "accepted"
    assert "acp-private-binding" not in json.dumps(first.to_dict())


@pytest.mark.parametrize("observed_status", ["idle", "active"])
def test_live_acp_route_uses_advertised_steering_despite_observer_lag(
    tmp_path: Path,
    observed_status: str,
) -> None:
    config = _config(tmp_path)
    worker = _seed(config)
    assert config.db_path is not None
    observed = replace(worker, status=observed_status)
    save_snapshot(
        config.db_path,
        Snapshot(
            host_id=config.host_id,
            updated_at="2026-07-31T00:00:01+00:00",
            workers=[observed],
            backend_health=[
                BackendHealth(
                    name="herdr",
                    status="healthy",
                    outcome="healthy_non_empty",
                )
            ],
        ),
    )
    upsert_worker_bindings(config.db_path, [_binding()])
    route = _SteeringRoute()

    envelope = submit_command(
        config,
        _request("request-active-steer"),
        acp_prompt_router=lambda routed: route if routed.id == observed.id else None,
    )

    assert envelope.status == "accepted"
    assert envelope.disposition == "terminal_accepted"
    assert route.calls == []
    assert len(route.steering_calls) == 1
    receipt = get_command_request(
        config.db_path,
        config.host_id,
        "request-active-steer",
    )
    assert receipt is not None and receipt["state"] == "accepted"


def test_definite_acp_steering_failure_is_rejected_not_uncertain(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    worker = _seed(config)
    assert config.db_path is not None
    active = replace(worker, status="active")
    save_snapshot(
        config.db_path,
        Snapshot(
            host_id=config.host_id,
            updated_at="2026-07-31T00:00:01+00:00",
            workers=[active],
            backend_health=[
                BackendHealth(
                    name="herdr",
                    status="healthy",
                    outcome="healthy_non_empty",
                )
            ],
        ),
    )
    upsert_worker_bindings(config.db_path, [_binding()])
    route = _SteeringRoute("failed")

    envelope = submit_command(
        config,
        _request("request-active-steer-failed"),
        acp_prompt_router=lambda _routed: route,
    )

    assert envelope.status == "rejected"
    assert envelope.disposition == "terminal_rejected"
    receipt = get_command_request(
        config.db_path,
        config.host_id,
        "request-active-steer-failed",
    )
    assert receipt is not None and receipt["state"] == "rejected"


def test_acp_failure_after_send_started_is_uncertain_and_never_falls_back(tmp_path: Path) -> None:
    config = _config(tmp_path)
    _seed(config)
    route = _Route(RuntimeError("private ticket and argv"))
    legacy_calls: list[str] = []
    envelope = submit_command(
        config,
        _request("request-uncertain"),
        acp_prompt_router=lambda _worker: route,
    )
    assert envelope.status == "request_state_uncertain"
    assert envelope.disposition == "terminal_uncertain"
    assert len(route.calls) == 1
    assert "private ticket" not in json.dumps(envelope.to_dict())
    receipt = get_command_request(
        config.db_path,
        config.host_id,
        "request-uncertain",
    )
    assert receipt is not None and receipt["state"] == "uncertain"


def test_acp_generation_preflight_failure_is_retryable_before_receipt(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    worker = _seed(config)
    route = _PreparationFailureRoute()

    envelope = submit_command(
        config,
        _request("request-preflight-retry"),
        acp_prompt_router=lambda routed: route if routed == worker else None,
    )

    assert envelope.status == "backend_unavailable"
    assert envelope.disposition == "no_receipt"
    assert route.calls == []
    assert get_command_request(
        config.db_path,
        config.host_id,
        "request-preflight-retry",
    ) is None


def test_production_route_checks_generation_before_reserving_receipt(
    tmp_path: Path,
    console_executor_factory,
) -> None:
    config = _config(tmp_path)
    worker = _seed(config)
    coordinator = AcpRuntimeCoordinator(
        config,
        threading.Event(),
        reconcile_interval=60.0,
    )
    coordinator._state = RuntimeState.RUNNING
    runtime = SimpleNamespace(
        status=lambda: SimpleNamespace(healthy=True, failure_type=None),
        _binding=_binding(),
    )
    coordinator._slots[worker.id] = _RuntimeSlot(
        _binding(),
        "42",
        runtime,
        console=HerdrAcpConsoleEndpoint(42, "coordinator-lease"),
        console_executor=console_executor_factory(),
    )
    coordinator._require_attached_generation = (  # type: ignore[method-assign]
        lambda _slot: (_ for _ in ()).throw(
            AcpCoordinatorError("transient Herdr status timeout")
        )
    )

    envelope = submit_command(
        config,
        _request("request-production-preflight"),
        acp_prompt_router=coordinator.prompt_route,
    )

    assert envelope.status == "backend_unavailable"
    assert envelope.disposition == "no_receipt"
    assert get_command_request(
        config.db_path,
        config.host_id,
        "request-production-preflight",
    ) is None


def test_acp_failure_before_transport_boundary_is_immediately_retryable(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    worker = _seed(config)
    failed_route = _BeforeTransportFailureRoute()

    first = submit_command(
        config,
        _request("request-prewrite-retry"),
        acp_prompt_router=lambda routed: failed_route if routed == worker else None,
    )
    assert first.status == "backend_unavailable"
    assert first.disposition == "no_receipt"
    receipt = get_command_request(
        config.db_path,
        config.host_id,
        "request-prewrite-retry",
    )
    assert receipt is not None
    assert receipt["state"] == "reserved"
    assert receipt["binding_fingerprint"] is None

    good_route = _Route()
    second = submit_command(
        config,
        _request("request-prewrite-retry"),
        acp_prompt_router=lambda routed: good_route if routed == worker else None,
    )
    assert second.status == "accepted"
    assert second.disposition == "terminal_accepted"
    assert len(good_route.calls) == 1
    receipt = get_command_request(
        config.db_path,
        config.host_id,
        "request-prewrite-retry",
    )
    assert receipt is not None and receipt["state"] == "accepted"


def test_route_authority_failure_is_safe_before_receipt_reservation(tmp_path: Path) -> None:
    config = _config(tmp_path)
    worker = _seed(config)

    class VanishedRoute:
        @property
        def binding_fingerprint(self) -> str:
            raise AcpCoordinatorError("slot changed")

        def prompt(self, *_args: Any, **_kwargs: Any) -> None:
            raise AssertionError("a route without authority must not send")

    required = submit_command(
        config,
        _request("route-race-required"),
        acp_prompt_router=lambda routed: VanishedRoute() if routed == worker else None,
    )
    assert required.status == "backend_unavailable"
    assert get_command_request(
        config.db_path,
        config.host_id,
        "route-race-required",
    ) is None


def test_concurrent_duplicate_acp_command_has_one_external_send(tmp_path: Path) -> None:
    config = _config(tmp_path)
    _seed(config)
    entered = threading.Event()
    release = threading.Event()

    class BlockingRoute(_Route):
        def prompt(
            self,
            text: str,
            *,
            producer_turn_id: str,
            timeout: float,
            on_send_start: Any = None,
        ) -> None:
            self.calls.append((text, producer_turn_id, timeout))
            if callable(on_send_start):
                on_send_start()
            entered.set()
            assert release.wait(1.0)

    route = BlockingRoute()
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(
            submit_command,
            config,
            _request("request-concurrent"),
            acp_prompt_router=lambda _worker: route,
        )
        assert entered.wait(1.0)
        second = pool.submit(
            submit_command,
            config,
            _request("request-concurrent"),
            acp_prompt_router=lambda _worker: route,
        )
        second_result = second.result(timeout=1.0)
        release.set()
        first_result = first.result(timeout=1.0)
    assert first_result.status == "accepted"
    assert second_result.status == "pending"
    assert len(route.calls) == 1


def test_required_has_no_legacy_fallback_when_route_is_absent(tmp_path: Path) -> None:
    config = _config(tmp_path)
    _seed(config)
    envelope = submit_command(
        config,
        _request("request-required"),
        acp_prompt_router=lambda _worker: None,
    )
    assert envelope.status == "backend_unavailable"
    assert get_command_request(
        config.db_path,
        config.host_id,
        "request-required",
    ) is None


def test_new_and_resumed_endpoint_session_invariants(tmp_path: Path) -> None:
    config = _config(tmp_path)
    invalid_new = _endpoint()
    invalid_new["session"] = {"mode": "new", "id": "already-bound"}
    with pytest.raises(AcpCoordinatorError, match="already has"):
        _parse_endpoint(config, _binding(), invalid_new)
    resumed = _endpoint()
    resumed["session"] = {"mode": "resume", "id": "session-private"}
    assert _parse_endpoint(config, _binding(), resumed).session_id == "session-private"
    missing = _endpoint()
    missing["session"] = {"mode": "resume"}
    with pytest.raises(AcpCoordinatorError, match="session id"):
        _parse_endpoint(config, _binding(), missing)


def test_derived_acp_binding_outlives_observation_lease() -> None:
    continuity = replace(
        _binding(),
        observed_at="2026-07-31T00:00:00+00:00",
        expires_at="2026-07-31T00:00:05+00:00",
    )
    derived = _derived_binding(continuity, "session-private")
    assert derived.expires_at.startswith("9999-")
    assert derived.observed_at > continuity.observed_at
    assert derived.private_fingerprint != continuity.private_fingerprint


def test_reconnect_remints_endpoint_instead_of_replaying_attach_ticket(tmp_path: Path) -> None:
    config = _config(tmp_path)
    assert config.db_path is not None
    init_store(config.db_path)
    upsert_worker_bindings(config.db_path, [_binding()])
    minted: list[str] = []
    generation = [42]
    runtimes: list[Any] = []

    class EndpointClient:
        def agent_acp_endpoint(self, _target: Any, *, timeout: float) -> Any:
            value = _endpoint(generation=generation[0])
            ticket = f"one-shot-private-ticket-{len(minted) + 1}"
            value["endpoint"]["args"][6] = ticket
            minted.append(ticket)
            return value

        def agent_acp_status(self, _target: Any, *, timeout: float) -> Any:
            return _status(generation=generation[0])

        def close(self) -> None:
            return None

    class Runtime:
        def __init__(self, _client: Any, **kwargs: Any) -> None:
            # The endpoint generation fences Herdr authority, but must not be
            # reused as the ACP transport stream generation. AcpRuntime owns a
            # fresh nonce for every adapter process.
            assert "stream_generation" not in kwargs
            self._binding = kwargs["binding"]
            self.stopped = False
            self.prompt_calls = 0
            runtimes.append(self)

        def start(self) -> None:
            return None

        def stop(self, *, timeout: float) -> None:
            self.stopped = True

        def status(self) -> Any:
            return SimpleNamespace(
                healthy=not self.stopped,
                failure_type=None,
                updates_ingested=0,
                permissions_ingested=0,
                permissions_selected=0,
                permissions_cancelled=0,
                invalid_permission_selections=0,
                prompts_started=0,
                prompts_completed=0,
                prompts_failed=0,
                cancellation_requests=0,
            )

        def submit_prompt(self, *_args: Any, **_kwargs: Any) -> None:
            self.prompt_calls += 1
            return None

    coordinator = AcpRuntimeCoordinator(
        config,
        threading.Event(),
        endpoint_client_factory=lambda _config: EndpointClient(),
        connection_factory=lambda *_args, **_kwargs: object(),
        session_factory=Runtime,
        reconcile_interval=60.0,
    ).start()
    try:
        assert minted == ["one-shot-private-ticket-1"]
        coordinator._reconcile_worker("worker-1", strict=True)
        worker = Worker(
            id="worker-1",
            name="worker",
            status="idle",
            fingerprint="worker-fingerprint",
        )
        route = coordinator.prompt_route(worker)
        assert route is not None
        route.prompt("hello", producer_turn_id="producer", timeout=1.0)
        assert minted == ["one-shot-private-ticket-1"]
        assert runtimes[0].prompt_calls == 1
        generation[0] = 43
        with pytest.raises(AcpCoordinatorError, match="generation lease"):
            route.prompt("stale", producer_turn_id="producer-2", timeout=1.0)
        assert runtimes[0].prompt_calls == 1
        coordinator._reconcile_worker("worker-1", strict=True)
        assert minted == [
            "one-shot-private-ticket-1",
            "one-shot-private-ticket-2",
        ]
        replaced_route = coordinator.prompt_route(worker)
        assert replaced_route is not None
        generation[0] = 44
        coordinator._reconcile_worker("worker-1", strict=True)
        with pytest.raises(AcpCoordinatorError, match="stale"):
            _ = replaced_route.binding_fingerprint
        with pytest.raises(AcpCoordinatorError, match="stale"):
            replaced_route.prompt(
                "must-not-cross-generations",
                producer_turn_id="producer-3",
                timeout=1.0,
            )
        assert runtimes[-1].prompt_calls == 0
    finally:
        coordinator.stop()


def test_concurrent_reconcile_mints_only_one_endpoint(tmp_path: Path) -> None:
    config = _config(tmp_path)
    assert config.db_path is not None
    init_store(config.db_path)
    entered = threading.Event()
    release = threading.Event()
    minted = 0

    class EndpointClient:
        def agent_acp_endpoint(self, _target: Any, *, timeout: float) -> Any:
            nonlocal minted
            minted += 1
            entered.set()
            assert release.wait(2.0)
            return _endpoint()

        def agent_acp_status(self, _target: Any, *, timeout: float) -> Any:
            return _status()

        def close(self) -> None:
            return None

    class Runtime:
        def __init__(self, _client: Any, **kwargs: Any) -> None:
            self._binding = kwargs["binding"]
            self.stopped = False

        def start(self) -> None:
            return None

        def stop(self, *, timeout: float) -> None:
            self.stopped = True

        def status(self) -> Any:
            return SimpleNamespace(healthy=not self.stopped, failure_type=None)

    coordinator = AcpRuntimeCoordinator(
        config,
        threading.Event(),
        endpoint_client_factory=lambda _config: EndpointClient(),
        connection_factory=lambda *_args, **_kwargs: object(),
        session_factory=Runtime,
        reconcile_interval=60.0,
    ).start()
    upsert_worker_bindings(config.db_path, [_binding()])
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(coordinator._reconcile_worker, "worker-1", strict=True)
            assert entered.wait(1.0)
            second = pool.submit(coordinator._reconcile_worker, "worker-1", strict=True)
            release.set()
            first.result(timeout=2.0)
            second.result(timeout=2.0)
        assert minted == 1
    finally:
        release.set()
        coordinator.stop()


def test_stop_cannot_leave_inflight_reconcile_runtime_published(tmp_path: Path) -> None:
    config = _config(tmp_path)
    assert config.db_path is not None
    init_store(config.db_path)
    entered = threading.Event()
    release = threading.Event()
    stop_done = threading.Event()
    runtimes: list[Any] = []

    class EndpointClient:
        def agent_acp_endpoint(self, _target: Any, *, timeout: float) -> Any:
            entered.set()
            assert release.wait(2.0)
            return _endpoint()

        def close(self) -> None:
            return None

    class Runtime:
        def __init__(self, _client: Any, **kwargs: Any) -> None:
            self._binding = kwargs["binding"]
            self.stopped = False
            runtimes.append(self)

        def start(self) -> None:
            return None

        def stop(self, *, timeout: float) -> None:
            self.stopped = True

        def status(self) -> Any:
            return SimpleNamespace(healthy=not self.stopped, failure_type=None)

    coordinator = AcpRuntimeCoordinator(
        config,
        threading.Event(),
        endpoint_client_factory=lambda _config: EndpointClient(),
        connection_factory=lambda *_args, **_kwargs: object(),
        session_factory=Runtime,
        reconcile_interval=60.0,
    ).start()
    upsert_worker_bindings(config.db_path, [_binding()])
    with ThreadPoolExecutor(max_workers=2) as pool:
        reconcile = pool.submit(coordinator._reconcile_worker, "worker-1", strict=True)
        assert entered.wait(1.0)

        def stop() -> None:
            coordinator.stop()
            stop_done.set()

        stopping = pool.submit(stop)
        assert not stop_done.wait(0.1)
        release.set()
        with pytest.raises(AcpCoordinatorError, match="stopping"):
            reconcile.result(timeout=2.0)
        stopping.result(timeout=2.0)
    assert coordinator._slots == {}
    assert runtimes and all(runtime.stopped for runtime in runtimes)


def test_stop_closes_permission_waiter_before_generation_fence_deadline(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    assert config.db_path is not None
    init_store(config.db_path)
    upsert_worker_bindings(config.db_path, [_binding()])
    answer_entered = threading.Event()
    broker_closed = threading.Event()
    runtime_stopped = threading.Event()

    class EndpointClient:
        def agent_acp_endpoint(self, _target: Any, *, timeout: float) -> Any:
            return _endpoint()

        def agent_acp_status(self, _target: Any, *, timeout: float) -> Any:
            return _status()

        def close(self) -> None:
            return None

    class Broker:
        def owns(self, _decision: Any) -> bool:
            return True

        def answer(self, _decision: Any, *, timeout: float) -> None:
            del timeout
            answer_entered.set()
            assert broker_closed.wait(2.0)
            raise AcpCoordinatorError("permission bridge closed")

        def close(self) -> None:
            broker_closed.set()

    broker = Broker()

    class Runtime:
        def __init__(self, _client: Any, **kwargs: Any) -> None:
            self._binding = kwargs["binding"]
            self._binder = kwargs["session_binding_callback"]

        def start(self) -> None:
            if self._binder is not None:
                self._binding = self._binder("session-private", self._binding)

        def stop(self, *, timeout: float) -> None:
            del timeout
            runtime_stopped.set()

        def status(self) -> Any:
            return SimpleNamespace(
                healthy=not runtime_stopped.is_set(),
                failure_type=None,
            )

    coordinator = AcpRuntimeCoordinator(
        config,
        threading.Event(),
        endpoint_client_factory=lambda _config: EndpointClient(),
        connection_factory=lambda *_args, **_kwargs: object(),
        session_factory=Runtime,
        reconcile_interval=60.0,
    ).start()
    slot = coordinator._slots["worker-1"]
    slot.permission_broker = broker  # type: ignore[assignment]
    decision = SimpleNamespace(worker_id="worker-1")

    with ThreadPoolExecutor(max_workers=2) as pool:
        answer = pool.submit(
            coordinator.answer_permission_decision,
            decision,
            timeout=30.0,
        )
        assert answer_entered.wait(1.0)
        started = time.monotonic()
        coordinator.stop(timeout=0.5)
        assert time.monotonic() - started < 0.5
        with pytest.raises(AcpCoordinatorError, match="closed"):
            answer.result(timeout=1.0)

    assert broker_closed.is_set()
    assert runtime_stopped.is_set()


def test_prompt_frame_acknowledgement_fences_generation_retirement(tmp_path: Path) -> None:
    config = _config(tmp_path)
    assert config.db_path is not None
    init_store(config.db_path)
    upsert_worker_bindings(config.db_path, [_binding()])
    generation = [42]
    endpoint_calls = 0
    prompt_entered = threading.Event()
    prompt_release = threading.Event()
    runtimes: list[Any] = []

    class EndpointClient:
        def agent_acp_endpoint(self, _target: Any, *, timeout: float) -> Any:
            nonlocal endpoint_calls
            endpoint_calls += 1
            return _endpoint(generation=generation[0])

        def agent_acp_status(self, _target: Any, *, timeout: float) -> Any:
            return _status(generation=generation[0])

        def close(self) -> None:
            return None

    class Runtime:
        def __init__(self, _client: Any, **kwargs: Any) -> None:
            self._binding = kwargs["binding"]
            self._binder = kwargs["session_binding_callback"]
            self.stopped = False
            runtimes.append(self)

        def start(self) -> None:
            if self._binder is not None:
                self._binding = self._binder(
                    f"session-private-{len(runtimes)}", self._binding
                )

        def submit_prompt(self, *_args: Any, **_kwargs: Any) -> None:
            prompt_entered.set()
            assert prompt_release.wait(2.0)

        def stop(self, *, timeout: float) -> None:
            self.stopped = True

        def status(self) -> Any:
            return SimpleNamespace(healthy=not self.stopped, failure_type=None)

    coordinator = AcpRuntimeCoordinator(
        config,
        threading.Event(),
        endpoint_client_factory=lambda _config: EndpointClient(),
        connection_factory=lambda *_args, **_kwargs: object(),
        session_factory=Runtime,
        reconcile_interval=60.0,
    ).start()
    worker = Worker(
        id="worker-1",
        name="worker",
        status="working",
        fingerprint="worker-fingerprint",
    )
    route = coordinator.prompt_route(worker)
    assert route is not None
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            prompt = pool.submit(
                route.prompt,
                "one frame",
                producer_turn_id="producer-1",
                timeout=1.0,
            )
            assert prompt_entered.wait(1.0)
            generation[0] = 43
            reconcile = pool.submit(
                coordinator._reconcile_worker, "worker-1", strict=True
            )
            # A new endpoint cannot be minted and the old transport cannot be
            # stopped while its request frame is still being acknowledged.
            assert not reconcile.done()
            assert endpoint_calls == 1
            assert not runtimes[0].stopped
            prompt_release.set()
            prompt.result(timeout=2.0)
            reconcile.result(timeout=2.0)
        assert endpoint_calls == 2
        assert runtimes[0].stopped
        assert len(runtimes) == 2
    finally:
        prompt_release.set()
        coordinator.stop()


def test_runtime_factory_failure_rolls_back_resumed_binding(tmp_path: Path) -> None:
    config = _config(tmp_path)
    assert config.db_path is not None
    init_store(config.db_path)

    class EndpointClient:
        def agent_acp_endpoint(self, _target: Any, *, timeout: float) -> Any:
            endpoint = _endpoint()
            endpoint["session"] = {"mode": "resume", "id": "session-private"}
            return endpoint

        def close(self) -> None:
            return None

    class Client:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    client = Client()
    coordinator = AcpRuntimeCoordinator(
        config,
        threading.Event(),
        endpoint_client_factory=lambda _config: EndpointClient(),
        connection_factory=lambda *_args, **_kwargs: client,
        session_factory=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("constructor failed with --ticket private")
        ),
        reconcile_interval=60.0,
    ).start()
    upsert_worker_bindings(config.db_path, [_binding()])
    try:
        with pytest.raises(RuntimeError, match="constructor failed"):
            coordinator._reconcile_worker("worker-1", strict=True)
        assert client.closed
        assert list_worker_bindings(config.db_path, config.host_id, backend="acp") == []
    finally:
        coordinator.stop()


def test_coordinator_start_revokes_orphaned_process_binding(tmp_path: Path) -> None:
    config = _config(tmp_path)
    assert config.db_path is not None
    init_store(config.db_path)
    orphaned = replace(
        _derived_binding(_binding(), "orphaned-session"),
        observed_at="2099-01-01T00:00:00+00:00",
    )
    upsert_worker_bindings(config.db_path, [orphaned])

    coordinator = AcpRuntimeCoordinator(
        config,
        threading.Event(),
        endpoint_client_factory=lambda _config: object(),
        connection_factory=lambda *_args, **_kwargs: object(),
        reconcile_interval=60.0,
    ).start()
    try:
        assert list_worker_bindings(
            config.db_path, config.host_id, backend="acp"
        ) == []
    finally:
        coordinator.stop()


def test_coordinator_fallback_revokes_future_observed_binding_now(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    assert config.db_path is not None
    derived = replace(
        _derived_binding(_binding(), "future-session"),
        observed_at="2099-01-01T00:00:00+00:00",
    )
    upsert_worker_bindings(config.db_path, [derived])

    _expire_derived_binding(config, derived, reason="acp_runtime_retired")

    assert list_worker_bindings(config.db_path, config.host_id, backend="acp") == []
    retired = list_worker_bindings(
        config.db_path,
        config.host_id,
        backend="acp",
        include_expired=True,
    )
    assert len(retired) == 1
    assert retired[0].reason == "acp_runtime_retired"


def test_production_coordinator_installs_durable_permission_bridge(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class EmptyLifecycleClient:
        def workspace_list(self, *, timeout: float) -> list[Any]:
            return []

        def pane_list(self, *, timeout: float) -> list[Any]:
            return []

        def agent_list(self, *, timeout: float) -> list[Any]:
            return []

        def close(self) -> None:
            return None

    monkeypatch.setattr(
        "tendwire.backends.acp_coordinator._default_endpoint_client_factory",
        lambda _config: EmptyLifecycleClient(),
    )
    config = _config(tmp_path)
    assert config.db_path is not None
    init_store(config.db_path)
    coordinator = production_acp_runtime_factory(config, threading.Event())

    coordinator.start()
    try:
        assert coordinator.status()["state"] == "running"
    finally:
        coordinator.stop()


def test_coordinator_rejects_disabled_reconciliation(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="reconcile_interval must be finite and positive"):
        AcpRuntimeCoordinator(
            _config(tmp_path),
            threading.Event(),
            reconcile_interval=0,
        )


def test_required_zero_workers_is_idle_healthy_then_new_unowned_worker_degrades(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    assert config.db_path is not None
    init_store(config.db_path)

    class RejectingClient:
        def agent_acp_endpoint(self, _target: Any, *, timeout: float) -> Any:
            raise AcpCoordinatorError("ACP ownership required")

        def close(self) -> None:
            return None

    coordinator = AcpRuntimeCoordinator(
        config,
        threading.Event(),
        endpoint_client_factory=lambda _config: RejectingClient(),
        reconcile_interval=60.0,
    ).start()
    try:
        assert coordinator.status()["healthy"] is True
        upsert_worker_bindings(config.db_path, [_binding()])
        coordinator._reconcile(strict=False)
        health = coordinator.status()
        assert health["healthy"] is False
        assert health["failure_type"] == "AcpCoordinatorError"
    finally:
        coordinator.stop()


def test_required_does_not_cache_non_acp_endpoint_failure(tmp_path: Path) -> None:
    config = _config(tmp_path)
    assert config.db_path is not None
    init_store(config.db_path)
    upsert_worker_bindings(config.db_path, [_binding()])
    endpoint_calls = 0

    class NonAcpClient:
        def agent_acp_endpoint(self, _target: Any, *, timeout: float) -> Any:
            nonlocal endpoint_calls
            endpoint_calls += 1
            raise HerdrErrorResponse(
                {"code": "acp_ownership_required", "message": "PTY-owned"},
                "request-private",
            )

        def close(self) -> None:
            return None

    coordinator = AcpRuntimeCoordinator(
        config,
        threading.Event(),
        endpoint_client_factory=lambda _config: NonAcpClient(),
        reconcile_interval=60.0,
    )
    with pytest.raises(AcpCoordinatorError, match="failed to attach"):
        coordinator.start()
    assert endpoint_calls == 1
