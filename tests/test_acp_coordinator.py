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
    AcpCoordinatorError,
    AcpRuntimeCoordinator,
    HerdrAcpConsoleEndpoint,
    _RuntimeSlot,
    _CONSOLE_BRIDGE_INTERVAL_SECONDS,
    _derived_binding,
    _console_event_output,
    _bounded_console_output,
    _console_output_wire_bytes,
    _fit_console_output_batch,
    _console_permission_selection,
    _load_console_event_cursor,
    _load_console_input_cursor,
    _parse_console_exchange,
    _parse_endpoint,
    _parse_status,
    _prepare_console_event_cursor,
    _record_console_submission_outcome,
    production_acp_runtime_factory,
)
from tendwire.backends.acp_runtime import RuntimeState, SessionOpenMode
from tendwire.backends.herdr_protocol import HerdrErrorResponse
from tendwire.backends.herdr_turns import TurnIngestionScheduler, TurnRefreshResult
from tendwire.command_submission import submit_acp_command, submit_command
from tendwire.config import Config
from tendwire.core.models import (
    BackendHealth,
    Snapshot,
    Worker,
    WorkerBinding,
)
from tendwire.daemon import DaemonHooks, TendwireDaemon
from tendwire.store.sqlite import (
    get_command_request,
    init_store,
    list_agent_events,
    list_worker_bindings,
    record_agent_event,
    save_snapshot,
    upsert_worker_bindings,
)


def _config(tmp_path: Path, *, policy: str = "acp_preferred") -> Config:
    return Config(
        host_id="acp-host",
        data_dir=tmp_path,
        db_path=tmp_path / "tendwire.db",
        herdr_backend="socket",
        herdr_bin="herdr",
        agent_event_source=policy,
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
    with pytest.raises(AcpCoordinatorError, match="gap"):
        _parse_console_exchange(lost, 2)

    incomplete = dict(result, inputs=[], next_input_sequence=4)
    with pytest.raises(AcpCoordinatorError, match="incomplete"):
        _parse_console_exchange(incomplete, 2)

    output = dict(
        result,
        outputs=[
            {
                "sequence": 7,
                "event_id": "event-7",
                "stream": "assistant",
                "text": "done",
            }
        ],
        output_floor_sequence=7,
        next_output_sequence=8,
    )
    assert _parse_console_exchange(output, 2) == ((3, "continue"),)
    malformed_output = dict(output, outputs=[{"sequence": 7, "text": "done"}])
    with pytest.raises(AcpCoordinatorError, match="output shape"):
        _parse_console_exchange(malformed_output, 2)
    wrong_output_next = dict(output, next_output_sequence=9)
    with pytest.raises(AcpCoordinatorError, match="next output"):
        _parse_console_exchange(wrong_output_next, 2)


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
        "tool_call_update",
        {
            "snapshot": {
                "title": "Edit source",
                "status": "completed",
                "rawInput": {"secret": "not rendered"},
                "rawOutput": "not rendered",
                "content": [
                    {"type": "content", "content": {"type": "text", "text": "done"}},
                    {
                        "type": "diff",
                        "path": "/workspace/source.py",
                        "oldText": "old",
                        "newText": "new",
                    },
                ],
            }
        },
    ) == (
        "tool",
        "Edit source [completed]\ndone\ndiff /workspace/source.py\n- old\n+ new",
    )
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


def test_console_cursors_survive_restart_crash_boundaries(tmp_path: Path) -> None:
    config = _config(tmp_path)
    assert config.db_path is not None
    init_store(config.db_path)
    record_agent_event(
        config.db_path,
        config.host_id,
        kind="extension",
        source="tendwire-console",
        worker_id="worker-1",
        payload={"extension": "tendwire.acp.console_cursor", "sequence": 41},
        source_session_id="session-a",
        source_event_id="cursor:41",
        visibility="private",
    )
    record_agent_event(
        config.db_path,
        config.host_id,
        kind="extension",
        source="tendwire-console",
        worker_id="worker-1",
        payload={
            "extension": "tendwire.acp.console_input_cursor",
            "generation": 42,
            "input_sequence": 7,
        },
        source_session_id="session-a",
        source_event_id="input:42:7",
        visibility="private",
    )
    assert _load_console_event_cursor(
        config.db_path, config.host_id, "worker-1", "session-a"
    ) == 41
    assert _load_console_input_cursor(
        config.db_path, config.host_id, "worker-1", "session-a", 42
    ) == 7
    # The visible Herdr input queue survives an ACP adapter remint. Its cursor
    # is owned by worker generation 42, so a replacement session must recover
    # the already acknowledged input rather than reporting a false gap.
    assert _load_console_input_cursor(
        config.db_path, config.host_id, "worker-1", "session-b", 42
    ) == 7
    assert _load_console_input_cursor(
        config.db_path, config.host_id, "worker-1", "session-a", 43
    ) == 0


def test_missing_checkpoint_replays_from_zero_including_new_session_start_updates(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    assert config.db_path is not None
    init_store(config.db_path)
    record_agent_event(
        config.db_path,
        config.host_id,
        kind="agent_message",
        source="acp",
        worker_id="worker-1",
        payload={"text_delta": "stored before coordinator checkpoint"},
        source_session_id="session-resume",
        source_event_id="agent-before-crash",
        visibility="private",
    )
    assert _prepare_console_event_cursor(
        config.db_path,
        config.host_id,
        "worker-1",
        "session-resume",
        session_mode=SessionOpenMode.RESUME,
        generation=42,
    ) == 0
    assert _load_console_event_cursor(
        config.db_path, config.host_id, "worker-1", "session-resume"
    ) == 0

    record_agent_event(
        config.db_path,
        config.host_id,
        kind="agent_message",
        source="acp",
        worker_id="worker-1",
        payload={"text_delta": "setup replay"},
        source_session_id="session-new",
        source_event_id="agent-during-setup",
        visibility="private",
    )
    baseline = _prepare_console_event_cursor(
        config.db_path,
        config.host_id,
        "worker-1",
        "session-new",
        session_mode=SessionOpenMode.NEW,
        generation=43,
    )
    assert baseline == 0
    assert _load_console_event_cursor(
        config.db_path, config.host_id, "worker-1", "session-new"
    ) == baseline


def test_console_failure_outcome_is_durable_before_input_ack(tmp_path: Path) -> None:
    config = _config(tmp_path)
    assert config.db_path is not None
    init_store(config.db_path)
    _record_console_submission_outcome(
        config.db_path,
        config.host_id,
        "worker-1",
        "session-a",
        generation=42,
        input_sequence=7,
        outcome="error",
    )
    # Simulate a crash before the following input-cursor write. The error is
    # replayable even though Herdr input 7 was not acknowledged yet.
    assert _load_console_input_cursor(
        config.db_path, config.host_id, "worker-1", "session-a", 42
    ) == 0
    stored = list_agent_events(
        config.db_path,
        config.host_id,
        worker_id="worker-1",
        source="acp",
        session_id="session-a",
    )
    assert len(stored) == 1
    assert _console_event_output(stored[0].event.kind, stored[0].event.payload) == (
        "error",
        "instruction failed",
    )


def test_console_output_batch_is_utf8_bounded_and_replay_deterministic() -> None:
    first = _bounded_console_output("event-a", "tool", "😀" * 100_000)
    second = _bounded_console_output("event-b", "assistant", "β" * 100_000)
    assert first["text"].encode("utf-8").decode("utf-8") == first["text"]
    assert "[console output truncated]" in first["text"]
    budget = _console_output_wire_bytes([first])
    assert _fit_console_output_batch([first, second], budget=budget) == [first]
    assert _fit_console_output_batch([first, second], budget=budget) == [first]
    assert _console_output_wire_bytes([first]) <= budget


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
        console_cursor_loaded=True,
        console_executor=executor,
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
        assert slot.console_event_sequence > first_cursor
    finally:
        executor.shutdown(wait=True, cancel_futures=True)


def test_console_bridge_polls_independently_of_slow_reconcile_interval(
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
    coordinator._run_console_bridge()
    assert len(ticks) == 3
    assert 0.25 <= _CONSOLE_BRIDGE_INTERVAL_SECONDS <= 0.5
    assert ticks[-1] - started < 3.5 * _CONSOLE_BRIDGE_INTERVAL_SECONDS


def test_console_bridge_dispatches_slow_workers_independently(tmp_path: Path) -> None:
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
    )
    fast_slot = _RuntimeSlot(
        replace(_binding(), worker_id="worker-2"),
        "43",
        SimpleNamespace(),
        console=HerdrAcpConsoleEndpoint(43, "fast-lease"),
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


def test_console_failure_remains_degraded_after_slot_disappears(tmp_path: Path) -> None:
    coordinator = AcpRuntimeCoordinator(
        _config(tmp_path, policy="acp_required"),
        threading.Event(),
        reconcile_interval=60.0,
    )
    coordinator._state = RuntimeState.RUNNING
    slot = _RuntimeSlot(
        _binding(),
        "42",
        SimpleNamespace(),
        console=HerdrAcpConsoleEndpoint(42, "console-lease"),
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
) -> None:
    coordinator = AcpRuntimeCoordinator(
        _config(tmp_path, policy="acp_required"),
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
) -> None:
    coordinator = AcpRuntimeCoordinator(
        _config(tmp_path, policy="acp_required"),
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
    )
    replacement = _RuntimeSlot(
        _binding(),
        "43",
        runtime,
        console=HerdrAcpConsoleEndpoint(43, "replacement-lease"),
    )
    coordinator._slots["worker-1"] = replacement
    coordinator._console_failed_workers.add("worker-1")
    coordinator._console_degraded = True
    coordinator._bridge_console_slot = lambda _slot: None  # type: ignore[method-assign]

    coordinator._bridge_console_slot_supervised(old)
    assert "worker-1" in coordinator._console_failed_workers
    assert coordinator.status()["healthy"] is False

    coordinator._bridge_console_slot_supervised(replacement)
    assert "worker-1" not in coordinator._console_failed_workers
    assert coordinator.status()["healthy"] is True


def test_failed_remint_retains_exact_acp_claim_and_blocks_preferred_fallback(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, policy="acp_preferred")
    worker = _seed(config)
    coordinator = AcpRuntimeCoordinator(
        config,
        threading.Event(),
        reconcile_interval=60.0,
    )
    coordinator._state = RuntimeState.RUNNING
    runtime = SimpleNamespace(
        status=lambda: SimpleNamespace(healthy=True, failure_type=None),
        stop=lambda *, timeout: None,
        _binding=_binding(),
    )
    slot = _RuntimeSlot(
        _binding(),
        "42",
        runtime,
        console=HerdrAcpConsoleEndpoint(42, "console-lease"),
    )
    coordinator._slots[worker.id] = slot

    def fail_console(_slot: _RuntimeSlot) -> None:
        raise OSError("console unavailable")

    coordinator._bridge_console_slot = fail_console  # type: ignore[method-assign]
    coordinator._continuity_bindings = (  # type: ignore[method-assign]
        lambda: ({worker.id: _binding()}, 0)
    )

    def fail_remint(_continuity: WorkerBinding) -> None:
        raise AcpCoordinatorError("replacement attach failed")

    coordinator._reconcile_binding = fail_remint  # type: ignore[method-assign]

    for _attempt in range(3):
        coordinator._bridge_console_slot_supervised(slot)

    assert worker.id not in coordinator._slots
    assert coordinator.claims_worker(worker.id, worker.fingerprint) is True
    assert coordinator.prompt_route(worker) is None

    def forbidden_legacy(_config: Config) -> Any:
        raise AssertionError("retired ACP claim must not reopen legacy pane I/O")

    envelope = submit_command(
        config,
        _request("failed-remint-no-fallback"),
        socket_client_factory=forbidden_legacy,
        acp_prompt_router=coordinator.prompt_route,
        acp_worker_owner=coordinator.claims_worker,
    )
    assert envelope.status == "backend_unavailable"
    assert envelope.disposition == "no_receipt"
    assert get_command_request(
        config.db_path,
        config.host_id,
        "failed-remint-no-fallback",
    ) is None


def test_failed_claim_clears_only_after_exact_herdr_authority_disappears(
    tmp_path: Path,
) -> None:
    coordinator = AcpRuntimeCoordinator(
        _config(tmp_path, policy="acp_preferred"),
        threading.Event(),
        reconcile_interval=60.0,
    )
    coordinator._state = RuntimeState.RUNNING
    coordinator._console_failed_workers.add("worker-1")
    coordinator._console_failed_claims["worker-1"] = "worker-fingerprint"
    coordinator._console_degraded = True
    coordinator._continuity_bindings = (  # type: ignore[method-assign]
        lambda: ({}, 1)
    )
    coordinator._herdr_authority_claims = (  # type: ignore[method-assign]
        lambda: {("worker-1", "worker-fingerprint")}
    )

    coordinator._reconcile_locked(strict=False)
    assert coordinator.claims_worker("worker-1", "worker-fingerprint") is True
    assert coordinator.status()["healthy"] is False

    coordinator._herdr_authority_claims = lambda: set()  # type: ignore[method-assign]
    coordinator._reconcile_locked(strict=False)
    assert coordinator.claims_worker("worker-1", "worker-fingerprint") is False
    assert coordinator._console_degraded is False


def test_first_console_failure_survives_unique_route_ambiguity_and_retirement(
    tmp_path: Path,
) -> None:
    coordinator = AcpRuntimeCoordinator(
        _config(tmp_path, policy="acp_preferred"),
        threading.Event(),
        reconcile_interval=60.0,
    )
    coordinator._state = RuntimeState.RUNNING
    runtime = SimpleNamespace(
        status=lambda: SimpleNamespace(healthy=True, failure_type=None),
        stop=lambda *, timeout: None,
        _binding=_binding(),
    )
    slot = _RuntimeSlot(
        _binding(),
        "42",
        runtime,
        console=HerdrAcpConsoleEndpoint(42, "console-lease"),
    )
    coordinator._slots["worker-1"] = slot
    coordinator._bridge_console_slot = (  # type: ignore[method-assign]
        lambda _slot: (_ for _ in ()).throw(OSError("console unavailable"))
    )
    coordinator._bridge_console_slot_supervised(slot)
    assert coordinator.claims_worker("worker-1", "worker-fingerprint") is True

    # Two sendable routes make the worker non-unique. The old slot is stale,
    # but exact Herdr authority remains and therefore so must the ACP claim.
    coordinator._continuity_bindings = lambda: ({}, 1)  # type: ignore[method-assign]
    coordinator._herdr_authority_claims = (  # type: ignore[method-assign]
        lambda: {("worker-1", "worker-fingerprint")}
    )
    coordinator._reconcile_locked(strict=False)

    assert "worker-1" not in coordinator._slots
    assert coordinator.claims_worker("worker-1", "worker-fingerprint") is True
    assert coordinator.status()["healthy"] is False


def test_published_claim_survives_unhealthy_runtime_retire_and_failed_remint(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, policy="acp_preferred")
    worker = _seed(config)
    coordinator = AcpRuntimeCoordinator(
        config,
        threading.Event(),
        reconcile_interval=60.0,
    )
    coordinator._state = RuntimeState.RUNNING
    runtime = SimpleNamespace(
        status=lambda: SimpleNamespace(healthy=False, failure_type="runtime_failed"),
        stop=lambda *, timeout: None,
        _binding=_binding(),
    )
    slot = _RuntimeSlot(
        _binding(),
        "42",
        runtime,
        console=HerdrAcpConsoleEndpoint(42, "console-lease"),
    )
    coordinator._slots[worker.id] = slot
    coordinator._published_acp_claims[worker.id] = worker.fingerprint
    coordinator._continuity_bindings = (  # type: ignore[method-assign]
        lambda: ({worker.id: _binding()}, 0)
    )
    coordinator._resolve_endpoint = (  # type: ignore[method-assign]
        lambda _continuity: (_ for _ in ()).throw(
            AcpCoordinatorError("replacement attach failed")
        )
    )

    coordinator._reconcile_locked(strict=False)

    assert worker.id not in coordinator._slots
    assert coordinator.claims_worker(worker.id, worker.fingerprint) is True

    def forbidden_legacy(_config: Config) -> Any:
        raise AssertionError("published ACP ownership must survive failed remint")

    envelope = submit_command(
        config,
        _request("unhealthy-runtime-failed-remint"),
        socket_client_factory=forbidden_legacy,
        acp_prompt_router=coordinator.prompt_route,
        acp_worker_owner=coordinator.claims_worker,
    )
    assert envelope.status == "backend_unavailable"
    assert envelope.disposition == "no_receipt"
    assert get_command_request(
        config.db_path,
        config.host_id,
        "unhealthy-runtime-failed-remint",
    ) is None


def test_console_submission_rejects_a_retired_generation_before_store_access(
    tmp_path: Path,
) -> None:
    coordinator = AcpRuntimeCoordinator(
        _config(tmp_path), threading.Event(), reconcile_interval=60.0
    )
    coordinator._state = RuntimeState.RUNNING
    stale = _RuntimeSlot(_binding(), "42", SimpleNamespace())
    replacement = _RuntimeSlot(_binding(), "43", SimpleNamespace())
    coordinator._slots["worker-1"] = replacement
    with pytest.raises(AcpCoordinatorError, match="generation is stale"):
        coordinator._submit_console_input(stale, 1, "must not cross sessions")


def test_console_local_turn_is_suppressed_before_acp_submission_emits(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
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
        console_cursor_loaded=True,
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
    ) -> object:
        self.steering_calls.append((text, producer_turn_id, timeout))
        if callable(on_send_start):
            on_send_start()
        return SimpleNamespace(outcome=self.outcome)


def _seed(config: Config) -> Worker:
    assert config.db_path is not None
    init_store(config.db_path)
    worker = Worker(
        id="worker-1",
        name="worker",
        status="idle",
        fingerprint="worker-fingerprint",
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
    upsert_worker_bindings(config.db_path, [_binding()])
    return worker


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


def test_active_acp_worker_uses_advertised_steering_instead_of_second_prompt(
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
    route = _SteeringRoute()

    envelope = submit_command(
        config,
        _request("request-active-steer"),
        acp_prompt_router=lambda routed: route if routed.id == active.id else None,
        acp_required=True,
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
        acp_required=True,
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
        acp_required=True,
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
        acp_worker_owner=coordinator.claims_worker,
        acp_required=True,
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
        acp_required=True,
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
    assert receipt["send_started_at"] is None

    good_route = _Route()
    second = submit_command(
        config,
        _request("request-prewrite-retry"),
        acp_prompt_router=lambda routed: good_route if routed == worker else None,
        acp_required=True,
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


def test_preferred_acp_owned_route_loss_fails_closed_without_receipt_or_legacy(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, policy="acp_preferred")
    worker = _seed(config)

    def forbidden_legacy(_config: Config) -> Any:
        raise AssertionError("ACP-owned console loss must not reach legacy pane I/O")

    for _attempt in range(2):
        envelope = submit_command(
            config,
            _request("preferred-owned-console-loss"),
            socket_client_factory=forbidden_legacy,
            acp_prompt_router=lambda _worker: None,
            acp_worker_owner=lambda worker_id, fingerprint: (
                worker_id == worker.id and fingerprint == worker.fingerprint
            ),
        )
        assert envelope.status == "backend_unavailable"
        assert envelope.disposition == "no_receipt"
    assert get_command_request(
        config.db_path,
        config.host_id,
        "preferred-owned-console-loss",
    ) is None


def test_preferred_non_acp_worker_still_uses_legacy_sender(tmp_path: Path) -> None:
    config = _config(tmp_path, policy="acp_preferred")
    _seed(config)
    legacy_calls: list[str] = []

    class LegacyClient:
        def connect(self) -> "LegacyClient":
            return self

        def request(
            self,
            method: str,
            params: dict[str, Any],
            *,
            timeout: float | None = None,
        ) -> dict[str, Any]:
            del timeout
            legacy_calls.append(method)
            if method == "agent.get":
                return {"result": {"agent": {"pane_id": "pane-private"}}}
            if method == "agent.prompt":
                return {
                    "type": "agent_prompted",
                    "agent": {"pane_id": "pane-private"},
                    "delivery": "submitted",
                }
            return {"accepted": True, "params": params}

        def close(self) -> None:
            return None

    envelope = submit_command(
        config,
        _request("preferred-non-acp-worker"),
        socket_client_factory=lambda _config: LegacyClient(),
        acp_prompt_router=lambda _worker: None,
        acp_worker_owner=lambda _worker_id, _fingerprint: False,
    )

    assert envelope.status == "accepted"
    assert legacy_calls[-1] == "agent.prompt"


def test_preferred_snapshot_failure_with_owner_oracle_never_falls_back(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path, policy="acp_preferred")
    _seed(config)

    def unavailable_snapshot(_config: Config) -> Snapshot:
        raise OSError("authority store temporarily unavailable")

    def forbidden_legacy(_config: Config) -> Any:
        raise AssertionError("unknown ACP ownership must not reach legacy pane I/O")

    monkeypatch.setattr(
        "tendwire.command_submission._current_snapshot", unavailable_snapshot
    )
    envelope = submit_command(
        config,
        _request("preferred-authority-read-failure"),
        socket_client_factory=forbidden_legacy,
        acp_prompt_router=lambda _worker: None,
        acp_worker_owner=lambda _worker_id, _fingerprint: False,
    )

    assert envelope.status == "backend_unavailable"
    assert envelope.disposition == "no_receipt"
    assert get_command_request(
        config.db_path,
        config.host_id,
        "preferred-authority-read-failure",
    ) is None


def test_shadow_owned_command_is_observation_only_before_receipt(tmp_path: Path) -> None:
    config = _config(tmp_path, policy="acp_shadow")
    worker = replace(_seed(config), status="working")
    assert config.db_path is not None
    save_snapshot(
        config.db_path,
        Snapshot(
            host_id=config.host_id,
            updated_at="2026-07-31T00:00:01+00:00",
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
    route = _Route()

    envelope = submit_acp_command(
        config,
        _request("shadow-observation-only"),
        prompt_router=lambda _worker: route,
        observation_only=True,
    )

    assert envelope is not None
    assert envelope.status == "backend_unavailable"
    assert envelope.disposition == "no_receipt"
    assert route.calls == []
    assert get_command_request(
        config.db_path,
        config.host_id,
        "shadow-observation-only",
    ) is None


def test_daemon_shadow_owned_command_never_reaches_legacy_sender(tmp_path: Path) -> None:
    config = _config(tmp_path, policy="acp_shadow")
    worker = _seed(config)
    route = _Route()
    legacy_calls: list[str] = []

    class Runtime:
        def prompt_route(self, routed: Worker) -> _Route | None:
            return route if routed == worker else None

    def legacy_sender(_config: Config, _payload: str) -> Any:
        legacy_calls.append("calibrate-or-write")
        raise AssertionError("ACP-owned shadow target must not use legacy PTY I/O")

    daemon = TendwireDaemon(
        config,
        hooks=DaemonHooks(submit_command=legacy_sender),
    )
    daemon._acp_runtime = Runtime()

    envelope = daemon.submit_command(_request("shadow-daemon-fence"))

    assert envelope.status == "backend_unavailable"
    assert envelope.disposition == "no_receipt"
    assert route.calls == []
    assert legacy_calls == []


def test_daemon_preferred_console_loss_uses_claim_to_block_legacy_sender(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path, policy="acp_preferred")
    worker = _seed(config)

    class Runtime:
        def prompt_route(self, _worker: Worker) -> None:
            return None

        def claims_worker(self, worker_id: str, fingerprint: str) -> bool:
            return worker_id == worker.id and fingerprint == worker.fingerprint

    def forbidden_legacy(_config: Config) -> Any:
        raise AssertionError("ACP-owned console loss must not use legacy pane I/O")

    monkeypatch.setattr(
        "tendwire.command_submission._default_socket_client_factory",
        forbidden_legacy,
    )
    daemon = TendwireDaemon(config)
    daemon._acp_runtime = Runtime()

    envelope = daemon.submit_command(_request("daemon-preferred-console-loss"))

    assert envelope.status == "backend_unavailable"
    assert envelope.disposition == "no_receipt"
    assert get_command_request(
        config.db_path,
        config.host_id,
        "daemon-preferred-console-loss",
    ) is None


def test_daemon_shadow_preserves_ordinary_legacy_worker_submission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path, policy="acp_shadow")
    _seed(config)
    legacy_calls: list[str] = []

    class Runtime:
        def prompt_route(self, _worker: Worker) -> None:
            return None

    class LegacyClient:
        def connect(self) -> "LegacyClient":
            return self

        def request(
            self,
            method: str,
            params: dict[str, Any],
            *,
            timeout: float | None = None,
        ) -> dict[str, Any]:
            del timeout
            legacy_calls.append(method)
            if method == "agent.get":
                return {"result": {"agent": {"pane_id": "pane-private"}}}
            if method == "agent.prompt":
                return {
                    "type": "agent_prompted",
                    "agent": {"pane_id": "pane-private"},
                    "delivery": "submitted",
                }
            return {"accepted": True, "params": params}

        def close(self) -> None:
            return None

    monkeypatch.setattr(
        "tendwire.command_submission._default_socket_client_factory",
        lambda _config: LegacyClient(),
    )
    daemon = TendwireDaemon(config)
    daemon._acp_runtime = Runtime()

    envelope = daemon.submit_command(_request("shadow-legacy-worker"))

    assert envelope.status == "accepted"
    assert legacy_calls[-1] == "agent.prompt"


def test_daemon_wires_shadow_ownership_fence_into_legacy_scheduler(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = replace(
        _config(tmp_path, policy="acp_shadow"),
        herdr_backend="cli",
        socket_path=tmp_path / "shadow.sock",
    )
    callback: Any | None = None

    class Runtime:
        def start(self) -> None:
            return None

        def stop(self, *, timeout: float) -> None:
            return None

        def status(self) -> dict[str, Any]:
            return {"state": "running", "healthy": True}

        def owns_worker(self, worker_id: str, fingerprint: str) -> bool:
            return worker_id == "worker-1" and fingerprint == "worker-fingerprint"

    class Scheduler:
        def set_worker_exclusion(self, value: Any) -> None:
            nonlocal callback
            callback = value

        def start(self) -> None:
            return None

        def request_refresh(self) -> None:
            return None

        def stop(self, *, flush_timeout_seconds: float) -> None:
            return None

    def observe(_config: Config) -> Snapshot:
        snapshot = Snapshot(
            host_id=config.host_id,
            updated_at="2026-07-31T00:00:00+00:00",
            workers=[],
            backend_health=[],
        )
        assert config.db_path is not None
        save_snapshot(config.db_path, snapshot)
        return snapshot

    daemon = TendwireDaemon(
        config,
        hooks=DaemonHooks(
            observe_initial_snapshot=observe,
            turn_scheduler_factory=lambda _config: Scheduler(),
            acp_runtime_factory=lambda _config, _stop: Runtime(),
        ),
    )
    monkeypatch.setattr("tendwire.daemon.UnixSocketJSONServer.start", lambda _self: None)
    try:
        daemon.start()
        assert callable(callback)
        assert callback("worker-1", "worker-fingerprint") is True
        assert callback("legacy-worker", "legacy-fingerprint") is False
    finally:
        daemon.stop()


def test_route_authority_failure_is_safe_before_receipt_reservation(tmp_path: Path) -> None:
    config = _config(tmp_path)
    _seed(config)

    class VanishedRoute:
        @property
        def binding_fingerprint(self) -> str:
            raise AcpCoordinatorError("slot changed")

        def prompt(self, *_args: Any, **_kwargs: Any) -> None:
            raise AssertionError("a route without authority must not send")

    assert (
        submit_acp_command(
            config,
            _request("route-race-preferred"),
            prompt_router=lambda _worker: VanishedRoute(),
        )
        is None
    )
    required = submit_acp_command(
        config,
        _request("route-race-required"),
        prompt_router=lambda _worker: VanishedRoute(),
        required=True,
    )
    assert required is not None
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
    config = _config(tmp_path, policy="acp_required")
    _seed(config)
    envelope = submit_command(
        config,
        _request("request-required"),
        acp_prompt_router=lambda _worker: None,
        acp_required=True,
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
        client_factory=lambda *_args, **_kwargs: object(),
        runtime_factory=Runtime,
        reconcile_interval=60.0,
    ).start()
    try:
        assert minted == ["one-shot-private-ticket-1"]
        assert coordinator._published_acp_claims == {
            "worker-1": "worker-fingerprint"
        }
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
        client_factory=lambda *_args, **_kwargs: object(),
        runtime_factory=Runtime,
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
        client_factory=lambda *_args, **_kwargs: object(),
        runtime_factory=Runtime,
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
        client_factory=lambda *_args, **_kwargs: object(),
        runtime_factory=Runtime,
        durable_permission_bridge=True,
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
        client_factory=lambda *_args, **_kwargs: object(),
        runtime_factory=Runtime,
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
        client_factory=lambda *_args, **_kwargs: client,
        runtime_factory=lambda *_args, **_kwargs: (_ for _ in ()).throw(
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
    orphaned = _derived_binding(_binding(), "orphaned-session")
    upsert_worker_bindings(config.db_path, [orphaned])

    coordinator = AcpRuntimeCoordinator(
        config,
        threading.Event(),
        endpoint_client_factory=lambda _config: object(),
        client_factory=lambda *_args, **_kwargs: object(),
        reconcile_interval=60.0,
    ).start()
    try:
        assert list_worker_bindings(
            config.db_path, config.host_id, backend="acp"
        ) == []
    finally:
        coordinator.stop()


def test_production_coordinator_installs_durable_permission_bridge(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    assert config.db_path is not None
    init_store(config.db_path)
    coordinator = production_acp_runtime_factory(config, threading.Event())

    coordinator.start()
    try:
        assert coordinator.status()["state"] == "running"
        assert coordinator._durable_permission_bridge is True
    finally:
        coordinator.stop()


def test_coordinator_forwards_explicit_permission_bridge(tmp_path: Path) -> None:
    config = _config(tmp_path)
    assert config.db_path is not None
    init_store(config.db_path)
    upsert_worker_bindings(config.db_path, [_binding()])
    seen: list[Any] = []

    class EndpointClient:
        def agent_acp_endpoint(self, _target: Any, *, timeout: float) -> Any:
            return _endpoint()

        def close(self) -> None:
            return None

    class Runtime:
        def __init__(self, _client: Any, **kwargs: Any) -> None:
            seen.append(kwargs["permission_callback"])
            self._binding = kwargs["binding"]

        def start(self) -> None:
            return None

        def stop(self, *, timeout: float) -> None:
            return None

        def status(self) -> Any:
            return SimpleNamespace(healthy=True, failure_type=None)

    def permission_bridge(_request: Any) -> str | None:
        return None

    coordinator = AcpRuntimeCoordinator(
        config,
        threading.Event(),
        endpoint_client_factory=lambda _config: EndpointClient(),
        client_factory=lambda *_args, **_kwargs: object(),
        runtime_factory=Runtime,
        permission_callback=permission_bridge,
        require_permission_bridge=True,
        reconcile_interval=60.0,
    ).start()
    try:
        assert seen == [permission_bridge]
    finally:
        coordinator.stop()


@pytest.mark.parametrize("policy", ["acp_shadow", "acp_preferred"])
def test_acp_owned_worker_is_excluded_from_legacy_scheduler(
    tmp_path: Path,
    policy: str,
) -> None:
    config = _config(tmp_path, policy=policy)
    assert config.db_path is not None
    init_store(config.db_path)
    upsert_worker_bindings(config.db_path, [_binding()])
    read = threading.Event()

    def reader(*_args: Any, **_kwargs: Any) -> TurnRefreshResult:
        read.set()
        return TurnRefreshResult("updated", 1)

    scheduler = TurnIngestionScheduler(
        config,
        refresh_interval_seconds=0.05,
        max_workers=1,
        reader=reader,
    )
    scheduler.set_worker_exclusion(
        lambda worker_id, fingerprint: (
            worker_id == "worker-1" and fingerprint == "worker-fingerprint"
        )
    )
    scheduler.start()
    try:
        assert not read.wait(0.2)
    finally:
        scheduler.stop(flush_timeout_seconds=1.0)


def test_required_zero_workers_is_idle_healthy_then_new_unowned_worker_degrades(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, policy="acp_required")
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


def test_preferred_caches_exact_non_acp_generation_without_endpoint_churn(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, policy="acp_preferred")
    assert config.db_path is not None
    init_store(config.db_path)
    upsert_worker_bindings(config.db_path, [_binding()])
    endpoint_calls = 0
    status_calls = 0
    registered = False

    class NonAcpClient:
        def agent_acp_endpoint(self, _target: Any, *, timeout: float) -> Any:
            nonlocal endpoint_calls
            endpoint_calls += 1
            if registered:
                return _endpoint()
            raise HerdrErrorResponse(
                {
                    "code": "acp_worker_unauthenticated",
                    "message": "worker is not ACP-owned",
                },
                "request-private",
            )

        def agent_acp_status(self, _target: Any, *, timeout: float) -> Any:
            nonlocal status_calls
            status_calls += 1
            if registered:
                return _status(lifecycle="acp_owned_ready")
            raise HerdrErrorResponse(
                {
                    "code": "acp_worker_unauthenticated",
                    "message": "worker is not ACP-owned",
                },
                "request-private",
            )

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
        endpoint_client_factory=lambda _config: NonAcpClient(),
        client_factory=lambda *_args, **_kwargs: object(),
        runtime_factory=Runtime,
        reconcile_interval=60.0,
    ).start()
    try:
        assert endpoint_calls == 1
        assert coordinator.status()["healthy"] is True
        coordinator._reconcile(strict=False)
        coordinator._reconcile(strict=False)
        assert endpoint_calls == 1
        assert status_calls == 2
        assert coordinator.status()["failure_type"] is None

        registered = True
        coordinator._reconcile(strict=False)
        assert endpoint_calls == 2
        assert status_calls == 3
        assert "worker-1" in coordinator._slots
    finally:
        coordinator.stop()


def test_required_does_not_cache_non_acp_endpoint_failure(tmp_path: Path) -> None:
    config = _config(tmp_path, policy="acp_required")
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
