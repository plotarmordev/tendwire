"""Production Herdr-to-ACP worker discovery and runtime coordination.

Herdr remains the authority for live worker identity and mints one-shot attach
endpoints.  This module validates that private contract, supervises one ACP
runtime per worker generation, and exposes opaque prompt routes to the daemon.
No endpoint ticket, process argv, cwd, adapter identity, or ACP session ID is
part of the public health surface.
"""

from __future__ import annotations

import json
import math
import threading
import time
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from contextlib import contextmanager
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

from ..config import Config
from ..core.models import (
    BackendHealth,
    Space,
    WORKER_BINDING_ACTIVE_EXPIRES_AT,
    Worker,
    WorkerBinding,
    normalize_status,
    utc_timestamp,
    worker_binding_private_fingerprint,
)
from ..core.commands import turn_submission_id
from ..core.models import stable_fingerprint
from ..store.events import list_agent_events
from ..store.pending import pending_payload_from_store
from ..store.projection import (
    expire_worker_bindings,
    latest_snapshot,
    list_worker_bindings,
    save_snapshot,
    upsert_worker_bindings,
)
from ..core.projector import project_from_observations
from ..worker_identity import (
    STABLE_KEY_VERSION,
    canonical_herdr_pane_identity,
    load_or_create_installation_key,
    stable_worker_key,
)
from .acp_client import BoundedAcpConnection
from .acp_permissions import AcpPermissionBroker
from .acp_runtime import (
    AcpWorkerSession,
    AcpWorkerSessionStatus,
    RuntimeState,
    SessionOpenMode,
)
from .herdr_protocol import HerdrErrorResponse
from .herdr_socket import HerdrSocketClient


class AcpCoordinatorError(RuntimeError):
    """The private Herdr ACP endpoint contract or supervisor failed."""


class AcpConsoleInputGap(AcpCoordinatorError):
    """The bounded Herdr console queue lost unconsumed pane input."""

    def __init__(self, message: str, *, recovery_after_sequence: int) -> None:
        super().__init__(message)
        self.recovery_after_sequence = recovery_after_sequence


class AcpVisibleConsoleUnavailable(AcpCoordinatorError):
    """A worker cannot accept new prompts while its pane bridge is lost."""


def _ensure(valid: bool, message: str) -> None:
    if not valid:
        raise AcpCoordinatorError(message)


# Herdr's aggregate console queue admission is 768 KiB and its normal control
# frame is 2 MiB. Keep Tendwire's retained output contribution below 512 KiB
# so queued pane input and the echoed response retain explicit headroom.
_CONSOLE_OUTPUT_QUEUE_BUDGET_BYTES = 512 * 1024
_CONSOLE_OUTPUT_ITEM_TEXT_BYTES = 128 * 1024
# Three idle ACP panes previously produced roughly 30 control exchanges per
# second on the Raspberry Pi. A 500 ms cadence keeps pane input/output latency
# sub-second while leaving enough server capacity for delivery and health API
# traffic. Active prompts continue independently inside their ACP runtimes.
_CONSOLE_BRIDGE_INTERVAL_SECONDS = 0.5
_WORKER_IDENTITY_FIELDS = frozenset({
    "terminal_id", "workspace_id", "tab_id", "pane_id",
    "name", "agent", "generation",
})
_ENDPOINT_FIELDS = frozenset({
    "type", "endpoint", "console", "worker", "adapter", "session", "cwd", "lifecycle",
})
_STATUS_FIELDS = frozenset({
    "type", "worker", "adapter", "session", "cwd", "lifecycle", "console_lifecycle",
})
_CONSOLE_EXCHANGE_FIELDS = frozenset({
    "type", "inputs", "outputs", "input_floor_sequence", "output_floor_sequence",
    "next_input_sequence", "next_output_sequence",
})


@dataclass(frozen=True, slots=True)
class HerdrAcpEndpoint:
    command: tuple[str, ...]
    cwd: Path
    generation: str
    session_mode: SessionOpenMode
    session_id: str | None
    console: HerdrAcpConsoleEndpoint


@dataclass(frozen=True, slots=True)
class HerdrAcpConsoleEndpoint:
    generation: int
    lease: str


@dataclass(slots=True)
class _SessionSlot:
    continuity: WorkerBinding
    generation: str
    runtime: AcpWorkerSession
    console: HerdrAcpConsoleEndpoint
    console_executor: ThreadPoolExecutor
    permission_broker: AcpPermissionBroker | None = None
    console_input_sequence: int = 0
    console_event_sequence: int = 0
    # Unknown until one empty-output exchange observes Herdr's retained pane
    # queue for this freshly minted generation.
    console_retained_output_bytes: int | None = None
    console_local_turns: set[str] = field(default_factory=set)
    console_submissions: dict[int, Future[Any]] = field(default_factory=dict)
    console_failures: int = 0
    lock: threading.RLock = field(default_factory=threading.RLock, repr=False)
    retired: bool = False
    console_bridge_thread: threading.Thread | None = None


class _PromptRoute:
    def __init__(
        self,
        owner: "AcpSupervisor",
        worker: Worker,
        slot: _SessionSlot,
    ) -> None:
        self._owner = owner
        self._worker = worker
        self._slot = slot
        self._prepared = threading.local()

    @property
    def binding_fingerprint(self) -> str:
        with self._owner._reconcile_lock:
            self._owner._require_reconcile_state(allow_starting=False)
            if self._owner._current_slot(self._worker) is not self._slot:
                raise AcpCoordinatorError("ACP worker route is stale")
            return str(self._slot.runtime._binding.private_fingerprint)

    def prompt(
        self,
        text: str,
        *,
        producer_turn_id: str,
        timeout: float,
        on_send_start: Callable[[], None] | None = None,
    ) -> object:
        return self._submit(text, producer_turn_id, timeout, on_send_start, False)

    @property
    def supports_steering(self) -> bool:
        try:
            current = self._owner._current_slot(self._worker)
            return current is self._slot and self._slot.runtime.can_steer()
        except Exception:
            return False

    def steer(
        self,
        text: str,
        *,
        producer_turn_id: str,
        timeout: float,
        on_send_start: Callable[[], None] | None = None,
    ) -> object:
        return self._submit(text, producer_turn_id, timeout, on_send_start, True)

    def _submit(
        self,
        text: str,
        producer_turn_id: str,
        timeout: float,
        on_send_start: Callable[[], None] | None,
        steering: bool,
    ) -> object:
        return self._owner._submit_route(
            self._worker,
            self._slot,
            text,
            producer_turn_id=producer_turn_id,
            acknowledgement_timeout=timeout,
            on_send_start=on_send_start,
            generation_prepared=bool(getattr(self._prepared, "depth", 0)),
            steering=steering,
        )

    @contextmanager
    def prepare(self):
        """Fence one exact generation before its durable send receipt exists."""

        with self._owner._reconcile_lock:
            self._owner._require_reconcile_state(allow_starting=False)
            if self._owner._current_slot(self._worker) is not self._slot:
                raise AcpCoordinatorError("ACP worker route is stale")
            self._owner._require_attached_generation(self._slot)
            if self._owner._current_slot(self._worker) is not self._slot:
                raise AcpCoordinatorError("ACP worker route is stale")
            depth = int(getattr(self._prepared, "depth", 0))
            self._prepared.depth = depth + 1
            try:
                yield self
            finally:
                self._prepared.depth = depth


EndpointClientFactory = Callable[[Config], Any]
WorkerSessionFactory = Callable[..., AcpWorkerSession]
ConnectionFactory = Callable[..., BoundedAcpConnection]


class AcpSupervisor:
    """Reconcile Herdr endpoint ownership into per-worker ACP sessions."""

    def __init__(
        self,
        config: Config,
        stop_event: threading.Event,
        *,
        endpoint_client_factory: EndpointClientFactory | None = None,
        discovery_client_factory: EndpointClientFactory | None = None,
        session_factory: WorkerSessionFactory = AcpWorkerSession,
        connection_factory: ConnectionFactory = BoundedAcpConnection,
        reconcile_interval: float | None = None,
    ) -> None:
        self.config = config
        self._daemon_stop = stop_event
        self._endpoint_client_factory = (
            endpoint_client_factory or _default_endpoint_client_factory
        )
        self._discovery_client_factory = discovery_client_factory
        if self._discovery_client_factory is None and endpoint_client_factory is None:
            self._discovery_client_factory = _default_endpoint_client_factory
        self._session_factory = session_factory
        self._connection_factory = connection_factory
        interval = (
            config.reconcile_interval_seconds
            if reconcile_interval is None
            else reconcile_interval
        )
        self._reconcile_interval = float(interval)
        if not math.isfinite(self._reconcile_interval) or self._reconcile_interval <= 0:
            raise ValueError("reconcile_interval must be finite and positive")
        self._lock = threading.RLock()
        # Endpoint minting, runtime publication, prompt lease validation, and
        # shutdown are one private generation transaction. Herdr
        # tickets are one-shot, so overlapping reconciles cannot be repaired
        # after the fact by selecting whichever runtime happened to attach.
        self._reconcile_lock = threading.RLock()
        self._stop = threading.Event()
        self._slots: dict[str, _SessionSlot] = {}
        # Retirement removes routing authority immediately, but shutdown must
        # still account for bridge and executor work that already started.
        self._retired_slots: dict[int, _SessionSlot] = {}
        self._pending_binding_releases: dict[tuple[str, str, str], WorkerBinding] = {}
        self._thread: threading.Thread | None = None
        self._state = RuntimeState.NEW
        self._failure_type: str | None = None
        self._required_degraded = False
        self._console_failure_type: str | None = None
        self._console_failed_claims: dict[str, str] = {}
        self._last_discovery_at: str | None = None
        self._worker_count = 0

    def start(self) -> "AcpSupervisor":
        with self._lock:
            if self._state is RuntimeState.RUNNING:
                return self
            if self._state is not RuntimeState.NEW:
                raise AcpCoordinatorError("ACP coordinator cannot be restarted")
            self._state = RuntimeState.STARTING
        try:
            # ACP adapter processes cannot survive this coordinator process.
            # Revoke any process-owned rows left by an unclean prior exit before
            # a fresh Herdr generation is allowed to attach.
            self._expire_orphaned_bindings()
            self._reconcile(strict=True)
        except Exception as exc:
            with self._lock:
                self._state = RuntimeState.FAILED
                self._failure_type = type(exc).__name__
            self._stop_all()
            raise
        with self._lock:
            if self._state is not RuntimeState.STARTING or self._stop.is_set():
                self._state = RuntimeState.STOPPED
                raise AcpCoordinatorError("ACP coordinator is stopping")
            self._state = RuntimeState.RUNNING
            thread = threading.Thread(
                target=self._run,
                name="tendwire-acp-coordinator",
                daemon=True,
            )
            self._thread = thread
            thread.start()
        return self

    def stop(self, *, timeout: float | None = None) -> None:
        limit = (
            self.config.acp_shutdown_timeout_seconds
            if timeout is None
            else float(timeout)
        )
        if limit <= 0:
            raise ValueError("stop timeout must be positive")
        with self._lock:
            if self._state is RuntimeState.STOPPED:
                return
            self._state = RuntimeState.STOPPING
            slots = tuple(self._slots.values())
        self._stop.set()
        # A durable permission answer keeps the generation fence until the
        # complete JSON-RPC response frame is written.  Wake any broker waiters
        # before waiting for that fence: otherwise a slow/stuck adapter write
        # can hold ``_reconcile_lock`` for the request timeout (normally much
        # longer than the coordinator's bounded shutdown deadline).  Closing a
        # broker is fail-closed; an answer that has not observed a complete
        # frame becomes uncertain and releases the fence without selecting a
        # second route.
        for slot in slots:
            if slot.permission_broker is not None:
                slot.permission_broker.close()
        deadline = time.monotonic() + limit
        # Wait for endpoint mint/start or prompt lease validation to leave its
        # critical section before clearing slots. A reconcile that observed
        # STOPPING stops its provisional runtime instead of publishing it.
        acquired = self._reconcile_lock.acquire(timeout=limit)
        if not acquired:
            with self._lock:
                self._failure_type = "AcpCoordinatorError"
            raise AcpCoordinatorError(
                "ACP coordinator reconciliation did not stop within the deadline"
            )
        try:
            self._stop_all(timeout=max(0.001, deadline - time.monotonic()))
            try:
                self._retry_binding_releases()
            except Exception as exc:
                with self._lock:
                    self._failure_type = type(exc).__name__
        finally:
            self._reconcile_lock.release()
        with self._lock:
            slots = tuple(self._retired_slots.values())
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=max(0.0, deadline - time.monotonic()))
        unfinished = bool(
            (thread is not None and thread.is_alive())
            or self._pending_binding_releases
        )
        for slot in slots:
            with slot.lock:
                bridge_thread = slot.console_bridge_thread
                futures = tuple(slot.console_submissions.values())
                executor = slot.console_executor
            if bridge_thread is not None and bridge_thread is not threading.current_thread():
                bridge_thread.join(timeout=max(0.0, deadline - time.monotonic()))
                unfinished = unfinished or bridge_thread.is_alive()
            slot_unfinished = False
            for future in futures:
                if future.done():
                    continue
                try:
                    future.result(timeout=max(0.0, deadline - time.monotonic()))
                except Exception:
                    pass
                slot_unfinished = slot_unfinished or not future.done()
            unfinished = unfinished or slot_unfinished
            if not slot_unfinished:
                # The executor was already asked to shut down by retirement;
                # once every task is terminal this join is immediate and
                # ensures no non-daemon worker survives a successful stop.
                executor.shutdown(wait=True, cancel_futures=True)
        with self._lock:
            if unfinished:
                self._state = RuntimeState.FAILED
                self._failure_type = "AcpRuntimeStopTimeout"
            elif self._state is not RuntimeState.FAILED:
                self._state = RuntimeState.STOPPED

    def status(self) -> dict[str, Any]:
        counters = dict.fromkeys(AcpWorkerSessionStatus.counter_names(), 0)
        with self._lock:
            state = self._state
            slots = tuple(self._slots.values())
            failure_type = self._failure_type
            required_degraded = self._required_degraded
            console_degraded = bool(self._console_failed_claims)
            console_failure_type = self._console_failure_type
        failure_type = failure_type or console_failure_type
        healthy = (
            state is RuntimeState.RUNNING
            and not required_degraded
            and not console_degraded
        )
        for slot in slots:
            try:
                status = slot.runtime.status()
            except Exception as exc:  # noqa: BLE001
                healthy = False
                failure_type = failure_type or type(exc).__name__
                continue
            healthy = healthy and status.healthy
            for field in counters:
                counters[field] += max(0, int(getattr(status, field, 0)))
            failure_type = failure_type or status.failure_type
        return {
            "state": state.value,
            "healthy": healthy,
            "failure_type": failure_type,
            "last_reconcile_at": self._last_discovery_at,
            "worker_count": self._worker_count,
            **counters,
        }

    def prompt_route(self, worker: Worker) -> _PromptRoute | None:
        try:
            slot = self._current_slot(worker)
        except AcpVisibleConsoleUnavailable:
            # Console loss is a deliberate hard fence, not a stale runtime
            # that reconciliation should remint synchronously on this command.
            return None
        except AcpCoordinatorError:
            # A just-observed worker may not have reached the periodic pass.
            try:
                self._reconcile_worker(worker.id, strict=False)
                slot = self._current_slot(worker)
            except AcpVisibleConsoleUnavailable:
                return None
            except Exception:  # noqa: BLE001
                return None
        return _PromptRoute(self, worker, slot)

    def owns_permission_decision(self, decision: Any) -> bool:
        """Return whether one pending decision belongs to an exact live slot."""
        try:
            self._permission_route(decision)
            return True
        except Exception:
            return False

    def answer_permission_decision(self, decision: Any, *, timeout: float) -> None:
        """Select an offered option and wait for its response-frame write."""
        with self._reconcile_lock:
            self._require_reconcile_state(allow_starting=False)
            broker = self._permission_route(decision)
            # Keep retirement fenced until respond_permission has acknowledged
            # writing the complete JSON-RPC response frame.
            broker.answer(decision, timeout=timeout)

    def _permission_route(self, decision: Any) -> AcpPermissionBroker:
        worker_id = str(getattr(decision, "worker_id", "") or "")
        with self._lock:
            slot = self._slots.get(worker_id)
        if slot is None or slot.permission_broker is None:
            raise AcpCoordinatorError("ACP permission route is unavailable")
        self._require_attached_generation(slot)
        if not slot.permission_broker.owns(decision):
            raise AcpCoordinatorError("ACP permission authority changed")
        return slot.permission_broker

    def _current_slot(self, worker: Worker) -> _SessionSlot:
        with self._lock:
            if self._state is not RuntimeState.RUNNING:
                raise AcpCoordinatorError("ACP coordinator is not running")
            slot = self._slots.get(worker.id)
            console_failed = worker.id in self._console_failed_claims
        if slot is None:
            raise AcpCoordinatorError("ACP worker route is unavailable")
        if console_failed:
            # The visible pane is part of the required transport, not an
            # optional observer.  Fence every newly resolved route (and every
            # use of a route resolved before the failure) on the first bridge
            # loss.  The in-flight ACP turn itself may continue to drain, but
            # pane and Telegram callers cannot start another headless turn.
            raise AcpVisibleConsoleUnavailable(
                "ACP worker visible console is unavailable"
            )
        if slot.continuity.worker_fingerprint != worker.fingerprint:
            raise AcpCoordinatorError("ACP worker authority is stale")
        if not slot.runtime.status().healthy:
            raise AcpCoordinatorError("ACP worker runtime is unhealthy")
        return slot

    def _run(self) -> None:
        next_reconcile = time.monotonic() + self._reconcile_interval
        while not self._stop.wait(_CONSOLE_BRIDGE_INTERVAL_SECONDS):
            if self._daemon_stop.is_set():
                return
            self._bridge_console_slots()
            if time.monotonic() < next_reconcile:
                continue
            try:
                self._reconcile(strict=False)
            except Exception as exc:  # noqa: BLE001
                with self._lock:
                    self._failure_type = type(exc).__name__
            next_reconcile = time.monotonic() + self._reconcile_interval

    def _bridge_console_slots(self) -> None:
        """Dispatch independent visible pane bridges without head-of-line blocking."""
        self._reap_retired_slots()
        with self._lock:
            slots = tuple(self._slots.values())
        for slot in slots:
            with slot.lock:
                if slot.retired:
                    continue
                thread = slot.console_bridge_thread
                if thread is not None and thread.is_alive():
                    continue
                thread = threading.Thread(
                    target=self._bridge_console_slot_supervised,
                    args=(slot,),
                    name=f"tendwire-acp-console-bridge-{slot.continuity.worker_id}",
                    daemon=True,
                )
                slot.console_bridge_thread = thread
                thread.start()

    def _reap_retired_slots(self) -> None:
        """Release terminal retired work without losing shutdown accounting."""

        with self._lock:
            retired = tuple(self._retired_slots.items())
        for slot_id, slot in retired:
            with slot.lock:
                bridge = slot.console_bridge_thread
                futures = tuple(slot.console_submissions.values())
                executor = slot.console_executor
            if (bridge is not None and bridge.is_alive()) or any(
                not future.done() for future in futures
            ):
                continue
            executor.shutdown(wait=True, cancel_futures=True)
            with self._lock:
                if self._retired_slots.get(slot_id) is slot:
                    self._retired_slots.pop(slot_id, None)

    def _bridge_console_slot_supervised(self, slot: _SessionSlot) -> None:
        worker_id = slot.continuity.worker_id
        try:
            self._bridge_console_slot(slot)
            with slot.lock:
                if slot.retired:
                    return
                slot.console_failures = 0
            with self._lock:
                # A superseded bridge must not clear the fence for its
                # replacement.  Recovery requires a successful pass by the
                # exact slot currently published for this worker.
                if self._slots.get(worker_id) is not slot:
                    return
                self._console_failed_claims.pop(worker_id, None)
                if not self._console_failed_claims:
                    self._console_failure_type = None
        except Exception as exc:
            with slot.lock:
                if slot.retired:
                    return
                slot.console_failures += 1
                failure_count = slot.console_failures
            # Console availability is supervised independently of the ACP
            # runtime. A later pass replays inputs and idempotent outputs.
            with self._lock:
                self._console_failed_claims[worker_id] = (
                    slot.continuity.worker_fingerprint
                )
                self._console_failure_type = type(exc).__name__
            if failure_count >= 3:
                try:
                    with self._reconcile_lock:
                        self._require_reconcile_state(allow_starting=False)
                        self._retire_worker(worker_id, expected=slot)
                        self._reconcile_worker(worker_id, strict=True)
                except Exception:
                    # Keep the worker in the persistent failure set until a
                    # replacement slot completes a successful console pass.
                    pass

    def _bridge_console_slot(self, slot: _SessionSlot) -> None:
        with slot.lock:
            if slot.retired:
                return
            console = slot.console
        binding = getattr(slot.runtime, "_binding", None)
        if not isinstance(binding, WorkerBinding):
            raise AcpCoordinatorError("ACP console session binding is unavailable")
        with slot.lock:
            event_sequence = slot.console_event_sequence
            input_sequence = slot.console_input_sequence
            retained_output_bytes = slot.console_retained_output_bytes
            submissions = slot.console_submissions
        events = list_agent_events(
            Path(self.config.db_path),
            self.config.host_id,
            worker_id=slot.continuity.worker_id,
            source="acp",
            session_id=binding.turn_target_value,
            after_sequence=event_sequence,
            limit=100,
        )
        # Snapshot suppression after the event page. A console submission
        # registers its deterministic turn id before it can emit the ACP user
        # event, so this ordering closes the race where the bridge could copy
        # an old set and then observe the newly emitted event in the same page.
        with slot.lock:
            if slot.retired:
                return
            local_turns = set(slot.console_local_turns)
        output_budget = max(
            0,
            _CONSOLE_OUTPUT_QUEUE_BUDGET_BYTES - (retained_output_bytes or 0),
        )
        output: list[dict[str, str]] = []
        completed_submissions: list[int] = []
        for sequence, future in tuple(submissions.items()):
            if not future.done():
                continue
            item = _console_submission_output(slot.generation, sequence, future)
            if item is not None and not _append_console_output(
                output, item, output_budget
            ):
                continue
            completed_submissions.append(sequence)
            input_sequence = max(input_sequence, sequence)
        item = _console_permission_output(self.config, slot)
        if item is not None:
            _append_console_output(output, item, output_budget)
        processed_event_sequence, consumed_local_turns = _append_console_events(
            output, events, local_turns, output_budget, event_sequence)
        probing_retained_output = retained_output_bytes is None
        client = self._endpoint_client_factory(self.config)
        inputs: tuple[tuple[int, str], ...] = ()
        try:
            result = client.agent_acp_console_exchange(
                slot.continuity.target_value,
                generation=console.generation,
                lease=console.lease,
                after_input_sequence=input_sequence,
                output=[] if probing_retained_output else output,
                timeout=self.config.herdr_timeout_seconds,
            )
            retained_output_bytes = _console_output_wire_bytes(result["outputs"])
            inputs = _parse_console_exchange(result, input_sequence)
        except AcpConsoleInputGap as exc:
            # Historical pane input is out of scope. Move to Herdr's retained
            # tail; the next bridge cadence begins with fresh input.
            input_sequence = max(input_sequence, exc.recovery_after_sequence)
            processed_event_sequence = event_sequence
            consumed_local_turns.clear()
        finally:
            client.close()
        if probing_retained_output:
            # The first pass is an empty-output probe; no locally rendered
            # event or submission outcome was accepted by Herdr yet.
            processed_event_sequence = event_sequence
            completed_submissions.clear()
            consumed_local_turns.clear()
        with slot.lock:
            if slot.retired:
                return
            slot.console_input_sequence = input_sequence
            slot.console_retained_output_bytes = retained_output_bytes
            for sequence in completed_submissions:
                submissions.pop(sequence, None)
        if processed_event_sequence > event_sequence:
            with slot.lock:
                if slot.retired:
                    return
                slot.console_event_sequence = processed_event_sequence
                slot.console_local_turns.difference_update(consumed_local_turns)
        for sequence, text in inputs[:1]:
            with slot.lock:
                if slot.retired or sequence in submissions:
                    continue
                executor = slot.console_executor
                submissions[sequence] = executor.submit(
                    self._submit_console_input, slot, sequence, text
                )

    def _submit_console_input(
        self, slot: _SessionSlot, sequence: int, text: str
    ) -> str:
        with self._reconcile_lock:
            self._require_reconcile_state(allow_starting=False)
            with self._lock:
                current = self._slots.get(slot.continuity.worker_id)
            with slot.lock:
                retired = slot.retired
            if current is not slot or retired:
                raise AcpCoordinatorError("ACP console input generation is stale")
            return self._submit_console_input_fenced(slot, sequence, text)

    def _submit_console_input_fenced(
        self, slot: _SessionSlot, sequence: int, text: str
    ) -> str:
        snapshot = latest_snapshot(Path(self.config.db_path), self.config.host_id)
        workers = snapshot.workers if snapshot is not None else ()
        worker = next((item for item in workers if (
            item.id == slot.continuity.worker_id
            and item.fingerprint == slot.continuity.worker_fingerprint)), None)
        stable_key = worker.meta.get("stable_key") if worker is not None else None
        stable_version = worker.meta.get("stable_key_version") if worker else None
        if not isinstance(stable_key, str) or stable_version != 1:
            raise AcpCoordinatorError("ACP console stable worker identity is unavailable")
        request_id = "acpc." + stable_fingerprint({
            "generation": slot.generation, "input_sequence": sequence,
            "worker_id": slot.continuity.worker_id,
        })
        if text.strip() == "/cancel":
            slot.runtime.cancel()
            return "cancelled"
        request: dict[str, Any] = {
            "schema_version": 1,
            "action": "send_instruction",
            "request_id": request_id,
            "target": {
                "stable_key": stable_key,
                "stable_key_version": 1,
            },
            "instruction": {"text": text},
        }
        pending = _console_pending_decision(
            Path(self.config.db_path),
            self.config.host_id,
            slot.continuity.worker_id,
        )
        if pending is not None:
            decision_ref, options, _prompt = pending
            selected = _console_permission_selection(text, options)
            if selected is None:
                raise AcpCoordinatorError(
                    "console input is not a valid permission selection"
                )
            request.pop("instruction", None)
            request["action"] = "answer_decision"
            request["target"] = {"worker_id": slot.continuity.worker_id}
            request["params"] = {
                "decision_ref": decision_ref,
                "selection": {"option_refs": [selected]},
            }
        from ..command_submission import submit_command

        local_turn_id: str | None = None
        if request["action"] == "send_instruction":
            submission_id = turn_submission_id(self.config.host_id, request_id)
            session_id = str(getattr(slot.runtime, "_session_id", "") or "")
            if session_id:
                local_turn_id = "acpt_" + stable_fingerprint(
                    {
                        "source": "acp",
                        "session": session_id,
                        "producer_turn": submission_id,
                    }
                )
                # Register before submit_command can emit the ACP user event;
                # the independent bridge thread may observe it immediately.
                with slot.lock:
                    if slot.retired:
                        raise AcpCoordinatorError(
                            "ACP console input generation is stale"
                        )
                    slot.console_local_turns.add(local_turn_id)
        try:
            result = submit_command(
                self.config,
                json.dumps(request, sort_keys=True, separators=(",", ":")),
                acp_prompt_router=self.prompt_route,
                acp_permission_router=self,
            )
        except Exception:
            _discard_local_turn(slot, local_turn_id)
            raise
        if result.status not in {"accepted", "duplicate_request"}:
            _discard_local_turn(slot, local_turn_id)
            raise AcpCoordinatorError("ACP console input was not accepted")
        if request["action"] == "answer_decision":
            return "permission"
        if result.status == "duplicate_request" and local_turn_id is not None:
            # A duplicate receipt does not start a new ACP turn, so there is no
            # corresponding user event left to suppress in this process.
            _discard_local_turn(slot, local_turn_id)
        return "instruction"

    def _require_reconcile_state(self, *, allow_starting: bool) -> None:
        with self._lock:
            allowed = {RuntimeState.RUNNING}
            if allow_starting:
                allowed.add(RuntimeState.STARTING)
            if self._state not in allowed or self._stop.is_set():
                raise AcpCoordinatorError("ACP coordinator is stopping")

    def _continuity_bindings(self) -> tuple[dict[str, WorkerBinding], int]:
        bindings = list_worker_bindings(
            Path(self.config.db_path),
            self.config.host_id,
            backend="herdr",
        )
        grouped: dict[str, list[WorkerBinding]] = {}
        for binding in bindings:
            if binding.sendable and binding.target_kind in {
                "terminal_id",
                "pane_id",
            }:
                grouped.setdefault(binding.worker_id, []).append(binding)
        current = {
            worker_id: rows[0]
            for worker_id, rows in grouped.items()
            if len(rows) == 1
        }
        ambiguities = sum(1 for rows in grouped.values() if len(rows) != 1)
        return current, ambiguities

    def _herdr_authority_claims(self) -> set[tuple[str, str]]:
        """Return exact sendable identities without requiring unique routing."""

        return {
            (binding.worker_id, binding.worker_fingerprint)
            for binding in list_worker_bindings(
                Path(self.config.db_path),
                self.config.host_id,
                backend="herdr",
            )
            if binding.sendable
        }

    def _reconcile(self, *, strict: bool) -> None:
        with self._reconcile_lock:
            try:
                self._require_reconcile_state(allow_starting=True)
                self._reconcile_locked(strict=strict)
            finally:
                if self._stop.is_set():
                    self._stop_all()

    def _reconcile_locked(self, *, strict: bool) -> None:
        try:
            self._retry_binding_releases()
        except Exception as exc:
            with self._lock:
                self._failure_type = type(exc).__name__
            if strict:
                raise
        discovery_omissions = 0
        if self._discovery_client_factory is not None:
            try:
                discovery_omissions = self._discover_continuity()
            except Exception as exc:
                with self._lock:
                    self._required_degraded = True
                    self._failure_type = type(exc).__name__
                if strict:
                    raise
                return
        current, ambiguities = self._continuity_bindings()
        with self._lock:
            failed_claims = tuple(self._console_failed_claims.items())
        exact_authorities = (
            self._herdr_authority_claims()
            if failed_claims
            else set()
        )
        with self._lock:
            stale = [worker_id for worker_id in self._slots if worker_id not in current]
            disappeared_failures = [
                worker_id
                for worker_id, fingerprint in failed_claims
                if (worker_id, fingerprint) not in exact_authorities
                and worker_id not in self._slots
                and self._console_failed_claims.get(worker_id) == fingerprint
            ]
            for worker_id in disappeared_failures:
                self._console_failed_claims.pop(worker_id, None)
            if not self._console_failed_claims:
                self._console_failure_type = None
        for worker_id in stale:
            self._retire_worker(worker_id)
        failure_type = (
            "AcpCoordinatorError"
            if ambiguities or discovery_omissions
            else None
        )
        for worker_id, continuity in current.items():
            self._require_reconcile_state(allow_starting=True)
            try:
                self._reconcile_binding(continuity)
            except Exception as exc:  # noqa: BLE001
                failure_type = failure_type or type(exc).__name__
                self._retire_worker(worker_id)
        with self._lock:
            self._required_degraded = failure_type is not None
            self._failure_type = failure_type
        if strict and failure_type is not None:
            raise AcpCoordinatorError("one or more ACP workers failed to attach")

    def _discover_continuity(self) -> int:
        """Refresh the one Herdr lifecycle projection consumed by ACP routing."""

        client = self._discovery_client_factory(self.config)
        try:
            workspaces = client.workspace_list(timeout=self.config.herdr_timeout_seconds)
            panes = client.pane_list(timeout=self.config.herdr_timeout_seconds)
            agents = client.agent_list(timeout=self.config.herdr_timeout_seconds)
        finally:
            client.close()

        observed_at = utc_timestamp()
        spaces = _discovered_spaces(workspaces)
        workers, bindings, omissions = _discovered_workers(
            self.config,
            panes,
            agents,
            observed_at,
        )
        health = BackendHealth(
            name="herdr",
            status="degraded" if omissions else "healthy",
            outcome=(
                "continuity_unavailable"
                if omissions
                else "healthy_non_empty" if spaces or workers else "empty_healthy"
            ),
            observed_at=observed_at,
            message=(f"{omissions} Herdr panes omitted" if omissions else ""),
            counts={"spaces": len(spaces), "workers": len(workers)},
        )
        snapshot = project_from_observations(
            self.config,
            spaces=spaces,
            workers=workers,
            backend_health=[health],
        )
        from ..store.projection import SnapshotObservationContext

        save_snapshot(
            Path(self.config.db_path),
            snapshot,
            observation=SnapshotObservationContext(
                authority="complete",
                observed_at=observed_at,
            ),
            worker_bindings=bindings,
            binding_backend="herdr",
            binding_observation_authoritative=True,
            binding_workers_present=bool(workers),
        )
        with self._lock:
            self._last_discovery_at = observed_at
            self._worker_count = len(workers)
        return omissions

    def _reconcile_worker(self, worker_id: str, *, strict: bool) -> None:
        with self._reconcile_lock:
            try:
                self._require_reconcile_state(allow_starting=False)
                current, _ambiguities = self._continuity_bindings()
                continuity = current.get(worker_id)
                if continuity is None:
                    with self._lock:
                        fingerprint = self._console_failed_claims.get(worker_id)
                        slot_exists = worker_id in self._slots
                    disappeared = False
                    if fingerprint is not None and not slot_exists:
                        try:
                            disappeared = (
                                worker_id, fingerprint) not in self._herdr_authority_claims()
                        except Exception:
                            # A failed ownership check cannot prove that the
                            # Herdr endpoint disappeared; retry periodically.
                            pass
                    if disappeared:
                        with self._lock:
                            self._console_failed_claims.pop(worker_id, None)
                            if not self._console_failed_claims:
                                self._console_failure_type = None
                    if strict:
                        raise AcpCoordinatorError(
                            "worker has no unique Herdr authority"
                        )
                    return
                self._reconcile_binding(continuity)
            finally:
                if self._stop.is_set():
                    self._stop_all()

    def _reconcile_binding(self, continuity: WorkerBinding) -> None:
        with self._lock:
            existing = self._slots.get(continuity.worker_id)
        if (
            existing is not None
            and _same_continuity(existing.continuity, continuity)
            and existing.runtime.status().healthy
        ):
            try:
                self._require_attached_generation(existing)
                return
            except Exception:
                self._retire_worker(continuity.worker_id, expected=existing)
                existing = None
        if existing is not None:
            self._retire_worker(continuity.worker_id, expected=existing)
        endpoint = _parse_endpoint(
            self.config,
            continuity,
            self._herdr_request("agent_acp_endpoint", continuity.target_value),
        )
        runtime, permission_broker, owned_bindings = self._build_runtime(
            continuity,
            endpoint,
        )
        console_executor: ThreadPoolExecutor | None = None
        try:
            runtime.start()
            self._require_reconcile_state(allow_starting=True)
            runtime_binding = getattr(runtime, "_binding", None)
            console_cursor = 0
            if (
                isinstance(runtime_binding, WorkerBinding)
                and runtime_binding.turn_target_value
            ):
                console_cursor = _latest_console_event_sequence(
                    Path(self.config.db_path),
                    self.config.host_id,
                    continuity.worker_id,
                    runtime_binding.turn_target_value,
                )
            console_executor = ThreadPoolExecutor(
                max_workers=1,
                thread_name_prefix=(
                    f"tendwire-acp-console-{continuity.worker_id}"
                ),
            )
            slot = _SessionSlot(
                continuity,
                endpoint.generation,
                runtime,
                console=endpoint.console,
                console_executor=console_executor,
                permission_broker=permission_broker,
                console_event_sequence=console_cursor,
            )
            with self._lock:
                self._slots[continuity.worker_id] = slot
        except Exception:
            if console_executor is not None:
                console_executor.shutdown(wait=False, cancel_futures=True)
            permission_broker.close()
            owned = owned_bindings[0] if owned_bindings else None
            self._stop_runtime(runtime, owned_binding=owned)
            raise

    def _herdr_request(self, method: str, target: str) -> Any:
        client = self._endpoint_client_factory(self.config)
        try:
            return getattr(client, method)(
                target, timeout=self.config.herdr_timeout_seconds)
        finally:
            client.close()

    def _require_attached_generation(self, slot: _SessionSlot) -> None:
        try:
            status = _parse_status(
                slot.continuity,
                self._herdr_request(
                    "agent_acp_status", slot.continuity.target_value
                ),
            )
        except Exception:
            self._retire_worker(slot.continuity.worker_id, expected=slot)
            raise
        if status != (slot.generation, "acp_owned_attached", "attached"):
            self._retire_worker(slot.continuity.worker_id, expected=slot)
            raise AcpCoordinatorError("ACP worker generation lease is not current")

    def _submit_route(
        self,
        worker: Worker,
        slot: _SessionSlot,
        text: str,
        *,
        producer_turn_id: str,
        acknowledgement_timeout: float,
        on_send_start: Callable[[], None] | None,
        generation_prepared: bool,
        steering: bool,
    ) -> object:
        """Fence and submit one prompt operation on an exact generation."""

        with self._reconcile_lock:
            self._require_reconcile_state(allow_starting=False)
            if self._current_slot(worker) is not slot:
                raise AcpCoordinatorError("ACP worker route is stale")
            if not generation_prepared:
                self._require_attached_generation(slot)
            if self._current_slot(worker) is not slot:
                raise AcpCoordinatorError("ACP worker route is stale")
            if steering and not slot.runtime.can_steer():
                raise AcpCoordinatorError("ACP steering route is unavailable")
            submit = (
                slot.runtime.submit_steering
                if steering
                else slot.runtime.submit_prompt
            )
            # Hold the route lease through the complete request-frame write;
            # end-of-turn completion remains supervised by the runtime.
            return submit(
                text,
                producer_turn_id=producer_turn_id,
                acknowledgement_timeout=acknowledgement_timeout,
                on_send_start=on_send_start,
            )

    def _build_runtime(
        self,
        continuity: WorkerBinding,
        endpoint: HerdrAcpEndpoint,
    ) -> tuple[
        AcpWorkerSession,
        AcpPermissionBroker,
        list[WorkerBinding],
    ]:
        client = self._connection_factory(
            endpoint.command,
            cwd=endpoint.cwd,
            request_timeout=self.config.acp_request_timeout_seconds,
            prompt_timeout=float(self.config.submission_hard_ttl_seconds),
            close_timeout=self.config.acp_shutdown_timeout_seconds,
            max_frame_bytes=self.config.acp_max_frame_bytes,
        )
        owned_bindings: list[WorkerBinding] = []
        if endpoint.session_mode is SessionOpenMode.NEW:
            binding = continuity

            def callback(
                session_id: str,
                anchor: WorkerBinding,
            ) -> WorkerBinding:
                bound = _derived_binding(anchor, session_id)
                upsert_worker_bindings(Path(self.config.db_path), [bound])
                owned_bindings[:] = [bound]
                return bound
        else:
            assert endpoint.session_id is not None
            binding = _derived_binding(continuity, endpoint.session_id)
            upsert_worker_bindings(Path(self.config.db_path), [binding])
            owned_bindings.append(binding)
            callback = None
        permission_broker = AcpPermissionBroker(
            self.config,
            worker_id=continuity.worker_id,
            worker_fingerprint=continuity.worker_fingerprint,
            generation=endpoint.generation,
            timeout=float(self.config.submission_hard_ttl_seconds),
        )
        try:
            runtime = self._session_factory(
                client,
                config=self.config,
                binding=binding,
                cwd=endpoint.cwd,
                session_mode=endpoint.session_mode,
                session_id=endpoint.session_id,
                # Herdr's generation authenticates the worker lease and can
                # remain stable across several freshly minted adapter
                # transports. AcpWorkerSession deliberately creates a new stream
                # nonce when this argument is omitted; reusing the Herdr
                # generation would make synthetic notification identities
                # collide after a Tendwire restart.
                session_binding_callback=callback,
                permission_callback=permission_broker,
                poll_timeout=min(0.25, self.config.acp_request_timeout_seconds),
                stop_timeout=self.config.acp_shutdown_timeout_seconds,
            )
            return runtime, permission_broker, owned_bindings
        except Exception:
            permission_broker.close()
            try:
                client.close()
            except Exception:
                pass
            if owned_bindings:
                try:
                    self._retire_binding(owned_bindings[0])
                except Exception:
                    pass
            raise

    def _retire_worker(
        self,
        worker_id: str,
        *,
        expected: _SessionSlot | None = None,
        timeout: float | None = None,
    ) -> None:
        with self._reconcile_lock:
            with self._lock:
                slot = self._slots.get(worker_id)
                if slot is None or (expected is not None and slot is not expected):
                    return
                self._slots.pop(worker_id, None)
                self._retired_slots[id(slot)] = slot
                # A visible-console failure is an exact sticky ownership
                # claim. Retirement, ambiguity, and failed reminting must not
                # erase endpoint ownership while Herdr still publishes that
                # identity. Only a successful current console pass or the
                # positive-disappearance path in reconciliation may remove it.
                if not self._console_failed_claims:
                    self._console_failure_type = None
            self._retire_slot(slot, timeout=timeout)

    def _retire_slot(self, slot: _SessionSlot, *, timeout: float | None = None) -> None:
        """Fence one slot and retire all generation-owned resources."""

        with slot.lock:
            slot.retired = True
            executor = slot.console_executor
        if slot.permission_broker is not None:
            slot.permission_broker.close()
        executor.shutdown(wait=False, cancel_futures=True)
        self._stop_runtime(slot.runtime, timeout=timeout)

    def _stop_runtime(
        self,
        runtime: AcpWorkerSession,
        *,
        timeout: float | None = None,
        owned_binding: WorkerBinding | None = None,
    ) -> None:
        bindings: dict[tuple[str, str, str], WorkerBinding] = {}
        for candidate in (owned_binding, getattr(runtime, "_binding", None)):
            if isinstance(candidate, WorkerBinding) and candidate.backend == "acp":
                bindings[_binding_key(candidate)] = candidate
        try:
            runtime.stop(
                timeout=(
                    self.config.acp_shutdown_timeout_seconds
                    if timeout is None
                    else timeout
                )
            )
        except Exception:
            pass
        finally:
            for binding in bindings.values():
                try:
                    self._retire_binding(binding)
                except Exception:
                    pass

    def _retire_binding(self, binding: WorkerBinding) -> None:
        self._pending_binding_releases[_binding_key(binding)] = binding
        self._retry_binding_releases()

    def _retry_binding_releases(self) -> None:
        """Retry exact ACP lease retirement without losing cleanup handles."""

        for key, binding in tuple(self._pending_binding_releases.items()):
            _expire_derived_binding(
                self.config,
                binding,
                reason="acp_runtime_retired",
            )
            live = next((row for row in list_worker_bindings(
                Path(self.config.db_path), binding.host_id, backend="acp"
            ) if _binding_key(row) == key), None)
            if live is not None:
                self._pending_binding_releases[key] = live
                raise AcpCoordinatorError("ACP binding changed during retirement")
            self._pending_binding_releases.pop(key, None)

    def _expire_orphaned_bindings(self) -> None:
        """Revoke ACP leases that no runtime in this process can own."""

        expire_worker_bindings(
            Path(self.config.db_path),
        self.config.host_id,
        backend="acp",
        observed_before=WORKER_BINDING_ACTIVE_EXPIRES_AT,
        reason="acp_coordinator_restarted",
        )

    def _stop_all(self, *, timeout: float | None = None) -> None:
        with self._lock:
            worker_ids = tuple(self._slots)
        total = self.config.acp_shutdown_timeout_seconds if timeout is None else timeout
        deadline = time.monotonic() + total
        for worker_id in worker_ids:
            self._retire_worker(
                worker_id,
                timeout=max(0.001, deadline - time.monotonic()),
            )


def _default_endpoint_client_factory(config: Config) -> HerdrSocketClient:
    return HerdrSocketClient(timeout=config.herdr_timeout_seconds)


def _derived_binding(
    continuity: WorkerBinding,
    session_id: str,
) -> WorkerBinding:
    return replace(
        continuity,
        backend="acp",
        turn_target_kind="acp_session_id",
        turn_target_value=session_id,
        # This row is established by the current Tendwire process, not by the
        # Herdr observation that supplied continuity.  Reusing the observer's
        # timestamp can lose the upsert to a newer expired row left by the
        # previous process and make an otherwise valid restart fail closed.
        observed_at=utc_timestamp(),
        # The ACP runtime owns this private lease until explicit stop/failure.
        # Inheriting the observer's short Herdr lease would strand a healthy
        # attached runtime after the next observation-expiry boundary.
        expires_at=None,
        private_fingerprint="",
    )


def _expire_derived_binding(
    config: Config,
    binding: WorkerBinding,
    *,
    reason: str,
) -> None:
    expire_worker_bindings(
        Path(config.db_path),
        binding.host_id,
        backend="acp",
        worker_id=binding.worker_id,
        private_fingerprints=[binding.private_fingerprint],
        observed_before=binding.observed_at,
        reason=reason,
    )


def _binding_key(binding: WorkerBinding) -> tuple[str, str, str]:
    return (binding.worker_id, binding.worker_fingerprint, binding.private_fingerprint)


def _same_continuity(left: WorkerBinding, right: WorkerBinding) -> bool:
    """Compare authority identity while ignoring observation lease refreshes."""
    return replace(
        left, observed_at=right.observed_at, expires_at=right.expires_at) == right


def _text(item: Mapping[str, Any], *names: str) -> str | None:
    for name in names:
        value = item.get(name)
        if isinstance(value, (str, int)) and not isinstance(value, bool):
            text = str(value).strip()
            if text:
                return text
    return None


def _items(payload: Any, name: str) -> list[dict[str, Any]]:
    values = payload if isinstance(payload, list) else (
        payload.get(name, []) if isinstance(payload, Mapping) else [])
    if not isinstance(values, list):
        return []
    return [dict(value) for value in values if isinstance(value, Mapping)]


def _discovered_spaces(payload: Any) -> list[Space]:
    spaces = []
    for item in _items(payload, "workspaces"):
        if space_id := _text(item, "workspace_id", "space_id", "id", "name"):
            spaces.append(Space(
                id=space_id,
                name=_text(item, "label", "name", "title") or space_id,
                status=normalize_status(_text(item, "status", "state")),
                updated_at=_text(item, "updated_at", "observed_at"),
            ))
    return spaces


def _pane_match(
    agent: Mapping[str, Any],
    panes: Sequence[Mapping[str, Any]],
) -> tuple[Mapping[str, Any] | None, bool]:
    pane_id = _text(agent, "pane_id")
    terminal_id = _text(agent, "terminal_id")
    matches = [
        pane
        for pane in panes
        if (pane_id or terminal_id)
        and (not pane_id or _text(pane, "pane_id") == pane_id)
        and (not terminal_id or _text(pane, "terminal_id") == terminal_id)
    ]
    return (matches[0], False) if len(matches) == 1 else (None, len(matches) > 1)


def _materialize_discovered_workers(
    config: Config,
    rows: list[dict[str, Any]],
    observed_at: str,
) -> tuple[list[Worker], list[WorkerBinding]]:
    workers: list[Worker] = []
    bindings: list[WorkerBinding] = []
    for row in rows:
        worker_id = str(row["desired_id"])
        pane = row["pane"]
        agent = row["agent"]
        meta = {
            "stable_key": row["stable_key"],
            "stable_key_version": STABLE_KEY_VERSION,
        }
        label = _text(pane, "label")
        if label:
            meta["label"] = label
        worker = Worker(
            id=worker_id,
            name=(
                _text(agent, "agent", "name")
                or _text(pane, "agent", "label", "name")
                or worker_id
            ),
            status=normalize_status(
                _text(agent, "status", "agent_status")
                or _text(pane, "agent_status", "status")
            ),
            space_id=_text(pane, "workspace_id"),
            meta=meta,
            last_seen_at=observed_at,
            backend_target={
                "kind": row["target_kind"],
                "value": row["target_value"],
                "sendable": True,
                "reason": None,
            },
        )
        workers.append(worker)
        bindings.append(
            WorkerBinding(
                host_id=config.host_id,
                worker_id=worker.id,
                worker_fingerprint=worker.fingerprint,
                backend="herdr",
                target_kind=str(row["target_kind"]),
                target_value=str(row["target_value"]),
                sendable=True,
                reason=None,
                observed_at=observed_at,
                expires_at=None,
                private_fingerprint=str(row["private_fingerprint"]),
            )
        )
    return workers, bindings


def _discovered_workers(
    config: Config,
    pane_payload: Any,
    agent_payload: Any,
    observed_at: str,
) -> tuple[list[Worker], list[WorkerBinding], int]:
    panes = _items(pane_payload, "panes")
    # Herdr detects ordinary PTY-owned agents as codex/claude/kimi too, but
    # those rows deliberately have no authority name. Only a named agent is an
    # ACP ownership claim. Unnamed conventional panes remain wholly outside the
    # coordinator; malformed or unattached named claims still fail closed.
    agents = [
        agent
        for agent in _items(agent_payload, "agents")
        if _text(agent, "name") is not None
    ]
    rows: list[dict[str, Any]] = []
    candidate_count = len(agents)
    installation_key: bytes | None = None
    for agent in agents:
        pane, ambiguous_pane = _pane_match(agent, panes)
        if pane is None or ambiguous_pane:
            continue
        workspace_id = _text(pane, "workspace_id")
        pane_id = _text(pane, "pane_id")
        identity = canonical_herdr_pane_identity(workspace_id, pane_id)
        if identity is None:
            continue
        target_kind = ""
        target_value = ""
        for kind, value in (
            ("terminal_id", _text(pane, "terminal_id")),
            ("pane_id", pane_id),
        ):
            if value:
                target_kind, target_value = kind, value
                break
        if not target_value:
            continue
        if installation_key is None:
            installation_key = load_or_create_installation_key(config.data_dir)
        stable_key = stable_worker_key(
            installation_key, backend="herdr", host_id=config.host_id,
            workspace_id=identity[0], pane_id=identity[1],
        )
        private_fingerprint = worker_binding_private_fingerprint(
            host_id=config.host_id,
            backend="herdr",
            identity_material={
                "workspace_id": workspace_id,
                "pane_id": pane_id,
                "terminal_id": _text(pane, "terminal_id"),
            },
        )
        rows.append(
            {
                "agent": agent,
                "pane": pane,
                "private_fingerprint": private_fingerprint,
                "stable_key": stable_key,
                "target_kind": target_kind,
                "target_value": target_value,
                "desired_id": "worker-" + stable_fingerprint(
                    {"stable_key": stable_key}),
            }
        )
    private_counts = Counter(str(row["private_fingerprint"]) for row in rows)
    target_counts = Counter(str(row["target_value"]) for row in rows)
    stable_counts = Counter(str(row["stable_key"]) for row in rows)
    unique = [
        row for row in rows
        if private_counts[str(row["private_fingerprint"])] == 1
        and target_counts[str(row["target_value"])] == 1
        and stable_counts[str(row["stable_key"])] == 1
    ]
    workers, bindings = _materialize_discovered_workers(config, unique, observed_at)
    return workers, bindings, candidate_count - len(unique)


def _exact_mapping(
    value: Any, fields: set[str] | frozenset[str], message: str
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != fields:
        raise AcpCoordinatorError(message)
    return value


def _u64(value: Any) -> bool:
    return type(value) is int and 0 <= value <= (1 << 64) - 1


def _nonempty_text(value: Any, field: str) -> str:
    message = f"Herdr ACP endpoint {field} is invalid"
    _ensure(isinstance(value, str) and bool(value) and value.strip() == value, message)
    _ensure(len(value) <= 4096 and "\x00" not in value, message)
    return value


def _require_target_identity(
    continuity: WorkerBinding,
    worker: Mapping[str, Any],
    *,
    response: str,
) -> None:
    """Prove the returned worker still owns the requested Herdr target."""

    direct_fields = {
        "terminal_id": "terminal_id",
        "pane_id": "pane_id",
    }
    field = direct_fields.get(continuity.target_kind)
    matched = field is not None and worker.get(field) == continuity.target_value
    _ensure(matched, f"Herdr ACP {response} target changed")


def _parse_endpoint_identity(
    continuity: WorkerBinding,
    value: Mapping[str, Any],
    *,
    response: str,
) -> tuple[int, SessionOpenMode, str | None, Path]:
    worker = value.get("worker")
    adapter = value.get("adapter")
    session = value.get("session")
    _ensure(
        all(isinstance(item, Mapping) for item in (worker, adapter, session)),
        f"Herdr ACP {response} nested shape is invalid",
    )
    worker = _exact_mapping(
        worker, _WORKER_IDENTITY_FIELDS,
        f"Herdr ACP {response} worker identity shape is invalid",
    )
    generation = worker.get("generation")
    _ensure(
        _u64(generation),
        f"Herdr ACP {response} generation is invalid",
    )
    for field in _WORKER_IDENTITY_FIELDS - {"generation"}:
        _nonempty_text(worker.get(field), field)
    _require_target_identity(continuity, worker, response=response)
    adapter = _exact_mapping(
        adapter, {"name", "version"},
        f"Herdr ACP {response} adapter identity shape is invalid",
    )
    _nonempty_text(adapter.get("name"), "adapter name")
    _nonempty_text(adapter.get("version"), "adapter version")
    if not isinstance(session, Mapping) or set(session) not in ({"mode"}, {"id", "mode"}):
        raise AcpCoordinatorError(f"Herdr ACP {response} session shape is invalid")
    try:
        mode = SessionOpenMode(session.get("mode"))
    except (TypeError, ValueError) as exc:
        raise AcpCoordinatorError(f"Herdr ACP {response} session mode is invalid") from exc
    session_id_value = session.get("id")
    if mode is SessionOpenMode.NEW:
        if "id" in session and session_id_value is not None:
            raise AcpCoordinatorError(f"new Herdr ACP {response} already has a session")
        session_id = None
    else:
        session_id = _nonempty_text(session_id_value, "session id")
    cwd = Path(_nonempty_text(value.get("cwd"), "cwd"))
    _ensure(cwd.is_absolute(), f"Herdr ACP {response} cwd must be absolute")
    return generation, mode, session_id, cwd


def _parse_endpoint(
    config: Config,
    continuity: WorkerBinding,
    value: Any,
) -> HerdrAcpEndpoint:
    """Strictly validate the private, one-shot Herdr launch contract."""
    value = _exact_mapping(
        value, _ENDPOINT_FIELDS, "Herdr ACP endpoint response shape is invalid")
    ready = (value.get("type"), value.get("lifecycle")) == (
        "agent_acp_endpoint", "acp_owned_ready")
    _ensure(ready, "Herdr ACP endpoint is not ready")
    endpoint = value.get("endpoint")
    console = value.get("console")
    worker = value.get("worker")
    nested = (endpoint, console, worker, value.get("adapter"), value.get("session"))
    nested_valid = all(isinstance(item, Mapping) for item in nested)
    _ensure(nested_valid, "Herdr ACP endpoint nested shape is invalid")
    endpoint = _exact_mapping(
        endpoint, {"transport", "command", "args", "protocol_version"},
        "Herdr ACP launch specification is invalid",
    )
    protocol = endpoint.get("protocol_version")
    launch_valid = endpoint.get("transport") == "stdio" and type(protocol) is int
    _ensure(launch_valid and protocol == 1, "Herdr ACP launch protocol is unsupported")
    command = _nonempty_text(endpoint.get("command"), "command")
    executable_valid = command == "herdr" and Path(config.herdr_bin).name == "herdr"
    _ensure(executable_valid, "Herdr ACP endpoint executable is not configured Herdr")
    raw_generation, mode, session_id, cwd = _parse_endpoint_identity(
        continuity, value, response="endpoint"
    )
    generation = str(raw_generation)
    console = _exact_mapping(
        console, {"generation", "lease"},
        "Herdr ACP console endpoint shape is invalid",
    )
    console_current = console.get("generation") == raw_generation
    _ensure(console_current, "Herdr ACP console generation is inconsistent")
    console_lease = _nonempty_text(console.get("lease"), "console lease")
    _ensure(len(console_lease) <= 512, "Herdr ACP console lease is invalid")
    args = endpoint.get("args")
    valid_args = isinstance(args, list) and len(args) == 7
    valid_args = valid_args and args[0:2] == ["agent", "acp-attach"]
    valid_args = valid_args and args[3::2] == ["--generation", "--ticket"]
    _ensure(valid_args, "Herdr ACP attach arguments are invalid")
    target = _nonempty_text(args[2], "target")
    _ensure(target == continuity.target_value, "Herdr ACP endpoint target changed")
    argument_generation = _nonempty_text(args[4], "argument generation")
    _ensure(argument_generation == generation,
            "Herdr ACP endpoint generation is inconsistent")
    _nonempty_text(args[6], "ticket")
    return HerdrAcpEndpoint(
        command=(
            config.herdr_bin,
            *(_nonempty_text(item, "argument") for item in args),
        ),
        cwd=cwd,
        generation=generation,
        session_mode=mode,
        session_id=session_id,
        console=HerdrAcpConsoleEndpoint(raw_generation, console_lease),
    )


def _parse_status(
    continuity: WorkerBinding,
    value: Any,
) -> tuple[str, str, str]:
    """Validate Herdr's non-ticketing ACP generation/lease status."""
    value = _exact_mapping(
        value, _STATUS_FIELDS, "Herdr ACP status response shape is invalid")
    lifecycle = value.get("lifecycle")
    console_lifecycle = value.get("console_lifecycle")
    _ensure(
        value.get("type") == "agent_acp_status"
        and lifecycle in {"acp_owned_ready", "acp_owned_attached"},
        "Herdr ACP status lifecycle is invalid",
    )
    _ensure(
        console_lifecycle in {"starting", "attached", "missing"},
        "Herdr ACP console lifecycle is invalid",
    )
    raw_generation, _mode, _session_id, _cwd = _parse_endpoint_identity(
        continuity, value, response="status"
    )
    return str(raw_generation), str(lifecycle), str(console_lifecycle)


def _parse_console_exchange(
    value: Any, after_sequence: int
) -> tuple[tuple[int, str], ...]:
    value = _exact_mapping(
        value, _CONSOLE_EXCHANGE_FIELDS,
        "Herdr ACP console exchange shape is invalid",
    )
    _ensure(
        value.get("type") == "agent_acp_console_exchange",
        "Herdr ACP console exchange type is invalid",
    )
    raw_inputs, raw_outputs = value.get("inputs"), value.get("outputs")
    _ensure(
        isinstance(raw_inputs, list) and isinstance(raw_outputs, list),
        "Herdr ACP console exchange lists are invalid",
    )
    input_floor, output_floor, next_input, next_output = (
        value.get(name) for name in (
            "input_floor_sequence", "output_floor_sequence",
            "next_input_sequence", "next_output_sequence",
        )
    )
    numbers = (after_sequence, input_floor, output_floor, next_input, next_output)
    _ensure(
        all(_u64(number) for number in numbers)
        and next_input >= input_floor
        and next_output >= output_floor,
        "Herdr ACP console exchange floors are invalid",
    )
    if input_floor > after_sequence + 1:
        raise AcpConsoleInputGap(
            "Herdr ACP console input floor has a gap",
            recovery_after_sequence=next_input - 1,
        )
    parsed: list[tuple[int, str]] = []
    expected = after_sequence + 1
    for item in raw_inputs:
        item = _exact_mapping(
            item, {"sequence", "text"},
            "Herdr ACP console input shape is invalid",
        )
        sequence = item.get("sequence")
        text = item.get("text")
        if (
            type(sequence) is not int
            or sequence != expected
            or not isinstance(text, str)
            or not text.strip()
        ):
            if type(sequence) is int and sequence > expected:
                raise AcpConsoleInputGap(
                    "Herdr ACP console input sequence has a gap",
                    recovery_after_sequence=next_input - 1,
                )
            raise AcpCoordinatorError("Herdr ACP console input sequence is invalid")
        parsed.append((sequence, text))
        expected += 1
    if next_input != expected:
        if next_input > expected:
            raise AcpConsoleInputGap(
                "Herdr ACP console input response is incomplete",
                recovery_after_sequence=next_input - 1,
            )
        raise AcpCoordinatorError(
            "Herdr ACP console next input sequence is invalid"
        )
    return tuple(parsed)


def _console_submission_output(
    generation: str, sequence: int, future: Future[Any]
) -> dict[str, str] | None:
    try:
        outcome = future.result()
    except Exception:
        stream, text = "error", "instruction failed"
    else:
        text = {
            "permission": "permission selection accepted",
            "cancelled": "active turn cancellation requested",
        }.get(outcome) if isinstance(outcome, str) else None
        if text is None:
            return None
        stream = "status"
    return _bounded_console_output(
        f"console-input:{generation}:{sequence}", stream, text)


def _discard_local_turn(slot: _SessionSlot, turn_id: str | None) -> None:
    if turn_id is not None:
        with slot.lock:
            slot.console_local_turns.discard(turn_id)


def _append_console_output(
    output: list[dict[str, str]], item: dict[str, str], budget: int
) -> bool:
    if _console_output_wire_bytes([*output, item]) > budget:
        return False
    output.append(item)
    return True


def _append_console_events(
    output: list[dict[str, str]],
    events: Sequence[Any],
    local_turns: set[str],
    budget: int,
    after: int,
) -> tuple[int, set[str]]:
    consumed: set[str] = set()
    for stored in events:
        event = stored.event
        if event.kind == "user_message" and event.source_turn_id in local_turns:
            if event.source_turn_id is not None:
                consumed.add(event.source_turn_id)
        else:
            rendered = _console_event_output(event.kind, event.payload)
            if rendered is not None:
                item = _bounded_console_output(event.event_id, *rendered)
                if not _append_console_output(output, item, budget):
                    break
        after = stored.sequence
    return after, consumed


def _console_event_output(
    kind: str, payload: Mapping[str, Any]
) -> tuple[str, str] | None:
    if kind in {"user_message", "agent_message", "thought"}:
        delta = payload.get("text_delta")
        if not isinstance(delta, str) or not delta:
            return None
        return (
            {
                "user_message": "user",
                "agent_message": "assistant",
                "thought": "thought",
            }[kind],
            delta,
        )
    if kind in {"tool_call", "tool_call_update"}:
        snapshot = payload.get("snapshot")
        if not isinstance(snapshot, Mapping):
            return None
        title = str(snapshot.get("title") or snapshot.get("kind") or "tool")
        status = str(snapshot.get("status") or "updated")
        return "tool", f"{title} [{status}]"
    if kind == "plan":
        entries = payload.get("entries")
        if not isinstance(entries, list):
            return None
        lines = [
            f"[{str(item.get('status') or 'pending')}] {str(item.get('content') or '').strip()}"
            for item in entries
            if isinstance(item, Mapping) and str(item.get("content") or "").strip()
        ]
        return ("plan", "\n".join(lines)) if lines else None
    if (
        kind == "extension"
        and payload.get("extension") == "tendwire.acp.prompt_completion"
    ):
        return "status", f"turn {str(payload.get('outcome') or 'complete')}"
    return None


def _bounded_console_output(event_id: str, stream: str, text: str) -> dict[str, str]:
    encoded = text.encode("utf-8")
    if len(encoded) > _CONSOLE_OUTPUT_ITEM_TEXT_BYTES:
        encoded = encoded[:_CONSOLE_OUTPUT_ITEM_TEXT_BYTES]
        while True:
            try:
                text = encoded.decode("utf-8")
                break
            except UnicodeDecodeError as exc:
                encoded = encoded[: exc.start]
        text += "\n[console output truncated]"
    return {"event_id": event_id, "stream": stream, "text": text}


def _console_output_wire_bytes(output: list[dict[str, str]]) -> int:
    return len(
        json.dumps(output, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    )


def _latest_console_event_sequence(
    db_path: Path, host_id: str, worker_id: str, session_id: str
) -> int:
    after = 0
    while True:
        page = list_agent_events(
            db_path, host_id, worker_id=worker_id, source="acp",
            session_id=session_id, after_sequence=after, limit=1000,
        )
        for stored in page:
            after = max(after, stored.sequence)
        if len(page) < 1000:
            return after


def _console_permission_output(
    config: Config, slot: _SessionSlot
) -> dict[str, str] | None:
    pending = _console_pending_decision(
        Path(config.db_path), config.host_id, slot.continuity.worker_id)
    if pending is None:
        return None
    decision_ref, _options, prompt = pending
    event_id = "permission:" + stable_fingerprint({
        "worker_id": slot.continuity.worker_id,
        "decision_ref": decision_ref,
    })
    return _bounded_console_output(event_id, "status", prompt)


def _console_pending_decision(
    db_path: Path,
    host_id: str,
    worker_id: str,
) -> tuple[str, tuple[tuple[str, str], ...], str] | None:
    payload = pending_payload_from_store(db_path, host_id)
    interactions = payload.get("pending_interactions")
    if not isinstance(interactions, list):
        return None
    matches = [
        item
        for item in interactions
        if isinstance(item, Mapping)
        and item.get("worker_id") == worker_id
        and item.get("status") not in {"resolved", "closed", "stale"}
        and isinstance(item.get("meta"), Mapping)
        and isinstance(item["meta"].get("decision"), Mapping)
    ]
    if len(matches) != 1:
        return None
    item = matches[0]
    decision = item["meta"]["decision"]
    decision_ref = decision.get("decision_ref")
    raw_options = decision.get("options")
    if not isinstance(decision_ref, str) or not isinstance(raw_options, list):
        return None
    options = tuple(
        (str(option.get("ref") or ""), str(option.get("label") or ""))
        for option in raw_options
        if isinstance(option, Mapping)
        and str(option.get("ref") or "")
        and str(option.get("label") or "")
    )
    if not options:
        return None
    question = str(item.get("question") or "Permission required")
    choices = " ".join(f"{ref}={label}" for ref, label in options)
    prompt = f"permission> {question} ({choices}); reply with a number or /cancel"
    return decision_ref, options, prompt


def _console_permission_selection(
    text: str, options: tuple[tuple[str, str], ...]
) -> str | None:
    value = text.strip().lower()
    for ref, label in options:
        if value == ref.lower() or value == label.lower():
            return ref
    aliases = {
        "allow": ("allow",),
        "yes": ("allow",),
        "deny": ("deny", "reject"),
        "reject": ("deny", "reject"),
        "no": ("deny", "reject"),
    }
    terms = aliases.get(value)
    if terms is None:
        return None
    matches = [
        ref
        for ref, label in options
        if any(term in label.lower() for term in terms)
    ]
    return matches[0] if len(matches) == 1 else None


def production_acp_supervisor_factory(
    config: Config,
    stop_event: threading.Event,
) -> AcpSupervisor:
    """Build the stock daemon's Herdr-backed ACP session supervisor."""
    return AcpSupervisor(
        config,
        stop_event,
    )
