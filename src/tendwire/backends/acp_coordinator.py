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
        return self._owner._route_binding_fingerprint(self._worker, self._slot)

    def prompt(
        self,
        text: str,
        *,
        producer_turn_id: str,
        timeout: float,
        on_send_start: Callable[[], None] | None = None,
    ) -> object:
        return self._owner._submit_prompt(
            self._worker,
            self._slot,
            text,
            producer_turn_id=producer_turn_id,
            acknowledgement_timeout=timeout,
            on_send_start=on_send_start,
            generation_prepared=bool(
                getattr(self._prepared, "depth", 0)
            ),
        )

    @property
    def supports_steering(self) -> bool:
        return self._owner._supports_steering(self._worker, self._slot)

    def steer(
        self,
        text: str,
        *,
        producer_turn_id: str,
        timeout: float,
        on_send_start: Callable[[], None] | None = None,
    ) -> object:
        return self._owner._submit_steering(
            self._worker,
            self._slot,
            text,
            producer_turn_id=producer_turn_id,
            acknowledgement_timeout=timeout,
            on_send_start=on_send_start,
            generation_prepared=bool(getattr(self._prepared, "depth", 0)),
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
        resolved_reconcile_interval = float(
            config.reconcile_interval_seconds
            if reconcile_interval is None
            else reconcile_interval
        )
        if not math.isfinite(resolved_reconcile_interval) or resolved_reconcile_interval <= 0:
            raise ValueError("reconcile_interval must be finite and positive")
        self._reconcile_interval = resolved_reconcile_interval
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
        finally:
            self._reconcile_lock.release()
        with self._lock:
            slots = tuple(self._retired_slots.values())
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=max(0.0, deadline - time.monotonic()))
        unfinished = bool(thread is not None and thread.is_alive())
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
        counters = {
            "updates_ingested": 0,
            "permissions_ingested": 0,
            "permissions_selected": 0,
            "permissions_cancelled": 0,
            "invalid_permission_selections": 0,
            "prompts_started": 0,
            "prompts_completed": 0,
            "prompts_failed": 0,
            "cancellation_requests": 0,
        }
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
        worker_id = str(getattr(decision, "worker_id", "") or "")
        with self._lock:
            slot = self._slots.get(worker_id)
        if slot is None or slot.permission_broker is None:
            return False
        try:
            self._require_attached_generation(slot)
            return slot.permission_broker.owns(decision)
        except Exception:
            return False

    def answer_permission_decision(self, decision: Any, *, timeout: float) -> None:
        """Select an offered option and wait for its response-frame write."""
        worker_id = str(getattr(decision, "worker_id", "") or "")
        with self._reconcile_lock:
            self._require_reconcile_state(allow_starting=False)
            with self._lock:
                slot = self._slots.get(worker_id)
            if slot is None or slot.permission_broker is None:
                raise AcpCoordinatorError("ACP permission route is unavailable")
            self._require_attached_generation(slot)
            if not slot.permission_broker.owns(decision):
                raise AcpCoordinatorError("ACP permission authority changed")
            # Keep retirement fenced until respond_permission has acknowledged
            # writing the complete JSON-RPC response frame.
            slot.permission_broker.answer(decision, timeout=timeout)

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
                        self._retire_worker(
                            worker_id,
                            expected=slot,
                        )
                        current, _ambiguities = self._continuity_bindings()
                        continuity = current.get(worker_id)
                        if continuity is None:
                            raise AcpCoordinatorError(
                                "worker has no unique Herdr authority"
                            )
                        self._reconcile_binding(continuity)
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
        output: list[dict[str, str]] = []
        completed_submissions: list[int] = []
        output_budget = max(
            0,
            _CONSOLE_OUTPUT_QUEUE_BUDGET_BYTES - (retained_output_bytes or 0),
        )
        for sequence, future in tuple(submissions.items()):
            if not future.done():
                continue
            item: dict[str, str] | None = None
            try:
                outcome = future.result()
            except Exception:
                item = _bounded_console_output(
                    f"console-input:{slot.generation}:{sequence}",
                    "error",
                    "instruction failed",
                )
            else:
                status = (
                    {
                        "permission": "permission selection accepted",
                        "cancelled": "active turn cancellation requested",
                    }.get(outcome)
                    if isinstance(outcome, str)
                    else None
                )
                if status is not None:
                    item = _bounded_console_output(
                        f"console-input:{slot.generation}:{sequence}",
                        "status",
                        status,
                    )
            if item is not None:
                if _console_output_wire_bytes([*output, item]) > output_budget:
                    continue
                output.append(item)
            completed_submissions.append(sequence)
            input_sequence = max(input_sequence, sequence)
        pending = _console_pending_decision(
            Path(self.config.db_path),
            self.config.host_id,
            slot.continuity.worker_id,
        )
        if pending is not None:
            decision_ref, _options, prompt = pending
            item = _bounded_console_output(
                "permission:" + stable_fingerprint(
                    {
                        "worker_id": slot.continuity.worker_id,
                        "decision_ref": decision_ref,
                    }
                ),
                "status",
                prompt,
            )
            if _console_output_wire_bytes([*output, item]) <= output_budget:
                output.append(item)
        consumed_local_turns: set[str] = set()
        processed_event_sequence = event_sequence
        for stored in events:
            event = stored.event
            if event.kind == "user_message" and event.source_turn_id in local_turns:
                if event.source_turn_id is not None:
                    consumed_local_turns.add(event.source_turn_id)
                processed_event_sequence = stored.sequence
                continue
            rendered = _console_event_output(event.kind, event.payload)
            if rendered is None:
                processed_event_sequence = stored.sequence
                continue
            stream, text = rendered
            item = _bounded_console_output(event.event_id, stream, text)
            if _console_output_wire_bytes([*output, item]) > output_budget:
                break
            output.append(item)
            processed_event_sequence = stored.sequence
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
        worker = next(
            (
                item
                for item in (snapshot.workers if snapshot is not None else ())
                if item.id == slot.continuity.worker_id
                and item.fingerprint == slot.continuity.worker_fingerprint
            ),
            None,
        )
        stable_key = worker.meta.get("stable_key") if worker is not None else None
        stable_version = (
            worker.meta.get("stable_key_version") if worker is not None else None
        )
        if not isinstance(stable_key, str) or stable_version != 1:
            raise AcpCoordinatorError("ACP console stable worker identity is unavailable")
        request_id = "acpc." + stable_fingerprint(
            {
                "generation": slot.generation,
                "input_sequence": sequence,
                "worker_id": slot.continuity.worker_id,
            }
        )
        if text.strip() == "/cancel":
            slot.runtime.cancel()
            return "cancelled"
        request: dict[str, Any] = {
            "schema_version": 1,
            "response_schema_version": 3,
            "action": "send_instruction",
            "request_id": request_id,
            "dry_run": False,
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
            if local_turn_id is not None:
                with slot.lock:
                    slot.console_local_turns.discard(local_turn_id)
            raise
        if result.status not in {"accepted", "duplicate_request"}:
            if local_turn_id is not None:
                with slot.lock:
                    slot.console_local_turns.discard(local_turn_id)
            raise AcpCoordinatorError("ACP console input was not accepted")
        if request["action"] == "answer_decision":
            return "permission"
        if result.status == "duplicate_request" and local_turn_id is not None:
            # A duplicate receipt does not start a new ACP turn, so there is no
            # corresponding user event left to suppress in this process.
            with slot.lock:
                slot.console_local_turns.discard(local_turn_id)
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
                "agent_id",
                "agent",
                "name",
                "label",
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
        failures: list[BaseException] = [
            AcpCoordinatorError("worker has ambiguous Herdr authority")
            for _ in range(ambiguities)
        ]
        if discovery_omissions:
            failures.append(
                AcpCoordinatorError("one or more Herdr pane routes were omitted")
            )
        for worker_id, continuity in current.items():
            self._require_reconcile_state(allow_starting=True)
            try:
                self._reconcile_binding(continuity)
            except Exception as exc:  # noqa: BLE001
                failures.append(exc)
                self._retire_worker(worker_id)
        with self._lock:
            self._required_degraded = bool(failures)
            if failures:
                self._failure_type = type(failures[0]).__name__
            elif not self._required_degraded:
                self._failure_type = None
        if strict and failures:
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
                        failed_fingerprint = self._console_failed_claims.get(worker_id)
                    claimed_fingerprint = failed_fingerprint
                    authority_remains = True
                    if claimed_fingerprint is not None:
                        try:
                            authority_remains = (
                                worker_id,
                                claimed_fingerprint,
                            ) in self._herdr_authority_claims()
                        except Exception:
                            # A failed ownership check cannot prove that the
                            # Herdr endpoint disappeared; retry periodically.
                            authority_remains = True
                    with self._lock:
                        if worker_id not in self._slots and not authority_remains:
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
        endpoint = self._resolve_endpoint(continuity)
        runtime, permission_broker = self._build_runtime(continuity, endpoint)
        try:
            runtime.start()
        except Exception:
            permission_broker.close()
            try:
                runtime.stop(timeout=self.config.acp_shutdown_timeout_seconds)
            except Exception:
                pass
            raise
        try:
            self._require_reconcile_state(allow_starting=True)
        except Exception:
            permission_broker.close()
            self._stop_runtime(runtime)
            raise
        runtime_binding = getattr(runtime, "_binding", None)
        console_cursor = 0
        if isinstance(runtime_binding, WorkerBinding) and runtime_binding.turn_target_value:
            console_cursor = _latest_console_event_sequence(
                Path(self.config.db_path),
                self.config.host_id,
                continuity.worker_id,
                runtime_binding.turn_target_value,
            )
        slot = _SessionSlot(
            continuity,
            endpoint.generation,
            runtime,
            console=endpoint.console,
            console_executor=ThreadPoolExecutor(
                max_workers=1,
                thread_name_prefix=f"tendwire-acp-console-{continuity.worker_id}",
            ),
            permission_broker=permission_broker,
            console_event_sequence=console_cursor,
        )
        with self._lock:
            displaced = self._slots.get(continuity.worker_id)
            self._slots[continuity.worker_id] = slot
        if displaced is not None:
            self._stop_runtime(displaced.runtime)

    def _resolve_endpoint(self, continuity: WorkerBinding) -> HerdrAcpEndpoint:
        client = self._endpoint_client_factory(self.config)
        try:
            result = client.agent_acp_endpoint(
                continuity.target_value,
                timeout=self.config.herdr_timeout_seconds,
            )
        finally:
            client.close()
        return _parse_endpoint(self.config, continuity, result)

    def _resolve_status(self, continuity: WorkerBinding) -> tuple[str, str, str]:
        client = self._endpoint_client_factory(self.config)
        try:
            result = client.agent_acp_status(
                continuity.target_value,
                timeout=self.config.herdr_timeout_seconds,
            )
        finally:
            client.close()
        return _parse_status(continuity, result)

    def _require_attached_generation(self, slot: _SessionSlot) -> None:
        try:
            status = self._resolve_status(slot.continuity)
        except Exception:
            self._retire_worker(slot.continuity.worker_id, expected=slot)
            raise
        if status != (slot.generation, "acp_owned_attached", "attached"):
            self._retire_worker(slot.continuity.worker_id, expected=slot)
            raise AcpCoordinatorError("ACP worker generation lease is not current")

    def _submit_prompt(
        self,
        worker: Worker,
        slot: _SessionSlot,
        text: str,
        *,
        producer_turn_id: str,
        acknowledgement_timeout: float,
        on_send_start: Callable[[], None] | None = None,
        generation_prepared: bool = False,
    ) -> object:
        """Write through the exact route generation used by the receipt."""

        with self._reconcile_lock:
            self._require_reconcile_state(allow_starting=False)
            current = self._current_slot(worker)
            if current is not slot:
                raise AcpCoordinatorError("ACP worker route is stale")
            if not generation_prepared:
                self._require_attached_generation(slot)
            if self._current_slot(worker) is not slot:
                raise AcpCoordinatorError("ACP worker route is stale")
            # The route lease covers the complete JSON-RPC request frame, not
            # just status validation. Retirement can proceed as soon as the
            # runtime acknowledges that the frame is written; turn completion
            # remains supervised asynchronously by the runtime.
            return slot.runtime.submit_prompt(
                text,
                producer_turn_id=producer_turn_id,
                acknowledgement_timeout=acknowledgement_timeout,
                on_send_start=on_send_start,
            )

    def _supports_steering(self, worker: Worker, slot: _SessionSlot) -> bool:
        try:
            return self._current_slot(worker) is slot and slot.runtime.can_steer()
        except Exception:
            return False

    def _submit_steering(
        self,
        worker: Worker,
        slot: _SessionSlot,
        text: str,
        *,
        producer_turn_id: str,
        acknowledgement_timeout: float,
        on_send_start: Callable[[], None] | None = None,
        generation_prepared: bool = False,
    ) -> object:
        """Steer the exact attached generation used by the receipt."""

        with self._reconcile_lock:
            self._require_reconcile_state(allow_starting=False)
            current = self._current_slot(worker)
            if current is not slot:
                raise AcpCoordinatorError("ACP worker route is stale")
            if not generation_prepared:
                self._require_attached_generation(slot)
            if self._current_slot(worker) is not slot or not slot.runtime.can_steer():
                raise AcpCoordinatorError("ACP steering route is unavailable")
            return slot.runtime.submit_steering(
                text,
                producer_turn_id=producer_turn_id,
                acknowledgement_timeout=acknowledgement_timeout,
                on_send_start=on_send_start,
            )

    def _route_binding_fingerprint(
        self,
        worker: Worker,
        slot: _SessionSlot,
    ) -> str:
        """Return authority only while this exact route remains current."""

        with self._reconcile_lock:
            self._require_reconcile_state(allow_starting=False)
            if self._current_slot(worker) is not slot:
                raise AcpCoordinatorError("ACP worker route is stale")
            binding = getattr(slot.runtime, "_binding", None)
            return (
                str(binding.private_fingerprint)
                if isinstance(binding, WorkerBinding)
                else ""
            )

    def _build_runtime(
        self,
        continuity: WorkerBinding,
        endpoint: HerdrAcpEndpoint,
    ) -> tuple[AcpWorkerSession, AcpPermissionBroker]:
        client = self._connection_factory(
            endpoint.command,
            cwd=endpoint.cwd,
            request_timeout=self.config.acp_request_timeout_seconds,
            prompt_timeout=float(self.config.submission_hard_ttl_seconds),
            close_timeout=self.config.acp_shutdown_timeout_seconds,
            max_frame_bytes=self.config.acp_max_frame_bytes,
        )
        if endpoint.session_mode is SessionOpenMode.NEW:
            binding = continuity
            callback = self._bind_new_session
        else:
            assert endpoint.session_id is not None
            binding = _derived_binding(continuity, endpoint.session_id)
            upsert_worker_bindings(Path(self.config.db_path), [binding])
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
            return runtime, permission_broker
        except Exception:
            permission_broker.close()
            try:
                client.close()
            except Exception:
                pass
            if endpoint.session_mode is not SessionOpenMode.NEW:
                _expire_derived_binding(
                    self.config,
                    binding,
                    reason="acp_runtime_construction_failed",
                )
            raise

    def _bind_new_session(
        self,
        session_id: str,
        continuity: WorkerBinding,
    ) -> WorkerBinding:
        bound = _derived_binding(continuity, session_id)
        upsert_worker_bindings(Path(self.config.db_path), [bound])
        return bound

    def _retire_worker(
        self,
        worker_id: str,
        *,
        expected: _SessionSlot | None = None,
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
            with slot.lock:
                slot.retired = True
                executor = slot.console_executor
            if slot.permission_broker is not None:
                slot.permission_broker.close()
            executor.shutdown(wait=False, cancel_futures=True)
            self._stop_runtime(slot.runtime)

    def _stop_runtime(
        self, runtime: AcpWorkerSession, *, timeout: float | None = None
    ) -> None:
        binding = getattr(runtime, "_binding", None)
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
            if isinstance(binding, WorkerBinding) and binding.backend == "acp":
                _expire_derived_binding(
                    self.config,
                    binding,
                    reason="acp_runtime_retired",
                )

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
            slots = tuple(self._slots.values())
            self._slots.clear()
            self._retired_slots.update((id(slot), slot) for slot in slots)
        if not slots:
            return
        total = (
            self.config.acp_shutdown_timeout_seconds
            if timeout is None
            else timeout
        )
        deadline = time.monotonic() + total
        for slot in slots:
            # Bridge-held slot critical sections are deliberately local and
            # non-blocking; synchronize the retirement flag with every reader
            # so shutdown cannot race a last console mutation or submission.
            with slot.lock:
                slot.retired = True
                executor = slot.console_executor
            if slot.permission_broker is not None:
                slot.permission_broker.close()
            executor.shutdown(wait=False, cancel_futures=True)
            self._stop_runtime(
                slot.runtime,
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


def _same_continuity(left: WorkerBinding, right: WorkerBinding) -> bool:
    """Compare authority identity while ignoring observation lease refreshes."""
    return replace(
        left,
        observed_at=right.observed_at,
        expires_at=right.expires_at,
    ) == right


def _text(item: Mapping[str, Any], *names: str) -> str | None:
    for name in names:
        value = item.get(name)
        if isinstance(value, (str, int)) and not isinstance(value, bool):
            text = str(value).strip()
            if text:
                return text
    return None


def _items(payload: Any, name: str) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        values = payload
    elif isinstance(payload, Mapping):
        values = payload.get(name, [])
    else:
        return []
    return (
        [dict(value) for value in values if isinstance(value, Mapping)]
        if isinstance(values, list)
        else []
    )


def _discovered_spaces(payload: Any) -> list[Space]:
    spaces: list[Space] = []
    for item in _items(payload, "workspaces"):
        space_id = _text(item, "workspace_id", "space_id", "id", "name")
        if space_id is None:
            continue
        spaces.append(
            Space(
                id=space_id,
                name=_text(item, "label", "name", "title") or space_id,
                status=normalize_status(_text(item, "status", "state")),
                updated_at=_text(item, "updated_at", "observed_at"),
            )
        )
    return spaces


def _agent_match(
    pane: Mapping[str, Any],
    agents: Sequence[Mapping[str, Any]],
) -> tuple[Mapping[str, Any] | None, bool]:
    pane_id = _text(pane, "pane_id")
    terminal_id = _text(pane, "terminal_id")
    matches = [
        agent
        for agent in agents
        if (pane_id and _text(agent, "pane_id") == pane_id)
        or (terminal_id and _text(agent, "terminal_id") == terminal_id)
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
    agents = _items(agent_payload, "agents")
    rows: list[dict[str, Any]] = []
    candidate_count = 0
    for pane in panes:
        agent, ambiguous_agent = _agent_match(pane, agents)
        if agent is None and not ambiguous_agent and not _text(
            pane, "agent", "name", "label"
        ):
            continue
        candidate_count += 1
        if ambiguous_agent:
            continue
        workspace_id = _text(pane, "workspace_id")
        pane_id = _text(pane, "pane_id")
        identity = canonical_herdr_pane_identity(workspace_id, pane_id)
        if identity is None:
            continue
        target_kind = ""
        target_value = ""
        for kind, value in (
            ("agent_id", _text(agent or {}, "agent_id")),
            ("terminal_id", _text(pane, "terminal_id")),
            ("pane_id", pane_id),
        ):
            if value:
                target_kind, target_value = kind, value
                break
        if not target_value:
            continue
        base_id = (
            _text(agent or {}, "agent", "name")
            or _text(pane, "agent", "name", "label")
            or "worker"
        )
        private_fingerprint = worker_binding_private_fingerprint(
            host_id=config.host_id,
            backend="herdr",
            identity_material={
                "workspace_id": workspace_id,
                "pane_id": pane_id,
                "terminal_id": _text(pane, "terminal_id"),
                "agent_id": _text(agent or {}, "agent_id"),
            },
        )
        rows.append(
            {
                "agent": agent or {},
                "base_id": base_id,
                "identity": identity,
                "pane": pane,
                "private_fingerprint": private_fingerprint,
                "target_kind": target_kind,
                "target_value": target_value,
            }
        )

    installation_key = load_or_create_installation_key(config.data_dir) if rows else None
    for row in rows:
        identity = row["identity"]
        assert installation_key is not None and identity is not None
        workspace_id, pane_id = identity
        row["stable_key"] = stable_worker_key(
            installation_key, backend="herdr", host_id=config.host_id,
            workspace_id=workspace_id, pane_id=pane_id,
        )
        row["desired_id"] = "worker-" + stable_fingerprint(
            {"stable_key": row["stable_key"]}
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


def _nonempty_text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value or value.strip() != value:
        raise AcpCoordinatorError(f"Herdr ACP endpoint {field} is invalid")
    if len(value) > 4096 or "\x00" in value:
        raise AcpCoordinatorError(f"Herdr ACP endpoint {field} is invalid")
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
        "name": "name",
        "label": "name",
        "agent": "agent",
    }
    field = direct_fields.get(continuity.target_kind)
    if field is not None:
        matched = worker.get(field) == continuity.target_value
    else:
        # Older Herdr projections can call the terminal identity `agent_id`.
        # The v1 endpoint contract has no agent_id member, so require that the
        # authority token still matches one of its immutable identity fields.
        matched = continuity.target_value in {
            worker.get("terminal_id"),
            worker.get("pane_id"),
            worker.get("name"),
            worker.get("agent"),
        }
    if not matched:
        raise AcpCoordinatorError(f"Herdr ACP {response} target changed")


def _parse_endpoint_identity(
    continuity: WorkerBinding,
    value: Mapping[str, Any],
    *,
    response: str,
) -> tuple[int, SessionOpenMode, str | None, Path]:
    worker = value.get("worker")
    adapter = value.get("adapter")
    session = value.get("session")
    if not all(isinstance(item, Mapping) for item in (worker, adapter, session)):
        raise AcpCoordinatorError(f"Herdr ACP {response} nested shape is invalid")
    assert isinstance(worker, Mapping)
    assert isinstance(adapter, Mapping)
    assert isinstance(session, Mapping)
    worker_fields = {
        "terminal_id", "workspace_id", "tab_id", "pane_id",
        "name", "agent", "generation",
    }
    if set(worker) != worker_fields:
        raise AcpCoordinatorError(f"Herdr ACP {response} worker identity shape is invalid")
    generation = worker.get("generation")
    if type(generation) is not int or not 0 <= generation <= (1 << 64) - 1:
        raise AcpCoordinatorError(f"Herdr ACP {response} generation is invalid")
    for field in worker_fields - {"generation"}:
        _nonempty_text(worker.get(field), field)
    _require_target_identity(continuity, worker, response=response)
    if set(adapter) != {"name", "version"}:
        raise AcpCoordinatorError(f"Herdr ACP {response} adapter identity shape is invalid")
    _nonempty_text(adapter.get("name"), "adapter name")
    _nonempty_text(adapter.get("version"), "adapter version")
    if set(session) not in ({"mode"}, {"id", "mode"}):
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
    if not cwd.is_absolute():
        raise AcpCoordinatorError(f"Herdr ACP {response} cwd must be absolute")
    return generation, mode, session_id, cwd


def _parse_endpoint(
    config: Config,
    continuity: WorkerBinding,
    value: Any,
) -> HerdrAcpEndpoint:
    """Strictly validate the private, one-shot Herdr launch contract."""
    if not isinstance(value, Mapping) or set(value) != {
        "type",
        "endpoint",
        "console",
        "worker",
        "adapter",
        "session",
        "cwd",
        "lifecycle",
    }:
        raise AcpCoordinatorError("Herdr ACP endpoint response shape is invalid")
    if (
        value.get("type") != "agent_acp_endpoint"
        or value.get("lifecycle") != "acp_owned_ready"
    ):
        raise AcpCoordinatorError("Herdr ACP endpoint is not ready")
    endpoint = value.get("endpoint")
    console = value.get("console")
    worker = value.get("worker")
    if not all(
        isinstance(item, Mapping)
        for item in (endpoint, console, worker, value.get("adapter"), value.get("session"))
    ):
        raise AcpCoordinatorError("Herdr ACP endpoint nested shape is invalid")
    assert isinstance(endpoint, Mapping)
    assert isinstance(console, Mapping)
    assert isinstance(worker, Mapping)
    if set(endpoint) != {"transport", "command", "args", "protocol_version"}:
        raise AcpCoordinatorError("Herdr ACP launch specification is invalid")
    if (
        endpoint.get("transport") != "stdio"
        or type(endpoint.get("protocol_version")) is not int
        or endpoint.get("protocol_version") != 1
    ):
        raise AcpCoordinatorError("Herdr ACP launch protocol is unsupported")
    command = _nonempty_text(endpoint.get("command"), "command")
    if command != "herdr" or Path(config.herdr_bin).name != "herdr":
        raise AcpCoordinatorError("Herdr ACP endpoint executable is not configured Herdr")
    raw_generation, mode, session_id, cwd = _parse_endpoint_identity(
        continuity, value, response="endpoint"
    )
    generation = str(raw_generation)
    if set(console) != {"generation", "lease"}:
        raise AcpCoordinatorError("Herdr ACP console endpoint shape is invalid")
    if console.get("generation") != raw_generation:
        raise AcpCoordinatorError("Herdr ACP console generation is inconsistent")
    console_lease = _nonempty_text(console.get("lease"), "console lease")
    if len(console_lease) > 512:
        raise AcpCoordinatorError("Herdr ACP console lease is invalid")
    args = endpoint.get("args")
    if (
        not isinstance(args, list)
        or len(args) != 7
        or args[0:2] != ["agent", "acp-attach"]
        or args[3] != "--generation"
        or args[5] != "--ticket"
    ):
        raise AcpCoordinatorError("Herdr ACP attach arguments are invalid")
    target = _nonempty_text(args[2], "target")
    if target != continuity.target_value:
        raise AcpCoordinatorError("Herdr ACP endpoint target changed")
    if _nonempty_text(args[4], "argument generation") != generation:
        raise AcpCoordinatorError("Herdr ACP endpoint generation is inconsistent")
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
    if not isinstance(value, Mapping) or set(value) != {
        "type",
        "worker",
        "adapter",
        "session",
        "cwd",
        "lifecycle",
        "console_lifecycle",
    }:
        raise AcpCoordinatorError("Herdr ACP status response shape is invalid")
    lifecycle = value.get("lifecycle")
    console_lifecycle = value.get("console_lifecycle")
    if value.get("type") != "agent_acp_status" or lifecycle not in {
        "acp_owned_ready",
        "acp_owned_attached",
    }:
        raise AcpCoordinatorError("Herdr ACP status lifecycle is invalid")
    if console_lifecycle not in {"starting", "attached", "missing"}:
        raise AcpCoordinatorError("Herdr ACP console lifecycle is invalid")
    raw_generation, _mode, _session_id, _cwd = _parse_endpoint_identity(
        continuity, value, response="status"
    )
    return str(raw_generation), str(lifecycle), str(console_lifecycle)


def _parse_console_exchange(
    value: Any, after_sequence: int
) -> tuple[tuple[int, str], ...]:
    if not isinstance(value, Mapping) or set(value) != {
        "type",
        "inputs",
        "outputs",
        "input_floor_sequence",
        "output_floor_sequence",
        "next_input_sequence",
        "next_output_sequence",
    }:
        raise AcpCoordinatorError("Herdr ACP console exchange shape is invalid")
    if value.get("type") != "agent_acp_console_exchange":
        raise AcpCoordinatorError("Herdr ACP console exchange type is invalid")
    raw_inputs, raw_outputs = value.get("inputs"), value.get("outputs")
    if not isinstance(raw_inputs, list) or not isinstance(raw_outputs, list):
        raise AcpCoordinatorError("Herdr ACP console exchange lists are invalid")
    input_floor, output_floor, next_input, next_output = (
        value.get(name) for name in (
            "input_floor_sequence", "output_floor_sequence",
            "next_input_sequence", "next_output_sequence",
        )
    )
    if (
        any(type(number) is not int or not 0 <= number <= (1 << 64) - 1
            for number in (after_sequence, input_floor, output_floor, next_input, next_output))
        or next_input < input_floor
        or next_output < output_floor
    ):
        raise AcpCoordinatorError("Herdr ACP console exchange floors are invalid")
    if input_floor > after_sequence + 1:
        raise AcpConsoleInputGap(
            "Herdr ACP console input floor has a gap",
            recovery_after_sequence=next_input - 1,
        )
    parsed: list[tuple[int, str]] = []
    expected = after_sequence + 1
    for item in raw_inputs:
        if not isinstance(item, Mapping) or set(item) != {"sequence", "text"}:
            raise AcpCoordinatorError("Herdr ACP console input shape is invalid")
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
    return decision_ref, options, f"permission> {question} ({choices}); reply with a number or /cancel"


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
