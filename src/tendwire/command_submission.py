"""Authoritative daemon command submission path for Tendwire."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Any, Protocol

from .config import Config
from .core.actions import CommandContext, execute_command
from .core.commands import (
    COMMAND_ENVELOPE_SCHEMA_VERSION,
    COMMAND_ENVELOPE_V3_SCHEMA_VERSION,
    DISPOSITION_IN_PROGRESS,
    DISPOSITION_NO_RECEIPT,
    DISPOSITION_TERMINAL_ACCEPTED,
    DISPOSITION_TERMINAL_REJECTED,
    DISPOSITION_TERMINAL_UNCERTAIN,
    STATUS_ACCEPTED,
    STATUS_ANSWER_IN_PROGRESS,
    STATUS_AMBIGUOUS_BACKEND_TARGET,
    STATUS_AMBIGUOUS_TARGET,
    STATUS_BACKEND_FAILED,
    STATUS_BACKEND_UNAVAILABLE,
    STATUS_BACKEND_UNSUPPORTED,
    STATUS_DRY_RUN,
    STATUS_DECISION_NOT_PENDING,
    STATUS_DUPLICATE_REQUEST,
    STATUS_INVALID_SELECTION,
    STATUS_NOT_FOUND,
    STATUS_PENDING,
    STATUS_REJECTED,
    STATUS_REQUEST_STATE_UNCERTAIN,
    STATUS_RESOLVED,
    STATUS_STALE_TARGET,
    STATUS_UNKNOWN_WORKER,
    STATUS_UNSUPPORTED_DECISION,
    CanonicalMutation,
    CommandEnvelope,
    CommandRequest,
    build_canonical_mutation,
    build_selector_proof,
    error_value,
    is_selector_proof,
    turn_submission_id,
    parse_command_request,
    resolve_target,
    validate_request,
    worker_candidate,
)
from .core.models import BackendHealth, Snapshot, Worker, WorkerBinding
from .core.projector import project_from_observations
from .backends.herdr_decision import calibrate_decision_steps
from .store.sqlite import (
    abandon_backend_pending_choice_claim,
    abandon_command_request_reservation,
    backend_pending_choice_terminal_effect,
    claim_backend_pending_choice,
    claim_backend_pending_decision,
    command_reservation_is_live,
    envelope_to_receipt_json,
    finish_command_request,
    finish_queued_command_request,
    finish_unverified_queued_command_request,
    get_command_request,
    linked_turn_for_submission,
    latest_snapshot,
    list_worker_bindings,
    mark_command_send_started,
    record_command_send_queued,
    recover_unresolved_command_send,
    reserve_command_request,
    reserve_terminal_command_replay,
    settle_submission_link_for_request,
    start_backend_pending_choice_send,
    start_backend_pending_decision_send,
)


HERDR_BACKEND = "herdr"
_MUTATING_ACTIONS = frozenset(
    {"send_instruction", "answer_pending", "answer_decision"}
)
_LEGACY_V0_REPLAY_WORKER_ID = "legacy-v0-replay-only"
_PENDING_CHANGED_MESSAGE = "pending interaction changed or is no longer answerable"
_DISALLOWED_SEND_STATUSES = frozenset({"closed", "failed", "unknown"})
_AMBIGUOUS_BINDING_REASONS = frozenset({"duplicate_backend_target", "not_unique"})
_PRIVATE_PANE_CLEAR_KEY_SEQUENCES = (
    ("ctrl+u",),
    ("ctrl+a", "ctrl+k"),
    ("ctrl+a", "backspace"),
)
_PANE_SUBMIT_TARGET_KINDS = frozenset(
    {
        "agent_id",
        "agent",
        "name",
        "label",
        "terminal_id",
        "pane_id",
    }
)

SocketClientFactory = Callable[[Config], Any]


class AcpPromptRoute(Protocol):
    """One live, authority-checked ACP prompt route owned by the daemon.

    The route deliberately exposes neither adapter argv nor session identity.
    Its binding fingerprint is private durable evidence used only by the
    command receipt state machine.
    """

    binding_fingerprint: str

    def prompt(
        self,
        text: str,
        *,
        producer_turn_id: str,
        timeout: float,
        on_send_start: Callable[[], None] | None = None,
    ) -> object: ...

    # Production routes may expose a context manager that fences their exact
    # generation from the final authority check through the prompt-frame
    # acknowledgement.  Test and third-party routes remain compatible without
    # it; submit_acp_command probes this method dynamically.
    def prepare(self) -> Any: ...

    @property
    def supports_steering(self) -> bool: ...

    def steer(
        self,
        text: str,
        *,
        producer_turn_id: str,
        timeout: float,
        on_send_start: Callable[[], None] | None = None,
    ) -> object: ...


AcpPromptRouter = Callable[[Worker], AcpPromptRoute | None]
AcpWorkerOwner = Callable[[str, str], bool]


class AcpPermissionDecisionRouter(Protocol):
    """Private daemon-owned bridge for one exact ACP permission decision."""

    def owns_permission_decision(self, decision: Any) -> bool: ...

    def answer_permission_decision(self, decision: Any, *, timeout: float) -> None: ...


@dataclass(frozen=True)
class ResolvedCommandTarget:
    worker: Worker
    binding: WorkerBinding


def _raw_payload_from_mapping(params: Mapping[str, Any]) -> str:
    return json.dumps(
        dict(params),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )




def _default_socket_client_factory(config: Config) -> Any:
    from .backends.herdr_socket import HerdrSocketClient

    return HerdrSocketClient(timeout=config.herdr_timeout_seconds)




def _backend_health(snapshot: Snapshot) -> BackendHealth:
    for health in snapshot.backend_health:
        if health.name == HERDR_BACKEND:
            return health
    return BackendHealth(name=HERDR_BACKEND, status="unknown", outcome="unknown")


def _current_snapshot(config: Config) -> Snapshot:
    if config.db_path is not None:
        snapshot = latest_snapshot(config.db_path, config.host_id)
        if snapshot is not None:
            return snapshot
    return project_from_observations(config)


def _backend_unavailable(
    request: CommandRequest,
    message: str,
    *,
    health: BackendHealth | None = None,
) -> CommandEnvelope:
    details: dict[str, Any] = {}
    if health is not None:
        details["backend"] = {
            "name": health.name,
            "status": health.status,
            "outcome": health.outcome,
        }
    return CommandEnvelope.from_error(
        request,
        error_value(STATUS_BACKEND_UNAVAILABLE, message, details=details),
    )


def _backend_health_error(config: Config, request: CommandRequest, snapshot: Snapshot) -> CommandEnvelope | None:
    if config.herdr_backend != "socket":
        return _backend_unavailable(
            request,
            "Herdr socket backend is not enabled",
        )
    health = _backend_health(snapshot)
    if health.status != "healthy":
        return _backend_unavailable(
            request,
            "Herdr socket backend is not healthy",
            health=health,
        )
    return None


def _target_resolution_error(
    request: CommandRequest,
    status: str,
    candidates: list[dict[str, Any]],
) -> CommandEnvelope:
    if status == STATUS_STALE_TARGET:
        return CommandEnvelope.from_result(
            request,
            ok=False,
            status=STATUS_STALE_TARGET,
            result={"candidates": candidates},
            error=error_value(
                STATUS_STALE_TARGET,
                "target worker fingerprint does not match the current worker",
            ),
        )
    if status == STATUS_AMBIGUOUS_TARGET:
        return CommandEnvelope.from_result(
            request,
            ok=False,
            status=STATUS_AMBIGUOUS_TARGET,
            result={"candidates": candidates},
            error=error_value(STATUS_AMBIGUOUS_TARGET, "target matches more than one worker"),
        )
    if status == STATUS_REJECTED:
        message = "target worker status does not allow instructions"
        if candidates:
            message = f"target worker status does not allow instructions: {candidates[0]['status']!r}"
        return CommandEnvelope.from_result(
            request,
            ok=False,
            status=STATUS_REJECTED,
            result={"candidates": candidates},
            error=error_value(STATUS_REJECTED, message),
        )
    return CommandEnvelope.from_result(
        request,
        ok=False,
        status=STATUS_NOT_FOUND,
        result={"candidates": []},
        error=error_value(STATUS_NOT_FOUND, "no worker matches the target"),
    )


def _binding_error(request: CommandRequest, status: str, message: str) -> CommandEnvelope:
    return CommandEnvelope.from_result(
        request,
        ok=False,
        status=status,
        error=error_value(status, message),
    )


def _binding_for_worker(
    request: CommandRequest,
    worker: Worker,
    bindings: list[WorkerBinding],
) -> ResolvedCommandTarget | CommandEnvelope:
    worker_bindings = [
        binding
        for binding in bindings
        if binding.backend == HERDR_BACKEND and binding.worker_id == worker.id
    ]
    if not worker_bindings:
        return _binding_error(
            request,
            STATUS_BACKEND_UNSUPPORTED,
            "target has no backend-owned sendable private binding",
        )

    exact = [binding for binding in worker_bindings if binding.worker_fingerprint == worker.fingerprint]
    if not exact:
        return _binding_error(
            request,
            STATUS_STALE_TARGET,
            "target private binding is stale for the current worker",
        )
    if len(exact) != 1:
        return _binding_error(
            request,
            STATUS_AMBIGUOUS_BACKEND_TARGET,
            "target resolves to an ambiguous backend send target",
        )

    binding = exact[0]
    if (
        not binding.sendable
        or not binding.target_value
        or binding.target_kind not in _PANE_SUBMIT_TARGET_KINDS
    ):
        if (binding.reason or "") in _AMBIGUOUS_BINDING_REASONS:
            return _binding_error(
                request,
                STATUS_AMBIGUOUS_BACKEND_TARGET,
                "target resolves to an ambiguous backend send target",
            )
        return _binding_error(
            request,
            STATUS_BACKEND_UNSUPPORTED,
            "target has no backend-owned sendable private binding",
        )

    return ResolvedCommandTarget(worker=worker, binding=binding)


def _resolve_authoritative_worker(
    request: CommandRequest,
    snapshot: Snapshot,
) -> Worker | CommandEnvelope:
    resolved, candidates, status = resolve_target(
        request.target,
        list(snapshot.workers),
        allow_disallowed_status=True,
        include_backend_target=False,
    )
    if status != STATUS_RESOLVED:
        return _target_resolution_error(request, status, candidates)

    worker = next(
        (item for item in snapshot.workers if item.id == (resolved or {}).get("worker_id")),
        None,
    )
    if worker is None:
        return _target_resolution_error(request, STATUS_NOT_FOUND, [])
    return worker


def _worker_status_error(
    request: CommandRequest,
    worker: Worker,
) -> CommandEnvelope | None:
    if worker.status not in _DISALLOWED_SEND_STATUSES:
        return None
    return _target_resolution_error(
        request,
        STATUS_REJECTED,
        [worker_candidate(worker)],
    )


def _socket_request(client: Any, method: str, params: Mapping[str, Any], *, timeout: float) -> Any:
    if not hasattr(client, "request"):
        raise TypeError("socket client does not expose generic request")
    return client.request(method, params, timeout=timeout)


def _pane_id_from_agent_info(value: Any) -> str:
    if not isinstance(value, Mapping):
        return ""
    result = value.get("result")
    agent = result.get("agent") if isinstance(result, Mapping) else None
    if not isinstance(agent, Mapping):
        agent = value.get("agent")
    if not isinstance(agent, Mapping):
        return ""
    pane_id = agent.get("pane_id") or agent.get("paneId")
    return str(pane_id or "").strip()


def _pane_id_from_terminal_listing(value: Any, terminal_id: str) -> str:
    if not isinstance(value, Mapping):
        raise ValueError("invalid agent.list response")
    agents = value.get("agents")
    if not isinstance(agents, list):
        raise ValueError("invalid agent.list agents")
    matches: set[str] = set()
    for agent in agents:
        if not isinstance(agent, Mapping):
            raise ValueError("invalid agent.list entry")
        listed_terminal_id = agent.get("terminal_id")
        listed_pane_id = agent.get("pane_id")
        if (
            not isinstance(listed_terminal_id, str)
            or not listed_terminal_id
            or not isinstance(listed_pane_id, str)
            or not listed_pane_id.strip()
        ):
            raise ValueError("invalid agent.list entry")
        if listed_terminal_id == terminal_id:
            matches.add(listed_pane_id.strip())
    if len(matches) > 1:
        raise ValueError("ambiguous agent.list terminal_id match")
    return next(iter(matches), "")


def _private_pane_id_for_binding(client: Any, binding: WorkerBinding, *, timeout: float) -> str:
    if binding.target_kind == "pane_id":
        return str(binding.target_value or "").strip()
    try:
        response = _socket_request(
            client,
            "agent.get",
            {"target": binding.target_value},
            timeout=timeout,
        )
    except Exception as exc:  # noqa: BLE001
        from .backends.herdr_protocol import HerdrErrorResponse

        # Herdr 0.7.5 stopped resolving terminal-id targets through agent.get
        # while agent.list still publishes the terminal_id -> pane_id mapping,
        # so a definite target-lookup error falls back to the listing before
        # the caller terminalizes the request.
        if not isinstance(exc, HerdrErrorResponse) or binding.target_kind != "terminal_id":
            raise
        listing = _socket_request(client, "agent.list", {}, timeout=timeout)
        pane_id = _pane_id_from_terminal_listing(
            listing,
            str(binding.target_value or ""),
        )
        if pane_id:
            return pane_id
        raise
    return _pane_id_from_agent_info(response)


def _submit_private_pane_input(client: Any, pane_id: str, instruction_text: str, *, timeout: float) -> None:
    # A single ctrl+u is not reliable across all foreground TUIs. Clear stale
    # input first, then submit text and Enter in one Herdr operation so the
    # foreground application cannot observe a staged prompt between requests.
    try:
        for keys in _PRIVATE_PANE_CLEAR_KEY_SEQUENCES:
            _socket_request(
                client,
                "pane.send_keys",
                {"pane_id": pane_id, "keys": list(keys)},
                timeout=timeout,
            )
    except Exception as exc:
        raise _PaneInputNotStartedError from exc
    _socket_request(
        client,
        "pane.send_input",
        {"pane_id": pane_id, "text": instruction_text, "keys": ["Enter"]},
        timeout=timeout,
    )


class _PaneInputNotStartedError(RuntimeError):
    """The instruction input operation was never attempted."""


def _agent_prompt_delivery(value: Any) -> str:
    if not isinstance(value, Mapping):
        return ""
    result = value.get("result")
    candidate = result if isinstance(result, Mapping) else value
    delivery = candidate.get("delivery")
    return delivery if isinstance(delivery, str) else ""


def _herdr_error_code(exc: BaseException) -> str:
    from .backends.herdr_protocol import HerdrErrorResponse

    if not isinstance(exc, HerdrErrorResponse) or not isinstance(exc.error, Mapping):
        return ""
    code = exc.error.get("code")
    return code if isinstance(code, str) else ""


def _target_state_at_send(worker: Worker) -> str:
    status = str(worker.status or "").strip().lower().replace("-", "_")
    return status or "unknown"


def _instruction_text(request: CommandRequest) -> str:
    instruction = request.instruction if isinstance(request.instruction, dict) else {}
    text = instruction.get("text")
    return text if isinstance(text, str) else ""




def _backend_failure(request: CommandRequest, message: str) -> CommandEnvelope:
    return CommandEnvelope.from_result(
        request,
        ok=False,
        status=STATUS_BACKEND_FAILED,
        error=error_value(STATUS_BACKEND_FAILED, message),
    )


def _backend_uncertain(request: CommandRequest, message: str) -> CommandEnvelope:
    return CommandEnvelope.from_result(
        request,
        ok=False,
        status=STATUS_REQUEST_STATE_UNCERTAIN,
        disposition=DISPOSITION_TERMINAL_UNCERTAIN,
        error=error_value(STATUS_REQUEST_STATE_UNCERTAIN, message),
    )


def _request_in_progress(request: CommandRequest) -> CommandEnvelope:
    return CommandEnvelope.from_result(
        request,
        ok=False,
        status=STATUS_PENDING,
        disposition=DISPOSITION_IN_PROGRESS,
        error=error_value(STATUS_PENDING, "request is already in progress"),
    )


def _answer_in_progress(
    request: CommandRequest,
    *,
    receipt_reserved: bool = False,
) -> CommandEnvelope:
    return CommandEnvelope.from_result(
        request,
        ok=False,
        status=STATUS_ANSWER_IN_PROGRESS,
        disposition=(
            DISPOSITION_IN_PROGRESS
            if receipt_reserved
            else DISPOSITION_NO_RECEIPT
        ),
        error=error_value(
            STATUS_ANSWER_IN_PROGRESS,
            "another request is currently answering this decision",
        ),
    )


def _duplicate_request(request: CommandRequest) -> CommandEnvelope:
    return CommandEnvelope.from_result(
        request,
        ok=False,
        status=STATUS_DUPLICATE_REQUEST,
        disposition=DISPOSITION_TERMINAL_REJECTED,
        error=error_value(
            STATUS_DUPLICATE_REQUEST,
            "request_id reused with a different canonical mutation",
        ),
    )


def _pending_changed_envelope(request: CommandRequest) -> CommandEnvelope:
    return CommandEnvelope.from_result(
        request,
        ok=False,
        status=STATUS_STALE_TARGET,
        error=error_value(STATUS_STALE_TARGET, _PENDING_CHANGED_MESSAGE),
    )


def _pending_public_result(
    request: CommandRequest,
    claim: Any,
    *,
    delivery_state: str,
) -> dict[str, Any]:
    params = request.params or {}
    result: dict[str, Any] = {
        "target": {"worker_id": claim.worker_id},
        "pending": {
            "id": params.get("pending_id"),
            "fingerprint": params.get("pending_fingerprint"),
        },
        "choice": {"choice_id": params.get("choice_id")},
        "delivery_state": delivery_state,
    }
    if delivery_state == "submitted":
        result.update(
            {
                "transport_state": "submitted",
                "observed_pending_state": "pending_observation",
            }
        )
    return result


def _pending_claim_has_exact_route(claim: Any) -> bool:
    return (
        isinstance(getattr(claim, "worker_id", None), str)
        and bool(claim.worker_id)
        and isinstance(getattr(claim, "worker_fingerprint", None), str)
        and bool(claim.worker_fingerprint)
        and isinstance(getattr(claim, "binding_private_fingerprint", None), str)
        and bool(claim.binding_private_fingerprint)
        and isinstance(getattr(claim, "turn_target_value", None), str)
        and bool(claim.turn_target_value.strip())
        and not isinstance(getattr(claim, "picker_ordinal", None), bool)
        and isinstance(claim.picker_ordinal, int)
        and claim.picker_ordinal >= 1
    )


def _same_pending_route(left: Any, right: Any) -> bool:
    return _pending_claim_has_exact_route(left) and _pending_claim_has_exact_route(right) and (
        left.worker_id,
        left.worker_fingerprint,
        left.binding_private_fingerprint,
        left.turn_target_value,
        left.picker_ordinal,
    ) == (
        right.worker_id,
        right.worker_fingerprint,
        right.binding_private_fingerprint,
        right.turn_target_value,
        right.picker_ordinal,
    )


def _decision_failure_envelope(
    request: CommandRequest,
    status: str,
) -> CommandEnvelope:
    messages = {
        STATUS_ANSWER_IN_PROGRESS: "another request is currently answering this decision",
        STATUS_DECISION_NOT_PENDING: "decision is not the worker's current pending decision",
        STATUS_UNKNOWN_WORKER: "target worker does not exist or is not open",
        STATUS_INVALID_SELECTION: "selection is invalid for the current decision",
        STATUS_UNSUPPORTED_DECISION: "multi-question decisions are not supported",
    }
    return CommandEnvelope.from_result(
        request,
        ok=False,
        status=status,
        error=error_value(status, messages[status]),
    )


def _decision_claim_has_exact_route(claim: Any) -> bool:
    return (
        isinstance(getattr(claim, "worker_id", None), str)
        and bool(claim.worker_id)
        and isinstance(getattr(claim, "worker_fingerprint", None), str)
        and bool(claim.worker_fingerprint)
        and isinstance(getattr(claim, "binding_private_fingerprint", None), str)
        and bool(claim.binding_private_fingerprint)
        and isinstance(getattr(claim, "turn_target_value", None), str)
        and bool(claim.turn_target_value.strip())
        and isinstance(getattr(claim, "decision_ref", None), str)
        and bool(claim.decision_ref)
        and getattr(claim, "decision_kind", None) in {"single", "multi", "plan"}
        and isinstance(getattr(claim, "option_count", None), int)
        and not isinstance(claim.option_count, bool)
        and claim.option_count >= 1
        and isinstance(getattr(claim, "option_refs", None), tuple)
        and getattr(claim, "route_kind", None) in {"legacy", "acp_permission"}
        and (
            (claim.text is None and bool(claim.option_refs))
            or (
                isinstance(claim.text, str)
                and bool(claim.text)
                and not claim.option_refs
            )
        )
    )


def _same_decision_route(left: Any, right: Any) -> bool:
    return (
        _decision_claim_has_exact_route(left)
        and _decision_claim_has_exact_route(right)
        and (
            left.worker_id,
            left.worker_fingerprint,
            left.binding_private_fingerprint,
            left.turn_target_value,
            left.decision_ref,
            left.decision_kind,
            left.option_count,
            left.option_refs,
            left.text,
            left.route_kind,
        )
        == (
            right.worker_id,
            right.worker_fingerprint,
            right.binding_private_fingerprint,
            right.turn_target_value,
            right.decision_ref,
            right.decision_kind,
            right.option_count,
            right.option_refs,
            right.text,
            right.route_kind,
        )
    )


def _decision_uses_acp_binding(_config: Config, decision: Any) -> bool:
    """Classify from durable provenance, independent of current liveness."""
    return getattr(decision, "route_kind", "legacy") == "acp_permission"


class PreSendCertainty(Enum):
    """How a pre-send failure must be classified before any external mutation.

    The distinction is which stage's evidence produced the failure, not the
    status text. An authoritative snapshot observation or proven target
    unsuitability is deterministic and may terminalize; a failed local or
    backend *operation* proves nothing durable and must stay retryable.
    """

    #: Proven target unsuitability -- a disallowed worker status, an unavailable
    #: backend, a missing/stale/ambiguous private binding, or a definite backend
    #: answer (Herdr rejection, unsupported target, no resolvable pane). A durable
    #: rejection is justified and a same-ID retry replays it.
    PERMANENT = "permanent"
    #: A local or backend operation failed before any send: the binding store,
    #: socket connect, pane-resolution read, or receipt-store open raised. No
    #: external mutation began and no durable authority exists, so the request ID
    #: stays retryable with no receipt written.
    SAFE_TRANSIENT = "safe_transient"


@dataclass(frozen=True)
class PreSendFailure:
    """A classified failure that occurred before any external mutation began."""

    envelope: CommandEnvelope
    certainty: PreSendCertainty

    @property
    def is_transient(self) -> bool:
        return self.certainty is PreSendCertainty.SAFE_TRANSIENT


def _permanent_pre_send(envelope: CommandEnvelope) -> PreSendFailure:
    return PreSendFailure(envelope=envelope, certainty=PreSendCertainty.PERMANENT)


def _safe_transient_pre_send(envelope: CommandEnvelope) -> PreSendFailure:
    return PreSendFailure(envelope=envelope, certainty=PreSendCertainty.SAFE_TRANSIENT)


def _close_socket_client(client: Any | None) -> None:
    if client is None or not hasattr(client, "close"):
        return
    try:
        client.close()
    except Exception:
        pass


def _abandon_pending_claim(config: Config, claim_token: str | None) -> bool:
    if config.db_path is None or not claim_token:
        return False
    try:
        return abandon_backend_pending_choice_claim(
            config.db_path,
            config.host_id,
            claim_token,
        )
    except Exception:
        return False


def _abandon_request_reservation(
    config: Config,
    request: CommandRequest,
    reservation: ReservedCommandMutation,
) -> bool:
    if config.db_path is None:
        return False
    try:
        return abandon_command_request_reservation(
            config.db_path,
            host_id=config.host_id,
            request_id=request.request_id or "",
            canonical_fingerprint=reservation.canonical.fingerprint,
            owner_token=reservation.owner_token,
        )
    except Exception:
        return False


def _connect_socket(
    config: Config,
    request: CommandRequest,
    socket_client_factory: SocketClientFactory | None,
) -> Any | CommandEnvelope:
    factory = socket_client_factory or _default_socket_client_factory
    client: Any | None = None
    try:
        client = factory(config)
        if not hasattr(client, "request"):
            raise TypeError("socket client does not expose generic request")
        if hasattr(client, "connect"):
            client.connect()
        return client
    except Exception:  # noqa: BLE001
        _close_socket_client(client)
        return _backend_unavailable(request, "Herdr socket could not be reached")


def _resolve_private_pane(
    config: Config,
    request: CommandRequest,
    client: Any,
    binding: WorkerBinding,
) -> str | PreSendFailure:
    try:
        pane_id = _private_pane_id_for_binding(
            client,
            binding,
            timeout=config.herdr_timeout_seconds,
        )
    except Exception as exc:  # noqa: BLE001
        from .backends.herdr_protocol import HerdrErrorResponse, HerdrProtocolError
        from .backends.herdr_socket import (
            HerdrSocketConnectionError,
            HerdrSocketDisconnectedError,
            HerdrSocketTimeoutError,
        )

        # A definite error response from Herdr is an authoritative answer that
        # this target cannot be resolved; a same-ID retry would get the same
        # answer, so it terminalizes rather than looping to the retry horizon.
        # It is checked before the transport branch because HerdrErrorResponse
        # subclasses HerdrProtocolError and must not be mistaken for framing loss.
        if isinstance(exc, HerdrErrorResponse):
            return _permanent_pre_send(
                _backend_failure(
                    request,
                    "Herdr socket could not resolve the private send target",
                )
            )
        # A transport read that could not complete -- timeout, disconnect,
        # connection loss, protocol framing, or an OS-level socket error --
        # proves nothing about the target and never began a send, so it stays
        # retryable.
        if isinstance(
            exc,
            HerdrSocketConnectionError
            | HerdrSocketTimeoutError
            | HerdrSocketDisconnectedError
            | HerdrProtocolError,
        ) or isinstance(exc, OSError):
            return _safe_transient_pre_send(
                _backend_unavailable(
                    request,
                    "Herdr socket could not resolve the private send target",
                )
            )
        # A malformed or unsupported resolution response is a proven target
        # property, not a transient operation failure.
        if isinstance(exc, (TypeError, ValueError)):
            return _permanent_pre_send(
                _backend_failure(
                    request,
                    "Herdr socket private send target is unsupported",
                )
            )
        # An unclassifiable resolution error is not proven safe to retry, so it
        # retains the prior terminal behavior rather than looping indefinitely.
        return _permanent_pre_send(
            _backend_failure(
                request,
                "Herdr socket private send target resolution failed",
            )
        )
    if not pane_id:
        # Herdr answered, and the answer is that the target has no resolvable
        # pane. That is authoritative target unsuitability, not a read failure.
        return _permanent_pre_send(
            _backend_failure(request, "Herdr socket private send target has no pane")
        )
    return pane_id


def _transition_payload(
    request: CommandRequest,
    *,
    worker_id: str,
    envelope: CommandEnvelope | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": 1,
        "action": request.action,
        "request_id": request.request_id,
        "target": {"worker_id": worker_id},
    }
    if envelope is not None:
        payload["envelope"] = envelope.to_dict()
    return payload


def _receipt_is_canonical(
    request: CommandRequest,
    canonical: CanonicalMutation,
    receipt: Mapping[str, Any],
) -> bool:
    version = receipt.get("canonical_version")
    common_identity = (
        isinstance(version, int)
        and not isinstance(version, bool)
        and receipt.get("request_id") == request.request_id
        and receipt.get("action") == canonical.action
    )
    if not common_identity:
        return False
    if version == 0:
        return (
            receipt.get("legacy_collision") is False
            and receipt.get("canonical_fingerprint") == request.payload_fingerprint()
        )
    return (
        version == canonical.canonical_version
        and receipt.get("canonical_fingerprint") == canonical.fingerprint
        and receipt.get("canonical_request_json") == canonical.canonical_json
        and receipt.get("public_worker_id") == canonical.public_worker_id
    )


def _stored_terminal_envelope(
    request: CommandRequest,
    receipt: Mapping[str, Any],
) -> CommandEnvelope:
    malformed = "stored request result is malformed; not retrying mutation"
    try:
        data = json.loads(receipt["result_json"])
    except (KeyError, TypeError, json.JSONDecodeError):
        return _backend_uncertain(
            request,
            "stored request result is unreadable; not retrying mutation",
        )
    if not isinstance(data, dict):
        return _backend_uncertain(
            request,
            "stored request result is unreadable; not retrying mutation",
        )

    state = receipt.get("state")
    if state == "accepted":
        expected_disposition = DISPOSITION_TERMINAL_ACCEPTED
    elif state == "rejected":
        expected_disposition = DISPOSITION_TERMINAL_REJECTED
    elif state == "uncertain":
        expected_disposition = DISPOSITION_TERMINAL_UNCERTAIN
    else:
        return _backend_uncertain(request, malformed)

    schema_version = data.get("schema_version")
    try:
        if type(schema_version) is not int:
            raise ValueError("stored envelope schema_version must be an exact integer")
        if schema_version == COMMAND_ENVELOPE_SCHEMA_VERSION:
            envelope = CommandEnvelope.from_dict(data)
        elif schema_version == 1:
            legacy_fields = {
                "schema_version",
                "action",
                "request_id",
                "ok",
                "dry_run",
                "status",
                "result",
                "error",
                "warnings",
            }
            if set(data) != legacy_fields:
                raise ValueError("legacy envelope has an invalid field set")
            upgraded = dict(data)
            upgraded["schema_version"] = COMMAND_ENVELOPE_SCHEMA_VERSION
            upgraded["disposition"] = expected_disposition
            envelope = CommandEnvelope.from_dict(upgraded)
            roundtrip = envelope.to_dict()
            roundtrip.pop("disposition")
            roundtrip["schema_version"] = 1
            if roundtrip != data:
                raise ValueError("legacy envelope is not an exact public roundtrip")
        else:
            raise ValueError("unsupported stored envelope schema")
    except (TypeError, ValueError):
        return _backend_uncertain(request, malformed)

    status = receipt.get("status")
    valid_identity = (
        envelope.action == request.action
        and envelope.request_id == request.request_id
        and envelope.dry_run is False
        and envelope.status == status
        and envelope.disposition == expected_disposition
    )
    valid_terminal = (
        state == "accepted"
        and status == STATUS_ACCEPTED
        and envelope.ok is True
        or state == "rejected"
        and status
        not in {STATUS_PENDING, STATUS_ACCEPTED, STATUS_REQUEST_STATE_UNCERTAIN}
        and envelope.ok is False
        or state == "uncertain"
        and status == STATUS_REQUEST_STATE_UNCERTAIN
        and envelope.ok is False
    )
    if not valid_identity or not valid_terminal:
        return _backend_uncertain(
            request,
            "stored request result is inconsistent; not retrying mutation",
        )
    return envelope


def _stored_in_progress_envelope(
    request: CommandRequest,
    receipt: Mapping[str, Any],
) -> CommandEnvelope:
    try:
        data = json.loads(receipt["result_json"])
        envelope = CommandEnvelope.from_dict(data)
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        return _request_in_progress(request)
    if (
        envelope.action != request.action
        or envelope.request_id != request.request_id
        or envelope.dry_run is not False
        or envelope.ok is not False
        or envelope.status != STATUS_PENDING
        or envelope.disposition != DISPOSITION_IN_PROGRESS
        or receipt.get("status") != STATUS_PENDING
    ):
        return _request_in_progress(request)
    return envelope


def _submission_verdict(envelope: CommandEnvelope) -> str:
    result = envelope.result
    if not isinstance(result, Mapping):
        return ""
    verdict = result.get("submission_verdict")
    return verdict if isinstance(verdict, str) else ""


def _envelope_from_receipt(
    request: CommandRequest,
    canonical: CanonicalMutation,
    receipt: Any,
) -> CommandEnvelope:
    if not isinstance(receipt, Mapping):
        return _backend_uncertain(
            request,
            "stored request receipt is missing or malformed; not retrying mutation",
        )
    required = {
        "request_id",
        "action",
        "canonical_version",
        "canonical_fingerprint",
        "canonical_request_json",
        "public_worker_id",
        "state",
        "status",
        "result_json",
        "legacy_collision",
    }
    if not required.issubset(receipt) or receipt.get("legacy_collision") is not False:
        return _backend_uncertain(
            request,
            "stored request receipt is malformed; not retrying mutation",
        )
    if not _receipt_is_canonical(request, canonical, receipt):
        return _duplicate_request(request)
    state = receipt.get("state")
    if state in {"reserved", "send_started"}:
        if receipt.get("status") != STATUS_PENDING:
            return _backend_uncertain(
                request,
                "stored request receipt is inconsistent; not retrying mutation",
            )
        return _stored_in_progress_envelope(request, receipt)
    if state == "uncertain":
        if receipt.get("status") != STATUS_REQUEST_STATE_UNCERTAIN:
            return _backend_uncertain(
                request,
                "stored request receipt is inconsistent; not retrying mutation",
            )
        stored = _stored_terminal_envelope(request, receipt)
        if (
            request.action == "send_instruction"
            and _submission_verdict(stored)
        ):
            return stored
        return _backend_uncertain(
            request,
            "previous request state is uncertain; not retrying mutation",
        )
    if state in {"accepted", "rejected"}:
        return _stored_terminal_envelope(request, receipt)
    return _backend_uncertain(
        request,
        "stored request receipt has an illegal state; not retrying mutation",
    )


@dataclass(frozen=True)
class ReservedCommandMutation:
    canonical: CanonicalMutation
    owner_token: str

@dataclass(frozen=True)
class PreparedInstructionMutation:
    client: Any
    pane_id: str
    binding_fingerprint: str


def _prepare_instruction(
    config: Config,
    request: CommandRequest,
    worker: Worker,
    *,
    socket_client_factory: SocketClientFactory | None,
) -> PreparedInstructionMutation | PreSendFailure:
    assert config.db_path is not None
    try:
        bindings = list_worker_bindings(
            config.db_path,
            config.host_id,
            backend=HERDR_BACKEND,
        )
    except Exception:
        # The binding store raised. This is an operation failure, not a proven
        # target property, and no send began: stay retryable under the same ID.
        return _safe_transient_pre_send(
            _backend_unavailable(request, "private binding store is unavailable")
        )
    resolved = _binding_for_worker(request, worker, bindings)
    if isinstance(resolved, CommandEnvelope):
        # A missing, stale, or ambiguous binding read from current data is a
        # proven target property; a same-ID retry would resolve it the same way.
        return _permanent_pre_send(resolved)

    binding_fingerprint = str(resolved.binding.private_fingerprint or "").strip()
    if not binding_fingerprint:
        return _permanent_pre_send(
            _binding_error(
                request,
                STATUS_BACKEND_UNSUPPORTED,
                "target private binding has no durable identity",
            )
        )

    # A socket that cannot be reached is an operation failure before any send,
    # so it stays retryable rather than burning the request ID.
    client_or_error = _connect_socket(config, request, socket_client_factory)
    if isinstance(client_or_error, CommandEnvelope):
        return _safe_transient_pre_send(client_or_error)
    client = client_or_error
    # Pane resolution classifies its own outcome: a transport read failure is a
    # safe transient, while a definite backend answer (rejection, unsupported,
    # or no pane) is a proven-permanent target failure.
    pane_or_error = _resolve_private_pane(
        config,
        request,
        client,
        resolved.binding,
    )
    if isinstance(pane_or_error, PreSendFailure):
        _close_socket_client(client)
        return pane_or_error
    return PreparedInstructionMutation(
        client=client,
        pane_id=pane_or_error,
        binding_fingerprint=binding_fingerprint,
    )


def _reserve_canonical_request(
    config: Config,
    request: CommandRequest,
    canonical: CanonicalMutation,
) -> ReservedCommandMutation | CommandEnvelope:
    if config.db_path is None:
        return _backend_unavailable(request, "command receipt store is unavailable")
    pending = _request_in_progress(request)
    try:
        reservation = reserve_command_request(
            config.db_path,
            host_id=config.host_id,
            request_id=request.request_id or "",
            action=canonical.action,
            canonical_version=canonical.canonical_version,
            canonical_fingerprint=canonical.fingerprint,
            canonical_request_json=canonical.canonical_json,
            public_worker_id=canonical.public_worker_id,
            pending_result_json=envelope_to_receipt_json(pending),
            selector_proof=_selector_proof(request),
            legacy_raw_payload_fingerprint=request.payload_fingerprint(),
        )
    except Exception:  # noqa: BLE001
        try:
            receipt = get_command_request(
                config.db_path,
                config.host_id,
                request.request_id or "",
            )
        except Exception:
            receipt = None
        if receipt is not None:
            return _envelope_from_receipt(request, canonical, receipt)
        return _backend_unavailable(request, "command receipt store is unavailable")

    if not isinstance(reservation, Mapping):
        return _recover_request(config, request, canonical)
    status = reservation.get("status")
    if status == "request_id_conflict":
        return _duplicate_request(request)
    if status != "reserved":
        return _envelope_from_receipt(
            request,
            canonical,
            reservation.get("receipt"),
        )
    receipt = reservation.get("receipt")
    owner_token = reservation.get("owner_token")
    if (
        not isinstance(receipt, Mapping)
        or not _receipt_is_canonical(request, canonical, receipt)
        or receipt.get("state") != "reserved"
        or receipt.get("status") != STATUS_PENDING
        or not isinstance(owner_token, str)
        or not owner_token
    ):
        return _recover_request(config, request, canonical)
    return ReservedCommandMutation(canonical=canonical, owner_token=owner_token)


def _recover_request(
    config: Config,
    request: CommandRequest,
    canonical: CanonicalMutation,
) -> CommandEnvelope:
    if config.db_path is None:
        return _backend_uncertain(request, "command request state could not be recovered")
    try:
        receipt = get_command_request(
            config.db_path,
            config.host_id,
            request.request_id or "",
        )
    except Exception:
        receipt = None
    return _negotiated_submission_envelope(
        config,
        request,
        _envelope_from_receipt(request, canonical, receipt),
    )


def _terminal_envelope(
    request: CommandRequest,
    envelope: CommandEnvelope,
    terminal_state: str,
) -> CommandEnvelope:
    dispositions = {
        "accepted": DISPOSITION_TERMINAL_ACCEPTED,
        "rejected": DISPOSITION_TERMINAL_REJECTED,
        "uncertain": DISPOSITION_TERMINAL_UNCERTAIN,
    }
    try:
        disposition = dispositions[terminal_state]
    except KeyError as exc:
        raise ValueError("invalid terminal command state") from exc
    return CommandEnvelope.from_result(
        request,
        ok=envelope.ok,
        status=envelope.status,
        disposition=disposition,
        result=envelope.result,
        error=envelope.error,
        warnings=envelope.warnings,
    )


def _finish_request(
    config: Config,
    request: CommandRequest,
    reservation: ReservedCommandMutation,
    envelope: CommandEnvelope,
    *,
    expected_state: str,
    terminal_state: str,
    terminal_effect: Callable[[Any], Any] | None = None,
) -> CommandEnvelope:
    if config.db_path is None:
        return _backend_uncertain(request, "command receipt store is unavailable")
    try:
        terminal = _terminal_envelope(request, envelope, terminal_state)
        finished = finish_command_request(
            config.db_path,
            host_id=config.host_id,
            request_id=request.request_id or "",
            canonical_fingerprint=reservation.canonical.fingerprint,
            owner_token=reservation.owner_token,
            expected_state=expected_state,
            terminal_state=terminal_state,
            status=terminal.status,
            result_json=envelope_to_receipt_json(terminal),
            event_payload=_transition_payload(
                request,
                worker_id=reservation.canonical.public_worker_id,
                envelope=terminal,
            ),
            terminal_effect=terminal_effect,
        )
    except Exception:  # noqa: BLE001
        return _recover_request(
            config,
            request,
            reservation.canonical,
        )
    if not isinstance(finished, Mapping):
        return _recover_request(
            config,
            request,
            reservation.canonical,
        )
    return _envelope_from_receipt(
        request,
        reservation.canonical,
        finished.get("receipt"),
    )


def _finish_before_send(
    config: Config,
    request: CommandRequest,
    reservation: ReservedCommandMutation,
    envelope: CommandEnvelope,
) -> CommandEnvelope:
    terminal_state = (
        "uncertain"
        if envelope.status == STATUS_REQUEST_STATE_UNCERTAIN
        else "rejected"
    )
    return _finish_request(
        config,
        request,
        reservation,
        envelope,
        expected_state="reserved",
        terminal_state=terminal_state,
    )

def _reserve_terminal_replay(
    config: Config,
    request: CommandRequest,
    canonical: CanonicalMutation,
    previous_receipt: Mapping[str, Any],
    replay_envelope: CommandEnvelope,
) -> CommandEnvelope:
    if config.db_path is None:
        return _backend_uncertain(request, "command receipt store is unavailable")
    if (
        previous_receipt.get("state") == "rejected"
        and replay_envelope.ok is False
        and replay_envelope.status
        not in {STATUS_PENDING, STATUS_ACCEPTED, STATUS_REQUEST_STATE_UNCERTAIN}
    ):
        terminal = replay_envelope
        terminal_state = "rejected"
    else:
        terminal = _backend_uncertain(
            request,
            "stored request evidence disappeared during replay; not retrying mutation",
        )
        terminal_state = "uncertain"
    try:
        replay = reserve_terminal_command_replay(
            config.db_path,
            host_id=config.host_id,
            request_id=request.request_id or "",
            action=canonical.action,
            canonical_version=canonical.canonical_version,
            canonical_fingerprint=canonical.fingerprint,
            canonical_request_json=canonical.canonical_json,
            public_worker_id=canonical.public_worker_id,
            terminal_state=terminal_state,
            status=terminal.status,
            result_json=envelope_to_receipt_json(terminal),
            # Preserve the original spelling's evidence. This caller may have
            # proven equivalence with a different one, and overwriting it would
            # strand a later retry of the request as it was actually issued.
            selector_proof=_stored_selector_proof(previous_receipt),
            legacy_raw_payload_fingerprint=request.payload_fingerprint(),
            event_payload=_transition_payload(
                request,
                worker_id=canonical.public_worker_id,
                envelope=terminal,
            ),
        )
    except Exception:  # noqa: BLE001
        return _recover_request(config, request, canonical)
    if not isinstance(replay, Mapping):
        return _recover_request(config, request, canonical)
    return _envelope_from_receipt(
        request,
        canonical,
        replay.get("receipt"),
    )


def _mark_request_send_started(
    config: Config,
    request: CommandRequest,
    reservation: ReservedCommandMutation,
    *,
    binding_fingerprint: str,
    worker: Worker | None = None,
    instruction_text: str | None = None,
) -> CommandEnvelope | Mapping[str, Any] | None:
    if config.db_path is None:
        return _backend_uncertain(request, "command receipt store is unavailable")
    try:
        started = mark_command_send_started(
            config.db_path,
            host_id=config.host_id,
            request_id=request.request_id or "",
            canonical_fingerprint=reservation.canonical.fingerprint,
            owner_token=reservation.owner_token,
            binding_fingerprint=binding_fingerprint,
            send_started_effect=None,
            submission_worker=worker,
            instruction_text=instruction_text,
            submission_link_window_seconds=(
                config.submission_link_window_seconds
            ),
            submission_hard_ttl_seconds=config.submission_hard_ttl_seconds,
            event_payload=_transition_payload(
                request,
                worker_id=reservation.canonical.public_worker_id,
            ),
        )
    except Exception:  # noqa: BLE001
        return _recover_request(
            config,
            request,
            reservation.canonical,
        )
    if (
        isinstance(started, Mapping)
        and started.get("status") == "send_started"
        and started.get("owner_token") == reservation.owner_token
        and isinstance(started.get("receipt"), Mapping)
        and started["receipt"].get("state") == "send_started"
        and _receipt_is_canonical(request, reservation.canonical, started["receipt"])
    ):
        if worker is None:
            return None
        linked = linked_turn_for_submission(
            config.db_path,
            host_id=config.host_id,
            request_id=request.request_id or "",
        )
        return linked or {"id": None}
    if isinstance(started, Mapping) and isinstance(started.get("receipt"), Mapping):
        embedded = _envelope_from_receipt(
            request,
            reservation.canonical,
            started["receipt"],
        )
        if embedded.status != STATUS_REQUEST_STATE_UNCERTAIN:
            return embedded
        if started["receipt"].get("state") in {"send_started", "uncertain"}:
            return embedded
    return _recover_request(
        config,
        request,
        reservation.canonical,
    )


def _accepted_send_envelope(
    request: CommandRequest,
    worker: Worker,
    turn: Mapping[str, Any],
    *,
    submission_verdict: str = "submitted",
) -> CommandEnvelope:
    observed_turn_state = "pending_observation"
    if str(turn.get("source_turn_id") or "").strip():
        observed_turn_state = (
            "complete" if turn.get("complete") is True else "observed"
        )
    raw_turn_id = turn.get("id")
    turn_id = raw_turn_id if isinstance(raw_turn_id, str) and raw_turn_id else None
    return CommandEnvelope.from_result(
        request,
        ok=True,
        status=STATUS_ACCEPTED,
        disposition=DISPOSITION_TERMINAL_ACCEPTED,
        result={
            "target": {"worker_id": worker.id},
            "delivery_state": "submitted",
            "transport_state": "submitted",
            "target_state_at_send": _target_state_at_send(worker),
            "turn_id": turn_id,
            "observed_turn_state": observed_turn_state,
            "submission_verdict": submission_verdict,
        },
    )


def _queued_send_envelope(
    request: CommandRequest,
    worker: Worker,
) -> CommandEnvelope:
    return CommandEnvelope.from_result(
        request,
        ok=False,
        status=STATUS_PENDING,
        disposition=DISPOSITION_IN_PROGRESS,
        result={
            "target": {"worker_id": worker.id},
            "delivery_state": "queued",
            "transport_state": "queued",
            "target_state_at_send": _target_state_at_send(worker),
            "turn_id": None,
            "observed_turn_state": "pending_observation",
            "submission_verdict": "written_to_pty",
        },
        error=error_value(
            STATUS_PENDING,
            "instruction is queued for the next agent turn boundary",
        ),
    )


def _instruction_rejected_envelope(
    request: CommandRequest,
    worker: Worker,
    *,
    verdict: str,
) -> CommandEnvelope:
    return CommandEnvelope.from_result(
        request,
        ok=False,
        status=STATUS_REJECTED,
        disposition=DISPOSITION_TERMINAL_REJECTED,
        result={
            "target": {"worker_id": worker.id},
            "delivery_state": "not_delivered",
            "transport_state": "not_submitted",
            "target_state_at_send": _target_state_at_send(worker),
            "submission_verdict": verdict,
        },
        error=error_value(STATUS_REJECTED, "instruction was not delivered"),
    )


def _instruction_uncertain_envelope(
    request: CommandRequest,
    worker: Worker,
    *,
    verdict: str,
) -> CommandEnvelope:
    return CommandEnvelope.from_result(
        request,
        ok=False,
        status=STATUS_REQUEST_STATE_UNCERTAIN,
        disposition=DISPOSITION_TERMINAL_UNCERTAIN,
        result={
            "target": {"worker_id": worker.id},
            "delivery_state": "unknown",
            "transport_state": "unknown",
            "target_state_at_send": _target_state_at_send(worker),
            "submission_verdict": verdict,
        },
        error=error_value(
            STATUS_REQUEST_STATE_UNCERTAIN,
            "instruction delivery is unknown; not retrying mutation",
        ),
    )


def _recovered_unknown_send_envelope(
    request: CommandRequest,
    *,
    worker_id: str,
) -> CommandEnvelope:
    return CommandEnvelope.from_result(
        request,
        ok=False,
        status=STATUS_REQUEST_STATE_UNCERTAIN,
        disposition=DISPOSITION_TERMINAL_UNCERTAIN,
        result={
            "target": {"worker_id": worker_id},
            "delivery_state": "unknown",
            "transport_state": "unknown",
            "submission_verdict": "unknown",
        },
        error=error_value(
            STATUS_REQUEST_STATE_UNCERTAIN,
            "instruction delivery is unknown after process recovery; not retrying mutation",
        ),
    )


def _accepted_queued_send_envelope(
    request: CommandRequest,
    queued: CommandEnvelope,
    turn: Mapping[str, Any],
) -> CommandEnvelope:
    queued_result = queued.result if isinstance(queued.result, Mapping) else {}
    raw_turn_id = turn.get("id")
    turn_id = raw_turn_id if isinstance(raw_turn_id, str) and raw_turn_id else None
    observed_turn_state = (
        "complete"
        if turn.get("complete") is True
        else "observed"
        if str(turn.get("source_turn_id") or "").strip()
        else "pending_observation"
    )
    return CommandEnvelope.from_result(
        request,
        ok=True,
        status=STATUS_ACCEPTED,
        disposition=DISPOSITION_TERMINAL_ACCEPTED,
        result={
            "target": queued_result.get("target"),
            "delivery_state": "submitted",
            "transport_state": "queued",
            "target_state_at_send": queued_result.get("target_state_at_send"),
            "turn_id": turn_id,
            "observed_turn_state": observed_turn_state,
            "submission_verdict": "written_to_pty",
        },
    )


def _unverified_queued_send_envelope(
    request: CommandRequest,
    queued: CommandEnvelope,
) -> CommandEnvelope:
    queued_result = queued.result if isinstance(queued.result, Mapping) else {}
    return CommandEnvelope.from_result(
        request,
        ok=False,
        status=STATUS_REQUEST_STATE_UNCERTAIN,
        disposition=DISPOSITION_TERMINAL_UNCERTAIN,
        result={
            "target": queued_result.get("target"),
            "delivery_state": "unknown",
            "transport_state": "unknown",
            "target_state_at_send": queued_result.get("target_state_at_send"),
            "turn_id": None,
            "submission_verdict": "written_to_pty",
        },
        error=error_value(
            STATUS_REQUEST_STATE_UNCERTAIN,
            (
                "instruction verification expired; delivery is unknown "
                "and will not be retried"
            ),
        ),
    )


def _record_queued_send(
    config: Config,
    request: CommandRequest,
    worker: Worker,
    reservation: ReservedCommandMutation,
    envelope: CommandEnvelope,
) -> CommandEnvelope:
    assert config.db_path is not None
    try:
        queued = record_command_send_queued(
            config.db_path,
            host_id=config.host_id,
            request_id=request.request_id or "",
            canonical_fingerprint=reservation.canonical.fingerprint,
            owner_token=reservation.owner_token,
            result_json=envelope_to_receipt_json(envelope),
            event_payload=_transition_payload(
                request,
                worker_id=worker.id,
                envelope=envelope,
            ),
        )
    except Exception:  # noqa: BLE001
        return _recover_request(config, request, reservation.canonical)
    if not isinstance(queued, Mapping):
        return _recover_request(config, request, reservation.canonical)
    return _envelope_from_receipt(
        request,
        reservation.canonical,
        queued.get("receipt"),
    )


def _submit_instruction(
    config: Config,
    request: CommandRequest,
    worker: Worker,
    reservation: ReservedCommandMutation,
    prepared: PreparedInstructionMutation,
) -> CommandEnvelope:
    assert config.db_path is not None
    try:
        send_started = _mark_request_send_started(
            config,
            request,
            reservation,
            binding_fingerprint=prepared.binding_fingerprint,
            worker=worker,
            instruction_text=_instruction_text(request),
        )
        if isinstance(send_started, CommandEnvelope):
            return send_started
        if not isinstance(send_started, Mapping):
            return _recover_request(
                config,
                request,
                reservation.canonical,
            )
        observed_turn = send_started

        try:
            response = _socket_request(
                prepared.client,
                "agent.prompt",
                {
                    # Herdr 0.7.5 deliberately restricts agent.prompt to a
                    # current pane id or a unique live agent name. Private
                    # bindings may instead be keyed by terminal id, so use the
                    # pane resolved and validated during the pre-send phase.
                    "target": prepared.pane_id,
                    "text": _instruction_text(request),
                    "wait": {
                        "until": ["working"],
                        "timeout_ms": max(
                            1,
                            int(config.herdr_timeout_seconds * 1000),
                        ),
                    },
                },
                timeout=config.herdr_timeout_seconds + 1.0,
            )
        except Exception as exc:  # noqa: BLE001
            verdict = _herdr_error_code(exc)
            if verdict in {
                "agent_not_ready",
                "agent_target_ambiguous",
                "agent_prompt_not_received",
                "agent_prompt_unsubmitted",
                "agent_input_pending",
            }:
                envelope = _instruction_rejected_envelope(
                    request,
                    worker,
                    verdict=verdict,
                )
                return _finish_request(
                    config,
                    request,
                    reservation,
                    envelope,
                    expected_state="send_started",
                    terminal_state="rejected",
                )
            if verdict == "agent_prompt_stalled":
                envelope = _instruction_uncertain_envelope(
                    request,
                    worker,
                    verdict=verdict,
                )
            else:
                envelope = _instruction_uncertain_envelope(
                    request,
                    worker,
                    verdict="unknown",
                )
            return _finish_request(
                config,
                request,
                reservation,
                envelope,
                expected_state="send_started",
                terminal_state="uncertain",
            )

        verdict = _agent_prompt_delivery(response)
        if verdict == "written_to_pty":
            return _record_queued_send(
                config,
                request,
                worker,
                reservation,
                _queued_send_envelope(request, worker),
            )
        if verdict != "submitted":
            return _finish_request(
                config,
                request,
                reservation,
                _instruction_uncertain_envelope(
                    request,
                    worker,
                    verdict="unknown",
                ),
                expected_state="send_started",
                terminal_state="uncertain",
            )

        # The observation may arrive while the pane call is in flight. Re-read
        # the durable link so the accepted envelope can report it immediately.
        try:
            refreshed_turn = linked_turn_for_submission(
                config.db_path,
                host_id=config.host_id,
                request_id=request.request_id or "",
            )
        except Exception:  # noqa: BLE001
            refreshed_turn = None
        if isinstance(refreshed_turn, Mapping):
            observed_turn = refreshed_turn
    finally:
        _close_socket_client(prepared.client)

    accepted = _accepted_send_envelope(
        request,
        worker,
        observed_turn,
        submission_verdict="submitted",
    )
    return _finish_request(
        config,
        request,
        reservation,
        accepted,
        expected_state="send_started",
        terminal_state="accepted",
    )


def _validate_pending_choice(
    config: Config,
    request: CommandRequest,
) -> Any | PreSendFailure:
    if config.db_path is None:
        return _safe_transient_pre_send(
            _backend_unavailable(request, "pending state store is unavailable")
        )
    params = request.params or {}
    try:
        validated = claim_backend_pending_choice(
            config.db_path,
            config.host_id,
            str(params.get("pending_id") or ""),
            str(params.get("pending_fingerprint") or ""),
            str(params.get("choice_id") or ""),
            claim=False,
        )
    except Exception:
        # The pending store raised; nothing was claimed or sent.
        return _safe_transient_pre_send(
            _backend_unavailable(request, "pending state store is unavailable")
        )
    if validated.status != "validated" or not _pending_claim_has_exact_route(validated):
        # The pending interaction provably changed or is no longer answerable.
        return _permanent_pre_send(_pending_changed_envelope(request))
    return validated


def _claim_pending_choice(
    config: Config,
    request: CommandRequest,
    validated: Any,
) -> Any | CommandEnvelope:
    assert config.db_path is not None
    params = request.params or {}
    try:
        claim = claim_backend_pending_choice(
            config.db_path,
            config.host_id,
            str(params.get("pending_id") or ""),
            str(params.get("pending_fingerprint") or ""),
            str(params.get("choice_id") or ""),
            claim=True,
        )
    except Exception:
        return _backend_uncertain(request, "pending answer claim state is uncertain")
    if (
        claim.status != "claimed"
        or not isinstance(getattr(claim, "claim_token", None), str)
        or not claim.claim_token
        or not _same_pending_route(validated, claim)
    ):
        return _pending_changed_envelope(request)
    return claim


def _uncertain_pending_effect(
    config: Config,
    claim_token: str,
) -> Callable[[Any], Any] | None:
    try:
        return backend_pending_choice_terminal_effect(
            host_id=config.host_id,
            claim_token=claim_token,
            accepted=False,
        )
    except Exception:
        return None


def _answer_pending(
    config: Config,
    request: CommandRequest,
    validated: Any,
    reservation: ReservedCommandMutation,
    client: Any,
) -> CommandEnvelope:
    assert config.db_path is not None
    claim = _claim_pending_choice(config, request, validated)
    if isinstance(claim, CommandEnvelope):
        _close_socket_client(client)
        return _finish_before_send(config, request, reservation, claim)
    claim_token = claim.claim_token

    send_start_error = _mark_request_send_started(
        config,
        request,
        reservation,
        binding_fingerprint=claim.binding_private_fingerprint,
    )
    if send_start_error is not None:
        _close_socket_client(client)
        claim_released = _abandon_pending_claim(config, claim_token)
        if send_start_error.status == STATUS_PENDING and not claim_released:
            return _finish_before_send(
                config,
                request,
                reservation,
                _backend_uncertain(
                    request,
                    "pending answer claim could not be safely released",
                ),
            )
        return send_start_error

    try:
        started = start_backend_pending_choice_send(
            config.db_path,
            config.host_id,
            claim_token,
        )
    except Exception:
        _close_socket_client(client)
        _abandon_pending_claim(config, claim_token)
        return _finish_request(
            config,
            request,
            reservation,
            _backend_uncertain(request, "pending answer start state is uncertain"),
            expected_state="send_started",
            terminal_state="uncertain",
            terminal_effect=_uncertain_pending_effect(config, claim_token),
        )
    if getattr(started, "status", None) != "started" or not _same_pending_route(claim, started):
        _close_socket_client(client)
        _abandon_pending_claim(config, claim_token)
        return _finish_request(
            config,
            request,
            reservation,
            _backend_uncertain(
                request,
                "pending answer state is uncertain after send start",
            ),
            expected_state="send_started",
            terminal_state="uncertain",
            terminal_effect=_uncertain_pending_effect(config, claim_token),
        )

    try:
        _submit_private_pane_input(
            client,
            started.turn_target_value.strip(),
            str(started.picker_ordinal),
            timeout=config.herdr_timeout_seconds,
        )
    except Exception:  # noqa: BLE001
        uncertain = _backend_uncertain(
            request,
            "Herdr socket pane input state is uncertain after send start",
        )
        return _finish_request(
            config,
            request,
            reservation,
            uncertain,
            expected_state="send_started",
            terminal_state="uncertain",
            terminal_effect=_uncertain_pending_effect(config, claim_token),
        )
    finally:
        _close_socket_client(client)

    accepted = CommandEnvelope.from_result(
        request,
        ok=True,
        status=STATUS_ACCEPTED,
        disposition=DISPOSITION_TERMINAL_ACCEPTED,
        result=_pending_public_result(request, started, delivery_state="submitted"),
    )
    try:
        effect = backend_pending_choice_terminal_effect(
            host_id=config.host_id,
            claim_token=claim_token,
            accepted=True,
        )
    except Exception:
        return _recover_request(
            config,
            request,
            reservation.canonical,
        )
    return _finish_request(
        config,
        request,
        reservation,
        accepted,
        expected_state="send_started",
        terminal_state="accepted",
        terminal_effect=effect,
    )


def _validate_pending_decision(
    config: Config,
    request: CommandRequest,
) -> Any | PreSendFailure:
    if config.db_path is None:
        return _safe_transient_pre_send(
            _backend_unavailable(request, "pending state store is unavailable")
        )
    params = request.params or {}
    target = request.target or {}
    try:
        validated = claim_backend_pending_decision(
            config.db_path,
            config.host_id,
            str(target.get("worker_id") or ""),
            str(params.get("decision_ref") or ""),
            params.get("selection")
            if isinstance(params.get("selection"), Mapping)
            else {},
            claim=False,
        )
    except Exception:
        return _safe_transient_pre_send(
            _backend_unavailable(request, "pending state store is unavailable")
        )
    if validated.status == "validated" and _decision_claim_has_exact_route(validated):
        return validated
    if validated.status == "acp_authority_unavailable":
        return _safe_transient_pre_send(
            _backend_unavailable(
                request,
                "ACP permission authority is temporarily unavailable",
            )
        )
    status = {
        "already_claimed": STATUS_ANSWER_IN_PROGRESS,
        "unknown_worker": STATUS_UNKNOWN_WORKER,
        "invalid_selection": STATUS_INVALID_SELECTION,
        "unsupported_decision": STATUS_UNSUPPORTED_DECISION,
    }.get(validated.status, STATUS_DECISION_NOT_PENDING)
    return _permanent_pre_send(_decision_failure_envelope(request, status))


def _claim_pending_decision(
    config: Config,
    request: CommandRequest,
    validated: Any,
) -> Any | CommandEnvelope:
    assert config.db_path is not None
    params = request.params or {}
    target = request.target or {}
    try:
        claim = claim_backend_pending_decision(
            config.db_path,
            config.host_id,
            str(target.get("worker_id") or ""),
            str(params.get("decision_ref") or ""),
            params.get("selection")
            if isinstance(params.get("selection"), Mapping)
            else {},
            claim=True,
        )
    except Exception:
        return _backend_uncertain(request, "pending decision claim state is uncertain")
    if (
        claim.status == "claimed"
        and isinstance(claim.claim_token, str)
        and claim.claim_token
    ):
        if _same_decision_route(validated, claim):
            return claim
    if claim.status == "acp_authority_unavailable":
        return _backend_unavailable(
            request,
            "ACP permission authority is temporarily unavailable",
        )
    status = {
        "already_claimed": STATUS_ANSWER_IN_PROGRESS,
        "unknown_worker": STATUS_UNKNOWN_WORKER,
        "invalid_selection": STATUS_INVALID_SELECTION,
        "unsupported_decision": STATUS_UNSUPPORTED_DECISION,
    }.get(claim.status, STATUS_DECISION_NOT_PENDING)
    return _decision_failure_envelope(request, status)


def _submit_decision_calibration(
    client: Any,
    pane_id: str,
    decision: Any,
    *,
    timeout: float,
) -> None:
    steps = calibrate_decision_steps(
        kind=decision.decision_kind,
        option_count=decision.option_count,
        option_refs=decision.option_refs,
        text=decision.text,
    )
    for step in steps:
        if step.operation == "keys":
            _socket_request(
                client,
                "pane.send_keys",
                {"pane_id": pane_id, "keys": list(step.keys)},
                timeout=timeout,
            )
        elif step.operation == "text":
            _socket_request(
                client,
                "pane.send_text",
                {"pane_id": pane_id, "text": step.text},
                timeout=timeout,
            )
        else:
            _socket_request(
                client,
                "pane.send_input",
                {"pane_id": pane_id, "text": step.text, "keys": list(step.keys)},
                timeout=timeout,
            )


def _decision_public_result(
    request: CommandRequest,
    claim: Any,
) -> dict[str, Any]:
    return {
        "target": {"worker_id": claim.worker_id},
        "decision": {"decision_ref": (request.params or {}).get("decision_ref")},
        "delivery_state": "submitted",
        "transport_state": "submitted",
        "observed_pending_state": "pending_observation",
    }


def _answer_decision(
    config: Config,
    request: CommandRequest,
    validated: Any,
    reservation: ReservedCommandMutation,
    client: Any,
    *,
    acp_permission_router: AcpPermissionDecisionRouter | None = None,
    acp_handoff: bool = False,
) -> CommandEnvelope:
    assert config.db_path is not None
    claim = _claim_pending_decision(config, request, validated)
    if isinstance(claim, CommandEnvelope):
        _close_socket_client(client)
        if claim.status == STATUS_ANSWER_IN_PROGRESS:
            _abandon_request_reservation(config, request, reservation)
            return _answer_in_progress(request, receipt_reserved=True)
        if claim.status == STATUS_BACKEND_UNAVAILABLE:
            if _abandon_request_reservation(config, request, reservation):
                return claim
            return _finish_before_send(
                config,
                request,
                reservation,
                _backend_uncertain(
                    request,
                    "ACP permission reservation state is uncertain",
                ),
            )
        return _finish_before_send(config, request, reservation, claim)
    claim_token = claim.claim_token

    send_start_error = _mark_request_send_started(
        config,
        request,
        reservation,
        binding_fingerprint=claim.binding_private_fingerprint,
    )
    if send_start_error is not None:
        _close_socket_client(client)
        claim_released = _abandon_pending_claim(config, claim_token)
        if send_start_error.status == STATUS_PENDING and not claim_released:
            return _finish_before_send(
                config,
                request,
                reservation,
                _backend_uncertain(
                    request,
                    "pending decision claim could not be safely released",
                ),
            )
        return send_start_error

    try:
        started = start_backend_pending_decision_send(
            config.db_path,
            config.host_id,
            claim_token,
        )
    except Exception:
        _close_socket_client(client)
        _abandon_pending_claim(config, claim_token)
        return _finish_request(
            config,
            request,
            reservation,
            _backend_uncertain(request, "pending decision start state is uncertain"),
            expected_state="send_started",
            terminal_state="uncertain",
            terminal_effect=_uncertain_pending_effect(config, claim_token),
        )
    if getattr(started, "status", None) != "started" or not _same_decision_route(claim, started):
        _close_socket_client(client)
        _abandon_pending_claim(config, claim_token)
        return _finish_request(
            config,
            request,
            reservation,
            _backend_uncertain(
                request,
                "pending decision state is uncertain after send start",
            ),
            expected_state="send_started",
            terminal_state="uncertain",
            terminal_effect=_uncertain_pending_effect(config, claim_token),
        )

    try:
        if acp_handoff:
            if acp_permission_router is None:
                raise RuntimeError("ACP permission bridge is unavailable")
            acp_permission_router.answer_permission_decision(
                started,
                timeout=config.acp_request_timeout_seconds,
            )
        else:
            _submit_decision_calibration(
                client,
                started.turn_target_value.strip(),
                started,
                timeout=config.herdr_timeout_seconds,
            )
    except Exception:  # noqa: BLE001
        return _finish_request(
            config,
            request,
            reservation,
            _backend_uncertain(
                request,
                "permission decision state is uncertain after send start",
            ),
            expected_state="send_started",
            terminal_state="uncertain",
            terminal_effect=_uncertain_pending_effect(config, claim_token),
        )
    finally:
        _close_socket_client(client)

    accepted = CommandEnvelope.from_result(
        request,
        ok=True,
        status=STATUS_ACCEPTED,
        disposition=DISPOSITION_TERMINAL_ACCEPTED,
        result=_decision_public_result(request, started),
    )
    try:
        effect = backend_pending_choice_terminal_effect(
            host_id=config.host_id,
            claim_token=claim_token,
            accepted=True,
        )
    except Exception:
        return _recover_request(config, request, reservation.canonical)
    return _finish_request(
        config,
        request,
        reservation,
        accepted,
        expected_state="send_started",
        terminal_state="accepted",
        terminal_effect=effect,
    )


def _execute_non_mutating(config: Config, request: CommandRequest) -> CommandEnvelope:
    if request.action == "noop":
        return execute_command(request, CommandContext(host_id=config.host_id, workers=[]))
    snapshot = _current_snapshot(config)
    return execute_command(
        request,
        CommandContext(
            host_id=config.host_id,
            workers=list(snapshot.workers),
            snapshot=snapshot,
        ),
    )
def _mutation_dry_run(request: CommandRequest) -> CommandEnvelope:
    """Preview a validated mutation without consulting mutable authority."""
    if request.action == "send_instruction":
        return CommandEnvelope.from_result(
            request,
            ok=True,
            status=STATUS_DRY_RUN,
            result={
                "target": dict(request.target or {}),
                "instruction": {"text": _instruction_text(request)},
            },
        )
    params = request.params or {}
    if request.action == "answer_decision":
        return CommandEnvelope.from_result(
            request,
            ok=True,
            status=STATUS_DRY_RUN,
            result={
                "target": dict(request.target or {}),
                "decision": {"decision_ref": params.get("decision_ref")},
                "delivery_state": "not_submitted",
            },
        )
    return CommandEnvelope.from_result(
        request,
        ok=True,
        status=STATUS_DRY_RUN,
        result={
            "pending": {
                "id": params.get("pending_id"),
                "fingerprint": params.get("pending_fingerprint"),
            },
            "choice": {"choice_id": params.get("choice_id")},
            "delivery_state": "not_submitted",
        },
    )


def _direct_replay_worker_id(request: CommandRequest) -> str | None:
    """Return an explicit public ID when no mutable selector must resolve."""
    target = request.target or {}
    if not set(target).issubset({"worker_id", "worker_fingerprint"}):
        return None
    worker_id = target.get("worker_id")
    if not isinstance(worker_id, str) or not worker_id.strip():
        return None
    return worker_id


@dataclass(frozen=True)
class _ReceiptTakeover:
    """An abandoned reservation this caller may re-drive to a terminal state."""

    public_worker_id: str


def _receipt_malformed(request: CommandRequest) -> CommandEnvelope:
    return _backend_uncertain(
        request,
        "stored request receipt is malformed; not retrying mutation",
    )


def _receipt_target_unprovable(request: CommandRequest) -> CommandEnvelope:
    return _backend_uncertain(
        request,
        "stored request target cannot be proven; not retrying mutation",
    )


def _selector_proof(request: CommandRequest) -> str:
    """Return the request's selector proof, or empty when none can be built."""
    try:
        return build_selector_proof(request)
    except (TypeError, ValueError):
        return ""


def _stored_selector_proof(receipt: Mapping[str, Any]) -> str:
    """Return the receipt's selector proof, or empty when it proves nothing.

    An absent, malformed, or unsupported-version proof is evidence this path
    cannot interpret, so it must decide nothing rather than decide wrongly.
    """
    proof = receipt.get("selector_proof")
    return proof if is_selector_proof(proof) else ""


def _proven_replay_worker_id(
    config: Config,
    request: CommandRequest,
    receipt: Mapping[str, Any],
    *,
    allow_current_authority: bool = True,
) -> str | CommandEnvelope | None:
    """Prove which public worker an existing receipt's retry belongs to.

    Returns the receipt's stored public worker ID when this retry is the same
    request, a fail-closed envelope when it provably is not, or None when no
    available evidence can decide. Stored evidence always outranks mutable
    authority, so a vanished or churned worker cannot hide a live receipt.
    """
    version = receipt.get("canonical_version")
    if isinstance(version, bool) or not isinstance(version, int) or version < 0:
        return _receipt_malformed(request)
    stored = receipt.get("public_worker_id")
    stored_worker_id = stored if isinstance(stored, str) and stored else ""

    # A v0 receipt is validated against the exact raw request payload, which
    # already pins the original selector spelling. It needs no resolution, and
    # a changed payload fails its canonical check rather than replaying here.
    if version == 0:
        return stored_worker_id or _LEGACY_V0_REPLAY_WORKER_ID
    if not stored_worker_id:
        return _receipt_malformed(request)
    if request.action == "answer_decision":
        explicit_worker_id = _direct_replay_worker_id(request)
        if explicit_worker_id != stored_worker_id:
            return _duplicate_request(request)
        return stored_worker_id
    if request.action != "send_instruction":
        return stored_worker_id

    # 1. An explicit worker ID names the canonical worker outright. A refreshed
    #    worker_fingerprint beside it stays noncanonical.
    explicit_worker_id = _direct_replay_worker_id(request)
    if explicit_worker_id is not None:
        if explicit_worker_id != stored_worker_id:
            return _duplicate_request(request)
        return stored_worker_id

    # 2. An exact selector proof recognizes the original spelling of a name or
    #    space alias even after the resolved worker left current authority.
    stored_proof = _stored_selector_proof(receipt)
    if stored_proof and stored_proof == _selector_proof(request):
        return stored_worker_id

    # 3. Only a current, healthy observation can prove that a different spelling
    #    names the same canonical worker. A degraded one proves nothing, and a
    #    legacy receipt carries no proof to fall back on.
    if not allow_current_authority:
        return None
    try:
        snapshot = _current_snapshot(config)
    except Exception:  # noqa: BLE001
        # Current authority is optional evidence for alias equivalence. A
        # transient store/open race cannot erase the existing receipt or make a
        # second send safe, so leave the target unproven and fail closed through
        # the receipt-authority path.
        return None
    if _backend_health_error(config, request, snapshot) is not None:
        return None
    worker = _resolve_authoritative_worker(request, snapshot)
    if isinstance(worker, CommandEnvelope):
        return None
    if worker.id != stored_worker_id:
        return _duplicate_request(request)
    return stored_worker_id


def _receipt_authority(
    config: Config,
    request: CommandRequest,
    receipt: Mapping[str, Any],
) -> CommandEnvelope | _ReceiptTakeover:
    """Decide one existing host/request from stored evidence before authority.

    Returns the envelope this retry must get, or a takeover marker when the
    stored reservation was abandoned before any send and the normal path may
    re-drive it.
    """
    if receipt.get("legacy_collision") is not False:
        return _receipt_malformed(request)
    if receipt.get("action") != request.action:
        return _duplicate_request(request)

    proven = _proven_replay_worker_id(config, request, receipt)
    if isinstance(proven, CommandEnvelope):
        return proven
    if proven is None:
        return _receipt_target_unprovable(request)

    try:
        canonical = build_canonical_mutation(request, public_worker_id=proven)
    except (TypeError, ValueError):
        return _receipt_malformed(request)

    replay = _envelope_from_receipt(request, canonical, receipt)
    state = receipt.get("state")
    if state in {"accepted", "rejected", "uncertain"}:
        if replay.status == STATUS_DUPLICATE_REQUEST:
            # A changed canonical mutation never rewrites the original receipt.
            return replay
        if receipt.get("canonical_version") == 0:
            # Legacy evidence cannot be re-expressed as a canonical v1 row, so
            # replay it as read instead of inventing one.
            return replay
        return _reserve_terminal_replay(config, request, canonical, receipt, replay)
    if replay.status != STATUS_PENDING:
        return replay
    if state == "send_started":
        verdict = _submission_verdict(replay)
        if verdict == "written_to_pty":
            if config.db_path is None:
                return replay
            try:
                settlement = settle_submission_link_for_request(
                    config.db_path,
                    host_id=config.host_id,
                    request_id=request.request_id or "",
                )
                linked = linked_turn_for_submission(
                    config.db_path,
                    host_id=config.host_id,
                    request_id=request.request_id or "",
                )
            except Exception:  # noqa: BLE001
                return replay
            if isinstance(linked, Mapping):
                accepted = _accepted_queued_send_envelope(request, replay, linked)
                try:
                    finished = finish_queued_command_request(
                        config.db_path,
                        host_id=config.host_id,
                        request_id=request.request_id or "",
                        canonical_fingerprint=canonical.fingerprint,
                        queued_result_json=str(receipt.get("result_json") or ""),
                        accepted_result_json=envelope_to_receipt_json(accepted),
                        event_payload=_transition_payload(
                            request,
                            worker_id=proven,
                            envelope=accepted,
                        ),
                    )
                except Exception:  # noqa: BLE001
                    return replay
                if not isinstance(finished, Mapping):
                    return replay
                return _envelope_from_receipt(
                    request,
                    canonical,
                    finished.get("receipt"),
                )
            if (
                not isinstance(settlement, Mapping)
                or settlement.get("state") not in {"ambiguous", "expired"}
            ):
                return replay
            uncertain = _unverified_queued_send_envelope(request, replay)
            try:
                finished = finish_unverified_queued_command_request(
                    config.db_path,
                    host_id=config.host_id,
                    request_id=request.request_id or "",
                    canonical_fingerprint=canonical.fingerprint,
                    queued_result_json=str(receipt.get("result_json") or ""),
                    uncertain_result_json=envelope_to_receipt_json(uncertain),
                    event_payload=_transition_payload(
                        request,
                        worker_id=proven,
                        envelope=uncertain,
                    ),
                )
            except Exception:  # noqa: BLE001
                return replay
            if not isinstance(finished, Mapping):
                return replay
            return _envelope_from_receipt(
                request,
                canonical,
                finished.get("receipt"),
            )
        if command_reservation_is_live(receipt):
            return replay
        unknown = _recovered_unknown_send_envelope(
            request,
            worker_id=proven,
        )
        try:
            recovered = recover_unresolved_command_send(
                config.db_path,
                host_id=config.host_id,
                request_id=request.request_id or "",
                canonical_fingerprint=canonical.fingerprint,
                unresolved_result_json=str(receipt.get("result_json") or ""),
                uncertain_result_json=envelope_to_receipt_json(unknown),
                event_payload=_transition_payload(
                    request,
                    worker_id=proven,
                    envelope=unknown,
                ),
            )
        except Exception:  # noqa: BLE001
            return replay
        if not isinstance(recovered, Mapping):
            return replay
        return _envelope_from_receipt(
            request,
            canonical,
            recovered.get("receipt"),
        )
    if command_reservation_is_live(receipt):
        return replay
    # An abandoned reservation never reached a send. Re-driving it is the
    # existing state machine's recovery, not a replay of a finished mutation.
    return _ReceiptTakeover(public_worker_id=proven)


def replay_command_receipt(
    config: Config,
    params: Mapping[str, Any] | str,
) -> CommandEnvelope | None:
    """Read one existing receipt without reserving, resending, or rewriting it."""
    payload = params if isinstance(params, str) else _raw_payload_from_mapping(params)
    request, parse_error = parse_command_request(payload)
    if parse_error is not None or request is None or validate_request(request) is not None:
        return None
    if request.action not in _MUTATING_ACTIONS or request.dry_run or config.db_path is None:
        return None
    try:
        receipt = get_command_request(
            config.db_path,
            config.host_id,
            request.request_id or "",
        )
    except Exception:
        return None
    if not isinstance(receipt, Mapping):
        return None
    if receipt.get("legacy_collision") is not False:
        return _receipt_malformed(request)
    if receipt.get("action") != request.action:
        return _duplicate_request(request)
    proven = _proven_replay_worker_id(
        config,
        request,
        receipt,
        allow_current_authority=False,
    )
    if isinstance(proven, CommandEnvelope):
        return proven
    if proven is None:
        # Proving that a different selector spelling names the stored worker
        # would need a current observation, and this path must never observe
        # private sources. Leave the result unresolved for the caller instead.
        return None
    try:
        canonical = build_canonical_mutation(request, public_worker_id=proven)
    except (TypeError, ValueError):
        return _receipt_malformed(request)
    return _negotiated_submission_envelope(
        config,
        request,
        _envelope_from_receipt(request, canonical, receipt),
    )


def submit_acp_command(
    config: Config,
    params: Mapping[str, Any] | str,
    *,
    prompt_router: AcpPromptRouter,
    worker_owner: AcpWorkerOwner | None = None,
    required: bool = False,
    observation_only: bool = False,
) -> CommandEnvelope | None:
    """Submit ``send_instruction`` through a live ACP worker route.

    ``None`` means the ACP path made no durable change and an optional policy
    may safely use the legacy Herdr sender.  Once a receipt reaches
    ``send_started``, every failure is terminally uncertain and this function
    never permits a second transport attempt.

    In observation-only shadow mode a live ACP route is ownership evidence,
    not a transport. Such a target fails closed before receipt reservation;
    returning ``None`` would incorrectly fall through to its legacy route.
    """

    payload = params if isinstance(params, str) else _raw_payload_from_mapping(params)
    request, parse_error = parse_command_request(payload)
    if parse_error is not None or request is None:
        return None
    if validate_request(request) is not None:
        return None
    if request.dry_run:
        return None
    if request.action != "send_instruction":
        return (
            _backend_unavailable(
                request,
                "command is not supported by the required ACP control path",
            )
            if required and request.action in _MUTATING_ACTIONS
            else None
        )

    existing_receipt: Mapping[str, Any] | None = None
    if config.db_path is not None:
        try:
            candidate = get_command_request(
                config.db_path,
                config.host_id,
                request.request_id or "",
            )
        except Exception:
            candidate = None
        if isinstance(candidate, Mapping):
            existing_receipt = candidate

    takeover: _ReceiptTakeover | None = None
    if existing_receipt is not None:
        decided = _receipt_authority(config, request, existing_receipt)
        if isinstance(decided, CommandEnvelope):
            return _negotiated_submission_envelope(config, request, decided)
        takeover = decided

    try:
        snapshot = _current_snapshot(config)
    except Exception:  # noqa: BLE001
        if takeover is not None:
            return _request_in_progress(request)
        return (
            _backend_unavailable(
                request,
                "Current worker authority is temporarily unavailable",
            )
            # A configured ownership oracle means preferred mode can route
            # both ACP and never-ACP workers. Without the authoritative
            # snapshot there is no exact worker identity to ask it about, so
            # falling through would treat "unknown" as proof of never-ACP.
            if required or worker_owner is not None
            else None
        )

    health_error = _backend_health_error(config, request, snapshot)
    worker = _resolve_authoritative_worker(request, snapshot)
    if isinstance(worker, CommandEnvelope):
        if takeover is not None:
            return _request_in_progress(request)
        if health_error is not None:
            return health_error if required else None
        return worker
    if takeover is not None and worker.id != takeover.public_worker_id:
        return _duplicate_request(request)

    # Preferred mode may fall back only for workers that ACP has never
    # claimed. Once the coordinator publishes an exact worker generation,
    # losing its visible console or runtime is an ACP outage, not permission
    # to inject keys through the legacy PTY path.
    owned_by_acp = False
    if worker_owner is not None:
        try:
            owned_by_acp = bool(worker_owner(worker.id, worker.fingerprint))
        except Exception:  # noqa: BLE001
            # An ownership oracle failure cannot prove that legacy pane I/O is
            # safe. Prefer a retryable no-send result over crossing transports.
            owned_by_acp = True
    route_required = required or owned_by_acp

    route: AcpPromptRoute | None = None
    route_resolved = False
    if observation_only:
        route_resolved = True
        try:
            route = prompt_router(worker)
        except Exception:  # noqa: BLE001
            route = None
        if route is not None:
            return _backend_unavailable(
                request,
                "ACP shadow is observation-only for ACP-owned workers; use an "
                "isolated ACP preferred or required canary to validate prompt "
                "execution",
            )

    permanent_error = _worker_status_error(request, worker) or health_error
    if permanent_error is not None:
        if route_required:
            canonical = build_canonical_mutation(request, public_worker_id=worker.id)
            reservation = _reserve_canonical_request(config, request, canonical)
            if isinstance(reservation, CommandEnvelope):
                return reservation
            return _finish_before_send(config, request, reservation, permanent_error)
        return None

    if not route_resolved:
        try:
            route = prompt_router(worker)
        except Exception:  # noqa: BLE001
            route = None
    if route is None:
        if takeover is not None:
            return _request_in_progress(request)
        return (
            _backend_unavailable(request, "ACP worker route is unavailable")
            if route_required
            else None
        )
    def submit_through(active_route: AcpPromptRoute) -> CommandEnvelope | None:
        try:
            binding_fingerprint = str(
                getattr(active_route, "binding_fingerprint", "") or ""
            ).strip()
        except Exception:  # noqa: BLE001
            binding_fingerprint = ""
        if not binding_fingerprint:
            if takeover is not None:
                return _request_in_progress(request)
            return (
                _backend_unavailable(
                    request, "ACP worker route has no durable authority"
                )
                if route_required
                else None
            )

        canonical = build_canonical_mutation(request, public_worker_id=worker.id)
        reservation = _reserve_canonical_request(config, request, canonical)
        if isinstance(reservation, CommandEnvelope):
            return reservation

        send_started: Mapping[str, Any] | None = None
        send_start_outcome: CommandEnvelope | None = None

        class SendStartRejected(RuntimeError):
            pass

        def mark_send_started_at_transport_boundary() -> None:
            nonlocal send_started, send_start_outcome
            started = _mark_request_send_started(
                config,
                request,
                reservation,
                binding_fingerprint=binding_fingerprint,
                worker=worker,
                instruction_text=_instruction_text(request),
            )
            if isinstance(started, CommandEnvelope):
                send_start_outcome = started
                raise SendStartRejected
            if not isinstance(started, Mapping):
                send_start_outcome = _recover_request(
                    config, request, reservation.canonical
                )
                raise SendStartRejected
            send_started = started

        def retryable_before_transport() -> CommandEnvelope:
            abandoned = False
            if config.db_path is not None:
                try:
                    abandoned = abandon_command_request_reservation(
                        config.db_path,
                        host_id=config.host_id,
                        request_id=request.request_id or "",
                        canonical_fingerprint=reservation.canonical.fingerprint,
                        owner_token=reservation.owner_token,
                    )
                except Exception:  # noqa: BLE001
                    abandoned = False
            if abandoned:
                return _backend_unavailable(
                    request,
                    "ACP prompt did not reach its transport boundary",
                )
            return _recover_request(config, request, reservation.canonical)

        try:
            use_steering = False
            steer = getattr(active_route, "steer", None)
            if callable(steer):
                try:
                    # The ACP runtime is authoritative about whether this
                    # exact session currently has an appendable prompt.  A
                    # Herdr snapshot can briefly remain idle after the ACP
                    # prompt has started; requiring both signals opens a
                    # second session/prompt that the adapter serializes behind
                    # the live turn, so its submission acknowledgement can
                    # never arrive in time.  An actually idle runtime reports
                    # supports_steering=False and keeps the normal prompt path.
                    use_steering = (
                        getattr(active_route, "supports_steering", False) is True
                    )
                except Exception:
                    use_steering = False
            submit = steer if use_steering else active_route.prompt
            route_result = submit(
                _instruction_text(request),
                producer_turn_id=turn_submission_id(
                    config.host_id,
                    request.request_id or "",
                ),
                timeout=config.acp_request_timeout_seconds,
                on_send_start=mark_send_started_at_transport_boundary,
            )
            if use_steering:
                raw_outcome = getattr(route_result, "outcome", None)
                steering_outcome = getattr(raw_outcome, "value", raw_outcome)
                if steering_outcome == "failed":
                    return _finish_request(
                        config,
                        request,
                        reservation,
                        _instruction_rejected_envelope(
                            request,
                            worker,
                            verdict="steering_failed",
                        ),
                        expected_state="send_started",
                        terminal_state="rejected",
                    )
                if steering_outcome not in {"injected", "startedNewTurn"}:
                    return _finish_request(
                        config,
                        request,
                        reservation,
                        _instruction_uncertain_envelope(
                            request,
                            worker,
                            verdict="unknown",
                        ),
                        expected_state="send_started",
                        terminal_state="uncertain",
                    )
        except SendStartRejected:
            return send_start_outcome or _recover_request(
                config, request, reservation.canonical
            )
        except Exception:  # noqa: BLE001
            if send_started is None:
                return retryable_before_transport()
            return _finish_request(
                config,
                request,
                reservation,
                _instruction_uncertain_envelope(
                    request,
                    worker,
                    verdict="unknown",
                ),
                expected_state="send_started",
                terminal_state="uncertain",
            )

        if send_started is None:
            # A route that claims success without crossing the durable
            # transport boundary is not an accepted implementation.
            return retryable_before_transport()

        observed_turn: Mapping[str, Any] | None = send_started
        if config.db_path is not None:
            try:
                refreshed = linked_turn_for_submission(
                    config.db_path,
                    host_id=config.host_id,
                    request_id=request.request_id or "",
                )
            except Exception:  # noqa: BLE001
                refreshed = None
            if isinstance(refreshed, Mapping):
                observed_turn = refreshed
        accepted = _accepted_send_envelope(
            request,
            worker,
            observed_turn,
            submission_verdict="submitted",
        )
        return _finish_request(
            config,
            request,
            reservation,
            accepted,
            expected_state="send_started",
            terminal_state="accepted",
        )

    prepare_route = getattr(route, "prepare", None)
    if not callable(prepare_route):
        return submit_through(route)

    # Enter the generation fence before reserving a receipt. A failed status
    # check is therefore a retryable no-receipt outcome, never a fabricated
    # send_started ambiguity. Keep the context active until the ACP client has
    # acknowledged writing the complete prompt frame.
    try:
        preparation = prepare_route()
        active_route = preparation.__enter__()
    except Exception:  # noqa: BLE001 - no receipt or transport exists yet
        if takeover is not None:
            return _request_in_progress(request)
        return (
            _backend_unavailable(
                request, "ACP worker route could not be prepared"
            )
            if route_required
            else None
        )
    if active_route is None:
        active_route = route
    try:
        return submit_through(active_route)
    finally:
        preparation.__exit__(None, None, None)


def _submit_command_v2(
    config: Config,
    params: Mapping[str, Any] | str,
    *,
    socket_client_factory: SocketClientFactory | None = None,
    acp_permission_router: AcpPermissionDecisionRouter | None = None,
) -> CommandEnvelope:
    """Submit one command through the authoritative daemon/socket path."""
    payload = params if isinstance(params, str) else _raw_payload_from_mapping(params)
    request, parse_error = parse_command_request(payload)
    if parse_error is not None:
        if request is not None:
            return CommandEnvelope.from_error(request, parse_error)
        return CommandEnvelope.from_error(None, parse_error)

    validation_error = validate_request(request)
    if validation_error is not None:
        return CommandEnvelope.from_error(request, validation_error)

    if request.action not in _MUTATING_ACTIONS:
        return _execute_non_mutating(config, request)
    if request.dry_run:
        return _mutation_dry_run(request)

    existing_receipt: Mapping[str, Any] | None = None
    if config.db_path is not None:
        try:
            candidate = get_command_request(
                config.db_path,
                config.host_id,
                request.request_id or "",
            )
        except Exception:
            candidate = None
        if isinstance(candidate, Mapping):
            existing_receipt = candidate

    # An existing receipt is the authority for its request ID. It decides the
    # retry from stored evidence before any mutable worker snapshot is read, so
    # a vanished, renamed, or recycled worker can never downgrade a live receipt
    # to a no-receipt failure or drive a second backend mutation. Only an
    # abandoned reservation returns here, to be re-driven by the normal path.
    takeover: _ReceiptTakeover | None = None
    if existing_receipt is not None:
        decided = _receipt_authority(config, request, existing_receipt)
        if isinstance(decided, CommandEnvelope):
            return decided
        takeover = decided

    try:
        snapshot = _current_snapshot(config)
    except Exception:  # noqa: BLE001
        # No external mutation has begun. Store/open contention while reading
        # current authority is safely retryable when no receipt exists. An
        # abandoned reservation remains authoritative and stays in progress.
        if takeover is not None:
            return _request_in_progress(request)
        return _backend_unavailable(
            request,
            "Current worker authority is temporarily unavailable",
        )
    health_error = _backend_health_error(config, request, snapshot)

    if request.action == "send_instruction":
        worker = _resolve_authoritative_worker(request, snapshot)
        if isinstance(worker, CommandEnvelope):
            if takeover is not None:
                # The receipt says this request is reserved and unsent. Mutable
                # authority may not restate that as a no-receipt failure.
                return _request_in_progress(request)
            # An unhealthy observation cannot authoritatively establish that a
            # selector is absent, stale, or ambiguous, so keep the request ID
            # retryable until a canonical worker can be proven.
            if health_error is not None:
                return health_error
            return worker
        if takeover is not None and worker.id != takeover.public_worker_id:
            # The abandoned reservation named a different worker, so this is a
            # changed target. Fail before any socket or backend work.
            return _duplicate_request(request)
        canonical = build_canonical_mutation(request, public_worker_id=worker.id)
        # A disallowed worker status or an unavailable backend is an authoritative
        # observation of proven target unsuitability: a durable rejection is
        # justified, and a same-ID retry replays it.
        permanent_error = _worker_status_error(request, worker) or health_error
        prepared: PreparedInstructionMutation | PreSendFailure | None = None
        if permanent_error is None:
            prepared = _prepare_instruction(
                config,
                request,
                worker,
                socket_client_factory=socket_client_factory,
            )
        # A safe transient preparation failure never began a send and never
        # created durable authority. Keep the request ID retryable without
        # reserving, so a command that was never sent is never silently dropped.
        if isinstance(prepared, PreSendFailure) and prepared.is_transient:
            if takeover is not None:
                return _request_in_progress(request)
            return prepared.envelope
        reservation = _reserve_canonical_request(config, request, canonical)
        if isinstance(reservation, CommandEnvelope):
            if isinstance(prepared, PreparedInstructionMutation):
                _close_socket_client(prepared.client)
            return reservation
        if permanent_error is not None:
            return _finish_before_send(
                config,
                request,
                reservation,
                permanent_error,
            )
        if isinstance(prepared, PreSendFailure):
            return _finish_before_send(config, request, reservation, prepared.envelope)
        assert isinstance(prepared, PreparedInstructionMutation)
        return _submit_instruction(
            config,
            request,
            worker,
            reservation,
            prepared,
        )

    answer_pre_send: PreSendFailure | None = None
    validate_answer = (
        _validate_pending_decision
        if request.action == "answer_decision"
        else _validate_pending_choice
    )
    if takeover is not None:
        # Re-driving an abandoned answer reservation: the receipt already fixed
        # which worker this request answers, so a pending interaction that now
        # routes elsewhere is a changed target, not a new one.
        existing_worker_id = takeover.public_worker_id
        canonical = build_canonical_mutation(
            request,
            public_worker_id=existing_worker_id,
        )
        validated = validate_answer(config, request)
        if isinstance(validated, PreSendFailure):
            answer_pre_send = validated
        elif validated.worker_id != existing_worker_id:
            answer_pre_send = _permanent_pre_send(_duplicate_request(request))
    else:
        validated = validate_answer(config, request)
        if isinstance(validated, PreSendFailure):
            # No reservation exists yet, so neither a transient nor a permanent
            # validation failure writes a receipt here. Return it directly.
            if health_error is not None and request.action != "answer_decision":
                return health_error
            return validated.envelope
        canonical = build_canonical_mutation(
            request,
            public_worker_id=validated.worker_id,
        )

    # A safe transient pre-send failure (the pending store raised) never began a
    # send. Keep it retryable under the same request ID without reserving.
    if answer_pre_send is not None and answer_pre_send.is_transient:
        if takeover is not None:
            return _request_in_progress(request)
        return answer_pre_send.envelope
    if (
        answer_pre_send is not None
        and answer_pre_send.envelope.status == STATUS_ANSWER_IN_PROGRESS
    ):
        # Another request owns the still-live decision claim. Keep this
        # abandoned reservation nonterminal so it can take over after that
        # claim is released or expires.
        return _answer_in_progress(request, receipt_reserved=True)

    acp_handoff = False
    if answer_pre_send is None and request.action == "answer_decision":
        acp_binding = _decision_uses_acp_binding(config, validated)
        if acp_binding:
            try:
                acp_handoff = bool(
                    acp_permission_router is not None
                    and acp_permission_router.owns_permission_decision(validated)
                )
            except Exception:
                acp_handoff = False
            if not acp_handoff:
                unavailable = _backend_unavailable(
                    request,
                    "ACP permission authority is temporarily unavailable",
                )
                if takeover is not None:
                    return _request_in_progress(request)
                return unavailable

    client_or_error: Any | CommandEnvelope | None = None
    if answer_pre_send is None and health_error is None:
        client_or_error = (
            object()
            if acp_handoff
            else _connect_socket(config, request, socket_client_factory)
        )
    if isinstance(client_or_error, CommandEnvelope):
        # The socket could not be reached before any transmission -> safe
        # transient. Stay retryable rather than reserving a durable rejection.
        if takeover is not None:
            return _request_in_progress(request)
        return client_or_error

    reservation = _reserve_canonical_request(config, request, canonical)
    if isinstance(reservation, CommandEnvelope):
        if client_or_error is not None and not isinstance(client_or_error, CommandEnvelope):
            _close_socket_client(client_or_error)
        return reservation
    if answer_pre_send is not None:
        return _finish_before_send(
            config,
            request,
            reservation,
            answer_pre_send.envelope,
        )
    if health_error is not None:
        return _finish_before_send(
            config,
            request,
            reservation,
            health_error,
        )
    assert client_or_error is not None
    if request.action == "answer_decision":
        return _answer_decision(
            config,
            request,
            validated,
            reservation,
            client_or_error,
            acp_permission_router=acp_permission_router,
            acp_handoff=acp_handoff,
        )
    return _answer_pending(
        config,
        request,
        validated,
        reservation,
        client_or_error,
    )


def _negotiated_submission_envelope(
    config: Config,
    request: CommandRequest,
    envelope: CommandEnvelope,
) -> CommandEnvelope:
    """Project an accepted send into v3 only for an explicit client opt-in."""
    if (
        request.response_schema_version != COMMAND_ENVELOPE_V3_SCHEMA_VERSION
        or request.action != "send_instruction"
        or envelope.action != "send_instruction"
        or envelope.disposition != DISPOSITION_TERMINAL_ACCEPTED
        or envelope.status != STATUS_ACCEPTED
        or not isinstance(envelope.result, Mapping)
    ):
        return envelope
    result = dict(envelope.result)
    result["submission_id"] = turn_submission_id(
        config.host_id,
        request.request_id or "",
    )
    if config.db_path is not None:
        try:
            settle_submission_link_for_request(
                config.db_path,
                host_id=config.host_id,
                request_id=request.request_id or "",
            )
            linked_turn = linked_turn_for_submission(
                config.db_path,
                host_id=config.host_id,
                request_id=request.request_id or "",
            )
        except Exception:  # noqa: BLE001
            linked_turn = None
        result["turn_id"] = (
            linked_turn.get("id")
            if isinstance(linked_turn, Mapping)
            else None
        )
        if isinstance(linked_turn, Mapping):
            result["observed_turn_state"] = (
                "complete" if linked_turn.get("complete") is True else "observed"
            )
        else:
            result["observed_turn_state"] = "pending_observation"
    return CommandEnvelope(
        ok=envelope.ok,
        status=envelope.status,
        action=envelope.action,
        disposition=envelope.disposition,
        request_id=envelope.request_id,
        dry_run=envelope.dry_run,
        result=result,
        error=envelope.error,
        warnings=list(envelope.warnings),
        schema_version=COMMAND_ENVELOPE_V3_SCHEMA_VERSION,
    )


def submit_command(
    config: Config,
    params: Mapping[str, Any] | str,
    *,
    socket_client_factory: SocketClientFactory | None = None,
    acp_prompt_router: AcpPromptRouter | None = None,
    acp_worker_owner: AcpWorkerOwner | None = None,
    acp_required: bool = False,
    acp_observation_only: bool = False,
    acp_permission_router: AcpPermissionDecisionRouter | None = None,
) -> CommandEnvelope:
    """Submit one command and apply optional response-envelope negotiation."""
    if acp_prompt_router is not None:
        acp_envelope = submit_acp_command(
            config,
            params,
            prompt_router=acp_prompt_router,
            worker_owner=acp_worker_owner,
            required=acp_required,
            observation_only=acp_observation_only,
        )
        if acp_envelope is not None:
            payload = (
                params
                if isinstance(params, str)
                else _raw_payload_from_mapping(params)
            )
            request, parse_error = parse_command_request(payload)
            if parse_error is None and request is not None:
                return _negotiated_submission_envelope(
                    config,
                    request,
                    acp_envelope,
                )
            return acp_envelope
    envelope = _submit_command_v2(
        config,
        params,
        socket_client_factory=socket_client_factory,
        acp_permission_router=acp_permission_router,
    )
    payload = params if isinstance(params, str) else _raw_payload_from_mapping(params)
    request, parse_error = parse_command_request(payload)
    if parse_error is not None or request is None:
        return envelope
    return _negotiated_submission_envelope(config, request, envelope)
