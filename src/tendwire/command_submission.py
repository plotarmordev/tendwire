"""Authoritative daemon command submission path for Tendwire."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
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
    STATUS_AMBIGUOUS_TARGET,
    STATUS_BACKEND_UNAVAILABLE,
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
from .core.models import BackendHealth, Snapshot, Worker
from .core.projector import project_from_observations
from .store.pending import (
    abandon_backend_pending_decision_claim,
    backend_pending_decision_terminal_effect,
    claim_backend_pending_decision,
    start_backend_pending_decision_send,
)
from .store.projection import latest_snapshot
from .store.receipts import (
    abandon_command_request_reservation,
    command_reservation_is_live,
    envelope_to_receipt_json,
    finish_command_request,
    get_command_request,
    linked_turn_for_submission,
    mark_command_send_started,
    recover_unresolved_command_send,
    reserve_command_request,
    settle_submission_link_for_request,
)


HERDR_BACKEND = "herdr"
_MUTATING_ACTIONS = frozenset(
    {"send_instruction", "answer_decision"}
)
_DISALLOWED_SEND_STATUSES = frozenset({"closed", "failed", "unknown"})
_AMBIGUOUS_BINDING_REASONS = frozenset({"duplicate_backend_target", "not_unique"})
_TERMINAL_DISPOSITIONS = {
    "accepted": DISPOSITION_TERMINAL_ACCEPTED,
    "rejected": DISPOSITION_TERMINAL_REJECTED,
    "uncertain": DISPOSITION_TERMINAL_UNCERTAIN,
}


class AcpPromptRoute(Protocol):
    binding_fingerprint: str
    supports_steering: bool
    prompt: Callable[..., object]
    steer: Callable[..., object]
    prepare: Callable[[], Any]


AcpPromptRouter = Callable[[Worker], AcpPromptRoute | None]


class AcpPermissionDecisionRouter(Protocol):
    def owns_permission_decision(self, decision: Any) -> bool: ...

    def answer_permission_decision(self, decision: Any, *, timeout: float) -> None: ...


def _backend_health(snapshot: Snapshot) -> BackendHealth:
    for health in snapshot.backend_health:
        if health.name == HERDR_BACKEND:
            return health
    return BackendHealth(name=HERDR_BACKEND, status="unknown", outcome="unknown")


def _current_snapshot(config: Config) -> Snapshot:
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
    error = error_value(STATUS_BACKEND_UNAVAILABLE, message, details=details)
    return CommandEnvelope.from_error(request, error)


def _target_resolution_error(
    request: CommandRequest,
    status: str,
    candidates: list[dict[str, Any]],
) -> CommandEnvelope:
    if status == STATUS_STALE_TARGET:
        message = "target worker fingerprint does not match the current worker"
    elif status == STATUS_AMBIGUOUS_TARGET:
        message = "target matches more than one worker"
    elif status == STATUS_REJECTED:
        message = "target worker status does not allow instructions"
        if candidates:
            message = f"target worker status does not allow instructions: {candidates[0]['status']!r}"
    else:
        status, candidates, message = STATUS_NOT_FOUND, [], "no worker matches the target"
    return CommandEnvelope.from_result(
        request,
        ok=False,
        status=status,
        result={"candidates": candidates},
        error=error_value(status, message),
    )


def _resolve_authoritative_worker(
    request: CommandRequest,
    snapshot: Snapshot,
) -> Worker | CommandEnvelope:
    resolved, candidates, status = resolve_target(
        request.target,
        list(snapshot.workers),
        allow_disallowed_status=True,
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


def _target_state_at_send(worker: Worker) -> str:
    status = str(worker.status or "").strip().lower().replace("-", "_")
    return status or "unknown"


def _instruction_text(request: CommandRequest) -> str:
    instruction = request.instruction if isinstance(request.instruction, dict) else {}
    text = instruction.get("text")
    return text if isinstance(text, str) else ""




def _backend_uncertain(request: CommandRequest, message: str) -> CommandEnvelope:
    return _failure_envelope(
        request, STATUS_REQUEST_STATE_UNCERTAIN, message,
        disposition=DISPOSITION_TERMINAL_UNCERTAIN,
    )


def _failure_envelope(
    request: CommandRequest,
    status: str,
    message: str,
    *,
    disposition: str = DISPOSITION_NO_RECEIPT,
    result: dict[str, Any] | None = None,
) -> CommandEnvelope:
    return CommandEnvelope.from_result(
        request,
        ok=False,
        status=status,
        disposition=disposition,
        result=result,
        error=error_value(status, message),
    )


def _request_in_progress(request: CommandRequest) -> CommandEnvelope:
    return _failure_envelope(
        request, STATUS_PENDING, "request is already in progress",
        disposition=DISPOSITION_IN_PROGRESS,
    )


def _answer_in_progress(
    request: CommandRequest,
) -> CommandEnvelope:
    return _failure_envelope(
        request, STATUS_ANSWER_IN_PROGRESS,
        "another request is currently answering this decision",
        disposition=DISPOSITION_IN_PROGRESS,
    )


def _duplicate_request(request: CommandRequest) -> CommandEnvelope:
    return _failure_envelope(
        request, STATUS_DUPLICATE_REQUEST,
        "request_id reused with a different canonical mutation",
        disposition=DISPOSITION_TERMINAL_REJECTED,
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
    return _failure_envelope(request, status, messages[status])


def _decision_route(claim: Any) -> tuple[Any, ...] | None:
    route = tuple(
        getattr(claim, field, None)
        for field in (
            "worker_id", "worker_fingerprint", "binding_private_fingerprint",
            "turn_target_value", "decision_ref", "decision_kind", "option_count",
            "option_refs", "text",
        )
    )
    worker_id, worker_fp, binding_fp, target, ref, kind, count, refs, text = route
    valid = (
        all(isinstance(value, str) and bool(value) for value in (worker_id, worker_fp, binding_fp, ref))
        and isinstance(target, str) and bool(target.strip())
        and kind in {"single", "multi", "plan"}
        and type(count) is int and count >= 1
        and isinstance(refs, tuple)
        and ((text is None and bool(refs)) or (isinstance(text, str) and bool(text) and not refs))
    )
    return route if valid else None


PreSendFailure = tuple[CommandEnvelope, bool]


def _abandon_pending_claim(config: Config, claim_token: str | None) -> bool:
    if not claim_token:
        return False
    try:
        return abandon_backend_pending_decision_claim(
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
    return (
        type(version) is int
        and version == canonical.canonical_version
        and receipt.get("request_id") == request.request_id
        and receipt.get("action") == canonical.action
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
        if not isinstance(data, dict):
            raise TypeError
    except (KeyError, TypeError, json.JSONDecodeError):
        return _backend_uncertain(
            request,
            "stored request result is unreadable; not retrying mutation",
        )

    state = receipt.get("state")
    expected_disposition = _TERMINAL_DISPOSITIONS.get(state)
    if expected_disposition is None:
        return _backend_uncertain(request, malformed)

    try:
        if data.get("schema_version") != COMMAND_ENVELOPE_SCHEMA_VERSION:
            raise ValueError("unsupported stored envelope schema")
        envelope = CommandEnvelope.from_dict(data)
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
    if not valid_identity:
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
        or envelope.status != STATUS_PENDING
    ):
        return _request_in_progress(request)
    return envelope


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
    required = (
        "request_id", "action", "canonical_version", "canonical_fingerprint",
        "canonical_request_json", "public_worker_id", "state", "status", "result_json",
    )
    if any(field not in receipt for field in required):
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
    if state in {"accepted", "rejected", "uncertain"}:
        return _stored_terminal_envelope(request, receipt)
    return _backend_uncertain(
        request,
        "stored request receipt has an illegal state; not retrying mutation",
    )


@dataclass(frozen=True)
class ReservedCommandMutation:
    canonical: CanonicalMutation
    owner_token: str

def _reserve_canonical_request(
    config: Config,
    request: CommandRequest,
    canonical: CanonicalMutation,
) -> ReservedCommandMutation | CommandEnvelope:
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
    try:
        terminal = replace(
            envelope, disposition=_TERMINAL_DISPOSITIONS[terminal_state]
        )
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
        finished = None
    if not isinstance(finished, Mapping):
        return _recover_request(config, request, reservation.canonical)
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

def _mark_request_send_started(
    config: Config,
    request: CommandRequest,
    reservation: ReservedCommandMutation,
    *,
    binding_fingerprint: str,
    worker: Worker | None = None,
    instruction_text: str | None = None,
) -> CommandEnvelope | Mapping[str, Any] | None:
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
        return _recover_request(config, request, reservation.canonical)
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
    return _recover_request(config, request, reservation.canonical)


def _linked_submission_turn(
    config: Config,
    request: CommandRequest,
    *,
    settle: bool = False,
) -> Mapping[str, Any] | None:
    try:
        if settle:
            settle_submission_link_for_request(
                config.db_path, host_id=config.host_id,
                request_id=request.request_id or "",
            )
        turn = linked_turn_for_submission(
            config.db_path, host_id=config.host_id,
            request_id=request.request_id or "",
        )
    except Exception:  # noqa: BLE001
        return None
    return turn if isinstance(turn, Mapping) else None


def _accepted_send_envelope(
    request: CommandRequest,
    worker: Worker,
    turn: Mapping[str, Any],
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
            "submission_verdict": "submitted",
        },
    )


def _instruction_failure_envelope(
    request: CommandRequest,
    *,
    worker_id: str,
    verdict: str,
    target_state: str | None = None,
) -> CommandEnvelope:
    rejected = verdict == "steering_failed"
    status = STATUS_REJECTED if rejected else STATUS_REQUEST_STATE_UNCERTAIN
    result = {
        "target": {"worker_id": worker_id},
        "delivery_state": "not_delivered" if rejected else "unknown",
        "transport_state": "not_submitted" if rejected else "unknown",
        "submission_verdict": verdict,
    }
    if target_state is not None:
        result["target_state_at_send"] = target_state
    message = (
        "instruction was not delivered"
        if rejected
        else "instruction delivery is unknown; not retrying mutation"
    )
    if target_state is None and not rejected:
        message = "instruction delivery is unknown after process recovery; not retrying mutation"
    return _failure_envelope(
        request, status, message,
        disposition=DISPOSITION_TERMINAL_REJECTED if rejected else DISPOSITION_TERMINAL_UNCERTAIN,
        result=result,
    )


def _pending_terminal_effect(
    config: Config,
    claim_token: str,
    *,
    accepted: bool = False,
) -> Callable[[Any], Any] | None:
    try:
        return backend_pending_decision_terminal_effect(
            host_id=config.host_id,
            claim_token=claim_token,
            accepted=accepted,
        )
    except Exception:
        return None


def _pending_decision(config: Config, request: CommandRequest, *, claim: bool) -> Any:
    params = request.params or {}
    return claim_backend_pending_decision(
        config.db_path,
        config.host_id,
        str((request.target or {}).get("worker_id") or ""),
        str(params.get("decision_ref") or ""),
        params.get("selection") if isinstance(params.get("selection"), Mapping) else {},
        claim=claim,
    )


def _pending_failure_status(status: str) -> str:
    return {
        "already_claimed": STATUS_ANSWER_IN_PROGRESS,
        "unknown_worker": STATUS_UNKNOWN_WORKER,
        "invalid_selection": STATUS_INVALID_SELECTION,
        "unsupported_decision": STATUS_UNSUPPORTED_DECISION,
    }.get(status, STATUS_DECISION_NOT_PENDING)


def _validate_pending_decision(
    config: Config,
    request: CommandRequest,
) -> Any | PreSendFailure:
    try:
        validated = _pending_decision(config, request, claim=False)
    except Exception:
        return _backend_unavailable(request, "pending state store is unavailable"), True
    if validated.status == "validated" and _decision_route(validated) is not None:
        return validated
    if validated.status == "acp_authority_unavailable":
        return (
            _backend_unavailable(
                request,
                "ACP permission authority is temporarily unavailable",
            ),
            True,
        )
    return _decision_failure_envelope(
        request, _pending_failure_status(validated.status)
    ), False


def _claim_pending_decision(
    config: Config,
    request: CommandRequest,
    validated: Any,
) -> Any | CommandEnvelope:
    try:
        claim = _pending_decision(config, request, claim=True)
    except Exception:
        return _backend_uncertain(request, "pending decision claim state is uncertain")
    if (
        claim.status == "claimed"
        and isinstance(claim.claim_token, str)
        and claim.claim_token
    ):
        if _decision_route(validated) == _decision_route(claim):
            return claim
    if claim.status == "acp_authority_unavailable":
        return _backend_unavailable(
            request,
            "ACP permission authority is temporarily unavailable",
        )
    return _decision_failure_envelope(request, _pending_failure_status(claim.status))


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


def _finish_uncertain_decision(
    config: Config,
    request: CommandRequest,
    reservation: ReservedCommandMutation,
    claim_token: str,
    message: str,
) -> CommandEnvelope:
    return _finish_request(
        config,
        request,
        reservation,
        _backend_uncertain(request, message),
        expected_state="send_started",
        terminal_state="uncertain",
        terminal_effect=_pending_terminal_effect(config, claim_token),
    )


def _answer_decision(
    config: Config,
    request: CommandRequest,
    validated: Any,
    reservation: ReservedCommandMutation,
    acp_permission_router: AcpPermissionDecisionRouter | None,
) -> CommandEnvelope:
    claim = _claim_pending_decision(config, request, validated)
    if isinstance(claim, CommandEnvelope):
        if claim.status == STATUS_ANSWER_IN_PROGRESS:
            _abandon_request_reservation(config, request, reservation)
            return _answer_in_progress(request)
        if claim.status == STATUS_BACKEND_UNAVAILABLE:
            if _abandon_request_reservation(config, request, reservation):
                return claim
            claim = _backend_uncertain(
                request, "ACP permission reservation state is uncertain"
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
        _abandon_pending_claim(config, claim_token)
        return _finish_uncertain_decision(
            config, request, reservation, claim_token,
            "pending decision start state is uncertain",
        )
    if getattr(started, "status", None) != "started" or _decision_route(claim) != _decision_route(started):
        _abandon_pending_claim(config, claim_token)
        return _finish_uncertain_decision(
            config, request, reservation, claim_token,
            "pending decision state is uncertain after send start",
        )

    try:
        if acp_permission_router is None:
            raise RuntimeError("ACP permission bridge is unavailable")
        acp_permission_router.answer_permission_decision(
            started,
            timeout=config.acp_request_timeout_seconds,
        )
    except Exception:  # noqa: BLE001
        return _finish_uncertain_decision(
            config, request, reservation, claim_token,
            "permission decision state is uncertain after send start",
        )
    accepted = CommandEnvelope.from_result(
        request,
        ok=True,
        status=STATUS_ACCEPTED,
        disposition=DISPOSITION_TERMINAL_ACCEPTED,
        result=_decision_public_result(request, started),
    )
    effect = _pending_terminal_effect(config, claim_token, accepted=True)
    if effect is None:
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


@dataclass(frozen=True)
class _ReceiptTakeover:
    """An abandoned reservation this caller may re-drive to a terminal state."""

    public_worker_id: str


def _selector_proof(request: CommandRequest) -> str:
    """Return the request's selector proof, or empty when none can be built."""
    try:
        return build_selector_proof(request)
    except (TypeError, ValueError):
        return ""


def _proven_replay_worker_id(
    request: CommandRequest,
    receipt: Mapping[str, Any],
) -> str | CommandEnvelope | None:
    """Prove which public worker an existing receipt's retry belongs to.

    Returns the receipt's stored public worker ID when this retry is the same
    request, a fail-closed envelope when it provably is not, or None when no
    available evidence can decide. Stored evidence always outranks mutable
    authority, so a vanished or churned worker cannot hide a live receipt.
    """
    version = receipt.get("canonical_version")
    if isinstance(version, bool) or not isinstance(version, int) or version < 0:
        return _backend_uncertain(
            request, "stored request receipt is malformed; not retrying mutation"
        )
    stored = receipt.get("public_worker_id")
    stored_worker_id = stored if isinstance(stored, str) and stored else ""

    if not stored_worker_id:
        return _backend_uncertain(
            request, "stored request receipt is malformed; not retrying mutation"
        )
    target = request.target or {}
    worker_id = target.get("worker_id")
    explicit_worker_id = (
        worker_id
        if set(target).issubset({"worker_id", "worker_fingerprint"})
        and isinstance(worker_id, str) and bool(worker_id.strip())
        else None
    )
    if request.action == "answer_decision":
        if explicit_worker_id != stored_worker_id:
            return _duplicate_request(request)
        return stored_worker_id
    # 1. An explicit worker ID names the canonical worker outright. A refreshed
    #    worker_fingerprint beside it stays noncanonical.
    if explicit_worker_id is not None:
        if explicit_worker_id != stored_worker_id:
            return _duplicate_request(request)
        return stored_worker_id

    # 2. An exact selector proof recognizes the original spelling of a name or
    #    space alias even after the resolved worker left current authority.
    proof = receipt.get("selector_proof")
    stored_proof = proof if is_selector_proof(proof) else ""
    if stored_proof and stored_proof == _selector_proof(request):
        return stored_worker_id

    # Mutable authority cannot safely reinterpret an existing mutation. Only
    # the stored selector proof or explicit worker ID may authorize replay.
    return None


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
    if receipt.get("action") != request.action:
        return _duplicate_request(request)

    proven = _proven_replay_worker_id(request, receipt)
    if isinstance(proven, CommandEnvelope):
        return proven
    if proven is None:
        return _backend_uncertain(
            request,
            "stored request target cannot be proven; not retrying mutation",
        )

    try:
        canonical = build_canonical_mutation(request, public_worker_id=proven)
    except (TypeError, ValueError):
        return _backend_uncertain(
            request, "stored request receipt is malformed; not retrying mutation"
        )

    replay = _envelope_from_receipt(request, canonical, receipt)
    state = receipt.get("state")
    if state in {"accepted", "rejected", "uncertain"}:
        return replay
    if replay.status != STATUS_PENDING:
        return replay
    if state == "send_started":
        if command_reservation_is_live(receipt):
            return replay
        unknown = _instruction_failure_envelope(
            request,
            worker_id=proven,
            verdict="unknown",
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


def _existing_command_authority(
    config: Config,
    request: CommandRequest,
) -> CommandEnvelope | _ReceiptTakeover | None:
    try:
        receipt = get_command_request(
            config.db_path, config.host_id, request.request_id or ""
        )
    except Exception:
        return None
    return _receipt_authority(config, request, receipt) if isinstance(receipt, Mapping) else None


def _submit_instruction(
    config: Config,
    request: CommandRequest,
    *,
    prompt_router: AcpPromptRouter,
) -> CommandEnvelope:
    """Submit one validated live instruction through its exact ACP route."""
    existing = _existing_command_authority(config, request)
    if isinstance(existing, CommandEnvelope):
        return existing
    takeover = existing

    try:
        snapshot = _current_snapshot(config)
    except Exception:  # noqa: BLE001
        if takeover is not None:
            return _request_in_progress(request)
        return _backend_unavailable(
            request,
            "Current worker authority is temporarily unavailable",
        )

    health = _backend_health(snapshot)
    health_error = None if health.status == "healthy" else _backend_unavailable(
        request, "Herdr socket backend is not healthy", health=health
    )
    worker = _resolve_authoritative_worker(request, snapshot)
    if isinstance(worker, CommandEnvelope):
        if takeover is not None:
            return _request_in_progress(request)
        if health_error is not None:
            return health_error
        return worker
    if takeover is not None and worker.id != takeover.public_worker_id:
        return _duplicate_request(request)

    route: AcpPromptRoute | None = None
    status_error = None if worker.status not in _DISALLOWED_SEND_STATUSES else (
        _target_resolution_error(request, STATUS_REJECTED, [worker_candidate(worker)])
    )
    permanent_error = status_error or health_error
    if permanent_error is not None:
        canonical = build_canonical_mutation(request, public_worker_id=worker.id)
        reservation = _reserve_canonical_request(config, request, canonical)
        if isinstance(reservation, CommandEnvelope):
            return reservation
        return _finish_before_send(config, request, reservation, permanent_error)

    try:
        route = prompt_router(worker)
    except Exception:  # noqa: BLE001
        route = None
    if route is None:
        if takeover is not None:
            return _request_in_progress(request)
        return _backend_unavailable(request, "ACP worker route is unavailable")
    def submit_through(active_route: AcpPromptRoute) -> CommandEnvelope | None:
        try:
            binding_fingerprint = active_route.binding_fingerprint.strip()
        except Exception:  # noqa: BLE001
            binding_fingerprint = ""
        if not binding_fingerprint:
            if takeover is not None:
                return _request_in_progress(request)
            return _backend_unavailable(
                request, "ACP worker route has no durable authority"
            )

        canonical = build_canonical_mutation(request, public_worker_id=worker.id)
        reservation = _reserve_canonical_request(config, request, canonical)
        if isinstance(reservation, CommandEnvelope):
            return reservation

        send_started: Mapping[str, Any] | None = None

        class SendStartRejected(RuntimeError):
            pass

        def mark_send_started_at_transport_boundary() -> None:
            nonlocal send_started
            started = _mark_request_send_started(
                config,
                request,
                reservation,
                binding_fingerprint=binding_fingerprint,
                worker=worker,
                instruction_text=_instruction_text(request),
            )
            if isinstance(started, CommandEnvelope):
                raise SendStartRejected(started)
            if not isinstance(started, Mapping):
                raise SendStartRejected(
                    _recover_request(config, request, reservation.canonical)
                )
            send_started = started

        def retryable_before_transport() -> CommandEnvelope:
            if _abandon_request_reservation(config, request, reservation):
                return _backend_unavailable(
                    request,
                    "ACP prompt did not reach its transport boundary",
                )
            return _recover_request(config, request, reservation.canonical)

        try:
            use_steering = active_route.supports_steering
            submit = active_route.steer if use_steering else active_route.prompt
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
                steering_outcome = getattr(route_result, "value", route_result)
                if steering_outcome not in {"injected", "startedNewTurn"}:
                    rejected = steering_outcome == "failed"
                    return _finish_request(
                        config,
                        request,
                        reservation,
                        _instruction_failure_envelope(
                            request,
                            worker_id=worker.id,
                            verdict="steering_failed" if rejected else "unknown",
                            target_state=_target_state_at_send(worker),
                        ),
                        expected_state="send_started",
                        terminal_state="rejected" if rejected else "uncertain",
                    )
        except SendStartRejected as exc:
            return exc.args[0]
        except Exception:  # noqa: BLE001
            if send_started is None:
                return retryable_before_transport()
            return _finish_request(
                config,
                request,
                reservation,
                _instruction_failure_envelope(
                    request,
                    worker_id=worker.id,
                    verdict="unknown",
                    target_state=_target_state_at_send(worker),
                ),
                expected_state="send_started",
                terminal_state="uncertain",
            )

        if send_started is None:
            # A route that claims success without crossing the durable
            # transport boundary is not an accepted implementation.
            return retryable_before_transport()

        refreshed = _linked_submission_turn(config, request)
        observed_turn = refreshed if refreshed is not None else send_started
        accepted = _accepted_send_envelope(
            request,
            worker,
            observed_turn,
        )
        return _finish_request(
            config,
            request,
            reservation,
            accepted,
            expected_state="send_started",
            terminal_state="accepted",
        )

    try:
        preparation = route.prepare()
        active_route = preparation.__enter__()
    except Exception:  # noqa: BLE001 - no receipt or transport exists yet
        if takeover is not None:
            return _request_in_progress(request)
        return _backend_unavailable(
            request, "ACP worker route could not be prepared"
        )
    try:
        return submit_through(active_route)
    finally:
        preparation.__exit__(None, None, None)


def _submit_answer_decision(
    config: Config,
    request: CommandRequest,
    *,
    acp_permission_router: AcpPermissionDecisionRouter | None = None,
) -> CommandEnvelope:
    """Submit one validated live ACP permission decision."""
    existing = _existing_command_authority(config, request)
    if isinstance(existing, CommandEnvelope):
        return existing
    takeover = existing

    validated = _validate_pending_decision(config, request)
    pre_send = validated if isinstance(validated, tuple) else None
    if takeover is None and pre_send is not None:
        return pre_send[0]
    if takeover is not None and pre_send is not None and pre_send[1]:
        return _request_in_progress(request)
    if takeover is not None and pre_send is not None and pre_send[0].status == STATUS_ANSWER_IN_PROGRESS:
        return _answer_in_progress(request)
    worker_id = takeover.public_worker_id if takeover is not None else validated.worker_id
    if takeover is not None and pre_send is None and validated.worker_id != worker_id:
        pre_send = _duplicate_request(request), False
    canonical = build_canonical_mutation(request, public_worker_id=worker_id)

    if pre_send is None:
        try:
            owns_decision = bool(
                acp_permission_router is not None
                and acp_permission_router.owns_permission_decision(validated)
            )
        except Exception:
            owns_decision = False
        if not owns_decision:
            return _request_in_progress(request) if takeover is not None else (
                _backend_unavailable(
                    request, "ACP permission authority is temporarily unavailable"
                )
            )

    reservation = _reserve_canonical_request(config, request, canonical)
    if isinstance(reservation, CommandEnvelope):
        return reservation
    if pre_send is not None:
        return _finish_before_send(config, request, reservation, pre_send[0])
    return _answer_decision(
        config, request, validated, reservation, acp_permission_router
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
        or envelope.disposition != DISPOSITION_TERMINAL_ACCEPTED
        or not isinstance(envelope.result, Mapping)
    ):
        return envelope
    result = dict(envelope.result)
    result["submission_id"] = turn_submission_id(
        config.host_id,
        request.request_id or "",
    )
    linked_turn = _linked_submission_turn(config, request, settle=True)
    result["turn_id"] = linked_turn.get("id") if linked_turn is not None else None
    result["observed_turn_state"] = (
        "complete" if linked_turn is not None and linked_turn.get("complete") is True
        else "observed" if linked_turn is not None else "pending_observation"
    )
    return replace(
        envelope,
        result=result,
        schema_version=COMMAND_ENVELOPE_V3_SCHEMA_VERSION,
    )


def submit_command(
    config: Config,
    params: Mapping[str, Any] | str,
    *,
    acp_prompt_router: AcpPromptRouter | None = None,
    acp_permission_router: AcpPermissionDecisionRouter | None = None,
) -> CommandEnvelope:
    """Submit one command with ACP as the only instruction transport."""
    payload = params if isinstance(params, str) else json.dumps(
        dict(params), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    request, parse_error = parse_command_request(payload)
    if parse_error is not None:
        return CommandEnvelope.from_error(request, parse_error)
    validation_error = validate_request(request)
    if validation_error is not None:
        return CommandEnvelope.from_error(request, validation_error)
    if request.action not in _MUTATING_ACTIONS:
        return _execute_non_mutating(config, request)
    if request.dry_run:
        return _mutation_dry_run(request)
    if request.action == "send_instruction":
        envelope = _submit_instruction(
            config,
            request,
            prompt_router=acp_prompt_router or (lambda _worker: None),
        )
    else:
        envelope = _submit_answer_decision(
            config,
            request,
            acp_permission_router=acp_permission_router,
        )
    return _negotiated_submission_envelope(config, request, envelope)
