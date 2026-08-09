"""Authoritative daemon command submission path for Tendwire."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from typing import Any

from .config import Config
from .core import commands as contract
from .core.models import Worker
from .store import pending as pending_store
from .store import receipts
from .store.projection import latest_snapshot


_DISALLOWED_SEND_STATUSES = frozenset({"closed", "failed", "unknown"})
_TERMINAL_DISPOSITIONS = {
    "accepted": contract.DISPOSITION_TERMINAL_ACCEPTED,
    "rejected": contract.DISPOSITION_TERMINAL_REJECTED,
    "uncertain": contract.DISPOSITION_TERMINAL_UNCERTAIN,
}
_FAILURE_MESSAGES = {
    contract.STATUS_PENDING: "request is already in progress",
    contract.STATUS_ANSWER_IN_PROGRESS: "another request is currently answering this decision",
    contract.STATUS_DUPLICATE_REQUEST: "request_id reused with a different canonical mutation",
    contract.STATUS_DECISION_NOT_PENDING: "decision is not the worker's current pending decision",
    contract.STATUS_UNKNOWN_WORKER: "target worker does not exist or is not open",
    contract.STATUS_INVALID_SELECTION: "selection is invalid for the current decision",
    contract.STATUS_UNSUPPORTED_DECISION: "multi-question decisions are not supported",
}
_TARGET_MESSAGES = {
    contract.STATUS_STALE_TARGET: "target worker fingerprint does not match the current worker",
    contract.STATUS_AMBIGUOUS_TARGET: "target matches more than one worker",
    contract.STATUS_REJECTED: "target worker status does not allow instructions",
    contract.STATUS_NOT_FOUND: "no worker matches the target",
}
_PENDING_FAILURE_STATUS = {
    "already_claimed": contract.STATUS_ANSWER_IN_PROGRESS,
    "unknown_worker": contract.STATUS_UNKNOWN_WORKER,
    "invalid_selection": contract.STATUS_INVALID_SELECTION,
    "unsupported_decision": contract.STATUS_UNSUPPORTED_DECISION,
}
_RECEIPT_MESSAGES = {
    "missing": "stored request receipt is missing or malformed; not retrying mutation",
    "malformed": "stored request receipt is malformed; not retrying mutation",
    "illegal": "stored request receipt has an illegal state; not retrying mutation",
    "inconsistent": "stored request receipt is inconsistent; not retrying mutation",
    "unreadable": "stored request result is unreadable; not retrying mutation",
    "result_malformed": "stored request result is malformed; not retrying mutation",
}
def _attempt(call: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    try:
        return call(*args, **kwargs)
    except Exception:
        return None


def _backend_unavailable(
    request: contract.CommandRequest,
    message: str,
) -> contract.CommandEnvelope:
    error = contract.error_value(contract.STATUS_BACKEND_UNAVAILABLE, message)
    return contract.CommandEnvelope.from_error(request, error)


def _target_resolution_error(
    request: contract.CommandRequest,
    status: str,
    candidates: list[dict[str, Any]],
) -> contract.CommandEnvelope:
    if status not in _TARGET_MESSAGES:
        status, candidates = contract.STATUS_NOT_FOUND, []
    message = _TARGET_MESSAGES[status]
    if status == contract.STATUS_REJECTED and candidates:
        message = f"{message}: {candidates[0]['status']!r}"
    return contract.CommandEnvelope.from_result(
        request,
        ok=False,
        status=status,
        result={"candidates": candidates},
        error=contract.error_value(status, message),
    )


def _failure_envelope(
    request: contract.CommandRequest,
    status: str,
    message: str | None = None,
    *,
    disposition: str | None = None,
    result: dict[str, Any] | None = None,
) -> contract.CommandEnvelope:
    if disposition is None:
        disposition = {
            contract.STATUS_PENDING: contract.DISPOSITION_IN_PROGRESS,
            contract.STATUS_ANSWER_IN_PROGRESS: contract.DISPOSITION_IN_PROGRESS,
            contract.STATUS_DUPLICATE_REQUEST: contract.DISPOSITION_TERMINAL_REJECTED,
            contract.STATUS_REQUEST_STATE_UNCERTAIN: contract.DISPOSITION_TERMINAL_UNCERTAIN,
        }.get(status, contract.DISPOSITION_NO_RECEIPT)
    return contract.CommandEnvelope.from_result(
        request,
        ok=False,
        status=status,
        disposition=disposition,
        result=result,
        error=contract.error_value(status, message or _FAILURE_MESSAGES[status]),
    )


def _backend_uncertain(request: contract.CommandRequest, message: str) -> contract.CommandEnvelope:
    return _failure_envelope(request, contract.STATUS_REQUEST_STATE_UNCERTAIN, message)


def _request_in_progress(request: contract.CommandRequest) -> contract.CommandEnvelope:
    return _failure_envelope(request, contract.STATUS_PENDING)


def _answer_in_progress(request: contract.CommandRequest) -> contract.CommandEnvelope:
    return _failure_envelope(request, contract.STATUS_ANSWER_IN_PROGRESS)


def _duplicate_request(request: contract.CommandRequest) -> contract.CommandEnvelope:
    return _failure_envelope(request, contract.STATUS_DUPLICATE_REQUEST)


def _accepted(
    request: contract.CommandRequest,
    result: dict[str, Any],
) -> contract.CommandEnvelope:
    return contract.CommandEnvelope.from_result(
        request, ok=True, status=contract.STATUS_ACCEPTED,
        disposition=contract.DISPOSITION_TERMINAL_ACCEPTED, result=result,
    )


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
    strings = (worker_id, worker_fp, binding_fp, target, ref)
    valid = all(isinstance(value, str) and bool(value.strip()) for value in strings)
    valid = valid and kind in {"single", "multi", "plan"}
    valid = valid and type(count) is int and count >= 1 and isinstance(refs, tuple)
    valid = valid and (
        text is None and bool(refs)
        or isinstance(text, str) and bool(text) and not refs
    )
    return route if valid else None


def _receipt_error(request: contract.CommandRequest, kind: str) -> contract.CommandEnvelope:
    return _backend_uncertain(request, _RECEIPT_MESSAGES[kind])


def _decode_receipt(
    request: contract.CommandRequest,
    canonical: contract.CanonicalMutation,
    receipt: Any,
) -> contract.CommandEnvelope:
    if not isinstance(receipt, Mapping):
        return _receipt_error(request, "missing")
    required = {
        "request_id", "action", "canonical_version", "canonical_fingerprint",
        "canonical_request_json", "public_worker_id", "state", "status",
        "result_json",
    }
    if not required.issubset(receipt):
        return _receipt_error(request, "malformed")
    expected_identity = {
        "request_id": request.request_id,
        "action": canonical.action,
        "canonical_version": canonical.canonical_version,
        "canonical_fingerprint": canonical.fingerprint,
        "canonical_request_json": canonical.canonical_json,
        "public_worker_id": canonical.public_worker_id,
    }
    if type(receipt.get("canonical_version")) is not int or any(
        receipt.get(key) != value for key, value in expected_identity.items()
    ):
        return _duplicate_request(request)
    state = receipt.get("state")
    if state not in {"reserved", "send_started", "accepted", "rejected", "uncertain"}:
        return _receipt_error(request, "illegal")
    live = state in {"reserved", "send_started"}
    if live and receipt.get("status") != contract.STATUS_PENDING:
        return _receipt_error(request, "inconsistent")
    try:
        data = json.loads(receipt["result_json"])
        if not isinstance(data, dict):
            raise TypeError
    except (KeyError, TypeError, json.JSONDecodeError):
        return _request_in_progress(request) if live else _receipt_error(request, "unreadable")
    try:
        if data.get("schema_version") != contract.COMMAND_ENVELOPE_SCHEMA_VERSION:
            raise ValueError("unsupported stored envelope schema")
        envelope = contract.CommandEnvelope.from_dict(data)
    except (TypeError, ValueError):
        return (
            _request_in_progress(request)
            if live else _receipt_error(request, "result_malformed")
        )
    correlation = (envelope.action, envelope.request_id) == (
        request.action, request.request_id
    )
    valid = correlation and (
        envelope.status == contract.STATUS_PENDING
        if live else (
            envelope.status == receipt.get("status")
            and envelope.disposition == _TERMINAL_DISPOSITIONS[state]
        )
    )
    return envelope if valid else _receipt_error(request, "inconsistent")


@dataclass(frozen=True)
class _ReceiptTakeover:
    public_worker_id: str
    selector_proof: str


def _read_receipt(config: Config, request: contract.CommandRequest) -> Mapping[str, Any] | None:
    receipt = _attempt(
        receipts.get_command_request, config.db_path, config.host_id, request.request_id or ""
    )
    return receipt if isinstance(receipt, Mapping) else None


@dataclass(frozen=True)
class _Mutation:
    """One owned receipt moving through reserved, send_started, and terminal."""

    config: Config
    request: contract.CommandRequest
    canonical: contract.CanonicalMutation
    owner_token: str

    @classmethod
    def reserve(
        cls,
        config: Config,
        request: contract.CommandRequest,
        worker_id: str,
        takeover: _ReceiptTakeover | None,
    ) -> _Mutation | contract.CommandEnvelope:
        canonical = contract.build_canonical_mutation(request, public_worker_id=worker_id)
        proof = takeover.selector_proof if takeover else contract.build_selector_proof(request)
        try:
            pending_json = receipts.envelope_to_receipt_json(_request_in_progress(request))
            result = receipts.reserve_command_request(
                config.db_path,
                host_id=config.host_id,
                request_id=request.request_id or "",
                action=canonical.action,
                canonical_version=canonical.canonical_version,
                canonical_fingerprint=canonical.fingerprint,
                canonical_request_json=canonical.canonical_json,
                public_worker_id=canonical.public_worker_id,
                pending_result_json=pending_json,
                selector_proof=proof,
            )
        except Exception:  # the reserve may have committed before its reply was lost
            receipt = _read_receipt(config, request)
            if receipt is not None and receipt.get("selector_proof") != proof:
                return _duplicate_request(request)
            return (
                _decode_receipt(request, canonical, receipt)
                if receipt is not None
                else _backend_unavailable(request, "command receipt store is unavailable")
            )
        if not isinstance(result, Mapping):
            return _decode_receipt(request, canonical, _read_receipt(config, request))
        if result.get("status") == "request_id_conflict":
            return _duplicate_request(request)
        if result.get("status") != "reserved":
            return _decode_receipt(request, canonical, result.get("receipt"))
        receipt, token = result.get("receipt"), result.get("owner_token")
        valid = (
            isinstance(receipt, Mapping)
            and receipt.get("state") == "reserved"
            and isinstance(token, str)
            and bool(token)
            and _decode_receipt(request, canonical, receipt).status == contract.STATUS_PENDING
        )
        return cls(config, request, canonical, token) if valid else _decode_receipt(
            request, canonical, _read_receipt(config, request)
        )

    def readback(self) -> contract.CommandEnvelope:
        return _decode_receipt(self.request, self.canonical, _read_receipt(
            self.config, self.request
        ))

    def finish(
        self,
        envelope: contract.CommandEnvelope,
        *,
        expected: str,
        terminal: str,
        effect: Callable[[Any], Any] | None = None,
    ) -> contract.CommandEnvelope:
        result = replace(envelope, disposition=_TERMINAL_DISPOSITIONS[terminal])
        try:
            finished = receipts.finish_command_request(
                self.config.db_path,
                host_id=self.config.host_id,
                request_id=self.request.request_id or "",
                canonical_fingerprint=self.canonical.fingerprint,
                owner_token=self.owner_token,
                expected_state=expected,
                terminal_state=terminal,
                status=result.status,
                result_json=receipts.envelope_to_receipt_json(result),
                terminal_effect=effect,
            )
        except Exception:  # terminal commit may have succeeded before reply loss
            finished = None
        return (
            _decode_receipt(self.request, self.canonical, finished.get("receipt"))
            if isinstance(finished, Mapping)
            else self.readback()
        )

    def finish_before_send(self, envelope: contract.CommandEnvelope) -> contract.CommandEnvelope:
        uncertain = envelope.status == contract.STATUS_REQUEST_STATE_UNCERTAIN
        terminal = "uncertain" if uncertain else "rejected"
        return self.finish(envelope, expected="reserved", terminal=terminal)

    def abandon(self) -> bool:
        return bool(_attempt(
            receipts.abandon_command_request_reservation,
            self.config.db_path,
            host_id=self.config.host_id,
            request_id=self.request.request_id or "",
            canonical_fingerprint=self.canonical.fingerprint,
            owner_token=self.owner_token,
        ))

    def mark_started(
        self,
        binding_fingerprint: str,
        worker: Worker | None = None,
    ) -> contract.CommandEnvelope | Mapping[str, Any] | None:
        try:
            started = receipts.mark_command_send_started(
                self.config.db_path,
                host_id=self.config.host_id,
                request_id=self.request.request_id or "",
                canonical_fingerprint=self.canonical.fingerprint,
                owner_token=self.owner_token,
                binding_fingerprint=binding_fingerprint,
                send_started_effect=None,
                submission_worker=worker,
                instruction_text=self.request.instruction["text"] if worker is not None else None,
                submission_link_window_seconds=self.config.submission_link_window_seconds,
                submission_hard_ttl_seconds=self.config.submission_hard_ttl_seconds,
            )
        except Exception:  # send-start commit may have succeeded before reply loss
            return self.readback()
        receipt = started.get("receipt") if isinstance(started, Mapping) else None
        valid = (
            isinstance(receipt, Mapping)
            and started.get("status") == "send_started"
            and started.get("owner_token") == self.owner_token
            and receipt.get("state") == "send_started"
            and _decode_receipt(
                self.request, self.canonical, receipt
            ).status == contract.STATUS_PENDING
        )
        if valid:
            if worker is None:
                return None
            linked = receipts.linked_turn_for_submission(
                self.config.db_path,
                host_id=self.config.host_id,
                request_id=self.request.request_id or "",
            )
            return linked or {"id": None}
        if isinstance(receipt, Mapping):
            embedded = _decode_receipt(self.request, self.canonical, receipt)
            if embedded.status != contract.STATUS_REQUEST_STATE_UNCERTAIN:
                return embedded
            if receipt.get("state") in {"send_started", "uncertain"}:
                return embedded
        return self.readback()

    def finish_instruction_failure(
        self,
        worker: Worker,
        verdict: str,
    ) -> contract.CommandEnvelope:
        rejected = verdict == "steering_failed"
        status = contract.STATUS_REJECTED if rejected else contract.STATUS_REQUEST_STATE_UNCERTAIN
        target_state = str(worker.status or "").strip().lower().replace("-", "_") or "unknown"
        result = {
            "target": {"worker_id": worker.id},
            "delivery_state": "not_delivered" if rejected else "unknown",
            "transport_state": "not_submitted" if rejected else "unknown",
            "submission_verdict": verdict,
            "target_state_at_send": target_state,
        }
        message = (
            "instruction was not delivered"
            if rejected else "instruction delivery is unknown; not retrying mutation"
        )
        return self.finish(
            _failure_envelope(self.request, status, message, result=result),
            expected="send_started",
            terminal="rejected" if rejected else "uncertain",
        )

    def finish_permission_uncertain(
        self, claim_token: str, message: str,
    ) -> contract.CommandEnvelope:
        return self.finish(
            _backend_uncertain(self.request, message),
            expected="send_started",
            terminal="uncertain",
            effect=_pending_terminal_effect(self.config, claim_token),
        )


def _abandon_pending_claim(config: Config, claim_token: str) -> bool:
    return bool(
        _attempt(
            pending_store.abandon_backend_pending_decision_claim,
            config.db_path,
            config.host_id,
            claim_token,
        )
    )


def _linked_submission_turn(
    config: Config,
    request: contract.CommandRequest,
    *,
    settle: bool = False,
) -> Mapping[str, Any] | None:
    try:
        if settle:
            receipts.settle_submission_link_for_request(
                config.db_path, host_id=config.host_id,
                request_id=request.request_id or "",
            )
        turn = receipts.linked_turn_for_submission(
            config.db_path, host_id=config.host_id,
            request_id=request.request_id or "",
        )
    except Exception:  # noqa: BLE001
        return None
    return turn if isinstance(turn, Mapping) else None


def _pending_terminal_effect(
    config: Config,
    claim_token: str,
    *,
    accepted: bool = False,
) -> Callable[[Any], Any] | None:
    effect = _attempt(
        pending_store.backend_pending_decision_terminal_effect,
        host_id=config.host_id,
        claim_token=claim_token,
        accepted=accepted,
    )
    return effect if callable(effect) else None


def _pending_decision(config: Config, request: contract.CommandRequest, *, claim: bool) -> Any:
    params = request.params or {}
    return pending_store.claim_backend_pending_decision(
        config.db_path,
        config.host_id,
        (request.target or {})["worker_id"],
        params["decision_ref"],
        params["selection"],
        claim=claim,
    )


def _validate_pending_decision(
    config: Config,
    request: contract.CommandRequest,
) -> Any | contract.CommandEnvelope:
    try:
        validated = _pending_decision(config, request, claim=False)
    except Exception:
        return _backend_unavailable(request, "pending state store is unavailable")
    if validated.status == "validated" and _decision_route(validated) is not None:
        return validated
    if validated.status == "acp_authority_unavailable":
        return _backend_unavailable(
            request, "ACP permission authority is temporarily unavailable",
        )
    status = _PENDING_FAILURE_STATUS.get(
        validated.status, contract.STATUS_DECISION_NOT_PENDING
    )
    return _failure_envelope(
        request, status, disposition=contract.DISPOSITION_NO_RECEIPT
    )


def _claim_pending_decision(
    config: Config,
    request: contract.CommandRequest,
    validated: Any,
) -> Any | contract.CommandEnvelope:
    try:
        claim = _pending_decision(config, request, claim=True)
    except Exception:
        return _backend_uncertain(request, "pending decision claim state is uncertain")
    if (
        claim.status == "claimed"
        and isinstance(claim.claim_token, str)
        and claim.claim_token
        and _decision_route(validated) == _decision_route(claim)
    ):
        return claim
    if claim.status == "acp_authority_unavailable":
        return _backend_unavailable(
            request, "ACP permission authority is temporarily unavailable"
        )
    status = _PENDING_FAILURE_STATUS.get(claim.status, contract.STATUS_DECISION_NOT_PENDING)
    return _failure_envelope(request, status)


def _answer_decision(
    mutation: _Mutation,
    validated: Any,
    acp_permission_router: Any | None,
) -> contract.CommandEnvelope:
    config, request = mutation.config, mutation.request
    claim = _claim_pending_decision(config, request, validated)
    if isinstance(claim, contract.CommandEnvelope):
        if claim.status == contract.STATUS_ANSWER_IN_PROGRESS:
            mutation.abandon()
            return _answer_in_progress(request)
        if claim.status == contract.STATUS_BACKEND_UNAVAILABLE:
            if mutation.abandon():
                return claim
            claim = _backend_uncertain(
                request, "ACP permission reservation state is uncertain"
            )
        return mutation.finish_before_send(claim)
    claim_token = claim.claim_token

    send_start_error = mutation.mark_started(claim.binding_private_fingerprint)
    if send_start_error is not None:
        claim_released = _abandon_pending_claim(config, claim_token)
        if send_start_error.status == contract.STATUS_PENDING and not claim_released:
            return mutation.finish_before_send(
                _backend_uncertain(
                    request,
                    "pending decision claim could not be safely released",
                ),
            )
        return send_start_error

    try:
        started = pending_store.start_backend_pending_decision_send(
            config.db_path, config.host_id, claim_token
        )
    except Exception:
        _abandon_pending_claim(config, claim_token)
        return mutation.finish_permission_uncertain(
            claim_token, "pending decision start state is uncertain"
        )
    if (
        getattr(started, "status", None) != "started"
        or _decision_route(claim) != _decision_route(started)
    ):
        _abandon_pending_claim(config, claim_token)
        return mutation.finish_permission_uncertain(
            claim_token, "pending decision state is uncertain after send start"
        )

    try:
        if acp_permission_router is None:
            raise RuntimeError("ACP permission bridge is unavailable")
        acp_permission_router.answer_permission_decision(
            started,
            timeout=config.acp_request_timeout_seconds,
        )
    except Exception:  # noqa: BLE001
        return mutation.finish_permission_uncertain(
            claim_token, "permission decision state is uncertain after send start"
        )
    accepted = _accepted(
        request, {
            "target": {"worker_id": started.worker_id},
            "decision": {"decision_ref": (request.params or {}).get("decision_ref")},
            "delivery_state": "submitted",
            "transport_state": "submitted",
            "observed_pending_state": "pending_observation",
        },
    )
    effect = _pending_terminal_effect(config, claim_token, accepted=True)
    if effect is None:
        return mutation.readback()
    return mutation.finish(
        accepted,
        expected="send_started",
        terminal="accepted",
        effect=effect,
    )


def _receipt_authority(
    config: Config,
    request: contract.CommandRequest,
    receipt: Mapping[str, Any],
) -> contract.CommandEnvelope | _ReceiptTakeover:
    """Decide one existing host/request from stored evidence before authority.

    Returns the envelope this retry must get, or a takeover marker when the
    stored reservation was abandoned before any send and the normal path may
    re-drive it.
    """
    if receipt.get("action") != request.action:
        return _duplicate_request(request)

    version = receipt.get("canonical_version")
    stored = receipt.get("public_worker_id")
    if (
        isinstance(version, bool)
        or not isinstance(version, int)
        or version < 1
        or not isinstance(stored, str)
        or not stored
    ):
        return _receipt_error(request, "malformed")
    explicit = (request.target or {}).get("worker_id")
    if explicit is not None:
        if explicit != stored:
            return _duplicate_request(request)
    else:
        proof = receipt.get("selector_proof")
        if not contract.is_selector_proof(proof) or proof != contract.build_selector_proof(request):
            return _backend_uncertain(
                request, "stored request target cannot be proven; not retrying mutation"
            )
    canonical = contract.build_canonical_mutation(request, public_worker_id=stored)
    replay = _decode_receipt(request, canonical, receipt)
    state = receipt.get("state")
    if state in {"accepted", "rejected", "uncertain"}:
        return replay
    if replay.status != contract.STATUS_PENDING:
        return replay
    if state == "send_started":
        if receipts.command_reservation_is_live(receipt):
            return replay
        unknown = (
            _failure_envelope(
                request,
                contract.STATUS_REQUEST_STATE_UNCERTAIN,
                "instruction delivery is unknown after process recovery; not retrying mutation",
                result={
                "target": {"worker_id": stored},
                    "delivery_state": "unknown",
                    "transport_state": "unknown",
                    "submission_verdict": "unknown",
                },
            )
            if request.action == "send_instruction"
            else _backend_uncertain(
                request,
                "permission decision state is unknown after process recovery; "
                "not retrying mutation",
            )
        )
        try:
            recovered = receipts.recover_unresolved_command_send(
                config.db_path,
                host_id=config.host_id,
                request_id=request.request_id or "",
                canonical_fingerprint=canonical.fingerprint,
                unresolved_result_json=str(receipt.get("result_json") or ""),
                uncertain_result_json=receipts.envelope_to_receipt_json(unknown),
            )
        except Exception:  # noqa: BLE001
            return replay
        if not isinstance(recovered, Mapping):
            return replay
        return _decode_receipt(request, canonical, recovered.get("receipt"))
    if receipts.command_reservation_is_live(receipt):
        return replay
    # An abandoned reservation never reached a send. Re-driving it is the
    # existing state machine's recovery, not a replay of a finished mutation.
    proof = receipt.get("selector_proof")
    return _ReceiptTakeover(stored, proof if isinstance(proof, str) else "")


def _send_instruction_through_route(
    config: Config,
    request: contract.CommandRequest,
    worker: Worker,
    takeover: _ReceiptTakeover | None,
    active_route: Any,
) -> contract.CommandEnvelope:
    binding_fingerprint = _attempt(
        lambda: active_route.binding_fingerprint.strip()
    ) or ""
    if not binding_fingerprint:
        if takeover is not None:
            return _request_in_progress(request)
        return _backend_unavailable(
            request, "ACP worker route has no durable authority"
        )

    mutation = _Mutation.reserve(config, request, worker.id, takeover)
    if isinstance(mutation, contract.CommandEnvelope):
        return mutation

    send_started: Mapping[str, Any] | None = None

    class SendStartRejected(RuntimeError):
        pass

    def mark_send_started_at_transport_boundary() -> None:
        nonlocal send_started
        started = mutation.mark_started(binding_fingerprint, worker)
        if isinstance(started, contract.CommandEnvelope):
            raise SendStartRejected(started)
        if not isinstance(started, Mapping):
            raise SendStartRejected(mutation.readback())
        send_started = started

    def retryable_before_transport() -> contract.CommandEnvelope:
        if mutation.abandon():
            return _backend_unavailable(
                request,
                "ACP prompt did not reach its transport boundary",
            )
        return mutation.readback()

    try:
        use_steering = active_route.supports_steering
        submit = active_route.steer if use_steering else active_route.prompt
        route_result = submit(
            request.instruction["text"],
            producer_turn_id=contract.turn_submission_id(
                config.host_id,
                request.request_id or "",
            ),
            timeout=config.acp_request_timeout_seconds,
            on_send_start=mark_send_started_at_transport_boundary,
        )
        if use_steering:
            steering_outcome = getattr(route_result, "value", route_result)
            if steering_outcome not in {"injected", "startedNewTurn"}:
                return mutation.finish_instruction_failure(worker, "unknown")
    except SendStartRejected as exc:
        return exc.args[0]
    except Exception:  # noqa: BLE001
        if send_started is None:
            return retryable_before_transport()
        return mutation.finish_instruction_failure(worker, "unknown")

    if send_started is None:
        # A route that claims success without crossing the durable
        # transport boundary is not an accepted implementation.
        return retryable_before_transport()

    refreshed = _linked_submission_turn(config, request)
    observed_turn = refreshed if refreshed is not None else send_started
    raw_turn_id = observed_turn.get("id")
    observed_state = (
        "complete" if observed_turn.get("complete") is True else "observed"
    ) if str(observed_turn.get("source_turn_id") or "").strip() else "pending_observation"
    accepted = _accepted(
        request, {
            "target": {"worker_id": worker.id},
            "delivery_state": "submitted",
            "transport_state": "submitted",
            "target_state_at_send": (
                str(worker.status or "").strip().lower().replace("-", "_") or "unknown"
            ),
            "turn_id": raw_turn_id if isinstance(raw_turn_id, str) and raw_turn_id else None,
            "observed_turn_state": observed_state,
            "submission_verdict": "submitted",
        },
    )
    return mutation.finish(
        accepted,
        expected="send_started",
        terminal="accepted",
    )


def _submit_instruction(
    config: Config,
    request: contract.CommandRequest,
    takeover: _ReceiptTakeover | None,
    *,
    prompt_router: Callable[[Worker], Any | None],
) -> contract.CommandEnvelope:
    """Submit one validated live instruction through its exact ACP route."""
    def retry_or(error: contract.CommandEnvelope) -> contract.CommandEnvelope:
        return _request_in_progress(request) if takeover is not None else error

    try:
        snapshot = latest_snapshot(config.db_path, config.host_id)
        if snapshot is None:
            raise RuntimeError("persisted worker authority is unavailable")
    except Exception:  # noqa: BLE001
        return retry_or(
            _backend_unavailable(
                request, "Current worker authority is temporarily unavailable"
            )
        )

    resolved, candidates, status = contract.resolve_target(
        request.target, list(snapshot.workers), allow_disallowed_status=True
    )
    worker = next(
        (item for item in snapshot.workers if item.id == (resolved or {}).get("worker_id")),
        None,
    )
    if status != "resolved" or worker is None:
        return retry_or(
            _target_resolution_error(
                request, status if status != "resolved" else contract.STATUS_NOT_FOUND, candidates
            )
        )
    if takeover is not None and worker.id != takeover.public_worker_id:
        return _duplicate_request(request)

    if worker.status in _DISALLOWED_SEND_STATUSES:
        status_error = _target_resolution_error(
            request, contract.STATUS_REJECTED, [contract.worker_candidate(worker)]
        )
        mutation = _Mutation.reserve(config, request, worker.id, takeover)
        return (
            mutation
            if isinstance(mutation, contract.CommandEnvelope)
            else mutation.finish_before_send(status_error)
        )

    try:
        route = prompt_router(worker)
    except Exception:  # noqa: BLE001
        route = None
    if route is None:
        return retry_or(_backend_unavailable(request, "ACP worker route is unavailable"))

    try:
        preparation = route.prepare()
        active_route = preparation.__enter__()
    except Exception:  # noqa: BLE001 - no receipt or transport exists yet
        return retry_or(
            _backend_unavailable(request, "ACP worker route could not be prepared")
        )
    try:
        return _send_instruction_through_route(
            config, request, worker, takeover, active_route
        )
    finally:
        preparation.__exit__(None, None, None)


def _submit_answer_decision(
    config: Config,
    request: contract.CommandRequest,
    takeover: _ReceiptTakeover | None,
    *,
    acp_permission_router: Any | None = None,
) -> contract.CommandEnvelope:
    """Submit one validated live ACP permission decision."""
    validated = _validate_pending_decision(config, request)
    pre_send = validated if isinstance(validated, contract.CommandEnvelope) else None
    if takeover is None and pre_send is not None:
        return pre_send
    if takeover is not None and pre_send is not None:
        if pre_send.status == contract.STATUS_BACKEND_UNAVAILABLE:
            return _request_in_progress(request)
        if pre_send.status == contract.STATUS_ANSWER_IN_PROGRESS:
            return _answer_in_progress(request)
    validated_route = None if pre_send is not None else validated
    worker_id = (
        takeover.public_worker_id
        if takeover is not None
        else validated_route.worker_id
    )
    if (
        takeover is not None
        and validated_route is not None
        and validated_route.worker_id != worker_id
    ):
        pre_send = _duplicate_request(request)
    if pre_send is None:
        owns_decision = bool(
            acp_permission_router is not None
            and _attempt(acp_permission_router.owns_permission_decision, validated_route)
        )
        if not owns_decision:
            return _request_in_progress(request) if takeover is not None else (
                _backend_unavailable(
                    request, "ACP permission authority is temporarily unavailable"
                )
            )

    mutation = _Mutation.reserve(config, request, worker_id, takeover)
    if isinstance(mutation, contract.CommandEnvelope):
        return mutation
    if pre_send is not None:
        return mutation.finish_before_send(pre_send)
    return _answer_decision(mutation, validated, acp_permission_router)


def submit_command(
    config: Config,
    params: Mapping[str, Any] | str,
    *,
    acp_prompt_router: Callable[[Worker], Any | None] | None = None,
    acp_permission_router: Any | None = None,
) -> contract.CommandEnvelope:
    """Submit one command with ACP as the only instruction transport."""
    payload = params if isinstance(params, str) else json.dumps(
        dict(params), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    request, parse_error = contract.parse_command_request(payload)
    if parse_error is not None:
        return contract.CommandEnvelope.from_error(request, parse_error)
    validation_error = contract.validate_request(request)
    if validation_error is not None:
        return contract.CommandEnvelope.from_error(request, validation_error)
    receipt = _read_receipt(config, request)
    existing = _receipt_authority(config, request, receipt) if receipt is not None else None
    if isinstance(existing, contract.CommandEnvelope):
        envelope = existing
    elif request.action == "send_instruction":
        envelope = _submit_instruction(
            config,
            request,
            existing,
            prompt_router=acp_prompt_router or (lambda _worker: None),
        )
    else:
        envelope = _submit_answer_decision(
            config,
            request,
            existing,
            acp_permission_router=acp_permission_router,
        )
    if (
        request.action == "send_instruction"
        and envelope.disposition == contract.DISPOSITION_TERMINAL_ACCEPTED
        and isinstance(envelope.result, Mapping)
    ):
        result = dict(envelope.result)
        result["submission_id"] = contract.turn_submission_id(
            config.host_id, request.request_id or ""
        )
        linked = _linked_submission_turn(config, request, settle=True)
        result["turn_id"] = linked.get("id") if linked is not None else None
        result["observed_turn_state"] = (
            "complete" if linked is not None and linked.get("complete") is True
            else "observed" if linked is not None else "pending_observation"
        )
        envelope = replace(envelope, result=result)
    return contract.validate_public_command_envelope(
        envelope, request
    )
