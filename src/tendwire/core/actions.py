"""Pure action execution for Tendwire command requests.

This module implements the allowed action handlers for the milestone-1 command
contract. It performs no I/O and depends only on stdlib and sibling core
helpers. It must not import subprocess, backends, stores, Herdr, Herdres,
Telegram, or connector modules.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from ..config import Config
from .commands import (
    STATUS_AMBIGUOUS_TARGET,
    STATUS_AMBIGUOUS_BACKEND_TARGET,
    STATUS_BACKEND_UNSUPPORTED,
    STATUS_DRY_RUN,
    STATUS_NOOP,
    STATUS_NOT_FOUND,
    STATUS_REJECTED,
    STATUS_RESOLVED,
    STATUS_SNAPSHOT,
    STATUS_STALE_TARGET,
    CommandEnvelope,
    CommandRequest,
    Snapshot,
    Worker,
    error_value,
    resolve_target,
    snapshot_result,
    validate_request,
    worker_candidate,
)
from .projector import project_from_observations


SendInstructionCallback = Callable[[dict[str, Any], dict[str, Any]], CommandEnvelope]


@dataclass(frozen=True)
class CommandContext:
    """Pure context for executing a command request."""

    host_id: str
    workers: list[Worker]
    snapshot: Snapshot | None = None
    backend_sender: SendInstructionCallback | None = None


def _config_for_host(host_id: str) -> Config:
    return Config(host_id=host_id)


def _noop_result(request: CommandRequest) -> CommandEnvelope:
    return CommandEnvelope.from_result(request, ok=True, status=STATUS_NOOP, result={})


def _read_snapshot_result(request: CommandRequest, snapshot: Snapshot) -> CommandEnvelope:
    return CommandEnvelope.from_result(
        request,
        ok=True,
        status=STATUS_SNAPSHOT,
        result=snapshot_result(snapshot),
    )


def _resolve_target_result(request: CommandRequest, workers: list[Worker]) -> CommandEnvelope:
    resolved, candidates, status = resolve_target(request.target, workers)
    if status == STATUS_RESOLVED:
        return CommandEnvelope.from_result(
            request,
            ok=True,
            status=STATUS_RESOLVED,
            result={"target": resolved},
        )
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
        return CommandEnvelope.from_result(
            request,
            ok=False,
            status=STATUS_REJECTED,
            result={"candidates": candidates},
            error=error_value(
                STATUS_REJECTED,
                f"target worker status does not allow instructions: {candidates[0]['status']!r}",
            ),
        )
    return CommandEnvelope.from_result(
        request,
        ok=False,
        status=STATUS_NOT_FOUND,
        result={"candidates": []},
        error=error_value(STATUS_NOT_FOUND, "no worker matches the target"),
    )


def _send_instruction_result(request: CommandRequest, context: CommandContext) -> CommandEnvelope:
    resolved, candidates, status = resolve_target(
        request.target,
        context.workers,
        allow_disallowed_status=True,
        include_backend_target=True,
    )
    if status != STATUS_RESOLVED:
        return _resolve_target_result(request, context.workers)

    # Even though resolve_target succeeded, send_instruction must reject workers
    # whose current status is closed, failed, or unknown.
    resolved_worker = next(
        (w for w in context.workers if w.id == (resolved or {}).get("worker_id")),
        None,
    )
    disallowed = {"closed", "failed", "unknown"}
    if resolved_worker is not None and resolved_worker.status in disallowed:
        return CommandEnvelope.from_result(
            request,
            ok=False,
            status=STATUS_REJECTED,
            result={"candidates": [worker_candidate(resolved_worker)]},
            error=error_value(
                STATUS_REJECTED,
                f"target worker status does not allow instructions: {resolved_worker.status!r}",
            ),
        )

    instruction = request.instruction or {}
    target = resolved or {}
    text = instruction.get("text", "")

    if request.dry_run:
        public_target = worker_candidate(resolved_worker) if resolved_worker is not None else target
        return CommandEnvelope.from_result(
            request,
            ok=True,
            status=STATUS_DRY_RUN,
            result={"target": public_target, "instruction": {"text": text}},
        )

    backend_target = target.get("backend_target")
    backend_reason = ""
    if isinstance(backend_target, dict):
        backend_reason = str(backend_target.get("reason") or "")
    if not isinstance(backend_target, dict) or backend_target.get("sendable") is not True:
        if backend_reason in {"duplicate_backend_target", "not_unique"}:
            return CommandEnvelope.from_result(
                request,
                ok=False,
                status=STATUS_AMBIGUOUS_BACKEND_TARGET,
                error=error_value(
                    STATUS_AMBIGUOUS_BACKEND_TARGET,
                    "target resolves to an ambiguous backend send target",
                ),
            )
        return CommandEnvelope.from_result(
            request,
            ok=False,
            status=STATUS_BACKEND_UNSUPPORTED,
            error=error_value(
                STATUS_BACKEND_UNSUPPORTED,
                "target has no backend-owned sendable private binding",
            ),
        )

    if context.backend_sender is None:
        return CommandEnvelope.from_result(
            request,
            ok=False,
            status=STATUS_BACKEND_UNSUPPORTED,
            error=error_value(
                STATUS_BACKEND_UNSUPPORTED,
                "send_instruction is not supported by this backend in this milestone",
            ),
        )

    backend_envelope = context.backend_sender(target, instruction)
    # Rebuild the backend envelope against the original request so that
    # caller context such as request_id and dry_run is always preserved.
    return CommandEnvelope.from_result(
        request,
        ok=backend_envelope.ok,
        status=backend_envelope.status,
        result=backend_envelope.result,
        error=backend_envelope.error,
        warnings=backend_envelope.warnings,
    )


def _send_keys_result(request: CommandRequest, context: CommandContext) -> CommandEnvelope:
    """Pure-path handling for send_keys: resolve the target and preview a dry run. The real key
    replay is delivered through the daemon socket path (command_submission), so a non-dry-run
    send_keys that reaches this pure path (no socket backend) reports backend_unsupported."""
    resolved, _candidates, status = resolve_target(
        request.target,
        context.workers,
        allow_disallowed_status=True,
        include_backend_target=True,
    )
    if status != STATUS_RESOLVED:
        return _resolve_target_result(request, context.workers)
    resolved_worker = next(
        (w for w in context.workers if w.id == (resolved or {}).get("worker_id")),
        None,
    )
    if resolved_worker is not None and resolved_worker.status in {"closed", "failed", "unknown"}:
        return CommandEnvelope.from_result(
            request,
            ok=False,
            status=STATUS_REJECTED,
            result={"candidates": [worker_candidate(resolved_worker)]},
            error=error_value(
                STATUS_REJECTED,
                f"target worker status does not allow input: {resolved_worker.status!r}",
            ),
        )
    steps = (request.params or {}).get("steps") or []
    if request.dry_run:
        public_target = worker_candidate(resolved_worker) if resolved_worker is not None else (resolved or {})
        return CommandEnvelope.from_result(
            request,
            ok=True,
            status=STATUS_DRY_RUN,
            result={"target": public_target, "steps": len(steps)},
        )
    return CommandEnvelope.from_result(
        request,
        ok=False,
        status=STATUS_BACKEND_UNSUPPORTED,
        error=error_value(
            STATUS_BACKEND_UNSUPPORTED,
            "send_keys is delivered through the daemon socket path",
        ),
    )


def execute_command(request: CommandRequest, context: CommandContext) -> CommandEnvelope:
    """Execute a validated command request and return a neutral envelope."""
    validation_error = validate_request(request)
    if validation_error is not None:
        return CommandEnvelope.error(request, validation_error)

    if request.action == "noop":
        return _noop_result(request)

    if request.action == "read_snapshot":
        snapshot = context.snapshot
        if snapshot is None:
            snapshot = project_from_observations(
                _config_for_host(context.host_id),
                spaces=[],
                workers=list(context.workers),
            )
        return _read_snapshot_result(request, snapshot)

    if request.action == "resolve_target":
        return _resolve_target_result(request, context.workers)

    if request.action == "send_instruction":
        return _send_instruction_result(request, context)

    if request.action == "send_keys":
        return _send_keys_result(request, context)

    return CommandEnvelope.error(
        request,
        error_value(STATUS_REJECTED, f"unknown action {request.action!r}"),
    )
