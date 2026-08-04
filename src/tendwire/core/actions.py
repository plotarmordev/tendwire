"""Pure action execution for Tendwire command requests.

This module implements the allowed action handlers for the milestone-1 command
contract. It performs no I/O and depends only on stdlib and sibling core
helpers. It must not import subprocess, backends, stores, Herdr, Herdres,
Telegram, or connector modules.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..config import Config
from .commands import (
    STATUS_AMBIGUOUS_TARGET,
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
)
from .projector import project_from_observations



@dataclass(frozen=True)
class CommandContext:
    """Pure context for executing a command request."""

    host_id: str
    workers: list[Worker]
    snapshot: Snapshot | None = None


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


def execute_command(request: CommandRequest, context: CommandContext) -> CommandEnvelope:
    """Execute a validated command request and return a neutral envelope."""
    validation_error = validate_request(request)
    if validation_error is not None:
        return CommandEnvelope.from_error(request, validation_error)

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

    return CommandEnvelope.from_error(
        request,
        error_value(STATUS_REJECTED, f"unknown action {request.action!r}"),
    )
