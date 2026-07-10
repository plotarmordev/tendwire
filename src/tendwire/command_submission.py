"""Authoritative daemon command submission path for Tendwire."""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from .config import Config
from .core.actions import CommandContext, execute_command
from .core.commands import (
    STATUS_ACCEPTED,
    STATUS_AMBIGUOUS_BACKEND_TARGET,
    STATUS_AMBIGUOUS_TARGET,
    STATUS_BACKEND_FAILED,
    STATUS_BACKEND_UNAVAILABLE,
    STATUS_BACKEND_UNSUPPORTED,
    STATUS_DUPLICATE_INSTRUCTION,
    STATUS_DUPLICATE_REQUEST,
    STATUS_NOT_FOUND,
    STATUS_PENDING,
    STATUS_REJECTED,
    STATUS_REQUEST_STATE_UNCERTAIN,
    STATUS_RESOLVED,
    STATUS_STALE_TARGET,
    CommandEnvelope,
    CommandRequest,
    error_value,
    has_nonblank_request_id,
    parse_command_request,
    resolve_target,
    validate_request,
    worker_candidate,
)
from .core.models import BackendHealth, Snapshot, Worker, WorkerBinding, stable_json_dumps
from .core.projector import project_from_observations
from .store.sqlite import (
    append_event,
    envelope_to_receipt_json,
    find_recent_matching_command_submission,
    latest_snapshot,
    list_worker_bindings,
    reserve_command_receipt,
    save_command_receipt,
    upsert_command_pending_turn,
)


HERDR_BACKEND = "herdr"
_DISALLOWED_SEND_STATUSES = frozenset({"closed", "failed", "unknown"})
_AMBIGUOUS_BINDING_REASONS = frozenset({"duplicate_backend_target", "not_unique"})
_SUBMIT_ENTER_DELAY_SECONDS = 0.2
_DUPLICATE_INSTRUCTION_REPLAY_WINDOW_SECONDS = 6 * 60 * 60
_DUPLICATE_INSTRUCTION_MIN_CHARS = 40
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


def _request_json(request: CommandRequest) -> str:
    return stable_json_dumps(request.to_dict())


def _default_socket_client_factory(config: Config) -> Any:
    from .backends.herdr_socket import HerdrSocketClient

    return HerdrSocketClient(timeout=config.herdr_timeout_seconds)


def _safe_event_payload(
    request: CommandRequest,
    *,
    status: str,
    envelope: CommandEnvelope | None = None,
    target_worker_id: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": 1,
        "action": request.action,
        "request_id": request.request_id,
        "dry_run": request.dry_run,
        "status": status,
    }
    if target_worker_id:
        payload["target"] = {"worker_id": target_worker_id}
    if envelope is not None:
        payload["envelope"] = envelope.to_dict()
    return payload


def _append_command_event(
    config: Config,
    event_type: str,
    request: CommandRequest,
    *,
    status: str,
    envelope: CommandEnvelope | None = None,
    target_worker_id: str | None = None,
) -> None:
    if config.db_path is None or not has_nonblank_request_id(request.request_id):
        return
    append_event(
        config.db_path,
        config.host_id,
        event_type,
        _safe_event_payload(
            request,
            status=status,
            envelope=envelope,
            target_worker_id=target_worker_id,
        ),
        aggregate_type="command",
        aggregate_id=request.request_id,
    )


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
    return CommandEnvelope.error(
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


def _resolve_authoritative_target(
    request: CommandRequest,
    snapshot: Snapshot,
    bindings: list[WorkerBinding],
) -> ResolvedCommandTarget | CommandEnvelope:
    resolved, candidates, status = resolve_target(
        request.target,
        list(snapshot.workers),
        allow_disallowed_status=True,
        include_backend_target=False,
    )
    if status != STATUS_RESOLVED:
        return _target_resolution_error(request, status, candidates)

    worker = next((item for item in snapshot.workers if item.id == (resolved or {}).get("worker_id")), None)
    if worker is None:
        return _target_resolution_error(request, STATUS_NOT_FOUND, [])
    if worker.status in _DISALLOWED_SEND_STATUSES:
        return _target_resolution_error(request, STATUS_REJECTED, [worker_candidate(worker)])

    return _binding_for_worker(request, worker, bindings)


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


def _private_pane_id_for_binding(client: Any, binding: WorkerBinding, *, timeout: float) -> str:
    if binding.target_kind == "pane_id":
        return str(binding.target_value or "").strip()
    response = _socket_request(
        client,
        "agent.get",
        {"target": binding.target_value},
        timeout=timeout,
    )
    return _pane_id_from_agent_info(response)


def _submit_private_pane_input(client: Any, pane_id: str, instruction_text: str, *, timeout: float) -> None:
    # Keep the reliable Telegram contract from the legacy path: clear any stale
    # staged input, write literal text, then press Enter to submit it. Herdr's
    # pane.send_input can leave text staged in some TUI states, while send_text
    # plus Enter matches the older CLI path Herdres used successfully. The
    # small delay mirrors the process/IO gap in that CLI path; without it, some
    # panes acknowledge Enter before the text is visible to the foreground app.
    # A single ctrl+u is not reliable across all foreground TUIs. Use a small
    # readline-compatible sequence before writing new text so a later Enter
    # cannot submit stale text left by an earlier uncertain send.
    for keys in _PRIVATE_PANE_CLEAR_KEY_SEQUENCES:
        _socket_request(
            client,
            "pane.send_keys",
            {"pane_id": pane_id, "keys": list(keys)},
            timeout=timeout,
        )
    _socket_request(
        client,
        "pane.send_text",
        {"pane_id": pane_id, "text": instruction_text},
        timeout=timeout,
    )
    time.sleep(_SUBMIT_ENTER_DELAY_SECONDS)
    _socket_request(
        client,
        "pane.send_keys",
        {"pane_id": pane_id, "keys": ["enter"]},
        timeout=timeout,
    )


def _submit_private_pane_keys(client: Any, pane_id: str, steps: list[Mapping[str, Any]], *, timeout: float) -> None:
    """Replay an ordered send_keys step list to a pane. Each step sends EITHER a run of neutral key
    tokens (pane.send_keys) or a run of literal text (pane.send_text). herdres builds the sequence
    that answers the TUI prompt (digit+enter, arrow toggles, or navigate + write-in); tendwire relays
    it verbatim. A small inter-step delay lets the foreground TUI repaint between groups."""
    for index, step in enumerate(steps):
        if index:
            time.sleep(_SUBMIT_ENTER_DELAY_SECONDS)
        if "text" in step:
            _socket_request(
                client,
                "pane.send_text",
                {"pane_id": pane_id, "text": str(step.get("text") or "")},
                timeout=timeout,
            )
        else:
            _socket_request(
                client,
                "pane.send_keys",
                {"pane_id": pane_id, "keys": [str(token) for token in (step.get("keys") or [])]},
                timeout=timeout,
            )


def _target_state_at_send(worker: Worker) -> str:
    status = str(worker.status or "").strip().lower().replace("-", "_")
    return status or "unknown"


def _instruction_text(request: CommandRequest) -> str:
    instruction = request.instruction if isinstance(request.instruction, dict) else {}
    text = instruction.get("text")
    return text if isinstance(text, str) else ""


def _duplicate_instruction_since() -> str:
    since = datetime.now(timezone.utc) - timedelta(seconds=_DUPLICATE_INSTRUCTION_REPLAY_WINDOW_SECONDS)
    return since.isoformat()


def _duplicate_instruction_envelope(
    config: Config,
    request: CommandRequest,
    worker: Worker,
) -> CommandEnvelope | None:
    if config.db_path is None:
        return None
    text = _instruction_text(request)
    if len(text.strip()) < _DUPLICATE_INSTRUCTION_MIN_CHARS:
        return None
    match = find_recent_matching_command_submission(
        config.db_path,
        config.host_id,
        action=request.action,
        worker_id=worker.id,
        worker_fingerprint=worker.fingerprint,
        instruction_text=text,
        since=_duplicate_instruction_since(),
        exclude_request_id=request.request_id or "",
    )
    if match is None:
        return None
    return CommandEnvelope.from_result(
        request,
        ok=True,
        status=STATUS_DUPLICATE_INSTRUCTION,
        result={
            "target": {"worker_id": worker.id},
            "delivery_state": "duplicate_suppressed",
            "deduplicated": True,
            "replay_window_seconds": _DUPLICATE_INSTRUCTION_REPLAY_WINDOW_SECONDS,
        },
    )


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
        error=error_value(STATUS_REQUEST_STATE_UNCERTAIN, message),
    )


def _socket_send_envelope(
    config: Config,
    request: CommandRequest,
    resolved: ResolvedCommandTarget,
    *,
    socket_client_factory: SocketClientFactory | None = None,
) -> CommandEnvelope:
    instruction = request.instruction or {}
    instruction_text = instruction.get("text")
    if not isinstance(instruction_text, str) or not instruction_text:
        return CommandEnvelope.from_result(
            request,
            ok=False,
            status=STATUS_BACKEND_FAILED,
            error=error_value(STATUS_BACKEND_FAILED, "instruction text is missing after validation"),
        )

    factory = socket_client_factory or _default_socket_client_factory
    client: Any | None = None
    try:
        client = factory(config)
        if not hasattr(client, "request"):
            raise TypeError("socket client does not expose generic request")
        if hasattr(client, "connect"):
            client.connect()
    except Exception:  # noqa: BLE001
        if client is not None and hasattr(client, "close"):
            try:
                client.close()
            except Exception:
                pass
        return _backend_unavailable(request, "Herdr socket could not be reached")

    try:
        pane_id = _private_pane_id_for_binding(
            client,
            resolved.binding,
            timeout=config.herdr_timeout_seconds,
        )
    except Exception as exc:  # noqa: BLE001
        from .backends.herdr_protocol import HerdrErrorResponse, HerdrProtocolError
        from .backends.herdr_socket import (
            HerdrSocketConnectionError,
            HerdrSocketDisconnectedError,
            HerdrSocketTimeoutError,
        )

        if hasattr(client, "close"):
            client.close()
        if isinstance(exc, HerdrErrorResponse):
            return _backend_failure(request, "Herdr socket could not resolve the private send target")
        if isinstance(
            exc,
            HerdrSocketConnectionError
            | HerdrSocketTimeoutError
            | HerdrSocketDisconnectedError
            | HerdrProtocolError,
        ) or isinstance(exc, OSError):
            return _backend_unavailable(request, "Herdr socket could not resolve the private send target")
        if isinstance(exc, (TypeError, ValueError)):
            return _backend_failure(request, "Herdr socket private send target is unsupported")
        raise

    if not pane_id:
        if hasattr(client, "close"):
            client.close()
        return _backend_failure(request, "Herdr socket private send target has no pane")

    _append_command_event(
        config,
        "command.send_started",
        request,
        status=STATUS_PENDING,
        target_worker_id=resolved.worker.id,
    )
    try:
        _submit_private_pane_input(
            client,
            pane_id,
            instruction_text,
            timeout=config.herdr_timeout_seconds,
        )
    except Exception as exc:  # noqa: BLE001
        from .backends.herdr_protocol import HerdrErrorResponse, HerdrProtocolError
        from .backends.herdr_socket import (
            HerdrSocketConnectionError,
            HerdrSocketDisconnectedError,
            HerdrSocketTimeoutError,
        )

        if isinstance(exc, HerdrErrorResponse):
            return _backend_failure(request, "Herdr socket pane input returned an error response")
        if isinstance(
            exc,
            HerdrSocketConnectionError
            | HerdrSocketTimeoutError
            | HerdrSocketDisconnectedError
            | HerdrProtocolError,
        ):
            return _backend_uncertain(request, "Herdr socket pane input state is uncertain after send start")
        if isinstance(exc, OSError):
            return _backend_uncertain(request, "Herdr socket pane input state is uncertain after send start")
        raise
    finally:
        if hasattr(client, "close"):
            client.close()

    return CommandEnvelope.from_result(
        request,
        ok=True,
        status=STATUS_ACCEPTED,
        result={
            "target": {"worker_id": resolved.worker.id},
            "delivery_state": "submitted",
            "transport_state": "submitted",
            "target_state_at_send": _target_state_at_send(resolved.worker),
            "observed_turn_state": "pending_observation",
        },
    )


def _socket_send_keys_envelope(
    config: Config,
    request: CommandRequest,
    resolved: ResolvedCommandTarget,
    *,
    socket_client_factory: SocketClientFactory | None = None,
) -> CommandEnvelope:
    """Deliver a send_keys request over the Herdr socket: resolve the neutral target to a pane and
    replay params.steps via pane.send_keys / pane.send_text. Mirrors _socket_send_envelope's
    connect/resolve/error handling, but replays a key/text step list instead of instruction text."""
    steps = list((request.params or {}).get("steps") or [])
    if not steps:
        return CommandEnvelope.from_result(
            request,
            ok=False,
            status=STATUS_BACKEND_FAILED,
            error=error_value(STATUS_BACKEND_FAILED, "send_keys steps are missing after validation"),
        )

    factory = socket_client_factory or _default_socket_client_factory
    client: Any | None = None
    try:
        client = factory(config)
        if not hasattr(client, "request"):
            raise TypeError("socket client does not expose generic request")
        if hasattr(client, "connect"):
            client.connect()
    except Exception:  # noqa: BLE001
        if client is not None and hasattr(client, "close"):
            try:
                client.close()
            except Exception:
                pass
        return _backend_unavailable(request, "Herdr socket could not be reached")

    from .backends.herdr_protocol import HerdrErrorResponse, HerdrProtocolError
    from .backends.herdr_socket import (
        HerdrSocketConnectionError,
        HerdrSocketDisconnectedError,
        HerdrSocketTimeoutError,
    )

    try:
        pane_id = _private_pane_id_for_binding(
            client,
            resolved.binding,
            timeout=config.herdr_timeout_seconds,
        )
    except Exception as exc:  # noqa: BLE001
        if hasattr(client, "close"):
            client.close()
        if isinstance(exc, HerdrErrorResponse):
            return _backend_failure(request, "Herdr socket could not resolve the private send target")
        if isinstance(
            exc,
            HerdrSocketConnectionError | HerdrSocketTimeoutError | HerdrSocketDisconnectedError | HerdrProtocolError,
        ) or isinstance(exc, OSError):
            return _backend_unavailable(request, "Herdr socket could not resolve the private send target")
        if isinstance(exc, (TypeError, ValueError)):
            return _backend_failure(request, "Herdr socket private send target is unsupported")
        raise

    if not pane_id:
        if hasattr(client, "close"):
            client.close()
        return _backend_failure(request, "Herdr socket private send target has no pane")

    _append_command_event(
        config,
        "command.send_started",
        request,
        status=STATUS_PENDING,
        target_worker_id=resolved.worker.id,
    )
    try:
        _submit_private_pane_keys(
            client,
            pane_id,
            steps,
            timeout=config.herdr_timeout_seconds,
        )
    except Exception as exc:  # noqa: BLE001
        if isinstance(exc, HerdrErrorResponse):
            return _backend_failure(request, "Herdr socket pane keys returned an error response")
        if isinstance(
            exc,
            HerdrSocketConnectionError | HerdrSocketTimeoutError | HerdrSocketDisconnectedError | HerdrProtocolError,
        ) or isinstance(exc, OSError):
            return _backend_uncertain(request, "Herdr socket pane keys state is uncertain after send start")
        raise
    finally:
        if hasattr(client, "close"):
            client.close()

    return CommandEnvelope.from_result(
        request,
        ok=True,
        status=STATUS_ACCEPTED,
        result={
            "target": {"worker_id": resolved.worker.id},
            "delivery_state": "submitted",
            "transport_state": "submitted",
            "target_state_at_send": _target_state_at_send(resolved.worker),
            "observed_turn_state": "pending_observation",
        },
    )


def _envelope_from_receipt(request: CommandRequest, receipt: Mapping[str, Any]) -> CommandEnvelope:
    if receipt.get("payload_fingerprint") != request.payload_fingerprint():
        return CommandEnvelope.error(
            request,
            error_value(
                STATUS_DUPLICATE_REQUEST,
                "request_id reused with a different payload",
            ),
        )
    if receipt.get("uncertain"):
        return CommandEnvelope.error(
            request,
            error_value(
                STATUS_REQUEST_STATE_UNCERTAIN,
                "previous request state is uncertain; not retrying mutation",
            ),
        )
    try:
        data = json.loads(str(receipt.get("result_json") or "{}"))
    except json.JSONDecodeError:
        return CommandEnvelope.error(
            request,
            error_value(
                STATUS_REQUEST_STATE_UNCERTAIN,
                "previous request result is unreadable; not retrying mutation",
            ),
        )
    if not isinstance(data, dict):
        return CommandEnvelope.error(
            request,
            error_value(
                STATUS_REQUEST_STATE_UNCERTAIN,
                "previous request result is unreadable; not retrying mutation",
            ),
        )
    return CommandEnvelope.from_dict(data)


# Actions that mutate a pane (real, non-dry-run send) and so need receipts + socket delivery.
_MUTATING_ACTIONS = frozenset({"send_instruction", "send_keys"})


def _reserve_mutating_request(config: Config, request: CommandRequest) -> CommandEnvelope | None:
    if request.action not in _MUTATING_ACTIONS or request.dry_run or not has_nonblank_request_id(request.request_id):
        return None
    if config.db_path is None:
        return _backend_unavailable(request, "command receipt store is unavailable")

    pending = CommandEnvelope.error(
        request,
        error_value(
            STATUS_REQUEST_STATE_UNCERTAIN,
            "request is pending backend mutation",
        ),
    )
    reservation = reserve_command_receipt(
        config.db_path,
        host_id=config.host_id,
        request_id=request.request_id,
        action=request.action,
        payload_fingerprint=request.payload_fingerprint(),
        pending_result_json=envelope_to_receipt_json(pending),
        status=STATUS_PENDING,
        request_json=_request_json(request),
    )
    if reservation["reserved"]:
        _append_command_event(config, "command.reserved", request, status=STATUS_PENDING)
        return None

    envelope = _envelope_from_receipt(request, reservation["receipt"])
    event_type = "command.cached"
    if envelope.status == STATUS_DUPLICATE_REQUEST:
        event_type = "command.duplicate"
    elif envelope.status == STATUS_REQUEST_STATE_UNCERTAIN:
        event_type = "command.uncertain"
    _append_command_event(config, event_type, request, status=envelope.status, envelope=envelope)
    return envelope


def _save_mutating_result(
    config: Config,
    request: CommandRequest,
    envelope: CommandEnvelope,
    *,
    worker: Worker | None = None,
) -> None:
    if request.action not in _MUTATING_ACTIONS or request.dry_run or not has_nonblank_request_id(request.request_id):
        return
    if config.db_path is None:
        return
    save_command_receipt(
        config.db_path,
        host_id=config.host_id,
        request_id=request.request_id,
        action=request.action,
        payload_fingerprint=request.payload_fingerprint(),
        status=envelope.status,
        result_json=envelope_to_receipt_json(envelope),
        uncertain=envelope.status == STATUS_REQUEST_STATE_UNCERTAIN,
    )
    if request.action == "send_instruction" and envelope.status == STATUS_ACCEPTED and worker is not None:
        upsert_command_pending_turn(
            config.db_path,
            config.host_id,
            worker,
            request_id=request.request_id,
            instruction_text=_instruction_text(request),
        )
    event_type = "command.submitted"
    if envelope.status == STATUS_DUPLICATE_INSTRUCTION:
        event_type = "command.duplicate_instruction"
    _append_command_event(
        config,
        event_type,
        request,
        status=envelope.status,
        envelope=envelope,
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


def submit_command(
    config: Config,
    params: Mapping[str, Any] | str,
    *,
    socket_client_factory: SocketClientFactory | None = None,
) -> CommandEnvelope:
    """Submit one command through the authoritative daemon/socket path."""
    payload = params if isinstance(params, str) else _raw_payload_from_mapping(params)
    request, parse_error = parse_command_request(payload)
    if parse_error is not None:
        if request is not None:
            return CommandEnvelope.error(request, parse_error)
        return CommandEnvelope.error(None, parse_error)

    validation_error = validate_request(request)
    if validation_error is not None:
        return CommandEnvelope.error(request, validation_error)

    if request.action not in _MUTATING_ACTIONS or request.dry_run:
        return _execute_non_mutating(config, request)

    receipt_envelope = _reserve_mutating_request(config, request)
    if receipt_envelope is not None:
        return receipt_envelope

    snapshot = _current_snapshot(config)
    envelope = _backend_health_error(config, request, snapshot)
    resolved_worker: Worker | None = None
    if envelope is None:
        bindings = []
        if config.db_path is not None:
            bindings = list_worker_bindings(config.db_path, config.host_id, backend=HERDR_BACKEND)
        resolved = _resolve_authoritative_target(request, snapshot, bindings)
        if isinstance(resolved, CommandEnvelope):
            envelope = resolved
        else:
            resolved_worker = resolved.worker
            if request.action == "send_keys":
                envelope = _socket_send_keys_envelope(
                    config,
                    request,
                    resolved,
                    socket_client_factory=socket_client_factory,
                )
            else:
                envelope = _duplicate_instruction_envelope(config, request, resolved.worker)
                if envelope is None:
                    envelope = _socket_send_envelope(
                        config,
                        request,
                        resolved,
                        socket_client_factory=socket_client_factory,
                    )

    _save_mutating_result(config, request, envelope, worker=resolved_worker)
    return envelope
