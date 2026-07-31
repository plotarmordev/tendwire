"""Tendwire command-line interface.

Console script entry point: tendwire = tendwire.cli:main
Module entry point: python -m tendwire.cli snapshot --json
"""

from __future__ import annotations

import argparse
import json
import re
import socket
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .backends.herdr_cli import (
    bindings_from_workers,
    diagnose_herdr,
    fetch_herdr_snapshot_observation,
    fetch_herdr_state,
    herdr_backend_health,
    rehydrate_workers_from_bindings,
)
from .backends.herdr_turns import refresh_structured_turn_content
from .config import Config, load_config
from .core.actions import CommandContext, execute_command
from .core.attention import attention_payload_from_snapshot
from .core.commands import (
    STATUS_BACKEND_UNAVAILABLE,
    CommandEnvelope,
    error_value,
    parse_command_request,
    validate_request,
)
from .core.projector import project_from_observations
from .core.models import (
    BackendHealth,
    WorkerBinding,
    public_json_dumps,
    sanitize_public_mapping,
    separate_duplicate_worker_bindings,
)
from .core.turns import (
    TURN_DELTA_DEFAULT_LIMIT,
    TURN_DELTA_MAX_LIMIT,
    TURN_LIST_DEFAULT_LIMIT,
    TURN_LIST_MAX_LIMIT,
    turns_payload_from_snapshot,
)
from .local_state import repair_config_state
from .store.sqlite import (
    CompactionOptions,
    attention_payload_from_store,
    compact_store,
    expire_stale_worker_bindings,
    latest_healthy_backend_snapshot,
    latest_snapshot,
    list_worker_bindings,
    pending_payload_from_store,
    run_store_maintenance,
    store_status,
    tail_event_metadata,
    turns_payload_from_store,
    turn_delta_payload_from_store,
    upsert_worker_bindings,
)


_HERDR_BACKEND = "herdr"
_DEFAULT_FETCH_HERDR_STATE = fetch_herdr_state
_DAEMON_FAST_CLIENT_TIMEOUT_SECONDS = 2.0
_DAEMON_CONTENT_CLIENT_TIMEOUT_SECONDS = 10.0
_DAEMON_CONNECTOR_CLIENT_TIMEOUT_SECONDS = 30.0
_DAEMON_COMMAND_CLIENT_TIMEOUT_FLOOR_SECONDS = 2.0
_DAEMON_COMMAND_CLIENT_TIMEOUT_GRACE_SECONDS = 0.5


@dataclass(frozen=True)
class _DaemonAttempt:
    result: dict[str, Any] | None = None
    response_error: dict[str, Any] | None = None
    error_kind: str | None = None
    request_started: bool | None = None


def _daemon_client_timeout_seconds(config: Config, method: str) -> float:
    if method == "command.submit":
        return max(
            _DAEMON_COMMAND_CLIENT_TIMEOUT_FLOOR_SECONDS,
            float(config.herdr_timeout_seconds) + _DAEMON_COMMAND_CLIENT_TIMEOUT_GRACE_SECONDS,
        )
    if method in {"turn.list", "turn.content.get", "turn.delta"}:
        return _DAEMON_CONTENT_CLIENT_TIMEOUT_SECONDS
    if method.startswith("connector."):
        return _DAEMON_CONNECTOR_CLIENT_TIMEOUT_SECONDS
    return _DAEMON_FAST_CLIENT_TIMEOUT_SECONDS


def _turn_list_limit(value: str) -> int:
    try:
        limit = int(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("limit must be an integer") from exc
    if not 1 <= limit <= TURN_LIST_MAX_LIMIT:
        raise argparse.ArgumentTypeError(
            f"limit must be between 1 and {TURN_LIST_MAX_LIMIT}"
        )
    return limit


def _turn_delta_limit(value: str) -> int:
    try:
        limit = int(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("limit must be an integer") from exc
    if not 1 <= limit <= TURN_DELTA_MAX_LIMIT:
        raise argparse.ArgumentTypeError(
            f"limit must be between 1 and {TURN_DELTA_MAX_LIMIT}"
        )
    return limit


def _connector_inspect_limit(value: str) -> int:
    try:
        limit = int(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("limit must be an integer") from exc
    if not 1 <= limit <= 100:
        raise argparse.ArgumentTypeError("limit must be between 1 and 100")
    return limit


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="tendwire",
        description="Local-first control plane for terminal-based agents.",
    )
    parser.add_argument(
        "--host-id",
        dest="host_id",
        default=None,
        help="Override the host identifier used in snapshots.",
    )
    parser.add_argument(
        "--herdr-bin",
        dest="herdr_bin",
        default=None,
        help="Path or name of the herdr binary (default: herdr).",
    )
    parser.add_argument(
        "--herdr-timeout",
        dest="herdr_timeout_seconds",
        default=None,
        help="Seconds to wait for each Herdr CLI probe (default: 5.0).",
    )
    parser.add_argument(
        "--socket-path",
        dest="socket_path",
        default=None,
        help="Unix socket path for daemon-backed requests when explicitly enabled.",
    )

    subparsers = parser.add_subparsers(dest="command")

    snapshot_parser = subparsers.add_parser(
        "snapshot",
        help="Print a neutral device-independent snapshot.",
    )
    snapshot_parser.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        default=True,
        help="Print snapshot as JSON (default).",
    )
    snapshot_parser.add_argument(
        "--store",
        dest="store_snapshot",
        action="store_true",
        default=False,
        help="Persist the snapshot to the sqlite store without changing stdout.",
    )
    snapshot_parser.add_argument(
        "--db-path",
        dest="db_path",
        default=None,
        help="SQLite database path to use with --store (default: config path).",
    )

    attention_parser = subparsers.add_parser(
        "attention",
        help="Print neutral public attention items.",
    )
    attention_parser.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        default=True,
        help="Print attention as JSON (default).",
    )
    attention_parser.add_argument(
        "--store",
        dest="store_snapshot",
        action="store_true",
        default=False,
        help="Persist a fresh snapshot before listing store-backed attention.",
    )
    attention_parser.add_argument(
        "--db-path",
        dest="db_path",
        default=None,
        help="SQLite database path for store-backed attention (default: config path).",
    )

    turns_parser = subparsers.add_parser(
        "turns",
        help="Print neutral public turns derived from the current snapshot.",
    )
    turns_parser.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        default=True,
        help="Print turns as JSON (default).",
    )
    turns_parser.add_argument(
        "--schema-version",
        dest="schema_version",
        type=int,
        choices=(1, 2),
        default=1,
        help="Turn-list schema version (default: 1).",
    )
    turns_parser.add_argument(
        "--limit",
        type=_turn_list_limit,
        default=TURN_LIST_DEFAULT_LIMIT,
        help=(
            "Maximum turns in this page "
            f"(default: {TURN_LIST_DEFAULT_LIMIT}, maximum: {TURN_LIST_MAX_LIMIT})."
        ),
    )
    turn_page_position = turns_parser.add_mutually_exclusive_group()
    turn_page_position.add_argument(
        "--cursor",
        default=None,
        help="Opaque cursor for the next page.",
    )
    turn_page_position.add_argument(
        "--since",
        default=None,
        help="Opaque token for turns newer than a completed traversal.",
    )
    turns_parser.add_argument(
        "--db-path",
        dest="db_path",
        default=None,
        help="SQLite database path for store-backed turns (default: config path).",
    )

    turn_parser = subparsers.add_parser(
        "turn",
        help="Access bounded canonical turn content.",
    )
    turn_actions = turn_parser.add_subparsers(dest="turn_action", required=True)
    content_parser = turn_actions.add_parser("content", help="Access turn content.")
    content_actions = content_parser.add_subparsers(dest="content_action", required=True)
    content_get = content_actions.add_parser("get", help="Fetch one bounded content page.")
    content_get.add_argument("--json", dest="json_output", action="store_true", default=True)
    content_get.add_argument("--turn-id", dest="turn_id", required=True)
    content_get.add_argument("--revision", dest="content_revision", required=True)
    content_get.add_argument(
        "--field",
        choices=("user_text", "assistant_final_text"),
        required=True,
    )
    content_get.add_argument("--cursor", default=None)
    content_get.add_argument("--db-path", dest="db_path", default=None)
    delta_parser = turn_actions.add_parser(
        "delta",
        help="Read one cache-only public turn change page.",
    )
    delta_parser.add_argument("--json", dest="json_output", action="store_true", default=True)
    delta_parser.add_argument(
        "--limit", type=_turn_delta_limit, default=TURN_DELTA_DEFAULT_LIMIT,
    )
    delta_position = delta_parser.add_mutually_exclusive_group()
    delta_position.add_argument("--watermark", default=None)
    delta_position.add_argument("--cursor", default=None)
    delta_parser.add_argument("--db-path", dest="db_path", default=None)

    pending_parser = subparsers.add_parser(
        "pending",
        help="Print neutral public pending interactions from daemon or durable store state.",
    )
    pending_parser.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        default=True,
        help="Print pending interactions as JSON (default).",
    )
    pending_parser.add_argument(
        "--db-path",
        dest="db_path",
        default=None,
        help="SQLite database path for store-backed pending fallback (default: config path).",
    )

    command_parser = subparsers.add_parser(
        "command",
        help="Read a JSON command request from stdin and print a JSON result.",
    )
    command_parser.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        default=True,
        help="Print result as JSON (default).",
    )
    command_parser.add_argument(
        "--db-path",
        dest="db_path",
        default=None,
        help="SQLite database path for command receipts (default: config path).",
    )

    daemon_parser = subparsers.add_parser(
        "daemon",
        help="Run the local Tendwire JSON request daemon.",
    )
    daemon_parser.add_argument(
        "--db-path",
        dest="db_path",
        default=None,
        help="SQLite database path for daemon state (default: config path).",
    )
    daemon_parser.add_argument(
        "--socket-path",
        dest="socket_path",
        default=argparse.SUPPRESS,
        help="Unix socket path to listen on (default: data_dir/tendwire.sock).",
    )
    daemon_parser.add_argument(
        "--socket-group",
        dest="socket_group",
        default=argparse.SUPPRESS,
        metavar="GROUP",
        help="Share the daemon socket with a validated local group.",
    )

    doctor_parser = subparsers.add_parser(
        "doctor",
        help="Print read-only Herdr diagnostics.",
    )
    doctor_parser.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        default=True,
        help="Print diagnostics as JSON (default).",
    )

    _add_store_parser(subparsers)
    _add_connector_parser(subparsers)

    return parser


def _add_store_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    store_parser = subparsers.add_parser(
        "store",
        help="Run bounded public-safe store operations with JSON-only output.",
    )
    actions = store_parser.add_subparsers(dest="store_action", required=True)

    def add_common(action_parser: argparse.ArgumentParser) -> None:
        action_parser.add_argument("--db-path", dest="db_path", default=None)

    status_parser = actions.add_parser("status", help="Print host-scoped store status.")
    add_common(status_parser)

    tail_parser = actions.add_parser("events-tail", help="Print bounded event metadata.")
    add_common(tail_parser)
    tail_parser.add_argument("--limit", type=int, default=20)

    cleanup_parser = actions.add_parser("cleanup", help="Run bounded maintenance cleanup.")
    add_common(cleanup_parser)
    cleanup_parser.add_argument("--dry-run", dest="dry_run", action="store_true", default=False)
    cleanup_parser.add_argument("--retention-days", dest="retention_days", type=int, default=None)
    cleanup_parser.add_argument("--max-outbox-attempts", dest="max_outbox_attempts", type=int, default=None)
    cleanup_parser.add_argument(
        "--acknowledged-final-retention-days",
        dest="acknowledged_final_retention_days",
        type=int,
        default=None,
        metavar="DAYS",
        help=(
            "Retain proven acknowledged finals for this age window "
            "(default: configured policy)."
        ),
    )
    cleanup_parser.add_argument(
        "--acknowledged-final-retention-count",
        dest="acknowledged_final_retention_count",
        type=int,
        default=None,
        metavar="COUNT",
        help=(
            "Retain this many newest proven acknowledged finals "
            "(default: configured policy)."
        ),
    )
    cleanup_parser.add_argument(
        "--snapshot-retention-days",
        dest="snapshot_retention_days",
        type=int,
        default=None,
    )
    cleanup_parser.add_argument(
        "--snapshot-retention-count",
        dest="snapshot_retention_count",
        type=int,
        default=None,
    )
    cleanup_parser.add_argument(
        "--snapshot-batch-size",
        dest="snapshot_batch_size",
        type=int,
        default=None,
    )

    compact_parser = actions.add_parser(
        "compact",
        help="Inspect or explicitly compact the current store while offline.",
    )
    add_common(compact_parser)
    compact_mode = compact_parser.add_mutually_exclusive_group(required=True)
    compact_mode.add_argument(
        "--dry-run",
        dest="compact_dry_run",
        action="store_true",
        default=False,
    )
    compact_mode.add_argument(
        "--execute",
        dest="compact_execute",
        action="store_true",
        default=False,
    )
    compact_parser.add_argument(
        "--acknowledge-offline",
        dest="acknowledge_offline",
        action="store_true",
        default=False,
    )
    compact_parser.add_argument("--backup-path", dest="backup_path", default=None)
    compact_parser.add_argument(
        "--snapshot-retention-days",
        dest="snapshot_retention_days",
        type=int,
        default=None,
    )
    compact_parser.add_argument(
        "--snapshot-retention-count",
        dest="snapshot_retention_count",
        type=int,
        default=None,
    )
    compact_parser.add_argument(
        "--batch-size",
        dest="snapshot_batch_size",
        type=int,
        default=None,
    )


def _add_connector_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    connector_parser = subparsers.add_parser(
        "connector",
        help="Exercise the neutral connector outbox boundary with JSON-only output.",
    )
    actions = connector_parser.add_subparsers(dest="connector_action", required=True)

    def add_common(action_parser: argparse.ArgumentParser) -> None:
        action_parser.add_argument("--db-path", dest="db_path", default=None)
        action_parser.add_argument("--name", required=True, help="Neutral connector queue name.")

    prepare_parser = actions.add_parser(
        "prepare",
        help="Stage one bounded neutral presentation-plan action from stdin.",
    )
    add_common(prepare_parser)
    prepare_parser.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        default=True,
        help="Read one schema-v1 JSON action from stdin and print JSON.",
    )

    poll_parser = actions.add_parser("poll", help="Lease due connector outbox items.")
    add_common(poll_parser)
    poll_parser.add_argument("--limit", type=int, default=1)
    poll_parser.add_argument("--lease-seconds", dest="lease_seconds", type=int, default=None)

    inspect_parser = actions.add_parser(
        "inspect",
        help="Inspect bounded unresolved connector items by neutral status.",
    )
    add_common(inspect_parser)
    inspect_parser.add_argument("--status", choices=("dead_letter",), required=True)
    inspect_parser.add_argument("--limit", type=_connector_inspect_limit, default=100)

    retry_parser = actions.add_parser(
        "retry",
        help="Explicitly requeue one unresolved final-ready item.",
    )
    add_common(retry_parser)
    retry_parser.add_argument("--final-identity", dest="final_identity", required=True)

    reclaim_parser = actions.add_parser("reclaim", help="Expire stale connector leases.")
    add_common(reclaim_parser)

    for action in ("ack", "fail", "defer", "renew", "release"):
        action_parser = actions.add_parser(action, help=f"Apply connector.{action} to a live ref.")
        add_common(action_parser)
        action_parser.add_argument("--ref", required=True)
        if action in {"ack", "fail", "defer"}:
            action_parser.add_argument("--response-json", dest="response_json", default=None)
        if action in {"fail", "defer"}:
            action_parser.add_argument("--reason", default="")
            action_parser.add_argument("--available-at", dest="available_at", default=None)
            action_parser.add_argument("--delay-seconds", dest="delay_seconds", type=int, default=None)
        if action == "renew":
            action_parser.add_argument("--lease-seconds", dest="lease_seconds", type=int, default=None)


def _load_worker_bindings(config: Config) -> list[WorkerBinding]:
    if config.db_path is None:
        return []
    return list_worker_bindings(
        config.db_path,
        config.host_id,
        backend=_HERDR_BACKEND,
    )


def _fetch_state_with_bindings(
    config: Config,
    stored_bindings: list[WorkerBinding],
) -> tuple[list[Any], list[Any], list[WorkerBinding]]:
    try:
        result = fetch_herdr_state(
            config,
            stored_bindings=stored_bindings,
            include_bindings=True,
        )
    except TypeError:
        spaces, workers = fetch_herdr_state(config)
        return spaces, workers, bindings_from_workers(config, workers)

    if len(result) == 3:
        spaces, workers, bindings = result
        return spaces, workers, bindings
    spaces, workers = result
    return spaces, workers, bindings_from_workers(config, workers)


def _legacy_backend_health(spaces: list[Any], workers: list[Any]) -> list[BackendHealth]:
    return [
        herdr_backend_health(
            "healthy_non_empty" if spaces or workers else "unknown",
            spaces=spaces,
            workers=workers,
        )
    ]


def _fetch_snapshot_observation_with_bindings(
    config: Config,
    stored_bindings: list[WorkerBinding],
) -> tuple[list[Any], list[Any], list[WorkerBinding], list[BackendHealth], bool]:
    complete_barrier = False
    if fetch_herdr_state is not _DEFAULT_FETCH_HERDR_STATE:
        spaces, workers, bindings = _fetch_state_with_bindings(config, stored_bindings)
        backend_health = _legacy_backend_health(spaces, workers)
    else:
        try:
            observation = fetch_herdr_snapshot_observation(
                config,
                stored_bindings=stored_bindings,
            )
        except TypeError:
            spaces, workers, bindings = _fetch_state_with_bindings(config, stored_bindings)
            backend_health = _legacy_backend_health(spaces, workers)
        else:
            spaces = list(getattr(observation, "spaces", []) or [])
            workers = list(getattr(observation, "workers", []) or [])
            bindings = list(getattr(observation, "bindings", []) or [])
            backend_health = list(getattr(observation, "backend_health", []) or [])
            complete_barrier = bool(backend_health)
            if not backend_health:
                backend_health = _legacy_backend_health(spaces, workers)

    health = _herdr_health_from_items(backend_health)
    if health.status == "healthy":
        return spaces, workers, bindings, backend_health, complete_barrier

    # Failed observations are not an authority for routing or continuity.
    # Never persist their bindings, and retain the last authenticated public
    # state when one has already been stored.
    bindings = []
    if config.db_path is None:
        return spaces, workers, bindings, backend_health, complete_barrier

    db_path = Path(config.db_path)
    latest = latest_snapshot(db_path, config.host_id)
    if latest is not None:
        latest_health = _herdr_health_from_items(list(latest.backend_health))
        if latest_health.outcome == "continuity_unavailable":
            health = latest_health

    previous = latest_healthy_backend_snapshot(
        db_path,
        config.host_id,
        backend=_HERDR_BACKEND,
    )
    if previous is not None:
        spaces = list(previous.spaces)
        workers = list(previous.workers)

    retained_health = herdr_backend_health(
        health.outcome,
        observed_at=health.observed_at,
        message=health.message,
        spaces=spaces,
        workers=workers,
    )
    backend_health = [
        retained_health if item.name == _HERDR_BACKEND else item
        for item in backend_health
    ]
    if not any(item.name == _HERDR_BACKEND for item in backend_health):
        backend_health.append(retained_health)
    return spaces, workers, bindings, backend_health, complete_barrier




def _herdr_health_from_items(items: list[BackendHealth]) -> BackendHealth:
    for item in items:
        if getattr(item, "name", "") == _HERDR_BACKEND:
            return item
    return herdr_backend_health("unknown")




def observe_public_snapshot(
    config: Config,
    *,
    store_snapshot: bool = False,
) -> Any:
    """Build the public snapshot through the existing one-shot observation path."""
    # Always seed observation with stored bindings: they are what keeps public
    # worker ids stable across snapshots. Skipping them re-letters duplicate
    # worker names (claude, claude-1, ...) from scratch on every observation.
    stored_bindings = _load_worker_bindings(config)
    spaces, workers, bindings, backend_health, complete_barrier = (
        _fetch_snapshot_observation_with_bindings(
            config,
            stored_bindings,
        )
    )
    snapshot = project_from_observations(
        config,
        spaces=spaces,
        workers=workers,
        backend_health=backend_health,
    )

    if store_snapshot:
        from .store.sqlite import SnapshotObservationContext, save_snapshot

        if config.db_path is None:
            raise RuntimeError("snapshot persistence requires a db path")
        health = _herdr_health_from_items(backend_health)
        authority = (
            "complete"
            if complete_barrier
            and health.status == "healthy"
            and health.outcome in {"healthy_non_empty", "empty_healthy"}
            else "none"
        )
        save_snapshot(
            config.db_path,
            snapshot,
            turn_model=config.turn_model,
            observation=SnapshotObservationContext(
                authority=authority,
                observed_at=health.observed_at,
            ),
            worker_bindings=bindings,
            binding_backend=_HERDR_BACKEND,
            binding_observation_authoritative=health.status == "healthy",
            binding_workers_present=bool(workers),
        )

    return snapshot


def _current_public_snapshot(config: Config) -> Any:
    return observe_public_snapshot(config)


def _try_daemon_attempt(
    config: Config,
    method: str,
    params: dict[str, Any] | None = None,
    *,
    preserve_content_text: bool = False,
) -> _DaemonAttempt:
    """Return a daemon result only when a daemon socket was explicitly selected."""
    if config.socket_path is None:
        return _DaemonAttempt(error_kind="unavailable", request_started=False)
    socket_path = config.socket_path

    try:
        from .daemon_api import (
            DaemonAPIClient,
            DaemonAPIError,
            DaemonProtocolError,
            DaemonUnavailable,
        )
    except Exception:
        return _DaemonAttempt(error_kind="protocol", request_started=False)
    try:
        timeout_seconds = _daemon_client_timeout_seconds(config, method)
        if config.socket_group is None:
            client = DaemonAPIClient(
                socket_path,
                timeout_seconds=timeout_seconds,
            )
        else:
            client = DaemonAPIClient(
                socket_path,
                timeout_seconds=timeout_seconds,
                socket_group=config.socket_group,
            )
        response = client.request(method, params or {})
    except DaemonUnavailable as exc:
        cause = exc.__cause__
        if exc.request_started is False:
            return _DaemonAttempt(error_kind="unavailable", request_started=False)
        if exc.timed_out or isinstance(cause, (TimeoutError, socket.timeout)):
            return _DaemonAttempt(error_kind="timeout", request_started=True)
        return _DaemonAttempt(
            error_kind="unavailable",
            request_started=exc.request_started,
        )
    except DaemonProtocolError as exc:
        return _DaemonAttempt(
            error_kind="protocol",
            request_started=exc.request_started,
        )
    except DaemonAPIError:
        return _DaemonAttempt(error_kind="protocol", request_started=None)
    if not isinstance(response, dict):
        return _DaemonAttempt(error_kind="protocol", request_started=True)
    if response.get("ok") is False:
        if isinstance(response.get("error"), dict):
            return _DaemonAttempt(
                error_kind="daemon_error",
                response_error=sanitize_public_mapping(response),
                request_started=True,
            )
        return _DaemonAttempt(error_kind="protocol", request_started=True)
    if response.get("ok") is not True:
        return _DaemonAttempt(error_kind="protocol", request_started=True)
    result = response.get("result")
    if isinstance(result, dict):
        if method == "command.submit":
            try:
                command_result = CommandEnvelope.from_dict(result).to_dict()
            except (TypeError, ValueError):
                return _DaemonAttempt(error_kind="protocol", request_started=True)
            return _DaemonAttempt(result=command_result, request_started=True)
        sanitized = sanitize_public_mapping(result)
        if preserve_content_text:
            _restore_cli_content_text(sanitized, result)
        if method == "turn.list":
            _restore_cli_turn_list_text(sanitized, result)
        if method == "turn.delta":
            _restore_cli_turn_delta_text(sanitized, result)
        if method.startswith("connector."):
            _restore_cli_plan_token(sanitized, result)
        return _DaemonAttempt(result=sanitized, request_started=True)
    return _DaemonAttempt(error_kind="protocol", request_started=True)


def _try_daemon_result(
    config: Config,
    method: str,
    params: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Return only a daemon result, preserving read-only fallback behavior."""
    return _try_daemon_attempt(config, method, params).result


def _persist_binding_observation(
    config: Config,
    bindings: list[WorkerBinding],
    *,
    observed_at: str,
    workers_present: bool,
    authoritative: bool = True,
) -> list[WorkerBinding]:
    bindings = separate_duplicate_worker_bindings(bindings)
    if config.db_path is None:
        return bindings
    if bindings:
        upsert_worker_bindings(config.db_path, bindings)
    if authoritative and (bindings or not workers_present):
        expire_stale_worker_bindings(
            config.db_path,
            config.host_id,
            backend=_HERDR_BACKEND,
            current_private_fingerprints=[binding.private_fingerprint for binding in bindings],
            now=observed_at,
        )
    return bindings


def cmd_snapshot(
    config: Config,
    *,
    json_output: bool = True,
    store_snapshot: bool = False,
) -> int:
    """Build and print a neutral snapshot."""
    if json_output:
        daemon_attempt = (
            _DaemonAttempt(error_kind="unavailable", request_started=False)
            if store_snapshot
            else _try_daemon_attempt(config, "snapshot.get")
        )
        if daemon_attempt.result is not None:
            payload = daemon_attempt.result
            code = 0
        elif daemon_attempt.response_error is not None:
            payload = daemon_attempt.response_error
            code = 1
        elif daemon_attempt.request_started is False:
            payload = observe_public_snapshot(config, store_snapshot=store_snapshot).to_dict()
            code = 0
        elif daemon_attempt.error_kind == "timeout":
            payload = {
                "schema_version": 2,
                "ok": False,
                "status": "daemon_timeout",
                "error": {
                    "code": "daemon_timeout",
                    "message": "Tendwire daemon request timed out",
                },
            }
            code = 1
        else:
            payload = {
                "schema_version": 2,
                "ok": False,
                "status": "daemon_protocol_error",
                "error": {
                    "code": "daemon_protocol_error",
                    "message": "Tendwire daemon returned an invalid response",
                },
            }
            code = 1
        print(public_json_dumps(payload, indent=2))
    else:
        # Non-JSON output is out of scope; reject cleanly.
        print("error: only --json output is supported", file=sys.stderr)
        return 2

    return code


def _restore_cli_turn_list_text(
    sanitized: dict[str, Any],
    original: dict[str, Any],
) -> None:
    if original.get("schema_version") not in {1, 2}:
        return
    original_turns = original.get("turns")
    sanitized_turns = sanitized.get("turns")
    if not isinstance(original_turns, list) or not isinstance(sanitized_turns, list):
        return
    by_id = {
        item.get("id"): item
        for item in sanitized_turns
        if isinstance(item, dict) and isinstance(item.get("id"), str)
    }
    for original_turn in original_turns:
        if not isinstance(original_turn, dict):
            continue
        target = by_id.get(original_turn.get("id"))
        if not isinstance(target, dict):
            continue
        descriptors = (original_turn.get("content") or {}).get("fields", {})
        for field in ("user_text", "assistant_final_text"):
            text = original_turn.get(field)
            descriptor = descriptors.get(field) if isinstance(descriptors, dict) else None
            trusted_inline = original.get("schema_version") == 1 or (
                isinstance(descriptor, dict)
                and descriptor.get("availability") == "complete"
                and descriptor.get("inline") is True
            )
            if trusted_inline and isinstance(text, str):
                target[field] = text
        for preview_key in ("user_preview", "assistant_final_preview"):
            preview = original_turn.get(preview_key)
            if isinstance(preview, str):
                target[preview_key] = preview


def _turn_list_payload_json(payload: dict[str, Any], *, indent: int | None = None) -> str:
    sanitized = sanitize_public_mapping(payload)
    _restore_cli_turn_list_text(sanitized, payload)
    return json.dumps(
        sanitized,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        indent=indent,
    )


def _restore_cli_turn_delta_text(
    sanitized: dict[str, Any],
    original: Mapping[str, Any],
) -> None:
    """Restore trusted schema-v2 inline fields inside delta upserts."""
    original_changes = original.get("changes")
    target_changes = sanitized.get("changes")
    if not isinstance(original_changes, list) or not isinstance(target_changes, list):
        return
    for original_change, target_change in zip(
        original_changes, target_changes, strict=False
    ):
        if not isinstance(original_change, Mapping) or not isinstance(target_change, dict):
            continue
        original_turn = original_change.get("turn")
        target_turn = target_change.get("turn")
        if isinstance(original_turn, Mapping) and isinstance(target_turn, dict):
            _restore_cli_turn_list_text(
                {"turns": [target_turn]},
                {"schema_version": 2, "turns": [original_turn]},
            )


def _turn_delta_payload_json(payload: dict[str, Any], *, indent: int | None = None) -> str:
    sanitized = sanitize_public_mapping(payload)
    _restore_cli_turn_delta_text(sanitized, payload)
    return json.dumps(
        sanitized,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        indent=indent,
    )


def _restore_cli_content_text(
    sanitized: dict[str, Any],
    original: dict[str, Any],
) -> None:
    text = original.get("text")
    if (
        isinstance(text, str)
        and original.get("schema_version") == 1
        and original.get("field") in {"user_text", "assistant_final_text"}
        and original.get("availability") == "complete"
    ):
        sanitized["text"] = text


def _restore_cli_plan_token(
    sanitized: dict[str, Any],
    original: dict[str, Any],
) -> None:
    for key in (
        "plan_token",
        "failed_plan_token",
        "recovered_plan_token",
        "replaces_plan_token",
        "recovers_plan_token",
    ):
        token = original.get(key)
        if isinstance(token, str) and re.fullmatch(
            r"twplan1\.[A-Za-z0-9_-]+", token
        ):
            sanitized[key] = token
    final_identity = original.get("final_identity")
    if isinstance(final_identity, str) and re.fullmatch(
        r"twfinal1\.[A-Za-z0-9_-]+", final_identity
    ):
        sanitized["final_identity"] = final_identity
    delivery_key = original.get("key")
    if isinstance(delivery_key, str) and re.fullmatch(
        r"turn-final:revision:twfinal1\.[A-Za-z0-9_-]+", delivery_key
    ):
        sanitized["key"] = delivery_key
    for nested_key in ("turn", "final", "payload"):
        nested_original = original.get(nested_key)
        nested_sanitized = sanitized.get(nested_key)
        if isinstance(nested_original, dict) and isinstance(nested_sanitized, dict):
            _restore_cli_plan_token(nested_sanitized, nested_original)
    original_items = original.get("items")
    sanitized_items = sanitized.get("items")
    if isinstance(original_items, list) and isinstance(sanitized_items, list):
        for sanitized_item, original_item in zip(
            sanitized_items,
            original_items,
            strict=False,
        ):
            if isinstance(sanitized_item, dict) and isinstance(original_item, dict):
                _restore_cli_plan_token(sanitized_item, original_item)


def _connector_payload_json(payload: dict[str, Any], *, indent: int | None = None) -> str:
    sanitized = sanitize_public_mapping(payload)
    _restore_cli_plan_token(sanitized, payload)
    return json.dumps(
        sanitized,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        indent=indent,
    )


def _content_payload_json(payload: dict[str, Any], *, indent: int | None = None) -> str:
    sanitized = sanitize_public_mapping(payload)
    _restore_cli_content_text(sanitized, payload)
    return json.dumps(
        sanitized,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        indent=indent,
    )


def cmd_turns(
    config: Config,
    *,
    json_output: bool = True,
    schema_version: int = 1,
    limit: int = TURN_LIST_DEFAULT_LIMIT,
    cursor: str | None = None,
    since: str | None = None,
) -> int:
    """Print exactly one insertion-stable public turn-list page."""
    if not json_output:
        print("error: only --json output is supported", file=sys.stderr)
        return 2
    params: dict[str, Any] = {
        "schema_version": schema_version,
        "limit": limit,
        "cursor": cursor,
        "since": since,
    }
    daemon_attempt = _try_daemon_attempt(config, "turn.list", params)
    if daemon_attempt.result is not None:
        payload = daemon_attempt.result
    elif daemon_attempt.response_error is not None:
        payload = daemon_attempt.response_error
    elif (
        daemon_attempt.error_kind in {"unavailable", "timeout"}
        and daemon_attempt.request_started is False
    ):
        if config.db_path is None:
            payload = {
                "schema_version": schema_version,
                "host_id": config.host_id,
                "ok": False,
                "status": "store_unavailable",
            }
        else:
            if cursor is None and since is None:
                refresh_structured_turn_content(
                    config,
                    adapter_timeout_seconds=config.herdr_timeout_seconds,
                    max_workers=config.turn_refresh_workers,
                    total_timeout_seconds=config.herdr_timeout_seconds + 1.0,
                )
            payload = turns_payload_from_store(
                config.db_path,
                config.host_id,
                schema_version=schema_version,
                limit=limit,
                cursor=cursor,
                since=since,
                turn_refresh_interval_seconds=config.turn_refresh_interval_seconds,
                turn_model=config.turn_model,
            )
    elif daemon_attempt.error_kind == "timeout":
        payload = {
            "schema_version": 1,
            "ok": False,
            "status": "daemon_timeout",
            "error": {
                "code": "daemon_timeout",
                "message": "Tendwire daemon request timed out",
            },
        }
    else:
        payload = {
            "schema_version": 1,
            "ok": False,
            "status": "daemon_protocol_error",
            "error": {
                "code": "daemon_protocol_error",
                "message": "Tendwire daemon returned an invalid response",
            },
        }
    print(_turn_list_payload_json(payload, indent=2))
    return 0 if payload.get("ok") is not False else 1


def cmd_turn_content_get(config: Config, args: argparse.Namespace) -> int:
    """Fetch one bounded canonical content page with daemon/store parity."""
    params: dict[str, Any] = {
        "schema_version": 1,
        "turn_id": args.turn_id,
        "content_revision": args.content_revision,
        "field": args.field,
    }
    if args.cursor is not None:
        params["cursor"] = args.cursor
    daemon_attempt = _try_daemon_attempt(
        config,
        "turn.content.get",
        params,
        preserve_content_text=True,
    )
    if daemon_attempt.result is not None:
        payload = daemon_attempt.result
    elif daemon_attempt.response_error is not None:
        payload = daemon_attempt.response_error
    elif daemon_attempt.error_kind not in {"unavailable", "timeout"}:
        payload = {
            "schema_version": 1,
            "ok": False,
            "status": "daemon_protocol_error",
            "error": {
                "code": "daemon_protocol_error",
                "message": "daemon returned an invalid response",
            },
        }
    elif config.db_path is None:
        payload = {
            "schema_version": 1,
            "ok": False,
            "status": "store_unavailable",
            "error": {
                "code": "store_unavailable",
                "message": "command requires --db-path or a reachable daemon",
            },
        }
    else:
        from .store.sqlite import get_turn_content, init_store

        init_store(
            config.db_path,
            connector_ack_ttl_seconds=config.connector_ack_ttl_seconds,
        )
        payload = get_turn_content(
            config.db_path,
            config.host_id,
            turn_id=args.turn_id,
            content_revision=args.content_revision,
            field=args.field,
            cursor=args.cursor,
            schema_version=1,
            turn_model=config.turn_model,
        )
    print(_content_payload_json(payload, indent=2))
    return 0 if payload.get("ok") is not False and isinstance(payload.get("text"), str) else 1


def cmd_attention(
    config: Config,
    *,
    json_output: bool = True,
    store_snapshot: bool = False,
) -> int:
    """Print neutral public attention items."""
    if not json_output:
        print("error: only --json output is supported", file=sys.stderr)
        return 2
    if not store_snapshot:
        daemon_result = _try_daemon_result(config, "attention.list")
        if daemon_result is not None:
            print(public_json_dumps(daemon_result, indent=2))
            return 0
    if store_snapshot:
        observe_public_snapshot(config, store_snapshot=True)
    if config.db_path is not None:
        payload = attention_payload_from_store(config.db_path, config.host_id)
        if payload is not None:
            print(public_json_dumps(payload, indent=2))
            return 0
    snapshot = _current_public_snapshot(config)
    print(public_json_dumps(attention_payload_from_snapshot(snapshot), indent=2))
    return 0


def cmd_pending(
    config: Config,
    *,
    json_output: bool = True,
) -> int:
    """Print pending interactions from one daemon attempt or durable fallback."""
    if not json_output:
        print("error: only --json output is supported", file=sys.stderr)
        return 2

    daemon_attempt = _try_daemon_attempt(config, "pending.list")
    if daemon_attempt.result is not None:
        payload = daemon_attempt.result
    elif daemon_attempt.response_error is not None:
        payload = daemon_attempt.response_error
    elif (
        daemon_attempt.error_kind == "unavailable"
        and daemon_attempt.request_started is False
    ):
        payload = pending_payload_from_store(config.db_path, config.host_id)
    elif daemon_attempt.error_kind == "timeout":
        payload = {
            "schema_version": 1,
            "ok": False,
            "status": "daemon_timeout",
            "error": {
                "code": "daemon_timeout",
                "message": "Tendwire daemon request timed out",
            },
        }
    else:
        payload = {
            "schema_version": 1,
            "ok": False,
            "status": "daemon_protocol_error",
            "error": {
                "code": "daemon_protocol_error",
                "message": "Tendwire daemon returned an invalid response",
            },
        }
    print(public_json_dumps(payload, indent=2))
    return 0 if payload.get("ok") is not False else 1


def cmd_turn_delta(config: Config, args: argparse.Namespace) -> int:
    """Read one delta page via daemon, with read-only store fallback."""
    params = {
        "limit": args.limit,
        "watermark": args.watermark,
        "cursor": args.cursor,
    }
    daemon_attempt = _try_daemon_attempt(config, "turn.delta", params)
    if daemon_attempt.result is not None:
        payload = daemon_attempt.result
    elif daemon_attempt.response_error is not None:
        payload = daemon_attempt.response_error
    elif (
        daemon_attempt.error_kind in {"unavailable", "timeout"}
        and daemon_attempt.request_started is False
        and config.db_path is not None
    ):
        payload = turn_delta_payload_from_store(
            config.db_path,
            config.host_id,
            watermark=args.watermark,
            cursor=args.cursor,
            limit=args.limit,
            turn_model=config.turn_model,
        )
    elif daemon_attempt.error_kind == "timeout":
        payload = {
            "schema_version": 1,
            "projection_schema_version": 2,
            "ok": False,
            "status": "daemon_timeout",
        }
    else:
        payload = {
            "schema_version": 1,
            "projection_schema_version": 2,
            "ok": False,
            "status": "store_unavailable" if config.db_path is None else "daemon_protocol_error",
        }
    print(_turn_delta_payload_json(payload, indent=2))
    return 0 if payload.get("ok") is not False else 1


def cmd_doctor(
    config: Config,
    *,
    json_output: bool = True,
) -> int:
    """Run read-only backend diagnostics and print a JSON result."""
    if not json_output:
        print("error: only --json output is supported", file=sys.stderr)
        return 2
    payload = diagnose_herdr(config)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if payload.get("status") == "ok" else 1


def command_envelope_from_payload(config: Config, payload: str) -> CommandEnvelope:
    """Execute a JSON command request through the authoritative command path."""
    request, parse_error = parse_command_request(payload)
    if parse_error is not None:
        if request is not None:
            return CommandEnvelope.from_error(request, parse_error)
        return CommandEnvelope.from_error(
            None,
            parse_error or error_value("invalid_request", "unknown parse error"),
        )

    validation_error = validate_request(request)
    if validation_error is not None:
        return CommandEnvelope.from_error(request, validation_error)

    if request.action in {"send_instruction", "answer_pending", "answer_decision"}:
        from .command_submission import submit_command

        return submit_command(config, payload)

    if request.action == "noop":
        return execute_command(
            request,
            CommandContext(host_id=config.host_id, workers=[]),
        )

    stored_bindings = _load_worker_bindings(config)
    spaces, workers, current_bindings, backend_health, _complete_barrier = (
        _fetch_snapshot_observation_with_bindings(
            config,
            stored_bindings,
        )
    )
    workers = rehydrate_workers_from_bindings(
        workers,
        current_bindings,
        stored_bindings,
    )
    snapshot = project_from_observations(
        config,
        spaces=spaces,
        workers=workers,
        backend_health=backend_health,
    )
    return execute_command(
        request,
        CommandContext(
            host_id=config.host_id,
            workers=workers,
            snapshot=snapshot,
        ),
    )


def _command_exit_code(envelope: CommandEnvelope) -> int:
    return 0 if envelope.ok else 1


def _requires_daemon_for_mutating_command(config: Config, payload: str) -> Any | None:
    """Return a live mutating request that must not fall back from the daemon."""
    if config.socket_path is None and config.herdr_backend != "socket":
        return None
    request, parse_error = parse_command_request(payload)
    if parse_error is not None or request is None:
        return None
    validation_error = validate_request(request)
    if validation_error is not None:
        return None
    if (
        request.action in {"send_instruction", "answer_pending", "answer_decision"}
        and not request.dry_run
    ):
        return request
    return None


def _daemon_backend_failure_envelope(
    request: Any,
    attempt: _DaemonAttempt,
) -> CommandEnvelope:
    if attempt.request_started is not False:
        raise ValueError("ambiguous daemon attempt has no authoritative command envelope")
    return CommandEnvelope.from_error(
        request,
        error_value(
            STATUS_BACKEND_UNAVAILABLE,
            "Tendwire daemon backend is unavailable",
        ),
    )


def _strict_daemon_command_envelope(
    request: Any,
    value: dict[str, Any],
) -> CommandEnvelope | None:
    try:
        envelope = CommandEnvelope.from_dict(value)
    except (TypeError, ValueError):
        return None
    if (
        envelope.action != request.action
        or envelope.request_id != request.request_id
        or envelope.dry_run != request.dry_run
    ):
        return None
    return envelope


def _replay_daemon_command_receipt(
    config: Config,
    payload: str,
) -> CommandEnvelope | None:
    from .command_submission import replay_command_receipt

    return replay_command_receipt(config, payload)


def cmd_command(
    config: Config,
    *,
    json_output: bool = True,
) -> int:
    """Read a JSON command request from stdin and print a JSON result envelope."""
    payload = sys.stdin.read()
    if json_output:
        try:
            request_payload = json.loads(payload)
        except (json.JSONDecodeError, TypeError, ValueError):
            request_payload = None
        parsed_request, parse_error = parse_command_request(payload)
        validation_error = (
            None
            if parse_error is not None or parsed_request is None
            else validate_request(parsed_request)
        )
        pure_mutation_dry_run = (
            parse_error is None
            and validation_error is None
            and parsed_request is not None
            and parsed_request.action
            in {"send_instruction", "answer_pending", "answer_decision"}
            and parsed_request.dry_run
        )
        daemon_required_request = _requires_daemon_for_mutating_command(config, payload)
        daemon_eligible = (
            isinstance(request_payload, dict)
            and parse_error is None
            and validation_error is None
            and not pure_mutation_dry_run
        )
        if daemon_eligible:
            daemon_attempt = _try_daemon_attempt(config, "command.submit", request_payload)
            daemon_result = daemon_attempt.result
            if daemon_result is not None and parsed_request is not None:
                daemon_envelope = _strict_daemon_command_envelope(
                    parsed_request,
                    daemon_result,
                )
                if daemon_envelope is not None:
                    print(daemon_envelope.to_json(indent=2))
                    return _command_exit_code(daemon_envelope)
                daemon_attempt = _DaemonAttempt(
                    error_kind="protocol",
                    request_started=True,
                )
            if daemon_required_request is not None:
                if daemon_attempt.request_started is False:
                    envelope = _daemon_backend_failure_envelope(
                        daemon_required_request,
                        daemon_attempt,
                    )
                    print(envelope.to_json(indent=2))
                    return _command_exit_code(envelope)
                envelope = _replay_daemon_command_receipt(config, payload)
                if envelope is not None:
                    print(envelope.to_json(indent=2))
                    return _command_exit_code(envelope)
                print(
                    "error: Tendwire daemon command result is unresolved",
                    file=sys.stderr,
                )
                return 2
    envelope = command_envelope_from_payload(config, payload)
    print(envelope.to_json(indent=2))
    return _command_exit_code(envelope)


def _connector_params_from_args(args: argparse.Namespace) -> dict[str, Any]:
    params: dict[str, Any] = {"name": args.name}
    if args.connector_action == "prepare":
        try:
            parsed = json.loads(sys.stdin.read())
        except json.JSONDecodeError as exc:
            raise ValueError("connector prepare requires valid JSON on stdin") from exc
        if not isinstance(parsed, dict):
            raise ValueError("connector prepare request must be a JSON object")
        params.update(parsed)
        params["name"] = args.name
        return params
    if args.connector_action == "inspect":
        if args.name != "turn-final":
            raise ValueError("inspect requires --name turn-final")
        return {
            "schema_version": 1,
            "name": args.name,
            "status": args.status,
            "limit": args.limit,
        }
    if args.connector_action == "retry":
        if args.name != "turn-final":
            raise ValueError("retry requires --name turn-final")
        final_identity = str(args.final_identity).strip()
        if not final_identity:
            raise ValueError("retry requires a final identity")
        return {
            "schema_version": 1,
            "name": args.name,
            "final_identity": final_identity,
        }
    if args.connector_action == "poll":
        params["limit"] = args.limit
        if args.lease_seconds is not None:
            params["lease_seconds"] = args.lease_seconds
    if args.connector_action in {"ack", "fail", "defer", "renew", "release"}:
        params["ref"] = args.ref
        if getattr(args, "response_json", None):
            try:
                parsed = json.loads(args.response_json)
            except json.JSONDecodeError:
                parsed = {}
            if isinstance(parsed, dict):
                params["response"] = parsed
        if args.connector_action in {"fail", "defer"}:
            params["reason"] = args.reason
            if args.available_at:
                params["available_at"] = args.available_at
            if args.delay_seconds is not None:
                params["delay_seconds"] = args.delay_seconds
        if args.connector_action == "renew" and args.lease_seconds is not None:
            params["lease_seconds"] = args.lease_seconds
    return params


def cmd_connector(config: Config, args: argparse.Namespace) -> int:
    """Run a neutral connector boundary action and print one JSON object."""
    method = f"connector.{args.connector_action}"
    try:
        params = _connector_params_from_args(args)
    except ValueError as exc:
        payload = {
            "schema_version": 1,
            "ok": False,
            "status": "invalid_request",
            "error": {
                "code": "invalid_request",
                "message": str(exc),
            },
        }
        print(public_json_dumps(payload, indent=2))
        return 2
    daemon_attempt = _try_daemon_attempt(config, method, params)
    if daemon_attempt.result is not None:
        print(_connector_payload_json(daemon_attempt.result, indent=2))
        return 0 if daemon_attempt.result.get("ok") is not False else 1
    if daemon_attempt.response_error is not None:
        print(_connector_payload_json(daemon_attempt.response_error, indent=2))
        return 1
    if daemon_attempt.request_started is not False:
        status = (
            "daemon_timeout"
            if daemon_attempt.error_kind == "timeout"
            else "daemon_protocol_error"
        )
        payload = {
            "schema_version": 1,
            "ok": False,
            "status": status,
            "host_id": config.host_id,
            "name": params.get("name", ""),
            "error": {
                "code": status,
                "message": (
                    "Tendwire daemon request timed out"
                    if status == "daemon_timeout"
                    else "Tendwire daemon returned an invalid response"
                ),
            },
        }
        print(_connector_payload_json(payload, indent=2))
        return 1
    if config.db_path is None:
        payload = {
            "schema_version": 1,
            "ok": False,
            "status": "store_unavailable",
            "host_id": config.host_id,
            "name": params.get("name", ""),
            "error": {
                "code": "store_unavailable",
                "message": "command requires --db-path or a reachable daemon",
            },
        }
        print(public_json_dumps(payload, indent=2))
        return 1
    from .connectors import ConnectorOutboxAPI
    from .store.sqlite import init_store

    init_store(
        config.db_path,
        connector_ack_ttl_seconds=config.connector_ack_ttl_seconds,
    )
    payload = ConnectorOutboxAPI(
        config.db_path,
        config.host_id,
        default_lease_seconds=config.connector_claim_ttl_seconds,
        max_lease_seconds=config.connector_max_claim_ttl_seconds,
        ack_ttl_seconds=config.connector_ack_ttl_seconds,
        max_attempts=config.max_outbox_attempts,
        turn_model=config.turn_model,
    ).dispatch(method, params)
    print(_connector_payload_json(payload, indent=2))
    return 0 if payload.get("ok") is not False else 1


def cmd_store(config: Config, args: argparse.Namespace) -> int:
    """Run a bounded store operation and print one JSON object."""
    if config.db_path is None:
        payload = {
            "schema_version": 1,
            "ok": False,
            "status": "store_unavailable",
            "host_id": config.host_id,
            "error": {
                "code": "store_unavailable",
                "message": "command requires --db-path or a configured store",
            },
        }
        print(public_json_dumps(payload, indent=2))
        return 1
    if args.store_action == "status":
        payload = store_status(
            config.db_path,
            config.host_id,
            acknowledged_final_retention_days=config.acknowledged_final_retention_days,
            acknowledged_final_retention_count=config.acknowledged_final_retention_count,
            snapshot_retention_days=config.snapshot_retention_days,
            snapshot_retention_count=config.snapshot_retention_count,
            maintenance_batch_size=config.snapshot_maintenance_batch_size,
            maintenance_cadence_seconds=config.store_maintenance_cadence_seconds,
            command_retry_horizon_seconds=config.command_retry_horizon_seconds,
            command_receipt_retention_seconds=config.command_receipt_retention_seconds,
            command_receipt_retention_count=config.command_receipt_retention_count,
        )
    elif args.store_action == "events-tail":
        payload = tail_event_metadata(config.db_path, config.host_id, limit=args.limit)
    elif args.store_action == "cleanup":
        payload = run_store_maintenance(
            config.db_path,
            config.host_id,
            retention_days=args.retention_days
            if args.retention_days is not None
            else config.event_retention_days,
            max_outbox_attempts=args.max_outbox_attempts
            if args.max_outbox_attempts is not None
            else config.max_outbox_attempts,
            acknowledged_final_retention_days=(
                args.acknowledged_final_retention_days
                if args.acknowledged_final_retention_days is not None
                else config.acknowledged_final_retention_days
            ),
            acknowledged_final_retention_count=(
                args.acknowledged_final_retention_count
                if args.acknowledged_final_retention_count is not None
                else config.acknowledged_final_retention_count
            ),
            command_retry_horizon_seconds=config.command_retry_horizon_seconds,
            command_receipt_retention_seconds=config.command_receipt_retention_seconds,
            command_receipt_retention_count=config.command_receipt_retention_count,
            dry_run=args.dry_run,
            snapshot_retention_days=args.snapshot_retention_days
            if args.snapshot_retention_days is not None
            else config.snapshot_retention_days,
            snapshot_retention_count=args.snapshot_retention_count
            if args.snapshot_retention_count is not None
            else config.snapshot_retention_count,
            snapshot_batch_size=args.snapshot_batch_size
            if args.snapshot_batch_size is not None
            else config.snapshot_maintenance_batch_size,
            turn_change_retention_days=config.turn_change_retention_days,
            turn_change_retention_count=config.turn_change_retention_count,
            turn_change_batch_size=config.turn_change_compaction_batch_size,
        )
    elif args.store_action == "compact":
        try:
            options = CompactionOptions(
                dry_run=bool(args.compact_dry_run),
                acknowledge_offline=bool(args.acknowledge_offline),
                backup_path=Path(args.backup_path)
                if args.backup_path is not None
                else None,
                snapshot_retention_days=args.snapshot_retention_days
                if args.snapshot_retention_days is not None
                else config.snapshot_retention_days,
                snapshot_retention_count=args.snapshot_retention_count
                if args.snapshot_retention_count is not None
                else config.snapshot_retention_count,
                batch_size=args.snapshot_batch_size
                if args.snapshot_batch_size is not None
                else config.snapshot_maintenance_batch_size,
            )
        except (TypeError, ValueError):
            options = CompactionOptions(dry_run=True)
            payload = {
                "schema_version": 1,
                "ok": False,
                "status": "invalid_request",
                "command": "store.compact",
                "scope": "database",
                "dry_run": bool(args.compact_dry_run),
            }
        else:
            payload = compact_store(config.db_path, options=options)
    else:
        payload = {
            "schema_version": 1,
            "ok": False,
            "status": "invalid_params",
            "host_id": config.host_id,
        }
    if args.store_action == "compact":
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2))
    else:
        print(public_json_dumps(payload, indent=2))
    return 0 if payload.get("ok") is not False else 1


def cmd_daemon(config: Config) -> int:
    """Run the long-lived local daemon."""
    from .daemon import run_daemon
    from .daemon_api import DaemonUnavailable
    from ._version import __version__

    try:
        return run_daemon(config)
    except DaemonUnavailable as exc:
        print(
            f"tendwire daemon {__version__}: startup failed: {exc}",
            file=sys.stderr,
        )
        return 1


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        return 0

    config = load_config(
        host_id=args.host_id,
        herdr_bin=args.herdr_bin,
        db_path=getattr(args, "db_path", None),
        socket_path=getattr(args, "socket_path", None),
        socket_group=getattr(args, "socket_group", None),
        herdr_timeout_seconds=args.herdr_timeout_seconds,
    )
    if args.command not in {"daemon", "doctor"} and not (
        args.command == "store" and args.store_action == "compact"
    ):
        repair_config_state(
            config.data_dir,
            config.db_path,
            private_files=(
                config.installation_key_path,
                config.installation_key_marker_path,
                config.installation_key_sentinel_path,
            ),
        )

    if args.command == "snapshot":
        return cmd_snapshot(
            config,
            json_output=args.json_output,
            store_snapshot=args.store_snapshot,
        )

    if args.command == "attention":
        return cmd_attention(
            config,
            json_output=args.json_output,
            store_snapshot=args.store_snapshot,
        )

    if args.command == "turns":
        return cmd_turns(
            config,
            json_output=args.json_output,
            schema_version=args.schema_version,
            limit=args.limit,
            cursor=args.cursor,
            since=args.since,
        )

    if args.command == "turn":
        if args.turn_action == "delta":
            return cmd_turn_delta(config, args)
        return cmd_turn_content_get(config, args)

    if args.command == "pending":
        return cmd_pending(config, json_output=args.json_output)

    if args.command == "command":
        return cmd_command(config, json_output=args.json_output)

    if args.command == "connector":
        return cmd_connector(config, args)

    if args.command == "store":
        return cmd_store(config, args)

    if args.command == "daemon":
        return cmd_daemon(config)

    if args.command == "doctor":
        return cmd_doctor(config, json_output=args.json_output)

    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
