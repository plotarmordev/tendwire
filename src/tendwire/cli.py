"""Tendwire command-line interface.

Console script entry point: tendwire = tendwire.cli:main
Module entry point: python -m tendwire.cli snapshot --json
"""

from __future__ import annotations

import argparse
import json
import socket
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .config import Config, load_config
from .core.commands import (
    STATUS_BACKEND_UNAVAILABLE,
    CommandEnvelope,
    error_value,
    parse_command_request,
    validate_request,
)
from .core.models import (
    public_json_dumps,
)
from .core.turns import (
    TURN_DELTA_DEFAULT_LIMIT,
    TURN_DELTA_MAX_LIMIT,
    TURN_LIST_DEFAULT_LIMIT,
    TURN_LIST_MAX_LIMIT,
)
from .local_state import repair_config_state


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
        herdr = float(config.herdr_timeout_seconds)
        acp = float(config.acp_request_timeout_seconds)
        shutdown = float(config.acp_shutdown_timeout_seconds)
        return max(
            _DAEMON_COMMAND_CLIENT_TIMEOUT_FLOOR_SECONDS,
            6 * herdr + 3 * acp + 2 * shutdown,
            4 * herdr + 2 * acp + 3 * shutdown,
        ) + _DAEMON_COMMAND_CLIENT_TIMEOUT_GRACE_SECONDS
    if method in {"turn.list", "turn.content.get", "turn.delta"}:
        return _DAEMON_CONTENT_CLIENT_TIMEOUT_SECONDS
    if method.startswith("connector."):
        return _DAEMON_CONNECTOR_CLIENT_TIMEOUT_SECONDS
    return _DAEMON_FAST_CLIENT_TIMEOUT_SECONDS


def _bounded_limit(value: str, maximum: int) -> int:
    try:
        limit = int(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("limit must be an integer") from exc
    if not 1 <= limit <= maximum:
        raise argparse.ArgumentTypeError(
            f"limit must be between 1 and {maximum}"
        )
    return limit


def _turn_list_limit(value: str) -> int:
    return _bounded_limit(value, TURN_LIST_MAX_LIMIT)


def _turn_delta_limit(value: str) -> int:
    return _bounded_limit(value, TURN_DELTA_MAX_LIMIT)


def _connector_inspect_limit(value: str) -> int:
    return _bounded_limit(value, 100)


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
        help="Seconds to wait for each Herdr socket request (default: 5.0).",
    )
    parser.add_argument(
        "--socket-path",
        dest="socket_path",
        default=None,
        help="Unix socket path for daemon requests (default: data-dir/tendwire.sock).",
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

    pending_parser = subparsers.add_parser(
        "pending",
        help="Print neutral public pending interactions from the daemon.",
    )
    pending_parser.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        default=True,
        help="Print pending interactions as JSON (default).",
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

    _add_connector_parser(subparsers)

    return parser


def _add_connector_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    connector_parser = subparsers.add_parser(
        "connector",
        help="Exercise the neutral connector outbox boundary with JSON-only output.",
    )
    actions = connector_parser.add_subparsers(dest="connector_action", required=True)

    def add_common(action_parser: argparse.ArgumentParser) -> None:
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


def _try_daemon_attempt(
    config: Config,
    method: str,
    params: dict[str, Any] | None = None,
) -> _DaemonAttempt:
    """Return one result from the daemon's authoritative socket API."""
    from .daemon import default_socket_path

    socket_path = default_socket_path(config)

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
                response_error=dict(response),
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
        return _DaemonAttempt(result=dict(result), request_started=True)
    return _DaemonAttempt(error_kind="protocol", request_started=True)


def cmd_snapshot(
    config: Config,
    *,
    json_output: bool = True,
) -> int:
    """Read the daemon's current neutral snapshot."""
    return _cmd_simple_read(
        config, "snapshot.get", json_output=json_output, schema_version=2
    )


def _daemon_payload_json(payload: dict[str, Any], *, indent: int | None = None) -> str:
    """Serialize a response already validated by the local daemon boundary."""
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        indent=indent,
    )


def _daemon_read_payload(
    attempt: _DaemonAttempt,
    *,
    schema_version: int = 1,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if attempt.result is not None:
        return attempt.result
    if attempt.response_error is not None:
        return attempt.response_error
    status = (
        "daemon_timeout"
        if attempt.error_kind == "timeout"
        else "daemon_unavailable"
        if attempt.request_started is False
        else "daemon_protocol_error"
    )
    payload: dict[str, Any] = {
        "schema_version": schema_version,
        "ok": False,
        "status": status,
        "error": {
            "code": status,
            "message": {
                "daemon_timeout": "Tendwire daemon request timed out",
                "daemon_unavailable": "Tendwire daemon is unavailable",
                "daemon_protocol_error": "Tendwire daemon returned an invalid response",
            }[status],
        },
    }
    if extra:
        payload.update(extra)
    return payload


def _cmd_simple_read(
    config: Config,
    method: str,
    *,
    json_output: bool,
    schema_version: int = 1,
    require_ok_status: bool = False,
    trusted_daemon_json: bool = False,
) -> int:
    if not json_output:
        print("error: only --json output is supported", file=sys.stderr)
        return 2
    payload = _daemon_read_payload(
        _try_daemon_attempt(config, method), schema_version=schema_version
    )
    serialized = (
        _daemon_payload_json(payload, indent=2)
        if trusted_daemon_json
        else public_json_dumps(payload, indent=2)
    )
    print(serialized)
    success = payload.get("ok") is not False
    if require_ok_status:
        success = success and payload.get("status") == "ok"
    return 0 if success else 1


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
    payload = _daemon_read_payload(
        _try_daemon_attempt(config, "turn.list", params),
        schema_version=schema_version,
        extra={"host_id": config.host_id},
    )
    print(_daemon_payload_json(payload, indent=2))
    return 0 if payload.get("ok") is not False else 1


def cmd_turn_content_get(config: Config, args: argparse.Namespace) -> int:
    """Fetch one bounded canonical content page from the daemon."""
    params: dict[str, Any] = {
        "schema_version": 1,
        "turn_id": args.turn_id,
        "content_revision": args.content_revision,
        "field": args.field,
    }
    if args.cursor is not None:
        params["cursor"] = args.cursor
    payload = _daemon_read_payload(
        _try_daemon_attempt(config, "turn.content.get", params)
    )
    print(_daemon_payload_json(payload, indent=2))
    return 0 if payload.get("ok") is not False and isinstance(payload.get("text"), str) else 1


def cmd_turn_delta(config: Config, args: argparse.Namespace) -> int:
    """Read one delta page from the daemon."""
    params = {
        "limit": args.limit,
        "watermark": args.watermark,
        "cursor": args.cursor,
    }
    payload = _daemon_read_payload(
        _try_daemon_attempt(config, "turn.delta", params),
        extra={"projection_schema_version": 2},
    )
    print(_daemon_payload_json(payload, indent=2))
    return 0 if payload.get("ok") is not False else 1


def _command_exit_code(envelope: CommandEnvelope) -> int:
    return 0 if envelope.ok else 1


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


def cmd_command(
    config: Config,
    *,
    json_output: bool = True,
) -> int:
    """Read a JSON command request from stdin and print a JSON result envelope."""
    payload = sys.stdin.read()
    if not json_output:
        print("error: only --json output is supported", file=sys.stderr)
        return 2
    try:
        request_payload = json.loads(payload)
    except (json.JSONDecodeError, TypeError, ValueError):
        request_payload = None
    request, parse_error = parse_command_request(payload)
    if parse_error is not None or request is None:
        envelope = CommandEnvelope.from_error(request, parse_error or error_value(
            "invalid_request", "unknown parse error"
        ))
        print(envelope.to_json(indent=2))
        return _command_exit_code(envelope)
    validation_error = validate_request(request)
    if validation_error is not None or not isinstance(request_payload, dict):
        envelope = CommandEnvelope.from_error(
            request,
            validation_error or error_value("invalid_request", "request must be an object"),
        )
        print(envelope.to_json(indent=2))
        return _command_exit_code(envelope)
    attempt = _try_daemon_attempt(config, "command.submit", request_payload)
    if attempt.result is not None:
        envelope = _strict_daemon_command_envelope(request, attempt.result)
        if envelope is not None:
            print(envelope.to_json(indent=2))
            return _command_exit_code(envelope)
    if attempt.request_started is False:
        envelope = _daemon_backend_failure_envelope(request, attempt)
        print(envelope.to_json(indent=2))
        return _command_exit_code(envelope)
    print("error: Tendwire daemon command result is unresolved", file=sys.stderr)
    return 2


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
        print(_daemon_payload_json(daemon_attempt.result, indent=2))
        return 0 if daemon_attempt.result.get("ok") is not False else 1
    if daemon_attempt.response_error is not None:
        print(_daemon_payload_json(daemon_attempt.response_error, indent=2))
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
        print(_daemon_payload_json(payload, indent=2))
        return 1
    payload = _daemon_read_payload(
        daemon_attempt,
        extra={"host_id": config.host_id, "name": params.get("name", "")},
    )
    print(_daemon_payload_json(payload, indent=2))
    return 1


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
    if args.command == "daemon":
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
        )

    if args.command == "attention":
        return _cmd_simple_read(
            config, "attention.list", json_output=args.json_output
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
        return _cmd_simple_read(
            config, "pending.list", json_output=args.json_output
        )

    if args.command == "command":
        return cmd_command(config, json_output=args.json_output)

    if args.command == "connector":
        return cmd_connector(config, args)

    if args.command == "daemon":
        return cmd_daemon(config)

    if args.command == "doctor":
        return _cmd_simple_read(
            config,
            "health.get",
            json_output=args.json_output,
            require_ok_status=True,
            trusted_daemon_json=True,
        )

    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
