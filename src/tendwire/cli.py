"""Tendwire command-line interface.

Console script entry point: tendwire = tendwire.cli:main
Module entry point: python -m tendwire.cli snapshot --json
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from typing import Any, Mapping

from .config import Config, load_config
from .core.commands import (
    ALLOWED_ACTIONS,
    STATUS_BACKEND_UNAVAILABLE,
    STATUS_INVALID_REQUEST,
    CommandEnvelope,
    CommandRequest,
    error_value,
    is_valid_request_id,
    parse_command_request,
    validate_public_command_envelope,
)
from .core.models import public_json_dumps
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


def _json_flag(parser: argparse.ArgumentParser, help_text: str | None = None) -> None:
    parser.add_argument(
        "--json", dest="json_output", action="store_true", default=True,
        help=help_text,
    )


def _json_subcommand(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
    name: str,
    description: str,
    subject: str,
) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(name, help=description)
    _json_flag(parser, f"Print {subject} as JSON (default).")
    return parser


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="tendwire", description="Local-first control plane for terminal-based agents."
    )
    parser.add_argument("--host-id", help="Override the host identifier used in snapshots.")
    parser.add_argument("--herdr-bin", help="Path or name of the herdr binary (default: herdr).")
    parser.add_argument(
        "--herdr-timeout", dest="herdr_timeout_seconds",
        help="Seconds to wait for each Herdr socket request (default: 5.0).",
    )
    parser.add_argument(
        "--socket-path",
        help="Unix socket path for daemon requests (default: data-dir/tendwire.sock).",
    )

    subparsers = parser.add_subparsers(dest="command")

    _json_subcommand(
        subparsers, "snapshot", "Print a neutral device-independent snapshot.", "snapshot"
    )
    _json_subcommand(
        subparsers, "attention", "Print neutral public attention items.", "attention"
    )
    turns_parser = subparsers.add_parser(
        "turns",
        help="Print neutral public turns derived from the current snapshot.",
    )
    _json_flag(turns_parser, "Print turns as JSON (default).")
    turns_parser.add_argument(
        "--schema-version", dest="schema_version", type=int, choices=(1, 2), default=1,
        help="Turn-list schema version (default: 1).",
    )
    turns_parser.add_argument(
        "--limit", type=_turn_list_limit, default=TURN_LIST_DEFAULT_LIMIT,
        help=(
            f"Maximum turns in this page (default: {TURN_LIST_DEFAULT_LIMIT}, "
            f"maximum: {TURN_LIST_MAX_LIMIT})."
        ),
    )
    turn_page_position = turns_parser.add_mutually_exclusive_group()
    turn_page_position.add_argument("--cursor", help="Opaque cursor for the next page.")
    turn_page_position.add_argument(
        "--since", help="Opaque token for turns newer than a completed traversal."
    )
    turn_parser = subparsers.add_parser("turn", help="Access bounded canonical turn content.")
    turn_actions = turn_parser.add_subparsers(dest="turn_action", required=True)
    content_parser = turn_actions.add_parser("content", help="Access turn content.")
    content_actions = content_parser.add_subparsers(dest="content_action", required=True)
    content_get = content_actions.add_parser("get", help="Fetch one bounded content page.")
    _json_flag(content_get)
    content_get.add_argument("--turn-id", dest="turn_id", required=True)
    content_get.add_argument("--revision", dest="content_revision", required=True)
    content_get.add_argument(
        "--field", choices=("user_text", "assistant_final_text"), required=True
    )
    content_get.add_argument("--cursor", default=None)
    delta_parser = turn_actions.add_parser(
        "delta", help="Read one cache-only public turn change page."
    )
    _json_flag(delta_parser)
    delta_parser.add_argument("--limit", type=_turn_delta_limit, default=TURN_DELTA_DEFAULT_LIMIT)
    delta_position = delta_parser.add_mutually_exclusive_group()
    delta_position.add_argument("--watermark", default=None)
    delta_position.add_argument("--cursor", default=None)

    _json_subcommand(
        subparsers, "pending",
        "Print neutral public pending interactions from the daemon.", "pending interactions",
    )
    _json_subcommand(
        subparsers, "command",
        "Read a JSON command request from stdin and print a JSON result.", "result",
    )
    daemon_parser = subparsers.add_parser(
        "daemon", help="Run the local Tendwire JSON request daemon."
    )
    daemon_parser.add_argument(
        "--db-path", dest="db_path", default=None,
        help="SQLite database path for daemon state (default: config path).",
    )
    daemon_parser.add_argument(
        "--socket-path", dest="socket_path", default=argparse.SUPPRESS,
        help="Unix socket path to listen on (default: data_dir/tendwire.sock).",
    )
    daemon_parser.add_argument(
        "--socket-group", dest="socket_group", default=argparse.SUPPRESS, metavar="GROUP",
        help="Share the daemon socket with a validated local group.",
    )

    _json_subcommand(
        subparsers, "doctor", "Print read-only Herdr diagnostics.", "diagnostics",
    )

    _add_connector_parser(subparsers)

    return parser


def _add_connector_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    connector_parser = subparsers.add_parser(
        "connector",
        help="Exercise the neutral connector outbox boundary with JSON-only output.",
    )
    actions = connector_parser.add_subparsers(dest="connector_action", required=True)

    def named_action(name: str, help_text: str) -> argparse.ArgumentParser:
        action_parser = actions.add_parser(name, help=help_text)
        action_parser.add_argument(
            "--name", required=True, help="Neutral connector queue name."
        )
        return action_parser

    prepare_parser = named_action(
        "prepare", "Stage one bounded neutral presentation-plan action from stdin."
    )
    _json_flag(prepare_parser, "Read one schema-v1 JSON action from stdin and print JSON.")

    poll_parser = named_action("poll", "Lease due connector outbox items.")
    poll_parser.add_argument("--limit", type=int, default=1)
    poll_parser.add_argument("--lease-seconds", dest="lease_seconds", type=int, default=None)

    inspect_parser = named_action(
        "inspect", "Inspect bounded unresolved connector items by neutral status."
    )
    inspect_parser.add_argument("--status", choices=("dead_letter",), required=True)
    inspect_parser.add_argument("--limit", type=_connector_inspect_limit, default=100)

    retry_parser = named_action(
        "retry", "Explicitly requeue one unresolved final-ready item."
    )
    retry_parser.add_argument("--final-identity", dest="final_identity", required=True)

    named_action("reclaim", "Expire stale connector leases.")

    for action in ("ack", "fail", "defer", "renew", "release"):
        action_parser = named_action(action, f"Apply connector.{action} to a live ref.")
        action_parser.add_argument("--ref", required=True)
        if action in {"ack", "fail", "defer"}:
            action_parser.add_argument("--response-json", dest="response_json", default=None)
        if action in {"fail", "defer"}:
            action_parser.add_argument("--reason", default="")
            action_parser.add_argument("--available-at", dest="available_at", default=None)
            action_parser.add_argument(
                "--delay-seconds", dest="delay_seconds", type=int, default=None
            )
        if action == "renew":
            action_parser.add_argument(
                "--lease-seconds", dest="lease_seconds", type=int, default=None
            )


def _try_daemon_attempt(
    config: Config,
    method: str,
    params: dict[str, Any] | None = None,
) -> _DaemonAttempt:
    """Return one result from the daemon's authoritative socket API."""
    from .daemon import default_socket_path
    try:
        from . import daemon_api
    except Exception:
        return _DaemonAttempt(error_kind="protocol", request_started=False)
    try:
        client = daemon_api.DaemonAPIClient(
            default_socket_path(config),
            timeout_seconds=_daemon_client_timeout_seconds(config, method),
            socket_group=config.socket_group,
        )
        response = client.request(method, params or {})
    except daemon_api.DaemonUnavailable as exc:
        if exc.request_started is False:
            return _DaemonAttempt(error_kind="unavailable", request_started=False)
        if exc.timed_out:
            return _DaemonAttempt(error_kind="timeout", request_started=True)
        return _DaemonAttempt(
            error_kind="unavailable",
            request_started=exc.request_started,
        )
    except daemon_api.DaemonProtocolError as exc:
        return _DaemonAttempt(error_kind="protocol", request_started=exc.request_started)
    except daemon_api.DaemonAPIError:
        return _DaemonAttempt(error_kind="protocol", request_started=None)
    if not isinstance(response, dict) or type(response.get("ok")) is not bool:
        return _DaemonAttempt(error_kind="protocol", request_started=True)
    if response.get("ok") is False:
        if isinstance(response.get("error"), dict):
            return _DaemonAttempt(
                response_error=dict(response),
                request_started=True,
            )
        return _DaemonAttempt(error_kind="protocol", request_started=True)
    result = response.get("result")
    if isinstance(result, dict):
        return _DaemonAttempt(result=dict(result), request_started=True)
    return _DaemonAttempt(error_kind="protocol", request_started=True)


def cmd_snapshot(
    config: Config,
    *,
    json_output: bool = True,
) -> int:
    """Read the daemon's current neutral snapshot."""
    return _cmd_simple_read(config, "snapshot.get", json_output=json_output, schema_version=2)


def _daemon_payload_json(payload: dict[str, Any], *, indent: int | None = None) -> str:
    """Serialize a response already validated by the local daemon boundary."""
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), indent=indent)


def _cmd_simple_read(
    config: Config,
    method: str,
    *,
    json_output: bool,
    params: dict[str, Any] | None = None,
    schema_version: int = 1,
    extra: Mapping[str, Any] | None = None,
    require_ok_status: bool = False,
    require_text: bool = False,
    trusted_daemon_json: bool = False,
) -> int:
    if not json_output:
        print("error: only --json output is supported", file=sys.stderr)
        return 2
    attempt = _try_daemon_attempt(config, method, params)
    if attempt.result is not None:
        payload = attempt.result
    elif attempt.response_error is not None:
        payload = attempt.response_error
    else:
        if attempt.error_kind == "timeout":
            status, message = "daemon_timeout", "Tendwire daemon request timed out"
        elif attempt.request_started is False:
            status, message = "daemon_unavailable", "Tendwire daemon is unavailable"
        else:
            status = "daemon_protocol_error"
            message = "Tendwire daemon returned an invalid response"
        payload = {
            "schema_version": schema_version,
            "ok": False,
            "status": status,
            "error": {"code": status, "message": message},
        }
        if extra:
            payload.update(extra)
    serializer = _daemon_payload_json if trusted_daemon_json else public_json_dumps
    print(serializer(payload, indent=2))
    success = payload.get("ok") is not False
    if require_ok_status:
        success = success and payload.get("status") == "ok"
    if require_text:
        success = success and isinstance(payload.get("text"), str)
    return 0 if success else 1


def _cmd_turn_read(config: Config, args: argparse.Namespace) -> int:
    """Read one authoritative turn list, content page, or delta page."""
    options: dict[str, Any] = {"trusted_daemon_json": True}
    if args.command == "turns":
        method = "turn.list"
        params = {
            "schema_version": args.schema_version,
            "limit": args.limit,
            "cursor": args.cursor,
            "since": args.since,
        }
        options.update(schema_version=args.schema_version, extra={"host_id": config.host_id})
    elif args.turn_action == "delta":
        method = "turn.delta"
        params = {"limit": args.limit, "watermark": args.watermark, "cursor": args.cursor}
        options["extra"] = {"projection_schema_version": 2}
    else:
        method = "turn.content.get"
        params = {
            "schema_version": 1,
            "turn_id": args.turn_id,
            "content_revision": args.content_revision,
            "field": args.field,
        }
        if args.cursor is not None:
            params["cursor"] = args.cursor
        options["require_text"] = True
    return _cmd_simple_read(config, method, json_output=args.json_output, params=params, **options)


def cmd_command(
    config: Config,
    *,
    json_output: bool = True,
) -> int:
    """Read a JSON command request from stdin and print a JSON result envelope."""
    if not json_output:
        print("error: only --json output is supported", file=sys.stderr)
        return 2
    raw = sys.stdin.read()
    try:
        payload = json.loads(raw)
    except (json.JSONDecodeError, TypeError, ValueError):
        payload = None
    if not isinstance(payload, dict):
        try:
            request, parse_error = parse_command_request(raw)
        except Exception:
            request, parse_error = None, None
        error = parse_error or error_value(STATUS_INVALID_REQUEST, "request must be a JSON object")
        envelope = CommandEnvelope.from_error(request, error)
        print(envelope.to_json(indent=2))
        return 1
    candidate = None
    expected_identity: tuple[str, str | None] = ("", None)
    try:
        candidate, parse_error = parse_command_request(raw)
        if candidate is not None:
            action = candidate.action if candidate.action in ALLOWED_ACTIONS else ""
            request_id = (
                candidate.request_id
                if action and is_valid_request_id(candidate.request_id)
                else None
            )
            expected_identity = (action, request_id)
    except Exception:
        candidate = None
    attempt = _try_daemon_attempt(config, "command.submit", payload)
    if attempt.result is not None:
        try:
            envelope = validate_public_command_envelope(
                CommandEnvelope.from_dict(attempt.result), candidate
            )
        except (TypeError, ValueError):
            envelope = None
        if envelope is not None and (
            envelope.action, envelope.request_id
        ) != expected_identity:
            envelope = None
        if envelope is not None:
            print(envelope.to_json(indent=2))
            return 0 if envelope.ok else 1
    if attempt.request_started is False:
        action = payload.get("action") if isinstance(payload.get("action"), str) else ""
        request_id = payload.get("request_id")
        request_id = request_id if isinstance(request_id, str) else None
        request = CommandRequest(action=action, request_id=request_id)
        envelope = CommandEnvelope.from_error(
            request,
            error_value(STATUS_BACKEND_UNAVAILABLE, "Tendwire daemon backend is unavailable"),
        )
        print(envelope.to_json(indent=2))
        return 1
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
    if args.connector_action in {"inspect", "retry"} and args.name != "turn-final":
        raise ValueError(f"{args.connector_action} requires --name turn-final")
    if args.connector_action == "inspect":
        params.update(schema_version=1, status=args.status, limit=args.limit)
        return params
    if args.connector_action == "retry":
        final_identity = str(args.final_identity).strip()
        if not final_identity:
            raise ValueError("retry requires a final identity")
        params.update(schema_version=1, final_identity=final_identity)
        return params
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
            "error": {"code": "invalid_request", "message": str(exc)},
        }
        print(public_json_dumps(payload, indent=2))
        return 2
    return _cmd_simple_read(
        config,
        method,
        json_output=True,
        params=params,
        extra={"host_id": config.host_id, "name": params.get("name", "")},
        trusted_daemon_json=True,
    )


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

    if args.command in {"attention", "pending"}:
        return _cmd_simple_read(
            config, f"{args.command}.list", json_output=args.json_output
        )

    if args.command in {"turns", "turn"}:
        return _cmd_turn_read(config, args)

    if args.command == "command":
        return cmd_command(config, json_output=args.json_output)

    if args.command == "connector":
        return cmd_connector(config, args)

    if args.command == "daemon":
        from .daemon import run_daemon
        from .daemon_api import DaemonUnavailable
        from ._version import __version__

        try:
            return run_daemon(config)
        except DaemonUnavailable as exc:
            print(f"tendwire daemon {__version__}: startup failed: {exc}", file=sys.stderr)
            return 1

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
