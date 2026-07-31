"""Bounded black-box compatibility probe for external ACP v1 adapters.

The probe deliberately knows only the stable ACP wire protocol.  It launches a
caller-supplied executable without a shell, negotiates a fresh connection, and
returns a fixed public-safe capability summary.  Adapter output, argv, paths,
environment values, stderr, exception text, and extension names never enter the
report.

Operator entry point::

    python -m tendwire.backends.acp_probe -- adapter-command arg ...
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Any

from .acp_client import (
    AcpClient,
    AcpProtocolVersionError,
    AcpRequestTimeoutError,
    AcpTransportError,
)
from .acp_protocol import (
    ACP_PROTOCOL_VERSION,
    AcpEnvelopeError,
    AcpFramingError,
    AcpRemoteError,
    InitializeResult,
)

PROBE_SCHEMA_VERSION = 1
DEFAULT_PROBE_TIMEOUT_SECONDS = 5.0
DEFAULT_PROBE_CLOSE_TIMEOUT_SECONDS = 1.0
MAX_PROBE_TIMEOUT_SECONDS = 30.0
MAX_PROBE_CLOSE_TIMEOUT_SECONDS = 5.0
PROBE_MAX_FRAME_BYTES = 1024 * 1024
PROBE_MAX_PENDING_EVENTS = 16
PROBE_STDERR_LIMIT_BYTES = 4096
_MAX_REPORTED_COUNT = 1000

_CAPABILITY_KEYS = (
    "session_new",
    "session_prompt",
    "session_cancel",
    "session_update",
    "session_load",
    "session_list",
    "session_delete",
    "session_resume",
    "session_close",
    "additional_directories",
    "prompt_image",
    "prompt_audio",
    "prompt_embedded_context",
    "mcp_http",
    "mcp_sse",
    "auth_logout",
)
_KNOWN_TOP_LEVEL_CAPABILITIES = {
    "loadSession",
    "promptCapabilities",
    "mcpCapabilities",
    "sessionCapabilities",
    "auth",
    "_meta",
}
_KNOWN_NESTED_CAPABILITIES = {
    "promptCapabilities": {"image", "audio", "embeddedContext", "_meta"},
    "mcpCapabilities": {"http", "sse", "_meta"},
    "sessionCapabilities": {
        "list",
        "delete",
        "additionalDirectories",
        "resume",
        "close",
        "_meta",
    },
    "auth": {"logout", "_meta"},
}


class ProbeFailure(str, Enum):
    """Fixed failure categories safe to expose to operators and automation."""

    INVALID_CONFIGURATION = "invalid_configuration"
    LAUNCH_FAILED = "launch_failed"
    TIMEOUT = "timeout"
    PROTOCOL_VERSION = "protocol_version"
    PROTOCOL = "protocol_error"
    TRANSPORT = "transport_error"
    SHUTDOWN = "shutdown_failed"
    INTERNAL = "internal_error"


@dataclass(frozen=True, slots=True)
class AcpAdapterProbeReport:
    """Fixed-shape, bounded result that contains no adapter-controlled text."""

    compatible: bool
    protocol_version: int | None
    capabilities: Mapping[str, bool]
    authentication_method_count: int
    authentication_method_count_capped: bool
    extension_capability_count: int
    extension_capability_count_capped: bool
    process_reaped: bool
    failure: ProbeFailure | None
    schema_version: int = PROBE_SCHEMA_VERSION

    def to_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "compatible": self.compatible,
            "protocol_version": self.protocol_version,
            "capabilities": {
                key: bool(self.capabilities.get(key, False))
                for key in _CAPABILITY_KEYS
            },
            "authentication": {
                "method_count": self.authentication_method_count,
                "method_count_capped": self.authentication_method_count_capped,
            },
            "extensions": {
                "capability_count": self.extension_capability_count,
                "capability_count_capped": self.extension_capability_count_capped,
            },
            "process_reaped": self.process_reaped,
            "failure": self.failure.value if self.failure is not None else None,
        }


def probe_adapter(
    argv: Sequence[str | os.PathLike[str]],
    *,
    cwd: str | os.PathLike[str] | None = None,
    env: Mapping[str, str] | None = None,
    timeout_seconds: float = DEFAULT_PROBE_TIMEOUT_SECONDS,
    close_timeout_seconds: float = DEFAULT_PROBE_CLOSE_TIMEOUT_SECONDS,
) -> AcpAdapterProbeReport:
    """Negotiate ACP v1 with one separately installed adapter executable.

    A new subprocess and initialization exchange are used for every call.  No
    session is created and no adapter source package is imported.  All expected
    failures become a fixed-category incompatible report; no raw diagnostic text
    crosses this black-box boundary.
    """

    try:
        timeout = _bounded_timeout(
            timeout_seconds,
            "timeout_seconds",
            maximum=MAX_PROBE_TIMEOUT_SECONDS,
        )
        close_timeout = _bounded_timeout(
            close_timeout_seconds,
            "close_timeout_seconds",
            maximum=MAX_PROBE_CLOSE_TIMEOUT_SECONDS,
        )
        client = AcpClient(
            argv,
            cwd=cwd,
            env=env,
            request_timeout=timeout,
            close_timeout=close_timeout,
            max_frame_bytes=PROBE_MAX_FRAME_BYTES,
            max_pending_events=PROBE_MAX_PENDING_EVENTS,
            stderr_limit_bytes=PROBE_STDERR_LIMIT_BYTES,
        )
    except Exception:
        return _failure_report(ProbeFailure.INVALID_CONFIGURATION)

    initialized: InitializeResult | None = None
    failure: ProbeFailure | None = None
    started = False
    try:
        client.start()
        started = True
        initialized = client.initialize(timeout=timeout)
    except Exception as exc:  # exception text is intentionally never returned
        failure = _failure_category(exc, started=started)

    process_reaped = False
    try:
        client.close()
    except Exception:
        if failure is None:
            failure = ProbeFailure.SHUTDOWN
    finally:
        process = client.process
        process_reaped = process is None or process.poll() is not None
        if not process_reaped and failure is None:
            failure = ProbeFailure.SHUTDOWN

    if initialized is None:
        return _failure_report(failure or ProbeFailure.INTERNAL, process_reaped)

    capabilities = _capability_summary(initialized)
    auth_count, auth_capped = _bounded_count(len(initialized.auth_methods))
    extension_count, extension_capped = _bounded_count(
        _extension_capability_count(initialized.capabilities.raw)
    )
    compatible = (
        failure is None
        and process_reaped
        and initialized.protocol_version == ACP_PROTOCOL_VERSION
    )
    return AcpAdapterProbeReport(
        compatible=compatible,
        protocol_version=initialized.protocol_version,
        capabilities=MappingProxyType(capabilities),
        authentication_method_count=auth_count,
        authentication_method_count_capped=auth_capped,
        extension_capability_count=extension_count,
        extension_capability_count_capped=extension_capped,
        process_reaped=process_reaped,
        failure=failure,
    )


def _capability_summary(initialized: InitializeResult) -> dict[str, bool]:
    raw = initialized.capabilities.raw
    prompt = _mapping(raw.get("promptCapabilities"))
    mcp = _mapping(raw.get("mcpCapabilities"))
    session = _mapping(raw.get("sessionCapabilities"))
    auth = _mapping(raw.get("auth"))
    return {
        # ACP v1 baseline methods are guaranteed by a successful negotiation.
        "session_new": True,
        "session_prompt": True,
        "session_cancel": True,
        "session_update": True,
        "session_load": initialized.capabilities.load_session,
        "session_list": initialized.capabilities.session_list,
        "session_delete": initialized.capabilities.session_delete,
        "session_resume": initialized.capabilities.session_resume,
        "session_close": initialized.capabilities.session_close,
        "additional_directories": initialized.capabilities.additional_directories,
        "prompt_image": prompt.get("image") is True,
        "prompt_audio": prompt.get("audio") is True,
        "prompt_embedded_context": prompt.get("embeddedContext") is True,
        "mcp_http": mcp.get("http") is True,
        "mcp_sse": mcp.get("sse") is True,
        "auth_logout": isinstance(auth.get("logout"), Mapping),
    }


def _extension_capability_count(raw: Mapping[str, Any]) -> int:
    count = sum(key not in _KNOWN_TOP_LEVEL_CAPABILITIES for key in raw)
    for section, known_keys in _KNOWN_NESTED_CAPABILITIES.items():
        nested = raw.get(section)
        if isinstance(nested, Mapping):
            count += sum(key not in known_keys for key in nested)
    return count


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _bounded_count(value: int) -> tuple[int, bool]:
    return min(value, _MAX_REPORTED_COUNT), value > _MAX_REPORTED_COUNT


def _failure_report(
    failure: ProbeFailure,
    process_reaped: bool = True,
) -> AcpAdapterProbeReport:
    return AcpAdapterProbeReport(
        compatible=False,
        protocol_version=None,
        capabilities=MappingProxyType({key: False for key in _CAPABILITY_KEYS}),
        authentication_method_count=0,
        authentication_method_count_capped=False,
        extension_capability_count=0,
        extension_capability_count_capped=False,
        process_reaped=process_reaped,
        failure=failure,
    )


def _failure_category(exc: Exception, *, started: bool) -> ProbeFailure:
    if isinstance(exc, AcpRequestTimeoutError):
        return ProbeFailure.TIMEOUT
    if isinstance(exc, AcpProtocolVersionError):
        return ProbeFailure.PROTOCOL_VERSION
    if isinstance(exc, (AcpFramingError, AcpEnvelopeError, AcpRemoteError)):
        return ProbeFailure.PROTOCOL
    if isinstance(exc, AcpTransportError):
        return ProbeFailure.TRANSPORT if started else ProbeFailure.LAUNCH_FAILED
    if isinstance(exc, (TypeError, ValueError, OSError)):
        return ProbeFailure.INVALID_CONFIGURATION
    return ProbeFailure.INTERNAL


def _bounded_timeout(value: float, name: str, *, maximum: float) -> float:
    result = float(value)
    if not math.isfinite(result) or not 0 < result <= maximum:
        raise ValueError(f"{name} must be positive and at most {maximum:g}")
    return result


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m tendwire.backends.acp_probe",
        description="Probe a separately installed ACP v1 adapter executable.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_PROBE_TIMEOUT_SECONDS,
        help=f"initialization timeout in seconds (maximum {MAX_PROBE_TIMEOUT_SECONDS:g})",
    )
    parser.add_argument(
        "--close-timeout",
        type=float,
        default=DEFAULT_PROBE_CLOSE_TIMEOUT_SECONDS,
        help=(
            "shutdown stage timeout in seconds "
            f"(maximum {MAX_PROBE_CLOSE_TIMEOUT_SECONDS:g})"
        ),
    )
    parser.add_argument(
        "--cwd",
        default=None,
        help="absolute process working directory (never included in output)",
    )
    parser.add_argument(
        "adapter_argv",
        nargs=argparse.REMAINDER,
        help="adapter executable and arguments, conventionally after --",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    adapter_argv = list(args.adapter_argv)
    if adapter_argv and adapter_argv[0] == "--":
        adapter_argv.pop(0)
    if not adapter_argv:
        parser.error("an adapter executable is required after --")
    report = probe_adapter(
        adapter_argv,
        cwd=args.cwd,
        timeout_seconds=args.timeout,
        close_timeout_seconds=args.close_timeout,
    )
    sys.stdout.write(json.dumps(report.to_payload(), sort_keys=True, separators=(",", ":")))
    sys.stdout.write("\n")
    return 0 if report.compatible else 1


if __name__ == "__main__":
    raise SystemExit(main())
