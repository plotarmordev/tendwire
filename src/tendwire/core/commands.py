"""Neutral command request/result/envelope contract for Tendwire.

This module defines the milestone-1 command contract: JSON request shapes,
result/envelope shapes, validation, and sanitization. It depends only on the
Python standard library and sibling core helpers. It must not import subprocess,
backends, stores, Herdr, Herdres, Telegram, or connector modules.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Literal

from .models import (
    FORBIDDEN_FIELD_NAMES,
    Snapshot,
    Worker,
    public_json_dumps,
    sanitize_public_mapping,
    sanitize_public_text,
    sanitize_public_value,
    stable_fingerprint,
    stable_json_dumps,
    _optional_string,
    _string_value,
)


COMMAND_REQUEST_SCHEMA_VERSION = 1
COMMAND_ENVELOPE_SCHEMA_VERSION = 2
COMMAND_ENVELOPE_V3_SCHEMA_VERSION = 3
SUPPORTED_COMMAND_ENVELOPE_SCHEMA_VERSIONS = frozenset(
    {COMMAND_ENVELOPE_SCHEMA_VERSION, COMMAND_ENVELOPE_V3_SCHEMA_VERSION}
)

ALLOWED_ACTIONS = frozenset(
    {
        "noop",
        "read_snapshot",
        "resolve_target",
        "send_instruction",
        "answer_pending",
        "answer_decision",
    }
)
REQUEST_ALLOWED_FIELDS = frozenset(
    {
        "schema_version",
        "action",
        "request_id",
        "dry_run",
        "target",
        "instruction",
        "params",
        "response_schema_version",
    }
)

# Canonical status values for command results/envelopes.
STATUS_NOOP = "noop"
STATUS_SNAPSHOT = "snapshot"
STATUS_RESOLVED = "resolved"
STATUS_DRY_RUN = "dry_run"
STATUS_ACCEPTED = "accepted"
STATUS_REJECTED = "rejected"
STATUS_NOT_FOUND = "not_found"
STATUS_AMBIGUOUS_TARGET = "ambiguous_target"
STATUS_STALE_TARGET = "stale_target"
STATUS_BACKEND_UNAVAILABLE = "backend_unavailable"
STATUS_BACKEND_UNSUPPORTED = "backend_unsupported"
STATUS_AMBIGUOUS_BACKEND_TARGET = "ambiguous_backend_target"
STATUS_BACKEND_FAILED = "backend_failed"
STATUS_DUPLICATE_REQUEST = "duplicate_request"
STATUS_REQUEST_STATE_UNCERTAIN = "request_state_uncertain"
STATUS_INVALID_REQUEST = "invalid_request"
STATUS_PENDING = "pending"
STATUS_ANSWER_IN_PROGRESS = "answer_in_progress"
STATUS_DECISION_NOT_PENDING = "decision_not_pending"
STATUS_UNKNOWN_WORKER = "unknown_worker"
STATUS_INVALID_SELECTION = "invalid_selection"
STATUS_UNSUPPORTED_DECISION = "unsupported_decision"

CommandDisposition = Literal[
    "no_receipt",
    "in_progress",
    "terminal_accepted",
    "terminal_rejected",
    "terminal_uncertain",
]

DISPOSITION_NO_RECEIPT: CommandDisposition = "no_receipt"
DISPOSITION_IN_PROGRESS: CommandDisposition = "in_progress"
DISPOSITION_TERMINAL_ACCEPTED: CommandDisposition = "terminal_accepted"
DISPOSITION_TERMINAL_REJECTED: CommandDisposition = "terminal_rejected"
DISPOSITION_TERMINAL_UNCERTAIN: CommandDisposition = "terminal_uncertain"
VALID_DISPOSITIONS = frozenset(
    {
        DISPOSITION_NO_RECEIPT,
        DISPOSITION_IN_PROGRESS,
        DISPOSITION_TERMINAL_ACCEPTED,
        DISPOSITION_TERMINAL_REJECTED,
        DISPOSITION_TERMINAL_UNCERTAIN,
    }
)

VALID_STATUSES = frozenset(
    {
        STATUS_NOOP,
        STATUS_SNAPSHOT,
        STATUS_RESOLVED,
        STATUS_DRY_RUN,
        STATUS_ACCEPTED,
        STATUS_REJECTED,
        STATUS_NOT_FOUND,
        STATUS_AMBIGUOUS_TARGET,
        STATUS_STALE_TARGET,
        STATUS_BACKEND_UNAVAILABLE,
        STATUS_BACKEND_UNSUPPORTED,
        STATUS_AMBIGUOUS_BACKEND_TARGET,
        STATUS_BACKEND_FAILED,
        STATUS_DUPLICATE_REQUEST,
        STATUS_REQUEST_STATE_UNCERTAIN,
        STATUS_INVALID_REQUEST,
        STATUS_PENDING,
        STATUS_ANSWER_IN_PROGRESS,
        STATUS_DECISION_NOT_PENDING,
        STATUS_UNKNOWN_WORKER,
        STATUS_INVALID_SELECTION,
        STATUS_UNSUPPORTED_DECISION,
    }
)

# Durable rejections are possible only after a live mutation has a canonical
# identity. Keep this explicit so a new neutral, success, pending, or uncertain
# status cannot silently become valid stored rejection evidence.
TERMINAL_MUTATION_REJECTION_STATUSES = frozenset(
    {
        STATUS_REJECTED,
        STATUS_STALE_TARGET,
        STATUS_BACKEND_UNAVAILABLE,
        STATUS_BACKEND_UNSUPPORTED,
        STATUS_AMBIGUOUS_BACKEND_TARGET,
        STATUS_BACKEND_FAILED,
        STATUS_DUPLICATE_REQUEST,
        STATUS_DECISION_NOT_PENDING,
        STATUS_UNKNOWN_WORKER,
        STATUS_INVALID_SELECTION,
        STATUS_UNSUPPORTED_DECISION,
    }
)

# A live no-receipt failure has no durable authority. These statuses cover
# validation/target failures and the intermediate pre-reservation envelopes
# used by the authoritative submission path before it terminalizes a failure.
LIVE_MUTATION_NO_RECEIPT_REJECTION_STATUSES = frozenset(
    {
        STATUS_INVALID_REQUEST,
        STATUS_REJECTED,
        STATUS_NOT_FOUND,
        STATUS_AMBIGUOUS_TARGET,
        STATUS_STALE_TARGET,
        STATUS_BACKEND_UNAVAILABLE,
        STATUS_BACKEND_UNSUPPORTED,
        STATUS_AMBIGUOUS_BACKEND_TARGET,
        STATUS_BACKEND_FAILED,
        STATUS_ANSWER_IN_PROGRESS,
        STATUS_DECISION_NOT_PENDING,
        STATUS_UNKNOWN_WORKER,
        STATUS_INVALID_SELECTION,
        STATUS_UNSUPPORTED_DECISION,
    }
)

# Dry-run mutations may fail only during validation or public target
# resolution. Backend failures cannot describe a preview that performs no I/O.
DRY_RUN_MUTATION_NO_RECEIPT_REJECTION_STATUSES = frozenset(
    {
        STATUS_INVALID_REQUEST,
        STATUS_INVALID_SELECTION,
        STATUS_REJECTED,
        STATUS_NOT_FOUND,
        STATUS_AMBIGUOUS_TARGET,
        STATUS_STALE_TARGET,
    }
)

# Neutral target fields permitted in command requests.
TARGET_ALLOWED_FIELDS = frozenset({"worker_id", "worker_fingerprint", "space_id", "name"})

# Selectors that name a worker durably. A worker_fingerprint is a mutable
# observation precondition -- "proceed only if the worker still looks like this"
# -- and never worker identity, so it names no target on its own. Two different
# fingerprints would then be indistinguishable to any identity-based idempotency
# key, letting one request ID claim another worker's stored result. Every target
# must carry at least one of these.
TARGET_STABLE_SELECTOR_FIELDS = frozenset({"worker_id", "space_id", "name"})
INSTRUCTION_ALLOWED_FIELDS = frozenset({"text"})
ANSWER_PENDING_PARAM_FIELDS = frozenset(
    {"pending_id", "pending_fingerprint", "choice_id"}
)
ANSWER_DECISION_PARAM_FIELDS = frozenset({"decision_ref", "selection"})

# Connector, low-level terminal, routing, and private fields rejected anywhere in a request.
FORBIDDEN_REQUEST_FIELDS = FORBIDDEN_FIELD_NAMES

# Conservative forbidden-field matching including common case and separator variants.
_FORBIDDEN_REQUEST_COMPACT = frozenset(name.replace("_", "") for name in FORBIDDEN_REQUEST_FIELDS)

MAX_INSTRUCTION_LENGTH = 4096
_REQUEST_ID_RE = re.compile(r"[A-Za-z0-9._-]{1,128}", re.ASCII)
_TURN_SUBMISSION_ID_RE = re.compile(r"twsub1\.[0-9a-f]{64}", re.ASCII)
_INSTRUCTION_FINGERPRINT_DOMAIN = b"tendwire.instruction-fingerprint.v1"
_TURN_SUBMISSION_ID_DOMAIN = b"tendwire.turn-submission-id.v1"

# Workers that must not receive instructions.
_DISALLOWED_WORKER_STATUSES = frozenset({"closed", "failed", "unknown"})


def _compact_field_name(key: Any) -> str:
    return str(key).lower().replace("-", "_").replace(".", "_").replace("_", "")


def _is_forbidden_request_field(key: Any) -> bool:
    normalized = str(key).lower().replace("-", "_").replace(".", "_")
    compact = _compact_field_name(key)
    return normalized in FORBIDDEN_REQUEST_FIELDS or compact in _FORBIDDEN_REQUEST_COMPACT


def sanitize_command_result(value: Any) -> Any:
    """Return a JSON-safe value with command-public forbidden fields removed.

    Command results share the same public forbidden-key superset as snapshots,
    turns, and pending interactions so public JSON surfaces do not drift.
    """
    return sanitize_public_value(value)


def _clean_mapping(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return {str(k): v for k, v in value.items()}
    return {}


def _clean_public_mapping(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    return sanitize_public_mapping(value)


def _find_forbidden_fields(value: Any, path: str = "$") -> list[str]:
    """Return paths of forbidden connector/terminal fields found in a value."""
    found: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            if _is_forbidden_request_field(key):
                found.append(f"{path}.{key}")
            found.extend(_find_forbidden_fields(item, f"{path}.{key}"))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            found.extend(_find_forbidden_fields(item, f"{path}[{index}]"))
    return found


def error_value(
    code: str,
    message: str,
    *,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return a neutral, sanitized error object."""
    return sanitize_public_mapping(
        {
            "code": _string_value(code, STATUS_REJECTED),
            "message": _string_value(message),
            "details": details or {},
        }
    )


def instruction_text_error(code: str, message: str) -> dict[str, Any]:
    return error_value(code, message, details={"field": "instruction.text"})


def _contains_bracketed_paste(text: str) -> bool:
    return "\x1b[200~" in text or "\x1b[201~" in text


def _contains_c1_control(text: str) -> bool:
    return any(0x80 <= ord(char) <= 0x9F for char in text)


def _is_raw_instruction_control(char: str) -> bool:
    code = ord(char)
    return (code < 32 and code not in {9, 10}) or code == 127


def validate_instruction_text(text: Any) -> dict[str, Any] | None:
    """Validate instruction text and return an error dict, or None if valid."""
    if text is None:
        return instruction_text_error(STATUS_INVALID_REQUEST, "instruction.text is required")
    if not isinstance(text, str):
        return instruction_text_error(STATUS_INVALID_REQUEST, "instruction.text must be a string")
    if not text:
        return instruction_text_error(STATUS_INVALID_REQUEST, "instruction.text must not be empty")
    if len(text) > MAX_INSTRUCTION_LENGTH:
        return instruction_text_error(
            STATUS_INVALID_REQUEST,
            f"instruction.text exceeds maximum length of {MAX_INSTRUCTION_LENGTH}",
        )
    if "\x00" in text:
        return instruction_text_error(STATUS_INVALID_REQUEST, "instruction.text must not contain NUL")
    if _contains_bracketed_paste(text):
        return instruction_text_error(
            STATUS_INVALID_REQUEST,
            "instruction.text must not contain bracketed-paste sequences",
        )
    if "\x1b" in text:
        return instruction_text_error(
            STATUS_INVALID_REQUEST,
            "instruction.text must not contain escape sequences",
        )
    if _contains_c1_control(text):
        return instruction_text_error(
            STATUS_INVALID_REQUEST,
            "instruction.text must not contain C1 control characters",
        )
    # Reject C0 controls except LF and tab, plus DEL.
    for char in text:
        if _is_raw_instruction_control(char):
            return instruction_text_error(
                STATUS_INVALID_REQUEST,
                "instruction.text must not contain raw control characters",
            )
    return None


def normalize_instruction_text(text: Any) -> str:
    """Return the Phase-1 whitespace-normalized instruction text.

    Line boundaries remain significant, while runs of horizontal or other
    intra-line whitespace are collapsed. The function is deliberately pure so
    claim matching and the Phase-2 ledger cannot drift apart.

    Herdr observations may retain terminal framing bytes at the outer edge of
    otherwise exact input (for example a trailing U+0001). Submitted
    instructions reject every such C0/C1 byte, so ignoring only edge controls
    is collision-safe for ledger matching. Controls inside the instruction
    remain significant and therefore fail closed.
    """
    raw = str(text or "")
    start = 0
    end = len(raw)
    while start < end and (
        _is_raw_instruction_control(raw[start])
        or 0x80 <= ord(raw[start]) <= 0x9F
    ):
        start += 1
    while end > start and (
        _is_raw_instruction_control(raw[end - 1])
        or 0x80 <= ord(raw[end - 1]) <= 0x9F
    ):
        end -= 1
    return "\n".join(
        " ".join(line.split()) for line in raw[start:end].splitlines()
    ).strip()


def instruction_fingerprint(text: Any) -> str:
    """Return an opaque, versioned digest suitable for ledger matching.

    Valid instruction text can consist entirely of whitespace even though its
    normalized form is empty. Preserve the normalized matching behavior for
    ordinary text, but fingerprint the raw text in that edge case so shadow
    ledger bookkeeping can never reject an otherwise valid legacy send.
    """
    normalized = normalize_instruction_text(text)
    fingerprint_text = normalized or str(text or "")
    digest = hashlib.sha256(
        _INSTRUCTION_FINGERPRINT_DOMAIN
        + b"\x00"
        + fingerprint_text.encode("utf-8")
    ).hexdigest()
    return f"twins1.{digest}"


def turn_submission_id(host_id: Any, request_id: Any) -> str:
    """Return the deterministic opaque submission ID for one host/request."""
    clean_host_id = str(host_id or "").strip()
    clean_request_id = str(request_id or "").strip()
    if not clean_host_id or not clean_request_id:
        raise ValueError("turn submission identity fields must be non-empty")
    digest = hashlib.sha256(
        _TURN_SUBMISSION_ID_DOMAIN
        + b"\x00"
        + clean_host_id.encode("utf-8")
        + b"\x00"
        + clean_request_id.encode("utf-8")
    ).hexdigest()
    return f"twsub1.{digest}"


def is_turn_submission_id(value: Any) -> bool:
    """Return whether value is a supported opaque submission ID."""
    return isinstance(value, str) and _TURN_SUBMISSION_ID_RE.fullmatch(value) is not None


def _validate_target_shape(target: dict[str, Any] | None) -> dict[str, Any] | None:
    if target is None:
        return None
    if not isinstance(target, dict):
        return error_value(STATUS_INVALID_REQUEST, "target must be an object", details={"field": "target"})
    extra = set(target.keys()) - TARGET_ALLOWED_FIELDS
    if extra:
        return error_value(
            STATUS_INVALID_REQUEST,
            f"target contains disallowed fields: {sorted(extra)}",
            details={"field": "target", "disallowed": sorted(extra)},
        )
    if _string_value(target.get("worker_fingerprint")) and not _target_has_stable_selector(
        target
    ):
        return error_value(
            STATUS_INVALID_REQUEST,
            "target requires a stable selector beside worker_fingerprint",
            details={
                "field": "target",
                "allowed": sorted(TARGET_STABLE_SELECTOR_FIELDS),
            },
        )
    return None


def _target_has_stable_selector(target: dict[str, Any] | None) -> bool:
    if not isinstance(target, dict):
        return False
    return any(
        _string_value(target.get(field)) for field in TARGET_STABLE_SELECTOR_FIELDS
    )


def is_valid_request_id(value: Any) -> bool:
    """Return whether value is an exact command request-ID token.

    IDs are opaque ASCII and are never trimmed, normalized, or case-folded.
    """
    return isinstance(value, str) and _REQUEST_ID_RE.fullmatch(value) is not None


def _validate_instruction_shape(instruction: dict[str, Any] | None) -> dict[str, Any] | None:
    if instruction is None:
        return None
    if not isinstance(instruction, dict):
        return error_value(
            STATUS_INVALID_REQUEST, "instruction must be an object", details={"field": "instruction"}
        )
    extra = set(instruction.keys()) - INSTRUCTION_ALLOWED_FIELDS
    if extra:
        return error_value(
            STATUS_INVALID_REQUEST,
            f"instruction contains disallowed fields: {sorted(extra)}",
            details={"field": "instruction", "disallowed": sorted(extra)},
        )
    return validate_instruction_text(instruction.get("text"))


@dataclass(frozen=True)
class CommandRequest:
    """A neutral, validated command request."""

    action: str
    schema_version: int = COMMAND_REQUEST_SCHEMA_VERSION
    request_id: str | None = None
    dry_run: bool = True
    target: dict[str, Any] | None = None
    instruction: dict[str, Any] | None = None
    params: dict[str, Any] | None = None
    response_schema_version: int = COMMAND_ENVELOPE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "action", _string_value(self.action))
        object.__setattr__(self, "schema_version", self.schema_version)
        object.__setattr__(self, "dry_run", self.dry_run)
        object.__setattr__(self, "target", _clean_mapping(self.target))
        object.__setattr__(self, "instruction", _clean_mapping(self.instruction))
        object.__setattr__(self, "params", _clean_mapping(self.params))
        object.__setattr__(self, "response_schema_version", self.response_schema_version)

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "schema_version": self.schema_version,
            "action": self.action,
            "request_id": self.request_id,
            "dry_run": self.dry_run,
            "target": self.target,
            "instruction": self.instruction,
            "params": self.params,
        }
        if self.response_schema_version != COMMAND_ENVELOPE_SCHEMA_VERSION:
            payload["response_schema_version"] = self.response_schema_version
        return payload

    def payload_fingerprint(self) -> str:
        """Return the legacy raw-request fingerprint used by compatibility callers.

        This includes request identity and unresolved selector spelling, so it is
        not authoritative for mutating-command idempotency.  New mutation
        persistence must use :func:`build_canonical_mutation` after resolving the
        public worker identity.
        """
        return stable_fingerprint(self.to_dict())

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CommandRequest":
        return cls(
            action=data.get("action", ""),
            schema_version=data.get("schema_version", COMMAND_REQUEST_SCHEMA_VERSION),
            request_id=data.get("request_id"),
            dry_run=data.get("dry_run", True),
            target=data.get("target"),
            instruction=data.get("instruction"),
            params=data.get("params"),
            response_schema_version=data.get(
                "response_schema_version",
                COMMAND_ENVELOPE_SCHEMA_VERSION,
            ),
        )


CANONICAL_MUTATION_VERSION = 1


@dataclass(frozen=True)
class CanonicalMutation:
    """One resolved public mutation and its authoritative canonical identity."""

    canonical_version: int
    action: str
    public_worker_id: str
    canonical_json: str
    fingerprint: str


def build_canonical_mutation(
    request: CommandRequest,
    *,
    public_worker_id: str,
) -> CanonicalMutation:
    """Build canonical v1 identity after authoritative public-worker resolution.

    Request IDs, dry-run state, unresolved selectors, worker observation
    fingerprints, connector origin metadata, and private binding data are
    intentionally outside this representation.
    """
    if not isinstance(request, CommandRequest):
        raise TypeError("request must be a CommandRequest")
    if request.action not in {"send_instruction", "answer_pending", "answer_decision"}:
        raise ValueError(
            "canonical mutations require send_instruction, answer_pending, or answer_decision"
        )
    if request.dry_run is not False:
        raise ValueError("canonical mutations require a non-dry-run request")
    request_error = validate_request(request)
    if request_error is not None:
        raise ValueError(str(request_error.get("message") or "invalid command request"))
    if not isinstance(public_worker_id, str) or not public_worker_id.strip():
        raise ValueError("public_worker_id must be a nonblank string")

    if request.action == "send_instruction":
        assert request.instruction is not None
        canonical_payload: dict[str, Any] = {
            "canonical_version": CANONICAL_MUTATION_VERSION,
            "action": "send_instruction",
            "target": {"worker_id": public_worker_id},
            "instruction": {"text": request.instruction["text"]},
            "options": {},
        }
    elif request.action == "answer_pending":
        assert request.params is not None
        canonical_payload = {
            "canonical_version": CANONICAL_MUTATION_VERSION,
            "action": "answer_pending",
            "target": {"worker_id": public_worker_id},
            "pending": {
                "pending_id": request.params["pending_id"],
                "pending_fingerprint": request.params["pending_fingerprint"],
                "choice_id": request.params["choice_id"],
            },
            "options": {},
        }
    else:
        assert request.params is not None
        selection = request.params["selection"]
        if "option_refs" in selection:
            canonical_selection: dict[str, Any] = {
                "option_refs": sorted(
                    selection["option_refs"],
                    key=lambda ref: int(ref) if str(ref).isdigit() else -1,
                )
            }
        else:
            canonical_selection = {"text": selection["text"]}
        canonical_payload = {
            "canonical_version": CANONICAL_MUTATION_VERSION,
            "action": "answer_decision",
            "target": {"worker_id": public_worker_id},
            "decision": {
                "decision_ref": request.params["decision_ref"],
                "selection": canonical_selection,
            },
            "options": {},
        }

    return CanonicalMutation(
        canonical_version=CANONICAL_MUTATION_VERSION,
        action=request.action,
        public_worker_id=public_worker_id,
        canonical_json=stable_json_dumps(canonical_payload),
        fingerprint=stable_fingerprint(canonical_payload),
    )


SELECTOR_PROOF_VERSION = 1
_SELECTOR_PROOF_DOMAIN = b"tendwire.command-selector-proof.v1"
_SELECTOR_PROOF_RE = re.compile(r"v1:[0-9a-f]{64}", re.ASCII)


def is_selector_proof(value: Any) -> bool:
    """Return whether a value is a supported, well-formed selector proof.

    Unknown proof versions and malformed digests are not supported here, so a
    caller can only fall back to a conservative decision instead of silently
    accepting evidence it cannot interpret.
    """
    return isinstance(value, str) and _SELECTOR_PROOF_RE.fullmatch(value) is not None


def build_selector_proof(request: CommandRequest) -> str:
    """Return the private, bounded proof of one request's immutable selector.

    The canonical mutation records which worker a request resolved to, not how
    the caller spelled that target. This proof records the spelling, so an exact
    retry of a name or space selector can be recognized after the resolved
    worker disappears from current authority.

    ``worker_fingerprint`` is deliberately excluded: it is a mutable observation
    precondition, not command identity, so refreshing it must not create a
    different command. Excluding it is only safe because a validated target
    always carries a stable selector (see TARGET_STABLE_SELECTOR_FIELDS) -- were
    a fingerprint-only target legal, every one of them would share this proof and
    a changed target could claim another worker's stored result.

    The proof is a fixed-width digest, so it is bounded independently of
    untrusted input, carries no private binding or backend-routing data, and is
    never part of any public surface.
    """
    if not isinstance(request, CommandRequest):
        raise TypeError("request must be a CommandRequest")
    if request.action not in {"send_instruction", "answer_pending", "answer_decision"}:
        raise ValueError(
            "selector proofs require send_instruction, answer_pending, or answer_decision"
        )
    if request.dry_run is not False:
        raise ValueError("selector proofs require a non-dry-run request")
    request_error = validate_request(request)
    if request_error is not None:
        raise ValueError(str(request_error.get("message") or "invalid command request"))

    target = request.target
    if target is None:
        selector: dict[str, Any] = {"shape": "none"}
    else:
        # Mirror resolve_target's accepted value semantics exactly, so a stored
        # proof and a live resolution can never disagree about what a selector
        # says. A missing space_id is null and an empty one is "", because
        # resolution treats those as different filters.
        selector = {
            "shape": "target",
            "worker_id": _string_value(target.get("worker_id")),
            "name": _string_value(target.get("name")),
            "space_id": _optional_string(target.get("space_id")),
        }
    payload = {
        "proof_version": SELECTOR_PROOF_VERSION,
        "action": request.action,
        "selector": selector,
    }
    digest = hashlib.sha256(
        _SELECTOR_PROOF_DOMAIN
        + b"\x00"
        + json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return f"v{SELECTOR_PROOF_VERSION}:{digest}"


def validate_request(request: CommandRequest) -> dict[str, Any] | None:
    """Validate a command request; return an error dict or None if valid."""
    if (
        isinstance(request.schema_version, bool)
        or not isinstance(request.schema_version, int)
        or request.schema_version != COMMAND_REQUEST_SCHEMA_VERSION
    ):
        return error_value(
            STATUS_INVALID_REQUEST,
            f"schema_version must be {COMMAND_REQUEST_SCHEMA_VERSION}",
            details={"field": "schema_version"},
        )

    if not request.action:
        return error_value(STATUS_INVALID_REQUEST, "action is required", details={"field": "action"})

    if not isinstance(request.dry_run, bool):
        return error_value(
            STATUS_INVALID_REQUEST,
            "dry_run must be a JSON boolean",
            details={"field": "dry_run"},
        )

    if (
        isinstance(request.response_schema_version, bool)
        or not isinstance(request.response_schema_version, int)
        or request.response_schema_version
        not in SUPPORTED_COMMAND_ENVELOPE_SCHEMA_VERSIONS
    ):
        return error_value(
            STATUS_INVALID_REQUEST,
            "response_schema_version must be 2 or 3",
            details={"field": "response_schema_version"},
        )

    if request.action not in ALLOWED_ACTIONS:
        return error_value(
            STATUS_REJECTED,
            f"unknown action {request.action!r}",
            details={"field": "action", "allowed": sorted(ALLOWED_ACTIONS)},
        )
    if (
        request.action in {"send_instruction", "answer_pending", "answer_decision"}
        and request.dry_run is False
        and not is_valid_request_id(request.request_id)
    ):
        return error_value(
            STATUS_INVALID_REQUEST,
            f"non-dry-run {request.action} requires a valid request_id",
            details={"field": "request_id"},
        )

    forbidden = _find_forbidden_fields(request.to_dict())
    if forbidden:
        return error_value(
            STATUS_INVALID_REQUEST,
            "request contains forbidden connector or terminal fields",
            details={"fields": forbidden},
        )

    target_err = _validate_target_shape(request.target)
    if target_err:
        return target_err

    instruction_err = _validate_instruction_shape(request.instruction)
    if instruction_err:
        return instruction_err

    if request.action == "send_instruction":
        if request.target is None:
            return error_value(
                STATUS_INVALID_REQUEST,
                "send_instruction requires a target",
                details={"field": "target"},
            )
        if not _target_has_stable_selector(request.target):
            return error_value(
                STATUS_INVALID_REQUEST,
                "send_instruction requires at least one stable target selector",
                details={
                    "field": "target",
                    "allowed": sorted(TARGET_STABLE_SELECTOR_FIELDS),
                },
            )
        if request.instruction is None or not _string_value(request.instruction.get("text")):
            return error_value(
                STATUS_INVALID_REQUEST,
                "send_instruction requires instruction.text",
                details={"field": "instruction.text"},
            )

    if request.action == "answer_pending":
        if request.target is not None:
            return error_value(
                STATUS_INVALID_REQUEST,
                "answer_pending does not accept a target",
                details={"field": "target"},
            )
        if request.instruction is not None:
            return error_value(
                STATUS_INVALID_REQUEST,
                "answer_pending does not accept an instruction",
                details={"field": "instruction"},
            )
        if not isinstance(request.params, dict):
            return error_value(
                STATUS_INVALID_REQUEST,
                "answer_pending requires params",
                details={"field": "params"},
            )
        actual_fields = set(request.params)
        if actual_fields != ANSWER_PENDING_PARAM_FIELDS:
            return error_value(
                STATUS_INVALID_REQUEST,
                "answer_pending params must contain exactly pending_id, pending_fingerprint, and choice_id",
                details={
                    "field": "params",
                    "required": sorted(ANSWER_PENDING_PARAM_FIELDS),
                    "missing": sorted(ANSWER_PENDING_PARAM_FIELDS - actual_fields),
                    "disallowed": sorted(actual_fields - ANSWER_PENDING_PARAM_FIELDS),
                },
            )
        for field in sorted(ANSWER_PENDING_PARAM_FIELDS):
            value = request.params.get(field)
            if not isinstance(value, str) or not value.strip():
                return error_value(
                    STATUS_INVALID_REQUEST,
                    f"answer_pending requires nonblank params.{field}",
                    details={"field": f"params.{field}"},
                )

    if request.action == "answer_decision":
        if request.target is None or set(request.target) != {"worker_id"}:
            return error_value(
                STATUS_INVALID_REQUEST,
                "answer_decision requires exactly target.worker_id",
                details={"field": "target"},
            )
        worker_id = request.target.get("worker_id")
        if not isinstance(worker_id, str) or not worker_id.strip():
            return error_value(
                STATUS_INVALID_REQUEST,
                "answer_decision requires nonblank target.worker_id",
                details={"field": "target.worker_id"},
            )
        if request.instruction is not None:
            return error_value(
                STATUS_INVALID_REQUEST,
                "answer_decision does not accept an instruction",
                details={"field": "instruction"},
            )
        if not isinstance(request.params, dict) or set(request.params) != ANSWER_DECISION_PARAM_FIELDS:
            return error_value(
                STATUS_INVALID_REQUEST,
                "answer_decision params must contain exactly decision_ref and selection",
                details={"field": "params"},
            )
        decision_ref = request.params.get("decision_ref")
        if not isinstance(decision_ref, str) or not decision_ref.strip():
            return error_value(
                STATUS_INVALID_REQUEST,
                "answer_decision requires nonblank params.decision_ref",
                details={"field": "params.decision_ref"},
            )
        selection = request.params.get("selection")
        if not isinstance(selection, Mapping) or len(selection) != 1:
            return error_value(
                STATUS_INVALID_SELECTION,
                "selection must contain exactly one selection form",
                details={"field": "params.selection"},
            )
        if set(selection) == {"option_refs"}:
            option_refs = selection.get("option_refs")
            if not isinstance(option_refs, list) or not option_refs or any(
                not isinstance(ref, str) or not ref.strip() for ref in option_refs
            ):
                return error_value(
                    STATUS_INVALID_SELECTION,
                    "selection.option_refs must be a nonempty array of strings",
                    details={"field": "params.selection.option_refs"},
                )
        elif set(selection) == {"text"}:
            if validate_instruction_text(selection.get("text")) is not None:
                return error_value(
                    STATUS_INVALID_SELECTION,
                    "selection.text must be nonempty safe text",
                    details={"field": "params.selection.text"},
                )
        else:
            return error_value(
                STATUS_INVALID_SELECTION,
                "selection must contain option_refs or text",
                details={"field": "params.selection"},
            )

    return None


def parse_command_request(payload: str) -> tuple[CommandRequest | None, dict[str, Any] | None]:
    """Parse a JSON string into a CommandRequest or an error dict."""
    try:
        data = json.loads(payload)
    except Exception as exc:  # noqa: BLE001
        return None, error_value(
            STATUS_INVALID_REQUEST,
            f"invalid JSON: {exc}",
            details={"field": "request"},
        )
    if not isinstance(data, dict):
        return None, error_value(
            STATUS_INVALID_REQUEST,
            "request must be a JSON object",
            details={"field": "request"},
        )
    forbidden = _find_forbidden_fields(data)
    if forbidden:
        request = None
        top_level_forbidden = any(_is_forbidden_request_field(key) for key in data)
        if not top_level_forbidden:
            try:
                request = CommandRequest.from_dict(data)
            except Exception:
                request = None
        return request, error_value(
            STATUS_INVALID_REQUEST,
            "request contains forbidden connector or terminal fields",
            details={"fields": forbidden},
        )
    unknown = sorted(str(key) for key in data if str(key) not in REQUEST_ALLOWED_FIELDS)
    if unknown:
        return None, error_value(
            STATUS_INVALID_REQUEST,
            f"request contains unknown top-level fields: {unknown}",
            details={"fields": [f"$.{field}" for field in unknown]},
        )
    if "schema_version" not in data:
        return None, error_value(
            STATUS_INVALID_REQUEST,
            f"schema_version must be {COMMAND_REQUEST_SCHEMA_VERSION}",
            details={"field": "schema_version"},
        )
    schema_version = data.get("schema_version")
    if (
        isinstance(schema_version, bool)
        or not isinstance(schema_version, int)
        or schema_version != COMMAND_REQUEST_SCHEMA_VERSION
    ):
        return None, error_value(
            STATUS_INVALID_REQUEST,
            f"schema_version must be {COMMAND_REQUEST_SCHEMA_VERSION}",
            details={"field": "schema_version"},
        )
    if "dry_run" in data and not isinstance(data.get("dry_run"), bool):
        return None, error_value(
            STATUS_INVALID_REQUEST,
            "dry_run must be a JSON boolean",
            details={"field": "dry_run"},
        )
    if "response_schema_version" in data and (
        isinstance(data.get("response_schema_version"), bool)
        or not isinstance(data.get("response_schema_version"), int)
        or data.get("response_schema_version")
        not in SUPPORTED_COMMAND_ENVELOPE_SCHEMA_VERSIONS
    ):
        return None, error_value(
            STATUS_INVALID_REQUEST,
            "response_schema_version must be 2 or 3",
            details={"field": "response_schema_version"},
        )
    try:
        request = CommandRequest.from_dict(data)
    except Exception as exc:  # noqa: BLE001
        return None, error_value(
            STATUS_INVALID_REQUEST,
            f"request shape error: {exc}",
            details={"field": "request"},
        )
    return request, None


@dataclass(frozen=True)
class CommandEnvelope:
    """A strict public command result envelope."""

    ok: bool
    status: str
    action: str
    disposition: CommandDisposition = DISPOSITION_NO_RECEIPT
    request_id: str | None = None
    dry_run: bool = True
    result: dict[str, Any] | None = None
    error: dict[str, Any] | None = None
    warnings: list[str] = field(default_factory=list)
    schema_version: int = COMMAND_ENVELOPE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if not isinstance(self.ok, bool):
            raise TypeError("ok must be a boolean")
        if not isinstance(self.status, str) or self.status not in VALID_STATUSES:
            raise ValueError("status must be a supported command status")
        if not isinstance(self.action, str):
            raise TypeError("action must be a string")
        if self.disposition not in VALID_DISPOSITIONS:
            raise ValueError("disposition must be a supported command disposition")
        if self.request_id is not None and not isinstance(self.request_id, str):
            raise TypeError("request_id must be a string or null")
        if not isinstance(self.dry_run, bool):
            raise TypeError("dry_run must be a boolean")
        if self.result is not None and not isinstance(self.result, Mapping):
            raise TypeError("result must be an object or null")
        if self.error is not None and not isinstance(self.error, Mapping):
            raise TypeError("error must be an object or null")
        if not isinstance(self.warnings, list) or any(
            not isinstance(warning, str) for warning in self.warnings
        ):
            raise TypeError("warnings must be an array of strings")
        if (
            isinstance(self.schema_version, bool)
            or not isinstance(self.schema_version, int)
            or self.schema_version not in SUPPORTED_COMMAND_ENVELOPE_SCHEMA_VERSIONS
        ):
            raise ValueError(
                "command envelope schema_version must be 2 or 3"
            )

        mutating = self.action in {
            "send_instruction",
            "answer_pending",
            "answer_decision",
        }
        live_mutation = mutating and self.dry_run is False
        if live_mutation and not is_valid_request_id(self.request_id):
            raise ValueError("non-dry-run mutation requires a valid request_id")
        if (
            self.disposition != DISPOSITION_NO_RECEIPT
            and not is_valid_request_id(self.request_id)
        ):
            raise ValueError("receipt-bearing disposition requires a valid request_id")

        clean_result = _clean_public_mapping(self.result)
        clean_error = _clean_public_mapping(self.error)
        clean_warnings = [
            clean for warning in self.warnings if (clean := sanitize_public_text(warning))
        ]
        object.__setattr__(self, "result", clean_result)
        object.__setattr__(self, "error", clean_error)
        object.__setattr__(self, "warnings", clean_warnings)

        if self.ok and clean_error is not None:
            raise ValueError("successful command envelope must not include an error")
        if self.disposition == DISPOSITION_NO_RECEIPT:
            if live_mutation:
                valid_no_receipt_tuple = (
                    not self.ok
                    and self.status in LIVE_MUTATION_NO_RECEIPT_REJECTION_STATUSES
                    and (
                        self.status != STATUS_ANSWER_IN_PROGRESS
                        or self.action == "answer_decision"
                    )
                )
            elif mutating:
                valid_no_receipt_tuple = (
                    self.ok and self.status == STATUS_DRY_RUN
                ) or (
                    not self.ok
                    and self.status
                    in DRY_RUN_MUTATION_NO_RECEIPT_REJECTION_STATUSES
                )
            else:
                valid_no_receipt_tuple = True
            if not valid_no_receipt_tuple:
                raise ValueError("no_receipt disposition has an inconsistent command tuple")
        elif self.disposition == DISPOSITION_IN_PROGRESS:
            if (
                not mutating
                or self.dry_run
                or self.ok
                or self.status not in {STATUS_PENDING, STATUS_ANSWER_IN_PROGRESS}
                or (
                    self.status == STATUS_ANSWER_IN_PROGRESS
                    and self.action != "answer_decision"
                )
            ):
                raise ValueError("in_progress disposition has an inconsistent command tuple")
        elif self.disposition == DISPOSITION_TERMINAL_ACCEPTED:
            if not mutating or self.dry_run or not self.ok or self.status != STATUS_ACCEPTED:
                raise ValueError("terminal_accepted disposition has an inconsistent command tuple")
        elif self.disposition == DISPOSITION_TERMINAL_REJECTED:
            if (
                not mutating
                or self.dry_run
                or self.ok
                or self.status not in TERMINAL_MUTATION_REJECTION_STATUSES
            ):
                raise ValueError("terminal_rejected disposition has an inconsistent command tuple")
        elif self.disposition == DISPOSITION_TERMINAL_UNCERTAIN:
            if (
                not mutating
                or self.dry_run
                or self.ok
                or self.status != STATUS_REQUEST_STATE_UNCERTAIN
            ):
                raise ValueError("terminal_uncertain disposition has an inconsistent command tuple")
        if self.schema_version == COMMAND_ENVELOPE_V3_SCHEMA_VERSION:
            submission_id = (
                clean_result.get("submission_id")
                if isinstance(clean_result, Mapping)
                else None
            )
            turn_id = (
                clean_result.get("turn_id")
                if isinstance(clean_result, Mapping)
                else None
            )
            if (
                self.action != "send_instruction"
                or self.disposition != DISPOSITION_TERMINAL_ACCEPTED
                or not is_turn_submission_id(submission_id)
                or not isinstance(clean_result, Mapping)
                or "turn_id" not in clean_result
                or not (
                    turn_id is None
                    or (isinstance(turn_id, str) and bool(turn_id))
                )
            ):
                raise ValueError(
                    "schema-v3 envelopes require an accepted instruction submission"
                )
        if self.status == STATUS_ANSWER_IN_PROGRESS and not (
            self.action == "answer_decision"
            and live_mutation
            and not self.ok
            and self.disposition
            in {DISPOSITION_NO_RECEIPT, DISPOSITION_IN_PROGRESS}
        ):
            raise ValueError(
                "answer_in_progress has an inconsistent command tuple"
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "action": self.action,
            "request_id": self.request_id,
            "ok": self.ok,
            "dry_run": self.dry_run,
            "status": self.status,
            "disposition": self.disposition,
            "result": self.result,
            "error": self.error,
            "warnings": self.warnings,
        }

    def to_json(self, indent: int | None = None) -> str:
        return public_json_dumps(self.to_dict(), indent=indent)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CommandEnvelope":
        if not isinstance(data, dict):
            raise TypeError("command envelope must be an object")
        required_fields = {
            "schema_version",
            "action",
            "request_id",
            "ok",
            "dry_run",
            "status",
            "disposition",
            "result",
            "error",
            "warnings",
        }
        if set(data) != required_fields:
            raise ValueError("command envelope must contain exactly the schema fields")
        envelope = cls(
            ok=data["ok"],
            status=data["status"],
            action=data["action"],
            disposition=data["disposition"],
            request_id=data["request_id"],
            dry_run=data["dry_run"],
            result=data["result"],
            error=data["error"],
            warnings=data["warnings"],
            schema_version=data["schema_version"],
        )
        if envelope.to_dict() != data:
            raise ValueError("command envelope is not an exact public roundtrip")
        return envelope

    @classmethod
    def from_result(
        cls,
        request: CommandRequest,
        *,
        ok: bool,
        status: str,
        disposition: CommandDisposition = DISPOSITION_NO_RECEIPT,
        result: dict[str, Any] | None = None,
        error: dict[str, Any] | None = None,
        warnings: list[str] | None = None,
        schema_version: int = COMMAND_ENVELOPE_SCHEMA_VERSION,
    ) -> "CommandEnvelope":
        return cls(
            ok=ok,
            status=status,
            action=request.action,
            disposition=disposition,
            request_id=request.request_id,
            dry_run=request.dry_run,
            result=result,
            error=error,
            warnings=list(warnings or []),
            schema_version=schema_version,
        )

    @classmethod
    def from_error(cls, request: CommandRequest | None, error: dict[str, Any]) -> "CommandEnvelope":
        """Build a no-receipt rejection envelope from a partial or missing request."""
        if request is None:
            return cls(
                ok=False,
                status=error.get("code", STATUS_INVALID_REQUEST),
                action="",
                request_id=None,
                dry_run=True,
                result=None,
                error=error,
            )
        mutating = request.action in {
            "send_instruction",
            "answer_pending",
            "answer_decision",
        }
        valid_mutation_id = is_valid_request_id(request.request_id)
        request_id = (
            request.request_id
            if valid_mutation_id or (not mutating and isinstance(request.request_id, str))
            else None
        )
        dry_run = request.dry_run
        if mutating and dry_run is False and not valid_mutation_id:
            # An invalid request was rejected before it became a live mutation.
            # Do not emit a wire envelope that claims otherwise.
            dry_run = True
        return cls(
            ok=False,
            status=error.get("code", STATUS_REJECTED),
            action=request.action,
            request_id=request_id,
            dry_run=dry_run,
            result=None,
            error=error,
        )


def worker_candidate(worker: Worker, *, include_backend_target: bool = False) -> dict[str, Any]:
    """Return a sanitized neutral candidate description for a worker."""
    candidate: dict[str, Any] = {
        "worker_id": worker.id,
        "name": worker.name,
        "space_id": worker.space_id,
        "status": worker.status,
        "worker_fingerprint": worker.fingerprint,
    }
    if worker.summary:
        candidate["summary"] = worker.summary
    if include_backend_target and worker.backend_target:
        candidate["backend_target"] = dict(worker.backend_target)
    return candidate


def resolve_target(
    target: dict[str, Any] | None,
    workers: list[Worker],
    *,
    allow_disallowed_status: bool = False,
    include_backend_target: bool = False,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]], str]:
    """Resolve a target dict against live workers.

    Returns a tuple of (resolved_candidate, candidates, status). When status is
    STATUS_RESOLVED, resolved_candidate is set and candidates is a one-element
    list. For other statuses resolved_candidate is None and candidates contains
    zero or more sanitized candidates.
    """
    if target is None:
        return None, [], STATUS_NOT_FOUND

    worker_id = _string_value(target.get("worker_id"))
    name = _string_value(target.get("name"))
    space_id = _optional_string(target.get("space_id"))
    fingerprint = _optional_string(target.get("worker_fingerprint"))

    # First match by identity/name/space, excluding fingerprint.
    identity_matches: list[Worker] = []
    for worker in workers:
        if worker_id and worker.id != worker_id:
            continue
        if name and worker.name != name:
            continue
        if space_id is not None and worker.space_id != space_id:
            continue
        identity_matches.append(worker)

    # If a fingerprint was supplied, filter further. A non-empty identity match
    # that becomes empty due to fingerprint mismatch signals a stale target.
    if fingerprint is not None:
        fingerprint_matches = [w for w in identity_matches if w.fingerprint == fingerprint]
        if identity_matches and not fingerprint_matches:
            if len(identity_matches) == 1:
                return None, [worker_candidate(identity_matches[0])], STATUS_STALE_TARGET
            # Multiple identity matches with no fingerprint match is ambiguous.
            return None, [worker_candidate(w) for w in identity_matches], STATUS_AMBIGUOUS_TARGET
        candidates = fingerprint_matches
    else:
        candidates = identity_matches

    sanitized = [
        worker_candidate(worker, include_backend_target=include_backend_target)
        for worker in candidates
    ]

    if len(candidates) == 0:
        return None, [], STATUS_NOT_FOUND
    if len(candidates) > 1:
        return None, sanitized, STATUS_AMBIGUOUS_TARGET

    resolved = candidates[0]
    if not allow_disallowed_status and resolved.status in _DISALLOWED_WORKER_STATUSES:
        return None, sanitized, STATUS_REJECTED

    return sanitized[0], sanitized, STATUS_RESOLVED


def snapshot_result(snapshot: Snapshot) -> dict[str, Any]:
    """Return a neutral result payload for read_snapshot."""
    return {"snapshot": snapshot.to_dict()}
