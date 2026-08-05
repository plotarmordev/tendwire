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
    Worker,
    public_json_dumps,
    sanitize_public_mapping,
    sanitize_public_text,
    stable_fingerprint,
    stable_json_dumps,
    _optional_string,
    _string_value,
)


COMMAND_REQUEST_SCHEMA_VERSION = 1
COMMAND_ENVELOPE_SCHEMA_VERSION = 2
ALLOWED_ACTIONS = frozenset({"send_instruction", "answer_decision"})
REQUEST_ALLOWED_FIELDS = frozenset(
    {
        "schema_version",
        "action",
        "request_id",
        "target",
        "instruction",
        "params",
    }
)

# Canonical status values for command results/envelopes.
STATUS_ACCEPTED = "accepted"
STATUS_REJECTED = "rejected"
STATUS_NOT_FOUND = "not_found"
STATUS_AMBIGUOUS_TARGET = "ambiguous_target"
STATUS_STALE_TARGET = "stale_target"
STATUS_BACKEND_UNAVAILABLE = "backend_unavailable"
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
    {DISPOSITION_NO_RECEIPT, DISPOSITION_IN_PROGRESS, DISPOSITION_TERMINAL_ACCEPTED,
     DISPOSITION_TERMINAL_REJECTED, DISPOSITION_TERMINAL_UNCERTAIN}
)

VALID_STATUSES = frozenset(
    {STATUS_ACCEPTED, STATUS_REJECTED, STATUS_NOT_FOUND, STATUS_AMBIGUOUS_TARGET,
     STATUS_STALE_TARGET, STATUS_BACKEND_UNAVAILABLE,
     STATUS_DUPLICATE_REQUEST, STATUS_REQUEST_STATE_UNCERTAIN,
     STATUS_INVALID_REQUEST, STATUS_PENDING, STATUS_ANSWER_IN_PROGRESS,
     STATUS_DECISION_NOT_PENDING, STATUS_UNKNOWN_WORKER, STATUS_INVALID_SELECTION,
     STATUS_UNSUPPORTED_DECISION}
)

# Durable rejections are possible only after a live mutation has a canonical
# identity. Keep this explicit so a new neutral, success, pending, or uncertain
# status cannot silently become valid stored rejection evidence.
TERMINAL_MUTATION_REJECTION_STATUSES = frozenset(
    {STATUS_REJECTED, STATUS_STALE_TARGET, STATUS_BACKEND_UNAVAILABLE,
     STATUS_DUPLICATE_REQUEST, STATUS_DECISION_NOT_PENDING, STATUS_UNKNOWN_WORKER,
     STATUS_INVALID_SELECTION, STATUS_UNSUPPORTED_DECISION}
)

# A live no-receipt failure has no durable authority. These statuses cover
# validation/target failures and the intermediate pre-reservation envelopes
# used by the authoritative submission path before it terminalizes a failure.
LIVE_MUTATION_NO_RECEIPT_REJECTION_STATUSES = frozenset(
    TERMINAL_MUTATION_REJECTION_STATUSES - {STATUS_DUPLICATE_REQUEST}
    | {STATUS_INVALID_REQUEST, STATUS_NOT_FOUND, STATUS_AMBIGUOUS_TARGET,
       STATUS_ANSWER_IN_PROGRESS}
)

INSTRUCTION_ALLOWED_FIELDS = frozenset({"text"})
ANSWER_DECISION_PARAM_FIELDS = frozenset({"decision_ref", "selection"})

# Connector, low-level terminal, routing, and private fields rejected anywhere in a request.
FORBIDDEN_REQUEST_FIELDS = FORBIDDEN_FIELD_NAMES

# Conservative forbidden-field matching including common case and separator variants.
_FORBIDDEN_REQUEST_COMPACT = frozenset(name.replace("_", "") for name in FORBIDDEN_REQUEST_FIELDS)

MAX_INSTRUCTION_LENGTH = 4096
_REQUEST_ID_RE = re.compile(r"[A-Za-z0-9._-]{1,128}", re.ASCII)
_STABLE_WORKER_KEY_RE = re.compile(r"wsk1_[0-9a-f]{64}", re.ASCII)
_TURN_SUBMISSION_ID_RE = re.compile(r"twsub1\.[0-9a-f]{64}", re.ASCII)
_INSTRUCTION_FINGERPRINT_DOMAIN = b"tendwire.instruction-fingerprint.v1"
_TURN_SUBMISSION_ID_DOMAIN = b"tendwire.turn-submission-id.v1"
_EDGE_INSTRUCTION_CONTROLS = "".join(
    chr(code) for code in (*range(9), *range(11, 32), *range(127, 160))
)

# Workers that must not receive instructions.
_DISALLOWED_WORKER_STATUSES = frozenset({"closed", "failed", "unknown"})
_TARGET_RESOLVED = "resolved"


def _is_forbidden_request_field(key: Any) -> bool:
    normalized = str(key).lower().replace("-", "_").replace(".", "_")
    compact = normalized.replace("_", "")
    return normalized in FORBIDDEN_REQUEST_FIELDS or compact in _FORBIDDEN_REQUEST_COMPACT


def _clean_mapping(value: Any) -> dict[str, Any] | None:
    return None if value is None else (
        {str(k): v for k, v in value.items()} if isinstance(value, dict) else {}
    )


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
            "code": _string_value(code, STATUS_REJECTED), "message": _string_value(message),
            "details": details or {},
        }
    )


def _field_error(
    message: str, field_name: str, *, status: str = STATUS_INVALID_REQUEST, **details: Any,
) -> dict[str, Any]:
    return error_value(status, message, details={"field": field_name, **details})


def _domain_digest(domain: bytes, *parts: str) -> str:
    return hashlib.sha256(
        b"\x00".join((domain, *(part.encode("utf-8") for part in parts)))
    ).hexdigest()


def validate_instruction_text(text: Any) -> dict[str, Any] | None:
    """Validate instruction text and return an error dict, or None if valid."""
    basic_errors = (
        (text is None, "instruction.text is required"),
        (not isinstance(text, str), "instruction.text must be a string"),
        (text == "", "instruction.text must not be empty"),
    )
    for invalid, message in basic_errors:
        if invalid:
            return _field_error(message, "instruction.text")
    assert isinstance(text, str)
    if len(text) > MAX_INSTRUCTION_LENGTH:
        return _field_error(
            f"instruction.text exceeds maximum length of {MAX_INSTRUCTION_LENGTH}",
            "instruction.text",
        )
    unsafe = (
        ("\x00" in text, "instruction.text must not contain NUL"),
        ("\x1b[200~" in text or "\x1b[201~" in text,
         "instruction.text must not contain bracketed-paste sequences"),
        ("\x1b" in text, "instruction.text must not contain escape sequences"),
        (any(0x80 <= ord(char) <= 0x9F for char in text),
         "instruction.text must not contain C1 control characters"),
        (any((code := ord(char)) < 32 and code not in {9, 10} or code == 127 for char in text),
         "instruction.text must not contain raw control characters"),
    )
    for invalid, message in unsafe:
        if invalid:
            return _field_error(message, "instruction.text")
    return None


def instruction_fingerprint(text: Any) -> str:
    """Return an opaque, versioned digest suitable for ledger matching.

    Valid instruction text can consist entirely of whitespace even though its
    normalized form is empty. Preserve the normalized matching behavior for
    ordinary text, but fingerprint the raw text in that edge case so submission
    bookkeeping can never reject an otherwise valid send.
    """
    raw = str(text or "").strip(_EDGE_INSTRUCTION_CONTROLS)
    normalized = "\n".join(" ".join(line.split()) for line in raw.splitlines()).strip()
    fingerprint_text = normalized or str(text or "")
    digest = _domain_digest(_INSTRUCTION_FINGERPRINT_DOMAIN, fingerprint_text)
    return f"twins1.{digest}"


def turn_submission_id(host_id: Any, request_id: Any) -> str:
    """Return the deterministic opaque submission ID for one host/request."""
    clean_host_id = str(host_id or "").strip()
    clean_request_id = str(request_id or "").strip()
    if not clean_host_id or not clean_request_id:
        raise ValueError("turn submission identity fields must be non-empty")
    digest = _domain_digest(_TURN_SUBMISSION_ID_DOMAIN, clean_host_id, clean_request_id)
    return f"twsub1.{digest}"


def _validate_target_shape(target: dict[str, Any] | None) -> dict[str, Any] | None:
    if target is None:
        return None
    if not isinstance(target, dict):
        return _field_error("target must be an object", "target")
    shape = frozenset(target)
    allowed = {
        frozenset({"worker_id"}),
        frozenset({"worker_id", "worker_fingerprint"}),
        frozenset({"space_id"}),
        frozenset({"stable_key", "stable_key_version"}),
    }
    if shape not in allowed:
        return _field_error(
            "target selector shape is unsupported", "target",
            allowed=sorted(sorted(value) for value in allowed),
        )
    if shape == {"stable_key", "stable_key_version"}:
        stable_key = target.get("stable_key")
        if (
            not isinstance(stable_key, str)
            or _STABLE_WORKER_KEY_RE.fullmatch(stable_key) is None
            or type(target.get("stable_key_version")) is not int
            or target.get("stable_key_version") != 1
        ):
            return _field_error(
                "target stable worker identity is invalid or unsupported", "target"
            )
        return None
    for field in shape:
        if not isinstance(target.get(field), str) or not target[field].strip():
            return _field_error(f"target.{field} must be nonblank", f"target.{field}")
    return None

def is_valid_request_id(value: Any) -> bool:
    """Return whether value is an exact command request-ID token.

    IDs are opaque ASCII and are never trimmed, normalized, or case-folded.
    """
    return isinstance(value, str) and _REQUEST_ID_RE.fullmatch(value) is not None


def _validate_instruction_shape(instruction: dict[str, Any] | None) -> dict[str, Any] | None:
    if instruction is None:
        return None
    if not isinstance(instruction, dict):
        return _field_error("instruction must be an object", "instruction")
    extra = set(instruction.keys()) - INSTRUCTION_ALLOWED_FIELDS
    if extra:
        return _field_error(
            f"instruction contains disallowed fields: {sorted(extra)}", "instruction",
            disallowed=sorted(extra),
        )
    return validate_instruction_text(instruction.get("text"))


def _request_header_error(
    schema_version: Any,
) -> dict[str, Any] | None:
    if type(schema_version) is not int or schema_version != COMMAND_REQUEST_SCHEMA_VERSION:
        return _field_error(
            f"schema_version must be {COMMAND_REQUEST_SCHEMA_VERSION}", "schema_version"
        )
    return None


@dataclass(frozen=True)
class CommandRequest:
    """A neutral, validated command request."""

    action: str
    schema_version: int = COMMAND_REQUEST_SCHEMA_VERSION
    request_id: str | None = None
    target: dict[str, Any] | None = None
    instruction: dict[str, Any] | None = None
    params: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "action", _string_value(self.action))
        object.__setattr__(self, "target", _clean_mapping(self.target))
        object.__setattr__(self, "instruction", _clean_mapping(self.instruction))
        object.__setattr__(self, "params", _clean_mapping(self.params))

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "schema_version": self.schema_version, "action": self.action,
            "request_id": self.request_id, "target": self.target,
        }
        if self.instruction is not None:
            payload["instruction"] = self.instruction
        if self.params is not None:
            payload["params"] = self.params
        return payload

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CommandRequest":
        return cls(
            action=data.get("action", ""), request_id=data.get("request_id"),
            schema_version=data.get("schema_version", COMMAND_REQUEST_SCHEMA_VERSION),
            target=data.get("target"), instruction=data.get("instruction"),
            params=data.get("params"),
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


def _validated_mutation(request: CommandRequest, purpose: str) -> None:
    if not isinstance(request, CommandRequest):
        raise TypeError("request must be a CommandRequest")
    if request.action not in {"send_instruction", "answer_decision"}:
        raise ValueError(f"{purpose} require send_instruction or answer_decision")
    request_error = validate_request(request)
    if request_error is not None:
        raise ValueError(str(request_error.get("message") or "invalid command request"))


def build_canonical_mutation(
    request: CommandRequest,
    *,
    public_worker_id: str,
) -> CanonicalMutation:
    """Build canonical v1 identity after authoritative public-worker resolution.

    Request IDs, unresolved selectors, worker observation fingerprints,
    connector origin metadata, and private binding data are
    intentionally outside this representation.
    """
    _validated_mutation(request, "canonical mutations")
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
        canonical_version=CANONICAL_MUTATION_VERSION, action=request.action,
        public_worker_id=public_worker_id, canonical_json=stable_json_dumps(canonical_payload),
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
    always carries an exact stable selector shape -- were a fingerprint-only
    target legal, every one of them would share this proof and
    a changed target could claim another worker's stored result.

    The proof is a fixed-width digest, so it is bounded independently of
    untrusted input, carries no private binding or backend-routing data, and is
    never part of any public surface.
    """
    _validated_mutation(request, "selector proofs")
    assert request.target is not None
    target = request.target
    selector = {
        "shape": "target",
        "worker_id": _string_value(target.get("worker_id")),
        # Frozen v1 proof shape: name selectors are no longer accepted, but
        # this empty slot preserves existing space/stable-key receipt proofs.
        "name": "",
        "space_id": _optional_string(target.get("space_id")),
        "stable_key": _string_value(target.get("stable_key")),
        "stable_key_version": target.get("stable_key_version"),
    }
    payload = {
        "proof_version": SELECTOR_PROOF_VERSION,
        "action": request.action,
        "selector": selector,
    }
    digest = _domain_digest(_SELECTOR_PROOF_DOMAIN, stable_json_dumps(payload))
    return f"v{SELECTOR_PROOF_VERSION}:{digest}"


def validate_request(request: CommandRequest) -> dict[str, Any] | None:
    """Validate one live ACP command request."""
    header_error = _request_header_error(request.schema_version)
    if header_error is not None:
        return header_error
    if not request.action:
        return _field_error("action is required", "action")
    if request.action not in ALLOWED_ACTIONS:
        return _field_error(
            f"unknown action {request.action!r}", "action", status=STATUS_REJECTED,
            allowed=sorted(ALLOWED_ACTIONS),
        )
    if not is_valid_request_id(request.request_id):
        return _field_error(
            f"{request.action} requires a valid request_id", "request_id"
        )
    forbidden = _find_forbidden_fields(request.to_dict())
    if forbidden:
        return error_value(
            STATUS_INVALID_REQUEST,
            "request contains forbidden connector or terminal fields",
            details={"fields": forbidden},
        )
    target_error = _validate_target_shape(request.target)
    if target_error is not None:
        return target_error
    instruction_error = _validate_instruction_shape(request.instruction)
    if instruction_error is not None:
        return instruction_error

    if request.action == "send_instruction":
        requirements = (
            (request.target is None, "send_instruction requires a target", "target"),
            (request.instruction is None or not _string_value(request.instruction.get("text")),
             "send_instruction requires instruction.text", "instruction.text"),
            (request.params is not None, "send_instruction does not accept params", "params"),
        )
        for invalid, message, field_name in requirements:
            if invalid:
                return _field_error(message, field_name)
        return None

    target, params = request.target or {}, request.params or {}
    worker_id, decision_ref = target.get("worker_id"), params.get("decision_ref")
    requirements = (
        (set(target) != {"worker_id"}, "answer_decision requires exactly target.worker_id", "target"),
        (not isinstance(worker_id, str) or not worker_id.strip(),
         "answer_decision requires nonblank target.worker_id", "target.worker_id"),
        (request.instruction is not None, "answer_decision does not accept instruction", "instruction"),
        (set(params) != ANSWER_DECISION_PARAM_FIELDS,
         "answer_decision params must contain exactly decision_ref and selection", "params"),
        (not isinstance(decision_ref, str) or not decision_ref.strip(),
         "answer_decision requires nonblank params.decision_ref", "params.decision_ref"),
    )
    for invalid, message, field_name in requirements:
        if invalid:
            return _field_error(message, field_name)
    selection = params.get("selection")
    if not isinstance(selection, Mapping) or len(selection) != 1:
        return _field_error(
            "selection must contain exactly one selection form", "params.selection",
            status=STATUS_INVALID_SELECTION,
        )
    if set(selection) == {"option_refs"}:
        option_refs = selection.get("option_refs")
        invalid = not isinstance(option_refs, list) or not option_refs or any(
            not isinstance(ref, str) or not ref.strip() for ref in option_refs
        ) or len(set(option_refs)) != len(option_refs)
        message, field_name = (
            "selection.option_refs must be a nonempty array of strings",
            "params.selection.option_refs",
        )
    elif set(selection) == {"text"}:
        invalid = validate_instruction_text(selection.get("text")) is not None
        message, field_name = (
            "selection.text must be nonempty safe text",
            "params.selection.text",
        )
    else:
        return _field_error(
            "selection must contain option_refs or text", "params.selection",
            status=STATUS_INVALID_SELECTION,
        )
    return (
        _field_error(
            message, field_name, status=STATUS_INVALID_SELECTION
        )
        if invalid else None
    )

def parse_command_request(payload: str) -> tuple[CommandRequest | None, dict[str, Any] | None]:
    """Parse one strict fixed-protocol command request."""
    try:
        data = json.loads(payload)
    except Exception as exc:  # noqa: BLE001
        return None, _field_error(f"invalid JSON: {exc}", "request")
    if not isinstance(data, dict):
        return None, _field_error("request must be a JSON object", "request")
    forbidden = _find_forbidden_fields(data)
    if forbidden:
        return None, error_value(
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
    header_error = _request_header_error(data.get("schema_version"))
    if header_error is not None:
        return None, header_error
    action = data.get("action")
    expected = {
        "send_instruction": {
            "schema_version", "action", "request_id", "target", "instruction",
        },
        "answer_decision": {
            "schema_version", "action", "request_id", "target", "params",
        },
    }.get(action)
    if expected is not None and set(data) != expected:
        return None, _field_error(
            f"{action} requires exactly {sorted(expected)}", "request",
            missing=sorted(expected - set(data)),
            unexpected=sorted(set(data) - expected),
        )
    try:
        return CommandRequest.from_dict(data), None
    except Exception as exc:  # noqa: BLE001
        return None, _field_error(f"request shape error: {exc}", "request")


@dataclass(frozen=True)
class CommandEnvelope:
    """A strict public command result envelope."""

    ok: bool
    status: str
    action: str
    disposition: CommandDisposition = DISPOSITION_NO_RECEIPT
    request_id: str | None = None
    dry_run: Literal[False] = False
    result: dict[str, Any] | None = None
    error: dict[str, Any] | None = None
    warnings: list[str] = field(default_factory=list)
    schema_version: int = COMMAND_ENVELOPE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if not isinstance(self.ok, bool):
            raise TypeError("ok must be a boolean")
        if not isinstance(self.action, str):
            raise TypeError("action must be a string")
        if self.request_id is not None and not isinstance(self.request_id, str):
            raise TypeError("request_id must be a string or null")
        if self.dry_run is not False:
            raise ValueError("dry_run must be false")
        if self.result is not None and not isinstance(self.result, Mapping):
            raise TypeError("result must be an object or null")
        if self.error is not None and not isinstance(self.error, Mapping):
            raise TypeError("error must be an object or null")
        if not isinstance(self.warnings, list) or not all(
            isinstance(warning, str) for warning in self.warnings
        ):
            raise TypeError("warnings must be an array of strings")
        if not isinstance(self.status, str) or self.status not in VALID_STATUSES:
            raise ValueError("status must be a supported command status")
        if self.disposition not in VALID_DISPOSITIONS:
            raise ValueError("disposition must be a supported command disposition")
        if type(self.schema_version) is not int or self.schema_version != 2:
            raise ValueError("command envelope schema_version must be 2")

        mutating = self.action in ALLOWED_ACTIONS
        if self.disposition != DISPOSITION_NO_RECEIPT and not is_valid_request_id(
            self.request_id
        ):
            raise ValueError("receipt-bearing disposition requires a valid request_id")

        clean_result = None if self.result is None else sanitize_public_mapping(self.result)
        clean_error = None if self.error is None else sanitize_public_mapping(self.error)
        clean_warnings = [
            clean for warning in self.warnings if (clean := sanitize_public_text(warning))
        ]
        object.__setattr__(self, "result", clean_result)
        object.__setattr__(self, "error", clean_error)
        object.__setattr__(self, "warnings", clean_warnings)

        if self.ok and clean_error is not None:
            raise ValueError("successful command envelope must not include an error")
        if not _valid_envelope_tuple(self, mutating):
            raise ValueError(f"{self.disposition} disposition has an inconsistent command tuple")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version, "action": self.action,
            "request_id": self.request_id, "ok": self.ok, "dry_run": self.dry_run,
            "status": self.status, "disposition": self.disposition,
            "result": self.result, "error": self.error, "warnings": self.warnings,
        }

    def to_json(self, indent: int | None = None) -> str:
        return public_json_dumps(self.to_dict(), indent=indent)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CommandEnvelope":
        if not isinstance(data, dict):
            raise TypeError("command envelope must be an object")
        required_fields = {
            "schema_version", "action", "request_id", "ok", "dry_run", "status",
            "disposition", "result", "error", "warnings",
        }
        if set(data) != required_fields:
            raise ValueError("command envelope must contain exactly the schema fields")
        envelope = cls(**data)
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
    ) -> "CommandEnvelope":
        return cls(
            ok=ok, status=status, action=request.action, disposition=disposition,
            request_id=request.request_id, result=result, error=error,
            warnings=list(warnings or []),
        )

    @classmethod
    def from_error(cls, request: CommandRequest | None, error: dict[str, Any]) -> "CommandEnvelope":
        """Build a no-receipt rejection envelope from a partial or missing request."""
        if request is None or request.action not in ALLOWED_ACTIONS:
            action, request_id = "", None
            error = {**error, "code": STATUS_INVALID_REQUEST}
        else:
            action = request.action
            request_id = request.request_id if is_valid_request_id(request.request_id) else None
        return cls(
            ok=False, status=error.get("code", STATUS_REJECTED), action=action,
            request_id=request_id, error=error,
        )


def validate_public_command_envelope(
    envelope: CommandEnvelope,
    request: CommandRequest | None = None,
) -> CommandEnvelope:
    """Validate the fixed successful public result shape.

    Stored schema-2 receipts intentionally predate send-result enrichment and
    therefore continue to use :meth:`CommandEnvelope.from_dict` directly.
    Public command boundaries must additionally call this validator.
    """
    if envelope.disposition != DISPOSITION_TERMINAL_ACCEPTED:
        return envelope
    if request is None or validate_request(request) is not None:
        raise ValueError("accepted public command envelope requires its request")
    result = envelope.result or {}
    target = result.get("target")
    worker_id = target.get("worker_id") if isinstance(target, dict) else None
    requested_worker_id = (request.target or {}).get("worker_id")
    valid = (
        isinstance(envelope.result, dict)
        and isinstance(target, dict)
        and set(target) == {"worker_id"}
        and isinstance(worker_id, str)
        and bool(worker_id.strip())
        and (envelope.action, envelope.request_id)
        == (request.action, request.request_id)
        and (requested_worker_id is None or worker_id == requested_worker_id)
    )
    if envelope.action == "answer_decision":
        decision = result.get("decision")
        decision_ref = (
            decision.get("decision_ref") if isinstance(decision, dict) else None
        )
        valid = valid and set(result) == {
            "target", "decision", "delivery_state", "transport_state",
            "observed_pending_state",
        } and isinstance(decision, dict) and set(decision) == {"decision_ref"}
        valid = valid and (
            isinstance(decision_ref, str)
            and bool(decision_ref.strip())
            and decision_ref == (request.params or {}).get("decision_ref")
            and result.get("delivery_state") == "submitted"
            and result.get("transport_state") == "submitted"
            and result.get("observed_pending_state") == "pending_observation"
        )
    else:
        turn_id, submission_id = result.get("turn_id"), result.get("submission_id")
        valid = valid and envelope.action == "send_instruction" and set(result) == {
            "target", "delivery_state", "transport_state", "target_state_at_send",
            "turn_id", "observed_turn_state", "submission_verdict", "submission_id",
        }
        valid = valid and (
            result.get("delivery_state") == "submitted"
            and result.get("transport_state") == "submitted"
            and isinstance(result.get("target_state_at_send"), str)
            and bool(result["target_state_at_send"].strip())
            and (turn_id is None or isinstance(turn_id, str) and bool(turn_id.strip()))
            and result.get("observed_turn_state") in {
                "pending_observation", "observed", "complete",
            }
            and result.get("submission_verdict") == "submitted"
            and isinstance(submission_id, str)
            and _TURN_SUBMISSION_ID_RE.fullmatch(submission_id) is not None
        )
    if not valid:
        raise ValueError("accepted command result violates the fixed public schema")
    return envelope


def _valid_envelope_tuple(
    envelope: CommandEnvelope,
    mutating: bool,
) -> bool:
    if not mutating:
        return (
            envelope.action == ""
            and envelope.disposition == DISPOSITION_NO_RECEIPT
            and not envelope.ok
            and envelope.status == STATUS_INVALID_REQUEST
            and envelope.request_id is None
        )
    answer_progress = envelope.status != STATUS_ANSWER_IN_PROGRESS or (
        envelope.action == "answer_decision" and not envelope.ok
    )
    if not answer_progress:
        return False
    expected_ok, statuses = {
        DISPOSITION_NO_RECEIPT: (
            False, LIVE_MUTATION_NO_RECEIPT_REJECTION_STATUSES,
        ),
        DISPOSITION_IN_PROGRESS: (
            False, {STATUS_PENDING, STATUS_ANSWER_IN_PROGRESS},
        ),
        DISPOSITION_TERMINAL_ACCEPTED: (True, {STATUS_ACCEPTED}),
        DISPOSITION_TERMINAL_REJECTED: (
            False, TERMINAL_MUTATION_REJECTION_STATUSES,
        ),
        DISPOSITION_TERMINAL_UNCERTAIN: (
            False, {STATUS_REQUEST_STATE_UNCERTAIN},
        ),
    }[envelope.disposition]
    return envelope.ok is expected_ok and envelope.status in statuses


def worker_candidate(worker: Worker) -> dict[str, Any]:
    """Return a sanitized neutral candidate description for a worker."""
    candidate: dict[str, Any] = {
        "worker_id": worker.id, "name": worker.name, "space_id": worker.space_id,
        "status": worker.status, "worker_fingerprint": worker.fingerprint,
    }
    if worker.summary:
        candidate["summary"] = worker.summary
    return candidate


def resolve_target(
    target: dict[str, Any] | None,
    workers: list[Worker],
    *,
    allow_disallowed_status: bool = False,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]], str]:
    """Resolve a target dict against live workers.

    Returns a tuple of (resolved_candidate, candidates, status). When status is
    ``resolved``, resolved_candidate is set and candidates is a one-element
    list. For other statuses resolved_candidate is None and candidates contains
    zero or more sanitized candidates.
    """
    if target is None:
        return None, [], STATUS_NOT_FOUND

    worker_id = _string_value(target.get("worker_id"))
    space_id = _optional_string(target.get("space_id"))
    fingerprint = _optional_string(target.get("worker_fingerprint"))
    stable_key = _string_value(target.get("stable_key"))
    stable_key_version = target.get("stable_key_version")

    identity_matches = [worker for worker in workers if (
        (not worker_id or worker.id == worker_id)
        and (space_id is None or worker.space_id == space_id)
        and (not stable_key or (
            worker.meta.get("stable_key") == stable_key
            and worker.meta.get("stable_key_version") == stable_key_version
        ))
    )]

    # If a fingerprint was supplied, filter further. A non-empty identity match
    # that becomes empty due to fingerprint mismatch signals a stale target.
    if fingerprint is not None:
        fingerprint_matches = [w for w in identity_matches if w.fingerprint == fingerprint]
        if identity_matches and not fingerprint_matches:
            status = STATUS_STALE_TARGET if len(identity_matches) == 1 else STATUS_AMBIGUOUS_TARGET
            return None, [worker_candidate(w) for w in identity_matches], status
        candidates = fingerprint_matches
    else:
        candidates = identity_matches

    sanitized = [worker_candidate(worker) for worker in candidates]

    if not candidates:
        return None, [], STATUS_NOT_FOUND
    if len(candidates) > 1:
        return None, sanitized, STATUS_AMBIGUOUS_TARGET

    resolved = candidates[0]
    if not allow_disallowed_status and resolved.status in _DISALLOWED_WORKER_STATUSES:
        return None, sanitized, STATUS_REJECTED

    return sanitized[0], sanitized, _TARGET_RESOLVED
