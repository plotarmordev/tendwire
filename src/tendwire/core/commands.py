"""Fixed two-action command protocol and public command envelopes."""

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
    _is_forbidden_field_name,
    _optional_string,
    _string_value,
    public_json_dumps,
    sanitize_public_mapping,
    sanitize_public_text,
    stable_fingerprint,
    stable_json_dumps,
)


COMMAND_REQUEST_SCHEMA_VERSION = 1
COMMAND_ENVELOPE_SCHEMA_VERSION = 2
CANONICAL_MUTATION_VERSION = 1
SELECTOR_PROOF_VERSION = 1
ALLOWED_ACTIONS = frozenset({"send_instruction", "answer_decision"})

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
    "no_receipt", "in_progress", "terminal_accepted", "terminal_rejected", "terminal_uncertain"]
DISPOSITION_NO_RECEIPT: CommandDisposition = "no_receipt"
DISPOSITION_IN_PROGRESS: CommandDisposition = "in_progress"
DISPOSITION_TERMINAL_ACCEPTED: CommandDisposition = "terminal_accepted"
DISPOSITION_TERMINAL_REJECTED: CommandDisposition = "terminal_rejected"
DISPOSITION_TERMINAL_UNCERTAIN: CommandDisposition = "terminal_uncertain"

TERMINAL_MUTATION_REJECTION_STATUSES = frozenset({
    STATUS_REJECTED, STATUS_STALE_TARGET, STATUS_BACKEND_UNAVAILABLE,
    STATUS_DUPLICATE_REQUEST, STATUS_DECISION_NOT_PENDING, STATUS_UNKNOWN_WORKER,
    STATUS_INVALID_SELECTION, STATUS_UNSUPPORTED_DECISION})
LIVE_MUTATION_NO_RECEIPT_REJECTION_STATUSES = frozenset(
    TERMINAL_MUTATION_REJECTION_STATUSES - {STATUS_DUPLICATE_REQUEST}
    | {STATUS_INVALID_REQUEST, STATUS_NOT_FOUND, STATUS_AMBIGUOUS_TARGET,
       STATUS_ANSWER_IN_PROGRESS})
_DISPOSITION_RULES = {
    DISPOSITION_NO_RECEIPT: (False, LIVE_MUTATION_NO_RECEIPT_REJECTION_STATUSES),
    DISPOSITION_IN_PROGRESS: (False, {STATUS_PENDING, STATUS_ANSWER_IN_PROGRESS}),
    DISPOSITION_TERMINAL_ACCEPTED: (True, {STATUS_ACCEPTED}),
    DISPOSITION_TERMINAL_REJECTED: (False, TERMINAL_MUTATION_REJECTION_STATUSES),
    DISPOSITION_TERMINAL_UNCERTAIN: (False, {STATUS_REQUEST_STATE_UNCERTAIN}),
}
VALID_DISPOSITIONS = frozenset(_DISPOSITION_RULES)
VALID_STATUSES = frozenset().union(
    *(statuses for _ok, statuses in _DISPOSITION_RULES.values()), {STATUS_INVALID_REQUEST})

MAX_INSTRUCTION_LENGTH = 4096
FORBIDDEN_REQUEST_FIELDS = FORBIDDEN_FIELD_NAMES
INSTRUCTION_ALLOWED_FIELDS = frozenset({"text"})
ANSWER_DECISION_PARAM_FIELDS = frozenset({"decision_ref", "selection"})
_REQUEST_FIELDS = {
    "send_instruction": {"schema_version", "action", "request_id", "target", "instruction"},
    "answer_decision": {"schema_version", "action", "request_id", "target", "params"},
}
REQUEST_ALLOWED_FIELDS = frozenset().union(*_REQUEST_FIELDS.values())
_TARGET_SHAPES = {
    frozenset({"worker_id"}), frozenset({"worker_id", "worker_fingerprint"}),
    frozenset({"space_id"}), frozenset({"stable_key", "stable_key_version"})}
_EDGE_CONTROLS = "".join(chr(code) for code in (*range(9), *range(11, 32), *range(127, 160)))


def _clean_mapping(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    return {str(key): item for key, item in value.items()} if isinstance(value, dict) else {}


def _forbidden_paths(value: Any, path: str = "$") -> list[str]:
    found: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            if _is_forbidden_field_name(key):
                found.append(f"{path}.{key}")
            found.extend(_forbidden_paths(item, f"{path}.{key}"))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            found.extend(_forbidden_paths(item, f"{path}[{index}]"))
    return found


def error_value(
    code: str, message: str, *, details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    value = {"code": _string_value(code, STATUS_REJECTED),
             "message": _string_value(message), "details": details or {}}
    return sanitize_public_mapping(value)


def _field_error(message: str, field_name: str, *, status: str = STATUS_INVALID_REQUEST,
                 **details: Any) -> dict[str, Any]:
    return error_value(status, message, details={"field": field_name, **details})


def _digest(domain: bytes, *parts: str) -> str:
    return hashlib.sha256(b"\x00".join((domain, *(part.encode() for part in parts)))).hexdigest()


def is_valid_request_id(value: Any) -> bool:
    return isinstance(value, str) and re.fullmatch(
        r"[A-Za-z0-9._-]{1,128}", value, re.ASCII) is not None


def _nonblank(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def validate_instruction_text(text: Any) -> dict[str, Any] | None:
    if text is None:
        return _field_error("instruction.text is required", "instruction.text")
    if not isinstance(text, str):
        return _field_error("instruction.text must be a string", "instruction.text")
    if text == "":
        return _field_error("instruction.text must not be empty", "instruction.text")
    if len(text) > MAX_INSTRUCTION_LENGTH:
        message = f"instruction.text exceeds maximum length of {MAX_INSTRUCTION_LENGTH}"
        return _field_error(message, "instruction.text")
    unsafe = (
        ("\x00" in text, "NUL"),
        ("\x1b[200~" in text or "\x1b[201~" in text, "bracketed-paste sequences"),
        ("\x1b" in text, "escape sequences"),
        (any(0x80 <= ord(char) <= 0x9F for char in text), "C1 control characters"),
        (any((n := ord(char)) < 32 and n not in {9, 10} or n == 127 for char in text),
         "raw control characters"),
    )
    reason = next((reason for invalid, reason in unsafe if invalid), None)
    return None if reason is None else _field_error(
        f"instruction.text must not contain {reason}", "instruction.text")


def instruction_fingerprint(text: Any) -> str:
    raw = str(text or "").strip(_EDGE_CONTROLS)
    normalized = "\n".join(" ".join(line.split()) for line in raw.splitlines()).strip()
    domain = b"tendwire.instruction-fingerprint.v1"
    return f"twins1.{_digest(domain, normalized or str(text or ''))}"


def turn_submission_id(host_id: Any, request_id: Any) -> str:
    host, request = str(host_id or "").strip(), str(request_id or "").strip()
    if not host or not request:
        raise ValueError("turn submission identity fields must be non-empty")
    return f"twsub1.{_digest(b'tendwire.turn-submission-id.v1', host, request)}"


def is_turn_submission_id(value: Any) -> bool:
    """Return whether value is an exact v1 durable submission identifier."""

    return (
        isinstance(value, str)
        and re.fullmatch(r"twsub1\.[0-9a-f]{64}", value, re.ASCII) is not None
    )


def acp_producer_turn_id(session_id: Any, producer_turn_id: Any) -> str:
    """Derive the exact public ACP turn ID for one producer submission."""

    session = str(session_id or "").strip()
    producer = str(producer_turn_id or "").strip()
    if not session or not producer:
        raise ValueError("ACP producer turn identity fields must be non-empty")
    return "acpt_" + stable_fingerprint({
        "source": "acp",
        "session": session,
        "producer_turn": producer,
    })


@dataclass(frozen=True)
class CommandRequest:
    action: str
    schema_version: int = COMMAND_REQUEST_SCHEMA_VERSION
    request_id: str | None = None
    target: dict[str, Any] | None = None
    instruction: dict[str, Any] | None = None
    params: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "action", _string_value(self.action))
        for name in ("target", "instruction", "params"):
            object.__setattr__(self, name, _clean_mapping(getattr(self, name)))

    def to_dict(self) -> dict[str, Any]:
        return {name: item for name, item in self.__dict__.items()
                if name not in {"instruction", "params"} or item is not None}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CommandRequest":
        names = ("request_id", "target", "instruction", "params")
        return cls(data.get("action", ""), data.get("schema_version", 1),
                   *(data.get(name) for name in names))


def _validate_target(target: dict[str, Any] | None) -> dict[str, Any] | None:
    if target is None:
        return None
    shape = frozenset(target)
    if shape not in _TARGET_SHAPES:
        return _field_error(
            "target selector shape is unsupported", "target",
            allowed=sorted(sorted(item) for item in _TARGET_SHAPES))
    if shape == {"stable_key", "stable_key_version"}:
        valid = (
            isinstance(target.get("stable_key"), str)
            and re.fullmatch(r"wsk1_[0-9a-f]{64}", target["stable_key"], re.ASCII) is not None
            and type(target.get("stable_key_version")) is int
            and target["stable_key_version"] == 1
        )
        return None if valid else _field_error(
            "target stable worker identity is invalid or unsupported", "target")
    for name in shape:
        if not _nonblank(target.get(name)):
            return _field_error(f"target.{name} must be nonblank", f"target.{name}")
    return None


def _validate_selection(selection: Any) -> dict[str, Any] | None:
    if not isinstance(selection, Mapping) or len(selection) != 1:
        return _field_error("selection must contain exactly one selection form",
                            "params.selection", status=STATUS_INVALID_SELECTION)
    if set(selection) == {"option_refs"}:
        refs = selection.get("option_refs")
        valid = (
            isinstance(refs, list) and bool(refs)
            and all(_nonblank(ref) for ref in refs)
            and len(set(refs)) == len(refs)
        )
        return None if valid else _field_error(
            "selection.option_refs must be a nonempty array of strings",
            "params.selection.option_refs", status=STATUS_INVALID_SELECTION)
    if set(selection) == {"text"}:
        return None if validate_instruction_text(selection.get("text")) is None else _field_error(
            "selection.text must be nonempty safe text", "params.selection.text",
            status=STATUS_INVALID_SELECTION)
    return _field_error(
        "selection must contain option_refs or text", "params.selection",
        status=STATUS_INVALID_SELECTION)


def validate_request(request: CommandRequest) -> dict[str, Any] | None:
    if type(request.schema_version) is not int or request.schema_version != 1:
        return _field_error("schema_version must be 1", "schema_version")
    if not request.action:
        return _field_error("action is required", "action")
    if request.action not in ALLOWED_ACTIONS:
        return _field_error(
            f"unknown action {request.action!r}", "action", status=STATUS_REJECTED,
            allowed=sorted(ALLOWED_ACTIONS))
    if not is_valid_request_id(request.request_id):
        return _field_error(f"{request.action} requires a valid request_id", "request_id")
    if paths := _forbidden_paths(request.to_dict()):
        return error_value(
            STATUS_INVALID_REQUEST, "request contains forbidden connector or terminal fields",
            details={"fields": paths})
    if error := _validate_target(request.target):
        return error
    if request.instruction is not None:
        if extra := sorted(set(request.instruction) - INSTRUCTION_ALLOWED_FIELDS):
            return _field_error(
                f"instruction contains disallowed fields: {extra}",
                "instruction", disallowed=extra)
        if error := validate_instruction_text(request.instruction.get("text")):
            return error
    if request.action == "send_instruction":
        if request.target is None:
            return _field_error("send_instruction requires a target", "target")
        if request.instruction is None:
            return _field_error("send_instruction requires instruction.text", "instruction.text")
        if request.params is not None:
            return _field_error("send_instruction does not accept params", "params")
        return None
    target, params = request.target or {}, request.params or {}
    if set(target) != {"worker_id"}:
        return _field_error("answer_decision requires exactly target.worker_id", "target")
    if request.instruction is not None:
        return _field_error("answer_decision does not accept instruction", "instruction")
    if set(params) != ANSWER_DECISION_PARAM_FIELDS:
        return _field_error(
            "answer_decision params must contain exactly decision_ref and selection", "params")
    if not _nonblank(params.get("decision_ref")):
        return _field_error(
            "answer_decision requires nonblank params.decision_ref", "params.decision_ref")
    return _validate_selection(params.get("selection"))


def parse_command_request(payload: str) -> tuple[CommandRequest | None, dict[str, Any] | None]:
    try:
        data = json.loads(payload)
    except Exception as exc:  # noqa: BLE001
        return None, _field_error(f"invalid JSON: {exc}", "request")
    if not isinstance(data, dict):
        return None, _field_error("request must be a JSON object", "request")
    if paths := _forbidden_paths(data):
        return None, error_value(
            STATUS_INVALID_REQUEST, "request contains forbidden connector or terminal fields",
            details={"fields": paths})
    if unknown := sorted(set(data) - REQUEST_ALLOWED_FIELDS):
        return None, error_value(
            STATUS_INVALID_REQUEST, f"request contains unknown top-level fields: {unknown}",
            details={"fields": [f"$.{name}" for name in unknown]})
    if type(data.get("schema_version")) is not int or data["schema_version"] != 1:
        return None, _field_error("schema_version must be 1", "schema_version")
    expected = _REQUEST_FIELDS.get(data.get("action"))
    if expected is not None and set(data) != expected:
        return None, _field_error(
            f"{data['action']} requires exactly {sorted(expected)}", "request",
            missing=sorted(expected - set(data)), unexpected=sorted(set(data) - expected))
    return CommandRequest.from_dict(data), None


@dataclass(frozen=True)
class CanonicalMutation:
    canonical_version: int
    action: str
    public_worker_id: str
    canonical_json: str
    fingerprint: str


def _validated_mutation(request: CommandRequest, purpose: str) -> None:
    if not isinstance(request, CommandRequest):
        raise TypeError("request must be a CommandRequest")
    if request.action not in ALLOWED_ACTIONS:
        raise ValueError(f"{purpose} require send_instruction or answer_decision")
    if error := validate_request(request):
        raise ValueError(str(error.get("message") or f"invalid {purpose}"))


def build_canonical_mutation(
    request: CommandRequest, *, public_worker_id: str,
) -> CanonicalMutation:
    _validated_mutation(request, "canonical mutations")
    if not _nonblank(public_worker_id):
        raise ValueError("public_worker_id must be a nonblank string")
    if request.action == "send_instruction":
        body: dict[str, Any] = {"instruction": {"text": request.instruction["text"]}}
    else:
        selection = request.params["selection"]
        canonical_selection = (
            {"option_refs": sorted(
                selection["option_refs"],
                key=lambda ref: int(ref) if str(ref).isdigit() else -1,
            )}
            if "option_refs" in selection else {"text": selection["text"]}
        )
        body = {"decision": {
            "decision_ref": request.params["decision_ref"],
            "selection": canonical_selection,
        }}
    payload = {
        "canonical_version": 1, "action": request.action,
        "target": {"worker_id": public_worker_id}, **body, "options": {},
    }
    return CanonicalMutation(1, request.action, public_worker_id,
                             stable_json_dumps(payload), stable_fingerprint(payload))


def is_selector_proof(value: Any) -> bool:
    return isinstance(value, str) and re.fullmatch(r"v1:[0-9a-f]{64}", value, re.ASCII) is not None


def build_selector_proof(request: CommandRequest) -> str:
    _validated_mutation(request, "selector proofs")
    target = request.target
    selector = {
        "shape": "target", "worker_id": _string_value(target.get("worker_id")),
        "name": "", "space_id": _optional_string(target.get("space_id")),
        "stable_key": _string_value(target.get("stable_key")),
        "stable_key_version": target.get("stable_key_version"),
    }
    payload = {"proof_version": SELECTOR_PROOF_VERSION, "action": request.action,
               "selector": selector}
    digest = _digest(
        b"tendwire.command-selector-proof.v1", stable_json_dumps(payload))
    return f"v{SELECTOR_PROOF_VERSION}:{digest}"


def _valid_envelope_tuple(envelope: "CommandEnvelope") -> bool:
    if envelope.action in ALLOWED_ACTIONS:
        expected_ok, statuses = _DISPOSITION_RULES[envelope.disposition]
        return (envelope.ok is expected_ok and envelope.status in statuses
                and (envelope.status != STATUS_ANSWER_IN_PROGRESS
                     or envelope.action == "answer_decision"))
    return (envelope.action == "" and envelope.disposition == DISPOSITION_NO_RECEIPT
            and not envelope.ok and envelope.status == STATUS_INVALID_REQUEST
            and envelope.request_id is None)


def _require(valid: bool, error: Exception) -> None:
    if not valid:
        raise error


@dataclass(frozen=True)
class CommandEnvelope:
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
        _require(isinstance(self.ok, bool), TypeError("ok must be a boolean"))
        _require(isinstance(self.action, str), TypeError("action must be a string"))
        _require(self.request_id is None or isinstance(self.request_id, str),
                 TypeError("request_id must be a string or null"))
        _require(self.dry_run is False, ValueError("dry_run must be false"))
        _require(self.result is None or isinstance(self.result, Mapping),
                 TypeError("result must be an object or null"))
        _require(self.error is None or isinstance(self.error, Mapping),
                 TypeError("error must be an object or null"))
        warnings_valid = isinstance(self.warnings, list) and all(
            isinstance(item, str) for item in self.warnings)
        _require(warnings_valid, TypeError("warnings must be an array of strings"))
        status_valid = isinstance(self.status, str) and self.status in VALID_STATUSES
        _require(status_valid, ValueError("status must be a supported command status"))
        _require(self.disposition in _DISPOSITION_RULES,
                 ValueError("disposition must be a supported command disposition"))
        schema_valid = type(self.schema_version) is int and self.schema_version == 2
        _require(schema_valid, ValueError("command envelope schema_version must be 2"))
        if self.disposition != DISPOSITION_NO_RECEIPT and not is_valid_request_id(self.request_id):
            raise ValueError("receipt-bearing disposition requires a valid request_id")
        result = None if self.result is None else sanitize_public_mapping(self.result)
        error = None if self.error is None else sanitize_public_mapping(self.error)
        warnings = [clean for item in self.warnings if (clean := sanitize_public_text(item))]
        for name, value in (("result", result), ("error", error), ("warnings", warnings)):
            object.__setattr__(self, name, value)
        if self.ok and error is not None:
            raise ValueError("successful command envelope must not include an error")
        if not _valid_envelope_tuple(self):
            raise ValueError(f"{self.disposition} disposition has an inconsistent command tuple")

    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)

    def to_json(self, indent: int | None = None) -> str:
        return public_json_dumps(self.to_dict(), indent=indent)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CommandEnvelope":
        if not isinstance(data, dict):
            raise TypeError("command envelope must be an object")
        if set(data) != set(cls.__dataclass_fields__):
            raise ValueError("command envelope must contain exactly the schema fields")
        envelope = cls(**data)
        if envelope.to_dict() != data:
            raise ValueError("command envelope is not an exact public roundtrip")
        return envelope

    @classmethod
    def from_result(
        cls, request: CommandRequest, *, ok: bool, status: str,
        disposition: CommandDisposition = DISPOSITION_NO_RECEIPT,
        result: dict[str, Any] | None = None, error: dict[str, Any] | None = None,
        warnings: list[str] | None = None,
    ) -> "CommandEnvelope":
        return cls(
            ok, status, request.action, disposition, request.request_id, result=result,
            error=error, warnings=list(warnings or []),
        )

    @classmethod
    def from_error(
        cls, request: CommandRequest | None, error: dict[str, Any],
    ) -> "CommandEnvelope":
        if request is None or request.action not in ALLOWED_ACTIONS:
            request, error = None, {**error, "code": STATUS_INVALID_REQUEST}
        valid_request_id = request is not None and is_valid_request_id(request.request_id)
        request_id = request.request_id if valid_request_id else None
        return cls(
            False, error.get("code", STATUS_REJECTED), "" if request is None
            else request.action, request_id=request_id, error=error,
        )


def _valid_public_result(result: Mapping[str, Any], request: CommandRequest) -> bool:
    target = result.get("target")
    worker_id = target.get("worker_id") if isinstance(target, dict) else None
    requested = (request.target or {}).get("worker_id")
    common = (
        isinstance(target, dict) and set(target) == {"worker_id"}
        and _nonblank(worker_id)
        and (requested is None or requested == worker_id)
        and result.get("delivery_state") == result.get("transport_state") == "submitted"
    )
    action_fields = (
        {"target_state_at_send", "turn_id", "observed_turn_state",
         "submission_verdict", "submission_id"}
        if request.action == "send_instruction"
        else {"decision", "observed_pending_state"}
    )
    common_fields = {"target", "delivery_state", "transport_state"}
    if not common or set(result) != common_fields | action_fields:
        return False
    if request.action == "send_instruction":
        turn_id, submission_id = result.get("turn_id"), result.get("submission_id")
        return (
            _nonblank(result.get("target_state_at_send"))
            and (turn_id is None or _nonblank(turn_id))
            and result.get("observed_turn_state") in {
                "pending_observation", "observed", "complete",
            }
            and result.get("submission_verdict") == "submitted"
            and is_turn_submission_id(submission_id)
        )
    decision = result.get("decision")
    return (
        isinstance(decision, dict) and set(decision) == {"decision_ref"}
        and _nonblank(decision.get("decision_ref"))
        and decision["decision_ref"] == (request.params or {}).get("decision_ref")
        and result.get("observed_pending_state") == "pending_observation"
    )


def validate_public_command_envelope(envelope: CommandEnvelope,
                                     request: CommandRequest | None = None) -> CommandEnvelope:
    if envelope.disposition != DISPOSITION_TERMINAL_ACCEPTED:
        return envelope
    if request is None or validate_request(request) is not None:
        raise ValueError("accepted public command envelope requires its request")
    result = envelope.result
    if not (
        isinstance(result, dict)
        and (envelope.action, envelope.request_id) == (request.action, request.request_id)
        and _valid_public_result(result, request)
    ):
        raise ValueError("accepted command result violates the fixed public schema")
    return envelope


def worker_candidate(worker: Worker) -> dict[str, Any]:
    return {
        "worker_id": worker.id, "name": worker.name, "space_id": worker.space_id,
        "status": worker.status, "worker_fingerprint": worker.fingerprint,
        **({"summary": worker.summary} if worker.summary else {}),
    }


def resolve_target(target: dict[str, Any] | None, workers: list[Worker], *,
                   allow_disallowed_status: bool = False,
                   ) -> tuple[dict[str, Any] | None, list[dict[str, Any]], str]:
    if target is None:
        return None, [], STATUS_NOT_FOUND
    worker_id = _string_value(target.get("worker_id"))
    space_id = _optional_string(target.get("space_id"))
    stable_key = _string_value(target.get("stable_key"))
    matches = [
        worker for worker in workers
        if (not worker_id or worker.id == worker_id)
        and (space_id is None or worker.space_id == space_id)
        and (not stable_key or (
            worker.meta.get("stable_key") == stable_key
            and worker.meta.get("stable_key_version") == target.get("stable_key_version")
        ))
    ]
    fingerprint = _optional_string(target.get("worker_fingerprint"))
    if fingerprint is not None:
        observed = [worker for worker in matches if worker.fingerprint == fingerprint]
        if matches and not observed:
            status = STATUS_STALE_TARGET if len(matches) == 1 else STATUS_AMBIGUOUS_TARGET
            return None, [worker_candidate(worker) for worker in matches], status
        matches = observed
    candidates = [worker_candidate(worker) for worker in matches]
    if len(matches) != 1:
        status = STATUS_NOT_FOUND if not matches else STATUS_AMBIGUOUS_TARGET
        return None, candidates, status
    if not allow_disallowed_status and matches[0].status in {"closed", "failed", "unknown"}:
        return None, candidates, STATUS_REJECTED
    return candidates[0], candidates, "resolved"
