"""Stateful, transport-independent projection of ACP session events.

The projector deliberately does not own an ACP connection.  It accepts the
JSON-shaped values carried by ``session/update`` notifications and
``session/request_permission`` requests, then emits stable mappings suitable
for a durable event journal.  This keeps protocol transport, persistence, and
Tendwire's public-content boundary separate.

ACP streams reasoning and tool data independently from assistant messages.
That distinction is preserved here: the public turn projection only
contains user and assistant text and can never expose thought or tool payloads.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Final

from acp.schema import RequestPermissionRequest, SessionNotification
from pydantic import ValidationError

_UPDATE_KIND_MAP: Final[dict[str, str]] = {
    "user_message_chunk": "user_message",
    "agent_message_chunk": "agent_message",
    "agent_thought_chunk": "thought",
    "tool_call": "tool_call",
    "tool_call_update": "tool_call_update",
    "plan": "plan",
    "available_commands_update": "extension",
    "current_mode_update": "extension",
    "config_option_update": "extension",
    "usage_update": "usage",
    "session_info_update": "session_info",
}
_MESSAGE_KINDS: Final[frozenset[str]] = frozenset(
    {"user_message", "agent_message", "thought"}
)
_MAX_IDENTIFIER_CHARS: Final[int] = 2048
_MAX_SOURCE_ID_CHARS: Final[int] = 512
_PERMISSION_KINDS: Final[frozenset[str]] = frozenset(
    {"allow_once", "allow_always", "reject_once", "reject_always"}
)


class AcpProjectionError(ValueError):
    """Raised when an ACP value cannot be safely normalized."""


@dataclass
class _MessageAssembly:
    message_id: str
    text: str = ""
    explicit: bool = False


@dataclass
class _SessionState:
    sequence: int = 0
    messages: dict[str, list[_MessageAssembly]] = field(
        default_factory=lambda: {kind: [] for kind in _MESSAGE_KINDS}
    )
    tools: dict[str, dict[str, Any]] = field(default_factory=dict)
    plan: list[dict[str, Any]] = field(default_factory=list)
    usage: dict[str, Any] = field(default_factory=dict)
    info: dict[str, Any] = field(default_factory=dict)
    # The digest lets us distinguish a harmless replay from a producer reusing
    # one supposedly authoritative ID for different content.
    seen_source_events: dict[str, str] = field(default_factory=dict)
    retained_bytes: int = 0
    active_message: tuple[str, str] | None = None
    last_update_name: str | None = None
    implicit_message_ordinals: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class AcpProjectionCheckpoint:
    """Opaque rollback point for one session's in-memory projection state."""

    session_id: str
    state: _SessionState | None


class AcpEventProjector:
    """Normalize ACP notifications while retaining per-session assembly state.

    The instance is intentionally in-memory.  ``dedupe_hint`` is emitted on
    every event so a durable caller can deduplicate replays across process
    restarts.  Within one instance, events with an explicit protocol/transport
    identifier supplied by the transport as ``source_event_id`` are dropped
    when repeated. ACP ``_meta`` is opaque and is never trusted as identity.
    Content hashes are hints only: identical adjacent chunks can be legitimate
    and are therefore never blindly discarded.
    """

    def __init__(
        self,
        *,
        max_source_events_per_session: int = 4096,
        max_messages_per_kind: int = 1024,
        max_tool_calls_per_session: int = 4096,
        max_state_fields: int = 1024,
        max_plan_entries: int = 4096,
        max_text_chars_per_message: int = 4 * 1024 * 1024,
        max_event_bytes: int = 8 * 1024 * 1024,
        max_json_depth: int = 128,
        max_session_state_bytes: int = 8 * 1024 * 1024,
    ) -> None:
        limits = {
            "max_source_events_per_session": max_source_events_per_session,
            "max_messages_per_kind": max_messages_per_kind,
            "max_tool_calls_per_session": max_tool_calls_per_session,
            "max_state_fields": max_state_fields,
            "max_plan_entries": max_plan_entries,
            "max_text_chars_per_message": max_text_chars_per_message,
            "max_event_bytes": max_event_bytes,
            "max_json_depth": max_json_depth,
            "max_session_state_bytes": max_session_state_bytes,
        }
        for name, value in limits.items():
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        self._session_id: str | None = None
        self._state: _SessionState | None = None
        self._max_source_events_per_session = max_source_events_per_session
        self._max_messages_per_kind = max_messages_per_kind
        self._max_tool_calls_per_session = max_tool_calls_per_session
        self._max_state_fields = max_state_fields
        self._max_plan_entries = max_plan_entries
        self._max_text_chars_per_message = max_text_chars_per_message
        self._max_event_bytes = max_event_bytes
        self._max_json_depth = max_json_depth
        self._max_session_state_bytes = max_session_state_bytes

    def normalize_session_update(
        self,
        notification: Mapping[str, Any],
        *,
        source_event_id: str | None = None,
    ) -> dict[str, Any] | None:
        """Normalize one ACP ``session/update`` notification or its params.

        ``notification`` may be a full JSON-RPC notification, its ``params``
        object, or a direct object containing ``sessionId`` and ``update``.
        Unknown ACP update variants are ignored for forward compatibility.
        """

        params = _unwrap_params(notification)
        session_id = _required_string(params, "sessionId")
        update = params.get("update")
        if not isinstance(update, Mapping):
            raise AcpProjectionError("ACP session/update is missing an object update")

        update_name = update.get("sessionUpdate")
        if not isinstance(update_name, str):
            raise AcpProjectionError("ACP update is missing sessionUpdate")
        kind = _UPDATE_KIND_MAP.get(update_name)
        if kind is None:
            return None

        _bounded_json(
            {"update": update, "_meta": params.get("_meta")},
            label="ACP session update",
            max_bytes=self._max_event_bytes,
            max_depth=self._max_json_depth,
        )
        update = _validated_update(session_id, update)
        state = self._pending_session(session_id)
        explicit_id = _explicit_source_event_id(source_event_id)
        replay_digest = _event_digest(kind, update)
        if self._is_replay(state, explicit_id, replay_digest):
            return None

        if kind in _MESSAGE_KINDS:
            payload = self._normalize_message(state, kind, update)
        elif kind in {"tool_call", "tool_call_update"}:
            payload = self._normalize_tool(state, kind, update)
        elif kind == "plan":
            payload = self._normalize_plan(state, update)
        elif kind == "usage":
            payload = self._normalize_usage(state, update)
        elif kind == "extension":
            payload = {
                "schema_version": 1,
                "extension": f"acp.session_update.{update_name}",
                "update": _without_discriminator(update),
            }
        else:
            payload = self._normalize_session_info(state, update)
        extension_meta = _scoped_metadata(params=params, update=update)
        if extension_meta:
            payload["extensions"] = extension_meta

        self._commit_event(
            session_id,
            state,
            update_name,
            explicit_id,
            replay_digest,
            close_implicit=kind not in _MESSAGE_KINDS,
        )
        return _canonical_event(
            sequence=state.sequence,
            kind=kind,
            payload=payload,
            source_event_id=explicit_id,
        )

    def normalize_permission_request(
        self,
        request: Mapping[str, Any],
        *,
        source_event_id: str | None = None,
    ) -> dict[str, Any] | None:
        """Project ``session/request_permission`` as a tool lifecycle update.

        ACP attaches permission requests to tool calls, so no synthetic ninth
        event kind is introduced.  Options are retained in the canonical tool
        payload for a decision layer to consume.
        """

        params = _unwrap_params(request)
        session_id = _required_string(params, "sessionId")
        tool_call = params.get("toolCall")
        if not isinstance(tool_call, Mapping):
            raise AcpProjectionError("ACP permission request is missing toolCall")
        tool_call_id = _required_string(tool_call, "toolCallId")

        _bounded_json(
            params,
            label="ACP permission request",
            max_bytes=self._max_event_bytes,
            max_depth=self._max_json_depth,
        )
        try:
            validated = RequestPermissionRequest.model_validate(dict(params))
        except ValidationError as exc:
            raise AcpProjectionError(
                "ACP permission request does not match the upstream schema"
            ) from exc
        normalized = validated.model_dump(by_alias=True, exclude_none=True)
        tool_call = normalized["toolCall"]
        state = self._pending_session(session_id)
        explicit_id = _explicit_source_event_id(source_event_id)
        options = normalized.get("options")
        if not isinstance(options, list) or not options:
            raise AcpProjectionError("ACP permission request options must be non-empty")
        normalized_options = _permission_options(options)
        request_material = {"toolCall": tool_call, "options": normalized_options}
        replay_digest = _event_digest("tool_call_update", request_material)
        if self._is_replay(state, explicit_id, replay_digest):
            return None

        if (
            tool_call_id not in state.tools
            and len(state.tools) >= self._max_tool_calls_per_session
        ):
            raise AcpProjectionError("ACP tool call state limit exceeded")
        previous_tool = state.tools.get(tool_call_id)
        snapshot = _merge_tool_snapshot(previous_tool, tool_call)
        snapshot["permission"] = {
            "required": True,
            "options": deepcopy(normalized_options),
        }
        if len(snapshot) > self._max_state_fields:
            raise AcpProjectionError("ACP tool snapshot field limit exceeded")
        self._reserve_state(
            state,
            _json_size(snapshot) - (_json_size(previous_tool) if previous_tool else 0),
        )
        state.tools[tool_call_id] = snapshot
        self._commit_event(
            session_id, state, "tool_call_update", explicit_id, replay_digest
        )
        payload = {
            "tool_call_id": tool_call_id,
            "changes": _without_discriminator(tool_call),
            "snapshot": deepcopy(snapshot),
            "permission": deepcopy(snapshot["permission"]),
        }
        extension_meta = _scoped_metadata(params=params, toolCall=tool_call)
        if extension_meta:
            payload["extensions"] = extension_meta
        return _canonical_event(
            sequence=state.sequence,
            kind="tool_call_update",
            payload=payload,
            source_event_id=explicit_id,
        )

    def reset_turn(self, session_id: str) -> None:
        """Start a fresh prompt turn while preserving session-level ACP state."""

        state = self._pending_session(session_id)
        state.retained_bytes = max(
            0, state.retained_bytes - _messages_state_bytes(state.messages)
        )
        state.messages = {kind: [] for kind in _MESSAGE_KINDS}
        state.active_message = None
        state.last_update_name = None
        state.implicit_message_ordinals = {}
        self._commit_state(session_id, state)

    def checkpoint_session(self, session_id: str) -> AcpProjectionCheckpoint:
        """Capture one session so a failed durable append can be rolled back."""

        return AcpProjectionCheckpoint(
            session_id=session_id,
            state=self._state if self._session_id == session_id else None,
        )

    def restore_session(self, checkpoint: AcpProjectionCheckpoint) -> None:
        """Restore a checkpoint created before a speculative normalization."""

        if not isinstance(checkpoint, AcpProjectionCheckpoint):
            raise TypeError("checkpoint must be an AcpProjectionCheckpoint")
        if checkpoint.state is None:
            if self._session_id == checkpoint.session_id:
                self._session_id = None
                self._state = None
        else:
            self._session_id = checkpoint.session_id
            self._state = checkpoint.state

    def _normalize_message(
        self,
        state: _SessionState,
        kind: str,
        update: Mapping[str, Any],
    ) -> dict[str, Any]:
        content = update["content"]
        assert isinstance(content, Mapping)
        message_id_value = update.get("messageId")
        assemblies = state.messages[kind]
        if message_id_value is not None:
            message_id = _identifier(message_id_value, "messageId")
            explicit = True
        else:
            explicit = False
            active = state.active_message
            if active is not None and active[0] == kind:
                candidate = next(
                    (
                        item
                        for item in assemblies
                        if item.message_id == active[1] and not item.explicit
                    ),
                    None,
                )
            else:
                candidate = None
            if state.last_update_name == update.get("sessionUpdate") and candidate is not None:
                message_id = candidate.message_id
            else:
                ordinal = state.implicit_message_ordinals.get(kind, 0) + 1
                message_id = f"implicit-{kind}-{ordinal}"
        active_key = state.active_message
        assembly = None
        if active_key == (kind, message_id):
            assembly = next(
                (item for item in assemblies if item.message_id == message_id), None
            )
        elif explicit and any(
            item.explicit and item.message_id == message_id
            for items in state.messages.values()
            for item in items
        ):
            raise AcpProjectionError("ACP messageId was reused after a message boundary")
        text_delta = content.get("text") if content.get("type") == "text" else None
        text_delta = text_delta if isinstance(text_delta, str) else ""
        public_text_delta = text_delta if _content_is_user_visible(content) else ""
        content_copy = _content_payload(content)
        previous_text = assembly.text if assembly is not None else ""
        assembled_text = previous_text + public_text_delta
        if len(assembled_text) > self._max_text_chars_per_message:
            raise AcpProjectionError("ACP assembled message text limit exceeded")
        if assembly is None:
            if len(assemblies) >= self._max_messages_per_kind:
                raise AcpProjectionError("ACP message assembly limit exceeded")
            self._reserve_state(
                state,
                len(message_id.encode("utf-8"))
                + len(public_text_delta.encode("utf-8")),
            )
            assembly = _MessageAssembly(
                message_id=message_id,
                text=assembled_text,
                explicit=explicit,
            )
            assemblies.append(assembly)
            if not explicit:
                state.implicit_message_ordinals[kind] = int(message_id.rsplit("-", 1)[1])
        else:
            self._reserve_state(state, len(public_text_delta.encode("utf-8")))
            assembly.text = assembled_text
        state.active_message = (kind, message_id)
        return {
            "message_id": message_id,
            "content": content_copy,
            "text_delta": text_delta,
            "assembled_text": assembled_text,
            "message_index": assemblies.index(assembly),
        }

    def _normalize_tool(
        self,
        state: _SessionState,
        kind: str,
        update: Mapping[str, Any],
    ) -> dict[str, Any]:
        tool_call_id = _required_string(update, "toolCallId")
        previous = state.tools.get(tool_call_id)
        if previous is None and len(state.tools) >= self._max_tool_calls_per_session:
            raise AcpProjectionError("ACP tool call state limit exceeded")
        snapshot = _merge_tool_snapshot(previous, update)
        if len(snapshot) > self._max_state_fields:
            raise AcpProjectionError("ACP tool snapshot field limit exceeded")
        payload: dict[str, Any] = {
            "tool_call_id": tool_call_id,
            "snapshot": deepcopy(snapshot),
        }
        if kind == "tool_call_update":
            payload["changes"] = _without_discriminator(update)
        self._reserve_state(
            state, _json_size(snapshot) - (_json_size(previous) if previous else 0)
        )
        state.tools[tool_call_id] = snapshot
        return payload

    def _normalize_plan(
        self, state: _SessionState, update: Mapping[str, Any]
    ) -> dict[str, Any]:
        entries = update.get("entries")
        if not isinstance(entries, list):
            raise AcpProjectionError("ACP plan update entries must be an array")
        if len(entries) > self._max_plan_entries:
            raise AcpProjectionError("ACP plan entry limit exceeded")
        replacement = deepcopy(entries)
        payload: dict[str, Any] = {"entries": deepcopy(replacement), "snapshot": True}
        self._reserve_state(state, _json_size(replacement) - _json_size(state.plan))
        state.plan = replacement
        return payload

    def _normalize_usage(
        self, state: _SessionState, update: Mapping[str, Any]
    ) -> dict[str, Any]:
        replacement = {
            "used": update["used"],
            "size": update["size"],
            **({"cost": deepcopy(update["cost"])} if update.get("cost") is not None else {}),
        }
        if len(replacement) > self._max_state_fields:
            raise AcpProjectionError("ACP usage state field limit exceeded")
        self._reserve_state(state, _json_size(replacement) - _json_size(state.usage))
        state.usage = replacement
        payload = deepcopy(state.usage)
        return payload

    def _normalize_session_info(
        self, state: _SessionState, update: Mapping[str, Any]
    ) -> dict[str, Any]:
        # Presence is meaningful: explicit null clears an existing property.
        changes = {
            key: deepcopy(update[key]) for key in ("title", "updatedAt") if key in update
        }
        replacement = {**state.info, **changes}
        if len(replacement) > self._max_state_fields:
            raise AcpProjectionError("ACP session info state field limit exceeded")
        self._reserve_state(state, _json_size(replacement) - _json_size(state.info))
        state.info = replacement
        payload = deepcopy(state.info)
        return payload

    def _pending_session(self, session_id: str) -> _SessionState:
        if self._session_id not in {None, session_id}:
            raise AcpProjectionError("ACP projector is already bound to another session")
        return deepcopy(self._state) if self._state is not None else _SessionState()

    def _commit_state(self, session_id: str, state: _SessionState) -> None:
        self._session_id = session_id
        self._state = state

    def _reserve_state(self, state: _SessionState, retained_delta: int) -> None:
        retained = max(0, state.retained_bytes + retained_delta)
        if retained > self._max_session_state_bytes:
            raise AcpProjectionError("ACP retained session state limit exceeded")
        state.retained_bytes = retained

    def _is_replay(
        self, state: _SessionState, event_id: str | None, digest: str
    ) -> bool:
        if event_id is None:
            return False
        previous = state.seen_source_events.get(event_id)
        if previous == digest:
            return True
        if previous is not None:
            raise AcpProjectionError(
                "ACP source event ID was reused for different content"
            )
        if len(state.seen_source_events) >= self._max_source_events_per_session:
            raise AcpProjectionError("ACP source event replay window is full")
        return False

    def _commit_event(
        self,
        session_id: str,
        state: _SessionState,
        update_name: str,
        event_id: str | None,
        digest: str,
        *,
        close_implicit: bool = True,
    ) -> None:
        state.sequence += 1
        state.last_update_name = update_name
        if close_implicit and state.active_message is not None:
            kind, message_id = state.active_message
            active = next(
                (item for item in state.messages[kind] if item.message_id == message_id),
                None,
            )
            if active is not None and not active.explicit:
                state.active_message = None
        if event_id is not None:
            self._reserve_state(
                state, len(event_id.encode()) + len(digest.encode("ascii"))
            )
            state.seen_source_events[event_id] = digest
        self._commit_state(session_id, state)


def _unwrap_params(value: Mapping[str, Any]) -> Mapping[str, Any]:
    params = value.get("params")
    if isinstance(params, Mapping):
        return params
    return value


def _required_string(value: Mapping[str, Any], key: str) -> str:
    item = value.get(key)
    try:
        return _identifier(item, key)
    except AcpProjectionError:
        raise AcpProjectionError(f"ACP value is missing non-empty {key}") from None


def _identifier(value: Any, label: str) -> str:
    if not isinstance(value, str):
        raise AcpProjectionError(f"ACP value has invalid {label}")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise AcpProjectionError(f"ACP value has invalid {label}") from exc
    if not value or len(value) > _MAX_IDENTIFIER_CHARS or "\x00" in value:
        raise AcpProjectionError(f"ACP value has invalid {label}")
    return value


def _explicit_source_event_id(value: Any) -> str | None:
    if value is None:
        return None
    return _source_identifier(value, "source_event_id")


def _source_identifier(value: Any, label: str) -> str:
    identifier = _identifier(value, label)
    if len(identifier) > _MAX_SOURCE_ID_CHARS:
        raise AcpProjectionError(f"ACP value has invalid {label}")
    return identifier


def _without_discriminator(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: deepcopy(item)
        for key, item in value.items()
        if key not in {"sessionUpdate", "_meta"}
    }


def _merge_tool_snapshot(
    previous: Mapping[str, Any] | None, update: Mapping[str, Any]
) -> dict[str, Any]:
    snapshot = deepcopy(dict(previous)) if previous is not None else {}
    if update.get("sessionUpdate") == "tool_call":
        snapshot.setdefault("kind", "other")
        snapshot.setdefault("status", "pending")
        snapshot.setdefault("content", [])
        snapshot.setdefault("locations", [])
    for key in (
        "toolCallId",
        "title",
        "kind",
        "status",
        "content",
        "locations",
        "rawInput",
        "rawOutput",
    ):
        if key in update and update[key] is not None:
            snapshot[key] = deepcopy(update[key])
    return snapshot


def _content_payload(content: Mapping[str, Any]) -> dict[str, Any]:
    """Retain a validated content block, including opaque standard ``_meta``."""

    return deepcopy(dict(content))


def _content_is_user_visible(content: Mapping[str, Any]) -> bool:
    annotations = content.get("annotations")
    if not isinstance(annotations, Mapping) or "audience" not in annotations:
        return True
    audience = annotations.get("audience")
    if audience is None:
        return True
    return isinstance(audience, list) and "user" in audience


def _extension_metadata(value: Mapping[str, Any]) -> dict[str, Any]:
    meta = value.get("_meta")
    if meta is None or not isinstance(meta, Mapping) or any(
        not isinstance(key, str) for key in meta
    ):
        return {}
    return _safe_deepcopy(dict(meta), label="ACP _meta")


def _scoped_metadata(**values: Mapping[str, Any]) -> dict[str, Any]:
    """Preserve each protocol object's opaque metadata without key collisions."""

    return {
        scope: metadata
        for scope, value in values.items()
        if (metadata := _extension_metadata(value))
    }


def _validated_update(session_id: str, update: Mapping[str, Any]) -> dict[str, Any]:
    """Delegate stable ACP payload validation to the upstream schema."""

    if update.get("sessionUpdate") in {
        "available_commands_update",
        "current_mode_update",
        "config_option_update",
    }:
        # Control updates stay private and are intentionally projected as
        # opaque extensions; accepting older producers here is harmless.
        return deepcopy(dict(update))
    try:
        value = SessionNotification.model_validate(
            {"sessionId": session_id, "update": dict(update)}
        ).model_dump(by_alias=True, exclude_none=True)
    except ValidationError as exc:
        raise AcpProjectionError(
            "ACP session update does not match the upstream schema"
        ) from exc
    return value["update"]


def _permission_options(options: list[Any]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for option in options:
        if not isinstance(option, Mapping):
            raise AcpProjectionError("ACP permission request option must be an object")
        option_copy = deepcopy(dict(option))
        option_id = _required_string(option_copy, "optionId")
        _required_string(option_copy, "name")
        kind = _required_string(option_copy, "kind")
        if kind not in _PERMISSION_KINDS:
            raise AcpProjectionError("ACP permission option has invalid kind")
        if option_id in seen:
            raise AcpProjectionError("ACP permission option IDs must be unique")
        seen.add(option_id)
        normalized.append(option_copy)
    return normalized


def _bounded_json(
    value: Any, *, label: str, max_bytes: int, max_depth: int
) -> bytes:
    _validate_json_depth(value, label=label, max_depth=max_depth)
    try:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, RecursionError) as exc:
        raise AcpProjectionError(f"{label} must be bounded JSON data") from exc
    if len(encoded) > max_bytes:
        raise AcpProjectionError(f"{label} exceeds the size limit")
    return encoded


def _validate_json_depth(value: Any, *, label: str, max_depth: int) -> None:
    stack: list[tuple[Any, int]] = [(value, 0)]
    visited: set[int] = set()
    while stack:
        item, depth = stack.pop()
        if depth > max_depth:
            raise AcpProjectionError(f"{label} exceeds the nesting limit")
        if not isinstance(item, (Mapping, list, tuple)):
            continue
        identity = id(item)
        if identity in visited:
            continue
        visited.add(identity)
        children = item.values() if isinstance(item, Mapping) else item
        stack.extend((child, depth + 1) for child in children)


def _safe_deepcopy(value: Any, *, label: str) -> Any:
    try:
        return deepcopy(value)
    except (TypeError, ValueError, RecursionError, OverflowError) as exc:
        raise AcpProjectionError(f"{label} could not be copied safely") from exc


def _json_size(value: Any) -> int:
    return len(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    )


def _messages_state_bytes(
    messages: Mapping[str, list[_MessageAssembly]],
) -> int:
    return sum(
        len(message.message_id.encode("utf-8")) + len(message.text.encode("utf-8"))
        for assemblies in messages.values()
        for message in assemblies
    )


def _event_digest(kind: str, value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        {"kind": kind, "value": value},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _canonical_event(
    *,
    sequence: int,
    kind: str,
    payload: Mapping[str, Any],
    source_event_id: str | None,
) -> dict[str, Any]:
    return {
        "sequence": sequence,
        "kind": kind,
        "payload": deepcopy(dict(payload)),
        "source_event_id": source_event_id,
    }


__all__ = [
    "AcpEventProjector",
    "AcpProjectionCheckpoint",
    "AcpProjectionError",
]
