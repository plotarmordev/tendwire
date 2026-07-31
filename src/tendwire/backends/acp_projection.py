"""Stateful, transport-independent projection of ACP session events.

The projector deliberately does not own an ACP connection.  It accepts the
JSON-shaped values carried by ``session/update`` notifications and
``session/request_permission`` requests, then emits stable mappings suitable
for a durable event journal.  This keeps protocol transport, persistence, and
Tendwire's public-content boundary separate.

ACP streams reasoning and tool data independently from assistant messages.
That distinction is preserved here: the compatibility turn projection only
contains user and assistant text and can never expose thought or tool payloads.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Final


SUPPORTED_EVENT_KINDS: Final[frozenset[str]] = frozenset(
    {
        "user_message",
        "agent_message",
        "thought",
        "tool_call",
        "tool_call_update",
        "plan",
        "usage",
        "session_info",
    }
)

_UPDATE_KIND_MAP: Final[dict[str, str]] = {
    "user_message_chunk": "user_message",
    "agent_message_chunk": "agent_message",
    "agent_thought_chunk": "thought",
    "tool_call": "tool_call",
    "tool_call_update": "tool_call_update",
    "plan": "plan",
    "usage_update": "usage",
    "session_info_update": "session_info",
}
_RAW_TOOL_FIELDS: Final[tuple[str, str]] = ("rawInput", "rawOutput")
_MESSAGE_KINDS: Final[frozenset[str]] = frozenset(
    {"user_message", "agent_message", "thought"}
)
_LEGACY_EMPTY: Final[dict[str, Any]] = {
    "user_text": "",
    "assistant_stream_text": "",
    "assistant_final_text": "",
    "complete": False,
    "has_open_turn": False,
}
_MAX_IDENTIFIER_CHARS: Final[int] = 2048
_MAX_SOURCE_ID_CHARS: Final[int] = 512


class AcpProjectionError(ValueError):
    """Raised when an ACP value cannot be safely normalized."""


@dataclass
class _MessageAssembly:
    message_id: str
    text: str = ""


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
    complete: bool = False


class AcpEventProjector:
    """Normalize ACP notifications while retaining per-session assembly state.

    The instance is intentionally in-memory.  ``dedupe_hint`` is emitted on
    every event so a durable caller can deduplicate replays across process
    restarts.  Within one instance, events with an explicit protocol/transport
    identifier (``source_event_id`` or a recognized ``_meta`` key) are dropped
    when repeated.  Content hashes are hints only: identical adjacent chunks
    can be legitimate and are therefore never blindly discarded.
    """

    def __init__(
        self,
        *,
        max_sessions: int = 64,
        max_source_events_per_session: int = 4096,
        max_messages_per_kind: int = 1024,
        max_tool_calls_per_session: int = 4096,
        max_state_fields: int = 1024,
        max_plan_entries: int = 4096,
        max_text_chars_per_message: int = 4 * 1024 * 1024,
        max_event_bytes: int = 8 * 1024 * 1024,
        max_session_state_bytes: int = 8 * 1024 * 1024,
        max_total_state_bytes: int = 128 * 1024 * 1024,
    ) -> None:
        limits = {
            "max_sessions": max_sessions,
            "max_source_events_per_session": max_source_events_per_session,
            "max_messages_per_kind": max_messages_per_kind,
            "max_tool_calls_per_session": max_tool_calls_per_session,
            "max_state_fields": max_state_fields,
            "max_plan_entries": max_plan_entries,
            "max_text_chars_per_message": max_text_chars_per_message,
            "max_event_bytes": max_event_bytes,
            "max_session_state_bytes": max_session_state_bytes,
            "max_total_state_bytes": max_total_state_bytes,
        }
        for name, value in limits.items():
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        self._sessions: dict[str, _SessionState] = {}
        self._max_sessions = max_sessions
        self._max_source_events_per_session = max_source_events_per_session
        self._max_messages_per_kind = max_messages_per_kind
        self._max_tool_calls_per_session = max_tool_calls_per_session
        self._max_state_fields = max_state_fields
        self._max_plan_entries = max_plan_entries
        self._max_text_chars_per_message = max_text_chars_per_message
        self._max_event_bytes = max_event_bytes
        self._max_session_state_bytes = max_session_state_bytes
        self._max_total_state_bytes = max_total_state_bytes

    def normalize_session_update(
        self,
        notification: Mapping[str, Any],
        *,
        source_event_id: str | None = None,
        replay: bool = False,
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
        )
        state, is_new_session = self._pending_session(session_id)
        explicit_id = _explicit_source_event_id(source_event_id)
        if explicit_id is None:
            explicit_id = _source_event_id(notification, params, update)
        replay_digest = _event_digest(kind, update)
        if explicit_id is not None:
            previous_digest = state.seen_source_events.get(explicit_id)
            if previous_digest == replay_digest:
                return None
            if previous_digest is not None:
                raise AcpProjectionError(
                    "ACP source event ID was reused for different content"
                )
            if len(state.seen_source_events) >= self._max_source_events_per_session:
                raise AcpProjectionError("ACP source event replay window is full")

        if kind in _MESSAGE_KINDS:
            if state.complete:
                raise AcpProjectionError(
                    "ACP turn is complete; reset_turn is required before new message chunks"
                )
            payload = self._normalize_message(state, kind, update)
        elif kind in {"tool_call", "tool_call_update"}:
            payload = self._normalize_tool(state, kind, update)
        elif kind == "plan":
            payload = self._normalize_plan(state, update)
        elif kind == "usage":
            payload = self._normalize_usage(state, update)
        else:
            payload = self._normalize_session_info(state, update)
        extension_meta = {
            **_extension_metadata(params),
            **_extension_metadata(update),
        }
        if extension_meta:
            payload["extensions"] = extension_meta

        state.sequence += 1
        if explicit_id is not None:
            state.seen_source_events[explicit_id] = replay_digest
        if is_new_session:
            self._sessions[session_id] = state
        return _canonical_event(
            session_id=session_id,
            sequence=state.sequence,
            kind=kind,
            payload=payload,
            source_event_id=explicit_id,
            replay=replay,
            original_update=update,
        )

    def normalize_permission_request(
        self,
        request: Mapping[str, Any],
        *,
        source_event_id: str | None = None,
        replay: bool = False,
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
        )
        state, is_new_session = self._pending_session(session_id)
        explicit_id = _explicit_source_event_id(source_event_id)
        if explicit_id is None:
            explicit_id = _jsonrpc_request_id(request) or _source_event_id(
                request, params, tool_call
            )
        options = params.get("options")
        if not isinstance(options, list) or not options:
            raise AcpProjectionError("ACP permission request options must be non-empty")
        normalized_options = _permission_options(options)
        request_material = {"toolCall": tool_call, "options": normalized_options}
        replay_digest = _event_digest("tool_call_update", request_material)
        if explicit_id is not None:
            previous_digest = state.seen_source_events.get(explicit_id)
            if previous_digest == replay_digest:
                return None
            if previous_digest is not None:
                raise AcpProjectionError(
                    "ACP source event ID was reused for different content"
                )
            if len(state.seen_source_events) >= self._max_source_events_per_session:
                raise AcpProjectionError("ACP source event replay window is full")

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
        state.sequence += 1
        if explicit_id is not None:
            state.seen_source_events[explicit_id] = replay_digest
        if is_new_session:
            self._sessions[session_id] = state
        payload = {
            "tool_call_id": tool_call_id,
            "changes": _without_discriminator(tool_call),
            "snapshot": deepcopy(snapshot),
            "permission": deepcopy(snapshot["permission"]),
        }
        extension_meta = {
            **_extension_metadata(params),
            **_extension_metadata(tool_call),
        }
        if extension_meta:
            payload["extensions"] = extension_meta
        return _canonical_event(
            session_id=session_id,
            sequence=state.sequence,
            kind="tool_call_update",
            payload=payload,
            source_event_id=explicit_id,
            replay=replay,
            original_update={
                "sessionUpdate": "tool_call_update",
                "toolCall": dict(tool_call),
                "options": normalized_options,
            },
        )

    def project_turn_content(
        self,
        session_id: str,
        *,
        complete: bool | None = None,
    ) -> dict[str, Any]:
        """Return Tendwire's legacy text-only turn shape for one ACP session.

        Thought text, plans, tool content, raw inputs, raw outputs, and
        permission details are structurally unreachable from this projection.
        ``complete=True`` moves assembled assistant text from stream to final.
        Callers should only set it after the ACP ``session/prompt`` response.
        """

        state = self._sessions.get(session_id)
        if state is None:
            return dict(_LEGACY_EMPTY)
        # A read-only projection may promote a snapshot to final, but must
        # never demote already-final state.  Reopening requires reset_turn().
        is_complete = state.complete or (
            bool(complete) if complete is not None else False
        )
        user_text = _joined_messages(state.messages["user_message"])
        assistant_text = _joined_messages(state.messages["agent_message"])
        return {
            "user_text": user_text,
            "assistant_stream_text": "" if is_complete else assistant_text,
            "assistant_final_text": assistant_text if is_complete else "",
            "complete": is_complete,
            "has_open_turn": bool(user_text or assistant_text) and not is_complete,
        }

    def mark_turn_complete(self, session_id: str) -> dict[str, Any]:
        """Mark the current ACP prompt turn complete and return legacy content."""

        state = self._session(session_id)
        state.complete = True
        return self.project_turn_content(session_id)

    def reset_turn(self, session_id: str) -> None:
        """Start a fresh prompt turn while preserving session-level ACP state."""

        state = self._session(session_id)
        state.retained_bytes = max(
            0, state.retained_bytes - _messages_state_bytes(state.messages)
        )
        state.messages = {kind: [] for kind in _MESSAGE_KINDS}
        state.complete = False

    def drop_session(self, session_id: str) -> bool:
        """Release all in-memory state after the owning ACP session is closed."""

        return self._sessions.pop(session_id, None) is not None

    def session_snapshot(self, session_id: str) -> dict[str, Any] | None:
        """Return a defensive snapshot for persistence or diagnostics."""

        state = self._sessions.get(session_id)
        if state is None:
            return None
        return {
            "session_id": session_id,
            "sequence": state.sequence,
            "messages": {
                kind: [
                    {"message_id": message.message_id, "text": message.text}
                    for message in messages
                ]
                for kind, messages in state.messages.items()
            },
            "tools": deepcopy(state.tools),
            "plan": deepcopy(state.plan),
            "usage": deepcopy(state.usage),
            "session_info": deepcopy(state.info),
            "complete": state.complete,
        }

    def _normalize_message(
        self,
        state: _SessionState,
        kind: str,
        update: Mapping[str, Any],
    ) -> dict[str, Any]:
        content = update.get("content")
        if not isinstance(content, Mapping):
            raise AcpProjectionError(f"ACP {kind} update is missing content")
        message_id_value = update.get("messageId")
        assemblies = state.messages[kind]
        if message_id_value is not None:
            message_id = _identifier(message_id_value, "messageId")
        else:
            # ACP stable v1 chunks do not require message IDs.  Keep their
            # assembly separate from adapter extensions that do provide IDs.
            message_id = f"implicit-{kind}-1"
        assembly = next(
            (item for item in assemblies if item.message_id == message_id), None
        )
        text_delta = content.get("text") if content.get("type") == "text" else None
        if not isinstance(text_delta, str):
            text_delta = ""
        content_copy = _content_payload(content)
        extension_meta = _extension_metadata(update)
        previous_text = assembly.text if assembly is not None else ""
        assembled_text = previous_text + text_delta
        if len(assembled_text) > self._max_text_chars_per_message:
            raise AcpProjectionError("ACP assembled message text limit exceeded")
        if assembly is None:
            if len(assemblies) >= self._max_messages_per_kind:
                raise AcpProjectionError("ACP message assembly limit exceeded")
            self._reserve_state(
                state,
                len(message_id.encode("utf-8")) + len(text_delta.encode("utf-8")),
            )
            assembly = _MessageAssembly(message_id=message_id, text=assembled_text)
            assemblies.append(assembly)
        else:
            self._reserve_state(state, len(text_delta.encode("utf-8")))
            assembly.text = assembled_text
        return {
            "message_id": message_id,
            "content": content_copy,
            "text_delta": text_delta,
            "assembled_text": assembled_text,
            "message_index": assemblies.index(assembly),
            **({"extensions": extension_meta} if extension_meta else {}),
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
        extension_meta = _extension_metadata(update)
        if extension_meta:
            payload["extensions"] = extension_meta
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
        if any(not isinstance(entry, Mapping) for entry in entries):
            raise AcpProjectionError("ACP plan entry must be an object")
        replacement = [deepcopy(dict(entry)) for entry in entries]
        payload: dict[str, Any] = {"entries": deepcopy(replacement), "snapshot": True}
        extension_meta = _extension_metadata(update)
        if extension_meta:
            payload["extensions"] = extension_meta
        self._reserve_state(state, _json_size(replacement) - _json_size(state.plan))
        state.plan = replacement
        return payload

    def _normalize_usage(
        self, state: _SessionState, update: Mapping[str, Any]
    ) -> dict[str, Any]:
        replacement = {**state.usage, **_without_discriminator(update)}
        if len(replacement) > self._max_state_fields:
            raise AcpProjectionError("ACP usage state field limit exceeded")
        self._reserve_state(state, _json_size(replacement) - _json_size(state.usage))
        state.usage = replacement
        payload = deepcopy(state.usage)
        extension_meta = _extension_metadata(update)
        if extension_meta:
            payload["extensions"] = extension_meta
        return payload

    def _normalize_session_info(
        self, state: _SessionState, update: Mapping[str, Any]
    ) -> dict[str, Any]:
        # Presence is meaningful: explicit null clears an existing property.
        replacement = {**state.info, **_without_discriminator(update)}
        if len(replacement) > self._max_state_fields:
            raise AcpProjectionError("ACP session info state field limit exceeded")
        self._reserve_state(state, _json_size(replacement) - _json_size(state.info))
        state.info = replacement
        payload = deepcopy(state.info)
        extension_meta = _extension_metadata(update)
        if extension_meta:
            payload["extensions"] = extension_meta
        return payload

    def _session(self, session_id: str) -> _SessionState:
        state = self._sessions.get(session_id)
        if state is not None:
            return state
        if len(self._sessions) >= self._max_sessions:
            raise AcpProjectionError("ACP projector session limit exceeded")
        state = _SessionState()
        self._sessions[session_id] = state
        return state

    def _pending_session(self, session_id: str) -> tuple[_SessionState, bool]:
        state = self._sessions.get(session_id)
        if state is not None:
            return state, False
        if len(self._sessions) >= self._max_sessions:
            raise AcpProjectionError("ACP projector session limit exceeded")
        return _SessionState(), True

    def _reserve_state(self, state: _SessionState, retained_delta: int) -> None:
        retained = max(0, state.retained_bytes + retained_delta)
        if retained > self._max_session_state_bytes:
            raise AcpProjectionError("ACP retained session state limit exceeded")
        other_retained = sum(
            item.retained_bytes
            for item in self._sessions.values()
            if item is not state
        )
        if other_retained + retained > self._max_total_state_bytes:
            raise AcpProjectionError("ACP total retained state limit exceeded")
        state.retained_bytes = retained


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


def _source_event_id(*values: Mapping[str, Any]) -> str | None:
    for value in values:
        for key in ("eventId", "event_id", "notificationId", "notification_id"):
            candidate = value.get(key)
            if _valid_wire_id(candidate):
                return _source_identifier(str(candidate), "source event ID")
        meta = value.get("_meta")
        if isinstance(meta, Mapping):
            for key in ("eventId", "event_id", "notificationId", "notification_id"):
                candidate = meta.get(key)
                if _valid_wire_id(candidate):
                    return _source_identifier(str(candidate), "source event ID")
    return None


def _jsonrpc_request_id(value: Mapping[str, Any]) -> str | None:
    """Return a request ID, but never mistake a notification field for one."""

    if value.get("method") != "session/request_permission":
        return None
    candidate = value.get("id")
    if _valid_wire_id(candidate):
        # JSON-RPC request IDs and producer notification IDs are separate
        # namespaces and commonly both start at small integers.
        return _source_identifier(f"request:{candidate}", "JSON-RPC request ID")
    return None


def _valid_wire_id(value: Any) -> bool:
    return not isinstance(value, bool) and isinstance(value, (str, int)) and bool(
        str(value)
    )


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
    snapshot.update(_without_discriminator(update))
    return snapshot


def _content_payload(content: Mapping[str, Any]) -> dict[str, Any]:
    """Retain content plus only explicitly namespaced private metadata."""

    copied = {
        key: deepcopy(item) for key, item in content.items() if key != "_meta"
    }
    meta = _extension_metadata(content)
    if meta:
        copied["_meta"] = meta
    return copied


def _extension_metadata(value: Mapping[str, Any]) -> dict[str, Any]:
    meta = value.get("_meta")
    if not isinstance(meta, Mapping):
        return {}
    return {
        str(key): deepcopy(item)
        for key, item in meta.items()
        if isinstance(key, str) and "/" in key
    }


def _permission_options(options: list[Any]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for option in options:
        if not isinstance(option, Mapping):
            raise AcpProjectionError("ACP permission request option must be an object")
        option_id = _required_string(option, "optionId")
        _required_string(option, "name")
        _required_string(option, "kind")
        if option_id in seen:
            raise AcpProjectionError("ACP permission option IDs must be unique")
        seen.add(option_id)
        normalized.append(deepcopy(dict(option)))
    return normalized


def _bounded_json(value: Any, *, label: str, max_bytes: int) -> bytes:
    try:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
    except (TypeError, ValueError, RecursionError) as exc:
        raise AcpProjectionError(f"{label} must be bounded JSON data") from exc
    if len(encoded) > max_bytes:
        raise AcpProjectionError(f"{label} exceeds the size limit")
    return encoded


def _json_size(value: Any) -> int:
    return len(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
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
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _canonical_event(
    *,
    session_id: str,
    sequence: int,
    kind: str,
    payload: Mapping[str, Any],
    source_event_id: str | None,
    replay: bool,
    original_update: Mapping[str, Any],
) -> dict[str, Any]:
    if kind not in SUPPORTED_EVENT_KINDS:
        raise AcpProjectionError(f"unsupported canonical event kind: {kind}")
    dedupe_material = {
        "session_id": session_id,
        "kind": kind,
        "source_event_id": source_event_id,
        "update": original_update,
    }
    encoded = json.dumps(
        dedupe_material, sort_keys=True, separators=(",", ":"), default=str
    ).encode("utf-8")
    privacy = "session"
    private_fields: list[str] = []
    if kind == "thought":
        privacy = "private"
        private_fields = ["payload"]
    elif kind in {"tool_call", "tool_call_update"}:
        privacy = "mixed"
        for field_name in _RAW_TOOL_FIELDS:
            snake_name = "raw_input" if field_name == "rawInput" else "raw_output"
            private_fields.extend(
                [
                    f"payload.snapshot.{field_name}",
                    f"payload.snapshot.{snake_name}",
                    f"payload.changes.{field_name}",
                    f"payload.changes.{snake_name}",
                ]
            )
        if "permission" in payload:
            private_fields.extend(
                ["payload.permission", "payload.snapshot.permission"]
            )
    event_id = (
        f"acp:{session_id}:{source_event_id}"
        if source_event_id is not None
        else f"acp:{session_id}:local:{sequence}"
    )
    return {
        "schema_version": 1,
        "event_id": event_id,
        "source": "acp",
        "session_id": session_id,
        "sequence": sequence,
        "kind": kind,
        "payload": deepcopy(dict(payload)),
        "privacy": privacy,
        "private_fields": private_fields,
        "source_event_id": source_event_id,
        "replay": bool(replay),
        "dedupe_hint": hashlib.sha256(encoded).hexdigest(),
        "dedupe_safe": source_event_id is not None,
    }


def _joined_messages(messages: list[_MessageAssembly]) -> str:
    return "\n\n".join(message.text for message in messages if message.text)


__all__ = [
    "AcpEventProjector",
    "AcpProjectionError",
    "SUPPORTED_EVENT_KINDS",
]
