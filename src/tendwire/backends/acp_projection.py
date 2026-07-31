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
import math
import os
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
        "extension",
    }
)

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
_TOOL_KINDS: Final[frozenset[str]] = frozenset(
    {
        "read",
        "edit",
        "delete",
        "move",
        "search",
        "execute",
        "think",
        "fetch",
        "switch_mode",
        "other",
    }
)
_TOOL_STATUSES: Final[frozenset[str]] = frozenset(
    {"pending", "in_progress", "completed", "failed"}
)
_PLAN_PRIORITIES: Final[frozenset[str]] = frozenset({"high", "medium", "low"})
_PLAN_STATUSES: Final[frozenset[str]] = frozenset(
    {"pending", "in_progress", "completed"}
)
_PERMISSION_KINDS: Final[frozenset[str]] = frozenset(
    {"allow_once", "allow_always", "reject_once", "reject_always"}
)
_CONTENT_TYPES: Final[frozenset[str]] = frozenset(
    {"text", "image", "audio", "resource_link", "resource"}
)
_ANNOTATION_ROLES: Final[frozenset[str]] = frozenset({"assistant", "user"})


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
    complete: bool = False
    active_message: tuple[str, str] | None = None
    last_update_name: str | None = None
    implicit_message_ordinals: dict[str, int] = field(default_factory=dict)
    replaced_state: _SessionState | None = field(default=None, repr=False)


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
        max_sessions: int = 64,
        max_source_events_per_session: int = 4096,
        max_messages_per_kind: int = 1024,
        max_tool_calls_per_session: int = 4096,
        max_state_fields: int = 1024,
        max_plan_entries: int = 4096,
        max_text_chars_per_message: int = 4 * 1024 * 1024,
        max_event_bytes: int = 8 * 1024 * 1024,
        max_json_depth: int = 128,
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
            "max_json_depth": max_json_depth,
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
        self._max_json_depth = max_json_depth
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
            max_depth=self._max_json_depth,
        )
        update = _normalized_supported_update(update_name, update)
        state, _is_new_session = self._pending_session(session_id)
        explicit_id = _explicit_source_event_id(source_event_id)
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

        state.sequence += 1
        state.last_update_name = update_name
        if kind not in _MESSAGE_KINDS and state.active_message is not None:
            active_kind, active_id = state.active_message
            active = next(
                (
                    item
                    for item in state.messages[active_kind]
                    if item.message_id == active_id
                ),
                None,
            )
            if active is not None and not active.explicit:
                state.active_message = None
        if explicit_id is not None:
            self._reserve_state(
                state,
                len(explicit_id.encode("utf-8")) + len(replay_digest.encode("ascii")),
            )
            state.seen_source_events[explicit_id] = replay_digest
        state.replaced_state = None
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
            max_depth=self._max_json_depth,
        )
        tool_call = _normalized_tool_update(
            tool_call, label="ACP permission toolCall"
        )
        state, _is_new_session = self._pending_session(session_id)
        explicit_id = _explicit_source_event_id(source_event_id)
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
        state.last_update_name = "tool_call_update"
        if state.active_message is not None:
            active_kind, active_id = state.active_message
            active = next(
                (
                    item
                    for item in state.messages[active_kind]
                    if item.message_id == active_id
                ),
                None,
            )
            if active is not None and not active.explicit:
                state.active_message = None
        if explicit_id is not None:
            self._reserve_state(
                state,
                len(explicit_id.encode("utf-8")) + len(replay_digest.encode("ascii")),
            )
            state.seen_source_events[explicit_id] = replay_digest
        state.replaced_state = None
        self._sessions[session_id] = state
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

        state, _is_new_session = self._pending_session(session_id)
        state.complete = True
        state.replaced_state = None
        self._sessions[session_id] = state
        return self.project_turn_content(session_id)

    def reset_turn(self, session_id: str) -> None:
        """Start a fresh prompt turn while preserving session-level ACP state."""

        state, _is_new_session = self._pending_session(session_id)
        state.retained_bytes = max(
            0, state.retained_bytes - _messages_state_bytes(state.messages)
        )
        state.messages = {kind: [] for kind in _MESSAGE_KINDS}
        state.complete = False
        state.active_message = None
        state.last_update_name = None
        state.implicit_message_ordinals = {}
        state.replaced_state = None
        self._sessions[session_id] = state

    def checkpoint_session(self, session_id: str) -> AcpProjectionCheckpoint:
        """Capture one session so a failed durable append can be rolled back."""

        return AcpProjectionCheckpoint(
            session_id=session_id,
            state=self._sessions.get(session_id),
        )

    def restore_session(self, checkpoint: AcpProjectionCheckpoint) -> None:
        """Restore a checkpoint created before a speculative normalization."""

        if not isinstance(checkpoint, AcpProjectionCheckpoint):
            raise TypeError("checkpoint must be an AcpProjectionCheckpoint")
        if checkpoint.state is None:
            self._sessions.pop(checkpoint.session_id, None)
        else:
            self._sessions[checkpoint.session_id] = checkpoint.state

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
        replacement = [_normalized_plan_entry(entry) for entry in entries]
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

    def _pending_session(self, session_id: str) -> tuple[_SessionState, bool]:
        state = self._sessions.get(session_id)
        if state is not None:
            return _copy_session_state(state), False
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
            if item is not state and item is not state.replaced_state
        )
        if other_retained + retained > self._max_total_state_bytes:
            raise AcpProjectionError("ACP total retained state limit exceeded")
        state.retained_bytes = retained


def _unwrap_params(value: Mapping[str, Any]) -> Mapping[str, Any]:
    params = value.get("params")
    if isinstance(params, Mapping):
        return params
    return value


def _copy_session_state(state: _SessionState) -> _SessionState:
    """Copy mutable indexes while sharing immutable text and untouched snapshots."""

    return _SessionState(
        sequence=state.sequence,
        messages={
            kind: [
                _MessageAssembly(
                    message_id=message.message_id,
                    text=message.text,
                    explicit=message.explicit,
                )
                for message in messages
            ]
            for kind, messages in state.messages.items()
        },
        tools=dict(state.tools),
        plan=state.plan,
        usage=state.usage,
        info=state.info,
        seen_source_events=dict(state.seen_source_events),
        retained_bytes=state.retained_bytes,
        complete=state.complete,
        active_message=state.active_message,
        last_update_name=state.last_update_name,
        implicit_message_ordinals=dict(state.implicit_message_ordinals),
        replaced_state=state,
    )


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


def _normalized_supported_update(
    update_name: str, update: Mapping[str, Any]
) -> dict[str, Any]:
    """Apply ACP v1's tolerant decoding rules to optional/list fields."""

    normalized = _safe_deepcopy(dict(update), label="ACP session update")
    _salvage_meta(normalized)
    if update_name in {
        "user_message_chunk",
        "agent_message_chunk",
        "agent_thought_chunk",
    }:
        content = normalized.get("content")
        if not isinstance(content, Mapping):
            raise AcpProjectionError("ACP message update is missing object content")
        normalized["content"] = _normalized_content_block(
            content, label="ACP message content"
        )
        if "messageId" in normalized and normalized["messageId"] is not None:
            _identifier(normalized["messageId"], "messageId")
    elif update_name == "tool_call":
        normalized = _normalized_tool_call(normalized)
    elif update_name == "tool_call_update":
        normalized = _normalized_tool_update(normalized)
    elif update_name == "plan":
        if "entries" not in normalized:
            raise AcpProjectionError("ACP plan update entries must be an array")
        entries = normalized["entries"]
        if not isinstance(entries, list):
            normalized["entries"] = []
        else:
            accepted: list[dict[str, Any]] = []
            for entry in entries:
                try:
                    accepted.append(_normalized_plan_entry(entry))
                except AcpProjectionError:
                    continue
            normalized["entries"] = accepted
    elif update_name == "usage_update":
        _validate_usage(normalized)
        if normalized.get("cost") is not None:
            try:
                cost = normalized["cost"]
                if not isinstance(cost, Mapping):
                    raise AcpProjectionError("ACP usage cost must be an object or null")
                cost_copy = _safe_deepcopy(dict(cost), label="ACP usage cost")
                _salvage_meta(cost_copy)
                _validate_cost(cost_copy)
                normalized["cost"] = cost_copy
            except (AcpProjectionError, OverflowError):
                normalized.pop("cost", None)
    elif update_name == "session_info_update":
        for key in ("title", "updatedAt"):
            if key in normalized and normalized[key] is not None and not isinstance(
                normalized[key], str
            ):
                normalized.pop(key)
    return normalized


def _salvage_meta(value: dict[str, Any]) -> None:
    meta = value.get("_meta")
    if meta is not None and (
        not isinstance(meta, Mapping) or any(not isinstance(key, str) for key in meta)
    ):
        value.pop("_meta", None)


def _normalized_annotations(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    normalized = _safe_deepcopy(dict(value), label="ACP annotations")
    _salvage_meta(normalized)
    audience = normalized.get("audience")
    if isinstance(audience, list):
        normalized["audience"] = [item for item in audience if item in _ANNOTATION_ROLES]
    elif audience is not None:
        normalized.pop("audience", None)
    if normalized.get("lastModified") is not None and not isinstance(
        normalized.get("lastModified"), str
    ):
        normalized.pop("lastModified", None)
    priority = normalized.get("priority")
    if priority is not None and not _is_finite_number(priority):
        normalized.pop("priority", None)
    return normalized


def _normalized_content_block(
    content: Mapping[str, Any], *, label: str
) -> dict[str, Any]:
    normalized = _safe_deepcopy(dict(content), label=label)
    content_type = normalized.get("type")
    if content_type not in _CONTENT_TYPES:
        raise AcpProjectionError(f"{label} has unsupported type")
    _salvage_meta(normalized)
    annotations = _normalized_annotations(normalized.get("annotations"))
    if annotations is None:
        normalized.pop("annotations", None)
    else:
        normalized["annotations"] = annotations
    if content_type == "text":
        _required_text(normalized, "text", label=label)
    elif content_type in {"image", "audio"}:
        _required_text(normalized, "data", label=label)
        _required_text(normalized, "mimeType", label=label)
        if content_type == "image" and normalized.get("uri") is not None and not isinstance(
            normalized.get("uri"), str
        ):
            normalized.pop("uri", None)
    elif content_type == "resource_link":
        _required_text(normalized, "name", label=label)
        _required_text(normalized, "uri", label=label)
        for key in ("description", "mimeType", "title"):
            if normalized.get(key) is not None and not isinstance(normalized.get(key), str):
                normalized.pop(key, None)
        size = normalized.get("size")
        if size is not None and (
            isinstance(size, bool)
            or not isinstance(size, int)
            or not -(2**63) <= size <= 2**63 - 1
        ):
            normalized.pop("size", None)
    else:
        resource = normalized.get("resource")
        if not isinstance(resource, Mapping):
            raise AcpProjectionError(f"{label} resource must be an object")
        resource_copy = _safe_deepcopy(dict(resource), label=f"{label} resource")
        _salvage_meta(resource_copy)
        _required_text(resource_copy, "uri", label=f"{label} resource")
        has_text = "text" in resource_copy
        has_blob = "blob" in resource_copy
        if has_text == has_blob:
            raise AcpProjectionError(
                f"{label} resource must contain exactly one of text or blob"
            )
        _required_text(
            resource_copy,
            "text" if has_text else "blob",
            label=f"{label} resource",
        )
        if resource_copy.get("mimeType") is not None and not isinstance(
            resource_copy.get("mimeType"), str
        ):
            resource_copy.pop("mimeType", None)
        normalized["resource"] = resource_copy
    return normalized


def _permission_options(options: list[Any]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for option in options:
        if not isinstance(option, Mapping):
            raise AcpProjectionError("ACP permission request option must be an object")
        option_copy = _safe_deepcopy(dict(option), label="ACP permission option")
        _salvage_meta(option_copy)
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


def _normalized_tool_call(update: Mapping[str, Any]) -> dict[str, Any]:
    normalized = _safe_deepcopy(dict(update), label="ACP tool call")
    _required_string(normalized, "toolCallId")
    if not isinstance(normalized.get("title"), str):
        raise AcpProjectionError("ACP tool_call is missing string title")
    _salvage_tool_fields(normalized, creation=True, label="ACP tool call")
    return normalized


def _normalized_tool_update(
    update: Mapping[str, Any], *, label: str = "ACP tool_call_update"
) -> dict[str, Any]:
    normalized = _safe_deepcopy(dict(update), label=label)
    _required_string(normalized, "toolCallId")
    _salvage_tool_fields(normalized, creation=False, label=label)
    return normalized


def _salvage_tool_fields(
    update: dict[str, Any], *, creation: bool, label: str
) -> None:
    _salvage_meta(update)
    kind = update.get("kind")
    if kind is not None and (not isinstance(kind, str) or kind not in _TOOL_KINDS):
        update.pop("kind", None)
    status = update.get("status")
    if status is not None and (
        not isinstance(status, str) or status not in _TOOL_STATUSES
    ):
        update.pop("status", None)
    if not creation and update.get("title") is not None and not isinstance(
        update.get("title"), str
    ):
        update.pop("title", None)
    for key, normalizer in (
        ("content", _normalized_tool_content),
        ("locations", _normalized_tool_location),
    ):
        value = update.get(key)
        if not isinstance(value, list):
            if value is not None:
                update.pop(key, None)
            continue
        accepted: list[dict[str, Any]] = []
        for item in value:
            try:
                accepted.append(normalizer(item))
            except AcpProjectionError:
                continue
        update[key] = accepted


def _normalized_tool_content(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise AcpProjectionError("ACP tool content item must be an object")
    normalized = _safe_deepcopy(dict(value), label="ACP tool content item")
    _salvage_meta(normalized)
    item_type = normalized.get("type")
    if item_type == "content":
        content = normalized.get("content")
        if not isinstance(content, Mapping):
            raise AcpProjectionError("ACP tool content is missing content block")
        normalized["content"] = _normalized_content_block(
            content, label="ACP tool content block"
        )
    elif item_type == "diff":
        path = _required_text(normalized, "path", label="ACP tool diff")
        if not os.path.isabs(path):
            raise AcpProjectionError("ACP tool diff path must be absolute")
        _required_text(normalized, "newText", label="ACP tool diff")
        if normalized.get("oldText") is not None and not isinstance(
            normalized.get("oldText"), str
        ):
            normalized.pop("oldText", None)
    elif item_type == "terminal":
        _required_string(normalized, "terminalId")
    else:
        raise AcpProjectionError("ACP tool content item has unsupported type")
    return normalized


def _normalized_tool_location(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise AcpProjectionError("ACP tool location must be an object")
    normalized = _safe_deepcopy(dict(value), label="ACP tool location")
    _salvage_meta(normalized)
    path = _required_text(normalized, "path", label="ACP tool location")
    if not os.path.isabs(path):
        raise AcpProjectionError("ACP tool location path must be absolute")
    line = normalized.get("line")
    if line is not None and (
        isinstance(line, bool)
        or not isinstance(line, int)
        or not 0 <= line <= 2**32 - 1
    ):
        normalized.pop("line", None)
    return normalized


def _normalized_plan_entry(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise AcpProjectionError("ACP plan entry must be an object")
    content = value.get("content")
    priority = value.get("priority")
    status = value.get("status")
    if not isinstance(content, str):
        raise AcpProjectionError("ACP plan entry is missing string content")
    if priority not in _PLAN_PRIORITIES:
        raise AcpProjectionError("ACP plan entry has invalid priority")
    if status not in _PLAN_STATUSES:
        raise AcpProjectionError("ACP plan entry has invalid status")
    normalized = {"content": content, "priority": priority, "status": status}
    meta = _extension_metadata(value)
    if meta:
        normalized["_meta"] = meta
    return normalized


def _validate_usage(update: Mapping[str, Any]) -> None:
    for key in ("used", "size"):
        value = update.get(key)
        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or not 0 <= value <= 2**64 - 1
        ):
            raise AcpProjectionError(f"ACP usage {key} must be a u64")


def _validate_cost(cost: Any) -> None:
    if not isinstance(cost, Mapping):
        raise AcpProjectionError("ACP usage cost must be an object or null")
    amount = cost.get("amount")
    if not _is_finite_number(amount):
        raise AcpProjectionError("ACP usage cost amount must be a finite number")
    if not isinstance(cost.get("currency"), str):
        raise AcpProjectionError("ACP usage cost currency must be text")
    _extension_metadata(cost)


def _is_finite_number(value: Any) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    try:
        return math.isfinite(float(value))
    except (ValueError, OverflowError):
        return False


def _required_text(value: Mapping[str, Any], key: str, *, label: str) -> str:
    item = value.get(key)
    if not isinstance(item, str):
        raise AcpProjectionError(f"{label} is missing string {key}")
    return item


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
        dedupe_material,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    privacy = "session"
    private_fields: list[str] = []
    if kind == "thought":
        privacy = "private"
        private_fields = ["payload"]
    elif kind == "extension":
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
    "AcpProjectionCheckpoint",
    "AcpProjectionError",
    "SUPPORTED_EVENT_KINDS",
]
