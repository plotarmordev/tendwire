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
    seen_source_events: set[str] = field(default_factory=set)
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

    def __init__(self) -> None:
        self._sessions: dict[str, _SessionState] = {}

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

        state = self._sessions.setdefault(session_id, _SessionState())
        explicit_id = source_event_id or _source_event_id(notification, params, update)
        if explicit_id is not None:
            scoped_id = f"{session_id}:{explicit_id}"
            if scoped_id in state.seen_source_events:
                return None

        if kind in _MESSAGE_KINDS:
            payload = self._normalize_message(state, kind, update)
        elif kind in {"tool_call", "tool_call_update"}:
            payload = self._normalize_tool(state, kind, update)
        elif kind == "plan":
            payload = self._normalize_plan(state, update)
        elif kind == "usage":
            payload = self._normalize_usage(state, update)
        else:
            payload = self._normalize_session_info(state, update)

        state.sequence += 1
        if explicit_id is not None:
            state.seen_source_events.add(f"{session_id}:{explicit_id}")
        state.complete = False
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

        state = self._sessions.setdefault(session_id, _SessionState())
        explicit_id = source_event_id or _jsonrpc_request_id(request) or _source_event_id(
            request, params, tool_call
        )
        if explicit_id is not None:
            scoped_id = f"{session_id}:{explicit_id}"
            if scoped_id in state.seen_source_events:
                return None

        snapshot = _merge_tool_snapshot(state.tools.get(tool_call_id), tool_call)
        options = params.get("options", [])
        if not isinstance(options, list):
            options = []
        snapshot["permission"] = {
            "required": True,
            "options": [deepcopy(option) for option in options if isinstance(option, Mapping)],
        }
        state.tools[tool_call_id] = snapshot
        state.sequence += 1
        if explicit_id is not None:
            state.seen_source_events.add(f"{session_id}:{explicit_id}")
        payload = {
            "tool_call_id": tool_call_id,
            "changes": _without_discriminator(tool_call),
            "snapshot": deepcopy(snapshot),
            "permission": deepcopy(snapshot["permission"]),
        }
        return _canonical_event(
            session_id=session_id,
            sequence=state.sequence,
            kind="tool_call_update",
            payload=payload,
            source_event_id=explicit_id,
            replay=replay,
            original_update={"sessionUpdate": "tool_call_update", **dict(tool_call)},
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
        is_complete = state.complete if complete is None else bool(complete)
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

        state = self._sessions.setdefault(session_id, _SessionState())
        state.complete = True
        return self.project_turn_content(session_id)

    def reset_turn(self, session_id: str) -> None:
        """Start a fresh prompt turn while preserving session-level ACP state."""

        state = self._sessions.setdefault(session_id, _SessionState())
        state.messages = {kind: [] for kind in _MESSAGE_KINDS}
        state.complete = False

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

    @staticmethod
    def _normalize_message(
        state: _SessionState,
        kind: str,
        update: Mapping[str, Any],
    ) -> dict[str, Any]:
        content = update.get("content")
        if not isinstance(content, Mapping):
            raise AcpProjectionError(f"ACP {kind} update is missing content")
        message_id_value = update.get("messageId")
        assemblies = state.messages[kind]
        message_id = (
            message_id_value
            if isinstance(message_id_value, str) and message_id_value
            else assemblies[-1].message_id
            if assemblies
            else f"implicit-{kind}-1"
        )
        if not assemblies or assemblies[-1].message_id != message_id:
            assemblies.append(_MessageAssembly(message_id=message_id))
        text_delta = content.get("text") if content.get("type") == "text" else None
        if not isinstance(text_delta, str):
            text_delta = ""
        assemblies[-1].text += text_delta
        return {
            "message_id": message_id,
            "content": deepcopy(dict(content)),
            "text_delta": text_delta,
            "assembled_text": assemblies[-1].text,
            "message_index": len(assemblies) - 1,
        }

    @staticmethod
    def _normalize_tool(
        state: _SessionState,
        kind: str,
        update: Mapping[str, Any],
    ) -> dict[str, Any]:
        tool_call_id = _required_string(update, "toolCallId")
        previous = state.tools.get(tool_call_id)
        snapshot = _merge_tool_snapshot(previous, update)
        state.tools[tool_call_id] = snapshot
        payload: dict[str, Any] = {
            "tool_call_id": tool_call_id,
            "snapshot": deepcopy(snapshot),
        }
        if kind == "tool_call_update":
            payload["changes"] = _without_discriminator(update)
        return payload

    @staticmethod
    def _normalize_plan(
        state: _SessionState, update: Mapping[str, Any]
    ) -> dict[str, Any]:
        entries = update.get("entries", [])
        if not isinstance(entries, list):
            entries = []
        state.plan = [deepcopy(dict(entry)) for entry in entries if isinstance(entry, Mapping)]
        return {"entries": deepcopy(state.plan), "snapshot": True}

    @staticmethod
    def _normalize_usage(
        state: _SessionState, update: Mapping[str, Any]
    ) -> dict[str, Any]:
        state.usage.update(_without_discriminator(update))
        return deepcopy(state.usage)

    @staticmethod
    def _normalize_session_info(
        state: _SessionState, update: Mapping[str, Any]
    ) -> dict[str, Any]:
        # Presence is meaningful: explicit null clears an existing property.
        state.info.update(_without_discriminator(update))
        return deepcopy(state.info)


def _unwrap_params(value: Mapping[str, Any]) -> Mapping[str, Any]:
    params = value.get("params")
    if isinstance(params, Mapping):
        return params
    return value


def _required_string(value: Mapping[str, Any], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise AcpProjectionError(f"ACP value is missing non-empty {key}")
    return item


def _source_event_id(*values: Mapping[str, Any]) -> str | None:
    for value in values:
        for key in ("eventId", "event_id", "notificationId", "notification_id"):
            candidate = value.get(key)
            if isinstance(candidate, (str, int)) and str(candidate):
                return str(candidate)
        meta = value.get("_meta")
        if isinstance(meta, Mapping):
            for key in ("eventId", "event_id", "notificationId", "notification_id"):
                candidate = meta.get(key)
                if isinstance(candidate, (str, int)) and str(candidate):
                    return str(candidate)
    return None


def _jsonrpc_request_id(value: Mapping[str, Any]) -> str | None:
    """Return a request ID, but never mistake a notification field for one."""

    if value.get("method") != "session/request_permission":
        return None
    candidate = value.get("id")
    if isinstance(candidate, (str, int)) and str(candidate):
        return str(candidate)
    return None


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
