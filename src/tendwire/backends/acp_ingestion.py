"""Durable ACP ingestion bound to one authenticated Tendwire worker.

The transport, semantic projector, and SQLite journal are deliberately separate.
This module is the narrow authority bridge: it binds one ACP session generation
to one private Herdr worker binding, records every accepted semantic event, and
projects only user/assistant text into the existing turn model.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..config import Config
from ..core.agent_events import AgentEvent, agent_event
from ..core.models import WorkerBinding, stable_fingerprint
from ..store.sqlite import (
    AppendBoundAgentEventResult,
    AppendProjectedAgentEventResult,
    TurnRefreshApplyResult,
    append_agent_event_and_apply_turn_for_binding,
)
from .acp_projection import AcpEventProjector, AcpProjectionCheckpoint
from .acp_protocol import StopReason


PersistEvent = Callable[..., AppendProjectedAgentEventResult]


@dataclass(frozen=True)
class AcpIngestionResult:
    """Outcome of accepting, ignoring, or projecting one ACP event."""

    kind: str | None
    event: AppendBoundAgentEventResult | None = None
    turn: TurnRefreshApplyResult | None = None
    ignored_reason: str | None = None


class AcpSessionIngestor:
    """Ingest one ACP session generation for one private worker binding.

    ``stream_generation`` must change whenever a transport is recreated. ACP v1
    does not require a stable event ID for notifications, so generation-scoped
    synthetic IDs avoid corrupting the append-only journal. Notifications with
    authoritative source IDs still deduplicate across reconnects.
    """

    def __init__(
        self,
        config: Config,
        *,
        session_id: str,
        stream_generation: str,
        binding: WorkerBinding,
        projector: AcpEventProjector | None = None,
        persist_event: PersistEvent = append_agent_event_and_apply_turn_for_binding,
    ) -> None:
        if config.db_path is None:
            raise ValueError("ACP ingestion requires a sqlite db path")
        if not isinstance(session_id, str) or not session_id.strip():
            raise ValueError("ACP session and stream generation are required")
        if not isinstance(stream_generation, str) or not stream_generation.strip():
            raise ValueError("ACP session and stream generation are required")
        if binding.host_id != config.host_id:
            raise ValueError("ACP binding host does not match configuration")
        if not binding.private_fingerprint:
            raise ValueError("ACP ingestion requires an authenticated private binding")
        if (
            binding.turn_target_kind != "acp_session_id"
            or binding.turn_target_value != session_id.strip()
        ):
            raise ValueError("ACP session does not match the private worker binding")
        if config.agent_event_source == "legacy":
            raise ValueError("ACP ingestion is disabled by agent_event_source=legacy")
        self.config = config
        self.session_id = session_id.strip()
        self.stream_generation = stream_generation.strip()
        self.binding = binding
        self.projector = projector or AcpEventProjector()
        self._persist_event = persist_event
        self._turn_ordinal = 0
        self._source_turn_id: str | None = None
        self._turn_complete = False

    @property
    def source_turn_id(self) -> str | None:
        """Return the opaque public-safe identity of the active ACP turn."""

        return self._source_turn_id

    def start_turn(self, *, producer_turn_id: str | None = None) -> str:
        """Reset message assembly and allocate one opaque turn identity."""

        if producer_turn_id is not None and (
            not isinstance(producer_turn_id, str) or not producer_turn_id.strip()
        ):
            raise ValueError("producer_turn_id must be non-empty text or None")
        self._turn_ordinal += 1
        self.projector.reset_turn(self.session_id)
        # An authoritative producer turn ID must retain identity across ACP
        # transport recreation.  Generation only scopes locally synthesized
        # ordinals, whose meaning cannot survive a reconnect.
        identity = (
            {
                "source": "acp",
                "session": self.session_id,
                "producer_turn": producer_turn_id.strip(),
            }
            if producer_turn_id is not None
            else {
                "source": "acp",
                "session": self.session_id,
                "generation": self.stream_generation,
                "turn": self._turn_ordinal,
            }
        )
        self._source_turn_id = f"acpt_{stable_fingerprint(identity)}"
        self._turn_complete = False
        return self._source_turn_id

    def ingest_update(
        self,
        notification: Mapping[str, Any],
        *,
        source_event_id: str | None = None,
        replay: bool = False,
        setup_replay: bool = False,
    ) -> AcpIngestionResult:
        """Normalize, journal, and conditionally project ``session/update``."""

        mismatch = _notification_mismatch(
            notification,
            method="session/update",
            session_id=self.session_id,
        )
        if mismatch is not None:
            return AcpIngestionResult(None, ignored_reason=mismatch)
        update_kind = _session_update_kind(notification)
        if self._turn_complete and update_kind in _TURN_SCOPED_UPDATES:
            return AcpIngestionResult(None, ignored_reason="turn_already_complete")
        thought_rejection = _thought_rejection_reason(
            notification,
            policy=self.config.acp_thought_policy,
        )
        if thought_rejection is not None:
            return AcpIngestionResult("thought", ignored_reason=thought_rejection)
        checkpoint = self.projector.checkpoint_session(self.session_id)
        prior_turn_state = self._turn_state()
        if self._source_turn_id is None and update_kind in _TURN_SCOPED_UPDATES:
            self.start_turn()
        try:
            canonical = self.projector.normalize_session_update(
                notification,
                source_event_id=source_event_id,
                replay=replay,
            )
        except BaseException:
            self._restore_speculation(checkpoint, prior_turn_state)
            raise
        if canonical is None:
            self._restore_speculation(checkpoint, prior_turn_state)
            return AcpIngestionResult(None, ignored_reason="unsupported_or_duplicate")
        return self._accept(
            canonical,
            checkpoint=checkpoint,
            prior_turn_state=prior_turn_state,
            project_turn=not setup_replay,
            replay_namespace="load" if setup_replay else None,
        )

    def begin_prompt(
        self,
        prompt: Sequence[Mapping[str, Any]],
        *,
        producer_turn_id: str | None = None,
    ) -> AcpIngestionResult:
        """Durably record outgoing prompt content before transport send."""

        blocks = [dict(block) for block in prompt]
        if not blocks:
            raise ValueError("prompt must contain at least one content block")
        checkpoint = self.projector.checkpoint_session(self.session_id)
        prior_turn_state = self._turn_state()
        source_turn_id = self.start_turn(producer_turn_id=producer_turn_id)
        text = "\n".join(
            str(block.get("text"))
            for block in blocks
            if block.get("type") == "text" and isinstance(block.get("text"), str)
        )
        source_event_id = f"prompt-input:{source_turn_id}"
        try:
            canonical = self.projector.normalize_session_update(
                {
                    "method": "session/update",
                    "params": {
                        "sessionId": self.session_id,
                        "update": {
                            "sessionUpdate": "user_message_chunk",
                            "messageId": source_event_id,
                            "content": {"type": "text", "text": text},
                        },
                    },
                },
                source_event_id=source_event_id,
                replay=False,
            )
            if canonical is None:  # pragma: no cover - fresh turn invariant
                raise RuntimeError("outgoing ACP prompt was unexpectedly duplicated")
            payload = canonical.get("payload")
            if not isinstance(payload, Mapping):  # pragma: no cover - projector invariant
                raise RuntimeError("outgoing ACP prompt projection is invalid")
            canonical = {
                **canonical,
                "payload": {
                    **payload,
                    "prompt_content": deepcopy(blocks),
                    "outgoing": True,
                },
            }
        except BaseException:
            self._restore_speculation(checkpoint, prior_turn_state)
            raise
        return self._accept(
            canonical,
            checkpoint=checkpoint,
            prior_turn_state=prior_turn_state,
        )

    def reset_after_load(self) -> None:
        """Drop replay turn assembly before accepting a new active prompt."""

        self.projector.reset_turn(self.session_id)
        self._source_turn_id = None
        self._turn_complete = False

    def ingest_permission_request(
        self,
        request: Mapping[str, Any],
        *,
        source_event_id: str | None = None,
        replay: bool = False,
        setup_replay: bool = False,
    ) -> AcpIngestionResult:
        """Journal a permission request as a private tool lifecycle update."""

        mismatch = _notification_mismatch(
            request,
            method="session/request_permission",
            session_id=self.session_id,
        )
        if mismatch is not None:
            return AcpIngestionResult(None, ignored_reason=mismatch)
        if self._turn_complete:
            return AcpIngestionResult(None, ignored_reason="turn_already_complete")
        checkpoint = self.projector.checkpoint_session(self.session_id)
        prior_turn_state = self._turn_state()
        if self._source_turn_id is None:
            self.start_turn()
        try:
            canonical = self.projector.normalize_permission_request(
                request,
                source_event_id=source_event_id,
                replay=replay,
            )
        except BaseException:
            self._restore_speculation(checkpoint, prior_turn_state)
            raise
        if canonical is None:
            self._restore_speculation(checkpoint, prior_turn_state)
            return AcpIngestionResult(None, ignored_reason="duplicate")
        return self._accept(
            canonical,
            checkpoint=checkpoint,
            prior_turn_state=prior_turn_state,
            project_turn=not setup_replay,
            replay_namespace="load" if setup_replay else None,
        )

    def mark_prompt_complete(
        self,
        stop_reason: StopReason | str = StopReason.END_TURN,
    ) -> AcpIngestionResult:
        """Durably finalize the current turn after ``session/prompt`` returns."""

        if self._source_turn_id is None:
            return AcpIngestionResult(None, ignored_reason="no_active_turn")
        if self._turn_complete:
            return AcpIngestionResult(None, ignored_reason="turn_already_complete")
        checkpoint = self.projector.checkpoint_session(self.session_id)
        prior_turn_state = self._turn_state()
        try:
            try:
                normalized_reason = StopReason(stop_reason)
            except ValueError as exc:
                raise ValueError("unsupported ACP prompt stop reason") from exc
            content = self.projector.mark_turn_complete(self.session_id)
            content["source_turn_id"] = self._source_turn_id
            content["assistant_final_text"] = _final_text_for_stop_reason(
                str(content.get("assistant_final_text") or ""),
                normalized_reason,
            )
            marker = agent_event(
                kind="extension",
                source="acp",
                worker_id=self.binding.worker_id,
                payload={
                    "schema_version": 1,
                    "extension": "tendwire.acp.prompt_completion",
                    "complete": True,
                    "stop_reason": normalized_reason.value,
                    "outcome": _STOP_REASON_OUTCOMES[normalized_reason],
                    "projection": content,
                },
                source_session_id=self.session_id,
                source_turn_id=self._source_turn_id,
                source_event_id=f"prompt-complete:{self._source_turn_id}",
                visibility="private",
            )
            persisted = self._persist_event(
                Path(self.config.db_path),
                self.config.host_id,
                marker,
                expected_binding=self.binding,
                content=(
                    None
                    if self.config.agent_event_source == "acp_shadow"
                    else content
                ),
                observed_at=marker.observed_at,
                turn_model=self.config.turn_model,
            )
        except BaseException:
            self._restore_speculation(checkpoint, prior_turn_state)
            raise
        if persisted.event.status == "binding_changed":
            self._restore_speculation(checkpoint, prior_turn_state)
            return AcpIngestionResult(
                "extension",
                event=persisted.event,
                ignored_reason="stale_binding",
            )
        self._turn_complete = True
        return AcpIngestionResult(
            "extension",
            event=persisted.event,
            turn=persisted.turn,
            ignored_reason=(
                "duplicate_event"
                if persisted.event.status == "replayed"
                else None
            ),
        )

    def _accept(
        self,
        canonical: Mapping[str, Any],
        *,
        checkpoint: AcpProjectionCheckpoint,
        prior_turn_state: tuple[int, str | None, bool],
        project_turn: bool = True,
        replay_namespace: str | None = None,
    ) -> AcpIngestionResult:
        kind = str(canonical.get("kind") or "")
        if kind == "thought" and self.config.acp_thought_policy == "disabled":
            self._restore_speculation(checkpoint, prior_turn_state)
            return AcpIngestionResult(kind, ignored_reason="thought_policy_disabled")
        try:
            payload = canonical.get("payload")
            if not isinstance(payload, Mapping):
                raise ValueError("canonical ACP event payload must be a mapping")
            sequence = canonical.get("sequence")
            if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 0:
                raise ValueError("canonical ACP event sequence must be nonnegative")
            explicit_event_id = canonical.get("source_event_id")
            source_id = (
                str(explicit_event_id)
                if explicit_event_id is not None and str(explicit_event_id)
                else (
                    _replay_source_event_id(
                        replay_namespace,
                        session_id=self.session_id,
                        sequence=sequence,
                        kind=kind,
                        payload=payload,
                    )
                    if replay_namespace is not None
                    else f"stream:{self.stream_generation}:{sequence}"
                )
            )
            event = agent_event(
                kind=kind,
                source="acp",
                worker_id=self.binding.worker_id,
                payload=payload,
                source_session_id=self.session_id,
                source_turn_id=self._source_turn_id,
                source_item_id=_source_item_id(kind, payload),
                source_message_id=_source_message_id(kind, payload),
                source_event_id=source_id,
                source_sequence=sequence,
                # The complete structured journal is private initially. Public and
                # connector views require a separate explicit sanitizing projection.
                visibility="private",
            )
            projection: Mapping[str, Any] | None = None
            if (
                kind in {"user_message", "agent_message"}
                and self.config.agent_event_source != "acp_shadow"
                and project_turn
            ):
                content = self.projector.project_turn_content(self.session_id)
                if self._source_turn_id is not None:
                    content["source_turn_id"] = self._source_turn_id
                projection = content
            persisted = self._persist_event(
                Path(self.config.db_path),
                self.config.host_id,
                event,
                expected_binding=self.binding,
                content=projection,
                observed_at=event.observed_at,
                turn_model=self.config.turn_model,
            )
        except BaseException:
            self._restore_speculation(checkpoint, prior_turn_state)
            raise
        appended = persisted.event
        # A durable replay on a newly constructed ingestor is also the
        # reconstruction path for its in-memory projector.  Keep that state so
        # prompt completion can finalize the recovered text.  Only a stale
        # binding invalidates the speculative normalization.
        if appended.status == "binding_changed":
            self._restore_speculation(checkpoint, prior_turn_state)
        turn = persisted.turn
        return AcpIngestionResult(
            kind,
            event=appended,
            turn=turn,
            ignored_reason=(
                "stale_binding"
                if appended.status == "binding_changed"
                or (turn is not None and turn.stale_binding)
                else "duplicate_event"
                if appended.status == "replayed"
                else None
            ),
        )

    def _turn_state(self) -> tuple[int, str | None, bool]:
        return self._turn_ordinal, self._source_turn_id, self._turn_complete

    def _restore_speculation(
        self,
        checkpoint: AcpProjectionCheckpoint,
        prior_turn_state: tuple[int, str | None, bool],
    ) -> None:
        self.projector.restore_session(checkpoint)
        self._turn_ordinal, self._source_turn_id, self._turn_complete = prior_turn_state


_STOP_REASON_OUTCOMES = {
    StopReason.END_TURN: "completed",
    StopReason.MAX_TOKENS: "truncated_max_tokens",
    StopReason.MAX_TURN_REQUESTS: "truncated_max_turn_requests",
    StopReason.REFUSAL: "refused",
    StopReason.CANCELLED: "cancelled",
}

_STOP_REASON_NOTICES = {
    StopReason.MAX_TOKENS: "[ACP response truncated: token limit reached]",
    StopReason.MAX_TURN_REQUESTS: "[ACP response truncated: request limit reached]",
    StopReason.REFUSAL: "[ACP agent refused the request]",
    StopReason.CANCELLED: "[ACP prompt cancelled]",
}


def _final_text_for_stop_reason(text: str, stop_reason: StopReason) -> str:
    notice = _STOP_REASON_NOTICES.get(stop_reason)
    if notice is None:
        return text
    return f"{text}\n\n{notice}" if text else notice


def _replay_source_event_id(
    namespace: str,
    *,
    session_id: str,
    sequence: int,
    kind: str,
    payload: Mapping[str, Any],
) -> str:
    fingerprint = stable_fingerprint(
        {
            "session": session_id,
            "sequence": sequence,
            "kind": kind,
            "payload": payload,
        }
    )
    return f"{namespace}:{fingerprint}"


def _source_message_id(kind: str, payload: Mapping[str, Any]) -> str | None:
    if kind not in {"user_message", "agent_message", "thought"}:
        return None
    value = payload.get("message_id")
    return str(value) if value is not None and str(value) else None


def _source_item_id(kind: str, payload: Mapping[str, Any]) -> str | None:
    if kind not in {"tool_call", "tool_call_update"}:
        return None
    value = payload.get("tool_call_id")
    return str(value) if value is not None and str(value) else None


_TURN_SCOPED_UPDATES = frozenset(
    {
        "user_message_chunk",
        "agent_message_chunk",
        "agent_thought_chunk",
        "tool_call",
        "tool_call_update",
        "plan",
    }
)
_THOUGHT_RAW_LABELS = frozenset(
    {"raw", "reasoning", "raw_reasoning", "raw-reasoning", "chain_of_thought"}
)


def _params(value: Mapping[str, Any]) -> Mapping[str, Any]:
    params = value.get("params")
    return params if isinstance(params, Mapping) else value


def _notification_mismatch(
    value: Mapping[str, Any],
    *,
    method: str,
    session_id: str,
) -> str | None:
    supplied_method = value.get("method")
    if supplied_method is not None and supplied_method != method:
        return "method_mismatch"
    supplied_session = _params(value).get("sessionId")
    if supplied_session != session_id:
        return "session_mismatch"
    return None


def _session_update(value: Mapping[str, Any]) -> Mapping[str, Any] | None:
    update = _params(value).get("update")
    return update if isinstance(update, Mapping) else None


def _session_update_kind(value: Mapping[str, Any]) -> str | None:
    update = _session_update(value)
    kind = update.get("sessionUpdate") if update is not None else None
    return kind if isinstance(kind, str) else None


def _thought_classification(value: Mapping[str, Any]) -> str | None:
    update = _session_update(value)
    if update is None or update.get("sessionUpdate") != "agent_thought_chunk":
        return None
    candidates: list[Mapping[str, Any]] = []
    for container in (update, update.get("content")):
        if not isinstance(container, Mapping):
            continue
        meta = container.get("_meta")
        if not isinstance(meta, Mapping):
            continue
        candidates.append(meta)
        tendwire = meta.get("tendwire")
        if isinstance(tendwire, Mapping):
            candidates.insert(0, tendwire)
    for meta in candidates:
        for key in ("thought_kind", "thoughtKind", "reasoning_kind", "reasoningKind"):
            label = meta.get(key)
            if isinstance(label, str) and label.strip():
                return label.strip().lower()
    return "unclassified"


def _thought_rejection_reason(
    value: Mapping[str, Any],
    *,
    policy: str,
) -> str | None:
    classification = _thought_classification(value)
    if classification is None:
        return None
    if policy == "disabled":
        return "thought_policy_disabled"
    if policy == "private_summary" and classification in _THOUGHT_RAW_LABELS:
        return "thought_policy_requires_summary"
    return None


__all__ = ["AcpIngestionResult", "AcpSessionIngestor"]
