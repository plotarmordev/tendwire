"""Opt-in Herdr socket event backend and reconciliation layer.

This module is intentionally imported only from the explicit socket backend
path. It reuses the PR8 socket client for transport and the Herdr CLI adapter's
projection helpers for Tendwire model normalization.
"""

from __future__ import annotations

import inspect
import logging
import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..config import Config
from ..core.models import (
    BackendHealth,
    Snapshot,
    Space,
    Worker,
    WorkerBinding,
    normalize_status,
    utc_timestamp,
)
from ..worker_identity import (
    InstallationKeyError,
    STABLE_KEY_VERSION,
    canonical_herdr_pane_identity,
    is_stable_worker_key,
)
from ..core.projector import project_from_observations
from ..store.sqlite import (
    HerdrTurnWatermark,
    SnapshotObservationContext,
    SnapshotRetentionPolicy,
    expire_stale_worker_bindings,
    expire_worker_bindings,
    get_herdr_turn_watermark,
    latest_snapshot,
    list_worker_bindings,
    maybe_run_automatic_store_maintenance,
    record_herdr_turn_completeness_break,
    record_herdr_turn_completion,
    save_snapshot,
    set_herdr_turn_watermark,
    upsert_worker_bindings,
)
from .herdr_cli import (
    HerdrContinuityUnavailableError,
    _pane_has_agent,
    _payload_items,
    _spaces_from_payload,
    _records_from_agent_and_pane_payloads,
    _worker_record_from_item,
    _workers_and_bindings_from_records,
    _strip_stable_key_fields,
    herdr_backend_health,
)
from .herdr_protocol import (
    HERDR_EVENTS_SUBSCRIBE_METHOD,
    HERDR_OFFICIAL_EVENT_NAMES,
    HERDR_TURN_COMPLETED_EVENT_NAME,
    HerdrEnvelopeError,
    HerdrErrorResponse,
    HerdrMalformedLineError,
    HerdrProtocolError,
    build_events_subscribe_params,
)
from .herdr_socket import (
    HerdrSocketClient,
    HerdrSocketConnectionError,
    HerdrSocketDisconnectedError,
    HerdrSocketTimeoutError,
)


BACKEND_NAME = "herdr"
DEFAULT_SUBSCRIBE_METHOD = HERDR_EVENTS_SUBSCRIBE_METHOD
DEFAULT_DEBOUNCE_SECONDS = 0.05
DEFAULT_DEDUPE_SIZE = 512
DEFAULT_MAX_BATCH_SIZE = 64
DEFAULT_RECONNECT_DELAY_SECONDS = 0.25
_LOGGER = logging.getLogger(__name__)

_AGENT_PAYLOAD_KEYS = ("agents", "workers", "data", "items", "results", "result")
_PANE_PAYLOAD_KEYS = ("panes", "items", "data", "results", "result")
_SUPPORTED_EVENT_NAMES = (
    *HERDR_OFFICIAL_EVENT_NAMES,
    HERDR_TURN_COMPLETED_EVENT_NAME,
)
_SUPPORTED_EVENT_NAME_SET = frozenset(_SUPPORTED_EVENT_NAMES)
_HERDR_074_EVENT_NAMES = tuple(
    event_name
    for event_name in _SUPPORTED_EVENT_NAMES
    if event_name not in {"pane.updated", "pane.turn_completed"}
)
_HERDR_074_PANE_SCOPED_REPLAY_EVENT_NAMES = frozenset(
    {
        "workspace.focused",
        "pane.focused",
        "pane.agent_detected",
        "pane.output_matched",
    }
)
_HERDR_074_PANE_SCOPED_FALLBACK_EVENT_NAMES = tuple(
    event_name
    for event_name in _HERDR_074_EVENT_NAMES
    if event_name not in _HERDR_074_PANE_SCOPED_REPLAY_EVENT_NAMES
)
_PARAMETERIZED_EVENT_NAMES = frozenset(
    {
        "pane.agent_status_changed",
        "pane.output_matched",
        "pane.turn_completed",
    }
)
_GLOBAL_EVENT_NAMES = tuple(
    event_name
    for event_name in _SUPPORTED_EVENT_NAMES
    if event_name not in _PARAMETERIZED_EVENT_NAMES
)
_CLOSED_EVENT_NAMES = frozenset({"pane.closed", "pane.exited"})
_SPACE_EVENT_NAMES = frozenset(
    {
        "workspace.created",
        "workspace.updated",
        "workspace.renamed",
        "workspace.closed",
        "workspace.focused",
    }
)
_WORKTREE_EVENT_NAMES = frozenset({"worktree.created", "worktree.opened", "worktree.removed"})
# ``pane.updated`` is normalized from Herdr 0.7.5's scalar
# ``PaneOutputChanged`` event. It is a turn-refresh notification, not a
# PaneInfo observation, and therefore must never rebuild worker identity.
_PANE_WORKER_EVENT_NAMES = frozenset({"pane.created", "pane.focused"})
_TURN_REFRESH_EVENT_NAMES = frozenset(
    {
        "pane.created",
        "pane.updated",
        "pane.focused",
        "pane.moved",
        "pane.closed",
        "pane.exited",
        "pane.agent_detected",
        "pane.agent_status_changed",
        "pane.output_matched",
    }
)
_COMPLETED_TURN_REFRESH_STATUSES = frozenset({"updated", "unchanged", "missing"})


class HerdrEventBackendError(Exception):
    """Base error for the opt-in Herdr socket event backend."""


@dataclass(frozen=True)
class HerdrEventBackendHealth:
    """Small in-memory health state for the Herdr socket backend."""

    status: str
    outcome: str
    observed_at: str
    message: str

    def to_backend_health(
        self,
        *,
        spaces: Sequence[Space] | None = None,
        workers: Sequence[Worker] | None = None,
    ) -> BackendHealth:
        return herdr_backend_health(
            self.outcome,
            observed_at=self.observed_at,
            message=self.message,
            spaces=spaces or [],
            workers=workers or [],
        )


@dataclass(frozen=True)
class HerdrEventId:
    """Forward-compatible authoritative producer event identifier."""

    value: str


@dataclass(frozen=True)
class HerdrProducerSequence:
    """Forward-compatible producer-scoped sequence identifier."""

    producer_id: str
    sequence: str


HerdrProducerIdentity = HerdrEventId | HerdrProducerSequence


@dataclass(frozen=True)
class NormalizedHerdrEvent:
    """A validated Herdr event with optional durable producer identity."""

    name: str
    payload: Mapping[str, Any]
    producer_identity: HerdrProducerIdentity | None


@dataclass(frozen=True)
class HerdrTurnCompletionRecord:
    """Validated completion metadata; semantic content still comes from adapters."""

    pane_id: str
    turn: int
    turn_epoch: int
    outcome: str
    completed_unix_ms: int
    message: str | None = None
    message_truncated: bool = False
    agent_session_path: str | None = None


@dataclass(frozen=True)
class HerdrPaneTurnsReplay:
    pane_id: str
    turn_epoch: int
    records: tuple[HerdrTurnCompletionRecord, ...]
    truncated: bool
    oldest_available: int | None

    @property
    def newest_turn(self) -> int:
        return max((record.turn for record in self.records), default=0)


def _compact_key(value: object) -> str:
    return str(value).strip().lower().replace("-", "_").replace(".", "_").replace(":", "_")


def _field_value(item: Mapping[str, Any], expected_key: str) -> Any:
    expected = _compact_key(expected_key)
    for key, value in item.items():
        if _compact_key(key) == expected:
            return value
    return None


def _first_text(item: Mapping[str, Any], keys: Iterable[str]) -> str | None:
    for key in keys:
        value = _field_value(item, key)
        if value is None:
            continue
        if isinstance(value, Mapping):
            nested = _first_text(value, ("id", "value", "name", "label"))
            if nested:
                return nested
            continue
        if isinstance(value, (str, int, float, bool)):
            text = str(value)
            if text:
                return text
    return None


def _safe_mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _call_with_optional_keywords(
    callback: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: Mapping[str, Any],
) -> Any:
    """Invoke once, omitting optional keywords only when the signature requires it."""
    try:
        signature = inspect.signature(callback)
    except (TypeError, ValueError):
        return callback(*args, **dict(kwargs))
    try:
        signature.bind(*args, **dict(kwargs))
    except TypeError:
        return callback(*args)
    return callback(*args, **dict(kwargs))


def _nonnegative_protocol_integer(value: Any, field: str) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < 0
        or value > (1 << 63) - 1
    ):
        raise HerdrEnvelopeError(f"invalid Herdr {field}")
    return int(value)


def _turn_completion_record(
    value: Any,
    *,
    pane_id: str | None = None,
) -> HerdrTurnCompletionRecord:
    if not isinstance(value, Mapping):
        raise HerdrEnvelopeError("invalid Herdr turn completion record")
    pane = value.get("pane")
    resolved_pane_id = pane_id or (
        _first_text(pane, ("pane_id", "paneId", "id"))
        if isinstance(pane, Mapping)
        else None
    ) or _first_text(value, ("pane_id", "paneId"))
    if not resolved_pane_id:
        raise HerdrEnvelopeError("Herdr turn completion is missing pane_id")
    outcome = value.get("outcome")
    if outcome not in {"completed", "aborted"}:
        raise HerdrEnvelopeError("invalid Herdr turn completion outcome")
    message = value.get("message")
    if message is not None and not isinstance(message, str):
        raise HerdrEnvelopeError("invalid Herdr turn completion message")
    if isinstance(message, str) and len(message.encode("utf-8")) > 8 * 1024:
        raise HerdrEnvelopeError("Herdr turn completion message is too large")
    message_truncated = value.get("message_truncated", False)
    if not isinstance(message_truncated, bool):
        raise HerdrEnvelopeError("invalid Herdr message_truncated")
    agent_session_path = value.get("agent_session_path")
    if agent_session_path is not None and not isinstance(agent_session_path, str):
        raise HerdrEnvelopeError("invalid Herdr agent_session_path")
    return HerdrTurnCompletionRecord(
        pane_id=str(resolved_pane_id),
        turn=_nonnegative_protocol_integer(value.get("turn"), "turn"),
        turn_epoch=_nonnegative_protocol_integer(
            value.get("turn_epoch"),
            "turn_epoch",
        ),
        outcome=str(outcome),
        completed_unix_ms=_nonnegative_protocol_integer(
            value.get("completed_unix_ms"),
            "completed_unix_ms",
        ),
        message=message,
        message_truncated=message_truncated,
        agent_session_path=agent_session_path,
    )


def _pane_turns_replay(value: Any, pane_id: str) -> HerdrPaneTurnsReplay:
    if not isinstance(value, Mapping):
        raise HerdrEnvelopeError("invalid pane.turns response")
    turns = value.get("turns")
    if isinstance(turns, Mapping):
        value = turns
    response_pane_id = value.get("pane_id")
    if not isinstance(response_pane_id, str) or response_pane_id != pane_id:
        raise HerdrEnvelopeError("pane.turns returned the wrong pane")
    epoch = _nonnegative_protocol_integer(value.get("turn_epoch"), "turn_epoch")
    raw_records = value.get("records")
    if not isinstance(raw_records, list):
        raise HerdrEnvelopeError("pane.turns records must be an array")
    records = tuple(
        _turn_completion_record(record, pane_id=pane_id)
        for record in raw_records
    )
    if any(record.turn_epoch != epoch for record in records):
        raise HerdrEnvelopeError("pane.turns mixed turn epochs")
    if tuple(record.turn for record in records) != tuple(
        sorted({record.turn for record in records})
    ):
        raise HerdrEnvelopeError("pane.turns records are not strictly ordered")
    truncated = value.get("truncated", False)
    if not isinstance(truncated, bool):
        raise HerdrEnvelopeError("invalid pane.turns truncated marker")
    raw_oldest = value.get("oldest_available")
    oldest = (
        None
        if raw_oldest is None
        else _nonnegative_protocol_integer(raw_oldest, "oldest_available")
    )
    return HerdrPaneTurnsReplay(
        pane_id=pane_id,
        turn_epoch=epoch,
        records=records,
        truncated=truncated,
        oldest_available=oldest,
    )


def _entity_payload_with_source(payload: Mapping[str, Any], *entity_names: str) -> tuple[dict[str, Any], str | None]:
    """Return an event entity object plus the nested entity name selected."""
    merged: dict[str, Any] = {}
    nested_entity_keys: set[str] = set()
    selected_entity: str | None = None
    for entity_name in entity_names:
        nested = _field_value(payload, entity_name)
        if isinstance(nested, Mapping):
            merged.update(dict(nested))
            compact_name = _compact_key(entity_name)
            nested_entity_keys.add(compact_name)
            selected_entity = compact_name
            break
    for key, value in payload.items():
        if _compact_key(key) in nested_entity_keys:
            continue
        merged.setdefault(str(key), value)
    return merged, selected_entity


def _entity_payload(payload: Mapping[str, Any], *entity_names: str) -> dict[str, Any]:
    """Return a single object for an event entity while preserving scalar hints."""
    item, _selected_entity = _entity_payload_with_source(payload, *entity_names)
    return item


def _privatize_pane_event_id(item: dict[str, Any]) -> dict[str, Any]:
    """Treat generic ``id`` on pane events as a private pane identifier."""
    raw_id = _first_text(item, ("id",))
    if raw_id and _first_text(item, ("pane_id", "paneId")) is None:
        item["pane_id"] = raw_id
    for key in list(item):
        if _compact_key(key) == "id":
            item.pop(key, None)
    return item


def _pane_event_payload_with_provenance(
    payload: Mapping[str, Any],
    *entity_names: str,
    allow_top_level_pane_info: bool = False,
) -> tuple[dict[str, Any], bool]:
    """Return a pane-event item and whether a full PaneInfo authorizes it."""
    item, selected_entity = _entity_payload_with_source(payload, *entity_names)
    # EventData's internally tagged discriminator is envelope metadata, not a
    # PaneInfo field. Keeping it would make event and pane.list projections
    # differ even after their identity pair was canonicalized.
    for key in list(item):
        if _compact_key(key) == "type":
            item.pop(key, None)
    # Scalar agent/status events and nested agent/worker objects are not
    # PaneInfo, even when they repeat workspace_id/pane_id. Treating them as
    # PaneInfo was the observation-layer path that admitted alternate identity
    # representations. Pane lifecycle events historically also carry a full
    # top-level PaneInfo, so their caller opts into that established shape.
    top_level_identity = canonical_herdr_pane_identity(
        _first_text(item, ("workspace_id", "workspaceId")),
        _first_text(item, ("pane_id", "paneId")),
    )
    pane_info_observed = selected_entity == "pane" or (
        selected_entity is None
        and allow_top_level_pane_info
        and top_level_identity is not None
    )
    return _privatize_pane_event_id(item), pane_info_observed


def _pane_event_payload(payload: Mapping[str, Any], *entity_names: str) -> dict[str, Any]:
    """Return a pane-event item without exposing pane ``id`` as public worker id."""
    item, _pane_info_observed = _pane_event_payload_with_provenance(
        payload,
        *entity_names,
    )
    return item


def _event_alias_key(name: str) -> str:
    return "_".join(part for part in _compact_key(name).split("_") if part)


def _canonical_event_name(raw_name: Any) -> str | None:
    if not isinstance(raw_name, str) or not raw_name.strip():
        return None
    event_name = raw_name.strip()
    if event_name in _SUPPORTED_EVENT_NAME_SET:
        return event_name
    aliases = {_event_alias_key(name): name for name in _SUPPORTED_EVENT_NAMES}
    aliases.update(
        {
            "agent_detected": "pane.agent_detected",
            "agent_observed": "pane.agent_detected",
            "agent_status_changed": "pane.agent_status_changed",
            "agent_status_updated": "pane.agent_status_changed",
            "pane_output_changed": "pane.updated",
            "pane_observed": "pane.created",
            "pane_detected": "pane.created",
            "workspace_observed": "workspace.updated",
            "workspace_detected": "workspace.created",
            "worktree_observed": "worktree.opened",
            "worktree_detected": "worktree.created",
            "worktree_updated": "worktree.opened",
            "worktree_changed": "worktree.opened",
            "worktree_closed": "worktree.removed",
            "worktree_deleted": "worktree.removed",
        }
    )
    return aliases.get(_event_alias_key(event_name))


def _strict_producer_id(value: object) -> str | None:
    if type(value) is not str or not value or any(character.isspace() for character in value):
        return None
    return value


def _strict_producer_sequence(value: object) -> str | None:
    if type(value) is not int or value < 0:
        return None
    return str(value)


def _producer_identity(envelope: Mapping[str, Any]) -> HerdrProducerIdentity | None:
    """Return only valid explicit top-level producer identity when present.

    Herdr's confirmed EventEnvelope has only ``event`` and ``data``. The
    identity fields handled here are forward-compatible optional metadata;
    malformed metadata leaves the event idless, and entity fields inside
    ``data`` are never durable event identity.
    """
    if "event_id" in envelope:
        event_id = _strict_producer_id(envelope.get("event_id"))
        return HerdrEventId(event_id) if event_id is not None else None
    server_present = "server_id" in envelope
    sequence_present = "sequence" in envelope
    if not server_present and not sequence_present:
        return None
    producer_id = _strict_producer_id(envelope.get("server_id"))
    sequence = _strict_producer_sequence(envelope.get("sequence"))
    if not server_present or not sequence_present or producer_id is None or sequence is None:
        return None
    return HerdrProducerSequence(producer_id, sequence)


def normalize_event(envelope: Mapping[str, Any]) -> NormalizedHerdrEvent | None:
    """Normalize a Herdr event envelope; unsupported events return ``None``.

    Confirmed Herdr envelopes use ``event`` and ``data`` and are intentionally
    idless. ``payload`` remains receive-only compatibility for older clients.
    """
    name = _canonical_event_name(envelope.get("event"))
    if name is None:
        return None
    payload = envelope.get("data") if "data" in envelope else envelope.get("payload", {})
    if payload is None:
        payload = {}
    if not isinstance(payload, Mapping):
        return None
    return NormalizedHerdrEvent(
        name=name,
        payload=dict(payload),
        producer_identity=_producer_identity(envelope),
    )


def _worker_copy(
    worker: Worker,
    *,
    worker_id: str | None = None,
    name: str | None = None,
    status: str | None = None,
    space_id: str | None = None,
    meta: Mapping[str, Any] | None = None,
    last_seen_at: str | None = None,
    summary: str | None = None,
    backend_target: Mapping[str, Any] | None = None,
) -> Worker:
    return Worker(
        id=worker_id if worker_id is not None else worker.id,
        name=name if name is not None else worker.name,
        status=status if status is not None else worker.status,
        space_id=space_id if space_id is not None else worker.space_id,
        meta=dict(meta) if meta is not None else dict(worker.meta),
        last_seen_at=last_seen_at if last_seen_at is not None else worker.last_seen_at,
        summary=summary if summary is not None else worker.summary,
        backend_target=dict(backend_target) if isinstance(backend_target, Mapping) else worker.backend_target,
    )


def _merge_worker_update(
    existing: Worker | None,
    observed: Worker,
    *,
    status: str | None = None,
    preserve_existing_continuity: bool = False,
) -> Worker:
    if existing is None:
        if status is not None:
            return _worker_copy(observed, status=status)
        return observed
    merged_meta = _strip_stable_key_fields(existing.meta)
    if (
        preserve_existing_continuity
        and is_stable_worker_key(existing.meta.get("stable_key"))
        and type(existing.meta.get("stable_key_version")) is int
        and existing.meta.get("stable_key_version") == STABLE_KEY_VERSION
    ):
        merged_meta["stable_key"] = existing.meta["stable_key"]
        merged_meta["stable_key_version"] = STABLE_KEY_VERSION
    merged_meta.update(observed.meta)
    observed_name_is_identity = observed.name in {observed.id, "unknown"}
    resolved_status = status if status is not None else observed.status
    if resolved_status == "unknown" and existing.status != "unknown":
        resolved_status = existing.status
    return Worker(
        id=existing.id,
        name=existing.name if observed_name_is_identity else observed.name,
        status=resolved_status,
        space_id=observed.space_id or existing.space_id,
        meta=merged_meta,
        last_seen_at=observed.last_seen_at or utc_timestamp(),
        summary=observed.summary or existing.summary,
        backend_target=observed.backend_target or existing.backend_target,
    )


def _closed_worker(worker: Worker) -> Worker:
    return _worker_copy(worker, status="closed", last_seen_at=worker.last_seen_at or utc_timestamp())


def _observed_worker_count(workers: Sequence[Worker]) -> int:
    return len([worker for worker in workers if worker.status != "closed"])


def _binding_target(binding: WorkerBinding) -> tuple[str, str]:
    return (binding.target_kind, binding.target_value)

def _worker_state_equal(left: Worker, right: Worker) -> bool:
    """Compare effective worker state while ignoring observation timestamps."""
    return left.fingerprint == right.fingerprint and left.backend_target == right.backend_target


def _binding_state_equal(left: WorkerBinding, right: WorkerBinding) -> bool:
    """Compare effective private routing while ignoring observation timestamps."""
    return (
        left.host_id,
        left.worker_id,
        left.worker_fingerprint,
        left.backend,
        left.target_kind,
        left.target_value,
        left.turn_target_kind,
        left.turn_target_value,
        left.sendable,
        left.reason,
        left.expires_at,
        left.private_fingerprint,
    ) == (
        right.host_id,
        right.worker_id,
        right.worker_fingerprint,
        right.backend,
        right.target_kind,
        right.target_value,
        right.turn_target_kind,
        right.turn_target_value,
        right.sendable,
        right.reason,
        right.expires_at,
        right.private_fingerprint,
    )




def _target_pairs_from_item(item: Mapping[str, Any], *, old_first: bool = False) -> list[tuple[str, str]]:
    old_pairs = [
        ("pane_id", _first_text(item, ("old_pane_id", "previous_pane_id", "from_pane_id", "source_pane_id"))),
        (
            "terminal_id",
            _first_text(item, ("old_terminal_id", "previous_terminal_id", "from_terminal_id", "source_terminal_id")),
        ),
    ]
    current_pairs = [
        ("agent_id", _first_text(item, ("agent_id", "agentId"))),
        ("terminal_id", _first_text(item, ("terminal_id", "terminalId"))),
        ("pane_id", _first_text(item, ("pane_id", "paneId", "id"))),
        ("agent", _first_text(item, ("agent",))),
        ("name", _first_text(item, ("name", "label"))),
    ]
    pairs = [*old_pairs, *current_pairs] if old_first else [*current_pairs, *old_pairs]
    seen: set[tuple[str, str]] = set()
    result: list[tuple[str, str]] = []
    for kind, value in pairs:
        if not value:
            continue
        pair = (kind, value)
        if pair in seen:
            continue
        seen.add(pair)
        result.append(pair)
    return result


def _new_move_target(item: Mapping[str, Any]) -> tuple[str, str] | None:
    candidates = (
        ("pane_id", ("new_pane_id", "to_pane_id", "target_pane_id", "pane_id", "paneId", "id")),
        ("terminal_id", ("new_terminal_id", "to_terminal_id", "target_terminal_id", "terminal_id", "terminalId")),
        ("agent_id", ("agent_id", "agentId")),
    )
    for kind, keys in candidates:
        value = _first_text(item, keys)
        if value:
            return kind, value
    return None


def _has_public_worker_identity(item: Mapping[str, Any]) -> bool:
    return (
        _first_text(item, ("worker_id", "id", "slug", "agent_id", "agent", "name", "label", "title"))
        is not None
    )

def _has_authoritative_identity_tuple(item: Mapping[str, Any]) -> bool:
    return (
        _field_value(item, "workspace_id") is not None
        and _field_value(item, "pane_id") is not None
    )


def _has_authoritative_binding_target(item: Mapping[str, Any]) -> bool:
    agent_session = _safe_mapping(_field_value(item, "agent_session"))
    return (
        _first_text(item, ("agent_id", "terminal_id")) is not None
        or _first_text(agent_session, ("value", "id")) is not None
    )


def _authenticated_local_stable_key(worker: Worker) -> str | None:
    value = worker.meta.get("stable_key")
    version = worker.meta.get("stable_key_version")
    if (
        is_stable_worker_key(value)
        and type(version) is int
        and version == STABLE_KEY_VERSION
    ):
        return str(value)
    return None


class HerdrEventBackend:
    """Maintain Tendwire projections from Herdr socket reconcile and events."""

    def __init__(
        self,
        config: Config,
        *,
        client_factory: Callable[[Config], HerdrSocketClient] | None = None,
        subscribe_method: str = DEFAULT_SUBSCRIBE_METHOD,
        debounce_seconds: float | None = None,
        reconcile_interval_seconds: float | None = None,
        max_workers: int | None = None,
        output_excerpt_chars: int | None = None,
        dedupe_size: int = DEFAULT_DEDUPE_SIZE,
        max_batch_size: int = DEFAULT_MAX_BATCH_SIZE,
        reconnect_delay_seconds: float = DEFAULT_RECONNECT_DELAY_SECONDS,
        stop_event: threading.Event | None = None,
        turn_completion_processor: Callable[..., Any] | None = None,
    ) -> None:
        self.config = config
        self.client_factory = client_factory or self._default_client_factory
        requested_subscribe_method = str(subscribe_method or DEFAULT_SUBSCRIBE_METHOD)
        if requested_subscribe_method != HERDR_EVENTS_SUBSCRIBE_METHOD:
            raise HerdrEventBackendError("Herdr event backend requires events.subscribe")
        self.subscribe_method = HERDR_EVENTS_SUBSCRIBE_METHOD
        configured_debounce = config.event_debounce_seconds if debounce_seconds is None else debounce_seconds
        configured_reconcile = (
            config.reconcile_interval_seconds
            if reconcile_interval_seconds is None
            else reconcile_interval_seconds
        )
        self.debounce_seconds = max(0.0, float(configured_debounce))
        self.reconcile_interval_seconds = max(0.0, float(configured_reconcile))
        self.max_workers = max(1, int(config.max_workers if max_workers is None else max_workers))
        self.output_excerpt_chars = max(
            1,
            int(config.output_excerpt_chars if output_excerpt_chars is None else output_excerpt_chars),
        )
        self.dedupe_size = max(1, int(dedupe_size))
        self.max_batch_size = max(1, int(max_batch_size))
        self.reconnect_delay_seconds = max(0.0, float(reconnect_delay_seconds))
        self.stop_event = stop_event or threading.Event()
        self.turn_completion_processor = (
            turn_completion_processor
            or self._default_turn_completion_processor
        )
        self._lock = threading.RLock()
        self._turn_refresh_callback_lock = threading.Lock()
        self._turn_refresh_callback: Callable[[], None] | None = None
        self._ready = threading.Event()
        self._thread: threading.Thread | None = None
        self._producer_dedupe: OrderedDict[HerdrProducerIdentity, None] = OrderedDict()
        self._pending_events: list[NormalizedHerdrEvent] = []
        self._spaces: dict[str, Space] = {}
        self._workers: dict[str, Worker] = {}
        self._bindings: dict[str, WorkerBinding] = {}
        # pane_id -> terminal_id, remembered from reconcile pane lists so that
        # pane-id-only events (pane.agent_status_changed carries no terminal id)
        # can still resolve to the terminal-targeted stored binding.
        self._pane_terminals: dict[str, str] = {}
        self._pane_owners: dict[str, set[str]] = {}
        self._terminal_owners: dict[str, set[str]] = {}
        self._session_owners: dict[str, set[str]] = {}
        self._event_continuity_revalidated = False
        self._health = self._health_for("unknown")
        self._last_event_at: str | None = None
        self._last_reconcile_at: str | None = None
        self._last_snapshot_at: str | None = None
        self._last_cap_status_at: str | None = None
        self._automatic_maintenance_status: dict[str, Any] | None = None
        self._next_reconcile_monotonic: float | None = None
        self._subscription_pane_ids: list[str] = []
        self._turn_api_probed = False
        self._turn_api_supported = False
        self._turn_completion_diagnostic_counts: dict[str, int] = {}
        self._load_existing_state()

    @staticmethod
    def _default_client_factory(config: Config) -> HerdrSocketClient:
        return HerdrSocketClient(timeout=config.herdr_timeout_seconds)

    @staticmethod
    def _default_turn_completion_processor(
        config: Config,
        pane_id: str,
        *,
        terminal_id: str | None = None,
        binding_private_fingerprint: str | None = None,
    ) -> Any:
        from .herdr_turns import refresh_completed_pane_turn

        return refresh_completed_pane_turn(
            config,
            pane_id,
            terminal_id=terminal_id,
            binding_private_fingerprint=binding_private_fingerprint,
            adapter_timeout_seconds=config.herdr_timeout_seconds,
        )

    @property
    def db_path(self) -> Path:
        if self.config.db_path is None:
            raise HerdrEventBackendError("socket event backend requires a sqlite db path")
        return Path(self.config.db_path)

    @property
    def health(self) -> HerdrEventBackendHealth:
        with self._lock:
            return self._health

    @property
    def operational_status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "status": self._health.status,
                "outcome": self._health.outcome,
                "ready": self.ready,
                "running": self.running,
                "last_event_at": self._last_event_at,
                "last_reconcile_at": self._last_reconcile_at,
                "last_snapshot_at": self._last_snapshot_at,
                "last_cap_status_at": self._last_cap_status_at,
                "reconcile_enabled": self.reconcile_interval_seconds > 0,
                "automatic_maintenance": (
                    dict(self._automatic_maintenance_status)
                    if self._automatic_maintenance_status is not None
                    else None
                ),
                "turn_completion_diagnostics": dict(
                    self._turn_completion_diagnostic_counts
                ),
            }

    @property
    def ready(self) -> bool:
        return self._ready.is_set()

    @property
    def running(self) -> bool:
        thread = self._thread
        return thread is not None and thread.is_alive()

    def set_turn_refresh_callback(self, callback: Callable[[], None] | None) -> None:
        """Set the post-persistence turn refresh signal."""
        if callback is not None and not callable(callback):
            raise TypeError("turn refresh callback must be callable or None")
        with self._turn_refresh_callback_lock:
            self._turn_refresh_callback = callback

    def _notify_turn_refresh(self) -> None:
        with self._turn_refresh_callback_lock:
            callback = self._turn_refresh_callback
        if callback is None:
            return
        try:
            callback()
        except Exception:
            # Scheduling is best-effort after the durable backend commit.
            pass

    def _health_for(self, outcome: str) -> HerdrEventBackendHealth:
        health = herdr_backend_health(outcome)
        return HerdrEventBackendHealth(
            status=health.status,
            outcome=health.outcome,
            observed_at=health.observed_at or utc_timestamp(),
            message=health.message,
        )

    def _save_snapshot(
        self,
        snapshot: Snapshot,
        *,
        observation: SnapshotObservationContext,
        worker_bindings: Iterable[WorkerBinding] | None = None,
        binding_observation_authoritative: bool = False,
        binding_workers_present: bool = True,
    ) -> None:
        if not save_snapshot(
            self.db_path,
            snapshot,
            turn_model=self.config.turn_model,
            observation=observation,
            worker_bindings=worker_bindings,
            binding_backend=BACKEND_NAME if worker_bindings is not None else None,
            binding_observation_authoritative=binding_observation_authoritative,
            binding_workers_present=binding_workers_present,
        ):
            raise RuntimeError("snapshot rejected by store ordering")
        policy = SnapshotRetentionPolicy(
            retention_days=self.config.snapshot_retention_days,
            retention_count=self.config.snapshot_retention_count,
            batch_size=self.config.snapshot_maintenance_batch_size,
        )
        try:
            result = maybe_run_automatic_store_maintenance(
                self.db_path,
                policy=policy,
                turn_model=self.config.turn_model,
                acknowledged_final_retention_days=(
                    self.config.acknowledged_final_retention_days
                ),
                acknowledged_final_retention_count=(
                    self.config.acknowledged_final_retention_count
                ),
                command_retry_horizon_seconds=(
                    self.config.command_retry_horizon_seconds
                ),
                command_receipt_retention_seconds=(
                    self.config.command_receipt_retention_seconds
                ),
                command_receipt_retention_count=(
                    self.config.command_receipt_retention_count
                ),
                cadence_seconds=self.config.store_maintenance_cadence_seconds,
            )
            snapshot_result = result.get("snapshot")
            snapshot_counts = snapshot_result if isinstance(snapshot_result, Mapping) else {}
            maintenance_status = {
                "ok": bool(result.get("ok")),
                "status": str(result.get("status") or "unknown"),
                "due": bool(result.get("due")),
                "examined": int(snapshot_counts.get("examined") or 0),
                "deleted": int(snapshot_counts.get("deleted") or 0),
                "remaining_candidates": bool(snapshot_counts.get("remaining_candidates")),
            }
        except Exception:
            self._automatic_maintenance_status = {
                "ok": False,
                "status": "failed",
                "due": False,
                "examined": 0,
                "deleted": 0,
                "remaining_candidates": False,
            }
        else:
            self._automatic_maintenance_status = maintenance_status

    def _load_existing_state(self) -> None:
        try:
            snapshot = latest_snapshot(self.db_path, self.config.host_id)
        except Exception:
            snapshot = None
        if snapshot is not None:
            self._spaces = {space.id: space for space in snapshot.spaces}
            self._workers = {worker.id: worker for worker in snapshot.workers}
            self._last_snapshot_at = snapshot.updated_at
            for health in snapshot.backend_health:
                if health.name == BACKEND_NAME:
                    self._health = HerdrEventBackendHealth(
                        status=health.status,
                        outcome=health.outcome,
                        observed_at=health.observed_at or utc_timestamp(),
                        message=health.message,
                    )
                    break
        try:
            bindings = list_worker_bindings(self.db_path, self.config.host_id, backend=BACKEND_NAME)
        except Exception:
            bindings = []
        self._bindings = {binding.private_fingerprint: binding for binding in bindings}
        self._replace_ownership_maps([], bindings)

    def start(self, *, wait_for_reconcile: bool = True, timeout_seconds: float | None = None) -> None:
        if self._thread is not None:
            return
        self.stop_event.clear()
        self._ready.clear()
        thread = threading.Thread(target=self.run_forever, name="tendwire-herdr-events", daemon=True)
        self._thread = thread
        thread.start()
        if wait_for_reconcile:
            timeout = (
                self.config.herdr_initial_reconcile_timeout_seconds
                if timeout_seconds is None
                else timeout_seconds
            )
            if not self._ready.wait(max(0.001, float(timeout))):
                self.stop()
                raise HerdrSocketTimeoutError("initial Herdr reconciliation timed out")

    def stop(self) -> None:
        self.stop_event.set()
        self.flush()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=max(1.0, self.config.herdr_timeout_seconds + 1.0))
        if thread is None or not thread.is_alive():
            self._thread = None

    def run_forever(self) -> None:
        while not self.stop_event.is_set():
            reconciled = False
            try:
                client = self.client_factory(self.config)
                try:
                    self._turn_api_probed = False
                    self._turn_api_supported = False
                    self.reconcile_once(client=client)
                    reconciled = True
                    # Herdr ordinary RPC connections are one-shot. Keep this
                    # future subscription client out of pane.turns entirely;
                    # each replay request uses its own short-lived client.
                    if callable(getattr(client, "pane_turns", None)) or callable(
                        getattr(client, "request", None)
                    ):
                        self._replay_turns_after_reconcile()
                    else:
                        self._turn_api_probed = True
                        self._turn_api_supported = False
                    if self.stop_event.is_set():
                        break
                    if hasattr(client, "connect"):
                        client.connect()
                    stream = self._subscribe_event_stream(client)
                    # Herdr subscriptions intentionally start at the server's
                    # current sequence and deliver no backlog.  The first
                    # replay above capability-gates the new subscription kind;
                    # this second replay closes the probe-to-subscribe race.
                    # Events concurrent with it are buffered by the socket
                    # client and become harmless watermark-deduped duplicates.
                    if self._turn_api_supported:
                        self._replay_turns_after_reconcile()
                    self._ready.set()
                    self._read_event_stream(client, stream.subscription_id)
                finally:
                    if hasattr(client, "close"):
                        client.close()
            except HerdrSocketTimeoutError:
                self._mark_unhealthy_safe("timeout")
            except (HerdrSocketDisconnectedError, HerdrSocketConnectionError):
                # A complete list reconciliation is authoritative. The event
                # stream only accelerates later observations, so its closure
                # must not replace that healthy snapshot with unavailable.
                if not reconciled:
                    self._mark_unhealthy_safe("socket_disconnected")
            except (HerdrMalformedLineError, HerdrEnvelopeError, HerdrProtocolError, ValueError, TypeError):
                self._mark_unhealthy_safe("protocol_error")
            except Exception:
                self._mark_unhealthy_safe("unknown")
            if self.stop_event.is_set():
                break
            delay = self._reconnect_delay_seconds()
            if delay:
                self.stop_event.wait(delay)

    def _reconnect_delay_seconds(self) -> float:
        delay = self.reconnect_delay_seconds
        with self._lock:
            outcome = self._health.outcome
        if outcome == "protocol_error" and self.ready and self.reconcile_interval_seconds > 0:
            return max(delay, min(self.reconcile_interval_seconds, 60.0))
        return delay

    def _read_event_stream(self, client: Any, subscription_id: str) -> None:
        while not self.stop_event.is_set():
            self._run_periodic_reconcile_if_due()
            try:
                envelope = client.read_event(subscription_id, timeout=self.config.herdr_timeout_seconds)
            except HerdrSocketTimeoutError:
                self._run_periodic_reconcile_if_due()
                continue
            self.queue_event_envelope(envelope)
            disconnected = False
            deadline = time.monotonic() + self.debounce_seconds
            while (
                self.debounce_seconds > 0
                and self._pending_event_count() < self.max_batch_size
                and not self.stop_event.is_set()
            ):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    extra = client.read_event(subscription_id, timeout=max(0.001, remaining))
                except HerdrSocketTimeoutError:
                    break
                except HerdrSocketDisconnectedError:
                    disconnected = True
                    break
                self.queue_event_envelope(extra, flush=False)
            self.flush()
            self._run_periodic_reconcile_if_due()
            if disconnected:
                raise HerdrSocketDisconnectedError("Herdr socket disconnected during event drain")

    def _schedule_next_reconcile(self) -> None:
        if self.reconcile_interval_seconds <= 0:
            self._next_reconcile_monotonic = None
            return
        self._next_reconcile_monotonic = time.monotonic() + self.reconcile_interval_seconds

    def _run_periodic_reconcile_if_due(self, client: Any | None = None) -> None:
        if self.reconcile_interval_seconds <= 0:
            return
        due_at = self._next_reconcile_monotonic
        if due_at is None:
            self._schedule_next_reconcile()
            return
        if time.monotonic() < due_at:
            return
        self.reconcile_once(client=client)

    def _pending_event_count(self) -> int:
        with self._lock:
            return len(self._pending_events)

    def _current_bindings(self) -> list[WorkerBinding]:
        return list(self._bindings.values())

    def _records_from_reconcile_payloads(self, agent_payload: Any, pane_payload: Any) -> list[Any]:
        return _records_from_agent_and_pane_payloads(
            self.config,
            agent_payload,
            pane_payload,
        )

    def _pane_subscription_ids(self, pane_payload: Any) -> list[str]:
        pane_ids: list[str] = []
        seen: set[str] = set()
        for item in _payload_items(pane_payload, _PANE_PAYLOAD_KEYS):
            if not _pane_has_agent(item):
                continue
            pane_id = _first_text(item, ("pane_id", "paneId", "id"))
            if not pane_id or pane_id in seen:
                continue
            seen.add(pane_id)
            pane_ids.append(pane_id)
            if len(pane_ids) >= self.max_workers:
                break
        return pane_ids

    def reconcile_once(self, *, client: Any | None = None) -> Snapshot:
        """Perform a full Herdr list reconcile and persist Tendwire projections."""
        owns_client = client is None
        if client is None:
            client = self.client_factory(self.config)
        try:
            if owns_client and hasattr(client, "connect"):
                client.connect()
            payloads = {
                "workspace.list": self._call_list_method(client, "workspace_list"),
                "tab.list": self._call_list_method(client, "tab_list"),
                "pane.list": self._call_list_method(client, "pane_list"),
                "agent.list": self._call_list_method(client, "agent_list"),
            }
            # tab.list is intentionally part of the authoritative reconcile barrier;
            # Tendwire has no current public tab model to project into.
            _ = payloads["tab.list"]
            with self._lock:
                stored_bindings = list_worker_bindings(
                    self.db_path,
                    self.config.host_id,
                    backend=BACKEND_NAME,
                )
                spaces = _spaces_from_payload(payloads["workspace.list"])
                records = self._records_from_reconcile_payloads(
                    payloads["agent.list"],
                    payloads["pane.list"],
                )
                subscription_pane_ids = self._pane_subscription_ids(payloads["pane.list"])
                workers, bindings = _workers_and_bindings_from_records(
                    self.config,
                    records,
                    stored_bindings=stored_bindings,
                    require_authenticated_continuity=True,
                )
                if _observed_worker_count(workers) > self.max_workers:
                    return self._mark_worker_cap_exceeded_locked(
                        _observed_worker_count(workers)
                    )
                outcome = "healthy_non_empty" if spaces or workers else "empty_healthy"
                health = herdr_backend_health(outcome, spaces=spaces, workers=workers)
                previous = latest_snapshot(self.db_path, self.config.host_id)
                snapshot_workers = self._workers_with_closed_missing(
                    previous.workers if previous is not None else [],
                    workers,
                    bound_worker_ids={binding.worker_id for binding in stored_bindings},
                )
                snapshot = project_from_observations(
                    self.config,
                    spaces=spaces,
                    workers=snapshot_workers,
                    backend_health=[health],
                )
                self._save_snapshot(
                    snapshot,
                    observation=SnapshotObservationContext(
                        authority="complete",
                        observed_at=health.observed_at or snapshot.updated_at,
                    ),
                    worker_bindings=bindings,
                    binding_observation_authoritative=True,
                    binding_workers_present=bool(workers),
                )
                self._last_reconcile_at = snapshot.updated_at
                self._last_snapshot_at = snapshot.updated_at
                self._schedule_next_reconcile()
                self._spaces = {space.id: space for space in snapshot.spaces}
                self._workers = {worker.id: worker for worker in snapshot.workers}
                self._bindings = {binding.private_fingerprint: binding for binding in bindings}
                self._replace_ownership_maps(records, bindings)
                self._subscription_pane_ids = subscription_pane_ids
                self._health = HerdrEventBackendHealth(
                    status=health.status,
                    outcome=health.outcome,
                    observed_at=health.observed_at or snapshot.updated_at,
                    message=health.message,
                )
            self._notify_turn_refresh()
            return snapshot
        except (HerdrContinuityUnavailableError, InstallationKeyError):
            snapshot = self._mark_unhealthy("continuity_unavailable")
            with self._lock:
                self._last_reconcile_at = snapshot.updated_at
                self._schedule_next_reconcile()
            return snapshot
        except Exception:
            self._mark_unhealthy("unknown")
            raise
        finally:
            if owns_client and hasattr(client, "close"):
                client.close()

    def _call_list_method(self, client: Any, method_name: str) -> Any:
        method = getattr(client, method_name)
        try:
            try:
                return method(timeout=self.config.herdr_timeout_seconds)
            except TypeError:
                return method()
        finally:
            # Herdr 0.7.x may close a list connection immediately after its
            # response. Reconnect between authoritative read-only probes so a
            # late close cannot race the next request after its write.
            if hasattr(client, "close"):
                client.close()

    @staticmethod
    def _herdr_error_code(exc: HerdrErrorResponse) -> str:
        error = exc.error
        if isinstance(error, Mapping):
            code = error.get("code")
            if isinstance(code, str):
                return code
        return ""

    @staticmethod
    def _herdr_error_message(exc: HerdrErrorResponse) -> str:
        error = exc.error
        if isinstance(error, Mapping):
            message = error.get("message")
            if isinstance(message, str):
                return message
        return ""

    def _call_pane_turns(
        self,
        client: Any,
        pane_id: str,
        *,
        since: int,
        expected_epoch: int | None,
        allow_uncorrelated: bool = False,
    ) -> HerdrPaneTurnsReplay:
        params: dict[str, Any] = {
            "pane_id": str(pane_id),
            "since": int(since),
        }
        if expected_epoch is not None:
            params["expected_epoch"] = int(expected_epoch)
        optional_keywords = {"timeout": self.config.herdr_timeout_seconds}
        if allow_uncorrelated:
            optional_keywords["allow_uncorrelated"] = True
        method = getattr(client, "pane_turns", None)
        if callable(method):
            value = _call_with_optional_keywords(
                method,
                (params,),
                optional_keywords,
            )
        else:
            request = getattr(client, "request", None)
            if not callable(request):
                raise AttributeError("client does not expose pane.turns")
            value = _call_with_optional_keywords(
                request,
                ("pane.turns", params),
                optional_keywords,
            )
        return _pane_turns_replay(value, pane_id)

    def _call_pane_turns_isolated(
        self,
        pane_id: str,
        *,
        since: int,
        expected_epoch: int | None,
        allow_uncorrelated: bool = False,
    ) -> HerdrPaneTurnsReplay:
        """Call one pane.turns RPC on a fresh, always-closed client."""
        client = self.client_factory(self.config)
        try:
            if hasattr(client, "connect"):
                client.connect()
            return self._call_pane_turns(
                client,
                pane_id,
                since=since,
                expected_epoch=expected_epoch,
                allow_uncorrelated=allow_uncorrelated,
            )
        finally:
            if hasattr(client, "close"):
                client.close()

    def _record_turn_diagnostic(
        self,
        code: str,
        pane_id: str,
        *,
        status: str | None = None,
    ) -> None:
        count_key = f"{code}:{status}" if status else code
        with self._lock:
            self._turn_completion_diagnostic_counts[count_key] = (
                self._turn_completion_diagnostic_counts.get(count_key, 0) + 1
            )
        diagnostic = {
            "code": code,
            "host_id": self.config.host_id,
            "pane_id": pane_id,
        }
        if status:
            diagnostic["status"] = status
        _LOGGER.warning(
            code,
            extra={"tendwire_diagnostic": diagnostic},
        )

    def _record_completeness_break(
        self,
        replay: HerdrPaneTurnsReplay,
        reason: str,
    ) -> None:
        record_herdr_turn_completeness_break(
            self.db_path,
            self.config.host_id,
            replay.pane_id,
            turn_epoch=replay.turn_epoch,
            newest_turn=replay.newest_turn,
            reason=reason,
        )
        _LOGGER.warning(
            "herdr_turn_completeness_break",
            extra={
                "tendwire_diagnostic": {
                    "code": "herdr_turn_completeness_break",
                    "host_id": self.config.host_id,
                    "pane_id": replay.pane_id,
                    "reason": reason,
                }
            },
        )

    def _completion_processor_result(
        self,
        record: HerdrTurnCompletionRecord,
    ) -> tuple[str, str | None, str | None]:
        terminal_id = self._pane_terminals.get(record.pane_id)
        owner_ids = self._pane_owners.get(record.pane_id, set())
        owner_bindings = [
            binding
            for binding in self._bindings.values()
            if binding.worker_id in owner_ids
        ]
        binding_private_fingerprint = (
            owner_bindings[0].private_fingerprint
            if len(owner_bindings) == 1
            else None
        )
        result = _call_with_optional_keywords(
            self.turn_completion_processor,
            (self.config, record.pane_id),
            {
                "terminal_id": terminal_id,
                "binding_private_fingerprint": binding_private_fingerprint,
            },
        )
        if isinstance(result, Mapping):
            status = str(result.get("status") or "")
            worker_id = result.get("worker_id")
            refreshed_turn_id = result.get("refreshed_turn_id")
        else:
            status = str(getattr(result, "status", ""))
            worker_id = getattr(result, "worker_id", None)
            refreshed_turn_id = getattr(result, "refreshed_turn_id", None)
        return (
            status,
            str(worker_id) if worker_id else None,
            str(refreshed_turn_id) if refreshed_turn_id else None,
        )

    def _process_turn_record(
        self,
        record: HerdrTurnCompletionRecord,
    ) -> None:
        watermark = get_herdr_turn_watermark(
            self.db_path,
            self.config.host_id,
            record.pane_id,
        )
        if watermark is None:
            self._record_completeness_break(
                HerdrPaneTurnsReplay(
                    pane_id=record.pane_id,
                    turn_epoch=record.turn_epoch,
                    records=(record,),
                    truncated=False,
                    oldest_available=record.turn,
                ),
                "live_without_baseline",
            )
            return
        if watermark.turn_epoch != record.turn_epoch:
            self._record_completeness_break(
                HerdrPaneTurnsReplay(
                    pane_id=record.pane_id,
                    turn_epoch=record.turn_epoch,
                    records=(record,),
                    truncated=False,
                    oldest_available=record.turn,
                ),
                "turn_epoch_mismatch",
            )
            return
        if record.turn <= watermark.last_turn:
            return
        if record.turn != watermark.last_turn + 1:
            self._record_completeness_break(
                HerdrPaneTurnsReplay(
                    pane_id=record.pane_id,
                    turn_epoch=record.turn_epoch,
                    records=(record,),
                    truncated=False,
                    oldest_available=record.turn,
                ),
                "live_gap",
            )
            return
        status, worker_id, refreshed_turn_id = self._completion_processor_result(
            record
        )
        if status not in _COMPLETED_TURN_REFRESH_STATUSES:
            self._record_turn_diagnostic(
                "herdr_turn_completion_refresh_skipped",
                record.pane_id,
                status=status or "unknown",
            )
            refreshed_turn_id = None
        record_herdr_turn_completion(
            self.db_path,
            self.config.host_id,
            record.pane_id,
            turn_epoch=record.turn_epoch,
            turn=record.turn,
            outcome=record.outcome,
            completed_unix_ms=record.completed_unix_ms,
            message=record.message,
            message_truncated=record.message_truncated,
            agent_session_path=record.agent_session_path,
            worker_id=worker_id,
            refreshed_turn_id=refreshed_turn_id,
        )

    def _consume_replay(
        self,
        replay: HerdrPaneTurnsReplay,
        watermark: HerdrTurnWatermark | None,
    ) -> None:
        if watermark is None:
            set_herdr_turn_watermark(
                self.db_path,
                self.config.host_id,
                replay.pane_id,
                turn_epoch=replay.turn_epoch,
                last_turn=replay.newest_turn,
            )
            return
        if replay.turn_epoch != watermark.turn_epoch:
            self._record_completeness_break(replay, "turn_epoch_mismatch")
            return
        if replay.truncated:
            self._record_completeness_break(replay, "replay_truncated")
            return
        expected = watermark.last_turn + 1
        actual_turns = tuple(record.turn for record in replay.records)
        expected_turns = tuple(range(expected, expected + len(replay.records)))
        if actual_turns != expected_turns:
            self._record_completeness_break(replay, "replay_gap")
            return
        for record in replay.records:
            self._process_turn_record(record)

    @classmethod
    def _turn_api_method_unsupported(cls, exc: HerdrErrorResponse) -> bool:
        code = cls._herdr_error_code(exc).strip().lower().replace("-", "_")
        raw_message = cls._herdr_error_message(exc).lower()
        message = raw_message.strip()
        if code in {
            "method_not_found",
            "unknown_method",
            "unsupported_method",
            "not_implemented",
        }:
            return True
        if code == "invalid_request" and raw_message.startswith(
            "invalid request: unknown variant"
        ):
            return True
        return code == "invalid_params" and any(
            marker in message
            for marker in (
                "unknown method",
                "method not found",
                "unsupported method",
                "pane.turns is not supported",
            )
        )

    def _probe_turn_api(
        self,
        client: Any | None,
        pane_id: str,
        watermark: HerdrTurnWatermark | None,
    ) -> tuple[
        bool,
        HerdrPaneTurnsReplay | None,
        HerdrErrorResponse | AttributeError | None,
    ]:
        """Probe pane.turns once without treating pane-scoped errors as absence."""
        if client is not None and not callable(
            getattr(client, "pane_turns", None)
        ) and not callable(getattr(client, "request", None)):
            return False, None, None
        try:
            call = self._call_pane_turns_isolated if client is None else (
                lambda current_pane_id, **kwargs: self._call_pane_turns(
                    client,
                    current_pane_id,
                    **kwargs,
                )
            )
            replay = call(
                pane_id,
                since=watermark.last_turn if watermark is not None else 0,
                expected_epoch=watermark.turn_epoch if watermark is not None else None,
                allow_uncorrelated=True,
            )
        except HerdrErrorResponse as exc:
            if self._turn_api_method_unsupported(exc):
                return False, None, None
            return True, None, exc
        except AttributeError:
            return False, None, None
        return True, replay, None

    def _consume_pane_replay(
        self,
        client: Any | None,
        pane_id: str,
        watermark: HerdrTurnWatermark | None,
        *,
        replay: HerdrPaneTurnsReplay | None = None,
        error: HerdrErrorResponse | AttributeError | None = None,
    ) -> None:
        call = self._call_pane_turns_isolated if client is None else (
            lambda current_pane_id, **kwargs: self._call_pane_turns(
                client,
                current_pane_id,
                **kwargs,
            )
        )
        try:
            if error is not None:
                raise error
            current_replay = replay or call(
                pane_id,
                since=watermark.last_turn if watermark is not None else 0,
                expected_epoch=watermark.turn_epoch if watermark is not None else None,
            )
        except HerdrErrorResponse as exc:
            code = self._herdr_error_code(exc)
            message = self._herdr_error_message(exc)
            if code == "turn_epoch_mismatch":
                try:
                    current_replay = call(
                        pane_id,
                        since=0,
                        expected_epoch=None,
                    )
                except (HerdrErrorResponse, AttributeError) as retry_exc:
                    self._record_turn_diagnostic(
                        "herdr_turn_replay_pane_skipped",
                        pane_id,
                        status=(
                            self._herdr_error_code(retry_exc)
                            if isinstance(retry_exc, HerdrErrorResponse)
                            else "method_unavailable"
                        ),
                    )
                    return
                self._record_completeness_break(
                    current_replay,
                    "turn_epoch_mismatch",
                )
                return
            if code == "invalid_params" and "newer than current turn" in message:
                try:
                    current_replay = call(
                        pane_id,
                        since=0,
                        expected_epoch=None,
                    )
                except (HerdrErrorResponse, AttributeError) as retry_exc:
                    self._record_turn_diagnostic(
                        "herdr_turn_replay_pane_skipped",
                        pane_id,
                        status=(
                            self._herdr_error_code(retry_exc)
                            if isinstance(retry_exc, HerdrErrorResponse)
                            else "method_unavailable"
                        ),
                    )
                    return
                self._record_completeness_break(current_replay, "watermark_ahead")
                return
            self._record_turn_diagnostic(
                "herdr_turn_replay_pane_skipped",
                pane_id,
                status=code or "pane_error",
            )
            return
        except AttributeError:
            self._record_turn_diagnostic(
                "herdr_turn_replay_pane_skipped",
                pane_id,
                status="method_unavailable",
            )
            return
        self._consume_replay(current_replay, watermark)

    def _replay_turns_after_reconcile(self, client: Any | None = None) -> None:
        """Probe pane.turns once, then replay each pane independently."""
        if self.stop_event.is_set():
            return
        pane_ids = tuple(self._subscription_pane_ids)
        if not pane_ids:
            self._turn_api_probed = True
            self._turn_api_supported = False
            return
        watermarks = {
            pane_id: get_herdr_turn_watermark(
                self.db_path,
                self.config.host_id,
                pane_id,
            )
            for pane_id in pane_ids
        }
        probe_pane_id: str | None = None
        probe_replay: HerdrPaneTurnsReplay | None = None
        probe_error: HerdrErrorResponse | AttributeError | None = None
        if not self._turn_api_probed:
            if self.stop_event.is_set():
                return
            probe_pane_id = pane_ids[0]
            supported, probe_replay, probe_error = self._probe_turn_api(
                client,
                probe_pane_id,
                watermarks[probe_pane_id],
            )
            self._turn_api_probed = True
            self._turn_api_supported = supported
        if not self._turn_api_supported:
            return
        for pane_id in pane_ids:
            if self.stop_event.is_set():
                return
            self._consume_pane_replay(
                client,
                pane_id,
                watermarks[pane_id],
                replay=probe_replay if pane_id == probe_pane_id else None,
                error=probe_error if pane_id == probe_pane_id else None,
            )

    def _subscribe_event_stream(self, client: Any) -> Any:
        # Herdr 0.7.5 strictly validates pane-scoped status subscriptions and
        # added the general pane.updated event.  Use one bounded mixed
        # subscription: one global entry per lifecycle/update type and one
        # status entry per pane.
        # pane.output_matched is intentionally absent because 0.7.5 requires a
        # caller-provided match expression; pane.updated is the generic turn
        # and stream refresh signal.
        subscriptions = [{"type": event_name} for event_name in _GLOBAL_EVENT_NAMES]
        subscriptions.extend(
            {"type": "pane.agent_status_changed", "pane_id": pane_id}
            for pane_id in self._subscription_pane_ids
        )
        if self._turn_api_supported:
            subscriptions.extend(
                {"type": "pane.turn_completed", "pane_id": pane_id}
                for pane_id in self._subscription_pane_ids
            )
        params = {"subscriptions": subscriptions}
        if hasattr(client, "subscribe"):
            try:
                return client.subscribe(
                    self.subscribe_method,
                    params,
                    timeout=self.config.herdr_timeout_seconds,
                    event_timeout=self.config.herdr_timeout_seconds,
                )
            except TypeError:
                return client.subscribe(self.subscribe_method, params)
            except HerdrErrorResponse as exc:
                if not exc.uncorrelated:
                    raise
                if hasattr(client, "close"):
                    client.close()
                if hasattr(client, "connect"):
                    client.connect()
                return self._subscribe_legacy_event_stream(client)
        if hasattr(client, "events_subscribe"):
            try:
                return client.events_subscribe(
                    _HERDR_074_EVENT_NAMES,
                    timeout=self.config.herdr_timeout_seconds,
                    event_timeout=self.config.herdr_timeout_seconds,
                )
            except TypeError:
                return client.events_subscribe(_HERDR_074_EVENT_NAMES)
        try:
            return client.subscribe(
                self.subscribe_method,
                params,
                timeout=self.config.herdr_timeout_seconds,
                event_timeout=self.config.herdr_timeout_seconds,
            )
        except TypeError:
            return client.subscribe(self.subscribe_method, params)

    def _subscribe_legacy_event_stream(self, client: Any) -> Any:
        # Preserve 0.7.4's proven compatibility request: its status event is
        # parameterized by pane, while unrelated global event variants tolerate
        # the same pane_id field. Replay-only events remain excluded exactly as
        # before so reconnecting does not synthesize focus/detection activity.
        if self._subscription_pane_ids:
            subscriptions = [
                {"pane_id": pane_id, "type": event_name}
                for pane_id in self._subscription_pane_ids
                for event_name in _HERDR_074_PANE_SCOPED_FALLBACK_EVENT_NAMES
            ]
        else:
            # An empty installation has no pane id with which to build the
            # pane-scoped compatibility request. Fall back to the pre-0.7.5
            # global shape instead of retrying the rejected mixed shape on
            # every reconnect.
            subscriptions = [
                {"type": event_name} for event_name in _HERDR_074_EVENT_NAMES
            ]
        params = {"subscriptions": subscriptions}
        try:
            return client.subscribe(
                self.subscribe_method,
                params,
                timeout=self.config.herdr_timeout_seconds,
                event_timeout=self.config.herdr_timeout_seconds,
            )
        except TypeError:
            return client.subscribe(self.subscribe_method, params)

    def _workers_with_closed_missing(
        self,
        previous_workers: Sequence[Worker],
        current_workers: Sequence[Worker],
        *,
        bound_worker_ids: set[str] | None = None,
    ) -> list[Worker]:
        current_by_id = {worker.id: worker for worker in current_workers}
        merged = list(current_workers)
        for worker in previous_workers:
            if worker.id in current_by_id:
                continue
            if bound_worker_ids is not None and worker.id not in bound_worker_ids:
                # A missing worker with no live binding is a phantom (event
                # projections that never matched a binding); dropping it here
                # keeps it from riding along as "closed" forever.
                continue
            merged.append(_closed_worker(worker))
        return merged

    def queue_event_envelope(self, envelope: Mapping[str, Any], *, flush: bool | None = None) -> bool:
        event = normalize_event(envelope)
        if event is None:
            return False
        with self._lock:
            if (
                event.producer_identity is not None
                and self._is_duplicate_producer_identity(event.producer_identity)
            ):
                return False
            # Confirmed Herdr envelopes are idless. Every such event is queued;
            # current-state idempotence, not historical content, prevents side effects.
            self._pending_events.append(event)
            self._last_event_at = utc_timestamp()
            should_flush = self.debounce_seconds <= 0 if flush is None else flush
        if should_flush:
            self.flush()
        return True

    def _is_duplicate_producer_identity(self, identity: HerdrProducerIdentity) -> bool:
        if identity in self._producer_dedupe:
            self._producer_dedupe.move_to_end(identity)
            return True
        return any(event.producer_identity == identity for event in self._pending_events)

    def _commit_producer_identities(self, events: Sequence[NormalizedHerdrEvent]) -> None:
        for event in events:
            identity = event.producer_identity
            if identity is None:
                continue
            self._producer_dedupe[identity] = None
            self._producer_dedupe.move_to_end(identity)
        while len(self._producer_dedupe) > self.dedupe_size:
            self._producer_dedupe.popitem(last=False)

    def flush(self) -> None:
        # Draining, application, persistence, and producer-ID commitment share
        # one lock scope so later batches cannot overtake an earlier flush.
        notify_turn_refresh = False
        completed_turns: list[HerdrTurnCompletionRecord] = []
        with self._lock:
            if not self._pending_events:
                return
            events = list(self._pending_events)
            accepted_at = utc_timestamp()
            self._pending_events.clear()
            has_producer_identity = any(event.producer_identity is not None for event in events)
            has_turn_refresh_event = any(
                event.name in _TURN_REFRESH_EVENT_NAMES for event in events
            )
            notify_turn_refresh = has_turn_refresh_event
            for event in events:
                if event.name != "pane.turn_completed":
                    continue
                try:
                    completed_turns.append(_turn_completion_record(event.payload))
                except HerdrEnvelopeError:
                    self._record_turn_diagnostic(
                        "herdr_turn_completion_record_quarantined",
                        _first_text(event.payload, ("pane_id", "paneId"))
                        or "unknown",
                    )
            try:
                self._event_continuity_revalidated = False
                changed = False
                for event in events:
                    changed = self._apply_event(event) or changed
                # Producer identities become durable only after this barrier.
                # It also persists dirty memory when a failed first attempt made
                # a retry appear idempotent before any snapshot reached storage.
                if changed or has_producer_identity:
                    self._persist_current_state(observed_at=accepted_at)
                self._commit_producer_identities(events)
            except (HerdrContinuityUnavailableError, InstallationKeyError):
                self._mark_unhealthy("continuity_unavailable")
            finally:
                self._event_continuity_revalidated = False
        for record in completed_turns:
            self._process_turn_record(record)
        if notify_turn_refresh:
            self._notify_turn_refresh()

    def _apply_event(self, event: NormalizedHerdrEvent) -> bool:
        if event.name in _SPACE_EVENT_NAMES:
            status = "closed" if event.name == "workspace.closed" else None
            return self._apply_space_event(event.payload, status=status)
        if event.name in _WORKTREE_EVENT_NAMES:
            status = "closed" if event.name == "worktree.removed" else None
            return self._apply_worktree_event(event.payload, status=status)
        if event.name in _PANE_WORKER_EVENT_NAMES:
            item, pane_info_observed = _pane_event_payload_with_provenance(
                event.payload,
                "pane",
                "agent",
                "worker",
                allow_top_level_pane_info=True,
            )
            if not _pane_has_agent(item) and self._match_binding(item) is None:
                return False
            return self._upsert_worker_from_item(
                item,
                pane_info_observed=pane_info_observed,
                identity_source=f"event:{event.name}",
            )
        if event.name == "pane.agent_detected":
            item, pane_info_observed = _pane_event_payload_with_provenance(
                event.payload,
                "agent",
                "worker",
                "pane",
            )
            return self._upsert_worker_from_item(
                item,
                pane_info_observed=pane_info_observed,
                identity_source=f"event:{event.name}",
            )
        if event.name == "pane.agent_status_changed":
            item, pane_info_observed = _pane_event_payload_with_provenance(
                event.payload,
                "agent",
                "worker",
                "pane",
            )
            raw_status = _first_text(item, ("status", "agent_status", "state", "phase"))
            return self._upsert_worker_from_item(
                item,
                status=normalize_status(raw_status),
                update_binding=(
                    pane_info_observed
                    and _has_authoritative_identity_tuple(item)
                    and _has_authoritative_binding_target(item)
                ),
                pane_info_observed=pane_info_observed,
                identity_source=f"event:{event.name}",
            )
        if event.name == "pane.moved":
            item, pane_info_observed = _pane_event_payload_with_provenance(
                event.payload,
                "pane",
                allow_top_level_pane_info=True,
            )
            return self._apply_pane_moved(
                item,
                pane_info_observed=pane_info_observed,
                identity_source=f"event:{event.name}",
            )
        if event.name in _CLOSED_EVENT_NAMES:
            item, pane_info_observed = _pane_event_payload_with_provenance(
                event.payload,
                "pane",
                allow_top_level_pane_info=True,
            )
            return self._apply_pane_closed(
                item,
                reason=event.name.replace(".", "_"),
                pane_info_observed=pane_info_observed,
                identity_source=f"event:{event.name}",
            )
        if event.name == "pane.output_matched":
            return False
        return False

    def _apply_space_event(self, payload: Mapping[str, Any], *, status: str | None = None) -> bool:
        item = _entity_payload(payload, "workspace", "space")
        direct_name_hint = _first_text(item, ("label", "name", "title"))
        rename_hint = _first_text(item, ("new_name", "newName"))
        if _first_text(item, ("workspace_id", "space_id", "id", "slug", "name", "label", "title")) is None:
            return False
        name_hint = direct_name_hint or rename_hint
        if rename_hint and direct_name_hint is None:
            item["name"] = rename_hint
        if status is not None:
            item["status"] = status
        spaces = _spaces_from_payload([item] if item else [])
        if not spaces:
            return False
        space = spaces[0]
        existing = self._spaces.get(space.id)
        if existing is not None and status is None:
            meta = dict(existing.meta)
            meta.update(space.meta)
            space = Space(
                id=existing.id,
                name=space.name if name_hint else existing.name,
                status=space.status,
                meta=meta,
                updated_at=space.updated_at or utc_timestamp(),
                status_line=space.status_line or existing.status_line,
            )
        if existing is not None and space.fingerprint == existing.fingerprint:
            return False
        self._spaces[space.id] = space
        return True

    def _apply_worktree_event(self, payload: Mapping[str, Any], *, status: str | None = None) -> bool:
        item = _entity_payload(payload, "workspace", "space", "worktree")
        workspace_id = _first_text(item, ("workspace_id", "space_id"))
        if workspace_id is None or workspace_id not in self._spaces:
            return False
        item.setdefault("id", workspace_id)
        if status is not None:
            item["status"] = status
        return self._apply_space_event({"workspace": item}, status=status)

    def _event_worker_and_binding(
        self,
        item: Mapping[str, Any],
        *,
        status: str | None = None,
        pane_info_observed: bool = False,
        identity_source: str = "event",
    ) -> tuple[Worker | None, WorkerBinding | None, WorkerBinding | None]:
        try:
            record = _worker_record_from_item(
                item,
                self.config,
                pane_info_observed=pane_info_observed,
                identity_source=identity_source,
            )
            if pane_info_observed and canonical_herdr_pane_identity(
                record.workspace_id,
                record.pane_id,
            ) is None:
                matched_owner = self._match_binding(item)
                existing_owner = (
                    self._workers.get(matched_owner.worker_id)
                    if matched_owner is not None
                    else None
                )
                if (
                    existing_owner is not None
                    and _authenticated_local_stable_key(existing_owner) is not None
                ):
                    raise HerdrContinuityUnavailableError(
                        "Herdr event PaneInfo has no canonical public pane identity"
                    )
            workers, bindings = _workers_and_bindings_from_records(
                self.config,
                [record],
                stored_bindings=self._current_bindings(),
                require_authenticated_continuity=True,
            )
        except (HerdrContinuityUnavailableError, InstallationKeyError):
            raise
        except Exception:
            return None, None, self._match_binding(item)
        worker = workers[0] if workers else None
        binding = bindings[0] if bindings else None
        matched_binding = self._match_binding(item)
        if matched_binding is not None and worker is not None:
            worker = _worker_copy(worker, worker_id=matched_binding.worker_id)
        if worker is not None and status is not None:
            worker = _worker_copy(worker, status=status)
        return worker, binding, matched_binding

    @staticmethod
    def _add_owner(
        owners: dict[str, set[str]],
        value: str | None,
        worker_id: str,
    ) -> None:
        if value:
            owners.setdefault(value, set()).add(worker_id)

    def _remove_owner(self, worker_id: str) -> None:
        for owners in (
            self._pane_owners,
            self._terminal_owners,
            self._session_owners,
        ):
            for value in list(owners):
                owner_ids = owners[value]
                owner_ids.discard(worker_id)
                if not owner_ids:
                    owners.pop(value, None)

    def _remember_item_owner(
        self,
        item: Mapping[str, Any],
        worker_id: str,
        *,
        replace: bool,
    ) -> None:
        if replace:
            self._remove_owner(worker_id)
        agent_session = _safe_mapping(_field_value(item, "agent_session"))
        session_id = (
            _first_text(agent_session, ("value", "id"))
            or _first_text(item, ("session_id", "sessionId"))
        )
        self._add_owner(
            self._pane_owners,
            _first_text(item, ("pane_id", "paneId")),
            worker_id,
        )
        self._add_owner(
            self._terminal_owners,
            _first_text(item, ("terminal_id", "terminalId")),
            worker_id,
        )
        self._add_owner(self._session_owners, session_id, worker_id)

    def _replace_ownership_maps(
        self,
        records: Sequence[Any],
        bindings: Sequence[WorkerBinding],
    ) -> None:
        self._pane_terminals = {}
        self._pane_owners = {}
        self._terminal_owners = {}
        self._session_owners = {}
        worker_ids_by_private: dict[str, set[str]] = {}
        for binding in bindings:
            worker_ids_by_private.setdefault(
                binding.private_fingerprint,
                set(),
            ).add(binding.worker_id)
            if binding.target_kind == "pane_id":
                self._add_owner(
                    self._pane_owners,
                    binding.target_value,
                    binding.worker_id,
                )
            if binding.target_kind == "terminal_id":
                self._add_owner(
                    self._terminal_owners,
                    binding.target_value,
                    binding.worker_id,
                )
            if binding.turn_target_value:
                self._add_owner(
                    self._session_owners,
                    binding.turn_target_value,
                    binding.worker_id,
                )
        for record in records:
            if not record.pane_info_observed:
                continue
            owner_ids = worker_ids_by_private.get(
                record.private_fingerprint,
                set(),
            )
            if len(owner_ids) != 1:
                continue
            worker_id = next(iter(owner_ids))
            observed_pane_id = record.observed_pane_id or record.pane_id
            if observed_pane_id and record.terminal_id:
                self._pane_terminals[observed_pane_id] = record.terminal_id
            self._add_owner(
                self._pane_owners,
                observed_pane_id,
                worker_id,
            )
            self._add_owner(
                self._terminal_owners,
                record.terminal_id,
                worker_id,
            )
            self._add_owner(
                self._session_owners,
                record.agent_session_id,
                worker_id,
            )

    def _ownership_worker_ids(
        self,
        item: Mapping[str, Any],
        observed_binding: WorkerBinding | None = None,
    ) -> set[str]:
        owner_ids = {
            binding.worker_id
            for binding in self._matching_bindings(item)
            if binding.worker_id
        }
        if observed_binding is not None:
            owner_ids.update(
                binding.worker_id
                for binding in self._bindings.values()
                if binding.worker_id
                and binding.target_value
                == observed_binding.target_value
            )
        pane_id = _first_text(item, ("pane_id", "paneId"))
        terminal_id = _first_text(item, ("terminal_id", "terminalId"))
        agent_session = _safe_mapping(_field_value(item, "agent_session"))
        session_id = (
            _first_text(agent_session, ("value", "id"))
            or _first_text(item, ("session_id", "sessionId"))
        )
        owner_ids.update(self._pane_owners.get(pane_id or "", ()))
        owner_ids.update(self._terminal_owners.get(terminal_id or "", ()))
        owner_ids.update(self._session_owners.get(session_id or "", ()))
        return owner_ids

    def _target_worker_ids(
        self,
        target_kind: str,
        target_value: str,
    ) -> set[str]:
        owner_ids = {
            binding.worker_id
            for binding in self._bindings.values()
            if binding.worker_id
            and (
                binding.target_value == target_value
                or (
                    binding.turn_target_kind == target_kind
                    and binding.turn_target_value == target_value
                )
            )
        }
        if target_kind == "pane_id":
            owner_ids.update(self._pane_owners.get(target_value, ()))
        if target_kind == "terminal_id":
            owner_ids.update(self._terminal_owners.get(target_value, ()))
        return owner_ids


    def _matching_bindings(
        self,
        item: Mapping[str, Any],
        *,
        old_first: bool = False,
    ) -> list[WorkerBinding]:
        pairs = _target_pairs_from_item(item, old_first=old_first)
        mapped_pairs = [
            ("terminal_id", self._pane_terminals[value])
            for kind, value in pairs
            if kind == "pane_id" and value in self._pane_terminals
        ]
        agent_session = _safe_mapping(_field_value(item, "agent_session"))
        session_id = (
            _first_text(agent_session, ("value", "id"))
            or _first_text(item, ("session_id", "sessionId"))
        )
        keys = set([*pairs, *mapped_pairs])
        if session_id:
            keys.add(("agent_session", session_id))
        if not keys:
            return []
        return [
            binding
            for binding in self._bindings.values()
            if _binding_target(binding) in keys
            or (
                str(binding.turn_target_kind or ""),
                str(binding.turn_target_value or ""),
            )
            in keys
            or (
                "agent_session",
                str(binding.turn_target_value or ""),
            )
            in keys
        ]

    def _fail_closed_ownership(self, worker_ids: set[str]) -> bool:
        """Remove continuity and routing from every current ambiguous owner."""
        if not worker_ids:
            return False
        if (
            self._health.outcome == "continuity_unavailable"
            and not self._event_continuity_revalidated
        ):
            return False
        changed = False
        binding_updates: list[WorkerBinding] = []
        bindings_by_worker: dict[str, list[WorkerBinding]] = {}
        for private_fingerprint, binding in list(self._bindings.items()):
            if binding.worker_id not in worker_ids:
                continue
            bindings_by_worker.setdefault(binding.worker_id, []).append(binding)
            ambiguous = WorkerBinding(
                host_id=binding.host_id,
                worker_id=binding.worker_id,
                worker_fingerprint=binding.worker_fingerprint,
                backend=binding.backend,
                target_kind=binding.target_kind,
                target_value=binding.target_value,
                turn_target_kind=None,
                turn_target_value=None,
                sendable=False,
                reason="ambiguous_pane_match",
                observed_at=utc_timestamp(),
                expires_at=binding.expires_at,
                private_fingerprint=binding.private_fingerprint,
            )
            if _binding_state_equal(binding, ambiguous):
                continue
            self._bindings[private_fingerprint] = ambiguous
            binding_updates.append(ambiguous)
            changed = True

        for worker_id in worker_ids:
            worker = self._workers.get(worker_id)
            if worker is None:
                continue
            target = worker.backend_target
            if not isinstance(target, Mapping):
                candidates = bindings_by_worker.get(worker_id, [])
                target = candidates[0].backend_target() if candidates else None
            ambiguous_target = None
            if isinstance(target, Mapping):
                kind = str(target.get("kind") or "")
                value = str(target.get("value") or "")
                if kind and value:
                    ambiguous_target = {
                        "kind": kind,
                        "value": value,
                        "sendable": False,
                        "reason": "ambiguous_pane_match",
                    }
            ambiguous_worker = _worker_copy(
                worker,
                meta=_strip_stable_key_fields(worker.meta),
                backend_target=ambiguous_target,
            )
            if _worker_state_equal(worker, ambiguous_worker):
                continue
            self._workers[worker_id] = ambiguous_worker
            changed = True
        if binding_updates:
            upsert_worker_bindings(self.db_path, binding_updates)
        return changed


    def _upsert_worker_from_item(
        self,
        item: Mapping[str, Any],
        *,
        status: str | None = None,
        update_binding: bool = True,
        pane_info_observed: bool = False,
        identity_source: str = "event",
    ) -> bool:
        if not item:
            return False
        if not _has_public_worker_identity(item) and self._match_binding(item) is None:
            return False
        worker, binding, matched_binding = self._event_worker_and_binding(
            item,
            status=status,
            pane_info_observed=pane_info_observed,
            identity_source=identity_source,
        )
        if worker is None:
            return False

        authoritative_identity = (
            pane_info_observed and _has_authoritative_identity_tuple(item)
        )
        stable_owner_reused = False
        if authoritative_identity:
            observed_stable_key = _authenticated_local_stable_key(worker)
            stable_owner_ids = (
                {
                    current.id
                    for current in self._workers.values()
                    if _authenticated_local_stable_key(current)
                    == observed_stable_key
                }
                if observed_stable_key is not None
                else set()
            )
            target_owner_ids = self._ownership_worker_ids(
                item,
                binding,
            )
            conflicting_owner_ids: set[str] = set()
            if len(stable_owner_ids) > 1 or len(target_owner_ids) > 1:
                conflicting_owner_ids.update(stable_owner_ids)
                conflicting_owner_ids.update(target_owner_ids)
            elif len(stable_owner_ids) == 1:
                stable_owner_id = next(iter(stable_owner_ids))
                other_target_owners = target_owner_ids - {stable_owner_id}
                if other_target_owners:
                    conflicting_owner_ids.add(stable_owner_id)
                    conflicting_owner_ids.update(other_target_owners)
                else:
                    worker = _worker_copy(
                        worker,
                        worker_id=stable_owner_id,
                    )
                    stable_owner_reused = True
            elif len(target_owner_ids) == 1:
                target_owner_id = next(iter(target_owner_ids))
                target_owner = self._workers.get(target_owner_id)
                target_stable_key = (
                    _authenticated_local_stable_key(target_owner)
                    if target_owner is not None
                    else None
                )
                observed_pane_id = _first_text(
                    item,
                    ("pane_id", "paneId"),
                )
                observed_canonical_identity = canonical_herdr_pane_identity(
                    _first_text(item, ("workspace_id", "workspaceId")),
                    observed_pane_id,
                )
                same_pane_owner_ids = self._pane_owners.get(
                    observed_pane_id or "",
                    set(),
                )
                target_is_ambiguous = any(
                    current_binding.worker_id == target_owner_id
                    and current_binding.reason == "ambiguous_pane_match"
                    for current_binding in self._bindings.values()
                )
                if target_is_ambiguous or (
                    observed_stable_key is None
                    and (
                        observed_canonical_identity is None
                        or target_owner_id not in same_pane_owner_ids
                    )
                ) or (
                    observed_stable_key is not None
                    and target_stable_key is not None
                    and target_stable_key != observed_stable_key
                ):
                    conflicting_owner_ids.add(target_owner_id)
                else:
                    worker = _worker_copy(
                        worker,
                        worker_id=target_owner_id,
                    )
                    stable_owner_reused = True
            if conflicting_owner_ids:
                return self._fail_closed_ownership(
                    conflicting_owner_ids
                )
        else:
            compatibility_owner_ids = self._ownership_worker_ids(
                item,
                binding,
            )
            compatibility_identity = canonical_herdr_pane_identity(
                _first_text(item, ("workspace_id", "workspaceId")),
                _first_text(item, ("pane_id", "paneId")),
            )
            pane_owner_ids = (
                self._pane_owners.get(compatibility_identity[1], set())
                if compatibility_identity is not None
                else set()
            )
            if (
                matched_binding is None
                and len(compatibility_owner_ids) == 1
                and compatibility_owner_ids <= pane_owner_ids
            ):
                compatibility_owner_id = next(iter(compatibility_owner_ids))
                owner_bindings = [
                    current_binding
                    for current_binding in self._bindings.values()
                    if current_binding.worker_id == compatibility_owner_id
                ]
                if len(owner_bindings) == 1:
                    # Scalar events can locate an already-authenticated owner
                    # through the private pane map, but cannot replace that
                    # owner's identity or binding with event-only fields.
                    matched_binding = owner_bindings[0]
            matched_owner_ids = (
                {matched_binding.worker_id}
                if matched_binding is not None
                else set()
            )
            if compatibility_owner_ids - matched_owner_ids:
                return self._fail_closed_ownership(
                    compatibility_owner_ids
                )

        existing = self._workers.get(worker.id)
        if existing is None and matched_binding is not None:
            existing = self._workers.get(matched_binding.worker_id)
        if matched_binding is not None and not stable_owner_reused:
            worker = _worker_copy(worker, worker_id=matched_binding.worker_id)
        worker = _merge_worker_update(
            existing,
            worker,
            status=status,
            preserve_existing_continuity=not authoritative_identity,
        )
        if not authoritative_identity and matched_binding is not None:
            worker = _worker_copy(
                worker,
                backend_target=matched_binding.backend_target(),
            )
        if self._would_exceed_worker_cap(worker, existing=existing):
            self._mark_worker_cap_exceeded_locked(_observed_worker_count(list(self._workers.values())) + 1)
            return False
        changed = existing is None or not _worker_state_equal(existing, worker)
        if changed:
            self._workers[worker.id] = worker
        else:
            assert existing is not None
            worker = existing
        if update_binding and binding is not None:
            if stable_owner_reused:
                stale_private_fingerprints = [
                    private_fingerprint
                    for private_fingerprint, current_binding in self._bindings.items()
                    if current_binding.worker_id == worker.id
                    and private_fingerprint != binding.private_fingerprint
                ]
                if stale_private_fingerprints:
                    expire_worker_bindings(
                        self.db_path,
                        self.config.host_id,
                        backend=BACKEND_NAME,
                        private_fingerprints=stale_private_fingerprints,
                        reason="identity_replaced",
                    )
                    for private_fingerprint in stale_private_fingerprints:
                        self._bindings.pop(private_fingerprint, None)
                    changed = True
                binding = self._binding_with_worker(binding, worker)
            elif matched_binding is not None and (
                not authoritative_identity
                or binding.private_fingerprint
                != matched_binding.private_fingerprint
            ):
                binding = self._binding_with_worker(matched_binding, worker)
            else:
                binding = self._binding_with_worker(binding, worker)
            current_binding = self._bindings.get(binding.private_fingerprint)
            if current_binding is None or not _binding_state_equal(current_binding, binding):
                self._bindings[binding.private_fingerprint] = binding
                upsert_worker_bindings(self.db_path, [binding])
                changed = True
        if authoritative_identity:
            self._remember_item_owner(
                item,
                worker.id,
                replace=update_binding,
            )
            self._note_pane_terminal(item)
            if _authenticated_local_stable_key(worker) is not None:
                if self._health.outcome == "continuity_unavailable":
                    changed = True
                self._event_continuity_revalidated = True
        return changed

    def _binding_with_worker(self, binding: WorkerBinding, worker: Worker) -> WorkerBinding:
        return WorkerBinding(
            host_id=binding.host_id,
            worker_id=worker.id,
            worker_fingerprint=worker.fingerprint,
            backend=binding.backend,
            target_kind=binding.target_kind,
            target_value=binding.target_value,
            turn_target_kind=binding.turn_target_kind,
            turn_target_value=binding.turn_target_value,
            sendable=binding.sendable,
            reason=binding.reason,
            observed_at=utc_timestamp(),
            expires_at=None,
            private_fingerprint=binding.private_fingerprint,
        )

    def _match_binding(self, item: Mapping[str, Any], *, old_first: bool = False) -> WorkerBinding | None:
        pairs = _target_pairs_from_item(item, old_first=old_first)
        if not pairs:
            return None
        binding_by_target = {_binding_target(binding): binding for binding in self._bindings.values()}
        # Event payloads often carry only a pane id while stored bindings target
        # a terminal id; fall back to the turn target so a known pane never
        # spawns a duplicate re-lettered worker.
        for binding in self._bindings.values():
            turn_key = (str(binding.turn_target_kind or ""), str(binding.turn_target_value or ""))
            if turn_key[0] and turn_key[1]:
                binding_by_target.setdefault(turn_key, binding)
        # Translate pane ids to the terminal ids remembered from the last
        # reconcile: agent kinds whose turn target is not a pane id (codex
        # session ids) would otherwise never match a pane-id-only event.
        mapped_pairs = [
            ("terminal_id", self._pane_terminals[value])
            for kind, value in pairs
            if kind == "pane_id" and value in self._pane_terminals
        ]
        for pair in [*pairs, *mapped_pairs]:
            binding = binding_by_target.get(pair)
            if binding is not None:
                return binding
        return None

    def _previous_ownership_worker_ids(
        self,
        item: Mapping[str, Any],
    ) -> set[str]:
        previous_pane_id = _first_text(
            item,
            (
                "old_pane_id",
                "previous_pane_id",
                "from_pane_id",
                "source_pane_id",
            ),
        )
        previous_terminal_id = _first_text(
            item,
            (
                "old_terminal_id",
                "previous_terminal_id",
                "from_terminal_id",
                "source_terminal_id",
            ),
        )
        owner_ids = set(self._pane_owners.get(previous_pane_id or "", ()))
        owner_ids.update(
            self._terminal_owners.get(previous_terminal_id or "", ())
        )
        if previous_pane_id and previous_pane_id in self._pane_terminals:
            previous_terminal_id = self._pane_terminals[previous_pane_id]
            owner_ids.update(
                self._terminal_owners.get(previous_terminal_id, ())
            )
        previous_keys = {
            ("pane_id", previous_pane_id or ""),
            ("terminal_id", previous_terminal_id or ""),
        }
        owner_ids.update(
            binding.worker_id
            for binding in self._bindings.values()
            if binding.worker_id
            and (
                _binding_target(binding) in previous_keys
                or (
                    str(binding.turn_target_kind or ""),
                    str(binding.turn_target_value or ""),
                )
                in previous_keys
            )
        )
        return owner_ids


    def _note_pane_terminal(self, item: Mapping[str, Any]) -> None:
        pane_id = _first_text(item, ("pane_id", "paneId"))
        terminal_id = _first_text(item, ("terminal_id", "terminalId"))
        if pane_id and terminal_id:
            self._pane_terminals[pane_id] = terminal_id

    def _apply_pane_moved(
        self,
        item: Mapping[str, Any],
        *,
        pane_info_observed: bool = False,
        identity_source: str = "event:pane.moved",
    ) -> bool:
        authoritative_identity = (
            pane_info_observed and _has_authoritative_identity_tuple(item)
        )
        observed_worker, observed_binding, _matched = self._event_worker_and_binding(
            item,
            pane_info_observed=pane_info_observed,
            identity_source=identity_source,
        )
        source_owner_ids = self._previous_ownership_worker_ids(item)
        if len(source_owner_ids) > 1:
            return self._fail_closed_ownership(source_owner_ids)
        if not source_owner_ids:
            return False
        source_owner_id = next(iter(source_owner_ids))
        existing = self._workers.get(source_owner_id)
        if existing is None:
            return False
        source_bindings = [
            binding
            for binding in self._bindings.values()
            if binding.worker_id == source_owner_id
        ]
        new_target = _new_move_target(item)
        if new_target is not None:
            destination_owner_ids = self._target_worker_ids(*new_target)
            conflicting_destination_ids = destination_owner_ids - {
                source_owner_id
            }
            if conflicting_destination_ids:
                return self._fail_closed_ownership(
                    {source_owner_id, *conflicting_destination_ids}
                )

        if authoritative_identity:
            if observed_worker is None:
                return False
            observed_stable_key = _authenticated_local_stable_key(
                observed_worker
            )
            destination_owner_ids = self._ownership_worker_ids(
                item,
                observed_binding,
            )
            if observed_stable_key is not None:
                destination_owner_ids.update(
                    current.id
                    for current in self._workers.values()
                    if _authenticated_local_stable_key(current)
                    == observed_stable_key
                )
            conflicting_owner_ids = destination_owner_ids - {
                source_owner_id
            }
            if conflicting_owner_ids:
                return self._fail_closed_ownership(
                    {source_owner_id, *conflicting_owner_ids}
                )
            worker = _merge_worker_update(
                existing,
                _worker_copy(
                    observed_worker,
                    worker_id=source_owner_id,
                ),
                preserve_existing_continuity=False,
            )
        else:
            worker = existing
            if observed_worker is not None:
                worker = _merge_worker_update(
                    existing,
                    _worker_copy(
                        observed_worker,
                        worker_id=source_owner_id,
                    ),
                    preserve_existing_continuity=True,
                )

        if self._would_exceed_worker_cap(worker, existing=existing):
            self._mark_worker_cap_exceeded_locked(
                _observed_worker_count(list(self._workers.values())) + 1
            )
            return False

        if authoritative_identity and observed_binding is not None:
            if len(source_bindings) != 1:
                return self._fail_closed_ownership({source_owner_id})
            old_binding = source_bindings[0]
            moved_binding = WorkerBinding(
                host_id=observed_binding.host_id,
                worker_id=worker.id,
                worker_fingerprint=worker.fingerprint,
                backend=observed_binding.backend,
                target_kind=observed_binding.target_kind,
                target_value=observed_binding.target_value,
                turn_target_kind=observed_binding.turn_target_kind,
                turn_target_value=observed_binding.turn_target_value,
                sendable=observed_binding.sendable,
                reason=observed_binding.reason,
                observed_at=utc_timestamp(),
                expires_at=None,
                private_fingerprint=old_binding.private_fingerprint,
            )
        else:
            if new_target is None or len(source_bindings) != 1:
                return False
            old_binding = source_bindings[0]
            target_kind, target_value = new_target
            moved_binding = WorkerBinding(
                host_id=old_binding.host_id,
                worker_id=worker.id,
                worker_fingerprint=worker.fingerprint,
                backend=old_binding.backend,
                target_kind=target_kind,
                target_value=target_value,
                turn_target_kind=old_binding.turn_target_kind,
                turn_target_value=(
                    target_value
                    if old_binding.turn_target_kind == "pane_id"
                    and target_kind == "pane_id"
                    else old_binding.turn_target_value
                ),
                sendable=old_binding.sendable,
                reason=old_binding.reason,
                observed_at=utc_timestamp(),
                expires_at=None,
                private_fingerprint=old_binding.private_fingerprint,
            )

        stale_private_fingerprints = [
            binding.private_fingerprint
            for binding in source_bindings
            if binding.private_fingerprint
            != moved_binding.private_fingerprint
        ]
        if stale_private_fingerprints:
            expire_worker_bindings(
                self.db_path,
                self.config.host_id,
                backend=BACKEND_NAME,
                private_fingerprints=stale_private_fingerprints,
                reason="identity_replaced",
            )
            for private_fingerprint in stale_private_fingerprints:
                self._bindings.pop(private_fingerprint, None)

        worker = _worker_copy(
            worker,
            backend_target=moved_binding.backend_target(),
        )
        moved_binding = self._binding_with_worker(moved_binding, worker)
        self._workers[worker.id] = worker
        self._bindings[moved_binding.private_fingerprint] = moved_binding
        upsert_worker_bindings(self.db_path, [moved_binding])

        previous_pane_ids: set[str] = set()
        previous_pane_id = _first_text(
            item,
            (
                "old_pane_id",
                "previous_pane_id",
                "from_pane_id",
                "source_pane_id",
            ),
        )
        if previous_pane_id:
            previous_pane_ids.add(previous_pane_id)
        previous_terminal_id = _first_text(
            item,
            (
                "old_terminal_id",
                "previous_terminal_id",
                "from_terminal_id",
                "source_terminal_id",
            ),
        )
        if previous_terminal_id:
            previous_pane_ids.update(
                pane_id
                for pane_id, terminal_id in self._pane_terminals.items()
                if terminal_id == previous_terminal_id
                and source_owner_id in self._pane_owners.get(pane_id, ())
            )
        current_pane_id = _first_text(item, ("pane_id", "paneId"))
        for source_pane_id in previous_pane_ids:
            if source_pane_id != current_pane_id:
                self._pane_terminals.pop(source_pane_id, None)

        self._remove_owner(worker.id)
        if authoritative_identity:
            self._remember_item_owner(
                item,
                worker.id,
                replace=False,
            )
            self._note_pane_terminal(item)
            if _authenticated_local_stable_key(worker) is not None:
                self._event_continuity_revalidated = True
        else:
            if moved_binding.target_kind == "pane_id":
                self._add_owner(
                    self._pane_owners,
                    moved_binding.target_value,
                    worker.id,
                )
            if moved_binding.target_kind == "terminal_id":
                self._add_owner(
                    self._terminal_owners,
                    moved_binding.target_value,
                    worker.id,
                )
            if moved_binding.turn_target_value:
                self._add_owner(
                    self._session_owners,
                    moved_binding.turn_target_value,
                    worker.id,
                )
        return True

    def _apply_pane_closed(
        self,
        item: Mapping[str, Any],
        *,
        reason: str,
        pane_info_observed: bool = False,
        identity_source: str = "event",
    ) -> bool:
        observed_worker: Worker | None = None
        matched_binding: WorkerBinding | None = None
        observed_binding: WorkerBinding | None = None
        if pane_info_observed:
            observed_worker, observed_binding, matched_binding = self._event_worker_and_binding(
                item,
                status="closed",
                pane_info_observed=True,
                identity_source=identity_source,
            )
        observed_stable_key = (
            _authenticated_local_stable_key(observed_worker)
            if observed_worker is not None
            else None
        )
        if observed_stable_key is not None:
            stable_owner_ids = {
                current.id
                for current in self._workers.values()
                if _authenticated_local_stable_key(current) == observed_stable_key
            }
            target_owner_ids = self._ownership_worker_ids(item, observed_binding)
            if len(stable_owner_ids | target_owner_ids) > 1:
                return False
        binding = matched_binding or self._match_binding(item)
        worker: Worker | None = None
        if binding is not None:
            worker = self._workers.get(binding.worker_id)
        if worker is None:
            if binding is None and not _has_public_worker_identity(item):
                return False
            if observed_worker is None:
                observed_worker, _event_binding, matched_binding = self._event_worker_and_binding(
                    item,
                    status="closed",
                    pane_info_observed=False,
                    identity_source=identity_source,
                )
            if matched_binding is not None:
                binding = matched_binding
            worker = observed_worker
        if worker is None:
            return False
        if binding is None and self._would_exceed_worker_cap(worker, existing=self._workers.get(worker.id)):
            self._mark_worker_cap_exceeded_locked(_observed_worker_count(list(self._workers.values())) + 1)
            return False
        closed = _closed_worker(worker)
        current = self._workers.get(closed.id)
        changed = current is None or not _worker_state_equal(current, closed)
        if changed:
            self._workers[closed.id] = closed
        if binding is not None:
            expire_worker_bindings(
                self.db_path,
                self.config.host_id,
                backend=BACKEND_NAME,
                private_fingerprints=[binding.private_fingerprint],
                now=utc_timestamp(),
                reason=reason,
            )
            self._bindings.pop(binding.private_fingerprint, None)
            changed = True
        elif changed:
            expire_worker_bindings(
                self.db_path,
                self.config.host_id,
                backend=BACKEND_NAME,
                worker_id=closed.id,
                now=utc_timestamp(),
                reason=reason,
            )
        if (
            pane_info_observed
            and observed_worker is not None
            and _authenticated_local_stable_key(observed_worker) is not None
        ):
            if self._health.outcome == "continuity_unavailable":
                changed = True
            self._event_continuity_revalidated = True
        return changed

    def _persist_current_state(self, *, observed_at: str | None = None) -> Snapshot:
        accepted_at = observed_at or utc_timestamp()
        spaces = list(self._spaces.values())
        workers = list(self._workers.values())
        if (
            self._health.outcome == "continuity_unavailable"
            and not self._event_continuity_revalidated
        ):
            health = self._health.to_backend_health(spaces=spaces, workers=workers)
        else:
            outcome = (
                "healthy_non_empty"
                if spaces or _observed_worker_count(workers)
                else "empty_healthy"
            )
            health = herdr_backend_health(
                outcome,
                observed_at=accepted_at,
                spaces=spaces,
                workers=workers,
            )
        snapshot = project_from_observations(
            self.config,
            spaces=spaces,
            workers=workers,
            backend_health=[health],
        )
        self._save_snapshot(
            snapshot,
            observation=SnapshotObservationContext(
                authority="positive" if health.status == "healthy" else "none",
                observed_at=health.observed_at or accepted_at,
            ),
        )
        self._last_snapshot_at = snapshot.updated_at
        self._health = HerdrEventBackendHealth(
            status=health.status,
            outcome=health.outcome,
            observed_at=health.observed_at or snapshot.updated_at,
            message=health.message,
        )
        return snapshot

    def _would_exceed_worker_cap(self, worker: Worker, *, existing: Worker | None = None) -> bool:
        if worker.status == "closed":
            return False
        previous = existing if existing is not None else self._workers.get(worker.id)
        if previous is not None and previous.status != "closed":
            return False
        return _observed_worker_count(list(self._workers.values())) + 1 > self.max_workers

    def _mark_worker_cap_exceeded_locked(self, observed_workers: int) -> Snapshot:
        now = utc_timestamp()
        previous = latest_snapshot(self.db_path, self.config.host_id)
        spaces = list(previous.spaces) if previous is not None else list(self._spaces.values())
        workers = list(previous.workers) if previous is not None else list(self._workers.values())
        if (
            self._health.outcome == "continuity_unavailable"
            and not self._event_continuity_revalidated
        ):
            health = self._health.to_backend_health(spaces=spaces, workers=workers)
        else:
            health = herdr_backend_health(
                "worker_cap_exceeded",
                observed_at=now,
                message="Herdr observation exceeded the configured worker cap",
                spaces=spaces,
                workers=workers,
            )
        snapshot = project_from_observations(
            self.config,
            spaces=spaces,
            workers=workers,
            backend_health=[health],
        )
        self._save_snapshot(
            snapshot,
            observation=SnapshotObservationContext(
                authority="none",
                observed_at=health.observed_at or now,
            ),
        )
        self._spaces = {space.id: space for space in snapshot.spaces}
        self._workers = {worker.id: worker for worker in snapshot.workers}
        self._health = HerdrEventBackendHealth(
            status=health.status,
            outcome=health.outcome,
            observed_at=health.observed_at or now,
            message=health.message,
        )
        self._last_cap_status_at = now
        self._last_reconcile_at = now
        self._last_snapshot_at = snapshot.updated_at
        self._schedule_next_reconcile()
        return snapshot

    def _mark_unhealthy(self, outcome: str) -> Snapshot:
        with self._lock:
            if (
                self._health.outcome == "continuity_unavailable"
                and not self._event_continuity_revalidated
            ):
                health_state = self._health
            else:
                health_state = self._health_for(outcome)
            self._health = health_state
            self._event_continuity_revalidated = False
            spaces = list(self._spaces.values())
            workers = list(self._workers.values())
            if not spaces and not workers:
                snapshot = latest_snapshot(self.db_path, self.config.host_id)
                if snapshot is not None:
                    spaces = list(snapshot.spaces)
                    workers = list(snapshot.workers)
            health = health_state.to_backend_health(spaces=spaces, workers=workers)
            snapshot = project_from_observations(
                self.config,
                spaces=spaces,
                workers=workers,
                backend_health=[health],
            )
            self._save_snapshot(
                snapshot,
                observation=SnapshotObservationContext(
                    authority="none",
                    observed_at=health.observed_at or snapshot.updated_at,
                ),
            )
            self._last_snapshot_at = snapshot.updated_at
            self._spaces = {space.id: space for space in snapshot.spaces}
            self._workers = {worker.id: worker for worker in snapshot.workers}
            return snapshot

    def _mark_unhealthy_safe(self, outcome: str) -> Snapshot | None:
        try:
            return self._mark_unhealthy(outcome)
        finally:
            self._ready.set()
