"""Structured turn ingestion through Tendwire-owned private Herdr bindings."""

from __future__ import annotations

import base64
import hashlib
import json
import multiprocessing
import os
import re
import secrets
import select
import shlex
import socket
import stat
import struct
import subprocess
import threading
import time
from collections import OrderedDict, deque
from collections.abc import Callable, Mapping
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field, replace
from datetime import datetime
from pathlib import Path
from typing import Any, Literal
from uuid import UUID

from ..config import Config
from ..core.models import WorkerBinding, stable_fingerprint, utc_timestamp
from ..core.turns import (
    InteractionChoice,
    PendingObservation,
    PendingObservedChoice,
    is_internal_automation_turn_payload,
    redact_private_prompt_text,
)
from ..store.sqlite import (
    apply_turn_refresh,
    latest_turn_id_for_worker,
    list_worker_bindings,
    prune_backend_pending,
)


_TURN_CONTENT_KEYS = (
    "user_text",
    "assistant_final_text",
    "assistant_stream_text",
    "model",
    "complete",
    "has_open_turn",
    "awaiting_input",
    "pending_decision",
)
_CODEX_SESSION_TURN_KIND = "codex_session_id"
_OMP_SESSION_TURN_KIND = "omp_session_path"
_PANE_TURN_KIND = "pane_id"
_MAX_CODEX_STREAM_MESSAGES = 4
_CODEX_RECORD_MAX_BYTES = 8 * 1024 * 1024
_CODEX_TURN_ID_MAX_BYTES = 1024
_CODEX_READ_CHUNK_BYTES = 64 * 1024
_CODEX_RESYNC_INITIAL_BYTES = 64 * 1024
_CODEX_RESYNC_MAX_BYTES = 16 * 1024 * 1024
_CODEX_RESYNC_MAX_RECORDS = 65_536
_CODEX_INDEX_MAX_DEPTH = 4
_CODEX_INDEX_MAX_VISITS = 100_000
_CODEX_INDEX_MAX_ENTRIES = _CODEX_INDEX_MAX_VISITS
_CODEX_INDEX_MAX_BYTES = 16 * 1024 * 1024
_CODEX_PATH_CACHE_CAPACITY = 256
_CODEX_PATH_CACHE_MAX_BYTES = 256 * 1024
_CODEX_NEGATIVE_TTL_SECONDS = 2.0
# A found path is inode-validated on every hit. Duplicate discovery is
# intentionally bounded-stale by this complete-index refresh interval so the
# daemon never walks a 20k tree on each two-second poll.
_CODEX_POSITIVE_TTL_SECONDS = 60.0
_CODEX_SESSION_CACHE_CAPACITY = 64
_CODEX_SESSION_CACHE_MAX_BYTES = 16 * 1024 * 1024
_CODEX_STATE_IPC_MAX_BYTES = 12 * 1024 * 1024
_CODEX_IPC_FRAME_MAX_BYTES = 64 * 1024 * 1024
_CODEX_POLL_MAX_BYTES = 64 * 1024 * 1024
_OMP_IPC_RESPONSE_CHUNK_BYTES = 1024 * 1024
_OMP_TAIL_BYTES = 786432
_OMP_TOOL_SNIPPET_CHARS = 160
_OMP_SESSION_CACHE_CAPACITY = 64
_OMP_SESSION_CACHE_MAX_BYTES = 64 * 1024
_OMP_REQUEST_MAX_BYTES = 16 * 1024
_OMP_TARGET_MAX_CHARS = 4096
_OMP_TEARDOWN_GRACE_SECONDS = 0.25
_OMP_FRAME_HEADER = struct.Struct("!Q")
_UNCHANGED_TURN = object()
_PROMPTLESS_STATUS_FINAL_RE = re.compile(
    r"\b(?:"
    r"standing by|"
    r"waiting(?: quietly)?|"
    r"wait for|"
    r"no new state|"
    r"repeat tick|"
    r"startup line|"
    r"initial state|"
    r"current state|"
    r"monitor(?: reports|ing)?|"
    r"review in progress|"
    r"gate (?:phase|verdict|held|reached)|"
    r"not yet (?:reached|materialized)|"
    r"handoff|"
    r"phase"
    r")\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class _CodexPathResolution:
    status: str
    root: str
    root_file_id: tuple[int, int]
    session_id: str
    canonical_path: str | None
    relative_path: str | None
    file_id: tuple[int, int] | None
    generation: int
    expires_at: float


@dataclass(frozen=True)
class _CodexIndexGeneration:
    root: str
    root_signature: tuple[int, int, int, int]
    generation: int
    built_at: float
    entries: Mapping[str, tuple[str, ...]]
    retained_bytes: int
    visited: int
    overflowed: bool


@dataclass(frozen=True)
class _CodexRecordSpan:
    start: int
    end: int


@dataclass(frozen=True)
class _CodexSessionState:
    resolver_generation: int
    root: str
    session_id: str
    canonical_path: str
    file_id: tuple[int, int] | None
    observed_size: int
    mtime_ns: int
    ctime_ns: int
    committed_offset: int
    partial_record: bytes
    active_turn_id: str
    last_content_turn_id: str
    turn_open: bool
    final_seen: bool
    complete: bool
    stream_spans: tuple[_CodexRecordSpan, ...]
    internal_turn: bool = False
    root_file_id: tuple[int, int] | None = None


@dataclass(frozen=True)
class _CodexSemanticEvent:
    kind: str
    turn_id: str
    text: str = ""


@dataclass
class _CodexWorkState:
    resolver_generation: int
    root: str
    session_id: str
    canonical_path: str
    file_id: tuple[int, int]
    observed_size: int
    mtime_ns: int
    ctime_ns: int
    committed_offset: int = 0
    partial_record: bytes = b""
    active_turn_id: str = ""
    last_content_turn_id: str = ""
    turn_open: bool = False
    final_seen: bool = False
    complete: bool = False
    internal_turn: bool = False
    root_file_id: tuple[int, int] | None = None
    stream_items: list[tuple[_CodexRecordSpan, str]] = field(default_factory=list)
    user_text: str | None = None
    final_text: str | None = None
    public_changed: bool = False


_CODEX_ROLLOUT_RE = re.compile(
    r"^rollout-(\d{4})-(\d{2})-(\d{2})T"
    r"(\d{2})-(\d{2})-(\d{2})-"
    r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"
    r"\.jsonl$",
    re.ASCII,
)
_CODEX_PATH_CACHE: OrderedDict[tuple[str, str], _CodexPathResolution] = OrderedDict()
_CODEX_PATH_CACHE_LOCK = threading.RLock()
_CODEX_INDEX_GENERATION: _CodexIndexGeneration | None = None
_CODEX_INDEX_GENERATION_COUNTER = 0
_CODEX_RESOLUTION_GENERATION_COUNTER = 0
_CODEX_INDEX_BUILD_OBSERVER: Callable[[int], None] | None = None
_CODEX_SESSION_CACHE: OrderedDict[tuple[str, str], _CodexSessionState] = OrderedDict()
_CODEX_SESSION_CACHE_LOCK = threading.RLock()
_CODEX_SESSION_CACHE_LIVE_KEYS: set[tuple[str, str]] | None = None
_CODEX_SESSION_CACHE_BINDING_GENERATIONS: dict[tuple[str, str], int] = {}
_CODEX_SESSION_CACHE_BINDING_FINGERPRINTS: dict[tuple[str, str], tuple[str, ...]] = {}
_CODEX_SESSION_CACHE_GENERATION_COUNTER = 0
_CODEX_ISOLATED_READ_OBSERVER: Callable[[int], None] | None = None


@dataclass
class _OmpSessionState:
    """Constant-size parser coordinates retained between polls."""

    offset: int = 0
    observed_size: int = 0
    file_id: tuple[int, int] | None = None
    mtime_ns: int = 0
    ctime_ns: int = 0
    replay_offset: int = 0
    turn_open: bool = False
    project_root: Path | None = None


@dataclass
class _OmpTurnState:
    """Canonical turn data confined to one parse and one response."""

    prompt_id: str = ""
    user_text: str = ""
    stream_parts: list[str] = field(default_factory=list)
    final_text: str = ""
    tool_count: int = 0
    project_root: Path | None = None


@dataclass(frozen=True)
class _PublicOmpToolProgress:
    """Progress assembled only from constants and proven-safe relative paths."""

    action: str
    subject: str | None = None

    def render(self, step: int) -> str:
        body = f"{self.action}: {self.subject}" if self.subject else self.action
        return f"step {step} · {body}"




_OMP_SESSION_CACHE: dict[str, _OmpSessionState] = {}
_OMP_SESSION_CACHE_LOCK = threading.RLock()
_OMP_SESSION_CACHE_LIVE_KEYS: set[str] | None = None
_OMP_SESSION_CACHE_BINDING_GENERATIONS: dict[str, int] = {}
_OMP_SESSION_CACHE_BINDING_FINGERPRINTS: dict[str, tuple[str, ...]] = {}
_OMP_SESSION_CACHE_GENERATION_COUNTER = 0
_OMP_ISOLATED_READ_OBSERVER: Callable[[int], None] | None = None


def _omp_cache_get_locked(cache_key: str) -> _OmpSessionState | None:
    state = _OMP_SESSION_CACHE.pop(cache_key, None)
    if state is not None:
        _OMP_SESSION_CACHE[cache_key] = state
    return state


def _omp_cache_store_locked(cache_key: str, state: _OmpSessionState) -> None:
    _OMP_SESSION_CACHE.pop(cache_key, None)
    _OMP_SESSION_CACHE[cache_key] = state
    while _OMP_SESSION_CACHE and (
        len(_OMP_SESSION_CACHE) > _OMP_SESSION_CACHE_CAPACITY
        or _omp_cache_weight_locked() > _OMP_SESSION_CACHE_MAX_BYTES
    ):
        del _OMP_SESSION_CACHE[next(iter(_OMP_SESSION_CACHE))]


def _omp_cache_weight_locked() -> int:
    return sum(
        len(cache_key.encode("utf-8"))
        + len(
            json.dumps(
                _serialize_omp_state(state),
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        for cache_key, state in _OMP_SESSION_CACHE.items()
    )


def _omp_cache_binding_generation_locked(cache_key: str) -> int | None:
    if _OMP_SESSION_CACHE_LIVE_KEYS is None:
        return None
    return _OMP_SESSION_CACHE_BINDING_GENERATIONS.get(cache_key)


def _serialize_omp_state(state: _OmpSessionState | None) -> dict[str, Any] | None:
    if state is None:
        return None
    return {
        "offset": state.offset,
        "observed_size": state.observed_size,
        "file_id": list(state.file_id) if state.file_id is not None else None,
        "mtime_ns": state.mtime_ns,
        "ctime_ns": state.ctime_ns,
        "replay_offset": state.replay_offset,
        "turn_open": state.turn_open,
        "project_root": os.fspath(state.project_root) if state.project_root is not None else None,
    }


def _deserialize_omp_state(value: Any) -> _OmpSessionState | None:
    if value is None:
        return None
    if not isinstance(value, Mapping) or set(value) != {
        "offset",
        "observed_size",
        "file_id",
        "mtime_ns",
        "ctime_ns",
        "replay_offset",
        "turn_open",
        "project_root",
    }:
        raise ValueError("invalid OMP parser state")
    offset = value["offset"]
    observed_size = value["observed_size"]
    file_id_value = value["file_id"]
    mtime_ns = value["mtime_ns"]
    ctime_ns = value["ctime_ns"]
    replay_offset = value["replay_offset"]
    turn_open = value["turn_open"]
    project_root_value = value["project_root"]
    if (
        type(offset) is not int
        or offset < 0
        or type(observed_size) is not int
        or observed_size < offset
        or type(replay_offset) is not int
        or replay_offset < 0
        or replay_offset > offset
        or type(turn_open) is not bool
        or type(mtime_ns) is not int
        or mtime_ns < 0
        or type(ctime_ns) is not int
        or ctime_ns < 0
    ):
        raise ValueError("invalid OMP coordinates")
    if (
        file_id_value is not None
        and (
            not isinstance(file_id_value, (list, tuple))
            or len(file_id_value) != 2
            or any(type(part) is not int or part < 0 for part in file_id_value)
        )
    ):
        raise ValueError("invalid OMP file identity")
    if project_root_value is not None and type(project_root_value) is not str:
        raise ValueError("invalid OMP project root")
    return _OmpSessionState(
        offset=offset,
        observed_size=observed_size,
        file_id=tuple(file_id_value) if file_id_value is not None else None,
        mtime_ns=mtime_ns,
        ctime_ns=ctime_ns,
        replay_offset=replay_offset,
        turn_open=turn_open,
        project_root=Path(project_root_value) if project_root_value is not None else None,
    )


def _extract_turn_payload(value: Any) -> Mapping[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    result = value.get("result")
    if isinstance(result, Mapping) and isinstance(result.get("turn"), Mapping):
        return result["turn"]
    if isinstance(value.get("turn"), Mapping):
        return value["turn"]
    return value


# Every driven ordinal must stay a single keystroke (digits select/toggle
# absolutely in the pane, live-verified on Claude Code 2.1.211), so 9 is the
# hard bound; larger decisions fail closed to the read-only interaction.
PENDING_DECISION_MAX_OPTIONS = 9
_PENDING_TEXT_MAX = 2000
_SINGLE_WRITE_IN_OPTION_IDS = frozenset(
    {"custom", "other", "writein", "write_in", "write-in"}
)


def _private_pending_revision(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:24]


def _pending_observation_from_turn(turn: Mapping[str, Any]) -> PendingObservation:
    """Build one explicit private-neutral pending observation from a pane read."""
    decision = turn.get("pending_decision")
    if decision is not None:
        if not isinstance(decision, Mapping):
            return PendingObservation("read_succeeded_invalid_prompt")
        revision = _private_pending_revision(decision)
        question = redact_private_prompt_text(
            decision.get("prompt") or decision.get("question"),
            max_chars=_PENDING_TEXT_MAX,
        )
        options = decision.get("options")
        if options is None:
            options = []
        if not isinstance(options, list):
            return PendingObservation("read_succeeded_invalid_prompt")
        # A single-choice Claude prompt may have one trailing write-in row in
        # addition to the digit-addressable options. Validate the effective
        # selectable rows after the decision kind and write-in shape are known.
        if len(options) > PENDING_DECISION_MAX_OPTIONS + 1:
            return PendingObservation("read_succeeded_unsupported_decision")
        choices: list[PendingObservedChoice] = []
        for ordinal, option in enumerate(options, 1):
            if not isinstance(option, Mapping):
                return PendingObservation("read_succeeded_invalid_prompt")
            label = redact_private_prompt_text(
                option.get("label"),
                max_chars=_PENDING_TEXT_MAX,
            )
            if not label:
                return PendingObservation("read_succeeded_invalid_prompt")
            label = InteractionChoice(label=label).label
            choices.append(
                PendingObservedChoice(
                    choice_id=f"choice-{stable_fingerprint({'revision': revision, 'ordinal': ordinal, 'label': label})}",
                    label=label,
                    picker_ordinal=ordinal,
                )
            )
        if not question:
            return PendingObservation("read_succeeded_invalid_prompt")
        option_ids = {
            str(option.get("id") or "")
            for option in options
            if isinstance(option, Mapping)
        }
        raw_kind = str(
            decision.get("kind")
            or decision.get("tool_name")
            or decision.get("name")
            or ""
        ).strip().lower().replace("-", "_")
        compact_kind = raw_kind.replace("_", "")
        raw_mode = (
            str(decision.get("mode") or "")
            .strip()
            .lower()
            .replace("-", "_")
        )
        compact_mode = raw_mode.replace("_", "")
        if compact_kind not in {
            "",
            "askuserquestion",
            "single",
            "multi",
            "multiselect",
            "exitplanmode",
            "plan",
        } or compact_mode not in {
            "",
            "buttons",
            "single",
            "multi",
            "multiselect",
            "plan",
        }:
            return PendingObservation("read_succeeded_unsupported_decision")
        raw_multi_select = decision.get(
            "multi_select",
            decision.get("multiSelect", False),
        )
        if not isinstance(raw_multi_select, bool):
            return PendingObservation("read_succeeded_invalid_prompt")
        if (
            compact_mode == "plan"
            or compact_kind in {"exitplanmode", "plan"}
            or (not compact_kind and "approve" in option_ids)
        ):
            decision_kind: Literal["single", "multi", "plan"] = "plan"
        elif (
            compact_mode in {"multi", "multiselect"}
            or raw_multi_select
            or compact_kind in {"multi", "multiselect"}
        ):
            decision_kind = "multi"
        else:
            decision_kind = "single"
        if raw_multi_select is not (decision_kind == "multi"):
            return PendingObservation("read_succeeded_unsupported_decision")
        raw_question_count = decision.get("question_count")
        if raw_question_count is None:
            raw_questions = decision.get("questions")
            raw_question_count = (
                len(raw_questions) if isinstance(raw_questions, list) else 1
            )
        if (
            not isinstance(raw_question_count, int)
            or isinstance(raw_question_count, bool)
            or raw_question_count < 1
        ):
            return PendingObservation("read_succeeded_invalid_prompt")
        decision_option_labels = [choice.label for choice in choices]
        if (
            decision_kind == "single"
            and options
            and isinstance(options[len(decision_option_labels) - 1], Mapping)
            and str(
                options[len(decision_option_labels) - 1].get("id") or ""
            ).strip().lower()
            in _SINGLE_WRITE_IN_OPTION_IDS
        ):
            decision_option_labels.pop()
        decision_options = tuple(decision_option_labels)
        if not decision_options:
            return PendingObservation("read_succeeded_invalid_prompt")
        if len(decision_options) > PENDING_DECISION_MAX_OPTIONS:
            return PendingObservation("read_succeeded_unsupported_decision")
        return PendingObservation(
            "open_prompt",
            question=question,
            pending_kind="approval" if "approve" in option_ids else "question",
            choices=tuple(choices),
            revision_digest=revision,
            decision_kind=decision_kind,
            decision_options=decision_options,
            decision_multi_select=decision_kind == "multi",
            decision_question_count=raw_question_count,
        )
    interaction = turn.get("pending_interaction")
    if interaction is not None:
        if not isinstance(interaction, Mapping):
            return PendingObservation("read_succeeded_invalid_prompt")
        questions = interaction.get("questions")
        if questions is None:
            questions = []
        if not isinstance(questions, list):
            return PendingObservation("read_succeeded_invalid_prompt")
        parts: list[str] = []
        for item in questions[:4]:
            if not isinstance(item, Mapping):
                return PendingObservation("read_succeeded_invalid_prompt")
            part = redact_private_prompt_text(item.get("question"))
            if part:
                parts.append(part)
        if not parts:
            return PendingObservation("read_succeeded_invalid_prompt")
        return PendingObservation(
            "open_prompt",
            question=" / ".join(parts)[:_PENDING_TEXT_MAX],
            pending_kind="review",
            revision_digest=_private_pending_revision(interaction),
        )
    return PendingObservation("read_succeeded_no_prompt")


def _backend_pending_from_turn(turn: Mapping[str, Any]) -> dict[str, Any] | None:
    """Compatibility public projection of the explicit observation."""
    observation = _pending_observation_from_turn(turn)
    if observation.kind != "open_prompt":
        return None
    meta: dict[str, Any] = {"source": "backend"}
    if observation.decision_kind is not None:
        meta["decision"] = {
            "decision_ref": (
                "decision-"
                + stable_fingerprint(
                    {"decision_revision": observation.revision_digest}
                )
            ),
            "kind": observation.decision_kind,
            "prompt": observation.question,
            "options": [
                {"ref": str(ordinal), "label": label}
                for ordinal, label in enumerate(observation.decision_options, 1)
            ],
            "multi_select": observation.decision_multi_select,
            "question_count": observation.decision_question_count,
        }
    return {
        "question": observation.question,
        "kind": observation.pending_kind or "question",
        "choices": [
            {"choice_id": choice.choice_id, "label": choice.label}
            for choice in observation.choices
        ],
        "meta": meta,
    }


def _public_turn_pending_projection(
    observation: PendingObservation,
) -> dict[str, Any]:
    """Return the private-neutral decision fields persisted with an open turn."""
    if observation.kind != "open_prompt" or observation.decision_kind is None:
        return {}
    mode = {
        "single": "buttons",
        "multi": "multi",
        "plan": "plan",
    }[observation.decision_kind]
    return {
        "awaiting_input": True,
        "pending_decision": {
            "prompt": observation.question,
            "mode": mode,
            "options": [
                {"id": str(ordinal), "label": label}
                for ordinal, label in enumerate(observation.decision_options, 1)
            ],
            "multi_select": observation.decision_multi_select,
            "question_count": observation.decision_question_count,
        },
    }


class _TurnReadTimeout(Exception):
    """Fixed internal timeout signal; never serialized with private details."""


class _TurnReadFailed(Exception):
    """Fixed internal adapter failure signal; never serialized with raw errors."""


def _read_private_turn(
    config: Config,
    pane_id: str,
    *,
    timeout_seconds: float | None = None,
    raise_timeout: bool = False,
    cancel_event: threading.Event | None = None,
) -> Mapping[str, Any] | None:
    argv = [
        config.herdr_bin,
        "pane",
        "turn",
        pane_id,
        "--last",
        "--format",
        "json",
    ]
    timeout = config.herdr_timeout_seconds if timeout_seconds is None else timeout_seconds
    try:
        if cancel_event is None:
            completed = subprocess.run(
                argv,
                capture_output=True,
                text=True,
                check=False,
                timeout=timeout,
            )
        else:
            process = subprocess.Popen(
                argv,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            deadline = time.monotonic() + float(timeout)
            while True:
                try:
                    stdout, stderr = process.communicate(
                        timeout=max(0.001, min(0.05, deadline - time.monotonic()))
                    )
                    completed = subprocess.CompletedProcess(
                        argv,
                        process.returncode,
                        stdout,
                        stderr,
                    )
                    break
                except subprocess.TimeoutExpired:
                    if not cancel_event.is_set() and time.monotonic() < deadline:
                        continue
                    process.terminate()
                    try:
                        process.wait(0.25)
                    except subprocess.TimeoutExpired:
                        process.kill()
                    process.communicate()
                    raise _TurnReadTimeout from None
    except subprocess.TimeoutExpired:
        if raise_timeout:
            raise _TurnReadTimeout from None
        return None
    except _TurnReadTimeout:
        if raise_timeout:
            raise
        return None
    except (OSError, UnicodeDecodeError, ValueError):
        if raise_timeout:
            raise _TurnReadFailed from None
        return None
    if completed.returncode != 0:
        if raise_timeout:
            raise _TurnReadFailed
        return None
    try:
        payload = json.loads(completed.stdout)
    except (json.JSONDecodeError, TypeError, ValueError):
        if raise_timeout:
            raise _TurnReadFailed from None
        return None
    turn = _extract_turn_payload(payload)
    if not isinstance(turn, Mapping):
        if raise_timeout:
            raise _TurnReadFailed
        return None
    pending_observation = _pending_observation_from_turn(turn)
    pending_projection = _public_turn_pending_projection(pending_observation)
    if turn.get("available") is False:
        return (
            {"_backend_pending_observation": pending_observation}
            if raise_timeout
            else None
        )
    open_user_text = turn.get("open_user_text")
    open_turn_id = str(turn.get("open_turn_id") or "").strip()
    if turn.get("has_open_turn") and (open_user_text or open_turn_id):
        # An in-progress turn is reported alongside the last completed one via
        # open_* fields. Emit it as its own open turn keyed by the stable prompt
        # id so the connector streams a live "working" card that later edits
        # into the final once this same id completes.
        opened = _open_turn_content(
            open_user_text,
            turn.get("assistant_stream_text"),
            open_turn_id,
        )
        if raise_timeout:
            opened_data = dict(opened or {})
            opened_data.update(pending_projection)
            opened_data["_backend_pending_observation"] = pending_observation
            return opened_data

        return opened

    content = {key: turn.get(key) for key in _TURN_CONTENT_KEYS if key in turn}
    content.update(pending_projection)
    if raise_timeout:
        content["_backend_pending_observation"] = pending_observation
    # Prefer the stable prompt-scoped id so a turn keeps one identity from
    # open through complete; fall back to turn_id for backends without it.
    source_turn_id = str(turn.get("source_turn_id") or turn.get("turn_id") or "").strip()
    if source_turn_id:
        content["source_turn_id"] = source_turn_id[:160]
    if _is_internal_turn_content(content):
        return (
            {"_backend_pending_observation": pending_observation}
            if raise_timeout
            else None
        )
    # Never clobber stored text with an empty value: notification-triggered
    # turns legitimately carry no prompt, but the previous real prompt and
    # stream must survive the merge.
    for key in ("user_text", "assistant_final_text", "assistant_stream_text"):
        if key in content and not (content.get(key) or "").strip():
            content.pop(key)
    if not any(value not in (None, "", False) for value in content.values()):
        return None
    return content


def _pending_public_payload(
    observation: PendingObservation,
) -> dict[str, Any] | None:
    if observation.kind != "open_prompt":
        return None
    return {
        "question": observation.question,
        "kind": observation.pending_kind or "question",
        "choices": [
            {"choice_id": choice.choice_id, "label": choice.label}
            for choice in observation.choices
        ],
        "meta": {"source": "backend"},
    }


def _pop_backend_pending_observation(
    content: Mapping[str, Any] | None,
) -> tuple[dict[str, Any] | None, PendingObservation | None]:
    if content is None:
        return None, None
    data = dict(content)
    observation = data.pop("_backend_pending_observation", None)
    data.pop("_backend_pending", None)
    if not any(value not in (None, "", False) for value in data.values()):
        data = None
    return (
        data,
        observation if isinstance(observation, PendingObservation) else None,
    )


def _pop_backend_pending(content: Mapping[str, Any] | None) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Split the reserved _backend_pending key off a turn-content read. Returns (content, pending);
    content becomes None when nothing else remains."""
    if content is None:
        return None, None
    data = dict(content)
    pending = data.pop("_backend_pending", None)
    if not any(value not in (None, "", False) for value in data.values()):
        data = None
    return data, pending if isinstance(pending, dict) else None


def _open_turn_content(
    open_user_text: Any,
    stream_text: Any,
    open_turn_id: str,
) -> Mapping[str, Any] | None:
    if isinstance(open_user_text, str) and _is_internal_user_text(open_user_text):
        return None
    content: dict[str, Any] = {
        "assistant_final_text": None,
        "complete": False,
        "has_open_turn": True,
    }
    if isinstance(open_user_text, str) and open_user_text.strip():
        content["user_text"] = open_user_text
    if isinstance(stream_text, str) and stream_text.strip():
        content["assistant_stream_text"] = stream_text
    if open_turn_id:
        content["source_turn_id"] = open_turn_id[:160]
    if _is_internal_turn_content(content):
        return None
    if not (content.get("user_text") or content.get("assistant_stream_text")):
        return None
    return content


def _codex_home() -> Path:
    raw = os.environ.get("CODEX_HOME")
    if raw:
        return Path(raw).expanduser()
    return Path.home() / ".codex"


def _canonical_codex_session_id(value: Any) -> str | None:
    if type(value) is not str or len(value) != 36 or not value.isascii():
        return None
    try:
        parsed = UUID(value)
    except (ValueError, AttributeError):
        return None
    if parsed.int == 0 or str(parsed) != value:
        return None
    return value


def _codex_rollout_identity(
    relative_date: tuple[str, str, str],
    basename: str,
) -> str | None:
    if len(relative_date) != 3:
        return None
    match = _CODEX_ROLLOUT_RE.fullmatch(basename)
    if match is None:
        return None
    year, month, day, hour, minute, second, session_id = match.groups()
    if relative_date != (year, month, day):
        return None
    try:
        datetime(
            int(year),
            int(month),
            int(day),
            int(hour),
            int(minute),
            int(second),
        )
    except ValueError:
        return None
    return _canonical_codex_session_id(session_id)


class _CodexIndexLimit(Exception):
    pass


def _resolve_codex_sessions_root() -> Path:
    lexical_root = _codex_home() / "sessions"
    flags = (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = os.open(lexical_root, flags)
    except OSError as exc:
        raise OSError("Codex sessions root is unavailable") from exc
    try:
        opened = os.fstat(descriptor)
        root = lexical_root.resolve(strict=True)
        current = root.lstat()
        if (
            not stat.S_ISDIR(opened.st_mode)
            or not stat.S_ISDIR(current.st_mode)
            or (int(opened.st_dev), int(opened.st_ino))
            != (int(current.st_dev), int(current.st_ino))
        ):
            raise OSError("Codex sessions root changed during resolution")
        return root
    finally:
        os.close(descriptor)


def _codex_root_signature(root: Path) -> tuple[int, int, int, int]:
    value = root.lstat()
    if not stat.S_ISDIR(value.st_mode):
        raise OSError("Codex sessions root is not a directory")
    return (
        int(value.st_dev),
        int(value.st_ino),
        int(value.st_mtime_ns),
        int(value.st_ctime_ns),
    )






def _build_codex_index(root: Path) -> _CodexIndexGeneration:
    global _CODEX_INDEX_GENERATION_COUNTER
    entries: dict[str, tuple[str, ...]] = {}
    retained_bytes = 0
    visited = 0

    def visit() -> None:
        nonlocal visited
        visited += 1
        if visited > _CODEX_INDEX_MAX_VISITS:
            raise _CodexIndexLimit
    def scan(path: str | os.PathLike[str]) -> list[Any]:
        iterator = os.scandir(path)
        items: list[Any] = []
        try:
            for item in iterator:
                visit()
                items.append(item)
        finally:
            close = getattr(iterator, "close", None)
            if callable(close):
                close()
        items.sort(key=lambda item: item.name)
        return items


    try:
        for year_entry in scan(root):
            if (
                len(year_entry.name) != 4
                or not year_entry.name.isascii()
                or not year_entry.name.isdecimal()
                or not year_entry.is_dir(follow_symlinks=False)
            ):
                continue
            for month_entry in scan(year_entry.path):
                if (
                    len(month_entry.name) != 2
                    or not month_entry.name.isascii()
                    or not month_entry.name.isdecimal()
                    or not month_entry.is_dir(follow_symlinks=False)
                ):
                    continue
                for day_entry in scan(month_entry.path):
                    if (
                        len(day_entry.name) != 2
                        or not day_entry.name.isascii()
                        or not day_entry.name.isdecimal()
                        or not day_entry.is_dir(follow_symlinks=False)
                    ):
                        continue
                    date_parts = (
                        year_entry.name,
                        month_entry.name,
                        day_entry.name,
                    )
                    for file_entry in scan(day_entry.path):
                        if not file_entry.is_file(follow_symlinks=False):
                            continue
                        session_id = _codex_rollout_identity(
                            date_parts,
                            file_entry.name,
                        )
                        if session_id is None:
                            continue
                        relative_path = "/".join((*date_parts, file_entry.name))
                        previous = entries.get(session_id, ())
                        if relative_path in previous:
                            continue
                        retained = (*previous, relative_path)
                        added_weight = len(relative_path.encode("utf-8"))
                        if not previous:
                            added_weight += len(session_id)
                        if (
                            len(entries) + (0 if previous else 1)
                            > _CODEX_INDEX_MAX_ENTRIES
                            or retained_bytes + added_weight
                            > _CODEX_INDEX_MAX_BYTES
                        ):
                            raise _CodexIndexLimit
                        entries[session_id] = retained
                        retained_bytes += added_weight
        overflowed = False
    except _CodexIndexLimit:
        entries = {}
        retained_bytes = 0
        overflowed = True
    _CODEX_INDEX_GENERATION_COUNTER += 1
    generation = _CodexIndexGeneration(
        root=os.fspath(root),
        root_signature=_codex_root_signature(root),
        generation=_CODEX_INDEX_GENERATION_COUNTER,
        built_at=time.monotonic(),
        entries=entries,
        retained_bytes=retained_bytes,
        visited=visited,
        overflowed=overflowed,
    )
    observer = _CODEX_INDEX_BUILD_OBSERVER
    if observer is not None:
        observer(visited)
    return generation


def _codex_path_cache_weight_locked() -> int:
    return sum(
        len(root.encode("utf-8"))
        + len(session_id)
        + len((entry.relative_path or "").encode("utf-8"))
        + 96
        for (root, session_id), entry in _CODEX_PATH_CACHE.items()
    )


def _codex_path_cache_store_locked(
    key: tuple[str, str],
    resolution: _CodexPathResolution,
) -> None:
    _CODEX_PATH_CACHE.pop(key, None)
    _CODEX_PATH_CACHE[key] = resolution
    while _CODEX_PATH_CACHE and (
        len(_CODEX_PATH_CACHE) > _CODEX_PATH_CACHE_CAPACITY
        or _codex_path_cache_weight_locked() > _CODEX_PATH_CACHE_MAX_BYTES
    ):
        _CODEX_PATH_CACHE.popitem(last=False)


def _codex_resolution_from_index_locked(
    root: Path,
    session_id: str,
    index: _CodexIndexGeneration,
    previous: _CodexPathResolution | None,
) -> _CodexPathResolution:
    global _CODEX_RESOLUTION_GENERATION_COUNTER
    relative_paths = index.entries.get(session_id, ())
    status = (
        "index_limit"
        if index.overflowed
        else "missing"
        if not relative_paths
        else "ambiguous"
        if len(relative_paths) != 1
        else "found"
    )
    canonical_path: str | None = None
    relative_path: str | None = None
    file_id: tuple[int, int] | None = None
    if status == "found":
        relative_path = relative_paths[0]
        candidate = root.joinpath(*relative_path.split("/"))
        try:
            path_stat = candidate.lstat()
            resolved = candidate.resolve(strict=True)
            resolved.relative_to(root)
            if (
                not stat.S_ISREG(path_stat.st_mode)
                or resolved != candidate
                or (int(path_stat.st_dev), int(path_stat.st_ino))
                != (int(resolved.stat().st_dev), int(resolved.stat().st_ino))
            ):
                status = "unsafe_path"
            else:
                canonical_path = os.fspath(resolved)
                file_id = (int(path_stat.st_dev), int(path_stat.st_ino))
        except (OSError, ValueError):
            status = "unsafe_path"
    same_resolution = bool(
        previous is not None
        and previous.status == status
        and previous.root_file_id == index.root_signature[:2]
        and previous.canonical_path == canonical_path
        and previous.relative_path == relative_path
        and previous.file_id == file_id
    )
    if same_resolution:
        generation = previous.generation
    else:
        _CODEX_RESOLUTION_GENERATION_COUNTER += 1
        generation = _CODEX_RESOLUTION_GENERATION_COUNTER
    return _CodexPathResolution(
        status=status,
        root=os.fspath(root),
        root_file_id=(index.root_signature[0], index.root_signature[1]),
        session_id=session_id,
        canonical_path=canonical_path,
        relative_path=relative_path,
        file_id=file_id,
        generation=generation,
        expires_at=time.monotonic()
        + (
            _CODEX_POSITIVE_TTL_SECONDS
            if status == "found"
            else _CODEX_NEGATIVE_TTL_SECONDS
        ),
    )


def _resolve_codex_session(session_id: Any) -> _CodexPathResolution | None:
    canonical_id = _canonical_codex_session_id(session_id)
    if canonical_id is None:
        return None
    try:
        root = _resolve_codex_sessions_root()
        root_signature = _codex_root_signature(root)
    except OSError:
        return None
    key = (os.fspath(root), canonical_id)
    now = time.monotonic()
    global _CODEX_INDEX_GENERATION
    with _CODEX_PATH_CACHE_LOCK:
        cached = _CODEX_PATH_CACHE.get(key)
        index = _CODEX_INDEX_GENERATION
        invalidated = False
        current_root_id = root_signature[:2]
        root_changed = bool(
            (cached is not None and cached.root_file_id != current_root_id)
            or (
                index is not None
                and index.root == os.fspath(root)
                and index.root_signature[:2] != current_root_id
            )
        )
        if root_changed:
            for path_key in tuple(_CODEX_PATH_CACHE):
                if path_key[0] == os.fspath(root):
                    del _CODEX_PATH_CACHE[path_key]
            _CODEX_INDEX_GENERATION = None
            cached = None
            index = None
            invalidated = True
        if cached is not None and cached.expires_at > now:
            if cached.status != "found":
                _CODEX_PATH_CACHE.move_to_end(key)
                return cached
            try:
                current = Path(cached.canonical_path or "").lstat()
            except OSError:
                invalidated = True
            else:
                if (
                    stat.S_ISREG(current.st_mode)
                    and (int(current.st_dev), int(current.st_ino)) == cached.file_id
                ):
                    _CODEX_PATH_CACHE.move_to_end(key)
                    return cached
                invalidated = True
        rebuild = bool(
            invalidated
            or index is None
            or index.root != os.fspath(root)
            or index.root_signature != root_signature
            or now - index.built_at >= _CODEX_POSITIVE_TTL_SECONDS
        )
        if rebuild:
            index = _build_codex_index(root)
            _CODEX_INDEX_GENERATION = index
        assert index is not None
        resolution = _codex_resolution_from_index_locked(
            root,
            canonical_id,
            index,
            cached,
        )
        _codex_path_cache_store_locked(key, resolution)
        return resolution


def _find_codex_session_file(session_id: Any) -> Path | None:
    resolution = _resolve_codex_session(session_id)
    if resolution is None or resolution.status != "found":
        return None
    return Path(resolution.canonical_path or "")


def _payload_turn_id(payload: Mapping[str, Any]) -> str:
    raw = payload.get("turn_id")
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    metadata = payload.get("internal_chat_message_metadata_passthrough")
    if isinstance(metadata, Mapping):
        raw = metadata.get("turn_id")
        if isinstance(raw, str) and raw.strip():
            return raw.strip()
    return ""


def _message_text(payload: Mapping[str, Any]) -> str:
    content = payload.get("content")
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if not isinstance(item, Mapping):
                continue
            text = item.get("text")
            if isinstance(text, str) and text.strip():
                parts.append(text)
        return "\n".join(parts)
    text = payload.get("message")
    if isinstance(text, str):
        return text
    return ""


_INTERNAL_USER_TEXT_PREFIXES = (
    "<subagent_notification>",
    "<environment_context>",
    "<local-command-caveat>",
    "<local-command-stdout>",
    "<local-command-stderr>",
    "<command-name>",
    "<command-message>",
    "<system-reminder>",
    "Caveat: The messages below were generated by the user while running local commands.",
)

def _is_internal_user_text(text: str) -> bool:
    clean = text.lstrip().replace("\r\n", "\n")
    return clean.startswith(_INTERNAL_USER_TEXT_PREFIXES) or is_internal_automation_turn_payload(
        {"user_text": clean}
    )


def _is_internal_turn_content(content: Mapping[str, Any]) -> bool:
    user_text = content.get("user_text")
    if isinstance(user_text, str) and _is_internal_user_text(user_text):
        return True
    if _is_promptless_status_final(content):
        return True
    return is_internal_automation_turn_payload(content)


def _is_promptless_status_final(content: Mapping[str, Any]) -> bool:
    if str(content.get("user_text") or "").strip():
        return False
    if content.get("complete") is False or content.get("has_open_turn") is True:
        return False
    final_text = str(content.get("assistant_final_text") or "").strip()
    if not final_text or len(final_text) > 800:
        return False
    return bool(_PROMPTLESS_STATUS_FINAL_RE.search(final_text))


def _append_unique_recent(items: list[str], text: str) -> None:
    clean = text.strip()
    if not clean:
        return
    if clean in items:
        items.remove(clean)
    items.append(clean)
    if len(items) > _MAX_CODEX_STREAM_MESSAGES:
        del items[: len(items) - _MAX_CODEX_STREAM_MESSAGES]


def _open_verified_codex_file(
    resolution: _CodexPathResolution,
) -> tuple[int, os.stat_result]:
    if (
        resolution.status != "found"
        or resolution.canonical_path is None
        or resolution.file_id is None
    ):
        raise _TurnReadFailed
    root = Path(resolution.root)
    candidate = Path(resolution.canonical_path)
    try:
        before = candidate.lstat()
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(root)
    except (OSError, ValueError) as exc:
        raise _TurnReadFailed from exc
    before_id = (int(before.st_dev), int(before.st_ino))
    if (
        not stat.S_ISREG(before.st_mode)
        or resolved != candidate
        or before_id != resolution.file_id
    ):
        raise _TurnReadFailed
    if resolution.relative_path is None:
        raise _TurnReadFailed
    relative_parts = tuple(resolution.relative_path.split("/"))
    if len(relative_parts) != _CODEX_INDEX_MAX_DEPTH:
        raise _TurnReadFailed
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    close_on_exec = getattr(os, "O_CLOEXEC", 0)
    directory_flags = (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | close_on_exec
        | nofollow
    )
    file_flags = os.O_RDONLY | close_on_exec | nofollow
    directory_fd: int | None = None
    try:
        directory_fd = os.open(root, directory_flags)
        root_stat = os.fstat(directory_fd)
        if (int(root_stat.st_dev), int(root_stat.st_ino)) != resolution.root_file_id:
            raise _TurnReadFailed
        for component in relative_parts[:-1]:
            next_fd = os.open(
                component,
                directory_flags,
                dir_fd=directory_fd,
            )
            os.close(directory_fd)
            directory_fd = next_fd
        descriptor = os.open(
            relative_parts[-1],
            file_flags,
            dir_fd=directory_fd,
        )
    except OSError as exc:
        raise _TurnReadFailed from exc
    finally:
        if directory_fd is not None:
            os.close(directory_fd)
    try:
        opened = os.fstat(descriptor)
        after = candidate.stat()
        opened_id = (int(opened.st_dev), int(opened.st_ino))
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened_id != before_id
            or opened_id != (int(after.st_dev), int(after.st_ino))
        ):
            raise _TurnReadFailed
        return descriptor, opened
    except BaseException:
        os.close(descriptor)
        raise


def _serialize_codex_state(state_value: _CodexSessionState | None) -> dict[str, Any] | None:
    if state_value is None:
        return None
    value = {
        "resolver_generation": state_value.resolver_generation,
        "root": state_value.root,
        "root_file_id": list(state_value.root_file_id) if state_value.root_file_id is not None else None,
        "session_id": state_value.session_id,
        "canonical_path": state_value.canonical_path,
        "file_id": list(state_value.file_id) if state_value.file_id is not None else None,
        "observed_size": state_value.observed_size,
        "mtime_ns": state_value.mtime_ns,
        "ctime_ns": state_value.ctime_ns,
        "committed_offset": state_value.committed_offset,
        "partial_record_b64": base64.b64encode(state_value.partial_record).decode("ascii"),
        "active_turn_id": state_value.active_turn_id,
        "last_content_turn_id": state_value.last_content_turn_id,
        "turn_open": state_value.turn_open,
        "final_seen": state_value.final_seen,
        "complete": state_value.complete,
        "internal_turn": state_value.internal_turn,
        "stream_spans": [
            [span.start, span.end]
            for span in state_value.stream_spans
        ],
    }
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    if len(encoded) > _CODEX_STATE_IPC_MAX_BYTES:
        raise ValueError("Codex parser state exceeds IPC limit")
    return value


def _deserialize_codex_state(value: Any) -> _CodexSessionState | None:
    if value is None:
        return None
    expected = {
        "resolver_generation",
        "root",
        "root_file_id",
        "session_id",
        "canonical_path",
        "file_id",
        "observed_size",
        "mtime_ns",
        "ctime_ns",
        "committed_offset",
        "partial_record_b64",
        "active_turn_id",
        "last_content_turn_id",
        "turn_open",
        "final_seen",
        "complete",
        "internal_turn",
        "stream_spans",
    }
    if not isinstance(value, Mapping) or set(value) != expected:
        raise ValueError("invalid Codex parser state")
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    if len(encoded) > _CODEX_STATE_IPC_MAX_BYTES:
        raise ValueError("oversized Codex parser state")
    resolver_generation = value["resolver_generation"]
    root = value["root"]
    root_file_id_value = value["root_file_id"]
    session_id = value["session_id"]
    canonical_path = value["canonical_path"]
    file_id_value = value["file_id"]
    observed_size = value["observed_size"]
    mtime_ns = value["mtime_ns"]
    ctime_ns = value["ctime_ns"]
    committed_offset = value["committed_offset"]
    partial_value = value["partial_record_b64"]
    active_turn_id = value["active_turn_id"]
    last_content_turn_id = value["last_content_turn_id"]
    stream_value = value["stream_spans"]
    if (
        type(resolver_generation) is not int
        or resolver_generation <= 0
        or type(root) is not str
        or not root
        or type(session_id) is not str
        or _canonical_codex_session_id(session_id) != session_id
        or type(canonical_path) is not str
        or not canonical_path
        or type(observed_size) is not int
        or observed_size < 0
        or type(mtime_ns) is not int
        or mtime_ns < 0
        or type(ctime_ns) is not int
        or ctime_ns < 0
        or type(committed_offset) is not int
        or committed_offset < 0
        or type(partial_value) is not str
        or type(active_turn_id) is not str
        or type(last_content_turn_id) is not str
        or type(value["turn_open"]) is not bool
        or type(value["final_seen"]) is not bool
        or type(value["complete"]) is not bool
        or type(value["internal_turn"]) is not bool
        or not isinstance(stream_value, list)
        or len(stream_value) > _MAX_CODEX_STREAM_MESSAGES
    ):
        raise ValueError("invalid Codex parser state values")
    for turn_id in (active_turn_id, last_content_turn_id):
        if len(turn_id.encode("utf-8")) > _CODEX_TURN_ID_MAX_BYTES:
            raise ValueError("oversized Codex turn identity")
    try:
        partial_record = base64.b64decode(partial_value, validate=True)
    except (ValueError, base64.binascii.Error) as exc:
        raise ValueError("invalid Codex partial record") from exc
    if (
        base64.b64encode(partial_record).decode("ascii") != partial_value
        or len(partial_record) > _CODEX_RECORD_MAX_BYTES
        or committed_offset + len(partial_record) > observed_size
    ):
        raise ValueError("invalid Codex partial coordinates")
    if (
        not isinstance(root_file_id_value, (list, tuple))
        or len(root_file_id_value) != 2
        or any(type(part) is not int or part < 0 for part in root_file_id_value)
    ):
        raise ValueError("invalid Codex root identity")
    if (
        file_id_value is not None
        and (
            not isinstance(file_id_value, (list, tuple))
            or len(file_id_value) != 2
            or any(type(part) is not int or part < 0 for part in file_id_value)
        )
    ):
        raise ValueError("invalid Codex file identity")
    if file_id_value is None and (
        observed_size != 0
        or committed_offset != 0
        or partial_record
        or mtime_ns != 0
        or ctime_ns != 0
        or active_turn_id
        or last_content_turn_id
        or value["internal_turn"]
        or stream_value
    ):
        raise ValueError("invalid Codex bootstrap state")
    spans: list[_CodexRecordSpan] = []
    prior_end = -1
    for raw_span in stream_value:
        if (
            not isinstance(raw_span, (list, tuple))
            or len(raw_span) != 2
            or any(type(part) is not int or part < 0 for part in raw_span)
        ):
            raise ValueError("invalid Codex record span")
        start, end = raw_span
        if start >= end or end > committed_offset or end - start > _CODEX_RECORD_MAX_BYTES:
            raise ValueError("invalid Codex record coordinates")
        if start <= prior_end:
            raise ValueError("overlapping Codex record spans")
        spans.append(_CodexRecordSpan(start, end))
        prior_end = end
    try:
        root_path = Path(root)
        path = Path(canonical_path)
        relative = path.relative_to(root_path)
    except ValueError as exc:
        raise ValueError("Codex path outside root") from exc
    if (
        len(relative.parts) != _CODEX_INDEX_MAX_DEPTH
        or _codex_rollout_identity(
            (relative.parts[0], relative.parts[1], relative.parts[2]),
            relative.parts[3],
        )
        != session_id
    ):
        raise ValueError("invalid Codex rollout path")
    return _CodexSessionState(
        resolver_generation=resolver_generation,
        root=root,
        root_file_id=tuple(root_file_id_value),
        session_id=session_id,
        canonical_path=canonical_path,
        file_id=tuple(file_id_value) if file_id_value is not None else None,
        observed_size=observed_size,
        mtime_ns=mtime_ns,
        ctime_ns=ctime_ns,
        committed_offset=committed_offset,
        partial_record=partial_record,
        active_turn_id=active_turn_id,
        last_content_turn_id=last_content_turn_id,
        turn_open=value["turn_open"],
        final_seen=value["final_seen"],
        complete=value["complete"],
        internal_turn=value["internal_turn"],
        stream_spans=tuple(spans),
    )


def _codex_cache_weight_locked() -> int:
    total = 0
    for (root, session_id), state_value in _CODEX_SESSION_CACHE.items():
        serialized = _serialize_codex_state(state_value)
        total += (
            len(root.encode("utf-8"))
            + len(session_id)
            + len(
                json.dumps(
                    serialized,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ).encode("utf-8")
            )
        )
    return total


def _codex_cache_get_locked(
    cache_key: tuple[str, str],
) -> _CodexSessionState | None:
    state_value = _CODEX_SESSION_CACHE.pop(cache_key, None)
    if state_value is not None:
        _CODEX_SESSION_CACHE[cache_key] = state_value
    return state_value


def _codex_cache_store_locked(
    cache_key: tuple[str, str],
    state_value: _CodexSessionState,
) -> bool:
    encoded = _serialize_codex_state(state_value)
    entry_weight = (
        len(cache_key[0].encode("utf-8"))
        + len(cache_key[1])
        + len(
            json.dumps(
                encoded,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
        )
    )
    if entry_weight > _CODEX_SESSION_CACHE_MAX_BYTES:
        return False
    _CODEX_SESSION_CACHE.pop(cache_key, None)
    _CODEX_SESSION_CACHE[cache_key] = state_value
    while _CODEX_SESSION_CACHE and (
        len(_CODEX_SESSION_CACHE) > _CODEX_SESSION_CACHE_CAPACITY
        or _codex_cache_weight_locked() > _CODEX_SESSION_CACHE_MAX_BYTES
    ):
        _CODEX_SESSION_CACHE.popitem(last=False)
    return cache_key in _CODEX_SESSION_CACHE


def _codex_binding_cache_key(value: Any) -> tuple[str, str] | None:
    session_id = _canonical_codex_session_id(value)
    if session_id is None:
        return None
    try:
        root = _resolve_codex_sessions_root()
        _codex_root_signature(root)
    except OSError:
        return None
    return (os.fspath(root), session_id)


def _codex_cache_binding_generation_locked(
    cache_key: tuple[str, str],
) -> int | None:
    if _CODEX_SESSION_CACHE_LIVE_KEYS is None:
        return None
    return _CODEX_SESSION_CACHE_BINDING_GENERATIONS.get(cache_key)


def _prune_codex_cache_for_bindings(bindings: list[WorkerBinding]) -> None:
    live_fingerprint_sets: dict[tuple[str, str], set[str]] = {}
    for binding in bindings:
        if (
            binding.turn_target_kind != _CODEX_SESSION_TURN_KIND
            or not _eligible_turn_binding(binding)
        ):
            continue
        cache_key = _codex_binding_cache_key(binding.turn_target_value)
        if cache_key is not None:
            live_fingerprint_sets.setdefault(cache_key, set()).add(
                binding.private_fingerprint
            )
    live_keys = set(live_fingerprint_sets)
    live_fingerprints = {
        key: tuple(sorted(values))
        for key, values in live_fingerprint_sets.items()
    }
    global _CODEX_SESSION_CACHE_GENERATION_COUNTER
    global _CODEX_SESSION_CACHE_BINDING_FINGERPRINTS
    global _CODEX_SESSION_CACHE_BINDING_GENERATIONS
    global _CODEX_SESSION_CACHE_LIVE_KEYS
    with _CODEX_SESSION_CACHE_LOCK:
        changed = {
            key
            for key in live_keys
            if (
                _CODEX_SESSION_CACHE_LIVE_KEYS is not None
                and key in _CODEX_SESSION_CACHE_LIVE_KEYS
                and _CODEX_SESSION_CACHE_BINDING_FINGERPRINTS.get(key)
                != live_fingerprints[key]
            )
        }
        generations: dict[tuple[str, str], int] = {}
        for key in live_keys:
            if (
                _CODEX_SESSION_CACHE_LIVE_KEYS is not None
                and key in _CODEX_SESSION_CACHE_LIVE_KEYS
                and _CODEX_SESSION_CACHE_BINDING_FINGERPRINTS.get(key)
                == live_fingerprints[key]
            ):
                generations[key] = _CODEX_SESSION_CACHE_BINDING_GENERATIONS[key]
            else:
                _CODEX_SESSION_CACHE_GENERATION_COUNTER += 1
                generations[key] = _CODEX_SESSION_CACHE_GENERATION_COUNTER
        _CODEX_SESSION_CACHE_LIVE_KEYS = live_keys
        _CODEX_SESSION_CACHE_BINDING_FINGERPRINTS = live_fingerprints
        _CODEX_SESSION_CACHE_BINDING_GENERATIONS = generations
        for key in tuple(_CODEX_SESSION_CACHE):
            if key not in live_keys or key in changed:
                del _CODEX_SESSION_CACHE[key]


def _codex_record_event(record: Mapping[str, Any]) -> _CodexSemanticEvent | None:
    payload = record.get("payload")
    if not isinstance(payload, Mapping):
        return None
    payload_type = str(payload.get("type") or "")
    turn_id = _payload_turn_id(payload)
    if turn_id and len(turn_id.encode("utf-8")) > _CODEX_TURN_ID_MAX_BYTES:
        raise ValueError("oversized Codex turn identity")
    if record.get("type") == "event_msg" and payload_type == "task_started":
        return _CodexSemanticEvent("start", turn_id)
    if record.get("type") == "event_msg" and payload_type == "task_complete":
        text = str(payload.get("last_agent_message") or "")
        if not text:
            return _CodexSemanticEvent("complete_empty", turn_id)
        return _CodexSemanticEvent("final", turn_id, text)
    if record.get("type") != "response_item" or payload_type != "message":
        return None
    text = _message_text(payload)
    if not text:
        return None
    role = str(payload.get("role") or "")
    if role == "user":
        return _CodexSemanticEvent("user", turn_id, text)
    if role != "assistant":
        return None
    if str(payload.get("phase") or "") == "commentary":
        return _CodexSemanticEvent("commentary", turn_id, text)
    return _CodexSemanticEvent("final", turn_id, text)


def _apply_codex_event(
    state_value: _CodexWorkState,
    event: _CodexSemanticEvent | None,
    span: _CodexRecordSpan,
) -> None:
    if event is None:
        return
    if event.kind == "start":
        if not event.turn_id:
            return
        state_value.active_turn_id = event.turn_id
        state_value.last_content_turn_id = ""
        state_value.turn_open = True
        state_value.final_seen = False
        state_value.complete = False
        state_value.internal_turn = False
        state_value.stream_items.clear()
        state_value.user_text = None
        state_value.final_text = None
        state_value.public_changed = True
        return
    turn_id = event.turn_id or state_value.active_turn_id
    if not turn_id or event.kind == "complete_empty":
        return
    selected = state_value.active_turn_id or state_value.last_content_turn_id
    if state_value.active_turn_id and turn_id != state_value.active_turn_id:
        return
    if not state_value.active_turn_id and selected and turn_id != selected:
        state_value.stream_items.clear()
        state_value.final_seen = False
        state_value.complete = False
        state_value.internal_turn = False
        state_value.user_text = None
        state_value.final_text = None
    if event.kind == "user":
        if _is_internal_user_text(event.text):
            # Codex may emit environment or command context before the real
            # user message under the same turn ID. Suppress that context
            # without allowing later internal metadata to erase a user turn
            # that has already become public.
            if state_value.user_text is None:
                state_value.internal_turn = True
                state_value.last_content_turn_id = turn_id
                state_value.stream_items.clear()
                state_value.final_text = None
            return
        state_value.internal_turn = False
        state_value.user_text = event.text
        state_value.last_content_turn_id = turn_id
        state_value.turn_open = not state_value.final_seen
        state_value.public_changed = True
        return
    if state_value.internal_turn:
        return
    if event.kind == "commentary":
        clean = event.text.strip()
        if not clean:
            return
        state_value.stream_items = [
            item for item in state_value.stream_items if item[1] != clean
        ]
        state_value.stream_items.append((span, clean))
        if len(state_value.stream_items) > _MAX_CODEX_STREAM_MESSAGES:
            del state_value.stream_items[
                : len(state_value.stream_items) - _MAX_CODEX_STREAM_MESSAGES
            ]
        state_value.last_content_turn_id = turn_id
        state_value.turn_open = not state_value.final_seen
        state_value.public_changed = True
        return
    if event.kind == "final":
        state_value.final_text = event.text
        state_value.last_content_turn_id = turn_id
        state_value.final_seen = True
        state_value.complete = True
        state_value.turn_open = False
        state_value.stream_items.clear()
        state_value.public_changed = True


def _codex_decode_record(raw: bytes) -> Mapping[str, Any]:
    if len(raw) > _CODEX_RECORD_MAX_BYTES:
        raise ValueError("oversized Codex record")
    try:
        decoded = raw.decode("utf-8")
        value = json.loads(decoded)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("invalid Codex record") from exc
    if not isinstance(value, Mapping):
        raise ValueError("invalid Codex record shape")
    return value


def _codex_materialize_stream_items(
    descriptor: int,
    spans: tuple[_CodexRecordSpan, ...],
) -> tuple[list[tuple[_CodexRecordSpan, str]], int]:
    items: list[tuple[_CodexRecordSpan, str]] = []
    bytes_read = 0
    for span in spans:
        length = span.end - span.start
        raw = os.pread(descriptor, length, span.start)
        bytes_read += len(raw)
        if len(raw) != length:
            raise _TurnReadFailed
        event = _codex_record_event(_codex_decode_record(raw))
        if event is None or event.kind != "commentary":
            raise _TurnReadFailed
        clean = event.text.strip()
        items = [item for item in items if item[1] != clean]
        items.append((span, clean))
    return items, bytes_read


def _codex_content_from_work(
    state_value: _CodexWorkState,
) -> Mapping[str, Any] | None:
    if state_value.internal_turn:
        return None
    if not state_value.public_changed:
        return None
    turn_id = state_value.active_turn_id or state_value.last_content_turn_id
    if not turn_id:
        return None
    stream_text = "\n\n".join(text for _span, text in state_value.stream_items) or None
    content = {
        "user_text": state_value.user_text,
        "assistant_stream_text": None if state_value.final_seen else stream_text,
        "assistant_final_text": state_value.final_text,
        "complete": state_value.complete if state_value.final_seen else False,
        "has_open_turn": not state_value.final_seen,
        "source_turn_id": turn_id[:160],
    }
    if _is_internal_turn_content(content):
        return None
    if not any(item not in (None, "", False) for item in content.values()):
        return None
    return content


def _codex_freeze_work(state_value: _CodexWorkState) -> _CodexSessionState:
    return _CodexSessionState(
        resolver_generation=state_value.resolver_generation,
        root=state_value.root,
        root_file_id=state_value.root_file_id,
        session_id=state_value.session_id,
        canonical_path=state_value.canonical_path,
        file_id=state_value.file_id,
        observed_size=state_value.observed_size,
        mtime_ns=state_value.mtime_ns,
        ctime_ns=state_value.ctime_ns,
        committed_offset=state_value.committed_offset,
        partial_record=state_value.partial_record,
        active_turn_id=state_value.active_turn_id,
        last_content_turn_id=state_value.last_content_turn_id,
        turn_open=state_value.turn_open,
        final_seen=state_value.final_seen,
        complete=state_value.complete,
        internal_turn=state_value.internal_turn,
        stream_spans=tuple(span for span, _text in state_value.stream_items),
    )


def _codex_work_from_prior(
    descriptor: int,
    prior: _CodexSessionState,
    opened: os.stat_result,
) -> tuple[_CodexWorkState, int]:
    stream_items, bytes_read = _codex_materialize_stream_items(
        descriptor,
        prior.stream_spans,
    )
    return (
        _CodexWorkState(
            resolver_generation=prior.resolver_generation,
            root=prior.root,
            root_file_id=prior.root_file_id,
            session_id=prior.session_id,
            canonical_path=prior.canonical_path,
            file_id=(int(opened.st_dev), int(opened.st_ino)),
            observed_size=int(opened.st_size),
            mtime_ns=int(opened.st_mtime_ns),
            ctime_ns=int(opened.st_ctime_ns),
            committed_offset=prior.committed_offset,
            partial_record=prior.partial_record,
            active_turn_id=prior.active_turn_id,
            last_content_turn_id=prior.last_content_turn_id,
            turn_open=prior.turn_open,
            final_seen=prior.final_seen,
            complete=prior.complete,
            internal_turn=prior.internal_turn,
            stream_items=stream_items,
        ),
        bytes_read,
    )


def _codex_apply_complete_line(
    state_value: _CodexWorkState,
    raw: bytes,
    start: int,
    end: int,
) -> _CodexSemanticEvent | None:
    event = None
    if raw.strip():
        record = _codex_decode_record(raw)
        event = _codex_record_event(record)
        _apply_codex_event(
            state_value,
            event,
            _CodexRecordSpan(start, end),
        )
    state_value.committed_offset = end + 1
    return event


def _read_codex_incremental(
    descriptor: int,
    prior: _CodexSessionState,
    opened: os.stat_result,
) -> tuple[Mapping[str, Any] | None, _CodexSessionState, int]:
    state_value, bytes_read = _codex_work_from_prior(descriptor, prior, opened)
    buffer = bytearray(prior.partial_record)
    line_start = prior.committed_offset
    physical_offset = prior.committed_offset + len(prior.partial_record)
    target_size = int(opened.st_size)
    if bytes_read + (target_size - physical_offset) > _CODEX_POLL_MAX_BYTES:
        raise ValueError("Codex poll byte limit exceeded")
    while physical_offset < target_size:
        amount = min(_CODEX_READ_CHUNK_BYTES, target_size - physical_offset)
        chunk = os.pread(descriptor, amount, physical_offset)
        if not chunk:
            raise _TurnReadFailed
        physical_offset += len(chunk)
        bytes_read += len(chunk)
        buffer.extend(chunk)
        while True:
            newline = buffer.find(b"\n")
            if newline < 0:
                if len(buffer) > _CODEX_RECORD_MAX_BYTES:
                    raise ValueError("oversized Codex record")
                break
            if newline > _CODEX_RECORD_MAX_BYTES:
                raise ValueError("oversized Codex record")
            raw = bytes(buffer[:newline])
            was_final = state_value.final_seen
            event = _codex_apply_complete_line(
                state_value,
                raw,
                line_start,
                line_start + newline,
            )
            del buffer[: newline + 1]
            line_start = state_value.committed_offset
            if (
                event is not None
                and event.kind == "final"
                and not was_final
                and state_value.final_seen
            ):
                # Publish each newly completed turn before consuming a later
                # turn already present in the same append batch. Bytes after
                # this record may already have been read into ``buffer``; do
                # not checkpoint them. The next refresh rereads from the exact
                # committed newline and advances to the following turn.
                state_value.partial_record = b""
                state_value.observed_size = state_value.committed_offset
                state_value.mtime_ns = int(opened.st_mtime_ns)
                state_value.ctime_ns = int(opened.st_ctime_ns)
                return (
                    _codex_content_from_work(state_value),
                    _codex_freeze_work(state_value),
                    bytes_read,
                )
    state_value.partial_record = bytes(buffer)
    state_value.observed_size = target_size
    state_value.mtime_ns = int(opened.st_mtime_ns)
    state_value.ctime_ns = int(opened.st_ctime_ns)
    return (
        _codex_content_from_work(state_value),
        _codex_freeze_work(state_value),
        bytes_read,
    )


def _codex_tail_candidate(
    data: bytes,
    absolute_start: int,
    file_size: int,
) -> tuple[int, list[tuple[int, int, Mapping[str, Any], _CodexSemanticEvent | None]], bytes] | None:
    aligned_start = absolute_start
    if absolute_start > 0:
        first_lf = data.find(b"\n")
        if first_lf < 0:
            return None
        aligned_start += first_lf + 1
        data = data[first_lf + 1 :]
    last_lf = data.rfind(b"\n")
    if last_lf < 0:
        return None
    complete = data[: last_lf + 1]
    partial = data[last_lf + 1 :]
    if len(partial) > _CODEX_RECORD_MAX_BYTES:
        raise ValueError("oversized Codex record")
    records: list[
        tuple[int, int, Mapping[str, Any], _CodexSemanticEvent | None]
    ] = []
    cursor = 0
    while cursor < len(complete):
        newline = complete.find(b"\n", cursor)
        if newline < 0:
            break
        raw = complete[cursor:newline]
        start = aligned_start + cursor
        end = aligned_start + newline
        cursor = newline + 1
        if not raw.strip():
            continue
        if len(records) >= _CODEX_RESYNC_MAX_RECORDS:
            raise ValueError("too many Codex resync records")
        try:
            record = _codex_decode_record(raw)
            event = _codex_record_event(record)
        except ValueError:
            records.append((start, end, {}, _CodexSemanticEvent("invalid", "")))
            continue
        records.append((start, end, record, event))
    latest_start: int | None = None
    for index, (_start, _end, _record, event) in enumerate(records):
        if event is not None and event.kind == "start" and event.turn_id:
            latest_start = index
    boundary = latest_start
    if boundary is None:
        last_identity = ""
        for _start, _end, _record, event in records:
            if (
                event is not None
                and event.kind in {"user", "commentary", "final"}
                and event.turn_id
            ):
                last_identity = event.turn_id
        if last_identity:
            boundary = next(
                index
                for index, (_start, _end, _record, event) in enumerate(records)
                if (
                    event is not None
                    and event.turn_id == last_identity
                    and event.kind in {"user", "commentary", "final"}
                )
            )
    if boundary is None:
        return None
    if any(
        event is not None and event.kind == "invalid"
        for _start, _end, _record, event in records[boundary:]
    ):
        raise ValueError("invalid Codex record")
    return boundary, records, partial


def _resync_codex(
    descriptor: int,
    bootstrap: _CodexSessionState,
    opened: os.stat_result,
) -> tuple[Mapping[str, Any] | None, _CodexSessionState, int] | None:
    file_size = int(opened.st_size)
    if file_size == 0:
        return None
    chunks: deque[bytes] = deque()
    total = 0
    next_amount = _CODEX_RESYNC_INITIAL_BYTES
    candidate = None
    absolute_start = file_size
    while total < min(file_size, _CODEX_RESYNC_MAX_BYTES):
        amount = min(
            next_amount,
            file_size - total,
            _CODEX_RESYNC_MAX_BYTES - total,
        )
        absolute_start = file_size - total - amount
        chunk = os.pread(descriptor, amount, absolute_start)
        if len(chunk) != amount:
            raise _TurnReadFailed
        chunks.appendleft(chunk)
        total += amount
        data = b"".join(chunks)
        candidate = _codex_tail_candidate(data, absolute_start, file_size)
        if candidate is not None:
            boundary, records, _partial = candidate
            boundary_event = records[boundary][3]
            if (
                boundary_event is not None
                and boundary_event.kind == "start"
            ) or absolute_start == 0 or total >= min(file_size, _CODEX_RESYNC_MAX_BYTES):
                break
            candidate = None
        next_amount = min(next_amount * 2, _CODEX_RESYNC_MAX_BYTES - total)
        if next_amount <= 0:
            break
    if candidate is None:
        return None
    boundary, records, partial = candidate
    first_start = records[boundary][0]
    state_value = _CodexWorkState(
        resolver_generation=bootstrap.resolver_generation,
        root=bootstrap.root,
        root_file_id=bootstrap.root_file_id,
        session_id=bootstrap.session_id,
        canonical_path=bootstrap.canonical_path,
        file_id=(int(opened.st_dev), int(opened.st_ino)),
        observed_size=file_size,
        mtime_ns=int(opened.st_mtime_ns),
        ctime_ns=int(opened.st_ctime_ns),
        committed_offset=first_start,
    )
    for start, end, _record, event in records[boundary:]:
        if event is not None and event.kind == "invalid":
            raise ValueError("invalid Codex record")
        _apply_codex_event(
            state_value,
            event,
            _CodexRecordSpan(start, end),
        )
        state_value.committed_offset = end + 1
    state_value.partial_record = partial
    if state_value.committed_offset + len(partial) != file_size:
        raise _TurnReadFailed
    return (
        _codex_content_from_work(state_value),
        _codex_freeze_work(state_value),
        total,
    )


def _codex_resolution_for_state(
    state_value: _CodexSessionState,
) -> _CodexPathResolution:
    try:
        configured_root = _resolve_codex_sessions_root()
        configured_signature = _codex_root_signature(configured_root)
        state_root = Path(state_value.root)
        path = Path(state_value.canonical_path)
        relative = path.relative_to(state_root)
    except (OSError, ValueError) as exc:
        raise _TurnReadFailed from exc
    if (
        configured_root != state_root
        or state_value.root_file_id != configured_signature[:2]
        or len(relative.parts) != _CODEX_INDEX_MAX_DEPTH
    ):
        raise _TurnReadFailed
    parsed_id = _codex_rollout_identity(
        (relative.parts[0], relative.parts[1], relative.parts[2]),
        relative.parts[3],
    )
    if parsed_id != state_value.session_id:
        raise _TurnReadFailed
    try:
        current = path.lstat()
    except OSError as exc:
        raise _TurnReadFailed from exc
    return _CodexPathResolution(
        status="found",
        root=state_value.root,
        root_file_id=(configured_signature[0], configured_signature[1]),
        session_id=state_value.session_id,
        canonical_path=state_value.canonical_path,
        relative_path="/".join(relative.parts),
        file_id=(int(current.st_dev), int(current.st_ino)),
        generation=state_value.resolver_generation,
        expires_at=0.0,
    )


def _read_codex_session_turn_with_state(
    session_id: str,
    supplied_state: _CodexSessionState,
) -> tuple[Mapping[str, Any] | None, _CodexSessionState, int] | None:
    if _canonical_codex_session_id(session_id) != supplied_state.session_id:
        raise _TurnReadFailed
    prior: _CodexSessionState | None = supplied_state
    for _attempt in range(2):
        resolution = _codex_resolution_for_state(supplied_state)
        descriptor, opened = _open_verified_codex_file(resolution)
        try:
            file_id = (int(opened.st_dev), int(opened.st_ino))
            reusable = bool(
                prior is not None
                and prior.file_id == file_id
                and prior.resolver_generation == supplied_state.resolver_generation
                and prior.canonical_path == supplied_state.canonical_path
                and int(opened.st_size)
                >= prior.committed_offset + len(prior.partial_record)
                and (
                    int(opened.st_size) > prior.observed_size
                    or (
                        int(opened.st_size) == prior.observed_size
                        and int(opened.st_mtime_ns) == prior.mtime_ns
                        and int(opened.st_ctime_ns) == prior.ctime_ns
                    )
                )
            )
            if reusable:
                parsed = _read_codex_incremental(descriptor, prior, opened)
            else:
                parsed = _resync_codex(descriptor, supplied_state, opened)
            after_fd = os.fstat(descriptor)
            after_path = Path(supplied_state.canonical_path).stat()
            if (
                (int(after_fd.st_dev), int(after_fd.st_ino)) != file_id
                or (int(after_path.st_dev), int(after_path.st_ino)) != file_id
                or int(after_fd.st_size) != int(opened.st_size)
                or int(after_fd.st_mtime_ns) != int(opened.st_mtime_ns)
                or int(after_fd.st_ctime_ns) != int(opened.st_ctime_ns)
            ):
                raise _TurnReadFailed
            return parsed
        except (_TurnReadFailed, OSError):
            prior = None
            if _attempt:
                raise _TurnReadFailed
        finally:
            os.close(descriptor)
    raise _TurnReadFailed


def _codex_checkpoint_matches_stat(
    state_value: _CodexSessionState,
    resolution: _CodexPathResolution,
) -> bool:
    if (
        state_value.file_id is None
        or resolution.status != "found"
        or resolution.canonical_path != state_value.canonical_path
        or resolution.root_file_id != state_value.root_file_id
        or resolution.generation != state_value.resolver_generation
    ):
        return False
    try:
        current = Path(state_value.canonical_path).stat()
    except OSError:
        return False
    return bool(
        stat.S_ISREG(current.st_mode)
        and (int(current.st_dev), int(current.st_ino)) == state_value.file_id
        and int(current.st_size) == state_value.observed_size
        and int(current.st_mtime_ns) == state_value.mtime_ns
        and int(current.st_ctime_ns) == state_value.ctime_ns
    )


def _publish_codex_cache_state(
    cache_key: tuple[str, str],
    prior_state_value: Mapping[str, Any] | None,
    updated_state: _CodexSessionState,
    content: Mapping[str, Any] | None,
    binding_generation: int | None,
) -> Mapping[str, Any] | None:
    with _CODEX_SESSION_CACHE_LOCK:
        current_generation = _codex_cache_binding_generation_locked(cache_key)
        if (
            binding_generation != current_generation
            or (
                _CODEX_SESSION_CACHE_LIVE_KEYS is not None
                and cache_key not in _CODEX_SESSION_CACHE_LIVE_KEYS
            )
        ):
            return None
        current = _CODEX_SESSION_CACHE.get(cache_key)
        current_value = _serialize_codex_state(current)
        exact_prior = current_value == prior_state_value
        monotone = bool(
            current is not None
            and current.resolver_generation == updated_state.resolver_generation
            and current.root_file_id == updated_state.root_file_id
            and current.canonical_path == updated_state.canonical_path
            and current.file_id == updated_state.file_id
            and updated_state.committed_offset > current.committed_offset
        )
        if not exact_prior and not monotone:
            return None
        if prior_state_value is not None and current is None:
            return None
        if not _codex_cache_store_locked(cache_key, updated_state):
            return None
        return content


def _read_codex_session_turn(session_id: str) -> Mapping[str, Any] | None:
    resolution = _resolve_codex_session(session_id)
    if resolution is None or resolution.status != "found":
        return None
    cache_key = (resolution.root, resolution.session_id)
    with _CODEX_SESSION_CACHE_LOCK:
        prior = _codex_cache_get_locked(cache_key)
        if (
            prior is not None
            and (
                prior.resolver_generation != resolution.generation
                or prior.root_file_id != resolution.root_file_id
                or prior.canonical_path != resolution.canonical_path
                or prior.file_id != resolution.file_id
            )
        ):
            _CODEX_SESSION_CACHE.pop(cache_key, None)
            prior = None
        prior_value = _serialize_codex_state(prior)
        binding_generation = _codex_cache_binding_generation_locked(cache_key)
    if prior is not None and _codex_checkpoint_matches_stat(prior, resolution):
        observer = _CODEX_ISOLATED_READ_OBSERVER
        if observer is not None:
            observer(0)
        return None
    supplied = prior
    if (
        supplied is None
        or supplied.resolver_generation != resolution.generation
        or supplied.root_file_id != resolution.root_file_id
        or supplied.canonical_path != resolution.canonical_path
    ):
        supplied = _CodexSessionState(
            resolver_generation=resolution.generation,
            root=resolution.root,
            root_file_id=resolution.root_file_id,
            session_id=resolution.session_id,
            canonical_path=resolution.canonical_path or "",
            file_id=None,
            observed_size=0,
            mtime_ns=0,
            ctime_ns=0,
            committed_offset=0,
            partial_record=b"",
            active_turn_id="",
            last_content_turn_id="",
            turn_open=False,
            final_seen=False,
            complete=False,
            stream_spans=(),
        )
        prior_value = None
    parsed = _read_codex_session_turn_with_state(session_id, supplied)
    if parsed is None:
        return None
    content, updated_state, bytes_read = parsed
    observer = _CODEX_ISOLATED_READ_OBSERVER
    if observer is not None:
        observer(bytes_read)
    return _publish_codex_cache_state(
        cache_key,
        prior_value,
        updated_state,
        content,
        binding_generation,
    )


def _omp_sessions_root() -> Path:
    raw = os.environ.get("OMP_SESSIONS_DIR")
    if raw:
        return Path(raw).expanduser()
    return Path.home() / ".omp" / "agent" / "sessions"


def _safe_omp_session_path(value: str) -> Path | None:
    candidate = Path(str(value or "")).expanduser()
    root = _omp_sessions_root()
    try:
        candidate.resolve().relative_to(root.resolve())
    except (ValueError, OSError):
        return None
    if candidate.suffix != ".jsonl" or not candidate.is_file():
        return None
    return candidate


def _omp_valid_git_directory(path: Path) -> bool:
    """Require bounded, parseable Git HEAD evidence inside a git directory."""
    try:
        if not path.is_dir():
            return False
        with open(path / "HEAD", encoding="ascii") as handle:
            head_text = handle.read(257)
    except (OSError, UnicodeError):
        return False
    if len(head_text) > 256:
        return False
    lines = head_text.splitlines()
    if len(lines) != 1:
        return False
    head = lines[0]
    if re.fullmatch(r"[0-9a-fA-F]{40}|[0-9a-fA-F]{64}", head):
        return True
    if not head.startswith("ref: refs/"):
        return False
    reference = head[len("ref: ") :]
    return bool(reference) and all(
        part not in {"", ".", ".."} for part in reference.split("/")
    ) and all(character.isalnum() or character in "._-/" for character in reference)


def _omp_project_root(session_file: Path) -> Path | None:
    """Return the nearest repository root proven by the session cwd."""
    try:
        with open(session_file, encoding="utf-8", errors="replace") as handle:
            for _index in range(32):
                line = handle.readline()
                if not line or handle.tell() > 65536:
                    break
                try:
                    entry = json.loads(line)
                except (TypeError, json.JSONDecodeError):
                    continue
                if not isinstance(entry, Mapping) or entry.get("type") != "session":
                    continue
                cwd = entry.get("cwd")
                if type(cwd) is not str or not cwd.strip():
                    return None
                resolved_cwd = Path(cwd).expanduser().resolve(strict=True)
                if not resolved_cwd.is_dir():
                    return None
                for root in (resolved_cwd, *resolved_cwd.parents):
                    git_marker = root / ".git"
                    if git_marker.is_dir():
                        return root if _omp_valid_git_directory(git_marker) else None
                    if not git_marker.exists():
                        continue
                    if not git_marker.is_file():
                        return None
                    with open(git_marker, encoding="utf-8") as marker_handle:
                        marker_text = marker_handle.read(4097)
                    if len(marker_text) > 4096:
                        return None
                    marker_lines = marker_text.splitlines()
                    if len(marker_lines) != 1 or not marker_lines[0].startswith("gitdir:"):
                        return None
                    reference = marker_lines[0][len("gitdir:") :].strip()
                    if not reference:
                        return None
                    git_dir = Path(reference)
                    if not git_dir.is_absolute():
                        git_dir = git_marker.parent / git_dir
                    resolved_git_dir = git_dir.resolve(strict=True)
                    return root if _omp_valid_git_directory(resolved_git_dir) else None
                return None
    except (OSError, RuntimeError, UnicodeError, ValueError):
        return None
    return None


def _omp_file_id(stat_result: os.stat_result) -> tuple[int, int]:
    return (int(stat_result.st_dev), int(stat_result.st_ino))


def _omp_checkpoint_matches_stat(
    state: _OmpSessionState | None,
    stat_result: os.stat_result,
) -> bool:
    return bool(
        state is not None
        and state.file_id == _omp_file_id(stat_result)
        and state.observed_size == int(stat_result.st_size)
        and state.mtime_ns == int(stat_result.st_mtime_ns)
        and state.ctime_ns == int(stat_result.st_ctime_ns)
    )


class _OmpFileChanged(Exception):
    pass


def _omp_binding_cache_key(path_value: str) -> str | None:
    candidate = Path(str(path_value or "")).expanduser()
    if candidate.suffix != ".jsonl":
        return None
    try:
        resolved = candidate.resolve()
        resolved.relative_to(_omp_sessions_root().resolve())
    except (ValueError, OSError):
        return None
    return os.fspath(resolved)


def _omp_cache_key(path_value: str) -> str | None:
    if _safe_omp_session_path(path_value) is None:
        return None
    return _omp_binding_cache_key(path_value)


def _prune_omp_cache_for_bindings(bindings: list[WorkerBinding]) -> None:
    live_fingerprint_sets: dict[str, set[str]] = {}
    live_keys: set[str] = set()
    for binding in bindings:
        if (
            binding.turn_target_kind != _OMP_SESSION_TURN_KIND
            or not _eligible_turn_binding(binding)
        ):
            continue
        cache_key = _omp_binding_cache_key(str(binding.turn_target_value or ""))
        if cache_key is not None:
            live_keys.add(cache_key)
            live_fingerprint_sets.setdefault(cache_key, set()).add(
                binding.private_fingerprint
            )
    global _OMP_SESSION_CACHE_GENERATION_COUNTER
    global _OMP_SESSION_CACHE_BINDING_FINGERPRINTS
    global _OMP_SESSION_CACHE_BINDING_GENERATIONS
    global _OMP_SESSION_CACHE_LIVE_KEYS
    with _OMP_SESSION_CACHE_LOCK:
        live_fingerprints = {
            cache_key: tuple(sorted(fingerprints))
            for cache_key, fingerprints in live_fingerprint_sets.items()
        }
        changed_keys = {
            cache_key
            for cache_key in live_keys
            if (
                _OMP_SESSION_CACHE_LIVE_KEYS is not None
                and cache_key in _OMP_SESSION_CACHE_LIVE_KEYS
                and _OMP_SESSION_CACHE_BINDING_FINGERPRINTS.get(cache_key)
                != live_fingerprints.get(cache_key)
            )
        }
        generations: dict[str, int] = {}
        for cache_key in live_keys:
            if (
                _OMP_SESSION_CACHE_LIVE_KEYS is not None
                and cache_key in _OMP_SESSION_CACHE_LIVE_KEYS
                and _OMP_SESSION_CACHE_BINDING_FINGERPRINTS.get(cache_key)
                == live_fingerprints.get(cache_key)
            ):
                generations[cache_key] = _OMP_SESSION_CACHE_BINDING_GENERATIONS[cache_key]
            else:
                _OMP_SESSION_CACHE_GENERATION_COUNTER += 1
                generations[cache_key] = _OMP_SESSION_CACHE_GENERATION_COUNTER
        _OMP_SESSION_CACHE_LIVE_KEYS = live_keys
        _OMP_SESSION_CACHE_BINDING_FINGERPRINTS = live_fingerprints
        _OMP_SESSION_CACHE_BINDING_GENERATIONS = generations
        for cache_key in tuple(_OMP_SESSION_CACHE):
            if cache_key not in live_keys or cache_key in changed_keys:
                del _OMP_SESSION_CACHE[cache_key]


def _read_omp_jsonl_lines(
    session_file: Path,
    *,
    start_offset: int,
    drop_first_partial: bool,
    expected_file_id: tuple[int, int],
) -> tuple[list[str], int, int]:
    try:
        with open(session_file, "rb") as handle:
            opened_stat = os.fstat(handle.fileno())
            if (
                _omp_file_id(opened_stat) != expected_file_id
                or int(opened_stat.st_size) < start_offset
            ):
                raise _OmpFileChanged
            handle.seek(start_offset)
            blob = handle.read()
            completed_stat = os.fstat(handle.fileno())
            current_stat = session_file.stat()
            opened_signature = (
                _omp_file_id(opened_stat),
                int(opened_stat.st_size),
                int(opened_stat.st_mtime_ns),
                int(opened_stat.st_ctime_ns),
            )
            completed_signature = (
                _omp_file_id(completed_stat),
                int(completed_stat.st_size),
                int(completed_stat.st_mtime_ns),
                int(completed_stat.st_ctime_ns),
            )
            current_signature = (
                _omp_file_id(current_stat),
                int(current_stat.st_size),
                int(current_stat.st_mtime_ns),
                int(current_stat.st_ctime_ns),
            )
            if (
                completed_signature != opened_signature
                or current_signature != completed_signature
            ):
                raise _OmpFileChanged
    except _OmpFileChanged:
        raise
    except OSError as exc:
        raise _TurnReadFailed from exc
    if not blob:
        return [], start_offset, 0

    offset = start_offset
    segments = blob.splitlines(keepends=True)
    if drop_first_partial and segments:
        offset += len(segments[0])
        segments = segments[1:]

    lines: list[str] = []
    for index, segment in enumerate(segments):
        line_bytes = segment.rstrip(b"\r\n")
        if not line_bytes:
            offset += len(segment)
            continue
        text = line_bytes.decode("utf-8", "replace")
        has_line_end = segment.endswith(b"\n") or segment.endswith(b"\r")
        if not has_line_end and index == len(segments) - 1:
            try:
                json.loads(text)
            except (TypeError, json.JSONDecodeError):
                break
        lines.append(text)
        offset += len(segment)
    return lines, offset, len(blob)


def _omp_message_entry_from_line(line: str) -> tuple[Mapping[str, Any], Mapping[str, Any]] | None:
    try:
        entry = json.loads(line)
    except (TypeError, json.JSONDecodeError):
        return None
    if not isinstance(entry, Mapping) or entry.get("type") != "message":
        return None
    message = entry.get("message")
    if not isinstance(message, Mapping):
        return None
    return entry, message


def _is_omp_user_message(message: Mapping[str, Any]) -> bool:
    return str(message.get("role") or "") == "user" and str(message.get("attribution") or "") == "user"


def _last_omp_user_line_index(lines: list[str]) -> int | None:
    found: int | None = None
    for index, line in enumerate(lines):
        parsed = _omp_message_entry_from_line(line)
        if parsed is None:
            continue
        _entry, message = parsed
        text = _omp_message_text(message)
        if _is_omp_user_message(message) and text and not _is_internal_user_text(text):
            found = index
    return found


def _omp_thinking_snippet(message: Mapping[str, Any]) -> str:
    """Compact progress line from an omp thinking block: its bold headline,
    falling back to a trimmed first line."""
    content = message.get("content")
    if not isinstance(content, list):
        return ""
    for item in content:
        if not isinstance(item, Mapping) or item.get("type") != "thinking":
            continue
        text = str(item.get("thinking") or "").strip()
        if not text:
            continue
        first = text.splitlines()[0].strip()
        if first.startswith("**") and first.endswith("**") and len(first) > 4:
            return first.strip("*").strip()
        return first[:120]
    return ""


_OMP_FILE_ACTIONS = {
    "read": "read",
    "read_file": "read",
    "write": "write",
    "write_file": "write",
    "edit": "edit",
    "edit_file": "edit",
    "apply_patch": "edit",
}
_OMP_ACTIONS = {
    "grep": "search",
    "search": "search",
    "ast_grep": "search",
    "glob": "list files",
    "list_files": "list files",
    "browser": "browse",
    "web_search": "search web",
    "task": "delegate",
    "agent": "delegate",
    "lsp": "inspect code",
}
_OMP_PRIVATE_PATH_SEGMENTS = frozenset(
    {
        ".git",
        ".netrc",
        ".npmrc",
        ".pypirc",
        ".ssh",
        "auth.json",
        "credential",
        "credentials",
        "credentials.json",
        "id_dsa",
        "id_ecdsa",
        "id_ed25519",
        "id_rsa",
        "secret",
        "secrets",
        "secrets.json",
        "token",
        "tokens",
    }
)


def _omp_path_segment_is_private(value: str) -> bool:
    lowered = value.lower()
    return (
        lowered in _OMP_PRIVATE_PATH_SEGMENTS
        or lowered.startswith(".env")
        or lowered.endswith(".key")
    )
_OMP_SHELL_TOOLS = frozenset({"bash", "shell", "sh", "exec", "execute", "run"})


def _omp_tool_name(item: Mapping[str, Any]) -> str:
    raw = item.get("name")
    if raw is None:
        raw = item.get("toolName")
    if raw is None:
        raw = item.get("tool")
    if type(raw) is not str:
        return ""
    return re.sub(r"[^a-z0-9]+", "_", raw.strip().lower()).strip("_")


def _omp_tool_arguments(item: Mapping[str, Any]) -> Mapping[str, Any]:
    for key in ("arguments", "input", "args"):
        value = item.get(key)
        if isinstance(value, Mapping):
            return value
    return {}


def _omp_repo_relative_path(
    arguments: Mapping[str, Any],
    project_root: Path | None,
) -> str | None:
    if project_root is None:
        return None
    raw: Any = None
    for key in ("path", "file_path", "filepath", "file"):
        if key in arguments:
            raw = arguments.get(key)
            break
    if type(raw) is not str:
        return None
    text = raw.strip()
    if not text or text.startswith("~") or any(ord(character) < 32 for character in text):
        return None
    try:
        root = project_root.resolve(strict=True)
        candidate = Path(text)
        if ".." in candidate.parts:
            return None
        resolved = candidate.resolve(strict=False) if candidate.is_absolute() else (root / candidate).resolve(strict=False)
        relative = resolved.relative_to(root)
    except (OSError, RuntimeError, ValueError):
        return None
    if any(_omp_path_segment_is_private(part) for part in relative.parts):
        return None
    public_path = relative.as_posix()
    if not public_path or public_path == "." or len(public_path) > 120:
        return None
    allowed = frozenset("._-+@/ ")
    if any(not (character.isascii() and (character.isalnum() or character in allowed)) for character in public_path):
        return None
    return public_path


def _omp_shell_progress(arguments: Mapping[str, Any]) -> _PublicOmpToolProgress:
    command = arguments.get("command")
    if command is None:
        command = arguments.get("cmd")
    if type(command) is not str or any(character in command for character in "\r\n;&|`$<>"):
        return _PublicOmpToolProgress("run command")
    try:
        tokens = shlex.split(command)
    except ValueError:
        return _PublicOmpToolProgress("run command")
    if not tokens:
        return _PublicOmpToolProgress("run command")
    if tokens[:2] == ["git", "status"] and all(
        token in {"--short", "--porcelain", "--branch", "-s", "-sb"}
        for token in tokens[2:]
    ):
        return _PublicOmpToolProgress("git status")
    if tokens[0] == "pytest" or tokens[:3] in (["python", "-m", "pytest"], ["python3", "-m", "pytest"]):
        return _PublicOmpToolProgress("test", "pytest")
    if tokens[:3] == ["uv", "run", "pytest"]:
        return _PublicOmpToolProgress("test", "pytest")
    if len(tokens) >= 2 and tokens[0] in {"bun", "cargo", "go", "npm"} and tokens[1] == "test":
        return _PublicOmpToolProgress("test", tokens[0])
    if len(tokens) >= 2 and tokens[0] in {"cargo", "go"} and tokens[1] == "build":
        return _PublicOmpToolProgress("build", tokens[0])
    if tokens[:3] == ["npm", "run", "build"]:
        return _PublicOmpToolProgress("build", "npm")
    if tokens[0] == "make":
        return _PublicOmpToolProgress("build", "make")
    if tokens[:3] in (["python", "-m", "build"], ["python3", "-m", "build"]):
        return _PublicOmpToolProgress("build", "python")
    return _PublicOmpToolProgress("run command")


def _omp_public_tool_progress(
    item: Mapping[str, Any],
    project_root: Path | None,
) -> _PublicOmpToolProgress:
    name = _omp_tool_name(item)
    arguments = _omp_tool_arguments(item)
    if name in _OMP_FILE_ACTIONS:
        action = _OMP_FILE_ACTIONS[name]
        subject = _omp_repo_relative_path(arguments, project_root)
        return _PublicOmpToolProgress(action, subject) if subject else _PublicOmpToolProgress(f"{action} file")
    if name in _OMP_SHELL_TOOLS:
        return _omp_shell_progress(arguments)
    return _PublicOmpToolProgress(_OMP_ACTIONS.get(name, "tool"))


def _omp_tool_snippet(
    item: Mapping[str, Any],
    step: int,
    project_root: Path | None = None,
) -> str:
    return _omp_public_tool_progress(item, project_root).render(step)


def _apply_omp_progress_message(
    state: _OmpTurnState,
    message: Mapping[str, Any],
) -> bool:
    contributed = False
    text = _omp_message_text(message)
    if text:
        _append_unique_recent(state.stream_parts, text)
        contributed = True

    content = message.get("content")
    if not isinstance(content, list):
        return contributed
    for item in content:
        if not isinstance(item, Mapping):
            continue
        kind = str(item.get("type") or "")
        if kind == "thinking":
            snippet = _omp_thinking_snippet({"content": [item]})
            if snippet:
                _append_unique_recent(state.stream_parts, snippet)
                contributed = True
            continue
        if kind == "toolCall":
            state.tool_count += 1
            snippet = _omp_tool_snippet(item, state.tool_count, state.project_root)
            if snippet:
                _append_unique_recent(state.stream_parts, snippet)
                contributed = True
    return contributed


def _omp_message_text(message: Mapping[str, Any]) -> str:
    content = message.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "\n".join(
            str(item.get("text") or "")
            for item in content
            if isinstance(item, Mapping)
            and item.get("type") == "text"
            and str(item.get("text") or "").strip()
        )
    return ""


def _apply_omp_lines_to_state(state: _OmpTurnState, lines: list[str]) -> None:
    for line in lines:
        parsed = _omp_message_entry_from_line(line)
        if parsed is None:
            continue
        entry, message = parsed
        role = str(message.get("role") or "")
        text = _omp_message_text(message)
        if role == "user":
            if not _is_omp_user_message(message):
                continue
            if not text or _is_internal_user_text(text):
                continue
            state.prompt_id = str(entry.get("id") or "")
            state.user_text = text
            state.stream_parts = []
            state.final_text = ""
            state.tool_count = 0
            continue
        if role != "assistant" or not state.prompt_id:
            continue
        if state.final_text:
            continue
        if str(message.get("stopReason") or "") == "stop" and text:
            state.final_text = text
            state.stream_parts = []
            continue
        _apply_omp_progress_message(state, message)


def _read_omp_state_from_recent(
    session_file: Path,
    size: int,
    file_id: tuple[int, int],
    project_root: Path | None,
) -> tuple[_OmpTurnState, int, int, int]:
    turn = _OmpTurnState(project_root=project_root)
    if size <= 0:
        return turn, 0, 0, 0

    window = min(size, max(1, _OMP_TAIL_BYTES))
    selected_lines: list[str] = []
    selected_offset = size
    replay_offset = size
    bytes_read = 0
    while True:
        start = max(0, size - window)
        lines, next_offset, consumed = _read_omp_jsonl_lines(
            session_file,
            start_offset=start,
            drop_first_partial=start > 0,
            expected_file_id=file_id,
        )
        bytes_read += consumed
        user_index = _last_omp_user_line_index(lines)
        if user_index is not None:
            selected_lines = lines[user_index:]
            selected_offset = next_offset
            replay_offset = start
            break
        if start == 0:
            selected_lines = lines
            selected_offset = next_offset
            replay_offset = 0
            break
        window = min(size, window * 2)

    _apply_omp_lines_to_state(turn, selected_lines)
    return turn, selected_offset, replay_offset, bytes_read


def _omp_state_to_content(state: _OmpTurnState) -> Mapping[str, Any] | None:
    if not state.prompt_id:
        return None
    has_final = bool(state.final_text)
    content: dict[str, Any] = {
        "user_text": state.user_text or None,
        "assistant_stream_text": None if has_final else ("\n\n".join(state.stream_parts) or None),
        "assistant_final_text": state.final_text or None,
        "complete": has_final,
        "has_open_turn": not has_final,
        "source_turn_id": state.prompt_id[:160],
    }
    if _is_internal_turn_content(content):
        return None
    if not (content.get("user_text") or content.get("assistant_stream_text") or content.get("assistant_final_text")):
        return None
    return content


def _publish_omp_cache_state(
    cache_key: str,
    prior_state_value: Mapping[str, Any] | None,
    updated_state: _OmpSessionState,
    content: Mapping[str, Any] | None,
    binding_generation: int | None = None,
) -> Mapping[str, Any] | None:
    """Atomically publish a winning parser state and never return a losing view."""
    with _OMP_SESSION_CACHE_LOCK:
        current_generation = _omp_cache_binding_generation_locked(cache_key)
        if (
            binding_generation != current_generation
            or (
                _OMP_SESSION_CACHE_LIVE_KEYS is not None
                and cache_key not in _OMP_SESSION_CACHE_LIVE_KEYS
            )
        ):
            return None
        current_state = _OMP_SESSION_CACHE.get(cache_key)
        current_value = _serialize_omp_state(current_state)
        if (
            current_value == prior_state_value
            or current_state is None
            or (
                current_state.file_id == updated_state.file_id
                and updated_state.offset > current_state.offset
            )
        ):
            _omp_cache_store_locked(cache_key, updated_state)
            return content
        return None


def _read_omp_session_turn_with_state(
    path_value: str,
    prior_state: _OmpSessionState | None,
) -> tuple[Mapping[str, Any] | None, _OmpSessionState, int] | None:
    """Parse one OMP session while returning only compact checkpoint coordinates."""
    session_file = _safe_omp_session_path(path_value)
    if session_file is None:
        return None
    for _attempt in range(2):
        try:
            stat_result = session_file.stat()
        except OSError as exc:
            raise _TurnReadFailed from exc
        size = int(stat_result.st_size)
        file_id = _omp_file_id(stat_result)
        reusable = bool(
            prior_state is not None
            and prior_state.file_id == file_id
            and size >= prior_state.offset
            and (
                size > prior_state.observed_size
                or (
                    size == prior_state.observed_size
                    and prior_state.mtime_ns == int(stat_result.st_mtime_ns)
                    and prior_state.ctime_ns == int(stat_result.st_ctime_ns)
                )
            )
        )
        try:
            if reusable:
                start_offset = (
                    prior_state.replay_offset if prior_state.turn_open else prior_state.offset
                )
                lines, next_offset, bytes_read = _read_omp_jsonl_lines(
                    session_file,
                    start_offset=start_offset,
                    drop_first_partial=False,
                    expected_file_id=file_id,
                )
                turn = _OmpTurnState(project_root=prior_state.project_root)
                _apply_omp_lines_to_state(turn, lines)
                replay_offset = start_offset
            else:
                project_root = _omp_project_root(session_file)
                turn, next_offset, replay_offset, bytes_read = _read_omp_state_from_recent(
                    session_file,
                    size,
                    file_id,
                    project_root,
                )
            content = _omp_state_to_content(turn)
            turn_open = bool(content is not None and content.get("has_open_turn") is True)
            checkpoint = _OmpSessionState(
                offset=next_offset,
                observed_size=max(size, next_offset),
                file_id=file_id,
                mtime_ns=int(stat_result.st_mtime_ns),
                ctime_ns=int(stat_result.st_ctime_ns),
                replay_offset=replay_offset if turn_open else next_offset,
                turn_open=turn_open,
                project_root=turn.project_root,
            )
            return content, checkpoint, bytes_read
        except _OmpFileChanged:
            prior_state = None
    raise _TurnReadFailed


def _read_omp_session_turn(path_value: str) -> Mapping[str, Any] | None:
    """Parse an OMP session while retaining private state in the local process."""
    cache_key = _omp_cache_key(path_value)
    if cache_key is None:
        return None
    with _OMP_SESSION_CACHE_LOCK:
        prior_state = _omp_cache_get_locked(cache_key)
        prior_state_value = _serialize_omp_state(prior_state)
        binding_generation = _omp_cache_binding_generation_locked(cache_key)
    try:
        unchanged = prior_state is not None and _omp_checkpoint_matches_stat(
            prior_state,
            Path(cache_key).stat(),
        )
    except OSError as exc:
        raise _TurnReadFailed from exc
    if unchanged:
        return None
    parsed = _read_omp_session_turn_with_state(path_value, prior_state)
    if parsed is None:
        return None
    content, state, _bytes_read = parsed
    return _publish_omp_cache_state(
        cache_key,
        prior_state_value,
        state,
        content,
        binding_generation,
    )


@dataclass(frozen=True)
class TurnRefreshKey:
    """Opaque scheduler identity for one durable private binding."""

    private_fingerprint: str


@dataclass(frozen=True)
class _TurnRefreshItem:
    key: TurnRefreshKey
    worker_id: str
    worker_fingerprint: str
    turn_target_kind: str
    turn_target_value: str

    @classmethod
    def from_binding(cls, binding: WorkerBinding) -> "_TurnRefreshItem":
        return cls(
            key=TurnRefreshKey(binding.private_fingerprint),
            worker_id=binding.worker_id,
            worker_fingerprint=binding.worker_fingerprint,
            turn_target_kind=str(binding.turn_target_kind or ""),
            turn_target_value=str(binding.turn_target_value or ""),
        )


TurnRefreshStatus = Literal[
    "updated",
    "unchanged",
    "missing",
    "timeout",
    "failed",
    "stale_binding",
]


class _BindingLookupFailed(Exception):
    pass


@dataclass(frozen=True)
class TurnRefreshResult:
    """Public-safe result of one binding refresh."""

    status: TurnRefreshStatus
    updated: int
    pending_changed: bool = False
    retry_binding_lookup: bool = False
    binding_validated: bool = False


@dataclass(frozen=True)
class CompletedPaneTurnRefreshResult:
    """Result of one completion-authoritative, pane-targeted semantic refresh."""

    status: str
    worker_id: str | None = None
    refreshed_turn_id: str | None = None


_ELIGIBLE_TURN_TARGET_KINDS = frozenset(
    {_CODEX_SESSION_TURN_KIND, _OMP_SESSION_TURN_KIND, _PANE_TURN_KIND}
)
_TURN_INGESTION_QUEUE_CAPACITY = 64


def _eligible_turn_binding(binding: WorkerBinding) -> bool:
    return bool(
        binding.turn_target_kind in _ELIGIBLE_TURN_TARGET_KINDS
        and binding.turn_target_value
        and binding.private_fingerprint
    )


def _isolated_content_is_valid(value: Any) -> bool:
    if not isinstance(value, Mapping):
        return False
    if not set(value).issubset({*_TURN_CONTENT_KEYS, "source_turn_id"}):
        return False
    for key in ("user_text", "assistant_final_text", "assistant_stream_text", "model", "source_turn_id"):
        if key in value and value[key] is not None and type(value[key]) is not str:
            return False
    for key in ("complete", "has_open_turn"):
        if key in value and type(value[key]) is not bool:
            return False
    return True


@dataclass(frozen=True)
class _OmpCachePublication:
    cache_key: str
    prior_state_value: Mapping[str, Any] | None
    updated_state: _OmpSessionState
    binding_generation: int | None
@dataclass(frozen=True)
class _CodexCachePublication:
    cache_key: tuple[str, str]
    prior_state_value: Mapping[str, Any] | None
    updated_state: _CodexSessionState
    binding_generation: int | None
    resolver_generation: int




@dataclass(frozen=True)
class _ObservedFileTurn:
    content: Mapping[str, Any] | None
    publication: _OmpCachePublication | _CodexCachePublication


def _blocking_recv_frame(sock: socket.socket, maximum: int) -> bytes:
    header = bytearray()
    while len(header) < _OMP_FRAME_HEADER.size:
        chunk = sock.recv(_OMP_FRAME_HEADER.size - len(header))
        if not chunk:
            raise EOFError
        header.extend(chunk)
    length = _OMP_FRAME_HEADER.unpack(header)[0]
    if length > maximum:
        raise ValueError("oversized IPC frame")
    payload = bytearray()
    while len(payload) < length:
        chunk = sock.recv(min(65536, length - len(payload)))
        if not chunk:
            raise EOFError
        payload.extend(chunk)
    return bytes(payload)


def _blocking_send_frame(sock: socket.socket, payload: bytes) -> None:
    sock.sendall(_OMP_FRAME_HEADER.pack(len(payload)) + payload)


def _check_ipc_deadline(
    deadline: float,
    cancel_event: threading.Event | None,
) -> float:
    if cancel_event is not None and cancel_event.is_set():
        raise _TurnReadTimeout
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise _TurnReadTimeout
    return remaining


def _send_frame_until(
    sock: socket.socket,
    payload: bytes,
    deadline: float,
    cancel_event: threading.Event | None,
) -> None:
    framed = _OMP_FRAME_HEADER.pack(len(payload)) + payload
    view = memoryview(framed)
    while view:
        remaining = _check_ipc_deadline(deadline, cancel_event)
        _readable, writable, _exceptional = select.select(
            [],
            [sock],
            [],
            min(0.05, remaining),
        )
        if not writable:
            continue
        sent = sock.send(view)
        if sent <= 0:
            raise OSError("IPC socket closed")
        view = view[sent:]


def _recv_frame_until(
    sock: socket.socket,
    deadline: float,
    cancel_event: threading.Event | None,
    maximum: int = _CODEX_IPC_FRAME_MAX_BYTES,
) -> bytes:
    framed = bytearray()
    expected: int | None = None
    while expected is None or len(framed) < _OMP_FRAME_HEADER.size + expected:
        remaining = _check_ipc_deadline(deadline, cancel_event)
        readable, _writable, _exceptional = select.select(
            [sock],
            [],
            [],
            min(0.05, remaining),
        )
        if not readable:
            continue
        if expected is None:
            remaining_bytes = _OMP_FRAME_HEADER.size - len(framed)
        else:
            remaining_bytes = _OMP_FRAME_HEADER.size + expected - len(framed)
        chunk = sock.recv(min(65536, remaining_bytes))
        if not chunk:
            raise EOFError
        framed.extend(chunk)
        if expected is None and len(framed) >= _OMP_FRAME_HEADER.size:
            expected = _OMP_FRAME_HEADER.unpack(framed[: _OMP_FRAME_HEADER.size])[0]
            if expected > maximum:
                raise ValueError("oversized IPC frame")
    assert expected is not None
    return bytes(framed[_OMP_FRAME_HEADER.size : _OMP_FRAME_HEADER.size + expected])
def _blocking_send_streamed_omp_response(
    sock: socket.socket,
    payload: bytes,
    nonce: str,
    *,
    chunk_bytes: int = _OMP_IPC_RESPONSE_CHUNK_BYTES,
) -> None:
    if (
        type(chunk_bytes) is not int
        or chunk_bytes <= 0
        or chunk_bytes > _OMP_IPC_RESPONSE_CHUNK_BYTES
    ):
        raise ValueError("invalid OMP IPC chunk bound")
    chunk_count = (len(payload) + chunk_bytes - 1) // chunk_bytes
    manifest = {
        "protocol": 1,
        "nonce": nonce,
        "stream": "omp_response",
        "chunks": chunk_count,
        "total_bytes": len(payload),
        "chunk_bytes": chunk_bytes,
    }
    _blocking_send_frame(
        sock,
        json.dumps(manifest, separators=(",", ":")).encode("utf-8"),
    )
    view = memoryview(payload)
    for offset in range(0, len(payload), chunk_bytes):
        _blocking_send_frame(sock, bytes(view[offset : offset + chunk_bytes]))
    end = {
        "protocol": 1,
        "nonce": nonce,
        "stream": "omp_response_end",
    }
    _blocking_send_frame(
        sock,
        json.dumps(end, separators=(",", ":")).encode("utf-8"),
    )


def _recv_streamed_omp_response_until(
    sock: socket.socket,
    first_payload: bytes,
    nonce: str,
    target_kind: str,
    deadline: float,
    cancel_event: threading.Event | None,
) -> bytes:
    try:
        manifest = json.loads(first_payload.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError):
        return first_payload
    if not isinstance(manifest, Mapping) or "stream" not in manifest:
        return first_payload
    if (
        target_kind != _OMP_SESSION_TURN_KIND
        or set(manifest)
        != {
            "protocol",
            "nonce",
            "stream",
            "chunks",
            "total_bytes",
            "chunk_bytes",
        }
        or manifest["protocol"] != 1
        or manifest["stream"] != "omp_response"
        or type(manifest["nonce"]) is not str
        or not secrets.compare_digest(manifest["nonce"], nonce)
        or type(manifest["chunks"]) is not int
        or manifest["chunks"] <= 0
        or type(manifest["total_bytes"]) is not int
        or manifest["total_bytes"] <= 0
        or type(manifest["chunk_bytes"]) is not int
        or manifest["chunk_bytes"] <= 0
        or manifest["chunk_bytes"] > _OMP_IPC_RESPONSE_CHUNK_BYTES
        or manifest["chunks"]
        != (
            manifest["total_bytes"] + manifest["chunk_bytes"] - 1
        )
        // manifest["chunk_bytes"]
    ):
        raise _TurnReadFailed
    assembled = bytearray()
    for index in range(manifest["chunks"]):
        chunk = _recv_frame_until(
            sock,
            deadline,
            cancel_event,
            manifest["chunk_bytes"],
        )
        expected = (
            manifest["chunk_bytes"]
            if index + 1 < manifest["chunks"]
            else manifest["total_bytes"] - len(assembled)
        )
        if len(chunk) != expected:
            raise _TurnReadFailed
        assembled.extend(chunk)
    if len(assembled) != manifest["total_bytes"]:
        raise _TurnReadFailed
    end_payload = _recv_frame_until(sock, deadline, cancel_event, 1024)
    try:
        end = json.loads(end_payload.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise _TurnReadFailed from exc
    if (
        not isinstance(end, Mapping)
        or set(end) != {"protocol", "nonce", "stream"}
        or end["protocol"] != 1
        or end["stream"] != "omp_response_end"
        or type(end["nonce"]) is not str
        or not secrets.compare_digest(end["nonce"], nonce)
    ):
        raise _TurnReadFailed
    while True:
        remaining = _check_ipc_deadline(deadline, cancel_event)
        readable, _writable, _exceptional = select.select(
            [sock],
            [],
            [],
            min(0.05, remaining),
        )
        if not readable:
            continue
        if sock.recv(1):
            raise _TurnReadFailed
        break
    return bytes(assembled)




def _omp_publication_commit(
    publication: _OmpCachePublication,
    content: Mapping[str, Any] | None,
) -> Mapping[str, Any] | None:
    return _publish_omp_cache_state(
        publication.cache_key,
        publication.prior_state_value,
        publication.updated_state,
        content,
        publication.binding_generation,
    )
def _codex_publication_commit(
    publication: _CodexCachePublication,
    content: Mapping[str, Any] | None,
) -> Mapping[str, Any] | None:
    resolution = _resolve_codex_session(publication.cache_key[1])
    if (
        resolution is None
        or resolution.status != "found"
        or resolution.root != publication.cache_key[0]
        or resolution.root_file_id != publication.updated_state.root_file_id
        or resolution.generation != publication.resolver_generation
        or resolution.canonical_path != publication.updated_state.canonical_path
        or resolution.file_id != publication.updated_state.file_id
    ):
        return None
    return _publish_codex_cache_state(
        publication.cache_key,
        publication.prior_state_value,
        publication.updated_state,
        content,
        publication.binding_generation,
    )


def _file_publication_commit(
    publication: _OmpCachePublication | _CodexCachePublication,
    content: Mapping[str, Any] | None,
) -> Mapping[str, Any] | None:
    if isinstance(publication, _CodexCachePublication):
        return _codex_publication_commit(publication, content)
    return _omp_publication_commit(publication, content)






def _file_turn_child(channel: socket.socket) -> None:
    """Spawn entry point using one bounded, source-tagged private frame."""
    nonce = ""
    try:
        payload = _blocking_recv_frame(channel, _CODEX_STATE_IPC_MAX_BYTES)
        request = json.loads(payload.decode("utf-8"))
        if not isinstance(request, Mapping) or set(request) != {
            "protocol",
            "nonce",
            "target_kind",
            "target_value",
            "parser_state",
        }:
            raise ValueError("invalid turn reader request")
        nonce = request["nonce"]
        target_kind = request["target_kind"]
        target_value = request["target_value"]
        parser_state = request["parser_state"]
        if (
            request["protocol"] != 1
            or type(nonce) is not str
            or not nonce
            or type(target_kind) is not str
            or type(target_value) is not str
            or not isinstance(parser_state, Mapping)
            or set(parser_state) != {"source", "state"}
        ):
            raise ValueError("invalid turn reader request values")
        source = parser_state["source"]
        content: Mapping[str, Any] | None
        response_state: dict[str, Any] | None = None
        bytes_read = 0
        disposition = "ok"
        if target_kind == _CODEX_SESSION_TURN_KIND:
            if source != "codex":
                raise ValueError("wrong parser state source")
            supplied = _deserialize_codex_state(parser_state["state"])
            if supplied is None:
                raise ValueError("missing Codex parser state")
            parsed = _read_codex_session_turn_with_state(target_value, supplied)
            if parsed is None:
                content = None
                disposition = "missing"
            else:
                content, updated_state, bytes_read = parsed
                response_state = {
                    "source": "codex",
                    "state": _serialize_codex_state(updated_state),
                }
        elif target_kind == _OMP_SESSION_TURN_KIND:
            if source != "omp":
                raise ValueError("wrong parser state source")
            prior_state = _deserialize_omp_state(parser_state["state"])
            parsed = _read_omp_session_turn_with_state(target_value, prior_state)
            if parsed is None:
                content = None
                disposition = "missing"
            else:
                content, updated_state, bytes_read = parsed
                response_state = {
                    "source": "omp",
                    "state": _serialize_omp_state(updated_state),
                }
        else:
            raise ValueError("unsupported turn reader target")
        response = {
            "protocol": 1,
            "nonce": nonce,
            "disposition": disposition,
            "content": dict(content) if content is not None else None,
            "parser_state": response_state,
            "bytes_read": bytes_read,
        }
        encoded = json.dumps(
            response,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        if target_kind == _CODEX_SESSION_TURN_KIND:
            if len(encoded) > _CODEX_IPC_FRAME_MAX_BYTES:
                raise ValueError("oversized Codex turn reader response")
            _blocking_send_frame(channel, encoded)
        elif len(encoded) > _OMP_IPC_RESPONSE_CHUNK_BYTES:
            _blocking_send_streamed_omp_response(channel, encoded, nonce)
        else:
            _blocking_send_frame(channel, encoded)
    except BaseException:
        try:
            response = {
                "protocol": 1,
                "nonce": nonce,
                "disposition": "failed",
                "content": None,
                "parser_state": None,
                "bytes_read": 0,
            }
            _blocking_send_frame(
                channel,
                json.dumps(response, separators=(",", ":")).encode("utf-8"),
            )
        except BaseException:
            pass
    finally:
        channel.close()


def _terminate_and_reap(process: multiprocessing.Process) -> None:
    """Reap within a fixed grace after the request deadline under normal POSIX scheduling."""
    if process.pid is None:
        return
    teardown_deadline = time.monotonic() + _OMP_TEARDOWN_GRACE_SECONDS
    if not process.is_alive():
        process.join(0)
        return
    process.terminate()
    process.join(min(0.05, max(0.0, teardown_deadline - time.monotonic())))
    if process.is_alive():
        process.kill()
        process.join(max(0.0, teardown_deadline - time.monotonic()))
    if process.is_alive():
        # Never trade the caller's hard teardown bound for an unbounded join.
        # A SIGKILL-resistant kernel task remains an OS-level exceptional case.
        process.kill()


def _read_file_turn_isolated(
    target_kind: str,
    target_value: str,
    *,
    timeout_seconds: float,
    cancel_event: threading.Event | None = None,
    defer_cache: bool = False,
) -> Mapping[str, Any] | _ObservedFileTurn | object | None:
    """Read through one bounded source-tagged IPC request."""
    deadline = time.monotonic() + float(timeout_seconds)
    if (
        type(target_kind) is not str
        or type(target_value) is not str
        or len(target_value) > _OMP_TARGET_MAX_CHARS
    ):
        raise _TurnReadFailed
    codex_resolution: _CodexPathResolution | None = None
    codex_cache_key: tuple[str, str] | None = None
    codex_prior: _CodexSessionState | None = None
    omp_cache_key: str | None = None
    omp_prior: _OmpSessionState | None = None
    prior_state_value: dict[str, Any] | None = None
    binding_generation: int | None = None
    parser_state: dict[str, Any]
    if target_kind == _CODEX_SESSION_TURN_KIND:
        canonical_id = _canonical_codex_session_id(target_value)
        if canonical_id is None:
            return None
        codex_resolution = _resolve_codex_session(canonical_id)
        if codex_resolution is None or codex_resolution.status != "found":
            return None
        codex_cache_key = (codex_resolution.root, canonical_id)
        with _CODEX_SESSION_CACHE_LOCK:
            codex_prior = _codex_cache_get_locked(codex_cache_key)
            if (
                codex_prior is not None
                and (
                    codex_prior.resolver_generation != codex_resolution.generation
                    or codex_prior.root_file_id != codex_resolution.root_file_id
                    or codex_prior.canonical_path != codex_resolution.canonical_path
                    or codex_prior.file_id != codex_resolution.file_id
                )
            ):
                _CODEX_SESSION_CACHE.pop(codex_cache_key, None)
                codex_prior = None
            prior_state_value = _serialize_codex_state(codex_prior)
            binding_generation = _codex_cache_binding_generation_locked(
                codex_cache_key
            )
        if (
            codex_prior is not None
            and _codex_checkpoint_matches_stat(codex_prior, codex_resolution)
        ):
            _check_ipc_deadline(deadline, cancel_event)
            observer = _CODEX_ISOLATED_READ_OBSERVER
            if observer is not None:
                observer(0)
            return _UNCHANGED_TURN
        supplied = codex_prior or _CodexSessionState(
            resolver_generation=codex_resolution.generation,
            root=codex_resolution.root,
            root_file_id=codex_resolution.root_file_id,
            session_id=canonical_id,
            canonical_path=codex_resolution.canonical_path or "",
            file_id=None,
            observed_size=0,
            mtime_ns=0,
            ctime_ns=0,
            committed_offset=0,
            partial_record=b"",
            active_turn_id="",
            last_content_turn_id="",
            turn_open=False,
            final_seen=False,
            complete=False,
            stream_spans=(),
        )
        parser_state = {
            "source": "codex",
            "state": _serialize_codex_state(supplied),
        }
    elif target_kind == _OMP_SESSION_TURN_KIND:
        omp_cache_key = _omp_cache_key(target_value)
        if omp_cache_key is not None:
            with _OMP_SESSION_CACHE_LOCK:
                omp_prior = _omp_cache_get_locked(omp_cache_key)
                prior_state_value = _serialize_omp_state(omp_prior)
                binding_generation = _omp_cache_binding_generation_locked(
                    omp_cache_key
                )
            try:
                unchanged = (
                    omp_prior is not None
                    and _omp_checkpoint_matches_stat(
                        omp_prior,
                        Path(omp_cache_key).stat(),
                    )
                )
            except OSError as exc:
                raise _TurnReadFailed from exc
            if unchanged:
                _check_ipc_deadline(deadline, cancel_event)
                observer = _OMP_ISOLATED_READ_OBSERVER
                if observer is not None:
                    observer(0)
                return _UNCHANGED_TURN
        parser_state = {"source": "omp", "state": prior_state_value}
    else:
        raise _TurnReadFailed
    nonce = secrets.token_urlsafe(32)
    request = {
        "protocol": 1,
        "nonce": nonce,
        "target_kind": target_kind,
        "target_value": target_value,
        "parser_state": parser_state,
    }
    request_payload = json.dumps(
        request,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    maximum_request = (
        _OMP_REQUEST_MAX_BYTES
        if target_kind == _OMP_SESSION_TURN_KIND
        else _CODEX_STATE_IPC_MAX_BYTES
    )
    if len(request_payload) > maximum_request:
        raise _TurnReadFailed
    _check_ipc_deadline(deadline, cancel_event)
    context = multiprocessing.get_context("spawn")
    parent_channel, child_channel = socket.socketpair()
    process: multiprocessing.Process | None = None
    try:
        parent_channel.setblocking(False)
        process = context.Process(
            target=_file_turn_child,
            args=(child_channel,),
            name="tendwire-turn-reader",
        )
        process.start()
        child_channel.close()
        _check_ipc_deadline(deadline, cancel_event)
        _send_frame_until(parent_channel, request_payload, deadline, cancel_event)
        response_payload = _recv_frame_until(
            parent_channel,
            deadline,
            cancel_event,
            (
                _OMP_IPC_RESPONSE_CHUNK_BYTES
                if target_kind == _OMP_SESSION_TURN_KIND
                else _CODEX_IPC_FRAME_MAX_BYTES
            ),
        )
        response_payload = _recv_streamed_omp_response_until(
            parent_channel,
            response_payload,
            nonce,
            target_kind,
            deadline,
            cancel_event,
        )
        _check_ipc_deadline(deadline, cancel_event)
        response = json.loads(response_payload.decode("utf-8"))
        _check_ipc_deadline(deadline, cancel_event)
        process.join(max(0.0, deadline - time.monotonic()))
        if process.is_alive():
            raise _TurnReadTimeout
        if not isinstance(response, Mapping) or set(response) != {
            "protocol",
            "nonce",
            "disposition",
            "content",
            "parser_state",
            "bytes_read",
        }:
            raise _TurnReadFailed
        response_nonce = response["nonce"]
        disposition = response["disposition"]
        content = response["content"]
        bytes_read = response["bytes_read"]
        response_state = response["parser_state"]
        if (
            response["protocol"] != 1
            or type(response_nonce) is not str
            or not secrets.compare_digest(response_nonce, nonce)
            or disposition not in {"ok", "missing"}
            or type(bytes_read) is not int
            or bytes_read < 0
            or (
                target_kind == _CODEX_SESSION_TURN_KIND
                and bytes_read > _CODEX_POLL_MAX_BYTES
            )
        ):
            raise _TurnReadFailed
        if disposition == "missing":
            if content is not None or response_state is not None or bytes_read != 0:
                raise _TurnReadFailed
            return None
        if content is not None and not _isolated_content_is_valid(content):
            raise _TurnReadFailed
        if (
            not isinstance(response_state, Mapping)
            or set(response_state) != {"source", "state"}
        ):
            raise _TurnReadFailed
        if target_kind == _OMP_SESSION_TURN_KIND:
            if response_state["source"] != "omp" or omp_cache_key is None:
                raise _TurnReadFailed
            updated_omp = _deserialize_omp_state(response_state["state"])
            if updated_omp is None:
                raise _TurnReadFailed
            observer = _OMP_ISOLATED_READ_OBSERVER
            if observer is not None:
                observer(bytes_read)
            publication: _OmpCachePublication | _CodexCachePublication = (
                _OmpCachePublication(
                    omp_cache_key,
                    prior_state_value,
                    updated_omp,
                    binding_generation,
                )
            )
        else:
            if (
                response_state["source"] != "codex"
                or codex_cache_key is None
                or codex_resolution is None
            ):
                raise _TurnReadFailed
            updated_codex = _deserialize_codex_state(response_state["state"])
            if (
                updated_codex is None
                or updated_codex.root != codex_resolution.root
                or updated_codex.root_file_id != codex_resolution.root_file_id
                or updated_codex.session_id != codex_resolution.session_id
                or updated_codex.canonical_path
                != codex_resolution.canonical_path
                or updated_codex.resolver_generation
                != codex_resolution.generation
            ):
                raise _TurnReadFailed
            observer = _CODEX_ISOLATED_READ_OBSERVER
            if observer is not None:
                observer(bytes_read)
            publication = _CodexCachePublication(
                codex_cache_key,
                prior_state_value,
                updated_codex,
                binding_generation,
                codex_resolution.generation,
            )
        if defer_cache:
            return _ObservedFileTurn(content, publication)
        return _file_publication_commit(publication, content)
    except _TurnReadTimeout:
        raise
    except (
        OSError,
        EOFError,
        BrokenPipeError,
        UnicodeError,
        ValueError,
        json.JSONDecodeError,
    ):
        raise _TurnReadFailed from None
    finally:
        parent_channel.close()
        child_channel.close()
        if process is not None and process.pid is not None:
            _terminate_and_reap(process)


def _read_turn_for_binding(
    config: Config,
    binding: WorkerBinding,
    *,
    timeout_seconds: float | None = None,
    cancel_event: threading.Event | None = None,
) -> Mapping[str, Any] | _ObservedFileTurn | object | None:
    target_kind = str(binding.turn_target_kind or "")
    target_value = str(binding.turn_target_value or "")
    if not target_value:
        return None
    deadline = config.herdr_timeout_seconds if timeout_seconds is None else float(timeout_seconds)
    if target_kind in {_CODEX_SESSION_TURN_KIND, _OMP_SESSION_TURN_KIND}:
        return _read_file_turn_isolated(
            target_kind,
            target_value,
            timeout_seconds=deadline,
            cancel_event=cancel_event,
            defer_cache=True,
        )
    if target_kind == _PANE_TURN_KIND:
        return _read_private_turn(
            config,
            target_value,
            timeout_seconds=deadline,
            raise_timeout=True,
            cancel_event=cancel_event,
        )
    return None


def _binding_still_matches(
    config: Config,
    item: _TurnRefreshItem,
) -> bool:
    if config.db_path is None:
        return False
    try:
        current = list_worker_bindings(config.db_path, config.host_id, backend="herdr")
    except Exception:
        return False
    return any(
        binding.private_fingerprint == item.key.private_fingerprint
        and binding.worker_id == item.worker_id
        and binding.worker_fingerprint == item.worker_fingerprint
        and str(binding.turn_target_kind or "") == item.turn_target_kind
        and str(binding.turn_target_value or "") == item.turn_target_value
        for binding in current
    )


def _refresh_turn_binding(
    config: Config,
    binding: WorkerBinding,
    *,
    pane_target_override: str | None = None,
    adapter_timeout_seconds: float | None = None,
    cancel_event: threading.Event | None = None,
    apply_deadline_monotonic: float | None = None,
    observed_at: str | None = None,
) -> TurnRefreshResult:
    """Read and atomically apply exactly one immutable private binding.

    Completion-authoritative callers may read through the live pane while the
    immutable stored binding remains the ownership and transaction guard. This
    prevents a prior agent-session generation from choosing the content source
    after Herdr has reported completion for the current pane.
    """
    if config.db_path is None or not _eligible_turn_binding(binding):
        return TurnRefreshResult("missing", 0)
    timeout_seconds = (
        config.herdr_timeout_seconds
        if adapter_timeout_seconds is None
        else float(adapter_timeout_seconds)
    )
    if timeout_seconds <= 0:
        return TurnRefreshResult("failed", 0)
    item = _TurnRefreshItem.from_binding(binding)
    read_binding = binding
    if pane_target_override is not None:
        live_pane_id = str(pane_target_override).strip()
        if not live_pane_id:
            return TurnRefreshResult("missing", 0)
        read_binding = replace(
            binding,
            turn_target_kind=_PANE_TURN_KIND,
            turn_target_value=live_pane_id,
        )
    read_target_kind = str(read_binding.turn_target_kind or "")
    current_time = observed_at or utc_timestamp()
    grace_seconds = float(config.pending_stale_grace_seconds)
    def failed_pending_result(status: Literal["failed", "timeout"]) -> TurnRefreshResult:
        if read_target_kind != _PANE_TURN_KIND:
            return TurnRefreshResult(status, 0)
        if not _binding_still_matches(config, item):
            return TurnRefreshResult("stale_binding", 0)
        try:
            applied_failure = apply_turn_refresh(
                config.db_path,
                config.host_id,
                item.worker_id,
                {},
                backend_pending_observation=PendingObservation("read_failed"),
                expected_binding=binding,
                deadline_monotonic=apply_deadline_monotonic,
                cancelled=cancel_event.is_set if cancel_event is not None else None,
                observed_at=current_time,
                pending_stale_grace_seconds=grace_seconds,
                turn_model=config.turn_model,
            )
        except Exception:
            return TurnRefreshResult(status, 0)
        if applied_failure.stale_binding:
            return TurnRefreshResult("stale_binding", 0)
        return TurnRefreshResult(
            status,
            0,
            pending_changed=applied_failure.pending_changed,
        )
    try:
        observed = _read_turn_for_binding(
            config,
            read_binding,
            timeout_seconds=timeout_seconds,
            cancel_event=cancel_event,
        )
    except _TurnReadTimeout:
        return failed_pending_result("timeout")
    except Exception:
        return failed_pending_result("failed")
    if observed is _UNCHANGED_TURN:
        return TurnRefreshResult("unchanged", 0)
    publication: _OmpCachePublication | _CodexCachePublication | None = None
    if isinstance(observed, _ObservedFileTurn):
        publication = observed.publication
        observed = observed.content
        if observed is None:
            if not _binding_still_matches(config, item):
                return TurnRefreshResult("stale_binding", 0)
            _file_publication_commit(publication, None)
            return TurnRefreshResult("unchanged", 0)
    if observed is None:
        return TurnRefreshResult("missing", 0)
    if read_target_kind == _PANE_TURN_KIND:
        content, pending_observation = _pop_backend_pending_observation(observed)
    else:
        content, pending_observation = dict(observed), None
    if not _binding_still_matches(config, item):
        return TurnRefreshResult("stale_binding", 0)
    try:
        if read_target_kind == _PANE_TURN_KIND:
            applied = apply_turn_refresh(
                config.db_path,
                config.host_id,
                item.worker_id,
                content or {},
                backend_pending_observation=(
                    pending_observation
                    or PendingObservation("read_succeeded_no_prompt")
                ),
                expected_binding=binding,
                deadline_monotonic=apply_deadline_monotonic,
                cancelled=cancel_event.is_set if cancel_event is not None else None,
                observed_at=current_time,
                pending_stale_grace_seconds=grace_seconds,
                turn_model=config.turn_model,
            )
        elif content is not None:
            applied = apply_turn_refresh(
                config.db_path,
                config.host_id,
                item.worker_id,
                content,
                expected_binding=binding,
                deadline_monotonic=apply_deadline_monotonic,
                cancelled=cancel_event.is_set if cancel_event is not None else None,
                observed_at=current_time,
                turn_model=config.turn_model,
            )
        else:
            return TurnRefreshResult("missing", 0)
    except Exception:
        return TurnRefreshResult("failed", 0)
    if applied.cancelled:
        return TurnRefreshResult("timeout", 0)
    if applied.stale_binding:
        return TurnRefreshResult("stale_binding", 0)
    if (
        pending_observation is not None
        and pending_observation.kind == "read_failed"
    ):
        return TurnRefreshResult(
            "failed",
            int(applied.updated),
            bool(applied.pending_changed),
        )
    if publication is not None:
        _file_publication_commit(publication, content)
    updated = int(applied.updated)
    return TurnRefreshResult(
        "updated" if updated else "unchanged",
        updated,
        bool(applied.pending_changed),
    )


def refresh_turn_binding(
    config: Config,
    binding: WorkerBinding,
    *,
    adapter_timeout_seconds: float | None = None,
) -> TurnRefreshResult:
    return _refresh_turn_binding(
        config,
        binding,
        adapter_timeout_seconds=adapter_timeout_seconds,
    )


def refresh_completed_pane_turn(
    config: Config,
    pane_id: str,
    *,
    terminal_id: str | None = None,
    binding_private_fingerprint: str | None = None,
    adapter_timeout_seconds: float | None = None,
) -> CompletedPaneTurnRefreshResult:
    """Capture one completed turn through its current pane adapter target."""
    if config.db_path is None:
        return CompletedPaneTurnRefreshResult("store_unavailable")
    try:
        bindings = list_worker_bindings(
            config.db_path,
            config.host_id,
            backend="herdr",
        )
    except Exception:
        return CompletedPaneTurnRefreshResult("store_unavailable")
    eligible = [binding for binding in bindings if _eligible_turn_binding(binding)]
    candidate_tiers: list[list[WorkerBinding]] = []
    if binding_private_fingerprint is not None:
        candidate_tiers.append(
            [
                binding
                for binding in eligible
                if binding.private_fingerprint == binding_private_fingerprint
            ]
        )
    candidate_tiers.append(
        [
            binding
            for binding in eligible
            if binding.turn_target_kind == _PANE_TURN_KIND
            and str(binding.turn_target_value or "") == str(pane_id)
        ]
    )
    if terminal_id is not None:
        candidate_tiers.append(
            [
                binding
                for binding in eligible
                if str(binding.target_kind or "") == "terminal_id"
                and str(binding.target_value or "") == str(terminal_id)
            ]
        )
    binding = None
    for candidates in candidate_tiers:
        if len(candidates) > 1:
            return CompletedPaneTurnRefreshResult("binding_ambiguous")
        if candidates:
            binding = candidates[0]
            break
    if binding is None:
        return CompletedPaneTurnRefreshResult("binding_missing")
    refreshed = _refresh_turn_binding(
        config,
        binding,
        pane_target_override=pane_id,
        adapter_timeout_seconds=adapter_timeout_seconds,
    )
    if refreshed.status not in {"updated", "unchanged", "missing"}:
        return CompletedPaneTurnRefreshResult(
            refreshed.status,
            worker_id=binding.worker_id,
        )
    try:
        turn_id = latest_turn_id_for_worker(
            config.db_path,
            config.host_id,
            binding.worker_id,
        )
    except Exception:
        return CompletedPaneTurnRefreshResult(
            "store_unavailable",
            worker_id=binding.worker_id,
        )
    return CompletedPaneTurnRefreshResult(
        refreshed.status,
        worker_id=binding.worker_id,
        refreshed_turn_id=turn_id,
    )


def refresh_structured_turn_content(
    config: Config,
    *,
    adapter_timeout_seconds: float | None = None,
    max_workers: int | None = None,
    total_timeout_seconds: float | None = None,
) -> dict[str, Any]:
    """Run one bounded unavailable-only fallback scan with active feeding."""
    if config.db_path is None:
        return {"ok": False, "status": "store_unavailable", "updated": 0, "attempted": 0}
    try:
        bindings = list_worker_bindings(config.db_path, config.host_id, backend="herdr")
    except Exception:
        return {"ok": False, "status": "store_unavailable", "updated": 0, "attempted": 0}
    _prune_omp_cache_for_bindings(bindings)
    _prune_codex_cache_for_bindings(bindings)
    turn_bindings = [binding for binding in bindings if _eligible_turn_binding(binding)]
    worker_limit = max(
        1,
        min(
            32,
            int(
                max_workers
                if max_workers is not None
                else getattr(config, "turn_refresh_workers", 4)
            ),
        ),
    )
    adapter_deadline = (
        config.herdr_timeout_seconds
        if adapter_timeout_seconds is None
        else float(adapter_timeout_seconds)
    )
    waves = (len(turn_bindings) + worker_limit - 1) // worker_limit
    total_deadline = (
        max(1.0, (waves * adapter_deadline) + 1.0)
        if total_timeout_seconds is None
        else max(0.0, float(total_timeout_seconds))
    )
    deadline = time.monotonic() + total_deadline
    shutdown_reserve = min(0.75, total_deadline / 2.0)
    work_deadline = deadline - shutdown_reserve
    pending_bindings = deque(turn_bindings)
    active: dict[Future[TurnRefreshResult], WorkerBinding] = {}
    submitted: list[Future[TurnRefreshResult]] = []
    updated = 0
    deadline_reached = False
    pool = ThreadPoolExecutor(
        max_workers=worker_limit,
        thread_name_prefix="tendwire-turn-fallback",
    )
    def fallback_cancelled() -> bool:
        return time.monotonic() >= deadline


    def collect(done: set[Future[TurnRefreshResult]]) -> None:
        nonlocal updated
        for future in done:
            active.pop(future, None)
            try:
                updated += future.result().updated
            except Exception:
                pass

    while pending_bindings or active:
        while (
            pending_bindings
            and len(active) < worker_limit
            and time.monotonic() < work_deadline
        ):
            binding = pending_bindings.popleft()
            remaining_for_binding = max(
                0.001,
                min(adapter_deadline, work_deadline - time.monotonic()),
            )
            future = pool.submit(
                _refresh_turn_binding,
                config,
                binding,
                adapter_timeout_seconds=remaining_for_binding,
                apply_deadline_monotonic=work_deadline,
            )
            active[future] = binding
            submitted.append(future)
        if not active:
            deadline_reached = bool(pending_bindings)
            break
        remaining = max(0.0, work_deadline - time.monotonic())
        done, _ = wait(active, timeout=remaining, return_when=FIRST_COMPLETED)
        if not done:
            deadline_reached = True
            break
        collect(done)
        if time.monotonic() >= work_deadline and (pending_bindings or active):
            deadline_reached = True
            break
    if deadline_reached:
        for future in active:
            future.cancel()
        remaining = max(0.0, deadline - time.monotonic())
        done, _ = wait(active, timeout=remaining)
        collect(done)
    # Supported readers terminate and reap at their per-binding deadlines
    # inside the reserved shutdown window. Never use an executor context or
    # wait=True: the absolute fallback deadline owns return latency.
    pool.shutdown(wait=False, cancel_futures=True)
    if not deadline_reached:
        try:
            prune_backend_pending(
                config.db_path,
                config.host_id,
                {
                    binding.private_fingerprint
                    for binding in bindings
                    if binding.turn_target_kind == _PANE_TURN_KIND
                },
                deadline_monotonic=deadline,
                cancelled=fallback_cancelled,
                observed_at=utc_timestamp(),
            )
        except Exception:
            pass
        deadline_reached = fallback_cancelled()
    return {
        "ok": not deadline_reached,
        "status": "deadline_exceeded" if deadline_reached else "ok",
        "updated": updated,
        "attempted": len(submitted),
    }


class TurnIngestionScheduler:
    """One bounded coordinator and fixed worker pool for turn ingestion."""

    def __init__(
        self,
        config: Config,
        *,
        refresh_interval_seconds: float | None = None,
        max_workers: int | None = None,
        queue_capacity: int = _TURN_INGESTION_QUEUE_CAPACITY,
        adapter_timeout_seconds: float | None = None,
        clock: Callable[[], float] = time.monotonic,
        utc_clock: Callable[[], str] = utc_timestamp,
        reader: Callable[..., TurnRefreshResult] = refresh_turn_binding,
    ) -> None:
        self.config = config
        self.refresh_interval_seconds = float(
            refresh_interval_seconds
            if refresh_interval_seconds is not None
            else getattr(config, "turn_refresh_interval_seconds", 2.0)
        )
        self.max_workers = int(
            max_workers
            if max_workers is not None
            else getattr(config, "turn_refresh_workers", 4)
        )
        self.queue_capacity = int(queue_capacity)
        self.adapter_timeout_seconds = float(
            config.herdr_timeout_seconds
            if adapter_timeout_seconds is None
            else adapter_timeout_seconds
        )
        if self.refresh_interval_seconds <= 0:
            raise ValueError("refresh_interval_seconds must be positive")
        if not 1 <= self.max_workers <= 32:
            raise ValueError("max_workers must be between 1 and 32")
        if self.queue_capacity <= 0:
            raise ValueError("queue_capacity must be positive")
        if self.adapter_timeout_seconds <= 0:
            raise ValueError("adapter_timeout_seconds must be positive")
        self._clock = clock
        self._utc_clock = utc_clock
        self._reader = reader
        self._uses_default_reader = reader is refresh_turn_binding
        self._cancel_event = threading.Event()
        self._condition = threading.Condition(threading.RLock())
        self._queue: deque[_TurnRefreshItem] = deque()
        self._queued: set[TurnRefreshKey] = set()
        self._running: dict[TurnRefreshKey, tuple[_TurnRefreshItem, Future[TurnRefreshResult], float]] = {}
        self._dirty: set[TurnRefreshKey] = set()
        self._latest: dict[TurnRefreshKey, _TurnRefreshItem] = {}
        self._deferred_reruns: dict[TurnRefreshKey, _TurnRefreshItem] = {}
        self._executor: ThreadPoolExecutor | None = None
        self._coordinator: threading.Thread | None = None
        self._accepting = False
        self._started = False
        self._force_exit = False
        self._rescan_requested = False
        self._next_scan = 0.0
        self._scan_cursor = 0
        self._stopping = False
        self._refreshed = 0
        self._failed = 0
        self._timed_out = 0
        self._coalesced = 0
        self._queue_full = 0
        self._last_success: str | None = None
        self._last_success_clock: float | None = None
        self._last_duration_ms: float | None = None
        self._last_completed_outcome: str | None = None
        self._consecutive_failures = 0
        self._scan_failed = False
        self._scan_retry_remaining = 1
        self._binding_retry_remaining: dict[TurnRefreshKey, int] = {}
        self._binding_retry_due: dict[
            TurnRefreshKey,
            tuple[_TurnRefreshItem, float],
        ] = {}

    def start(self) -> None:
        with self._condition:
            if self._started:
                return
            if self._stopping:
                raise RuntimeError("turn ingestion scheduler cannot restart")
            self._executor = ThreadPoolExecutor(
                max_workers=self.max_workers,
                thread_name_prefix="tendwire-turn-ingestion",
            )
            self._accepting = True
            self._started = True
            self._rescan_requested = True
            self._next_scan = self._clock()
            self._coordinator = threading.Thread(
                target=self._coordinate,
                name="tendwire-turn-coordinator",
                daemon=False,
            )
            self._coordinator.start()

    def request_refresh(self) -> None:
        with self._condition:
            if not self._accepting:
                return
            if self._rescan_requested:
                self._coalesced += 1
            self._rescan_requested = True
            self._condition.notify()

    def _enqueue_locked(self, item: _TurnRefreshItem) -> None:
        key = item.key
        self._latest[key] = item
        if key in self._running:
            if key in self._dirty:
                self._coalesced += 1
            else:
                self._dirty.add(key)
            return
        if key in self._binding_retry_due:
            _previous, due = self._binding_retry_due[key]
            self._binding_retry_due[key] = (item, due)
            self._coalesced += 1
            return
        if key in self._deferred_reruns:
            self._deferred_reruns[key] = item
            self._coalesced += 1
            return
        if key in self._queued:
            self._coalesced += 1
            return
        if len(self._queue) >= self.queue_capacity:
            self._queue_full += 1
            self._latest.pop(key, None)
            return
        self._queue.append(item)
        self._queued.add(key)

    def _scan_bindings(self) -> None:
        if self.config.db_path is None:
            with self._condition:
                self._failed += 1
                self._scan_failed = True
            return
        try:
            bindings = list_worker_bindings(
                self.config.db_path,
                self.config.host_id,
                backend="herdr",
            )
        except Exception:
            with self._condition:
                self._failed += 1
                self._scan_failed = True
                if self._accepting and self._scan_retry_remaining > 0:
                    self._scan_retry_remaining -= 1
                    self._next_scan = min(
                        self._next_scan,
                        self._clock() + 0.05,
                    )
                    self._condition.notify()
            return
        _prune_omp_cache_for_bindings(bindings)
        _prune_codex_cache_for_bindings(bindings)
        items = [
            _TurnRefreshItem.from_binding(binding)
            for binding in bindings
            if _eligible_turn_binding(binding)
        ]
        with self._condition:
            self._scan_failed = False
            self._scan_retry_remaining = 1
            live_keys = {item.key for item in items}
            for key in tuple(self._binding_retry_remaining):
                if key not in live_keys:
                    self._binding_retry_remaining.pop(key, None)
                    self._binding_retry_due.pop(key, None)
            if self._accepting:
                if items:
                    offset = self._scan_cursor % len(items)
                    items = items[offset:] + items[:offset]
                    # Rotate each full scan so bindings dropped at the explicit
                    # queue cap are first on a later cadence instead of starving.
                    self._scan_cursor = (offset + self.queue_capacity) % len(items)
                for item in items:
                    self._enqueue_locked(item)
                self._dispatch_locked()
                self._condition.notify()
        # Lock order: no scheduler condition is held during store reads/writes,
        # adapter/process waits, SQLite transactions, or future waits.
        try:
            prune_backend_pending(
                self.config.db_path,
                self.config.host_id,
                {
                    binding.private_fingerprint
                    for binding in bindings
                    if binding.turn_target_kind == _PANE_TURN_KIND
                },
                cancelled=self._cancel_event.is_set,
                observed_at=self._utc_clock(),
            )
        except Exception:
            pass

    def _binding_for_item(self, item: _TurnRefreshItem) -> WorkerBinding | None:
        if self.config.db_path is None:
            return None
        try:
            bindings = list_worker_bindings(
                self.config.db_path,
                self.config.host_id,
                backend="herdr",
            )
        except Exception as exc:
            raise _BindingLookupFailed from exc
        return next(
            (
                binding
                for binding in bindings
                if binding.private_fingerprint == item.key.private_fingerprint
                and binding.worker_id == item.worker_id
                and binding.worker_fingerprint == item.worker_fingerprint
                and str(binding.turn_target_kind or "") == item.turn_target_kind
                and str(binding.turn_target_value or "") == item.turn_target_value
            ),
            None,
        )

    def _run_item(self, item: _TurnRefreshItem) -> TurnRefreshResult:
        try:
            binding = self._binding_for_item(item)
        except _BindingLookupFailed:
            return TurnRefreshResult("failed", 0, retry_binding_lookup=True)
        if binding is None:
            return TurnRefreshResult("stale_binding", 0)
        try:
            if self._uses_default_reader:
                result = _refresh_turn_binding(
                    self.config,
                    binding,
                    adapter_timeout_seconds=self.adapter_timeout_seconds,
                    cancel_event=self._cancel_event,
                    observed_at=self._utc_clock(),
                )
            else:
                result = self._reader(
                    self.config,
                    binding,
                    adapter_timeout_seconds=self.adapter_timeout_seconds,
                )
        except Exception:
            result = TurnRefreshResult("failed", 0)
        return TurnRefreshResult(
            result.status,
            result.updated,
            result.pending_changed,
            binding_validated=True,
        )

    def _future_finished(self, _future: Future[TurnRefreshResult]) -> None:
        with self._condition:
            self._condition.notify()

    def _collect_finished_locked(self) -> None:
        now = self._clock()
        for key, (item, future, started_at) in list(self._running.items()):
            if not future.done():
                continue
            self._running.pop(key, None)
            self._last_duration_ms = max(0.0, (now - started_at) * 1000.0)
            try:
                result = future.result()
            except Exception:
                result = TurnRefreshResult("failed", 0)
            self._last_completed_outcome = result.status
            if result.binding_validated or result.status == "stale_binding":
                self._binding_retry_remaining.pop(key, None)
                self._binding_retry_due.pop(key, None)
            if result.status == "timeout":
                self._timed_out += 1
                self._consecutive_failures += 1
            elif result.status == "failed":
                self._failed += 1
                self._consecutive_failures += 1
            elif result.status == "stale_binding":
                self._failed += 1
            else:
                self._refreshed += 1
                self._last_success = utc_timestamp()
                self._last_success_clock = now
                self._consecutive_failures = 0
            if result.retry_binding_lookup:
                retry_item = self._latest.get(key, item)
                self._dirty.discard(key)
                self._latest.pop(key, None)
                remaining = self._binding_retry_remaining.get(key, 1)
                if self._accepting and remaining > 0:
                    self._binding_retry_remaining[key] = remaining - 1
                    self._binding_retry_due[key] = (retry_item, now + 0.05)
                continue
            if key in self._dirty:
                self._dirty.discard(key)
                rerun = self._latest.get(key, item)
                if self._accepting and len(self._queue) < self.queue_capacity:
                    self._queue.append(rerun)
                    self._queued.add(key)
                elif self._accepting:
                    self._deferred_reruns[key] = rerun
                    self._queue_full += 1
                else:
                    self._latest.pop(key, None)
            else:
                self._latest.pop(key, None)

    def _promote_deferred_locked(self) -> None:
        while (
            self._accepting
            and self._deferred_reruns
            and len(self._queue) < self.queue_capacity
        ):
            key, item = next(iter(self._deferred_reruns.items()))
            del self._deferred_reruns[key]
            self._queue.append(item)
            self._queued.add(key)

    def _promote_binding_retries_locked(self, now: float) -> None:
        for key, (item, due) in tuple(self._binding_retry_due.items()):
            if due > now:
                continue
            self._binding_retry_due.pop(key, None)
            self._enqueue_locked(item)


    def _dispatch_locked(self) -> None:
        executor = self._executor
        if executor is None:
            return
        self._promote_deferred_locked()
        self._promote_binding_retries_locked(self._clock())
        while self._accepting and self._queue and len(self._running) < self.max_workers:
            item = self._queue.popleft()
            self._queued.discard(item.key)
            self._binding_retry_due.pop(item.key, None)
            self._promote_deferred_locked()
            future = executor.submit(self._run_item, item)
            self._running[item.key] = (item, future, self._clock())
            future.add_done_callback(self._future_finished)

    def _coordinate(self) -> None:
        while True:
            run_scan = False
            with self._condition:
                self._collect_finished_locked()
                if self._force_exit:
                    return
                if not self._accepting and not self._running:
                    return
                now = self._clock()
                self._promote_binding_retries_locked(now)
                if self._accepting and (
                    self._rescan_requested or now >= self._next_scan
                ):
                    run_scan = True
                    self._rescan_requested = False
                    self._next_scan = now + self.refresh_interval_seconds
                self._dispatch_locked()
                if not run_scan:
                    timeout: float | None = None
                    if self._accepting:
                        timeout = max(0.0, self._next_scan - self._clock())
                        if self._binding_retry_due:
                            retry_timeout = max(
                                0.0,
                                min(due for _item, due in self._binding_retry_due.values())
                                - self._clock(),
                            )
                            timeout = min(timeout, retry_timeout)
                    self._condition.wait(timeout)
                    continue
            self._scan_bindings()

    def stop(self, *, flush_timeout_seconds: float | None = None) -> None:
        timeout = (
            self.adapter_timeout_seconds + 1.0
            if flush_timeout_seconds is None
            else max(0.0, float(flush_timeout_seconds))
        )
        with self._condition:
            if self._stopping:
                return
            self._stopping = True
            self._accepting = False
            self._rescan_requested = False
            self._queue.clear()
            self._queued.clear()
            self._dirty.clear()
            self._deferred_reruns.clear()
            self._latest.clear()
            self._binding_retry_due.clear()
            self._binding_retry_remaining.clear()
            self._cancel_event.set()
            self._condition.notify_all()
            coordinator = self._coordinator
            executor = self._executor
        coordinator_clean = True
        if coordinator is not None:
            coordinator.join(timeout)
            if coordinator.is_alive():
                coordinator_clean = False
                with self._condition:
                    self._force_exit = True
                    self._condition.notify_all()
                coordinator.join(0.25)
                coordinator_clean = not coordinator.is_alive()
        if executor is not None:
            executor.shutdown(wait=coordinator_clean, cancel_futures=True)

    def operational_status(self) -> Mapping[str, Any]:
        with self._condition:
            now = self._clock()
            stale_age = (
                None
                if self._last_success_clock is None
                else max(0.0, now - self._last_success_clock)
            )
            if self._stopping:
                status = "stopping"
            elif self._scan_failed or self._consecutive_failures > 0:
                status = "degraded"
            elif self._last_success_clock is None:
                status = "stale"
            elif stale_age is not None and stale_age > max(
                self.refresh_interval_seconds * 3.0,
                self.adapter_timeout_seconds * 2.0,
            ):
                status = "stale"
            else:
                status = "healthy"
            return {
                "status": status,
                "queue_depth": len(self._queue),
                "active": len(self._running),
                "refreshed": self._refreshed,
                "failed": self._failed,
                "timed_out": self._timed_out,
                "coalesced": self._coalesced,
                "queue_full": self._queue_full,
                "last_success": self._last_success,
                "last_duration_ms": self._last_duration_ms,
                "stale_age_seconds": stale_age,
                "max_workers": self.max_workers,
                "queue_capacity": self.queue_capacity,
                "refresh_interval_seconds": self.refresh_interval_seconds,
                "adapter_timeout_seconds": self.adapter_timeout_seconds,
            }
