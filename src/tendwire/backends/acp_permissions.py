"""Durable, privacy-preserving bridge for ACP permission decisions."""

from __future__ import annotations

import secrets
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..config import Config
from ..core.models import WorkerBinding, sanitize_public_text, stable_fingerprint
from ..core.turns import PendingObservation, PendingObservedChoice
from ..store.pending import apply_backend_pending_observation
from ..store.projection import list_worker_bindings
from .acp_protocol import PermissionRequest
from .acp_runtime import PermissionSelection


class AcpPermissionBrokerError(RuntimeError):
    """A permission could not be safely correlated or acknowledged."""


_MAX_PERMISSION_OPTIONS = 64


@dataclass(slots=True)
class _Offer:
    decision_ref: str
    binding: WorkerBinding
    option_ids: tuple[str, ...]
    selected: int | None = None
    response_state: str = "pending"
    answer_abandoned: bool = False


class AcpPermissionBroker:
    """One bounded permission rendezvous for one worker generation."""

    def __init__(
        self,
        config: Config,
        *,
        worker_id: str,
        worker_fingerprint: str,
        generation: str,
        timeout: float,
    ) -> None:
        if timeout <= 0:
            raise ValueError("ACP permission broker timeout must be positive")
        self.config = config
        self.worker_id = worker_id
        self.worker_fingerprint = worker_fingerprint
        self.generation = generation
        self.timeout = float(timeout)
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._offer: _Offer | None = None
        self._closed = False

    def __call__(self, request: PermissionRequest) -> PermissionSelection | None:
        if not request.options or len(request.options) > _MAX_PERMISSION_OPTIONS:
            return None
        binding = self._exact_binding(request.session_id)
        title = _tool_title(request.tool_call)
        labels = tuple(
            _option_label(option.name, option.kind)
            for option in request.options
        )
        if not labels:
            return None
        nonce = secrets.token_urlsafe(24)
        source_revision = stable_fingerprint(
            {
                "route": "acp_permission_v1",
                "worker_id": self.worker_id,
                "worker_fingerprint": self.worker_fingerprint,
                "binding": binding.private_fingerprint,
                "session": binding.turn_target_value,
                "generation": self.generation,
                "nonce": nonce,
            }
        )
        persisted_revision = stable_fingerprint(
            {
                "decision_revision": source_revision,
                "binding_private_fingerprint": binding.private_fingerprint,
                "observed_turn_target_value": binding.turn_target_value,
            }
        )
        offer = _Offer(
            decision_ref=f"decision-{persisted_revision}",
            binding=binding,
            option_ids=tuple(option.option_id for option in request.options),
        )
        observation = PendingObservation(
            "open_prompt",
            question=title,
            pending_kind="approval",
            choices=tuple(
                PendingObservedChoice(
                    choice_id="choice-"
                    + stable_fingerprint(
                        {
                            "revision": source_revision,
                            "ordinal": index,
                            "label": label,
                        }
                    ),
                    label=label,
                    picker_ordinal=index,
                )
                for index, label in enumerate(labels, 1)
            ),
            revision_digest=source_revision,
            decision_kind="single",
            decision_options=labels,
            decision_multi_select=False,
            decision_question_count=1,
        )
        try:
            with self._lock:
                if self._closed or self._offer is not None:
                    return None
                self._offer = offer
                changed = apply_backend_pending_observation(
                    Path(self.config.db_path),
                    self.config.host_id,
                    self.worker_id,
                    observation,
                    binding_private_fingerprint=binding.private_fingerprint,
                    observed_turn_target_value=binding.turn_target_value,
                    binding_authoritative=True,
                )
            if not changed:
                raise AcpPermissionBrokerError("permission overlay was not published")
            deadline = time.monotonic() + self.timeout
            with self._condition:
                while offer.selected is None and not self._closed:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    self._condition.wait(remaining)
                if offer.selected is None:
                    self._clear_offer(offer)
                    return None
                option_id = offer.option_ids[offer.selected - 1]
            return PermissionSelection(
                option_id,
                response_written=lambda: self._response_complete(offer, None),
                response_failed=lambda exc: self._response_complete(offer, exc),
            )
        except BaseException:
            self._clear_offer(offer)
            raise

    def owns(self, decision: Any) -> bool:
        with self._lock:
            offer = self._offer
            return bool(
                not self._closed
                and offer is not None
                and decision.worker_id == self.worker_id
                and decision.worker_fingerprint == self.worker_fingerprint
                and decision.binding_private_fingerprint == offer.binding.private_fingerprint
                and decision.turn_target_value == offer.binding.turn_target_value
                and decision.decision_ref == offer.decision_ref
            )

    def answer(self, decision: Any, *, timeout: float) -> None:
        clear_failed = False
        uncertain = False
        with self._lock:
            offer = self._offer
            if offer is None or not self.owns(decision):
                raise AcpPermissionBrokerError("ACP permission authority changed")
            if decision.text is not None or len(decision.option_refs) != 1:
                raise AcpPermissionBrokerError("ACP permission selection is invalid")
            ordinal = int(decision.option_refs[0])
            if ordinal < 1 or ordinal > len(offer.option_ids) or offer.selected is not None:
                raise AcpPermissionBrokerError("ACP permission was already answered")
            offer.selected = ordinal
            self._condition.notify_all()
            deadline = time.monotonic() + max(0.1, float(timeout))
            while offer.response_state == "pending" and not self._closed:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._condition.wait(remaining)
            if offer.response_state != "written":
                # The command receipt is now terminally uncertain.  A late
                # transport callback must retire the stale public overlay and
                # its send_started claim, but only after it proves that the
                # response frame was written (or definitively failed).  Until
                # then the offer remains fail-closed and cannot be answered a
                # second time.
                offer.answer_abandoned = True
                clear_failed = offer.response_state == "failed"
                uncertain = True
            else:
                self._offer = None
        if clear_failed:
            self._clear_offer(offer)
        if uncertain:
            raise AcpPermissionBrokerError("ACP permission response state is uncertain")

    def close(self) -> None:
        with self._lock:
            self._closed = True
            offer = self._offer
            if offer is not None:
                self._condition.notify_all()
        if offer is not None:
            self._clear_offer(offer)

    def _response_complete(self, offer: _Offer, error: BaseException | None) -> None:
        clear = False
        with self._condition:
            if self._offer is offer:
                offer.response_state = "failed" if error is not None else "written"
                clear = error is not None or offer.answer_abandoned
                self._condition.notify_all()
        if clear:
            self._clear_offer(offer)

    def _clear_offer(self, offer: _Offer) -> None:
        with self._lock:
            if self._offer is not offer:
                return
            self._offer = None
        try:
            apply_backend_pending_observation(
                Path(self.config.db_path),
                self.config.host_id,
                self.worker_id,
                PendingObservation("read_succeeded_no_prompt"),
                binding_private_fingerprint=offer.binding.private_fingerprint,
                observed_turn_target_value=offer.binding.turn_target_value,
            )
        except Exception:
            pass

    def _exact_binding(self, session_id: str) -> WorkerBinding:
        rows = [
            row
            for row in list_worker_bindings(
                Path(self.config.db_path),
                self.config.host_id,
                backend="acp",
            )
            if row.worker_id == self.worker_id
            and row.worker_fingerprint == self.worker_fingerprint
            and row.turn_target_kind == "acp_session_id"
            and row.turn_target_value == session_id
            and row.sendable
        ]
        if len(rows) != 1:
            raise AcpPermissionBrokerError("ACP permission binding is not current")
        return rows[0]


def _tool_title(tool_call: Any) -> str:
    if isinstance(tool_call, Mapping):
        value = tool_call.get("title")
        if isinstance(value, str) and value.strip():
            clean = sanitize_public_text(value.strip()[:500])
            return clean or "Tool permission"
    return "Tool permission"


def _option_label(name: str, kind: str) -> str:
    clean_name = sanitize_public_text(name.strip()[:300]) or "Option"
    clean_kind = sanitize_public_text(kind.strip()[:80]) or "unknown"
    return f"{clean_name} ({clean_kind})"
