"""Typed snapshot builders used only by tests."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from tendwire.config import Config
from tendwire.core.models import BackendHealth, Snapshot, Space, Worker
from tendwire.core.projector import project_from_observations


def project_empty(config: Config) -> Snapshot:
    return project_from_observations(config)


def project_from_raw(
    config: Config,
    *,
    spaces: list[dict[str, Any]] | None = None,
    workers: list[dict[str, Any]] | None = None,
    backend_health: list[dict[str, Any]] | None = None,
    timestamp: datetime | None = None,
) -> Snapshot:
    return project_from_observations(
        config,
        spaces=[Space.from_dict(value) for value in spaces or ()],
        workers=[Worker.from_dict(value) for value in workers or ()],
        backend_health=[BackendHealth.from_dict(value) for value in backend_health or ()],
        timestamp=timestamp,
    )
