"""Project neutral Snapshot objects from backend observations and config.

The projector imports only stdlib and sibling core modules. It must not import
Telegram, Herdres, or concrete backend connector modules.
"""

from __future__ import annotations

from datetime import datetime

from ..config import Config
from .attention import update_snapshot_attention
from .models import BackendHealth, Snapshot, Space, Worker, utc_timestamp


def project_from_observations(
    config: Config,
    *,
    spaces: list[Space] | None = None,
    workers: list[Worker] | None = None,
    backend_health: list[BackendHealth] | None = None,
    timestamp: datetime | None = None,
) -> Snapshot:
    """Build a neutral snapshot from backend-neutral observations."""
    snapshot = Snapshot(
        host_id=config.host_id,
        updated_at=utc_timestamp(timestamp),
        spaces=list(spaces or []),
        workers=list(workers or []),
        attention=[],
        backend_health=list(backend_health or []),
    )
    return update_snapshot_attention(snapshot)
