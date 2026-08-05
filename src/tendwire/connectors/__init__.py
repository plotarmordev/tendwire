"""Neutral Tendwire connector delivery boundary."""

from typing import Any


def __getattr__(name: str) -> Any:
    if name != "ConnectorOutboxAPI":
        raise AttributeError(name)
    from .outbox import ConnectorOutboxAPI

    return ConnectorOutboxAPI

__all__ = ["ConnectorOutboxAPI"]
