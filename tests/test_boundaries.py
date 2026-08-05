"""Boundary tests: core modules must not load connector/runtime code."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


_CORE_MODULE_NAMES = (
    "tendwire.core.models",
    "tendwire.core.projector",
    "tendwire.core.attention",
    "tendwire.core.commands",
    "tendwire.core.turns",
)

_FORBIDDEN_PREFIXES = (
    "telegram",
    "herdres",
    "tendwire.backends",
    "tendwire.store",
    "tendwire.connectors",
    "tendwire.routing",
    "tendwire.delivery",
    "tendwire.daemon",
    "tendwire.outbox",
    "tendwire.source",
    "tendwire.source_mode",
)
_FORBIDDEN_EXACT = {"subprocess"}
_TURN_MODULE_FORBIDDEN_EXACT = {"socket", "subprocess"}


def _loaded_modules_after_import(module_name: str) -> set[str]:
    """Return sys.modules keys after importing a single module in isolation."""
    import os

    code = f"""
import sys
before = set(sys.modules.keys())
import {module_name}
after = set(sys.modules.keys())
for name in sorted(after - before):
    print(name)
"""
    env = os.environ.copy()
    env["PYTHONPATH"] = os.path.join(os.path.dirname(__file__), "..", "src")
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    if result.returncode != 0:
        raise AssertionError(
            f"Failed to inspect imports for {module_name}: {result.stderr}"
        )
    return {line.strip() for line in result.stdout.splitlines() if line.strip()}


def test_core_modules_do_not_load_connector_or_process_modules() -> None:
    for module_name in _CORE_MODULE_NAMES:
        loaded = _loaded_modules_after_import(module_name)
        forbidden_exact = _FORBIDDEN_EXACT
        if module_name == "tendwire.core.turns":
            forbidden_exact = _TURN_MODULE_FORBIDDEN_EXACT
        for name in loaded:
            lower = name.lower()
            if name in forbidden_exact or lower.startswith(_FORBIDDEN_PREFIXES):
                raise AssertionError(
                    f"{module_name} transitively loads forbidden module {name}"
                )


def test_readme_documents_production_risk_hardening_contracts() -> None:
    readme = (Path(__file__).resolve().parents[1] / "README.md").read_text(encoding="utf-8")

    for phrase in (
        "JSON integer `1` exactly",
        "ambiguous_backend_target",
        "healthy empty",
        "final `not_found`",
        "one durable row per key with a unique index",
        "aggregate deadline",
    ):
        assert phrase in readme
