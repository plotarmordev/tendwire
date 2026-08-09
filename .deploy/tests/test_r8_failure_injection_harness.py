from __future__ import annotations

import subprocess
import sys
from pathlib import Path


HARNESS = Path(__file__).with_name("r8_failure_injection_harness.py")


def test_all_r8_failure_injections_execute() -> None:
    result = subprocess.run(
        [sys.executable, "-I", str(HARNESS), "all"],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.stdout.strip() == "R8_FAILURE_INJECTION_OK all"
