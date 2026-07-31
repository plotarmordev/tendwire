from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from tendwire.backends.acp_probe import (
    MAX_PROBE_CLOSE_TIMEOUT_SECONDS,
    MAX_PROBE_TIMEOUT_SECONDS,
    ProbeFailure,
    _extension_capability_count,
    main,
    probe_adapter,
)


FAKE_AGENT = Path(__file__).parent / "fixtures" / "acp_fake_agent.py"


def adapter_argv(mode: str = "normal") -> list[str]:
    return [sys.executable, "-u", str(FAKE_AGENT), mode]


def test_probe_negotiates_fresh_stable_v1_capabilities_and_reaps_process() -> None:
    first = probe_adapter(adapter_argv())
    second = probe_adapter(adapter_argv())

    for report in (first, second):
        payload = report.to_payload()
        assert payload["schema_version"] == 2
        assert payload["probe_scope"] == "initialize"
        assert payload["initialization_compatible"] is True
        assert payload["protocol_version"] == 1
        assert payload["process_reaped"] is True
        assert payload["failure"] is None
        assert payload["advertised_capabilities"]["session_load"] is True
        assert payload["advertised_capabilities"]["session_close"] is True
        assert payload["advertised_capabilities"]["session_delete"] is True
        assert payload["extensions"] == {
            "capability_count": 1,
            "capability_count_capped": False,
        }


def test_baseline_adapter_reports_only_baseline_capabilities() -> None:
    payload = probe_adapter(adapter_argv("baseline")).to_payload()
    assert payload["initialization_compatible"] is True
    assert "session_prompt" not in payload["advertised_capabilities"]
    assert "session_new" not in payload["advertised_capabilities"]
    assert payload["advertised_capabilities"]["session_load"] is False
    assert payload["advertised_capabilities"]["session_list"] is False
    assert payload["extensions"]["capability_count"] == 0


def test_initialize_only_agent_does_not_gain_untested_baseline_claims() -> None:
    payload = probe_adapter(adapter_argv("initialize_only")).to_payload()
    assert payload["initialization_compatible"] is True
    assert payload["probe_scope"] == "initialize"
    assert not {
        "session_new",
        "session_prompt",
        "session_cancel",
        "session_update",
    }.intersection(payload["advertised_capabilities"])


@pytest.mark.parametrize(
    ("mode", "failure", "timeout"),
    [
        ("malformed", ProbeFailure.PROTOCOL, 0.5),
        ("partial_eof", ProbeFailure.PROTOCOL, 0.5),
        ("bool_version", ProbeFailure.PROTOCOL_VERSION, 0.5),
        ("no_read", ProbeFailure.TIMEOUT, 0.05),
    ],
)
def test_probe_fails_closed_with_fixed_failure_categories(
    mode: str,
    failure: ProbeFailure,
    timeout: float,
) -> None:
    report = probe_adapter(
        adapter_argv(mode),
        timeout_seconds=timeout,
        close_timeout_seconds=0.05,
    )
    assert report.initialization_compatible is False
    assert report.failure is failure
    assert report.process_reaped is True
    assert not any(report.advertised_capabilities.values())


def test_missing_executable_does_not_expose_argv_or_exception_text() -> None:
    secret = "TOP_SECRET_ADAPTER_ARGUMENT"
    payload = probe_adapter(["/definitely/not/an/acp-adapter", secret]).to_payload()
    encoded = json.dumps(payload)
    assert payload["initialization_compatible"] is False
    assert payload["failure"] == ProbeFailure.LAUNCH_FAILED.value
    assert secret not in encoded
    assert "/definitely/not" not in encoded


def test_report_never_exposes_agent_or_extension_controlled_text() -> None:
    payload = probe_adapter(adapter_argv("extensions")).to_payload()
    encoded = json.dumps(payload)
    assert len(encoded.encode("utf-8")) < 2048
    assert "fake" not in encoded
    assert "vendor.example" not in encoded
    assert "_vendor.example/future_notification" not in encoded
    assert "level" not in encoded
    assert payload["extensions"]["capability_count"] == 1


def test_timeout_configuration_is_strictly_bounded() -> None:
    for timeout in (0, -1, float("inf"), MAX_PROBE_TIMEOUT_SECONDS + 1):
        report = probe_adapter(adapter_argv(), timeout_seconds=timeout)
        assert report.failure is ProbeFailure.INVALID_CONFIGURATION
    report = probe_adapter(
        adapter_argv(),
        close_timeout_seconds=MAX_PROBE_CLOSE_TIMEOUT_SECONDS + 1,
    )
    assert report.failure is ProbeFailure.INVALID_CONFIGURATION


def test_stubborn_adapter_is_reaped_without_becoming_source_dependency() -> None:
    report = probe_adapter(
        adapter_argv("stubborn"),
        timeout_seconds=1,
        close_timeout_seconds=0.05,
    )
    assert report.initialization_compatible is True
    assert report.process_reaped is True


def test_module_cli_outputs_one_bounded_json_object(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = main(["--timeout", "1", "--", *adapter_argv("baseline")])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert exit_code == 0
    assert captured.err == ""
    assert payload["initialization_compatible"] is True
    assert len(captured.out.encode("utf-8")) < 2048


def test_module_runs_as_black_box_without_importing_adapter_source() -> None:
    source_root = str(Path(__file__).parents[1] / "src")
    env = dict(os.environ)
    env["PYTHONPATH"] = source_root
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "tendwire.backends.acp_probe",
            "--timeout",
            "1",
            "--",
            *adapter_argv("baseline"),
        ],
        cwd=Path(__file__).parents[1],
        env=env,
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    payload = json.loads(completed.stdout)
    assert completed.returncode == 0
    assert completed.stderr == ""
    assert payload["initialization_compatible"] is True
    assert payload["process_reaped"] is True


def test_extension_count_uses_only_spec_reserved_meta_locations() -> None:
    assert (
        _extension_capability_count(
            {
                "_meta": {"vendor.one": {}},
                "promptCapabilities": {"_meta": {"vendor.two": {}}},
                "sessionCapabilities": {
                    "list": {"_meta": {"vendor.three": {}}}
                },
            }
        )
        == 3
    )
    assert _extension_capability_count({"forbiddenRootExtension": {}}) == 0


def test_authentication_count_skips_invalid_stable_schema_items() -> None:
    payload = probe_adapter(adapter_argv("auth_shapes")).to_payload()
    assert payload["initialization_compatible"] is True
    assert payload["authentication"] == {
        "method_count": 1,
        "method_count_capped": False,
    }
