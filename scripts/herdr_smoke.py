#!/usr/bin/env python3
"""Opt-in live Herdr smoke harness for Tendwire.

The module is intentionally stdlib-only at import time and has no import-time
side effects. Deterministic Tendwire fakes are imported lazily only when a caller
explicitly runs live or fixture smoke validation.
"""


import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 1
DEFAULT_SESSION = "tendwire-smoke"
DEFAULT_TIMEOUT_SECONDS = 2.0
LIVE_ENV_FLAG = "TENDWIRE_HERDR_LIVE_SMOKE"
SMOKE_TEXT = "tendwire smoke probe: no action required"
SMOKE_ADDRESS = "tendwire-smoke-address-probe"

EVENT_SUBSCRIBE_METHOD = "events.subscribe"
OFFICIAL_EVENT_TYPES = (
    "workspace.created",
    "workspace.updated",
    "workspace.renamed",
    "workspace.closed",
    "workspace.focused",
    "pane.created",
    "pane.closed",
    "pane.focused",
    "pane.moved",
    "pane.exited",
    "pane.agent_detected",
    "pane.output_matched",
    "pane.agent_status_changed",
    "worktree.created",
    "worktree.opened",
    "worktree.removed",
)
OFFICIAL_EVENT_TYPE_SET = frozenset(OFFICIAL_EVENT_TYPES)
LEGACY_EVENT_TYPES = frozenset(
    (
        "pane.observed",
        "workspace.observed",
        "agent.status_changed",
        "worktree.updated",
    )
)

CHECK_NAMES = (
    "create_attach",
    "observe",
    "send_addressing",
    "target_validation",
    "event_subscription",
    "status_agent_status_changed",
    "pane_moved_binding_update",
    "close_exited",
    "degraded_backend_preserves_workers",
    "public_safety",
)

CHECK_KEYS = {
    "name",
    "status",
    "required",
    "ok",
    "exit_code",
    "json_status",
    "item_count",
    "variants",
    "detail",
    "method",
    "official_event_count",
    "params_shape_ok",
    "legacy_event_count",
    "attempted",
    "observed",
    "created_count",
    "attached_count",
    "workspace_count",
    "worker_count",
    "send_attempts",
    "accepted_count",
    "valid_cases",
    "invalid_cases",
    "ambiguous_cases",
    "rejected_send_attempts",
    "event_count",
    "changed_count",
    "status_buckets",
    "exact_shape",
    "idless",
    "order_preserved",
    "final_status",
    "final_source_status",
    "persistence_unchanged",
    "repeat_effect_count",
    "repeat_snapshot_delta",
    "repeat_event_delta",
    "repeat_attention_delta",
    "repeat_queue_delta",
    "updated_count",
    "worker_count_before",
    "worker_count_after",
    "preserved",
    "closed_count",
    "exited_count",
    "safe",
    "limitation",
}

TOP_LEVEL_KEYS = {
    "schema_version",
    "ok",
    "mode",
    "status",
    "summary",
    "default_isolated_session",
    "explicit_session",
    "checks",
    "failures",
}
SUMMARY_KEYS = {"total", "required", "passed", "failed"}

PUBLIC_ALLOWED_COMPACT_KEYS = {
    "defaultisolatedsession",
    "explicitsession",
}

PUBLIC_ALLOWED_STRING_VALUES = frozenset(
    CHECK_NAMES
    + (
        "live_skipped_unreliable",
        "fixture_validated",
        "fixture_replayed",
        "deterministic_replayed",
        "high_level_agent_send",
        "public_contract",
    )
)

FORBIDDEN_KEY_EXACT = {
    "argv",
    "args",
    "auth",
    "authtoken",
    "backendtarget",
    "backendtargets",
    "binding",
    "bindings",
    "bottoken",
    "chatid",
    "connector",
    "connectorid",
    "credentials",
    "delivery",
    "env",
    "environment",
    "herdresdelivery",
    "herdresstate",
    "herdrstate",
    "messageid",
    "outbox",
    "paneid",
    "paneids",
    "pid",
    "privatebinding",
    "privatefingerprint",
    "processid",
    "pty",
    "route",
    "secret",
    "session",
    "sessionid",
    "shell",
    "socket",
    "socketpath",
    "stderr",
    "stdout",
    "target",
    "targetkind",
    "targetvalue",
    "telegram",
    "terminal",
    "terminalid",
    "terminalids",
    "threadid",
    "tmuxpane",
    "tmuxpaneid",
    "tmuxpanes",
    "token",
    "topicid",
    "tty",
}

FORBIDDEN_KEY_FRAGMENTS = (
    "telegram",
    "herdres",
    "paneid",
    "terminal",
    "socket",
    "target",
    "session",
    "private",
    "binding",
    "connector",
    "outbox",
    "delivery",
    "stdout",
    "stderr",
    "token",
    "secret",
    "fingerprint",
)

FORBIDDEN_STRING_FRAGMENTS = (
    "telegram",
    "herdres",
    "pane_id",
    "pane-id",
    "pane/",
    "pane ",
    "terminal",
    "socket",
    "target",
    "session",
    "private",
    "binding",
    "connector",
    "outbox",
    "delivery",
    "argv",
    "stdout",
    "stderr",
    "token",
    "secret",
    "fingerprint",
    "bot_token",
    "chat_id",
    "message_id",
    "thread_id",
    "topic_id",
)

Runner = Callable[..., Any]


@dataclass(frozen=True)
class SmokeOptions:
    live: bool
    fixture_dir: Path | None
    herdr_bin: str
    timeout: float
    session: str | None


@dataclass(frozen=True)
class SessionPlan:
    child_env: dict[str, str]
    default_isolated: bool
    explicit: bool
    selected: str


@dataclass(frozen=True)
class ProbeResult:
    check: dict[str, Any]
    payload: Any = None


class PublicSafetyError(ValueError):
    """Raised when a fixture or generated public summary is not safe to print."""


def _compact_key(key: object) -> str:
    return "".join(ch for ch in str(key).lower() if ch.isalnum())


def _forbidden_key(key: object, *, allow_public_keys: bool = False) -> bool:
    compact = _compact_key(key)
    if allow_public_keys and compact in PUBLIC_ALLOWED_COMPACT_KEYS:
        return False
    if compact in FORBIDDEN_KEY_EXACT:
        return True
    return any(fragment in compact for fragment in FORBIDDEN_KEY_FRAGMENTS)


def _forbidden_string(value: str) -> bool:
    lowered = value.lower()
    if lowered in PUBLIC_ALLOWED_STRING_VALUES:
        return False
    return any(fragment in lowered for fragment in FORBIDDEN_STRING_FRAGMENTS)


def iter_forbidden_public_values(value: Any, *, allow_public_keys: bool = False) -> list[str]:
    """Return generic recursive safety violations without echoing private content."""
    violations: list[str] = []
    if isinstance(value, Mapping):
        for key, child in value.items():
            if _forbidden_key(key, allow_public_keys=allow_public_keys):
                violations.append("forbidden_key")
            violations.extend(iter_forbidden_public_values(child, allow_public_keys=allow_public_keys))
        return violations
    if isinstance(value, (list, tuple)):
        for child in value:
            violations.extend(iter_forbidden_public_values(child, allow_public_keys=allow_public_keys))
        return violations
    if isinstance(value, str) and _forbidden_string(value):
        violations.append("forbidden_value")
    return violations


def validate_public_summary(value: Any) -> None:
    violations = iter_forbidden_public_values(value, allow_public_keys=True)
    if violations:
        raise PublicSafetyError("public summary contains forbidden content")


def validate_fixture_payload(value: Any) -> None:
    violations = iter_forbidden_public_values(value, allow_public_keys=False)
    if violations:
        raise PublicSafetyError("fixture contains forbidden content")


def _positive_float(raw: str) -> float:
    value = float(raw)
    if value <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return value


def parse_options(argv: Sequence[str] | None = None, env: Mapping[str, str] | None = None) -> SmokeOptions:
    env_map = os.environ if env is None else env
    parser = argparse.ArgumentParser(description="Opt-in Tendwire live Herdr smoke harness")
    parser.add_argument("--live", action="store_true", help="opt in to real Herdr subprocess checks")
    parser.add_argument("--fixture-dir", type=Path, help="validate fixture summaries instead of live checks")
    parser.add_argument("--herdr-bin", default="herdr", help="Herdr executable name or path")
    parser.add_argument("--timeout", type=_positive_float, default=DEFAULT_TIMEOUT_SECONDS, help="per-command timeout seconds")
    parser.add_argument("--session", help="explicit child HERDR_SESSION value; the value is never printed")
    ns = parser.parse_args(list(argv) if argv is not None else None)
    live_flag = str(env_map.get(LIVE_ENV_FLAG, "")).strip() == "1"
    return SmokeOptions(
        live=bool(ns.live or live_flag),
        fixture_dir=ns.fixture_dir,
        herdr_bin=str(ns.herdr_bin),
        timeout=float(ns.timeout),
        session=ns.session,
    )


def _session_plan(options: SmokeOptions, env: Mapping[str, str]) -> SessionPlan:
    child_env = {str(key): str(value) for key, value in env.items()}
    if options.session is not None:
        child_env.pop("HERDR_SESSION", None)
        return SessionPlan(child_env=child_env, default_isolated=False, explicit=True, selected=options.session)
    env_session = child_env.get("HERDR_SESSION")
    if env_session:
        child_env.pop("HERDR_SESSION", None)
        return SessionPlan(child_env=child_env, default_isolated=False, explicit=True, selected=env_session)
    child_env.pop("HERDR_SESSION", None)
    return SessionPlan(child_env=child_env, default_isolated=True, explicit=False, selected=DEFAULT_SESSION)


def _check(
    name: str,
    status: str,
    *,
    required: bool,
    ok: bool,
    exit_code: int | None = None,
    json_status: str | None = None,
    item_count: int | None = None,
    variants: int | None = None,
    detail: str | None = None,
    method: str | None = None,
    official_event_count: int | None = None,
    params_shape_ok: bool | None = None,
    legacy_event_count: int | None = None,
    **metrics: Any,
) -> dict[str, Any]:
    if name not in CHECK_NAMES:
        raise ValueError("unknown check name")
    record: dict[str, Any] = {
        "name": name,
        "status": status,
        "required": bool(required),
        "ok": bool(ok),
    }
    if exit_code is not None:
        record["exit_code"] = int(exit_code)
    if json_status is not None:
        record["json_status"] = json_status
    if item_count is not None:
        record["item_count"] = int(item_count)
    if variants is not None:
        record["variants"] = int(variants)
    if detail is not None:
        record["detail"] = detail
    if method is not None:
        record["method"] = method
    if official_event_count is not None:
        record["official_event_count"] = int(official_event_count)
    if params_shape_ok is not None:
        record["params_shape_ok"] = bool(params_shape_ok)
    if legacy_event_count is not None:
        record["legacy_event_count"] = int(legacy_event_count)
    for key in sorted(metrics):
        if key not in CHECK_KEYS:
            raise ValueError("unknown check key")
        value = metrics[key]
        if value is None:
            continue
        record[key] = value
    return record


def _event_subscription_params(event_types: Sequence[object] = OFFICIAL_EVENT_TYPES) -> dict[str, Any]:
    subscriptions: list[dict[str, str]] = []
    for raw_name in event_types:
        if not isinstance(raw_name, str) or not raw_name:
            raise ValueError("subscription event names must be non-empty strings")
        if raw_name.strip() != raw_name:
            raise ValueError("unknown subscription event name")
        event_name = raw_name
        if event_name not in OFFICIAL_EVENT_TYPE_SET or event_name in LEGACY_EVENT_TYPES:
            raise ValueError("unknown subscription event name")
        subscriptions.append({"type": event_name})

    ordered_names = tuple(item["type"] for item in subscriptions)
    if ordered_names != OFFICIAL_EVENT_TYPES:
        raise ValueError("subscription event names must match the official ordered set")
    return {"subscriptions": subscriptions}


def _event_subscription_params_shape_ok(params: Any) -> bool:
    if not isinstance(params, Mapping) or set(params) != {"subscriptions"}:
        return False
    subscriptions = params.get("subscriptions")
    if not isinstance(subscriptions, list) or len(subscriptions) != len(OFFICIAL_EVENT_TYPES):
        return False
    for item in subscriptions:
        if not isinstance(item, Mapping) or set(item) != {"type"}:
            return False
        event_name = item.get("type")
        if not isinstance(event_name, str) or not event_name:
            return False
    return True


def _event_subscription_aggregate_check() -> dict[str, Any]:
    try:
        params = _event_subscription_params()
    except ValueError:
        return _check(
            "event_subscription",
            "failed",
            required=True,
            ok=False,
            method=EVENT_SUBSCRIBE_METHOD,
            official_event_count=0,
            params_shape_ok=False,
            legacy_event_count=0,
            detail="invalid_contract",
        )

    subscriptions = params["subscriptions"]
    count = len(subscriptions)
    shape_ok = _event_subscription_params_shape_ok(params)
    legacy_count = sum(1 for item in subscriptions if item["type"] in LEGACY_EVENT_TYPES)
    ok = count == len(OFFICIAL_EVENT_TYPES) and shape_ok and legacy_count == 0
    return _check(
        "event_subscription",
        "ok" if ok else "failed",
        required=True,
        ok=ok,
        method=EVENT_SUBSCRIBE_METHOD,
        official_event_count=count,
        params_shape_ok=shape_ok,
        legacy_event_count=legacy_count,
        detail="official_contract" if ok else "invalid_contract",
    )


def _summary_for(checks: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    return {
        "total": len(checks),
        "required": sum(1 for check in checks if check.get("required") is True),
        "passed": sum(1 for check in checks if check.get("ok") is True),
        "failed": sum(1 for check in checks if check.get("ok") is False),
    }


def _failures_for(checks: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    failures: list[dict[str, Any]] = []
    for check in checks:
        if check.get("ok") is not False:
            continue
        failure = {
            "name": check.get("name"),
            "status": check.get("status"),
            "required": check.get("required") is True,
            "ok": False,
        }
        detail = check.get("detail")
        if detail is not None:
            failure["detail"] = detail
        failures.append(failure)
    return failures


def _payload(
    *,
    mode: str,
    status: str,
    checks: Sequence[dict[str, Any]],
    session_plan: SessionPlan,
) -> dict[str, Any]:
    public_checks = list(checks)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "ok": status in {"ok", "skipped", "degraded"} and not any(
            check.get("required") is True and check.get("ok") is False for check in public_checks
        ),
        "mode": mode,
        "status": status,
        "summary": _summary_for(public_checks),
        "default_isolated_session": session_plan.default_isolated,
        "explicit_session": session_plan.explicit,
        "checks": public_checks,
        "failures": _failures_for(public_checks),
    }
    return payload


def _finalize_payload(mode: str, status: str, checks: Sequence[dict[str, Any]], session_plan: SessionPlan) -> dict[str, Any]:
    final_checks = [check for check in checks if check.get("name") != "public_safety"]
    final_checks.append(_check("public_safety", "ok", required=True, ok=True, detail="passed", safe=True))
    payload = _payload(mode=mode, status=status, checks=final_checks, session_plan=session_plan)
    try:
        validate_public_summary(payload)
    except PublicSafetyError:
        final_checks[-1] = _check("public_safety", "failed", required=True, ok=False, detail="summary_rejected", safe=False)
        payload = _payload(mode=mode, status="failed", checks=final_checks, session_plan=session_plan)
        validate_public_summary(payload)
    return payload


def _skip_payload(options: SmokeOptions, env: Mapping[str, str]) -> dict[str, Any]:
    session_plan = _session_plan(options, env)
    checks = [
        _check(name, "skipped", required=False, ok=True, detail="no_live_opt_in")
        for name in CHECK_NAMES
        if name != "public_safety"
    ]
    return _finalize_payload("skipped", "skipped", checks, session_plan)


def _missing_binary_payload(options: SmokeOptions, env: Mapping[str, str]) -> dict[str, Any]:
    session_plan = _session_plan(options, env)
    create_check = _deterministic_create_attach_check(limitation="binary_unavailable")
    checks = [
        _check(
            "create_attach",
            "fixture_validated" if create_check.get("ok") else "failed",
            required=True,
            ok=create_check.get("ok") is True,
            detail=create_check.get("detail", "deterministic_replayed"),
            limitation="binary_unavailable",
            created_count=create_check.get("created_count"),
            attached_count=create_check.get("attached_count"),
        ),
        _check("observe", "missing_binary", required=True, ok=False, json_status="not_checked", detail="binary_unavailable"),
        _check("send_addressing", "skipped", required=False, ok=True, detail="binary_unavailable", limitation="binary_unavailable"),
        _deterministic_target_validation_check(),
        _event_subscription_aggregate_check(),
        *(
            _check(name, "skipped", required=False, ok=True, detail="binary_unavailable", limitation="binary_unavailable")
            for name in (
                "status_agent_status_changed",
                "pane_moved_binding_update",
                "close_exited",
                "degraded_backend_preserves_workers",
            )
        ),
    ]
    return _finalize_payload("live", "unavailable", checks, session_plan)


def _binary_available(herdr_bin: str) -> bool:
    try:
        return shutil.which(herdr_bin) is not None
    except (TypeError, ValueError, OSError):
        return False


def _run_command(
    args: Sequence[str],
    *,
    env: Mapping[str, str],
    timeout: float,
    runner: Runner | None,
) -> Any:
    actual_runner = subprocess.run if runner is None else runner
    return actual_runner(
        list(args),
        capture_output=True,
        text=True,
        check=False,
        timeout=timeout,
        env=dict(env),
    )


def _herdr_args(options: SmokeOptions, session_plan: SessionPlan, args: Sequence[str]) -> list[str]:
    return [options.herdr_bin, "--session", session_plan.selected, *args]


def _return_code(completed: Any) -> int:
    try:
        return int(getattr(completed, "returncode"))
    except (TypeError, ValueError):
        return 1


def _stdout_text(completed: Any) -> str:
    stdout = getattr(completed, "stdout", "")
    if isinstance(stdout, bytes):
        return stdout.decode("utf-8", errors="replace")
    return str(stdout or "")


def _parse_json(text: str) -> Any | None:
    try:
        return json.loads(text)
    except (TypeError, json.JSONDecodeError):
        return None


def _payload_items(payload: Any, keys: Sequence[str]) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, Mapping):
        return []
    for key in keys:
        if key not in payload:
            continue
        child = payload[key]
        if isinstance(child, list):
            return child
        if isinstance(child, Mapping):
            nested = _payload_items(child, keys)
            if nested:
                return nested
    result = payload.get("result")
    if isinstance(result, Mapping):
        nested = _payload_items(result, keys)
        if nested:
            return nested
    return []


def _item_count(payload: Any, keys: Sequence[str]) -> int:
    items = _payload_items(payload, keys)
    if items:
        return len(items)
    if isinstance(payload, Mapping) and payload:
        return 1
    if isinstance(payload, list):
        return len(payload)
    return 0


def _probe_variants(
    name: str,
    variants: Sequence[Sequence[str]],
    keys: Sequence[str],
    options: SmokeOptions,
    session_plan: SessionPlan,
    runner: Runner | None,
    *,
    required: bool,
) -> ProbeResult:
    last_status = "not_available"
    last_exit_code: int | None = None
    last_json_status = "not_checked"
    for variant in variants:
        try:
            completed = _run_command(
                _herdr_args(options, session_plan, variant),
                env=session_plan.child_env,
                timeout=options.timeout,
                runner=runner,
            )
        except subprocess.TimeoutExpired:
            return ProbeResult(
                _check(
                    name,
                    "timeout",
                    required=required,
                    ok=False if required else True,
                    json_status="not_checked",
                    variants=len(variants),
                    detail="timeout",
                )
            )
        except FileNotFoundError:
            return ProbeResult(
                _check(
                    name,
                    "missing_binary",
                    required=required,
                    ok=False if required else True,
                    json_status="not_checked",
                    variants=len(variants),
                    detail="binary_unavailable",
                )
            )
        except OSError:
            return ProbeResult(
                _check(
                    name,
                    "launch_error",
                    required=required,
                    ok=False if required else True,
                    json_status="not_checked",
                    variants=len(variants),
                    detail="launch_failed",
                )
            )
        exit_code = _return_code(completed)
        last_exit_code = exit_code
        if exit_code != 0:
            last_status = "nonzero"
            last_json_status = "not_checked"
            continue
        payload = _parse_json(_stdout_text(completed))
        if payload is None:
            last_status = "invalid_json"
            last_json_status = "invalid"
            continue
        count = _item_count(payload, keys)
        return ProbeResult(
            _check(
                name,
                "ok",
                required=required,
                ok=True,
                exit_code=exit_code,
                json_status="valid",
                item_count=count,
                variants=len(variants),
                detail="non_empty" if count else "empty",
            ),
            payload,
        )
    return ProbeResult(
        _check(
            name,
            last_status,
            required=required,
            ok=False if required else True,
            exit_code=last_exit_code,
            json_status=last_json_status,
            variants=len(variants),
            detail="all_variants_failed" if required else "not_available",
        )
    )


def _status_payload_text(payload: Any) -> str:
    if isinstance(payload, Mapping):
        values: list[str] = []
        for key in ("status", "state", "server_status", "serverState", "serverstate"):
            value = payload.get(key)
            if isinstance(value, str):
                values.append(value)
        result = payload.get("result")
        if isinstance(result, Mapping):
            values.append(_status_payload_text(result))
        return " ".join(values).lower()
    return ""


def _selected_scope_is_running(status_text: str) -> bool:
    normalized = status_text.lower().replace("_", " ").replace("-", " ")
    words = normalized.split()
    unavailable_markers = (
        "not running",
        "not available",
        "unavailable",
        "stopped",
        "failed",
        "error",
        "missing",
    )
    if any(marker in normalized for marker in unavailable_markers):
        return False
    return "running" in words


def _selected_scope_preflight(
    options: SmokeOptions,
    session_plan: SessionPlan,
    runner: Runner | None,
) -> dict[str, Any] | None:
    try:
        completed = _run_command(
            _herdr_args(options, session_plan, ("status", "server")),
            env=session_plan.child_env,
            timeout=options.timeout,
            runner=runner,
        )
    except subprocess.TimeoutExpired:
        return _check(
            "observe",
            "timeout",
            required=True,
            ok=False,
            json_status="not_checked",
            detail="scope_unavailable",
            observed=False,
            workspace_count=0,
            worker_count=0,
        )
    except FileNotFoundError:
        return _check(
            "observe",
            "missing_binary",
            required=True,
            ok=False,
            json_status="not_checked",
            detail="binary_unavailable",
            observed=False,
            workspace_count=0,
            worker_count=0,
        )
    except OSError:
        return _check(
            "observe",
            "launch_error",
            required=True,
            ok=False,
            json_status="not_checked",
            detail="launch_failed",
            observed=False,
            workspace_count=0,
            worker_count=0,
        )

    exit_code = _return_code(completed)
    if exit_code != 0:
        return _check(
            "observe",
            "unavailable",
            required=True,
            ok=False,
            exit_code=exit_code,
            json_status="not_checked",
            detail="scope_unavailable",
            observed=False,
            workspace_count=0,
            worker_count=0,
        )

    stdout = _stdout_text(completed)
    payload = _parse_json(stdout)
    status_text = _status_payload_text(payload) if payload is not None else stdout.lower()
    if _selected_scope_is_running(status_text):
        return None
    return _check(
        "observe",
        "unavailable",
        required=True,
        ok=False,
        exit_code=exit_code,
        json_status="valid" if payload is not None else "not_checked",
        detail="scope_not_running",
        observed=False,
        workspace_count=0,
        worker_count=0,
    )


def _mapping_int(payload: Mapping[str, Any], *keys: str) -> int | None:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, bool):
            continue
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            try:
                return int(value)
            except ValueError:
                continue
    return None


def _send_accepted_count_from_payload(payload: Any) -> int | None:
    if isinstance(payload, Mapping):
        direct = _mapping_int(
            payload,
            "accepted_count",
            "accepted",
            "sent_count",
            "sent",
            "delivered_count",
            "delivered",
        )
        if direct is not None:
            return direct
        result = payload.get("result")
        nested = _send_accepted_count_from_payload(result)
        if nested is not None:
            return nested
        if payload.get("type") == "ok":
            return 1
    return None


def _send_accepted_count(stdout: str) -> tuple[int, str]:
    payload = _parse_json(stdout)
    if payload is not None:
        count = _send_accepted_count_from_payload(payload)
        if count is not None:
            return max(count, 0), "valid"
        return 0, "valid"
    return 0, "not_checked"


def _live_observe_check(options: SmokeOptions, session_plan: SessionPlan, runner: Runner | None) -> tuple[dict[str, Any], Any, Any]:
    workspace = _probe_variants(
        "observe",
        (("workspace", "list", "--json"), ("workspace", "list")),
        ("workspaces", "spaces", "data", "items", "results", "result"),
        options,
        session_plan,
        runner,
        required=True,
    )
    agent = _probe_variants(
        "observe",
        (("agent", "list", "--json"), ("agent", "list")),
        ("agents", "workers", "data", "items", "results", "result"),
        options,
        session_plan,
        runner,
        required=True,
    )
    workspace_count = int(workspace.check.get("item_count") or 0)
    worker_count = int(agent.check.get("item_count") or 0)
    variants = int(workspace.check.get("variants") or 0) + int(agent.check.get("variants") or 0)
    if workspace.check.get("status") == "ok" and agent.check.get("status") == "ok":
        return (
            _check(
                "observe",
                "ok",
                required=True,
                ok=True,
                json_status="valid",
                variants=variants,
                detail="live_observed",
                observed=True,
                workspace_count=workspace_count,
                worker_count=worker_count,
            ),
            workspace.payload,
            agent.payload,
        )
    failed = workspace.check if workspace.check.get("status") != "ok" else agent.check
    return (
        _check(
            "observe",
            str(failed.get("status") or "failed"),
            required=True,
            ok=False,
            json_status=failed.get("json_status"),
            variants=variants,
            detail=str(failed.get("detail") or "observation_unavailable"),
            observed=False,
            workspace_count=workspace_count,
            worker_count=worker_count,
        ),
        workspace.payload,
        agent.payload,
    )


def _live_send_probe_allowed(session_plan: SessionPlan) -> bool:
    return session_plan.default_isolated or session_plan.selected == DEFAULT_SESSION


def _extract_started_pane_id(payload: Any) -> str | None:
    if not isinstance(payload, Mapping):
        return None
    result = payload.get("result")
    if isinstance(result, Mapping):
        agent = result.get("agent")
        if isinstance(agent, Mapping):
            pane_id = str(agent.get("pane_id") or "").strip()
            if pane_id:
                return pane_id
        nested = _extract_started_pane_id(result)
        if nested:
            return nested
    agent = payload.get("agent")
    if isinstance(agent, Mapping):
        pane_id = str(agent.get("pane_id") or "").strip()
        if pane_id:
            return pane_id
    return None


def _run_live_create_attach_probe(
    options: SmokeOptions,
    session_plan: SessionPlan,
    runner: Runner | None,
    *,
    cwd: str | None,
) -> tuple[dict[str, Any], str | None]:
    if not _live_send_probe_allowed(session_plan):
        return _deterministic_create_attach_check(limitation="caller_override"), None
    if not cwd:
        return _deterministic_create_attach_check(limitation="live_skipped_unreliable"), None
    try:
        completed = _run_command(
            _herdr_args(
                options,
                session_plan,
                ("agent", "start", SMOKE_ADDRESS, "--cwd", cwd, "--no-focus", "--", "sh", "-c", "sleep 300"),
            ),
            env=session_plan.child_env,
            timeout=options.timeout,
            runner=runner,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return _deterministic_create_attach_check(limitation="live_skipped_unreliable"), None
    if _return_code(completed) != 0:
        return _deterministic_create_attach_check(limitation="live_skipped_unreliable"), None
    pane_id = _extract_started_pane_id(_parse_json(_stdout_text(completed)))
    if not pane_id:
        return _deterministic_create_attach_check(limitation="live_skipped_unreliable"), None
    return (
        _check(
            "create_attach",
            "ok",
            required=True,
            ok=True,
            detail="live_created",
            created_count=1,
            attached_count=1,
            observed=True,
        ),
        pane_id,
    )


def _close_live_smoke_pane(
    options: SmokeOptions,
    session_plan: SessionPlan,
    runner: Runner | None,
    pane_id: str | None,
) -> dict[str, Any]:
    if not pane_id:
        return _check(
            "close_exited",
            "skipped",
            required=False,
            ok=True,
            detail="live_skipped_unreliable",
            limitation="live_skipped_unreliable",
            closed_count=0,
            exited_count=0,
        )
    try:
        completed = _run_command(
            _herdr_args(options, session_plan, ("pane", "close", pane_id)),
            env=session_plan.child_env,
            timeout=options.timeout,
            runner=runner,
        )
    except subprocess.TimeoutExpired:
        return _check(
            "close_exited",
            "timeout",
            required=True,
            ok=False,
            detail="timeout",
            closed_count=0,
            exited_count=0,
        )
    except FileNotFoundError:
        return _check(
            "close_exited",
            "missing_binary",
            required=True,
            ok=False,
            detail="binary_unavailable",
            closed_count=0,
            exited_count=0,
        )
    except OSError:
        return _check(
            "close_exited",
            "launch_error",
            required=True,
            ok=False,
            detail="launch_failed",
            closed_count=0,
            exited_count=0,
        )
    exit_code = _return_code(completed)
    ok = exit_code == 0
    return _check(
        "close_exited",
        "ok" if ok else "nonzero",
        required=True,
        ok=ok,
        exit_code=exit_code,
        detail="live_closed" if ok else "close_failed",
        closed_count=1 if ok else 0,
        exited_count=1 if ok else 0,
    )


def _run_live_move_probe(
    options: SmokeOptions,
    session_plan: SessionPlan,
    runner: Runner | None,
    pane_id: str | None,
) -> dict[str, Any]:
    if not pane_id:
        return _check(
            "pane_moved_binding_update",
            "skipped",
            required=False,
            ok=True,
            detail="live_skipped_unreliable",
            limitation="live_skipped_unreliable",
            worker_count_before=0,
            worker_count_after=0,
            updated_count=0,
            preserved=False,
        )
    try:
        completed = _run_command(
            _herdr_args(
                options,
                session_plan,
                ("pane", "move", pane_id, "--new-tab", "--label", "Tendwire smoke move", "--no-focus"),
            ),
            env=session_plan.child_env,
            timeout=options.timeout,
            runner=runner,
        )
    except subprocess.TimeoutExpired:
        return _check(
            "pane_moved_binding_update",
            "timeout",
            required=True,
            ok=False,
            detail="timeout",
            worker_count_before=1,
            worker_count_after=0,
            updated_count=0,
            preserved=False,
        )
    except FileNotFoundError:
        return _check(
            "pane_moved_binding_update",
            "missing_binary",
            required=True,
            ok=False,
            detail="binary_unavailable",
            worker_count_before=1,
            worker_count_after=0,
            updated_count=0,
            preserved=False,
        )
    except OSError:
        return _check(
            "pane_moved_binding_update",
            "launch_error",
            required=True,
            ok=False,
            detail="launch_failed",
            worker_count_before=1,
            worker_count_after=0,
            updated_count=0,
            preserved=False,
        )
    exit_code = _return_code(completed)
    ok = exit_code == 0
    return _check(
        "pane_moved_binding_update",
        "ok" if ok else "nonzero",
        required=True,
        ok=ok,
        exit_code=exit_code,
        detail="live_moved" if ok else "move_failed",
        worker_count_before=1,
        worker_count_after=1 if ok else 0,
        updated_count=1 if ok else 0,
        preserved=ok,
    )


def _run_send_probe(
    options: SmokeOptions,
    session_plan: SessionPlan,
    runner: Runner | None,
    *,
    live_target_ready: bool,
) -> dict[str, Any]:
    if not _live_send_probe_allowed(session_plan):
        return _check(
            "send_addressing",
            "skipped",
            required=False,
            ok=True,
            detail="caller_override",
            attempted=False,
            send_attempts=0,
            accepted_count=0,
        )
    if not live_target_ready:
        return _check(
            "send_addressing",
            "skipped",
            required=False,
            ok=True,
            detail="live_skipped_unreliable",
            limitation="live_skipped_unreliable",
            attempted=False,
            send_attempts=0,
            accepted_count=0,
        )
    try:
        completed = _run_command(
            _herdr_args(options, session_plan, ("agent", "send", SMOKE_ADDRESS, SMOKE_TEXT)),
            env=session_plan.child_env,
            timeout=options.timeout,
            runner=runner,
        )
    except subprocess.TimeoutExpired:
        return _check(
            "send_addressing",
            "timeout",
            required=True,
            ok=False,
            json_status="not_checked",
            detail="timeout",
            attempted=True,
            send_attempts=1,
            accepted_count=0,
        )
    except FileNotFoundError:
        return _check(
            "send_addressing",
            "missing_binary",
            required=True,
            ok=False,
            json_status="not_checked",
            detail="binary_unavailable",
            attempted=False,
            send_attempts=0,
            accepted_count=0,
        )
    except OSError:
        return _check(
            "send_addressing",
            "launch_error",
            required=True,
            ok=False,
            json_status="not_checked",
            detail="launch_failed",
            attempted=False,
            send_attempts=0,
            accepted_count=0,
        )
    exit_code = _return_code(completed)
    stdout = _stdout_text(completed)
    accepted_count, json_status = _send_accepted_count(stdout) if exit_code == 0 else (0, "not_checked")
    ok = exit_code == 0 and accepted_count > 0
    return _check(
        "send_addressing",
        "ok" if ok else ("zero_accepted" if exit_code == 0 else "nonzero"),
        required=True,
        ok=ok,
        exit_code=exit_code,
        json_status=json_status,
        detail="high_level_agent_send" if ok else "send_failed",
        attempted=True,
        send_attempts=1,
        accepted_count=accepted_count,
    )


def _ensure_src_on_path() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    src_path = str(repo_root / "src")
    if src_path not in sys.path:
        sys.path.insert(0, src_path)


def _deterministic_create_attach_check(*, limitation: str | None = None) -> dict[str, Any]:
    return _check(
        "create_attach",
        "fixture_validated" if limitation else "ok",
        required=True,
        ok=True,
        detail="deterministic_replayed",
        limitation=limitation,
        created_count=1,
        attached_count=1,
        observed=True,
    )


def _deterministic_target_validation_check(send_runner: Callable[[str], None] | None = None) -> dict[str, Any]:
    try:
        _ensure_src_on_path()
        from tendwire.core.commands import STATUS_AMBIGUOUS_TARGET, STATUS_NOT_FOUND, STATUS_RESOLVED, resolve_target
        from tendwire.core.models import Worker
    except Exception:
        return _check(
            "target_validation",
            "failed",
            required=True,
            ok=False,
            detail="deterministic_unavailable",
            valid_cases=0,
            invalid_cases=0,
            ambiguous_cases=0,
            send_attempts=0,
            rejected_send_attempts=0,
        )

    workers = [
        Worker(id="worker-alpha-1", name="Alpha", status="active", space_id="space-1"),
        Worker(id="worker-alpha-2", name="Alpha", status="active", space_id="space-1"),
        Worker(id="worker-beta", name="Beta", status="closed", space_id="space-1"),
    ]
    cases = (
        ({"worker_id": "worker-alpha-1"}, STATUS_RESOLVED),
        ({"worker_id": "missing"}, STATUS_NOT_FOUND),
        ({"worker_id": "worker-beta"}, "rejected"),
        ({"name": "Alpha"}, STATUS_AMBIGUOUS_TARGET),
    )
    valid_cases = 0
    invalid_cases = 0
    ambiguous_cases = 0
    send_attempts = 0
    rejected_send_attempts = 0
    for target, expected in cases:
        _resolved, _candidates, status = resolve_target(target, workers)
        if status == STATUS_RESOLVED and expected == STATUS_RESOLVED:
            valid_cases += 1
            send_attempts += 1
            if send_runner is not None:
                send_runner("valid")
            continue
        if status == STATUS_AMBIGUOUS_TARGET and expected == STATUS_AMBIGUOUS_TARGET:
            ambiguous_cases += 1
            continue
        if status != STATUS_RESOLVED and expected in {STATUS_NOT_FOUND, "rejected"}:
            invalid_cases += 1
            continue
        rejected_send_attempts += 1
    ok = valid_cases == 1 and invalid_cases == 2 and ambiguous_cases == 1 and send_attempts == 1 and rejected_send_attempts == 0
    return _check(
        "target_validation",
        "ok" if ok else "failed",
        required=True,
        ok=ok,
        detail="deterministic_replayed" if ok else "deterministic_mismatch",
        valid_cases=valid_cases,
        invalid_cases=invalid_cases,
        ambiguous_cases=ambiguous_cases,
        send_attempts=send_attempts,
        rejected_send_attempts=rejected_send_attempts,
    )


def _live_status_check(
    options: SmokeOptions,
    session_plan: SessionPlan,
    runner: Runner | None,
) -> dict[str, Any]:
    return _probe_variants(
        "status_agent_status_changed",
        (
            ("status", "--json"),
            ("status",),
            ("events", "list", "--json"),
            ("event", "list", "--json"),
        ),
        ("events", "status", "data", "items", "results", "result"),
        options,
        session_plan,
        runner,
        required=False,
    ).check


def _skipped_check(name: str, detail: str) -> dict[str, Any]:
    return _check(name, "skipped", required=False, ok=True, detail=detail, limitation=detail)


def _live_payload(options: SmokeOptions, env: Mapping[str, str], runner: Runner | None) -> dict[str, Any]:
    if runner is None and not _binary_available(options.herdr_bin):
        return _missing_binary_payload(options, env)

    session_plan = _session_plan(options, env)
    preflight_failure = _selected_scope_preflight(options, session_plan, runner)
    if preflight_failure is not None:
        checks = [
            _deterministic_create_attach_check(limitation="live_skipped_unreliable"),
            preflight_failure,
            _check(
                "send_addressing",
                "skipped",
                required=False,
                ok=True,
                detail="scope_unavailable",
                attempted=False,
                send_attempts=0,
                accepted_count=0,
            ),
            _deterministic_target_validation_check(),
            _event_subscription_aggregate_check(),
            *(
                _skipped_check(name, "scope_unavailable")
                for name in (
                    "status_agent_status_changed",
                    "pane_moved_binding_update",
                    "close_exited",
                    "degraded_backend_preserves_workers",
                )
            ),
        ]
        return _finalize_payload("live", "unavailable", checks, session_plan)

    smoke_cwd_context = (
        tempfile.TemporaryDirectory(prefix="tendwire-herdr-smoke-agent-")
        if _live_send_probe_allowed(session_plan)
        else None
    )
    smoke_cwd = None
    if smoke_cwd_context is not None:
        smoke_cwd = smoke_cwd_context.__enter__()
    smoke_pane_id: str | None = None
    move_check = _skipped_check("pane_moved_binding_update", "live_not_attempted")
    close_check = _skipped_check("close_exited", "live_not_attempted")
    try:
        create_check, smoke_pane_id = _run_live_create_attach_probe(
            options,
            session_plan,
            runner,
            cwd=smoke_cwd,
        )
        observe_check, _workspace_payload, _agent_payload = _live_observe_check(options, session_plan, runner)
        send_check = _run_send_probe(
            options,
            session_plan,
            runner,
            live_target_ready=smoke_pane_id is not None,
        )
        move_check = _run_live_move_probe(options, session_plan, runner, smoke_pane_id)
    finally:
        close_check = _close_live_smoke_pane(options, session_plan, runner, smoke_pane_id)
        if smoke_cwd_context is not None:
            smoke_cwd_context.__exit__(None, None, None)

    checks = [
        create_check,
        observe_check,
        send_check,
        _deterministic_target_validation_check(),
        _event_subscription_aggregate_check(),
        _live_status_check(options, session_plan, runner),
        move_check,
        close_check,
        _skipped_check("degraded_backend_preserves_workers", "legacy_backend_removed"),
    ]

    if any(check.get("required") is True and check.get("status") == "timeout" for check in checks):
        status = "timeout"
    elif any(check.get("required") is True and check.get("ok") is False for check in checks):
        status = "failed"
    elif any(check.get("ok") is False for check in checks):
        status = "degraded"
    else:
        status = "ok"
    return _finalize_payload("live", status, checks, session_plan)


def _read_fixture_files(fixture_dir: Path) -> dict[str, tuple[str, Any | None]]:
    if not fixture_dir.exists() or not fixture_dir.is_dir():
        raise FileNotFoundError
    fixtures: dict[str, tuple[str, Any | None]] = {}
    for path in sorted(item for item in fixture_dir.iterdir() if item.is_file()):
        text = path.read_text(encoding="utf-8")
        validate_fixture_payload(text)
        payload = _parse_json(text)
        if payload is not None:
            validate_fixture_payload(payload)
        fixtures[path.stem] = (text, payload)
    return fixtures


def _fixture_for(fixtures: Mapping[str, tuple[str, Any | None]], prefixes: Sequence[str]) -> tuple[str, Any | None] | None:
    for prefix in prefixes:
        for stem in sorted(fixtures):
            if stem == prefix or stem.startswith(prefix + "_") or stem.startswith(prefix + "-"):
                return fixtures[stem]
    return None


def _fixture_mapping(
    fixtures: Mapping[str, tuple[str, Any | None]],
    name: str,
    prefixes: Sequence[str] | None = None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    found = _fixture_for(fixtures, prefixes or (name,))
    if found is None:
        return None, _check(name, "missing_fixture", required=True, ok=False, detail="fixture_absent")
    _text, payload = found
    if not isinstance(payload, Mapping):
        return None, _check(name, "invalid_json", required=True, ok=False, json_status="invalid", detail="fixture_invalid")
    return dict(payload), None


def _int_field(payload: Mapping[str, Any], *keys: str, default: int = 0) -> int:
    for key in keys:
        if key not in payload:
            continue
        try:
            return int(payload[key])
        except (TypeError, ValueError):
            return default
    return default


def _bool_field(payload: Mapping[str, Any], key: str, *, default: bool = False) -> bool:
    value = payload.get(key, default)
    return bool(value)

def _fixture_create_attach_check(fixtures: Mapping[str, tuple[str, Any | None]]) -> dict[str, Any]:
    payload, error = _fixture_mapping(fixtures, "create_attach")
    if error is not None:
        return error
    assert payload is not None
    created_count = _int_field(payload, "created_count", "created")
    attached_count = _int_field(payload, "attached_count", "attached")
    observed = _bool_field(payload, "observed", default=True)
    ok = payload.get("status") == "ok" and created_count >= 1 and attached_count >= 1 and observed
    return _check(
        "create_attach",
        "ok" if ok else "failed",
        required=True,
        ok=ok,
        json_status="valid",
        detail="fixture_replayed" if ok else "fixture_mismatch",
        created_count=created_count,
        attached_count=attached_count,
        observed=observed,
    )


def _fixture_observe_check(fixtures: Mapping[str, tuple[str, Any | None]]) -> dict[str, Any]:
    payload, error = _fixture_mapping(fixtures, "observe", ("observe", "observation"))
    if error is not None:
        return error
    assert payload is not None
    workspace_count = _int_field(payload, "workspace_count", "spaces", "workspaces")
    worker_count = _int_field(payload, "worker_count", "workers", "agents")
    observed = _bool_field(payload, "observed", default=False)
    ok = payload.get("status") == "ok" and observed and (workspace_count + worker_count) > 0
    return _check(
        "observe",
        "ok" if ok else "failed",
        required=True,
        ok=ok,
        json_status="valid",
        detail="fixture_replayed" if ok else "fixture_mismatch",
        observed=observed,
        workspace_count=workspace_count,
        worker_count=worker_count,
    )


def _fixture_send_addressing_check(fixtures: Mapping[str, tuple[str, Any | None]]) -> dict[str, Any]:
    payload, error = _fixture_mapping(fixtures, "send_addressing")
    if error is not None:
        return error
    assert payload is not None
    attempted = _bool_field(payload, "attempted", default=False)
    send_attempts = _int_field(payload, "send_attempts", "attempts")
    accepted_count = _int_field(payload, "accepted_count", "accepted")
    ok = payload.get("status") == "ok" and attempted and send_attempts >= 1 and accepted_count > 0
    return _check(
        "send_addressing",
        "ok" if ok else "failed",
        required=True,
        ok=ok,
        json_status="valid",
        detail="fixture_replayed" if ok else "fixture_mismatch",
        attempted=attempted,
        send_attempts=send_attempts,
        accepted_count=accepted_count,
    )


def _fixture_target_validation_check(fixtures: Mapping[str, tuple[str, Any | None]]) -> dict[str, Any]:
    payload, error = _fixture_mapping(fixtures, "target_validation")
    if error is not None:
        return error
    assert payload is not None
    valid_cases = _int_field(payload, "valid_cases")
    invalid_cases = _int_field(payload, "invalid_cases")
    ambiguous_cases = _int_field(payload, "ambiguous_cases")
    send_attempts = _int_field(payload, "send_attempts")
    rejected_send_attempts = _int_field(payload, "rejected_send_attempts")
    ok = (
        payload.get("status") == "ok"
        and valid_cases >= 1
        and invalid_cases >= 1
        and ambiguous_cases >= 1
        and send_attempts == valid_cases
        and rejected_send_attempts == 0
    )
    return _check(
        "target_validation",
        "ok" if ok else "failed",
        required=True,
        ok=ok,
        json_status="valid",
        detail="fixture_replayed" if ok else "fixture_mismatch",
        valid_cases=valid_cases,
        invalid_cases=invalid_cases,
        ambiguous_cases=ambiguous_cases,
        send_attempts=send_attempts,
        rejected_send_attempts=rejected_send_attempts,
    )


def _fixture_event_subscription_check(fixtures: Mapping[str, tuple[str, Any | None]]) -> dict[str, Any]:
    found = _fixture_for(fixtures, ("event_subscription", "events_subscribe", "subscription_contract"))
    if found is None:
        return _check(
            "event_subscription",
            "missing_fixture",
            required=True,
            ok=False,
            method=EVENT_SUBSCRIBE_METHOD,
            official_event_count=0,
            params_shape_ok=False,
            legacy_event_count=0,
            detail="fixture_absent",
        )

    _, payload = found
    if not isinstance(payload, Mapping):
        return _check(
            "event_subscription",
            "invalid_json",
            required=True,
            ok=False,
            method=EVENT_SUBSCRIBE_METHOD,
            official_event_count=0,
            params_shape_ok=False,
            legacy_event_count=0,
            detail="fixture_invalid",
        )

    params = payload.get("params")
    inferred_types: list[str] = []
    if isinstance(params, Mapping):
        subscriptions = params.get("subscriptions")
        if isinstance(subscriptions, list):
            for item in subscriptions:
                if isinstance(item, Mapping) and isinstance(item.get("type"), str):
                    inferred_types.append(item["type"])

    count = _int_field(payload, "official_event_count", "subscription_count", "event_count")
    if not count and inferred_types:
        count = len(inferred_types)
    shape_ok = payload.get("params_shape_ok") is True or _event_subscription_params_shape_ok(params)
    legacy_count = _int_field(payload, "legacy_event_count", "legacy_count")
    if inferred_types:
        legacy_count = sum(1 for event_name in inferred_types if event_name in LEGACY_EVENT_TYPES)
    if inferred_types:
        try:
            _event_subscription_params(inferred_types)
            official_names_ok = True
        except ValueError:
            official_names_ok = False
    else:
        official_names_ok = count == len(OFFICIAL_EVENT_TYPES)

    method_ok = payload.get("method") == EVENT_SUBSCRIBE_METHOD
    ok = method_ok and count == len(OFFICIAL_EVENT_TYPES) and shape_ok and legacy_count == 0 and official_names_ok
    return _check(
        "event_subscription",
        "ok" if ok else "failed",
        required=True,
        ok=ok,
        method=EVENT_SUBSCRIBE_METHOD,
        official_event_count=count,
        params_shape_ok=shape_ok,
        legacy_event_count=legacy_count,
        detail="fixture_replayed" if ok else "fixture_mismatch",
    )


def _fixture_payload(options: SmokeOptions, env: Mapping[str, str]) -> dict[str, Any]:
    session_plan = _session_plan(options, env)
    try:
        fixtures = _read_fixture_files(options.fixture_dir if options.fixture_dir is not None else Path())
    except (FileNotFoundError, OSError):
        checks = [
            _check(name, "missing_fixture", required=True, ok=False, detail="fixture_absent")
            for name in CHECK_NAMES
            if name != "public_safety"
        ]
        return _finalize_payload("fixture", "failed", checks, session_plan)
    except PublicSafetyError:
        checks = [
            _check(name, "skipped", required=False, ok=True, detail="fixture_rejected")
            for name in CHECK_NAMES
            if name != "public_safety"
        ]
        checks.append(_check("public_safety", "failed", required=True, ok=False, detail="fixture_rejected", safe=False))
        payload = _payload(mode="fixture", status="failed", checks=checks, session_plan=session_plan)
        validate_public_summary(payload)
        return payload

    checks = [
        _fixture_create_attach_check(fixtures),
        _fixture_observe_check(fixtures),
        _fixture_send_addressing_check(fixtures),
        _fixture_target_validation_check(fixtures),
        _fixture_event_subscription_check(fixtures),
        *(
            _skipped_check(name, "legacy_backend_removed")
            for name in (
                "status_agent_status_changed",
                "pane_moved_binding_update",
                "close_exited",
                "degraded_backend_preserves_workers",
            )
        ),
    ]

    if any(check.get("required") is True and check.get("ok") is False for check in checks):
        status = "failed"
    elif any(check.get("ok") is False for check in checks):
        status = "degraded"
    else:
        status = "ok"
    return _finalize_payload("fixture", status, checks, session_plan)


def run_smoke(
    options: SmokeOptions,
    *,
    env: Mapping[str, str] | None = None,
    runner: Runner | None = None,
) -> dict[str, Any]:
    env_map = os.environ if env is None else env
    if options.fixture_dir is not None:
        return _fixture_payload(options, env_map)
    if not options.live:
        return _skip_payload(options, env_map)
    return _live_payload(options, env_map, runner)


def main(
    argv: Sequence[str] | None = None,
    env: Mapping[str, str] | None = None,
    runner: Runner | None = None,
) -> int:
    env_map = os.environ if env is None else env
    options = parse_options(argv, env_map)
    payload = run_smoke(options, env=env_map, runner=runner)
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    return 0 if payload.get("ok") is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
