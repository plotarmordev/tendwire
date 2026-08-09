#!/usr/bin/env bash
set -Eeuo pipefail

readonly RELEASE_ID=67568f32-0b94403-f50af73-r8
readonly RELEASE=/home/smith/.local/share/acp-runtime/releases/${RELEASE_ID}
readonly ACTIVE=/home/smith/.local/share/acp-runtime/active
readonly TRANSACTION=/home/smith/.local/state/acp-cutover/frozen-0b94403-r8
readonly PHASE=${TRANSACTION}/phase
readonly TOPIC_PYTHON=/home/smith/.local/share/uv/tools/contexto/bin/python
readonly TENDWIRE_PYTHON=${RELEASE}/tendwire/bin/python
readonly TENDWIRE_SOCKET=/home/smith/.local/share/tendwire/tendwire.sock
readonly STATUS=${TRANSACTION}/rollout-status.json
readonly OWNER=${TRANSACTION}/rollout-owner
readonly HERDR_BASELINE=${TRANSACTION}/herdr-baseline.json

verify_herdr_identity() {
    python3 - "${HERDR_BASELINE}" <<'PY'
import json
import os
import subprocess
import sys

expected = json.load(open(sys.argv[1], encoding="utf-8"))
pid = int(subprocess.run(
    ["systemctl", "--user", "show", "--value", "-p", "MainPID", "herdr-server.service"],
    check=True,
    capture_output=True,
    text=True,
).stdout.strip())
actual = {
    "schema_version": 1,
    "pid": pid,
    "start_time": open(f"/proc/{pid}/stat", encoding="ascii").read().rsplit(") ", 1)[1].split()[19],
    "boot_id": open("/proc/sys/kernel/random/boot_id", encoding="ascii").read().strip(),
}

raise SystemExit(0 if actual == expected else 1)
PY
}

target_idle() {
    local agents pane
    agents="$(${RELEASE}/herdr agent list 2>/dev/null)" || return 1
    pane="$(${RELEASE}/herdr pane get w53:p8 2>/dev/null)" || return 1
    python3 - "${agents}" "${pane}" <<'PY'
import json
import sys

agents_raw, pane_raw = sys.argv[1:]
agents = (json.loads(agents_raw).get("result") or {}).get("agents") or []
expected_session = {
    "agent": "codex",
    "kind": "id",
    "source": "herdr:codex",
    "value": "019f96b6-3f4e-74a0-9ad9-6fbf68203f74",
}
matches = [
    row for row in agents
    if row.get("pane_id") == "w53:p8"
    and row.get("workspace_id") == "w53"
    and row.get("agent") == "codex"
    and row.get("name") is None
    and row.get("focused") is True
    and row.get("cwd") == "/home/smith/tendwire"
    and row.get("agent_status") == "idle"
    and row.get("agent_session") == expected_session
]
if len(matches) != 1:
    raise SystemExit(1)
row = matches[0]
completed = row.get("last_completed_turn") or {}
terminal = row.get("terminal_id")
if (
    row.get("turn") != completed.get("turn")
    or not isinstance(terminal, str)
    or not terminal
):
    raise SystemExit(1)
pane = (json.loads(pane_raw).get("result") or {}).get("pane") or {}
if not (
    pane.get("pane_id") == "w53:p8"
    and pane.get("workspace_id") == "w53"
    and pane.get("cwd") == "/home/smith/tendwire"
    and pane.get("agent") == "codex"
    and pane.get("agent_session") == expected_session
    and pane.get("terminal_id") == terminal
):
    raise SystemExit(1)
print(terminal)
PY
}

write_phase() {
    local value="$1"
    printf '%s\n' "${value}" >"${PHASE}.tmp.$$"
    chmod 600 "${PHASE}.tmp.$$"
    mv -f "${PHASE}.tmp.$$" "${PHASE}"
}

validation_monitor_ready() {
    python3 - "${PHASE}" \
        "${TRANSACTION}/validation-monitor-owner" \
        "${TRANSACTION}/validation-monitor-heartbeat" <<'PY'
import os
import time
import sys

phase_path, owner_path, heartbeat_path = sys.argv[1:]
try:
    if open(phase_path, encoding="ascii").read().strip() != "validating":
        raise ValueError
    prefix, raw_pid, expected_start = open(
        owner_path, encoding="ascii"
    ).read().split()
    pid = int(raw_pid)
    actual_start = open(f"/proc/{pid}/stat", encoding="ascii").read().rsplit(") ", 1)[1].split()[19]
    command = open(f"/proc/{pid}/cmdline", "rb").read().replace(b"\0", b" ")
    heartbeat_age = time.time() - os.stat(heartbeat_path).st_mtime
    heartbeat_prefix, heartbeat_pid = open(
        heartbeat_path, encoding="ascii"
    ).read().split()
except (OSError, ValueError, IndexError):
    raise SystemExit(1)
valid = (
    prefix == "FROZEN_ACP_VALIDATION_MONITOR"
    and pid > 1
    and expected_start == actual_start
    and b"monitor-one-hour-strict" in command
    and heartbeat_prefix == "FROZEN_ACP_STRICT_HEARTBEAT"
    and int(heartbeat_pid) == pid
    and 0 <= heartbeat_age <= 60
)
raise SystemExit(0 if valid else 1)
PY
}

write_owner() {
    local start_time
    start_time="$(python3 - "$$" <<'PY'
import sys
print(open(f"/proc/{int(sys.argv[1])}/stat", encoding="ascii").read().rsplit(") ", 1)[1].split()[19])
PY
)"
    printf 'R8_ROLLOUT %s %s\n' "$$" "${start_time}" >"${OWNER}.tmp"
    chmod 600 "${OWNER}.tmp"
    mv -f "${OWNER}.tmp" "${OWNER}"
}

write_status() {
    local state="$1"
    local phase="$2"
    "${TENDWIRE_PYTHON}" -I - "${STATUS}" "${state}" "${phase}" <<'PY'
import datetime
import json
import os
import sys

path, state, phase = sys.argv[1:]
temporary = f"{path}.tmp.{os.getpid()}"
body = json.dumps(
    {
        "schema_version": 1,
        "release_id": "67568f32-0b94403-f50af73-r8",
        "state": state,
        "phase": phase,
        "updated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "herdr_restarted": False,
        "historical_recovery": False,
    },
    sort_keys=True,
    separators=(",", ":"),
).encode() + b"\n"
descriptor = os.open(
    temporary,
    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
    0o600,
)
try:
    view = memoryview(body)
    while view:
        written = os.write(descriptor, view)
        if written < 1:
            raise OSError("rollout status write failed")
        view = view[written:]
    os.fsync(descriptor)
finally:
    os.close(descriptor)
os.replace(temporary, path)
PY
}

on_error() {
    local status="$?"
    trap - ERR HUP INT TERM
    write_phase failed
    rm -f -- "${OWNER}"
    write_status failed "line_${BASH_LINENO[0]:-unknown}_status_${status}" || true
    systemctl --user start --no-block acp-r8-rollback.service >/dev/null 2>&1 || true
    exit "${status}"
}

on_signal() {
    local status="$1"
    trap - ERR HUP INT TERM
    write_phase failed || true
    rm -f -- "${OWNER}"
    write_status failed "signal_status_${status}" || true
    systemctl --user start --no-block acp-r8-rollback.service >/dev/null 2>&1 || true
    exit "${status}"
}
trap on_error ERR
trap 'on_signal 129' HUP
trap 'on_signal 130' INT
trap 'on_signal 143' TERM

test "$(readlink -f "${ACTIVE}")" = "${RELEASE}"
test "$(tr -d '\n' <"${PHASE}")" = committing
systemctl --user is-active --quiet herdr-server.service
"${RELEASE}/release-integrity" verify \
    --manifest /home/smith/.local/share/acp-runtime/manifests/${RELEASE_ID}.json >/dev/null
"${RELEASE}/attest-r8" installed >/dev/null
verify_herdr_identity
for unit in tendwired.service herdres.service herdres-gateway.service; do
    if systemctl --user is-active --quiet "${unit}"; then
        exit 1
    fi
done
write_owner
systemctl --user start acp-r8-release-guard.service
systemctl --user is-active --quiet acp-r8-release-guard.service
write_status running wait_for_visible_pane_idle
# This service is started detached.  Waiting here lets the initiating Codex
# turn finish before migration, avoiding the self-termination loop seen in r7.
for _attempt in $(seq 1 600); do
    first_terminal="$(target_idle || true)"
    if [[ -n "${first_terminal}" ]]; then
        sleep 1
        second_terminal="$(target_idle || true)"
        if [[ "${first_terminal}" = "${second_terminal}" ]]; then
            break
        fi
    fi
    sleep 1
    test "${_attempt}" -lt 600
done
write_status running migrate_current_pane
"${RELEASE}/migrate-current-pane"

test "$(python3 -c 'import json; print(json.load(open("/home/smith/.local/state/acp-pane-migration/current/status.json"))["phase"])')" = complete
verify_herdr_identity
write_status running start_tendwire
for _attempt in $(seq 1 120); do
    sleep 1
    if systemctl --user is-active --quiet tendwired.service && \
        "${TENDWIRE_PYTHON}" -I - "${TENDWIRE_SOCKET}" <<'PY'
import sys
from tendwire.daemon_api import DaemonAPIClient

client = DaemonAPIClient(sys.argv[1], timeout_seconds=5)
health = (client.request("health.get", {}).get("result") or {})
snapshot = (client.request("snapshot.get", {}).get("result") or {})
acp = health.get("acp") or {}
workers = snapshot.get("workers") or []
raise SystemExit(
    0 if health.get("daemon", {}).get("status") == "healthy"
    and acp.get("healthy") is True
    and acp.get("state") == "running"
    and acp.get("worker_count") == 1
    and len(workers) == 1
    and workers[0].get("name") == "codex"
    else 1
)
PY
    then
        break
    fi
    test "${_attempt}" -lt 120
done

write_status running start_presenter
systemctl --user start herdres.service
for _attempt in $(seq 1 120); do
    sleep 1
    if "${TOPIC_PYTHON}" -I "${RELEASE}/reset-telegram-topics" \
        --verify-presenter >/dev/null 2>&1; then
        break
    fi
    test "${_attempt}" -lt 120
done

write_status running start_gateway
systemctl --user start herdres-gateway.service
for _attempt in $(seq 1 120); do
    sleep 1
    if "${TOPIC_PYTHON}" -I "${RELEASE}/reset-telegram-topics" \
        --verify-gateway >/dev/null 2>&1; then
        break
    fi
    test "${_attempt}" -lt 120
done

write_phase provisional
write_status running start_one_hour_monitor
systemctl --user start acp-frozen-live-monitor.service
systemctl --user is-active --quiet acp-frozen-live-monitor.service
for _attempt in $(seq 1 30); do
    if validation_monitor_ready; then
        break
    fi
    sleep 1
    test "${_attempt}" -lt 30
done
"${RELEASE}/attest-r8" live >/dev/null
verify_herdr_identity
rm -f -- "${OWNER}"
write_status monitoring validating
printf 'ROLLOUT_STARTED %s\n' "${RELEASE_ID}"
