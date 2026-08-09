#!/usr/bin/env bash
set -Eeuo pipefail

readonly HERDR=/home/smith/.local/share/acp-runtime/active/herdr
readonly TENDWIRE_PYTHON=/home/smith/.local/share/acp-runtime/active/tendwire/bin/python
readonly CODEX=/home/smith/.local/bin/codex
readonly CWD=/home/smith/tendwire
readonly NAME=tendwire-live
readonly KIND=codex
readonly EXPECTED_PANE=w53:p8
readonly EXPECTED_WORKSPACE=w53
readonly EXPECTED_WORKSPACE_LABEL=Tendwire
readonly EXPECTED_SESSION=019f96b6-3f4e-74a0-9ad9-6fbf68203f74
readonly STATE_DIR=/home/smith/.local/state/acp-pane-migration/current
readonly LOCK=/home/smith/.local/state/acp-pane-migration/current.lock
readonly ANCHOR=${STATE_DIR}/anchor.json

if [[ "$#" -gt 1 || ("$#" -eq 1 && "$1" != "--preflight-only") ]]; then
    exit 2
fi
readonly PREFLIGHT_ONLY="${1:-}"

exited=0
registered=0
tendwire_started=0
PANE=
STOPPED_GROUP=

write_status() {
    local phase="$1"
    install -d -m 700 "${STATE_DIR}"
    printf '{"phase":"%s","updated_at":"%s"}\n' \
        "${phase}" "$(date -u -Iseconds)" >"${STATE_DIR}/status.json.tmp"
    chmod 600 "${STATE_DIR}/status.json.tmp"
    mv -f "${STATE_DIR}/status.json.tmp" "${STATE_DIR}/status.json"
    printf '%s %s\n' "$(date -u -Iseconds)" "${phase}" >>"${STATE_DIR}/history.log"
    chmod 600 "${STATE_DIR}/history.log"
}

write_anchor() {
    python3 - "${ANCHOR}" "${PANE}" "${workspace_id}" "${terminal_id}" \
        "${session_id}" <<'PY'
import json
import os
import sys

path, pane, workspace, terminal, session = sys.argv[1:]
temporary = f"{path}.tmp.{os.getpid()}"
descriptor = os.open(
    temporary,
    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
    0o600,
)
try:
    body = json.dumps(
        {
            "schema_version": 1,
            "agent": "codex",
            "pane_id": pane,
            "workspace_id": workspace,
            "terminal_id": terminal,
            "session_id": session,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode() + b"\n"
    view = memoryview(body)
    while view:
        written = os.write(descriptor, view)
        if written < 1:
            raise OSError("anchor write failed")
        view = view[written:]
    os.fsync(descriptor)
finally:
    os.close(descriptor)
os.replace(temporary, path)
directory = os.open(os.path.dirname(path), os.O_RDONLY | os.O_DIRECTORY)
try:
    os.fsync(directory)
finally:
    os.close(directory)
PY
}

wait_for_shell() {
    local ready=0 pane_json process_json
    for _attempt in $(seq 1 120); do
        sleep 1
        if pane_json="$(${HERDR} pane get "${PANE}" 2>/dev/null)" \
            && process_json="$(${HERDR} pane process-info --pane "${PANE}" 2>/dev/null)" \
            && python3 - "${PANE}" "${CWD}" "${pane_json}" "${process_json}" <<'PY'
import json
import os
import sys

expected_pane, expected_cwd, raw_pane, raw_process = sys.argv[1:]
pane = json.loads(raw_pane)["result"]["pane"]
info = json.loads(raw_process)["result"]["process_info"]
shell_pid = info.get("shell_pid")
foreground = info.get("foreground_processes") or []
shell_names = {
    "sh", "bash", "dash", "zsh", "fish", "ksh", "mksh", "csh", "tcsh",
    "elvish", "xonsh", "nu", "pwsh", "powershell", "cmd",
}
shell_name = (
    os.path.basename(str(foreground[0].get("name", "")).replace("\\", "/"))
    .lstrip("-").lower().removesuffix(".exe")
    if foreground else ""
)
raise SystemExit(
    0 if info.get("pane_id") == expected_pane
    and isinstance(shell_pid, int) and shell_pid > 0
    and info.get("foreground_process_group_id") == shell_pid
    and len(foreground) == 1
    and foreground[0].get("pid") == shell_pid
    and foreground[0].get("cwd") == expected_cwd
    and shell_name in shell_names
    else 1
)
PY
        then
            ready=1
            break
        fi
    done
    test "${ready}" -eq 1
}

validate_visible_codex() {
    local process_json
    process_json="$(${HERDR} pane process-info --pane "${PANE}")"
    python3 - "${PANE}" "${CWD}" "${process_json}" <<'PY'
import json
import os
import sys

expected_pane, expected_cwd, raw = sys.argv[1:]
info = json.loads(raw)["result"]["process_info"]
foreground = info.get("foreground_processes") or []
shell_pid = info.get("shell_pid")
group = info.get("foreground_process_group_id")
names = [os.path.basename(str(item.get("name") or "")) for item in foreground]
if not (
    info.get("pane_id") == expected_pane
    and 1 <= len(foreground) <= 3
    and names.count("codex") == 1
    and all(name in {"node", "codex"} for name in names)
    and all(item.get("cwd") == expected_cwd for item in foreground)
    and isinstance(group, int) and group > 1
    and isinstance(shell_pid, int) and shell_pid > 1
    and group != shell_pid
):
    raise SystemExit(1)
PY
}

validate_idle_visible_codex() {
    local agents_json
    agents_json="${1:-}"
    if [[ -z "${agents_json}" ]]; then
        agents_json="$(${HERDR} agent list)"
    fi
    python3 - "${PANE}" "${CWD}" "${session_id}" "${terminal_id}" \
        "${agents_json}" <<'PY'
import json
import sys

expected_pane, expected_cwd, expected_session, expected_terminal, raw = sys.argv[1:]
agents = (json.loads(raw).get("result") or {}).get("agents") or []
matches = [
    row for row in agents
    if row.get("pane_id") == expected_pane
    and row.get("workspace_id") == "w53"
    and row.get("terminal_id") == expected_terminal
    and row.get("agent") == "codex"
    and row.get("name") is None
    and row.get("cwd") == expected_cwd
    and row.get("focused") is True
    and row.get("agent_status") == "idle"
    and (row.get("agent_session") or {}) == {
        "agent": "codex", "kind": "id", "source": "herdr:codex",
        "value": expected_session,
    }
]
if len(matches) != 1:
    raise SystemExit(1)
row = matches[0]
completed = row.get("last_completed_turn") or {}
if row.get("turn") != completed.get("turn"):
    raise SystemExit(1)
PY
}

terminate_visible_codex() {
    local first_agents_json second_agents_json pre_stop_agents_json
    local process_json stopped_agents_json stopped_process_json
    # Require a stable idle observation across a quiet interval.  Telegram
    # writers are still stopped at this point, so any intervening turn can only
    # come from the local pane and changes the compared agent snapshot.
    first_agents_json="$(${HERDR} agent list)"
    validate_idle_visible_codex "${first_agents_json}"
    validate_visible_codex
    sleep 2
    second_agents_json="$(${HERDR} agent list)"
    python3 - "${PANE}" "${CWD}" "${session_id}" "${terminal_id}" \
        "${first_agents_json}" "${second_agents_json}" <<'PY'
import json
import sys

expected_pane, expected_cwd, expected_session, expected_terminal, first_raw, second_raw = sys.argv[1:]

def target(raw):
    agents = (json.loads(raw).get("result") or {}).get("agents") or []
    matches = [
        row for row in agents
        if row.get("pane_id") == expected_pane
        and row.get("workspace_id") == "w53"
        and row.get("terminal_id") == expected_terminal
        and row.get("agent") == "codex"
        and row.get("name") is None
        and row.get("cwd") == expected_cwd
        and row.get("focused") is True
        and (row.get("agent_session") or {}) == {
            "agent": "codex", "kind": "id", "source": "herdr:codex",
            "value": expected_session,
        }
    ]
    if len(matches) != 1:
        raise SystemExit(1)
    row = matches[0]
    completed = row.get("last_completed_turn") or {}
    if row.get("agent_status") != "idle" or row.get("turn") != completed.get("turn"):
        raise SystemExit(1)
    return {
        key: row.get(key)
        for key in (
            "pane_id", "workspace_id", "terminal_id", "agent", "name",
            "focused", "agent_status", "agent_session", "turn",
            "last_completed_turn",
        )
    }

if target(first_raw) != target(second_raw):
    raise SystemExit(1)
PY
    validate_idle_visible_codex "${second_agents_json}"
    process_json="$(${HERDR} pane process-info --pane "${PANE}")"
    pre_stop_agents_json="$(${HERDR} agent list)"
    validate_idle_visible_codex "${pre_stop_agents_json}"
    STOPPED_GROUP="$(python3 - "${PANE}" "${CWD}" "${process_json}" <<'PY'
import json
import os
import signal
import sys

expected_pane, expected_cwd, raw = sys.argv[1:]
info = json.loads(raw)["result"]["process_info"]
foreground = info.get("foreground_processes") or []
shell_pid = info.get("shell_pid")
group = info.get("foreground_process_group_id")
names = [os.path.basename(str(item.get("name") or "")) for item in foreground]
if not (
    info.get("pane_id") == expected_pane
    and 1 <= len(foreground) <= 3
    and names.count("codex") == 1
    and all(name in {"node", "codex"} for name in names)
    and all(item.get("cwd") == expected_cwd for item in foreground)
    and isinstance(group, int) and group > 1
    and isinstance(shell_pid, int) and shell_pid > 1
    and group != shell_pid
):
    raise SystemExit(1)
os.killpg(group, signal.SIGSTOP)
print(group)
PY
)"
    test "${STOPPED_GROUP}" -gt 1

    # Stopping the full foreground process group closes the final local-input
    # race. Re-read both identities while it cannot advance; a mismatch resumes
    # the original group and aborts without killing the user's session.
    if ! stopped_agents_json="$(${HERDR} agent list)" \
        || ! stopped_process_json="$(${HERDR} pane process-info --pane "${PANE}")" \
        || ! python3 - "${PANE}" "${CWD}" "${session_id}" "${terminal_id}" \
            "${pre_stop_agents_json}" "${stopped_agents_json}" \
            "${process_json}" "${stopped_process_json}" "${STOPPED_GROUP}" <<'PY'
import json
import os
import sys

expected_pane, expected_cwd, expected_session, expected_terminal, before_agents_raw, after_agents_raw, before_process_raw, after_process_raw, raw_group = sys.argv[1:]
expected_group = int(raw_group)

def agent(raw):
    rows = (json.loads(raw).get("result") or {}).get("agents") or []
    matches = [
        row for row in rows
        if row.get("pane_id") == expected_pane
        and row.get("workspace_id") == "w53"
        and row.get("terminal_id") == expected_terminal
        and row.get("agent") == "codex"
        and row.get("name") is None
        and row.get("cwd") == expected_cwd
        and row.get("focused") is True
        and row.get("agent_status") == "idle"
        and (row.get("agent_session") or {}) == {
            "agent": "codex", "kind": "id", "source": "herdr:codex",
            "value": expected_session,
        }
    ]
    if len(matches) != 1:
        raise SystemExit(1)
    row = matches[0]
    if row.get("turn") != (row.get("last_completed_turn") or {}).get("turn"):
        raise SystemExit(1)
    return {key: row.get(key) for key in (
        "pane_id", "workspace_id", "terminal_id", "agent", "name",
        "focused", "agent_status", "agent_session", "turn",
        "last_completed_turn",
    )}

def process(raw):
    info = json.loads(raw)["result"]["process_info"]
    foreground = info.get("foreground_processes") or []
    identity = [
        {
            "pid": item.get("pid"),
            "name": os.path.basename(str(item.get("name") or "")),
            "cwd": item.get("cwd"),
        }
        for item in foreground
    ]
    if (
        info.get("pane_id") != expected_pane
        or info.get("foreground_process_group_id") != expected_group
        or not identity
        or sum(item["name"] == "codex" for item in identity) != 1
        or any(item["name"] not in {"node", "codex"} for item in identity)
        or any(item["cwd"] != expected_cwd for item in identity)
    ):
        raise SystemExit(1)
    return {
        "pane_id": info.get("pane_id"),
        "shell_pid": info.get("shell_pid"),
        "foreground_process_group_id": info.get("foreground_process_group_id"),
        "foreground_processes": identity,
    }

if agent(before_agents_raw) != agent(after_agents_raw):
    raise SystemExit(1)
if process(before_process_raw) != process(after_process_raw):
    raise SystemExit(1)
PY
    then
        kill -CONT -- "-${STOPPED_GROUP}" >/dev/null 2>&1 || true
        STOPPED_GROUP=
        return 1
    fi
    kill -TERM -- "-${STOPPED_GROUP}"
    kill -CONT -- "-${STOPPED_GROUP}" >/dev/null 2>&1 || true
    STOPPED_GROUP=
}

wait_for_console() {
    local ready=0 status process_json
    for _attempt in $(seq 1 60); do
        sleep 1
        if status="$(${HERDR} agent acp-status "${NAME}" 2>/dev/null)" \
            && process_json="$(${HERDR} pane process-info --pane "${PANE}" 2>/dev/null)" \
            && python3 - "${PANE}" "${workspace_id}" "${terminal_id}" \
                "${session_id}" "${status}" "${process_json}" <<'PY'
import json
import os
import sys

expected_pane, expected_workspace, expected_terminal, expected_session, raw_status, raw_process = sys.argv[1:]
status = json.loads(raw_status).get("result") or {}
worker = status.get("worker") or {}
adapter = status.get("adapter") or {}
session = status.get("session") or {}
process = json.loads(raw_process)["result"]["process_info"]
foreground = process.get("foreground_processes") or []
foreground_name = (
    os.path.basename(str(foreground[0].get("name", "")).replace("\\", "/"))
    if len(foreground) == 1 else ""
)
raise SystemExit(
    0 if status.get("lifecycle") in {"acp_owned_ready", "acp_owned_attached"}
    and status.get("console_lifecycle") == "attached"
    and adapter.get("name") == "codex-acp"
    and adapter.get("version") == "@agentclientprotocol/codex-acp 1.1.14"
    and session == {"mode": "resume", "id": expected_session}
    and worker.get("pane_id") == expected_pane
    and worker.get("workspace_id") == expected_workspace
    and worker.get("terminal_id") == expected_terminal
    and worker.get("name") == "tendwire-live"
    and worker.get("agent") == "codex"
    and status.get("cwd") == "/home/smith/tendwire"
    and foreground_name == "herdr"
    else 1
)
PY
        then
            ready=1
            break
        fi
    done
    test "${ready}" -eq 1
}

wait_for_acp() {
    local ready=0 status process_json
    for _attempt in $(seq 1 120); do
        sleep 1
        if status="$(${HERDR} agent acp-status "${NAME}" 2>/dev/null)" \
            && process_json="$(${HERDR} pane process-info --pane "${PANE}" 2>/dev/null)" \
            && python3 - "${PANE}" "${workspace_id}" "${terminal_id}" \
                "${session_id}" "${status}" "${process_json}" <<'PY'
import json
import os
import sys

expected_pane, expected_workspace, expected_terminal, expected_session, raw_status, raw_process = sys.argv[1:]
status = json.loads(raw_status).get("result") or {}
worker = status.get("worker") or {}
adapter = status.get("adapter") or {}
session = status.get("session") or {}
process = json.loads(raw_process)["result"]["process_info"]
foreground = process.get("foreground_processes") or []
foreground_name = (
    os.path.basename(str(foreground[0].get("name", "")).replace("\\", "/"))
    if len(foreground) == 1 else ""
)
raise SystemExit(
    0 if status.get("lifecycle") == "acp_owned_attached"
    and status.get("console_lifecycle") == "attached"
    and adapter.get("name") == "codex-acp"
    and adapter.get("version") == "@agentclientprotocol/codex-acp 1.1.14"
    and session == {"mode": "resume", "id": expected_session}
    and worker.get("pane_id") == expected_pane
    and worker.get("workspace_id") == expected_workspace
    and worker.get("terminal_id") == expected_terminal
    and worker.get("name") == "tendwire-live"
    and worker.get("agent") == "codex"
    and status.get("cwd") == "/home/smith/tendwire"
    and foreground_name == "herdr"
    else 1
)
PY
        then
            ready=1
            break
        fi
    done
    test "${ready}" -eq 1
}

# shellcheck disable=SC2329 # invoked from the signal/error cleanup path
unregister_owned_endpoint() {
    local status expected_herdr
    status="$(${HERDR} agent acp-status "${NAME}" 2>/dev/null)" || return 0
    expected_herdr="$(readlink -f "${HERDR}")" || return 1
    python3 - "${PANE}" "${workspace_id}" "${terminal_id}" "${session_id}" \
        "${expected_herdr}" "${registration_path}" "${status}" <<'PY'
import json
import sys

expected_pane, expected_workspace, expected_terminal, expected_session, expected_herdr, registration_path, raw = sys.argv[1:]
registered = json.load(open(registration_path, encoding="utf-8")).get("result") or {}
registered_agent = registered.get("agent") or {}
console = registered.get("console") or {}
args = console.get("args") or []
status = json.loads(raw).get("result") or {}
worker = status.get("worker") or {}
session = status.get("session") or {}
try:
    generation_index = args.index("--generation")
    lease_index = args.index("--lease")
    registered_generation = int(args[generation_index + 1])
    registered_lease = args[lease_index + 1]
except (ValueError, IndexError, TypeError):
    raise SystemExit(1)
raise SystemExit(
    0 if set(registered) >= {"agent", "console", "console_lifecycle"}
    and registered.get("console_lifecycle") == "attached"
    and registered_agent.get("pane_id") == expected_pane
    and registered_agent.get("workspace_id") == expected_workspace
    and registered_agent.get("terminal_id") == expected_terminal
    and registered_agent.get("name") == "tendwire-live"
    and registered_agent.get("agent") == "codex"
    and console.get("command") == expected_herdr
    and args[:3] == ["agent", "acp-console", "tendwire-live"]
    and len(args) == 7
    and isinstance(registered_lease, str) and len(registered_lease) >= 32
    and status.get("cwd") == "/home/smith/tendwire"
    and worker.get("pane_id") == expected_pane
    and worker.get("workspace_id") == expected_workspace
    and worker.get("terminal_id") == expected_terminal
    and worker.get("name") == "tendwire-live"
    and worker.get("agent") == "codex"
    and worker.get("generation") == registered_generation
    and session == {"mode": "resume", "id": expected_session}
    else 1
)
PY
    ${HERDR} agent acp-unregister "${NAME}" >/dev/null
}

# shellcheck disable=SC2329
restore_conventional_session() {
    trap - ERR HUP INT TERM
    if [[ -n "${STOPPED_GROUP}" ]]; then
        kill -CONT -- "-${STOPPED_GROUP}" >/dev/null 2>&1 || true
        STOPPED_GROUP=
    fi
    if [[ "${tendwire_started}" -eq 1 ]]; then
        systemctl --user stop tendwired.service >/dev/null 2>&1 || true
    fi
    if [[ "${registered}" -eq 1 ]]; then
        unregister_owned_endpoint >/dev/null 2>&1 || true
    fi
    if [[ "${exited}" -eq 1 ]]; then
        ${HERDR} pane send-keys "${PANE}" C-c >/dev/null 2>&1 || true
        if wait_for_shell; then
            rollback_command="$(python3 - "${CODEX}" "${session_id}" <<'PY'
import shlex
import sys
print(shlex.join([sys.argv[1], "resume", sys.argv[2]]))
PY
)"
            ${HERDR} pane run "${PANE}" "${rollback_command}" >/dev/null 2>&1 || true
        fi
    fi
    write_status rollback_attempted
}

# shellcheck disable=SC2329
on_error() {
    local status="$?"
    local line="${BASH_LINENO[0]:-unknown}"
    # Disable the ERR trap before recording or rolling back.  A failure in
    # either best-effort operation must not recursively invoke this handler.
    trap - ERR HUP INT TERM
    write_status "failed_line_${line}_status_${status}" || true
    restore_conventional_session
    exit "${status}"
}

# shellcheck disable=SC2329
on_signal() {
    local status="$1"
    local phase="$2"
    trap - ERR HUP INT TERM
    write_status "${phase}" || true
    restore_conventional_session
    exit "${status}"
}

trap on_error ERR
trap 'on_signal 129 hangup' HUP
trap 'on_signal 130 interrupted' INT
trap 'on_signal 143 terminated' TERM

install -d -m 700 "$(dirname "${LOCK}")"
if [[ "${PREFLIGHT_ONLY}" = "--preflight-only" ]]; then
    test -f "${LOCK}" && test ! -L "${LOCK}"
    exec 9<>"${LOCK}"
else
    exec 9>>"${LOCK}"
fi
flock -n 9
if [[ "${PREFLIGHT_ONLY}" != "--preflight-only" ]]; then
    install -d -m 700 "${STATE_DIR}"
    : >"${STATE_DIR}/history.log"
    chmod 600 "${STATE_DIR}/history.log"
    write_status validating
fi

systemctl --user is-active --quiet herdr-server.service
if systemctl --user is-active --quiet tendwired.service; then
    exit 1
fi
test -x "${HERDR}"
test -x "${TENDWIRE_PYTHON}"
test -x "${CODEX}"
# Never retire an already-owned endpoint as a side effect of migration.  Its
# name may belong to a live console in another pane; the operator must resolve
# that ownership before either preflight or cutover can proceed.
if ${HERDR} agent acp-status "${NAME}" >/dev/null 2>&1; then
    exit 1
fi

agent_json="$(${HERDR} agent list)"
workspace_json="$(${HERDR} workspace list)"
python3 - "${EXPECTED_WORKSPACE}" "${EXPECTED_WORKSPACE_LABEL}" \
    "${workspace_json}" <<'PY'
import json
import sys

expected_id, expected_label, raw = sys.argv[1:]
workspaces = (json.loads(raw).get("result") or {}).get("workspaces") or []
matches = [
    item for item in workspaces
    if item.get("workspace_id") == expected_id
    and item.get("label") == expected_label
]
raise SystemExit(0 if len(matches) == 1 else 1)
PY
readarray -t listed_identity < <(python3 - "${CWD}" "${PREFLIGHT_ONLY}" \
    "${EXPECTED_PANE}" "${EXPECTED_WORKSPACE}" "${EXPECTED_SESSION}" \
    "${agent_json}" <<'PY'
import json
import sys

expected_cwd, mode, expected_pane, expected_workspace, expected_session, raw = sys.argv[1:]
agents = (json.loads(raw).get("result") or {}).get("agents") or []
allowed_statuses = {"idle", "working"} if mode == "--preflight-only" else {"idle"}

def terminal_token(value):
    return (
        isinstance(value, str)
        and value.startswith("term_")
        and 1 <= len(value.removeprefix("term_")) <= 32
        and all(character in "0123456789abcdef" for character in value[5:])
    )

matches = [
    item for item in agents
    if item.get("agent") == "codex"
    and item.get("cwd") == expected_cwd
    and item.get("focused") is True
    and item.get("name") is None
    and item.get("pane_id") == expected_pane
    and item.get("workspace_id") == expected_workspace
    and (item.get("agent_session") or {}) == {
        "agent": "codex",
        "kind": "id",
        "source": "herdr:codex",
        "value": expected_session,
    }
    and terminal_token(item.get("terminal_id"))
    and item.get("agent_status") in allowed_statuses
]
if len(matches) != 1:
    raise SystemExit(1)
print(matches[0]["pane_id"])
print(matches[0]["terminal_id"])
PY
)
test "${#listed_identity[@]}" -eq 2
PANE="${listed_identity[0]}"
listed_terminal_id="${listed_identity[1]}"
test -n "${PANE}"
test "${PANE}" = "${EXPECTED_PANE}"
pane_json="$(${HERDR} pane get "${PANE}")"
readarray -t identity < <(python3 - "${PANE}" "${CWD}" "${pane_json}" <<'PY'
import json
import sys

expected_pane, expected_cwd, raw = sys.argv[1:]
pane = json.loads(raw)["result"]["pane"]
session = pane.get("agent_session") or {}
value = session.get("value")
terminal = pane.get("terminal_id")
workspace = pane.get("workspace_id")

def terminal_token(token):
    return (
        isinstance(token, str)
        and token.startswith("term_")
        and 1 <= len(token.removeprefix("term_")) <= 32
        and all(character in "0123456789abcdef" for character in token[5:])
    )

if (
    pane.get("pane_id") != expected_pane
    or pane.get("agent") != "codex"
    or pane.get("cwd") != expected_cwd
    or session.get("agent") != "codex"
    or session.get("kind") != "id"
    or session.get("source") != "herdr:codex"
    or not isinstance(value, str) or len(value) != 36
    or not terminal_token(terminal)
    or not isinstance(workspace, str) or not workspace
    or expected_pane != f"{workspace}:{expected_pane.split(':', 1)[1]}"
):
    raise SystemExit(1)
print(value)
print(terminal)
print(workspace)
PY
)
session_id="${identity[0]}"
terminal_id="${identity[1]}"
workspace_id="${identity[2]}"
test -n "${session_id}"
test -n "${terminal_id}"
test -n "${workspace_id}"
test "${session_id}" = "${EXPECTED_SESSION}"
test "${terminal_id}" = "${listed_terminal_id}"
test "${workspace_id}" = "${EXPECTED_WORKSPACE}"
if [[ "${PREFLIGHT_ONLY}" = "--preflight-only" ]]; then
    validate_visible_codex
    trap - ERR HUP INT TERM
    exit 0
fi
write_anchor
write_status captured

terminate_visible_codex
exited=1
wait_for_shell
write_status tui_exited

registration_path="${STATE_DIR}/registration.json"
test ! -e "${registration_path}" && test ! -L "${registration_path}"
registration_old_umask="$(umask)"
umask 077
set -o noclobber
exec 8>"${registration_path}"
set +o noclobber
umask "${registration_old_umask}"
registered=1
set +e
${HERDR} agent acp-register "${NAME}" --kind "${KIND}" --pane "${PANE}" \
    --cwd "${CWD}" --session-mode resume --session-id "${session_id}" >&8
registration_status="$?"
set -e
exec 8>&-
sync "${registration_path}"
sync -d "${STATE_DIR}"
test "${registration_status}" -eq 0
python3 - "${PANE}" "${registration_path}" <<'PY'
import json
import sys

expected_pane, path = sys.argv[1:]
result = json.load(open(path, encoding="utf-8")).get("result") or {}
agent = result.get("agent") or {}
console = result.get("console") or {}
command = console.get("command")
args = console.get("args")
if (
    agent.get("pane_id") != expected_pane
    or result.get("console_lifecycle") != "attached"
    or not isinstance(command, str) or not command
    or not isinstance(args, list) or not all(isinstance(item, str) for item in args)
):
    raise SystemExit(1)
PY
write_status registered
# `agent acp-register` atomically injects the console command into the shell and
# waits for console_lifecycle=attached.  Submitting its returned command again
# would type the lease-bearing launch string into the live ACP console.
wait_for_console
write_status console_attached
systemctl --user start tendwired.service
tendwire_started=1
write_status tendwire_started
wait_for_acp
write_status acp_attached
write_status complete
trap - ERR HUP INT TERM
registered=0
# Retain the generation/lease-bearing ownership proof through every fallible
# migration step.  Once completion is durable the endpoint is intentional,
# so remove the private proof without re-arming rollback on a cleanup error.
rm -f -- "${registration_path}" || true
sync -d "${STATE_DIR}" || true
exit 0
