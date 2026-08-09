#!/usr/bin/env bash
set -Eeuo pipefail

# The transaction restores the previous active release before invoking this
# helper.  Resolve the CLI through that exact restored release so rollback does
# not depend on the candidate Herdr binary being runnable.
readonly HERDR=/home/smith/.local/share/acp-runtime/active/herdr
readonly CODEX=/home/smith/.local/bin/codex
readonly ANCHOR=/home/smith/.local/state/acp-pane-migration/current/anchor.json
readonly LOCK=/home/smith/.local/state/acp-pane-migration/current.lock
readonly PANE=w53:p8
readonly SESSION=019f96b6-3f4e-74a0-9ad9-6fbf68203f74

install -d -m 700 "$(dirname "${LOCK}")"
exec 9>"${LOCK}"
chmod 600 "${LOCK}"
flock -n 9

resolve_target_terminal() {
    local pane workspaces
    pane="$(${HERDR} pane get "${PANE}" 2>/dev/null)" || return 1
    workspaces="$(${HERDR} workspace list 2>/dev/null)" || return 1
    python3 - "${pane}" "${workspaces}" <<'PY'
import json
import sys

pane_raw, workspaces_raw = sys.argv[1:]
pane = json.loads(pane_raw)["result"]["pane"]
session = pane.get("agent_session") or {}
terminal = pane.get("terminal_id")

def terminal_token(value):
    return (
        isinstance(value, str)
        and value.startswith("term_")
        and 1 <= len(value.removeprefix("term_")) <= 32
        and all(character in "0123456789abcdef" for character in value[5:])
    )

workspaces = (json.loads(workspaces_raw).get("result") or {}).get("workspaces") or []
workspace_matches = [
    row for row in workspaces
    if row.get("workspace_id") == "w53" and row.get("label") == "Tendwire"
]
if (
    pane.get("pane_id") != "w53:p8"
    or pane.get("workspace_id") != "w53"
    or pane.get("agent") != "codex"
    or pane.get("cwd") != "/home/smith/tendwire"
    or session != {
        "agent": "codex",
        "kind": "id",
        "source": "herdr:codex",
        "value": "019f96b6-3f4e-74a0-9ad9-6fbf68203f74",
    }
    or not terminal_token(terminal)
    or len(workspace_matches) != 1
):
    raise SystemExit(1)
print(terminal)
PY
}

conventional_ready() {
    local agents process workspaces terminal
    terminal="$(resolve_target_terminal)" || return 1
    agents="$(${HERDR} agent list 2>/dev/null)" || return 1
    process="$(${HERDR} pane process-info --pane "${PANE}" 2>/dev/null)" || return 1
    workspaces="$(${HERDR} workspace list 2>/dev/null)" || return 1
    python3 - "${terminal}" "${agents}" "${process}" "${workspaces}" <<'PY'
import json
import os
import sys

expected_terminal, agents_raw, process_raw, workspaces_raw = sys.argv[1:]
agents = (json.loads(agents_raw).get("result") or {}).get("agents") or []
matches = [
    row for row in agents
    if row.get("pane_id") == "w53:p8"
    and row.get("workspace_id") == "w53"
    and row.get("terminal_id") == expected_terminal
    and row.get("agent") == "codex"
    and row.get("name") is None
    and row.get("cwd") == "/home/smith/tendwire"
    and (row.get("agent_session") or {}) == {
        "agent": "codex",
        "kind": "id",
        "source": "herdr:codex",
        "value": "019f96b6-3f4e-74a0-9ad9-6fbf68203f74",
    }
]
workspaces = (json.loads(workspaces_raw).get("result") or {}).get("workspaces") or []
workspace_matches = [
    row for row in workspaces
    if row.get("workspace_id") == "w53" and row.get("label") == "Tendwire"
]
info = json.loads(process_raw)["result"]["process_info"]
foreground = info.get("foreground_processes") or []
names = [os.path.basename(str(item.get("name") or "")) for item in foreground]
shell = info.get("shell_pid")
group = info.get("foreground_process_group_id")
valid_process = (
    info.get("pane_id") == "w53:p8"
    and 1 <= len(foreground) <= 3
    and names.count("codex") == 1
    and all(name in {"node", "codex"} for name in names)
    and all(item.get("cwd") == "/home/smith/tendwire" for item in foreground)
    and isinstance(shell, int) and shell > 1
    and isinstance(group, int) and group > 1
    and group != shell
)
raise SystemExit(
    0 if len(matches) == 1 and len(workspace_matches) == 1 and valid_process else 1
)
PY
}

session_ready_on_target() {
    local agents candidate process workspaces terminal
    terminal="$(resolve_target_terminal)" || return 1
    agents="$(${HERDR} agent list 2>/dev/null)" || return 1
    workspaces="$(${HERDR} workspace list 2>/dev/null)" || return 1
    candidate="$(python3 - "${terminal}" "${agents}" "${workspaces}" <<'PY'
import json
import sys

expected_terminal, agents_raw, workspaces_raw = sys.argv[1:]
agents = (json.loads(agents_raw).get("result") or {}).get("agents") or []
matches = [
    row for row in agents
    if row.get("pane_id") == "w53:p8"
    and row.get("workspace_id") == "w53"
    and row.get("terminal_id") == expected_terminal
    and row.get("agent") == "codex"
    and row.get("cwd") == "/home/smith/tendwire"
    and row.get("name") is None
    and (row.get("agent_session") or {}) == {
        "agent": "codex",
        "kind": "id",
        "source": "herdr:codex",
        "value": "019f96b6-3f4e-74a0-9ad9-6fbf68203f74",
    }
]
workspaces = (json.loads(workspaces_raw).get("result") or {}).get("workspaces") or []
workspace_matches = [
    row for row in workspaces
    if row.get("workspace_id") == "w53" and row.get("label") == "Tendwire"
]
if len(matches) != 1 or len(workspace_matches) != 1:
    raise SystemExit(1)
print("w53:p8")
PY
)" || return 1
    process="$(${HERDR} pane process-info --pane "${candidate}" 2>/dev/null)" || return 1
    python3 - "${candidate}" "${process}" <<'PY'
import json
import os
import sys

expected_pane, process_raw = sys.argv[1:]
info = json.loads(process_raw)["result"]["process_info"]
foreground = info.get("foreground_processes") or []
names = [os.path.basename(str(item.get("name") or "")) for item in foreground]
shell = info.get("shell_pid")
group = info.get("foreground_process_group_id")
valid = (
    info.get("pane_id") == expected_pane
    and 1 <= len(foreground) <= 3
    and names.count("codex") == 1
    and all(name in {"node", "codex"} for name in names)
    and all(item.get("cwd") == "/home/smith/tendwire" for item in foreground)
    and isinstance(shell, int) and shell > 1
    and isinstance(group, int) and group > 1
    and group != shell
)
raise SystemExit(0 if valid else 1)
PY
}

if conventional_ready; then
    exit 0
fi
if session_ready_on_target; then
    exit 0
fi

# An anchor is required only when a candidate ACP console actually needs to be
# dismantled.  A failed prepare before migration leaves the native session
# intact and must remain rollback-safe even if an older transaction anchor is
# absent or unrelated.
anchor_terminal="$(python3 - "${ANCHOR}" <<'PY'
import json
import sys

value = json.load(open(sys.argv[1], encoding="utf-8"))
expected = {
    "pane_id": "w53:p8",
    "workspace_id": "w53",
    "session_id": "019f96b6-3f4e-74a0-9ad9-6fbf68203f74",
}

def terminal_token(token):
    return (
        isinstance(token, str)
        and token.startswith("term_")
        and 1 <= len(token.removeprefix("term_")) <= 32
        and all(character in "0123456789abcdef" for character in token[5:])
    )

if (
    set(value) != {
        "schema_version", "agent", "pane_id", "workspace_id", "terminal_id",
        "session_id",
    }
    or type(value.get("schema_version")) is not int
    or value.get("schema_version") != 1
    or value.get("agent") != "codex"
    or any(value.get(key) != expected_value for key, expected_value in expected.items())
    or not terminal_token(value.get("terminal_id"))
):
    raise SystemExit(1)
print(value["terminal_id"])
PY
)"
test -n "${anchor_terminal}"

current_terminal="$(resolve_target_terminal)"
test -n "${current_terminal}"
if status="$(${HERDR} agent acp-status tendwire-live 2>/dev/null)"; then
    python3 - "${anchor_terminal}" "${current_terminal}" "${status}" <<'PY'
import json
import sys

anchor_terminal, current_terminal, raw_status = sys.argv[1:]
expected_terminals = {anchor_terminal, current_terminal}
status = json.loads(raw_status).get("result") or {}
worker = status.get("worker") or {}
session = status.get("session") or {}
adapter = status.get("adapter") or {}
valid = (
    status.get("lifecycle")
    in {"acp_owned_ready", "acp_owned_attached", "acp_owned_failed"}
    and status.get("console_lifecycle")
    in {"missing", "starting", "attached", "failed"}
    and status.get("cwd") == "/home/smith/tendwire"
    and adapter.get("name") == "codex-acp"
    and adapter.get("version") == "@agentclientprotocol/codex-acp 1.1.14"
    and session == {
        "mode": "resume",
        "id": "019f96b6-3f4e-74a0-9ad9-6fbf68203f74",
    }
    and worker.get("pane_id") == "w53:p8"
    and worker.get("workspace_id") == "w53"
    and worker.get("terminal_id") in expected_terminals
    and worker.get("name") == "tendwire-live"
    and worker.get("agent") == "codex"
)
raise SystemExit(0 if valid else 1)
PY
    "${HERDR}" agent acp-unregister tendwire-live >/dev/null
    "${HERDR}" pane send-keys "${PANE}" C-c >/dev/null 2>&1 || true
fi
for _attempt in $(seq 1 60); do
    if session_ready_on_target; then
        exit 0
    fi
    info="$(${HERDR} pane process-info --pane "${PANE}" 2>/dev/null || true)"
    if python3 - "${info}" <<'PY'
import json
import os
import sys

try:
    value = json.loads(sys.argv[1])["result"]["process_info"]
    foreground = value.get("foreground_processes") or []
    shell = value.get("shell_pid")
    name = os.path.basename(str(foreground[0].get("name") or "")) if len(foreground) == 1 else ""
except (ValueError, KeyError):
    raise SystemExit(1)
raise SystemExit(
    0 if value.get("pane_id") == "w53:p8"
    and len(foreground) == 1
    and foreground[0].get("pid") == shell
    and foreground[0].get("cwd") == "/home/smith/tendwire"
    and name in {"sh", "bash", "dash", "zsh", "fish"}
    else 1
)
PY
    then
        command="$(python3 - "${CODEX}" "${SESSION}" <<'PY'
import shlex
import sys
print(shlex.join([sys.argv[1], "resume", sys.argv[2]]))
PY
)"
        "${HERDR}" pane run "${PANE}" "${command}" >/dev/null
        for _resume_attempt in $(seq 1 120); do
            if conventional_ready || session_ready_on_target; then
                exit 0
            fi
            sleep 1
        done
        exit 1
    fi
    sleep 1
done
exit 1
