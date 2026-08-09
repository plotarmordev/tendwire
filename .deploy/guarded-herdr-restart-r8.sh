#!/usr/bin/env bash
set -Eeuo pipefail

readonly ACTIVE=/home/smith/.local/share/acp-runtime/active
readonly NEW_RELEASE=/home/smith/.local/share/acp-runtime/releases/67568f32-0b94403-f50af73-r8
readonly TRANSACTION=/home/smith/.local/state/acp-cutover/frozen-0b94403-r8
readonly DROPIN=/home/smith/.config/systemd/user/herdr-server.service.d/99-codex-acp-r8.conf
readonly PRE=${TRANSACTION}/herdr-topology-pre.json
readonly POST=${TRANSACTION}/herdr-topology-post.json
readonly ROLLBACK=${TRANSACTION}/herdr-topology-rollback.json

restarted=0

capture_snapshot() {
    local destination="$1" temporary
    temporary="${destination}.tmp.$$"
    (umask 077; "${ACTIVE}/herdr" api snapshot >"${temporary}")
    python3 - "${temporary}" <<'PY'
import json
import sys

path = sys.argv[1]
value = json.load(open(path, encoding="utf-8"))
snapshot = (value.get("result") or {}).get("snapshot")
if not isinstance(snapshot, dict):
    raise SystemExit("Herdr snapshot is missing")
if not all(isinstance(snapshot.get(key), list) and snapshot[key] for key in ("workspaces", "tabs", "panes", "layouts")):
    raise SystemExit("Herdr topology is incomplete")
PY
    chmod 600 "${temporary}"
    mv -f "${temporary}" "${destination}"
}

compare_topology() {
    local first="$1" second="$2"
    "${NEW_RELEASE}/topology-normalizer" "${first}" "${second}" \
        --session 019f96b6-3f4e-74a0-9ad9-6fbf68203f74 \
        --pane w53:p8 \
        --cwd /home/smith/tendwire
}

wait_for_server() {
    for _attempt in $(seq 1 60); do
        if systemctl --user is-active --quiet herdr-server.service \
            && "${ACTIVE}/herdr" api snapshot >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    return 1
}

rollback() {
    local status="$1"
    trap - ERR HUP INT TERM
    # The enclosing prepare transaction owns exact pre-state restoration.
    # Performing a second ad-hoc switch here caused the r7 retry/restart loop.
    # Preserve failure evidence and return non-zero so the outer ERR trap runs
    # the single idempotent release rollback.
    if [[ "${restarted}" -eq 1 ]] && wait_for_server; then
        capture_snapshot "${ROLLBACK}" || true
    fi
    exit "${status}"
}

trap 'rollback $?' ERR
trap 'rollback 129' HUP
trap 'rollback 130' INT
trap 'rollback 143' TERM

test "$(readlink -f "${ACTIVE}")" = "${NEW_RELEASE}"
test -x "${ACTIVE}/herdr"
test -f "${DROPIN}"
test "$(<"${TRANSACTION}/backup-complete")" = BACKUP_COMPLETE
test "$(<"${TRANSACTION}/phase")" = preparing
for unit in tendwired.service herdres.service herdres-gateway.service; do
    if systemctl --user is-active --quiet "${unit}"; then
        false
    fi
done

install -d -m 700 "${TRANSACTION}"
capture_snapshot "${PRE}"
old_pid="$(systemctl --user show --value -p MainPID herdr-server.service)"
test "${old_pid}" -gt 1

restarted=1
systemctl --user restart herdr-server.service
wait_for_server
new_pid="$(systemctl --user show --value -p MainPID herdr-server.service)"
test "${new_pid}" -gt 1
test "${new_pid}" != "${old_pid}"
test "$(readlink -f "/proc/${new_pid}/exe")" = "$(readlink -f "${ACTIVE}/herdr")"
tr '\0' '\n' <"/proc/${new_pid}/environ" \
    | grep -Fx 'PATH=/home/smith/.local/share/acp-adapters/codex-acp-7cb0524624f2e730f48c3dac9b547ca130964ae9/bin:/home/smith/.local/bin:/usr/local/bin:/usr/bin:/bin' \
    >/dev/null
capture_snapshot "${POST}"
compare_topology "${PRE}" "${POST}"

printf '%s\n' "${new_pid}" >"${TRANSACTION}/herdr-restart-pid"
chmod 600 "${TRANSACTION}/herdr-restart-pid"
trap - ERR HUP INT TERM
printf 'HERDR_R8_RESTART_OK old=%s new=%s\n' "${old_pid}" "${new_pid}"
