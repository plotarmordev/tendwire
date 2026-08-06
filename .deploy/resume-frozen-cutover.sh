#!/bin/bash -p
set -Eeuo pipefail
clean_environment_valid() {
    [[ "${FROZEN_ACP_CLEAN_ENV:-}" = 1 \
        && "${HOME:-}" = /home/smith && "${USER:-}" = smith \
        && "${LOGNAME:-}" = smith && "${PATH:-}" = /usr/bin:/bin \
        && "${XDG_RUNTIME_DIR:-}" = /run/user/1000 \
        && "${DBUS_SESSION_BUS_ADDRESS:-}" = unix:path=/run/user/1000/bus ]] || return 1
    local name
    while IFS= read -r name; do
        case "${name}" in
            FROZEN_ACP_CLEAN_ENV|HOME|USER|LOGNAME|PATH|PWD|SHLVL|_|XDG_RUNTIME_DIR|DBUS_SESSION_BUS_ADDRESS) ;;
            *) return 1 ;;
        esac
    done < <(compgen -e)
}
if ! clean_environment_valid; then
    exec /usr/bin/env -i \
        HOME=/home/smith USER=smith LOGNAME=smith PATH=/usr/bin:/bin \
        XDG_RUNTIME_DIR=/run/user/1000 \
        DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/1000/bus \
        FROZEN_ACP_CLEAN_ENV=1 \
        /bin/bash -p "${BASH_SOURCE[0]}" "$@"
fi
unset -f clean_environment_valid
unset FROZEN_ACP_CLEAN_ENV
export PATH=/usr/bin:/bin
unset BASH_ENV ENV CDPATH GLOBIGNORE PYTHONHOME PYTHONPATH TAR_OPTIONS

readonly RELEASE_ID=9026d9bc-7446533-6c0d0f5
readonly NEW_RELEASE=/home/smith/.local/share/acp-runtime/releases/${RELEASE_ID}
readonly RELEASE_MANIFEST=/home/smith/.local/share/acp-runtime/manifests/${RELEASE_ID}.json
readonly ACTIVE_LINK=/home/smith/.local/share/acp-runtime/active
readonly TENDWIRE_RUNTIME=/home/smith/.local/share/tendwire-runtime/acp-7446533
readonly TENDWIRE_SOCKET=/home/smith/.local/share/tendwire/tendwire.sock
readonly HERDRES_STATE=/home/smith/.local/share/herdres/candidates/6c0d0f5/state.json
readonly HERDRES_INGRESS=/home/smith/.local/share/herdres/candidates/6c0d0f5/ingress.db
readonly TRANSACTION_ROOT=/home/smith/.local/state/acp-cutover/frozen-7446533-6c0d0f5
readonly PHASE_FILE=${TRANSACTION_ROOT}/phase
readonly RESET_STARTED=${TRANSACTION_ROOT}/telegram-topic-reset-started.json
readonly RESUME_AUTH=${TRANSACTION_ROOT}/forward-resume-authorized
readonly TOPIC_TOOL=${NEW_RELEASE}/reset-telegram-topics
readonly TOPIC_PYTHON=/home/smith/.local/share/uv/tools/contexto/bin/python
readonly USER_UNITS=/home/smith/.config/systemd/user
readonly LIVE_MONITOR_UNIT=${USER_UNITS}/acp-frozen-live-monitor.service
readonly CUTOVER_LOCK=/home/smith/.local/state/acp-cutover/frozen-7446533-6c0d0f5.lock
readonly EXPECTED_ACP_WORKER_COUNT=1
readonly EXPECTED_ACP_ADAPTER=codex

fail_stopped() {
    local status="${1:-1}"
    trap - ERR HUP INT TERM
    systemctl --user stop acp-frozen-live-monitor.service || true
    systemctl --user stop herdres-gateway.service herdres.service tendwired.service || true
    systemctl --user stop acp-frozen-release-recovery.service || true
    rm -f -- "${RESUME_AUTH}"
    printf '%s\n' 'Forward recovery failed; target services remain stopped and the transaction remains committing.' >&2
    exit "${status}"
}

trap 'fail_stopped $?' ERR
trap 'fail_stopped 129' HUP
trap 'fail_stopped 130' INT
trap 'fail_stopped 143' TERM

phase_write() {
    local temporary="${PHASE_FILE}.tmp.$$"
    printf '%s\n' "$1" >"${temporary}"
    chmod 0600 "${temporary}"
    sync -d "${temporary}"
    mv -T -- "${temporary}" "${PHASE_FILE}"
    sync -d "${TRANSACTION_ROOT}"
}

wait_tendwire() {
    local ready=0
    for _attempt in $(seq 1 240); do
        if [[ -S "${TENDWIRE_SOCKET}" ]] && \
            "${TENDWIRE_RUNTIME}/bin/python" -B -I - \
                "${TENDWIRE_SOCKET}" "${EXPECTED_ACP_WORKER_COUNT}" \
                "${EXPECTED_ACP_ADAPTER}" <<'PY'
import sys
from tendwire.daemon_api import DaemonAPIClient

client = DaemonAPIClient(sys.argv[1], timeout_seconds=5)
health_response = client.request("health.get", {})
snapshot_response = client.request("snapshot.get", {})
health = (health_response.get("result") or {}).get("acp") or {}
snapshot = snapshot_response.get("result") or {}
workers = snapshot.get("workers")
expected_count = int(sys.argv[2])
expected_adapter = sys.argv[3]
valid = (
    health_response.get("ok") is True
    and snapshot_response.get("ok") is True
    and health.get("healthy") is True
    and health.get("state") == "running"
    and health.get("failure_type") is None
    and health.get("worker_count") == expected_count
    and isinstance(workers, list)
    and len(workers) == expected_count
    and sorted(row.get("name") for row in workers if isinstance(row, dict))
        == [expected_adapter] * expected_count
    and all(
        isinstance(row, dict)
        and row.get("status") in {"active", "idle", "waiting", "blocked"}
        for row in workers
    )
)
raise SystemExit(0 if valid else 1)
PY
        then
            ready=1
            break
        fi
        sleep 1
    done
    test "${ready}" -eq 1
}

wait_herdres() {
    local ready=0
    for _attempt in $(seq 1 180); do
        if PYTHONPATH="${NEW_RELEASE}/herdres" /usr/bin/python3 -s - \
            "${HERDRES_STATE}" <<'PY'
import sys
from pathlib import Path
from herdres_connector.state import lifecycle_barrier

result = lifecycle_barrier(Path(sys.argv[1]))
raise SystemExit(0 if result.ok and result.live_workers == 1 and result.ready_routes == 1 else 1)
PY
        then
            ready=1
            break
        fi
        sleep 2
    done
    test "${ready}" -eq 1
}

wait_gateway() {
    local ready=0
    local first_pid=""
    for _attempt in $(seq 1 120); do
        if systemctl --user is-active --quiet herdres-gateway.service && \
            /usr/bin/python3 -I - "${HERDRES_INGRESS}" <<'PY'
import sqlite3
import sys

connection = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
try:
    cursors = {row[0] for row in connection.execute("SELECT receiver_id FROM receiver_cursors")}
    requests = int(connection.execute("SELECT COUNT(*) FROM requests").fetchone()[0])
finally:
    connection.close()
expected = {"manager", "managed-codex", "managed-omp", "managed-kimi"}
raise SystemExit(0 if cursors == expected and requests == 0 else 1)
PY
        then
            current_pid="$(systemctl --user show --value -p MainPID herdres-gateway.service)"
            if [[ "${current_pid}" =~ ^[1-9][0-9]*$ ]]; then
                if [[ -z "${first_pid}" ]]; then
                    first_pid="${current_pid}"
                elif [[ "${current_pid}" = "${first_pid}" ]]; then
                    ready=1
                    break
                else
                    first_pid="${current_pid}"
                fi
            fi
        fi
        sleep 1
    done
    test "${ready}" -eq 1
}

exec 9>"${CUTOVER_LOCK}"
flock -n 9
cutover_owner_start="$(awk '{print $22}' "/proc/${BASHPID}/stat")"
[[ "${cutover_owner_start}" =~ ^[1-9][0-9]*$ ]]
test "$(<"${PHASE_FILE}")" = committing
test -f "${RESET_STARTED}"
test "$(stat -c '%u:%a' "${RESET_STARTED}")" = "$(id -u):600"
test "$(readlink -f "${ACTIVE_LINK}")" = "${NEW_RELEASE}"
test "$(systemctl --user show --value -p MainPID herdr-server.service)" = \
    "$(<"${TRANSACTION_ROOT}/old-herdr-pid")"
for unit in tendwired herdres herdres-gateway; do
    cmp -s -- "${NEW_RELEASE}/systemd/${unit}-99.conf" \
        "${USER_UNITS}/${unit}.service.d/99-frozen-acp-release.conf"
done
cmp -s -- "${NEW_RELEASE}/systemd/acp-frozen-live-monitor.service" \
    "${LIVE_MONITOR_UNIT}"
"${NEW_RELEASE}/validate-frozen-release" \
    --runtime "${TENDWIRE_RUNTIME}" --release "${NEW_RELEASE}" \
    --manifest "${RELEASE_MANIFEST}" --verify

rm -f -- "${RESUME_AUTH}"
printf '%s %s %s\n' RESUME_FROZEN_ACP_CUTOVER "${BASHPID}" \
    "${cutover_owner_start}" >"${RESUME_AUTH}.tmp.$$"
chmod 0600 "${RESUME_AUTH}.tmp.$$"
sync -d "${RESUME_AUTH}.tmp.$$"
mv -T -- "${RESUME_AUTH}.tmp.$$" "${RESUME_AUTH}"
sync -d "${TRANSACTION_ROOT}"
systemctl --user reset-failed acp-frozen-release-recovery.service
systemctl --user start acp-frozen-release-recovery.service
systemctl --user is-active --quiet acp-frozen-release-recovery.service

systemctl --user stop herdres-gateway.service herdres.service tendwired.service
systemctl --user start tendwired.service
wait_tendwire
"${TOPIC_PYTHON}" -I "${TOPIC_TOOL}" --apply
if [[ ! -e "${HERDRES_STATE}" && ! -L "${HERDRES_STATE}" && \
      ! -e "${HERDRES_INGRESS}" && ! -L "${HERDRES_INGRESS}" ]]; then
    "${TOPIC_PYTHON}" -I "${TOPIC_TOOL}" --gate-presenter
else
    "${TOPIC_PYTHON}" -I "${TOPIC_TOOL}" --gate-existing-presenter
fi
systemctl --user start herdres.service
wait_herdres
"${TOPIC_PYTHON}" -I "${TOPIC_TOOL}" --verify-presenter
systemctl --user start herdres-gateway.service
wait_gateway
"${TOPIC_PYTHON}" -I "${TOPIC_TOOL}" --verify-gateway
for unit in tendwired.service herdres.service herdres-gateway.service; do
    systemctl --user is-active --quiet "${unit}"
    test "$(systemctl --user show --value -p NRestarts "${unit}")" = 0
done
phase_write provisional
systemctl --user reset-failed acp-frozen-live-monitor.service
systemctl --user start acp-frozen-live-monitor.service
monitor_ready=0
for _attempt in $(seq 1 30); do
    if [[ "$(<"${PHASE_FILE}")" = validating ]] && \
        systemctl --user is-active --quiet acp-frozen-live-monitor.service; then
        monitor_ready=1
        break
    fi
    sleep 1
done
test "${monitor_ready}" -eq 1
rm -- "${RESUME_AUTH}"
sync -d "${TRANSACTION_ROOT}"
trap - ERR HUP INT TERM
printf '%s\n' 'Forward recovery completed; candidate services are validating.'
