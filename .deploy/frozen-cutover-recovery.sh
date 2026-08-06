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
            FROZEN_ACP_CLEAN_ENV|HOME|USER|LOGNAME|PATH|PWD|SHLVL|_|XDG_RUNTIME_DIR|DBUS_SESSION_BUS_ADDRESS|NOTIFY_SOCKET) ;;
            *) return 1 ;;
        esac
    done < <(compgen -e)
}
if ! clean_environment_valid; then
    exec /usr/bin/env -i \
        HOME=/home/smith USER=smith LOGNAME=smith PATH=/usr/bin:/bin \
        XDG_RUNTIME_DIR=/run/user/1000 \
        DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/1000/bus \
        NOTIFY_SOCKET="${NOTIFY_SOCKET:-}" \
        FROZEN_ACP_CLEAN_ENV=1 \
        /bin/bash -p "${BASH_SOURCE[0]}" "$@"
fi
unset -f clean_environment_valid
unset FROZEN_ACP_CLEAN_ENV
export PATH=/usr/bin:/bin
unset BASH_ENV ENV CDPATH GLOBIGNORE PYTHONHOME PYTHONPATH TAR_OPTIONS

readonly TRANSACTION_ROOT=/home/smith/.local/state/acp-cutover/frozen-7446533-6c0d0f5
readonly PHASE_FILE=${TRANSACTION_ROOT}/phase
readonly RESUME_AUTH=${TRANSACTION_ROOT}/forward-resume-authorized
readonly MONITOR_OWNER=${TRANSACTION_ROOT}/validation-monitor-owner
readonly MONITOR_HEARTBEAT=${TRANSACTION_ROOT}/validation-monitor-heartbeat
readonly CUTOVER_LOCK=/home/smith/.local/state/acp-cutover/frozen-7446533-6c0d0f5.lock

phase_value() {
    local value=absent
    if [[ -f "${PHASE_FILE}" && ! -L "${PHASE_FILE}" ]]; then
        IFS= read -r value <"${PHASE_FILE}"
    fi
    printf '%s' "${value}"
}

committing_owner_is_live() {
    local tag=""
    local owner_pid=""
    local owner_start=""
    local extra=""
    local current_start=""
    [[ -f "${RESUME_AUTH}" && ! -L "${RESUME_AUTH}" ]]
    [[ "$(stat -c '%u:%a' "${RESUME_AUTH}")" = "$(id -u):600" ]]
    IFS=' ' read -r tag owner_pid owner_start extra <"${RESUME_AUTH}"
    [[ "${tag}" = RESUME_FROZEN_ACP_CUTOVER && -z "${extra}" ]]
    [[ "${owner_pid}" =~ ^[1-9][0-9]*$ && "${owner_start}" =~ ^[1-9][0-9]*$ ]]
    [[ -r "/proc/${owner_pid}/stat" ]]
    [[ "$(stat -c '%u' "/proc/${owner_pid}")" = "$(id -u)" ]]
    current_start="$(awk '{print $22}' "/proc/${owner_pid}/stat")"
    [[ "${current_start}" = "${owner_start}" ]]
    exec 9>"${CUTOVER_LOCK}"
    if flock -n 9; then
        flock -u 9
        exec 9>&-
        return 1
    fi
    exec 9>&-
}

validation_monitor_is_live() {
    local tag=""
    local owner_pid=""
    local owner_start=""
    local extra=""
    local current_start=""
    local heartbeat_tag=""
    local heartbeat_pid=""
    local heartbeat_index=""
    local heartbeat_extra=""
    local heartbeat_age=""
    [[ -f "${MONITOR_OWNER}" && ! -L "${MONITOR_OWNER}" ]]
    [[ "$(stat -c '%u:%a' "${MONITOR_OWNER}")" = "$(id -u):600" ]]
    IFS=' ' read -r tag owner_pid owner_start extra <"${MONITOR_OWNER}"
    [[ "${tag}" = FROZEN_ACP_VALIDATION_MONITOR && -z "${extra}" ]]
    [[ "${owner_pid}" =~ ^[1-9][0-9]*$ && "${owner_start}" =~ ^[1-9][0-9]*$ ]]
    [[ -r "/proc/${owner_pid}/stat" ]]
    [[ "$(stat -c '%u' "/proc/${owner_pid}")" = "$(id -u)" ]]
    current_start="$(awk '{print $22}' "/proc/${owner_pid}/stat")"
    [[ "${current_start}" = "${owner_start}" ]]
    [[ -f "${MONITOR_HEARTBEAT}" && ! -L "${MONITOR_HEARTBEAT}" ]]
    [[ "$(stat -c '%u:%a' "${MONITOR_HEARTBEAT}")" = "$(id -u):600" ]]
    IFS=' ' read -r heartbeat_tag heartbeat_pid heartbeat_index heartbeat_extra \
        <"${MONITOR_HEARTBEAT}"
    [[ "${heartbeat_tag}" = FROZEN_ACP_VALIDATION_HEARTBEAT ]]
    [[ "${heartbeat_pid}" = "${owner_pid}" && -z "${heartbeat_extra}" ]]
    [[ "${heartbeat_index}" =~ ^-1$|^[0-9]+$ ]]
    heartbeat_age=$(( $(date +%s) - $(stat -c '%Y' "${MONITOR_HEARTBEAT}") ))
    [[ "${heartbeat_age}" -ge 0 && "${heartbeat_age}" -le 60 ]]
}

phase_is_authorized() {
    case "$(phase_value)" in
        prepared|validation_passed|deployed|rolled_back)
            return 0
            ;;
        committing|provisional)
            committing_owner_is_live
            ;;
        validating)
            validation_monitor_is_live
            ;;
        *)
            return 1
            ;;
    esac
}

stop_targets() {
    systemctl --user stop --no-block \
        herdres-gateway.service herdres.service tendwired.service || true
}

if ! phase_is_authorized; then
    stop_targets
    printf '%s\n' \
        'Frozen ACP cutover is not authorized; target services were stopped.' >&2
    exit 1
fi

if [[ -n "${NOTIFY_SOCKET:-}" ]]; then
    /usr/bin/systemd-notify --ready --status="Frozen ACP cutover guard active"
fi

while true; do
    if ! phase_is_authorized; then
        stop_targets
        printf '%s\n' \
            'Frozen ACP cutover authorization was lost; target services were stopped.' >&2
        exit 1
    fi
    sleep 1
done
