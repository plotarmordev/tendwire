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

readonly RELEASE_ID=9026d9bc-7446533-3659994
readonly NEW_RELEASE=/home/smith/.local/share/acp-runtime/releases/${RELEASE_ID}
readonly RELEASE_MANIFEST=/home/smith/.local/share/acp-runtime/manifests/${RELEASE_ID}.json
readonly PREVIOUS_RELEASE=/home/smith/.local/share/acp-runtime/releases/9026d9bc-7446533-6c0d0f5
readonly PREVIOUS_RUNTIME=/home/smith/.local/share/tendwire-runtime/acp-7446533
readonly PREVIOUS_MANIFEST=/home/smith/.local/share/acp-runtime/manifests/9026d9bc-7446533-6c0d0f5.json
readonly ACTIVE_LINK=/home/smith/.local/share/acp-runtime/active
readonly HERDR_BINARY=/home/smith/.local/share/herdr-runtime/acp-9026d9bc/herdr
readonly TENDWIRE_RUNTIME=/home/smith/.local/share/tendwire-runtime/acp-7446533-3659994
readonly TENDWIRE_SOCKET=/home/smith/.local/share/tendwire/tendwire.sock
readonly TENDWIRE_DB=/home/smith/.local/share/tendwire/candidates/7446533/tendwire.db
readonly HERDRES_STATE=/home/smith/.local/share/herdres/candidates/3659994/state.json
readonly HERDRES_INGRESS=/home/smith/.local/share/herdres/candidates/3659994/ingress.db
readonly TRANSACTION_ROOT=/home/smith/.local/state/acp-cutover/frozen-7446533-6c0d0f5
readonly PHASE_FILE=${TRANSACTION_ROOT}/phase
readonly RESET_STARTED=${TRANSACTION_ROOT}/telegram-topic-reset-started.json
readonly RESUME_AUTH=${TRANSACTION_ROOT}/forward-resume-authorized
readonly MONITOR_OWNER=${TRANSACTION_ROOT}/validation-monitor-owner
readonly MONITOR_HEARTBEAT=${TRANSACTION_ROOT}/validation-monitor-heartbeat
readonly MONITOR_EVIDENCE=${TRANSACTION_ROOT}/one-hour-monitor.json
readonly TOPIC_TOOL=${NEW_RELEASE}/reset-telegram-topics
readonly TOPIC_PYTHON=/home/smith/.local/share/uv/tools/contexto/bin/python
readonly USER_UNITS=/home/smith/.config/systemd/user
readonly LIVE_MONITOR_UNIT=${USER_UNITS}/acp-frozen-live-monitor.service
readonly RECOVERY_UNIT=${USER_UNITS}/acp-frozen-release-recovery.service
readonly LEGACY_RECOVERY_OVERRIDE=${USER_UNITS}/acp-cutover-recovery.service.d/99-frozen-acp-release.conf
readonly PRIVATE_ENV=/home/smith/.config/herdres/frozen-7446533-3659994.env
readonly PREVIOUS_PRIVATE_ENV=/home/smith/.config/herdres/frozen-7446533-6c0d0f5.env
readonly PREVIOUS_VALIDATION=${TRANSACTION_ROOT}/release-validation.6c0d0f5.json
readonly PREVIOUS_VALIDATION_SHA256=cddd693567bec5152248896eb518e989ec27b4ba1f8325b359d1f6349ec214f1
readonly CUTOVER_LOCK=/home/smith/.local/state/acp-cutover/frozen-7446533-6c0d0f5.lock
readonly EXPECTED_ACP_WORKER_COUNT=1
readonly EXPECTED_ACP_ADAPTER=codex

fail_stopped() {
    local status="${1:-1}"
    trap - ERR HUP INT TERM
    systemctl --user stop acp-frozen-live-monitor.service || true
    systemctl --user stop herdres-gateway.service herdres.service tendwired.service || true
    systemctl --user stop acp-frozen-release-recovery.service || true
    systemctl --user stop acp-cutover-recovery.service || true
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

assert_previous_validation_file() {
    local path="$1"
    local digest extra
    test -f "${path}"
    test ! -L "${path}"
    test "$(stat -c '%u:%a:%h' "${path}")" = "$(id -u):600:1"
    read -r digest extra < <(sha256sum -- "${path}")
    test -n "${extra}"
    test "${digest}" = "${PREVIOUS_VALIDATION_SHA256}"
}

assert_working_directory() {
    local unit="$1"
    local expected="$2"
    local actual
    actual="$(systemctl --user show --value -p WorkingDirectory "${unit}")"
    [[ "${actual}" = "${expected}" || "${actual}" = "!${expected}" ]]
}

assert_dependency() {
    local unit="$1"
    local property="$2"
    local dependency="$3"
    local actual
    actual="$(systemctl --user show --value -p "${property}" "${unit}")"
    [[ " ${actual} " == *" ${dependency} "* ]]
}

assert_unit_quiescent() {
    local unit="$1"
    local active_state job main_pid control_pid
    active_state="$(systemctl --user show --value -p ActiveState "${unit}")"
    job="$(systemctl --user show --value -p Job "${unit}")"
    main_pid="$(systemctl --user show --value -p MainPID "${unit}")"
    control_pid="$(systemctl --user show --value -p ControlPID "${unit}")"
    [[ "${active_state}" = inactive || "${active_state}" = failed ]]
    test -z "${job}"
    test "${main_pid}" = 0
    test "${control_pid}" = 0
}

assert_unit_running() {
    local unit="$1"
    local active_state job main_pid
    active_state="$(systemctl --user show --value -p ActiveState "${unit}")"
    job="$(systemctl --user show --value -p Job "${unit}")"
    main_pid="$(systemctl --user show --value -p MainPID "${unit}")"
    test "${active_state}" = active
    test -z "${job}"
    [[ "${main_pid}" =~ ^[1-9][0-9]*$ ]]
}

assert_exec() {
    local unit="$1"
    local expected_path="$2"
    local expected_argv="$3"
    local actual
    actual="$(systemctl --user show --value -p ExecStart "${unit}")"
    [[ "${actual}" == "{ path=${expected_path} ; argv[]=${expected_argv} ; ignore_errors=no ; "* ]]
}

assert_environment_assignment() {
    local environment="$1"
    local name="$2"
    local expected="$3"
    local assignment
    local matches=0
    local assignments=()
    read -r -a assignments <<<"${environment}"
    for assignment in "${assignments[@]}"; do
        case "${assignment}" in
            "${name}="*)
                matches=$((matches + 1))
                test "${assignment}" = "${name}=${expected}"
                ;;
        esac
    done
    test "${matches}" -eq 1
}

assert_effective_units() {
    local tw_environment
    for unit in tendwired.service herdres.service herdres-gateway.service; do
        test -z "$(systemctl --user show --value -p ExecStartPre "${unit}")"
        test -z "$(systemctl --user show --value -p ExecStartPost "${unit}")"
        assert_dependency "${unit}" Requires acp-frozen-release-recovery.service
        assert_dependency "${unit}" After acp-frozen-release-recovery.service
    done
    assert_dependency tendwired.service Requires herdr-server.service
    assert_dependency tendwired.service After herdr-server.service
    assert_dependency herdres.service Requires tendwired.service
    assert_dependency herdres.service After tendwired.service
    assert_dependency herdres-gateway.service Requires tendwired.service
    assert_dependency herdres-gateway.service After tendwired.service
    assert_dependency herdres-gateway.service Requires herdres.service
    assert_dependency herdres-gateway.service After herdres.service
    for unit in acp-frozen-release-recovery.service \
        acp-frozen-live-monitor.service acp-cutover-recovery.service
    do
        test -z "$(systemctl --user show --value -p ExecStartPre "${unit}")"
        test -z "$(systemctl --user show --value -p ExecStartPost "${unit}")"
    done
    for unit in tendwired.service herdres.service herdres-gateway.service; do
        assert_dependency acp-frozen-release-recovery.service Before "${unit}"
        assert_dependency acp-frozen-live-monitor.service Requires "${unit}"
        assert_dependency acp-frozen-live-monitor.service After "${unit}"
    done
    assert_dependency acp-frozen-live-monitor.service Requires herdr-server.service
    assert_dependency acp-frozen-live-monitor.service After herdr-server.service
    tw_environment="$(systemctl --user show --value -p Environment tendwired.service)"
    assert_exec tendwired.service "${TENDWIRE_RUNTIME}/bin/python" \
        "${TENDWIRE_RUNTIME}/bin/python -B -I -m tendwire.cli daemon --db-path ${TENDWIRE_DB} --socket-path ${TENDWIRE_SOCKET}"
    assert_exec herdres.service /usr/bin/python3 \
        "/usr/bin/python3 -E -s ${NEW_RELEASE}/herdres/herdres sync --loop 5"
    assert_exec herdres-gateway.service /usr/bin/python3 \
        "/usr/bin/python3 -E -s ${NEW_RELEASE}/herdres/herdres-gateway"
    assert_exec acp-frozen-release-recovery.service \
        "${NEW_RELEASE}/frozen-cutover-recovery" \
        "${NEW_RELEASE}/frozen-cutover-recovery"
    assert_exec acp-frozen-live-monitor.service \
        "${NEW_RELEASE}/monitor-one-hour" "${NEW_RELEASE}/monitor-one-hour"
    assert_exec acp-cutover-recovery.service \
        "${NEW_RELEASE}/frozen-cutover-recovery" \
        "${NEW_RELEASE}/frozen-cutover-recovery"
    assert_environment_assignment "${tw_environment}" TENDWIRE_HERDR_BIN "${HERDR_BINARY}"
    assert_environment_assignment "${tw_environment}" PYTHONSAFEPATH 1
    assert_environment_assignment "${tw_environment}" PYTHONPATH ""
    test "$(systemctl --user show --value -p Type acp-frozen-release-recovery.service)" = notify
    test "$(systemctl --user show --value -p NotifyAccess acp-frozen-release-recovery.service)" = all
    test "$(systemctl --user show --value -p Type acp-frozen-live-monitor.service)" = simple
    test "$(systemctl --user show --value -p Restart acp-frozen-live-monitor.service)" = no
    assert_working_directory tendwired.service "${TENDWIRE_RUNTIME}"
    assert_working_directory herdres.service "${NEW_RELEASE}/herdres"
    assert_working_directory herdres-gateway.service "${NEW_RELEASE}/herdres"
    test "$(systemctl --user show --value -p EnvironmentFiles herdres.service)" = \
        "${PRIVATE_ENV} (ignore_errors=no)"
    test "$(systemctl --user show --value -p EnvironmentFiles herdres-gateway.service)" = \
        "${PRIVATE_ENV} (ignore_errors=no)"
}

switch_release() {
    local temporary
    temporary="$(dirname "${ACTIVE_LINK}")/.active.forward.$$.${RELEASE_ID}"
    [[ ! -e "${temporary}" && ! -L "${temporary}" ]]
    ln -s -- "${NEW_RELEASE}" "${temporary}"
    mv -Tf -- "${temporary}" "${ACTIVE_LINK}"
    sync -d "$(dirname "${ACTIVE_LINK}")"
}

publish_unit() {
    local source="$1"
    local target="$2"
    local temporary
    temporary="$(dirname "${target}")/.${target##*/}.forward.$$.tmp"
    [[ ! -e "${temporary}" && ! -L "${temporary}" ]]
    install -m 0644 "${source}" "${temporary}"
    sync "${temporary}"
    mv -T -- "${temporary}" "${target}"
    sync -d "$(dirname "${target}")"
}

prepare_private_env() {
    /usr/bin/python3 -I - \
        "${PREVIOUS_PRIVATE_ENV}" "${PRIVATE_ENV}" \
        "${HERDRES_STATE}" "${HERDRES_INGRESS}" <<'PY'
import os
import re
import stat
import sys
from pathlib import Path

source, target, state_path, ingress_path = map(Path, sys.argv[1:])

def secure_read(path: Path) -> bytes:
    named = path.lstat()
    if (
        not stat.S_ISREG(named.st_mode)
        or named.st_uid != os.geteuid()
        or named.st_nlink != 1
        or stat.S_IMODE(named.st_mode) != 0o600
        or named.st_size > 131_072
    ):
        raise RuntimeError("private environment ownership or mode is invalid")
    descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.geteuid()
            or opened.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) != 0o600
            or (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino)
            or opened.st_size > 131_072
        ):
            raise RuntimeError("private environment changed during open")
        chunks: list[bytes] = []
        length = 0
        while True:
            chunk = os.read(descriptor, min(65_536, 131_073 - length))
            if not chunk:
                after = os.fstat(descriptor)
                stable = (
                    after.st_dev, after.st_ino, after.st_mode, after.st_uid,
                    after.st_nlink, after.st_size, after.st_mtime_ns, after.st_ctime_ns,
                ) == (
                    opened.st_dev, opened.st_ino, opened.st_mode, opened.st_uid,
                    opened.st_nlink, opened.st_size, opened.st_mtime_ns, opened.st_ctime_ns,
                )
                if not stable:
                    raise RuntimeError("private environment changed during read")
                return b"".join(chunks)
            chunks.append(chunk)
            length += len(chunk)
            if length > 131_072:
                raise RuntimeError("private environment is too large")
    finally:
        os.close(descriptor)

def parse(body: bytes) -> tuple[list[str], dict[str, str]]:
    if len(body) > 131_072 or not body.endswith(b"\n"):
        raise RuntimeError("private environment is invalid")
    lines = body.decode("utf-8").splitlines()
    values: dict[str, str] = {}
    for line in lines:
        name, separator, value = line.partition("=")
        if (
            not separator
            or not re.fullmatch(r"[A-Z][A-Z0-9_]{0,63}", name)
            or name in values
            or any(character in value for character in "\r\n\0")
        ):
            raise RuntimeError("private environment is invalid")
        values[name] = value
    return lines, values

source_lines, source_values = parse(secure_read(source))
expected_keys = {
    "HERDRES_ENV_FILE", "HERDRES_INGRESS_PATH", "HERDRES_STATE_PATH",
    "HERDRES_TENDWIRE_MODE", "TENDWIRE_SOCKET_PATH", "TELEGRAM_CHAT_ID",
    "TELEGRAM_GENERAL_THREAD_ID", "TELEGRAM_OWNER_USER_IDS",
    "TELEGRAM_BOT_TOKEN", "TELEGRAM_BOT_USERNAME",
    "TELEGRAM_CODEX_BOT_TOKEN", "TELEGRAM_CODEX_BOT_USERNAME",
    "TELEGRAM_OMP_BOT_TOKEN", "TELEGRAM_OMP_BOT_USERNAME",
    "TELEGRAM_KIMI_BOT_TOKEN", "TELEGRAM_KIMI_BOT_USERNAME",
}
if set(source_values) != expected_keys:
    raise RuntimeError("previous private environment keys are invalid")
expected_source = {
    "HERDRES_ENV_FILE": str(source),
    "HERDRES_STATE_PATH": "/home/smith/.local/share/herdres/candidates/6c0d0f5/state.json",
    "HERDRES_INGRESS_PATH": "/home/smith/.local/share/herdres/candidates/6c0d0f5/ingress.db",
    "HERDRES_TENDWIRE_MODE": "source",
    "TENDWIRE_SOCKET_PATH": "/home/smith/.local/share/tendwire/tendwire.sock",
}
if any(source_values.get(key) != value for key, value in expected_source.items()):
    raise RuntimeError("previous private environment paths are invalid")
patterns = {
    "TELEGRAM_CHAT_ID": r"-?[0-9]{1,24}",
    "TELEGRAM_GENERAL_THREAD_ID": r"[0-9]{1,24}",
    "TELEGRAM_OWNER_USER_IDS": r"[0-9]{1,24}(?:,[0-9]{1,24})*",
}
for name, pattern in patterns.items():
    if re.fullmatch(pattern, source_values[name]) is None:
        raise RuntimeError("previous private environment routing is invalid")
token_names = [
    "TELEGRAM_BOT_TOKEN", "TELEGRAM_CODEX_BOT_TOKEN",
    "TELEGRAM_OMP_BOT_TOKEN", "TELEGRAM_KIMI_BOT_TOKEN",
]
username_names = [
    "TELEGRAM_BOT_USERNAME", "TELEGRAM_CODEX_BOT_USERNAME",
    "TELEGRAM_OMP_BOT_USERNAME", "TELEGRAM_KIMI_BOT_USERNAME",
]
tokens = [source_values[name] for name in token_names]
usernames = [source_values[name] for name in username_names]
if (
    any(re.fullmatch(r"[0-9]{6,20}:[A-Za-z0-9_-]{20,200}", value) is None for value in tokens)
    or any(re.fullmatch(r"[A-Za-z0-9_]{3,64}", value) is None for value in usernames)
    or len(set(tokens)) != len(tokens)
    or len({value.lower() for value in usernames}) != len(usernames)
):
    raise RuntimeError("previous private environment bot identities are invalid")
replacements = {
    "HERDRES_ENV_FILE": str(target),
    "HERDRES_STATE_PATH": str(state_path),
    "HERDRES_INGRESS_PATH": str(ingress_path),
}
expected_values = {**source_values, **replacements}
body = ("\n".join(
    f"{name}={replacements.get(name, value)}"
    for name, _, value in (line.partition("=") for line in source_lines)
) + "\n").encode()
if target.exists() or target.is_symlink():
    actual_body = secure_read(target)
    _, actual_values = parse(actual_body)
    if actual_values != expected_values or actual_body != body:
        raise RuntimeError("existing forward private environment changed")
else:
    descriptor = os.open(
        target,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
        0o600,
    )
    try:
        view = memoryview(body)
        while view:
            written = os.write(descriptor, view)
            if written < 1:
                raise RuntimeError("private environment write failed")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    directory = os.open(target.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
PY
}

rebind_forward_release() {
    local current
    local rotated=0
    current="$(readlink -f "${ACTIVE_LINK}")"
    if [[ "${current}" = "${NEW_RELEASE}" ]]; then
        assert_previous_validation_file "${PREVIOUS_VALIDATION}"
        test -f "${TRANSACTION_ROOT}/release-validation.json"
        test ! -L "${TRANSACTION_ROOT}/release-validation.json"
        test "$(stat -c '%u:%a:%h' "${TRANSACTION_ROOT}/release-validation.json")" = \
            "$(id -u):600:1"
        "${NEW_RELEASE}/validate-frozen-release" \
            --runtime "${TENDWIRE_RUNTIME}" --release "${NEW_RELEASE}" \
            --manifest "${RELEASE_MANIFEST}" --verify
        prepare_private_env
        systemctl --user daemon-reload
        return 0
    fi
    test "${current}" = "${PREVIOUS_RELEASE}"
    for unit in tendwired.service herdres.service herdres-gateway.service \
        acp-frozen-live-monitor.service acp-frozen-release-recovery.service \
        acp-cutover-recovery.service
    do
        assert_unit_quiescent "${unit}"
    done
    if [[ ! -e "${PREVIOUS_VALIDATION}" && ! -L "${PREVIOUS_VALIDATION}" ]]; then
        assert_previous_validation_file \
            "${TRANSACTION_ROOT}/release-validation.json"
        "${PREVIOUS_RELEASE}/validate-frozen-release" \
            --runtime "${PREVIOUS_RUNTIME}" --release "${PREVIOUS_RELEASE}" \
            --manifest "${PREVIOUS_MANIFEST}" --verify
        mv -T -- "${TRANSACTION_ROOT}/release-validation.json" "${PREVIOUS_VALIDATION}"
        sync -d "${TRANSACTION_ROOT}"
        assert_previous_validation_file "${PREVIOUS_VALIDATION}"
        rotated=1
    else
        assert_previous_validation_file "${PREVIOUS_VALIDATION}"
    fi
    if [[ -L "${TRANSACTION_ROOT}/release-validation.json" ]]; then
        return 1
    elif [[ ! -e "${TRANSACTION_ROOT}/release-validation.json" ]]; then
        if ! "${NEW_RELEASE}/validate-frozen-release" \
            --runtime "${TENDWIRE_RUNTIME}" --release "${NEW_RELEASE}" \
            --manifest "${RELEASE_MANIFEST}" --create
        then
            if [[ "${rotated}" -eq 1 ]]; then
                mv -T -- "${PREVIOUS_VALIDATION}" \
                    "${TRANSACTION_ROOT}/release-validation.json"
                sync -d "${TRANSACTION_ROOT}"
            fi
            return 1
        fi
    else
        "${NEW_RELEASE}/validate-frozen-release" \
            --runtime "${TENDWIRE_RUNTIME}" --release "${NEW_RELEASE}" \
            --manifest "${RELEASE_MANIFEST}" --verify
    fi
    test "$(stat -c '%u:%a:%h' "${TRANSACTION_ROOT}/release-validation.json")" = \
        "$(id -u):600:1"
    prepare_private_env
    install -d -m 0755 -- \
        "${USER_UNITS}/tendwired.service.d" \
        "${USER_UNITS}/herdres.service.d" \
        "${USER_UNITS}/herdres-gateway.service.d" \
        "${USER_UNITS}/acp-cutover-recovery.service.d"
    for unit in tendwired herdres herdres-gateway; do
        publish_unit "${NEW_RELEASE}/systemd/${unit}-99.conf" \
            "${USER_UNITS}/${unit}.service.d/99-frozen-acp-release.conf"
    done
    publish_unit "${NEW_RELEASE}/systemd/acp-frozen-live-monitor.service" \
        "${LIVE_MONITOR_UNIT}"
    publish_unit "${NEW_RELEASE}/systemd/acp-frozen-release-recovery.service" \
        "${RECOVERY_UNIT}"
    publish_unit "${NEW_RELEASE}/systemd/legacy-recovery-frozen-99.conf" \
        "${LEGACY_RECOVERY_OVERRIDE}"
    switch_release
    systemctl --user daemon-reload
    test "$(readlink -f "${ACTIVE_LINK}")" = "${NEW_RELEASE}"
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

assert_active_validation() {
    local owner_tag owner_pid owner_start owner_extra current_start
    local unit unit_pid unit_start validation_snapshot_state
    test "$(readlink -f "${ACTIVE_LINK}")" = "${NEW_RELEASE}"
    assert_previous_validation_file "${PREVIOUS_VALIDATION}"
    test -f "${TRANSACTION_ROOT}/release-validation.json"
    test ! -L "${TRANSACTION_ROOT}/release-validation.json"
    test "$(stat -c '%u:%a:%h' "${TRANSACTION_ROOT}/release-validation.json")" = \
        "$(id -u):600:1"
    "${NEW_RELEASE}/validate-frozen-release" \
        --runtime "${TENDWIRE_RUNTIME}" --release "${NEW_RELEASE}" \
        --manifest "${RELEASE_MANIFEST}" --verify
    test -f "${PRIVATE_ENV}"
    test ! -L "${PRIVATE_ENV}"
    prepare_private_env
    for unit in tendwired herdres herdres-gateway; do
        cmp -s -- "${NEW_RELEASE}/systemd/${unit}-99.conf" \
            "${USER_UNITS}/${unit}.service.d/99-frozen-acp-release.conf"
    done
    cmp -s -- "${NEW_RELEASE}/systemd/acp-frozen-live-monitor.service" \
        "${LIVE_MONITOR_UNIT}"
    cmp -s -- "${NEW_RELEASE}/systemd/acp-frozen-release-recovery.service" \
        "${RECOVERY_UNIT}"
    cmp -s -- "${NEW_RELEASE}/systemd/legacy-recovery-frozen-99.conf" \
        "${LEGACY_RECOVERY_OVERRIDE}"
    assert_effective_units
    for unit in herdr-server.service tendwired.service herdres.service \
        herdres-gateway.service acp-frozen-release-recovery.service \
        acp-frozen-live-monitor.service
    do
        assert_unit_running "${unit}"
    done
    test "$(systemctl --user show --value -p MainPID herdr-server.service)" = \
        "$(<"${TRANSACTION_ROOT}/old-herdr-pid")"
    for unit in herdr-server.service tendwired.service herdres.service \
        herdres-gateway.service
    do
        test "$(systemctl --user show --value -p NRestarts "${unit}")" = 0
    done
    test -f "${MONITOR_OWNER}"
    test ! -L "${MONITOR_OWNER}"
    test "$(stat -c '%u:%a:%h' "${MONITOR_OWNER}")" = "$(id -u):600:1"
    IFS=' ' read -r owner_tag owner_pid owner_start owner_extra <"${MONITOR_OWNER}"
    test "${owner_tag}" = FROZEN_ACP_VALIDATION_MONITOR
    test -z "${owner_extra}"
    [[ "${owner_pid}" =~ ^[1-9][0-9]*$ && "${owner_start}" =~ ^[1-9][0-9]*$ ]]
    test "${owner_pid}" = \
        "$(systemctl --user show --value -p MainPID acp-frozen-live-monitor.service)"
    test -r "/proc/${owner_pid}/stat"
    test "$(stat -c '%u' "/proc/${owner_pid}")" = "$(id -u)"
    current_start="$(awk '{print $22}' "/proc/${owner_pid}/stat")"
    test "${current_start}" = "${owner_start}"
    for unit in tendwired.service herdres.service herdres-gateway.service; do
        unit_pid="$(systemctl --user show --value -p MainPID "${unit}")"
        test -r "/proc/${unit_pid}/stat"
        test "$(stat -c '%u' "/proc/${unit_pid}")" = "$(id -u)"
        unit_start="$(awk '{print $22}' "/proc/${unit_pid}/stat")"
        [[ "${unit_start}" =~ ^[1-9][0-9]*$ ]]
        test "${unit_start}" -lt "${owner_start}"
    done
    assert_unit_quiescent acp-cutover-recovery.service
    validation_snapshot_state="$(
        /usr/bin/python3 -I - \
            "${MONITOR_HEARTBEAT}" "${MONITOR_EVIDENCE}" \
            "${owner_pid}" "${RELEASE_ID}" <<'PY'
import json
import os
import stat
import sys
import time
from pathlib import Path

heartbeat_path = Path(sys.argv[1])
evidence_path = Path(sys.argv[2])
owner_pid = int(sys.argv[3])
release_id = sys.argv[4]


class SnapshotAdvanced(RuntimeError):
    pass


def secure_read(path: Path, maximum: int) -> tuple[bytes, tuple[int, ...]]:
    try:
        named = path.lstat()
    except FileNotFoundError as error:
        raise SnapshotAdvanced("monitor publication advanced") from error
    if (
        not stat.S_ISREG(named.st_mode)
        or named.st_uid != os.geteuid()
        or named.st_nlink != 1
        or stat.S_IMODE(named.st_mode) != 0o600
        or named.st_size > maximum
    ):
        raise RuntimeError("monitor publication ownership or mode is invalid")
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    except FileNotFoundError as error:
        raise SnapshotAdvanced("monitor publication advanced") from error
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.geteuid()
            or opened.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) != 0o600
            or opened.st_size > maximum
        ):
            raise RuntimeError("monitor publication ownership or mode changed")
        if (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino):
            raise SnapshotAdvanced("monitor publication advanced")
        body = bytearray()
        while True:
            chunk = os.read(descriptor, min(65_536, maximum + 1 - len(body)))
            if not chunk:
                break
            body.extend(chunk)
            if len(body) > maximum:
                raise RuntimeError("monitor publication is too large")
        after = os.fstat(descriptor)
        identity = (
            after.st_dev, after.st_ino, after.st_mode, after.st_uid,
            after.st_nlink, after.st_size, after.st_mtime_ns, after.st_ctime_ns,
        )
        opened_identity = (
            opened.st_dev, opened.st_ino, opened.st_mode, opened.st_uid,
            opened.st_nlink, opened.st_size, opened.st_mtime_ns, opened.st_ctime_ns,
        )
        if identity != opened_identity:
            raise SnapshotAdvanced("monitor publication advanced")
        return bytes(body), identity
    finally:
        os.close(descriptor)


for attempt in range(100):
    try:
        heartbeat_body, heartbeat_identity = secure_read(heartbeat_path, 256)
        fields = heartbeat_body.decode("ascii").strip().split()
        if len(fields) != 3 or fields[0] != "FROZEN_ACP_VALIDATION_HEARTBEAT":
            raise RuntimeError("monitor heartbeat is invalid")
        heartbeat_pid = int(fields[1])
        heartbeat_index = int(fields[2])
        if heartbeat_pid != owner_pid:
            raise RuntimeError("monitor heartbeat owner is invalid")
        heartbeat_mtime = heartbeat_identity[6] / 1_000_000_000
        heartbeat_age = time.time() - heartbeat_mtime
        if heartbeat_age < 0 or heartbeat_age > 60:
            raise RuntimeError("monitor heartbeat is stale")
        if heartbeat_index == -1:
            heartbeat_body_after, heartbeat_identity_after = secure_read(
                heartbeat_path, 256
            )
            if (
                heartbeat_identity_after != heartbeat_identity
                or heartbeat_body_after != heartbeat_body
            ):
                raise SnapshotAdvanced("monitor heartbeat advanced")
            print("initializing")
            break
        if heartbeat_index < 0:
            raise RuntimeError("monitor heartbeat index is invalid")
        evidence_body, _evidence_identity = secure_read(evidence_path, 16_777_216)
        heartbeat_body_after, heartbeat_identity_after = secure_read(heartbeat_path, 256)
        if (
            heartbeat_identity_after != heartbeat_identity
            or heartbeat_body_after != heartbeat_body
        ):
            raise SnapshotAdvanced("monitor heartbeat advanced")
        evidence = json.loads(evidence_body)
        samples = evidence.get("samples")
        valid = (
            evidence.get("schema_version") == 1
            and evidence.get("release_id") == release_id
            and evidence.get("transaction_id") == "frozen-7446533-6c0d0f5"
            and evidence.get("tendwire_revision") == "7446533bb6fb2560a9a9dd871f638c4a6ccbb086"
            and evidence.get("herdres_revision") == "36599949daa64f68494d04f96a3bfee31904a804"
            and evidence.get("herdr_revision") == "9026d9bc5a12d9adc2d9f68ebdc564133e4098b4"
            and evidence.get("status") in {"running", "success"}
            and isinstance(samples, list)
            and len(samples) == evidence.get("sample_count")
            and len(samples) == heartbeat_index + 1
            and samples
            and samples[-1].get("index") == heartbeat_index
            and samples[-1].get("services")
                == {"active": 4, "stable": 4, "restart_count": 0}
            and samples[-1].get("health") == {"healthy": True, "worker_count": 1}
        )
        if not valid:
            raise SnapshotAdvanced("monitor publication pair is not yet consistent")
        print("sampled")
        break
    except SnapshotAdvanced:
        if attempt == 99:
            raise RuntimeError("monitor publication did not stabilize")
        time.sleep(0.05)
PY
    )"
    [[ "${validation_snapshot_state}" = initializing || \
        "${validation_snapshot_state}" = sampled ]]
}

exec 9>"${CUTOVER_LOCK}"
flock -n 9
cutover_owner_start="$(awk '{print $22}' "/proc/${BASHPID}/stat")"
[[ "${cutover_owner_start}" =~ ^[1-9][0-9]*$ ]]
current_phase="$(<"${PHASE_FILE}")"
if [[ "${current_phase}" = validating ]] && \
    [[ "$(systemctl --user show --value -p ActiveState acp-frozen-live-monitor.service)" = active ]] && \
    [[ "$(systemctl --user show --value -p ActiveState acp-frozen-release-recovery.service)" = active ]]
then
    assert_active_validation
    trap - ERR HUP INT TERM
    printf '%s\n' 'Forward recovery is already in a verified active validation window.'
    exit 0
fi
case "${current_phase}" in
    committing) ;;
    provisional|validating|validation_failed)
        for unit in tendwired.service herdres.service herdres-gateway.service \
            acp-frozen-live-monitor.service acp-frozen-release-recovery.service \
            acp-cutover-recovery.service
        do
            assert_unit_quiescent "${unit}"
        done
        rm -f -- "${MONITOR_OWNER}" "${MONITOR_HEARTBEAT}" "${RESUME_AUTH}"
        sync -d "${TRANSACTION_ROOT}"
        phase_write committing
        ;;
    *) false ;;
esac
test -f "${RESET_STARTED}"
test "$(stat -c '%u:%a' "${RESET_STARTED}")" = "$(id -u):600"
test "$(systemctl --user show --value -p MainPID herdr-server.service)" = \
    "$(<"${TRANSACTION_ROOT}/old-herdr-pid")"
rebind_forward_release
test "$(readlink -f "${ACTIVE_LINK}")" = "${NEW_RELEASE}"
for unit in tendwired herdres herdres-gateway; do
    cmp -s -- "${NEW_RELEASE}/systemd/${unit}-99.conf" \
        "${USER_UNITS}/${unit}.service.d/99-frozen-acp-release.conf"
done
cmp -s -- "${NEW_RELEASE}/systemd/acp-frozen-live-monitor.service" \
    "${LIVE_MONITOR_UNIT}"
cmp -s -- "${NEW_RELEASE}/systemd/acp-frozen-release-recovery.service" \
    "${RECOVERY_UNIT}"
cmp -s -- "${NEW_RELEASE}/systemd/legacy-recovery-frozen-99.conf" \
    "${LEGACY_RECOVERY_OVERRIDE}"
"${NEW_RELEASE}/validate-frozen-release" \
    --runtime "${TENDWIRE_RUNTIME}" --release "${NEW_RELEASE}" \
    --manifest "${RELEASE_MANIFEST}" --verify
assert_effective_units

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
systemctl --user reset-failed \
    herdres-gateway.service herdres.service tendwired.service
for unit in tendwired.service herdres.service herdres-gateway.service; do
    test "$(systemctl --user show --value -p NRestarts "${unit}")" = 0
done
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
