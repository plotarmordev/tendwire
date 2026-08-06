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
            FROZEN_ACP_CLEAN_ENV|HOME|USER|LOGNAME|PATH|PWD|SHLVL|_|XDG_RUNTIME_DIR|DBUS_SESSION_BUS_ADDRESS|ACP_CUTOVER_DISCARD_AUTHORIZATION) ;;
            *) return 1 ;;
        esac
    done < <(compgen -e)
}
if ! clean_environment_valid; then
    exec /usr/bin/env -i \
        HOME=/home/smith USER=smith LOGNAME=smith PATH=/usr/bin:/bin \
        XDG_RUNTIME_DIR=/run/user/1000 \
        DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/1000/bus \
        ACP_CUTOVER_DISCARD_AUTHORIZATION="${ACP_CUTOVER_DISCARD_AUTHORIZATION:-}" \
        FROZEN_ACP_CLEAN_ENV=1 \
        /bin/bash -p "${BASH_SOURCE[0]}" "$@"
fi
unset -f clean_environment_valid
unset FROZEN_ACP_CLEAN_ENV
export PATH=/usr/bin:/bin
unset BASH_ENV ENV CDPATH GLOBIGNORE PYTHONHOME PYTHONPATH TAR_OPTIONS

# Human-readable provenance; the manifest is the enforced copy.
# shellcheck disable=SC2034
readonly TENDWIRE_REVISION=7446533bb6fb2560a9a9dd871f638c4a6ccbb086
# shellcheck disable=SC2034
readonly HERDRES_REVISION=36599949daa64f68494d04f96a3bfee31904a804
# shellcheck disable=SC2034
readonly HERDR_REVISION=9026d9bc5a12d9adc2d9f68ebdc564133e4098b4
readonly HERDR_BINARY=/home/smith/.local/share/herdr-runtime/acp-9026d9bc/herdr
readonly HERDR_SHA256=2e58e1b11ed289d6a99ba36b80867e5e5d5920d03406bb40a1113e2d391f386f
readonly TENDWIRE_RUNTIME=/home/smith/.local/share/tendwire-runtime/acp-7446533-3659994-r2
readonly NEW_RELEASE=/home/smith/.local/share/acp-runtime/releases/9026d9bc-7446533-3659994-r2
readonly RELEASE_MANIFEST=/home/smith/.local/share/acp-runtime/manifests/9026d9bc-7446533-3659994-r2.json
readonly RUNNER_ROOT=/home/smith/.local/share/acp-runtime/runners/9026d9bc-7446533-3659994-r2
readonly DEPLOY_RUNNER=${RUNNER_ROOT}/deploy
readonly PREPARE_COMPLETE=/home/smith/.local/share/acp-runtime/prepared/9026d9bc-7446533-3659994-r2.complete
readonly EXPECTED_TENDWIRE_RUNTIME_SHA256=PREPARE_REQUIRED
readonly EXPECTED_TENDWIRE_RUNTIME_ENTRIES=PREPARE_REQUIRED
readonly EXPECTED_COMBINED_RELEASE_SHA256=PREPARE_REQUIRED
readonly EXPECTED_COMBINED_RELEASE_ENTRIES=PREPARE_REQUIRED
readonly ACTIVE_LINK=/home/smith/.local/share/acp-runtime/active
readonly USER_UNITS=/home/smith/.config/systemd/user
readonly PRIVATE_ENV=/home/smith/.config/herdres/frozen-7446533-3659994-r2.env
readonly PRIVATE_ENV_TEMP=${PRIVATE_ENV}.tmp
readonly LEGACY_ENV=/home/smith/.config/herdres/herdres.env
readonly LEGACY_STATE_PARENT=/home/smith/.local/share/herdres
readonly LEGACY_TENDWIRE_DB=/home/smith/.local/share/tendwire/tendwire.db
readonly LEGACY_HERDRES_STATE=${LEGACY_STATE_PARENT}/state.json
readonly LEGACY_HERDRES_INGRESS=${LEGACY_STATE_PARENT}/ingress.db
readonly TENDWIRE_CANDIDATE=/home/smith/.local/share/tendwire/candidates/7446533
readonly HERDRES_CANDIDATE=/home/smith/.local/share/herdres/candidates/3659994
readonly TENDWIRE_DB=${TENDWIRE_CANDIDATE}/tendwire.db
readonly TENDWIRE_SOCKET=/home/smith/.local/share/tendwire/tendwire.sock
readonly HERDRES_STATE=${HERDRES_CANDIDATE}/state.json
readonly HERDRES_INGRESS=${HERDRES_CANDIDATE}/ingress.db
readonly TRANSACTION_ROOT=/home/smith/.local/state/acp-cutover/frozen-7446533-6c0d0f5
readonly PHASE_FILE=${TRANSACTION_ROOT}/phase
readonly LEGACY_STATE_SNAPSHOT=${TRANSACTION_ROOT}/legacy-state.private
readonly DISCARD_INVENTORY=${TRANSACTION_ROOT}/discard-inventory.json
readonly DISCARD_AUTHORIZATION=LOUD_DISCARD_7446533_6c0d0f5
readonly CUTOVER_LOCK=/home/smith/.local/state/acp-cutover/frozen-7446533-6c0d0f5.lock
readonly NEW_RECOVERY_UNIT=${USER_UNITS}/acp-frozen-release-recovery.service
readonly LIVE_MONITOR_UNIT=${USER_UNITS}/acp-frozen-live-monitor.service
readonly OLD_RECOVERY_OVERRIDE=${USER_UNITS}/acp-cutover-recovery.service.d/99-frozen-acp-release.conf
readonly TOPIC_RESET_TOOL=${NEW_RELEASE}/reset-telegram-topics
readonly TOPIC_RESET_PYTHON=/home/smith/.local/share/uv/tools/contexto/bin/python
readonly TOPIC_RESET_SHA256=4ee7fcf7d226005cd95ae89910b850da5e581f6ab59b52e28c79f344ae955a69
readonly TOPIC_RESET_STARTED=${TRANSACTION_ROOT}/telegram-topic-reset-started.json
readonly EXPECTED_ACP_WORKER_COUNT=1
readonly EXPECTED_ACP_ADAPTER=codex
readonly ACP_WORKER_EVIDENCE=${TRANSACTION_ROOT}/named-acp-worker-evidence.json
readonly RESUME_AUTH=${TRANSACTION_ROOT}/forward-resume-authorized

old_release="$(readlink -f "${ACTIVE_LINK}")"
old_herdr_pid="$(systemctl --user show --value -p MainPID herdr-server.service)"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
failure_root="/home/smith/.local/share/acp-cutover-failures/${timestamp}-7446533-6c0d0f5-$$"
private_env_temp_failure="${PRIVATE_ENV_TEMP}.failed-${timestamp}-$$"
cutover_started=0
unit_backup_ready=0
candidate_paths_created=0
recovery_mutated=0
monitor_unit_installed=0
new_recovery_was_enabled="$(systemctl --user is-enabled acp-frozen-release-recovery.service 2>/dev/null || true)"
new_recovery_was_active="$(systemctl --user is-active acp-frozen-release-recovery.service 2>/dev/null || true)"

log() {
    printf '%s %s\n' "$(date -Iseconds)" "$*"
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

quarantine_private_env_temp() {
    if [[ -e "${PRIVATE_ENV_TEMP}" || -L "${PRIVATE_ENV_TEMP}" ]]; then
        [[ ! -e "${private_env_temp_failure}" && ! -L "${private_env_temp_failure}" ]]
        mv -T -- "${PRIVATE_ENV_TEMP}" "${private_env_temp_failure}"
        log "private environment temporary preserved at ${private_env_temp_failure}"
    fi
}

phase_write() {
    local phase="$1"
    local temporary="${PHASE_FILE}.tmp.$$"
    printf '%s\n' "${phase}" >"${temporary}"
    chmod 0600 "${temporary}"
    sync -d "${temporary}"
    mv -T -- "${temporary}" "${PHASE_FILE}"
    sync -d "${TRANSACTION_ROOT}"
}

switch_release() {
    local target="$1"
    local temporary
    temporary="$(dirname "${ACTIVE_LINK}")/.active.frozen.$$.${target##*/}"
    [[ ! -e "${temporary}" && ! -L "${temporary}" ]]
    ln -s -- "${target}" "${temporary}"
    mv -Tf -- "${temporary}" "${ACTIVE_LINK}"
    sync -d "$(dirname "${ACTIVE_LINK}")"
    test "$(readlink -f "${ACTIVE_LINK}")" = "${target}"
}

restore_units() {
    local unit
    local legacy
    for unit in tendwired.service herdres.service herdres-gateway.service; do
        if [[ -f "${TRANSACTION_ROOT}/units/${unit}.present" ]]; then
            install -m 0644 "${TRANSACTION_ROOT}/units/${unit}.conf" \
                "${USER_UNITS}/${unit}.d/99-frozen-acp-release.conf"
        else
            rm -f -- "${USER_UNITS}/${unit}.d/99-frozen-acp-release.conf"
        fi
        legacy="${USER_UNITS}/${unit}.d/90-acp-canary.conf"
        if [[ -f "${TRANSACTION_ROOT}/units/${unit}.legacy-present" ]]; then
            install -m 0644 "${TRANSACTION_ROOT}/units/${unit}.legacy-conf" "${legacy}"
        else
            rm -f -- "${legacy}"
        fi
    done
}

restore_recovery_configuration() {
    [[ "${recovery_mutated}" -eq 1 ]] || return 0
    if [[ "$(systemctl --user show --value -p LoadState acp-frozen-release-recovery.service)" != not-found ]]; then
        systemctl --user disable --now acp-frozen-release-recovery.service
    fi
    if [[ -f "${TRANSACTION_ROOT}/recovery/new-unit.present" ]]; then
        install -m 0644 "${TRANSACTION_ROOT}/recovery/new-unit.conf" "${NEW_RECOVERY_UNIT}"
    else
        rm -f -- "${NEW_RECOVERY_UNIT}"
    fi
    if [[ -f "${TRANSACTION_ROOT}/recovery/old-override.present" ]]; then
        install -d -m 0755 -- "$(dirname "${OLD_RECOVERY_OVERRIDE}")"
        install -m 0644 "${TRANSACTION_ROOT}/recovery/old-override.conf" "${OLD_RECOVERY_OVERRIDE}"
    else
        rm -f -- "${OLD_RECOVERY_OVERRIDE}"
    fi
    systemctl --user daemon-reload
    if [[ "${new_recovery_was_enabled}" = enabled ]]; then
        systemctl --user enable acp-frozen-release-recovery.service
    else
        if systemctl --user is-enabled --quiet acp-frozen-release-recovery.service; then
            return 1
        fi
    fi
    if [[ "${new_recovery_was_active}" = active ]]; then
        systemctl --user start acp-frozen-release-recovery.service
    else
        if systemctl --user is-active --quiet acp-frozen-release-recovery.service; then
            return 1
        fi
    fi
    recovery_mutated=0
}

rollback() {
    local status="$1"
    local line="${2:-0}"
    trap - ERR HUP INT TERM
    log "cutover failure status=${status} line=${line}"
    if [[ -f "${TOPIC_RESET_STARTED}" ]]; then
        systemctl --user stop acp-frozen-live-monitor.service || true
        systemctl --user stop herdres-gateway.service herdres.service tendwired.service
        systemctl --user stop acp-frozen-release-recovery.service || true
        rm -f -- "${RESUME_AUTH}"
        log "Telegram topic reset may be irreversible; leaving phase=committing and preserving candidate state for reviewed forward recovery"
        exit "${status}"
    fi
    if [[ "${cutover_started}" -eq 0 ]]; then
        if [[ "${monitor_unit_installed}" -eq 1 ]]; then
            rm -f -- "${LIVE_MONITOR_UNIT}"
            systemctl --user daemon-reload
        fi
        restore_recovery_configuration
        if [[ "${candidate_paths_created}" -eq 1 ]]; then
            quarantine_private_env_temp
            install -d -m 700 -- "${failure_root}"
            [[ ! -e "${PRIVATE_ENV}" ]] || mv -- "${PRIVATE_ENV}" "${failure_root}/"
            [[ ! -e "${TENDWIRE_CANDIDATE}" ]] || mv -- "${TENDWIRE_CANDIDATE}" "${failure_root}/tendwire-candidate"
            [[ ! -e "${HERDRES_CANDIDATE}" ]] || mv -- "${HERDRES_CANDIDATE}" "${failure_root}/herdres-candidate"
            [[ ! -e "${TRANSACTION_ROOT}" ]] || mv -- "${TRANSACTION_ROOT}" "${failure_root}/transaction"
            log "failed release artifacts preserved at ${failure_root}"
        fi
        log "deployment preflight failed; live target services were not touched"
        exit "${status}"
    fi
    log "candidate failed; restoring the prior service configuration"
    systemctl --user stop acp-frozen-live-monitor.service || true
    systemctl --user stop herdres-gateway.service herdres.service tendwired.service
    systemctl --user stop acp-frozen-release-recovery.service || true
    rm -f -- "${RESUME_AUTH}"
    if [[ "${unit_backup_ready}" -eq 1 ]]; then
        restore_units
    fi
    switch_release "${old_release}"
    phase_write rolled_back
    systemctl --user daemon-reload
    systemctl --user reset-failed acp-cutover-recovery.service \
        acp-frozen-release-recovery.service
    systemctl --user start tendwired.service herdres.service herdres-gateway.service
    for unit in tendwired.service herdres.service herdres-gateway.service; do
        systemctl --user is-active --quiet "${unit}"
    done
    test "$(systemctl --user show --value -p MainPID herdr-server.service)" = "${old_herdr_pid}"
    restore_recovery_configuration
    if [[ "${monitor_unit_installed}" -eq 1 ]]; then
        rm -f -- "${LIVE_MONITOR_UNIT}"
        systemctl --user daemon-reload
    fi
    quarantine_private_env_temp
    install -d -m 700 -- "${failure_root}"
    [[ ! -e "${PRIVATE_ENV}" ]] || mv -- "${PRIVATE_ENV}" "${failure_root}/"
    [[ ! -e "${TENDWIRE_CANDIDATE}" ]] || mv -- "${TENDWIRE_CANDIDATE}" "${failure_root}/tendwire-candidate"
    [[ ! -e "${HERDRES_CANDIDATE}" ]] || mv -- "${HERDRES_CANDIDATE}" "${failure_root}/herdres-candidate"
    [[ ! -e "${TRANSACTION_ROOT}" ]] || mv -- "${TRANSACTION_ROOT}" "${failure_root}/transaction"
    log "failed release artifacts preserved at ${failure_root}"
    exit "${status}"
}

trap 'rollback "$?" "${LINENO}"' ERR
trap 'rollback 129 0' HUP
trap 'rollback 130 0' INT
trap 'rollback 143 0' TERM

wait_tendwire() {
    local ready=0
    for _attempt in $(seq 1 240); do
        if [[ -S "${TENDWIRE_SOCKET}" ]] && \
            "${TENDWIRE_RUNTIME}/bin/python" -B -I - "${TENDWIRE_SOCKET}" <<'PY'
import sys
from tendwire.daemon_api import DaemonAPIClient

response = DaemonAPIClient(sys.argv[1], timeout_seconds=5).request("health.get", {})
result = response.get("result") or {}
daemon = result.get("daemon") or {}
backend = result.get("backend") or {}
acp = result.get("acp") or {}
healthy = (
    response.get("ok") is True
    and daemon.get("status") == "healthy"
    and backend.get("status") == "healthy"
    and backend.get("ready") is True
    and backend.get("running") is True
    and acp.get("healthy") is True
    and acp.get("state") == "running"
    and acp.get("failure_type") is None
)
raise SystemExit(0 if healthy else 1)
PY
        then
            ready=1
            break
        fi
        sleep 1
    done
    test "${ready}" -eq 1
}

verify_named_acp_worker_barrier() {
    "${TENDWIRE_RUNTIME}/bin/python" -B -I - \
        "${TENDWIRE_SOCKET}" "${ACP_WORKER_EVIDENCE}" \
        "${EXPECTED_ACP_WORKER_COUNT}" "${EXPECTED_ACP_ADAPTER}" <<'PY'
import hashlib
import json
import os
import sys
from pathlib import Path

from tendwire.daemon_api import DaemonAPIClient

socket_path = sys.argv[1]
evidence_path = Path(sys.argv[2])
expected_count = int(sys.argv[3])
expected_adapter = sys.argv[4]
client = DaemonAPIClient(socket_path, timeout_seconds=5)
health_response = client.request("health.get", {})
snapshot_response = client.request("snapshot.get", {})
health = (health_response.get("result") or {}).get("acp") or {}
snapshot = snapshot_response.get("result") or {}
workers = snapshot.get("workers")
if not isinstance(workers, list):
    raise RuntimeError("ACP worker snapshot is unavailable")
names = sorted(row.get("name") for row in workers if isinstance(row, dict))
valid = (
    health_response.get("ok") is True
    and snapshot_response.get("ok") is True
    and health.get("healthy") is True
    and health.get("state") == "running"
    and health.get("failure_type") is None
    and health.get("worker_count") == expected_count
    and len(workers) == expected_count
    and names == [expected_adapter] * expected_count
    and all(
        isinstance(row, dict)
        and row.get("status") in {"active", "idle", "waiting", "blocked"}
        for row in workers
    )
)
if not valid:
    raise RuntimeError("owner-bound named ACP worker set is unavailable")
body = (
    json.dumps(
        {
            "schema_version": 1,
            "expected_worker_count": expected_count,
            "observed_worker_count": len(workers),
            "adapter_set_sha256": hashlib.sha256(
                json.dumps(names, separators=(",", ":")).encode()
            ).hexdigest(),
            "snapshot_fingerprint": snapshot.get("content_fingerprint"),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    + "\n"
).encode()
temporary = evidence_path.with_name(evidence_path.name + f".tmp.{os.getpid()}")
descriptor = os.open(
    temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC, 0o600
)
try:
    view = memoryview(body)
    while view:
        written = os.write(descriptor, view)
        if written < 1:
            raise RuntimeError("ACP worker evidence write failed")
        view = view[written:]
    os.fsync(descriptor)
finally:
    os.close(descriptor)
os.replace(temporary, evidence_path)
directory = os.open(evidence_path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
try:
    os.fsync(directory)
finally:
    os.close(directory)
PY
    test "$(stat -c '%u:%a' "${ACP_WORKER_EVIDENCE}")" = "$(id -u):600"
}

wait_herdres_barrier() {
    local ready=0
    for _attempt in $(seq 1 180); do
        if PYTHONPATH="${NEW_RELEASE}/herdres" /usr/bin/python3 -s - "${HERDRES_STATE}" <<'PY'
import sys
from pathlib import Path
from herdres_connector.state import lifecycle_barrier

result = lifecycle_barrier(Path(sys.argv[1]))
raise SystemExit(
    0
    if result.ok
    and result.live_workers == 1
    and result.ready_routes == 1
    else 1
)
PY
        then
            ready=1
            break
        fi
        sleep 2
    done
    test "${ready}" -eq 1
}

wait_fresh_gateway() {
    local ready=0
    local first_pid=""
    for _attempt in $(seq 1 120); do
        if systemctl --user is-active --quiet herdres-gateway.service && \
            /usr/bin/python3 -I - "${HERDRES_INGRESS}" <<'PY'
import sqlite3
import sys

expected = {"manager", "managed-codex", "managed-omp", "managed-kimi"}
try:
    connection = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
    cursors = {row[0] for row in connection.execute("SELECT receiver_id FROM receiver_cursors")}
    requests = connection.execute("SELECT COUNT(*) FROM requests").fetchone()[0]
finally:
    try:
        connection.close()
    except Exception:
        pass
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
    sleep 10
    test "$(systemctl --user show --value -p MainPID herdres-gateway.service)" = "${first_pid}"
}

install -d -m 700 -- "$(dirname "${CUTOVER_LOCK}")"
exec 9>"${CUTOVER_LOCK}"
flock -n 9
cutover_owner_start="$(awk '{print $22}' "/proc/${BASHPID}/stat")"
[[ "${cutover_owner_start}" =~ ^[1-9][0-9]*$ ]]

[[ "${EXPECTED_TENDWIRE_RUNTIME_SHA256}" != PREPARE_REQUIRED ]]
[[ "${EXPECTED_COMBINED_RELEASE_SHA256}" != PREPARE_REQUIRED ]]
[[ "${BASH_SOURCE[0]}" = "$0" ]]
test "$(readlink -f -- "${BASH_SOURCE[0]}")" = "${DEPLOY_RUNNER}"
[[ "${ACP_CUTOVER_DISCARD_AUTHORIZATION:-}" = "${DISCARD_AUTHORIZATION}" ]]
[[ "${old_herdr_pid}" =~ ^[1-9][0-9]*$ ]]
for unit in tendwired.service herdres.service herdres-gateway.service herdr-server.service; do
    systemctl --user is-active --quiet "${unit}"
done
test -d "${old_release}"
test "$(stat -c '%a' "${LEGACY_ENV}")" = 600
test "$(sha256sum "${HERDR_BINARY}" | cut -d' ' -f1)" = "${HERDR_SHA256}"
test -x "${TOPIC_RESET_PYTHON}"
test "$(sha256sum "${TOPIC_RESET_TOOL}" | cut -d' ' -f1)" = "${TOPIC_RESET_SHA256}"
test "$(stat -c '%a:%s' "${HERDR_BINARY}")" = 555:20285816
test -f "${PREPARE_COMPLETE}"
test ! -L "${PREPARE_COMPLETE}"
test "$(stat -c '%u:%a:%h' "${PREPARE_COMPLETE}")" = "$(id -u):600:1"
test "$(<"${PREPARE_COMPLETE}")" = \
    "FROZEN_ACP_RELEASE_PREPARED $(<"${NEW_RELEASE}/operations-tooling-revision") $(sha256sum "${RELEASE_MANIFEST}" | cut -d' ' -f1)"

"${TENDWIRE_RUNTIME}/bin/python" -B -I - \
    "${TENDWIRE_RUNTIME}" "${NEW_RELEASE}" "${RELEASE_MANIFEST}" \
    "${DEPLOY_RUNNER}" "${NEW_RELEASE}/operations-tooling-revision" \
    "${EXPECTED_TENDWIRE_RUNTIME_SHA256}" "${EXPECTED_TENDWIRE_RUNTIME_ENTRIES}" \
    "${EXPECTED_COMBINED_RELEASE_SHA256}" "${EXPECTED_COMBINED_RELEASE_ENTRIES}" <<'PY'
import hashlib
import json
import os
import stat
import sys
from pathlib import Path

runtime, release, manifest, runner, tooling_path = map(Path, sys.argv[1:6])
expected_runtime, expected_runtime_entries, expected_release, expected_release_entries = sys.argv[6:]


def tree(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    count = 0
    for item in sorted(path.rglob("*"), key=lambda value: value.relative_to(path).as_posix()):
        relative = item.relative_to(path).as_posix().encode()
        info = item.lstat()
        kind = b"l" if stat.S_ISLNK(info.st_mode) else b"f" if stat.S_ISREG(info.st_mode) else b"d"
        content = os.readlink(item).encode() if kind == b"l" else item.read_bytes() if kind == b"f" else b""
        digest.update(len(relative).to_bytes(4, "big") + relative)
        digest.update(kind + stat.S_IMODE(info.st_mode).to_bytes(2, "big"))
        digest.update(len(content).to_bytes(8, "big") + content)
        count += 1
    return digest.hexdigest(), count


record = json.loads(manifest.read_text(encoding="utf-8"))
runtime_value = tree(runtime)
release_value = tree(release)
tooling_revision = tooling_path.read_text(encoding="utf-8").strip()
runner_digest = hashlib.sha256(runner.read_bytes()).hexdigest()
valid = (
    runtime_value == (expected_runtime, int(expected_runtime_entries))
    and release_value == (expected_release, int(expected_release_entries))
    and runtime.lstat().st_uid == os.geteuid()
    and release.lstat().st_uid == os.geteuid()
    and stat.S_IMODE(runtime.lstat().st_mode) == 0o555
    and stat.S_IMODE(release.lstat().st_mode) == 0o555
    and len(tooling_revision) == 40
    and all(value in "0123456789abcdef" for value in tooling_revision)
    and not runner.parent.is_symlink()
    and not runner.is_symlink()
    and runner.parent.is_dir()
    and runner.is_file()
    and runner.parent.lstat().st_uid == os.geteuid()
    and runner.lstat().st_uid == os.geteuid()
    and stat.S_IMODE(runner.parent.lstat().st_mode) == 0o555
    and stat.S_IMODE(runner.lstat().st_mode) == 0o555
    and not any(runtime.rglob("*.pyc"))
    and not any(item.name == "__pycache__" for item in runtime.rglob("*"))
    and record == {
        "schema_version": 1,
        "tendwire_revision": "7446533bb6fb2560a9a9dd871f638c4a6ccbb086",
        "herdres_revision": "36599949daa64f68494d04f96a3bfee31904a804",
        "herdr_revision": "9026d9bc5a12d9adc2d9f68ebdc564133e4098b4",
        "tooling_revision": tooling_revision,
        "deploy_runner_sha256": runner_digest,
        "herdr_sha256": "2e58e1b11ed289d6a99ba36b80867e5e5d5920d03406bb40a1113e2d391f386f",
        "acp_source_sha256": "f5e621738a5651da9d14559806ab1d3491e8a9da6a72e686baf087e67a87e5f6",
        "acp_source_files": 29,
        "acp_distribution_version": "0.11.0",
        "tendwire_runtime_sha256": expected_runtime,
        "tendwire_runtime_entries": int(expected_runtime_entries),
        "combined_release_sha256": expected_release,
        "combined_release_entries": int(expected_release_entries),
        "owner_uid": os.geteuid(),
        "tendwire_runtime_root_mode": 0o555,
        "combined_release_root_mode": 0o555,
    }
)
raise SystemExit(0 if valid else 1)
PY

[[ ! -e "${PRIVATE_ENV}" && ! -L "${PRIVATE_ENV}" ]]
[[ ! -e "${PRIVATE_ENV_TEMP}" && ! -L "${PRIVATE_ENV_TEMP}" ]]
[[ ! -e "${TENDWIRE_CANDIDATE}" && ! -L "${TENDWIRE_CANDIDATE}" ]]
[[ ! -e "${HERDRES_CANDIDATE}" && ! -L "${HERDRES_CANDIDATE}" ]]
[[ ! -e "${TRANSACTION_ROOT}" && ! -L "${TRANSACTION_ROOT}" ]]
[[ ! -e "${LIVE_MONITOR_UNIT}" && ! -L "${LIVE_MONITOR_UNIT}" ]]
candidate_paths_created=1
install -d -m 700 -- "${TENDWIRE_CANDIDATE}" "${HERDRES_CANDIDATE}" \
    "${TRANSACTION_ROOT}"
install -d -m 700 -- "${TRANSACTION_ROOT}/units" "${TRANSACTION_ROOT}/recovery"
test "$(stat -c '%a' "${TRANSACTION_ROOT}")" = 700
"${NEW_RELEASE}/validate-frozen-release" \
    --runtime "${TENDWIRE_RUNTIME}" --release "${NEW_RELEASE}" \
    --manifest "${RELEASE_MANIFEST}" --create
test "$(stat -c '%u:%a' "${TRANSACTION_ROOT}/release-validation.json")" = \
    "$(id -u):600"

"${TENDWIRE_RUNTIME}/bin/python" -B -I - \
    "${LEGACY_STATE_PARENT}" "${LEGACY_STATE_SNAPSHOT}" <<'PY'
import os
import stat
import sys
from pathlib import Path

parent_path, snapshot_path = map(Path, sys.argv[1:])
maximum = 16_777_216
directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
parent_fd = os.open(parent_path, directory_flags)
try:
    parent_info = os.fstat(parent_fd)
    parent_named = parent_path.lstat()
    if (
        not stat.S_ISDIR(parent_info.st_mode)
        or parent_info.st_uid != os.geteuid()
        or stat.S_IMODE(parent_info.st_mode) != 0o700
        or (parent_info.st_dev, parent_info.st_ino) != (parent_named.st_dev, parent_named.st_ino)
    ):
        raise RuntimeError("legacy state parent is unsafe")
    state_fd = os.open(
        "state.json",
        os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
        dir_fd=parent_fd,
    )
    try:
        before = os.fstat(state_fd)
        named_before = os.stat("state.json", dir_fd=parent_fd, follow_symlinks=False)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.geteuid()
            or before.st_nlink != 1
            or stat.S_IMODE(before.st_mode) not in {0o600, 0o664}
            or (before.st_dev, before.st_ino) != (named_before.st_dev, named_before.st_ino)
            or before.st_size > maximum
        ):
            raise RuntimeError("legacy state is unsafe")
        chunks: list[bytes] = []
        length = 0
        while True:
            chunk = os.read(state_fd, min(1_048_576, maximum + 1 - length))
            if not chunk:
                break
            chunks.append(chunk)
            length += len(chunk)
            if length > maximum:
                raise RuntimeError("legacy state exceeds bound")
        after = os.fstat(state_fd)
        named_after = os.stat("state.json", dir_fd=parent_fd, follow_symlinks=False)
        stable_fields = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")
        if (
            not stat.S_ISREG(after.st_mode)
            or after.st_uid != os.geteuid()
            or after.st_nlink != 1
            or stat.S_IMODE(after.st_mode) not in {0o600, 0o664}
            or any(getattr(before, name) != getattr(after, name) for name in stable_fields)
            or any(getattr(after, name) != getattr(named_after, name) for name in stable_fields)
        ):
            raise RuntimeError("legacy state changed during snapshot")
        body = b"".join(chunks)
    finally:
        os.close(state_fd)
finally:
    os.close(parent_fd)

temporary = snapshot_path.with_name(snapshot_path.name + f".tmp.{os.getpid()}")
fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC, 0o600)
try:
    view = memoryview(body)
    while view:
        written = os.write(fd, view)
        if written < 1:
            raise RuntimeError("legacy state snapshot write failed")
        view = view[written:]
    os.fsync(fd)
finally:
    os.close(fd)
os.replace(temporary, snapshot_path)
directory = os.open(snapshot_path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
try:
    os.fsync(directory)
finally:
    os.close(directory)
PY
test "$(stat -c '%a' "${LEGACY_STATE_SNAPSHOT}")" = 600

"${TENDWIRE_RUNTIME}/bin/python" -B -I - \
    "${LEGACY_ENV}" "${LEGACY_STATE_SNAPSHOT}" "${PRIVATE_ENV}" <<'PY'
import json
import os
import re
import stat
import sys
import urllib.request
from pathlib import Path

env_path, state_path, target_path = map(Path, sys.argv[1:])


def private_regular(path: Path, limit: int) -> bytes:
    flags = os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags)
    try:
        info = os.fstat(fd)
        named = path.lstat()
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.geteuid()
            or info.st_nlink != 1
            or stat.S_IMODE(info.st_mode) & 0o077
            or (info.st_dev, info.st_ino) != (named.st_dev, named.st_ino)
            or info.st_size > limit
        ):
            raise RuntimeError("private release input is unsafe")
        chunks: list[bytes] = []
        length = 0
        while True:
            chunk = os.read(fd, min(1_048_576, limit + 1 - length))
            if not chunk:
                break
            chunks.append(chunk)
            length += len(chunk)
            if length > limit:
                raise RuntimeError("private release input exceeds bound")
        value = b"".join(chunks)
    finally:
        os.close(fd)
    if len(value) > limit:
        raise RuntimeError("private release input exceeds bound")
    return value


def read_env(path: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for number, source in enumerate(private_regular(path, 65_536).decode().splitlines(), 1):
        line = source.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        key, separator, value = line.partition("=")
        if not separator or re.fullmatch(r"[A-Z][A-Z0-9_]{0,127}", key) is None:
            raise RuntimeError(f"invalid private environment line {number}")
        if len(value.encode()) > 4096 or "\x00" in value:
            raise RuntimeError(f"invalid private environment value {number}")
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'\"', "'"}:
            value = value[1:-1]
        result[key] = value
    return result


def safe(name: str, value: object, pattern: str) -> str:
    clean = str(value or "").strip()
    if re.fullmatch(pattern, clean) is None:
        raise RuntimeError(f"invalid private release field {name}")
    return clean


legacy_env = read_env(env_path)
state = json.loads(private_regular(state_path, 16_777_216).decode())
telegram = state.get("telegram") if isinstance(state.get("telegram"), dict) else {}
managed = telegram.get("managed_bots") if isinstance(telegram.get("managed_bots"), dict) else {}
values = {
    "HERDRES_ENV_FILE": str(target_path),
    "HERDRES_INGRESS_PATH": "/home/smith/.local/share/herdres/candidates/3659994/ingress.db",
    "HERDRES_STATE_PATH": "/home/smith/.local/share/herdres/candidates/3659994/state.json",
    "HERDRES_TENDWIRE_MODE": "source",
    "TENDWIRE_SOCKET_PATH": "/home/smith/.local/share/tendwire/tendwire.sock",
    "TELEGRAM_CHAT_ID": safe("chat", telegram.get("chat_id"), r"-?[0-9]{1,24}"),
    "TELEGRAM_GENERAL_THREAD_ID": safe(
        "general topic", telegram.get("general_thread_id") or "1", r"[0-9]{1,24}"
    ),
}
owners = telegram.get("owner_user_ids")
if not isinstance(owners, list) or not owners:
    raise RuntimeError("invalid private release field owners")
values["TELEGRAM_OWNER_USER_IDS"] = ",".join(
    safe("owner", value, r"[0-9]{1,24}") for value in owners
)
values["TELEGRAM_BOT_TOKEN"] = safe(
    "manager token", legacy_env.get("TELEGRAM_BOT_TOKEN"),
    r"[0-9]{6,20}:[A-Za-z0-9_-]{20,200}",
)
for kind in ("codex", "omp", "kimi"):
    row = managed.get(kind) if isinstance(managed.get(kind), dict) else {}
    token = str(row.get("token") or "").strip() or legacy_env.get(
        f"HERDR_TELEGRAM_TOPICS_MANAGED_BOT_{kind.upper()}_TOKEN", ""
    ).strip()
    values[f"TELEGRAM_{kind.upper()}_BOT_TOKEN"] = safe(
        f"{kind} token", token, r"[0-9]{6,20}:[A-Za-z0-9_-]{20,200}"
    )

tokens = [values[key] for key in (
    "TELEGRAM_BOT_TOKEN", "TELEGRAM_CODEX_BOT_TOKEN",
    "TELEGRAM_OMP_BOT_TOKEN", "TELEGRAM_KIMI_BOT_TOKEN",
)]
if len(set(tokens)) != len(tokens):
    raise RuntimeError("private release bot identities collide")

identities: set[int] = set()
usernames: set[str] = set()
for kind, token in zip(("", "CODEX_", "OMP_", "KIMI_"), tokens, strict=True):
    request = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/getMe",
        data=b"{}",
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            raw = response.read(65_537)
    except Exception:
        raise RuntimeError("private release bot verification unavailable") from None
    if len(raw) > 65_536:
        raise RuntimeError("private release bot verification failed")
    body = json.loads(raw)
    result = body.get("result") if isinstance(body, dict) else None
    if (
        body.get("ok") is not True
        or not isinstance(result, dict)
        or result.get("is_bot") is not True
        or type(result.get("id")) is not int
        or result["id"] < 1
    ):
        raise RuntimeError("private release bot verification failed")
    username = safe(
        "bot username", str(result.get("username") or "").lstrip("@"),
        r"[A-Za-z0-9_]{3,64}",
    )
    if result["id"] in identities or username.lower() in usernames:
        raise RuntimeError("private release bot identities collide")
    identities.add(result["id"])
    usernames.add(username.lower())
    values[f"TELEGRAM_{kind}BOT_USERNAME"] = username

expected = {
    "HERDRES_ENV_FILE", "HERDRES_INGRESS_PATH", "HERDRES_STATE_PATH",
    "HERDRES_TENDWIRE_MODE", "TENDWIRE_SOCKET_PATH", "TELEGRAM_CHAT_ID",
    "TELEGRAM_GENERAL_THREAD_ID", "TELEGRAM_OWNER_USER_IDS",
    "TELEGRAM_BOT_TOKEN", "TELEGRAM_BOT_USERNAME",
    "TELEGRAM_CODEX_BOT_TOKEN", "TELEGRAM_CODEX_BOT_USERNAME",
    "TELEGRAM_OMP_BOT_TOKEN", "TELEGRAM_OMP_BOT_USERNAME",
    "TELEGRAM_KIMI_BOT_TOKEN", "TELEGRAM_KIMI_BOT_USERNAME",
}
if set(values) != expected:
    raise RuntimeError("private release environment is incomplete")
temporary = target_path.with_name(target_path.name + ".tmp")
fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC, 0o600)
try:
    body = "".join(f"{key}={values[key]}\n" for key in sorted(values)).encode()
    with os.fdopen(fd, "wb", closefd=False) as target:
        target.write(body)
        target.flush()
    os.fsync(fd)
finally:
    os.close(fd)
os.replace(temporary, target_path)
directory = os.open(target_path.parent, os.O_RDONLY | os.O_DIRECTORY)
try:
    os.fsync(directory)
finally:
    os.close(directory)
PY
test "$(stat -c '%a' "${PRIVATE_ENV}")" = 600
[[ ! -e "${PRIVATE_ENV_TEMP}" && ! -L "${PRIVATE_ENV_TEMP}" ]]
rm -- "${LEGACY_STATE_SNAPSHOT}"

for unit in tendwired.service herdres.service herdres-gateway.service; do
    current="${USER_UNITS}/${unit}.d/99-frozen-acp-release.conf"
    if [[ -f "${current}" ]]; then
        cp -a -- "${current}" "${TRANSACTION_ROOT}/units/${unit}.conf"
        : >"${TRANSACTION_ROOT}/units/${unit}.present"
    fi
    legacy="${USER_UNITS}/${unit}.d/90-acp-canary.conf"
    if [[ -f "${legacy}" && ! -L "${legacy}" ]]; then
        test "$(stat -c '%U:%a' "${legacy}")" = "$(id -un):644"
        cp -a -- "${legacy}" "${TRANSACTION_ROOT}/units/${unit}.legacy-conf"
        : >"${TRANSACTION_ROOT}/units/${unit}.legacy-present"
    else
        [[ ! -e "${legacy}" && ! -L "${legacy}" ]]
    fi
done
if [[ -f "${NEW_RECOVERY_UNIT}" ]]; then
    cp -a -- "${NEW_RECOVERY_UNIT}" "${TRANSACTION_ROOT}/recovery/new-unit.conf"
    : >"${TRANSACTION_ROOT}/recovery/new-unit.present"
fi
if [[ -f "${OLD_RECOVERY_OVERRIDE}" ]]; then
    cp -a -- "${OLD_RECOVERY_OVERRIDE}" "${TRANSACTION_ROOT}/recovery/old-override.conf"
    : >"${TRANSACTION_ROOT}/recovery/old-override.present"
fi
unit_backup_ready=1
printf '%s\n' "${old_release}" >"${TRANSACTION_ROOT}/old-release"
printf '%s\n' "${old_herdr_pid}" >"${TRANSACTION_ROOT}/old-herdr-pid"
chmod 0600 "${TRANSACTION_ROOT}/old-release" "${TRANSACTION_ROOT}/old-herdr-pid"
/usr/bin/python3 -I - "${TRANSACTION_ROOT}" <<'PY'
import os
import sys
from pathlib import Path

root = Path(sys.argv[1])
for path in sorted(root.rglob("*")):
    if path.is_file() and not path.is_symlink():
        descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
for path in sorted((root, *root.rglob("*")), key=lambda item: len(item.parts), reverse=True):
    if path.is_dir() and not path.is_symlink():
        descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
PY
phase_write prepared
"${TOPIC_RESET_PYTHON}" -I "${TOPIC_RESET_TOOL}"

install -d -m 0755 -- "${USER_UNITS}/acp-cutover-recovery.service.d"
recovery_mutated=1
install -m 0644 "${NEW_RELEASE}/systemd/acp-frozen-release-recovery.service" \
    "${NEW_RECOVERY_UNIT}"
install -m 0644 "${NEW_RELEASE}/systemd/acp-frozen-live-monitor.service" \
    "${LIVE_MONITOR_UNIT}"
monitor_unit_installed=1
install -m 0644 "${NEW_RELEASE}/systemd/legacy-recovery-frozen-99.conf" \
    "${OLD_RECOVERY_OVERRIDE}"
systemctl --user daemon-reload
systemctl --user enable acp-frozen-release-recovery.service
systemctl --user stop acp-frozen-release-recovery.service
systemctl --user start acp-frozen-release-recovery.service
systemctl --user is-active --quiet acp-frozen-release-recovery.service
systemctl --user is-active --quiet acp-cutover-recovery.service
cmp -s -- "${NEW_RELEASE}/systemd/legacy-recovery-frozen-99.conf" "${OLD_RECOVERY_OVERRIDE}"
old_recovery_exec="$(systemctl --user show --value -p ExecStart acp-cutover-recovery.service)"
new_recovery_exec="$(systemctl --user show --value -p ExecStart acp-frozen-release-recovery.service)"
[[ "${old_recovery_exec}" == *"${NEW_RELEASE}/frozen-cutover-recovery"* ]]
[[ "${old_recovery_exec}" != *"/home/smith/.local/share/acp-runtime/bin/acp-cutover-recover"* ]]
[[ "${new_recovery_exec}" == *"${NEW_RELEASE}/frozen-cutover-recovery"* ]]
test "$(systemctl --user show --value -p MainPID herdr-server.service)" = "${old_herdr_pid}"

printf '%s %s %s\n' RESUME_FROZEN_ACP_CUTOVER "${BASHPID}" \
    "${cutover_owner_start}" >"${RESUME_AUTH}.tmp.$$"
chmod 0600 "${RESUME_AUTH}.tmp.$$"
sync -d "${RESUME_AUTH}.tmp.$$"
mv -T -- "${RESUME_AUTH}.tmp.$$" "${RESUME_AUTH}"
sync -d "${TRANSACTION_ROOT}"
phase_write committing
cutover_started=1
log "stopping only Tendwire and Herdres for the frozen cutover; Herdr remains untouched"
systemctl --user stop herdres-gateway.service herdres.service tendwired.service
test "$(systemctl --user show --value -p MainPID herdr-server.service)" = "${old_herdr_pid}"
log "capturing transaction-bound aggregate legacy discard inventory"
"${NEW_RELEASE}/capture-frozen-discard-inventory" \
    --tendwire-db "${LEGACY_TENDWIRE_DB}" \
    --herdres-ingress "${LEGACY_HERDRES_INGRESS}" \
    --herdres-state "${LEGACY_HERDRES_STATE}" \
    --output "${DISCARD_INVENTORY}"
test "$(stat -c '%u:%a' "${DISCARD_INVENTORY}")" = "$(id -u):600"
sync -d "${TRANSACTION_ROOT}"

install -d -m 0755 -- "${USER_UNITS}/tendwired.service.d" \
    "${USER_UNITS}/herdres.service.d" "${USER_UNITS}/herdres-gateway.service.d"
for unit in tendwired.service herdres.service herdres-gateway.service; do
    rm -f -- "${USER_UNITS}/${unit}.d/90-acp-canary.conf"
done
install -m 0644 "${NEW_RELEASE}/systemd/tendwired-99.conf" \
    "${USER_UNITS}/tendwired.service.d/99-frozen-acp-release.conf"
install -m 0644 "${NEW_RELEASE}/systemd/herdres-99.conf" \
    "${USER_UNITS}/herdres.service.d/99-frozen-acp-release.conf"
install -m 0644 "${NEW_RELEASE}/systemd/herdres-gateway-99.conf" \
    "${USER_UNITS}/herdres-gateway.service.d/99-frozen-acp-release.conf"
switch_release "${NEW_RELEASE}"
systemctl --user daemon-reload

for unit in tendwired.service herdres.service herdres-gateway.service; do
    [[ ! -e "${USER_UNITS}/${unit}.d/90-acp-canary.conf" ]]
    [[ ! -L "${USER_UNITS}/${unit}.d/90-acp-canary.conf" ]]
    requires="$(systemctl --user show --value -p Requires "${unit}")"
    wants="$(systemctl --user show --value -p Wants "${unit}")"
    after="$(systemctl --user show --value -p After "${unit}")"
    [[ " ${requires} " == *" acp-frozen-release-recovery.service "* ]]
    [[ " ${requires} " != *" acp-cutover-recovery.service "* ]]
    [[ " ${wants} " != *" acp-cutover-recovery.service "* ]]
    [[ " ${after} " == *" acp-frozen-release-recovery.service "* ]]
    test -z "$(systemctl --user show --value -p ExecStartPre "${unit}")"
    test -z "$(systemctl --user show --value -p ExecStartPost "${unit}")"
done
assert_dependency tendwired.service Requires herdr-server.service
assert_dependency tendwired.service After herdr-server.service
assert_dependency herdres.service Requires tendwired.service
assert_dependency herdres.service After tendwired.service
assert_dependency herdres-gateway.service Requires tendwired.service
assert_dependency herdres-gateway.service After tendwired.service
assert_dependency herdres-gateway.service Requires herdres.service
assert_dependency herdres-gateway.service After herdres.service
tw_exec="$(systemctl --user show --value -p ExecStart tendwired.service)"
hd_exec="$(systemctl --user show --value -p ExecStart herdres.service)"
gw_exec="$(systemctl --user show --value -p ExecStart herdres-gateway.service)"
tw_environment="$(systemctl --user show --value -p Environment tendwired.service)"
[[ "${tw_exec}" == *"${TENDWIRE_RUNTIME}/bin/python -B -I -m tendwire.cli daemon"* ]]
[[ "${tw_exec}" == *"--db-path ${TENDWIRE_DB} --socket-path ${TENDWIRE_SOCKET}"* ]]
[[ "${tw_exec}" != *"/home/smith/tendwire"* ]]
[[ "${tw_environment}" == *"TENDWIRE_HERDR_BIN=${HERDR_BINARY}"* ]]
[[ "${tw_environment}" == *"PYTHONSAFEPATH=1"* ]]
[[ "${tw_environment}" == *"PYTHONPATH="* ]]
[[ "${hd_exec}" == *"/usr/bin/python3 -E -s ${NEW_RELEASE}/herdres/herdres sync --loop 5"* ]]
[[ "${gw_exec}" == *"/usr/bin/python3 -E -s ${NEW_RELEASE}/herdres/herdres-gateway"* ]]
assert_working_directory tendwired.service "${TENDWIRE_RUNTIME}"
assert_working_directory herdres.service "${NEW_RELEASE}/herdres"
assert_working_directory herdres-gateway.service "${NEW_RELEASE}/herdres"
test "$(systemctl --user show --value -p EnvironmentFiles herdres.service)" = "${PRIVATE_ENV} (ignore_errors=no)"
test "$(systemctl --user show --value -p EnvironmentFiles herdres-gateway.service)" = "${PRIVATE_ENV} (ignore_errors=no)"

log "starting frozen Tendwire"
systemctl --user start tendwired.service
wait_tendwire
log "verifying the owner-bound named ACP worker set before Telegram reset"
verify_named_acp_worker_barrier
log "resetting the exact previewed non-General Telegram topic set"
"${TOPIC_RESET_PYTHON}" -I "${TOPIC_RESET_TOOL}" --apply
"${TOPIC_RESET_PYTHON}" -I "${TOPIC_RESET_TOOL}" --gate-presenter
log "starting frozen Herdres presenter"
systemctl --user start herdres.service
wait_herdres_barrier
"${TOPIC_RESET_PYTHON}" -I "${TOPIC_RESET_TOOL}" --verify-presenter
log "starting frozen Herdres gateway at a fresh Telegram tail cursor"
systemctl --user start herdres-gateway.service
wait_fresh_gateway
"${TOPIC_RESET_PYTHON}" -I "${TOPIC_RESET_TOOL}" --verify-gateway

for unit in tendwired.service herdres.service herdres-gateway.service; do
    systemctl --user is-active --quiet "${unit}"
    test "$(systemctl --user show --value -p NRestarts "${unit}")" = 0
done
test "$(systemctl --user show --value -p MainPID herdr-server.service)" = "${old_herdr_pid}"
wait_tendwire
phase_write provisional
log "starting the immutable one-hour live validator"
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
log "provisional deployment active; fresh Telegram cursor initialized with zero historical requests"
log "the immutable one-hour live validation window is running"
