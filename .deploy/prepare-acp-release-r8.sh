#!/usr/bin/env bash
set -Eeuo pipefail

# Live r8 preparation is deliberately separate from build/publish.  Every
# mutable local artifact is backed up before the first live change so the
# systemd rollback unit can restore the exact pre-deployment state.
readonly RELEASE_ID=67568f32-0b94403-f50af73-r8
readonly RELEASE=/home/smith/.local/share/acp-runtime/releases/${RELEASE_ID}
readonly MANIFEST=/home/smith/.local/share/acp-runtime/manifests/${RELEASE_ID}.json
readonly ACTIVE=/home/smith/.local/share/acp-runtime/active
readonly TRANSACTION=/home/smith/.local/state/acp-cutover/frozen-0b94403-r8
readonly LOCK=/home/smith/.local/state/acp-cutover/frozen-0b94403-r8.lock
readonly TOPIC_PYTHON=/home/smith/.local/share/uv/tools/contexto/bin/python
readonly PRIVATE_ENV=/home/smith/.config/herdres/frozen-0b94403-f50af73-r8.env
readonly TENDWIRE_CANDIDATE=/home/smith/.local/share/tendwire/candidates/0b94403-r8
readonly HERDRES_CANDIDATE=/home/smith/.local/share/herdres/candidates/f50af73-r8
readonly MIGRATION_STATE=/home/smith/.local/state/acp-pane-migration/current
readonly MIGRATION_LOCK=/home/smith/.local/state/acp-pane-migration/current.lock
readonly TENDWIRE_SOCKET=/home/smith/.local/share/tendwire/tendwire.sock
readonly GUARD_ENABLE_LINK=/home/smith/.config/systemd/user/default.target.wants/acp-r8-release-guard.service

readonly -a INSTALLED_FILES=(
    /home/smith/.config/systemd/user/tendwired.service.d/99-frozen-acp-release.conf
    /home/smith/.config/systemd/user/herdr-server.service.d/99-codex-acp-r8.conf
    /home/smith/.config/systemd/user/herdres.service.d/99-frozen-acp-release.conf
    /home/smith/.config/systemd/user/herdres-gateway.service.d/99-frozen-acp-release.conf
    /home/smith/.config/systemd/user/acp-frozen-live-monitor.service
    /home/smith/.config/systemd/user/acp-r8-release-guard.service
    /home/smith/.config/systemd/user/acp-r8-rollback.service
    /home/smith/.config/systemd/user/acp-r8-rollout.service
)
readonly -a TRACKED_UNITS=(
    acp-frozen-live-monitor.service
    acp-r6-release-guard.service
    acp-r6-rollback.service
    acp-r6-rollout.service
    acp-r7-release-guard.service
    acp-r7-rollback.service
    acp-r7-rollout.service
    acp-r8-release-guard.service
    acp-r8-rollback.service
    acp-r8-rollout.service
    acp-frozen-release-recovery.service
    acp-cutover-recovery.service
)
readonly -a SNAPSHOT_PATHS=(
    "${INSTALLED_FILES[@]}"
    "${GUARD_ENABLE_LINK}"
    "${ACTIVE}"
    "${MIGRATION_STATE}"
    "${MIGRATION_LOCK}"
    "${TENDWIRE_CANDIDATE}"
    "${HERDRES_CANDIDATE}"
    "${TENDWIRE_SOCKET}"
)

atomic_text() {
    local path="$1" value="$2"
    printf '%s\n' "${value}" >"${path}.tmp.$$"
    chmod 600 "${path}.tmp.$$"
    sync "${path}.tmp.$$"
    mv -f "${path}.tmp.$$" "${path}"
    sync -d "$(dirname "${path}")"
}

fsync_published_release() {
    python3 - "${RELEASE}" "${MANIFEST}" "${PRIVATE_ENV}" <<'PY'
import os
import stat
import sys

def sync_file(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)

def sync_dir(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)

for raw in sys.argv[1:]:
    root = os.path.abspath(raw)
    metadata = os.lstat(root)
    directories = []
    if stat.S_ISREG(metadata.st_mode):
        sync_file(root)
    elif stat.S_ISDIR(metadata.st_mode):
        for current, names, files in os.walk(root, followlinks=False):
            directories.append(current)
            for name in files:
                path = os.path.join(current, name)
                item = os.lstat(path)
                if stat.S_ISREG(item.st_mode):
                    sync_file(path)
                elif not stat.S_ISLNK(item.st_mode):
                    raise SystemExit("unsupported published release entry")
        for directory in reversed(directories):
            sync_dir(directory)
    else:
        raise SystemExit("unsupported published release artifact")
    sync_dir(os.path.dirname(root))
PY
}

write_prepare_owner() {
    local root="$1"
    python3 - "${root}/prepare-owner.json" "$$" <<'PY'
import json
import os
import sys

path, raw_pid = sys.argv[1:]
pid = int(raw_pid)
value = {
    "schema_version": 1,
    "tag": "R8_PREPARE",
    "pid": pid,
    "start_time": open(f"/proc/{pid}/stat", encoding="ascii").read().rsplit(") ", 1)[1].split()[19],
    "boot_id": open("/proc/sys/kernel/random/boot_id", encoding="ascii").read().strip(),
    "uid": os.geteuid(),
}
temporary = f"{path}.tmp.{pid}"
descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC, 0o600)
try:
    body = (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()
    os.write(descriptor, body)
    os.fsync(descriptor)
finally:
    os.close(descriptor)
os.replace(temporary, path)
directory = os.open(os.path.dirname(path), os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
try:
    os.fsync(directory)
finally:
    os.close(directory)
PY
}

capture_snapshot() {
    local destination="$1"
    (umask 077; "${RELEASE}/herdr" api snapshot >"${destination}.tmp.$$")
    python3 - "${destination}.tmp.$$" <<'PY'
import json
import sys

value = json.load(open(sys.argv[1], encoding="utf-8"))
snapshot = (value.get("result") or {}).get("snapshot")
if not isinstance(snapshot, dict) or not all(
    isinstance(snapshot.get(key), list) and snapshot[key]
    for key in ("workspaces", "tabs", "panes", "layouts")
):
    raise SystemExit("Herdr topology snapshot is incomplete")
PY
    chmod 600 "${destination}.tmp.$$"
    mv -f "${destination}.tmp.$$" "${destination}"
    sync "${destination}"
    sync -d "$(dirname "${destination}")"
}

sync_backup_tree() {
    python3 - "${TRANSACTION}/prestate" <<'PY'
import os
import stat
import sys

root = os.path.abspath(sys.argv[1])
if not root.startswith("/home/smith/.local/state/acp-cutover/"):
    raise SystemExit("unsafe backup root")
directories = []
for current, names, files in os.walk(root, topdown=True, followlinks=False):
    directories.append(current)
    for name in files:
        path = os.path.join(current, name)
        metadata = os.lstat(path)
        if stat.S_ISREG(metadata.st_mode):
            descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        elif not stat.S_ISLNK(metadata.st_mode):
            raise SystemExit("backup contains an unsupported entry")
for directory in reversed(directories):
    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
PY
}

snapshot_prestate() {
    local index path state unit enabled pid
    install -d -m 700 "${TRANSACTION}" "${TRANSACTION}/prestate" \
        "${TRANSACTION}/prestate/files" "${TRANSACTION}/prestate/units"
    readlink -f "${ACTIVE}" >"${TRANSACTION}/prestate/active-target"
    grep -Eq '^/home/smith/\.local/share/acp-runtime/releases/[^/]+$' \
        "${TRANSACTION}/prestate/active-target"
    for index in "${!SNAPSHOT_PATHS[@]}"; do
        path="${SNAPSHOT_PATHS[$index]}"
        state="${TRANSACTION}/prestate/files/${index}.state"
        if [[ -e "${path}" || -L "${path}" ]]; then
            cp -a -- "${path}" "${TRANSACTION}/prestate/files/${index}.entry"
            atomic_text "${state}" present
        else
            atomic_text "${state}" absent
        fi
    done
    for unit in "${TRACKED_UNITS[@]}"; do
        enabled="$(systemctl --user is-enabled "${unit}" 2>/dev/null || true)"
        atomic_text "${TRANSACTION}/prestate/units/${unit}.enabled" "${enabled:-not-found}"
        atomic_text "${TRANSACTION}/prestate/units/${unit}.active" \
            "$(systemctl --user is-active "${unit}" 2>/dev/null || true)"
    done
    for unit in tendwired.service herdres.service herdres-gateway.service; do
        test "$(systemctl --user is-active "${unit}" 2>/dev/null || true)" = inactive
    done
    test "$(systemctl --user is-active acp-frozen-live-monitor.service 2>/dev/null || true)" = inactive
    systemctl --user show \
        -p LoadState -p ActiveState -p SubState -p UnitFileState \
        -p Result -p ExecMainStatus acp-frozen-live-monitor.service \
        >"${TRANSACTION}/prestate/frozen-monitor-state"
    python3 - "${TRANSACTION}/prestate/frozen-monitor-state" <<'PY'
import sys

values = {}
for line in open(sys.argv[1], encoding="utf-8"):
    key, separator, value = line.rstrip("\n").partition("=")
    if separator:
        values[key] = value
if (
    values.get("ActiveState") != "inactive"
    or values.get("SubState") != "dead"
    or values.get("Result") != "success"
    # systemd records an operator stop as SIGTERM (15) while Result remains
    # success. Both states are canonical for an inactive, dead prior monitor.
    or values.get("ExecMainStatus") not in {"0", "15"}
):
    raise SystemExit("frozen monitor is not in canonical inactive state")
PY
    for unit in acp-r6-release-guard.service acp-r7-release-guard.service \
        acp-frozen-release-recovery.service acp-cutover-recovery.service; do
        test "$(systemctl --user is-active "${unit}" 2>/dev/null || true)" = inactive
        test "$(systemctl --user is-enabled "${unit}" 2>/dev/null || true)" = disabled
    done
    for unit in acp-r6-rollback.service acp-r6-rollout.service \
        acp-r7-rollback.service acp-r7-rollout.service \
        acp-r8-release-guard.service acp-r8-rollback.service \
        acp-r8-rollout.service acp-cutover-run.service; do
        test "$(systemctl --user is-active "${unit}" 2>/dev/null || true)" = inactive
    done
    enabled="$(systemctl --user is-enabled acp-r8-release-guard.service 2>/dev/null || true)"
    [[ "${enabled:-not-found}" =~ ^(disabled|not-found)$ ]]
    systemctl --user is-active --quiet herdr-server.service
    pid="$(systemctl --user show --value -p MainPID herdr-server.service)"
    test "${pid}" -gt 1
    readlink -f "/proc/${pid}/exe" >"${TRANSACTION}/prestate/herdr-exe"
    tr '\0' '\n' <"/proc/${pid}/environ" \
        | sed -n 's/^PATH=//p' >"${TRANSACTION}/prestate/herdr-path"
    test "$(wc -l <"${TRANSACTION}/prestate/herdr-path")" -eq 1
    capture_snapshot "${TRANSACTION}/prestate/herdr-topology.json"
    atomic_text "${TRANSACTION}/prestate/herdr-was-active" yes
    # The durable completion marker is written only after every regular backup
    # byte and every directory entry (including symlink names) is on disk.
    sync_backup_tree
    atomic_text "${TRANSACTION}/backup-complete" BACKUP_COMPLETE
}

on_error() {
    local status="$?"
    trap - ERR HUP INT TERM
    atomic_text "${TRANSACTION}/phase" validation_failed || true
    exec 9>&-
    "${RELEASE}/rollback-r8" || true
    exit "${status}"
}

on_signal() {
    local status="$1"
    trap - ERR HUP INT TERM
    atomic_text "${TRANSACTION}/phase" validation_failed || true
    exec 9>&-
    "${RELEASE}/rollback-r8" || true
    exit "${status}"
}

test -d "${RELEASE}"
test -f "${MANIFEST}"
test -f "${PRIVATE_ENV}"
# Stale recovery executes code from the candidate release, so authenticate and
# durably pin that tree before consulting any old transaction marker.
"${RELEASE}/release-integrity" verify --manifest "${MANIFEST}" >/dev/null
fsync_published_release
python3 -I "${RELEASE}/recover-stale-r8-artifacts" prepare
test ! -e "${TRANSACTION}"
test ! -e "${TENDWIRE_CANDIDATE}"
test ! -e "${HERDRES_CANDIDATE}"
test ! -e "${TENDWIRE_SOCKET}" && test ! -L "${TENDWIRE_SOCKET}"
"${RELEASE}/migrate-current-pane" --preflight-only

install -d -m 700 "$(dirname "${LOCK}")"
exec 9>"${LOCK}"
chmod 600 "${LOCK}"
flock -n 9
TRANSACTION_STAGE="$(mktemp -d "$(dirname "${TRANSACTION}")/.frozen-0b94403-r8.initializing.XXXXXX")"
chmod 700 "${TRANSACTION_STAGE}"
trap 'rm -rf -- "${TRANSACTION_STAGE}"' EXIT
write_prepare_owner "${TRANSACTION_STAGE}"
atomic_text "${TRANSACTION_STAGE}/phase" snapshotting
sync -d "${TRANSACTION_STAGE}"
mv -T "${TRANSACTION_STAGE}" "${TRANSACTION}"
TRANSACTION_STAGE=
sync -d "$(dirname "${TRANSACTION}")"
trap - EXIT
snapshot_prestate
atomic_text "${TRANSACTION}/phase" preparing
trap on_error ERR
trap 'on_signal 129' HUP
trap 'on_signal 130' INT
trap 'on_signal 143' TERM

# This marker is durable before the first mutation outside the transaction
# directory. Stale recovery may archive an ownerless snapshot only when this
# marker is absent; otherwise it must execute the exact rollback path.
atomic_text "${TRANSACTION}/live-mutation-started" LIVE_MUTATION_STARTED

install -d -m 700 "${TENDWIRE_CANDIDATE}" "${HERDRES_CANDIDATE}"
install -d -m 755 \
    /home/smith/.config/systemd/user/tendwired.service.d \
    /home/smith/.config/systemd/user/herdr-server.service.d \
    /home/smith/.config/systemd/user/herdres.service.d \
    /home/smith/.config/systemd/user/herdres-gateway.service.d
# Arm the recovery/control plane on disk before installing any live service
# drop-in.  A reboot after the guard is enabled but before commit observes the
# fail-closed `preparing` phase and invokes rollback.
# A host failure at any later point therefore either leaves the old runtime
# untouched or boots into a fail-closed guard with an available rollback unit.
install -m 644 "${RELEASE}/systemd/acp-r8-rollback.service" "${INSTALLED_FILES[6]}"
install -m 644 "${RELEASE}/systemd/acp-r8-release-guard.service" "${INSTALLED_FILES[5]}"
install -m 644 "${RELEASE}/systemd/acp-r8-rollout.service" "${INSTALLED_FILES[7]}"
systemctl --user daemon-reload
systemctl --user enable acp-r8-release-guard.service
test "$(systemctl --user is-enabled acp-r8-release-guard.service)" = enabled
sync "${INSTALLED_FILES[5]}" "${INSTALLED_FILES[6]}" "${INSTALLED_FILES[7]}" \
    "${GUARD_ENABLE_LINK}"
sync -d /home/smith/.config/systemd/user \
    /home/smith/.config/systemd/user/default.target.wants

install -m 644 "${RELEASE}/systemd/acp-frozen-live-monitor.service" "${INSTALLED_FILES[4]}"
install -m 644 "${RELEASE}/systemd/herdr-99-acp-adapter.conf" "${INSTALLED_FILES[1]}"
install -m 644 "${RELEASE}/systemd/tendwired-99.conf" "${INSTALLED_FILES[0]}"
install -m 644 "${RELEASE}/systemd/herdres-99.conf" "${INSTALLED_FILES[2]}"
install -m 644 "${RELEASE}/systemd/herdres-gateway-99.conf" "${INSTALLED_FILES[3]}"
systemctl --user daemon-reload

next_link=/home/smith/.local/share/acp-runtime/.active-r8.$$
ln -s "${RELEASE}" "${next_link}"
mv -Tf "${next_link}" "${ACTIVE}"
sync -d /home/smith/.local/share/acp-runtime
"${RELEASE}/guarded-herdr-restart-r8"

herdr_pid="$(systemctl --user show --value -p MainPID herdr-server.service)"
test "${herdr_pid}" -gt 1
python3 - "${TRANSACTION}/herdr-baseline.json" "${herdr_pid}" <<'PY'
import json
import os
import sys

path, raw_pid = sys.argv[1:]
pid = int(raw_pid)
value = {
    "schema_version": 1,
    "pid": pid,
    "start_time": open(f"/proc/{pid}/stat", encoding="ascii").read().rsplit(") ", 1)[1].split()[19],
    "boot_id": open("/proc/sys/kernel/random/boot_id", encoding="ascii").read().strip(),
}
temporary = f"{path}.tmp.{os.getpid()}"
descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC, 0o600)
try:
    body = (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()
    os.write(descriptor, body)
    os.fsync(descriptor)
finally:
    os.close(descriptor)
os.replace(temporary, path)
PY
"${RELEASE}/release-integrity" verify --manifest "${MANIFEST}" >/dev/null
"${RELEASE}/attest-r8" installed >/dev/null

atomic_text "${TRANSACTION}/phase" prepared
"${TOPIC_PYTHON}" -I "${RELEASE}/reset-telegram-topics"
atomic_text "${TRANSACTION}/phase" committing
"${TOPIC_PYTHON}" -I "${RELEASE}/reset-telegram-topics" --apply
"${TOPIC_PYTHON}" -I "${RELEASE}/reset-telegram-topics" --gate-presenter

systemctl --user start --no-block acp-r8-rollout.service
trap - ERR HUP INT TERM
exec 9>&-
printf 'R8_LIVE_PREPARED %s\n' "${RELEASE_ID}"
