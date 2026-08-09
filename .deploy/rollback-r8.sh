#!/usr/bin/env bash
set -Eeuo pipefail

readonly RELEASE=/home/smith/.local/share/acp-runtime/releases/67568f32-0b94403-f50af73-r8
readonly ACTIVE=/home/smith/.local/share/acp-runtime/active
readonly TRANSACTION=/home/smith/.local/state/acp-cutover/frozen-0b94403-r8
readonly PHASE=${TRANSACTION}/phase
readonly PRESTATE=${TRANSACTION}/prestate
readonly LOCK=/home/smith/.local/state/acp-cutover/frozen-0b94403-r8.lock
readonly RESTORE=${RELEASE}/restore-current-pane
readonly MIGRATION_STATE=/home/smith/.local/state/acp-pane-migration/current
readonly MIGRATION_LOCK=/home/smith/.local/state/acp-pane-migration/current.lock
readonly TENDWIRE_CANDIDATE=/home/smith/.local/share/tendwire/candidates/0b94403-r8
readonly HERDRES_CANDIDATE=/home/smith/.local/share/herdres/candidates/f50af73-r8
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
readonly CONFIG_LAST_INDEX=9
readonly STATE_FIRST_INDEX=10

entry_fingerprint() {
    python3 - "$1" <<'PY'
import hashlib
import json
import os
import stat
import sys

root = os.path.abspath(sys.argv[1])

def xattrs(path, *, follow):
    try:
        names = sorted(os.listxattr(path, follow_symlinks=follow))
        return [[name, os.getxattr(path, name, follow_symlinks=follow).hex()] for name in names]
    except OSError:
        return []

def row(path, relative):
    metadata = os.lstat(path)
    common = {
        "path": relative,
        "mode": stat.S_IMODE(metadata.st_mode),
        "uid": metadata.st_uid,
        "gid": metadata.st_gid,
        "mtime_ns": metadata.st_mtime_ns,
    }
    if stat.S_ISLNK(metadata.st_mode):
        return {**common, "kind": "link", "target": os.readlink(path), "xattrs": xattrs(path, follow=False)}
    if stat.S_ISREG(metadata.st_mode):
        digest = hashlib.sha256()
        with open(path, "rb") as source:
            for block in iter(lambda: source.read(1_048_576), b""):
                digest.update(block)
        return {**common, "kind": "file", "sha256": digest.hexdigest(), "xattrs": xattrs(path, follow=True)}
    if stat.S_ISDIR(metadata.st_mode):
        return {**common, "kind": "dir", "xattrs": xattrs(path, follow=True)}
    raise SystemExit("unsupported rollback entry")

rows = [row(root, ".")]
if stat.S_ISDIR(os.lstat(root).st_mode):
    for current, directories, files in os.walk(root, topdown=True, followlinks=False):
        directories.sort()
        files.sort()
        for name in directories + files:
            path = os.path.join(current, name)
            rows.append(row(path, os.path.relpath(path, root)))
print(json.dumps(rows, sort_keys=True, separators=(",", ":")))
PY
}

atomic_text() {
    local path="$1" value="$2"
    printf '%s\n' "${value}" >"${path}.tmp.$$"
    chmod 600 "${path}.tmp.$$"
    sync "${path}.tmp.$$"
    mv -f "${path}.tmp.$$" "${path}"
    sync -d "$(dirname "${path}")"
}

restore_entries() {
    local first="$1" last="$2" index path state temporary
    for ((index=first; index<=last; index++)); do
        path="${SNAPSHOT_PATHS[$index]}"
        state="$(<"${PRESTATE}/files/${index}.state")"
        case "${state}" in
            present)
                test -e "${PRESTATE}/files/${index}.entry" \
                    || test -L "${PRESTATE}/files/${index}.entry"
                install -d -m 755 "$(dirname "${path}")"
                temporary="${path}.rollback-r8.$$"
                rm -rf -- "${temporary}"
                cp -a -- "${PRESTATE}/files/${index}.entry" "${temporary}"
                rm -rf -- "${path}"
                mv -Tf "${temporary}" "${path}"
                ;;
            absent)
                rm -rf -- "${path}"
                ;;
            *) return 1 ;;
        esac
    done
}

verify_restored_prestate() {
    local index path state unit expected actual
    test "$(readlink -f "${ACTIVE}")" = "$(<"${PRESTATE}/active-target")"
    for index in "${!SNAPSHOT_PATHS[@]}"; do
        path="${SNAPSHOT_PATHS[$index]}"
        state="$(<"${PRESTATE}/files/${index}.state")"
        if [[ "${state}" = present ]]; then
            test -e "${path}" || test -L "${path}"
            test "$(entry_fingerprint "${PRESTATE}/files/${index}.entry")" \
                = "$(entry_fingerprint "${path}")"
        else
            test ! -e "${path}" && test ! -L "${path}"
        fi
    done
    # Unit enablement is restored by the exact backed-up symlink entry, rather
    # than collapsing linked/masked/indirect states through `systemctl enable`.
    for unit in "${TRACKED_UNITS[@]}"; do
        expected="$(<"${PRESTATE}/units/${unit}.enabled")"
        actual="$(systemctl --user is-enabled "${unit}" 2>/dev/null || true)"
        test "${actual:-not-found}" = "${expected}"
        if [[ "${unit}" != acp-r8-rollback.service ]]; then
            expected="$(<"${PRESTATE}/units/${unit}.active")"
            actual="$(systemctl --user is-active "${unit}" 2>/dev/null || true)"
            test "${actual:-inactive}" = "${expected:-inactive}"
        fi
    done
    systemctl --user is-active --quiet herdr-server.service
    systemctl --user show \
        -p LoadState -p ActiveState -p SubState -p UnitFileState \
        -p Result -p ExecMainStatus acp-frozen-live-monitor.service \
        | cmp -s - "${PRESTATE}/frozen-monitor-state"
    for unit in tendwired.service herdres.service herdres-gateway.service; do
        test "$(systemctl --user is-active "${unit}" 2>/dev/null || true)" != active
    done
}

capture_snapshot() {
    local destination="$1"
    (umask 077; "${ACTIVE}/herdr" api snapshot >"${destination}.tmp.$$")
    chmod 600 "${destination}.tmp.$$"
    mv -f "${destination}.tmp.$$" "${destination}"
}

verify_topology() {
    "${RELEASE}/topology-normalizer" \
        "${PRESTATE}/herdr-topology.json" \
        "${TRANSACTION}/rollback-herdr-topology.json" \
        --session 019f96b6-3f4e-74a0-9ad9-6fbf68203f74 \
        --pane w53:p8 \
        --cwd /home/smith/tendwire
}

verify_herdr_process() {
    local pid actual_path actual_exe
    pid="$(systemctl --user show --value -p MainPID herdr-server.service)"
    test "${pid}" -gt 1
    actual_exe="$(readlink -f "/proc/${pid}/exe")"
    test "${actual_exe}" = "$(<"${PRESTATE}/herdr-exe")"
    actual_path="$(tr '\0' '\n' <"/proc/${pid}/environ" | sed -n 's/^PATH=//p')"
    test "${actual_path}" = "$(<"${PRESTATE}/herdr-path")"
}

install -d -m 700 "$(dirname "${LOCK}")"
exec 9>"${LOCK}"
chmod 600 "${LOCK}"
flock -w 180 9
test "$(<"${TRANSACTION}/backup-complete")" = BACKUP_COMPLETE
if [[ -r "${PHASE}" && "$(<"${PHASE}")" = rolled_back ]]; then
    if verify_herdr_process \
        && verify_restored_prestate \
        && test -f "${TRANSACTION}/rollback-herdr-topology.json" \
        && verify_topology; then
        exit 0
    fi
fi
trap - ERR HUP INT TERM
atomic_text "${PHASE}" rolling_back

systemctl --user stop \
    acp-frozen-live-monitor.service tendwired.service herdres.service \
    herdres-gateway.service acp-r8-release-guard.service >/dev/null 2>&1 || true
for unit in acp-frozen-live-monitor.service tendwired.service herdres.service \
    herdres-gateway.service acp-r8-release-guard.service; do
    test "$(systemctl --user is-active "${unit}" 2>/dev/null || true)" != active
done

# Restore configuration and the exact previous active-release entry before
# requiring any Herdr operation.  This is what makes a failed candidate binary
# recoverable: the old unit/drop-in and old CLI are back on disk first.
# Keep the r8 recovery unit files and enablement link armed until the old
# runtime, pane, topology, and candidate state have all been restored.
restore_entries 0 4
restore_entries "${CONFIG_LAST_INDEX}" "${CONFIG_LAST_INDEX}"
systemctl --user daemon-reload

if ! verify_herdr_process; then
    systemctl --user restart herdr-server.service
    for _attempt in $(seq 1 120); do
        if systemctl --user is-active --quiet herdr-server.service \
            && verify_herdr_process; then
            break
        fi
        sleep 1
        test "${_attempt}" -lt 120
    done
fi

# The restored Herdr process can now dismantle an ACP console if migration had
# begun, or return immediately when the conventional session never moved.
"${RESTORE}"
capture_snapshot "${TRANSACTION}/rollback-herdr-topology.json"
verify_topology

# Candidate databases, socket, and the shared pane-migration state are restored
# only after the console helper has consumed its candidate anchor.
restore_entries "${STATE_FIRST_INDEX}" "$((${#SNAPSHOT_PATHS[@]} - 1))"
sync

# Disarm boot recovery only after the functional old system is exact.  Remove
# the enablement link first; any power loss after this point leaves merely inert
# candidate control files, never a candidate runtime or ACP pane.
restore_entries 8 8
restore_entries 5 7
sync -d /home/smith/.config/systemd/user \
    /home/smith/.config/systemd/user/default.target.wants
systemctl --user daemon-reload
# The candidate monitor may have failed before rollback. Its preflight state is
# required to be canonical inactive/success, so reset its systemd result after
# restoring the exact fragment and before comparing the frozen monitor state.
systemctl --user reset-failed acp-frozen-live-monitor.service >/dev/null 2>&1 || true
verify_restored_prestate

reset_started=false
if [[ -e "${TRANSACTION}/telegram-topic-reset-started.json" ]]; then
    reset_started=true
fi
presenter_started=false
if [[ -e "${TRANSACTION}/telegram-topic-presenter-evidence.json" ]]; then
    presenter_started=true
fi
python3 - "${TRANSACTION}/rollback-evidence.json" "${reset_started}" \
    "${presenter_started}" <<'PY'
import json
import os
import sys
from datetime import UTC, datetime

path, reset_started_raw, presenter_started_raw = sys.argv[1:]
reset_started = reset_started_raw == "true"
presenter_started = presenter_started_raw == "true"
value = {
    "schema_version": 1,
    "status": "rolled_back",
    "exact_local_prestate_restored": True,
    "exact_prestate_restored": not reset_started and not presenter_started,
    "writers_active": False,
    "native_session_restored": True,
    "telegram_topic_reset_started": reset_started,
    "telegram_topic_reset_reversed": False if reset_started else None,
    "telegram_candidate_topic_may_remain": presenter_started,
    "telegram_external_exception_recorded": reset_started or presenter_started,
    "telegram_topic_reset_was_user_authorized": True,
    "completed_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
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
atomic_text "${PHASE}" rolled_back
printf 'R8_ROLLBACK_COMPLETE\n'
