#!/usr/bin/env bash
set -Eeuo pipefail

readonly SOURCE=/home/smith/tendwire
readonly DEPLOY=${SOURCE}/.deploy
readonly RELEASE_LOCK=${DEPLOY}/r8-release-lock.json
readonly APP_REVISION=0b944031c94e99b9f5fac439a850e8b823e8b1f1
readonly APP_TREE=c3745fb8a8f5de3049f1e08f6468e20cb16f5fe0
readonly HERDRES_REVISION=f50af733033089020ace3b15e686299cb8a67f1e
readonly HERDRES_TREE=566a75fca9a8b7b54b111bfc5e87d5233c2002a8
readonly ADAPTER_REVISION=7cb0524624f2e730f48c3dac9b547ca130964ae9
readonly ADAPTER_TREE=69780d844fb96a07a46b2094b29c33c90c484a62
readonly ADAPTER_ARCHIVE_SHA256=ec99e386926feb32ae632c71e5d61c103ffc5dd235fbc1d7d008b6a074aca5dd
readonly ADAPTER_PATCH_SHA256=58350f0bad6e1dbd61c2a6767b98517035ded3cfd7df1aa4d227fdd92f34ae91
readonly ADAPTER_LOCK_SHA256=a87fc2305ce7a7c49fe2fd2da61ff4d93bfbc4db6cc09a4b51976a438719d101
readonly ADAPTER_BUNDLE_SHA256=ab2dcb6ab0866775895b245a46069baba357eda07ba98b818ab215d70c2dbf60
readonly ADAPTER_PROVENANCE_SHA256=a5135f4d0fbbddc0d0bfd423454ded4321753439b240c55d81c260f0ac32b7d1
readonly ADAPTER_RUNTIME_SHA256=c739f60b53771b959be9a510687d33f000e453715114241c426b589903290303
readonly ADAPTER_RUNTIME_ENTRIES=33
readonly HERDR_REVISION=67568f327cea67bdd1d197feff621c45476da84c
readonly HERDR_TREE=def32e75c4751d2acd504fdfc6de9303ab394b6b
readonly HERDR_ARCHIVE_SHA256=6135ff0d3cc2f4a82f22e410e846cfe4fdcedbcab694fd99c3ac4a1be4674752
readonly HERDR_BINARY_SHA256=9be82154a29ec730dd8ce3a4c063e12847cadb0c1c81fa4c64782057cd4137f0
readonly HERDR_ELF_BUILD_ID=2eef96277f09679f9f64bdcf4ca4aa0211001e43
readonly HERDR_EMBEDDED_VERSION='herdr 0.7.5-r8.67568f327cea67bdd1d197feff621c45476da84c'
readonly HERDR_PROVENANCE_SHA256=5c41a0e5b5ba5d26eb1f4229bde8f8f7fae1ff7316b8d809bd1239bb1f4a9b04
readonly HERDR_RUNTIME_SHA256=0cc868fee3ad5dc38e7c652e876e128d400faf6ccb9c60ae48fb06b4498ffba5
readonly HERDR_RUNTIME_ENTRIES=4
readonly HERDR_SOURCE=${SOURCE}/.worktrees/herdr-acp-console
readonly HERDR_RUNTIME=/home/smith/.local/share/herdr-runtime/acp-${HERDR_REVISION}
readonly HERDR_RUNTIME_BUILD=/home/smith/.local/share/herdr-runtime/.acp-${HERDR_REVISION}.building
readonly HERDR_TARGET=/home/smith/.local/share/herdr-runtime/.target-${HERDR_REVISION}.building
readonly ZIG=/home/smith/.local/share/build-tools/zig-0.15.2-linux-arm64/zig
readonly ZIG_SHA256=931b8e9a9327dac87bc439067f07604219510fa541133ff6e542c6243968cc86
readonly ZIG_GLOBAL_CACHE=${SOURCE}/.tmp-zig-cache
readonly OLD_RELEASE=/home/smith/.local/share/acp-runtime/releases/9026d9bc-7446533-3659994-r5
readonly OLD_RUNTIME=/home/smith/.local/share/tendwire-runtime/acp-7446533-3659994-r5
readonly OLD_MANIFEST=/home/smith/.local/share/acp-runtime/manifests/9026d9bc-7446533-3659994-r5.json
readonly RELEASE_ID=67568f32-0b94403-f50af73-r8
readonly NEW_RELEASE=/home/smith/.local/share/acp-runtime/releases/${RELEASE_ID}
readonly NEW_RUNTIME=/home/smith/.local/share/tendwire-runtime/acp-0b94403-f50af73-r8
readonly MANIFEST=/home/smith/.local/share/acp-runtime/manifests/${RELEASE_ID}.json
readonly PRIVATE_ENV=/home/smith/.config/herdres/frozen-0b94403-f50af73-r8.env
readonly TENDWIRE_CANDIDATE=/home/smith/.local/share/tendwire/candidates/0b94403-r8
readonly HERDRES_CANDIDATE=/home/smith/.local/share/herdres/candidates/f50af73-r8
readonly TRANSACTION=/home/smith/.local/state/acp-cutover/frozen-0b94403-r8
readonly RUNTIME_BUILD=/home/smith/.local/share/tendwire-runtime/.acp-0b94403-f50af73-r8.building
readonly RELEASE_BUILD=/home/smith/.local/share/acp-runtime/releases/.${RELEASE_ID}.building
readonly CODEX_ACP_ROOT=/home/smith/.local/share/acp-adapters/codex-acp-7cb0524624f2e730f48c3dac9b547ca130964ae9
readonly ADAPTER_BUILD=/home/smith/.local/share/acp-adapters/.codex-acp-7cb0524624f2e730f48c3dac9b547ca130964ae9.building
readonly HERDR_BINARY=${HERDR_RUNTIME}/herdr
readonly BUILD_STATE=/home/smith/.local/state/acp-cutover/r8-build-67568f32-0b94403-f50af73

runtime_published=0
release_published=0
private_env_published=0
manifest_published=0
adapter_published=0
herdr_published=0
build_complete=0
build_state_initialized=0
BUILD_STATE_TMP=
APP_SOURCE=
ADAPTER_SOURCE=
HERDR_SOURCE_TMP=
DEPLOY_SNAPSHOT=
PACKAGED_DEPLOY=
TOOLING_REVISION=
TOOLING_TREE=

build_phase() {
    local value="$1"
    if [[ "${build_state_initialized}" -ne 1 ]]; then
        return 0
    fi
    printf '%s\n' "${value}" >"${BUILD_STATE}/phase.tmp.$$"
    chmod 600 "${BUILD_STATE}/phase.tmp.$$"
    sync "${BUILD_STATE}/phase.tmp.$$"
    mv -f "${BUILD_STATE}/phase.tmp.$$" "${BUILD_STATE}/phase"
    sync -d "${BUILD_STATE}"
}

cleanup() {
    if [[ -n "${BUILD_STATE_TMP}" && -d "${BUILD_STATE_TMP}" ]]; then
        rm -rf -- "${BUILD_STATE_TMP}"
    fi
    # Exact fixed build paths are removable only after this process has
    # durably recorded that every one was initially absent.  A failure during
    # stale-owner preflight must never make this trap delete another builder's
    # artifacts.
    if [[ "${build_state_initialized}" -eq 1 ]]; then
        for owned_build_path in "${RUNTIME_BUILD}" "${RELEASE_BUILD}" \
            "${ADAPTER_BUILD}" "${HERDR_RUNTIME_BUILD}" "${HERDR_TARGET}"; do
            if [[ -d "${owned_build_path}" ]]; then
                chmod -R u+w "${owned_build_path}" || true
                rm -rf -- "${owned_build_path}"
            fi
        done
    fi
    if [[ -n "${WHEEL_DIR:-}" && -d "${WHEEL_DIR}" ]]; then
        rm -rf -- "${WHEEL_DIR}"
    fi
    if [[ -n "${APP_SOURCE}" && -d "${APP_SOURCE}" ]]; then
        rm -rf -- "${APP_SOURCE}"
    fi
    if [[ -n "${ADAPTER_SOURCE}" && -d "${ADAPTER_SOURCE}" ]]; then
        rm -rf -- "${ADAPTER_SOURCE}"
    fi
    if [[ -n "${HERDR_SOURCE_TMP}" && -d "${HERDR_SOURCE_TMP}" ]]; then
        rm -rf -- "${HERDR_SOURCE_TMP}"
    fi
    if [[ -n "${DEPLOY_SNAPSHOT}" && -d "${DEPLOY_SNAPSHOT}" ]]; then
        rm -rf -- "${DEPLOY_SNAPSHOT}"
    fi
    if [[ "${build_complete}" -eq 0 ]]; then
        build_phase failed || true
        if [[ "${manifest_published}" -eq 1 ]]; then
            rm -f -- "${MANIFEST}"
        fi
        rm -f -- "${BUILD_STATE}/owner.json" 2>/dev/null || true
        sync -d "${BUILD_STATE}" 2>/dev/null || true
        if [[ "${private_env_published}" -eq 1 ]]; then
            rm -f -- "${PRIVATE_ENV}"
        fi
        if [[ "${release_published}" -eq 1 && -d "${NEW_RELEASE}" ]]; then
            chmod -R u+w "${NEW_RELEASE}" || true
            rm -rf -- "${NEW_RELEASE}"
        fi
        if [[ "${runtime_published}" -eq 1 && -d "${NEW_RUNTIME}" ]]; then
            chmod -R u+w "${NEW_RUNTIME}" || true
            rm -rf -- "${NEW_RUNTIME}"
        fi
        if [[ "${adapter_published}" -eq 1 && -d "${CODEX_ACP_ROOT}" ]]; then
            chmod -R u+w "${CODEX_ACP_ROOT}" || true
            rm -rf -- "${CODEX_ACP_ROOT}"
        fi
        if [[ "${herdr_published}" -eq 1 && -d "${HERDR_RUNTIME}" ]]; then
            chmod -R u+w "${HERDR_RUNTIME}" || true
            rm -rf -- "${HERDR_RUNTIME}"
        fi
    fi
}
trap cleanup EXIT

fsync_published_artifacts() {
    python3 - "${NEW_RUNTIME}" "${NEW_RELEASE}" "${CODEX_ACP_ROOT}" \
        "${HERDR_RUNTIME}" \
        "${PRIVATE_ENV}" "${MANIFEST}" <<'PY'
import os
import stat
import sys

def fsync_regular(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)

def fsync_directory(path):
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
        fsync_regular(root)
    elif stat.S_ISDIR(metadata.st_mode):
        for current, names, files in os.walk(root, topdown=True, followlinks=False):
            directories.append(current)
            for name in files:
                path = os.path.join(current, name)
                item = os.lstat(path)
                if stat.S_ISREG(item.st_mode):
                    fsync_regular(path)
                elif not stat.S_ISLNK(item.st_mode):
                    raise SystemExit("published tree contains an unsupported entry")
            for name in names:
                item = os.lstat(os.path.join(current, name))
                if not (stat.S_ISDIR(item.st_mode) or stat.S_ISLNK(item.st_mode)):
                    raise SystemExit("published tree contains an unsupported entry")
        for directory in reversed(directories):
            fsync_directory(directory)
    else:
        raise SystemExit("published artifact has an unsupported type")
    fsync_directory(os.path.dirname(root))
PY
}

python3 - "${RELEASE_LOCK}" "${SOURCE}" \
    "${APP_REVISION}" "${APP_TREE}" "${HERDRES_REVISION}" "${HERDRES_TREE}" \
    "${ADAPTER_REVISION}" "${ADAPTER_TREE}" "${ADAPTER_BUNDLE_SHA256}" \
    "${ADAPTER_ARCHIVE_SHA256}" "${ADAPTER_PATCH_SHA256}" "${ADAPTER_LOCK_SHA256}" \
    "${ADAPTER_PROVENANCE_SHA256}" "${ADAPTER_RUNTIME_SHA256}" "${ADAPTER_RUNTIME_ENTRIES}" \
    "${HERDR_REVISION}" "${HERDR_TREE}" "${HERDR_ARCHIVE_SHA256}" \
    "${HERDR_BINARY_SHA256}" "${HERDR_ELF_BUILD_ID}" "${HERDR_EMBEDDED_VERSION}" \
    "${HERDR_PROVENANCE_SHA256}" "${HERDR_RUNTIME_SHA256}" "${HERDR_RUNTIME_ENTRIES}" <<'PY'
import hashlib
import json
import os
import subprocess
import sys

lock_path, source, app_commit, app_tree, herdres_commit, herdres_tree, adapter_commit, adapter_tree, adapter_bundle, adapter_archive, adapter_patch, adapter_lock, adapter_provenance, adapter_runtime, raw_adapter_entries, herdr_commit, herdr_tree, herdr_archive, herdr_binary, herdr_build_id, herdr_version, herdr_provenance, herdr_runtime, raw_herdr_entries = sys.argv[1:]
if any("REQUIRED" in value for value in (herdr_commit, herdr_tree, herdr_archive, herdr_binary, herdr_build_id, herdr_version, herdr_provenance, herdr_runtime, raw_herdr_entries)):
    raise SystemExit("DEPLOYMENT BLOCKED: authorized Herdr identity is required")
lock = json.load(open(lock_path, encoding="utf-8"))
expected = {
    "tendwire": (app_commit, app_tree),
    "herdres": (herdres_commit, herdres_tree),
    "codex_acp": (adapter_commit, adapter_tree),
    "herdr": (herdr_commit, herdr_tree),
}
if lock.get("schema_version") != 1 or lock.get("release") != "r8":
    raise SystemExit("invalid r8 release lock")
for component, (commit, tree) in expected.items():
    row = lock.get(component) or {}
    if row.get("commit") != commit or row.get("tree") != tree:
        raise SystemExit(f"{component} release-lock identity mismatch")
reviewed_ancestors = {
    "tendwire": "1f277cd562f3852cfbec4e52b2ffc8c406550fc4",
    "herdres": "36599949daa64f68494d04f96a3bfee31904a804",
    "herdr": "9026d9bc5a12d9adc2d9f68ebdc564133e4098b4",
}
for component, ancestor in reviewed_ancestors.items():
    if (lock.get(component) or {}).get("reviewed_ancestor") != ancestor:
        raise SystemExit(f"{component} reviewed ancestor mismatch")
if (lock.get("codex_acp") or {}).get("bundle_sha256") != adapter_bundle:
    raise SystemExit("adapter bundle identity mismatch")
adapter = lock.get("codex_acp") or {}
if (
    adapter.get("git_archive_sha256") != adapter_archive
    or adapter.get("git_patch_sha256") != adapter_patch
    or adapter.get("package_lock_sha256") != adapter_lock
    or adapter.get("provenance_sha256") != adapter_provenance
    or adapter.get("runtime_tree_sha256") != adapter_runtime
    or adapter.get("runtime_entries") != int(raw_adapter_entries)
    or adapter.get("upstream_commit") != "5faefec5d55ded33c54b68ffec93def4f6c547f5"
    or adapter.get("upstream_tag_object") != "c82ecc31a5b73014e13a91fa1c3251252ce55f94"
    or adapter.get("node_version") != "v22.22.3"
    or adapter.get("npm_version") != "10.9.8"
    or adapter.get("platform") != "linux-arm64"
    or adapter.get("version") != "@agentclientprotocol/codex-acp 1.1.14"
):
    raise SystemExit("adapter provenance contract mismatch")
herdr = lock.get("herdr") or {}
if (
    herdr.get("git_archive_sha256") != herdr_archive
    or herdr.get("binary_sha256") != herdr_binary
    or herdr.get("binary_build_id") != herdr_build_id
    or herdr.get("embedded_version") != herdr_version
    or herdr.get("provenance_sha256") != herdr_provenance
    or herdr.get("runtime_tree_sha256") != herdr_runtime
    or herdr.get("runtime_entries") != int(raw_herdr_entries)
    or herdr.get("rustc_version") != "rustc 1.96.1 (31fca3adb 2026-06-26)"
    or herdr.get("cargo_version") != "cargo 1.96.1 (356927216 2026-06-26)"
    or herdr.get("zig_version") != "0.15.2"
    or herdr.get("zig_sha256") != "931b8e9a9327dac87bc439067f07604219510fa541133ff6e542c6243968cc86"
    or herdr.get("platform") != "linux-arm64"
):
    raise SystemExit("Herdr build provenance mismatch")
inventory = lock.get("operational_sha256")
if not isinstance(inventory, dict) or not inventory:
    raise SystemExit("r8 operational inventory is empty")
tracked = subprocess.run(
    ["git", "-C", source, "ls-files", "--", ".deploy"],
    check=True, capture_output=True, text=True,
).stdout.splitlines()
tracked_r8 = {
    path for path in tracked
    if path == ".deploy/r8-release-lock.json"
    or path == ".deploy/topology-normalizer-r8.py"
    or path == ".deploy/recover-stale-r8-artifacts.py"
    or path.endswith("-r8.py") or path.endswith("-r8.sh")
    or (os.path.basename(path).startswith("acp-r8-") and path.endswith(".service"))
    or path.endswith("adapter-r8.conf")
    or path.startswith(".deploy/tests/") and "r8" in os.path.basename(path)
}
expected_paths = set(inventory) | {".deploy/r8-release-lock.json"}
if tracked_r8 != expected_paths:
    raise SystemExit("tracked r8 operational allowlist is not exact")
for relative, expected_digest in inventory.items():
    path = os.path.join(source, relative)
    digest = hashlib.sha256(open(path, "rb").read()).hexdigest()
    if digest != expected_digest:
        raise SystemExit(f"reviewed r8 operational digest mismatch: {relative}")
status = subprocess.run(
    ["git", "-C", source, "status", "--porcelain=v1", "--untracked-files=all", "--", *sorted(expected_paths)],
    check=True, capture_output=True, text=True,
).stdout
if status:
    raise SystemExit("reviewed r8 operational allowlist is not clean")
PY
test "$(git -C "${SOURCE}" rev-parse "${APP_REVISION}^{tree}")" = "${APP_TREE}"
git -C "${SOURCE}" merge-base --is-ancestor \
    1f277cd562f3852cfbec4e52b2ffc8c406550fc4 "${APP_REVISION}"
test "$(git -C "${SOURCE}/.worktrees/reduction-herdres-release-fixes" rev-parse HEAD)" = "${HERDRES_REVISION}"
test "$(git -C "${SOURCE}/.worktrees/reduction-herdres-release-fixes" rev-parse 'HEAD^{tree}')" = "${HERDRES_TREE}"
git -C "${SOURCE}/.worktrees/reduction-herdres-release-fixes" merge-base --is-ancestor \
    36599949daa64f68494d04f96a3bfee31904a804 "${HERDRES_REVISION}"
test "$(git -C "${SOURCE}/.worktrees/codex-acp-steering-lifecycle" rev-parse HEAD)" = "${ADAPTER_REVISION}"
test "$(git -C "${SOURCE}/.worktrees/codex-acp-steering-lifecycle" rev-parse 'HEAD^{tree}')" = "${ADAPTER_TREE}"
test "$(git -C "${SOURCE}/.worktrees/codex-acp-steering-lifecycle" show --format= --binary "${ADAPTER_REVISION}" | sha256sum | cut -d ' ' -f 1)" \
    = "${ADAPTER_PATCH_SHA256}"
test "$(node --version)" = v22.22.3
test "$(npm --version)" = 10.9.8
test "$(uname -m)" = aarch64
test -d "${HERDR_SOURCE}"
test "$(git -C "${HERDR_SOURCE}" rev-parse "${HERDR_REVISION}^{commit}")" = "${HERDR_REVISION}"
test "$(git -C "${HERDR_SOURCE}" rev-parse "${HERDR_REVISION}^{tree}")" = "${HERDR_TREE}"
git -C "${HERDR_SOURCE}" merge-base --is-ancestor \
    9026d9bc5a12d9adc2d9f68ebdc564133e4098b4 "${HERDR_REVISION}"
test "$(/home/smith/.cargo/bin/rustc --version)" = "rustc 1.96.1 (31fca3adb 2026-06-26)"
test "$(/home/smith/.cargo/bin/cargo --version)" = "cargo 1.96.1 (356927216 2026-06-26)"
test -x "${ZIG}"
test "$(sha256sum "${ZIG}" | cut -d ' ' -f 1)" = "${ZIG_SHA256}"
test "$("${ZIG}" version)" = 0.15.2
test "$(stat -c '%u:%g:%a:%F' "${ZIG_GLOBAL_CACHE}")" = "$(id -u):$(id -g):755:directory"
test -d "${SOURCE}/.worktrees/reduction-herdres-release-fixes"
test -d "${OLD_RUNTIME}"
test -x "${OLD_RELEASE}/validate-frozen-release"
old_validation="$(${OLD_RELEASE}/validate-frozen-release \
    --runtime "${OLD_RUNTIME}" --release "${OLD_RELEASE}" \
    --manifest "${OLD_MANIFEST}" --verify)"
python3 - "${old_validation}" <<'PY'
import json
import sys

if json.loads(sys.argv[1]) != {
    "release_id": "9026d9bc-7446533-3659994-r5",
    "schema_version": 1,
    "valid": True,
}:
    raise SystemExit("old release validation mismatch")
PY
TOOLING_REVISION="$(git -C "${SOURCE}" rev-parse HEAD)"
TOOLING_TREE="$(git -C "${SOURCE}" rev-parse 'HEAD^{tree}')"
DEPLOY_SNAPSHOT="$(mktemp -d /tmp/tendwire-r8-operational.XXXXXX)"
chmod 700 "${DEPLOY_SNAPSHOT}"
mapfile -t OPERATIONAL_PATHS < <(python3 - "${RELEASE_LOCK}" <<'PY'
import json
import sys

lock = json.load(open(sys.argv[1], encoding="utf-8"))
inventory = lock.get("operational_sha256")
if not isinstance(inventory, dict) or len(inventory) != 27:
    raise SystemExit("r8 operational inventory is not exact")
for path in sorted(set(inventory) | {".deploy/r8-release-lock.json"}):
    print(path)
PY
)
test "${#OPERATIONAL_PATHS[@]}" -eq 28
git -C "${SOURCE}" archive "${TOOLING_REVISION}" -- "${OPERATIONAL_PATHS[@]}" \
    | tar -x -C "${DEPLOY_SNAPSHOT}"
python3 - "${DEPLOY_SNAPSHOT}" "${RELEASE_LOCK}" <<'PY'
import hashlib
import json
import os
import stat
import sys

root, live_lock_path = map(os.path.abspath, sys.argv[1:])
snapshot_lock_path = os.path.join(root, ".deploy/r8-release-lock.json")
if open(snapshot_lock_path, "rb").read() != open(live_lock_path, "rb").read():
    raise SystemExit("archived r8 lock differs from the clean live lock")
lock = json.load(open(snapshot_lock_path, encoding="utf-8"))
inventory = lock["operational_sha256"]
expected = set(inventory) | {".deploy/r8-release-lock.json"}
actual = set()
for current, directories, files in os.walk(root, followlinks=False):
    for name in directories + files:
        path = os.path.join(current, name)
        metadata = os.lstat(path)
        if stat.S_ISLNK(metadata.st_mode):
            raise SystemExit("archived r8 tooling contains a symlink")
        if stat.S_ISREG(metadata.st_mode):
            actual.add(os.path.relpath(path, root))
if actual != expected:
    raise SystemExit("archived r8 operational allowlist is not exact")
for relative, expected_digest in inventory.items():
    digest = hashlib.sha256(open(os.path.join(root, relative), "rb").read()).hexdigest()
    if digest != expected_digest:
        raise SystemExit(f"archived r8 operational digest mismatch: {relative}")
PY
status="$(git -C "${SOURCE}" status --porcelain=v1 --untracked-files=all -- "${OPERATIONAL_PATHS[@]}")"
test -z "${status}"
PACKAGED_DEPLOY=${DEPLOY_SNAPSHOT}/.deploy
python3 -I "${PACKAGED_DEPLOY}/recover-stale-r8-artifacts.py" build
systemctl --user is-active --quiet herdr-server.service
for unit in tendwired.service herdres.service herdres-gateway.service; do
    if systemctl --user is-active --quiet "${unit}"; then
        exit 1
    fi
done

install -d -m 700 "$(dirname "${BUILD_STATE}")"
BUILD_STATE_TMP="$(mktemp -d "$(dirname "${BUILD_STATE}")/.r8-build-state.XXXXXX")"
chmod 700 "${BUILD_STATE_TMP}"
python3 - "${BUILD_STATE_TMP}" "$$" \
    "${NEW_RUNTIME}" "${NEW_RELEASE}" "${MANIFEST}" "${PRIVATE_ENV}" \
    "${RUNTIME_BUILD}" "${RELEASE_BUILD}" "${CODEX_ACP_ROOT}" \
    "${ADAPTER_BUILD}" "${HERDR_RUNTIME}" "${HERDR_RUNTIME_BUILD}" \
    "${HERDR_TARGET}" <<'PY'
import json
import os
import sys

root, raw_pid, *paths = sys.argv[1:]
pid = int(raw_pid)
if any(os.path.lexists(path) for path in paths):
    raise SystemExit("an r8 build artifact appeared before ownership was recorded")
boot_id = open("/proc/sys/kernel/random/boot_id", encoding="ascii").read().strip()
start = open(f"/proc/{pid}/stat", encoding="ascii").read().rsplit(") ", 1)[1].split()[19]

def atomic(name, value):
    target = os.path.join(root, name)
    temporary = f"{target}.tmp.{pid}"
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC, 0o600)
    try:
        body = (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()
        os.write(descriptor, body)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, target)

atomic("owned-artifacts.json", {"schema_version": 1, "all_initially_absent": True, "paths": paths})
atomic("owner.json", {
    "schema_version": 1, "tag": "R8_BUILD", "pid": pid, "start_time": start,
    "boot_id": boot_id, "uid": os.geteuid(),
})
phase = os.path.join(root, "phase")
descriptor = os.open(phase, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC, 0o600)
try:
    os.write(descriptor, b"building\n")
    os.fsync(descriptor)
finally:
    os.close(descriptor)
directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
try:
    os.fsync(directory)
finally:
    os.close(directory)
PY
mv -T "${BUILD_STATE_TMP}" "${BUILD_STATE}"
BUILD_STATE_TMP=
sync -d "$(dirname "${BUILD_STATE}")"
build_state_initialized=1
for path in "${NEW_RELEASE}" "${NEW_RUNTIME}" "${MANIFEST}" \
    "${PRIVATE_ENV}" "${TENDWIRE_CANDIDATE}" "${HERDRES_CANDIDATE}" \
    "${TRANSACTION}" "${RUNTIME_BUILD}" "${RELEASE_BUILD}" \
    "${CODEX_ACP_ROOT}" "${ADAPTER_BUILD}" "${HERDR_RUNTIME}" \
    "${HERDR_RUNTIME_BUILD}" "${HERDR_TARGET}"; do
    test ! -e "${path}"
done

# Build Herdr from the exact authorized Git object in a disposable checkout.
# The durable toolchain and every output identity are pinned by the release
# lock; no ambient binary can be inherited into the release.
# Use a commit-named owned source path, not a random mktemp path: Rust/native
# build products must not vary because an absolute source path leaked into an
# otherwise identical release binary.
HERDR_SOURCE_TMP="${HERDR_TARGET}/source-root"
herdr_checkout="${HERDR_SOURCE_TMP}/checkout"
herdr_archive="${HERDR_SOURCE_TMP}/herdr-${HERDR_REVISION}.tar"
install -d -m 700 "${HERDR_SOURCE_TMP}" "${herdr_checkout}"
git -C "${HERDR_SOURCE}" archive --format=tar --output="${herdr_archive}" \
    "${HERDR_REVISION}"
test "$(sha256sum "${herdr_archive}" | cut -d ' ' -f 1)" \
    = "${HERDR_ARCHIVE_SHA256}"
tar -x -f "${herdr_archive}" -C "${herdr_checkout}"
herdr_source_date_epoch="$(git -C "${HERDR_SOURCE}" show -s --format=%ct "${HERDR_REVISION}")"
case "${herdr_source_date_epoch}" in
    ''|*[!0-9]*) exit 1 ;;
esac
(
    cd "${herdr_checkout}"
    env -i \
        HOME=/home/smith \
        USER=smith \
        LANG=C \
        LC_ALL=C \
        TZ=UTC \
        PATH=/home/smith/.cargo/bin:/usr/bin:/bin \
        TMPDIR=/tmp \
        CARGO_HOME=/home/smith/.cargo \
        CARGO_NET_OFFLINE=true \
        CARGO_INCREMENTAL=0 \
        CARGO_BUILD_JOBS=2 \
        CARGO_TARGET_DIR="${HERDR_TARGET}" \
        SOURCE_DATE_EPOCH="${herdr_source_date_epoch}" \
        ZIG="${ZIG}" \
        ZIG_GLOBAL_CACHE_DIR="${ZIG_GLOBAL_CACHE}" \
        ZIG_LOCAL_CACHE_DIR="${HERDR_TARGET}/zig-local-cache" \
        HERDR_BUILD_CHANNEL=r8 \
        HERDR_BUILD_ID="${HERDR_REVISION}" \
        HERDR_BUILD_COMMIT="${HERDR_REVISION}" \
        /home/smith/.cargo/bin/cargo build --release --locked --offline \
            --jobs 2 --bin herdr
)
test -f "${HERDR_TARGET}/release/herdr"
install -d -m 755 "${HERDR_RUNTIME_BUILD}/source"
install -m 555 "${HERDR_TARGET}/release/herdr" "${HERDR_RUNTIME_BUILD}/herdr"
install -m 444 "${herdr_archive}" \
    "${HERDR_RUNTIME_BUILD}/source/herdr-${HERDR_REVISION}.tar"
test "$("${HERDR_RUNTIME_BUILD}/herdr" --version)" = "${HERDR_EMBEDDED_VERSION}"
test "$(sha256sum "${HERDR_RUNTIME_BUILD}/herdr" | cut -d ' ' -f 1)" \
    = "${HERDR_BINARY_SHA256}"
herdr_actual_build_id="$(readelf -n "${HERDR_RUNTIME_BUILD}/herdr" \
    | awk '/Build ID:/{print $3; exit}')"
test -n "${herdr_actual_build_id}"
test "${herdr_actual_build_id}" = "${HERDR_ELF_BUILD_ID}"
python3 - "${HERDR_RUNTIME_BUILD}/provenance.json" \
    "${HERDR_REVISION}" "${HERDR_TREE}" "${HERDR_ARCHIVE_SHA256}" \
    "${HERDR_BINARY_SHA256}" "${HERDR_ELF_BUILD_ID}" \
    "${HERDR_EMBEDDED_VERSION}" "${herdr_source_date_epoch}" <<'PY'
import json
import os
import sys

path, commit, tree, archive, binary, build_id, version, epoch = sys.argv[1:]
value = {
    "binary_build_id": build_id,
    "binary_sha256": binary,
    "build_channel": "r8",
    "build_id": commit,
    "cargo_version": "cargo 1.96.1 (356927216 2026-06-26)",
    "commit": commit,
    "embedded_version": version,
    "git_archive_sha256": archive,
    "platform": "linux-arm64",
    "rustc_version": "rustc 1.96.1 (31fca3adb 2026-06-26)",
    "schema_version": 1,
    "source_date_epoch": int(epoch),
    "tree": tree,
    "zig_sha256": "931b8e9a9327dac87bc439067f07604219510fa541133ff6e542c6243968cc86",
    "zig_version": "0.15.2",
}
descriptor = os.open(
    path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC, 0o444
)
try:
    body = (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()
    view = memoryview(body)
    while view:
        written = os.write(descriptor, view)
        if written < 1:
            raise OSError("Herdr provenance write made no progress")
        view = view[written:]
    os.fsync(descriptor)
finally:
    os.close(descriptor)
PY
test "$(sha256sum "${HERDR_RUNTIME_BUILD}/provenance.json" | cut -d ' ' -f 1)" \
    = "${HERDR_PROVENANCE_SHA256}"
find "${HERDR_RUNTIME_BUILD}" -type f ! -name herdr -exec chmod 444 {} +
find "${HERDR_RUNTIME_BUILD}" -type d -exec chmod 555 {} +
python3 - "${HERDR_RUNTIME_BUILD}" "${HERDR_RUNTIME_SHA256}" \
    "${HERDR_RUNTIME_ENTRIES}" "$(id -u)" "$(id -g)" <<'PY'
import hashlib
import json
import os
import stat
import sys
from pathlib import Path

root = Path(sys.argv[1])
expected_digest, raw_entries = sys.argv[2], sys.argv[3]
expected_uid, expected_gid = int(sys.argv[4]), int(sys.argv[5])
try:
    expected_entries = int(raw_entries)
except ValueError as error:
    raise SystemExit("authorized Herdr runtime entry count is required") from error
digest = hashlib.sha256()
count = 0
for path in sorted(root.rglob("*"), key=lambda item: item.relative_to(root).as_posix()):
    relative = path.relative_to(root).as_posix()
    metadata = path.lstat()
    if metadata.st_uid != expected_uid or metadata.st_gid != expected_gid:
        raise SystemExit("Herdr runtime ownership mismatch")
    if metadata.st_nlink != 1 and not stat.S_ISDIR(metadata.st_mode):
        raise SystemExit("Herdr runtime contains a multiply linked file")
    mode = stat.S_IMODE(metadata.st_mode)
    if stat.S_ISDIR(metadata.st_mode):
        kind, payload = "dir", ""
        if mode != 0o555:
            raise SystemExit("Herdr runtime directory mode mismatch")
    elif stat.S_ISREG(metadata.st_mode):
        kind, payload = "file", hashlib.sha256(path.read_bytes()).hexdigest()
        expected_mode = 0o555 if relative == "herdr" else 0o444
        if mode != expected_mode:
            raise SystemExit("Herdr runtime file mode mismatch")
    else:
        raise SystemExit("unsupported Herdr runtime entry")
    digest.update(json.dumps([relative, kind, mode, payload], separators=(",", ":")).encode() + b"\n")
    count += 1
if digest.hexdigest() != expected_digest or count != expected_entries:
    raise SystemExit("Herdr runtime tree identity mismatch")
PY
sync -f "${HERDR_RUNTIME_BUILD}"
mv -T "${HERDR_RUNTIME_BUILD}" "${HERDR_RUNTIME}"
herdr_published=1
sync -d "$(dirname "${HERDR_RUNTIME}")"

WHEEL_DIR="$(mktemp -d /tmp/tendwire-r8-wheel.XXXXXX)"
APP_SOURCE="$(mktemp -d /tmp/tendwire-r8-source.XXXXXX)"
git -C "${SOURCE}" archive "${APP_REVISION}" | tar -x -C "${APP_SOURCE}"
(
    cd "${APP_SOURCE}"
    "${SOURCE}/.venv/bin/python" -m hatchling build -t wheel -d "${WHEEL_DIR}"
)
wheel="${WHEEL_DIR}/tendwire-0.1.0rc5-py3-none-any.whl"
test -f "${wheel}"

# Reconstruct the adapter from its locked Git tree and npm lock. Never bless an
# ambient version-only installation: the built bundle digest is release data.
ADAPTER_SOURCE="$(mktemp -d /tmp/codex-acp-r8-source.XXXXXX)"
adapter_archive="${ADAPTER_SOURCE}/codex-acp.tar"
git -C "${SOURCE}/.worktrees/codex-acp-steering-lifecycle" \
    archive --format=tar --output="${adapter_archive}" "${ADAPTER_REVISION}"
test "$(sha256sum "${adapter_archive}" | cut -d ' ' -f 1)" = "${ADAPTER_ARCHIVE_SHA256}"
tar -x -f "${adapter_archive}" -C "${ADAPTER_SOURCE}"
rm -f -- "${adapter_archive}"
test "$(sha256sum "${ADAPTER_SOURCE}/package-lock.json" | cut -d ' ' -f 1)" \
    = "${ADAPTER_LOCK_SHA256}"
(
    cd "${ADAPTER_SOURCE}"
    npm ci --offline --no-audit --no-fund
    npm run build
)
test "$(sha256sum "${ADAPTER_SOURCE}/dist/index.js" | cut -d ' ' -f 1)" \
    = "${ADAPTER_BUNDLE_SHA256}"
install -d -m 755 "${ADAPTER_BUILD}/dist" "${ADAPTER_BUILD}/bin" \
    "${ADAPTER_BUILD}/source" "${ADAPTER_BUILD}/node_modules/@openai"
install -m 555 "${ADAPTER_SOURCE}/dist/index.js" "${ADAPTER_BUILD}/dist/index.js"
install -m 444 "${ADAPTER_SOURCE}/package.json" "${ADAPTER_BUILD}/package.json"
install -m 444 "${ADAPTER_SOURCE}/package-lock.json" "${ADAPTER_BUILD}/package-lock.json"
install -m 444 "${ADAPTER_SOURCE}/LICENSE" "${ADAPTER_BUILD}/LICENSE"
cp -a "${ADAPTER_SOURCE}/node_modules/@openai/codex" \
    "${ADAPTER_BUILD}/node_modules/@openai/codex"
cp -a "${ADAPTER_SOURCE}/node_modules/@openai/codex-linux-arm64" \
    "${ADAPTER_BUILD}/node_modules/@openai/codex-linux-arm64"
git -C "${SOURCE}/.worktrees/codex-acp-steering-lifecycle" archive --format=tar \
    --output="${ADAPTER_BUILD}/source/codex-acp-${ADAPTER_REVISION}.tar" \
    "${ADAPTER_REVISION}"
test "$(sha256sum "${ADAPTER_BUILD}/source/codex-acp-${ADAPTER_REVISION}.tar" | cut -d ' ' -f 1)" \
    = "${ADAPTER_ARCHIVE_SHA256}"
python3 - "${ADAPTER_BUILD}/provenance.json" <<'PY'
import json
import os
import sys

value = {
    "adapter_bundle_sha256": "ab2dcb6ab0866775895b245a46069baba357eda07ba98b818ab215d70c2dbf60",
    "adapter_commit": "7cb0524624f2e730f48c3dac9b547ca130964ae9",
    "adapter_tree": "69780d844fb96a07a46b2094b29c33c90c484a62",
    "entrypoint": "bin/codex-acp",
    "git_archive_sha256": "ec99e386926feb32ae632c71e5d61c103ffc5dd235fbc1d7d008b6a074aca5dd",
    "git_patch_sha256": "58350f0bad6e1dbd61c2a6767b98517035ded3cfd7df1aa4d227fdd92f34ae91",
    "node_version": "v22.22.3",
    "npm_version": "10.9.8",
    "package": "@agentclientprotocol/codex-acp",
    "package_lock_sha256": "a87fc2305ce7a7c49fe2fd2da61ff4d93bfbc4db6cc09a4b51976a438719d101",
    "platform": "linux-arm64",
    "schema_version": 1,
    "upstream_commit": "5faefec5d55ded33c54b68ffec93def4f6c547f5",
    "upstream_repository": "https://github.com/agentclientprotocol/codex-acp.git",
    "upstream_tag": "v1.1.14",
    "upstream_tag_object": "c82ecc31a5b73014e13a91fa1c3251252ce55f94",
    "version": "1.1.14",
}
path = sys.argv[1]
descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC, 0o444)
try:
    os.write(descriptor, (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode())
finally:
    os.close(descriptor)
PY
test "$(sha256sum "${ADAPTER_BUILD}/provenance.json" | cut -d ' ' -f 1)" \
    = "${ADAPTER_PROVENANCE_SHA256}"
ln -s ../dist/index.js "${ADAPTER_BUILD}/bin/codex-acp"
find "${ADAPTER_BUILD}" -type f -perm /111 -exec chmod 555 {} +
find "${ADAPTER_BUILD}" -type f ! -perm /111 -exec chmod 444 {} +
find "${ADAPTER_BUILD}" -type d -exec chmod 555 {} +
python3 - "${ADAPTER_BUILD}" "${ADAPTER_RUNTIME_SHA256}" "${ADAPTER_RUNTIME_ENTRIES}" <<'PY'
import hashlib
import json
import os
import stat
import sys
from pathlib import Path

root, expected_digest, raw_entries = Path(sys.argv[1]), sys.argv[2], int(sys.argv[3])
digest = hashlib.sha256()
count = 0
for path in sorted(root.rglob("*"), key=lambda item: item.relative_to(root).as_posix()):
    relative = path.relative_to(root).as_posix()
    metadata = path.lstat()
    mode = stat.S_IMODE(metadata.st_mode)
    if stat.S_ISLNK(metadata.st_mode):
        kind, payload = "link", os.readlink(path)
    elif stat.S_ISDIR(metadata.st_mode):
        kind, payload = "dir", ""
    elif stat.S_ISREG(metadata.st_mode):
        kind, payload = "file", hashlib.sha256(path.read_bytes()).hexdigest()
    else:
        raise SystemExit("unsupported adapter runtime entry")
    digest.update(json.dumps([relative, kind, mode, payload], separators=(",", ":")).encode() + b"\n")
    count += 1
if digest.hexdigest() != expected_digest or count != raw_entries:
    raise SystemExit("adapter runtime tree identity mismatch")
PY
mv -T "${ADAPTER_BUILD}" "${CODEX_ACP_ROOT}"
adapter_published=1
test "$("${CODEX_ACP_ROOT}/bin/codex-acp" --version)" \
    = "@agentclientprotocol/codex-acp 1.1.14"

cp -a --reflink=auto "${OLD_RUNTIME}" "${RUNTIME_BUILD}"
chmod -R u+w "${RUNTIME_BUILD}"
PIP_NO_INDEX=1 PIP_DISABLE_PIP_VERSION_CHECK=1 \
    "${RUNTIME_BUILD}/bin/python" -I -m pip install \
    --no-deps --force-reinstall "${wheel}"
"${RUNTIME_BUILD}/bin/python" -I - <<'PY'
import inspect
from tendwire.connectors.outbox import ConnectorOutboxAPI
from tendwire.store.projection import presentation_binding_row

source = inspect.getsource(ConnectorOutboxAPI.prepare)
assert "acpt_[0-9a-f]{24}" in source
assert callable(presentation_binding_row)
PY
find "${RUNTIME_BUILD}" -type f -exec chmod a-w {} +
find "${RUNTIME_BUILD}" -type d -exec chmod 555 {} +
build_phase publishing
runtime_published=1
mv -T "${RUNTIME_BUILD}" "${NEW_RUNTIME}"

install -d -m 755 "${RELEASE_BUILD}" "${RELEASE_BUILD}/systemd" \
    "${RELEASE_BUILD}/operational-snapshot"
cp -a "${DEPLOY_SNAPSHOT}/.deploy" "${RELEASE_BUILD}/operational-snapshot/.deploy"
python3 - "${RELEASE_BUILD}/tooling-identity.json" \
    "${TOOLING_REVISION}" "${TOOLING_TREE}" <<'PY'
import json
import os
import sys

path, commit, tree = sys.argv[1:]
descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC, 0o444)
try:
    body = json.dumps(
        {"schema_version": 1, "commit": commit, "tree": tree},
        sort_keys=True,
        separators=(",", ":"),
    ).encode() + b"\n"
    os.write(descriptor, body)
    os.fsync(descriptor)
finally:
    os.close(descriptor)
PY
ln -s "${HERDR_BINARY}" "${RELEASE_BUILD}/herdr"
ln -s "${NEW_RUNTIME}" "${RELEASE_BUILD}/tendwire"
install -m 755 "${PACKAGED_DEPLOY}/herdr-console-canary-r8.py" \
    "${RELEASE_BUILD}/herdr-console-canary"
"${RELEASE_BUILD}/herdr-console-canary" \
    --herdr "${HERDR_BINARY}" \
    --adapter-bin-dir "${CODEX_ACP_ROOT}/bin" \
    --expected-herdr-sha256 "${HERDR_BINARY_SHA256}" \
    --expected-adapter-sha256 "${ADAPTER_BUNDLE_SHA256}" \
    --expected-herdr-commit "${HERDR_REVISION}" \
    --expected-herdr-tree "${HERDR_TREE}" \
    --expected-herdr-version "${HERDR_EMBEDDED_VERSION#herdr }" \
    --expected-protocol 19 \
    --release-id "${RELEASE_ID}" \
    --evidence "${RELEASE_BUILD}/herdr-console-canary-evidence.json" \
    --protect-path /home/smith/.config/herdres \
    --protect-path /home/smith/.config/herdr/config.toml \
    --protect-path /home/smith/.config/herdr/herdr-client.sock \
    --protect-path /home/smith/.config/herdr/herdr.sock \
    --protect-path /home/smith/.config/herdr/session.json \
    --protect-path /home/smith/.config/herdr/sessions \
    --protect-path /home/smith/.config/systemd/user \
    --protect-path /home/smith/.local/state/herdr \
    --protect-path /home/smith/.local/share/acp-runtime/active \
    --protect-path /home/smith/.local/share/tendwire/tendwire.sock
install -d -m 755 "${RELEASE_BUILD}/herdres"
git -C "${SOURCE}/.worktrees/reduction-herdres-release-fixes" archive "${HERDRES_REVISION}" \
    | tar -x -C "${RELEASE_BUILD}/herdres"
install -m 755 "${OLD_RELEASE}/reset-telegram-topics" \
    "${RELEASE_BUILD}/reset-telegram-topics"
install -m 755 "${PACKAGED_DEPLOY}/monitor-one-hour-core-r8.py" \
    "${RELEASE_BUILD}/monitor-one-hour-core"
install -m 755 "${PACKAGED_DEPLOY}/monitor-one-hour-strict-r8.py" \
    "${RELEASE_BUILD}/monitor-one-hour-strict"
install -m 755 "${PACKAGED_DEPLOY}/monitor-exit-guard-r8.sh" \
    "${RELEASE_BUILD}/monitor-exit-guard"
install -m 755 "${PACKAGED_DEPLOY}/rollback-r8.sh" \
    "${RELEASE_BUILD}/rollback-r8"
install -m 755 "${PACKAGED_DEPLOY}/release-integrity-r8.py" \
    "${RELEASE_BUILD}/release-integrity"
install -m 755 "${PACKAGED_DEPLOY}/attest-r8.py" \
    "${RELEASE_BUILD}/attest-r8"
install -m 755 "${PACKAGED_DEPLOY}/release-guard-r8.py" \
    "${RELEASE_BUILD}/release-guard"
install -m 755 "${PACKAGED_DEPLOY}/verify-live-telegram-r8.py" \
    "${RELEASE_BUILD}/verify-live-telegram"
install -m 755 "${PACKAGED_DEPLOY}/restore-current-pane-r8.sh" \
    "${RELEASE_BUILD}/restore-current-pane"
install -m 755 "${PACKAGED_DEPLOY}/acp-migrate-current-pane-r8.sh" \
    "${RELEASE_BUILD}/migrate-current-pane"
install -m 755 "${PACKAGED_DEPLOY}/rollout-acp-release-r8.sh" \
    "${RELEASE_BUILD}/rollout-acp-release-r8"
install -m 755 "${PACKAGED_DEPLOY}/guarded-herdr-restart-r8.sh" \
    "${RELEASE_BUILD}/guarded-herdr-restart-r8"
install -m 755 "${PACKAGED_DEPLOY}/prepare-acp-release-r8.sh" \
    "${RELEASE_BUILD}/prepare-acp-release-r8"
install -m 755 "${PACKAGED_DEPLOY}/recover-stale-r8-artifacts.py" \
    "${RELEASE_BUILD}/recover-stale-r8-artifacts"
install -m 755 "${PACKAGED_DEPLOY}/topology-normalizer-r8.py" \
    "${RELEASE_BUILD}/topology-normalizer"
install -m 444 "${PACKAGED_DEPLOY}/r8-release-lock.json" \
    "${RELEASE_BUILD}/r8-release-lock.json"
cp -a "${OLD_RELEASE}/systemd/tendwired-99.conf" \
    "${RELEASE_BUILD}/systemd/tendwired-99.conf"
cp -a "${OLD_RELEASE}/systemd/herdres-99.conf" \
    "${RELEASE_BUILD}/systemd/herdres-99.conf"
cp -a "${OLD_RELEASE}/systemd/herdres-gateway-99.conf" \
    "${RELEASE_BUILD}/systemd/herdres-gateway-99.conf"
cp -a "${OLD_RELEASE}/systemd/acp-frozen-live-monitor.service" \
    "${RELEASE_BUILD}/systemd/acp-frozen-live-monitor.service"
chmod u+w \
    "${RELEASE_BUILD}/systemd/tendwired-99.conf" \
    "${RELEASE_BUILD}/systemd/herdres-99.conf" \
    "${RELEASE_BUILD}/systemd/herdres-gateway-99.conf" \
    "${RELEASE_BUILD}/systemd/acp-frozen-live-monitor.service"
install -m 644 "${PACKAGED_DEPLOY}/acp-r8-release-guard.service" \
    "${RELEASE_BUILD}/systemd/acp-r8-release-guard.service"
install -m 644 "${PACKAGED_DEPLOY}/acp-r8-rollback.service" \
    "${RELEASE_BUILD}/systemd/acp-r8-rollback.service"
install -m 644 "${PACKAGED_DEPLOY}/acp-r8-rollout.service" \
    "${RELEASE_BUILD}/systemd/acp-r8-rollout.service"
install -m 644 "${PACKAGED_DEPLOY}/herdr-99-acp-adapter-r8.conf" \
    "${RELEASE_BUILD}/systemd/herdr-99-acp-adapter.conf"

sed -i \
    -e 's/9026d9bc-7446533-3659994-r5/67568f32-0b94403-f50af73-r8/g' \
    -e 's/7446533bb6fb2560a9a9dd871f638c4a6ccbb086/0b944031c94e99b9f5fac439a850e8b823e8b1f1/g' \
    -e 's/frozen-7446533-6c0d0f5/frozen-0b94403-r8/g' \
    -e 's#acp-7446533-3659994-r5#acp-0b94403-f50af73-r8#g' \
    -e 's#/candidates/7446533/tendwire.db#/candidates/0b94403-r8/tendwire.db#g' \
    -e 's#/candidates/3659994/#/candidates/f50af73-r8/#g' \
    -e 's/frozen-7446533-3659994-r5.env/frozen-0b94403-f50af73-r8.env/g' \
    -e 's#/herdres/herdres sync#/herdres/herdres.py sync#g' \
    -e 's#/herdres/herdres-gateway#/herdres/herdres_gateway.py#g' \
    "${RELEASE_BUILD}/reset-telegram-topics" \
    "${RELEASE_BUILD}/monitor-one-hour-core" \
    "${RELEASE_BUILD}/systemd/tendwired-99.conf" \
    "${RELEASE_BUILD}/systemd/herdres-99.conf" \
    "${RELEASE_BUILD}/systemd/herdres-gateway-99.conf" \
    "${RELEASE_BUILD}/systemd/acp-frozen-live-monitor.service"
sed -i \
    -e 's/ acp-frozen-release-recovery.service//g' \
    -e 's#/monitor-one-hour#/monitor-one-hour-strict#g' \
    "${RELEASE_BUILD}/systemd/tendwired-99.conf" \
    "${RELEASE_BUILD}/systemd/herdres-99.conf" \
    "${RELEASE_BUILD}/systemd/herdres-gateway-99.conf" \
    "${RELEASE_BUILD}/systemd/acp-frozen-live-monitor.service"
sed -i \
    -e '/^Requires=herdr-server.service$/a BindsTo=acp-r8-release-guard.service' \
    -e 's/After=network-online.target herdr-server.service$/After=network-online.target herdr-server.service acp-r8-release-guard.service/' \
    "${RELEASE_BUILD}/systemd/tendwired-99.conf"
sed -i \
    -e '/^Requires=tendwired.service$/a BindsTo=acp-r8-release-guard.service' \
    -e 's/After=tendwired.service$/After=tendwired.service acp-r8-release-guard.service/' \
    "${RELEASE_BUILD}/systemd/herdres-99.conf"
sed -i \
    -e '/^Requires=tendwired.service herdres.service$/a BindsTo=acp-r8-release-guard.service' \
    -e 's/After=network-online.target tendwired.service herdres.service$/After=network-online.target tendwired.service herdres.service acp-r8-release-guard.service/' \
    "${RELEASE_BUILD}/systemd/herdres-gateway-99.conf"
sed -i '/^\[Service\]$/i OnFailure=acp-r8-rollback.service' \
    "${RELEASE_BUILD}/systemd/acp-frozen-live-monitor.service"
# systemd expands $SERVICE_RESULT when the service stops.
# shellcheck disable=SC2016
printf 'ExecStopPost=%s $SERVICE_RESULT\n' \
    "${NEW_RELEASE}/monitor-exit-guard" \
    >>"${RELEASE_BUILD}/systemd/acp-frozen-live-monitor.service"
# The core monitor retains all previously reviewed live checks. The strict
# wrapper supplies the fresh-ingress/final proof and poison-state gate.
sed -i \
    -e 's/        _begin_validation()/        _release_preflight()/' \
    -e 's/        _release_integrity_preflight()/        _release_preflight()/' \
    -e 's/            _heartbeat(index)/            pass  # strict wrapper owns the heartbeat/' \
    -e 's/        _finish_validation("validation_passed")/        pass  # strict wrapper owns the final phase/' \
    "${RELEASE_BUILD}/monitor-one-hour-core"

find "${RELEASE_BUILD}" -type f -exec chmod a-w {} +
find "${RELEASE_BUILD}" -type d -exec chmod 555 {} +
release_published=1
mv -T "${RELEASE_BUILD}" "${NEW_RELEASE}"
private_env_published=1
install -m 600 /home/smith/.config/herdres/frozen-7446533-3659994-r5.env \
    "${PRIVATE_ENV}"
sed -i \
    -e 's#/candidates/3659994/#/candidates/f50af73-r8/#g' \
    -e 's#frozen-7446533-3659994-r5.env#frozen-0b94403-f50af73-r8.env#g' \
    "${PRIVATE_ENV}"
install -d -m 755 "$(dirname "${MANIFEST}")"
manifest_published=1
"${NEW_RELEASE}/release-integrity" write --manifest "${MANIFEST}"
# Recovery is armed only by prepare-acp-release-r8 after this build returns.
# Make every immutable byte and every publishing rename durable first.
fsync_published_artifacts

build_complete=1
build_phase published
rm -f -- "${BUILD_STATE}/owner.json"
sync -d "${BUILD_STATE}"
trap - EXIT
cleanup
printf 'RELEASE_PUBLISHED %s\n' "${RELEASE_ID}"
