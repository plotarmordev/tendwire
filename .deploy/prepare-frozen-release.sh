#!/bin/bash -p
set -Eeuo pipefail
clean_environment_valid() {
    [[ "${FROZEN_ACP_CLEAN_ENV:-}" = 1 \
        && "${HOME:-}" = /home/smith && "${USER:-}" = smith \
        && "${LOGNAME:-}" = smith && "${PATH:-}" = /usr/bin:/bin ]] || return 1
    local name
    while IFS= read -r name; do
        case "${name}" in
            FROZEN_ACP_CLEAN_ENV|HOME|USER|LOGNAME|PATH|PWD|SHLVL|_) ;;
            *) return 1 ;;
        esac
    done < <(compgen -e)
}
if ! clean_environment_valid; then
    exec /usr/bin/env -i \
        HOME=/home/smith USER=smith LOGNAME=smith PATH=/usr/bin:/bin \
        FROZEN_ACP_CLEAN_ENV=1 \
        /bin/bash -p "${BASH_SOURCE[0]}" "$@"
fi
unset -f clean_environment_valid
unset FROZEN_ACP_CLEAN_ENV
export PATH=/usr/bin:/bin
unset BASH_ENV ENV CDPATH GLOBIGNORE PYTHONHOME PYTHONPATH TAR_OPTIONS

readonly TENDWIRE_SOURCE=/home/smith/tendwire
readonly HERDRES_SOURCE=/home/smith/tendwire/.worktrees/herdres-acp-route
readonly TENDWIRE_REVISION=7446533bb6fb2560a9a9dd871f638c4a6ccbb086
readonly HERDRES_REVISION=36599949daa64f68494d04f96a3bfee31904a804
readonly HERDR_REVISION=9026d9bc5a12d9adc2d9f68ebdc564133e4098b4
readonly HERDR_BINARY=/home/smith/.local/share/herdr-runtime/acp-9026d9bc/herdr
readonly HERDR_SHA256=2e58e1b11ed289d6a99ba36b80867e5e5d5920d03406bb40a1113e2d391f386f
readonly TENDWIRE_RUNTIME=/home/smith/.local/share/tendwire-runtime/acp-7446533-3659994
readonly RELEASE_ROOT=/home/smith/.local/share/acp-runtime/releases/9026d9bc-7446533-3659994
readonly MANIFEST=/home/smith/.local/share/acp-runtime/manifests/9026d9bc-7446533-3659994.json
readonly PREPARE_LOCK=/home/smith/.local/state/acp-cutover/prepare-9026d9bc-7446533-3659994.lock
readonly RUNNER_ROOT=/home/smith/.local/share/acp-runtime/runners/9026d9bc-7446533-3659994
readonly ACTIVE_LINK=/home/smith/.local/share/acp-runtime/active
readonly TENDWIRE_CANDIDATE=/home/smith/.local/share/tendwire/candidates/7446533
readonly HERDRES_CANDIDATE=/home/smith/.local/share/herdres/candidates/3659994
readonly TRANSACTION_ROOT=/home/smith/.local/state/acp-cutover/frozen-7446533-6c0d0f5
readonly COMPLETE_MARKER=/home/smith/.local/share/acp-runtime/prepared/9026d9bc-7446533-3659994.complete
readonly TOOLING_REVISION="$(git -C "${TENDWIRE_SOURCE}" rev-parse HEAD)"

build_root="$(mktemp -d /tmp/frozen-acp-release.XXXXXX)"
readonly DEPLOY_SOURCE="${build_root}/tooling-source/.deploy"
runtime_stage="$(dirname "${TENDWIRE_RUNTIME}")/.acp-7446533-3659994.stage.$$"
release_stage="$(dirname "${RELEASE_ROOT}")/.9026d9bc-7446533-3659994.stage.$$"
runner_stage="$(dirname "${RUNNER_ROOT}")/.9026d9bc-7446533-3659994.stage.$$"
manifest_stage="${build_root}/release-manifest.json"
manifest_publish_tmp="${MANIFEST}.tmp.$$"
complete_publish_tmp="${COMPLETE_MARKER}.tmp.$$"
published_runtime=0
published_release=0
published_manifest=0
published_runner=0
published_complete=0
runtime_stage_created=0
release_stage_created=0
runner_stage_created=0

fail() {
    printf 'prepare failed: %s\n' "$*" >&2
    exit 1
}

quarantine_partial_publication() {
    local present=0
    local path
    local active_target=""
    local failure_root
    for path in "${TENDWIRE_RUNTIME}" "${RELEASE_ROOT}" "${RUNNER_ROOT}" \
        "${MANIFEST}" "${COMPLETE_MARKER}"
    do
        if [[ -e "${path}" || -L "${path}" ]]; then
            present=$((present + 1))
        fi
    done
    [[ "${present}" -ne 0 ]] || return 0
    [[ "${present}" -ne 5 ]] || fail "complete immutable publication already exists"
    active_target="$(readlink -f -- "${ACTIVE_LINK}" 2>/dev/null || true)"
    [[ "${active_target}" != "${RELEASE_ROOT}" ]] || \
        fail "partial publication is referenced by the active release"
    [[ ! -e "${TENDWIRE_CANDIDATE}" && ! -L "${TENDWIRE_CANDIDATE}" ]] || \
        fail "partial publication has a Tendwire candidate"
    [[ ! -e "${HERDRES_CANDIDATE}" && ! -L "${HERDRES_CANDIDATE}" ]] || \
        fail "partial publication has a Herdres candidate"
    [[ ! -e "${TRANSACTION_ROOT}" && ! -L "${TRANSACTION_ROOT}" ]] || \
        fail "partial publication has a cutover transaction"
    failure_root="/home/smith/.local/share/acp-runtime/prepare-failures/$(date -u +%Y%m%dT%H%M%SZ)-9026d9bc-7446533-3659994-$$"
    install -d -m 0700 -- "${failure_root}"
    if [[ -e "${TENDWIRE_RUNTIME}" || -L "${TENDWIRE_RUNTIME}" ]]; then
        mv -T -- "${TENDWIRE_RUNTIME}" "${failure_root}/tendwire-runtime"
    fi
    if [[ -e "${RELEASE_ROOT}" || -L "${RELEASE_ROOT}" ]]; then
        mv -T -- "${RELEASE_ROOT}" "${failure_root}/combined-release"
    fi
    if [[ -e "${RUNNER_ROOT}" || -L "${RUNNER_ROOT}" ]]; then
        mv -T -- "${RUNNER_ROOT}" "${failure_root}/deploy-runner"
    fi
    if [[ -e "${MANIFEST}" || -L "${MANIFEST}" ]]; then
        mv -T -- "${MANIFEST}" "${failure_root}/release-manifest.json"
    fi
    if [[ -e "${COMPLETE_MARKER}" || -L "${COMPLETE_MARKER}" ]]; then
        mv -T -- "${COMPLETE_MARKER}" "${failure_root}/prepared.complete"
    fi
    sync -d "${failure_root}"
    printf 'quarantined incomplete prior publication at %s\n' "${failure_root}" >&2
}

cleanup_failed_publication() {
    local status="${1:-$?}"
    trap - ERR HUP INT TERM
    if [[ -f "${complete_publish_tmp}" ]]; then
        mv -T -- "${complete_publish_tmp}" \
            "$(dirname "${COMPLETE_MARKER}")/.failed-prepared.complete.tmp.$$"
    fi
    if [[ "${published_complete}" -eq 1 && -f "${COMPLETE_MARKER}" ]]; then
        mv -T -- "${COMPLETE_MARKER}" \
            "$(dirname "${COMPLETE_MARKER}")/.failed-prepared.complete.$$"
    fi
    if [[ -f "${manifest_publish_tmp}" ]]; then
        mv -T -- "${manifest_publish_tmp}" "$(dirname "${MANIFEST}")/.failed-release-manifest.tmp.$$"
    fi
    if [[ "${published_manifest}" -eq 1 && -f "${MANIFEST}" ]]; then
        mv -T -- "${MANIFEST}" "$(dirname "${MANIFEST}")/.failed-9026d9bc-7446533-3659994.json.$$"
    fi
    if [[ "${published_runner}" -eq 1 && -d "${RUNNER_ROOT}" ]]; then
        mv -T -- "${RUNNER_ROOT}" "$(dirname "${RUNNER_ROOT}")/.failed-runner-9026d9bc-7446533-3659994.$$"
    fi
    if [[ "${published_release}" -eq 1 && -d "${RELEASE_ROOT}" ]]; then
        mv -T -- "${RELEASE_ROOT}" "$(dirname "${RELEASE_ROOT}")/.failed-9026d9bc-7446533-3659994.$$"
    fi
    if [[ "${published_runtime}" -eq 1 && -d "${TENDWIRE_RUNTIME}" ]]; then
        mv -T -- "${TENDWIRE_RUNTIME}" "$(dirname "${TENDWIRE_RUNTIME}")/.failed-acp-7446533-3659994.$$"
    fi
    if [[ "${runtime_stage_created}" -eq 1 && -d "${runtime_stage}" ]]; then
        mv -T -- "${runtime_stage}" "$(dirname "${runtime_stage}")/.failed-acp-7446533-3659994.stage.$$"
    fi
    if [[ "${release_stage_created}" -eq 1 && -d "${release_stage}" ]]; then
        mv -T -- "${release_stage}" "$(dirname "${release_stage}")/.failed-9026d9bc-7446533-3659994.stage.$$"
    fi
    if [[ "${runner_stage_created}" -eq 1 && -d "${runner_stage}" ]]; then
        mv -T -- "${runner_stage}" "$(dirname "${runner_stage}")/.failed-runner-9026d9bc-7446533-3659994.stage.$$"
    fi
    exit "${status}"
}

trap cleanup_failed_publication ERR
trap 'cleanup_failed_publication 129' HUP
trap 'cleanup_failed_publication 130' INT
trap 'cleanup_failed_publication 143' TERM

install -d -m 700 -- "$(dirname "${PREPARE_LOCK}")"
exec 9>"${PREPARE_LOCK}"
flock -n 9

git -C "${TENDWIRE_SOURCE}" merge-base --is-ancestor \
    "${TENDWIRE_REVISION}" "${TOOLING_REVISION}"
git -C "${TENDWIRE_SOURCE}" diff --quiet \
    "${TENDWIRE_REVISION}" "${TOOLING_REVISION}" -- src/tendwire pyproject.toml
test "$(git -C "${HERDRES_SOURCE}" rev-parse HEAD)" = "${HERDRES_REVISION}"
test -z "$(git -C "${TENDWIRE_SOURCE}" status --porcelain --untracked-files=no)"
test -z "$(git -C "${HERDRES_SOURCE}" status --porcelain --untracked-files=no)"
test -x "${HERDR_BINARY}"
test "$(sha256sum "${HERDR_BINARY}" | cut -d' ' -f1)" = "${HERDR_SHA256}"
test "$(stat -c '%a:%s' "${HERDR_BINARY}")" = 555:20285816
quarantine_partial_publication
[[ ! -e "${TENDWIRE_RUNTIME}" && ! -L "${TENDWIRE_RUNTIME}" ]] || fail "immutable Tendwire target already exists"
[[ ! -e "${RELEASE_ROOT}" && ! -L "${RELEASE_ROOT}" ]] || fail "immutable combined target already exists"
[[ ! -e "${MANIFEST}" && ! -L "${MANIFEST}" ]] || fail "release manifest already exists"
[[ ! -e "${RUNNER_ROOT}" && ! -L "${RUNNER_ROOT}" ]] || fail "immutable deploy runner already exists"
[[ ! -e "${COMPLETE_MARKER}" && ! -L "${COMPLETE_MARKER}" ]] || fail "prepare completion marker already exists"
[[ ! -e "${manifest_publish_tmp}" && ! -L "${manifest_publish_tmp}" ]]
[[ ! -e "${complete_publish_tmp}" && ! -L "${complete_publish_tmp}" ]]
[[ ! -e "${runtime_stage}" && ! -L "${runtime_stage}" ]]
[[ ! -e "${release_stage}" && ! -L "${release_stage}" ]]
[[ ! -e "${runner_stage}" && ! -L "${runner_stage}" ]]

install -d -m 700 -- "${build_root}/tendwire-source" \
    "${build_root}/herdres-source" "${build_root}/tooling-source" \
    "${build_root}/wheel"
git -C "${TENDWIRE_SOURCE}" archive --format=tar --output="${build_root}/tendwire.tar" \
    "${TENDWIRE_REVISION}"
git -C "${HERDRES_SOURCE}" archive --format=tar --output="${build_root}/herdres.tar" \
    "${HERDRES_REVISION}"
git -C "${TENDWIRE_SOURCE}" archive --format=tar --output="${build_root}/tooling.tar" \
    "${TOOLING_REVISION}" -- .deploy
tar -xf "${build_root}/tendwire.tar" -C "${build_root}/tendwire-source"
tar -xf "${build_root}/herdres.tar" -C "${build_root}/herdres-source"
tar -xf "${build_root}/tooling.tar" -C "${build_root}/tooling-source"

"${TENDWIRE_SOURCE}/.venv/bin/python" -I -m build --wheel --no-isolation \
    --outdir "${build_root}/wheel" "${build_root}/tendwire-source"
runtime_stage_created=1
/usr/bin/python3.13 -I -m venv --copies "${runtime_stage}"
"${runtime_stage}/bin/python" -B -I -m pip install --disable-pip-version-check \
    --no-compile --no-index --no-deps \
    "${build_root}/wheel"/*.whl
for item in \
    acp agent_client_protocol-0.11.0.dist-info \
    annotated_types annotated_types-0.8.0.dist-info \
    pydantic pydantic-2.13.4.dist-info \
    pydantic_core pydantic_core-2.46.4.dist-info \
    typing_extensions.py typing_extensions-4.16.0.dist-info \
    typing_inspection typing_inspection-0.4.2.dist-info
do
    cp -a "${TENDWIRE_SOURCE}/.venv/lib/python3.13/site-packages/${item}" \
        "${runtime_stage}/lib/python3.13/site-packages/${item}"
done
"${TENDWIRE_SOURCE}/.venv/bin/python" -I - "${runtime_stage}" "${TENDWIRE_RUNTIME}" <<'PY'
import sys
from pathlib import Path

stage = Path(sys.argv[1])
source = str(stage).encode()
target = sys.argv[2].encode()
for path in (stage / "bin").iterdir():
    if not path.is_file() or path.is_symlink():
        continue
    content = path.read_bytes()
    if source in content:
        path.write_bytes(content.replace(source, target))
config = stage / "pyvenv.cfg"
config.write_bytes(config.read_bytes().replace(source, target))
PY
find "${runtime_stage}" -type f -name '*.pyc' -delete
find "${runtime_stage}" -depth -type d -name __pycache__ -empty -delete
"${runtime_stage}/bin/python" -B -I -c \
    'import acp, tendwire, tendwire.daemon; assert tendwire.__version__ == "0.1.0rc5"'
"${runtime_stage}/bin/python" -B -I -m \
    tendwire.cli --help >/dev/null
"${runtime_stage}/bin/python" -B -I -m pip check >/dev/null
"${runtime_stage}/bin/python" -B -I - \
    "${TENDWIRE_SOURCE}/.venv/lib/python3.13/site-packages/acp" \
    "${runtime_stage}/lib/python3.13/site-packages/acp" <<'PY'
import hashlib
import importlib.metadata
import sys
from pathlib import Path

EXPECTED = "f5e621738a5651da9d14559806ab1d3491e8a9da6a72e686baf087e67a87e5f6"


def digest(root: Path) -> tuple[str, int]:
    value = hashlib.sha256()
    files = sorted(root.rglob("*.py"), key=lambda item: item.relative_to(root).as_posix())
    for item in files:
        name = item.relative_to(root).as_posix().encode()
        content = item.read_bytes()
        value.update(len(name).to_bytes(4, "big") + name)
        value.update(len(content).to_bytes(8, "big") + content)
    return value.hexdigest(), len(files)


assert digest(Path(sys.argv[1])) == (EXPECTED, 29)
assert digest(Path(sys.argv[2])) == (EXPECTED, 29)
expected_versions = {
    "agent-client-protocol": "0.11.0",
    "annotated-types": "0.8.0",
    "pydantic": "2.13.4",
    "pydantic-core": "2.46.4",
    "typing-extensions": "4.16.0",
    "typing-inspection": "0.4.2",
}
for distribution, version in expected_versions.items():
    assert importlib.metadata.version(distribution) == version
PY
chmod -R a-w "${runtime_stage}"

release_stage_created=1
install -d -m 0755 -- "${release_stage}/herdres/herdres_connector" \
    "${release_stage}/systemd"
ln -s -- "${HERDR_BINARY}" "${release_stage}/herdr"
ln -s -- "${TENDWIRE_RUNTIME}" "${release_stage}/tendwire"
install -m 0555 "${build_root}/herdres-source/herdres.py" "${release_stage}/herdres/herdres"
install -m 0555 "${build_root}/herdres-source/herdres_gateway.py" \
    "${release_stage}/herdres/herdres-gateway"
for source_file in "${build_root}"/herdres-source/herdres_connector/*.py; do
    install -m 0444 "${source_file}" \
        "${release_stage}/herdres/herdres_connector/$(basename "${source_file}")"
done
install -m 0444 "${DEPLOY_SOURCE}/frozen-herdres-runtime.env" \
    "${release_stage}/herdres-runtime.env"
install -m 0555 "${DEPLOY_SOURCE}/frozen-cutover-recovery.sh" \
    "${release_stage}/frozen-cutover-recovery"
install -m 0555 "${DEPLOY_SOURCE}/resume-frozen-cutover.sh" \
    "${release_stage}/resume-frozen-cutover"
install -m 0555 "${DEPLOY_SOURCE}/validate-frozen-release.py" \
    "${release_stage}/validate-frozen-release"
install -m 0555 "${DEPLOY_SOURCE}/finalize-frozen-release.py" \
    "${release_stage}/finalize-frozen-release"
install -m 0555 "${DEPLOY_SOURCE}/capture-frozen-discard-inventory.py" \
    "${release_stage}/capture-frozen-discard-inventory"
install -m 0555 "${DEPLOY_SOURCE}/monitor-frozen-7446533-6c0d0f5-hour.py" \
    "${release_stage}/monitor-one-hour"
install -m 0555 "${DEPLOY_SOURCE}/reconcile-frozen-telegram-topics.py" \
    "${release_stage}/reset-telegram-topics"
install -m 0444 "${DEPLOY_SOURCE}/frozen-tendwired-99.conf" \
    "${release_stage}/systemd/tendwired-99.conf"
install -m 0444 "${DEPLOY_SOURCE}/frozen-herdres-99.conf" \
    "${release_stage}/systemd/herdres-99.conf"
install -m 0444 "${DEPLOY_SOURCE}/frozen-herdres-gateway-99.conf" \
    "${release_stage}/systemd/herdres-gateway-99.conf"
install -m 0444 "${DEPLOY_SOURCE}/acp-frozen-release-recovery.service" \
    "${release_stage}/systemd/acp-frozen-release-recovery.service"
install -m 0444 "${DEPLOY_SOURCE}/acp-frozen-live-monitor.service" \
    "${release_stage}/systemd/acp-frozen-live-monitor.service"
install -m 0444 "${DEPLOY_SOURCE}/legacy-recovery-frozen-99.conf" \
    "${release_stage}/systemd/legacy-recovery-frozen-99.conf"
printf '%s\n' "${TOOLING_REVISION}" >"${release_stage}/operations-tooling-revision"
chmod 0444 "${release_stage}/operations-tooling-revision"
chmod -R a-w "${release_stage}"

runner_stage_created=1
install -d -m 0755 -- "${runner_stage}"
"${TENDWIRE_SOURCE}/.venv/bin/python" -I - \
    "${runtime_stage}" "${release_stage}" "${manifest_stage}" \
    "${TOOLING_REVISION}" \
    "${DEPLOY_SOURCE}/deploy-frozen-7446533-6c0d0f5.sh" \
    "${runner_stage}/deploy" <<'PY'
import hashlib
import json
import os
import stat
import sys
from pathlib import Path

runtime, release, manifest = map(Path, sys.argv[1:4])
tooling_revision = sys.argv[4]
deploy_template = Path(sys.argv[5])
deploy_runner = Path(sys.argv[6])


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


runtime_digest, runtime_entries = tree(runtime)
release_digest, release_entries = tree(release)
runner = deploy_template.read_text(encoding="utf-8")
replacements = {
    "EXPECTED_TENDWIRE_RUNTIME_SHA256=PREPARE_REQUIRED": (
        f"EXPECTED_TENDWIRE_RUNTIME_SHA256={runtime_digest}"
    ),
    "EXPECTED_TENDWIRE_RUNTIME_ENTRIES=PREPARE_REQUIRED": (
        f"EXPECTED_TENDWIRE_RUNTIME_ENTRIES={runtime_entries}"
    ),
    "EXPECTED_COMBINED_RELEASE_SHA256=PREPARE_REQUIRED": (
        f"EXPECTED_COMBINED_RELEASE_SHA256={release_digest}"
    ),
    "EXPECTED_COMBINED_RELEASE_ENTRIES=PREPARE_REQUIRED": (
        f"EXPECTED_COMBINED_RELEASE_ENTRIES={release_entries}"
    ),
}
for original, replacement in replacements.items():
    if runner.count(original) != 1:
        raise RuntimeError("deploy runner template placeholder mismatch")
    runner = runner.replace(original, replacement)
deploy_runner.write_text(runner, encoding="utf-8")
deploy_runner.chmod(0o555)
runner_digest = hashlib.sha256(deploy_runner.read_bytes()).hexdigest()
value = {
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
    "tendwire_runtime_sha256": runtime_digest,
    "tendwire_runtime_entries": runtime_entries,
    "combined_release_sha256": release_digest,
    "combined_release_entries": release_entries,
    "owner_uid": os.geteuid(),
    "tendwire_runtime_root_mode": stat.S_IMODE(runtime.lstat().st_mode),
    "combined_release_root_mode": stat.S_IMODE(release.lstat().st_mode),
}
temporary = manifest.with_suffix(".tmp")
with temporary.open("x", encoding="utf-8") as target:
    json.dump(value, target, sort_keys=True, separators=(",", ":"))
    target.write("\n")
    target.flush()
    os.fsync(target.fileno())
os.chmod(temporary, 0o600)
os.replace(temporary, manifest)
directory = os.open(manifest.parent, os.O_RDONLY | os.O_DIRECTORY)
try:
    os.fsync(directory)
finally:
    os.close(directory)
print(json.dumps(value, sort_keys=True))
PY
chmod -R a-w "${runner_stage}"

install -d -m 0755 -- "$(dirname "${TENDWIRE_RUNTIME}")" \
    "$(dirname "${RELEASE_ROOT}")" "$(dirname "${RUNNER_ROOT}")"
install -d -m 700 -- "$(dirname "${MANIFEST}")"
published_runtime=1
mv -T -- "${runtime_stage}" "${TENDWIRE_RUNTIME}"
published_release=1
mv -T -- "${release_stage}" "${RELEASE_ROOT}"
published_runner=1
mv -T -- "${runner_stage}" "${RUNNER_ROOT}"
install -m 0600 "${manifest_stage}" "${manifest_publish_tmp}"
published_manifest=1
mv -T -- "${manifest_publish_tmp}" "${MANIFEST}"
sync -d "$(dirname "${TENDWIRE_RUNTIME}")"
sync -d "$(dirname "${RELEASE_ROOT}")"
sync -d "$(dirname "${RUNNER_ROOT}")"
sync -d "$(dirname "${MANIFEST}")"

test "$(stat -c '%u:%a' "${TENDWIRE_RUNTIME}")" = "$(id -u):555"
test "$(stat -c '%u:%a' "${RELEASE_ROOT}")" = "$(id -u):555"
test "$(stat -c '%u:%a' "${RUNNER_ROOT}")" = "$(id -u):555"
test "$(stat -c '%u:%a' "${RUNNER_ROOT}/deploy")" = "$(id -u):555"
test "$(stat -c '%u:%a' "${MANIFEST}")" = "$(id -u):600"
"${TENDWIRE_RUNTIME}/bin/python" -B -I -c \
    'import acp, tendwire, tendwire.daemon; assert tendwire.__version__ == "0.1.0rc5"'
"${TENDWIRE_RUNTIME}/bin/python" -B -I -m \
    tendwire.cli --help >/dev/null
PYTHONDONTWRITEBYTECODE=1 PYTHONSAFEPATH=1 "${TENDWIRE_RUNTIME}/bin/tendwire" --help >/dev/null
test -z "$(find "${TENDWIRE_RUNTIME}" \( -type f -name '*.pyc' -o -type d -name __pycache__ \) -print -quit)"

install -d -m 0700 -- "$(dirname "${COMPLETE_MARKER}")"
printf 'FROZEN_ACP_RELEASE_PREPARED %s %s\n' \
    "${TOOLING_REVISION}" "$(sha256sum "${MANIFEST}" | cut -d' ' -f1)" \
    >"${complete_publish_tmp}"
chmod 0600 "${complete_publish_tmp}"
sync "${complete_publish_tmp}"
published_complete=1
mv -T -- "${complete_publish_tmp}" "${COMPLETE_MARKER}"
sync -d "$(dirname "${COMPLETE_MARKER}")"
test "$(stat -c '%u:%a' "${COMPLETE_MARKER}")" = "$(id -u):600"

trap - ERR HUP INT TERM
