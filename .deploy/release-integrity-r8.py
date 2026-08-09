#!/usr/bin/python3 -I
"""Write or verify the immutable r8 release inventory."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import subprocess
from pathlib import Path


RELEASE_ID = "67568f32-0b94403-f50af73-r8"
TENDWIRE_REVISION = "0b944031c94e99b9f5fac439a850e8b823e8b1f1"
TENDWIRE_REVIEWED_ANCESTOR = "1f277cd562f3852cfbec4e52b2ffc8c406550fc4"
HERDRES_REVISION = "f50af733033089020ace3b15e686299cb8a67f1e"
HERDRES_REVIEWED_ANCESTOR = "36599949daa64f68494d04f96a3bfee31904a804"
HERDR_REVISION = "67568f327cea67bdd1d197feff621c45476da84c"
HERDR_TREE = "def32e75c4751d2acd504fdfc6de9303ab394b6b"
HERDR_REVIEWED_ANCESTOR = "9026d9bc5a12d9adc2d9f68ebdc564133e4098b4"
HERDR_ARCHIVE_SHA256 = "6135ff0d3cc2f4a82f22e410e846cfe4fdcedbcab694fd99c3ac4a1be4674752"
HERDR_BINARY_SHA256 = "9be82154a29ec730dd8ce3a4c063e12847cadb0c1c81fa4c64782057cd4137f0"
HERDR_ELF_BUILD_ID = "2eef96277f09679f9f64bdcf4ca4aa0211001e43"
HERDR_EMBEDDED_VERSION = "herdr 0.7.5-r8.67568f327cea67bdd1d197feff621c45476da84c"
HERDR_PROVENANCE_SHA256 = "5c41a0e5b5ba5d26eb1f4229bde8f8f7fae1ff7316b8d809bd1239bb1f4a9b04"
HERDR_RUNTIME_SHA256 = "0cc868fee3ad5dc38e7c652e876e128d400faf6ccb9c60ae48fb06b4498ffba5"
HERDR_RUNTIME_ENTRIES = 4
CODEX_ACP_REVISION = "7cb0524624f2e730f48c3dac9b547ca130964ae9"
CODEX_ACP_TREE = "69780d844fb96a07a46b2094b29c33c90c484a62"
CODEX_ACP_BUNDLE_SHA256 = "ab2dcb6ab0866775895b245a46069baba357eda07ba98b818ab215d70c2dbf60"
CODEX_ACP_RUNTIME_SHA256 = "c739f60b53771b959be9a510687d33f000e453715114241c426b589903290303"
CODEX_ACP_RUNTIME_ENTRIES = 33
CODEX_ACP_PROVENANCE_SHA256 = "a5135f4d0fbbddc0d0bfd423454ded4321753439b240c55d81c260f0ac32b7d1"
RELEASE = Path("/home/smith/.local/share/acp-runtime/releases") / RELEASE_ID
RUNTIME = Path(
    "/home/smith/.local/share/tendwire-runtime/acp-0b94403-f50af73-r8"
)
HERDR_ROOT = Path(
    "/home/smith/.local/share/herdr-runtime/acp-67568f327cea67bdd1d197feff621c45476da84c"
)
HERDR = HERDR_ROOT / "herdr"
CODEX_ACP = Path("/home/smith/.local/share/acp-adapters/codex-acp-7cb0524624f2e730f48c3dac9b547ca130964ae9")
PRIVATE_ENV = Path("/home/smith/.config/herdres/frozen-0b94403-f50af73-r8.env")
RELEASE_LOCK = RELEASE / "r8-release-lock.json"
CANARY_EVIDENCE = RELEASE / "herdr-console-canary-evidence.json"
OPERATIONAL_SNAPSHOT = RELEASE / "operational-snapshot"
TOOLING_IDENTITY = RELEASE / "tooling-identity.json"
EXPECTED_OPERATIONAL_PATHS = frozenset(
    {
        ".deploy/acp-migrate-current-pane-r8.sh",
        ".deploy/acp-r8-release-guard.service",
        ".deploy/acp-r8-rollback.service",
        ".deploy/acp-r8-rollout.service",
        ".deploy/attest-r8.py",
        ".deploy/build-acp-release-r8.sh",
        ".deploy/guarded-herdr-restart-r8.sh",
        ".deploy/herdr-99-acp-adapter-r8.conf",
        ".deploy/herdr-console-canary-r8.py",
        ".deploy/monitor-exit-guard-r8.sh",
        ".deploy/monitor-one-hour-core-r8.py",
        ".deploy/monitor-one-hour-strict-r8.py",
        ".deploy/prepare-acp-release-r8.sh",
        ".deploy/recover-stale-r8-artifacts.py",
        ".deploy/release-guard-r8.py",
        ".deploy/release-integrity-r8.py",
        ".deploy/restore-current-pane-r8.sh",
        ".deploy/rollback-r8.sh",
        ".deploy/rollout-acp-release-r8.sh",
        ".deploy/tests/r8_failure_injection_harness.py",
        ".deploy/tests/test_herdr_console_canary_r8.py",
        ".deploy/tests/test_r8_failure_injection_harness.py",
        ".deploy/tests/test_r8_stale_owned_recovery.py",
        ".deploy/tests/test_r8_topology_normalizer.py",
        ".deploy/tests/test_r8_transaction_safety.py",
        ".deploy/topology-normalizer-r8.py",
        ".deploy/verify-live-telegram-r8.py",
    }
)
EXPECTED_CANARY_CHECKS = {
    "actionable_statuses_visible": True,
    "ambiguous_ack_accepted_once": True,
    "ambiguous_ack_not_retried": True,
    "backpressure_retry_exactly_once": True,
    "dialogue_controls_sanitized": True,
    "explicit_exit_status": 1,
    "invalid_lease_locked": True,
    "known_agent_streams_visible": True,
    "oversize_input_rejected": True,
    "post_oversize_input_exactly_once": True,
    "prewrite_retry_exactly_once": True,
    "private_bookkeeping_hidden": True,
    "shell_fallthrough_blocked": True,
    "stdin_eof_held": True,
    "terminal_reports_rejected": True,
    "terminal_reset_prefix": True,
}
EXPECTED_PROTECTED_PATHS = sorted(
    (
        "/home/smith/.config/herdres",
        "/home/smith/.config/herdr/config.toml",
        "/home/smith/.config/herdr/herdr-client.sock",
        "/home/smith/.config/herdr/herdr.sock",
        "/home/smith/.config/herdr/session.json",
        "/home/smith/.config/herdr/sessions",
        "/home/smith/.config/systemd/user",
        "/home/smith/.local/state/herdr",
        "/home/smith/.local/share/acp-runtime/active",
        "/home/smith/.local/share/tendwire/tendwire.sock",
    )
)


def file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def tree_digest(root: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    count = 0
    for path in sorted(root.rglob("*"), key=lambda item: item.relative_to(root).as_posix()):
        relative = path.relative_to(root).as_posix()
        metadata = path.lstat()
        mode = stat.S_IMODE(metadata.st_mode)
        if stat.S_ISLNK(metadata.st_mode):
            kind = "link"
            payload = os.readlink(path)
        elif stat.S_ISDIR(metadata.st_mode):
            kind = "dir"
            payload = ""
        elif stat.S_ISREG(metadata.st_mode):
            kind = "file"
            payload = file_digest(path)
        else:
            raise RuntimeError("release tree contains an unsupported entry")
        digest.update(
            json.dumps(
                [relative, kind, mode, payload],
                ensure_ascii=True,
                separators=(",", ":"),
            ).encode()
            + b"\n"
        )
        count += 1
    return digest.hexdigest(), count


def verify_operational_snapshot(root: Path, release_lock: Path) -> tuple[str, int]:
    snapshot_lock = root / ".deploy/r8-release-lock.json"
    lock_metadata = snapshot_lock.lstat()
    if not stat.S_ISREG(lock_metadata.st_mode) or lock_metadata.st_nlink != 1:
        raise RuntimeError("unsafe packaged operational lock")
    if snapshot_lock.read_bytes() != release_lock.read_bytes():
        raise RuntimeError("packaged operational lock mismatch")
    lock = json.loads(snapshot_lock.read_text(encoding="utf-8"))
    inventory = lock.get("operational_sha256")
    if not isinstance(inventory, dict) or set(inventory) != EXPECTED_OPERATIONAL_PATHS:
        raise RuntimeError("packaged operational inventory is not exact")
    actual = {
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file() or path.is_symlink()
    }
    if actual != EXPECTED_OPERATIONAL_PATHS | {".deploy/r8-release-lock.json"}:
        raise RuntimeError("packaged operational file set is not exact")
    for relative, expected_digest in inventory.items():
        path = root / relative
        metadata = path.lstat()
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
            raise RuntimeError(f"unsafe packaged operational file: {relative}")
        if file_digest(path) != expected_digest:
            raise RuntimeError(f"packaged operational digest mismatch: {relative}")
    return tree_digest(root)


def tooling_identity() -> tuple[str, str]:
    metadata = TOOLING_IDENTITY.lstat()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or metadata.st_gid != os.getegid()
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) != 0o444
    ):
        raise RuntimeError("unsafe tooling identity")
    value = json.loads(TOOLING_IDENTITY.read_text(encoding="utf-8"))
    if set(value) != {"schema_version", "commit", "tree"} or value.get("schema_version") != 1:
        raise RuntimeError("tooling identity schema mismatch")
    commit, tree = value.get("commit"), value.get("tree")
    if not all(
        isinstance(item, str)
        and len(item) == 40
        and all(character in "0123456789abcdef" for character in item)
        for item in (commit, tree)
    ):
        raise RuntimeError("tooling Git identity is invalid")
    return commit, tree


def canary_evidence_digest() -> str:
    metadata = CANARY_EVIDENCE.lstat()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or metadata.st_gid != os.getegid()
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) != 0o444
    ):
        raise RuntimeError("unsafe Herdr console canary evidence")
    value = json.loads(CANARY_EVIDENCE.read_text(encoding="utf-8"))
    expected_top = {
        "schema_version": 1,
        "canary": "herdr_acp_console_r8",
        "release_id": RELEASE_ID,
        "valid": True,
        "isolated": True,
        "historical_recovery": False,
        "production_unchanged": True,
        "temporary_root_removed": True,
        "secret_values_emitted": False,
    }
    if set(value) != set(expected_top) | {
        "production_fingerprint", "herdr", "adapter", "checks"
    }:
        raise RuntimeError("Herdr console canary evidence key set is not exact")
    for key, expected_value in expected_top.items():
        if value.get(key) != expected_value:
            raise RuntimeError(f"invalid Herdr console canary evidence: {key}")
    if value.get("checks") != EXPECTED_CANARY_CHECKS:
        raise RuntimeError("Herdr console canary check set is not exact")
    herdr = value.get("herdr") or {}
    if (
        set(herdr) != {
            "path", "sha256", "size", "mode", "uid", "gid", "link_count",
            "commit", "tree", "reported_version", "protocol",
        }
        or herdr.get("path") != str(HERDR)
        or herdr.get("sha256") != HERDR_BINARY_SHA256
        or herdr.get("mode") != 0o555
        or herdr.get("link_count") != 1
        or herdr.get("commit") != HERDR_REVISION
        or herdr.get("tree") != HERDR_TREE
        or herdr.get("reported_version")
        != HERDR_EMBEDDED_VERSION.removeprefix("herdr ")
        or herdr.get("protocol") != 19
        or herdr.get("uid") != os.geteuid()
        or herdr.get("gid") != os.getegid()
        or not isinstance(herdr.get("size"), int)
        or herdr.get("size", 0) < 1
    ):
        raise RuntimeError("Herdr console canary candidate identity mismatch")
    adapter = value.get("adapter") or {}
    if (
        set(adapter) != {
            "path", "sha256", "size", "mode", "uid", "gid", "link_count",
            "bin_dir",
        }
        or adapter.get("path") != str(CODEX_ACP / "dist/index.js")
        or adapter.get("sha256") != CODEX_ACP_BUNDLE_SHA256
        or adapter.get("mode") != 0o555
        or adapter.get("link_count") != 1
        or adapter.get("bin_dir") != str(CODEX_ACP / "bin")
        or adapter.get("uid") != os.geteuid()
        or adapter.get("gid") != os.getegid()
        or not isinstance(adapter.get("size"), int)
        or adapter.get("size", 0) < 1
    ):
        raise RuntimeError("Herdr console canary adapter identity mismatch")
    fingerprint = value.get("production_fingerprint") or {}
    if (
        set(fingerprint) != {"sha256", "paths", "entries", "regular_bytes"}
        or fingerprint.get("paths") != EXPECTED_PROTECTED_PATHS
        or not isinstance(fingerprint.get("entries"), int)
        or fingerprint.get("entries", -1) < 0
        or not isinstance(fingerprint.get("regular_bytes"), int)
        or fingerprint.get("regular_bytes", -1) < 0
        or len(str(fingerprint.get("sha256", ""))) != 64
        or any(character not in "0123456789abcdef" for character in str(fingerprint.get("sha256", "")))
    ):
        raise RuntimeError("Herdr console canary production fingerprint mismatch")
    return file_digest(CANARY_EVIDENCE)


def expected() -> dict[str, object]:
    operational_sha, operational_entries = verify_operational_snapshot(
        OPERATIONAL_SNAPSHOT, RELEASE_LOCK
    )
    tooling_revision, tooling_tree = tooling_identity()
    runtime_sha, runtime_entries = tree_digest(RUNTIME)
    release_sha, release_entries = tree_digest(RELEASE)
    codex_acp_sha, codex_acp_entries = tree_digest(CODEX_ACP)
    herdr_runtime_sha, herdr_runtime_entries = tree_digest(HERDR_ROOT)
    if (
        file_digest(CODEX_ACP / "dist/index.js") != CODEX_ACP_BUNDLE_SHA256
        or file_digest(CODEX_ACP / "provenance.json")
        != CODEX_ACP_PROVENANCE_SHA256
        or codex_acp_sha != CODEX_ACP_RUNTIME_SHA256
        or codex_acp_entries != CODEX_ACP_RUNTIME_ENTRIES
    ):
        raise RuntimeError("patched adapter bundle identity mismatch")
    if (
        file_digest(HERDR) != HERDR_BINARY_SHA256
        or file_digest(HERDR_ROOT / "provenance.json")
        != HERDR_PROVENANCE_SHA256
        or file_digest(HERDR_ROOT / "source" / f"herdr-{HERDR_REVISION}.tar")
        != HERDR_ARCHIVE_SHA256
        or herdr_runtime_sha != HERDR_RUNTIME_SHA256
        or herdr_runtime_entries != int(HERDR_RUNTIME_ENTRIES)
    ):
        raise RuntimeError("Herdr runtime identity mismatch")
    reported_version = subprocess.run(
        [str(HERDR), "--version"],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    ).stdout.strip()
    if reported_version != HERDR_EMBEDDED_VERSION:
        raise RuntimeError("Herdr embedded version mismatch")
    build_id = ""
    notes = subprocess.run(
        ["/usr/bin/readelf", "-n", str(HERDR)],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    ).stdout.splitlines()
    for line in notes:
        if "Build ID:" in line:
            build_id = line.split("Build ID:", 1)[1].strip()
            break
    if build_id != HERDR_ELF_BUILD_ID:
        raise RuntimeError("Herdr ELF build-id mismatch")
    canary_sha256 = canary_evidence_digest()
    lock = json.loads(RELEASE_LOCK.read_text(encoding="utf-8"))
    if (
        (lock.get("tendwire") or {}).get("reviewed_ancestor")
        != TENDWIRE_REVIEWED_ANCESTOR
        or (lock.get("herdres") or {}).get("reviewed_ancestor")
        != HERDRES_REVIEWED_ANCESTOR
        or (lock.get("herdr") or {}).get("reviewed_ancestor")
        != HERDR_REVIEWED_ANCESTOR
        or (lock.get("codex_acp") or {}).get("commit") != CODEX_ACP_REVISION
        or (lock.get("codex_acp") or {}).get("tree") != CODEX_ACP_TREE
        or (lock.get("codex_acp") or {}).get("bundle_sha256")
        != CODEX_ACP_BUNDLE_SHA256
        or (lock.get("herdr") or {}).get("commit") != HERDR_REVISION
        or (lock.get("herdr") or {}).get("tree") != HERDR_TREE
        or (lock.get("herdr") or {}).get("git_archive_sha256")
        != HERDR_ARCHIVE_SHA256
        or (lock.get("herdr") or {}).get("binary_sha256")
        != HERDR_BINARY_SHA256
        or (lock.get("herdr") or {}).get("binary_build_id")
        != HERDR_ELF_BUILD_ID
        or (lock.get("herdr") or {}).get("embedded_version")
        != HERDR_EMBEDDED_VERSION
        or (lock.get("herdr") or {}).get("provenance_sha256")
        != HERDR_PROVENANCE_SHA256
        or (lock.get("herdr") or {}).get("runtime_tree_sha256")
        != HERDR_RUNTIME_SHA256
        or (lock.get("herdr") or {}).get("runtime_entries")
        != int(HERDR_RUNTIME_ENTRIES)
    ):
        raise RuntimeError("release-lock provenance mismatch")
    return {
        "schema_version": 1,
        "release_id": RELEASE_ID,
        "tendwire_revision": TENDWIRE_REVISION,
        "herdres_revision": HERDRES_REVISION,
        "tendwire_reviewed_ancestor": TENDWIRE_REVIEWED_ANCESTOR,
        "herdres_reviewed_ancestor": HERDRES_REVIEWED_ANCESTOR,
        "herdr_revision": HERDR_REVISION,
        "herdr_tree": HERDR_TREE,
        "herdr_reviewed_ancestor": HERDR_REVIEWED_ANCESTOR,
        "codex_acp_revision": CODEX_ACP_REVISION,
        "codex_acp_tree": CODEX_ACP_TREE,
        "historical_recovery": False,
        "tooling_revision": tooling_revision,
        "tooling_tree": tooling_tree,
        "operational_snapshot_sha256": operational_sha,
        "operational_snapshot_entries": operational_entries,
        "runtime_sha256": runtime_sha,
        "runtime_entries": runtime_entries,
        "release_sha256": release_sha,
        "release_entries": release_entries,
        "herdr_sha256": HERDR_BINARY_SHA256,
        "herdr_archive_sha256": HERDR_ARCHIVE_SHA256,
        "herdr_build_id": HERDR_ELF_BUILD_ID,
        "herdr_embedded_version": HERDR_EMBEDDED_VERSION,
        "herdr_provenance_sha256": HERDR_PROVENANCE_SHA256,
        "herdr_runtime_sha256": herdr_runtime_sha,
        "herdr_runtime_entries": herdr_runtime_entries,
        "herdr_console_canary_sha256": canary_sha256,
        "codex_acp_version": "@agentclientprotocol/codex-acp 1.1.14",
        "codex_acp_sha256": codex_acp_sha,
        "codex_acp_entries": codex_acp_entries,
        "codex_acp_bundle_sha256": CODEX_ACP_BUNDLE_SHA256,
        "codex_acp_provenance_sha256": CODEX_ACP_PROVENANCE_SHA256,
        "release_lock_sha256": file_digest(RELEASE_LOCK),
        "private_env_sha256": file_digest(PRIVATE_ENV),
    }


def atomic_json(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
        0o444,
    )
    try:
        body = (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()
        view = memoryview(body)
        while view:
            written = os.write(descriptor, view)
            if written < 1:
                raise OSError("manifest write failed")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, path)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("write", "verify"))
    parser.add_argument("--manifest", type=Path, required=True)
    args = parser.parse_args()
    value = expected()
    if args.mode == "write":
        if args.manifest.exists() or args.manifest.is_symlink():
            raise RuntimeError("release manifest already exists")
        atomic_json(args.manifest, value)
    else:
        actual = json.loads(args.manifest.read_text(encoding="utf-8"))
        if actual != value:
            raise RuntimeError("release integrity mismatch")
    print(json.dumps({"schema_version": 1, "release_id": RELEASE_ID, "valid": True}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
