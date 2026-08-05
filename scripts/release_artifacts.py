#!/usr/bin/env python3
"""Validate Tendwire RC source and built artifacts with standard-library checks."""

from __future__ import annotations

import argparse
import ast
import json
import os
import re
import stat
import subprocess
import sys
import tarfile
import tempfile
import tomllib
import zipfile
from pathlib import Path, PurePosixPath
from typing import Iterable


ROOT = Path(__file__).resolve().parents[1]
FORBIDDEN_PARTS = {
    ".git",
    ".pytest_cache",
    "__pycache__",
    "installation.key",
    "installation.key.sha256",
    "installation.key.initialized",
}
FORBIDDEN_SUFFIXES = (".db", ".db-wal", ".db-shm", ".db-journal", ".pyc", ".sock")
SECRET_PATTERNS = {
    "telegram_bot_token": re.compile(rb"(?<![A-Za-z0-9])[0-9]{8,12}:[A-Za-z0-9_-]{30,}"),
    "openai_key": re.compile(rb"(?<![A-Za-z0-9])sk-[A-Za-z0-9_-]{20,}"),
    "github_token": re.compile(rb"(?<![A-Za-z0-9])gh[pousr]_[A-Za-z0-9]{20,}"),
    "aws_access_key": re.compile(rb"(?<![A-Z0-9])AKIA[A-Z0-9]{16}"),
}
REQUIRED_SDIST = {
    ".env.example",
    "INSTALL.md",
    "LICENSE",
    "README.md",
    "RELEASE.md",
    "SECURITY.md",
    "docs/acp-primary-release-proof.md",
    "tendwired.service.example",
    "pyproject.toml",
    "scripts/release_artifacts.py",
}
INSTALL_SMOKE_DOCTOR_STATUSES = frozenset({"ok", "degraded", "unavailable"})
MAX_ARCHIVE_MEMBER_BYTES = 16 * 1024 * 1024
MAX_ARCHIVE_TOTAL_BYTES = 64 * 1024 * 1024


class ReleaseCheckError(RuntimeError):
    pass


def _project() -> dict[str, object]:
    return tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]


def _package_version() -> str:
    tree = ast.parse((ROOT / "src/tendwire/_version.py").read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "__version__"
            for target in node.targets
        ):
            return ast.literal_eval(node.value)
    raise ReleaseCheckError("package_version_missing")


def _tracked_files() -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files", "-z"], cwd=ROOT, check=True, capture_output=True
    )
    return [ROOT / value.decode("utf-8") for value in result.stdout.split(b"\0") if value]


def _forbidden_name(name: str) -> bool:
    path = PurePosixPath(name)
    return (
        path.is_absolute()
        or ".." in path.parts
        or bool(FORBIDDEN_PARTS.intersection(path.parts))
        or name.endswith(FORBIDDEN_SUFFIXES)
    )


def _scan_secret(name: str, content: bytes) -> None:
    if b"\0" in content[:4096]:
        return
    for label, pattern in SECRET_PATTERNS.items():
        if pattern.search(content):
            raise ReleaseCheckError(f"secret_pattern:{label}:{name}")


def check_source() -> dict[str, object]:
    project = _project()
    version = _package_version()
    if project.get("dynamic") != ["version"]:
        raise ReleaseCheckError("version_source_mismatch")
    if project.get("requires-python") != ">=3.13":
        raise ReleaseCheckError("python_support_mismatch")
    classifiers = project.get("classifiers")
    if not isinstance(classifiers, list) or "Programming Language :: Python :: 3.13" not in classifiers:
        raise ReleaseCheckError("python_classifier_missing")
    tracked = _tracked_files()
    for path in tracked:
        relative = path.relative_to(ROOT).as_posix()
        if _forbidden_name(relative):
            raise ReleaseCheckError(f"forbidden_source:{relative}")
        _scan_secret(relative, path.read_bytes())
    return {"status": "ok", "version": version, "tracked_files": len(tracked)}


def _archive_members(path: Path) -> list[tuple[str, bytes]]:
    total = 0
    if path.suffix == ".whl":
        with zipfile.ZipFile(path) as archive:
            members = []
            for item in archive.infolist():
                if item.is_dir():
                    continue
                mode = (item.external_attr >> 16) & 0xFFFF
                if mode and stat.S_ISLNK(mode):
                    raise ReleaseCheckError(f"archive_link_forbidden:{item.filename}")
                if item.file_size > MAX_ARCHIVE_MEMBER_BYTES:
                    raise ReleaseCheckError(f"archive_member_too_large:{item.filename}")
                total += item.file_size
                if total > MAX_ARCHIVE_TOTAL_BYTES:
                    raise ReleaseCheckError("archive_too_large")
                members.append((item.filename, archive.read(item)))
            return members
    if path.name.endswith(".tar.gz"):
        with tarfile.open(path, "r:gz") as archive:
            members = []
            for item in archive.getmembers():
                if not (item.isfile() or item.isdir()):
                    raise ReleaseCheckError(f"archive_link_forbidden:{item.name}")
                if item.isfile():
                    if item.size > MAX_ARCHIVE_MEMBER_BYTES:
                        raise ReleaseCheckError(f"archive_member_too_large:{item.name}")
                    total += item.size
                    if total > MAX_ARCHIVE_TOTAL_BYTES:
                        raise ReleaseCheckError("archive_too_large")
                    stream = archive.extractfile(item)
                    if stream is None:
                        raise ReleaseCheckError(f"archive_member_unreadable:{item.name}")
                    members.append((item.name, stream.read()))
            return members
    raise ReleaseCheckError(f"unsupported_artifact:{path.name}")


def _strip_sdist_root(name: str) -> str:
    parts = PurePosixPath(name).parts
    return PurePosixPath(*parts[1:]).as_posix() if len(parts) > 1 else name


def check_artifacts(directory: Path) -> dict[str, object]:
    artifacts = sorted([*directory.glob("*.whl"), *directory.glob("*.tar.gz")])
    if len(artifacts) != 2 or not any(path.suffix == ".whl" for path in artifacts):
        raise ReleaseCheckError("expected_one_wheel_and_one_sdist")
    version = _package_version()
    manifest: dict[str, list[str]] = {}
    for artifact in artifacts:
        members = _archive_members(artifact)
        names = sorted(name for name, _ in members)
        manifest[artifact.name] = names
        for name, content in members:
            if _forbidden_name(name):
                raise ReleaseCheckError(f"forbidden_artifact_member:{artifact.name}:{name}")
            _scan_secret(f"{artifact.name}:{name}", content)
        if artifact.name.endswith(".tar.gz"):
            relative = {_strip_sdist_root(name) for name in names}
            missing = sorted(REQUIRED_SDIST - relative)
            if missing:
                raise ReleaseCheckError(f"sdist_missing:{','.join(missing)}")
        metadata_suffix = ".dist-info/METADATA" if artifact.suffix == ".whl" else "/PKG-INFO"
        metadata = [content for name, content in members if name.endswith(metadata_suffix)]
        if (
            len(metadata) != 1
            or f"Version: {version}\n".encode() not in metadata[0]
            or b"Requires-Python: >=3.13\n" not in metadata[0]
        ):
            raise ReleaseCheckError("artifact_metadata_mismatch")
    (directory / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return {"status": "ok", "artifacts": [path.name for path in artifacts]}


def install_smoke(artifact: Path) -> dict[str, object]:
    with tempfile.TemporaryDirectory(prefix="tendwire-release-smoke-") as raw:
        root = Path(raw)
        venv = root / "venv"
        subprocess.run([sys.executable, "-m", "venv", str(venv)], check=True)
        python = venv / "bin/python"
        pip = venv / "bin/pip"
        install = [
            str(pip),
            "install",
            "--disable-pip-version-check",
            "--no-index",
            "--no-deps",
        ]
        if artifact.name.endswith(".tar.gz"):
            install.append("--no-build-isolation")
            # A clean sdist environment still needs its declared PEP 517 backend.
            subprocess.run(
                [str(pip), "install", "--disable-pip-version-check", "hatchling>=1.27,<2"],
                check=True,
            )
        subprocess.run([*install, str(artifact.resolve())], check=True)
        env = {**os.environ, "HOME": str(root / "home"), "TENDWIRE_DATA_DIR": str(root / "state")}
        subprocess.run([str(venv / "bin/tendwire"), "--help"], env=env, check=True, capture_output=True)
        doctor = subprocess.run(
            [str(venv / "bin/tendwire"), "doctor", "--json"],
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )
        if doctor.returncode not in {0, 1}:
            raise ReleaseCheckError("doctor_exit_invalid")
        payload = json.loads(doctor.stdout)
        if (
            not isinstance(payload, dict)
            or payload.get("status") not in INSTALL_SMOKE_DOCTOR_STATUSES
        ):
            raise ReleaseCheckError("doctor_payload_invalid")
        imported = subprocess.run(
            [str(python), "-c", "import tendwire; print(tendwire.__version__)"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        if imported != _package_version():
            raise ReleaseCheckError("installed_version_mismatch")
    return {"status": "ok", "artifact": artifact.name}


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("source")
    artifacts = subparsers.add_parser("artifacts")
    artifacts.add_argument("directory", type=Path)
    smoke = subparsers.add_parser("install-smoke")
    smoke.add_argument("artifact", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "source":
            result = check_source()
        elif args.command == "artifacts":
            result = check_artifacts(args.directory)
        else:
            result = install_smoke(args.artifact)
    except (ReleaseCheckError, OSError, subprocess.SubprocessError, json.JSONDecodeError) as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}, sort_keys=True))
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
