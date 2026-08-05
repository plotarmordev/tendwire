"""Static and cutover gates for the fresh concern-owned store."""

from __future__ import annotations

import ast
import io
import os
import re
import sqlite3
import tokenize
from collections import defaultdict
from pathlib import Path

import pytest

from tendwire.store.db import store_status
from tendwire.store.retention import RetentionPolicy, run_retention_cycle
from tendwire.store.schema import STORE_SCHEMA_VERSION, StoreSchemaError, init_store


ROOT = Path(__file__).resolve().parents[1]
SCAN_ROOTS = (ROOT / "src", ROOT / "scripts", ROOT / "tests")
DELETED_MAINTENANCE = {
    "CompactionOptions",
    "compact_store",
    "run_store_maintenance",
    "maybe_run_automatic_store_maintenance",
    "compact_turn_change_journal",
    "exhaust_connector_retries",
}
EXPECTED_TABLES = {
    "turns",
    "turn_content_revisions",
    "turn_content_page_boundaries",
    "attention_items",
    "pending_interactions",
    "snapshots",
    "agent_events",
    "command_receipts",
    "turn_submissions",
    "turn_supersessions",
    "backend_pending",
    "backend_pending_claims",
    "worker_bindings",
    "backend_health",
    "connector_outbox",
    "connector_deliveries",
}

_SQL_VERB = re.compile(r"\b(?:SELECT|INSERT|UPDATE|DELETE|CREATE|ALTER)\b", re.I)
_SQL_DENSITY_WORD = re.compile(
    r"\b(?:SELECT|INSERT|UPDATE|DELETE|CREATE|ALTER|FROM|WHERE|JOIN|ON|AND|OR|"
    r"SET|VALUES|INTO|CONFLICT|FOREIGN|KEY|PRIMARY|REFERENCES|RETURNING|ORDER|"
    r"GROUP|LIMIT|CHECK|NOT|NULL)\b",
    re.I,
)
_COMPOUND_STATEMENTS = (
    ast.If,
    ast.For,
    ast.AsyncFor,
    ast.While,
    ast.With,
    ast.AsyncWith,
    ast.FunctionDef,
    ast.AsyncFunctionDef,
    ast.ClassDef,
)


def _python_files() -> list[Path]:
    return sorted(path for root in SCAN_ROOTS for path in root.rglob("*.py"))


def _assignment_name(node: ast.AST, parents: dict[ast.AST, ast.AST]) -> str:
    current = node
    while current in parents:
        current = parents[current]
        if isinstance(current, (ast.Assign, ast.AnnAssign)):
            targets = (
                current.targets if isinstance(current, ast.Assign) else [current.target]
            )
            names = [target.id for target in targets if isinstance(target, ast.Name)]
            if len(names) == 1:
                return names[0]
    return "<unowned>"


def _protocol_ellipsis_lines(tree: ast.AST) -> set[int]:
    lines: set[int] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef) or not any(
            isinstance(base, ast.Name) and base.id == "Protocol" for base in node.bases
        ):
            continue
        for member in node.body:
            if not isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if (
                len(member.body) == 1
                and isinstance(member.body[0], ast.Expr)
                and isinstance(member.body[0].value, ast.Constant)
                and member.body[0].value.value is Ellipsis
                and member.body[0].lineno == member.lineno
            ):
                lines.add(member.lineno)
    return lines


def _structural_length(line: str) -> int:
    parts: list[str] = []
    try:
        tokens = tokenize.generate_tokens(io.StringIO(line).readline)
        for token in tokens:
            if token.type == tokenize.STRING:
                parts.append("S")
            elif token.type not in {
                tokenize.ENDMARKER,
                tokenize.NEWLINE,
                tokenize.NL,
                tokenize.INDENT,
                tokenize.DEDENT,
                tokenize.COMMENT,
            }:
                parts.append(token.string)
    except (IndentationError, tokenize.TokenError):
        return len(line.strip())
    return len("".join(parts))


def _statement_complexity(node: ast.stmt) -> int:
    dense_nodes = (
        ast.Call,
        ast.Dict,
        ast.List,
        ast.Tuple,
        ast.Set,
        ast.comprehension,
        ast.keyword,
        ast.BinOp,
        ast.BoolOp,
        ast.Compare,
        ast.IfExp,
    )
    return sum(isinstance(child, dense_nodes) for child in ast.walk(node))


def _packing_findings(path: Path) -> set[tuple[str, str, str]]:
    relative = str(path.relative_to(ROOT / "src"))
    source = path.read_text(encoding="utf-8")
    lines = source.splitlines()
    tree = ast.parse(source, filename=str(path))
    parents = {
        child: parent
        for parent in ast.walk(tree)
        for child in ast.iter_child_nodes(parent)
    }
    allowed_stub_lines = _protocol_ellipsis_lines(tree)
    findings: set[tuple[str, str, str]] = set()

    statements: dict[int, list[ast.stmt]] = defaultdict(list)
    for node in ast.walk(tree):
        if isinstance(node, ast.stmt) and node.lineno not in allowed_stub_lines:
            statements[node.lineno].append(node)
    for line_number, nodes in statements.items():
        if len(nodes) > 1:
            findings.add((relative, "multiple_statements", str(line_number)))

    for node in ast.walk(tree):
        if not isinstance(node, _COMPOUND_STATEMENTS):
            continue
        if node.lineno in allowed_stub_lines:
            continue
        bodies = [node.body]
        if isinstance(node, ast.If):
            bodies.append(node.orelse)
        if any(body and body[0].lineno == node.lineno for body in bodies):
            findings.add((relative, "one_line_suite", str(node.lineno)))

    for node in ast.walk(tree):
        if not isinstance(node, ast.stmt) or node.end_lineno != node.lineno:
            continue
        raw = lines[node.lineno - 1]
        if (
            len(raw) > 180
            and _structural_length(raw) > 120
            and _statement_complexity(node) >= 6
        ):
            findings.add((relative, "dense_statement", str(node.lineno)))
        value = getattr(node, "value", None)
        density = 0
        category = ""
        if isinstance(value, ast.Dict):
            density = sum(
                key is not None and key.lineno == node.lineno for key in value.keys
            )
            category = "mapping_density"
            threshold = 5
        elif isinstance(value, ast.Call):
            density = sum(
                item.lineno == node.lineno for item in (*value.args, *value.keywords)
            )
            category = "call_density"
            threshold = 6
        elif isinstance(value, (ast.List, ast.Tuple, ast.Set)):
            density = sum(item.lineno == node.lineno for item in value.elts)
            category = "sequence_density"
            threshold = 8
        else:
            threshold = 0
        if category and density >= threshold and len(raw) > 120:
            findings.add((relative, category, str(node.lineno)))

    for node in ast.walk(tree):
        if not (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "split"
            and isinstance(node.func.value, ast.Constant)
            and isinstance(node.func.value.value, str)
            and len(node.func.value.value.split()) >= 8
        ):
            continue
        findings.add(
            (
                relative,
                "vocabulary_literal",
                _assignment_name(node, parents),
            )
        )

    tokens = tokenize.generate_tokens(io.StringIO(source).readline)
    for token in tokens:
        if token.type != tokenize.STRING or not _SQL_VERB.search(token.string):
            continue
        for line_number in range(token.start[0], token.end[0] + 1):
            raw = lines[line_number - 1]
            if len(raw) <= 180:
                continue
            comma_count = raw.count(",")
            keyword_count = len(_SQL_DENSITY_WORD.findall(raw))
            if comma_count >= 6 or keyword_count >= 6:
                findings.add((relative, "sql_density", str(line_number)))
    return findings


def test_production_source_has_no_physical_packing() -> None:
    findings = set().union(
        *(
            _packing_findings(path)
            for path in sorted((ROOT / "src" / "tendwire").rglob("*.py"))
        )
    )
    assert findings == set()


def test_no_deleted_store_module_import_patch_or_reexport() -> None:
    forbidden = "tendwire." + "store" + "." + "sqlite"
    short = "store" + "." + "sqlite"
    findings: list[str] = []
    for path in _python_files():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                if any(alias.name == forbidden for alias in node.names):
                    findings.append(f"{path.relative_to(ROOT)}:{node.lineno}:import")
            elif isinstance(node, ast.ImportFrom):
                if node.module in {forbidden, short}:
                    findings.append(f"{path.relative_to(ROOT)}:{node.lineno}:from")
                if node.module == "tendwire.store" and any(
                    alias.name == "sqlite" for alias in node.names
                ):
                    findings.append(f"{path.relative_to(ROOT)}:{node.lineno}:attribute")
            elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                if forbidden in node.value or short in node.value:
                    findings.append(f"{path.relative_to(ROOT)}:{node.lineno}:string")
    assert findings == []


def test_deleted_public_maintenance_symbols_have_no_production_caller() -> None:
    findings: list[str] = []
    for path in sorted((ROOT / "src").rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            called = node.func.id if isinstance(node.func, ast.Name) else (
                node.func.attr if isinstance(node.func, ast.Attribute) else None
            )
            if called in DELETED_MAINTENANCE:
                findings.append(f"{path.relative_to(ROOT)}:{node.lineno}:{called}")
    assert findings == []


def test_fresh_schema_has_only_the_approved_application_tables(tmp_path: Path) -> None:
    db_path = tmp_path / "fresh.db"
    init_store(db_path)
    with sqlite3.connect(db_path) as conn:
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        }
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    assert version == STORE_SCHEMA_VERSION
    assert tables == EXPECTED_TABLES
    assert integrity == "ok"


@pytest.mark.parametrize("old_version", [1, STORE_SCHEMA_VERSION - 1, STORE_SCHEMA_VERSION + 1])
def test_version_mismatch_requires_explicit_discard_acknowledgement(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, old_version: int
) -> None:
    db_path = tmp_path / f"old-{old_version}.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE legacy_payload(secret TEXT)")
        conn.execute("INSERT INTO legacy_payload VALUES ('discard-me')")
        conn.execute(f"PRAGMA user_version={old_version}")
    with pytest.raises(StoreSchemaError):
        init_store(db_path)
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT secret FROM legacy_payload").fetchone() == (
            "discard-me",
        )
        assert conn.execute("PRAGMA user_version").fetchone()[0] == old_version

    caplog.set_level("WARNING", logger="tendwire.store.schema")
    init_store(db_path, discard_incompatible=True)
    with sqlite3.connect(db_path) as conn:
        tables = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        version = conn.execute("PRAGMA user_version").fetchone()[0]
    assert "legacy_payload" not in tables
    assert version == STORE_SCHEMA_VERSION
    warning = "\n".join(record.getMessage() for record in caplog.records)
    assert f"old_user_version={old_version}" in warning
    assert f"new_user_version={STORE_SCHEMA_VERSION}" in warning
    assert "tables=legacy_payload" in warning
    assert "aggregate_rows=1" in warning


def test_database_is_owner_only_and_health_is_read_only(tmp_path: Path) -> None:
    db_path = tmp_path / "secure.db"
    init_store(db_path)
    before = db_path.stat()
    health = store_status(db_path, "host-a")
    after = db_path.stat()
    assert os.stat(db_path).st_mode & 0o777 == 0o600
    assert health["schema_version"] == 1
    assert health["store_schema_version"] == STORE_SCHEMA_VERSION
    assert (after.st_dev, after.st_ino) == (before.st_dev, before.st_ino)


@pytest.mark.parametrize("extra_kind", ["trigger", "view"])
def test_same_version_extra_schema_object_requires_explicit_discard(
    tmp_path: Path, extra_kind: str
) -> None:
    db_path = tmp_path / f"extra-{extra_kind}.db"
    init_store(db_path)
    with sqlite3.connect(db_path) as conn:
        if extra_kind == "trigger":
            conn.execute(
                """CREATE TRIGGER unexpected_trigger AFTER INSERT ON snapshots
                BEGIN SELECT 1; END"""
            )
            object_name = "unexpected_trigger"
        else:
            conn.execute("CREATE VIEW unexpected_view AS SELECT host_id FROM snapshots")
            object_name = "unexpected_view"

    with pytest.raises(StoreSchemaError):
        init_store(db_path)
    with sqlite3.connect(db_path) as conn:
        assert conn.execute(
            "SELECT type FROM sqlite_schema WHERE name=?", (object_name,)
        ).fetchone() == (extra_kind,)

    init_store(db_path, discard_incompatible=True)
    with sqlite3.connect(db_path) as conn:
        assert conn.execute(
            "SELECT 1 FROM sqlite_schema WHERE name=?", (object_name,)
        ).fetchone() is None
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        }
    assert tables == EXPECTED_TABLES


def test_retention_is_bounded_and_never_runs_vacuum(tmp_path: Path) -> None:
    db_path = tmp_path / "retention.db"
    init_store(db_path)
    result = run_retention_cycle(
        db_path,
        policy=RetentionPolicy(batch_size=7),
        now="2026-08-05T00:00:00.000000Z",
    )
    assert sum(
        value for key, value in result.items() if key != "checkpoint"
    ) == 0
    assert set(result["checkpoint"]) == {"busy", "log", "checkpointed"}
