"""Single fresh Tendwire store schema and exact-version cutover."""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

from .db import _connect

STORE_SCHEMA_VERSION = 30
_LOG = logging.getLogger(__name__)


class StoreSchemaError(RuntimeError):
    """The database is not the exact current fresh schema."""


_DDL = """
CREATE TABLE turns (
 host_id TEXT NOT NULL, turn_id TEXT NOT NULL, worker_id TEXT NOT NULL,
 stable_key TEXT NOT NULL, stable_key_version INTEGER NOT NULL CHECK(stable_key_version=1),
 route_generation TEXT NOT NULL, partition_key TEXT NOT NULL,
 insertion_sequence INTEGER NOT NULL, change_sequence INTEGER NOT NULL,
 state TEXT NOT NULL, payload_json TEXT NOT NULL, content_revision TEXT,
 observed_at TEXT NOT NULL, removed_at TEXT,
 PRIMARY KEY(host_id,turn_id), UNIQUE(host_id,insertion_sequence),
 UNIQUE(host_id,change_sequence)
);
CREATE TABLE turn_content_revisions (
 id INTEGER PRIMARY KEY, host_id TEXT NOT NULL, turn_id TEXT NOT NULL,
 content_revision TEXT NOT NULL, user_text TEXT, assistant_final_text TEXT,
 known_incomplete INTEGER NOT NULL CHECK(known_incomplete IN(0,1)),
 is_current INTEGER NOT NULL CHECK(is_current IN(0,1)), created_at TEXT NOT NULL,
 UNIQUE(host_id,turn_id,content_revision),
 FOREIGN KEY(host_id,turn_id) REFERENCES turns(host_id,turn_id) ON DELETE RESTRICT
);
CREATE UNIQUE INDEX one_current_content ON turn_content_revisions(host_id,turn_id) WHERE is_current=1;
CREATE TABLE turn_content_page_boundaries (
 revision_id INTEGER NOT NULL, field TEXT NOT NULL CHECK(field IN('user_text','assistant_final_text')),
 page INTEGER NOT NULL CHECK(page>=0), start_char INTEGER NOT NULL, end_char INTEGER NOT NULL,
 start_byte INTEGER NOT NULL, end_byte INTEGER NOT NULL,
 PRIMARY KEY(revision_id,field,page),
 FOREIGN KEY(revision_id) REFERENCES turn_content_revisions(id) ON DELETE CASCADE
);
CREATE TABLE attention_items (
 host_id TEXT NOT NULL, attention_id TEXT NOT NULL, payload_json TEXT NOT NULL,
 state TEXT NOT NULL, observed_at TEXT NOT NULL, PRIMARY KEY(host_id,attention_id)
);
CREATE TABLE pending_interactions (
 host_id TEXT NOT NULL, decision_ref TEXT NOT NULL, revision_digest TEXT NOT NULL,
 worker_id TEXT NOT NULL, route_generation TEXT NOT NULL, payload_json TEXT NOT NULL,
 status TEXT NOT NULL, observed_at TEXT NOT NULL, PRIMARY KEY(host_id,decision_ref)
);
CREATE TABLE snapshots (
 id INTEGER PRIMARY KEY, host_id TEXT NOT NULL, observed_at TEXT NOT NULL,
 authority_fingerprint TEXT NOT NULL, content_fingerprint TEXT NOT NULL,
 payload_json TEXT NOT NULL,
 UNIQUE(host_id,content_fingerprint)
);
CREATE INDEX snapshots_latest ON snapshots(
 host_id,observed_at DESC,authority_fingerprint DESC
);
CREATE TABLE agent_events (
 id INTEGER PRIMARY KEY, host_id TEXT NOT NULL, backend TEXT NOT NULL,
 worker_id TEXT NOT NULL, session_id TEXT, event_id TEXT NOT NULL,
 kind TEXT NOT NULL, visibility TEXT NOT NULL, source_turn_id TEXT,
 source_item_id TEXT, source_message_id TEXT, source_event_id TEXT,
 source_sequence INTEGER, observed_at TEXT NOT NULL, payload_fingerprint TEXT NOT NULL,
 payload_json TEXT NOT NULL, public_payload_json TEXT NOT NULL,
 UNIQUE(host_id,event_id)
);
CREATE INDEX agent_events_order ON agent_events(host_id,backend,worker_id,session_id,id);
CREATE TABLE command_receipts (
 host_id TEXT NOT NULL, request_id TEXT NOT NULL, request_fingerprint TEXT NOT NULL,
 action TEXT NOT NULL, canonical_version INTEGER NOT NULL, public_worker_id TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN('reserved','send_started','accepted','rejected','uncertain')),
 status TEXT NOT NULL, owner_hash TEXT, owner_until TEXT, binding_fingerprint TEXT,
 selector_proof TEXT NOT NULL DEFAULT '', request_json TEXT NOT NULL, result_json TEXT,
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
 PRIMARY KEY(host_id,request_id)
);
CREATE TABLE turn_submissions (
 host_id TEXT NOT NULL, submission_id TEXT NOT NULL, request_id TEXT NOT NULL,
 worker_id TEXT NOT NULL, route_generation TEXT NOT NULL, instruction_fingerprint TEXT NOT NULL,
 state TEXT NOT NULL, turn_id TEXT, link_expires_at TEXT NOT NULL,
 hard_expires_at TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
 PRIMARY KEY(host_id,submission_id), UNIQUE(host_id,request_id),
 FOREIGN KEY(host_id,request_id) REFERENCES command_receipts(host_id,request_id) ON DELETE RESTRICT
);
CREATE TABLE turn_supersessions (
 host_id TEXT NOT NULL, predecessor_turn_id TEXT NOT NULL, replacement_turn_id TEXT NOT NULL,
 created_at TEXT NOT NULL, PRIMARY KEY(host_id,predecessor_turn_id,replacement_turn_id)
);
CREATE TABLE backend_pending (
 host_id TEXT NOT NULL, decision_ref TEXT NOT NULL, revision_digest TEXT NOT NULL,
 worker_id TEXT NOT NULL, route_generation TEXT NOT NULL, private_payload_json TEXT NOT NULL,
 state TEXT NOT NULL, observed_at TEXT NOT NULL, PRIMARY KEY(host_id,decision_ref)
);
CREATE TABLE backend_pending_claims (
 host_id TEXT NOT NULL, decision_ref TEXT NOT NULL, claim_token_hash TEXT NOT NULL,
 fence INTEGER NOT NULL, state TEXT NOT NULL, selection_json TEXT,
 claimed_until TEXT NOT NULL, send_started_at TEXT, settled_at TEXT,
 PRIMARY KEY(host_id,decision_ref),
 FOREIGN KEY(host_id,decision_ref) REFERENCES backend_pending(host_id,decision_ref) ON DELETE RESTRICT
);
CREATE TABLE worker_bindings (
 host_id TEXT NOT NULL, worker_id TEXT NOT NULL, backend TEXT NOT NULL,
 private_fingerprint TEXT NOT NULL, private_binding_json TEXT NOT NULL,
 stable_key TEXT NOT NULL, stable_key_version INTEGER NOT NULL CHECK(stable_key_version=1),
 route_generation TEXT NOT NULL, partition_key TEXT NOT NULL,
 next_partition_sequence INTEGER NOT NULL DEFAULT 1 CHECK(next_partition_sequence>0),
 route_retain_until TEXT NOT NULL, observed_at TEXT NOT NULL, expires_at TEXT,
 PRIMARY KEY(host_id,worker_id,route_generation),
 UNIQUE(host_id,stable_key_version,stable_key,backend,private_fingerprint),
 UNIQUE(host_id,route_generation), UNIQUE(host_id,partition_key)
);
CREATE TABLE backend_health (
 host_id TEXT NOT NULL, backend TEXT NOT NULL, payload_json TEXT NOT NULL,
 observed_at TEXT NOT NULL, PRIMARY KEY(host_id,backend)
);
CREATE TABLE connector_outbox (
 id INTEGER PRIMARY KEY, host_id TEXT NOT NULL, connector TEXT NOT NULL, key TEXT NOT NULL,
 kind TEXT NOT NULL CHECK(kind IN('generic','working','final_ready','final_part','retire','decision')),
 payload_version INTEGER NOT NULL, status TEXT NOT NULL CHECK(status IN
 ('staged','blocked','queued','leased','retry','deferred','awaiting_ack','delivered','superseded','dead_letter')),
 partition_key TEXT, partition_sequence INTEGER, turn_id TEXT, final_identity TEXT,
 decision_ref TEXT,
 content_revision TEXT, presentation_version TEXT, plan_token TEXT, plan_generation INTEGER,
 logical_sequence INTEGER, logical_ordinal INTEGER, predecessor_outbox_id INTEGER,
 replaces_outbox_id INTEGER, target_outbox_id INTEGER, source_outbox_id INTEGER,
 active_lineage_generation INTEGER, recovery_request_digest TEXT,
 recovered_from_plan_token TEXT, terminal_after_lease INTEGER NOT NULL DEFAULT 0 CHECK(terminal_after_lease IN(0,1)),
 retry_generation INTEGER NOT NULL DEFAULT 1, prior_attempt_count INTEGER NOT NULL DEFAULT 0,
 current_delivery_id INTEGER, payload_json TEXT NOT NULL,
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL, available_at TEXT NOT NULL,
 CHECK((connector='turn-final' AND kind<>'generic') OR (connector<>'turn-final' AND kind='generic')),
 CHECK((kind IN('working','retire','decision') AND payload_version=1)
    OR (kind='final_part' AND payload_version=2)
    OR (kind='final_ready' AND payload_version=3)
    OR (kind='generic' AND payload_version=1)),
 CHECK((kind='generic' AND status IN('queued','leased','retry','deferred','delivered','superseded','dead_letter'))
    OR (kind IN('working','decision') AND status IN('queued','leased','retry','deferred','delivered','superseded','dead_letter'))
    OR (kind='final_ready' AND status IN('queued','leased','retry','deferred','awaiting_ack','delivered','superseded','dead_letter'))
    OR (kind='final_part' AND status IN('staged','blocked','queued','leased','retry','deferred','delivered','superseded','dead_letter'))
    OR (kind='retire' AND status IN('blocked','queued','leased','retry','deferred','delivered','superseded','dead_letter'))),
 CHECK((kind='generic' AND partition_key IS NULL AND partition_sequence IS NULL)
    OR (kind<>'generic' AND partition_key IS NOT NULL AND partition_sequence>0)),
 CHECK((status IN('leased','awaiting_ack') AND current_delivery_id IS NOT NULL)
    OR (status NOT IN('leased','awaiting_ack') AND current_delivery_id IS NULL)),
 CHECK(retry_generation>0 AND prior_attempt_count>=0),
 CHECK(plan_generation IS NULL OR plan_generation>0),
 CHECK(logical_sequence IS NULL OR logical_sequence>0),
 CHECK(logical_ordinal IS NULL OR logical_ordinal>=0),
 CHECK(kind NOT IN('working','decision') OR
   (source_outbox_id IS NULL AND plan_token IS NULL AND plan_generation IS NULL
    AND logical_sequence IS NULL AND logical_ordinal IS NULL AND target_outbox_id IS NULL
    AND recovery_request_digest IS NULL AND recovered_from_plan_token IS NULL)),
 CHECK(kind<>'final_ready' OR
   (turn_id IS NOT NULL AND final_identity IS NOT NULL AND content_revision IS NOT NULL
    AND source_outbox_id IS NULL AND plan_token IS NULL AND plan_generation IS NULL
    AND logical_sequence IS NULL AND logical_ordinal IS NULL AND target_outbox_id IS NULL
    AND recovery_request_digest IS NULL AND recovered_from_plan_token IS NULL)),
 CHECK(kind<>'final_part' OR
   (turn_id IS NOT NULL AND final_identity IS NOT NULL AND content_revision IS NOT NULL
    AND source_outbox_id IS NOT NULL AND plan_token IS NOT NULL AND plan_generation IS NOT NULL
    AND logical_sequence IS NOT NULL AND logical_ordinal IS NOT NULL AND target_outbox_id IS NULL)),
 CHECK(kind<>'retire' OR target_outbox_id IS NOT NULL),
 UNIQUE(host_id,connector,key), UNIQUE(host_id,connector,partition_key,partition_sequence),
 UNIQUE(host_id,connector,source_outbox_id,plan_generation,logical_sequence),
 FOREIGN KEY(predecessor_outbox_id) REFERENCES connector_outbox(id) ON DELETE RESTRICT,
 FOREIGN KEY(replaces_outbox_id) REFERENCES connector_outbox(id) ON DELETE RESTRICT,
 FOREIGN KEY(target_outbox_id) REFERENCES connector_outbox(id) ON DELETE RESTRICT,
 FOREIGN KEY(source_outbox_id) REFERENCES connector_outbox(id) ON DELETE RESTRICT,
 FOREIGN KEY(current_delivery_id) REFERENCES connector_deliveries(id) ON DELETE RESTRICT
);
CREATE UNIQUE INDEX outbox_recovery_request ON connector_outbox(host_id,connector,recovery_request_digest)
 WHERE recovery_request_digest IS NOT NULL;
CREATE TABLE connector_deliveries (
 id INTEGER PRIMARY KEY, outbox_id INTEGER NOT NULL, retry_generation INTEGER NOT NULL,
 attempt INTEGER NOT NULL, ref_hash TEXT NOT NULL UNIQUE,
 status TEXT NOT NULL CHECK(status IN('leased','awaiting_ack','acknowledged','failed','deferred','released','expired')),
 leased_at TEXT NOT NULL, leased_until TEXT NOT NULL, ack_deadline_at TEXT,
 public_response_json TEXT, private_reason_enum TEXT,
 created_at TEXT NOT NULL, settled_at TEXT,
 CHECK(retry_generation>0 AND attempt>0),
 CHECK((status='awaiting_ack' AND ack_deadline_at IS NOT NULL)
    OR (status<>'awaiting_ack' AND ack_deadline_at IS NULL)),
 CHECK(private_reason_enum IS NULL OR private_reason_enum IN(
   'temporary','rate_limited','provider_rejected','provider_uncertain',
   'invalid_payload','content_unavailable','route_unavailable',
   'provider_binding_unknown','lease_expired','ack_deadline_expired',
   'superseded','attempts_exhausted','operator_recovery')),
 UNIQUE(outbox_id,retry_generation,attempt),
 FOREIGN KEY(outbox_id) REFERENCES connector_outbox(id) ON DELETE RESTRICT
);
CREATE UNIQUE INDEX one_live_delivery ON connector_deliveries(outbox_id)
 WHERE status IN('leased','awaiting_ack');
"""


def ensure_schema(conn: sqlite3.Connection) -> None:
    version = int(conn.execute("PRAGMA user_version").fetchone()[0])
    if version != STORE_SCHEMA_VERSION:
        raise StoreSchemaError(f"store schema {version} is not {STORE_SCHEMA_VERSION}")
    expected = sqlite3.connect(":memory:")
    try:
        expected.executescript(_DDL)
        expected_objects = _schema_objects(expected)
    finally:
        expected.close()
    actual_objects = _schema_objects(conn)
    if actual_objects != expected_objects:
        raise StoreSchemaError("store schema objects do not match the exact current schema")
    foreign_key_failure = conn.execute("PRAGMA foreign_key_check").fetchone()
    if foreign_key_failure is not None:
        raise StoreSchemaError("store foreign-key integrity check failed")


def _schema_objects(conn: sqlite3.Connection) -> dict[tuple[str, str], str]:
    rows = conn.execute(
        """SELECT type,name,sql FROM sqlite_schema
        WHERE type IN('table','index','trigger','view')
          AND name NOT LIKE 'sqlite_%' AND sql IS NOT NULL
        ORDER BY type,name"""
    ).fetchall()
    return {
        (str(row[0]), str(row[1])): " ".join(str(row[2]).split())
        for row in rows
    }


def _discard_summary(conn: sqlite3.Connection) -> tuple[list[str], int]:
    tables = [str(row[0]) for row in conn.execute(
        """SELECT name FROM sqlite_schema WHERE type='table'
        AND name NOT LIKE 'sqlite_%' ORDER BY name"""
    ).fetchall()]
    count = 0
    for table in tables:
        quoted = table.replace('"', '""')
        count += int(conn.execute(f'SELECT COUNT(*) FROM "{quoted}"').fetchone()[0])
    return tables, count


def _recreate(conn: sqlite3.Connection, tables: list[str]) -> None:
    objects = conn.execute(
        """SELECT type,name FROM sqlite_schema
        WHERE type IN('trigger','view') AND name NOT LIKE 'sqlite_%'
        ORDER BY CASE type WHEN 'trigger' THEN 0 ELSE 1 END,name"""
    ).fetchall()
    drops = []
    for kind, name in objects:
        quoted = str(name).replace('"', '""')
        drops.append(f'DROP {str(kind).upper()} IF EXISTS "{quoted}";')
    for table in tables:
        quoted = table.replace('"', '""')
        drops.append(f'DROP TABLE IF EXISTS "{quoted}";')
    conn.executescript(
        "PRAGMA foreign_keys=OFF;\nBEGIN IMMEDIATE;\n"
        + "\n".join(drops)
        + "\n"
        + _DDL
        + f"\nPRAGMA user_version = {STORE_SCHEMA_VERSION};\nCOMMIT;\n"
        + "PRAGMA foreign_keys=ON;"
    )


def init_store(path: Path | str, *, discard_incompatible: bool = False) -> None:
    """Create the exact schema; incompatible state needs explicit discard approval."""
    db_path = Path(path)
    existed = db_path.exists()
    conn = _connect(db_path, writable=True, create=not existed)
    try:
        version = int(conn.execute("PRAGMA user_version").fetchone()[0])
        objects = int(conn.execute("SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'").fetchone()[0])
        if version == 0 and objects == 0:
            conn.executescript(
                "BEGIN IMMEDIATE;\n"
                + _DDL
                + f"\nPRAGMA user_version = {STORE_SCHEMA_VERSION};\nCOMMIT;"
            )
        else:
            try:
                ensure_schema(conn)
            except StoreSchemaError:
                if not discard_incompatible:
                    raise
                tables, row_count = _discard_summary(conn)
                _LOG.warning(
                    "discarding incompatible Tendwire store after explicit approval: "
                    "old_user_version=%d new_user_version=%d tables=%s aggregate_rows=%d",
                    version,
                    STORE_SCHEMA_VERSION,
                    ",".join(tables) if tables else "<none>",
                    row_count,
                )
                _recreate(conn, tables)
                ensure_schema(conn)
    except BaseException:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        conn.close()
        if not existed and db_path.exists():
            db_path.unlink()
        raise
    conn.close()


__all__ = ("STORE_SCHEMA_VERSION", "StoreSchemaError", "ensure_schema", "init_store")
