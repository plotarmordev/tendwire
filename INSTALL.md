# Install

Tendwire can be run from a checkout or installed as a Python package. The
`0.1.0rc5` release candidate supports Python 3.13. Other Python runtimes have
not been release-qualified for this candidate.

## From A Checkout

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e .
tendwire doctor --json
```

For release development and artifact checks, install the bounded tool extra:

```bash
python -m pip install -e '.[dev]'
```

The installed-candidate sidecar benchmark requires an explicit paired Herdres
checkout via `--herdres-root` or `TENDWIRE_BENCHMARK_HERDRES_ROOT`; it has no
user-specific source-tree default.

For direct module execution from a source tree without installing:

```bash
PYTHONPATH=src python3 -m tendwire.cli doctor --json
```

## Source-Mode Service Setup

For daily source-mode use with Herdres, run Tendwire as the Herdr observation and
command-routing daemon:

```ini
[Unit]
Description=Tendwire daemon

[Service]
Type=simple
UMask=0077
Environment=TENDWIRE_HERDR_BACKEND=socket
Environment=TENDWIRE_DB_PATH=%h/.local/share/tendwire/tendwire.db
Environment=TENDWIRE_TURN_REFRESH_INTERVAL_SECONDS=2.0
Environment=TENDWIRE_TURN_REFRESH_WORKERS=4
ExecStart=%h/.local/bin/tendwire daemon --db-path %h/.local/share/tendwire/tendwire.db
KillMode=control-group
KillSignal=SIGTERM
TimeoutStopSec=20s
FinalKillSignal=SIGKILL
Restart=always
RestartSec=5s

[Install]
WantedBy=default.target
```

The same unit is shipped as `tendwired.service.example`. Install it as
`~/.config/systemd/user/tendwired.service`, then run:

```bash
systemctl --user daemon-reload
systemctl --user enable --now tendwired.service
systemctl --user is-active tendwired.service
```

Template ownership is split for Herdres-managed source installs: Herdres's
`systemd/user/tendwired.service.example` and `install-user.sh` generate the
production unit with its `PYTHONPATH`, adapter environment, and drop-ins; they
do not consume Tendwire's shipped example. Keep the four shutdown-hardening
directives synchronized in that Herdres-owned template and verify the generated
unit after either repository changes.

`KillMode=control-group` is intentional: the daemon's Herdr adapter and
isolated turn readers are supervised children in the same service cgroup.
`SIGTERM` starts Tendwire's bounded shutdown, closes the listening socket, and
cancels active ingestion; systemd sends `SIGKILL` to any exceptional survivor
after the 20-second stop deadline. Tendwire is designed not to fork, daemonize,
create a new session, or transfer the listening socket, so `MainPID` should be
the socket-holding daemon process. The observed stale-holder incident involved
an unhardened production unit and the former synchronous signal-teardown hazard;
the available evidence did not establish a specific process-escape mechanism.
If another live process already holds the
configured socket, startup fails with a non-zero status instead of replacing
the endpoint or running a second daemon.

## Background Turn Ingestion Operations

The daemon, not each read request, owns turn refresh. It performs an immediate
scan on startup, repeats it every
`TENDWIRE_TURN_REFRESH_INTERVAL_SECONDS` (default `2.0`), and accepts coalesced
signals from relevant persisted pane-event batches and completed full
reconciles. `TENDWIRE_TURN_REFRESH_WORKERS` defaults to `4`, must be from 1
through 32, and cannot exceed `TENDWIRE_MAX_WORKERS`. Every adapter uses
`TENDWIRE_HERDR_TIMEOUT_SECONDS`; the queue is fixed at 64. One private target
is serialized with itself while distinct targets can use the worker pool.
OMP JSONL cache/IPC state is coordinate-only: parse/EOF and replay offsets,
observed file identity/size/timestamps, an open-turn flag, and validated project
root. It never contains or transports prompt IDs or user/final/stream bodies.
An unchanged stable stat returns unchanged without a child spawn, file read, or
IPC frame. A changed open turn reconstructs from its replay coordinate; after a
completed final the checkpoint becomes idle at EOF, and only a new eligible
user message opens another turn. Its LRU is capped at 64 entries and 64 KiB of
serialized key-plus-checkpoint weight.

A Codex binding must be an exact canonical lowercase, non-nil UUID. Its only
eligible rollout path is
`sessions/YYYY/MM/DD/rollout-YYYY-MM-DDTHH-MM-SS-<uuid>.jsonl`, with a valid
date/time matching the hierarchy; no identifier is interpolated into a glob.
The resolver accepts exactly one canonical regular in-root file after symlink
and device/inode checks. A missing, unsafe, over-limit, or duplicate exact
identity is unavailable.
Every cache hit validates the current sessions-root device/inode; a found result
also validates the rollout inode. A root identity change immediately clears
that root's cached path results and complete index.

The complete Codex index is capped at 100,000 visited entries, 100,000 session
identities, and 16 MiB; its path-result LRU is capped at 256 entries and 256 KiB.
Negative results expire after 2 seconds. A lookup rebuilds the complete index
once its snapshot is 60 seconds old. Consequently, a newly created duplicate
may remain undiscovered for up to one 60-second snapshot interval before the
refreshed index makes that identity ambiguous and unavailable.

Codex parser state is private and held in a 64-entry, 16 MiB LRU. Only valid
newline-terminated records advance its committed offset; a partial final record
waits for its newline. The bounds are 8 MiB per record, a 64 KiB initial and
16 MiB maximum/65,536-record recovery scan, and 64 MiB of source data per poll.
Warm append-only polls read only appended bytes; an unchanged poll reads zero
source bytes. Truncation, rotation, replacement, or missing state uses this
bounded recovery, while malformed/oversized input or no recoverable boundary
fails without advancing the prior checkpoint.
No Codex session ID, rollout path, file identity/coordinate, parser/cache state,
or raw record enters public JSON.

OMP reader requests remain capped at 16 KiB. A canonical OMP response has no
total-size ceiling: its exact ordered payload is streamed in frames of at most
1 MiB under the same deadline, with manifest, nonce, end-marker, and EOF
validation, so canonical finals are not truncated by IPC. Codex parser-state
requests are capped at 12 MiB, and each Codex response remains one frame capped
at 64 MiB. Nonblocking framed-socket send/receive, parsing, and child join share
the single adapter deadline without a helper IPC thread. Timeout teardown spends
at most 250 ms on terminate/kill/join attempts and reaps the child under normal
POSIX scheduling; it does not wait beyond that grace for an OS-uninterruptible
child.

A file-reader checkpoint remains a candidate until Tendwire revalidates the
exact binding and durably applies any content; a no-content candidate still
requires binding validation. Cancellation, apply failure, stale/replaced
binding, or a same-path fingerprint-generation change leaves the prior cache
untouched.

`health.get`'s `result.turn_ingestion` reports only aggregate status,
queue/active counts, outcome counters, last-success/duration/staleness values,
and configured bounds. `stale` means no sufficiently recent successful
refresh. A failed binding scan or consecutive adapter failure/timeout produces
`degraded`; a later successful scan/refresh recovers current health even though
the lifetime `failed` and `timed_out` counters remain cumulative.
`stale_binding` churn contributes to the aggregate failed counter without
keeping health degraded. Worker IDs, target kinds/values, session paths, and
private fingerprints are never included. Cached `turn.list`, `health.get`,
`snapshot.get`, `attention.list`, and `pending.list` requests remain
independent of blocked adapters.

Daemon and CLI pending reads share one store projection that atomically reads
the latest stored public snapshot with durable `backend_pending` rows. A
malformed snapshot returns `store_unavailable`. On definite daemon
unavailability before transmission, the CLI reads only this store view and
never observes Herdr. A timeout after transmission returns `daemon_timeout`;
other ambiguous or invalid exchanges return `daemon_protocol_error`, with no
fallback source read.

Turn ingestion is also the sole owner of backend-pending transitions. An open
prompt is upserted, a successful no-prompt read stores a non-answerable
tombstone, and authoritative binding removal reaps the row. A transient read
retains the last prompt as stale for one fixed, non-sliding
`TENDWIRE_PENDING_STALE_GRACE_SECONDS` window (30 seconds by default); repeated
failures do not extend the deadline, and degraded freshness remains visible
after prompt expiry until recovery. A malformed prompt exposes snapshot
fallback immediately while reporting degraded freshness. `answer_pending`
commands bind the public pending ID, fingerprint, and choice ID to the current
durable revision, binding fingerprint, and exact private pane target before any
pane mutation.

On `SIGINT` or `SIGTERM`, the daemon stops accepting socket work, flushes and
detaches event refresh signaling, cancels and boundedly drains ingestion, then
stops the backend. Stop Tendwire only after its consumers for a coordinated
maintenance window. A stopped process cannot be restarted in place; a service
manager restart creates a fresh daemon, whose immediate scan reuses durable
bindings. After restart, confirm fresh health and cached reads are available
without losing final-turn or connector state; `turn_ingestion` may initially be
`stale` until a refresh succeeds and reports `degraded` if adapters fail. There
is no `SIGHUP` reload/restart contract.

## Local-State Permissions and Daemon Socket Access

Production POSIX deployments are private-only by default. Tendwire keeps the
configured state directory at mode `0700`; the database, its SQLite sidecars,
and regular private state files are mode `0600`. The default daemon Unix socket
is also mode `0600`. Keep the service `UMask=0077` line above: it protects every
process-created file, while Tendwire also enforces the final modes itself.

For existing entries owned by the service account, Tendwire removes broad
permission bits by intersecting the current mode with the required mode; it
never widens an already stricter mode. It refuses symlinks, entries owned by
another account, and entries of the wrong type rather than following, replacing,
or changing their ownership. New private files are created securely before
their names are published.

Daemon socket sharing is an explicit opt-in through
`tendwire daemon --socket-group GROUP` or `TENDWIRE_SOCKET_GROUP=GROUP`. Tendwire
normalizes the configured name, then resolves the existing group and verifies
that the service account is a current member before changing the socket group
or mode. The shared socket is mode `0660`; the database and other local state
remain private.

Every member of that group can invoke the daemon's full API, including mutating
commands and connector operations. Use a dedicated socket parent owned by the
service account, assigned to the selected group, group-traversable, and not
accessible by other users (for example, mode `0710`). A shared socket cannot
live under the default mode-`0700` state directory. Never put a Tendwire socket
in shared `/tmp`.

## Continuity State, Backup, Upgrade, Loss, and Rotation

Tendwire keeps its optional worker-continuity identity in the configured
`data_dir` (default `~/.local/share/tendwire`, controlled by
`TENDWIRE_DATA_DIR`), independently of an overridden `TENDWIRE_DB_PATH`. On
first use with a valid worker identity, Tendwire creates a 32-byte private
`installation.key`, publishes its nonsecret digest marker
`installation.key.sha256`, validates the pair, and only then creates
`installation.key.initialized`. The sentinel contains the exact nonsecret
one-byte value `1`. The data directory is restricted to mode `0700`; all three
files are restricted to mode `0600`. The service `UMask=0077` above protects
newly created local state as well. Once initialized, ordinary loads only
validate and reuse this state; they never rotate it.

The public continuity contract is the exact
`meta.stable_key`/`meta.stable_key_version` pair derived from an authoritative
Herdr public workspace/pane identity; the supported version is the integer `1`.
For the same installation and authoritative identity, the key must remain
byte-for-byte identical across Tendwire worker-ID churn. Treat an absent,
partial, malformed, or unknown-version pair as a continuity failure: do not
rebind it by worker ID. Herdres keeps such local state quarantined; routing can
recover only from a later valid Tendwire refresh that supplies the
authoritative pair.

Treat Tendwire and Herdres continuity data as one recovery unit:

1. Identify the active Tendwire database path, Tendwire `data_dir`, and the
   deployment-specific Herdres persistent state path. Stop Herdres consumers
   first, then stop `tendwired.service`, and confirm both are stopped before
   copying anything.
2. Into one access-restricted backup, copy the Tendwire database,
   `data_dir/installation.key`, `data_dir/installation.key.sha256`,
   `data_dir/installation.key.initialized`, and the complete Herdres persistent
   state. Preserve ownership and modes. Use the SQLite backup API for every
   SQLite database, or copy database files only after all writers are confirmed
   stopped; never copy a live database file by itself. The three identity
   artifacts must come from the same stopped-service checkpoint and must be
   backed up and restored together. Do not publish the backup as an issue
   attachment or build artifact.

Keep that checkpoint unchanged. Before allowing candidate code to initialize
or migrate local SQLite state, make a second access-restricted scratch copy,
point the candidate only at that copy, and run the status and read-only SQLite
integrity checks below. Confirm that a known authoritative worker retains the
exact version-1 stable key even if its Tendwire worker ID changed. If any check
fails or the identity is quarantined, discard the scratch copy and investigate;
the untouched checkpoint remains the rollback source. Never test a migration
against the only recovery copy or attempt to repair its rows by hand.
3. For an ordinary upgrade, update only the installed software or checkout.
   Retain the database, all three identity artifacts, and Herdres state
   unchanged. Restart Tendwire first, verify that a known same-workspace worker
   retains its `meta.stable_key`, then restart Herdres and verify the existing
   binding/topic remains singular.
4. For a restore, keep both services stopped and restore the database, all
   three Tendwire identity artifacts, and Herdres state from the same recovery
   checkpoint. Restore service-user ownership, set the Tendwire data directory
   to mode `0700`, and set `installation.key`, `installation.key.sha256`, and
   `installation.key.initialized` to mode `0600` before starting Tendwire.
   Confirm that the sentinel is exactly one byte containing `1`. Start Tendwire
   and validate continuity before starting Herdres.
5. Once `installation.key.initialized` exists, a missing key or digest marker
   fails closed, including loss of both files while the sentinel remains. A
   replaced key, key/digest mismatch, malformed sentinel, unsafe mode, or wrong
   ownership also fails closed instead of rotating or accepting source
   identity. An absent sentinel is only initial-bootstrap or legacy-migration
   state, never a rotation request: Tendwire must validate and publish the key
   and digest before publishing the sentinel. If continuity state is damaged,
   stop Tendwire and every identity consumer and restore the complete recovery
   set; never repair or copy individual artifacts.

Deliberate rotation is a separate, destructive identity operation, not an
ordinary load, upgrade, or recovery shortcut:

1. Stop Tendwire and every identity consumer, including Herdres, and make a
   fresh coherent backup as described above.
2. From a controlled operator Python environment, invoke
   `tendwire.worker_identity.reset_installation_key(Path(data_dir),
   acknowledge_continuity_break=True)`. This acknowledged reset validates and
   removes the existing three-artifact identity while consumers are offline.
   Do not delete, replace, or edit any identity artifact by hand.
3. On the next eligible Tendwire load, bootstrap a new key, digest, and
   one-byte initialized sentinel. Keep all consumers stopped until Tendwire has
   completed a valid observation. Confirm that every observed
   `meta.stable_key` changed; rotation intentionally provides no old-to-new
   equivalence.
4. Review and explicitly migrate or retire old Herdres state, bindings, and
   topics before enabling normal reconciliation. Herdres quarantines stale
   bindings and creates distinct topics for the new handles; it does not
   silently rebind workers or automatically reuse topics after rotation.

Restore the coherent recovery set instead whenever continuity is meant to
survive.

## SQLite Family Validation, Preparation, and Repair

Treat `TENDWIRE_DB_PATH` as the main member of a four-name SQLite family:
the main database, `-wal`, `-shm`, and `-journal`. The three sidecars are
optional. Their absence, including transient disappearance during one bounded
inspection, is valid. After the main database has been selected, it is
mandatory; disappearance or replacement is a fail-closed `entry_changed`
condition. Every present member must be an owned regular file. Wrong type,
symlink, wrong owner, insecure validation-time permissions, or identity
replacement fails closed without following or mutating the offending entry.

Authority is deliberately narrow:

- Ordinary reads, `tendwire doctor --json`, daemon health, and
  `tendwire store status` are validation-only and non-creating. They do not
  initialize a missing main database, synthesize an absent sidecar, or repair
  permissions.
- Store startup and creation-capable writes explicitly prepare the family.
  Prepare may create the missing main database and may intersect a validated
  present member's mode with `0600`; it never creates an optional sidecar and
  never widens a stricter mode.
- The explicit repair path only intersects modes on validated existing
  members. It performs whole-family prevalidation before changing any mode and
  never replaces an entry or changes ownership.
- Private-mode preparation and repair cannot disturb active Tendwire SQLite
  transactions, and a no-op private prepare preserves them. Any main creation or
  permission narrowing first requires bounded, nonblocking exclusive authority over
  the store parent directory. Current-schema filesystem reads stay cheap and
  nonmutating after their schema-version read: they take no exclusive parent
  authority and perform no persistent WAL negotiation or schema DDL. An
  uninitialized or migrating filesystem store takes that exclusive authority before
  persistent WAL negotiation or schema DDL, performs that work under private
  creation mode, then revalidates and narrows the resulting main database, `-wal`,
  and `-shm` members before restoring retained shared authority. A live Tendwire
  connection retains shared parent-directory authority, so a conflicting repair
  fails with a typed, path-free error before mutation; that shared authority also
  rejects the schema branch before WAL, DDL, or sidecar mutation. A connection
  obtains shared authority before preparation, promotes the same authority only for
  a necessary mutation, and restores shared authority for the remainder of its lifetime.

The family algorithm performs finite capture, preflight, and final validation
rather than recursively retrying churn. An optional sidecar that vanishes is
accepted as absent; a newly observed valid optional may be captured once; a
replacement, hostile appearance, or selected-main change aborts. Private code
receives typed, path-free `LocalStateError` failures. Public doctor/status
surfaces return fixed aggregate records such as `database_permissions:
unsafe` or `store_unavailable`, with no path, SQLite suffix, UID/GID, inode, raw
exception, or private bytes.

Automatic retention remains bounded to its configured batch and cadence and
has no authority to compact or replace the database. Only explicit offline
`store compact --execute --acknowledge-offline` has compaction authority, and
it must still revalidate the selected source identity before publishing its
verified replacement.

## Store Retention and Offline Compaction

Snapshot history stores changes, not polling volume. An observation whose
content fingerprint is identical to the immediately preceding snapshot for the
same host refreshes that row; a non-adjacent return to earlier content appends a
new row. The defaults retain a 14-day window and the newest 4096 changed rows
per host, including the latest. Fourteen days at one observation every five
minutes is $14 \times 288 = 4032$, so the count default leaves 64 rows of
headroom. A historical row must remain inside both the age and count windows to
be retained; falling outside either makes it eligible. The newest row for each
host is exempt from both limits.

Snapshot and binding acceptance is monotonic and atomic per host. The greatest
`(updated_at, content_fingerprint)` pair wins; an exact replay is a no-op.
A losing older/equal-time observation cannot replace history, regress/prune
worker or turn projections, mutate/expire private bindings, or duplicate/release
a final root. A winner publishes projections and same-scope binding freshness
in one transaction.

Final-turn retention is a separate per-host acknowledgment policy. An
authoritative owner-authenticated complete final has a durable neutral root
before any connector is available. Queued, leased, deferred, `retry`,
`awaiting_ack`, and `dead_letter` roots are unresolved and never
retention-deleted. A whole current graph enters acknowledged history only when
a completed/superseded lineage has exact canonical range coverage, every
declared host/name/turn/revision part is delivered with a durable delivered
attempt, and no unresolved root, plan, attempt, or unlinked part remains.

The defaults retain proven graphs for 30 days and the newest 4096 per host.
Cleanup preserves one immutable delivered-attempt tombstone per opaque final key
before deleting the graph; replay cannot recreate or repost it. Tombstones are
deduplicated and bounded per host by the same count policy. Missing/malformed
owners, known-incomplete content, and internal automation remain nonretryable
safety holds and are never converted by cleanup or retry.

Command requests use schema v1, while every authoritative command result uses
the exact schema-v2 envelope with `disposition`. Command receipts use one
host-wide `(host_id, request_id)` authority across mutating actions. A required
mutating request ID must match `[A-Za-z0-9._-]{1,128}` exactly. Tendwire never
trims, normalizes, or case-folds it, and an authoritative envelope round-trips
the supplied ID exactly. Canonical identity is computed only after
authoritative selector resolution and contains the resolved public worker
identity plus the exact mutation; raw selector spelling and private binding
data are not authority. A validated mutation dry-run is pure: it needs neither
a backend nor a store, creates no receipt, and does not resolve mutable target
authority.

The durable states are `reserved`, `send_started`, `accepted`, `rejected`, and
`uncertain`. An active `reserved` lease protects its pre-send owner; after lease
expiry the same canonical mutation may reclaim it. `send_started` is durable
evidence that an external effect may have begun, so replay never automatically
retries it. Accepted/rejected work replays the stored result. Different request
IDs remain independent sends even when instruction content matches.

Disposition is the finality authority, never status alone: `no_receipt` asserts
no terminal receipt authority, `in_progress` projects `reserved` or
`send_started`, `terminal_accepted` projects `accepted`, `terminal_rejected`
projects `rejected`, and `terminal_uncertain` projects `uncertain`.
`backend_unavailable/no_receipt` is therefore nonterminal at the receipt layer,
whereas `backend_unavailable/terminal_rejected` is a persisted pre-send terminal
rejection. For the CLI, an exact schema-v2 envelope exits `0` for `ok=true` and
`1` for `ok=false`. Exit `2` emits no stdout envelope: the process could not
prove whether a mutating daemon request started and must not forge one.

By default, a `send_started` receipt older than the 604800-second retry horizon
becomes `uncertain`; `reserved` is not converted merely because it is old.
The bounded deletion pool contains only expired pre-send `reserved` rows and
terminal `accepted`, `rejected`, or `uncertain` rows. A row in that pool is
eligible only when it is both older than 2592000 seconds and ranked beyond the
newest 4096 bounded rows for its host. Active owner leases and `send_started`
rows remain protected from deletion.
The unchanged 2592000-second default is greater than Herdres's maximum
604800-second connector retry horizon.

The daemon checks a persisted database-wide cadence after a stored snapshot.
By default, it removes at most 100 snapshot rows, shares a 100-graph final
budget across hosts, and processes a separate batch of at most 100 stale
`send_started` transitions or bounded inactive receipt deletions once per 3600
seconds.
Persisted per-host service cursors choose never-serviced then
least-recently-serviced final hosts, so one busy host cannot starve another.
Later batches resume backlog; automatic maintenance never runs `VACUUM` or
promises immediate enforcement.

Override defaults with `TENDWIRE_ACKNOWLEDGED_FINAL_RETENTION_DAYS`,
`TENDWIRE_ACKNOWLEDGED_FINAL_RETENTION_COUNT`,
`TENDWIRE_COMMAND_RETRY_HORIZON_SECONDS`,
`TENDWIRE_COMMAND_RECEIPT_RETENTION_SECONDS`,
`TENDWIRE_COMMAND_RECEIPT_RETENTION_COUNT`,
`TENDWIRE_SNAPSHOT_RETENTION_DAYS`, `TENDWIRE_SNAPSHOT_RETENTION_COUNT`,
`TENDWIRE_SNAPSHOT_MAINTENANCE_BATCH_SIZE`, and
`TENDWIRE_STORE_MAINTENANCE_CADENCE_SECONDS`. Values are positive integers:
the command retry horizon is at most 604800 seconds; command-receipt retention
is at least 691200 seconds, strictly greater than that horizon, and at most
31536000000 seconds; day policies are at most 365000; counts are at most
9223372036854775807; the maintenance batch size is at most 1000; and cadence is
at most 31536000000 seconds. Invalid configured values fail closed; an affected
cleanup class rejects an invalid per-invocation policy rather than applying
that class.

Use the JSON-only online hooks for aggregate inspection and bounded cleanup:

```bash
tendwire store status --db-path /path/to/tendwire.db
tendwire store cleanup --dry-run --db-path /path/to/tendwire.db
tendwire store cleanup --retention-days 14 --max-outbox-attempts 10 \
  --acknowledged-final-retention-days 30 \
  --acknowledged-final-retention-count 4096 \
  --snapshot-retention-days 14 --snapshot-retention-count 4096 \
  --snapshot-batch-size 100 --db-path /path/to/tendwire.db
```

Inspect exhausted roots and failed plans in a bounded public-safe view, then
retry only the selected opaque final identity:

```bash
tendwire connector inspect --name turn-final --status dead_letter \
  --limit 100 --db-path /path/to/tendwire.db
tendwire connector retry --name turn-final \
  --final-identity 'twfinal1.<opaque>' --db-path /path/to/tendwire.db
```

Copy the exact opaque `final_identity` from the inspect result only after
confirming that one final should be retried. Root items expose only status,
timestamps, cumulative attempt count, sanitized final descriptor, and an opaque
key only when it validates. Failed-plan items add opaque plan/final identities,
public turn/revision, generation, failed-job count, and cumulative attempt
count, and remain visible when the original source link is absent. Inspection
reserves room for one failed plan even when migration holds fill the limit.

Retry revalidates the current complete owner and schema-v2 route. A unique
failed plan uses deterministic recovery that retains the contiguous ACKed
prefix and creates a fresh suffix; an eligible exhausted root receives a fresh
budget while preserving cumulative attempts. Missing/malformed-owner,
known-incomplete, internal-automation, stale, ambiguous, and resolved cases
fail closed. Retry is never bulk and never justifies manual SQLite edits.

`store status` reports the database-wide cadence timestamp, but its snapshot
count/backlog and public-safe final-retention and command-request pressure are
scoped to the requested host. Final retention includes acknowledged,
unresolved/per-status counts, policy, eligibility, and `storage_pressure`;
command requests include only state/candidate counts, retry/retention policy,
and pressure. Daemon health validates the exact host, configured policy,
nonnegative/component totals, eligibility, and pressure relationships;
malformed/wrong-host data and valid pressure degrade health without exposing an
identity.

`cleanup` reports aggregate database-wide snapshots plus host-scoped outbox,
final-retention, command-request, and turn-content results. Command-request
output contains only policy, state/candidate counts, and storage pressure; it
does not expose request IDs, actions, canonical payloads, instructions, workers,
pending choices, or private bindings. Cleanup flags override policy only for
that invocation and do not rewrite configuration. A dry-run rolls back every
maintenance transaction and changes neither rows nor maintenance/service
cursors.

Page reclamation is a separate, explicit `store compact` CLI operation. It is
not exposed through the daemon API and must be run only in a controlled offline
window with all Tendwire, connector, and other SQLite writers stopped. The
dry-run mode is strictly read-only: it does not initialize or migrate a store,
repair permissions, checkpoint WAL, create a backup, prune rows, build a
replacement, update a marker or timestamp, or change the database family. It
requires current schema v14 and rejects both `--acknowledge-offline` and
`--backup-path`.

Follow this order exactly:

1. **Stop consumers.** Stop Herdres and every other Tendwire consumer first.
   Confirm they cannot reconnect or submit connector work.
2. **Stop Tendwire.** Stop `tendwired.service` and every one-shot or alternate
   writer. Confirm all database writers are stopped.
3. **Dry-run.** From the same installed release or source checkout that owns
   the current v12 store, run:

   ```bash
   tendwire store compact --dry-run \
     --snapshot-retention-days 14 --snapshot-retention-count 4096 \
     --batch-size 100 --db-path /path/to/tendwire.db
   ```

   Continue only when the JSON result has `ok: true`, status `dry_run`,
   `integrity.before: ok`, compliant permissions, and `space.headroom_ok:
   true`. The estimate reserves space for the current SQLite family, verified
   backup, and replacement; when the backup is on another filesystem, that
   destination also needs sufficient space.
4. **Execute.** Choose an access-restricted backup directory owned by the
   service account. The backup file itself must not already exist. Then run:

   ```bash
   tendwire store compact --execute --acknowledge-offline \
     --backup-path /secure/backup/tendwire.pre-compact.db \
     --snapshot-retention-days 14 --snapshot-retention-count 4096 \
     --batch-size 100 --db-path /path/to/tendwire.db
   ```

   Execute rechecks current schema, permissions, `PRAGMA quick_check`,
   headroom, and an exclusive writer lock. It creates a mode-restricted backup
   through the SQLite backup API and verifies that backup, drains eligible
   snapshot history in bounded batches, performs a truncating WAL checkpoint,
   builds an adjacent private `VACUUM INTO` replacement, checks the replacement
   with `quick_check` and `foreign_key_check`, and atomically publishes it only
   if the source identity is unchanged. It then restores normal WAL
   configuration and verifies integrity and private permissions after
   publication.
5. **Verify.** Require status `completed`, `integrity.backup: ok`,
   `integrity.replacement: ok`, `integrity.after: ok`,
   `checkpoint.status: completed`, `replacement.status: published`, and
   `rollback.status: not_needed`. While writers remain stopped, run the
   read-only verification below and confirm store status is `ok` with expected
   aggregate counts:

   ```bash
   tendwire store status --db-path /path/to/tendwire.db
   python3 - <<'PY'
   import sqlite3
   from pathlib import Path
   db = Path("/path/to/tendwire.db")
   with sqlite3.connect(f"{db.as_uri()}?mode=ro", uri=True) as conn:
       print(conn.execute("PRAGMA quick_check").fetchone()[0])
       print(conn.execute("PRAGMA foreign_key_check").fetchone())
   PY
   ```

   Expected lines are `ok` and `None`. Keep the verified compaction backup:
   Tendwire never deletes or rotates it. Retain it through the full
   verification and rollback window, then remove it only under the site's
   access-controlled backup-retention policy.
6. **Restart.** Restart Tendwire first and verify `health.get`/`store status`;
   only then restart Herdres and other consumers. Verify one authoritative
   reconcile and normal connector flow before closing the maintenance window.

Compaction reports only aggregate counts, byte estimates, check outcomes, and
fixed statuses. Besides `dry_run` and `completed`, defined statuses are
`invalid_request`, `store_unavailable`, `schema_not_current`,
`permissions_failed`, `offline_required`, `integrity_failed`,
`insufficient_space`, `backup_failed`, `maintenance_failed`,
`checkpoint_failed`, `replacement_failed`, `rollback_completed`, and
`rollback_failed`. A failure before publication leaves the source in place. A
failure after publication attempts restoration from the verified backup and
returns `rollback_completed` only after the restored store passes its checks.
For `rollback_failed`, or for an operator-directed state rollback, keep every
writer and consumer stopped, preserve the failed database family for diagnosis,
and restore the complete coherent pre-maintenance checkpoint described above.
Never restore or replace the database while any writer is running, mix
checkpoints, or treat an acknowledgement flag as proof that the store was
offline.

## Compatible Tendwire/Herdres Pair and Copy-First Dry Check

Goal 05B through Goal 11 are a paired producer/consumer contract. Install or
upgrade Tendwire with a Herdres revision that explicitly supports all of the
following together:

- Tendwire SQLite store schema v14, including transactional migration of v8
  turns, v9 pending rows, typed final-root columns/indexes, the private per-host
  fair-maintenance cursor table, the v11-to-v12 command-receipt rebuild, the
  v12-to-v13 selector-proof addition, and v13-to-v14 turn-list coordinate
  repair. A
  partial legacy final-table set, invalid recovery edge, descriptor/route
  failure, or later migration error rolls back every schema/root/cursor change
  for that step. Ambiguous legacy action-scoped rows for one host/request ID
  migrate to terminal uncertainty rather than selecting one as authoritative;
- owner-aware canonical turn identity, atomic monotonic snapshot plus
  same-scope binding freshness, and schema-v2 root routes containing the exact
  public stable-key pair captured from persisted turn metadata;
- conservative v10-to-v11 classification: delivery requires exact canonical
  range and host-bound all-part proof; a linkable unresolved plan also requires
  every job's schema-v2 route to match authoritative
  turn/revision/final-identity/stable-key values. Unknown/mismatched work,
  missing owners, known-incomplete finals, and internal automation become
  nonpollable `final_migration_hold`/`dead_letter` rather than a mass repost.
  Missing-owner/automation safety holds are permanently nonretryable;
- host-wide command request authority keyed by `(host_id, request_id)`,
  canonicalized only after resolution to the public worker identity. Explicit
  `reserved`/`send_started`/`accepted`/`rejected`/`uncertain` states prevent
  uncertain replay, while different request IDs always remain distinct sends.
  Receipt maintenance keeps active leases, changes only stale `send_started`
  rows to uncertainty, and bounds expired reservations plus
  accepted/rejected/uncertain history outside both its age and count floors;
- `turn.list` schema v2 with descriptor schema v1, 1,000-character previews and
  insertion-stable paging; schema-v1 `turn.content.get` with a 49,152-byte
  UTF-8 page ceiling;
- range-only schema-v1 `connector.prepare` begin/part/commit/recover, per-worker
  ordered roots and parts, bounded renewable/releasable leases, deadline-based
  awaiting-ACK recovery, independent part ACKs, and immutable schema-v2
  source-less route lineage. Cleanup retains bounded delivered tombstones so
  repeated snapshots cannot recreate/repost removed acknowledged roots; and
- fair dead-letter inspection, exact root/failed-plan retry including
  source-less recovery, retained ACK prefix, cumulative attempts, request-ID
  idempotency, immutable recovery audit, and no provider-perfect exactly-once
  claim.

Do not upgrade only one side and infer compatibility from short inline turns.
An older schema-v1-only consumer receives `upgrade_required` as soon as a long
or known-incomplete field makes the legacy projection unsafe. A compatible
Herdres consumer requests turn-list schema v2, accepts descriptor schema v1,
retrieves only complete non-inline fields, isolates known-incomplete turns, and
uses Tendwire's neutral range plans without making direct Herdr calls.

Before the first candidate reconciliation against existing state, preserve the
coherent stopped-writer checkpoint described above. Copy both state files of
interest again for the dry check, and point the candidates only at those
scratch files. Never point the first check at live state:

```sh
# Run from the compatible Herdres checkout after all writers are confirmed stopped.
tendwire_db="${TENDWIRE_DB_PATH:-$HOME/.local/share/tendwire/tendwire.db}"
herdres_state="${HERDR_TELEGRAM_TOPICS_STATE:-$HOME/.local/share/herdres/state.json}"
scratch_db="${tendwire_db}.pre-goal05b-check"
scratch_state="${herdres_state}.pre-goal05b-check"
cp -p -- "$tendwire_db" "$scratch_db"
cp -p -- "$herdres_state" "$scratch_state"
TENDWIRE_DB_PATH="$scratch_db" \
  HERDR_TELEGRAM_TOPICS_STATE="$scratch_state" \
  HERDRES_TENDWIRE_MODE=source \
  ./herdres.py tendwire source-smoke --with-outbox
```

Keep both the untouched paired checkpoint and the scratch files private. The
dry result must succeed with turn-list schema version `2`, content descriptor
schema version `1`, and `direct_herdr_calls=0`. It must not save the copied
Herdres state or send/edit provider messages. If it fails, leave live state
untouched, retain the checkpoint, and investigate; do not edit state, copy
public handles, delete individual identity files, rotate continuity identity,
or recover a failed presentation plan speculatively. These checks establish
pair compatibility and rollback readiness only; they do not deploy, restart,
or migrate live state.

## Verification

From a source checkout, the focused Goal 06 verification and synthetic
benchmark commands are:

```bash
PYTHONPATH=src python3 -m tendwire.cli store cleanup --help
PYTHONPATH=src python3 -m tendwire.cli store compact --help
PYTHONPATH=src python3 -m pytest -q \
  tests/test_config.py tests/test_cli.py tests/test_daemon.py \
  tests/test_store.py tests/test_release_readiness.py \
  -k 'snapshot_maintenance or adjacent_identical or snapshot_retention or maintenance_paths_never_issue_vacuum or store_status_require_current_schema or store_compact or compact_store or migration_registry_transition or maintenance_release_surfaces'
PYTHONPATH=src python3 scripts/store_benchmark.py --snapshot-rows 50000 --json
```

The benchmark is source-checkout-only, uses the Python standard library and
generated synthetic fixtures, and must never be pointed at or populated from a
live database. Record only its aggregate public-safe output. Its timings are
release evidence for bounded behavior, not CI thresholds or pass/fail gates.

The focused Goal 07 ingestion, transport, paging, and privacy verification is:

```bash
PYTHONPATH=src python3 -m pytest -q \
  tests/test_turn_ingestion.py tests/test_herdr_events.py tests/test_daemon.py \
  tests/test_cli.py tests/test_cli_command.py tests/test_store.py \
  tests/test_herdr_turns.py tests/test_turns.py tests/test_config.py \
  tests/test_public_content_safety.py
```

Run the complete suite separately:

```bash
PYTHONPATH=src python3 -m pytest -q
```

Run the frozen Goal 07 benchmark command exactly:

```bash
PYTHONPATH=src python3 scripts/turn_ingestion_benchmark.py \
  --workers 8 --blocked-workers 2 --blocked-seconds 5 \
  --warmups 3 --samples 21 --json
```

The benchmark uses generated private fixtures, real Unix-socket requests, and
deterministically blocked adapters; never point it at live state. Record its
aggregate public-safe host result in
`docs/evidence/goal07-turn-ingestion-benchmark.md`. The evidence budgets are
cached `turn.list` and `health.get` p95 no greater than 350 ms and immediate
synthetic `command.submit` p95 no greater than 250 ms. They are recorded-host
release evidence, not flaky unit-test or ordinary-CI timing thresholds and not
a generic SLA, scaling guarantee, or statistical service-level claim. Do not
claim observed timings unless the completed evidence records them.

The focused Goal 08 Codex reader, ingestion, and public-privacy verification is:

```bash
PYTHONPATH=src python3 -m pytest -q \
  tests/test_codex_session_reader.py tests/test_herdr_turns.py \
  tests/test_turn_ingestion.py tests/test_public_content_safety.py
```

After it passes, run the complete-suite command above. Then run the exact
synthetic Goal 08 benchmark:

```bash
PYTHONPATH=src python3 scripts/codex_session_reader_benchmark.py --json
```

The benchmark must use only its generated private fixture, never live Codex
state. Its stable gates are exact rejection before filesystem work; one
complete 20,000-file index build visiting no more than 100,000 entries and
retaining no more than 8 MiB; no extra build for the warm lookup; no more than
64 KiB read for the benchmark's cold resynchronization; exactly append-sized
incremental work; and zero source bytes for an unchanged poll. Its recursive
privacy gate must reject generated paths, session/turn identities, content,
filenames, and UUID-shaped strings from the success report. Record only the
compact aggregate output in
`docs/evidence/goal08-codex-session-reader-benchmark.md`.

The 60-second complete-index refresh interval is a deliberate bounded-work
tradeoff: a duplicate added after a successful lookup can remain undiscovered
until that refresh, after which the identity is ambiguous and unavailable.
Documented-host timing ceilings are release evidence, not ordinary-CI gates or
a generic service-level claim.

```bash
tendwire doctor --json
tendwire snapshot --json --store --db-path ~/.local/share/tendwire/tendwire.db
tendwire store status --db-path ~/.local/share/tendwire/tendwire.db
python3 - <<'PY'
import sqlite3
from pathlib import Path
db = Path.home() / ".local/share/tendwire/tendwire.db"
uri = f"{db.as_uri()}?mode=ro"  # Refuse a missing database instead of creating it.
with sqlite3.connect(uri, uri=True) as conn:
    print(conn.execute("PRAGMA integrity_check").fetchone()[0])
PY
```

Healthy release-candidate output has healthy backend health, an `ok` store
status, and SQLite integrity `ok`.

## RC Checklist

Before tagging a Tendwire/Herdres source-mode pair:

```bash
# Tendwire source checkout
git status -sb
python3 -m py_compile $(git ls-files '*.py')
python3 -m pytest -q
tendwire doctor --json
tendwire snapshot --json --store --db-path ~/.local/share/tendwire/tendwire.db
tendwire turns --schema-version 2 --json
tendwire pending --json
tendwire attention --json
tendwire store status --db-path ~/.local/share/tendwire/tendwire.db

# Local store integrity
python3 - <<'PY'
import sqlite3
from pathlib import Path
db = Path.home() / ".local/share/tendwire/tendwire.db"
uri = f"{db.as_uri()}?mode=ro"  # Refuse a missing database instead of creating it.
with sqlite3.connect(uri, uri=True) as conn:
    print(conn.execute("PRAGMA integrity_check").fetchone()[0])
PY
```

Pair this with the Herdres source-mode RC checklist. Herdres source smoke must
report `direct_herdr_calls=0`, two forced source syncs must not repost completed
turn text, and `herdr-server.service` should be checked with status-only
commands unless the operator explicitly asks for an external restart.

## Rollback

Tendwire and source-only Herdres must be rolled back as a compatible code pair,
by selecting reviewed matching branches or release tags and reinstalling them;
an `off` or `enrich` environment toggle is not a substitute for pair
compatibility or state recovery. Stop Herdres consumers before Tendwire when an
operator performs the coordinated rollback, and do not allow either revision
to write live state until its supported schemas and recovery actions match.

For a state rollback, stop Herdres consumers and Tendwire, then restore the
complete untouched pre-upgrade checkpoint: the Tendwire database, all three
Tendwire identity artifacts, and the matching Herdres state. Do not reverse a
SQLite migration in place or mix files from different checkpoints. Validate
Tendwire's store integrity and exact version-1 continuity before restarting
Herdres; if validation fails, leave consumers stopped and retain the checkpoint.
