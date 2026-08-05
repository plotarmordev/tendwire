# Wave 4 T6 store design

Status: design candidate. Production implementation is blocked until this note
passes adversarial review and the paired Herdres connector contract and tests
accept the same payload versions, recovery rules, and provider-binding model.

Baseline: Tendwire `9ab5597b55bb918ac8e62c86651ccac7c03d18fb` and
Herdres `ec0e36a25b16469979402abfc8bfa1525db6f68a`.

## Non-negotiable connector boundary

T6 keeps the authoritative connector boundary already in production:

- The connector name is exactly `turn-final`.
- The outer `connector.poll` item `key` is the sole durable replay and
  provider-message binding identity. It is stable across lease expiry, reclaim,
  release, defer, retry, daemon restart, and lost ACK responses.
- `ref` is an attempt-scoped `twref1.` capability. Only the newest live ref may
  settle or renew an attempt.
- `attempt` starts at one and increases within a retry generation. Explicit
  dead-letter retry starts a new generation at attempt one without changing the
  outer key.
- The socket methods remain exactly `connector.prepare`, `connector.poll`,
  `connector.ack`, `connector.fail`, `connector.defer`, `connector.renew`,
  `connector.release`, `connector.reclaim`, `connector.retry`, and
  `connector.inspect`.
- `connector.prepare` remains valid only for `name: "turn-final"`, request
  `schema_version: 1`, and the existing `begin`, `part`, `commit`, and `recover`
  action shapes. No required or optional request field is added.
- `connector.inspect` and `connector.retry` retain their existing strict request
  field shapes. Contract-v3 inspect covers every retained `turn-final` dead
  letter. Retry keeps the one `key` or `final_identity` selector: final identity
  selects a final root, while key selects an exact retryable root, standalone
  decision, or standalone retire as defined below.

The AF_UNIX framing and RPC document remains connector transport contract v2;
top-level and inner operation envelopes continue to use JSON
`schema_version: 1`. The paired `turn-final` payload and operation-response
document revision is presentation contract v3. As with transport contract v2,
the document revision is not copied into every payload or result. The exact
kind-specific schema versions below identify the paired payload family. This
is not a new socket method, action, framing version, or replay identity.

There is no `telegram-present` connector, alias connector, second presentation
queue, turn-list-driven final send, connector-specific socket method, inner
`job_key`, or copy of the current row's outer key as a parallel self-identity.
Lineage and retire payloads may reference another row's outer key only as an
explicit predecessor/replacement/target correlation; those references never
deduplicate or settle the current row. Herdres must continue to durably bind
the polled outer key to provider result before ACKing. That provider receipt
ledger remains required; the separate Herdres plan/pending-plan/recovery ledger
does not.

## Scope, module budget, and gates

T6 replaces `store/sqlite.py` with one fresh schema and concern-owned modules.
The target is 5,225 canonical SLOC, within the 4,500-5,500 gate.

| Module | Responsibility | Budget | Public API |
| --- | --- | ---: | --- |
| `store/schema.py` | one DDL, exact-version cutover | 400 | `STORE_SCHEMA_VERSION`, `init_store`, `ensure_schema` |
| `store/db.py` | secure open, pragmas, read/write transactions, bounded health | 225 | `connect_read`, `read_transaction`, `write_transaction`, `store_status` |
| `store/events.py` | append, authoritative dedupe, bounded query | 300 | `record_agent_event`, `list_agent_events` |
| `store/turns.py` | atomic projection, content, list, delta, paging | 1,050 | append/apply result types, `append_agent_event_and_apply_turn_for_binding`, `apply_turn_refresh`, `turns_payload_from_store`, `turn_delta_payload_from_store`, `get_turn_content` |
| `store/projection.py` | snapshots, attention, bindings, route generation, health | 475 | snapshot context/save/latest, attention payload, binding upsert/list/expiry, backend-pending health |
| `store/pending.py` | pending observations and fenced decision claims | 425 | observation/payload, claim/start/abandon/terminal-effect functions |
| `store/receipts.py` | command receipts, submissions, replay and linking | 800 | current command reservation/send/finish/recovery/link functions |
| `store/outbox.py` | one queue, FIFO/DAG, leases, prepare, recovery, dead-letter | 1,350 | the current connector store functions only |
| `store/retention.py` | bounded cutoff deletion and WAL checkpoint | 200 | `RetentionPolicy`, `run_retention_cycle` |
| **Total** |  | **5,225** |  |

The module rows still sum exactly 5,225; this correction adds no module or
second implementation path. Caller/configuration rewrites listed in file scope
replace existing lines and are measured in the repository-wide gate, not
double-counted as store-component SLOC.

No function may exceed 150 lines. Orchestration functions target at most 130
lines; transaction-local transition/query helpers target at most 60. There is
no compatibility re-export, generic repository/CRUD layer, migration chain,
authority registry, direct-SQLite CLI fallback, maintenance state machine,
automatic VACUUM, or compaction framework.

## Fresh schema

The application schema has 16 tables:

- Projection: `turns`, `turn_content_revisions`,
  `turn_content_page_boundaries`, `attention_items`, `pending_interactions`,
  `snapshots`, and `agent_events`.
- Commands: `command_receipts`, `turn_submissions`, `turn_supersessions`,
  `backend_pending`, `backend_pending_claims`, `worker_bindings`, and
  `backend_health`.
- Delivery: `connector_outbox` and `connector_deliveries`.

Explicitly absent are `events`, `spaces`, `workers`, `commands`, migration
tables, store-maintenance state/cursors, presentation plan/job/recovery tables,
turn-change state/floor tables, tombstone tables, and every `herdr_turn_*`
table. A retained removal row in `turns` is the delta tombstone. The greatest
turn/delta sequence row is retained as its allocation sentinel, and the
retained worker-binding allocator owns partition sequences, so a published
sequence is never reused.

`turns` is keyed by `(host_id, turn_id)` and carries current ownership,
`route_generation`, insertion and change sequence, projection state, and
`removed_at`. There is no prepare-authority or source-less-presentation column.
Every final presentation plan is rooted in one polled `final_ready` outbox row.
`worker_bindings` carries exact columns `stable_key`, `stable_key_version`,
`route_generation`, `partition_key`,
`next_partition_sequence`, and `route_retain_until` alongside its private
backend binding. Content revisions are immutable, with one partial-unique
current row per turn. Page boundaries are revision/field/page-coordinate unique
and cascade only with a revision that retention has proved unreferenced.

Command receipts preserve:

```text
missing -> reserved -> send_started -> accepted | rejected | uncertain
```

Only an identical canonical request may take over an expired reservation;
terminal receipts are immutable. Turn submissions preserve the existing
send-started/submitted/uncertain/link/ambiguous/expired/cancelled semantics,
one-to-one linkage, owner/instruction matching, and fail-closed ambiguity.
Backend-pending claims remain fenced and are abandonable only before send
start.

## Two delivery tables

`connector_outbox` contains both immediately executable work and staged
`final_part` rows. Its logical columns are:

```text
id, host_id, connector, key, kind, payload_version, status,
partition_key, partition_sequence,
turn_id, final_identity, content_revision, presentation_version,
plan_token, plan_generation, logical_sequence, logical_ordinal,
predecessor_outbox_id, replaces_outbox_id, target_outbox_id,
source_outbox_id, active_lineage_generation,
recovery_request_digest, recovered_from_plan_token,
terminal_after_lease, retry_generation, prior_attempt_count,
current_delivery_id,
payload_json,
created_at, updated_at, available_at
```

Required uniqueness is:

```text
(host_id, connector, key)
(host_id, connector, partition_key, partition_sequence)
(host_id, connector, source_outbox_id, plan_generation, logical_sequence)
(host_id, connector, recovery_request_digest) WHERE recovery_request_digest IS NOT NULL
```

`status` has an exact database CHECK over `staged`, `blocked`, `queued`,
`leased`, `retry`, `deferred`, `awaiting_ack`, `delivered`, `superseded`, and
`dead_letter`. `kind` is CHECKed over `generic`, `working`, `final_ready`,
`final_part`, `retire`, and `decision`; `connector = 'turn-final'` requires one
of the five typed kinds, and every other connector requires `generic`. A
kind/status CHECK permits only the combinations in the matrix below, and a
kind/payload-version CHECK fixes working/retire/decision at one, final-part at
two, and final-ready at three. `predecessor_outbox_id`, `replaces_outbox_id`, `target_outbox_id`, and
`source_outbox_id` are nullable self-foreign keys with `ON DELETE RESTRICT`.
`current_delivery_id` is a nullable foreign key to `connector_deliveries(id)`
with `ON DELETE RESTRICT`; transition code additionally proves that the pointed
delivery's `outbox_id` is this row. Terminal outbox rows have a null current
delivery. Leased and awaiting-ACK rows have exactly one.

`connector_deliveries` contains attempts only:

```text
id, outbox_id, retry_generation, attempt,
ref_hash, status, leased_at, leased_until, ack_deadline_at,
public_response_json, private_reason_enum,
created_at, settled_at
```

`outbox_id` is non-null and references `connector_outbox(id) ON DELETE
RESTRICT`. Delivery `status` has an exact CHECK over `leased`, `awaiting_ack`,
`acknowledged`, `failed`, `deferred`, `released`, and `expired`. Required
uniqueness is `(outbox_id, retry_generation, attempt)` and `(ref_hash)`, plus a
partial unique live-attempt index on `outbox_id` where status is `leased` or
`awaiting_ack`. Raw refs are never stored. `public_response_json` is a bounded,
exact sanitized object; `private_reason_enum` is nullable and CHECKed against a
fixed enum: `temporary`, `rate_limited`, `provider_rejected`,
`provider_uncertain`, `invalid_payload`, `content_unavailable`,
`route_unavailable`, `provider_binding_unknown`, `lease_expired`,
`ack_deadline_expired`, `superseded`, `attempts_exhausted`, or
`operator_recovery`. There is no
`private_state_json` in either delivery table.

`ack_deadline_at` is a canonical UTC timestamp or null. It is non-null exactly
when delivery status is `awaiting_ack`; it is null for ordinary leases and all
terminal attempts. The source-commit and recovery rules below are its only
initializers or mutators.

Database constraints provide identity and deletion fencing; transaction code
provides the cross-row domain checks SQLite CHECKs cannot express. A staged or
executable `final_part` and every plan retire must reference exactly one
`final_ready` source of the same host, connector, turn, final identity,
revision, partition, and active lineage generation. Its predecessor,
replacement, and target must
have the same host/connector and the declared compatible route/revision. A
predecessor must have a lower logical sequence unless it is the delivered tail
of an inherited recovery prefix. Cycles, cross-root children, a root pointing
at its child as current delivery, or a delivery pointing at a different outbox
row abort the transaction. Root completion, supersession, retry, and recovery
update the root and every affected child in the same `BEGIN IMMEDIATE`.

The exact nullability/correlation rules are also enforced on every write:

- `working` and `decision` have no source, plan, logical-ordinal, target, or
  recovery columns; only `replaces_outbox_id` may be set.
- `final_ready` has final identity/revision but no source, plan, predecessor,
  target, or recovery parent. Its current delivery exists only while leased or
  awaiting ACK.
- `final_part` has non-null plan token/generation/sequence/ordinal and a
  non-null final-ready source. Its target is null; predecessor/replacement must
  satisfy the declared lineage.
- A plan `retire` has plan coordinates, non-null target and predecessor, and the
  same non-null final-ready source as its lineage. A standalone retire has no
  plan/source columns, has a non-null decision target, and uses only the
  decision-resolution predecessor rule.
- Only recovery-created executable rows have `recovered_from_plan_token`.
  Exactly the first fresh row of a recovery generation carries the non-null
  `recovery_request_digest`; every other row has null, so the declared unique
  index is executable and that head is retained for the full lineage horizon.

The outer key grammars are exact:

```text
final_ready: turn-final:revision:twfinal1.<43 base64url characters>
final_part or plan retire: turn-final:twplan1.<1-256 base64url characters>:<six decimal digits>
working: turn-final:working:twwork1.<43 base64url characters>
decision: turn-final:decision:twdecision1.<43 base64url characters>
standalone retire: turn-final:retire:twretire1.<43 base64url characters>
```

Contract-v3 Tendwire final identities are always 32-byte digests encoded as 43
unpadded base64url characters. A contract-v3 `final_ready` payload, outer key,
prepare source root, inspect selector, and retry selector reject every other
length. Existing non-v3 RPC positions that accept opaque `twfinal1.*` or
`twplan1.*` values may retain their transport-v2 bounded 1--256-character
validator, but that compatibility does not widen the five-kind contract.

Producer identities are deterministic canonical SHA-256 digests, not random
per enqueue. `twwork1` hashes the domain, host, turn ID, content revision, and
route generation. `twdecision1` hashes the domain, host, decision ref, revision
digest, and route generation. `twretire1` hashes the domain, target decision
outer key, the fixed `decision_resolved` reason, resolving revision, and route
generation. There is no route-retirement producer. Replaying the same producer
transaction therefore finds the same outer key and must prove an identical
payload; a mismatch is a conflict. Plan tokens are the one persisted opaque
value allocated by idempotent begin, and plan child keys are deterministic from
that token and logical sequence.

These tokens occur only as parts of the outer key or as non-replay correlation
coordinates declared below. No payload has `key`, `job_key`, `delivery_key`,
`ref`, or `attempt`.

## Outbox status matrix

| Kind | Legal statuses | Legal atomic transitions |
| --- | --- | --- |
| `generic` (non-`turn-final` queues only) | `queued`, `leased`, `retry`, `deferred`, `delivered`, `superseded`, `dead_letter` | preserves the existing neutral enqueue/poll/ACK/fail/defer/release/reclaim transitions; it cannot participate in prepare, route partitions, final lineage, or the five-kind payload contract |
| `working` | `queued`, `leased`, `retry`, `deferred`, `delivered`, `superseded`, `dead_letter` | enqueue to `queued`; due poll to `leased`; ACK to `delivered`; fail to `retry` or `dead_letter`; defer to `deferred`; release/expiry to `queued`; unleased supersession to `superseded`; leased supersession sets `terminal_after_lease`, after which ACK is `delivered` and fail/defer/release/expiry is `superseded` |
| `final_ready` | `queued`, `leased`, `retry`, `deferred`, `awaiting_ack`, `delivered`, `superseded`, `dead_letter` | ordinary lease transitions; first successful commit `leased` to `awaiting_ack`; complete effective lineage to `delivered`; ACK-deadline expiry to `dead_letter`; only an uncommitted root dead letter may retry to `queued` in a new retry generation; recovery replaces a failed suffix while the committed root remains live `awaiting_ack`; an uncommitted superseded root becomes `superseded`; a committed terminal root is never re-prepared from scratch |
| `final_part` | `staged`, `blocked`, `queued`, `leased`, `retry`, `deferred`, `delivered`, `superseded`, `dead_letter` | begin creates `staged` placeholders; part fills one staged row idempotently; commit freezes payloads and moves the head to `queued` and successors to `blocked`; predecessor ACK unblocks exactly one successor; ordinary lease transitions; recovery retains a delivered prefix, supersedes the failed suffix, and creates a new suffix |
| `retire` | `blocked`, `queued`, `leased`, `retry`, `deferred`, `delivered`, `superseded`, `dead_letter` | commit/resolution creates `blocked`; a delivered replacement normally moves it to `queued`, with the exact leased-decision exception below; ordinary lease transitions; supersession is legal only when Tendwire proves the target never had a delivery attempt and therefore could not have been provider-accepted; a mandatory retire for a leased, expired, failed, deferred, acknowledged, or otherwise possibly accepted target remains retryable/dead-letter work and is never superseded |
| `decision` | `queued`, `leased`, `retry`, `deferred`, `delivered`, `superseded`, `dead_letter` | pending projection/enqueue to `queued`; ordinary lease transitions; resolution atomically supersedes unleased work or marks leased work terminal-after-lease and creates the ordered retire |

`staged`, retained `awaiting_ack` roots, and terminal rows do not block FIFO.
An executable row is pollable only when it is due, its explicit predecessor is
`delivered`, and no earlier executable row in the same partition is in
`queued`, `leased`, `retry`, or `deferred`. Poll reclaims expired leases before
selection in the same `BEGIN IMMEDIATE` transaction.

The one explicit predecessor exception is a mandatory standalone retire for a
resolved decision that was already leased. It becomes eligible after the target
decision has no live attempt and is terminal as `delivered`, `superseded`, or
`dead_letter`, because any prior lease may have reached the provider even when
Tendwire did not receive an ACK. A decision superseded before its first lease
cannot have a provider object; its retire is provably unnecessary and may be
superseded. Replacement-driven working/final retires still require the
replacement key itself to be `delivered`.

For that decision exception the retire payload has `predecessor_key` equal to
the target decision key. Herdres does not require a delivered provider job for
that key: it performs its immutable target lookup. If H7 has no provider job,
alias, or tombstone for a possibly accepted target, it returns the fixed
`provider_binding_unknown` failure. Tendwire immediately dead-letters the
mandatory retire, keeps its target/reason visible through inspect for the
30-day targetable horizon, and never converts absence into ACK, deletion, or
"unnecessary". Explicit retry may make the same target visible again but
cannot reconstruct an unknown provider coordinate.

## Delivery-attempt and CAS matrix

| Operation | Required current state | `connector_deliveries` transition | `connector_outbox` transition |
| --- | --- | --- | --- |
| poll | due eligible work, no blocker | insert `leased` at next attempt | executable work to `leased` |
| renew | newest unexpired ref; both rows `leased` | extend `leased_until` | remains `leased` |
| ACK | newest unexpired ref; both rows `leased` | `leased` to `acknowledged` | work to `delivered`; atomically unblock successor |
| fail, ambiguous new send | newest unexpired ref; exact `provider_uncertain` classification | `leased` to `failed` | immediate `dead_letter` regardless of attempt budget; effective root lineage updated atomically |
| fail, mandatory retire has unknown binding | newest unexpired ref; exact `provider_binding_unknown` classification | `leased` to `failed` | immediate visible `dead_letter`; target is retained and never inferred absent |
| fail, budget remains | newest unexpired ref; both rows `leased` | `leased` to `failed` | work to `retry` |
| fail, exhausted | newest unexpired ref; both rows `leased` | `leased` to `failed` | work/effective lineage to `dead_letter` |
| defer | newest unexpired ref; both rows `leased` | `leased` to `deferred` | work to `deferred` |
| release | newest unexpired ref; both rows `leased` | `leased` to `released` | work to `queued`, or `superseded` when terminal-after-lease |
| lease expiry | current `leased`, deadline due | `leased` to `expired` | work to `queued`, or `superseded` when terminal-after-lease |
| source commit | live final-ready source ref | `leased` to `awaiting_ack`; initialize `ack_deadline_at` once | root to `awaiting_ack`; head queued and tail blocked |
| lineage complete | every effective node delivered before the exact ACK deadline | `awaiting_ack` to `acknowledged`; clear deadline | root to `delivered` |
| lineage deadline | no live child lease; exact ACK deadline reached | `awaiting_ack` to `failed`; clear deadline | root and effective tail to `dead_letter` |
| explicit root retry | exact retryable root dead letter | prior generation remains terminal/aggregated | same root key to `queued`, retry generation +1; next attempt is one |
| recover | exact recoverable failed lineage under a live awaiting-ACK root | same attempt remains `awaiting_ack`; one new request digest replaces its deadline | prefix retained; failed non-retire tail superseded; possibly accepted mandatory retire audit rows retained dead-letter and linked to their fresh copies; fresh suffix created |

Every ref mutation uses one CAS equivalent to:

```text
outbox.id = delivery.outbox_id
AND outbox.host_id = requested host
AND outbox.connector = requested connector
AND outbox.status = expected outbox status
AND delivery.id = outbox.current_delivery_id
AND delivery.status = expected delivery status
AND delivery.ref_hash = H(presented ref)
AND delivery.leased_until > now
```

Zero changed rows returns the existing stale/invalid-ref result and commits no
partial transition. An old ref cannot mutate a later attempt. An explicit
retry never revives an old ref or attempt counter.

The retained configuration is exactly
`TENDWIRE_CONNECTOR_ACK_TTL_SECONDS`/`connector_ack_ttl_seconds`, default 60.
Startup accepts only a non-Boolean integer in `1..86400`; it never clamps. The
first successful source commit computes `ack_deadline_at = commit_now + ttl`
inside the commit transaction. A byte-equivalent commit replay returns the
persisted deadline and never extends it. Lineage completion CASes the exact
root/current-delivery pair only while `ack_deadline_at > now`; deadline expiry
CASes that same pair only when `ack_deadline_at <= now` and no child has a live
lease. Exactly one successful recover request digest may replace the still-live
awaiting-ACK deadline with `recover_now + ttl`; an identical request replay
returns the persisted replacement deadline, and a different request must
create the next recovery generation or fail conflict. Recover after deadline
expiry is rejected. The committed expired root is terminal and
`not_retryable`; only a later producer revision may create unrelated new work.
Retention treats every non-null deadline and its effective lineage
as live, and cannot delete either until the root is terminal and the applicable
reference horizon has elapsed.

Inspect request fields remain exactly `schema_version: 1`,
`name: "turn-final"`, `status: "dead_letter"`, and integer `limit` in `1..100`.
A successful inner result has exactly:

```text
schema_version, ok, status, host_id, name, total, items
```

Its values are `schema_version=1`, `ok=true`, `status="ok"`, and `total` is the
full matching root/effective-lineage count before the limit. Every item has all
of these exact fields, with inapplicable correlations present as null:

```text
kind, key, final_identity, failed_plan_token, decision_ref, target_key,
reason, attempt_count, prior_attempt_count, created_at, terminal_at, retryable,
recoverable
```

`reason` is one value from the fixed public reason enum corresponding to the
stored private classification. `retryable` is true only when the exact
`connector.retry` selector can currently act; `recoverable` is true only for an
exact failed plan whose source root is still awaiting ACK before its deadline
and can accept `connector.prepare(action="recover")`. Items contain no text,
payload, ref, provider fact/coordinate, private route, or raw diagnostic. Root
and plan-child failures are reported once at the root/effective-lineage level.
Inspect errors have exactly `schema_version`, `ok`, `status`, `host_id`, `name`,
`message`, and `items`, with `ok=false`, `items=[]`, and the common fixed error
rules below.

Retry keeps its exact mutually exclusive selector shapes. `final_identity`
and a final-ready key requeue only an exact uncommitted dead-letter root in a
new retry generation; the root is then polled and prepared with its new live
ref. A committed terminal root is `not_retryable` and is never prepared from
scratch. `connector.retry` never invokes plan recovery. A still-awaiting-ACK
root with a recoverable failed suffix uses only
`connector.prepare(action="recover")`.
A standalone decision key or standalone
retire key starts a new retry generation on that same outer key, with attempt
one and accumulated prior-attempt count. A plan retire/final-part key is not
independently retried because root recovery owns its suffix. Working dead
letters are observationally replaced only by a newer deterministic producer
revision and are not operator-retried. Unknown, terminal-success, stale
revision, nonstandalone child, and ambiguous selectors return fixed
`not_retryable`/`stale_revision`/`invalid_params` outcomes without mutation.

Provider-start classification is exact. A definitely-not-started provider call
uses `temporary` or `rate_limited` and follows the ordinary bounded retry/defer
path. An ambiguous new send for `working`, `final_part`, or `decision` uses
`provider_uncertain`; fail ignores remaining attempt budget and atomically moves
that row/effective lineage to `dead_letter` for inspection. Automatic retry is
forbidden because it can duplicate a message. Ambiguous known-target edit or
delete does not use `provider_uncertain`: Herdres defers the same immutable key
and target, then converges by exact not-modified or not-found handling. Invalid
reason/operation combinations are `invalid_params` without mutation. Explicit
operator retry/recover of a `provider_uncertain` row is allowed only with a
visible warning that the provider may already have accepted the send and the
operation can create a duplicate; recovery preserves the prior ambiguous audit
row and never claims the risk was repaired.

## Exact additive payload contract

The existing poll item envelope is unchanged:

```json
{
  "key": "...",
  "ref": "twref1....",
  "attempt": 1,
  "leased_until": "2026-08-05T00:00:00Z",
  "available_at": "2026-08-05T00:00:00Z",
  "created_at": "2026-08-05T00:00:00Z",
  "payload": {}
}
```

Each payload has exactly these common keys:

```text
schema_version, kind, created_at, worker, route
```

The kind-specific `schema_version` values below version that payload object;
they do not change the transport-v2 envelope's JSON schema version 1.

`worker` is exactly:

```text
worker_id, stable_key, stable_key_version, route_generation
```

`route` is exactly:

```text
partition_key, partition_sequence
```

The kind-specific versions and keys are:

### `working`, schema version 1

Adds only `turn`. `turn` is exactly:

```text
turn_id, content_revision, replaces_key, text
```

`replaces_key` is an outer key or null. `text` is exactly:

```text
assistant_stream_text, char_length, byte_length
```

The text is a bounded sanitized inline projection, and the two lengths must
match its exact Unicode scalar and UTF-8 byte lengths.

### `final_ready`, schema version 3

Adds only `turn`. `turn` is exactly:

```text
turn_id, final_identity, content_revision, replaces_key, content
```

`replaces_key` is an outer working/final key or null. `content` remains exact
content-schema-v1:

```text
schema_version, content_revision, known_incomplete, fields
```

`fields` has exactly `user_text` and `assistant_final_text`; each descriptor is
exactly `availability`, `inline`, `char_length`, `byte_length`, `page_count`,
and `first_cursor`. The final-ready revision, nested revision, cursors, lengths,
and outer-key final identity must correlate exactly.

### `final_part`, schema version 2

Adds exactly `turn`, `plan`, and `lineage`.

```text
turn: turn_id, final_identity, content_revision
plan: plan_token, generation, presentation_version, ordinal, part_count, spans
lineage: recovered_from_plan_token, predecessor_key, replaces_key
```

Nullable lineage values are present as null, not omitted. Each span is exactly
`field`, `start_char`, and `end_char`; field is `user_text` or
`assistant_final_text`. Spans are nonempty, ordered, nonoverlapping exact
character slices of the retained revision. No final text is copied into a
plan row.

### `retire`, schema version 1

Adds exactly `turn` and `retire`.

```text
turn: turn_id, final_identity, content_revision
retire: target_key, target_kind, target_ordinal, predecessor_key,
        plan_token, generation, reason
```

Nullable coordinates are present as null. `target_kind` is `working`,
`final_part`, or `decision`. Reason is exactly one of `working_replaced`,
`final_replaced`, `excess_part`, or `decision_resolved`.
There is no content or provider coordinate.

### `decision`, schema version 1

Adds only `decision`, exactly:

```text
decision_ref, revision_digest, mode, title, body, choices
```

`mode` is `single`, `multi`, or `plan`. Each choice is exactly `ordinal`,
`option_ref`, and `label`. `option_ref` is the public one-based ordinal from the
semantic decision contract; the private ACP option ID and backend route remain
only in `backend_pending`.

Unknown keys, unknown versions/kinds, booleans used as integers, noncanonical
timestamps, malformed opaque tokens, duplicate ordinals, inconsistent lengths,
cross-revision cursors/spans, and correlation mismatch reject enqueue, prepare,
or poll before any connector-visible mutation.

## Route generation and concurrency

`route_generation` has exact grammar:

```text
twroute1.<43 unpadded URL-safe base64 characters>
```

It encodes 32 cryptographically random bytes. In the atomic worker-binding
projection, Tendwire performs lookup-or-mint for this exact private tuple:

```text
(host_id, stable_key_version=1, stable_key, backend,
 binding_private_fingerprint)
```

An identical retained tuple reuses its generation. A changed stable identity,
backend/private binding fingerprint, deliberate installation-key rotation, or
resolved live-route collision mints a new generation. Ordinary daemon/Herdres
restart, worker label change, content update, lease attempt, or local Herdres
JSON revision does not rotate it. Multiple live route tuples claiming the same
stable identity fail closed.

Route enrichment is part of the same snapshot/binding `BEGIN IMMEDIATE` that
accepts the authoritative worker observation. It validates the derived stable
key pair, selects or mints the route generation, derives the partition key,
stores all route columns on `worker_bindings`, and copies the exact public
stable-key/version/generation triplet into the worker and affected turn
projection before commit. No snapshot, turn, delta, pending decision, or
outbox producer may publish a partially enriched route. Direct binding upsert
uses the same helper and cannot invent a generation independently.

`save_snapshot` and the binding transaction return the persisted enriched
snapshot, never the caller's pre-enrichment object. Before persistence and
return, every `Worker.fingerprint` is recomputed from the exact canonical
public Worker projection excluding the fingerprint itself and volatile
timestamps but including `stable_key`, `stable_key_version`, and the non-null
`route_generation`. Snapshot `content_fingerprint` is then recomputed from those
enriched workers. The coordinator replaces its in-memory snapshot with this
returned value before publishing, routing, or accepting a command.

Post-T6 command reservation/send CASes the exact current `worker_id`, enriched
`worker_fingerprint`, stable key/version, route generation, and private binding
fingerprint in the same transaction. A mismatch is stale route and authorizes
no backend send. H8's one-time removal of `worker_fingerprint` from a stored
command is legal only before the paired cutover when the captured route
generation is null. Once a command carries a non-null `twroute1.*`, fingerprint
removal is forbidden; a mismatch quarantines rather than weakening the CAS.

The generation is published in worker metadata, copied into current turn/list/
delta ownership, and frozen in every queued payload. It is not a Telegram
topic generation and is never compared to Herdres's local state revision.

`partition_key` has exact grammar `twpart1_` plus 64 lowercase hexadecimal
characters and is SHA-256 over canonical UTF-8 JSON:

```json
{
  "domain": "tendwire.turn-final.partition.v1",
  "host_id": "...",
  "route_generation": "twroute1....",
  "stable_key": "wsk1_...",
  "stable_key_version": 1
}
```

Different Tendwire partitions may lease concurrently. After resolving a
public route, Herdres must additionally serialize provider writes by its
private physical `(bot, chat, topic)` coordinate. Two route generations mapped
to one retained topic therefore cannot concurrently edit or delete it. Topic
creation and provider routing remain Herdres-owned; Tendwire sees only neutral
defer/fail/release/ACK outcomes.

`worker_bindings.next_partition_sequence` is the durable allocator for its
exact partition. Every producer transaction atomically increments it with
`UPDATE ... RETURNING` and inserts the outbox row with the returned positive
sequence; `MAX(outbox.sequence)+1` is never used. The binding/allocator row is
retained while current/sendable, referenced by any Tendwire turn or outbox row,
or before its Tendwire-owned `route_retain_until`.

The paired constants are exact minimum floors: terminal targetable Tendwire
outbox rows and their inspect/retry correlation are retained for 30 days;
referenced content revisions, page boundaries, worker bindings, route
generations, partition allocators, and installation-key derivation evidence are
retained for 45 days; H7 immutable provider jobs/messages/aliases/tombstones are
retained for 60 days. The corresponding names are
`TURN_FINAL_TARGETABLE_RETENTION_DAYS = 30`,
`TURN_FINAL_ROUTE_CONTENT_RETENTION_DAYS = 45`, and the cross-repository H7
`PROVIDER_FACT_RETENTION_DAYS = 60`. Configuration may lengthen but never
shorten these floors. Live references, awaiting-ACK roots, current/recovered
lineage, active routes, and unresolved mandatory retires override every cutoff.
After references and the applicable horizon are both gone, retention may remove
the rows child-first. Tendwire neither reads nor infers Herdres provider
bindings and never shortens a horizon based on unknown provider state.

## Prepare lease, idempotence, and response-loss recovery

Prepare requests keep transport-v2 `schema_version: 1` and the four existing
action field sets. Contract v3 removes source-less preparation completely.
`begin` requires `source_ref`; the first `commit` requires `source_ref`; omission
or explicit null is `invalid_ref`. `part` remains token-fenced and has no
source-ref field. `recover` is the sole recovery action and accepts only its
unchanged `failed_plan_token` and `request_id` fields. No internal producer,
startup scan, retry path, or rediscovery path may call `begin` without a polled
source root.

Successful action results have the following exact fields and no others:

```text
begin:
  schema_version, ok, status, host_id, name, plan_token, state, generation,
  part_count, accepted_ordinals
part:
  schema_version, ok, status, host_id, name, plan_token, state, generation,
  part_count, ordinal, accepted_ordinals
commit:
  schema_version, ok, status, host_id, name, plan_token, state, generation,
  part_count, job_count, accepted_ordinals
recover:
  schema_version, ok, status, host_id, name, failed_plan_token, plan_token,
  generation, content_revision, state, acknowledged_prefix_count,
  executable_job_count, retained_failed_job_count, prior_attempt_count,
  idempotent_replay
```

`schema_version` is one. `ok` is true. Begin/part/commit `status` is `ok`;
recover status is `recovered`. `accepted_ordinals` is a sorted unique bounded
array of non-Boolean zero-based integers in `[0, part_count)`, with
`part_count` bounded to `1..10000`. Begin returns all already accepted ordinals,
part includes its exact `ordinal`, and commit returns every ordinal
`0..part_count-1`. `generation`, `part_count`, `ordinal`, and every count reject
Boolean values; generation and part count are positive and the other counts are
nonnegative. Begin/part state is exactly `preparing`, `active`,
`waiting_predecessor`, `completed`, `failed`, or `superseded`; commit state
excludes `preparing`, and recover state is exactly `active`. An identical replay
returns the same persisted fields and never recomputes a token or count. The
persisted ACK deadline is an internal lifecycle fence and is not added to the
transport-v2 result. Recover deliberately has no `accepted_ordinals` field
because it creates an executable suffix rather than a preparing upload set.

Every prepare error has exactly `schema_version`, `ok`, `status`, `host_id`,
`name`, and `message`; `schema_version=1`, `ok=false`, and `message` is exactly
the same fixed enum token as `status`. The closed status/message set is
`invalid_params`, `invalid_ref`, `stale_ref`, `store_unavailable`,
`revision_not_found`, `stale_revision`, `content_unavailable`, `plan_not_found`,
`plan_conflict`, `part_conflict`, `plan_incomplete`, `plan_not_failed`,
`not_recoverable`, `request_conflict`, and `ack_deadline_expired`. Errors expose
no token, ordinal, count, deadline, payload, route, provider fact, or private
coordinate.

Generation-one begin identity is exact:

```text
(source final-ready outer key, final_identity, content_revision,
 route_generation, presentation_version, part_count, plan_generation=1)
```

Begin validates the live `source_ref`, inserts one opaque
`twplan1.*`, and creates `part_count` staged `final_part` placeholders in one
transaction. Repeating the same begin returns the same token, plan state, part
count, and sorted exact `accepted_ordinals`. A differing version or count is
`plan_conflict`. A repeated begin may bind the same staged plan to a newly
leased attempt of the same source outer key, never to another root, revision,
or route.

Part retains its current request shape. The first exact span set fills its
ordinal. An identical retry succeeds and returns the same accepted ordinals; a
different set is `part_conflict`. Although part carries no source ref, Tendwire
internally requires the plan to remain bound to the source's current live
attempt. Staged payloads become immutable when commit makes them executable.

The first commit requires the current live `source_ref`, all ordinals, exact
full coverage, and a still-current revision. It atomically freezes children,
creates retire nodes, installs the predecessor DAG, moves source outbox and
attempt from `leased` to `awaiting_ack`, initializes the persisted ACK deadline,
and exposes only the head. A repeated commit after success returns the persisted
committed token/state/generation/part/job counts and accepted ordinals
even when the supplied old source ref is no longer live. If the first commit
did not happen, the plan remains preparing and a current source ref is required.

Herdres renews the source lease immediately after root validation, after every
bounded content-page batch, before every part upload, and immediately before
commit. A failed or uncertain renew stops preparation. The same renew may be
retried while the ref can still be live; otherwise Herdres waits for expiry,
repolls the same outer key, repeats begin, and resumes missing ordinals.

Consequently:

- Lost begin response: repeat begin and rediscover token/accepted ordinals.
- Lost part response: repeat the identical part.
- Crash or source expiry before commit: the source requeues, staged rows remain,
  repoll returns the same outer key with a new ref, and begin rediscovers them.
- Lost commit response: committed child keys become pollable; repeating commit
  also returns the identical committed result.
- Failed lineage recovery: use only `recover` while the committed root remains
  awaiting ACK before its deadline. Source-less `begin` does not exist, and an
  expired committed root is terminal and `not_retryable`.
- Restart requires no Herdres plan token, pending-plan, accepted-ordinal, or
  recovery ledger. Provider receipts keyed by outer key remain authoritative.

## Recovery lineage

Recover accepts its unchanged `failed_plan_token` and `request_id` request.
Tendwire requires one linear logical sequence: a contiguous acknowledged
prefix followed by a failed nonleased suffix, plus the exact source root and
current delivery still in `awaiting_ack` with `ack_deadline_at > now`. A gap,
live child lease, unexpired retry/defer, absent source, or noncontiguous prefix
returns `not_recoverable`; an expired root returns `ack_deadline_expired`. A `provider_uncertain`
dead-letter is recoverable only through explicit operator action acknowledging
the duplicate-send risk; it is never selected by automatic recovery.

In one transaction Tendwire:

1. stores a unique digest of the bounded public request ID;
2. returns the same result for an identical replay;
3. allocates the next generation and a new plan token;
4. retains old delivered prefix rows and their original outer keys unchanged;
5. creates fresh-key copies only for the suffix, including retire nodes;
6. points the first new node to the last old acknowledged key, or leaves its
   executable predecessor null when the prefix is empty;
7. records the failed plan token and replaced old suffix row on each new node;
8. marks failed non-retire suffix rows `superseded`, but keeps every possibly
   accepted mandatory retire as a terminal dead-letter audit row linked to its
   fresh replacement rather than claiming it became unnecessary; and
9. replaces the live root delivery's ACK deadline once with
   `recover_now + connector_ack_ttl_seconds`; and
10. defines root completion as old delivered prefix plus new suffix.

Recovery may chain when a replacement suffix later fails. It never revives an
old ref or attempt and never erases provider-acceptance ambiguity. An explicit
operator recovery can duplicate an operation whose provider acceptance was not
durably bound. The retained old `provider_uncertain` audit item and its attempt
count remain inspectable even when the recovery suffix later succeeds.

The prefix-empty head is ordered by its fresh partition sequence after the old
failed tail has atomically become nonblocking; it never points to the retained
`awaiting_ack` root. Making the root its predecessor would deadlock because the
root cannot become delivered until that head and suffix are delivered.

## Retire DAG and provider-message alias safety

A retire payload addresses a logical outer key, never a provider message ID.
Its predecessor is the accepted replacement key. Tendwire does not make retire
eligible until that predecessor is `delivered`. The sole exception is the
standalone `decision_resolved` retire defined above: its predecessor/target may
be a terminal `delivered`, `superseded`, or `dead_letter` decision after a
lease, because provider acceptance may have preceded the terminal Tendwire
state.

Herdres provider/presentation ownership has three durable logical states:

```text
owns(provider_message_id)
alias(provider_message_id, current_owner_key)
retired
```

When a replacement edits or reuses an existing provider message, Herdres first
binds the replacement key as owner, records an ownership alias from the old
logical slot to the replacement key, durably checkpoints both changes, and only
then ACKs the replacement. The old outer-key `job_binding` itself is immutable:
its accepted operation, payload fingerprint, provider coordinate, and outcome
are never rewritten or rebound. Alias/current-owner state belongs only to the
provider-message and presentation-slot records and is never an alternate replay
lookup.

On retire:

- `owns` and still current owner: delete, durably tombstone, then ACK;
- `alias` whose current owner is a different delivered replacement: perform a
  logical retire only and never delete the reused message;
- already retired, or absent with a durable tombstone: idempotent no-op ACK;
- absent with no immutable job, alias, or tombstone for a possibly accepted
  mandatory target: fail `provider_binding_unknown`, never ACK; and
- uncertain provider response: fail closed and never claim deletion.

Thus a delayed retire for an old working/final key cannot delete a message that
has since been edited into the accepted replacement.

Tendwire cannot observe whether Herdres retained, aliased, lost, or deleted a
provider binding. Its only evidence is connector settlement and attempt state.
It therefore never declares a retire unnecessary because of assumed Herdres
state and never shortens a root, route, content, or retry horizon based on such
an assumption.

## Producer transactions and root invariants

- ACP working ingestion atomically appends the authoritative event, applies the
  turn/revision/delta projection, allocates a route sequence, supersedes older
  unleased working work, and enqueues the new immutable working row.
- Completion atomically fixes the current content revision, prevents later
  working enqueue for that turn/revision, supersedes outstanding working work,
  and enqueues final-ready after it in the same partition.
- Prepare commit atomically creates the active child/retire DAG and retains the
  source root in `awaiting_ack`.
- Pending-decision projection and decision enqueue are atomic. Resolution and
  retire enqueue are atomic.
- A newer final supersedes an uncommitted old root immediately. For a committed
  old root, its unaccepted tail is terminal-after-lease/superseded, but its
  accepted prefix remains retained until the new replacement/retire DAG is
  acknowledged. No delete-before-replace gap is possible.
- A root becomes `delivered` only when every node in its effective original or
  recovered lineage is delivered. It becomes `superseded` or `dead_letter`
  atomically with the relevant remaining lineage.

Lifecycle snapshots reconcile workers/routes/topics but never create or replay
presentation content. Herdres has no `turn.delta`, `pending.list`, transcript,
snapshot-content, or other parallel authority for final delivery.

## Production import and caller rewrite

Every production `store.sqlite` import is rewritten directly; there is no
`store/__init__.py` façade.

| Old symbol | New module or disposition | Current production callers after rewrite |
| --- | --- | --- |
| `init_store` | `store.schema` | daemon `_default_init_store`, `DaemonHooks`; smoke fixture setup |
| `record_agent_event` | `store.events` | ACP coordinator console bridge/cursor/outcome |
| `list_agent_events` | `store.events` | ACP coordinator console bridge/cursor/input loads |
| `tail_event_metadata` | **deleted** | only live production caller is deleted direct CLI `cmd_store`; no after-state caller |
| `AppendBoundAgentEventResult` | `core.agent_events` | ACP ingestion event result; never moved into a store module |
| `AppendProjectedAgentEventResult` | `store.turns` | ACP ingestion type/API |
| `TurnRefreshApplyResult` | `store.turns` | ACP ingestion result |
| `append_agent_event_and_apply_turn_for_binding` | `store.turns` | ACP ingestor default persistence path |
| `apply_turn_refresh` | `store.turns` | `scripts/sqlite_sidecar_race_benchmark.py`, `tests/store_helpers.py`, and focused turn/delta/submission/performance tests; no current `src` caller imports it directly, while ACP runtime uses the combined append/apply API |
| `turns_payload_from_store` | `store.turns` | daemon `get_turns` |
| `turn_delta_payload_from_store` | `store.turns` | daemon `get_turn_delta` |
| `get_turn_content` | `store.turns` | daemon `get_turn_content` |
| `SnapshotObservationContext` | `store.projection` | ACP discovery; daemon/tests |
| `save_snapshot` | `store.projection` | ACP coordinator discovery; returns the persisted route-enriched snapshot with recomputed worker/content fingerprints |
| `latest_snapshot` | `store.projection` | command submission; ACP coordinator; daemon start/snapshot; smoke |
| `attention_payload_from_store` | `store.projection` | daemon `get_attention` |
| `upsert_worker_bindings` | `store.projection` | ACP coordinator runtime/binding creation |
| `list_worker_bindings` | `store.projection` | ACP coordinator/runtime/permissions; smoke |
| `expire_worker_bindings` | `store.projection` | ACP coordinator/runtime |
| `expire_stale_worker_bindings` | `store.projection` | tests and projection API; no compatibility import |
| `backend_pending_health` | `store.projection` | daemon pending-ingestion health |
| `apply_backend_pending_observation` | `store.pending` | ACP permission broker |
| `pending_payload_from_store` | `store.pending` | ACP coordinator and daemon `get_pending` |
| `claim_backend_pending_decision` | `store.pending` | command submission decision validation/claim |
| `start_backend_pending_decision_send` | `store.pending` | command submission answer send |
| `abandon_backend_pending_choice_claim` | `store.pending` | command submission safe pre-send abandon |
| `backend_pending_choice_terminal_effect` | `store.pending` | command submission uncertain/terminal decision effect |
| `reserve_command_request` | `store.receipts` | command submission reservation |
| `reserve_terminal_command_replay` | `store.receipts` | command submission replay |
| `get_command_request` | `store.receipts` | command submission recovery/replay paths |
| `command_reservation_is_live` | `store.receipts` | command receipt authority |
| `abandon_command_request_reservation` | `store.receipts` | safe pre-transport abandon/retry |
| `mark_command_send_started` | `store.receipts` | command submission send fence |
| `finish_command_request` | `store.receipts` | command completion |
| `finish_queued_command_request` | `store.receipts` | queued receipt completion |
| `finish_unverified_queued_command_request` | `store.receipts` | uncertain queued completion |
| `recover_unresolved_command_send` | `store.receipts` | receipt authority recovery |
| `linked_turn_for_submission` | `store.receipts` | send/link/replay correlation |
| `settle_submission_link_for_request` | `store.receipts` | receipt and negotiated submission linkage |
| `envelope_to_receipt_json` | `store.receipts` | canonical receipt persistence |
| `poll_connector_outbox` | `store.outbox` | `ConnectorOutboxAPI.poll` |
| `prepare_connector_plan_begin` | `store.outbox` | `ConnectorOutboxAPI.prepare(begin)` |
| `prepare_connector_plan_part` | `store.outbox` | `ConnectorOutboxAPI.prepare(part)` |
| `prepare_connector_plan_commit` | `store.outbox` | `ConnectorOutboxAPI.prepare(commit)` |
| `prepare_connector_plan_recover` | `store.outbox` | `ConnectorOutboxAPI.prepare(recover)` |
| `ack_connector_delivery` | `store.outbox` | `ConnectorOutboxAPI.ack` |
| `fail_connector_delivery` | `store.outbox` | `ConnectorOutboxAPI.fail` |
| `defer_connector_delivery` | `store.outbox` | `ConnectorOutboxAPI.defer` |
| `renew_connector_delivery` | `store.outbox` | `ConnectorOutboxAPI.renew` |
| `release_connector_delivery` | `store.outbox` | `ConnectorOutboxAPI.release` |
| `reclaim_expired_connector_leases` | `store.outbox` | connector API reclaim; daemon periodic tick |
| `connector_reclaim_due` | `store.outbox` | daemon periodic connector tick |
| `inspect_connector_outbox` | `store.outbox` | `ConnectorOutboxAPI.inspect` |
| `retry_final_ready_delivery` | `store.outbox.retry_connector_dead_letter` | `ConnectorOutboxAPI.retry`; the old final-only name is deleted and the replacement handles exact retryable final roots, standalone decisions, and standalone retires |
| `store_status` | `store.db` | daemon health only |
| `SnapshotRetentionPolicy` | `store.retention.RetentionPolicy` | rewritten daemon retention cycle |
| `cleanup_*_retention` | private `store.retention` helpers | `run_retention_cycle` only |
| `CompactionOptions`, `compact_store`, `run_store_maintenance`, `maybe_run_automatic_store_maintenance`, `compact_turn_change_journal`, `exhaust_connector_retries` | **deleted as public APIs** | direct CLI/daemon maintenance paths removed; outbox exhaustion remains a private transition helper |

The CLI rewrite removes the `store` parser subtree, `cmd_store`, direct status,
event-tail, maintenance, and compact options, and all five direct SQLite
imports. Operator reads use the daemon socket. There is no compaction RPC.

Daemon `_after_snapshot_saved` removes maintenance-state and turn-journal
compaction calls and invokes one bounded `retention.run_retention_cycle` on an
in-memory cadence. Connector periodic reclaim remains independent. Health uses
`db.store_status`; it does not mutate or repair the store.

`scripts/store_benchmark.py` is rewritten to import the owning schema,
projection, outbox, and retention APIs and to seed only the fresh schema; its
old direct assumptions about maintenance tables and `store.sqlite` are deleted.
`scripts/sqlite_sidecar_race_benchmark.py` is rewritten to import
`store.schema`/`store.db`, patch the new pinned-path connection authority rather
than a deleted module, and retain installed-wheel sidecar-race, inode, mode,
integrity, and descriptor evidence. Neither script is allowed a compatibility
import or private direct-SQL mutation that bypasses the invariant it claims to
benchmark.

## Test-file disposition

| File | Disposition |
| --- | --- |
| `tests/store_helpers.py` | rewrite direct imports to owning modules |
| `tests/test_acp_atomic_ingestion.py` | retain atomic journal/projection proof; rewrite imports |
| `tests/test_acp_coordinator.py` | rewrite imports; add route-generation publication/rotation/collision cases |
| `tests/test_acp_ingestion.py` | rewrite result-type and event/turn imports |
| `tests/test_acp_permissions.py` | replace module-wide sqlite monkeypatches with exact pending/projection patches |
| `tests/test_acp_runtime.py` | rewrite projection/event imports |
| `tests/test_agent_events.py` | rewrite to `store.events`; preserve authoritative dedupe/conflict, visibility, paging, and retention proofs |
| `tests/test_daemon.py` | rewrite imports/patch paths; delete automatic-maintenance-state assertions; add bounded retention and daemon-only health |
| `tests/test_daemon_acp.py` | rewrite schema/projection imports |
| `tests/test_connector_daemon_cli.py` | retain real socket RPC contract; delete direct CLI store fallback cases |
| `tests/test_connector_outbox.py` | replace with the five-kind status/CAS matrix, FIFO/DAG, lease, response-loss, recovery, and alias-retire tests |
| `tests/test_delivery_retention.py` | rewrite for kind-aware live/root/effective-lineage protection |
| `tests/test_delivery_retention_hardening.py` | delete maintenance-state tests; retain deadline/dead-letter/cutoff/security tests |
| `tests/test_delivery_retention_projection.py` | retain atomic projection/outbox proof; add route correlation |
| `tests/test_delivery_retention_recovery.py` | rewrite for retained old prefix plus fresh suffix lineage |
| `tests/test_local_state_permissions.py` | rewrite `init_store` import; retain secure-open proof |
| `tests/test_public_content_safety.py` | rewrite imports; add exact five-kind forbidden-key/value scans |
| `tests/test_release_readiness.py` | delete VACUUM/backup/compaction ceremony; retain fresh cutover, integrity, permissions, and retention proof |
| `tests/test_snapshot_sanitize_performance.py` | rewrite projection imports and preserve sanitizer/transaction performance bounds without a sqlite compatibility path |
| `tests/test_store.py` | split by owning module; delete migration-chain/generic CRUD/direct-maintenance internals |
| `tests/test_turn_delta.py` | rewrite turn imports; add route-generation publication/correlation |
| `tests/test_turn_submissions.py` | rewrite schema/turn imports; preserve state machine |
| `tests/test_worker_stable_key.py` | add stable/reused/rotated/collision route-generation tests |

The paired Herdres H7/H6 table is authoritative here and in
`docs/wave4-presenter-state-design.md`; every baseline file has one matching
disposition:

| Herdres baseline test file | Final disposition |
| --- | --- |
| `conftest.py` | retain; edit only bounded shared fixtures, never protocol or production logic |
| `test_accounts.py` | delete with pinned boards |
| `test_collapse_previous.py` | rewrite for immutable target keys, flattened one-hop aliases, and reuse-safe retire |
| `test_command_ingress_idempotency.py` | rewrite/retain H8 HMAC request identity, key security, and replay vectors |
| `test_gateway_cleanup.py` | delete old gateway cleanup; move known-ID topic cases to `test_topics.py` |
| `test_ingress_lanes.py` | delete with lanes; surviving FIFO/crash cases move to `test_ingress.py` |
| `test_ingress_requests.py` | delete with JSON request workers; surviving receipt/quarantine cases move to `test_ingress.py` |
| `test_lossless_turn_rendering.py` | rewrite for contract-v3 deterministic exact spans and no stored content |
| `test_model_in_pins.py` | delete with pinned boards |
| `test_offlock_delivery.py` | rewrite for shared guard, lock order, route revalidation, and uncertainty classes |
| `test_outbound_latency.py` | rewrite for one poll/presenter and bounded lease/guard budgets |
| `test_pane_topic_binding_integrity.py` | rewrite for typed lifecycle, current slots, aliases, and immutable provider facts |
| `test_pending_inputs.py` | delete pending-list/bare-number local presentation path |
| `test_release_readiness.py` | rewrite for exact scope, static gates, 8,920 SLOC target, security, and paired cutover |
| `test_remote_decisions.py` | rewrite for decision controls, composite phases, shared guard, and resolution retire |
| `test_restart_rekey_continuity.py` | delete old rekey machinery; route continuity moves to state/presenter tests |
| `test_rich_delivery.py` | rewrite for deterministic one-request materialization and no fallback mutation |
| `test_source_only.py` | rewrite surviving connector receipt/crash cases; delete local-source/source-less cases |
| `test_source_status_placeholders.py` | delete local source/status placeholder presentation |
| `test_speak_back.py` | delete with voice/TTS |
| `test_speech.py` | delete with voice/STT/TTS |
| `test_stable_generation_delivery.py` | rewrite for exact route token, enriched fingerprint, and stale-route fence |
| `test_stable_worker_key.py` | retain and extend stable identity, route reuse/rotation, and collision cases |
| `test_table_rendering.py` | retain supported renderer; remove pin/voice coupling if present |
| `test_telegram_backpressure.py` | rewrite for one guarded mutation, exact 429 defer, and no tight loop |
| `test_tendwire_client.py` | rewrite for five kinds, exact prepare/content responses, renew/release, and socket validation |
| `test_tendwire_socket_pairing.py` | rewrite for real paired five-kind/ingress/receipt/restart integration |
| `test_topic_lifecycle_cleanup.py` | rewrite as bounded known-ID cases in `test_topics.py` |
| `test_topic_names.py` | retain/rewrite for minimal supported topic naming only |
| `test_turn_delta_sync.py` | delete; final presentation has no turn-delta source |
| `test_turn_final_delivery.py` | replace with source-bound root, provider kinds, recovery, retry, and retire integration |
| `test_worker_topic_dedup.py` | rewrite for physical-owner serialization across route generations |

The final stack adds exactly `test_ingress.py`, `test_state.py`,
`test_presenter.py`, `test_presentation.py`, and `test_topics.py`. No unlisted
Herdres test file may be added, deleted, or used as a compatibility dump.
Rewritten files may share fixtures through `tests/conftest.py`; production logic
and copied protocol validators are forbidden there.

Tests assert public behavior and database constraints, not private helper names or
copied transition logic. Required real-daemon integration covers working to
final ordering, paged multipart prepare, lost begin/part/commit responses,
provider acceptance plus lost ACK, source lease expiry, recovered prefix/suffix,
supersession and message reuse, 429 defer, decision buttons/resolution, privacy
rejection, stale refs, dead-letter/retry generations, and concurrent logical
partitions sharing one physical provider route.

A static AST/import gate scans `src`, `scripts`, and tests and fails on any
production import, attribute patch, string import, or compatibility re-export
of `tendwire.store.sqlite`/`store.sqlite`. It also proves each deleted public
maintenance symbol has no production caller.

## Precise implementation file scope

Tendwire T6 deletes/adds/edits only:

```text
delete  src/tendwire/store/sqlite.py
add     src/tendwire/store/schema.py
add     src/tendwire/store/db.py
add     src/tendwire/store/events.py
add     src/tendwire/store/turns.py
add     src/tendwire/store/projection.py
add     src/tendwire/store/pending.py
add     src/tendwire/store/receipts.py
add     src/tendwire/store/outbox.py
add     src/tendwire/store/retention.py
edit    src/tendwire/store/__init__.py       # package marker only; no re-exports
edit    src/tendwire/cli.py
edit    src/tendwire/config.py              # bounded ACK TTL and retention floors
edit    src/tendwire/daemon.py
edit    src/tendwire/daemon_api.py
edit    src/tendwire/command_submission.py
edit    src/tendwire/worker_identity.py
edit    src/tendwire/core/models.py
edit    src/tendwire/backends/acp_coordinator.py
edit    src/tendwire/backends/acp_ingestion.py
edit    src/tendwire/backends/acp_permissions.py
edit    src/tendwire/backends/acp_runtime.py
edit    src/tendwire/connectors/outbox.py
edit    scripts/herdr_smoke.py
edit    scripts/store_benchmark.py
edit    scripts/sqlite_sidecar_race_benchmark.py
edit    docs/connector-rpc-contract.md
edit    docs/wave4-store-design.md
edit    only the Tendwire tests listed in the disposition table
```

The reconciled Herdres scope is split at one safe merge boundary.

H8 may be implemented, reviewed, and deployed independently against the current
Tendwire/current presenter pair. Its exact file disposition is:

```text
add     herdres_connector/ingress.py
add     herdres_connector/ingress_queue.py
edit    herdres_gateway.py                 # retained small executable wrapper
edit    herdres.py                         # remove command-child ingress only
edit    herdres_connector/state.py         # frozen typed H8 seam
edit    herdres_connector/decisions.py
edit    herdres_connector/doctor.py
edit    herdres_connector/config.py
edit    herdres_connector/source_sync.py     # remove JSON-ingress/receipt-working joins
edit    herdres_connector/ingress_identity.py  # trim retained key/identity code to 80--100 SLOC
edit    herdres_connector/managed_bots.py    # ephemeral receiver/policy typing only
edit    herdres_connector/tendwire_client.py
edit    herdres_connector/telegram_delivery.py  # bounded markup ambiguity marker only
delete  herdres_connector/ingress_lanes.py
delete  herdres_connector/ingress_requests.py
edit/delete only the H8 tests and docs declared by the approved H8 design
```

H8's frozen route result uses `route_generation: str | None`, and its ingress
queue column is nullable `TEXT`, never integer. Current routes publish null,
and stored null is immutable for that request. Contract-v3 H7 routes require a
non-null exact `twroute1.*`; open pre-T6 null-route requests are drained or
explicitly discarded at paired cutover. H8 does not depend on the five-kind
outbox. The old presenter continues the current observational working/final
behavior until the paired cutover.

After H8 is deployed, corrected T6 and the H7/H6 presenter stack form one
inseparable compatibility and deployment unit. H7/H6's exact final disposition
is:

```text
add     herdres_connector/presenter.py
add     herdres_connector/presentation.py
add     herdres_connector/topics.py
edit    herdres.py
retain  herdres_gateway.py                 # small H8 compatibility wrapper
edit    herdres_connector/__init__.py
edit    herdres_connector/config.py
edit    herdres_connector/decisions.py
edit    herdres_connector/doctor.py
edit    herdres_connector/ingress.py
edit    herdres_connector/ingress_queue.py
keep    herdres_connector/ingress_identity.py
edit    herdres_connector/managed_bots.py
edit    herdres_connector/rendering.py
edit    herdres_connector/rich_delivery.py
edit    herdres_connector/safe.py
edit    herdres_connector/state.py
edit    herdres_connector/telegram_delivery.py
edit    herdres_connector/tendwire_client.py
delete  herdres_connector/source_sync.py
delete  herdres_connector/accounts.py       # pinned-board deletion
delete  herdres_connector/speech.py         # voice/STT/TTS deletion
edit    README.md
edit    RELEASE.md
edit    SECURITY.md
edit    docs/connector-rpc-contract.md
edit    docs/remote-decisions.md
edit    docs/wave4-ingress-dependency-design.md
edit    docs/wave4-presenter-state-design.md
edit    docs/wave4-store-design.md
edit/delete/add only the paired Herdres tests in the exact table above
```

H7 state keeps immutable job bindings keyed only by Tendwire outer key and
provider/presentation ownership aliases separately; it contains no local plan,
accepted-ordinal, pending-retire, retry, or recovery ledger. Current Herdres
strictly accepts final-ready schema v2 and plan-job schema v1, so T6 cannot ship
alone. H7/H6 likewise cannot ship against current Tendwire. Only H8 has an
independent deployment boundary; the next deployable boundary is the complete
T6+H7/H6 paired contract-v3 unit.

## Transactions, security, privacy, and retention

Every mutation uses `BEGIN IMMEDIATE`. Frozen list/content/delta pages use a
read transaction. Event append, binding/route fence, turn and content revision,
delta sequence, command/submission effect, pending overlay, and outbox enqueue
are atomic where the producer invariant requires them.

Connections are short-lived and use WAL, foreign keys, `FULL` synchronous,
bounded busy timeout, and `trusted_schema=OFF`. The configured database must be
an absolute leaf beneath the exact configured owner-private data directory; it
cannot be `:memory:`, a URI, a relative path, contain `..`, or select another
authority root. Code walks and pins every parent with directory fds and
`O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC`, rejects symlinks, wrong UID, group/world
writable directories, and parent identity changes, and opens/creates the leaf
relative to the pinned parent with restrictive umask and
`O_NOFOLLOW|O_CLOEXEC`, mode `0600`, regular-file, single-link, UID, and
device/inode checks.

SQLite connects through the pinned `/proc/self/fd/<parent-fd>/<leaf>` path while
the schema/sidecar authority is held. The implementation verifies the database
identity before and after connect and transaction use; validates or securely
creates `-wal` and `-shm` only as same-parent, same-UID, regular, single-link
0600 files; rejects unexpected journal/sidecar types or inode swaps; and pins
and revalidates the family across schema mutation, checkpoint, and close. A
separately pinned owner-only lock serializes schema/sidecar creation without
becoming a general authority registry. No fallback connect occurs after any
identity or permission failure.

The connector-edge scanner rejects raw pane/session/terminal identities,
backend targets, private fingerprints, provider chat/topic/message IDs,
credentials/tokens, socket and absolute paths, command/argv/environment data,
ACP option IDs, stdout/stderr, and exception prose, including forbidden values
embedded in otherwise allowed strings. Only enumerated public errors and reason
codes cross the socket. Logs and cutover reports contain aggregate counts and
schema versions only, never IDs, refs, keys, payloads, or provider coordinates.

Retention performs bounded child-first cutoff deletes. It never deletes a live
lease, staged plan, retained awaiting-ACK root, current/recovered lineage,
dead-letter inside its inspection cutoff, command replay floor, current content
revision, page referenced by work, active route generation, or maximum sequence
sentinel/partition allocator. A delivery with non-null `ack_deadline_at` is live
regardless of wall-clock age; after terminal settlement the deadline is cleared
and deletion still waits for the 30-day targetable floor. Targetable outbox
correlations use 30 days, route/content/key evidence uses 45 days, and the
paired H7 provider-fact floor is 60 days, with references overriding all three.
It retains only bounded terminal attempt detail plus aggregate prior attempt
counts, then runs `PRAGMA wal_checkpoint(TRUNCATE)`. It performs no
VACUUM, backup swap, direct CLI repair, or automatic schema migration.

## Cutover and rollback

H8 has the only independent deployment boundary. Its release stops the old
gateway writer, drains or explicitly discards both old ingress durability
stores, archives the old `inbound_spool.db` family and matching schema-2
Herdres state, and preserves the one active H8 request-ID HMAC key byte-for-byte
at its configured owner-only path. It starts exactly one H8 writer on the fresh queue
with that key and proves one daemon-socket receipt. Snapshot rollback is
straightforward only before H8 accepts new work. After acceptance it requires the H8 design's
explicit cursor/request inventory and operator drain/discard decision; the old
snapshot is not described as lossless merely because it can be restored.

T6+H7/H6 is the next single deployment boundary. Tendwire schema mismatch and
H7 state mismatch never migrate silently. Before that paired upgrade operators
must choose:

1. drain old command reservations, claims, interactions, leases,
   awaiting-ACK roots, presentation plans, and outbox work to zero; or
2. stop every old writer and explicitly acknowledge discarding all in-flight
   state before fresh recreation.

The quiescent cutover snapshot is one matched set: Tendwire database/WAL/SHM,
the active Tendwire installation key/marker/sentinel, H8 ingress
database/WAL/SHM, the same active H8 request-ID HMAC key used
before cutover, and old Herdres state/provider receipts. The paired release
must not rotate, regenerate, or silently replace the Tendwire key family or the
H8 request-ID key. Old/new
versions, discarded table names, and aggregate active counts are reported
without identities or payloads. Old and new presenters never run against the
same topics.

Fresh H7 state starts behind a reconciliation barrier. While H8 submission and
all Telegram mutators remain stopped, one lifecycle writer reconciles every
current worker/private route, Tendwire durably publishes each non-null
`twroute1.*`, `save_snapshot` returns the enriched fingerprints, and H7 resolves
the same stable owner/generation pairs through the frozen seam. The barrier is
released only after one transactionally consistent inventory proves no live
route is missing/ambiguous and every retained H8 request either has the exact
non-null generation/fingerprint or was drained or explicitly discarded. Only
then may H8 and the contract-v3 presenter resume. Failure leaves the barrier
closed and authorizes no command or provider mutation.

Rollback is mechanically safe before recreation and before any new provider
mutation. After the paired release has sent, edited, deleted, or created a
provider object, restoring old local snapshots alone is unsafe: it forgets
new provider facts and can replay or delete the wrong message. Rollback then
requires stopping all new writers, draining/ACKing or explicitly retiring every
new known provider mutation with the new release, resolving/reporting ambiguous
claims, and only then restoring the complete matched old snapshot. If that
drain/retire proof is unavailable, rollback is blocked rather than described as
safe. Installation-key or request-key replacement is a continuity break, not
recovery.

No implementation, merge, deployment, or service restart is authorized by
this design. Production implementation remains blocked until the paired
Herdres payload, outer-key binding, no-plan-ledger recovery, physical-route
locking, and alias-retire tests pass against this exact contract.
