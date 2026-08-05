# Wave 16 reduction architecture reassessment

Status: planning evidence only. This document authorizes no production edit, public
capability deletion, target amendment, migration, dependency, merge, or deployment.

The baseline is Tendwire `b9e1f39d3f12e9933a842ffad407a5e56f666818`.
Its latest production-code ancestor is `5158553ed8f2a49a57ffffe5a2f30d4d195f19e4`.
The production tree contains 39 Python files and 20,985 canonical SLOC. The paired
Herdres checkpoint is `d581368f14f063a146bc700b00f47065a45e8939` at 8,115
canonical SLOC. The paired visible-console Herdr fork is
`9026d9bc5a12d9adc2d9f68ebdc564133e4098b4`; Herdr remains outside the reduction count
and its behavior is frozen.

This reassessment follows the stop recorded by `f467056`. It turns the component ledger
into conditional architecture choices and measurable gates before another major rewrite.
It does not convert provisional ranges into proven floors.

## Completion arithmetic

Tendwire must remove at least 6,985 production SLOC to reach the unchanged 14,000 global
ceiling. Because the next architecture is still estimate-only, its plan must total no
more than 13,500, leaving at least 500 SLOC for integration and estimate variance. A plan
between 13,501 and 14,000 is acceptable only after every high-variance row has a measured
conventional-code prototype.

The evidence-backed provisional low allocations from the component ledger sum to 14,200:

| Component | Provisional low |
|---|---:|
| Store | 4,500 |
| ACP transport | 700 |
| Runtime/coordinator/permissions | 1,800 |
| Ingestion/projection | 1,700 |
| Command submission | 900 |
| Daemon/API/config/identity/security | 2,250 |
| Herdr socket client | 400 |
| Privacy/connector facade | 1,500 |
| CLI/core remainder | 450 |
| **Total** | **14,200** |

These are planning allocations, not independently implemented or measured results.
Reaching all nine low ends simultaneously is already an optimistic assumption.

## Variant P: preserve every current public capability

The conventional reviewed ranges for the three largest unresolved semantic rows are:

| Component | Current | Preserve-all range | Concrete consolidation boundary |
|---|---:|---:|---|
| Ingestion/projection | 2,661 | 1,700–2,100 | one event identity owner, one projector dispatch, shared exact token codec, unchanged atomic append/apply |
| Command submission | 1,380 | 1,000–1,150 | one receipt transition engine and one closed command/selector parser |
| Privacy/connector facade | 2,455 | 1,500–1,800 | one bounded sanitizer policy, exact model builders, one connector shape validator, table-driven facade |

Their conservative low subtotal is 4,200. Combining it with the other six provisional
lows of 10,100 produces 14,300. Variant P therefore has no reviewed path to 14,000 and
no contingency. Claiming a 14,000 plan would instead require unproved stretch caps of
approximately 1,650 ingestion, 850 command, and 1,400 privacy while every other row also
hits its optimistic low.

Variant P is a prototype-gated target-amendment path, not an implementation candidate.
If its three prototypes miss any stretch cap, the honest action is to retain behavior and
amend the global target, not to hide authority in another row or weaken tests.

The reviewed preserve-all file plan makes the difference between a conventional target
and an arithmetic low visible:

| Row | Current | Preserve point | D point | Preserve → D file targets |
|---|---:|---:|---:|---|
| Store | 6,192 | 5,125 | 4,900 | `db` 190→190; `events` 85→85; `outbox` 1,450→1,450; `pending` 650→650; `projection` 430→430; `receipts` 580→580; `retention` 290→290; `schema` 250→250; `turns` 1,200→975; initializer 0→0 |
| ACP transport | 1,202 | 830 | 830 | `acp_client` 650→650; `acp_protocol` 180→180 |
| Runtime/coordinator/permissions | 2,753 | 2,000 | 2,000 | `acp_coordinator` 1,300→1,300; `acp_permissions` 180→180; `acp_runtime` 520→520 |
| Daemon/API/config/identity/security | 3,407 | 2,320 | 2,235 | `daemon` 385→365; `daemon_api` 1,070→1,005; `config` 259→259; `local_state` 360→360; `worker_identity` 246→246 |
| Herdr socket client | 404 | 350 | 350 | `herdr_protocol` 140→140; `herdr_socket` 210→210 |
| CLI/core remainder | 531 | 470 | 448 | `cli` 467→445; package/version files 3→3 |
| **Six-row subtotal** | **14,489** | **11,095** | **10,763** | exact hypothesis sums |

Combining the preserve six-row point target with the three semantic rows' 4,200
conservative low produces 15,295. The lower 14,300 figure elsewhere in this document uses all six
arithmetic-only provisional lows instead of these conventional file targets. Neither is
a global-ceiling plan. Both point columns are consolidation hypotheses, not delivery caps.

The three semantic row estimates also have file-level ownership:

| Row | File-level conventional planning ranges |
|---|---|
| Ingestion/projection | `acp_ingestion` 360–420; `acp_projection` 430–500; `agent_events` 220–260; `attention` 90–110; `projector` 10–15; `core/turns` 590–695. The function plan sums to 1,700–2,000, with 100 SLOC high-side risk retained in the reviewed 1,700–2,100 range. |
| Command submission | `command_submission` 650–730; `core/commands` 350–420, total 1,000–1,150. |
| Privacy/connector facade | connector package initializer 0–3; `connectors/outbox` 400–500; `connectors/protocol` 180–230; `core/models` 920–1,070, rounded total 1,500–1,800. |

## Variant D: owner-authorized public deletion

This variant deletes both of the following release-visible capabilities:

1. `turn.delta` in one atomic cross-row change: RPC, CLI, daemon callback, store query,
   watermark/cursor implementation, tests, and frozen method inventory. Its direct
   exclusive charge is 582 canonical SLOC and its conventional net is approximately
   580–590. Only 250 belongs to ingestion; the rest remains charged to Store,
   daemon/API, and CLI.
2. Turn-list schema 1 while retaining schema 2. The exclusive leaf is seven SLOC and the
   conventional net is approximately 25–40. The request still accepts exactly schema 2,
   and existing schema-2 cursor and since-token formats remain byte-for-byte stable.
   This does not authorize deleting `turn.content.get` schema 1 or any connector schema-1
   envelope.

Current Herdres production has no caller for either surface.
`test_source_only.py::test_presenter_has_no_turn_list_delta_or_pending_source` is
presenter-only supporting evidence; release proof also requires a production-and-scripts
grep across every Herdres module and an inventory of external Tendwire RPC/CLI clients.
Unknown external local clients remain a release impact, so absence of an in-repository
caller is not owner authorization.

There is no approved combined saving. The delta deletion has an exact direct charge and
an estimated conventional net. The schema-1 saving is itself a shared-path estimate and
must be measured only after the delta RPC, callback, store query, codecs, CLI, dispatch,
and tests have been removed. Adding the two headlines would double-count or miss their
shared API/core/store seams. No combined total or resulting global target may be reported
until the conventional combined prototype exists. The only rigorous intermediate
sensitivity is the exact direct D-A charge: 14,200 minus 582 equals 13,618. Before any
measured D-B or further prototype deletion, that is still 118 above the 13,500 planning
gate.

Variant D is therefore prototype-gated, not a credible 14,000-SLOC implementation plan.
Before it can become a major wave, it needs a combined conventional delta-plus-schema
prototype followed by conventional prototypes for every high-variance row that prove the
combined target at or below 13,500. Until then neither its contingency nor even its final
global SLOC is established.

The following table makes the knife-edge allocation explicit. It is a proof obligation,
not a forecast:

| Row | Required cap for an exact 13,500 allocation | Evidence status |
|---|---:|---|
| Store | 4,180 | unmeasured; 320 below the original low, 48 below the A-adjusted 4,228, and 41 below that value after the seven-line B leaf |
| ACP transport | 700 | unmeasured optimistic provisional low |
| Runtime/coordinator/permissions | 1,800 | unmeasured optimistic provisional low |
| Ingestion/projection | 1,450 | optimistic post-delta low; exact combined diff absent |
| Command submission | 900 | published low but 100 below the conservative reviewed implementation range |
| Daemon/API/config/identity/security | 2,220 | post-delta knife edge and blocked by the SQLite threat ruling |
| Herdr socket client | 350 | reviewed contraction low; frozen behavior and unmeasured diff |
| Privacy/connector facade | 1,500 | optimistic reviewed low; privacy prototype absent |
| CLI/core remainder | 400 | 50 below its corrected planning low; combined CLI diff absent |
| **Total** | **13,500** | **NO-GO until every row is measured and reviewed** |

The corresponding file-level spike hypothesis is deliberately exposed so no aggregate
can hide a miss:

| Row | Hypothetical file allocation |
|---|---|
| Store 4,180 | `db` 190; `events` 70; `outbox` 1,300; `pending` 500; `projection` 330; `receipts` 450; `retention` 240; `schema` 230; `turns` 870; initializer 0 |
| ACP transport 700 | `acp_client` 545; `acp_protocol` 155 |
| Runtime 1,800 | `acp_coordinator` 1,170; `acp_permissions` 170; `acp_runtime` 460 |
| Ingestion 1,450 | `acp_ingestion` 360; `acp_projection` 430; `agent_events` 220; `attention` 90; `projector` 10; `core/turns` 340 |
| Command 900 | `command_submission` 580; `core/commands` 320 |
| Daemon/security 2,220 | `daemon` 360; `daemon_api` 995; `config` 259; `local_state` 360; `worker_identity` 246 |
| Herdr socket 350 | `herdr_protocol` 140; `herdr_socket` 210 |
| Privacy/connectors 1,500 | package initializer 0; `connectors/outbox` 400; `connectors/protocol` 180; `core/models` 920 |
| CLI/core 400 | `cli` 397; package/version files 3 |

Every figure in this table is an unmeasured prototype hypothesis. A miss in one file or
row must be recovered by an identified measured deletion elsewhere; aggregate slack may
not conceal it.

The delta allocation is exact before conventional cleanup: 250 ingestion, 272 Store,
44 daemon/API, and 16 CLI. The schema-1 exclusive leaf is seven Store SLOC; its remaining
core, Store, daemon/API, and CLI effects have no approved per-row allocation until the
post-delta prototype is counted. No line may be credited to both a semantic row and the
other-six subtotal.

Deleting connector dead-letter inspection or retry is not part of Variant D. Those RPCs
are the bounded operator discovery and manual requeue surface. `connector.prepare` action
`recover`, automatic lease reclaim, plan-token resume, and every connector settlement
operation remain required.

### Variant D wire and release contract

D-A and D-B are separate checkpoints. D-A removes `turn.delta`; a request then receives
the existing closed error envelope with `schema_version: 1`, `ok: false`,
`status: "error"`, null result, error code `unknown_method`, message `unknown method`,
and the newly frozen allowed-method list. D-B accepts `turn.list` only when
`schema_version` is exactly integer 2. A missing, boolean, schema-1, or other value receives
error code `unsupported_schema`, message `unsupported turn list schema version`, and
details `{"supported_turn_schema_versions":[2]}`. The CLI always sends schema 2 and no
longer offers a schema-1 choice.

D-B invalidates only schema-1 list cursors and requests. It preserves the schema-2 cursor
and since-token wire material, including the signed schema-version field fixed at 2. It
also preserves `turn.content.get` schema 1, snapshot schema 2, connector schema 1,
command RPC, receipts, and pending decisions.

The caller inventory must cover production and scripts in both repositories, for example:

```text
git grep -n -E 'turn\.delta|turn\.list|pending\.list' -- '*.py' ':!tests/**'
git -C /home/smith/tendwire/.worktrees/herdres-acp-route grep -n -E \
  'turn\.delta|turn\.list|pending\.list' -- '*.py' ':!tests/**'
```

The Herdres source-only readiness test is supporting evidence, not a complete caller
proof. Release notes must declare the break and inventory known external clients. D-A/B
do not alter either database schema and require no drain/discard ceremony themselves;
that ceremony applies to a later fresh-schema rewrite. They still require matched binary
rollback because old local cursors are not translated and no compatibility shim survives.

## Required prototypes before production authority

Prototypes are throwaway or separately checkpointed conventional implementations. They
must use the canonical counter, include all integration code, preserve ordinary formatting,
and must not be merged merely because they meet a line target.

### P0/D: combined public-surface measurement

After explicit capability authorization, measure D-A first across exactly
`core/turns.py`, `store/turns.py`, `daemon.py`, `daemon_api.py`, and `cli.py`, together
with the corresponding tests and contract documents. Freeze fixtures for the remaining
method list, error envelopes, schema-2 list/cursor/since bytes, content paging, and CLI
behavior in the same checkpoint. Canonical-count the conventional D-A diff.

Only then prototype D-B from the D-A tree. Re-run the caller inventory, remove the
schema-1 list projection/default/CLI choice and its tests, and canonical-count the
combined tree rather than adding a headline estimate. This cross-row prototype precedes
P1–P5 and supplies each original ledger row's measured credit. Its files remain owned by
their original rows; it is not a tenth architecture component.

### P1: Store contraction

Scope: `store/` only. Preserve the current exact fresh schema, exact-version rejection,
and explicit incompatible-state discard contract; introduce no migration path or
compatibility schema. Demonstrate one atomic projection path, one receipt state machine,
one outbox, and bounded retention. Remove the no-source-callsite compatibility helper
`events.record_agent_event` and the no-source-callsite `turns.apply_turn_refresh` only
with all exports, scripts, tests, and stale design claims. The latter is still called by
`scripts/sqlite_sidecar_race_benchmark.py`, which must be rewritten in the same change.
Inline the private unreachable receipt branch only if replay, conflict, and lost-response
tests prove no semantic change.

The prototype must retain stable outer keys, attempt-scoped refs, ACK-loss deduplication,
partition FIFO, source and plan-token binding, dead-letter/retry, atomic receipt/submission
claims, pending-decision fencing, and retention reference safety. It must report per-file
before/after counts; a Store subtotal alone is insufficient.

The preserve-all Store proof uses the provisional file-plan range 4,900–5,400; it is not
a proven floor. Conditional
Variant D requires the explicit 4,180 allocation only after P0/D measures the Store
effect; it is not inferred from the 272-line delta charge or seven-line schema leaf.
Any P1 work touching `store/db.py` remains blocked by the SQLite threat ruling even when
other Store files could be prototyped independently.

### P2: ACP transport and runtime contraction

Scope: `backends/acp_client.py`, `acp_protocol.py`, `acp_coordinator.py`,
`acp_runtime.py`, and `acp_permissions.py`. Retain official ACP 0.11 generated schemas and
the custom bounded synchronous transport. There is no upstream-transport adoption work
unless a measured bridge proves equivalent frame, deadline, queue, shutdown, steering,
and privacy behavior with less code.

The prototype must retain one connection per worker, Herdr-minted endpoint ownership,
generation fencing, one ingestion route, prompt and active-turn steering, permission
round trips, request-start uncertainty, 8 MiB default ACP frames within the existing
1-byte-to-64-MiB configured bound, bounded queues/workers/depth/items,
stderr bounds, and bounded shutdown. It may consolidate lifecycle state and capability
parsing; it may not reintroduce transcript, shadow, fallback, or pane-input routing.
The preserve planning points are ACP transport at 830 and runtime/coordinator/permissions
at 2,000. Variant D's exact 13,500 allocation requires transport at most 700 and runtime
at most 1,800. Those lower caps are unmeasured proof targets, not consequences of D-A/B.

The visible Herdr-owned `acp-console` bridge is preserved. It is the ACP endpoint's
bounded console input/output and active-turn steering path for the visible pane; it is not
the forbidden raw PTY injection, pane scraping, or transcript-reading path. The complete
ACP event/UX path remains observable through it: prompt, thought and agent updates, tool
and plan lifecycle, permissions, final, cancellation, and steering while a turn is active.

The frozen Herdr fork supports exactly six ACP adapter labels and launch shapes:
`codex` → `codex-acp`; `claude` → `claude-agent-acp`; `gemini` →
`gemini --acp`; `hermes` → `hermes acp`; `omp` → `omp acp`; and `kimi` →
`kimi acp`. Version probes, the Hermes/OMP/Kimi capability probes, adapter hashes,
registration deadlines, and generation ownership remain fail-closed; every other label is
unsupported. No Kimi model is used as Wave 16 planning, coding, or review workforce.

### P3: projection, command, and privacy contraction

Scope is the exact 12 files charged to ingestion/projection, command submission, and
privacy/connectors in the component ledger. File ownership does not move if common
validators are imported across a boundary.

Prototype targets are conditional:

| Row | Preserve-all proof | Variant D proof |
|---|---:|---:|
| Ingestion/projection | demonstrate whether 1,650 is possible | at most 1,450 after authorized deletions |
| Command submission | demonstrate whether 850 is possible | at most 900 |
| Privacy/connector facade | demonstrate whether 1,400 is possible | at most 1,500 |

The privacy prototype gets a separate adversarial review. Its connector-edge
forbidden-token backstop and hostile nested key/value/error tests cannot be traded for
SLOC. Command uncertainty, selector proofs, terminal replay, claim races, and dry-run
purity remain exact. Ingestion retains durable event/content/pending projection, replay
conflict detection, schema-2 listing, content paging, and token tamper/host/expiry checks.

### P4: daemon and local-state boundary

This prototype remains blocked until the owner chooses one of the three SQLite threat
contracts in `wave14-checkpoint1-reassessment.md`. Standard-library `sqlite3` cannot be
credited with preventing a concurrent same-UID leaf replacement during every internal
main/WAL/SHM open.

P4's edit allowlist is `daemon.py`, `daemon_api.py`, `config.py`, `local_state.py`, and
`worker_identity.py`. P1 alone edits and charges `store/db.py`; P4 consumes its pinned
result as an integrated security-test dependency and never double-credits it.

After the owner ruling, the row must retain strict private/group parent validation,
anchored ancestor descriptors, socket identity pins, peer credentials, exact endpoint
cleanup, 1 MiB daemon request/response frames, bounded request execution, and hostile
request/result validation. The target lifecycle reserves, binds, and pins a non-accepting
socket before Store/backend hooks, initializes Store and ACP, and begins admission only
after startup succeeds; every post-bind failure identity-cleans the owned socket. This is
redesigned target behavior, not a claim about the current checkpoint. The preserve-all
reviewed one-charge range is 2,250–2,395, not the
published 1,200–1,500 band. Conditional Variant D requires at most 2,220 only after P0/D
measures the daemon/API effect and the SQLite contract is resolved; no pre-ruling estimate
makes 2,220 reachable.

Retain the current separate capacity bounds: eight request workers, at most 32 admitted
in-flight connections, kernel listen backlog 32, request-executor queue capacity 32, and
a periodic lane of one worker and one queued item. These are distinct limits and may not
be collapsed or merely described as bounded.

At the hypothetical D file caps, Store `db` at 190 makes the security trio
`daemon_api` 995 + `local_state` 360 + `db` 190 = 1,545, and all six security files total
2,410. These are prototype arithmetic only. The standard library still cannot prevent
same-UID substitution at every internal main/WAL/SHM/journal open; hostile connector
callbacks still require independent API validation; path-string cleanup and unbounded
admission/worker/queue/force-close/reap paths remain forbidden.

### P5: Herdr socket and CLI shell

Scope: `backends/herdr_protocol.py`, `backends/herdr_socket.py`, `cli.py`, and the four
package/version initializer files charged to the CLI row. Preserve protocol transcript
equivalence, one request per connection, 8 MiB Herdr frames, ambient-socket isolation,
timeout/EOF/correlation behavior, and complete resource reaping. Preserve CLI golden
argv/request JSON, mutation ambiguity with no fallback, exact exits 0/1/2, JSON-only
stdout, and packaging/import behavior. Variant D inherits the measured P0/D CLI baseline
and credit, then P5 proves the residual CLI/core target without remeasuring or
double-crediting D-A or D-B.
The preserve planning points are Herdr socket 350 and CLI/core 470. Variant D's exact
13,500 allocation requires Herdr socket at most 350 and CLI/core at most 400; the latter
is 48 below the D consolidation point and is an independent unmeasured spike.

## Sequencing and non-overlap

1. Owner explicitly authorizes or rejects Variant D. If authorized, freeze byte-level
   fixtures and method/callback inventories and run P0/D; D-A/B measurement does not
   require the SQLite ruling because it changes no database schema.
2. Owner rules on the global target, component-band semantics, and SQLite threat scope.
   P4 cannot begin before the SQLite ruling, and no prototype is merge or release
   authority.
3. Run P1–P5 as measurements on disjoint file sets; do not merge prototypes.
4. Recompute a 39-file one-charge table from the conventional diffs. Dead helpers count
   only after every surviving caller is absent. Cross-row deletion is charged to each
   file's original row.
5. Approve task cards only if the estimate-only target is at most 13,500, or if every
   high-variance row has a measured conventional prototype producing at most 14,000 with
   an explicitly reviewed contingency. All preserved invariants must map to positive,
   negative, fault, and concurrency tests.
6. Implement one accepted wave at a time. Each wave receives a stable checkpoint and
   release rollback note before the next begins.

No SLOC estimate from two variants may be added blindly. In particular, `turn.delta` and
schema-1 listing share API restore and dispatch seams, and outbox helpers become dead only
after all surviving operation callers are checked.

## Verification contract

Every accepted checkpoint requires:

- an exact file allowlist, parent commit, clean tracked tree, `git show --check`, and
  canonical before/after counts;
- focused positive, negative, hostile-input, fault, concurrency, and replay tests for the
  touched responsibility;
- byte-for-byte differential public envelopes and signed token vectors where the public
  surface is preserved;
- the full Tendwire and Herdres suites with no new skips;
- a real daemon-socket cross-repo run covering poll, provider binding, ACK, ACK-loss
  re-poll without duplicate, final replacing working, decision delivery/answer, and
  command ingress; and
- at final completion, the preserved Telegram UX against a live provider, as required by
  the unchanged goal.

Variant D focused coverage includes turn listing, daemon/API, CLI, connector framing,
release readiness, and `test_public_content_safety.py`. Delta-only tests are deleted with
the capability, but shared schema-2 list/cursor/tamper/privacy assertions move to surviving
tests in the same commit. New negative RPC and CLI tests pin the exact D-A and D-B error
envelopes above. Existing schema-2 coverage must retain
`test_turn_list_cursor_round_trip_binds_complete_request_and_expiry`,
`test_turn_list_cursor_rejects_tamper_cross_binding_and_expiry_distinctly`, and
`test_turn_since_token_is_deterministic_strict_and_store_epoch_bound`; the daemon version
test is rewritten to require schema 2 and reject schema 1. Cursor/since bytes, signing
material, host and store-epoch binding, expiry, watermark, and list-sequence semantics
remain unchanged. Herdres compatibility runs its source-only, Tendwire client, socket
pairing, presenter, final-delivery, and readiness suites.

Static and socket-pairing tests do not satisfy the integration gate. The full-process
harness must start the production Tendwire daemon with its required ACP supervisor,
connect the authoritative Herdres socket client through a real Unix socket, and use no
injected or mocked daemon API or connector callbacks. If ACP is externally provisioned,
the artifact binds its exact version, health, adapter, and endpoint-generation evidence.
The existing socket-pairing test uses production Store/API/socket code but not the complete
production daemon lifecycle and uses a mock Telegram provider.
The live-Telegram harness reads credentials only from private configuration, records no
raw coordinates or content, and emits a recursively privacy-scanned JSON result.

ACP verification includes a contract matrix for all six supported adapters. It pins each
label, executable, fixed arguments, version and capability probe, registration timeout,
adapter hash, initialization capabilities, session open/load/resume, prompt, steering,
cancellation, permission, update/tool/plan, final, and cleanup behavior. Full-process and
live delivery evidence is mandatory for Codex, Claude, OMP, and Kimi because those are
explicit operational paths; configured Gemini and Hermes paths receive the same evidence.
A missing binary, credential, or authority—including the standing prohibition on using
Kimi as work-force—produces an explicit blocked matrix entry and cannot be reported as a
generic ACP pass.

Required closure artifacts are a Variant D contract, two-repository caller inventory,
focused and full-suite outputs, cross-repo E2E JSON, live-Telegram E2E JSON, cutover and
rollback note, final SLOC ledger, and release summary. Each records both commit IDs,
tree/package hashes, exact commands, schema/API versions, bounded resource counts, and
cleanup results.

The intended artifact names are
`wave16-variant-d-contract.md`, `wave16-production-caller-inventory.txt`,
`wave16-external-client-inventory.md`, `wave16-focused-tests.txt`,
`wave16-full-suites.txt`, `wave16-cross-repo-e2e.json`,
`wave16-live-telegram-e2e.json`, `wave16-acp-adapter-matrix.json`,
`wave16-cutover-rollback.md`,
`wave16-sloc-ledger.md`, and `wave16-release-summary.md` under `docs/evidence/`.
A fresh-schema drain/discard artifact is required only if a later Store schema rewrite is
authorized.

The task cards pin these repository-root commands:

```text
# Tendwire focused Variant D
PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_turns.py tests/test_store.py tests/test_daemon.py \
  tests/test_cli.py tests/test_connector_daemon_cli.py \
  tests/test_release_readiness.py tests/test_public_content_safety.py

# Herdres focused compatibility, from the paired Herdres root
PYTHONPATH=. /home/smith/tendwire/.venv/bin/python -m pytest -q \
  tests/test_source_only.py tests/test_tendwire_client.py \
  tests/test_tendwire_socket_pairing.py tests/test_presenter.py \
  tests/test_turn_final_delivery.py tests/test_release_readiness.py

# Tendwire full suite, from the Tendwire root
PYTHONPATH=src /home/smith/tendwire/.venv/bin/python -m pytest tests/ -x -q

# Herdres full suite, from the paired Herdres root
PYTHONPATH=. /home/smith/tendwire/.venv/bin/python -m pytest tests/ -x -q

# Canonical production counts
python3 /home/smith/acp-reduction-goal/tools/sloc_count.py src --verbose
python3 /home/smith/acp-reduction-goal/tools/sloc_count.py \
  /home/smith/tendwire/.worktrees/herdres-acp-route --verbose
```

Unix-socket tests run in an environment that permits local AF_UNIX bind/connect; sandbox
`EPERM` is an environmental failure, never an accepted skip. The future full-process and
live-Telegram harness commands and their private configuration contracts must be checked
in with the harnesses rather than invented as unverifiable command stubs here.

The seven architecture failure modes remain mandatory: provider acceptance before lost
ACK, working/final races, superseded finals, rate-limit defer, split-final resume,
topic-create crash recovery, and explicit drain-or-discard on fresh-schema release.
Decision idempotency and route-generation fencing are additional required gates.

## Owner decisions

No production work resumes until the owner chooses:

1. retain the 14,000 global ceiling with measured prototypes and, if desired, Variant D;
   or preserve every public capability and amend the target after P1–P5 measurements;
2. interpret component bands as upper capacities with +15 percent hard ceilings, or keep
   symmetric ±15 percent compliance that makes smaller complete rows fail low;
3. choose the SQLite same-UID threat contract: scope hostile concurrent same-UID namespace
   replacement out, accept transient open followed by checkpoint detection, or authorize
   a custom VFS/binding and a larger dependency scope; and
4. independently authorize or reject D-A deletion of `turn.delta`; authorization requires
   re-freezing the Wave 13/14 method and callback inventory plus an external-client release
   notice; and
5. independently authorize or reject D-B retirement of turn-list schema 1; authorization
   requires the schema-2 default/output migration notice and exact rejection contract.

If D-A or D-B is not authorized, or if the SQLite semantics remain unresolved, the safe
default is to reject Variant D and keep production frozen. Until the rulings above, this
branch remains a planning checkpoint only.
