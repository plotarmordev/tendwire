# Tendwire reduction component ledger

This evidence records a mechanically auditable, non-overlapping assignment of every
production Python file to exactly one Tendwire architecture component. It measures the
production tree at `5158553`, `5e65e27`, and `f467056`. The changes between these
checkpoints after `5158553` are documentation-only, so all three references have the same
production files and the same canonical total: **20,985 SLOC**.

This document introduces no production authority, changes no production code, and does
not reclassify individual lines. Each complete file is charged once to its dominant
production responsibility. Counts use
`python3 /home/smith/acp-reduction-goal/tools/sloc_count.py src`.

## One-charge file ledger

All paths in the ledger are relative to `src/tendwire/`.

| Architecture component | Production files charged | Exact SLOC |
|---|---|---:|
| Store | `store/__init__.py` 0; `store/db.py` 186; `store/events.py` 111; `store/outbox.py` 1,773; `store/pending.py` 854; `store/projection.py` 538; `store/receipts.py` 750; `store/retention.py` 381; `store/schema.py` 286; `store/turns.py` 1,313 | **6,192** |
| ACP transport | `backends/acp_client.py` 953; `backends/acp_protocol.py` 249 | **1,202** |
| ACP runtime + coordinator + permissions | `backends/acp_coordinator.py` 1,793; `backends/acp_permissions.py` 246; `backends/acp_runtime.py` 714 | **2,753** |
| Ingestion + projection | `backends/acp_ingestion.py` 512; `backends/acp_projection.py` 645; `core/agent_events.py` 328; `core/attention.py` 136; `core/projector.py` 22; `core/turns.py` 1,018 | **2,661** |
| Command submission | `command_submission.py` 875; `core/commands.py` 505 | **1,380** |
| Daemon/API/config/identity/security | `daemon.py` 571; `daemon_api.py` 1,794; `config.py` 259; `local_state.py` 537; `worker_identity.py` 246 | **3,407** |
| Herdr socket client | `backends/herdr_protocol.py` 164; `backends/herdr_socket.py` 240 | **404** |
| Privacy + connector facade | `connectors/__init__.py` 7; `connectors/outbox.py` 688; `connectors/protocol.py` 283; `core/models.py` 1,477 | **2,455** |
| CLI + remaining core/package shell | `__init__.py` 2; `_version.py` 1; `backends/__init__.py` 0; `cli.py` 528; `core/__init__.py` 0 | **531** |
| **Total** | **39 files, each charged once** | **20,985** |

The row subtotals sum exactly to the canonical production total. Zero-SLOC package files
remain listed so the file inventory is complete rather than silently omitting them.

## Current SLOC versus architecture bands

The hard component ceiling is the published upper band plus 15 percent.

| Component | Current | Published band | Hard +15% ceiling | Result |
|---|---:|---:|---:|---|
| Store | 6,192 | 4,500–5,500 | 6,325 | 692 above band; passes hard by 133 |
| ACP transport | 1,202 | 700–900 | 1,035 | fails hard by 167 |
| Runtime/coordinator/permissions | 2,753 | 1,800–2,400 | 2,760 | 353 above band; passes hard by 7 |
| Ingestion/projection | 2,661 | 1,000–1,300 | 1,495 | fails hard by 1,166 |
| Command submission | 1,380 | 900–1,200 | 1,380 | exactly at hard ceiling |
| Daemon/API/config/identity/security | 3,407 | 1,200–1,500 | 1,725 | fails hard by 1,682 |
| Herdr socket client | 404 | 400–600 | 690 | inside band |
| Privacy/connector facade | 2,455 | 700–900 | 1,035 | fails hard by 1,420 |
| CLI/core remainder | 531 | 800–1,100 | 1,265 | 269 below band |
| **Total** | **20,985** | **12,000–15,400** | **17,710 combined** | 3,275 above combined hard ceilings; 6,985 above the 14,000 global ceiling |

There is an arithmetic defect in `ARCHITECTURE.md`: the nine published Tendwire bands
sum to **12,000–15,400**, not the printed **approximately 10,000–12,400**. The summed
component maximum also exceeds the separate 14,000 global ceiling. Component compliance
therefore cannot imply global compliance until the total row and the component bands are
made arithmetically consistent.

`GOAL.md` also leaves the meaning of its ±15 percent component tolerance ambiguous at
the lower bound. If applied symmetrically, the CLI/core remainder at 531 would fail the
800-line lower bound less 15 percent, which is 680, despite being smaller than budget and
retaining its authority. Reduction budgets should use the +15 percent upper bound as the
hard ceiling and treat lower bounds as planning guidance, not require adding code or
reclassifying files to meet a minimum.

## Mixed-responsibility charging decisions

The ledger keeps mixed files whole and makes the charging decision explicit:

- `core/models.py` contains domain models and the primary privacy sanitizer. Privacy
  enforcement dominates the file and its public projection models apply that boundary,
  so the complete file is charged to privacy.
- `core/turns.py` combines paging tokens, content projection, and pending wire models.
  The reduction plan names it as the single turn-projection owner, so the complete file is
  charged to ingestion and projection.
- `core/commands.py` is charged to command submission because canonical mutation,
  selector proof, and result envelopes protect submission and receipt semantics.
- `core/agent_events.py`, `core/attention.py`, and `core/projector.py` are projection
  inputs rather than generic CLI remainder.
- `daemon_api.py` contains protocol, bounded transport, privacy backstops, and security
  checks. Its dominant authority is the daemon boundary, so it remains whole in that row.
- `local_state.py` is not named in the published daemon row, but it owns daemon/socket
  filesystem security. Charging it elsewhere would hide the security-boundary cost.
- `store/db.py` participates in the Wave 14 security trio but owns database connections
  and database security, so it remains charged to Store. Wave 14 consequently spans two
  ledger rows; that is reported rather than concealed.
- `store/outbox.py` is durable Store authority, while `connectors/outbox.py` is the
  connector facade. Similar names do not justify swapping their charges.
- `backends/acp_coordinator.py` invokes projection, but runtime lifecycle and backend
  coordination dominate it, so the complete file remains in the runtime row.

Moving privacy code into Store, pending projection into ACP, or API validation into the
CLI would not improve this ledger: the destination file would inherit the entire charge.
Only a net canonical-SLOC deletion changes the total.

## Bands that code waves can credibly close

- **Store** already passes its hard ceiling. A 692-line reduction across outbox, pending,
  turn duplication, retention, and schema can return it to the published band.
- **ACP transport** needs 167 lines to pass hard or 302 lines to reach its band. A focused
  client/protocol trim can credibly close that difference.
- **Runtime/coordinator/permissions** passes hard by seven lines and needs 353 lines to
  reach its band. Coordinator lifecycle cleanup can credibly close it.
- **Command submission** is exactly at its hard ceiling. About 180 lines of compatibility
  and generalized-contract cleanup can return it to band, provided selector proofs and
  durable receipt state machines remain unchanged.
- **Herdr socket client** is already inside its band.
- **CLI/core remainder** is already below its nominal band under this one-charge ledger.
  Its band is capacity, not a minimum that should be filled or reached through favorable
  reassignment.

## Bands requiring owner reassessment

Three published bands are below credible conventional floors for the authority charged to
them.

### Ingestion + projection

The current row is 2,661 SLOC against a 1,495 hard ceiling. A compact implementation that
retains durable event projection, attention, cursor/content projection, and pending
observation is approximately **1,700–2,100 SLOC**. No reviewed design reaches 1,495; doing
so requires a separately reviewed architectural rewrite and may require owner-authorized
capability deletion. Merging files alone is not evidence that the band can be reached.

The evidence-backed provisional band is **1,700–2,100**.

### Daemon/API/config/identity/security

The current row is 3,407 SLOC against a 1,725 hard ceiling. The `f467056` reassessment
establishes a corrected **1,905–2,025** honest range for the four files explicitly named
in the architecture row and **345–370** for `local_state.py`. The exact one-charge range
is therefore **2,250–2,395 SLOC**. At the reassessment's planning value of 1,960 for the
quartet, the row plans to **2,305–2,330** after adding the corrected local-state range.
The old hard ceiling is impossible even if that redesign succeeds.

The evidence-backed provisional band is rounded to **2,250–2,400**. Reaching it from the
current charge requires **1,007–1,157** lines of real deletion. Adding the Store-charged
`store/db.py` honest range of 180–195 to the exact daemon-row range reproduces the
reassessment's all-six range exactly: **2,430–2,590**. This cross-row reconciliation
preserves one-charge accounting rather than hiding database security in the daemon row.

### Privacy + connector facade

The current row is 2,455 SLOC against a 1,035 hard ceiling. `core/models.py` alone is
1,477 SLOC and contains both the primary privacy boundary and essential snapshot and
binding models; the connector facade adds 978. Strict-model and facade cleanup can remove
duplication, but no reviewed path reaches 1,035. Reaching it requires a deep redesign;
relocation is forbidden by one-charge accounting, and capability deletion is an owner
option rather than a proven necessity.

The evidence-backed provisional band is **1,500–1,800**, still requiring approximately
650–950 lines of real deletion.

For accurate one-charge reporting, the CLI/core remainder may be described as roughly
**450–650 SLOC**. This is a definition correction, not permission to transfer its former
800–1,100 allocation to another component.

## Required program decision

The provisional evidence-backed bands sum to approximately **14,200–17,550 SLOC** before
any contingency. The exact upper sum using the exact 2,395 daemon-row high is 17,545;
17,550 is the rounded architecture figure. The reduction program must therefore choose
explicitly between:

1. retaining the **14,000 global ceiling**, which requires at least approximately 200
   net SLOC below the provisional low ends through authorized public-capability deletion
   or a separately proven deeper design, in addition to completing the planned waves; or
2. preserving the current public capabilities and revising the Tendwire target to an
   evidence-backed range of approximately **14.2k–17.6k SLOC**.

Independently, the owner must rule how component bands are enforced. The recommended
interpretation treats each range as an upper capacity and its upper bound plus 15 percent
as the hard ceiling. The alternative symmetric ±15 percent interpretation makes a
smaller, complete row such as CLI/core at 531 fail the 680 lower tolerance and would need
an explicit reason that does not reward adding code or reclassification.

It is not mechanically honest to preserve all current capabilities, use this non-overlap
ledger, and continue claiming that the printed 10,000–12,400 component total is reachable.
Any revised architecture table must make its component arithmetic equal its declared
global target and must continue charging every production file exactly once.

## Authority statement

This is a docs-only accounting artifact prepared on the branch whose baseline is
`f467056`, based on the unchanged production tree at `5158553` and `5e65e27`. It creates
no new runtime behavior, protocol, migration, fallback, or security authority. There are
no production edits in this checkpoint. This ledger authorizes no production wave and no
component-band or global-target change. Reduction work pauses after this documentation
checkpoint pending the owner's global-target, component-band, and budget-semantics
rulings.
