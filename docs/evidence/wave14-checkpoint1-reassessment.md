# Wave 14 Checkpoint 1 reassessment

This evidence supersedes the implementation authority of the Wave 14 security-boundary
contract at Tendwire commit `5e65e27214d3f6039bbca131d19e8c63f3203fa1`.
That commit remains a mechanically valid documentation checkpoint, but its Checkpoint 1
numeric and SQLite security premises did not survive the required read-only feasibility
proof. No Wave 14 production implementation is authorized under the unchanged contract.

The production baseline remains `5158553ed8f2a49a57ffffe5a2f30d4d195f19e4` at
20,985 canonical SLOC. The six security-boundary files remain byte-for-byte unchanged
from the hashes recorded in the Wave 14 contract.

## Corrected honest floors

Moving SQLite-family ownership into `store/db.py` increases that file's natural floor.
Conventional formatting, explicit cleanup, no compatibility wrappers, and no weakened
checks produce these reviewed ranges:

| File or aggregate | Honest range | Wave 14 gate |
|---|---:|---:|
| `local_state.py` | 345–370 | planned 335 |
| `store/db.py` | 180–195 | planned 150 |
| local state + database | 525–565 | planned 485 |
| security trio | 1,555–1,685 | at most 1,575 |
| architecture quartet | 1,905–2,025 | at most 1,980 |
| all six files | 2,430–2,590 | at most 2,475 |

At the Wave 14 planning values for `daemon.py` and `daemon_api.py`, the corrected results
are:

```text
security trio     1070 + (345..370) + (180..195) = 1595..1635
quartet            385 + 1070 + 259 + 246 = 1960
all six            385 + 1070 + (345..370) + (180..195) + 259 + 246
                 = 2485..2525
```

The quartet's intermediate gate remains plausible: it requires daemon plus API at or
below 1,475, and the 1,455 planning sum passes, although the quartet still finishes 235
SLOC above the canonical 1,725 completion gate. The other unchanged gates are only
optimistic-edge possibilities, not established targets. The security-trio gate requires
`daemon_api.py` around 1,010–1,050 depending on the measured local-state/database result.
The all-six gate requires daemon plus API around 1,405–1,445, or an API around
1,005–1,075 across the planned daemon range. Incidental savings in configuration or
installation identity are not valid inputs to this proof.

## SQLite proof boundary

Descriptor-relative, no-follow path walking can pin the database parent and prevent
ancestor replacement from redirecting later operations. Main-file and sidecar identities
can be checked before connect, immediately after connect, after pragmas and sidecar
creation, before SQLite close, and after SQLite close while the parent descriptor remains
retained.

Python's standard `sqlite3` interface nevertheless gives SQLite a filename or URI. It
does not expose an open-by-descriptor API or the `SQLITE_OPEN_NOFOLLOW` flag for every
internal main, WAL, SHM, and rollback-journal open. Consequently, the approved standard
library surface cannot prove prevention of a concurrent same-UID leaf replacement at
the exact instant of an internal SQLite open. Checkpoint checks detect substitution; they
do not prevent every such race.

Production work therefore requires an explicit owner decision among these materially
different contracts:

1. Scope a hostile concurrent same-UID namespace actor outside the prevention claim,
   while retaining anchored ancestors, strict static validation, and replacement
   detection at every defined checkpoint.
2. Keep same-UID replacement inside the threat scope, but explicitly accept that SQLite
   may transiently open or operate on a substituted object before a later checkpoint
   detects it and fails closed.
3. Authorize a larger technology and dependency change, such as an openat-aware custom
   SQLite VFS or binding, and discard the current SLOC plan.

The absolute prevention claim and the standard-library-only implementation cannot both
remain requirements.

## Stopping decision

- Keep `5158553` as the latest accepted Tendwire production checkpoint and `af0e5be` as
  the ACP runtime rollback point.
- Keep the Wave 14 docs checkpoint `5e65e27` for its frozen inventory, connector-contract
  correction, and bind-first lifecycle design, but do not treat it as production
  authority.
- Do not begin its local-state/database, protocol, transport, or daemon implementation
  checkpoints until the SQLite contract and numeric gates are explicitly amended.
- Do not pressure the failed gates through packed formatting, responsibility relocation,
  compatibility wrappers, negative-test deletion, or an assumed future saving.
- Continue only read-only component-ledger and alternative-wave audits before owner
  reassessment.

This is a planned reassessment stop, not a blocker declaration for the overall reduction
goal and not a claim that the 14,000-SLOC ceiling has been reached.
