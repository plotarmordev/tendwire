# Goal 13: Low-latency turn delta sync

Status: urgent, not implemented

Owners: Tendwire and Herdres

## Problem

Herdres must currently traverse the complete retained schema-v2 turn list on
every source sync. The existing `twsince1.*` token is an insertion watermark;
it cannot report mutations to an existing turn, including Working progress and
the transition to a final response. Herdres therefore cannot use it for live
updates without missing state changes.

Production evidence collected on 2026-07-16:

- 348 retained turns required four 100-row API pages.
- A sequential cached traversal took approximately 11.3 seconds before any
  Telegram work.
- An otherwise idle source pass took approximately 25 seconds end to end.
- Under the former two-second ingestion cadence and stale client pairing, CPU
  contention and repeated traversals produced user-visible delays measured in
  minutes.
- Telegram HTTP timeouts can add independent provider delay and must remain
  distinguishable from Tendwire source delay.

Increasing page size is only a bounded mitigation. Retention is intentional,
and deleting history to make the live path fast is not acceptable.

## Required design

Tendwire must expose a cache-only, host-scoped change feed for the current turn
projection. It must report insertions, updates, and deletions using a durable
opaque mutation watermark. It must not parse source sessions or depend on the
insertion-only list sequence.

The change contract must provide:

- A stable initial bootstrap from schema-v2 turn data.
- A durable token that advances whenever a publicly observable turn projection
  changes, including Working text, status, current content revision, ownership,
  and removal.
- Bounded pages below the daemon's fixed one-MiB frame limit.
- Explicit tombstones for removals so consumers do not retain stale cards.
- Atomic page/watermark reads with deterministic ordering.
- Explicit invalid, expired, cross-host, and incompatible-schema outcomes.
- No private binding, path, pane, session, Telegram, or backend identifiers.
- No canonical long-content copies in the change feed; Goal 05 content paging
  remains authoritative.
- No weakening of Goal 10 final-ready roots, delivery acknowledgments,
  retention, or replay protection.

Herdres must persist the accepted change watermark with its local state and use
the feed after one successful bootstrap. It must checkpoint a new watermark
only after the complete bounded change batch is durably applied. Invalid or
expired state may trigger one bounded full bootstrap; transport ambiguity must
not trigger a second source observation.

Final delivery remains outbox-driven. Turn changes may update Working cards and
local projections, but a list/change row must not independently authorize a
final Telegram send.

## Operational behavior

- An unchanged sync performs no full-history traversal, canonical page fetch,
  Telegram operation, or Tendwire mutation.
- A single active turn update reads only bounded changed rows, independent of
  retained-history size.
- Provider timeouts are reported separately from source/change-feed latency.
- Aggregate health exposes bounded counts and timings only; it must not expose
  turn identities or private routing data.
- Restart resumes from the durable watermark without replaying acknowledged
  Telegram operations.

## Acceptance

1. Seed at least 10,000 retained historical turns and multiple active workers.
2. Bootstrap Herdres once, then perform two unchanged forced syncs.
3. Prove both unchanged syncs read zero full-list pages, zero content pages, and
   perform zero provider operations.
4. Update one existing Working turn without inserting a turn row. Prove the
   next delta contains that update exactly once.
5. Complete that turn. Prove the Working card is finalized through the accepted
   Goal 10 delivery path exactly once.
6. Remove a current turn and prove one tombstone removes stale local state.
7. Insert and mutate rows concurrently with pagination. Prove every change is
   observed exactly once or safely repeated, with no omission.
8. Lose an ACK/checkpoint response, restart both processes, and prove no
   provider operation is replayed.
9. Expire or invalidate a watermark and prove one bounded bootstrap restores a
   correct state.
10. Measure cached no-op and one-turn-update latency through the production Unix
    socket on the recorded four-core deployment class. Target p95 is at most
    350 ms for Tendwire API work and at most five seconds from a durably ingested
    change to Herdres beginning the corresponding provider operation, excluding
    a separately reported provider timeout.
11. Run complete Tendwire and Herdres suites, compilation, public-safety scans,
    and a paired installed-candidate benchmark.

## Non-goals

- Reducing retention merely to hide traversal cost.
- Polling private source files from Herdres.
- Raising the daemon frame cap.
- Replacing Goal 05 canonical paging or Goal 10 final-delivery authority.
- Claiming provider-perfect exactly-once delivery after an unrecorded Telegram
  acceptance.
