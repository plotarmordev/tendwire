# Connector outbox delivery-lane recovery

## ACK deadline and ordered recovery

`final_ready` sources enter `awaiting_ack` only after their presentation plan is
committed. The outbox now stores the bounded ACK deadline in both the private
state and `next_attempt_at`. The daemon's one-second periodic connector sweep
therefore sees the deadline without waiting for another consumer poll.

When the deadline expires, the sweep:

1. leaves a still-valid leased part alone;
2. fails the abandoned presentation generation;
3. retains delivered prefix evidence;
4. moves the source to `retry` with exponential backoff capped at 30 seconds;
5. keeps the same source `delivery_key` and ordering key.

A legacy or restart-orphaned `awaiting_ack` row without any deadline is treated
as already overdue. It cannot remain a non-terminal row without a future sweep.
Malformed-only ACK or lease deadline state is handled the same way; when one
stored deadline is malformed, a second valid persisted deadline still wins.
Likewise, a malformed retry/defer availability timestamp is treated as due
rather than permanently future.
The retrying source remains the lane head, so a later final on the same ordering
key cannot pass it during backoff.

Redelivery is safe at the consumer boundary. Herdres
`herdres_connector/state.py` stores turn-final receipts by the stable outbox job
key in `find_tendwire_turn_job` and reserves the immutable intent
idempotently in `reserve_tendwire_turn_job`; transient lease refs are explicitly
not ledger keys. The consumer checkpoints `telegram_applied` before ACK.
Herdres regression coverage includes
`test_provider_acceptance_crash_persists_reserved_and_restart_never_resends`
and
`test_restart_reconciles_committed_last_part_ack_without_turn_list_or_resend`
in `tests/test_turn_final_delivery.py`.

## Drain health

Tendwire's daemon reclaim cadence is one second and reclaim is not pass-capped.
The Tendwire poll boundary accepts up to 100 rows, and a regression test leases
100 independent due ordering lanes in one pass before the 30-second target.

The production consumer inspected alongside this change runs
`herdres sync --loop 5`; its `SyncRuntime.max_sends` default is 8, shared with
ordinary turn sends, and `_drain_turn_final` polls one job at a time. That
consumer budget can take more than 60 seconds for a 100-item backlog even when
all ordering keys are unblocked. Tendwire health now reports:

- due pollable rows;
- the oldest due timestamp (retry/defer age starts when the row becomes due);
- overdue `awaiting_ack` rows;
- a `starved` flag after 30 seconds.

A starved outbox degrades daemon health instead of allowing hours of silent
queue age. The consumer-side budget/cadence must also be raised in Herdres to
meet the end-to-end target; Tendwire cannot make the external Telegram consumer
perform more physical operations.

## Dead-letter autopsy and recovery boundary

A read-only production snapshot taken after the reported 648-row count had 658
cumulative dead-letter rows:

| Structural class | Rows | Latest recorded outcome |
| --- | ---: | --- |
| Failed presentation-plan parts | 429 | 375 retry scheduled, 32 missing, 15 deferred, 7 expired |
| Retryable final anchors | 130 | 117 deferred, 13 attempts exhausted |
| Historical migration holds | 98 | no delivery attempt |
| Other generic work | 1 | expired |

The count was cumulative, not "created today": 424 of those rows were updated
on 2026-07-23 and 8 on 2026-07-24 at inspection time.

Failed plan parts are audit evidence and must not be resurrected independently;
doing so could reorder a presentation suffix. Their recovery is the source
retry or explicit failed-plan recovery, which creates a new ordered generation.
Final anchors and eligible migration holds use the existing bounded
`connector.retry` operation, one exact delivery key/final identity at a time.
That operation revalidates the authoritative current revision and returns
`stale_revision` or `not_retryable` instead of reviving unsafe history.

`connector.inspect` now returns structural classification counts and the
allowed recovery boundary. Tendwire also retains the consumer's safe,
enumerated turn-final reason codes instead of collapsing all future failures to
`unknown`, so subsequent autopsies can distinguish routing churn, revision
conflicts, rate limiting, and delivery uncertainty.
