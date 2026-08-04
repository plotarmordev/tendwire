# Tendwire connector RPC contract v2

Status: authoritative for the connector boundary implemented at Tendwire baseline
`8bc8d8b` and descendants that contain this document. Contract revision `v2` is a
document revision; every request and response that has a `schema_version` field
still uses JSON schema version `1`.

## Scope and ownership

Tendwire is the sole owner of delivery durability, ordering, leases, retry state,
dead letters, receipts, and public payload projection. A connector such as Herdres
owns provider ingress, presentation, and its provider-message binding keyed by the
Tendwire delivery key. Herdr owns worker/pane lifecycle and ACP endpoint ownership;
it is not part of this delivery protocol.

The supported cross-process boundary is Tendwire's local Unix socket. A connector
must not invoke a Tendwire CLI, spawn a subprocess, read Tendwire's database, or
read transcript/runtime files as a fallback.

## Framing and response envelopes

The socket carries one newline-terminated UTF-8 JSON request and response per
connection. The canonical request is:

```json
{"method":"connector.poll","params":{"name":"attention"}}
```

`params` must be an object. The optional top-level `id` is echoed when it passes
the daemon's public-boundary validation. No other top-level request fields are
accepted. Frames are bounded by the daemon's configured request and response
limits (1 MiB by default).

Every connector response has two distinct layers:

```json
{
  "schema_version": 1,
  "ok": true,
  "status": "ok",
  "error": null,
  "result": {
    "schema_version": 1,
    "ok": false,
    "status": "invalid_ref",
    "error": {"code": "invalid_ref", "message": "..."}
  }
}
```

The outer envelope reports daemon framing, method routing, and execution. The
inner `result` reports the connector operation. A caller must require both
`outer.ok` and `outer.result.ok`; outer `ok: true` alone never proves that an ACK
or other mutation succeeded. Invalid framing, invalid top-level parameters, an
unknown method, or an internal daemon failure produces outer `ok: false` with
`result: null`. Invalid or stale connector data produces outer `ok: true` and an
inner `ok: false` result.

## Stable identities and opaque values

- `key` is the durable delivery identity. It remains stable across lease expiry,
  reclaim, release, defer, and retry. A presentation connector must use it as its
  provider-message deduplication/binding key.
- `ref` is an attempt-scoped capability with prefix `twref1.`. It changes whenever
  a delivery is leased again. Only the ref from the current live lease may mutate
  that attempt; an older ref must be treated as stale or invalid.
- `attempt` increases when a delivery is leased again.
- `plan_token` (`twplan1.`), `content_revision` (`twrev1.`), and
  `final_identity` (`twfinal1.`) are opaque, case-sensitive values. Clients must
  preserve their exact UTF-8 bytes and must not parse, normalize, case-fold, or
  regenerate them. The same rule applies when a token is nested in `payload`,
  `turn`, `final`, or `content`.
- Connector payloads are already public, backend-neutral projections. Private
  routing, terminal, provider, and credential fields are intentionally absent.

## Required crash-recovery sequence

The correct provider-delivery transaction is:

1. Poll and receive `{key, ref, attempt, payload}`.
2. Look up the connector's durable provider-message binding by `key`.
3. If absent, send `payload` to the provider and durably store `key -> provider
   message id` before ACKing Tendwire.
4. ACK the current `ref`.

If the provider accepted the send but the ACK was lost, lease expiry followed by
`connector.poll` (or an explicit `connector.reclaim` first) returns the same
`key` and payload with a new `ref` and a higher `attempt`. The connector finds its
existing binding, does not send again, and ACKs the new ref. `connector.poll`
reclaims expired leases atomically before selecting work, so explicit reclaim is
an optional eager-maintenance operation, not a correctness requirement.

This is the only supported ACK-loss recovery behavior. Payload mutation across
attempts or provider deduplication by `ref` would violate the contract.

## Socket methods

The complete connector method set is:

```text
connector.prepare
connector.poll
connector.ack
connector.fail
connector.defer
connector.renew
connector.release
connector.reclaim
connector.retry
connector.inspect
```

There are no `turn_final_*`, `turn-final.*`, or similarly named socket methods.
Turn-final work uses the methods above with `name: "turn-final"`.

### `connector.poll`

Canonical params are `name`, with optional `limit` and `lease_seconds`. `limit`
defaults to 1 and is bounded to 1..100. The lease defaults to daemon configuration;
turn-final leases are bounded by the configured maximum, while other neutral
queues permit leases up to 86,400 seconds.

A successful inner result contains `items`. Each item contains `ref`, `key`,
`attempt`, `leased_until`, `available_at`, and `payload`; turn-final items also
carry `created_at` when available. Polling is FIFO subject to Tendwire's stored
turn-final plan and ordering constraints.

### `connector.ack`

Params are `name`, live `ref`, and optional public `response`. Success is inner
status `acknowledged`; the delivery is terminal and is not polled again. An ACK
must use the newest ref obtained after any re-poll.

### `connector.fail`

Params are `name`, live `ref`, and optional `reason`, public `response`,
`available_at`, or `delay_seconds`. It records a failed attempt and either returns
the item to the retry schedule or exhausts it according to Tendwire's configured
attempt budget. Tendwire owns that budget and final state.

### `connector.defer`

Params have the same scheduling shape as `connector.fail`. Defer releases the
current lease onto the requested future schedule without classifying the provider
operation as a delivery failure.

### `connector.renew`

Params are `name`, live `ref`, and optional `lease_seconds`. It extends the current
lease within the same queue-specific bounds used by poll and returns status
`renewed`. It does not create a new delivery identity.

### `connector.release`

Params are `name` and live `ref`. It ends the current lease and makes the durable
delivery eligible for another poll, which creates a new attempt/ref while retaining
the key and payload.

### `connector.reclaim`

Params are `name`. It expires overdue leases for that queue and reports the
`reclaimed` count. Normal poll already performs this operation transactionally.

### `connector.prepare`

Prepare is valid only for `name: "turn-final"` and requires `schema_version: 1`.
It is a strict four-action protocol:

- `begin`: `action`, `turn_id`, `content_revision`, `presentation_version`,
  `part_count`, and optional live `source_ref`.
- `part`: `action`, `plan_token`, zero-based `ordinal`, and non-empty `spans`.
  Each span is exactly `{field,start_char,end_char}`, where `field` is
  `user_text` or `assistant_final_text` and the range is a valid non-empty slice.
- `commit`: `action`, `plan_token`, and optional live `source_ref`. Commit
  materializes durable ordered delivery jobs.
- `recover`: `action`, `failed_plan_token`, and idempotency `request_id`. Recovery
  retains the acknowledged prefix and creates executable work for the remaining
  suffix according to Tendwire's stored plan state.

Each action accepts only its declared fields. Callers must retain returned opaque
tokens exactly and honor the returned inner `ok` and `status`.

### `connector.inspect`

This is the strict dead-letter query for turn-final work. Params are exactly
`schema_version: 1`, `name: "turn-final"`, `status: "dead_letter"`, and `limit`
in 1..100.

### `connector.retry`

This is the strict operator retry for a turn-final dead letter. Params are exactly
`schema_version: 1`, `name: "turn-final"`, and one selector: either the durable
revision delivery `key` or its `final_identity`. Tendwire validates the
`turn-final:revision:twfinal1.*` identity form and owns the resulting transition.

## Failure handling

Known inner failures include `invalid_params`, `invalid_ref`, stale/expired-ref
variants, `store_unavailable`, revision/plan conflicts, missing plan or delivery
state, and exhausted attempts. Callers must branch on `result.ok` and
`result.status`, not error-message text. A timeout or broken socket after a
mutation is an unknown outcome: do not assume the mutation failed. Re-poll and use
the durable-key/provider-binding rule to converge safely.

The daemon sanitizes all connector results at the public boundary. A connector
must not depend on private fields that happen to exist in Tendwire storage, and it
must reject any design that makes its own local cache authoritative for Tendwire
delivery state.
