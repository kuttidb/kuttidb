# Atomic job completion — design

Status: implemented (this document describes shipped behavior).
Related: [DURABILITY.md](DURABILITY.md), [PROTOCOL.md](PROTOCOL.md),
[QUEUES.md](../messaging/QUEUES.md), ADR 0002 (`docs/adr/0002-atomic-job-completion-on-queue-wal.md`),
guide: [ATOMIC_JOB_COMPLETION.md](../guides/ATOMIC_JOB_COMPLETION.md).

## 1. What the feature guarantees

The core user operation:

> Finish this input Queue job, write its durable result state, and optionally
> create its next Queue job, as one durable completion. Retrying the same
> completion returns its original result without repeating its effects.

One commit records four effects together: a version-checked durable-state
PUT, the input message ACK, an optional output publish with its pre-reserved
message id, and the completion receipt. The guarantee holds on intact storage
for one KuttiDB storage instance and while the receipt is retained. It is not
exactly-once computation, an external database transaction, node-loss
protection, or an unlimited-duration deduplication promise: user computation
and external side effects are outside the transaction, and later explicit
state deletion, Queue purge, consumption, or receipt expiry are later
operations, not violations.

Normative guarantees, all covered by tests named in section 8:

1. **Atomic effects** — no externally usable partial result: the output
   message becomes consumable only under the same lock window that publishes
   the state and the receipt.
2. **Durable acknowledgement** — the success reply is released only after the
   commit record's fsync completes. Cache `--durability periodic` never
   weakens this: the commit authority is the Queue WAL, not the cache WAL.
3. **Retry identity** — the same completion ID and same semantic request
   return the original result while the receipt is retained. `replayed` may
   differ between the first success and a matched retry; commit ID, state
   version, output message id, and timestamps never change.
4. **Conflict** — a retained ID reused for a different semantic request is a
   typed `idempotency_conflict` and produces no new effects.
5. **Fencing** — an uncommitted completion needs a valid current delivery
   proof; an expired, NACKed, ACKed, purged, deleted, or superseded attempt
   cannot commit.
6. **Replay before lease validation** — a matching committed receipt is
   returned before consulting the now-stale delivery; restart retries work
   with any (or no) live proof.
7. **No state eviction** — memory pressure rejects new protected work
   (`resource_exhausted`) instead of evicting durable state or unexpired
   receipts.
8. **No resurrection** — recovery never republishes a consumed output,
   restores a deleted Queue, or overwrites a newer state value; ordinary
   later ACKs and state writes win on replay.
9. **One core** — the native protocol, the Management API, the C companion
   library, every SDK, and the console gateway all drive the same core
   functions and the same receipt ledger.
10. **Failure honesty** — a failed append/fsync after an attempted commit
    leaves the outcome unknown (`operation_in_doubt`); the engine latches
    failed rather than serving a misleading partial state.

## 2. Durable state (`durable` keyspace)

A fixed, non-evictable keyspace separate from the evictable cache `default`.
It shares no eviction eligibility, TTL sweeping, cache reset, cache WAL
semantics, or mmap write paths with the cache. Entries are exact bytes
(empty values valid), keyed by exact bytes (1..65535). Operations:

- `state_get(key)` → `{value, version, last_commit_id}` or absent.
- `state_put(key, value, expected_version, operation_id)` → mutation receipt.
- `state_delete(key, expected_version, operation_id)` → mutation receipt.
- Atomic completion embeds the same checked PUT primitive inside the commit
  (no separate public request).
- Bounded metadata enumeration for the Management API inventory.

Versions are nonzero 64-bit values allocated from a persisted high-water
mark. `expected_version = 0` creates only; a positive value must match
exactly; there is no unchecked overwrite path. Deleting an entry consumes
the next version as a tombstone high-water mark, so delete/recreate can never
validate an old version again (ABA prevention). Counter overflow latches the
engine failed instead of wrapping. No TTL exists on durable state; TTL fields
are rejected. A state conflict leaves the input unacknowledged and publishes
no output.

Standalone DELETE requires the entry's current positive version; deleting an
absent entry without a retained receipt is a definite not-found with no
mutation, while retrying a previously committed delete by the same ID returns
its retained receipt even though the entry is now absent — receipt lookup
precedes existence validation. Deleting state never deletes a completion
receipt.

## 3. Identity, proofs, and receipts

- **Store id**: 16 random bytes persisted in the Queue WAL at first feature
  use (`LOG_JOB_META`); scopes all identities.
- **Queue incarnation**: a monotonic per-store counter assigned at every
  Queue creation, persisted per Queue in checkpoints (`LOG_QUEUE_META`) and
  reproducibly reassigned during replay. Delete+recreate always yields a new
  incarnation. The stable input identity is
  `(store_id, queue_incarnation, message_id)`; names are locators only.
- **Delivery proof**: 16 random bytes (urandom; TLS-disabled builds keep
  strong randomness) bound to the live delivery, its native owner, and the
  process epoch. Proofs live in a bounded registry; a committed completion
  retires its proof (one-use). Proofs are never persisted in receipts,
  logged, or exposed in STATS.
- **Completion ID** (`operation_id`): a caller-generated UUID identifying one
  logical intent. SDKs generate it once at intent composition; retries must
  retain it.
- **Receipt**: `{kind, request digest, commit_id, state_version, input/output
  identity, completed_at, receipt_expires_at}` retained in a bounded ledger
  until its absolute wall-clock deadline (`--job-receipt-retention-ms`,
  default 24 h). Deadlines never shorten on restart or retry; retention
  policy changes never shorten stored deadlines. Bounded incremental GC
  (bucket-cursor, ≤64 buckets/pass) forgets only expired receipts; after
  forgetting, lookup returns definite "no retained receipt" — never "never
  executed".

### Canonical semantic identity

The semantic request is canonicalized in the core after transport decoding
into a versioned byte string (`"KJS1"` domain tag):

```
"KJS1" kind:1 op_id:16 store_id:16
in_present:1 [ in_qlen:2 in_name in_incarnation:8 in_msg_id:8 ]
klen:2 key expected_version:8 vlen:4 value
out_present:1 [ out_qlen:2 out_name out_incarnation:8 out_vlen:4 out_value ]
```

SHA-256 (self-contained; works in `TLS=0` builds; known-answer vectors
tested) digests these bytes into the receipt. Excluded by construction:
transport request ids, JSON property order, trace fields, and the ephemeral
delivery proof — so a retry with a renewed delivery (after a definite
uncommitted attempt) still matches, and lookup after restart works. The
digest decides same-request replay versus `idempotency_conflict`; the
Management API's non-cryptographic request hash is never used as proof of
semantic equality.

## 4. Commit authority and sequence

The durable Queue WAL is the authoritative log (ADR 0002). Five new
versioned, CRC-checked record types extend the existing framing
(`[op:1][durable:1][name_len:2][len:4][id:8][aux:8][crc32:4][name][data]`):

| op | record | purpose |
|---|---|---|
| 17 | `LOG_JOB_STATE` | one durable-state mutation (result value + version + receipt metadata) |
| 18 | `LOG_JOB_COMPLETION` | the complete completion commit: state PUT, input message id, output queue + pre-reserved message id + payload, receipt metadata |
| 19 | `LOG_JOB_RECEIPT` | receipt-only restore (checkpoint emission; no effects) |
| 20 | `LOG_JOB_META` | store id + allocation high-water marks (message ids, incarnations, state versions, commit ids) |
| 21 | `LOG_QUEUE_META` | one queue's incarnation |

Runtime commit sequence (single synchronous path; group fsync is an
explicitly deferred optimization):

1. Resolve input/output queues under the metadata lock (durable, live,
   incarnations match) and release.
2. Receipt pre-check under the job-state lock: a matching receipt returns the
   original result immediately; a digest mismatch is an `idempotency_conflict`.
3. Take the input and output Queue locks in creation order (deduplicated when
   they are the same Queue). Re-check the receipt (a concurrent same-ID
   commit may have won the race), then fence the delivery: the message must
   be in-flight with the same delivery tag and owner and an unexpired
   (monotonic) visibility deadline. The Queue lock is held across the whole
   commit, so the visibility reaper can never reassign an admitted attempt;
   expiry after admission finishes while the attempt is reserved.
4. Net-capacity admission: a distinct output Queue must have room for one
   more message; same-Queue output admits on the projected (net) depth. A
   full output Queue never causes an input ACK.
5. Reserve the output message id, state version, commit id, and receipt slot;
   pre-build every post-commit object (state application plan, receipt,
   output message). Nothing that can fail remains after the record becomes
   durable.
6. Encode one complete `LOG_JOB_COMPLETION` record and append it with fsync
   under the WAL lock (no interleaved writers). This is the commit point.
7. Apply the in-memory effects under the same lock window — engine side
   (state entry + receipt + proof retirement), then queue side (output
   insert, input removal) — release the locks, and only then reply.

Lock order (total, documented in ARCHITECTURE.md):
metadata → Queue locks (creation order) → job-state lock → job-completion
proof lock → WAL lock. The completion path takes the job locks twice (the
receipt-only pre-check without Queue locks, then under the Queue locks);
direct state mutations take only the job-state lock; checkpoints take the
metadata and every Queue lock before the job locks. No reverse path exists
(dead-letter routing keeps its pre-existing trylock exception).

Direct state PUT/DELETE follow the same pattern minus Queue locks: receipt
first, CAS validation, budgets, reservation, one `LOG_JOB_STATE` record with
fsync, then in-memory application.

## 5. Recovery and checkpoints

Replay applies records in WAL order and never re-validates CAS versions or
leases: a surviving record is committed history. A valid completion record
applies all its effects together (engine state + receipt first, then the
output insert, then the input removal — the output is never visible before
its state); a torn or CRC-failing terminal record applies none and is
truncated by the pre-existing tail rule. Missing resources are skipped
leniently so replay stays monotone across later deletes, purges, and
recreations; a hook failure refuses the open instead of truncating.

Distinguishing unsupported format from corruption: a CRC-valid feature record
whose encoding version (`fmt`) is unknown refuses startup with an
"unsupported job record format" error and preserves the file byte-for-byte.
Job records found in a WAL opened with the feature disabled refuse startup
with an enablement instruction — never silent truncation. No promise of
recovery from arbitrary middle-of-log media corruption is introduced.

Checkpoints extend the existing crash-safe rewrite: the emitted stream now
contains `LOG_JOB_META` (identity + high-water marks) first, each queue's
declaration followed by its `LOG_QUEUE_META`, retained messages, live state
entries (`LOG_JOB_STATE` with `expires_at = 0` — receipts are never
resurrected by state records), unexpired receipts (`LOG_JOB_RECEIPT` —
historical results only, never republish commands), and all pre-existing
exchange/consumer/transaction records. State and receipt bytes are included
in the live-footprint trigger so completion history still folds the WAL.
Ordinary Queue ACKs after a completion still win on subsequent replay; later
state updates and deletes are preserved.

First-use format transition: the feature is off by default; legacy WALs are
untouched until `--job-completion` is supplied. First use appends the
`LOG_JOB_META` identity record eagerly (before any other feature record) and
works on legacy files. Older binaries do not understand the new record types
and will treat the affected WAL suffix as a torn tail — back up before
enabling, and never test downgrade against the only copy of data.

## 6. Fencing and delivery proofs

`job_consume` (op 0x70) requires a registered durable named consumer and a
durable queue; it delivers through the ordinary consumer path (same
prefetch, durability, and visibility rules) and registers a proof bound to
`(epoch, queue incarnation, message id, delivery tag, owner)`. The commit
fence re-validates the live delivery under the input Queue lock: same
message, in-flight, same tag and owner, unexpired monotonic deadline. An
expired proof answers `delivery_expired`; a missing or mismatched one
answers `delivery_not_owned`. Proofs are one-use: the winning commit retires
its proof, and any competing worker fails its live-delivery check. Pool size
> 1 is safe by construction (a named owner plus validated opaque proof, not
a socket), and tested.

## 7. Budgets and configuration

| Flag | Default | Accepted range | Contract |
|---|---|---|---|
| `--job-completion` | off | flag (no value) | Enables the feature; requires a durable Queue WAL (refuses `-`/missing with exit 2). |
| `--job-state-max-memory-mb` | 64 | 1..65536 | Rejects growth, never evicts. |
| `--job-receipts-max-memory-mb` | 64 | 1..65536 | Receipt + index + digest bytes. |
| `--job-receipts-max-count` | 100000 | 1..100000000 | Additional ceiling; the byte ceiling may bind first. |
| `--job-receipt-retention-ms` | 86400000 | 1000..315360000000 | Deadline assigned per receipt at commit (wall clock). |
| `--job-completion-max-bytes` | 131072 | 1024..67108864 | Aggregate canonical operation bound (keys, metadata, state and output payload) for completions and direct state mutations. |

Accounting includes object and index overhead. Pressure answers
`resource_exhausted` (definitely not committed) instead of evicting.
Lowering a budget below retained data refuses startup with an actionable
error rather than dropping committed state. Cross-restart deadlines assume a
correctly maintained system clock; in-process fencing uses the monotonic
clock. Enabling the feature does not relax any ownership lock. The flags are
propagated through `kuttidb ensure` (boolean `--job-completion` plus the five
value flags) and every SDK's managed settings; readiness includes the job
engine's health, while capacity pressure is reported separately and does not
flip readiness.

## 8. Failure semantics and verification

The commit/checkpoint matrix is exercised by `job_crash_test` (failpoints
compiled only into the test binary via `KUTTIDB_JOB_FAILPOINTS`; production
builds never read the variable): before append (no effects, no receipt),
after write before fsync (either all effects or none after recovery — never
partial), after fsync (all effects + receipt), after apply (retry returns the
original receipt); torn terminal records; recovery executed twice
(idempotence); output consumed → never republished; later state change →
never overwritten; checkpoint boundary (state, queues, incarnations, ids,
receipts intact); receipt GC across checkpoints; legacy first-use
transition; unsupported-format refusal with a byte-preserved WAL;
feature-disabled refusal with job records present.

Protocol-level behavior lives in `test_job_protocol.py` (raw frames) and
`test_job_client.py`/`test_job_client_c.c` (SDK level), including the full
vertical scenario: declare → consume → complete → lost response → restart →
same-id replay returning the original commit ID, state version, and output
message id — after checkpoints, after the output was ACKed, and after the
state changed. Same-ID/different-request conflicts, different-ID races for
one delivery, same-Queue output at max depth, and full-output-queue
rejections are covered in `job_completion_test`.
