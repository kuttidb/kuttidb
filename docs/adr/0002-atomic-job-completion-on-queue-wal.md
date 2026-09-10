# ADR 0002: Atomic job completion commits through the Queue WAL

Date: 2026-09-14
Status: Accepted
Related: `docs/plans/ATOMIC_JOB_COMPLETION_IMPLEMENTATION_INSTRUCTION.md` (local, gitignored)

## Context

Atomic job completion must commit four effects together — a durable-state PUT,
the input message ACK, an optional output Queue publish, and the completion
receipt — and the commit must survive crash, restart, and checkpoint
compaction. KuttiDB already has three write-ahead logs (cache, Queue, stream)
and two transaction mechanisms:

1. Cache-plus-Queue atomic transactions: a cache-WAL commit marker written
   between Queue-WAL `TX_PREPARE`/`TX_COMMIT` records.
2. Per-engine checkpoints that rewrite each WAL when history outgrows live
   state.

## Decision

For this slice, the durable Queue WAL is the authoritative log for durable
state, receipts, and completion operations. One new versioned, CRC-checked
`LOG_JOB_COMPLETION` record contains every effect of one completion: the state
key/value, the reserved state version, the input message id, the output queue
name and pre-reserved output message id with its payload, and the full receipt
metadata. Recovery applies a valid record's effects together; an incomplete or
CRC-failing terminal record applies none. Durable state additionally gets a
dedicated `LOG_JOB_STATE` record for version-checked standalone PUT/DELETE,
and receipts get a non-effect record (`LOG_JOB_RECEIPT`) used only by
checkpoints to restore historical results without re-publishing recorded
outputs. Store identity and allocation high-water marks are re-asserted in a
`LOG_JOB_META` checkpoint record so identities and monotonic counters survive
compaction of empty stores.

The durable-state index and the receipt ledger live in new focused modules
(`src/job_state.c/.h`, `src/job_completion.c/.h`). Their locks join the
documented total order as the leaf-most mutexes before the Queue WAL lock:
metadata → Queue (creation order) → job-state lock → job completion lock →
`wal_lock`. The completion path takes the job locks twice (an early
receipt-only pre-check, then the fenced revalidation under the Queue locks);
no code path acquires a Queue lock while holding a job lock except the
completion commit and the checkpoint, which take Queue locks in creation
order first.

## Alternatives considered

- **Separate completion journal (own WAL + prepare/commit).** Rejected for
  v1: a third multi-log commit protocol multiplies recovery reconciliation
  paths for no user-visible benefit. If cross-engine atomicity (cache or
  stream participants) is ever required, revisit with a dedicated ADR.
- **Cache-WAL commit marker like the existing atomic transactions.** Rejected:
  durable state must not share eviction eligibility, TTL sweeping, cache
  reset, or cache WAL semantics, and completion must not inherit `periodic`
  cache fsync behavior. The Queue WAL already provides per-record fsync
  sequencing and group fsync.
- **Applying public PUT/publish/ACK in sequence under a wrapper mutex.**
  Rejected by the assignment: separate records permit a torn transaction.

## Consequences

- `periodic` cache durability never weakens completion acknowledgement: the
  Queue WAL fsync is the acknowledgement point for all four effects.
- Older binaries do not understand the new record types and treat the WAL
  suffix as a torn tail. This is a documented first-use format transition:
  the feature is off by default, legacy files are preserved while unused, and
  first use requires a backup. Unknown *feature* record types are rejected at
  startup as an unsupported format (clear error), not silently truncated,
  when the feature is enabled.
- When feature records exist in a Queue WAL, startup with the feature
  disabled is refused with an enablement instruction; writable storage is
  never opened while omitting existing state from checkpoints.
- Checkpoints must include durable state, unexpired receipts, store identity,
  and monotonic high-water marks (message ids, incarnations, versions, commit
  ids), or identity guarantees would degrade across compaction.
