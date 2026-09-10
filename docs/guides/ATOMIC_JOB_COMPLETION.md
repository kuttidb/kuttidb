# Atomic job completion guide

Finish a Queue job, save its durable result, and queue what comes next — in
one durable operation. Retrying the same completion returns its original
result without repeating its effects, including after a restart, while its
receipt is retained.

- Design and guarantees: [ATOMIC_JOB_COMPLETION.md](../design/ATOMIC_JOB_COMPLETION.md)
- Wire details: [PROTOCOL.md](../design/PROTOCOL.md)
- Queue semantics: [QUEUES.md](../messaging/QUEUES.md)

## 1. When to use this instead of separate calls

Without the feature, a worker that finishes a job must (1) write its result
into the evictable cache, (2) publish the next message, and (3) ACK the
input — three separate operations. A crash between them leaves a partial
result: the next job may exist without its input result, or the input may be
ACKed with nothing saved. Atomic job completion commits all of it together
with a retryable receipt.

Use it whenever the result is correctness-critical: order processing,
billing steps, pipeline checkpoints, deduplication-sensitive integrations.
Use the evictable cache for anything that may disappear under memory
pressure; use durable state for values that must not.

## 2. Enable the feature

```sh
./kuttidb 7379 kuttidb.wal --queue-wal kuttidb.wal.queues --job-completion
```

`--job-completion` requires a durable Queue WAL (the completion commit is a
Queue WAL record). Optional budgets: `--job-state-max-memory-mb` (default
64), `--job-receipts-max-memory-mb` (64), `--job-receipts-max-count`
(100000), `--job-receipt-retention-ms` (86400000 = 24 h),
`--job-completion-max-bytes` (131072). Capacity pressure rejects new work
with `resource_exhausted`; it never evicts state or unexpired receipts.

Back up the data directory before first use on an existing WAL: the feature
appends new record types to the Queue WAL that older binaries do not
understand.

## 3. The worker loop (Python)

```python
from kuttidb import KuttiDBClient

db = KuttiDBClient(port=7379)
db.queue_declare("extract-pdf", durable=True)
db.queue_declare("index-text", durable=True)

# Register once per worker process; registration survives restarts and is
# NOT removed when the connection closes.
db.queue_consumer_register("pdf-worker")

delivery = db.job_consume("extract-pdf", "pdf-worker", visibility=60.0)
if delivery is None:
    raise SystemExit(0)  # queue empty

text = extract_pdf(delivery.value)  # user computation, outside KuttiDB

# Compose the intent: the operation id is generated ONCE, here. Persist it
# (with the semantic request) BEFORE submitting if you need crash-safe
# recovery of the submission itself.
intent = delivery.to_intent(
    state_key="pdf:42",          # exact bytes; here: ascii
    expected_version=0,          # 0 = create the state entry
    state_value=text,
    output_queue="index-text",
    output_incarnation=out_inc,  # from queue_manifest()
    output_value=b"pdf:42",
)
persist_intent(intent.to_json())

result = db.job_complete(intent, proof=delivery.proof)
print(result.commit_id, result.state_version, result.output_message_id)
```

What the server commits together: the durable state PUT (`pdf:42` at the
checked version), the ACK of the consumed `extract-pdf` message, the publish
of one message to `index-text`, and the completion receipt. **Do not ACK the
input separately after a success** — the completion was the ACK.

## 4. Lost responses and restarts

A timeout or dropped connection after submitting leaves the outcome unknown.
Recovery always uses the SAME intent (id + semantic request):

```python
saved = load_intent()                       # JobCompletionIntent.from_json(...)
try:
    result = db.job_complete(saved, proof=stale_proof)  # proof may be stale
    print(result.commit_id, result.replayed)  # replayed=True after recovery
except JobIdempotencyConflictError:
    ...  # the id was reused for a different request: stop and investigate
```

- If the original attempt committed, the retry returns the original result
  with `replayed=True` — even after a server restart, even with an expired
  delivery, even after the output was already consumed, and without
  re-publishing anything.
- If it did not commit and the delivery has expired, the server refuses with
  `delivery_expired`: receive a fresh delivery of the same message and
  compose a NEW intent for it (the stable message id stays the same; the
  attempt is different).
- You can always ask first: `db.job_completion(operation_id)` returns the
  retained receipt without needing any delivery proof. A miss means "no
  retained receipt", never "never executed".

Receipts are retained for `--job-receipt-retention-ms` (24 h default).
After expiry the receipt is forgotten and lookup returns absent; re-submitting
the old id then needs a live delivery and the current state version, exactly
like any first attempt.

## 5. Version-checked durable state

```python
db.state_put("pdf:42", b"corrected", expected_version=1)   # exact CAS
db.state_put("pdf:42", b"created", expected_version=0)     # create-only
st = db.state_get("pdf:42")       # {"version": 2, "commit_id": 5, "value": b"..."}
db.state_delete("pdf:42", expected_version=2)
```

Every mutation takes an operation id (generated when omitted) and returns a
durable receipt; retrying the same mutation by id returns the retained
receipt. A stale `expected_version` answers `state_version_conflict` —
re-read and decide; the server never overwrites blindly, including through
the Management API. Deleting consumes a version (delete/recreate never
revives an old one). Versions, commit ids, and message ids are 64-bit and
delivered losslessly (decimal strings in JSON, native ints in the SDKs).

## 6. Same-Queue outputs and capacity

An output may target the input Queue (a self-perpetuating job chain). The
server admits it on the projected net depth: consuming the input offsets the
produced output, so a queue at its maximum depth still accepts a net-zero
completion. A distinct output Queue without room rejects the whole
completion (`resource_exhausted`) and leaves the input unacknowledged.

## 7. Error handling map

| Error | Meaning | Worker behavior |
|---|---|---|
| `unsupported_feature` | server lacks the feature / flag off | Upgrade or enable; never emulate with separate writes. |
| `validation_failed` / `request_too_large` | rejected before any attempt | Fix the request. |
| `idempotency_conflict` | id reused with different content | Stop; never auto-generate a new id. |
| `state_version_conflict` | CAS rejected | Re-read state; decide deliberately. |
| `delivery_expired` / `delivery_not_owned` | fencing refused | Obtain a current delivery; never guess proofs. |
| `resource_exhausted` | capacity refused (nothing written) | Retry later with the same intent. |
| `operation_in_doubt` | outcome unknown | Same-id lookup or exact retry only; preserve the request. |
| `persistence_unavailable` | storage cannot admit | Includes whether the outcome is unknown. |

All clients raise typed errors carrying the stable code and the outcome
(`not_committed` vs `unknown`). See
[CLIENT_FEATURE_MATRIX.md](CLIENT_FEATURE_MATRIX.md) for the per-language
method mapping and runtime coverage.

## 8. Management API and console

The Management API exposes the same core: durable-keyspace inventory and
entry PUT/DELETE (`/keyspaces/durable/...`), completion submission and
receipt lookup (`/job-completions`, `/durable-operations`), and
completion-capable deliveries via `/queue-consumers/{id}/deliveries` with
`{"mode":"completion"}`. Receipt lookup is authenticated but never requires
the old delivery proof. The console's Durable state and Atomic operations
views cover inspection, deliberate consumption, completion, and
operation-id recovery after a reload.
