# Client feature matrix

Atomic job completion coverage per client. All surfaces drive the same core
(server opcodes 0x70–0x77, capability bit 16, protocol 1.8+) and share the
same semantic behavior: stable serializable intent, lossless 64-bit values,
typed errors with `not_committed`/`unknown` outcomes, no automatic retries,
and no fallback emulation with separate writes.

Method family mapping (frozen; all languages cover the complete feature):

| Surface | Consume | Complete | Receipt lookup | State get/put/delete | Mutation lookup | Queue identity |
|---|---|---|---|---|---|---|
| Python (`KuttiDBClient`) | `job_consume` | `job_complete` | `job_completion` | `state_get` / `state_put` / `state_delete` | `durable_operation` | `queue_manifest` |
| Node.js (`Client`) | `jobConsume` | `jobComplete` | `jobCompletion` | `stateGet` / `statePut` / `stateDelete` | `durableOperation` | `queueManifest` |
| Go (`Client`) | `JobConsume` | `JobComplete` | `JobCompletion` | `StateGet` / `StatePut` / `StateDelete` | `DurableOperation` | `QueueManifest` |
| Java (`KuttiDBClient`) | `jobConsume` | `jobComplete` | `jobCompletion` | `stateGet` / `statePut` / `stateDelete` | `durableOperation` | `queueManifest` |
| Rust (`Client`, `Pool`) | `job_consume` | `job_complete` | `job_completion` | `state_get` / `state_put` / `state_delete` | `durable_operation` | `queue_manifest` |
| C/C++ (`libkuttidb_client`) | `kuttidb_job_consume` | `kuttidb_job_complete` | `kuttidb_job_completion` | `kuttidb_state_get` / `kuttidb_state_put` / `kuttidb_state_delete` | `kuttidb_durable_operation` | `kuttidb_queue_manifest` |
| CLI (`kuttidb-cli`) | `job-consume` | `job-complete` | `job-completion` | `state-get` / `state-put` / `state-delete` | `durable-operation` | `manifest` |
| Management API | `POST /queue-consumers/{id}/deliveries` (`mode:"completion"`) | `POST /job-completions` | `GET /job-completions/{op}` | `PUT` / `DELETE` / `GET /keyspaces/durable/entries/{id}` | `GET /durable-operations/{op}` | `GET /queues/{id}` |

## Semantics common to every client

- **Capability negotiation:** the job family requires capability bit 16
  (protocol ≥ 1.8). Unsupported/disabled servers answer the typed
  `unsupported_feature` error; no client falls back to separate
  PUT/publish/ACK calls.
- **Intent:** `JobCompletionIntent` (per-language name above) carries the
  caller-owned operation id (16 bytes / UUID; generated once at composition)
  plus the full semantic request. JSON serialization is lossless: 64-bit
  identity/version fields are decimal strings (tested beyond 2^53), byte
  spans are Base64, `output: null` encodes the absent output. The ephemeral
  delivery proof is never serialized.
- **Receipts:** `JobCompletionResult` (submit) and `JobReceipt` (lookup)
  expose `commit_id`, `state_version`, `output_message_id`, `completed_at`,
  `receipt_expires_at`; submit adds `replayed`. All immutable across
  retries.
- **Errors:** codes `unsupported_feature`, `validation_failed`,
  `request_too_large`, `idempotency_conflict`, `state_version_conflict`,
  `delivery_expired`, `delivery_not_owned`, `resource_exhausted`,
  `operation_in_doubt`, `persistence_unavailable`, `not_found` with outcome
  `not_committed`/`unknown`. `operation_in_progress` is reserved.
- **Managed mode:** every SDK's managed constructor propagates
  `--job-completion` (boolean) plus `--job-state-max-memory-mb`,
  `--job-receipts-max-memory-mb`, `--job-receipts-max-count`,
  `--job-receipt-retention-ms`, `--job-completion-max-bytes` through the
  `kuttidb ensure` allowlist. Enabling requires a durable Queue WAL.
- **Named consumers:** `job_consume` requires a durable named consumer
  (register once via the existing consumer APIs or the CLI
  `consumer-register`). Registration survives restarts; closing a connection
  never unregisters.

## Per-client notes

| Client | Transports | 64-bit handling | Tests |
|---|---|---|---|
| Python | TCP, Unix, TLS, managed | native ints (arbitrary precision); JSON decimal strings | `test_job_client.py`, `test_job_protocol.py` |
| Node.js | TCP, TLS, managed | `BigInt` on the wire; decimal strings in intent JSON | `clients/nodejs/job_smoke.js` |
| Go | TCP, managed | `uint64`; JSON decimal strings (`OperationID` UUID) | `clients/go/job_test.go`, managed integration |
| Java | TCP, TLS, managed | `long` + decimal strings in JSON | `clients/java/JobSmoke.java` |
| Rust | TCP, managed | `u64`; decimal strings in JSON | `clients/rust/tests/job_completion.rs` |
| C/C++ | TCP, Unix, AUTH, verified TLS (OpenSSL builds) | `uint64_t` | `job_client_test`, C++ link smoke `job_client_cpp_test` |
| CLI | TCP, Unix (`--unix-path`), TLS | decimal strings in JSON output; exit 3 conflict / 4 unknown outcome | exercised via `test_management_api.py` fixtures and manual flow |
| Management API | HTTPS/HTTP loopback + console gateway | decimal strings (`b64u:` ids, Base64 payloads) | `test_management_api.py` job section |

## Unsupported / not in this release

- No native Windows server (unchanged; see [PROTOCOL.md](../design/PROTOCOL.md)).
- No embedded/shared-memory completion path: `libkuttidb_embed` stays
  cache-only; C applications open `libkuttidb_client` to the same server for
  durable work.
- No receipt purge API (a receipt lives exactly for its promised retention
  window).
- Multi-key transactions, Exchange/Stream participation, multiple input ACKs,
  and TTL on durable state are explicitly out of scope (v1).
