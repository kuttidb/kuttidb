#ifndef KUTTIDB_JOB_STATE_H
#define KUTTIDB_JOB_STATE_H

/* Durable state ("durable" Keyspace) and the shared operation-receipt
 * ledger for atomic job completion.  The durable source of truth for both
 * is the Queue WAL, never the evictable cache WAL; see
 * docs/design/ATOMIC_JOB_COMPLETION.md and ADR 0002.
 *
 * Guarantees implemented here:
 *   - versions are durable, nonzero, monotonic, and never reset by delete,
 *     restart, or compaction (delete/recreate cannot validate an old
 *     version: ABA prevention);
 *   - expected_version 0 means create-only, positive values must match
 *     exactly, and no unchecked overwrite path exists;
 *   - receipts are retained until their absolute wall-clock deadline,
 *     never evicted early, never extended by retries, and their lookup is
 *     the fast path before any lease or version validation;
 *   - memory pressure rejects new protected work instead of evicting. */

#include <stddef.h>
#include <stdint.h>

#include "job_status.h"
#include "queue.h"

#ifdef __cplusplus
extern "C" {
#endif

#define JOB_ID_LEN 16u
#define JOB_DIGEST_LEN 32u

/* Non-zero when the durable outcome is unknown rather than definitely
 * "not committed" (a possibly-committed append could not be resolved). */
int job_status_in_doubt(JobStatus status);
const char *job_status_name(JobStatus status);

typedef struct JobEngineConfig {
    uint64_t state_max_bytes;        /* resident state/index budget */
    uint64_t receipts_max_bytes;     /* receipt/index/digest budget */
    uint64_t receipts_max_count;     /* additional count ceiling */
    uint64_t receipt_retention_ms;   /* deadline assigned at commit */
    uint64_t max_op_bytes;           /* aggregate canonical operation bound */
    uint32_t proof_capacity;         /* 0 selects the default bound */
} JobEngineConfig;

typedef struct JobEngine JobEngine;

/* Public receipt snapshot (fixed-size value type; no allocation). */
typedef struct JobReceipt {
    unsigned char op_id[JOB_ID_LEN];
    unsigned char kind;
    uint64_t commit_id;
    uint64_t state_version;
    uint64_t completed_at_ms;
    uint64_t receipt_expires_ms;
    char input_queue[QUEUE_NAME_MAX];
    uint16_t input_queue_len;
    uint64_t input_message_id;
    char output_queue[QUEUE_NAME_MAX];
    uint16_t output_queue_len;
    uint64_t output_message_id;
} JobReceipt;

JobEngine *job_engine_create(const JobEngineConfig *cfg);
void job_engine_destroy(JobEngine *je);
/* Effective (defaulted) configuration of an engine. */
void job_engine_config_get(const JobEngine *je, JobEngineConfig *out);

/* Bind the engine to a Queue store after a successful feature-enabled open.
 * Adopts the recovered store id or generates one and appends the durable
 * meta record; a persistence failure here latches the engine failed. */
JobStatus job_engine_attach(JobEngine *je, QueueStore *store);

/* Replay/checkpoint hooks handed to queue_store_open_ex(); every callback
 * reads or mutates only engine state. */
QueueJobReplayHooks job_engine_replay_hooks(JobEngine *je);

/* Non-zero while the feature is enabled, bound to a store, and not latched
 * failed. Readiness treats a latched failure as unhealthy. */
int job_engine_writable(const JobEngine *je);
/* Copies the stable durable-store id (16 bytes). Returns 1 when set. */
int job_engine_store_id(const JobEngine *je, unsigned char out[JOB_ID_LEN]);
/* Effective configuration accessors for capabilities/status reporting. */
uint64_t job_engine_state_budget(const JobEngine *je);
uint64_t job_engine_receipts_budget(const JobEngine *je);
uint64_t job_engine_receipts_max_count(const JobEngine *je);
uint64_t job_engine_receipt_retention_ms(const JobEngine *je);
uint64_t job_engine_max_op_bytes(const JobEngine *je);
/* Latch the persistence-failure state (outcome unknown afterwards). */
void job_engine_fail(JobEngine *je);

/* ---- Durable state ---- */

typedef struct JobStateValue {
    void *value;               /* owned by the caller; release with free() */
    uint32_t len;
    uint64_t version;
    uint64_t last_commit_id;
} JobStateValue;

/* Returns JOB_OK (out filled), JOB_NOT_FOUND (absent), or a typed error.
 * Never allocates on failure paths that matter for correctness. */
JobStatus job_state_get(JobEngine *je, const char *key, uint32_t key_len,
                        JobStateValue *out);

/* Receipt of one direct durable mutation; also embedded in completion
 * responses. `replayed` distinguishes the original commit from a matched
 * retry; every other field is immutable across retries. */
typedef struct JobMutationReceipt {
    unsigned char op_id[JOB_ID_LEN];
    unsigned char kind;
    uint64_t commit_id;
    uint64_t state_version;
    uint64_t completed_at_ms;
    uint64_t receipt_expires_ms;
    int replayed;
} JobMutationReceipt;

/* Version-checked standalone PUT. expected_version 0 creates only; a
 * positive value must match exactly. The receipt is durable before the
 * response. JOB_STATE_VERSION_CONFLICT leaves the input untouched. */
JobStatus job_state_put(JobEngine *je, const char *key, uint32_t key_len,
                        const void *value, uint32_t value_len,
                        uint64_t expected_version,
                        const unsigned char op_id[JOB_ID_LEN],
                        JobMutationReceipt *out);

/* Version-checked standalone DELETE. Deleting an absent entry without a
 * retained receipt is a definite JOB_NOT_FOUND with no mutation; retrying
 * a previously committed delete returns its retained receipt. */
JobStatus job_state_delete(JobEngine *je, const char *key, uint32_t key_len,
                           uint64_t expected_version,
                           const unsigned char op_id[JOB_ID_LEN],
                           JobMutationReceipt *out);

/* Receipt lookup by operation id across all kinds. JOB_NOT_FOUND means "no
 * retained receipt", never "never executed". */
JobStatus job_receipt_lookup(JobEngine *je,
                             const unsigned char op_id[JOB_ID_LEN],
                             JobReceipt *out);

/* Bounded metadata enumeration for the Management API inventory. Callback
 * data is valid only during the call; keys/values are not copied. */
typedef void (*JobStateForeachFn)(const char *key, uint32_t key_len,
                                  uint64_t version, uint64_t commit_id,
                                  uint32_t value_len, void *ud);
void job_state_foreach(JobEngine *je, JobStateForeachFn fn, void *ud,
                       uint32_t max_entries, uint32_t *out_count);

/* Bounded receipt enumeration for the Management API receipt inventory.
 * Only receipts whose kind matches `kind_filter` (0 = every kind) and that
 * sort strictly after the keyset position (after_completed_ms,
 * after_op_id) are selected; use a zero after_op_id for an unpositioned
 * scan. The callback receives full receipt snapshots in ascending
 * (completed_at_ms, op_id) order. Returns the total number of matching
 * receipts, so a caller that bounded the output at max_entries can detect
 * additional matches (total > returned). */
typedef void (*JobReceiptForeachFn)(const JobReceipt *receipt, void *ud);
uint64_t job_receipt_foreach(JobEngine *je, unsigned kind_filter,
                             uint64_t after_completed_ms,
                             const unsigned char after_op[JOB_ID_LEN],
                             JobReceiptForeachFn fn, void *ud,
                             uint32_t max_entries);

/* Byte/count accounting for STATS, readiness, and checkpoints. */
void job_engine_usage(const JobEngine *je, uint64_t *state_entries,
                      uint64_t *state_bytes, uint64_t *receipt_count,
                      uint64_t *receipt_bytes);
/* Process-lifetime counters, separate from persisted receipt counts. */
typedef struct JobCounters {
    uint64_t completions, replays, id_conflicts, state_conflicts;
    uint64_t delivery_rejects, receipt_gc, too_large_rejects, resource_rejects;
} JobCounters;
void job_engine_counters(const JobEngine *je, JobCounters *out);

/* Remove expired receipts (bounded incremental pass). Returns the number
 * of receipts removed. Called by maintenance; admission runs it lazily. */
uint64_t job_receipt_gc(JobEngine *je, uint64_t now_ms);

#ifdef __cplusplus
}
#endif

#endif
