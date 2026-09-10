#ifndef KUTTIDB_JOB_INT_H
#define KUTTIDB_JOB_INT_H

/* Private definitions shared by job_state.c and job_completion.c. Not part
 * of any installed header. */

#include <pthread.h>
#include <stdatomic.h>

#include "job_state.h"
#include "job_completion.h"

#define JOB_STATE_BUCKETS 4096u
#define JOB_RECEIPT_BUCKETS 4096u
#define JOB_PROOF_BUCKETS 1024u
/* Incremental GC budget: buckets visited per pass. Bounded so no scan
 * pauses the server proportionally to receipt count. */
#define JOB_GC_BUDGET 64u
/* Internal admission sentinel: a matching receipt was found (replay path).
 * Never part of the public status enum. */
#define JOB_OK_REPLAY_SENTINEL 100
/* Fixed per-object index/accounting overhead charged against budgets. */
#define JOB_STATE_ENTRY_OVERHEAD 96u
#define JOB_RECEIPT_OVERHEAD 96u

typedef struct JobStateEntry {
    struct JobStateEntry *next;
    char *key;
    uint32_t key_len;
    void *value;
    uint32_t value_len;
    uint64_t version;
    uint64_t last_commit_id;
    uint32_t wal_footprint;
} JobStateEntry;

typedef struct JobReceiptRec {
    struct JobReceiptRec *next;
    unsigned char op_id[JOB_ID_LEN];
    unsigned char digest[JOB_DIGEST_LEN];
    unsigned char kind;
    uint64_t commit_id;
    uint64_t state_version;
    char *input_queue;
    uint16_t input_queue_len;
    uint64_t input_message_id;
    char *output_queue;
    uint16_t output_queue_len;
    uint64_t output_message_id;
    uint64_t completed_at_ms;
    uint64_t receipt_expires_ms;
    uint32_t wal_footprint;
} JobReceiptRec;

typedef struct JobProof {
    struct JobProof *next;
    unsigned char proof[JOB_ID_LEN];
    unsigned char epoch[JOB_ID_LEN];   /* process epoch (random, 16 bytes) */
    char *queue;
    uint16_t queue_len;
    uint64_t queue_incarnation;
    uint64_t message_id;
    uint64_t delivery_tag;
    uint64_t owner;
    uint64_t expires_mono_ms;
} JobProof;

struct JobEngine {
    /* Lock order: state_lock -> proof_lock. Callers holding Queue locks
     * take these last (metadata -> Queue -> job_state -> job_completion);
     * nothing acquires a Queue lock while holding either. */
    pthread_mutex_t state_lock;
    pthread_mutex_t proof_lock;
    QueueStore *store;                 /* set by job_engine_attach */
    JobEngineConfig cfg;
    unsigned char store_id[JOB_ID_LEN];
    int store_id_set;
    int failed;                        /* latched persistence failure */
    int attached;

    JobStateEntry *state[JOB_STATE_BUCKETS];
    JobReceiptRec *receipts[JOB_RECEIPT_BUCKETS];
    JobProof *proofs[JOB_PROOF_BUCKETS];
    uint64_t state_entries;
    uint64_t state_bytes;
    uint64_t receipt_count;
    uint64_t receipt_bytes;
    uint64_t proof_count;
    uint32_t proof_capacity;
    uint64_t version_hwm;              /* last assigned state version */
    uint64_t commit_hwm;               /* last assigned commit id */
    uint32_t gc_cursor;                /* rotating receipt-GC bucket */
    /* The one operation id whose append could not be resolved; retries of
     * this id answer JOB_OPERATION_IN_DOUBT until a receipt appears. */
    unsigned char in_doubt_op[JOB_ID_LEN];
    int has_in_doubt;
    /* Process epoch bound into every delivery proof (restarts invalidate). */
    unsigned char epoch[JOB_ID_LEN];
    uint32_t proof_gc_cursor;

    _Atomic uint64_t completions;
    _Atomic uint64_t replays;
    _Atomic uint64_t id_conflicts;
    _Atomic uint64_t state_conflicts;
    _Atomic uint64_t delivery_rejects;
    _Atomic uint64_t receipt_gc;
    _Atomic uint64_t too_large_rejects;
    _Atomic uint64_t resource_rejects;
};

/* SHA-256 over up to four concatenated byte spans; TLS-independent. */
void job_sha256(const void *a, size_t alen, const void *b, size_t blen,
                const void *c, size_t clen, const void *d, size_t dlen,
                unsigned char out[JOB_DIGEST_LEN]);

typedef struct JobShaPart {
    const void *data;
    size_t len;
} JobShaPart;

/* SHA-256 over an ordered list of byte spans. */
void job_sha256_parts(unsigned char out[JOB_DIGEST_LEN],
                      const JobShaPart *parts, int count);

/* Process-lifetime random bytes (urandom pattern; no TLS dependency).
 * Returns 0 on success. Used for store ids, epochs, and proofs. */
int job_random_bytes(unsigned char *out, size_t len);

/* Wall/monotonic clocks (ms), mirroring queue.c semantics. */
uint64_t job_now_ms(void);
uint64_t job_monotonic_ms(void);

/* Canonical semantic request digest ("KJS1", versioned). Excludes the
 * ephemeral delivery proof and transport identifiers by construction. */
void job_canonical_digest(unsigned char out[JOB_DIGEST_LEN], unsigned kind,
                          const unsigned char op_id[JOB_ID_LEN],
                          const unsigned char store_id[JOB_ID_LEN],
                          const char *in_name, uint32_t in_len,
                          uint64_t in_incarnation, uint64_t in_msg_id,
                          const char *key, uint32_t key_len,
                          uint64_t expected_version,
                          const void *value, uint32_t value_len,
                          int has_output, const char *out_name,
                          uint32_t out_len, uint64_t out_incarnation,
                          const void *out_value, uint32_t out_value_len);

/* Receipt ledger primitives (state_lock held by callers where noted). */
uint32_t job_receipt_bucket(const unsigned char op_id[JOB_ID_LEN]);
JobReceiptRec *job_receipt_find_locked(JobEngine *je,
                                       const unsigned char op_id[JOB_ID_LEN]);
/* Inserts an exact receipt; a record with the same op id and identical
 * content is a no-op (replay idempotence). Returns 0 on success/dupe,
 * -1 on allocation failure, -2 on conflicting content. */
int job_receipt_insert_locked(JobEngine *je, unsigned char kind,
                              const unsigned char op_id[JOB_ID_LEN],
                              const unsigned char digest[JOB_DIGEST_LEN],
                              uint64_t commit_id, uint64_t state_version,
                              const char *in_name, uint32_t in_len,
                              uint64_t in_msg_id, const char *out_name,
                              uint32_t out_len, uint64_t out_msg_id,
                              uint64_t completed_at, uint64_t expires_at);
void job_receipt_fill(const JobReceiptRec *rec, JobReceipt *out);

/* Prepared-object constructors (all fallible allocation happens here, so
 * post-append application stays infallible). Callers link with the *_link
 * helpers. */
JobStateEntry *entry_create(const char *key, uint32_t key_len,
                            const void *value, uint32_t value_len,
                            uint64_t version, uint64_t commit_id);
JobReceiptRec *receipt_create(unsigned char kind,
                              const unsigned char op_id[JOB_ID_LEN],
                              const unsigned char digest[JOB_DIGEST_LEN],
                              uint64_t commit_id, uint64_t state_version,
                              const char *in_name, uint32_t in_len,
                              uint64_t in_msg_id, const char *out_name,
                              uint32_t out_len, uint64_t out_msg_id,
                              uint64_t completed_at, uint64_t expires_at);
/* Infallible publication of prepared objects (state_lock held). */
void entry_link_locked(JobEngine *je, JobStateEntry *e);
void receipt_link_locked(JobEngine *je, JobReceiptRec *r);

/* Durable-state application plan: builds (allocating) everything a state
 * PUT needs — an in-place replacement for an existing entry or a fresh
 * linked entry — so post-append application is infallible. All calls take
 * state_lock held by the caller. */
typedef struct StateApplyPlan {
    JobStateEntry *existing;      /* in-place update target, or NULL */
    void *replacement_value;      /* owned; consumed by apply */
    JobStateEntry *fresh;         /* fresh entry, or NULL */
    int64_t bytes_delta;          /* accounting applied by apply */
    uint64_t version;             /* result version for either path */
    uint64_t commit_id;           /* result commit id for either path */
} StateApplyPlan;

int job_state_plan_locked(JobEngine *je, const char *key, uint32_t key_len,
                          const void *value, uint32_t value_len,
                          uint64_t version, uint64_t commit_id,
                          StateApplyPlan *plan);
void job_state_plan_apply(JobEngine *je, StateApplyPlan *plan);
void job_state_plan_discard(JobEngine *je, StateApplyPlan *plan);

/* Durable-state primitives. All require state_lock held by the caller. */
uint32_t job_state_bucket(const char *key, uint32_t key_len);
JobStateEntry *job_state_find_locked(JobEngine *je, const char *key,
                                     uint32_t key_len);
/* Applies a put without WAL writes or version allocation (replay/commit
 * application). Returns 0, -1 on allocation failure. */
int job_state_apply_put_locked(JobEngine *je, const char *key,
                               uint32_t key_len, const void *value,
                               uint32_t value_len, uint64_t version,
                               uint64_t commit_id);
/* Removes an entry (replay of delete). */
void job_state_apply_delete_locked(JobEngine *je, const char *key,
                                   uint32_t key_len);

/* Latches persistence failure and records the unresolved operation id; its
 * retries answer JOB_OPERATION_IN_DOUBT (state_lock taken internally). */
void job_engine_mark_in_doubt(JobEngine *je,
                              const unsigned char op_id[JOB_ID_LEN]);

/* Little-endian field helpers shared by the job modules (fixed-width,
 * independent of machine word size). */
static inline void jput16(unsigned char *p, unsigned v) {
    p[0] = (unsigned char)v;
    p[1] = (unsigned char)(v >> 8);
}

static inline unsigned jget16(const unsigned char *p) {
    return (unsigned)p[0] | ((unsigned)p[1] << 8);
}

static inline void jput32(unsigned char *p, uint32_t v) {
    for (int i = 0; i < 4; i++) p[i] = (unsigned char)(v >> (i * 8));
}

static inline uint32_t jget32(const unsigned char *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) |
           ((uint32_t)p[3] << 24);
}

static inline void jput64(unsigned char *p, uint64_t v) {
    for (int i = 0; i < 8; i++) p[i] = (unsigned char)(v >> (i * 8));
}

static inline uint64_t jget64(const unsigned char *p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) v |= (uint64_t)p[i] << (i * 8);
    return v;
}

/* Receipt byte/count footprint used for budget accounting. */
uint32_t receipt_footprint(uint16_t in_len, uint16_t out_len);

/* Bounded incremental receipt GC (state_lock held by the caller). */
uint64_t job_receipt_gc_locked(JobEngine *je, uint64_t now);

/* Latches persistence failure and records the unresolved operation id; its
 * retries answer JOB_OPERATION_IN_DOUBT (state_lock taken internally). */
void job_engine_mark_in_doubt(JobEngine *je,
                              const unsigned char op_id[JOB_ID_LEN]);

/* Proof registry primitives (proof_lock held by callers). */
uint32_t job_proof_bucket(const unsigned char proof[JOB_ID_LEN]);
JobProof *job_proof_find_locked(JobEngine *je,
                                const unsigned char proof[JOB_ID_LEN]);
void job_proof_remove_locked(JobEngine *je, JobProof *proof);
/* Bounded sweep of expired proofs; returns entries removed. */
uint64_t job_proof_gc_locked(JobEngine *je, uint64_t now_mono);

/* Queue WAL feature-record payload encoding/decoding (shared with the
 * completion module). All multi-byte fields are little-endian. */

/* LOG_JOB_STATE payload:
 * [kind:1][fmt:1][op_id:16][expected_version:8][klen:2][key][vlen:4]
 * [value][completed_at:8][expires_at:8] */
uint32_t job_state_record_len(uint32_t key_len, uint32_t value_len);
void job_state_record_encode(unsigned char *buf, int kind,
                             const unsigned char op_id[JOB_ID_LEN],
                             uint64_t expected_version, const char *key,
                             uint32_t key_len, const void *value,
                             uint32_t value_len, uint64_t completed_at,
                             uint64_t expires_at);
/* Returns 0 and fills the outputs when the payload is well-formed. */
int job_state_record_decode(const unsigned char *buf, uint32_t len,
                            int *kind, const unsigned char **op_id,
                            uint64_t *expected_version, const char **key,
                            uint32_t *key_len, const void **value,
                            uint32_t *value_len, uint64_t *completed_at,
                            uint64_t *expires_at);

#endif
