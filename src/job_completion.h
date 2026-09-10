#ifndef KUTTIDB_JOB_COMPLETION_H
#define KUTTIDB_JOB_COMPLETION_H

/* Completion-capable consumption, atomic completion submission, and the
 * delivery-proof registry.  One core implementation serves the native
 * protocol, the C companion library, and the Management API adapters.
 * See docs/design/ATOMIC_JOB_COMPLETION.md; commit authority is the Queue
 * WAL (ADR 0002). */

#include <stddef.h>
#include <stdint.h>

#include "job_state.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Typed delivery produced by the completion-capable consume.  The opaque
 * proof is the only credential: native owner tokens and delivery tags stay
 * private to the Queue engine and never appear in this type, in receipts,
 * or in responses. */
typedef struct JobDelivery {
    unsigned char store_id[JOB_ID_LEN];
    char queue[QUEUE_NAME_MAX];
    uint16_t queue_len;
    uint64_t queue_incarnation;
    uint64_t message_id;
    uint32_t attempts;
    unsigned redelivered : 1;
    /* Wall-clock mirror of the monotonic visibility lease for display and
     * logging only; fencing uses the engine's monotonic deadline. */
    uint64_t lease_deadline_ms;
    unsigned char proof[JOB_ID_LEN];
    void *data;                /* owned; release with job_delivery_free */
    uint32_t len;
} JobDelivery;

void job_delivery_free(JobDelivery *delivery);

/* Deliver one message from a durable Queue with a completion proof.
 * Requires a registered named consumer; the consumer's stable owner token
 * owns the delivery, so pooled SDK connections stay interchangeable.
 * Returns JOB_OK, JOB_NOT_FOUND (no ready message), JOB_UNSUPPORTED_FEATURE
 * (feature disabled), JOB_VALIDATION_FAILED (unknown consumer), or a
 * typed failure. The proof registry is bounded: a full registry answers
 * JOB_RESOURCE_EXHAUSTED rather than silently dropping fencing. */
JobStatus job_consume(JobEngine *je, const char *queue, uint32_t queue_len,
                      const char *consumer, uint32_t consumer_len,
                      uint64_t visibility_ms, JobDelivery *out);

/* One logical completion intent. The caller owns every byte span; the
 * request is not retained. `proof` must be the opaque proof returned by
 * job_consume for this stable input identity. */
typedef struct JobCompletionRequest {
    const char *input_queue;
    uint32_t input_queue_len;
    uint64_t input_incarnation;
    uint64_t input_message_id;
    const unsigned char *proof;          /* JOB_ID_LEN bytes */
    const char *state_key;
    uint32_t state_key_len;
    uint64_t expected_version;           /* 0 = create the state entry */
    const void *state_value;             /* may be empty (len 0) */
    uint32_t state_value_len;
    int has_output;                      /* 0 = no output message */
    const char *output_queue;
    uint32_t output_queue_len;
    uint64_t output_incarnation;
    const void *output_value;
    uint32_t output_value_len;
    unsigned char op_id[JOB_ID_LEN];     /* caller-generated, stable */
} JobCompletionRequest;

typedef struct JobCompletionResult {
    uint64_t commit_id;
    uint64_t state_version;
    uint64_t output_message_id;          /* 0 = no output */
    uint64_t completed_at_ms;
    uint64_t receipt_expires_ms;
    int replayed;                        /* 1 = matched stored receipt */
} JobCompletionResult;

/* Submit one atomic completion. Dispatch order (docs/design section 6.4):
 * authorize (caller), decode (caller), receipt lookup, ID conflict,
 * fencing/validation, single-record commit. A matching committed receipt
 * is returned before any delivery, version, depth, or existence check. */
JobStatus job_complete(JobEngine *je, const JobCompletionRequest *req,
                       JobCompletionResult *out);

/* Invalidate one proof explicitly (consumer unregister/close paths).
 * Safe to call with an unknown proof. */
void job_proof_forget(JobEngine *je, const unsigned char proof[JOB_ID_LEN]);

#ifdef __cplusplus
}
#endif

#endif
