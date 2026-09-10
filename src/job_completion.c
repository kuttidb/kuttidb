#define _GNU_SOURCE
#include "job_int.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Completion-capable consumption, delivery proofs, and the atomic
 * completion commit. See docs/design/ATOMIC_JOB_COMPLETION.md; the Queue
 * WAL record is the commit authority (ADR 0002).
 *
 * Lock discipline inside a completion commit: the Queue engine holds the
 * input/output Queue locks and calls back in dispatch order — recheck
 * (receipt), prepare (CAS, budgets, proof, reservations; returns holding
 * state_lock), encode, record append+fsync, apply (releases state_lock) —
 * so a direct state mutation can never interleave between the version
 * check and the application of the committed record. */

/* ---- proof registry ---- */

JobProof *job_proof_find_locked(JobEngine *je,
                                const unsigned char proof[JOB_ID_LEN]) {
    uint32_t b = job_proof_bucket(proof);
    for (JobProof *p = je->proofs[b]; p; p = p->next)
        if (memcmp(p->proof, proof, JOB_ID_LEN) == 0) return p;
    return NULL;
}

void job_proof_remove_locked(JobEngine *je, JobProof *proof) {
    uint32_t b = job_proof_bucket(proof->proof);
    JobProof **link = &je->proofs[b];
    while (*link && *link != proof) link = &(*link)->next;
    if (*link) {
        *link = proof->next;
        je->proof_count--;
        free(proof->queue);
        free(proof);
    }
}

uint64_t job_proof_gc_locked(JobEngine *je, uint64_t now_mono) {
    uint64_t removed = 0;
    for (uint32_t i = 0; i < JOB_GC_BUDGET; i++) {
        uint32_t b = je->proof_gc_cursor;
        je->proof_gc_cursor =
            (je->proof_gc_cursor + 1) & (JOB_PROOF_BUCKETS - 1);
        JobProof **link = &je->proofs[b];
        while (*link) {
            JobProof *p = *link;
            if (p->expires_mono_ms + 60000 <= now_mono) {
                *link = p->next;
                je->proof_count--;
                free(p->queue);
                free(p);
                removed++;
            } else {
                link = &p->next;
            }
        }
    }
    return removed;
}

void job_proof_forget(JobEngine *je, const unsigned char proof[JOB_ID_LEN]) {
    if (!je || !proof) return;
    pthread_mutex_lock(&je->proof_lock);
    JobProof *p = job_proof_find_locked(je, proof);
    if (p) job_proof_remove_locked(je, p);
    pthread_mutex_unlock(&je->proof_lock);
}

void job_delivery_free(JobDelivery *delivery) {
    if (!delivery) return;
    free(delivery->data);
    delivery->data = NULL;
}

/* ---- completion-capable consume ---- */

JobStatus job_consume(JobEngine *je, const char *queue, uint32_t queue_len,
                      const char *consumer, uint32_t consumer_len,
                      uint64_t visibility_ms, JobDelivery *out) {
    if (!je || !queue || !queue_len || queue_len > QUEUE_NAME_MAX ||
        !consumer || !consumer_len || !out)
        return JOB_VALIDATION_FAILED;
    memset(out, 0, sizeof *out);
    pthread_mutex_lock(&je->state_lock);
    int attached = je->attached;
    int failed = je->failed;
    int store_id_set = je->store_id_set;
    unsigned char store_id[JOB_ID_LEN];
    memcpy(store_id, je->store_id, JOB_ID_LEN);
    pthread_mutex_unlock(&je->state_lock);
    if (!attached) return JOB_UNSUPPORTED_FEATURE;
    if (failed) return JOB_PERSISTENCE_UNAVAILABLE;

    QueueConfigSnapshot snap;
    if (queue_config_snapshot(je->store, queue, queue_len, &snap) != 1)
        return JOB_VALIDATION_FAILED; /* unknown Queue */
    if (!snap.durable)
        return JOB_VALIDATION_FAILED; /* completions require durable input */

    uint64_t owner = 0;
    if (queue_consumer_lookup(je->store, consumer, consumer_len, &owner) != 1)
        return JOB_VALIDATION_FAILED; /* unknown consumer */

    unsigned char proof[JOB_ID_LEN];
    if (job_random_bytes(proof, JOB_ID_LEN) < 0)
        return JOB_PERSISTENCE_UNAVAILABLE;

    pthread_mutex_lock(&je->proof_lock);
    uint64_t now_mono = job_monotonic_ms();
    job_proof_gc_locked(je, now_mono);
    int full = je->proof_count >= je->proof_capacity;
    pthread_mutex_unlock(&je->proof_lock);
    if (full) {
        atomic_fetch_add(&je->resource_rejects, 1);
        return JOB_RESOURCE_EXHAUSTED;
    }

    QueueMessage msg;
    int rc = queue_consume_for_consumer(je->store, queue, queue_len, consumer,
                                        consumer_len, visibility_ms, &msg);
    if (rc < 0) return JOB_PERSISTENCE_UNAVAILABLE;
    if (rc == 0) return JOB_NOT_FOUND; /* no ready message */

    uint64_t incarnation = 0;
    (void)queue_incarnation(je->store, queue, queue_len, &incarnation);

    JobProof *p = calloc(1, sizeof *p);
    if (p) {
        p->queue = malloc(queue_len);
        if (!p->queue) {
            free(p);
            p = NULL;
        }
    }
    if (!p) {
        /* The delivery exists but cannot be fenced; it returns to the
         * ready set at its visibility deadline. */
        queue_message_free(&msg);
        atomic_fetch_add(&je->resource_rejects, 1);
        return JOB_RESOURCE_EXHAUSTED;
    }
    memcpy(p->proof, proof, JOB_ID_LEN);
    memcpy(p->queue, queue, queue_len);
    p->queue_len = (uint16_t)queue_len;
    p->queue_incarnation = incarnation;
    p->message_id = msg.id;
    p->delivery_tag = msg.delivery_tag;
    p->owner = msg.owner;
    p->expires_mono_ms = msg.visibility_deadline_ms
                             ? msg.visibility_deadline_ms
                             : now_mono + (visibility_ms ? visibility_ms : 30000);
    pthread_mutex_lock(&je->proof_lock);
    memcpy(p->epoch, je->epoch, JOB_ID_LEN);
    uint32_t b = job_proof_bucket(proof);
    p->next = je->proofs[b];
    je->proofs[b] = p;
    je->proof_count++;
    pthread_mutex_unlock(&je->proof_lock);

    memcpy(out->store_id, store_id_set ? store_id : (unsigned char *)"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0",
           JOB_ID_LEN);
    memcpy(out->queue, queue, queue_len);
    out->queue_len = (uint16_t)queue_len;
    out->queue_incarnation = incarnation;
    out->message_id = msg.id;
    out->attempts = msg.delivery_count;
    out->redelivered = msg.redelivered ? 1 : 0;
    out->lease_deadline_ms =
        job_now_ms() + (visibility_ms ? visibility_ms : 30000);
    memcpy(out->proof, proof, JOB_ID_LEN);
    out->data = msg.data; /* ownership transfers */
    out->len = msg.len;
    return JOB_OK;
}

/* ---- atomic completion ---- */

/* LOG_JOB_COMPLETION payload (little-endian throughout):
 * [fmt:1][op_id:16][in_incarnation:8][out_present:1][out_qlen:2][out_queue]
 * [out_incarnation:8][out_msg_id:8][out_vlen:4][out_payload]
 * [state_version:8][klen:2][key][expected_version:8][vlen:4][state_value]
 * [completed_at:8][expires_at:8]
 * Record header: name = input queue, id = input message id, aux = commit.
 * Fixed prefix = 1+16+8+1+2+8+8+4+8+2+8+4+8+8 = 86 bytes. */
#define COMPLETION_FIXED 86u

static uint32_t completion_len(const JobCompletionRequest *req) {
    uint32_t out_q = req->has_output ? req->output_queue_len : 0;
    uint32_t out_v = req->has_output ? req->output_value_len : 0;
    return COMPLETION_FIXED + out_q + out_v + req->state_key_len +
           req->state_value_len;
}

typedef struct CompleteCtx {
    JobEngine *je;
    const JobCompletionRequest *req;
    unsigned char digest[JOB_DIGEST_LEN];
    StateApplyPlan plan;
    int plan_built;
    JobReceiptRec *rec;
    uint64_t completed_at;
    uint64_t expires_at;
    int lock_held; /* state_lock acquired by prepare, released by apply */
} CompleteCtx;

static void ctx_free_prepared(CompleteCtx *c) {
    if (c->plan_built) {
        job_state_plan_discard(c->je, &c->plan);
        c->plan_built = 0;
    }
    if (c->rec) {
        free(c->rec->input_queue);
        free(c->rec->output_queue);
        free(c->rec);
        c->rec = NULL;
    }
}

static int complete_recheck(void *ud) {
    CompleteCtx *c = ud;
    JobEngine *je = c->je;
    pthread_mutex_lock(&je->state_lock);
    JobReceiptRec *r = job_receipt_find_locked(je, c->req->op_id);
    int rc;
    if (!r)
        rc = 0;
    else if (r->kind == JOB_KIND_COMPLETION &&
             memcmp(r->digest, c->digest, JOB_DIGEST_LEN) == 0)
        rc = 1; /* replay: the caller rebuilds the original result */
    else {
        atomic_fetch_add(&je->id_conflicts, 1);
        rc = JOB_IDEMPOTENCY_CONFLICT;
    }
    pthread_mutex_unlock(&je->state_lock);
    return rc;
}

/* Returns holding state_lock on JOB_OK (see queue.h contract). */
static int complete_prepare(void *ud, uint64_t output_msg_id,
                            uint64_t *out_commit_id,
                            uint64_t *out_state_version) {
    CompleteCtx *c = ud;
    JobEngine *je = c->je;
    const JobCompletionRequest *req = c->req;
    pthread_mutex_lock(&je->state_lock);
    c->lock_held = 1;
    if (je->version_hwm == UINT64_MAX || je->commit_hwm == UINT64_MAX) {
        je->failed = 1;
        pthread_mutex_unlock(&je->state_lock);
        c->lock_held = 0;
        return JOB_PERSISTENCE_UNAVAILABLE;
    }
    /* Version-checked state PUT inside the transaction. */
    JobStateEntry *cur =
        job_state_find_locked(je, req->state_key, req->state_key_len);
    if (req->expected_version == 0
            ? cur != NULL
            : (!cur || cur->version != req->expected_version)) {
        atomic_fetch_add(&je->state_conflicts, 1);
        pthread_mutex_unlock(&je->state_lock);
        c->lock_held = 0;
        return JOB_STATE_VERSION_CONFLICT;
    }
    int64_t delta =
        cur ? (int64_t)req->state_value_len - (int64_t)cur->value_len
            : (int64_t)(sizeof(JobStateEntry) + req->state_key_len +
                        req->state_value_len + JOB_STATE_ENTRY_OVERHEAD);
    if (delta > 0 &&
        je->state_bytes + (uint64_t)delta > je->cfg.state_max_bytes) {
        atomic_fetch_add(&je->resource_rejects, 1);
        pthread_mutex_unlock(&je->state_lock);
        c->lock_held = 0;
        return JOB_RESOURCE_EXHAUSTED;
    }
    uint16_t out_len = req->has_output ? req->output_queue_len : 0;
    if (je->receipt_count + 1 > je->cfg.receipts_max_count ||
        je->receipt_bytes + receipt_footprint(req->input_queue_len, out_len) >
            je->cfg.receipts_max_bytes) {
        job_receipt_gc_locked(je, job_now_ms());
        if (je->receipt_count + 1 > je->cfg.receipts_max_count ||
            je->receipt_bytes +
                    receipt_footprint(req->input_queue_len, out_len) >
                je->cfg.receipts_max_bytes) {
            atomic_fetch_add(&je->resource_rejects, 1);
            pthread_mutex_unlock(&je->state_lock);
            c->lock_held = 0;
            return JOB_RESOURCE_EXHAUSTED;
        }
    }
    if (je->version_hwm == UINT64_MAX || je->commit_hwm == UINT64_MAX) {
        je->failed = 1;
        pthread_mutex_unlock(&je->state_lock);
        c->lock_held = 0;
        return JOB_PERSISTENCE_UNAVAILABLE;
    }

    /* Delivery proof (state_lock held, proof_lock nested per order). */
    pthread_mutex_lock(&je->proof_lock);
    JobProof *p = job_proof_find_locked(je, req->proof);
    int proof_ok = 0;
    if (p && memcmp(p->epoch, je->epoch, JOB_ID_LEN) == 0 &&
        p->queue_len == req->input_queue_len &&
        memcmp(p->queue, req->input_queue, req->input_queue_len) == 0 &&
        p->queue_incarnation == req->input_incarnation &&
        p->message_id == req->input_message_id)
        proof_ok = p->expires_mono_ms > job_monotonic_ms() ? 1 : 2;
    pthread_mutex_unlock(&je->proof_lock);
    if (proof_ok != 1) {
        atomic_fetch_add(&je->delivery_rejects, 1);
        pthread_mutex_unlock(&je->state_lock);
        c->lock_held = 0;
        return proof_ok == 2 ? JOB_DELIVERY_EXPIRED : JOB_DELIVERY_NOT_OWNED;
    }

    /* Reserve ids and pre-build every post-append object: nothing below
     * may fail once the record becomes durable. */
    c->completed_at = job_now_ms();
    c->expires_at = c->completed_at + je->cfg.receipt_retention_ms;
    uint64_t version = ++je->version_hwm;
    uint64_t commit = ++je->commit_hwm;
    if (job_state_plan_locked(je, req->state_key, req->state_key_len,
                              req->state_value, req->state_value_len, version,
                              commit, &c->plan) < 0) {
        atomic_fetch_add(&je->resource_rejects, 1);
        ctx_free_prepared(c);
        pthread_mutex_unlock(&je->state_lock);
        c->lock_held = 0;
        return JOB_RESOURCE_EXHAUSTED;
    }
    c->plan_built = 1;
    c->rec = receipt_create(JOB_KIND_COMPLETION, req->op_id, c->digest, commit,
                            version, req->input_queue, req->input_queue_len,
                            req->input_message_id,
                            req->has_output ? req->output_queue : NULL,
                            out_len, req->has_output ? output_msg_id : 0,
                            c->completed_at, c->expires_at);
    if (!c->rec) {
        atomic_fetch_add(&je->resource_rejects, 1);
        ctx_free_prepared(c);
        pthread_mutex_unlock(&je->state_lock);
        c->lock_held = 0;
        return JOB_RESOURCE_EXHAUSTED;
    }
    *out_commit_id = commit;
    *out_state_version = version;
    return JOB_OK;
}

static int complete_encode(void *ud, uint64_t output_msg_id,
                           uint64_t commit_id, uint64_t state_version,
                           unsigned char *buf, uint32_t cap) {
    CompleteCtx *c = ud;
    const JobCompletionRequest *req = c->req;
    uint32_t need = completion_len(req);
    if (cap < need) return -1;
    unsigned char *p = buf;
    *p++ = (unsigned char)QUEUE_WAL_JOB_FMT;
    memcpy(p, req->op_id, JOB_ID_LEN);
    p += JOB_ID_LEN;
    jput64(p, req->input_incarnation);
    p += 8;
    *p++ = req->has_output ? 1 : 0;
    uint32_t out_q = req->has_output ? req->output_queue_len : 0;
    uint32_t out_v = req->has_output ? req->output_value_len : 0;
    jput16(p, out_q);
    p += 2;
    if (out_q) {
        memcpy(p, req->output_queue, out_q);
        p += out_q;
    }
    jput64(p, req->has_output ? req->output_incarnation : 0);
    p += 8;
    jput64(p, req->has_output ? output_msg_id : 0);
    p += 8;
    jput32(p, out_v);
    p += 4;
    if (out_v) {
        memcpy(p, req->output_value, out_v);
        p += out_v;
    }
    jput64(p, state_version);
    p += 8;
    jput16(p, req->state_key_len);
    p += 2;
    memcpy(p, req->state_key, req->state_key_len);
    p += req->state_key_len;
    jput64(p, req->expected_version);
    p += 8;
    jput32(p, req->state_value_len);
    p += 4;
    if (req->state_value_len) {
        memcpy(p, req->state_value, req->state_value_len);
        p += req->state_value_len;
    }
    jput64(p, c->completed_at);
    p += 8;
    jput64(p, c->expires_at);
    (void)commit_id;
    return (int)need;
}

static void complete_apply(void *ud, uint64_t output_msg_id,
                           uint64_t commit_id, uint64_t state_version) {
    CompleteCtx *c = ud;
    JobEngine *je = c->je;
    const JobCompletionRequest *req = c->req;
    /* state_lock is held from prepare; apply publishes and releases. */
    c->rec->commit_id = commit_id;
    c->rec->state_version = state_version;
    receipt_link_locked(je, c->rec);
    c->rec = NULL;
    job_state_plan_apply(je, &c->plan);
    c->plan_built = 0;
    pthread_mutex_unlock(&je->state_lock);
    c->lock_held = 0;
    /* The proof is one-use: a committed completion retires it. */
    pthread_mutex_lock(&je->proof_lock);
    JobProof *p = job_proof_find_locked(je, req->proof);
    if (p) job_proof_remove_locked(je, p);
    pthread_mutex_unlock(&je->proof_lock);
    atomic_fetch_add(&je->completions, 1);
    (void)output_msg_id;
}

static void complete_cancel(void *ud) {
    CompleteCtx *c = ud;
    if (c->lock_held) {
        pthread_mutex_unlock(&c->je->state_lock);
        c->lock_held = 0;
    }
    ctx_free_prepared(c);
}

static void complete_result_fill(const JobReceiptRec *r,
                                 JobCompletionResult *out, int replayed) {
    out->commit_id = r->commit_id;
    out->state_version = r->state_version;
    out->output_message_id = r->output_message_id;
    out->completed_at_ms = r->completed_at_ms;
    out->receipt_expires_ms = r->receipt_expires_ms;
    out->replayed = replayed;
}

JobStatus job_complete(JobEngine *je, const JobCompletionRequest *req,
                       JobCompletionResult *out) {
    if (!je || !req || !out) return JOB_VALIDATION_FAILED;
    memset(out, 0, sizeof *out);
    if (!req->input_queue || !req->input_queue_len ||
        req->input_queue_len > QUEUE_NAME_MAX || !req->input_incarnation ||
        !req->input_message_id || !req->proof || !req->state_key ||
        !req->state_key_len || req->state_key_len > 65535 ||
        (!req->state_value && req->state_value_len) ||
        (req->has_output &&
         (!req->output_queue || !req->output_queue_len ||
          req->output_queue_len > QUEUE_NAME_MAX || !req->output_incarnation ||
          (!req->output_value && req->output_value_len))))
        return JOB_VALIDATION_FAILED;
    static const unsigned char zero_id[JOB_ID_LEN];
    if (memcmp(req->op_id, zero_id, JOB_ID_LEN) == 0)
        return JOB_VALIDATION_FAILED;

    pthread_mutex_lock(&je->state_lock);
    int attached = je->attached;
    int failed = je->failed;
    int store_id_set = je->store_id_set;
    unsigned char store_id[JOB_ID_LEN];
    memcpy(store_id, je->store_id, JOB_ID_LEN);
    int has_doubt = je->has_in_doubt;
    unsigned char doubt[JOB_ID_LEN];
    memcpy(doubt, je->in_doubt_op, JOB_ID_LEN);
    pthread_mutex_unlock(&je->state_lock);
    if (!attached) return JOB_UNSUPPORTED_FEATURE;
    if (!store_id_set) return JOB_PERSISTENCE_UNAVAILABLE;

    uint64_t agg = (uint64_t)req->input_queue_len + req->state_key_len +
                   req->state_value_len + 64;
    if (req->has_output)
        agg += (uint64_t)req->output_queue_len + req->output_value_len + 32;
    if (agg > je->cfg.max_op_bytes) {
        atomic_fetch_add(&je->too_large_rejects, 1);
        return JOB_REQUEST_TOO_LARGE;
    }

    unsigned char digest[JOB_DIGEST_LEN];
    job_canonical_digest(digest, JOB_KIND_COMPLETION, req->op_id, store_id,
                         req->input_queue, req->input_queue_len,
                         req->input_incarnation, req->input_message_id,
                         req->state_key, req->state_key_len,
                         req->expected_version, req->state_value,
                         req->state_value_len, req->has_output,
                         req->output_queue,
                         req->has_output ? req->output_queue_len : 0,
                         req->has_output ? req->output_incarnation : 0,
                         req->output_value,
                         req->has_output ? req->output_value_len : 0);

    /* Dispatch steps 3-5: receipt lookup, ID conflict, in-doubt, and a
     * latched engine — all before any delivery validation. */
    pthread_mutex_lock(&je->state_lock);
    JobReceiptRec *r = job_receipt_find_locked(je, req->op_id);
    if (r) {
        if (r->kind != JOB_KIND_COMPLETION ||
            memcmp(r->digest, digest, JOB_DIGEST_LEN) != 0) {
            atomic_fetch_add(&je->id_conflicts, 1);
            pthread_mutex_unlock(&je->state_lock);
            return JOB_IDEMPOTENCY_CONFLICT;
        }
        complete_result_fill(r, out, 1);
        atomic_fetch_add(&je->replays, 1);
        pthread_mutex_unlock(&je->state_lock);
        return JOB_OK;
    }
    if (has_doubt && memcmp(doubt, req->op_id, JOB_ID_LEN) == 0) {
        pthread_mutex_unlock(&je->state_lock);
        return JOB_OPERATION_IN_DOUBT;
    }
    if (failed) {
        pthread_mutex_unlock(&je->state_lock);
        return JOB_PERSISTENCE_UNAVAILABLE;
    }
    pthread_mutex_unlock(&je->state_lock);

    CompleteCtx ctx;
    memset(&ctx, 0, sizeof ctx);
    ctx.je = je;
    ctx.req = req;
    memcpy(ctx.digest, digest, JOB_DIGEST_LEN);

    /* Resolve the attempt's native tag/owner from the proof registry: the
     * Queue-engine fence revalidates both against the live delivery under
     * the commit locks, so a reaped-and-redelivered message fails closed. */
    uint64_t input_tag = 0, input_owner = 0;
    pthread_mutex_lock(&je->proof_lock);
    JobProof *p = job_proof_find_locked(je, req->proof);
    if (p && p->queue_len == req->input_queue_len &&
        memcmp(p->queue, req->input_queue, req->input_queue_len) == 0 &&
        p->queue_incarnation == req->input_incarnation &&
        p->message_id == req->input_message_id) {
        input_tag = p->delivery_tag;
        input_owner = p->owner;
    }
    pthread_mutex_unlock(&je->proof_lock);
    if (!input_tag || !input_owner) {
        atomic_fetch_add(&je->delivery_rejects, 1);
        return JOB_DELIVERY_NOT_OWNED;
    }

    QueueJobCommit spec;
    memset(&spec, 0, sizeof spec);
    spec.input_queue = req->input_queue;
    spec.input_queue_len = req->input_queue_len;
    spec.input_message_id = req->input_message_id;
    spec.input_tag = input_tag;
    spec.input_owner = input_owner;
    spec.output_queue = req->has_output ? req->output_queue : NULL;
    spec.output_queue_len = req->has_output ? req->output_queue_len : 0;
    spec.output_data = req->has_output ? req->output_value : NULL;
    spec.output_len = req->has_output ? req->output_value_len : 0;
    spec.ud = &ctx;
    spec.recheck = complete_recheck;
    spec.prepare = complete_prepare;
    spec.encode = complete_encode;
    spec.apply = complete_apply;
    spec.cancel = complete_cancel;
    spec.record_cap = completion_len(req);

    int status = JOB_OK;
    int replayed = 0;
    int rc = queue_job_commit(je->store, &spec, &status, &replayed);
    if (rc == 1) {
        /* Committed: apply filled the receipt via the ledger. */
        pthread_mutex_lock(&je->state_lock);
        JobReceiptRec *committed = job_receipt_find_locked(je, req->op_id);
        if (committed) complete_result_fill(committed, out, 0);
        pthread_mutex_unlock(&je->state_lock);
        return committed ? JOB_OK : JOB_OPERATION_IN_DOUBT;
    }
    if (rc == 0) {
        if (replayed) {
            pthread_mutex_lock(&je->state_lock);
            JobReceiptRec *stored = job_receipt_find_locked(je, req->op_id);
            if (stored) complete_result_fill(stored, out, 1);
            pthread_mutex_unlock(&je->state_lock);
            return stored ? JOB_OK : JOB_OPERATION_IN_DOUBT;
        }
        if (status == JOB_DELIVERY_NOT_OWNED) {
            pthread_mutex_lock(&je->proof_lock);
            JobProof *pp = job_proof_find_locked(je, req->proof);
            int expired = pp && pp->queue_len == req->input_queue_len &&
                          memcmp(pp->queue, req->input_queue,
                                 req->input_queue_len) == 0 &&
                          pp->queue_incarnation == req->input_incarnation &&
                          pp->message_id == req->input_message_id &&
                          pp->expires_mono_ms <= job_monotonic_ms() &&
                          memcmp(pp->epoch, je->epoch, JOB_ID_LEN) == 0;
            pthread_mutex_unlock(&je->proof_lock);
            if (expired) {
                atomic_fetch_add(&je->delivery_rejects, 1);
                return JOB_DELIVERY_EXPIRED;
            }
        }
        return (JobStatus)status;
    }
    if (rc == -1) {
        /* Append attempted; durability unknown. */
        job_engine_mark_in_doubt(je, req->op_id);
        return JOB_OPERATION_IN_DOUBT;
    }
    return JOB_VALIDATION_FAILED; /* -2: invalid arguments */
}
