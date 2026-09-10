#define _GNU_SOURCE
#include "job_int.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* Durable state ("durable" Keyspace) and the operation-receipt ledger.
 * Commit authority is the Queue WAL (ADR 0002); this module owns the
 * in-memory indexes, version/commit allocation, digest canonicalization,
 * replay application, and checkpoint emission. */

#define JOB_RECORD_FMT 1u

/* ---- status helpers ---- */

int job_status_in_doubt(JobStatus status) {
    return status == JOB_OPERATION_IN_DOUBT;
}

const char *job_status_name(JobStatus status) {
    switch (status) {
    case JOB_OK: return "ok";
    case JOB_UNSUPPORTED_FEATURE: return "unsupported_feature";
    case JOB_VALIDATION_FAILED: return "validation_failed";
    case JOB_REQUEST_TOO_LARGE: return "request_too_large";
    case JOB_IDEMPOTENCY_CONFLICT: return "idempotency_conflict";
    case JOB_STATE_VERSION_CONFLICT: return "state_version_conflict";
    case JOB_DELIVERY_EXPIRED: return "delivery_expired";
    case JOB_DELIVERY_NOT_OWNED: return "delivery_not_owned";
    case JOB_RESOURCE_EXHAUSTED: return "resource_exhausted";
    case JOB_OPERATION_IN_PROGRESS: return "operation_in_progress";
    case JOB_OPERATION_IN_DOUBT: return "operation_in_doubt";
    case JOB_PERSISTENCE_UNAVAILABLE: return "persistence_unavailable";
    case JOB_NOT_FOUND: return "not_found";
    }
    return "unknown";
}

/* ---- SHA-256 (FIPS 180-4; self-contained, TLS=0 safe, known vectors
 * tested in test_job_state.c) ---- */

static const uint32_t sha_k[64] = {
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1,
    0x923f82a4, 0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
    0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786,
    0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147,
    0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
    0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
    0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a,
    0x5b9cca4f, 0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
    0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2};

typedef struct Sha256 {
    uint32_t h[8];
    unsigned char buf[64];
    size_t at;
    uint64_t total;
} Sha256;

static uint32_t sha_ror(uint32_t x, int n) {
    return (x >> n) | (x << (32 - n));
}

static void sha_compress(Sha256 *s, const unsigned char *block) {
    uint32_t w[64];
    for (int i = 0; i < 16; i++)
        w[i] = ((uint32_t)block[i * 4] << 24) |
               ((uint32_t)block[i * 4 + 1] << 16) |
               ((uint32_t)block[i * 4 + 2] << 8) | (uint32_t)block[i * 4 + 3];
    for (int i = 16; i < 64; i++) {
        uint32_t s0 = sha_ror(w[i - 15], 7) ^ sha_ror(w[i - 15], 18) ^
                      (w[i - 15] >> 3);
        uint32_t s1 = sha_ror(w[i - 2], 17) ^ sha_ror(w[i - 2], 19) ^
                      (w[i - 2] >> 10);
        w[i] = w[i - 16] + s0 + w[i - 7] + s1;
    }
    uint32_t a = s->h[0], b = s->h[1], c = s->h[2], d = s->h[3];
    uint32_t e = s->h[4], f = s->h[5], g = s->h[6], h = s->h[7];
    for (int i = 0; i < 64; i++) {
        uint32_t s1 = sha_ror(e, 6) ^ sha_ror(e, 11) ^ sha_ror(e, 25);
        uint32_t ch = (e & f) ^ (~e & g);
        uint32_t t1 = h + s1 + ch + sha_k[i] + w[i];
        uint32_t s0 = sha_ror(a, 2) ^ sha_ror(a, 13) ^ sha_ror(a, 22);
        uint32_t mj = (a & b) ^ (a & c) ^ (b & c);
        uint32_t t2 = s0 + mj;
        h = g; g = f; f = e; e = d + t1;
        d = c; c = b; b = a; a = t1 + t2;
    }
    s->h[0] += a; s->h[1] += b; s->h[2] += c; s->h[3] += d;
    s->h[4] += e; s->h[5] += f; s->h[6] += g; s->h[7] += h;
}

static void sha_init(Sha256 *s) {
    static const uint32_t iv[8] = {0x6a09e667, 0xbb67ae85, 0x3c6ef372,
                                   0xa54ff53a, 0x510e527f, 0x9b05688c,
                                   0x1f83d9ab, 0x5be0cd19};
    memcpy(s->h, iv, sizeof iv);
    s->at = 0;
    s->total = 0;
}

static void sha_update(Sha256 *s, const void *data, size_t len) {
    const unsigned char *p = data;
    s->total += len;
    while (len) {
        size_t take = 64 - s->at;
        if (take > len) take = len;
        memcpy(s->buf + s->at, p, take);
        s->at += take;
        p += take;
        len -= take;
        if (s->at == 64) {
            sha_compress(s, s->buf);
            s->at = 0;
        }
    }
}

static void sha_final(Sha256 *s, unsigned char out[32]) {
    uint64_t bits = s->total * 8;
    unsigned char pad = 0x80;
    sha_update(s, &pad, 1);
    unsigned char zero = 0;
    while (s->at != 56) sha_update(s, &zero, 1);
    unsigned char lenb[8];
    for (int i = 0; i < 8; i++) lenb[i] = (unsigned char)(bits >> (56 - i * 8));
    /* sha_update would bump total again; the length trailer is appended
     * directly because exactly one block remains. */
    memcpy(s->buf + 56, lenb, 8);
    sha_compress(s, s->buf);
    s->at = 0;
    for (int i = 0; i < 8; i++) {
        out[i * 4] = (unsigned char)(s->h[i] >> 24);
        out[i * 4 + 1] = (unsigned char)(s->h[i] >> 16);
        out[i * 4 + 2] = (unsigned char)(s->h[i] >> 8);
        out[i * 4 + 3] = (unsigned char)s->h[i];
    }
}

void job_sha256_parts(unsigned char out[JOB_DIGEST_LEN],
                      const JobShaPart *parts, int count) {
    Sha256 s;
    sha_init(&s);
    for (int i = 0; i < count; i++)
        if (parts[i].len) sha_update(&s, parts[i].data, parts[i].len);
    sha_final(&s, out);
}

void job_sha256(const void *a, size_t alen, const void *b, size_t blen,
                const void *c, size_t clen, const void *d, size_t dlen,
                unsigned char out[JOB_DIGEST_LEN]) {
    JobShaPart parts[4];
    int n = 0;
    if (alen) { parts[n].data = a; parts[n++].len = alen; }
    if (blen) { parts[n].data = b; parts[n++].len = blen; }
    if (clen) { parts[n].data = c; parts[n++].len = clen; }
    if (dlen) { parts[n].data = d; parts[n++].len = dlen; }
    job_sha256_parts(out, parts, n);
}

/* ---- randomness (repository urandom pattern; TLS=0 safe) ---- */

int job_random_bytes(unsigned char *out, size_t len) {
    int fd = open("/dev/urandom", O_RDONLY | O_CLOEXEC);
    if (fd < 0) return -1;
    size_t got = 0;
    while (got < len) {
        ssize_t n = read(fd, out + got, len - got);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) {
            close(fd);
            return -1;
        }
        got += (size_t)n;
    }
    close(fd);
    return 0;
}

/* ---- clocks (ms), mirroring queue.c semantics ---- */

uint64_t job_now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

uint64_t job_monotonic_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

/* ---- canonical semantic digest ("KJS1") ----
 *
 * Layout (versioned; changing it changes the domain tag):
 *   "KJS1" kind:1 op_id:16 store_id:16
 *   in_present:1 [ in_qlen:2 in_name in_incarnation:8 in_msg_id:8 ]
 *   klen:2 key expected_version:8 vlen:4 value
 *   out_present:1 [ out_qlen:2 out_name out_incarnation:8 out_vlen:4 out_value ]
 * Excluded by construction: transport request ids, JSON property order,
 * trace fields, and the ephemeral delivery proof. */

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
                          const void *out_value, uint32_t out_value_len) {
    static const unsigned char tag[4] = {'K', 'J', 'S', '1'};
    /* 85 bytes: tag(4) kind(1) op_id(16) store_id(16) in_present(1)
     * in_len(2) in_incarnation(8) in_msg(8) klen(2) expected(8) vlen(4)
     * out_present(1) out_qlen(2) out_incarnation(8) out_vlen(4). */
    unsigned char fixed[96];
    size_t at = 0;
    fixed[at++] = tag[0];
    fixed[at++] = tag[1];
    fixed[at++] = tag[2];
    fixed[at++] = tag[3];
    fixed[at++] = (unsigned char)kind;
    memcpy(fixed + at, op_id, JOB_ID_LEN);
    at += JOB_ID_LEN;
    memcpy(fixed + at, store_id, JOB_ID_LEN);
    at += JOB_ID_LEN;
    fixed[at++] = in_name ? 1 : 0;
    fixed[at++] = (unsigned char)(in_len & 0xff);
    fixed[at++] = (unsigned char)(in_len >> 8);
    for (int i = 0; i < 8; i++) fixed[at + i] = (unsigned char)(in_incarnation >> (i * 8));
    at += 8;
    for (int i = 0; i < 8; i++) fixed[at + i] = (unsigned char)(in_msg_id >> (i * 8));
    at += 8;
    fixed[at++] = (unsigned char)(key_len & 0xff);
    fixed[at++] = (unsigned char)(key_len >> 8);
    for (int i = 0; i < 8; i++) fixed[at + i] = (unsigned char)(expected_version >> (i * 8));
    at += 8;
    fixed[at++] = (unsigned char)(value_len & 0xff);
    fixed[at++] = (unsigned char)((value_len >> 8) & 0xff);
    fixed[at++] = (unsigned char)((value_len >> 16) & 0xff);
    fixed[at++] = (unsigned char)((value_len >> 24) & 0xff);
    fixed[at++] = has_output ? 1 : 0;
    fixed[at++] = (unsigned char)(out_len & 0xff);
    fixed[at++] = (unsigned char)(out_len >> 8);
    for (int i = 0; i < 8; i++) fixed[at + i] = (unsigned char)(out_incarnation >> (i * 8));
    at += 8;
    fixed[at++] = (unsigned char)(out_value_len & 0xff);
    fixed[at++] = (unsigned char)((out_value_len >> 8) & 0xff);
    fixed[at++] = (unsigned char)((out_value_len >> 16) & 0xff);
    fixed[at++] = (unsigned char)((out_value_len >> 24) & 0xff);
    /* at == 61: the fixed header ends here; the byte spans below are
     * hashed in fixed order so the canonical form is unambiguous. */
    JobShaPart parts[6];
    int n = 0;
    parts[n].data = fixed;
    parts[n++].len = at;
    if (in_name && in_len) {
        parts[n].data = in_name;
        parts[n++].len = in_len;
    }
    if (key && key_len) {
        parts[n].data = key;
        parts[n++].len = key_len;
    }
    if (value && value_len) {
        parts[n].data = value;
        parts[n++].len = value_len;
    }
    if (has_output && out_name && out_len) {
        parts[n].data = out_name;
        parts[n++].len = out_len;
    }
    if (out_value && out_value_len) {
        parts[n].data = out_value;
        parts[n++].len = out_value_len;
    }
    job_sha256_parts(out, parts, n);
}

/* ---- engine lifecycle ---- */

static void engine_locks_init(JobEngine *je) {
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK);
    pthread_mutex_init(&je->state_lock, &attr);
    pthread_mutex_init(&je->proof_lock, &attr);
    pthread_mutexattr_destroy(&attr);
}

JobEngine *job_engine_create(const JobEngineConfig *cfg) {
    JobEngine *je = calloc(1, sizeof *je);
    if (!je) return NULL;
    engine_locks_init(je);
    if (cfg) je->cfg = *cfg;
    if (!je->cfg.state_max_bytes) je->cfg.state_max_bytes = 64ull << 20;
    if (!je->cfg.receipts_max_bytes) je->cfg.receipts_max_bytes = 64ull << 20;
    if (!je->cfg.receipts_max_count) je->cfg.receipts_max_count = 100000;
    if (!je->cfg.receipt_retention_ms) je->cfg.receipt_retention_ms = 86400000;
    if (!je->cfg.max_op_bytes) je->cfg.max_op_bytes = 131072;
    if (!je->cfg.proof_capacity) je->cfg.proof_capacity = 4096;
    je->proof_capacity = je->cfg.proof_capacity;
    je->gc_cursor = 0;
    if (job_random_bytes(je->epoch, JOB_ID_LEN) < 0) {
        /* Without entropy the registry still works per process (it is
         * empty after every restart); proofs remain random. */
        memset(je->epoch, 0, JOB_ID_LEN);
    }
    return je;
}

void job_engine_config_get(const JobEngine *je, JobEngineConfig *out) {
    if (!je || !out) return;
    JobEngine *e = (JobEngine *)je;
    pthread_mutex_lock(&e->state_lock);
    *out = e->cfg;
    pthread_mutex_unlock(&e->state_lock);
}

void job_engine_destroy(JobEngine *je) {
    if (!je) return;
    for (uint32_t b = 0; b < JOB_STATE_BUCKETS; b++)
        for (JobStateEntry *e = je->state[b]; e;) {
            JobStateEntry *next = e->next;
            free(e->key);
            free(e->value);
            free(e);
            e = next;
        }
    for (uint32_t b = 0; b < JOB_RECEIPT_BUCKETS; b++)
        for (JobReceiptRec *r = je->receipts[b]; r;) {
            JobReceiptRec *next = r->next;
            free(r->input_queue);
            free(r->output_queue);
            free(r);
            r = next;
        }
    for (uint32_t b = 0; b < JOB_PROOF_BUCKETS; b++)
        for (JobProof *p = je->proofs[b]; p;) {
            JobProof *next = p->next;
            free(p->queue);
            free(p);
            p = next;
        }
    pthread_mutex_destroy(&je->state_lock);
    pthread_mutex_destroy(&je->proof_lock);
    free(je);
}

int job_engine_store_id(const JobEngine *je, unsigned char out[JOB_ID_LEN]) {
    if (!je || !out) return 0;
    JobEngine *e = (JobEngine *)je;
    pthread_mutex_lock(&e->state_lock);
    int set = je->store_id_set;
    if (set) memcpy(out, je->store_id, JOB_ID_LEN);
    pthread_mutex_unlock(&e->state_lock);
    return set;
}

uint64_t job_engine_state_budget(const JobEngine *je) {
    return je ? je->cfg.state_max_bytes : 0;
}

uint64_t job_engine_receipts_budget(const JobEngine *je) {
    return je ? je->cfg.receipts_max_bytes : 0;
}

uint64_t job_engine_receipts_max_count(const JobEngine *je) {
    return je ? je->cfg.receipts_max_count : 0;
}

uint64_t job_engine_receipt_retention_ms(const JobEngine *je) {
    return je ? je->cfg.receipt_retention_ms : 0;
}

uint64_t job_engine_max_op_bytes(const JobEngine *je) {
    return je ? je->cfg.max_op_bytes : 0;
}

void job_engine_fail(JobEngine *je) {
    if (!je) return;
    pthread_mutex_lock(&je->state_lock);
    je->failed = 1;
    pthread_mutex_unlock(&je->state_lock);
}

void job_engine_mark_in_doubt(JobEngine *je,
                              const unsigned char op_id[JOB_ID_LEN]) {
    if (!je) return;
    pthread_mutex_lock(&je->state_lock);
    je->failed = 1;
    memcpy(je->in_doubt_op, op_id, JOB_ID_LEN);
    je->has_in_doubt = 1;
    pthread_mutex_unlock(&je->state_lock);
}

int job_engine_writable(const JobEngine *je) {
    if (!je) return 0;
    /* state_lock guards `failed`/`attached`; the cast keeps this accessor
     * const while still serializing with writers. */
    JobEngine *e = (JobEngine *)je;
    pthread_mutex_lock(&e->state_lock);
    int ok = je->attached && !je->failed && je->store_id_set;
    pthread_mutex_unlock(&e->state_lock);
    return ok;
}

/* ---- hash buckets ---- */

/* FNV-1a; stability across restarts is irrelevant (in-memory only). */
static uint32_t fnv1a(const void *data, size_t len) {
    const unsigned char *p = data;
    uint32_t h = 2166136261u;
    for (size_t i = 0; i < len; i++) {
        h ^= p[i];
        h *= 16777619u;
    }
    return h;
}

uint32_t job_state_bucket(const char *key, uint32_t key_len) {
    return fnv1a(key, key_len) & (JOB_STATE_BUCKETS - 1);
}

uint32_t job_receipt_bucket(const unsigned char op_id[JOB_ID_LEN]) {
    return fnv1a(op_id, JOB_ID_LEN) & (JOB_RECEIPT_BUCKETS - 1);
}

uint32_t job_proof_bucket(const unsigned char proof[JOB_ID_LEN]) {
    return fnv1a(proof, JOB_ID_LEN) & (JOB_PROOF_BUCKETS - 1);
}

/* ---- durable state index ---- */

JobStateEntry *job_state_find_locked(JobEngine *je, const char *key,
                                     uint32_t key_len) {
    uint32_t b = job_state_bucket(key, key_len);
    for (JobStateEntry *e = je->state[b]; e; e = e->next)
        if (e->key_len == key_len && memcmp(e->key, key, key_len) == 0)
            return e;
    return NULL;
}

JobStateEntry *entry_create(const char *key, uint32_t key_len,
                                   const void *value, uint32_t value_len,
                                   uint64_t version, uint64_t commit_id) {
    JobStateEntry *e = calloc(1, sizeof *e);
    if (!e) return NULL;
    e->key = malloc(key_len ? key_len : 1);
    e->value = malloc(value_len ? value_len : 1);
    if (!e->key || !e->value) {
        free(e->key);
        free(e->value);
        free(e);
        return NULL;
    }
    memcpy(e->key, key, key_len);
    if (value_len) memcpy(e->value, value, value_len);
    e->key_len = key_len;
    e->value_len = value_len;
    e->version = version;
    e->last_commit_id = commit_id;
    e->wal_footprint = 0; /* set by the caller for durable accounting */
    return e;
}

void entry_link_locked(JobEngine *je, JobStateEntry *e) {
    uint32_t b = job_state_bucket(e->key, e->key_len);
    e->next = je->state[b];
    je->state[b] = e;
    je->state_entries++;
    je->state_bytes += sizeof(*e) + e->key_len + e->value_len +
                       JOB_STATE_ENTRY_OVERHEAD;
}

static void entry_unlink_locked(JobEngine *je, JobStateEntry *e) {
    uint32_t b = job_state_bucket(e->key, e->key_len);
    JobStateEntry **link = &je->state[b];
    while (*link && *link != e) link = &(*link)->next;
    if (*link) {
        *link = e->next;
        je->state_entries--;
        je->state_bytes -= sizeof(*e) + e->key_len + e->value_len +
                           JOB_STATE_ENTRY_OVERHEAD;
    }
}

int job_state_plan_locked(JobEngine *je, const char *key, uint32_t key_len,
                          const void *value, uint32_t value_len,
                          uint64_t version, uint64_t commit_id,
                          StateApplyPlan *plan) {
    memset(plan, 0, sizeof *plan);
    plan->version = version;
    plan->commit_id = commit_id;
    JobStateEntry *cur = job_state_find_locked(je, key, key_len);
    if (cur) {
        void *replacement = malloc(value_len ? value_len : 1);
        if (!replacement) return -1;
        if (value_len) memcpy(replacement, value, value_len);
        plan->existing = cur;
        plan->replacement_value = replacement;
        plan->bytes_delta = (int64_t)value_len - (int64_t)cur->value_len;
        return 0;
    }
    JobStateEntry *fresh = entry_create(key, key_len, value, value_len,
                                        version, commit_id);
    if (!fresh) return -1;
    plan->fresh = fresh;
    plan->bytes_delta = (int64_t)(sizeof(*fresh) + key_len + value_len +
                                  JOB_STATE_ENTRY_OVERHEAD);
    return 0;
}

void job_state_plan_apply(JobEngine *je, StateApplyPlan *plan) {
    if (plan->existing) {
        JobStateEntry *cur = plan->existing;
        free(cur->value);
        cur->value = plan->replacement_value;
        plan->replacement_value = NULL;
        cur->value_len = (uint32_t)((int64_t)cur->value_len +
                                    plan->bytes_delta);
        cur->version = plan->version;
        cur->last_commit_id = plan->commit_id;
        je->state_bytes = (uint64_t)((int64_t)je->state_bytes +
                                     plan->bytes_delta);
        return;
    }
    if (plan->fresh) {
        entry_link_locked(je, plan->fresh);
        plan->fresh = NULL;
    }
}

void job_state_plan_discard(JobEngine *je, StateApplyPlan *plan) {
    (void)je;
    free(plan->replacement_value);
    if (plan->fresh) {
        free(plan->fresh->key);
        free(plan->fresh->value);
        free(plan->fresh);
    }
    memset(plan, 0, sizeof *plan);
}

int job_state_apply_put_locked(JobEngine *je, const char *key,
                               uint32_t key_len, const void *value,
                               uint32_t value_len, uint64_t version,
                               uint64_t commit_id) {
    JobStateEntry *e = job_state_find_locked(je, key, key_len);
    if (e) {
        void *grown = realloc(e->value, value_len ? value_len : 1);
        if (!grown) return -1;
        e->value = grown;
        je->state_bytes -= e->value_len;
        if (value_len) memcpy(e->value, value, value_len);
        e->value_len = value_len;
        e->version = version;
        e->last_commit_id = commit_id;
        je->state_bytes += value_len;
        return 0;
    }
    e = entry_create(key, key_len, value, value_len, version, commit_id);
    if (!e) return -1;
    entry_link_locked(je, e);
    return 0;
}

void job_state_apply_delete_locked(JobEngine *je, const char *key,
                                   uint32_t key_len) {
    JobStateEntry *e = job_state_find_locked(je, key, key_len);
    if (e) {
        entry_unlink_locked(je, e);
        free(e->key);
        free(e->value);
        free(e);
    }
}

/* ---- receipt ledger ---- */

JobReceiptRec *job_receipt_find_locked(JobEngine *je,
                                       const unsigned char op_id[JOB_ID_LEN]) {
    uint32_t b = job_receipt_bucket(op_id);
    for (JobReceiptRec *r = je->receipts[b]; r; r = r->next)
        if (memcmp(r->op_id, op_id, JOB_ID_LEN) == 0) return r;
    return NULL;
}

static char *dup_span(const char *src, uint16_t len) {
    if (!len) return NULL;
    char *copy = malloc(len);
    if (copy && src) memcpy(copy, src, len);
    return copy;
}

uint32_t receipt_footprint(uint16_t in_len, uint16_t out_len) {
    return (uint32_t)(sizeof(JobReceiptRec) + in_len + out_len +
                      JOB_RECEIPT_OVERHEAD);
}

/* Builds a complete receipt record (all fallible allocation happens here).
 * Returns NULL on allocation failure. */
JobReceiptRec *receipt_create(unsigned char kind,
                                     const unsigned char op_id[JOB_ID_LEN],
                                     const unsigned char digest[JOB_DIGEST_LEN],
                                     uint64_t commit_id, uint64_t state_version,
                                     const char *in_name, uint32_t in_len,
                                     uint64_t in_msg_id, const char *out_name,
                                     uint32_t out_len, uint64_t out_msg_id,
                                     uint64_t completed_at,
                                     uint64_t expires_at) {
    JobReceiptRec *r = calloc(1, sizeof *r);
    if (!r) return NULL;
    memcpy(r->op_id, op_id, JOB_ID_LEN);
    if (digest) memcpy(r->digest, digest, JOB_DIGEST_LEN);
    r->kind = kind;
    r->commit_id = commit_id;
    r->state_version = state_version;
    r->completed_at_ms = completed_at;
    r->receipt_expires_ms = expires_at;
    r->input_message_id = in_msg_id;
    r->output_message_id = out_msg_id;
    if (in_len) {
        r->input_queue = dup_span(in_name, (uint16_t)in_len);
        if (!r->input_queue) {
            free(r);
            return NULL;
        }
        r->input_queue_len = (uint16_t)in_len;
    }
    if (out_len) {
        r->output_queue = dup_span(out_name, (uint16_t)out_len);
        if (!r->output_queue) {
            free(r->input_queue);
            free(r);
            return NULL;
        }
        r->output_queue_len = (uint16_t)out_len;
    }
    r->wal_footprint = receipt_footprint(r->input_queue_len,
                                         r->output_queue_len);
    return r;
}

int job_receipt_insert_locked(JobEngine *je, unsigned char kind,
                              const unsigned char op_id[JOB_ID_LEN],
                              const unsigned char digest[JOB_DIGEST_LEN],
                              uint64_t commit_id, uint64_t state_version,
                              const char *in_name, uint32_t in_len,
                              uint64_t in_msg_id, const char *out_name,
                              uint32_t out_len, uint64_t out_msg_id,
                              uint64_t completed_at, uint64_t expires_at) {
    JobReceiptRec *existing = job_receipt_find_locked(je, op_id);
    if (existing) {
        /* Replay idempotence: the same operation id may be re-applied by a
         * state record and a receipt record. Identical content is a no-op;
         * conflicting content keeps the original (first commit wins) — the
         * case is impossible from the live path and tolerated on replay. */
        return existing->commit_id == commit_id &&
                       existing->state_version == state_version &&
                       memcmp(existing->digest, digest, JOB_DIGEST_LEN) == 0
                   ? 0
                   : -2;
    }
    JobReceiptRec *r = receipt_create(kind, op_id, digest, commit_id,
                                      state_version, in_name, in_len,
                                      in_msg_id, out_name, out_len, out_msg_id,
                                      completed_at, expires_at);
    if (!r) return -1;
    uint32_t b = job_receipt_bucket(op_id);
    r->next = je->receipts[b];
    je->receipts[b] = r;
    je->receipt_count++;
    je->receipt_bytes += r->wal_footprint;
    return 0;
}

/* Links an already-built receipt (infallible application after commit). */
void receipt_link_locked(JobEngine *je, JobReceiptRec *r) {
    JobReceiptRec *existing = job_receipt_find_locked(je, r->op_id);
    if (existing) {
        free(r->input_queue);
        free(r->output_queue);
        free(r);
        return;
    }
    uint32_t b = job_receipt_bucket(r->op_id);
    r->next = je->receipts[b];
    je->receipts[b] = r;
    je->receipt_count++;
    je->receipt_bytes += r->wal_footprint;
}

void job_receipt_fill(const JobReceiptRec *rec, JobReceipt *out) {
    memset(out, 0, sizeof *out);
    memcpy(out->op_id, rec->op_id, JOB_ID_LEN);
    out->kind = rec->kind;
    out->commit_id = rec->commit_id;
    out->state_version = rec->state_version;
    out->completed_at_ms = rec->completed_at_ms;
    out->receipt_expires_ms = rec->receipt_expires_ms;
    out->input_queue_len = rec->input_queue_len;
    out->input_message_id = rec->input_message_id;
    out->output_queue_len = rec->output_queue_len;
    out->output_message_id = rec->output_message_id;
    if (rec->input_queue_len)
        memcpy(out->input_queue, rec->input_queue, rec->input_queue_len);
    if (rec->output_queue_len)
        memcpy(out->output_queue, rec->output_queue, rec->output_queue_len);
}

/* ---- feature record payload codec (LOG_JOB_STATE) ---- */

uint32_t job_state_record_len(uint32_t key_len, uint32_t value_len) {
    return 1 + 1 + JOB_ID_LEN + 8 + 2 + key_len + 4 + value_len + 8 + 8;
}

void job_state_record_encode(unsigned char *buf, int kind,
                             const unsigned char op_id[JOB_ID_LEN],
                             uint64_t expected_version, const char *key,
                             uint32_t key_len, const void *value,
                             uint32_t value_len, uint64_t completed_at,
                             uint64_t expires_at) {
    buf[0] = (unsigned char)QUEUE_WAL_JOB_FMT;
    buf[1] = (unsigned char)kind;
    memcpy(buf + 2, op_id, JOB_ID_LEN);
    jput64(buf + 18, expected_version);
    jput16(buf + 26, key_len);
    if (key_len) memcpy(buf + 28, key, key_len);
    jput32(buf + 28 + key_len, value_len);
    if (value_len) memcpy(buf + 32 + key_len, value, value_len);
    jput64(buf + 32 + key_len + value_len, completed_at);
    jput64(buf + 40 + key_len + value_len, expires_at);
}

int job_state_record_decode(const unsigned char *buf, uint32_t len,
                            int *kind, const unsigned char **op_id,
                            uint64_t *expected_version, const char **key,
                            uint32_t *key_len, const void **value,
                            uint32_t *value_len, uint64_t *completed_at,
                            uint64_t *expires_at) {
    if (len < 48 || buf[0] != (unsigned char)QUEUE_WAL_JOB_FMT) return -1;
    int k = buf[1];
    if (k != JOB_KIND_STATE_PUT && k != JOB_KIND_STATE_DELETE) return -1;
    uint32_t kl = jget16(buf + 26);
    if (28 + (size_t)kl + 4 > len) return -1;
    uint32_t vl = jget32(buf + 28 + kl);
    if (32 + (size_t)kl + (size_t)vl + 16 != len) return -1;
    *kind = k;
    *op_id = buf + 2;
    *expected_version = jget64(buf + 18);
    *key = (const char *)(buf + 28);
    *key_len = kl;
    *value = buf + 32 + kl;
    *value_len = vl;
    *completed_at = jget64(buf + 32 + kl + vl);
    *expires_at = jget64(buf + 40 + kl + vl);
    return 0;
}

/* ---- public durable-state operations ---- */

static void mutation_fill(JobMutationReceipt *out, unsigned char kind,
                          const unsigned char op_id[JOB_ID_LEN],
                          uint64_t commit_id, uint64_t version,
                          uint64_t completed_at, uint64_t expires_at,
                          int replayed) {
    memcpy(out->op_id, op_id, JOB_ID_LEN);
    out->kind = kind;
    out->commit_id = commit_id;
    out->state_version = version;
    out->completed_at_ms = completed_at;
    out->receipt_expires_ms = expires_at;
    out->replayed = replayed;
}

/* Bounded incremental GC of expired receipts (state_lock held). */
uint64_t job_receipt_gc_locked(JobEngine *je, uint64_t now) {
    uint64_t removed = 0;
    for (uint32_t i = 0; i < JOB_GC_BUDGET; i++) {
        uint32_t b = je->gc_cursor;
        je->gc_cursor = (je->gc_cursor + 1) & (JOB_RECEIPT_BUCKETS - 1);
        JobReceiptRec **link = &je->receipts[b];
        while (*link) {
            JobReceiptRec *r = *link;
            if (r->receipt_expires_ms && r->receipt_expires_ms <= now) {
                *link = r->next;
                je->receipt_count--;
                je->receipt_bytes -= r->wal_footprint;
                free(r->input_queue);
                free(r->output_queue);
                free(r);
                removed++;
            } else {
                link = &r->next;
            }
        }
    }
    if (removed) atomic_fetch_add(&je->receipt_gc, removed);
    return removed;
}

uint64_t job_receipt_gc(JobEngine *je, uint64_t now_ms) {
    if (!je) return 0;
    pthread_mutex_lock(&je->state_lock);
    uint64_t removed = job_receipt_gc_locked(je, now_ms);
    pthread_mutex_unlock(&je->state_lock);
    return removed;
}

JobStatus job_state_get(JobEngine *je, const char *key, uint32_t key_len,
                        JobStateValue *out) {
    if (!je || !key || !key_len || key_len > 65535 || !out)
        return JOB_VALIDATION_FAILED;
    pthread_mutex_lock(&je->state_lock);
    JobStateEntry *e = job_state_find_locked(je, key, key_len);
    if (!e) {
        pthread_mutex_unlock(&je->state_lock);
        return JOB_NOT_FOUND;
    }
    void *copy = malloc(e->value_len ? e->value_len : 1);
    if (!copy) {
        pthread_mutex_unlock(&je->state_lock);
        return JOB_RESOURCE_EXHAUSTED;
    }
    if (e->value_len) memcpy(copy, e->value, e->value_len);
    out->value = copy;
    out->len = e->value_len;
    out->version = e->version;
    out->last_commit_id = e->last_commit_id;
    pthread_mutex_unlock(&je->state_lock);
    return JOB_OK;
}

/* Shared admission logic for direct state mutations. Runs with state_lock
 * held; on JOB_OK the caller owns the reservation fields. */
typedef struct StateAdmission {
    unsigned char digest[JOB_DIGEST_LEN];
    uint64_t version;        /* assigned (put) or tombstone (delete) */
    uint64_t commit_id;
    uint64_t completed_at;
    uint64_t expires_at;
    JobReceiptRec *rec;      /* pre-built receipt */
    StateApplyPlan plan;     /* pre-built state application (put only) */
    unsigned char *payload;  /* pre-encoded record payload */
    uint32_t payload_len;
} StateAdmission;

static void admission_free(StateAdmission *a) {
    if (a->rec) {
        free(a->rec->input_queue);
        free(a->rec->output_queue);
        free(a->rec);
    }
    job_state_plan_discard(NULL, &a->plan);
    free(a->payload);
    memset(a, 0, sizeof *a);
}

/* Returns JOB_OK when admitted, JOB_OK_REPLAY_SENTINEL on a matched
 * receipt, or a typed rejection; the caller must admission_free() in every
 * path. `digest` must be precomputed by the caller (canonical identity). */
static int state_admit(JobEngine *je, int is_put, const char *key,
                       uint32_t key_len, const void *value,
                       uint32_t value_len, uint64_t expected_version,
                       const unsigned char op_id[JOB_ID_LEN],
                       const unsigned char digest[JOB_DIGEST_LEN],
                       StateAdmission *a) {
    memset(a, 0, sizeof *a);
    /* Receipt lookup precedes existence/CAS validation (guarantee 6). */
    JobReceiptRec *existing = job_receipt_find_locked(je, op_id);
    if (existing) {
        if (existing->kind != (is_put ? JOB_KIND_STATE_PUT : JOB_KIND_STATE_DELETE) ||
            memcmp(existing->digest, digest, JOB_DIGEST_LEN) != 0) {
            atomic_fetch_add(&je->id_conflicts, 1);
            return JOB_IDEMPOTENCY_CONFLICT;
        }
        atomic_fetch_add(&je->replays, 1);
        return JOB_OK_REPLAY_SENTINEL; /* filled by the caller */
    }
    if (je->has_in_doubt && memcmp(je->in_doubt_op, op_id, JOB_ID_LEN) == 0)
        return JOB_OPERATION_IN_DOUBT;
    if (je->failed) return JOB_PERSISTENCE_UNAVAILABLE;

    JobStateEntry *cur = job_state_find_locked(je, key, key_len);
    if (is_put) {
        if (expected_version == 0 ? cur != NULL
                                  : (!cur || cur->version != expected_version)) {
            atomic_fetch_add(&je->state_conflicts, 1);
            return JOB_STATE_VERSION_CONFLICT;
        }
    } else {
        if (!cur) {
            /* Definite not-found without a mutation (the receipt fast path
             * above already handled retries of committed deletes). */
            return JOB_NOT_FOUND;
        }
        if (cur->version != expected_version) {
            atomic_fetch_add(&je->state_conflicts, 1);
            return JOB_STATE_VERSION_CONFLICT;
        }
    }

    /* Budgets: state growth first (put; an in-place update counts only
     * its positive delta), then the receipt this mutation itself retains.
     * Pressure rejects; nothing is evicted. */
    if (is_put) {
        int64_t delta = cur
            ? (int64_t)value_len - (int64_t)cur->value_len
            : (int64_t)(sizeof(JobStateEntry) + key_len + value_len +
                        JOB_STATE_ENTRY_OVERHEAD);
        if (delta > 0 &&
            je->state_bytes + (uint64_t)delta > je->cfg.state_max_bytes) {
            atomic_fetch_add(&je->resource_rejects, 1);
            return JOB_RESOURCE_EXHAUSTED;
        }
    }
    uint64_t now = job_now_ms();
    if (je->receipt_count + 1 > je->cfg.receipts_max_count ||
        je->receipt_bytes + receipt_footprint(0, 0) >
            je->cfg.receipts_max_bytes) {
        job_receipt_gc_locked(je, now);
        if (je->receipt_count + 1 > je->cfg.receipts_max_count ||
            je->receipt_bytes + receipt_footprint(0, 0) >
                je->cfg.receipts_max_bytes) {
            atomic_fetch_add(&je->resource_rejects, 1);
            return JOB_RESOURCE_EXHAUSTED;
        }
    }

    if (je->version_hwm == UINT64_MAX || je->commit_hwm == UINT64_MAX) {
        /* Counter overflow: detect and latch; never wrap versions. */
        je->failed = 1;
        return JOB_PERSISTENCE_UNAVAILABLE;
    }
    a->version = ++je->version_hwm;
    a->commit_id = ++je->commit_hwm;
    a->completed_at = now;
    a->expires_at = now + je->cfg.receipt_retention_ms;
    memcpy(a->digest, digest, JOB_DIGEST_LEN);

    uint32_t plen = job_state_record_len(key_len, is_put ? value_len : 0);
    a->payload = malloc(plen);
    a->rec = receipt_create(is_put ? JOB_KIND_STATE_PUT : JOB_KIND_STATE_DELETE,
                            op_id, digest, a->commit_id, a->version, NULL, 0,
                            0, NULL, 0, 0, a->completed_at, a->expires_at);
    if (is_put &&
        job_state_plan_locked(je, key, key_len, value, value_len, a->version,
                              a->commit_id, &a->plan) < 0) {
        if (a->payload) {
            free(a->payload);
            a->payload = NULL;
        }
        if (a->rec) {
            free(a->rec->input_queue);
            free(a->rec->output_queue);
            free(a->rec);
            a->rec = NULL;
        }
        atomic_fetch_add(&je->resource_rejects, 1);
        return JOB_RESOURCE_EXHAUSTED;
    }
    if (!a->payload || !a->rec || (is_put && !a->plan.existing && !a->plan.fresh)) {
        atomic_fetch_add(&je->resource_rejects, 1);
        return JOB_RESOURCE_EXHAUSTED;
    }
    a->payload_len = plen;
    job_state_record_encode(a->payload, is_put ? JOB_KIND_STATE_PUT : JOB_KIND_STATE_DELETE,
                            op_id, expected_version, key, key_len,
                            is_put ? value : NULL, is_put ? value_len : 0,
                            a->completed_at, a->expires_at);
    return JOB_OK;
}

JobStatus job_state_put(JobEngine *je, const char *key, uint32_t key_len,
                        const void *value, uint32_t value_len,
                        uint64_t expected_version,
                        const unsigned char op_id[JOB_ID_LEN],
                        JobMutationReceipt *out) {
    if (!je || !key || !key_len || key_len > 65535 || (!value && value_len) ||
        !op_id || !out)
        return JOB_VALIDATION_FAILED;
    static const unsigned char zero_id[JOB_ID_LEN];
    if (memcmp(op_id, zero_id, JOB_ID_LEN) == 0) return JOB_VALIDATION_FAILED;
    if ((uint64_t)key_len + value_len + 64 > je->cfg.max_op_bytes) {
        atomic_fetch_add(&je->too_large_rejects, 1);
        return JOB_REQUEST_TOO_LARGE;
    }
    unsigned char digest[JOB_DIGEST_LEN];
    job_canonical_digest(digest, JOB_KIND_STATE_PUT, op_id, je->store_id,
                         NULL, 0, 0, 0, key, key_len, expected_version,
                         value, value_len, 0, NULL, 0, 0, NULL, 0);

    pthread_mutex_lock(&je->state_lock);
    StateAdmission a;
    JobStatus st = state_admit(je, 1, key, key_len, value, value_len,
                               expected_version, op_id, digest, &a);
    if (st == JOB_OK) {
        if (queue_job_record_append_sync(je->store, QUEUE_WAL_JOB_STATE,
                                         QUEUE_WAL_JOB_STATE_NAME,
                                         (uint32_t)(sizeof(QUEUE_WAL_JOB_STATE_NAME) - 1),
                                         a.version, a.commit_id, a.payload,
                                         a.payload_len) < 0) {
            /* The record's durability is unknown; latch and report doubt. */
            je->failed = 1;
            memcpy(je->in_doubt_op, op_id, JOB_ID_LEN);
            je->has_in_doubt = 1;
            admission_free(&a);
            pthread_mutex_unlock(&je->state_lock);
            return JOB_OPERATION_IN_DOUBT;
        }
        job_state_plan_apply(je, &a.plan);
        receipt_link_locked(je, a.rec);
        a.rec = NULL;
        mutation_fill(out, JOB_KIND_STATE_PUT, op_id, a.commit_id, a.version,
                      a.completed_at, a.expires_at, 0);
        st = JOB_OK;
    } else if (st == JOB_OK_REPLAY_SENTINEL) {
        JobReceiptRec *existing = job_receipt_find_locked(je, op_id);
        mutation_fill(out, existing->kind, existing->op_id,
                      existing->commit_id, existing->state_version,
                      existing->completed_at_ms, existing->receipt_expires_ms, 1);
        st = JOB_OK;
    }
    admission_free(&a);
    pthread_mutex_unlock(&je->state_lock);
    return st;
}

JobStatus job_state_delete(JobEngine *je, const char *key, uint32_t key_len,
                           uint64_t expected_version,
                           const unsigned char op_id[JOB_ID_LEN],
                           JobMutationReceipt *out) {
    if (!je || !key || !key_len || key_len > 65535 || !op_id || !out)
        return JOB_VALIDATION_FAILED;
    static const unsigned char zero_id[JOB_ID_LEN];
    if (memcmp(op_id, zero_id, JOB_ID_LEN) == 0) return JOB_VALIDATION_FAILED;
    if (expected_version == 0) return JOB_VALIDATION_FAILED;
    if ((uint64_t)key_len + 64 > je->cfg.max_op_bytes) {
        atomic_fetch_add(&je->too_large_rejects, 1);
        return JOB_REQUEST_TOO_LARGE;
    }
    unsigned char digest[JOB_DIGEST_LEN];
    job_canonical_digest(digest, JOB_KIND_STATE_DELETE, op_id, je->store_id,
                         NULL, 0, 0, 0, key, key_len, expected_version, NULL,
                         0, 0, NULL, 0, 0, NULL, 0);

    pthread_mutex_lock(&je->state_lock);
    StateAdmission a;
    JobStatus st = state_admit(je, 0, key, key_len, NULL, 0, expected_version,
                               op_id, digest, &a);
    if (st == JOB_OK) {
        if (queue_job_record_append_sync(je->store, QUEUE_WAL_JOB_STATE,
                                         QUEUE_WAL_JOB_STATE_NAME,
                                         (uint32_t)(sizeof(QUEUE_WAL_JOB_STATE_NAME) - 1),
                                         a.version, a.commit_id, a.payload,
                                         a.payload_len) < 0) {
            je->failed = 1;
            memcpy(je->in_doubt_op, op_id, JOB_ID_LEN);
            je->has_in_doubt = 1;
            admission_free(&a);
            pthread_mutex_unlock(&je->state_lock);
            return JOB_OPERATION_IN_DOUBT;
        }
        job_state_apply_delete_locked(je, key, key_len);
        receipt_link_locked(je, a.rec);
        a.rec = NULL;
        mutation_fill(out, JOB_KIND_STATE_DELETE, op_id, a.commit_id,
                      a.version, a.completed_at, a.expires_at, 0);
        st = JOB_OK;
    } else if (st == JOB_OK_REPLAY_SENTINEL) {
        JobReceiptRec *existing = job_receipt_find_locked(je, op_id);
        mutation_fill(out, existing->kind, existing->op_id,
                      existing->commit_id, existing->state_version,
                      existing->completed_at_ms, existing->receipt_expires_ms, 1);
        st = JOB_OK;
    }
    admission_free(&a);
    pthread_mutex_unlock(&je->state_lock);
    return st;
}

JobStatus job_receipt_lookup(JobEngine *je,
                             const unsigned char op_id[JOB_ID_LEN],
                             JobReceipt *out) {
    if (!je || !op_id || !out) return JOB_VALIDATION_FAILED;
    static const unsigned char zero_id[JOB_ID_LEN];
    if (memcmp(op_id, zero_id, JOB_ID_LEN) == 0) return JOB_VALIDATION_FAILED;
    pthread_mutex_lock(&je->state_lock);
    JobReceiptRec *r = job_receipt_find_locked(je, op_id);
    if (r) job_receipt_fill(r, out);
    pthread_mutex_unlock(&je->state_lock);
    return r ? JOB_OK : JOB_NOT_FOUND;
}

void job_state_foreach(JobEngine *je, JobStateForeachFn fn, void *ud,
                       uint32_t max_entries, uint32_t *out_count) {
    if (out_count) *out_count = 0;
    if (!je || !fn || !max_entries) return;
    uint32_t seen = 0;
    pthread_mutex_lock(&je->state_lock);
    for (uint32_t b = 0; b < JOB_STATE_BUCKETS && seen < max_entries; b++)
        for (JobStateEntry *e = je->state[b]; e && seen < max_entries;
             e = e->next) {
            fn(e->key, e->key_len, e->version, e->last_commit_id,
               e->value_len, ud);
            seen++;
        }
    pthread_mutex_unlock(&je->state_lock);
    if (out_count) *out_count = seen;
}

/* Bounded K-smallest selection over the receipt ledger. The caller's
 * callback fires only for the max_entries smallest matches (ascending
 * keyset order), so a paged inventory never allocates per receipt. */
typedef struct ReceiptSel {
    uint64_t completed_at_ms;
    unsigned char op_id[JOB_ID_LEN];
} ReceiptSel;

static int receipt_sel_less(uint64_t a_ms, const unsigned char a[JOB_ID_LEN],
                            uint64_t b_ms, const unsigned char b[JOB_ID_LEN]) {
    if (a_ms != b_ms) return a_ms < b_ms;
    return memcmp(a, b, JOB_ID_LEN) < 0;
}

uint64_t job_receipt_foreach(JobEngine *je, unsigned kind_filter,
                             uint64_t after_completed_ms,
                             const unsigned char after_op[JOB_ID_LEN],
                             JobReceiptForeachFn fn, void *ud,
                             uint32_t max_entries) {
    if (!je || !fn || !max_entries) return 0;
    static const unsigned char zero_id[JOB_ID_LEN];
    int positioned = after_op && memcmp(after_op, zero_id, JOB_ID_LEN) != 0;
    ReceiptSel *sel = calloc(max_entries, sizeof *sel);
    if (!sel) return 0;
    uint32_t kept = 0, largest = 0;
    uint64_t total = 0;
    pthread_mutex_lock(&je->state_lock);
    for (uint32_t b = 0; b < JOB_RECEIPT_BUCKETS; b++)
        for (JobReceiptRec *r = je->receipts[b]; r; r = r->next) {
            if (kind_filter && r->kind != (unsigned char)kind_filter) continue;
            if (positioned &&
                !receipt_sel_less(after_completed_ms, after_op,
                                  r->completed_at_ms, r->op_id))
                continue;
            total++;
            if (kept < max_entries) {
                sel[kept].completed_at_ms = r->completed_at_ms;
                memcpy(sel[kept].op_id, r->op_id, JOB_ID_LEN);
                if (kept == 0 ||
                    receipt_sel_less(sel[largest].completed_at_ms,
                                     sel[largest].op_id,
                                     sel[kept].completed_at_ms,
                                     sel[kept].op_id))
                    largest = kept;
                kept++;
                continue;
            }
            /* Replace the current largest only when this match is smaller;
             * the largest index is then re-derived once per replacement. */
            if (!receipt_sel_less(r->completed_at_ms, r->op_id,
                                  sel[largest].completed_at_ms,
                                  sel[largest].op_id))
                continue;
            sel[largest].completed_at_ms = r->completed_at_ms;
            memcpy(sel[largest].op_id, r->op_id, JOB_ID_LEN);
            for (uint32_t i = 1; i < kept; i++)
                if (receipt_sel_less(sel[largest].completed_at_ms,
                                     sel[largest].op_id,
                                     sel[i].completed_at_ms, sel[i].op_id))
                    largest = i;
        }
    /* Ascending keyset order for the callback. */
    for (uint32_t i = 1; i < kept; i++) {
        ReceiptSel key = sel[i];
        uint32_t j = i;
        while (j > 0 && receipt_sel_less(key.completed_at_ms, key.op_id,
                                         sel[j - 1].completed_at_ms,
                                         sel[j - 1].op_id)) {
            sel[j] = sel[j - 1];
            j--;
        }
        sel[j] = key;
    }
    for (uint32_t i = 0; i < kept; i++) {
        JobReceiptRec *r = job_receipt_find_locked(je, sel[i].op_id);
        if (!r) continue; /* unreachable under state_lock; defensive */
        JobReceipt snapshot;
        job_receipt_fill(r, &snapshot);
        fn(&snapshot, ud);
    }
    pthread_mutex_unlock(&je->state_lock);
    free(sel);
    return total;
}

void job_engine_usage(const JobEngine *je, uint64_t *state_entries,
                      uint64_t *state_bytes, uint64_t *receipt_count,
                      uint64_t *receipt_bytes) {
    if (!je) return;
    JobEngine *e = (JobEngine *)je;
    pthread_mutex_lock(&e->state_lock);
    if (state_entries) *state_entries = je->state_entries;
    if (state_bytes) *state_bytes = je->state_bytes;
    if (receipt_count) *receipt_count = je->receipt_count;
    if (receipt_bytes) *receipt_bytes = je->receipt_bytes;
    pthread_mutex_unlock(&e->state_lock);
}

void job_engine_counters(const JobEngine *je, JobCounters *out) {
    if (!out) return;
    memset(out, 0, sizeof *out);
    if (!je) return;
    out->completions = atomic_load(&je->completions);
    out->replays = atomic_load(&je->replays);
    out->id_conflicts = atomic_load(&je->id_conflicts);
    out->state_conflicts = atomic_load(&je->state_conflicts);
    out->delivery_rejects = atomic_load(&je->delivery_rejects);
    out->receipt_gc = atomic_load(&je->receipt_gc);
    out->too_large_rejects = atomic_load(&je->too_large_rejects);
    out->resource_rejects = atomic_load(&je->resource_rejects);
}

/* ---- attach: bind to a store, establish the durable identity ---- */

JobStatus job_engine_attach(JobEngine *je, QueueStore *store) {
    if (!je || !store) return JOB_VALIDATION_FAILED;
    if (!queue_wal_enabled(store)) return JOB_UNSUPPORTED_FEATURE;
    pthread_mutex_lock(&je->state_lock);
    if (je->attached) {
        pthread_mutex_unlock(&je->state_lock);
        return JOB_VALIDATION_FAILED; /* single attach; engine is private */
    }
    int need_meta = !je->store_id_set;
    unsigned char fresh[JOB_ID_LEN];
    if (need_meta && job_random_bytes(fresh, JOB_ID_LEN) < 0) {
        pthread_mutex_unlock(&je->state_lock);
        return JOB_PERSISTENCE_UNAVAILABLE;
    }
    if (need_meta) {
        unsigned char payload[1 + JOB_ID_LEN + 8 * 4];
        payload[0] = (unsigned char)QUEUE_WAL_JOB_FMT;
        memcpy(payload + 1, fresh, JOB_ID_LEN);
        jput64(payload + 17, queue_msg_id_hwm(store));
        jput64(payload + 25, queue_incarnation_hwm(store));
        jput64(payload + 33, je->version_hwm);
        jput64(payload + 41, je->commit_hwm);
        /* Released while the synchronous append runs: no other engine
         * caller exists before `attached` is set. */
        pthread_mutex_unlock(&je->state_lock);
        if (queue_job_record_append_sync(store, QUEUE_WAL_JOB_META,
                                         QUEUE_WAL_JOB_META_NAME,
                                         (uint32_t)(sizeof(QUEUE_WAL_JOB_META_NAME) - 1),
                                         0, 0, payload, sizeof payload) < 0) {
            job_engine_fail(je);
            return JOB_OPERATION_IN_DOUBT; /* record may or may not exist */
        }
        pthread_mutex_lock(&je->state_lock);
        memcpy(je->store_id, fresh, JOB_ID_LEN);
        je->store_id_set = 1;
    }
    je->store = store;
    je->attached = 1;
    pthread_mutex_unlock(&je->state_lock);
    return JOB_OK;
}

/* ---- replay hooks (installed via job_engine_replay_hooks) ---- */

static int hook_state_apply(void *ud, int kind, const unsigned char *op_id,
                            uint64_t version, uint64_t commit_id,
                            uint64_t expected_version, const char *key,
                            uint32_t key_len, const void *value,
                            uint32_t value_len, uint64_t completed_at,
                            uint64_t expires_at) {
    JobEngine *je = ud;
    pthread_mutex_lock(&je->state_lock);
    unsigned char digest[JOB_DIGEST_LEN];
    job_canonical_digest(digest,
                         kind == JOB_KIND_STATE_PUT ? JOB_KIND_STATE_PUT
                                                    : JOB_KIND_STATE_DELETE,
                         op_id, je->store_id, NULL, 0, 0, 0, key, key_len,
                         expected_version,
                         kind == JOB_KIND_STATE_PUT ? value : NULL,
                         kind == JOB_KIND_STATE_PUT ? value_len : 0, 0, NULL,
                         0, 0, NULL, 0);
    int rc = 0;
    if (kind == JOB_KIND_STATE_PUT)
        rc = job_state_apply_put_locked(je, key, key_len, value, value_len,
                                        version, commit_id);
    else
        job_state_apply_delete_locked(je, key, key_len);
    if (rc == 0) {
        if (version > je->version_hwm) je->version_hwm = version;
        if (commit_id > je->commit_hwm) je->commit_hwm = commit_id;
        /* expires_at == 0 restores state only (checkpoint emission of an
         * entry whose receipt was already forgotten). */
        if (expires_at)
            rc = job_receipt_insert_locked(
                je, (unsigned char)kind, op_id, digest, commit_id, version,
                NULL, 0, 0, NULL, 0, 0, completed_at, expires_at);
    }
    pthread_mutex_unlock(&je->state_lock);
    return rc == 0 ? 0 : -1;
}

static int hook_completion_apply(void *ud, const unsigned char *op_id,
                                 uint64_t commit_id, uint64_t state_version,
                                 uint64_t expected_version, const char *key,
                                 uint32_t key_len, const void *value,
                                 uint32_t value_len, const char *in_name,
                                 uint32_t in_len, uint64_t in_incarnation,
                                 uint64_t in_msg_id, const char *out_name,
                                 uint32_t out_len, uint64_t out_incarnation,
                                 uint64_t out_msg_id, const void *out_value,
                                 uint32_t out_value_len, uint64_t completed_at,
                                 uint64_t expires_at) {
    JobEngine *je = ud;
    pthread_mutex_lock(&je->state_lock);
    unsigned char digest[JOB_DIGEST_LEN];
    job_canonical_digest(digest, JOB_KIND_COMPLETION, op_id, je->store_id,
                         in_name, in_len, in_incarnation, in_msg_id, key,
                         key_len, expected_version, value, value_len,
                         out_name != NULL, out_name, out_len, out_incarnation,
                         out_value, out_value_len);
    int rc = job_state_apply_put_locked(je, key, key_len, value, value_len,
                                        state_version, commit_id);
    if (rc == 0) {
        if (state_version > je->version_hwm) je->version_hwm = state_version;
        if (commit_id > je->commit_hwm) je->commit_hwm = commit_id;
        rc = job_receipt_insert_locked(je, JOB_KIND_COMPLETION, op_id, digest,
                                       commit_id, state_version, in_name,
                                       in_len, in_msg_id, out_name, out_len,
                                       out_msg_id, completed_at, expires_at);
    }
    pthread_mutex_unlock(&je->state_lock);
    return rc == 0 ? 0 : -1;
}

static int hook_receipt_apply(void *ud, int kind, const unsigned char *op_id,
                              const unsigned char *digest, uint64_t commit_id,
                              uint64_t state_version, const char *in_name,
                              uint32_t in_len, uint64_t in_msg_id,
                              const char *out_name, uint32_t out_len,
                              uint64_t out_msg_id, uint64_t completed_at,
                              uint64_t expires_at) {
    JobEngine *je = ud;
    pthread_mutex_lock(&je->state_lock);
    if (commit_id > je->commit_hwm) je->commit_hwm = commit_id;
    if (state_version > je->version_hwm) je->version_hwm = state_version;
    int rc = job_receipt_insert_locked(je, (unsigned char)kind, op_id, digest,
                                       commit_id, state_version, in_name,
                                       in_len, in_msg_id, out_name, out_len,
                                       out_msg_id, completed_at, expires_at);
    pthread_mutex_unlock(&je->state_lock);
    return rc == 0 ? 0 : -1;
}

static int hook_meta_apply(void *ud, const unsigned char *store_id,
                           uint64_t next_msg_id, uint64_t next_incarnation,
                           uint64_t version_hwm, uint64_t commit_hwm) {
    JobEngine *je = ud;
    (void)next_msg_id;
    (void)next_incarnation; /* adopted by the Queue engine itself */
    pthread_mutex_lock(&je->state_lock);
    if (!je->store_id_set) {
        memcpy(je->store_id, store_id, JOB_ID_LEN);
        je->store_id_set = 1;
    }
    if (version_hwm > je->version_hwm) je->version_hwm = version_hwm;
    if (commit_hwm > je->commit_hwm) je->commit_hwm = commit_hwm;
    pthread_mutex_unlock(&je->state_lock);
    return 0;
}

/* ---- checkpoint emission (one consistent cut under state_lock) ---- */

static int hook_checkpoint_emit(void *ud, void *emit_ud,
                                int (*emit)(void *emit_ud, unsigned op,
                                            const char *name, uint32_t name_len,
                                            uint64_t id, uint64_t aux,
                                            const void *data, uint32_t len)) {
    JobEngine *je = ud;
    uint32_t state_name_len = (uint32_t)(sizeof(QUEUE_WAL_JOB_STATE_NAME) - 1);
    uint32_t receipt_name_len =
        (uint32_t)(sizeof(QUEUE_WAL_JOB_RECEIPT_NAME) - 1);
    pthread_mutex_lock(&je->state_lock);
    uint64_t now = job_now_ms();
    int rc = 0;
    static const unsigned char zero_op[JOB_ID_LEN];
    for (uint32_t b = 0; b < JOB_STATE_BUCKETS && !rc; b++) {
        for (JobStateEntry *e = je->state[b]; e && !rc; e = e->next) {
            uint32_t plen = job_state_record_len(e->key_len, e->value_len);
            unsigned char *payload = malloc(plen);
            if (!payload) {
                rc = -1;
                break;
            }
            /* expires_at 0: restore state only — receipts come from
             * LOG_JOB_RECEIPT records, never resurrected by state records.
             * The op id travels with the receipt record instead. */
            job_state_record_encode(payload, JOB_KIND_STATE_PUT, zero_op, 0,
                                    e->key, e->key_len, e->value,
                                    e->value_len, 0, 0);
            rc = emit(emit_ud, QUEUE_WAL_JOB_STATE, QUEUE_WAL_JOB_STATE_NAME,
                      state_name_len, e->version, e->last_commit_id, payload,
                      plen);
            free(payload);
        }
    }
    for (uint32_t b = 0; b < JOB_RECEIPT_BUCKETS && !rc; b++) {
        for (JobReceiptRec *r = je->receipts[b]; r && !rc; r = r->next) {
            if (r->receipt_expires_ms && r->receipt_expires_ms <= now)
                continue; /* expired: forgotten at the checkpoint boundary */
            uint32_t plen = 1 + 1 + JOB_ID_LEN + JOB_DIGEST_LEN + 1 + 2 +
                            (uint32_t)r->output_queue_len + 8 + 2 +
                            (uint32_t)r->input_queue_len + 8 + 8 + 8;
            unsigned char *payload = malloc(plen);
            if (!payload) {
                rc = -1;
                break;
            }
            unsigned char *p = payload;
            *p++ = r->kind;
            *p++ = (unsigned char)QUEUE_WAL_JOB_FMT;
            memcpy(p, r->op_id, JOB_ID_LEN);
            p += JOB_ID_LEN;
            memcpy(p, r->digest, JOB_DIGEST_LEN);
            p += JOB_DIGEST_LEN;
            *p++ = r->output_queue_len ? 1 : 0;
            jput16(p, r->output_queue_len);
            p += 2;
            if (r->output_queue_len) {
                memcpy(p, r->output_queue, r->output_queue_len);
                p += r->output_queue_len;
            }
            jput64(p, r->output_message_id);
            p += 8;
            jput16(p, r->input_queue_len);
            p += 2;
            if (r->input_queue_len) {
                memcpy(p, r->input_queue, r->input_queue_len);
                p += r->input_queue_len;
            }
            jput64(p, r->input_message_id);
            p += 8;
            jput64(p, r->completed_at_ms);
            p += 8;
            jput64(p, r->receipt_expires_ms);
            rc = emit(emit_ud, QUEUE_WAL_JOB_RECEIPT, QUEUE_WAL_JOB_RECEIPT_NAME,
                      receipt_name_len, r->commit_id, r->state_version,
                      payload, plen);
            free(payload);
        }
    }
    pthread_mutex_unlock(&je->state_lock);
    return rc;
}

static uint64_t hook_live_bytes(void *ud) {
    JobEngine *je = ud;
    uint64_t bytes = 0;
    pthread_mutex_lock(&je->state_lock);
    bytes = je->state_bytes + je->receipt_bytes;
    pthread_mutex_unlock(&je->state_lock);
    return bytes;
}

static const unsigned char *hook_store_id(void *ud) {
    JobEngine *je = ud;
    pthread_mutex_lock(&je->state_lock);
    const unsigned char *id = je->store_id_set ? je->store_id : NULL;
    pthread_mutex_unlock(&je->state_lock);
    return id;
}

static uint64_t hook_version_hwm(void *ud) {
    JobEngine *je = ud;
    pthread_mutex_lock(&je->state_lock);
    uint64_t v = je->version_hwm;
    pthread_mutex_unlock(&je->state_lock);
    return v;
}

static uint64_t hook_commit_hwm(void *ud) {
    JobEngine *je = ud;
    pthread_mutex_lock(&je->state_lock);
    uint64_t v = je->commit_hwm;
    pthread_mutex_unlock(&je->state_lock);
    return v;
}

QueueJobReplayHooks job_engine_replay_hooks(JobEngine *je) {
    QueueJobReplayHooks hooks;
    memset(&hooks, 0, sizeof hooks);
    hooks.ud = je;
    hooks.state_apply = hook_state_apply;
    hooks.completion_apply = hook_completion_apply;
    hooks.receipt_apply = hook_receipt_apply;
    hooks.meta_apply = hook_meta_apply;
    hooks.checkpoint_emit = hook_checkpoint_emit;
    hooks.live_bytes = hook_live_bytes;
    hooks.store_id = hook_store_id;
    hooks.version_hwm = hook_version_hwm;
    hooks.commit_hwm = hook_commit_hwm;
    return hooks;
}
