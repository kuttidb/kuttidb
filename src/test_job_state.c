/* Core tests for the durable-state engine and the receipt ledger:
 * SHA-256 known vectors, canonical digest identity, version-checked CAS
 * with ABA prevention, budget admission, retention, and direct mutation
 * receipts. Run against an in-memory or file-backed Queue store. */
#include "job_int.h"
#include "job_state.h"
#include "job_completion.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int failures;

#define CHECK(cond, name)                                     \
    do {                                                      \
        if (!(cond)) {                                        \
            fprintf(stderr, "FAIL: %s\n", name);              \
            failures++;                                       \
        } else {                                              \
            printf("ok: %s\n", name);                         \
        }                                                     \
    } while (0)

static int hexeq(const unsigned char *got, size_t n, const char *want_hex) {
    if (strlen(want_hex) != n * 2) return 0;
    for (size_t i = 0; i < n; i++) {
        unsigned v;
        if (sscanf(want_hex + i * 2, "%2x", &v) != 1) return 0;
        if (got[i] != (unsigned char)v) return 0;
    }
    return 1;
}

static void test_sha256_vectors(void) {
    unsigned char d[32];
    job_sha256(NULL, 0, NULL, 0, NULL, 0, NULL, 0, d);
    CHECK(hexeq(d, 32,
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
          "sha256 empty vector");
    job_sha256("abc", 3, NULL, 0, NULL, 0, NULL, 0, d);
    CHECK(hexeq(d, 32,
                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"),
          "sha256 abc vector");
    const char *long_input =
        "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq";
    job_sha256(long_input, strlen(long_input), NULL, 0, NULL, 0, NULL, 0, d);
    CHECK(hexeq(d, 32,
                "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"),
          "sha256 448-bit vector");
    /* Multi-part hashing equals single-shot hashing. */
    JobShaPart parts[3] = {{"ab", 2}, {"c", 1}, {"", 0}};
    unsigned char d2[32];
    job_sha256_parts(d2, parts, 3);
    job_sha256("abc", 3, NULL, 0, NULL, 0, NULL, 0, d);
    CHECK(memcmp(d, d2, 32) == 0, "sha256 parts equal single-shot");
}

static void test_digest_identity(void) {
    unsigned char op[16], store[16], a[32], b[32];
    for (int i = 0; i < 16; i++) {
        op[i] = (unsigned char)(i + 1);
        store[i] = (unsigned char)(0x40 + i);
    }
    job_canonical_digest(a, JOB_KIND_STATE_PUT, op, store, NULL, 0, 0, 0,
                         "k", 1, 0, "v", 1, 0, NULL, 0, 0, NULL, 0);
    job_canonical_digest(b, JOB_KIND_STATE_PUT, op, store, NULL, 0, 0, 0,
                         "k", 1, 0, "v", 1, 0, NULL, 0, 0, NULL, 0);
    CHECK(memcmp(a, b, 32) == 0, "digest stable across equal requests");
    job_canonical_digest(b, JOB_KIND_STATE_PUT, op, store, NULL, 0, 0, 0,
                         "k", 1, 1, "v", 1, 0, NULL, 0, 0, NULL, 0);
    CHECK(memcmp(a, b, 32) != 0, "digest changes with expected_version");
    job_canonical_digest(b, JOB_KIND_STATE_PUT, op, store, NULL, 0, 0, 0,
                         "k", 1, 0, "w", 1, 0, NULL, 0, 0, NULL, 0);
    CHECK(memcmp(a, b, 32) != 0, "digest changes with value");
    job_canonical_digest(b, JOB_KIND_STATE_DELETE, op, store, NULL, 0, 0, 0,
                         "k", 1, 0, "v", 1, 0, NULL, 0, 0, NULL, 0);
    CHECK(memcmp(a, b, 32) != 0, "digest changes with kind");
    unsigned char op2[16];
    memcpy(op2, op, 16);
    op2[0] ^= 1;
    job_canonical_digest(b, JOB_KIND_STATE_PUT, op2, store, NULL, 0, 0, 0,
                         "k", 1, 0, "v", 1, 0, NULL, 0, 0, NULL, 0);
    CHECK(memcmp(a, b, 32) != 0, "digest changes with operation id");
    unsigned char store2[16];
    memcpy(store2, store, 16);
    store2[15] ^= 1;
    job_canonical_digest(b, JOB_KIND_STATE_PUT, op, store2, NULL, 0, 0, 0,
                         "k", 1, 0, "v", 1, 0, NULL, 0, 0, NULL, 0);
    CHECK(memcmp(a, b, 32) != 0, "digest changes with store id");
    job_canonical_digest(b, JOB_KIND_COMPLETION, op, store, "q", 1, 7, 9,
                         "k", 1, 0, "v", 1, 0, NULL, 0, 0, NULL, 0);
    CHECK(memcmp(a, b, 32) != 0, "digest changes with input identity");
    job_canonical_digest(a, JOB_KIND_COMPLETION, op, store, "q", 1, 7, 9,
                         "k", 1, 0, "v", 1, 1, "o", 1, 3, "p", 1);
    job_canonical_digest(b, JOB_KIND_COMPLETION, op, store, "q", 1, 7, 9,
                         "k", 1, 0, "v", 1, 1, "o", 1, 3, "p", 1);
    CHECK(memcmp(a, b, 32) == 0, "completion digest stable");
    job_canonical_digest(b, JOB_KIND_COMPLETION, op, store, "q", 1, 7, 9,
                         "k", 1, 0, "v", 1, 1, "o", 1, 3, "q", 1);
    CHECK(memcmp(a, b, 32) != 0, "completion digest covers output payload");
    /* Empty values and empty output are distinct from absent. */
    job_canonical_digest(a, JOB_KIND_STATE_PUT, op, store, NULL, 0, 0, 0,
                         "k", 1, 0, "", 0, 0, NULL, 0, 0, NULL, 0);
    job_canonical_digest(b, JOB_KIND_STATE_PUT, op, store, NULL, 0, 0, 0,
                         "k", 1, 0, "x", 1, 0, NULL, 0, 0, NULL, 0);
    CHECK(memcmp(a, b, 32) != 0, "empty value differs from one-byte value");
}

typedef struct {
    JobEngineConfig cfg;
    char path[128]; /* file-backed: the feature requires a durable WAL */
} Fixture;

static void fixture_path(Fixture *fx) {
    snprintf(fx->path, sizeof fx->path, "/tmp/kuttidb-job-XXXXXX");
    int fd = mkstemp(fx->path);
    if (fd < 0) { perror("mkstemp"); exit(1); }
    close(fd);
    unlink(fx->path);
}

static JobEngine *fixture_open(Fixture *fx, QueueStore **store_out) {
    if (!fx->path[0]) fixture_path(fx);
    QueueStore *store = queue_store_open(fx->path);
    if (!store) return NULL;
    JobEngine *je = job_engine_create(&fx->cfg);
    if (!je) {
        queue_store_close(store);
        return NULL;
    }
    if (job_engine_attach(je, store) != JOB_OK) {
        job_engine_destroy(je);
        queue_store_close(store);
        return NULL;
    }
    if (store_out) *store_out = store;
    return je;
}

static void fixture_close(JobEngine *je, QueueStore *store) {
    job_engine_destroy(je);
    queue_store_close(store);
}

static void test_state_cas_and_aba(void) {
    QueueStore *store = NULL;
    JobEngine *je = fixture_open(&(Fixture){0}, &store);
    CHECK(je != NULL, "aba: fixture");
    if (!je) return;
    unsigned char op[16];
    memset(op, 7, 16);
    JobMutationReceipt r;
    CHECK(job_state_put(je, "k", 1, "v1", 2, 0, op, &r) == JOB_OK,
          "aba: create put");
    CHECK(r.state_version == 1 && !r.replayed, "aba: first version is 1");

    /* create-only on an existing key conflicts */
    unsigned char op2[16];
    memset(op2, 8, 16);
    CHECK(job_state_put(je, "k", 1, "v2", 2, 0, op2, &r) ==
              JOB_STATE_VERSION_CONFLICT,
          "aba: create-only conflict");

    /* exact-match update */
    CHECK(job_state_put(je, "k", 1, "v2", 2, 1, op2, &r) == JOB_OK,
          "aba: update put");
    CHECK(r.state_version == 2, "aba: version monotonic");

    JobStateValue v;
    CHECK(job_state_get(je, "k", 1, &v) == JOB_OK && v.version == 2 &&
              v.len == 2 && memcmp(v.value, "v2", 2) == 0,
          "aba: read-back");
    free(v.value);

    /* delete requires the current version */
    unsigned char op3[16];
    memset(op3, 9, 16);
    CHECK(job_state_delete(je, "k", 1, 1, op3, &r) == JOB_STATE_VERSION_CONFLICT,
          "aba: stale delete conflicts");
    CHECK(job_state_delete(je, "k", 1, 2, op3, &r) == JOB_OK, "aba: delete");
    CHECK(job_state_get(je, "k", 1, &v) == JOB_NOT_FOUND, "aba: gone");
    /* tombstone version consumed: recreate is version 3, never 1 (ABA) */
    unsigned char op4[16];
    memset(op4, 10, 16);
    CHECK(job_state_put(je, "k", 1, "v3", 2, 1, op4, &r) ==
              JOB_STATE_VERSION_CONFLICT,
          "aba: old version invalid after recreate");
    CHECK(job_state_put(je, "k", 1, "v3", 2, 0, op4, &r) == JOB_OK &&
              r.state_version == 4,
          "aba: recreate takes a fresh version (tombstone consumed 3)");
    fixture_close(je, store);
}

static void test_delete_receipts(void) {
    QueueStore *store = NULL;
    JobEngine *je = fixture_open(&(Fixture){0}, &store);
    CHECK(je != NULL, "receipts: fixture");
    if (!je) return;
    unsigned char op[16];
    memset(op, 3, 16);
    JobMutationReceipt r;
    /* deleting an absent key: definite not-found, no mutation */
    CHECK(job_state_delete(je, "absent", 6, 1, op, &r) == JOB_NOT_FOUND,
          "receipts: absent delete is not-found");
    /* commit a put then delete it, then retry the delete by id */
    CHECK(job_state_put(je, "k", 1, "v", 1, 0, op, &r) == JOB_OK,
          "receipts: put");
    unsigned char opd[16];
    memset(opd, 4, 16);
    uint64_t version = 0;
    JobStateValue v;
    CHECK(job_state_get(je, "k", 1, &v) == JOB_OK, "receipts: read version");
    version = v.version;
    free(v.value);
    CHECK(job_state_delete(je, "k", 1, version, opd, &r) == JOB_OK,
          "receipts: delete commit");
    CHECK(r.kind == JOB_KIND_STATE_DELETE, "receipts: delete kind");
    /* retry with the same id returns the retained receipt even though the
     * entry is absent */
    JobMutationReceipt r2;
    CHECK(job_state_delete(je, "k", 1, version, opd, &r2) == JOB_OK &&
              r2.replayed == 1 && r2.commit_id == r.commit_id,
          "receipts: delete replay");
    /* lookup through the shared operation surface */
    JobReceipt rec;
    CHECK(job_receipt_lookup(je, opd, &rec) == JOB_OK &&
              rec.kind == JOB_KIND_STATE_DELETE,
          "receipts: durable-operation lookup");
    unsigned char unknown[16];
    memset(unknown, 5, 16);
    CHECK(job_receipt_lookup(je, unknown, &rec) == JOB_NOT_FOUND,
          "receipts: unknown id is not found");
    /* a put with the delete's id is an idempotency conflict */
    CHECK(job_state_put(je, "k", 1, "v", 1, 0, opd, &r2) ==
              JOB_IDEMPOTENCY_CONFLICT,
          "receipts: kind mismatch conflicts");
    fixture_close(je, store);
}

static void test_budgets(void) {
    JobEngineConfig cfg = {0};
    cfg.state_max_bytes = 512;
    cfg.receipts_max_bytes = 4096;
    cfg.receipts_max_count = 100;
    cfg.receipt_retention_ms = 86400000;
    cfg.max_op_bytes = 512;
    QueueStore *store = NULL;
    JobEngine *je = fixture_open(&(Fixture){.cfg = cfg}, &store);
    CHECK(je != NULL, "budgets: fixture");
    if (!je) return;
    unsigned char op[16];
    memset(op, 1, 16);
    JobMutationReceipt r;
    char big[600];
    memset(big, 'x', sizeof big);
    CHECK(job_state_put(je, "big", 3, big, sizeof big, 0, op, &r) ==
              JOB_REQUEST_TOO_LARGE,
          "budgets: aggregate op limit");
    char small[200];
    memset(small, 'y', sizeof small);
    CHECK(job_state_put(je, "fits", 4, small, sizeof small, 0, op, &r) ==
              JOB_OK,
          "budgets: small put fits");
    unsigned char op2[16];
    memset(op2, 2, 16);
    CHECK(job_state_put(je, "overflow", 8, small, sizeof small, 0, op2, &r) ==
              JOB_RESOURCE_EXHAUSTED,
          "budgets: state budget rejects, never evicts");
    /* existing state is untouched after a rejection */
    JobStateValue v;
    CHECK(job_state_get(je, "fits", 4, &v) == JOB_OK &&
              memcmp(v.value, small, sizeof small) == 0,
          "budgets: rejection leaves state intact");
    free(v.value);
    fixture_close(je, store);

    /* receipt count ceiling */
    cfg.state_max_bytes = 1 << 20;
    cfg.receipts_max_count = 2;
    je = fixture_open(&(Fixture){.cfg = cfg}, &store);
    CHECK(je != NULL, "budgets: count fixture");
    unsigned char op3[16], op4[16], op5[16];
    memset(op3, 11, 16);
    memset(op4, 12, 16);
    memset(op5, 13, 16);
    CHECK(job_state_put(je, "a", 1, "1", 1, 0, op3, &r) == JOB_OK,
          "budgets: count put 1");
    CHECK(job_state_put(je, "b", 1, "1", 1, 0, op4, &r) == JOB_OK,
          "budgets: count put 2");
    CHECK(job_state_put(je, "c", 1, "1", 1, 0, op5, &r) == JOB_RESOURCE_EXHAUSTED,
          "budgets: receipt count ceiling");
    fixture_close(je, store);
}

static void test_retention(void) {
    JobEngineConfig cfg = {0};
    cfg.receipt_retention_ms = 1000; /* minimum accepted range */
    QueueStore *store = NULL;
    JobEngine *je = fixture_open(&(Fixture){.cfg = cfg}, &store);
    CHECK(je != NULL, "retention: fixture");
    if (!je) return;
    unsigned char op[16];
    memset(op, 6, 16);
    JobMutationReceipt r;
    CHECK(job_state_put(je, "k", 1, "v", 1, 0, op, &r) == JOB_OK,
          "retention: commit");
    CHECK(r.receipt_expires_ms >= r.completed_at_ms + 1000,
          "retention: deadline assigned at commit");
    /* nothing forgotten before the deadline */
    CHECK(job_receipt_gc(je, r.completed_at_ms + 500) == 0,
          "retention: gc keeps unexpired");
    JobReceipt rec;
    CHECK(job_receipt_lookup(je, op, &rec) == JOB_OK,
          "retention: receipt retained");
    /* after the deadline the bounded incremental gc forgets it (the window
     * rotates through all buckets; loop until the sweep reaches it) */
    uint64_t removed_total = 0;
    for (unsigned i = 0; i < JOB_RECEIPT_BUCKETS / JOB_GC_BUDGET + 2u; i++)
        removed_total += job_receipt_gc(je, r.receipt_expires_ms + 1);
    CHECK(removed_total == 1, "retention: gc forgets expired");
    CHECK(job_receipt_lookup(je, op, &rec) == JOB_NOT_FOUND,
          "retention: forgotten lookup is absent");
    /* the state itself survives receipt forgetting */
    JobStateValue v;
    CHECK(job_state_get(je, "k", 1, &v) == JOB_OK, "retention: state survives");
    free(v.value);
    /* a retry after forgetting needs a live proof/current CAS, and the old
     * receipt is not resurrected */
    unsigned char op2[16];
    memset(op2, 7, 16);
    CHECK(job_state_put(je, "k", 1, "w", 1, 1, op, &r) == JOB_OK &&
              r.replayed == 0,
          "retention: forgotten id commits as new work");
    fixture_close(je, store);
}

static void test_key_validation(void) {
    QueueStore *store = NULL;
    JobEngine *je = fixture_open(&(Fixture){0}, &store);
    CHECK(je != NULL, "keys: fixture");
    if (!je) return;
    unsigned char op[16];
    memset(op, 1, 16);
    JobMutationReceipt r;
    JobStateValue v;
    CHECK(job_state_put(je, NULL, 0, "v", 1, 0, op, &r) == JOB_VALIDATION_FAILED,
          "keys: empty key rejected");
    char huge[65536];
    memset(huge, 'k', sizeof huge);
    CHECK(job_state_put(je, huge, sizeof huge, "v", 1, 0, op, &r) ==
              JOB_VALIDATION_FAILED,
          "keys: over-limit key rejected");
    static const unsigned char zero[16];
    CHECK(job_state_put(je, "k", 1, "v", 1, 0, zero, &r) ==
              JOB_VALIDATION_FAILED,
          "keys: zero operation id rejected");
    /* empty values are valid */
    CHECK(job_state_put(je, "empty", 5, NULL, 0, 0, op, &r) == JOB_OK,
          "keys: empty value accepted");
    CHECK(job_state_get(je, "empty", 5, &v) == JOB_OK && v.len == 0,
          "keys: empty value read back");
    free(v.value);
    /* binary keys and values are exact bytes (fresh operation id) */
    unsigned char keyb[3] = {0x00, 0xff, 0x7f};
    unsigned char valb[2] = {0xfe, 0x01};
    unsigned char opb[16];
    memset(opb, 0xAB, 16);
    CHECK(job_state_put(je, (const char *)keyb, 3, valb, 2, 0, opb, &r) == JOB_OK,
          "keys: binary key accepted");
    CHECK(job_state_get(je, (const char *)keyb, 3, &v) == JOB_OK &&
              v.len == 2 && memcmp(v.value, valb, 2) == 0,
          "keys: binary exactness");
    free(v.value);
    fixture_close(je, store);
}

int main(void) {
    test_sha256_vectors();
    test_digest_identity();
    test_state_cas_and_aba();
    test_delete_receipts();
    test_budgets();
    test_retention();
    test_key_validation();
    if (failures) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("test_job_state: OK\n");
    return 0;
}
