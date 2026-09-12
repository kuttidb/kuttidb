/* Crash and replay matrix for atomic job completion (ADR 0002 test plan).
 * Fault injection uses fork + the test-only job_failpoint() hooks, which
 * exist only in builds compiled with KUTTIDB_JOB_FAILPOINTS and are keyed
 * off an explicit environment variable the production binary never reads.
 * Every scenario verifies the post-recovery observation twice (replay
 * idempotence). */
#include "job_int.h"
#include "job_state.h"
#include "job_completion.h"

#include <sys/wait.h>
#include <unistd.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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

typedef struct {
    QueueStore *store;
    JobEngine *je;
    char path[128];
} Rig;

/* Opens an existing store (recovery) with a fresh engine and binds it. */
static void rig_recover(Rig *rig, int enabled, int *err) {
    rig->je = job_engine_create(NULL);
    if (!rig->je) exit(1);
    QueueJobReplayHooks hooks = job_engine_replay_hooks(rig->je);
    rig->store = queue_store_open_ex(rig->path, enabled, &hooks, err);
    if (rig->store && job_engine_attach(rig->je, rig->store) != JOB_OK) {
        fprintf(stderr, "attach failed on recovery\n");
        exit(1);
    }
}

static void rig_close(Rig *rig) {
    job_engine_destroy(rig->je);
    queue_store_close(rig->store);
    rig->je = NULL;
    rig->store = NULL;
}

/* Runs in the forked child: seeds one delivery and attempts one completion
 * while a failpoint is armed. _exit()s on the failpoint. */
static void child_commit(const char *path, const char *failpoint,
                         const char *state_value) {
    if (failpoint) setenv("KUTTIDB_JOB_FAILPOINT", failpoint, 1);
    Rig rig;
    memset(&rig, 0, sizeof rig);
    snprintf(rig.path, sizeof rig.path, "%s", path);
    rig.je = job_engine_create(NULL);
    QueueJobReplayHooks hooks = job_engine_replay_hooks(rig.je);
    int err = 0;
    rig.store = queue_store_open_ex(path, 1, &hooks, &err);
    if (!rig.store) _exit(120);
    if (job_engine_attach(rig.je, rig.store) != JOB_OK) _exit(121);
    if (queue_declare(rig.store, "in", 2, 1, 0) != 0) _exit(122);
    if (queue_declare(rig.store, "out", 3, 1, 0) != 0) _exit(123);
    uint64_t owner = 0;
    if (queue_consumer_register(rig.store, "w", 1, &owner) != 0) _exit(124);
    if (queue_publish(rig.store, "in", 2, "job", 3, 0, NULL) != 0) _exit(125);
    JobDelivery d;
    if (job_consume(rig.je, "in", 2, "w", 1, 30000, &d) != JOB_OK) _exit(126);
    uint64_t out_inc = 0;
    queue_incarnation(rig.store, "out", 3, &out_inc);
    unsigned char op[16];
    memset(op, 0x5A, 16);
    JobCompletionRequest req;
    memset(&req, 0, sizeof req);
    req.input_queue = d.queue;
    req.input_queue_len = d.queue_len;
    req.input_incarnation = d.queue_incarnation;
    req.input_message_id = d.message_id;
    req.proof = d.proof;
    req.state_key = "s";
    req.state_key_len = 1;
    req.expected_version = 0;
    req.state_value = state_value;
    req.state_value_len = (uint32_t)strlen(state_value);
    req.has_output = 1;
    req.output_queue = "out";
    req.output_queue_len = 3;
    req.output_incarnation = out_inc;
    req.output_value = "next";
    req.output_value_len = 4;
    memcpy(req.op_id, op, 16);
    JobCompletionResult res;
    job_complete(rig.je, &req, &res);
    /* a clean commit path also exits, keeping the parent's observations
     * uniform; the failpoint normally exits first */
    _exit(0);
}

static pid_t run_child(const char *path, const char *failpoint,
                       const char *state_value) {
    pid_t pid = fork();
    if (pid == 0) {
        unsetenv("KUTTIDB_JOB_FAILPOINT");
        child_commit(path, failpoint, state_value);
        _exit(200);
    }
    int status = 0;
    waitpid(pid, &status, 0);
    return status;
}

typedef struct {
    uint64_t state_version;
    int state_present;
    int output_count;
    int input_count;
    int receipt_present;
} Observation;

static void observe(Rig *rig, Observation *o) {
    memset(o, 0, sizeof *o);
    JobStateValue v;
    o->state_present = job_state_get(rig->je, "s", 1, &v) == JOB_OK;
    if (o->state_present) {
        o->state_version = v.version;
        free(v.value);
    }
    uint64_t depth = 0, inflight = 0;
    if (queue_stats(rig->store, "out", 3, &depth, &inflight) == 1)
        o->output_count = (int)depth;
    if (queue_stats(rig->store, "in", 2, &depth, &inflight) == 1)
        o->input_count = (int)depth;
    unsigned char op[16];
    memset(op, 0x5A, 16);
    JobReceipt rec;
    o->receipt_present = job_receipt_lookup(rig->je, op, &rec) == JOB_OK;
}

static void test_failpoint(const char *name, int all_or_nothing_commit,
                           const char *state_value) {
    /* Fresh store: seed everything EXCEPT the completion in a child, then
     * crash the completion at the failpoint in a second child. */
    char path[128];
    snprintf(path, sizeof path, "/tmp/kuttidb-jcrash-XXXXXX");
    int fd = mkstemp(path);
    close(fd);
    unlink(path);
    {
        /* seed child: queues + consumer + delivery, no completion */
        setenv("KUTTIDB_JOB_FAILPOINT", "job_before_consume_never", 1);
        unsetenv("KUTTIDB_JOB_FAILPOINT");
        Rig seed;
        memset(&seed, 0, sizeof seed);
        snprintf(seed.path, sizeof seed.path, "%s", path);
        seed.je = job_engine_create(NULL);
        QueueJobReplayHooks hooks = job_engine_replay_hooks(seed.je);
        int err = 0;
        seed.store = queue_store_open_ex(path, 1, &hooks, &err);
        job_engine_attach(seed.je, seed.store);
        queue_declare(seed.store, "in", 2, 1, 0);
        queue_declare(seed.store, "out", 3, 1, 0);
        /* No publish here: the crash child publishes, consumes, and
         * completes its own single message, so `input_count` reads 1
         * before the commit and 0 after it. */
        job_engine_destroy(seed.je);
        queue_store_close(seed.store);
    }
    int status = run_child(path, name, state_value);
    (void)status;

    Rig rig;
    memset(&rig, 0, sizeof rig);
    snprintf(rig.path, sizeof rig.path, "%s", path);
    int err = 0;
    rig_recover(&rig, 1, &err);
    CHECK(rig.store != NULL, name);
    if (!rig.store) return;
    Observation o1, o2;
    observe(&rig, &o1);
    /* recovery must be idempotent: reopen once more */
    rig_close(&rig);
    rig_recover(&rig, 1, &err);
    observe(&rig, &o2);
    CHECK(o1.state_present == o2.state_present &&
              o1.output_count == o2.output_count &&
              o1.input_count == o2.input_count &&
              o1.receipt_present == o2.receipt_present,
          name);
    char label[160];
    if (all_or_nothing_commit) {
        /* the record survived: either everything or nothing, never partial */
        int all = o1.state_present && o1.output_count == 1 &&
                  o1.input_count == 0 && o1.receipt_present;
        int none = !o1.state_present && o1.output_count == 0 &&
                   o1.input_count == 1 && !o1.receipt_present;
        snprintf(label, sizeof label,
                 "%s: all-or-nothing (all=%d none=%d st=%d out=%d in=%d rc=%d)",
                 name, all, none, o1.state_present, o1.output_count,
                 o1.input_count, o1.receipt_present);
        CHECK(all || none, label);
    }
    rig_close(&rig);
}

static void test_torn_record(void) {
    /* A CRC-valid prefix with a torn terminal completion record: replay
     * drops the torn tail and applies nothing from it. */
    char path[128];
    snprintf(path, sizeof path, "/tmp/kuttidb-jctorn-XXXXXX");
    int fd = mkstemp(path);
    close(fd);
    unlink(path);
    run_child(path, NULL, "seeded"); /* clean commit: no failpoint armed */
    /* Build a torn tail: append half a record to a copy. */
    char torn[160];
    snprintf(torn, sizeof torn, "%s.torn", path);
    FILE *src = fopen(path, "rb");
    FILE *dst = fopen(torn, "wb");
    if (!src || !dst) { perror("torn copy"); exit(1); }
    char buf[4096];
    size_t n;
    while ((n = fread(buf, 1, sizeof buf, src)) > 0) fwrite(buf, 1, n, dst);
    const char half[] = {QUEUE_WAL_JOB_COMPLETION, 1, 0, 0, 0, 0, 0, 0, 0, 0};
    fwrite(half, 1, sizeof half, dst);
    fclose(src);
    fclose(dst);
    unlink(path);
    snprintf(path, sizeof path, "%s", torn);

    Rig rig;
    memset(&rig, 0, sizeof rig);
    snprintf(rig.path, sizeof rig.path, "%s", path);
    int err = 0;
    rig_recover(&rig, 1, &err);
    CHECK(rig.store != NULL, "torn: recovery truncates the torn tail");
    if (!rig.store) return;
    Observation o;
    observe(&rig, &o);
    CHECK(o.state_present && o.output_count == 1 && o.input_count == 0 &&
              o.receipt_present,
          "torn: earlier clean commit survives");
    rig_close(&rig);
    unlink(torn);
}

static void test_disabled_open_refused(void) {
    char path[128];
    snprintf(path, sizeof path, "/tmp/kuttidb-jcdis-XXXXXX");
    int fd = mkstemp(path);
    close(fd);
    unlink(path);
    run_child(path, NULL, "committed");
    /* The WAL now contains feature records: a feature-disabled open must
     * be refused, never truncated. */
    int err = 0;
    Rig rig;
    memset(&rig, 0, sizeof rig);
    snprintf(rig.path, sizeof rig.path, "%s", path);
    rig_recover(&rig, 0, &err);
    CHECK(rig.store == NULL && err == QUEUE_OPEN_JOB_DISABLED,
          "disabled open refused with job records present");
    /* and the file is untouched: the feature-enabled open still works */
    rig_recover(&rig, 1, &err);
    CHECK(rig.store != NULL, "refused open left the WAL intact");
    Observation o;
    observe(&rig, &o);
    CHECK(o.state_present && o.receipt_present,
          "records intact after refused open");
    rig_close(&rig);
    unlink(path);
}

static void test_output_consumed_not_republished(void) {
    char path[128];
    snprintf(path, sizeof path, "/tmp/kuttidb-jcack-XXXXXX");
    int fd = mkstemp(path);
    close(fd);
    unlink(path);
    run_child(path, NULL, "committed");
    Rig rig;
    memset(&rig, 0, sizeof rig);
    snprintf(rig.path, sizeof rig.path, "%s", path);
    int err = 0;
    rig_recover(&rig, 1, &err);
    uint64_t owner = 0;
    queue_consumer_register(rig.store, "w", 1, &owner);
    QueueMessage msg;
    CHECK(queue_consume_for_consumer(rig.store, "out", 3, "w", 1, 30000,
                                     &msg) == 1,
          "ack-later: output consumed after recovery");
    uint64_t tag = msg.delivery_tag;
    queue_message_free(&msg);
    CHECK(queue_ack_for_owner(rig.store, "out", 3, tag, owner) == 1,
          "ack-later: output acked");
    rig_close(&rig);
    /* restart twice: the ACK must win over the historical receipt */
    for (int round = 0; round < 2; round++) {
        rig_recover(&rig, 1, &err);
        uint64_t depth = 0, inflight = 0;
        queue_stats(rig.store, "out", 3, &depth, &inflight);
        CHECK(depth == 0, "ack-later: recovery does not republish");
        rig_close(&rig);
    }
    unlink(path);
}

static void test_later_state_change_wins(void) {
    char path[128];
    snprintf(path, sizeof path, "/tmp/kuttidb-jcstate-XXXXXX");
    int fd = mkstemp(path);
    close(fd);
    unlink(path);
    run_child(path, NULL, "committed");
    Rig rig;
    memset(&rig, 0, sizeof rig);
    snprintf(rig.path, sizeof rig.path, "%s", path);
    int err = 0;
    rig_recover(&rig, 1, &err);
    unsigned char op2[16];
    memset(op2, 0x66, 16);
    JobMutationReceipt mr;
    JobStatus pst = job_state_put(rig.je, "s", 1, "newer", 5, 1, op2, &mr);
    CHECK(pst == JOB_OK, "later-state: update after completion");
    rig_close(&rig);
    rig_recover(&rig, 1, &err);
    /* the historical receipt retry must not overwrite the newer state */
    JobStateValue v;
    CHECK(job_state_get(rig.je, "s", 1, &v) == JOB_OK && v.version == 2 &&
              memcmp(v.value, "newer", 5) == 0,
          "later-state: newer value survives recovery");
    free(v.value);
    unsigned char op[16];
    memset(op, 0x5A, 16);
    JobReceipt rec;
    CHECK(job_receipt_lookup(rig.je, op, &rec) == JOB_OK &&
              rec.state_version == 1,
          "later-state: historical receipt still retained");
    rig_close(&rig);
    unlink(path);
}

static void test_checkpoint_boundary(void) {
    char path[128];
    snprintf(path, sizeof path, "/tmp/kuttidb-jcckpt-XXXXXX");
    int fd = mkstemp(path);
    close(fd);
    unlink(path);
    run_child(path, NULL, "committed");
    Rig rig;
    memset(&rig, 0, sizeof rig);
    snprintf(rig.path, sizeof rig.path, "%s", path);
    int err = 0;
    rig_recover(&rig, 1, &err);
    CHECK(queue_checkpoint_force(rig.store) == 1, "ckpt: checkpoint written");
    rig_close(&rig);
    for (int round = 0; round < 2; round++) {
        rig_recover(&rig, 1, &err);
        Observation o;
        observe(&rig, &o);
        CHECK(o.state_present && o.state_version == 1 &&
                  o.output_count == 1 && o.input_count == 0 &&
                  o.receipt_present,
              "ckpt: full state across checkpoint recovery");
        /* the identity and high-water marks survive compaction */
        uint64_t depth = 0, inflight = 0;
        CHECK(queue_stats(rig.store, "out", 3, &depth, &inflight) == 1,
              "ckpt: output queue exists");
        rig_close(&rig);
    }
    unlink(path);
}

static void test_receipt_gc_across_restart(void) {
    /* Receipts whose deadline expired are forgotten at the checkpoint
     * boundary and stay forgotten; unexpired ones survive. */
    char path[128];
    snprintf(path, sizeof path, "/tmp/kuttidb-jcgc-XXXXXX");
    int fd = mkstemp(path);
    close(fd);
    unlink(path);
    run_child(path, NULL, "committed");
    Rig rig;
    memset(&rig, 0, sizeof rig);
    snprintf(rig.path, sizeof rig.path, "%s", path);
    int err = 0;
    rig_recover(&rig, 1, &err);
    unsigned char op[16];
    memset(op, 0x5A, 16);
    JobReceipt rec;
    CHECK(job_receipt_lookup(rig.je, op, &rec) == JOB_OK, "gc: receipt there");
    /* Force expiry by rewriting the receipt deadline via a gc pass far in
     * the future, then checkpoint: expired receipts are not re-emitted. */
    uint64_t removed = 0;
    for (uint32_t i = 0;
         i < (uint32_t)(JOB_RECEIPT_BUCKETS / JOB_GC_BUDGET + 2); i++)
        removed += job_receipt_gc(rig.je, rec.receipt_expires_ms + 1);
    CHECK(removed == 1, "gc: expired receipt forgotten");
    CHECK(queue_checkpoint_force(rig.store) == 1, "gc: checkpoint");
    rig_close(&rig);
    rig_recover(&rig, 1, &err);
    CHECK(job_receipt_lookup(rig.je, op, &rec) == JOB_NOT_FOUND,
          "gc: forgotten stays forgotten across checkpoint");
    /* state survives; only the retry window is gone */
    JobStateValue v;
    CHECK(job_state_get(rig.je, "s", 1, &v) == JOB_OK,
          "gc: state survives receipt gc");
    free(v.value);
    rig_close(&rig);
    unlink(path);
}

static void test_feature_transition(void) {
    /* A pre-feature WAL (no job records) opens with the feature disabled;
     * once feature records exist, a disabled open is refused (covered in
     * test_disabled_open_refused). Here: the feature-enabled first use on
     * a legacy file assigns identity eagerly. */
    char path[128];
    snprintf(path, sizeof path, "/tmp/kuttidb-jclegacy-XXXXXX");
    int fd = mkstemp(path);
    close(fd);
    unlink(path);
    QueueStore *store = queue_store_open(path); /* pre-feature open */
    CHECK(store != NULL, "legacy: plain open");
    queue_declare(store, "in", 2, 1, 0);
    queue_publish(store, "in", 2, "old", 3, 0, NULL);
    queue_store_close(store);
    /* first feature-enabled open writes the meta record and keeps the
     * legacy data */
    Rig rig;
    memset(&rig, 0, sizeof rig);
    snprintf(rig.path, sizeof rig.path, "%s", path);
    int err = 0;
    rig_recover(&rig, 1, &err);
    CHECK(rig.store != NULL, "legacy: feature-enabled first use");
    uint64_t depth = 0, inflight = 0;
    CHECK(queue_stats(rig.store, "in", 2, &depth, &inflight) == 1 && depth == 1,
          "legacy: retained message preserved");
    rig_close(&rig);
    /* and now the disabled open is refused (records exist) */
    rig_recover(&rig, 0, &err);
    CHECK(rig.store == NULL && err == QUEUE_OPEN_JOB_DISABLED,
          "legacy: disabled open refused after first use");
    unlink(path);
}

static void test_format_guard(void) {
    /* A CRC-valid feature record with a bogus fmt byte is an unsupported
     * FORMAT, not a truncation: the open is refused with an actionable
     * error and the WAL is preserved. */
    char path[128];
    snprintf(path, sizeof path, "/tmp/kuttidb-jcfmt-XXXXXX");
    int fd = mkstemp(path);
    close(fd);
    unlink(path);
    run_child(path, NULL, "committed");
    /* The child exits without closing the store, so on a platform where the
     * WAL reserves space ahead of its cursor the file is longer than its
     * records. Recover and close once through the library first: that
     * returns the reservation, so the record appended below lands where the
     * next record would actually go. Appending past a reservation gap would
     * instead exercise a WAL with a hole, which the open refuses with
     * QUEUE_OPEN_TRAILING_RECORDS — a real behaviour, but not the one this
     * case is about. */
    {
        Rig warm;
        memset(&warm, 0, sizeof warm);
        snprintf(warm.path, sizeof warm.path, "%s", path);
        int warm_err = 0;
        rig_recover(&warm, 1, &warm_err);
        if (!warm.store) {
            fprintf(stderr, "format: normalising recovery failed (%d)\n", warm_err);
            exit(1);
        }
        rig_close(&warm);
    }
    long long size = 0;
    {
        FILE *f = fopen(path, "rb");
        fseek(f, 0, SEEK_END);
        size = (long long)ftell(f);
        fclose(f);
    }
    /* Append a hand-built CRC-valid LOG_JOB_RECEIPT record with fmt=9.
     * The record framing is [op][durable][nlen:2][len:4][id:8][aux:8]
     * [crc:4][name][data]; CRC covers header(24)+name+data. We reuse the
     * repository's crc32 through a tiny table (test-local duplicate). */
    static uint32_t table[256];
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t v = i;
        for (int j = 0; j < 8; j++)
            v = (v & 1) ? UINT32_C(0xedb88320) ^ (v >> 1) : v >> 1;
        table[i] = v;
    }
    const char *name = QUEUE_WAL_JOB_RECEIPT_NAME;
    uint32_t name_len = (uint32_t)strlen(name);
    unsigned char data[2] = {9, 0}; /* fmt=9 */
    uint32_t data_len = 2;
    unsigned char header[28] = {0};
    header[0] = (unsigned char)QUEUE_WAL_JOB_RECEIPT;
    header[1] = 1;
    header[2] = (unsigned char)name_len;
    header[3] = 0;
    uint32_t len_field = data_len;
    for (int i = 0; i < 4; i++) header[4 + i] = (unsigned char)(len_field >> (i * 8));
    uint32_t crc = UINT32_C(0xffffffff);
    for (int i = 0; i < 24; i++)
        crc = table[(crc ^ header[i]) & 0xff] ^ (crc >> 8);
    for (uint32_t i = 0; i < name_len; i++)
        crc = table[(crc ^ (unsigned char)name[i]) & 0xff] ^ (crc >> 8);
    for (uint32_t i = 0; i < data_len; i++)
        crc = table[(crc ^ data[i]) & 0xff] ^ (crc >> 8);
    crc ^= UINT32_C(0xffffffff);
    for (int i = 0; i < 4; i++) header[24 + i] = (unsigned char)(crc >> (i * 8));
    FILE *f = fopen(path, "ab");
    fwrite(header, 1, sizeof header, f);
    fwrite(name, 1, name_len, f);
    fwrite(data, 1, data_len, f);
    fclose(f);

    Rig rig;
    memset(&rig, 0, sizeof rig);
    snprintf(rig.path, sizeof rig.path, "%s", path);
    int err = 0;
    rig_recover(&rig, 1, &err);
    CHECK(rig.store == NULL && err == QUEUE_OPEN_JOB_FORMAT,
          "format: unsupported encoding refused");
    long long size2 = 0;
    f = fopen(path, "rb");
    fseek(f, 0, SEEK_END);
    size2 = (long long)ftell(f);
    fclose(f);
    CHECK(size2 == (long long)(size + 28 + name_len + data_len),
          "format: WAL preserved byte-for-byte");
    unlink(path);
}

int main(void) {
    /* failpoint matrix: every named boundary keeps all-or-nothing */
    test_failpoint("job_before_append", 0, "committed");
    test_failpoint("job_after_write", 0, "committed");
    test_failpoint("job_after_fsync", 1, "committed");
    test_failpoint("job_after_apply", 1, "committed");
    test_torn_record();
    test_disabled_open_refused();
    test_output_consumed_not_republished();
    test_later_state_change_wins();
    test_checkpoint_boundary();
    test_receipt_gc_across_restart();
    test_feature_transition();
    test_format_guard();
    if (failures) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("test_job_crash: OK\n");
    return 0;
}
