/* Core tests for atomic job completion: the single-record commit,
 * replay-before-fencing dispatch order, delivery fencing, same-Queue
 * capacity, and concurrent racing completions. File-backed stores only
 * (the feature requires a durable Queue WAL). */
#include "job_int.h"
#include "job_state.h"
#include "job_completion.h"

#include <pthread.h>
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

static void temp_path(char *out, size_t cap) {
    snprintf(out, cap, "/tmp/kuttidb-jc-XXXXXX");
    int fd = mkstemp(out);
    if (fd < 0) { perror("mkstemp"); exit(1); }
    close(fd);
    unlink(out);
}

typedef struct {
    QueueStore *store;
    JobEngine *je;
    char path[128];
} Rig;

static void rig_open(Rig *rig) {
    memset(rig, 0, sizeof *rig);
    temp_path(rig->path, sizeof rig->path);
    rig->je = job_engine_create(NULL);
    if (!rig->je) { perror("engine"); exit(1); }
    QueueJobReplayHooks hooks = job_engine_replay_hooks(rig->je);
    int err = 0;
    rig->store = queue_store_open_ex(rig->path, 1, &hooks, &err);
    if (!rig->store) { fprintf(stderr, "open err=%d\n", err); exit(1); }
    if (job_engine_attach(rig->je, rig->store) != JOB_OK) {
        fprintf(stderr, "attach failed\n");
        exit(1);
    }
}

static void rig_close(Rig *rig) {
    job_engine_destroy(rig->je);
    queue_store_close(rig->store);
}

/* Reopens the same path: a fresh engine + replayed store (recovery). */
static void rig_reopen(Rig *rig) {
    job_engine_destroy(rig->je);
    queue_store_close(rig->store);
    rig->je = job_engine_create(NULL);
    if (!rig->je) { perror("engine"); exit(1); }
    QueueJobReplayHooks hooks = job_engine_replay_hooks(rig->je);
    int err = 0;
    rig->store = queue_store_open_ex(rig->path, 1, &hooks, &err);
    if (!rig->store) { fprintf(stderr, "reopen err=%d\n", err); exit(1); }
    if (job_engine_attach(rig->je, rig->store) != JOB_OK) {
        fprintf(stderr, "attach failed on reopen\n");
        exit(1);
    }
}

static void setup_queues(Rig *rig, const char *in, const char *out,
                         uint64_t in_max, uint64_t out_max) {
    CHECK(queue_declare(rig->store, in, (uint32_t)strlen(in), 1, in_max) == 0,
          "declare input");
    if (out)
        CHECK(queue_declare(rig->store, out, (uint32_t)strlen(out), 1,
                            out_max) == 0,
              "declare output");
    uint64_t owner = 0;
    CHECK(queue_consumer_register(rig->store, "w", 1, &owner) == 0,
          "register consumer");
}

static JobDelivery deliver_one(Rig *rig, const char *queue,
                               const char *payload) {
    CHECK(queue_publish(rig->store, queue, (uint32_t)strlen(queue), payload,
                        (uint32_t)strlen(payload), 0, NULL) == 0,
          "publish input");
    JobDelivery d;
    JobStatus st = job_consume(rig->je, queue, (uint32_t)strlen(queue), "w", 1,
                               30000, &d);
    CHECK(st == JOB_OK, "job consume");
    return d;
}

static void fill_request(JobCompletionRequest *req, const JobDelivery *d,
                         const unsigned char *op_id, uint64_t expected,
                         const char *state_value, const char *out_queue,
                         uint64_t out_inc, const char *out_payload) {
    memset(req, 0, sizeof *req);
    req->input_queue = d->queue;
    req->input_queue_len = d->queue_len;
    req->input_incarnation = d->queue_incarnation;
    req->input_message_id = d->message_id;
    req->proof = d->proof;
    req->state_key = "s";
    req->state_key_len = 1;
    req->expected_version = expected;
    req->state_value = state_value;
    req->state_value_len = state_value ? (uint32_t)strlen(state_value) : 0;
    if (out_queue) {
        req->has_output = 1;
        req->output_queue = out_queue;
        req->output_queue_len = (uint32_t)strlen(out_queue);
        req->output_incarnation = out_inc;
        req->output_value = out_payload;
        req->output_value_len = out_payload ? (uint32_t)strlen(out_payload) : 0;
    }
    memcpy(req->op_id, op_id, 16);
}

static uint64_t queue_incarnation_of(Rig *rig, const char *name) {
    uint64_t inc = 0;
    queue_incarnation(rig->store, name, (uint32_t)strlen(name), &inc);
    return inc;
}

static int queue_depth_of(Rig *rig, const char *name) {
    /* Queue depth counts every retained message (ready + in-flight). */
    uint64_t depth = 0, inflight = 0;
    queue_stats(rig->store, name, (uint32_t)strlen(name), &depth, &inflight);
    (void)inflight;
    return (int)depth;
}

static void test_commit_and_replay(void) {
    Rig rig;
    rig_open(&rig);
    setup_queues(&rig, "in", "out", 0, 0);
    JobDelivery d = deliver_one(&rig, "in", "job-1");
    uint64_t out_inc = queue_incarnation_of(&rig, "out");

    unsigned char op[16];
    memset(op, 0xAA, 16);
    JobCompletionRequest req;
    fill_request(&req, &d, op, 0, "result-1", "out", out_inc, "next-1");
    JobCompletionResult res;
    CHECK(job_complete(rig.je, &req, &res) == JOB_OK, "commit completion");
    CHECK(!res.replayed && res.commit_id != 0 && res.state_version == 1 &&
              res.output_message_id != 0,
          "commit: versions and output id");
    job_delivery_free(&d);

    /* state + output + input-ACK all applied */
    JobStateValue v;
    CHECK(job_state_get(rig.je, "s", 1, &v) == JOB_OK && v.version == 1 &&
              v.len == 8 && memcmp(v.value, "result-1", 8) == 0,
          "commit: state written");
    free(v.value);
    CHECK(queue_depth_of(&rig, "in") == 0, "commit: input acked");
    CHECK(queue_depth_of(&rig, "out") == 1, "commit: output published");

    /* replay with the SAME semantic request and id (proof is now stale —
     * the delivery is gone) must return the original result */
    JobCompletionRequest replay;
    fill_request(&replay, &d, op, 0, "result-1", "out", out_inc, "next-1");
    JobCompletionResult res2;
    CHECK(job_complete(rig.je, &replay, &res2) == JOB_OK, "replay after ack");
    CHECK(res2.replayed == 1 && res2.commit_id == res.commit_id &&
              res2.state_version == res.state_version &&
              res2.output_message_id == res.output_message_id &&
              res2.completed_at_ms == res.completed_at_ms &&
              res2.receipt_expires_ms == res.receipt_expires_ms,
          "replay: immutable original result");
    CHECK(queue_depth_of(&rig, "out") == 1, "replay: no republish");

    /* same id, different semantic request -> idempotency conflict */
    JobCompletionRequest different;
    fill_request(&different, &d, op, 0, "changed", "out", out_inc, "next-1");
    JobCompletionResult res3;
    CHECK(job_complete(rig.je, &different, &res3) == JOB_IDEMPOTENCY_CONFLICT,
          "same id different request conflicts");
    /* same id, no output -> also a conflict (output absence is semantic) */
    JobCompletionRequest noout;
    fill_request(&noout, &d, op, 0, "result-1", NULL, 0, NULL);
    CHECK(job_complete(rig.je, &noout, &res3) == JOB_IDEMPOTENCY_CONFLICT,
          "same id output absence conflicts");

    /* a different id for the (already acked) input is fenced */
    unsigned char op2[16];
    memset(op2, 0xBB, 16);
    JobCompletionRequest other;
    fill_request(&other, &d, op2, 0, "result-1", "out", out_inc, "next-1");
    JobStatus st = job_complete(rig.je, &other, &res3);
    CHECK(st == JOB_DELIVERY_NOT_OWNED || st == JOB_DELIVERY_EXPIRED,
          "second id fenced off the consumed delivery");
    rig_close(&rig);
}

static void test_replay_before_fence(void) {
    /* Guarantee 6: a matching committed receipt is returned before
     * consulting the now-stale delivery — even after restart. */
    Rig rig;
    rig_open(&rig);
    setup_queues(&rig, "in", NULL, 0, 0);
    JobDelivery d = deliver_one(&rig, "in", "job-2");
    unsigned char op[16];
    memset(op, 0xCC, 16);
    JobCompletionRequest req;
    fill_request(&req, &d, op, 0, "result-2", NULL, 0, NULL);
    JobCompletionResult res;
    CHECK(job_complete(rig.je, &req, &res) == JOB_OK, "fence: first commit");
    job_delivery_free(&d);

    rig_reopen(&rig); /* restart: proofs and delivery tags are gone */
    JobCompletionRequest retry;
    /* zeroed proof + a fresh (bogus) delivery view: replay must still win */
    JobDelivery stale;
    memset(&stale, 0, sizeof stale);
    strcpy(stale.queue, "in");
    stale.queue_len = 2;
    stale.queue_incarnation = queue_incarnation_of(&rig, "in");
    stale.message_id = req.input_message_id;
    memset(stale.proof, 0xEE, 16);
    fill_request(&retry, &stale, op, 0, "result-2", NULL, 0, NULL);
    JobCompletionResult res2;
    JobStatus rst = job_complete(rig.je, &retry, &res2);
    if (rst != JOB_OK)
        fprintf(stderr, "[replaydbg] status=%d (%s)\n", rst,
                job_status_name(rst));
    CHECK(rst == JOB_OK, "fence: replay precedes proof validation");
    CHECK(res2.replayed == 1 && res2.commit_id == res.commit_id,
          "fence: original receipt across restart");
    rig_close(&rig);
}

static void test_fencing(void) {
    /* Expired lease: a new-id completion must be refused; the same message
     * re-delivered under a fresh proof commits. */
    Rig rig;
    rig_open(&rig);
    setup_queues(&rig, "in", NULL, 0, 0);
    CHECK(queue_publish(rig.store, "in", 2, "job-3", 5, 0, NULL) == 0,
          "fence: publish");
    JobDelivery d;
    CHECK(job_consume(rig.je, "in", 2, "w", 1, 1, &d) == JOB_OK,
          "fence: consume with a 1 ms lease");
    usleep(5000); /* let the lease pass */
    unsigned char op[16];
    memset(op, 0xDD, 16);
    JobCompletionRequest req;
    fill_request(&req, &d, op, 0, "late", NULL, 0, NULL);
    JobCompletionResult res;
    CHECK(job_complete(rig.je, &req, &res) == JOB_DELIVERY_EXPIRED,
          "fence: expired lease cannot commit");
    { uint64_t dd=0, ii=0; queue_stats(rig.store, "in", 2, &dd, &ii);
      fprintf(stderr, "[depthdbg] fence depth=%llu inflight=%llu\n",
              (unsigned long long)dd, (unsigned long long)ii); }
    CHECK(queue_depth_of(&rig, "in") == 1, "fence: input retained");
    /* the stale proof cannot be refreshed by retry: still expired */
    CHECK(job_complete(rig.je, &req, &res) == JOB_DELIVERY_EXPIRED,
          "fence: stale proof stays expired");

    /* a fresh delivery of the same message is a different attempt */
    JobDelivery d2;
    CHECK(job_consume(rig.je, "in", 2, "w", 1, 30000, &d2) == JOB_OK &&
              d2.message_id == d.message_id && d2.redelivered == 1,
          "fence: redelivery of the same message");
    JobCompletionRequest req2;
    fill_request(&req2, &d2, op, 0, "late", NULL, 0, NULL);
    CHECK(job_complete(rig.je, &req2, &res) == JOB_OK,
          "fence: fresh attempt commits");
    job_delivery_free(&d);
    job_delivery_free(&d2);
    rig_close(&rig);
}

static void test_forged_credentials(void) {
    Rig rig;
    rig_open(&rig);
    setup_queues(&rig, "in", NULL, 0, 0);
    JobDelivery d = deliver_one(&rig, "in", "job-4");
    unsigned char op[16];
    memset(op, 0x21, 16);
    JobCompletionResult res;

    /* wrong proof: not owned */
    JobDelivery forged = d;
    unsigned char wrong[16];
    memset(wrong, 0x11, 16);
    memcpy(forged.proof, wrong, 16);
    JobCompletionRequest req2;
    fill_request(&req2, &forged, op, 0, "x", NULL, 0, NULL);
    CHECK(job_complete(rig.je, &req2, &res) == JOB_DELIVERY_NOT_OWNED,
          "forge: unguessable proof enforced");

    /* wrong message id under a valid proof */
    JobDelivery swapped = d;
    swapped.message_id = 999;
    JobCompletionRequest req3;
    fill_request(&req3, &swapped, op, 0, "x", NULL, 0, NULL);
    CHECK(job_complete(rig.je, &req3, &res) == JOB_DELIVERY_NOT_OWNED,
          "forge: proof bound to its message");

    /* wrong incarnation (stale identity after delete/recreate) */
    uint64_t revision = 0;
    CHECK(queue_revision(rig.store, "in", 2, &revision) == 1, "forge: revision");
    CHECK(queue_delete_if_revision(rig.store, "in", 2, revision, NULL) == 1,
          "forge: delete queue");
    CHECK(queue_declare(rig.store, "in", 2, 1, 0) == 0, "forge: recreate");
    uint64_t fresh_inc = queue_incarnation_of(&rig, "in");
    CHECK(fresh_inc != d.queue_incarnation, "forge: incarnation changed");
    JobDelivery stale = d;
    stale.queue_incarnation = d.queue_incarnation; /* old identity */
    JobCompletionRequest req4;
    fill_request(&req4, &stale, op, 0, "x", NULL, 0, NULL);
    CHECK(job_complete(rig.je, &req4, &res) == JOB_DELIVERY_NOT_OWNED,
          "forge: old incarnation cannot commit");
    job_delivery_free(&d);
    rig_close(&rig);
}

static void test_same_queue_and_capacity(void) {
    /* Same-Queue output at its maximum depth: net capacity admits. */
    Rig rig;
    rig_open(&rig);
    CHECK(queue_declare(rig.store, "solo", 4, 1, 2) == 0, "cap: declare");
    uint64_t owner = 0;
    CHECK(queue_consumer_register(rig.store, "w", 1, &owner) == 0,
          "cap: consumer");
    uint64_t m1 = 0, m2 = 0;
    queue_publish(rig.store, "solo", 4, "a", 1, 0, &m1);
    queue_publish(rig.store, "solo", 4, "b", 1, 0, &m2);
    CHECK(queue_depth_of(&rig, "solo") == 2, "cap: full");
    JobDelivery d;
    CHECK(job_consume(rig.je, "solo", 4, "w", 1, 30000, &d) == JOB_OK,
          "cap: consume");
    unsigned char op[16];
    memset(op, 0x31, 16);
    JobCompletionRequest req;
    fill_request(&req, &d, op, 0, "r", "solo", d.queue_incarnation, "next");
    JobCompletionResult res;
    CHECK(job_complete(rig.je, &req, &res) == JOB_OK,
          "cap: same-queue net admission at max depth");
    CHECK(queue_depth_of(&rig, "solo") == 2,
          "cap: projected depth unchanged");
    job_delivery_free(&d);
    rig_close(&rig);

    /* A full distinct output Queue must not cause the input ACK. */
    rig_open(&rig);
    CHECK(queue_declare(rig.store, "in", 2, 1, 0) == 0, "cap2: in");
    CHECK(queue_declare(rig.store, "out", 3, 1, 1) == 0, "cap2: out max 1");
    uint64_t owner2 = 0;
    queue_consumer_register(rig.store, "w", 1, &owner2);
    queue_publish(rig.store, "in", 2, "j", 1, 0, NULL);
    queue_publish(rig.store, "out", 3, "occupied", 8, 0, NULL);
    uint64_t out_inc = queue_incarnation_of(&rig, "out");
    JobDelivery d2;
    CHECK(job_consume(rig.je, "in", 2, "w", 1, 30000, &d2) == JOB_OK,
          "cap2: consume");
    unsigned char op2[16];
    memset(op2, 0x32, 16);
    JobCompletionRequest req2;
    fill_request(&req2, &d2, op2, 0, "r", "out", out_inc, "n");
    CHECK(job_complete(rig.je, &req2, &res) == JOB_RESOURCE_EXHAUSTED,
          "cap2: full output queue rejects");
    CHECK(queue_depth_of(&rig, "in") == 1,
          "cap2: input NOT acked on rejection");
    /* with no output the same completion commits */
    JobCompletionRequest req3;
    fill_request(&req3, &d2, op2, 0, "r", NULL, 0, NULL);
    CHECK(job_complete(rig.je, &req3, &res) == JOB_OK,
          "cap2: output-less completion commits");
    job_delivery_free(&d2);
    rig_close(&rig);
}

static void test_cas_conflict_inside_completion(void) {
    Rig rig;
    rig_open(&rig);
    setup_queues(&rig, "in", NULL, 0, 0);
    /* Pre-existing state at version 1: a completion with expected 5 must
     * conflict and leave the input unacked. */
    unsigned char seed[16];
    memset(seed, 0x77, 16);
    JobMutationReceipt mr;
    CHECK(job_state_put(rig.je, "s", 1, "v0", 2, 0, seed, &mr) == JOB_OK,
          "cas: seed state");
    JobDelivery d = deliver_one(&rig, "in", "job-5");
    unsigned char op[16];
    memset(op, 0x41, 16);
    JobCompletionRequest req;
    fill_request(&req, &d, op, 5, "v1", NULL, 0, NULL);
    JobCompletionResult res;
    CHECK(job_complete(rig.je, &req, &res) == JOB_STATE_VERSION_CONFLICT,
          "cas: stale expected version rejected");
    { uint64_t dd=0, ii=0; queue_stats(rig.store, "in", 2, &dd, &ii);
      fprintf(stderr, "[depthdbg] cas depth=%llu inflight=%llu\n",
              (unsigned long long)dd, (unsigned long long)ii); }
    CHECK(queue_depth_of(&rig, "in") == 1,
          "cas: input unacked on conflict");
    CHECK(job_state_get(rig.je, "s", 1, &(JobStateValue){0}) == JOB_OK,
          "cas: state still present");
    JobStateValue v;
    CHECK(job_state_get(rig.je, "s", 1, &v) == JOB_OK && v.version == 1,
          "cas: state version unchanged");
    free(v.value);
    /* matching version commits */
    JobCompletionRequest req2;
    fill_request(&req2, &d, op, 1, "v1", NULL, 0, NULL);
    CHECK(job_complete(rig.je, &req2, &res) == JOB_OK && res.state_version == 2,
          "cas: matching version commits");
    job_delivery_free(&d);
    rig_close(&rig);
}

typedef struct {
    Rig *rig;
    JobCompletionRequest *req;
    JobStatus status;
    JobCompletionResult result;
} RaceArg;

static void *race_thread(void *p) {
    RaceArg *a = p;
    a->status = job_complete(a->rig->je, a->req, &a->result);
    return NULL;
}

static void test_concurrent_races(void) {
    /* Same id, two simultaneous submissions: one commits, the other
     * replays the same receipt (never a second effect). */
    Rig rig;
    rig_open(&rig);
    setup_queues(&rig, "in", "out", 0, 0);
    uint64_t out_inc = queue_incarnation_of(&rig, "out");
    JobDelivery d = deliver_one(&rig, "in", "job-6");
    unsigned char op[16];
    memset(op, 0x51, 16);
    JobCompletionRequest r1, r2;
    fill_request(&r1, &d, op, 0, "r", "out", out_inc, "n");
    fill_request(&r2, &d, op, 0, "r", "out", out_inc, "n");
    RaceArg a1 = {&rig, &r1, JOB_OK, {0}}, a2 = {&rig, &r2, JOB_OK, {0}};
    pthread_t t1, t2;
    pthread_create(&t1, NULL, race_thread, &a1);
    pthread_create(&t2, NULL, race_thread, &a2);
    pthread_join(t1, NULL);
    pthread_join(t2, NULL);
    CHECK(a1.status == JOB_OK && a2.status == JOB_OK, "race: same id both ok");
    CHECK((a1.result.replayed + a2.result.replayed) == 1,
          "race: exactly one first commit");
    CHECK(a1.result.commit_id == a2.result.commit_id &&
              a1.result.output_message_id == a2.result.output_message_id,
          "race: identical receipts");
    CHECK(queue_depth_of(&rig, "out") == 1, "race: one output only");
    job_delivery_free(&d);
    rig_close(&rig);
}

int main(void) {
    test_commit_and_replay();
    test_replay_before_fence();
    test_fencing();
    test_forged_credentials();
    test_same_queue_and_capacity();
    test_cas_conflict_inside_completion();
    test_concurrent_races();
    if (failures) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("test_job_completion: OK\n");
    return 0;
}
