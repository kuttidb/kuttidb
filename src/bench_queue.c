/* Queue engine depth-scaling benchmark.
 *
 * Drives the QueueStore API directly (no server, no protocol) so results
 * isolate engine cost. Records the evidence BENCHMARKS.md needs for the
 * queue engine:
 *
 *   PUBLISH  - durable publish ops/s as retained depth grows 10k -> 20k
 *   CONSUME  - consume ops/s behind a wall of in-flight deliveries (the
 *              ready-scan pathology) and from a clean head
 *   ACK      - ack ops/s as the outstanding in-flight set grows
 *   NACK     - requeue cost with many outstanding deliveries
 *   VISREAP  - one visibility-expiry pass requeueing 2,000 deliveries
 *   PCACK    - publish+consume+ack steady state, in-memory and durable
 *   METRICS  - per-queue stats scrape at depth
 *
 * Usage: bench-queue [dir] [per_phase_count] [value_bytes]
 */
#define _POSIX_C_SOURCE 200809L
#include "../src/queue.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>

#include <time.h>

static uint64_t now_ns(void) {
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (uint64_t)t.tv_sec * 1000000000ull + (uint64_t)t.tv_nsec;
}

static int cmp_u64(const void *a, const void *b) {
    uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
    return x < y ? -1 : x > y ? 1 : 0;
}

static uint64_t pct(uint64_t *v, size_t n, double p) {
    if (!n) return 0;
    qsort(v, n, sizeof *v, cmp_u64);
    size_t i = (size_t)(p * (double)(n - 1) + 0.5);
    if (i >= n) i = n - 1;
    return v[i];
}

static double secs(uint64_t ns) { return (double)ns / 1e9; }

/* Concurrent durable phases: threads share one store, as the server's event
 * loops do. CONCPUB measures publish throughput with the group fsync; CONCPC
 * runs 4 producers feeding a shared durable queue and 4 consumers draining it
 * with ACKs (a publish+delivery+ack pipeline under contention). */
#define CONC_T 8
#define CONC_N 5000
static QueueStore *conc_store;
static uint64_t *conc_lat;
static void *conc_publish_worker(void *arg) {
    long tid = (long)arg;
    char name[16];
    snprintf(name, sizeof name, "c%ld", tid);
    char payload[100];
    memset(payload, 'x', sizeof payload);
    if (queue_declare(conc_store, name, (uint32_t)strlen(name), 1, 1000000)) return (void *)1;
    for (int i = 0; i < CONC_N; i++) {
        uint64_t st = now_ns();
        if (queue_publish(conc_store, name, (uint32_t)strlen(name), payload,
                          sizeof payload, 0, NULL)) {
            return (void *)1;
        }
        conc_lat[tid * CONC_N + i] = now_ns() - st;
    }
    return NULL;
}
static long pc_consumed;
static void *pc_producer(void *arg) {
    (void)arg;
    char payload[100];
    memset(payload, 'x', sizeof payload);
    for (int i = 0; i < CONC_N; i++) {
        if (queue_publish(conc_store, "shared", 6, payload, sizeof payload, 0, NULL))
            return (void *)1;
    }
    return NULL;
}
static void *pc_consumer(void *arg) {
    (void)arg;
    for (;;) {
        if (__atomic_load_n(&pc_consumed, __ATOMIC_RELAXED) >= 4 * CONC_N)
            return NULL;
        QueueMessage m;
        int rc = queue_consume(conc_store, "shared", 6, 60000, &m);
        if (rc < 0) { fprintf(stderr, "consume rc=%d failed_flag=%d\n", rc, queue_persistence_failed(conc_store)); return (void *)1; }
        if (rc == 0) { usleep(50); continue; }
        int arc = queue_ack(conc_store, "shared", 6, m.delivery_tag);
        if (arc != 1) {
            fprintf(stderr, "ack rc=%d tag=%llu failed_flag=%d\n", arc, (unsigned long long)m.delivery_tag, queue_persistence_failed(conc_store));
            queue_message_free(&m);
            return (void *)1;
        }
        queue_message_free(&m);
        __atomic_add_fetch(&pc_consumed, 1, __ATOMIC_RELAXED);
    }
}

int main(int argc, char **argv) {
    const char *dir = argc > 1 ? argv[1] : "/tmp";
    uint32_t N = argc > 2 ? (uint32_t)strtoul(argv[2], NULL, 10) : 20000u;
    uint32_t rsize = argc > 3 ? (uint32_t)strtoul(argv[3], NULL, 10) : 100u;
    if (rsize < 1) rsize = 1;
    char *payload = malloc(rsize);
    memset(payload, 'x', rsize);
    char path[512];
    snprintf(path, sizeof path, "%s/bench_queue.wal", dir);

    /* ---- durable publish vs depth ---- */
    unlink(path);
    QueueStore *s = queue_store_open(path);
    if (!s) { fprintf(stderr, "open failed\n"); return 1; }
    const char *q = "bench";
    if (queue_declare(s, q, 5, 1, 1000000)) { fprintf(stderr, "declare failed\n"); return 1; }
    uint32_t chunk = 5000u < N ? 5000u : N;
    uint64_t *lat = malloc(sizeof(uint64_t) * chunk);
    uint64_t *tags = malloc(sizeof(uint64_t) * N);
    printf("queue-bench N=%u rsize=%u\n", N, rsize);
    for (uint32_t done = 0; done < N; done += chunk) {
        uint64_t t0 = now_ns();
        for (uint32_t i = 0; i < chunk; i++) {
            uint64_t st = now_ns();
            if (queue_publish(s, q, 5, payload, rsize, 0, NULL)) {
                fprintf(stderr, "publish failed at %u\n", done + i); return 1;
            }
            lat[i] = now_ns() - st;
        }
        uint64_t dt = now_ns() - t0;
        printf("PUBLISH durable depth=%u ops=%u ops/s=%.0f p50_us=%llu p99_us=%llu\n",
               done + chunk, chunk, (double)chunk / secs(dt),
               (unsigned long long)pct(lat, chunk, 0.50) / 1000ull,
               (unsigned long long)pct(lat, chunk, 0.99) / 1000ull);
    }

    /* ---- consume behind an in-flight wall ---- */
    /* Move the first half in-flight with a long visibility deadline, then
     * consume from a queue whose head is a wall of in-flight deliveries. */
    uint32_t wall = N / 2;
    uint64_t t0 = now_ns();
    for (uint32_t i = 0; i < wall; i++) {
        QueueMessage m;
        int rc = queue_consume(s, q, 5, 60000, &m);
        if (rc != 1) { fprintf(stderr, "wall consume stopped early at %u\n", i); return 1; }
        tags[i] = m.delivery_tag;
        queue_message_free(&m);
    }
    uint64_t wall_ns = now_ns() - t0;
    uint32_t probe = 2000u;
    memset(lat, 0, sizeof(uint64_t) * chunk);
    t0 = now_ns();
    for (uint32_t i = 0; i < probe; i++) {
        uint64_t st = now_ns();
        QueueMessage m;
        if (queue_consume(s, q, 5, 60000, &m) != 1) {
            fprintf(stderr, "behind-wall consume failed at %u\n", i); return 1;
        }
        lat[i] = now_ns() - st;
        queue_message_free(&m);
    }
    uint64_t behind_ns = now_ns() - t0;
    printf("CONSUME clean_head ops=%u ops/s=%.0f avg_us=%llu\n",
           wall, (double)wall / secs(wall_ns),
           (unsigned long long)(wall_ns / wall) / 1000ull);
    printf("CONSUME behind_inflight_wall outstanding=%u ops=%u ops/s=%.0f p50_us=%llu p99_us=%llu\n",
           wall, probe, (double)probe / secs(behind_ns),
           (unsigned long long)pct(lat, probe, 0.50) / 1000ull,
           (unsigned long long)pct(lat, probe, 0.99) / 1000ull);

    /* ---- ack with a growing outstanding set ---- */
    /* Outstanding set currently ~wall + probe. ACK the oldest `wall` tags,
     * chunked, so per-ACK cost against outstanding count is visible. */
    uint32_t ackn = wall;
    for (uint32_t base = 0; base < ackn; base += chunk) {
        uint32_t c = ackn - base < chunk ? ackn - base : chunk;
        uint64_t a0 = now_ns();
        for (uint32_t i = 0; i < c; i++)
            if (queue_ack(s, q, 5, tags[base + i]) != 1) {
                fprintf(stderr, "ack failed at %u\n", base + i); return 1;
            }
        uint64_t dt = now_ns() - a0;
        printf("ACK outstanding=%u ops=%u ops/s=%.0f avg_us=%llu\n",
               ackn - base, c, (double)c / secs(dt),
               (unsigned long long)(dt / c) / 1000ull);
    }

    /* ---- nack/requeue with many outstanding ---- */
    uint32_t nk = 2000u;
    uint64_t *ntags = malloc(sizeof(uint64_t) * nk);
    for (uint32_t i = 0; i < nk; i++) {
        QueueMessage m;
        if (queue_consume(s, q, 5, 60000, &m) != 1) {
            fprintf(stderr, "nack setup consume failed\n"); return 1;
        }
        ntags[i] = m.delivery_tag;
        queue_message_free(&m);
    }
    t0 = now_ns();
    for (uint32_t i = 0; i < nk; i++)
        if (queue_nack(s, q, 5, ntags[i], 1) != 1) {
            fprintf(stderr, "nack failed at %u\n", i); return 1;
        }
    uint64_t nk_ns = now_ns() - t0;
    printf("NACK requeue outstanding~%u ops=%u ops/s=%.0f avg_us=%llu\n",
           N - wall - probe, nk, (double)nk / secs(nk_ns),
           (unsigned long long)(nk_ns / nk) / 1000ull);
    free(ntags);

    /* ---- visibility expiry pass ---- */
    const char *vq = "vis";
    if (queue_declare(s, vq, 3, 1, 1000000)) { fprintf(stderr, "vis declare failed\n"); return 1; }
    for (uint32_t i = 0; i < 2000u; i++)
        if (queue_publish(s, vq, 3, payload, rsize, 0, NULL)) {
            fprintf(stderr, "vis publish failed\n"); return 1;
        }
    for (uint32_t i = 0; i < 2000u; i++) {
        QueueMessage m;
        if (queue_consume(s, vq, 3, 40, &m) != 1) {
            fprintf(stderr, "vis consume failed\n"); return 1;
        }
        queue_message_free(&m);
    }
    usleep(90000); /* let the 40 ms visibility window lapse */
    t0 = now_ns();
    queue_reap(s);
    uint64_t reap_ns = now_ns() - t0;
    printf("VISREAP expired=2000 took_ms=%llu\n", (unsigned long long)reap_ns / 1000000ull);

    /* ---- metrics scrape at depth ---- */
    t0 = now_ns();
    for (int i = 0; i < 100; i++) {
        (void)queue_total_depth(s);
        (void)queue_total_inflight(s);
        (void)queue_depth(s, q, 5);
        (void)queue_inflight(s, q, 5);
    }
    uint64_t met_ns = (now_ns() - t0) / 100;
    printf("METRICS scrape took_us=%llu\n", (unsigned long long)met_ns / 1000ull);
    queue_store_close(s);
    unlink(path);
    free(tags); free(lat);

    /* ---- publish+consume+ack steady state ---- */
    uint32_t loop = 20000u < N ? 20000u : N;
    s = queue_store_open(NULL); /* in-memory */
    if (!s || queue_declare(s, q, 5, 0, 1000000)) { fprintf(stderr, "mem setup failed\n"); return 1; }
    t0 = now_ns();
    for (uint32_t i = 0; i < loop; i++) {
        if (queue_publish(s, q, 5, payload, rsize, 0, NULL)) { fprintf(stderr, "mem publish failed\n"); return 1; }
        QueueMessage m;
        if (queue_consume(s, q, 5, 60000, &m) != 1) { fprintf(stderr, "mem consume failed\n"); return 1; }
        if (queue_ack(s, q, 5, m.delivery_tag) != 1) { fprintf(stderr, "mem ack failed\n"); return 1; }
        queue_message_free(&m);
    }
    uint64_t mem_ns = now_ns() - t0;
    printf("PCACK memory ops=%u ops/s=%.0f avg_us=%llu\n",
           loop, (double)loop / secs(mem_ns), (unsigned long long)(mem_ns / loop) / 1000ull);
    queue_store_close(s);

    snprintf(path, sizeof path, "%s/bench_queue_pcack.wal", dir);
    unlink(path);
    s = queue_store_open(path);
    if (!s || queue_declare(s, q, 5, 1, 1000000)) { fprintf(stderr, "dur setup failed\n"); return 1; }
    t0 = now_ns();
    for (uint32_t i = 0; i < loop; i++) {
        if (queue_publish(s, q, 5, payload, rsize, 0, NULL)) { fprintf(stderr, "dur publish failed\n"); return 1; }
        QueueMessage m;
        if (queue_consume(s, q, 5, 60000, &m) != 1) { fprintf(stderr, "dur consume failed\n"); return 1; }
        if (queue_ack(s, q, 5, m.delivery_tag) != 1) { fprintf(stderr, "dur ack failed\n"); return 1; }
        queue_message_free(&m);
    }
    uint64_t dur_ns = now_ns() - t0;
    printf("PCACK durable ops=%u ops/s=%.0f avg_us=%llu\n",
           loop, (double)loop / secs(dur_ns), (unsigned long long)(dur_ns / loop) / 1000ull);
    queue_store_close(s);
    unlink(path);

    /* Concurrent durable publish: 8 threads, own queues, 5k each. */
    snprintf(path, sizeof path, "%s/bench_queue_conc.wal", dir);
    unlink(path);
    s = queue_store_open(path);
    if (!s) { fprintf(stderr, "conc open failed\n"); return 1; }
    conc_store = s;
    conc_lat = malloc(sizeof(uint64_t) * (size_t)CONC_T * CONC_N);
    pthread_t th[CONC_T];
    t0 = now_ns();
    for (long i = 0; i < CONC_T; i++)
        if (pthread_create(&th[i], NULL, conc_publish_worker, (void *)i)) return 1;
    for (int i = 0; i < CONC_T; i++) {
        void *ret = NULL;
        if (pthread_join(th[i], &ret) || ret) { fprintf(stderr, "conc publish worker failed\n"); return 1; }
    }
    uint64_t pubdt = now_ns() - t0;
    uint64_t total = (uint64_t)CONC_T * CONC_N;
    printf("CONCPUB threads=%d ops=%llu ops/s=%.0f p50_us=%llu p95_us=%llu p99_us=%llu\n",
           CONC_T, (unsigned long long)total, (double)total / secs(pubdt),
           (unsigned long long)pct(conc_lat, total, 0.50) / 1000ull,
           (unsigned long long)pct(conc_lat, total, 0.95) / 1000ull,
           (unsigned long long)pct(conc_lat, total, 0.99) / 1000ull);
    free(conc_lat);
    queue_store_close(s);
    unlink(path);

    /* Concurrent publish/consume/ack pipeline on one shared durable queue. */
    snprintf(path, sizeof path, "%s/bench_queue_pc.wal", dir);
    unlink(path);
    s = queue_store_open(path);
    if (!s || queue_declare(s, "shared", 6, 1, 1000000)) { fprintf(stderr, "pc open failed\n"); return 1; }
    conc_store = s;
    pc_consumed = 0;
    t0 = now_ns();
    for (long i = 0; i < 4; i++) {
        if (pthread_create(&th[i], NULL, pc_producer, NULL)) return 1;
        if (pthread_create(&th[4 + i], NULL, pc_consumer, NULL)) return 1;
    }
    for (int i = 0; i < CONC_T; i++) {
        void *ret = NULL;
        if (pthread_join(th[i], &ret) || ret) { fprintf(stderr, "pc worker %d failed\n", i); return 1; }
    }
    uint64_t pcdt = now_ns() - t0;
    total = (uint64_t)CONC_T * CONC_N;
    printf("CONCPC threads=%d ops=%llu ops/s=%.0f avg_us=%llu\n",
           CONC_T, (unsigned long long)total, (double)total / secs(pcdt),
           (unsigned long long)(pcdt / total) / 1000ull);
    queue_store_close(s);
    unlink(path);

    /* Batch publish vs single publish: same message count, durable queues.
     * The batch amortizes one lock hold and one group fsync over 256
     * messages; singles pay one durability wait per message. */
    snprintf(path, sizeof path, "%s/bench_queue_batch.wal", dir);
    unlink(path);
    s = queue_store_open(path);
    if (!s || queue_declare(s, "b1", 2, 1, 1000000) ||
        queue_declare(s, "b2", 2, 1, 1000000)) { fprintf(stderr, "batch setup failed\n"); return 1; }
    uint32_t bn = 8192, slice = 256;
    const void *bdata[256];
    uint32_t blen[256];
    uint64_t bids[256];
    for (uint32_t i = 0; i < slice; i++) { bdata[i] = payload; blen[i] = rsize; }
    t0 = now_ns();
    for (uint32_t i = 0; i < bn; i++)
        if (queue_publish(s, "b1", 2, payload, rsize, 0, NULL)) {
            fprintf(stderr, "single publish failed\n"); return 1;
        }
    uint64_t sdt = now_ns() - t0;
    t0 = now_ns();
    for (uint32_t i = 0; i < bn / slice; i++)
        if (queue_publish_batch(s, "b2", 2, slice, bdata, blen, bids)) {
            fprintf(stderr, "batch publish failed\n"); return 1;
        }
    uint64_t bdt = now_ns() - t0;
    printf("BATCH singles=%u ops/s=%.0f avg_us=%llu\n",
           bn, (double)bn / secs(sdt), (unsigned long long)(sdt / bn) / 1000ull);
    printf("BATCH batch256 ops=%u ops/s=%.0f avg_us_per_batch=%llu speedup=%.2fx\n",
           bn, (double)bn / secs(bdt), (unsigned long long)(bdt / (bn / slice)) / 1000ull,
           (double)sdt / (double)bdt);
    queue_store_close(s);
    unlink(path);

    /* Disconnect cleanup: one owner holds one in-flight delivery per queue
     * across 8 queues holding 20k retained messages; a disconnect must not
     * scan every message. */
    snprintf(path, sizeof path, "%s/bench_queue_disc.wal", dir);
    unlink(path);
    s = queue_store_open(path);
    if (!s) { fprintf(stderr, "disc open failed\n"); return 1; }
    for (int qn = 0; qn < 8; qn++) {
        char qname[16];
        snprintf(qname, sizeof qname, "d%d", qn);
        if (queue_declare(s, qname, (uint32_t)strlen(qname), 1, 1000000)) {
            fprintf(stderr, "disc declare failed\n"); return 1;
        }
        for (uint32_t i = 0; i < bn / 8; i++)
            if (queue_publish(s, qname, (uint32_t)strlen(qname), payload, rsize, 0, NULL)) {
                fprintf(stderr, "disc publish failed\n"); return 1;
            }
        QueueMessage dm;
        if (queue_consume_for_owner(s, qname, (uint32_t)strlen(qname), 60000, 77, &dm) != 1) {
            fprintf(stderr, "disc consume failed\n"); return 1;
        }
        queue_message_free(&dm);
    }
    t0 = now_ns();
    queue_requeue_owner(s, 77);
    uint64_t ddt = now_ns() - t0;
    printf("DISCONNECT queues=8 depth=%u inflight=8 took_us=%llu\n",
           bn, (unsigned long long)ddt / 1000ull);
    queue_store_close(s);
    unlink(path);

    /* Recovery: a WAL full of publish+deliver+ACK triples; replay resolves
     * every delivery/ACK by message ID against the live list. */
    snprintf(path, sizeof path, "%s/bench_queue_rec.wal", dir);
    unlink(path);
    s = queue_store_open(path);
    if (!s || queue_declare(s, "rec", 3, 1, 1000000)) { fprintf(stderr, "rec setup failed\n"); return 1; }
    uint32_t rn = 5000;
    for (uint32_t i = 0; i < rn; i++)
        if (queue_publish(s, "rec", 3, payload, rsize, 0, NULL)) {
            fprintf(stderr, "rec publish failed\n"); return 1;
        }
    for (uint32_t i = 0; i < rn; i++) {
        QueueMessage rm;
        if (queue_consume(s, "rec", 3, 60000, &rm) != 1) { fprintf(stderr, "rec consume failed\n"); return 1; }
        if (queue_ack(s, "rec", 3, rm.delivery_tag) != 1) { fprintf(stderr, "rec ack failed\n"); return 1; }
        queue_message_free(&rm);
    }
    queue_store_close(s);
    t0 = now_ns();
    s = queue_store_open(path);
    if (!s) { fprintf(stderr, "rec reopen failed\n"); return 1; }
    uint64_t rdt = now_ns() - t0;
    printf("RECOVERY records=%u (publish+deliver+ack each) took_ms=%llu\n",
           rn * 3, (unsigned long long)rdt / 1000000ull);
    queue_store_close(s);
    unlink(path);
    return 0;
}
