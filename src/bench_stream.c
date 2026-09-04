/* Stream engine retained-history benchmark.
 *
 * Drives the StreamStore API directly (no server, no protocol) so results
 * isolate engine cost. It records the evidence docs/operations/BENCHMARKS.md
 * needs for the stream engine:
 *
 *   APPEND   - durable append ops/s and p50/p95/p99 as retained history grows
 *              (a linear per-op history cost shows up as falling ops/s)
 *   FETCH    - head fetch (oldest offset) and tail fetch at full history
 *   METRICS  - full-store labeled stats scrape (metrics endpoint path)
 *   COMMIT   - consumer-group offset commits at full history
 *   TRIMBURST- one batch append that pushes far past max_bytes, forcing the
 *              retention path to evict thousands of records in one call
 *   RECOVERY - store reopen (full WAL replay) after the run
 *
 * Usage: bench-stream [dir] [records] [record_bytes] [partitions]
 */
#define _POSIX_C_SOURCE 200809L
#include "../src/stream.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>

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

static char *mkpath(const char *dir, const char *name) {
    /* Two rotating buffers: callers keep two paths alive at once. */
    static char bufs[2][512];
    static unsigned rot;
    char *buf = bufs[rot++ & 1];
    snprintf(buf, 512, "%s/%s", dir, name);
    return buf;
}

static void stats_cb(const char *name, uint32_t name_len, uint32_t partitions,
                     uint64_t bytes, uint64_t records, void *ud) {
    (void)name; (void)name_len; (void)partitions;
    uint64_t *tot = ud;
    tot[0] += bytes;
    tot[1] += records;
}

/* Concurrent durable-append phase: T threads share one store, as the server's
 * event loops do. Measures aggregate throughput and pooled per-op latency;
 * the group-fsync coordinator should lift throughput toward write+mutate
 * cost while single-op latency stays at one fsync. */
#define CONC_T 8
#define CONC_N 10000
static StreamStore *conc_store;
static uint64_t *conc_lat;
static long conc_parts;
static void *conc_worker(void *arg) {
    long tid = (long)arg;
    char payload[100];
    memset(payload, 'x', sizeof payload);
    for (int i = 0; i < CONC_N; i++) {
        uint64_t st = now_ns();
        if (stream_append(conc_store, "conc", 4, (uint32_t)(tid % conc_parts),
                          NULL, 0, payload, sizeof payload, NULL, NULL)) {
            return (void *)1;
        }
        conc_lat[tid * CONC_N + i] = now_ns() - st;
    }
    return NULL;
}

int main(int argc, char **argv) {
    const char *dir = argc > 1 ? argv[1] : "/tmp";
    uint32_t records = argc > 2 ? (uint32_t)strtoul(argv[2], NULL, 10) : 100000u;
    uint32_t rsize = argc > 3 ? (uint32_t)strtoul(argv[3], NULL, 10) : 100u;
    uint32_t parts = argc > 4 ? (uint32_t)strtoul(argv[4], NULL, 10) : 8u;
    if (rsize < 1) rsize = 1;

    char *path = mkpath(dir, "bench_stream.wal");
    unlink(path);
    StreamStore *s = stream_store_open(path);
    if (!s) { fprintf(stderr, "open failed\n"); return 1; }

    const char *topic = "bench";
    if (stream_declare(s, topic, 5, parts, 0 /* unlimited */, 0)) {
        fprintf(stderr, "declare failed\n"); return 1;
    }

    char *payload = malloc(rsize);
    memset(payload, 'x', rsize);
    uint32_t chunk = 10000u < records ? 10000u : records;
    uint64_t *lat = malloc(sizeof(uint64_t) * chunk);
    uint64_t last_off = 0; /* highest offset appended to partition 0 */
    pthread_t th[CONC_T];

    printf("stream-bench records=%u rsize=%u partitions=%u\n", records, rsize, parts);
    double total_secs = 0.0;
    for (uint32_t done = 0; done < records; done += chunk) {
        uint64_t t0 = now_ns();
        for (uint32_t i = 0; i < chunk; i++) {
            uint64_t st = now_ns();
            uint64_t off = 0;
            if (stream_append(s, topic, 5, UINT32_MAX, NULL, 0, payload, rsize, NULL, &off)) {
                fprintf(stderr, "append failed at %u\n", done + i); return 1;
            }
            last_off = off;
            lat[i] = now_ns() - st;
        }
        uint64_t dt = now_ns() - t0;
        total_secs += (double)dt / 1e9;
        printf("APPEND history=%u ops=%u ops/s=%.0f p50_us=%llu p95_us=%llu p99_us=%llu\n",
               done + chunk, chunk, (double)chunk / ((double)dt / 1e9),
               (unsigned long long)pct(lat, chunk, 0.50) / 1000ull,
               (unsigned long long)pct(lat, chunk, 0.95) / 1000ull,
               (unsigned long long)pct(lat, chunk, 0.99) / 1000ull);
    }
    free(lat);

    /* Fetch 128 records from the oldest offset: the cheap head case. */
    StreamRecordView *views = NULL; uint32_t nv = 0;
    uint64_t t0 = now_ns();
    int rc = stream_fetch(s, topic, 5, 0, 0, 128, 1ull << 20, &views, &nv);
    uint64_t head_ns = now_ns() - t0;
    if (rc != 1) { fprintf(stderr, "head fetch failed rc=%d\n", rc); return 1; }
    printf("FETCH head records=%u took_us=%llu\n", nv, (unsigned long long)head_ns / 1000ull);
    stream_fetch_free(views, nv);

    /* Fetch near the write head: the cheap case. */
    t0 = now_ns();
    rc = stream_fetch(s, topic, 5, 0, last_off, 128, 1ull << 20, &views, &nv);
    uint64_t tail_ns = now_ns() - t0;
    if (rc != 1) { fprintf(stderr, "tail fetch failed rc=%d\n", rc); return 1; }
    printf("FETCH tail records=%u took_us=%llu\n", nv, (unsigned long long)tail_ns / 1000ull);
    stream_fetch_free(views, nv);

    /* Lagging consumer: fetch 128 records starting half the history back.
     * The scan must walk every retained record before the requested offset
     * in partition 0, so its cost shows the no-index head walk. */
    uint64_t lag_off = last_off / 2;
    t0 = now_ns();
    rc = stream_fetch(s, topic, 5, 0, lag_off, 128, 1ull << 20, &views, &nv);
    uint64_t lag_ns = now_ns() - t0;
    if (rc != 1) { fprintf(stderr, "lag fetch failed rc=%d\n", rc); return 1; }
    printf("FETCH lag half_history records=%u took_us=%llu\n", nv,
           (unsigned long long)lag_ns / 1000ull);
    stream_fetch_free(views, nv);

    /* Metrics scrape: per-topic record counting under the store lock. */
    uint64_t tot[2] = {0, 0};
    t0 = now_ns();
    for (int i = 0; i < 10; i++) { tot[0] = 0; tot[1] = 0; stream_foreach_stats(s, stats_cb, tot); }
    uint64_t stats_ns = (now_ns() - t0) / 10;
    printf("METRICS stats_scrape took_us=%llu live_bytes=%llu live_records=%llu\n",
           (unsigned long long)stats_ns / 1000ull,
           (unsigned long long)tot[0], (unsigned long long)tot[1]);

    /* Offset commits: group lookup + one fsynced WAL record each. */
    uint32_t commits = 2000u;
    t0 = now_ns();
    for (uint32_t i = 0; i < commits; i++)
        if (stream_commit(s, topic, 5, "g", 1, i % parts, i)) {
            fprintf(stderr, "commit failed\n"); return 1;
        }
    uint64_t cdt = now_ns() - t0;
    printf("COMMIT ops=%u ops/s=%.0f\n", commits, (double)commits / ((double)cdt / 1e9));

    /* Retention burst: unlimited-history topic is closed; a fresh store gets
     * a max_bytes topic and one batch append far past the ceiling, so enforce
     * must evict thousands of records inside one call. */
    stream_store_close(s);
    char *tpath = mkpath(dir, "bench_stream_trim.wal");
    unlink(tpath);
    s = stream_store_open(tpath);
    if (!s) { fprintf(stderr, "trim store open failed\n"); return 1; }
    const char *ttopic = "trim";
    if (stream_declare(s, ttopic, 4, parts, 10000ull, 0)) {
        fprintf(stderr, "trim declare failed\n"); return 1;
    }
    StreamAppendInput *in = malloc(sizeof(*in) * 1000);
    StreamAppendResult *res = malloc(sizeof(*res) * 1000);
    for (int i = 0; i < 1000; i++) { in[i].key = NULL; in[i].key_len = 0; in[i].data = payload; in[i].len = rsize; }
    t0 = now_ns();
    if (stream_append_batch(s, ttopic, 4, UINT32_MAX, in, 1000, res)) {
        fprintf(stderr, "trim burst failed\n"); return 1;
    }
    uint64_t burst_ns = now_ns() - t0;
    printf("TRIMBURST batch=1000 max_bytes=10000 took_ms=%llu\n",
           (unsigned long long)burst_ns / 1000000ull);
    free(in); free(res);
    stream_store_close(s);
    unlink(tpath);

    /* Concurrent appends: fresh durable store, 8 threads sharing the store
     * (as the server's event loops do), split across 8 partitions. */
    char *cpath = mkpath(dir, "bench_stream_conc.wal");
    unlink(cpath);
    s = stream_store_open(cpath);
    if (!s || stream_declare(s, "conc", 4, parts, 0, 0)) {
        fprintf(stderr, "conc setup failed\n"); return 1;
    }
    conc_store = s;
    conc_parts = 8 < (long)parts ? 8 : (long)parts;
    conc_lat = malloc(sizeof(uint64_t) * (size_t)CONC_T * CONC_N);
    t0 = now_ns();
    for (long i = 0; i < CONC_T; i++)
        if (pthread_create(&th[i], NULL, conc_worker, (void *)i)) return 1;
    for (int i = 0; i < CONC_T; i++) {
        void *ret = NULL;
        if (pthread_join(th[i], &ret) || ret) { fprintf(stderr, "conc worker failed\n"); return 1; }
    }
    uint64_t cdt2 = now_ns() - t0;
    uint64_t total = (uint64_t)CONC_T * CONC_N;
    printf("CONC threads=%d ops=%llu ops/s=%.0f p50_us=%llu p95_us=%llu p99_us=%llu\n",
           CONC_T, (unsigned long long)total, (double)total / ((double)cdt2 / 1e9),
           (unsigned long long)pct(conc_lat, total, 0.50) / 1000ull,
           (unsigned long long)pct(conc_lat, total, 0.95) / 1000ull,
           (unsigned long long)pct(conc_lat, total, 0.99) / 1000ull);
    free(conc_lat);
    stream_store_close(s);
    unlink(cpath);

    /* Recovery: replay the full WAL from scratch. */
    t0 = now_ns();
    s = stream_store_open(path);
    if (!s) { fprintf(stderr, "reopen failed\n"); return 1; }
    uint64_t rec_ns = now_ns() - t0;
    uint64_t live[2] = {0, 0};
    stream_foreach_stats(s, stats_cb, live);
    printf("RECOVERY reopen took_ms=%llu live_bytes=%llu live_records=%llu append_ops_s=%.0f\n",
           (unsigned long long)rec_ns / 1000000ull,
           (unsigned long long)live[0], (unsigned long long)live[1],
           (double)records / total_secs);
    stream_store_close(s);
    unlink(path);
    free(payload);
    return 0;
}
