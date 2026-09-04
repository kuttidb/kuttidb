/* Multi-threaded C benchmark client: the true wire ceiling. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

#define DEFAULT_BATCH 256
#define DEFAULT_VLEN 100

static void put_u32le(unsigned char *p, uint32_t v) {
    p[0] = v & 0xff; p[1] = (v >> 8) & 0xff;
    p[2] = (v >> 16) & 0xff; p[3] = (v >> 24) & 0xff;
}

static int read_full(int fd, void *buf, size_t n) {
    char *p = buf;
    while (n > 0) {
        ssize_t r = read(fd, p, n);
        if (r <= 0) { if (r < 0 && errno == EINTR) continue; return -1; }
        p += r;
        n -= (size_t)r;
    }
    return 0;
}

static int write_full(int fd, const void *buf, size_t n) {
    const char *p = buf;
    while (n > 0) {
        ssize_t r = write(fd, p, n);
        if (r <= 0) { if (r < 0 && errno == EINTR) continue; return -1; }
        p += r;
        n -= (size_t)r;
    }
    return 0;
}

typedef struct {
    int id;
    int port;
    long nops;       /* put+get per thread */
    long done;
    int batch;
    int vlen;
    uint64_t *latencies;
    size_t latency_count;
    size_t latency_capacity;
    /* Single-op mode: isolated PUT/GET/DELETE latency series. */
    int single;
    uint64_t *lat_put, *lat_get, *lat_del;
    size_t cnt_put, cnt_get, cnt_del;
} Job;

static uint64_t elapsed_ns(const struct timespec *start,
                           const struct timespec *end) {
    return (uint64_t)(end->tv_sec - start->tv_sec) * UINT64_C(1000000000) +
           (uint64_t)(end->tv_nsec - start->tv_nsec);
}

static void record_latency(Job *j, const struct timespec *start) {
    struct timespec end;
    clock_gettime(CLOCK_MONOTONIC, &end);
    if (j->latency_count < j->latency_capacity)
        j->latencies[j->latency_count++] = elapsed_ns(start, &end);
}

static int compare_u64(const void *a, const void *b) {
    uint64_t av = *(const uint64_t *)a;
    uint64_t bv = *(const uint64_t *)b;
    return (av > bv) - (av < bv);
}

/* One connection per thread; each operation is one round trip measured
 * individually so PUT, GET, and DELETE get isolated latency series. */
static void *runner_single(void *arg) {
    Job *j = arg;
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return NULL; }
    struct sockaddr_in a = {0};
    a.sin_family = AF_INET;
    a.sin_port = htons(j->port);
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(fd, (struct sockaddr *)&a, sizeof a) < 0) {
        perror("connect");
        close(fd);
        return NULL;
    }
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);

    size_t vcap = (size_t)j->vlen ? (size_t)j->vlen : 1;
    unsigned char *val = malloc(vcap);
    unsigned char *req = malloc(7 + 32 + (size_t)j->vlen);
    char key[32];
    unsigned char resp[5];
    if (!val || !req) { close(fd); free(val); free(req); return NULL; }
    memset(val, 'v', vcap);

    j->lat_put = malloc((size_t)j->nops * sizeof(uint64_t));
    j->lat_get = malloc((size_t)j->nops * sizeof(uint64_t));
    j->lat_del = malloc((size_t)j->nops * sizeof(uint64_t));
    if (!j->lat_put || !j->lat_get || !j->lat_del) goto cleanup_single;

    for (int phase = 0; phase < 3; phase++) {
        for (long i = 0; i < j->nops; i++) {
            int n = snprintf(key, sizeof key, "s%d-%ld", j->id, i);
            size_t klen = (size_t)n;
            size_t reqlen;
            if (phase == 0) {        /* PUT (0x01) */
                req[0] = 0x01;
                req[1] = n & 0xff; req[2] = (n >> 8) & 0xff;
                put_u32le(req + 3, (uint32_t)j->vlen);
                memcpy(req + 7, key, klen);
                memcpy(req + 7 + klen, val, (size_t)j->vlen);
                reqlen = 7 + klen + (size_t)j->vlen;
            } else {                 /* GET (0x02) / DELETE (0x03) */
                req[0] = phase == 1 ? 0x02 : 0x03;
                req[1] = n & 0xff; req[2] = (n >> 8) & 0xff;
                req[3] = req[4] = req[5] = req[6] = 0;
                memcpy(req + 7, key, klen);
                reqlen = 7 + klen;
            }
            struct timespec t;
            clock_gettime(CLOCK_MONOTONIC, &t);
            if (write_full(fd, req, reqlen) < 0) { perror("write"); goto cleanup_single; }
            if (read_full(fd, resp, 5) < 0) { perror("read"); goto cleanup_single; }
            if (resp[0] != 0x00) {
                fprintf(stderr, "single op failed (op=0x%02x status=0x%02x)\n",
                        req[0], resp[0]);
                goto cleanup_single;
            }
            uint32_t rlen = (uint32_t)resp[1] | ((uint32_t)resp[2] << 8) |
                            ((uint32_t)resp[3] << 16) | ((uint32_t)resp[4] << 24);
            if (rlen) {
                if (rlen > (uint32_t)j->vlen ||
                    read_full(fd, val, rlen) < 0) {
                    fprintf(stderr, "bad get response\n");
                    goto cleanup_single;
                }
            }
            struct timespec te;
            clock_gettime(CLOCK_MONOTONIC, &te);
            uint64_t lat = elapsed_ns(&t, &te);
            if (phase == 0) j->lat_put[j->cnt_put++] = lat;
            else if (phase == 1) j->lat_get[j->cnt_get++] = lat;
            else j->lat_del[j->cnt_del++] = lat;
        }
    }
    j->done = 3 * j->nops;
cleanup_single:
    close(fd);
    free(val);
    free(req);
    return NULL;
}

static double series_dt = 1.0;

static void report_series(const char *name, uint64_t **arrays, size_t *counts,
                          int nthreads) {
    size_t total = 0;
    for (int i = 0; i < nthreads; i++) total += counts[i];
    if (!total) return;
    uint64_t *all = malloc(total * sizeof(uint64_t));
    if (!all) return;
    size_t off = 0;
    for (int i = 0; i < nthreads; i++) {
        memcpy(all + off, arrays[i], counts[i] * sizeof(uint64_t));
        off += counts[i];
    }
    qsort(all, total, sizeof(uint64_t), compare_u64);
    printf("%s latency: p50=%.1f us p95=%.1f us p99=%.1f us (%zu samples, %.0f ops/s)\n",
           name, all[(total - 1) / 2] / 1000.0,
           all[(total * 95 + 99) / 100 - 1] / 1000.0,
           all[(total * 99 + 99) / 100 - 1] / 1000.0,
           total, total / series_dt);
    free(all);
}

static void *runner(void *arg) {
    Job *j = arg;
    if (j->single) return runner_single(j);
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return NULL; }
    struct sockaddr_in a = {0};
    a.sin_family = AF_INET;
    a.sin_port = htons(j->port);
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(fd, (struct sockaddr *)&a, sizeof a) < 0) {
        perror("connect");
        close(fd);
        return NULL;
    }
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);

    size_t req_cap = 7 + (size_t)j->batch * (6 + 32 + (size_t)j->vlen);
    unsigned char *req = malloc(req_cap);
    unsigned char *val = malloc((size_t)j->vlen ? (size_t)j->vlen : 1);
    if (!req || !val) { close(fd); free(req); free(val); return NULL; }
    memset(val, 'v', (size_t)j->vlen);
    char key[32];
    long ops = 0;

    j->latency_capacity = (size_t)(((j->nops + j->batch - 1) / j->batch) * 2);
    j->latencies = malloc(j->latency_capacity * sizeof(*j->latencies));
    if (!j->latencies) { close(fd); free(req); free(val); return NULL; }

    while (ops < j->nops) {
        long cnt = j->nops - ops > j->batch ? j->batch : (j->nops - ops);
        /* PUT_BATCH */
        unsigned char *p = req;
        *p++ = 0x11; *p++ = 0; *p++ = 0;
        put_u32le(p, (uint32_t)cnt); p += 4;
        for (long i = 0; i < cnt; i++) {
            int n = snprintf(key, sizeof key, "k%d-%ld", j->id, ops + i);
            *p++ = n & 0xff; *p++ = (n >> 8) & 0xff;
            put_u32le(p, (uint32_t)j->vlen); p += 4;
            memcpy(p, key, (size_t)n); p += n;
            memcpy(p, val, (size_t)j->vlen); p += j->vlen;
        }
        struct timespec batch_start;
        clock_gettime(CLOCK_MONOTONIC, &batch_start);
        if (write_full(fd, req, (size_t)(p - req)) < 0) {
            perror("write");
            goto cleanup;
        }
        unsigned char st;
        if (read_full(fd, &st, 1) < 0 || st != 0x00) {
            fprintf(stderr, "put batch failed\n");
            goto cleanup;
        }
        record_latency(j, &batch_start);
        ops += cnt;
    }
    /* GET_BATCH verify */
    ops = 0;
    while (ops < j->nops) {
        long cnt = j->nops - ops > j->batch ? j->batch : (j->nops - ops);
        unsigned char *p = req;
        *p++ = 0x12; *p++ = 0; *p++ = 0;
        put_u32le(p, (uint32_t)cnt); p += 4;
        for (long i = 0; i < cnt; i++) {
            int n = snprintf(key, sizeof key, "k%d-%ld", j->id, ops + i);
            *p++ = n & 0xff; *p++ = (n >> 8) & 0xff;
            memcpy(p, key, (size_t)n); p += n;
        }
        struct timespec batch_start;
        clock_gettime(CLOCK_MONOTONIC, &batch_start);
        if (write_full(fd, req, (size_t)(p - req)) < 0) goto cleanup;
        unsigned char rcount[4];
        if (read_full(fd, rcount, 4) < 0) goto cleanup;
        for (long i = 0; i < cnt; i++) {
            unsigned char sh[5];
            if (read_full(fd, sh, 5) < 0) goto cleanup;
            uint32_t vlen = (uint32_t)sh[1] | ((uint32_t)sh[2] << 8) |
                            ((uint32_t)sh[3] << 16) | ((uint32_t)sh[4] << 24);
            if (sh[0] != 0x00 || vlen != (uint32_t)j->vlen) {
                fprintf(stderr, "bad get\n");
                goto cleanup;
            }
            if (read_full(fd, val, (size_t)j->vlen) < 0) goto cleanup;
        }
        record_latency(j, &batch_start);
        ops += cnt;
    }
    j->done = 2 * j->nops;
cleanup:
    close(fd);
    free(req);
    free(val);
    return NULL;
}

int main(int argc, char **argv) {
    int port = argc > 1 ? atoi(argv[1]) : 7379;
    int nthreads = argc > 2 ? atoi(argv[2]) : 8;
    long per = argc > 3 ? atol(argv[3]) : 100000;
    int batch = argc > 4 ? atoi(argv[4]) : DEFAULT_BATCH;
    int vlen = argc > 5 ? atoi(argv[5]) : DEFAULT_VLEN;
    int single = argc > 6 && strcmp(argv[6], "single") == 0;
    if (nthreads < 1 || per < 1 || batch < 1 || batch > 65536 ||
        vlen < 0 || vlen > (64 << 20) ||
        (size_t)batch * (38 + (size_t)vlen) > (64u << 20)) {
        fprintf(stderr, "usage: %s [port [threads [ops-per-thread [batch [value-bytes [single]]]]]]\n", argv[0]);
        return 2;
    }

    Job *jobs = calloc((size_t)nthreads, sizeof(Job));
    pthread_t *th = calloc((size_t)nthreads, sizeof(pthread_t));

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (int i = 0; i < nthreads; i++) {
        jobs[i] = (Job){.id = i, .port = port, .nops = per,
                        .batch = batch, .vlen = vlen, .single = single};
        pthread_create(&th[i], NULL, runner, &jobs[i]);
    }
    long total = 0;
    size_t latency_count = 0;
    for (int i = 0; i < nthreads; i++) {
        pthread_join(th[i], NULL);
        total += jobs[i].done;
        latency_count += jobs[i].latency_count;
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double dt = (double)(t1.tv_sec - t0.tv_sec) + (double)(t1.tv_nsec - t0.tv_nsec) / 1e9;
    printf("%ld ops, clients=%d batch=%d value=%dB, %.2fs = %.0f ops/s\n",
           total, nthreads, batch, vlen, dt, total / dt);
    if (single) {
        series_dt = dt;
        uint64_t *lp[nthreads], *lg[nthreads], *ld[nthreads];
        size_t cp[nthreads], cg[nthreads], cd[nthreads];
        for (int i = 0; i < nthreads; i++) {
            lp[i] = jobs[i].lat_put; cp[i] = jobs[i].cnt_put;
            lg[i] = jobs[i].lat_get; cg[i] = jobs[i].cnt_get;
            ld[i] = jobs[i].lat_del; cd[i] = jobs[i].cnt_del;
        }
        report_series("single PUT", lp, cp, nthreads);
        report_series("single GET", lg, cg, nthreads);
        report_series("single DELETE", ld, cd, nthreads);
    } else if (latency_count > 0) {
        uint64_t *latencies = malloc(latency_count * sizeof(*latencies));
        if (!latencies) {
            fprintf(stderr, "could not aggregate latency samples\n");
        } else {
            size_t offset = 0;
            for (int i = 0; i < nthreads; i++) {
                memcpy(latencies + offset, jobs[i].latencies,
                       jobs[i].latency_count * sizeof(*latencies));
                offset += jobs[i].latency_count;
            }
            qsort(latencies, latency_count, sizeof(*latencies), compare_u64);
            size_t p50 = (latency_count - 1) / 2;
            size_t p95 = ((latency_count * 95 + 99) / 100) - 1;
            size_t p99 = ((latency_count * 99 + 99) / 100) - 1;
            printf("batch latency: p50=%.1f us p95=%.1f us p99=%.1f us (%zu samples)\n",
                   latencies[p50] / 1000.0, latencies[p95] / 1000.0,
                   latencies[p99] / 1000.0,
                   latency_count);
            free(latencies);
        }
    }
    for (int i = 0; i < nthreads; i++) {
        free(jobs[i].latencies);
        free(jobs[i].lat_put);
        free(jobs[i].lat_get);
        free(jobs[i].lat_del);
    }
    free(jobs);
    free(th);
    return 0;
}
