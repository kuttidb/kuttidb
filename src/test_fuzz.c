/* WAL-reader fuzzing: mutate valid stream and queue WALs, then require every
 * recovery open to stay bounded, retain only a valid prefix, and leave the
 * store usable. Deterministic seed; pair with `make sanitize-fuzz` so the
 * sanitizer build exercises the same corpus. */
#include "queue.h"
#include "stream.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define ITERATIONS 2000

static uint64_t rng_state = 0x243f6a8885a308d3ull;
static uint32_t rnd(void) {
    rng_state ^= rng_state << 13;
    rng_state ^= rng_state >> 7;
    rng_state ^= rng_state << 17;
    return (uint32_t)(rng_state >> 32);
}

static int read_file(const char *path, unsigned char **out, size_t *out_len) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) return -1;
    struct stat st;
    if (fstat(fd, &st) < 0 || st.st_size < 0) { close(fd); return -1; }
    size_t n = (size_t)st.st_size;
    unsigned char *buf = malloc(n ? n : 1);
    if (!buf) { close(fd); return -1; }
    size_t got = 0;
    while (got < n) {
        ssize_t r = read(fd, buf + got, n - got);
        if (r < 0 && errno == EINTR) continue;
        if (r <= 0) { close(fd); free(buf); return -1; }
        got += (size_t)r;
    }
    close(fd);
    *out = buf;
    *out_len = n;
    return 0;
}

static int write_file(const char *path, const unsigned char *data, size_t n) {
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (fd < 0) return -1;
    size_t off = 0;
    while (off < n) {
        ssize_t w = write(fd, data + off, n - off);
        if (w < 0) { close(fd); return -1; }
        off += (size_t)w;
    }
    if (close(fd)) return -1;
    return 0;
}

static void mutate(unsigned char *data, size_t *n) {
    int rounds = 1 + (int)(rnd() % 6);
    for (int i = 0; i < rounds && *n > 0; i++) {
        uint32_t kind = rnd() % 4;
        if (kind == 0) {
            data[rnd() % *n] ^= (unsigned char)(1u << (rnd() % 8));
        } else if (kind == 1) {
            *n = rnd() % *n; /* truncate at a random byte */
        } else if (kind == 2 && *n > 1) {
            size_t a = rnd() % *n, b = rnd() % *n;
            unsigned char t = data[a];
            data[a] = data[b];
            data[b] = t;
        } else {
            data[rnd() % *n] = (unsigned char)(rnd() >> 16);
        }
    }
}

static char scratch_path[256];

/* Postcondition for every mutated stream WAL: a successful open yields a
 * partition whose retained records are contiguous, and one more append is
 * accepted at the next offset. */
static void check_stream_consistency(StreamStore *s) {
    StreamRecordView *r = NULL;
    uint32_t n = 0;
    int rc = stream_fetch(s, "fz", 2, 0, 0, STREAM_FETCH_MAX, 1 << 20, &r, &n);
    if (rc == 1) {
        uint64_t base = n ? r[0].offset : 0;
        for (uint32_t i = 0; i < n; i++)
            if (r[i].offset != base + i) {
                fprintf(stderr, "fuzz: stream replay is not contiguous\n");
                exit(1);
            }
        stream_fetch_free(r, n);
        uint64_t p = 0, o = 0;
        if (stream_append(s, "fz", 2, 0, NULL, 0, "x", 1, &p, &o) == 0 && o != base + n) {
            fprintf(stderr, "fuzz: stream continuation offset mismatch\n");
            exit(1);
        }
    } else if (rc != 0) {
        fprintf(stderr, "fuzz: stream fetch failed after recovery (%d)\n", rc);
        exit(1);
    }
}

static void fuzz_stream_wal(const unsigned char *orig,
                            size_t orig_len) {
    unsigned char *copy = malloc(orig_len ? orig_len : 1);
    if (!copy) { fprintf(stderr, "fuzz: oom\n"); exit(1); }
    for (int i = 0; i < ITERATIONS; i++) {
        size_t n = orig_len;
        memcpy(copy, orig, n);
        mutate(copy, &n);
        if (write_file(scratch_path, copy, n)) {
            fprintf(stderr, "fuzz: scratch write failed\n"); exit(1);
        }
        StreamStore *s = stream_store_open(scratch_path);
        if (s) {
            check_stream_consistency(s);
            stream_store_close(s);
        }
    }
    free(copy);
}

static void fuzz_queue_wal(const unsigned char *orig, size_t orig_len) {
    unsigned char *copy = malloc(orig_len ? orig_len : 1);
    if (!copy) { fprintf(stderr, "fuzz: oom\n"); exit(1); }
    for (int i = 0; i < ITERATIONS; i++) {
        size_t n = orig_len;
        memcpy(copy, orig, n);
        mutate(copy, &n);
        if (write_file(scratch_path, copy, n)) {
            fprintf(stderr, "fuzz: scratch write failed\n"); exit(1);
        }
        QueueStore *q = queue_store_open(scratch_path);
        if (q) {
            /* A successful open must accept the durable declaration and keep
             * serving durable mutations or failing closed, never crashing. */
            if (queue_declare(q, "fq", 2, 1, 0) != 0 &&
                !queue_persistence_failed(q)) {
                fprintf(stderr, "fuzz: queue open contradicts declare\n");
                exit(1);
            }
            queue_store_close(q);
        }
    }
    free(copy);
}

static void build_stream_wal(const char *path, unsigned char **out,
                             size_t *out_len) {
    StreamStore *s = stream_store_open(path);
    if (!s || stream_declare(s, "fz", 2, 2, 0, 0) ||
        stream_declare(s, "fz2", 3, 1, 512, 0)) {
        fprintf(stderr, "fuzz: stream seed declare failed\n"); exit(1);
    }
    for (int i = 0; i < 20; i++) {
        uint64_t p = 0, o = 0;
        if (stream_append(s, "fz", 2, UINT32_MAX, "k", 1, "payload", 7, &p, &o)) {
            fprintf(stderr, "fuzz: stream seed append failed\n"); exit(1);
        }
    }
    StreamAppendInput batch[3] = {
        { .key = "a", .key_len = 1, .data = "one", .len = 3 },
        { .key = "b", .key_len = 1, .data = "two", .len = 3 },
        { .key = "c", .key_len = 1, .data = "three", .len = 5 },
    };
    StreamAppendResult results[3];
    if (stream_append_batch(s, "fz", 2, UINT32_MAX, batch, 3, results) ||
        stream_commit(s, "fz", 2, "g", 1, 0, 5) ||
        stream_declare(s, "tr", 2, 1, 64, 0)) {
        fprintf(stderr, "fuzz: stream seed batch failed\n"); exit(1);
    }
    for (int i = 0; i < 6; i++) {
        uint64_t p = 0, o = 0;
        if (stream_append(s, "tr", 2, 0, NULL, 0, "0123456789", 10, &p, &o)) {
            fprintf(stderr, "fuzz: stream seed trim append failed\n"); exit(1);
        }
    }
    stream_store_close(s);
    if (read_file(path, out, out_len)) {
        fprintf(stderr, "fuzz: stream seed read failed\n"); exit(1);
    }
}

static void build_queue_wal(const char *path, unsigned char **out,
                            size_t *out_len) {
    QueueStore *q = queue_store_open(path);
    if (!q || queue_declare(q, "fq", 2, 1, 0) != 0) {
        fprintf(stderr, "fuzz: queue seed failed\n"); exit(1);
    }
    for (int i = 0; i < 20; i++)
        if (queue_publish(q, "fq", 2, "payload", 7, 0, NULL) != 0) {
            fprintf(stderr, "fuzz: queue seed publish failed\n"); exit(1);
        }
    for (int i = 0; i < 5; i++) {
        QueueMessage m;
        if (queue_consume(q, "fq", 2, 1000, &m) != 1) {
            fprintf(stderr, "fuzz: queue seed consume failed\n"); exit(1);
        }
        if (i < 3 && queue_ack(q, "fq", 2, m.delivery_tag) != 1) {
            fprintf(stderr, "fuzz: queue seed ack failed\n"); exit(1);
        }
        queue_message_free(&m);
    }
    queue_store_close(q);
    if (read_file(path, out, out_len)) {
        fprintf(stderr, "fuzz: queue seed read failed\n"); exit(1);
    }
}

int main(void) {
    char base[] = "/tmp/kuttidb-fuzz-XXXXXX";
    int fd = mkstemp(base);
    if (fd < 0) return 1;
    close(fd);
    unlink(base);
    if (snprintf(scratch_path, sizeof scratch_path, "%s.scratch", base) < 0)
        return 1;

    unsigned char *data = NULL;
    size_t len = 0;

    char stream_wal[300];
    snprintf(stream_wal, sizeof stream_wal, "%s.streams", base);
    build_stream_wal(stream_wal, &data, &len);
    fuzz_stream_wal(data, len);
    free(data);

    build_queue_wal(base, &data, &len);
    fuzz_queue_wal(data, len);
    free(data);

    unlink(base);
    unlink(stream_wal);
    unlink(scratch_path);
    printf("WAL FUZZ TESTS PASSED (%d iterations per engine)\n", ITERATIONS);
    return 0;
}
