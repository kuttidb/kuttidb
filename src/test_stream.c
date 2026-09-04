#include "stream.h"
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

struct member_snapshot_test { uint32_t count, assigned[STREAM_GROUP_MEMBERS_MAX]; uint64_t leases[STREAM_GROUP_MEMBERS_MAX]; };
static void member_snapshot_cb(uint32_t index, uint32_t assigned, uint64_t lease, void *ud) {
    struct member_snapshot_test *snapshot = ud;
    if (index < STREAM_GROUP_MEMBERS_MAX) {
        snapshot->assigned[index] = assigned;
        snapshot->leases[index] = lease;
    }
    snapshot->count++;
}

/* Child process: fills the stream WAL until the size limit rejects writes,
 * then verifies fail-closed behavior. Exit 0 = pass. */
static int disk_full_child(const char *path) {
    struct stat st;
    if (stat(path, &st) < 0) return 1;
    struct rlimit limit = {.rlim_cur = (rlim_t)st.st_size + 512,
                           .rlim_max = (rlim_t)st.st_size + 512};
    if (setrlimit(RLIMIT_FSIZE, &limit) < 0) return 1;
    signal(SIGXFSZ, SIG_IGN);
    StreamStore *s = stream_store_open(path);
    if (!s || stream_declare(s, "fill", 4, 1, 0, 0)) return 1;
    unsigned char buf[400];
    memset(buf, 0x5a, sizeof buf);
    int published = 0, failed = 0;
    for (int i = 0; i < 100; i++) {
        uint64_t part = 0, off = 0;
        if (stream_append(s, "fill", 4, 0, NULL, 0, buf, sizeof buf, &part, &off) == 0)
            published++;
        else { failed = 1; break; }
    }
    if (!failed || published == 0) return 1;
    /* Once persistence fails, every durable mutation must fail closed. */
    if (!stream_persistence_failed(s)) return 1;
    uint64_t part = 0, off = 0;
    if (stream_append(s, "fill", 4, 0, NULL, 0, buf, sizeof buf, &part, &off) == 0)
        return 1;
    if (stream_declare(s, "more", 4, 1, 0, 0) == 0) return 1;
    /* Reads of the acknowledged prefix keep working. */
    StreamRecordView *r = NULL; uint32_t n = 0;
    if (stream_fetch(s, "fill", 4, 0, 0, 1000, 1 << 20, &r, &n) != 1 ||
        n != (uint32_t)published) return 1;
    stream_fetch_free(r, n);
    stream_store_close(s);
    return 0;
}

/* Group-commit concurrency: many threads append and commit against one
 * SHARED store instance (the server shares g_streams across event loops).
 * Every acknowledgement must mean its record reached the WAL, per-partition
 * offsets must stay contiguous, and a clean reopen must show every
 * acknowledged record. */
enum { CONC_THREADS = 8, CONC_APPENDS = 2000, CONC_PARTS = 4 };
static char conc_path[256];
static StreamStore *conc_store;
static void *conc_worker(void *arg) {
    long tid = (long)arg;
    StreamStore *s = conc_store;
    char payload[64];
    memset(payload, (int)('a' + tid), sizeof payload);
    for (int i = 0; i < CONC_APPENDS; i++) {
        uint64_t part = 0, off = 0;
        if (stream_append(s, "conc", 4, (uint32_t)(tid % CONC_PARTS), NULL, 0,
                          payload, sizeof payload, &part, &off)) {
            fprintf(stderr, "conc append failed tid=%ld i=%d\n", tid, i);
            return (void *)1;
        }
        if (part != (uint64_t)(tid % CONC_PARTS)) {
            fprintf(stderr, "conc wrong partition tid=%ld part=%llu\n", tid,
                    (unsigned long long)part);
            return (void *)1;
        }
        if (stream_commit(s, "conc", 4, "g", 1, (uint32_t)part, off + 1)) {
            fprintf(stderr, "conc commit failed tid=%ld i=%d\n", tid, i);
            return (void *)1;
        }
    }
    return stream_persistence_failed(s) ? (void *)1 : NULL;
}

static int group_commit_test(void) {
    snprintf(conc_path, sizeof conc_path, "/tmp/kuttidb-stream-conc-XXXXXX");
    int fd = mkstemp(conc_path);
    if (fd < 0) return 1;
    close(fd); unlink(conc_path);
    StreamStore *s = stream_store_open(conc_path);
    if (!s || stream_declare(s, "conc", 4, CONC_PARTS, 0, 0)) return 1;
    conc_store = s;
    pthread_t th[CONC_THREADS];
    for (long i = 0; i < CONC_THREADS; i++)
        if (pthread_create(&th[i], NULL, conc_worker, (void *)i)) return 1;
    for (int i = 0; i < CONC_THREADS; i++) {
        void *ret = NULL;
        if (pthread_join(th[i], &ret) || ret) return 1;
    }
    uint64_t full_records = 0, full_live = 0, incr_live = 0;
    stream_debug_recount(s, &full_records, &full_live, &incr_live);
    if (full_records != (uint64_t)CONC_THREADS * CONC_APPENDS || full_live != incr_live) {
        fprintf(stderr, "group commit lost records: %llu != %d\n",
                (unsigned long long)full_records, CONC_THREADS * CONC_APPENDS);
        return 1;
    }
    uint64_t committed = 0, per_part = (uint64_t)CONC_THREADS * CONC_APPENDS / CONC_PARTS;
    if (stream_group_offset(s, "conc", 4, "g", 1, 0, &committed) != 1 ||
        committed != per_part) {
        fprintf(stderr, "group commit offset wrong: %llu\n", (unsigned long long)committed);
        return 1;
    }
    /* Per-partition offsets must be contiguous 0..N-1 with no gaps. */
    for (uint32_t p = 0; p < CONC_PARTS; p++) {
        uint64_t next = 0;
        while (next < per_part) {
            StreamRecordView *v = NULL; uint32_t nv = 0;
            if (stream_fetch(s, "conc", 4, p, next, 1024, 1ull << 24, &v, &nv) != 1 || !nv) {
                fprintf(stderr, "conc fetch gap partition=%u at=%llu\n", p,
                        (unsigned long long)next);
                return 1;
            }
            for (uint32_t i = 0; i < nv; i++) {
                if (v[i].offset != next) {
                    fprintf(stderr, "conc offset hole partition=%u got=%llu want=%llu\n",
                            p, (unsigned long long)v[i].offset, (unsigned long long)next);
                    return 1;
                }
                next++;
            }
            stream_fetch_free(v, nv);
        }
    }
    stream_store_close(s);
    unlink(conc_path);
    return 0;
}

static int disk_full_test(const char *path) {
    StreamStore *store = stream_store_open(path);
    uint64_t p = 0, o = 0;
    if (!store || stream_declare(store, "seed", 4, 1, 0, 0) ||
        stream_append(store, "seed", 4, 0, NULL, 0, "seed", 4, &p, &o)) {
        fprintf(stderr, "stream disk-full seed failed\n");
        return 1;
    }
    stream_store_close(store);
    pid_t pid = fork();
    if (pid < 0) { perror("fork"); return 1; }
    if (pid == 0) _exit(disk_full_child(path));
    int wstatus = 0;
    if (waitpid(pid, &wstatus, 0) < 0 || !WIFEXITED(wstatus) ||
        WEXITSTATUS(wstatus) != 0) {
        fprintf(stderr, "stream disk-full child failed (status %d)\n", wstatus);
        return 1;
    }
    /* Recovery retains the acknowledged prefix written before the failure. */
    store = stream_store_open(path);
    if (!store || stream_persistence_failed(store)) {
        fprintf(stderr, "stream disk-full recovery open failed\n");
        return 1;
    }
    StreamRecordView *r = NULL; uint32_t n = 0;
    if (stream_fetch(store, "fill", 4, 0, 0, 1000, 1 << 20, &r, &n) != 1 ||
        n != 1 || r[0].len != 400) {
        fprintf(stderr, "stream disk-full prefix lost\n");
        return 1;
    }
    stream_fetch_free(r, n);
    /* A fresh fd without the limit keeps accepting durable writes. */
    if (stream_append(store, "fill", 4, 0, NULL, 0, "after", 5, &p, &o) || o != 1) {
        fprintf(stderr, "stream disk-full recovery append failed\n");
        return 1;
    }
    stream_store_close(store);
    return 0;
}

/* Incremental-accounting cross-check: the O(1) live estimate that drives WAL
 * compaction eligibility must always equal a full traversal recount, and the
 * per-topic record counters must sum to the traversal count. */
static uint64_t stats_records_total;
static void count_cb(const char *name, uint32_t name_len, uint32_t partitions,
                     uint64_t bytes, uint64_t records, void *ud) {
    (void)name; (void)name_len; (void)partitions; (void)bytes; (void)ud;
    stats_records_total += records;
}
static int accounting_ok(StreamStore *s, const char *label) {
    uint64_t full_records = 0, full_live = 0, incr_live = 0;
    stream_debug_recount(s, &full_records, &full_live, &incr_live);
    stats_records_total = 0;
    stream_foreach_stats(s, count_cb, NULL);
    if (full_records != stats_records_total || full_live != incr_live) {
        fprintf(stderr, "stream accounting mismatch (%s): records %llu vs %llu, live %llu vs %llu\n",
                label, (unsigned long long)full_records,
                (unsigned long long)stats_records_total,
                (unsigned long long)full_live, (unsigned long long)incr_live);
        return 0;
    }
    return 1;
}

int main(void) {
    char path[] = "/tmp/kuttidb-stream-XXXXXX";
    int fd = mkstemp(path);
    if (fd < 0) return 1;
    close(fd); unlink(path);
    StreamStore *s = stream_store_open(path);
    uint64_t p = 0, o = 0;
    if (!s || stream_declare(s, "orders", 6, 2, 0, 0) ||
        stream_append(s, "orders", 6, 0, NULL, 0, "one", 3, &p, &o) || p || o ||
        stream_append(s, "orders", 6, 0, NULL, 0, "two", 3, &p, &o) || o != 1 ||
        stream_append(s, "orders", 6, UINT32_MAX, "customer-1", 10, "keyed", 5, &p, &o) ||
        stream_commit(s, "orders", 6, "workers", 7, 0, 2)) {
        fprintf(stderr, "stream setup failed\n"); return 1;
    }
    if (!accounting_ok(s, "setup")) return 1;
    StreamRecordView *r = NULL; uint32_t n = 0; uint64_t committed = 0;
    if (stream_fetch(s, "orders", 6, 0, 0, 10, 1 << 20, &r, &n) != 1 || n != 2 ||
        r[0].offset != 0 || r[1].offset != 1 || r[0].len != 3 ||
        memcmp(r[1].data, "two", 3) ||
        stream_group_offset(s, "orders", 6, "workers", 7, 0, &committed) != 1 || committed != 2 ||
        stream_group_lag(s, "orders", 6, "workers", 7, 0, &committed) != 1 || committed != 0) {
        fprintf(stderr, "stream fetch/commit failed\n"); return 1;
    }
    stream_fetch_free(r, n);
    uint64_t generation = UINT64_MAX;
    if (stream_group_generation(s, "orders", 6, "workers", 7, &generation) != 1 ||
        generation != 0 ||
        stream_commit_if_generation(s, "orders", 6, "workers", 7, 0, 2, generation) != 0 ||
        stream_commit_if_generation(s, "orders", 6, "workers", 7, 0, 3, generation) != 2 ||
        stream_commit_if_generation(s, "orders", 6, "workers", 7, 0, 2, generation + 1) != 1 ||
        stream_commit_if_generation(s, "orders", 6, "missing", 7, 0, 2, generation) != 3 ||
        stream_group_offset(s, "orders", 6, "workers", 7, 0, &committed) != 1 ||
        committed != 2) {
        fprintf(stderr, "conditional Consumer Group commit failed\n"); return 1;
    }
    StreamCommitInput conditional_batch[] = {{.partition = 0, .offset = 2}};
    if (stream_commit_batch_if_generation(s, "orders", 6, "workers", 7,
                                          conditional_batch, 1, generation + 1) != 1 ||
        stream_commit_batch_if_generation(s, "orders", 6, "workers", 7,
                                          conditional_batch, 1, generation) != 0 ||
        stream_commit_batch_if_generation(s, "orders", 6, "workers", 7,
                                          conditional_batch, 1, generation + 1) != 1) {
        fprintf(stderr, "conditional Consumer Group batch commit failed\n"); return 1;
    }
    StreamCommitInput invalid_conditional_batch[] = {{.partition = 0, .offset = 2},
                                                     {.partition = 0, .offset = 3}};
    if (stream_commit_batch_if_generation(s, "orders", 6, "workers", 7,
                                          invalid_conditional_batch, 2, generation) != 2 ||
        stream_group_offset(s, "orders", 6, "workers", 7, 0, &committed) != 1 ||
        committed != 2) {
        fprintf(stderr, "conditional Consumer Group batch validation was not atomic\n"); return 1;
    }
    uint64_t reset_old[STREAM_PARTITIONS_MAX], reset_new[STREAM_PARTITIONS_MAX];
    uint32_t reset_count = 0;
    if (stream_group_reset_offsets_if_generation(s, "orders", 6, "workers", 7,
                                                 generation, STREAM_OFFSET_RESET_ABSOLUTE,
                                                 0, 0, 0, reset_old, reset_new,
                                                 STREAM_PARTITIONS_MAX, &reset_count) ||
        reset_count != 2 || reset_old[0] != 2 || reset_new[0] != 0 ||
        stream_group_offset(s, "orders", 6, "workers", 7, 0, &committed) != 1 ||
        committed != 0 ||
        stream_group_reset_offsets_if_generation(s, "orders", 6, "workers", 7,
                                                 generation + 1, STREAM_OFFSET_RESET_LATEST,
                                                 0, 0, 0, NULL, NULL, 0, NULL) != 1 ||
        stream_group_reset_offsets_if_generation(s, "orders", 6, "workers", 7,
                                                 generation, STREAM_OFFSET_RESET_LATEST,
                                                 0, 0, 0, reset_old, reset_new,
                                                 STREAM_PARTITIONS_MAX, &reset_count) ||
        reset_new[0] != 2 ||
        stream_group_offset(s, "orders", 6, "workers", 7, 0, &committed) != 1 ||
        committed != 2) {
        fprintf(stderr, "conditional Consumer Group reset failed\n"); return 1;
    }
    StreamAppendInput batch[] = {
        { .key = "first", .key_len = 5, .data = "alpha", .len = 5 },
        { .key = "second", .key_len = 6, .data = "beta", .len = 4 },
    };
    StreamAppendResult batch_results[2];
    if (stream_declare(s, "batch", 5, 1, 0, 0) ||
        stream_append_batch(s, "batch", 5, UINT32_MAX, batch, 2, batch_results) ||
        batch_results[0].partition || batch_results[0].offset ||
        batch_results[1].partition || batch_results[1].offset != 1 ||
        stream_fetch(s, "batch", 5, 0, 0, 10, 1 << 20, &r, &n) != 1 || n != 2 ||
        r[0].key_len != 5 || r[1].key_len != 6 ||
        memcmp(r[0].key, "first", 5) || memcmp(r[1].key, "second", 6) ||
        memcmp(r[0].data, "alpha", 5) || memcmp(r[1].data, "beta", 4)) {
        fprintf(stderr, "stream batch append failed\n"); return 1;
    }
    stream_fetch_free(r, n);
    if (stream_fetch(s, "orders", 6, 0, 0, 10, 18, &r, &n) != -2) {
        fprintf(stderr, "stream fetch byte limit failed\n"); return 1;
    }
    uint32_t *parts = NULL, assigned = 0;
    uint64_t gen = 0, gen2 = 0;
    if (stream_group_join(s, "orders", 6, "team", 4, 20, 1000, &parts, &assigned, &gen) ||
        assigned != 2 || parts[0] != 0 || parts[1] != 1 || gen != 1) {
        free(parts); fprintf(stderr, "first group assignment failed\n"); return 1;
    }
    free(parts); parts = NULL;
    if (stream_group_reset_offsets_if_generation(s, "orders", 6, "team", 4,
                                                 gen, STREAM_OFFSET_RESET_LATEST,
                                                 0, 0, 0, NULL, NULL, 0, NULL) != 4 ||
        stream_group_reset_offsets_if_generation(s, "orders", 6, "team", 4,
                                                 gen, STREAM_OFFSET_RESET_LATEST,
                                                 0, 0, 1, reset_old, reset_new,
                                                 STREAM_PARTITIONS_MAX, &reset_count) ||
        reset_count != 2 || reset_new[0] != 2) {
        fprintf(stderr, "active Consumer Group reset safeguards failed\n"); return 1;
    }
    if (stream_group_member_assigned(s, "orders", 6, "team", 4, 20, 0, &gen2) != 1 ||
        gen2 != gen || stream_group_member_assigned(s, "orders", 6, "team", 4, 99, 0, NULL) != 0 ||
        stream_commit_for_owner_if_generation(s, "orders", 6, "team", 4, 0, 1, 20, gen) ||
        stream_commit_for_owner_if_generation(s, "orders", 6, "team", 4, 0, 1, 20, gen + 1) != 1 ||
        stream_commit_for_owner_if_generation(s, "orders", 6, "team", 4, 0, 3, 20, gen) != 2 ||
        stream_commit_for_owner_if_generation(s, "orders", 6, "team", 4, 0, 1, 99, gen) != 3) {
        fprintf(stderr, "owner-aware conditional Consumer Group commit failed\n"); return 1;
    }
    struct member_snapshot_test member_snapshot = {0}; uint32_t member_count = 0;
    if (stream_group_member_snapshot(s, "orders", 6, "team", 4,
                                     member_snapshot_cb, &member_snapshot,
                                     &gen2, &member_count) != 1 ||
        gen2 != gen || member_count != 1 || member_snapshot.count != 1 ||
        member_snapshot.assigned[0] != 2 || !member_snapshot.leases[0] ||
        stream_group_member_snapshot(s, "orders", 6, "missing", 7,
                                     member_snapshot_cb, &member_snapshot,
                                     NULL, NULL) != 0) {
        fprintf(stderr, "Consumer Group member snapshot failed\n"); return 1;
    }
    /* A plain heartbeat must not change the generation. */
    if (stream_group_join(s, "orders", 6, "team", 4, 20, 1000, &parts, &assigned, &gen2) ||
        gen2 != 1) {
        free(parts); fprintf(stderr, "heartbeat changed generation\n"); return 1;
    }
    free(parts); parts = NULL;
    if (stream_group_join(s, "orders", 6, "team", 4, 10, 1000, &parts, &assigned, &gen) ||
        assigned != 1 || parts[0] != 0 || gen != 2 ||
        stream_commit_for_owner(s, "orders", 6, "team", 4, 1, 1, 10) == 0 ||
        stream_commit_for_owner(s, "orders", 6, "team", 4, 0, 1, 10) != 0) {
        free(parts); fprintf(stderr, "group assignment enforcement failed\n"); return 1;
    }
    free(parts); parts = NULL;
    if (stream_group_join(s, "orders", 6, "team", 4, 20, 1000, &parts, &assigned, &gen) ||
        assigned != 1 || parts[0] != 1 || gen != 2) {
        free(parts); fprintf(stderr, "group rebalance failed\n"); return 1;
    }
    free(parts); parts = NULL;
    /* Graceful leave: the departing member releases its partitions at once
     * and the generation moves so the survivors know a rebalance happened. */
    if (stream_group_leave_member(s, "orders", 6, "team", 4, 10) != 0 ||
        stream_group_leave_member(s, "orders", 6, "team", 4, 10) == 0 ||
        stream_group_leave_member(s, "orders", 6, "team", 4, 99) == 0 ||
        stream_group_leave_member(s, "orders", 6, "ghost", 5, 10) == 0) {
        fprintf(stderr, "graceful leave failed\n"); return 1;
    }
    if (stream_group_join(s, "orders", 6, "team", 4, 20, 1000, &parts, &assigned, &gen) ||
        assigned != 2 || parts[0] != 0 || parts[1] != 1 || gen != 3) {
        free(parts); fprintf(stderr, "leave rebalance failed\n"); return 1;
    }
    free(parts); parts = NULL;
    /* Leaving one group must not touch memberships in other groups. */
    uint32_t *other = NULL, other_count = 0;
    if (stream_group_join(s, "orders", 6, "pair", 4, 10, 1000, &other, &other_count, &gen) ||
        other_count != 2 || gen != 1) {
        free(other); fprintf(stderr, "second group join failed\n"); return 1;
    }
    free(other);
    stream_group_leave_owner(s, 10);
    if (stream_group_join(s, "orders", 6, "pair", 4, 10, 1000, &other, &other_count, &gen) ||
        other_count != 2 || gen != 3) {
        free(other); fprintf(stderr, "leave_owner rebalance failed\n"); return 1;
    }
    free(other); parts = NULL;
    if (stream_group_join(s, "orders", 6, "team", 4, 20, 1000, &parts, &assigned, &gen) ||
        assigned != 2 || gen != 3) {
        free(parts); fprintf(stderr, "cross-group leave disturbed team\n"); return 1;
    }
    free(parts);
    uint64_t truncate_revision = 0;
    if (stream_declare(s, "truncate", 8, 1, 0, 0) ||
        stream_append(s, "truncate", 8, 0, NULL, 0, "zero", 4, &p, &o) ||
        stream_append(s, "truncate", 8, 0, NULL, 0, "one", 3, &p, &o) ||
        stream_append(s, "truncate", 8, 0, NULL, 0, "two", 3, &p, &o) ||
        stream_revision(s, "truncate", 8, &truncate_revision) != 1 ||
        stream_truncate_if_revision(s, "truncate", 8, 0, 2, truncate_revision + 1) != 1 ||
        stream_truncate_if_revision(s, "truncate", 8, 0, 4, truncate_revision) != 2 ||
        stream_truncate_if_revision(s, "truncate", 8, 0, 2, truncate_revision) != 0 ||
        stream_fetch(s, "truncate", 8, 0, 0, 10, 1 << 20, &r, &n) != 1 || n != 1 ||
        r[0].offset != 2 || memcmp(r[0].data, "two", 3)) {
        stream_fetch_free(r, n);
        fprintf(stderr, "conditional stream truncation failed\n"); return 1;
    }
    stream_fetch_free(r, n);
    StreamPartitionStats truncate_stats[1]; uint32_t truncate_parts = 0;
    if (stream_partition_snapshot(s, "truncate", 8, truncate_stats, 1,
                                  &truncate_parts, &truncate_revision) != 1 ||
        truncate_parts != 1 || truncate_stats[0].base_offset != 2 ||
        truncate_stats[0].next_offset != 3 || truncate_stats[0].retained_bytes != 3) {
        fprintf(stderr, "stream partition snapshot failed\n"); return 1;
    }
    if (!accounting_ok(s, "post-group-ops")) return 1;
    stream_store_close(s);
    /* A torn stream-WAL tail is discarded without losing the acknowledged
     * prefix, matching the crash-recovery contract. */
    fd = open(path, O_WRONLY | O_APPEND);
    if (fd < 0 || write(fd, "tail", 4) != 4 || close(fd)) {
        fprintf(stderr, "stream torn-tail injection failed\n"); return 1;
    }
    s = stream_store_open(path);
    if (!s || stream_fetch(s, "orders", 6, 0, 0, 10, 1 << 20, &r, &n) != 1 || n != 2 ||
        stream_group_offset(s, "orders", 6, "workers", 7, 0, &committed) != 1 || committed != 2) {
        fprintf(stderr, "stream recovery failed\n"); return 1;
    }
    stream_fetch_free(r, n);
    if (stream_fetch(s, "truncate", 8, 0, 0, 10, 1 << 20, &r, &n) != 1 || n != 1 ||
        r[0].offset != 2 || memcmp(r[0].data, "two", 3)) {
        stream_fetch_free(r, n);
        fprintf(stderr, "stream truncation recovery failed\n"); return 1;
    }
    stream_fetch_free(r, n);
    if (stream_fetch(s, "batch", 5, 0, 0, 10, 1 << 20, &r, &n) != 1 || n != 2 ||
        r[0].key_len != 5 || r[1].key_len != 6 ||
        memcmp(r[0].key, "first", 5) || memcmp(r[1].key, "second", 6) ||
        memcmp(r[0].data, "alpha", 5) || memcmp(r[1].data, "beta", 4)) {
        fprintf(stderr, "stream batch recovery failed\n"); return 1;
    }
    stream_fetch_free(r, n);
    if (!accounting_ok(s, "post-torn-tail-recovery")) return 1;
    stream_store_close(s);
    s = stream_store_open(path);
    if (!s || stream_declare(s, "retained", 8, 1, 3, 0) ||
        stream_append(s, "retained", 8, 0, NULL, 0, "aaa", 3, &p, &o) ||
        stream_append(s, "retained", 8, 0, NULL, 0, "bbb", 3, &p, &o) ||
        stream_fetch(s, "retained", 8, 0, 0, 10, 1 << 20, &r, &n) != 1 || n != 1 ||
        r[0].offset != 1 || memcmp(r[0].data, "bbb", 3)) {
        fprintf(stderr, "stream retention failed\n"); return 1;
    }
    stream_fetch_free(r, n);
    if (!accounting_ok(s, "post-size-retention-trim")) return 1;
    stream_store_close(s);
    s = stream_store_open(path);
    if (!s || stream_declare(s, "compact", 7, 1, 2050, 0)) {
        fprintf(stderr, "stream compaction setup failed\n"); return 1;
    }
    char block[1024]; memset(block, 'x', sizeof block);
    for (int i = 0; i < 1100; i++)
        if (stream_append(s, "compact", 7, 0, i == 1099 ? "k" : NULL,
                          i == 1099 ? 1 : 0, block, sizeof block, &p, &o)) {
            fprintf(stderr, "stream compaction append failed\n"); return 1;
        }
    /* The next mutation performs a compact-before-append checkpoint. */
    if (stream_append(s, "compact", 7, 0, NULL, 0, "z", 1, &p, &o) || o != 1100 ||
        stream_commit(s, "compact", 7, "workers", 7, 0, 1101)) {
        fprintf(stderr, "stream compaction checkpoint failed\n"); return 1;
    }
    stream_store_close(s);
    struct stat st;
    if (stat(path, &st) || st.st_size > (1 << 20)) {
        fprintf(stderr, "stream WAL did not compact (%lld bytes)\n", (long long)st.st_size); return 1;
    }
    s = stream_store_open(path);
    if (!s || stream_fetch(s, "compact", 7, 0, 0, 10, 1 << 20, &r, &n) != 1 || n != 3 ||
        r[0].offset != 1098 || r[1].offset != 1099 || r[1].key_len != 1 ||
        memcmp(r[1].key, "k", 1) || r[2].offset != 1100 || r[2].len != 1 ||
        stream_group_offset(s, "compact", 7, "workers", 7, 0, &committed) != 1 || committed != 1101) {
        fprintf(stderr, "stream compaction recovery failed\n"); return 1;
    }
    stream_fetch_free(r, n);
    uint64_t compact_revision = 0;
    if (stream_revision(s, "compact", 7, &compact_revision) != 1 ||
        compact_revision != 2200) {
        fprintf(stderr, "stream checkpoint revision recovery failed\n"); return 1;
    }
    if (!accounting_ok(s, "post-compaction-recovery")) return 1;
    /* Age-based retention on a durable store with a long topic name: the
     * expired prefix must be trimmed with one coalesced trim boundary, the
     * trim must be durable, and the acknowledged append must survive. */
    if (stream_declare(s, "age-retention-with-a-longer-topic-name", 36, 2, 0, 40) ||
        stream_append(s, "age-retention-with-a-longer-topic-name", 36, 0, NULL, 0, "old1", 4, &p, &o) ||
        stream_append(s, "age-retention-with-a-longer-topic-name", 36, 0, NULL, 0, "old2", 4, &p, &o)) {
        fprintf(stderr, "stream age retention setup failed\n"); return 1;
    }
    usleep(120000); /* let the 40 ms age window pass */
    if (stream_append(s, "age-retention-with-a-longer-topic-name", 36, 0, NULL, 0, "new", 3, &p, &o) ||
        o != 2 ||
        stream_fetch(s, "age-retention-with-a-longer-topic-name", 36, 0, 0, 10, 1 << 20, &r, &n) != 1 ||
        n != 1 || r[0].offset != 2 || memcmp(r[0].data, "new", 3)) {
        fprintf(stderr, "stream age retention trim failed\n"); return 1;
    }
    stream_fetch_free(r, n);
    if (!accounting_ok(s, "post-age-retention-trim")) return 1;
    stream_store_close(s);
    s = stream_store_open(path);
    if (!s || stream_fetch(s, "age-retention-with-a-longer-topic-name", 36, 0, 0, 10, 1 << 20, &r, &n) != 1 ||
        n != 1 || r[0].offset != 2 || memcmp(r[0].data, "new", 3) ||
        stream_fetch(s, "age-retention-with-a-longer-topic-name", 36, 1, 0, 10, 1 << 20, &r, &n) != 1 || n != 0) {
        fprintf(stderr, "stream age retention recovery failed\n"); return 1;
    }
    stream_fetch_free(r, n);
    if (!accounting_ok(s, "post-age-retention-recovery")) return 1;

    /* Retention updates and deletion have their own WAL records.  A stale
     * revision must have no effect, an acknowledged delete must remove every
     * private record/group allocation, and a restart must not resurrect it. */
    uint64_t lifecycle_revision = 0;
    if (stream_declare(s, "lifecycle", 9, 1, 0, 0) ||
        stream_append(s, "lifecycle", 9, 0, NULL, 0, "ok", 2, &p, &o) ||
        stream_revision(s, "lifecycle", 9, &lifecycle_revision) != 1 ||
        stream_set_retention_if_revision(s, "lifecycle", 9, 2, 0,
                                         lifecycle_revision + 1) != 1 ||
        stream_set_retention_if_revision(s, "lifecycle", 9, 2, 0,
                                         lifecycle_revision) != 0 ||
        stream_revision(s, "lifecycle", 9, &lifecycle_revision) != 1 ||
        stream_delete_if_revision(s, "lifecycle", 9, lifecycle_revision + 1) != 1 ||
        stream_delete_if_revision(s, "lifecycle", 9, lifecycle_revision) != 0 ||
        stream_revision(s, "lifecycle", 9, &lifecycle_revision) != 0 ||
        !accounting_ok(s, "post-stream-delete")) {
        fprintf(stderr, "stream lifecycle mutation failed\n"); return 1;
    }
    stream_store_close(s);
    s = stream_store_open(path);
    if (!s || stream_revision(s, "lifecycle", 9, &lifecycle_revision) != 0 ||
        !accounting_ok(s, "post-stream-delete-recovery")) {
        fprintf(stderr, "stream deletion recovery failed\n"); return 1;
    }
    uint64_t updated_revision = 0;
    if (stream_declare(s, "updated", 7, 1, 0, 0) ||
        stream_append(s, "updated", 7, 0, NULL, 0, "a", 1, &p, &o) ||
        stream_append(s, "updated", 7, 0, NULL, 0, "b", 1, &p, &o) ||
        stream_revision(s, "updated", 7, &updated_revision) != 1 ||
        stream_set_retention_if_revision(s, "updated", 7, 1, 0,
                                         updated_revision) != 0 ||
        stream_fetch(s, "updated", 7, 0, 0, 10, 1 << 20, &r, &n) != 1 ||
        n != 1 || r[0].offset != 1 || memcmp(r[0].data, "b", 1)) {
        stream_fetch_free(r, n);
        fprintf(stderr, "stream retention update failed\n"); return 1;
    }
    stream_fetch_free(r, n);
    stream_store_close(s);
    s = stream_store_open(path);
    if (!s || stream_append(s, "updated", 7, 0, NULL, 0, "c", 1, &p, &o) ||
        o != 2 || stream_fetch(s, "updated", 7, 0, 0, 10, 1 << 20, &r, &n) != 1 ||
        n != 1 || r[0].offset != 2 || memcmp(r[0].data, "c", 1) ||
        !accounting_ok(s, "post-retention-update-recovery")) {
        stream_fetch_free(r, n);
        fprintf(stderr, "stream retention update recovery failed\n"); return 1;
    }
    stream_fetch_free(r, n);
    stream_store_close(s);

    /* A corrupt final record is rejected while the acknowledged prefix and
     * the offset sequence survive (corruption detection contract). */
    char cpath[] = "/tmp/kuttidb-stream-corrupt-XXXXXX";
    fd = mkstemp(cpath);
    if (fd < 0) return 1;
    close(fd); unlink(cpath);
    s = stream_store_open(cpath);
    if (!s || stream_declare(s, "corrupt", 7, 1, 0, 0) ||
        stream_append(s, "corrupt", 7, 0, NULL, 0, "one", 3, &p, &o) ||
        stream_append(s, "corrupt", 7, 0, NULL, 0, "two", 3, &p, &o) ||
        stream_append(s, "corrupt", 7, 0, NULL, 0, "three", 5, &p, &o) || o != 2) {
        fprintf(stderr, "stream corruption setup failed\n"); return 1;
    }
    stream_store_close(s);
    struct stat cst;
    if (stat(cpath, &cst) || cst.st_size < 10) return 1;
    fd = open(cpath, O_RDWR);
    if (fd < 0) return 1;
    unsigned char byte = 0;
    if (pread(fd, &byte, 1, cst.st_size - 6) != 1) return 1;
    byte ^= 0xff;
    if (pwrite(fd, &byte, 1, cst.st_size - 6) != 1 || close(fd)) return 1;
    s = stream_store_open(cpath);
    if (!s || stream_fetch(s, "corrupt", 7, 0, 0, 10, 1 << 20, &r, &n) != 1 || n != 2 ||
        memcmp(r[0].data, "one", 3) || memcmp(r[1].data, "two", 3)) {
        fprintf(stderr, "stream corruption recovery failed\n"); return 1;
    }
    stream_fetch_free(r, n);
    if (stream_append(s, "corrupt", 7, 0, NULL, 0, "four", 4, &p, &o) || o != 2) {
        fprintf(stderr, "stream corruption offset continuation failed\n"); return 1;
    }
    stream_store_close(s);
    unlink(cpath);

    /* Retention cleanup interrupted mid-record: the torn tail is rejected,
     * the valid prefix stays contiguous, and appends continue the sequence. */
    char tpath[] = "/tmp/kuttidb-stream-trim-XXXXXX";
    fd = mkstemp(tpath);
    if (fd < 0) return 1;
    close(fd); unlink(tpath);
    s = stream_store_open(tpath);
    if (!s || stream_declare(s, "trims", 5, 1, 48, 0)) {
        fprintf(stderr, "stream trim setup failed\n"); return 1;
    }
    char blk[10]; memset(blk, 'b', sizeof blk);
    for (int i = 0; i < 8; i++)
        if (stream_append(s, "trims", 5, 0, NULL, 0, blk, sizeof blk, &p, &o)) {
            fprintf(stderr, "stream trim append failed\n"); return 1;
        }
    stream_store_close(s);
    struct stat tst;
    if (stat(tpath, &tst) || tst.st_size < 4) return 1;
    if (truncate(tpath, tst.st_size - 3)) return 1;
    s = stream_store_open(tpath);
    if (!s || stream_fetch(s, "trims", 5, 0, 0, 100, 1 << 20, &r, &n) != 1) {
        fprintf(stderr, "stream torn-trim recovery failed\n"); return 1;
    }
    uint64_t base = n ? r[0].offset : 0;
    for (uint32_t i = 0; i < n; i++)
        if (r[i].offset != base + i) {
            fprintf(stderr, "stream torn-trim prefix not contiguous\n"); return 1;
        }
    stream_fetch_free(r, n);
    if (stream_append(s, "trims", 5, 0, NULL, 0, "tail", 4, &p, &o) || o != base + n) {
        fprintf(stderr, "stream torn-trim continuation failed\n"); return 1;
    }
    stream_store_close(s);
    unlink(tpath);

    /* A crashed compaction leaves a stale `.compact.*` sibling behind; open
     * must ignore it, keep the old WAL fully valid, and remove the litter. */
    char kpath[] = "/tmp/kuttidb-stream-temp-XXXXXX";
    fd = mkstemp(kpath);
    if (fd < 0) return 1;
    close(fd); unlink(kpath);
    s = stream_store_open(kpath);
    if (!s || stream_declare(s, "kept", 4, 1, 0, 0) ||
        stream_append(s, "kept", 4, 0, NULL, 0, "one", 3, &p, &o) ||
        stream_append(s, "kept", 4, 0, NULL, 0, "two", 3, &p, &o)) {
        fprintf(stderr, "stream stale-temp setup failed\n"); return 1;
    }
    stream_store_close(s);
    char tmppath[600];
    if (snprintf(tmppath, sizeof tmppath, "%s.compact.stale01", kpath) < 0) return 1;
    fd = open(tmppath, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (fd < 0 || write(fd, "junkjunkjunk", 12) != 12 || close(fd)) return 1;
    s = stream_store_open(kpath);
    if (!s || stream_fetch(s, "kept", 4, 0, 0, 10, 1 << 20, &r, &n) != 1 || n != 2) {
        fprintf(stderr, "stream stale-temp recovery failed\n"); return 1;
    }
    stream_fetch_free(r, n);
    stream_store_close(s);
    struct stat tmpst;
    if (stat(tmppath, &tmpst) == 0) {
        fprintf(stderr, "stream stale compaction temp not removed\n"); return 1;
    }
    unlink(kpath);

    /* Disk-exhaustion fail-closed behavior with recovery of the prefix. */
    char dfpath[] = "/tmp/kuttidb-stream-full-XXXXXX";
    fd = mkstemp(dfpath);
    if (fd < 0) return 1;
    close(fd); unlink(dfpath);
    if (disk_full_test(dfpath)) return 1;
    unlink(dfpath);
    if (group_commit_test()) {
        fprintf(stderr, "stream group-commit concurrency failed\n"); return 1;
    }
    unlink(path);
    puts("STREAM CORE TESTS PASSED");
    return 0;
}
