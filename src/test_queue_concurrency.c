/* Phase -1 concurrency gates: independent queues mutate in parallel under
 * the three lock domains, and grouped-fsync waiters cannot be lost. Run
 * under TSan in CI. */
#define _POSIX_C_SOURCE 200809L
#include "queue.h"
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define THREADS 8
#define OPS 2000
#define QUEUES 8

static atomic_int shared_remaining;

static QueueStore *store;

static void *independent_worker(void *arg) {
    long id = (long)arg;
    char name[16];
    snprintf(name, sizeof name, "q%ld", id);
    unsigned char payload[64];
    memset(payload, (int)('a' + id), sizeof payload);
    /* Own queue: no cross-queue contention except metadata + WAL. */
    for (int i = 0; i < OPS; i++) {
        QueueMessage m;
        if (queue_publish(store, name, strlen(name), payload, sizeof payload, 0, NULL))
            return (void *)1;
        if (queue_consume_for_owner(store, name, strlen(name), 60000,
                                    (uint64_t)id + 1, &m) != 1)
            return (void *)1;
        if (queue_ack_for_owner(store, name, strlen(name), m.delivery_tag,
                                (uint64_t)id + 1) != 1)
            return (void *)1;
        queue_message_free(&m);
    }
    return NULL;
}

static void *shared_worker_pub(void *arg) {
    long id = (long)arg;
    unsigned char payload[64];
    memset(payload, (int)('A' + id), sizeof payload);
    for (int i = 0; i < OPS; i++)
        if (queue_publish(store, "shared", 6, payload, sizeof payload, 0, NULL))
            return (void *)1;
    return NULL;
}


static void *shared_worker_con(void *arg) {
    long id = (long)arg;
    for (;;) {
        QueueMessage m;
        int rc = queue_consume_for_owner(store, "shared", 6, 2000,
                                         (uint64_t)(QUEUES + id + 1), &m);
        if (rc < 0) return (void *)1;
        if (rc == 0) {
            if (atomic_load(&shared_remaining) == 0) return NULL;
            usleep(200);
            continue;
        }
        if (queue_ack_for_owner(store, "shared", 6, m.delivery_tag,
                                (uint64_t)(QUEUES + id + 1)) != 1) {
            queue_message_free(&m);
            return (void *)1;
        }
        queue_message_free(&m);
        atomic_fetch_sub(&shared_remaining, 1);
    }
}

int main(void) {
    unlink("/tmp/kuttidb-conc.wal");
    store = queue_store_open("/tmp/kuttidb-conc.wal");
    if (!store) { puts("open failed"); return 1; }
    for (long i = 0; i < QUEUES; i++) {
        char name[16];
        snprintf(name, sizeof name, "q%ld", i);
        if (queue_declare(store, name, strlen(name), 1, OPS * 2)) {
            puts("declare failed"); return 1;
        }
    }
    if (queue_declare(store, "shared", 6, 1, THREADS * OPS)) {
        puts("shared declare failed"); return 1;
    }

    pthread_t th[THREADS];
    for (long i = 0; i < THREADS; i++)
        if (pthread_create(&th[i], NULL, independent_worker, (void *)i)) return 1;
    for (int i = 0; i < THREADS; i++) {
        void *r = NULL;
        if (pthread_join(th[i], &r) || r) {
            fprintf(stderr, "independent worker %d failed\n", i);
            return 1;
        }
    }
    for (long i = 0; i < QUEUES; i++) {
        char name[16];
        snprintf(name, sizeof name, "q%ld", i);
        if (queue_depth(store, name, strlen(name)) != 0 ||
            queue_inflight(store, name, strlen(name)) != 0) {
            fprintf(stderr, "independent queue %s not drained\n", name);
            return 1;
        }
    }
    puts("INDEPENDENT QUEUES OK"); fflush(stdout);

    /* Grouped fsync under contention: 4 publishers + 4 consumers share one
     * durable queue and every waiter must be released. */
    atomic_store(&shared_remaining, (THREADS / 2) * OPS);
    pthread_t st[THREADS];

    struct { QueueStore *store; int index; } dummy[THREADS];
    for (int i = 0; i < THREADS; i++) dummy[i].store = store, dummy[i].index = i;
    for (int i = 0; i < THREADS/2; i++)
        if (pthread_create(&st[i], NULL, shared_worker_pub, &dummy[i])) return 1;
    for (int i = THREADS/2; i < THREADS; i++)
        if (pthread_create(&st[i], NULL, shared_worker_con, &dummy[i])) return 1;
    for (int i = 0; i < THREADS; i++) {
        void *r = NULL;
        if (pthread_join(st[i], &r) || r) {
            fprintf(stderr, "shared worker %d failed (grouped-fsync lost wakeup?)\n", i);
            return 1;
        }
    }
    if (queue_depth(store, "shared", 6) != 0 ||
        queue_persistence_failed(store)) {
        puts("shared queue not drained"); return 1;
    }
    puts("GROUPED FSYNC OK"); fflush(stdout);
    queue_store_close(store);
    unlink("/tmp/kuttidb-conc.wal");
    puts("QUEUE CONCURRENCY TESTS PASSED");
    return 0;
}
