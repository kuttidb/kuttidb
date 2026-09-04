/* Queue failure tests: disk-exhaustion fail-closed behavior and concurrent
 * publishers/consumers. The disk-full path uses RLIMIT_FSIZE so the WAL write
 * fails with EFBIG once the limit is reached, exercising the same
 * persistence-failure handling as a full filesystem. */
#include "queue.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#define PUBLISHERS 4
#define CONSUMERS 4
#define PER_PUBLISHER 4000

static atomic_int publishers_done;

typedef struct {
    QueueStore *store;
    int index;
} Worker;

static void payload(unsigned char *buf, int publisher, int seq) {
    for (int i = 0; i < 8; i++) buf[i] = 0;
    buf[0] = (unsigned char)publisher;
    for (int i = 0; i < 4; i++)
        buf[1 + i] = (unsigned char)((uint32_t)seq >> (i * 8));
}

static int payload_seq(const unsigned char *buf) {
    return (int)buf[1] | ((int)buf[2] << 8) | ((int)buf[3] << 16) |
           ((int)buf[4] << 24);
}

static int payload_publisher(const unsigned char *buf) { return buf[0]; }

static void *publisher_main(void *arg) {
    Worker *w = arg;
    unsigned char buf[8];
    for (int seq = 0; seq < PER_PUBLISHER; seq++) {
        payload(buf, w->index, seq);
        if (queue_publish(w->store, "stress", 6, buf, sizeof buf, 0, NULL) != 0)
            return (void *)1;
    }
    atomic_fetch_add(&publishers_done, 1);
    return NULL;
}

static int acked[PUBLISHERS][PER_PUBLISHER];
static pthread_mutex_t tally_mu = PTHREAD_MUTEX_INITIALIZER;
static unsigned long long ack_events;

static void *consumer_main(void *arg) {
    Worker *w = arg;
    for (;;) {
        QueueMessage message;
        int rc = queue_consume_for_owner(w->store, "stress", 6, 60000,
                                         (uint64_t)(w->index + 1), &message);
        if (rc < 0) {
            fprintf(stderr, "consume error (persistence_failed=%d)\n",
                    queue_persistence_failed(w->store));
            return (void *)1;
        }
        if (rc == 0) {
            /* Only leave when every publisher has finished and the queue is
             * still empty; otherwise wait for more work. */
            if (atomic_load(&publishers_done) < PUBLISHERS) {
                usleep(200);
                continue;
            }
            queue_reap(w->store);
            rc = queue_consume_for_owner(w->store, "stress", 6, 60000,
                                         (uint64_t)(w->index + 1), &message);
            if (rc == 0) return NULL;
            if (rc < 0) {
                fprintf(stderr, "consume error (persistence_failed=%d)\n",
                        queue_persistence_failed(w->store));
                return (void *)1;
            }
        }
        int publisher = payload_publisher(message.data);
        int seq = payload_seq(message.data);
        uint64_t tag = message.delivery_tag;
        queue_message_free(&message);
        if (queue_ack_for_owner(w->store, "stress", 6, tag,
                                (uint64_t)(w->index + 1)) != 1) {
            fprintf(stderr, "ack rejected for tag %llu (persistence_failed=%d)\n",
                    (unsigned long long)tag,
                    queue_persistence_failed(w->store));
            return (void *)1;
        }
        pthread_mutex_lock(&tally_mu);
        acked[publisher][seq]++;
        ack_events++;
        pthread_mutex_unlock(&tally_mu);
    }
}

static int concurrency_test(const char *path) {
    QueueStore *store = queue_store_open(path);
    if (!store || queue_declare(store, "stress", 6, 1, 0) != 0) {
        fprintf(stderr, "stress declare failed\n");
        return 1;
    }
    Worker workers[PUBLISHERS + CONSUMERS];
    pthread_t threads[PUBLISHERS + CONSUMERS];
    for (int i = 0; i < PUBLISHERS + CONSUMERS; i++) {
        workers[i].store = store;
        workers[i].index = i;
    }
    for (int i = 0; i < PUBLISHERS; i++)
        if (pthread_create(&threads[i], NULL, publisher_main, &workers[i]) != 0) {
            fprintf(stderr, "publisher thread failed\n");
            return 1;
        }
    for (int i = 0; i < CONSUMERS; i++)
        if (pthread_create(&threads[PUBLISHERS + i], NULL, consumer_main,
                           &workers[PUBLISHERS + i]) != 0) {
            fprintf(stderr, "consumer thread failed\n");
            return 1;
        }
    void *status;
    for (int i = 0; i < PUBLISHERS; i++) {
        pthread_join(threads[i], &status);
        if (status) {
            fprintf(stderr, "publisher %d failed\n", i);
            return 1;
        }
    }
    for (int i = 0; i < CONSUMERS; i++) {
        pthread_join(threads[PUBLISHERS + i], &status);
        if (status) {
            fprintf(stderr, "consumer %d failed\n", i);
            return 1;
        }
    }
    if (queue_depth(store, "stress", 6) != 0 ||
        queue_inflight(store, "stress", 6) != 0 ||
        queue_persistence_failed(store)) {
        fprintf(stderr, "stress queue did not drain\n");
        return 1;
    }
    unsigned long long covered = 0;
    for (int p = 0; p < PUBLISHERS; p++)
        for (int s = 0; s < PER_PUBLISHER; s++)
            if (acked[p][s] >= 1) covered++;
            else fprintf(stderr, "missing message %d/%d\n", p, s);
    if (covered != (unsigned long long)PUBLISHERS * PER_PUBLISHER ||
        ack_events < covered) {
        fprintf(stderr, "stress coverage failed (%llu/%llu, acks %llu)\n",
                covered, (unsigned long long)PUBLISHERS * PER_PUBLISHER,
                ack_events);
        return 1;
    }
    queue_store_close(store);
    return 0;
}

/* Child process: fills the queue WAL until the size limit rejects writes,
 * then verifies fail-closed behavior. Exit 0 = pass. */
static int disk_full_child(const char *path) {
    struct stat st;
    if (stat(path, &st) < 0) return 1;
    struct rlimit limit = {.rlim_cur = (rlim_t)st.st_size + 1024,
                           .rlim_max = (rlim_t)st.st_size + 1024};
    if (setrlimit(RLIMIT_FSIZE, &limit) < 0) return 1;
    signal(SIGXFSZ, SIG_IGN);

    QueueStore *store = queue_store_open(path);
    if (!store || queue_declare(store, "fill", 4, 1, 0) != 0) return 1;
    unsigned char buf[400];
    memset(buf, 0x5a, sizeof buf);
    int published = 0, failed = 0;
    for (int i = 0; i < 100; i++) {
        if (queue_publish(store, "fill", 4, buf, sizeof buf, 0, NULL) == 0)
            published++;
        else { failed = 1; break; }
    }
    if (!failed || published == 0) return 1;
    /* Once persistence fails, every durable mutation must fail closed. */
    if (!queue_persistence_failed(store)) return 1;
    if (queue_publish(store, "fill", 4, buf, sizeof buf, 0, NULL) == 0) return 1;
    QueueMessage message;
    if (queue_consume(store, "fill", 4, 1000, &message) != -1) return 1;
    /* Non-durable queues keep working: they never touch the WAL. */
    if (queue_declare(store, "volatile", 8, 0, 0) != 0 ||
        queue_publish(store, "volatile", 8, "ok", 2, 0, NULL) != 0 ||
        queue_consume(store, "volatile", 8, 1000, &message) != 1 ||
        message.len != 2 || memcmp(message.data, "ok", 2) != 0) {
        queue_message_free(&message);
        return 1;
    }
    queue_message_free(&message);
    queue_store_close(store);
    return 0;
}

static int disk_full_test(const char *path) {
    QueueStore *store = queue_store_open(path);
    if (!store || queue_declare(store, "seed", 4, 1, 0) != 0 ||
        queue_publish(store, "seed", 4, "seed", 4, 0, NULL) != 0) {
        fprintf(stderr, "disk-full seed failed\n");
        return 1;
    }
    queue_store_close(store);
    pid_t pid = fork();
    if (pid < 0) { perror("fork"); return 1; }
    if (pid == 0) _exit(disk_full_child(path));
    int wstatus = 0;
    if (waitpid(pid, &wstatus, 0) < 0 || !WIFEXITED(wstatus) ||
        WEXITSTATUS(wstatus) != 0) {
        fprintf(stderr, "disk-full child failed (status %d)\n", wstatus);
        return 1;
    }
    /* Recovery must retain the valid durable prefix written before the
     * failure, including the seeded message. */
    store = queue_store_open(path);
    if (!store || queue_persistence_failed(store)) {
        fprintf(stderr, "disk-full recovery open failed\n");
        return 1;
    }
    QueueMessage message;
    int rc = queue_consume(store, "seed", 4, 1000, &message);
    if (rc != 1 || message.len != 4 || memcmp(message.data, "seed", 4) != 0 ||
        queue_ack(store, "seed", 4, message.delivery_tag) != 1) {
        queue_message_free(&message);
        fprintf(stderr, "disk-full recovery lost the durable prefix\n");
        return 1;
    }
    queue_message_free(&message);
    queue_store_close(store);
    return 0;
}

int main(void) {
    char dir[] = "/tmp/kuttidb-qfail-XXXXXX";
    if (!mkdtemp(dir)) { perror("mkdtemp"); return 1; }
    char path[256];
    snprintf(path, sizeof path, "%s/queues.wal", dir);
    int rc = concurrency_test(path);
    if (rc == 0) rc = disk_full_test(path);
    unlink(path);
    rmdir(dir);
    if (rc == 0) puts("QUEUE FAILURE TESTS PASSED");
    return rc;
}
