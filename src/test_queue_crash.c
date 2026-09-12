/* Queue crash tests: SIGKILL during publish and between delivery and ACK.
 * A publish is "confirmed" only after its durable record is fsynced and the
 * child reports the message ID over a pipe; recovery must retain every
 * confirmed publish and make delivered-but-unacked messages available again
 * with the redelivery flag set. */
#include "queue.h"

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#define ROUNDS 5
#define CONFIRMED_PER_ROUND 20

static int read_full(int fd, void *buf, size_t len) {
    char *p = buf;
    while (len) {
        ssize_t n = read(fd, p, len);
        if (n <= 0) return -1;
        p += n;
        len -= (size_t)n;
    }
    return 0;
}

static int write_full(int fd, const void *buf, size_t len) {
    const char *p = buf;
    while (len) {
        ssize_t n = write(fd, p, len);
        if (n <= 0) return -1;
        p += n;
        len -= (size_t)n;
    }
    return 0;
}

/* Child: publish until killed, reporting each confirmed message ID. */
static void publish_child(const char *path, int report_fd) {
    QueueStore *store = queue_store_open(path);
    if (!store || queue_declare(store, "crash", 5, 1, 0) != 0) _exit(3);
    for (int i = 0;; i++) {
        char buf[32];
        int len = snprintf(buf, sizeof buf, "m%d", i);
        uint64_t id = 0;
        if (queue_publish(store, "crash", 5, buf, (uint32_t)len, 0, &id) != 0)
            _exit(3);
        if (write_full(report_fd, &id, sizeof id) < 0) _exit(3);
    }
}

static int publish_crash_test(const char *path) {
    uint64_t confirmed[ROUNDS * CONFIRMED_PER_ROUND];
    int confirmed_count = 0;
    for (int round = 0; round < ROUNDS; round++) {
        int fds[2];
        if (pipe(fds) < 0) { perror("pipe"); return 1; }
        pid_t pid = fork();
        if (pid < 0) { perror("fork"); return 1; }
        if (pid == 0) {
            close(fds[0]);
            publish_child(path, fds[1]);
            _exit(0);
        }
        close(fds[1]);
        for (int i = 0; i < CONFIRMED_PER_ROUND; i++)
            if (read_full(fds[0], &confirmed[confirmed_count++], 8) < 0) {
                fprintf(stderr, "publish report failed\n");
                return 1;
            }
        close(fds[0]);
        if (kill(pid, SIGKILL) < 0) { perror("kill"); return 1; }
        int wstatus = 0;
        waitpid(pid, &wstatus, 0);

        QueueStore *store = queue_store_open(path);
        if (!store) { fprintf(stderr, "reopen after crash failed\n"); return 1; }
        uint64_t depth = queue_depth(store, "crash", 5);
        if (depth < (uint64_t)confirmed_count) {
            fprintf(stderr, "round %d lost confirmed publishes: depth %llu < %d\n",
                    round, (unsigned long long)depth, confirmed_count);
            return 1;
        }
        /* Collect every live message ID without acknowledging; in-flight
         * messages still count toward depth across rounds. */
        int *confirmed_seen = calloc((size_t)confirmed_count, sizeof(int));
        QueueMessage message;
        uint64_t live = 0;
        while (queue_consume_for_owner(store, "crash", 5, 60000, 999,
                                       &message) == 1) {
            for (int i = 0; i < confirmed_count; i++)
                if (confirmed[i] == message.id) confirmed_seen[i] = 1;
            if (message.len < 2 ||
                ((const char *)message.data)[0] != 'm') {
                queue_message_free(&message);
                fprintf(stderr, "round %d recovered a corrupt payload\n", round);
                return 1;
            }
            queue_message_free(&message);
            live++;
        }
        if (live != depth) {
            fprintf(stderr, "round %d live %llu != depth %llu\n", round,
                    (unsigned long long)live, (unsigned long long)depth);
            return 1;
        }
        for (int i = 0; i < confirmed_count; i++) {
            if (!confirmed_seen[i]) {
                fprintf(stderr, "round %d lost confirmed publish %d\n",
                        round, i);
                return 1;
            }
        }
        free(confirmed_seen);
        queue_store_close(store);
    }
    return 0;
}

/* Child: consume one message, report the delivery tag, then wait to be
 * killed before it can acknowledge. */
static void ack_child(const char *path, int report_fd) {
    QueueStore *store = queue_store_open(path);
    if (!store) _exit(3);
    QueueMessage message;
    int rc = queue_consume_for_owner(store, "ackq", 4, 60000, 777, &message);
    if (rc != 1) _exit(3);
    uint64_t tag = message.delivery_tag;
    queue_message_free(&message);
    if (write_full(report_fd, &tag, sizeof tag) < 0) _exit(3);
    sleep(30);
    _exit(0);
}

static int ack_crash_test(const char *path) {
    QueueStore *store = queue_store_open(path);
    if (!store || queue_declare(store, "ackq", 4, 1, 0) != 0 ||
        queue_publish(store, "ackq", 4, "ackme", 5, 0, NULL) != 0) {
        fprintf(stderr, "ack-crash setup failed\n");
        return 1;
    }
    queue_store_close(store);

    int fds[2];
    if (pipe(fds) < 0) { perror("pipe"); return 1; }
    pid_t pid = fork();
    if (pid < 0) { perror("fork"); return 1; }
    if (pid == 0) {
        close(fds[0]);
        ack_child(path, fds[1]);
        _exit(0);
    }
    close(fds[1]);
    uint64_t tag = 0;
    if (read_full(fds[0], &tag, sizeof tag) < 0) {
        fprintf(stderr, "ack report failed\n");
        return 1;
    }
    close(fds[0]);
    if (kill(pid, SIGKILL) < 0) { perror("kill"); return 1; }
    waitpid(pid, NULL, 0);
    if (tag == 0) { fprintf(stderr, "invalid delivery tag\n"); return 1; }

    /* Restart: the durable delivery record survived, so the message must be
     * available again and flagged as a redelivery with count 2. */
    store = queue_store_open(path);
    if (!store) { fprintf(stderr, "ack-crash reopen failed\n"); return 1; }
    QueueMessage message;
    int rc = queue_consume_for_owner(store, "ackq", 4, 60000, 888, &message);
    if (rc != 1 || message.len != 5 || memcmp(message.data, "ackme", 5) != 0 ||
        !message.redelivered || message.delivery_count != 2) {
        queue_message_free(&message);
        fprintf(stderr, "ack-crash redelivery failed (rc %d)\n", rc);
        return 1;
    }
    uint64_t redelivery_tag = message.delivery_tag;
    queue_message_free(&message);
    if (queue_ack_for_owner(store, "ackq", 4, redelivery_tag, 888) != 1 ||
        queue_depth(store, "ackq", 4) != 0) {
        fprintf(stderr, "ack-crash final ack failed\n");
        return 1;
    }
    queue_store_close(store);
    return 0;
}

/* Force writev to stop inside the header, name and payload, then crash
 * without close/checkpoint. Recovery must keep only the confirmed prefix. */
static int partial_record_crash_test(const char *path) {
    const unsigned cuts[] = {1, 27, 28, 29, 32, 33, 50, 132};
    for (unsigned i = 0; i < sizeof cuts / sizeof cuts[0]; i++) {
        unlink(path);
        QueueStore *store = queue_store_open(path);
        if (!store || queue_declare(store, "parts", 5, 1, 0) != 0 ||
            queue_publish(store, "parts", 5, "kept", 4, 0, NULL) != 0)
            return 1;
        queue_store_close(store);
        struct stat prefix;
        if (stat(path, &prefix)) return 1;
        pid_t pid = fork();
        if (pid < 0) return 1;
        if (pid == 0) {
            store = queue_store_open(path);
            struct rlimit limit = {
                .rlim_cur = (rlim_t)prefix.st_size + cuts[i],
                .rlim_max = (rlim_t)prefix.st_size + cuts[i]
            };
            if (!store || setrlimit(RLIMIT_FSIZE, &limit)) _exit(2);
            signal(SIGXFSZ, SIG_IGN);
            char payload[100];
            memset(payload, 'x', sizeof payload);
            if (queue_publish(store, "parts", 5, payload, sizeof payload, 0, NULL) != -1 ||
                !queue_persistence_failed(store)) _exit(3);
            kill(getpid(), SIGKILL);
            _exit(4);
        }
        int status = 0;
        if (waitpid(pid, &status, 0) != pid || !WIFSIGNALED(status) ||
            WTERMSIG(status) != SIGKILL) return 1;
        store = queue_store_open(path);
        struct stat recovered;
        if (!store || stat(path, &recovered) || recovered.st_size != prefix.st_size ||
            queue_depth(store, "parts", 5) != 1 || queue_persistence_failed(store)) {
            fprintf(stderr, "partial record recovery failed at byte %u\n", cuts[i]);
            return 1;
        }
        QueueMessage message;
        if (queue_consume(store, "parts", 5, 60000, &message) != 1 ||
            message.len != 4 || memcmp(message.data, "kept", 4) ||
            queue_ack(store, "parts", 5, message.delivery_tag) != 1)
            return 1;
        queue_message_free(&message);
        if (queue_publish(store, "parts", 5, "after", 5, 0, NULL) != 0)
            return 1;
        queue_store_close(store);
    }
    return 0;
}

/* Compact timer storage must never persist a monotonic visibility deadline
 * as a wall-clock delayed delivery, or expose one in the other API field. */
static int checkpoint_timer_crash_test(const char *path) {
    unlink(path);
    pid_t pid = fork();
    if (pid < 0) return 1;
    if (pid == 0) {
        QueueStore *store = queue_store_open(path);
        if (!store || queue_declare(store, "timers", 6, 1, 0) != 0 ||
            queue_publish(store, "timers", 6, "delayed", 7, 0, NULL) != 0 ||
            queue_publish(store, "timers", 6, "inflight", 8, 0, NULL) != 0) _exit(2);
        QueueMessage message;
        if (queue_consume_for_owner(store, "timers", 6, 60000, 1, &message) != 1 ||
            queue_nack_for_owner_delay(store, "timers", 6, message.delivery_tag,
                                       1, 1, 60000) != 1) _exit(3);
        queue_message_free(&message);
        if (queue_consume_for_owner(store, "timers", 6, 60000, 2, &message) != 1)
            _exit(4);
        QueueMessageSnapshot snapshot;
        if (queue_message_snapshot(store, "timers", 6, message.id, 0, 0, &snapshot) != 1 ||
            snapshot.not_before_ms != 0 || snapshot.visibility_deadline_ms == 0)
            _exit(5);
        queue_message_snapshot_free(&snapshot);
        queue_message_free(&message);
        QueueMessageSnapshot *messages = NULL;
        uint32_t count = 0;
        if (queue_peek(store, "timers", 6, QUEUE_PEEK_DELAYED | QUEUE_PEEK_INFLIGHT,
                       2, 0, 0, &messages, &count) != 1 || count != 2 ||
            messages[0].not_before_ms == 0 || messages[0].visibility_deadline_ms != 0 ||
            messages[1].not_before_ms != 0 || messages[1].visibility_deadline_ms == 0)
            _exit(6);
        queue_peek_free(messages, count);
        if (queue_checkpoint_force(store) != 1) _exit(7);
        kill(getpid(), SIGKILL);
        _exit(8);
    }
    int status = 0;
    if (waitpid(pid, &status, 0) != pid || !WIFSIGNALED(status) ||
        WTERMSIG(status) != SIGKILL) {
        fprintf(stderr, "checkpoint timer child failed: %d\n", status);
        return 1;
    }
    QueueStore *store = queue_store_open(path);
    if (!store || queue_depth(store, "timers", 6) != 2) return 1;
    QueueMessage message;
    if (queue_consume_for_owner(store, "timers", 6, 60000, 3, &message) != 1 ||
        message.len != 8 || memcmp(message.data, "inflight", 8) || !message.redelivered ||
        queue_ack_for_owner(store, "timers", 6, message.delivery_tag, 3) != 1) return 1;
    queue_message_free(&message);
    if (queue_consume(store, "timers", 6, 60000, &message) != 0) return 1;
    QueueMessageSnapshot *messages = NULL;
    uint32_t count = 0;
    if (queue_peek(store, "timers", 6, QUEUE_PEEK_DELAYED, 1, 0, 0,
                   &messages, &count) != 1 || count != 1 ||
        messages[0].not_before_ms == 0 || messages[0].visibility_deadline_ms != 0)
        return 1;
    queue_peek_free(messages, count);
    queue_store_close(store);
    return 0;
}

int main(void) {
    char dir[] = "/tmp/kuttidb-qcrash-XXXXXX";
    if (!mkdtemp(dir)) { perror("mkdtemp"); return 1; }
    char path[256];
    snprintf(path, sizeof path, "%s/queues.wal", dir);
    int rc = publish_crash_test(path);
    if (rc == 0) rc = ack_crash_test(path);
    if (rc == 0) rc = partial_record_crash_test(path);
    if (rc == 0) rc = checkpoint_timer_crash_test(path);
    unlink(path);
    rmdir(dir);
    if (rc == 0) puts("QUEUE CRASH TESTS PASSED");
    return rc;
}
