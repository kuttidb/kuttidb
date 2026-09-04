#include "queue.h"

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static int consume_value(QueueStore *store, const char *expected,
                         int expected_redelivery, uint64_t *id) {
    QueueMessage message;
    int rc = queue_consume(store, "jobs", 4, 1000, &message);
    if (rc != 1 || message.len != strlen(expected) ||
        memcmp(message.data, expected, message.len) != 0 ||
        (int)message.redelivered != expected_redelivery) {
        queue_message_free(&message);
        return -1;
    }
    *id = message.delivery_tag;
    queue_message_free(&message);
    return 0;
}

int main(void) {
    char path[] = "/tmp/kuttidb-queue-XXXXXX";
    uint64_t owner = 0;
    int fd = mkstemp(path);
    if (fd < 0) { perror("mkstemp"); return 1; }
    close(fd);
    unlink(path);

    QueueStore *store = queue_store_open(path);
    uint64_t id = 0;
    if (!store || queue_declare(store, "jobs", 4, 1, 2) != 0 ||
        queue_publish(store, "jobs", 4, "one", 3, 0, &id) != 0 ||
        queue_publish(store, "jobs", 4, "two", 3, 0, NULL) != 0 ||
        queue_publish(store, "jobs", 4, "overflow", 8, 0, NULL) == 0 ||
        queue_depth(store, "jobs", 4) != 2) {
        fprintf(stderr, "declare/publish/backpressure failed\n");
        return 1;
    }
    QueueMessageSnapshot *peek = NULL;
    uint32_t peek_count = 0;
    if (queue_peek(store, "jobs", 4, QUEUE_PEEK_READY, 2, 1, 64,
                   &peek, &peek_count) != 1 || peek_count != 2 ||
        peek[0].id != id || peek[0].len != 3 ||
        memcmp(peek[0].data, "one", 3) != 0 ||
        peek[1].len != 3 || memcmp(peek[1].data, "two", 3) != 0 ||
        peek[0].delivery_count != 0 || peek[0].state != QUEUE_PEEK_READY ||
        queue_depth(store, "jobs", 4) != 2 || queue_inflight(store, "jobs", 4) != 0) {
        queue_peek_free(peek, peek_count);
        fprintf(stderr, "non-mutating queue browse failed\n");
        return 1;
    }
    queue_peek_free(peek, peek_count);
    peek = NULL;
    peek_count = 0;
    if (queue_peek_after(store, "jobs", 4, id, QUEUE_PEEK_READY, 2, 1, 64,
                         &peek, &peek_count, NULL) != 1 || peek_count != 1 ||
        peek[0].id <= id || peek[0].len != 3 ||
        memcmp(peek[0].data, "two", 3) != 0) {
        queue_peek_free(peek, peek_count);
        fprintf(stderr, "keyset queue browse failed\n");
        return 1;
    }
    queue_peek_free(peek, peek_count);
    peek = NULL;
    peek_count = 0;
    uint64_t first = 0;
    if (consume_value(store, "one", 0, &first) < 0 ||
        queue_peek(store, "jobs", 4, QUEUE_PEEK_INFLIGHT, 1, 0, 0,
                   &peek, &peek_count) != 1 || peek_count != 1 ||
        peek[0].id != id || peek[0].state != QUEUE_PEEK_INFLIGHT ||
        peek[0].delivery_count != 1 || peek[0].data != NULL ||
        queue_inflight(store, "jobs", 4) != 1 ||
        queue_nack(store, "jobs", 4, first, 1) != 1 ||
        consume_value(store, "one", 1, &first) < 0 ||
        queue_ack(store, "jobs", 4, first) != 1) {
        queue_peek_free(peek, peek_count);
        fprintf(stderr, "ack/nack/redelivery failed\n");
        return 1;
    }
    queue_peek_free(peek, peek_count);
    /* Persist one delivery without ACK; recovery must make it available again
     * and indicate redelivery because the durable delivery record survived. */
    uint64_t second = 0;
    if (consume_value(store, "two", 0, &second) < 0 ||
        queue_inflight(store, "jobs", 4) != 1) {
        fprintf(stderr, "consume state failed\n");
        return 1;
    }
    queue_store_close(store);

    store = queue_store_open(path);
    if (!store || consume_value(store, "two", 1, &second) < 0 ||
        queue_ack(store, "jobs", 4, second) != 1 ||
        queue_depth(store, "jobs", 4) != 0 || queue_persistence_failed(store)) {
        fprintf(stderr, "durable recovery failed\n");
        return 1;
    }
    if (queue_declare(store, "leases", 6, 0, 0) != 0 ||
        queue_publish(store, "leases", 6, "lease", 5, 0, NULL) != 0) {
        fprintf(stderr, "lease queue setup failed\n");
        return 1;
    }
    QueueMessage lease;
    if (queue_consume(store, "leases", 6, 1, &lease) != 1) {
        fprintf(stderr, "lease delivery failed\n");
        return 1;
    }
    queue_message_free(&lease);
    /* Removing the tail while an older message remains in flight must retain
     * that older node. This used to leave tail NULL, so the next publish
     * overwrote head and made the first delivery impossible to ACK. */
    if (queue_declare(store, "tailack", 7, 0, 0) != 0 ||
        queue_publish(store, "tailack", 7, "a", 1, 0, NULL) != 0 ||
        queue_publish(store, "tailack", 7, "b", 1, 0, NULL) != 0 ||
        queue_consume_for_owner(store, "tailack", 7, 60000, 301, &lease) != 1) {
        queue_message_free(&lease);
        fprintf(stderr, "tail ACK setup failed\n"); return 1;
    }
    uint64_t first_tailack = lease.delivery_tag;
    queue_message_free(&lease);
    if (queue_consume_for_owner(store, "tailack", 7, 60000, 302, &lease) != 1 ||
        queue_ack_for_owner(store, "tailack", 7, lease.delivery_tag, 302) != 1 ||
        queue_publish(store, "tailack", 7, "c", 1, 0, NULL) != 0 ||
        queue_ack_for_owner(store, "tailack", 7, first_tailack, 301) != 1) {
        queue_message_free(&lease);
        fprintf(stderr, "tail ACK preservation failed\n"); return 1;
    }
    queue_message_free(&lease);
    usleep(5000);
    queue_reap(store);
    if (queue_consume(store, "leases", 6, 1000, &lease) != 1 || !lease.redelivered ||
        queue_ack(store, "leases", 6, lease.delivery_tag) != 1) {
        queue_message_free(&lease);
        fprintf(stderr, "visibility timeout failed\n");
        return 1;
    }
    queue_message_free(&lease);
    if (queue_declare(store, "owners", 6, 0, 0) != 0 ||
        queue_publish(store, "owners", 6, "owned", 5, 0, NULL) != 0 ||
        queue_consume_for_owner(store, "owners", 6, 1000, 101, &lease) != 1 ||
        queue_ack_for_owner(store, "owners", 6, lease.delivery_tag, 202) != 0) {
        queue_message_free(&lease);
        fprintf(stderr, "delivery ownership check failed\n");
        return 1;
    }
    queue_message_free(&lease);
    queue_requeue_owner(store, 101);
    if (queue_consume_for_owner(store, "owners", 6, 1000, 202, &lease) != 1 ||
        !lease.redelivered ||
        queue_ack_for_owner(store, "owners", 6, lease.delivery_tag, 202) != 1) {
        queue_message_free(&lease);
        fprintf(stderr, "owner disconnect requeue failed\n");
        return 1;
    }
    queue_message_free(&lease);
    if (queue_declare(store, "purge", 5, 1, 0) != 0 ||
        queue_publish(store, "purge", 5, "ready", 5, 0, NULL) != 0 ||
        queue_publish(store, "purge", 5, "lease", 5, 0, NULL) != 0 ||
        queue_consume(store, "purge", 5, 60000, &lease) != 1) {
        queue_message_free(&lease);
        fprintf(stderr, "purge setup failed\n"); return 1;
    }
    queue_message_free(&lease);
    uint64_t purged = 0, purge_revision = 0;
    if (queue_revision(store, "purge", 5, &purge_revision) != 1 ||
        queue_purge_if_revision(store, "purge", 5, purge_revision + 1, &purged) != 2 ||
        queue_purge_if_revision(store, "purge", 5, purge_revision, &purged) != 1 || purged != 2 ||
        queue_depth(store, "purge", 5) != 0 || queue_inflight(store, "purge", 5) != 0) {
        fprintf(stderr, "durable purge failed\n"); return 1;
    }
    queue_store_close(store);
    store = queue_store_open(path);
    if (!store || queue_depth(store, "purge", 5) != 0 ||
        queue_consume(store, "purge", 5, 1000, &lease) != 0) {
        queue_message_free(&lease);
        fprintf(stderr, "durable purge recovery failed\n"); return 1;
    }
    if (queue_declare(store, "delete", 6, 1, 0) != 0 ||
        queue_publish(store, "delete", 6, "gone", 4, 0, NULL) != 0) {
        fprintf(stderr, "queue delete setup failed\n"); return 1;
    }
    uint64_t delete_revision = 0, deleted_messages = 0;
    if (queue_revision(store, "delete", 6, &delete_revision) != 1 ||
        queue_delete_if_revision(store, "delete", 6, delete_revision + 1, &deleted_messages) != 2 ||
        queue_delete_if_revision(store, "delete", 6, delete_revision, &deleted_messages) != 1 ||
        deleted_messages != 1 || queue_depth(store, "delete", 6) != 0 ||
        queue_publish(store, "delete", 6, "nope", 4, 0, NULL) == 0) {
        fprintf(stderr, "durable queue delete failed\n"); return 1;
    }
    /* A durable route is an explicit topology dependency: deletion refuses
     * it until the caller removes the route through its own durable action. */
    if (queue_declare(store, "bound", 5, 1, 0) != 0 ||
        exchange_declare(store, "delete-ex", 9, 1, EXCHANGE_DIRECT, NULL, 0) != 0 ||
        exchange_bind(store, "delete-ex", 9, "bound", 5, "k", 1) != 0 ||
        queue_revision(store, "bound", 5, &delete_revision) != 1 ||
        queue_delete_if_revision(store, "bound", 5, delete_revision, NULL) != 3 ||
        exchange_unbind(store, "delete-ex", 9, "bound", 5, "k", 1) != 1 ||
        queue_delete_if_revision(store, "bound", 5, delete_revision, NULL) != 1) {
        fprintf(stderr, "bound queue delete protection failed\n"); return 1;
    }
    queue_store_close(store);
    store = queue_store_open(path);
    uint64_t deleted_depth = 0, deleted_inflight = 0;
    if (!store || queue_stats(store, "delete", 6, &deleted_depth, &deleted_inflight) != 0 ||
        queue_declare(store, "delete", 6, 1, 0) != 0 ||
        queue_publish(store, "delete", 6, "new", 3, 0, NULL) != 0) {
        fprintf(stderr, "durable queue delete recovery failed\n"); return 1;
    }
    if (queue_declare(store, "expiry", 6, 1, 1) != 0 ||
        queue_publish(store, "expiry", 6, "old", 3, 1, NULL) != 0) {
        fprintf(stderr, "expiry queue setup failed\n");
        return 1;
    }
    usleep(5000);
    if (queue_publish(store, "expiry", 6, "new", 3, 0, NULL) != 0 ||
        queue_consume(store, "expiry", 6, 1000, &lease) != 1 ||
        memcmp(lease.data, "new", 3) != 0 ||
        queue_ack(store, "expiry", 6, lease.delivery_tag) != 1) {
        queue_message_free(&lease);
        fprintf(stderr, "expiry/capacity reclamation failed\n");
        return 1;
    }
    queue_message_free(&lease);
    if (queue_declare(store, "restartttl", 10, 1, 0) != 0 ||
        queue_publish(store, "restartttl", 10, "gone", 4, 1, NULL) != 0) {
        fprintf(stderr, "durable expiry setup failed\n");
        return 1;
    }
    queue_store_close(store);
    usleep(5000);
    store = queue_store_open(path);
    if (!store || queue_consume(store, "restartttl", 10, 1000, &lease) != 0) {
        fprintf(stderr, "durable expiry recovery failed\n");
        return 1;
    }
    if (queue_declare(store, "retry", 5, 1, 0) != 0 ||
        queue_publish(store, "retry", 5, "later", 5, 0, NULL) != 0 ||
        queue_consume(store, "retry", 5, 1000, &lease) != 1 ||
        lease.delivery_count != 1 ||
        queue_nack_for_owner_delay(store, "retry", 5, lease.delivery_tag, 0, 1, 20) != 1) {
        queue_message_free(&lease);
        fprintf(stderr, "delayed retry setup failed\n");
        return 1;
    }
    queue_message_free(&lease);
    queue_store_close(store);
    store = queue_store_open(path);
    if (!store || queue_consume(store, "retry", 5, 1000, &lease) != 0) {
        fprintf(stderr, "delayed retry recovered too early\n");
        return 1;
    }
    usleep(25000);
    if (queue_consume(store, "retry", 5, 1000, &lease) != 1 ||
        !lease.redelivered || lease.delivery_count != 2 ||
        queue_ack(store, "retry", 5, lease.delivery_tag) != 1) {
        queue_message_free(&lease);
        fprintf(stderr, "delayed retry recovery failed\n");
        return 1;
    }
    queue_message_free(&lease);
    queue_store_close(store);
    store = queue_store_open(path);
    if (!store || queue_declare(store, "tail", 4, 1, 0) != 0 ||
        queue_publish(store, "tail", 4, "valid", 5, 0, NULL) != 0) {
        fprintf(stderr, "torn WAL setup failed\n");
        return 1;
    }
    queue_store_close(store);
    fd = open(path, O_WRONLY | O_APPEND);
    if (fd < 0 || write(fd, "bad", 3) != 3 || close(fd) < 0) {
        if (fd >= 0) close(fd);
        fprintf(stderr, "torn WAL injection failed\n");
        return 1;
    }
    store = queue_store_open(path);
    if (!store || queue_consume(store, "tail", 4, 1000, &lease) != 1 ||
        memcmp(lease.data, "valid", 5) != 0 ||
        queue_ack(store, "tail", 4, lease.delivery_tag) != 1) {
        queue_message_free(&lease);
        fprintf(stderr, "torn WAL recovery failed\n");
        return 1;
    }
    queue_message_free(&lease);
    if (queue_declare(store, "corrupt", 7, 1, 0) != 0 ||
        queue_publish(store, "corrupt", 7, "first", 5, 0, NULL) != 0 ||
        queue_publish(store, "corrupt", 7, "last", 4, 0, NULL) != 0) {
        fprintf(stderr, "corrupt WAL setup failed\n");
        return 1;
    }
    queue_store_close(store);
    fd = open(path, O_RDWR);
    unsigned char byte;
    if (fd < 0 || lseek(fd, -1, SEEK_END) < 0 || read(fd, &byte, 1) != 1 ||
        lseek(fd, -1, SEEK_END) < 0 || write(fd, &(unsigned char){byte ^ 0xff}, 1) != 1 ||
        close(fd) < 0) {
        if (fd >= 0) close(fd);
        fprintf(stderr, "corrupt WAL injection failed\n");
        return 1;
    }
    store = queue_store_open(path);
    int corrupt_rc = store ? queue_consume(store, "corrupt", 7, 1000, &lease) : -1;
    int corrupt_body = corrupt_rc == 1 && lease.len == 5 &&
                       memcmp(lease.data, "first", 5) == 0;
    int corrupt_ack = corrupt_rc == 1
        ? queue_ack(store, "corrupt", 7, lease.delivery_tag) : -1;
    int corrupt_empty = corrupt_ack == 1
        ? queue_consume(store, "corrupt", 7, 1000, &lease) : -1;
    if (!store || corrupt_rc != 1 || !corrupt_body ||
        corrupt_ack != 1 || corrupt_empty != 0) {
        queue_message_free(&lease);
        fprintf(stderr, "corrupt WAL recovery failed (%d/%d/%d)\n",
                corrupt_rc, corrupt_ack, corrupt_empty);
        return 1;
    }
    queue_store_close(store);

    /* ---- dead-letter queues ---- */
    store = queue_store_open(path);
    if (!store) { fprintf(stderr, "dlq reopen failed\n"); return 1; }
    if (queue_declare_ex(store, "self", 4, 1, 0, "self", 4, 0) == 0 ||
        queue_declare_ex(store, "work", 4, 1, 0, "dead1", 5, 0) != 0 ||
        queue_declare_ex(store, "work", 4, 1, 0, "dead2", 5, 0) == 0 ||
        queue_declare_ex(store, "work", 4, 1, 0, "dead1", 5, 0) != 0) {
        fprintf(stderr, "dlq declaration validation failed\n");
        return 1;
    }
    if (queue_publish(store, "work", 4, "job", 3, 0, NULL) != 0 ||
        queue_consume(store, "work", 4, 1000, &lease) != 1 ||
        queue_nack(store, "work", 4, lease.delivery_tag, 0) != 1 ||
        queue_depth(store, "work", 4) != 0 ||
        queue_depth(store, "dead1", 5) != 1) {
        queue_message_free(&lease);
        fprintf(stderr, "dlq reject routing failed\n");
        return 1;
    }
    queue_message_free(&lease);
    if (queue_consume(store, "dead1", 5, 1000, &lease) != 1 ||
        lease.len != 3 || memcmp(lease.data, "job", 3) != 0 ||
        queue_ack(store, "dead1", 5, lease.delivery_tag) != 1) {
        queue_message_free(&lease);
        fprintf(stderr, "dlq consume failed\n");
        return 1;
    }
    queue_message_free(&lease);
    if (queue_declare_ex(store, "limited", 7, 1, 0, "dead2", 5, 1) != 0 ||
        queue_publish(store, "limited", 7, "poison", 6, 0, NULL) != 0 ||
        queue_consume(store, "limited", 7, 1000, &lease) != 1 ||
        lease.delivery_count != 1 ||
        queue_nack(store, "limited", 7, lease.delivery_tag, 1) != 1) {
        queue_message_free(&lease);
        fprintf(stderr, "dlq max-delivery setup failed\n");
        return 1;
    }
    queue_message_free(&lease);
    if (queue_consume(store, "limited", 7, 1000, &lease) != 0 ||
        queue_depth(store, "limited", 7) != 0 ||
        queue_depth(store, "dead2", 5) != 1) {
        fprintf(stderr, "dlq max-delivery routing failed\n");
        return 1;
    }
    if (queue_declare_ex(store, "expdlq", 6, 1, 0, "dead3", 5, 0) != 0 ||
        queue_publish(store, "expdlq", 6, "stale", 5, 1, NULL) != 0) {
        fprintf(stderr, "dlq expiry setup failed\n");
        return 1;
    }
    usleep(5000);
    if (queue_consume(store, "expdlq", 6, 1000, &lease) != 0 ||
        queue_depth(store, "dead3", 5) != 1 ||
        queue_consume(store, "dead3", 5, 1000, &lease) != 1 ||
        lease.len != 5 || memcmp(lease.data, "stale", 5) != 0 ||
        queue_ack(store, "dead3", 5, lease.delivery_tag) != 1) {
        queue_message_free(&lease);
        fprintf(stderr, "dlq expiry routing failed: consume0=%d dead3depth=%llu\n",
                queue_consume(store, "expdlq", 6, 1000, &lease),
                (unsigned long long)queue_depth(store, "dead3", 5));
        return 1;
    }
    queue_message_free(&lease);
    /* The routed copy lost its expiry: a ping-pong pair of DLQs must not
     * route it back and forth. */
    if (queue_declare_ex(store, "ping", 4, 1, 0, "pong", 4, 0) != 0 ||
        queue_declare_ex(store, "pong", 4, 1, 0, "ping", 4, 0) != 0 ||
        queue_publish(store, "ping", 4, "x", 1, 1, NULL) != 0) {
        fprintf(stderr, "dlq ping-pong setup failed\n");
        return 1;
    }
    usleep(5000);
    queue_reap(store);
    if (queue_depth(store, "ping", 4) != 0 || queue_depth(store, "pong", 4) != 1) {
        fprintf(stderr, "dlq ping-pong guard failed\n");
        return 1;
    }
    /* A full DLQ must fail closed: the rejection is refused and the message
     * stays in-flight instead of being dropped. */
    if (queue_declare(store, "bounded", 7, 1, 1) != 0 ||
        queue_publish(store, "bounded", 7, "fill", 4, 0, NULL) != 0 ||
        queue_declare_ex(store, "src", 3, 1, 0, "bounded", 7, 0) != 0 ||
        queue_publish(store, "src", 3, "keepme", 6, 0, NULL) != 0 ||
        queue_consume(store, "src", 3, 1000, &lease) != 1 ||
        queue_nack(store, "src", 3, lease.delivery_tag, 0) != -1 ||
        queue_inflight(store, "src", 3) != 1 ||
        queue_ack(store, "src", 3, lease.delivery_tag) != 1) {
        queue_message_free(&lease);
        fprintf(stderr, "dlq full fail-closed failed\n");
        return 1;
    }
    queue_message_free(&lease);
    if (queue_deadlettered(store) != 4) {
        fprintf(stderr, "dlq counter mismatch: %llu\n",
                (unsigned long long)queue_deadlettered(store));
        return 1;
    }
    if (queue_publish(store, "work", 4, "durable", 7, 0, NULL) != 0 ||
        queue_consume(store, "work", 4, 1000, &lease) != 1 ||
        queue_nack(store, "work", 4, lease.delivery_tag, 0) != 1) {
        queue_message_free(&lease);
        fprintf(stderr, "dlq durable routing setup failed\n");
        return 1;
    }
    queue_message_free(&lease);
    queue_store_close(store);
    /* Checkpoint: build heterogeneous state (retained messages, in-flight
     * deliveries, delivery counts, delayed retry, exchanges, consumers),
     * force a checkpoint, and verify the WAL shrank and the state survived a
     * clean restart. */
    {
        uint64_t routed = 0, cowner = 0;
        char path[] = "/tmp/kuttidb-ckpt-XXXXXX";
        int ckfd = mkstemp(path);
        if (ckfd < 0) return 1;
        close(ckfd); unlink(path);
        QueueStore *cs = queue_store_open(path);
        if (!cs) { fprintf(stderr, "checkpoint: open failed\n"); return 1; }
        if (queue_declare_ex(cs, "cq", 2, 1, 0, "cdlq", 4, 5) != 0 ||
            queue_declare(cs, "cplain", 6, 1, 0) != 0 ||
            exchange_declare(cs, "cex", 3, 0, EXCHANGE_DIRECT, NULL, 0) != 0 ||
            exchange_bind(cs, "cex", 3, "cplain", 6, "k", 1) != 0 ||
            queue_consumer_register(cs, "cworker", 7, &cowner) != 0) {
            fprintf(stderr, "checkpoint: setup failed\n"); return 1;
        }
        /* Drain enough triples to push the WAL past the checkpoint floor:
         * acknowledged history is exactly what the checkpoint retires. */
        for (int i = 0; i < 12000; i++) {
            QueueMessage hm;
            if (queue_publish(cs, "cq", 2, "hist", 4, 0, NULL) != 0 ||
                queue_consume(cs, "cq", 2, 60000, &hm) != 1 ||
                queue_ack(cs, "cq", 2, hm.delivery_tag) != 1) {
                queue_message_free(&hm);
                fprintf(stderr, "checkpoint: history drain failed at %d\n", i);
                return 1;
            }
            queue_message_free(&hm);
        }
        for (int i = 0; i < 500; i++)
            if (queue_publish(cs, "cq", 2, "msg", 3, 0, NULL) != 0) {
                fprintf(stderr, "checkpoint: publish failed\n"); return 1;
            }
        QueueMessage cm;
        if (queue_consume(cs, "cq", 2, 60000, &cm) != 1) {
            fprintf(stderr, "checkpoint: consume failed\n"); return 1;
        }
        /* One delayed retry plus an exchange delivery on the side queue. */
        if (queue_nack_for_owner_delay(cs, "cq", 2, cm.delivery_tag, 0, 1, 250) != 1 ||
            queue_publish(cs, "cplain", 6, "side", 4, 0, NULL) != 0 ||
            exchange_publish(cs, "cex", 3, "k", 1, "routed", 6, 0, &routed) != 0) {
            fprintf(stderr, "checkpoint: mixed ops failed\n"); return 1;
        }
        uint64_t live_depth = queue_depth(cs, "cq", 2);
        struct stat cst;
        if (stat(path, &cst) || cst.st_size < 8192) {
            fprintf(stderr, "checkpoint: WAL too small to prove compaction\n"); return 1;
        }
        /* The checkpoint fired automatically when history crossed the
         * trigger: the WAL must now be bounded by live state, not history
         * (pre-checkpoint size would exceed 1.2 MB). */
        if (queue_checkpoint_maybe(cs) != 1) {
            fprintf(stderr, "checkpoint: forced run did not compact\n"); return 1;
        }
        if (stat(path, &cst) || cst.st_size > 500 * 1024) {
            fprintf(stderr, "checkpoint: WAL not compacted (%lld bytes)\n",
                    (long long)cst.st_size); return 1;
        }
        if (queue_depth(cs, "cq", 2) != live_depth ||
            queue_inflight(cs, "cq", 2) != 0) {
            fprintf(stderr, "checkpoint: state drifted\n"); return 1;
        }
        queue_message_free(&cm);
        queue_store_close(cs);
        /* Recovery: everything must replay from the checkpointed WAL. */
        cs = queue_store_open(path);
        if (!cs) { fprintf(stderr, "checkpoint: reopen failed\n"); return 1; }
        if (queue_depth(cs, "cq", 2) != live_depth ||
            queue_depth(cs, "cplain", 6) != 2 ||
            queue_consumer_register(cs, "cworker", 7, &cowner) != 0 ||
            cowner == 0) {
            fprintf(stderr, "checkpoint: recovery mismatch cq=%llu want=%llu cplain=%llu cowner=%llu\n",
                    (unsigned long long)queue_depth(cs, "cq", 2),
                    (unsigned long long)live_depth,
                    (unsigned long long)queue_depth(cs, "cplain", 6),
                    (unsigned long long)cowner);
            return 1;
        }
        int got_delayed = 0;
        /* The delayed delivery becomes eligible again after its delay. */
        usleep(300000);
        for (int i = 0; i < 60; i++) {
            QueueMessage dm;
            int drc = queue_consume(cs, "cq", 2, 60000, &dm);
            if (drc == 1) {
                if (dm.redelivered && dm.delivery_count >= 2) got_delayed = 1;
                queue_ack(cs, "cq", 2, dm.delivery_tag);
                queue_message_free(&dm);
            }
            if (got_delayed) break;
            usleep(20000);
        }
        if (!got_delayed) {
            fprintf(stderr, "checkpoint: delayed retry state lost\n"); return 1;
        }
        queue_store_close(cs);
        unlink(path);
    }

    store = queue_store_open(path);
    if (!store || queue_depth(store, "work", 4) != 0 ||
        queue_depth(store, "dead1", 5) != 1 ||
        queue_consume(store, "dead1", 5, 1000, &lease) != 1 ||
        lease.len != 7 || memcmp(lease.data, "durable", 7) != 0 ||
        queue_ack(store, "dead1", 5, lease.delivery_tag) != 1) {
        queue_message_free(&lease);
        fprintf(stderr, "dlq durable recovery failed\n");
        return 1;
    }
    queue_message_free(&lease);
    queue_store_close(store);
    /* Durable named consumers: stable identity, disconnect semantics, and
     * graceful unregister. */
    store = queue_store_open(path);
    if (!store || queue_declare(store, "subs", 4, 1, 0) != 0 ||
        queue_consumer_register(store, "worker", 6, &owner) != 0 ||
        owner == 0) {
        fprintf(stderr, "consumer register failed\n");
        return 1;
    }
    for (int i = 0; i < 3; i++)
        if (queue_publish(store, "subs", 4, "payload", 7, 0, NULL) != 0) {
            fprintf(stderr, "consumer seed publish failed\n");
            return 1;
        }
    QueueMessage owned;
    if (queue_consume_for_consumer(store, "subs", 4, "worker", 6, 60000,
                                   &owned) != 1 ||
        owned.delivery_count != 1) {
        queue_message_free(&owned);
        fprintf(stderr, "consume as named consumer failed\n");
        return 1;
    }
    /* Reconnecting with the same name must regain the same owner token, and
     * the pre-disconnect delivery stays owned by that token (it follows its
     * visibility deadline instead of being requeued). */
    queue_store_close(store);
    store = queue_store_open(path);
    uint64_t owner2 = 0;
    if (!store || queue_consumer_register(store, "worker", 6, &owner2) != 0 ||
        owner2 != owner) {
        fprintf(stderr, "consumer owner was not stable across restart\n");
        return 1;
    }
    /* The old delivery did not survive the restart as an owned in-flight
     * message (delivery tags are per-process); it must be ready again. */
    QueueMessage again;
    if (queue_consume_for_consumer(store, "subs", 4, "worker", 6, 60000,
                                   &again) != 1 ||
        again.id != owned.id || again.delivery_count != 2) {
        queue_message_free(&again);
        fprintf(stderr, "post-restart redelivery failed\n");
        return 1;
    }
    uint64_t again_id = again.id;
    queue_message_free(&again);
    /* Graceful unregister requeues the in-flight delivery immediately and
     * removes the registration durably. */
    if (queue_consumer_unregister(store, "worker", 6) != 1 ||
        queue_consume(store, "subs", 4, 1000, &lease) != 1 ||
        lease.id != again_id || !lease.redelivered) {
        queue_message_free(&lease);
        fprintf(stderr, "graceful unregister requeue failed\n");
        return 1;
    }
    queue_message_free(&lease);
    if (queue_consumer_unregister(store, "worker", 6) != 0 ||
        queue_consume_for_consumer(store, "subs", 4, "worker", 6, 1000,
                                   &again) != 0 ||
        queue_consumer_count(store) != 0) {
        fprintf(stderr, "consumer unregister cleanup failed\n");
        return 1;
    }
    /* A fresh registration gets a new owner token; deliveries under it hold
     * across a plain disconnect until the visibility deadline. */
    if (queue_consumer_register(store, "worker-2", 8, &owner) != 0 ||
        owner == owner2 ||
        queue_consume_for_consumer(store, "subs", 4, "worker-2", 8, 60000,
                                   &owned) != 1) {
        fprintf(stderr, "second consumer registration failed\n");
        return 1;
    }
    /* Unregistering an unknown consumer is a MISS, not an error. */
    if (queue_consumer_unregister(store, "ghost", 5) != 0) {
        fprintf(stderr, "unknown unregister should be a miss\n");
        return 1;
    }
    queue_store_close(store);
    store = queue_store_open(path);
    uint64_t owner3 = 0;
    if (!store || queue_consumer_lookup(store, "worker", 6, &owner3) ||
        !queue_consumer_lookup(store, "worker-2", 8, &owner3) ||
        owner3 != owner) {
        fprintf(stderr, "durable consumer registry recovery failed\n");
        return 1;
    }
    queue_store_close(store);
    unlink(path);
    puts("QUEUE CORE TESTS PASSED");
    return 0;
}
