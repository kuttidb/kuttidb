/* Core tests for atomic cache-plus-message transactions (queue side). */
#include "queue.h"

#include <fcntl.h>
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
        }                                                     \
    } while (0)

static char *temp_path(void) {
    static char path[128];
    snprintf(path, sizeof path, "/tmp/kuttidb-tx-XXXXXX");
    int fd = mkstemp(path);
    if (fd < 0) { perror("mkstemp"); exit(1); }
    close(fd);
    unlink(path);
    return path;
}

static int expect(QueueStore *store, const char *queue, const char *expected) {
    QueueMessage message;
    int rc = queue_consume(store, queue, (uint32_t)strlen(queue), 60000,
                           &message);
    if (!expected) {
        if (rc == 1) { queue_message_free(&message); return 1; }
        return 0; /* empty or missing queue */
    }
    if (rc != 1 || message.len != strlen(expected) ||
        memcmp(message.data, expected, message.len) != 0) {
        if (rc == 1) queue_message_free(&message);
        return -1;
    }
    queue_message_free(&message);
    return 0;
}

static void test_commit_and_recovery(void) {
    char *path = temp_path();
    QueueStore *store = queue_store_open(path);
    CHECK(store != NULL, "commit: open");
    if (!store) return;
    CHECK(queue_declare(store, "q", 1, 1, 0) == 0, "commit: declare queue");
    CHECK(queue_declare(store, "q2", 2, 1, 0) == 0, "commit: declare second");

    QueueTx *tx = NULL;
    CHECK(queue_tx_prepare(store, NULL, 0, "q", 1, "m1", 2, 42, &tx) == 0,
          "commit: enqueue prepare");
    CHECK(queue_tx_id(tx) == 42, "commit: id carried");
    uint64_t routed = 0;
    CHECK(queue_tx_commit(tx, &routed) == 0 && routed == 1,
          "commit: materialized one");
    CHECK(expect(store, "q", "m1") == 0, "commit: message delivered");

    /* fanout transaction through an exchange */
    CHECK(exchange_declare(store, "fx", 2, 1, EXCHANGE_FANOUT, NULL, 0) == 0 &&
          exchange_bind(store, "fx", 2, "q", 1, "", 0) == 0 &&
          exchange_bind(store, "fx", 2, "q2", 2, "x", 1) == 0,
          "commit: fanout setup");
    CHECK(queue_tx_prepare(store, "fx", 2, "rk", 2, "m2", 2, 43, &tx) == 0,
          "commit: fanout prepare");
    CHECK(queue_tx_commit(tx, &routed) == 0 && routed == 2,
          "commit: fanout materialized two");
    CHECK(expect(store, "q", "m2") == 0 && expect(store, "q2", "m2") == 0,
          "commit: both copies delivered");
    CHECK(!queue_persistence_failed(store), "commit: no WAL failure");
    queue_store_close(store);

    /* Restart: prepare+commit records materialize the same messages once. */
    store = queue_store_open(path);
    CHECK(store != NULL, "commit: reopen");
    if (!store) return;
    CHECK(expect(store, "q", "m1") == 0, "commit: m1 recovered");
    CHECK(expect(store, "q", "m2") == 0, "commit: m2 recovered");
    CHECK(expect(store, "q2", "m2") == 0, "commit: fanout copy recovered");
    uint64_t *pending = NULL;
    CHECK(queue_tx_pending_ids(store, &pending) == 0,
          "commit: no pending after replay");
    free(pending);
    queue_store_close(store);
    unlink(path);
}

static void test_prepare_only_is_discarded(void) {
    char *path = temp_path();
    QueueStore *store = queue_store_open(path);
    CHECK(store != NULL, "discard: open");
    if (!store) return;
    CHECK(queue_declare(store, "q", 1, 1, 0) == 0, "discard: declare");
    QueueTx *tx = NULL;
    CHECK(queue_tx_prepare(store, NULL, 0, "q", 1, "lost", 4, 7, &tx) == 0,
          "discard: prepared");
    /* Simulate a crash after the prepare: close without committing. The
     * prepare record is on disk, the commit authority (cache WAL) is not. */
    queue_store_close(store);

    store = queue_store_open(path);
    CHECK(store != NULL, "discard: reopen");
    if (!store) return;
    CHECK(expect(store, "q", NULL) == 0, "discard: nothing materialized");
    /* Reconciliation without a cache commit drops the pending transaction. */
    uint64_t *ids = NULL;
    uint64_t n = queue_tx_pending_ids(store, &ids);
    CHECK(n == 1 && ids && ids[0] == 7, "discard: prepare is pending");
    free(ids);
    CHECK(queue_tx_resolve(store, 7, 0) == 1, "discard: resolved as dropped");
    CHECK(expect(store, "q", NULL) == 0, "discard: still nothing delivered");
    queue_store_close(store);

    /* Reopening again changes nothing: the stale prepare stays inert. */
    store = queue_store_open(path);
    CHECK(store != NULL, "discard: reopen twice");
    if (store) {
        CHECK(expect(store, "q", NULL) == 0, "discard: still empty");
        queue_store_close(store);
    }
    unlink(path);
}

static void test_reconciliation_completes_commit(void) {
    char *path = temp_path();
    QueueStore *store = queue_store_open(path);
    CHECK(store != NULL, "reconcile: open");
    if (!store) return;
    CHECK(queue_declare(store, "q", 1, 1, 0) == 0, "reconcile: declare");
    QueueTx *tx = NULL;
    CHECK(queue_tx_prepare(store, NULL, 0, "q", 1, "keep", 4, 9, &tx) == 0,
          "reconcile: prepared");
    /* The process dies here: the cache commit exists but the queue WAL has
     * only the prepare. Recovery calls queue_tx_resolve(committed=1). */
    queue_store_close(store);

    store = queue_store_open(path);
    CHECK(store != NULL, "reconcile: reopen");
    if (!store) return;
    CHECK(expect(store, "q", NULL) == 0, "reconcile: nothing before resolve");
    CHECK(queue_tx_resolve(store, 9, 1) == 1, "reconcile: transaction finished");
    CHECK(expect(store, "q", "keep") == 0, "reconcile: message materialized");
    CHECK(!queue_persistence_failed(store), "reconcile: commit record durable");
    queue_store_close(store);

    /* The written TX_COMMIT makes the queue WAL self-contained: replaying it
     * materializes the message once, and a repeated resolve is idempotent. */
    store = queue_store_open(path);
    CHECK(store != NULL, "reconcile: reopen again");
    if (!store) return;
    CHECK(expect(store, "q", "keep") == 0, "reconcile: replayed once");
    CHECK(expect(store, "q", NULL) == 0, "reconcile: no duplicate");
    CHECK(queue_tx_resolve(store, 9, 1) == 0, "reconcile: resolve idempotent");
    queue_store_close(store);
    unlink(path);
}

static void test_prepare_failures(void) {
    QueueStore *store = queue_store_open(NULL); /* no WAL: refused */
    CHECK(store != NULL, "failures: open in-memory");
    if (!store) return;
    CHECK(queue_declare(store, "q", 1, 1, 0) < 0, "failures: durable refused");
    CHECK(queue_declare(store, "q", 1, 0, 0) == 0, "failures: declare");
    QueueTx *tx = NULL;
    CHECK(queue_tx_prepare(store, NULL, 0, "q", 1, "m", 1, 1, &tx) < 0,
          "failures: tx without WAL refused");
    queue_store_close(store);

    char *path = temp_path();
    store = queue_store_open(path);
    if (!store) { failures++; return; }
    CHECK(queue_declare(store, "full", 4, 1, 1) == 0 &&
          queue_declare(store, "room", 4, 1, 0) == 0,
          "failures: declare queues");
    CHECK(queue_publish(store, "full", 4, "slot", 4, 0, NULL) == 0,
          "failures: fill bounded queue");
    CHECK(queue_tx_prepare(store, NULL, 0, "full", 4, "m", 1, 2, &tx) < 0,
          "failures: capacity fail closed");
    CHECK(exchange_declare(store, "mixed", 5, 1, EXCHANGE_FANOUT, NULL, 0) == 0 &&
          exchange_bind(store, "mixed", 5, "full", 4, "", 0) == 0,
          "failures: exchange setup");
    CHECK(queue_declare(store, "nd", 2, 0, 0) == 0 &&
          exchange_bind(store, "mixed", 5, "nd", 2, "", 0) == 0,
          "failures: bind non-durable target");
    CHECK(queue_tx_prepare(store, "mixed", 5, "k", 1, "m", 1, 3, &tx) < 0,
          "failures: mixed durability refused");
    CHECK(exchange_declare(store, "empty", 5, 1, EXCHANGE_DIRECT, NULL, 0) == 0,
          "failures: unroutable exchange");
    CHECK(queue_tx_prepare(store, "empty", 5, "k", 1, "m", 1, 4, &tx) == 1,
          "failures: unroutable publish");
    CHECK(expect(store, "full", NULL) == 1,
          "failures: bounded slot still in flight");
    queue_store_close(store);
    unlink(path);
}

static void test_corrupt_tail(void) {
    char *path = temp_path();
    QueueStore *store = queue_store_open(path);
    CHECK(store != NULL, "corrupt: open");
    if (!store) return;
    CHECK(queue_declare(store, "q", 1, 1, 0) == 0, "corrupt: declare");
    QueueTx *tx = NULL;
    CHECK(queue_tx_prepare(store, NULL, 0, "q", 1, "m", 1, 11, &tx) == 0,
          "corrupt: prepare");
    CHECK(queue_tx_commit(tx, NULL) == 0, "corrupt: commit");
    queue_store_close(store);
    int fd = open(path, O_WRONLY | O_APPEND);
    if (fd < 0) { perror("open"); failures++; return; }
    if (write(fd, "TORN-TX-TAIL", 12) != 12) perror("write");
    close(fd);
    store = queue_store_open(path);
    CHECK(store != NULL, "corrupt: reopen");
    if (store) {
        CHECK(expect(store, "q", "m") == 0, "corrupt: valid prefix retained");
        CHECK(expect(store, "q", NULL) == 0, "corrupt: no duplication");
        queue_store_close(store);
    }
    unlink(path);
}

int main(void) {
    test_commit_and_recovery();
    test_prepare_only_is_discarded();
    test_reconciliation_completes_commit();
    test_prepare_failures();
    test_corrupt_tail();
    if (failures) {
        fprintf(stderr, "transaction tests: %d failure(s)\n", failures);
        return 1;
    }
    printf("transaction tests: all passed\n");
    return 0;
}
