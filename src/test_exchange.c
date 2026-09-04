/* Core tests for the exchange and routing engine. */
#include "queue.h"

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int failures;

struct exchange_snapshot_test { unsigned routers, routes; uint32_t direct_routes; uint64_t direct_published, direct_unroutable; };
static void exchange_snapshot_cb(const char *name, uint32_t name_len, int durable,
                                 unsigned type, const char *alternate,
                                 uint32_t alternate_len, uint32_t route_count,
                                 uint64_t revision, uint64_t published,
                                 uint64_t unroutable, void *ud) {
    (void)revision;
    struct exchange_snapshot_test *snapshot=ud;
    (void)durable; (void)alternate; (void)alternate_len;
    snapshot->routers++;
    if(type==EXCHANGE_DIRECT && name_len==6&&!memcmp(name,"direct",6)){snapshot->direct_routes=route_count;snapshot->direct_published=published;snapshot->direct_unroutable=unroutable;}
}
static void exchange_route_snapshot_cb(const char *queue, uint32_t queue_len,
                                       const char *key, uint32_t key_len, void *ud) {
    struct exchange_snapshot_test *snapshot=ud;
    (void)queue; (void)queue_len; (void)key; (void)key_len; snapshot->routes++;
}

#define CHECK(cond, name)                                         \
    do {                                                          \
        if (!(cond)) {                                            \
            fprintf(stderr, "FAIL: %s\n", name);                  \
            failures++;                                           \
        }                                                         \
    } while (0)

/* Consume one message from `queue` and compare it to `expected`; NULL
 * expects no message (returns 1 when a message was found, 0 when none, -1
 * on error). Returns the delivery tag through `tag` when found. */
static int expect(QueueStore *store, const char *queue, const char *expected,
                  uint64_t *tag) {
    QueueMessage message;
    int rc = queue_consume(store, queue, (uint32_t)strlen(queue), 60000,
                           &message);
    if (!expected) {
        if (rc == 1) { queue_message_free(&message); return 1; }
        return 0; /* empty or missing queue: nothing delivered */
    }
    if (rc != 1 || message.len != strlen(expected) ||
        memcmp(message.data, expected, message.len) != 0) {
        if (rc == 1) queue_message_free(&message);
        return -1;
    }
    if (tag) *tag = message.delivery_tag;
    queue_message_free(&message);
    return 0;
}

static void test_topic_patterns(void) {
    static const struct {
        const char *pattern;
        const char *key;
        int match;
    } cases[] = {
        {"#", "", 1},          {"#", "a", 1},        {"#", "a.b.c", 1},
        {"*", "", 1},          {"*", "a", 1},        {"*", "a.b", 0},
        {"", "", 1},           {"", "a", 0},
        {"a", "a", 1},         {"a", "ab", 0},       {"a", "a.b", 0},
        {"a.#", "a", 1},       {"a.#", "a.b", 1},    {"a.#", "a.b.c", 1},
        {"a.#", "ab", 0},      {"a.#", "b.a", 0},    {"a.#", "a.b.a", 1},
        {"a.*", "a.b", 1},     {"a.*", "a.", 1},     {"a.*", "a", 0},
        {"a.*", "a.b.c", 0},
        {"*.b", "a.b", 1},     {"*.b", ".b", 1},     {"*.b", "b", 0},
        {"*.b", "a.b.c", 0},
        {"*.*", "a.b", 1},     {"*.*", ".", 1},      {"*.*", "a", 0},
        {"a.#.b", "a.b", 1},   {"a.#.b", "a.x.y.b", 1},
        {"a.#.b", "a.b.c", 0}, {"a.#.b", "x.a.b", 0},
        {"#.#", "x.y.z", 1},   {"#.#", "", 1},
        {"a.*.c", "a.b.c", 1}, {"a.*.c", "a..c", 1}, {"a.*.c", "a.c", 0},
        {"*.#", "a", 1},       {"*.#", "a.b.c", 1},
        {"#.a.#", "x.a.y", 1}, {"#.a.#", "a", 1},    {"#.a.#", "b.c", 0},
    };
    QueueStore *store = queue_store_open(NULL);
    CHECK(store != NULL, "topic: open in-memory store");
    if (!store) return;
    CHECK(queue_declare(store, "t", 1, 0, 0) == 0, "topic: declare queue");
    CHECK(exchange_declare(store, "topic", 5, 0, EXCHANGE_TOPIC, NULL, 0) == 0,
          "topic: declare exchange");

    /* Bind every pattern; publish each key and count which patterns fired by
     * consuming from the single queue between publishes. */
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        const char *pattern = cases[i].pattern;
        if (exchange_bind(store, "topic", 5, "t", 1, pattern,
                          (uint32_t)strlen(pattern)) != 0) {
            fprintf(stderr, "FAIL: topic: bind %s\n", pattern);
            failures++;
            continue;
        }
        const char *key = cases[i].key;
        uint64_t routed = 99;
        int rc = exchange_publish(store, "topic", 5, key, (uint32_t)strlen(key),
                                  "m", 1, 0, &routed);
        int got = rc == 0 ? expect(store, "t", NULL, NULL) : -1;
        if (rc != 0 || got != cases[i].match || (int)routed != got) {
            fprintf(stderr, "FAIL: topic: pattern '%s' key '%s' rc=%d routed=%llu got=%d expected %d\n",
                    pattern, key, rc, (unsigned long long)routed, got,
                    cases[i].match);
            failures++;
        }
        CHECK(exchange_unbind(store, "topic", 5, "t", 1, pattern,
                              (uint32_t)strlen(pattern)) == 1,
              "topic: unbind pattern");
    }
    queue_store_close(store);
}

static void test_direct_fanout_default(void) {
    QueueStore *store = queue_store_open(NULL);
    CHECK(store != NULL, "direct/fanout: open");
    if (!store) return;
    CHECK(queue_declare(store, "d1", 2, 0, 0) == 0 &&
          queue_declare(store, "d2", 2, 0, 0) == 0 &&
          queue_declare(store, "other", 5, 0, 0) == 0,
          "direct: declare queues");
    CHECK(exchange_declare(store, "direct", 6, 0, EXCHANGE_DIRECT, NULL, 0) == 0,
          "direct: declare");
    CHECK(exchange_bind(store, "direct", 6, "d1", 2, "blue", 4) == 0 &&
          exchange_bind(store, "direct", 6, "d2", 2, "red", 3) == 0,
          "direct: bind");
    struct exchange_snapshot_test exchange_snapshot={0}; uint32_t route_count=0;
    exchange_foreach_stats(store,exchange_snapshot_cb,&exchange_snapshot);
    CHECK(exchange_snapshot.routers >= 1 && exchange_snapshot.direct_routes == 2 &&
          exchange_foreach_route(store,"direct",6,exchange_route_snapshot_cb,
                                 &exchange_snapshot,&route_count) == 1 &&
          route_count == 2 && exchange_snapshot.routes == 2 &&
          exchange_route_exists(store,"direct",6,"d1",2,"blue",4) == 1 &&
          exchange_route_exists(store,"direct",6,"d1",2,"missing",7) == 0 &&
          exchange_foreach_route(store,"missing",7,exchange_route_snapshot_cb,
                                 &exchange_snapshot,NULL) == 0,
          "direct: routing snapshots");

    uint64_t routed = 99;
    CHECK(exchange_publish(store, "direct", 6, "blue", 4, "b1", 2, 0,
                           &routed) == 0 && routed == 1,
          "direct: blue routes to one");
    CHECK(expect(store, "d1", "b1", NULL) == 0, "direct: d1 got blue");
    CHECK(exchange_publish(store, "direct", 6, "red", 3, "r1", 2, 0,
                           &routed) == 0 && routed == 1,
          "direct: red routes");
    CHECK(expect(store, "d2", "r1", NULL) == 0, "direct: d2 got red");
    CHECK(expect(store, "d1", NULL, NULL) == 0, "direct: d1 untouched");
    CHECK(exchange_publish(store, "direct", 6, "green", 5, "x", 1, 0,
                           &routed) == 0 && routed == 0,
          "direct: no match is unroutable");
    CHECK(exchange_unroutable(store) >= 1, "direct: unroutable counted");
    exchange_foreach_stats(store,exchange_snapshot_cb,&exchange_snapshot);
    CHECK(exchange_snapshot.direct_published >= 3 && exchange_snapshot.direct_unroutable >= 1,
          "direct: per-router publish counters");

    /* empty routing key binds and matches exactly */
    CHECK(exchange_bind(store, "direct", 6, "d1", 2, "", 0) == 0,
          "direct: bind empty key");
    CHECK(exchange_publish(store, "direct", 6, "", 0, "e1", 2, 0,
                           &routed) == 0 && routed == 1,
          "direct: empty key routes");
    CHECK(expect(store, "d1", "e1", NULL) == 0, "direct: empty key delivered");

    /* unknown exchange is an error, not unroutable */
    CHECK(exchange_publish(store, "nope", 4, "blue", 4, "x", 1, 0,
                           &routed) < 0,
          "direct: unknown exchange errors");

    /* fanout: every bound queue gets exactly one copy, double binding deduped */
    CHECK(exchange_declare(store, "fan", 3, 0, EXCHANGE_FANOUT, NULL, 0) == 0,
          "fanout: declare");
    CHECK(exchange_bind(store, "fan", 3, "d1", 2, "", 0) == 0 &&
          exchange_bind(store, "fan", 3, "d1", 2, "ignored", 7) == 0 &&
          exchange_bind(store, "fan", 3, "d2", 2, "x", 1) == 0 &&
          exchange_bind(store, "fan", 3, "other", 5, "", 0) == 0,
          "fanout: bind");
    CHECK(exchange_publish(store, "fan", 3, "anything", 8, "f1", 2, 0,
                           &routed) == 0 && routed == 3,
          "fanout: three targets, deduped");
    CHECK(expect(store, "d1", "f1", NULL) == 0 &&
          expect(store, "d2", "f1", NULL) == 0 &&
          expect(store, "other", "f1", NULL) == 0,
          "fanout: all targets got one copy");

    /* default exchange: routing key names the queue */
    CHECK(exchange_publish(store, "", 0, "d2", 2, "def", 3, 0,
                           &routed) == 0 && routed == 1,
          "default: routes by queue name");
    CHECK(expect(store, "d2", "def", NULL) == 0, "default: delivered");
    CHECK(exchange_publish(store, "", 0, "missing", 6, "x", 1, 0,
                           &routed) == 0 && routed == 0,
          "default: missing queue is unroutable");
    CHECK(exchange_declare(store, "", 0, 0, EXCHANGE_DIRECT, NULL, 0) < 0,
          "default: cannot declare the default exchange");

    /* redeclare mismatches fail; matching redeclare is idempotent */
    CHECK(exchange_declare(store, "direct", 6, 0, EXCHANGE_DIRECT, NULL, 0) == 0,
          "direct: redeclare identical");
    CHECK(exchange_declare(store, "direct", 6, 0, EXCHANGE_TOPIC, NULL, 0) < 0,
          "direct: redeclare type mismatch fails");
    CHECK(exchange_declare(store, "direct", 6, 1, EXCHANGE_DIRECT, NULL, 0) < 0,
          "direct: redeclare durability mismatch fails");
    CHECK(exchange_declare(store, "ae1", 3, 0, EXCHANGE_DIRECT, "ae1", 3) < 0,
          "declare: alternate exchange cannot be self");
    CHECK(exchange_declare(store, "bad", 3, 0, 7, NULL, 0) < 0,
          "declare: invalid type rejected");

    /* binding rules */
    CHECK(exchange_bind(store, "direct", 6, "ghost", 5, "k", 1) < 0,
          "bind: queue must exist");
    CHECK(exchange_unbind(store, "direct", 6, "ghost", 5, "k", 1) == 0,
          "unbind: missing binding reports absent");
    CHECK(exchange_unbind(store, "direct", 6, "d2", 2, "red", 3) == 1,
          "unbind: removes binding");
    CHECK(exchange_publish(store, "direct", 6, "red", 3, "x", 1, 0,
                           &routed) == 0 && routed == 0,
          "unbind: target no longer routed");
    CHECK(exchange_bind(store, "direct", 6, "d2", 2, "red", 3) == 0,
          "unbind: rebind after removal");

    queue_store_close(store);
}

static void test_alternate_exchange(void) {
    QueueStore *store = queue_store_open(NULL);
    CHECK(store != NULL, "AE: open");
    if (!store) return;
    /* The alternate exchange is a fanout so it catches every unroutable
     * message regardless of its routing key. */
    CHECK(queue_declare(store, "sink", 4, 0, 0) == 0 &&
          exchange_declare(store, "sink.x", 5, 0, EXCHANGE_FANOUT, NULL, 0) == 0 &&
          exchange_bind(store, "sink.x", 5, "sink", 4, "", 0) == 0,
          "AE: set up sink");
    CHECK(exchange_declare(store, "main", 4, 0, EXCHANGE_DIRECT, "sink.x", 5) == 0,
          "AE: main with alternate");

    uint64_t routed = 0;
    CHECK(exchange_publish(store, "main", 4, "nomatch", 7, "m1", 2, 0,
                           &routed) == 0 && routed == 1,
          "AE: unroutable routed to alternate");
    CHECK(expect(store, "sink", "m1", NULL) == 0, "AE: sink got the message");

    /* Without bindings on main every publish lands in the alternate. */
    CHECK(exchange_publish(store, "main", 4, "k", 1, "m2", 2, 0,
                           &routed) == 0 && routed == 1,
          "AE: second message also rerouted");
    CHECK(expect(store, "sink", "m2", NULL) == 0, "AE: sink got m2");

    /* Once main can route by itself, the message does not reach the
     * alternate a second time: exactly one copy is delivered. */
    CHECK(exchange_bind(store, "main", 4, "sink", 4, "k", 1) == 0,
          "AE: bind main to sink");
    CHECK(exchange_publish(store, "main", 4, "k", 1, "m3", 2, 0,
                           &routed) == 0 && routed == 1,
          "AE: publish routed by main");
    CHECK(expect(store, "sink", "m3", NULL) == 0, "AE: exactly one copy");
    CHECK(expect(store, "sink", NULL, NULL) == 0, "AE: nothing extra");

    /* AE hop works: mid's unroutable messages land in its alternate. */
    CHECK(queue_declare(store, "deep", 4, 0, 0) == 0 &&
          exchange_declare(store, "deep.x", 6, 0, EXCHANGE_FANOUT, NULL, 0) == 0 &&
          exchange_bind(store, "deep.x", 6, "deep", 4, "", 0) == 0 &&
          exchange_declare(store, "mid", 3, 0, EXCHANGE_DIRECT, "deep.x", 6) == 0,
          "AE: chain set up");
    CHECK(exchange_publish(store, "mid", 3, "nomatch", 7, "m4", 2, 0,
                           &routed) == 0 && routed == 1,
          "AE: mid reroutes into its alternate");
    CHECK(expect(store, "deep", "m4", NULL) == 0, "AE: deep got the message");

    /* One hop only: top's AE is mid, and mid's own AE is not followed, so a
     * message unroutable in mid stops there instead of reaching deep.x. */
    CHECK(exchange_declare(store, "top", 3, 0, EXCHANGE_DIRECT, "mid", 3) == 0,
          "AE: top with mid alternate");
    CHECK(exchange_publish(store, "top", 3, "nomatch", 7, "m5", 2, 0,
                           &routed) == 0 && routed == 0,
          "AE: one hop only, then unroutable");
    CHECK(expect(store, "deep", NULL, NULL) == 0, "AE: deep not reached");

    /* Missing alternate exchange degrades to unroutable, not error. */
    CHECK(exchange_declare(store, "dangling", 8, 0, EXCHANGE_DIRECT, "gone", 4) == 0,
          "AE: declare with missing alternate");
    CHECK(exchange_publish(store, "dangling", 8, "x", 1, "m6", 2, 0,
                           &routed) == 0 && routed == 0,
          "AE: missing alternate is unroutable");

    queue_store_close(store);
}

static void test_capacity_and_ttl(void) {
    QueueStore *store = queue_store_open(NULL);
    CHECK(store != NULL, "capacity: open");
    if (!store) return;
    CHECK(queue_declare(store, "full", 4, 0, 1) == 0 &&
          queue_declare(store, "room", 4, 0, 0) == 0 &&
          exchange_declare(store, "cap", 3, 0, EXCHANGE_FANOUT, NULL, 0) == 0 &&
          exchange_bind(store, "cap", 3, "full", 4, "", 0) == 0 &&
          exchange_bind(store, "cap", 3, "room", 4, "", 0) == 0,
          "capacity: setup");
    CHECK(queue_publish(store, "full", 4, "slot", 4, 0, NULL) == 0,
          "capacity: fill the bounded queue");
    uint64_t routed = 0;
    /* Fail closed before touching any target when one is full. */
    CHECK(exchange_publish(store, "cap", 3, "x", 1, "m", 1, 0, &routed) < 0,
          "capacity: publish fails closed");
    CHECK(expect(store, "room", NULL, NULL) == 0,
          "capacity: room queue untouched");
    uint64_t slot_tag = 0;
    CHECK(expect(store, "full", "slot", &slot_tag) == 0,
          "capacity: full drained");
    /* An in-flight delivery still holds a bounded-queue slot until it is
     * acknowledged, so the ACK is required before capacity frees up. */
    CHECK(queue_ack(store, "full", 4, slot_tag) == 1, "capacity: ack slot");

    /* TTL travels through the exchange and expires independently per copy. */
    CHECK(exchange_publish(store, "cap", 3, "x", 1, "t", 1, 10, &routed) == 0 &&
          routed == 2,
          "capacity: ttl publish routed");
    CHECK(expect(store, "room", "t", NULL) == 0,
          "capacity: room got ttl copy");
    usleep(20000);
    queue_reap(store);
    CHECK(expect(store, "full", NULL, NULL) == 0, "capacity: expired copy reaped");
    queue_store_close(store);
}

static void test_binding_cap(void) {
    QueueStore *store = queue_store_open(NULL);
    CHECK(store != NULL, "cap: open");
    if (!store) return;
    CHECK(queue_declare(store, "c", 1, 0, 0) == 0 &&
          exchange_declare(store, "cap", 3, 0, EXCHANGE_FANOUT, NULL, 0) == 0,
          "cap: setup");
    char key[8];
    for (uint32_t i = 0; i < EXCHANGE_BINDINGS_MAX; i++) {
        key[0] = 'k';
        int n = snprintf(key + 1, sizeof key - 1, "%u", i);
        if (n <= 0 || exchange_bind(store, "cap", 3, "c", 1, key,
                                    (uint32_t)n + 1) != 0) {
            fprintf(stderr, "FAIL: cap: binding %u rejected\n", i);
            failures++;
            break;
        }
    }
    uint64_t routed = 0;
    CHECK(exchange_publish(store, "cap", 3, "zz", 2, "m", 1, 0, &routed) == 0 &&
          routed == 1,
          "cap: fanout bounded to declared bindings");
    CHECK(expect(store, "c", "m", NULL) == 0, "cap: delivered once");
    CHECK(exchange_bind(store, "cap", 3, "c", 1, "overflow", 8) < 0,
          "cap: one binding past the cap fails");
    CHECK(exchange_unbind(store, "cap", 3, "c", 1, "k0", 2) == 1,
          "cap: unbind frees a slot");
    CHECK(exchange_bind(store, "cap", 3, "c", 1, "again", 5) == 0,
          "cap: binding possible after unbind");
    /* over-long topic pattern is rejected */
    CHECK(exchange_declare(store, "tp", 2, 0, EXCHANGE_TOPIC, NULL, 0) == 0,
          "cap: topic exchange");
    char big[256];
    memset(big, '.', sizeof big);
    big[sizeof big - 1] = 0;
    CHECK(exchange_bind(store, "tp", 2, "c", 1, big, (uint32_t)strlen(big)) < 0,
          "cap: over-long pattern rejected");
    queue_store_close(store);
}

static void test_durability_and_recovery(void) {
    char path[] = "/tmp/kuttidb-exchange-XXXXXX";
    int fd = mkstemp(path);
    if (fd < 0) { perror("mkstemp"); failures++; return; }
    close(fd);
    unlink(path);

    QueueStore *store = queue_store_open(path);
    CHECK(store != NULL, "durability: open");
    if (!store) return;
    CHECK(queue_declare(store, "dq", 2, 1, 0) == 0 &&
          queue_declare(store, "ephemeral", 9, 0, 0) == 0,
          "durability: queues");
    CHECK(exchange_declare(store, "dx", 2, 1, EXCHANGE_TOPIC, NULL, 0) == 0,
          "durability: durable exchange");
    CHECK(exchange_declare(store, "vx", 2, 0, EXCHANGE_TOPIC, NULL, 0) == 0,
          "durability: non-durable exchange");
    CHECK(exchange_bind(store, "dx", 2, "dq", 2, " orders.* ", 10) == 0 &&
          exchange_unbind(store, "dx", 2, "dq", 2, " orders.* ", 10) == 1 &&
          exchange_bind(store, "dx", 2, "dq", 2, "orders.*", 8) == 0 &&
          exchange_bind(store, "dx", 2, "ephemeral", 9, "#", 1) == 0 &&
          exchange_bind(store, "vx", 2, "dq", 2, "orders.*", 8) == 0 &&
          exchange_bind(store, "vx", 2, "ephemeral", 9, "#", 1) == 0,
          "durability: bindings (one removed, the rest kept)");
    uint64_t dx_revision = 0;
    CHECK(exchange_revision(store, "dx", 2, &dx_revision) == 1 && dx_revision == 5,
          "durability: router revision advances for each route change");
    uint64_t routed = 0;
    CHECK(exchange_publish(store, "dx", 2, "orders.new", 10, "keep", 4, 0,
                           &routed) == 0 && routed == 2,
          "durability: publish reaches both targets");
    CHECK(expect(store, "dq", "keep", NULL) == 0 &&
          expect(store, "ephemeral", "keep", NULL) == 0,
          "durability: copies delivered");
    CHECK(exchange_publish(store, "vx", 2, "orders.new", 10, "mem", 3, 0,
                           &routed) == 0 && routed == 2,
          "durability: non-durable exchange routes in memory");
    CHECK(expect(store, "dq", "mem", NULL) == 0 &&
          expect(store, "ephemeral", "mem", NULL) == 0,
          "durability: non-durable copies delivered");
    queue_store_close(store);

    store = queue_store_open(path);
    CHECK(store != NULL, "durability: reopen");
    if (!store) return;
    CHECK(exchange_revision(store, "dx", 2, &dx_revision) == 1 && dx_revision == 5 &&
          exchange_unbind_if_revision(store, "dx", 2, "dq", 2, "orders.*", 8, 4) == 2,
          "durability: router revision survives restart and rejects stale unbind");
    /* durable exchange and bindings survive; the non-durable exchange and
     * the non-durable queue do not. Delivered-but-unacked durable messages
     * ("keep", "mem") come back ready for redelivery, before "again". */
    CHECK(exchange_publish(store, "dx", 2, "orders.new", 10, "again", 5, 0,
                           &routed) == 0 && routed == 1,
          "durability: exchange and bindings recovered");
    CHECK(expect(store, "dq", "keep", NULL) == 0 &&
          expect(store, "dq", "mem", NULL) == 0 &&
          expect(store, "dq", "again", NULL) == 0,
          "durability: durable copies recovered in order");
    CHECK(expect(store, "ephemeral", NULL, NULL) == 0,
          "durability: non-durable queue gone");
    CHECK(exchange_publish(store, "vx", 2, "x", 1, "y", 1, 0, &routed) < 0,
          "durability: non-durable exchange not recovered");
    CHECK(!queue_persistence_failed(store), "durability: no WAL failure");
    queue_store_close(store);

    /* A torn or corrupt tail is truncated; the valid prefix (including
     * exchange state) is retained. */
    store = queue_store_open(path);
    if (!store) { failures++; return; }
    CHECK(exchange_declare(store, "tail", 4, 1, EXCHANGE_FANOUT, NULL, 0) == 0 &&
          exchange_bind(store, "tail", 4, "dq", 2, "", 0) == 0,
          "durability: exchange records before corruption");
    queue_store_close(store);
    fd = open(path, O_WRONLY | O_APPEND);
    if (fd < 0) { perror("open"); failures++; return; }
    if (write(fd, "GARBAGE-TORN-TAIL", 17) != 17) { perror("write"); }
    close(fd);
    store = queue_store_open(path);
    CHECK(store != NULL, "durability: reopen after torn tail");
    if (store) {
        uint64_t routed2 = 0;
        CHECK(exchange_publish(store, "tail", 4, "x", 1, "t", 1, 0,
                               &routed2) == 0 && routed2 == 1,
              "durability: valid prefix retained");
        queue_store_close(store);
    }
    unlink(path);
}

static void test_in_memory_durability_policy(void) {
    /* A store without a WAL refuses durable exchange declarations. */
    QueueStore *store = queue_store_open(NULL);
    CHECK(store != NULL, "memstore: open");
    if (!store) return;
    CHECK(queue_declare(store, "q", 1, 1, 0) < 0,
          "memstore: durable queue refused");
    CHECK(exchange_declare(store, "x", 1, 1, EXCHANGE_DIRECT, NULL, 0) < 0,
          "memstore: durable exchange refused");
    CHECK(exchange_declare(store, "x", 1, 0, EXCHANGE_DIRECT, NULL, 0) == 0,
          "memstore: non-durable exchange allowed");
    CHECK(queue_declare(store, "q", 1, 0, 0) == 0 &&
          exchange_bind(store, "x", 1, "q", 1, "k", 1) == 0,
          "memstore: non-durable bind allowed");
    uint64_t routed = 0;
    CHECK(exchange_publish(store, "x", 1, "k", 1, "m", 1, 0, &routed) == 0 &&
          routed == 1,
          "memstore: non-durable publish works");
    queue_store_close(store);
}

static void test_durable_router_delete(void) {
    char path[] = "/tmp/kuttidb-router-delete-XXXXXX";
    int fd = mkstemp(path);
    if (fd < 0) { perror("mkstemp"); failures++; return; }
    close(fd);
    unlink(path);
    QueueStore *store = queue_store_open(path);
    CHECK(store && queue_declare(store, "q", 1, 1, 0) == 0 &&
          exchange_declare(store, "x", 1, 1, EXCHANGE_DIRECT, NULL, 0) == 0 &&
          exchange_bind(store, "x", 1, "q", 1, "k", 1) == 0,
          "router delete: durable topology setup");
    if (!store) { unlink(path); return; }
    uint64_t revision = 0;
    CHECK(exchange_revision(store, "x", 1, &revision) == 1 && revision == 2 &&
          exchange_delete_if_revision(store, "x", 1, revision) == 3 &&
          exchange_unbind_if_revision(store, "x", 1, "q", 1, "k", 1, revision) == 1 &&
          exchange_revision(store, "x", 1, &revision) == 1 && revision == 3 &&
          exchange_delete_if_revision(store, "x", 1, revision) == 1,
          "router delete: routes block deletion until durable unbind");
    queue_store_close(store);
    store = queue_store_open(path);
    CHECK(store && exchange_revision(store, "x", 1, &revision) == 0,
          "router delete: durable tombstone survives restart");
    queue_store_close(store);
    unlink(path);
}

static void test_durable_alternate_update(void) {
    char path[] = "/tmp/kuttidb-router-update-XXXXXX";
    int fd = mkstemp(path);
    if (fd < 0) { perror("mkstemp"); failures++; return; }
    close(fd); unlink(path);
    QueueStore *store = queue_store_open(path);
    uint64_t revision = 0, routed = 0;
    CHECK(store && queue_declare(store, "q", 1, 1, 0) == 0 &&
          exchange_declare(store, "primary", 7, 1, EXCHANGE_DIRECT, NULL, 0) == 0 &&
          exchange_declare(store, "fallback", 8, 1, EXCHANGE_DIRECT, NULL, 0) == 0 &&
          exchange_bind(store, "fallback", 8, "q", 1, "k", 1) == 0 &&
          exchange_revision(store, "primary", 7, &revision) == 1 && revision == 1 &&
          exchange_set_alternate_if_revision(store, "primary", 7, "fallback", 8, revision) == 1,
          "router update: durable alternate setup");
    if (!store) { unlink(path); return; }
    queue_store_close(store);
    store = queue_store_open(path);
    CHECK(store && exchange_revision(store, "primary", 7, &revision) == 1 && revision == 2 &&
          exchange_publish(store, "primary", 7, "k", 1, "x", 1, 0, &routed) == 0 && routed == 1 &&
          expect(store, "q", "x", NULL) == 0,
          "router update: alternate survives restart and routes");
    queue_store_close(store); unlink(path);
}

int main(void) {
    test_topic_patterns();
    test_direct_fanout_default();
    test_alternate_exchange();
    test_capacity_and_ttl();
    test_binding_cap();
    test_durability_and_recovery();
    test_in_memory_durability_policy();
    test_durable_router_delete();
    test_durable_alternate_update();
    if (failures) {
        fprintf(stderr, "exchange tests: %d failure(s)\n", failures);
        return 1;
    }
    printf("exchange tests: all passed\n");
    return 0;
}
