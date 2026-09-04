#define _GNU_SOURCE
#include "queue.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define QUEUE_MESSAGE_MAX (64u << 20)
#define LOG_HEADER 28u

/* Mutex-operation result checks. The metadata and WAL mutexes are
 * ERRORCHECK: a recursive acquisition returns EDEADLK instead of blocking,
 * so an ignored return value can silently corrupt lock ownership. Test and
 * sanitizer builds abort on any mutex error; production keeps the calls but
 * the checks compile to the plain operation. */
#ifdef NDEBUG
#define QLOCK(mu) pthread_mutex_lock(mu)
#define QUNLOCK(mu) pthread_mutex_unlock(mu)
#else
static inline int qlock_check(int rc, const char *what, const char *file, int line) {
    if (rc != 0) {
        fprintf(stderr, "kuttidb queue lock error: %s returned %d at %s:%d\n",
                what, rc, file, line);
        abort();
    }
    return rc;
}
#define QLOCK(mu) qlock_check(pthread_mutex_lock(mu), #mu, __FILE__, __LINE__)
#define QUNLOCK(mu) qlock_check(pthread_mutex_unlock(mu), #mu, __FILE__, __LINE__)
#endif

enum { LOG_DECLARE = 1, LOG_PUBLISH = 2, LOG_ACK = 3, LOG_DELIVER = 4,
       LOG_REQUEUE = 5,
       /* Exchange records: name = exchange name, payload per apply_* below. */
       LOG_EXDECLARE = 6, LOG_EXBIND = 7, LOG_EXUNBIND = 8,
       /* Atomic cache-plus-message transactions. TX_PREPARE reserves the
        * durable intent (with pre-allocated message IDs) and TX_COMMIT
        * materializes it; the commit authority is the cache WAL TX record
        * written between the two, and startup reconciliation finishes
        * interrupted transactions. */
       LOG_TX_PREPARE = 9, LOG_TX_COMMIT = 10,
       /* Durable consumer registrations: name = consumer name, id = owner
        * token (register) or zero (delete). */
       LOG_CONSUMER = 11, LOG_CONSUMER_DEL = 12,
       /* A Queue-wide retained-message removal. `id` is the pre-purge count
        * for observability only; replay clears whatever remains. */
       LOG_PURGE = 13,
       /* Queue tombstones keep allocations stable for concurrent callers but
        * remove the Queue from every public lookup and checkpoint. */
       LOG_DELETE = 14,
       /* Empty durable router tombstone. */
       LOG_EXDELETE = 15,
       LOG_EXUPDATE = 16 };
enum { MESSAGE_READY = 0, MESSAGE_INFLIGHT = 1 };

typedef struct QueueConsumer {
    struct QueueConsumer *next;
    char *name;
    uint32_t name_len;
    uint64_t owner;
} QueueConsumer;

typedef struct Message {
    struct Message *next;
    struct Message *prev;
    struct Message *tag_next; /* intrusive delivery-tag index chain */
    uint64_t id;
    uint64_t delivery_tag;
    uint64_t owner;
    uint64_t expires_ms;
    uint64_t not_before_ms;
    uint64_t visibility_deadline_ms;
    uint32_t len;
    uint32_t deliveries;
    uint32_t wal_footprint;   /* WAL bytes this message contributes */
    unsigned state;
    char data[];
} Message;

typedef struct Queue {
    struct Queue *next;
    pthread_mutex_t lock;         /* per-queue message state */
    uint64_t lock_id;             /* creation order; fanout locks ascending */
    char *name;
    uint32_t name_len;
    uint64_t max_depth;
    uint64_t depth;
    uint64_t inflight;
    uint64_t revision;
    int durable;
    int deleted;
    char *dlq_name;
    uint32_t dlq_len;
    uint32_t max_deliveries;
    Message *head;
    Message *tail;
    /* Delivery-tag index: open chaining, intrusive (no per-node heap use).
     * Only in-flight messages carry tags, so the index is proportional to
     * live in-flight state, not queue depth. */
    Message **tag_buckets;
    uint32_t tag_mask;
    uint32_t tag_count;
    /* WAL bytes a checkpoint would write for this queue's live state
     * (declaration + retained messages + delivery counts). Updated under
     * queue->lock; drives the checkpoint trigger. */
    uint64_t live_bytes;
    /* Messages with a TTL; when zero and nothing is in flight, the retention
     * scan is skipped entirely instead of walking the queue per operation. */
    uint32_t ttl_count;
    /* Earliest visibility deadline among in-flight messages (lazy: it may
     * under-report after removals, which only costs one extra pass). The
     * visibility walk runs only when this deadline is actually due. */
    uint64_t earliest_visibility;
    /* Consume scan hint: every message strictly before this one is
     * in-flight, so consume starts here instead of walking the wall of
     * in-flight deliveries at the head. Reset to head whenever a message is
     * requeued (its position relative to the hint is unknown). */
    Message *ready_hint;
} Queue;

typedef struct Binding {
    struct Binding *next;
    char *queue;
    uint32_t queue_len;
    char *key;
    uint32_t key_len;
    uint32_t wal_footprint;
} Binding;

typedef struct Exchange {
    struct Exchange *next;
    char *name;
    uint32_t name_len;
    unsigned type;
    int durable;
    char *ae_name;
    uint32_t ae_len;
    Binding *bindings;
    uint32_t binding_count;
    uint64_t revision, publish_attempt_count, unroutable_count;
} Exchange;

/* One resolved delivery inside a transaction. `queue` is set on the runtime
 * path (store lock held); replay-side pending transactions carry only the
 * name and re-resolve at materialization. */
typedef struct QueueTxTarget {
    char *name;
    uint32_t name_len;
    Queue *queue;
    uint64_t msg_id;
} QueueTxTarget;

struct QueueTx {
    QueueStore *store;
    uint64_t tx_id;
    uint64_t expires_ms;
    char *source;         /* exchange/queue name the client addressed */
    uint32_t source_len;
    void *data;
    uint32_t len;
    QueueTxTarget *targets;
    uint32_t target_count;
    uint32_t wal_footprint;   /* prepare-record bytes counted against the WAL */
    struct QueueTx *next; /* pending list during recovery */
};

/* Lock architecture (documented in docs/design/ARCHITECTURE.md):
 *   metadata (store->lock): queue/exchange/consumer declarations and name
 *   lookups, transaction list. Queues and exchanges are never freed while
 *   the store lives, so pointers stay valid after the metadata lock is
 *   released.
 *   queue->lock: one per queue; message list, tag index, hints, counters.
 *   wal_lock: record writes (sequence order equals byte order), the group
 *   fsync state, and the synced high-water mark.
 *   Order: metadata -> queue -> wal. Dead-letter routing takes the DLQ lock
 *   with trylock (a busy DLQ keeps the message for a later pass, matching
 *   the documented failure behavior), so no queue-vs-queue order exists.
 *   Fanout locks all targets in creation order before mutating. The atomic
 *   cache-plus-message transaction protocol keeps the metadata lock across
 *   its prepare/marker/commit window by design. */
struct QueueStore {
    pthread_mutex_t lock;         /* metadata */
    pthread_mutex_t wal_lock;     /* WAL writes + group fsync state */
    pthread_cond_t sync_cond;
    Queue *queues;
    Exchange *exchanges;
    QueueConsumer *consumers;
    QueueTx *tx_pending;      /* prepared-but-unmaterialized transactions */
    uint64_t next_id;
    uint64_t next_owner;
    uint64_t next_delivery_tag;
    uint64_t redeliveries;
    uint64_t deadlettered;
    uint64_t unroutable;
    uint64_t wal_seq;         /* durable records appended to the WAL */
    uint64_t synced_seq;      /* high-water mark known durable */
    uint64_t meta_live;       /* metadata-side WAL bytes (exchanges, consumers) */
    int syncer;               /* a group fsync is in flight */
    int log_fd;
    int failed;
    char *path;               /* NULL for in-memory stores */
};

static uint32_t crc_table[256];
static pthread_once_t crc_once = PTHREAD_ONCE_INIT;

static void crc_init(void) {
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t value = i;
        for (int j = 0; j < 8; j++)
            value = (value & 1) ? UINT32_C(0xedb88320) ^ (value >> 1) : value >> 1;
        crc_table[i] = value;
    }
}

static uint32_t crc32(const void *a, size_t alen, const void *b, size_t blen,
                      const void *d, size_t dlen) {
    pthread_once(&crc_once, crc_init);
    uint32_t crc = UINT32_C(0xffffffff);
    const unsigned char *items[3] = {a, b, d};
    size_t lens[3] = {alen, blen, dlen};
    for (int item = 0; item < 3; item++) {
        const unsigned char *p = items[item];
        for (size_t i = 0; i < lens[item]; i++)
            crc = crc_table[(crc ^ p[i]) & 0xff] ^ (crc >> 8);
    }
    return crc ^ UINT32_C(0xffffffff);
}

static void put32(unsigned char *p, uint32_t value) {
    p[0] = (unsigned char)value;
    p[1] = (unsigned char)(value >> 8);
    p[2] = (unsigned char)(value >> 16);
    p[3] = (unsigned char)(value >> 24);
}

static uint32_t get32(const unsigned char *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) |
           ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

static void put64(unsigned char *p, uint64_t value) {
    for (int i = 0; i < 8; i++) p[i] = (unsigned char)(value >> (i * 8));
}

static uint64_t get64(const unsigned char *p) {
    uint64_t value = 0;
    for (int i = 0; i < 8; i++) value |= (uint64_t)p[i] << (i * 8);
    return value;
}

/* Durable expiry and retry timestamps use wall time so they retain their
 * meaning across restart. Visibility leases are process-local timing and
 * use a monotonic clock so wall-clock changes cannot end a lease early. */
static uint64_t now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

static uint64_t monotonic_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

static int write_all(int fd, const void *data, size_t len) {
    const char *p = data;
    while (len) {
        ssize_t n = write(fd, p, len);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return -1;
        p += n;
        len -= (size_t)n;
    }
    return 0;
}

static int read_all(int fd, void *data, size_t len) {
    char *p = data;
    char *start = p;
    while (len) {
        ssize_t n = read(fd, p, len);
        if (n < 0 && errno == EINTR) continue;
        if (n == 0) return p == start ? 0 : -1;
        if (n < 0) return -1;
        p += n;
        len -= (size_t)n;
    }
    return 1;
}

static Queue *find_queue(QueueStore *store, const char *name, uint32_t name_len) {
    for (Queue *queue = store->queues; queue; queue = queue->next)
        if (!queue->deleted && queue->name_len == name_len &&
            memcmp(queue->name, name, name_len) == 0)
            return queue;
    return NULL;
}

static Exchange *find_exchange(QueueStore *store, const char *name,
                               uint32_t name_len) {
    for (Exchange *exchange = store->exchanges; exchange; exchange = exchange->next)
        if (exchange->name_len == name_len &&
            memcmp(exchange->name, name, name_len) == 0)
            return exchange;
    return NULL;
}

static Binding *find_binding(Exchange *exchange, const char *queue,
                             uint32_t queue_len, const char *key,
                             uint32_t key_len) {
    for (Binding *b = exchange->bindings; b; b = b->next)
        if (b->queue_len == queue_len && memcmp(b->queue, queue, queue_len) == 0 &&
            b->key_len == key_len && (key_len == 0 || memcmp(b->key, key, key_len) == 0))
            return b;
    return NULL;
}

static Exchange *create_exchange(QueueStore *store, const char *name,
                                 uint32_t name_len, int durable, unsigned type,
                                 const char *ae_name, uint32_t ae_len) {
    Exchange *exchange = calloc(1, sizeof(*exchange));
    if (!exchange) return NULL;
    exchange->name = malloc(name_len ? name_len : 1);
    if (!exchange->name) { free(exchange); return NULL; }
    memcpy(exchange->name, name, name_len);
    exchange->name_len = name_len;
    exchange->durable = durable != 0;
    exchange->type = type;
    exchange->revision = 1;
    if (ae_len) {
        exchange->ae_name = malloc(ae_len);
        if (!exchange->ae_name) { free(exchange->name); free(exchange); return NULL; }
        memcpy(exchange->ae_name, ae_name, ae_len);
        exchange->ae_len = ae_len;
    }
    exchange->next = store->exchanges;
    store->exchanges = exchange;
    return exchange;
}

static void free_exchange(Exchange *exchange) {
    if (!exchange) return;
    for (Binding *binding = exchange->bindings; binding;) {
        Binding *next = binding->next;
        free(binding->queue);
        free(binding->key);
        free(binding);
        binding = next;
    }
    free(exchange->name);
    free(exchange->ae_name);
    free(exchange);
}

static Queue *create_queue(QueueStore *store, const char *name, uint32_t name_len,
                           int durable, uint64_t max_depth,
                           const char *dlq_name, uint32_t dlq_len,
                           uint32_t max_deliveries) {
    Queue *queue = calloc(1, sizeof(*queue));
    if (!queue) return NULL;
    queue->name = malloc(name_len ? name_len : 1);
    if (!queue->name) { free(queue); return NULL; }
    memcpy(queue->name, name, name_len);
    queue->name_len = name_len;
    queue->durable = durable;
    queue->max_depth = max_depth;
    queue->revision = 1;
    if (dlq_len) {
        queue->dlq_name = malloc(dlq_len);
        if (!queue->dlq_name) { free(queue->name); free(queue); return NULL; }
        memcpy(queue->dlq_name, dlq_name, dlq_len);
        queue->dlq_len = dlq_len;
        queue->max_deliveries = max_deliveries;
    }
    queue->earliest_visibility = UINT64_MAX;
    queue->ready_hint = NULL;
    if (durable)
        queue->live_bytes = LOG_HEADER + name_len + 9 +
                            (dlq_len ? 8 + dlq_len : 0);
    /* Called under the metadata lock: creation order gives fanout a
     * deadlock-free multi-lock acquisition order. */
    static uint64_t next_lock_id;
    queue->lock_id = ++next_lock_id;
    pthread_mutexattr_t qa;
    pthread_mutexattr_init(&qa);
    pthread_mutexattr_settype(&qa, PTHREAD_MUTEX_ERRORCHECK);
    pthread_mutex_init(&queue->lock, &qa);
    pthread_mutexattr_destroy(&qa);
    queue->next = store->queues;
    store->queues = queue;
    return queue;
}

static void free_message(Message *message) { free(message); }

/* Delivery-tag index: delivery tags are unique per store and only ever set
 * on in-flight messages, so a per-queue chained hash keyed by tag resolves
 * ACK/NACK lookups in O(1) and stays proportional to live in-flight state. */
static uint32_t tag_hash(uint64_t tag) {
    tag *= UINT64_C(0x9E3779B97F4A7C15);
    return (uint32_t)(tag >> 32);
}

static Message *tag_find(Queue *queue, uint64_t tag) {
    if (!queue->tag_buckets || !tag) return NULL;
    for (Message *m = queue->tag_buckets[tag_hash(tag) & queue->tag_mask]; m;
         m = m->tag_next)
        if (m->delivery_tag == tag) return m;
    return NULL;
}

static void tag_index_remove(Queue *queue, Message *message) {
    if (!queue->tag_buckets || !message->delivery_tag) return;
    Message **link = &queue->tag_buckets[tag_hash(message->delivery_tag) & queue->tag_mask];
    while (*link && *link != message) link = &(*link)->tag_next;
    if (*link) {
        *link = message->tag_next;
        message->tag_next = NULL;
        if (queue->tag_count) queue->tag_count--;
    }
    message->delivery_tag = 0;
}

static void tag_index_insert(Queue *queue, Message *message) {
    if (!message->delivery_tag) return;
    /* Grow at 100% load; sizes are powers of two. */
    if (!queue->tag_buckets || queue->tag_count > queue->tag_mask) {
        uint32_t size = queue->tag_buckets ? (queue->tag_mask + 1) * 2 : 64;
        Message **buckets = calloc(size, sizeof *buckets);
        if (!buckets) return; /* degrade to list scans, never fail delivery */
        free(queue->tag_buckets);
        queue->tag_buckets = buckets;
        queue->tag_mask = size - 1;
        queue->tag_count = 0;
        for (Message *m = queue->head; m; m = m->next) {
            if (!m->delivery_tag) continue;
            uint32_t slot = tag_hash(m->delivery_tag) & queue->tag_mask;
            m->tag_next = buckets[slot];
            buckets[slot] = m;
            queue->tag_count++;
        }
        return;
    }
    uint32_t slot = tag_hash(message->delivery_tag) & queue->tag_mask;
    message->tag_next = queue->tag_buckets[slot];
    queue->tag_buckets[slot] = message;
    queue->tag_count++;
}

/* Advance the consume scan hint past consecutive in-flight messages; stop
 * at anything that can still become ready (or at the tail). */
static Message *ready_hint_advance(Queue *queue, Message *from) {
    Message *m = from;
    (void)queue;
    while (m && m->state == MESSAGE_INFLIGHT) m = m->next;
    return m;
}

static void remove_message(Queue *queue, Message *message) {
    if (message->delivery_tag) tag_index_remove(queue, message);
    if (queue->ready_hint == message) queue->ready_hint = message->next;
    if (message->prev) message->prev->next = message->next;
    else queue->head = message->next;
    if (message->next) message->next->prev = message->prev;
    else queue->tail = message->prev;
    if (message->state == MESSAGE_INFLIGHT && queue->inflight) queue->inflight--;
    if (message->expires_ms && queue->ttl_count) queue->ttl_count--;
    if (queue->depth) queue->depth--;
    if (queue->durable) queue->live_bytes -= message->wal_footprint;
    queue->revision++;
    free_message(message);
}

static uint64_t purge_queue_locked(Queue *queue) {
    uint64_t removed = queue->depth;
    while (queue->head) remove_message(queue, queue->head);
    queue->ready_hint = NULL;
    queue->earliest_visibility = UINT64_MAX;
    return removed;
}

static int append_log(QueueStore *store, unsigned op, Queue *queue, uint64_t id,
                      uint64_t expires_ms, const void *data, uint32_t len);
static int sync_log(QueueStore *store);
static QueueConsumer *find_consumer(QueueStore *store, const char *name,
                                    uint32_t name_len);
static int append_message(Queue *queue, uint64_t id, uint64_t expires_ms,
                          const void *data, uint32_t len);
static int dead_letter_locked(QueueStore *store, Queue *queue, Message *message,
                              int clear_expiry, int metadata_held);

/* Called with store->lock held. Expiry must run before checking capacity so an
 * expired message cannot hold a bounded queue full until a maintenance tick. */
static void reap_queue_locked(QueueStore *store, Queue *queue, uint64_t now,
                              uint64_t mono_now, int metadata_held);
static void reap_queue_locked(QueueStore *store, Queue *queue, uint64_t now,
                              uint64_t mono_now, int metadata_held) {
    /* Skip the walkk entirely when nothing can expire: no TTL message, and
     * either nothing in flight or the earliest visibility deadline is still
     * ahead. This keeps publish/consume cost independent of queue depth in
     * the common case. */
    if (!queue->ttl_count && (!queue->inflight || mono_now < queue->earliest_visibility))
        return;
    uint64_t next_deadline = UINT64_MAX;
    Message *message = queue->head;
    while (message) {
        Message *next = message->next;
        if (message->expires_ms && message->expires_ms <= now) {
            /* Route expired messages to the dead-letter queue when one is
             * configured. The routed copy loses its expiry so an expired
             * message cannot ping-pong between two dead-letter queues. If
             * routing fails (full DLQ, persistence failure) the message is
             * kept and retried on a later pass. */
            int routed = dead_letter_locked(store, queue, message, 1, metadata_held);
            if (routed < 0) { message = next; continue; }
            if (routed == 0) remove_message(queue, message);
            message = next;
            continue; /* routed > 0 already removed the message */
        }
        if (message->state == MESSAGE_INFLIGHT) {
            if (message->visibility_deadline_ms <= mono_now) {
                tag_index_remove(queue, message); /* clears the delivery tag */
                message->state = MESSAGE_READY;
                message->visibility_deadline_ms = 0;
                message->owner = 0;
                if (queue->inflight) queue->inflight--;
                __atomic_fetch_add(&store->redeliveries, 1, __ATOMIC_RELAXED);
                queue->ready_hint = queue->head; /* requeue may precede hint */
                message = next;
                continue;
            } else if (message->visibility_deadline_ms < next_deadline) {
                next_deadline = message->visibility_deadline_ms;
            }
        }
        message = next;
    }
    queue->earliest_visibility = next_deadline;
}

/* Called with store->lock held. Routes the message at *link to the source
 * queue's dead-letter queue. Returns 1 when routed (the message was removed
 * from the source queue), 0 when no dead-letter policy is configured, and -1
 * on failure (the message is left untouched). Durable ordering: the DLQ
 * publish record is appended before the source removal record, so a crash
 * between the two leaves the message in the source queue for re-routing. */
/* Called with the SOURCE queue lock held. The DLQ resolution needs the
 * metadata lock and the DLQ copy needs the DLQ lock; both are taken with
 * trylock so a busy store or DLQ never deadlocks against another routing in
 * progress — the message simply stays and is retried on a later pass, the
 * same documented behavior as a full or failed DLQ. Returns 1 routed,
 * 0 no policy, -1 failure (keep message), -2 busy (keep message). */
static int dead_letter_locked(QueueStore *store, Queue *queue, Message *message,
                              int clear_expiry, int metadata_held) {
    if (!queue->dlq_len) return 0;
    int held_here = 0;
    if (!metadata_held) {
        if (pthread_mutex_trylock(&store->lock) != 0) return -1;
        held_here = 1;
    }
    Queue *dlq = find_queue(store, queue->dlq_name, queue->dlq_len);
    if (!dlq) {
        Queue temp = {.name = (char *)queue->dlq_name,
                      .name_len = queue->dlq_len, .durable = queue->durable};
        if (queue->durable &&
            append_log(store, LOG_DECLARE, &temp, 0, 0, NULL, 0) < 0) {
            if (held_here) QUNLOCK(&store->lock);
            return -1;
        }
        dlq = create_queue(store, queue->dlq_name, queue->dlq_len,
                           queue->durable, 0, NULL, 0, 0);
        if (!dlq) {
            if (held_here) QUNLOCK(&store->lock);
            return -1;
        }
    }
    if (held_here) QUNLOCK(&store->lock);
    /* The DLQ lock is always a trylock: two queues dead-lettering into each
     * other while holding their own locks could otherwise deadlock. */
    if (pthread_mutex_trylock(&dlq->lock) != 0) return -1;
    if (dlq->max_depth && dlq->depth >= dlq->max_depth) {
        pthread_mutex_unlock(&dlq->lock);
        return -1;
    }
    uint64_t id = __atomic_fetch_add(&store->next_id, 1, __ATOMIC_RELAXED);
    uint64_t expires_ms = clear_expiry ? 0 : message->expires_ms;
    int wrote = 0;
    if (dlq->durable) {
        if (append_log(store, LOG_PUBLISH, dlq, id, expires_ms,
                       message->data, message->len) < 0) {
            pthread_mutex_unlock(&dlq->lock);
            return -1;
        }
        if (append_log(store, LOG_ACK, queue, message->id, 0, NULL, 0) < 0) {
            pthread_mutex_unlock(&dlq->lock);
            return -1;
        }
        wrote = 1;
    }
    /* Contract ordering: both records durable before the source removal and
     * the DLQ copy become visible. One fsync covers both. */
    if (wrote && sync_log(store) < 0) {
        pthread_mutex_unlock(&dlq->lock);
        return -1;
    }
    if (append_message(dlq, id, expires_ms, message->data, message->len) < 0) {
        pthread_mutex_unlock(&dlq->lock);
        return -1;
    }
    pthread_mutex_unlock(&dlq->lock);
    remove_message(queue, message);
    __atomic_fetch_add(&store->deadlettered, 1, __ATOMIC_RELAXED);
    return 1;
}

static int append_record(QueueStore *store, unsigned op, int durable,
                         const char *name, uint32_t name_len, uint64_t id,
                         uint64_t aux, const void *data, uint32_t len,
                         int do_fsync) {
    if (!durable) return 0;
    if (store->log_fd < 0 || __atomic_load_n(&store->failed, __ATOMIC_RELAXED))
        return -1;
    unsigned char header[LOG_HEADER] = {0};
    header[0] = op;
    header[1] = (unsigned char)(durable != 0);
    header[2] = (unsigned char)name_len;
    header[3] = (unsigned char)(name_len >> 8);
    put32(header + 4, len);
    put64(header + 8, id);
    put64(header + 16, aux);
    uint32_t checksum = crc32(header, 24, name, name_len, data, len);
    put32(header + 24, checksum);
    /* The write happens under wal_lock so record byte order equals sequence
     * order and concurrent per-queue operations cannot interleave records.
     * The do_fsync variant (transaction and consumer records) keeps the lock
     * across the fsync; those records acknowledge nothing else. */
    QLOCK(&store->wal_lock);
    if (write_all(store->log_fd, header, sizeof(header)) < 0 ||
        write_all(store->log_fd, name, name_len) < 0 ||
        (len && write_all(store->log_fd, data, len) < 0) ||
        (do_fsync && fsync(store->log_fd) < 0)) {
        __atomic_store_n(&store->failed, 1, __ATOMIC_RELAXED);
        pthread_cond_broadcast(&store->sync_cond);
        QUNLOCK(&store->wal_lock);
        return -1;
    }
    __atomic_fetch_add(&store->wal_seq, 1, __ATOMIC_RELAXED);
    if (do_fsync) {
        /* The fsync covered every record appended so far. */
        __atomic_store_n(&store->synced_seq, __atomic_load_n(&store->wal_seq, __ATOMIC_RELAXED), __ATOMIC_RELAXED);
        pthread_cond_broadcast(&store->sync_cond);
    }
    QUNLOCK(&store->wal_lock);
    return 0;
}

/* Make every record appended so far durable before an acknowledgment. The
 * target sequence is snapshotted under wal_lock, the fsync runs without it,
 * and only the snapshot is published — records written during the fsync stay
 * unsynced until their own round. */
static int sync_log(QueueStore *store) {
    if (store->log_fd < 0) return -1;
    QLOCK(&store->wal_lock);
    uint64_t target = __atomic_load_n(&store->wal_seq, __ATOMIC_RELAXED);
    int done = __atomic_load_n(&store->synced_seq, __ATOMIC_RELAXED) >= target;
    QUNLOCK(&store->wal_lock);
    if (done) return 0;
    int rc = fsync(store->log_fd);
    QLOCK(&store->wal_lock);
    if (rc) {
        __atomic_store_n(&store->failed, 1, __ATOMIC_RELAXED);
        pthread_cond_broadcast(&store->sync_cond);
        QUNLOCK(&store->wal_lock);
        return -1;
    }
    if (target > __atomic_load_n(&store->synced_seq, __ATOMIC_RELAXED))
        __atomic_store_n(&store->synced_seq, target, __ATOMIC_RELAXED);
    pthread_cond_broadcast(&store->sync_cond);
    QUNLOCK(&store->wal_lock);
    return 0;
}

/* Group-fsync durability wait. Called with the store lock held; the lock is
 * released while fsync runs so writers and readers proceed concurrently.
 * One thread at a time fsyncs the current high-water mark, so under
 * concurrency a single fsync completes every writer that arrived during the
 * previous round, and a sole writer fsyncs immediately (low-load latency is
 * unchanged). Because the WAL is a single sequential log, a record's
 * durability implies every earlier record's, so additions may be applied at
 * write time while transform operations (ACK, NACK, dead-letter) keep the
 * contract ordering: their fsync completes before their mutation. */
/* Wait until `target` is durable. Called with the WAL lock held and no
 * queue or metadata lock held; the fsync runs with only the WAL lock
 * released, so concurrent per-queue operations keep writing records and one
 * fsync completes many operations. */
static int q_wait_durable_locked(QueueStore *store, uint64_t target) {
    for (;;) {
        if (__atomic_load_n(&store->failed, __ATOMIC_RELAXED)) return -1;
        if (__atomic_load_n(&store->synced_seq, __ATOMIC_RELAXED) >= target) return 0;
        if (__atomic_load_n(&store->syncer, __ATOMIC_RELAXED)) {
            /* Bounded wait: the predicate is re-checked on every wakeup and
             * on timeout, so even a lost wakeup costs at most 50 ms of
             * latency instead of a stall. */
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_nsec += 50 * 1000000;
            if (ts.tv_nsec >= 1000000000) {
                ts.tv_nsec -= 1000000000;
                ts.tv_sec += 1;
            }
            pthread_cond_timedwait(&store->sync_cond, &store->wal_lock, &ts);
            continue;
        }
        /* Fast path: this thread holds the only outstanding record, so an
         * fsync under the lock is exactly the historical behavior — no
         * unlock/relock round trip, and low-load latency is unchanged. */
        if (__atomic_load_n(&store->wal_seq, __ATOMIC_RELAXED) == target &&
            __atomic_load_n(&store->synced_seq, __ATOMIC_RELAXED) == target - 1) {
            if (fsync(store->log_fd) < 0) {
                __atomic_store_n(&store->failed, 1, __ATOMIC_RELAXED);
                pthread_cond_broadcast(&store->sync_cond);
                return -1;
            }
            __atomic_store_n(&store->synced_seq, target, __ATOMIC_RELAXED);
            pthread_cond_broadcast(&store->sync_cond);
            return 0;
        }
        __atomic_store_n(&store->syncer, 1, __ATOMIC_RELAXED);
        uint64_t covered = __atomic_load_n(&store->wal_seq, __ATOMIC_RELAXED); /* fsync covers everything to date */
        QUNLOCK(&store->wal_lock);
        int rc = fsync(store->log_fd);
        QLOCK(&store->wal_lock);
        __atomic_store_n(&store->syncer, 0, __ATOMIC_RELAXED);
        if (rc) {
            __atomic_store_n(&store->failed, 1, __ATOMIC_RELAXED);
            pthread_cond_broadcast(&store->sync_cond);
            return -1;
        }
        if (covered > __atomic_load_n(&store->synced_seq, __ATOMIC_RELAXED))
            __atomic_store_n(&store->synced_seq, covered, __ATOMIC_RELAXED);
        pthread_cond_broadcast(&store->sync_cond);
    }
}

static int append_log(QueueStore *store, unsigned op, Queue *queue, uint64_t id,
                      uint64_t expires_ms, const void *data, uint32_t len) {
    /* Write only; durability comes from sync_log (transforms), the group
     * fsync wait (additions), or an explicit do_fsync record. */
    return append_record(store, op, queue->durable, queue->name, queue->name_len,
                         id, expires_ms, data, len, 0);
}

static int append_message(Queue *queue, uint64_t id, uint64_t expires_ms,
                          const void *data, uint32_t len) {
    Message *message = malloc(sizeof(*message) + len);
    if (!message) return -1;
    message->next = NULL;
    message->id = id;
    message->delivery_tag = 0;
    message->owner = 0;
    message->expires_ms = expires_ms;
    message->not_before_ms = 0;
    message->visibility_deadline_ms = 0;
    message->len = len;
    message->deliveries = 0;
    message->state = MESSAGE_READY;
    if (len) memcpy(message->data, data, len);
    if (queue->tail) {
        message->prev = queue->tail;
        queue->tail->next = message;
    } else {
        message->prev = NULL;
        queue->head = message;
    }
    queue->tail = message;
    queue->depth++;
    queue->revision++;
    if (expires_ms) queue->ttl_count++;
    if (!queue->ready_hint) queue->ready_hint = message;
    if (queue->durable) {
        message->wal_footprint = LOG_HEADER + queue->name_len + len;
        queue->live_bytes += message->wal_footprint;
    }
    return 0;
}

/* Called with store->lock held: publish one message into one queue. Expiry
 * runs before the capacity check, the durable record is appended (and fsynced)
 * before the in-memory append, and `*out_id` receives the message ID. */
static int publish_one_locked(QueueStore *store, Queue *queue, const void *data,
                              uint32_t len, uint64_t ttl_ms, uint64_t *out_id) {
    uint64_t now = now_ms();
    reap_queue_locked(store, queue, now, monotonic_ms(), 0);
    if (queue->max_depth && queue->depth >= queue->max_depth) return -1;
    uint64_t id = __atomic_fetch_add(&store->next_id, 1, __ATOMIC_RELAXED);
    uint64_t expires_ms = ttl_ms ? now + ttl_ms : 0;
    if (queue->durable &&
        append_log(store, LOG_PUBLISH, queue, id, expires_ms, data, len) < 0)
        return -1;
    if (append_message(queue, id, expires_ms, data, len) < 0) return -1;
    if (out_id) *out_id = id;
    return 0;
}

/* DECLARE log records carry an optional dead-letter policy payload:
 * `[dlq_len:2 LE][dlq_name][max_deliveries:4 LE]`, or no payload at all. */
static int apply_declare(QueueStore *store, const char *name, uint32_t name_len,
                         int durable, uint64_t max_depth,
                         const unsigned char *ext, uint32_t ext_len) {
    Queue *queue = find_queue(store, name, name_len);
    const char *dlq_name = NULL;
    uint32_t dlq_len = 0, max_deliveries = 0;
    if (ext_len) {
        if (ext_len < 6 || ext_len > 6 + QUEUE_NAME_MAX) return -1;
        dlq_len = (uint32_t)ext[0] | ((uint32_t)ext[1] << 8);
        if (!dlq_len || ext_len != 6 + dlq_len) return -1;
        dlq_name = (const char *)ext + 2;
        max_deliveries = (uint32_t)ext[2 + dlq_len] |
                         ((uint32_t)ext[2 + dlq_len + 1] << 8) |
                         ((uint32_t)ext[2 + dlq_len + 2] << 16) |
                         ((uint32_t)ext[2 + dlq_len + 3] << 24);
    }
    if (queue) {
        return queue->durable == durable && queue->dlq_len == dlq_len &&
               (!dlq_len || (queue->max_deliveries == max_deliveries &&
                             memcmp(queue->dlq_name, dlq_name, dlq_len) == 0))
                   ? 0 : -1;
    }
    if (dlq_len && dlq_len == name_len && memcmp(dlq_name, name, name_len) == 0)
        return -1;
    return create_queue(store, name, name_len, durable, max_depth,
                        dlq_name, dlq_len, max_deliveries) ? 0 : -1;
}

static int apply_ack(QueueStore *store, const char *name, uint32_t name_len,
                     uint64_t id) {
    Queue *queue = find_queue(store, name, name_len);
    if (!queue) return -1;
    for (Message *message = queue->head; message; message = message->next) {
        if (message->id == id) { remove_message(queue, message); return 0; }
    }
    return 0; /* ACK replay is idempotent after a compacted/replayed state. */
}

static int apply_delivery(QueueStore *store, const char *name, uint32_t name_len,
                          uint64_t id) {
    Queue *queue = find_queue(store, name, name_len);
    if (!queue) return -1;
    for (Message *message = queue->head; message; message = message->next) {
        if (message->id == id) {
            message->deliveries++;
            return 0;
        }
    }
    return 0; /* Delivery followed an ACK in a compacted/replayed log. */
}

static int apply_requeue(QueueStore *store, const char *name, uint32_t name_len,
                         uint64_t id, uint64_t not_before_ms) {
    Queue *queue = find_queue(store, name, name_len);
    if (!queue) return -1;
    for (Message *message = queue->head; message; message = message->next) {
        if (message->id == id) {
            message->state = MESSAGE_READY;
            message->visibility_deadline_ms = 0;
            message->not_before_ms = not_before_ms;
            message->owner = 0;
            message->delivery_tag = 0;
            return 0;
        }
    }
    return 0; /* Requeue followed an ACK in a compacted/replayed log. */
}

static int apply_purge(QueueStore *store, const char *name, uint32_t name_len) {
    Queue *queue = find_queue(store, name, name_len);
    if (!queue) return -1;
    (void)purge_queue_locked(queue);
    return 0;
}

static int apply_delete(QueueStore *store, const char *name, uint32_t name_len) {
    Queue *queue = find_queue(store, name, name_len);
    if (!queue) return 0; /* replay is idempotent after a tombstone */
    (void)purge_queue_locked(queue);
    queue->live_bytes = 0;
    queue->deleted = 1;
    queue->revision++;
    return 0;
}

/* ---- topic pattern matching ----
 *
 * Routing keys and patterns are '.'-separated words; every string has at
 * least one (possibly empty) word, so "" is one empty word and "a." is
 * ["a", ""]. A pattern word '*' matches exactly one word, '#' matches zero
 * or more words, anything else must be byte-equal. The matcher is a
 * bottom-up DP over pattern words (a '#' never re-scans earlier words), so
 * matching is linear in key words and cannot backtrack catastrophically. */

#define TOPIC_WORDS_MAX 128u

static uint32_t topic_word_count(const char *s, uint32_t len) {
    uint32_t words = 1;
    for (uint32_t i = 0; i < len; i++)
        if (s[i] == '.') words++;
    return words;
}

static void topic_word_at(const char *s, uint32_t len, uint32_t index,
                          const char **out, uint32_t *out_len) {
    uint32_t start = 0, word = 0;
    for (uint32_t i = 0; i <= len; i++) {
        if (i == len || s[i] == '.') {
            if (word == index) { *out = s + start; *out_len = i - start; return; }
            word++;
            start = i + 1;
        }
    }
    *out = s;
    *out_len = 0;
}

static int topic_matches(const char *pattern, uint32_t plen,
                         const char *key, uint32_t klen) {
    uint32_t m = topic_word_count(pattern, plen);
    if (m > TOPIC_WORDS_MAX) return 0; /* bind-time validation keeps m <= 128 */
    const char *pw[TOPIC_WORDS_MAX];
    uint32_t pw_len[TOPIC_WORDS_MAX];
    for (uint32_t i = 0; i < m; i++)
        topic_word_at(pattern, plen, i, &pw[i], &pw_len[i]);
    uint32_t n = topic_word_count(key, klen);

    /* dp[i] == 1  <=>  pattern words i.. match key words j.. ; seeded with
     * j == n (no key words left), where only a '#' tail can match. */
    unsigned char dp[TOPIC_WORDS_MAX + 1], cur[TOPIC_WORDS_MAX + 1];
    dp[m] = 1;
    for (uint32_t i = m; i-- > 0;)
        dp[i] = (pw_len[i] == 1 && pw[i][0] == '#') ? dp[i + 1] : 0;
    for (uint32_t j = n; j-- > 0;) {
        const char *kw;
        uint32_t kw_len;
        topic_word_at(key, klen, j, &kw, &kw_len);
        cur[m] = 0;
        for (uint32_t i = m; i-- > 0;) {
            int hash = pw_len[i] == 1 && pw[i][0] == '#';
            int star = pw_len[i] == 1 && pw[i][0] == '*';
            if (hash) cur[i] = cur[i + 1] || dp[i];
            else if (star) cur[i] = dp[i + 1];
            else cur[i] = (pw_len[i] == kw_len && memcmp(pw[i], kw, kw_len) == 0)
                        ? dp[i + 1] : 0;
        }
        memcpy(dp, cur, (size_t)m + 1);
    }
    return dp[0];
}

/* EXDECLARE payload: `[type:1][ae_len:2 LE][ae_name]` (ae_len may be 0). */
static int apply_exdeclare(QueueStore *store, const char *name,
                           uint32_t name_len, int durable,
                           uint64_t revision,
                           const unsigned char *payload, uint32_t plen) {
    if (plen < 1 || plen > 3 + EXCHANGE_NAME_MAX) return -1;
    unsigned type = payload[0];
    if (type > EXCHANGE_TOPIC) return -1;
    uint32_t ae_len = plen >= 3
        ? (uint32_t)payload[1] | ((uint32_t)payload[2] << 8) : 0;
    if ((plen == 1 && ae_len == 0) || (plen >= 3 && plen == 3 + ae_len)) {
        /* shape is valid */
    } else {
        return -1;
    }
    const char *ae_name = ae_len ? (const char *)payload + 3 : NULL;
    Exchange *exchange = find_exchange(store, name, name_len);
    if (exchange) {
        int match = exchange->durable == (durable != 0) &&
                    exchange->type == type &&
                    exchange->ae_len == ae_len &&
                    (!ae_len || memcmp(exchange->ae_name, ae_name, ae_len) == 0);
        return match ? 0 : -1;
    }
    exchange = create_exchange(store, name, name_len, durable, type,
                               ae_name, ae_len);
    if (!exchange) return -1;
    if (revision) exchange->revision = revision;
    return 0;
}

/* Called with store->lock held. Appends a new binding to an exchange. */
static int add_binding_locked(Exchange *exchange, const char *queue,
                              uint32_t queue_len, const char *key,
                              uint32_t key_len) {
    if (exchange->binding_count >= EXCHANGE_BINDINGS_MAX) return -1;
    Binding *b = calloc(1, sizeof(*b));
    if (!b) return -1;
    b->queue = malloc(queue_len);
    b->key = malloc(key_len ? key_len : 1);
    if (!b->queue || !b->key) {
        free(b->queue);
        free(b->key);
        free(b);
        return -1;
    }
    memcpy(b->queue, queue, queue_len);
    b->queue_len = queue_len;
    if (key_len) memcpy(b->key, key, key_len);
    b->key_len = key_len;
    b->next = exchange->bindings;
    exchange->bindings = b;
    exchange->binding_count++;
    return 0;
}

/* EXBIND / EXUNBIND payload:
 * `[queue_len:2 LE][queue][key_len:2 LE][key]`. */
static int apply_exbind(QueueStore *store, const char *name, uint32_t name_len,
                        uint64_t revision, const unsigned char *payload,
                        uint32_t plen, int remove) {
    if (plen < 4 || plen > 4 + QUEUE_NAME_MAX + ROUTING_KEY_MAX) return -1;
    uint32_t queue_len = (uint32_t)payload[0] | ((uint32_t)payload[1] << 8);
    if (!queue_len || queue_len > QUEUE_NAME_MAX) return -1;
    uint32_t key_off = 2 + queue_len;
    if (plen < key_off + 2) return -1;
    uint32_t key_len = (uint32_t)payload[key_off] | ((uint32_t)payload[key_off + 1] << 8);
    if (key_len > ROUTING_KEY_MAX || plen != key_off + 2 + key_len) return -1;
    Exchange *exchange = find_exchange(store, name, name_len);
    if (!exchange) return -1;
    const char *queue = (const char *)payload + 2;
    const char *key = (const char *)payload + key_off + 2;
    Binding *existing = find_binding(exchange, queue, queue_len, key, key_len);
    if (remove) {
        if (!existing) return 0; /* unbind replay is idempotent */
        for (Binding **link = &exchange->bindings; *link; link = &(*link)->next) {
            if (*link == existing) {
                *link = existing->next;
                free(existing->queue);
                free(existing->key);
                free(existing);
                exchange->binding_count--;
                exchange->revision = revision ? revision : exchange->revision + 1;
                return 0;
            }
        }
        return 0;
    }
    if (existing) return 0; /* bind replay is idempotent */
    if (add_binding_locked(exchange, queue, queue_len, key, key_len)) return -1;
    exchange->revision = revision ? revision : exchange->revision + 1;
    return 0;
}

static int apply_exdelete(QueueStore *store, const char *name, uint32_t name_len) {
    for (Exchange **link = &store->exchanges; *link; link = &(*link)->next) {
        Exchange *exchange = *link;
        if (exchange->name_len != name_len || memcmp(exchange->name, name, name_len))
            continue;
        *link = exchange->next;
        free_exchange(exchange);
        return 0;
    }
    return 0;
}

/* EXUPDATE payload: `[alternate_len:2 LE][alternate]`. */
static int apply_exupdate(QueueStore *store, const char *name, uint32_t name_len,
                          uint64_t revision, const unsigned char *payload,
                          uint32_t plen) {
    if (plen < 2 || plen > 2 + EXCHANGE_NAME_MAX) return -1;
    uint32_t alternate_len = (uint32_t)payload[0] | ((uint32_t)payload[1] << 8);
    if (!alternate_len || plen != 2 + alternate_len || alternate_len > EXCHANGE_NAME_MAX)
        return -1;
    Exchange *exchange = find_exchange(store, name, name_len);
    if (!exchange || (alternate_len == name_len &&
                      !memcmp(payload + 2, name, name_len))) return -1;
    char *alternate = malloc(alternate_len);
    if (!alternate) return -1;
    memcpy(alternate, payload + 2, alternate_len);
    free(exchange->ae_name);
    exchange->ae_name = alternate;
    exchange->ae_len = alternate_len;
    exchange->revision = revision ? revision : exchange->revision + 1;
    return 0;
}

/* ---- atomic transactions ----
 *
 * TX_PREPARE payload (name = addressed exchange/queue):
 * `[tx_id:8][msg_len:4][expires_ms:8][target_count:2]`
 *   then target_count x `[qname_len:2][qname][msg_id:8]`
 *   then the message bytes. TX_COMMIT payload: `[tx_id:8]`. */

static void tx_free(QueueTx *tx) {
    if (!tx) return;
    for (uint32_t i = 0; i < tx->target_count; i++) free(tx->targets[i].name);
    free(tx->targets);
    free(tx->source);
    free(tx->data);
    free(tx);
}

/* Materializes a transaction's reserved messages into their queues. Called
 * with the store lock held at runtime, or single-threaded during replay. */
static void tx_materialize_locked(QueueStore *store, QueueTx *tx) {
    /* Metadata lock is held; each target queue's lock is taken in turn
     * (lock order metadata -> queue). */
    for (uint32_t i = 0; i < tx->target_count; i++) {
        QueueTxTarget *target = &tx->targets[i];
        Queue *queue = target->queue
            ? target->queue
            : find_queue(store, target->name, target->name_len);
        /* A missing queue can only be a vanished non-durable target; the
         * reserved copy cannot be delivered and is dropped with it. */
        if (queue) {
            QLOCK(&queue->lock);
            append_message(queue, target->msg_id, tx->expires_ms,
                           tx->data, tx->len);
            QUNLOCK(&queue->lock);
        }
    }
}

static int apply_tx_prepare(QueueStore *store, const char *name,
                            uint32_t name_len, const unsigned char *payload,
                            uint32_t plen) {
    if (plen < 22 || !name || !name_len) return -1;
    uint64_t tx_id = get64(payload);
    uint32_t msg_len = get32(payload + 8);
    uint64_t expires_ms = get64(payload + 12);
    uint32_t count = (uint32_t)payload[20] | ((uint32_t)payload[21] << 8);
    if (!count || count > EXCHANGE_BINDINGS_MAX || msg_len > QUEUE_MESSAGE_MAX)
        return -1;
    size_t off = 22;
    QueueTx *tx = calloc(1, sizeof(*tx));
    if (!tx) return -1;
    tx->targets = calloc(count, sizeof(*tx->targets));
    if (!tx->targets) { tx_free(tx); return -1; }
    for (uint32_t i = 0; i < count; i++) {
        if (off + 2 > plen) goto corrupt;
        uint32_t qlen = (uint32_t)payload[off] | ((uint32_t)payload[off + 1] << 8);
        if (!qlen || qlen > QUEUE_NAME_MAX || off + 10 + qlen > plen)
            goto corrupt;
        uint64_t msg_id = get64(payload + off + 2 + qlen);
        tx->targets[i].name = malloc(qlen);
        if (!tx->targets[i].name) goto corrupt;
        memcpy(tx->targets[i].name, payload + off + 2, qlen);
        tx->targets[i].name_len = qlen;
        tx->targets[i].msg_id = msg_id;
        if (msg_id >= store->next_id) store->next_id = msg_id + 1;
        off += 10 + (size_t)qlen;
    }
    if (off + msg_len != plen) goto corrupt;
    tx->tx_id = tx_id;
    tx->expires_ms = expires_ms;
    tx->source = malloc(name_len);
    tx->data = malloc(msg_len ? msg_len : 1);
    if (!tx->source || !tx->data) goto corrupt;
    memcpy(tx->source, name, name_len);
    tx->source_len = name_len;
    if (msg_len) memcpy(tx->data, payload + off, msg_len);
    tx->len = msg_len;
    tx->target_count = count;
    tx->next = store->tx_pending;
    store->tx_pending = tx;
    return 0;
corrupt:
    tx_free(tx);
    return -1;
}

static int apply_tx_commit(QueueStore *store, const unsigned char *payload,
                           uint32_t plen) {
    if (plen != 8) return -1;
    uint64_t tx_id = get64(payload);
    for (QueueTx **link = &store->tx_pending; *link; link = &(*link)->next) {
        if ((*link)->tx_id == tx_id) {
            QueueTx *tx = *link;
            *link = tx->next;
            tx_materialize_locked(store, tx);
            tx_free(tx);
            return 0; /* commit replay is idempotent per transaction */
        }
    }
    return 0; /* unknown or already-materialized commit: ignore */
}

static int replay_log(QueueStore *store) {
    off_t good = 0;
    for (;;) {
        off_t start = lseek(store->log_fd, 0, SEEK_CUR);
        if (start < 0) return -1;
        good = start;
        unsigned char header[LOG_HEADER];
        int rc = read_all(store->log_fd, header, sizeof(header));
        if (rc == 0) return 0;
        if (rc < 0) goto corrupt;
        unsigned op = header[0];
        uint32_t name_len = (uint32_t)header[2] | ((uint32_t)header[3] << 8);
        uint32_t len = get32(header + 4);
        uint64_t id = get64(header + 8);
        uint64_t expires_ms = get64(header + 16);
        uint32_t expected = get32(header + 24);
        if ((op != LOG_DECLARE && op != LOG_PUBLISH && op != LOG_ACK &&
             op != LOG_DELIVER && op != LOG_REQUEUE &&
             op != LOG_EXDECLARE && op != LOG_EXBIND && op != LOG_EXUNBIND &&
             op != LOG_TX_PREPARE && op != LOG_TX_COMMIT &&
             op != LOG_CONSUMER && op != LOG_CONSUMER_DEL && op != LOG_PURGE &&
             op != LOG_DELETE && op != LOG_EXDELETE && op != LOG_EXUPDATE) ||
            name_len == 0 || name_len > QUEUE_NAME_MAX || len > QUEUE_MESSAGE_MAX ||
            (op != LOG_PUBLISH && op != LOG_DECLARE && op != LOG_EXDECLARE &&
             op != LOG_EXBIND && op != LOG_EXUNBIND && op != LOG_EXUPDATE && op != LOG_TX_PREPARE &&
             op != LOG_TX_COMMIT && len != 0) ||
            (op == LOG_DECLARE && len > 6 + QUEUE_NAME_MAX) ||
            ((op == LOG_CONSUMER || op == LOG_CONSUMER_DEL) &&
             (op == LOG_CONSUMER ? id == 0 : id != 0))) goto corrupt;
        char name[QUEUE_NAME_MAX];
        void *data = len ? malloc(len) : NULL;
        if ((len && !data) || read_all(store->log_fd, name, name_len) != 1 ||
            (len && read_all(store->log_fd, data, len) != 1)) {
            free(data);
            goto corrupt;
        }
        if (crc32(header, 24, name, name_len, data, len) != expected) {
            free(data);
            goto corrupt;
        }
        int result = 0;
        if (op == LOG_DECLARE)
            result = apply_declare(store, name, name_len, header[1] != 0,
                                   expires_ms, data, len);
        else if (op == LOG_PUBLISH) {
            Queue *queue = find_queue(store, name, name_len);
            result = queue && queue->durable ? append_message(queue, id, expires_ms, data, len) : -1;
            if (id >= store->next_id) store->next_id = id + 1;
        } else if (op == LOG_ACK) {
            result = apply_ack(store, name, name_len, id);
        } else if (op == LOG_DELIVER) {
            result = apply_delivery(store, name, name_len, id);
        } else if (op == LOG_REQUEUE) {
            result = apply_requeue(store, name, name_len, id, expires_ms);
        } else if (op == LOG_PURGE) {
            result = apply_purge(store, name, name_len);
        } else if (op == LOG_DELETE) {
            result = apply_delete(store, name, name_len);
        } else if (op == LOG_EXDECLARE) {
            result = apply_exdeclare(store, name, name_len, header[1] != 0,
                                     expires_ms, data, len);
        } else if (op == LOG_EXBIND) {
            result = apply_exbind(store, name, name_len, expires_ms, data, len, 0);
        } else if (op == LOG_EXUNBIND) {
            result = apply_exbind(store, name, name_len, expires_ms, data, len, 1);
        } else if (op == LOG_EXDELETE) {
            result = apply_exdelete(store, name, name_len);
        } else if (op == LOG_EXUPDATE) {
            result = apply_exupdate(store, name, name_len, expires_ms, data, len);
        } else if (op == LOG_TX_PREPARE) {
            result = apply_tx_prepare(store, name, name_len, data, len);
        } else if (op == LOG_CONSUMER) {
            QueueConsumer *c = find_consumer(store, name, name_len);
            if (!c) {
                c = calloc(1, sizeof(*c));
                if (!c) goto corrupt;
                c->name = malloc(name_len);
                if (!c->name) { free(c); goto corrupt; }
                memcpy(c->name, name, name_len);
                c->name_len = name_len;
                c->owner = id;
                c->next = store->consumers;
                store->consumers = c;
            }
            if (id >= store->next_owner) store->next_owner = id + 1;
            result = 0;
        } else if (op == LOG_CONSUMER_DEL) {
            for (QueueConsumer **link = &store->consumers; *link;) {
                QueueConsumer *c = *link;
                if (c->name_len == name_len &&
                    memcmp(c->name, name, name_len) == 0) {
                    *link = c->next;
                    free(c->name);
                    free(c);
                    break;
                }
                link = &(*link)->next;
            }
            result = 0; /* delete is idempotent across replayed logs */
        } else { /* LOG_TX_COMMIT */
            result = apply_tx_commit(store, data, len);
        }
        free(data);
        if (result < 0) goto corrupt;
        continue;
corrupt:
        if (ftruncate(store->log_fd, good) < 0) return -1;
        if (lseek(store->log_fd, good, SEEK_SET) < 0) return -1;
        return 0;
    }
}

/* A crashed checkpoint leaves a `.checkpoint.` temp behind; the pre-rename
 * WAL was still valid, so the stale temp is safe to delete at startup. The
 * store is single-owner, so no concurrent checkpoint can be in progress. */
static void remove_checkpoint_temps(const char *path) {
    const char *base = strrchr(path, '/');
    size_t bl;
    if (base) { base++; bl = strlen(base); } else { base = path; bl = strlen(path); }
    if (!bl || bl > 400) return;
    char prefix[480];
    memcpy(prefix, base, bl);
    memcpy(prefix + bl, ".checkpoint.", 13);
    size_t dl = (size_t)(base - path);
    char dir[480];
    if (dl == 0) dir[0] = 0;
    else if (dl == 1) { dir[0] = '/'; dir[1] = 0; }
    else { if (dl - 1 > sizeof dir - 1) return; memcpy(dir, path, dl - 1); dir[dl - 1] = 0; }
    DIR *x = opendir(dl ? dir : ".");
    if (!x) return;
    struct dirent *e;
    while ((e = readdir(x)))
        if (!strncmp(e->d_name, prefix, bl + 12)) {
            char full[512];
            int n = snprintf(full, sizeof full, "%s%s%s", dl ? dir : "",
                             (dl && dir[0] && dir[strlen(dir) - 1] != '/') ? "/" : "",
                             e->d_name);
            if (n > 0 && n < (int)sizeof full) unlink(full);
        }
    closedir(x);
}

QueueStore *queue_store_open(const char *path) {
    QueueStore *store = calloc(1, sizeof(*store));
    if (!store) return NULL;
    store->log_fd = -1;
    store->next_id = 1;
    store->next_owner = 1;
    store->next_delivery_tag = 1;
    pthread_mutexattr_t da;
    pthread_mutexattr_init(&da);
    pthread_mutexattr_settype(&da, PTHREAD_MUTEX_ERRORCHECK);
    pthread_mutex_init(&store->lock, &da);
    pthread_mutex_init(&store->wal_lock, &da);
    pthread_mutexattr_destroy(&da);
    pthread_cond_init(&store->sync_cond, NULL);
    if (!path) return store;
    remove_checkpoint_temps(path);
    store->path = strdup(path);
    if (!store->path) {
        pthread_mutex_destroy(&store->lock);
        pthread_mutex_destroy(&store->wal_lock);
        pthread_cond_destroy(&store->sync_cond);
        free(store);
        return NULL;
    }
    store->log_fd = open(path, O_RDWR | O_CREAT | O_APPEND | O_NOFOLLOW, 0600);
    if (store->log_fd < 0 || fchmod(store->log_fd, 0600) < 0) {
        if (store->log_fd >= 0) close(store->log_fd);
        pthread_mutex_destroy(&store->lock);
        pthread_mutex_destroy(&store->wal_lock);
        pthread_cond_destroy(&store->sync_cond);
        free(store->path);
        free(store);
        return NULL;
    }
    if (lseek(store->log_fd, 0, SEEK_SET) < 0 || replay_log(store) < 0 ||
        lseek(store->log_fd, 0, SEEK_END) < 0) {
        queue_store_close(store);
        return NULL;
    }
    return store;
}

void queue_store_close(QueueStore *store) {
    if (!store) return;
    for (QueueConsumer *c = store->consumers; c;) {
        QueueConsumer *next_c = c->next;
        free(c->name);
        free(c);
        c = next_c;
    }
    for (Queue *queue = store->queues; queue;) {
        Queue *next_queue = queue->next;
        Message *message = queue->head;
        while (message) {
            Message *next_message = message->next;
            free_message(message);
            message = next_message;
        }
        free(queue->name);
        free(queue->dlq_name);
        free(queue->tag_buckets);
        pthread_mutex_destroy(&queue->lock);
        free(queue);
        queue = next_queue;
    }
    for (Exchange *exchange = store->exchanges; exchange;) {
        Exchange *next_exchange = exchange->next;
        for (Binding *b = exchange->bindings; b;) {
            Binding *next_binding = b->next;
            free(b->queue);
            free(b->key);
            free(b);
            b = next_binding;
        }
        free(exchange->name);
        free(exchange->ae_name);
        free(exchange);
        exchange = next_exchange;
    }
    for (QueueTx *tx = store->tx_pending; tx;) {
        QueueTx *next_tx = tx->next;
        tx_free(tx);
        tx = next_tx;
    }
    /* No lock here: close requires quiescence (the store is single-owner),
     * and queue_tx_prepare deliberately returns with the lock held across
     * the prepare/commit pair, so a waiting close could self-deadlock. */
    if (store->log_fd >= 0) close(store->log_fd);
    pthread_mutex_destroy(&store->lock);
    pthread_mutex_destroy(&store->wal_lock);
    pthread_cond_destroy(&store->sync_cond);
    free(store->path);
    free(store);
}

int queue_declare_ex(QueueStore *store, const char *name, uint32_t name_len,
                     int durable, uint64_t max_depth,
                     const char *dlq_name, uint32_t dlq_len,
                     uint32_t max_deliveries) {
    if (!store || !name || !name_len || name_len > QUEUE_NAME_MAX) return -1;
    if (dlq_len) {
        if (!dlq_name || dlq_len > QUEUE_NAME_MAX) return -1;
        if (dlq_len == name_len && memcmp(dlq_name, name, name_len) == 0)
            return -1;
    } else if (dlq_name || max_deliveries) {
        return -1;
    }
    QLOCK(&store->lock);
    Queue *existing = find_queue(store, name, name_len);
    if (existing) {
        int match = existing->durable == !!durable &&
                    existing->dlq_len == dlq_len &&
                    (!dlq_len || (existing->max_deliveries == max_deliveries &&
                                  memcmp(existing->dlq_name, dlq_name, dlq_len) == 0));
        QUNLOCK(&store->lock);
        return match ? 0 : -1;
    }
    Queue temp = {.name = (char *)name, .name_len = name_len, .durable = !!durable};
    uint64_t before = __atomic_load_n(&store->wal_seq, __ATOMIC_RELAXED);
    unsigned char ext[6 + QUEUE_NAME_MAX];
    uint32_t ext_len = 0;
    if (dlq_len) {
        ext[0] = (unsigned char)dlq_len;
        ext[1] = (unsigned char)(dlq_len >> 8);
        memcpy(ext + 2, dlq_name, dlq_len);
        put32(ext + 2 + dlq_len, max_deliveries);
        ext_len = 6 + dlq_len;
    }
    if (durable && append_log(store, LOG_DECLARE, &temp, 0, max_depth,
                              ext_len ? ext : NULL, ext_len) < 0) {
        QUNLOCK(&store->lock);
        return -1;
    }
    int created = create_queue(store, name, name_len, durable, max_depth,
                               dlq_name, dlq_len, max_deliveries) ? 0 : -1;
    int waited = __atomic_load_n(&store->wal_seq, __ATOMIC_RELAXED) != before;
    QUNLOCK(&store->lock);
    int rc = created;
    if (!rc && waited) {
        QLOCK(&store->wal_lock);
        rc = q_wait_durable_locked(store, __atomic_load_n(&store->wal_seq, __ATOMIC_RELAXED));
        QUNLOCK(&store->wal_lock);
    }
    return rc;
}

int queue_declare(QueueStore *store, const char *name, uint32_t name_len,
                  int durable, uint64_t max_depth) {
    return queue_declare_ex(store, name, name_len, durable, max_depth,
                            NULL, 0, 0);
}

int queue_publish(QueueStore *store, const char *name, uint32_t name_len,
                  const void *data, uint32_t len, uint64_t ttl_ms,
                  uint64_t *out_id) {
    if (!store || !name || !name_len || name_len > QUEUE_NAME_MAX ||
        len > QUEUE_MESSAGE_MAX || (len && !data)) return -1;
    QLOCK(&store->lock);   /* metadata: resolve the queue */
    Queue *queue = find_queue(store, name, name_len);
    QUNLOCK(&store->lock);
    if (!queue) return -1;
    QLOCK(&queue->lock);                /* message state */
    if (queue->deleted) { QUNLOCK(&queue->lock); return -1; }
    uint64_t before = __atomic_load_n(&store->wal_seq, __ATOMIC_RELAXED);
    int rc = publish_one_locked(store, queue, data, len, ttl_ms, out_id);
    QUNLOCK(&queue->lock);
    /* Only a publish that wrote a durable record waits for the fsync; a
     * non-durable queue must keep working even when the WAL has failed. No
     * queue or metadata lock is held during the durability wait. */
    if (!rc && __atomic_load_n(&store->wal_seq, __ATOMIC_RELAXED) != before) {
        QLOCK(&store->wal_lock);
        rc = q_wait_durable_locked(store, __atomic_load_n(&store->wal_seq, __ATOMIC_RELAXED));
        QUNLOCK(&store->wal_lock);
    }
    return rc;
}

static int queue_purge_checked(QueueStore *store, const char *name, uint32_t name_len,
                               uint64_t expected_revision, int conditional,
                               uint64_t *out_removed) {
    if (!store || !name || !name_len || name_len > QUEUE_NAME_MAX) return -1;
    if (out_removed) *out_removed = 0;
    QLOCK(&store->lock);
    Queue *queue = find_queue(store, name, name_len);
    QUNLOCK(&store->lock);
    if (!queue) return 0;
    QLOCK(&queue->lock);
    if (queue->deleted) { QUNLOCK(&queue->lock); return 0; }
    if (conditional && queue->revision != expected_revision) {
        QUNLOCK(&queue->lock);
        return 2;
    }
    uint64_t removed = queue->depth;
    uint64_t before = __atomic_load_n(&store->wal_seq, __ATOMIC_RELAXED);
    if (queue->durable && append_log(store, LOG_PURGE, queue, removed, 0, NULL, 0) < 0) {
        QUNLOCK(&queue->lock);
        return -1;
    }
    (void)purge_queue_locked(queue);
    QUNLOCK(&queue->lock);
    if (queue->durable && __atomic_load_n(&store->wal_seq, __ATOMIC_RELAXED) != before) {
        QLOCK(&store->wal_lock);
        if (q_wait_durable_locked(store, __atomic_load_n(&store->wal_seq, __ATOMIC_RELAXED)) < 0) {
            QUNLOCK(&store->wal_lock);
            return -1;
        }
        QUNLOCK(&store->wal_lock);
    }
    if (out_removed) *out_removed = removed;
    return 1;
}

int queue_purge(QueueStore *store, const char *name, uint32_t name_len,
                uint64_t *out_removed) {
    return queue_purge_checked(store,name,name_len,0,0,out_removed);
}

int queue_purge_if_revision(QueueStore *store, const char *name, uint32_t name_len,
                            uint64_t expected_revision, uint64_t *out_removed) {
    return queue_purge_checked(store,name,name_len,expected_revision,1,out_removed);
}

int queue_delete_if_revision(QueueStore *store, const char *name, uint32_t name_len,
                             uint64_t expected_revision, uint64_t *out_removed) {
    if (!store || !name || !name_len || name_len > QUEUE_NAME_MAX) return -1;
    if (out_removed) *out_removed = 0;
    /* Retain the metadata lock through the Queue lock.  This prevents a
     * route or transaction from resolving this Queue while its tombstone is
     * committed, and matches the store -> queue lock order everywhere else. */
    QLOCK(&store->lock);
    Queue *queue = find_queue(store, name, name_len);
    if (!queue) { QUNLOCK(&store->lock); return 0; }
    QLOCK(&queue->lock);
    if (queue->revision != expected_revision) {
        QUNLOCK(&queue->lock); QUNLOCK(&store->lock); return 2;
    }
    /* A binding is a durable topology reference.  Refuse rather than leave a
     * stale route or perform a non-atomic series of unbind mutations. */
    for (Exchange *exchange = store->exchanges; exchange; exchange = exchange->next)
        for (Binding *binding = exchange->bindings; binding; binding = binding->next)
            if (binding->queue_len == name_len &&
                memcmp(binding->queue, name, name_len) == 0) {
                QUNLOCK(&queue->lock); QUNLOCK(&store->lock); return 3;
            }
    uint64_t removed = queue->depth;
    uint64_t before = __atomic_load_n(&store->wal_seq, __ATOMIC_RELAXED);
    if (queue->durable && append_log(store, LOG_DELETE, queue, removed, 0, NULL, 0) < 0) {
        QUNLOCK(&queue->lock); QUNLOCK(&store->lock); return -1;
    }
    (void)purge_queue_locked(queue);
    queue->live_bytes = 0;
    queue->deleted = 1;
    queue->revision++;
    QUNLOCK(&queue->lock);
    QUNLOCK(&store->lock);
    if (queue->durable && __atomic_load_n(&store->wal_seq, __ATOMIC_RELAXED) != before) {
        QLOCK(&store->wal_lock);
        int rc = q_wait_durable_locked(store, __atomic_load_n(&store->wal_seq, __ATOMIC_RELAXED));
        QUNLOCK(&store->wal_lock);
        if (rc < 0) return -1;
    }
    if (out_removed) *out_removed = removed;
    return 1;
}

int queue_consume_for_owner(QueueStore *store, const char *name, uint32_t name_len,
                            uint64_t visibility_ms, uint64_t owner, QueueMessage *out) {
    if (!store || !name || !out) return -1;
    memset(out, 0, sizeof(*out));
    QLOCK(&store->lock);            /* metadata: name lookup */
    Queue *queue = find_queue(store, name, name_len);
    QUNLOCK(&store->lock);
    if (!queue) return -1;
    QLOCK(&queue->lock);            /* message state */
    if (queue->deleted) { QUNLOCK(&queue->lock); return -1; }
    uint64_t now = now_ms();
    reap_queue_locked(store, queue, now, monotonic_ms(), 0);
    if (!queue->ready_hint) { QUNLOCK(&queue->lock); return 0; }
    Message *first_delayed = NULL;
    for (Message *message = queue->ready_hint; message;) {
        Message *next = message->next;
        if (message->state == MESSAGE_READY && message->not_before_ms > now &&
            !first_delayed)
            first_delayed = message;
        if (message->state == MESSAGE_READY && message->not_before_ms <= now) {
            if (queue->max_deliveries && queue->dlq_len &&
                message->deliveries >= queue->max_deliveries) {
                /* The delivery limit is exhausted: dead-letter instead of
                 * delivering again. On routing failure skip the message and
                 * keep scanning; it stays ready and is retried later. */
                int routed = dead_letter_locked(store, queue, message, 0, 0);
                if (routed > 0) { message = next; continue; }
                if (routed < 0) { message = next; continue; }
            }
            void *copy = malloc(message->len ? message->len : 1);
            if (!copy) { QUNLOCK(&queue->lock); return -1; }
            if (message->len) memcpy(copy, message->data, message->len);
            if (queue->durable && append_log(store, LOG_DELIVER, queue,
                                             message->id, 0, NULL, 0) < 0) {
                free(copy);
                QUNLOCK(&queue->lock);
                return -1;
            }
            message->state = MESSAGE_INFLIGHT;
            queue->revision++;
            message->visibility_deadline_ms = monotonic_ms() +
                (visibility_ms ? visibility_ms : 30000);
            if (message->visibility_deadline_ms < queue->earliest_visibility)
                queue->earliest_visibility = message->visibility_deadline_ms;
            message->not_before_ms = 0;
            message->deliveries++;
            message->owner = owner;
            message->delivery_tag = __atomic_fetch_add(&store->next_delivery_tag, 1, __ATOMIC_RELAXED);
            if (!message->delivery_tag) message->delivery_tag = __atomic_fetch_add(&store->next_delivery_tag, 1, __ATOMIC_RELAXED);
            tag_index_insert(queue, message);
            queue->inflight++;
            if (queue->durable) {
                queue->live_bytes += LOG_HEADER + queue->name_len;
                message->wal_footprint += LOG_HEADER + queue->name_len;
            }
            queue->ready_hint = first_delayed ? first_delayed
                              : ready_hint_advance(queue, message->next);
            out->id = message->id;
            out->delivery_tag = message->delivery_tag;
            out->data = copy;
            out->len = message->len;
            out->delivery_count = message->deliveries;
            out->redelivered = message->deliveries > 1;
            /* The delivery record joins the group fsync; the handoff is
             * acknowledged only once the record is durable. The wait needs
             * wal_lock and no queue lock, so release the queue lock first:
             * the delivery mutation is additive and already applied. */
            QUNLOCK(&queue->lock);
            if (queue->durable) {
                QLOCK(&store->wal_lock);
                int wrc = q_wait_durable_locked(store,
                    __atomic_load_n(&store->wal_seq, __ATOMIC_RELAXED));
                QUNLOCK(&store->wal_lock);
                if (wrc < 0) { free(copy); return -1; }
            }
            return 1;
        }
        message = next;
    }
    QUNLOCK(&queue->lock);
    return 0;
}

int queue_consume(QueueStore *store, const char *name, uint32_t name_len,
                  uint64_t visibility_ms, QueueMessage *out) {
    return queue_consume_for_owner(store, name, name_len, visibility_ms, 0, out);
}

int queue_ack_for_owner(QueueStore *store, const char *name, uint32_t name_len,
                        uint64_t delivery_tag, uint64_t owner) {
    if (!store || !name || !delivery_tag) return -1;
    pthread_mutex_lock(&store->lock);   /* metadata: resolve */
    Queue *queue = find_queue(store, name, name_len);
    QUNLOCK(&store->lock);
    if (!queue) return -1;
    QLOCK(&queue->lock);
    if (queue->deleted) { QUNLOCK(&queue->lock); return -1; }
    Message *message = tag_find(queue, delivery_tag);
    if (!message || message->state != MESSAGE_INFLIGHT || message->owner != owner) {
        QUNLOCK(&queue->lock);
        return 0;
    }
    if (queue->durable && append_log(store, LOG_ACK, queue, message->id, 0, NULL, 0) < 0) {
        QUNLOCK(&queue->lock);
        return -1;
    }
    /* A durable ACK removes the message only after the ACK record is
     * durable; this fsync is what the acknowledgement point means. */
    if (queue->durable && sync_log(store) < 0) {
        QUNLOCK(&queue->lock);
        return -1;
    }
    remove_message(queue, message);
    QUNLOCK(&queue->lock);
    return 1;
}

int queue_ack(QueueStore *store, const char *name, uint32_t name_len, uint64_t delivery_tag) {
    return queue_ack_for_owner(store, name, name_len, delivery_tag, 0);
}

int queue_nack_for_owner_delay(QueueStore *store, const char *name, uint32_t name_len,
                               uint64_t delivery_tag, uint64_t owner, int requeue,
                               uint64_t delay_ms) {
    if (!store || !name || !delivery_tag) return -1;
    pthread_mutex_lock(&store->lock);   /* metadata: resolve */
    Queue *queue = find_queue(store, name, name_len);
    QUNLOCK(&store->lock);
    if (!queue) return -1;
    QLOCK(&queue->lock);
    if (queue->deleted) { QUNLOCK(&queue->lock); return -1; }
    Message *message = tag_find(queue, delivery_tag);
    if (!message || message->state != MESSAGE_INFLIGHT || message->owner != owner) {
        QUNLOCK(&queue->lock);
        return 0;
    }
    if (!requeue) {
        if (queue->dlq_len) {
            /* Rejected messages go to the dead-letter queue when configured.
             * Failure (full DLQ, persistence failure) keeps the message
             * in-flight and reports an error; nothing is dropped. */
            int routed = dead_letter_locked(store, queue, message, 0, 0);
            QUNLOCK(&queue->lock);
            return routed;
        }
        if (queue->durable &&
            append_log(store, LOG_ACK, queue, message->id, 0, NULL, 0) < 0) {
            QUNLOCK(&queue->lock);
            return -1;
        }
        if (queue->durable && sync_log(store) < 0) {
            QUNLOCK(&queue->lock);
            return -1;
        }
    }
    if (requeue) {
        uint64_t not_before_ms = delay_ms ? now_ms() + delay_ms : 0;
        if (queue->durable && append_log(store, LOG_REQUEUE, queue, message->id,
                                         not_before_ms, NULL, 0) < 0) {
            QUNLOCK(&queue->lock);
            return -1;
        }
        if (queue->durable && sync_log(store) < 0) {
            QUNLOCK(&queue->lock);
            return -1;
        }
        tag_index_remove(queue, message);
        message->state = MESSAGE_READY;
        message->visibility_deadline_ms = 0;
        message->not_before_ms = not_before_ms;
        message->owner = 0;
        if (queue->inflight) queue->inflight--;
        __atomic_fetch_add(&store->redeliveries, 1, __ATOMIC_RELAXED);
        queue->ready_hint = queue->head; /* requeued before hint? rescan */
        if (queue->durable) {
            queue->live_bytes += LOG_HEADER + queue->name_len;
            message->wal_footprint += LOG_HEADER + queue->name_len;
        }
    } else {
        remove_message(queue, message);
    }
    QUNLOCK(&queue->lock);
    return 1;
}

int queue_nack_for_owner(QueueStore *store, const char *name, uint32_t name_len,
                         uint64_t delivery_tag, uint64_t owner, int requeue) {
    return queue_nack_for_owner_delay(store, name, name_len, delivery_tag, owner,
                                      requeue, 0);
}

int queue_nack(QueueStore *store, const char *name, uint32_t name_len,
               uint64_t delivery_tag, int requeue) {
    return queue_nack_for_owner(store, name, name_len, delivery_tag, 0, requeue);
}

/* ---- exchanges and routing ---- */

static int exchange_payload_valid(const char *name, uint32_t name_len,
                                  const char *key, uint32_t key_len) {
    return name && name_len >= 1 && name_len <= EXCHANGE_NAME_MAX &&
           key_len <= ROUTING_KEY_MAX && (key_len == 0 || key);
}

int exchange_declare(QueueStore *store, const char *name, uint32_t name_len,
                     int durable, unsigned type,
                     const char *ae_name, uint32_t ae_len) {
    if (!store || !exchange_payload_valid(name, name_len, NULL, 0) ||
        type > EXCHANGE_TOPIC)
        return -1;
    if (ae_len) {
        if (!ae_name || ae_len > EXCHANGE_NAME_MAX ||
            (ae_len == name_len && memcmp(ae_name, name, name_len) == 0))
            return -1;
    } else if (ae_name) {
        return -1;
    }
    QLOCK(&store->lock);
    Exchange *existing = find_exchange(store, name, name_len);
    if (existing) {
        int match = existing->durable == (durable != 0) &&
                    existing->type == type &&
                    existing->ae_len == ae_len &&
                    (!ae_len || memcmp(existing->ae_name, ae_name, ae_len) == 0);
        QUNLOCK(&store->lock);
        return match ? 0 : -1;
    }
    if (durable) {
        /* Log payload `[type:1][ae_len:2][ae]`; name and durability live in
         * the record header. */
        unsigned char payload[3 + EXCHANGE_NAME_MAX];
        uint32_t plen = 1;
        payload[0] = (unsigned char)type;
        if (ae_len) {
            payload[1] = (unsigned char)ae_len;
            payload[2] = (unsigned char)(ae_len >> 8);
            memcpy(payload + 3, ae_name, ae_len);
            plen = 3 + ae_len;
        }
        if (append_record(store, LOG_EXDECLARE, 1, name, name_len, 0, 1,
                          payload, plen, 1) < 0) {
            QUNLOCK(&store->lock);
            return -1;
        }
    }
    int rc = create_exchange(store, name, name_len, durable, type,
                             ae_name, ae_len) ? 0 : -1;
    QUNLOCK(&store->lock);
    return rc;
}

int exchange_bind(QueueStore *store, const char *ex_name, uint32_t ex_len,
                  const char *queue, uint32_t queue_len,
                  const char *key, uint32_t key_len) {
    if (!store || !exchange_payload_valid(ex_name, ex_len, key, key_len) ||
        !queue || !queue_len || queue_len > QUEUE_NAME_MAX)
        return -1;
    QLOCK(&store->lock);
    Exchange *exchange = find_exchange(store, ex_name, ex_len);
    Queue *target = find_queue(store, queue, queue_len);
    if (!exchange || !target) {
        QUNLOCK(&store->lock);
        return -1; /* exchanges and bound queues must exist */
    }
    if (find_binding(exchange, queue, queue_len, key, key_len)) {
        QUNLOCK(&store->lock);
        return 0; /* re-binding the same (queue, key) pair is idempotent */
    }
    /* A topic pattern longer than the matcher's word budget would never
     * match; reject it at bind time instead of accepting a silent no-op. */
    if (exchange->type == EXCHANGE_TOPIC &&
        topic_word_count(key, key_len) > TOPIC_WORDS_MAX) {
        QUNLOCK(&store->lock);
        return -1;
    }
    uint64_t new_revision = exchange->revision + 1;
    if (exchange->durable) {
        unsigned char payload[4 + QUEUE_NAME_MAX + ROUTING_KEY_MAX];
        payload[0] = (unsigned char)queue_len;
        payload[1] = (unsigned char)(queue_len >> 8);
        memcpy(payload + 2, queue, queue_len);
        payload[2 + queue_len] = (unsigned char)key_len;
        payload[3 + queue_len] = (unsigned char)(key_len >> 8);
        if (key_len) memcpy(payload + 4 + queue_len, key, key_len);
        if (append_record(store, LOG_EXBIND, 1, exchange->name, exchange->name_len,
                          0, new_revision, payload, 4 + queue_len + key_len, 1) < 0) {
            QUNLOCK(&store->lock);
            return -1;
        }
    }
    int rc = add_binding_locked(exchange, queue, queue_len, key, key_len);
    if (!rc) exchange->revision = new_revision;
    QUNLOCK(&store->lock);
    return rc;
}

static int exchange_unbind_checked(QueueStore *store, const char *ex_name,
                                   uint32_t ex_len, const char *queue,
                                   uint32_t queue_len, const char *key,
                                   uint32_t key_len, uint64_t expected_revision,
                                   int conditional) {
    if (!store || !exchange_payload_valid(ex_name, ex_len, key, key_len) ||
        !queue || !queue_len || queue_len > QUEUE_NAME_MAX)
        return -1;
    QLOCK(&store->lock);
    Exchange *exchange = find_exchange(store, ex_name, ex_len);
    if (exchange && conditional && exchange->revision != expected_revision) {
        QUNLOCK(&store->lock);
        return 2;
    }
    Binding *b = exchange ? find_binding(exchange, queue, queue_len, key, key_len)
                          : NULL;
    if (!b) {
        QUNLOCK(&store->lock);
        return 0;
    }
    uint64_t new_revision = exchange->revision + 1;
    if (exchange->durable) {
        unsigned char payload[4 + QUEUE_NAME_MAX + ROUTING_KEY_MAX];
        payload[0] = (unsigned char)queue_len;
        payload[1] = (unsigned char)(queue_len >> 8);
        memcpy(payload + 2, queue, queue_len);
        payload[2 + queue_len] = (unsigned char)key_len;
        payload[3 + queue_len] = (unsigned char)(key_len >> 8);
        if (key_len) memcpy(payload + 4 + queue_len, key, key_len);
        if (append_record(store, LOG_EXUNBIND, 1, exchange->name, exchange->name_len,
                          0, new_revision, payload, 4 + queue_len + key_len, 1) < 0) {
            QUNLOCK(&store->lock);
            return -1; /* the binding stays until the removal is durable */
        }
    }
    for (Binding **link = &exchange->bindings; *link; link = &(*link)->next) {
        if (*link == b) {
            *link = b->next;
            free(b->queue);
            free(b->key);
            free(b);
            exchange->binding_count--;
            exchange->revision = new_revision;
            break;
        }
    }
    QUNLOCK(&store->lock);
    return 1;
}

int exchange_unbind(QueueStore *store, const char *ex_name, uint32_t ex_len,
                    const char *queue, uint32_t queue_len,
                    const char *key, uint32_t key_len) {
    return exchange_unbind_checked(store, ex_name, ex_len, queue, queue_len,
                                   key, key_len, 0, 0);
}

int exchange_unbind_if_revision(QueueStore *store, const char *ex_name,
                                uint32_t ex_len, const char *queue,
                                uint32_t queue_len, const char *key,
                                uint32_t key_len, uint64_t expected_revision) {
    return exchange_unbind_checked(store, ex_name, ex_len, queue, queue_len,
                                   key, key_len, expected_revision, 1);
}

int exchange_revision(QueueStore *store, const char *name, uint32_t name_len,
                      uint64_t *out_revision) {
    if (!store || !name || !name_len || !out_revision) return -1;
    QLOCK(&store->lock);
    Exchange *exchange = find_exchange(store, name, name_len);
    if (exchange) *out_revision = exchange->revision;
    QUNLOCK(&store->lock);
    return exchange ? 1 : 0;
}

int exchange_delete_if_revision(QueueStore *store, const char *name,
                                uint32_t name_len, uint64_t expected_revision) {
    if (!store || !exchange_payload_valid(name, name_len, NULL, 0)) return -1;
    QLOCK(&store->lock);
    Exchange *exchange = find_exchange(store, name, name_len);
    if (!exchange) { QUNLOCK(&store->lock); return 0; }
    if (exchange->revision != expected_revision) { QUNLOCK(&store->lock); return 2; }
    if (exchange->binding_count) { QUNLOCK(&store->lock); return 3; }
    for (Exchange *candidate = store->exchanges; candidate; candidate = candidate->next)
        if (candidate != exchange && candidate->ae_len == name_len &&
            !memcmp(candidate->ae_name, name, name_len)) {
            QUNLOCK(&store->lock);
            return 4;
        }
    if (exchange->durable && append_record(store, LOG_EXDELETE, 1,
                                            exchange->name, exchange->name_len,
                                            0, 0, NULL, 0, 1) < 0) {
        QUNLOCK(&store->lock);
        return -1;
    }
    for (Exchange **link = &store->exchanges; *link; link = &(*link)->next)
        if (*link == exchange) {
            *link = exchange->next;
            free_exchange(exchange);
            QUNLOCK(&store->lock);
            return 1;
        }
    QUNLOCK(&store->lock);
    return -1;
}

int exchange_set_alternate_if_revision(QueueStore *store, const char *name,
                                       uint32_t name_len, const char *alternate,
                                       uint32_t alternate_len,
                                       uint64_t expected_revision) {
    if (!store || !exchange_payload_valid(name, name_len, NULL, 0) || !alternate ||
        !alternate_len || alternate_len > EXCHANGE_NAME_MAX ||
        (alternate_len == name_len && !memcmp(alternate, name, name_len))) return -1;
    QLOCK(&store->lock);
    Exchange *exchange = find_exchange(store, name, name_len);
    if (!exchange) { QUNLOCK(&store->lock); return 0; }
    if (exchange->revision != expected_revision) { QUNLOCK(&store->lock); return 2; }
    if (exchange->ae_len == alternate_len && !memcmp(exchange->ae_name, alternate, alternate_len)) {
        QUNLOCK(&store->lock);
        return 1;
    }
    uint64_t new_revision = exchange->revision + 1;
    unsigned char payload[2 + EXCHANGE_NAME_MAX];
    payload[0] = (unsigned char)alternate_len;
    payload[1] = (unsigned char)(alternate_len >> 8);
    memcpy(payload + 2, alternate, alternate_len);
    char *replacement = malloc(alternate_len);
    if (!replacement) { QUNLOCK(&store->lock); return -1; }
    memcpy(replacement, alternate, alternate_len);
    if (exchange->durable && append_record(store, LOG_EXUPDATE, 1,
                                            exchange->name, exchange->name_len,
                                            0, new_revision, payload,
                                            2 + alternate_len, 1) < 0) {
        free(replacement);
        QUNLOCK(&store->lock);
        return -1;
    }
    free(exchange->ae_name);
    exchange->ae_name = replacement;
    exchange->ae_len = alternate_len;
    exchange->revision = new_revision;
    QUNLOCK(&store->lock);
    return 1;
}

/* Route resolution: read-only under the metadata lock. Returns the number
 * of deduplicated targets (0 = unroutable) or -1 on error. `depth` caps
 * alternate-exchange hops at one so an AE cycle cannot recurse forever. */
static int exchange_resolve_locked(QueueStore *store, const char *ex_name,
                                   uint32_t ex_len, const char *key,
                                   uint32_t key_len, Queue **targets,
                                   uint32_t max_targets, int depth) {
    if (ex_len == 0) {
        /* Default exchange: the routing key names the target queue. */
        Queue *queue = key_len ? find_queue(store, key, key_len) : NULL;
        if (!queue) return 0;
        targets[0] = queue;
        return 1;
    }
    Exchange *exchange = find_exchange(store, ex_name, ex_len);
    if (!exchange) return -1; /* unknown exchange is a client error */

    uint32_t ntargets = 0;
    for (Binding *b = exchange->bindings; b; b = b->next) {
        if (exchange->type == EXCHANGE_DIRECT) {
            if (b->key_len != key_len ||
                (key_len && memcmp(b->key, key, key_len) != 0))
                continue;
        } else if (exchange->type == EXCHANGE_TOPIC) {
            if (!topic_matches(b->key, b->key_len, key, key_len))
                continue;
        }
        Queue *target = find_queue(store, b->queue, b->queue_len);
        if (!target) continue; /* bound non-durable queue not redeclared yet */
        int seen = 0;
        for (uint32_t i = 0; i < ntargets; i++)
            if (targets[i] == target) { seen = 1; break; }
        if (!seen && ntargets < max_targets) targets[ntargets++] = target;
    }
    if (ntargets) return (int)ntargets;
    /* Unroutable: try the alternate exchange once. A missing or itself-
     * unroutable alternate degrades to unroutable. */
    if (depth == 0 && exchange->ae_len &&
        find_exchange(store, exchange->ae_name, exchange->ae_len))
        return exchange_resolve_locked(store, exchange->ae_name, exchange->ae_len,
                                       key, key_len, targets, max_targets,
                                       depth + 1);
    return 0;
}

int exchange_publish(QueueStore *store, const char *ex_name, uint32_t ex_len,
                     const char *key, uint32_t key_len,
                     const void *data, uint32_t len, uint64_t ttl_ms,
                     uint64_t *out_routed) {
    if (!store || !out_routed || ex_len > EXCHANGE_NAME_MAX ||
        (ex_len && !ex_name) || key_len > ROUTING_KEY_MAX ||
        (key_len && !key) || len > QUEUE_MESSAGE_MAX || (len && !data))
        return -1;
    Queue *targets[EXCHANGE_BINDINGS_MAX];
    QLOCK(&store->lock);
    Exchange *source = ex_len ? find_exchange(store, ex_name, ex_len) : NULL;
    int ntargets = exchange_resolve_locked(store, ex_name, ex_len, key,
                                           key_len, targets,
                                           EXCHANGE_BINDINGS_MAX, 0);
    if (source && ntargets >= 0) {
        source->publish_attempt_count++;
        if (ntargets == 0) source->unroutable_count++;
    }
    QUNLOCK(&store->lock);
    if (ntargets < 0) return -1;
    if (ntargets == 0) {
        __atomic_fetch_add(&store->unroutable, 1, __ATOMIC_RELAXED);
        if (out_routed) *out_routed = 0;
        return 0;
    }
    /* Lock every target in creation order, capacity-check all of them, and
     * only then write: the all-targets capacity contract holds against one
     * consistent snapshot and no lock-order cycle is possible. */
    for (uint32_t i = 1; i < (uint32_t)ntargets; i++) {
        Queue *t = targets[i];
        uint32_t j = i;
        while (j > 0 && targets[j - 1]->lock_id > t->lock_id) {
            targets[j] = targets[j - 1];
            j--;
        }
        targets[j] = t;
    }
    for (uint32_t i = 0; i < (uint32_t)ntargets; i++)
        pthread_mutex_lock(&targets[i]->lock);
    uint64_t now = now_ms();
    for (uint32_t i = 0; i < (uint32_t)ntargets; i++) {
        Queue *target = targets[i];
        reap_queue_locked(store, target, now, monotonic_ms(), 0);
        if (target->max_depth && target->depth >= target->max_depth) {
            for (uint32_t j = 0; j < (uint32_t)ntargets; j++)
                pthread_mutex_unlock(&targets[j]->lock);
            return -1;
        }
    }
    uint64_t expires_ms = ttl_ms ? now + ttl_ms : 0;
    uint64_t before = __atomic_load_n(&store->wal_seq, __ATOMIC_RELAXED);
    for (uint32_t i = 0; i < (uint32_t)ntargets; i++) {
        Queue *target = targets[i];
        uint64_t id = __atomic_fetch_add(&store->next_id, 1, __ATOMIC_RELAXED);
        if (target->durable &&
            append_record(store, LOG_PUBLISH, 1, target->name,
                          target->name_len, id, expires_ms, data, len, 0) < 0) {
            for (uint32_t j = 0; j < (uint32_t)ntargets; j++)
                pthread_mutex_unlock(&targets[j]->lock);
            return -1;
        }
        if (append_message(target, id, expires_ms, data, len) < 0) {
            for (uint32_t j = 0; j < (uint32_t)ntargets; j++)
                pthread_mutex_unlock(&targets[j]->lock);
            return -1;
        }
    }
    for (uint32_t i = 0; i < (uint32_t)ntargets; i++)
        pthread_mutex_unlock(&targets[i]->lock);
    int rc = 0;
    if (__atomic_load_n(&store->wal_seq, __ATOMIC_RELAXED) != before) {
        QLOCK(&store->wal_lock);
        rc = q_wait_durable_locked(store, __atomic_load_n(&store->wal_seq, __ATOMIC_RELAXED));
        QUNLOCK(&store->wal_lock);
    }
    if (!rc && out_routed) *out_routed = (uint64_t)ntargets;
    return rc;
}

/* ---- atomic transactions ---- */

int queue_tx_prepare(QueueStore *store, const char *ex_name, uint32_t ex_len,
                     const char *key, uint32_t key_len,
                     const void *data, uint32_t len, uint64_t tx_id,
                     QueueTx **out) {
    if (!store || !out || (!data && len) || len > QUEUE_MESSAGE_MAX ||
        ex_len > EXCHANGE_NAME_MAX || (ex_len && !ex_name) ||
        key_len > ROUTING_KEY_MAX || (key_len && !key))
        return -1;
    QLOCK(&store->lock);
    if (store->log_fd < 0 || __atomic_load_n(&store->failed, __ATOMIC_RELAXED)) {
        /* Atomic operations are a durability feature: without a healthy
         * queue WAL they are refused rather than degraded. */
        QUNLOCK(&store->lock);
        return -1;
    }

    Queue *targets[EXCHANGE_BINDINGS_MAX];
    Exchange *exchange = NULL;
    Queue *enq_target = NULL;
    if (ex_len == 0) {
        /* Enqueue path: the key names the target queue. */
        if (!key_len || key_len > QUEUE_NAME_MAX) {
            QUNLOCK(&store->lock);
            return -1;
        }
        enq_target = find_queue(store, key, key_len);
        if (!enq_target) {
            QUNLOCK(&store->lock);
            return -1;
        }
    } else {
        exchange = find_exchange(store, ex_name, ex_len);
        if (!exchange) {
            QUNLOCK(&store->lock);
            return -1;
        }
    }

    /* Collect the deduplicated durable targets for this delivery. */
    uint32_t ntargets = 0;
    if (ex_len == 0) {
        targets[0] = enq_target;
        ntargets = 1;
    } else {
        for (Binding *b = exchange->bindings; b; b = b->next) {
            if (exchange->type == EXCHANGE_DIRECT) {
                if (b->key_len != key_len ||
                    (key_len && memcmp(b->key, key, key_len) != 0))
                    continue;
            } else if (exchange->type == EXCHANGE_TOPIC) {
                if (!topic_matches(b->key, b->key_len, key, key_len))
                    continue;
            }
            Queue *target = find_queue(store, b->queue, b->queue_len);
            if (!target) continue;
            if (!target->durable) {
                /* Mixed-durability fanout cannot satisfy all-or-nothing
                 * recovery; the operation is refused, not degraded. */
                QUNLOCK(&store->lock);
                return -1;
            }
            int seen = 0;
            for (uint32_t i = 0; i < ntargets; i++)
                if (targets[i] == target) { seen = 1; break; }
            if (!seen && ntargets < EXCHANGE_BINDINGS_MAX) targets[ntargets++] = target;
        }
    }
    if (ntargets == 0) {
        QUNLOCK(&store->lock);
        return 1; /* unroutable publish: nothing reserved */
    }

    /* Capacity pass: expiry first, then fail closed before anything is
     * written. The metadata lock stays held through commit, so the
     * reservation cannot be overtaken by another publisher. */
    uint64_t now = now_ms();
    for (uint32_t i = 0; i < ntargets; i++) {
        Queue *target = targets[i];
        reap_queue_locked(store, target, now, monotonic_ms(), 1);
        if (target->max_depth && target->depth >= target->max_depth) {
            QUNLOCK(&store->lock);
            return -1;
        }
    }

    QueueTx *tx = calloc(1, sizeof(*tx));
    if (!tx) { QUNLOCK(&store->lock); return -1; }
    tx->store = store;
    tx->tx_id = tx_id;
    tx->target_count = ntargets;
    tx->targets = calloc(ntargets, sizeof(*tx->targets));
    tx->source = malloc(ex_len ? ex_len : key_len);
    tx->data = malloc(len ? len : 1);
    if (!tx->targets || !tx->source || !tx->data) goto fail;
    memcpy(tx->source, ex_len ? ex_name : key, ex_len ? ex_len : key_len);
    tx->source_len = ex_len ? ex_len : key_len;
    memcpy(tx->data, data, len);
    tx->len = len;
    size_t payload_len = 22;
    for (uint32_t i = 0; i < ntargets; i++) {
        Queue *target = targets[i];
        tx->targets[i].name = malloc(target->name_len);
        if (!tx->targets[i].name) goto fail;
        memcpy(tx->targets[i].name, target->name, target->name_len);
        tx->targets[i].name_len = target->name_len;
        tx->targets[i].queue = target;
        tx->targets[i].msg_id = __atomic_fetch_add(&store->next_id, 1, __ATOMIC_RELAXED);
        payload_len += 10 + (size_t)target->name_len;
    }
    payload_len += len;
    tx->wal_footprint = (uint32_t)payload_len;
    store->meta_live += payload_len;

    /* Durable prepare record: [tx_id][msg_len][expires][count] targets msg */
    unsigned char *payload = malloc(payload_len);
    if (!payload) goto fail;
    put64(payload, tx_id);
    put32(payload + 8, len);
    put64(payload + 12, 0); /* message expiry is a later protocol addition */
    payload[20] = (unsigned char)ntargets;
    payload[21] = (unsigned char)(ntargets >> 8);
    size_t off = 22;
    for (uint32_t i = 0; i < ntargets; i++) {
        QueueTxTarget *t = &tx->targets[i];
        payload[off] = (unsigned char)t->name_len;
        payload[off + 1] = (unsigned char)(t->name_len >> 8);
        memcpy(payload + off + 2, t->name, t->name_len);
        put64(payload + off + 2 + t->name_len, t->msg_id);
        off += 10 + (size_t)t->name_len;
    }
    memcpy(payload + off, data, len);
    int rc = append_record(store, LOG_TX_PREPARE, 1, tx->source, tx->source_len,
                           0, 0, payload, (uint32_t)payload_len, 1);
    free(payload);
    if (rc < 0) goto fail;
    *out = tx;
    return 0; /* store lock intentionally left held for queue_tx_commit */

fail:
    QUNLOCK(&store->lock);
    tx_free(tx);
    return -1;
}

int queue_tx_commit(QueueTx *tx, uint64_t *out_routed) {
    if (!tx || !tx->store) return -1;
    QueueStore *store = tx->store; /* store lock is held */
    unsigned char payload[8];
    put64(payload, tx->tx_id);
    if (append_record(store, LOG_TX_COMMIT, 1, tx->source, tx->source_len,
                      0, 0, payload, 8, 1) < 0) {
        /* The cache commit already exists, so recovery will reconcile this
         * transaction from its prepare record. */
        QUNLOCK(&store->lock);
        tx_free(tx);
        return -1;
    }
    tx_materialize_locked(store, tx);
    uint64_t routed = tx->target_count;
    QUNLOCK(&store->lock);
    if (out_routed) *out_routed = routed;
    tx_free(tx);
    return 0;
}

void queue_tx_abort(QueueTx *tx) {
    if (!tx || !tx->store) return;
    pthread_mutex_unlock(&tx->store->lock);
    tx_free(tx);
}

uint64_t queue_tx_id(const QueueTx *tx) {
    return tx ? tx->tx_id : 0;
}

uint64_t queue_tx_pending_ids(QueueStore *store, uint64_t **out_ids) {
    if (out_ids) *out_ids = NULL;
    if (!store) return 0;
    QLOCK(&store->lock);
    uint64_t count = 0;
    for (QueueTx *tx = store->tx_pending; tx; tx = tx->next) count++;
    if (count) {
        uint64_t *ids = malloc(count * sizeof(*ids));
        if (ids) {
            uint64_t i = 0;
            for (QueueTx *tx = store->tx_pending; tx; tx = tx->next)
                ids[i++] = tx->tx_id;
            *out_ids = ids;
        } else {
            count = 0;
        }
    }
    QUNLOCK(&store->lock);
    return count;
}

int queue_tx_resolve(QueueStore *store, uint64_t tx_id, int committed) {
    if (!store) return -1;
    QLOCK(&store->lock);
    for (QueueTx **link = &store->tx_pending; *link; link = &(*link)->next) {
        if ((*link)->tx_id != tx_id) continue;
        QueueTx *tx = *link;
        *link = tx->next;
        int rc = 1;
        if (committed) {
            unsigned char payload[8];
            put64(payload, tx_id);
            if (append_record(store, LOG_TX_COMMIT, 1, tx->source,
                              tx->source_len, 0, 0, payload, 8, 1) < 0) {
                /* Leave it pending; the next recovery retries. */
                tx->next = store->tx_pending;
                store->tx_pending = tx;
                QUNLOCK(&store->lock);
                return -1;
            }
            tx_materialize_locked(store, tx);
        }
        tx_free(tx);
        QUNLOCK(&store->lock);
        return rc;
    }
    QUNLOCK(&store->lock);
    return 0;
}

int queue_wal_enabled(QueueStore *store) {
    if (!store) return 0;
    QLOCK(&store->lock);
    int enabled = store->log_fd >= 0 && !__atomic_load_n(&store->failed, __ATOMIC_RELAXED);
    QUNLOCK(&store->lock);
    return enabled;
}

/* ---- crash-safe WAL checkpoint ---- */

#define QUEUE_CHECKPOINT_FLOOR (1ull << 20)

/* Emit one record into the checkpoint temp file (same format as
 * append_record; the checkpoint is not part of the live sequence). */
static int ckpt_emit(int fd, unsigned op, int durable, const char *name,
                     uint32_t name_len, uint64_t id, uint64_t aux,
                     const void *data, uint32_t len, uint64_t *bytes,
                     uint64_t *records) {
    unsigned char header[LOG_HEADER] = {0};
    header[0] = op;
    header[1] = (unsigned char)(durable != 0);
    header[2] = (unsigned char)name_len;
    header[3] = (unsigned char)(name_len >> 8);
    put32(header + 4, len);
    put64(header + 8, id);
    put64(header + 16, aux);
    put32(header + 24, crc32(header, 24, name, name_len, data, len));
    if (write_all(fd, header, sizeof(header)) < 0 ||
        write_all(fd, name, name_len) < 0 ||
        (len && write_all(fd, data, len) < 0)) return -1;
    *bytes += LOG_HEADER + name_len + len;
    (*records)++;
    return 0;
}

/* Rewrite the full live state (declarations, exchanges, consumers, pending
 * transactions, retained messages with delivery counts and retry delays)
 * into a synced temp file, atomically publish it, and append to it from then
 * on. Requires the metadata lock and every queue lock (acquired here in
 * creation order), so no durable operation can interleave; the pause is the
 * checkpoint write. An interruption before the rename leaves the old valid
 * WAL; after the rename, the checkpoint is the WAL. */
int queue_checkpoint_maybe(QueueStore *store) {
    if (!store || !store->path || store->log_fd < 0) return 0;
    struct stat st;
    if (fstat(store->log_fd, &st) < 0) return 0;
    uint64_t live_total = 0;
    QLOCK(&store->lock);            /* metadata: stable queue set + counters */
    for (Queue *queue = store->queues; queue; queue = queue->next) {
        QLOCK(&queue->lock);
        if (!queue->deleted) live_total += queue->live_bytes;
    }
    live_total += store->meta_live;
    if ((uint64_t)st.st_size <= live_total * 2 + QUEUE_CHECKPOINT_FLOOR) {
        for (Queue *queue = store->queues; queue; queue = queue->next)
            QUNLOCK(&queue->lock);
        QUNLOCK(&store->lock);
        return 0; /* history is still proportional to live state */
    }

    /* Sort queues into creation order and hold every lock: no durable op
     * can write a record while the snapshot is taken. */
    uint32_t count = 0;
    for (Queue *queue = store->queues; queue; queue = queue->next) count++;
    Queue **sorted = malloc(count * sizeof *sorted);
    if (!sorted) {
        for (Queue *queue = store->queues; queue; queue = queue->next)
            QUNLOCK(&queue->lock);
        QUNLOCK(&store->lock);
        return -1;
    }
    uint32_t at = 0;
    for (Queue *queue = store->queues; queue; queue = queue->next) sorted[at++] = queue;
    for (uint32_t i = 1; i < count; i++) {
        Queue *q = sorted[i];
        uint32_t j = i;
        while (j > 0 && sorted[j - 1]->lock_id > q->lock_id) {
            sorted[j] = sorted[j - 1];
            j--;
        }
        sorted[j] = q;
    }

    char tmp[512];
    snprintf(tmp, sizeof tmp, "%s.checkpoint.XXXXXX", store->path);
    int fd = mkstemp(tmp);
    if (fd < 0) {
        for (uint32_t i = 0; i < count; i++) QUNLOCK(&sorted[i]->lock);
        QUNLOCK(&store->lock);
        return -1;
    }
    fchmod(fd, 0600);
    uint64_t bytes = 0, records = 0;
    int rc = 0;
    /* Declarations first (data = DLQ extension only; durable and max_depth
     * travel in the header), then each queue's retained messages. */
    for (uint32_t i = 0; i < count && !rc; i++) {
        Queue *queue = sorted[i];
        if (queue->deleted) continue;
        unsigned char dval[2 + QUEUE_NAME_MAX + 4];
        uint32_t dlen = 0;
        if (queue->dlq_len) {
            dval[0] = (unsigned char)queue->dlq_len;
            dval[1] = (unsigned char)(queue->dlq_len >> 8);
            memcpy(dval + 2, queue->dlq_name, queue->dlq_len);
            put32(dval + 2 + queue->dlq_len, queue->max_deliveries);
            dlen = 2 + queue->dlq_len + 4;
        }
        rc = ckpt_emit(fd, LOG_DECLARE, queue->durable, queue->name,
                       queue->name_len, 0, queue->max_depth,
                       dlen ? dval : NULL, dlen, &bytes, &records);
    }
    for (uint32_t i = 0; i < count && !rc; i++) {
        Queue *queue = sorted[i];
        if (queue->deleted) continue;
        for (Message *m = queue->head; m && !rc; m = m->next) {
            rc = ckpt_emit(fd, LOG_PUBLISH, 1, queue->name, queue->name_len,
                           m->id, m->expires_ms, m->data, m->len, &bytes, &records);
            for (uint32_t d = 0; d < m->deliveries && !rc; d++)
                rc = ckpt_emit(fd, LOG_DELIVER, 1, queue->name, queue->name_len,
                               m->id, 0, NULL, 0, &bytes, &records);
            if (!rc && m->not_before_ms)
                rc = ckpt_emit(fd, LOG_REQUEUE, 1, queue->name, queue->name_len,
                               m->id, m->not_before_ms, NULL, 0, &bytes, &records);
        }
    }
    /* Exchanges, bindings, consumers, pending transactions. */
    for (Exchange *exchange = store->exchanges; exchange && !rc; exchange = exchange->next) {
        unsigned char pval[3 + EXCHANGE_NAME_MAX];
        pval[0] = (unsigned char)exchange->type;
        pval[1] = (unsigned char)(exchange->ae_len & 0xff);
        pval[2] = (unsigned char)(exchange->ae_len >> 8);
        if (exchange->ae_len) memcpy(pval + 3, exchange->ae_name, exchange->ae_len);
        rc = ckpt_emit(fd, LOG_EXDECLARE, exchange->durable, exchange->name,
                       exchange->name_len, 0, exchange->revision, pval, 3 + exchange->ae_len,
                       &bytes, &records);
        for (Binding *b = exchange->bindings; b && !rc; b = b->next) {
            unsigned char bval[4 + QUEUE_NAME_MAX + ROUTING_KEY_MAX];
            bval[0] = (unsigned char)b->queue_len;
            bval[1] = (unsigned char)(b->queue_len >> 8);
            memcpy(bval + 2, b->queue, b->queue_len);
            bval[2 + b->queue_len] = (unsigned char)b->key_len;
            bval[3 + b->queue_len] = (unsigned char)(b->key_len >> 8);
            if (b->key_len) memcpy(bval + 4 + b->queue_len, b->key, b->key_len);
            rc = ckpt_emit(fd, LOG_EXBIND, 1, exchange->name, exchange->name_len,
                           0, exchange->revision, bval, 4 + b->queue_len + b->key_len, &bytes, &records);
        }
    }
    for (QueueConsumer *c = store->consumers; c && !rc; c = c->next)
        rc = ckpt_emit(fd, LOG_CONSUMER, 1, c->name, c->name_len, c->owner,
                       0, NULL, 0, &bytes, &records);
    for (QueueTx *tx = store->tx_pending; tx && !rc; tx = tx->next) {
        size_t plen = 22;
        for (uint32_t i = 0; i < tx->target_count; i++)
            plen += 10 + (size_t)tx->targets[i].name_len;
        plen += tx->len;
        unsigned char *payload = malloc(plen);
        if (!payload) { rc = -1; break; }
        put64(payload, tx->tx_id);
        put32(payload + 8, tx->len);
        put64(payload + 12, tx->expires_ms);
        payload[20] = (unsigned char)tx->target_count;
        payload[21] = (unsigned char)(tx->target_count >> 8);
        size_t off = 22;
        for (uint32_t i = 0; i < tx->target_count; i++) {
            QueueTxTarget *t = &tx->targets[i];
            payload[off] = (unsigned char)t->name_len;
            payload[off + 1] = (unsigned char)(t->name_len >> 8);
            memcpy(payload + off + 2, t->name, t->name_len);
            put64(payload + off + 2 + t->name_len, t->msg_id);
            off += 10 + (size_t)t->name_len;
        }
        memcpy(payload + off, tx->data, tx->len);
        rc = ckpt_emit(fd, LOG_TX_PREPARE, 1, tx->source, tx->source_len, 0, 0,
                       payload, (uint32_t)plen, &bytes, &records);
        free(payload);
    }
    if (!rc && fsync(fd)) { rc = -1; fprintf(stderr, "[ckpt dbg] fsync failed errno=%d\n", errno); }
    if (close(fd)) rc = -1;
    if (!rc && rename(tmp, store->path)) { rc = -1; fprintf(stderr, "[ckpt dbg] rename failed errno=%d\n", errno); }
    if (!rc) {
        /* Publish: fsync the parent directory, swap the append fd, and mark
         * every checkpointed record durable. */
        char dirbuf[512];
        snprintf(dirbuf, sizeof dirbuf, "%s", store->path);
        char *slash = strrchr(dirbuf, '/');
        const char *dir = ".";
        if (slash) { if (slash == dirbuf) slash[1] = 0; else *slash = 0; dir = dirbuf; }
        int dfd = open(dir, O_RDONLY | O_DIRECTORY);
        int prc = 0;
        if (dfd < 0) prc = -1;
        else { prc = fsync(dfd); close(dfd); }
        if (prc) rc = -1;
    }
    if (!rc) {
        close(store->log_fd);
        store->log_fd = open(store->path, O_RDWR | O_APPEND | O_NOFOLLOW, 0600);
        if (store->log_fd < 0) {
                __atomic_store_n(&store->failed, 1, __ATOMIC_RELAXED);
            rc = -1;
        } else {
            /* The WAL now consists of exactly the checkpointed records. */
            __atomic_store_n(&store->wal_seq, records, __ATOMIC_RELAXED);
            __atomic_store_n(&store->synced_seq, records, __ATOMIC_RELAXED);
        }
    }
    if (rc) unlink(tmp);
    for (uint32_t i = 0; i < count; i++) QUNLOCK(&sorted[i]->lock);
    QUNLOCK(&store->lock);
    return rc ? -1 : 1;
}

void queue_reap(QueueStore *store) {
    if (!store) return;
    QLOCK(&store->lock);
    uint64_t now = now_ms();
    uint64_t mono_now = monotonic_ms();
    for (Queue *queue = store->queues; queue; queue = queue->next) {
        if (queue->deleted) continue;
        QLOCK(&queue->lock);
        reap_queue_locked(store, queue, now, mono_now, 1);
        QUNLOCK(&queue->lock);
    }
    QUNLOCK(&store->lock);
}

/* ---- Batch operations (one group fsync per batch) ---- */

int queue_publish_batch(QueueStore *store, const char *name, uint32_t name_len,
                        uint32_t count, const void *const *data,
                        const uint32_t *lens, uint64_t *out_ids) {
    if (!store || !name || !name_len || !count || !data || !lens)
        return -1;
    QLOCK(&store->lock);
    Queue *queue = find_queue(store, name, name_len);
    QUNLOCK(&store->lock);
    if (!queue) return -1;
    QLOCK(&queue->lock);
    if (queue->deleted) { QUNLOCK(&queue->lock); return -1; }
    uint64_t now = now_ms();
    reap_queue_locked(store, queue, now, monotonic_ms(), 0);
    /* Capacity pass first: fail closed before anything is touched. */
    if (queue->max_depth && queue->depth + count > queue->max_depth) {
        QUNLOCK(&queue->lock);
        return -1;
    }
    uint64_t before = __atomic_load_n(&store->wal_seq, __ATOMIC_RELAXED);
    for (uint32_t i = 0; i < count; i++) {
        if (!data[i] || !lens[i] || lens[i] > QUEUE_MESSAGE_MAX) {
            QUNLOCK(&queue->lock);
            return -1;
        }
        uint64_t id = __atomic_fetch_add(&store->next_id, 1, __ATOMIC_RELAXED);
        uint64_t expires_ms = 0; /* batch publish carries no per-message TTL */
        if (queue->durable &&
            append_record(store, LOG_PUBLISH, 1, queue->name, queue->name_len,
                          id, expires_ms, data[i], lens[i], 0) < 0) {
            QUNLOCK(&queue->lock);
            return -1;
        }
        if (append_message(queue, id, expires_ms, data[i], lens[i]) < 0) {
            QUNLOCK(&queue->lock);
            return -1;
        }
        if (out_ids) out_ids[i] = id;
    }
    int rc = 0;
    if (__atomic_load_n(&store->wal_seq, __ATOMIC_RELAXED) != before) {
        QLOCK(&store->wal_lock);
        rc = q_wait_durable_locked(store, __atomic_load_n(&store->wal_seq, __ATOMIC_RELAXED));
        QUNLOCK(&store->wal_lock);
    }
    QUNLOCK(&queue->lock);
    return rc;
}

int queue_consume_batch(QueueStore *store, const char *name, uint32_t name_len,
                        uint64_t visibility_ms, uint64_t owner, uint32_t max,
                        QueueMessage *out, uint32_t *out_count) {
    if (!store || !name || !name_len || !out || !out_count || !max) return -1;
    *out_count = 0;
    QLOCK(&store->lock);
    Queue *queue = find_queue(store, name, name_len);
    QUNLOCK(&store->lock);
    if (!queue) return -1;
    QLOCK(&queue->lock);
    if (queue->deleted) { QUNLOCK(&queue->lock); return -1; }
    uint64_t now = now_ms();
    reap_queue_locked(store, queue, now, monotonic_ms(), 0);
    uint64_t before = __atomic_load_n(&store->wal_seq, __ATOMIC_RELAXED);
    uint32_t n = 0;
    if (!queue->ready_hint) { *out_count = 0; QUNLOCK(&queue->lock); return 0; }
    Message *scan = queue->ready_hint;
    Message *first_delayed = NULL;
    Message *last_delivered = NULL;
    while (scan && n < max) {
        Message *message = scan;
        Message *next = message->next;
        if (message->state == MESSAGE_READY && message->not_before_ms > now &&
            !first_delayed)
            first_delayed = message;
        if (message->state == MESSAGE_READY && message->not_before_ms <= now) {
            if (queue->max_deliveries && queue->dlq_len &&
                message->deliveries >= queue->max_deliveries) {
                int routed = dead_letter_locked(store, queue, message, 0, 0);
                if (routed > 0) { scan = next; continue; }
                if (routed < 0) { scan = next; continue; }
            }
            void *copy = malloc(message->len ? message->len : 1);
            if (!copy) { QUNLOCK(&queue->lock); return -1; }
            if (message->len) memcpy(copy, message->data, message->len);
            if (queue->durable && append_log(store, LOG_DELIVER, queue,
                                             message->id, 0, NULL, 0) < 0) {
                free(copy);
                QUNLOCK(&queue->lock);
                return -1;
            }
            message->state = MESSAGE_INFLIGHT;
            queue->revision++;
            message->visibility_deadline_ms = monotonic_ms() +
                (visibility_ms ? visibility_ms : 30000);
            if (message->visibility_deadline_ms < queue->earliest_visibility)
                queue->earliest_visibility = message->visibility_deadline_ms;
            message->not_before_ms = 0;
            message->deliveries++;
            message->owner = owner;
            message->delivery_tag = __atomic_fetch_add(&store->next_delivery_tag, 1, __ATOMIC_RELAXED);
            if (!message->delivery_tag) message->delivery_tag = __atomic_fetch_add(&store->next_delivery_tag, 1, __ATOMIC_RELAXED);
            tag_index_insert(queue, message);
            queue->inflight++;
            if (queue->durable) {
                queue->live_bytes += LOG_HEADER + queue->name_len;
                message->wal_footprint += LOG_HEADER + queue->name_len;
            }
            last_delivered = message;
            out[n].id = message->id;
            out[n].delivery_tag = message->delivery_tag;
            out[n].data = copy;
            out[n].len = message->len;
            out[n].delivery_count = message->deliveries;
            out[n].redelivered = message->deliveries > 1;
            n++;
        }
        scan = next;
    }
    queue->ready_hint = first_delayed ? first_delayed
                      : ready_hint_advance(queue, last_delivered ? last_delivered->next : queue->ready_hint);
    *out_count = n;
    int rc = 0;
    if (n && queue->durable && __atomic_load_n(&store->wal_seq, __ATOMIC_RELAXED) != before) {
        QLOCK(&store->wal_lock);
        rc = q_wait_durable_locked(store, __atomic_load_n(&store->wal_seq, __ATOMIC_RELAXED));
        QUNLOCK(&store->wal_lock);
    }
    if (rc) {
        /* The deliveries were not acknowledged; hand back nothing. */
        for (uint32_t i = 0; i < n; i++) queue_message_free(&out[i]);
        *out_count = 0;
    }
    QUNLOCK(&queue->lock);
    return rc;
}

int queue_delivery_snapshot(QueueStore *store, const char *name, uint32_t name_len,
                            uint64_t delivery_tag, uint64_t owner, QueueMessage *out) {
    if (!store || !name || !name_len || !delivery_tag || !owner || !out) return -1;
    memset(out,0,sizeof *out);
    QLOCK(&store->lock);
    Queue *queue=find_queue(store,name,name_len);
    QUNLOCK(&store->lock);
    if (!queue) return -1;
    QLOCK(&queue->lock);
    if (queue->deleted) { QUNLOCK(&queue->lock); return 0; }
    Message *message=tag_find(queue,delivery_tag);
    if (!message || message->state!=MESSAGE_INFLIGHT || message->owner!=owner) {
        QUNLOCK(&queue->lock); return 0;
    }
    void *copy=malloc(message->len?message->len:1);
    if (!copy) { QUNLOCK(&queue->lock); return -1; }
    if (message->len) memcpy(copy,message->data,message->len);
    out->id=message->id;out->delivery_tag=message->delivery_tag;out->data=copy;
    out->len=message->len;out->delivery_count=message->deliveries;
    out->redelivered=message->deliveries>1;
    QUNLOCK(&queue->lock);
    return 1;
}

/* Shared machinery for the ACK/NACK batches: pass 1 resolves tags and writes
 * one record per processed tag; the single fsync then covers every record
 * before pass 2 applies the mutations. Tag lookup stays O(depth) per tag
 * until the Phase 4 delivery-tag index lands. */
static int ack_nack_batch_locked(QueueStore *store, Queue *queue,
                                 uint64_t owner, uint64_t owner_fallback,
                                 const uint64_t *tags, uint32_t count,
                                 int requeue, uint32_t *out_acked) {
    uint64_t before = __atomic_load_n(&store->wal_seq, __ATOMIC_RELAXED);
    uint32_t acked = 0;
    int have_fallback = owner_fallback && owner_fallback != owner;
    if (requeue) {
        for (uint32_t i = 0; i < count; i++) {
            Message *message = tag_find(queue, tags[i]);
            if (!message || message->state != MESSAGE_INFLIGHT ||
                (message->owner != owner &&
                 (!have_fallback || message->owner != owner_fallback)))
                continue;
            if (queue->durable &&
                append_log(store, LOG_REQUEUE, queue, message->id, 0, NULL, 0) < 0) {
                QUNLOCK(&store->lock);
                return -1;
            }
            acked++;
        }
        if (queue->durable && acked &&
            __atomic_load_n(&store->wal_seq, __ATOMIC_RELAXED) != before &&
            sync_log(store) < 0) {
            QUNLOCK(&store->lock);
            return -1;
        }
        /* Pass 2: re-resolve each processed tag in order and apply. */
        uint32_t applied = 0;
        for (uint32_t i = 0; i < count && applied < acked; i++) {
            Message *message = tag_find(queue, tags[i]);
            if (!message || message->state != MESSAGE_INFLIGHT ||
                (message->owner != owner &&
                 (!have_fallback || message->owner != owner_fallback)))
                continue;
            tag_index_remove(queue, message);
            message->state = MESSAGE_READY;
            message->visibility_deadline_ms = 0;
            message->not_before_ms = 0;
            message->owner = 0;
            if (queue->inflight) queue->inflight--;
            __atomic_fetch_add(&store->redeliveries, 1, __ATOMIC_RELAXED);
            queue->ready_hint = queue->head; /* requeued before hint? rescan */
            if (queue->durable) {
                queue->live_bytes += LOG_HEADER + queue->name_len;
                message->wal_footprint += LOG_HEADER + queue->name_len;
            }
            applied++;
        }
    } else {
        for (uint32_t i = 0; i < count; i++) {
            Message *message = tag_find(queue, tags[i]);
            if (!message || message->state != MESSAGE_INFLIGHT ||
                (message->owner != owner &&
                 (!have_fallback || message->owner != owner_fallback)))
                continue;
            if (queue->dlq_len) {
                /* Per-message dead-letter routing syncs internally, so the
                 * batch cannot coalesce here. Fail closed on routing errors:
                 * the remaining tags keep their deliveries. */
                int routed = dead_letter_locked(store, queue, message, 0, 0);
                if (routed < 0) { QUNLOCK(&store->lock); return -1; }
                if (routed > 0) acked++;
                continue;
            }
            if (queue->durable &&
                append_log(store, LOG_ACK, queue, message->id, 0, NULL, 0) < 0) {
                QUNLOCK(&store->lock);
                return -1;
            }
            acked++;
        }
        if (queue->durable && acked &&
            __atomic_load_n(&store->wal_seq, __ATOMIC_RELAXED) != before &&
            sync_log(store) < 0) {
            QUNLOCK(&store->lock);
            return -1;
        }
        /* Removal pass: every processed tag's record is durable now. */
        uint32_t applied = 0;
        for (uint32_t i = 0; i < count && applied < acked; i++) {
            Message *message = tag_find(queue, tags[i]);
            if (!message || message->state != MESSAGE_INFLIGHT ||
                (message->owner != owner &&
                 (!have_fallback || message->owner != owner_fallback)))
                continue;
            remove_message(queue, message);
            applied++;
        }
    }
    if (out_acked) *out_acked = acked;
    return 0;
}

int queue_ack_batch(QueueStore *store, const char *name, uint32_t name_len,
                    uint64_t owner, uint64_t owner_fallback,
                    const uint64_t *tags, uint32_t count, uint32_t *out_acked) {
    if (!store || !name || !name_len || !count || !tags) return -1;
    if (out_acked) *out_acked = 0;
    QLOCK(&store->lock);
    Queue *queue = find_queue(store, name, name_len);
    QUNLOCK(&store->lock);
    if (!queue) return -1;
    QLOCK(&queue->lock);
    int rc = ack_nack_batch_locked(store, queue, owner, owner_fallback, tags,
                                   count, 0, out_acked);
    QUNLOCK(&queue->lock);
    return rc;
}

int queue_nack_batch(QueueStore *store, const char *name, uint32_t name_len,
                     uint64_t owner, uint64_t owner_fallback,
                     const uint64_t *tags, uint32_t count, int requeue,
                     uint32_t *out_acked) {
    if (!store || !name || !name_len || !count || !tags) return -1;
    if (out_acked) *out_acked = 0;
    QLOCK(&store->lock);
    Queue *queue = find_queue(store, name, name_len);
    QUNLOCK(&store->lock);
    if (!queue) return -1;
    QLOCK(&queue->lock);
    int rc = ack_nack_batch_locked(store, queue, owner, owner_fallback, tags,
                                   count, requeue ? 1 : 0, out_acked);
    QUNLOCK(&queue->lock);
    return rc;
}

void queue_requeue_owner(QueueStore *store, uint64_t owner) {
    if (!store || !owner) return;
    QLOCK(&store->lock);            /* metadata: walk the stable queue set */
    for (Queue *queue = store->queues; queue; queue = queue->next) {
        QLOCK(&queue->lock);
        for (Message *message = queue->head; message; message = message->next) {
            if (message->state == MESSAGE_INFLIGHT && message->owner == owner) {
                tag_index_remove(queue, message);
                message->state = MESSAGE_READY;
                message->visibility_deadline_ms = 0;
                message->owner = 0;
                if (queue->inflight) queue->inflight--;
                __atomic_fetch_add(&store->redeliveries, 1, __ATOMIC_RELAXED);
                queue->ready_hint = queue->head;
            }
        }
        QUNLOCK(&queue->lock);
    }
    QUNLOCK(&store->lock);
}

/* Durable named consumers. A registration maps a stable name to a stable
 * owner token. Consuming as a named consumer delivers under that token, so a
 * disconnected worker's in-flight deliveries follow their visibility
 * deadlines instead of being requeued immediately, and a reconnect with the
 * same name keeps ownership and prefetch accounting stable. Deliveries stay
 * one-use per process: delivery tags are not valid across a restart. */
static QueueConsumer *find_consumer(QueueStore *store, const char *name,
                                    uint32_t name_len) {
    for (QueueConsumer *c = store->consumers; c; c = c->next)
        if (c->name_len == name_len && memcmp(c->name, name, name_len) == 0)
            return c;
    return NULL;
}

int queue_consumer_register(QueueStore *store, const char *name,
                            uint32_t name_len, uint64_t *out_owner) {
    if (!store || !name || !name_len || name_len > QUEUE_NAME_MAX || !out_owner)
        return -1;
    QLOCK(&store->lock);
    QueueConsumer *c = find_consumer(store, name, name_len);
    if (!c) {
        if (__atomic_load_n(&store->failed, __ATOMIC_RELAXED)) { QUNLOCK(&store->lock); return -1; }
        uint64_t owner = store->next_owner;
        if (store->log_fd >= 0 &&
            append_record(store, LOG_CONSUMER, 1, name, name_len,
                          owner, 0, NULL, 0, 1) < 0) {
            QUNLOCK(&store->lock);
            return -1;
        }
        c = calloc(1, sizeof(*c));
        c->name = malloc(name_len);
        if (!c || !c->name) {
            free(c);
            QUNLOCK(&store->lock);
            return -1;
        }
        memcpy(c->name, name, name_len);
        c->name_len = name_len;
        c->owner = owner;
        c->next = store->consumers;
        store->consumers = c;
        store->next_owner++;
        store->meta_live += LOG_HEADER + name_len;
        if (!store->next_owner) store->next_owner = 1;
    }
    *out_owner = c->owner;
    QUNLOCK(&store->lock);
    return 0;
}

int queue_consumer_unregister(QueueStore *store, const char *name,
                              uint32_t name_len) {
    if (!store || !name || !name_len || name_len > QUEUE_NAME_MAX) return -1;
    QLOCK(&store->lock);
    QueueConsumer *c = find_consumer(store, name, name_len);
    if (!c) { QUNLOCK(&store->lock); return 0; }
    uint64_t owner = c->owner;
    if (store->log_fd >= 0 &&
        append_record(store, LOG_CONSUMER_DEL, 1, name, name_len,
                      0, 0, NULL, 0, 1) < 0) {
        QUNLOCK(&store->lock);
        return -1;
    }
    for (QueueConsumer **link = &store->consumers; *link;) {
        if (*link == c) { *link = c->next; free(c->name); free(c); break; }
        link = &(*link)->next;
    }
    store->meta_live -= LOG_HEADER + name_len;
    QUNLOCK(&store->lock);
    /* Graceful shutdown: release the consumer's in-flight deliveries now. */
    queue_requeue_owner(store, owner);
    return 1;
}

int queue_consume_for_consumer(QueueStore *store, const char *queue,
                               uint32_t qlen, const char *consumer,
                               uint32_t clen, uint64_t visibility_ms,
                               QueueMessage *out) {
    if (!store || !consumer || !clen || clen > QUEUE_NAME_MAX) return -1;
    QLOCK(&store->lock);
    QueueConsumer *c = find_consumer(store, consumer, clen);
    if (!c) { QUNLOCK(&store->lock); return 0; }
    uint64_t owner = c->owner;
    QUNLOCK(&store->lock);
    return queue_consume_for_owner(store, queue, qlen, visibility_ms, owner, out);
}

int queue_consumer_lookup(QueueStore *store, const char *name,
                          uint32_t name_len, uint64_t *out_owner) {
    if (!store || !name || !name_len || !out_owner) return 0;
    QLOCK(&store->lock);
    QueueConsumer *c = find_consumer(store, name, name_len);
    if (c) *out_owner = c->owner;
    QUNLOCK(&store->lock);
    return c != NULL;
}

void queue_consumer_foreach(QueueStore *store, QueueConsumerStatsFn fn,
                            void *ud) {
    if (!store || !fn) return;
    QLOCK(&store->lock);
    for (QueueConsumer *consumer = store->consumers; consumer;
         consumer = consumer->next)
        fn(consumer->name, consumer->name_len, ud);
    QUNLOCK(&store->lock);
}

uint64_t queue_consumer_count(QueueStore *store) {
    if (!store) return 0;
    QLOCK(&store->lock);
    uint64_t n = 0;
    for (QueueConsumer *c = store->consumers; c; c = c->next) n++;
    QUNLOCK(&store->lock);
    return n;
}

void queue_message_free(QueueMessage *message) {
    if (!message) return;
    free(message->data);
    memset(message, 0, sizeof(*message));
}

int queue_peek(QueueStore *store, const char *name, uint32_t name_len,
               unsigned state_mask, uint32_t max, int include_bodies,
               uint64_t body_budget, QueueMessageSnapshot **out,
               uint32_t *out_count) {
    return queue_peek_after(store, name, name_len, 0, state_mask, max,
                            include_bodies, body_budget, out, out_count, NULL);
}

int queue_peek_after(QueueStore *store, const char *name, uint32_t name_len,
                     uint64_t after_id, unsigned state_mask, uint32_t max,
                     int include_bodies, uint64_t body_budget,
                     QueueMessageSnapshot **out, uint32_t *out_count,
                     uint64_t *out_revision) {
    if (!store || !name || !name_len || name_len > QUEUE_NAME_MAX || !max ||
        !out || !out_count || (state_mask & ~QUEUE_PEEK_ALL) || !state_mask)
        return -1;
    *out = NULL;
    *out_count = 0;
    if (out_revision) *out_revision = 0;
    QLOCK(&store->lock);
    Queue *queue = find_queue(store, name, name_len);
    QUNLOCK(&store->lock);
    if (!queue) return 0;

    QueueMessageSnapshot *copy = calloc(max, sizeof(*copy));
    if (!copy) return -1;
    uint64_t remaining = body_budget;
    uint32_t count = 0;
    uint64_t wall_now = now_ms();
    QLOCK(&queue->lock);
    if (queue->deleted) { QUNLOCK(&queue->lock); free(copy); return 0; }
    if (out_revision) *out_revision = queue->revision;
    for (Message *message = queue->head; message && count < max;
         message = message->next) {
        /* Expiry is normally reaped by queue operations, but browsing itself
         * must never mutate or write a WAL record.  Treat an overdue item as
         * no longer retained rather than peeking it. */
        if (message->id <= after_id ||
            (message->expires_ms && message->expires_ms <= wall_now)) continue;
        unsigned state = message->state == MESSAGE_INFLIGHT ? QUEUE_PEEK_INFLIGHT
            : message->not_before_ms > wall_now ? QUEUE_PEEK_DELAYED
            : QUEUE_PEEK_READY;
        if (!(state & state_mask)) continue;
        QueueMessageSnapshot *snapshot = &copy[count++];
        snapshot->id = message->id;
        snapshot->expires_ms = message->expires_ms;
        snapshot->not_before_ms = message->not_before_ms;
        snapshot->visibility_deadline_ms = message->visibility_deadline_ms;
        snapshot->len = message->len;
        snapshot->delivery_count = message->deliveries;
        snapshot->state = state;
        snapshot->redelivered = message->deliveries > 1;
        if (!include_bodies) continue;
        if ((uint64_t)message->len > remaining) {
            snapshot->body_omitted = 1;
            continue;
        }
        snapshot->data = malloc(message->len ? message->len : 1);
        if (!snapshot->data) {
            QUNLOCK(&queue->lock);
            queue_peek_free(copy, count);
            return -1;
        }
        if (message->len) memcpy(snapshot->data, message->data, message->len);
        remaining -= message->len;
    }
    QUNLOCK(&queue->lock);
    *out = copy;
    *out_count = count;
    return 1;
}

void queue_peek_free(QueueMessageSnapshot *messages, uint32_t count) {
    if (!messages) return;
    for (uint32_t i = 0; i < count; i++) free(messages[i].data);
    free(messages);
}

int queue_message_snapshot(QueueStore *store, const char *name,
                           uint32_t name_len, uint64_t message_id,
                           int include_body, uint64_t body_budget,
                           QueueMessageSnapshot *out) {
    if (!store || !name || !name_len || name_len > QUEUE_NAME_MAX ||
        !message_id || !out) return -1;
    memset(out,0,sizeof *out);
    QLOCK(&store->lock);Queue *queue=find_queue(store,name,name_len);QUNLOCK(&store->lock);
    if(!queue)return 0;
    QLOCK(&queue->lock);
    if(queue->deleted){QUNLOCK(&queue->lock);return 0;}
    uint64_t wall_now=now_ms();Message *message=queue->head;
    while(message&&message->id!=message_id)message=message->next;
    if(!message||(message->expires_ms&&message->expires_ms<=wall_now)){QUNLOCK(&queue->lock);return 0;}
    out->id=message->id;out->expires_ms=message->expires_ms;out->not_before_ms=message->not_before_ms;
    out->visibility_deadline_ms=message->visibility_deadline_ms;out->len=message->len;
    out->delivery_count=message->deliveries;out->state=message->state==MESSAGE_INFLIGHT?QUEUE_PEEK_INFLIGHT:message->not_before_ms>wall_now?QUEUE_PEEK_DELAYED:QUEUE_PEEK_READY;out->redelivered=message->deliveries>1;
    if(include_body){if((uint64_t)message->len>body_budget){QUNLOCK(&queue->lock);return-2;}out->data=malloc(message->len?message->len:1);if(!out->data){QUNLOCK(&queue->lock);return-1;}if(message->len)memcpy(out->data,message->data,message->len);}
    QUNLOCK(&queue->lock);return 1;
}

void queue_message_snapshot_free(QueueMessageSnapshot *message){if(!message)return;free(message->data);memset(message,0,sizeof *message);}

uint64_t queue_depth(QueueStore *store, const char *name, uint32_t name_len) {
    if (!store || !name) return 0;
    QLOCK(&store->lock);
    Queue *queue = find_queue(store, name, name_len);
    QUNLOCK(&store->lock);
    if (!queue) return 0;
    QLOCK(&queue->lock);
    if (queue->deleted) { QUNLOCK(&queue->lock); return 0; }
    uint64_t depth = queue->depth;
    QUNLOCK(&queue->lock);
    return depth;
}

uint64_t queue_inflight(QueueStore *store, const char *name, uint32_t name_len) {
    if (!store || !name) return 0;
    QLOCK(&store->lock);
    Queue *queue = find_queue(store, name, name_len);
    QUNLOCK(&store->lock);
    if (!queue) return 0;
    QLOCK(&queue->lock);
    if (queue->deleted) { QUNLOCK(&queue->lock); return 0; }
    uint64_t inflight = queue->inflight;
    QUNLOCK(&queue->lock);
    return inflight;
}

int queue_stats(QueueStore *store, const char *name, uint32_t name_len,
                uint64_t *depth, uint64_t *inflight) {
    if (!store || !name || !depth || !inflight) return -1;
    QLOCK(&store->lock);
    Queue *queue = find_queue(store, name, name_len);
    QUNLOCK(&store->lock);
    if (!queue) return 0;
    QLOCK(&queue->lock);
    if (queue->deleted) { QUNLOCK(&queue->lock); return 0; }
    *depth = queue->depth;
    *inflight = queue->inflight;
    QUNLOCK(&queue->lock);
    return 1;
}

int queue_revision(QueueStore *store, const char *name, uint32_t name_len,
                   uint64_t *out_revision) {
    if (!store || !name || !out_revision) return -1;
    QLOCK(&store->lock);
    Queue *queue = find_queue(store, name, name_len);
    QUNLOCK(&store->lock);
    if (!queue) return 0;
    QLOCK(&queue->lock);
    *out_revision = queue->revision;
    QUNLOCK(&queue->lock);
    return 1;
}

int queue_config_snapshot(QueueStore *store, const char *name, uint32_t name_len,
                          QueueConfigSnapshot *out) {
    if (!store || !name || !name_len || !out) return -1;
    QLOCK(&store->lock);
    Queue *queue = find_queue(store, name, name_len);
    QUNLOCK(&store->lock);
    if (!queue) return 0;
    QLOCK(&queue->lock);
    if (queue->deleted) { QUNLOCK(&queue->lock); return 0; }
    memset(out,0,sizeof *out);out->durable=queue->durable;out->max_depth=queue->max_depth;
    out->max_deliveries=queue->max_deliveries;out->dead_letter_queue_len=queue->dlq_len;
    if(queue->dlq_len)memcpy(out->dead_letter_queue,queue->dlq_name,queue->dlq_len);
    out->revision=queue->revision;QUNLOCK(&queue->lock);return 1;
}

uint64_t queue_owner_inflight(QueueStore *store, uint64_t owner) {
    if (!store || !owner) return 0;
    uint64_t count = 0;
    QLOCK(&store->lock);            /* metadata: walk the stable queue set */
    for (Queue *queue = store->queues; queue; queue = queue->next) {
        QLOCK(&queue->lock);
        for (Message *message = queue->head; message; message = message->next)
            if (message->state == MESSAGE_INFLIGHT && message->owner == owner) count++;
        QUNLOCK(&queue->lock);
    }
    QUNLOCK(&store->lock);
    return count;
}

uint64_t queue_redeliveries(QueueStore *store) {
    if (!store) return 0;
    QLOCK(&store->lock);
    uint64_t value = __atomic_load_n(&store->redeliveries, __ATOMIC_RELAXED);
    QUNLOCK(&store->lock);
    return value;
}

uint64_t queue_deadlettered(QueueStore *store) {
    if (!store) return 0;
    QLOCK(&store->lock);
    uint64_t value = __atomic_load_n(&store->deadlettered, __ATOMIC_RELAXED);
    QUNLOCK(&store->lock);
    return value;
}

uint64_t queue_count(QueueStore *store) {
    if (!store) return 0;
    QLOCK(&store->lock);
    uint64_t count = 0;
    for (Queue *queue = store->queues; queue; queue = queue->next)
        if (!queue->deleted) count++;
    QUNLOCK(&store->lock);
    return count;
}

uint64_t queue_total_depth(QueueStore *store) {
    if (!store) return 0;
    QLOCK(&store->lock);
    uint64_t depth = 0;
    for (Queue *queue = store->queues; queue; queue = queue->next) {
        if (queue->deleted) continue;
        QLOCK(&queue->lock);
        depth += queue->depth;
        QUNLOCK(&queue->lock);
    }
    QUNLOCK(&store->lock);
    return depth;
}

void queue_foreach_stats(QueueStore *store, QueueStatsFn fn, void *ud) {
    if (!store || !fn) return;
    QLOCK(&store->lock);            /* metadata: walk the stable queue set */
    for (Queue *queue = store->queues; queue; queue = queue->next) {
        if (queue->deleted) continue;
        QLOCK(&queue->lock);
        uint64_t depth = queue->depth, inflight = queue->inflight;
        QUNLOCK(&queue->lock);
        fn(queue->name, queue->name_len, depth, inflight, ud);
    }
    QUNLOCK(&store->lock);
}

uint64_t queue_total_inflight(QueueStore *store) {
    if (!store) return 0;
    QLOCK(&store->lock);
    uint64_t inflight = 0;
    for (Queue *queue = store->queues; queue; queue = queue->next) {
        if (queue->deleted) continue;
        QLOCK(&queue->lock);
        inflight += queue->inflight;
        QUNLOCK(&queue->lock);
    }
    QUNLOCK(&store->lock);
    return inflight;
}

int queue_persistence_failed(QueueStore *store) {
    if (!store) return 1;
    QLOCK(&store->lock);
    int failed = __atomic_load_n(&store->failed, __ATOMIC_RELAXED);
    QUNLOCK(&store->lock);
    return failed;
}

uint64_t exchange_count(QueueStore *store) {
    if (!store) return 0;
    QLOCK(&store->lock);
    uint64_t count = 0;
    for (Exchange *exchange = store->exchanges; exchange; exchange = exchange->next)
        count++;
    QUNLOCK(&store->lock);
    return count;
}

uint64_t exchange_binding_count(QueueStore *store) {
    if (!store) return 0;
    QLOCK(&store->lock);
    uint64_t count = 0;
    for (Exchange *exchange = store->exchanges; exchange; exchange = exchange->next)
        count += exchange->binding_count;
    QUNLOCK(&store->lock);
    return count;
}

void exchange_foreach_stats(QueueStore *store, ExchangeStatsFn fn, void *ud) {
    if (!store || !fn) return;
    QLOCK(&store->lock);
    for (Exchange *exchange=store->exchanges;exchange;exchange=exchange->next)
        fn(exchange->name,exchange->name_len,exchange->durable,exchange->type,
           exchange->ae_name,exchange->ae_len,exchange->binding_count,
           exchange->revision,exchange->publish_attempt_count,
           exchange->unroutable_count,ud);
    QUNLOCK(&store->lock);
}

int exchange_foreach_route(QueueStore *store, const char *name,
                           uint32_t name_len, ExchangeRouteFn fn, void *ud,
                           uint32_t *out_count) {
    if (!store || !name || !name_len || !fn) return -1;
    QLOCK(&store->lock); Exchange *exchange=find_exchange(store,name,name_len);
    if (!exchange) { QUNLOCK(&store->lock); return 0; }
    uint32_t count=0; for (Binding *binding=exchange->bindings;binding;binding=binding->next) {
        fn(binding->queue,binding->queue_len,binding->key,binding->key_len,ud); count++;
    }
    if(out_count)*out_count=count; QUNLOCK(&store->lock); return 1;
}

int exchange_route_exists(QueueStore *store, const char *name,
                          uint32_t name_len, const char *queue,
                          uint32_t queue_len, const char *key,
                          uint32_t key_len) {
    if (!store || !name || !name_len || !queue || !queue_len || !key) return -1;
    QLOCK(&store->lock);
    Exchange *exchange=find_exchange(store,name,name_len);
    int found=exchange&&find_binding(exchange,queue,queue_len,key,key_len)!=NULL;
    QUNLOCK(&store->lock);
    return found ? 1 : 0;
}

uint64_t exchange_unroutable(QueueStore *store) {
    if (!store) return 0;
    QLOCK(&store->lock);
    uint64_t value = __atomic_load_n(&store->unroutable, __ATOMIC_RELAXED);
    QUNLOCK(&store->lock);
    return value;
}
