#ifndef KUTTIDB_QUEUE_H
#define KUTTIDB_QUEUE_H

#include <stddef.h>
#include <stdint.h>

#define QUEUE_NAME_MAX 255u
#define EXCHANGE_NAME_MAX 255u
#define ROUTING_KEY_MAX 255u

/* Per-exchange binding cap. It bounds the fanout of a single publish so a
 * misbehaving producer cannot create unbounded work or response buffering. */
#define EXCHANGE_BINDINGS_MAX 1024u

typedef struct QueueStore QueueStore;
typedef struct QueueTx QueueTx;

/* Exchange types. The default exchange is unnamed: publishing with a zero
 * length exchange name routes directly to the queue named by the routing
 * key. Exchanges and queues live in separate namespaces. */
enum { EXCHANGE_DIRECT = 0, EXCHANGE_FANOUT = 1, EXCHANGE_TOPIC = 2 };

typedef struct QueueMessage {
    uint64_t id;           /* durable message ID */
    uint64_t delivery_tag; /* one-use lease token for ACK/NACK */
    void *data;
    uint32_t len;
    uint32_t delivery_count;
    unsigned redelivered : 1;
} QueueMessage;

/* A non-mutating, bounded view of one retained message.  The native delivery
 * tag and owner are deliberately absent: this API is for administrative
 * browsing, never acknowledgement.  `data` is either an owned full copy or
 * NULL when the caller did not request bodies or its bounded body budget was
 * exhausted. */
enum { QUEUE_PEEK_READY = 1u, QUEUE_PEEK_DELAYED = 2u,
       QUEUE_PEEK_INFLIGHT = 4u, QUEUE_PEEK_ALL = 7u };
typedef struct QueueMessageSnapshot {
    uint64_t id;
    uint64_t expires_ms;
    uint64_t not_before_ms;
    uint64_t visibility_deadline_ms;
    void *data;
    uint32_t len;
    uint32_t delivery_count;
    unsigned state;
    unsigned redelivered : 1;
    unsigned body_omitted : 1;
} QueueMessageSnapshot;

/* `path == NULL` creates an in-memory-only store. Durable declarations and
 * mutations return an error if a persistence log cannot be opened or synced. */
QueueStore *queue_store_open(const char *path);
void queue_store_close(QueueStore *store);

/* Declare idempotently. A declaration cannot change the durable mode. */
int queue_declare(QueueStore *store, const char *name, uint32_t name_len,
                  int durable, uint64_t max_depth);
/* As queue_declare, plus an optional dead-letter policy: messages rejected
 * with NACK(requeue=false), expired, or whose delivery count would exceed
 * `max_deliveries` (0 = unlimited) are routed to `dlq_name` instead of being
 * dropped. The DLQ is created on demand with matching durability. Routing is
 * durable before the source removal; if the DLQ is full the source message is
 * kept and the operation fails closed. A queue cannot be its own DLQ. */
int queue_declare_ex(QueueStore *store, const char *name, uint32_t name_len,
                     int durable, uint64_t max_depth,
                     const char *dlq_name, uint32_t dlq_len,
                     uint32_t max_deliveries);
int queue_publish(QueueStore *store, const char *name, uint32_t name_len,
                  const void *data, uint32_t len, uint64_t ttl_ms,
                  uint64_t *out_id);
/* Durably discard every retained message in one Queue, including in-flight
 * deliveries.  The purge record is appended before memory is changed and is
 * replay-idempotent. Returns 1 on success, 0 for an unknown Queue, and -1 on
 * invalid input or persistence failure. */
int queue_purge(QueueStore *store, const char *name, uint32_t name_len,
                uint64_t *out_removed);
/* As queue_purge, but rejects a stale Queue revision while holding the Queue
 * lock. Returns 2 when `expected_revision` is stale. */
int queue_purge_if_revision(QueueStore *store, const char *name, uint32_t name_len,
                            uint64_t expected_revision, uint64_t *out_removed);
/* Tombstone one Queue after its exact revision has been checked.  A deleted
 * Queue is immediately invisible to publishers, consumers, routing, scans,
 * and checkpoints while its allocation remains stable for in-flight native
 * callers.  Returns 1 when deleted, 0 when absent, 2 for a stale revision,
 * 3 when a route still targets it, and -1 for invalid input or WAL failure. */
int queue_delete_if_revision(QueueStore *store, const char *name, uint32_t name_len,
                             uint64_t expected_revision, uint64_t *out_removed);

/* Delivers one ready message and marks it in-flight until ACK/NACK or the
 * visibility deadline.  `out->data` belongs to the caller; use
 * queue_message_free.  Returns 1 on delivery, 0 when no message is ready. */
int queue_consume(QueueStore *store, const char *name, uint32_t name_len,
                  uint64_t visibility_ms, QueueMessage *out);
int queue_consume_for_owner(QueueStore *store, const char *name, uint32_t name_len,
                            uint64_t visibility_ms, uint64_t owner, QueueMessage *out);
/* Copy an active delivery only when both the opaque registry's private tag
 * and its dedicated owner still match.  This is intentionally narrower than
 * queue_peek(): it never scans, changes state, or exposes another owner's
 * in-flight body.  Returns 1 on success, 0 when it is no longer active, or
 * -1 for an unknown Queue or allocation failure. */
int queue_delivery_snapshot(QueueStore *store, const char *name, uint32_t name_len,
                            uint64_t delivery_tag, uint64_t owner, QueueMessage *out);

/* Per-queue labeled metrics snapshot, taken under the store lock. */
typedef void (*QueueStatsFn)(const char *name, uint32_t name_len,
                             uint64_t depth, uint64_t inflight, void *ud);
void queue_foreach_stats(QueueStore *store, QueueStatsFn fn, void *ud);

int queue_ack(QueueStore *store, const char *name, uint32_t name_len,
              uint64_t delivery_tag);
int queue_ack_for_owner(QueueStore *store, const char *name, uint32_t name_len,
                        uint64_t delivery_tag, uint64_t owner);
int queue_nack(QueueStore *store, const char *name, uint32_t name_len,
               uint64_t delivery_tag, int requeue);
int queue_nack_for_owner(QueueStore *store, const char *name, uint32_t name_len,
                         uint64_t delivery_tag, uint64_t owner, int requeue);
/* As queue_nack_for_owner, but a positive delay keeps a requeued message
 * ineligible until the supplied delay has elapsed. */
int queue_nack_for_owner_delay(QueueStore *store, const char *name, uint32_t name_len,
                               uint64_t delivery_tag, uint64_t owner, int requeue,
                               uint64_t delay_ms);
void queue_requeue_owner(QueueStore *store, uint64_t owner);
void queue_reap(QueueStore *store);
void queue_message_free(QueueMessage *message);

/* Copy up to `max` retained messages without changing queue order, delivery
 * state, visibility, counters, or WAL contents.  `state_mask` selects the
 * public ready/delayed/in-flight states.  When `include_bodies` is non-zero,
 * only complete bodies fitting in the aggregate `body_budget` are copied;
 * omitted bodies are marked in their individual snapshots.  Returns 1 for
 * an existing queue, 0 when it does not exist, or -1 on invalid input or OOM.
 * The caller owns `*out` and releases it with queue_peek_free(). */
int queue_peek(QueueStore *store, const char *name, uint32_t name_len,
               unsigned state_mask, uint32_t max, int include_bodies,
               uint64_t body_budget, QueueMessageSnapshot **out,
               uint32_t *out_count);
/* As queue_peek(), but return only messages whose monotonically assigned ID
 * is greater than after_id.  This is a keyset boundary, not an offset: it
 * remains valid when older messages are acknowledged or expire. */
int queue_peek_after(QueueStore *store, const char *name, uint32_t name_len,
                     uint64_t after_id, unsigned state_mask, uint32_t max,
                     int include_bodies, uint64_t body_budget,
                     QueueMessageSnapshot **out, uint32_t *out_count,
                     uint64_t *out_revision);
void queue_peek_free(QueueMessageSnapshot *messages, uint32_t count);
/* Copy one exact retained message without consuming or changing its state.
 * Returns 1 when copied, 0 when the Queue/message is absent or expired, -2
 * when its requested complete body exceeds body_budget, or -1 on invalid
 * input/allocation failure. */
int queue_message_snapshot(QueueStore *store, const char *name,
                           uint32_t name_len, uint64_t message_id,
                           int include_body, uint64_t body_budget,
                           QueueMessageSnapshot *out);
void queue_message_snapshot_free(QueueMessageSnapshot *message);

/* Batch operations (protocol 0x2d/0x2e/0x2f). All records in one batch share
 * one group fsync before the response, and the durability contract stays
 * per-message: every removal happens only after the fsync covering its own
 * record. Batch counts are bounded by the caller (protocol cap 256). */
/* Capacity is pre-checked for the whole batch: nothing is written when the
 * batch would exceed max_depth. data[i]/lens[i] describe message i;
 * out_ids (may be NULL) receives the message IDs. Returns 0 on success, -1
 * when the queue is missing, the batch does not fit, or persistence fails. */
int queue_publish_batch(QueueStore *store, const char *name, uint32_t name_len,
                        uint32_t count, const void *const *data,
                        const uint32_t *lens, uint64_t *out_ids);
/* Fills up to `max` ready messages in one store pass with one fsync covering
 * all delivery records. `out` must have room for `max` entries and
 * *out_count reports how many were delivered (0 for an empty queue).
 * Returns 0, or -1 on a missing queue or persistence failure. */
int queue_consume_batch(QueueStore *store, const char *name, uint32_t name_len,
                        uint64_t visibility_ms, uint64_t owner, uint32_t max,
                        QueueMessage *out, uint32_t *out_count);
/* Every tag is matched against `owner` or `owner_fallback` (0 disables the
 * fallback). Unknown or not-in-flight tags are skipped. Returns 0 with
 * *out_acked processed tags, -1 on a missing queue or persistence failure. */
int queue_ack_batch(QueueStore *store, const char *name, uint32_t name_len,
                    uint64_t owner, uint64_t owner_fallback,
                    const uint64_t *tags, uint32_t count, uint32_t *out_acked);
/* As queue_ack_batch, with NACK semantics: requeue=1 returns the messages to
 * the ready set immediately; requeue=0 discards them or routes them to the
 * queue's dead-letter queue with per-message durability (fail closed). */
int queue_nack_batch(QueueStore *store, const char *name, uint32_t name_len,
                     uint64_t owner, uint64_t owner_fallback,
                     const uint64_t *tags, uint32_t count, int requeue,
                     uint32_t *out_acked);

/* Durable named consumers. A registration maps a stable name to a stable
 * owner token: consuming as a named consumer delivers under that token, so a
 * disconnected worker's in-flight deliveries follow their visibility
 * deadlines instead of being requeued immediately, and reconnecting with the
 * same name keeps ownership and prefetch accounting stable. Deliveries stay
 * one-use per process; delivery tags are not valid across a restart.
 * Registration survives restart on durable stores. */
int queue_consumer_register(QueueStore *store, const char *name,
                            uint32_t name_len, uint64_t *out_owner);
/* Returns 0 when the consumer does not exist, 1 when it was removed (its
 * in-flight deliveries are requeued immediately), -1 on persistence failure. */
int queue_consumer_unregister(QueueStore *store, const char *name,
                              uint32_t name_len);
int queue_consume_for_consumer(QueueStore *store, const char *queue,
                               uint32_t qlen, const char *consumer,
                               uint32_t clen, uint64_t visibility_ms,
                               QueueMessage *out);
/* Returns 1 with *out_owner when the consumer exists, 0 when it does not. */
int queue_consumer_lookup(QueueStore *store, const char *name,
                          uint32_t name_len, uint64_t *out_owner);
/* Enumerates public durable-consumer names under the metadata lock. Native
 * owner tokens remain private to the Queue engine. */
typedef void (*QueueConsumerStatsFn)(const char *name, uint32_t name_len,
                                     void *ud);
void queue_consumer_foreach(QueueStore *store, QueueConsumerStatsFn fn,
                            void *ud);
uint64_t queue_consumer_count(QueueStore *store);

uint64_t queue_depth(QueueStore *store, const char *name, uint32_t name_len);
uint64_t queue_inflight(QueueStore *store, const char *name, uint32_t name_len);
int queue_stats(QueueStore *store, const char *name, uint32_t name_len,
                uint64_t *depth, uint64_t *inflight);
int queue_revision(QueueStore *store, const char *name, uint32_t name_len,
                   uint64_t *out_revision);
typedef struct QueueConfigSnapshot {
    int durable;
    uint64_t max_depth;
    uint32_t max_deliveries;
    uint32_t dead_letter_queue_len;
    char dead_letter_queue[QUEUE_NAME_MAX];
    uint64_t revision;
} QueueConfigSnapshot;
/* Copy Queue declaration options and revision under the Queue lock. The
 * dead-letter name is byte-exact and valid only in this output structure. */
int queue_config_snapshot(QueueStore *store, const char *name, uint32_t name_len,
                          QueueConfigSnapshot *out);
uint64_t queue_owner_inflight(QueueStore *store, uint64_t owner);
uint64_t queue_redeliveries(QueueStore *store);
uint64_t queue_deadlettered(QueueStore *store);
uint64_t queue_count(QueueStore *store);
uint64_t queue_total_depth(QueueStore *store);
uint64_t queue_total_inflight(QueueStore *store);
int queue_persistence_failed(QueueStore *store);
/* Rewrite the WAL into a crash-safe checkpoint when history outgrows live
 * state (WAL size > 2x live estimate + 1 MiB floor). Emits only existing
 * record types, so replay code and format are unchanged; the temp file is
 * fsynced, atomically renamed, and the parent directory fsynced before the
 * append fd swaps. Returns 1 checkpointed, 0 not needed, -1 on failure (the
 * previous WAL stays valid either way). Maintenance calls this periodically;
 * exposed for tests and tooling. */
int queue_checkpoint_maybe(QueueStore *store);

/* ---- Exchanges and routing ----
 *
 * An exchange is a router in front of queues. Publish delivers one copy to
 * each bound queue that matches the routing key, respecting each target
 * queue's own capacity and durability settings. Routing semantics:
 *
 * - direct:  binding key equal to the routing key (byte-exact).
 * - fanout:  every bound queue matches; the routing key is ignored.
 * - topic:   binding key is a pattern of '.'-separated words where '*'
 *            matches exactly one word and '#' matches zero or more words.
 *
 * `exchange_publish` returns 0 on success with `*out_routed` set to the
 * number of target queues that received a copy. Zero routed copies is an
 * "unroutable" outcome (counted in exchange_unroutable), not an error: when
 * the exchange declares an alternate exchange, an unroutable message is
 * routed there once before being reported. A negative return is a hard
 * failure (unknown exchange, a target queue is full, or persistence
 * failed); nothing was acknowledged. A publish that fails partway through a
 * durable fanout may leave confirmed copies in some target queues, so
 * exchange routing is at-least-once, never silently lossy. */

/* Declare idempotently. Redeclaring with a different type, durability, or
 * alternate exchange fails. `ae_name` is optional; it must differ from the
 * exchange name. The alternate exchange is not auto-created. */
int exchange_declare(QueueStore *store, const char *name, uint32_t name_len,
                     int durable, unsigned type,
                     const char *ae_name, uint32_t ae_len);
/* Bind an existing queue to an exchange. The queue must already be
 * declared; the binding key is the exact key (direct), ignored (fanout), or
 * a '*'/ '#' pattern (topic) with at most 128 words. Re-binding an existing
 * (queue, key) pair is idempotent. Fails when the per-exchange binding cap
 * is reached. Returns 0 on success, -1 on error. */
int exchange_bind(QueueStore *store, const char *ex_name, uint32_t ex_len,
                  const char *queue, uint32_t queue_len,
                  const char *key, uint32_t key_len);
/* Remove one binding. Returns 1 when removed, 0 when absent, -1 on error. */
int exchange_unbind(QueueStore *store, const char *ex_name, uint32_t ex_len,
                    const char *queue, uint32_t queue_len,
                    const char *key, uint32_t key_len);
/* As exchange_unbind, but rejects a stale router revision while holding the
 * routing metadata lock. Returns 2 for a stale revision. */
int exchange_unbind_if_revision(QueueStore *store, const char *ex_name,
                                uint32_t ex_len, const char *queue,
                                uint32_t queue_len, const char *key,
                                uint32_t key_len, uint64_t expected_revision);
/* Return 1 and the current router revision when found, 0 when absent. */
int exchange_revision(QueueStore *store, const char *name, uint32_t name_len,
                      uint64_t *out_revision);
/* Delete an empty router at its exact revision. Returns 1 when deleted, 0
 * when absent, 2 when stale, 3 when routes remain, and 4 when another router
 * names it as an alternate. */
int exchange_delete_if_revision(QueueStore *store, const char *name,
                                uint32_t name_len, uint64_t expected_revision);
/* Change a router's alternate router at its exact revision. */
int exchange_set_alternate_if_revision(QueueStore *store, const char *name,
                                       uint32_t name_len, const char *alternate,
                                       uint32_t alternate_len,
                                       uint64_t expected_revision);
int exchange_publish(QueueStore *store, const char *ex_name, uint32_t ex_len,
                     const char *key, uint32_t key_len,
                     const void *data, uint32_t len, uint64_t ttl_ms,
                     uint64_t *out_routed);

uint64_t exchange_count(QueueStore *store);
uint64_t exchange_binding_count(QueueStore *store);
uint64_t exchange_unroutable(QueueStore *store);
/* Read-only routing snapshots. Callback data is valid only during the call
 * and must be copied by the receiver. */
typedef void (*ExchangeStatsFn)(const char *name, uint32_t name_len,
                                int durable, unsigned type,
                                const char *alternate, uint32_t alternate_len,
                                uint32_t route_count, uint64_t revision,
                                uint64_t publish_attempt_count,
                                uint64_t unroutable_count,
                                void *ud);
void exchange_foreach_stats(QueueStore *store, ExchangeStatsFn fn, void *ud);
typedef void (*ExchangeRouteFn)(const char *queue, uint32_t queue_len,
                                const char *key, uint32_t key_len, void *ud);
int exchange_foreach_route(QueueStore *store, const char *name,
                           uint32_t name_len, ExchangeRouteFn fn, void *ud,
                           uint32_t *out_count);
/* Check one exact route without exposing private binding pointers. */
int exchange_route_exists(QueueStore *store, const char *name,
                          uint32_t name_len, const char *queue,
                          uint32_t queue_len, const char *key,
                          uint32_t key_len);

/* ---- Atomic cache-plus-message transactions ----
 *
 * The atomic operations commit a cache mutation and one or more queue
 * messages together. The commit authority is a cache-WAL transaction record
 * written between two queue-WAL records: TX_PREPARE durably reserves the
 * intent (including pre-allocated message IDs), the cache record is the
 * commit marker, and TX_COMMIT materializes the messages. A crash anywhere
 * leaves either both sides visible after recovery or neither:
 *
 * - prepare without a cache commit  -> discarded (neither);
 * - cache commit without TX_COMMIT  -> startup reconciliation materializes
 *   the messages and writes the missing TX_COMMIT record;
 * - both records                    -> materialized directly by replay.
 *
 * Runtime flow (the store lock is held across the whole window so the
 * resolved target set and capacity checks stay authoritative):
 *
 *   queue_tx_prepare  resolves/validates targets, capacity-checks, writes
 *                     and fsyncs TX_PREPARE, and returns with the lock held;
 *   queue_tx_commit   writes and fsyncs TX_COMMIT, materializes the
 *                     messages, releases the lock;
 *   queue_tx_abort    releases the lock with nothing materialized (the
 *                     stale prepare is discarded by any later recovery).
 *
 * Callers must therefore never hold a cache-side lock while calling
 * queue_tx_prepare (lock order: queue store -> cache WAL), and must finish
 * every prepared transaction on the same thread. */

/* Resolve and durably reserve a transaction delivery. `ex_len == 0` is the
 * default exchange: `key` names the target queue directly (enqueue path).
 * Publishing requires at least one routed target; all targets must be
 * durable queues, and the queue WAL must be enabled. Returns 0 and leaves
 * the store lock held on success, 1 when a publish is unroutable (nothing
 * reserved), -1 on error (nothing reserved). `tx_id` is the caller-supplied
 * commit identifier written into both logs. */
int queue_tx_prepare(QueueStore *store, const char *ex_name, uint32_t ex_len,
                     const char *key, uint32_t key_len,
                     const void *data, uint32_t len, uint64_t tx_id,
                     QueueTx **out);
/* Commit a prepared transaction: fsync the TX_COMMIT record, materialize
 * the reserved messages into their queues, release the store lock. On
 * failure the transaction stays prepared (recovery will reconcile it from
 * the cache commit) and the lock is released. */
int queue_tx_commit(QueueTx *tx, uint64_t *out_routed);
/* Release a prepared transaction without materializing anything. */
void queue_tx_abort(QueueTx *tx);
/* The commit identifier carried by a prepared transaction. */
uint64_t queue_tx_id(const QueueTx *tx);

/* Recovery reconciliation. `queue_tx_pending_ids` returns the ids of all
 * prepared-but-unmaterialized transactions (caller frees `*out_ids`).
 * `queue_tx_resolve` materializes a pending transaction and appends its
 * durable TX_COMMIT record when `committed` is non-zero, or discards it
 * when zero. Returns 1 when the transaction was pending, 0 when the id was
 * unknown, -1 on error. */
uint64_t queue_tx_pending_ids(QueueStore *store, uint64_t **out_ids);
int queue_tx_resolve(QueueStore *store, uint64_t tx_id, int committed);

/* Non-zero when the store has a durable queue WAL (required for atomic
 * transactions). */
int queue_wal_enabled(QueueStore *store);

#endif
