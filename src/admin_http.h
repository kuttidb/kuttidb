#ifndef KUTTIDB_ADMIN_HTTP_H
#define KUTTIDB_ADMIN_HTTP_H

#include <stddef.h>
#include <stdint.h>

typedef struct KuttiDB KuttiDB;
typedef struct QueueStore QueueStore;
typedef struct StreamStore StreamStore;
typedef struct AdminHttp AdminHttp;

typedef struct AdminHttpStatus {
    uint64_t uptime_seconds, connections, rejected_connections, auth_failures;
    uint64_t mutation_attempts, audit_failures, rate_limit_rejections, operation_in_doubt;
    uint64_t admin_connections, active_tails, active_deliveries, active_claims;
    uint64_t queued_jobs, running_jobs;
    uint64_t keyspace_entries, keyspace_live_bytes, keyspace_allocated_bytes;
    uint64_t keyspace_expired, keyspace_evicted;
    uint64_t queue_count, queue_ready, queue_inflight, queue_redeliveries, queue_deadletters;
    uint64_t stream_count, stream_partitions, stream_retained_bytes, stream_groups, stream_members;
    int ready, keyspace_persistence_failed, queue_persistence_failed, stream_persistence_failed;
    int persistence_enabled, tls_available;
    int event_loops;
    const char *event_backend;
    const char *durability;
} AdminHttpStatus;

typedef void (*AdminHttpStatusFn)(void *ud, AdminHttpStatus *out);
typedef void (*AdminHttpAuthFailureFn)(void *ud);
typedef int (*AdminHttpKeyspacePutFn)(const char *key, uint32_t key_len,
                                      const char *value, uint32_t value_len,
                                      uint64_t ttl_ms, int batch);
typedef int (*AdminHttpKeyspaceDeleteFn)(const char *key, uint32_t key_len);
/* Single-flight claim adapters use an opaque, server-private owner token.
 * Return 1 on a newly acquired/completed/released claim, 0 when the claim is
 * absent, busy, expired, or owned by another caller, and -1 on failure. */
typedef int (*AdminHttpKeyspaceClaimAcquireFn)(const char *key, uint32_t key_len,
                                               uint64_t owner, uint64_t lease_ms);
typedef int (*AdminHttpKeyspaceClaimCompleteFn)(const char *key, uint32_t key_len,
                                                uint64_t owner, const char *value,
                                                uint32_t value_len, uint64_t ttl_ms,
                                                int negative);
typedef int (*AdminHttpKeyspaceClaimReleaseFn)(const char *key, uint32_t key_len,
                                               uint64_t owner);
/* Runs the server's existing crash-safe Keyspace snapshot/checkpoint. */
typedef int (*AdminHttpKeyspaceCheckpointFn)(void);
/* Executes one existing all-or-nothing Keyspace-plus-Queue/Routing
 * transaction.  Operation values are the stable Management API union:
 * 1 put-and-route, 2 put-and-enqueue, 3 delete-and-route, and 4
 * update-if-present-and-route. */
typedef int (*AdminHttpAtomicFn)(unsigned operation, const char *key,
                                 uint32_t key_len, const char *target,
                                 uint32_t target_len, const char *routing_key,
                                 uint32_t routing_key_len, const void *body,
                                 uint32_t body_len, uint64_t ttl_ms,
                                 uint64_t *out_transaction_id,
                                 uint64_t *out_routed);

typedef struct AdminHttpConfig {
    const char *bind;                 /* IPv4:PORT */
    const unsigned char *token;
    size_t token_len;
    const char *const *allow_origins;
    size_t allow_origin_count;
    const char *tls_cert;
    const char *tls_key;
    /* Required whenever the listener is enabled.  Mutation attempts are
     * appended and synced before dispatch, so an unhealthy audit log makes
     * the mutation path fail closed. */
    const char *audit_log;
    unsigned max_clients;
    unsigned max_tail_clients;
    unsigned session_limit;
    unsigned job_limit;
    KuttiDB *keyspace;
    QueueStore *queues;
    StreamStore *streams;
    AdminHttpStatusFn status;
    void *status_ud;
    AdminHttpAuthFailureFn auth_failure;
    void *auth_failure_ud;
    /* The server supplies its durability-aware Keyspace adapter; the HTTP
     * module never reaches into the server WAL or Keyspace internals. */
    AdminHttpKeyspacePutFn keyspace_put;
    AdminHttpKeyspaceDeleteFn keyspace_delete;
    AdminHttpKeyspaceClaimAcquireFn keyspace_claim_acquire;
    AdminHttpKeyspaceClaimCompleteFn keyspace_claim_complete;
    AdminHttpKeyspaceClaimReleaseFn keyspace_claim_release;
    AdminHttpKeyspaceCheckpointFn keyspace_checkpoint;
    AdminHttpAtomicFn atomic_execute;
} AdminHttpConfig;

/* Reads a token under the same restrictive rules as the data listener. */
int admin_http_load_token(const char *path, unsigned char *out, size_t *out_len,
                          size_t out_cap);
/* Creates the listener.  The caller starts it only after engine setup. */
AdminHttp *admin_http_create(const AdminHttpConfig *config);
int admin_http_start(AdminHttp *admin);
void admin_http_stop(AdminHttp *admin);
void admin_http_destroy(AdminHttp *admin);

#endif
