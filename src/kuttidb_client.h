#ifndef KUTTIDB_CLIENT_H
#define KUTTIDB_CLIENT_H

/* Public C client for KuttiDB — atomic job completion surface.
 *
 * This companion library is a network client (TCP or Unix socket, optional
 * AUTH and verified TLS when built with OpenSSL). It never touches the
 * shared-memory cache ABI: an application using `libkuttidb_embed` for
 * cache work opens this client to the SAME server for durable jobs and
 * must verify the instance identity itself when combining local paths.
 *
 * Ownership rules:
 *   - Functions returning `const char *` or byte spans point into
 *     result-owned buffers valid until the result is released.
 *   - Every result is released with kuttidb_job_*_free / ..._result_free.
 *   - No function retains caller buffers beyond the call.
 * All 64-bit fields are unsigned; encoding is little-endian on the wire.
 *
 * Feature negotiation: call kuttidb_job_check_supported() once; every job
 * call fails with KUTTIDB_JOB_UNSUPPORTED_FEATURE when the server lacks
 * the feature (capability bit 16) or runs with --job-completion off. There
 * is deliberately NO fallback of composing the operation from separate
 * writes.
 *
 * Error handling: every call returns a KuttidbJobStatus. Statuses mirror
 * the server's typed outcomes (see docs/design/PROTOCOL.md);
 * KUTTIDB_JOB_IN_DOUBT means the commit outcome is unknown — the only safe
 * continuation is an exact retry with the SAME operation id or a receipt
 * lookup. kuttidb_last_error() returns a stable human-readable message.
 */

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define KUTTIDB_JOB_ID_LEN 16
#define KUTTIDB_JOB_DIGEST_LEN 32

typedef struct KuttiDBClient KuttiDBClient;

/* ---- status codes (numeric values match the wire error byte) ---- */

typedef enum {
    KUTTIDB_JOB_OK = 0,
    KUTTIDB_JOB_UNSUPPORTED_FEATURE = 1,
    KUTTIDB_JOB_VALIDATION_FAILED = 2,
    KUTTIDB_JOB_REQUEST_TOO_LARGE = 3,
    KUTTIDB_JOB_IDEMPOTENCY_CONFLICT = 4,
    KUTTIDB_JOB_STATE_VERSION_CONFLICT = 5,
    KUTTIDB_JOB_DELIVERY_EXPIRED = 6,
    KUTTIDB_JOB_DELIVERY_NOT_OWNED = 7,
    KUTTIDB_JOB_RESOURCE_EXHAUSTED = 8,
    KUTTIDB_JOB_OPERATION_IN_PROGRESS = 9,
    KUTTIDB_JOB_OPERATION_IN_DOUBT = 10,
    KUTTIDB_JOB_PERSISTENCE_UNAVAILABLE = 11,
    KUTTIDB_JOB_NOT_FOUND = 12,
    KUTTIDB_JOB_TRANSPORT_ERROR = 100 /* connect/timeout/protocol */
} KuttidbJobStatus;

const char *kuttidb_job_status_name(KuttidbJobStatus status);
/* Non-zero when the durable outcome is unknown rather than definitely
 * "not committed". */
int kuttidb_job_status_in_doubt(KuttidbJobStatus status);
/* Last error detail for the calling thread's most recent call. */
const char *kuttidb_last_error(const KuttiDBClient *client);

/* ---- connection lifecycle ---- */

typedef struct KuttiDBClientOptions {
    const char *host;        /* TCP host; default 127.0.0.1 */
    int port;                /* default 7379 */
    const char *unix_path;   /* Unix socket; overrides host/port */
    const char *auth_token;  /* optional; 1..1024 bytes */
    size_t auth_token_len;
    double timeout_seconds;  /* default 5.0 */
    /* Verified TLS (built with OpenSSL support only; NULL = plaintext). */
    const char *tls_ca_file;
    const char *tls_server_name;
} KuttiDBClientOptions;

KuttiDBClient *kuttidb_client_create(const KuttiDBClientOptions *options);
void kuttidb_client_destroy(KuttiDBClient *client);
/* Reconnects after a transport error; configuration is preserved. */
KuttidbJobStatus kuttidb_client_reconnect(KuttiDBClient *client);
/* 1 when the server advertises CAP_JOBS (bit 16) under protocol >= 1.8. */
KuttidbJobStatus kuttidb_job_check_supported(KuttiDBClient *client, int *supported);

/* Generates a random UUIDv4 operation id (16 bytes). Returns 0 on
 * success; -1 when no secure entropy source is available. */
int kuttidb_job_new_operation_id(unsigned char out[KUTTIDB_JOB_ID_LEN]);

/* ---- Queue manifest (stable identity discovery) ---- */

typedef struct KuttiDBQueueManifestEntry {
    const char *name;
    uint32_t name_len;
    int durable;
    uint64_t incarnation;
    uint64_t depth;
    uint64_t inflight;
    uint64_t max_depth;
    uint64_t revision;
} KuttiDBQueueManifestEntry;

typedef struct KuttiDBQueueManifest {
    KuttiDBQueueManifestEntry *entries;
    uint32_t count;
} KuttiDBQueueManifest;

KuttidbJobStatus kuttidb_queue_manifest(KuttiDBClient *client,
                                        KuttiDBQueueManifest *out);
void kuttidb_queue_manifest_free(KuttiDBQueueManifest *manifest);

/* ---- completion-capable consume ---- */

typedef struct KuttiDBJobDelivery {
    unsigned char store_id[KUTTIDB_JOB_ID_LEN];
    const char *queue;
    uint32_t queue_len;
    uint64_t queue_incarnation;
    uint64_t message_id;
    uint32_t attempts;
    int redelivered;
    uint64_t lease_deadline_ms; /* wall-clock mirror; display only */
    unsigned char proof[KUTTIDB_JOB_ID_LEN]; /* opaque, one-use */
    const unsigned char *value;
    uint32_t value_len;
} KuttiDBJobDelivery;

KuttidbJobStatus kuttidb_job_consume(KuttiDBClient *client,
                                     const char *queue, uint32_t queue_len,
                                     const char *consumer,
                                     uint32_t consumer_len,
                                     double visibility_seconds,
                                     KuttiDBJobDelivery *out);
void kuttidb_job_delivery_free(KuttiDBJobDelivery *delivery);

/* ---- atomic completion ---- */

typedef struct KuttiDBJobOutput {
    const char *queue;
    uint32_t queue_len;
    uint64_t queue_incarnation;
    const unsigned char *value;
    uint32_t value_len;
} KuttiDBJobOutput;

typedef struct KuttiDBJobCompletion {
    /* Stable intent: the caller owns operation_id and every span.
     * output_queue == NULL means no output message. */
    const unsigned char *operation_id; /* KUTTIDB_JOB_ID_LEN bytes */
    const char *input_queue;
    uint32_t input_queue_len;
    uint64_t input_incarnation;
    uint64_t input_message_id;
    const unsigned char *proof;        /* from the live delivery */
    const unsigned char *state_key;
    uint32_t state_key_len;
    uint64_t expected_version;         /* 0 = create the state entry */
    const unsigned char *state_value;
    uint32_t state_value_len;
    const KuttiDBJobOutput *output;    /* optional */
} KuttiDBJobCompletion;

typedef struct KuttiDBJobCompletionResult {
    uint64_t commit_id;
    uint64_t state_version;
    uint64_t output_message_id; /* 0 = none */
    uint64_t completed_at_ms;
    uint64_t receipt_expires_at_ms;
    int replayed;               /* 1 = matched stored receipt */
} KuttiDBJobCompletionResult;

KuttidbJobStatus kuttidb_job_complete(KuttiDBClient *client,
                                      const KuttiDBJobCompletion *request,
                                      KuttiDBJobCompletionResult *out);

/* ---- receipts ---- */

typedef struct KuttiDBJobReceipt {
    unsigned char operation_id[KUTTIDB_JOB_ID_LEN];
    uint64_t commit_id;
    uint64_t state_version;
    uint64_t output_message_id; /* 0 = none */
    uint64_t completed_at_ms;
    uint64_t receipt_expires_at_ms;
} KuttiDBJobReceipt;

/* Receipt lookup by operation id; works after restarts without the
 * delivery proof. KUTTIDB_JOB_NOT_FOUND means "no retained receipt",
 * never "never executed". */
KuttidbJobStatus kuttidb_job_completion(KuttiDBClient *client,
                                        const unsigned char operation_id
                                            [KUTTIDB_JOB_ID_LEN],
                                        KuttiDBJobReceipt *out);

/* ---- direct durable-state mutations ---- */

typedef struct KuttiDBJobMutationReceipt {
    unsigned char operation_id[KUTTIDB_JOB_ID_LEN];
    uint64_t commit_id;
    uint64_t state_version;
    uint64_t completed_at_ms;
    uint64_t receipt_expires_at_ms;
    int replayed;
} KuttiDBJobMutationReceipt;

typedef struct KuttiDBStateValue {
    unsigned char *value;   /* owned; release with kuttidb_state_value_free */
    uint32_t value_len;
    uint64_t version;
    uint64_t commit_id;
} KuttiDBStateValue;

KuttidbJobStatus kuttidb_state_get(KuttiDBClient *client, const char *key,
                                   uint32_t key_len, KuttiDBStateValue *out);
void kuttidb_state_value_free(KuttiDBStateValue *value);

KuttidbJobStatus kuttidb_state_put(KuttiDBClient *client, const char *key,
                                   uint32_t key_len,
                                   const unsigned char *value,
                                   uint32_t value_len,
                                   uint64_t expected_version,
                                   const unsigned char operation_id
                                       [KUTTIDB_JOB_ID_LEN],
                                   KuttiDBJobMutationReceipt *out);
KuttidbJobStatus kuttidb_state_delete(KuttiDBClient *client, const char *key,
                                      uint32_t key_len,
                                      uint64_t expected_version,
                                      const unsigned char operation_id
                                          [KUTTIDB_JOB_ID_LEN],
                                      KuttiDBJobMutationReceipt *out);

/* Kind of a direct mutation receipt: 2 = state_put, 3 = state_delete. */
typedef struct KuttiDBDurableOperation {
    unsigned char operation_id[KUTTIDB_JOB_ID_LEN];
    unsigned char kind;
    uint64_t commit_id;
    uint64_t state_version;
    uint64_t completed_at_ms;
    uint64_t receipt_expires_at_ms;
} KuttiDBDurableOperation;

KuttidbJobStatus kuttidb_durable_operation(KuttiDBClient *client,
                                           const unsigned char operation_id
                                               [KUTTIDB_JOB_ID_LEN],
                                           KuttiDBDurableOperation *out);

#ifdef __cplusplus
}
#endif

#endif
