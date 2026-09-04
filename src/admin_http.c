#define _GNU_SOURCE
#include "admin_http.h"
#include "admin_json.h"
#include "kuttidb.h"
#include "queue.h"
#include "stream.h"
#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#ifdef HAVE_OPENSSL
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>
#endif

#define ADMIN_REQ_MAX 8192u
#define ADMIN_BODY_MAX (256u << 10)
#define ADMIN_LIST_MAX 500u
#define ADMIN_NAME_MAX 255u
#define ADMIN_REQUEST_ID_LEN 32u
#define ADMIN_DELIVERY_LIMIT 256u
#define ADMIN_CURSOR_LIMIT 512u
#define ADMIN_CURSOR_TTL_SECONDS 600u
#define ADMIN_JOB_LIMIT 32u
#define ADMIN_TAIL_RECORD_BYTES (64u << 10)
#define ADMIN_TAIL_MAX_MS 60000u
#define ADMIN_TAIL_HEARTBEAT_MS 5000u
#define ADMIN_TAIL_POLL_MS 100u
#define ADMIN_TAIL_EVENTS_PER_SECOND 100u
#define ADMIN_THREAD_STACK_BYTES (2u << 20)
/* These are deliberately separate from data-listener limits.  A Management
 * UI can make at most this many state-changing requests in a rolling second
 * and send this many mutation-body bytes in a rolling minute. */
#define ADMIN_MUTATIONS_PER_SECOND 512u
#define ADMIN_MUTATION_BYTES_PER_MINUTE (16u << 20)
#define ADMIN_RATE_SAMPLES 1024u

typedef struct AdminName { unsigned char bytes[ADMIN_NAME_MAX]; uint32_t len; } AdminName;
typedef struct AdminQueue { AdminName name; uint64_t depth, inflight; } AdminQueue;
typedef struct AdminStream { AdminName name; uint32_t partitions; uint64_t bytes, records; } AdminStream;
typedef struct AdminGroup { AdminName stream, group; uint64_t generation; uint32_t members; } AdminGroup;
typedef struct AdminGroupMember { uint32_t index, assigned; uint64_t lease_remaining_ms; } AdminGroupMember;
typedef struct AdminGroupMemberSnapshot { AdminGroupMember members[STREAM_GROUP_MEMBERS_MAX]; uint32_t count; } AdminGroupMemberSnapshot;
typedef struct AdminRouter { AdminName name, alternate; unsigned type; int durable; uint32_t routes; uint64_t revision, published, unroutable; } AdminRouter;
typedef struct AdminRouterSnapshot { AdminRouter routers[ADMIN_LIST_MAX]; uint32_t count; int truncated; } AdminRouterSnapshot;
typedef struct AdminRoute { AdminName queue, key; } AdminRoute;
typedef struct AdminRouteSnapshot { AdminRoute routes[ADMIN_LIST_MAX]; uint32_t count; int truncated; } AdminRouteSnapshot;
typedef struct AdminConsumer { AdminName name; } AdminConsumer;
typedef struct AdminKeyMeta { AdminName key; uint32_t value_len, expiry; } AdminKeyMeta;
typedef struct AdminKeyScan { AdminKeyMeta entries[ADMIN_LIST_MAX]; uint32_t count; int key_too_large; AdminName prefix; unsigned char expiry_filter; } AdminKeyScan;
typedef struct AdminIdempotency {
    char key[129];
    uint64_t fingerprint, expires_at;
    char response[4096];
    size_t response_len;
    int status;
} AdminIdempotency;
typedef struct AdminDelivery {
    char id[ADMIN_REQUEST_ID_LEN + 3];
    AdminName queue;
    uint64_t message_id, tag, owner, deadline_ms;
    unsigned char active, expired;
} AdminDelivery;
typedef struct AdminClaim {
    char id[ADMIN_REQUEST_ID_LEN + 4];
    AdminName key;
    uint64_t owner, deadline_ms;
    unsigned char active, expired;
} AdminClaim;
/* Cursor state stays server-side so a client never receives a native position
 * or a reversible message ID. The random handle is scoped to one Queue and
 * one browse filter and expires promptly. */
typedef struct AdminQueueCursor {
    char id[ADMIN_REQUEST_ID_LEN + 4];
    AdminName queue;
    uint64_t after_id, expires_at;
    unsigned states;
    unsigned char include_bodies, active;
} AdminQueueCursor;
typedef struct AdminKeyCursor {
    char id[ADMIN_REQUEST_ID_LEN + 4];
    KuttiDBMetadataCursor position;
    AdminName prefix;
    uint64_t expires_at;
    unsigned char expiry_filter, active;
} AdminKeyCursor;
typedef struct AdminGroupSession {
    char id[ADMIN_REQUEST_ID_LEN + 4];
    AdminName stream, group;
    uint64_t owner, deadline_ms;
    uint32_t lease_ms;
    unsigned char active, expired;
} AdminGroupSession;
typedef enum AdminJobState { ADMIN_JOB_UNUSED, ADMIN_JOB_QUEUED, ADMIN_JOB_RUNNING, ADMIN_JOB_SUCCEEDED, ADMIN_JOB_FAILED, ADMIN_JOB_CANCELLED } AdminJobState;
typedef enum AdminJobKind { ADMIN_JOB_STREAM_TRUNCATE, ADMIN_JOB_STREAM_DELETE, ADMIN_JOB_KEYSPACE_CHECKPOINT,
                            ADMIN_JOB_QUEUE_CHECKPOINT, ADMIN_JOB_STREAM_CHECKPOINT,
                            ADMIN_JOB_CHECKPOINT_ALL } AdminJobKind;
typedef struct AdminJob {
    uint64_t id, expected_revision, base_offset, created_at, completed_at;
    AdminName stream;
    uint32_t partition;
    AdminJobKind kind;
    AdminJobState state;
    int native_rc;
    char request_id[ADMIN_REQUEST_ID_LEN + 1];
    uint64_t audit_idempotency_hash;
} AdminJob;
typedef struct AdminTail {
    struct AdminHttp *admin;
    int fd;
    void *ssl;
    AdminName stream;
    uint32_t partition;
    uint64_t offset;
    char request_id[ADMIN_REQUEST_ID_LEN + 1];
    char origin[512];
    unsigned transfer_ready;
} AdminTail;
typedef struct AdminConnection { struct AdminHttp *admin; int fd; } AdminConnection;
typedef struct AdminRateSample { uint64_t when_ms; size_t bytes; } AdminRateSample;
struct AdminHttp {
    AdminHttpConfig c;
    int fd, running;
    int audit_fd;
    unsigned max_clients, max_tail_clients, session_limit, job_limit;
    AdminIdempotency idempotency[128];
    AdminDelivery deliveries[ADMIN_DELIVERY_LIMIT];
    AdminClaim claims[ADMIN_DELIVERY_LIMIT];
    AdminQueueCursor queue_cursors[ADMIN_CURSOR_LIMIT];
    AdminKeyCursor key_cursors[ADMIN_CURSOR_LIMIT];
    AdminGroupSession group_sessions[ADMIN_DELIVERY_LIMIT];
    AdminJob jobs[ADMIN_JOB_LIMIT];
    uint64_t next_delivery_owner;
    uint64_t next_group_session_owner;
    uint64_t next_job_id;
    uint64_t mutation_attempts, audit_failures, rate_limit_rejections, operation_in_doubt, audit_events;
    AdminRateSample rate_samples[ADMIN_RATE_SAMPLES];
    unsigned rate_next, rate_count;
    int audit_failed;
    pthread_t thread;
    pthread_t job_thread;
    pthread_mutex_t jobs_mu;
    pthread_cond_t jobs_cv;
    pthread_mutex_t audit_mu;
    pthread_mutex_t tails_mu;
    /* The accept loop admits bounded connections independently of the
     * listening backlog.  Handler serialization keeps the small resource
     * adapter state safe while still allowing the accept loop to reject a
     * stalled UI connection promptly. */
    pthread_mutex_t handlers_mu;
    unsigned active_clients;
    int jobs_running, jobs_started;
    unsigned active_tails;
    int tail_transferred_fd;
    AdminTail *pending_tail;
    pthread_cond_t tails_cv;
#ifdef HAVE_OPENSSL
    SSL_CTX *tls;
#endif
};
struct out { char *p; size_t cap, len; int full; };
static _Thread_local char request_id[ADMIN_REQUEST_ID_LEN + 1];
static _Thread_local uint64_t audit_idempotency_hash;
static _Thread_local uint64_t collection_snapshot_revision;
static int audit_event(AdminHttp *a, const char *operation, const char *result);
static int make_claim_id(char out[ADMIN_REQUEST_ID_LEN + 4]);
static int make_queue_cursor_id(char out[ADMIN_REQUEST_ID_LEN + 4]);
static int make_key_cursor_id(char out[ADMIN_REQUEST_ID_LEN + 4]);
static int make_private_owner(uint64_t *out);
static int claim_path(AdminHttp *a, const char *path, size_t path_len,
                      const char *suffix, AdminClaim **out);
static int stream_tail_path(const char *path, size_t path_len,
                            AdminName *stream, uint32_t *partition,
                            uint64_t *offset);
static int admin_start_tail(AdminHttp *a, int fd, void *ssl,
                            const AdminName *stream, uint32_t partition,
                            uint64_t offset, const char *origin);
static void make_request_id(void);

static const char *http_status(int status) {
    return status == 201 ? "201 Created" : status == 202 ? "202 Accepted" : "200 OK";
}
static const char *job_state_name(AdminJobState state) {
    switch (state) {
    case ADMIN_JOB_QUEUED: return "queued";
    case ADMIN_JOB_RUNNING: return "running";
    case ADMIN_JOB_SUCCEEDED: return "succeeded";
    case ADMIN_JOB_FAILED: return "failed";
    case ADMIN_JOB_CANCELLED: return "cancelled";
    default: return "unknown";
    }
}
static const char *job_kind_name(AdminJobKind kind) {
    return kind == ADMIN_JOB_KEYSPACE_CHECKPOINT ? "maintenance.keyspace.checkpoint" :
           kind == ADMIN_JOB_QUEUE_CHECKPOINT ? "maintenance.queue.checkpoint" :
           kind == ADMIN_JOB_STREAM_CHECKPOINT ? "maintenance.stream.checkpoint" :
           kind == ADMIN_JOB_CHECKPOINT_ALL ? "maintenance.checkpoint.all" :
           kind == ADMIN_JOB_STREAM_DELETE ? "stream.delete" :
           "stream.partition.truncate";
}
static void job_counts(AdminHttp *a, uint64_t *queued, uint64_t *running) {
    *queued = *running = 0;
    pthread_mutex_lock(&a->jobs_mu);
    for (size_t i=0;i<a->job_limit;i++) {
        if (a->jobs[i].state == ADMIN_JOB_QUEUED) (*queued)++;
        else if (a->jobs[i].state == ADMIN_JOB_RUNNING) (*running)++;
    }
    pthread_mutex_unlock(&a->jobs_mu);
}
static int job_enqueue_stream(AdminHttp *a, AdminJobKind kind, const AdminName *stream, uint32_t partition,
                              uint64_t base_offset, uint64_t expected_revision, uint64_t *id) {
    pthread_mutex_lock(&a->jobs_mu);
    size_t slot=a->job_limit;
    for (size_t i=0;i<a->job_limit;i++) if (a->jobs[i].state == ADMIN_JOB_UNUSED || a->jobs[i].state == ADMIN_JOB_SUCCEEDED || a->jobs[i].state == ADMIN_JOB_FAILED || a->jobs[i].state == ADMIN_JOB_CANCELLED) { slot=i; break; }
    if (slot == a->job_limit) { pthread_mutex_unlock(&a->jobs_mu); return -1; }
    AdminJob *job=&a->jobs[slot]; memset(job,0,sizeof *job);
    job->id=++a->next_job_id; job->kind=kind; job->stream=*stream; job->partition=partition;
    job->base_offset=base_offset; job->expected_revision=expected_revision;
    job->created_at=(uint64_t)time(NULL); job->state=ADMIN_JOB_QUEUED;
    memcpy(job->request_id,request_id,sizeof job->request_id);job->audit_idempotency_hash=audit_idempotency_hash;
    *id=job->id; pthread_cond_signal(&a->jobs_cv); pthread_mutex_unlock(&a->jobs_mu);
    return 0;
}
static int job_enqueue_maintenance(AdminHttp *a, AdminJobKind kind, uint64_t *id) {
    pthread_mutex_lock(&a->jobs_mu);
    size_t slot=a->job_limit;
    for(size_t i=0;i<a->job_limit;i++)if(a->jobs[i].state==ADMIN_JOB_UNUSED||a->jobs[i].state==ADMIN_JOB_SUCCEEDED||a->jobs[i].state==ADMIN_JOB_FAILED||a->jobs[i].state==ADMIN_JOB_CANCELLED){slot=i;break;}
    if(slot==a->job_limit){pthread_mutex_unlock(&a->jobs_mu);return -1;}
    AdminJob *job=&a->jobs[slot];memset(job,0,sizeof *job);job->id=++a->next_job_id;job->kind=kind;job->created_at=(uint64_t)time(NULL);job->state=ADMIN_JOB_QUEUED;memcpy(job->request_id,request_id,sizeof job->request_id);job->audit_idempotency_hash=audit_idempotency_hash;*id=job->id;pthread_cond_signal(&a->jobs_cv);pthread_mutex_unlock(&a->jobs_mu);return 0;
}
static int job_snapshot(AdminHttp *a, uint64_t id, AdminJob *out) {
    int found=0; pthread_mutex_lock(&a->jobs_mu);
    for(size_t i=0;i<a->job_limit;i++) if(a->jobs[i].state!=ADMIN_JOB_UNUSED&&a->jobs[i].id==id){*out=a->jobs[i];found=1;break;}
    pthread_mutex_unlock(&a->jobs_mu); return found;
}
static int job_cancel(AdminHttp *a, uint64_t id) {
    int result=0; pthread_mutex_lock(&a->jobs_mu);
    for(size_t i=0;i<a->job_limit;i++) if(a->jobs[i].state!=ADMIN_JOB_UNUSED&&a->jobs[i].id==id){
        if(a->jobs[i].state==ADMIN_JOB_QUEUED){a->jobs[i].state=ADMIN_JOB_CANCELLED;a->jobs[i].completed_at=(uint64_t)time(NULL);result=1;}else result=-1;break;
    }
    pthread_mutex_unlock(&a->jobs_mu); return result;
}
static void *admin_job_worker(void *ud) {
    AdminHttp *a=ud;
    for (;;) {
        AdminJob work={0}; size_t slot=a->job_limit;
        pthread_mutex_lock(&a->jobs_mu);
        for (;;) {
            for(size_t i=0;i<a->job_limit;i++) if(a->jobs[i].state==ADMIN_JOB_QUEUED){slot=i;break;}
            if(slot<a->job_limit || !a->jobs_running) break;
            pthread_cond_wait(&a->jobs_cv,&a->jobs_mu);
        }
        if(slot==a->job_limit){pthread_mutex_unlock(&a->jobs_mu);break;}
        a->jobs[slot].state=ADMIN_JOB_RUNNING; work=a->jobs[slot]; pthread_mutex_unlock(&a->jobs_mu);
        memcpy(request_id,work.request_id,sizeof request_id);audit_idempotency_hash=work.audit_idempotency_hash;
        const char *operation=job_kind_name(work.kind);int rc;
        if(audit_event(a,operation,"attempt")) rc=-4;
        else {
            if(work.kind==ADMIN_JOB_KEYSPACE_CHECKPOINT)rc=a->c.keyspace_checkpoint?a->c.keyspace_checkpoint():-1;
            else if(work.kind==ADMIN_JOB_QUEUE_CHECKPOINT)rc=queue_checkpoint_maybe(a->c.queues)<0?-1:0;
            else if(work.kind==ADMIN_JOB_STREAM_CHECKPOINT)rc=stream_checkpoint_maybe(a->c.streams)<0?-1:0;
            else if(work.kind==ADMIN_JOB_CHECKPOINT_ALL){rc=a->c.keyspace_checkpoint?a->c.keyspace_checkpoint():-1;if(!rc&&queue_checkpoint_maybe(a->c.queues)<0)rc=-1;if(!rc&&stream_checkpoint_maybe(a->c.streams)<0)rc=-1;}
            else if(work.kind==ADMIN_JOB_STREAM_DELETE)rc=stream_delete_if_revision(a->c.streams,(const char *)work.stream.bytes,work.stream.len,work.expected_revision);
            else rc=stream_truncate_if_revision(a->c.streams,(const char *)work.stream.bytes,work.stream.len,work.partition,work.base_offset,work.expected_revision);
            if(rc==0&&audit_event(a,operation,"completed"))rc=-5;
            else if(rc!=0)(void)audit_event(a,operation,"failed");
        }
        pthread_mutex_lock(&a->jobs_mu);
        if(a->jobs[slot].id==work.id&&a->jobs[slot].state==ADMIN_JOB_RUNNING){a->jobs[slot].native_rc=rc;a->jobs[slot].completed_at=(uint64_t)time(NULL);a->jobs[slot].state=rc==0?ADMIN_JOB_SUCCEEDED:ADMIN_JOB_FAILED;}
        pthread_mutex_unlock(&a->jobs_mu);
    }
    return NULL;
}

static int key_metadata_copy(const char *key, uint32_t key_len, uint32_t value_len,
                             uint32_t expiry, void *ud) {
    AdminKeyScan *scan = ud;
    if (key_len > ADMIN_NAME_MAX) { scan->key_too_large = 1; return 1; }
    if (scan->count >= ADMIN_LIST_MAX) return 1;
    AdminKeyMeta *entry = &scan->entries[scan->count++];
    entry->key.len = key_len;
    memcpy(entry->key.bytes, key, entry->key.len);
    entry->value_len = value_len; entry->expiry = expiry;
    return 0;
}
static int key_metadata_matches(const char *key, uint32_t key_len,
                                uint32_t value_len, uint32_t expiry, void *ud) {
    (void)value_len;
    AdminKeyScan *scan=ud;
    if(scan->key_too_large)return 0;
    if(scan->prefix.len&&(key_len<scan->prefix.len||memcmp(key,scan->prefix.bytes,scan->prefix.len)))return 0;
    if(scan->expiry_filter==1&&!expiry)return 0;
    if(scan->expiry_filter==2&&expiry)return 0;
    return 1;
}

static void make_request_id(void) {
    audit_idempotency_hash = 0;
    unsigned char raw[16];
    int fd = open("/dev/urandom", O_RDONLY | O_CLOEXEC);
    size_t used = 0;
    while (fd >= 0 && used < sizeof raw) {
        ssize_t n = read(fd, raw + used, sizeof raw - used);
        if (n > 0) used += (size_t)n;
        else if (n < 0 && errno == EINTR) continue;
        else break;
    }
    if (fd >= 0) close(fd);
    if (used != sizeof raw) {
        uint64_t seed = (uint64_t)time(NULL) ^ ((uint64_t)getpid() << 32) ^ (uintptr_t)raw;
        for (size_t i = 0; i < sizeof raw; i++) { seed = seed * 6364136223846793005ULL + 1; raw[i] = (unsigned char)(seed >> 32); }
    }
    for (size_t i = 0; i < sizeof raw; i++) snprintf(request_id + i * 2, 3, "%02x", raw[i]);
    request_id[ADMIN_REQUEST_ID_LEN] = 0;
}
/* Session identifiers are separate capabilities, never a derivation of the
 * request ID (which callers may supply for tracing).  Refuse creation if the
 * operating system random source is unavailable rather than making a
 * predictable fallback identifier. */
static int make_group_session_id(char out[ADMIN_REQUEST_ID_LEN + 4]) {
    unsigned char raw[16]; size_t used=0; int fd=open("/dev/urandom",O_RDONLY|O_CLOEXEC);
    while(fd>=0&&used<sizeof raw){ssize_t n=read(fd,raw+used,sizeof raw-used);if(n>0)used+=(size_t)n;else if(n<0&&errno==EINTR)continue;else break;}
    if(fd>=0)close(fd);if(used!=sizeof raw)return -1;
    memcpy(out,"gs:",3);for(size_t i=0;i<sizeof raw;i++)snprintf(out+3+i*2,3,"%02x",raw[i]);out[35]=0;return 0;
}
/* Delivery IDs are capabilities too: request IDs may be supplied by callers
 * for tracing, so never derive a one-use acknowledgement handle from them. */
static int make_delivery_id(char out[ADMIN_REQUEST_ID_LEN + 3]) {
    unsigned char raw[16]; size_t used=0; int fd=open("/dev/urandom",O_RDONLY|O_CLOEXEC);
    while(fd>=0&&used<sizeof raw){ssize_t n=read(fd,raw+used,sizeof raw-used);if(n>0)used+=(size_t)n;else if(n<0&&errno==EINTR)continue;else break;}
    if(fd>=0)close(fd);if(used!=sizeof raw)return -1;
    memcpy(out,"d:",2);for(size_t i=0;i<sizeof raw;i++)snprintf(out+2+i*2,3,"%02x",raw[i]);out[34]=0;return 0;
}
static int make_claim_id(char out[ADMIN_REQUEST_ID_LEN + 4]) {
    unsigned char raw[16];size_t used=0;int fd=open("/dev/urandom",O_RDONLY|O_CLOEXEC);
    while(fd>=0&&used<sizeof raw){ssize_t n=read(fd,raw+used,sizeof raw-used);if(n>0)used+=(size_t)n;else if(n<0&&errno==EINTR)continue;else break;}
    if(fd>=0)close(fd);if(used!=sizeof raw)return -1;
    memcpy(out,"kc:",3);for(size_t i=0;i<sizeof raw;i++)snprintf(out+3+i*2,3,"%02x",raw[i]);out[35]=0;return 0;
}
static int make_queue_cursor_id(char out[ADMIN_REQUEST_ID_LEN + 4]) {
    unsigned char raw[16]; size_t used=0; int fd=open("/dev/urandom",O_RDONLY|O_CLOEXEC);
    while(fd>=0&&used<sizeof raw){ssize_t n=read(fd,raw+used,sizeof raw-used);if(n>0)used+=(size_t)n;else if(n<0&&errno==EINTR)continue;else break;}
    if(fd>=0)close(fd); if(used!=sizeof raw)return -1;
    memcpy(out,"mc:",3); for(size_t i=0;i<sizeof raw;i++)snprintf(out+3+i*2,3,"%02x",raw[i]); out[35]=0; return 0;
}
static int make_key_cursor_id(char out[ADMIN_REQUEST_ID_LEN + 4]) {
    unsigned char raw[16]; size_t used=0; int fd=open("/dev/urandom",O_RDONLY|O_CLOEXEC);
    while(fd>=0&&used<sizeof raw){ssize_t n=read(fd,raw+used,sizeof raw-used);if(n>0)used+=(size_t)n;else if(n<0&&errno==EINTR)continue;else break;}
    if(fd>=0)close(fd); if(used!=sizeof raw)return -1;
    memcpy(out,"ke:",3); for(size_t i=0;i<sizeof raw;i++)snprintf(out+3+i*2,3,"%02x",raw[i]); out[35]=0; return 0;
}
static int make_private_owner(uint64_t *out) {
    unsigned char raw[8];size_t used=0;int fd=open("/dev/urandom",O_RDONLY|O_CLOEXEC);
    while(fd>=0&&used<sizeof raw){ssize_t n=read(fd,raw+used,sizeof raw-used);if(n>0)used+=(size_t)n;else if(n<0&&errno==EINTR)continue;else break;}
    if(fd>=0)close(fd);if(used!=sizeof raw)return -1;uint64_t owner=0;for(size_t i=0;i<sizeof raw;i++)owner|=(uint64_t)raw[i]<<(i*8);if(!owner)return -1;*out=owner;return 0;
}
static void accept_request_id(const char *value, size_t len) {
    if (!value || !len || len > ADMIN_REQUEST_ID_LEN) return;
    for (size_t i = 0; i < len; i++) {
        unsigned char c = (unsigned char)value[i];
        if (!isalnum(c) && c != '-' && c != '_') return;
    }
    memcpy(request_id, value, len); request_id[len] = 0;
}
static void put(struct out *o, const char *fmt, ...) {
    if (o->full || o->len >= o->cap) { o->full = 1; return; }
    va_list ap; va_start(ap, fmt); int n = vsnprintf(o->p + o->len, o->cap - o->len, fmt, ap); va_end(ap);
    if (n < 0 || (size_t)n >= o->cap - o->len) { o->full = 1; return; }
    o->len += (size_t)n;
}
static int loopback(const char *s) { struct in_addr a; return inet_pton(AF_INET, s, &a) == 1 && (ntohl(a.s_addr) >> 24) == 127; }
static int secret_equal(const unsigned char *a, size_t alen, const unsigned char *b, size_t blen) {
    unsigned int d = (unsigned int)(alen ^ blen); for (size_t i = 0; i < blen; i++) d |= (unsigned int)((i < alen ? a[i] : 0) ^ b[i]); return d == 0;
}
static void wipe(void *p, size_t n) { volatile unsigned char *v = p; while (n--) *v++ = 0; }

/* The audit file is deliberately opened independently of any engine WAL. It
 * never stores payloads or names: only the fixed operation vocabulary and a
 * request identifier are eligible for persistence here. */
static int open_audit_log(const char *path) {
    int fd = open(path, O_WRONLY | O_APPEND | O_CREAT | O_NOFOLLOW | O_CLOEXEC, 0600);
    if (fd < 0) { fprintf(stderr, "admin audit log could not be opened\n"); return -1; }
    struct stat st;
    if (fstat(fd, &st) < 0 || !S_ISREG(st.st_mode) || st.st_uid != geteuid() || (st.st_mode & 0077)) {
        fprintf(stderr, "admin audit log must be a server-owned regular file with mode 0600\n");
        close(fd);
        return -1;
    }
    return fd;
}

static int audit_event(AdminHttp *a, const char *operation, const char *result) {
    pthread_mutex_lock(&a->audit_mu);
    /* Failure injection is test-only and opt-in.  It lets the integration
     * suite exercise both sides of the audit/engine boundary without relying
     * on filesystem permissions after the descriptor has already been opened. */
    const char *fail_after=getenv("KUTTIDB_TEST_ADMIN_AUDIT_FAIL_AFTER");
    if(a->audit_failed)goto failed;
    if(fail_after){char *end=NULL;unsigned long long limit=strtoull(fail_after,&end,10);if(end&&!*end&&a->audit_events++>=limit)goto failed;}
    char line[320];
    int n = snprintf(line, sizeof line,
                     "{\"timestamp\":%llu,\"request_id\":\"%s\",\"operation\":\"%s\",\"principal\":\"admin-token\",\"idempotency_key_hash\":\"%016llx\",\"result\":\"%s\"}\n",
                     (unsigned long long)time(NULL), request_id, operation,
                     (unsigned long long)audit_idempotency_hash, result);
    if (n < 0 || (size_t)n >= sizeof line || a->audit_fd < 0) goto failed;
    size_t off = 0;
    while (off < (size_t)n) {
        ssize_t w = write(a->audit_fd, line + off, (size_t)n - off);
        if (w > 0) off += (size_t)w;
        else if (w < 0 && errno == EINTR) continue;
        else goto failed;
    }
    if (fdatasync(a->audit_fd) == 0) { pthread_mutex_unlock(&a->audit_mu); return 0; }
failed:
    a->audit_failed = 1;
    a->audit_failures++;
    pthread_mutex_unlock(&a->audit_mu);
    return -1;
}
int admin_http_load_token(const char *path, unsigned char *out, size_t *out_len, size_t out_cap) {
    int fd = open(path, O_RDONLY | O_NOFOLLOW); if (fd < 0) { perror("admin token file"); return -1; }
    struct stat st;
    if (fstat(fd, &st) < 0 || !S_ISREG(st.st_mode) || st.st_uid != geteuid()) { fprintf(stderr, "admin token file must be a regular file owned by the server user\n"); close(fd); return -1; }
    if (st.st_mode & 0077) { fprintf(stderr, "admin token file permissions must be 0600\n"); close(fd); return -1; }
    unsigned char raw[1027]; size_t used = 0; ssize_t n = 0;
    while (used < sizeof raw && (n = read(fd, raw + used, sizeof raw - used)) != 0) { if (n < 0) { if (errno == EINTR) continue; close(fd); wipe(raw, sizeof raw); return -1; } used += (size_t)n; }
    close(fd); int over = used == sizeof raw;
    /* A single final CRLF or LF is a line ending; other token bytes are not
     * silently transformed. */
    if (used >= 2 && raw[used - 2] == '\r' && raw[used - 1] == '\n') used -= 2;
    else if (used && raw[used - 1] == '\n') used--;
    if (over || !used || used > out_cap) { fprintf(stderr, "admin token must contain 1..%zu bytes\n", out_cap); wipe(raw, sizeof raw); return -1; }
    memcpy(out, raw, used); *out_len = used; wipe(raw, sizeof raw); return 0;
}
static int utf8(const unsigned char *s, size_t n) { for (size_t i=0;i<n;) { unsigned c=s[i++]; if(c<0x80)continue; if(c>=0xc2&&c<=0xdf){if(i>=n||(s[i++]&0xc0)!=0x80)return 0;}else if(c==0xe0){if(i+1>=n||s[i]<0xa0||s[i]>0xbf||(s[i+1]&0xc0)!=0x80)return 0;i+=2;}else if(c>=0xe1&&c<=0xec){if(i+1>=n||(s[i]&0xc0)!=0x80||(s[i+1]&0xc0)!=0x80)return 0;i+=2;}else if(c==0xed){if(i+1>=n||s[i]<0x80||s[i]>0x9f||(s[i+1]&0xc0)!=0x80)return 0;i+=2;}else if(c>=0xee&&c<=0xef){if(i+1>=n||(s[i]&0xc0)!=0x80||(s[i+1]&0xc0)!=0x80)return 0;i+=2;}else if(c==0xf0){if(i+2>=n||s[i]<0x90||s[i]>0xbf||(s[i+1]&0xc0)!=0x80||(s[i+2]&0xc0)!=0x80)return 0;i+=3;}else if(c>=0xf1&&c<=0xf3){if(i+2>=n||(s[i]&0xc0)!=0x80||(s[i+1]&0xc0)!=0x80||(s[i+2]&0xc0)!=0x80)return 0;i+=3;}else if(c==0xf4){if(i+2>=n||s[i]<0x80||s[i]>0x8f||(s[i+1]&0xc0)!=0x80||(s[i+2]&0xc0)!=0x80)return 0;i+=3;}else return 0;}return 1;}
static void json_string(struct out *o, const unsigned char *s, size_t n) { put(o, "\""); for(size_t i=0;i<n;i++){ unsigned c=s[i]; if(c=='\"'||c=='\\')put(o,"\\%c",c); else if(c=='\b')put(o,"\\b"); else if(c=='\f')put(o,"\\f"); else if(c=='\n')put(o,"\\n"); else if(c=='\r')put(o,"\\r"); else if(c=='\t')put(o,"\\t"); else if(c<32)put(o,"\\u%04x",c); else put(o,"%c",c); } put(o,"\""); }
static const char b64[]="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
static const char b64url[]="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
static void json_b64(struct out *o,const unsigned char*s,size_t n){ put(o,"\""); for(size_t i=0;i<n;i+=3){unsigned x=s[i]<<16; if(i+1<n)x|=s[i+1]<<8; if(i+2<n)x|=s[i+2]; put(o,"%c%c%c%c",b64[(x>>18)&63],b64[(x>>12)&63],i+1<n?b64[(x>>6)&63]:'=',i+2<n?b64[x&63]:'=');}put(o,"\"");}
static void json_b64url_id(struct out *o,const unsigned char*s,size_t n){
    put(o,"\"b64u:");
    for(size_t i=0;i<n;i+=3){unsigned x=s[i]<<16;if(i+1<n)x|=s[i+1]<<8;if(i+2<n)x|=s[i+2];put(o,"%c%c",b64url[(x>>18)&63],b64url[(x>>12)&63]);if(i+1<n)put(o,"%c",b64url[(x>>6)&63]);if(i+2<n)put(o,"%c",b64url[x&63]);}
    put(o,"\"");
}
static void route_id_field(struct out *o,const AdminRoute *route){
    unsigned char raw[4+QUEUE_NAME_MAX+ROUTING_KEY_MAX];
    raw[0]=(unsigned char)route->queue.len;raw[1]=(unsigned char)(route->queue.len>>8);
    memcpy(raw+2,route->queue.bytes,route->queue.len);
    raw[2+route->queue.len]=(unsigned char)route->key.len;
    raw[3+route->queue.len]=(unsigned char)(route->key.len>>8);
    if(route->key.len)memcpy(raw+4+route->queue.len,route->key.bytes,route->key.len);
    put(o,"\"route_id\":");json_b64url_id(o,raw,4u+route->queue.len+route->key.len);
}
/* Public resource names are byte-exact.  The identifier is URL-safe and
 * reversible; display text is intentionally separate from its path form. */
static void identifier_field(struct out *o, const char *field, const AdminName *n) {
    put(o,"\"id\":");json_b64url_id(o,n->bytes,n->len);put(o,",\"");put(o,"%s",field);put(o,"\":");
    if (utf8(n->bytes,n->len)) { json_string(o,n->bytes,n->len); put(o,",\"%s_encoding\":\"utf-8\"",field); }
    else { json_b64(o,n->bytes,n->len); put(o,",\"%s_encoding\":\"base64\"",field); }
}
static void job_json(struct out *o, const AdminJob *job) {
    put(o,"{\"job_id\":\"j-%llu\",\"kind\":\"%s\",\"state\":\"%s\",\"created_at\":%llu",
        (unsigned long long)job->id,job_kind_name(job->kind),job_state_name(job->state),(unsigned long long)job->created_at);
    if(job->completed_at)put(o,",\"completed_at\":%llu",(unsigned long long)job->completed_at);else put(o,",\"completed_at\":null");
    if(job->kind==ADMIN_JOB_STREAM_TRUNCATE||job->kind==ADMIN_JOB_STREAM_DELETE){put(o,",\"stream\":{");identifier_field(o,"name",&job->stream);put(o,"}");if(job->kind==ADMIN_JOB_STREAM_TRUNCATE)put(o,",\"partition\":%u,\"base_offset\":%llu",job->partition,(unsigned long long)job->base_offset);}
    if(job->state==ADMIN_JOB_FAILED)put(o,",\"failure_code\":\"%s\"",job->native_rc==1?"precondition_failed":job->native_rc==2?"conflict":job->native_rc==3?"not_found":job->native_rc==-5?"operation_in_doubt":job->native_rc==-4?"audit_unavailable":"engine_failure");
    put(o,"}");
}
struct qsnap { AdminQueue a[ADMIN_LIST_MAX]; size_t n; int trunc; };
static void qcb(const char *name,uint32_t len,uint64_t d,uint64_t f,void *ud){struct qsnap*s=ud;if(s->n==ADMIN_LIST_MAX){s->trunc=1;return;}AdminQueue*x=&s->a[s->n++];x->name.len=len>ADMIN_NAME_MAX?ADMIN_NAME_MAX:len;memcpy(x->name.bytes,name,x->name.len);x->depth=d;x->inflight=f;}
struct qfind { const AdminName *target; uint64_t depth, inflight; int found; };
static void qfindcb(const char *name,uint32_t len,uint64_t d,uint64_t f,void *ud){struct qfind*x=ud;if(!x->found&&len==x->target->len&&!memcmp(name,x->target->bytes,len)){x->depth=d;x->inflight=f;x->found=1;}}
struct ssnap { AdminStream a[ADMIN_LIST_MAX]; size_t n; int trunc; };
static void scb(const char *name,uint32_t len,uint32_t p,uint64_t b,uint64_t r,void *ud){struct ssnap*s=ud;if(s->n==ADMIN_LIST_MAX){s->trunc=1;return;}AdminStream*x=&s->a[s->n++];x->name.len=len>ADMIN_NAME_MAX?ADMIN_NAME_MAX:len;memcpy(x->name.bytes,name,x->name.len);x->partitions=p;x->bytes=b;x->records=r;}
struct sfind { const AdminName *target; uint32_t partitions; uint64_t bytes, records; int found; };
static void sfindcb(const char *name,uint32_t len,uint32_t p,uint64_t b,uint64_t r,void *ud){struct sfind*x=ud;if(!x->found&&len==x->target->len&&!memcmp(name,x->target->bytes,len)){x->partitions=p;x->bytes=b;x->records=r;x->found=1;}}
struct gsnap { AdminGroup a[ADMIN_LIST_MAX]; size_t n; int trunc; };
static void gcb(const char *t,uint32_t tl,const char*g,uint32_t gl,uint64_t gen,uint32_t mem,void *ud){struct gsnap*s=ud;if(s->n==ADMIN_LIST_MAX){s->trunc=1;return;}AdminGroup*x=&s->a[s->n++];x->stream.len=tl>ADMIN_NAME_MAX?ADMIN_NAME_MAX:tl;x->group.len=gl>ADMIN_NAME_MAX?ADMIN_NAME_MAX:gl;memcpy(x->stream.bytes,t,x->stream.len);memcpy(x->group.bytes,g,x->group.len);x->generation=gen;x->members=mem;}
static void group_member_cb(uint32_t index,uint32_t assigned,uint64_t lease,void *ud){AdminGroupMemberSnapshot*s=ud;if(s->count>=STREAM_GROUP_MEMBERS_MAX)return;s->members[s->count++]=(AdminGroupMember){.index=index,.assigned=assigned,.lease_remaining_ms=lease};}
static void router_cb(const char*n,uint32_t nl,int durable,unsigned type,const char*alternate,uint32_t al,uint32_t routes,uint64_t revision,uint64_t published,uint64_t unroutable,void*ud){AdminRouterSnapshot*s=ud;if(s->count>=ADMIN_LIST_MAX){s->truncated=1;return;}AdminRouter*r=&s->routers[s->count++];r->name.len=nl;r->alternate.len=al;memcpy(r->name.bytes,n,nl);if(al)memcpy(r->alternate.bytes,alternate,al);r->durable=durable;r->type=type;r->routes=routes;r->revision=revision;r->published=published;r->unroutable=unroutable;}
static void route_cb(const char*queue,uint32_t ql,const char*key,uint32_t kl,void*ud){AdminRouteSnapshot*s=ud;if(s->count>=ADMIN_LIST_MAX){s->truncated=1;return;}AdminRoute*r=&s->routes[s->count++];r->queue.len=ql;r->key.len=kl;memcpy(r->queue.bytes,queue,ql);if(kl)memcpy(r->key.bytes,key,kl);}
struct csnap { AdminConsumer a[ADMIN_LIST_MAX]; size_t n; int trunc; };
static void ccb(const char *name,uint32_t len,void *ud){struct csnap*s=ud;if(s->n==ADMIN_LIST_MAX){s->trunc=1;return;}AdminConsumer*x=&s->a[s->n++];x->name.len=len>ADMIN_NAME_MAX?ADMIN_NAME_MAX:len;memcpy(x->name.bytes,name,x->name.len);}
static const char *header(const char *h,size_t n,const char *name,size_t *len){size_t nl=strlen(name);for(size_t i=0;i+nl<n;){const char*e=memchr(h+i,'\n',n-i);if(!e)return NULL;size_t z=(size_t)(e-(h+i));if(z>nl&&strncasecmp(h+i,name,nl)==0&&h[i+nl]==':'){size_t a=i+nl+1,b=i+z;while(a<b&&(h[a]==' '||h[a]=='\t'))a++;while(b>a&&(h[b-1]=='\r'||h[b-1]==' '||h[b-1]=='\t'))b--;*len=b-a;return h+a;}i=(size_t)(e-h)+1;}return NULL;}
static int origin_allowed(AdminHttp *a,const char *origin,size_t n){for(size_t i=0;i<a->c.allow_origin_count;i++)if(strlen(a->c.allow_origins[i])==n&&memcmp(a->c.allow_origins[i],origin,n)==0)return 1;return 0;}
static int write_all(int fd, void *ssl, const char *p,size_t n){(void)ssl;size_t o=0;while(o<n){ssize_t w;
#ifdef HAVE_OPENSSL
if(ssl)w=SSL_write((SSL*)ssl,p+o,(int)(n-o));else
#endif
w=send(fd,p+o,n-o,MSG_NOSIGNAL);if(w<0&&errno==EINTR)continue;if(w<=0)return -1;o+=(size_t)w;}return 0;}
static ssize_t read_one(int fd, void *ssl, char *p,size_t n){(void)ssl;
#ifdef HAVE_OPENSSL
if(ssl)return SSL_read((SSL*)ssl,p,(int)n);
#endif
return recv(fd,p,n,0);}
static void reply_extra(AdminHttp*a,int fd,void*ssl,const char*status,const char*body,size_t blen,int head,const char*origin,int www,const char*extra){(void)a;char h[1024];int n=snprintf(h,sizeof h,"HTTP/1.1 %s\r\nContent-Type: application/json; charset=utf-8\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nReferrer-Policy: no-referrer\r\nX-KuttiDB-Request-ID: %s\r\nContent-Length: %zu\r\nConnection: close\r\n%s%s%s%s%s\r\n",status,request_id[0]?request_id:"00000000000000000000000000000000",blen,www?"WWW-Authenticate: Bearer\r\n":"",extra?extra:"",origin?"Access-Control-Allow-Origin: ":"",origin?origin:"",origin?"\r\nAccess-Control-Allow-Methods: GET, HEAD, POST, PUT, PATCH, DELETE, OPTIONS\r\nAccess-Control-Allow-Headers: Authorization, Content-Type, Idempotency-Key, If-Match, X-KuttiDB-Confirm, X-KuttiDB-Request-ID\r\nVary: Origin\r\n":"");if(n>0&&(size_t)n<sizeof h)write_all(fd,ssl,h,(size_t)n);if(!head&&blen)write_all(fd,ssl,body,blen);}
static void reply(AdminHttp*a,int fd,void*ssl,const char*status,const char*body,size_t blen,int head,const char*origin,int www){reply_extra(a,fd,ssl,status,body,blen,head,origin,www,NULL);}
static size_t error(struct out*o,const char*code,const char*msg){put(o,"{\"error\":{\"code\":\"");put(o,"%s",code);put(o,"\",\"message\":\"");put(o,"%s",msg);put(o,"\",\"request_id\":\"");put(o,"%s",request_id[0]?request_id:"00000000000000000000000000000000");put(o,"\"}}");return o->len;}
/* Legacy bounded scans do not retain an engine cursor, but still report the
 * process-lifetime Management mutation epoch instead of the old sentinel 0.
 * Clients can therefore detect a changed scan and refresh safely. */
static size_t collection_end(struct out*o,size_t count,unsigned limit,int trunc){put(o,"],\"meta\":{\"count\":%zu,\"limit\":%u,\"next_cursor\":null,\"snapshot_revision\":%llu,\"weakly_consistent\":%s}}",count,limit,(unsigned long long)collection_snapshot_revision,trunc?"true":"false");return o->len;}
static int limit_from(const char *path,size_t n,unsigned*out){*out=100;const char*q=memchr(path,'?',n);if(!q)return 0;size_t l=n-(size_t)(q+1-path);if(l<6||memcmp(q+1,"limit=",6)||memchr(q+7,'&',l-6))return -1;char z[12];size_t d=l-6;if(!d||d>=sizeof z)return -1;memcpy(z,q+7,d);z[d]=0;char*e;unsigned long v=strtoul(z,&e,10);if(*e||v<1||v>500)return -1;*out=(unsigned)v;return 0;}
static uint64_t admin_mono_ms(void);
static void render(AdminHttp*a,const char*path,size_t plen,struct out*o,int*service){unsigned limit;if(limit_from(path,plen,&limit)<0){error(o,"bad_request","The limit parameter is invalid.");return;}size_t base=plen;const char*q=memchr(path,'?',plen);if(q)base=(size_t)(q-path);AdminHttpStatus st={0};a->c.status(a->c.status_ud,&st);uint64_t active_deliveries=0,active_claims=0,active_tails=0,queued_jobs=0,running_jobs=0,now=admin_mono_ms();for(size_t i=0;i<ADMIN_DELIVERY_LIMIT;i++){if(a->deliveries[i].active)active_deliveries++;if(a->claims[i].active&&a->claims[i].deadline_ms>now)active_claims++;}pthread_mutex_lock(&a->tails_mu);active_tails=a->active_tails;pthread_mutex_unlock(&a->tails_mu);job_counts(a,&queued_jobs,&running_jobs);*service=!st.ready;
if(base==26&&!memcmp(path,"/api/admin/v1/capabilities",26)){put(o,"{\"product\":\"KuttiDB\",\"server_version\":\"0.2.0\",\"management_api_version\":\"v1\",\"enabled_engines\":[\"keyspaces\",\"queues\",\"streams\"],\"tls_available\":%s,\"persistence\":{\"keyspaces\":%s,\"queues\":%s,\"streams\":%s},\"resources\":[\"capabilities\",\"status\",\"keyspaces\",\"queues\",\"streams\",\"consumer-groups\",\"maintenance\"]}",st.tls_available?"true":"false",st.keyspace_persistence_failed?"false":"true",st.queue_persistence_failed?"false":"true",st.stream_persistence_failed?"false":"true");return;}
if(base==20&&!memcmp(path,"/api/admin/v1/status",20)){put(o,"{\"uptime_seconds\":%llu,\"ready\":%s,\"server_version\":\"0.2.0\",\"event_loops\":%d,\"event_backend\":\"%s\",\"connected_clients\":%llu,\"rejected_clients\":%llu,\"admin_authentication_failures\":%llu,\"durability\":\"%s\",\"keyspace\":{\"entry_count\":%llu,\"live_bytes\":%llu,\"allocated_bytes\":%llu,\"expired_count\":%llu,\"evicted_count\":%llu},\"queues\":{\"count\":%llu,\"ready_depth\":%llu,\"in_flight\":%llu,\"redelivery_count\":%llu,\"dead_letter_count\":%llu,\"persistence_healthy\":%s},\"streams\":{\"count\":%llu,\"partition_count\":%llu,\"retained_bytes\":%llu,\"group_count\":%llu,\"member_count\":%llu,\"persistence_healthy\":%s},\"management\":{\"admin_connections\":1,\"active_tails\":%llu,\"active_deliveries\":%llu,\"active_claims\":%llu,\"queued_jobs\":%llu,\"running_jobs\":%llu,\"mutation_attempts\":%llu,\"audit_failures\":%llu,\"rate_limit_rejections\":%llu,\"operation_in_doubt\":%llu},\"audit\":{\"healthy\":%s},\"persistence_healthy\":%s}",(unsigned long long)st.uptime_seconds,st.ready?"true":"false",st.event_loops,st.event_backend?st.event_backend:"unknown",(unsigned long long)st.connections,(unsigned long long)st.rejected_connections,(unsigned long long)st.auth_failures,st.durability?st.durability:"periodic",(unsigned long long)st.keyspace_entries,(unsigned long long)st.keyspace_live_bytes,(unsigned long long)st.keyspace_allocated_bytes,(unsigned long long)st.keyspace_expired,(unsigned long long)st.keyspace_evicted,(unsigned long long)st.queue_count,(unsigned long long)st.queue_ready,(unsigned long long)st.queue_inflight,(unsigned long long)st.queue_redeliveries,(unsigned long long)st.queue_deadletters,st.queue_persistence_failed?"false":"true",(unsigned long long)st.stream_count,(unsigned long long)st.stream_partitions,(unsigned long long)st.stream_retained_bytes,(unsigned long long)st.stream_groups,(unsigned long long)st.stream_members,st.stream_persistence_failed?"false":"true",(unsigned long long)active_tails,(unsigned long long)active_deliveries,(unsigned long long)active_claims,(unsigned long long)queued_jobs,(unsigned long long)running_jobs,(unsigned long long)a->mutation_attempts,(unsigned long long)a->audit_failures,(unsigned long long)a->rate_limit_rejections,(unsigned long long)a->operation_in_doubt,a->audit_failed?"false":"true",st.ready?"true":"false");return;}
if(base==25&&!memcmp(path,"/api/admin/v1/maintenance",25)){put(o,"{\"data\":[{\"engine\":\"keyspace\",\"checkpoint_available\":%s},{\"engine\":\"queue\",\"checkpoint_available\":%s},{\"engine\":\"stream\",\"checkpoint_available\":%s}],\"meta\":{\"count\":3,\"limit\":%u,\"next_cursor\":null,\"snapshot_revision\":0,\"weakly_consistent\":false}}",a->c.keyspace_checkpoint&&!st.keyspace_persistence_failed?"true":"false",!st.queue_persistence_failed?"true":"false",!st.stream_persistence_failed?"true":"false",limit);return;}
if(base==23&&!memcmp(path,"/api/admin/v1/keyspaces",23)){put(o,"{\"data\":[{\"name\":\"default\",\"entry_count\":%llu,\"live_bytes\":%llu,\"allocated_bytes\":%llu,\"expired_count\":%llu,\"evicted_count\":%llu,\"persistence_healthy\":%s}],\"meta\":{\"count\":1,\"limit\":%u,\"truncated\":false}}",(unsigned long long)st.keyspace_entries,(unsigned long long)st.keyspace_live_bytes,(unsigned long long)st.keyspace_allocated_bytes,(unsigned long long)st.keyspace_expired,(unsigned long long)st.keyspace_evicted,st.keyspace_persistence_failed?"false":"true",limit);return;}
if(base==20&&!memcmp(path,"/api/admin/v1/queues",20)){struct qsnap*s=calloc(1,sizeof *s);if(!s){error(o,"internal_error","The server could not complete the request.");return;}queue_foreach_stats(a->c.queues,qcb,s);put(o,"{\"data\":[");size_t n=s->n<limit?s->n:limit;for(size_t i=0;i<n;i++){if(i)put(o,",");put(o,"{");identifier_field(o,"name",&s->a[i].name);put(o,",\"ready_depth\":%llu,\"in_flight\":%llu}",(unsigned long long)s->a[i].depth,(unsigned long long)s->a[i].inflight);}collection_end(o,n,limit,s->trunc||s->n>limit);free(s);return;}
if(base==21&&!memcmp(path,"/api/admin/v1/streams",21)){struct ssnap*s=calloc(1,sizeof *s);if(!s){error(o,"internal_error","The server could not complete the request.");return;}stream_foreach_stats(a->c.streams,scb,s);put(o,"{\"data\":[");size_t n=s->n<limit?s->n:limit;for(size_t i=0;i<n;i++){if(i)put(o,",");put(o,"{");identifier_field(o,"name",&s->a[i].name);put(o,",\"partition_count\":%u,\"retained_bytes\":%llu,\"retained_record_count\":%llu}",s->a[i].partitions,(unsigned long long)s->a[i].bytes,(unsigned long long)s->a[i].records);}collection_end(o,n,limit,s->trunc||s->n>limit);free(s);return;}
if(base==29&&!memcmp(path,"/api/admin/v1/consumer-groups",29)){struct gsnap*s=calloc(1,sizeof *s);if(!s){error(o,"internal_error","The server could not complete the request.");return;}stream_group_foreach_stats(a->c.streams,gcb,s);put(o,"{\"data\":[");size_t n=s->n<limit?s->n:limit;for(size_t i=0;i<n;i++){if(i)put(o,",");put(o,"{");identifier_field(o,"stream",&s->a[i].stream);put(o,",");identifier_field(o,"group",&s->a[i].group);put(o,",\"generation\":%llu,\"active_member_count\":%u}",(unsigned long long)s->a[i].generation,s->a[i].members);}collection_end(o,n,limit,s->trunc||s->n>limit);free(s);return;}error(o,"not_found","The requested resource was not found.");}
static int route_ok(const char *p,size_t n){const char*q=memchr(p,'?',n);if(q)n=(size_t)(q-p);return (n==26&&!memcmp(p,"/api/admin/v1/capabilities",n))||(n==20&&!memcmp(p,"/api/admin/v1/status",n))||(n==25&&!memcmp(p,"/api/admin/v1/maintenance",n))||(n==23&&!memcmp(p,"/api/admin/v1/keyspaces",n))||(n==20&&!memcmp(p,"/api/admin/v1/queues",n))||(n==21&&!memcmp(p,"/api/admin/v1/streams",n))||(n==29&&!memcmp(p,"/api/admin/v1/consumer-groups",n));}
static void handle(AdminHttp*a,int fd,void*ssl){char req[ADMIN_REQ_MAX];size_t used=0;for(;;){if(used==sizeof req){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"431 Request Header Fields Too Large",b,error(&o,"request_too_large","Request headers are too large."),0,NULL,0);return;}ssize_t z=read_one(fd,ssl,req+used,sizeof req-used);if(z<=0)return;used+=(size_t)z;if(memmem(req,used,"\r\n\r\n",4))break;}char*e=memmem(req,used,"\r\n\r\n",4);size_t hn=(size_t)(e-req)+4;char*sp1=memchr(req,' ',hn);if(!sp1){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"400 Bad Request",b,error(&o,"bad_request","The HTTP request is malformed."),0,NULL,0);return;}char*path=sp1+1;char*sp2=memchr(path,' ',hn-(size_t)(path-req));if(!sp2||sp2==path){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"400 Bad Request",b,error(&o,"bad_request","The HTTP request is malformed."),0,NULL,0);return;}size_t ml=(size_t)(sp1-req),pl=(size_t)(sp2-path);int is_get=ml==3&&!memcmp(req,"GET",3),is_head=ml==4&&!memcmp(req,"HEAD",4),is_opt=ml==7&&!memcmp(req,"OPTIONS",7);size_t vl=0;const char*cl=header(req,hn,"content-length",&vl);if((cl&&!(vl==1&&cl[0]=='0'))||used>hn){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"413 Payload Too Large",b,error(&o,"request_too_large","Request bodies are not supported."),0,NULL,0);return;}const char*tok=header(req,hn,"authorization",&vl);int auth=tok&&vl>7&&!strncasecmp(tok,"Bearer ",7)&&secret_equal((const unsigned char*)tok+7,vl-7,a->c.token,a->c.token_len);if(!auth){if(a->c.auth_failure)a->c.auth_failure(a->c.auth_failure_ud);char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"401 Unauthorized",b,error(&o,"unauthorized","Authentication is required."),0,NULL,1);return;}size_t ol=0;const char*origin=header(req,hn,"origin",&ol);char origin_copy[512];const char*cors=NULL;if(origin){if(ol>=sizeof origin_copy||!origin_allowed(a,origin,ol)){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"403 Forbidden",b,error(&o,"forbidden_origin","The origin is not allowed."),0,NULL,0);return;}memcpy(origin_copy,origin,ol);origin_copy[ol]=0;cors=origin_copy;}if(!route_ok(path,pl)){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"404 Not Found",b,error(&o,"not_found","The requested resource was not found."),is_head,cors,0);return;}unsigned parsed_limit;if(limit_from(path,pl,&parsed_limit)<0){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"400 Bad Request",b,error(&o,"bad_request","The limit parameter is invalid."),is_head,cors,0);return;}if(is_opt){char b[4]="{}";reply(a,fd,ssl,"204 No Content",b,0,1,cors,0);return;}if(!is_get&&!is_head){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"405 Method Not Allowed",b,error(&o,"method_not_allowed","This method is not supported."),0,cors,0);return;}char*body=malloc(ADMIN_BODY_MAX);if(!body){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"500 Internal Server Error",b,error(&o,"internal_error","The server could not complete the request."),is_head,cors,0);return;}struct out o={body,ADMIN_BODY_MAX,0,0};int unavailable=0;render(a,path,pl,&o,&unavailable);if(o.full){o.len=0;o.full=0;error(&o,"internal_error","The server could not complete the request.");reply(a,fd,ssl,"500 Internal Server Error",body,o.len,is_head,cors,0);}else if(unavailable)reply(a,fd,ssl,"503 Service Unavailable",body,o.len,is_head,cors,0);else reply(a,fd,ssl,"200 OK",body,o.len,is_head,cors,0);free(body);}
static uint64_t request_fingerprint(const char *method, const char *path, const char *body, size_t body_len) {
    uint64_t h = 1469598103934665603ULL;
    const char *parts[2] = {method, path};
    for (unsigned p = 0; p < 2; p++) for (const unsigned char *s = (const unsigned char *)parts[p]; *s; s++) { h ^= *s; h *= 1099511628211ULL; }
    for (size_t i = 0; i < body_len; i++) { h ^= (unsigned char)body[i]; h *= 1099511628211ULL; }
    return h;
}

static int idempotency_lookup(AdminHttp *a, const char *key, size_t key_len, uint64_t fingerprint,
                              const char **response, size_t *response_len, int *status) {
    uint64_t now = (uint64_t)time(NULL);
    for (size_t i = 0; i < sizeof a->idempotency / sizeof a->idempotency[0]; i++) {
        AdminIdempotency *entry = &a->idempotency[i];
        if (!entry->expires_at || entry->expires_at < now) { entry->expires_at = 0; continue; }
        if (strlen(entry->key) == key_len && !memcmp(entry->key, key, key_len)) {
            if (entry->fingerprint != fingerprint) return -1;
            *response = entry->response; *response_len = entry->response_len; *status = entry->status;
            return 1;
        }
    }
    return 0;
}

static void idempotency_store(AdminHttp *a, const char *key, size_t key_len, uint64_t fingerprint,
                              const char *response, size_t response_len, int status) {
    if (key_len > 128 || response_len >= sizeof a->idempotency[0].response) return;
    uint64_t now=(uint64_t)time(NULL), oldest=UINT64_MAX; size_t slot=0;
    for(size_t i=0;i<sizeof a->idempotency/sizeof a->idempotency[0];i++){
        AdminIdempotency *candidate=&a->idempotency[i];
        if(candidate->expires_at<=now){slot=i;break;}
        if(strlen(candidate->key)==key_len&&!memcmp(candidate->key,key,key_len)){slot=i;break;}
        if(candidate->expires_at<oldest){oldest=candidate->expires_at;slot=i;}
    }
    AdminIdempotency *entry = &a->idempotency[slot];
    memcpy(entry->key, key, key_len); entry->key[key_len] = 0;
    entry->fingerprint = fingerprint; entry->expires_at = (uint64_t)time(NULL) + 600;
    memcpy(entry->response, response, response_len); entry->response_len = response_len; entry->status = status;
}

static int decode_id(const char *text, size_t text_len, AdminName *out) {
    static const char prefix[] = "b64u:";
    if (text_len <= sizeof prefix - 1 || memcmp(text, prefix, sizeof prefix - 1)) return -1;
    size_t decoded = 0;
    if (admin_base64_decode(text + sizeof prefix - 1, text_len - (sizeof prefix - 1), 1,
                            out->bytes, sizeof out->bytes, &decoded) || !decoded) return -1;
    out->len = (uint32_t)decoded;
    return 0;
}

/* Route IDs are a reversible canonical encoding of the exact Queue and
 * routing-key tuple. Unlike a list index, they stay valid across pagination,
 * restart, and unrelated route mutations; the router ETag still protects a
 * destructive request from stale topology. */
static int decode_route_id(const char *text, size_t text_len, AdminRoute *out) {
    static const char prefix[]="b64u:";
    unsigned char raw[4+QUEUE_NAME_MAX+ROUTING_KEY_MAX];size_t used=0;
    if(!out||text_len<=sizeof prefix-1||memcmp(text,prefix,sizeof prefix-1)||
       admin_base64_decode(text+sizeof prefix-1,text_len-(sizeof prefix-1),1,
                           raw,sizeof raw,&used)||used<4)return-1;
    uint32_t ql=(uint32_t)raw[0]|((uint32_t)raw[1]<<8);
    if(!ql||ql>QUEUE_NAME_MAX||used<2u+ql+2u)return-1;
    uint32_t kl=(uint32_t)raw[2+ql]|((uint32_t)raw[3+ql]<<8);
    if(kl>ROUTING_KEY_MAX||used!=4u+ql+kl)return-1;
    out->queue.len=ql;memcpy(out->queue.bytes,raw+2,ql);
    out->key.len=kl;if(kl)memcpy(out->key.bytes,raw+4+ql,kl);
    return 0;
}

static int router_route_path(const char *path,size_t path_len,AdminName *router,
                             AdminRoute *route){
    static const char prefix[]="/api/admin/v1/routing/routers/",marker[]="/routes/";
    const char*query=memchr(path,'?',path_len);size_t end=query?(size_t)(query-path):path_len,
        prefix_len=sizeof prefix-1,marker_len=sizeof marker-1;
    if(end<=prefix_len+marker_len||memcmp(path,prefix,prefix_len))return-1;
    const char*mark=memmem(path+prefix_len,end-prefix_len,marker,marker_len);
    if(!mark||decode_id(path+prefix_len,(size_t)(mark-(path+prefix_len)),router))return-1;
    return decode_route_id(mark+marker_len,end-(size_t)(mark+marker_len-path),route);
}

static int path_id_after(const char *path, size_t path_len, const char *prefix, AdminName *out) {
    size_t prefix_len = strlen(prefix);
    const char *query = memchr(path, '?', path_len);
    size_t end = query ? (size_t)(query - path) : path_len;
    if (end <= prefix_len || memcmp(path, prefix, prefix_len) || memchr(path + prefix_len, '/', end - prefix_len)) return -1;
    return decode_id(path + prefix_len, end - prefix_len, out);
}

static int path_id_action(const char *path, size_t path_len, const char *prefix,
                          const char *action, AdminName *out) {
    size_t prefix_len = strlen(prefix), action_len = strlen(action);
    const char *query = memchr(path, '?', path_len);
    size_t end = query ? (size_t)(query - path) : path_len;
    if (end <= prefix_len + action_len || memcmp(path, prefix, prefix_len) ||
        memcmp(path + end - action_len, action, action_len)) return -1;
    return decode_id(path + prefix_len, end - prefix_len - action_len, out);
}

static int queue_message_path(const char *path,size_t path_len,AdminName *queue,
                              uint64_t *message_id){
    static const char prefix[]="/api/admin/v1/queues/",marker[]="/messages/";
    const char *query=memchr(path,'?',path_len);size_t end=query?(size_t)(query-path):path_len,
        prefix_len=sizeof prefix-1,marker_len=sizeof marker-1;
    if(end<=prefix_len+marker_len||memcmp(path,prefix,prefix_len))return-1;
    const char*mark=memmem(path+prefix_len,end-prefix_len,marker,marker_len);
    if(!mark||decode_id(path+prefix_len,(size_t)(mark-(path+prefix_len)),queue))return-1;
    const char *number=mark+marker_len;size_t n=end-(size_t)(number-path);char text[32],*tail;
    if(!n||n>=sizeof text)return-1;memcpy(text,number,n);text[n]=0;errno=0;unsigned long long id=strtoull(text,&tail,10);
    if(errno||!*text||*tail||!id)return-1;*message_id=id;return 0;
}

static uint64_t admin_mono_ms(void) {
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) == 0) return (uint64_t)ts.tv_sec * 1000u + (uint64_t)ts.tv_nsec / 1000000u;
    return (uint64_t)time(NULL) * 1000u;
}

static void reap_deliveries(AdminHttp *a) {
    uint64_t now = admin_mono_ms();
    for (size_t i = 0; i < ADMIN_DELIVERY_LIMIT; i++) {
        AdminDelivery *d = &a->deliveries[i];
        if (!d->active || d->deadline_ms > now) continue;
        (void)queue_nack_for_owner(a->c.queues, (const char *)d->queue.bytes, d->queue.len, d->tag, d->owner, 1);
        d->active = 0; d->expired = 1;
    }
}

static void release_deliveries(AdminHttp *a) {
    for (size_t i = 0; i < ADMIN_DELIVERY_LIMIT; i++) {
        AdminDelivery *d = &a->deliveries[i];
        if (!d->active) continue;
        queue_requeue_owner(a->c.queues, d->owner);
        d->active = 0; d->expired = 1;
    }
}

static void reap_claims(AdminHttp *a) {
    uint64_t now=admin_mono_ms();
    for(size_t i=0;i<ADMIN_DELIVERY_LIMIT;i++){AdminClaim*c=&a->claims[i];if(!c->active||c->deadline_ms>now)continue;if(a->c.keyspace_claim_release)(void)a->c.keyspace_claim_release((const char *)c->key.bytes,c->key.len,c->owner);c->active=0;c->expired=1;}
}

static void release_claims(AdminHttp *a) {
    for(size_t i=0;i<ADMIN_DELIVERY_LIMIT;i++){AdminClaim*c=&a->claims[i];if(!c->active)continue;if(a->c.keyspace_claim_release)(void)a->c.keyspace_claim_release((const char *)c->key.bytes,c->key.len,c->owner);c->active=0;c->expired=1;}
}

static void reap_group_sessions(AdminHttp *a) {
    uint64_t now = admin_mono_ms();
    for (size_t i = 0; i < ADMIN_DELIVERY_LIMIT; i++) {
        AdminGroupSession *s = &a->group_sessions[i];
        if (!s->active || s->deadline_ms > now) continue;
        (void)stream_group_leave_member(a->c.streams, (const char *)s->stream.bytes,
                                        s->stream.len, (const char *)s->group.bytes,
                                        s->group.len, s->owner);
        s->active = 0; s->expired = 1;
    }
}

static void release_group_sessions(AdminHttp *a) {
    for (size_t i = 0; i < ADMIN_DELIVERY_LIMIT; i++) {
        AdminGroupSession *s = &a->group_sessions[i];
        if (!s->active) continue;
        (void)stream_group_leave_member(a->c.streams, (const char *)s->stream.bytes,
                                        s->stream.len, (const char *)s->group.bytes,
                                        s->group.len, s->owner);
        s->active = 0; s->expired = 1;
    }
}

static AdminDelivery *delivery_slot(AdminHttp *a) {
    reap_deliveries(a);
    size_t limit = a->session_limit < ADMIN_DELIVERY_LIMIT ? a->session_limit : ADMIN_DELIVERY_LIMIT;
    for (size_t i = 0; i < limit; i++) if (!a->deliveries[i].active && !a->deliveries[i].expired) return &a->deliveries[i];
    for (size_t i = 0; i < limit; i++) if (!a->deliveries[i].active) return &a->deliveries[i];
    return NULL;
}

/* Reserve distinct registry slots before a batch consume changes Queue state.
 * Slots remain inactive until the native delivery batch has durably completed. */
static unsigned delivery_slots(AdminHttp *a, AdminDelivery **out, unsigned wanted) {
    if (!a || !out || !wanted) return 0;
    reap_deliveries(a);
    unsigned limit=a->session_limit<ADMIN_DELIVERY_LIMIT?a->session_limit:ADMIN_DELIVERY_LIMIT;
    unsigned found=0;
    for(unsigned i=0;i<limit&&found<wanted;i++)if(!a->deliveries[i].active)out[found++]=&a->deliveries[i];
    return found;
}

static AdminGroupSession *group_session_slot(AdminHttp *a) {
    reap_group_sessions(a);
    size_t limit = a->session_limit < ADMIN_DELIVERY_LIMIT ? a->session_limit : ADMIN_DELIVERY_LIMIT;
    for (size_t i = 0; i < limit; i++) if (!a->group_sessions[i].active && !a->group_sessions[i].expired) return &a->group_sessions[i];
    for (size_t i = 0; i < limit; i++) if (!a->group_sessions[i].active) return &a->group_sessions[i];
    return NULL;
}

static AdminClaim *claim_slot(AdminHttp *a) {
    reap_claims(a);size_t limit=a->session_limit<ADMIN_DELIVERY_LIMIT?a->session_limit:ADMIN_DELIVERY_LIMIT;
    for(size_t i=0;i<limit;i++)if(!a->claims[i].active&&!a->claims[i].expired)return &a->claims[i];
    for(size_t i=0;i<limit;i++)if(!a->claims[i].active)return &a->claims[i];
    return NULL;
}

/* Return 1 for a live delivery, -2 for a known expired delivery, and 0 for
 * an unknown or wrong-Queue identifier. */
static int path_delivery(AdminHttp *a, const char *path, size_t path_len, const char *suffix,
                         AdminDelivery **out) {
    static const char prefix[] = "/api/admin/v1/queues/";
    static const char marker_text[] = "/deliveries/";
    size_t prefix_len=sizeof prefix-1, marker_len=sizeof marker_text-1, suffix_len=strlen(suffix);
    const char *query=memchr(path,'?',path_len); size_t end=query?(size_t)(query-path):path_len;
    if(end<=prefix_len+marker_len+suffix_len || memcmp(path,prefix,prefix_len) || memcmp(path+end-suffix_len,suffix,suffix_len))return 0;
    const char *marker=memmem(path+prefix_len,end-prefix_len,marker_text,marker_len); if(!marker)return 0;
    AdminName queue; if(decode_id(path+prefix_len,(size_t)(marker-(path+prefix_len)),&queue))return 0;
    const char *id=marker+marker_len; size_t id_len=end-suffix_len-(size_t)(id-path); if(!id_len||id_len>=sizeof a->deliveries[0].id)return 0;
    reap_deliveries(a);
    for(size_t i=0;i<ADMIN_DELIVERY_LIMIT;i++){AdminDelivery*d=&a->deliveries[i];if(strlen(d->id)!=id_len||memcmp(d->id,id,id_len)||d->queue.len!=queue.len||memcmp(d->queue.bytes,queue.bytes,queue.len))continue;if(d->active){*out=d;return 1;}if(d->expired)return -2;}
    return 0;
}

/* Resolve only an opaque delivery that belongs to this exact Queue.  Batch
 * actions use this before touching the Queue engine so a malformed, expired,
 * cross-Queue, or duplicate entry can never make a partially applied batch. */
static int delivery_id_lookup(AdminHttp *a, const AdminName *queue,
                              const AdminJsonSlice *id, AdminDelivery **out) {
    if (!a || !queue || !id || !id->len || id->len >= sizeof a->deliveries[0].id)
        return 0;
    reap_deliveries(a);
    for (size_t i=0;i<ADMIN_DELIVERY_LIMIT;i++) {
        AdminDelivery *d=&a->deliveries[i];
        if (strlen(d->id)!=id->len || memcmp(d->id,id->data,id->len) ||
            d->queue.len!=queue->len || memcmp(d->queue.bytes,queue->bytes,queue->len))
            continue;
        if (d->active) { *out=d; return 1; }
        if (d->expired) return -2;
    }
    return 0;
}

static int claim_path(AdminHttp *a, const char *path, size_t path_len,
                      const char *suffix, AdminClaim **out) {
    static const char prefix[]="/api/admin/v1/keyspaces/default/claims/";
    size_t prefix_len=sizeof prefix-1,suffix_len=strlen(suffix);const char *query=memchr(path,'?',path_len);size_t end=query?(size_t)(query-path):path_len;
    if(end<=prefix_len+suffix_len||memcmp(path,prefix,prefix_len)||memcmp(path+end-suffix_len,suffix,suffix_len))return 0;
    const char *id=path+prefix_len;size_t id_len=end-prefix_len-suffix_len;if(!id_len||id_len>=sizeof a->claims[0].id)return 0;
    reap_claims(a);for(size_t i=0;i<ADMIN_DELIVERY_LIMIT;i++){AdminClaim*c=&a->claims[i];if(strlen(c->id)!=id_len||memcmp(c->id,id,id_len))continue;if(c->active){*out=c;return 1;}if(c->expired)return -2;}return 0;
}

static int query_u64(const char *path, size_t path_len, const char *field, uint64_t *out) {
    const char *p=memchr(path,'?',path_len), *end=path+path_len; size_t field_len=strlen(field);
    if(!p)return 0; p++;
    while(p<end){const char *amp=memchr(p,'&',(size_t)(end-p));const char *part_end=amp?amp:end; if((size_t)(part_end-p)>field_len+1&&!memcmp(p,field,field_len)&&p[field_len]=='='){char tmp[32],*ep;size_t len=(size_t)(part_end-(p+field_len+1));if(!len||len>=sizeof tmp)return -1;memcpy(tmp,p+field_len+1,len);tmp[len]=0;errno=0;unsigned long long n=strtoull(tmp,&ep,10);if(errno||*ep)return -1;*out=n;return 1;}p=amp?amp+1:end;}
    return 0;
}

/* Queue browsing deliberately accepts a small, closed query grammar.  It
 * avoids URL decoding because every accepted value is ASCII and prevents a
 * silently ignored filter from showing an operator the wrong messages. */
static int queue_browse_params(const char *path, size_t path_len, unsigned *limit,
                               unsigned *states, int *include_bodies,
                               int *state_supplied, int *include_supplied,
                               char cursor[ADMIN_REQUEST_ID_LEN + 4],
                               int *cursor_supplied) {
    const char *p = memchr(path, '?', path_len), *end = path + path_len;
    int seen_limit = 0, seen_state = 0, seen_include = 0, seen_cursor = 0;
    *limit = 100; *states = QUEUE_PEEK_ALL; *include_bodies = 0;
    *state_supplied = 0; *include_supplied = 0; *cursor_supplied = 0; cursor[0] = 0;
    if (!p) return 0;
    for (p++; p < end;) {
        const char *amp = memchr(p, '&', (size_t)(end - p));
        const char *part_end = amp ? amp : end;
        const char *eq = memchr(p, '=', (size_t)(part_end - p));
        if (!eq || eq == p || eq + 1 == part_end) return -1;
        size_t key_len = (size_t)(eq - p), value_len = (size_t)(part_end - eq - 1);
        const char *value = eq + 1;
        if (key_len == 5 && !memcmp(p, "limit", 5) && !seen_limit++) {
            char number[12], *number_end;
            if (value_len >= sizeof number) return -1;
            memcpy(number, value, value_len); number[value_len] = 0;
            errno = 0; unsigned long parsed = strtoul(number, &number_end, 10);
            if (errno || *number_end || !parsed || parsed > ADMIN_LIST_MAX) return -1;
            *limit = (unsigned)parsed;
        } else if (key_len == 5 && !memcmp(p, "state", 5) && !seen_state++) {
            if (value_len == 5 && !memcmp(value, "ready", 5)) *states = QUEUE_PEEK_READY;
            else if (value_len == 7 && !memcmp(value, "delayed", 7)) *states = QUEUE_PEEK_DELAYED;
            else if (value_len == 9 && !memcmp(value, "in-flight", 9)) *states = QUEUE_PEEK_INFLIGHT;
            else return -1;
            *state_supplied = 1;
        } else if (key_len == 7 && !memcmp(p, "include", 7) && !seen_include++) {
            if (value_len != 4 || memcmp(value, "body", 4)) return -1;
            *include_bodies = 1;
            *include_supplied = 1;
        } else if (key_len == 6 && !memcmp(p, "cursor", 6) && !seen_cursor++) {
            if (value_len != ADMIN_REQUEST_ID_LEN + 3 || memcmp(value, "mc:", 3)) return -1;
            for (size_t i = 3; i < value_len; i++)
                if (!isxdigit((unsigned char)value[i])) return -1;
            memcpy(cursor, value, value_len); cursor[value_len] = 0;
            *cursor_supplied = 1;
        } else return -1;
        p = amp ? amp + 1 : end;
    }
    return 0;
}

static AdminQueueCursor *queue_cursor_find(AdminHttp *a, const char *id) {
    uint64_t now = (uint64_t)time(NULL);
    for (size_t i=0;i<ADMIN_CURSOR_LIMIT;i++) {
        AdminQueueCursor *cursor=&a->queue_cursors[i];
        if (cursor->active && cursor->expires_at <= now) cursor->active=0;
        if (cursor->active && !strcmp(cursor->id,id)) return cursor;
    }
    return NULL;
}

static int queue_cursor_issue(AdminHttp *a, const AdminName *queue,
                              unsigned states, int include_bodies,
                              uint64_t after_id,
                              char out[ADMIN_REQUEST_ID_LEN + 4]) {
    uint64_t now=(uint64_t)time(NULL), oldest=UINT64_MAX; size_t slot=0;
    for(size_t i=0;i<ADMIN_CURSOR_LIMIT;i++) {
        AdminQueueCursor *candidate=&a->queue_cursors[i];
        if(!candidate->active || candidate->expires_at<=now){slot=i;break;}
        if(candidate->expires_at<oldest){oldest=candidate->expires_at;slot=i;}
    }
    AdminQueueCursor *cursor=&a->queue_cursors[slot];
    if(make_queue_cursor_id(cursor->id)) return -1;
    cursor->queue=*queue; cursor->states=states; cursor->include_bodies=include_bodies;
    cursor->after_id=after_id; cursor->expires_at=now+ADMIN_CURSOR_TTL_SECONDS;
    cursor->active=1; memcpy(out,cursor->id,sizeof cursor->id); return 0;
}

static int keyspace_browse_params(const char *path, size_t path_len,
                                  unsigned *limit, AdminName *prefix,
                                  int *prefix_supplied, unsigned char *expiry_filter,
                                  int *expiry_supplied,
                                  char cursor[ADMIN_REQUEST_ID_LEN + 4],
                                  int *cursor_supplied) {
    const char *p=memchr(path,'?',path_len), *end=path+path_len;
    int seen_limit=0, seen_cursor=0, seen_prefix=0, seen_expiry=0; *limit=100; *cursor_supplied=0; cursor[0]=0;prefix->len=0;*prefix_supplied=0;*expiry_filter=0;*expiry_supplied=0;
    if(!p)return 0;
    for(p++;p<end;) {
        const char *amp=memchr(p,'&',(size_t)(end-p)), *part_end=amp?amp:end;
        const char *eq=memchr(p,'=',(size_t)(part_end-p));
        if(!eq||eq==p||eq+1==part_end)return -1;
        size_t key_len=(size_t)(eq-p), value_len=(size_t)(part_end-eq-1);const char*value=eq+1;
        if(key_len==5&&!memcmp(p,"limit",5)&&!seen_limit++){
            char number[12],*number_end;if(value_len>=sizeof number)return -1;memcpy(number,value,value_len);number[value_len]=0;errno=0;unsigned long parsed=strtoul(number,&number_end,10);if(errno||*number_end||!parsed||parsed>ADMIN_LIST_MAX)return -1;*limit=(unsigned)parsed;
        }else if(key_len==6&&!memcmp(p,"cursor",6)&&!seen_cursor++){
            if(value_len!=ADMIN_REQUEST_ID_LEN+3||memcmp(value,"ke:",3))return -1;
            for(size_t i=3;i<value_len;i++)if(!isxdigit((unsigned char)value[i]))return -1;
            memcpy(cursor,value,value_len);cursor[value_len]=0;*cursor_supplied=1;
        }else if(key_len==6&&!memcmp(p,"prefix",6)&&!seen_prefix++){
            if(decode_id(value,value_len,prefix))return -1;*prefix_supplied=1;
        }else if(key_len==7&&!memcmp(p,"expires",7)&&!seen_expiry++){
            if(value_len==7&&!memcmp(value,"present",7))*expiry_filter=1;
            else if(value_len==4&&!memcmp(value,"none",4))*expiry_filter=2;
            else return -1;*expiry_supplied=1;
        }else return -1;
        p=amp?amp+1:end;
    }
    return 0;
}

static AdminKeyCursor *key_cursor_find(AdminHttp *a,const char *id) {
    uint64_t now=(uint64_t)time(NULL);
    for(size_t i=0;i<ADMIN_CURSOR_LIMIT;i++){AdminKeyCursor *cursor=&a->key_cursors[i];if(cursor->active&&cursor->expires_at<=now)cursor->active=0;if(cursor->active&&!strcmp(cursor->id,id))return cursor;}
    return NULL;
}

static int key_cursor_issue(AdminHttp *a,const KuttiDBMetadataCursor *position,
                            const AdminName *prefix,unsigned char expiry_filter,
                            char out[ADMIN_REQUEST_ID_LEN + 4]) {
    uint64_t now=(uint64_t)time(NULL),oldest=UINT64_MAX;size_t slot=0;
    for(size_t i=0;i<ADMIN_CURSOR_LIMIT;i++){AdminKeyCursor *candidate=&a->key_cursors[i];if(!candidate->active||candidate->expires_at<=now){slot=i;break;}if(candidate->expires_at<oldest){oldest=candidate->expires_at;slot=i;}}
    AdminKeyCursor *cursor=&a->key_cursors[slot];if(make_key_cursor_id(cursor->id))return -1;
    cursor->position=*position;cursor->prefix=*prefix;cursor->expiry_filter=expiry_filter;cursor->expires_at=now+ADMIN_CURSOR_TTL_SECONDS;cursor->active=1;memcpy(out,cursor->id,sizeof cursor->id);return 0;
}

static int stream_records_path(const char *path, size_t path_len, AdminName *stream, uint32_t *partition) {
    static const char prefix[]="/api/admin/v1/streams/", marker[]="/partitions/", suffix[]="/records";
    size_t prefix_len=sizeof prefix-1, marker_len=sizeof marker-1, suffix_len=sizeof suffix-1;const char*query=memchr(path,'?',path_len);size_t end=query?(size_t)(query-path):path_len;
    if(end<=prefix_len+marker_len+suffix_len||memcmp(path,prefix,prefix_len)||memcmp(path+end-suffix_len,suffix,suffix_len))return -1;
    const char *mark=memmem(path+prefix_len,end-prefix_len,marker,marker_len);if(!mark||decode_id(path+prefix_len,(size_t)(mark-(path+prefix_len)),stream))return -1;
    const char *part=mark+marker_len;size_t part_len=end-suffix_len-(size_t)(part-path);char tmp[16],*ep;if(!part_len||part_len>=sizeof tmp)return -1;memcpy(tmp,part,part_len);tmp[part_len]=0;errno=0;unsigned long n=strtoul(tmp,&ep,10);if(errno||*ep||n>UINT32_MAX)return -1;*partition=(uint32_t)n;return 0;
}

static int stream_tail_path(const char *path, size_t path_len, AdminName *stream,
                            uint32_t *partition, uint64_t *offset) {
    static const char prefix[]="/api/admin/v1/streams/", marker[]="/partitions/", suffix[]="/records:tail";
    const char *query=memchr(path,'?',path_len);size_t end=query?(size_t)(query-path):path_len,prefix_len=sizeof prefix-1,marker_len=sizeof marker-1,suffix_len=sizeof suffix-1;
    if(end<=prefix_len+marker_len+suffix_len||memcmp(path,prefix,prefix_len)||memcmp(path+end-suffix_len,suffix,suffix_len))return -1;
    const char *mark=memmem(path+prefix_len,end-prefix_len,marker,marker_len);if(!mark||decode_id(path+prefix_len,(size_t)(mark-(path+prefix_len)),stream))return -1;
    const char *part=mark+marker_len;size_t part_len=end-suffix_len-(size_t)(part-path);char number[32],*number_end;if(!part_len||part_len>=sizeof number)return -1;memcpy(number,part,part_len);number[part_len]=0;errno=0;unsigned long parsed=strtoul(number,&number_end,10);if(errno||*number_end||parsed>UINT32_MAX)return -1;*partition=(uint32_t)parsed;
    if(!query||memcmp(query,"?offset=",8))return -1;const char *value=query+8;size_t value_len=path_len-(size_t)(value-path);if(!value_len||value_len>=sizeof number)return -1;memcpy(number,value,value_len);number[value_len]=0;errno=0;unsigned long long parsed_offset=strtoull(number,&number_end,10);if(errno||*number_end)return -1;*offset=parsed_offset;return 0;
}

static int tail_write_header(AdminTail *tail) {
    char header[1536];int n=snprintf(header,sizeof header,"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nReferrer-Policy: no-referrer\r\nX-KuttiDB-Request-ID: %s\r\nConnection: close\r\n%s%s%s\r\n",tail->request_id,tail->origin[0]?"Access-Control-Allow-Origin: ":"",tail->origin[0]?tail->origin:"",tail->origin[0]?"\r\nVary: Origin\r\n":"");return n>0&&(size_t)n<sizeof header?write_all(tail->fd,tail->ssl,header,(size_t)n):-1;
}

static int tail_write_event(AdminTail *tail, const char *event, const char *data) {
    char header[128];int n=snprintf(header,sizeof header,"event: %s\ndata: %s\n\n",event,data);return n>0&&(size_t)n<sizeof header?write_all(tail->fd,tail->ssl,header,(size_t)n):-1;
}

static int tail_write_record(AdminTail *tail, const StreamRecordView *record) {
    size_t cap=(size_t)record->len*2+(size_t)record->key_len*2+512;char *event=malloc(cap);if(!event)return -1;struct out output={event,cap,0,0};put(&output,"id: %u:%llu\nevent: record\ndata: {\"partition\":%u,\"offset\":%llu,\"key\":",tail->partition,(unsigned long long)record->offset,tail->partition,(unsigned long long)record->offset);if(record->key_len){put(&output,"{\"encoding\":\"base64\",\"data\":");json_b64(&output,record->key,record->key_len);put(&output,",\"size\":%u,\"content_type\":\"application/octet-stream\"}",record->key_len);}else put(&output,"null");put(&output,",\"body\":{\"encoding\":\"base64\",\"data\":");json_b64(&output,record->data,record->len);put(&output,",\"size\":%u,\"content_type\":\"application/octet-stream\"}}\n\n",record->len);int rc=output.full?-1:write_all(tail->fd,tail->ssl,event,output.len);free(event);return rc;
}

static void tail_finish(AdminTail *tail) {
#ifdef HAVE_OPENSSL
    if(tail->ssl){SSL_shutdown((SSL*)tail->ssl);SSL_free((SSL*)tail->ssl);}
#endif
    close(tail->fd);pthread_mutex_lock(&tail->admin->tails_mu);if(tail->admin->active_tails)tail->admin->active_tails--;if(tail->admin->active_clients)tail->admin->active_clients--;pthread_cond_broadcast(&tail->admin->tails_cv);pthread_mutex_unlock(&tail->admin->tails_mu);free(tail);
}

static void *admin_tail_worker(void *ud) {
    AdminTail *tail=ud;AdminHttp *a=tail->admin;
    /* The HTTP worker must relinquish the socket before this detached worker
     * can write to it.  This removes a close/reuse race at the connection
     * cap boundary. */
    pthread_mutex_lock(&a->tails_mu);while(a->running&&!tail->transfer_ready)pthread_cond_wait(&a->tails_cv,&a->tails_mu);int ready=tail->transfer_ready;pthread_mutex_unlock(&a->tails_mu);
    if(!ready){tail_finish(tail);return NULL;}
    uint64_t started=admin_mono_ms(),last_heartbeat=started,rate_window=started;unsigned sent_in_window=0;
    if(tail_write_header(tail)<0){tail_finish(tail);return NULL;}
    while(a->running&&admin_mono_ms()-started<ADMIN_TAIL_MAX_MS){
        StreamPartitionStats stats[STREAM_PARTITIONS_MAX];uint32_t count=0;
        int snapshot=stream_partition_snapshot(a->c.streams,(const char *)tail->stream.bytes,tail->stream.len,stats,STREAM_PARTITIONS_MAX,&count,NULL);
        if(snapshot!=1||tail->partition>=count){(void)tail_write_event(tail,"error","{\"code\":\"not_found\"}");break;}
        if(tail->offset<stats[tail->partition].base_offset){char data[192];snprintf(data,sizeof data,"{\"code\":\"offset_out_of_range\",\"base_offset\":%llu}",(unsigned long long)stats[tail->partition].base_offset);(void)tail_write_event(tail,"offset_out_of_range",data);break;}
        StreamRecordView *records=NULL;uint32_t record_count=0;int fetched=stream_fetch(a->c.streams,(const char *)tail->stream.bytes,tail->stream.len,tail->partition,tail->offset,16,ADMIN_TAIL_RECORD_BYTES,&records,&record_count);
        if(fetched==-2){(void)tail_write_event(tail,"error","{\"code\":\"record_too_large\"}");break;}
        if(fetched<0){(void)tail_write_event(tail,"error","{\"code\":\"engine_unavailable\"}");break;}
        int failed=0,throttled=0;for(uint32_t i=0;i<record_count;i++){
            uint64_t sent_now=admin_mono_ms();
            if(sent_now-rate_window>=1000u){rate_window=sent_now;sent_in_window=0;}
            if(sent_in_window>=ADMIN_TAIL_EVENTS_PER_SECOND){throttled=1;break;}
            if(tail_write_record(tail,&records[i])){failed=1;break;}
            tail->offset=records[i].offset+1;sent_in_window++;
        }stream_fetch_free(records,record_count);if(failed)break;
        if(throttled){uint64_t elapsed=admin_mono_ms()-rate_window;if(elapsed<1000u){uint64_t wait_ms=1000u-elapsed;struct timespec pause={(time_t)(wait_ms/1000u),(long)((wait_ms%1000u)*1000000u)};nanosleep(&pause,NULL);}continue;}
        uint64_t now=admin_mono_ms();if(!record_count&&now-last_heartbeat>=ADMIN_TAIL_HEARTBEAT_MS){if(write_all(tail->fd,tail->ssl,": heartbeat\n\n",13))break;last_heartbeat=now;}if(!record_count){struct timespec pause={0,ADMIN_TAIL_POLL_MS*1000000u};nanosleep(&pause,NULL);}
    }
    tail_finish(tail);return NULL;
}

static int admin_start_tail(AdminHttp *a, int fd, void *ssl, const AdminName *stream,
                            uint32_t partition, uint64_t offset, const char *origin) {
    AdminTail *tail=calloc(1,sizeof *tail);if(!tail)return -1;tail->admin=a;tail->fd=fd;tail->ssl=ssl;tail->stream=*stream;tail->partition=partition;tail->offset=offset;memcpy(tail->request_id,request_id,sizeof tail->request_id);if(origin)snprintf(tail->origin,sizeof tail->origin,"%s",origin);
    pthread_mutex_lock(&a->tails_mu);if(a->active_tails>=a->max_tail_clients||a->pending_tail){pthread_mutex_unlock(&a->tails_mu);free(tail);return 0;}a->active_tails++;a->pending_tail=tail;pthread_mutex_unlock(&a->tails_mu);
    pthread_t thread;if(pthread_create(&thread,NULL,admin_tail_worker,tail)!=0){pthread_mutex_lock(&a->tails_mu);a->active_tails--;a->pending_tail=NULL;pthread_cond_broadcast(&a->tails_cv);pthread_mutex_unlock(&a->tails_mu);free(tail);return -1;}pthread_detach(thread);a->tail_transferred_fd=fd;return 1;
}
static int stream_record_path(const char *path, size_t path_len, AdminName *stream, uint32_t *partition, uint64_t *offset) {
    static const char prefix[]="/api/admin/v1/streams/", marker[]="/partitions/", records[]="/records/";
    const char *query=memchr(path,'?',path_len);size_t end=query?(size_t)(query-path):path_len;size_t prefix_len=sizeof prefix-1,marker_len=sizeof marker-1,records_len=sizeof records-1;
    if(end<=prefix_len+marker_len+records_len||memcmp(path,prefix,prefix_len))return -1;const char *mark=memmem(path+prefix_len,end-prefix_len,marker,marker_len);if(!mark||decode_id(path+prefix_len,(size_t)(mark-(path+prefix_len)),stream))return -1;
    const char *record=memmem(mark+marker_len,end-(size_t)(mark+marker_len-path),records,records_len);if(!record)return -1;char number[32],*number_end;size_t partition_len=(size_t)(record-(mark+marker_len));if(!partition_len||partition_len>=sizeof number)return -1;memcpy(number,mark+marker_len,partition_len);number[partition_len]=0;errno=0;unsigned long parsed=strtoul(number,&number_end,10);if(errno||*number_end||parsed>UINT32_MAX)return -1;*partition=(uint32_t)parsed;
    size_t offset_len=end-(size_t)(record+records_len-path);if(!offset_len||offset_len>=sizeof number)return -1;memcpy(number,record+records_len,offset_len);number[offset_len]=0;errno=0;unsigned long long parsed_offset=strtoull(number,&number_end,10);if(errno||*number_end)return -1;*offset=parsed_offset;return 0;
}

static int stream_truncate_path(const char *path, size_t path_len, AdminName *stream, uint32_t *partition) {
    static const char prefix[]="/api/admin/v1/streams/", marker[]="/partitions/", suffix[]=":truncate";
    const char *query=memchr(path,'?',path_len);size_t end=query?(size_t)(query-path):path_len;size_t prefix_len=sizeof prefix-1,marker_len=sizeof marker-1,suffix_len=sizeof suffix-1;
    if(end<=prefix_len+marker_len+suffix_len||memcmp(path,prefix,prefix_len)||memcmp(path+end-suffix_len,suffix,suffix_len))return -1;
    const char *mark=memmem(path+prefix_len,end-prefix_len,marker,marker_len);if(!mark||decode_id(path+prefix_len,(size_t)(mark-(path+prefix_len)),stream))return -1;
    const char *number=mark+marker_len;char tmp[16],*number_end;size_t number_len=end-suffix_len-(size_t)(number-path);if(!number_len||number_len>=sizeof tmp)return -1;memcpy(tmp,number,number_len);tmp[number_len]=0;errno=0;unsigned long parsed=strtoul(tmp,&number_end,10);if(errno||*number_end||parsed>UINT32_MAX)return -1;*partition=(uint32_t)parsed;return 0;
}

static int group_offsets_path(const char *path, size_t path_len, AdminName *stream, AdminName *group) {
    static const char prefix[]="/api/admin/v1/streams/", marker[]="/consumer-groups/", suffix[]="/offsets";
    size_t prefix_len=sizeof prefix-1, marker_len=sizeof marker-1, suffix_len=sizeof suffix-1;const char*query=memchr(path,'?',path_len);size_t end=query?(size_t)(query-path):path_len;
    if(end<=prefix_len+marker_len+suffix_len||memcmp(path,prefix,prefix_len)||memcmp(path+end-suffix_len,suffix,suffix_len))return -1;
    const char*mark=memmem(path+prefix_len,end-prefix_len,marker,marker_len);if(!mark||decode_id(path+prefix_len,(size_t)(mark-(path+prefix_len)),stream))return -1;
    return decode_id(mark+marker_len,end-suffix_len-(size_t)(mark+marker_len-path),group);
}

static int group_path(const char *path, size_t path_len, const char *suffix,
                      AdminName *stream, AdminName *group) {
    static const char prefix[]="/api/admin/v1/streams/", marker[]="/consumer-groups/";
    const char *query=memchr(path,'?',path_len);size_t end=query?(size_t)(query-path):path_len;
    size_t prefix_len=sizeof prefix-1,marker_len=sizeof marker-1,suffix_len=strlen(suffix);
    if(end<=prefix_len+marker_len+suffix_len||memcmp(path,prefix,prefix_len)||memcmp(path+end-suffix_len,suffix,suffix_len))return -1;
    const char *mark=memmem(path+prefix_len,end-prefix_len,marker,marker_len);if(!mark||decode_id(path+prefix_len,(size_t)(mark-(path+prefix_len)),stream))return -1;
    return decode_id(mark+marker_len,end-suffix_len-(size_t)(mark+marker_len-path),group);
}

static int group_offset_partition_path(const char *path, size_t path_len,
                                       AdminName *stream, AdminName *group,
                                       uint32_t *partition) {
    static const char prefix[]="/api/admin/v1/streams/", marker[]="/consumer-groups/", suffix[]="/offsets/";
    const char *query=memchr(path,'?',path_len);size_t end=query?(size_t)(query-path):path_len;
    size_t prefix_len=sizeof prefix-1,marker_len=sizeof marker-1,suffix_len=sizeof suffix-1;
    if(end<=prefix_len+marker_len+suffix_len||memcmp(path,prefix,prefix_len))return -1;
    const char *mark=memmem(path+prefix_len,end-prefix_len,marker,marker_len);if(!mark||decode_id(path+prefix_len,(size_t)(mark-(path+prefix_len)),stream))return -1;
    const char *offsets=memmem(mark+marker_len,end-(size_t)(mark+marker_len-path),suffix,suffix_len);if(!offsets||decode_id(mark+marker_len,(size_t)(offsets-(mark+marker_len)),group))return -1;
    const char *number=offsets+suffix_len;char tmp[16],*number_end;size_t number_len=end-(size_t)(number-path);if(!number_len||number_len>=sizeof tmp)return -1;memcpy(tmp,number,number_len);tmp[number_len]=0;errno=0;unsigned long parsed=strtoul(tmp,&number_end,10);if(errno||*number_end||parsed>UINT32_MAX)return -1;*partition=(uint32_t)parsed;return 0;
}

static int group_offset_batch_path(const char *path, size_t path_len,
                                   AdminName *stream, AdminName *group) {
    static const char prefix[]="/api/admin/v1/streams/", marker[]="/consumer-groups/", suffix[]="/offsets:batch";
    const char *query=memchr(path,'?',path_len);size_t end=query?(size_t)(query-path):path_len;
    size_t prefix_len=sizeof prefix-1,marker_len=sizeof marker-1,suffix_len=sizeof suffix-1;
    if(end<=prefix_len+marker_len+suffix_len||memcmp(path,prefix,prefix_len)||memcmp(path+end-suffix_len,suffix,suffix_len))return -1;
    const char *mark=memmem(path+prefix_len,end-prefix_len,marker,marker_len);if(!mark||decode_id(path+prefix_len,(size_t)(mark-(path+prefix_len)),stream))return -1;
    return decode_id(mark+marker_len,end-suffix_len-(size_t)(mark+marker_len-path),group);
}

static int group_offset_reset_path(const char *path, size_t path_len,
                                   AdminName *stream, AdminName *group) {
    static const char prefix[]="/api/admin/v1/streams/", marker[]="/consumer-groups/", suffix[]=":reset-offsets";
    const char *query=memchr(path,'?',path_len);size_t end=query?(size_t)(query-path):path_len;
    size_t prefix_len=sizeof prefix-1,marker_len=sizeof marker-1,suffix_len=sizeof suffix-1;
    if(end<=prefix_len+marker_len+suffix_len||memcmp(path,prefix,prefix_len)||memcmp(path+end-suffix_len,suffix,suffix_len))return -1;
    const char *mark=memmem(path+prefix_len,end-prefix_len,marker,marker_len);if(!mark||decode_id(path+prefix_len,(size_t)(mark-(path+prefix_len)),stream))return -1;
    return decode_id(mark+marker_len,end-suffix_len-(size_t)(mark+marker_len-path),group);
}

static int group_sessions_path(const char *path, size_t path_len,
                               AdminName *stream, AdminName *group) {
    static const char prefix[]="/api/admin/v1/streams/", marker[]="/consumer-groups/", suffix[]="/sessions";
    const char *query=memchr(path,'?',path_len);size_t end=query?(size_t)(query-path):path_len;
    size_t prefix_len=sizeof prefix-1,marker_len=sizeof marker-1,suffix_len=sizeof suffix-1;
    if(end<=prefix_len+marker_len+suffix_len||memcmp(path,prefix,prefix_len)||memcmp(path+end-suffix_len,suffix,suffix_len))return -1;
    const char *mark=memmem(path+prefix_len,end-prefix_len,marker,marker_len);if(!mark||decode_id(path+prefix_len,(size_t)(mark-(path+prefix_len)),stream))return -1;
    return decode_id(mark+marker_len,end-suffix_len-(size_t)(mark+marker_len-path),group);
}

/* Return 1 for a live session, -2 for a known expired session, and 0 when
 * the path is not a session path or the opaque ID is unknown. */
static int group_session_path(AdminHttp *a, const char *path, size_t path_len,
                              const char *suffix, AdminGroupSession **out) {
    static const char prefix[]="/api/admin/v1/streams/", marker[]="/consumer-groups/", sessions[]="/sessions/";
    const char *query=memchr(path,'?',path_len);size_t end=query?(size_t)(query-path):path_len;
    size_t prefix_len=sizeof prefix-1,marker_len=sizeof marker-1,sessions_len=sizeof sessions-1,suffix_len=strlen(suffix);
    if(end<=prefix_len+marker_len+sessions_len+suffix_len||memcmp(path,prefix,prefix_len)||memcmp(path+end-suffix_len,suffix,suffix_len))return 0;
    const char *mark=memmem(path+prefix_len,end-prefix_len,marker,marker_len);if(!mark)return 0;AdminName stream,group;if(decode_id(path+prefix_len,(size_t)(mark-(path+prefix_len)),&stream))return 0;
    const char *session_marker=memmem(mark+marker_len,end-(size_t)(mark+marker_len-path),sessions,sessions_len);if(!session_marker||decode_id(mark+marker_len,(size_t)(session_marker-(mark+marker_len)),&group))return 0;
    const char *id=session_marker+sessions_len;size_t id_len=end-suffix_len-(size_t)(id-path);if(!id_len||id_len>=sizeof a->group_sessions[0].id)return 0;
    reap_group_sessions(a);for(size_t i=0;i<ADMIN_DELIVERY_LIMIT;i++){AdminGroupSession*s=&a->group_sessions[i];if(strlen(s->id)!=id_len||memcmp(s->id,id,id_len)||s->stream.len!=stream.len||memcmp(s->stream.bytes,stream.bytes,stream.len)||s->group.len!=group.len||memcmp(s->group.bytes,group.bytes,group.len))continue;if(s->active){*out=s;return 1;}if(s->expired)return -2;}return 0;
}

static int generation_etag(const char *value, size_t value_len, uint64_t *out) {
    char number[32], *end;
    if (!value || value_len < 5 || value[0] != '"' || value[1] != 'g' ||
        value[2] != '-' || value[value_len-1] != '"' || value_len-4 >= sizeof number) return -1;
    memcpy(number,value+3,value_len-4);number[value_len-4]=0;errno=0;unsigned long long parsed=strtoull(number,&end,10);if(errno||*end)return -1;*out=parsed;return 0;
}

static int queue_etag(const char *value, size_t value_len, uint64_t *out) {
    char number[32], *end;
    if (!value || value_len < 5 || value[0] != '"' || value[1] != 'q' ||
        value[2] != '-' || value[value_len-1] != '"' || value_len-4 >= sizeof number) return -1;
    memcpy(number,value+3,value_len-4);number[value_len-4]=0;errno=0;unsigned long long parsed=strtoull(number,&end,10);if(errno||*end)return -1;*out=parsed;return 0;
}
static int router_etag(const char *value, size_t value_len, uint64_t *out) { char number[32],*end;if(!value||value_len<5||value[0]!='"'||value[1]!='r'||value[2]!='-'||value[value_len-1]!='"'||value_len-4>=sizeof number)return -1;memcpy(number,value+3,value_len-4);number[value_len-4]=0;errno=0;unsigned long long parsed=strtoull(number,&end,10);if(errno||*end||!parsed)return -1;*out=parsed;return 0; }
static int consumer_etag(const char *value, size_t value_len, uint64_t *out) { char number[32],*end;if(!value||value_len<5||value[0]!='"'||value[1]!='c'||value[2]!='-'||value[value_len-1]!='"'||value_len-4>=sizeof number)return -1;memcpy(number,value+3,value_len-4);number[value_len-4]=0;errno=0;unsigned long long parsed=strtoull(number,&end,10);if(errno||*end||!parsed)return -1;*out=parsed;return 0; }
static int stream_etag(const char *value, size_t value_len, uint64_t *out) { char number[32],*end;if(!value||value_len<5||value[0]!='"'||value[1]!='s'||value[2]!='-'||value[value_len-1]!='"'||value_len-4>=sizeof number)return-1;memcpy(number,value+3,value_len-4);number[value_len-4]=0;errno=0;unsigned long long parsed=strtoull(number,&end,10);if(errno||*end)return-1;*out=parsed;return 0; }
static int job_path_id(const char *path, size_t path_len, uint64_t *out) {
    static const char prefix[]="/api/admin/v1/jobs/";
    const char *query=memchr(path,'?',path_len); if(query)path_len=(size_t)(query-path);
    if(path_len<=sizeof prefix-1||memcmp(path,prefix,sizeof prefix-1))return -1;
    size_t len=path_len-(sizeof prefix-1);char number[32],*end;
    if(len>2&&path[sizeof prefix-1]=='j'&&path[sizeof prefix]=='-'){path+=2;len-=2;}
    if(len>=sizeof number)return -1;memcpy(number,path+sizeof prefix-1,len);number[len]=0;errno=0;
    unsigned long long id=strtoull(number,&end,10);if(errno||!*number||*end||!id)return -1;*out=id;return 0;
}

static int stream_groups_path(const char *path, size_t path_len, AdminName *stream) {
    static const char prefix[]="/api/admin/v1/streams/", suffix[]="/consumer-groups";
    size_t prefix_len=sizeof prefix-1,suffix_len=sizeof suffix-1;const char *query=memchr(path,'?',path_len);size_t end=query?(size_t)(query-path):path_len;
    if(end<=prefix_len+suffix_len||memcmp(path,prefix,prefix_len)||memcmp(path+end-suffix_len,suffix,suffix_len))return -1;
    return decode_id(path+prefix_len,end-suffix_len-prefix_len,stream);
}
static int stream_partitions_path(const char *path, size_t path_len, AdminName *stream) {
    static const char prefix[]="/api/admin/v1/streams/", suffix[]="/partitions";
    size_t prefix_len=sizeof prefix-1,suffix_len=sizeof suffix-1;const char *query=memchr(path,'?',path_len);size_t end=query?(size_t)(query-path):path_len;
    if(end<=prefix_len+suffix_len||memcmp(path,prefix,prefix_len)||memcmp(path+end-suffix_len,suffix,suffix_len))return -1;
    return decode_id(path+prefix_len,end-prefix_len-suffix_len,stream);
}

static int json_bytes(const char *body, size_t body_len, const char *field, unsigned char *out,
                      size_t cap, size_t *out_len) {
    AdminJsonSlice encoded;
    int rc = admin_json_string(body, body_len, field, &encoded);
    if (rc != 1 || admin_base64_decode(encoded.data, encoded.len, 0, out, cap, out_len)) return -1;
    return 0;
}
/* An omitted key is distinct from a malformed key.  Empty keys are accepted
 * and normalized to the native no-key representation. */
static int json_optional_bytes(const char *body, size_t body_len, const char *field,
                               unsigned char *out, size_t cap, size_t *out_len) {
    AdminJsonSlice encoded;
    int rc = admin_json_string(body, body_len, field, &encoded);
    if (rc == 0) { *out_len = 0; return 0; }
    if (rc != 1 || admin_base64_decode(encoded.data, encoded.len, 0, out, cap, out_len)) return -1;
    return 1;
}

static void mutation_reply(AdminHttp *a, int fd, void *ssl, const char *origin, const char *operation,
                           int native_rc, const char *success, int success_status) {
    char body[1024]; struct out out = {body, sizeof body, 0, 0};
    if (audit_event(a, operation, "attempt")) {
        reply(a, fd, ssl, "503 Service Unavailable", body, error(&out, "audit_unavailable", "The audit trail is unavailable."), 0, origin, 0);
        return;
    }
    if (native_rc == 0) {
        if (audit_event(a, operation, "completed")) {
            out.len = 0; out.full = 0;
            reply(a, fd, ssl, "500 Internal Server Error", body, error(&out, "operation_in_doubt", "The operation may have completed. Refresh before retrying."), 0, origin, 0);
            return;
        }
        reply(a, fd, ssl, success_status == 201 ? "201 Created" : "200 OK", success, strlen(success), 0, origin, 0);
    } else {
        (void)audit_event(a, operation, "failed");
        reply(a, fd, ssl, "409 Conflict", body, error(&out, "conflict", "The requested operation could not be applied."), 0, origin, 0);
    }
}

/* Returns non-zero only after reserving the request in both rolling windows.
 * This runs after full validation and before audit/engine dispatch, so a
 * rejected request cannot create a partial mutation or an audit attempt. */
static int mutation_rate_allow(AdminHttp *a, size_t body_bytes, unsigned *retry_after) {
    uint64_t now=admin_mono_ms(), second_count=0, minute_bytes=0, oldest=now;
    for (unsigned i=0;i<a->rate_count;i++) {
        const AdminRateSample *sample=&a->rate_samples[i];
        if (now-sample->when_ms < 1000u) second_count++;
        if (now-sample->when_ms < 60000u) {
            minute_bytes+=sample->bytes;
            if(sample->when_ms<oldest)oldest=sample->when_ms;
        }
    }
    if(second_count>=ADMIN_MUTATIONS_PER_SECOND ||
       body_bytes>ADMIN_MUTATION_BYTES_PER_MINUTE ||
       minute_bytes>ADMIN_MUTATION_BYTES_PER_MINUTE-body_bytes) {
        uint64_t remaining=second_count>=ADMIN_MUTATIONS_PER_SECOND ? 1000u :
                           60000u-(now-oldest);
        *retry_after=(unsigned)((remaining+999u)/1000u);
        if(!*retry_after)*retry_after=1;
        return 0;
    }
    /* Reuse a ring slot only when it has expired from the minute window; at
     * this configured rate, a full ring is otherwise conservatively denied. */
    if(a->rate_count==ADMIN_RATE_SAMPLES && now-a->rate_samples[a->rate_next].when_ms<60000u) {
        *retry_after=1;
        return 0;
    }
    a->rate_samples[a->rate_next]=(AdminRateSample){now,body_bytes};
    a->rate_next=(a->rate_next+1u)%ADMIN_RATE_SAMPLES;
    if(a->rate_count<ADMIN_RATE_SAMPLES)a->rate_count++;
    return 1;
}

/* The mutation transport is intentionally conservative: it accepts one
 * bounded JSON request, validates it before calling an engine primitive, and
 * never writes while an engine lock is held.  Resource-specific handlers are
 * added here rather than to the data-protocol dispatcher. */
static void handle_management(AdminHttp *a, int fd, void *ssl) {
    char headers[ADMIN_REQ_MAX]; size_t used = 0;
    for (;;) {
        if (used == sizeof headers) { char b[256]; struct out o={b,sizeof b,0,0}; reply(a,fd,ssl,"431 Request Header Fields Too Large",b,error(&o,"request_too_large","Request headers are too large."),0,NULL,0); return; }
        ssize_t n = read_one(fd, ssl, headers + used, sizeof headers - used);
        if (n <= 0) return;
        used += (size_t)n;
        if (memmem(headers, used, "\r\n\r\n", 4)) break;
    }
    char *end_headers = memmem(headers, used, "\r\n\r\n", 4);
    make_request_id();
    collection_snapshot_revision=a->mutation_attempts;
    size_t header_len = (size_t)(end_headers - headers) + 4;
    char *first_space = memchr(headers, ' ', header_len);
    char *path = first_space ? first_space + 1 : NULL;
    char *second_space = path ? memchr(path, ' ', header_len - (size_t)(path - headers)) : NULL;
    if (!first_space || !second_space || first_space == headers || second_space == path) { char b[256]; struct out o={b,sizeof b,0,0}; reply(a,fd,ssl,"400 Bad Request",b,error(&o,"bad_request","The HTTP request is malformed."),0,NULL,0); return; }
    size_t method_len = (size_t)(first_space - headers), path_len = (size_t)(second_space - path);
    char method[8];
    if (method_len >= sizeof method || path_len >= 2048 || memcmp(path, "/api/admin/v1/", 14)) { char b[256]; struct out o={b,sizeof b,0,0}; reply(a,fd,ssl,"404 Not Found",b,error(&o,"not_found","The requested resource was not found."),0,NULL,0); return; }
    memcpy(method, headers, method_len); method[method_len] = 0;
    size_t value_len = 0;
    const char *client_request_id = header(headers, header_len, "x-kuttidb-request-id", &value_len);
    accept_request_id(client_request_id, value_len);
    const char *authorization = header(headers, header_len, "authorization", &value_len);
    int authenticated = authorization && value_len > 7 && !strncasecmp(authorization, "Bearer ", 7) && secret_equal((const unsigned char *)authorization + 7, value_len - 7, a->c.token, a->c.token_len);
    if (!authenticated) { if (a->c.auth_failure) a->c.auth_failure(a->c.auth_failure_ud); char b[256]; struct out o={b,sizeof b,0,0}; reply(a,fd,ssl,"401 Unauthorized",b,error(&o,"unauthorized","Authentication is required."),0,NULL,1); return; }
    size_t origin_len = 0; const char *origin = header(headers, header_len, "origin", &origin_len); char origin_copy[512]; const char *cors = NULL;
    if (origin) { if (origin_len >= sizeof origin_copy || !origin_allowed(a, origin, origin_len)) { char b[256]; struct out o={b,sizeof b,0,0}; reply(a,fd,ssl,"403 Forbidden",b,error(&o,"forbidden_origin","The origin is not allowed."),0,NULL,0); return; } memcpy(origin_copy,origin,origin_len); origin_copy[origin_len]=0; cors=origin_copy; }
    size_t content_len = 0; const char *content_length = header(headers, header_len, "content-length", &value_len);
    if (content_length) { char nbuf[24], *np; if (!value_len || value_len >= sizeof nbuf) { char b[256]; struct out o={b,sizeof b,0,0}; reply(a,fd,ssl,"400 Bad Request",b,error(&o,"bad_request","The content length is invalid."),0,cors,0); return; } memcpy(nbuf,content_length,value_len); nbuf[value_len]=0; errno=0; content_len=strtoull(nbuf,&np,10); if(errno||*np||content_len>ADMIN_BODY_MAX){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"413 Payload Too Large",b,error(&o,"request_too_large","The request body is too large."),0,cors,0);return;} }
    if (header(headers, header_len, "transfer-encoding", &value_len)) { char b[256]; struct out o={b,sizeof b,0,0}; reply(a,fd,ssl,"400 Bad Request",b,error(&o,"bad_request","Chunked request bodies are not supported."),0,cors,0); return; }
    size_t content_type_len=0;const char *content_type=header(headers,header_len,"content-type",&content_type_len);
    char *body = calloc(1, content_len + 1); if (!body) { char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"500 Internal Server Error",b,error(&o,"internal_error","The server could not complete the request."),0,cors,0);return; }
    size_t available = used - header_len, copied = available < content_len ? available : content_len;
    if (copied) memcpy(body, headers + header_len, copied);
    while (copied < content_len) { ssize_t n = read_one(fd,ssl,body+copied,content_len-copied); if(n<=0){free(body);return;} copied+=(size_t)n; }
    if(content_len&&(!content_type||content_type_len<16||strncasecmp(content_type,"application/json",16))){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"415 Unsupported Media Type",b,error(&o,"unsupported_media_type","Management API request bodies must use application/json."),0,cors,0);free(body);return;}
    int is_get=!strcmp(method,"GET"), is_head=!strcmp(method,"HEAD"), is_options=!strcmp(method,"OPTIONS");
    if (is_options) { reply(a,fd,ssl,"204 No Content","",0,1,cors,0); free(body); return; }
    AdminName tail_stream;uint32_t tail_partition=0;uint64_t tail_offset=0;
    if(is_get&&stream_tail_path(path,path_len,&tail_stream,&tail_partition,&tail_offset)==0){
        size_t accept_len=0;const char *accept=header(headers,header_len,"accept",&accept_len);
        if(!accept||accept_len!=17||memcmp(accept,"text/event-stream",17)){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"400 Bad Request",b,error(&o,"validation_failed","Accept: text/event-stream is required for a Stream tail."),0,cors,0);free(body);return;}
        int started=admin_start_tail(a,fd,ssl,&tail_stream,tail_partition,tail_offset,cors);if(started==1){free(body);return;}char b[256];struct out o={b,sizeof b,0,0};reply_extra(a,fd,ssl,started==0?"429 Too Many Requests":"503 Service Unavailable",b,error(&o,started==0?"resource_exhausted":"internal_error",started==0?"The tail worker limit is reached.":"The tail worker could not be started."),0,cors,0,started==0?"Retry-After: 1\r\n":NULL);free(body);return;
    }
    { const char *query=memchr(path,'?',path_len); size_t bare=query?(size_t)(query-path):path_len;
      if((is_get||is_head)&&bare==18&&!memcmp(path,"/api/admin/v1/jobs",18)){
        AdminJob jobs[ADMIN_JOB_LIMIT];size_t count=0;pthread_mutex_lock(&a->jobs_mu);for(size_t i=0;i<a->job_limit;i++)if(a->jobs[i].state!=ADMIN_JOB_UNUSED)jobs[count++]=a->jobs[i];pthread_mutex_unlock(&a->jobs_mu);
        char result[16384];struct out encoded={result,sizeof result,0,0};put(&encoded,"{\"data\":[");for(size_t i=0;i<count;i++){if(i)put(&encoded,",");job_json(&encoded,&jobs[i]);}put(&encoded,"],\"meta\":{\"count\":%zu,\"limit\":%u,\"truncated\":false}}",count,a->job_limit);reply(a,fd,ssl,"200 OK",result,encoded.len,is_head,cors,0);free(body);return;
      }
      uint64_t requested_job=0;
      if((is_get||is_head)&&job_path_id(path,path_len,&requested_job)==0){AdminJob job;if(!job_snapshot(a,requested_job,&job)){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"404 Not Found",b,error(&o,"not_found","The requested job was not found."),is_head,cors,0);free(body);return;}char result[1024];struct out encoded={result,sizeof result,0,0};put(&encoded,"{\"data\":");job_json(&encoded,&job);put(&encoded,"}");reply(a,fd,ssl,"200 OK",result,encoded.len,is_head,cors,0);free(body);return;}
    }
    if ((is_get || is_head) && path_len == 26 && !memcmp(path,"/api/admin/v1/capabilities",26)) {
        AdminHttpStatus capability_status={0};a->c.status(a->c.status_ud,&capability_status);
        unsigned delivery_limit=a->session_limit<ADMIN_DELIVERY_LIMIT?a->session_limit:ADMIN_DELIVERY_LIMIT;char capabilities[4096];int n=snprintf(capabilities,sizeof capabilities,"{\"product\":\"KuttiDB\",\"server_version\":\"0.2.0\",\"management_api_contract\":\"1.0\",\"enabled_engines\":[\"keyspaces\",\"queues\",\"streams\",\"consumer-groups\",\"routing\"],\"sse\":{\"available\":true},\"audit\":{\"required\":true,\"healthy\":%s},\"operations\":{\"keyspaces\":[\"list\",\"get\",\"list_entries\",\"read_entry\",\"batch_get\",\"batch_put\",\"batch_delete\",\"put_entry\",\"delete_entry\",\"claims\",\"get_or_refresh\"],\"queues\":[\"list\",\"get\",\"create\",\"browse_messages\",\"publish\",\"publish_batch\",\"consume\",\"consume_batch\",\"get_delivery\",\"ack\",\"nack\",\"ack_batch\",\"nack_batch\",\"purge\"],\"queue_consumers\":[\"list\",\"get\",\"register\",\"delete\"],\"streams\":[\"list\",\"get\",\"create\",\"inspect_partitions\",\"append\",\"append_batch\",\"fetch\",\"tail\",\"truncate\",\"update_retention\",\"delete\"],\"consumer_groups\":[\"inspect_offsets\",\"batch_commit_offsets\",\"reset_offsets\",\"management_sessions\"],\"jobs\":[\"list\",\"get\",\"cancel\"],\"maintenance\":[\"list\",\"keyspace_checkpoint\",\"queue_checkpoint\",\"stream_checkpoint\",\"checkpoint_all\"],\"routing\":[\"list_routers\",\"get_router\",\"list_routes\",\"create_router\",\"create_route\",\"delete_route\",\"delete_router\",\"update_router\",\"publish\"],\"atomic_operations\":[\"put-and-route\",\"put-and-enqueue\",\"delete-and-route\",\"update-if-present-and-route\"]},\"routing_modes\":[\"exact\",\"broadcast\",\"pattern\"],\"idempotency\":{\"required_for\":[\"put_entry\",\"delete_entry\",\"create\",\"publish\",\"append\",\"consume\",\"ack\",\"nack\",\"claims\",\"get_or_refresh\",\"atomic_operations\",\"purge\",\"truncate\",\"stream_update\",\"stream_delete\",\"cancel\",\"checkpoint\",\"consumer_delete\",\"offset_batch_commit\",\"offset_reset\",\"group_session\"],\"persistence\":\"process-lifetime\"},\"limits\":{\"request_body_bytes\":262144,\"batch_items\":100,\"page_items\":500,\"tail_clients\":%u,\"tail_events_per_second\":%u,\"delivery_sessions\":%u,\"claim_sessions\":%u,\"jobs\":%u}}",a->audit_failed?"false":"true",a->max_tail_clients,ADMIN_TAIL_EVENTS_PER_SECOND,delivery_limit,delivery_limit,a->job_limit);
        if(n>0&&(size_t)n<sizeof capabilities){int extra;capabilities[n-1]=0;extra=snprintf(capabilities+n-1,sizeof capabilities-(size_t)n+1,",\"durable_consumer_deliveries\":true,\"keyspace_entry_filters\":[\"prefix\",\"expires\"],\"cursors\":{\"queue_messages\":{\"opaque\":true,\"ttl_seconds\":%u,\"max_live\":%u},\"keyspace_entries\":{\"opaque\":true,\"ttl_seconds\":%u,\"max_live\":%u}},\"tls\":{\"available\":%s},\"persistence\":{\"keyspaces\":%s,\"queues\":%s,\"streams\":%s}}",ADMIN_CURSOR_TTL_SECONDS,ADMIN_CURSOR_LIMIT,ADMIN_CURSOR_TTL_SECONDS,ADMIN_CURSOR_LIMIT,capability_status.tls_available?"true":"false",capability_status.keyspace_persistence_failed?"false":"true",capability_status.queue_persistence_failed?"false":"true",capability_status.stream_persistence_failed?"false":"true");if(extra>=0)n=n-1+extra;}
        if(n<0||(size_t)n>=sizeof capabilities){free(body);return;}reply(a,fd,ssl,"200 OK",capabilities,(size_t)n,is_head,cors,0);free(body);return;
    }
    { const char *query=memchr(path,'?',path_len);size_t bare=query?(size_t)(query-path):path_len;
      if ((is_get || is_head) && bare==31 && !memcmp(path,"/api/admin/v1/keyspaces/default",31)) {
        AdminHttpStatus status={0};uint64_t revision=kuttidb_revision(a->c.keyspace);a->c.status(a->c.status_ud,&status);char result[768],etag[64];int n=snprintf(result,sizeof result,"{\"data\":{\"id\":\"default\",\"name\":\"default\",\"entry_count\":%llu,\"live_bytes\":%llu,\"allocated_bytes\":%llu,\"expired_count\":%llu,\"evicted_count\":%llu,\"revision\":%llu,\"persistence_healthy\":%s}}",(unsigned long long)status.keyspace_entries,(unsigned long long)status.keyspace_live_bytes,(unsigned long long)status.keyspace_allocated_bytes,(unsigned long long)status.keyspace_expired,(unsigned long long)status.keyspace_evicted,(unsigned long long)revision,status.keyspace_persistence_failed?"false":"true");
        if(n<0||(size_t)n>=sizeof result){free(body);return;}snprintf(etag,sizeof etag,"ETag: \"k-%llu\"\r\n",(unsigned long long)revision);reply_extra(a,fd,ssl,"200 OK",result,(size_t)n,is_head,cors,0,etag);free(body);return;
      }
      if ((is_get || is_head) && bare==39 && !memcmp(path,"/api/admin/v1/keyspaces/default/entries",39)) {
        unsigned limit;int cursor_supplied,prefix_supplied,expiry_supplied;unsigned char expiry_filter;AdminName prefix;char cursor_id[ADMIN_REQUEST_ID_LEN+4];KuttiDBMetadataCursor start={0},next={0};
        if(keyspace_browse_params(path,path_len,&limit,&prefix,&prefix_supplied,&expiry_filter,&expiry_supplied,cursor_id,&cursor_supplied)){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"400 Bad Request",b,error(&o,"validation_failed","limit, prefix, expires, or cursor is invalid."),is_head,cors,0);free(body);return;}
        if(cursor_supplied){AdminKeyCursor *saved=key_cursor_find(a,cursor_id);if(!saved||(prefix_supplied&&(prefix.len!=saved->prefix.len||memcmp(prefix.bytes,saved->prefix.bytes,prefix.len)))||(expiry_supplied&&expiry_filter!=saved->expiry_filter)){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"400 Bad Request",b,error(&o,"validation_failed","The cursor is invalid, expired, or does not match this Keyspace browse query."),is_head,cors,0);free(body);return;}start=saved->position;prefix=saved->prefix;expiry_filter=saved->expiry_filter;}
        AdminKeyScan *scan=calloc(1,sizeof *scan);if(!scan){free(body);return;}scan->prefix=prefix;scan->expiry_filter=expiry_filter;int more=0;int scanned=kuttidb_foreach_metadata_page_filtered(a->c.keyspace,&start,limit,key_metadata_copy,key_metadata_matches,scan,&next,&more);if(scanned<0||scan->key_too_large){free(scan);char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"503 Service Unavailable",b,error(&o,"engine_unavailable","A bounded Keyspace metadata scan is unavailable."),is_head,cors,0);free(body);return;}
        char next_cursor[ADMIN_REQUEST_ID_LEN+4]={0};if(more&&key_cursor_issue(a,&next,&prefix,expiry_filter,next_cursor)){free(scan);char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"503 Service Unavailable",b,error(&o,"engine_unavailable","A secure cursor could not be created."),is_head,cors,0);free(body);return;}
        char *result=malloc(ADMIN_BODY_MAX);if(!result){free(scan);free(body);return;}uint64_t wall_now=(uint64_t)time(NULL),snapshot_revision=kuttidb_revision(a->c.keyspace);struct out encoded={result,ADMIN_BODY_MAX,0,0};put(&encoded,"{\"data\":[");for(uint32_t i=0;i<scan->count;i++){if(i)put(&encoded,",");put(&encoded,"{");identifier_field(&encoded,"key",&scan->entries[i].key);put(&encoded,",\"value_size\":%u,\"expires_at\":",scan->entries[i].value_len);if(scan->entries[i].expiry){put(&encoded,"%u,\"remaining_ttl_ms\":%llu",scan->entries[i].expiry,(unsigned long long)((scan->entries[i].expiry>wall_now?scan->entries[i].expiry-wall_now:0)*1000));}else put(&encoded,"null,\"remaining_ttl_ms\":null");put(&encoded,"}");}put(&encoded,"],\"meta\":{\"count\":%u,\"limit\":%u,\"next_cursor\":",scan->count,limit);if(more)put(&encoded,"\"%s\"",next_cursor);else put(&encoded,"null");put(&encoded,",\"snapshot_revision\":%llu,\"weakly_consistent\":true}}",(unsigned long long)snapshot_revision);reply(a,fd,ssl,"200 OK",result,encoded.len,is_head,cors,0);free(result);free(scan);free(body);return;
      }
      if ((is_get || is_head) && bare==29 && !memcmp(path,"/api/admin/v1/queue-consumers",29)) {
        unsigned limit=100;if(limit_from(path,path_len,&limit)){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"400 Bad Request",b,error(&o,"bad_request","The limit parameter is invalid."),is_head,cors,0);free(body);return;}struct csnap *consumers=calloc(1,sizeof *consumers);if(!consumers){free(body);return;}queue_consumer_foreach(a->c.queues,ccb,consumers);char *result=malloc(ADMIN_BODY_MAX);if(!result){free(consumers);free(body);return;}struct out encoded={result,ADMIN_BODY_MAX,0,0};put(&encoded,"{\"data\":[");size_t count=consumers->n<limit?consumers->n:limit;for(size_t i=0;i<count;i++){if(i)put(&encoded,",");put(&encoded,"{");identifier_field(&encoded,"name",&consumers->a[i].name);put(&encoded,"}");}collection_end(&encoded,count,limit,consumers->trunc||consumers->n>limit);reply(a,fd,ssl,"200 OK",result,encoded.len,is_head,cors,0);free(result);free(consumers);free(body);return;
      }
      if (!strcmp(method,"POST") && bare==49 && !memcmp(path,"/api/admin/v1/keyspaces/default/entries:batch-get",49)) {
        AdminJsonSlice ids[100];AdminName names[100];size_t count=0;
        if(!content_len||admin_json_validate(body,content_len)||admin_json_string_array(body,content_len,"entry_ids",ids,100,&count)!=1||!count){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"400 Bad Request",b,error(&o,"validation_failed","entry_ids must be a non-empty array of at most 100 identifiers."),0,cors,0);free(body);return;}
        for(size_t i=0;i<count;i++)if(decode_id(ids[i].data,ids[i].len,&names[i])){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"400 Bad Request",b,error(&o,"validation_failed","An entry identifier is invalid."),0,cors,0);free(body);return;}
        char *result=malloc(ADMIN_BODY_MAX);if(!result){free(body);return;}struct out encoded={result,ADMIN_BODY_MAX,0,0};put(&encoded,"{\"data\":[");
        for(size_t i=0;i<count;i++){char *value=NULL;uint32_t value_len=0;int found=kuttidb_get(a->c.keyspace,(const char *)names[i].bytes,names[i].len,&value,&value_len);if(i)put(&encoded,",");put(&encoded,"{");identifier_field(&encoded,"key",&names[i]);if(found==1){size_t b64_len=((size_t)value_len+2)/3*4;if(b64_len+encoded.len+192<encoded.cap){put(&encoded,",\"found\":true,\"value\":{\"encoding\":\"base64\",\"data\":");json_b64(&encoded,(const unsigned char *)value,value_len);put(&encoded,",\"size\":%u,\"content_type\":\"application/octet-stream\"}",value_len);}else put(&encoded,",\"found\":true,\"value_size\":%u,\"value_omitted\":true",value_len);free(value);}else if(found==0)put(&encoded,",\"found\":false");else{free(result);free(body);char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"503 Service Unavailable",b,error(&o,"engine_unavailable","The Keyspace engine is unavailable."),0,cors,0);return;}put(&encoded,"}");}
        put(&encoded,"],\"meta\":{\"count\":%zu,\"limit\":100,\"truncated\":false}}",count);if(encoded.full){free(result);free(body);char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"500 Internal Server Error",b,error(&o,"internal_error","The server could not complete the request."),0,cors,0);}else{reply(a,fd,ssl,"200 OK",result,encoded.len,0,cors,0);free(result);free(body);}return;
      }
    }
    AdminName entry_name;
    if ((is_get || is_head) && path_id_after(path,path_len,"/api/admin/v1/keyspaces/default/entries/",&entry_name)==0) {
        char *value=NULL; uint32_t value_len=0; int found=kuttidb_get(a->c.keyspace,(const char *)entry_name.bytes,entry_name.len,&value,&value_len);
        if(found!=1){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,found==0?"404 Not Found":"500 Internal Server Error",b,error(&o,found==0?"not_found":"internal_error",found==0?"The requested resource was not found.":"The server could not complete the request."),is_head,cors,0);free(body);return;}
        size_t cap=(size_t)value_len*2+1024;char *result=malloc(cap);if(!result){free(value);free(body);return;}uint64_t revision=kuttidb_revision(a->c.keyspace);char etag[64];struct out encoded={result,cap,0,0};put(&encoded,"{\"data\":{");identifier_field(&encoded,"key",&entry_name);put(&encoded,",\"value\":{\"encoding\":\"base64\",\"data\":");json_b64(&encoded,(const unsigned char *)value,value_len);put(&encoded,",\"size\":%u,\"content_type\":\"application/octet-stream\"},\"revision\":%llu}}",value_len,(unsigned long long)revision);snprintf(etag,sizeof etag,"ETag: \"k-%llu\"\r\n",(unsigned long long)revision);reply_extra(a,fd,ssl,"200 OK",result,encoded.len,is_head,cors,0,etag);free(result);free(value);free(body);return;
    }
    AdminName resource_name;
    if (is_get || is_head) {
        AdminClaim *claim=NULL;int found=claim_path(a,path,path_len,"",&claim);
        if(found==-2){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"410 Gone",b,error(&o,"claim_expired","The claim has expired."),is_head,cors,0);free(body);return;}
        if(found==1){uint64_t now=admin_mono_ms();char result[1024];struct out encoded={result,sizeof result,0,0};put(&encoded,"{\"data\":{\"claim_id\":\"");put(&encoded,"%s",claim->id);put(&encoded,"\",");identifier_field(&encoded,"key",&claim->key);put(&encoded,",\"state\":\"active\",\"lease_remaining_ms\":%llu}}",(unsigned long long)(claim->deadline_ms>now?claim->deadline_ms-now:0));reply(a,fd,ssl,"200 OK",result,encoded.len,is_head,cors,0);free(body);return;}
    }
    if (is_get || is_head) {
        AdminDelivery *delivery = NULL;
        int found = path_delivery(a,path,path_len,"",&delivery);
        if (found == -2) {
            char b[256]; struct out o={b,sizeof b,0,0};
            reply(a,fd,ssl,"410 Gone",b,error(&o,"delivery_expired","The delivery has expired."),is_head,cors,0);
            free(body);return;
        }
        if (found == 1) {
            const char *query=memchr(path,'?',path_len);int include_body=query&&path_len-(size_t)(query-path)==13&&!memcmp(query,"?include=body",13);
            QueueMessage snapshot={0};
            if(include_body&&queue_delivery_snapshot(a->c.queues,(const char *)delivery->queue.bytes,delivery->queue.len,delivery->tag,delivery->owner,&snapshot)!=1){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"410 Gone",b,error(&o,"delivery_expired","The delivery is no longer active."),is_head,cors,0);free(body);return;}
            size_t cap=include_body?(size_t)snapshot.len*2+1024:1024;char *result=malloc(cap);if(!result){queue_message_free(&snapshot);free(body);return;}struct out encoded={result,cap,0,0};uint64_t now=admin_mono_ms();
            put(&encoded,"{\"data\":{\"delivery_id\":\"");put(&encoded,"%s",delivery->id);
            put(&encoded,"\",\"message_id\":%llu,\"queue\":{",(unsigned long long)delivery->message_id);
            identifier_field(&encoded,"name",&delivery->queue);
            put(&encoded,"},\"state\":\"active\",\"lease_remaining_ms\":%llu",(unsigned long long)(delivery->deadline_ms>now?delivery->deadline_ms-now:0));
            if(include_body){put(&encoded,",\"body\":{\"encoding\":\"base64\",\"data\":");json_b64(&encoded,snapshot.data,snapshot.len);put(&encoded,",\"size\":%u,\"content_type\":\"application/octet-stream\"}",snapshot.len);}
            put(&encoded,"}}");
            if(encoded.full){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"413 Payload Too Large",b,error(&o,"request_too_large","The delivery body exceeds the response limit."),is_head,cors,0);}else reply(a,fd,ssl,"200 OK",result,encoded.len,is_head,cors,0);free(result);queue_message_free(&snapshot);free(body);return;
        }
    }
    if ((is_get || is_head) && path_id_after(path,path_len,"/api/admin/v1/queue-consumers/",&resource_name)==0) {
        uint64_t owner=0;if(queue_consumer_lookup(a->c.queues,(const char *)resource_name.bytes,resource_name.len,&owner)!=1){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"404 Not Found",b,error(&o,"not_found","The requested Queue consumer was not found."),is_head,cors,0);free(body);return;}char result[512],etag[64];struct out encoded={result,sizeof result,0,0};put(&encoded,"{\"data\":{");identifier_field(&encoded,"name",&resource_name);put(&encoded,"}}");snprintf(etag,sizeof etag,"ETag: \"c-%llu\"\r\n",(unsigned long long)owner);reply_extra(a,fd,ssl,"200 OK",result,encoded.len,is_head,cors,0,etag);free(body);return;
    }
    uint64_t exact_message_id=0;
    if ((is_get || is_head) && queue_message_path(path,path_len,&resource_name,&exact_message_id)==0) {
        const char *query=memchr(path,'?',path_len);int include_body=!query?0:(path_len-(size_t)(query-path)==13&&!memcmp(query,"?include=body",13));
        if(query&&!include_body){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"400 Bad Request",b,error(&o,"validation_failed","Only include=body is supported for exact Queue message reads."),is_head,cors,0);free(body);return;}
        QueueMessageSnapshot message={0};int got=queue_message_snapshot(a->c.queues,(const char *)resource_name.bytes,resource_name.len,exact_message_id,include_body,96u<<10,&message);
        if(got!=1){char b[256];struct out o={b,sizeof b,0,0};const char*status=got==-2?"413 Payload Too Large":got==0?"404 Not Found":"500 Internal Server Error";const char*code=got==-2?"request_too_large":got==0?"not_found":"internal_error";reply(a,fd,ssl,status,b,error(&o,code,got==-2?"The Queue message body exceeds the response limit.":got==0?"The requested Queue message was not found.":"The Queue message snapshot could not be created."),is_head,cors,0);free(body);return;}
        size_t cap=include_body?(size_t)message.len*2+1024:1024;char *result=malloc(cap);if(!result){queue_message_snapshot_free(&message);free(body);return;}struct out encoded={result,cap,0,0};const char*state=message.state==QUEUE_PEEK_READY?"ready":message.state==QUEUE_PEEK_DELAYED?"delayed":"in-flight";
        put(&encoded,"{\"data\":{\"message_id\":%llu,\"state\":\"%s\",\"size\":%u,\"expires_at_ms\":",(unsigned long long)message.id,state,message.len);if(message.expires_ms)put(&encoded,"%llu",(unsigned long long)message.expires_ms);else put(&encoded,"null");put(&encoded,",\"delivery_count\":%u,\"redelivered\":%s",message.delivery_count,message.redelivered?"true":"false");if(message.state==QUEUE_PEEK_DELAYED)put(&encoded,",\"available_at_ms\":%llu",(unsigned long long)message.not_before_ms);if(message.state==QUEUE_PEEK_INFLIGHT)put(&encoded,",\"visibility_deadline_ms\":%llu",(unsigned long long)message.visibility_deadline_ms);if(include_body){put(&encoded,",\"body\":{\"encoding\":\"base64\",\"data\":");json_b64(&encoded,message.data,message.len);put(&encoded,",\"size\":%u,\"content_type\":\"application/octet-stream\"}",message.len);}put(&encoded,"}}");
        if(encoded.full){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"413 Payload Too Large",b,error(&o,"request_too_large","The Queue message response exceeds the configured bound."),is_head,cors,0);}else reply(a,fd,ssl,"200 OK",result,encoded.len,is_head,cors,0);free(result);queue_message_snapshot_free(&message);free(body);return;
    }
    if ((is_get || is_head) && path_id_action(path,path_len,"/api/admin/v1/queues/","/messages",&resource_name)==0) {
        unsigned limit, states; int include_bodies, state_supplied, include_supplied, cursor_supplied;
        char cursor_id[ADMIN_REQUEST_ID_LEN + 4]; uint64_t after_id=0, snapshot_revision=0;
        if (queue_browse_params(path,path_len,&limit,&states,&include_bodies,&state_supplied,
                                &include_supplied,cursor_id,&cursor_supplied)) {
            char b[256]; struct out o={b,sizeof b,0,0};
            reply(a,fd,ssl,"400 Bad Request",b,error(&o,"validation_failed","limit, state, include, or cursor is invalid."),is_head,cors,0);free(body);return;
        }
        if (cursor_supplied) {
            AdminQueueCursor *saved=queue_cursor_find(a,cursor_id);
            if (!saved || saved->queue.len != resource_name.len ||
                memcmp(saved->queue.bytes,resource_name.bytes,resource_name.len) ||
                (state_supplied && states != saved->states) ||
                (include_supplied && include_bodies != saved->include_bodies)) {
                char b[256]; struct out o={b,sizeof b,0,0};
                reply(a,fd,ssl,"400 Bad Request",b,error(&o,"validation_failed","The cursor is invalid, expired, or does not match this Queue browse query."),is_head,cors,0);free(body);return;
            }
            after_id=saved->after_id; states=saved->states; include_bodies=saved->include_bodies;
        }
        QueueMessageSnapshot *messages = NULL; uint32_t count = 0;
        int peeked = queue_peek_after(a->c.queues,(const char *)resource_name.bytes,resource_name.len,
                                      after_id,states,limit + 1,include_bodies,96u<<10,&messages,&count,
                                      &snapshot_revision);
        if (peeked != 1) {
            char b[256]; struct out o={b,sizeof b,0,0};
            reply(a,fd,ssl,peeked==0?"404 Not Found":"500 Internal Server Error",b,
                  error(&o,peeked==0?"not_found":"internal_error",peeked==0?"The requested Queue was not found.":"The Queue browse snapshot could not be created."),is_head,cors,0);
            free(body);return;
        }
        int has_more = count > limit;
        if (has_more) {
            free(messages[limit].data);
            memset(&messages[limit], 0, sizeof messages[limit]);
            count = limit;
        }
        char next_cursor[ADMIN_REQUEST_ID_LEN + 4] = {0};
        if (has_more && queue_cursor_issue(a,&resource_name,states,include_bodies,
                                           messages[count - 1].id,next_cursor)) {
            queue_peek_free(messages,count); char b[256]; struct out o={b,sizeof b,0,0};
            reply(a,fd,ssl,"503 Service Unavailable",b,error(&o,"engine_unavailable","A secure cursor could not be created."),is_head,cors,0);free(body);return;
        }
        char *result = malloc(ADMIN_BODY_MAX);
        if (!result) { queue_peek_free(messages,count);free(body);return; }
        struct out encoded={result,ADMIN_BODY_MAX,0,0};
        put(&encoded,"{\"data\":[");
        for (uint32_t i=0;i<count;i++) {
            QueueMessageSnapshot *message=&messages[i];
            const char *state=message->state==QUEUE_PEEK_READY?"ready":message->state==QUEUE_PEEK_DELAYED?"delayed":"in-flight";
            if(i)put(&encoded,",");
            put(&encoded,"{\"message_id\":%llu,\"state\":\"%s\",\"size\":%u,\"expires_at_ms\":",(unsigned long long)message->id,state,message->len);
            if(message->expires_ms)put(&encoded,"%llu",(unsigned long long)message->expires_ms);else put(&encoded,"null");
            put(&encoded,",\"delivery_count\":%u,\"redelivered\":%s",message->delivery_count,message->redelivered?"true":"false");
            if(message->state==QUEUE_PEEK_DELAYED)put(&encoded,",\"available_at_ms\":%llu",(unsigned long long)message->not_before_ms);
            if(include_bodies) {
                if(message->body_omitted)put(&encoded,",\"body_omitted\":true");
                else { put(&encoded,",\"body\":{\"encoding\":\"base64\",\"data\":");json_b64(&encoded,message->data,message->len);put(&encoded,",\"size\":%u,\"content_type\":\"application/octet-stream\"}",message->len); }
            }
            put(&encoded,"}");
        }
        put(&encoded,"],\"meta\":{\"count\":%u,\"limit\":%u,\"next_cursor\":",count,limit);
        if (has_more) put(&encoded,"\"%s\"",next_cursor);
        else put(&encoded,"null");
        put(&encoded,",\"snapshot_revision\":%llu,\"weakly_consistent\":true}}",
            (unsigned long long)snapshot_revision);
        if(encoded.full){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"413 Payload Too Large",b,error(&o,"request_too_large","The Queue browse response exceeds the configured bound."),is_head,cors,0);}else reply(a,fd,ssl,"200 OK",result,encoded.len,is_head,cors,0);
        free(result);queue_peek_free(messages,count);free(body);return;
    }
    if ((is_get || is_head) && path_id_after(path,path_len,"/api/admin/v1/queues/",&resource_name)==0) {
        struct qfind found={.target=&resource_name};QueueConfigSnapshot config;queue_foreach_stats(a->c.queues,qfindcb,&found);if(!found.found||queue_config_snapshot(a->c.queues,(const char *)resource_name.bytes,resource_name.len,&config)!=1){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"404 Not Found",b,error(&o,"not_found","The requested Queue was not found."),is_head,cors,0);free(body);return;}char result[1024],etag[64];struct out encoded={result,sizeof result,0,0};put(&encoded,"{\"data\":{");identifier_field(&encoded,"name",&resource_name);put(&encoded,",\"durable\":%s,\"max_depth\":%llu,\"max_deliveries\":%u,\"dead_letter_queue\":",config.durable?"true":"false",(unsigned long long)config.max_depth,config.max_deliveries);if(config.dead_letter_queue_len){AdminName dlq={0};dlq.len=config.dead_letter_queue_len;memcpy(dlq.bytes,config.dead_letter_queue,dlq.len);put(&encoded,"{");identifier_field(&encoded,"name",&dlq);put(&encoded,"}");}else put(&encoded,"null");put(&encoded,",\"ready_depth\":%llu,\"in_flight\":%llu,\"revision\":%llu}}",(unsigned long long)found.depth,(unsigned long long)found.inflight,(unsigned long long)config.revision);snprintf(etag,sizeof etag,"ETag: \"q-%llu\"\r\n",(unsigned long long)config.revision);reply_extra(a,fd,ssl,"200 OK",result,encoded.len,is_head,cors,0,etag);free(body);return;
    }
    if ((is_get || is_head) && path_id_after(path,path_len,"/api/admin/v1/streams/",&resource_name)==0) {
        struct sfind found={.target=&resource_name};uint32_t partitions=0;uint64_t revision=0,max_bytes=0,max_age=0;stream_foreach_stats(a->c.streams,sfindcb,&found);if(!found.found||stream_config_snapshot(a->c.streams,(const char *)resource_name.bytes,resource_name.len,&partitions,&max_bytes,&max_age,&revision)!=1){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"404 Not Found",b,error(&o,"not_found","The requested Stream was not found."),is_head,cors,0);free(body);return;}char result[768],etag[64];struct out encoded={result,sizeof result,0,0};put(&encoded,"{\"data\":{");identifier_field(&encoded,"name",&resource_name);put(&encoded,",\"partition_count\":%u,\"max_retained_bytes\":%llu,\"max_retained_age_ms\":%llu,\"retained_bytes\":%llu,\"retained_record_count\":%llu,\"revision\":%llu}",partitions,(unsigned long long)max_bytes,(unsigned long long)max_age,(unsigned long long)found.bytes,(unsigned long long)found.records,(unsigned long long)revision);put(&encoded,"}");snprintf(etag,sizeof etag,"ETag: \"s-%llu\"\r\n",(unsigned long long)revision);reply_extra(a,fd,ssl,"200 OK",result,encoded.len,is_head,cors,0,etag);free(body);return;
    }
    AdminName partition_stream;
    if ((is_get || is_head) && stream_partitions_path(path,path_len,&partition_stream)==0) {
        StreamPartitionStats parts[STREAM_PARTITIONS_MAX];uint32_t count=0;uint64_t revision=0;
        int got=stream_partition_snapshot(a->c.streams,(const char *)partition_stream.bytes,partition_stream.len,parts,STREAM_PARTITIONS_MAX,&count,&revision);
        if(got!=1){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,got==0?"404 Not Found":"500 Internal Server Error",b,error(&o,got==0?"not_found":"internal_error",got==0?"The requested Stream was not found.":"The Stream partition snapshot could not be created."),is_head,cors,0);free(body);return;}
        char *result=malloc(ADMIN_BODY_MAX);if(!result){free(body);return;}struct out encoded={result,ADMIN_BODY_MAX,0,0};put(&encoded,"{\"data\":[");for(uint32_t p=0;p<count;p++){if(p)put(&encoded,",");put(&encoded,"{\"partition\":%u,\"base_offset\":%llu,\"next_offset\":%llu,\"retained_bytes\":%llu}",p,(unsigned long long)parts[p].base_offset,(unsigned long long)parts[p].next_offset,(unsigned long long)parts[p].retained_bytes);}put(&encoded,"],\"meta\":{\"count\":%u,\"limit\":%u,\"next_cursor\":null,\"snapshot_revision\":%llu,\"weakly_consistent\":false}}",count,count,(unsigned long long)revision);if(encoded.full){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"500 Internal Server Error",b,error(&o,"internal_error","The Stream partition snapshot could not be encoded."),is_head,cors,0);}else reply(a,fd,ssl,"200 OK",result,encoded.len,is_head,cors,0);free(result);free(body);return;
    }
    AdminName stream_name; uint32_t stream_partition=0;
    if ((is_get || is_head) && stream_groups_path(path,path_len,&stream_name)==0) {
        unsigned limit=100;if(limit_from(path,path_len,&limit)){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"400 Bad Request",b,error(&o,"bad_request","The limit parameter is invalid."),is_head,cors,0);free(body);return;}struct gsnap *groups=calloc(1,sizeof *groups);if(!groups){free(body);return;}stream_group_foreach_stats(a->c.streams,gcb,groups);char *result=malloc(ADMIN_BODY_MAX);if(!result){free(groups);free(body);return;}struct out encoded={result,ADMIN_BODY_MAX,0,0};put(&encoded,"{\"data\":[");size_t count=0;int truncated=groups->trunc;for(size_t i=0;i<groups->n;i++){if(groups->a[i].stream.len!=stream_name.len||memcmp(groups->a[i].stream.bytes,stream_name.bytes,stream_name.len))continue;if(count>=limit){truncated=1;continue;}if(count)put(&encoded,",");put(&encoded,"{");identifier_field(&encoded,"group",&groups->a[i].group);put(&encoded,",\"generation\":%llu,\"active_member_count\":%u}",(unsigned long long)groups->a[i].generation,groups->a[i].members);count++;}collection_end(&encoded,count,limit,truncated);reply(a,fd,ssl,"200 OK",result,encoded.len,is_head,cors,0);free(result);free(groups);free(body);return;
    }
    uint64_t exact_offset=0;
    if ((is_get || is_head) && stream_record_path(path,path_len,&stream_name,&stream_partition,&exact_offset)==0) {
        uint64_t max_bytes=ADMIN_BODY_MAX/2;int supplied=query_u64(path,path_len,"max_bytes",&max_bytes);if(supplied<0||!max_bytes||max_bytes>(ADMIN_BODY_MAX/2)){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"400 Bad Request",b,error(&o,"validation_failed","max_bytes is invalid."),is_head,cors,0);free(body);return;}
        StreamRecordView *records=NULL;uint32_t count=0;int fetched=stream_fetch(a->c.streams,(const char *)stream_name.bytes,stream_name.len,stream_partition,exact_offset,1,max_bytes,&records,&count);
        if(fetched!=1||!count||records[0].offset!=exact_offset){stream_fetch_free(records,count);char b[256];struct out o={b,sizeof b,0,0};const char*status=fetched==-2?"413 Payload Too Large":fetched==1?"404 Not Found":fetched==0?"404 Not Found":"500 Internal Server Error";const char*code=fetched==-2?"request_too_large":fetched==1||fetched==0?"not_found":"internal_error";reply(a,fd,ssl,status,b,error(&o,code,fetched==-2?"The record exceeds max_bytes.":"The requested record was not found."),is_head,cors,0);free(body);return;}
        size_t cap=((size_t)records[0].len+records[0].key_len)*2+640;char*result=malloc(cap);if(!result){stream_fetch_free(records,count);free(body);return;}struct out encoded={result,cap,0,0};put(&encoded,"{\"data\":{\"partition\":%u,\"offset\":%llu,\"key\":",stream_partition,(unsigned long long)records[0].offset);if(records[0].key_len){put(&encoded,"{\"encoding\":\"base64\",\"data\":");json_b64(&encoded,(const unsigned char *)records[0].key,records[0].key_len);put(&encoded,",\"size\":%u,\"content_type\":\"application/octet-stream\"}",records[0].key_len);}else put(&encoded,"null");put(&encoded,",\"body\":{\"encoding\":\"base64\",\"data\":");json_b64(&encoded,(const unsigned char *)records[0].data,records[0].len);put(&encoded,",\"size\":%u,\"content_type\":\"application/octet-stream\"}}}",records[0].len);if(encoded.full){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"413 Payload Too Large",b,error(&o,"request_too_large","The record response exceeds the configured bound."),is_head,cors,0);}else reply(a,fd,ssl,"200 OK",result,encoded.len,is_head,cors,0);free(result);stream_fetch_free(records,count);free(body);return;
    }
    if ((is_get || is_head) && stream_records_path(path,path_len,&stream_name,&stream_partition)==0) {
        uint64_t offset=0,max_bytes=0,max_records=0;int have_records=query_u64(path,path_len,"max_records",&max_records),have_bytes=query_u64(path,path_len,"max_bytes",&max_bytes),have_offset=query_u64(path,path_len,"offset",&offset);
        if(have_records!=1||have_bytes!=1||have_offset<0||!max_records||max_records>500||max_bytes<16||max_bytes>(ADMIN_BODY_MAX/2)){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"400 Bad Request",b,error(&o,"validation_failed","offset, max_records, and max_bytes are required bounded parameters."),is_head,cors,0);free(body);return;}
        StreamRecordView *records=NULL;uint32_t count=0;int fetched=stream_fetch(a->c.streams,(const char *)stream_name.bytes,stream_name.len,stream_partition,offset,(uint32_t)max_records,max_bytes,&records,&count);
        if(fetched!=1){char b[256];struct out o={b,sizeof b,0,0};const char*status=fetched==0?"404 Not Found":fetched==-2?"413 Payload Too Large":"500 Internal Server Error";const char*code=fetched==0?"not_found":fetched==-2?"request_too_large":"internal_error";reply(a,fd,ssl,status,b,error(&o,code,fetched==-2?"The next record exceeds max_bytes.":"The requested resource was not found."),is_head,cors,0);free(body);return;}
        size_t cap=(size_t)max_bytes*2+4096;char*result=malloc(cap);if(!result){stream_fetch_free(records,count);free(body);return;}struct out encoded={result,cap,0,0};put(&encoded,"{\"data\":[");for(uint32_t i=0;i<count;i++){if(i)put(&encoded,",");put(&encoded,"{\"partition\":%u,\"offset\":%llu,\"key\":",stream_partition,(unsigned long long)records[i].offset);if(records[i].key_len){put(&encoded,"{\"encoding\":\"base64\",\"data\":");json_b64(&encoded,(const unsigned char *)records[i].key,records[i].key_len);put(&encoded,",\"size\":%u,\"content_type\":\"application/octet-stream\"}",records[i].key_len);}else put(&encoded,"null");put(&encoded,",\"body\":{\"encoding\":\"base64\",\"data\":");json_b64(&encoded,(const unsigned char *)records[i].data,records[i].len);put(&encoded,",\"size\":%u,\"content_type\":\"application/octet-stream\"}}",records[i].len);}put(&encoded,"],\"meta\":{\"count\":%u,\"limit\":%llu,\"next_cursor\":null,\"snapshot_revision\":0,\"weakly_consistent\":true}}",count,(unsigned long long)max_records);if(encoded.full){free(result);stream_fetch_free(records,count);char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"413 Payload Too Large",b,error(&o,"request_too_large","The response exceeds the configured bound."),is_head,cors,0);}else{reply(a,fd,ssl,"200 OK",result,encoded.len,is_head,cors,0);free(result);stream_fetch_free(records,count);}free(body);return;
    }
    AdminName group_name;
    if ((is_get || is_head) && group_path(path,path_len,"/members",&stream_name,&group_name)==0) {
        unsigned limit=100;if(limit_from(path,path_len,&limit)){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"400 Bad Request",b,error(&o,"bad_request","The limit parameter is invalid."),is_head,cors,0);free(body);return;}AdminGroupMemberSnapshot snapshot={0};uint64_t generation=0;uint32_t member_count=0;int found=stream_group_member_snapshot(a->c.streams,(const char *)stream_name.bytes,stream_name.len,(const char *)group_name.bytes,group_name.len,group_member_cb,&snapshot,&generation,&member_count);if(found!=1){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,found==0?"404 Not Found":"500 Internal Server Error",b,error(&o,found==0?"not_found":"internal_error",found==0?"The requested Consumer Group was not found.":"The member snapshot could not be created."),is_head,cors,0);free(body);return;}char *result=malloc(ADMIN_BODY_MAX);if(!result){free(body);return;}struct out encoded={result,ADMIN_BODY_MAX,0,0};put(&encoded,"{\"data\":[");uint32_t returned=snapshot.count<limit?snapshot.count:limit;for(uint32_t i=0;i<returned;i++){if(i)put(&encoded,",");put(&encoded,"{\"member_id\":\"member-%u\",\"assigned_partition_count\":%u,\"lease_remaining_ms\":%llu}",snapshot.members[i].index,snapshot.members[i].assigned,(unsigned long long)snapshot.members[i].lease_remaining_ms);}put(&encoded,"],\"meta\":{\"count\":%u,\"limit\":%u,\"truncated\":%s,\"snapshot_generation\":%llu}}",returned,limit,snapshot.count>returned?"true":"false",(unsigned long long)generation);reply(a,fd,ssl,"200 OK",result,encoded.len,is_head,cors,0);free(result);free(body);return;
    }
    if ((is_get || is_head) && group_path(path,path_len,"",&stream_name,&group_name)==0) {
        uint64_t offsets[STREAM_PARTITIONS_MAX],lags[STREAM_PARTITIONS_MAX],generation=0;uint32_t count=0;int generation_found=stream_group_generation(a->c.streams,(const char *)stream_name.bytes,stream_name.len,(const char *)group_name.bytes,group_name.len,&generation);for(uint32_t p=0;p<STREAM_PARTITIONS_MAX;p++){uint64_t offset=0,lag=0;int got=stream_group_offset(a->c.streams,(const char *)stream_name.bytes,stream_name.len,(const char *)group_name.bytes,group_name.len,p,&offset);if(got!=1)break;(void)stream_group_lag(a->c.streams,(const char *)stream_name.bytes,stream_name.len,(const char *)group_name.bytes,group_name.len,p,&lag);offsets[count]=offset;lags[count++]=lag;}AdminGroupMemberSnapshot snapshot={0};uint32_t members=0;int member_found=stream_group_member_snapshot(a->c.streams,(const char *)stream_name.bytes,stream_name.len,(const char *)group_name.bytes,group_name.len,group_member_cb,&snapshot,NULL,&members);if(generation_found!=1||member_found!=1||!count){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"404 Not Found",b,error(&o,"not_found","The requested Consumer Group was not found."),is_head,cors,0);free(body);return;}char result[16384],etag[64];struct out encoded={result,sizeof result,0,0};put(&encoded,"{\"data\":{");identifier_field(&encoded,"stream",&stream_name);put(&encoded,",");identifier_field(&encoded,"group",&group_name);put(&encoded,",\"generation\":%llu,\"active_member_count\":%u,\"offsets\":[",(unsigned long long)generation,members);for(uint32_t p=0;p<count;p++){if(p)put(&encoded,",");put(&encoded,"{\"partition\":%u,\"offset\":%llu,\"high_water_offset\":%llu,\"lag\":%llu}",p,(unsigned long long)offsets[p],(unsigned long long)(offsets[p]+lags[p]),(unsigned long long)lags[p]);}put(&encoded,"]}} ");snprintf(etag,sizeof etag,"ETag: \"g-%llu\"\r\n",(unsigned long long)generation);reply_extra(a,fd,ssl,"200 OK",result,encoded.len,is_head,cors,0,etag);free(body);return;
    }
    if (is_get || is_head) {
        AdminGroupSession *session=NULL;int found=group_session_path(a,path,path_len,"/records",&session);
        if(found==-2){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"410 Gone",b,error(&o,"session_expired","The Consumer Group session has expired."),is_head,cors,0);free(body);return;}
        if(found==1){uint64_t partition=0,offset=0,max_records=0,max_bytes=0;int hp=query_u64(path,path_len,"partition",&partition),ho=query_u64(path,path_len,"offset",&offset),hr=query_u64(path,path_len,"max_records",&max_records),hb=query_u64(path,path_len,"max_bytes",&max_bytes);if(hp!=1||partition>UINT32_MAX||ho<0||hr!=1||!max_records||max_records>500||hb!=1||max_bytes<16||max_bytes>(ADMIN_BODY_MAX/2)){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"400 Bad Request",b,error(&o,"validation_failed","partition, offset, max_records, and max_bytes are required bounded parameters."),is_head,cors,0);free(body);return;}if(stream_group_member_assigned(a->c.streams,(const char *)session->stream.bytes,session->stream.len,(const char *)session->group.bytes,session->group.len,session->owner,(uint32_t)partition,NULL)!=1){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"409 Conflict",b,error(&o,"assignment_lost","The session does not own that partition."),is_head,cors,0);free(body);return;}StreamRecordView *records=NULL;uint32_t count=0;int fetched=stream_fetch(a->c.streams,(const char *)session->stream.bytes,session->stream.len,(uint32_t)partition,offset,(uint32_t)max_records,max_bytes,&records,&count);if(fetched!=1){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,fetched==-2?"413 Payload Too Large":fetched==0?"404 Not Found":"500 Internal Server Error",b,error(&o,fetched==-2?"request_too_large":fetched==0?"not_found":"internal_error",fetched==-2?"The next record exceeds max_bytes.":"The requested resource was not found."),is_head,cors,0);free(body);return;}size_t cap=(size_t)max_bytes*2+4096;char *result=malloc(cap);if(!result){stream_fetch_free(records,count);free(body);return;}struct out encoded={result,cap,0,0};put(&encoded,"{\"data\":[");for(uint32_t i=0;i<count;i++){if(i)put(&encoded,",");put(&encoded,"{\"partition\":%llu,\"offset\":%llu,\"key\":",(unsigned long long)partition,(unsigned long long)records[i].offset);if(records[i].key_len){put(&encoded,"{\"encoding\":\"base64\",\"data\":");json_b64(&encoded,(const unsigned char *)records[i].key,records[i].key_len);put(&encoded,",\"size\":%u,\"content_type\":\"application/octet-stream\"}",records[i].key_len);}else put(&encoded,"null");put(&encoded,",\"body\":{\"encoding\":\"base64\",\"data\":");json_b64(&encoded,(const unsigned char *)records[i].data,records[i].len);put(&encoded,",\"size\":%u,\"content_type\":\"application/octet-stream\"}}",records[i].len);}put(&encoded,"],\"meta\":{\"count\":%u,\"limit\":%llu}}",count,(unsigned long long)max_records);if(encoded.full){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"413 Payload Too Large",b,error(&o,"request_too_large","The response exceeds the configured bound."),is_head,cors,0);}else reply(a,fd,ssl,"200 OK",result,encoded.len,is_head,cors,0);free(result);stream_fetch_free(records,count);free(body);return;}
    }
    if ((is_get || is_head) && group_offsets_path(path,path_len,&stream_name,&group_name)==0) {
        uint64_t offsets[STREAM_PARTITIONS_MAX],lags[STREAM_PARTITIONS_MAX],generation=0;uint32_t count=0;int generation_found=stream_group_generation(a->c.streams,(const char *)stream_name.bytes,stream_name.len,(const char *)group_name.bytes,group_name.len,&generation);for(uint32_t p=0;p<STREAM_PARTITIONS_MAX;p++){uint64_t offset=0,lag=0;int got=stream_group_offset(a->c.streams,(const char *)stream_name.bytes,stream_name.len,(const char *)group_name.bytes,group_name.len,p,&offset);if(got!=1)break;(void)stream_group_lag(a->c.streams,(const char *)stream_name.bytes,stream_name.len,(const char *)group_name.bytes,group_name.len,p,&lag);offsets[count]=offset;lags[count++]=lag;}
        if(!count){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"404 Not Found",b,error(&o,"not_found","The requested Consumer Group was not found."),is_head,cors,0);free(body);return;}
        char result[16384],etag[64];struct out encoded={result,sizeof result,0,0};put(&encoded,"{\"data\":[");for(uint32_t p=0;p<count;p++){if(p)put(&encoded,",");put(&encoded,"{\"partition\":%u,\"offset\":%llu,\"lag\":%llu}",p,(unsigned long long)offsets[p],(unsigned long long)lags[p]);}put(&encoded,"],\"meta\":{\"count\":%u,\"limit\":%u,\"next_cursor\":null,\"snapshot_revision\":%llu,\"weakly_consistent\":true}}",count,count,(unsigned long long)(generation_found==1?generation:0));snprintf(etag,sizeof etag,"ETag: \"g-%llu\"\r\n",(unsigned long long)generation);reply_extra(a,fd,ssl,"200 OK",result,encoded.len,is_head,cors,0,etag);free(body);return;
    }
    if (is_get || is_head) {
        AdminGroupSession *session=NULL;int found=group_session_path(a,path,path_len,"",&session);
        if(found==-2){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"410 Gone",b,error(&o,"session_expired","The Consumer Group session has expired."),is_head,cors,0);free(body);return;}
        if(found==1){uint64_t generation=0,now=admin_mono_ms();(void)stream_group_generation(a->c.streams,(const char *)session->stream.bytes,session->stream.len,(const char *)session->group.bytes,session->group.len,&generation);char result[1024];struct out encoded={result,sizeof result,0,0};put(&encoded,"{\"data\":{\"session_id\":\"");put(&encoded,"%s",session->id);put(&encoded,"\",");identifier_field(&encoded,"stream",&session->stream);put(&encoded,",");identifier_field(&encoded,"group",&session->group);put(&encoded,",\"generation\":%llu,\"state\":\"active\",\"lease_remaining_ms\":%llu}}",(unsigned long long)generation,(unsigned long long)(session->deadline_ms>now?session->deadline_ms-now:0));reply(a,fd,ssl,"200 OK",result,encoded.len,is_head,cors,0);free(body);return;}
    }
    {const char*query=memchr(path,'?',path_len);size_t bare=query?(size_t)(query-path):path_len;AdminName router_name;
     if((is_get||is_head)&&bare==29&&!memcmp(path,"/api/admin/v1/routing/routers",29)){unsigned limit=100;if(limit_from(path,path_len,&limit)){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"400 Bad Request",b,error(&o,"bad_request","The limit parameter is invalid."),is_head,cors,0);free(body);return;}AdminRouterSnapshot snapshot={0};exchange_foreach_stats(a->c.queues,router_cb,&snapshot);char *result=malloc(ADMIN_BODY_MAX);if(!result){free(body);return;}struct out encoded={result,ADMIN_BODY_MAX,0,0};put(&encoded,"{\"data\":[");uint32_t returned=snapshot.count<limit?snapshot.count:limit;for(uint32_t i=0;i<returned;i++){AdminRouter*r=&snapshot.routers[i];const char*mode=r->type==EXCHANGE_DIRECT?"exact":r->type==EXCHANGE_FANOUT?"broadcast":"pattern";if(i)put(&encoded,",");put(&encoded,"{");identifier_field(&encoded,"name",&r->name);put(&encoded,",\"mode\":\"%s\",\"durable\":%s,\"route_count\":%u,\"revision\":%llu,\"publish_attempt_count\":%llu,\"unroutable_count\":%llu,\"metrics_scope\":\"process_lifetime\",\"alternate_router\":",mode,r->durable?"true":"false",r->routes,(unsigned long long)r->revision,(unsigned long long)r->published,(unsigned long long)r->unroutable);if(r->alternate.len){put(&encoded,"{");identifier_field(&encoded,"name",&r->alternate);put(&encoded,"}");}else put(&encoded,"null");put(&encoded,"}");}collection_end(&encoded,returned,limit,snapshot.truncated||snapshot.count>returned);reply(a,fd,ssl,"200 OK",result,encoded.len,is_head,cors,0);free(result);free(body);return;}
     AdminRoute route_resource={0};uint64_t route_revision=0;
     if((is_get||is_head)&&router_route_path(path,path_len,&router_name,&route_resource)==0){int found=exchange_route_exists(a->c.queues,(const char*)router_name.bytes,router_name.len,(const char*)route_resource.queue.bytes,route_resource.queue.len,(const char*)route_resource.key.bytes,route_resource.key.len);if(found!=1||exchange_revision(a->c.queues,(const char*)router_name.bytes,router_name.len,&route_revision)!=1){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"404 Not Found",b,error(&o,"not_found","The requested route was not found."),is_head,cors,0);free(body);return;}char result[2048],extra[64];struct out encoded={result,sizeof result,0,0};put(&encoded,"{\"data\":{");route_id_field(&encoded,&route_resource);put(&encoded,",\"queue\":{");identifier_field(&encoded,"name",&route_resource.queue);put(&encoded,"},\"routing_key\":{");identifier_field(&encoded,"value",&route_resource.key);put(&encoded,"}}}");snprintf(extra,sizeof extra,"ETag: \"r-%llu\"\r\n",(unsigned long long)route_revision);reply_extra(a,fd,ssl,"200 OK",result,encoded.len,is_head,cors,0,extra);free(body);return;}
     if((is_get||is_head)&&path_id_action(path,path_len,"/api/admin/v1/routing/routers/","/routes",&router_name)==0){unsigned limit=100;if(limit_from(path,path_len,&limit)){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"400 Bad Request",b,error(&o,"bad_request","The limit parameter is invalid."),is_head,cors,0);free(body);return;}AdminRouteSnapshot snapshot={0};uint32_t routes=0;int found=exchange_foreach_route(a->c.queues,(const char*)router_name.bytes,router_name.len,route_cb,&snapshot,&routes);if(found!=1){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"404 Not Found",b,error(&o,"not_found","The requested router was not found."),is_head,cors,0);free(body);return;}char *result=malloc(ADMIN_BODY_MAX);if(!result){free(body);return;}struct out encoded={result,ADMIN_BODY_MAX,0,0};put(&encoded,"{\"data\":[");uint32_t returned=snapshot.count<limit?snapshot.count:limit;for(uint32_t i=0;i<returned;i++){if(i)put(&encoded,",");put(&encoded,"{");route_id_field(&encoded,&snapshot.routes[i]);put(&encoded,",\"queue\":{");identifier_field(&encoded,"name",&snapshot.routes[i].queue);put(&encoded,"},\"routing_key\":{");identifier_field(&encoded,"value",&snapshot.routes[i].key);put(&encoded,"}}");}collection_end(&encoded,returned,limit,snapshot.truncated||snapshot.count>returned);reply(a,fd,ssl,"200 OK",result,encoded.len,is_head,cors,0);free(result);free(body);return;}
     if((is_get||is_head)&&path_id_after(path,path_len,"/api/admin/v1/routing/routers/",&router_name)==0){AdminRouterSnapshot snapshot={0};exchange_foreach_stats(a->c.queues,router_cb,&snapshot);for(uint32_t i=0;i<snapshot.count;i++)if(snapshot.routers[i].name.len==router_name.len&&!memcmp(snapshot.routers[i].name.bytes,router_name.bytes,router_name.len)){AdminRouter*r=&snapshot.routers[i];const char*mode=r->type==EXCHANGE_DIRECT?"exact":r->type==EXCHANGE_FANOUT?"broadcast":"pattern";char result[2048],extra[64];struct out encoded={result,sizeof result,0,0};put(&encoded,"{\"data\":{");identifier_field(&encoded,"name",&r->name);put(&encoded,",\"mode\":\"%s\",\"durable\":%s,\"route_count\":%u,\"revision\":%llu,\"publish_attempt_count\":%llu,\"unroutable_count\":%llu,\"metrics_scope\":\"process_lifetime\"}}",mode,r->durable?"true":"false",r->routes,(unsigned long long)r->revision,(unsigned long long)r->published,(unsigned long long)r->unroutable);snprintf(extra,sizeof extra,"ETag: \"r-%llu\"\r\n",(unsigned long long)r->revision);reply_extra(a,fd,ssl,"200 OK",result,encoded.len,is_head,cors,0,extra);free(body);return;}char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"404 Not Found",b,error(&o,"not_found","The requested router was not found."),is_head,cors,0);free(body);return;}
    }
    if ((is_get || is_head) && !route_ok(path,path_len)) { char b[256]; struct out o={b,sizeof b,0,0}; reply(a,fd,ssl,"404 Not Found",b,error(&o,"not_found","The requested resource was not found."),is_head,cors,0); free(body); return; }
    { unsigned ignored_limit; if ((is_get || is_head) && limit_from(path,path_len,&ignored_limit)) { char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"400 Bad Request",b,error(&o,"bad_request","The limit parameter is invalid."),is_head,cors,0);free(body);return;} }
    if ((is_get || is_head) && !content_len) { char *result=malloc(ADMIN_BODY_MAX); if(!result){free(body);return;} struct out out={result,ADMIN_BODY_MAX,0,0};int unavailable=0;render(a,path,path_len,&out,&unavailable);reply(a,fd,ssl,unavailable?"503 Service Unavailable":"200 OK",result,out.len,is_head,cors,0);free(result);free(body);return; }
    if (!strcmp(method,"POST") && path_len == 20 && !memcmp(path,"/api/admin/v1/status",20)) { char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"405 Method Not Allowed",b,error(&o,"method_not_allowed","This method is not supported."),0,cors,0);free(body);return; }
    if (strcmp(method,"POST") && strcmp(method,"PUT") && strcmp(method,"PATCH") && strcmp(method,"DELETE")) { char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"405 Method Not Allowed",b,error(&o,"method_not_allowed","This method is not supported."),0,cors,0);free(body);return; }
    if (!content_len || admin_json_validate(body, content_len)) { char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"400 Bad Request",b,error(&o,"validation_failed","The JSON body is invalid."),0,cors,0);free(body);return; }
    size_t idem_len=0; const char *idem=header(headers,header_len,"idempotency-key",&idem_len);
    if (!idem || !idem_len || idem_len > 128) { char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"428 Precondition Required",b,error(&o,"precondition_required","Idempotency-Key is required for this operation."),0,cors,0);free(body);return; }
    audit_idempotency_hash=1469598103934665603ULL;for(size_t i=0;i<idem_len;i++){audit_idempotency_hash^=(unsigned char)idem[i];audit_idempotency_hash*=1099511628211ULL;}
    uint64_t fingerprint=request_fingerprint(method,path,body,content_len);const char *cached=NULL;size_t cached_len=0;int cached_status=0;int cached_rc=idempotency_lookup(a,idem,idem_len,fingerprint,&cached,&cached_len,&cached_status);
    if(cached_rc<0){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"409 Conflict",b,error(&o,"idempotency_conflict","This Idempotency-Key was used for a different request."),0,cors,0);free(body);return;}if(cached_rc>0){reply(a,fd,ssl,http_status(cached_status),cached,cached_len,0,cors,0);free(body);return;}
    AdminJsonSlice name, mode, queue_id, alternate, atomic_kind, atomic_key, atomic_target, batch_items[100]; AdminName target={0}, target2={0}; AdminRoute route_id={0}; QueueMessage delivered={0}; AdminDelivery *delivery=NULL; AdminGroupSession *group_session=NULL; uint32_t *session_parts=NULL,session_assigned=0; uint64_t session_generation=0; unsigned char *payload=NULL, atomic_routing_key[ROUTING_KEY_MAX]; const void *batch_payloads[100], *stream_key=NULL; StreamAppendInput stream_batch[100]; StreamAppendResult stream_batch_results[100]; StreamCommitInput offset_batch[100]; uint32_t batch_lens[100],batch_count=0,group_offset_count=0; size_t batch_item_count=0,payload_len=0,stream_key_len=0,atomic_routing_key_len=0,if_match_len=0,confirm_len=0; uint64_t n1=0,n2=0,age=0,generated_id=0,generated_partition=0,generated_offset=0,transaction_id=0,routed_count=0,expected_generation=0,expected_revision=0; int64_t offset_delta=0; uint32_t group_partition=0; unsigned router_mode=EXCHANGE_DIRECT,atomic_operation=0; StreamOffsetResetStrategy reset_strategy=STREAM_OFFSET_RESET_EARLIEST; int durable=0,deleted=0; int rc=-1, action=0; char success[4096]; size_t success_len=0; int success_status=200; const char *operation=NULL;
    AdminDelivery *delivery_batch[50]; uint64_t delivery_tags[50], delivery_owner=0; int batch_nack=0;
    QueueMessage delivered_batch[50]={{0}}; uint32_t delivered_batch_count=0;
    AdminName key_batch[100]; uint64_t key_ttls[100]; uint32_t batch_applied=0,batch_deleted=0;
    AdminClaim *claim=NULL;
    if (!strcmp(method,"POST") && path_len==20 && !memcmp(path,"/api/admin/v1/queues",20) && admin_json_string(body,content_len,"name",&name)==1 && name.len && name.len<=QUEUE_NAME_MAX) {
        (void)admin_json_bool(body,content_len,"durable",&durable); (void)admin_json_u64(body,content_len,"max_depth",&n1); operation="queue.create"; action=1; success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"name\":\"%.*s\",\"durable\":%s}}",(int)name.len,name.data,durable?"true":"false"); success_status=201;
    } else if (!strcmp(method,"POST") && path_len==21 && !memcmp(path,"/api/admin/v1/streams",21) && admin_json_string(body,content_len,"name",&name)==1 && name.len && name.len<=STREAM_NAME_MAX && admin_json_u64(body,content_len,"partitions",&n1)==1 && n1 && n1<=STREAM_PARTITIONS_MAX) {
        (void)admin_json_u64(body,content_len,"max_retained_bytes",&n2); (void)admin_json_u64(body,content_len,"max_retained_age_ms",&age);operation="stream.create";action=2;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"name\":\"%.*s\",\"partition_count\":%llu}}",(int)name.len,name.data,(unsigned long long)n1);success_status=201;
    } else if (!strcmp(method,"POST") && path_len==38 && !memcmp(path,"/api/admin/v1/routing/default/messages",38) && admin_json_string(body,content_len,"queue_id",&queue_id)==1 && decode_id(queue_id.data,queue_id.len,&target)==0 && (payload=malloc(content_len)) != NULL && json_bytes(body,content_len,"body",payload,content_len,&payload_len)==0) {
        (void)admin_json_u64(body,content_len,"ttl_ms",&n1); operation="routing.default.publish"; action=3; success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"message_id\":0,\"durability\":\"known\"}}"); success_status=201;
    } else if (!strcmp(method,"POST") && path_len==sizeof "/api/admin/v1/keyspaces/default/entries:batch-put"-1 && !memcmp(path,"/api/admin/v1/keyspaces/default/entries:batch-put",path_len)) {
        if(!a->c.keyspace_put||admin_json_object_array(body,content_len,"entries",batch_items,100,&batch_item_count)!=1||!batch_item_count||(payload=malloc(content_len))==NULL){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,a->c.keyspace_put?"400 Bad Request":"503 Service Unavailable",b,error(&o,a->c.keyspace_put?"validation_failed":"engine_unavailable",a->c.keyspace_put?"entries must contain 1..100 exact entry identifiers and Base64 values.":"The Keyspace mutation adapter is unavailable."),0,cors,0);free(body);return;}
        batch_count=(uint32_t)batch_item_count;size_t used=0;int valid=1;for(uint32_t i=0;i<batch_count;i++){AdminJsonSlice id;size_t value_len=0;int ttl_rc=admin_json_u64(batch_items[i].data,batch_items[i].len,"ttl_ms",&key_ttls[i]);if(admin_json_string(batch_items[i].data,batch_items[i].len,"entry_id",&id)!=1||decode_id(id.data,id.len,&key_batch[i])||ttl_rc<0||json_bytes(batch_items[i].data,batch_items[i].len,"value",payload+used,content_len-used,&value_len)){valid=0;break;}batch_payloads[i]=payload+used;batch_lens[i]=(uint32_t)value_len;used+=value_len;}
        if(!valid){free(payload);char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"400 Bad Request",b,error(&o,"validation_failed","Every batch entry needs an exact entry_id, Base64 value, and optional non-negative ttl_ms."),0,cors,0);free(body);return;}
        operation="keyspace.entry.batch_put";action=35;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{}}");success_status=200;
    } else if (!strcmp(method,"POST") && path_len==sizeof "/api/admin/v1/keyspaces/default/entries:batch-delete"-1 && !memcmp(path,"/api/admin/v1/keyspaces/default/entries:batch-delete",path_len)) {
        if(!a->c.keyspace_delete||admin_json_string_array(body,content_len,"entry_ids",batch_items,100,&batch_item_count)!=1||!batch_item_count){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,a->c.keyspace_delete?"400 Bad Request":"503 Service Unavailable",b,error(&o,a->c.keyspace_delete?"validation_failed":"engine_unavailable",a->c.keyspace_delete?"entry_ids must contain 1..100 exact identifiers.":"The Keyspace mutation adapter is unavailable."),0,cors,0);free(body);return;}
        batch_count=(uint32_t)batch_item_count;for(uint32_t i=0;i<batch_count;i++)if(decode_id(batch_items[i].data,batch_items[i].len,&key_batch[i])){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"400 Bad Request",b,error(&o,"validation_failed","Every batch entry_id must be an exact identifier."),0,cors,0);free(body);return;}
        operation="keyspace.entry.batch_delete";action=36;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{}}");success_status=200;
    } else if (!strcmp(method,"POST") && path_len==sizeof "/api/admin/v1/keyspaces/default/claims"-1 && !memcmp(path,"/api/admin/v1/keyspaces/default/claims",path_len) && a->c.keyspace_claim_acquire && admin_json_string(body,content_len,"entry_id",&name)==1 && decode_id(name.data,name.len,&target)==0) {
        int lease_present=admin_json_u64(body,content_len,"lease_ms",&n1);if(!lease_present)n1=30000;
        if(lease_present<0||n1<100||n1>60000||(claim=claim_slot(a))==NULL||make_claim_id(claim->id)||make_private_owner(&claim->owner)){char b[256];struct out o={b,sizeof b,0,0};int exhausted=!claim;reply(a,fd,ssl,exhausted?"429 Too Many Requests":"400 Bad Request",b,error(&o,exhausted?"resource_exhausted":"validation_failed",exhausted?"The claim registry is at capacity.":"entry_id or lease_ms is invalid, or secure claim state could not be created."),0,cors,0);if(exhausted)a->rate_limit_rejections++;free(body);return;}
        memcpy(claim->key.bytes,target.bytes,target.len);claim->key.len=target.len;operation="keyspace.claim.create";action=37;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{}}");success_status=201;
    } else if (!strcmp(method,"POST") && claim_path(a,path,path_len,":complete",&claim)==1 && a->c.keyspace_claim_complete && (payload=malloc(content_len))!=NULL && json_bytes(body,content_len,"value",payload,content_len,&payload_len)==0) {
        (void)admin_json_u64(body,content_len,"ttl_ms",&n1);operation="keyspace.claim.complete";action=38;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{}}");
    } else if (!strcmp(method,"POST") && claim_path(a,path,path_len,":release",&claim)==1 && a->c.keyspace_claim_release) {
        operation="keyspace.claim.release";action=39;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{}}");
    } else if (!strcmp(method,"POST") && (claim_path(a,path,path_len,":complete",&claim)==-2 || claim_path(a,path,path_len,":release",&claim)==-2)) {
        char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"410 Gone",b,error(&o,"claim_expired","The claim has expired."),0,cors,0);free(body);return;
    } else if (!strcmp(method,"POST") && path_id_action(path,path_len,"/api/admin/v1/keyspaces/default/entries/",":get-or-refresh",&target)==0 && a->c.keyspace_claim_acquire) {
        int lease_present=admin_json_u64(body,content_len,"lease_ms",&n1);if(!lease_present)n1=30000;
        if(lease_present<0||n1<100||n1>60000){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"400 Bad Request",b,error(&o,"validation_failed","lease_ms is invalid."),0,cors,0);free(body);return;}
        char *current=NULL;uint32_t current_len=0;int found=kuttidb_get(a->c.keyspace,(const char *)target.bytes,target.len,&current,&current_len);
        if(found<0){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"503 Service Unavailable",b,error(&o,"engine_unavailable","The Keyspace engine is unavailable."),0,cors,0);free(body);return;}
        if(found==1){if(current_len>(ADMIN_BODY_MAX-1024u)/2u){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"413 Payload Too Large",b,error(&o,"request_too_large","The Keyspace value exceeds the response limit."),0,cors,0);free(current);free(body);return;}size_t cap=(size_t)current_len*2+1024;char *encoded=malloc(cap);if(!encoded){free(current);free(body);return;}struct out output={encoded,cap,0,0};put(&output,"{\"data\":{\"outcome\":\"value\",");identifier_field(&output,"key",&target);put(&output,",\"value\":{\"encoding\":\"base64\",\"data\":");json_b64(&output,(const unsigned char *)current,current_len);put(&output,",\"size\":%u,\"content_type\":\"application/octet-stream\"}}}",current_len);if(output.full){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"413 Payload Too Large",b,error(&o,"request_too_large","The Keyspace value exceeds the response limit."),0,cors,0);}else reply(a,fd,ssl,"200 OK",encoded,output.len,0,cors,0);free(encoded);free(current);free(body);return;}
        if((claim=claim_slot(a))==NULL||make_claim_id(claim->id)||make_private_owner(&claim->owner)){char b[256];struct out o={b,sizeof b,0,0};int exhausted=!claim;reply(a,fd,ssl,exhausted?"429 Too Many Requests":"400 Bad Request",b,error(&o,exhausted?"resource_exhausted":"validation_failed",exhausted?"The claim registry is at capacity.":"Secure claim state could not be created."),0,cors,0);if(exhausted)a->rate_limit_rejections++;free(body);return;}
        memcpy(claim->key.bytes,target.bytes,target.len);claim->key.len=target.len;operation="keyspace.entry.get_or_refresh";action=40;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{}}");success_status=201;
    } else if (!strcmp(method,"POST") && path_id_action(path,path_len,"/api/admin/v1/queues/","/messages:batch",&target)==0 && admin_json_object_array(body,content_len,"messages",batch_items,100,&batch_item_count)==1 && batch_item_count && (batch_count=(uint32_t)batch_item_count) && (payload=malloc(content_len)) != NULL) {
        size_t used_payload=0;int valid=1;for(uint32_t i=0;i<batch_count;i++){size_t item_len=0;if(json_bytes(batch_items[i].data,batch_items[i].len,"body",payload+used_payload,content_len-used_payload,&item_len)||!item_len){valid=0;break;}batch_payloads[i]=payload+used_payload;batch_lens[i]=(uint32_t)item_len;used_payload+=item_len;}
        if(!valid){free(payload);char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"400 Bad Request",b,error(&o,"validation_failed","messages must contain 1..100 complete Base64 bodies."),0,cors,0);free(body);return;}
        operation="queue.publish.batch";action=19;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"message_count\":%u,\"durability\":\"known\"}}",batch_count);success_status=201;
    } else if (!strcmp(method,"POST") && path_id_action(path,path_len,"/api/admin/v1/streams/","/records:batch",&target)==0 && admin_json_object_array(body,content_len,"records",batch_items,100,&batch_item_count)==1 && batch_item_count && (batch_count=(uint32_t)batch_item_count) && (payload=malloc(content_len)) != NULL) {
        int partition_present=admin_json_u64(body,content_len,"partition",&n1);if(partition_present==0)n1=UINT32_MAX;
        size_t used_payload=0;int valid=partition_present>=0&&n1<=UINT32_MAX;for(uint32_t i=0;valid&&i<batch_count;i++){size_t item_len=0,item_key_len=0;int key_rc;if(json_bytes(batch_items[i].data,batch_items[i].len,"body",payload+used_payload,content_len-used_payload,&item_len)){valid=0;break;}stream_batch[i].data=payload+used_payload;stream_batch[i].len=(uint32_t)item_len;used_payload+=item_len;key_rc=json_optional_bytes(batch_items[i].data,batch_items[i].len,"key",payload+used_payload,content_len-used_payload,&item_key_len);if(key_rc<0||item_key_len>UINT16_MAX){valid=0;break;}stream_batch[i].key=item_key_len?payload+used_payload:NULL;stream_batch[i].key_len=(uint16_t)item_key_len;used_payload+=item_key_len;}
        if(!valid){free(payload);char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"400 Bad Request",b,error(&o,"validation_failed","records must contain 1..100 complete Base64 bodies, optional Base64 keys, and a valid partition."),0,cors,0);free(body);return;}
        operation="stream.append.batch";action=20;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":[]}");success_status=201;
    } else if (!strcmp(method,"POST") && path_id_action(path,path_len,"/api/admin/v1/queues/","/messages",&target)==0 && (payload=malloc(content_len)) != NULL && json_bytes(body,content_len,"body",payload,content_len,&payload_len)==0) {
        (void)admin_json_u64(body,content_len,"ttl_ms",&n1); operation="queue.publish"; action=3;
        success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"message_id\":0,\"durability\":\"known\"}}"); success_status=201;
    } else if (!strcmp(method,"POST") && path_id_action(path,path_len,"/api/admin/v1/streams/","/records",&target)==0 && (payload=malloc(content_len)) != NULL && json_bytes(body,content_len,"body",payload,content_len,&payload_len)==0 && json_optional_bytes(body,content_len,"key",payload+payload_len,content_len-payload_len,&stream_key_len)>=0 && stream_key_len<=UINT16_MAX) {
        int partition_present=admin_json_u64(body,content_len,"partition",&n1); if(partition_present==0)n1=UINT32_MAX;
        if(partition_present<0 || n1>UINT32_MAX){free(payload);char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"400 Bad Request",b,error(&o,"validation_failed","The partition is invalid."),0,cors,0);free(body);return;}
        stream_key=stream_key_len?payload+payload_len:NULL;
        operation="stream.append"; action=4; success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"partition\":0,\"offset\":0,\"durability\":\"known\"}}"); success_status=201;
    } else if (!strcmp(method,"PUT") && a->c.keyspace_put && path_id_after(path,path_len,"/api/admin/v1/keyspaces/default/entries/",&target)==0 && (payload=malloc(content_len)) != NULL && json_bytes(body,content_len,"value",payload,content_len,&payload_len)==0) {
        (void)admin_json_u64(body,content_len,"ttl_ms",&n1);operation="keyspace.entry.put";action=11;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"updated\":true,\"durability\":\"known\"}}");success_status=200;
    } else if (!strcmp(method,"DELETE") && a->c.keyspace_delete && path_id_after(path,path_len,"/api/admin/v1/keyspaces/default/entries/",&target)==0) {
        operation="keyspace.entry.delete";action=12;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"deleted\":false,\"durability\":\"known\"}}");success_status=200;
    } else if (!strcmp(method,"POST") && path_id_action(path,path_len,"/api/admin/v1/queues/",":purge",&target)==0) {
        const char *if_match=header(headers,header_len,"if-match",&if_match_len),*confirm=header(headers,header_len,"x-kuttidb-confirm",&confirm_len);const char *segment=path+21;const char *suffix=strstr(segment,":purge");
        if(!suffix||!confirm||confirm_len!=(size_t)(suffix-segment)||memcmp(confirm,segment,confirm_len)||queue_etag(if_match,if_match_len,&expected_revision)){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"428 Precondition Required",b,error(&o,"precondition_required","Current If-Match and exact X-KuttiDB-Confirm are required for Queue purge."),0,cors,0);free(body);return;}
        operation="queue.purge";action=16;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"removed_count\":0,\"durability\":\"known\"}}");
    } else if (!strcmp(method,"PATCH") && path_id_after(path,path_len,"/api/admin/v1/queues/",&target)==0) {
        const char *if_match=header(headers,header_len,"if-match",&if_match_len);uint64_t current_revision=0;
        if(queue_etag(if_match,if_match_len,&expected_revision)){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"428 Precondition Required",b,error(&o,"precondition_required","If-Match with the current Queue ETag is required for Queue updates."),0,cors,0);free(body);return;}
        if(queue_revision(a->c.queues,(const char *)target.bytes,target.len,&current_revision)!=1){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"404 Not Found",b,error(&o,"not_found","The requested Queue was not found."),0,cors,0);free(body);return;}
        if(current_revision!=expected_revision){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"412 Precondition Failed",b,error(&o,"precondition_failed","The Queue changed. Refresh it and try again."),0,cors,0);free(body);return;}
        {char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"409 Conflict",b,error(&o,"unsupported_option","Queue declaration options are immutable in the current durable Queue engine."),0,cors,0);free(body);return;}
    } else if (!strcmp(method,"DELETE") && path_id_after(path,path_len,"/api/admin/v1/queues/",&target)==0) {
        const char *if_match=header(headers,header_len,"if-match",&if_match_len),*confirm=header(headers,header_len,"x-kuttidb-confirm",&confirm_len);const char *segment=path+21;
        if(!confirm||confirm_len!=path_len-(size_t)(segment-path)||memcmp(confirm,segment,confirm_len)||queue_etag(if_match,if_match_len,&expected_revision)){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"428 Precondition Required",b,error(&o,"precondition_required","Current If-Match and exact X-KuttiDB-Confirm are required for Queue deletion."),0,cors,0);free(body);return;}
        operation="queue.delete";action=42;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"deleted\":true,\"removed_count\":0,\"durability\":\"known\"}}");
    } else if (!strcmp(method,"PATCH") && path_id_after(path,path_len,"/api/admin/v1/streams/",&target)==0 && admin_json_u64(body,content_len,"max_retained_bytes",&n1)==1 && admin_json_u64(body,content_len,"max_retained_age_ms",&age)==1) {
        const char *if_match=header(headers,header_len,"if-match",&if_match_len);
        if(stream_etag(if_match,if_match_len,&expected_revision)){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"428 Precondition Required",b,error(&o,"precondition_required","If-Match with the current Stream ETag is required for retention updates."),0,cors,0);free(body);return;}
        operation="stream.retention.update";action=46;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"updated\":true,\"durability\":\"known\"}}");
    } else if (!strcmp(method,"DELETE") && path_id_after(path,path_len,"/api/admin/v1/streams/",&target)==0) {
        static const char prefix[]="/api/admin/v1/streams/";const char *segment=path+sizeof prefix-1,*confirm=header(headers,header_len,"x-kuttidb-confirm",&confirm_len),*if_match=header(headers,header_len,"if-match",&if_match_len);
        if(!confirm||confirm_len!=path_len-(size_t)(segment-path)||memcmp(confirm,segment,confirm_len)||stream_etag(if_match,if_match_len,&expected_revision)){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"428 Precondition Required",b,error(&o,"precondition_required","Current If-Match and exact X-KuttiDB-Confirm are required for Stream deletion."),0,cors,0);free(body);return;}
        operation="stream.delete.schedule";action=47;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"job_id\":\"j-0\",\"state\":\"queued\"}}");success_status=202;
    } else if (!strcmp(method,"POST") && stream_truncate_path(path,path_len,&target,&group_partition)==0 && admin_json_u64(body,content_len,"base_offset",&n1)==1) {
        const char *if_match=header(headers,header_len,"if-match",&if_match_len),*confirm=header(headers,header_len,"x-kuttidb-confirm",&confirm_len);const char *segment=path+22;const char *marker=strstr(segment,"/partitions/");
        if(!marker||!confirm||confirm_len!=(size_t)(marker-segment)||memcmp(confirm,segment,confirm_len)||stream_etag(if_match,if_match_len,&expected_revision)){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"428 Precondition Required",b,error(&o,"precondition_required","Current If-Match and exact X-KuttiDB-Confirm are required for Stream truncation."),0,cors,0);free(body);return;}
        operation="stream.partition.truncate.schedule";action=17;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"job_id\":\"j-0\",\"state\":\"queued\"}}");success_status=202;
    } else if (!strcmp(method,"DELETE") && job_path_id(path,path_len,&n1)==0) {
        operation="job.cancel";action=18;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"cancelled\":true}}");
    } else if (!strcmp(method,"POST") && path_len==45 && !memcmp(path,"/api/admin/v1/maintenance/keyspace-checkpoint",45)) {
        operation="maintenance.keyspace.checkpoint.schedule";action=23;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"job_id\":\"j-0\",\"state\":\"queued\"}}");success_status=202;
    } else if (!strcmp(method,"POST") && path_len==42 && !memcmp(path,"/api/admin/v1/maintenance/queue-checkpoint",42)) {
        operation="maintenance.queue.checkpoint.schedule";action=21;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"job_id\":\"j-0\",\"state\":\"queued\"}}");success_status=202;
    } else if (!strcmp(method,"POST") && path_len==43 && !memcmp(path,"/api/admin/v1/maintenance/stream-checkpoint",43)) {
        operation="maintenance.stream.checkpoint.schedule";action=22;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"job_id\":\"j-0\",\"state\":\"queued\"}}");success_status=202;
    } else if (!strcmp(method,"POST") && path_len==40 && !memcmp(path,"/api/admin/v1/maintenance/checkpoint-all",40)) {
        operation="maintenance.checkpoint.all.schedule";action=24;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"job_id\":\"j-0\",\"state\":\"queued\"}}");success_status=202;
    } else if (!strcmp(method,"POST") && group_offset_batch_path(path,path_len,&target,&target2)==0 && admin_json_object_array(body,content_len,"offsets",batch_items,100,&batch_item_count)==1 && batch_item_count && (batch_count=(uint32_t)batch_item_count)) {
        const char *if_match=header(headers,header_len,"if-match",&if_match_len);int valid=!generation_etag(if_match,if_match_len,&expected_generation);for(uint32_t i=0;valid&&i<batch_count;i++){uint64_t partition=0,offset=0;if(admin_json_u64(batch_items[i].data,batch_items[i].len,"partition",&partition)!=1||partition>UINT32_MAX||admin_json_u64(batch_items[i].data,batch_items[i].len,"offset",&offset)!=1){valid=0;break;}offset_batch[i]=(StreamCommitInput){.partition=(uint32_t)partition,.offset=offset};}
        if(!valid){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"400 Bad Request",b,error(&o,"validation_failed","offsets must contain 1..100 partition and offset pairs with a current group ETag."),0,cors,0);free(body);return;}
        operation="stream.group.offset.batch_commit";action=26;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":[]}");
    } else if (!strcmp(method,"POST") && group_offset_reset_path(path,path_len,&target,&target2)==0 && admin_json_string(body,content_len,"strategy",&name)==1) {
        const char *if_match=header(headers,header_len,"if-match",&if_match_len),*confirm=header(headers,header_len,"x-kuttidb-confirm",&confirm_len);static const char marker[]="/consumer-groups/",suffix[]=":reset-offsets";const char *mark=memmem(path,path_len,marker,sizeof marker-1),*group_segment=mark?mark+sizeof marker-1:NULL;size_t group_segment_len=mark?(size_t)((path+path_len-(sizeof suffix-1))-group_segment):0;
        if(!mark||!confirm||confirm_len!=group_segment_len||memcmp(confirm,group_segment,confirm_len)||generation_etag(if_match,if_match_len,&expected_generation)){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"428 Precondition Required",b,error(&o,"precondition_required","Current group If-Match and exact X-KuttiDB-Confirm are required for offset reset."),0,cors,0);free(body);return;}
        if(name.len==8&&!memcmp(name.data,"earliest",8))reset_strategy=STREAM_OFFSET_RESET_EARLIEST;else if(name.len==6&&!memcmp(name.data,"latest",6))reset_strategy=STREAM_OFFSET_RESET_LATEST;else if(name.len==8&&!memcmp(name.data,"absolute",8)){reset_strategy=STREAM_OFFSET_RESET_ABSOLUTE;if(admin_json_u64(body,content_len,"offset",&n1)!=1){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"400 Bad Request",b,error(&o,"validation_failed","absolute reset requires a non-negative offset."),0,cors,0);free(body);return;}}else if(name.len==8&&!memcmp(name.data,"relative",8)){reset_strategy=STREAM_OFFSET_RESET_RELATIVE;if(admin_json_i64(body,content_len,"delta",&offset_delta)!=1){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"400 Bad Request",b,error(&o,"validation_failed","relative reset requires an integer delta."),0,cors,0);free(body);return;}}else{char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"400 Bad Request",b,error(&o,"validation_failed","strategy must be earliest, latest, absolute, or relative."),0,cors,0);free(body);return;}
        int force_present=admin_json_bool(body,content_len,"force",&durable);if(force_present<0){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"400 Bad Request",b,error(&o,"validation_failed","force must be a boolean when present."),0,cors,0);free(body);return;}if(!force_present)durable=0;
        operation="stream.group.offset.reset";action=27;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{}}");
    } else if (!strcmp(method,"POST") && group_sessions_path(path,path_len,&target,&target2)==0) {
        const char *confirm=header(headers,header_len,"x-kuttidb-confirm",&confirm_len);static const char marker[]="/consumer-groups/",suffix[]="/sessions";const char *mark=memmem(path,path_len,marker,sizeof marker-1),*group_segment=mark?mark+sizeof marker-1:NULL;size_t group_segment_len=mark?(size_t)((path+path_len-(sizeof suffix-1))-group_segment):0;
        int lease_present=admin_json_u64(body,content_len,"lease_ms",&n1);if(lease_present==0)n1=30000;if(lease_present<0||n1<100||n1>60000||!mark||!confirm||confirm_len!=group_segment_len||memcmp(confirm,group_segment,confirm_len)){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,confirm?"400 Bad Request":"428 Precondition Required",b,error(&o,confirm?"validation_failed":"precondition_required",confirm?"lease_ms is invalid or X-KuttiDB-Confirm does not match the group identifier.":"Explicit group confirmation is required to create a Consumer Group session."),0,cors,0);free(body);return;}
        if(!(group_session=group_session_slot(a))){a->rate_limit_rejections++;char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"429 Too Many Requests",b,error(&o,"resource_exhausted","The Consumer Group session registry is at capacity."),0,cors,0);free(body);return;}if(make_group_session_id(group_session->id)){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"503 Service Unavailable",b,error(&o,"entropy_unavailable","A secure session identifier could not be created."),0,cors,0);free(body);return;}
        operation="stream.group.session.join";action=28;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{}}");success_status=201;
    } else if (!strcmp(method,"POST") && group_session_path(a,path,path_len,":heartbeat",&group_session)==1) {
        operation="stream.group.session.heartbeat";action=29;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{}}");
    } else if (!strcmp(method,"POST") && group_session_path(a,path,path_len,":leave",&group_session)==1) {
        operation="stream.group.session.leave";action=30;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"left\":true}}");
    } else if (!strcmp(method,"POST") && group_session_path(a,path,path_len,"/offsets:commit",&group_session)==1 && admin_json_u64(body,content_len,"partition",&n1)==1 && n1<=UINT32_MAX && admin_json_u64(body,content_len,"offset",&n2)==1) {
        const char *if_match=header(headers,header_len,"if-match",&if_match_len);if(generation_etag(if_match,if_match_len,&expected_generation)){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"428 Precondition Required",b,error(&o,"precondition_required","If-Match with the current group ETag is required."),0,cors,0);free(body);return;}
        group_partition=(uint32_t)n1;operation="stream.group.session.offset.commit";action=31;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"partition\":%u,\"offset\":%llu,\"durability\":\"known\"}}",group_partition,(unsigned long long)n2);
    } else if (!strcmp(method,"POST") && (group_session_path(a,path,path_len,":heartbeat",&group_session)==-2 || group_session_path(a,path,path_len,":leave",&group_session)==-2 || group_session_path(a,path,path_len,"/offsets:commit",&group_session)==-2)) {
        char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"410 Gone",b,error(&o,"session_expired","The Consumer Group session has expired."),0,cors,0);free(body);return;
    } else if (!strcmp(method,"PUT") && group_offset_partition_path(path,path_len,&target,&target2,&group_partition)==0 && admin_json_u64(body,content_len,"offset",&n1)==1) {
        const char *if_match=header(headers,header_len,"if-match",&if_match_len);
        if(generation_etag(if_match,if_match_len,&expected_generation)){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"428 Precondition Required",b,error(&o,"precondition_required","If-Match with the current group ETag is required."),0,cors,0);free(body);return;}
        operation="stream.group.offset.commit";action=15;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"partition\":%u,\"offset\":%llu,\"durability\":\"known\"}}",group_partition,(unsigned long long)n1);
    } else if (!strcmp(method,"POST") && path_len==31 && !memcmp(path,"/api/admin/v1/atomic-operations",31) && a->c.atomic_execute) {
        if(admin_json_string(body,content_len,"operation",&atomic_kind)!=1||admin_json_string(body,content_len,"key_id",&atomic_key)!=1||admin_json_string(body,content_len,"target_id",&atomic_target)!=1||decode_id(atomic_key.data,atomic_key.len,&target2)||decode_id(atomic_target.data,atomic_target.len,&target)){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"400 Bad Request",b,error(&o,"validation_failed","operation, key_id, and target_id are required exact identifiers."),0,cors,0);free(body);return;}
        if(atomic_kind.len==13&&!memcmp(atomic_kind.data,"put-and-route",13))atomic_operation=1;else if(atomic_kind.len==15&&!memcmp(atomic_kind.data,"put-and-enqueue",15))atomic_operation=2;else if(atomic_kind.len==16&&!memcmp(atomic_kind.data,"delete-and-route",16))atomic_operation=3;else if(atomic_kind.len==27&&!memcmp(atomic_kind.data,"update-if-present-and-route",27))atomic_operation=4;else {char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"400 Bad Request",b,error(&o,"validation_failed","The atomic operation is unsupported."),0,cors,0);free(body);return;}
        if((payload=malloc(content_len))==NULL||json_bytes(body,content_len,atomic_operation==3?"body":"value",payload,content_len,&payload_len)|| (atomic_operation!=2&&json_bytes(body,content_len,"routing_key",atomic_routing_key,sizeof atomic_routing_key,&atomic_routing_key_len))){free(payload);char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"400 Bad Request",b,error(&o,"validation_failed","The atomic payload or routing key is invalid."),0,cors,0);free(body);return;}
        (void)admin_json_u64(body,content_len,"ttl_ms",&n1);operation=atomic_operation==1?"atomic.put_and_route":atomic_operation==2?"atomic.put_and_enqueue":atomic_operation==3?"atomic.delete_and_route":"atomic.update_if_present_and_route";action=14;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"transaction_id\":0,\"routed_queue_count\":0,\"durability\":\"known\"}}");
    } else if (!strcmp(method,"POST") && path_len==29 && !memcmp(path,"/api/admin/v1/routing/routers",29) && admin_json_string(body,content_len,"name",&name)==1 && name.len && name.len<=EXCHANGE_NAME_MAX && admin_json_string(body,content_len,"mode",&mode)==1) {
        if(mode.len==5&&!memcmp(mode.data,"exact",5))router_mode=EXCHANGE_DIRECT;else if(mode.len==9&&!memcmp(mode.data,"broadcast",9))router_mode=EXCHANGE_FANOUT;else if(mode.len==7&&!memcmp(mode.data,"pattern",7))router_mode=EXCHANGE_TOPIC;else { char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"400 Bad Request",b,error(&o,"validation_failed","The Routing mode is invalid."),0,cors,0);free(body);return; }
        int alternate_present=admin_json_string(body,content_len,"alternate_router_id",&alternate);if(alternate_present<0||(alternate_present==1&&decode_id(alternate.data,alternate.len,&target2))){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"400 Bad Request",b,error(&o,"validation_failed","alternate_router_id is invalid."),0,cors,0);free(body);return;}
        (void)admin_json_bool(body,content_len,"durable",&durable);operation="routing.router.create";action=5;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"name\":\"%.*s\",\"mode\":\"%.*s\"}}",(int)name.len,name.data,(int)mode.len,mode.data);success_status=201;
    } else if (!strcmp(method,"PATCH") && path_id_after(path,path_len,"/api/admin/v1/routing/routers/",&target)==0 && admin_json_string(body,content_len,"alternate_router_id",&alternate)==1 && decode_id(alternate.data,alternate.len,&target2)==0) {
        const char *if_match=header(headers,header_len,"if-match",&if_match_len);if(router_etag(if_match,if_match_len,&expected_revision)){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"428 Precondition Required",b,error(&o,"precondition_required","If-Match with the current router ETag is required."),0,cors,0);free(body);return;}
        operation="routing.router.update";action=45;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"updated\":true,\"durability\":\"known\"}}");
    } else if (!strcmp(method,"POST") && path_len==29 && !memcmp(path,"/api/admin/v1/queue-consumers",29) && admin_json_string(body,content_len,"name",&name)==1 && name.len && name.len<=QUEUE_NAME_MAX) {
        operation="queue.consumer.create";action=13;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"name\":\"%.*s\"}}",(int)name.len,name.data);success_status=201;
    } else if (!strcmp(method,"POST") && path_id_action(path,path_len,"/api/admin/v1/queue-consumers/","/deliveries",&target)==0 && admin_json_string(body,content_len,"queue_id",&queue_id)==1 && decode_id(queue_id.data,queue_id.len,&target2)==0) {
        if (!(delivery=delivery_slot(a))) { a->rate_limit_rejections++;char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"429 Too Many Requests",b,error(&o,"resource_exhausted","The delivery registry is at capacity."),0,cors,0);free(body);return; }
        int visibility_present=admin_json_u64(body,content_len,"visibility_ms",&n1);if(!visibility_present)n1=30000;
        if(visibility_present<0||n1<100||n1>60000){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"400 Bad Request",b,error(&o,"validation_failed","queue_id or visibility_ms is invalid."),0,cors,0);free(body);return;}
        if(queue_consumer_lookup(a->c.queues,(const char *)target.bytes,target.len,&delivery_owner)!=1){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"404 Not Found",b,error(&o,"not_found","The requested Queue consumer was not found."),0,cors,0);free(body);return;}
        uint64_t queue_revision_value=0;if(queue_revision(a->c.queues,(const char *)target2.bytes,target2.len,&queue_revision_value)!=1){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"404 Not Found",b,error(&o,"not_found","The requested Queue was not found."),0,cors,0);free(body);return;}
        if(make_delivery_id(delivery->id)){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"503 Service Unavailable",b,error(&o,"entropy_unavailable","A secure delivery identifier could not be created."),0,cors,0);free(body);return;}
        operation="queue.consumer.delivery.create";action=41;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":null}");success_status=201;
    } else if (!strcmp(method,"DELETE") && path_id_after(path,path_len,"/api/admin/v1/queue-consumers/",&target)==0) {
        const char *if_match=header(headers,header_len,"if-match",&if_match_len),*confirm=header(headers,header_len,"x-kuttidb-confirm",&confirm_len);const char *segment=path+30;uint64_t owner=0;
        if(!confirm||confirm_len!=path_len-30||memcmp(confirm,segment,confirm_len)||consumer_etag(if_match,if_match_len,&expected_revision)){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"428 Precondition Required",b,error(&o,"precondition_required","Current If-Match and exact X-KuttiDB-Confirm are required for Queue consumer deletion."),0,cors,0);free(body);return;}
        if(queue_consumer_lookup(a->c.queues,(const char *)target.bytes,target.len,&owner)!=1){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"404 Not Found",b,error(&o,"not_found","The requested Queue consumer was not found."),0,cors,0);free(body);return;}
        if(owner!=expected_revision){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"412 Precondition Failed",b,error(&o,"precondition_failed","The Queue consumer changed. Refresh it and try again."),0,cors,0);free(body);return;}
        operation="queue.consumer.delete";action=25;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"deleted\":true,\"durability\":\"known\"}}");
    } else if (!strcmp(method,"DELETE") && router_route_path(path,path_len,&target,&route_id)==0) {
        static const char prefix[]="/api/admin/v1/routing/routers/",marker[]="/routes/";const char *segment=path+sizeof prefix-1,*mark=memmem(segment,path_len-(sizeof prefix-1),marker,sizeof marker-1),*route_segment=mark?mark+sizeof marker-1:NULL,*confirm=header(headers,header_len,"x-kuttidb-confirm",&confirm_len),*if_match=header(headers,header_len,"if-match",&if_match_len);const char*query=memchr(path,'?',path_len);size_t end=query?(size_t)(query-path):path_len;
        if(!route_segment||!confirm||confirm_len!=end-(size_t)(route_segment-path)||memcmp(confirm,route_segment,confirm_len)||router_etag(if_match,if_match_len,&expected_revision)){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"428 Precondition Required",b,error(&o,"precondition_required","Current router If-Match and exact route_id confirmation are required for route deletion."),0,cors,0);free(body);return;}
        target2=route_id.queue;name.data=(const char*)route_id.key.bytes;name.len=route_id.key.len;operation="routing.route.delete";action=43;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"deleted\":true,\"durability\":\"known\"}}");
    } else if (!strcmp(method,"DELETE") && path_id_after(path,path_len,"/api/admin/v1/routing/routers/",&target)==0) {
        static const char prefix[]="/api/admin/v1/routing/routers/";const char *segment=path+sizeof prefix-1,*confirm=header(headers,header_len,"x-kuttidb-confirm",&confirm_len),*if_match=header(headers,header_len,"if-match",&if_match_len);
        if(!confirm||confirm_len!=path_len-(size_t)(segment-path)||memcmp(confirm,segment,confirm_len)||router_etag(if_match,if_match_len,&expected_revision)){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"428 Precondition Required",b,error(&o,"precondition_required","Current If-Match and exact X-KuttiDB-Confirm are required for router deletion."),0,cors,0);free(body);return;}
        operation="routing.router.delete";action=44;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"deleted\":true,\"durability\":\"known\"}}");
    } else if (!strcmp(method,"DELETE") && path_id_action(path,path_len,"/api/admin/v1/routing/routers/","/routes",&target)==0 && admin_json_string(body,content_len,"queue_id",&queue_id)==1 && decode_id(queue_id.data,queue_id.len,&target2)==0 && admin_json_string(body,content_len,"routing_key",&name)==1 && name.len<=ROUTING_KEY_MAX) {
        static const char prefix[]="/api/admin/v1/routing/routers/";const char *segment=path+sizeof prefix-1,*query=memchr(path,'?',path_len);size_t end=query?(size_t)(query-path):path_len;const char *suffix=path+end-(sizeof "/routes"-1),*confirm=header(headers,header_len,"x-kuttidb-confirm",&confirm_len),*if_match=header(headers,header_len,"if-match",&if_match_len);
        if(!confirm||confirm_len!=(size_t)(suffix-segment)||memcmp(confirm,segment,confirm_len)||router_etag(if_match,if_match_len,&expected_revision)){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"428 Precondition Required",b,error(&o,"precondition_required","Current If-Match and exact X-KuttiDB-Confirm are required for route deletion."),0,cors,0);free(body);return;}
        operation="routing.route.delete";action=43;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"deleted\":true,\"durability\":\"known\"}}");
    } else if (!strcmp(method,"POST") && path_id_action(path,path_len,"/api/admin/v1/routing/routers/","/routes",&target)==0 && admin_json_string(body,content_len,"queue_id",&queue_id)==1 && decode_id(queue_id.data,queue_id.len,&target2)==0 && admin_json_string(body,content_len,"routing_key",&name)==1 && name.len<=ROUTING_KEY_MAX) {
        operation="routing.route.create";action=6;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"router_id\":\"bound\",\"queue_id\":\"bound\"}}");success_status=201;
    } else if (!strcmp(method,"POST") && path_id_action(path,path_len,"/api/admin/v1/routing/routers/","/messages",&target)==0 && admin_json_string(body,content_len,"routing_key",&name)==1 && name.len<=ROUTING_KEY_MAX && (payload=malloc(content_len)) != NULL && json_bytes(body,content_len,"body",payload,content_len,&payload_len)==0) {
        (void)admin_json_u64(body,content_len,"ttl_ms",&n1);operation="routing.publish";action=7;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"routed_queue_count\":0,\"outcome\":\"unroutable\"}}");success_status=201;
    } else if (!strcmp(method,"POST") && path_id_action(path,path_len,"/api/admin/v1/queues/","/deliveries:batch",&target)==0) {
        int visibility_present=admin_json_u64(body,content_len,"visibility_ms",&n1),count_present=admin_json_u64(body,content_len,"max_messages",&n2);
        if(!visibility_present)n1=30000;if(!count_present)n2=10;
        if(visibility_present<0||count_present<0||n1<100||n1>60000||!n2||n2>50){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"400 Bad Request",b,error(&o,"validation_failed","visibility_ms or max_messages is invalid."),0,cors,0);free(body);return;}
        batch_count=(uint32_t)n2;
        if(delivery_slots(a,delivery_batch,batch_count)!=batch_count){a->rate_limit_rejections++;char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"429 Too Many Requests",b,error(&o,"resource_exhausted","The delivery registry is at capacity."),0,cors,0);free(body);return;}
        for(uint32_t i=0;i<batch_count;i++)if(make_delivery_id(delivery_batch[i]->id)){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"503 Service Unavailable",b,error(&o,"entropy_unavailable","A secure delivery identifier could not be created."),0,cors,0);free(body);return;}
        operation="queue.delivery.batch_create";action=34;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":[]}");success_status=201;
    } else if (!strcmp(method,"POST") &&
               ((path_id_action(path,path_len,"/api/admin/v1/queues/","/deliveries:ack-batch",&target)==0) ||
                ((batch_nack=1) && path_id_action(path,path_len,"/api/admin/v1/queues/","/deliveries:nack-batch",&target)==0)) &&
               admin_json_string_array(body,content_len,"delivery_ids",batch_items,50,&batch_item_count)==1 &&
               batch_item_count && (batch_count=(uint32_t)batch_item_count)) {
        if (batch_nack) { int requeue_present=admin_json_bool(body,content_len,"requeue",&durable); if(requeue_present<0){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"400 Bad Request",b,error(&o,"validation_failed","requeue must be a boolean when present."),0,cors,0);free(body);return;} if(!requeue_present)durable=1; }
        for(uint32_t i=0;i<batch_count;i++) {
            int found=delivery_id_lookup(a,&target,&batch_items[i],&delivery_batch[i]);
            if(found!=1){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,found==-2?"410 Gone":"404 Not Found",b,error(&o,found==-2?"delivery_expired":"not_found",found==-2?"A requested delivery has expired.":"A requested delivery was not found for this Queue."),0,cors,0);free(body);return;}
            delivery_tags[i]=delivery_batch[i]->tag;
            if(i==0)delivery_owner=delivery_batch[i]->owner;
            if(delivery_batch[i]->owner!=delivery_owner){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"409 Conflict",b,error(&o,"owner_mismatch","Batch delivery IDs must belong to the same opaque owner."),0,cors,0);free(body);return;}
            for(uint32_t j=0;j<i;j++)if(delivery_batch[i]==delivery_batch[j]){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"400 Bad Request",b,error(&o,"validation_failed","delivery_ids must not contain duplicates."),0,cors,0);free(body);return;}
        }
        operation=batch_nack?"queue.delivery.nack_batch":"queue.delivery.ack_batch";action=batch_nack?33:32;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":[]}");
    } else if (!strcmp(method,"POST") && path_id_action(path,path_len,"/api/admin/v1/queues/","/deliveries",&target)==0) {
        if (!(delivery=delivery_slot(a))) { a->rate_limit_rejections++;char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"429 Too Many Requests",b,error(&o,"resource_exhausted","The delivery registry is at capacity."),0,cors,0);free(body);return; }
        int visibility_present=admin_json_u64(body,content_len,"visibility_ms",&n1); if(visibility_present==0)n1=30000;
        if(visibility_present<0 || n1<100 || n1>60000){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"400 Bad Request",b,error(&o,"validation_failed","The visibility timeout is invalid."),0,cors,0);free(body);return;}
        if(make_delivery_id(delivery->id)){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"503 Service Unavailable",b,error(&o,"entropy_unavailable","A secure delivery identifier could not be created."),0,cors,0);free(body);return;}
        operation="queue.delivery.create";action=8;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":null}");success_status=201;
    } else if (!strcmp(method,"POST") && path_delivery(a,path,path_len,":ack",&delivery)==1) {
        operation="queue.delivery.ack";action=9;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"acknowledged\":true}}");
    } else if (!strcmp(method,"POST") && path_delivery(a,path,path_len,":nack",&delivery)==1) {
        int requeue_present=admin_json_bool(body,content_len,"requeue",&durable);if(requeue_present==0)durable=1;(void)admin_json_u64(body,content_len,"delay_ms",&n1);if(n1>60000){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"400 Bad Request",b,error(&o,"validation_failed","The delay is invalid."),0,cors,0);free(body);return;}
        operation="queue.delivery.nack";action=10;success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"requeued\":%s}}",durable?"true":"false");
    } else if (!strcmp(method,"POST") && (path_delivery(a,path,path_len,":ack",&delivery)==-2 || path_delivery(a,path,path_len,":nack",&delivery)==-2)) {
        char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"410 Gone",b,error(&o,"delivery_expired","The delivery has expired."),0,cors,0);free(body);return;
    } else { char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"404 Not Found",b,error(&o,"not_found","The requested resource was not found."),0,cors,0);free(payload);free(body);return; }
    if (success_len >= sizeof success) { char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"500 Internal Server Error",b,error(&o,"internal_error","The server could not complete the request."),0,cors,0);free(payload);free(body);return; }
    { AdminHttpStatus health={0};a->c.status(a->c.status_ud,&health);
      if(health.keyspace_persistence_failed||health.queue_persistence_failed||health.stream_persistence_failed){char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"503 Service Unavailable",b,error(&o,"persistence_unavailable","A required persistence engine is unavailable."),0,cors,0);free(session_parts);free(payload);free(body);return;}
    }
    { unsigned retry_after=1;
      if(!mutation_rate_allow(a,content_len,&retry_after)) {
          char b[256], retry[64]; struct out o={b,sizeof b,0,0};
          snprintf(retry,sizeof retry,"Retry-After: %u\\r\\n",retry_after);
          a->rate_limit_rejections++;
          reply_extra(a,fd,ssl,"429 Too Many Requests",b,
                      error(&o,"rate_limited","The Management API mutation rate limit is reached."),
                      0,cors,0,retry);
          free(session_parts); free(payload); free(body); return;
      }
    }
    a->mutation_attempts++;
    if (audit_event(a,operation,"attempt")) { char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"503 Service Unavailable",b,error(&o,"audit_unavailable","The audit trail is unavailable."),0,cors,0);free(payload);free(body);return; }
    if (action == 1) rc = queue_declare(a->c.queues,name.data,(uint32_t)name.len,durable,n1);
    else if (action == 2) rc = stream_declare(a->c.streams,name.data,(uint32_t)name.len,(uint32_t)n1,n2,age);
    else if (action == 3) rc = queue_publish(a->c.queues,(const char *)target.bytes,target.len,payload,(uint32_t)payload_len,n1,&generated_id);
    else if (action == 4) rc = stream_append(a->c.streams,(const char *)target.bytes,target.len,(uint32_t)n1,stream_key,(uint16_t)stream_key_len,payload,(uint32_t)payload_len,&generated_partition,&generated_offset);
    else if (action == 5) rc = exchange_declare(a->c.queues,name.data,(uint32_t)name.len,durable,router_mode,target2.len?(const char *)target2.bytes:NULL,target2.len);
    else if (action == 6) rc = exchange_bind(a->c.queues,(const char *)target.bytes,target.len,(const char *)target2.bytes,target2.len,name.data,(uint32_t)name.len);
    else if (action == 43) rc = exchange_unbind_if_revision(a->c.queues,(const char *)target.bytes,target.len,(const char *)target2.bytes,target2.len,name.data,(uint32_t)name.len,expected_revision);
    else if (action == 44) rc = exchange_delete_if_revision(a->c.queues,(const char *)target.bytes,target.len,expected_revision);
    else if (action == 45) rc = exchange_set_alternate_if_revision(a->c.queues,(const char *)target.bytes,target.len,(const char *)target2.bytes,target2.len,expected_revision);
    else if (action == 7) rc = exchange_publish(a->c.queues,(const char *)target.bytes,target.len,name.data,(uint32_t)name.len,payload,(uint32_t)payload_len,n1,&generated_id);
    else if (action == 8) {
        uint64_t owner = 0x8000000000000001ULL;
        int got = queue_consume_for_owner(a->c.queues,(const char *)target.bytes,target.len,n1,owner,&delivered);
        if (got == 1) {
            memcpy(delivery->queue.bytes,target.bytes,target.len); delivery->queue.len=target.len;delivery->message_id=delivered.id;delivery->tag=delivered.delivery_tag;delivery->owner=owner;delivery->deadline_ms=admin_mono_ms()+n1;delivery->active=1;delivery->expired=0;
            rc=0;
            struct out encoded={success,sizeof success,0,0};put(&encoded,"{\"data\":{\"delivery_id\":\"");put(&encoded,"%s",delivery->id);put(&encoded,"\",\"message_id\":%llu,\"body\":{\"encoding\":\"base64\",\"data\":",(unsigned long long)delivered.id);json_b64(&encoded,delivered.data,delivered.len);put(&encoded," ,\"size\":%u,\"content_type\":\"application/octet-stream\"}}}",delivered.len);success_len=encoded.len;
        } else rc=-1;
    }
    else if (action == 9) { rc=queue_ack_for_owner(a->c.queues,(const char *)delivery->queue.bytes,delivery->queue.len,delivery->tag,delivery->owner);if(rc==1){delivery->active=0;rc=0;} }
    else if (action == 10) { rc=queue_nack_for_owner_delay(a->c.queues,(const char *)delivery->queue.bytes,delivery->queue.len,delivery->tag,delivery->owner,durable,n1);if(rc==1){delivery->active=0;rc=0;} }
    else if (action == 11) rc=a->c.keyspace_put((const char *)target.bytes,target.len,(const char *)payload,(uint32_t)payload_len,n1,0);
    else if (action == 12) { rc=a->c.keyspace_delete((const char *)target.bytes,target.len);if(rc>=0){deleted=rc;rc=0;} }
    else if (action == 13) rc=queue_consumer_register(a->c.queues,name.data,(uint32_t)name.len,&generated_id);
    else if (action == 14) rc=a->c.atomic_execute(atomic_operation,(const char *)target2.bytes,target2.len,(const char *)target.bytes,target.len,(const char *)atomic_routing_key,(uint32_t)atomic_routing_key_len,payload,(uint32_t)payload_len,n1,&transaction_id,&routed_count);
    else if (action == 15) rc=stream_commit_if_generation(a->c.streams,(const char *)target.bytes,target.len,(const char *)target2.bytes,target2.len,group_partition,n1,expected_generation);
    else if (action == 16) rc=queue_purge_if_revision(a->c.queues,(const char *)target.bytes,target.len,expected_revision,&generated_id);
    else if (action == 42) rc=queue_delete_if_revision(a->c.queues,(const char *)target.bytes,target.len,expected_revision,&generated_id);
    else if (action == 17) rc=job_enqueue_stream(a,ADMIN_JOB_STREAM_TRUNCATE,&target,group_partition,n1,expected_revision,&generated_id);
    else if (action == 46) rc=stream_set_retention_if_revision(a->c.streams,(const char *)target.bytes,target.len,n1,age,expected_revision);
    else if (action == 47) rc=job_enqueue_stream(a,ADMIN_JOB_STREAM_DELETE,&target,0,0,expected_revision,&generated_id);
    else if (action == 18) rc=job_cancel(a,n1);
    else if (action == 19) rc=queue_publish_batch(a->c.queues,(const char *)target.bytes,target.len,batch_count,batch_payloads,batch_lens,NULL);
    else if (action == 20) rc=stream_append_batch(a->c.streams,(const char *)target.bytes,target.len,(uint32_t)n1,stream_batch,batch_count,stream_batch_results);
    else if (action == 21) rc=job_enqueue_maintenance(a,ADMIN_JOB_QUEUE_CHECKPOINT,&generated_id);
    else if (action == 22) rc=job_enqueue_maintenance(a,ADMIN_JOB_STREAM_CHECKPOINT,&generated_id);
    else if (action == 23) rc=job_enqueue_maintenance(a,ADMIN_JOB_KEYSPACE_CHECKPOINT,&generated_id);
    else if (action == 24) rc=job_enqueue_maintenance(a,ADMIN_JOB_CHECKPOINT_ALL,&generated_id);
    else if (action == 25) rc=queue_consumer_unregister(a->c.queues,(const char *)target.bytes,target.len);
    else if (action == 26) rc=stream_commit_batch_if_generation(a->c.streams,(const char *)target.bytes,target.len,(const char *)target2.bytes,target2.len,offset_batch,batch_count,expected_generation);
    else if (action == 27) rc=stream_group_reset_offsets_if_generation(a->c.streams,(const char *)target.bytes,target.len,(const char *)target2.bytes,target2.len,expected_generation,reset_strategy,n1,offset_delta,durable,NULL,NULL,0,&group_offset_count);
    else if (action == 28) {
        uint64_t owner=0x4000000000000000ULL | (++a->next_group_session_owner);
        rc=stream_group_join(a->c.streams,(const char *)target.bytes,target.len,(const char *)target2.bytes,target2.len,owner,(uint32_t)n1,&session_parts,&session_assigned,&session_generation);
        if(!rc){memcpy(group_session->stream.bytes,target.bytes,target.len);group_session->stream.len=target.len;memcpy(group_session->group.bytes,target2.bytes,target2.len);group_session->group.len=target2.len;group_session->owner=owner;group_session->lease_ms=(uint32_t)n1;group_session->deadline_ms=admin_mono_ms()+n1;group_session->active=1;group_session->expired=0;}
    }
    else if (action == 29) { rc=stream_group_join(a->c.streams,(const char *)group_session->stream.bytes,group_session->stream.len,(const char *)group_session->group.bytes,group_session->group.len,group_session->owner,group_session->lease_ms,&session_parts,&session_assigned,&session_generation);if(!rc)group_session->deadline_ms=admin_mono_ms()+group_session->lease_ms; }
    else if (action == 30) { rc=stream_group_leave_member(a->c.streams,(const char *)group_session->stream.bytes,group_session->stream.len,(const char *)group_session->group.bytes,group_session->group.len,group_session->owner);if(!rc){group_session->active=0;group_session->expired=1;} }
    else if (action == 31) rc=stream_commit_for_owner_if_generation(a->c.streams,(const char *)group_session->stream.bytes,group_session->stream.len,(const char *)group_session->group.bytes,group_session->group.len,group_partition,n2,group_session->owner,expected_generation);
    else if (action == 32) { uint32_t processed=0;rc=queue_ack_batch(a->c.queues,(const char *)target.bytes,target.len,delivery_owner,0,delivery_tags,batch_count,&processed);if(!rc&&processed!=batch_count)rc=-1; }
    else if (action == 33) { uint32_t processed=0;rc=queue_nack_batch(a->c.queues,(const char *)target.bytes,target.len,delivery_owner,0,delivery_tags,batch_count,durable,&processed);if(!rc&&processed!=batch_count)rc=-1; }
    else if (action == 34) rc=queue_consume_batch(a->c.queues,(const char *)target.bytes,target.len,n1,0x8000000000000001ULL,batch_count,delivered_batch,&delivered_batch_count);
    else if (action == 35) { rc=0;for(uint32_t i=0;i<batch_count;i++){rc=a->c.keyspace_put((const char *)key_batch[i].bytes,key_batch[i].len,(const char *)batch_payloads[i],batch_lens[i],key_ttls[i],0);if(rc)break;batch_applied++;} }
    else if (action == 36) { rc=0;for(uint32_t i=0;i<batch_count;i++){int deleted_now=a->c.keyspace_delete((const char *)key_batch[i].bytes,key_batch[i].len);if(deleted_now<0){rc=-1;break;}batch_applied++;if(deleted_now)batch_deleted++;} }
    else if (action == 37) { rc=a->c.keyspace_claim_acquire((const char *)claim->key.bytes,claim->key.len,claim->owner,n1);if(rc==1){claim->deadline_ms=admin_mono_ms()+n1;claim->active=1;claim->expired=0;rc=0;}else rc=-1; }
    else if (action == 38) { rc=a->c.keyspace_claim_complete((const char *)claim->key.bytes,claim->key.len,claim->owner,(const char *)payload,(uint32_t)payload_len,n1,0);if(rc==1){claim->active=0;claim->expired=1;rc=0;} }
    else if (action == 39) { rc=a->c.keyspace_claim_release((const char *)claim->key.bytes,claim->key.len,claim->owner);if(rc==1){claim->active=0;claim->expired=1;rc=0;} }
    else if (action == 40) { rc=a->c.keyspace_claim_acquire((const char *)claim->key.bytes,claim->key.len,claim->owner,n1);if(rc==1){claim->deadline_ms=admin_mono_ms()+n1;claim->active=1;claim->expired=0;rc=0;}else rc=-1; }
    else if (action == 41) { int got=queue_consume_for_consumer(a->c.queues,(const char *)target2.bytes,target2.len,(const char *)target.bytes,target.len,n1,&delivered);if(got==1){memcpy(delivery->queue.bytes,target2.bytes,target2.len);delivery->queue.len=target2.len;delivery->message_id=delivered.id;delivery->tag=delivered.delivery_tag;delivery->owner=delivery_owner;delivery->deadline_ms=admin_mono_ms()+n1;delivery->active=1;delivery->expired=0;rc=0;struct out encoded={success,sizeof success,0,0};put(&encoded,"{\"data\":{\"delivery_id\":\"");put(&encoded,"%s",delivery->id);put(&encoded,"\",\"message_id\":%llu,\"body\":{\"encoding\":\"base64\",\"data\":",(unsigned long long)delivered.id);json_b64(&encoded,delivered.data,delivered.len);put(&encoded,",\"size\":%u,\"content_type\":\"application/octet-stream\"}}}",delivered.len);success_len=encoded.len;}else rc=-1; }
    if (action == 3 && rc == 0) success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"message_id\":%llu,\"durability\":\"known\"}}",(unsigned long long)generated_id);
    if (action == 4 && rc == 0) success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"partition\":%llu,\"offset\":%llu,\"durability\":\"known\"}}",(unsigned long long)generated_partition,(unsigned long long)generated_offset);
    if (action == 7 && rc == 0) success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"routed_queue_count\":%llu,\"outcome\":\"%s\"}}",(unsigned long long)generated_id,generated_id?"routed":"unroutable");
    if (action == 12 && rc == 0) success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"deleted\":%s,\"durability\":\"known\"}}",deleted?"true":"false");
    if (action == 14 && rc == 0) success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"transaction_id\":%llu,\"routed_queue_count\":%llu,\"durability\":\"known\"}}",(unsigned long long)transaction_id,(unsigned long long)routed_count);
    if (action == 14 && rc == 1) { success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"outcome\":\"unroutable\",\"routed_queue_count\":0,\"durability\":\"known\"}}");rc=0; }
    if (action == 20 && rc == 0) { struct out encoded={success,sizeof success,0,0};put(&encoded,"{\"data\":[");for(uint32_t i=0;i<batch_count;i++){if(i)put(&encoded,",");put(&encoded,"{\"partition\":%llu,\"offset\":%llu}",(unsigned long long)stream_batch_results[i].partition,(unsigned long long)stream_batch_results[i].offset);}put(&encoded,"],\"meta\":{\"count\":%u,\"durability\":\"known\"}}",batch_count);success_len=encoded.len; }
    if(action==15 && rc==1){(void)audit_event(a,operation,"failed");char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"412 Precondition Failed",b,error(&o,"precondition_failed","The Consumer Group generation changed. Refresh it and try again."),0,cors,0);free(payload);free(body);return;}
    if(action==15 && rc==2){(void)audit_event(a,operation,"failed");char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"409 Conflict",b,error(&o,"conflict","The requested offset is outside the retained partition range."),0,cors,0);free(payload);free(body);return;}
    if(action==15 && rc==3){(void)audit_event(a,operation,"failed");char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"404 Not Found",b,error(&o,"not_found","The Stream, Consumer Group, or partition was not found."),0,cors,0);free(payload);free(body);return;}
    if(action==26 && rc==1){(void)audit_event(a,operation,"failed");char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"412 Precondition Failed",b,error(&o,"precondition_failed","The Consumer Group generation changed. Refresh it and try again."),0,cors,0);free(payload);free(body);return;}
    if(action==26 && rc==2){(void)audit_event(a,operation,"failed");char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"409 Conflict",b,error(&o,"conflict","One requested offset is outside its retained partition range."),0,cors,0);free(payload);free(body);return;}
    if(action==26 && rc==3){(void)audit_event(a,operation,"failed");char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"404 Not Found",b,error(&o,"not_found","The Stream, Consumer Group, or partition was not found."),0,cors,0);free(payload);free(body);return;}
    if(action==27 && rc==1){(void)audit_event(a,operation,"failed");char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"412 Precondition Failed",b,error(&o,"precondition_failed","The Consumer Group generation changed. Refresh it and try again."),0,cors,0);free(payload);free(body);return;}
    if(action==27 && rc==2){(void)audit_event(a,operation,"failed");char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"409 Conflict",b,error(&o,"conflict","The requested reset target is outside a retained partition range."),0,cors,0);free(payload);free(body);return;}
    if(action==27 && rc==3){(void)audit_event(a,operation,"failed");char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"404 Not Found",b,error(&o,"not_found","The Stream or Consumer Group was not found."),0,cors,0);free(payload);free(body);return;}
    if(action==27 && rc==4){(void)audit_event(a,operation,"failed");char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"409 Conflict",b,error(&o,"active_group","The Consumer Group has active members; force is required."),0,cors,0);free(payload);free(body);return;}
    if((action==28||action==29||action==30) && rc){(void)audit_event(a,operation,"failed");char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"409 Conflict",b,error(&o,"conflict","The Consumer Group session operation could not be applied."),0,cors,0);free(session_parts);free(payload);free(body);return;}
    if(action==31 && rc==1){(void)audit_event(a,operation,"failed");char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"412 Precondition Failed",b,error(&o,"precondition_failed","The Consumer Group generation changed. Refresh it and try again."),0,cors,0);free(payload);free(body);return;}
    if(action==31 && rc==2){(void)audit_event(a,operation,"failed");char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"409 Conflict",b,error(&o,"conflict","The requested offset is outside the retained partition range."),0,cors,0);free(payload);free(body);return;}
    if(action==31 && rc==3){(void)audit_event(a,operation,"failed");char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"409 Conflict",b,error(&o,"assignment_lost","The session does not own that partition."),0,cors,0);free(payload);free(body);return;}
    if(action==16 && rc==2){(void)audit_event(a,operation,"failed");char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"412 Precondition Failed",b,error(&o,"precondition_failed","The Queue changed. Refresh it and try again."),0,cors,0);free(payload);free(body);return;}
    if(action==16 && rc==0){(void)audit_event(a,operation,"failed");char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"404 Not Found",b,error(&o,"not_found","The requested Queue was not found."),0,cors,0);free(payload);free(body);return;}
    if(action==16 && rc==1){success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"removed_count\":%llu,\"durability\":\"known\"}}",(unsigned long long)generated_id);rc=0;}
    if(action==42 && rc==2){(void)audit_event(a,operation,"failed");char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"412 Precondition Failed",b,error(&o,"precondition_failed","The Queue changed. Refresh it and try again."),0,cors,0);free(payload);free(body);return;}
    if(action==42 && rc==0){(void)audit_event(a,operation,"failed");char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"404 Not Found",b,error(&o,"not_found","The requested Queue was not found."),0,cors,0);free(payload);free(body);return;}
    if(action==42 && rc==3){(void)audit_event(a,operation,"failed");char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"409 Conflict",b,error(&o,"conflict","Remove the Queue's durable routes before deleting it."),0,cors,0);free(payload);free(body);return;}
    if(action==42 && rc==1){for(size_t i=0;i<ADMIN_DELIVERY_LIMIT;i++)if(a->deliveries[i].active&&a->deliveries[i].queue.len==target.len&&!memcmp(a->deliveries[i].queue.bytes,target.bytes,target.len)){a->deliveries[i].active=0;a->deliveries[i].expired=1;}success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"deleted\":true,\"removed_count\":%llu,\"durability\":\"known\"}}",(unsigned long long)generated_id);rc=0;}
    if((action==17||action==21||action==22||action==23||action==24||action==47) && rc){(void)audit_event(a,operation,"failed");a->rate_limit_rejections++;char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"429 Too Many Requests",b,error(&o,"resource_exhausted","The bounded job registry is at capacity."),0,cors,0);free(payload);free(body);return;}
    if(action==17||action==21||action==22||action==23||action==24||action==47){success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"job_id\":\"j-%llu\",\"state\":\"queued\"}}",(unsigned long long)generated_id);}
    if(action==18 && rc==0){(void)audit_event(a,operation,"failed");char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"404 Not Found",b,error(&o,"not_found","The requested job was not found."),0,cors,0);free(payload);free(body);return;}
    if(action==25 && rc==0){(void)audit_event(a,operation,"failed");char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"404 Not Found",b,error(&o,"not_found","The requested Queue consumer was not found."),0,cors,0);free(payload);free(body);return;}
    if(action==25 && rc==1)rc=0;
    if(action==26 && rc==0){struct out encoded={success,sizeof success,0,0};put(&encoded,"{\"data\":[");for(uint32_t i=0;i<batch_count;i++){if(i)put(&encoded,",");put(&encoded,"{\"partition\":%u,\"offset\":%llu}",offset_batch[i].partition,(unsigned long long)offset_batch[i].offset);}put(&encoded,"],\"meta\":{\"count\":%u,\"durability\":\"known\"}}",batch_count);success_len=encoded.len;}
    if(action==27 && rc==0){const char *strategy=reset_strategy==STREAM_OFFSET_RESET_EARLIEST?"earliest":reset_strategy==STREAM_OFFSET_RESET_LATEST?"latest":reset_strategy==STREAM_OFFSET_RESET_ABSOLUTE?"absolute":"relative";success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"strategy\":\"%s\",\"partition_count\":%u,\"durability\":\"known\"}}",strategy,group_offset_count);}
    if((action==32||action==33) && rc==0){struct out encoded={success,sizeof success,0,0};put(&encoded,"{\"data\":[");for(uint32_t i=0;i<batch_count;i++){if(i)put(&encoded,",");put(&encoded,"{\"delivery_id\":\"");put(&encoded,"%s",delivery_batch[i]->id);put(&encoded,"\",\"outcome\":\"%s\"}",action==32?"acknowledged":durable?"requeued":"discarded");delivery_batch[i]->active=0;}put(&encoded,"],\"meta\":{\"count\":%u,\"durability\":\"known\"}}",batch_count);success_len=encoded.len;}
    if(action==34 && rc==0){uint64_t owner=0x8000000000000001ULL;struct out encoded={success,sizeof success,0,0};put(&encoded,"{\"data\":[");for(uint32_t i=0;i<delivered_batch_count;i++){AdminDelivery*d=delivery_batch[i];memcpy(d->queue.bytes,target.bytes,target.len);d->queue.len=target.len;d->message_id=delivered_batch[i].id;d->tag=delivered_batch[i].delivery_tag;d->owner=owner;d->deadline_ms=admin_mono_ms()+n1;d->active=1;d->expired=0;if(i)put(&encoded,",");put(&encoded,"{\"delivery_id\":\"");put(&encoded,"%s",d->id);put(&encoded,"\",\"message_id\":%llu}",(unsigned long long)d->message_id);}put(&encoded,"],\"meta\":{\"count\":%u,\"requested_max\":%u,\"durability\":\"known\"}}",delivered_batch_count,batch_count);success_len=encoded.len;}
    if(action==35 && rc==0)success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"applied_count\":%u,\"atomic\":false,\"durability\":\"known\"}}",batch_applied);
    if(action==36 && rc==0)success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"applied_count\":%u,\"deleted_count\":%u,\"atomic\":false,\"durability\":\"known\"}}",batch_applied,batch_deleted);
    if(action==37 && rc==0){struct out encoded={success,sizeof success,0,0};put(&encoded,"{\"data\":{\"claim_id\":\"");put(&encoded,"%s",claim->id);put(&encoded,"\",");identifier_field(&encoded,"key",&claim->key);put(&encoded,",\"lease_ms\":%llu}}",(unsigned long long)n1);success_len=encoded.len;}
    if(action==38 && rc==0)success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"completed\":true,\"durability\":\"known\"}}");
    if(action==39 && rc==0)success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"released\":true}}");
    if(action==40 && rc==0){struct out encoded={success,sizeof success,0,0};put(&encoded,"{\"data\":{\"outcome\":\"claimed\",\"claim_id\":\"");put(&encoded,"%s",claim->id);put(&encoded,"\",");identifier_field(&encoded,"key",&claim->key);put(&encoded,",\"lease_ms\":%llu}}",(unsigned long long)n1);success_len=encoded.len;}
    if(action==43 && rc==2){(void)audit_event(a,operation,"failed");char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"412 Precondition Failed",b,error(&o,"precondition_failed","The router changed. Refresh it and try again."),0,cors,0);free(payload);free(body);return;}
    if(action==43 && rc==0){(void)audit_event(a,operation,"failed");char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"404 Not Found",b,error(&o,"not_found","The requested route was not found."),0,cors,0);free(payload);free(body);return;}
    if(action==43 && rc==1){success_len=(size_t)snprintf(success,sizeof success,"{\"data\":{\"deleted\":true,\"durability\":\"known\"}}");rc=0;}
    if(action==44 && rc==2){(void)audit_event(a,operation,"failed");char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"412 Precondition Failed",b,error(&o,"precondition_failed","The router changed. Refresh it and try again."),0,cors,0);free(payload);free(body);return;}
    if(action==44 && rc==0){(void)audit_event(a,operation,"failed");char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"404 Not Found",b,error(&o,"not_found","The requested router was not found."),0,cors,0);free(payload);free(body);return;}
    if(action==44 && (rc==3||rc==4)){(void)audit_event(a,operation,"failed");char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"409 Conflict",b,error(&o,"conflict",rc==3?"Remove the router's routes before deleting it.":"Remove alternate-router references before deleting it."),0,cors,0);free(payload);free(body);return;}
    if(action==44 && rc==1)rc=0;
    if(action==45 && rc==2){(void)audit_event(a,operation,"failed");char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"412 Precondition Failed",b,error(&o,"precondition_failed","The router changed. Refresh it and try again."),0,cors,0);free(payload);free(body);return;}
    if(action==45 && rc==0){(void)audit_event(a,operation,"failed");char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"404 Not Found",b,error(&o,"not_found","The requested router was not found."),0,cors,0);free(payload);free(body);return;}
    if(action==45 && rc==1)rc=0;
    if(action==46 && rc==1){(void)audit_event(a,operation,"failed");char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"412 Precondition Failed",b,error(&o,"precondition_failed","The Stream changed. Refresh it and try again."),0,cors,0);free(payload);free(body);return;}
    if(action==46 && rc==3){(void)audit_event(a,operation,"failed");char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"404 Not Found",b,error(&o,"not_found","The requested Stream was not found."),0,cors,0);free(payload);free(body);return;}
    if((action==28||action==29) && rc==0){struct out encoded={success,sizeof success,0,0};put(&encoded,"{\"data\":{\"session_id\":\"");put(&encoded,"%s",group_session->id);put(&encoded,"\",\"generation\":%llu,\"lease_ms\":%u,\"assigned_partitions\":[",(unsigned long long)session_generation,group_session->lease_ms);for(uint32_t i=0;i<session_assigned;i++){if(i)put(&encoded,",");put(&encoded,"%u",session_parts[i]);}put(&encoded,"]}} ");success_len=encoded.len;}
    if(action==18 && rc<0){(void)audit_event(a,operation,"failed");char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"409 Conflict",b,error(&o,"conflict","Only queued jobs can be cancelled."),0,cors,0);free(payload);free(body);return;}
    if(action==18)rc=0;
    if(rc==0 && audit_event(a,operation,"completed")){a->operation_in_doubt++;char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"500 Internal Server Error",b,error(&o,"operation_in_doubt","The operation may have completed. Refresh before retrying."),0,cors,0);free(session_parts);queue_message_free(&delivered);for(uint32_t i=0;i<delivered_batch_count;i++)queue_message_free(&delivered_batch[i]);free(payload);free(body);return;}
    if((action==35||action==36)&&rc&&batch_applied){(void)audit_event(a,operation,"failed");a->operation_in_doubt++;char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"500 Internal Server Error",b,error(&o,"operation_in_doubt","The non-atomic batch may be partially applied. Refresh before retrying."),0,cors,0);free(session_parts);queue_message_free(&delivered);for(uint32_t i=0;i<delivered_batch_count;i++)queue_message_free(&delivered_batch[i]);free(payload);free(body);return;}
    if(rc){(void)audit_event(a,operation,"failed");char b[256];struct out o={b,sizeof b,0,0};reply(a,fd,ssl,"409 Conflict",b,error(&o,"conflict","The requested operation could not be applied."),0,cors,0);free(session_parts);queue_message_free(&delivered);for(uint32_t i=0;i<delivered_batch_count;i++)queue_message_free(&delivered_batch[i]);free(payload);free(body);return;}
    idempotency_store(a,idem,idem_len,fingerprint,success,success_len,success_status);reply(a,fd,ssl,http_status(success_status),success,success_len,0,cors,0);free(session_parts);queue_message_free(&delivered);for(uint32_t i=0;i<delivered_batch_count;i++)queue_message_free(&delivered_batch[i]);free(payload);free(body);
}

/* Kept reachable while the remaining resource handlers are migrated from the
 * original read-only dispatcher. */
static void __attribute__((unused)) admin_http_pending_handlers(void) {
    (void)handle;
    (void)decode_id;
    (void)path_id_after;
    (void)json_bytes;
    (void)mutation_reply;
}

static void *admin_connection_worker(void *ud) {
    AdminConnection *connection=ud;AdminHttp*a=connection->admin;int fd=connection->fd;free(connection);void*ssl=NULL;int transferred=0,handshake_ok=1;
    struct timeval tv={5,0};setsockopt(fd,SOL_SOCKET,SO_RCVTIMEO,&tv,sizeof tv);setsockopt(fd,SOL_SOCKET,SO_SNDTIMEO,&tv,sizeof tv);fcntl(fd,F_SETFD,FD_CLOEXEC);
#ifdef HAVE_OPENSSL
if(a->tls){ssl=SSL_new(a->tls);if(!ssl)handshake_ok=0;else {SSL_set_fd((SSL*)ssl,fd);if(SSL_accept((SSL*)ssl)!=1)handshake_ok=0;}}
#endif
    if(handshake_ok){pthread_mutex_lock(&a->handlers_mu);handle_management(a,fd,ssl);pthread_mutex_lock(&a->tails_mu);if(a->tail_transferred_fd==fd&&a->pending_tail){a->tail_transferred_fd=-1;a->pending_tail->transfer_ready=1;a->pending_tail=NULL;pthread_cond_broadcast(&a->tails_cv);transferred=1;}pthread_mutex_unlock(&a->tails_mu);pthread_mutex_unlock(&a->handlers_mu);}
    if(!transferred){
#ifdef HAVE_OPENSSL
        if(ssl){SSL_shutdown((SSL*)ssl);SSL_free((SSL*)ssl);}
#endif
        close(fd);pthread_mutex_lock(&a->tails_mu);if(a->active_clients)a->active_clients--;pthread_cond_broadcast(&a->tails_cv);pthread_mutex_unlock(&a->tails_mu);
    }
    return NULL;
}
static int admin_start_connection_worker(AdminConnection *connection) {
    pthread_attr_t attr; pthread_t thread;
    if(pthread_attr_init(&attr)!=0)return -1;
    (void)pthread_attr_setstacksize(&attr,ADMIN_THREAD_STACK_BYTES);
    int rc=pthread_create(&thread,&attr,admin_connection_worker,connection);
    pthread_attr_destroy(&attr);
    if(rc)return -1;
    pthread_detach(thread);
    return 0;
}
static void *admin_thread(void *ud){
    AdminHttp*a=ud;
    while(a->running){
        int fd=accept(a->fd,NULL,NULL);
        if(fd<0){if(errno==EINTR)continue;if(errno==EBADF||errno==EINVAL)break;continue;}
        int admitted=0;
        pthread_mutex_lock(&a->tails_mu);
        if(a->active_clients<a->max_clients){a->active_clients++;admitted=1;}
        pthread_mutex_unlock(&a->tails_mu);
        if(!admitted){
            /* Consume the bounded request first so close() sends the 429 rather than a TCP reset. */
            struct timeval tv={1,0};char discard[ADMIN_REQ_MAX];
            setsockopt(fd,SOL_SOCKET,SO_RCVTIMEO,&tv,sizeof tv);
            (void)recv(fd,discard,sizeof discard,0);shutdown(fd,SHUT_RD);
            make_request_id();char b[256];struct out o={b,sizeof b,0,0};
            reply_extra(a,fd,NULL,"429 Too Many Requests",b,error(&o,"resource_exhausted","The accepted Management API connection limit is reached."),0,NULL,0,"Retry-After: 1\r\n");
            close(fd);continue;
        }
        AdminConnection*connection=malloc(sizeof *connection);
        if(!connection){close(fd);pthread_mutex_lock(&a->tails_mu);a->active_clients--;pthread_mutex_unlock(&a->tails_mu);continue;}
        connection->admin=a;connection->fd=fd;
        if(admin_start_connection_worker(connection)!=0){free(connection);close(fd);pthread_mutex_lock(&a->tails_mu);a->active_clients--;pthread_mutex_unlock(&a->tails_mu);continue;}
    }
    return NULL;
}
#ifdef HAVE_OPENSSL
static int tls_file_ok(const char *p,int key){struct stat st;if(stat(p,&st)<0||!S_ISREG(st.st_mode)||st.st_uid!=geteuid()||(key&&(st.st_mode&0077))){fprintf(stderr,"admin TLS %s must be a server-owned regular file%s\n",key?"key":"certificate",key?" with mode 0600":"");return -1;}return 0;}
static SSL_CTX *make_tls(const char*cert,const char*key){if(tls_file_ok(cert,0)<0||tls_file_ok(key,1)<0)return NULL;OPENSSL_init_ssl(OPENSSL_INIT_LOAD_SSL_STRINGS,NULL);SSL_CTX*x=SSL_CTX_new(TLS_server_method());if(!x)return NULL;SSL_CTX_set_min_proto_version(x,TLS1_2_VERSION);SSL_CTX_set_options(x,SSL_OP_NO_COMPRESSION|SSL_OP_NO_RENEGOTIATION);if(SSL_CTX_use_certificate_chain_file(x,cert)!=1||SSL_CTX_use_PrivateKey_file(x,key,SSL_FILETYPE_PEM)!=1||SSL_CTX_check_private_key(x)!=1){ERR_print_errors_fp(stderr);SSL_CTX_free(x);return NULL;}return x;}
#endif
AdminHttp *admin_http_create(const AdminHttpConfig *c){if(!c||!c->bind||!c->token||!c->token_len||!c->status||!c->audit_log){return NULL;}char copy[128];size_t n=strlen(c->bind);if(!n||n>=sizeof copy){fprintf(stderr,"invalid --admin-bind\n");return NULL;}memcpy(copy,c->bind,n+1);char*colon=strrchr(copy,':');if(!colon||colon==copy){fprintf(stderr,"--admin-bind must be IPv4:PORT\n");return NULL;}*colon=0;char*end;unsigned long port=strtoul(colon+1,&end,10);struct in_addr addr;if(*end||!port||port>65535||inet_pton(AF_INET,copy,&addr)!=1){fprintf(stderr,"invalid --admin-bind\n");return NULL;}if((c->tls_cert==NULL)!=(c->tls_key==NULL)){fprintf(stderr,"--admin-tls-cert and --admin-tls-key must be provided together\n");return NULL;}if(!loopback(copy)&&!c->tls_cert){fprintf(stderr,"refusing non-loopback --admin-bind without native TLS\n");return NULL;}
#ifndef HAVE_OPENSSL
if(c->tls_cert){fprintf(stderr,"admin TLS support is unavailable; rebuild with OpenSSL development files\n");return NULL;}
#endif
AdminHttp*a=calloc(1,sizeof *a);if(!a)return NULL;pthread_mutex_init(&a->jobs_mu,NULL);pthread_cond_init(&a->jobs_cv,NULL);pthread_mutex_init(&a->audit_mu,NULL);pthread_mutex_init(&a->tails_mu,NULL);pthread_cond_init(&a->tails_cv,NULL);a->tail_transferred_fd=-1;a->c=*c;a->audit_fd=open_audit_log(c->audit_log);if(a->audit_fd<0){pthread_cond_destroy(&a->tails_cv);pthread_mutex_destroy(&a->tails_mu);pthread_mutex_destroy(&a->audit_mu);pthread_cond_destroy(&a->jobs_cv);pthread_mutex_destroy(&a->jobs_mu);free(a);return NULL;}a->max_clients=c->max_clients?a->c.max_clients:16;a->max_tail_clients=c->max_tail_clients?a->c.max_tail_clients:4;a->session_limit=c->session_limit?a->c.session_limit:256;a->job_limit=c->job_limit&&c->job_limit<ADMIN_JOB_LIMIT?c->job_limit:ADMIN_JOB_LIMIT;a->fd=socket(AF_INET,SOCK_STREAM,0);if(a->fd<0){close(a->audit_fd);pthread_cond_destroy(&a->tails_cv);pthread_mutex_destroy(&a->tails_mu);pthread_mutex_destroy(&a->audit_mu);pthread_cond_destroy(&a->jobs_cv);pthread_mutex_destroy(&a->jobs_mu);free(a);return NULL;}int one=1;setsockopt(a->fd,SOL_SOCKET,SO_REUSEADDR,&one,sizeof one);struct sockaddr_in sa={0};sa.sin_family=AF_INET;sa.sin_addr=addr;sa.sin_port=htons((uint16_t)port);if(bind(a->fd,(struct sockaddr*)&sa,sizeof sa)<0||listen(a->fd,(int)a->max_clients)<0){perror("admin bind");close(a->fd);close(a->audit_fd);pthread_cond_destroy(&a->tails_cv);pthread_mutex_destroy(&a->tails_mu);pthread_mutex_destroy(&a->audit_mu);pthread_cond_destroy(&a->jobs_cv);pthread_mutex_destroy(&a->jobs_mu);free(a);return NULL;}fcntl(a->fd,F_SETFD,FD_CLOEXEC);
#ifdef HAVE_OPENSSL
if(c->tls_cert){a->tls=make_tls(c->tls_cert,c->tls_key);if(!a->tls){close(a->fd);close(a->audit_fd);pthread_cond_destroy(&a->tails_cv);pthread_mutex_destroy(&a->tails_mu);pthread_mutex_destroy(&a->audit_mu);pthread_cond_destroy(&a->jobs_cv);pthread_mutex_destroy(&a->jobs_mu);free(a);return NULL;}}
#endif
pthread_mutex_init(&a->handlers_mu,NULL);
return a;}
int admin_http_start(AdminHttp*a){
    if(!a)return -1;
    a->jobs_running=1;
    if(pthread_create(&a->job_thread,NULL,admin_job_worker,a)!=0){a->jobs_running=0;return -1;}
    a->jobs_started=1;
    a->running=1;
    pthread_attr_t attr;
    if(pthread_attr_init(&attr)!=0){
        a->running=0;pthread_mutex_lock(&a->jobs_mu);a->jobs_running=0;
        pthread_cond_broadcast(&a->jobs_cv);pthread_mutex_unlock(&a->jobs_mu);
        pthread_join(a->job_thread,NULL);a->jobs_started=0;return -1;
    }
    (void)pthread_attr_setstacksize(&attr, ADMIN_THREAD_STACK_BYTES);
    if(pthread_create(&a->thread,&attr,admin_thread,a)!=0){
        pthread_attr_destroy(&attr);a->running=0;pthread_mutex_lock(&a->jobs_mu);a->jobs_running=0;
        pthread_cond_broadcast(&a->jobs_cv);pthread_mutex_unlock(&a->jobs_mu);pthread_join(a->job_thread,NULL);a->jobs_started=0;return -1;
    }
    pthread_attr_destroy(&attr);
    return 0;
}
void admin_http_stop(AdminHttp*a){if(!a)return;if(a->running){a->running=0;shutdown(a->fd,SHUT_RDWR);close(a->fd);pthread_join(a->thread,NULL);a->fd=-1;}pthread_mutex_lock(&a->tails_mu);while(a->active_tails)pthread_cond_wait(&a->tails_cv,&a->tails_mu);pthread_mutex_unlock(&a->tails_mu);if(a->jobs_started){pthread_mutex_lock(&a->jobs_mu);a->jobs_running=0;for(size_t i=0;i<a->job_limit;i++)if(a->jobs[i].state==ADMIN_JOB_QUEUED){a->jobs[i].state=ADMIN_JOB_CANCELLED;a->jobs[i].completed_at=(uint64_t)time(NULL);}pthread_cond_broadcast(&a->jobs_cv);pthread_mutex_unlock(&a->jobs_mu);pthread_join(a->job_thread,NULL);a->jobs_started=0;}}
void admin_http_destroy(AdminHttp*a){if(!a)return;admin_http_stop(a);release_deliveries(a);release_claims(a);release_group_sessions(a);if(a->audit_fd>=0)close(a->audit_fd);pthread_cond_destroy(&a->tails_cv);pthread_mutex_destroy(&a->tails_mu);pthread_mutex_destroy(&a->audit_mu);pthread_cond_destroy(&a->jobs_cv);pthread_mutex_destroy(&a->jobs_mu);
pthread_mutex_destroy(&a->handlers_mu);
#ifdef HAVE_OPENSSL
SSL_CTX_free(a->tls);
#endif
free(a);}
