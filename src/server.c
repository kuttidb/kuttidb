#define _GNU_SOURCE
#include "kuttidb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <sys/un.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <stdatomic.h>
#include <limits.h>
#include <stdarg.h>
#include <strings.h>
#ifdef HAVE_OPENSSL
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#endif
#include "embed.h"
#include "platform.h"
#include "queue.h"
#include "stream.h"
#include "admin_http.h"
#include "instance_lock.h"
#include "managed_lifecycle.h"
#include "managed_launcher.h"

#define DEFAULT_PORT 7379
#define NSHARDS 256
#define MAX_KEY UINT16_MAX
#define ABS_MAX_VAL (1u << 30)
#define DEFAULT_MAX_VAL (64u << 20)
#define MAX_BATCH 65536u
#define DEFAULT_MAX_BATCH_BYTES (64ull << 20)
#define DEFAULT_MAX_CLIENTS 1024u
#define AUTH_MAX 1024u
#define GUARD_TIMEOUT_SECONDS 10
#define WAL_FLUSH_LIMIT (1 << 20)
#define OUT_FLUSH_LIMIT (1 << 20)
#define SNAPSHOT_WAL_BYTES (64ull << 20)
#define USER_IDENT 1
#define QUEUE_DECLARE 0x20
#define QUEUE_PUBLISH 0x21
#define QUEUE_CONSUME 0x22
#define QUEUE_ACK 0x23
#define QUEUE_NACK 0x24
#define QUEUE_PUBLISH_TTL 0x25
#define QUEUE_STATS 0x26
#define QUEUE_PREFETCH 0x27
#define QUEUE_CANCEL 0x28
#define QUEUE_CONSUMER_REGISTER 0x29
#define QUEUE_CONSUMER_UNREGISTER 0x2A
#define QUEUE_CONSUME_AS 0x2B
#define QUEUE_LIST 0x2C
#define QUEUE_PUBLISH_BATCH 0x2D
#define QUEUE_CONSUME_BATCH 0x2E
#define QUEUE_ACK_BATCH 0x2F
#define QUEUE_OP_MAX QUEUE_ACK_BATCH
#define EXCHANGE_DECLARE 0x30
#define EXCHANGE_BIND 0x31
#define EXCHANGE_UNBIND 0x32
#define EXCHANGE_PUBLISH 0x33
#define ATOMIC_PUT_PUBLISH 0x40
#define ATOMIC_PUT_ENQUEUE 0x41
#define ATOMIC_DELETE_PUBLISH 0x42
#define ATOMIC_UPDATE_EMIT 0x43
#define SF_GET_OR_CLAIM 0x50
#define SF_WAIT_FOR_KEY 0x51
#define SF_PUT_AND_RELEASE 0x52
#define SF_RELEASE_CLAIM 0x53
#define SF_GET_OR_REFRESH 0x54
#define SF_OP_MAX SF_GET_OR_REFRESH
#define STREAM_DECLARE 0x60
#define STREAM_APPEND 0x61
#define STREAM_FETCH 0x62
#define STREAM_COMMIT 0x63
#define STREAM_GROUP_OFFSET 0x64
#define STREAM_GROUP_JOIN 0x65
#define STREAM_GROUP_LAG 0x66
#define STREAM_APPEND_BATCH 0x67
#define STREAM_GROUP_LEAVE 0x68
#define STREAM_LIST 0x69
#define STREAM_GROUP_LIST 0x6a
#define STREAM_COMMIT_BATCH 0x6b
#define STREAM_FETCH_KEYS 0x6c
#define STREAM_OP_MAX STREAM_FETCH_KEYS
#define HEALTH 0x09
#define CAPABILITIES 0x0a
#define PUT_SWR 0x0b
#define PROTOCOL_MAJOR 1u
#define PROTOCOL_MINOR 7u
#define CAP_CACHE        (1ull << 0)
#define CAP_QUEUES       (1ull << 1)
#define CAP_EXCHANGES    (1ull << 2)
#define CAP_ATOMIC       (1ull << 3)
#define CAP_SINGLEFLIGHT (1ull << 4)
#define CAP_STREAMS      (1ull << 5)
#define CAP_STREAM_BATCH (1ull << 6)
#define CAP_HEALTH       (1ull << 7)
#define CAP_STREAM_GEN   (1ull << 8)
#define CAP_QUEUE_CONSUMERS (1ull << 9)
#define CAP_ATOMIC_UPDATE (1ull << 10)
#define CAP_SWR (1ull << 11)
#define CAP_QUEUE_BATCHES (1ull << 12)
#define CAP_STREAM_COMMIT_BATCH (1ull << 13)
#define CAP_STREAM_KEYS (1ull << 14)
#define CAP_SERVER_INFO (1ull << 15)
#define SERVER_INFO 0x0c
/* Protocol batch-operation entry cap shared by 0x2d/0x2e/0x2f/0x6b. */
#define PROTO_BATCH_MAX 256u

/* wire protocol (little-endian):
 * single op:  [op:1][klen:2][vlen:4][key][value]
 *   0x01=PUT 0x02=GET 0x03=DELETE 0x04=STATS
 *   response: [status:1][vlen:4][value]  status 0=OK/HIT 1=MISS 2=ERROR
 * put w/ttl:  [0x05][klen:2][vlen:4][ttl_ms:4][key][value]
 * put batch:  [0x11][pad:2][count:4] ( count*[klen:2][vlen:4][key][value] )
 *   response: [status:1]
 * get batch:  [0x12][pad:2][count:4] ( count*[klen:2][key] )
 *   response: [count:4] ( count*[status:1][vlen:4][value] )  (streamed)
 * ttl batch:  [0x13][pad:2][count:4] ( count*[klen:2][vlen:4][ttl_ms:4][key][value] )
 *   response: [status:1]
 * queue:      [0x20][qname][durable:1,max_depth:8]
 *             [0x21][qname][message] -> [OK][id:8]
 *             [0x22][qname][visibility_ms:8] -> [status][id:8][redelivered:1][message]
 *             [0x23][qname][id:8], [0x24][qname][id:8,requeue:1]
 * exchange:   [0x30][xname][durable:1,type:1,ext_len:2,ext]  ext=[ae_len:2][ae]
 *             [0x31][xname][queue_len:2,queue,key_len:2,key] (0x32 = unbind)
 *             [0x33][xname][key_len:2,ttl_ms:8,key,message]
 *               -> [OK][len=4][routed:4] / 0x01 unroutable / 0x02 error
 *               (xname empty = default exchange: routing key names the queue)
 * atomic:     [0x40][kuttidb_key][xlen:2,exchange,rklen:2,rkey,ttl_ms:4,value]
 *             [0x41][kuttidb_key][qlen:2,queue,ttl_ms:4,value]
 *             [0x42][kuttidb_key][xlen:2,exchange,rklen:2,rkey,mlen:4,message]
 *             [0x43][kuttidb_key][xlen:2,exchange,rklen:2,rkey,ttl_ms:4,value]
 *               (0x43 UPDATE_AND_EMIT is conditional: the key must already
 *               exist or the whole operation answers MISS and commits
 *               nothing)
 *               -> [OK][len=12][tx_id:8][routed:4]; one commit marker in the
 *               cache WAL (op 0x08) sits between queue WAL TX records, so a
 *               crash leaves both sides or neither
 * inspection: [0x2c] -> [OK][n:2] ( n * [nlen:2,name,depth:8,inflight:8] )
 *             [0x69] -> [OK][n:2] ( n * [tlen:2,topic,parts:4,records:8,bytes:8] )
 *             [0x6a] -> [OK][n:2] ( n * [tlen:2,topic,glen:2,group,gen:8,members:4] )
 *               bounded at 256 entries per response; read-only snapshots
 * singleflight: [0x50][key][lease_ms:4]   -> [OK][len][state:1,value]
 *               [0x51][key][timeout_ms:4] -> deferred [OK][len][state:1,value]
 *               [0x52][key][ttl_ms:4,neg:1,value] (put + release + wake)
 *               [0x53][key]                        (release + wake)
 *               states: 0 hit/value 1 claimed 2 wait 3 negative 4 released
 *                       5 timeout 6 lost 7 stale (SWR value) 8 fresh value
 *                       with refresh due; WAIT responses are deferred, so
 *                       pipelined clients must not rely on response order
 *               [0x54][key][lease_ms:4]   -> stale-while-revalidate read:
 *                       fresh hit answers 0 (or 8 when refresh-ahead is due),
 *                       an expired key with a retained stale copy answers 7
 *                       immediately while one revalidator claims the lease,
 *                       otherwise the 0x50 claim/wait machinery applies.
 * swr put:    [0x0b][klen:2][vlen:4][ttl_ms:4,stale_ms:4,refresh_ms:4,key,value]
 *               value+TTL behave exactly like 0x05; the stale window and
 *               refresh-ahead metadata live in a bounded in-memory registry
 *               (lost on restart, like claims) and never affect plain GET.
 *
 * WAL record: [op:1][klen:2][vlen:4][crc32:4][key][value]
 *             0x07 (PUT_EXP) carries [absolute_exp:4] before key;
 *             legacy 0x05 relative-TTL records remain replay-compatible.
 * snapshot:   "CSN2" ( records*[klen:2][vlen:4][exp:4][key][value] )
 *             legacy headerless snapshots = pre-TTL format, exp=0
 *
 * architecture: N native event loops (one thread each), round-robin accept.
 * no thread per connection; the io path takes no global locks except the
 * WAL ordering mutex and per-shard locks. Batch reads copy directly into the
 * response buffer without an intermediate allocation. Snapshot disk writes
 * happen after releasing each shard lock.
 */

static KuttiDB *g_cache;
static QueueStore *g_queues;
static StreamStore *g_streams;
static volatile sig_atomic_t g_stop = 0;
static char g_wal_path[512];
static int g_wal_fd = -1;
static int g_persist_enabled = 0;
static long g_fsync_ms = 100;
static unsigned long long g_last_snapshot_off = 0;
static _Atomic unsigned long long g_wal_offset = 0;
static int g_wal_shared = 0; /* embed clients may append: serialize via flock */
static pthread_mutex_t g_wal_mu = PTHREAD_MUTEX_INITIALIZER;
typedef enum { DUR_PERIODIC = 0, DUR_ALWAYS = 1 } Durability;
static Durability g_durability = DUR_PERIODIC;
static _Atomic int g_wal_failed = 0;
static pthread_t g_maintenance_thread;
static int g_maintenance_started = 0;
static uint32_t g_max_val = DEFAULT_MAX_VAL;
static unsigned long long g_max_batch_bytes = DEFAULT_MAX_BATCH_BYTES;
static unsigned int g_max_clients = DEFAULT_MAX_CLIENTS;
static ManagedLifecycle g_lifecycle;
static ManagedLifecycleMode g_lifecycle_mode = MANAGED_STANDALONE;
static unsigned long long g_idle_timeout_ms = 60000;
static unsigned long long g_orphan_timeout_ms = 60000;
static char g_instance_id[33];
static int g_has_instance_id = 0;
static InstanceLocks g_instance_locks;
static int g_requested_loops = 0;
static _Atomic unsigned int g_connections = 0;
static _Atomic uint64_t g_next_connection_id = 1;
static _Atomic unsigned long long g_rejected_connections = 0;
static _Atomic unsigned long long g_auth_failures = 0;
static unsigned char g_auth_token[AUTH_MAX];
static size_t g_auth_len = 0;
/* Prometheus scrape endpoint (disabled unless --metrics-bind is given). */
static char g_metrics_secret[AUTH_MAX];
static size_t g_metrics_secret_len = 0;
static int g_metrics_fd = -1;
static unsigned char g_admin_token[AUTH_MAX];
static size_t g_admin_token_len = 0;
static _Atomic unsigned long long g_admin_auth_failures = 0;
static AdminHttp *g_admin_http = NULL;
static time_t g_started_at = 0;
#ifdef HAVE_OPENSSL
static SSL_CTX *g_tls_ctx = NULL;
#endif

static void on_sigint(int sig) { (void)sig; g_stop = 1; }

static int open_private(const char *path, int flags) {
    int fd = open(path, flags | O_NOFOLLOW, 0600);
    if (fd >= 0) {
        fcntl(fd, F_SETFD, FD_CLOEXEC);
        if (fchmod(fd, 0600) < 0) {
            close(fd);
            return -1;
        }
    }
    return fd;
}

static void fsync_parent_dir(const char *path) {
    char copy[600];
    size_t n = strlen(path);
    if (n >= sizeof copy) return;
    memcpy(copy, path, n + 1);
    char *slash = strrchr(copy, '/');
    const char *dir = ".";
    if (slash) {
        if (slash == copy) slash[1] = 0;
        else *slash = 0;
        dir = copy;
    }
    int fd = open(dir, O_RDONLY | O_DIRECTORY);
    if (fd >= 0) { fsync(fd); close(fd); }
}

static void wipe_secret(void *p, size_t n) {
    volatile unsigned char *v = p;
    while (n--) *v++ = 0;
}

/* Runtime depends only on the configured secret length, not matching bytes. */
static int secret_equal(const unsigned char *candidate, size_t len,
                        const unsigned char *secret, size_t secret_len) {
    unsigned int diff = (unsigned int)(len ^ secret_len);
    for (size_t i = 0; i < secret_len; i++) {
        unsigned char b = i < len ? candidate[i] : 0;
        diff |= (unsigned int)(b ^ secret[i]);
    }
    return diff == 0;
}

static int auth_equal(const unsigned char *candidate, size_t len) {
    return secret_equal(candidate, len, g_auth_token, g_auth_len);
}

static int load_auth_file(const char *path) {
    int fd = open(path, O_RDONLY | O_NOFOLLOW);
    if (fd < 0) { perror("auth file"); return -1; }
    struct stat st;
    if (fstat(fd, &st) < 0 || !S_ISREG(st.st_mode) || st.st_uid != geteuid()) {
        fprintf(stderr, "auth file must be a regular file owned by the server user\n");
        close(fd);
        return -1;
    }
    if (st.st_mode & 0077) {
        fprintf(stderr, "auth file permissions must be 0600 (no group/other access)\n");
        close(fd);
        return -1;
    }
    unsigned char raw[AUTH_MAX + 3]; /* token plus optional CRLF and overflow byte */
    size_t used = 0;
    ssize_t r = 0;
    while (used < sizeof raw) {
        r = read(fd, raw + used, sizeof raw - used);
        if (r < 0 && errno == EINTR) continue;
        if (r <= 0) break;
        used += (size_t)r;
    }
    close(fd);
    if (r < 0) { perror("auth file read"); return -1; }
    int overflow = used == sizeof raw;
    while (used > 0 && (raw[used - 1] == '\n' || raw[used - 1] == '\r')) used--;
    if (overflow || used == 0 || used > AUTH_MAX) {
        fprintf(stderr, "auth token must contain 1..%u bytes\n", AUTH_MAX);
        wipe_secret(g_auth_token, sizeof g_auth_token);
        wipe_secret(raw, sizeof raw);
        return -1;
    }
    memcpy(g_auth_token, raw, used);
    g_auth_len = used;
    wipe_secret(raw, sizeof raw);
    return 0;
}

#ifdef HAVE_OPENSSL
static int validate_tls_path(const char *path, int private_key) {
    struct stat st;
    if (lstat(path, &st) < 0 || !S_ISREG(st.st_mode)) {
        fprintf(stderr, "TLS %s must be a regular, non-symlink file: %s\n",
                private_key ? "key" : "certificate", path);
        return -1;
    }
    if (private_key && (st.st_uid != geteuid() || (st.st_mode & 0077))) {
        fprintf(stderr, "TLS private key must be server-owned with mode 0600\n");
        return -1;
    }
    return 0;
}

static int no_key_password(char *buf, int size, int rwflag, void *userdata) {
    (void)buf; (void)size; (void)rwflag; (void)userdata;
    return 0; /* never prompt or read a secret from the terminal */
}

static EVP_PKEY *load_tls_private_key(const char *path) {
    int fd = open(path, O_RDONLY | O_NOFOLLOW);
    if (fd < 0) { perror("TLS private key"); return NULL; }
    struct stat st;
    if (fstat(fd, &st) < 0 || !S_ISREG(st.st_mode) || st.st_uid != geteuid() ||
        (st.st_mode & 0077)) {
        fprintf(stderr, "TLS private key must be server-owned with mode 0600\n");
        close(fd);
        return NULL;
    }
    BIO *bio = BIO_new_fd(fd, BIO_CLOSE);
    if (!bio) { close(fd); return NULL; }
    EVP_PKEY *key = PEM_read_bio_PrivateKey(bio, NULL, no_key_password, NULL);
    BIO_free(bio);
    return key;
}
#endif

static int tls_init(const char *cert_path, const char *key_path) {
#ifdef HAVE_OPENSSL
    if (validate_tls_path(cert_path, 0) < 0)
        return -1;
    OPENSSL_init_ssl(OPENSSL_INIT_LOAD_SSL_STRINGS, NULL);
    g_tls_ctx = SSL_CTX_new(TLS_server_method());
    if (!g_tls_ctx) goto fail;
    if (SSL_CTX_set_min_proto_version(g_tls_ctx, TLS1_2_VERSION) != 1) goto fail;
    SSL_CTX_set_options(g_tls_ctx, SSL_OP_NO_COMPRESSION | SSL_OP_NO_RENEGOTIATION);
    SSL_CTX_set_mode(g_tls_ctx, SSL_MODE_ENABLE_PARTIAL_WRITE |
                                SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);
    if (SSL_CTX_set_cipher_list(g_tls_ctx,
            "ECDHE+AESGCM:ECDHE+CHACHA20") != 1) goto fail;
#ifdef TLS1_3_VERSION
    if (SSL_CTX_set_ciphersuites(g_tls_ctx,
            "TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256:"
            "TLS_AES_128_GCM_SHA256") != 1) goto fail;
#endif
    if (SSL_CTX_use_certificate_chain_file(g_tls_ctx, cert_path) != 1) goto fail;
    EVP_PKEY *private_key = load_tls_private_key(key_path);
    if (!private_key) goto fail;
    int key_ok = SSL_CTX_use_PrivateKey(g_tls_ctx, private_key);
    EVP_PKEY_free(private_key);
    if (key_ok != 1 || SSL_CTX_check_private_key(g_tls_ctx) != 1) goto fail;
    static const unsigned char sid[] = "KuttiDB-TLS-v1";
    if (SSL_CTX_set_session_id_context(g_tls_ctx, sid, sizeof sid - 1) != 1) goto fail;
    return 0;
fail:
    ERR_print_errors_fp(stderr);
    SSL_CTX_free(g_tls_ctx);
    g_tls_ctx = NULL;
    return -1;
#else
    (void)cert_path; (void)key_path;
    fprintf(stderr, "TLS support is unavailable; rebuild with OpenSSL development files\n");
    return -1;
#endif
}

/* ---------------- crc32 ---------------- */

static uint32_t crc_table[256];

static void crc_init(void) {
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i;
        for (int j = 0; j < 8; j++)
            c = (c & 1) ? 0xEDB88320u ^ (c >> 1) : c >> 1;
        crc_table[i] = c;
    }
}

static uint32_t crc32_chain(const void *a, size_t alen, const void *b, size_t blen,
                            const void *d, size_t dlen) {
    uint32_t c = 0xFFFFFFFFu;
    const unsigned char *p = a;
    size_t n = alen;
    while (n--) c = crc_table[(c ^ *p++) & 0xFF] ^ (c >> 8);
    p = b; n = blen;
    while (n--) c = crc_table[(c ^ *p++) & 0xFF] ^ (c >> 8);
    p = d; n = dlen;
    while (n--) c = crc_table[(c ^ *p++) & 0xFF] ^ (c >> 8);
    return c ^ 0xFFFFFFFFu;
}

static uint32_t crc32_two(const void *a, size_t alen, const void *b, size_t blen) {
    uint32_t c = 0xFFFFFFFFu;
    const unsigned char *p = a;
    size_t n = alen;
    while (n--) c = crc_table[(c ^ *p++) & 0xFF] ^ (c >> 8);
    p = b;
    n = blen;
    while (n--) c = crc_table[(c ^ *p++) & 0xFF] ^ (c >> 8);
    return c ^ 0xFFFFFFFFu;
}

static void put_u32le(unsigned char *p, uint32_t v) {
    p[0] = v & 0xff; p[1] = (v >> 8) & 0xff;
    p[2] = (v >> 16) & 0xff; p[3] = (v >> 24) & 0xff;
}

static void put_u16le(unsigned char *p, uint16_t v) {
    p[0] = v & 0xff; p[1] = (v >> 8) & 0xff;
}

static uint32_t get_u32le(const unsigned char *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) |
           ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

static uint32_t get_u16le(const unsigned char *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8);
}

static void put_u64le(unsigned char *p, uint64_t v) {
    for (int i = 0; i < 8; i++) p[i] = (unsigned char)(v >> (i * 8));
}

static uint64_t get_u64le(const unsigned char *p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) v |= (uint64_t)p[i] << (i * 8);
    return v;
}

/* ---------------- WAL (mutex-guarded, shared by all loops) ---------------- */

static KuttiVec g_wal_buf;

static int wal_flush_bytes_locked(void) {
    if (!g_wal_buf.len) return 0;
    size_t off = 0;
    while (off < g_wal_buf.len) {
        ssize_t r = write(g_wal_fd, g_wal_buf.data + off, g_wal_buf.len - off);
        if (r < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        off += (size_t)r;
        g_wal_offset += (unsigned long long)r;
    }
    g_wal_buf.len = 0;
    if (g_wal_buf.cap > (4u << 20)) {
        char *small = realloc(g_wal_buf.data, 1u << 20);
        if (small) { g_wal_buf.data = small; g_wal_buf.cap = 1u << 20; }
    }
    return 0;
}

static int wal_flush_locked(void) {
    int file_locked = 0;
    if (g_wal_shared) {
        if (flock(g_wal_fd, LOCK_EX) < 0) return -1;
        file_locked = 1;
    }
    int rc = wal_flush_bytes_locked();
    if (file_locked) flock(g_wal_fd, LOCK_UN);
    return rc;
}

static int wal_log_locked(unsigned char op, const char *key, uint32_t klen,
                          const char *value, uint32_t vlen, uint32_t meta) {
    if (g_wal_fd < 0) return 0;
    if ((klen && !key) || (vlen && !value)) return -1;
    int extra = (op == 0x05 || op == 0x07) ? 4 : 0;
    int hlen = 11 + extra;
    if (kuttidb_vec_reserve(&g_wal_buf, (size_t)hlen + klen + vlen) < 0) return -1;
    unsigned char hdr[15];
    hdr[0] = op;
    hdr[1] = klen & 0xff;
    hdr[2] = (klen >> 8) & 0xff;
    put_u32le(hdr + 3, vlen);
    if (extra) put_u32le(hdr + 11, meta);
    uint32_t crc;
    if (extra) {
        unsigned char tb[4];
        put_u32le(tb, meta);
        crc = crc32_chain(tb, 4, key, klen, value, vlen);
    } else {
        crc = crc32_two(key, klen, value, vlen);
    }
    put_u32le(hdr + 7, crc);
    memcpy(g_wal_buf.data + g_wal_buf.len, hdr, (size_t)hlen);
    g_wal_buf.len += (size_t)hlen;
    if (klen) memcpy(g_wal_buf.data + g_wal_buf.len, key, klen);
    g_wal_buf.len += klen;
    if (vlen) {
        memcpy(g_wal_buf.data + g_wal_buf.len, value, vlen);
        g_wal_buf.len += vlen;
    }
    return 0;
}

static int wal_flush(void) {
    if (g_wal_fd < 0) return 0;
    pthread_mutex_lock(&g_wal_mu);
    int rc = wal_flush_locked();
    if (rc < 0) atomic_store(&g_wal_failed, 1);
    pthread_mutex_unlock(&g_wal_mu);
    return rc;
}

/* ---------------- recovery ---------------- */

/* Atomic cache-plus-message transactions: the cache WAL record 0x08 is the
 * commit marker ([tx_id:8][sub_op:1][exp:4][value]); it is always flushed and
 * fsynced before the queue WAL TX_COMMIT record is written, so recovery sees
 * either both sides or neither. Atomic operations always fsync both WALs at
 * their commit boundaries regardless of the periodic durability mode. */

static _Atomic uint64_t g_tx_counter;
static _Atomic int g_tx_inflight;   /* transactions inside the commit window */
static uint32_t g_tx_nonce;
static uint64_t *g_tx_committed;    /* cache-committed ids from WAL replay */
static size_t g_tx_committed_n, g_tx_committed_cap;

static uint64_t next_tx_id(void) {
    uint64_t c = atomic_fetch_add(&g_tx_counter, 1) + 1;
    return ((uint64_t)g_tx_nonce << 32) | (c & 0xffffffffull);
}

/* Collects commit ids during WAL replay for startup reconciliation. */
static void tx_committed_add(uint64_t tx_id) {
    if (g_tx_committed_n == g_tx_committed_cap) {
        size_t cap = g_tx_committed_cap ? g_tx_committed_cap * 2 : 64;
        uint64_t *grown = realloc(g_tx_committed, cap * sizeof(*grown));
        if (!grown) {
            fprintf(stderr, "out of memory during transaction recovery\n");
            exit(1);
        }
        g_tx_committed = grown;
        g_tx_committed_cap = cap;
    }
    g_tx_committed[g_tx_committed_n++] = tx_id;
}

static int tx_id_cmp(const void *a, const void *b) {
    uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
    return x < y ? -1 : (x > y ? 1 : 0);
}

static int replay_wal(int fd, off_t *good_off) {
    char key[MAX_KEY];
    for (;;) {
        off_t rec_start = lseek(fd, 0, SEEK_CUR);
        if (rec_start < 0) return -1;
        *good_off = rec_start;
        unsigned char hdr[15];
        ssize_t r = read(fd, hdr, 11);
        if (r == 0) return 0;
        if (r < 0) { if (errno == EINTR) continue; return -1; }
        if (r < 11) return -1;
        unsigned char op = hdr[0];
        uint32_t klen = hdr[1] | ((uint32_t)hdr[2] << 8);
        uint32_t vlen = get_u32le(hdr + 3);
        uint32_t crc = get_u32le(hdr + 7);
        uint32_t meta = 0;
        int extra = 0;
        if (op == 0x05 || op == 0x07) {
            extra = 4;
            if (read(fd, hdr + 11, 4) != 4) return -1;
            meta = get_u32le(hdr + 11);
        }
        if (vlen > g_max_val + (op == 0x08 ? 13u : 0u) ||
            (op != 0x01 && op != 0x03 && op != 0x05 && op != 0x07 &&
             op != 0x08))
            return -1;
        char *val = malloc(vlen ? vlen : 1);
        if (!val) return -1;
        if (read(fd, key, klen) != (ssize_t)klen) { free(val); return -1; }
        if (vlen && read(fd, val, vlen) != (ssize_t)vlen) { free(val); return -1; }
        uint32_t crc_actual;
        if (extra) {
            unsigned char tb[4];
            put_u32le(tb, meta);
            crc_actual = crc32_chain(tb, 4, key, klen, val, vlen);
        } else {
            crc_actual = crc32_two(key, klen, val, vlen);
        }
        if (crc_actual != crc) { free(val); return -1; }
        if (op == 0x01) kuttidb_put(g_cache, key, klen, val, vlen);
        else if (op == 0x05) kuttidb_put_ex(g_cache, key, klen, val, vlen, meta);
        else if (op == 0x07) {
            uint32_t now = (uint32_t)time(NULL);
            if (!meta || meta > now) kuttidb_put_abs(g_cache, key, klen, val, vlen, meta);
        }
        else if (op == 0x08) {
            /* Transaction commit marker: [tx_id:8][sub_op:1][exp:4][value]. */
            if (vlen < 13) { free(val); return -1; }
            uint64_t tx_id = get_u64le((unsigned char *)val);
            unsigned char sub_op = (unsigned char)val[8];
            uint32_t exp = get_u32le((unsigned char *)val + 9);
            if (sub_op != 1 && sub_op != 3 && sub_op != 7 &&
                sub_op != 8 && sub_op != 9) {
                free(val);
                return -1;
            }
            if (sub_op == 1) {
                kuttidb_put_abs(g_cache, key, klen, val + 13, vlen - 13, 0);
            } else if (sub_op == 7) {
                uint32_t now = (uint32_t)time(NULL);
                if (!exp || exp > now)
                    kuttidb_put_abs(g_cache, key, klen, val + 13, vlen - 13, exp);
            } else if (sub_op == 8 || sub_op == 9) {
                /* Conditional update marker (UPDATE_AND_EMIT). The value is
                 * applied only when the key still exists at this point of
                 * the replay, mirroring the live existence check. A crafted
                 * marker for a missing key is an impossible live state: the
                 * transaction stays uncommitted so reconciliation discards
                 * the queue side too (both sides or neither). An expired
                 * conditional marker mirrors sub_op 7: the queue side is
                 * materialized even though the cache apply is skipped. */
                uint32_t now = (uint32_t)time(NULL);
                char *old = NULL;
                uint32_t old_len = 0;
                int expired = sub_op == 9 && exp && exp <= now;
                int hit = expired ? 0
                    : kuttidb_get(g_cache, key, klen, &old, &old_len);
                if (old) free(old);
                if (hit == 1) {
                    kuttidb_put_abs(g_cache, key, klen, val + 13, vlen - 13,
                                  sub_op == 9 ? exp : 0);
                    tx_committed_add(tx_id);
                } else if (expired) {
                    tx_committed_add(tx_id);
                }
            } else {
                kuttidb_delete(g_cache, key, klen);
            }
            if (sub_op == 1 || sub_op == 3 || sub_op == 7)
                tx_committed_add(tx_id);
        }
        else kuttidb_delete(g_cache, key, klen);
        free(val);
    }
}

static int load_snapshot(const char *path) {
    int fd = open(path, O_RDONLY | O_NOFOLLOW);
    if (fd < 0) return -1;
    char key[MAX_KEY];
    size_t count = 0;

    /* v2: "CSN2" magic, records carry [exp:4]; legacy files start with a
     * record header (klen low byte never equals 'C') */
    unsigned char magic[4];
    ssize_t mr = read(fd, magic, 4);
    int has_exp = 0;
    if (mr == 4 && memcmp(magic, "CSN2", 4) == 0)
        has_exp = 1;
    else if (mr < 0)
        goto out;
    else if (lseek(fd, 0, SEEK_SET) < 0)
        goto out;

    for (;;) {
        unsigned char hdr[10];
        size_t hlen = has_exp ? 10 : 6;
        ssize_t r = read(fd, hdr, hlen);
        if (r == 0) break;
        if (r < 0) { if (errno == EINTR) continue; break; }
        if ((size_t)r < hlen) break;
        uint32_t klen = hdr[0] | ((uint32_t)hdr[1] << 8);
        uint32_t vlen = get_u32le(hdr + 2);
        uint32_t exp = 0;
        if (has_exp) exp = get_u32le(hdr + 6);
        if (vlen > g_max_val) break;
        char *val = malloc(vlen ? vlen : 1);
        if (!val) break;
        if (read(fd, key, klen) != (ssize_t)klen ||
            (vlen && read(fd, val, vlen) != (ssize_t)vlen)) {
            free(val);
            break;
        }
        uint32_t now = (uint32_t)time(NULL);
        if (!exp || exp > now) {
            kuttidb_put_abs(g_cache, key, klen, val, vlen, exp);
            count++;
        }
        free(val);
    }
out:
    close(fd);
    fprintf(stderr, "loaded snapshot %s (%zu records, %s)\n",
            path, count, has_exp ? "v2" : "v1");
    return 0;
}

static void load_persisted(void) {
    char path[576];
    snprintf(path, sizeof path, "%s.snap", g_wal_path);
    load_snapshot(path);

    int fd = open(g_wal_path, O_RDONLY | O_NOFOLLOW);
    if (fd < 0) return;
    off_t good = 0;
    int rc = replay_wal(fd, &good);
    close(fd);
    if (rc < 0) {
        int wfd = open(g_wal_path, O_WRONLY | O_NOFOLLOW);
        if (wfd >= 0) {
            ftruncate(wfd, good);
            close(wfd);
        }
        fprintf(stderr, "wal had torn tail, truncated to %lld bytes\n", (long long)good);
    }
    fprintf(stderr, "replayed wal %s\n", g_wal_path);
    g_wal_offset = (unsigned long long)good;
    g_last_snapshot_off = g_wal_offset;
}

/* ---------------- snapshot ---------------- */

static int g_snap_fd = -1;

static int snap_cb(const char *k, uint32_t klen, const char *v,
                   uint32_t vlen, uint32_t exp, void *ctx) {
    KuttiVec *b = ctx;
    if (kuttidb_vec_reserve(b, 10 + (size_t)klen + vlen) < 0) return 1;
    unsigned char hdr[10];
    hdr[0] = klen & 0xff;
    hdr[1] = (klen >> 8) & 0xff;
    put_u32le(hdr + 2, vlen);
    put_u32le(hdr + 6, exp);
    memcpy(b->data + b->len, hdr, 10);
    b->len += 10;
    memcpy(b->data + b->len, k, klen);
    b->len += klen;
    memcpy(b->data + b->len, v, vlen);
    b->len += vlen;
    if (b->len >= WAL_FLUSH_LIMIT) {
        size_t off = 0;
        while (off < b->len) {
            ssize_t r = write(g_snap_fd, b->data + off, b->len - off);
            if (r < 0) { if (errno == EINTR) continue; return 1; }
            off += (size_t)r;
        }
        b->len = 0;
    }
    return 0;
}

static int copy_fd_range(int rfd, int wfd, off_t start, off_t end) {
    char tmp[1 << 16];
    if (lseek(rfd, start, SEEK_SET) < 0) return -1;
    off_t pos = start;
    while (pos < end) {
        size_t want = (size_t)(end - pos) < sizeof tmp ? (size_t)(end - pos) : sizeof tmp;
        ssize_t r = read(rfd, tmp, want);
        if (r <= 0) { if (r < 0 && errno == EINTR) continue; return -1; }
        size_t sent = 0;
        while (sent < (size_t)r) {
            ssize_t w = write(wfd, tmp + sent, (size_t)r - sent);
            if (w < 0) { if (errno == EINTR) continue; return -1; }
            if (w == 0) return -1;
            sent += (size_t)w;
        }
        pos += r;
    }
    return 0;
}

/* snapshot without stalling request processing; brief wal-mutex hold to
 * atomically fold the wal tail into a fresh file */
static int do_snapshot(void) {
    if (g_wal_fd < 0 || atomic_load(&g_wal_failed)) return -1;
    /* Folding drops records below the fold point. A transaction inside its
     * commit window still needs its cache record for reconciliation, so the
     * fold is deferred until every in-flight transaction is committed. */
    if (atomic_load(&g_tx_inflight)) return -1;
    char tmp[600], path[576], tmp2[600];
    snprintf(tmp, sizeof tmp, "%s.snap.tmp", g_wal_path);
    snprintf(path, sizeof path, "%s.snap", g_wal_path);
    snprintf(tmp2, sizeof tmp2, "%s.tmp", g_wal_path);

    unsigned long long start = g_wal_offset;
    if (g_wal_shared) {
        struct stat st;
        if (fstat(g_wal_fd, &st) == 0) start = (unsigned long long)st.st_size;
    }

    int fd = open_private(tmp, O_WRONLY | O_CREAT | O_TRUNC);
    if (fd < 0) return -1;
    g_snap_fd = fd;

    KuttiVec b = {0};
    int failed = 0;
    if (write(fd, "CSN2", 4) < 0) failed = 1;
    if (kuttidb_foreach(g_cache, snap_cb, &b) != 0) failed = 1;
    if (b.len) {
        size_t off = 0;
        while (off < b.len) {
            ssize_t r = write(fd, b.data + off, b.len - off);
            if (r < 0) { if (errno == EINTR) continue; failed = 1; break; }
            off += (size_t)r;
        }
    }
    free(b.data);
    if (failed || fsync(fd) < 0) {
        close(fd);
        unlink(tmp);
        g_snap_fd = -1;
        return -1;
    }
    close(fd);
    g_snap_fd = -1;
    if (rename(tmp, path) < 0) return -1;
    fsync_parent_dir(path);

    /* Copy the stable part of the WAL tail without blocking writers. */
    pthread_mutex_lock(&g_wal_mu);
    int shared_locked = 0;
    if (g_wal_shared) {
        if (flock(g_wal_fd, LOCK_EX) < 0) {
            pthread_mutex_unlock(&g_wal_mu);
            return 0; /* The durable snapshot is already published. */
        }
        shared_locked = 1;
    }
    if (wal_flush_bytes_locked() < 0) atomic_store(&g_wal_failed, 1);
    unsigned long long middle = g_wal_offset;
    if (g_wal_shared) {
        struct stat st;
        if (fstat(g_wal_fd, &st) == 0) middle = (unsigned long long)st.st_size;
    }
    if (middle < start) atomic_store(&g_wal_failed, 1);
    if (shared_locked) flock(g_wal_fd, LOCK_UN);
    pthread_mutex_unlock(&g_wal_mu);

    int wfd = open_private(tmp2, O_RDWR | O_CREAT | O_TRUNC);
    int prepared = wfd >= 0 && middle >= start;
    if (prepared && middle > start) {
        int rfd = open(g_wal_path, O_RDONLY | O_NOFOLLOW);
        prepared = rfd >= 0 &&
            copy_fd_range(rfd, wfd, (off_t)start, (off_t)middle) == 0;
        if (rfd >= 0) close(rfd);
    }
    if (!prepared || fsync(wfd) < 0) {
        if (wfd >= 0) close(wfd);
        unlink(tmp2);
        fprintf(stderr, "snapshot written to %s; wal fold deferred\n", path);
        return 0; /* The durable snapshot is already published. */
    }

    /* Only the writes since `middle` are copied while mutation ordering is held. */
    pthread_mutex_lock(&g_wal_mu);
    shared_locked = 0;
    if (g_wal_shared) {
        if (flock(g_wal_fd, LOCK_EX) < 0) {
            pthread_mutex_unlock(&g_wal_mu);
            close(wfd); unlink(tmp2); return 0; /* Snapshot remains usable. */
        }
        shared_locked = 1;
    }
    int fold_ok = wal_flush_bytes_locked() == 0;
    unsigned long long end = g_wal_offset;
    if (g_wal_shared) {
        struct stat st;
        if (fstat(g_wal_fd, &st) == 0) end = (unsigned long long)st.st_size;
        else fold_ok = 0;
    }
    if (end < start || end < middle) fold_ok = 0;
    if (fold_ok && end > middle) {
        int rfd = open(g_wal_path, O_RDONLY | O_NOFOLLOW);
        fold_ok = rfd >= 0 &&
            copy_fd_range(rfd, wfd, (off_t)middle, (off_t)end) == 0;
        if (rfd >= 0) close(rfd);
    }
    if (fold_ok) fold_ok = fsync(wfd) == 0;

    if (fold_ok && g_wal_shared) {
        off_t tail = (off_t)(end - start);
        fold_ok = ftruncate(g_wal_fd, 0) == 0 &&
                  copy_fd_range(wfd, g_wal_fd, 0, tail) == 0 &&
                  fsync(g_wal_fd) == 0;
        if (fold_ok) g_wal_offset = (unsigned long long)tail;
        close(wfd);
        unlink(tmp2);
    } else if (fold_ok) {
        close(wfd);
        close(g_wal_fd);
        fold_ok = rename(tmp2, g_wal_path) == 0;
        if (fold_ok) {
            fsync_parent_dir(g_wal_path);
            g_wal_fd = open_private(g_wal_path, O_WRONLY | O_APPEND);
            fold_ok = g_wal_fd >= 0;
            g_wal_offset = end - start;
        }
    } else {
        close(wfd);
        unlink(tmp2);
    }
    if (!fold_ok) atomic_store(&g_wal_failed, 1);
    g_last_snapshot_off = g_wal_offset;
    if (shared_locked) flock(g_wal_fd, LOCK_UN);
    pthread_mutex_unlock(&g_wal_mu);
    fprintf(stderr, "snapshot written to %s, wal folded\n", path);
    return fold_ok ? 0 : -1;
}

static void sf_sweep(void);
static void sf_wake_waiters(const char *key, uint32_t klen, unsigned char state);
static void sf_stale_drop(const char *key, uint32_t klen);
static void sf_stale_recount_locked(void);

static void *maintenance_thread(void *arg) {
    (void)arg;
    for (;;) {
        for (int i = 0; i < 10 && !g_stop; i++)
            usleep(g_fsync_ms > 0 ? (useconds_t)g_fsync_ms * 100 : 100000);
        if (managed_lifecycle_should_stop(&g_lifecycle)) {
            /* kill is async-signal-safe and wakes main's pause loop.  The
             * normal signal path below performs the same graceful teardown. */
            fprintf(stderr, "managed lifecycle idle deadline expired; stopping\n");
            kill(getpid(), SIGTERM);
        }
        if (g_stop) {
            if (g_wal_fd >= 0) {
                wal_flush();
                fsync(g_wal_fd);
            }
            return NULL;
        }
        sf_sweep();
        kuttidb_sweep_expired(g_cache, 1024);
        queue_reap(g_queues);
        queue_checkpoint_maybe(g_queues);
        stream_reap(g_streams);
        if (g_wal_fd < 0) continue;
        if (wal_flush() < 0) atomic_store(&g_wal_failed, 1);
        if (g_fsync_ms > 0 && fsync(g_wal_fd) < 0)
            atomic_store(&g_wal_failed, 1);
        if (g_wal_shared) {
            struct stat st;
            if (fstat(g_wal_fd, &st) == 0)
                g_wal_offset = (unsigned long long)st.st_size;
        }
        if (g_wal_offset - g_last_snapshot_off > SNAPSHOT_WAL_BYTES)
            do_snapshot();
    }
}

/* ---------------- event loop core ---------------- */

typedef struct Loop Loop;

typedef struct Conn {
    int fd;
    int is_unix;
    int lifecycle_lease;
    uint64_t id;
    uint32_t queue_prefetch;
    uint64_t queue_consumer_owner; /* named durable consumer, 0 = none */
#ifdef HAVE_OPENSSL
    SSL *tls;
#endif
    int tls_ready;
    KuttiVec in;
    KuttiVec out;
    size_t in_pos;
    size_t out_pos;
    int batch_op;       /* 0 none, 0x11 put batch, 0x12 get batch */
    uint32_t batch_left;
    int batch_err;
    int stage;          /* batch item sub-state */
    uint32_t cur_klen, cur_vlen, cur_ttl;
    uint32_t batch_now;
    unsigned long long batch_bytes;
    unsigned long long batch_out_bytes;
    int authenticated;
    int eof;            /* peer closed / fatal */
    int dead;           /* freed */
    int want_read;      /* read filter registered */
    int want_write;     /* write filter registered */
    int wait_registered; /* singleflight WAIT_FOR_KEY pending */
    PlatformTimer guard; /* handshake/AUTH slowloris deadline */
    struct Conn *next_dead;
} Conn;

/* ---- singleflight (anti-cache-stampede) state ----
 *
 * Claims, waiters, and negative entries are server-side, in-memory only:
 * they are ephemeral coordination state, not data. Claims carry a lease so a
 * crashed loader cannot strand a key. Waiters never block the event loop of
 * the loader: a completed load pushes wake nodes to each waiter's own loop
 * via platform_loop_notify, and each waiter's response is built and flushed
 * on the waiter's own loop thread. */

#define SF_STATE_VALUE 0x00    /* envelope carries the value */
#define SF_STATE_CLAIMED 0x01  /* caller must load and PUT_AND_RELEASE */
#define SF_STATE_WAIT 0x02     /* someone else is loading */
#define SF_STATE_NEGATIVE 0x03 /* cached negative answer */
#define SF_STATE_RELEASED 0x04 /* claim released without a value */
#define SF_STATE_TIMEOUT 0x05  /* wait deadline elapsed */
#define SF_STATE_LOST 0x06     /* value evicted/expired before the wake drain */
#define SF_STATE_STALE 0x07    /* stale-while-revalidate value (may be old) */
#define SF_STATE_REFRESH 0x08  /* fresh value, refresh-ahead window is due */

#define SF_LEASE_MAX_MS 60000ull
#define SF_WAIT_MAX_MS 60000ull
#define SF_NEGATIVE_MAX 4096
#define SF_WAITERS_PER_KEY_MAX 256
#define SF_STALE_MAX 4096
#define SF_STALE_BYTES_MAX (32ull << 20)
#define SF_STALE_WINDOW_MAX_MS (7ull * 24 * 3600 * 1000)

typedef struct Claim {
    struct Claim *next;
    char *key;
    uint32_t klen;
    uint64_t deadline_ms, owner; /* CLOCK_MONOTONIC ms; 0 = legacy protocol */
    unsigned char completing;
} Claim;

typedef struct Waiter {
    struct Waiter *next;
    Conn *conn;
    Loop *loop;
    char *key;
    uint32_t klen;
    uint64_t deadline_ms;
} Waiter;

typedef struct WaiterWake {
    struct WaiterWake *next;
    Conn *conn;
    char *key;
    uint32_t klen;
    unsigned char state;
} WaiterWake;

typedef struct NegEntry {
    struct NegEntry *next;
    char *key;
    uint32_t klen;
    uint64_t deadline_ms;
} NegEntry;

/* Stale-while-revalidate registry: a bounded in-memory copy of the last
 * value written through PUT_SWR, kept past the entry's TTL so GET_OR_REFRESH
 * can serve it while one revalidator reloads. Coordination state like
 * claims/negatives: never visible to plain GET, lost on restart, and never
 * durable (plain cache durability rules apply). */
typedef struct StaleEntry {
    struct StaleEntry *next;
    char *key;
    uint32_t klen;
    char *val;
    uint32_t vlen;
    uint64_t until_ms;    /* CLOCK_MONOTONIC: stale window deadline */
    uint64_t written_ms;  /* CLOCK_MONOTONIC: refresh-ahead age base */
    uint64_t refresh_ms;  /* 0 = no refresh-ahead */
} StaleEntry;

static pthread_mutex_t g_sf_mu = PTHREAD_MUTEX_INITIALIZER;
static Claim *g_claims;
static Waiter *g_waiters;
static NegEntry *g_negatives;
static StaleEntry *g_stale;
static int g_waiters_n;
static int g_negatives_n;
static int g_stale_n;
static size_t g_stale_bytes;
static _Atomic int g_sf_waiters; /* lock-free gate for the put path */
static _Atomic int g_sf_stale;   /* lock-free gate for the put path */

static uint64_t sf_now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

struct Loop {
    PlatformLoop poller;
    int listen_fd;      /* per-loop SO_REUSEPORT listener (udata=NULL marks it) */
    pthread_t th;
    Conn *garbage;
    WaiterWake *wake_head; /* singleflight wakeups for this loop's waiters */
};

static Loop *g_loops;
static int g_nloops;

static void close_conn(Loop *L, Conn *c);
static void flush_out(Loop *L, Conn *c);
#ifdef HAVE_OPENSSL
static int tls_handshake(Loop *L, Conn *c);
#endif
static void cancel_guard_timer(Loop *L, Conn *c);

static inline size_t out_pending(const Conn *c) {
    return c->out.len - c->out_pos;
}

static void vec_shrink_idle(KuttiVec *v, size_t keep) {
    if (v->len || v->cap <= (4u << 20)) return;
    char *p = realloc(v->data, keep);
    if (p) { v->data = p; v->cap = keep; }
}

/* append [status:1][vlen:4][value] for a GET, zero-copy */
static void resp_get_at(Conn *c, const char *key, uint32_t klen, uint32_t now) {
    if (kuttidb_vec_reserve(&c->out, 5) < 0) { c->eof = 1; return; }
    size_t pos = c->out.len;
    c->out.len += 5;
    int rc = now ? kuttidb_get_into_at(g_cache, key, klen, &c->out, now)
                 : kuttidb_get_into(g_cache, key, klen, &c->out);
    unsigned char *sh = (unsigned char *)c->out.data + pos;
    sh[0] = rc == 1 ? 0x00 : (rc == 0 ? 0x01 : 0x02);
    put_u32le(sh + 1, rc == 1 ? (uint32_t)(c->out.len - pos - 5) : 0);
}

static void resp_get(Conn *c, const char *key, uint32_t klen) {
    resp_get_at(c, key, klen, 0);
}

static void resp_status(Conn *c, unsigned char status) {
    if (kuttidb_vec_reserve(&c->out, 5) < 0) { c->eof = 1; return; }
    unsigned char *p = (unsigned char *)c->out.data + c->out.len;
    p[0] = status;
    put_u32le(p + 1, 0);
    c->out.len += 5;
}

static void resp_queue_id(Conn *c, unsigned char status, uint64_t id) {
    if (status != 0x00) { resp_status(c, status); return; }
    if (kuttidb_vec_reserve(&c->out, 13) < 0) { c->eof = 1; return; }
    unsigned char *p = (unsigned char *)c->out.data + c->out.len;
    p[0] = status;
    put_u32le(p + 1, 8);
    put_u64le(p + 5, id);
    c->out.len += 13;
}

static void resp_queue_stats(Conn *c, int rc, uint64_t depth, uint64_t inflight) {
    if (rc < 0) { resp_status(c, 0x02); return; }
    if (rc == 0) { resp_status(c, 0x01); return; }
    if (kuttidb_vec_reserve(&c->out, 21) < 0) { c->eof = 1; return; }
    unsigned char *p = (unsigned char *)c->out.data + c->out.len;
    p[0] = 0x00;
    put_u32le(p + 1, 16);
    put_u64le(p + 5, depth);
    put_u64le(p + 13, inflight);
    c->out.len += 21;
}

static void resp_queue_message(Conn *c, int rc, QueueMessage *message) {
    if (rc < 0) { resp_status(c, 0x02); return; }
    if (rc == 0) { resp_status(c, 0x01); return; }
    if (message->len > UINT32_MAX - 21 || kuttidb_vec_reserve(&c->out, 26 + message->len) < 0) {
        c->eof = 1;
        return;
    }
    unsigned char *p = (unsigned char *)c->out.data + c->out.len;
    p[0] = 0x00;
    put_u32le(p + 1, message->len + 21);
    put_u64le(p + 5, message->delivery_tag);
    put_u64le(p + 13, message->id);
    p[21] = message->redelivered ? 1 : 0;
    put_u32le(p + 22, message->delivery_count);
    if (message->len) memcpy(p + 26, message->data, message->len);
    c->out.len += 26 + message->len;
}

static void resp_stream_append(Conn *c, int rc, uint64_t partition, uint64_t offset) {
    if (rc < 0) { resp_status(c, 0x02); return; }
    if (kuttidb_vec_reserve(&c->out, 21) < 0) { c->eof = 1; return; }
    unsigned char *p = (unsigned char *)c->out.data + c->out.len;
    p[0] = 0x00; put_u32le(p + 1, 16); put_u64le(p + 5, partition);
    put_u64le(p + 13, offset); c->out.len += 21;
}

static void resp_capabilities(Conn *c) {
    if (kuttidb_vec_reserve(&c->out, 17) < 0) { c->eof = 1; return; }
    unsigned char *p = (unsigned char *)c->out.data + c->out.len;
    uint64_t caps = CAP_CACHE | CAP_QUEUES | CAP_EXCHANGES | CAP_ATOMIC |
                    CAP_SINGLEFLIGHT | CAP_STREAMS | CAP_STREAM_BATCH | CAP_HEALTH |
                    CAP_STREAM_GEN | CAP_QUEUE_CONSUMERS | CAP_ATOMIC_UPDATE |
                    CAP_SWR | CAP_QUEUE_BATCHES | CAP_STREAM_COMMIT_BATCH |
                    CAP_STREAM_KEYS | CAP_SERVER_INFO;
    p[0] = 0x00; put_u32le(p + 1, 12); put_u16le(p + 5, PROTOCOL_MAJOR);
    put_u16le(p + 7, PROTOCOL_MINOR); put_u64le(p + 9, caps);
    c->out.len += 17;
}

/* Authenticated instance proof for managed clients.  This deliberately omits
 * storage, credential, and command-line paths. */
static void resp_server_info(Conn *c, unsigned char transport) {
    if (!g_has_instance_id) { resp_status(c, 0x01); return; }
    const size_t bytes = 1 + 1 + 32 + 1 + 8 + 8 + 1;
    if (kuttidb_vec_reserve(&c->out, 5 + bytes) < 0) { c->eof = 1; return; }
    unsigned char *p = (unsigned char *)c->out.data + c->out.len;
    p[0] = 0x00; put_u32le(p + 1, (uint32_t)bytes);
    p[5] = 1; p[6] = 32; memcpy(p + 7, g_instance_id, 32);
    p[39] = g_lifecycle_mode == MANAGED_IDLE ? 1 : 0;
    put_u64le(p + 40, (uint64_t)g_started_at * 1000);
    put_u64le(p + 48, (uint64_t)getpid()); p[56] = transport;
    c->out.len += 5 + bytes;
}

static void resp_stream_append_batch(Conn *c, int rc,
                                     const StreamAppendResult *results,
                                     uint32_t count) {
    if (rc < 0 || count > STREAM_FETCH_MAX) { resp_status(c, 0x02); return; }
    size_t bytes = 4 + (size_t)count * 16;
    if (kuttidb_vec_reserve(&c->out, 5 + bytes) < 0) { c->eof = 1; return; }
    unsigned char *p = (unsigned char *)c->out.data + c->out.len;
    p[0] = 0x00; put_u32le(p + 1, (uint32_t)bytes); put_u32le(p + 5, count);
    for (uint32_t i = 0; i < count; i++) {
        put_u64le(p + 9 + (size_t)i * 16, results[i].partition);
        put_u64le(p + 17 + (size_t)i * 16, results[i].offset);
    }
    c->out.len += 5 + bytes;
}

static void resp_stream_fetch(Conn *c, int rc, StreamRecordView *records,
                              uint32_t count) {
    if (rc < 0) { resp_status(c, 0x02); return; }
    if (rc == 0) { resp_status(c, 0x01); return; }
    size_t bytes = 4;
    for (uint32_t i = 0; i < count; i++) {
        if (records[i].len > g_max_val || bytes > UINT32_MAX - 12 - records[i].len ||
            bytes > g_max_batch_bytes - 12 - records[i].len) {
            resp_status(c, 0x02); return;
        }
        bytes += 12 + records[i].len;
    }
    if (kuttidb_vec_reserve(&c->out, 5 + bytes) < 0) { c->eof = 1; return; }
    unsigned char *p = (unsigned char *)c->out.data + c->out.len;
    p[0] = 0x00; put_u32le(p + 1, (uint32_t)bytes); put_u32le(p + 5, count);
    size_t at = 9;
    for (uint32_t i = 0; i < count; i++) {
        put_u64le(p + at, records[i].offset); put_u32le(p + at + 8, records[i].len);
        if (records[i].len) memcpy(p + at + 12, records[i].data, records[i].len);
        at += 12 + records[i].len;
    }
    c->out.len += 5 + bytes;
}

/* 0x6c is deliberately additive: 0x62 keeps its original body-only reply
 * shape for old clients, while this format round-trips each binary key. */
static void resp_stream_fetch_keys(Conn *c, int rc, StreamRecordView *records,
                                   uint32_t count) {
    if (rc < 0) { resp_status(c, 0x02); return; }
    if (rc == 0) { resp_status(c, 0x01); return; }
    size_t bytes = 4;
    for (uint32_t i = 0; i < count; i++) {
        if (records[i].len > g_max_val || records[i].key_len > g_max_val ||
            bytes > UINT32_MAX - 14 - records[i].len - records[i].key_len ||
            bytes > g_max_batch_bytes - 14 - records[i].len - records[i].key_len) {
            resp_status(c, 0x02); return;
        }
        bytes += 14 + records[i].len + records[i].key_len;
    }
    if (kuttidb_vec_reserve(&c->out, 5 + bytes) < 0) { c->eof = 1; return; }
    unsigned char *p = (unsigned char *)c->out.data + c->out.len;
    p[0] = 0x00; put_u32le(p + 1, (uint32_t)bytes); put_u32le(p + 5, count);
    size_t at = 9;
    for (uint32_t i = 0; i < count; i++) {
        put_u64le(p + at, records[i].offset); put_u16le(p + at + 8, records[i].key_len);
        put_u32le(p + at + 10, records[i].len);
        if (records[i].key_len) memcpy(p + at + 14, records[i].key, records[i].key_len);
        if (records[i].len) memcpy(p + at + 14 + records[i].key_len, records[i].data, records[i].len);
        at += 14 + records[i].key_len + records[i].len;
    }
    c->out.len += 5 + bytes;
}

/* Join responses carry the group generation after the assignment so members
 * can detect rebalances without diffing partitions (CAP_STREAM_GEN). */
static void resp_stream_assignment(Conn *c, int rc, const uint32_t *parts,
                                   uint32_t count, uint64_t generation) {
    if (rc < 0 || count > STREAM_PARTITIONS_MAX) { resp_status(c, 0x02); return; }
    size_t bytes = 4 + (size_t)count * 4 + 8;
    if (kuttidb_vec_reserve(&c->out, 5 + bytes) < 0) { c->eof = 1; return; }
    unsigned char *p = (unsigned char *)c->out.data + c->out.len;
    p[0] = 0x00; put_u32le(p + 1, (uint32_t)bytes); put_u32le(p + 5, count);
    for (uint32_t i = 0; i < count; i++) put_u32le(p + 9 + i * 4, parts[i]);
    put_u64le(p + 9 + (size_t)count * 4, generation);
    c->out.len += 5 + bytes;
}

static int persist_put(const char *key, uint32_t klen, const char *val,
                       uint32_t vlen, uint64_t ttl_ms, int batch) {
    uint32_t exp = kuttidb_expiry_from_ttl(ttl_ms);
    if (!g_persist_enabled) {
        int rc = kuttidb_put_abs(g_cache, key, klen, val, vlen, exp);
        if (rc == 0) {
            if (atomic_load(&g_sf_waiters) > 0)
                sf_wake_waiters(key, klen, SF_STATE_VALUE);
            if (atomic_load(&g_sf_stale) > 0) sf_stale_drop(key, klen);
        }
        return rc;
    }
    if (g_wal_fd < 0) return -1;

    pthread_mutex_lock(&g_wal_mu);
    int file_locked = 0;
    int rc = -1;
    if (g_wal_fd < 0 || atomic_load(&g_wal_failed)) goto out;
    int wal_fd = g_wal_fd;
    if (g_wal_shared) {
        if (flock(g_wal_fd, LOCK_EX) < 0) goto fail;
        file_locked = 1;
    }
    if (kuttidb_vec_reserve(&g_wal_buf, 15 + (size_t)klen + vlen) < 0) goto fail;
    rc = kuttidb_put_abs(g_cache, key, klen, val, vlen, exp);
    if (rc < 0) goto out;
    if (wal_log_locked(exp ? 0x07 : 0x01, key, klen, val, vlen, exp) < 0)
        goto fail;
    if (g_wal_shared || g_wal_buf.len >= WAL_FLUSH_LIMIT ||
        (!batch && g_durability == DUR_ALWAYS)) {
        int wr = file_locked ? wal_flush_bytes_locked() : wal_flush_locked();
        if (wr < 0) goto fail;
    }
    if (!batch && g_durability == DUR_ALWAYS && fsync(wal_fd) < 0) goto fail;
    rc = 0;
    /* A waiter on this key is satisfied by any successful put. */
    if (atomic_load(&g_sf_waiters) > 0)
        sf_wake_waiters(key, klen, SF_STATE_VALUE);
    if (atomic_load(&g_sf_stale) > 0)
        sf_stale_drop(key, klen); /* fresh value supersedes the stale copy */
    goto out;
fail:
    atomic_store(&g_wal_failed, 1);
    rc = -1;
out:
    if (file_locked) flock(g_wal_fd, LOCK_UN);
    pthread_mutex_unlock(&g_wal_mu);
    return rc;
}

static int persist_delete(const char *key, uint32_t klen) {
    if (!g_persist_enabled) {
        int rc = kuttidb_delete(g_cache, key, klen);
        if (rc > 0 && atomic_load(&g_sf_stale) > 0) sf_stale_drop(key, klen);
        return rc;
    }
    if (g_wal_fd < 0) return -1;
    pthread_mutex_lock(&g_wal_mu);
    int file_locked = 0;
    int found = -1;
    if (g_wal_fd < 0 || atomic_load(&g_wal_failed)) goto out;
    int wal_fd = g_wal_fd;
    if (g_wal_shared) {
        if (flock(g_wal_fd, LOCK_EX) < 0) goto fail;
        file_locked = 1;
    }
    if (kuttidb_vec_reserve(&g_wal_buf, 11 + (size_t)klen) < 0) goto fail;
    found = kuttidb_delete(g_cache, key, klen);
    if (!found) goto out;
    if (wal_log_locked(0x03, key, klen, "", 0, 0) < 0) goto fail;
    if (g_wal_shared || g_durability == DUR_ALWAYS) {
        int wr = file_locked ? wal_flush_bytes_locked() : wal_flush_locked();
        if (wr < 0) goto fail;
    }
    if (g_durability == DUR_ALWAYS && fsync(wal_fd) < 0) goto fail;
    if (atomic_load(&g_sf_stale) > 0)
        sf_stale_drop(key, klen); /* a delete is a fresh state change */
    goto out;
fail:
    atomic_store(&g_wal_failed, 1);
    found = -1;
out:
    if (file_locked) flock(g_wal_fd, LOCK_UN);
    pthread_mutex_unlock(&g_wal_mu);
    return found;
}

/* ---------------- atomic cache-plus-message transactions ----------------
 *
 * The cache-WAL record 0x08 is the commit marker: [tx_id:8][sub_op:1][exp:4]
 * followed by the payload value (empty for deletes). It is always flushed and
 * fsynced before the queue-WAL TX_COMMIT record is written, so recovery sees
 * either both sides or neither. */

static int wal_log_tx_locked(uint64_t tx_id, unsigned char sub_op,
                             const char *key, uint32_t klen,
                             const char *val, uint32_t vlen, uint32_t exp) {
    if (g_wal_fd < 0) return -1;
    if ((klen && !key) || (vlen && !val)) return -1;
    unsigned char pre[13];
    put_u64le(pre, tx_id);
    pre[8] = sub_op;
    put_u32le(pre + 9, exp);
    if (kuttidb_vec_reserve(&g_wal_buf, 24 + (size_t)klen + vlen) < 0) return -1;
    unsigned char hdr[11];
    hdr[0] = 0x08;
    hdr[1] = klen & 0xff;
    hdr[2] = (klen >> 8) & 0xff;
    put_u32le(hdr + 3, 13 + vlen);
    /* replay reads the record value as one blob (prefix + payload) and
     * checksums key||blob, matching crc32_two(key, blob) */
    put_u32le(hdr + 7, crc32_chain(key, klen, pre, 13, val, vlen));
    memcpy(g_wal_buf.data + g_wal_buf.len, hdr, 11);
    g_wal_buf.len += 11;
    if (klen) {
        memcpy(g_wal_buf.data + g_wal_buf.len, key, klen);
        g_wal_buf.len += klen;
    }
    memcpy(g_wal_buf.data + g_wal_buf.len, pre, 13);
    g_wal_buf.len += 13;
    if (vlen) {
        memcpy(g_wal_buf.data + g_wal_buf.len, val, vlen);
        g_wal_buf.len += vlen;
    }
    return 0;
}

/* The commit point of an atomic operation: log, flush, fsync, then apply the
 * cache mutation in memory under the same ordering lock. The mutation is
 * applied only after its commit record is durable, so a failed commit leaves
 * memory untouched (unlike persist_put, where the value is applied first and
 * a lost WAL tail merely loses it). */
static int persist_tx_commit(uint64_t tx_id, unsigned char sub_op,
                             const char *key, uint32_t klen,
                             const char *val, uint32_t vlen, uint32_t exp,
                             int require_exists) {
    if (!g_persist_enabled || g_wal_fd < 0) return -1;
    pthread_mutex_lock(&g_wal_mu);
    int file_locked = 0;
    int rc = -1;
    if (g_wal_fd < 0 || atomic_load(&g_wal_failed)) goto out;
    int wal_fd = g_wal_fd;
    if (g_wal_shared) {
        if (flock(g_wal_fd, LOCK_EX) < 0) goto fail;
        file_locked = 1;
    }
    if (require_exists) {
        /* Conditional update: refuse (and write nothing) when the key is
         * absent. -2 tells the caller nothing was persisted. */
        char *old = NULL;
        uint32_t old_len = 0;
        int hit = kuttidb_get(g_cache, key, klen, &old, &old_len);
        if (old) free(old);
        if (hit != 1) { rc = -2; goto out; }
    }
    if (kuttidb_vec_reserve(&g_wal_buf, 24 + (size_t)klen + vlen) < 0) goto fail;
    if (wal_log_tx_locked(tx_id, sub_op, key, klen, val, vlen, exp) < 0)
        goto fail;
    int wr = file_locked ? wal_flush_bytes_locked() : wal_flush_locked();
    if (wr < 0) goto fail;
    if (fsync(wal_fd) < 0) goto fail;
    if (sub_op == 3) {
        kuttidb_delete(g_cache, key, klen);
    } else if (sub_op == 7 || sub_op == 9) {
        uint32_t now = (uint32_t)time(NULL);
        if (exp && exp <= now) { rc = 0; goto out; } /* already expired */
        kuttidb_put_abs(g_cache, key, klen, val, vlen, exp);
    } else {
        kuttidb_put_abs(g_cache, key, klen, val, vlen, 0);
    }
    rc = 0;
    if (atomic_load(&g_sf_waiters) > 0)
        sf_wake_waiters(key, klen, SF_STATE_VALUE);
    goto out;
fail:
    atomic_store(&g_wal_failed, 1);
    rc = -1;
out:
    if (file_locked) flock(g_wal_fd, LOCK_UN);
    pthread_mutex_unlock(&g_wal_mu);
    return rc;
}

/* After both recoveries: finish or discard transactions that were inside
 * their commit window when the process died. */
static void reconcile_transactions(void) {
    if (g_tx_committed_n) qsort(g_tx_committed, g_tx_committed_n,
                                sizeof(*g_tx_committed), tx_id_cmp);
    uint64_t *ids = NULL;
    uint64_t n = queue_tx_pending_ids(g_queues, &ids);
    for (uint64_t i = 0; i < n; i++) {
        uint64_t key = ids[i];
        int committed = g_tx_committed_n
            ? bsearch(&key, g_tx_committed, g_tx_committed_n,
                      sizeof(*g_tx_committed), tx_id_cmp) != NULL : 0;
        if (queue_tx_resolve(g_queues, key, committed) < 0) {
            fprintf(stderr,
                    "fatal: could not finish transaction recovery for %llu\n",
                    (unsigned long long)key);
            exit(1);
        }
    }
    free(ids);
    free(g_tx_committed);
    g_tx_committed = NULL;
    g_tx_committed_n = 0;
    g_tx_committed_cap = 0;
}

static int persist_batch_commit(void) {
    if (!g_persist_enabled) return 0;
    if (g_wal_fd < 0) return -1;
    pthread_mutex_lock(&g_wal_mu);
    int rc = (g_wal_fd < 0 || atomic_load(&g_wal_failed))
           ? -1 : wal_flush_locked();
    if (rc == 0 && g_durability == DUR_ALWAYS) rc = fsync(g_wal_fd);
    if (rc < 0) atomic_store(&g_wal_failed, 1);
    pthread_mutex_unlock(&g_wal_mu);
    return rc;
}

/* Execute one atomic cache-plus-message operation. Returns 0 committed
 * (tx_id/routed set), 1 unroutable (nothing happened), 2 required cache key
 * missing (nothing happened), -1 error. The cache commit marker is fsynced
 * between the queue WAL prepare and commit records, so every crash point
 * leaves both sides or neither after recovery. */
static int atomic_execute(int op, const char *ckey, uint32_t cklen,
                          const char *target, uint32_t target_len,
                          const char *rkey, uint32_t rklen,
                          const void *msg, uint32_t msg_len,
                          uint64_t kuttidb_ttl_ms, uint64_t *out_tx,
                          uint64_t *out_routed, int require_exists) {
    if (!queue_wal_enabled(g_queues) || g_wal_fd < 0) return -1;
    uint64_t tx_id = next_tx_id();
    atomic_fetch_add(&g_tx_inflight, 1);
    QueueTx *tx = NULL;
    int rc = queue_tx_prepare(g_queues,
                              op == ATOMIC_PUT_ENQUEUE ? NULL : target,
                              op == ATOMIC_PUT_ENQUEUE ? 0 : target_len,
                              op == ATOMIC_PUT_ENQUEUE ? target : rkey,
                              op == ATOMIC_PUT_ENQUEUE ? target_len : rklen,
                              msg, msg_len, tx_id, &tx);
    if (rc != 0) {
        atomic_fetch_sub(&g_tx_inflight, 1);
        return rc == 1 ? 1 : -1;
    }
    unsigned char sub_op = op == ATOMIC_DELETE_PUBLISH ? 3
                         : (kuttidb_ttl_ms ? (require_exists ? 9 : 7)
                                         : (require_exists ? 8 : 1));
    const char *val = op == ATOMIC_DELETE_PUBLISH ? "" : (const char *)msg;
    uint32_t vlen = op == ATOMIC_DELETE_PUBLISH ? 0 : msg_len;
    uint32_t exp = kuttidb_expiry_from_ttl(kuttidb_ttl_ms);
    int pc = persist_tx_commit(tx_id, sub_op, ckey, cklen, val, vlen, exp,
                               require_exists);
    if (pc != 0) {
        queue_tx_abort(tx);
        atomic_fetch_sub(&g_tx_inflight, 1);
        return pc == -2 ? 2 : -1;
    }
    uint64_t routed = 0;
    int crc = queue_tx_commit(tx, &routed);
    atomic_fetch_sub(&g_tx_inflight, 1);
    if (crc < 0) return -1; /* in doubt: recovery reconciles this transaction */
    *out_tx = tx_id;
    *out_routed = routed;
    return 0;
}

/* Public boundary for the Management HTTP adapter.  The HTTP module supplies
 * a compact API operation union; this wrapper keeps the server's protocol
 * opcodes and cache/queue WAL ordering private. */
static int admin_atomic_execute(unsigned operation, const char *key,
                                uint32_t key_len, const char *target,
                                uint32_t target_len, const char *routing_key,
                                uint32_t routing_key_len, const void *body,
                                uint32_t body_len, uint64_t ttl_ms,
                                uint64_t *out_transaction_id,
                                uint64_t *out_routed) {
    int op = operation == 1 ? ATOMIC_PUT_PUBLISH :
             operation == 2 ? ATOMIC_PUT_ENQUEUE :
             operation == 3 ? ATOMIC_DELETE_PUBLISH :
             operation == 4 ? ATOMIC_UPDATE_EMIT : -1;
    if (op < 0) return -1;
    return atomic_execute(op, key, key_len, target, target_len, routing_key,
                          routing_key_len, body, body_len, ttl_ms,
                          out_transaction_id, out_routed, operation == 4);
}

static int admin_keyspace_checkpoint(void) {
    if (!g_persist_enabled || g_wal_fd < 0 || atomic_load(&g_wal_failed)) return -1;
    return do_snapshot();
}

/* ---- singleflight (anti-cache-stampede) implementation ----
 *
 * All registry mutations happen under g_sf_mu and only ever do bounded list
 * work, so neither the loader's put path nor any event loop blocks on loader
 * progress. Lock order: g_wal_mu -> g_sf_mu (never the reverse). */

static int sf_claim_get_or_create_owned(const char *key, uint32_t klen,
                                        uint64_t lease_ms, uint64_t owner) {
    uint64_t now = sf_now_ms();
    pthread_mutex_lock(&g_sf_mu);
    Claim **link = &g_claims;
    int created = 0;
    for (Claim *claim = g_claims; claim;) {
        if (claim->deadline_ms <= now && !claim->completing) { /* expired lease: reclaimable */
            Claim *dead = claim;
            claim = claim->next;
            *link = claim;
            free(dead->key);
            free(dead);
            continue;
        }
        if (claim->klen == klen && memcmp(claim->key, key, klen) == 0) {
            pthread_mutex_unlock(&g_sf_mu);
            return 0; /* someone else holds a live lease */
        }
        link = &claim->next;
        claim = claim->next;
    }
    Claim *claim = calloc(1, sizeof(*claim));
    if (claim) {
        claim->key = malloc(klen);
        if (claim->key) {
            memcpy(claim->key, key, klen);
            claim->klen = klen;
            claim->deadline_ms = now + lease_ms;
            claim->owner = owner;
            claim->next = g_claims;
            g_claims = claim;
            created = 1;
        } else {
            free(claim);
        }
    }
    pthread_mutex_unlock(&g_sf_mu);
    return created ? 1 : -1;
}

static int sf_claim_get_or_create(const char *key, uint32_t klen,
                                  uint64_t lease_ms) {
    return sf_claim_get_or_create_owned(key,klen,lease_ms,0);
}

static int sf_claim_remove_owned(const char *key, uint32_t klen,
                                 uint64_t owner) {
    pthread_mutex_lock(&g_sf_mu);
    for (Claim **link = &g_claims; *link; link = &(*link)->next) {
        Claim *claim = *link;
        if (claim->klen == klen && memcmp(claim->key, key, klen) == 0) {
            if (claim->owner != owner) { pthread_mutex_unlock(&g_sf_mu); return -2; }
            *link = claim->next;
            free(claim->key);
            free(claim);
            pthread_mutex_unlock(&g_sf_mu);
            return 1;
        }
    }
    pthread_mutex_unlock(&g_sf_mu);
    return 0;
}

static int sf_claim_remove(const char *key, uint32_t klen) {
    return sf_claim_remove_owned(key,klen,0);
}

/* A completion temporarily pins its live claim across the durable write.
 * No replacement owner can acquire the key in that window; restart clears
 * this entirely in-memory state if the process dies. */
static int sf_claim_begin_complete_owned(const char *key, uint32_t klen,
                                         uint64_t owner) {
    uint64_t now=sf_now_ms();pthread_mutex_lock(&g_sf_mu);
    for(Claim *claim=g_claims;claim;claim=claim->next)if(claim->klen==klen&&!memcmp(claim->key,key,klen)){
        if(claim->owner!=owner||claim->deadline_ms<=now||claim->completing){pthread_mutex_unlock(&g_sf_mu);return 0;}
        claim->completing=1;pthread_mutex_unlock(&g_sf_mu);return 1;
    }
    pthread_mutex_unlock(&g_sf_mu);return 0;
}

/* Detach one connection's waiter and any wake node already queued for it. */
static void sf_waiter_detach(Loop *L, Conn *c) {
    if (!c->wait_registered) return;
    pthread_mutex_lock(&g_sf_mu);
    for (Waiter **link = &g_waiters; *link; link = &(*link)->next) {
        Waiter *w = *link;
        if (w->conn == c) {
            *link = w->next;
            free(w->key);
            free(w);
            g_waiters_n--;
            atomic_fetch_sub(&g_sf_waiters, 1);
            break;
        }
    }
    for (WaiterWake **link = &L->wake_head; *link;) {
        WaiterWake *node = *link;
        if (node->conn == c) {
            *link = node->next;
            free(node->key);
            free(node);
        } else {
            link = &node->next;
        }
    }
    pthread_mutex_unlock(&g_sf_mu);
    c->wait_registered = 0;
}

/* Push a wake node to each waiter of the key and notify its loop. Called with
 * the caller's own locks possibly held (put path); never blocks on I/O. */
static void sf_wake_waiters(const char *key, uint32_t klen, unsigned char state) {
    pthread_mutex_lock(&g_sf_mu);
    Waiter *woken = NULL;
    Waiter **link = &g_waiters;
    while (*link) {
        Waiter *w = *link;
        if (w->klen == klen && memcmp(w->key, key, klen) == 0) {
            *link = w->next;
            g_waiters_n--;
            atomic_fetch_sub(&g_sf_waiters, 1);
            w->next = woken;
            woken = w;
            continue;
        }
        link = &w->next;
    }
    for (Waiter *w = woken; w;) {
        Waiter *next = w->next;
        WaiterWake *node = malloc(sizeof(*node));
        if (node) {
            node->key = malloc(klen);
            if (node->key) {
                memcpy(node->key, key, klen);
                node->klen = klen;
                node->conn = w->conn;
                node->state = state;
                node->next = w->loop->wake_head;
                w->loop->wake_head = node;
                platform_loop_notify(&w->loop->poller);
            } else {
                free(node);
            }
        }
        free(w->key);
        free(w);
        w = next;
    }
    pthread_mutex_unlock(&g_sf_mu);
}

/* Maintenance tick: expire overdue waiters (their loop answers with a
 * timeout state), prune expired negative entries, and prune expired
 * stale-while-revalidate copies. */
static void sf_sweep(void) {
    uint64_t now = sf_now_ms();
    if (atomic_load(&g_sf_waiters) == 0 && g_negatives_n == 0 &&
        atomic_load(&g_sf_stale) == 0) return;
    pthread_mutex_lock(&g_sf_mu);
    Waiter *expired = NULL;
    Waiter **link = &g_waiters;
    while (*link) {
        Waiter *w = *link;
        if (w->deadline_ms <= now) {
            *link = w->next;
            g_waiters_n--;
            atomic_fetch_sub(&g_sf_waiters, 1);
            w->next = expired;
            expired = w;
            continue;
        }
        link = &w->next;
    }
    for (Waiter *w = expired; w;) {
        Waiter *next = w->next;
        WaiterWake *node = malloc(sizeof(*node));
        if (node) {
            node->conn = w->conn;
            node->key = NULL;
            node->klen = 0;
            node->state = SF_STATE_TIMEOUT;
            node->next = w->loop->wake_head;
            w->loop->wake_head = node;
            platform_loop_notify(&w->loop->poller);
        }
        free(w->key);
        free(w);
        w = next;
    }
    NegEntry **nlink = &g_negatives;
    while (*nlink) {
        NegEntry *n = *nlink;
        if (n->deadline_ms <= now) {
            *nlink = n->next;
            free(n->key);
            free(n);
            g_negatives_n--;
        } else {
            nlink = &n->next;
        }
    }
    StaleEntry **slink = &g_stale;
    while (*slink) {
        StaleEntry *s = *slink;
        if (s->until_ms <= now) {
            *slink = s->next;
            g_stale_bytes -= sizeof(*s) + (size_t)s->klen + s->vlen;
            free(s->key);
            free(s->val);
            free(s);
            g_stale_n--;
        } else {
            slink = &s->next;
        }
    }
    sf_stale_recount_locked();
    pthread_mutex_unlock(&g_sf_mu);
}

/* On the waiter's own loop: build the deferred response and flush. */
static void sf_drain_wakes(Loop *L) {
    pthread_mutex_lock(&g_sf_mu);
    WaiterWake *list = L->wake_head;
    L->wake_head = NULL;
    pthread_mutex_unlock(&g_sf_mu);
    while (list) {
        WaiterWake *node = list;
        list = list->next;
        Conn *c = node->conn;
        if (!c->dead) {
            if (node->state == SF_STATE_VALUE && node->key) {
                /* Value envelope: [OK][len][state][value]; the value is read
                 * from the cache on this loop. If it was already evicted the
                 * waiter gets a loss state instead of stale data. */
                if (kuttidb_vec_reserve(&c->out, 6) < 0) {
                    c->eof = 1;
                } else {
                    size_t pos = c->out.len;
                    c->out.len += 6;
                    ((unsigned char *)c->out.data)[pos] = 0x00;
                    put_u32le((unsigned char *)c->out.data + pos + 1, 1);
                    ((unsigned char *)c->out.data)[pos + 5] = SF_STATE_VALUE;
                    int rc = kuttidb_get_into(g_cache, node->key, node->klen,
                                            &c->out);
                    if (rc != 1) {
                        c->out.len = pos; /* fall back to a loss state */
                        if (kuttidb_vec_reserve(&c->out, 6) >= 0) {
                            unsigned char *p =
                                (unsigned char *)c->out.data + pos;
                            p[0] = 0x00;
                            put_u32le(p + 1, 1);
                            p[5] = SF_STATE_LOST;
                            c->out.len += 6;
                        } else {
                            c->eof = 1;
                        }
                    } else {
                        put_u32le((unsigned char *)c->out.data + pos + 1,
                                  (uint32_t)(c->out.len - pos - 5));
                    }
                }
            } else {
                if (kuttidb_vec_reserve(&c->out, 6) >= 0) {
                    unsigned char *p =
                        (unsigned char *)c->out.data + c->out.len;
                    p[0] = 0x00;
                    put_u32le(p + 1, 1);
                    p[5] = node->state;
                    c->out.len += 6;
                } else {
                    c->eof = 1;
                }
            }
            flush_out(L, c);
        }
        free(node->key);
        free(node);
    }
}

static int sf_negative_check(const char *key, uint32_t klen) {
    uint64_t now = sf_now_ms();
    pthread_mutex_lock(&g_sf_mu);
    for (NegEntry **link = &g_negatives; *link;) {
        NegEntry *n = *link;
        if (n->deadline_ms <= now) {
            *link = n->next;
            free(n->key);
            free(n);
            g_negatives_n--;
            continue;
        }
        if (n->klen == klen && memcmp(n->key, key, klen) == 0) {
            pthread_mutex_unlock(&g_sf_mu);
            return 1;
        }
        link = &n->next;
    }
    pthread_mutex_unlock(&g_sf_mu);
    return 0;
}

static void sf_negative_record(const char *key, uint32_t klen, uint64_t ttl_ms) {
    uint64_t now = sf_now_ms();
    pthread_mutex_lock(&g_sf_mu);
    for (NegEntry *n = g_negatives; n; n = n->next) {
        if (n->klen == klen && memcmp(n->key, key, klen) == 0) {
            n->deadline_ms = now + (ttl_ms ? ttl_ms : 60000);
            pthread_mutex_unlock(&g_sf_mu);
            return;
        }
    }
    if (g_negatives_n >= SF_NEGATIVE_MAX) {
        pthread_mutex_unlock(&g_sf_mu);
        return; /* bounded registry: the negative answer is simply not cached */
    }
    NegEntry *n = calloc(1, sizeof(*n));
    if (n && (n->key = malloc(klen))) {
        memcpy(n->key, key, klen);
        n->klen = klen;
        n->deadline_ms = now + (ttl_ms ? ttl_ms : 60000);
        n->next = g_negatives;
        g_negatives = n;
        g_negatives_n++;
    } else {
        free(n);
    }
    pthread_mutex_unlock(&g_sf_mu);
}

/* ---- stale-while-revalidate registry (bounded, in-memory) -------------- */

static void sf_stale_recount_locked(void) {
    atomic_store(&g_sf_stale, g_stale_n > 0);
}

/* Store the value written through PUT_SWR. Replaces any entry for the key,
 * prunes expired siblings first, and refuses inserts once the bounded
 * registry is full (SWR then degrades to plain claim/wait). */
static void sf_stale_store(const char *key, uint32_t klen,
                           const char *val, uint32_t vlen,
                           uint64_t ttl_ms, uint64_t stale_ms,
                           uint64_t refresh_ms) {
    uint64_t now = sf_now_ms();
    pthread_mutex_lock(&g_sf_mu);
    for (StaleEntry **link = &g_stale; *link;) {
        StaleEntry *s = *link;
        if (s->until_ms <= now ||
            (s->klen == klen && memcmp(s->key, key, klen) == 0)) {
            *link = s->next;
            g_stale_bytes -= sizeof(*s) + (size_t)s->klen + s->vlen;
            free(s->key);
            free(s->val);
            free(s);
            g_stale_n--;
            continue;
        }
        link = &s->next;
    }
    if (g_stale_n >= SF_STALE_MAX ||
        g_stale_bytes + sizeof(StaleEntry) + (size_t)klen + vlen
            > SF_STALE_BYTES_MAX) {
        sf_stale_recount_locked();
        pthread_mutex_unlock(&g_sf_mu);
        return; /* bounded registry: the stale copy is simply not kept */
    }
    StaleEntry *s = calloc(1, sizeof(*s));
    char *kcopy = malloc(klen);
    char *vcopy = malloc(vlen ? vlen : 1);
    if (s && kcopy && vcopy) {
        memcpy(kcopy, key, klen);
        memcpy(vcopy, val, vlen);
        s->key = kcopy;
        s->klen = klen;
        s->val = vcopy;
        s->vlen = vlen;
        s->until_ms = now + ttl_ms + stale_ms;
        s->written_ms = now;
        s->refresh_ms = refresh_ms;
        s->next = g_stale;
        g_stale = s;
        g_stale_n++;
        g_stale_bytes += sizeof(*s) + (size_t)klen + vlen;
    } else {
        free(s);
        free(kcopy);
        free(vcopy);
    }
    sf_stale_recount_locked();
    pthread_mutex_unlock(&g_sf_mu);
}

/* Serve the retained stale copy: appends the value bytes to vec and reports
 * whether the refresh-ahead window is due. Prunes expired entries it passes. */
static int sf_stale_serve(const char *key, uint32_t klen, KuttiVec *vec,
                          int *refresh_due) {
    uint64_t now = sf_now_ms();
    pthread_mutex_lock(&g_sf_mu);
    for (StaleEntry **link = &g_stale; *link;) {
        StaleEntry *s = *link;
        if (s->until_ms <= now) {
            *link = s->next;
            g_stale_bytes -= sizeof(*s) + (size_t)s->klen + s->vlen;
            free(s->key);
            free(s->val);
            free(s);
            g_stale_n--;
            continue;
        }
        if (s->klen == klen && memcmp(s->key, key, klen) == 0) {
            *refresh_due = s->refresh_ms && now >= s->written_ms + s->refresh_ms;
            int rc = 1;
            if (kuttidb_vec_reserve(vec, s->vlen) < 0) {
                rc = -1;
            } else {
                memcpy(vec->data + vec->len, s->val, s->vlen);
                vec->len += s->vlen;
            }
            pthread_mutex_unlock(&g_sf_mu);
            return rc;
        }
        link = &s->next;
    }
    sf_stale_recount_locked();
    pthread_mutex_unlock(&g_sf_mu);
    return 0;
}

/* Any successful fresh write (plain PUT, PUT_TTL, PUT_AND_RELEASE) or a
 * delete supersedes the retained stale copy. Gated on a lock-free counter
 * so the hot put path pays nothing when the registry is empty. */
static void sf_stale_drop(const char *key, uint32_t klen) {
    pthread_mutex_lock(&g_sf_mu);
    for (StaleEntry **link = &g_stale; *link; link = &(*link)->next) {
        StaleEntry *s = *link;
        if (s->klen == klen && memcmp(s->key, key, klen) == 0) {
            *link = s->next;
            g_stale_bytes -= sizeof(*s) + (size_t)s->klen + s->vlen;
            free(s->key);
            free(s->val);
            free(s);
            g_stale_n--;
            break;
        }
    }
    sf_stale_recount_locked();
    pthread_mutex_unlock(&g_sf_mu);
}

/* WAIT_FOR_KEY: answer immediately when the result already exists (the
 * loader may have finished between the client's GET_OR_CLAIM and its WAIT);
 * register a deferred waiter only when a live claim could still produce the
 * value. Everything runs under g_sf_mu so a loader finishing concurrently
 * either wakes the registered waiter or has already committed the answer.
 * Returns 0 when handled (response written or waiter registered), -1 on
 * resource refusal. */
static int sf_resolve_or_register(Loop *L, Conn *c, const char *key,
                                  uint32_t klen, uint64_t timeout_ms) {
    pthread_mutex_lock(&g_sf_mu);
    uint64_t now = sf_now_ms();
    int live_claim = 0;
    for (Claim **link = &g_claims; *link;) {
        Claim *claim = *link;
        if (claim->deadline_ms <= now && !claim->completing) { /* expired lease */
            *link = claim->next;
            free(claim->key);
            free(claim);
            continue;
        }
        if (claim->klen == klen && memcmp(claim->key, key, klen) == 0)
            live_claim = 1;
        link = &claim->next;
    }
    for (NegEntry **link = &g_negatives; *link;) {
        NegEntry *n = *link;
        if (n->deadline_ms <= now) {
            *link = n->next;
            free(n->key);
            free(n);
            g_negatives_n--;
            continue;
        }
        if (!live_claim && n->klen == klen &&
            memcmp(n->key, key, klen) == 0) {
            pthread_mutex_unlock(&g_sf_mu);
            if (kuttidb_vec_reserve(&c->out, 6) < 0) { c->eof = 1; return 0; }
            unsigned char *p = (unsigned char *)c->out.data + c->out.len;
            p[0] = 0x00;
            put_u32le(p + 1, 1);
            p[5] = SF_STATE_NEGATIVE;
            c->out.len += 6;
            return 0;
        }
        link = &n->next;
    }
    if (!live_claim) {
        /* No live claim: the answer already exists in the cache, or the
         * claim was released or expired without one. Either way the waiter
         * must not hang. */
        pthread_mutex_unlock(&g_sf_mu);
        if (kuttidb_vec_reserve(&c->out, 6) < 0) { c->eof = 1; return 0; }
        size_t pos = c->out.len;
        c->out.len += 6;
        ((unsigned char *)c->out.data)[pos] = 0x00;
        put_u32le((unsigned char *)c->out.data + pos + 1, 1);
        ((unsigned char *)c->out.data)[pos + 5] = SF_STATE_RELEASED;
        int rc = kuttidb_get_into(g_cache, key, klen, &c->out);
        if (rc == 1) {
            ((unsigned char *)c->out.data)[pos + 5] = SF_STATE_VALUE;
            put_u32le((unsigned char *)c->out.data + pos + 1,
                      (uint32_t)(c->out.len - pos - 5));
        } else {
            c->out.len = pos;
            if (kuttidb_vec_reserve(&c->out, 6) >= 0) {
                unsigned char *p = (unsigned char *)c->out.data + pos;
                p[0] = 0x00;
                put_u32le(p + 1, 1);
                p[5] = SF_STATE_RELEASED;
                c->out.len += 6;
            } else {
                c->eof = 1;
            }
        }
        return 0;
    }
    if (g_waiters_n >= (int)g_max_clients) {
        pthread_mutex_unlock(&g_sf_mu);
        return -1; /* backpressure limit */
    }
    int per_key = 0;
    for (Waiter *w = g_waiters; w; w = w->next)
        if (w->klen == klen && memcmp(w->key, key, klen) == 0)
            per_key++;
    if (per_key >= SF_WAITERS_PER_KEY_MAX) {
        pthread_mutex_unlock(&g_sf_mu);
        return -1;
    }
    Waiter *w = calloc(1, sizeof(*w));
    if (w && (w->key = malloc(klen))) {
        memcpy(w->key, key, klen);
        w->klen = klen;
        w->conn = c;
        w->loop = L;
        w->deadline_ms = now + timeout_ms;
        w->next = g_waiters;
        g_waiters = w;
        g_waiters_n++;
        atomic_fetch_add(&g_sf_waiters, 1);
        c->wait_registered = 1; /* response deferred to a wake */
        pthread_mutex_unlock(&g_sf_mu);
        return 0;
    }
    free(w);
    pthread_mutex_unlock(&g_sf_mu);
    return -1;
}

static void sf_shutdown(void) {
    pthread_mutex_lock(&g_sf_mu);
    while (g_claims) {
        Claim *c = g_claims;
        g_claims = c->next;
        free(c->key);
        free(c);
    }
    while (g_waiters) {
        Waiter *w = g_waiters;
        g_waiters = w->next;
        free(w->key);
        free(w);
    }
    while (g_negatives) {
        NegEntry *n = g_negatives;
        g_negatives = n->next;
        free(n->key);
        free(n);
    }
    pthread_mutex_unlock(&g_sf_mu);
}

static void sf_stats_counts(uint64_t *claims, uint64_t *waiters,
                            uint64_t *negatives) {
    pthread_mutex_lock(&g_sf_mu);
    uint64_t c = 0;
    for (Claim *claim = g_claims; claim; claim = claim->next) c++;
    *claims = c;
    *waiters = (uint64_t)g_waiters_n;
    *negatives = (uint64_t)g_negatives_n;
    pthread_mutex_unlock(&g_sf_mu);
}

/* Management API adapters.  They deliberately share the production
 * single-flight registry rather than inventing a parallel HTTP-only lock. */
static int admin_keyspace_claim_acquire(const char *key, uint32_t key_len,
                                        uint64_t owner, uint64_t lease_ms) {
    if (!key || !key_len || !owner || !lease_ms || lease_ms>SF_LEASE_MAX_MS) return -1;
    return sf_claim_get_or_create_owned(key,key_len,lease_ms,owner);
}

static int admin_keyspace_claim_release(const char *key, uint32_t key_len,
                                        uint64_t owner) {
    int rc=sf_claim_remove_owned(key,key_len,owner);
    if(rc==1&&atomic_load(&g_sf_waiters)>0)sf_wake_waiters(key,key_len,SF_STATE_RELEASED);
    return rc;
}

static int admin_keyspace_claim_complete(const char *key, uint32_t key_len,
                                         uint64_t owner, const char *value,
                                         uint32_t value_len, uint64_t ttl_ms,
                                         int negative) {
    int begun=sf_claim_begin_complete_owned(key,key_len,owner);
    if(begun!=1)return begun;
    int rc;
    if(negative){sf_negative_record(key,key_len,ttl_ms);rc=0;}
    else rc=persist_put(key,key_len,value,value_len,ttl_ms,0);
    int released=sf_claim_remove_owned(key,key_len,owner);
    if(rc||released!=1)return -1;
    if(atomic_load(&g_sf_waiters)>0)
        sf_wake_waiters(key,key_len,negative?SF_STATE_NEGATIVE:SF_STATE_VALUE);
    return 1;
}

static _Atomic uint64_t g_stale_serves;   /* GET_OR_REFRESH stale answers */
static _Atomic uint64_t g_refresh_serves; /* refresh-ahead-due answers */

static uint64_t sf_stale_entries(void) {
    pthread_mutex_lock(&g_sf_mu);
    uint64_t n = (uint64_t)g_stale_n;
    pthread_mutex_unlock(&g_sf_mu);
    return n;
}

/* ---- read-only inventory snapshots (QUEUE_LIST / STREAM_LIST / etc.) ----
 * Entries are appended under the owning store's lock; each response is
 * bounded at 256 entries so one request cannot produce unbounded output. */
typedef struct QListCtx { KuttiVec *out; size_t at; uint32_t n; } QListCtx;
typedef struct SListCtx { KuttiVec *out; size_t at; uint32_t n; int groups; } SListCtx;

#define LIST_MAX 256

static int list_reserve(KuttiVec *out, size_t extra) {
    return kuttidb_vec_reserve(out, extra);
}

static void qlist_cb(const char *name, uint32_t name_len,
                     uint64_t depth, uint64_t inflight, void *ud) {
    QListCtx *ctx = ud;
    if (ctx->n >= LIST_MAX || list_reserve(ctx->out, 2 + name_len + 16) < 0)
        return;
    unsigned char *p = (unsigned char *)ctx->out->data + ctx->out->len;
    put_u16le(p, name_len);
    if (name_len) memcpy(p + 2, name, name_len);
    put_u64le(p + 2 + name_len, depth);
    put_u64le(p + 10 + name_len, inflight);
    ctx->out->len += 2 + name_len + 16;
    ctx->n++;
}

static void slist_cb(const char *name, uint32_t name_len,
                     uint32_t partitions, uint64_t bytes, uint64_t records,
                     void *ud) {
    SListCtx *ctx = ud;
    if (ctx->n >= LIST_MAX || list_reserve(ctx->out, 2 + name_len + 20) < 0)
        return;
    unsigned char *p = (unsigned char *)ctx->out->data + ctx->out->len;
    put_u16le(p, name_len);
    if (name_len) memcpy(p + 2, name, name_len);
    put_u32le(p + 2 + name_len, partitions);
    put_u64le(p + 6 + name_len, records);
    put_u64le(p + 14 + name_len, bytes);
    ctx->out->len += 2 + name_len + 20;
    ctx->n++;
}

static void sgroup_cb(const char *topic, uint32_t topic_len,
                      const char *group, uint32_t group_len,
                      uint64_t generation, uint32_t members, void *ud) {
    SListCtx *ctx = ud;
    if (ctx->n >= LIST_MAX ||
        list_reserve(ctx->out, 2 + topic_len + 2 + group_len + 12) < 0)
        return;
    unsigned char *p = (unsigned char *)ctx->out->data + ctx->out->len;
    put_u16le(p, topic_len);
    if (topic_len) memcpy(p + 2, topic, topic_len);
    put_u16le(p + 2 + topic_len, group_len);
    if (group_len) memcpy(p + 4 + topic_len, group, group_len);
    put_u64le(p + 4 + topic_len + group_len, generation);
    put_u32le(p + 12 + topic_len + group_len, members);
    ctx->out->len += 4 + topic_len + group_len + 12;
    ctx->n++;
}

/* process all complete requests in the input buffer */
static void conn_process(Loop *L, Conn *c) {
    KuttiVec *in = &c->in;
    for (;;) {
        if (out_pending(c) >= OUT_FLUSH_LIMIT) {
            flush_out(L, c);
            if (c->want_write || c->eof) break; /* apply socket backpressure */
        }
        size_t avail = in->len - c->in_pos;
        if (avail && !in->data) { c->eof = 1; break; }
        char *base = in->data ? in->data + c->in_pos : NULL;

        if (c->batch_op == 0) {
            if (avail < 7) break;
            unsigned char op = (unsigned char)base[0];
            uint32_t klen = (unsigned char)base[1] |
                            ((uint32_t)(unsigned char)base[2] << 8);
            uint32_t vlen = get_u32le((unsigned char *)base + 3);

            if (!c->authenticated) {
                if (op != 0x06 || vlen != 0 || klen == 0 || klen > AUTH_MAX) {
                    atomic_fetch_add(&g_auth_failures, 1);
                    resp_status(c, 0x02);
                    c->eof = 1;
                    break;
                }
                size_t need = 7 + (size_t)klen;
                if (avail < need) break;
                if (auth_equal((unsigned char *)base + 7, klen)) {
                    c->authenticated = 1;
                    cancel_guard_timer(L, c);
                    resp_status(c, 0x00);
                } else {
                    atomic_fetch_add(&g_auth_failures, 1);
                    resp_status(c, 0x02);
                    c->eof = 1;
                }
                c->in_pos += need;
                if (c->eof) break;
                continue;
            }

            if (op == 0x06) {
                resp_status(c, 0x02);
                c->eof = 1;
                break;
            }

            if (op >= SF_GET_OR_CLAIM && op <= SF_OP_MAX) {
                /* Singleflight: the envelope is [OK][len][state:1][value].
                 * WAIT_FOR_KEY defers its response to a later wake; clients
                 * must not rely on response order across it. Payload shapes
                 * are validated per op below; PUT_AND_RELEASE carries a full
                 * cache value, so the common gate only bounds the frame. */
                if (vlen > g_max_val || !klen) { c->eof = 1; break; }
                size_t need = 7 + (size_t)klen + vlen;
                if (avail < need) break;
                const char *skey = base + 7;
                const unsigned char *arg = (const unsigned char *)skey + klen;
                if (op == SF_GET_OR_CLAIM) {
                    if (vlen != 4 || get_u32le(arg) > SF_LEASE_MAX_MS) {
                        resp_status(c, 0x02);
                        c->in_pos += need;
                        continue;
                    }
                    uint64_t lease_ms = get_u32le(arg);
                    int rc = -1;
                    if (1) {
                        if (sf_negative_check(skey, klen)) {
                            rc = -2; /* negative answer, no value */
                        } else {
                            /* Fast path: an existing entry is a plain hit
                             * wrapped in the singleflight envelope. */
                            if (kuttidb_vec_reserve(&c->out, 6) < 0) { c->eof = 1; break; }
                            size_t pos = c->out.len;
                            c->out.len += 6;
                            ((unsigned char *)c->out.data)[pos] = 0x00;
                            put_u32le((unsigned char *)c->out.data + pos + 1, 1);
                            ((unsigned char *)c->out.data)[pos + 5] =
                                SF_STATE_VALUE;
                            int hit = kuttidb_get_into(g_cache, skey, klen,
                                                     &c->out);
                            if (hit == 1) {
                                put_u32le((unsigned char *)c->out.data + pos + 1,
                                          (uint32_t)(c->out.len - pos - 5));
                                rc = 0;
                            } else {
                                c->out.len = pos; /* miss: fall through */
                                int created = sf_claim_get_or_create(
                                    skey, klen,
                                    lease_ms ? lease_ms : SF_LEASE_MAX_MS);
                                rc = created == 1 ? -3 : -4; /* claimed|wait */
                            }
                        }
                    }
                    if (rc == 0) {
                        /* envelope already written */
                    } else if (kuttidb_vec_reserve(&c->out, 6) < 0) {
                        c->eof = 1;
                    } else {
                        unsigned char *p =
                            (unsigned char *)c->out.data + c->out.len;
                        p[0] = 0x00;
                        put_u32le(p + 1, 1);
                        p[5] = rc == -2 ? SF_STATE_NEGATIVE
                             : rc == -3 ? SF_STATE_CLAIMED
                             : rc == -4 ? SF_STATE_WAIT
                             : SF_STATE_LOST;
                        c->out.len += 6;
                    }
                } else if (op == SF_WAIT_FOR_KEY) {
                    if (vlen != 4) {
                        resp_status(c, 0x02);
                    } else if (c->wait_registered) {
                        resp_status(c, 0x02); /* one pending wait per conn */
                    } else {
                        uint64_t timeout_ms = get_u32le(arg);
                        if (timeout_ms == 0 || timeout_ms > SF_WAIT_MAX_MS) {
                            resp_status(c, 0x02);
                        } else if (sf_resolve_or_register(L, c, skey, klen,
                                                          timeout_ms) < 0) {
                            resp_status(c, 0x02);
                        }
                        /* success: response is immediate or deferred */
                    }
                } else if (op == SF_PUT_AND_RELEASE) {
                    /* value: [ttl_ms:4][negative:1][value]. A negative answer
                     * is recorded only in the bounded in-memory registry: it
                     * must stay invisible to plain GETs and never occupy the
                     * cache. */
                    int rc = -1;
                    if (vlen >= 5 && arg[4] <= 1) {
                        uint64_t ttl_ms = get_u32le(arg);
                        const char *val = (const char *)arg + 5;
                        uint32_t val_len = vlen - 5;
                        if (arg[4]) {
                            sf_negative_record(skey, klen, ttl_ms);
                            sf_claim_remove(skey, klen);
                            if (atomic_load(&g_sf_waiters) > 0)
                                sf_wake_waiters(skey, klen, SF_STATE_NEGATIVE);
                            rc = 0;
                        } else {
                            rc = persist_put(skey, klen, val, val_len,
                                             ttl_ms, 0);
                            if (rc == 0) {
                                sf_claim_remove(skey, klen);
                                if (atomic_load(&g_sf_waiters) > 0)
                                    sf_wake_waiters(skey, klen,
                                                    SF_STATE_VALUE);
                            }
                        }
                    }
                    resp_status(c, rc == 0 ? 0x00 : 0x02);
                } else if (op == SF_RELEASE_CLAIM) {
                    if (vlen != 0) {
                        resp_status(c, 0x02);
                    } else {
                        sf_claim_remove(skey, klen);
                        if (atomic_load(&g_sf_waiters) > 0)
                            sf_wake_waiters(skey, klen, SF_STATE_RELEASED);
                        resp_status(c, 0x00);
                    }
                } else { /* SF_GET_OR_REFRESH: stale-while-revalidate read */
                    if (vlen != 4 || get_u32le(arg) > SF_LEASE_MAX_MS) {
                        resp_status(c, 0x02);
                        c->in_pos += need;
                        continue;
                    }
                    uint64_t lease_ms = get_u32le(arg);
                    /* Envelope: [OK][len][state:1][holder:1][value]. holder=1
                     * tells the caller it now owns the revalidation lease and
                     * must reload (states stale/refresh); holder=0 means
                     * another caller is already revalidating, so the value
                     * can be used as-is. */
                    int negative = sf_negative_check(skey, klen);
                    int holder = 0;
                    int answered = 0;
                    /* Fast path: a fresh value answers immediately, exactly
                     * like GET_OR_CLAIM. Refresh-ahead patches the state and
                     * may hand this caller the revalidation lease. */
                    if (!negative && kuttidb_vec_reserve(&c->out, 7) >= 0) {
                        size_t pos = c->out.len;
                        c->out.len += 7;
                        unsigned char *p0 =
                            (unsigned char *)c->out.data + pos;
                        p0[0] = 0x00;
                        p0[5] = SF_STATE_VALUE;
                        p0[6] = 0;
                        if (kuttidb_get_into(g_cache, skey, klen, &c->out) == 1) {
                            answered = 1;
                            int refresh_due = 0;
                            if (atomic_load(&g_sf_stale) > 0) {
                                pthread_mutex_lock(&g_sf_mu);
                                uint64_t now = sf_now_ms();
                                for (StaleEntry *s = g_stale; s; s = s->next) {
                                    if (s->klen == klen &&
                                        memcmp(s->key, skey, klen) == 0) {
                                        refresh_due = s->refresh_ms &&
                                            now >= s->written_ms + s->refresh_ms;
                                        break;
                                    }
                                }
                                pthread_mutex_unlock(&g_sf_mu);
                            }
                            if (refresh_due) {
                                p0[5] = SF_STATE_REFRESH;
                                atomic_fetch_add(&g_refresh_serves, 1);
                                holder = sf_claim_get_or_create(skey, klen,
                                        lease_ms ? lease_ms : SF_LEASE_MAX_MS) == 1;
                            }
                            put_u32le(p0 + 1,
                                      (uint32_t)(c->out.len - pos - 5));
                            p0[6] = (unsigned char)holder;
                        } else {
                            c->out.len = pos; /* miss: fall through */
                        }
                    }
                    if (!answered) {
                        /* Miss: serve the retained stale copy if one exists
                         * (SWR), otherwise fall back to claim/wait. */
                        int served = 0;
                        size_t pos = c->out.len;
                        if (!negative && atomic_load(&g_sf_stale) > 0 &&
                            kuttidb_vec_reserve(&c->out, 7) >= 0) {
                            int refresh_due = 0;
                            c->out.len += 7; /* envelope header placeholder */
                            served = sf_stale_serve(skey, klen, &c->out,
                                                    &refresh_due);
                            if (served == 1) {
                                unsigned char *p =
                                    (unsigned char *)c->out.data + pos;
                                p[0] = 0x00;
                                p[5] = SF_STATE_STALE;
                                atomic_fetch_add(&g_stale_serves, 1);
                                /* exactly one revalidator gets the lease;
                                 * concurrent readers keep receiving the
                                 * stale copy without waiting */
                                holder = sf_claim_get_or_create(skey, klen,
                                        lease_ms ? lease_ms : SF_LEASE_MAX_MS) == 1;
                                put_u32le(p + 1,
                                          (uint32_t)(c->out.len - pos - 5));
                                p[6] = (unsigned char)holder;
                            } else {
                                c->out.len = pos; /* no stale copy */
                                if (served < 0) { c->eof = 1; break; }
                            }
                        }
                        if (served != 1) {
                            int rc2 = sf_claim_get_or_create(skey, klen,
                                    lease_ms ? lease_ms : SF_LEASE_MAX_MS);
                            if (kuttidb_vec_reserve(&c->out, 7) < 0) {
                                c->eof = 1;
                            } else {
                                unsigned char *p =
                                    (unsigned char *)c->out.data + c->out.len;
                                p[0] = 0x00;
                                put_u32le(p + 1, 2);
                                p[5] = negative ? SF_STATE_NEGATIVE
                                     : rc2 == 1 ? SF_STATE_CLAIMED
                                     : SF_STATE_WAIT;
                                p[6] = 0;
                                c->out.len += 7;
                            }
                        }
                    }
                }
                c->in_pos += need;
                continue;
            }

            if (op >= ATOMIC_PUT_PUBLISH && op <= ATOMIC_UPDATE_EMIT) {
                if (vlen > g_max_val || !klen) { c->eof = 1; break; }
                size_t need = 7 + (size_t)klen + vlen;
                if (avail < need) break;
                const char *ckey = base + 7;
                const unsigned char *arg = (const unsigned char *)ckey + klen;
                const char *target = NULL, *rkey = NULL, *msg = NULL;
                uint32_t target_len = 0, rklen = 0, msg_len = 0;
                uint64_t ttl_ms = 0;
                int parsed = 0;
                if (op == ATOMIC_PUT_ENQUEUE) {
                    /* [qlen:2][queue][ttl_ms:4][kuttidb_value] */
                    if (vlen >= 6) {
                        uint32_t qlen = get_u16le(arg);
                        if (qlen && qlen <= QUEUE_NAME_MAX &&
                            6 + (size_t)qlen <= vlen) {
                            target = (const char *)arg + 2;
                            target_len = qlen;
                            ttl_ms = get_u32le(arg + 2 + qlen);
                            msg = (const char *)arg + 6 + qlen;
                            msg_len = vlen - 6 - qlen;
                            parsed = 1;
                        }
                    }
                } else {
                    /* publish: [xlen:2][exchange][rklen:2][rkey]
                     *   followed by [ttl_ms:4][value] (0x40, 0x43)
                     *   or          [mlen:4][message] (0x42) */
                    if (vlen >= 4) {
                        uint32_t xlen = get_u16le(arg);
                        if (xlen <= EXCHANGE_NAME_MAX &&
                            4 + (size_t)xlen <= vlen) {
                            target = (const char *)arg + 2;
                            target_len = xlen;
                            const unsigned char *p = arg + 2 + xlen;
                            uint32_t rklen2 = get_u16le(p);
                            size_t rest = vlen - xlen - 2;
                            if (rklen2 <= ROUTING_KEY_MAX &&
                                2 + (size_t)rklen2 + 4 <= rest) {
                                rkey = (const char *)p + 2;
                                rklen = rklen2;
                                const unsigned char *tail = p + 2 + rklen2;
                                size_t expected = rest - 2 - rklen2 - 4;
                                if (op == ATOMIC_PUT_PUBLISH ||
                                    op == ATOMIC_UPDATE_EMIT) {
                                    ttl_ms = get_u32le(tail);
                                    msg = (const char *)tail + 4;
                                    msg_len = (uint32_t)expected;
                                    parsed = 1;
                                } else {
                                    uint32_t declared = get_u32le(tail);
                                    if (declared == expected) {
                                        msg = (const char *)tail + 4;
                                        msg_len = declared;
                                        parsed = 1;
                                    }
                                }
                                if (parsed && msg_len > g_max_val) parsed = 0;
                            }
                        }
                    }
                }
                if (!parsed) resp_status(c, 0x02);
                else {
                    uint64_t tx_id = 0, routed = 0;
                    int rc = atomic_execute(op, ckey, klen, target, target_len,
                                            rkey, rklen, msg, msg_len, ttl_ms,
                                            &tx_id, &routed,
                                            op == ATOMIC_UPDATE_EMIT ? 1 : 0);
                    if (rc < 0) resp_status(c, 0x02);
                    else if (rc == 0) {
                        if (kuttidb_vec_reserve(&c->out, 17) < 0) { c->eof = 1; break; }
                        unsigned char *p =
                            (unsigned char *)c->out.data + c->out.len;
                        p[0] = 0x00;
                        put_u32le(p + 1, 12);
                        put_u64le(p + 5, tx_id);
                        put_u32le(p + 13, (uint32_t)routed);
                        c->out.len += 17;
                    } else {
                        resp_status(c, 0x01); /* unroutable: nothing committed */
                    }
                }
                c->in_pos += need;
                continue;
            }

            if (op == STREAM_LIST || op == STREAM_GROUP_LIST) {
                if (klen != 0 || vlen != 0) { resp_status(c, 0x02); c->in_pos += 7; continue; }
                if (kuttidb_vec_reserve(&c->out, 7) < 0) { c->eof = 1; break; }
                size_t pos = c->out.len;
                c->out.len += 7;
                ((unsigned char *)c->out.data)[pos] = 0x00;
                put_u32le((unsigned char *)c->out.data + pos + 1, 2);
                put_u16le((unsigned char *)c->out.data + pos + 5, 0);
                SListCtx sctx = { &c->out, pos + 7, 0, op == STREAM_GROUP_LIST };
                if (sctx.groups) stream_group_foreach_stats(g_streams, sgroup_cb, &sctx);
                else stream_foreach_stats(g_streams, slist_cb, &sctx);
                put_u16le((unsigned char *)c->out.data + pos + 5, (uint16_t)sctx.n);
                put_u32le((unsigned char *)c->out.data + pos + 1,
                          (uint32_t)(c->out.len - pos - 5));
                c->in_pos += 7;
                continue;
            }

            if (op >= STREAM_DECLARE && op <= STREAM_OP_MAX) {
                if (!klen || klen > STREAM_NAME_MAX ||
                    (op == STREAM_APPEND_BATCH ? vlen > g_max_batch_bytes : vlen > g_max_val)) {
                    c->eof = 1; break;
                }
                size_t need = 7 + (size_t)klen + vlen;
                if (avail < need) break;
                const char *topic = base + 7;
                const unsigned char *arg = (const unsigned char *)topic + klen;
                if (op == STREAM_DECLARE) {
                    int rc = vlen == 20 ? stream_declare(g_streams, topic, klen,
                        get_u32le(arg), get_u64le(arg + 4), get_u64le(arg + 12)) : -1;
                    resp_status(c, rc == 0 ? 0x00 : 0x02);
                } else if (op == STREAM_APPEND) {
                    uint64_t partition = 0, offset = 0;
                    int rc = -1;
                    if (vlen >= 6) {
                        uint16_t key_len = get_u16le(arg + 4);
                        if (vlen >= 6u + key_len)
                            rc = stream_append(g_streams, topic, klen, get_u32le(arg),
                                arg + 6, key_len, arg + 6 + key_len,
                                vlen - 6 - key_len, &partition, &offset);
                    }
                    resp_stream_append(c, rc, partition, offset);
                } else if (op == STREAM_APPEND_BATCH) {
                    StreamAppendInput *inputs = NULL;
                    StreamAppendResult *results = NULL;
                    uint32_t count = 0;
                    int rc = -1;
                    if (vlen >= 8) {
                        count = get_u32le(arg + 4);
                        if (count && count <= STREAM_FETCH_MAX) {
                            inputs = calloc(count, sizeof *inputs);
                            results = calloc(count, sizeof *results);
                            size_t at = 8;
                            if (inputs && results) {
                                rc = 0;
                                for (uint32_t i = 0; i < count; i++) {
                                    if (at + 6 > vlen) { rc = -1; break; }
                                    uint16_t key_len = get_u16le(arg + at);
                                    uint32_t value_len = get_u32le(arg + at + 2);
                                    at += 6;
                                    if (value_len > g_max_val || key_len > vlen - at ||
                                        value_len > vlen - at - key_len) { rc = -1; break; }
                                    inputs[i].key = arg + at;
                                    inputs[i].key_len = key_len;
                                    inputs[i].data = arg + at + key_len;
                                    inputs[i].len = value_len;
                                    at += key_len + value_len;
                                }
                                if (rc == 0 && at == vlen)
                                    rc = stream_append_batch(g_streams, topic, klen,
                                                             get_u32le(arg), inputs, count, results);
                                else rc = -1;
                            }
                        }
                    }
                    resp_stream_append_batch(c, rc, results, count);
                    free(results);
                    free(inputs);
                } else if (op == STREAM_FETCH || op == STREAM_FETCH_KEYS) {
                    StreamRecordView *records = NULL; uint32_t count = 0;
                    int rc = vlen == 16 ? stream_fetch(g_streams, topic, klen,
                        get_u32le(arg), get_u64le(arg + 4), get_u32le(arg + 12),
                        g_max_batch_bytes, &records, &count) : -1;
                    if (op == STREAM_FETCH_KEYS) resp_stream_fetch_keys(c, rc, records, count);
                    else resp_stream_fetch(c, rc, records, count);
                    stream_fetch_free(records, count);
                } else if (op == STREAM_COMMIT) {
                    int rc = -1;
                    if (vlen >= 14) {
                        uint16_t group_len = get_u16le(arg);
                        if (group_len && group_len <= STREAM_GROUP_MAX && vlen == 14u + group_len)
                            rc = stream_commit_for_owner(g_streams, topic, klen, (const char *)arg + 2,
                                group_len, get_u32le(arg + 2 + group_len),
                                get_u64le(arg + 6 + group_len), c->id);
                    }
                    resp_status(c, rc == 0 ? 0x00 : 0x02);
                } else if (op == STREAM_COMMIT_BATCH) {
                    /* value = [glen:2][group][count:4] (
                     *   count * [partition:4][offset:8] ) */
                    int rc = -1;
                    if (vlen >= 6) {
                        uint16_t group_len = get_u16le(arg);
                        uint32_t count = vlen >= 6u + group_len ? get_u32le(arg + 2 + group_len) : 0;
                        if (group_len && group_len <= STREAM_GROUP_MAX &&
                            count >= 1 && count <= PROTO_BATCH_MAX &&
                            vlen == 6u + group_len + 12u * count) {
                            StreamCommitInput inputs[PROTO_BATCH_MAX];
                            size_t at = 6u + group_len;
                            for (uint32_t i = 0; i < count; i++) {
                                inputs[i].partition = get_u32le(arg + at);
                                inputs[i].offset = get_u64le(arg + at + 4);
                                at += 12;
                            }
                            rc = stream_commit_batch_for_owner(g_streams, topic, klen,
                                (const char *)arg + 2, group_len, c->id, inputs, count);
                        }
                    }
                    resp_status(c, rc == 0 ? 0x00 : 0x02);
                } else if (op == STREAM_GROUP_JOIN) {
                    uint32_t *parts = NULL, count = 0; uint64_t generation = 0; int rc = -1;
                    if (vlen >= 6) {
                        uint16_t group_len = get_u16le(arg);
                        if (group_len && group_len <= STREAM_GROUP_MAX && vlen == 6u + group_len)
                            rc = stream_group_join(g_streams, topic, klen,
                                (const char *)arg + 2, group_len, c->id,
                                get_u32le(arg + 2 + group_len), &parts, &count,
                                &generation);
                    }
                    resp_stream_assignment(c, rc, parts, count, generation);
                    free(parts);
                } else if (op == STREAM_GROUP_LEAVE) {
                    /* Graceful drain-and-leave: releases this connection's
                     * membership in one group so the others rebalance now. */
                    int rc = -1;
                    if (vlen >= 2) {
                        uint16_t group_len = get_u16le(arg);
                        if (group_len && group_len <= STREAM_GROUP_MAX && vlen == 2u + group_len)
                            rc = stream_group_leave_member(g_streams, topic, klen,
                                (const char *)arg + 2, group_len, c->id);
                    }
                    resp_status(c, rc == 0 ? 0x00 : 0x02);
                } else if (op == STREAM_GROUP_LAG) {
                    uint64_t lag = 0; int rc = -1;
                    if (vlen >= 6) {
                        uint16_t group_len = get_u16le(arg);
                        if (group_len && group_len <= STREAM_GROUP_MAX && vlen == 6u + group_len)
                            rc = stream_group_lag(g_streams, topic, klen,
                                (const char *)arg + 2, group_len,
                                get_u32le(arg + 2 + group_len), &lag);
                    }
                    if (rc < 0) resp_status(c, 0x02);
                    else if (rc == 0) resp_status(c, 0x01);
                    else resp_queue_id(c, 0x00, lag);
                } else {
                    uint64_t offset = 0; int rc = -1;
                    if (vlen >= 6) {
                        uint16_t group_len = get_u16le(arg);
                        if (group_len && group_len <= STREAM_GROUP_MAX && vlen == 6u + group_len)
                            rc = stream_group_offset(g_streams, topic, klen,
                                (const char *)arg + 2, group_len,
                                get_u32le(arg + 2 + group_len), &offset);
                    }
                    if (rc < 0) resp_status(c, 0x02);
                    else if (rc == 0) resp_status(c, 0x01);
                    else resp_queue_id(c, 0x00, offset);
                }
                c->in_pos += need;
                continue;
            }

            if (op >= EXCHANGE_DECLARE && op <= EXCHANGE_PUBLISH) {
                if (op == EXCHANGE_PUBLISH
                    ? (vlen < 10 || vlen - 10 > g_max_val)
                    : vlen > g_max_val) { c->eof = 1; break; }
                size_t need = 7 + (size_t)klen + vlen;
                if (avail < need) break;
                const char *exchange = base + 7;
                const unsigned char *arg = (const unsigned char *)exchange + klen;
                if (op == EXCHANGE_DECLARE) {
                    /* value: [durable:1][type:1] optionally followed by
                     * [ext_len:2][ext] where ext carries the alternate
                     * exchange: [ae_len:2][ae_name]. ext_len == 0 clears it. */
                    int rc = -1;
                    if (vlen == 2 && arg[0] <= 1 && arg[1] <= EXCHANGE_TOPIC) {
                        rc = exchange_declare(g_queues, exchange, klen, arg[0],
                                              arg[1], NULL, 0);
                    } else if (vlen >= 4 && arg[0] <= 1 && arg[1] <= EXCHANGE_TOPIC) {
                        uint32_t ext_len = get_u16le(arg + 2);
                        if (vlen == 4 + (size_t)ext_len && ext_len == 0) {
                            rc = exchange_declare(g_queues, exchange, klen,
                                                  arg[0], arg[1], NULL, 0);
                        } else if (vlen == 4 + (size_t)ext_len &&
                                   ext_len >= 2 && ext_len <= 2 + EXCHANGE_NAME_MAX) {
                            uint32_t ae_len = get_u16le(arg + 4);
                            if (ext_len == 2 + ae_len)
                                rc = exchange_declare(g_queues, exchange, klen,
                                                      arg[0], arg[1],
                                                      (const char *)arg + 6,
                                                      ae_len);
                        }
                    }
                    resp_status(c, rc == 0 ? 0x00 : 0x02);
                } else if (op == EXCHANGE_BIND) {
                    /* value: [queue_len:2][queue][key_len:2][key] */
                    int rc = -1;
                    if (vlen >= 4) {
                        uint32_t q_len = get_u16le(arg);
                        if (q_len && q_len <= QUEUE_NAME_MAX &&
                            4 + (size_t)q_len <= vlen) {
                            uint32_t key_len = get_u16le(arg + 2 + q_len);
                            if (key_len <= ROUTING_KEY_MAX &&
                                vlen == 4 + (size_t)q_len + key_len)
                                rc = exchange_bind(g_queues, exchange, klen,
                                                   (const char *)arg + 2, q_len,
                                                   (const char *)arg + 4 + q_len,
                                                   key_len);
                        }
                    }
                    resp_status(c, rc == 0 ? 0x00 : 0x02);
                } else if (op == EXCHANGE_UNBIND) {
                    int rc = -1;
                    if (vlen >= 4) {
                        uint32_t q_len = get_u16le(arg);
                        if (q_len && q_len <= QUEUE_NAME_MAX &&
                            4 + (size_t)q_len <= vlen) {
                            uint32_t key_len = get_u16le(arg + 2 + q_len);
                            if (key_len <= ROUTING_KEY_MAX &&
                                vlen == 4 + (size_t)q_len + key_len)
                                rc = exchange_unbind(g_queues, exchange, klen,
                                                     (const char *)arg + 2, q_len,
                                                     (const char *)arg + 4 + q_len,
                                                     key_len);
                        }
                    }
                    resp_status(c, rc < 0 ? 0x02 : (rc ? 0x00 : 0x01));
                } else { /* EXCHANGE_PUBLISH */
                    /* value: [key_len:2][ttl_ms:8][key][message]. An empty
                     * exchange name routes via the default exchange. */
                    uint64_t routed = 0;
                    int rc = -1;
                    if (vlen >= 10) {
                        uint32_t key_len = get_u16le(arg);
                        if (key_len <= ROUTING_KEY_MAX &&
                            10 + (size_t)key_len <= vlen) {
                            rc = exchange_publish(g_queues, exchange, klen,
                                                  (const char *)arg + 10, key_len,
                                                  arg + 10 + key_len,
                                                  vlen - 10 - key_len,
                                                  get_u64le(arg + 2), &routed);
                        }
                    }
                    if (rc < 0) {
                        resp_status(c, 0x02);
                    } else if (routed == 0) {
                        resp_status(c, 0x01); /* unroutable */
                    } else {
                        if (kuttidb_vec_reserve(&c->out, 9) < 0) { c->eof = 1; break; }
                        unsigned char *p =
                            (unsigned char *)c->out.data + c->out.len;
                        p[0] = 0x00;
                        put_u32le(p + 1, 4);
                        put_u32le(p + 5, (uint32_t)routed);
                        c->out.len += 9;
                    }
                }
                c->in_pos += need;
                continue;
            }

            if (op == QUEUE_LIST) {
                if (klen != 0 || vlen != 0) { resp_status(c, 0x02); c->in_pos += 7; continue; }
                if (kuttidb_vec_reserve(&c->out, 7) < 0) { c->eof = 1; break; }
                size_t pos = c->out.len;
                c->out.len += 7;
                ((unsigned char *)c->out.data)[pos] = 0x00;
                put_u32le((unsigned char *)c->out.data + pos + 1, 2);
                put_u16le((unsigned char *)c->out.data + pos + 5, 0);
                QListCtx qctx = { &c->out, pos + 7, 0 };
                queue_foreach_stats(g_queues, qlist_cb, &qctx);
                put_u16le((unsigned char *)c->out.data + pos + 5, (uint16_t)qctx.n);
                put_u32le((unsigned char *)c->out.data + pos + 1,
                          (uint32_t)(c->out.len - pos - 5));
                c->in_pos += 7;
                continue;
            }

            if (op >= QUEUE_DECLARE && op <= QUEUE_OP_MAX) {
                if (!klen || (op == QUEUE_PUBLISH_TTL
                              ? (vlen < 8 || vlen - 8 > g_max_val)
                              : vlen > g_max_val)) { c->eof = 1; break; }
                size_t need = 7 + (size_t)klen + vlen;
                if (avail < need) break;
                const char *queue = base + 7;
                const unsigned char *arg = (const unsigned char *)queue + klen;
                if (op == QUEUE_DECLARE) {
                    /* Legacy value: [durable:1][max_depth:8]. Extended value
                     * appends [ext_len:2][ext] where ext carries the
                     * dead-letter policy: [dlq_len:2][dlq_name]
                     * [max_deliveries:4]. ext_len == 0 clears the policy. */
                    int rc = -1;
                    if (vlen == 9 && arg[0] <= 1) {
                        rc = queue_declare(g_queues, queue, klen, arg[0],
                                           get_u64le(arg + 1));
                    } else if (vlen >= 11 && arg[0] <= 1) {
                        uint32_t ext_len = get_u16le(arg + 9);
                        if (vlen == 11 + (size_t)ext_len && ext_len == 0) {
                            rc = queue_declare_ex(g_queues, queue, klen, arg[0],
                                                  get_u64le(arg + 1), NULL, 0, 0);
                        } else if (vlen == 11 + (size_t)ext_len && ext_len >= 6 &&
                                   ext_len <= 6 + QUEUE_NAME_MAX) {
                            uint32_t dlq_len = get_u16le(arg + 11);
                            if (dlq_len && ext_len == 6 + dlq_len)
                                rc = queue_declare_ex(g_queues, queue, klen,
                                                      arg[0], get_u64le(arg + 1),
                                                      (const char *)arg + 13,
                                                      dlq_len,
                                                      get_u32le(arg + 13 + dlq_len));
                        }
                    }
                    resp_status(c, rc == 0 ? 0x00 : 0x02);
                } else if (op == QUEUE_PUBLISH) {
                    uint64_t id = 0;
                    int rc = queue_publish(g_queues, queue, klen, arg, vlen, 0, &id);
                    resp_queue_id(c, rc == 0 ? 0x00 : 0x02, id);
                } else if (op == QUEUE_PUBLISH_TTL) {
                    uint64_t id = 0;
                    int rc = vlen >= 8 ? queue_publish(g_queues, queue, klen, arg + 8,
                                                        vlen - 8, get_u64le(arg), &id) : -1;
                    resp_queue_id(c, rc == 0 ? 0x00 : 0x02, id);
                } else if (op == QUEUE_STATS) {
                    uint64_t depth = 0, inflight = 0;
                    resp_queue_stats(c, vlen == 0
                        ? queue_stats(g_queues, queue, klen, &depth, &inflight) : -1,
                        depth, inflight);
                } else if (op == QUEUE_PREFETCH) {
                    if (vlen != 4) resp_status(c, 0x02);
                    else { c->queue_prefetch = get_u32le(arg); resp_status(c, 0x00); }
                } else if (op == QUEUE_CANCEL) {
                    if (vlen != 0) resp_status(c, 0x02);
                    else { queue_requeue_owner(g_queues, c->id); resp_status(c, 0x00); }
                } else if (op == QUEUE_CONSUMER_REGISTER) {
                    /* Durable named consumer: key = consumer name, no value.
                     * Registers (or heartbeats) and binds this connection to
                     * the consumer's stable owner token. */
                    if (vlen != 0) resp_status(c, 0x02);
                    else {
                        uint64_t owner = 0;
                        int rc = queue_consumer_register(g_queues, queue, klen, &owner);
                        if (rc == 0) c->queue_consumer_owner = owner;
                        resp_queue_id(c, rc == 0 ? 0x00 : 0x02, owner);
                    }
                } else if (op == QUEUE_CONSUMER_UNREGISTER) {
                    if (vlen != 0) resp_status(c, 0x02);
                    else {
                        int rc = queue_consumer_unregister(g_queues, queue, klen);
                        if (rc == 1) c->queue_consumer_owner = 0;
                        resp_status(c, rc < 0 ? 0x02 : (rc ? 0x00 : 0x01));
                    }
                } else if (op == QUEUE_CONSUME_AS) {
                    /* key = queue, value = [consumer_len:2][consumer][visibility:8].
                     * The delivery uses the consumer's stable owner token, so
                     * the per-connection prefetch limit applies to that
                     * token's in-flight deliveries. */
                    QueueMessage message;
                    int rc = -1;
                    uint64_t owner = 0;
                    if (vlen >= 10) {
                        uint16_t clen = get_u16le(arg);
                        if (clen && vlen == 10u + clen)
                            rc = queue_consumer_lookup(g_queues,
                                (const char *)arg + 2, clen, &owner);
                    }
                    if (rc == 0) resp_status(c, 0x01); /* unknown consumer */
                    else if (rc < 0) resp_status(c, 0x02);
                    else if (c->queue_prefetch &&
                             queue_owner_inflight(g_queues, owner) >= c->queue_prefetch) {
                        resp_status(c, 0x01);
                    } else {
                        uint16_t clen = get_u16le(arg);
                        rc = queue_consume_for_owner(g_queues, queue, klen,
                            get_u64le(arg + 2 + clen), owner, &message);
                        c->queue_consumer_owner = owner;
                        resp_queue_message(c, rc, &message);
                        if (rc > 0) queue_message_free(&message);
                    }
                } else if (op == QUEUE_CONSUME) {
                    if (vlen != 8) {
                        resp_status(c, 0x02);
                    } else if (c->queue_prefetch &&
                               queue_owner_inflight(g_queues, c->id) >= c->queue_prefetch) {
                        resp_status(c, 0x01);
                    } else {
                        QueueMessage message;
                        int rc = queue_consume_for_owner(g_queues, queue, klen,
                                                         get_u64le(arg), c->id, &message);
                        resp_queue_message(c, rc, &message);
                        if (rc > 0) queue_message_free(&message);
                    }
                } else if (op == QUEUE_ACK) {
                    int rc = -1;
                    uint64_t tag = vlen == 8 ? get_u64le(arg) : 0;
                    if (vlen == 8) {
                        rc = queue_ack_for_owner(g_queues, queue, klen, tag, c->id);
                        if (rc == 0 && c->queue_consumer_owner)
                            rc = queue_ack_for_owner(g_queues, queue, klen, tag,
                                                     c->queue_consumer_owner);
                    }
                    resp_status(c, rc < 0 ? 0x02 : (rc ? 0x00 : 0x01));
                } else if (op == QUEUE_NACK) {
                    int rc = -1;
                    if (vlen == 9 && arg[8] <= 1) {
                        rc = queue_nack_for_owner(g_queues, queue, klen,
                                                  get_u64le(arg), c->id, arg[8]);
                        if (rc == 0 && c->queue_consumer_owner)
                            rc = queue_nack_for_owner(g_queues, queue, klen,
                                                      get_u64le(arg),
                                                      c->queue_consumer_owner, arg[8]);
                    }
                    else if (vlen == 17 && arg[8] <= 1)
                        rc = queue_nack_for_owner_delay(g_queues, queue, klen,
                                                        get_u64le(arg), c->id, arg[8],
                                                        get_u64le(arg + 9));
                    resp_status(c, rc < 0 ? 0x02 : (rc ? 0x00 : 0x01));
                } else if (op == QUEUE_PUBLISH_BATCH) {
                    /* value = [count:4] ( count * [len:4][message] ) */
                    int rc = -1;
                    uint32_t count = vlen >= 4 ? get_u32le(arg) : 0;
                    if (count >= 1 && count <= PROTO_BATCH_MAX) {
                        const void *data[PROTO_BATCH_MAX];
                        uint32_t lens[PROTO_BATCH_MAX];
                        uint64_t ids[PROTO_BATCH_MAX];
                        size_t at = 4;
                        uint32_t i = 0;
                        for (; i < count; i++) {
                            if (at + 4 > vlen) break;
                            uint32_t mlen = get_u32le(arg + at);
                            at += 4;
                            if (mlen > vlen - at) break;
                            data[i] = arg + at;
                            lens[i] = mlen;
                            at += mlen;
                        }
                        if (i == count && at == vlen)
                            rc = queue_publish_batch(g_queues, queue, klen, count,
                                                     data, lens, ids);
                        if (rc == 0 && kuttidb_vec_reserve(&c->out, 8 + 8 * count) == 0) {
                            unsigned char *p = (unsigned char *)c->out.data + c->out.len;
                            p[0] = 0x00;
                            put_u32le(p + 1, 4 + 8 * count);
                            put_u32le(p + 5, count);
                            for (i = 0; i < count; i++) put_u64le(p + 9 + 8 * i, ids[i]);
                            c->out.len += 9 + 8 * count;
                        } else if (rc != 0) {
                            resp_status(c, 0x02);
                        }
                    } else {
                        resp_status(c, 0x02);
                    }
                } else if (op == QUEUE_CONSUME_BATCH) {
                    /* value = [max:4]; response [OK][n:4] (
                     *   n * [tag:8][id:8][dcount:4][redelivered:1][len:4][data] ) */
                    uint32_t max = vlen == 4 ? get_u32le(arg) : 0;
                    if (max < 1 || max > PROTO_BATCH_MAX) {
                        resp_status(c, 0x02);
                    } else if (c->queue_prefetch &&
                               queue_owner_inflight(g_queues, c->id) >= c->queue_prefetch) {
                        resp_status(c, 0x01);
                    } else {
                        uint32_t want = max;
                        if (c->queue_prefetch &&
                            want > c->queue_prefetch - queue_owner_inflight(g_queues, c->id))
                            want = c->queue_prefetch - queue_owner_inflight(g_queues, c->id);
                        QueueMessage messages[PROTO_BATCH_MAX];
                        uint32_t n = 0;
                        int rc = want ? queue_consume_batch(g_queues, queue, klen, 0,
                                                            c->id, want, messages, &n) : 0;
                        if (rc < 0) {
                            resp_status(c, 0x02);
                        } else {
                            size_t need = 9;
                            for (uint32_t i = 0; i < n; i++) need += 25 + messages[i].len;
                            if (kuttidb_vec_reserve(&c->out, need) == 0) {
                                unsigned char *p = (unsigned char *)c->out.data + c->out.len;
                                p[0] = 0x00;
                                put_u32le(p + 1, (uint32_t)(need - 5));
                                put_u32le(p + 5, n);
                                size_t at = 9;
                                for (uint32_t i = 0; i < n; i++) {
                                    put_u64le(p + at, messages[i].delivery_tag);
                                    put_u64le(p + at + 8, messages[i].id);
                                    put_u32le(p + at + 16, messages[i].delivery_count);
                                    p[at + 20] = messages[i].redelivered ? 1 : 0;
                                    put_u32le(p + at + 21, messages[i].len);
                                    if (messages[i].len)
                                        memcpy(p + at + 25, messages[i].data, messages[i].len);
                                    at += 25 + messages[i].len;
                                }
                                c->out.len += need;
                            } else {
                                c->eof = 1;
                            }
                        }
                        for (uint32_t i = 0; i < n; i++) queue_message_free(&messages[i]);
                    }
                } else if (op == QUEUE_ACK_BATCH) {
                    /* value = [mode:1][count:4] ( count * [tag:8] );
                     * mode 0 = ACK, 1 = NACK requeue, 2 = NACK discard/DLQ. */
                    int rc = -1;
                    uint32_t count = vlen >= 5 ? get_u32le(arg + 1) : 0;
                    unsigned char mode = vlen >= 1 ? arg[0] : 0xff;
                    if (mode <= 2 && count >= 1 && count <= PROTO_BATCH_MAX &&
                        vlen == 5u + 8u * count) {
                        uint64_t tags[PROTO_BATCH_MAX];
                        for (uint32_t i = 0; i < count; i++)
                            tags[i] = get_u64le(arg + 5 + 8 * i);
                        uint32_t acked = 0;
                        if (mode == 0)
                            rc = queue_ack_batch(g_queues, queue, klen, c->id,
                                                 c->queue_consumer_owner, tags,
                                                 count, &acked);
                        else
                            rc = queue_nack_batch(g_queues, queue, klen, c->id,
                                                  c->queue_consumer_owner, tags,
                                                  count, mode == 1, &acked);
                        if (rc == 0 && kuttidb_vec_reserve(&c->out, 9) == 0) {
                            unsigned char *p = (unsigned char *)c->out.data + c->out.len;
                            p[0] = 0x00;
                            put_u32le(p + 1, 4);
                            put_u32le(p + 5, acked);
                            c->out.len += 9;
                        } else if (rc != 0) {
                            resp_status(c, 0x02);
                        }
                    } else {
                        resp_status(c, 0x02);
                    }
                } else {
                    resp_status(c, 0x02);
                }
                c->in_pos += need;
                continue;
            }

            if (op == 0x11 || op == 0x12 || op == 0x13) {
                if (klen != 0 || vlen > MAX_BATCH) { c->eof = 1; break; }
                c->batch_op = op;
                c->batch_left = vlen;
                c->batch_err = 0;
                c->batch_bytes = 0;
                c->batch_out_bytes = 0;
                c->batch_now = op == 0x12 ? (uint32_t)time(NULL) : 0;
                c->stage = 0;
                c->in_pos += 7;
                if (op == 0x12) {
                    if (kuttidb_vec_reserve(&c->out, 4) < 0) { c->eof = 1; break; }
                    put_u32le((unsigned char *)c->out.data + c->out.len, vlen);
                    c->out.len += 4;
                    c->batch_out_bytes = 4;
                }
                if (vlen == 0) {
                    if (op != 0x12) {
                        if (kuttidb_vec_reserve(&c->out, 1) < 0) { c->eof = 1; break; }
                        c->out.data[c->out.len++] = 0x00;
                    }
                    c->batch_op = 0;
                }
                continue;
            }

            if (op == 0x05) { /* PUT with TTL */
                if (vlen > g_max_val) { c->eof = 1; break; }
                size_t need = 11 + (size_t)klen + vlen;
                if (avail < need) break;
                uint64_t ttl_ms = get_u32le((unsigned char *)base + 7);
                const char *key = base + 11;
                const char *val = base + 11 + klen;
                int rc = persist_put(key, klen, val, vlen, ttl_ms, 0);
                resp_status(c, rc == 0 ? 0x00 : 0x02);
                c->in_pos += need;
                continue;
            }

            if (op == PUT_SWR) { /* PUT with stale-while-revalidate window */
                if (vlen > g_max_val) { c->eof = 1; break; }
                size_t need = 19 + (size_t)klen + vlen;
                if (avail < need) break;
                const unsigned char *arg = (const unsigned char *)base + 7;
                uint64_t ttl_ms = get_u32le(arg);
                uint64_t stale_ms = get_u32le(arg + 4);
                uint64_t refresh_ms = get_u32le(arg + 8);
                const char *key = base + 19;
                const char *val = base + 19 + klen;
                /* SWR is meaningless without an expiry; both windows are
                 * capped so registry deadlines stay bounded. */
                if (!ttl_ms || !stale_ms ||
                    stale_ms > SF_STALE_WINDOW_MAX_MS ||
                    refresh_ms > SF_STALE_WINDOW_MAX_MS) {
                    resp_status(c, 0x02);
                } else {
                    int rc = persist_put(key, klen, val, vlen, ttl_ms, 0);
                    if (rc == 0)
                        sf_stale_store(key, klen, val, vlen, ttl_ms,
                                       stale_ms, refresh_ms);
                    resp_status(c, rc == 0 ? 0x00 : 0x02);
                }
                c->in_pos += need;
                continue;
            }

            if (op != 0x01 && op != 0x02 && op != 0x03 && op != 0x04 &&
                op != HEALTH && op != CAPABILITIES && op != SERVER_INFO) {
                resp_status(c, 0x02);
                c->eof = 1;
                break;
            }
            if (vlen > g_max_val ||
                ((op == 0x02 || op == 0x03) && vlen != 0) ||
                ((op == 0x04 || op == HEALTH || op == SERVER_INFO) && (klen != 0 || vlen != 0)) ||
                (op == CAPABILITIES && (klen != 0 || vlen != 4))) {
                c->eof = 1;
                break;
            }
            size_t need = 7 + (size_t)klen +
                ((op == 0x01 || op == CAPABILITIES) ? vlen : 0);
            if (avail < need) break;
            const char *key = base + 7;
            const char *val = base + 7 + klen;

            if (op == 0x01) { /* PUT */
                int rc = persist_put(key, klen, val, vlen, 0, 0);
                resp_status(c, rc == 0 ? 0x00 : 0x02);
            } else if (op == 0x02) { /* GET */
                resp_get(c, key, klen);
            } else if (op == 0x03) { /* DELETE */
                int found = persist_delete(key, klen);
                resp_status(c, found < 0 ? 0x02 : (found ? 0x00 : 0x01));
            } else if (op == HEALTH) {
                /* Health is intentionally authenticated like every other
                 * administrative operation. It reports ready only while all
                 * configured persistence engines remain writable. */
                int failed = atomic_load(&g_wal_failed) ||
                    queue_persistence_failed(g_queues) ||
                    stream_persistence_failed(g_streams);
                resp_status(c, failed ? 0x02 : 0x00);
            } else if (op == CAPABILITIES) {
                if (get_u16le((const unsigned char *)val) != PROTOCOL_MAJOR)
                    resp_status(c, 0x01);
                else
                    resp_capabilities(c);
            } else if (op == SERVER_INFO) {
                /* Listener identity is enough for the public transport kind:
                 * the optional Unix listener is only registered on loop 0. */
                resp_server_info(c, c->is_unix ? 1 : 0);
            } else if (op == 0x04) { /* STATS */
                char st[2048];
                int n = snprintf(st, sizeof st,
                    "{\"count\":%zu,\"mem_bytes\":%zu,\"allocated_bytes\":%zu,"
                    "\"wal_offset\":%llu,\"wal_failed\":%d,"
                    "\"expired\":%llu,\"evicted\":%llu,\"connections\":%u,"
                    "\"rejected_connections\":%llu,\"auth_failures\":%llu,"
                    "\"event_loops\":%d,\"event_backend\":\"%s\","
                    "\"durability\":\"%s\",\"queues\":%llu,"
                    "\"queue_depth\":%llu,\"queue_inflight\":%llu,"
                    "\"queue_redeliveries\":%llu,\"queue_wal_failed\":%d,"
                    "\"queue_consumers\":%llu,"
                    "\"queue_deadlettered\":%llu,\"exchanges\":%llu,"
                    "\"exchange_bindings\":%llu,\"exchange_unroutable\":%llu,"
                    "\"stream_topics\":%llu,\"stream_partitions\":%llu,"
                    "\"stream_retention_bytes\":%llu,\"stream_groups\":%llu,"
                    "\"stream_group_members\":%llu,\"stream_wal_failed\":%d",
                    kuttidb_count(g_cache), kuttidb_memusage(g_cache), kuttidb_allocated(g_cache),
                    (unsigned long long)g_wal_offset,
                    atomic_load(&g_wal_failed),
                    kuttidb_expired_count(g_cache),
                    kuttidb_evicted_count(g_cache), atomic_load(&g_connections),
                    atomic_load(&g_rejected_connections), atomic_load(&g_auth_failures),
                    g_nloops, platform_event_backend(),
                    g_durability == DUR_ALWAYS ? "always" : "periodic",
                    (unsigned long long)queue_count(g_queues),
                    (unsigned long long)queue_total_depth(g_queues),
                    (unsigned long long)queue_total_inflight(g_queues),
                    (unsigned long long)queue_redeliveries(g_queues),
                    queue_persistence_failed(g_queues),
                    (unsigned long long)queue_consumer_count(g_queues),
                    (unsigned long long)queue_deadlettered(g_queues),
                    (unsigned long long)exchange_count(g_queues),
                    (unsigned long long)exchange_binding_count(g_queues),
                    (unsigned long long)exchange_unroutable(g_queues),
                    (unsigned long long)stream_topic_count(g_streams),
                    (unsigned long long)stream_partition_count(g_streams),
                    (unsigned long long)stream_retention_bytes(g_streams),
                    (unsigned long long)stream_group_count(g_streams),
                    (unsigned long long)stream_group_member_count(g_streams),
                    stream_persistence_failed(g_streams));
                uint64_t sf_claims = 0, sf_waiters = 0, sf_neg = 0;
                sf_stats_counts(&sf_claims, &sf_waiters, &sf_neg);
                char identity_suffix[64] = "";
                if (g_has_instance_id)
                    snprintf(identity_suffix, sizeof identity_suffix,
                             ",\"instance_id\":\"%s\"", g_instance_id);
                n += snprintf(st + n, sizeof st - (size_t)n,
                              ",\"claims\":%llu,\"singleflight_waiters\":%llu,"
                              "\"negatives\":%llu,\"stale_entries\":%llu,"
                              "\"stale_serves\":%llu,\"refresh_serves\":%llu,"
                              "\"lifecycle\":\"%s\",\"lifecycle_state\":\"%s\","
                              "\"managed_connections\":%u,\"managed_idle_remaining_ms\":%llu%s}",
                              (unsigned long long)sf_claims,
                              (unsigned long long)sf_waiters,
                              (unsigned long long)sf_neg,
                              (unsigned long long)sf_stale_entries(),
                              (unsigned long long)atomic_load(&g_stale_serves),
                              (unsigned long long)atomic_load(&g_refresh_serves),
                              managed_lifecycle_name(g_lifecycle_mode),
                              managed_lifecycle_is_stopping(&g_lifecycle) ? "stopping" : "running",
                              managed_lifecycle_connections(&g_lifecycle),
                              (unsigned long long)managed_lifecycle_deadline_remaining_ms(&g_lifecycle),
                              identity_suffix);
                if (n < 0) { c->eof = 1; break; }
                if (n >= (int)sizeof st) n = (int)sizeof st - 1;
                if (kuttidb_vec_reserve(&c->out, 5 + (size_t)n) < 0) { c->eof = 1; break; }
                unsigned char *p = (unsigned char *)c->out.data + c->out.len;
                p[0] = 0x00;
                put_u32le(p + 1, (uint32_t)n);
                memcpy(p + 5, st, (size_t)n);
                c->out.len += 5 + (size_t)n;
            } else {
                resp_status(c, 0x02);
            }

            c->in_pos += need;
            continue;
        }

        if (c->batch_op == 0x11 || c->batch_op == 0x13) { /* PUT_BATCH(/TTL) */
            int with_ttl = c->batch_op == 0x13;
            if (c->stage == 0) {
                size_t ih = with_ttl ? 10 : 6;
                if (avail < ih) break;
                c->cur_klen = (unsigned char)base[0] |
                              ((uint32_t)(unsigned char)base[1] << 8);
                c->cur_vlen = get_u32le((unsigned char *)base + 2);
                c->cur_ttl = with_ttl ? get_u32le((unsigned char *)base + 6) : 0;
                unsigned long long item_bytes = ih +
                    (unsigned long long)c->cur_klen + c->cur_vlen;
                if (c->cur_vlen > g_max_val ||
                    item_bytes > g_max_batch_bytes - c->batch_bytes) {
                    c->eof = 1;
                    break;
                }
                c->batch_bytes += item_bytes;
                c->in_pos += ih;
                c->stage = 1;
                continue;
            }
            size_t need = (size_t)c->cur_klen + c->cur_vlen;
            if (avail < need) break;
            int rc;
            if (with_ttl) {
                rc = persist_put(base, c->cur_klen,
                                 base + c->cur_klen, c->cur_vlen,
                                 c->cur_ttl, 1);
            } else {
                rc = persist_put(base, c->cur_klen,
                                 base + c->cur_klen, c->cur_vlen, 0, 1);
            }
            if (rc != 0)
                c->batch_err = 1;
            c->in_pos += need;
            c->stage = 0;
            if (--c->batch_left == 0) {
                if (persist_batch_commit() < 0) c->batch_err = 1;
                if (kuttidb_vec_reserve(&c->out, 1) < 0) { c->eof = 1; break; }
                c->out.data[c->out.len++] = c->batch_err ? 0x02 : 0x00;
                c->batch_op = 0;
            }
            continue;
        }

        /* GET_BATCH */
        if (c->stage == 0) {
            if (avail < 2) break;
            c->cur_klen = (unsigned char)base[0] |
                          ((uint32_t)(unsigned char)base[1] << 8);
            if ((unsigned long long)c->cur_klen + 2 >
                g_max_batch_bytes - c->batch_bytes) { c->eof = 1; break; }
            c->batch_bytes += (unsigned long long)c->cur_klen + 2;
            c->in_pos += 2;
            c->stage = 1;
            continue;
        }
        if (avail < c->cur_klen) break;
        size_t out_before = c->out.len;
        resp_get_at(c, base, c->cur_klen, c->batch_now);
        size_t added = c->out.len - out_before;
        if ((unsigned long long)added > g_max_batch_bytes - c->batch_out_bytes) {
            c->eof = 1;
            break;
        }
        c->batch_out_bytes += added;
        c->in_pos += c->cur_klen;
        c->stage = 0;
        if (--c->batch_left == 0)
            c->batch_op = 0;
        continue;
    }

    /* keep memory bounded on huge batches: opportunistically drain */
    if (c->in_pos == in->len) {
        c->in_pos = 0;
        in->len = 0;
        vec_shrink_idle(in, 256u << 10);
    } else if (c->in_pos) {
        size_t remain = in->len - c->in_pos;
        if (!in->data) { c->eof = 1; return; }
        memmove(in->data, in->data + c->in_pos, remain);
        in->len = remain;
        c->in_pos = 0;
    }

    while (out_pending(c) >= OUT_FLUSH_LIMIT && !c->want_write)
        flush_out(L, c);
    if (out_pending(c) > g_max_batch_bytes + g_max_val + OUT_FLUSH_LIMIT)
        c->eof = 1;
}

static void close_conn(Loop *L, Conn *c) {
    if (c->wait_registered) sf_waiter_detach(L, c);
    queue_requeue_owner(g_queues, c->id);
    stream_group_leave_owner(g_streams, c->id);
    platform_timer_destroy(&L->poller, &c->guard);
    if (c->want_read || c->want_write)
        platform_watch_remove(&L->poller, c->fd);
#ifdef HAVE_OPENSSL
    if (c->tls) SSL_free(c->tls);
#endif
    close(c->fd);
    free(c->in.data);
    free(c->out.data);
    c->dead = 1;
    if (c->lifecycle_lease) managed_lifecycle_connection_close(&g_lifecycle);
    atomic_fetch_sub(&g_connections, 1);
    c->next_dead = L->garbage;
    L->garbage = c;
}

static void set_write_filter(Loop *L, Conn *c, int on) {
    if (platform_watch_update(&L->poller, c->fd, c, c->want_read, on) == 0)
        c->want_write = on;
    else
        c->eof = 1;
}

static void set_read_filter(Loop *L, Conn *c, int on) {
    if (platform_watch_update(&L->poller, c->fd, c, on, c->want_write) == 0)
        c->want_read = on;
    else
        c->eof = 1;
}

static void cancel_guard_timer(Loop *L, Conn *c) {
    if (!c->guard.active || !c->authenticated || !c->tls_ready) return;
    if (platform_timer_cancel(&L->poller, &c->guard) < 0)
        c->eof = 1;
}

static void update_request_guard(Loop *L, Conn *c) {
    if (!c->authenticated || !c->tls_ready) return;
    if (c->in.len == 0 && c->batch_op == 0) {
        cancel_guard_timer(L, c);
        return;
    }
    if (platform_timer_set(&L->poller, &c->guard, c->fd, c,
                           GUARD_TIMEOUT_SECONDS * 1000u) < 0)
        c->eof = 1;
}

#ifdef HAVE_OPENSSL
static int tls_handshake(Loop *L, Conn *c) {
    if (!c->tls || c->tls_ready) return 1;
    ERR_clear_error();
    int rc = SSL_accept(c->tls);
    if (rc == 1) {
        c->tls_ready = 1;
        cancel_guard_timer(L, c);
        if (c->want_write) set_write_filter(L, c, 0);
        if (!c->want_read) set_read_filter(L, c, 1);
        return 1;
    }
    int err = SSL_get_error(c->tls, rc);
    if (err == SSL_ERROR_WANT_READ) {
        if (c->want_write) set_write_filter(L, c, 0);
        if (!c->want_read) set_read_filter(L, c, 1);
        return 0;
    }
    if (err == SSL_ERROR_WANT_WRITE) {
        if (!c->want_write) set_write_filter(L, c, 1);
        return 0;
    }
    atomic_fetch_add(&g_auth_failures, 1);
    c->eof = 1;
    return -1;
}
#endif

static void flush_out(Loop *L, Conn *c) {
    while (out_pending(c)) {
        size_t pending = out_pending(c);
        char *data = c->out.data + c->out_pos;
        ssize_t r;
#ifdef HAVE_OPENSSL
        int tls_err = SSL_ERROR_NONE;
        if (c->tls) {
            if (!c->tls_ready && tls_handshake(L, c) != 1) return;
            size_t want = pending > INT_MAX ? INT_MAX : pending;
            ERR_clear_error();
            int n = SSL_write(c->tls, data, (int)want);
            r = n;
            if (n <= 0) tls_err = SSL_get_error(c->tls, n);
        } else
#endif
        {
            r = send(c->fd, data, pending, MSG_NOSIGNAL);
        }
        if (r > 0) {
            c->out_pos += (size_t)r;
            continue;
        }
#ifdef HAVE_OPENSSL
        if (c->tls && tls_err == SSL_ERROR_WANT_WRITE) {
            if (!c->want_write) set_write_filter(L, c, 1);
            if (c->want_read) set_read_filter(L, c, 0);
            return;
        }
        if (c->tls && tls_err == SSL_ERROR_WANT_READ) {
            if (c->want_write) set_write_filter(L, c, 0);
            if (!c->want_read) set_read_filter(L, c, 1);
            return;
        }
        if (c->tls) { c->eof = 1; return; }
#endif
        if (r < 0 && errno == EINTR) continue;
        if (r < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            if (!c->want_write) set_write_filter(L, c, 1);
            if (c->want_read) set_read_filter(L, c, 0);
            return;
        }
        c->eof = 1;
        return;
    }
    c->out_pos = 0;
    c->out.len = 0;
    vec_shrink_idle(&c->out, 256u << 10);
    if (c->want_write) set_write_filter(L, c, 0);
    if (!c->want_read) set_read_filter(L, c, 1);
}

static void on_readable(Loop *L, Conn *c) {
#ifdef HAVE_OPENSSL
    if (c->tls && !c->tls_ready && tls_handshake(L, c) != 1) return;
#endif
    char buf[65536];
    for (;;) {
        ssize_t r;
#ifdef HAVE_OPENSSL
        int tls_err = SSL_ERROR_NONE;
        if (c->tls) {
            ERR_clear_error();
            int n = SSL_read(c->tls, buf, (int)sizeof buf);
            r = n;
            if (n <= 0) tls_err = SSL_get_error(c->tls, n);
        } else
#endif
        {
            r = recv(c->fd, buf, sizeof buf, 0);
        }
        if (r > 0) {
            if (kuttidb_vec_reserve(&c->in, (size_t)r) < 0) { c->eof = 1; break; }
            memcpy(c->in.data + c->in.len, buf, (size_t)r);
            c->in.len += (size_t)r;
            if (c->in.len - c->in_pos >
                (size_t)g_max_val + MAX_KEY + sizeof buf + 11) {
                c->eof = 1;
                break;
            }
            conn_process(L, c);
            update_request_guard(L, c);
            if (c->eof || c->want_write) break;
#ifdef HAVE_OPENSSL
            if (c->tls) {
                if (SSL_pending(c->tls) == 0) break;
            } else
#endif
            if (r < (ssize_t)sizeof buf) break;
            continue;
        }
#ifdef HAVE_OPENSSL
        if (c->tls && tls_err == SSL_ERROR_WANT_READ) break;
        if (c->tls && tls_err == SSL_ERROR_WANT_WRITE) {
            if (!c->want_write) set_write_filter(L, c, 1);
            break;
        }
        if (c->tls && tls_err == SSL_ERROR_ZERO_RETURN) { c->eof = 1; break; }
        if (c->tls) { c->eof = 1; break; }
#endif
        if (r == 0) { c->eof = 1; break; }
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
        c->eof = 1;
        break;
    }
    if (c->in.len && !c->eof && !c->want_write) conn_process(L, c);
    if (!c->eof) update_request_guard(L, c);
    flush_out(L, c);
}

static void on_readable(Loop *L, Conn *c);

/* accept all pending connections on a per-loop listener */
static void on_acceptable(Loop *L, int lfd) {
    for (;;) {
        int cfd = accept(lfd, NULL, NULL);
        if (cfd < 0) {
            if (errno == EINTR) continue;
            return; /* EAGAIN etc */
        }
        unsigned int prior = atomic_fetch_add(&g_connections, 1);
        if (prior >= g_max_clients) {
            atomic_fetch_sub(&g_connections, 1);
            atomic_fetch_add(&g_rejected_connections, 1);
            close(cfd);
            continue;
        }
        fcntl(cfd, F_SETFL, O_NONBLOCK);
        fcntl(cfd, F_SETFD, FD_CLOEXEC);
        int one = 1;
        setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
        setsockopt(cfd, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof(one));

        Conn *c = calloc(1, sizeof(Conn));
        if (!c) { atomic_fetch_sub(&g_connections, 1); close(cfd); continue; }
        c->fd = cfd;
        c->is_unix = lfd != L->listen_fd;
        c->id = atomic_fetch_add_explicit(&g_next_connection_id, 1, memory_order_relaxed);
        if (!c->id) c->id = atomic_fetch_add_explicit(&g_next_connection_id, 1,
                                                       memory_order_relaxed);
        c->guard.fd = -1;
        c->authenticated = g_auth_len == 0;
        c->tls_ready = 1;
#ifdef HAVE_OPENSSL
        if (g_tls_ctx && lfd == L->listen_fd) {
            c->tls = SSL_new(g_tls_ctx);
            if (!c->tls || SSL_set_fd(c->tls, cfd) != 1) {
                if (c->tls) SSL_free(c->tls);
                atomic_fetch_sub(&g_connections, 1);
                close(cfd);
                free(c);
                continue;
            }
            SSL_set_accept_state(c->tls);
            c->tls_ready = 0;
        }
#endif

        if (!managed_lifecycle_connection_open(&g_lifecycle)) {
#ifdef HAVE_OPENSSL
            if (c->tls) SSL_free(c->tls);
#endif
            atomic_fetch_sub(&g_connections, 1);
            atomic_fetch_add(&g_rejected_connections, 1);
            close(cfd);
            free(c);
            continue;
        }
        c->lifecycle_lease = 1;
        if (platform_watch_add(&L->poller, cfd, c, 1, 0) < 0) {
            close_conn(L, c);
        } else {
            c->want_read = 1;
            if (!c->authenticated || !c->tls_ready) {
                if (platform_timer_set(&L->poller, &c->guard, cfd, c,
                                       GUARD_TIMEOUT_SECONDS * 1000u) < 0)
                    c->eof = 1;
            }
        }
    }
}

static void *loop_main(void *arg) {
    Loop *L = arg;
    PlatformEvent evs[256];
    for (;;) {
        int n = platform_wait(&L->poller, evs, 256, -1);
        if (n < 0) {
            if (errno == EINTR) continue;
            break;
        }
        int stopping = 0;
        int accepts = 0;
        /* Pass 1: connection events. Accepts are deferred to pass 2 so a
         * closing connection's fd is released before the kernel can hand
         * the same number to a new connection; otherwise a stale queued
         * event for the old connection would close the new one's socket. */
        for (int i = 0; i < n; i++) {
            PlatformEvent *e = &evs[i];
            if (e->flags & PLATFORM_EVENT_NOTIFY) {
                /* Cross-thread singleflight wakeups: responses are built and
                 * flushed here, on the waiter's own loop. */
                sf_drain_wakes(L);
                continue;
            }
            if (e->flags & PLATFORM_EVENT_WAKE) { stopping = 1; break; }
            if (e->userdata == NULL) {       /* our listener: pass 2 */
                if (e->flags & PLATFORM_EVENT_READ) accepts = 1;
                continue;
            }
            Conn *c = e->userdata;
            if (!c || c->dead) continue;

            if (e->flags & PLATFORM_EVENT_TIMER) {
                c->guard.active = 0;
                c->eof = 1;
            } else if (e->flags & PLATFORM_EVENT_WRITE) {
#ifdef HAVE_OPENSSL
                if (c->tls && !c->tls_ready) {
                    tls_handshake(L, c);
                } else
#endif
                flush_out(L, c);
                if (!c->want_write && c->in.len && !c->eof) {
                    conn_process(L, c);
                    flush_out(L, c);
                }
            } else if ((e->flags & PLATFORM_EVENT_HANGUP) &&
                       !(e->flags & PLATFORM_EVENT_READ) &&
                       c->in.len == 0 && c->out.len == 0) {
                /* Bare hangup with nothing pending: peer vanished before
                 * sending anything. When READ is also set the socket may
                 * still hold a request (macOS can report EV_EOF alongside
                 * it), so the read path must run first and handle the EOF
                 * itself; closing here would reset a valid request. */
                close_conn(L, c);
                continue;
            } else {
                on_readable(L, c);
            }

            /* close at most once per conn, after all work on it is done */
            if (!c->dead && c->eof) {
                if (c->out.len > 0) flush_out(L, c); /* best effort drain */
                if (!c->dead) close_conn(L, c);
            }
        }
        /* Pass 2: accept pending connections (fd numbers now safe to reuse). */
        if (accepts && !stopping)
            for (int i = 0; i < n; i++) {
                PlatformEvent *e = &evs[i];
                if (e->userdata == NULL &&
                    (e->flags & (PLATFORM_EVENT_NOTIFY | PLATFORM_EVENT_WAKE |
                                 PLATFORM_EVENT_TIMER)) == 0 &&
                    (e->flags & PLATFORM_EVENT_READ))
                    on_acceptable(L, e->fd);
            }
        while (L->garbage) {
            Conn *dead = L->garbage;
            L->garbage = dead->next_dead;
            free(dead);
        }
        if (stopping) return NULL;
    }
    return NULL;
}

/* ---------------- server ---------------- */

static int make_tcp_listener(const char *bind_addr, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return -1; }
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    if (inet_pton(AF_INET, bind_addr, &addr.sin_addr) != 1) {
        fprintf(stderr, "invalid IPv4 bind address: %s\n", bind_addr);
        close(fd);
        return -1;
    }
    addr.sin_port = htons(port);
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) { perror("bind"); close(fd); return -1; }
    if (listen(fd, 1024) < 0) { perror("listen"); close(fd); return -1; }
    fcntl(fd, F_SETFL, O_NONBLOCK); /* required: accept loop drains until EAGAIN */
    fcntl(fd, F_SETFD, FD_CLOEXEC);
    return fd;
}

/* optional local fast path: unix domain socket (added to loop 0) */
static int make_unix_listener(const char *path) {
    struct stat st;
    if (lstat(path, &st) == 0) {
        if (!S_ISSOCK(st.st_mode) || st.st_uid != geteuid()) {
            fprintf(stderr, "refusing to replace non-socket or foreign unix path: %s\n", path);
            return -1;
        }
        if (unlink(path) < 0) { perror("unix unlink"); return -1; }
    } else if (errno != ENOENT) {
        perror("unix lstat");
        return -1;
    }
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) { perror("unix socket"); return -1; }
    struct sockaddr_un addr = {0};
    addr.sun_family = AF_UNIX;
    if (strlen(path) >= sizeof(addr.sun_path)) { fprintf(stderr, "unix path too long\n"); close(fd); return -1; }
    strcpy(addr.sun_path, path);
    if (bind(fd, (struct sockaddr *)&addr, sizeof addr) < 0) { perror("unix bind"); close(fd); return -1; }
    if (chmod(path, 0600) < 0) { perror("unix chmod"); close(fd); unlink(path); return -1; }
    if (listen(fd, 1024) < 0) { perror("unix listen"); close(fd); return -1; }
    fcntl(fd, F_SETFL, O_NONBLOCK);
    fcntl(fd, F_SETFD, FD_CLOEXEC);
    return fd;
}

static int parse_ull(const char *s, unsigned long long *out) {
    char *end = NULL;
    errno = 0;
    unsigned long long v = strtoull(s, &end, 10);
    if (errno || !s[0] || !end || *end) return -1;
    *out = v;
    return 0;
}

static int absolute_lock_path(const char *path, char *out, size_t out_size) {
    if (!path || !*path || strcmp(path, "-") == 0) return -1;
    if (path[0] == '/') {
        if (snprintf(out, out_size, "%s", path) >= (int)out_size) return -1;
        return 0;
    }
    char cwd[PATH_MAX];
    if (!getcwd(cwd, sizeof cwd) || snprintf(out, out_size, "%s/%s", cwd, path) >= (int)out_size)
        return -1;
    return 0;
}

static int acquire_server_ownership(const char *data_dir, const char *unix_path,
                                    const char *bind, int port, const char *wal,
                                    const char *queue_path, const char *stream_path,
                                    const char *embed_path) {
    char endpoint[PATH_MAX], paths_storage[6][PATH_MAX];
    const char *paths[6]; size_t count = 0;
    if (unix_path) {
        char canonical[PATH_MAX];
        if (absolute_lock_path(unix_path, canonical, sizeof canonical) < 0 ||
            snprintf(endpoint, sizeof endpoint, "%s.server.lock", canonical) >= (int)sizeof endpoint)
            return -1;
    } else if (instance_tcp_lock_path(bind, port, endpoint, sizeof endpoint) < 0) {
        return -1;
    }
    paths[count++] = endpoint;
    const char *storage[] = {wal, queue_path, stream_path, embed_path};
    for (size_t i = 0; i < sizeof storage / sizeof storage[0]; i++) {
        if (!storage[i] || strcmp(storage[i], "-") == 0) continue;
        char canonical[PATH_MAX];
        if (absolute_lock_path(storage[i], canonical, sizeof canonical) < 0 ||
            snprintf(paths_storage[count], sizeof paths_storage[count], "%s.server.lock", canonical) >= (int)sizeof paths_storage[count])
            return -1;
        paths[count] = paths_storage[count]; count++;
    }
    if (data_dir) {
        if (snprintf(paths_storage[count], sizeof paths_storage[count], "%s/.server.lock", data_dir) >= (int)sizeof paths_storage[count]) return -1;
        paths[count] = paths_storage[count]; count++;
    }
    char endpoint_metadata[PATH_MAX + 8];
    snprintf(endpoint_metadata, sizeof endpoint_metadata, "%s%s", unix_path ? "unix:" : "tcp:", unix_path ? unix_path : endpoint);
    return instance_locks_acquire(&g_instance_locks, paths, count,
                                  g_has_instance_id ? g_instance_id : "", endpoint_metadata);
}

static int is_loopback_addr(const char *s) {
    struct in_addr a;
    if (inet_pton(AF_INET, s, &a) != 1) return 0;
    return (ntohl(a.s_addr) >> 24) == 127;
}

static void admin_status(void *unused, AdminHttpStatus *out) {
    (void)unused;
    memset(out, 0, sizeof *out);
    time_t now = time(NULL);
    out->uptime_seconds = now > g_started_at ? (uint64_t)(now - g_started_at) : 0;
    out->connections = atomic_load(&g_connections);
    out->rejected_connections = atomic_load(&g_rejected_connections);
    out->auth_failures = atomic_load(&g_admin_auth_failures);
    out->keyspace_entries = kuttidb_count(g_cache);
    out->keyspace_live_bytes = kuttidb_memusage(g_cache);
    out->keyspace_allocated_bytes = kuttidb_allocated(g_cache);
    out->keyspace_expired = kuttidb_expired_count(g_cache);
    out->keyspace_evicted = kuttidb_evicted_count(g_cache);
    out->queue_count = queue_count(g_queues);
    out->queue_ready = queue_total_depth(g_queues);
    out->queue_inflight = queue_total_inflight(g_queues);
    out->queue_redeliveries = queue_redeliveries(g_queues);
    out->queue_deadletters = queue_deadlettered(g_queues);
    out->stream_count = stream_topic_count(g_streams);
    out->stream_partitions = stream_partition_count(g_streams);
    out->stream_retained_bytes = stream_retention_bytes(g_streams);
    out->stream_groups = stream_group_count(g_streams);
    out->stream_members = stream_group_member_count(g_streams);
    out->keyspace_persistence_failed = atomic_load(&g_wal_failed);
    out->queue_persistence_failed = queue_persistence_failed(g_queues);
    out->stream_persistence_failed = stream_persistence_failed(g_streams);
    out->ready = !(out->keyspace_persistence_failed || out->queue_persistence_failed || out->stream_persistence_failed);
    out->persistence_enabled = g_persist_enabled;
#ifdef HAVE_OPENSSL
    out->tls_available = 1;
#endif
    out->event_loops = g_nloops;
    out->event_backend = platform_event_backend();
    out->durability = g_durability == DUR_ALWAYS ? "always" : "periodic";
}
static void admin_auth_failure(void *unused) { (void)unused; atomic_fetch_add(&g_admin_auth_failures, 1); }

/* ---- Prometheus scrape endpoint ---------------------------------------- */
/* Deliberately minimal HTTP/1.x: one request per connection, bounded request
 * size, bounded socket timeouts, a single sequential handler thread. The
 * endpoint is disabled unless --metrics-bind is provided; a non-loopback
 * bind requires --metrics-token-file, mirroring the main listener policy. */

struct mbuf { char *p; size_t cap; size_t len; };

static void mput(struct mbuf *m, const char *fmt, ...) {
    if (m->len >= m->cap) return;
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(m->p + m->len, m->cap - m->len, fmt, ap);
    va_end(ap);
    if (n < 0) return;
    size_t room = m->cap - m->len;
    m->len += (size_t)n < room ? (size_t)n : room;
}

/* Labeled metric families: bounded entry counts keep a scrape's output size
 * deterministic; exceeding a cap sets a `*_truncated` gauge instead of
 * emitting a partial line. */
#define METRICS_LABELED_MAX 256

struct metrics_iter { struct mbuf *m; int n; int truncated; };

/* Label values are restricted to printable ASCII; anything else becomes an
 * underscore, so a binary queue or topic name cannot corrupt the exposition. */
static void label_escape(struct mbuf *m, const char *s, uint32_t len) {
    uint32_t out = 0;
    for (uint32_t i = 0; i < len && out < 200; i++, out++) {
        unsigned char c = (unsigned char)s[i];
        if (c == '\\' || c == '"') mput(m, "\\%c", c);
        else if (c == '\n') mput(m, "\\n");
        else if (c >= 32 && c < 127) mput(m, "%c", c);
        else mput(m, "_");
    }
}

static void queue_metric_cb(const char *name, uint32_t name_len,
                            uint64_t depth, uint64_t inflight, void *ud) {
    struct metrics_iter *ctx = ud;
    if (ctx->n >= METRICS_LABELED_MAX) { ctx->truncated = 1; return; }
    ctx->n++;
    mput(ctx->m, "kuttidb_queue_depth{name=\"");
    label_escape(ctx->m, name, name_len);
    mput(ctx->m, "\"} %llu\n", (unsigned long long)depth);
    mput(ctx->m, "kuttidb_queue_inflight{name=\"");
    label_escape(ctx->m, name, name_len);
    mput(ctx->m, "\"} %llu\n", (unsigned long long)inflight);
}

static void stream_metric_cb(const char *name, uint32_t name_len,
                             uint32_t partitions, uint64_t bytes,
                             uint64_t records, void *ud) {
    struct metrics_iter *ctx = ud;
    if (ctx->n >= METRICS_LABELED_MAX) { ctx->truncated = 1; return; }
    ctx->n++;
    mput(ctx->m, "kuttidb_topic_partitions{topic=\"");
    label_escape(ctx->m, name, name_len);
    mput(ctx->m, "\"} %u\n", partitions);
    mput(ctx->m, "kuttidb_topic_retained_bytes{topic=\"");
    label_escape(ctx->m, name, name_len);
    mput(ctx->m, "\"} %llu\n", (unsigned long long)bytes);
    mput(ctx->m, "kuttidb_topic_records{topic=\"");
    label_escape(ctx->m, name, name_len);
    mput(ctx->m, "\"} %llu\n", (unsigned long long)records);
}

/* A point-in-time snapshot; counters are read independently, so a scrape may
 * straddle a mutation. That is the same skew every pull exporter accepts.
 * Returns the number of bytes written (always NUL-terminated). */
static size_t metrics_render(char *buf, size_t cap) {
    struct mbuf m = {buf, cap, 0};
    int ready = !(atomic_load(&g_wal_failed) ||
                  queue_persistence_failed(g_queues) ||
                  stream_persistence_failed(g_streams));
    mput(&m,
         "# HELP kuttidb_up Server process is serving.\n"
         "# TYPE kuttidb_up gauge\n"
         "kuttidb_up 1\n"
         "# HELP kuttidb_ready All configured persistence engines writable (HEALTH contract).\n"
         "# TYPE kuttidb_ready gauge\n"
         "kuttidb_ready %d\n", ready);
    mput(&m,
         "# HELP kuttidb_managed_lifecycle_info Managed lifecycle mode and state.\n"
         "# TYPE kuttidb_managed_lifecycle_info gauge\n"
         "kuttidb_managed_lifecycle_info{mode=\"%s\",state=\"%s\"} 1\n"
         "# HELP kuttidb_managed_connections Native lifecycle leases.\n"
         "# TYPE kuttidb_managed_connections gauge\n"
         "kuttidb_managed_connections %u\n"
         "# HELP kuttidb_managed_idle_remaining_ms Remaining idle or orphan deadline.\n"
         "# TYPE kuttidb_managed_idle_remaining_ms gauge\n"
         "kuttidb_managed_idle_remaining_ms %llu\n",
         managed_lifecycle_name(g_lifecycle_mode),
         managed_lifecycle_is_stopping(&g_lifecycle) ? "stopping" : "running",
         managed_lifecycle_connections(&g_lifecycle),
         (unsigned long long)managed_lifecycle_deadline_remaining_ms(&g_lifecycle));
    mput(&m,
         "# HELP kuttidb_cache_entries Live cache records.\n"
         "# TYPE kuttidb_cache_entries gauge\n"
         "kuttidb_cache_entries %zu\n", kuttidb_count(g_cache));
    mput(&m,
         "# HELP kuttidb_cache_mem_bytes Bytes of live cache value data.\n"
         "# TYPE kuttidb_cache_mem_bytes gauge\n"
         "kuttidb_cache_mem_bytes %zu\n", kuttidb_memusage(g_cache));
    mput(&m,
         "# HELP kuttidb_cache_allocated_bytes Bytes allocated for cache storage.\n"
         "# TYPE kuttidb_cache_allocated_bytes gauge\n"
         "kuttidb_cache_allocated_bytes %zu\n", kuttidb_allocated(g_cache));
    mput(&m,
         "# HELP kuttidb_cache_wal_offset_bytes Cache WAL byte offset.\n"
         "# TYPE kuttidb_cache_wal_offset_bytes gauge\n"
         "kuttidb_cache_wal_offset_bytes %llu\n", (unsigned long long)g_wal_offset);
    mput(&m,
         "# HELP kuttidb_cache_wal_failed Whether the cache WAL entered its fail-closed state.\n"
         "# TYPE kuttidb_cache_wal_failed gauge\n"
         "kuttidb_cache_wal_failed %d\n", atomic_load(&g_wal_failed));
    mput(&m,
         "# HELP kuttidb_cache_expired_total Cache records removed by TTL expiry.\n"
         "# TYPE kuttidb_cache_expired_total counter\n"
         "kuttidb_cache_expired_total %llu\n", (unsigned long long)kuttidb_expired_count(g_cache));
    mput(&m,
         "# HELP kuttidb_cache_evicted_total Cache records removed by eviction.\n"
         "# TYPE kuttidb_cache_evicted_total counter\n"
         "kuttidb_cache_evicted_total %llu\n", (unsigned long long)kuttidb_evicted_count(g_cache));
    mput(&m,
         "# HELP kuttidb_connections Currently connected clients.\n"
         "# TYPE kuttidb_connections gauge\n"
         "kuttidb_connections %u\n", atomic_load(&g_connections));
    mput(&m,
         "# HELP kuttidb_rejected_connections_total Connections refused by a server limit.\n"
         "# TYPE kuttidb_rejected_connections_total counter\n"
         "kuttidb_rejected_connections_total %llu\n", (unsigned long long)atomic_load(&g_rejected_connections));
    mput(&m,
         "# HELP kuttidb_auth_failures_total Failed authentication attempts.\n"
         "# TYPE kuttidb_auth_failures_total counter\n"
         "kuttidb_auth_failures_total %llu\n", (unsigned long long)atomic_load(&g_auth_failures));
    mput(&m,
         "# HELP kuttidb_event_loops Configured event-loop threads.\n"
         "# TYPE kuttidb_event_loops gauge\n"
         "kuttidb_event_loops %d\n", g_nloops);
    mput(&m,
         "# HELP kuttidb_event_backend_info Native event backend in use.\n"
         "# TYPE kuttidb_event_backend_info gauge\n"
         "kuttidb_event_backend_info{backend=\"%s\"} 1\n", platform_event_backend());
    mput(&m,
         "# HELP kuttidb_durability_info Configured durability mode.\n"
         "# TYPE kuttidb_durability_info gauge\n"
         "kuttidb_durability_info{mode=\"%s\"} 1\n",
         g_durability == DUR_ALWAYS ? "always" : "periodic");
    mput(&m,
         "# HELP kuttidb_queues Declared queues.\n"
         "# TYPE kuttidb_queues gauge\n"
         "kuttidb_queues %llu\n", (unsigned long long)queue_count(g_queues));
    mput(&m,
         "# HELP kuttidb_queue_depth Messages waiting across all queues.\n"
         "# TYPE kuttidb_queue_depth gauge\n"
         "kuttidb_queue_depth %llu\n", (unsigned long long)queue_total_depth(g_queues));
    mput(&m,
         "# HELP kuttidb_queue_inflight Unacknowledged deliveries across all queues.\n"
         "# TYPE kuttidb_queue_inflight gauge\n"
         "kuttidb_queue_inflight %llu\n", (unsigned long long)queue_total_inflight(g_queues));
    mput(&m,
         "# HELP kuttidb_queue_redeliveries_total Redelivered messages since startup.\n"
         "# TYPE kuttidb_queue_redeliveries_total counter\n"
         "kuttidb_queue_redeliveries_total %llu\n", (unsigned long long)queue_redeliveries(g_queues));
    mput(&m,
         "# HELP kuttidb_queue_wal_failed Whether the queue WAL entered its fail-closed state.\n"
         "# TYPE kuttidb_queue_wal_failed gauge\n"
         "kuttidb_queue_wal_failed %d\n", queue_persistence_failed(g_queues));
    mput(&m,
         "# HELP kuttidb_queue_deadlettered_total Messages routed to a dead-letter queue.\n"
         "# TYPE kuttidb_queue_deadlettered_total counter\n"
         "kuttidb_queue_deadlettered_total %llu\n", (unsigned long long)queue_deadlettered(g_queues));
    mput(&m,
         "# HELP kuttidb_exchanges Declared exchanges.\n"
         "# TYPE kuttidb_exchanges gauge\n"
         "kuttidb_exchanges %llu\n", (unsigned long long)exchange_count(g_queues));
    mput(&m,
         "# HELP kuttidb_exchange_bindings Queue bindings across exchanges.\n"
         "# TYPE kuttidb_exchange_bindings gauge\n"
         "kuttidb_exchange_bindings %llu\n", (unsigned long long)exchange_binding_count(g_queues));
    mput(&m,
         "# HELP kuttidb_exchange_unroutable_total Publishes that matched no binding.\n"
         "# TYPE kuttidb_exchange_unroutable_total counter\n"
         "kuttidb_exchange_unroutable_total %llu\n", (unsigned long long)exchange_unroutable(g_queues));
    mput(&m,
         "# HELP kuttidb_stream_topics Declared stream topics.\n"
         "# TYPE kuttidb_stream_topics gauge\n"
         "kuttidb_stream_topics %llu\n", (unsigned long long)stream_topic_count(g_streams));
    mput(&m,
         "# HELP kuttidb_stream_partitions Stream partitions across topics.\n"
         "# TYPE kuttidb_stream_partitions gauge\n"
         "kuttidb_stream_partitions %llu\n", (unsigned long long)stream_partition_count(g_streams));
    mput(&m,
         "# HELP kuttidb_stream_retention_bytes Retained stream record bytes.\n"
         "# TYPE kuttidb_stream_retention_bytes gauge\n"
         "kuttidb_stream_retention_bytes %llu\n", (unsigned long long)stream_retention_bytes(g_streams));
    mput(&m,
         "# HELP kuttidb_stream_groups Stream consumer groups with durable offsets.\n"
         "# TYPE kuttidb_stream_groups gauge\n"
         "kuttidb_stream_groups %llu\n", (unsigned long long)stream_group_count(g_streams));
    mput(&m,
         "# HELP kuttidb_stream_group_members Live consumer-group members.\n"
         "# TYPE kuttidb_stream_group_members gauge\n"
         "kuttidb_stream_group_members %llu\n", (unsigned long long)stream_group_member_count(g_streams));
    mput(&m,
         "# HELP kuttidb_stream_wal_failed Whether the stream WAL entered its fail-closed state.\n"
         "# TYPE kuttidb_stream_wal_failed gauge\n"
         "kuttidb_stream_wal_failed %d\n", stream_persistence_failed(g_streams));
    uint64_t sf_claims = 0, sf_waiters = 0, sf_neg = 0;
    sf_stats_counts(&sf_claims, &sf_waiters, &sf_neg);
    mput(&m,
         "# HELP kuttidb_singleflight_claims_total Single-flight claims taken since startup.\n"
         "# TYPE kuttidb_singleflight_claims_total counter\n"
         "kuttidb_singleflight_claims_total %llu\n", (unsigned long long)sf_claims);
    mput(&m,
         "# HELP kuttidb_singleflight_waiters Waiters currently blocked on a claim.\n"
         "# TYPE kuttidb_singleflight_waiters gauge\n"
         "kuttidb_singleflight_waiters %llu\n", (unsigned long long)sf_waiters);
    mput(&m,
         "# HELP kuttidb_negative_cache_entries Cached negative answers.\n"
         "# TYPE kuttidb_negative_cache_entries gauge\n"
         "kuttidb_negative_cache_entries %llu\n", (unsigned long long)sf_neg);
    mput(&m,
         "# HELP kuttidb_stale_entries Retained stale-while-revalidate copies.\n"
         "# TYPE kuttidb_stale_entries gauge\n"
         "kuttidb_stale_entries %llu\n", (unsigned long long)sf_stale_entries());
    mput(&m,
         "# HELP kuttidb_stale_serves GET_OR_REFRESH answers served stale.\n"
         "# TYPE kuttidb_stale_serves counter\n"
         "kuttidb_stale_serves %llu\n",
         (unsigned long long)atomic_load(&g_stale_serves));
    mput(&m,
         "# HELP kuttidb_refresh_serves GET_OR_REFRESH answers with refresh due.\n"
         "# TYPE kuttidb_refresh_serves counter\n"
         "kuttidb_refresh_serves %llu\n",
         (unsigned long long)atomic_load(&g_refresh_serves));

    /* Labeled per-queue and per-topic series, bounded so one scrape can
     * neither grow without limit nor produce a partial line. */
    mput(&m,
         "# HELP kuttidb_queue_depth Labeled: messages waiting in a named queue.\n"
         "# TYPE kuttidb_queue_depth gauge\n");
    mput(&m,
         "# HELP kuttidb_queue_inflight Labeled: unacknowledged deliveries per queue.\n"
         "# TYPE kuttidb_queue_inflight gauge\n");
    struct metrics_iter qctx = {&m, 0, 0};
    queue_foreach_stats(g_queues, queue_metric_cb, &qctx);
    if (qctx.truncated)
        mput(&m, "kuttidb_queue_metrics_truncated 1\n");
    mput(&m,
         "# HELP kuttidb_topic_partitions Labeled: partitions per stream topic.\n"
         "# TYPE kuttidb_topic_partitions gauge\n");
    mput(&m,
         "# HELP kuttidb_topic_retained_bytes Labeled: retained record bytes per topic.\n"
         "# TYPE kuttidb_topic_retained_bytes gauge\n");
    mput(&m,
         "# HELP kuttidb_topic_records Labeled: retained records per topic.\n"
         "# TYPE kuttidb_topic_records gauge\n");
    struct metrics_iter sctx = {&m, 0, 0};
    stream_foreach_stats(g_streams, stream_metric_cb, &sctx);
    if (sctx.truncated)
        mput(&m, "kuttidb_stream_metrics_truncated 1\n");
    return m.len;
}

static void http_send(int fd, const char *status, const char *extra,
                      const char *body, size_t blen) {
    char head[320];
    int n = snprintf(head, sizeof head,
        "HTTP/1.1 %s\r\n"
        "Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n"
        "Content-Length: %zu\r\n"
        "Connection: close\r\n"
        "%s\r\n", status, blen, extra ? extra : "");
    if (n <= 0 || n >= (int)sizeof head) return;
    size_t off = 0;
    while (off < (size_t)n) {
        ssize_t w = send(fd, head + off, (size_t)n - off, MSG_NOSIGNAL);
        if (w < 0 && errno == EINTR) continue;
        if (w <= 0) return;
        off += (size_t)w;
    }
    off = 0;
    while (off < blen) {
        ssize_t w = send(fd, body + off, blen - off, MSG_NOSIGNAL);
        if (w < 0 && errno == EINTR) continue;
        if (w <= 0) return;
        off += (size_t)w;
    }
}

/* Extract a case-insensitive `Authorization: Bearer <token>` header value. */
static int http_bearer(const char *h, size_t n, const char **out, size_t *out_len) {
    size_t i = 0;
    while (i < n) {
        if (i + 14 <= n && strncasecmp(h + i, "authorization:", 14) == 0) {
            i += 14;
            while (i < n && h[i] == ' ') i++;
            if (i + 7 <= n && strncasecmp(h + i, "bearer", 6) == 0 &&
                (h[i + 6] == ' ' || h[i + 6] == '\r' || h[i + 6] == '\n')) {
                i += 6;
                while (i < n && h[i] == ' ') i++;
                size_t start = i;
                while (i < n && h[i] != '\r' && h[i] != '\n') i++;
                if (i > start) { *out = h + start; *out_len = i - start; return 1; }
            }
            return 0;
        }
        const char *nl = memchr(h + i, '\n', n - i);
        if (!nl) return 0;
        i = (size_t)(nl - h) + 1;
    }
    return 0;
}

static void metrics_handle(int fd) {
    char req[8192];
    size_t used = 0;
    for (;;) {
        if (used >= sizeof req) return; /* oversized request line/header: drop */
        ssize_t r = recv(fd, req + used, sizeof req - used, 0);
        if (r < 0 && errno == EINTR) continue;
        if (r <= 0) return;
        used += (size_t)r;
        if (memmem(req, used, "\r\n\r\n", 4)) break;
    }
    char *sp1 = memchr(req, ' ', used);
    if (!sp1) { http_send(fd, "400 Bad Request", NULL, "", 0); return; }
    char *path = sp1 + 1;
    char *sp2 = memchr(path, ' ', used - (size_t)(path - req));
    if (!sp2) { http_send(fd, "400 Bad Request", NULL, "", 0); return; }
    size_t path_len = (size_t)(sp2 - path);
    /* The bearer token, when configured, gates every path. */
    if (g_metrics_secret_len) {
        const char *tok = NULL; size_t tok_len = 0;
        size_t hdr_end = used > 4 ? used - 4 : used; /* exclude final CRLFCRLF */
        if (!http_bearer(req, hdr_end, &tok, &tok_len) ||
            !secret_equal((const unsigned char *)tok, tok_len,
                          (const unsigned char *)g_metrics_secret,
                          g_metrics_secret_len)) {
            http_send(fd, "401 Unauthorized", "WWW-Authenticate: Bearer\r\n", "", 0);
            return;
        }
    }
    if (path_len == 6 && memcmp(path, "/ready", 6) == 0) {
        /* Readiness mirrors the HEALTH contract: ready only while every
         * configured persistence engine remains writable. */
        int ready = !(atomic_load(&g_wal_failed) ||
                      queue_persistence_failed(g_queues) ||
                      stream_persistence_failed(g_streams));
        if (ready) http_send(fd, "200 OK", NULL, "ready\n", 6);
        else http_send(fd, "503 Service Unavailable", NULL, "not ready\n", 10);
        return;
    }
    if (path_len == 5 && memcmp(path, "/live", 5) == 0) {
        /* Liveness: the admin listener itself is answering. */
        http_send(fd, "200 OK", NULL, "live\n", 5);
        return;
    }
    int ok = (path_len == 8 && memcmp(path, "/metrics", 8) == 0) ||
             (path_len == 1 && path[0] == '/');
    if (!ok) { http_send(fd, "404 Not Found", NULL, "", 0); return; }
    /* The render buffer is heap-allocated: it is too large for the small
     * default stacks some libc thread implementations provide (musl: 128 KiB). */
    char *body = malloc(131072);
    if (!body) { http_send(fd, "500 Internal Server Error", NULL, "", 0); return; }
    size_t blen = metrics_render(body, 131072);
    http_send(fd, "200 OK", NULL, body, blen);
    free(body);
}

/* Metrics scrape credentials follow the same file rules as the auth token:
 * regular file, owned by the server user, mode 0600, no symlinks. */
static int load_metrics_token(const char *path) {
    int fd = open(path, O_RDONLY | O_NOFOLLOW);
    if (fd < 0) { perror("metrics token file"); return -1; }
    struct stat st;
    if (fstat(fd, &st) < 0 || !S_ISREG(st.st_mode) || st.st_uid != geteuid()) {
        fprintf(stderr, "metrics token file must be a regular file owned by the server user\n");
        close(fd);
        return -1;
    }
    if (st.st_mode & 0077) {
        fprintf(stderr, "metrics token file permissions must be 0600\n");
        close(fd);
        return -1;
    }
    unsigned char raw[AUTH_MAX + 3];
    size_t used = 0;
    ssize_t r = 0;
    while (used < sizeof raw) {
        r = read(fd, raw + used, sizeof raw - used);
        if (r < 0 && errno == EINTR) continue;
        if (r <= 0) break;
        used += (size_t)r;
    }
    close(fd);
    if (r < 0) { perror("metrics token read"); return -1; }
    int overflow = used == sizeof raw;
    while (used > 0 && (raw[used - 1] == '\n' || raw[used - 1] == '\r')) used--;
    if (overflow || used == 0 || used > AUTH_MAX) {
        fprintf(stderr, "metrics token must contain 1..%u bytes\n", AUTH_MAX);
        wipe_secret(raw, sizeof raw);
        return -1;
    }
    memcpy(g_metrics_secret, raw, used);
    g_metrics_secret_len = used;
    wipe_secret(raw, sizeof raw);
    return 0;
}

static void *metrics_thread(void *unused) {
    (void)unused;
    for (;;) {
        int cfd = accept(g_metrics_fd, NULL, NULL);
        if (cfd < 0) {
            if (errno == EINTR) continue;
            if (errno == EBADF || errno == EINVAL) break;
            continue;
        }
        struct timeval tv = {5, 0};
        setsockopt(cfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
        setsockopt(cfd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);
        fcntl(cfd, F_SETFD, FD_CLOEXEC);
        metrics_handle(cfd);
        close(cfd);
    }
    return NULL;
}

/* Metrics listener bound before the event threads start; a bind failure is
 * fatal so a misconfigured deployment fails loudly instead of silently
 * losing its scrape target. */
static int metrics_listen(const char *spec) {
    char copy[128];
    size_t n = strlen(spec);
    if (!n || n >= sizeof copy) { fprintf(stderr, "invalid --metrics-bind: %s\n", spec); return -1; }
    memcpy(copy, spec, n + 1);
    char *colon = strrchr(copy, ':');
    if (!colon || colon == copy) { fprintf(stderr, "--metrics-bind must be IPv4:PORT\n"); return -1; }
    *colon = 0;
    unsigned long long port;
    if (parse_ull(colon + 1, &port) < 0 || port == 0 || port > 65535) {
        fprintf(stderr, "invalid --metrics-bind port: %s\n", colon + 1); return -1;
    }
    struct in_addr addr;
    if (inet_pton(AF_INET, copy, &addr) != 1) {
        fprintf(stderr, "invalid --metrics-bind address: %s\n", copy); return -1;
    }
    if (!is_loopback_addr(copy) && g_metrics_secret_len == 0) {
        fprintf(stderr, "refusing non-loopback --metrics-bind without --metrics-token-file\n");
        return -1;
    }
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("metrics socket"); return -1; }
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    struct sockaddr_in sa = {0};
    sa.sin_family = AF_INET;
    sa.sin_addr = addr;
    sa.sin_port = htons((uint16_t)port);
    if (bind(fd, (struct sockaddr *)&sa, sizeof sa) < 0 || listen(fd, 8) < 0) {
        perror("metrics bind");
        close(fd);
        return -1;
    }
    fcntl(fd, F_SETFD, FD_CLOEXEC);
    g_metrics_fd = fd;
    return 0;
}

static void usage(const char *prog) {
    fprintf(stderr,
        "usage: %s [PORT [WAL [FSYNC_MS [UNIX_PATH [MAX_MEM_MB [EMBED_PATH]]]]]]\n"
        "       [--bind IPv4] [--auth-file PATH] [--tls-cert PATH --tls-key PATH]\n"
        "       [--max-value-mb N] [--threads N] [--embed-region-mb N]\n"
        "       [--durability periodic|always] [--fsync-ms N]\n"
        "       [--data-dir ABS_PATH --listen unix:ABS_PATH|tcp:127.x.x.x:PORT]\n"
        "       [--lifecycle standalone|managed-idle --idle-timeout-ms N]\n"
        "       [--max-batch-mb N] [--max-clients N] [--queue-wal PATH|-] [--stream-wal PATH|-]\n"
        "       [--metrics-bind IPv4:PORT [--metrics-token-file PATH]]\n"
        "       [--admin-bind IPv4:PORT --admin-token-file PATH --admin-audit-log PATH\n"
        "        [--admin-allow-origin ORIGIN] [--admin-tls-cert PATH --admin-tls-key PATH]\n"
        "        [--admin-max-clients N] [--admin-max-tail-clients N]\n"
        "        [--admin-session-limit N] [--admin-job-limit N]]\n", prog);
}

int main(int argc, char **argv) {
    int ensure_status = managed_launcher_maybe_run(argc, argv);
    if (ensure_status >= 0) return ensure_status;
    umask(0077);
    const char *bind_addr = "127.0.0.1";
    const char *auth_file = NULL;
    const char *tls_cert = NULL;
    const char *tls_key = NULL;
    const char *queue_wal = NULL;
    const char *stream_wal = NULL;
    const char *metrics_bind = NULL;
    const char *metrics_token_file = NULL;
    const char *admin_bind = NULL, *admin_token_file = NULL, *admin_audit_log = NULL;
    const char *admin_tls_cert = NULL, *admin_tls_key = NULL;
    const char *admin_origins[16]; size_t admin_origin_count = 0;
    unsigned admin_max_clients = 16, admin_max_tail_clients = 4;
    unsigned admin_session_limit = 256, admin_job_limit = 32;
    unsigned long long embed_region_mb = 1024;
    unsigned long long named_max_memory_mb = 0;
    const char *data_dir = NULL;
    const char *named_unix_path = NULL;
    char named_tcp_bind[INET_ADDRSTRLEN] = "";
    int named_tcp_port = 0;
    int no_tcp = 0, ready_fd = -1;
    const char *pos[6];
    int npos = 0;
    for (int i = 1; i < argc; i++) {
        const char *a = argv[i];
        if (strcmp(a, "--help") == 0) { usage(argv[0]); return 0; }
        if (strcmp(a, "--no-tcp") == 0) { no_tcp = 1; continue; }
        if (strcmp(a, "--features") == 0) {
#ifdef HAVE_OPENSSL
            puts("tls=openssl");
            puts("admin-tls=openssl");
#else
            puts("tls=off");
            puts("admin-tls=off");
#endif
            puts("management-api=v1");
            puts("management-api-contract=1.0");
            puts("management-api-audit=required");
            puts("management-api-sse=off");
            return 0;
        }
        if (strcmp(a, "--bind") == 0 || strcmp(a, "--auth-file") == 0 ||
            strcmp(a, "--tls-cert") == 0 || strcmp(a, "--tls-key") == 0 ||
            strcmp(a, "--max-value-mb") == 0 || strcmp(a, "--max-batch-mb") == 0 ||
            strcmp(a, "--max-clients") == 0 || strcmp(a, "--threads") == 0 ||
            strcmp(a, "--embed-region-mb") == 0 || strcmp(a, "--durability") == 0 ||
            strcmp(a, "--queue-wal") == 0 || strcmp(a, "--stream-wal") == 0 ||
            strcmp(a, "--metrics-bind") == 0 || strcmp(a, "--metrics-token-file") == 0 ||
            strcmp(a, "--admin-bind") == 0 || strcmp(a, "--admin-token-file") == 0 ||
            strcmp(a, "--admin-allow-origin") == 0 || strcmp(a, "--admin-tls-cert") == 0 || strcmp(a, "--admin-tls-key") == 0 ||
            strcmp(a, "--admin-audit-log") == 0 || strcmp(a, "--admin-max-clients") == 0 ||
            strcmp(a, "--admin-max-tail-clients") == 0 || strcmp(a, "--admin-session-limit") == 0 ||
            strcmp(a, "--admin-job-limit") == 0 || strcmp(a, "--fsync-ms") == 0) {
            if (++i >= argc) { usage(argv[0]); return 2; }
            const char *v = argv[i];
            unsigned long long n;
            if (strcmp(a, "--bind") == 0) bind_addr = v;
            else if (strcmp(a, "--auth-file") == 0) auth_file = v;
            else if (strcmp(a, "--tls-cert") == 0) tls_cert = v;
            else if (strcmp(a, "--tls-key") == 0) tls_key = v;
            else if (strcmp(a, "--queue-wal") == 0) queue_wal = v;
            else if (strcmp(a, "--stream-wal") == 0) stream_wal = v;
            else if (strcmp(a, "--metrics-bind") == 0) metrics_bind = v;
            else if (strcmp(a, "--metrics-token-file") == 0) metrics_token_file = v;
            else if (strcmp(a, "--admin-bind") == 0) admin_bind = v;
            else if (strcmp(a, "--admin-token-file") == 0) admin_token_file = v;
            else if (strcmp(a, "--admin-tls-cert") == 0) admin_tls_cert = v;
            else if (strcmp(a, "--admin-tls-key") == 0) admin_tls_key = v;
            else if (strcmp(a, "--admin-audit-log") == 0) admin_audit_log = v;
            else if (strcmp(a, "--admin-allow-origin") == 0) {
                if (strcmp(v, "*") == 0 || !strstr(v, "://") || admin_origin_count == 16) { fprintf(stderr, "invalid --admin-allow-origin\n"); return 2; }
                admin_origins[admin_origin_count++] = v;
            }
            else if (strcmp(a, "--durability") == 0) {
                if (strcmp(v, "periodic") == 0) g_durability = DUR_PERIODIC;
                else if (strcmp(v, "always") == 0) g_durability = DUR_ALWAYS;
                else { fprintf(stderr, "durability must be periodic or always\n"); return 2; }
            }
            else if (strcmp(a, "--fsync-ms") == 0) {
                if (parse_ull(v, &n) < 0 || n > LONG_MAX) { fprintf(stderr, "invalid fsync interval\n"); return 2; }
                g_fsync_ms = (long)n;
            }
            else if (parse_ull(v, &n) < 0 || n == 0) {
                fprintf(stderr, "invalid value for %s: %s\n", a, v);
                return 2;
            } else if (strcmp(a, "--max-value-mb") == 0) {
                if (n > (ABS_MAX_VAL >> 20)) { fprintf(stderr, "max value exceeds 1024 MiB\n"); return 2; }
                g_max_val = (uint32_t)(n << 20);
            } else if (strcmp(a, "--max-batch-mb") == 0) {
                if (n > (ABS_MAX_VAL >> 20)) { fprintf(stderr, "max batch exceeds 1024 MiB\n"); return 2; }
                g_max_batch_bytes = n << 20;
            } else if (strcmp(a, "--threads") == 0) {
                if (n > 64) { fprintf(stderr, "threads must be 1..64\n"); return 2; }
                g_requested_loops = (int)n;
            } else if (strcmp(a, "--embed-region-mb") == 0) {
                if (n < 16 || n > (SIZE_MAX >> 20)) {
                    fprintf(stderr, "embed region must be at least 16 MiB\n"); return 2;
                }
                embed_region_mb = n;
            } else if (strcmp(a, "--admin-max-clients") == 0) {
                if (n > 1024) { fprintf(stderr, "admin max clients must be 1..1024\n"); return 2; }
                admin_max_clients = (unsigned)n;
            } else if (strcmp(a, "--admin-max-tail-clients") == 0) {
                if (n > 256) { fprintf(stderr, "admin max tail clients must be 1..256\n"); return 2; }
                admin_max_tail_clients = (unsigned)n;
            } else if (strcmp(a, "--admin-session-limit") == 0) {
                if (n > 65536) { fprintf(stderr, "admin session limit must be 1..65536\n"); return 2; }
                admin_session_limit = (unsigned)n;
            } else if (strcmp(a, "--admin-job-limit") == 0) {
                if (n > 1024) { fprintf(stderr, "admin job limit must be 1..1024\n"); return 2; }
                admin_job_limit = (unsigned)n;
            } else {
                if (n > 1000000u) { fprintf(stderr, "max clients is too large\n"); return 2; }
                g_max_clients = (unsigned int)n;
            }
            continue;
        }
        if (strcmp(a, "--data-dir") == 0 || strcmp(a, "--unix-path") == 0 || strcmp(a, "--listen") == 0 ||
            strcmp(a, "--lifecycle") == 0 || strcmp(a, "--idle-timeout-ms") == 0 ||
            strcmp(a, "--startup-orphan-timeout-ms") == 0 || strcmp(a, "--ready-fd") == 0 ||
            strcmp(a, "--max-memory-mb") == 0) {
            if (++i >= argc) { usage(argv[0]); return 2; }
            const char *v = argv[i]; unsigned long long n;
            if (strcmp(a, "--data-dir") == 0) data_dir = v;
            else if (strcmp(a, "--unix-path") == 0) named_unix_path = v;
            else if (strcmp(a, "--listen") == 0) {
                if (strncmp(v, "unix:", 5) == 0 && v[5] == '/') {
                    named_unix_path = v + 5; no_tcp = 1;
                } else if (strncmp(v, "tcp:", 4) == 0) {
                    const char *endpoint = v + 4;
                    const char *colon = strrchr(endpoint, ':');
                    unsigned long long endpoint_port;
                    struct in_addr endpoint_addr;
                    size_t host_len = colon ? (size_t)(colon - endpoint) : 0;
                    if (!colon || host_len == 0 || host_len >= sizeof named_tcp_bind ||
                        parse_ull(colon + 1, &endpoint_port) < 0 || endpoint_port == 0 ||
                        endpoint_port > 65535) {
                        fprintf(stderr, "--listen tcp requires a literal IPv4 loopback endpoint\n");
                        return 2;
                    }
                    memcpy(named_tcp_bind, endpoint, host_len);
                    named_tcp_bind[host_len] = 0;
                    if (inet_pton(AF_INET, named_tcp_bind, &endpoint_addr) != 1 ||
                        (ntohl(endpoint_addr.s_addr) >> 24) != 127) {
                        fprintf(stderr, "--listen tcp requires a literal IPv4 loopback endpoint\n");
                        return 2;
                    }
                    named_tcp_port = (int)endpoint_port;
                    bind_addr = named_tcp_bind;
                    no_tcp = 0;
                } else {
                    fprintf(stderr, "--listen requires unix:/absolute/path or tcp:127.x.x.x:port\n");
                    return 2;
                }
            }
            else if (strcmp(a, "--lifecycle") == 0) {
                if (strcmp(v, "standalone") == 0) g_lifecycle_mode = MANAGED_STANDALONE;
                else if (strcmp(v, "managed-idle") == 0) g_lifecycle_mode = MANAGED_IDLE;
                else { fprintf(stderr, "lifecycle must be standalone or managed-idle\n"); return 2; }
            } else if (strcmp(a, "--max-memory-mb") == 0) {
                if (parse_ull(v, &n) < 0 || n == 0 || n > (ULLONG_MAX >> 20)) { fprintf(stderr, "invalid max memory\n"); return 2; }
                named_max_memory_mb = n;
            } else if (parse_ull(v, &n) < 0 || n == 0 || n > 3600000ull) {
                fprintf(stderr, "invalid value for %s\n", a); return 2;
            } else if (strcmp(a, "--idle-timeout-ms") == 0) g_idle_timeout_ms = n;
            else if (strcmp(a, "--startup-orphan-timeout-ms") == 0) g_orphan_timeout_ms = n;
            else ready_fd = (int)n;
            continue;
        }
        if (strncmp(a, "--", 2) == 0 || npos == 6) { usage(argv[0]); return 2; }
        pos[npos++] = a;
    }

    int port = DEFAULT_PORT;
    unsigned long long parsed;
    if (npos > 0) {
        if (parse_ull(pos[0], &parsed) < 0 || parsed == 0 || parsed > 65535) {
            fprintf(stderr, "invalid port: %s\n", pos[0]); return 2;
        }
        port = (int)parsed;
    }
    if (named_tcp_port) port = named_tcp_port;
    snprintf(g_wal_path, sizeof g_wal_path, "kuttidb.wal");
    if (npos > 1) {
        if (strlen(pos[1]) >= sizeof g_wal_path) { fprintf(stderr, "wal path too long\n"); return 2; }
        snprintf(g_wal_path, sizeof g_wal_path, "%s", pos[1]);
    }
    if (npos > 2) {
        if (parse_ull(pos[2], &parsed) < 0 || parsed > LONG_MAX) {
            fprintf(stderr, "invalid fsync interval: %s\n", pos[2]); return 2;
        }
        g_fsync_ms = (long)parsed;
    }
    const char *unix_path = named_unix_path ? named_unix_path :
        (npos > 3 && pos[3][0] && strcmp(pos[3], "-") != 0 ? pos[3] : NULL);
    unsigned long long max_mem_mb = 0;
    if (npos > 4 && parse_ull(pos[4], &max_mem_mb) < 0) {
        fprintf(stderr, "invalid memory budget: %s\n", pos[4]); return 2;
    }
    if (max_mem_mb > ULLONG_MAX / (1ull << 20)) { fprintf(stderr, "memory budget too large\n"); return 2; }
    if (named_max_memory_mb) max_mem_mb = named_max_memory_mb;
    const char *embed_path = npos > 5 && pos[5][0] && strcmp(pos[5], "-") != 0 ? pos[5] : NULL;

    if (data_dir) {
        if (npos > 1 || data_dir[0] != '/' || instance_secure_dir(data_dir) < 0 ||
            instance_load_or_create_id(data_dir, g_instance_id) < 0) {
            fprintf(stderr, "unsafe managed data directory\n"); return 2;
        }
        int n = snprintf(g_wal_path, sizeof g_wal_path, "%s/kuttidb.wal", data_dir);
        if (n < 0 || (size_t)n >= sizeof g_wal_path) { fprintf(stderr, "data directory path too long\n"); return 2; }
        g_has_instance_id = 1;
        if (!unix_path && !named_tcp_port) {
            fprintf(stderr, "--data-dir requires a managed Unix or TCP listener\n");
            return 2;
        }
    }
    if (g_lifecycle_mode == MANAGED_IDLE &&
        (!data_dir || (!unix_path && !named_tcp_port) || (no_tcp && !unix_path) || embed_path)) {
        fprintf(stderr, "managed-idle requires --data-dir and a managed local listener (embedded mode is unsupported)\n");
        return 2;
    }

    struct in_addr bind_check;
    if (inet_pton(AF_INET, bind_addr, &bind_check) != 1) {
        fprintf(stderr, "invalid IPv4 bind address: %s\n", bind_addr); return 2;
    }
    if (auth_file && load_auth_file(auth_file) < 0) return 2;
    if ((tls_cert == NULL) != (tls_key == NULL)) {
        fprintf(stderr, "--tls-cert and --tls-key must be provided together\n");
        return 2;
    }
    if (tls_cert && tls_init(tls_cert, tls_key) < 0) return 2;
    if (!is_loopback_addr(bind_addr) && g_auth_len == 0) {
        fprintf(stderr, "refusing non-loopback bind without --auth-file\n");
        return 2;
    }
    if (metrics_bind) {
        if (metrics_token_file && load_metrics_token(metrics_token_file) < 0) return 2;
    }
    if (admin_bind) {
        if (!admin_token_file || !admin_audit_log) { fprintf(stderr, "--admin-token-file and --admin-audit-log are required with --admin-bind\n"); return 2; }
        if (admin_http_load_token(admin_token_file, g_admin_token, &g_admin_token_len, sizeof g_admin_token) < 0) return 2;
    } else if (admin_token_file || admin_audit_log || admin_tls_cert || admin_tls_key || admin_origin_count) {
        fprintf(stderr, "admin options require --admin-bind\n"); return 2;
    }

    signal(SIGPIPE, SIG_IGN);
    struct sigaction sa = {0};
    sa.sa_handler = on_sigint;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    crc_init();
    int persist = g_wal_path[0] && strcmp(g_wal_path, "-") != 0;
    g_persist_enabled = persist;

    char inferred_queue_wal[576];
    const char *queue_path = queue_wal;
    if (!queue_path && persist) {
        int n = snprintf(inferred_queue_wal, sizeof inferred_queue_wal, "%s.queues", g_wal_path);
        if (n < 0 || (size_t)n >= sizeof inferred_queue_wal) {
            fprintf(stderr, "queue wal path too long\n");
            return 1;
        }
        queue_path = inferred_queue_wal;
    }
    if (queue_path && strcmp(queue_path, "-") == 0) queue_path = NULL;

    char inferred_stream_wal[576];
    const char *stream_path = stream_wal;
    if (!stream_path && persist) {
        int n = snprintf(inferred_stream_wal, sizeof inferred_stream_wal, "%s.streams", g_wal_path);
        if (n < 0 || (size_t)n >= sizeof inferred_stream_wal) {
            fprintf(stderr, "stream wal path too long\n");
            return 1;
        }
        stream_path = inferred_stream_wal;
    }
    if (stream_path && strcmp(stream_path, "-") == 0) stream_path = NULL;
    if (acquire_server_ownership(data_dir, unix_path, bind_addr, port,
                                 persist ? g_wal_path : NULL, queue_path,
                                 stream_path, embed_path) < 0) {
        fprintf(stderr, "instance already owned or ownership lock is unsafe\n");
        return 73;
    }
    /* Every listener, including observability, comes after the lifetime locks
     * so a losing server never exposes a partially initialized instance. */
    if (metrics_bind && metrics_listen(metrics_bind) < 0) return 1;

    /* The embed backend can open/attach writable state as part of creation,
     * so it must come after every lifetime lock is held. */
    if (embed_path) {
        g_wal_shared = persist;
        g_cache = kuttidb_embed_create_sized(embed_path, NSHARDS,
                                           persist ? g_wal_path : NULL,
                                           (size_t)embed_region_mb << 20);
        if (!g_cache) { fprintf(stderr, "embed region create/attach failed\n"); return 1; }
        fprintf(stderr, "embedded shared-memory region: %s\n", embed_path);
    } else {
        g_cache = kuttidb_create(NSHARDS, 64);
        if (!g_cache) { fprintf(stderr, "kuttidb_create failed\n"); return 1; }
    }
    if (max_mem_mb)
        kuttidb_set_budget(g_cache, max_mem_mb * (1ull << 20));

    /* Transaction ids embed a per-process nonce so ids never collide across
     * restarts while reconciling prepared transactions. */
    {
        unsigned char seed[4] = {0};
        int sfd = open("/dev/urandom", O_RDONLY);
        if (sfd >= 0) {
            ssize_t got = read(sfd, seed, sizeof seed);
            (void)got;
            close(sfd);
        }
        uint32_t nonce = (uint32_t)seed[0] | ((uint32_t)seed[1] << 8) |
                         ((uint32_t)seed[2] << 16) | ((uint32_t)seed[3] << 24);
        if (!nonce) nonce = (uint32_t)time(NULL) ^ (uint32_t)getpid();
        if (!nonce) nonce = 1;
        g_tx_nonce = nonce;
    }

    g_queues = queue_store_open(queue_path);
    if (!g_queues) { fprintf(stderr, "queue store open failed\n"); return 1; }
    g_streams = stream_store_open(stream_path);
    if (!g_streams) {
        fprintf(stderr, "stream store open failed\n");
        queue_store_close(g_queues);
        return 1;
    }

    if (persist) {
        g_wal_fd = open_private(g_wal_path, O_WRONLY | O_CREAT | O_APPEND);
        if (g_wal_fd < 0) { perror("open wal"); return 1; }
        load_persisted();
        fprintf(stderr, "persistence: %s (fsync every %ld ms)\n", g_wal_path, g_fsync_ms);
    }
    reconcile_transactions();
    g_nloops = g_requested_loops ? g_requested_loops
                                 : (int)sysconf(_SC_NPROCESSORS_ONLN);
    if (g_nloops < 1) g_nloops = 1;
    if (!g_requested_loops && g_nloops > 4) g_nloops = 4;
    if (no_tcp) g_nloops = 1;
    g_started_at = time(NULL);
    managed_lifecycle_init(&g_lifecycle, g_lifecycle_mode, g_idle_timeout_ms,
                           g_orphan_timeout_ms);

    if (admin_bind) {
        AdminHttpConfig ac = {0};
        ac.bind = admin_bind; ac.token = g_admin_token; ac.token_len = g_admin_token_len;
        ac.allow_origins = admin_origins; ac.allow_origin_count = admin_origin_count;
        ac.tls_cert = admin_tls_cert; ac.tls_key = admin_tls_key;
        ac.audit_log = admin_audit_log; ac.max_clients = admin_max_clients;
        ac.max_tail_clients = admin_max_tail_clients; ac.session_limit = admin_session_limit;
        ac.job_limit = admin_job_limit;
        ac.keyspace = g_cache; ac.queues = g_queues; ac.streams = g_streams;
        ac.status = admin_status; ac.auth_failure = admin_auth_failure; ac.keyspace_put = persist_put; ac.keyspace_delete = persist_delete; ac.keyspace_claim_acquire = admin_keyspace_claim_acquire; ac.keyspace_claim_complete = admin_keyspace_claim_complete; ac.keyspace_claim_release = admin_keyspace_claim_release; ac.keyspace_checkpoint = admin_keyspace_checkpoint; ac.atomic_execute = admin_atomic_execute;
        g_admin_http = admin_http_create(&ac);
        if (!g_admin_http || admin_http_start(g_admin_http) < 0) {
            fprintf(stderr, "admin listener failed to start\n");
            admin_http_destroy(g_admin_http); return 1;
        }
    }

    if (pthread_create(&g_maintenance_thread, NULL, maintenance_thread, NULL) == 0)
        g_maintenance_started = 1;

    if (g_metrics_fd >= 0) {
        pthread_t metrics_tid;
        pthread_attr_t mattr;
        pthread_attr_init(&mattr);
        pthread_attr_setstacksize(&mattr, 1 << 20);
        if (pthread_create(&metrics_tid, &mattr, metrics_thread, NULL) != 0) {
            fprintf(stderr, "metrics thread failed to start\n");
            return 1;
        }
        pthread_attr_destroy(&mattr);
        pthread_detach(metrics_tid);
    }

    g_loops = calloc((size_t)g_nloops, sizeof(Loop));

    /* one SO_REUSEPORT listener per loop: kernel load-balances accepts */
    for (int i = 0; i < g_nloops; i++) {
        if (platform_loop_init(&g_loops[i].poller) < 0) {
            perror("event loop init");
            return 1;
        }
        g_loops[i].listen_fd = -1;
        if (!no_tcp) {
            g_loops[i].listen_fd = make_tcp_listener(bind_addr, port);
            if (g_loops[i].listen_fd < 0) return 1;
            if (platform_watch_add(&g_loops[i].poller, g_loops[i].listen_fd,
                                   NULL, 1, 0) < 0) {
                perror("listener event registration");
                return 1;
            }
        }
        pthread_create(&g_loops[i].th, NULL, loop_main, &g_loops[i]);
    }

    int ufd = -1;
    if (unix_path) {
        ufd = make_unix_listener(unix_path);
        if (ufd < 0 && no_tcp) {
            fprintf(stderr, "managed Unix listener failed to start\n");
            return 1;
        }
        if (ufd >= 0) {
            if (platform_watch_add(&g_loops[0].poller, ufd, NULL, 1, 0) < 0) {
                perror("unix listener event registration");
                close(ufd);
                ufd = -1;
            }
            fprintf(stderr, "unix socket: %s\n", unix_path);
        }
    }

    fprintf(stderr, "kuttidb listening on %s:%d (%d event loops, %d shards, wal=%s, auth=%s, tls=%s, lifecycle=%s)\n",
            bind_addr, port, g_nloops, NSHARDS, persist ? g_wal_path : "off",
            g_auth_len ? "required" : "off", tls_cert ? "on" : "off",
            managed_lifecycle_name(g_lifecycle_mode));
    if (ready_fd >= 0) {
        if (dprintf(ready_fd, "READY 1\n") < 0) fprintf(stderr, "ready notification failed\n");
        close(ready_fd);
    }

    while (!g_stop)
        pause();

    for (int i = 0; i < g_nloops; i++) {
        platform_loop_wake(&g_loops[i].poller);
        pthread_join(g_loops[i].th, NULL);
        if (g_loops[i].listen_fd >= 0) close(g_loops[i].listen_fd);
        platform_loop_close(&g_loops[i].poller);
    }
    if (ufd >= 0) {
        close(ufd);
        unlink(unix_path);
    }

    if (g_maintenance_started)
        pthread_join(g_maintenance_thread, NULL);

    admin_http_destroy(g_admin_http);

    sf_shutdown();
    stream_store_close(g_streams);
    queue_store_close(g_queues);

    if (persist && g_wal_fd >= 0) {
        do_snapshot();
        wal_flush();
        if (g_wal_fd >= 0) {
            int wal_fd = g_wal_fd;
            fsync(wal_fd);
            close(wal_fd);
        }
    }
    /* listeners are closed with their loops */
    kuttidb_destroy(g_cache); /* no-op in embed mode: region persists */
#ifdef HAVE_OPENSSL
    SSL_CTX_free(g_tls_ctx);
#endif
    wipe_secret(g_auth_token, sizeof g_auth_token);
    wipe_secret(g_admin_token, sizeof g_admin_token);
    instance_locks_release(&g_instance_locks);
    return 0;
}
