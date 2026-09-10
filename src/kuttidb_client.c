#define _GNU_SOURCE
#include "kuttidb_client.h"

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#ifdef HAVE_OPENSSL
#include <openssl/ssl.h>
#include <openssl/x509v3.h>
#endif

/* Public C companion client for the atomic job completion surface.
 * Wire contract: docs/design/PROTOCOL.md (opcodes 0x70-0x77, protocol
 * 1.8+). This is a plain sequential request/response client; it performs
 * no retries and never emulates a completion with separate writes. */

#define JC_KEY_MAX 65535u
#define JC_VALUE_MAX (64u << 20)

struct KuttiDBClient {
    int fd;
    int connected;
    char host[256];
    int port;
    char unix_path[512];
    char auth_token[1024];
    size_t auth_token_len;
    int timeout_ms;
    char ca_file[512];
    char server_name[256];
    int use_tls;
    char error[256];
#ifdef HAVE_OPENSSL
    SSL *ssl;
#endif
    /* Owned response buffer for read-back values. */
    unsigned char *value_buf;
    uint32_t value_len;
};

static void jc_set_error(KuttiDBClient *c, const char *text) {
    if (!c) return;
    snprintf(c->error, sizeof c->error, "%s", text);
}

static void jc_set_error_errno(KuttiDBClient *c, const char *what) {
    if (!c) return;
    snprintf(c->error, sizeof c->error, "%s: %s", what, strerror(errno));
}

const char *kuttidb_job_status_name(KuttidbJobStatus status) {
    switch (status) {
    case KUTTIDB_JOB_OK: return "ok";
    case KUTTIDB_JOB_UNSUPPORTED_FEATURE: return "unsupported_feature";
    case KUTTIDB_JOB_VALIDATION_FAILED: return "validation_failed";
    case KUTTIDB_JOB_REQUEST_TOO_LARGE: return "request_too_large";
    case KUTTIDB_JOB_IDEMPOTENCY_CONFLICT: return "idempotency_conflict";
    case KUTTIDB_JOB_STATE_VERSION_CONFLICT: return "state_version_conflict";
    case KUTTIDB_JOB_DELIVERY_EXPIRED: return "delivery_expired";
    case KUTTIDB_JOB_DELIVERY_NOT_OWNED: return "delivery_not_owned";
    case KUTTIDB_JOB_RESOURCE_EXHAUSTED: return "resource_exhausted";
    case KUTTIDB_JOB_OPERATION_IN_PROGRESS: return "operation_in_progress";
    case KUTTIDB_JOB_OPERATION_IN_DOUBT: return "operation_in_doubt";
    case KUTTIDB_JOB_PERSISTENCE_UNAVAILABLE: return "persistence_unavailable";
    case KUTTIDB_JOB_NOT_FOUND: return "not_found";
    case KUTTIDB_JOB_TRANSPORT_ERROR: return "transport_error";
    }
    return "unknown";
}

int kuttidb_job_status_in_doubt(KuttidbJobStatus status) {
    return status == KUTTIDB_JOB_OPERATION_IN_DOUBT;
}

const char *kuttidb_last_error(const KuttiDBClient *client) {
    return client ? client->error : "null client";
}

int kuttidb_job_new_operation_id(unsigned char out[KUTTIDB_JOB_ID_LEN]) {
    int fd = open("/dev/urandom", O_RDONLY | O_CLOEXEC);
    if (fd < 0) return -1;
    size_t got = 0;
    while (got < KUTTIDB_JOB_ID_LEN) {
        ssize_t n = read(fd, out + got, KUTTIDB_JOB_ID_LEN - got);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) {
            close(fd);
            return -1;
        }
        got += (size_t)n;
    }
    close(fd);
    /* RFC 4122 version 4 shape; the server treats ids as opaque bytes. */
    out[6] = (unsigned char)((out[6] & 0x0f) | 0x40);
    out[8] = (unsigned char)((out[8] & 0x3f) | 0x80);
    return 0;
}

KuttiDBClient *kuttidb_client_create(const KuttiDBClientOptions *options) {
    if (!options) return NULL;
    KuttiDBClient *c = (KuttiDBClient *)calloc(1, sizeof *c);
    if (!c) return NULL;
    c->fd = -1;
    if (options->host) {
        snprintf(c->host, sizeof c->host, "%s", options->host);
    } else {
        snprintf(c->host, sizeof c->host, "127.0.0.1");
    }
    c->port = options->port ? options->port : 7379;
    if (options->unix_path)
        snprintf(c->unix_path, sizeof c->unix_path, "%s", options->unix_path);
    if (options->auth_token && options->auth_token_len &&
        options->auth_token_len <= sizeof c->auth_token) {
        memcpy(c->auth_token, options->auth_token, options->auth_token_len);
        c->auth_token_len = options->auth_token_len;
    }
    c->timeout_ms = options->timeout_seconds > 0
                        ? (int)(options->timeout_seconds * 1000.0)
                        : 5000;
    if (options->tls_ca_file) {
        snprintf(c->ca_file, sizeof c->ca_file, "%s", options->tls_ca_file);
        c->use_tls = 1;
    }
    if (options->tls_server_name)
        snprintf(c->server_name, sizeof c->server_name, "%s",
                 options->tls_server_name);
    return c;
}

static int jc_connect_once(KuttiDBClient *c) {
    if (c->unix_path[0]) {
        struct sockaddr_un addr;
        memset(&addr, 0, sizeof addr);
        addr.sun_family = AF_UNIX;
        if (strlen(c->unix_path) >= sizeof addr.sun_path) {
            jc_set_error(c, "unix path too long");
            return -1;
        }
        snprintf(addr.sun_path, sizeof addr.sun_path, "%s", c->unix_path);
        c->fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (c->fd < 0) {
            jc_set_error_errno(c, "socket");
            return -1;
        }
        if (connect(c->fd, (struct sockaddr *)&addr, sizeof addr) < 0) {
            jc_set_error_errno(c, "connect");
            close(c->fd);
            c->fd = -1;
            return -1;
        }
    } else {
        char port_text[16];
        snprintf(port_text, sizeof port_text, "%d", c->port);
        struct addrinfo hints;
        memset(&hints, 0, sizeof hints);
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        struct addrinfo *res = NULL;
        if (getaddrinfo(c->host, port_text, &hints, &res) != 0 || !res) {
            jc_set_error(c, "address resolution failed");
            return -1;
        }
        c->fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
        if (c->fd < 0) {
            jc_set_error_errno(c, "socket");
            freeaddrinfo(res);
            return -1;
        }
        if (connect(c->fd, res->ai_addr, res->ai_addrlen) < 0) {
            jc_set_error_errno(c, "connect");
            freeaddrinfo(res);
            close(c->fd);
            c->fd = -1;
            return -1;
        }
        freeaddrinfo(res);
        int one = 1;
        setsockopt(c->fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    }
    c->connected = 1;
#ifdef HAVE_OPENSSL
    if (c->use_tls) {
        SSL_CTX *ctx = SSL_CTX_new(TLS_client_method());
        if (!ctx) {
            jc_set_error(c, "TLS context failed");
            return -1;
        }
        SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);
        if (SSL_CTX_load_verify_locations(ctx, c->ca_file[0] ? c->ca_file : NULL,
                                          c->ca_file[0] ? NULL
                                                        : "/etc/ssl/certs") != 1) {
            SSL_CTX_free(ctx);
            jc_set_error(c, "TLS CA load failed");
            return -1;
        }
        SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, NULL);
        c->ssl = SSL_new(ctx);
        SSL_set_fd(c->ssl, c->fd);
        if (SSL_set1_host(c->ssl,
                          c->server_name[0] ? c->server_name : c->host) != 1 ||
            SSL_connect(c->ssl) != 1) {
            SSL_free(c->ssl);
            c->ssl = NULL;
            SSL_CTX_free(ctx);
            jc_set_error(c, "TLS handshake or hostname verification failed");
            return -1;
        }
        SSL_CTX_free(ctx); /* SSL_new took its own reference */
    }
#endif
    /* AUTH must be the first request on an authenticated connection. */
    if (c->auth_token_len) {
        unsigned char frame[7];
        frame[0] = 0x06;
        frame[1] = (unsigned char)(c->auth_token_len & 0xff);
        frame[2] = (unsigned char)(c->auth_token_len >> 8);
        memset(frame + 3, 0, 4);
        size_t at = 0;
        while (at < sizeof frame) {
            ssize_t n = write(c->fd, (char *)frame + at, sizeof frame - at);
            if (n < 0 && errno == EINTR) continue;
            if (n <= 0) {
                jc_set_error(c, "auth write failed");
                return -1;
            }
            at += (size_t)n;
        }
        at = 0;
        while (at < c->auth_token_len) {
            ssize_t n = write(c->fd, c->auth_token + at, c->auth_token_len - at);
            if (n < 0 && errno == EINTR) continue;
            if (n <= 0) {
                jc_set_error(c, "auth write failed");
                return -1;
            }
            at += (size_t)n;
        }
        unsigned char head[5];
        size_t got = 0;
        while (got < sizeof head) {
            ssize_t n = read(c->fd, head + got, sizeof head - got);
            if (n < 0 && errno == EINTR) continue;
            if (n <= 0) {
                jc_set_error(c, "auth read failed");
                return -1;
            }
            got += (size_t)n;
        }
        if (head[0] != 0x00) {
            jc_set_error(c, "authentication failed");
            return -1;
        }
    }
    return 0;
}

void kuttidb_client_destroy(KuttiDBClient *client) {
    if (!client) return;
#ifdef HAVE_OPENSSL
    if (client->ssl) {
        SSL_free(client->ssl);
    }
#endif
    if (client->fd >= 0) close(client->fd);
    free(client->value_buf);
    free(client);
}

KuttidbJobStatus kuttidb_client_reconnect(KuttiDBClient *client) {
    if (!client) return KUTTIDB_JOB_TRANSPORT_ERROR;
    if (client->fd >= 0) close(client->fd);
    client->fd = -1;
    client->connected = 0;
#ifdef HAVE_OPENSSL
    if (client->ssl) {
        SSL_free(client->ssl);
        client->ssl = NULL;
    }
#endif
    if (jc_connect_once(client) < 0) return KUTTIDB_JOB_TRANSPORT_ERROR;
    return KUTTIDB_JOB_OK;
}

/* ---- framed IO (blocking with an overall timeout) ---- */

static int jc_write_all(KuttiDBClient *c, const void *data, size_t len) {
    const unsigned char *p = (const unsigned char *)data;
    size_t at = 0;
    while (at < len) {
        ssize_t n;
#ifdef HAVE_OPENSSL
        if (c->ssl)
            n = SSL_write(c->ssl, p + at, (int)(len - at));
        else
#endif
            n = write(c->fd, p + at, len - at);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) {
            jc_set_error(c, "write failed");
            return -1;
        }
        at += (size_t)n;
    }
    return 0;
}

static int jc_read_all(KuttiDBClient *c, void *data, size_t len) {
    unsigned char *p = (unsigned char *)data;
    size_t at = 0;
    while (at < len) {
        ssize_t n;
#ifdef HAVE_OPENSSL
        if (c->ssl)
            n = SSL_read(c->ssl, p + at, (int)(len - at));
        else
#endif
        {
            struct pollfd pfd = {c->fd, POLLIN, 0};
            if (poll(&pfd, 1, c->timeout_ms) <= 0) {
                jc_set_error(c, "read timeout");
                return -1;
            }
            n = read(c->fd, p + at, len - at);
        }
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) {
            jc_set_error(c, "connection closed");
            return -1;
        }
        at += (size_t)n;
    }
    return 0;
}

static void jc_put_u16(unsigned char *p, unsigned v) {
    p[0] = (unsigned char)v;
    p[1] = (unsigned char)(v >> 8);
}

static unsigned jc_get_u16(const unsigned char *p) {
    return (unsigned)p[0] | ((unsigned)p[1] << 8);
}

static void jc_put_u64(unsigned char *p, uint64_t v) {
    for (int i = 0; i < 8; i++) p[i] = (unsigned char)(v >> (i * 8));
}

static uint64_t jc_get_u64(const unsigned char *p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) v |= (uint64_t)p[i] << (i * 8);
    return v;
}

static void jc_put_u32(unsigned char *p, uint32_t v) {
    for (int i = 0; i < 4; i++) p[i] = (unsigned char)(v >> (i * 8));
}

static uint32_t jc_get_u32(const unsigned char *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) |
           ((uint32_t)p[3] << 24);
}

/* One request/response round trip. The caller receives the response body
 * in c->value_buf (owned, reused per call). */
static KuttidbJobStatus jc_roundtrip(KuttiDBClient *c, unsigned char opcode,
                                     const char *key, uint32_t key_len,
                                     const unsigned char *value,
                                     uint32_t value_len) {
    if (!c || !c->connected) {
        jc_set_error(c, "not connected");
        return KUTTIDB_JOB_TRANSPORT_ERROR;
    }
    if (key_len > JC_KEY_MAX || value_len > JC_VALUE_MAX ||
        (key_len && !key) || (value_len && !value)) {
        jc_set_error(c, "invalid request spans");
        return KUTTIDB_JOB_VALIDATION_FAILED;
    }
    unsigned char header[7];
    header[0] = opcode;
    jc_put_u16(header + 1, key_len);
    jc_put_u32(header + 3, value_len);
    if (jc_write_all(c, header, sizeof header) < 0 ||
        (key_len && jc_write_all(c, key, key_len) < 0) ||
        (value_len && jc_write_all(c, value, value_len) < 0)) {
        c->connected = 0;
        return KUTTIDB_JOB_TRANSPORT_ERROR;
    }
    unsigned char head[5];
    if (jc_read_all(c, head, sizeof head) < 0) {
        c->connected = 0;
        return KUTTIDB_JOB_TRANSPORT_ERROR;
    }
    unsigned char status = head[0];
    uint32_t len = jc_get_u32(head + 1);
    if (len > JC_VALUE_MAX) {
        jc_set_error(c, "invalid response length");
        c->connected = 0;
        return KUTTIDB_JOB_TRANSPORT_ERROR;
    }
    unsigned char *buf = (unsigned char *)realloc(c->value_buf, len ? len : 1);
    if (!buf) {
        jc_set_error(c, "out of memory");
        return KUTTIDB_JOB_RESOURCE_EXHAUSTED;
    }
    c->value_buf = buf;
    c->value_len = len;
    if (len && jc_read_all(c, c->value_buf, len) < 0) {
        c->connected = 0;
        return KUTTIDB_JOB_TRANSPORT_ERROR;
    }
    if (status == 0x02) {
        /* Typed error envelope: [code:1][outcome:1][detail]. */
        unsigned code = len >= 1 ? c->value_buf[0] : 0;
        unsigned outcome = len >= 2 ? c->value_buf[1] : 0;
        if (outcome && code == 11) return KUTTIDB_JOB_OPERATION_IN_DOUBT;
        switch (code) {
        case 1: return KUTTIDB_JOB_UNSUPPORTED_FEATURE;
        case 2: return KUTTIDB_JOB_VALIDATION_FAILED;
        case 3: return KUTTIDB_JOB_REQUEST_TOO_LARGE;
        case 4: return KUTTIDB_JOB_IDEMPOTENCY_CONFLICT;
        case 5: return KUTTIDB_JOB_STATE_VERSION_CONFLICT;
        case 6: return KUTTIDB_JOB_DELIVERY_EXPIRED;
        case 7: return KUTTIDB_JOB_DELIVERY_NOT_OWNED;
        case 8: return KUTTIDB_JOB_RESOURCE_EXHAUSTED;
        case 10: return KUTTIDB_JOB_OPERATION_IN_DOUBT;
        case 11: return KUTTIDB_JOB_PERSISTENCE_UNAVAILABLE;
        case 12: return KUTTIDB_JOB_NOT_FOUND;
        default:
            jc_set_error(c, "server error");
            return KUTTIDB_JOB_TRANSPORT_ERROR;
        }
    }
    if (status == 0x01) return KUTTIDB_JOB_NOT_FOUND;
    if (status != 0x00) {
        jc_set_error(c, "unexpected response status");
        c->connected = 0;
        return KUTTIDB_JOB_TRANSPORT_ERROR;
    }
    return KUTTIDB_JOB_OK;
}

KuttidbJobStatus kuttidb_job_check_supported(KuttiDBClient *client,
                                             int *supported) {
    if (supported) *supported = 0;
    if (!client) return KUTTIDB_JOB_TRANSPORT_ERROR;
    if (!client->connected && jc_connect_once(client) < 0)
        return KUTTIDB_JOB_TRANSPORT_ERROR;
    unsigned char req[4];
    jc_put_u16(req, 1); /* protocol major */
    jc_put_u16(req + 2, 8);
    KuttidbJobStatus st = jc_roundtrip(client, 0x0a, NULL, 0, req, 4);
    if (st == KUTTIDB_JOB_NOT_FOUND) {
        jc_set_error(client, "incompatible protocol major version");
        return KUTTIDB_JOB_TRANSPORT_ERROR;
    }
    if (st != KUTTIDB_JOB_OK) return st;
    if (client->value_len != 12) {
        jc_set_error(client, "capability negotiation failed");
        return KUTTIDB_JOB_TRANSPORT_ERROR;
    }
    unsigned major = jc_get_u16(client->value_buf);
    uint64_t features = jc_get_u64(client->value_buf + 4);
    if (major != 1) {
        jc_set_error(client, "incompatible protocol major version");
        return KUTTIDB_JOB_TRANSPORT_ERROR;
    }
    if (supported) *supported = (features >> 16) & 1;
    return KUTTIDB_JOB_OK;
}

KuttidbJobStatus kuttidb_queue_manifest(KuttiDBClient *client,
                                        KuttiDBQueueManifest *out) {
    if (out) {
        out->entries = NULL;
        out->count = 0;
    }
    KuttidbJobStatus st = jc_roundtrip(client, 0x77, NULL, 0, NULL, 0);
    if (st != KUTTIDB_JOB_OK) return st;
    if (client->value_len < 2) {
        jc_set_error(client, "malformed manifest");
        return KUTTIDB_JOB_TRANSPORT_ERROR;
    }
    unsigned n = jc_get_u16(client->value_buf);
    uint32_t at = 2;
    KuttiDBQueueManifestEntry *entries =
        (KuttiDBQueueManifestEntry *)calloc(n ? n : 1, sizeof *entries);
    if (!entries) return KUTTIDB_JOB_RESOURCE_EXHAUSTED;
    for (unsigned i = 0; i < n; i++) {
        if (at + 2 > client->value_len) goto malformed;
        unsigned nlen = jc_get_u16(client->value_buf + at);
        at += 2;
        if (at + nlen + 1 + 40 > client->value_len) goto malformed;
        /* Name pointers reference the response buffer, which is reused per
         * call: copy them into a stable arena appended after the entries. */
        char *names = (char *)malloc(nlen ? nlen : 1);
        if (!names) goto malformed;
        memcpy(names, client->value_buf + at, nlen);
        at += nlen;
        entries[i].name = names;
        entries[i].name_len = nlen;
        entries[i].durable = client->value_buf[at] ? 1 : 0;
        at += 1;
        entries[i].incarnation = jc_get_u64(client->value_buf + at);
        entries[i].depth = jc_get_u64(client->value_buf + at + 8);
        entries[i].inflight = jc_get_u64(client->value_buf + at + 16);
        entries[i].max_depth = jc_get_u64(client->value_buf + at + 24);
        entries[i].revision = jc_get_u64(client->value_buf + at + 32);
        at += 40;
    }
    if (out) {
        out->entries = entries;
        out->count = n;
    }
    return KUTTIDB_JOB_OK;
malformed:
    for (unsigned i = 0; i < n; i++) free((void *)entries[i].name);
    free(entries);
    jc_set_error(client, "malformed manifest");
    return KUTTIDB_JOB_TRANSPORT_ERROR;
}

void kuttidb_queue_manifest_free(KuttiDBQueueManifest *manifest) {
    if (!manifest) return;
    for (uint32_t i = 0; i < manifest->count; i++)
        free((void *)manifest->entries[i].name);
    free(manifest->entries);
    manifest->entries = NULL;
    manifest->count = 0;
}

KuttidbJobStatus kuttidb_job_consume(KuttiDBClient *client,
                                     const char *queue, uint32_t queue_len,
                                     const char *consumer,
                                     uint32_t consumer_len,
                                     double visibility_seconds,
                                     KuttiDBJobDelivery *out) {
    if (out) memset(out, 0, sizeof *out);
    if (!queue || !queue_len || !consumer || !consumer_len ||
        consumer_len > 255 || visibility_seconds < 0) {
        jc_set_error(client, "invalid job consume request");
        return KUTTIDB_JOB_VALIDATION_FAILED;
    }
    unsigned char req[2 + 255 + 8];
    jc_put_u16(req, consumer_len);
    memcpy(req + 2, consumer, consumer_len);
    jc_put_u64(req + 2 + consumer_len,
               (uint64_t)(visibility_seconds * 1000.0));
    KuttidbJobStatus st = jc_roundtrip(client, 0x70, queue, queue_len, req,
                                       2 + consumer_len + 8);
    if (st != KUTTIDB_JOB_OK) return st;
    if (client->value_len < 61) {
        jc_set_error(client, "malformed delivery");
        return KUTTIDB_JOB_TRANSPORT_ERROR;
    }
    if (out) {
        memcpy(out->store_id, client->value_buf, 16);
        /* queue points at the caller's buffer. */
        out->queue = queue;
        out->queue_len = queue_len;
        out->queue_incarnation = jc_get_u64(client->value_buf + 16);
        out->message_id = jc_get_u64(client->value_buf + 24);
        out->attempts = jc_get_u32(client->value_buf + 32);
        out->redelivered = client->value_buf[36] ? 1 : 0;
        out->lease_deadline_ms = jc_get_u64(client->value_buf + 37);
        memcpy(out->proof, client->value_buf + 45, 16);
        out->value = client->value_buf + 61;
        out->value_len = client->value_len - 61;
    }
    return KUTTIDB_JOB_OK;
}

void kuttidb_job_delivery_free(KuttiDBJobDelivery *delivery) {
    /* The delivery references the client's response buffer; nothing to
     * release here. Kept for ABI stability and symmetric ownership docs. */
    (void)delivery;
}

KuttidbJobStatus kuttidb_job_complete(KuttiDBClient *client,
                                      const KuttiDBJobCompletion *request,
                                      KuttiDBJobCompletionResult *out) {
    if (out) memset(out, 0, sizeof *out);
    if (!request || !request->operation_id || !request->input_queue ||
        !request->input_queue_len || !request->proof || !request->state_key ||
        !request->state_key_len) {
        jc_set_error(client, "invalid completion request");
        return KUTTIDB_JOB_VALIDATION_FAILED;
    }
    if (request->output && (!request->output->queue ||
                            !request->output->queue_len)) {
        jc_set_error(client, "output request requires a queue");
        return KUTTIDB_JOB_VALIDATION_FAILED;
    }
    uint32_t cap = 16 + 8 + 8 + 16 + 2 + request->state_key_len + 8 + 4 +
                   request->state_value_len + 1;
    if (request->output)
        cap += 2 + request->output->queue_len + 8 + 4 +
               request->output->value_len;
    unsigned char *buf = (unsigned char *)malloc(cap);
    if (!buf) return KUTTIDB_JOB_RESOURCE_EXHAUSTED;
    uint32_t at = 0;
    memcpy(buf + at, request->operation_id, 16);
    at += 16;
    jc_put_u64(buf + at, request->input_incarnation);
    at += 8;
    jc_put_u64(buf + at, request->input_message_id);
    at += 8;
    memcpy(buf + at, request->proof, 16);
    at += 16;
    jc_put_u16(buf + at, request->state_key_len);
    at += 2;
    memcpy(buf + at, request->state_key, request->state_key_len);
    at += request->state_key_len;
    jc_put_u64(buf + at, request->expected_version);
    at += 8;
    jc_put_u32(buf + at, request->state_value_len);
    at += 4;
    if (request->state_value_len)
        memcpy(buf + at, request->state_value, request->state_value_len);
    at += request->state_value_len;
    if (request->output) {
        buf[at++] = 1;
        jc_put_u16(buf + at, request->output->queue_len);
        at += 2;
        memcpy(buf + at, request->output->queue, request->output->queue_len);
        at += request->output->queue_len;
        jc_put_u64(buf + at, request->output->queue_incarnation);
        at += 8;
        jc_put_u32(buf + at, request->output->value_len);
        at += 4;
        if (request->output->value_len)
            memcpy(buf + at, request->output->value,
                   request->output->value_len);
        at += request->output->value_len;
    } else {
        buf[at++] = 0;
    }
    KuttidbJobStatus st = jc_roundtrip(client, 0x71, request->input_queue,
                                       request->input_queue_len, buf, at);
    free(buf);
    if (st != KUTTIDB_JOB_OK) return st;
    if (client->value_len != 41) {
        jc_set_error(client, "malformed completion response");
        return KUTTIDB_JOB_TRANSPORT_ERROR;
    }
    if (out) {
        out->commit_id = jc_get_u64(client->value_buf);
        out->state_version = jc_get_u64(client->value_buf + 8);
        out->output_message_id = jc_get_u64(client->value_buf + 16);
        out->completed_at_ms = jc_get_u64(client->value_buf + 24);
        out->receipt_expires_at_ms = jc_get_u64(client->value_buf + 32);
        out->replayed = client->value_buf[40] ? 1 : 0;
    }
    return KUTTIDB_JOB_OK;
}

KuttidbJobStatus kuttidb_job_completion(KuttiDBClient *client,
                                        const unsigned char operation_id
                                            [KUTTIDB_JOB_ID_LEN],
                                        KuttiDBJobReceipt *out) {
    if (out) memset(out, 0, sizeof *out);
    KuttidbJobStatus st = jc_roundtrip(client, 0x72, NULL, 0, operation_id,
                                       KUTTIDB_JOB_ID_LEN);
    if (st != KUTTIDB_JOB_OK) return st;
    if (client->value_len != 41) {
        jc_set_error(client, "malformed receipt");
        return KUTTIDB_JOB_TRANSPORT_ERROR;
    }
    if (out) {
        memcpy(out->operation_id, operation_id, KUTTIDB_JOB_ID_LEN);
        out->commit_id = jc_get_u64(client->value_buf);
        out->state_version = jc_get_u64(client->value_buf + 8);
        out->output_message_id = jc_get_u64(client->value_buf + 16);
        out->completed_at_ms = jc_get_u64(client->value_buf + 24);
        out->receipt_expires_at_ms = jc_get_u64(client->value_buf + 32);
    }
    return KUTTIDB_JOB_OK;
}

KuttidbJobStatus kuttidb_state_get(KuttiDBClient *client, const char *key,
                                   uint32_t key_len, KuttiDBStateValue *out) {
    if (out) memset(out, 0, sizeof *out);
    KuttidbJobStatus st = jc_roundtrip(client, 0x73, key, key_len, NULL, 0);
    if (st != KUTTIDB_JOB_OK) return st;
    if (client->value_len < 16) {
        jc_set_error(client, "malformed state value");
        return KUTTIDB_JOB_TRANSPORT_ERROR;
    }
    if (out) {
        out->value_len = client->value_len - 16;
        out->value = (unsigned char *)malloc(out->value_len ? out->value_len : 1);
        if (!out->value) return KUTTIDB_JOB_RESOURCE_EXHAUSTED;
        if (out->value_len)
            memcpy(out->value, client->value_buf + 16, out->value_len);
        out->version = jc_get_u64(client->value_buf);
        out->commit_id = jc_get_u64(client->value_buf + 8);
    }
    return KUTTIDB_JOB_OK;
}

void kuttidb_state_value_free(KuttiDBStateValue *value) {
    if (!value) return;
    free(value->value);
    value->value = NULL;
    value->value_len = 0;
}

KuttidbJobStatus kuttidb_state_put(KuttiDBClient *client, const char *key,
                                   uint32_t key_len,
                                   const unsigned char *value,
                                   uint32_t value_len,
                                   uint64_t expected_version,
                                   const unsigned char operation_id
                                       [KUTTIDB_JOB_ID_LEN],
                                   KuttiDBJobMutationReceipt *out) {
    if (out) memset(out, 0, sizeof *out);
    if (!key || !key_len || (!value && value_len) || !operation_id) {
        jc_set_error(client, "invalid state put");
        return KUTTIDB_JOB_VALIDATION_FAILED;
    }
    uint32_t cap = 16 + 8 + value_len;
    unsigned char *buf = (unsigned char *)malloc(cap);
    if (!buf) return KUTTIDB_JOB_RESOURCE_EXHAUSTED;
    memcpy(buf, operation_id, 16);
    jc_put_u64(buf + 16, expected_version);
    if (value_len) memcpy(buf + 24, value, value_len);
    KuttidbJobStatus st = jc_roundtrip(client, 0x74, key, key_len, buf, cap);
    free(buf);
    if (st != KUTTIDB_JOB_OK) return st;
    if (client->value_len != 33) {
        jc_set_error(client, "malformed mutation receipt");
        return KUTTIDB_JOB_TRANSPORT_ERROR;
    }
    if (out) {
        memcpy(out->operation_id, operation_id, KUTTIDB_JOB_ID_LEN);
        out->commit_id = jc_get_u64(client->value_buf);
        out->state_version = jc_get_u64(client->value_buf + 8);
        out->completed_at_ms = jc_get_u64(client->value_buf + 16);
        out->receipt_expires_at_ms = jc_get_u64(client->value_buf + 24);
        out->replayed = client->value_buf[32] ? 1 : 0;
    }
    return KUTTIDB_JOB_OK;
}

KuttidbJobStatus kuttidb_state_delete(KuttiDBClient *client, const char *key,
                                      uint32_t key_len,
                                      uint64_t expected_version,
                                      const unsigned char operation_id
                                          [KUTTIDB_JOB_ID_LEN],
                                      KuttiDBJobMutationReceipt *out) {
    if (out) memset(out, 0, sizeof *out);
    if (!key || !key_len || expected_version == 0 || !operation_id) {
        jc_set_error(client, "invalid state delete");
        return KUTTIDB_JOB_VALIDATION_FAILED;
    }
    unsigned char buf[24];
    memcpy(buf, operation_id, 16);
    jc_put_u64(buf + 16, expected_version);
    KuttidbJobStatus st = jc_roundtrip(client, 0x75, key, key_len, buf,
                                       sizeof buf);
    if (st != KUTTIDB_JOB_OK) return st;
    if (client->value_len != 33) {
        jc_set_error(client, "malformed mutation receipt");
        return KUTTIDB_JOB_TRANSPORT_ERROR;
    }
    if (out) {
        memcpy(out->operation_id, operation_id, KUTTIDB_JOB_ID_LEN);
        out->commit_id = jc_get_u64(client->value_buf);
        out->state_version = jc_get_u64(client->value_buf + 8);
        out->completed_at_ms = jc_get_u64(client->value_buf + 16);
        out->receipt_expires_at_ms = jc_get_u64(client->value_buf + 24);
        out->replayed = client->value_buf[32] ? 1 : 0;
    }
    return KUTTIDB_JOB_OK;
}

KuttidbJobStatus kuttidb_durable_operation(KuttiDBClient *client,
                                           const unsigned char operation_id
                                               [KUTTIDB_JOB_ID_LEN],
                                           KuttiDBDurableOperation *out) {
    if (out) memset(out, 0, sizeof *out);
    KuttidbJobStatus st = jc_roundtrip(client, 0x76, NULL, 0, operation_id,
                                       KUTTIDB_JOB_ID_LEN);
    if (st != KUTTIDB_JOB_OK) return st;
    if (client->value_len != 33) {
        jc_set_error(client, "malformed durable operation");
        return KUTTIDB_JOB_TRANSPORT_ERROR;
    }
    if (out) {
        memcpy(out->operation_id, operation_id, KUTTIDB_JOB_ID_LEN);
        out->kind = client->value_buf[0];
        out->commit_id = jc_get_u64(client->value_buf + 1);
        out->state_version = jc_get_u64(client->value_buf + 9);
        out->completed_at_ms = jc_get_u64(client->value_buf + 17);
        out->receipt_expires_at_ms = jc_get_u64(client->value_buf + 25);
    }
    return KUTTIDB_JOB_OK;
}
