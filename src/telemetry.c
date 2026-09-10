#define _POSIX_C_SOURCE 200809L
#include "telemetry.h"
#include <string.h>

#ifdef HAVE_TELEMETRY
#include <curl/curl.h>
#include <openssl/hmac.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

static pthread_t reporter_thread;
static int reporter_started;
static pthread_mutex_t reporter_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t reporter_cv = PTHREAD_COND_INITIALIZER;
static int reporter_stopping;
static const _Atomic unsigned int *reporter_connections;
static const volatile sig_atomic_t *reporter_stop_flag;
static char reporter_endpoint[2049];
static char reporter_state_dir[1024];

static int canonical_endpoint(char output[2049], const char *endpoint) {
    const char *authority = endpoint + 8;
    const char *path = strchr(authority, '/');
    size_t authority_length = (size_t)(path - authority);
    if (authority_length > 4 && memcmp(authority + authority_length - 4, ":443", 4) == 0) authority_length -= 4;
    size_t path_length = strlen(path);
    if (8 + authority_length + path_length >= 2049) return -1;
    memcpy(output, "https://", 8);
    for (size_t i = 0; i < authority_length; ++i) {
        unsigned char c = (unsigned char)authority[i];
        output[8 + i] = (char)((c >= 'A' && c <= 'Z') ? c + ('a' - 'A') : c);
    }
    memcpy(output + 8 + authority_length, path, path_length + 1);
    return 0;
}

int telemetry_endpoint_valid(const char *endpoint) {
    if (!endpoint || strncmp(endpoint, "https://", 8) != 0) return 0;
    size_t length = strlen(endpoint);
    if (length <= 8 || length >= sizeof reporter_endpoint) return 0;
    const char *authority = endpoint + 8;
    const char *path = strchr(authority, '/');
    size_t authority_length = path ? (size_t)(path - authority) : strlen(authority);
    if (!path || !authority_length || authority_length > 255 || memchr(authority, '@', authority_length)) return 0;
    for (size_t i = 0; i < length; ++i) {
        unsigned char c = (unsigned char)endpoint[i];
        if (c < 0x21 || c == 0x7f || c == '?' || c == '#') return 0;
    }
    return 1;
}

static const char *bucket(unsigned int n) {
    if (!n) return "0"; if (n == 1) return "1"; if (n <= 5) return "2-5";
    if (n <= 20) return "6-20"; if (n <= 100) return "21-100";
    if (n <= 1000) return "101-1000"; return "1001+";
}

static int make_state(char id[33]) {
    if (mkdir(reporter_state_dir, 0700) < 0 && errno != EEXIST) return -1;
    struct stat st;
    if (lstat(reporter_state_dir, &st) < 0 || !S_ISDIR(st.st_mode) || st.st_uid != geteuid() || (st.st_mode & 0077)) return -1;
    char path[1100];
    if (snprintf(path, sizeof path, "%s/seed", reporter_state_dir) >= (int)sizeof path) return -1;
    int fd = open(path, O_RDWR | O_CREAT | O_NOFOLLOW | O_CLOEXEC, 0600);
    if (fd < 0) return -1;
    if (fstat(fd, &st) < 0 || !S_ISREG(st.st_mode) || st.st_uid != geteuid() || (st.st_mode & 0077)) { close(fd); return -1; }
    unsigned char seed[32]; ssize_t n = read(fd, seed, sizeof seed);
    if (n == 0) {
        int random = open("/dev/urandom", O_RDONLY | O_CLOEXEC);
        if (random < 0 || read(random, seed, sizeof seed) != (ssize_t)sizeof seed) { if (random >= 0) close(random); close(fd); return -1; }
        close(random);
        if (pwrite(fd, seed, sizeof seed, 0) != (ssize_t)sizeof seed || fsync(fd) < 0) { close(fd); return -1; }
    } else if (n != (ssize_t)sizeof seed) { close(fd); return -1; }
    if (fchmod(fd, 0600) < 0) { close(fd); return -1; }
    close(fd);
    unsigned char digest[32]; unsigned int digest_len = 0;
    HMAC(EVP_sha256(), seed, sizeof seed, (const unsigned char *)reporter_endpoint, strlen(reporter_endpoint), digest, &digest_len);
    if (digest_len < 16) return -1;
    for (int i = 0; i < 16; i++) sprintf(id + i * 2, "%02x", digest[i]);
    id[32] = 0;
    return 0;
}

static int lock_state(void) {
    if (mkdir(reporter_state_dir, 0700) < 0 && errno != EEXIST) return -1;
    struct stat directory;
    if (lstat(reporter_state_dir, &directory) < 0 || !S_ISDIR(directory.st_mode) || directory.st_uid != geteuid() || (directory.st_mode & 0077)) return -1;
    char path[1100];
    if (snprintf(path, sizeof path, "%s/lock", reporter_state_dir) >= (int)sizeof path) return -1;
    int fd = open(path, O_RDWR | O_CREAT | O_NOFOLLOW | O_CLOEXEC, 0600);
    if (fd < 0) return -1;
    struct stat st;
    struct flock lock = { .l_type = F_WRLCK, .l_whence = SEEK_SET };
    if (fstat(fd, &st) < 0 || !S_ISREG(st.st_mode) || st.st_uid != geteuid() || (st.st_mode & 0077) || fcntl(fd, F_SETLK, &lock) < 0) {
        close(fd); return -1;
    }
    return fd;
}

/* State contains only the local seed and a next eligible Unix second. Reserve
 * the next slot before sending so an ordinary restart cannot cause a burst. */
static time_t reserve_next_slot(time_t now, time_t initial_delay) {
    char path[1100], data[64] = {0};
    if (snprintf(path, sizeof path, "%s/next", reporter_state_dir) >= (int)sizeof path) return 0;
    int fd = open(path, O_RDWR | O_CREAT | O_NOFOLLOW | O_CLOEXEC, 0600);
    if (fd < 0) return 0;
    struct stat st;
    if (fstat(fd, &st) < 0 || !S_ISREG(st.st_mode) || st.st_uid != geteuid() || (st.st_mode & 0077)) { close(fd); return 0; }
    ssize_t n = read(fd, data, sizeof data - 1);
    char *end = NULL;
    long long saved = n > 0 ? strtoll(data, &end, 10) : 0;
    time_t eligible = (n > 0 && end && (*end == '\n' || *end == 0) && saved > now && saved <= now + 26 * 60 * 60)
        ? (time_t)saved : now + initial_delay;
    time_t following = eligible <= now ? now + 24 * 60 * 60 : eligible;
    if (ftruncate(fd, 0) < 0 || lseek(fd, 0, SEEK_SET) < 0 || dprintf(fd, "%lld\n", (long long)following) < 0 || fsync(fd) < 0) {
        close(fd); return 0;
    }
    close(fd);
    return eligible;
}

static size_t discard_response(char *data, size_t size, size_t count, void *unused) {
    (void)data; (void)unused; return size * count <= 1024 ? size * count : 0;
}

static void *reporter_main(void *unused) {
    (void)unused;
    char id[33];
    int lock_fd = lock_state();
    if (lock_fd < 0) { fprintf(stderr, "telemetry disabled: telemetry state is unavailable or already in use\n"); return NULL; }
    if (make_state(id) < 0) { fprintf(stderr, "telemetry disabled: cannot create private state\n"); close(lock_fd); return NULL; }
    time_t eligible = reserve_next_slot(time(NULL), 15 * 60);
    if (!eligible) { fprintf(stderr, "telemetry disabled: cannot reserve report schedule\n"); close(lock_fd); return NULL; }
    for (;;) {
        struct timespec until = { .tv_sec = eligible, .tv_nsec = 0 };
        pthread_mutex_lock(&reporter_mu);
        while (!reporter_stopping && !*reporter_stop_flag && pthread_cond_timedwait(&reporter_cv, &reporter_mu, &until) == 0) {}
        int stop = reporter_stopping || *reporter_stop_flag;
        pthread_mutex_unlock(&reporter_mu);
        if (stop) break;
        /* Reserve before I/O. A failed report waits for the normal next slot. */
        (void)reserve_next_slot(time(NULL), 24 * 60 * 60);
        unsigned int connections = atomic_load(reporter_connections);
        char body[256];
        int length = snprintf(body, sizeof body, "{\"schema_version\":1,\"installation_id\":\"%s\",\"native_connections_bucket\":\"%s\"}", id, bucket(connections));
        if (length <= 0 || length >= (int)sizeof body) continue;
        CURL *curl = curl_easy_init();
        if (!curl) continue;
        struct curl_slist *headers = curl_slist_append(NULL, "Content-Type: application/json");
        curl_easy_setopt(curl, CURLOPT_URL, reporter_endpoint);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (long)length);
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        curl_easy_setopt(curl, CURLOPT_USERAGENT, "KuttiDB-Telemetry/1");
        curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, 1000L);
        curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, 3000L);
        curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
        curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 0L);
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, discard_response);
        (void)curl_easy_perform(curl);
        curl_slist_free_all(headers); curl_easy_cleanup(curl);
        eligible = reserve_next_slot(time(NULL), 24 * 60 * 60);
        if (!eligible) break;
    }
    close(lock_fd);
    return NULL;
}

int telemetry_start(const TelemetryConfig *config, const _Atomic unsigned int *connections, const volatile sig_atomic_t *stop_flag) {
    if (!config || !config->enabled) return 0;
    if (!telemetry_endpoint_valid(config->endpoint) ||
        !config->state_dir || config->state_dir[0] != '/' || strlen(config->state_dir) >= sizeof reporter_state_dir) return -1;
    if (canonical_endpoint(reporter_endpoint, config->endpoint) < 0) return -1;
    strcpy(reporter_state_dir, config->state_dir);
    reporter_connections = connections; reporter_stop_flag = stop_flag; reporter_stopping = 0;
    if (curl_global_init(CURL_GLOBAL_DEFAULT) != CURLE_OK) return -1;
    if (pthread_create(&reporter_thread, NULL, reporter_main, NULL) != 0) { curl_global_cleanup(); return -1; }
    reporter_started = 1;
    fprintf(stderr, "telemetry enabled: daily opt-in reports to %s; disable with --telemetry off or DO_NOT_TRACK=1\n", reporter_endpoint);
    return 0;
}
void telemetry_stop(void) {
    if (!reporter_started) return;
    pthread_mutex_lock(&reporter_mu); reporter_stopping = 1; pthread_cond_broadcast(&reporter_cv); pthread_mutex_unlock(&reporter_mu);
    pthread_join(reporter_thread, NULL); reporter_started = 0; curl_global_cleanup();
}
#else
int telemetry_endpoint_valid(const char *endpoint) {
    if (!endpoint || strncmp(endpoint, "https://", 8) != 0) return 0;
    size_t length = strlen(endpoint);
    if (length <= 8 || length >= 2049) return 0;
    const char *authority = endpoint + 8;
    const char *path = strchr(authority, '/');
    size_t authority_length = path ? (size_t)(path - authority) : strlen(authority);
    if (!path || !authority_length || authority_length > 255 || memchr(authority, '@', authority_length)) return 0;
    for (size_t i = 0; i < length; ++i) {
        unsigned char c = (unsigned char)endpoint[i];
        if (c < 0x21 || c == 0x7f || c == '?' || c == '#') return 0;
    }
    return 1;
}
int telemetry_start(const TelemetryConfig *config, const _Atomic unsigned int *connections, const volatile sig_atomic_t *stop_flag) { (void)config; (void)connections; (void)stop_flag; return config && config->enabled ? -1 : 0; }
void telemetry_stop(void) {}
#endif
