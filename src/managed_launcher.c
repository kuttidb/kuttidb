#define _POSIX_C_SOURCE 200809L
#include "managed_launcher.h"
#include "instance_lock.h"

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <time.h>
#include <unistd.h>

#define ENSURE_BAD_CONFIG 64
#define ENSURE_UNSAFE_PATH 65
#define ENSURE_OWNERSHIP 66
#define ENSURE_STARTUP_FAILED 67
#define ENSURE_TIMEOUT 68
#define MANAGED_LOG_MAX_BYTES (5 * 1024 * 1024)
#define FAILURE_COOLDOWN_MS 1000

static long long monotonic_ms(void) { struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts); return (long long)ts.tv_sec * 1000 + ts.tv_nsec / 1000000; }
static long long unix_ms(void) { struct timespec ts; clock_gettime(CLOCK_REALTIME, &ts); return (long long)ts.tv_sec * 1000 + ts.tv_nsec / 1000000; }
static int parse_ms(const char *text, unsigned long long *out) { char *end = NULL; errno = 0; unsigned long long value = strtoull(text, &end, 10); if (errno || !text[0] || *end || value > 3600000ull) return -1; *out = value; return 0; }

static int private_regular_fd(const char *path, int flags) {
    int fd = open(path, flags | O_NOFOLLOW | O_CLOEXEC, 0600);
    if (fd < 0) return -1;
    struct stat st;
    if (fstat(fd, &st) < 0 || !S_ISREG(st.st_mode) || st.st_uid != geteuid() ||
        (st.st_mode & 0077) || fchmod(fd, 0600) < 0) {
        int saved = errno ? errno : EPERM;
        close(fd); errno = saved; return -1;
    }
    return fd;
}

/* The managed daemon must not accidentally keep an application's sockets,
 * pipes, or files alive.  `ready_fd` is the only descriptor deliberately
 * carried over to the normal server image; stdin/out/err have already been
 * redirected before this runs. */
static void close_inherited_fds(int ready_fd) {
    long limit = sysconf(_SC_OPEN_MAX);
    if (limit < 0 || limit > 65536) limit = 65536;
    for (int fd = 3; fd < limit; fd++)
        if (fd != ready_fd) close(fd);
}

static int rotate_log(const char *path) {
    int fd = private_regular_fd(path, O_RDONLY);
    if (fd < 0) return errno == ENOENT ? 0 : -1;
    struct stat st;
    int large = fstat(fd, &st) == 0 && st.st_size > MANAGED_LOG_MAX_BYTES;
    close(fd);
    if (!large) return 0;
    char previous[1056];
    if (snprintf(previous, sizeof previous, "%s.1", path) >= (int)sizeof previous) {
        errno = ENAMETOOLONG; return -1;
    }
    fd = private_regular_fd(previous, O_RDONLY);
    if (fd >= 0) {
        close(fd);
        if (unlink(previous) < 0) return -1;
    } else if (errno != ENOENT) return -1;
    return rename(path, previous);
}

/* A short-lived hint, not an ownership mechanism.  The record contains only
 * a normalized configuration digest and an exit category—never arguments or
 * credentials—so concurrent callers do not fork-storm on a broken launch. */
static unsigned long long config_digest(const char *data_dir, const char *listen,
                                        const char *const *forward, size_t forward_count) {
    const char *parts[] = {data_dir, listen};
    unsigned long long h = 1469598103934665603ull;
    for (size_t i = 0; i < sizeof parts / sizeof parts[0]; i++) {
        for (const unsigned char *p = (const unsigned char *)parts[i]; *p; p++) { h ^= *p; h *= 1099511628211ull; }
        h ^= 0xff; h *= 1099511628211ull;
    }
    for (size_t i = 0; i < forward_count; i++) {
        for (const unsigned char *p = (const unsigned char *)forward[i]; *p; p++) { h ^= *p; h *= 1099511628211ull; }
        h ^= 0xff; h *= 1099511628211ull;
    }
    return h;
}

static int cooldown_path(const char *data_dir, char *out, size_t out_size) {
    if (snprintf(out, out_size, "%s/.startup-failure", data_dir) >= (int)out_size) { errno = ENAMETOOLONG; return -1; }
    return 0;
}

static int cooldown_read(const char *data_dir, unsigned long long digest) {
    char path[1024], buf[160] = {0};
    if (cooldown_path(data_dir, path, sizeof path) < 0) return 0;
    int fd = private_regular_fd(path, O_RDONLY);
    if (fd < 0) return 0;
    ssize_t n = read(fd, buf, sizeof buf - 1); close(fd);
    unsigned long long saved_digest = 0; long long until = 0; int status = 0;
    if (n > 0 && sscanf(buf, "format=1\ndigest=%llx\nuntil_unix_ms=%lld\nstatus=%d", &saved_digest, &until, &status) == 3 &&
        saved_digest == digest && until > unix_ms() && status >= ENSURE_BAD_CONFIG && status <= ENSURE_TIMEOUT)
        return status;
    return 0;
}

static int cooldown_write(const char *data_dir, unsigned long long digest, int status) {
    char path[1024];
    if (cooldown_path(data_dir, path, sizeof path) < 0) return status;
    int fd = private_regular_fd(path, O_WRONLY | O_CREAT | O_TRUNC);
    if (fd >= 0) {
        dprintf(fd, "format=1\ndigest=%016llx\nuntil_unix_ms=%lld\nstatus=%d\n", digest,
                unix_ms() + FAILURE_COOLDOWN_MS, status);
        fsync(fd); close(fd);
    }
    return status;
}

static void cooldown_clear(const char *data_dir) {
    char path[1024];
    if (cooldown_path(data_dir, path, sizeof path) == 0) (void)unlink(path);
}

/* 1 connected, 0 definitely absent, -1 occupied/unavailable. */
static int probe_unix(const char *path) {
    int fd = socket(AF_UNIX, SOCK_STREAM, 0); if (fd < 0) return -1;
    struct sockaddr_un addr = {0}; addr.sun_family = AF_UNIX;
    if (strlen(path) >= sizeof addr.sun_path) { close(fd); errno = ENAMETOOLONG; return -1; }
    strcpy(addr.sun_path, path);
    int rc = connect(fd, (struct sockaddr *)&addr, sizeof addr);
    int saved = errno; close(fd);
    if (rc == 0) return 1;
    if (saved == ENOENT || saved == ECONNREFUSED) return 0;
    errno = saved; return -1;
}

static int parse_loopback_tcp(const char *text, struct sockaddr_in *out) {
    const char *colon = strrchr(text, ':');
    char host[INET_ADDRSTRLEN], *end = NULL;
    unsigned long port;
    size_t host_len = colon ? (size_t)(colon - text) : 0;
    if (!colon || host_len == 0 || host_len >= sizeof host) return -1;
    memcpy(host, text, host_len); host[host_len] = 0;
    errno = 0; port = strtoul(colon + 1, &end, 10);
    if (errno || !colon[1] || *end || port == 0 || port > 65535) return -1;
    memset(out, 0, sizeof *out); out->sin_family = AF_INET; out->sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, host, &out->sin_addr) != 1 ||
        (ntohl(out->sin_addr.s_addr) >> 24) != 127) return -1;
    return 0;
}

static int probe_tcp(const struct sockaddr_in *addr) {
    int fd = socket(AF_INET, SOCK_STREAM, 0); if (fd < 0) return -1;
    int rc = connect(fd, (const struct sockaddr *)addr, sizeof *addr);
    int saved = errno; close(fd);
    if (rc == 0) return 1;
    if (saved == ECONNREFUSED) return 0;
    errno = saved; return -1;
}

static void emit_json_string(const char *text) {
    putchar('"');
    for (const unsigned char *p = (const unsigned char *)text; *p; p++) {
        if (*p == '"' || *p == '\\') { putchar('\\'); putchar(*p); }
        else if (*p < 0x20) printf("\\u%04x", *p);
        else putchar(*p);
    }
    putchar('"');
}

static void emit_json(const char *status, const char *id, const char *endpoint, pid_t pid, const char *log) {
    printf("{\"status\":"); emit_json_string(status);
    printf(",\"instance_id\":"); emit_json_string(id);
    printf(",\"endpoint\":"); emit_json_string(endpoint);
    printf(",\"pid\":%ld,\"log\":", (long)pid); emit_json_string(log);
    puts("}");
    fflush(stdout);
}

int managed_launcher_maybe_run(int argc, char **argv) {
    if (argc < 2 || strcmp(argv[1], "ensure") != 0) return -1;
    const char *data_dir = NULL, *listen = NULL;
    /* This is deliberately an allowlist of server settings, never a generic
     * argv pass-through.  Lifecycle, listener, ready-fd and data-dir stay
     * owned by ensure so a client cannot subvert managed ownership. */
    const char *forward[80]; size_t forward_count = 0;
    unsigned long long idle_ms = 60000, orphan_ms = 60000, startup_ms = 10000;
    int json = 0;
    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "--json") == 0) { json = 1; continue; }
        if (strcmp(argv[i], "--data-dir") == 0 || strcmp(argv[i], "--listen") == 0 ||
            strcmp(argv[i], "--idle-timeout-ms") == 0 || strcmp(argv[i], "--startup-timeout-ms") == 0 ||
            strcmp(argv[i], "--startup-orphan-timeout-ms") == 0 ||
            strcmp(argv[i], "--durability") == 0 || strcmp(argv[i], "--auth-file") == 0 ||
            strcmp(argv[i], "--max-memory-mb") == 0 || strcmp(argv[i], "--max-value-mb") == 0 ||
            strcmp(argv[i], "--max-batch-mb") == 0 || strcmp(argv[i], "--max-clients") == 0 ||
            strcmp(argv[i], "--threads") == 0 || strcmp(argv[i], "--fsync-ms") == 0 ||
            strcmp(argv[i], "--queue-wal") == 0 || strcmp(argv[i], "--stream-wal") == 0 ||
            strcmp(argv[i], "--tls-cert") == 0 || strcmp(argv[i], "--tls-key") == 0 ||
            strcmp(argv[i], "--metrics-bind") == 0 || strcmp(argv[i], "--metrics-token-file") == 0 ||
            strcmp(argv[i], "--admin-bind") == 0 || strcmp(argv[i], "--admin-token-file") == 0 ||
            strcmp(argv[i], "--admin-allow-origin") == 0 || strcmp(argv[i], "--admin-tls-cert") == 0 ||
            strcmp(argv[i], "--admin-tls-key") == 0 || strcmp(argv[i], "--admin-audit-log") == 0 ||
            strcmp(argv[i], "--admin-max-clients") == 0 || strcmp(argv[i], "--admin-max-tail-clients") == 0 ||
            strcmp(argv[i], "--admin-session-limit") == 0 || strcmp(argv[i], "--admin-job-limit") == 0) {
            if (++i >= argc) return ENSURE_BAD_CONFIG;
            const char *flag = argv[i - 1];
            const char *v = argv[i];
            if (strcmp(flag, "--data-dir") == 0) data_dir = v;
            else if (strcmp(flag, "--listen") == 0) listen = v;
            else if (strcmp(flag, "--durability") == 0) { if (strcmp(v, "periodic") && strcmp(v, "always")) return ENSURE_BAD_CONFIG; }
            else if (strcmp(flag, "--idle-timeout-ms") == 0 || strcmp(flag, "--startup-timeout-ms") == 0 || strcmp(flag, "--startup-orphan-timeout-ms") == 0) {
                if (parse_ms(v, strcmp(flag, "--idle-timeout-ms") == 0 ? &idle_ms : strcmp(flag, "--startup-timeout-ms") == 0 ? &startup_ms : &orphan_ms) < 0) return ENSURE_BAD_CONFIG;
            } else {
                if (forward_count + 2 > sizeof forward / sizeof forward[0]) return ENSURE_BAD_CONFIG;
                forward[forward_count++] = flag;
                forward[forward_count++] = v;
            }
            continue;
        }
        return ENSURE_BAD_CONFIG;
    }
    if (!data_dir || data_dir[0] != '/' || !listen || !idle_ms || !orphan_ms || !startup_ms) return ENSURE_BAD_CONFIG;
    int is_unix = strncmp(listen, "unix:", 5) == 0 && listen[5] == '/';
    struct sockaddr_in tcp_addr;
    if (!is_unix && (strncmp(listen, "tcp:", 4) != 0 || parse_loopback_tcp(listen + 4, &tcp_addr) < 0))
        return ENSURE_BAD_CONFIG;
    const char *socket_path = is_unix ? listen + 5 : NULL;
    char expected_socket[1024], bootstrap[1024], log_path[1024];
    if ((is_unix && (snprintf(expected_socket, sizeof expected_socket, "%s/kuttidb.sock", data_dir) >= (int)sizeof expected_socket ||
                     strcmp(expected_socket, socket_path) != 0)) ||
        snprintf(bootstrap, sizeof bootstrap, "%s/.bootstrap.lock", data_dir) >= (int)sizeof bootstrap ||
        snprintf(log_path, sizeof log_path, "%s/kuttidb.log", data_dir) >= (int)sizeof log_path)
        return ENSURE_BAD_CONFIG;
    if (instance_secure_dir(data_dir) < 0) { perror("managed data directory"); return ENSURE_UNSAFE_PATH; }
    char instance_id[33]; if (instance_load_or_create_id(data_dir, instance_id) < 0) { perror("managed instance identity"); return ENSURE_UNSAFE_PATH; }
    unsigned long long digest = config_digest(data_dir, listen, forward, forward_count);
    int endpoint = is_unix ? probe_unix(socket_path) : probe_tcp(&tcp_addr);
    if (endpoint == 1) { cooldown_clear(data_dir); if (json) emit_json("existing", instance_id, listen, 0, log_path); return 0; }
    if (endpoint < 0) return ENSURE_OWNERSHIP;
    int lock_fd = private_regular_fd(bootstrap, O_RDWR | O_CREAT);
    if (lock_fd < 0) { perror("managed bootstrap lock"); return ENSURE_UNSAFE_PATH; }
    fcntl(lock_fd, F_SETFD, FD_CLOEXEC);
    long long deadline = monotonic_ms() + (long long)startup_ms;
    while (flock(lock_fd, LOCK_EX | LOCK_NB) < 0) {
        if (errno != EWOULDBLOCK && errno != EAGAIN) { close(lock_fd); return ENSURE_OWNERSHIP; }
        endpoint = is_unix ? probe_unix(socket_path) : probe_tcp(&tcp_addr);
        if (endpoint == 1) { cooldown_clear(data_dir); close(lock_fd); if (json) emit_json("existing", instance_id, listen, 0, log_path); return 0; }
        if (monotonic_ms() >= deadline) { close(lock_fd); return ENSURE_TIMEOUT; }
        struct timespec pause = {0, 20 * 1000 * 1000}; nanosleep(&pause, NULL);
    }
    endpoint = is_unix ? probe_unix(socket_path) : probe_tcp(&tcp_addr);
    if (endpoint == 1) { cooldown_clear(data_dir); close(lock_fd); if (json) emit_json("existing", instance_id, listen, 0, log_path); return 0; }
    if (endpoint < 0) { close(lock_fd); return ENSURE_OWNERSHIP; }
    int cooled = cooldown_read(data_dir, digest);
    if (cooled) { close(lock_fd); return cooled; }
    if (rotate_log(log_path) < 0) { perror("managed log rotation"); close(lock_fd); return ENSURE_UNSAFE_PATH; }
launch_child: ;
    /* A graceful predecessor may unlink its socket before its persistence
     * locks are released.  Keep bootstrap ownership while a successor waits
     * for that narrow hand-off instead of reporting a false startup failure. */
    int ready[2]; if (pipe(ready) < 0) { close(lock_fd); return cooldown_write(data_dir, digest, ENSURE_STARTUP_FAILED); }
    fcntl(ready[0], F_SETFD, FD_CLOEXEC);
    pid_t pid = fork();
    if (pid < 0) { close(ready[0]); close(ready[1]); close(lock_fd); return cooldown_write(data_dir, digest, ENSURE_STARTUP_FAILED); }
    if (pid == 0) {
        close(ready[0]); setsid();
        int nullfd = open("/dev/null", O_RDONLY); if (nullfd >= 0) { dup2(nullfd, STDIN_FILENO); if (nullfd > 2) close(nullfd); }
        int logfd = private_regular_fd(log_path, O_WRONLY | O_CREAT | O_APPEND);
        if (logfd < 0) { dprintf(ready[1], "ERR log\n"); _exit(126); }
        dup2(logfd, STDOUT_FILENO); dup2(logfd, STDERR_FILENO); if (logfd > 2) close(logfd);
        char ready_fd[32], idle[32], orphan[32]; snprintf(ready_fd, sizeof ready_fd, "%d", ready[1]); snprintf(idle, sizeof idle, "%llu", idle_ms); snprintf(orphan, sizeof orphan, "%llu", orphan_ms);
        char *child_argv[112]; size_t at = 0;
        child_argv[at++] = argv[0]; child_argv[at++] = "--data-dir"; child_argv[at++] = (char *)data_dir;
        child_argv[at++] = "--listen"; child_argv[at++] = (char *)listen;
        child_argv[at++] = "--lifecycle"; child_argv[at++] = "managed-idle"; child_argv[at++] = "--idle-timeout-ms"; child_argv[at++] = idle;
        child_argv[at++] = "--startup-orphan-timeout-ms"; child_argv[at++] = orphan;
        for (size_t i = 0; i < forward_count; i++) child_argv[at++] = (char *)forward[i];
        child_argv[at++] = "--ready-fd"; child_argv[at++] = ready_fd; child_argv[at] = NULL;
        close_inherited_fds(ready[1]);
        execv(argv[0], child_argv);
        dprintf(ready[1], "ERR exec\n"); _exit(127);
    }
    close(ready[1]);
    char result[64] = {0};
    struct pollfd pfd = {.fd = ready[0], .events = POLLIN};
    int remaining = (int)(deadline - monotonic_ms());
    int poll_rc = poll(&pfd, 1, remaining > 0 ? remaining : 0);
    ssize_t n = poll_rc > 0 ? read(ready[0], result, sizeof result - 1) : -1;
    close(ready[0]);
    if (n > 0 && strncmp(result, "READY 1", 7) == 0) { cooldown_clear(data_dir); close(lock_fd); if (json) emit_json("started", instance_id, listen, pid, log_path); return 0; }
    int status = 0;
    pid_t reaped = waitpid(pid, &status, WNOHANG);
    if (reaped == pid && WIFEXITED(status) && WEXITSTATUS(status) == 73 &&
        monotonic_ms() < deadline) {
        endpoint = is_unix ? probe_unix(socket_path) : probe_tcp(&tcp_addr);
        if (endpoint == 1) {
            cooldown_clear(data_dir);
            close(lock_fd);
            if (json) emit_json("existing", instance_id, listen, 0, log_path);
            return 0;
        }
        if (endpoint == 0) {
            struct timespec pause = {0, 20 * 1000 * 1000};
            nanosleep(&pause, NULL);
            goto launch_child;
        }
    }
    int failure = poll_rc == 0 ? ENSURE_TIMEOUT : ENSURE_STARTUP_FAILED;
    close(lock_fd);
    return cooldown_write(data_dir, digest, failure);
}
