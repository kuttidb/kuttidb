#define _POSIX_C_SOURCE 200809L
#include "instance_lock.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

static int verify_dir(const char *path) {
    struct stat st;
    if (lstat(path, &st) < 0 || !S_ISDIR(st.st_mode) || st.st_uid != geteuid() ||
        (st.st_mode & 0077)) {
        errno = EPERM;
        return -1;
    }
    return 0;
}

int instance_secure_dir(const char *path) {
    if (!path || !*path || path[0] != '/') { errno = EINVAL; return -1; }
    char work[PATH_MAX];
    if (strlen(path) >= sizeof work) { errno = ENAMETOOLONG; return -1; }
    strcpy(work, path);
    /* Build absent parents with owner-only permissions. Existing parents are
     * deliberately not required to be private: `/Users` and `/var` normally
     * are not, while the managed instance directory itself must be. */
    for (char *p = work + 1; ; p++) {
        if (*p != '/' && *p != '\0') continue;
        char saved = *p; *p = '\0';
        if (work[0] && mkdir(work, 0700) < 0 && errno != EEXIST) return -1;
        *p = saved;
        if (!saved) break;
    }
    return verify_dir(path);
}

static int fsync_parent(const char *path) {
    char parent[PATH_MAX];
    size_t n = strlen(path);
    if (n >= sizeof parent) { errno = ENAMETOOLONG; return -1; }
    memcpy(parent, path, n + 1);
    char *slash = strrchr(parent, '/');
    if (!slash || slash == parent) { errno = EINVAL; return -1; }
    *slash = '\0';
    int fd = open(parent, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0) return -1;
    int rc = fsync(fd);
    close(fd);
    return rc;
}

static int hex_id_valid(const char *id, ssize_t n) {
    if (n != 33 || id[32] != '\n') return 0;
    for (int i = 0; i < 32; i++)
        if (!((id[i] >= '0' && id[i] <= '9') || (id[i] >= 'a' && id[i] <= 'f')))
            return 0;
    return 1;
}

int instance_read_id(const char *data_dir, char out[33]) {
    char path[PATH_MAX];
    if (!data_dir || snprintf(path, sizeof path, "%s/instance.id", data_dir) >= (int)sizeof path) {
        errno = ENAMETOOLONG; return -1;
    }
    int fd = open(path, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
    if (fd < 0) return -1;
    struct stat st;
    char raw[34] = {0};
    errno = 0;
    ssize_t n = read(fd, raw, 33);
    int saved = errno;
    int ok = fstat(fd, &st) == 0 && S_ISREG(st.st_mode) && st.st_uid == geteuid() &&
             !(st.st_mode & 0077) && hex_id_valid(raw, n);
    close(fd);
    if (!ok) { errno = saved ? saved : EINVAL; return -1; }
    memcpy(out, raw, 32); out[32] = '\0';
    return 0;
}

int instance_load_or_create_id(const char *data_dir, char out[33]) {
    if (instance_read_id(data_dir, out) == 0) return 0;
    if (errno != ENOENT) {
        /* A concurrent O_EXCL creator has installed the name before it has
         * written all 33 bytes.  EINVAL is the deliberately strict reader's
         * result for that incomplete record; wait briefly, but still reject a
         * permanently malformed identity. */
        if (errno != EINVAL) return -1;
        for (int attempt = 0; attempt < 50; attempt++) {
            struct timespec pause = {0, 2 * 1000 * 1000};
            nanosleep(&pause, NULL);
            if (instance_read_id(data_dir, out) == 0) return 0;
            if (errno != EINVAL && errno != ENOENT) return -1;
        }
        errno = EINVAL;
        return -1;
    }
    char path[PATH_MAX];
    if (snprintf(path, sizeof path, "%s/instance.id", data_dir) >= (int)sizeof path) {
        errno = ENAMETOOLONG; return -1;
    }
    int fd = open(path, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0600);
    if (fd < 0) {
        if (errno == EEXIST) {
            /* The creator writes and fsyncs the identity after claiming this
             * name. Concurrent launchers can observe that small interval;
             * wait for a complete, valid record rather than treating it as
             * an unsafe directory or minting a second identity. */
            for (int attempt = 0; attempt < 50; attempt++) {
                if (instance_read_id(data_dir, out) == 0) return 0;
                if (errno != EINVAL) return -1;
                struct timespec pause = {0, 2 * 1000 * 1000};
                nanosleep(&pause, NULL);
            }
            errno = EAGAIN;
            return -1;
        }
        return -1;
    }
    unsigned char random_bytes[16];
    int rfd = open("/dev/urandom", O_RDONLY | O_CLOEXEC);
    ssize_t got = rfd >= 0 ? read(rfd, random_bytes, sizeof random_bytes) : -1;
    if (rfd >= 0) close(rfd);
    if (got != (ssize_t)sizeof random_bytes) {
        int saved = errno ? errno : EIO;
        close(fd);
        unlink(path);
        errno = saved;
        return -1;
    }
    static const char hex[] = "0123456789abcdef";
    char raw[33];
    for (int i = 0; i < 16; i++) { raw[2 * i] = hex[random_bytes[i] >> 4]; raw[2 * i + 1] = hex[random_bytes[i] & 15]; }
    raw[32] = '\n';
    ssize_t wrote = write(fd, raw, sizeof raw);
    int rc = wrote == (ssize_t)sizeof raw && fsync(fd) == 0 ? 0 : -1;
    int saved = errno;
    close(fd);
    if (rc < 0) { unlink(path); errno = saved; return -1; }
    if (fsync_parent(path) < 0) return -1;
    memcpy(out, raw, 32); out[32] = '\0';
    return 0;
}

static int compare_strings(const void *a, const void *b) {
    const char *const *aa = a, *const *bb = b;
    return strcmp(*aa, *bb);
}

void instance_locks_release(InstanceLocks *locks) {
    if (!locks) return;
    for (size_t i = 0; i < locks->count; i++) if (locks->fds[i] >= 0) close(locks->fds[i]);
    free(locks->fds); free(locks->paths);
    memset(locks, 0, sizeof *locks);
}

int instance_locks_acquire(InstanceLocks *locks, const char *const *paths,
                           size_t count, const char *instance_id,
                           const char *endpoint) {
    memset(locks, 0, sizeof *locks);
    if (!count) return 0;
    char **ordered = calloc(count, sizeof *ordered);
    int *fds = calloc(count, sizeof *fds);
    if (!ordered || !fds) { free(ordered); free(fds); return -1; }
    for (size_t i = 0; i < count; i++) { ordered[i] = (char *)paths[i]; fds[i] = -1; }
    qsort(ordered, count, sizeof *ordered, compare_strings);
    for (size_t i = 0; i < count; i++) {
        if (!ordered[i] || ordered[i][0] != '/') { errno = EINVAL; goto fail; }
        if (i && strcmp(ordered[i - 1], ordered[i]) == 0) continue;
        int fd = open(ordered[i], O_RDWR | O_CREAT | O_NOFOLLOW | O_CLOEXEC, 0600);
        if (fd < 0) goto fail;
        struct stat st;
        if (fstat(fd, &st) < 0 || !S_ISREG(st.st_mode) || st.st_uid != geteuid() || (st.st_mode & 0077) || flock(fd, LOCK_EX | LOCK_NB) < 0) {
            int saved = errno; close(fd); errno = saved; goto fail;
        }
        if (ftruncate(fd, 0) < 0 ||
            dprintf(fd, "format=1\npid=%ld\ninstance_id=%s\nendpoint=%s\nstarted_unix_ms=%lld\n",
                    (long)getpid(), instance_id ? instance_id : "", endpoint ? endpoint : "",
                    (long long)time(NULL) * 1000) < 0 || fsync(fd) < 0) {
            int saved = errno; close(fd); errno = saved; goto fail;
        }
        fds[i] = fd;
    }
    locks->fds = fds; locks->paths = ordered; locks->count = count;
    return 0;
fail:
    for (size_t i = 0; i < count; i++) if (fds[i] >= 0) close(fds[i]);
    free(fds); free(ordered);
    return -1;
}

int instance_tcp_lock_path(const char *bind, int port, char *out, size_t out_size) {
    const char *runtime = getenv("XDG_RUNTIME_DIR");
    char dir[PATH_MAX];
    if (runtime && runtime[0] == '/' && snprintf(dir, sizeof dir, "%s/kuttidb", runtime) < (int)sizeof dir) {
        if (mkdir(dir, 0700) < 0 && errno != EEXIST) return -1;
        if (verify_dir(dir) < 0) return -1;
    } else {
        if (snprintf(dir, sizeof dir, "/tmp/kuttidb-%lu", (unsigned long)geteuid()) >= (int)sizeof dir) { errno = ENAMETOOLONG; return -1; }
        if (mkdir(dir, 0700) < 0 && errno != EEXIST) return -1;
        if (verify_dir(dir) < 0) return -1;
    }
    if (snprintf(out, out_size, "%s/tcp-%s-%d.server.lock", dir, bind, port) >= (int)out_size) { errno = ENAMETOOLONG; return -1; }
    return 0;
}
