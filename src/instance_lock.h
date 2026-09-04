#ifndef KUTTIDB_INSTANCE_LOCK_H
#define KUTTIDB_INSTANCE_LOCK_H

#include <stddef.h>

/* Advisory ownership locks.  File existence is deliberately not ownership:
 * a lock is valid only while its descriptor remains open. */
typedef struct InstanceLocks {
    int *fds;
    char **paths;
    size_t count;
} InstanceLocks;

/* Creates/validates an owner-only directory without following a final
 * symlink.  Returns 0 on success. */
int instance_secure_dir(const char *path);

/* Loads a 128-bit hexadecimal identity, atomically creating it when missing.
 * The returned value is always NUL terminated and exactly 32 lowercase hex
 * characters. */
int instance_load_or_create_id(const char *data_dir, char out[33]);
int instance_read_id(const char *data_dir, char out[33]);

/* Acquires distinct lock paths in lexical order.  The paths must be absolute
 * regular files owned by this uid (new files are created 0600). */
int instance_locks_acquire(InstanceLocks *locks, const char *const *paths,
                           size_t count, const char *instance_id,
                           const char *endpoint);
void instance_locks_release(InstanceLocks *locks);

/* Creates an owner-only per-uid runtime directory and derives a stable TCP
 * endpoint lock path. */
int instance_tcp_lock_path(const char *bind, int port, char *out, size_t out_size);

#endif
