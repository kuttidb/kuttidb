#include "instance_lock.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

int main(void) {
    char template[] = "/tmp/kuttidb-managed-lock.XXXXXX";
    char *dir = mkdtemp(template);
    assert(dir);
    assert(chmod(dir, 0700) == 0);
    char identity[33], loaded[33], lock_path[1024];
    assert(instance_load_or_create_id(dir, identity) == 0);
    assert(instance_read_id(dir, loaded) == 0);
    assert(strcmp(identity, loaded) == 0);
    assert(snprintf(lock_path, sizeof lock_path, "%s/.server.lock", dir) < (int)sizeof lock_path);
    const char *paths[] = {lock_path};
    InstanceLocks owner;
    assert(instance_locks_acquire(&owner, paths, 1, identity, "unix:test") == 0);
    pid_t child = fork();
    assert(child >= 0);
    if (child == 0) {
        InstanceLocks contender;
        _exit(instance_locks_acquire(&contender, paths, 1, identity, "unix:test") == 0 ? 1 : 0);
    }
    int status = 0;
    assert(waitpid(child, &status, 0) == child);
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    instance_locks_release(&owner);
    InstanceLocks successor;
    assert(instance_locks_acquire(&successor, paths, 1, identity, "unix:test") == 0);
    instance_locks_release(&successor);
    unlink(lock_path);
    char id_path[1024]; snprintf(id_path, sizeof id_path, "%s/instance.id", dir); unlink(id_path);
    rmdir(dir);
    puts("managed lock tests passed");
    return 0;
}
