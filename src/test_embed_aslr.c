#include "kuttidb.h"
#include "embed.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#ifndef MAP_ANON
#define MAP_ANON MAP_ANONYMOUS
#endif

typedef struct {
    uintptr_t address;
    int status;
} ChildResult;

static int child_attach(const char *path, KuttiDB *inherited, int write_fd) {
    uintptr_t inherited_address = kuttidb_embed_mapping_address(inherited);
    size_t region_size = kuttidb_embed_mapping_size(inherited);
    ChildResult result = {0, 1};
    kuttidb_embed_detach(inherited);
    void *reservation = mmap((void *)inherited_address, region_size, PROT_NONE,
                             MAP_PRIVATE | MAP_ANON | MAP_FIXED, -1, 0);
    if (reservation == MAP_FAILED) goto out;
    KuttiDB *attached = kuttidb_embed_attach(path);
    if (!attached) goto out_unmap;
    result.address = kuttidb_embed_mapping_address(attached);
    char *value = NULL;
    uint32_t value_len = 0;
    if (result.address == inherited_address ||
        kuttidb_get(attached, "parent", 6, &value, &value_len) != 1 ||
        value_len != 5 || memcmp(value, "value", 5) != 0 ||
        kuttidb_put(attached, "child", 5, "reply", 5) != 0) {
        free(value);
        kuttidb_embed_detach(attached);
        goto out_unmap;
    }
    free(value);
    kuttidb_embed_detach(attached);
    result.status = 0;
out_unmap:
    munmap(reservation, region_size);
out:
    (void)write(write_fd, &result, sizeof(result));
    close(write_fd);
    return result.status;
}

int main(void) {
    char path[] = "/tmp/kuttidb-aslr-XXXXXX";
    int fd = mkstemp(path);
    if (fd < 0) { perror("mkstemp"); return 1; }
    close(fd);
    unlink(path);

    KuttiDB *parent = kuttidb_embed_create_sized(path, 16, NULL, 32u << 20);
    if (!parent || kuttidb_put(parent, "parent", 6, "value", 5) != 0) {
        fprintf(stderr, "could not create shared cache\n");
        return 1;
    }
    int pipefd[2];
    if (pipe(pipefd) < 0) { perror("pipe"); return 1; }
    pid_t child = fork();
    if (child < 0) { perror("fork"); return 1; }
    if (child == 0) {
        close(pipefd[0]);
        _exit(child_attach(path, parent, pipefd[1]) == 0 ? 0 : 1);
    }
    close(pipefd[1]);
    ChildResult result = {0, 1};
    ssize_t got = read(pipefd[0], &result, sizeof(result));
    close(pipefd[0]);
    int wait_status = 0;
    waitpid(child, &wait_status, 0);
    char *value = NULL;
    uint32_t value_len = 0;
    int ok = got == (ssize_t)sizeof(result) && result.status == 0 &&
             WIFEXITED(wait_status) && WEXITSTATUS(wait_status) == 0 &&
             result.address != kuttidb_embed_mapping_address(parent) &&
             kuttidb_get(parent, "child", 5, &value, &value_len) == 1 &&
             value_len == 5 && memcmp(value, "reply", 5) == 0;
    free(value);
    kuttidb_embed_detach(parent);
    unlink(path);
    if (!ok) {
        fprintf(stderr, "ASLR-safe cross-process attachment failed\n");
        return 1;
    }
    puts("EMBED ASLR TESTS PASSED");
    return 0;
}
