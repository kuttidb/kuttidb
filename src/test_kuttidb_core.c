#include "kuttidb.h"
#include <assert.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct { KuttiDB *cache; int id; } Worker;

static void *run_worker(void *arg) {
    Worker *w = arg;
    char key[64];
    unsigned char value[256];
    memset(value, (unsigned char)w->id, sizeof value);
    for (int i = 0; i < 10000; i++) {
        int n = snprintf(key, sizeof key, "worker-%d-%d", w->id, i);
        assert(kuttidb_put(w->cache, key, (uint32_t)n,
                         (char *)value, sizeof value) == 0);
        char *out = NULL;
        uint32_t outlen = 0;
        assert(kuttidb_get(w->cache, key, (uint32_t)n, &out, &outlen) == 1);
        assert(outlen == sizeof value && memcmp(out, value, outlen) == 0);
        free(out);
    }
    return NULL;
}

static int count_cb(const char *k, uint32_t klen, const char *v,
                    uint32_t vlen, uint32_t exp, void *ctx) {
    (void)k; (void)klen; (void)v; (void)vlen; (void)exp;
    (*(size_t *)ctx)++;
    return 0;
}
static int metadata_count_cb(const char *k, uint32_t klen, uint32_t vlen,
                             uint32_t exp, void *ctx) {
    (void)k; (void)klen; (void)vlen; (void)exp;
    (*(size_t *)ctx)++;
    return 0;
}

int main(void) {
    /* A shard that becomes empty must return its slab footprint. */
    KuttiDB *reclaim = kuttidb_create(2, 16);
    assert(reclaim);
    size_t reclaim_baseline = kuttidb_allocated(reclaim);
    assert(kuttidb_put(reclaim, "only", 4, "value", 5) == 0);
    assert(kuttidb_allocated(reclaim) > reclaim_baseline);
    assert(kuttidb_delete(reclaim, "only", 4) == 1);
    assert(kuttidb_allocated(reclaim) == reclaim_baseline);
    kuttidb_destroy(reclaim);

    KuttiDB *paged = kuttidb_create(2, 16);
    assert(paged);
    assert(kuttidb_put(paged, "page-a", 6, "a", 1) == 0);
    assert(kuttidb_put(paged, "page-b", 6, "b", 1) == 0);
    assert(kuttidb_put(paged, "page-c", 6, "c", 1) == 0);
    KuttiDBMetadataCursor cursor = {0}, next = {0};
    size_t page_count = 0; int more = 0;
    assert(kuttidb_foreach_metadata_page(paged, &cursor, 1,
                                         metadata_count_cb, &page_count,
                                         &next, &more) == 1 && more);
    cursor = next;
    assert(kuttidb_foreach_metadata_page(paged, &cursor, 8,
                                         metadata_count_cb, &page_count,
                                         &next, &more) == 2 && !more);
    assert(page_count == 3 && kuttidb_revision(paged) >= 3);
    kuttidb_destroy(paged);

    KuttiDB *cache = kuttidb_create(256, 64);
    assert(cache);
    assert(kuttidb_allocated(cache) < (1u << 20));

    static const size_t sizes[] = {0, 1, 31, 100, 4000, 9000, 70000, 120000};
    for (size_t i = 0; i < sizeof sizes / sizeof sizes[0]; i++) {
        size_t size = sizes[i];
        char *value = malloc(size ? size : 1);
        assert(value);
        memset(value, (int)i, size);
        char key[32];
        int klen = snprintf(key, sizeof key, "size-%zu", size);
        assert(kuttidb_put(cache, key, (uint32_t)klen, value, (uint32_t)size) == 0);
        char *out = NULL;
        uint32_t outlen = 0;
        assert(kuttidb_get(cache, key, (uint32_t)klen, &out, &outlen) == 1);
        assert(outlen == size && memcmp(out, value, size) == 0);
        free(out);
        free(value);
    }

    /* Same-class overwrite reuse and absolute-expiry recovery path. */
    assert(kuttidb_put(cache, "overwrite", 9, "first", 5) == 0);
    size_t allocated = kuttidb_allocated(cache);
    assert(kuttidb_put(cache, "overwrite", 9, "second", 6) == 0);
    assert(kuttidb_allocated(cache) == allocated);
    assert(kuttidb_put_abs(cache, "expired", 7, "x", 1,
                         (uint32_t)time(NULL) - 1) == 0);
    char *out = NULL;
    uint32_t outlen = 0;
    assert(kuttidb_get(cache, "expired", 7, &out, &outlen) == 0);

    pthread_t threads[4];
    Worker workers[4];
    for (int i = 0; i < 4; i++) {
        workers[i] = (Worker){cache, i};
        assert(pthread_create(&threads[i], NULL, run_worker, &workers[i]) == 0);
    }
    for (int i = 0; i < 4; i++) pthread_join(threads[i], NULL);

    size_t iterated = 0;
    assert(kuttidb_foreach(cache, count_cb, &iterated) == 0);
    assert(iterated == kuttidb_count(cache));

    kuttidb_set_budget(cache, 4u << 20);
    unsigned char fill[512] = {0};
    for (int i = 0; i < 20000; i++) {
        char key[32];
        int n = snprintf(key, sizeof key, "fill-%d", i);
        assert(kuttidb_put(cache, key, (uint32_t)n,
                         (char *)fill, sizeof fill) == 0);
    }
    assert(kuttidb_memusage(cache) < (5u << 20));
    assert(kuttidb_evicted_count(cache) > 0);

    kuttidb_destroy(cache);
    puts("CORE KUTTIDB TESTS PASSED");
    return 0;
}
