#ifndef KUTTIDB_INT_H
#define KUTTIDB_INT_H

#include <stddef.h>
#include <stdint.h>
#include <pthread.h>
#include <errno.h>
#include <stdatomic.h>
#include <signal.h>
#include <sched.h>
#include <unistd.h>

typedef struct CacheLock {
    pthread_mutex_t local;
    _Atomic int owner_pid;
    int shared;
} CacheLock;

#define LOCK_TYPE CacheLock
#define USE_LOCK(l) CacheLock l

static inline int kuttidb_mutex_lock(CacheLock *l) {
    if (!l->shared) return pthread_mutex_lock(&l->local);
    int me = (int)getpid();
    unsigned spins = 0;
    for (;;) {
        int expected = 0;
        if (atomic_compare_exchange_weak_explicit(&l->owner_pid, &expected, me,
                memory_order_acquire, memory_order_relaxed)) return 0;
        if (++spins >= 1024) {
            spins = 0;
            int owner = atomic_load_explicit(&l->owner_pid, memory_order_relaxed);
            if (owner > 0 && kill(owner, 0) < 0 && errno == ESRCH) {
                expected = owner;
                atomic_compare_exchange_strong_explicit(&l->owner_pid, &expected, 0,
                    memory_order_release, memory_order_relaxed);
            } else {
                sched_yield();
            }
        }
    }
}

static inline int kuttidb_mutex_unlock(CacheLock *l) {
    if (!l->shared) return pthread_mutex_unlock(&l->local);
    atomic_store_explicit(&l->owner_pid, 0, memory_order_release);
    return 0;
}

static inline int kuttidb_mutex_destroy(CacheLock *l) {
    return l->shared ? 0 : pthread_mutex_destroy(&l->local);
}
#define LOCK_WR(l) kuttidb_mutex_lock(l)
#define LOCK_RD(l) kuttidb_mutex_lock(l)
#define LOCK_UN(l) kuttidb_mutex_unlock(l)
#define LOCK_DESTROY(l) kuttidb_mutex_destroy(l)

int init_shards(KuttiDB *c, size_t initial_buckets);

/* ---- design notes ------------------------------------------------------
 * - 256 shards (power of two), shard chosen from HIGH hash bits, bucket
 *   from low bits: full bucket independence, no shared write contention.
 * - entries are (key,value) inline in one object. Small records use compact
 *   size-class freelists backed by 16 KiB slabs; larger records are direct
 *   allocations to avoid large-class amplification.
 * - TTL: absolute expiry seconds in the entry; checked lazily on read and
 *   actively swept by the caller's background thread.
 * - memory budget: relaxed atomic live-byte counter, with bounded random
 *   eviction after writes that cross the configured limit.
 * ------------------------------------------------------------------------ */

typedef struct Entry {
    struct Entry *next;
    uint32_t exp;  /* absolute unix seconds, 0 = no expiry */
    uint32_t hash;
    uint32_t klen;
    uint32_t vlen;
    uint16_t cls;  /* size class, 0xFFFF = direct malloc */
    char data[];   /* key then value */
} Entry;

#define NCLASS 27
static const size_t class_sz[NCLASS] = {
    48, 64, 80, 96, 128, 160, 192, 256, 320, 384, 512, 640, 768,
    1024, 1280, 1536, 2048, 3072, 4096, 6144, 8192, 12288, 16384,
    24576, 32768, 49152, 65536
};
#define SLAB_SIZE (16 * 1024)
#define SLAB_MAX_ENTRY 4096

/* ---- embed heap: power-of-two block allocator inside the shared region ---- */
#define HEAP_MIN_SHIFT 6                 /* 64 B */
#define HEAP_MAX_SHIFT 26                /* 64 MB */
#define NHEAP_CLASSES (HEAP_MAX_SHIFT - HEAP_MIN_SHIFT + 1)

typedef struct BlockHead {
    size_t capacity;
    struct BlockHead *next;
} BlockHead;

typedef struct HeapState {
    USE_LOCK(lock);                      /* process-shared in embed mode */
    char *bump;
    char *end;
    BlockHead *freelists[NHEAP_CLASSES];
} HeapState;

#ifndef NSHARD_DEFAULT
#define NSHARD_DEFAULT 256
#endif
#define DIRECT_CLS 0xFFFF

typedef struct {
    Entry **buckets;
    size_t nbuckets;
    size_t count;
    size_t mem;  /* live bytes (entries + buckets) */
    USE_LOCK(lock);

    /* slab allocator */
    void *slabs;
    char *cur;
    size_t left;
    Entry *freelist[NCLASS];
} Shard;

struct KuttiDB {
    Shard *shards;
    size_t nshards;
    size_t initialized_shards;
    int shard_bits;
    int embedded;          /* lives in a shared-memory region */
    void *region;          /* region base (embed mode) */
    size_t region_size;
    HeapState *heap;
    _Atomic unsigned long long budget;     /* 0 = unlimited */
    _Atomic unsigned long long total_mem;  /* global, lock-free reads */
    _Atomic unsigned long long allocated_mem; /* allocator-owned bytes */
    _Atomic unsigned long long expired;
    _Atomic unsigned long long evicted;
    _Atomic unsigned long long ttl_entries;
    _Atomic unsigned long long rseed;
    _Atomic uint64_t revision; /* process-lifetime mutation epoch */
    _Atomic size_t sweep_cursor;
};

/* Relocatable CEMBv3 shared-memory implementation (embed_kuttidb.c). */
int embed_kuttidb_put_abs(KuttiDB *c, const char *key, uint32_t klen,
                        const char *value, uint32_t vlen, uint32_t exp);
int embed_kuttidb_get(KuttiDB *c, const char *key, uint32_t klen,
                    char **out_value, uint32_t *out_vlen, KuttiVec *vec,
                    uint32_t now_sec);
int embed_kuttidb_delete(KuttiDB *c, const char *key, uint32_t klen);
void embed_kuttidb_set_budget(KuttiDB *c, uint64_t budget_bytes);
void embed_kuttidb_sweep_expired(KuttiDB *c, size_t bucket_work);
size_t embed_kuttidb_count(KuttiDB *c);
size_t embed_kuttidb_memusage(KuttiDB *c);
size_t embed_kuttidb_allocated(KuttiDB *c);
unsigned long long embed_kuttidb_expired_count(KuttiDB *c);
unsigned long long embed_kuttidb_evicted_count(KuttiDB *c);
int embed_kuttidb_foreach(KuttiDB *c,
                        int (*cb)(const char *, uint32_t, const char *,
                                  uint32_t, uint32_t, void *),
                        void *ctx);

#endif
