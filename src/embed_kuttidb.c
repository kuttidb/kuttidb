#include "embed_int.h"

#include <errno.h>
#include <sched.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static int embed_lock(EmbedLock *lock) {
    int me = (int)getpid();
    unsigned spins = 0;
    for (;;) {
        int expected = 0;
        if (atomic_compare_exchange_weak_explicit(&lock->owner_pid, &expected, me,
                memory_order_acquire, memory_order_relaxed)) return 0;
        if (++spins == 1024) {
            spins = 0;
            int owner = atomic_load_explicit(&lock->owner_pid, memory_order_relaxed);
            if (owner > 0 && kill(owner, 0) < 0 && errno == ESRCH) {
                expected = owner;
                atomic_compare_exchange_strong_explicit(&lock->owner_pid, &expected, 0,
                    memory_order_release, memory_order_relaxed);
            } else {
                sched_yield();
            }
        }
    }
}

static void embed_unlock(EmbedLock *lock) {
    atomic_store_explicit(&lock->owner_pid, 0, memory_order_release);
}

static uint32_t embed_hash(const char *data, size_t len) {
    uint32_t h = (uint32_t)len;
    while (len >= 4) {
        uint32_t k;
        memcpy(&k, data, 4);
        h += k;
        h *= 0xc2b2ae35u;
        h = (h << 13) | (h >> 19);
        data += 4;
        len -= 4;
    }
    while (len) {
        h += (unsigned char)*data++;
        h *= 0xc2b2ae35u;
        h = (h << 13) | (h >> 19);
        len--;
    }
    h ^= h >> 16;
    h *= 0x85ebca6bu;
    h ^= h >> 13;
    h *= 0xc2b2ae35u;
    h ^= h >> 16;
    return h;
}

static EmbedEntry *entry_at(const KuttiDB *c, uint64_t off) {
    if (off < sizeof(EmbedBlock) || off > c->region_size - sizeof(EmbedEntry))
        return NULL;
    return embed_ptr(c, off);
}

static EmbedBlock *block_for(const KuttiDB *c, const EmbedEntry *entry) {
    uint64_t off = embed_off(c, entry);
    if (off < sizeof(EmbedBlock)) return NULL;
    return (EmbedBlock *)((char *)entry - sizeof(EmbedBlock));
}

static uint64_t entry_bytes(const KuttiDB *c, const EmbedEntry *entry) {
    EmbedBlock *block = block_for(c, entry);
    return block ? block->capacity : 0;
}

static EmbedShard *shard_for(KuttiDB *c, uint32_t hash) {
    EmbedShared *shared = embed_shared(c);
    EmbedShard *shards = shared ? embed_ptr(c, shared->shards_off) : NULL;
    return shards ? &shards[hash >> (32 - shared->shard_bits)] : NULL;
}

static EmbedEntry *heap_alloc(KuttiDB *c, uint64_t entry_size) {
    EmbedShared *shared = embed_shared(c);
    if (!shared || entry_size > UINT64_MAX - sizeof(EmbedBlock)) return NULL;
    uint64_t need = embed_align((uint64_t)sizeof(EmbedBlock) + entry_size);
    EmbedHeap *heap = &shared->heap;
    embed_lock(&heap->lock);
    uint64_t prev_off = 0;
    uint64_t off = heap->free_off;
    while (off) {
        EmbedBlock *block = embed_ptr(c, off);
        if (!block || block->capacity < sizeof(EmbedBlock) ||
            block->capacity > c->region_size - off) {
            embed_unlock(&heap->lock);
            return NULL; /* malformed shared heap: fail closed */
        }
        if (block->capacity >= need) {
            if (prev_off) {
                EmbedBlock *prev = embed_ptr(c, prev_off);
                prev->next_off = block->next_off;
            } else {
                heap->free_off = block->next_off;
            }
            block->next_off = 0;
            embed_unlock(&heap->lock);
            return (EmbedEntry *)(block + 1);
        }
        prev_off = off;
        off = block->next_off;
    }
    if (heap->bump_off > heap->end_off || need > heap->end_off - heap->bump_off) {
        embed_unlock(&heap->lock);
        return NULL;
    }
    EmbedBlock *block = embed_ptr(c, heap->bump_off);
    uint64_t block_off = heap->bump_off;
    heap->bump_off += need;
    block->next_off = 0;
    block->capacity = need;
    atomic_fetch_add_explicit(&shared->allocated_mem, need, memory_order_relaxed);
    embed_unlock(&heap->lock);
    (void)block_off;
    return (EmbedEntry *)(block + 1);
}

static void entry_free(KuttiDB *c, EmbedShared *shared, EmbedShard *shard,
                       EmbedEntry *entry) {
    uint64_t bytes = entry_bytes(c, entry);
    if (!bytes) return;
    if (entry->exp)
        atomic_fetch_sub_explicit(&shared->ttl_entries, 1, memory_order_relaxed);
    EmbedBlock *block = block_for(c, entry);
    EmbedHeap *heap = &shared->heap;
    embed_lock(&heap->lock);
    block->next_off = heap->free_off;
    heap->free_off = embed_off(c, block);
    embed_unlock(&heap->lock);
    shard->mem -= bytes;
    atomic_fetch_sub_explicit(&shared->total_mem, bytes, memory_order_relaxed);
}

static void remove_locked(KuttiDB *c, EmbedShared *shared, EmbedShard *shard,
                          uint64_t *link) {
    EmbedEntry *entry = entry_at(c, *link);
    if (!entry) { *link = 0; return; }
    *link = entry->next_off;
    entry_free(c, shared, shard, entry);
    if (shard->count) shard->count--;
}

static uint64_t next_random(EmbedShared *shared) {
    uint64_t x = atomic_fetch_add_explicit(&shared->rseed,
        UINT64_C(0x9e3779b97f4a7c15), memory_order_relaxed);
    x ^= x >> 30; x *= UINT64_C(0xbf58476d1ce4e5b9);
    x ^= x >> 27; x *= UINT64_C(0x94d049bb133111eb);
    x ^= x >> 31;
    return x;
}

static void evict_under_budget(KuttiDB *c, int max_iter) {
    EmbedShared *shared = embed_shared(c);
    if (!shared) return;
    uint64_t budget = atomic_load_explicit(&shared->budget, memory_order_relaxed);
    if (!budget) return;
    for (int i = 0; i < max_iter; i++) {
        if (atomic_load_explicit(&shared->total_mem, memory_order_relaxed) <= budget)
            return;
        EmbedShard *shard = shard_for(c, (uint32_t)next_random(shared));
        if (!shard) return;
        embed_lock(&shard->lock);
        uint64_t *buckets = embed_ptr(c, shard->buckets_off);
        if (buckets && shard->count) {
            uint64_t start = next_random(shared) & (shard->nbuckets - 1);
            for (uint64_t probe = 0; probe < shard->nbuckets; probe++) {
                uint64_t *link = &buckets[(start + probe) & (shard->nbuckets - 1)];
                if (*link) {
                    remove_locked(c, shared, shard, link);
                    atomic_fetch_add_explicit(&shared->evicted, 1, memory_order_relaxed);
                    break;
                }
            }
        }
        embed_unlock(&shard->lock);
    }
}

int embed_kuttidb_init(KuttiDB *c, uint64_t kuttidb_off, size_t requested_shards) {
    if (!c || !c->region || c->region_size < sizeof(EmbedShared) ||
        kuttidb_off > c->region_size - sizeof(EmbedShared)) return -1;
    size_t nshards = 1;
    while (nshards < requested_shards) {
        if (nshards > SIZE_MAX / 2) return -1;
        nshards <<= 1;
    }
    if (nshards < 2) nshards = 2;
    int bits = 1;
    while (((size_t)1 << bits) < nshards) bits++;
    uint64_t shards_off = embed_align(kuttidb_off + sizeof(EmbedShared));
    uint64_t shards_bytes = (uint64_t)nshards * sizeof(EmbedShard);
    uint64_t heap_start = embed_align(shards_off + shards_bytes);
    if (shards_off > c->region_size || shards_bytes > c->region_size - shards_off ||
        heap_start >= c->region_size) return -1;

    EmbedShared *shared = embed_ptr(c, kuttidb_off);
    if (!shared) return -1;
    memset(shared, 0, sizeof(*shared));
    shared->shards_off = shards_off;
    shared->nshards = nshards;
    shared->shard_bits = (uint32_t)bits;
    shared->heap.bump_off = heap_start;
    shared->heap.end_off = c->region_size;
    atomic_store(&shared->rseed, ((uint64_t)time(NULL) << 32) | UINT32_C(0x9e3779b9));
    EmbedShard *shards = embed_ptr(c, shards_off);
    if (!shards) return -1;
    memset(shards, 0, (size_t)shards_bytes);

    const uint64_t initial_buckets = 256;
    const uint64_t bucket_bytes = initial_buckets * sizeof(uint64_t);
    for (size_t i = 0; i < nshards; i++) {
        uint64_t bucket_off = shared->heap.bump_off;
        if (bucket_bytes > shared->heap.end_off - bucket_off) return -1;
        uint64_t *buckets = embed_ptr(c, bucket_off);
        if (!buckets) return -1;
        memset(buckets, 0, (size_t)bucket_bytes);
        shared->heap.bump_off = embed_align(bucket_off + bucket_bytes);
        shards[i].buckets_off = bucket_off;
        shards[i].nbuckets = initial_buckets;
        shards[i].mem = bucket_bytes;
        shared->initialized_shards = i + 1;
    }
    uint64_t table_bytes = (uint64_t)nshards * bucket_bytes;
    atomic_store(&shared->total_mem, table_bytes);
    atomic_store(&shared->allocated_mem, table_bytes);
    return 0;
}

int embed_kuttidb_put_abs(KuttiDB *c, const char *key, uint32_t klen,
                        const char *value, uint32_t vlen, uint32_t exp) {
    EmbedShared *shared = embed_shared(c);
    if (!shared || !key || (!value && vlen)) return -1;
    uint32_t hash = embed_hash(key, klen);
    uint64_t budget = atomic_load_explicit(&shared->budget, memory_order_relaxed);
    if (budget && atomic_load_explicit(&shared->total_mem, memory_order_relaxed) >= budget)
        evict_under_budget(c, (int)(shared->nshards * 2));
    EmbedShard *shard = shard_for(c, hash);
    if (!shard) return -1;
    embed_lock(&shard->lock);
    uint64_t *buckets = embed_ptr(c, shard->buckets_off);
    if (!buckets) { embed_unlock(&shard->lock); return -1; }
    uint64_t *link = &buckets[hash & (shard->nbuckets - 1)];
    EmbedEntry *entry;
    while (*link) {
        entry = entry_at(c, *link);
        if (!entry || entry->klen > c->region_size || entry->vlen > c->region_size) {
            embed_unlock(&shard->lock);
            return -1;
        }
        if (entry->hash == hash && entry->klen == klen &&
            memcmp(entry->data, key, klen) == 0) {
            uint64_t need = sizeof(EmbedEntry) + (uint64_t)klen + vlen;
            if (need <= entry_bytes(c, entry) - sizeof(EmbedBlock)) {
                if (!entry->exp && exp)
                    atomic_fetch_add_explicit(&shared->ttl_entries, 1, memory_order_relaxed);
                else if (entry->exp && !exp)
                    atomic_fetch_sub_explicit(&shared->ttl_entries, 1, memory_order_relaxed);
                entry->vlen = vlen;
                entry->exp = exp;
                if (vlen) memcpy(entry->data + klen, value, vlen);
                embed_unlock(&shard->lock);
                return 0;
            }
            EmbedEntry *replacement = heap_alloc(c, need);
            if (!replacement) { embed_unlock(&shard->lock); return -1; }
            replacement->hash = hash;
            replacement->klen = klen;
            replacement->vlen = vlen;
            replacement->exp = exp;
            replacement->next_off = entry->next_off;
            memcpy(replacement->data, key, klen);
            if (vlen) memcpy(replacement->data + klen, value, vlen);
            if (exp) atomic_fetch_add_explicit(&shared->ttl_entries, 1, memory_order_relaxed);
            uint64_t replacement_off = embed_off(c, replacement);
            *link = replacement_off;
            uint64_t bytes = entry_bytes(c, replacement);
            shard->mem += bytes;
            atomic_fetch_add_explicit(&shared->total_mem, bytes, memory_order_relaxed);
            entry_free(c, shared, shard, entry);
            embed_unlock(&shard->lock);
            if (budget && atomic_load_explicit(&shared->total_mem, memory_order_relaxed) > budget)
                evict_under_budget(c, (int)(shared->nshards * 4));
            return 0;
        }
        link = &entry->next_off;
    }

    uint64_t need = sizeof(EmbedEntry) + (uint64_t)klen + vlen;
    EmbedEntry *created = heap_alloc(c, need);
    if (!created) { embed_unlock(&shard->lock); return -1; }
    created->hash = hash;
    created->klen = klen;
    created->vlen = vlen;
    created->exp = exp;
    created->next_off = *link;
    memcpy(created->data, key, klen);
    if (vlen) memcpy(created->data + klen, value, vlen);
    if (exp) atomic_fetch_add_explicit(&shared->ttl_entries, 1, memory_order_relaxed);
    *link = embed_off(c, created);
    uint64_t bytes = entry_bytes(c, created);
    shard->mem += bytes;
    atomic_fetch_add_explicit(&shared->total_mem, bytes, memory_order_relaxed);
    shard->count++;
    embed_unlock(&shard->lock);
    if (budget && atomic_load_explicit(&shared->total_mem, memory_order_relaxed) > budget)
        evict_under_budget(c, (int)(shared->nshards * 4));
    return 0;
}

int embed_kuttidb_get(KuttiDB *c, const char *key, uint32_t klen,
                    char **out_value, uint32_t *out_vlen, KuttiVec *vec,
                    uint32_t now_sec) {
    if (!c || !key || (!vec && (!out_value || !out_vlen))) return -1;
    if (!now_sec) now_sec = (uint32_t)time(NULL);
    EmbedShared *shared = embed_shared(c);
    uint32_t hash = embed_hash(key, klen);
    EmbedShard *shard = shard_for(c, hash);
    if (!shared || !shard) return -1;
    embed_lock(&shard->lock);
    uint64_t *buckets = embed_ptr(c, shard->buckets_off);
    if (!buckets) { embed_unlock(&shard->lock); return -1; }
    uint64_t *link = &buckets[hash & (shard->nbuckets - 1)];
    while (*link) {
        EmbedEntry *entry = entry_at(c, *link);
        if (!entry || entry->klen > c->region_size || entry->vlen > c->region_size) {
            embed_unlock(&shard->lock);
            return -1;
        }
        if (entry->hash == hash && entry->klen == klen &&
            memcmp(entry->data, key, klen) == 0) {
            if (entry->exp && entry->exp <= now_sec) {
                remove_locked(c, shared, shard, link);
                atomic_fetch_add_explicit(&shared->expired, 1, memory_order_relaxed);
                embed_unlock(&shard->lock);
                return 0;
            }
            if (vec) {
                if (kuttidb_vec_reserve(vec, entry->vlen) < 0) {
                    embed_unlock(&shard->lock);
                    return -1;
                }
                if (entry->vlen) memcpy(vec->data + vec->len,
                                        entry->data + entry->klen, entry->vlen);
                vec->len += entry->vlen;
            } else {
                char *copy = malloc(entry->vlen ? entry->vlen : 1);
                if (!copy) { embed_unlock(&shard->lock); return -1; }
                if (entry->vlen) memcpy(copy, entry->data + entry->klen, entry->vlen);
                *out_value = copy;
                *out_vlen = entry->vlen;
            }
            embed_unlock(&shard->lock);
            return 1;
        }
        link = &entry->next_off;
    }
    embed_unlock(&shard->lock);
    return 0;
}

int embed_kuttidb_delete(KuttiDB *c, const char *key, uint32_t klen) {
    if (!c || !key) return -1;
    EmbedShared *shared = embed_shared(c);
    uint32_t hash = embed_hash(key, klen);
    EmbedShard *shard = shard_for(c, hash);
    if (!shared || !shard) return -1;
    embed_lock(&shard->lock);
    uint64_t *buckets = embed_ptr(c, shard->buckets_off);
    if (!buckets) { embed_unlock(&shard->lock); return -1; }
    uint64_t *link = &buckets[hash & (shard->nbuckets - 1)];
    while (*link) {
        EmbedEntry *entry = entry_at(c, *link);
        if (!entry) { embed_unlock(&shard->lock); return -1; }
        if (entry->hash == hash && entry->klen == klen &&
            memcmp(entry->data, key, klen) == 0) {
            remove_locked(c, shared, shard, link);
            embed_unlock(&shard->lock);
            return 1;
        }
        link = &entry->next_off;
    }
    embed_unlock(&shard->lock);
    return 0;
}

void embed_kuttidb_set_budget(KuttiDB *c, uint64_t budget_bytes) {
    EmbedShared *shared = embed_shared(c);
    if (shared) atomic_store_explicit(&shared->budget, budget_bytes, memory_order_relaxed);
}

void embed_kuttidb_sweep_expired(KuttiDB *c, size_t bucket_work) {
    EmbedShared *shared = embed_shared(c);
    if (!shared || !atomic_load_explicit(&shared->ttl_entries, memory_order_relaxed)) return;
    uint32_t now = (uint32_t)time(NULL);
    for (size_t n = 0; n < bucket_work; n++) {
        uint64_t ticket = atomic_fetch_add_explicit(&shared->sweep_cursor, 1,
                                                    memory_order_relaxed);
        EmbedShard *shard = shard_for(c, (uint32_t)(ticket * UINT32_C(2654435761)));
        if (!shard) return;
        embed_lock(&shard->lock);
        uint64_t *buckets = embed_ptr(c, shard->buckets_off);
        if (buckets) {
            uint64_t *link = &buckets[(ticket / shared->nshards) & (shard->nbuckets - 1)];
            while (*link) {
                EmbedEntry *entry = entry_at(c, *link);
                if (!entry) { *link = 0; break; }
                if (entry->exp && entry->exp <= now) {
                    remove_locked(c, shared, shard, link);
                    atomic_fetch_add_explicit(&shared->expired, 1, memory_order_relaxed);
                } else {
                    link = &entry->next_off;
                }
            }
        }
        embed_unlock(&shard->lock);
    }
}

size_t embed_kuttidb_count(KuttiDB *c) {
    EmbedShared *shared = embed_shared(c);
    EmbedShard *shards = shared ? embed_ptr(c, shared->shards_off) : NULL;
    uint64_t count = 0;
    if (!shards) return 0;
    for (uint64_t i = 0; i < shared->nshards; i++) {
        embed_lock(&shards[i].lock);
        count += shards[i].count;
        embed_unlock(&shards[i].lock);
    }
    return count > SIZE_MAX ? SIZE_MAX : (size_t)count;
}

size_t embed_kuttidb_memusage(KuttiDB *c) {
    EmbedShared *shared = embed_shared(c);
    uint64_t n = shared ? atomic_load_explicit(&shared->total_mem, memory_order_relaxed) : 0;
    return n > SIZE_MAX ? SIZE_MAX : (size_t)n;
}

size_t embed_kuttidb_allocated(KuttiDB *c) {
    EmbedShared *shared = embed_shared(c);
    uint64_t n = shared ? atomic_load_explicit(&shared->allocated_mem, memory_order_relaxed) : 0;
    return n > SIZE_MAX ? SIZE_MAX : (size_t)n;
}

unsigned long long embed_kuttidb_expired_count(KuttiDB *c) {
    EmbedShared *shared = embed_shared(c);
    return shared ? atomic_load_explicit(&shared->expired, memory_order_relaxed) : 0;
}

unsigned long long embed_kuttidb_evicted_count(KuttiDB *c) {
    EmbedShared *shared = embed_shared(c);
    return shared ? atomic_load_explicit(&shared->evicted, memory_order_relaxed) : 0;
}

int embed_kuttidb_foreach(KuttiDB *c,
                        int (*cb)(const char *, uint32_t, const char *,
                                  uint32_t, uint32_t, void *),
                        void *ctx) {
    EmbedShared *shared = embed_shared(c);
    EmbedShard *shards = shared ? embed_ptr(c, shared->shards_off) : NULL;
    if (!shared || !shards || !cb) return -1;
    uint32_t now = (uint32_t)time(NULL);
    KuttiVec stage = {0};
    int result = 0;
    for (uint64_t i = 0; i < shared->nshards && !result; i++) {
        stage.len = 0;
        embed_lock(&shards[i].lock);
        uint64_t *buckets = embed_ptr(c, shards[i].buckets_off);
        for (uint64_t b = 0; buckets && b < shards[i].nbuckets && !result; b++) {
            for (uint64_t off = buckets[b]; off; ) {
                EmbedEntry *entry = entry_at(c, off);
                if (!entry) { result = -1; break; }
                off = entry->next_off;
                if (entry->exp && entry->exp <= now) continue;
                uint64_t n = 12 + (uint64_t)entry->klen + entry->vlen;
                if (n > SIZE_MAX || kuttidb_vec_reserve(&stage, (size_t)n) < 0) {
                    result = -1;
                    break;
                }
                unsigned char *p = (unsigned char *)stage.data + stage.len;
                memcpy(p, &entry->klen, 4);
                memcpy(p + 4, &entry->vlen, 4);
                memcpy(p + 8, &entry->exp, 4);
                memcpy(p + 12, entry->data, (size_t)entry->klen + entry->vlen);
                stage.len += (size_t)n;
            }
        }
        embed_unlock(&shards[i].lock);
        for (size_t at = 0; !result && at < stage.len; ) {
            uint32_t klen, vlen, exp;
            memcpy(&klen, stage.data + at, 4);
            memcpy(&vlen, stage.data + at + 4, 4);
            memcpy(&exp, stage.data + at + 8, 4);
            const char *key = stage.data + at + 12;
            result = cb(key, klen, key + klen, vlen, exp, ctx);
            at += 12 + (size_t)klen + vlen;
        }
    }
    free(stage.data);
    return result;
}
