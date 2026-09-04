#include "kuttidb.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdatomic.h>

#include "kuttidb_int.h"

/* word-at-a-time hash with murmur3 finalizer */
static inline uint32_t hash_key(const char *data, size_t len) {
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

/* ---- vector ------------------------------------------------------------ */

int kuttidb_vec_reserve(KuttiVec *v, size_t extra) {
    if (extra > SIZE_MAX - v->len) return -1;
    size_t need = v->len + extra;
    if (need <= v->cap) return 0;
    size_t ncap;
    if (!v->cap) ncap = 4096;
    else if (v->cap >= (1u << 20)) {
        size_t grow = v->cap / 2;
        ncap = grow > SIZE_MAX - v->cap ? need : v->cap + grow;
    } else {
        ncap = v->cap > SIZE_MAX / 2 ? need : v->cap * 2;
    }
    while (ncap < need) {
        size_t grow = ncap >= (1u << 20) ? ncap / 2 : ncap;
        if (grow > SIZE_MAX - ncap) { ncap = need; break; }
        ncap += grow;
    }
    char *nd = realloc(v->data, ncap);
    if (!nd) return -1;
    v->data = nd;
    v->cap = ncap;
    return 0;
}

/* ---- block allocator: malloc locally, shared-heap in embed mode -------- */

static size_t heap_shift(size_t need) {
    size_t s = HEAP_MIN_SHIFT;
    while (((size_t)1 << s) < need) s++;
    return s;
}

static void *block_alloc(KuttiDB *c, size_t size) {
    if (!c->embedded) {
        void *p = malloc(size);
        if (p) atomic_fetch_add_explicit(&c->allocated_mem, size, memory_order_relaxed);
        return p;
    }
    HeapState *h = c->heap;
    if (size < ((size_t)1 << HEAP_MIN_SHIFT)) size = (size_t)1 << HEAP_MIN_SHIFT;
    size_t need = size + sizeof(BlockHead);

    LOCK_WR(&h->lock);
    if (need <= ((size_t)1 << HEAP_MAX_SHIFT)) {
        size_t sh = heap_shift(need);
        BlockHead *b = h->freelists[sh - HEAP_MIN_SHIFT];
        if (b) {
            h->freelists[sh - HEAP_MIN_SHIFT] = b->next;
            LOCK_UN(&h->lock);
            return (void *)(b + 1);
        }
    }
    size_t capacity = need <= ((size_t)1 << HEAP_MAX_SHIFT)
                    ? ((size_t)1 << heap_shift(need)) : need;
    if (h->bump && capacity <= (size_t)(h->end - h->bump)) {
        BlockHead *b = (BlockHead *)h->bump;
        h->bump += capacity;
        b->capacity = capacity - sizeof(BlockHead);
        atomic_fetch_add_explicit(&c->allocated_mem, capacity, memory_order_relaxed);
        LOCK_UN(&h->lock);
        return (void *)(b + 1);
    }
    LOCK_UN(&h->lock);
    return NULL;
}

static void block_free(KuttiDB *c, void *p) {
    if (!c->embedded) {
        if (!p) return;
        /* Local callers know and account their block sizes separately. */
        free(p);
        return;
    }
    if (!p) return;
    BlockHead *b = ((BlockHead *)p) - 1;
    HeapState *h = c->heap;
    size_t need = b->capacity + sizeof(BlockHead);
    if (need > ((size_t)1 << HEAP_MAX_SHIFT)) return; /* oversize: reclaimed on destroy */
    size_t sh = heap_shift(need);
    LOCK_WR(&h->lock);
    b->next = h->freelists[sh - HEAP_MIN_SHIFT];
    h->freelists[sh - HEAP_MIN_SHIFT] = b;
    LOCK_UN(&h->lock);
}

static void shard_slab_free_all(KuttiDB *c, Shard *s) {
    void *sl = s->slabs;
    while (sl) {
        void *n = *(void **)sl;
        if (!c->embedded) atomic_fetch_sub_explicit(&c->allocated_mem, SLAB_SIZE,
                                                     memory_order_relaxed);
        block_free(c, sl);
        sl = n;
    }
    s->slabs = NULL;
    s->cur = NULL;
    s->left = 0;
    memset(s->freelist, 0, sizeof(s->freelist));
}

/* process-shared mutexes so embed clients share shard locks with the server */
static void lock_init(KuttiDB *c, LOCK_TYPE *l) {
    memset(l, 0, sizeof(*l));
    if (c->embedded) {
        l->shared = 1;
        atomic_store(&l->owner_pid, 0);
    } else {
        pthread_mutex_init(&l->local, NULL);
    }
}

static Entry *entry_alloc(Shard *s, KuttiDB *c, size_t total) {
    size_t got;
    Entry *e;
    if (total > SLAB_MAX_ENTRY) {
        e = block_alloc(c, total);
        if (!e) return NULL;
        e->cls = DIRECT_CLS;
        got = total;
    } else {
        unsigned cls = 0;
        while (class_sz[cls] < total) cls++;
        size_t csz = class_sz[cls];
        if (s->freelist[cls]) {
            e = s->freelist[cls];
            s->freelist[cls] = *(Entry **)e;
        } else {
            if (s->left < csz) {
                char *slab = block_alloc(c, SLAB_SIZE);
                if (!slab) return NULL;
                *(void **)slab = s->slabs;
                s->slabs = slab;
                s->cur = slab + sizeof(void *);
                s->left = SLAB_SIZE - sizeof(void *);
            }
            e = (Entry *)s->cur;
            s->cur += csz;
            s->left -= csz;
        }
        e->cls = (uint16_t)cls;
        got = csz;
    }
    s->mem += got;
    atomic_fetch_add_explicit(&c->total_mem, got, memory_order_relaxed);
    return e;
}

static void entry_free(Shard *s, KuttiDB *c, Entry *e) {
    size_t got;
    if (e->exp)
        atomic_fetch_sub_explicit(&c->ttl_entries, 1, memory_order_relaxed);
    if (e->cls == DIRECT_CLS) {
        got = e->klen + e->vlen + sizeof(Entry);
        if (!c->embedded) atomic_fetch_sub_explicit(&c->allocated_mem, got,
                                                     memory_order_relaxed);
        block_free(c, e);
    } else {
        got = class_sz[e->cls];
        *(Entry **)e = s->freelist[e->cls];
        s->freelist[e->cls] = e;
    }
    s->mem -= got;
    atomic_fetch_sub_explicit(&c->total_mem, got, memory_order_relaxed);
}

static inline uint32_t kuttidb_rand(KuttiDB *c) {
    unsigned long long x = atomic_fetch_add_explicit(
        &c->rseed, 0x9e3779b97f4a7c15ull, memory_order_relaxed);
    x ^= x >> 30; x *= 0xbf58476d1ce4e5b9ull;
    x ^= x >> 27; x *= 0x94d049bb133111ebull;
    x ^= x >> 31;
    return (uint32_t)(x >> 32);
}

/* ---- core -------------------------------------------------------------- */

static inline Shard *shard_for(KuttiDB *c, uint32_t h) {
    return &c->shards[h >> (32 - c->shard_bits)];
}

int init_shards(KuttiDB *c, size_t initial_buckets) {
    if (initial_buckets < 16) initial_buckets = 16;
    size_t rounded = 16;
    while (rounded < initial_buckets && rounded <= SIZE_MAX / 2) rounded <<= 1;
    if (rounded < initial_buckets) return -1;
    initial_buckets = rounded;
    for (size_t i = 0; i < c->nshards; i++) {
        Shard *s = &c->shards[i];
        memset(s, 0, sizeof(Shard));
        s->buckets = block_alloc(c, initial_buckets * sizeof(Entry *));
        if (!s->buckets) return -1;
        memset(s->buckets, 0, initial_buckets * sizeof(Entry *));
        s->nbuckets = initial_buckets;
        s->mem = initial_buckets * sizeof(Entry *);
        lock_init(c, &s->lock);
        c->initialized_shards = i + 1;
    }
    atomic_fetch_add_explicit(&c->total_mem,
        c->nshards * initial_buckets * sizeof(Entry *), memory_order_relaxed);
    return 0;
}

KuttiDB *kuttidb_create(size_t nshards, size_t initial_buckets) {
    if (nshards == 0) nshards = 16;
    int bits = 1;
    while ((size_t)1 << bits < nshards) bits++;
    nshards = (size_t)1 << bits;
    KuttiDB *c = calloc(1, sizeof(KuttiDB));
    if (!c) return NULL;
    c->shards = calloc(nshards, sizeof(Shard));
    if (!c->shards) { free(c); return NULL; }
    c->nshards = nshards;
    c->shard_bits = bits;
    c->rseed = (unsigned long long)time(NULL) << 32 | 0x1234abcd;
    if (init_shards(c, initial_buckets) < 0) { kuttidb_destroy(c); return NULL; }
    return c;
}

void kuttidb_destroy(KuttiDB *c) {
    if (c->embedded) return; /* region outlives callers; detach instead */
    for (size_t i = 0; i < c->initialized_shards; i++) {
        shard_slab_free_all(c, &c->shards[i]);
        if (!c->embedded)
            atomic_fetch_sub_explicit(&c->allocated_mem,
                c->shards[i].nbuckets * sizeof(Entry *), memory_order_relaxed);
        block_free(c, c->shards[i].buckets);
        LOCK_DESTROY(&c->shards[i].lock);
    }
    free(c->shards);
    free(c);
}

static void maybe_resize(Shard *s, KuttiDB *c) {
    if (c->embedded) return; /* avoid crash-sensitive pointer rewrites */
    KuttiDB *s_for_resize_cache = c;
    if (s->count <= s->nbuckets * 2) return;
    size_t newn = s->nbuckets * 2;
    Entry **nb = block_alloc(s_for_resize_cache, newn * sizeof(Entry *));
    if (!nb) return; /* keep old table on OOM */
    memset(nb, 0, newn * sizeof(Entry *));
    for (size_t i = 0; i < s->nbuckets; i++) {
        Entry *e = s->buckets[i];
        while (e) {
            Entry *n = e->next;
            size_t idx = e->hash & (newn - 1);
            e->next = nb[idx];
            nb[idx] = e;
            e = n;
        }
    }
    size_t delta = newn * sizeof(Entry *) - s->nbuckets * sizeof(Entry *);
    s->mem += delta;
    atomic_fetch_add_explicit(&s_for_resize_cache->total_mem, delta,
                              memory_order_relaxed);
    if (!c->embedded)
        atomic_fetch_sub_explicit(&c->allocated_mem,
            s->nbuckets * sizeof(Entry *), memory_order_relaxed);
    block_free(s_for_resize_cache, s->buckets);
    s->buckets = nb;
    s->nbuckets = newn;
}

static inline int is_expired(const Entry *e, uint32_t now) {
    return e->exp && e->exp <= now;
}

/* delete an expired entry found while holding the shard lock (write mode) */
static void remove_locked(Shard *s, KuttiDB *c, Entry **pp) {
    Entry *e = *pp;
    *pp = e->next;
    entry_free(s, c, e);
    s->count--;
    if (s->count == 0) shard_slab_free_all(c, s);
}

void kuttidb_set_budget(KuttiDB *c, unsigned long long budget_bytes) {
    if (c->embedded) {
        embed_kuttidb_set_budget(c, budget_bytes);
        return;
    }
    atomic_store(&c->budget, budget_bytes);
}

/* random eviction until under budget or max iterations. takes one shard
 * lock at a time; safe to call while holding no locks. */
static void evict_under_budget(KuttiDB *c, int max_iter) {
    unsigned long long budget = atomic_load(&c->budget);
    if (!budget) return;
    for (int i = 0; i < max_iter; i++) {
        if (atomic_load(&c->total_mem) <= (unsigned long long)budget) return;
        Shard *s = &c->shards[kuttidb_rand(c) & (c->nshards - 1)];
        LOCK_WR(&s->lock);
        if (s->count) {
            size_t idx = kuttidb_rand(c) & (s->nbuckets - 1);
            for (size_t probe = 0; probe < s->nbuckets; probe++) {
                Entry **pp = &s->buckets[(idx + probe) & (s->nbuckets - 1)];
                if (*pp) {
                    remove_locked(s, c, pp);
                    atomic_fetch_add_explicit(&c->evicted, 1, memory_order_relaxed);
                    break;
                }
            }
        }
        LOCK_UN(&s->lock);
    }
}

uint32_t kuttidb_expiry_from_ttl(uint64_t ttl_ms) {
    if (!ttl_ms) return 0;
    uint64_t now = (uint64_t)time(NULL);
    uint64_t secs = ttl_ms / 1000 + (ttl_ms % 1000 ? 1 : 0);
    uint64_t max_add = 0xFFFFFFF0ull;
    uint64_t target = now + (secs > max_add ? max_add : secs);
    if (target > UINT32_MAX) target = UINT32_MAX;
    if (target <= now && now < UINT32_MAX) target = now + 1;
    return (uint32_t)target;
}

static size_t entry_capacity(const Entry *e) {
    return e->cls == DIRECT_CLS
         ? sizeof(Entry) + (size_t)e->klen + e->vlen
         : class_sz[e->cls];
}

int kuttidb_put_abs(KuttiDB *c, const char *key, uint32_t klen,
                  const char *value, uint32_t vlen, uint32_t exp) {
    if (c->embedded)
        return embed_kuttidb_put_abs(c, key, klen, value, vlen, exp);
    uint32_t h = hash_key(key, klen);
    Shard *s = shard_for(c, h);
    unsigned long long budget = atomic_load(&c->budget);
    if (budget && atomic_load(&c->total_mem) >= budget)
        evict_under_budget(c, (int)(c->nshards * 2));

    LOCK_WR(&s->lock);
    size_t idx = h & (s->nbuckets - 1);
    Entry **find_pp = &s->buckets[idx];
    Entry *e = *find_pp;
    while (e) {
        if (e->hash == h && e->klen == klen && memcmp(e->data, key, klen) == 0) {
            size_t need = sizeof(Entry) + (size_t)klen + vlen;
            if (!c->embedded && need <= entry_capacity(e)) {
                if (!e->exp && exp)
                    atomic_fetch_add_explicit(&c->ttl_entries, 1, memory_order_relaxed);
                else if (e->exp && !exp)
                    atomic_fetch_sub_explicit(&c->ttl_entries, 1, memory_order_relaxed);
                e->vlen = vlen;
                e->exp = exp;
                if (vlen) memcpy(e->data + klen, value, vlen);
                atomic_fetch_add_explicit(&c->revision, 1, memory_order_relaxed);
                LOCK_UN(&s->lock);
                return 0;
            }
            Entry *ne = entry_alloc(s, c, sizeof(Entry) + (size_t)klen + vlen);
            if (!ne) { LOCK_UN(&s->lock); return -1; }
            ne->hash = h;
            ne->klen = klen;
            ne->vlen = vlen;
            ne->exp = exp;
            if (exp) atomic_fetch_add_explicit(&c->ttl_entries, 1,
                                                memory_order_relaxed);
            memcpy(ne->data, key, klen);
            memcpy(ne->data + klen, value, vlen);
            ne->next = e->next;
            *find_pp = ne;
            entry_free(s, c, e);
            atomic_fetch_add_explicit(&c->revision, 1, memory_order_relaxed);
            LOCK_UN(&s->lock);
            if (budget && atomic_load(&c->total_mem) > budget)
                evict_under_budget(c, (int)(c->nshards * 4));
            return 0;
        }
        find_pp = &e->next;
        e = e->next;
    }

    Entry *ne = entry_alloc(s, c, sizeof(Entry) + (size_t)klen + vlen);
    if (!ne) { LOCK_UN(&s->lock); return -1; }
    ne->hash = h;
    ne->klen = klen;
    ne->vlen = vlen;
    ne->exp = exp;
    if (exp) atomic_fetch_add_explicit(&c->ttl_entries, 1, memory_order_relaxed);
    memcpy(ne->data, key, klen);
    memcpy(ne->data + klen, value, vlen);
    ne->next = s->buckets[idx];
    s->buckets[idx] = ne;
    s->count++;
    maybe_resize(s, c);
    atomic_fetch_add_explicit(&c->revision, 1, memory_order_relaxed);
    LOCK_UN(&s->lock);
    if (budget && atomic_load(&c->total_mem) > budget)
        evict_under_budget(c, (int)(c->nshards * 4));
    return 0;
}

int kuttidb_put_ex(KuttiDB *c, const char *key, uint32_t klen,
                 const char *value, uint32_t vlen, uint64_t ttl_ms) {
    return kuttidb_put_abs(c, key, klen, value, vlen,
                         kuttidb_expiry_from_ttl(ttl_ms));
}

int kuttidb_put(KuttiDB *c, const char *key, uint32_t klen,
              const char *value, uint32_t vlen) {
    return kuttidb_put_ex(c, key, klen, value, vlen, 0);
}

static inline int lookup_copy(Shard *s, uint32_t h, const char *key,
                              uint32_t klen, char **out_value,
                              uint32_t *out_vlen, KuttiVec *vec, uint32_t now) {
    if (!vec && (!out_value || !out_vlen)) return -1;
    Entry *e = s->buckets[h & (s->nbuckets - 1)];
    while (e) {
        if (e->hash == h && e->klen == klen && memcmp(e->data, key, klen) == 0) {
            if (is_expired(e, now)) return -2; /* signal: needs wrlock delete */
            uint32_t vlen = e->vlen;
            if (vec) {
                if (kuttidb_vec_reserve(vec, vlen) < 0) return -1;
                if (vlen) memcpy(vec->data + vec->len, e->data + e->klen, vlen);
                vec->len += vlen;
            } else {
                char *copy = malloc(vlen ? vlen : 1);
                if (!copy) return -1;
                memcpy(copy, e->data + e->klen, vlen);
                *out_value = copy;
                *out_vlen = vlen;
            }
            return 1;
        }
        e = e->next;
    }
    return 0;
}

static int get_common(KuttiDB *c, const char *key, uint32_t klen,
                      char **out_value, uint32_t *out_vlen, KuttiVec *vec,
                      uint32_t now) {
    if (c->embedded)
        return embed_kuttidb_get(c, key, klen, out_value, out_vlen, vec, now);
    if (!now) now = (uint32_t)time(NULL);
    uint32_t h = hash_key(key, klen);
    Shard *s = shard_for(c, h);
    LOCK_RD(&s->lock);
    int rc = lookup_copy(s, h, key, klen, out_value, out_vlen, vec, now);
    LOCK_UN(&s->lock);
    if (rc != -2) return rc == -1 ? -1 : rc;

    /* expired: upgrade to write lock and delete */
    LOCK_WR(&s->lock);
    size_t idx = h & (s->nbuckets - 1);
    Entry **pp = &s->buckets[idx];
    while (*pp) {
        if ((*pp)->hash == h && (*pp)->klen == klen &&
            memcmp((*pp)->data, key, klen) == 0) {
            remove_locked(s, c, pp);
            atomic_fetch_add_explicit(&c->expired, 1, memory_order_relaxed);
            break;
        }
        pp = &(*pp)->next;
    }
    LOCK_UN(&s->lock);
    return 0;
}

int kuttidb_get(KuttiDB *c, const char *key, uint32_t klen,
              char **out_value, uint32_t *out_vlen) {
    if (!out_value || !out_vlen) return -1;
    return get_common(c, key, klen, out_value, out_vlen, NULL, 0);
}

int kuttidb_get_into(KuttiDB *c, const char *key, uint32_t klen, KuttiVec *vec) {
    return get_common(c, key, klen, NULL, NULL, vec, 0);
}

int kuttidb_get_into_at(KuttiDB *c, const char *key, uint32_t klen, KuttiVec *vec,
                      uint32_t now_sec) {
    return get_common(c, key, klen, NULL, NULL, vec, now_sec);
}

int kuttidb_delete(KuttiDB *c, const char *key, uint32_t klen) {
    if (c->embedded) return embed_kuttidb_delete(c, key, klen);
    uint32_t h = hash_key(key, klen);
    Shard *s = shard_for(c, h);
    LOCK_WR(&s->lock);

    Entry **p = &s->buckets[h & (s->nbuckets - 1)];
    while (*p) {
        Entry *e = *p;
        if (e->hash == h && e->klen == klen && memcmp(e->data, key, klen) == 0) {
            *p = e->next;
            entry_free(s, c, e);
            s->count--;
            if (s->count == 0) shard_slab_free_all(c, s);
            atomic_fetch_add_explicit(&c->revision, 1, memory_order_relaxed);
            LOCK_UN(&s->lock);
            return 1;
        }
        p = &e->next;
    }
    LOCK_UN(&s->lock);
    return 0;
}

void kuttidb_sweep_expired(KuttiDB *c, size_t bucket_work) {
    if (c->embedded) {
        embed_kuttidb_sweep_expired(c, bucket_work);
        return;
    }
    if (!atomic_load_explicit(&c->ttl_entries, memory_order_relaxed)) return;
    uint32_t now = (uint32_t)time(NULL);
    for (size_t k = 0; k < bucket_work; k++) {
        size_t ticket = atomic_fetch_add(&c->sweep_cursor, 1);
        size_t i = ticket & (c->nshards - 1);
        Shard *s = &c->shards[i];
        LOCK_WR(&s->lock);
        size_t b = (ticket / c->nshards) & (s->nbuckets - 1);
        Entry **pp = &s->buckets[b];
        while (*pp) {
            if (is_expired(*pp, now)) {
                remove_locked(s, c, pp);
                atomic_fetch_add_explicit(&c->revision, 1, memory_order_relaxed);
                atomic_fetch_add_explicit(&c->expired, 1, memory_order_relaxed);
            } else {
                pp = &(*pp)->next;
            }
        }
        LOCK_UN(&s->lock);
    }
}

uint64_t kuttidb_revision(KuttiDB *c) {
    if (!c || c->embedded) return 0;
    return atomic_load_explicit(&c->revision, memory_order_relaxed);
}

size_t kuttidb_count(KuttiDB *c) {
    if (c->embedded) return embed_kuttidb_count(c);
    size_t n = 0;
    for (size_t i = 0; i < c->nshards; i++) {
        LOCK_RD(&c->shards[i].lock);
        n += c->shards[i].count;
        LOCK_UN(&c->shards[i].lock);
    }
    return n;
}

size_t kuttidb_memusage(KuttiDB *c) {
    if (c->embedded) return embed_kuttidb_memusage(c);
    return (size_t)atomic_load_explicit(&c->total_mem, memory_order_relaxed);
}

size_t kuttidb_allocated(KuttiDB *c) {
    if (c->embedded) return embed_kuttidb_allocated(c);
    return (size_t)atomic_load_explicit(&c->allocated_mem, memory_order_relaxed);
}

unsigned long long kuttidb_expired_count(KuttiDB *c) {
    if (c->embedded) return embed_kuttidb_expired_count(c);
    return atomic_load(&c->expired);
}

unsigned long long kuttidb_evicted_count(KuttiDB *c) {
    if (c->embedded) return embed_kuttidb_evicted_count(c);
    return atomic_load(&c->evicted);
}

int kuttidb_foreach(KuttiDB *c,
                  int (*cb)(const char *k, uint32_t klen,
                            const char *v, uint32_t vlen,
                            uint32_t exp, void *ctx),
                  void *ctx) {
    if (c->embedded) return embed_kuttidb_foreach(c, cb, ctx);
    uint32_t now = (uint32_t)time(NULL);
    KuttiVec stage = {0};
    int result = 0;
    for (size_t i = 0; i < c->nshards; i++) {
        Shard *s = &c->shards[i];
        int stop = 0;
        stage.len = 0;
        LOCK_RD(&s->lock);
        for (size_t b = 0; b < s->nbuckets && !stop; b++) {
            for (Entry *e = s->buckets[b]; e; e = e->next) {
                if (is_expired(e, now)) continue;
                size_t n = 12 + (size_t)e->klen + e->vlen;
                if (kuttidb_vec_reserve(&stage, n) < 0) { stop = -1; break; }
                if (!stage.data) { stop = -1; break; }
                unsigned char *p = (unsigned char *)stage.data + stage.len;
                memcpy(p, &e->klen, 4); memcpy(p + 4, &e->vlen, 4);
                memcpy(p + 8, &e->exp, 4);
                memcpy(p + 12, e->data, (size_t)e->klen + e->vlen);
                stage.len += n;
            }
        }
        LOCK_UN(&s->lock);
        size_t off = 0;
        while (!stop && off < stage.len) {
            uint32_t klen, vlen, exp;
            memcpy(&klen, stage.data + off, 4);
            memcpy(&vlen, stage.data + off + 4, 4);
            memcpy(&exp, stage.data + off + 8, 4);
            const char *kptr = stage.data + off + 12;
            stop = cb(kptr, klen, kptr + klen, vlen, exp, ctx);
            off += 12 + (size_t)klen + vlen;
        }
        if (stop) { result = stop; break; }
    }
    free(stage.data);
    return result;
}

int kuttidb_foreach_metadata(KuttiDB *c, uint32_t max_entries,
                             KuttiDBMetadataFn fn, void *ctx) {
    if (!c || !fn || !max_entries || c->embedded) return -1;
    uint32_t now = (uint32_t)time(NULL), delivered = 0;
    for (size_t i = 0; i < c->nshards && delivered < max_entries; i++) {
        Shard *s = &c->shards[i];
        LOCK_RD(&s->lock);
        for (size_t b = 0; b < s->nbuckets && delivered < max_entries; b++) {
            for (Entry *e = s->buckets[b]; e && delivered < max_entries; e = e->next) {
                if (is_expired(e, now)) continue;
                if (fn(e->data, e->klen, e->vlen, e->exp, ctx)) {
                    LOCK_UN(&s->lock);
                    return (int)delivered;
                }
                delivered++;
            }
        }
        LOCK_UN(&s->lock);
    }
    return (int)delivered;
}

int kuttidb_foreach_metadata_page_filtered(KuttiDB *c,
                                  const KuttiDBMetadataCursor *start,
                                  uint32_t max_entries, KuttiDBMetadataFn fn,
                                  KuttiDBMetadataMatchFn match, void *ctx, KuttiDBMetadataCursor *next,
                                  int *more) {
    if (!c || !fn || !max_entries || !next || !more || c->embedded) return -1;
    KuttiDBMetadataCursor position = start ? *start : (KuttiDBMetadataCursor){0};
    if (position.shard >= c->nshards) { *next = position; *more = 0; return 0; }
    uint32_t now=(uint32_t)time(NULL), delivered=0; int found_more=0;
    for (size_t i=position.shard;i<c->nshards;i++) {
        Shard *s=&c->shards[i]; LOCK_RD(&s->lock);
        size_t first_bucket=i==position.shard?position.bucket:0;
        for (size_t b=first_bucket;b<s->nbuckets;b++) {
            uint32_t entry_index=0, skip=(i==position.shard&&b==position.bucket)?position.entry:0;
            for (Entry *e=s->buckets[b];e;e=e->next,entry_index++) {
                if (entry_index < skip || is_expired(e,now) ||
                    (match && !match(e->data,e->klen,e->vlen,e->exp,ctx))) continue;
                if (delivered == max_entries) { found_more=1; break; }
                if (fn(e->data,e->klen,e->vlen,e->exp,ctx)) { LOCK_UN(&s->lock); return (int)delivered; }
                delivered++;
                *next=(KuttiDBMetadataCursor){(uint32_t)i,(uint32_t)b,entry_index+1};
            }
            if (found_more) break;
            position=(KuttiDBMetadataCursor){(uint32_t)i,(uint32_t)b+1,0};
        }
        LOCK_UN(&s->lock);
        if (found_more) break;
        position=(KuttiDBMetadataCursor){(uint32_t)i+1,0,0};
    }
    if (!delivered) *next=position;
    *more=found_more;
    return (int)delivered;
}

int kuttidb_foreach_metadata_page(KuttiDB *c,
                                  const KuttiDBMetadataCursor *start,
                                  uint32_t max_entries, KuttiDBMetadataFn fn,
                                  void *ctx, KuttiDBMetadataCursor *next,
                                  int *more) {
    return kuttidb_foreach_metadata_page_filtered(c,start,max_entries,fn,NULL,
                                                   ctx,next,more);
}
