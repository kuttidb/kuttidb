#ifndef KUTTIDB_EMBED_INT_H
#define KUTTIDB_EMBED_INT_H

#include "kuttidb.h"
#include "kuttidb_int.h"

/*
 * CEMBv3 is deliberately a fixed-width, offset-only on-disk/shared-memory
 * format.  No field below stores a process virtual address.  The KuttiDB object
 * returned to each process is private; it merely supplies the local mapping
 * base used to resolve these offsets.
 */
#define EMBED_MAGIC "CEMBv3"
#define EMBED_MAGIC_LEN 6
#define EMBED_ALIGN 16u

typedef struct EmbedHeader {
    char magic[8];
    uint32_t version;
    uint32_t header_size;
    uint64_t region_size;
    uint64_t kuttidb_off;
    char wal_path[512];
    uint8_t reserved[64];
} EmbedHeader;

typedef struct EmbedLock {
    _Atomic int owner_pid;
} EmbedLock;

typedef struct EmbedHeap {
    EmbedLock lock;
    uint64_t bump_off;
    uint64_t end_off;
    uint64_t free_off;
} EmbedHeap;

typedef struct EmbedShard {
    uint64_t buckets_off; /* uint64_t offsets to EmbedEntry */
    uint64_t nbuckets;
    uint64_t count;
    uint64_t mem;
    EmbedLock lock;
} EmbedShard;

typedef struct EmbedShared {
    uint64_t shards_off;
    uint64_t nshards;
    uint64_t initialized_shards;
    uint32_t shard_bits;
    uint32_t format_flags;
    EmbedHeap heap;
    _Atomic uint64_t budget;
    _Atomic uint64_t total_mem;
    _Atomic uint64_t allocated_mem;
    _Atomic uint64_t expired;
    _Atomic uint64_t evicted;
    _Atomic uint64_t ttl_entries;
    _Atomic uint64_t rseed;
    _Atomic uint64_t sweep_cursor;
} EmbedShared;

typedef struct EmbedBlock {
    uint64_t next_off;
    uint64_t capacity;
} EmbedBlock;

typedef struct EmbedEntry {
    uint64_t next_off;
    uint32_t exp;
    uint32_t hash;
    uint32_t klen;
    uint32_t vlen;
    char data[];
} EmbedEntry;

static inline uint64_t embed_align(uint64_t n) {
    return (n + (EMBED_ALIGN - 1)) & ~(uint64_t)(EMBED_ALIGN - 1);
}

static inline void *embed_ptr(const KuttiDB *c, uint64_t off) {
    if (!off || off >= c->region_size) return NULL;
    return (char *)c->region + off;
}

static inline uint64_t embed_off(const KuttiDB *c, const void *ptr) {
    if (!ptr) return 0;
    return (uint64_t)((const char *)ptr - (const char *)c->region);
}

static inline EmbedHeader *embed_header(const KuttiDB *c) {
    return c && c->embedded ? (EmbedHeader *)c->region : NULL;
}

static inline EmbedShared *embed_shared(const KuttiDB *c) {
    EmbedHeader *header = embed_header(c);
    return header ? embed_ptr(c, header->kuttidb_off) : NULL;
}

int embed_kuttidb_init(KuttiDB *c, uint64_t kuttidb_off, size_t nshards);
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
