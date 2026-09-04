#ifndef KUTTIDB_H
#define KUTTIDB_H

#include <stddef.h>
#include <stdint.h>
#include <pthread.h>

typedef struct KuttiDB KuttiDB;

/* growable byte vector shared with kuttidb_get_into (zero-copy reads) */
typedef struct { char *data; size_t len, cap; } KuttiVec;
int kuttidb_vec_reserve(KuttiVec *v, size_t extra);

KuttiDB *kuttidb_create(size_t nshards, size_t initial_buckets_per_shard);
void kuttidb_destroy(KuttiDB *c);

/* returns 0 on success, -1 on error (OOM) */
int kuttidb_put(KuttiDB *c, const char *key, uint32_t klen,
              const char *value, uint32_t vlen);

/* put with expiry: ttl_ms > 0 makes the record expire after ttl_ms */
int kuttidb_put_ex(KuttiDB *c, const char *key, uint32_t klen,
                 const char *value, uint32_t vlen, uint64_t ttl_ms);

/* persistence/recovery path: exp is an absolute Unix timestamp in seconds. */
int kuttidb_put_abs(KuttiDB *c, const char *key, uint32_t klen,
                  const char *value, uint32_t vlen, uint32_t exp);
uint32_t kuttidb_expiry_from_ttl(uint64_t ttl_ms);

/* returns 1 and fills out_value/out_vlen on hit (caller must free), 0 on miss */
int kuttidb_get(KuttiDB *c, const char *key, uint32_t klen,
              char **out_value, uint32_t *out_vlen);

/* zero-copy read: appends the value bytes directly to vec under the shard
 * lock. returns 1 hit, 0 miss, -1 OOM. vec->len advances on hit. */
int kuttidb_get_into(KuttiDB *c, const char *key, uint32_t klen, KuttiVec *vec);
int kuttidb_get_into_at(KuttiDB *c, const char *key, uint32_t klen, KuttiVec *vec,
                      uint32_t now_sec);

int kuttidb_delete(KuttiDB *c, const char *key, uint32_t klen);

/* memory budget: when total record memory exceeds budget, random records are
 * evicted on writes until back under. 0 = unlimited (default). */
void kuttidb_set_budget(KuttiDB *c, unsigned long long budget_bytes);

/* active expiry sweep: checks at most `bucket_work` buckets. */
void kuttidb_sweep_expired(KuttiDB *c, size_t bucket_work);

size_t kuttidb_count(KuttiDB *c);
size_t kuttidb_memusage(KuttiDB *c);
size_t kuttidb_allocated(KuttiDB *c);
unsigned long long kuttidb_expired_count(KuttiDB *c);
unsigned long long kuttidb_evicted_count(KuttiDB *c);

/* iterate all live (non-expired) records; cb returns nonzero to stop early */
int kuttidb_foreach(KuttiDB *c,
                  int (*cb)(const char *k, uint32_t klen,
                            const char *v, uint32_t vlen,
                            uint32_t exp, void *ctx),
                  void *ctx);

/* Bounded metadata-only scan. The callback runs while one shard read lock is
 * held and must copy anything it needs; no value bytes are exposed and no
 * database-sized staging allocation is performed. Returns the number of
 * entries delivered, or -1 when unsupported or invalid. */
typedef int (*KuttiDBMetadataFn)(const char *key, uint32_t key_len,
                                 uint32_t value_len, uint32_t expiry,
                                 void *ctx);
typedef int (*KuttiDBMetadataMatchFn)(const char *key, uint32_t key_len,
                                      uint32_t value_len, uint32_t expiry,
                                      void *ctx);
/* Private-to-the-caller continuation state for a bounded metadata scan. It
 * intentionally names only traversal indexes, never an Entry pointer. */
typedef struct KuttiDBMetadataCursor {
    uint32_t shard, bucket, entry;
} KuttiDBMetadataCursor;
int kuttidb_foreach_metadata(KuttiDB *c, uint32_t max_entries,
                             KuttiDBMetadataFn fn, void *ctx);
/* Resume a metadata scan after `start`. `next` is valid only when `more` is
 * non-zero. The callback receives at most max_entries live entries. */
int kuttidb_foreach_metadata_page(KuttiDB *c,
                                  const KuttiDBMetadataCursor *start,
                                  uint32_t max_entries, KuttiDBMetadataFn fn,
                                  void *ctx, KuttiDBMetadataCursor *next,
                                  int *more);
int kuttidb_foreach_metadata_page_filtered(KuttiDB *c,
                                  const KuttiDBMetadataCursor *start,
                                  uint32_t max_entries, KuttiDBMetadataFn fn,
                                  KuttiDBMetadataMatchFn match, void *ctx,
                                  KuttiDBMetadataCursor *next, int *more);
/* Process-lifetime mutation epoch for scan change detection. */
uint64_t kuttidb_revision(KuttiDB *c);

#endif
