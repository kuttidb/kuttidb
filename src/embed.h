#ifndef KUTTIDB_EMBED_H
#define KUTTIDB_EMBED_H

#include "kuttidb.h"

/* Shared-memory embedded mode. The server creates/attaches the region and
 * serves network clients from the same table; embed clients map it directly
 * (no socket, no syscall on the data path). */

/* create (or re-attach if the file already holds a valid region) */
KuttiDB *kuttidb_embed_create(const char *path, size_t nshards, const char *wal_path);
KuttiDB *kuttidb_embed_create_sized(const char *path, size_t nshards,
                                const char *wal_path, size_t region_size);

/* map an existing region; NULL on error */
KuttiDB *kuttidb_embed_attach(const char *path);

void kuttidb_embed_detach(KuttiDB *c);

/* Diagnostics for trusted-local attachment tests and observability.  The
 * address is process-local and must never be persisted or sent to a peer. */
uintptr_t kuttidb_embed_mapping_address(const KuttiDB *c);
size_t kuttidb_embed_mapping_size(const KuttiDB *c);

/* convenience client: cache handle + WAL append under flock */
typedef struct KuttiEmbed KuttiEmbed;

KuttiEmbed *kuttidb_embed_open(const char *path);
KuttiDB *kuttidb_embed_cache(KuttiEmbed *ec);
int kuttidb_embed_put(KuttiEmbed *ec, const char *key, uint32_t klen,
                    const char *val, uint32_t vlen, uint64_t ttl_ms);
int kuttidb_embed_delete(KuttiEmbed *ec, const char *key, uint32_t klen);
void kuttidb_embed_close(KuttiEmbed *ec);

/* free a value buffer returned by kuttidb_get (same allocator domain) */
void kuttidb_free_value(void *p);

#endif
