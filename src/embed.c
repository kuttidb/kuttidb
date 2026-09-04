/* Embedded shared-memory mode.
 *
 * The shared cache state (shards, locks, heap, bucket arrays) lives inside one
 * mmap'd file. Every process receives a private KuttiDB wrapper and resolves
 * CEMBv3's relative offsets against its own mapping base.
 *
 * Address discipline: the mapped format contains no process pointers. An
 * attacher may map it at any address, so ASLR and unrelated mappings cannot
 * prevent a trusted local client from using the direct path.
 *
 * Durability: embed writers append the same WAL records the server writes,
 * under an flock on the WAL file, so crash recovery is identical no matter
 * which frontend wrote the data.
 */

#include "kuttidb.h"
#include "embed_int.h"
#include "embed.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/file.h>
#include <sys/uio.h>
#include <sys/stat.h>
#include <time.h>

#define EMBED_REGION_SIZE (1ull << 30) /* default: 1 GiB sparse mapping */
#define EMBED_REGION_MIN  (16ull << 20)

static uint32_t embed_crc(const void *a, size_t alen, const void *b, size_t blen,
                          const void *d, size_t dlen) {
    static uint32_t table[256];
    static int init = 0;
    if (!init) {
        for (uint32_t i = 0; i < 256; i++) {
            uint32_t cc = i;
            for (int j = 0; j < 8; j++)
                cc = (cc & 1) ? 0xEDB88320u ^ (cc >> 1) : cc >> 1;
            table[i] = cc;
        }
        init = 1;
    }
    uint32_t c = 0xFFFFFFFFu;
    const unsigned char *p = a; size_t n = alen;
    while (n--) c = table[(c ^ *p++) & 0xFF] ^ (c >> 8);
    p = b; n = blen;
    while (n--) c = table[(c ^ *p++) & 0xFF] ^ (c >> 8);
    p = d; n = dlen;
    while (n--) c = table[(c ^ *p++) & 0xFF] ^ (c >> 8);
    return c ^ 0xFFFFFFFFu;
}

struct KuttiEmbed {
    KuttiDB *db;
    EmbedHeader *hdr;
    void *region;
    int wal_fd;
    int wal_failed;
};

static void *map_file(int fd, size_t size) {
    return mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
}

static KuttiDB *build_region(void *region, size_t region_size, size_t nshards,
                           const char *wal_path) {
    EmbedHeader *hdr = (EmbedHeader *)region;
    memset(hdr, 0, sizeof(EmbedHeader));
    hdr->region_size = region_size;
    hdr->version = 3;
    hdr->header_size = sizeof(*hdr);
    hdr->kuttidb_off = embed_align(sizeof(*hdr));
    if (hdr->kuttidb_off >= region_size) return NULL;

    KuttiDB *c = calloc(1, sizeof(*c));
    if (!c) return NULL;
    c->embedded = 1;
    c->region = region;
    c->region_size = region_size;
    if (wal_path)
        snprintf(hdr->wal_path, sizeof hdr->wal_path, "%s", wal_path);
    if (embed_kuttidb_init(c, hdr->kuttidb_off, nshards) < 0) {
        free(c);
        return NULL;
    }
    atomic_thread_fence(memory_order_release);
    memcpy(hdr->magic, EMBED_MAGIC, EMBED_MAGIC_LEN); /* creation commit marker */
    msync(hdr, sizeof(*hdr), MS_ASYNC);
    return c;
}

/* process-local registry: repeated opens in one process reuse the mapping */
#define REG_CAP 16
static struct {
    char path[512];
    void *region;
    size_t size;
    unsigned refs;
    KuttiDB *db;
} g_reg[REG_CAP];
static pthread_mutex_t g_reg_mu = PTHREAD_MUTEX_INITIALIZER;

static KuttiDB *registry_find(const char *path) {
    KuttiDB *found = NULL;
    pthread_mutex_lock(&g_reg_mu);
    for (int i = 0; i < REG_CAP; i++) {
        if (g_reg[i].region && strcmp(g_reg[i].path, path) == 0) {
            found = g_reg[i].db;
            g_reg[i].refs++;
            break;
        }
    }
    pthread_mutex_unlock(&g_reg_mu);
    return found;
}

static void registry_add(const char *path, void *region, size_t size, KuttiDB *db) {
    pthread_mutex_lock(&g_reg_mu);
    for (int i = 0; i < REG_CAP; i++) {
        if (!g_reg[i].region) {
            snprintf(g_reg[i].path, sizeof g_reg[i].path, "%s", path);
            g_reg[i].region = region;
            g_reg[i].size = size;
            g_reg[i].refs = 1;
            g_reg[i].db = db;
            break;
        }
    }
    pthread_mutex_unlock(&g_reg_mu);
}


KuttiDB *kuttidb_embed_create_sized(const char *path, size_t nshards,
                                const char *wal_path, size_t region_size) {
    if (region_size < EMBED_REGION_MIN) {
        fprintf(stderr, "embed: region must be at least 16 MiB\n");
        return NULL;
    }
    region_size = (region_size + 4095) & ~(size_t)4095;
    struct stat existing;
    if (lstat(path, &existing) == 0) {
        if (!S_ISREG(existing.st_mode) || existing.st_uid != geteuid()) {
            fprintf(stderr, "embed: refusing non-regular or foreign region %s\n", path);
            return NULL;
        }
        KuttiDB *c = kuttidb_embed_attach(path);
        if (c) return c;
        fprintf(stderr, "embed: existing region is invalid; refusing to overwrite %s\n", path);
        return NULL;
    } else if (errno != ENOENT) {
        perror("embed: lstat");
        return NULL;
    }

    int fd = open(path, O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW, 0600);
    if (fd < 0) { perror("embed: open"); return NULL; }
    fcntl(fd, F_SETFD, FD_CLOEXEC);
    if (fchmod(fd, 0600) < 0) { perror("embed: chmod"); close(fd); return NULL; }
    if (ftruncate(fd, (off_t)region_size) < 0) { perror("embed: ftruncate"); close(fd); return NULL; }

    void *region = map_file(fd, region_size);
    if (region == MAP_FAILED) { perror("embed: mmap"); close(fd); return NULL; }
    close(fd);

    KuttiDB *c = build_region(region, region_size, nshards, wal_path);
    if (!c) { munmap(region, region_size); return NULL; }
    registry_add(path, region, region_size, c);
    return c;
}

KuttiDB *kuttidb_embed_create(const char *path, size_t nshards, const char *wal_path) {
    return kuttidb_embed_create_sized(path, nshards, wal_path, EMBED_REGION_SIZE);
}

KuttiDB *kuttidb_embed_attach(const char *path) {
    KuttiDB *cached = registry_find(path);
    if (cached) return cached;
    int fd = open(path, O_RDWR | O_NOFOLLOW);
    if (fd < 0) return NULL;
    struct stat st;
    if (fstat(fd, &st) < 0 || !S_ISREG(st.st_mode) || st.st_uid != geteuid() ||
        (uint64_t)st.st_size < EMBED_REGION_MIN) {
        close(fd);
        return NULL;
    }
    if (fchmod(fd, 0600) < 0) { close(fd); return NULL; }
    fcntl(fd, F_SETFD, FD_CLOEXEC);
    EmbedHeader probe;
    if (pread(fd, &probe, sizeof probe, 0) != (ssize_t)sizeof probe ||
        memcmp(probe.magic, EMBED_MAGIC, EMBED_MAGIC_LEN) != 0 ||
        probe.version != 3 || probe.header_size != sizeof(probe) ||
        probe.region_size != (uint64_t)st.st_size ||
        probe.kuttidb_off < sizeof(probe) ||
        probe.kuttidb_off > probe.region_size - sizeof(EmbedShared)) {
        close(fd);
        return NULL;
    }
    void *region = map_file(fd, (size_t)probe.region_size);
    close(fd);
    if (region == MAP_FAILED) { perror("embed: attach mmap"); return NULL; }
    KuttiDB *c = calloc(1, sizeof(*c));
    if (!c) { munmap(region, (size_t)probe.region_size); return NULL; }
    c->embedded = 1;
    c->region = region;
    c->region_size = (size_t)probe.region_size;
    registry_add(path, region, (size_t)probe.region_size, c);
    return c;
}

void kuttidb_embed_detach(KuttiDB *c) {
    if (!c || !c->embedded) return;
    pthread_mutex_lock(&g_reg_mu);
    for (int i = 0; i < REG_CAP; i++) {
        if (g_reg[i].region == c->region) {
            if (--g_reg[i].refs == 0) {
                void *region = g_reg[i].region;
                size_t size = g_reg[i].size;
                KuttiDB *owned_db = g_reg[i].db;
                g_reg[i].region = NULL;
                g_reg[i].path[0] = 0;
                g_reg[i].db = NULL;
                pthread_mutex_unlock(&g_reg_mu);
                munmap(region, size);
                free(owned_db);
                return;
            }
            pthread_mutex_unlock(&g_reg_mu);
            return;
        }
    }
    pthread_mutex_unlock(&g_reg_mu);
}

uintptr_t kuttidb_embed_mapping_address(const KuttiDB *c) {
    return c && c->embedded ? (uintptr_t)c->region : 0;
}

size_t kuttidb_embed_mapping_size(const KuttiDB *c) {
    return c && c->embedded ? c->region_size : 0;
}

/* ---- embed client: cache handle + WAL append under flock ---- */

KuttiEmbed *kuttidb_embed_open(const char *path) {
    KuttiDB *c = kuttidb_embed_attach(path);
    if (!c) return NULL;
    EmbedHeader *hdr = (EmbedHeader *)c->region;
    KuttiEmbed *ec = calloc(1, sizeof(KuttiEmbed));
    if (!ec) { kuttidb_embed_detach(c); return NULL; }
    ec->db = c;
    ec->hdr = hdr;
    ec->region = c->region;
    ec->wal_fd = -1;
    if (hdr->wal_path[0]) {
        ec->wal_fd = open(hdr->wal_path,
                          O_WRONLY | O_CREAT | O_APPEND | O_NOFOLLOW, 0600);
        if (ec->wal_fd >= 0) {
            if (fchmod(ec->wal_fd, 0600) < 0) {
                close(ec->wal_fd);
                ec->wal_fd = -1;
            }
        }
        if (ec->wal_fd >= 0) {
            fcntl(ec->wal_fd, F_SETFD, FD_CLOEXEC);
        }
        if (ec->wal_fd < 0) {
            perror("embed: wal open");
            kuttidb_embed_detach(c);
            free(ec);
            return NULL;
        }
    }
    return ec;
}

KuttiDB *kuttidb_embed_cache(KuttiEmbed *ec) { return ec ? ec->db : NULL; }

static int embed_wal_log(KuttiEmbed *ec, unsigned char op, const char *key,
                         uint32_t klen, const char *val, uint32_t vlen,
                         uint32_t meta) {
    if (ec->wal_fd < 0) return 0;
    int extra = (op == 0x05 || op == 0x07) ? 4 : 0;
    unsigned char hdr[15];
    hdr[0] = op;
    hdr[1] = klen & 0xff;
    hdr[2] = (klen >> 8) & 0xff;
    uint32_t v = vlen;
    hdr[3] = v & 0xff; hdr[4] = (v >> 8) & 0xff; hdr[5] = (v >> 16) & 0xff; hdr[6] = (v >> 24) & 0xff;
    uint32_t crc;
    if (extra) {
        unsigned char tb[4];
        tb[0] = meta & 0xff; tb[1] = (meta >> 8) & 0xff;
        tb[2] = (meta >> 16) & 0xff; tb[3] = (meta >> 24) & 0xff;
        crc = embed_crc(tb, 4, key, klen, val, vlen);
        hdr[11] = tb[0]; hdr[12] = tb[1]; hdr[13] = tb[2]; hdr[14] = tb[3];
    } else {
        crc = embed_crc(key, klen, val, vlen, NULL, 0);
    }
    hdr[7] = crc & 0xff; hdr[8] = (crc >> 8) & 0xff; hdr[9] = (crc >> 16) & 0xff; hdr[10] = (crc >> 24) & 0xff;

    size_t hlen = 11 + extra;

    struct iovec iov[3];
    int n = 0;
    iov[n].iov_base = hdr; iov[n++].iov_len = hlen;
    iov[n].iov_base = (void *)key; iov[n++].iov_len = klen;
    if (vlen) { iov[n].iov_base = (void *)val; iov[n++].iov_len = vlen; }

    int rc = 0;
    int at = 0;
    while (at < n) {
        ssize_t r = writev(ec->wal_fd, &iov[at], n - at);
        if (r < 0) {
            if (errno == EINTR) continue;
            rc = -1;
            break;
        }
        if (r == 0) { rc = -1; break; }
        size_t used = (size_t)r;
        while (at < n && used >= iov[at].iov_len) {
            used -= iov[at].iov_len;
            at++;
        }
        if (at < n && used) {
            iov[at].iov_base = (char *)iov[at].iov_base + used;
            iov[at].iov_len -= used;
        }
    }
    return rc;
}

int kuttidb_embed_put(KuttiEmbed *ec, const char *key, uint32_t klen,
                    const char *val, uint32_t vlen, uint64_t ttl_ms) {
    if (!ec) return -1;
    if (ec->wal_failed) return -1;
    uint32_t exp = kuttidb_expiry_from_ttl(ttl_ms);
    if (ec->wal_fd < 0)
        return kuttidb_put_abs(ec->db, key, klen, val, vlen, exp);
    if (flock(ec->wal_fd, LOCK_EX) < 0) { ec->wal_failed = 1; return -1; }
    int rc = kuttidb_put_abs(ec->db, key, klen, val, vlen, exp);
    if (rc == 0)
        rc = embed_wal_log(ec, exp ? 0x07 : 0x01, key, klen, val, vlen, exp);
    flock(ec->wal_fd, LOCK_UN);
    if (rc < 0) ec->wal_failed = 1;
    return rc;
}

int kuttidb_embed_delete(KuttiEmbed *ec, const char *key, uint32_t klen) {
    if (!ec) return -1;
    if (ec->wal_failed) return -1;
    if (ec->wal_fd >= 0 && flock(ec->wal_fd, LOCK_EX) < 0) {
        ec->wal_failed = 1;
        return -1;
    }
    int found = kuttidb_delete(ec->db, key, klen);
    if (found) {
        if (embed_wal_log(ec, 0x03, key, klen, "", 0, 0) < 0) found = -1;
    }
    if (ec->wal_fd >= 0) flock(ec->wal_fd, LOCK_UN);
    if (found < 0) ec->wal_failed = 1;
    return found;
}

void kuttidb_free_value(void *p) { free(p); }

void kuttidb_embed_close(KuttiEmbed *ec) {
    if (!ec) return;
    if (ec->wal_fd >= 0) close(ec->wal_fd);
    kuttidb_embed_detach(ec->db);
    free(ec);
}
