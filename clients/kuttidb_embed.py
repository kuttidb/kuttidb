"""Zero-syscall embedded client for kuttidb shared-memory regions.

Attaches to the region the server created (same file), then does put/get/
delete by touching memory directly - no socket, no wire protocol.

    from kuttidb_embed import KuttiEmbed
    db = KuttiEmbed("/tmp/mydb.embed")
    db.put("k", b"v")
    print(db.get("k"))

Requires libkuttidb_embed.dylib (built by `make`). CEMBv3 uses relative offsets,
so the process may map the region at any ASLR-selected address.
"""

import ctypes as ct
import os

_UINT32_MAX = 0xFFFFFFFF


class KuttiEmbedError(Exception):
    pass


def _load_lib():
    here = os.path.dirname(os.path.abspath(__file__))
    for cand in (os.path.join(here, "..", "libkuttidb_embed.dylib"),
                 os.path.join(here, "..", "libkuttidb_embed.so"),
                 "libkuttidb_embed.dylib",
                 "libkuttidb_embed.so"):
        if os.path.exists(cand) or not os.path.isabs(cand):
            try:
                return ct.CDLL(cand)
            except OSError:
                continue
    raise KuttiEmbedError("libkuttidb_embed not found; run `make` first")


class KuttiEmbed:
    def __init__(self, path):
        self.lib = _load_lib()
        lib = self.lib
        libc = ct.CDLL(None)

        lib.kuttidb_embed_open.restype = ct.c_void_p
        lib.kuttidb_embed_open.argtypes = [ct.c_char_p]
        lib.kuttidb_embed_cache.restype = ct.c_void_p
        lib.kuttidb_embed_cache.argtypes = [ct.c_void_p]
        lib.kuttidb_embed_close.argtypes = [ct.c_void_p]
        lib.kuttidb_embed_put.restype = ct.c_int
        lib.kuttidb_embed_put.argtypes = [
            ct.c_void_p, ct.c_char_p, ct.c_uint32, ct.c_char_p,
            ct.c_uint32, ct.c_uint64]
        lib.kuttidb_embed_delete.restype = ct.c_int
        lib.kuttidb_embed_delete.argtypes = [ct.c_void_p, ct.c_char_p, ct.c_uint32]
        lib.kuttidb_get.restype = ct.c_int
        lib.kuttidb_get.argtypes = [
            ct.c_void_p, ct.c_char_p, ct.c_uint32,
            ct.POINTER(ct.c_void_p), ct.POINTER(ct.c_uint32)]
        lib.kuttidb_count.restype = ct.c_size_t
        lib.kuttidb_count.argtypes = [ct.c_void_p]
        lib.kuttidb_memusage.restype = ct.c_size_t
        lib.kuttidb_memusage.argtypes = [ct.c_void_p]
        self._free = libc.free

        self._ec = lib.kuttidb_embed_open(path.encode())
        if not self._ec:
            raise KuttiEmbedError(f"cannot attach embed region {path}")
        self._c = lib.kuttidb_embed_cache(self._ec)

    def put(self, key, value, ttl=None):
        kb = key.encode() if isinstance(key, str) else key
        ttl_ms = max(1, int(ttl * 1000)) if ttl else 0
        rc = self.lib.kuttidb_embed_put(
            self._ec, kb, len(kb), value, len(value), ttl_ms)
        if rc != 0:
            raise KuttiEmbedError(f"put failed: {rc}")

    def get(self, key):
        kb = key.encode() if isinstance(key, str) else key
        out = ct.c_void_p()
        outlen = ct.c_uint32()
        rc = self.lib.kuttidb_get(self._c, kb, len(kb), ct.byref(out), ct.byref(outlen))
        if rc == 1:
            try:
                return ct.string_at(out, outlen.value)
            finally:
                self._free(out)
        if rc == 0:
            return None
        raise KuttiEmbedError("get error")

    def delete(self, key):
        kb = key.encode() if isinstance(key, str) else key
        return self.lib.kuttidb_embed_delete(self._ec, kb, len(kb)) == 1

    def count(self):
        return self.lib.kuttidb_count(self._c)

    def memusage(self):
        return self.lib.kuttidb_memusage(self._c)

    def close(self):
        if self._ec:
            self.lib.kuttidb_embed_close(self._ec)
            self._ec = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
