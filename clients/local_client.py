"""Trusted-local KuttiDB client with safe shared-memory fallback.

`LocalKuttiDB` first attempts CEMBv3 shared memory when `embed_path` is given.
If the region is unavailable or cannot be attached, it opens the normal TCP/TLS
client instead. It never changes transport after an operation has started.
"""

from __future__ import annotations

import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_SRC = os.path.join(_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from kuttidb_client import KuttiDBClient
from kuttidb_embed import KuttiEmbed, KuttiEmbedError


class LocalKuttiDB:
    """Use trusted local shared memory when safe, otherwise TCP/TLS."""

    def __init__(self, *, embed_path: str | None = None, host="127.0.0.1",
                 port=7379, timeout=5.0, auth_token=None, tls=False,
                 ca_file=None, server_hostname=None, unix_path=None, server=None):
        self._direct = None
        self._network = None
        self.transport = "tcp"
        # Managed lifecycle needs a native socket lease. A shared-memory
        # attachment cannot reliably report process death, so never select it
        # when the caller asked the SDK to own local server lifecycle.
        if embed_path and server is None:
            try:
                self._direct = KuttiEmbed(embed_path)
                self.transport = "shared_memory"
                return
            except KuttiEmbedError:
                # Attachment failure is expected for a missing region, denied
                # file access, or a server configured without embedded mode.
                pass
        self._network = KuttiDBClient(host=host, port=port, timeout=timeout,
                                    auth_token=auth_token, tls=tls,
                                    ca_file=ca_file,
                                    server_hostname=server_hostname,
                                    unix_path=unix_path, server=server)
        self.transport = "unix_socket" if unix_path else "tcp"

    def put(self, key, value, ttl=None):
        if self._direct:
            return self._direct.put(key, value, ttl)
        return self._network.put(key, value, ttl)

    def get(self, key):
        return self._direct.get(key) if self._direct else self._network.get(key)

    def delete(self, key):
        return self._direct.delete(key) if self._direct else self._network.delete(key)

    def put_many(self, items):
        if not self._direct:
            return self._network.put_many(items)
        for item in items:
            key, value, *rest = item
            self._direct.put(key, value, rest[0] if rest else None)

    def get_many(self, keys):
        return self._network.get_many(keys) if not self._direct else [self._direct.get(k) for k in keys]

    def stats(self):
        if not self._direct:
            result = self._network.stats()
            result["transport"] = self.transport
            return result
        return {"transport": "shared_memory", "count": self._direct.count(),
                "mem_bytes": self._direct.memusage()}

    def close(self):
        if self._direct:
            self._direct.close()
            self._direct = None
        if self._network:
            self._network.close()
            self._network = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
