"""KuttiDB Python client.

One import surface for the three transports:

- ``KuttiDBClient`` — binary wire protocol over TCP, TLS, or a Unix socket,
  including queues, exchanges, atomic operations, single-flight, streams,
  and the managed local server lifecycle.
- ``KuttiEmbed`` — zero-syscall shared-memory client; requires the native
  ``libkuttidb_embed`` library (built by ``make``) on the library path.
- ``LocalKuttiDB`` — picks shared memory when safe, otherwise TCP/TLS.

    import kuttidb

    with kuttidb.KuttiDBClient(port=7379) as db:
        db.put("greeting", b"hello", ttl=60)
        print(db.get("greeting"))

Canonical sources live in the repository (``src/kuttidb_client.py``,
``clients/kuttidb_embed.py``, ``clients/local_client.py``) and are staged
into this package by ``clients/python/prepare.py`` at build time.
"""

from .client import (
    CAP_ATOMIC,
    CAP_ATOMIC_UPDATE,
    CAP_CACHE,
    CAP_EXCHANGES,
    CAP_HEALTH,
    CAP_QUEUE_CONSUMERS,
    CAP_QUEUES,
    CAP_SERVER_INFO,
    CAP_SINGLEFLIGHT,
    CAP_STREAM_BATCH,
    CAP_STREAM_GEN,
    CAP_STREAM_KEYS,
    CAP_STREAMS,
    CAP_SWR,
    KuttiDBClient,
    KuttiDBError,
    ManagedServerConfigurationError,
    ManagedServerEndpointOccupied,
    ManagedServerInstanceMismatch,
    ManagedServerStartupError,
    ManagedServerStartupTimeout,
    ServerParams,
    StreamAssignment,
)
from .embed import KuttiEmbed, KuttiEmbedError
from .local import LocalKuttiDB

__version__ = "0.0.6b0"

__all__ = [
    "CAP_ATOMIC",
    "CAP_ATOMIC_UPDATE",
    "CAP_CACHE",
    "CAP_EXCHANGES",
    "CAP_HEALTH",
    "CAP_QUEUE_CONSUMERS",
    "CAP_QUEUES",
    "CAP_SERVER_INFO",
    "CAP_SINGLEFLIGHT",
    "CAP_STREAM_BATCH",
    "CAP_STREAM_GEN",
    "CAP_STREAM_KEYS",
    "CAP_STREAMS",
    "CAP_SWR",
    "KuttiDBClient",
    "KuttiDBError",
    "KuttiEmbed",
    "KuttiEmbedError",
    "LocalKuttiDB",
    "ManagedServerConfigurationError",
    "ManagedServerEndpointOccupied",
    "ManagedServerInstanceMismatch",
    "ManagedServerStartupError",
    "ManagedServerStartupTimeout",
    "ServerParams",
    "StreamAssignment",
    "__version__",
]
