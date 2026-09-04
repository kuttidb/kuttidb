# kuttidb (Python client)

Python client for [KuttiDB](https://github.com/kuttidb/kuttidb) — stdlib only,
no dependencies.

```sh
pip install kuttidb
```

```python
import kuttidb

# TCP / TLS / Unix socket — full wire protocol
with kuttidb.KuttiDBClient(port=7379) as db:
    db.put("greeting", b"hello", ttl=60)
    print(db.get("greeting"))

# Managed local lifecycle: starts and stops a private server for you
with kuttidb.KuttiDBClient.managed(data_dir="./data") as db:
    db.put("k", b"v")

# Shared-memory embed (needs libkuttidb_embed built by `make`)
# and LocalKuttiDB (shared memory when safe, TCP otherwise)
```

| Export | Purpose |
|---|---|
| `KuttiDBClient` | Binary wire protocol: TCP, TLS, Unix socket, managed lifecycle |
| `KuttiEmbed` | Zero-syscall shared-memory client (native library required) |
| `LocalKuttiDB` | Chooses shared memory when safe, TCP/TLS otherwise |
| `KuttiDBError` | Base error type (plus the `ManagedServer*` subclasses) |
| `CAP_*` | Server capability bits reported by the handshake |

Docs: [architecture](https://github.com/kuttidb/kuttidb/blob/main/docs/design/ARCHITECTURE.md) ·
[wire protocol](https://github.com/kuttidb/kuttidb/blob/main/docs/design/PROTOCOL.md) ·
[getting started](https://github.com/kuttidb/kuttidb/blob/main/docs/guides/GETTING_STARTED.md)

Apache-2.0. This package is built from the repository sources by
`clients/python/prepare.py`; see
[CLIENT_PUBLISHING.md](https://github.com/kuttidb/kuttidb/blob/main/docs/operations/CLIENT_PUBLISHING.md).
