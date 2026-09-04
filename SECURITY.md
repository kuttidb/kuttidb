# KuttiDB security model

KuttiDB is a cache engine, not an Internet-facing database gateway. The
security patch makes accidental exposure fail closed while keeping the normal
authenticated data path small and predictable.

## Secure defaults

- TCP binds to `127.0.0.1`, not every interface.
- Binding to a non-loopback address requires a pre-shared authentication file.
- Optional native TLS encrypts and authenticates every TCP connection before
  the binary KuttiDB protocol or AUTH token is processed. TLS 1.2 is the minimum
  and TLS 1.3 is preferred.
- Authentication is performed once per connection with a constant-time token
  comparison. Failed authentication closes the connection.
- Values, batches, buffered input/output, and concurrent connections have hard
  limits to contain memory and file-descriptor denial of service.
- Invalid opcodes, inconsistent lengths, nonzero batch padding, and malformed
  state transitions fail closed instead of allowing protocol desynchronization.
- WAL, snapshots, embedded regions, and Unix sockets are mode `0600`.
- Persistence and shared-memory files are opened without following symlinks.
  KuttiDB refuses to replace a non-socket Unix path or an invalid embedded
  region.
- Persistence loaders apply the configured value-size limit before allocating.

## Recommended deployment

Create the token without exposing it in a process argument:

```sh
umask 077
openssl rand -hex 32 > /run/kuttidb.token
./kuttidb 7379 /var/lib/kuttidb/kuttidb.wal 100 \
  --bind 10.0.0.12 --auth-file /run/kuttidb.token \
  --tls-cert /etc/kuttidb/server-chain.pem \
  --tls-key /etc/kuttidb/server-key.pem \
  --max-value-mb 16 --max-batch-mb 64 --max-clients 2048
```

Clients authenticate with the contents of the same file and validate the TLS
certificate against a trusted CA. For example:

```sh
kuttidb-cli --host cache.internal --tls --ca-file /etc/kuttidb/ca.pem \
  --auth-file /run/kuttidb.token get my-key
```

`kuttidb-cli` also accepts `KUTTIDB_TLS=1`, `KUTTIDB_CA_FILE`, and
`KUTTIDB_AUTH_FILE`. The Python client accepts `tls=True`, `ca_file=...`, and
an optional `server_hostname`. Go exposes `NewTLS`; Java accepts an
`SSLContext` in its four-argument constructor.

AUTH alone does not encrypt keys, values, or the token. For traffic that can
cross an untrusted network, enable native TLS. A mutually authenticated TLS
proxy or private VPN remains a valid deployment option. Do not expose a
plaintext KuttiDB TCP port directly to the Internet.

The server certificate and private key are loaded at startup. Rotate them with
a controlled restart. Use certificates issued by your internal/public CA in
production; self-signed certificates are appropriate only when their exact CA
certificate is explicitly distributed to clients.

## Trust boundaries

An authenticated network client can read, overwrite, and delete every key;
there are no per-key ACLs or multi-tenant namespaces. Use a separate KuttiDB
process and token for each trust domain.

Embedded mode deliberately gives local processes direct read/write access to
the database memory and its process-shared locks. File permissions are the
access-control boundary. Only mutually trusted processes should receive access
to the embedded-region file. Locks recover when an owner process dies, but a
hostile process with direct mapping access can still modify arbitrary region
bytes; use socket mode across trust boundaries.

WAL CRCs detect corruption and torn writes; they are not cryptographic
signatures. Protect the persistence directory with operating-system ownership
and permissions, and use encrypted storage if data-at-rest confidentiality is
required.

## Operational checks

Monitor `connections`, `rejected_connections`, `auth_failures`, `wal_failed`,
`mem_bytes`, and `allocated_bytes` in `STATS`.
Unexpected increases should be treated as scanning, a leaked token, or an
undersized connection limit. Rotate a token by restarting the server with a new
token file and updating clients; existing connections end during restart.
## Management API

The optional Management API requires a separate server-owned regular token
file with mode `0600` and a server-owned audit log with mode `0600`, including
on loopback. It never accepts credentials in
URLs. Plain HTTP is restricted to loopback; remote administration requires
native TLS. Exact CORS origins may be configured, but wildcard origins are not
accepted and CORS never bypasses bearer authentication. See
[MANAGEMENT_API.md](MANAGEMENT_API.md).
