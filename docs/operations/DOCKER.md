# Docker deployment

KuttiDB ships a minimal, non-root Linux image built from a multi-stage
`Dockerfile`: a `build-base` Alpine stage compiles `make TLS=0 kuttidb`,
and the runtime stage is `alpine:3.21` with a dedicated `kuttidb` user
(uid/gid 10001), the binary, and an entrypoint that stages secrets.

- **TLS is disabled in the baseline image** so it carries no OpenSSL runtime.
  Terminate TLS at an ingress, or build a site-specific image with
  `make TLS=1` (then pass `--tls-cert`/`--tls-key`).
- The runtime user is non-root. Auth and metrics tokens arrive through
  mounted secrets, which are typically root-owned; the entrypoint copies
  them to a `0600` private path under the data directory before exec, because
  the server intentionally rejects group/other-readable auth files.

## Image layout

| Path | Purpose |
|---|---|
| `/usr/local/bin/kuttidb` | the single KuttiDB binary |
| `/usr/local/bin/docker-entrypoint` | secret staging, then `exec` the server |
| `/var/lib/kuttidb` | data directory (cache WAL, queue WAL, stream WAL, snapshot), volume-mount this |

## Running

```sh
docker run --rm -p 127.0.0.1:7379:7379 kuttidb:local 7379 /var/lib/kuttidb/kuttidb.wal 100
```

The entrypoint passes arguments straight to the server; see
[PROTOCOL.md](../design/PROTOCOL.md) for the full flag list. A durable container
typically uses:

```sh
docker run -d --name kuttidb \
  -v kuttidb-data:/var/lib/kuttidb \
  -p 127.0.0.1:7379:7379 \
  kuttidb:local 7379 /var/lib/kuttidb/kuttidb.wal 100 - 64 \
    --durability always --queue-wal /var/lib/kuttidb/queue.wal
```

`--durability always` makes every acknowledged write fsync before the
acknowledgement; without a mounted volume the data does not survive the
container. `docker stop` triggers a graceful SIGTERM shutdown, which flushes
and closes all WALs cleanly.

## Healthcheck

The image defines a `HEALTHCHECK` (`nc -z 127.0.0.1 7379`): it passes while
the process accepts client connections. That is a **liveness** signal only.
The durability-aware readiness check is the authenticated HTTP `GET /ready`
on the optional metrics listener (it returns 503 while cache, queue, or
stream persistence is fail-closed); a container-level healthcheck cannot
carry the bearer token, so use it from Kubernetes probes or an authenticated
scraper instead. See [KUBERNETES.md](KUBERNETES.md).

## Multi-architecture

CI builds `linux/amd64` and `linux/arm64` images via QEMU
(`.github/workflows/ci.yml`, job `multiarch`) and smoke-tests the arm64
variant. Publishing to a registry is release work, not part of CI.

## Compose example

[`compose.yaml`](../../compose.yaml) runs one durable instance with a named
volume, an auth token from a file (`KUTTIDB_AUTH_TOKEN_FILE` must point at a
1–1024-byte token file), loopback-only client and metrics ports, and
`--durability always`:

```sh
KUTTIDB_AUTH_TOKEN_FILE=./token docker compose up --build
```

The metrics scrape is bound to `127.0.0.1:9099` inside the container and
published loopback-only; loopback metric binds require no bearer token, which
keeps the example simple. For anything non-loopback, mount a token file and
pass `--metrics-token-file` as the Kubernetes manifest does.

The Compose file deliberately runs **one instance**: KuttiDB is a single-node
durable store. There is no replication yet, so a volume does not protect
against node loss — see [DURABILITY.md](../design/DURABILITY.md).

## Verifying recovery

`src/test_container_recovery.py` (CI job `container-recovery`) seeds durable
cache, queue, and stream state into a Docker volume, SIGKILLs the container,
restarts on the same volume, and verifies every acknowledged durable item
survived. Run it locally with:

```sh
docker build -t kuttidb:ci .
KUTTIDB_TEST_IMAGE=kuttidb:ci python3 src/test_container_recovery.py
```
