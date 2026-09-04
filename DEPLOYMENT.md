# Deployment

## Managed local lifecycle

Managed local mode is for one application on one host. It uses a persistent,
owner-only data directory as its identity and lock domain; it does not elect a
server across machines or containers, provide replication, or make failed
operations safe to replay. Containers and service-managed deployments should
continue to use the standalone command line below. Never mount one managed
data directory for active writers on more than one machine.

The simple managed API uses an owner-only Unix socket. Explicit managed TCP is
limited to a literal IPv4 loopback address; it is useful for local tooling but
does not provide cross-container coordination or remote access.

Managed startup appends to `<data_dir>/kuttidb.log`; under its bootstrap lock it
rotates that file at 5 MiB, retaining one owner-only `.1` file. A failed launch
records only a short-lived, non-secret configuration digest and failure code,
which prevents concurrent callers from repeatedly starting the same broken
configuration for one second. It is not a lock and never suppresses a healthy
endpoint.

KuttiDB runs as one executable with its cache WAL, queue WAL, snapshots, and
optional embedded region in one data directory. The checked-in container image
is a small Linux image for x86-64 or ARM64 builders. It uses the dependency-free
plaintext build; terminate TLS at an ingress/load balancer or build a variant
with `make TLS=1` and certificates supplied securely.

## Docker Compose

Create an owner-only token file, then start the durable local service:

```sh
umask 077
head -c 32 /dev/urandom | base64 > ./kuttidb.token
KUTTIDB_AUTH_TOKEN_FILE="$PWD/kuttidb.token" docker compose up --build
```

The service is bound to host loopback on port 7379, persists `/var/lib/kuttidb`
in the named volume, uses `--durability always`, and fsyncs the independent
queue WAL before a successful durable queue response. Remove the volume only
when intentionally discarding both cache and queue state.

## Kubernetes

`deploy/kubernetes/ephemeral-cache.yaml` is an evictable cache-only Deployment
with no persistent volume. Apply it only when losing cache contents on restart
is acceptable.

`deploy/kubernetes/durable-single-node.yaml` is a one-replica StatefulSet with
a persistent volume and cache/queue durability set to `always`. Create its
authentication secret before applying it:

```sh
kubectl create secret generic kuttidb-auth --from-file=token=./kuttidb.token
kubectl apply -f deploy/kubernetes/durable-single-node.yaml
```

Both manifests run as the image's non-root UID, drop Linux capabilities, and use
a read-only root filesystem.

## Metrics and authenticated probes

`--metrics-bind IPv4:PORT` serves a minimal Prometheus endpoint with
`GET /metrics` (exposition format v0.0.4), plus `GET /ready` and `GET /live`
for Kubernetes HTTP probes. The endpoint is disabled unless `--metrics-bind`
is given. A non-loopback bind requires `--metrics-token-file PATH` (mode 0600,
owned by the server user) and then answers `401` without a matching
`Authorization: Bearer <token>` header, compared in constant time. Bound to
loopback it needs no token, which suits a host-local scrape.

`/ready` reports `503` while cache, queue, or stream persistence has entered
its fail-closed state — the same durability contract as the authenticated
native `HEALTH` command. `/live` answers while the admin listener is alive.
Use them as readiness/liveness probes; TCP probes remain the lightweight
listener-only alternative.

The durable manifest demonstrates the full pattern: the entrypoint copies a
private metrics token from the same secret, binds metrics on `0.0.0.0:9099`
(the kubelet scrapes from outside the pod), and probes `/ready` and `/live`
with an `Authorization` header whose value must be replaced with the token.
A Prometheus server can scrape the same `metrics` port with the same header.

```sh
curl -H "Authorization: Bearer $(cat ./kuttidb.token)" \
    http://127.0.0.1:9099/metrics
```

Metric families include `kuttidb_up`, `kuttidb_ready`, cache entries/memory/
WAL offset/expired/evicted, connection and authentication counters, queue
depth/inflight/redeliveries/dead-lettered, exchange counts, stream topics/
partitions/retention bytes, consumer-group membership, single-flight claims
and waiters, and negative-cache entries. Values are point-in-time samples;
families are read independently, so one scrape may straddle a mutation.

Per-object series carry labels: `kuttidb_queue_depth{name=...}` and
`kuttidb_queue_inflight{name=...}` for every queue, and
`kuttidb_topic_partitions{topic=...}`, `kuttidb_topic_retained_bytes`, and
`kuttidb_topic_records` for every stream topic. A scrape emits at most 256
labeled objects per engine; beyond that a `kuttidb_queue_metrics_truncated`
or `kuttidb_stream_metrics_truncated` gauge is set, so output size stays
bounded and parseable. Label values are restricted to printable ASCII
(escaping quotes, backslashes, and newlines; other bytes become `_`).

The durable StatefulSet protects acknowledged durable queue data across a
process crash and clean restart of its healthy volume. It **does not** provide
replication or protection from volume, machine, or node loss; do not describe
this deployment as no-data-loss or highly available.

`src/test_container_recovery.py` exercises exactly this contract in Docker:
it seeds durable cache records, queue messages, and stream records into a
container volume, SIGKILLs the container, restarts it on the same volume, and
verifies every acknowledged durable item is present. CI runs it on every
push (`container-recovery` job).

## Backup and restore

One data directory holds everything: the cache WAL, its snapshot, the queue
WAL, and the stream WAL. With the default inference the files are
`<WAL>`, `<WAL>.snap`, `<WAL>.queues`, and `<WAL>.streams` (explicit
`--queue-wal`/`--stream-wal` paths override the suffixes).

**Cold backup (recommended).** Stop the server cleanly — a graceful shutdown
flushes and closes every persistence engine — then copy the whole directory:

```sh
kill -TERM $(pidof kuttidb)   # or kubectl delete pod (graceful)
tar -czf kuttidb-backup.tgz /var/lib/kuttidb
```

**Restore.** Place the copied files back into the data directory and start the
server with the same flags. Startup replays each CRC-checked WAL, truncating
any torn or corrupt tail to the last valid record (idempotently — restoring
the same backup twice is safe). Verify with `kuttidb-cli health`.

**Live copies are not supported.** Copying an open WAL while the server runs
produces a torn copy that recovery may have to truncate. Use a filesystem
snapshot (or freeze the filesystem) only if you can guarantee the copy is a
single point-in-time image of every file in the directory; otherwise always
back up cold. Snapshots (`<WAL>.snap`) are checkpoints, not backups — they
fold acknowledged cache state and are not a substitute for the backup above.

**What a backup restores.** Acknowledged durable queue messages, durable
stream records, group offsets, and durable cache records as of the last
clean stop. Evictable cache entries are restored only if they were persisted
before the stop; by policy, evicted entries are gone. Restoring a backup on a
different host is supported — all persistence is file-based with no
host-specific state (the embedded shared-memory region is the one exception;
it is process-local and never part of a backup).

## Upgrade and compatibility policy

- The wire protocol is `major.minor`. A different **major** is refused during
  capability negotiation; a higher **minor** is backward-compatible, and
  clients check the capability bitset before using newer features.
- Durability formats are versioned and CRC-checked: cache WAL/snapshot
  records, queue WAL record shapes, and the offset-based `CEMBv3` embedded
  region each identify themselves, and unknown record types fail closed
  during replay rather than being silently skipped.
- Upgrading is: stop the old binary cleanly, replace the executable, start
  the new one on the same data directory. Recovery is tested at every
  milestone, including SIGKILL-crash restarts, so an upgrade interrupted by a
  crash resumes through the normal recovery path.
- Rolling back to an older binary can drop protocol features (newer opcodes
  answer an error) but must not corrupt durable state; WAL records written by
  a newer minor version are the one rollback hazard — do not write with a
  newer binary and then run an older one against the same data directory
  unless its release notes state the formats are unchanged.
## Management API

The Management API is disabled by default. For a local operational dashboard,
start it with `--admin-bind 127.0.0.1:7380 --admin-token-file admin.token
--admin-audit-log admin-audit.jsonl`.
For any remote bind, add `--admin-tls-cert` and `--admin-tls-key`; plaintext
remote administration is refused. Keep this token separate from data-listener
and metrics tokens. Details: [MANAGEMENT_API.md](MANAGEMENT_API.md).
