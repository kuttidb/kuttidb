# Kubernetes deployment

KuttiDB deploys on Kubernetes through plain manifests in
[`deploy/kubernetes/`](../../deploy/kubernetes/) — no Helm chart is required for
the supported topologies. Two shapes exist:

| Manifest | Shape | Persistence | Use |
|---|---|---|---|
| [`ephemeral-cache.yaml`](../../deploy/kubernetes/ephemeral-cache.yaml) | Deployment | none (`-` WAL) | cache-only, pods are disposable |
| [`durable-single-node.yaml`](../../deploy/kubernetes/durable-single-node.yaml) | StatefulSet (1 replica) | PVC + `--durability always` | durable queues, streams, atomic operations |

KuttiDB is a **single-node durable store**: a StatefulSet with one replica
and a `ReadWriteOnce` volume survives pod restarts and rescheduling onto a
healthy node, but it is **not** replicated — node or disk destruction loses
acknowledged durable data. There is no "replicated no-node-loss"
configuration; do not increase `replicas` on the durable manifest, because
two writers on separate volumes are independent databases, not a cluster.
See [DURABILITY.md](../design/DURABILITY.md) for the exact guarantees.

## Ephemeral cache mode

The Deployment runs the server with `-` as the WAL path (persistence
disabled) and no volumes. Eviction and TTL behave exactly as documented;
pod churn loses cache state by design. Probes: TCP on the client port
(readiness) and liveness.

## Durable single-node mode

The StatefulSet is the complete reference for durable operation:

- **PVC** via `volumeClaimTemplates` mounted at `/var/lib/kuttidb` — cache,
  queue, and stream WALs plus the snapshot live there.
- **`--durability always`** — every acknowledged cache write, queue
  publish/ack, and stream append is fsynced before its acknowledgement.
- **Secret-based auth and metrics token**: the secret is mounted read-only at
  `/etc/kuttidb-auth`; the entrypoint copies it to a `0600` tmpfs path
  (`/run/kuttidb`) because the server rejects group/other-readable auth
  files.
- **Non-loopback bind** (`0.0.0.0`) requires `--auth-file`; the metrics
  listener binds `0.0.0.0:9099` with `--metrics-token-file`.
- **Authenticated probes**: readiness is HTTP `GET /ready` and liveness is
  `GET /live` on the metrics port, each carrying
  `Authorization: Bearer <token>`. `/ready` returns **503 while any
  persistence engine is degraded** (cache, queue, or stream WAL fail-closed),
  so a pod that cannot durably acknowledge is removed from Service endpoints.
  The image `HEALTHCHECK` is a plain TCP liveness check and does not carry a
  token; see [DOCKER.md](DOCKER.md).
- **SecurityContext**: `runAsNonRoot`, `runAsUser: 10001`, all capabilities
  dropped, no privilege escalation, read-only root filesystem; `fsGroup:
  10001` lets the pod own the mounted volume.
- **Resource requests/limits** (100m/256Mi requests, 2 CPU/1Gi limits) — tune
  to the working set; the in-memory cache budget is the fifth positional
  argument (`MAX_MEM_MB`, evictable cache data only — durable queue and
  stream data have separate accounting).

### Tokens

Create the secret from a 1–1024-byte token file (see DEPLOYMENT.md for
generation):

```sh
kubectl create secret generic kuttidb-auth \
  --from-file=token=./token
```

The manifest reuses the same token for the auth file and the metrics bearer
token. Replace `REPLACE_WITH_METRICS_TOKEN` in the probe headers with the
token value (or template it through an env var and `$(KUTTIDB_TOKEN)`).

### Graceful termination

`docker stop`/pod deletion sends SIGTERM; the server flushes and closes the
cache, queue, and stream WALs before exiting, so acknowledged durable state
is complete. Give the pod a `terminationGracePeriodSeconds` that covers a
final fsync (the default 30s is ample).

## Prometheus

The metrics listener serves exposition-format metrics at `/metrics`
(client port and connection gauges, per-queue depth/in-flight series,
per-topic partition/retention series, single-flight and SWR counters,
durability-failure flags). Scrape port `9099` with the bearer token. The
`kuttidb_ready` gauge mirrors the readiness contract so alerts can watch
persistence degradation directly.

## Persistent-volume recovery

`src/test_container_recovery.py` (CI job `container-recovery`) validates the
durable path end to end: seed cache, queue, and stream state through the
client port, SIGKILL the container, restart on the same volume, and verify
every acknowledged durable item survived (cache 500/500, queue 50/50, stream
20/20 in the recorded runs).
