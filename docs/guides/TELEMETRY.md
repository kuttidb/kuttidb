# Telemetry

KuttiDB telemetry is optional and disabled by default. Normal binaries are built
without telemetry support. A telemetry-capable server sends one delayed,
best-effort HTTPS report after explicit opt-in; reporting does not run in SDKs,
install scripts, the web console, or the public website.

Its sole purpose is to publish a privacy-preserving, community-level view of
KuttiDB adoption. It is not a diagnostic, support, monitoring, account, or
individual-user telemetry system, and it must never be used to investigate a
specific installation.

## Enable or disable it

Build a telemetry-capable binary, then provide a private state directory:

```sh
make TELEMETRY=1
./kuttidb --telemetry on --telemetry-state-dir /var/lib/kuttidb/.telemetry
```

`--telemetry off` disables reporting. `DO_NOT_TRACK=1` always disables it,
including when the CLI or `KUTTIDB_TELEMETRY=on` requests it. Restart the server
after changing a setting. `--telemetry-endpoint HTTPS_URL` changes the sole
destination; it does not enable telemetry by itself. The URL must be HTTPS with
a path and cannot contain credentials, a query string, or a fragment.

Managed local mode accepts `telemetry`, `telemetry_endpoint`, and
`telemetry_state_dir` in `ServerParams`. Its default state path is
`<data-dir>/.telemetry`. The state directory is independent from database WALs
and `instance.id`; never copy it into an image or template.

## What is reported

The v1 body has exactly three fields: `schema_version: 1`, a random endpoint-
specific installation identifier, and one bucket of open native connections:
`0`, `1`, `2-5`, `6-20`, `21-100`, `101-1000`, or `1001+`.

The identifier comes from private local random state; it is not a hostname,
hardware ID, account, path, token, or managed instance identity. Open native
connections include pools, handshakes, and probes. They exclude Management API,
metrics HTTP, and embedded clients. This is not a count of people, customers,
SDK users, daily connections, or a peak.

The first attempt is delayed for 15 minutes after startup. Later attempts occur
no more than once per 24 hours. Failed attempts are dropped and wait for the next
slot. TLS certificate and hostname verification are enabled; redirects and
responses are ignored. The reporter has bounded connection and total timeouts,
does not add work to database requests, and a failure never makes KuttiDB
unhealthy.

## Public statistics and retention

The official collector accepts only the strict v1 schema. It stores at most one
row per installation and UTC day, keeps ID-bearing records for 35 days, and
publishes only rounded 7- and 30-day reporting-installation aggregates. Totals
below 20 are withheld. Connection distributions are withheld unless every
nonempty bucket reaches the same threshold. These measures are directional:
opt-in, offline, short-lived, custom-endpoint, and telemetry-free installations
are not included. Public statistics update from aggregate snapshots; they do not
mean users are online now.

The network operator still handles connection metadata such as source IP while a
report is in transit. The collector does not retain raw report bodies or IP
addresses in its application database. Disabling or resetting reporter state
stops future reports; existing official records expire on the stated schedule.
