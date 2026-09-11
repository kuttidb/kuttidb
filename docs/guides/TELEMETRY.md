# Telemetry

KuttiDB telemetry is optional. Normal binaries are built without telemetry
support — reporting is impossible in them. The official telemetry-capable
build, which the installer ships when a user accepts the community opt-in,
defaults to reporting on; self-built capable binaries default to off until you
switch them on. Native reporting never runs in SDKs, the web console, or the
public website; the official installer sends a separate count-only invocation
signal described below.

Its sole purpose is to publish a privacy-preserving, community-level view of
KuttiDB adoption. It is not a diagnostic, support, monitoring, account, or
individual-user telemetry system, and it must never be used to investigate a
specific installation.

## Enable or disable it

Telemetry mode resolves in this order: the `--telemetry on|off` flag, then the
`KUTTIDB_TELEMETRY` environment variable, then the build default — and
`DO_NOT_TRACK=1` always forces it off.

```sh
make TELEMETRY=1                      # capable build, reporting off by default
./kuttidb --telemetry on --telemetry-state-dir /var/lib/kuttidb/.telemetry
```

`--telemetry off` disables reporting. Restart the server after changing a
setting. A build compiled with the opt-in default (official installer builds,
or `make TELEMETRY=1 TELEMETRY_DEFAULT=1`) reports without any flags and picks
its own private state directory under `${XDG_STATE_HOME:-$HOME/.local/state}`
unless one is provided; a startup line announces it. An explicitly enabled
telemetry that cannot resolve a state directory refuses to start (exit 2), a
build-default enabled telemetry degrades to disabled with a warning instead.
`--telemetry-endpoint HTTPS_URL` changes the sole destination; it does not
enable telemetry by itself. The URL must be HTTPS with a path and cannot
contain credentials, a query string, or a fragment.

Managed local mode accepts `telemetry`, `telemetry_endpoint`, and
`telemetry_state_dir` in `ServerParams`. Its default state path is
`<data-dir>/.telemetry`. The state directory is independent from database WALs
and `instance.id`; never copy it into an image or template.

## Official binaries: with or without telemetry

Every release publishes two tarball variants per platform
([RELEASE.md](../operations/RELEASE.md)):

| Tarball | Reporter |
|---|---|
| `kuttidb-<version>-<os>-<arch>.tar.gz` | not compiled in — reporting is impossible, even by misconfiguration |
| `kuttidb-<version>-telemetry-<os>-<arch>.tar.gz` | compiled in, reporting **on by default** |

The official installer (`curl -fsSL https://kuttidb.com/install.sh | bash`)
asks once whether you want to contribute to the community adoption
statistics — that answer is the decision point:

- **No (the default)** — installs the telemetry-not-included build. Nothing
  can ever report from it.
- **Yes** — installs the telemetry-capable build whose servers report by
  default (one delayed, best-effort HTTPS report per day, first one at least
  15 minutes after startup). The installer also writes
  `~/.config/kuttidb/telemetry.env` with `KUTTIDB_TELEMETRY=on` plus a private
  state directory, which covers servers started from a shell that loads it and
  telemetry-capable binaries from older releases. Declining later removes the
  env file again.

Non-interactive installs (CI, no terminal) default to the telemetry-free
build; pass `--telemetry yes|no` or set `KUTTIDB_TELEMETRY_OPTIN=yes|no` to
choose explicitly. `DO_NOT_TRACK=1` always wins — over the build default, the
env file, the CLI, and this installer. `--telemetry off` likewise disables it.
The installer verifies after installing that `kuttidb --features` matches the
choice it made (`telemetry=v1` + `telemetry-default=on` for the opt-in build).

## Installer invocation count

Every run of the official installer sends one count-only HTTPS signal before
anything else — before the telemetry question, the platform check, and any
download. The request is `POST https://telemetry.kuttidb.com/v1/install` with
the body `{"schema_version":1}` and nothing else: no installation identifier,
no device details, no operating system or version, no answer to the telemetry
question, no timestamps, and no local state. The route itself identifies the
event, so every accepted request adds exactly one — Yes and No count alike,
re-runs count again, and a run that later fails still counts once the signal
was received. Help output, unknown options, and invalid telemetry choices send
nothing.

`DO_NOT_TRACK=1` disables the counter: no DNS, no request, no state. The
signal is best-effort: one bounded request (at most ~2 seconds), no retries,
no redirect following, and an unreachable or rejecting collector is dropped
silently while the install continues normally. If no suitable HTTP client is
available the signal is skipped rather than sent through a weaker transport.

The collector stores only aggregates: daily count rows for 396 days plus a
lifetime total and collection start date. Public statistics publish installer
runs as exact counts — they carry no identifiers or device dimensions, so the
native below-20 suppression does not apply. An unauthenticated, identity-free
endpoint cannot prove uniqueness or prevent fabricated counts: installer runs
are a directional invocation metric, not a count of installations, people, or
completed setups, and are never merged with the opt-in reporting totals.

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
