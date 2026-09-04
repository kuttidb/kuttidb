# KuttiDB managed local server implementation plan

Status: implementation-ready design

Primary outcome: let a small or medium application use KuttiDB as one local
cache, queue, and stream service without separately starting or supervising it,
while preserving the existing explicitly managed server behavior.

## 1. Product contract

KuttiDB will support two deliberately separate lifecycle modes.

### 1.1 Standalone mode

Standalone mode is the current behavior:

- an operator, service manager, container, or deployment system starts KuttiDB;
- clients only connect;
- constructing or closing a client never starts or stops the server;
- the server stays alive until it receives an explicit shutdown signal;
- existing client constructors and server command lines remain compatible.

This remains the appropriate mode for remote clients, multiple machines,
multiple containers sharing one instance, and infrastructure-managed
production deployments.

### 1.2 Managed local mode

Managed local mode is opt-in:

- the application supplies a local data directory and optional typed server
  settings;
- the SDK first attempts a normal connection;
- if the local endpoint is definitely absent, the SDK invokes the KuttiDB
  launcher to ensure that exactly one server is running;
- concurrent applications and workers converge on the same server;
- the SDK connects only after the server reports readiness;
- the server shuts itself down gracefully after every native client connection
  is gone for the configured idle grace period;
- a later client can start the same instance again and recover it from the same
  data directory.

Managed mode is an application-owned local service, not an in-process database.
It must not be described as providing SQLite or DuckDB process semantics.

### 1.3 Recommended public experience

Make the simplest safe API a factory centered on a data directory:

```python
db = KuttiDBClient.managed(
    data_dir="./data/kuttidb",
    idle_timeout=60,
)
```

The factory resolves `data_dir` to an absolute path and derives the default
Unix socket, cache WAL, queue WAL, stream WAL, log, lock, and instance-identity
paths from it. Managed local mode should prefer an owner-only Unix socket so it
does not consume or race over a TCP port.

Also support advanced, typed parameters without accepting a shell command:

```python
db = KuttiDBClient(
    host="127.0.0.1",
    port=7379,
    server=ServerParams(
        data_dir="./data/kuttidb",
        executable="/opt/kuttidb/bin/kuttidb",
        transport="tcp",
        durability="always",
        max_memory_mb=512,
        idle_timeout=60,
        startup_timeout=10,
        auth_file="./secrets/kuttidb.token",
    ),
)
```

Supplying `server` opts into start-if-missing behavior. Omitting it retains the
current connect-only behavior. Connection settings are authoritative for the
endpoint; the implementation must reject contradictory server bind, port,
Unix-socket, TLS, or authentication settings before launching anything.

## 2. Non-goals

The first release must not:

- auto-start remote servers;
- turn every existing client constructor into managed mode;
- stop a standalone server when a client closes;
- make the first client process the permanent parent of the server;
- use PID-file existence as mutual exclusion;
- replay an operation after a connection failure;
- restart a server in the middle of an existing client's operation;
- elect one KuttiDB server across machines or containers;
- implement Windows process, named-pipe, or file-lock support before the server
  itself has a Windows backend;
- add reference-count files that become stale when a client crashes;
- compare every startup flag against an already-running standalone server;
- accept tokens, passwords, certificate contents, or other secrets in process
  arguments.

## 3. Repository baseline and constraints

The implementation must account for these current facts:

1. `KuttiDBClient.__init__` immediately opens its socket, and `LocalKuttiDB`
   falls back from shared memory to the network client.
2. The Go client eagerly fills its pool, while the Node.js client creates pool
   connections lazily. Java and Rust own normal long-lived connections.
3. The server always creates a TCP listener and optionally creates a Unix
   listener. Managed Unix-only mode therefore needs an explicit server option.
4. The server creates one TCP listener per event loop with `SO_REUSEPORT`.
   Port binding failure alone cannot elect one process: two KuttiDB processes
   can join the same reuse-port group.
5. Cache, queue, and stream persistence files currently have no server-lifetime
   ownership lock. Their internal mutexes and short WAL `flock` operations do
   not make two independent server processes safe.
6. The server main thread waits in `pause()`, so a non-signal idle-shutdown
   request needs a reliable way to wake the main lifecycle loop.
7. The native protocol is version 1.7 and authenticates before other commands
   when authentication is enabled.
8. macOS and Linux are supported; the current server does not support Windows.
9. Existing deployment manifests, launchd configurations, tests, Make builds,
   and CMake builds must continue to start standalone servers unchanged.

The managed implementation should live in bounded modules such as
`src/managed_lifecycle.c/.h` and `src/managed_launcher.c/.h`. Keep only command
dispatch and narrow lifecycle hooks in `src/server.c`; do not add another large
subsystem directly to that file.

## 4. Instance directory contract

For the managed factory, one absolute data directory defines one logical
KuttiDB instance. Use the existing persistence naming where possible:

```text
<data_dir>/
  kuttidb.wal
  kuttidb.wal.snap
  kuttidb.wal.queues
  kuttidb.wal.streams
  kuttidb.sock
  kuttidb.log
  instance.id
  .bootstrap.lock
  .server.lock
```

Requirements:

- Create the directory with mode `0700`; accept an existing directory only if
  it is a real directory and safe for the current user.
- Resolve a relative path in the client process before invoking the launcher.
- Refuse symlinks for identity, lock, log, socket replacement, and newly
  created persistence-control files.
- Create files with the existing owner-only `umask(0077)` policy.
- Use `O_NOFOLLOW` where available and verify regular-file type and owner after
  open.
- Never place correctness-critical locks solely in a cleanup-prone temporary
  directory when a persistent data directory is available.
- Do not delete lock files on normal exit. Correctness comes from the live
  advisory lock on the open descriptor, not from file existence.

`instance.id` is a random 128-bit identifier encoded as lowercase hexadecimal.
Create it once with `O_CREAT|O_EXCL`, fsync the file, and fsync its parent
directory. Reuse it on subsequent starts. Moving or restoring a complete data
directory preserves the instance ID; copying only WAL files into a new managed
directory creates a distinct instance unless the operator deliberately copies
the ID too.

## 5. Concurrency and ownership model

### 5.1 Why two lock classes are required

Use OS advisory locks held on open file descriptors:

- **Bootstrap lock:** held briefly by `kuttidb ensure` while deciding whether
  and how to launch.
- **Server ownership locks:** held by the actual server process for its entire
  lifetime, acquired before opening or recovering any persistence engine and
  before binding listeners.

The bootstrap lock prevents a process storm. Lifetime locks enforce the actual
safety invariant even if launchers crash, applications bypass the launcher, or
two starts occur through different entry points.

### 5.2 Locks the server must hold

Every new KuttiDB server, including standalone servers, must obtain:

1. An endpoint ownership lock:
   - managed Unix socket: a companion lock derived from the canonical socket
     path;
   - TCP: an owner-specific runtime lock derived without ambiguity from the
     canonical IPv4 bind and port, for example
     `tcp-127.0.0.1-7379.server.lock` under the platform runtime directory.
2. A deterministic companion ownership lock for every enabled cache, queue,
   and stream persistence path. All new server entry points use these same
   per-path identities, including managed and legacy-style invocations, so the
   same WAL cannot be opened through two different lifecycle modes.
3. For a managed data directory, the additional `.server.lock` umbrella lock.
   This prevents two differently configured managed instances from treating
   separate files inside one directory as independent stores; it does not
   replace the per-persistence-path locks.
4. An embedded-region ownership lock when the server creates or attaches a
   writable shared-memory region.

Canonicalize and sort all lock identities before acquisition to avoid lock
order cycles. Acquire every required lock non-blocking. If any acquisition
fails, release all locks already acquired and exit with a distinct
`instance already owned` status before WAL recovery, truncation, compaction, or
listener creation.

Do not hold a lifetime exclusive lock on the cache WAL descriptor itself;
embedded clients currently take short `flock` locks on that WAL to serialize
writes. Use separate companion lock files so embedded mode keeps working.

Create TCP endpoint locks under a verified owner-only runtime directory such
as `$XDG_RUNTIME_DIR/kuttidb` or a securely created per-UID platform temporary
directory. Refuse an unsafe runtime directory rather than silently proceeding
without endpoint ownership.

### 5.3 Lock metadata

After obtaining a lock, write diagnostic metadata while continuing to rely on
the advisory lock for truth:

```text
format=1
pid=12345
instance_id=...
endpoint=unix:/absolute/path/kuttidb.sock
started_unix_ms=...
```

Metadata is informational and may be stale after a crash. Never kill a process
solely because the PID recorded in the file appears alive; PID reuse makes that
unsafe.

### 5.4 External-start race

The server-side lifetime lock is mandatory because an externally launched
server may start during a client's check-and-launch window. All entry points in
the new binary must participate in the same locks.

An older KuttiDB binary does not know these locks and may still join a
`SO_REUSEPORT` group. Document this rolling-upgrade limitation. Storage locks
prevent corruption only after every writer has upgraded. Do not claim safe
mixed-version concurrent startup.

## 6. Central `kuttidb ensure` launcher

Implement lifecycle election once in the executable rather than separately in
every SDK:

```text
kuttidb ensure \
  --data-dir /absolute/path \
  --listen unix:/absolute/path/kuttidb.sock \
  --lifecycle idle \
  --idle-timeout-ms 60000 \
  --startup-timeout-ms 10000 \
  --json
```

Keep the current positional server invocation valid. Detect the `ensure`
subcommand before legacy argument parsing. Prefer canonical named flags for the
new managed path, translating them to the existing internal server config.

### 6.1 Ensure algorithm

1. Parse and validate all paths, endpoint settings, limits, and timeouts.
2. Reject any non-local endpoint. Initially accept only an absolute Unix socket
   path or literal IPv4 loopback address.
3. Securely create or validate the data directory and instance ID.
4. Perform a bounded endpoint probe. A successful transport connection means
   something owns the endpoint; return `existing` and let the SDK perform TLS,
   AUTH, protocol, health, and identity checks.
5. If the endpoint is definitely absent, attempt the bootstrap lock.
6. While another launcher holds the lock, poll the endpoint and ownership lock
   with monotonic deadlines and short jittered backoff. Never wait forever in a
   blocking `flock` call.
7. After acquiring the bootstrap lock, probe the endpoint and ownership state
   again. This second check closes the normal client race.
8. If a server owns the endpoint but is not ready, wait for readiness rather
   than starting another process.
9. Otherwise start one detached server using an argument vector and no shell.
10. Wait for its explicit readiness channel, early failure, or the startup
    deadline.
    If the child loses an ownership race, wait for and probe the winning server
    instead of immediately reporting a false startup failure.
11. Return machine-readable status, instance identity, endpoint, server PID,
    and log path. Do not return secrets or complete command arguments.
12. Release the bootstrap lock on every exit path.

Example successful output:

```json
{
  "status": "started",
  "instance_id": "2d9f...",
  "endpoint": "unix:/data/app/kuttidb.sock",
  "pid": 12345,
  "log": "/data/app/kuttidb.log"
}
```

`status` is `started`, `existing`, or `starting`. Define stable launcher exit
codes for invalid configuration, unsafe path, ownership conflict, startup
failure, and timeout. SDKs should not parse human-readable stderr.

### 6.2 Process detachment

On macOS and Linux:

- fork without invoking a shell;
- create a new session so application terminal signals do not kill the server;
- redirect stdin from `/dev/null`;
- append stdout/stderr to the owner-only managed log;
- close inherited descriptors except the readiness channel and required lock
  descriptors;
- exec the same resolved KuttiDB binary in normal server mode with internal
  managed lifecycle arguments;
- ensure that the final long-lived server is not left as an unreaped child of
  an SDK process.

The starter client dying must not kill the server. Conversely, the managed
server must not inherit application sockets, files, or pipe writers that keep
unrelated resources alive.

Rotate `kuttidb.log` at managed startup when it exceeds a conservative bounded
size, retaining one previous owner-only file. Do the rotation only while the
bootstrap lock is held.

### 6.3 Explicit readiness

Add an internal `--ready-fd` mechanism used only by the launcher. The server
writes a small versioned success record only after:

- configuration and credentials are validated;
- every ownership lock is held;
- cache, queue, and stream recovery succeeds;
- optional admin and metrics listeners start;
- native listeners are bound and registered;
- worker and maintenance threads have started successfully.

On startup failure, send a bounded failure category where possible, close the
channel, log the detailed message, and exit non-zero. The launcher must not use
an arbitrary sleep as proof of readiness.

## 7. Server identity protocol

Managed clients must never silently connect to the wrong KuttiDB instance just
because a valid server occupies the expected endpoint.

Add an authenticated native `SERVER_INFO` operation and increment the protocol
minor version. Reserve a new network opcode that does not overlap WAL record
numbers or existing protocol operations, tentatively `0x0C`, and add capability
bit 15.

The request has an empty key and value. The bounded response includes:

- format version;
- instance-ID length and bytes;
- lifecycle mode (`standalone` or `managed-idle`);
- process start timestamp;
- server PID for local diagnostics;
- configured transport kind.

Do not return persistence paths, token paths, certificate paths, command-line
arguments, or other sensitive configuration.

Behavior:

- ordinary connect-only clients do not require `SERVER_INFO`;
- managed clients authenticate, negotiate capabilities, fetch server info, and
  require the returned instance ID to equal `<data_dir>/instance.id`;
- a different ID produces a dedicated instance-mismatch error and never starts
  or stops either server;
- an old or unrelated service occupying the endpoint produces a clear
  incompatible/occupied-endpoint error;
- a standalone server started through the new named `--data-dir` configuration
  with the same identity is a valid connection target but remains standalone
  and is never idle-stopped;
- a legacy positional standalone command with no instance identity remains
  usable by connect-only clients, but a managed client must reject it as
  unverifiable even if an operator believes its WAL path is equivalent.

Update `PROTOCOL.md` and every official client's constants without making the
new operation mandatory for older connect-only use.

## 8. Managed idle shutdown

### 8.1 Lifecycle modes

Add explicit server lifecycle configuration:

```text
--lifecycle standalone
--lifecycle managed-idle --idle-timeout-ms 60000
```

`standalone` is the default, including for every legacy command line.
`managed-idle` is passed by `ensure`. Use an explicit mode instead of treating
an idle timeout of zero as a hidden semantic switch.

### 8.2 Lease definition

For the first release, an accepted native client connection is a server lease:

- one client with several pooled connections still means `leases > 0`;
- abrupt client death releases its sockets through the OS and normal connection
  cleanup decrements the count;
- unauthenticated or incomplete connections remain bounded by the existing
  connection guard timeout;
- admin and metrics probes do not keep a managed data server alive forever;
- shared-memory attachments cannot currently provide reliable close
  notification and therefore do not participate in managed idle shutdown.

Managed mode with shared-memory clients is consequently out of scope for the
first release. `LocalKuttiDB` must use its network path when it owns managed
lifecycle, or the embedded format must later add explicit crash-recoverable
leases.

Because the Node.js pool is lazy, its managed constructor must eagerly open and
retain at least one native connection. Other managed SDKs must likewise keep a
connection for the lifetime of the client object instead of launching a server
and returning with no lease.

### 8.3 Idle timer

Use monotonic time:

- after readiness, start a `startup_orphan_timeout` deadline in case the
  launcher or creating application dies before opening a client connection;
- the first accepted native connection cancels the orphan deadline;
- whenever the connection count transitions from one to zero, set
  `idle_deadline = now + idle_timeout`;
- any accepted connection before the deadline cancels it;
- normal request activity does not extend the deadline while the count is zero.

A default between 30 and 60 seconds is appropriate. The SDK should default to
60 seconds, reject negative values, and enforce a small non-zero minimum to
avoid restart thrashing.

### 8.4 Shutdown race

Use a lifecycle mutex around admission state and connection-count transitions.
The idle-expiry path must:

1. lock lifecycle state;
2. verify mode is managed, state is `RUNNING`, connection count is still zero,
   and the monotonic deadline has expired;
3. atomically change state to `STOPPING`, preventing later accepts from becoming
   active connections;
4. unlock and wake the main thread plus every event loop;
5. close listeners and reject sockets accepted after the transition;
6. run the same graceful flush, snapshot, store close, socket unlink, and lock
   release path used by `SIGTERM`.

An arriving client either obtains a lease before `STOPPING` and cancels the
deadline, or receives a connection failure after `STOPPING`. In the latter
case its `ensure` invocation waits for the old ownership lock to release and
starts the recovered instance. It must never connect halfway through shutdown.

Replace or wrap the main thread's `pause()` loop with a lifecycle wake
primitive suitable for both signals and internal shutdown requests. Keep
signal handlers async-signal-safe; do not call mutex or condition-variable APIs
from a signal handler.

### 8.5 No first-client ownership

Closing the client that happened to win startup has no special meaning. The
server exits only after all native connections are gone for the grace period.
This prevents one web worker from terminating a server still used by other
workers.

## 9. Client connection algorithm

Every SDK must implement the same state machine:

1. Validate managed configuration and confirm the target is local.
2. Read the expected `instance.id` if it exists. Do not create or overwrite it
   merely because an endpoint is already occupied.
3. Attempt one normal connection using the caller's connection timeout.
4. If transport connects, complete TLS, AUTH, capabilities, health as currently
   required, and managed instance-identity validation. Return on success.
   If no expected identity file exists, reject the connected endpoint as
   unmanaged/unverifiable rather than adopting it.
5. Invoke `ensure` only for a definite local absence:
   - TCP `ECONNREFUSED`;
   - Unix socket `ENOENT` or `ECONNREFUSED`.
6. Do not launch after DNS errors, route failures, timeouts, TLS errors,
   certificate errors, authentication rejection, protocol mismatch, permission
   errors, resource exhaustion, or a connection that closes after protocol
   traffic begins.
7. Run `ensure --json` with a bounded startup timeout and capture a bounded
   amount of output.
8. Read and validate the instance ID returned by `ensure`, then connect again
   and perform the complete handshake and identity validation.
9. If the retry fails, report both the final connection category and safe
   launcher context, including the log path.

Never replay a request. Automatic startup occurs only during client creation or
an explicit reconnect call before an application operation is sent.

### 9.1 Executable discovery

Use this order:

1. explicit typed `executable` parameter;
2. `KUTTIDB_SERVER` environment variable;
3. a packaged sibling executable where the SDK distribution defines one;
4. `kuttidb` found on `PATH`.

Resolve the selected executable before spawning and pass an argument vector.
Never concatenate a command string or use `shell=True`.

### 9.2 Error model

Add client errors that retain existing general error compatibility:

- `ManagedServerConfigurationError`;
- `ManagedServerStartupError` with safe category and log path;
- `ManagedServerStartupTimeout`;
- `ManagedServerInstanceMismatch`;
- `ManagedServerEndpointOccupied`.

Do not include auth tokens or unrestricted stderr in exception strings.

## 10. SDK rollout

### 10.1 Python first

Implement the reference behavior in `src/kuttidb_client.py`:

- extract the existing socket/TLS/AUTH setup into `_connect_once`;
- add immutable `ServerParams` with validation;
- add `KuttiDBClient.managed(...)`;
- add bounded launcher invocation and JSON validation;
- add capabilities/server-info identity validation;
- preserve current constructor behavior when `server` is absent;
- update `clients/local_client.py` so managed lifecycle never silently selects
  an untracked shared-memory attachment;
- ensure context-manager close releases the connection lease normally.

### 10.2 Node.js

- add `Client.managed(options)` or a `server` option;
- keep existing lazy pools for standalone mode;
- eagerly create one pool connection in managed mode and expose an async
  creation API, because a synchronous constructor cannot safely await launch;
- ensure `close()` destroys every connection so idle shutdown can begin.

### 10.3 Go

- add a typed `ManagedOptions` and `NewManaged` constructor;
- use `exec.CommandContext` with argument slices and deadlines;
- retain the current eagerly populated pool as the lease;
- keep `New`, `NewAuthenticated`, and `NewTLS` connect-only.

### 10.4 Rust

- add `ManagedOptions` and `Client::connect_managed` plus the corresponding
  pool constructor;
- use `std::process::Command` without a shell;
- preserve current `connect` and `connect_authenticated` behavior.

### 10.5 Java

- add a builder or immutable `ManagedServerOptions` rather than multiplying
  positional constructors;
- use `ProcessBuilder` with a list of arguments and a bounded wait;
- preserve every current constructor as connect-only.

Do not release an SDK as managed-capable until it passes the same behavior
matrix as Python. A server-side launcher keeps SDK implementations thin, but
each SDK still owns correct error classification, timeout handling, executable
discovery, and identity validation.

## 11. Server configuration mapping

The managed typed configuration should initially cover:

- data directory;
- executable path;
- Unix or loopback TCP transport;
- durability mode and fsync interval;
- memory, value, batch, client, thread, and embedded-region limits where
  applicable;
- auth token file path;
- TLS certificate/key paths for explicit managed TCP;
- queue and stream WAL overrides only in the advanced API;
- optional metrics and admin listener configuration;
- lifecycle, idle timeout, startup orphan timeout, startup timeout, and log
  path.

Defaults for the simple factory:

- Unix-only transport;
- `<data_dir>/kuttidb.sock`;
- `<data_dir>/kuttidb.wal` with existing inferred queue and stream WAL names;
- periodic cache durability, preserving the current default;
- managed-idle lifecycle;
- 60-second idle and startup-orphan timeouts;
- `<data_dir>/kuttidb.log`;
- no admin or metrics listener;
- no TLS for the owner-only Unix socket;
- no embedded shared-memory transport in managed v1.

Add `--no-tcp` or a unified repeatable `--listen` configuration so a managed
Unix instance does not also bind the default TCP port. Existing invocations
must continue to create the current loopback TCP listener.

## 12. Security requirements

- Auto-start only literal loopback TCP or local Unix sockets. Avoid DNS-based
  locality decisions in the first release.
- Prefer Unix-only managed mode and place its socket inside the owner-only data
  directory.
- Never pass an auth token value in launcher arguments, environment, JSON
  output, lock metadata, or logs. Pass an owner-only token file path.
- Apply existing certificate and private-key ownership checks to managed mode.
- Do not weaken non-loopback authentication enforcement.
- Bound launcher stdout/stderr, JSON size, instance ID, paths, timeouts, and
  every metadata field.
- Refuse foreign-owned files and unsafe symlink replacements.
- Ensure managed log messages omit keys, values, tokens, and complete process
  arguments.
- A local user who can connect to an explicitly configured loopback port can
  keep a connection open and postpone idle shutdown. Owner-only Unix mode is
  the security and lifecycle default.
- Treat network filesystems with unreliable advisory locking as unsupported for
  managed mode unless their locking behavior is explicitly verified.

## 13. Failure semantics

| Condition | Required result |
| --- | --- |
| Server already running and identity matches | Connect; do not spawn |
| Standalone server already running and identity matches | Connect; never idle-stop it |
| Valid KuttiDB with another identity occupies endpoint | Instance-mismatch error |
| Unrelated process occupies TCP port/socket endpoint | Occupied/incompatible error; do not replace it |
| Twenty clients start simultaneously | One server becomes owner; all clients converge on it |
| Winning launcher crashes before fork | OS releases bootstrap lock; another launcher retries |
| Launcher dies after server exec | Server continues; waiters observe ownership/readiness |
| Server dies during startup | Launcher returns categorized failure and log path |
| Startup hangs | Deadline expires; no unbounded wait |
| Bad executable or permissions | Startup error; no repeated shell fallback |
| Wrong token or certificate | Auth/TLS error; never start a second server |
| Persistence ownership conflict | Server exits before recovery or WAL mutation |
| Client is killed | OS closes connection; idle deadline begins when count reaches zero |
| New client arrives during idle grace | Deadline is canceled |
| New client loses race with graceful stop | Wait for ownership release, then restart and recover |
| Server crashes after client creation | Current operation fails normally; no automatic replay |
| Repeated invalid startup config | Waiting clients receive the same failure category; bounded cooldown prevents a fork storm |
| Stale lock metadata | Ignored for ownership; advisory lock state is authoritative |
| Stale owned Unix socket | Safely replaced only after ownership lock and existing owner checks |

Persist a short failure timestamp/category and a safe normalized-configuration
digest under the bootstrap lock after a startup failure. During a small
cooldown, waiters with the same digest should return that failure instead of
launching identical configurations serially. A different configuration is not
suppressed by the old failure. The record is an anti-stampede hint, not a
correctness lock, and must not contain secrets.

## 14. Observability

Add safe server statistics and metrics:

- lifecycle mode and state;
- managed instance ID;
- managed starts, startup failures, and ownership conflicts;
- active native connections;
- idle-shutdown deadline remaining, when armed;
- idle shutdown count;
- startup-orphan shutdown count;
- last shutdown reason category.

Log lifecycle transitions once:

- ownership acquired;
- recovery started/completed;
- readiness signaled;
- first client attached;
- idle deadline armed/canceled;
- graceful idle shutdown started/completed;
- ownership conflict or startup failure.

Do not emit a message on every connection poll or every idle timer tick.

## 15. Testing strategy

Add focused C unit tests plus a multiprocess Python integration suite. Use
polling with deadlines and explicit readiness signals instead of fixed sleeps.

### 15.1 Lock and ownership tests

- Two processes contend for the same data-directory lock; exactly one wins.
- Two processes request the same TCP endpoint with different data directories;
  exactly one endpoint owner wins despite `SO_REUSEPORT`.
- One data directory requested on different endpoints still has one storage
  owner.
- Killing the owner releases locks and permits recovery.
- Stale metadata does not block a new owner.
- Lock acquisition order is deterministic for separate WAL paths.
- Foreign-owned, symlinked, non-regular, and permission-denied lock targets are
  rejected before storage changes.

### 15.2 Concurrent ensure tests

- Start 2, 8, 32, and at least 64 independent client processes behind a
  barrier; assert one server PID and one instance ID.
- Every client performs a unique write and reads it through another client.
- Kill the elected launcher at each boundary: before lock, after lock, after
  fork, before ready, and immediately after ready.
- Force server startup failure and verify waiters do not launch serially during
  the failure cooldown.
- Verify unrelated port occupation, old/incompatible protocol, wrong auth, and
  TLS failure never trigger another server.
- Verify a standalone server satisfies clients but is not converted to managed
  lifecycle.

### 15.3 Idle lifecycle tests

- No client connects after launch: startup-orphan timeout shuts the server down.
- One client closes: server remains during grace and exits after it.
- Several clients close in different orders: shutdown starts only after the
  final connection closes.
- A new client connects just before expiry: shutdown is canceled.
- Hammer the exact expiry boundary repeatedly: the client either keeps the old
  server or starts one recovered successor, never observes two owners.
- Kill clients with `SIGKILL`: sockets close and the managed server exits.
- Exercise Python context managers, Go pools, eager Node managed connection,
  Rust clients/pools, and Java close behavior.
- Confirm admin and metrics polling do not keep the data server alive.
- Confirm graceful idle shutdown preserves cache snapshot/WAL recovery, durable
  queue messages, ACKs, stream records, and group offsets.

### 15.4 Identity tests

- Matching ID succeeds across restart.
- Different data directory on the same endpoint fails closed.
- Copied WALs without `instance.id` form a different instance.
- A complete restored directory retains identity.
- An older server without `SERVER_INFO` remains usable by connect-only clients
  and is rejected clearly by a managed client expecting identity.

### 15.5 Regression gates

- Run the full existing `make test` suite.
- Add managed lifecycle tests to Make and CMake/CTest without requiring Node,
  Go, Java, or Rust for the C/Python core gate.
- Run ASan/UBSan for lock, ready-channel, accept/shutdown, and cleanup paths.
- Run TSan around lifecycle state and connection-count transitions.
- Repeat persistence crash/recovery and embedded WAL tests to ensure companion
  lifetime locks do not change acknowledgement semantics.
- Benchmark standalone connection acceptance before and after lifecycle locking;
  the lifecycle mutex must not enter the request hot path.

## 16. File-level change map

The exact module location may move under `src/app/` if the planned server
modularization lands first. Preserve these ownership boundaries either way.

| File | Planned responsibility |
| --- | --- |
| `src/instance_lock.c/.h` | Secure lock paths, canonical lock identities, deterministic multi-lock acquisition, metadata, and release |
| `src/managed_lifecycle.c/.h` | Standalone/managed state, connection leases, monotonic deadlines, admission synchronization, and shutdown request |
| `src/managed_launcher.c/.h` | `ensure` parsing, bootstrap election, detached exec, readiness channel, JSON output, log handling, and failure cooldown |
| `src/server.c` | Early subcommand dispatch, named managed configuration hooks, ownership acquisition before recovery, readiness notification, accept/close hooks, and shared graceful shutdown |
| `src/platform.c/.h` | Only portable wrappers genuinely shared by macOS/Linux, such as safe monotonic deadlines or lifecycle wakeup; keep process policy out of this layer |
| `src/test_managed_lock.c` | Deterministic lock, metadata, unsafe-path, and multiprocess ownership unit tests |
| `src/test_managed_server.py` | End-to-end launcher, race, identity, crash, idle-stop, recovery, and security tests |
| `src/kuttidb_client.py` | Python `ServerParams`, managed factory, launcher integration, identity handshake, and errors |
| `clients/local_client.py` | Safe interaction between managed lifecycle and shared-memory fallback |
| `clients/nodejs/kuttidb_client.js` | Async managed factory and eager managed lease connection |
| `clients/go/cache.go` | `ManagedOptions` and managed constructors |
| `clients/rust/src/lib.rs` | `ManagedOptions`, client, and pool managed constructors |
| `clients/java/KuttiDBClient.java` | Managed builder/options and launcher integration |
| `PROTOCOL.md` | Protocol minor bump, `SERVER_INFO`, capability bit, and wire format |
| `Makefile`, `CMakeLists.txt` | Compile new modules and register focused tests without changing the default dependency footprint |

Also update `make install` so a normal local installation places the `kuttidb`
server executable somewhere the managed SDK discovery order can find it. Do not
assume a source-tree build in the public API.

## 17. Implementation phases

### Phase 0: contract and compatibility tests

1. Add tests proving legacy clients remain connect-only.
2. Add tests proving legacy server commands default to standalone lifecycle.
3. Document the managed/standalone terminology and exact absence-error list.
4. Reserve the protocol opcode and capability bit.

Exit criterion: behavior is specified before process-control code lands.

### Phase 1: server ownership safety

1. Add secure companion lock helpers.
2. Acquire endpoint, persistence, and embedded ownership locks before recovery.
3. Hold descriptors through graceful shutdown and release on every failure path.
4. Add diagnostic metadata and distinct exit statuses.
5. Add multiprocess ownership tests, including reuse-port contention.

Exit criterion: two new server processes cannot serve one endpoint or mutate
one persistent instance concurrently, regardless of how they were started.

### Phase 2: instance identity and server info

1. Add atomic `instance.id` creation/loading.
2. Add `SERVER_INFO`, capability bit 15, and protocol documentation.
3. Add Python reference decoding and identity tests.
4. Add identity/lifecycle fields to safe stats and metrics.

Exit criterion: a managed client can prove that an endpoint represents its
requested data directory without learning sensitive server paths.

### Phase 3: managed server lifecycle

1. Add standalone and managed-idle modes.
2. Add monotonic orphan and idle deadlines.
3. Synchronize accepts, closes, and `RUNNING -> STOPPING` transitions.
4. Add a safe internal wakeup for main/event loops.
5. Route idle stop through the existing graceful shutdown sequence.
6. Add Unix-only listener support.

Exit criterion: killed clients leave no long-running managed server after the
grace period, and arrival/shutdown races never create simultaneous owners.

### Phase 4: `kuttidb ensure`

1. Add subcommand parsing while retaining legacy invocation.
2. Implement bootstrap election, second checks, detachment, readiness channel,
   JSON result, safe logging, deadlines, and failure cooldown.
3. Add high-contention and launcher-crash integration tests.

Exit criterion: many independent ensure calls converge on one ready process and
receive deterministic errors when startup cannot succeed.

### Phase 5: Python managed API

1. Refactor connection establishment without changing operation methods.
2. Add `ServerParams`, managed factory, executable discovery, and error types.
3. Implement strict error classification and identity verification.
4. Integrate `LocalKuttiDB` safely.
5. Add API examples and end-to-end tests.

Exit criterion: the documented Python example works from an empty directory,
under simultaneous processes, after crashes, and after idle restart.

### Phase 6: remaining official SDKs

Implement Node.js, Go, Rust, and Java in that order or according to actual user
demand. Each SDK must pass the shared lifecycle behavior matrix before its API
is documented as supported.

Exit criterion: managed mode has consistent semantics and error boundaries in
every advertised SDK.

### Phase 7: packaging and deployment guidance

1. Ensure packages can locate or explicitly configure the server executable.
2. Update the quick start to lead with managed local mode for appropriate
   single-host projects.
3. Keep standalone examples for containers, service managers, and remote use.
4. Document that each container-local data directory is an independent
   instance; managed mode is not cross-container coordination.
5. Add migration guidance for users upgrading from old binaries that do not
   participate in lifetime locks.

## 18. Documentation changes

Update:

- `GETTING_STARTED.md`: managed local first-run example and standalone alternative;
- `README.md`: two lifecycle modes and intended project sizes;
- `ARCHITECTURE.md`: launcher, ownership locks, identity, and idle state machine;
- `PROTOCOL.md`: protocol minor, `SERVER_INFO`, and capability bit 15;
- `DEPLOYMENT.md`: when not to use managed mode;
- `DURABILITY.md`: graceful idle stop has the same acknowledgement/recovery
  semantics as signal-driven graceful shutdown;
- every SDK README/example: opt-in API, executable resolution, local-only
  restriction, timeout behavior, and close semantics.

State explicitly:

- managed mode reduces process-management work; it does not provide
  replication, failover, or multi-node coordination;
- one data directory must not be mounted for active writing by servers on
  multiple machines;
- automatic restart never makes an in-flight operation safe to replay;
- externally started servers remain operator-controlled.

## 19. Acceptance criteria

The feature is complete only when all of the following are true:

1. Existing server commands and client constructors behave exactly as before.
2. A one-line managed client can create a fresh directory, start KuttiDB, use
   cache/queue/stream features, close, idle-stop, and recover later.
3. At least 64 simultaneous client processes produce exactly one server owner.
4. No new server process opens or mutates persistence files without holding all
   required lifetime ownership locks.
5. Managed clients refuse a healthy but wrong instance.
6. Auth, TLS, protocol, timeout, and permission failures never trigger an
   accidental start.
7. Client and launcher crashes do not leave permanent bootstrap ownership or an
   indefinitely running unused managed server.
8. A connection arriving at the idle-shutdown boundary either keeps the old
   instance or starts one successor; it never creates split traffic.
9. Graceful idle shutdown preserves all currently documented durability
   acknowledgement points.
10. Secrets never appear in arguments, JSON output, lock metadata, exceptions,
    or logs.
11. Managed mode works on both macOS and Linux.
12. The complete existing test suite, new lifecycle suite, sanitizers, and
    persistence recovery gates pass.

## 20. Recommended delivery boundary

The minimum valuable release is phases 0 through 5: server ownership safety,
identity, idle lifecycle, central launcher, and the Python managed API. This is
enough to validate the product experience without duplicating immature logic
across every SDK.

Do not ship client auto-start before the server lifetime locks land. Do not ship
idle shutdown before accept/close transitions are synchronized. Do not present
managed mode as safe for selecting a data directory before instance identity is
verified.
