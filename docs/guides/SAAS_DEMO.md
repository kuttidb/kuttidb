# Cache, jobs, and replay in 60 seconds

One report-generation workflow demonstrates all three capabilities of KuttiDB.
The demo makes real requests, checks the results, kills its own server, then
reopens the same data and verifies recovery. No account or Python package
installation is needed.

## Run it

On macOS or Linux with Python 3.10+ and curl:

```sh
curl -fsSL https://kuttidb.com/demo.sh | bash
```

The runner uses `kuttidb` on your PATH or in `~/.local/bin`. If neither exists,
it uses the existing [installer](../../landing/install.sh) to download a
checksum-verified release into a temporary folder. The usual
[release platform requirements](../operations/RELEASE.md) apply. Downloads
may take longer than a minute on a slow connection; the actual demo takes
seconds. It doesn't install into your system or change your PATH.

To inspect the runner before executing it, download
[demo.sh](../../landing/demo.sh) to a file and read it. The complete application
is [examples/saas_demo.py](../../examples/saas_demo.py).

From a source checkout:

```sh
make
python3 examples/saas_demo.py
```

To select a particular binary:

```sh
python3 examples/saas_demo.py --server /absolute/path/to/kuttidb
```

The hosted runner also accepts `KUTTIDB_SERVER`. If an older installed binary
cannot run the demo, select a current build or use the source-checkout path;
the runner reports the failure instead of replacing your existing binary.

## What happens

| Step | Real operation | Verified result |
|---|---|---|
| Cache | Store and read `report:42` with a five-minute TTL | Status is `queued` |
| Job | Publish `report:42` to a durable queue and consume it | The worker receives the same message identity and payload |
| Worker | Generate a three-row CSV and update the cached status | Status is `ready: 3 rows` |
| Event | Append the completion event to a bounded stream | The server returns the stored offset |
| ACK | Acknowledge the completed job | The queue is empty |
| Interrupted work | Publish and consume `report:43` without acknowledging it | A job is in flight when the process is killed |
| Restart | SIGKILL the demo child and reopen its data directory | Cache status recovers, report 43 is redelivered, report 42 remains acknowledged, and the event replays at its original offset |

Every check executes even if Python assertions are disabled. A failed check
exits nonzero and never prints the final success message. The same demo runs
in `make test` and the CMake release test suite.

The generated CSV is intentionally held in the demonstration worker's memory;
the recovered data is the cached status, queued work, and completion event.
In a real app, store the report artifact in durable application storage before
announcing it as ready.

## Isolation, memory, and delivery semantics

The demo owns a newly created `0700` temporary directory and a private Unix
socket. It opens no TCP listener, selects one event-loop thread, and sets a
16 MiB **cache** budget (this is not a process-memory limit). The queue is
limited to ten messages and the event stream to 1 MiB. It never connects to
or signals an existing instance. Its child process and temporary data are
cleaned up on success, error, Ctrl-C, or a handled termination signal.

`MEMORY` reports one observed server RSS sample using `ps`, excluding the
Python client. It reports unavailable when the host cannot supply that
measurement. This tiny fixture is not a load test, peak measurement, or a
whole-service resource guarantee.

Cache writes use `--durability always`; durable queue and stream operations
retain their fsync-before-confirmation behavior. SIGKILL verifies a process
crash on a healthy disk, not OS/power failure, disk destruction, or machine
loss. Queue delivery remains at-least-once.

The worker's cache update, stream append, and queue ACK are separate
operations. This demo does not claim an atomic transaction across all three.
A real worker must handle retries and duplicate external effects, including
the window between appending an event and acknowledging the job. See
[DURABILITY.md](../design/DURABILITY.md) for the existing atomic
cache-plus-queue operations and their exact scope.

## Recording and GitHub Pages

The landing page plays an actual captured local run, not a database running in
the visitor's browser. The README animation and page transcript come from the
same [recording](../../landing/demo-recording.json), which includes the host,
timestamp, server hash, and measured output. No production data is recorded.

To refresh it after a successful build:

```sh
python3 examples/saas_demo.py --delay 0.6 --record landing/demo-recording.json
python3 examples/render_demo_recording.py
```

Rendering the README GIF requires Pillow on the maintainer's machine; running
the demo does not. The renderer also updates the page's static transcript so
the result remains readable without JavaScript. Commit the JSON, GIF, and
updated landing page together.

The existing GitHub Pages workflow builds `landing/demo.tar.gz` from the
canonical example and `src/kuttidb_client.py` in the same checkout. It then
publishes the existing `landing/` directory. The tarball is generated and
ignored by Git; there is no second copy of the SDK to maintain.

For a local preview, stage the archive before starting a static server:

```sh
python3 examples/package_demo.py
python3 -m http.server 8000 --directory landing --bind 127.0.0.1
```

In another terminal, test the downloaded bundle against your local binary:

```sh
curl -fsSL http://127.0.0.1:8000/demo.sh | \
  KUTTIDB_DEMO_BASE_URL=http://127.0.0.1:8000 KUTTIDB_SERVER="$PWD/kuttidb" bash
```

`KUTTIDB_DEMO_BASE_URL` can also target a GitHub Pages project subpath. All
recording and asset links within the landing page are relative. On the public
site the default is `https://kuttidb.com`; the command becomes available when
the updated Pages workflow deploys this change.
