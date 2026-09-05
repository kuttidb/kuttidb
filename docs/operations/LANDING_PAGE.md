# Landing page

The homepage is `landing/index.html`. Its stylesheet is `landing/landing.css`,
interactions are in `landing/landing.js`, and client examples and installation
instructions are in `landing/sdk-examples.js`.

These are plain static files: no package installation, build step, external
fonts, framework, or application server is required to serve the website.

## Preview locally

From the repository root:

```sh
python3 -m http.server 4173 --directory landing
```

Open <http://localhost:4173/>. Serve the entire `landing/` directory so the logo,
recording, installer, and downloadable demo remain available.

The former `/alternative/` preview redirects to the main page. The old homepage
is available in Git history.

Clipboard buttons require HTTPS or localhost; commands remain selectable when
clipboard access is unavailable. Language selection also works when opening the
HTML directly from disk. Recording playback requires HTTP serving; a static
excerpt remains readable when the recording cannot be fetched or JavaScript is
disabled.

## GitHub Pages

The existing `.github/workflows/landing-pages.yml` workflow publishes all of
`landing/` when changes to that directory reach `main`. No workflow change is
needed. Relative assets support both repository-path URLs and custom domains.

## Client examples

The language selector updates the install command or dependency configuration,
usage examples, and client source link together. Cache, Queue, and Stream tabs
retain their selection when switching between Python, Node.js, Go, Java, and Rust.
The native C/C++ embedded library exposes cache operations, so its Queue and
Stream tabs are disabled and selecting it switches to Cache.

Examples include complete programs, connection cleanup, and error handling.
Queue examples print the message before acknowledging it; applications should
perform their real work before acknowledging. Socket examples expect a server
on port 7379. C/C++ needs a server-created shared-memory region, as documented in
the [embedded protocol guide](../design/PROTOCOL.md#embedded-shared-memory-mode).

Keep published package coordinates aligned with the client manifests and
[publishing guide](CLIENT_PUBLISHING.md). C/C++ uses the repository's native
library build instructions; it has no registry entry in that publishing guide.

## Server installation

The homepage's installation section distinguishes the official binary installer
from a source build. Keep its command and support claims aligned with
[`landing/install.sh`](../../landing/install.sh) and
[GETTING_STARTED.md](../guides/GETTING_STARTED.md): the official installer
supports macOS and Linux on arm64 and x86_64, verifies release checksums, and
installs to `~/.local/bin` by default. Source-build commands are run from a
cloned repository.

## Content sources

Product descriptions follow the [project README](../../README.md),
[durability contract](../design/DURABILITY.md), and
[stream semantics](../messaging/STREAMS.md). Browser playback reads the real
recording; it does not simulate a running database. Performance links point to
the recorded [benchmark methodology](BENCHMARKS.md).
