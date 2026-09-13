# Client SDK publishing

How the native client libraries (plus the management console, which follows
the same npm scheme) are packaged and published to the language registries.
The scheme mirrors the server pipeline ([RELEASE.md](RELEASE.md)) and is
fully tag-driven: **a tag is the release**. Server binaries use plain `v*`
tags; each client uses a language-prefixed tag so every SDK versions
independently — clients only need to agree on the wire protocol, not on the
server version number.

## Packages and tags

| Language | Registry | Package | Release tag | Version lives in | Workflow |
|---|---|---|---|---|---|
| Python | [PyPI](https://pypi.org/project/kuttidb) | `kuttidb` | `py-vX.Y.Z` | `clients/python/kuttidb/__init__.py` (`__version__`) | `release-python.yml` |
| Node.js | [npm](https://www.npmjs.com/package/@kuttidb/client) | `@kuttidb/client` | `node-vX.Y.Z` | `clients/nodejs/package.json` | `release-node.yml` |
| Rust | [crates.io](https://crates.io/crates/kuttidb) | `kuttidb` | `rust-vX.Y.Z` | `clients/rust/Cargo.toml` | `release-rust.yml` |
| Go | git only (no registry) | `github.com/kuttidb/kuttidb/clients/go` | `go-vX.Y.Z` | — | `release-go.yml` (gate) |
| Java | Maven Central | `io.github.kuttidb:kuttidb-client` | `java-vX.Y.Z` | `clients/java/pom.xml` | `release-java.yml` |
| Console | [npm](https://www.npmjs.com/package/@kuttidb/management-ui) | `@kuttidb/management-ui` | `console-vX.Y.Z` | `apps/management-ui/package.json` | `release-management-ui.yml` |

Tag names use the manifest version string, e.g. `node-v0.0.1-beta`,
`rust-v0.0.1-beta`, `py-v0.0.1b0` (PEP 440 spelling of the same version),
`console-v0.0.1-beta`.

The C companion (`libkuttidb_client` + `src/kuttidb_client.h`) is distributed
inside the server release tarballs (see
[RELEASE.md](RELEASE.md#release-artifacts)) and installed by `make install`;
it is not a registry package. Coordinate its ABI/version notes with the
server version in the same release: the companion and the server always ship
from the same commit.

The Python package is staged from the canonical source:
`clients/python/prepare.py` copies `src/kuttidb_client.py` into
`clients/python/kuttidb/` — edit the canonical file only, and verify the
staged wheel imports the installed package (not a repository-relative path)
before publishing. All six SDKs must expose the atomic job completion
surface with the frozen method mapping in
[../guides/CLIENT_FEATURE_MATRIX.md](../guides/CLIENT_FEATURE_MATRIX.md);
a release whose SDKs disagree on the mapping is not publishable.

## Installing as a consumer

```sh
pip install kuttidb            # import kuttidb
npm install @kuttidb/client    # require("@kuttidb/client")
cargo add kuttidb
go get github.com/kuttidb/kuttidb/clients/go
# Maven Central (io.github.kuttidb.client package):
#   <dependency><groupId>io.github.kuttidb</groupId>
#              <artifactId>kuttidb-client</artifactId></dependency>
npx @kuttidb/management-ui     # management console, no install needed
```

## Cutting a release

1. **Bump the version** in the manifest of the client you are releasing
   (see the table above) and commit. One commit per client is fine; never
   mix an SDK bump into a server release commit.
2. **Tag and push:**
   ```sh
   git tag -a node-v0.0.7-beta -m "@kuttidb/client 0.0.7-beta" && git push origin node-v0.0.7-beta
   ```
3. **The workflow takes over.** Each release workflow runs its language gate
   first and publishes only if the gate passes. A failed gate means nothing
   is published.

Per-language notes:

- **Python** — `prepare.py` stages `src/kuttidb_client.py`,
  `clients/kuttidb_embed.py`, and `clients/local_client.py` into the
  `kuttidb` package (staged files are gitignored; edit the sources, never
  the staged copies). The workflow builds sdist + wheel, runs `twine check`
  and an import smoke test, then publishes via **Trusted Publishing (OIDC)** —
  no API token is stored. Test locally with `make package-python` followed by
  `python3 -m build clients/python`.
- **Node.js** — CommonJS, zero dependencies; `files` ships only
  `kuttidb_client.js`. Published with `npm publish --access public
  --provenance --tag <dist-tag>`; the workflow derives the dist-tag from the
  semver (prereleases publish under `beta`, stable versions under `latest`),
  so `npm install @kuttidb/client` never hands out a beta — beta testers use
  `npm install @kuttidb/client@beta`.
- **Rust** — `cargo test` gates, then `cargo publish` via
  `CARGO_REGISTRY_TOKEN`.
- **Go** — publishing *is* the tag (see [Go modules](#go-modules)); the
  workflow is a build/vet/smoke gate so a `go-v*` tag carries the same
  guarantees as the other clients.
- **Console** — not a client SDK but published the same way. The gate runs
  `pnpm --filter @kuttidb/management-ui lint` and `test`, then `pnpm --filter
  @kuttidb/management-ui build` (Vite for the static client, `tsc` for the
  Fastify gateway and the `kuttidb-management-ui` CLI entry point), then
  `npm publish` ships only `dist/` and `README.md` — the React/Radix/Tailwind
  build-time packages stay in `devDependencies` and are never installed by
  `npx`. The build step also chmods the compiled CLI entry point executable
  as a defensive measure (npm and pnpm both fix the bit themselves on
  install, but a bare `node_modules/.bin` symlink or a manual tarball
  extraction should not depend on that). Keep the `bin` path in
  `package.json` without a leading `./` — with one, `npm publish` logs a
  `"bin[...] script name ... was invalid and removed"` warning; the mapping
  itself still survives (npm only normalizes the path), but the warning is
  worth avoiding.

## First-release checklist

Package names are claimed on first publish and squatters are a real risk, so
release in this order once the manifests land on `main`:

1. **TestPyPI dry run** — Actions tab → *Release Python client (PyPI)* →
   Run workflow (manual runs publish to TestPyPI). Verifies the whole
   pipeline without touching the real name.
2. **`rust-v0.0.1-beta`** — claims `kuttidb` on crates.io.
3. **`node-v0.0.1-beta`** — claims `@kuttidb/client` on npm.
4. **`console-v0.0.1-beta`** — claims `@kuttidb/management-ui` on npm.
5. **`py-v0.0.1b0`** — claims `kuttidb` on PyPI.
6. **`go-v0.1.0`** — any time; the module was made fetchable when
   `go.mod` moved to the `github.com/kuttidb/kuttidb/clients/go` path.

## Registry configuration (one-time setup, already done)

- **PyPI / TestPyPI Trusted Publishing.** Pending publishers are registered
  on both registries: owner `kuttidb`, repository `kuttidb`, workflow
  filename `release-python.yml`, environment `pypi`. The GitHub
  `pypi` environment exists in repo settings. If a publish fails OIDC
  validation, check these four values first — the workflow and the pending
  publisher must match exactly.
- **npm.** Org `kuttidb` exists; both scoped packages (`@kuttidb/client` and
  `@kuttidb/management-ui`) publish as public via `publishConfig`. Auth
  currently uses the same `NPM_TOKEN` granular token for both.
- **crates.io.** `CRATES_IO_TOKEN` is stored; the token is only exposed to
  the release workflow.

### Switching npm to Trusted Publishing (after the first release)

npm attaches trusted publishers per package, so the package must exist
first. On npmjs.com → package `@kuttidb/client` → Settings → **Trusted
Publisher**: owner `kuttidb`, repository `kuttidb`, workflow filename
`release-node.yml`, environment *empty*. Repeat for `@kuttidb/management-ui`
with workflow filename `release-management-ui.yml`. Only delete `NPM_TOKEN`
from the repo secrets once every package that uses it has switched — no
workflow change is needed either way, `id-token: write` and `--provenance`
are already in place for both.

## Maven Central

The Java client publishes to the Sonatype **Central Portal** (the legacy
OSSRH staging flow is retired). The `io.github.kuttidb` namespace is
verified, `clients/java/pom.xml` and `release-java.yml` are in place, and a
`java-v*` tag deploys `io.github.kuttidb:kuttidb-client` after the protocol
gate passes. Secrets required (repo settings only — never committed):

- `MAVEN_CENTRAL_USERNAME` / `MAVEN_CENTRAL_PASSWORD` — Central Portal user
  token, mapped to the `central` server id of a runner-local settings.xml.
- `GPG_PRIVATE_KEY` (armored secret key) / `GPG_PASSPHRASE` — artifact
  signing key, mapped to the `gpg.passphrase` server id that
  maven-gpg-plugin reads. The public key must be published to
  `keyserver.ubuntu.com` before the first deploy or Central rejects the
  signatures.

The client sources live under `src/main/java/io/github/kuttidb/client/`;
`Smoke.java` and `ManagedSmoke.java` stay at the `clients/java` root as
default-package test entry points so the Makefile smokes keep running
without a Maven toolchain.

## Go modules

Go has no registry — `go get` fetches directly from the repository, so:

- `clients/go/go.mod` must declare the full fetchable path
  `module github.com/kuttidb/kuttidb/clients/go`.
- Versions are git tags on that path: `go-v0.1.0`, `go-v1.2.3`, …
- **v2 and later require the module path to gain a `/v2` suffix**
  (`github.com/kuttidb/kuttidb/clients/go/v2`) *and* the tag to match —
  treat that as a breaking-change decision, not a chore.
- Pseudo-versions (`go get ...@<commit>`) work for testing untagged commits.

### Go embedding is explicitly opt-in

The default network package must build and import with **either CGO
setting and no KuttiDB headers or libraries** — embedding is gated behind a
build tag so a plain `go get github.com/kuttidb/kuttidb/clients/go` never
enters the shared-memory path:

- `clients/go/embed.go` (and the embedding smoke command
  `clients/go/cmd/embedsmoke`) carry `//go:build cgo && kuttidb_embed`.
- Building the embedding API requires **all** of: `CGO_ENABLED=1`, the
  `-tags kuttidb_embed` tag, and a working C toolchain. With the tag but no
  CGO the embedding symbols stay unavailable — nothing silently falls back
  to network behavior.
- Header/library search paths: build from a full repository checkout (the
  cgo directives use `<checkout>/src` for headers and
  `<checkout>/libkuttidb_embed.{dylib,so}` from `make`, with an rpath), or
  link an installed library by staging `embed.h` + `kuttidb.h` and the
  shared object in a prefix and passing `CGO_CPPFLAGS`/`CGO_LDFLAGS` (e.g.
  `-I<prefix>/include`, `-L<prefix>/lib -lkuttidb_embed`).
- `clients/go/scripts/external-consumer-check.sh` is the release gate for
  this: it copies the module into a fresh consumer module with **no ancestor
  `src/`, no built library, no repository `go.work`, and no checkout-wide
  replace**, strips the repository-dependent integration tests, and builds
  with CGO disabled and enabled (the enabled case compiles an unrelated tiny
  cgo package), verifying `embed.go` is never selected and KuttiDB never
  enters the native linker flags. After a release exists, verify the real
  downloaded module (`go get ...@go-vX.Y.Z`) the same way instead of the
  local replace.
- The repository-dependent Go integration tests (they build the server with
  `make`) run only in the checkout — never inside the stripped consumer
  fixture. `make go-embed-smoke` and `make go-external-consumer` wire both
  checks into `make test`, the PR validation workflow, and
  `release-go.yml`.

## Rules

- **A tag must exactly match its manifest version.** The release workflows
  reject mismatches before any registry upload: for example,
  `py-v0.0.7b0`, `node-v0.0.7-beta`, `rust-v0.0.7-beta`,
  `java-v0.0.9-beta`, and `console-v0.0.7-beta`.
- **Never re-push a tag** — registries reject duplicate versions and
  re-publishing is treated as a broken release (same policy as
  [RELEASE.md](RELEASE.md)).
- **Never edit a staged file** under `clients/python/kuttidb/` other than
  `__init__.py`; they are overwritten from the canonical sources at build
  time.
- **Workflow file names are contractual.** The PyPI/TestPyPI pending
  publishers and (later) the npm trusted publisher reference
  `release-python.yml` / `release-node.yml` / `release-management-ui.yml` by
  name; renaming a workflow breaks publishing until the registry side is
  updated.

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| PyPI: "Invalid or non-existent authentication information" / OIDC rejected | Workflow filename or environment no longer matches the pending publisher on pypi.org |
| PyPI: "File already exists" | That version was already uploaded — bump and re-tag (a new tag, not a re-push) |
| npm: "You must specify a tag using --tag when publishing a prerelease version" | A prerelease cannot land on `latest` — `release-node.yml`/`release-management-ui.yml` derive the dist-tag automatically now; older runs need a version bump and a new tag |
| npm: 403 Forbidden | `NPM_TOKEN` expired or lacks write scope for `@kuttidb/client` or `@kuttidb/management-ui` |
| npm: provenance failure | `id-token: write` missing or the package's repository field does not point at this repo |
| npm publish: `"bin[...] script name ... was invalid and removed"` warning | The `bin` path in `package.json` had a leading `./`; drop it — the mapping itself is not actually removed, npm only normalizes the path |
| `npx @kuttidb/management-ui`: command not found or does nothing | The published tarball is missing `dist/` — check that the gate's build step ran before `npm publish` (it does in `release-management-ui.yml`; a manual `npm publish` outside CI relies on the `prepublishOnly` script) |
| crates.io: "crate `kuttidb` already exists" | Version already published; bump `Cargo.toml` |
| PyPI wheel missing `kuttidb/__init__.py` or import fails after install | Hatchling applied repository `.gitignore` patterns (anchored at the package root) and pruned the package — keep `ignore-vcs = true` in `clients/python/pyproject.toml` |
| Go: "module ... not found" after tagging | Tag pushed before the `go.mod` path fix landed on `main` — re-tag from a commit that contains it |
