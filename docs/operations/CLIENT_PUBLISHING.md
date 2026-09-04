# Client SDK publishing

How the native client libraries are packaged and published to the language
registries. The scheme mirrors the server pipeline
([RELEASE.md](RELEASE.md)) and is fully tag-driven: **a tag is the release**.
Server binaries use plain `v*` tags; each client uses a language-prefixed tag
so every SDK versions independently — clients only need to agree on the wire
protocol, not on the server version number.

## Packages and tags

| Language | Registry | Package | Release tag | Version lives in | Workflow |
|---|---|---|---|---|---|
| Python | [PyPI](https://pypi.org/project/kuttidb) | `kuttidb` | `py-vX.Y.Z` | `clients/python/kuttidb/__init__.py` (`__version__`) | `release-python.yml` |
| Node.js | [npm](https://www.npmjs.com/package/@kuttidb/client) | `@kuttidb/client` | `node-vX.Y.Z` | `clients/nodejs/package.json` | `release-node.yml` |
| Rust | [crates.io](https://crates.io/crates/kuttidb) | `kuttidb` | `rust-vX.Y.Z` | `clients/rust/Cargo.toml` | `release-rust.yml` |
| Go | git only (no registry) | `github.com/kuttidb/kuttidb/clients/go` | `go-vX.Y.Z` | — | `release-go.yml` (gate) |
| Java | Maven Central | *pending — see below* | `java-vX.Y.Z` | `clients/java` | *not yet written* |

Tag names use the manifest version string, e.g. `node-v0.0.1-beta`,
`rust-v0.0.1-beta`, `py-v0.0.1b0` (PEP 440 spelling of the same version).

## Installing as a consumer

```sh
pip install kuttidb            # import kuttidb
npm install @kuttidb/client    # require("@kuttidb/client")
cargo add kuttidb
go get github.com/kuttidb/kuttidb/clients/go
```

## Cutting a release

1. **Bump the version** in the manifest of the client you are releasing
   (see the table above) and commit. One commit per client is fine; never
   mix an SDK bump into a server release commit.
2. **Tag and push:**
   ```sh
   git tag -a node-v0.0.2 -m "@kuttidb/client 0.0.2" && git push origin node-v0.0.2
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
  --provenance`.
- **Rust** — `cargo test` gates, then `cargo publish` via
  `CARGO_REGISTRY_TOKEN`.
- **Go** — publishing *is* the tag (see [Go modules](#go-modules)); the
  workflow is a build/vet/smoke gate so a `go-v*` tag carries the same
  guarantees as the other clients.

## First-release checklist

Package names are claimed on first publish and squatters are a real risk, so
release in this order once the manifests land on `main`:

1. **TestPyPI dry run** — Actions tab → *Release Python client (PyPI)* →
   Run workflow (manual runs publish to TestPyPI). Verifies the whole
   pipeline without touching the real name.
2. **`rust-v0.0.1-beta`** — claims `kuttidb` on crates.io.
3. **`node-v0.0.1-beta`** — claims `@kuttidb/client` on npm.
4. **`py-v0.0.1b0`** — claims `kuttidb` on PyPI.
5. **`go-v0.1.0`** — any time; the module was made fetchable when
   `go.mod` moved to the `github.com/kuttidb/kuttidb/clients/go` path.

## Registry configuration (one-time setup, already done)

- **PyPI / TestPyPI Trusted Publishing.** Pending publishers are registered
  on both registries: owner `kuttidb`, repository `kuttidb`, workflow
  filename `release-python.yml`, environment `pypi`. The GitHub
  `pypi` environment exists in repo settings. If a publish fails OIDC
  validation, check these four values first — the workflow and the pending
  publisher must match exactly.
- **npm.** Org `kuttidb` exists; the scoped package publishes as public via
  `publishConfig`. Auth currently uses the `NPM_TOKEN` granular token.
- **crates.io.** `CRATES_IO_TOKEN` is stored; the token is only exposed to
  the release workflow.

### Switching npm to Trusted Publishing (after the first release)

npm attaches trusted publishers per package, so the package must exist
first. On npmjs.com → package `@kuttidb/client` → Settings → **Trusted
Publisher**: owner `kuttidb`, repository `kuttidb`, workflow filename
`release-node.yml`, environment *empty*. Then delete `NPM_TOKEN` from the
repo secrets. No workflow change is needed — `id-token: write` and
`--provenance` are already in place.

## Maven Central (deferred)

The Java client publishes through the Sonatype **Central Portal** (the
legacy OSSRH staging flow is retired). Blocked on namespace verification;
the remaining steps once `io.github.kuttidb` is verified:

1. Generate a Portal user token → secrets `MAVEN_CENTRAL_USERNAME` /
   `MAVEN_CENTRAL_PASSWORD`.
2. Create a GPG signing key, publish the public key to
   `keyserver.ubuntu.com`, store `GPG_PRIVATE_KEY` / `GPG_PASSPHRASE`
   secrets.
3. Add `clients/java/pom.xml` (groupId `io.github.kuttidb`, artifactId
   `kuttidb-client`, sources + javadoc + GPG signing via the
   `central-publishing-maven-plugin`).
4. Write `release-java.yml` (tag `java-v*`) and add the row to the table
   above.

## Go modules

Go has no registry — `go get` fetches directly from the repository, so:

- `clients/go/go.mod` must declare the full fetchable path
  `module github.com/kuttidb/kuttidb/clients/go`.
- Versions are git tags on that path: `go-v0.1.0`, `go-v1.2.3`, …
- **v2 and later require the module path to gain a `/v2` suffix**
  (`github.com/kuttidb/kuttidb/clients/go/v2`) *and* the tag to match —
  treat that as a breaking-change decision, not a chore.
- Pseudo-versions (`go get ...@<commit>`) work for testing untagged commits.

## Rules

- **Never re-push a tag** — registries reject duplicate versions and
  re-publishing is treated as a broken release (same policy as
  [RELEASE.md](RELEASE.md)).
- **Never edit a staged file** under `clients/python/kuttidb/` other than
  `__init__.py`; they are overwritten from the canonical sources at build
  time.
- **Workflow file names are contractual.** The PyPI/TestPyPI pending
  publishers and (later) the npm trusted publisher reference
  `release-python.yml` / `release-node.yml` by name; renaming a workflow
  breaks publishing until the registry side is updated.

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| PyPI: "Invalid or non-existent authentication information" / OIDC rejected | Workflow filename or environment no longer matches the pending publisher on pypi.org |
| PyPI: "File already exists" | That version was already uploaded — bump and re-tag (a new tag, not a re-push) |
| npm: 403 Forbidden | `NPM_TOKEN` expired or lacks write scope for `@kuttidb/client` |
| npm: provenance failure | `id-token: write` missing or the package's repository field does not point at this repo |
| crates.io: "crate `kuttidb` already exists" | Version already published; bump `Cargo.toml` |
| PyPI wheel missing `kuttidb/__init__.py` or import fails after install | Hatchling applied repository `.gitignore` patterns (anchored at the package root) and pruned the package — keep `ignore-vcs = true` in `clients/python/pyproject.toml` |
| Go: "module ... not found" after tagging | Tag pushed before the `go.mod` path fix landed on `main` — re-tag from a commit that contains it |
