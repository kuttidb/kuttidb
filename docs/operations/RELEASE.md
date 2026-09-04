# Release process

How official KuttiDB binaries are built, tested, and published. The pipeline
lives in [`.github/workflows/release.yml`](../../.github/workflows/release.yml)
and is fully tag-driven: a tag is the release.

Client SDK packages (PyPI, npm, crates.io, the Go module) are released
separately with their own language-prefixed tag scheme — see
[CLIENT_PUBLISHING.md](CLIENT_PUBLISHING.md).

## Release cycle

KuttiDB is pre-1.0 and uses `MAJOR.MINOR.PATCH` with optional pre-release
suffixes:

| Tag | Result |
|---|---|
| `v0.1.0` | Stable release of the 0.1 line |
| `v0.1.0-beta.1` | Pre-release — the hyphen makes the GitHub Release a *prerelease* |
| `v0.0.2` | Patch release |

Conventions while pre-1.0:

- **MINOR** bumps for milestone features (new messaging semantics, new
  platform support). These may include protocol-visible additions; the wire
  protocol negotiates capabilities, so clients keep working.
- **PATCH** bumps for fixes only — no new protocol capabilities.
- **Pre-releases** (`-beta.N`, `-rc.N`) for wider testing before a stable
  cut. Breaking durability or acknowledgement changes must land behind a
  pre-release first and be called out in the release notes.

There is no time-based cadence yet; releases follow milestones in
[ROADMAP.md](../plans/ROADMAP.md).

## Cutting a release

1. **Gate locally.** The CI gate is the same suite, but run it first:
   ```sh
   make test
   ```
   A change to acknowledgement or recovery behavior must ship with its
   crash-test (see [AGENTS.md](../../AGENTS.md)).

2. **Tag and push.** Annotated tags, `v` prefix:
   ```sh
   git tag -a v0.0.2 -m "KuttiDB 0.0.2"
   git push origin v0.0.2
   ```

3. **The pipeline takes over.** On the tag push, the `Release official
   binaries` workflow runs four build jobs in parallel:

   | Build | Runner | Runtime floor |
   |---|---|---|
   | `linux-x86_64` | `ubuntu-22.04` | glibc 2.35, OpenSSL 3 (Ubuntu 22.04+, Debian 12+, RHEL 9+) |
   | `linux-arm64` | `ubuntu-22.04-arm` | same |
   | `macos-x86_64` | `macos-15-intel` | macOS 12+ (`CMAKE_OSX_DEPLOYMENT_TARGET=12.0`) |
   | `macos-arm64` | `macos-15` | macOS 12+ |

   Each job: CMake `Release` build with TLS → **full `ctest` suite as a
   release gate** (all 14 tests, including crash-recovery) → verification that
   OpenSSL is really linked → tarball + SHA-256.

4. **Release job publishes.** When all builds pass, it aggregates
   `SHASUMS256.txt` and creates the GitHub Release with the tarballs. A
   failed gate anywhere means **no release is published** — a partial release
   is not possible.

### Dry runs

Run the identical pipeline without publishing from the Actions tab
(*Release official binaries → Run workflow*) or:

```sh
gh workflow run release.yml
```

Artifacts appear on the workflow run; no GitHub Release is created. Use this
when touching the build, packaging, or the workflow itself.

## Release artifacts

Every tarball `kuttidb-<version>-<os>-<arch>.tar.gz` contains:

| File | Purpose |
|---|---|
| `kuttidb` | Server binary (TLS via OpenSSL linked in) |
| `kuttidb-bench` | Benchmark client |
| `libkuttidb_embed.so` / `.dylib` | Embedded library for SDK managed mode |
| `kuttidb-cli` | Python CLI client (needs `python3` at runtime) |
| `README.md`, `LICENSE` | Pointers and license terms |

Install: extract and copy `kuttidb` (and optionally `kuttidb-cli`) somewhere
on `PATH`. See [GETTING_STARTED.md](../guides/GETTING_STARTED.md).

## Hotfixes

For a regression on a released line, branch from the release tag:

```sh
git checkout -b hotfix-0.0.2 v0.0.2
# ... fix, test ...
git tag -a v0.0.3 -m "KuttiDB 0.0.3" && git push origin v0.0.3 hotfix-0.0.2
```

Then merge the fix back into `main`. Re-publishing an existing tag is not
supported: delete the release and tag only if the release is broken beyond
repair, and prefer a new patch version.

## Operational notes

- **Do not re-push a tag.** Releases are immutable; move forward with a new
  version instead.
- **Linux floor is set by the oldest runner.** The workflow pins
  `ubuntu-22.04` deliberately; moving to a newer image raises the glibc floor
  for every user — treat that as a compatibility decision, not a chore.
- **Windows is not built.** The platform layer still needs the IOCP /
  named-pipe / file-locking backend ([ROADMAP.md](../plans/ROADMAP.md)).
- **macOS signing/notarization is not done yet.** Binaries are unsigned; the
  first run may need a right-click → Open or
  `xattr -d com.apple.quarantine`. Gatekeeper policy is tracked in the
  roadmap.
- Workflow file changes only take effect once merged to the default branch —
  tag runs use the workflow definition *on the tag*, so a pipeline fix must be
  tagged too.
