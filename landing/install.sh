#!/usr/bin/env bash
# ============================================================================
# KuttiDB installer.
#
# Downloads the right official binary for your platform from GitHub Releases
# and installs it into your PATH. No build tools required.
#
#   curl -fsSL https://kuttidb.github.io/kuttidb/install.sh | bash
#
# Options:
#   --version vX.Y.Z   pin a release (default: latest)
#   --bin-dir DIR      install location (default: ~/.local/bin)
#   --help             this message
#
# Environment overrides (useful for mirrors and CI):
#   KUTTIDB_RELEASES_URL  releases base (default
#                         https://github.com/kuttidb/kuttidb/releases)
#   KUTTIDB_VERSION       same as --version
#   KUTTIDB_INSTALL_DIR   same as --bin-dir
#   KUTTIDB_OS / KUTTIDB_ARCH  force platform detection
#
# Each tarball contains the `kuttidb` server, `kuttidb-bench`, the embedded
# library (libkuttidb_embed.so / .dylib), and the `kuttidb-cli` client.
# Checksums are verified against the release's SHASUMS256.txt before install.
#
# Process doc: docs/operations/RELEASE.md
# ============================================================================
set -euo pipefail

REPO="${KUTTIDB_REPO:-kuttidb/kuttidb}"
BASE="${KUTTIDB_RELEASES_URL:-https://github.com/${REPO}/releases}"
INSTALL_DIR="${KUTTIDB_INSTALL_DIR:-${HOME}/.local/bin}"
WANT_VERSION="${KUTTIDB_VERSION:-}"

# ---- output helpers (color only when attached to a terminal) ---------------
if [ -t 1 ] && [ -z "${NO_COLOR:-}" ]; then
  C_GOLD=$'\033[38;5;214m'; C_DIM=$'\033[2m'; C_RED=$'\033[31m'
  C_GREEN=$'\033[32m'; C_OFF=$'\033[0m'
else
  C_GOLD=""; C_DIM=""; C_RED=""; C_GREEN=""; C_OFF=""
fi
info() { printf '%s\n' "${C_GOLD}kuttidb${C_OFF} $*"; }
dim()  { printf '%s\n' "${C_DIM}$*${C_OFF}"; }
fail() { printf '%s\n' "${C_RED}error:${C_OFF} $*" >&2; exit 1; }

# ---- args ------------------------------------------------------------------
while [ $# -gt 0 ]; do
  case "$1" in
    --version)  [ $# -ge 2 ] || fail "--version needs a value (e.g. v0.0.5)"
                WANT_VERSION="$2"; shift 2 ;;
    --bin-dir)  [ $# -ge 2 ] || fail "--bin-dir needs a value"
                INSTALL_DIR="$2"; shift 2 ;;
    -h|--help)  sed -n '2,16p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *)          fail "unknown argument: $1 (try --help)" ;;
  esac
done

# ---- platform detection ----------------------------------------------------
# KUTTIDB_OS / KUTTIDB_ARCH are canonical wire names (linux|macos, x86_64|arm64)
# and are taken verbatim; otherwise map uname output.
if [ -n "${KUTTIDB_OS:-}" ]; then
  OS="$KUTTIDB_OS"
else
  case "$(uname -s)" in
    Linux)  OS="linux" ;;
    Darwin) OS="macos" ;;
    *) fail "unsupported operating system: $(uname -s)
KuttiDB ships Linux and macOS binaries today; on Windows use WSL2:
  wsl --install" ;;
  esac
fi
if [ -n "${KUTTIDB_ARCH:-}" ]; then
  ARCH="$KUTTIDB_ARCH"
else
  case "$(uname -m)" in
    x86_64|amd64)  ARCH="x86_64" ;;
    arm64|aarch64) ARCH="arm64" ;;
    *) fail "unsupported architecture: $(uname -m) (supported: x86_64, arm64)" ;;
  esac
fi
case "$OS-$ARCH" in
  linux-x86_64|linux-arm64|macos-x86_64|macos-arm64) ;;
  *) fail "unsupported platform: ${OS}-${ARCH} (supported: linux-x86_64, linux-arm64, macos-x86_64, macos-arm64)" ;;
esac

# ---- download helper (curl, then wget) -------------------------------------
fetch() { # fetch URL DEST   (DEST "-" = stdout)
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL --retry 3 --retry-delay 1 -o "$2" "$1"
  elif command -v wget >/dev/null 2>&1; then
    wget -q -O "$2" "$1"
  else
    fail "need curl or wget to download (or set KUTTIDB_VERSION and copy the
tarball from https://github.com/${REPO}/releases by hand)"
  fi
}
sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1
  else shasum -a 256 "$1" | cut -d' ' -f1; fi
}

# ---- resolve version and asset name ----------------------------------------
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

if [ -n "$WANT_VERSION" ]; then
  VERSION="${WANT_VERSION#v}"
  info "installing KuttiDB ${C_GOLD}v${VERSION}${C_OFF} for ${OS}-${ARCH}"
  FILENAME="kuttidb-${VERSION}-${OS}-${ARCH}.tar.gz"
  TARBALL_URL="${BASE}/download/v${VERSION}/${FILENAME}"
  SUMS_URL="${BASE}/download/v${VERSION}/SHASUMS256.txt"
else
  info "resolving the latest release for ${OS}-${ARCH}"
  SUMS_URL="${BASE}/latest/download/SHASUMS256.txt"
  fetch "$SUMS_URL" "$TMP/SHASUMS256.txt"
  # The sums file lists every platform's tarball; ours names the version.
  FILENAME="$(grep -o "kuttidb-[^\` ]*-${OS}-${ARCH}\.tar\.gz" "$TMP/SHASUMS256.txt" | head -1 || true)"
  [ -n "$FILENAME" ] || fail "no ${OS}-${ARCH} build found in the latest release
Try pinning one: --version v0.0.5"
  VERSION="${FILENAME#kuttidb-}"; VERSION="${VERSION%-${OS}-${ARCH}.tar.gz}"
  info "installing KuttiDB ${C_GOLD}v${VERSION}${C_OFF} for ${OS}-${ARCH}"
  TARBALL_URL="${BASE}/latest/download/${FILENAME}"
fi

# ---- download and verify ----------------------------------------------------
dim "  ↓ ${TARBALL_URL}"
fetch "$TARBALL_URL" "${TMP}/${FILENAME}"
fetch "$SUMS_URL" "$TMP/SHASUMS256.txt"

EXPECTED="$(grep " ${FILENAME}\$" "$TMP/SHASUMS256.txt" | awk '{print $1}' | head -1)"
[ -n "$EXPECTED" ] || fail "checksum for ${FILENAME} missing from SHASUMS256.txt"
ACTUAL="$(sha256_of "${TMP}/${FILENAME}")"
if [ "$ACTUAL" != "$EXPECTED" ]; then
  fail "checksum mismatch for ${FILENAME}
  expected $EXPECTED
  actual   $ACTUAL"
fi
dim "  ✓ sha256 verified"

# ---- unpack and install -----------------------------------------------------
mkdir -p "${TMP}/pkg"
tar -xzf "${TMP}/${FILENAME}" -C "${TMP}/pkg"
PKG_DIR="$(find "${TMP}/pkg" -maxdepth 1 -type d -name 'kuttidb-*' | head -1)"
[ -n "$PKG_DIR" ] || fail "unexpected tarball layout (no kuttidb-* directory)"

mkdir -p "$INSTALL_DIR"
for f in kuttidb kuttidb-cli kuttidb-bench; do
  [ -f "${PKG_DIR}/$f" ] && cp -p "${PKG_DIR}/$f" "${INSTALL_DIR}/$f" && chmod 0755 "${INSTALL_DIR}/$f"
done
for f in "${PKG_DIR}"/libkuttidb_embed.*; do
  [ -e "$f" ] && cp -p "$f" "$INSTALL_DIR/" && chmod 0644 "${INSTALL_DIR}/$(basename "$f")"
done

# ---- PATH hint ---------------------------------------------------------------
case ":${PATH}:" in
  *":${INSTALL_DIR}:"*) ;;
  *) dim "  ! ${INSTALL_DIR} is not on your PATH. Add it:"
     dim "      export PATH=\"${INSTALL_DIR}:\$PATH\"   # add to ~/.zshrc or ~/.bashrc" ;;
esac

info "${C_GREEN}installed ✓${C_OFF} kuttidb, kuttidb-cli, kuttidb-bench, libkuttidb_embed → ${INSTALL_DIR}"
dim "  start a server:  kuttidb 7379 kuttidb.wal"
dim "  full guide:      https://github.com/${REPO}#readme"
