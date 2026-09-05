#!/usr/bin/env bash
# Run the isolated cache/jobs/replay demo. See docs/guides/SAAS_DEMO.md.
# Override KUTTIDB_DEMO_BASE_URL for a GitHub Pages subpath or local preview.
# KUTTIDB_SERVER selects an existing executable; otherwise install temporarily.
set -euo pipefail

DEMO_BASE="${KUTTIDB_DEMO_BASE_URL:-https://kuttidb.com}"
DEMO_BASE="${DEMO_BASE%/}"
command -v python3 >/dev/null 2>&1 || { echo 'Python 3.10+ is needed for the demo client.' >&2; exit 1; }
python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)' || {
  echo 'Python 3.10+ is needed for the demo client.' >&2; exit 1;
}
command -v curl >/dev/null 2>&1 || { echo 'curl is needed to download the demo.' >&2; exit 1; }

DEMO_TMP="$(mktemp -d /tmp/kutti-try-XXXXXX)"
trap 'rm -rf "$DEMO_TMP"' EXIT
fetch() { curl -fsSL --connect-timeout 10 --max-time 120 --retry 2 "$1" -o "$2"; }
fetch "$DEMO_BASE/demo.tar.gz" "$DEMO_TMP/demo.tar.gz"
# Extract only the two expected regular files, never archive-supplied paths.
python3 - "$DEMO_TMP" <<'PY'
from pathlib import Path
import sys, tarfile
directory = Path(sys.argv[1])
with tarfile.open(directory / "demo.tar.gz") as archive:
    for name in ("saas_demo.py", "kuttidb_client.py"):
        entry = archive.getmember(name)
        if not entry.isfile() or entry.size > 2 * 1024 * 1024:
            raise SystemExit("Unexpected demo archive contents")
        (directory / name).write_bytes(archive.extractfile(entry).read())
PY

DEMO_SERVER="${KUTTIDB_SERVER:-}"
if [ -z "$DEMO_SERVER" ]; then
  DEMO_SERVER="$(command -v kuttidb || true)"
fi
if [ -z "$DEMO_SERVER" ] && [ -x "$HOME/.local/bin/kuttidb" ]; then
  DEMO_SERVER="$HOME/.local/bin/kuttidb"
fi
if [ -z "$DEMO_SERVER" ]; then
  echo 'Downloading a checksum-verified KuttiDB binary for this temporary demo...'
  fetch "$DEMO_BASE/install.sh" "$DEMO_TMP/install.sh"
  bash "$DEMO_TMP/install.sh" --bin-dir "$DEMO_TMP/bin"
  DEMO_SERVER="$DEMO_TMP/bin/kuttidb"
fi
python3 "$DEMO_TMP/saas_demo.py" --server "$DEMO_SERVER" --delay 0.4
