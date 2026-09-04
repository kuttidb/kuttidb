#!/usr/bin/env python3
"""Stage the Python client sources into the publishable `kuttidb` package.

The canonical sources stay where the tests expect them:

- ``src/kuttidb_client.py``     -> ``clients/python/kuttidb/client.py``
- ``clients/kuttidb_embed.py``  -> ``clients/python/kuttidb/embed.py``
- ``clients/local_client.py``   -> ``clients/python/kuttidb/local.py``
- ``LICENSE``                   -> ``clients/python/LICENSE``

The staged files are build artifacts (gitignored); editing them has no
effect. Run ``make package-python`` (or this script) before building a
wheel, or just let the release workflow do it.

Usage:
    python3 clients/python/prepare.py
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent          # clients/python
REPO = HERE.parent.parent                        # repository root

SOURCES = [
    (REPO / "src" / "kuttidb_client.py", HERE / "kuttidb" / "client.py"),
    (REPO / "clients" / "kuttidb_embed.py", HERE / "kuttidb" / "embed.py"),
    (REPO / "clients" / "local_client.py", HERE / "kuttidb" / "local.py"),
    (REPO / "LICENSE", HERE / "LICENSE"),
]


def main() -> int:
    if not (HERE / "kuttidb" / "__init__.py").exists():
        print("error: clients/python/kuttidb/__init__.py is missing", file=sys.stderr)
        return 1
    for src, dest in SOURCES:
        if not src.exists():
            print(f"error: source not found: {src}", file=sys.stderr)
            return 1
        shutil.copyfile(src, dest)
        print(f"staged {dest.relative_to(REPO)}")
    print("staging complete; build with: python3 -m build clients/python")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
