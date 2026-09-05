#!/usr/bin/env python3
"""Stage the current demo + canonical client for the static GitHub Pages site."""
from pathlib import Path
import tarfile

root = Path(__file__).resolve().parent.parent
with tarfile.open(root / "landing/demo.tar.gz", "w:gz") as archive:
    archive.add(root / "examples/saas_demo.py", arcname="saas_demo.py")
    archive.add(root / "src/kuttidb_client.py", arcname="kuttidb_client.py")
print("Staged landing/demo.tar.gz from the current checkout")
