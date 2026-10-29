"""Where the application's own files live, named once and engine-free.

`DATA_DIR` is the repository's `data/` — `/app/data` in both images, the one
directory both containers bind-mount (`./data:/app/data`) and the only one the
web container (uid 999) is known to be able to write. The write-chain markers,
the report ledgers' spool, the backup drills' evidence and the UTM re-parse
marker all live there.

It used to be `core.duckdb_constants.DB_DIR`, named for the DuckDB file that
sits in it, so every one of those modules imported a DuckDB module to find a
directory. Step 14's S14-01 moved it here; `DB_DIR` stays in
`core.duckdb_constants` as the same object for one release, because
`core.duckdb_store` and the suite still address the DuckDB file through it.
"""
from __future__ import annotations

from pathlib import Path

DATA_DIR: Path = Path(__file__).parent.parent / "data"
