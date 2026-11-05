"""A shim: the analytics store's old name, for one release.

`DuckDBStore` was the DuckDB analytics store — a DuckDB connection, its
schema, migrations, checkpoint, backup and FATAL bookkeeping, with the
repository mixins on top. Since step 14's S14-18 nothing of DuckDB is left in
it: the mixins and the writers' doors are `core.store.AnalyticsStore`, and
this module re-exports that under the old name with its two functions, so the
callers that import `core.duckdb_store` (about 150 `get_store()` sites, and
the suite's patches of `core.duckdb_store.get_store`) keep working until
S14-21 renames them. It imports no DuckDB driver.

What went with S14-18, and where it is now: the schema (`_init_schema`) and
the migrations (`core/migrations.py`, `core/duckdb_sequences.py`) live only in
the image tagged `rollback-step14`, whose `scripts/compact_duckdb.py` rebuilds
a file from the Parquet archive (`deploy/restore_from_export.py`); the switch
(`core/duckdb_switch.py`) and its page; the hourly checkpoint and the nightly
backup jobs, unregistered since the week of silence began.
"""
from core.store import AnalyticsStore, StoreRemoved, close_store, get_store

DuckDBStore = AnalyticsStore

__all__ = ["AnalyticsStore", "DuckDBStore", "StoreRemoved", "close_store", "get_store"]
