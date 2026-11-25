"""What the reclassify dry run must never write through."""
from __future__ import annotations


# What the audit hook cannot see, so what a run may not even load. DuckDB opens
# and writes its file from C++ without raising a single audit event, so a
# DuckDB write would leave the event list empty; `core.pg` is the application's
# pool and the one reader of `KS_PG_DSN`, whose server is the very one the
# read-only login reaches, so its connection would look like the script's own.
# A module never imported can do neither.
UNSEEN_WRITERS = ("duckdb", "core.duckdb_store", "core.pg")


# The application's read-write DSN, handed to every audited run pointing at a
# port nothing listens on: reading it and connecting shows up as a connection
# there, which none of these runs may make.
POISONED_PG_DSN = "postgresql://ks_app:poison@127.0.0.1:9/poison"
