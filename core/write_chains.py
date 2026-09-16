"""Which tables have changed hands — the one answer the shipper and the
comparison both ask.

Stage 4 moves WRITES chain by chain. When a chain writes Postgres, two things
must stop touching its tables, and they must stop together:

- the hourly `replicate_operational`, whose full replace out of a frozen DuckDB
  would roll back every row written since the switch, once an hour, looking
  healthy in between;
- the daily `reconcile_operational`, which would otherwise report every new
  row as a discrepancy the check itself created.

Chain 1 answered that with its own `CHAIN_TABLES` read at both sites. Chain 8 is
the second writer, and that is the moment the answer needs one home: spelled at
two sites per chain, the shipper and the comparison would come to disagree the
first time a chain was added to one list and not the other. So both sites call
`stood_down_tables()`, and `WRITE_CHAINS` is the list.

`tests/unit/test_write_chains.py` walks `core/` for every module that declares
both `CHAIN_TABLES` and `writes_postgres`, and fails if one is not registered
here — so a new chain cannot be written and forgotten.
"""
from __future__ import annotations

from typing import FrozenSet

from core import pg_expenses_write, pg_inventory_write

WRITE_CHAINS = (pg_inventory_write, pg_expenses_write)


def stood_down_tables() -> FrozenSet[str]:
    """Every table whose chain currently writes Postgres."""
    tables = set()
    for chain in WRITE_CHAINS:
        if chain.writes_postgres():
            tables.update(chain.CHAIN_TABLES)
    return frozenset(tables)


def stood_down_sync_keys() -> FrozenSet[str]:
    """Every `last_sync_*` key whose chain currently writes Postgres.

    The getter, the setter and the freshness check all ask this, so they cannot
    come to disagree about where a watermark lives. A chain with no sync keys —
    chain 8 — simply contributes none.
    """
    keys = set()
    for chain in WRITE_CHAINS:
        if chain.writes_postgres():
            keys.update(getattr(chain, "CHAIN_SYNC_KEYS", ()))
    return frozenset(keys)
