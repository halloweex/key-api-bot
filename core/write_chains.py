"""The write chains — the modules that write Postgres's tables in DuckDB's
place — and what each one declares.

Stage 4 moved WRITES chain by chain, and while a chain could still be routed
back to DuckDB this module was the one answer every consumer asked: where a
chain's writes went (`chain_modes`), which of its tables the hourly copy and
the daily comparison stood down on (`stood_down_*`), which flags disagreed
with the latch (`mismatched_chains`). Step 14's S14-00 made every answer a
constant — every chain writes Postgres — and S14-11 deleted the answers with
the flags: no `KS_WRITE_*` is read by this build.

What is left is the list and what each module declares on it:

- `CHAIN` — the name of the chain's latch marker and of its `/api/health`
  entry;
- `CHAIN_TABLES` — the tables it writes, which nothing else here may ship;
- `CHAIN_SYNC_KEYS` — the `last_sync_*` watermarks it stamps, which the
  store's getter and setter keep in `meta.chain_watermarks`
  (`chain_for_sync_key`).

`tests/unit/test_write_chains.py` walks `core/` for every module that declares
`CHAIN_TABLES` and a latch, and fails if one is not registered here — so a new
chain cannot be written and forgotten.
"""
from __future__ import annotations

from types import ModuleType
from typing import Dict, FrozenSet, Optional

from core.analytics_captures import CAPTURES
from core import (
    pg_buyers_write, pg_expense_types_write, pg_expenses_write, pg_goals_write,
    pg_inventory_write, pg_managers_write, pg_orders_write,
)
from core import pg_catalogue_write
from core import pg_forecast_write
from core import (
    pg_dq_journal_write, pg_traffic_ledger_write, pg_watchdog_write,
    pg_weekly_ledger_write,
)

# In the order they were registered: chain 1 (inventory), 8 (manual
# expenses), 7a (goals), 6a (expense types), 4 (buyers), 3 (orders), 5
# (managers), 6 (catalogue), 7b-3 (goal tables and forecast), then the four
# that shadowed DuckDB until step 14's S14-10 — 9 (quality journal), 10
# (watchdog samples), 11a and 11b (the two report ledgers).
WRITE_CHAINS = (pg_inventory_write, pg_expenses_write, pg_goals_write,
                pg_expense_types_write, pg_buyers_write, pg_orders_write,
                pg_managers_write, pg_catalogue_write, pg_forecast_write,
                pg_dq_journal_write, pg_watchdog_write,
                pg_weekly_ledger_write, pg_traffic_ledger_write)


# The analysts' captures (revision 0036): each capture table and the one
# module that writes it, read off `core.analytics_captures.CAPTURES`.
# Deliberately NOT a chain's `CHAIN_TABLES` (the design's §3.3): a chain's
# tables must each succeed a DuckDB table in the frozen `FATES`, and none of
# these ever had one; and `chain_latch.claim` would stamp `owner:<table>` rows
# for them on every write, which an image from before step 14 reads. The walk
# in `tests/unit/test_write_chains.py` holds each to its one writer.
CAPTURE_TABLES: Dict[str, str] = {table: spec.writer
                                  for table, spec in CAPTURES.items()}

# The key tables revision 0036 creates beside the captures, and the module
# that will write them: the warehouse job, phase 2 (the buyer key's top-up and,
# by owner decision D3, the person key). Nothing in this build writes them,
# and the same walk holds that.
KEY_TABLES: Dict[str, str] = {
    "app.analytics_buyer_key": "core.ch_mart",
    "app.analytics_person_key": "core.ch_mart",
    "app.analytics_phone_excluded": "core.ch_mart",
    "app.analytics_person_merges": "core.ch_mart",
}


def chain_name(chain: ModuleType) -> str:
    return chain.__name__.rsplit(".", 1)[-1]


def chain_for_sync_key(key: str) -> Optional[ModuleType]:
    """The one chain that declares `key` in its CHAIN_SYNC_KEYS, or None."""
    for chain in WRITE_CHAINS:
        if key in getattr(chain, "CHAIN_SYNC_KEYS", ()):
            return chain
    return None


def chain_tables() -> FrozenSet[str]:
    """Every table a registered chain writes. Pure.

    Since step 14's S14-11 this is what `stood_down_tables()` answered for
    every state production could be in: every chain writes Postgres, so every
    chain's tables are held from the hourly copy and the daily comparison out
    of DuckDB."""
    return frozenset(t for chain in WRITE_CHAINS for t in chain.CHAIN_TABLES)


def sync_keys() -> FrozenSet[str]:
    """Every `last_sync_*` key a registered chain stamps. Pure — what
    `stood_down_sync_keys()` answered since S14-00."""
    return frozenset(k for chain in WRITE_CHAINS
                     for k in getattr(chain, "CHAIN_SYNC_KEYS", ()))
