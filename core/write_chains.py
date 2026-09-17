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

import logging
from types import ModuleType
from typing import Dict, FrozenSet, Optional, Tuple

from core import pg_expenses_write, pg_inventory_write

logger = logging.getLogger(__name__)

WRITE_CHAINS = (pg_inventory_write, pg_expenses_write)

# A KS_WRITE_* value no chain understands must stop that chain and nothing
# else. The registry used to evaluate every chain's flag for every question, so
# a typo in KS_WRITE_EXPENSES raised inside `get_last_sync_time("orders")` — the
# first thing the incremental sync asks — and order intake stopped silently for
# the eight hours the canary allows the orders mirror. Found planning stage 4's
# remaining work (DN-01), 2026-09-17; the policy question is the owner's OD-18.
#
# The rule now:
# - a sync key consults only the chain that declares it;
# - a chain whose flag is not understood is STOOD DOWN — its tables are neither
#   shipped nor compared, because shipping could overwrite rows Postgres alone
#   holds — and the error is carried out to be seen, never swallowed;
# - the chain's own writers still raise on their own typo.


def chain_name(chain: ModuleType) -> str:
    return chain.__name__.rsplit(".", 1)[-1]


def chain_modes() -> Dict[str, Dict[str, Optional[str]]]:
    """`{chain: {"env", "mode", "error"}}` — mode is "postgres", "duckdb" or
    None when the value was not understood. Never raises."""
    out: Dict[str, Dict[str, Optional[str]]] = {}
    for chain in WRITE_CHAINS:
        try:
            mode, error = ("postgres" if chain.writes_postgres() else "duckdb"), None
        except Exception as exc:  # noqa: BLE001 — carried out, not swallowed
            mode, error = None, str(exc)
        out[chain_name(chain)] = {"env": chain.WRITE_ENV, "mode": mode, "error": error}
    return out


def chain_for_sync_key(key: str) -> Optional[ModuleType]:
    """The one chain that declares `key` in its CHAIN_SYNC_KEYS, or None."""
    for chain in WRITE_CHAINS:
        if key in getattr(chain, "CHAIN_SYNC_KEYS", ()):
            return chain
    return None


def stood_down_tables_checked() -> Tuple[FrozenSet[str], Dict[str, str]]:
    """`(tables, {chain: error})`. A chain whose flag is not understood stands
    down with the chains that write Postgres."""
    tables, errors = set(), {}
    for name, state in chain_modes().items():
        chain = next(c for c in WRITE_CHAINS if chain_name(c) == name)
        if state["mode"] != "duckdb":
            tables.update(chain.CHAIN_TABLES)
        if state["error"]:
            errors[name] = state["error"]
    if errors:
        logger.error("write chain flag(s) not understood, stood down: %s", errors)
    return frozenset(tables), errors


def stood_down_sync_keys_checked() -> Tuple[FrozenSet[str], Dict[str, str]]:
    """`(keys, {chain: error})`, the same rule for `last_sync_*` keys."""
    keys, errors = set(), {}
    for name, state in chain_modes().items():
        chain = next(c for c in WRITE_CHAINS if chain_name(c) == name)
        if state["mode"] != "duckdb":
            keys.update(getattr(chain, "CHAIN_SYNC_KEYS", ()))
        if state["error"]:
            errors[name] = state["error"]
    return frozenset(keys), errors


def stood_down_tables() -> FrozenSet[str]:
    """Every table whose chain writes Postgres, or whose flag is not understood.
    Never raises — see `stood_down_tables_checked` for the errors."""
    return stood_down_tables_checked()[0]


def stood_down_sync_keys() -> FrozenSet[str]:
    """Every `last_sync_*` key whose chain writes Postgres, or whose flag is not
    understood. Never raises.

    The freshness check asks this; the store's getter and setter ask
    `chain_for_sync_key` instead, so a key no chain declares evaluates no flag
    at all. A chain with no sync keys — chain 8 — contributes none.
    """
    return stood_down_sync_keys_checked()[0]
