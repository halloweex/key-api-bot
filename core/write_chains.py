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
from typing import Dict, FrozenSet, Iterable, Optional, Tuple

from core import (
    pg_expense_types_write, pg_expenses_write, pg_goals_write, pg_inventory_write,
)

logger = logging.getLogger(__name__)

# Chain 7a (`pg_goals_write`, DN-25) is the third: `app.revenue_goals`, the
# three goal amounts a human types. Chain 6a (`pg_expense_types_write`, DN-26)
# is the fourth: `bronze.expense_types`, the dictionary /expenses names its
# costs by. Both flags are off by default, so until `KS_WRITE_GOALS` or
# `KS_WRITE_EXPENSE_TYPES` says postgres each stands down nothing and only adds
# a row to the `/api/health` block.
WRITE_CHAINS = (pg_inventory_write, pg_expenses_write, pg_goals_write,
                pg_expense_types_write)

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
#
# And since DN-06 the flag is not the last word. A chain that has already
# written Postgres is LATCHED (`core/chain_latch.py`, owner decision OD-19 (a)),
# and `writes_postgres()` answers True whatever the variable says — including a
# value nobody can read, because routing a latched chain back to DuckDB would
# start a second writer beside the first. Everything here reads that one answer,
# so the writers, the sync keys, the shipper and the comparison move together.


def chain_name(chain: ModuleType) -> str:
    return chain.__name__.rsplit(".", 1)[-1]


def _chain_state(chain: ModuleType) -> Dict[str, Optional[object]]:
    """One chain's entry in `chain_modes()`. Never raises.

    Separate so a question about particular tables can ask only the chains
    that declare them (`stood_down_among`) and still reach the verdict through
    the same lines as every other consumer — one rule, whichever door."""
    from core import chain_latch

    name = chain_name(chain)
    # The environment on its own, so the published error survives the latch
    # answering over it.
    try:
        env_mode, error = (
            "postgres" if chain.env_writes_postgres() else "duckdb"), None
    except Exception as exc:  # noqa: BLE001 — carried out, not swallowed
        env_mode, error = None, str(exc)
    since = chain_latch.latched_at(name)
    return {
        "env": chain.WRITE_ENV,
        "mode": "postgres" if (since or env_mode == "postgres") else env_mode,
        "error": error,
        "latched": since is not None,
        "latched_at": since,
        "mismatch": since is not None and env_mode != "postgres",
    }


def chain_modes() -> Dict[str, Dict[str, Optional[object]]]:
    """`{chain: {"env", "mode", "error", "latched", "latched_at", "mismatch"}}`.

    `mode` is where the chain's writes actually go — "postgres", "duckdb", or
    None when the environment was not understood and no latch overrides it.
    `mismatch` is the state DN-06 exists to make visible: the chain has already
    written Postgres, so it keeps writing Postgres (OD-19 (a)), while its
    variable says something else. Both halves are published, because a latched
    chain with an unreadable flag is two problems and reporting one would hide
    the other. Never raises.

    **`mismatch` is latch against environment, and never latch against owner
    row.** The other disagreement — a marker whose first Postgres write failed,
    or an owner row whose marker was lost — needs the copy that lives in
    Postgres, and this block is read from the local marker cache precisely so
    it still answers while Postgres is down. A query here would make the one
    place that can say "the writes are going to a store you cannot reach" fail
    with that store. So the two copies are compared where both are already in
    hand: the daily `reconcile_operational`, which files
    `chain_latch_disagrees` (CRITICAL) either way and reaches a human through
    the 09:00 digest. The cost is named rather than discovered: a failed first
    write is invisible until the next morning, and `latched: true` with
    `mismatch: false` is what it looks like here in the meantime.
    """
    return {chain_name(chain): _chain_state(chain) for chain in WRITE_CHAINS}


def mismatched_chains() -> Dict[str, str]:
    """`{chain: latched_at}` for chains that own their tables in Postgres while
    their variable says otherwise.

    The shipper stamps these tables failing and the canary pages WARN on them.
    Neither may repair it: the only way back is `scripts/chain_copy_back.py`,
    which copies the rows to DuckDB, compares them at zero and releases both
    copies of the latch.
    """
    return {name: str(state["latched_at"])
            for name, state in chain_modes().items() if state["mismatch"]}


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


def stood_down_among(tables: Iterable[str]) -> FrozenSet[str]:
    """Which of `tables` have changed hands, asking only the chains that
    declare one of them. Never raises.

    Always `stood_down_tables() & tables` — the same `_chain_state` verdict,
    so a flag error or a latch stands a table down here exactly as it does
    for the shipper. What differs is who is asked, and that is DN-01's rule
    carried from sync keys to tables: the order sync asks this on every
    write, and evaluating every chain there would log an unrelated
    `KS_WRITE_*` typo once a minute from the one path that must stay quiet
    about other chains' business. With no chain declaring a table — every
    order table today — nothing is evaluated at all: no variable read, no
    marker file opened.
    """
    wanted = frozenset(tables)
    moved: set = set()
    errors: Dict[str, str] = {}
    for chain in WRITE_CHAINS:
        mine = wanted.intersection(chain.CHAIN_TABLES)
        if not mine:
            continue
        state = _chain_state(chain)
        if state["mode"] != "duckdb":
            moved |= mine
        if state["error"]:
            errors[chain_name(chain)] = str(state["error"])
    if errors:
        logger.error("write chain flag(s) not understood, stood down: %s", errors)
    return frozenset(moved)


def stood_down_sync_keys() -> FrozenSet[str]:
    """Every `last_sync_*` key whose chain writes Postgres, or whose flag is not
    understood. Never raises.

    The freshness check asks this; the store's getter and setter ask
    `chain_for_sync_key` instead, so a key no chain declares evaluates no flag
    at all. A chain with no sync keys — chain 8 — contributes none.
    """
    return stood_down_sync_keys_checked()[0]
