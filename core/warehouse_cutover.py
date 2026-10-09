"""Step 13 (DN-28, DN-29): the warehouse writer's mode, what it stands down,
whether this deployment is ready for the switch — and the switch.

Step 13 stops DuckDB deriving Silver, Gold and the UTM verdicts, and leaves
Postgres the only engine that computes them. DN-28 built everything the switch
needed in place; DN-29 is the switch.

- **`KS_WRITE_WAREHOUSE` is read once, in `core.runtime_modes.configure_modes()`**,
  before web's boot sync — a mode cached at scheduler start misses everything
  the boot does (DN-05b), and on an empty DuckDB the boot runs a full rebuild.
  `duckdb` is the default, and under it nothing here changes a byte of what
  DuckDB does. An unknown value runs as `duckdb` and publishes the error — on
  `/api/health` under `warehouse_writer_mode`, where the canary warns
  `warehouse_mode_invalid` — `KS_PG_DERIVE`'s rule and OD-09's answer: web is
  the only syncer, so refusing to start over this variable would stop order
  intake. After a flip the same value is the way back, so there it is also
  published as the unmet `value_understood`, and pages as one (OD-09 (b)).
- **`postgres` switches only when every precondition holds** —
  `evaluate_preconditions` over the environment and the facts it cannot
  hold, the Postgres revision among them. One unmet item and the process runs
  as `duckdb` and publishes the keys of what is unmet; the canary pages
  `warehouse_preconditions_unmet` (CRITICAL). OD-09 (b): never a raise. The
  verdict is reached once per process and kept, so web's startup and the
  scheduler cannot disagree about it — a revision read that timed out the
  second time must not hand a boot that ran under `postgres` a scheduler that
  runs under `duckdb`. **After a flip, a start with anything unmet IS the way
  back** — a full DuckDB rebuild, a new `since` on the next start under
  `postgres`, the `warehouse` group closed again — so a Postgres read that
  failed is asked again before the verdict (`REVISION_READ_ATTEMPTS`), and
  the facts this process holds itself are read outside that read's bound.
- **Under `postgres`** DuckDB stops deriving: `duckdb_derives()` is False, so
  the `warehouse_refresh` job is not registered, every production call of
  `refresh_warehouse_layers` is skipped (a test walks them), the DuckDB half of
  rebuild-silver is skipped, and `mark_warehouse_dirty` is a no-op. The DuckDB
  checks over Silver, Gold and UTM stand down (`stood_down_duckdb_checks`), as
  do the three mirror-landing comparisons that set DuckDB's Silver, Gold and
  UTM against Postgres'.
- **The writer is recorded** in DuckDB's `sync_metadata` under
  `warehouse_writer` by `settle_writer`, once per process before the boot sync.
  The first start under `postgres` records it and closes the `warehouse` alert
  group — its only resolver was the DuckDB tick, which no longer runs — once,
  recorded, so a second restart does not announce it again.
- **The way back.** A start under `duckdb` that finds `postgres` recorded owes
  DuckDB a full rebuild: it marks the warehouse dirty in full, and holds the
  stood-down checks down until a full tick validates and a UTM parse finishes
  — the Silver they would read is as old as the switch, and the verdicts a
  parse that raised leaves can be partial. When DuckDB's `silver_order_utm`
  is empty (a Sunday compaction ran in between) it also flags a reclassify as
  needed. A way back taken by a value nobody meant as `duckdb` publishes
  `value_understood` among the unmet preconditions.
- **The way back is refused while a write chain is latched over a table
  DuckDB derives from** (chain 3's orders, chain 5's classification): the
  start stays `postgres` instead, publishes the chains as `way_back_refused`
  and the canary pages — see THE WAY BACK, REFUSED.
- **The UTM doors** (refresh, reclassify, the `manager_comment` backfill and
  its CLI) parse in Postgres alone under `postgres`. The two that re-parse
  everything note it in the record, and the way back then empties DuckDB's
  `silver_order_utm` so the full tick it owes re-parses every verdict under
  the rules in force — see THE UTM DOORS.
- **`evaluate_preconditions(env, facts)`** names every unmet precondition of
  the switch, one entry each, so the readiness a person reads is a list of
  things to do rather than a "no". Pure over what it is handed. Postgres is
  asked two things inside one bound: its revision, and whether
  `bronze.expenses` holds its history — the /expenses read gate no read
  switch covers (`EXPENSES_HISTORY`).
  `readiness()` gathers the facts and is what `GET /api/warehouse/status`
  publishes under `cutover`.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, FrozenSet, List, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)

ENV = "KS_WRITE_WAREHOUSE"
DUCKDB = "duckdb"
POSTGRES = "postgres"
_VALID = (DUCKDB, POSTGRES)

# DN-29: `postgres` is acted on when its preconditions hold.
SWITCH_BUILT = True

_value: Optional[str] = None
_mode: Optional[str] = None
_mode_error: Optional[str] = None
# The unmet preconditions `configure_mode` found for `postgres`, and the value
# the verdict was reached for — kept for the life of the process.
_unmet: Tuple["Unmet", ...] = ()
_decided_for: Optional[str] = None
# `value_understood`, when `settle_writer` finds that a value this build does
# not understand took the way back from a recorded `postgres` — kept apart
# from `_unmet`, which `configure_mode` rewrites on every call.
_value_unmet: Tuple["Unmet", ...] = ()
# What `settle_writer` found and did, per process — see THE RECORDED WRITER.
_settled = False
_held = False
# When the hold began, for its age: `/api/health` publishes it and the canary
# warns on a hold that outlives what a way back takes (`held_for_s`).
_held_since: Optional[datetime] = None
# The way back's full DuckDB tick has validated — the Silver half of what the
# hold waits for; the UTM half is a parse that finished (`note_refresh`).
_full_validated = False
_reclassify_needed = False
_writer_record: Optional[Dict[str, Any]] = None
# The latched write chains that made this start refuse the way back and stay
# `postgres` — see THE WAY BACK, REFUSED.
_way_back_refused: Tuple[str, ...] = ()


def configure_mode() -> str:
    """Read `KS_WRITE_WAREHOUSE` once. Never raises. Called by
    `configure_modes()`; idempotent, and logs a cause once rather than on every
    call — web's startup and the scheduler both configure.

    Under `postgres` the preconditions are evaluated here, the Postgres
    revision included (`_gather_facts_blocking`), and only the first call of a
    process asks: the verdict is kept."""
    global _value, _mode, _mode_error, _unmet, _decided_for, _way_back_refused
    value = os.getenv(ENV, DUCKDB).strip().lower() or DUCKDB
    # Local, and asked whatever the value: a start that would run as duckdb
    # after a flip is a way back, and a latched chain owning what DuckDB
    # derives from refuses it (THE WAY BACK, REFUSED, below).
    holding = _chains_holding_duckdbs_sources()
    if value in _VALID:
        error = None
    else:
        error = (f"{ENV}={value!r} is not one of {_VALID}; running as "
                 f"{(POSTGRES if holding else DUCKDB)!r}")
    if error and error != _mode_error:
        logger.error(error)
    _value, _mode_error = value, error
    if value != POSTGRES:
        _unmet, _decided_for = (), None
        _mode = _the_way_back(holding, f"{ENV}={value!r}")
        return _mode
    if _decided_for == POSTGRES and _mode is not None:
        return _mode
    env = os.environ
    unmet = tuple(evaluate_preconditions(env, _gather_facts_blocking(env)))
    _unmet, _decided_for = unmet, POSTGRES
    if unmet:
        _mode = _the_way_back(holding, f"{ENV}=postgres with {len(unmet)} "
                                       "precondition(s) of the switch unmet ("
                                       + ", ".join(u.key for u in unmet) + ")")
        if _mode == DUCKDB:
            logger.critical(
                "%s=postgres, but %d precondition(s) of the switch are unmet — "
                "running as duckdb, DuckDB goes on deriving: %s", ENV, len(unmet),
                "; ".join(f"{u.key}: {u.detail}" for u in unmet))
    else:
        _mode, _way_back_refused = POSTGRES, ()
        logger.warning(
            "%s=postgres and every precondition holds: DuckDB no longer derives "
            "Silver, Gold or the UTM verdicts in this process", ENV)
    return _mode


def mode() -> str:
    """The mode this process runs; `duckdb` until `configure_mode` has run."""
    return _mode or DUCKDB


def value() -> Optional[str]:
    """What the variable said, as read; None until `configure_mode` has run."""
    return _value


def mode_error() -> Optional[str]:
    return _mode_error


def preconditions_unmet() -> Tuple["Unmet", ...]:
    """What held `postgres` back at start; empty unless it was asked for —
    or unless, after a flip, a value this build does not understand took the
    way back (`value_understood`, found by `settle_writer`): the same full
    DuckDB rebuild and hold an unmet precondition costs, and the same page."""
    return _unmet + _value_unmet


def writes_postgres() -> bool:
    """Postgres alone derives the warehouse. Reads the cache, never the env."""
    return _mode == POSTGRES


def verdict_reached() -> bool:
    """`configure_mode` has decided this process's mode.

    False before it has run, and while it runs. `writes_postgres()` is False
    then, and means "not decided yet", never "decided against". A chain that
    remembers its first unmet answer (chain 3's start hold) must not remember
    that one: it would hold the chain for the life of the process on the very
    start that found step 13 in force. Until chain 7b-4 the start itself asked
    that question — the facts it gathered included the DN-12 goal bridge's
    owners, read by asking every write chain over a bridge table for its mode
    (the batch-E review). The bridge is gone and the start no longer asks; any
    consumer that does before the verdict still gets "not decided yet"."""
    return _mode is not None


def duckdb_derives() -> bool:
    """DuckDB derives its own Silver and Gold in this process — the predicate
    every production call of `refresh_warehouse_layers` stands behind, and
    `mark_warehouse_dirty` and the `warehouse_refresh` job with it.
    `tests/unit/test_warehouse_writer.py` walks for a call that does not."""
    return not writes_postgres()


def warehouse_checks_stand_down() -> bool:
    """The DuckDB checks over Silver, Gold and UTM — and the mirror-landing
    comparisons of those tables — do not run: Postgres alone derives, or the
    way back is still owed its first validated full tick (`held()`)."""
    return writes_postgres() or _held


# ─── What stands down ────────────────────────────────────────────────────────
#
# The names `check_internal_integrity` guards each check under. Each reads a
# DuckDB table the switch freezes — `silver_orders`, `gold_daily_revenue`,
# `silver_order_utm` — and each has its replacement on the Postgres side: the
# arc, attribution and line-item twins (`core/pg_warehouse_dq.py`), and for the
# Gold recompute, which a same-engine rerun would only check against itself,
# ClickHouse's `compare_gold_cells` and `pg_gold_internal_check`.
#
# Not here: the checks over landing (`orders`, `order_products`), which the
# switch does not freeze — chains 3, 4 and 6 move those. And not the three
# mirror-landing comparisons that set DuckDB's Silver and Gold against
# Postgres', which DN-29 retires in the job itself.
#
# Stood down is not raised: the scan does not report these through
# `raised_out`, so on the first run under the stand-down a page one of them
# delivered would be announced "✅ Resolved" with no check looking at it. The
# switch therefore waits for there to be none — `retired_conditions_clear`
# below — rather than holding them: nothing will ever re-examine a retired
# check's condition, so a held page would stand open for as long as Postgres
# derives. The twins open their own series under their `pg_` names.
STOOD_DOWN_WHEN_POSTGRES: FrozenSet[str] = frozenset({
    "silver_arc",
    "attribution_coverage",
    "gold_cell_values",
    "headline_vs_line_items",
    "goods_shipped_without_sale",
})


def stood_down_duckdb_checks() -> FrozenSet[str]:
    """The DuckDB integrity checks that do not run, and that the twins do not
    compare against, in this process. Empty unless Postgres alone derives, or
    the way back has not yet validated a full DuckDB tick — and, since chain
    3, the checks over the order tables once the orders are written to
    Postgres (`pg_orders_write.landing_checks_stood_down`). One answer for
    both, because the integrity job reads it once for the scan and for
    `duckdb_looked`. Step 13's own `retired_conditions` stays its five: the
    landing checks retire on chain 3's flip, which waits on its own
    `landing_pages_clear`."""
    from core import pg_orders_write

    warehouse = STOOD_DOWN_WHEN_POSTGRES if warehouse_checks_stand_down() else frozenset()
    return warehouse | pg_orders_write.landing_checks_stood_down()


# ─── What the stood-down checks leave behind ─────────────────────────────────
#
# `_resolve_dq_layer` holds a condition only when a check in the run raised or
# said it was not watching; a stood-down check did neither, so every page it
# delivered resolves on the first run after the switch — a recovery nobody
# verified (the silent-green attack on PR 13, DUCKDB_EXIT_CHAIN2_DESIGN.md).
# Two answers were open. Holding those conditions, as for a raised check, is
# the worse one: nothing re-examines a retired check again, so the page would
# stand open for as long as Postgres derives — in the digest's tail every
# morning, escalated once — and could only resolve on a rollback. The switch
# waits instead, while the DuckDB check still runs and can clear it, so the
# last verdict a retired check gives is a verified one.
#
# Read from the Alert Gate's delivered map, not from `app.alert_series`: the
# map is what `resolve_group` takes from and announces, so it is exact. The
# ledger is a copy that can disagree both ways — its fired row is written
# fire-and-forget, so a delivered page can be missing from it, which is the
# case that matters; and a gate re-armed by `reset_gate` leaves a series
# firing there that no resolve can close until it fires again — a clean check
# never does — which would hold the switch for good over a page the stand-down
# cannot announce.


# The conditions `compare_gold` alone reports, which DN-29 retires with
# `reconcile_gold`: on `dq:mirror_landing` a page of theirs would be announced
# resolved by the first run after the switch exactly as a stood-down integrity
# check's would. `gold_cell_values` is also the integrity check's name.
# `tests/unit/test_warehouse_writer.py` reads the set out of `compare_gold`'s
# source. `gold_rollup_mismatch` is not here: `pg_gold_internal_check` goes on
# asking it.
RETIRED_COMPARISON_CONDITIONS: FrozenSet[str] = frozenset({
    "gold_missing_cells",
    "gold_orphan_cells",
    "gold_cell_values",
})

# The comparisons in `dq_mirror_landing` that DN-29 retires, by the name the
# job runs each under.
RETIRED_COMPARISONS: Tuple[str, ...] = (
    "reconcile_silver", "reconcile_order_utm", "reconcile_gold")

# Their other conditions are not retired by name, and cannot be: `reconcile_
# silver` and `reconcile_order_utm` report under the `mirror_*` names every
# comparison in the job shares (`mirror_row_values`, `mirror_buckets_disagree`,
# `mirror_missing_rows`, ...), and the Gate keys a page by its condition and
# its group alone — `mirror_row_values` on `silver.orders` and on
# `bronze.orders` is one page. Retiring the names would hold the switch over
# every page a comparison that stays up delivers, and — worse — after a flip,
# since those comparisons go on delivering, make a restart run as duckdb over
# a page about `bronze.orders`: the way back, over nothing it concerns.
#
# So the job says whose page it is. After every run in which the retired
# comparisons ran, it marks (`COMPARISON_MARK`) each delivered page of its
# group they were still reporting, and takes the mark off the rest when all
# three reached a verdict (`note_comparisons_reported`); a marked page counts
# as retired. Only a run that compared can mark, so nothing is marked after a
# flip, and a page a stood-down run delivers is never marked. A page
# delivered before this was built carries no mark until the next run that
# compares — daily; every build that carried this ran that comparison while
# `od10_doors` still held the switch, until OD-10 was answered on 2026-09-30.
COMPARISON_MARK = "warehouse_comparison"


def note_comparisons_reported(reported, *, complete: bool) -> None:
    """After a `dq_mirror_landing` run in which the retired comparisons ran:
    `reported` are the CRITICAL conditions they found, `complete` whether all
    three reached a verdict. Marks those pages of the job's group as theirs,
    and — when complete — unmarks the rest. Local state, no I/O."""
    from core.alerting import mark_delivered
    from core.mirror_reconciliation import MIRROR_LAYER

    mark_delivered(f"dq:{MIRROR_LAYER}", COMPARISON_MARK, sorted(reported),
                   complete=complete)


def retired_conditions() -> FrozenSet[str]:
    """The conditions the stood-down checks report, from the table the
    integrity job holds a raised check's conditions by — one source, so a
    condition added to a stood-down check is covered here too — and those
    only the retired Gold comparison reports."""
    from core.data_quality import GUARDED_CHECK_CONDITIONS

    return frozenset(condition for guard in STOOD_DOWN_WHEN_POSTGRES
                     for condition in GUARDED_CHECK_CONDITIONS[guard]) \
        | RETIRED_COMPARISON_CONDITIONS


def open_retired_conditions() -> Dict[str, Optional[str]]:
    """`{condition: group}` for every page under a retired condition that was
    delivered and not yet announced resolved, in this process's Alert Gate —
    the one `resolve_group` would announce from. Whatever group delivered it:
    DN-29 retires the mirror-landing comparisons too, and `gold_cell_values` is
    one of theirs. And every page the retired comparisons were still
    reporting under a name they share (`COMPARISON_MARK`). Local state, no
    I/O; raises only if the gate cannot be read, which `gather_facts` reports
    as unmet."""
    from core.alerting import delivered_conditions, delivered_marked

    retired = retired_conditions()
    return {**{key: group for key, group in delivered_conditions().items()
               if key in retired},
            **delivered_marked(COMPARISON_MARK)}


# ─── The doors OD-10 has not yet decided ─────────────────────────────────────
#
# Doors in `web/` that read DuckDB's Silver with no read switch and no
# stand-down. After the switch each would serve numbers as old as the flip,
# and after the first Sunday compaction none. OD-10 decided each, port or
# retire, and blocked step 13 until it had: the owner retired all seven on
# 2026-09-30 — buyers/stats, the two debug routes, purge-orders and the three
# detail cards (`tests/unit/test_od10_retired_doors.py` keeps them retired).
# The list is empty and `od10_doors` is met.
#
# It stays, because it is not a list anybody keeps:
# `tests/unit/test_warehouse_writer.py` walks `web/` for every function naming
# a table the switch freezes — `silver_orders`, the `silver_order_lines` view
# over it, `gold_daily_revenue`, `silver_order_utm`, an inventory view reading
# one of them (derived from `core.sql_dialect._INVENTORY_VIEWS`), or a dialect
# hole that could render to one, `{views}` included — without asking
# `duckdb_derives()`, and requires exactly these. A door added after OD-10
# joins the list and holds the switch until it is ported, retired or put
# behind the predicate. The readers in `core/` ride the `KS_READ_*` switches
# below, and the checks and comparisons stand down on
# `warehouse_checks_stand_down()`.
OD10_DOORS: Tuple[Tuple[str, str], ...] = ()


# ─── The preconditions ───────────────────────────────────────────────────────
#
# Every read switch whose answer comes out of Silver, Gold or the UTM verdicts,
# and must therefore be on Postgres before DuckDB's copies freeze: each of them
# would otherwise serve frozen numbers, and after the first Sunday compaction
# empty ones. `tests/unit/test_warehouse_cutover.py` reads every string in
# `core/`, `web/` and `bot/` — however it is declared, an inline `os.getenv`
# included — for a `KS_READ_*` or `KS_*_STORE` name, and fails on one that is
# neither here nor excluded there by name, with its reason: a read switch added
# later is decided rather than forgotten.
WAREHOUSE_READERS: Tuple[str, ...] = (
    "KS_READ_GOLD",
    "KS_READ_SILVER",
    "KS_READ_DASHBOARD",
    "KS_READ_REPORTS",
    "KS_READ_MARKETING",
    "KS_READ_TRAFFIC",
    "KS_READ_EXPENSES",
    "KS_READ_MARGIN",
    "KS_READ_GOALS",
    "KS_READ_INVENTORY",
    "KS_READ_WEEKLY",
    "KS_READ_FORECAST_INPUT",
    "KS_READ_SEARCH_INDEX",
    "KS_READ_PRODUCTS_INTEL",
    "KS_READ_LOOKUPS",      # promocodes are read from Silver
    "KS_READ_CHAT",
    "KS_READ_BUYER_SYNC",
    "KS_SMS_STORE",         # the audience reads Silver's order lines
)
# The one reader answered by ClickHouse, from its copy of Postgres Silver.
COHORTS = "KS_READ_COHORTS"
CH_URL = "KS_CH_URL"

PG_DSN = "KS_PG_DSN"
PG_DERIVE = "KS_PG_DERIVE"
PG_TWINS = "KS_DQ_PG_WAREHOUSE"
UTM_PARSE = "KS_UTM_PARSE"
READ_FALLBACK = "KS_READ_FALLBACK"
MIRROR_LANDING = "KS_MIRROR_LANDING"

# `(key, what is required)`, in the order they are reported.
PRECONDITIONS: Tuple[Tuple[str, str], ...] = (
    ("value_understood", f"{ENV} unset, {DUCKDB} or {POSTGRES}"),
    ("pg_derive_own", f"{PG_DERIVE}=own"),
    ("pg_twins_on", f"{PG_TWINS}=on"),
    ("utm_parse_postgres", f"{UTM_PARSE}=postgres"),
    ("read_fallback_off", f"{READ_FALLBACK}=off"),
    ("pg_dsn", f"{PG_DSN} set"),
    ("pg_revision", "Postgres at the revision this build requires"),
    ("expenses_backfilled", "meta.mirror_state.backfilled_at set for "
                            "bronze.expenses (the /expenses history gate)"),
    ("mirror_landing", f"{MIRROR_LANDING} on"),
    *((f"reader:{name}", f"{name}=postgres") for name in WAREHOUSE_READERS),
    ("cohorts_clickhouse", f"{COHORTS}=clickhouse"),
    ("ch_url", f"{CH_URL} set"),
    ("od10_doors", "every DuckDB-only door reading Silver ported or retired "
                   "(OD-10)"),
    ("retired_conditions_clear", "no delivered page open under a condition "
                                 "only a stood-down check re-examines"),
)

# How long the readiness waits for Postgres to say its revision. A status page
# reports; it does not hang on a database that has stopped answering.
REVISION_READ_TIMEOUT_S = 5.0

# The one read a Postgres switch cannot see. `_expenses_run`
# (`core/repositories/expenses.py`) sends the two statements over
# `{expenses}` to Postgres only once `pg_expenses_read.backfilled()` says
# `bronze.expenses` holds its history, whatever KS_READ_EXPENSES says — and a
# NULL `backfilled_at` is an answer, not a failure, so DuckDB's Silver serves
# the /expenses summary and profit analysis uncounted and unrefused even
# under KS_READ_FALLBACK=off. Production has it set; a fresh host, or a
# restore older than the backfill, would not. So the switch asks it too, of
# the same Postgres and inside the same bound as the revision.
EXPENSES_HISTORY = "bronze.expenses"
_EXPENSES_HISTORY_SQL = ("SELECT backfilled_at IS NOT NULL FROM meta.mirror_state "
                         "WHERE table_name = $1")

# How many times a start asks Postgres before its verdict, and how long it
# waits between asks — only after a read that failed, never after an answer.
# Worst case, `start_bound_s()`: 3 × 10 s + 7 s = 37 s, once per process.
REVISION_READ_ATTEMPTS = 3
REVISION_RETRY_DELAYS_S: Tuple[float, ...] = (2.0, 5.0)


@dataclass(frozen=True)
class Unmet:
    key: str
    detail: str


@dataclass(frozen=True)
class Facts:
    """What the environment cannot say. `revision` is what Postgres answered,
    None with `revision_error` when it could not be asked or did not answer.
    `open_retired` is None with `open_retired_error` when the Alert Gate could
    not be read. `od10_doors` is `OD10_DOORS`, handed in so the evaluator
    stays pure over what it is given. `expenses_backfilled` is whether
    `bronze.expenses` has its history (`EXPENSES_HISTORY`), None with
    `expenses_backfill_error` when it was not asked or not answered — asked
    only of a Postgres that said its revision. `expenses_history_row` tells
    the two ways it can be False apart: a `meta.mirror_state` row whose
    `backfilled_at` is NULL, or no row at all."""
    revision: Optional[str] = None
    revision_error: Optional[str] = None
    required_revision: Optional[str] = None
    expenses_backfilled: Optional[bool] = None
    expenses_backfill_error: Optional[str] = None
    expenses_history_row: bool = True
    open_retired: Optional[Mapping[str, Optional[str]]] = field(default_factory=dict)
    open_retired_error: Optional[str] = None
    od10_doors: Tuple[Tuple[str, str], ...] = ()


def _read(env: Mapping[str, str], name: str, default: str = "") -> str:
    return (env.get(name) or default).strip().lower()


def readers_not_on_postgres(env: Mapping[str, str]) -> Tuple[str, ...]:
    """The `WAREHOUSE_READERS` switches `env` does not set to postgres, in
    their order. Pure.

    The one reading of that list, shared: `evaluate_preconditions` files one
    `reader:<name>` per name here, and chain 6 holds its catalogue writes on
    DuckDB while any is left (`core.pg_catalogue_write.unmet_precondition`) —
    so the switch and the chain cannot come to disagree about which readers
    still read DuckDB. Unset is duckdb; anything that is not exactly postgres,
    a typo included, is not on postgres."""
    return tuple(name for name in WAREHOUSE_READERS
                 if (_read(env, name, "duckdb") or "duckdb") != "postgres")


def evaluate_preconditions(env: Mapping[str, str], facts: Facts) -> List[Unmet]:
    """Every precondition of the switch that `env` and `facts` leave unmet, in
    `PRECONDITIONS` order, each naming what it found. Empty is ready. Pure."""
    from core.pg_landing import mirror_on

    unmet: List[Unmet] = []

    def need(key: str, ok: bool, detail: str) -> None:
        if not ok:
            unmet.append(Unmet(key, detail))

    def equals(key: str, name: str, wanted: str, default: str) -> None:
        found = _read(env, name, default) or default
        need(key, found == wanted, f"{name} is {found!r}, needs {wanted!r}")

    value = _read(env, ENV, DUCKDB) or DUCKDB
    need("value_understood", value in _VALID,
         f"{ENV} is {value!r}, which this build does not understand: it runs as "
         f"{DUCKDB!r}, and after a flip that is the way back")
    equals("pg_derive_own", PG_DERIVE, "own", "piggyback")
    equals("pg_twins_on", PG_TWINS, "on", "off")
    equals("utm_parse_postgres", UTM_PARSE, "postgres", "duckdb")
    equals("read_fallback_off", READ_FALLBACK, "off", "duckdb")
    need("pg_dsn", bool((env.get(PG_DSN) or "").strip()), f"{PG_DSN} is not set")

    required = facts.required_revision
    if facts.revision is None:
        need("pg_revision", False,
             f"revision unknown: {facts.revision_error or 'not asked'}")
    else:
        need("pg_revision", required is not None and facts.revision == required,
             f"Postgres is at {facts.revision!r}, this build requires {required!r}")
        # Asked only of a Postgres that answered: one that did not is
        # `pg_revision`'s, and a start that could not reach it names that alone.
        if facts.expenses_backfilled is None:
            need("expenses_backfilled", False,
                 f"whether {EXPENSES_HISTORY} holds its history is unknown: "
                 f"{facts.expenses_backfill_error or 'not asked'}")
        else:
            found = (f"meta.mirror_state.backfilled_at is NULL for {EXPENSES_HISTORY}"
                     if facts.expenses_history_row else
                     f"meta.mirror_state has no row for {EXPENSES_HISTORY}: nothing "
                     "has shipped it to this Postgres, neither the landing mirror "
                     "nor a backfill")
            need("expenses_backfilled", facts.expenses_backfilled,
                 f"{found}. Until its backfill completes, the /expenses summary "
                 "and the profit analysis read DuckDB's Silver whatever "
                 "KS_READ_EXPENSES says — frozen after the switch, empty after "
                 "a compaction, and never counted as a fallback. POST "
                 "/api/mirror/backfill/expenses (it writes the row), or let the "
                 "hourly ids-diff finish it (it runs while "
                 f"{MIRROR_LANDING} is on).")

    need("mirror_landing", mirror_on(env.get(MIRROR_LANDING)),
         f"{MIRROR_LANDING} is {env.get(MIRROR_LANDING)!r}: the landing mirror "
         "is off, so Postgres would derive from a bronze nothing writes")
    for name in readers_not_on_postgres(env):
        equals(f"reader:{name}", name, "postgres", "duckdb")
    equals("cohorts_clickhouse", COHORTS, "clickhouse", "duckdb")
    need("ch_url", bool((env.get(CH_URL) or "").strip()),
         f"{CH_URL} is not set: ClickHouse is the only independent aggregation "
         "of Gold once DuckDB stops")

    need("od10_doors", not facts.od10_doors,
         f"{len(facts.od10_doors)} door(s) still read DuckDB's Silver with no "
         f"switch — {'; '.join(label for _where, label in facts.od10_doors)}. "
         "Port or retire each (OD-10): after the switch they answer from a "
         "Silver as old as the flip, and after a compaction from none.")

    if facts.open_retired is None:
        need("retired_conditions_clear", False,
             f"the Alert Gate could not be read: {facts.open_retired_error}")
    else:
        pages = ", ".join(f"{key} ({group or 'no group'})"
                          for key, group in sorted(facts.open_retired.items()))
        need("retired_conditions_clear", not facts.open_retired,
             f"page(s) open under a check the switch stands down — {pages}. "
             "The first run after it would announce them resolved with no check "
             "looking; let the DuckDB checks clear them first.")
    return unmet


async def _revision_on_its_own_connection(dsn: str) -> Optional[str]:
    """The Alembic revision, read on a connection opened for it and closed
    after — never the application's pool, which belongs to the loop that
    opened it (see `_gather_facts_blocking`). Strict: None only for a version
    table that is not there, and every other failure raises, so a connection
    lost in the middle of the SELECT is a read to ask again — never "the
    database was never migrated", which is an answer and is not asked again."""
    import asyncpg

    from core.pg import current_revision

    conn = await asyncpg.connect(dsn=dsn)
    try:
        return await current_revision(conn, strict=True)
    finally:
        await conn.close()


async def _expenses_backfilled(conn=None) -> Optional[bool]:
    """Whether `meta.mirror_state.backfilled_at` is set for `bronze.expenses`
    — the question `pg_expenses_read.backfilled()` asks before it lets a
    history read reach Postgres, which reads both False and None as "not
    yet". Kept apart here only so the precondition can say which: False is a
    row whose `backfilled_at` is NULL, None is no row at all. On `conn`, or
    on the application's pool. Raises what the read raises."""
    if conn is None:
        from core.pg import get_pool

        pool = await get_pool()
        async with pool.acquire() as acquired:
            return await _expenses_backfilled(acquired)
    answer = await conn.fetchval(_EXPENSES_HISTORY_SQL, EXPENSES_HISTORY)
    return None if answer is None else bool(answer)


async def _expenses_backfilled_on_its_own_connection(dsn: str) -> Optional[bool]:
    """`_expenses_backfilled` on a connection opened for it and closed after —
    `_revision_on_its_own_connection`'s reason: the start reads from a loop
    that is not the application's."""
    import asyncpg

    conn = await asyncpg.connect(dsn=dsn)
    try:
        return await _expenses_backfilled(conn)
    finally:
        await conn.close()


async def _read_expenses_history(env: Mapping[str, str], *, own_connection: bool,
                                 timeout: float) -> Dict[str, Any]:
    """`{"expenses_backfilled", "expenses_backfill_error", "unread"}`, plus
    `expenses_history_row` with an answer, by `_read_postgres`'s rules: a
    read that failed is unread and by its class alone, an answer is not.
    Never raises."""
    try:
        read = (_expenses_backfilled_on_its_own_connection(env[PG_DSN].strip())
                if own_connection else _expenses_backfilled())
        done = await asyncio.wait_for(read, timeout=timeout)
    except asyncio.TimeoutError:
        return {"expenses_backfilled": None,
                "expenses_backfill_error": f"Postgres did not answer in {timeout:g} s",
                "unread": True}
    except Exception as exc:  # noqa: BLE001 — reported as unmet, by class
        logger.error("cutover readiness: whether %s holds its history could not "
                     "be read: %s: %s", EXPENSES_HISTORY, type(exc).__name__, exc)
        return {"expenses_backfilled": None,
                "expenses_backfill_error": type(exc).__name__, "unread": True}
    return {"expenses_backfilled": bool(done), "expenses_backfill_error": None,
            "expenses_history_row": done is not None, "unread": False}


async def _read_postgres(env: Mapping[str, str], *,
                         own_connection: bool) -> Dict[str, Any]:
    """What `gather_facts` asks Postgres: `{"revision", "revision_error",
    "unread"}`, and — of a Postgres that said its revision — whether
    `bronze.expenses` holds its history (`expenses_backfilled`,
    `expenses_backfill_error`). `unread` is a read that failed — no answer in
    time, or an exception — as against an answer: a revision, a database
    never migrated, a backfill done or not, or no DSN to ask. Only an unread
    fact is worth asking again, and the two are asked again together. Never
    raises.

    One bound for both: the second read gets what the first left of
    `REVISION_READ_TIMEOUT_S`, and never less than a fifth of it, so the two
    stay inside the bound `_read_postgres_in_a_worker` holds them to.

    An exception is published by its class alone and logged whole at ERROR:
    a driver's text names the database user, the host and the port, and a
    status page — admin-only or not — is not where those go. `/api/health`'s
    rule, and the log is where a person goes next anyway."""
    if not (env.get(PG_DSN) or "").strip():
        return {"revision": None, "revision_error": f"not asked: {PG_DSN} is not set",
                "unread": False}
    loop = asyncio.get_running_loop()
    started = loop.time()
    try:
        from core.pg import current_revision

        read = (_revision_on_its_own_connection(env[PG_DSN].strip())
                if own_connection else current_revision(strict=True))
        revision = await asyncio.wait_for(read, timeout=REVISION_READ_TIMEOUT_S)
    except asyncio.TimeoutError:
        return {"revision": None,
                "revision_error": f"Postgres did not answer in {REVISION_READ_TIMEOUT_S:g} s",
                "unread": True}
    except Exception as exc:  # noqa: BLE001 — reported as unmet, by class
        logger.error("cutover readiness: the Postgres revision could not be "
                     "read: %s: %s", type(exc).__name__, exc)
        return {"revision": None, "revision_error": type(exc).__name__, "unread": True}
    if revision is None:
        return {"revision": None, "revision_error": "Postgres recorded no Alembic revision",
                "unread": False}
    left = REVISION_READ_TIMEOUT_S - (loop.time() - started)
    history = await _read_expenses_history(
        env, own_connection=own_connection,
        timeout=max(left, REVISION_READ_TIMEOUT_S / 5))
    return {"revision": revision, "revision_error": None, **history}


def _local_facts() -> Dict[str, Any]:
    """The facts this process holds itself — the Alert Gate's delivered map
    and the OD-10 doors. No I/O, so never behind the bound Postgres is read
    under: a start that could not reach Postgres must not also report these
    as unread, naming preconditions nobody failed. Never raises.

    The write-chain registry was one of them until chain 7b-4: step 13 asked
    which chain had moved a table the DN-12 goal bridge read out of DuckDB.
    The bridge is deleted, and with it the question."""
    open_retired: Optional[Mapping[str, Optional[str]]]
    open_retired_error = None
    try:
        open_retired = open_retired_conditions()
    except Exception as exc:  # noqa: BLE001 — reported as unmet, by class
        logger.error("cutover readiness: the Alert Gate could not be read: %s: %s",
                     type(exc).__name__, exc)
        open_retired, open_retired_error = None, type(exc).__name__

    return {"open_retired": open_retired, "open_retired_error": open_retired_error,
            "od10_doors": OD10_DOORS}


async def gather_facts(env: Optional[Mapping[str, str]] = None, *,
                       own_connection: bool = False) -> Facts:
    """The facts `evaluate_preconditions` needs from outside the environment.
    Never raises: a fact that cannot be read is reported as unmet, by why.

    `own_connection` reads the revision on a connection of its own instead of
    the pool: what `configure_mode` needs, from a loop that is not the
    application's. Asked once — the status page reports what it finds; only
    the start asks again (`_gather_facts_blocking`)."""
    env = os.environ if env is None else env
    from core.pg import REQUIRED_REVISION

    read = await _read_postgres(env, own_connection=own_connection)
    read.pop("unread")
    return Facts(required_revision=REQUIRED_REVISION, **read, **_local_facts())


def _read_postgres_in_a_worker(env: Mapping[str, str]) -> Dict[str, Any]:
    """`_read_postgres` from synchronous code, once, bounded. Never raises.

    `configure_modes()` is synchronous and web calls it inside its own event
    loop, before the boot sync, so the revision cannot be awaited there; and it
    must not be read through `core.pg.get_pool()` from another loop either — a
    pool opened on a loop that then ends is a pool nobody after it can use. So
    it is read in a worker thread, on a loop and a connection of its own,
    bounded by `REVISION_READ_TIMEOUT_S` inside and by twice that out here:
    `asyncio.run` waits at its end for the loop's resolver threads, and a
    name that does not resolve can keep one busy past the inner bound. A
    worker still stuck is left to finish on its own."""
    import concurrent.futures

    def run() -> Dict[str, Any]:
        return asyncio.run(_read_postgres(env, own_connection=True))

    pool = concurrent.futures.ThreadPoolExecutor(
        max_workers=1, thread_name_prefix="warehouse-cutover")
    try:
        return pool.submit(run).result(timeout=2 * REVISION_READ_TIMEOUT_S)
    except concurrent.futures.TimeoutError:
        logger.error("cutover preconditions: Postgres was not read within %g s",
                     2 * REVISION_READ_TIMEOUT_S)
        return {"revision": None, "unread": True,
                "revision_error": f"Postgres did not answer in "
                                  f"{2 * REVISION_READ_TIMEOUT_S:g} s"}
    except Exception as exc:  # noqa: BLE001 — _read_postgres promises not to; unmet, by class
        logger.error("cutover preconditions: Postgres could not be read: %s: %s",
                     type(exc).__name__, exc)
        return {"revision": None, "revision_error": type(exc).__name__, "unread": True}
    finally:
        pool.shutdown(wait=False)


def _gather_facts_blocking(env: Mapping[str, str]) -> Facts:
    """`gather_facts`, from synchronous code — `configure_mode`'s. Never raises.

    **A read that failed is asked again before the verdict** — up to
    `REVISION_READ_ATTEMPTS` times, `REVISION_RETRY_DELAYS_S` apart — and an
    answer never is. The verdict is kept for the life of the process, and
    after a flip a start that runs as `duckdb` IS the way back: a full DuckDB
    rebuild, a new `since` on the next start under `postgres`, and the
    `warehouse` group closed again. A Postgres slow to come up beside web
    (both started at once after a host reboot) must not cost that; a Postgres
    still unread after the last ask does, and says so. The local facts are
    read here, on the caller's thread, and never time out with Postgres.
    Bounded: at most `start_bound_s()` seconds, once per process, and only
    when `postgres` is asked for."""
    import time

    from core.pg import REQUIRED_REVISION

    local = _local_facts()
    read = _read_postgres_in_a_worker(env)
    for attempt in range(1, REVISION_READ_ATTEMPTS):
        if not read["unread"]:
            break
        delay = REVISION_RETRY_DELAYS_S[min(attempt - 1, len(REVISION_RETRY_DELAYS_S) - 1)]
        logger.warning("cutover preconditions: Postgres was not read (%s); asking "
                       "again in %g s, %d of %d",
                       read.get("revision_error") or read.get("expenses_backfill_error"),
                       delay, attempt + 1, REVISION_READ_ATTEMPTS)
        time.sleep(delay)
        read = _read_postgres_in_a_worker(env)
    read.pop("unread")
    return Facts(required_revision=REQUIRED_REVISION, **read, **local)


def start_bound_s() -> float:
    """The longest `_gather_facts_blocking` holds a start: every attempt at
    its outer bound, and every wait between them."""
    waits = [REVISION_RETRY_DELAYS_S[min(i, len(REVISION_RETRY_DELAYS_S) - 1)]
             for i in range(REVISION_READ_ATTEMPTS - 1)]
    return REVISION_READ_ATTEMPTS * 2 * REVISION_READ_TIMEOUT_S + sum(waits)


# ─── THE RECORDED WRITER ─────────────────────────────────────────────────────
#
# The mode is per process; the question "did Postgres alone derive since DuckDB
# last did?" outlives it. It is written into DuckDB's own `sync_metadata`,
# under `warehouse_writer`, as JSON: `{"writer": "postgres", "since": …,
# "resolved": …}` from the first start under `postgres`, and `{"writer":
# "duckdb", "since": …}` once the way back has validated a full DuckDB tick.
# Absent means DuckDB has always derived, and a start under `duckdb` that
# finds it absent writes nothing — the default path is unchanged.
#
# DuckDB and not Postgres: the way back must be readable by the process that
# needs it, from the file whose Silver it describes. `sync_metadata` survives
# the Sunday compaction (only the derived tables are dropped there), and the
# hourly copy carries the row to `app.sync_metadata` like every other key; it
# is never deleted, so it is not one of the transient keys that copy excludes.
#
# Why both halves are needed, in the order they were found:
#
# - **The resolve.** The `warehouse` alert group's only resolver is the DuckDB
#   tick (`refresh_warehouse_layers`), which never runs under `postgres`. A
#   page it delivered would stand open forever, so the first start under
#   `postgres` closes the group — without a validating tick, which is the price
#   the design names — and records that it did. A later start closes it again
#   only when a page of it is open: a way back that never validated has DuckDB
#   ticks that can page, and its record still says `postgres`, resolved. When
#   the ledger cannot record the resolution the gate keeps the keys, and the
#   process stays unsettled, so the derivation tick asks again.
# - **The way back.** On a rollback the warehouse_refresh job is registered
#   again and its first tick would peek the id list abandoned at the switch — a
#   list, not "full" — and run an incremental rebuild over a Silver as old as
#   the switch; its checksums compare Silver with Gold, which agree, so it
#   would validate. So a start under `duckdb` that finds `postgres` recorded
#   marks the warehouse dirty in full, before anything registers, and holds the
#   stood-down checks and the three mirror comparisons down until a full tick
#   validates and a UTM parse has finished (`note_refresh`). The tick that
#   completes both writes `duckdb` back.

WRITER_KEY = "warehouse_writer"
# Appended to the resolve the first start under `postgres` announces.
RESOLVE_NOTE = ("DuckDB derivation retired (KS_WRITE_WAREHOUSE=postgres): the "
                "warehouse group is closed without a validating tick")

# Serialises every read-modify-write of the record in this process: settling,
# and a door noting a Postgres-only re-parse (`note_utm_reparsed_in_postgres`).
_settle_lock: Optional[asyncio.Lock] = None

# In the record while `postgres` is: when a door last re-parsed every UTM
# verdict in Postgres alone — see THE UTM DOORS below.
UTM_REPARSED = "utm_reparsed_at"


def _record_lock() -> asyncio.Lock:
    global _settle_lock
    if _settle_lock is None:
        _settle_lock = asyncio.Lock()
    return _settle_lock


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def read_writer(store) -> Optional[Dict[str, Any]]:
    """The recorded writer, or None when nothing was ever recorded. A value
    that is not the JSON this module writes is read as `postgres` unresolved:
    the cautious answer costs one full DuckDB rebuild, the other would skip
    one that is owed."""
    async with store.connection() as conn:
        row = conn.execute("SELECT value FROM sync_metadata WHERE key = ?",
                           [WRITER_KEY]).fetchone()
    if not row or not isinstance(row[0], str) or not row[0]:
        return None
    try:
        record = json.loads(row[0])
        if isinstance(record, dict) and record.get("writer") in _VALID:
            return record
    except ValueError:
        pass
    logger.error("sync_metadata.%s holds %r, which this build did not write; "
                 "read as postgres", WRITER_KEY, row[0])
    return {"writer": POSTGRES, "resolved": False, "unreadable": True}


async def _write_writer(store, record: Dict[str, Any]) -> None:
    async with store.connection() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO sync_metadata (key, value, updated_at) "
            "VALUES (?, ?, CURRENT_TIMESTAMP)",
            [WRITER_KEY, json.dumps(record, sort_keys=True)])


async def _count_utm_rows(store) -> int:
    async with store.connection() as conn:
        return int(conn.execute("SELECT COUNT(*) FROM silver_order_utm").fetchone()[0])


# ─── THE UTM DOORS ───────────────────────────────────────────────────────────
#
# Four doors re-parse the UTM verdicts outside the tick — `POST
# /api/traffic/refresh`, `/traffic/reclassify`, the `manager_comment` backfill
# and `scripts/backfill_utm.py` (`core/pg_utm_parse.py`, THE ROUTER). Under
# `postgres` each parses in Postgres alone. DuckDB derives no verdict there,
# and it must not stand in front of the table /traffic reads: a DuckDB parse
# that raised used to leave the Postgres reclassify unrun, and after a Sunday
# compaction every door re-parsed every order in DuckDB first (~281 s).
#
# Two of them re-parse everything, which is the only way a rule change reaches
# an order whose `updated_at` has not moved. From then on DuckDB's verdicts
# are older than the rules, and no incremental parse will ever notice — so the
# record says when, and the way back empties DuckDB's `silver_order_utm`: the
# full tick it owes then re-parses every verdict under the rules in force,
# which is what the reclassify it skipped would have done.


async def note_utm_reparsed_in_postgres(store) -> None:
    """A door is about to re-parse every UTM verdict in Postgres alone: say so
    in the record, so the way back re-parses DuckDB's. Noted before the parse —
    one that then fails costs the way back a re-parse it did not need, the
    other order would cost it one it did. Nothing unless `postgres` is the
    mode. Raises what DuckDB raises; the doors log it and parse anyway."""
    global _writer_record
    if not writes_postgres():
        return
    async with _record_lock():
        record = await read_writer(store)
        if record is None or record.get("writer") != POSTGRES:
            # Not recorded yet — the settle that would have raised. The
            # record it would have written; its retry finds it unresolved.
            record = {"writer": POSTGRES, "since": _now_iso(), "resolved": False}
        record = {key: value for key, value in record.items() if key != "unreadable"}
        record[UTM_REPARSED] = _now_iso()
        await _write_writer(store, record)
        _writer_record = record


async def _forget_duckdb_utm(store) -> None:
    """Empty DuckDB's `silver_order_utm` on the way back, so the owed full tick
    re-parses every verdict. Only while DuckDB derives."""
    if not duckdb_derives():
        return
    async with store.connection() as conn:
        conn.execute("DELETE FROM silver_order_utm")


def _warehouse_pages_open() -> bool:
    """A page of the `warehouse` group is delivered and not yet resolved, in
    this process's Alert Gate — what `resolve_group` would announce from."""
    from core.alerting import delivered_conditions

    return any(group == "warehouse" for group in delivered_conditions().values())


async def _resolve_the_warehouse_group() -> bool:
    """Close the `warehouse` group; whether nothing of it is left delivered."""
    from core.alerting import resolve_group

    try:
        await resolve_group("warehouse", note=RESOLVE_NOTE)
    except Exception as exc:  # noqa: BLE001 — retried by the next settle
        logger.error("warehouse group resolve raised: %s: %s", type(exc).__name__, exc)
        return False
    return not _warehouse_pages_open()


async def settle_writer(store) -> Dict[str, Any]:
    """Record the writer and settle what a change of writer owes. Once per
    process, and before anything derives: web's boot sync calls it first, then
    the scheduler's start, then every tick of whichever derivation runs — the
    DuckDB refresh under `duckdb`, the Postgres one under `postgres` — a no-op
    once settled. Raises what DuckDB raises; a caller that cannot afford that
    catches it, and the next caller tries again. Under `postgres` a group the
    ledger would not let it close leaves the process unsettled, so the next
    caller tries that again too."""
    global _settled, _held, _full_validated, _reclassify_needed, _writer_record
    global _value_unmet, _held_since
    if _settled:
        return status()
    async with _record_lock():
        if _settled:
            return status()
        record = await read_writer(store)
        settled = True
        if writes_postgres():
            if record is None or record.get("writer") != POSTGRES:
                record = {"writer": POSTGRES, "since": _now_iso(), "resolved": False}
                await _write_writer(store, record)
                logger.warning(
                    "warehouse writer recorded as postgres: DuckDB's Silver, Gold "
                    "and UTM verdicts are frozen from now, and the id list pending "
                    "in warehouse_dirty is abandoned")
            # Once on the switch, and again whenever a page of the group is
            # open: nothing under postgres resolves it otherwise, and a way
            # back that never validated leaves the record saying `resolved`
            # over a page its own DuckDB ticks delivered.
            if not record.get("resolved") or _warehouse_pages_open():
                if await _resolve_the_warehouse_group():
                    record = {**record, "resolved": True, "resolved_at": _now_iso()}
                    await _write_writer(store, record)
                else:
                    settled = False
                    logger.warning("the warehouse group is not yet closed; the next "
                                   "derivation tick tries again")
        elif record is not None and record.get("writer") == POSTGRES:
            if _mode_error:
                # A value nobody meant as `duckdb` took the way back: the cost
                # of an unmet precondition, so its page — not the typo's WARN,
                # whose lever does not say a rollback already happened.
                _value_unmet = (Unmet(
                    "value_understood",
                    f"{ENV}={_value!r} is not a value this build understands, "
                    f"and {POSTGRES!r} was the recorded writer: this start took "
                    "the way back"),)
                logger.critical("%s; a full DuckDB rebuild is owed", _value_unmet[0].detail)
            await store.mark_warehouse_dirty(None)
            _held, _full_validated = True, False
            _held_since = _held_since or datetime.now(timezone.utc)
            # After the mark, so a DuckDB that refuses the DELETE has still
            # been told what it owes; settling again repeats both.
            reparsed = record.get(UTM_REPARSED)
            if reparsed:
                await _forget_duckdb_utm(store)
            _reclassify_needed = await _count_utm_rows(store) == 0
            logger.warning(
                "warehouse writer back to duckdb from postgres (since %s): a full "
                "DuckDB rebuild is owed and marked; the DuckDB checks over Silver, "
                "Gold and UTM stay down until a full tick validates and a UTM "
                "parse finishes%s%s",
                record.get("since"),
                f"; the UTM verdicts were re-parsed in Postgres alone at {reparsed}, "
                "so DuckDB's silver_order_utm is emptied and the owed full tick "
                "re-parses every one" if reparsed else "",
                "; silver_order_utm is empty — POST /api/traffic/reclassify if the "
                "tick's own parse does not refill it"
                if _reclassify_needed else "")
        _writer_record = record
        _settled = settled
    return status()


async def note_refresh(store, *, silver_mode: str, validation_passed: bool,
                       utm_parsed: bool) -> bool:
    """Called by `refresh_warehouse_layers` after every successful tick, with
    whether the tick's UTM parse finished. Ends the way back's hold, and
    writes `duckdb` back as the recorded writer, on the first tick that
    validated and parsed once a full tick has validated since the hold began:
    the full tick itself, or a later one when the full one's parse raised.
    Whether it did. Nothing at all unless held.

    Both halves, because the checks the hold keeps down read both: the arc,
    the Gold cells, headline and goods read Silver and Gold, which only a full
    rebuild answers for, and `attribution_coverage` and `reconcile_order_utm`
    read `silver_order_utm`, which a parse that raised can leave partial —
    between two write batches, or after the way back emptied it for a re-parse.
    The parse reads landing, not Silver, so it need not be the full tick's own;
    asking for a second full rebuild instead would rebuild Silver whole every
    two minutes for as long as the parser keeps failing."""
    global _held, _full_validated, _reclassify_needed, _writer_record, _held_since
    if not _held:
        return False
    if silver_mode == "full" and validation_passed and not _full_validated:
        _full_validated = True
        if not utm_parsed:
            logger.warning(
                "the way back's full DuckDB tick validated, but its UTM parse "
                "raised: the DuckDB checks over Silver, Gold and UTM stay down "
                "until a tick's parse finishes")
    if not (_full_validated and validation_passed and utm_parsed):
        return False
    record = {"writer": DUCKDB, "since": _now_iso()}
    await _write_writer(store, record)
    _writer_record, _held, _full_validated, _held_since = record, False, False, None
    _reclassify_needed = await _count_utm_rows(store) == 0
    logger.warning(
        "warehouse writer is duckdb again: a full DuckDB tick validated and the "
        "UTM verdicts parsed, and the DuckDB checks over Silver, Gold and UTM "
        "resume%s",
        "; silver_order_utm is still empty — POST /api/traffic/reclassify"
        if _reclassify_needed else "")
    return True


def held() -> bool:
    """The way back is owed its first validated full DuckDB tick, or the
    UTM parse that finishes after it (`note_refresh`)."""
    return _held


def held_for_s() -> Optional[int]:
    """How long the hold has stood, in whole seconds; None when not held.

    A way back ends in its first full tick — minutes. A hold that stands for
    hours is a DuckDB UTM parse raising tick after tick (the tick logs it at
    WARNING and still reports success), or a full tick whose parse raised with
    no tick after it: nothing dirty, so nothing runs. Either way the five
    DuckDB checks and the three comparisons stay down with nothing saying so,
    which is why `/api/health` publishes the age and the canary judges it."""
    if not _held or _held_since is None:
        return None
    return max(0, int((datetime.now(timezone.utc) - _held_since).total_seconds()))


def reclassify_needed() -> bool:
    """DuckDB's `silver_order_utm` was found empty on the way back."""
    return _reclassify_needed


def status() -> Dict[str, Any]:
    """The mode as this process read it, and what settling found. Local state,
    no I/O. `preconditions_unmet` is the verdict reached at start; the live
    list is `readiness()`'s `unmet`."""
    return {
        "variable": ENV,
        "value": value(),
        "mode": mode(),
        "error": mode_error(),
        "switch_built": SWITCH_BUILT,
        "preconditions_unmet": [{"key": u.key, "detail": u.detail}
                                for u in preconditions_unmet()],
        "writer": _writer_record,
        "held": _held,
        "held_since": _held_since.isoformat() if _held and _held_since else None,
        "reclassify_needed": _reclassify_needed,
        "stood_down_duckdb_checks": sorted(stood_down_duckdb_checks()),
        "way_back_refused": list(way_back_refused()),
    }


async def readiness(env: Optional[Mapping[str, str]] = None) -> Dict[str, Any]:
    """`status()` plus every unmet precondition. What `/api/warehouse/status`
    publishes under `cutover`. Never raises."""
    env = os.environ if env is None else env
    facts = await gather_facts(env)
    unmet = evaluate_preconditions(env, facts)
    return {
        **status(),
        "preconditions_met": not unmet,
        "unmet": [{"key": u.key, "detail": u.detail} for u in unmet],
        "preconditions": [key for key, _ in PRECONDITIONS],
    }


# ─── THE WAY BACK, REFUSED ───────────────────────────────────────────────────
#
# After a flip, a start that runs as `duckdb` IS the way back — whether an
# operator unset the variable, a typo took it, or a precondition was unmet at
# the start — and the full DuckDB rebuild it owes derives Silver from DuckDB's
# own landing. That is right only while DuckDB's landing is current. A write
# chain that owns a table the DuckDB derivation reads froze DuckDB's copy at
# its latch: under chain 5 every order of a manager reclassified since the
# latch would come back with the wrong `sales_type` (the CASE reads
# `managers.is_retail` and the intervals), under chain 3 every order since the
# latch would be missing, and the Silver and Gold comparisons the way back
# brings back would be set against that Silver. The copy-back's
# `warehouse_dirty='full'` mark covers the order of ways back only when the
# copy-back has already run, and an automatic way back runs no runbook.
#
# So the start refuses it: it stays `postgres` — DuckDB does not derive,
# Postgres goes on deriving from the tables the chains write — publishes the
# chains in `/api/health` under `warehouse_writer_mode.way_back_refused`, and
# the canary pages `warehouse_way_back_refused` (CRITICAL) with the lever:
# copy the chains back (`scripts/chain_copy_back.py`, chain 5 before chain 3),
# then take the way back. The latch outranks KS_WRITE_WAREHOUSE as it outranks
# a chain's own flag (OD-19 (a)).
#
# Postgres derives on its own signal only under KS_PG_DERIVE=own; under
# `piggyback` its rebuild rides the DuckDB tick, which a `postgres` start does
# not register, so a refused way back with `own` gone leaves NOTHING deriving
# — screens frozen at the start, and correct up to it. Refused all the same:
# the alternative is a DuckDB Silver with the wrong `sales_type` that a
# rollback of the read switches would serve. The log and the canary's message
# say which it is (the canary reads the `derivation` block, since an unset
# variable evaluates no precondition). Never on a deployment that has not flipped:
# both chains hold themselves on DuckDB until step 13 is in force (their
# `step13` precondition), so a latched one means a flip happened —
# `tests/unit/test_managers_chain.py` holds every chain declaring one of these
# tables to that.
#
# Asked of the local markers alone, like every routing answer, before the boot
# sync and with Postgres possibly down. An owner row whose marker was lost is
# the one copy this cannot see; the daily comparison files it
# (`chain_latch_disagrees`, CRITICAL).

# The landing tables DuckDB's derivation reads that a write chain can freeze:
# the orders it derives Silver from (chain 3, which moves both as a unit) and
# the classification `sales_type` is decided from (chain 5). Not the buyers
# (chain 4): DuckDB's Silver and Gold read none of `bronze.buyers`' columns,
# and chain 4 latches without step 13.
DUCKDB_DERIVATION_SOURCES: Tuple[str, ...] = (
    "bronze.orders",
    "bronze.order_products",
    "bronze.managers",
    "app.manager_classifications",
)


def _chains_holding_duckdbs_sources() -> Tuple[str, ...]:
    """Every registered write chain that is latched and owns a table in
    `DUCKDB_DERIVATION_SOURCES`, by name. Never raises, and reads no database.

    A registry that cannot be read refuses nothing, and says so: refusing on
    it would stop DuckDB deriving on a deployment that never flipped, and a
    process that cannot import the registry cannot sync orders either."""
    try:
        from core import chain_latch
        from core.write_chains import WRITE_CHAINS, chain_name

        sources = frozenset(DUCKDB_DERIVATION_SOURCES)
        return tuple(sorted(
            chain_name(chain) for chain in WRITE_CHAINS
            if sources & frozenset(getattr(chain, "CHAIN_TABLES", ()))
            and chain_latch.latched(chain_name(chain))))
    except Exception as exc:  # noqa: BLE001 — logged; refuses nothing
        logger.error("the write-chain registry could not be read, so a way back "
                     "from %s=postgres is not checked against it: %s: %s",
                     ENV, type(exc).__name__, exc)
        return ()


def _the_way_back(holding: Tuple[str, ...], why: str) -> str:
    """The mode a start that would run as `duckdb` runs: `postgres` while a
    latched chain holds what DuckDB derives from, `duckdb` otherwise. Records
    the refusal; logs it once per distinct set of chains."""
    global _way_back_refused
    if holding and holding != _way_back_refused:
        from core import pg_derivation

        logger.critical(
            "%s would run as duckdb, but the way back is refused: write chain(s) "
            "%s own tables DuckDB derives from (%s) and froze DuckDB's copy at "
            "their latch, so a DuckDB rebuild would derive from them. Staying "
            "postgres: DuckDB does not derive, and %s. Copy them back first "
            "(scripts/chain_copy_back.py, chain 5 before chain 3), then take "
            "the way back.", why, ", ".join(holding),
            ", ".join(DUCKDB_DERIVATION_SOURCES),
            "Postgres derives on its own signal" if pg_derivation.owns() else
            f"NOTHING derives: {pg_derivation.ENV} is not own, so the Postgres "
            "rebuild rides a DuckDB tick this process does not run")
    _way_back_refused = holding
    return POSTGRES if holding else DUCKDB


def way_back_refused() -> Tuple[str, ...]:
    """The latched write chains for which this start refused the way back
    and stayed `postgres`; empty when none did. Local state, no I/O."""
    return _way_back_refused
