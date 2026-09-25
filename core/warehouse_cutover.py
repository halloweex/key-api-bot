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
  intake.
- **`postgres` switches only when every precondition holds** —
  `evaluate_preconditions` over the environment and the facts it cannot
  hold, the Postgres revision among them. One unmet item and the process runs
  as `duckdb` and publishes the keys of what is unmet; the canary pages
  `warehouse_preconditions_unmet` (CRITICAL). OD-09 (b): never a raise. The
  verdict is reached once per process and kept, so web's startup and the
  scheduler cannot disagree about it — a revision read that timed out the
  second time must not hand a boot that ran under `postgres` a scheduler that
  runs under `duckdb`.
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
  stood-down checks down until a full tick validates — the Silver they would
  read is as old as the switch. When DuckDB's `silver_order_utm` is empty (a
  Sunday compaction ran in between) it also flags a reclassify as needed.
- **`evaluate_preconditions(env, facts)`** names every unmet precondition of
  the switch, one entry each, so the readiness a person reads is a list of
  things to do rather than a "no". Pure over what it is handed.
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
# What `settle_writer` found and did, per process — see THE RECORDED WRITER.
_settled = False
_held = False
_reclassify_needed = False
_writer_record: Optional[Dict[str, Any]] = None


def configure_mode() -> str:
    """Read `KS_WRITE_WAREHOUSE` once. Never raises. Called by
    `configure_modes()`; idempotent, and logs a cause once rather than on every
    call — web's startup and the scheduler both configure.

    Under `postgres` the preconditions are evaluated here, the Postgres
    revision included (`_gather_facts_blocking`), and only the first call of a
    process asks: the verdict is kept."""
    global _value, _mode, _mode_error, _unmet, _decided_for
    value = os.getenv(ENV, DUCKDB).strip().lower() or DUCKDB
    if value in _VALID:
        error = None
    else:
        error = (f"{ENV}={value!r} is not one of {_VALID}; "
                 f"running as {DUCKDB!r}")
    if error and error != _mode_error:
        logger.error(error)
    _value, _mode_error = value, error
    if value != POSTGRES:
        _mode, _unmet, _decided_for = DUCKDB, (), None
        return _mode
    if _decided_for == POSTGRES and _mode is not None:
        return _mode
    env = os.environ
    unmet = tuple(evaluate_preconditions(env, _gather_facts_blocking(env)))
    _unmet, _decided_for = unmet, POSTGRES
    if unmet:
        _mode = DUCKDB
        logger.critical(
            "%s=postgres, but %d precondition(s) of the switch are unmet — "
            "running as duckdb, DuckDB goes on deriving: %s", ENV, len(unmet),
            "; ".join(f"{u.key}: {u.detail}" for u in unmet))
    else:
        _mode = POSTGRES
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
    """What held `postgres` back at start; empty unless it was asked for."""
    return _unmet


def writes_postgres() -> bool:
    """Postgres alone derives the warehouse. Reads the cache, never the env."""
    return _mode == POSTGRES


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
    the way back has not yet validated a full DuckDB tick."""
    return STOOD_DOWN_WHEN_POSTGRES if warehouse_checks_stand_down() else frozenset()


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
# asking it. The Silver and UTM comparisons report under the `mirror_*` names
# every comparison in that job shares, so a page of theirs cannot be told from
# one a comparison that stays up owns, and those names are not retired.
RETIRED_COMPARISON_CONDITIONS: FrozenSet[str] = frozenset({
    "gold_missing_cells",
    "gold_orphan_cells",
    "gold_cell_values",
})


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
    one of theirs. Local state, no I/O; raises only if the gate cannot be
    read, which `gather_facts` reports as unmet."""
    from core.alerting import delivered_conditions

    retired = retired_conditions()
    return {key: group for key, group in delivered_conditions().items()
            if key in retired}


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
    ("pg_derive_own", f"{PG_DERIVE}=own"),
    ("pg_twins_on", f"{PG_TWINS}=on"),
    ("utm_parse_postgres", f"{UTM_PARSE}=postgres"),
    ("read_fallback_off", f"{READ_FALLBACK}=off"),
    ("pg_dsn", f"{PG_DSN} set"),
    ("pg_revision", "Postgres at the revision this build requires"),
    ("mirror_landing", f"{MIRROR_LANDING} on"),
    *((f"reader:{name}", f"{name}=postgres") for name in WAREHOUSE_READERS),
    ("cohorts_clickhouse", f"{COHORTS}=clickhouse"),
    ("ch_url", f"{CH_URL} set"),
    ("goals_bridge", "no write chain owns a table the goal calculators' "
                     "bridge reads from DuckDB (DN-12)"),
    ("retired_conditions_clear", "no delivered page open under a condition "
                                 "only a stood-down check re-examines"),
)

# How long the readiness waits for Postgres to say its revision. A status page
# reports; it does not hang on a database that has stopped answering.
REVISION_READ_TIMEOUT_S = 5.0


@dataclass(frozen=True)
class Unmet:
    key: str
    detail: str


@dataclass(frozen=True)
class Facts:
    """What the environment cannot say. `revision` is what Postgres answered,
    None with `revision_error` when it could not be asked or did not answer.
    `bridge_owners` is None with `bridge_error` when the registry could not be
    read; `open_retired` likewise with `open_retired_error` when the Alert
    Gate could not."""
    revision: Optional[str] = None
    revision_error: Optional[str] = None
    required_revision: Optional[str] = None
    bridge_owners: Optional[Mapping[str, Tuple[str, ...]]] = field(default_factory=dict)
    bridge_error: Optional[str] = None
    open_retired: Optional[Mapping[str, Optional[str]]] = field(default_factory=dict)
    open_retired_error: Optional[str] = None


def _read(env: Mapping[str, str], name: str, default: str = "") -> str:
    return (env.get(name) or default).strip().lower()


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

    need("mirror_landing", mirror_on(env.get(MIRROR_LANDING)),
         f"{MIRROR_LANDING} is {env.get(MIRROR_LANDING)!r}: the landing mirror "
         "is off, so Postgres would derive from a bronze nothing writes")
    for name in WAREHOUSE_READERS:
        equals(f"reader:{name}", name, "postgres", "duckdb")
    equals("cohorts_clickhouse", COHORTS, "clickhouse", "duckdb")
    need("ch_url", bool((env.get(CH_URL) or "").strip()),
         f"{CH_URL} is not set: ClickHouse is the only independent aggregation "
         "of Gold once DuckDB stops")

    if facts.bridge_owners is None:
        need("goals_bridge", False,
             f"the write-chain registry could not be read: {facts.bridge_error}")
    else:
        owned = "; ".join(f"{chain}: {', '.join(tables)}"
                          for chain, tables in sorted(facts.bridge_owners.items()))
        need("goals_bridge", not facts.bridge_owners,
             f"write chain(s) own tables the goal calculators still read from "
             f"DuckDB — {owned}. Port chain 7b first.")

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
    opened it (see `_gather_facts_blocking`)."""
    import asyncpg

    from core.pg import current_revision

    conn = await asyncpg.connect(dsn=dsn)
    try:
        return await current_revision(conn)
    finally:
        await conn.close()


async def gather_facts(env: Optional[Mapping[str, str]] = None, *,
                       own_connection: bool = False) -> Facts:
    """The facts `evaluate_preconditions` needs from outside the environment.
    Never raises: a fact that cannot be read is reported as unmet, by why.

    An exception is published by its class alone and logged whole at ERROR:
    a driver's text names the database user, the host and the port, and a
    status page — admin-only or not — is not where those go. `/api/health`'s
    rule, and the log is where a person goes next anyway.

    `own_connection` reads the revision on a connection of its own instead of
    the pool: what `configure_mode` needs, from a loop that is not the
    application's."""
    env = os.environ if env is None else env
    from core.pg import REQUIRED_REVISION

    revision, revision_error = None, None
    if not (env.get(PG_DSN) or "").strip():
        revision_error = f"not asked: {PG_DSN} is not set"
    else:
        try:
            from core.pg import current_revision

            read = (_revision_on_its_own_connection(env[PG_DSN].strip())
                    if own_connection else current_revision())
            revision = await asyncio.wait_for(read, timeout=REVISION_READ_TIMEOUT_S)
            if revision is None:
                revision_error = "Postgres recorded no Alembic revision"
        except asyncio.TimeoutError:
            revision_error = f"Postgres did not answer in {REVISION_READ_TIMEOUT_S:g} s"
        except Exception as exc:  # noqa: BLE001 — reported as unmet, by class
            logger.error("cutover readiness: the Postgres revision could not be "
                         "read: %s: %s", type(exc).__name__, exc)
            revision_error = type(exc).__name__

    owners: Optional[Mapping[str, Tuple[str, ...]]]
    bridge_error = None
    try:
        from core.repositories.goals import sales_type_bridge_owners

        owners = sales_type_bridge_owners()
    except Exception as exc:  # noqa: BLE001 — reported as unmet, by class
        logger.error("cutover readiness: the write-chain registry could not be "
                     "read: %s: %s", type(exc).__name__, exc)
        owners, bridge_error = None, type(exc).__name__

    open_retired: Optional[Mapping[str, Optional[str]]]
    open_retired_error = None
    try:
        open_retired = open_retired_conditions()
    except Exception as exc:  # noqa: BLE001 — reported as unmet, by class
        logger.error("cutover readiness: the Alert Gate could not be read: %s: %s",
                     type(exc).__name__, exc)
        open_retired, open_retired_error = None, type(exc).__name__

    return Facts(revision=revision, revision_error=revision_error,
                 required_revision=REQUIRED_REVISION,
                 bridge_owners=owners, bridge_error=bridge_error,
                 open_retired=open_retired, open_retired_error=open_retired_error)


def _gather_facts_blocking(env: Mapping[str, str]) -> Facts:
    """`gather_facts`, from synchronous code — `configure_mode`'s. Never raises.

    `configure_modes()` is synchronous and web calls it inside its own event
    loop, before the boot sync, so the revision cannot be awaited there; and it
    must not be read through `core.pg.get_pool()` from another loop either — a
    pool opened on a loop that then ends is a pool nobody after it can use. So
    it is read in a worker thread, on a loop and a connection of its own,
    bounded by `REVISION_READ_TIMEOUT_S` inside and by twice that out here:
    `asyncio.run` waits at its end for the loop's resolver threads, and a
    name that does not resolve can keep one busy past the inner bound. The
    caller is never held longer, once per process, and only when `postgres`
    is asked for; a worker still stuck is left to finish on its own."""
    import concurrent.futures

    from core.pg import REQUIRED_REVISION

    def run() -> Facts:
        return asyncio.run(gather_facts(env, own_connection=True))

    pool = concurrent.futures.ThreadPoolExecutor(
        max_workers=1, thread_name_prefix="warehouse-cutover")
    try:
        return pool.submit(run).result(timeout=2 * REVISION_READ_TIMEOUT_S)
    except concurrent.futures.TimeoutError:
        logger.error("cutover preconditions were not gathered within %g s",
                     2 * REVISION_READ_TIMEOUT_S)
        return Facts(revision_error=f"Postgres did not answer in "
                                    f"{2 * REVISION_READ_TIMEOUT_S:g} s",
                     required_revision=REQUIRED_REVISION,
                     bridge_owners=None, bridge_error="not read",
                     open_retired=None, open_retired_error="not read")
    except Exception as exc:  # noqa: BLE001 — gather_facts promises not to; unmet, by class
        logger.error("cutover preconditions could not be gathered: %s: %s",
                     type(exc).__name__, exc)
        name = type(exc).__name__
        return Facts(revision_error=name, required_revision=REQUIRED_REVISION,
                     bridge_owners=None, bridge_error=name,
                     open_retired=None, open_retired_error=name)
    finally:
        pool.shutdown(wait=False)


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
#   validates. That tick writes `duckdb` back.

WRITER_KEY = "warehouse_writer"
# Appended to the resolve the first start under `postgres` announces.
RESOLVE_NOTE = ("DuckDB derivation retired (KS_WRITE_WAREHOUSE=postgres): the "
                "warehouse group is closed without a validating tick")

_settle_lock: Optional[asyncio.Lock] = None


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
    global _settled, _held, _reclassify_needed, _writer_record, _settle_lock
    if _settled:
        return status()
    if _settle_lock is None:
        _settle_lock = asyncio.Lock()
    async with _settle_lock:
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
            await store.mark_warehouse_dirty(None)
            _held = True
            _reclassify_needed = await _count_utm_rows(store) == 0
            logger.warning(
                "warehouse writer back to duckdb from postgres (since %s): a full "
                "DuckDB rebuild is owed and marked; the DuckDB checks over Silver, "
                "Gold and UTM stay down until a full tick validates%s",
                record.get("since"),
                "; silver_order_utm is empty — POST /api/traffic/reclassify"
                if _reclassify_needed else "")
        _writer_record = record
        _settled = settled
    return status()


async def note_refresh(store, *, silver_mode: str, validation_passed: bool) -> bool:
    """Called by `refresh_warehouse_layers` after every successful tick: a full
    one that validated ends the way back's hold, and writes `duckdb` back as
    the recorded writer. Whether it did. Nothing at all unless held."""
    global _held, _reclassify_needed, _writer_record
    if not _held or silver_mode != "full" or not validation_passed:
        return False
    record = {"writer": DUCKDB, "since": _now_iso()}
    await _write_writer(store, record)
    _writer_record, _held = record, False
    _reclassify_needed = await _count_utm_rows(store) == 0
    logger.warning(
        "warehouse writer is duckdb again: a full DuckDB tick validated, and the "
        "DuckDB checks over Silver, Gold and UTM resume%s",
        "; silver_order_utm is still empty — POST /api/traffic/reclassify"
        if _reclassify_needed else "")
    return True


def held() -> bool:
    """The way back is owed its first validated full DuckDB tick."""
    return _held


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
        "preconditions_unmet": [{"key": u.key, "detail": u.detail} for u in _unmet],
        "writer": _writer_record,
        "held": _held,
        "reclassify_needed": _reclassify_needed,
        "stood_down_duckdb_checks": sorted(stood_down_duckdb_checks()),
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
