"""Step 13a (DN-28): the warehouse writer's mode, what it stands down, and
whether this deployment is ready for the switch. **Nothing here switches.**

Step 13 stops DuckDB deriving Silver, Gold and the UTM verdicts, and leaves
Postgres the only engine that computes them. The switch itself is DN-29. This
is everything the switch needs in place before it can be built, and nothing
that changes what production does:

- **`KS_WRITE_WAREHOUSE` is read once, in `core.runtime_modes.configure_modes()`**,
  before web's boot sync — a mode cached at scheduler start misses everything
  the boot does (DN-05b), and on an empty DuckDB the boot runs a full rebuild.
  `duckdb` is the default. An unknown value runs as `duckdb` and publishes the
  error — on `/api/health` under `warehouse_writer_mode`, where the canary
  warns `warehouse_mode_invalid` — `KS_PG_DERIVE`'s rule and OD-09's
  recommendation: web is the only syncer, so refusing to start over this
  variable would stop order intake.
  `postgres` is understood and published, and **still runs as `duckdb`**: the
  switch is not in this build (`SWITCH_BUILT`), and a value that stood the
  DuckDB checks down while DuckDB went on deriving would be half a switch.
- **`stood_down_duckdb_checks()`** names the DuckDB integrity checks that read
  Silver, Gold or UTM and so go wrong on a frozen table — the arc pages four
  times a day for rows that are fine, attribution and the Gold recompute go
  vacuously green. The integrity scan skips them and the Postgres twins stop
  comparing against them, so the twins file their own findings: the standalone
  path the DN-14 pairing record has been recording in shadow. Empty while the
  mode is `duckdb`, which in this build is always.
- **`evaluate_preconditions(env, facts)`** names every unmet precondition of
  the switch, one entry each, so the readiness a person reads is a list of
  things to do rather than a "no". Pure over what it is handed.
  `readiness()` gathers the facts and is what `GET /api/warehouse/status`
  publishes under `cutover`.

The preconditions are the plan's, named by the variable an operator sets and
the value it needs — `KS_UTM_PARSE` and the enforcement behind
`KS_READ_FALLBACK=off` are built on other branches, and this names them by
value rather than importing them. A precondition met is not a promise that the
code behind it is deployed; DN-29 depends on those branches being merged.
"""
from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, FrozenSet, List, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)

ENV = "KS_WRITE_WAREHOUSE"
DUCKDB = "duckdb"
POSTGRES = "postgres"
_VALID = (DUCKDB, POSTGRES)

# DN-29 builds the switch. Until then `postgres` is read, published and not
# acted on — `configure_mode` never answers it.
SWITCH_BUILT = False

_value: Optional[str] = None
_mode: Optional[str] = None
_mode_error: Optional[str] = None


def configure_mode() -> str:
    """Read `KS_WRITE_WAREHOUSE` once. Never raises. Called by
    `configure_modes()`; idempotent, and logs a cause once rather than on every
    call — web's startup and the scheduler both configure."""
    global _value, _mode, _mode_error
    value = os.getenv(ENV, DUCKDB).strip().lower() or DUCKDB
    if value in _VALID:
        error = None
    else:
        error = (f"{ENV}={value!r} is not one of {_VALID}; "
                 f"running as {DUCKDB!r}")
    if error and error != _mode_error:
        logger.error(error)
    if value == POSTGRES and _value != POSTGRES and not SWITCH_BUILT:
        logger.warning(
            "%s=postgres is read but this build does not carry the switch "
            "(DN-29): DuckDB goes on deriving and every DuckDB check stays up",
            ENV)
    _value, _mode_error = value, error
    # The one line DN-29 changes: it answers `postgres` here when the value
    # asks for it and `evaluate_preconditions` finds nothing unmet.
    _mode = DUCKDB
    return _mode


def mode() -> str:
    """The mode this process runs; `duckdb` until `configure_mode` has run."""
    return _mode or DUCKDB


def value() -> Optional[str]:
    """What the variable said, as read; None until `configure_mode` has run."""
    return _value


def mode_error() -> Optional[str]:
    return _mode_error


def writes_postgres() -> bool:
    """Postgres alone derives the warehouse. Reads the cache, never the env."""
    return _mode == POSTGRES


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
    compare against, in this process. Empty unless Postgres alone derives."""
    return STOOD_DOWN_WHEN_POSTGRES if writes_postgres() else frozenset()


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


def retired_conditions() -> FrozenSet[str]:
    """The conditions the stood-down checks report, from the table the
    integrity job holds a raised check's conditions by — one source, so a
    condition added to a stood-down check is covered here too."""
    from core.data_quality import GUARDED_CHECK_CONDITIONS

    return frozenset(condition for guard in STOOD_DOWN_WHEN_POSTGRES
                     for condition in GUARDED_CHECK_CONDITIONS[guard])


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


async def gather_facts(env: Optional[Mapping[str, str]] = None) -> Facts:
    """The facts `evaluate_preconditions` needs from outside the environment.
    Never raises: a fact that cannot be read is reported as unmet, by why.

    An exception is published by its class alone and logged whole at ERROR:
    a driver's text names the database user, the host and the port, and a
    status page — admin-only or not — is not where those go. `/api/health`'s
    rule, and the log is where a person goes next anyway."""
    env = os.environ if env is None else env
    from core.pg import REQUIRED_REVISION

    revision, revision_error = None, None
    if not (env.get(PG_DSN) or "").strip():
        revision_error = f"not asked: {PG_DSN} is not set"
    else:
        try:
            from core.pg import current_revision

            revision = await asyncio.wait_for(
                current_revision(), timeout=REVISION_READ_TIMEOUT_S)
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


def status() -> Dict[str, Any]:
    """The mode as this process read it. Local state, no I/O."""
    return {
        "variable": ENV,
        "value": value(),
        "mode": mode(),
        "error": mode_error(),
        "switch_built": SWITCH_BUILT,
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
