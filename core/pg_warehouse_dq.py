"""Postgres twins of the Silver integrity checks — chain 2, step 8a.

The integrity scan checks Silver in DuckDB: the landing→Silver arc, website
attribution coverage, and the two line-item findings. When DuckDB stops deriving
(step 13) every one of those reads a frozen table: the arc pages four times a day
for rows that are fine, and attribution goes vacuously green. The replacements
must be proven equal on live data first, so they run beside the DuckDB checks,
behind `KS_DQ_PG_WAREHOUSE`, and are compared before anything stands alone.

The design was written and attacked before this was built (`.planning`, step 8
spec and two critics). What this module takes from it:

- **Names carry a `pg_` prefix.** The Gate's delivered map, `take_resolved`, the
  series ledger and the digest delta all key on the condition name alone. A
  twin sharing DuckDB's name would let one engine hold or announce the other's
  recovery, and a page could not say which engine it is about.
- **The grace is absolute**, on `bronze.orders.mirrored_at`. Relative to Silver's
  watermark, a derivation that stopped advancing would leave every new order "in
  flight" for ever — the check reporting zero exactly when Silver stopped being
  built.
- **Missing rows page only past the repair window.** Inside it (grace to
  heartbeat + floor + margin, 80 min) a lost mark or an owed rebuild is still on
  its way, so the finding is WARN. Past it nothing is coming, and it is CRITICAL.
- **Not dependent on KS_PG_DERIVE.** The grace is absolute, so the twins are
  sound under piggyback too, where they are the only pager for a lost in-memory
  owed rebuild.
- **Blindness is reported, never `[]`.** A group that could not be read files
  `pg_<group>_unwatched`; two or more, or the whole snapshot, file one
  `pg_warehouse_unwatched`. `data_quality.unverified_conditions` holds their
  conditions firing rather than announcing a recovery nobody verified.
- **One snapshot under PG_LAYER_LOCK, with a hold budget.** Every TRUNCATE of
  silver.orders and silver.order_utm happens under that lock, so a REPEATABLE
  READ snapshot alone could still catch an emptied Silver. The derivation and
  the DuckDB tick wait for this lock while holding the heavy lock the sync needs,
  so the hold is capped at HOLD_BUDGET_S whatever Postgres is doing.
- **Line items compare, not repeat.** While DuckDB's own headline and goods
  checks look, the twins do not file a second standing row each; they file
  `pg_line_items_disagree` when the engines differ by more than the orders still
  in flight. Only when DuckDB did not look does the twin stand in for it.

Step 8b adds the fourth group, **`pg_silver_row_values`**: every Silver row
recomputed from bronze and compared with what is stored. Silver is rebuilt
whole, so the arc cannot see a row that exists and kept its old values — a
status rewrite, a return that took revenue away — and neither can the
derivation's own validation, whose count and checksum net such changes out.
It stands alone: it compares Postgres with Postgres, not with DuckDB, because
it is the check that has to keep working once DuckDB stops deriving.

- **One recompute text for both engines.** `silver_recompute_ctes` renders pass
  1 and a SELECT form of pass 2; DuckDB's `silver_row_values` reads the same
  function, and the tolerances are its `_SILVER_ROW_COLUMNS`.
- **A row is judged by when its inputs last changed**, not by its own stamp
  alone: `bronze.orders.mirrored_at`; the classification's `mirrored_at` when
  `sales_type` is what moved; and, when a pass-2 column moved, the latest
  change to any order the buyer has or had — a first order cancelled today
  makes a months-old second order the buyer's first.
- **Covered is CRITICAL, whatever its age.** A change made before the last
  Silver rebuild began is in that rebuild's snapshot, so a row still wrong
  after it is a rebuild that does not produce what bronze holds. The rebuild
  is dated by Silver's own watermark as well as the journal, because under
  piggyback the watermark is the only record a rebuild leaves.
- **Not covered is the missing twin's windows**: in flight (not filed) inside
  the settle grace, WARN inside the repair window, CRITICAL past it — the
  absolute bound, so a derivation that stopped leaves rows paging rather than
  in flight for ever.
- **Last in the snapshot.** It reads the whole of Silver and bronze, so a hold
  budget it spends blinds it alone and the three cheap groups keep their
  verdicts.

Still deferred, with the owner's decisions they need: the positive pairing
record for the soak (DN-14) and making blindness news in every digest (OD-05).
"""
from __future__ import annotations

import asyncio
import logging
import math
import os
import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import Dict, FrozenSet, List, Optional, Sequence, Tuple, Union

logger = logging.getLogger(__name__)

ENV = "KS_DQ_PG_WAREHOUSE"
_VALID = ("off", "on")

# How long to wait for PG_LAYER_LOCK — a derivation or a ClickHouse ship holds
# it — and how long this may then hold it, pool acquire included.
#
# The row-values recompute (step 8b) shares this budget rather than taking a
# snapshot of its own, because it fits with room to spare: the whole snapshot,
# recompute included, took 0.26 s over 48,000 orders and 0.55 s over 96,000
# (medians of nine, postgres:17.2-alpine on a laptop; production held 47,756).
# Every run logs the recompute's own time beside this number.
LOCK_WAIT_S = 120
HOLD_BUDGET_S = 20
STATEMENT_TIMEOUT = "15s"

# The margin over the floor for "settled", and over heartbeat + floor for
# "nothing is coming". 20 and 80 minutes at the default 600 s floor.
GRACE_MARGIN_MIN = 10
# The orders mirror's age limit, as the canary judges it (bot/canary.py).
LANDING_MAX_AGE_S = 8 * 3600

WEBSITE_SOURCE_ID = 4


def flag() -> Tuple[bool, Optional[str]]:
    """`(enabled, error)`. An unknown value is off, with the error to report.

    Not a raise, `KS_PG_DERIVE`'s rule: this decides whether a check runs, and
    taking the integrity scan down over a typo would blind every other check.
    """
    value = os.getenv(ENV, "off").strip().lower() or "off"
    if value in _VALID:
        return value == "on", None
    return False, f"{ENV}={value!r} is not one of {_VALID}; treated as off"


def floor_seconds() -> int:
    return int(os.getenv("KS_PG_SILVER_INTERVAL_S", "600"))


def silver_grace_minutes() -> int:
    return math.ceil(floor_seconds() / 60) + GRACE_MARGIN_MIN


def page_after_minutes() -> int:
    from core.pg_derivation import HEARTBEAT

    return (math.ceil(HEARTBEAT.total_seconds() / 60)
            + math.ceil(floor_seconds() / 60) + GRACE_MARGIN_MIN)


# ─── Facts ────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Unwatched:
    reason: str


@dataclass(frozen=True)
class Watermark:
    today: date
    in_flight: int
    backfilled: bool
    landing_age_s: Optional[int]
    landing_failures: int


@dataclass(frozen=True)
class SilverArc:
    missing: int
    past_repair: int
    missing_amount: float
    oldest_age_s: Optional[int]
    missing_sample: Tuple[int, ...]
    orphans: int
    orphan_amount: float
    orphan_sample: Tuple[int, ...]


@dataclass(frozen=True)
class Attribution:
    orders: int
    tagged: int
    base_orders: int
    base_tagged: int


@dataclass(frozen=True)
class LineItems:
    headline: int
    headline_amount: float
    headline_sample: Tuple[int, ...]
    goods: int
    goods_amount: float
    goods_sample: Tuple[int, ...]


@dataclass(frozen=True)
class RowValues:
    """Silver rows whose stored values a recompute from bronze does not produce.

    `reported` is `covered` + `abandoned` + the rows inside the repair window;
    `in_flight` are the ones still inside the settle grace, counted apart and
    never filed. `compared` and `elapsed_ms` are the recompute's size and cost,
    kept so the hold budget can be judged against the real thing.
    """
    reported: int
    covered: int
    abandoned: int
    in_flight: int
    columns: Tuple[Tuple[str, int], ...]
    sample: Tuple[int, ...]
    oldest_age_s: Optional[int]
    rebuilt_at: Optional[datetime]
    compared: int
    elapsed_ms: int


Group = Union[SilverArc, Attribution, LineItems, RowValues, Unwatched, None]


@dataclass(frozen=True)
class Facts:
    whole: Optional[Unwatched]
    watermark: Optional[Watermark] = None
    silver_arc: Group = None
    attribution: Group = None
    line_items: Group = None
    silver_row_values: Group = None

    @classmethod
    def blind(cls, reason: str) -> "Facts":
        return cls(whole=Unwatched(reason))


# In reading order. The recompute is last on purpose: it is the one group heavy
# enough to spend the hold budget, and a budget spent on it should blind it
# alone rather than the cheap groups queued behind it.
GROUPS = ("silver_arc", "attribution", "line_items", "silver_row_values")

# Each guard and the conditions its twin emits — `data_quality.GUARDED_CHECK_CONDITIONS`'
# shape, for the same use: what a blind run must hold rather than resolve.
GUARD_CONDITIONS: Dict[str, Tuple[str, ...]] = {
    "silver_arc": ("pg_silver_missing_rows", "pg_silver_orphan_rows"),
    "attribution": ("pg_attribution_coverage_website",),
    "line_items": ("pg_headline_vs_line_items", "pg_goods_shipped_without_sale",
                   "pg_line_items_disagree"),
    "silver_row_values": ("pg_silver_row_values",),
}
UNWATCHED_NAMES: Dict[str, str] = {
    "silver_arc": "pg_silver_arc_unwatched",
    "attribution": "pg_attribution_coverage_unwatched",
    "line_items": "pg_line_items_unwatched",
    "silver_row_values": "pg_silver_row_values_unwatched",
}
WHOLE_UNWATCHED = "pg_warehouse_unwatched"
FLAG_INVALID = "pg_warehouse_dq_flag_invalid"


def all_conditions() -> FrozenSet[str]:
    return frozenset(c for cs in GUARD_CONDITIONS.values() for c in cs)


# ─── Reading ──────────────────────────────────────────────────────────────────

_WATERMARK_SQL = """
SELECT (now() AT TIME ZONE 'Europe/Kyiv')::date AS today,
       (SELECT count(*) FROM bronze.orders
         WHERE mirrored_at >= now() - make_interval(mins => $1)) AS in_flight,
       (SELECT backfilled_at IS NOT NULL FROM meta.mirror_state
         WHERE table_name = 'bronze.orders') AS backfilled,
       (SELECT EXTRACT(EPOCH FROM now() - last_ok_at)::int FROM meta.mirror_state
         WHERE table_name = 'bronze.orders') AS landing_age_s,
       (SELECT failures_since_ok FROM meta.mirror_state
         WHERE table_name = 'bronze.orders') AS landing_failures
"""

_MISSING_SQL = """
SELECT count(*) AS n,
       count(*) FILTER (WHERE o.mirrored_at < now() - make_interval(mins => $2)) AS past_repair,
       COALESCE(SUM(o.grand_total), 0) AS amount,
       EXTRACT(EPOCH FROM now() - MIN(o.mirrored_at))::int AS oldest_age_s,
       (array_agg(o.id ORDER BY o.grand_total DESC NULLS LAST, o.id DESC))[1:10] AS sample
FROM bronze.orders o
LEFT JOIN silver.orders s ON s.id = o.id
WHERE s.id IS NULL
  AND o.mirrored_at < now() - make_interval(mins => $1)
"""

_ORPHAN_SQL = """
SELECT count(*) AS n,
       COALESCE(SUM(s.grand_total), 0) AS amount,
       (array_agg(s.id ORDER BY s.grand_total DESC NULLS LAST, s.id DESC))[1:10] AS sample
FROM silver.orders s
LEFT JOIN bronze.orders o ON o.id = s.id
WHERE o.id IS NULL
"""

# The DuckDB check's windows, with today computed in Kyiv explicitly: Postgres'
# CURRENT_DATE is the UTC date, and the 01:00 Kyiv run always falls in the hours
# where the two differ.
_ATTRIBUTION_SQL = """
SELECT
  count(*) FILTER (WHERE s.order_date >= $1::date - $2::int),
  count(*) FILTER (WHERE s.order_date >= $1::date - $2::int AND u.utm_campaign IS NOT NULL),
  count(*) FILTER (WHERE s.order_date <  $1::date - $2::int),
  count(*) FILTER (WHERE s.order_date <  $1::date - $2::int AND u.utm_campaign IS NOT NULL)
FROM silver.orders s
LEFT JOIN silver.order_utm u ON u.order_id = s.id
WHERE s.source_id = $4 AND NOT s.is_return AND s.is_active_source
  AND s.order_date >= $1::date - $3::int AND s.order_date < $1::date
"""

_LINE_ITEMS_SQL = """
WITH li AS (
    SELECT order_id, SUM(price_sold * quantity) AS amount
    FROM bronze.order_products GROUP BY order_id
)
SELECT (s.sales_type = 'internal') AS internal,
       count(*) AS n,
       COALESCE(SUM(li.amount), 0) AS amount,
       (array_agg(s.id ORDER BY li.amount DESC, s.id DESC))[1:10] AS sample
FROM silver.orders s
JOIN li ON li.order_id = s.id
WHERE NOT s.is_return AND s.is_active_source
  AND s.grand_total = 0 AND li.amount > 0
GROUP BY 1
"""


def _row_values_sql() -> str:
    """Silver recomputed from bronze, compared row by row, judged in SQL.

    $1 the settle grace and $2 the repair window, in minutes; $3 the margin a
    change must clear to count as inside a rebuild's snapshot; $4 the
    derivation's layer; $5 Silver's row in `meta.mirror_state`. Rendered per
    call rather than at import: every import in this module is lazy, and this
    one would otherwise pull the DuckDB store in with it.

    **When Silver was last rebuilt** (`rebuilt_at`) is the later of two
    stamps. `rebuild_silver` writes Silver's watermark with `now()` inside its
    own transaction, so the stamp is that transaction's start: before pass 1
    takes its snapshot, and committed with the rows or not at all. That is the
    one record of a rebuild under **both** modes — `KS_PG_DERIVE=piggyback`,
    production's, rebuilds on DuckDB's tick and writes no journal row, and an
    own-mode run whose Gold or profile failed after Silver committed journals
    an error over a Silver that was rebuilt. The journal's last error-free
    `started_at` is the other stamp, the one the spec named; it precedes its
    own run's watermark, so where both exist the watermark decides.

    **When a row's inputs last changed** (`changed_at`) is the latest of:

    - its own `bronze.orders.mirrored_at` — every column but three reads only
      the order's own row;
    - the classification's `mirrored_at`, when `sales_type` is what moved: a
      reclassified manager changes the rows of orders nobody touched, and until
      the rebuild it owes, that is in flight rather than stale;
    - the buyer's latest change, when `is_new_customer` or
      `buyer_first_order_date` moved: pass 2 reads every order the buyer has.
      A buyer's clock runs over the orders bronze gives them *and* the orders
      Silver still gives them, so an order moved to another buyer is a recent
      change for the buyer it left — bronze alone no longer says it was theirs.
      (The moved order itself needs no such help: its own stamp is recent.)
    """
    from core.data_quality import _SILVER_ROW_COLUMNS, _silver_row_differs
    from core.duckdb_store import silver_recompute_ctes
    from core.pg_replication import CLASSIFICATIONS_TABLE, MANAGERS_TABLE
    from core.sql_dialect import POSTGRES

    columns = [c for c, _ in _SILVER_ROW_COLUMNS]
    flags = ",\n           ".join(
        f"({_silver_row_differs(c, t)}) AS d_{c}" for c, t in _SILVER_ROW_COLUMNS)
    any_differs = " OR ".join(f"c.d_{c}" for c in columns)
    per_column = ",\n       ".join(
        f"count(*) FILTER (WHERE reported AND d_{c}) AS n_{c}" for c in columns)
    orders, silver = POSTGRES.orders, POSTGRES.silver_orders
    return f"""
WITH {silver_recompute_ctes(POSTGRES)},
inputs AS (
    SELECT GREATEST((SELECT max(mirrored_at) FROM {MANAGERS_TABLE}),
                    (SELECT max(mirrored_at) FROM {CLASSIFICATIONS_TABLE})) AS classified_at,
           GREATEST((SELECT max(started_at) FROM meta.derivation_runs
                      WHERE layer = $4 AND error IS NULL),
                    (SELECT last_ok_at FROM meta.mirror_state
                      WHERE table_name = $5)) AS rebuilt_at
),
buyer_clock AS (
    SELECT buyer_id, max(mirrored_at) AS changed_at
    FROM (SELECT o.buyer_id, o.mirrored_at FROM {orders} o
           WHERE o.buyer_id IS NOT NULL
          UNION ALL
          SELECT s.buyer_id, o.mirrored_at FROM {silver} s
            JOIN {orders} o ON o.id = s.id
           WHERE s.buyer_id IS NOT NULL) b
    GROUP BY buyer_id
),
compared AS (
    SELECT r.id, o.mirrored_at, bc.changed_at AS buyer_changed_at,
           {flags}
    FROM recomputed r
    JOIN {silver} s ON s.id = r.id
    JOIN {orders} o ON o.id = r.id
    LEFT JOIN buyer_clock bc ON bc.buyer_id = r.buyer_id
),
stale AS (
    SELECT c.*,
           GREATEST(c.mirrored_at,
                    CASE WHEN c.d_sales_type THEN i.classified_at END,
                    CASE WHEN c.d_is_new_customer OR c.d_buyer_first_order_date
                         THEN c.buyer_changed_at END) AS changed_at,
           i.rebuilt_at
    FROM compared c CROSS JOIN inputs i
    WHERE {any_differs}
),
judged AS (
    SELECT st.*,
           (st.rebuilt_at IS NOT NULL
            AND st.changed_at < st.rebuilt_at - $3::interval) AS covered,
           st.changed_at < now() - make_interval(mins => $2) AS abandoned,
           st.changed_at < now() - make_interval(mins => $1) AS settled
    FROM stale st
),
marked AS (
    SELECT j.*, (j.covered OR j.settled) AS reported FROM judged j
)
SELECT (SELECT count(*) FROM compared) AS compared,
       (SELECT rebuilt_at FROM inputs) AS rebuilt_at,
       count(*) FILTER (WHERE reported) AS reported,
       count(*) FILTER (WHERE covered) AS covered,
       count(*) FILTER (WHERE abandoned AND NOT covered) AS abandoned,
       count(*) FILTER (WHERE NOT reported) AS in_flight,
       EXTRACT(EPOCH FROM now() - MIN(changed_at) FILTER (WHERE reported))::int AS oldest_age_s,
       (array_agg(id ORDER BY (covered OR abandoned) DESC, id DESC)
            FILTER (WHERE reported))[1:10] AS sample,
       {per_column}
FROM marked
"""


def _ids(value) -> Tuple[int, ...]:
    return tuple(int(v) for v in (value or ()))


async def _read_silver_arc(conn, grace: int, page_after: int) -> SilverArc:
    m = await conn.fetchrow(_MISSING_SQL, grace, page_after)
    o = await conn.fetchrow(_ORPHAN_SQL)
    return SilverArc(
        missing=int(m["n"]), past_repair=int(m["past_repair"]),
        missing_amount=float(m["amount"]), oldest_age_s=m["oldest_age_s"],
        missing_sample=_ids(m["sample"]),
        orphans=int(o["n"]), orphan_amount=float(o["amount"]),
        orphan_sample=_ids(o["sample"]),
    )


async def _read_attribution(conn, watermark: Watermark) -> Union[Attribution, Unwatched]:
    from core.data_quality import _attribution_coverage_check
    import inspect

    if not watermark.backfilled:
        return Unwatched("bronze.orders history is not backfilled (meta.mirror_state.backfilled_at)")
    defaults = inspect.signature(_attribution_coverage_check).parameters
    window = defaults["window_days"].default
    span = window + defaults["baseline_days"].default
    row = await conn.fetchrow(_ATTRIBUTION_SQL, watermark.today, window, span,
                              WEBSITE_SOURCE_ID)
    return Attribution(int(row[0]), int(row[1]), int(row[2]), int(row[3]))


async def _read_line_items(conn, watermark: Watermark) -> Union[LineItems, Unwatched]:
    if not watermark.backfilled:
        return Unwatched("bronze.orders history is not backfilled (meta.mirror_state.backfilled_at)")
    by = {bool(r["internal"]): r for r in await conn.fetch(_LINE_ITEMS_SQL)}

    def part(internal):
        r = by.get(internal)
        if r is None:
            return 0, 0.0, ()
        return int(r["n"]), float(r["amount"]), _ids(r["sample"])

    h, g = part(False), part(True)
    return LineItems(h[0], h[1], h[2], g[0], g[1], g[2])


async def _read_row_values(conn, grace: int, page_after: int) -> RowValues:
    """The recompute. Not gated on the backfill, unlike attribution and line
    items: Silver is derived from whatever bronze holds, history or not, so a
    partial bronze still has a Silver that must match it."""
    from core.data_quality import _SILVER_ROW_COLUMNS
    from core.pg_derivation import DROPPED_MARK_MARGIN, LAYER
    from core.pg_silver import SILVER_TABLE

    # The margin is DROPPED_MARK_MARGIN, for its two reasons: a writer outside
    # `_heavy_job_lock` commits a round trip after the instant its rows are
    # stamped with, and a journal's `started_at` comes from the web container's
    # clock where `mirrored_at` and the watermark come from Postgres'.
    started = time.monotonic()
    row = await conn.fetchrow(_row_values_sql(), grace, page_after,
                              DROPPED_MARK_MARGIN, LAYER, SILVER_TABLE)
    elapsed_ms = int((time.monotonic() - started) * 1000)
    columns = tuple(sorted(
        ((c, int(row[f"n_{c}"])) for c, _ in _SILVER_ROW_COLUMNS if row[f"n_{c}"]),
        key=lambda kv: (-kv[1], kv[0])))
    facts = RowValues(
        reported=int(row["reported"]), covered=int(row["covered"]),
        abandoned=int(row["abandoned"]), in_flight=int(row["in_flight"]),
        columns=columns, sample=_ids(row["sample"]),
        oldest_age_s=row["oldest_age_s"], rebuilt_at=row["rebuilt_at"],
        compared=int(row["compared"]), elapsed_ms=elapsed_ms)
    # INFO on every run: the cost is what decides whether this group can keep
    # sharing the snapshot's hold budget, and a number nobody logged is a
    # number nobody can compare with HOLD_BUDGET_S when Silver has grown.
    logger.info("Postgres Silver recomputed for the row-values twin: %d rows "
                "compared in %d ms (hold budget %d s)",
                facts.compared, elapsed_ms, HOLD_BUDGET_S)
    return facts


async def read_facts(*, today: Optional[date] = None, pool=None) -> Facts:
    """Every fact the twins judge, from one snapshot. Never raises: a failure is
    `Facts.blind(reason)` or an Unwatched group."""
    from core.mirror_reconciliation import configured
    from core.pg_silver import PG_LAYER_LOCK

    if not configured():
        return Facts.blind("KS_PG_DSN is not set")
    try:
        from core.pg import get_pool, require_revision

        pool = pool or await get_pool()
        await require_revision()
    except Exception as e:  # noqa: BLE001 — becomes the blindness reason
        return Facts.blind(f"{type(e).__name__}: {e}")

    try:
        grace, page_after = silver_grace_minutes(), page_after_minutes()
    except Exception as e:  # noqa: BLE001 — e.g. a KS_PG_SILVER_INTERVAL_S that is not a number
        return Facts.blind(f"the windows could not be computed: {type(e).__name__}: {e}")
    state: Dict[str, object] = {}
    groups: Dict[str, object] = {}

    # The wait and the hold are two limits, not one: waiting holds nothing, and
    # the hold is what stalls the sync behind the heavy lock.
    try:
        await asyncio.wait_for(PG_LAYER_LOCK.acquire(), timeout=LOCK_WAIT_S)
    except (asyncio.TimeoutError, TimeoutError):
        return Facts.blind(
            f"PG_LAYER_LOCK was not free within {LOCK_WAIT_S} s — a derivation "
            "or a ClickHouse ship held it")
    except Exception as e:  # noqa: BLE001 — never raises; CancelledError still propagates
        return Facts.blind(f"PG_LAYER_LOCK could not be taken: {type(e).__name__}: {e}")
    try:
        async with asyncio.timeout(HOLD_BUDGET_S):
            async with pool.acquire(timeout=HOLD_BUDGET_S) as conn:
                async with conn.transaction(isolation="repeatable_read", readonly=True):
                    await conn.execute(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")
                    # No JIT. The row-values recompute is costed past
                    # jit_above_cost, and compiling it never paid for itself
                    # (postgres:17.2-alpine, a fresh backend per run, measured
                    # for DN-13): +0.12 s on a 0.21 s query over 48,000 orders
                    # (cost ~147k), +0.10 s on 0.52 s over 96,000 (~447k). Past
                    # jit_optimize/inline_above_cost (500k, which 96,000 orders
                    # all but reach) it is +1.4 s and +0.9 s on the same two.
                    # Time spent holding PG_LAYER_LOCK, to speed up a query that
                    # runs four times a day.
                    await conn.execute("SET LOCAL jit = off")
                    w = await conn.fetchrow(_WATERMARK_SQL, grace)
                    watermark = Watermark(
                        today=today or w["today"], in_flight=int(w["in_flight"]),
                        backfilled=bool(w["backfilled"]),
                        landing_age_s=w["landing_age_s"],
                        landing_failures=int(w["landing_failures"] or 0))
                    state["watermark"] = watermark
                    readers = (
                        ("silver_arc", lambda c: _read_silver_arc(c, grace, page_after)),
                        ("attribution", lambda c: _read_attribution(c, watermark)),
                        ("line_items", lambda c: _read_line_items(c, watermark)),
                        ("silver_row_values",
                         lambda c: _read_row_values(c, grace, page_after)),
                    )
                    for name, reader in readers:
                        try:
                            async with conn.transaction():   # a savepoint per group
                                groups[name] = await reader(conn)
                        except (asyncio.CancelledError, TimeoutError):
                            raise
                        except Exception as e:  # noqa: BLE001 — this group only
                            logger.error("Postgres twin group %s unreadable: %s", name, e)
                            groups[name] = Unwatched(f"{type(e).__name__}: {e}")
    except (asyncio.TimeoutError, TimeoutError):
        if "watermark" not in state:
            return Facts.blind(f"the {HOLD_BUDGET_S} s hold budget was spent before the snapshot opened")
        for name in GROUPS:
            groups.setdefault(name, Unwatched(
                f"the {HOLD_BUDGET_S} s hold budget was spent before this group was read"))
    except Exception as e:  # noqa: BLE001
        if "watermark" not in state:
            return Facts.blind(f"{type(e).__name__}: {e}")
        for name in GROUPS:
            groups.setdefault(name, Unwatched(f"{type(e).__name__}: {e}"))
    finally:
        PG_LAYER_LOCK.release()

    return Facts(whole=None, watermark=state["watermark"], **{
        name: groups.get(name, Unwatched("not read")) for name in GROUPS})


# ─── Judging ──────────────────────────────────────────────────────────────────


def _issue(**kw):
    from core.data_quality import IntegrityIssue

    return IntegrityIssue(**kw)


def _unwatched_issue(group: str, reason: str):
    from core.data_quality import Severity

    held = ", ".join(GUARD_CONDITIONS[group])
    return _issue(
        check_name=UNWATCHED_NAMES[group], table_name="(postgres twins)",
        severity=Severity.WARN, count=1,
        description=(f"The Postgres twin '{group}' could not look this run: {reason}. "
                     f"{held} are held as they were, not cleared."))


def flag_invalid_issue(error: str):
    from core.data_quality import Severity

    return _issue(
        check_name=FLAG_INVALID, table_name="(postgres twins)",
        severity=Severity.WARN, count=1,
        description=f"{error}. The Postgres twins did not run; their conditions are held, not cleared.")


def _silver_arc_check(arc: SilverArc) -> list:
    from core.data_quality import Severity

    issues = []
    grace, page_after = silver_grace_minutes(), page_after_minutes()
    if arc.missing:
        oldest = "" if arc.oldest_age_s is None else f" (oldest {arc.oldest_age_s // 60} min)"
        if arc.past_repair:
            severity = Severity.CRITICAL
            tail = (f" {arc.past_repair} of them are older than {page_after} minutes — past "
                    "the heartbeat and the floor, so no rebuild is coming on its own. Every "
                    "number read from Postgres is short by that much.")
        else:
            severity = Severity.WARN
            tail = (f" Still inside the {page_after}-minute repair window: under own a lost "
                    "mark is rebuilt by the hourly heartbeat, under piggyback by the next "
                    "DuckDB dirty tick.")
        issues.append(_issue(
            check_name="pg_silver_missing_rows", table_name="silver.orders",
            severity=severity, count=arc.missing, sample_ids=arc.missing_sample,
            description=(f"{arc.missing} order(s) worth {arc.missing_amount:,.2f} are in "
                         f"bronze.orders with no silver.orders row {grace}+ minutes after they "
                         f"were mirrored{oldest}." + tail)))
    if arc.orphans:
        issues.append(_issue(
            check_name="pg_silver_orphan_rows", table_name="silver.orders",
            severity=Severity.WARN, count=arc.orphans, sample_ids=arc.orphan_sample,
            description=(f"{arc.orphans} silver.orders row(s) worth {arc.orphan_amount:,.2f} "
                         "have no order behind them in bronze.orders. They inflate every "
                         "number read from Postgres. No grace applies: Silver is only "
                         "written from landing.")))
    return issues


def _row_values_check(rv: RowValues) -> list:
    """One finding, CRITICAL when any row is covered or abandoned.

    Covered and abandoned are told apart in the words because their levers
    differ: an abandoned row is a derivation that stopped, and a rebuild fixes
    it; a covered row is a rebuild that ran and still did not produce what
    bronze holds, and another rebuild would only repeat it.
    """
    from core.data_quality import Severity

    if not rv.reported:
        return []
    grace, page_after = silver_grace_minutes(), page_after_minutes()
    critical = rv.covered + rv.abandoned
    waiting = rv.reported - critical
    columns = ", ".join(f"{c} ({n})" for c, n in rv.columns[:5]) or "none named"
    oldest = "" if rv.oldest_age_s is None else f" The oldest changed {rv.oldest_age_s // 60} min ago."
    parts = [f"{rv.reported} silver.orders row(s) hold values a recompute from "
             f"bronze.orders does not produce. Columns: {columns}.{oldest}"]
    if rv.covered:
        since = "?" if rv.rebuilt_at is None else rv.rebuilt_at.isoformat(timespec="seconds")
        parts.append(f"{rv.covered} changed before the last Silver rebuild began "
                     f"({since}), so its snapshot held them: the rebuild is not producing "
                     "what bronze holds, and another would repeat it.")
    if rv.abandoned:
        parts.append(f"{rv.abandoned} changed more than {page_after} minutes ago with no "
                     "Silver rebuild begun since — past the heartbeat and the floor, so "
                     "no rebuild is coming on its own.")
    if waiting:
        parts.append(f"{waiting} changed {grace}+ minutes ago and are inside the "
                     f"{page_after}-minute repair window: under own the next mark or the "
                     "hourly heartbeat rebuilds them, under piggyback the next DuckDB "
                     "dirty tick.")
    if rv.in_flight:
        parts.append(f"{rv.in_flight} more changed in the last {grace} minutes and are "
                     "in flight, not counted.")
    return [_issue(
        check_name="pg_silver_row_values", table_name="silver.orders",
        severity=Severity.CRITICAL if critical else Severity.WARN,
        count=rv.reported, sample_ids=rv.sample, description=" ".join(parts))]


def _attribution_check(att: Attribution, watermark: Watermark) -> list:
    """The DuckDB check's verdict over Postgres' counts — its thresholds read
    from its own signature, so the two cannot drift apart."""
    import inspect

    from core.data_quality import Severity, _attribution_coverage_check

    d = {k: p.default for k, p in inspect.signature(_attribution_coverage_check).parameters.items()}
    if att.orders < d["min_orders"]:
        landing_bad = (watermark.landing_failures > 0 or watermark.landing_age_s is None
                       or watermark.landing_age_s > LANDING_MAX_AGE_S)
        if landing_bad:
            age = "never" if watermark.landing_age_s is None else f"{watermark.landing_age_s // 3600} h ago"
            return [_unwatched_issue(
                "attribution",
                f"only {att.orders} website order(s) in {d['window_days']} days, and the orders "
                f"mirror last shipped {age} with {watermark.landing_failures} failure(s) since — "
                "a quiet week cannot be told from a mirror that stopped")]
        return []   # a quiet week is not an outage — the DuckDB rule

    pct = att.tagged / att.orders * 100.0
    base_pct = (att.base_tagged / att.base_orders * 100.0
                if att.base_orders >= d["min_orders"] else None)
    below = pct < d["floor_pct"]
    halved = base_pct is not None and base_pct > 0 and pct < base_pct * d["halved_against_baseline"]
    if not (below or halved):
        return []
    trend = "" if base_pct is None else f" Over the previous {d['baseline_days']} days it was {base_pct:.0f}%."
    return [_issue(
        check_name="pg_attribution_coverage_website", table_name="silver.order_utm",
        severity=Severity.WARN, count=att.orders - att.tagged,
        description=(f"In Postgres, {pct:.0f}% of website orders in the last "
                     f"{d['window_days']} days carry a campaign tag ({att.tagged} of "
                     f"{att.orders})." + trend + " Orders are arriving; their UTM parameters "
                     "are not."))]


def _line_items_check(li: LineItems, watermark: Watermark, *,
                      duckdb_issues: Sequence, duckdb_looked: FrozenSet[str]) -> list:
    from core.data_quality import Severity

    counts = {i.check_name: i.count for i in duckdb_issues}
    issues, differ = [], []

    def compare(duck_name: str, n: int) -> bool:
        """True when DuckDB looked, so the twin compares instead of standing in."""
        if duck_name not in duckdb_looked:
            return False
        duck = counts.get(duck_name, 0)
        if abs(n - duck) > watermark.in_flight:
            differ.append(f"{duck_name}: Postgres {n}, DuckDB {duck}")
        return True

    if not compare("headline_vs_line_items", li.headline) and li.headline:
        issues.append(_issue(
            check_name="pg_headline_vs_line_items", table_name="silver.orders",
            severity=Severity.WARN, count=li.headline, sample_ids=li.headline_sample,
            description=(f"In Postgres, {li.headline} order(s) have grand_total = 0 but "
                         f"{li.headline_amount:,.2f} in line items. Revenue figures read "
                         "grand_total and product/brand/category figures read line items, "
                         "so the two disagree by this amount.")))
    if not compare("goods_shipped_without_sale", li.goods) and li.goods:
        issues.append(_issue(
            check_name="pg_goods_shipped_without_sale", table_name="silver.orders",
            severity=Severity.INFO, count=li.goods, sample_ids=li.goods_sample,
            description=(f"In Postgres, {li.goods} internal order(s) shipped "
                         f"{li.goods_amount:,.2f} of goods with no sale attached — seeding "
                         "and gifts. Expected, and counted so it is a number rather than "
                         "a silence.")))
    if differ:
        issues.append(_issue(
            check_name="pg_line_items_disagree", table_name="silver.orders",
            severity=Severity.WARN, count=len(differ),
            description=("The engines count line-item findings differently by more than the "
                         f"{watermark.in_flight} order(s) still in flight: " + "; ".join(differ)
                         + ". One store's Silver or line items differ from the other's.")))
    return issues


def check_pg_warehouse(
    facts: Optional[Facts], *, duckdb_issues: Sequence = (),
    duckdb_looked: FrozenSet[str] = frozenset(),
    raised_out: Optional[List[str]] = None,
    held_out: Optional[List[str]] = None,
) -> list:
    """The twins over pre-read facts. Pure; never raises.

    `duckdb_looked` are the DuckDB guard names that ran this scan (empty when
    the DuckDB half failed). A twin whose raise is caught files its group's
    unwatched finding, and its guard name goes to `raised_out`. `held_out`
    receives the conditions of every group that could not look — only those:
    the collapsed finding is one line, but a group read clean in the same run
    still announces its recovery.
    """
    from core.data_quality import Severity

    def hold(group: str) -> None:
        if held_out is not None:
            held_out.extend(GUARD_CONDITIONS[group])

    if facts is None or facts.whole is not None:
        reason = "facts were not read" if facts is None else facts.whole.reason
        for group in GROUPS:
            hold(group)
        return [_issue(
            check_name=WHOLE_UNWATCHED, table_name="(postgres twins)",
            severity=Severity.WARN, count=len(GROUPS),
            description=(f"The Postgres twins could not look this run: {reason}. Every pg_* "
                         "condition is held, not cleared."))]

    blind = {g: getattr(facts, g) for g in GROUPS if isinstance(getattr(facts, g), Unwatched)}
    for group in blind:
        hold(group)
    issues = []
    if len(blind) >= 2:
        reasons = "; ".join(f"{g}: {u.reason}" for g, u in blind.items())
        issues.append(_issue(
            check_name=WHOLE_UNWATCHED, table_name="(postgres twins)",
            severity=Severity.WARN, count=len(blind),
            description=(f"The Postgres twins could not look at {', '.join(blind)} this run — "
                         f"{reasons}. Every pg_* condition is held, not cleared.")))
    elif blind:
        (g, u), = blind.items()
        issues.append(_unwatched_issue(g, u.reason))

    judges = {
        "silver_arc": lambda: _silver_arc_check(facts.silver_arc),
        "attribution": lambda: _attribution_check(facts.attribution, facts.watermark),
        "line_items": lambda: _line_items_check(
            facts.line_items, facts.watermark,
            duckdb_issues=duckdb_issues, duckdb_looked=duckdb_looked),
        "silver_row_values": lambda: _row_values_check(facts.silver_row_values),
    }
    for group, judge in judges.items():
        if group in blind:
            continue
        try:
            issues += judge()
        except Exception as exc:  # noqa: BLE001 — named, and its conditions held
            logger.error("Postgres twin %s raised: %s: %s", group, type(exc).__name__, exc)
            if raised_out is not None:
                raised_out.append(group)
            hold(group)
            issues.append(_unwatched_issue(group, f"the check raised {type(exc).__name__}: {exc}"))
    return issues


def unverified_pg_conditions(*, ran: bool, flag_invalid: bool, held: Sequence[str] = ()) -> List[str]:
    """What the twins' part of the run could not verify.

    - **Ran:** exactly the conditions of the groups that could not look (`held`,
      from `check_pg_warehouse(held_out=)`).
    - **Flag not understood:** everything — the twins were meant to run and did
      not, so nothing about them is known.
    - **Flag off:** nothing. Switched off on purpose is stood down, not blind: a
      page delivered while they were on resolves once rather than standing for
      as long as the flag stays off. Found reviewing #215.
    """
    if flag_invalid:
        return sorted(all_conditions())
    if not ran:
        return []
    return sorted(set(held))
