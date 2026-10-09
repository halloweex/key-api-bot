"""Chain 7b-3: the goal and forecast tables, written to Postgres.

Four tables a smart goal and the revenue forecast are built from:
`app.seasonal_indices`, `app.growth_metrics`, `app.weekly_patterns` and
`app.revenue_predictions` (revision 0025, 313 rows between them). Until this
chain DuckDB wrote all four and the hourly `replicate_operational` carried a
full replace of each into Postgres (`core/pg_operational.py`). Everything in
them is recomputable — the three goal tables from the order history, the
forecast from the model and the day it ran — which is why they moved after
chain 7a's `revenue_goals`, the one goal table nobody can recompute.

TWO WRITERS, ONE CHAIN

- `persist_goal_tables` — the three goal tables, in ONE transaction. Since 7b-1
  they have one writer, `GoalsMixin.recalculate_goal_tables`, reached by the
  Monday `seasonality_calc` job and `POST /api/goals/recalculate`; the routing
  lives one level down in `_persist_goal_tables`, where both arrive.
- `store_predictions` — the forecast, reached from `PredictionService
  .predict_month` after a training (Mon and Thu 03:30, the admin POST, and a
  start with no model artefact on disk).

Either writer's first Postgres write latches the chain and claims all four
owner rows: ownership passes as a unit, so the hourly copy and the daily
comparison stand down for the four together (`core.write_chains`).

BOTH SEASONAL WRITES IN ONE TRANSACTION, AND THE UPDATE MUST MATCH

The seasonal indices are an upsert that never touches `yoy_growth`, followed by
one `UPDATE ... SET yoy_growth` per month that has a pair of years. Split, or
reordered, that UPDATE matches no row and says nothing — and
`generate_smart_goals` then reads NULL and substitutes the dynamic cap with no
error. So both run in one transaction with the upsert first, and every UPDATE
must report `UPDATE 1` or the whole set rolls back. A month with a YoY pair
always has an index row (`_SILVER_MONTHLY_YOY_SQL` needs 25 days of a month in
two years, `_SILVER_SEASONALITY_SQL` 20 in one), so the guard never fires on
correct code and fires on any split. `executemany` returns no per-row status
(measured on asyncpg), which is why the UPDATEs are a loop.

WHAT POSTGRES WOULD REFUSE IS REFUSED BEFORE THE LATCH

The latch is permanent. A value Postgres rejects raises inside the write, after
the marker is on disk — latched for good with no owner row behind it — for an
input DuckDB would have refused without a trace. MAPE is not bounded anywhere
(sklearn's MAPE explodes on a zero-revenue validation day), so an overflow is a
realistic input. And the two engines disagree on one value: DuckDB 1.5.5
refuses NaN in `DECIMAL(6,2)`, Postgres 17.2 stores it (measured). So every
number goes through `core.pg_numeric.refusal`, which refuses NaN on purpose,
and the model's ISO-string dates are turned into `date`s — asyncpg refuses a
string where a DATE goes — before the latch, not inside the transaction.

THE READS FOLLOW THE CHAIN

Chain 7a's rule (`core/pg_goals_write.py`): while this chain writes Postgres,
every statement reading one of the four tables goes to Postgres whatever
`KS_READ_GOALS` says, and never falls back — after the flip DuckDB's copy is
frozen, not older. While it does not, each read keeps the engine it has today:
`revenue_predictions` by `KS_READ_GOALS`, the three goal tables from DuckDB,
where they are written. One difference from 7a: `reads_postgres` honours the
precondition, or a flagged-but-held chain would send reads to a Postgres the
hourly copy is still filling while the writes go to DuckDB.

A PRECONDITION THE FLAG ENFORCES

`unmet_precondition()` — chain 6a's arrangement. The calculators' inputs must
already be Postgres, or the chain would store, in the only copy left, numbers
read from a DuckDB that stops moving — and first of all, this build must
require `LOCKOUT_REVISION` (0035), which refuses every image built before
the chain (`chain_latch.lockout_unmet`):

1. `KS_GOALS_HISTORY` understood — unset or `silver`. The history is Silver
   either way since chain 7b-4 deleted the DN-12 bridge; any other value
   refuses every history read, so a recalculation could store nothing;
2. `KS_READ_GOALS=postgres` with `KS_PG_DSN` — what makes that Silver
   Postgres's;
3. `KS_READ_FALLBACK=off` as configured at start — under `duckdb` a failed
   Postgres history read is answered by DuckDB, and the recalculation would
   store those numbers;
4. `KS_READ_FORECAST_INPUT=postgres` with `KS_PG_DSN` — the training frame,
   which has no fallback of its own, so (3) does not cover it.

Until all four hold, an unlatched chain runs as duckdb whatever
`KS_WRITE_FORECAST` says; `/api/health` publishes why and the canary warns
(`write_chain_precondition_unmet`). A latched chain keeps writing Postgres
(OD-19 (a)), and the same warning then says its inputs may come from DuckDB.

THE WAY BACK

`scripts/chain_copy_back.py forecast`, the only thing that releases the latch.
The four tables are small and carried whole. Recalculating
(`POST /api/goals/recalculate`) and retraining
(`POST /api/revenue/forecast/train`) are the cheaper way to fix their CONTENT —
they write wherever the chain writes — but not a way around the copy-back.

The model artefact (`data/revenue_model.joblib` and its JSON siblings) is a
file, not a table, and does not move.
"""
from __future__ import annotations

import logging
import os
from datetime import date, datetime
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from core import chain_latch

logger = logging.getLogger(__name__)

WRITE_ENV = "KS_WRITE_FORECAST"

# What this chain is called in `core.write_chains`, in the local latch marker
# and in the `/api/health` block. One spelling, so a marker written by one
# release is still read by the next. `chain_copy_back.py forecast`.
CHAIN = "pg_forecast_write"

# The tables this chain writes. `core.write_chains` reads this, and through it
# the hourly shipper, the daily comparison and the copy-back. No sync keys:
# none of the four has a `last_sync_*` watermark.
CHAIN_TABLES: Tuple[str, ...] = (
    "app.seasonal_indices", "app.growth_metrics",
    "app.weekly_patterns", "app.revenue_predictions",
)

# The revision that refuses every image built before this chain; the flag
# moves nothing until this build requires it (`chain_latch.lockout_unmet`).
LOCKOUT_REVISION = "0035_batch_e_chains"

# The holes every shared statement that reads one of these tables carries —
# `core.sql_dialect.render_tables` fills them with the bare DuckDB name or the
# `app.` one. `reads_the_chain` recognises a read of the chain by them.
TABLE_HOLES: Tuple[str, ...] = (
    "{seasonal_indices}", "{growth_metrics}",
    "{weekly_patterns}", "{revenue_predictions}",
)

# The transaction-scoped advisory lock both writers take first. 'ks' in the
# high half and the chain's name in the low ("7b03"), so `pg_locks` shows it
# as classid 1802698752, objid 31491 — `pg_inventory_write.CHAIN_LOCK_KEY`'s
# convention. DuckDB's store lock admitted one writer; without this, two
# `store_predictions` over one range interleave their DELETE and INSERT and the
# second collides on the primary key, and a Monday job beside an admin POST
# would interleave two goal sets.
CHAIN_LOCK_KEY = 0x6B73_0000_0000_7B03

# `NUMERIC(precision, scale)` of revision 0025, and DuckDB's `DECIMAL`s beside it.
NUMERIC_COLUMNS = {
    "seasonality_index": (6, 4),
    "avg_revenue": (12, 2),
    "min_revenue": (12, 2),
    "max_revenue": (12, 2),
    "yoy_growth": (6, 4),
    "value": (8, 4),
    "weight": (6, 4),
    "predicted_revenue": (12, 2),
    "model_mae": (10, 2),
    "model_mape": (6, 2),
    "model_wape": (6, 2),
}

# `VARCHAR(n)` of revision 0025 — DuckDB's `sales_type` has no length.
CONFIDENCE_MAX = 10
SALES_TYPE_MAX = 32
_INT32_MAX = 2 ** 31 - 1


def env_writes_postgres() -> bool:
    """What `KS_WRITE_FORECAST` alone says.

    An unrecognised value raises — `KS_BOT_STORE`'s rule. Since DN-01 that
    stops this chain and nothing else: the registry stands its tables down,
    and the two writers raise.
    """
    value = (os.getenv(WRITE_ENV) or "duckdb").strip().lower()
    if value not in {"duckdb", "postgres"}:
        raise RuntimeError(
            f"{WRITE_ENV}={value!r} is not understood; expected "
            f"'duckdb' or 'postgres'"
        )
    return value == "postgres"


def _unmet_clauses() -> List[str]:
    """One sentence per unmet clause, names only — the block is public."""
    from core import pg_forecast_read, pg_goals_read, read_fallback

    unmet: List[str] = []
    try:
        pg_goals_read.history_mode()    # `silver`, or it raises (chain 7b-4)
    except Exception as exc:  # noqa: BLE001 — carried out, not swallowed
        unmet.append(f"{pg_goals_read.HISTORY_ENV} is not understood ({exc})")
    try:
        if not pg_goals_read.enabled():
            unmet.append(f"{pg_goals_read.ENV} is not postgres")
        elif not pg_goals_read.available():
            unmet.append(f"{pg_goals_read.ENV}=postgres but KS_PG_DSN is not set")
    except Exception as exc:  # noqa: BLE001
        unmet.append(f"{pg_goals_read.ENV} is not understood ({exc})")
    # The mode this process was configured with, not the live environment: the
    # process refuses (or falls back) by the configured value.
    if read_fallback.mode_error():
        unmet.append(f"{read_fallback.ENV} is not understood")
    elif read_fallback.mode() != read_fallback.OFF:
        unmet.append(f"{read_fallback.ENV} is not {read_fallback.OFF} "
                     f"(as configured at start)")
    try:
        if not pg_forecast_read.enabled():
            unmet.append(f"{pg_forecast_read.ENV} is not postgres")
        elif not pg_forecast_read.available():
            unmet.append(f"{pg_forecast_read.ENV}=postgres but KS_PG_DSN is not set")
    except Exception as exc:  # noqa: BLE001
        unmet.append(f"{pg_forecast_read.ENV} is not understood ({exc})")
    return unmet


def unmet_precondition() -> Optional[str]:
    """Why `KS_WRITE_FORECAST=postgres` must not move the writes yet, or None.

    Never raises and never asks Postgres: `/api/health` reads it through the
    registry and must answer with Postgres down. A flag nobody can parse is
    unmet, because nobody can then say where the inputs come from. The
    lock-out (`chain_latch.lockout_unmet`) comes first."""
    reasons = [why for why in (chain_latch.lockout_unmet(LOCKOUT_REVISION),
                               _inputs_unmet()) if why]
    return "; ".join(reasons) if reasons else None


def _inputs_unmet() -> Optional[str]:
    try:
        unmet = _unmet_clauses()
    except Exception as exc:  # noqa: BLE001 — carried out, not swallowed
        unmet = [f"the inputs could not be read ({exc})"]
    if not unmet:
        return None
    return ("; ".join(unmet) + ", so the goal and forecast calculators could "
            "read DuckDB while their results are stored in the only copy left; "
            "move the inputs to Postgres first")


def _routes_postgres(*, log: bool) -> bool:
    """Latch, then flag, then precondition. Raises on an unreadable flag of an
    unlatched chain."""
    if chain_latch.latched(CHAIN):
        return True
    if not env_writes_postgres():
        return False
    unmet = unmet_precondition()
    if unmet:
        if log:
            logger.warning("%s=postgres, but the chain stays on DuckDB: %s",
                           WRITE_ENV, unmet)
        return False
    return True


def writes_postgres() -> bool:
    """Whether this chain writes Postgres — the one answer every caller reads.

    The latch outranks the flag (OD-19 (a)), and a value nobody can read.
    Unlatched, the flag moves the writes only once `unmet_precondition()`
    holds. Raises on a flag nobody can read while unlatched.
    """
    return _routes_postgres(log=True)


def reads_postgres() -> bool:
    """Whether a read of this chain's tables must ask Postgres, whatever the
    page's own read flag says — `writes_postgres()` applied to reads.

    Two differences. An unreadable flag on a chain that never wrote here
    hands the choice back to today's engines instead of raising (7a's rule:
    the writers refuse and the tables stand down, so neither copy moves).
    And no WARNING here: this is asked on every page view.
    """
    if chain_latch.latched(CHAIN):
        return True
    try:
        return _routes_postgres(log=False)
    except RuntimeError:
        return False


def reads_the_chain(sql: str) -> bool:
    """Whether this statement reads one of the chain's tables while the chain
    owns them. Decided from the body, so a statement that starts reading one
    of the four tomorrow follows the chain through whichever router carries
    it; `tests/unit/test_goals_reads_follow_chain.py` walks `core/` for every
    such statement and fails if its router does not ask."""
    return any(hole in sql for hole in TABLE_HOLES) and reads_postgres()


def _fits(column: str, value, *, nullable: bool) -> None:
    """Raise `ValueError` for a number Postgres (or DuckDB) would refuse in
    `column`. Asked before the latch — see the module docstring."""
    from core.pg_numeric import refusal

    precision, scale = NUMERIC_COLUMNS[column]
    why = refusal(column, value, precision, scale, nullable=nullable)
    if why:
        raise ValueError(why)


def _int(column: str, value, *, low: int = 0, high: int = _INT32_MAX,
         nullable: bool = True) -> None:
    if value is None:
        if nullable:
            return
        raise ValueError(f"{column} must not be NULL")
    if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
        raise ValueError(f"{column}={value!r} is not an integer in {low}..{high}")


def _date(column: str, value, *, nullable: bool) -> Optional[date]:
    """A `date` for asyncpg, which refuses an ISO string where DuckDB coerces
    one. A string that is not a date raises `ValueError` here, not
    `DataError` inside the transaction."""
    if value is None:
        if nullable:
            return None
        raise ValueError(f"{column} must not be NULL")
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value.strip()[:10])
        except ValueError:
            raise ValueError(f"{column}={value!r} is not a date") from None
    raise ValueError(f"{column}={value!r} is not a date")


def _stamp(column: str, value) -> datetime:
    """The caller's clock, aware — the Postgres columns have no default
    (revision 0025 says why), and the standing watch reads this stamp."""
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError(f"{column}={value!r} must be an aware datetime")
    return value


def _latch() -> str:
    """Take the local half of the latch before this process writes Postgres.

    Called with a connection already acquired, after `require_revision()` has
    passed — `pg_expenses_write._latch` has the reason: the latch is permanent,
    and a write that never reaches Postgres must not spend the rollback.
    """
    return chain_latch.latch(CHAIN, WRITE_ENV)


async def _pool():
    from core.pg import get_pool, require_revision

    pool = await get_pool()
    await require_revision()
    return pool


def _goal_rows(
    seasonal_rows: Sequence[Sequence[Any]],
    yoy_overall: Sequence[Any],
    monthly_yoy: Mapping[int, Any],
    weekly_rows: Sequence[Sequence[Any]],
    now: datetime,
):
    """Every value `persist_goal_tables` will send, checked. Pure; raises
    `ValueError` before any connection is touched."""
    now = _stamp("updated_at", now)
    seasonal = []
    for row in seasonal_rows:
        month, index, sample, avg, low, high, confidence = row
        _int("month", month, low=1, high=12, nullable=False)
        _fits("seasonality_index", index, nullable=True)
        _int("sample_size", sample)
        _fits("avg_revenue", avg, nullable=True)
        _fits("min_revenue", low, nullable=True)
        _fits("max_revenue", high, nullable=True)
        if confidence is not None and (not isinstance(confidence, str)
                                       or len(confidence) > CONFIDENCE_MAX):
            raise ValueError(f"confidence={confidence!r} does not fit VARCHAR({CONFIDENCE_MAX})")
        seasonal.append((month, index, sample, avg, low, high, confidence, now))

    value, start, end, sample = yoy_overall
    _fits("value", value, nullable=True)
    _int("sample_size", sample, nullable=False)
    growth = (value, _date("period_start", start, nullable=True),
              _date("period_end", end, nullable=True), sample, now)

    yoy = []
    for month, rate in sorted(monthly_yoy.items()):
        _int("month", month, low=1, high=12, nullable=False)
        _fits("yoy_growth", rate, nullable=False)
        yoy.append((month, rate))

    weekly = []
    for row in weekly_rows:
        month, week, weight, sample = row
        _int("month", month, low=1, high=12, nullable=False)
        _int("week_of_month", week, low=1, high=5, nullable=False)
        _fits("weight", weight, nullable=True)
        _int("sample_size", sample)
        weekly.append((month, week, weight, sample, now))
    return now, seasonal, growth, yoy, weekly


async def persist_goal_tables(
    seasonal_rows: Sequence[Sequence[Any]],
    yoy_overall: Sequence[Any],
    monthly_yoy: Mapping[int, Any],
    weekly_rows: Sequence[Sequence[Any]],
    now: datetime,
) -> Dict[str, int]:
    """The three goal tables, in one transaction. Raises.

    `seasonal_rows` — `(month, index, sample_size, avg, min, max, confidence)`;
    `yoy_overall` — `(value, period_start, period_end, sample_size)`;
    `monthly_yoy` — `{month: rate}`; `weekly_rows` —
    `(month, week_of_month, weight, sample_size)`, empty for the Monday job;
    `now` — the one stamp of this recalculation, the caller's clock.

    The statements are DuckDB's (`goals._SEASONAL_UPSERT_SQL` and its three
    siblings) with the same SET lists — the index upsert never touches
    `yoy_growth`, so a month without a pair of years keeps the value it had —
    and the same placeholder rule: `yoy_overall` is written when it was
    measured, or when there is no row to keep.
    """
    stamp_now, seasonal, growth, yoy, weekly = _goal_rows(
        seasonal_rows, yoy_overall, monthly_yoy, weekly_rows, now)
    pool = await _pool()
    async with pool.acquire() as conn:
        # Inside the acquire, not before it: an acquire that ends without a
        # connection is a write that never reached Postgres (`_latch`).
        stamp = _latch()
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock($1)", CHAIN_LOCK_KEY)
            await chain_latch.claim(conn, CHAIN_TABLES, stamp)
            # Spelled here rather than hoisted to constants, so the latch
            # guard's walk sees a writer here (chain 7a's reason).
            if seasonal:
                await conn.executemany(
                    "INSERT INTO app.seasonal_indices "
                    "(month, seasonality_index, sample_size, avg_revenue, "
                    " min_revenue, max_revenue, confidence, updated_at) "
                    "VALUES ($1, $2, $3, $4, $5, $6, $7, $8) "
                    "ON CONFLICT (month) DO UPDATE SET "
                    "seasonality_index = EXCLUDED.seasonality_index, "
                    "sample_size = EXCLUDED.sample_size, "
                    "avg_revenue = EXCLUDED.avg_revenue, "
                    "min_revenue = EXCLUDED.min_revenue, "
                    "max_revenue = EXCLUDED.max_revenue, "
                    "confidence = EXCLUDED.confidence, "
                    "updated_at = EXCLUDED.updated_at",
                    seasonal)

            has_row = await conn.fetchval(
                "SELECT 1 FROM app.growth_metrics WHERE metric_type = 'yoy_overall'"
            ) is not None
            wrote_growth = 0
            if growth[3] or not has_row:
                await conn.execute(
                    "INSERT INTO app.growth_metrics "
                    "(metric_type, value, period_start, period_end, sample_size, "
                    " updated_at) "
                    "VALUES ('yoy_overall', $1, $2, $3, $4, $5) "
                    "ON CONFLICT (metric_type) DO UPDATE SET "
                    "value = EXCLUDED.value, "
                    "period_start = EXCLUDED.period_start, "
                    "period_end = EXCLUDED.period_end, "
                    "sample_size = EXCLUDED.sample_size, "
                    "updated_at = EXCLUDED.updated_at",
                    *growth)
                wrote_growth = 1
            else:
                logger.warning(
                    "YoY: no pair of full years in the retail history; keeping "
                    "the stored yoy_overall rather than overwriting it with "
                    "the %.2f fallback", float(growth[0] or 0))

            for month, rate in yoy:
                status = await conn.execute(
                    "UPDATE app.seasonal_indices "
                    "SET yoy_growth = $1, updated_at = $2 WHERE month = $3",
                    rate, stamp_now, month)
                if status != "UPDATE 1":
                    # Rolls the whole set back: an index row the YoY was
                    # meant for is missing, so the upsert above did not run
                    # first — the split this transaction exists to prevent.
                    raise RuntimeError(
                        f"app.seasonal_indices: the YoY update of month {month} "
                        f"reported {status!r}, not 'UPDATE 1'; nothing stored")

            if weekly:
                await conn.executemany(
                    "INSERT INTO app.weekly_patterns "
                    "(month, week_of_month, weight, sample_size, updated_at) "
                    "VALUES ($1, $2, $3, $4, $5) "
                    "ON CONFLICT (month, week_of_month) DO UPDATE SET "
                    "weight = EXCLUDED.weight, "
                    "sample_size = EXCLUDED.sample_size, "
                    "updated_at = EXCLUDED.updated_at",
                    weekly)
    return {"seasonal": len(seasonal), "growth": wrote_growth,
            "yoy_months": len(yoy), "weekly": len(weekly)}


def _prediction_rows(predictions, sales_type, metrics, created_at):
    """Every value `store_predictions` will send, checked. Pure."""
    if not isinstance(sales_type, str) or not sales_type or len(sales_type) > SALES_TYPE_MAX:
        raise ValueError(f"sales_type={sales_type!r} does not fit VARCHAR({SALES_TYPE_MAX})")
    created_at = _stamp("created_at", created_at)
    metrics = metrics or {}
    mae = metrics.get("mae", 0)
    mape = metrics.get("mape", 0)
    wape = metrics.get("wape", 0)
    _fits("model_mae", mae, nullable=True)
    _fits("model_mape", mape, nullable=True)
    _fits("model_wape", wape, nullable=True)
    rows = []
    for p in predictions:
        day = _date("prediction_date", p["date"], nullable=False)
        revenue = p["predicted_revenue"]
        _fits("predicted_revenue", revenue, nullable=True)
        rows.append((day, sales_type, revenue, mae, mape, wape, created_at))
    return rows


async def store_predictions(
    predictions: Sequence[Mapping[str, Any]],
    sales_type: str,
    metrics: Optional[Mapping[str, Any]],
    created_at: datetime,
) -> int:
    """The forecast of one training, replacing the stored range. Raises.

    DELETE over `[min(date), max(date)]` then INSERT, in one transaction —
    DuckDB's statement pair exactly: past rows outside the range stay.
    `created_at` is one stamp per run, the caller's clock.

    BOUND: past predictions are never deleted — one row per day per trained
    sales type, about 365 rows a year, as in DuckDB. Arithmetic, not policy.
    """
    rows = _prediction_rows(predictions, sales_type, metrics, created_at)
    if not rows:
        return 0
    first = min(r[0] for r in rows)
    last = max(r[0] for r in rows)
    pool = await _pool()
    async with pool.acquire() as conn:
        # Inside the acquire, not before it (`_latch`).
        stamp = _latch()
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock($1)", CHAIN_LOCK_KEY)
            await chain_latch.claim(conn, CHAIN_TABLES, stamp)
            await conn.execute(
                "DELETE FROM app.revenue_predictions "
                "WHERE sales_type = $1 AND prediction_date >= $2 "
                "AND prediction_date <= $3",
                sales_type, first, last)
            await conn.executemany(
                "INSERT INTO app.revenue_predictions "
                "(prediction_date, sales_type, predicted_revenue, model_mae, "
                " model_mape, model_wape, created_at) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7)",
                rows)
    logger.info("Stored %d predictions for %s in Postgres", len(rows), sales_type)
    return len(rows)
