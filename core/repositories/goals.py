"""DuckDBStore goals and forecast methods."""
from __future__ import annotations

import calendar
import logging
from collections import defaultdict
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any, Tuple

from core.duckdb_constants import DEFAULT_TZ
from core import read_fallback

logger = logging.getLogger(__name__)


# ─── The revenue history a goal is measured against ─────────────────────────
#
# One body per shape, two engines, and the table name the only difference —
# `core/pg_goals_read.py` carries the reasoning. Three things these no longer
# do, each of which was a rule this file kept its own copy of:
#
#   * the Kyiv date is not computed — `order_date` *is* it;
#   * returns come from `is_return`, which prefers KeyCRM's status group,
#     rather than from a second list of status ids;
#   * the channel filter is `is_active_source` rather than a literal
#     `source_id IN (1, 2, 4)` present in one of these bodies and absent from
#     the other.
#
# `{grain}` is `s.order_date` or a `DATE_TRUNC` of it, and `{floor}` the start
# of the window — both composed here from validated parts, never from user
# text. The window's upper bound is `TODAY_IN_KYIV` and not `CURRENT_DATE`,
# because the two engines disagree about what day that is for three hours out
# of every twenty-four.

_GOALS_PERIOD_REVENUE_SQL = """
    WITH period_revenue AS (
        SELECT {grain} AS period_start,
               SUM(s.grand_total) AS revenue
        FROM {silver_orders} s
        WHERE {where}
        GROUP BY {grain}
    )
    SELECT AVG(revenue), MIN(revenue), MAX(revenue), COUNT(*), STDDEV(revenue)
    FROM period_revenue
"""

_GOALS_WEEKLY_TREND_SQL = """
    WITH weekly_revenue AS (
        SELECT DATE_TRUNC('week', s.order_date) AS week_start,
               SUM(s.grand_total) AS revenue,
               ROW_NUMBER() OVER (
                   ORDER BY DATE_TRUNC('week', s.order_date) DESC) AS week_num
        FROM {silver_orders} s
        WHERE {where}
        GROUP BY DATE_TRUNC('week', s.order_date)
    )
    SELECT AVG(CASE WHEN week_num <= 2 THEN revenue END),
           AVG(CASE WHEN week_num > 2 THEN revenue END)
    FROM weekly_revenue
"""

_GOALS_DAILY_FOR_DATES_SQL = """
    SELECT s.order_date AS day, SUM(s.grand_total) AS revenue
    FROM {silver_orders} s
    WHERE {where}
    GROUP BY s.order_date
"""

# The forecast a reader sees on the revenue chart. Revision 0025 gave this table
# a Postgres home, which is the only reason this read can move at all — and the
# dates go over as `date` objects, never as ISO strings. The sibling method two
# rows down shipped that bug: DuckDB coerces the string, asyncpg refuses it, and
# under the flag the read falls back and the tab looks switched while it is not.
_PREDICTIONS_SQL = """
    SELECT prediction_date, predicted_revenue, model_mae, model_mape, model_wape
    FROM {revenue_predictions}
    WHERE sales_type = ?
      AND prediction_date >= ?
      AND prediction_date <= ?
    ORDER BY prediction_date
"""

_STORED_GOALS_SQL = """
    SELECT period_type, goal_amount, is_custom, calculated_goal, growth_factor
    FROM {revenue_goals}
    ORDER BY period_type
"""

# The ML signal of a smart goal: what the target month has already earned, plus
# what the model expects of the rest of it. Both halves go through `_goals_run`,
# so both ask the engine the flag names — the rule
# `PredictionService._get_actual_month_revenue` records: a sum that took one
# term from Postgres and the other from DuckDB *by design* was a defect there
# once already. One engine only while neither read falls back, though: the
# fallback is per read, so a Postgres failure on one half is answered by DuckDB
# while the other half stays Postgres', and that call's sum mixes engines —
# after step 13, a frozen DuckDB term beside a live one. `_goals_run` logs it
# at ERROR; `get_forecast` accepts the same mix, and DN-20's off mode is what
# turns it into a refusal. `{gold_revenue_rollup}` is not optional; Postgres'
# Gold holds a roll-up row beside a row per source, and a bare SUM counts every
# order twice. `{sales_filter}` is `sales_type = ?` or `1=1`, composed here.
_FORECAST_ACTUAL_SQL = """
    SELECT COALESCE(SUM(revenue), 0)
    FROM {gold_daily_revenue}
    WHERE date BETWEEN ? AND ?
      AND {gold_revenue_rollup}
      AND {sales_filter}
"""

_FORECAST_PREDICTED_SQL = """
    SELECT SUM(predicted_revenue)
    FROM {revenue_predictions}
    WHERE sales_type = ?
      AND prediction_date >= ?
      AND prediction_date <= ?
"""

# What a smart goal reads of the three shared goal tables, in ONE statement:
# their writer stores them in one transaction, so one statement reads one
# committed set in either engine — what a single hold of DuckDB's store lock
# gave the three separate reads this replaced. `part` says which table a row
# came from. `CAST(NULL AS INTEGER)` and not a bare NULL: Postgres types a
# UNION's column from its first branch and refuses `text` against `integer`
# (measured, 17.2); both engines accept the cast. Read through
# `_goal_tables_run`, which follows chain 7b-3.
_SHARED_GOAL_TABLES_SQL = """
    SELECT 'seasonal' AS part, CAST(NULL AS INTEGER) AS week,
           seasonality_index AS a, avg_revenue AS b, yoy_growth AS c,
           confidence AS d
    FROM {seasonal_indices}
    WHERE month = ?
    UNION ALL
    SELECT 'growth', NULL, value, NULL, NULL, NULL
    FROM {growth_metrics}
    WHERE metric_type = 'yoy_overall'
    UNION ALL
    SELECT 'weekly', week_of_month, weight, NULL, NULL, NULL
    FROM {weekly_patterns}
    WHERE month = ?
"""


# ─── Which orders a goal calculator counts ──────────────────────────────────
#
# Silver's, and only Silver's (chain 7b-4).
#
# From DN-12 to chain 7b-4 the calculators below read a BRIDGE: DuckDB
# `orders` narrowed by `silver_sales_type_case` rendered over DuckDB's
# `managers` and `manager_classifications` — right only while DuckDB held all
# three, and step 13 freezes them. Chain 7b-2 added the Silver history beside
# it under `KS_GOALS_HISTORY`; production read Silver from 2026-10-07 08:20
# UTC, after a dry run that found 0 differences; 7b-4 deleted the bridge.
# `KS_GOALS_HISTORY` is retired: unset and `silver` both mean Silver, and any
# other value — `bridge` included — refuses every history read
# (`core/pg_goals_read.py`), so a value naming the deleted history is never
# answered from Silver in silence.

# The tables the deleted bridge read out of DuckDB. Chains 3 and 5 still ask
# for this name in their `goals_bridge` preconditions; it goes with them.
SALES_TYPE_BRIDGE_TABLES = frozenset(
    {"bronze.orders", "bronze.managers", "app.manager_classifications"})


def sales_type_bridge_owners() -> Dict[str, Tuple[str, ...]]:
    """`{chain: tables}` for every registered write chain that has moved a
    table in `SALES_TYPE_BRIDGE_TABLES`. Nothing reads it but chain 5's
    tests; it goes with chains 3 and 5's `goals_bridge`. Never raises."""
    from core.write_chains import WRITE_CHAINS, _chain_state, chain_name

    owners: Dict[str, Tuple[str, ...]] = {}
    for chain in WRITE_CHAINS:
        owned = frozenset(getattr(chain, "CHAIN_TABLES", ())) & SALES_TYPE_BRIDGE_TABLES
        if not owned:
            continue
        try:
            moved = _chain_state(chain)["mode"] != "duckdb"
        except Exception:  # noqa: BLE001 — unreadable is moved
            moved = True
        if moved:
            owners[chain_name(chain)] = tuple(sorted(owned))
    return owners


# ─── The history, from Silver (chain 7b-2; the only one since 7b-4) ─────────
#
# Every calculator reads `{silver_orders}` through `_goals_run`, so the engine
# is `KS_READ_GOALS`' — Postgres Silver keeps deriving after step 13, from
# whichever store holds the classification — and a read DuckDB answers is
# counted (DN-20a), or refused under `KS_READ_FALLBACK=off` (DN-20b). Nothing
# below names DuckDB's `orders`, `managers` or `manager_classifications`.
#
# Silver's columns select what the deleted bridge selected: `is_return`
# (KeyCRM's status group first, the status list for rows synced before it) for
# the status list, `order_date` for the per-row Kyiv date, `sales_type` for the
# CASE the bridge rendered over `orders`. Measured on the 2026-08-31 production
# copy: 0 differing orders of 46 961, for every sales type, and every
# calculator output and every smart goal identical, on DuckDB and on Postgres.
#
# **`is_active_source` is deliberately absent** (OQ-1, the owner's question).
# Every dashboard read carries it; a growth baseline must not. Source 3 is the
# company website on Opencart, which carried 2 055 retail orders, ₴5.0M, from
# 2024-07-15 to 2024-12-07 — Shopify (4) took over from 2024-11-28, with no
# order on both. Dropping them shrinks 2024: retail YoY moves from 0.50 to
# 0.82, five monthly caps hit the 0.50 ceiling, and next month's retail goal
# moves from ₴4.0M to ₴4.2M. The bridge never filtered on source, so leaving it
# out is what kept the numbers. A test pins a source-3 order as counted.
#
# `{where}` is `_silver_history_where`'s, and every date bound is a parameter
# computed in Python from the Kyiv clock — never `CURRENT_DATE`, which the two
# engines disagree about for three hours a day (`core/pg_goals_read.py`).

_SILVER_SEASONALITY_SQL = """
    WITH monthly_data AS (
        SELECT EXTRACT(YEAR FROM s.order_date) AS year,
               EXTRACT(MONTH FROM s.order_date) AS month,
               SUM(s.grand_total) AS revenue,
               COUNT(DISTINCT s.order_date) AS days_with_orders
        FROM {silver_orders} s
        WHERE {where}
        GROUP BY EXTRACT(YEAR FROM s.order_date), EXTRACT(MONTH FROM s.order_date)
        HAVING COUNT(DISTINCT s.order_date) >= 20
    ),
    monthly_stats AS (
        SELECT month,
               AVG(revenue) AS avg_revenue,
               MIN(revenue) AS min_revenue,
               MAX(revenue) AS max_revenue,
               COUNT(*) AS sample_size,
               STDDEV(revenue) AS std_dev
        FROM monthly_data
        GROUP BY month
    ),
    overall_avg AS (
        SELECT AVG(avg_revenue) AS grand_avg FROM monthly_stats
    )
    SELECT ms.month, ms.avg_revenue, ms.min_revenue, ms.max_revenue,
           ms.sample_size, ms.std_dev, ms.avg_revenue / oa.grand_avg
    FROM monthly_stats ms, overall_avg oa
    ORDER BY ms.month
"""

# Full years only — eleven months with orders or more — and never the running
# Kyiv year, whose first day is the last parameter.
_SILVER_YEARLY_SQL = """
    WITH yearly_data AS (
        SELECT EXTRACT(YEAR FROM s.order_date) AS year,
               SUM(s.grand_total) AS revenue,
               COUNT(DISTINCT EXTRACT(MONTH FROM s.order_date)) AS months_active
        FROM {silver_orders} s
        WHERE {where}
          AND s.order_date < ?
        GROUP BY EXTRACT(YEAR FROM s.order_date)
    )
    SELECT year, revenue FROM yearly_data
    WHERE months_active >= 11
    ORDER BY year
"""

_SILVER_MONTHLY_YOY_SQL = """
    WITH monthly_by_year AS (
        SELECT EXTRACT(YEAR FROM s.order_date) AS year,
               EXTRACT(MONTH FROM s.order_date) AS month,
               SUM(s.grand_total) AS revenue
        FROM {silver_orders} s
        WHERE {where}
        GROUP BY EXTRACT(YEAR FROM s.order_date), EXTRACT(MONTH FROM s.order_date)
        HAVING COUNT(DISTINCT s.order_date) >= 25
    )
    SELECT curr.month, curr.year,
           (curr.revenue - prev.revenue) / NULLIF(prev.revenue, 0)
    FROM monthly_by_year curr
    JOIN monthly_by_year prev
      ON curr.month = prev.month AND curr.year = prev.year + 1
    ORDER BY curr.month, curr.year
"""

# `growth_metrics.period_start`/`period_end`: the first and last order date of
# the whole history, every sales type and every status — the question the
# bridge asked of `orders`, asked of Silver.
_SILVER_BOUNDS_SQL = """
    SELECT MIN(s.order_date), MAX(s.order_date) FROM {silver_orders} s
"""

# Week of month 1-5 by day of month. `CAST(… AS INTEGER)` rather than `::int`
# reads the same in both engines, and `CEIL` of a NUMERIC (Postgres) and of a
# DOUBLE (DuckDB) agree on every day 1-31.
_SILVER_WEEKLY_SQL = """
    WITH weekly_data AS (
        SELECT EXTRACT(YEAR FROM s.order_date) AS year,
               EXTRACT(MONTH FROM s.order_date) AS month,
               LEAST(5, CAST(CEIL(EXTRACT(DAY FROM s.order_date) / 7.0) AS INTEGER))
                   AS week_of_month,
               SUM(s.grand_total) AS revenue
        FROM {silver_orders} s
        WHERE {where}
        GROUP BY EXTRACT(YEAR FROM s.order_date),
                 EXTRACT(MONTH FROM s.order_date),
                 LEAST(5, CAST(CEIL(EXTRACT(DAY FROM s.order_date) / 7.0) AS INTEGER))
    ),
    monthly_totals AS (
        SELECT year, month, SUM(revenue) AS month_total
        FROM weekly_data
        GROUP BY year, month
    ),
    weekly_weights AS (
        SELECT wd.month, wd.week_of_month,
               AVG(wd.revenue / NULLIF(mt.month_total, 0)) AS avg_weight,
               COUNT(*) AS sample_size
        FROM weekly_data wd
        JOIN monthly_totals mt ON wd.year = mt.year AND wd.month = mt.month
        GROUP BY wd.month, wd.week_of_month
    )
    SELECT month, week_of_month, avg_weight, sample_size
    FROM weekly_weights
    ORDER BY month, week_of_month
"""

# The target month's consecutive-year YoY; the month is the first parameter.
_SILVER_GROWTH_CAP_SQL = """
    WITH monthly_by_year AS (
        SELECT EXTRACT(YEAR FROM s.order_date) AS year,
               SUM(s.grand_total) AS revenue
        FROM {silver_orders} s
        WHERE EXTRACT(MONTH FROM s.order_date) = ?
          AND {where}
        GROUP BY EXTRACT(YEAR FROM s.order_date)
        HAVING COUNT(DISTINCT s.order_date) >= 25
    ),
    yoy_pairs AS (
        SELECT (curr.revenue - prev.revenue) / NULLIF(prev.revenue, 0) AS yoy
        FROM monthly_by_year curr
        JOIN monthly_by_year prev ON curr.year = prev.year + 1
    )
    SELECT AVG(yoy), STDDEV(yoy), COUNT(*) FROM yoy_pairs
"""

# The target month a year earlier, as a half-open range of dates: the bound is
# a plain comparison on the column, in both engines.
_SILVER_LAST_YEAR_SQL = """
    SELECT SUM(s.grand_total)
    FROM {silver_orders} s
    WHERE s.order_date >= ? AND s.order_date < ?
      AND {where}
"""

# The last three complete months with 25 days of orders; the first of the
# current Kyiv month is the last parameter.
_SILVER_RECENT_MONTHS_SQL = """
    WITH monthly_revenue AS (
        SELECT EXTRACT(YEAR FROM s.order_date) AS year,
               EXTRACT(MONTH FROM s.order_date) AS month,
               SUM(s.grand_total) AS revenue
        FROM {silver_orders} s
        WHERE {where}
          AND s.order_date < ?
        GROUP BY EXTRACT(YEAR FROM s.order_date), EXTRACT(MONTH FROM s.order_date)
        HAVING COUNT(DISTINCT s.order_date) >= 25
        ORDER BY year DESC, month DESC
        LIMIT 3
    )
    SELECT AVG(revenue) FROM monthly_revenue
"""


def _silver_history_where(sales_type: str) -> tuple[str, List[Any]]:
    """`(clause, params)` over `{silver_orders} s`: not a return, and one
    `sales_type` unless `'all'`. No `is_active_source` — see above. An unknown
    sales_type raises: it used to fall through an else and quietly mean
    retail."""
    from core.duckdb_constants import KNOWN_SALES_TYPES

    if sales_type == "all":
        return "NOT s.is_return", []
    if sales_type not in KNOWN_SALES_TYPES:
        raise ValueError(
            f"unknown sales_type {sales_type!r}; expected one of "
            f"{', '.join(KNOWN_SALES_TYPES)} or 'all'"
        )
    return "NOT s.is_return AND s.sales_type = ?", [sales_type]


def _refuse_a_retired_history() -> None:
    """`KS_GOALS_HISTORY`, asked at every history read: unset or `silver`
    reads Silver, and anything else raises — `bridge` included, since chain
    7b-4 deleted it (`core/pg_goals_read.py`). Asked first, so a refused value
    reads nothing and writes nothing."""
    from core import pg_goals_read

    pg_goals_read.history_mode()


def _first_of_next_month(d: date) -> date:
    return date(d.year + (d.month == 12), d.month % 12 + 1, 1)


# ─── The three shared goal tables ───────────────────────────────────────────
#
# `seasonal_indices`, `growth_metrics` and `weekly_patterns` carry no
# sales_type in their keys, so whatever is stored there is what every user's
# goal is built from — `b2b` and `all` included. The Monday job stopped storing
# a b2b pass over the retail rows (`core/scheduler.py`), and the POST is held
# to the same answer: the rows mean retail. Making sales_type part of the keys
# is a migration, and out of chain 7b's scope.
GOAL_TABLES_SALES_TYPE = "retail"

# The statements `_persist_goal_tables` runs, in its one transaction. The same
# texts the calculators ran when they stored for themselves.
_SEASONAL_UPSERT_SQL = """
    INSERT INTO seasonal_indices
    (month, seasonality_index, sample_size, avg_revenue, min_revenue, max_revenue, confidence, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT (month) DO UPDATE SET
        seasonality_index = excluded.seasonality_index,
        sample_size = excluded.sample_size,
        avg_revenue = excluded.avg_revenue,
        min_revenue = excluded.min_revenue,
        max_revenue = excluded.max_revenue,
        confidence = excluded.confidence,
        updated_at = excluded.updated_at
"""

_YOY_OVERALL_EXISTS_SQL = (
    "SELECT 1 FROM growth_metrics WHERE metric_type = 'yoy_overall'")

_YOY_OVERALL_UPSERT_SQL = """
    INSERT INTO growth_metrics (metric_type, value, period_start, period_end, sample_size, updated_at)
    VALUES ('yoy_overall', ?, ?, ?, ?, ?)
    ON CONFLICT (metric_type) DO UPDATE SET
        value = excluded.value,
        period_start = excluded.period_start,
        period_end = excluded.period_end,
        sample_size = excluded.sample_size,
        updated_at = excluded.updated_at
"""

_MONTHLY_YOY_UPDATE_SQL = """
    UPDATE seasonal_indices
    SET yoy_growth = ?, updated_at = ?
    WHERE month = ?
"""

_WEEKLY_UPSERT_SQL = """
    INSERT INTO weekly_patterns (month, week_of_month, weight, sample_size, updated_at)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT (month, week_of_month) DO UPDATE SET
        weight = excluded.weight,
        sample_size = excluded.sample_size,
        updated_at = excluded.updated_at
"""

# Default: slightly more revenue in weeks 1-4, less in week 5.
_DEFAULT_WEEKLY_WEIGHTS = {1: 0.23, 2: 0.23, 3: 0.23, 4: 0.23, 5: 0.08}


def _recency_weighted(rates: List[float]) -> float:
    """Oldest pair weighs 1.0, newest 2.0, linear in between."""
    n = len(rates)
    if n == 1:
        return rates[0]
    weights = [1.0 + (i / (n - 1)) for i in range(n)]
    return sum(r * w for r, w in zip(rates, weights)) / sum(weights)


def _seasonality_answer(results) -> Dict[int, Dict[str, Any]]:
    """`{month: index row}` from `(month, avg, min, max, sample_size, stddev,
    index)` rows — the same arithmetic for both histories. `float()` and
    `int()` on every term: DuckDB answers BIGINT and DOUBLE, Postgres NUMERIC."""
    indices = {}
    for row in results:
        month = int(row[0])
        sample_size = int(row[4] or 0)

        # Determine confidence based on sample size
        if sample_size >= 3:
            confidence = "high"
        elif sample_size >= 2:
            confidence = "medium"
        else:
            confidence = "low"

        indices[month] = {
            "month": month,
            "avg_revenue": round(float(row[1] or 0), 2),
            "min_revenue": round(float(row[2] or 0), 2),
            "max_revenue": round(float(row[3] or 0), 2),
            "sample_size": sample_size,
            "std_dev": round(float(row[5] or 0), 2),
            "seasonality_index": round(float(row[6] or 1.0), 4),
            "confidence": confidence
        }

    logger.info(f"Calculated seasonality indices for {len(indices)} months")
    return indices


def _yoy_answer(yearly_results, monthly_yoy_results) -> Tuple[Dict[str, Any], float]:
    """The YoY arithmetic over the two reads: `(answer, overall)`, the second
    unrounded, as `growth_metrics.value` stores it.

    Floats, every term. The revenue arrives as DuckDB DECIMALs (and as asyncpg
    `Decimal`s on the Silver path) and the recency weights are floats, and
    `Decimal * float` raises — which it did not only because one pair of
    years had never needed a weight.
    """
    yoy_rates: List[float] = []
    for i in range(1, len(yearly_results)):
        prev_year = float(yearly_results[i - 1][1])
        curr_year = float(yearly_results[i][1])
        if prev_year > 0:
            yoy_rates.append((curr_year - prev_year) / prev_year)
    overall_yoy = _recency_weighted(yoy_rates) if yoy_rates else 0.10

    # Group by month, apply recency-weighted average
    month_pairs: Dict[int, List[float]] = defaultdict(list)
    for row in monthly_yoy_results:
        if row[2] is not None:
            month_pairs[int(row[0])].append(float(row[2]))
    monthly_yoy = {month: round(_recency_weighted(rates), 4)
                   for month, rates in month_pairs.items()}

    logger.info(f"Calculated YoY growth: {overall_yoy:.2%}")
    return {
        "overall_yoy": round(overall_yoy, 4),
        "monthly_yoy": monthly_yoy,
        "yearly_data": [
            {"year": int(row[0]), "revenue": round(float(row[1]), 2)}
            for row in yearly_results
        ],
        "sample_size": len(yoy_rates),
    }, overall_yoy


def _weekly_answer(results) -> Tuple[Dict[int, Dict[int, float]], List[List[Any]]]:
    """`(patterns, rows)`: every month and week filled for the reader, and
    only the measured ones, unrounded, for the table."""
    patterns: Dict[int, Dict[int, float]] = {}
    pattern_rows: List[List[Any]] = []
    for row in results:
        month = int(row[0])
        week = int(row[1])
        weight = float(row[2] or 0.25)  # Default to 25% if no data
        patterns.setdefault(month, {})[week] = round(weight, 4)
        pattern_rows.append([month, week, weight, int(row[3])])

    # Ensure all months have all 5 weeks (fill missing with equal distribution)
    for month in range(1, 13):
        weeks = patterns.setdefault(month, {})
        for week in range(1, 6):
            weeks.setdefault(week, _DEFAULT_WEEKLY_WEIGHTS[week])

    logger.info(f"Calculated weekly patterns for {len(patterns)} months")
    return patterns, pattern_rows


def _growth_cap(row) -> float:
    """`max(0.10, min(0.50, avg + 1.5 * stddev))` over the target month's
    consecutive-year YoY, from `(avg, stddev, count)`; 0.35 with no pair."""
    if not row or not row[2] or row[2] < 1 or row[0] is None:
        return 0.35  # fallback
    avg_yoy = float(row[0])
    std_yoy = float(row[1] or 0)
    cap = avg_yoy + 1.5 * std_yoy
    return max(0.10, min(0.50, cap))


class GoalsMixin:

    async def _goals_run(self, sql: str, params=None):
        """Run one goal query against whichever engine the flag names.

        `/goals` keeps its own switch for every other tab's reason, sharpened:
        this is the one tab that writes from the interface, and
        `app.revenue_goals` is an hourly read replica. Reading it from Postgres
        is safe; writing there through this router would land in a copy. The
        write moves only as chain 7a (`KS_WRITE_GOALS`, `set_goal` below),
        which stands the replica down for that table.

        So a statement reading `{revenue_goals}` goes where the chain writes,
        whatever this flag says, and does not fall back: once the chain writes
        Postgres, DuckDB's copy is frozen, and a fallback would show the goal
        from before the flip as if it were current. `core/pg_goals_write.py`
        has the reasoning; every other statement here keeps the flag.

        Chain 7b-3 (`KS_WRITE_FORECAST`) does the same for a statement
        reading `{revenue_predictions}` — the forecast a smart goal and
        `/api/revenue/forecast` read: Postgres, with no fallback, while that
        chain writes there, and this flag as before while it does not. A
        failure there is a counted refusal (`read_fallback.chain_refusal`),
        what the same failure was before the flip under the chain's `off`:
        raw, it reached `get_forecast_data` and `_get_ml_forecast_total`,
        which let `ReadUnavailable` through and contain everything else, so
        the forecast read "not available yet" and the goal lost its ML signal
        with nothing counted.
        """
        from core.sql_dialect import DUCKDB, POSTGRES, render_tables

        from core import pg_goals_read, pg_goals_write
        from core import pg_forecast_write

        params = list(params or [])
        if pg_goals_write.reads_the_chain(sql):
            return await pg_goals_read.fetch(render_tables(sql, POSTGRES), params)
        if pg_forecast_write.reads_the_chain(sql):
            try:
                return await pg_goals_read.fetch(
                    render_tables(sql, POSTGRES), params)
            except Exception as exc:  # noqa: BLE001 — refused, never DuckDB
                raise read_fallback.chain_refusal("goals", exc) from exc
        read_fallback.no_address("goals", pg_goals_read)
        if pg_goals_read.enabled() and pg_goals_read.available():
            try:
                return await pg_goals_read.fetch(
                    render_tables(sql, POSTGRES), params)
            except Exception as exc:  # noqa: BLE001
                read_fallback.fall_back("goals", exc)

        async with self.connection() as conn:
            return conn.execute(render_tables(sql, DUCKDB), params).fetchall()

    async def _goal_tables_run(self, sql: str, params=None):
        """Read the three shared goal tables from whichever store writes them.

        Not `_goals_run`, on purpose. While chain 7b-3 writes DuckDB these
        tables are read in DuckDB, where they are written, whatever
        `KS_READ_GOALS` says: that flag is `postgres` in production, and
        following it would read the hourly replica and show a POSTed
        recalculation up to an hour late. While the chain writes Postgres they
        are read there with no fallback — DuckDB's copy is then frozen, not
        older (`core/pg_forecast_write.py`) — and a failure is a counted
        refusal, a 503 naming `goals`, as in `_goals_run`.
        """
        from core.sql_dialect import DUCKDB, POSTGRES, render_tables

        from core import pg_goals_read
        from core import pg_forecast_write

        params = list(params or [])
        if pg_forecast_write.reads_the_chain(sql):
            try:
                return await pg_goals_read.fetch(
                    render_tables(sql, POSTGRES), params)
            except Exception as exc:  # noqa: BLE001 — refused, never DuckDB
                raise read_fallback.chain_refusal("goals", exc) from exc
        async with self.connection() as conn:
            return conn.execute(render_tables(sql, DUCKDB), params).fetchall()

    async def get_historical_revenue(
        self,
        period_type: str,
        weeks_back: int = 4,
        sales_type: str = "retail"
    ) -> Dict[str, Any]:
        """Historical revenue for goal calculation, from Silver.

        Args:
            period_type: 'daily', 'weekly', or 'monthly'
            weeks_back: Number of weeks of history to analyze
            sales_type: 'retail', 'b2b', or 'all'
        """
        from core.sql_dialect import TODAY_IN_KYIV

        params: List[Any] = []
        where = ["NOT s.is_return", "s.is_active_source"]
        if sales_type != "all":
            where.append("s.sales_type = ?")
            params.append(sales_type)

        if period_type == "daily":
            grain = "s.order_date"
            floor = f"{TODAY_IN_KYIV} - INTERVAL '{int(weeks_back * 7)} days'"
            ceiling = TODAY_IN_KYIV
        elif period_type == "weekly":
            grain = "DATE_TRUNC('week', s.order_date)"
            floor = f"{TODAY_IN_KYIV} - INTERVAL '{int(weeks_back * 7)} days'"
            ceiling = f"DATE_TRUNC('week', {TODAY_IN_KYIV})"
        else:  # monthly
            months_back = max(3, weeks_back // 4)
            grain = "DATE_TRUNC('month', s.order_date)"
            floor = f"{TODAY_IN_KYIV} - INTERVAL '{int(months_back)} months'"
            ceiling = f"DATE_TRUNC('month', {TODAY_IN_KYIV})"

        window = [f"s.order_date >= {floor}", f"s.order_date < {ceiling}"]
        where_sql = " AND ".join(window + where)

        rows = await self._goals_run(
            _GOALS_PERIOD_REVENUE_SQL
                .replace("{grain}", grain)
                .replace("{where}", where_sql),
            params)
        result = rows[0] if rows else (None,) * 5

        avg_revenue = float(result[0] or 0)
        min_revenue = float(result[1] or 0)
        max_revenue = float(result[2] or 0)
        period_count = result[3] or 0
        std_dev = float(result[4] or 0)

        # Trend: the two most recent weeks against the ones before them.
        trend = 0.0
        if period_type == "weekly" and period_count >= 4:
            trend_rows = await self._goals_run(
                _GOALS_WEEKLY_TREND_SQL.replace("{where}", where_sql), params)
            trend_result = trend_rows[0] if trend_rows else (None, None)
            recent = float(trend_result[0] or 0)
            older = float(trend_result[1] or 0)
            if older > 0:
                trend = ((recent - older) / older) * 100

        return {
            "periodType": period_type,
                "average": round(avg_revenue, 2),
                "min": round(min_revenue, 2),
                "max": round(max_revenue, 2),
                "periodCount": period_count,
                "stdDev": round(std_dev, 2),
                "trend": round(trend, 1),  # % change
                "weeksAnalyzed": weeks_back
            }

    async def calculate_suggested_goals(
        self,
        sales_type: str = "retail",
        growth_factor: float = 1.10
    ) -> Dict[str, Dict[str, Any]]:
        """
        Calculate suggested goals based on historical performance.

        Uses average of past 4 weeks × growth factor.

        Args:
            sales_type: 'retail', 'b2b', or 'all'
            growth_factor: Target growth multiplier (1.10 = 10% growth)

        Returns:
            Suggested goals for daily, weekly, and monthly periods
        """
        suggestions = {}

        for period_type in ["daily", "weekly", "monthly"]:
            history = await self.get_historical_revenue(period_type, weeks_back=4, sales_type=sales_type)

            # Base suggestion on average + growth factor
            suggested = history["average"] * growth_factor

            # Round to nice numbers
            if period_type == "daily":
                # Round to nearest 10K
                suggested = round(suggested / 10000) * 10000
            elif period_type == "weekly":
                # Round to nearest 50K
                suggested = round(suggested / 50000) * 50000
            else:  # monthly
                # Round to nearest 100K
                suggested = round(suggested / 100000) * 100000

            # Ensure minimum reasonable goal
            min_goals = {"daily": 50000, "weekly": 300000, "monthly": 1000000}
            suggested = max(suggested, min_goals[period_type])

            suggestions[period_type] = {
                "suggested": suggested,
                "basedOnAverage": history["average"],
                "growthFactor": growth_factor,
                "trend": history["trend"],
                "confidence": "high" if history["periodCount"] >= 4 else "medium" if history["periodCount"] >= 2 else "low"
            }

        return suggestions

    async def get_goals(self, sales_type: str = "retail") -> Dict[str, Dict[str, Any]]:
        """
        Get current revenue goals.

        Returns stored goals if set, otherwise returns calculated suggestions.

        Args:
            sales_type: 'retail', 'b2b', or 'all'

        Returns:
            Goals for daily, weekly, and monthly periods
        """
        results = await self._goals_run(_STORED_GOALS_SQL)
        stored_goals = {
            row[0]: {
                "amount": float(row[1]),
                "isCustom": row[2],
                "calculatedGoal": float(row[3]) if row[3] else None,
                "growthFactor": float(row[4]) if row[4] else 1.10
            }
            for row in results
        }

        # Calculate current suggestions
        suggestions = await self.calculate_suggested_goals(sales_type)

        # Merge stored goals with suggestions
        goals = {}
        for period_type in ["daily", "weekly", "monthly"]:
            if period_type in stored_goals:
                stored = stored_goals[period_type]
                goals[period_type] = {
                    "amount": stored["amount"],
                    "isCustom": stored["isCustom"],
                    "suggestedAmount": suggestions[period_type]["suggested"],
                    "basedOnAverage": suggestions[period_type]["basedOnAverage"],
                    "trend": suggestions[period_type]["trend"],
                    "confidence": suggestions[period_type]["confidence"]
                }
            else:
                # No stored goal - use suggestion
                goals[period_type] = {
                    "amount": suggestions[period_type]["suggested"],
                    "isCustom": False,
                    "suggestedAmount": suggestions[period_type]["suggested"],
                    "basedOnAverage": suggestions[period_type]["basedOnAverage"],
                    "trend": suggestions[period_type]["trend"],
                    "confidence": suggestions[period_type]["confidence"]
                }

        return goals

    async def set_goal(
        self,
        period_type: str,
        amount: float,
        is_custom: bool = True,
        growth_factor: float = 1.10
    ) -> Dict[str, Any]:
        """
        Set a revenue goal.

        Args:
            period_type: 'daily', 'weekly', or 'monthly'
            amount: Goal amount in UAH
            is_custom: True if manually set, False if auto-calculated
            growth_factor: Growth factor used for calculation

        Returns:
            Updated goal data
        """
        if period_type not in ["daily", "weekly", "monthly"]:
            raise ValueError(f"Invalid period_type: {period_type}")

        # Calculate what the system would suggest (for reference)
        suggestions = await self.calculate_suggested_goals()
        calculated = suggestions[period_type]["suggested"]

        # Chain 7a: `KS_WRITE_GOALS` decides which store holds the number a
        # human typed. Routed here and not in the route, because
        # `reset_goal_to_auto` writes through this method too. See
        # `core/pg_goals_write.py`.
        from core import pg_goals_write

        if pg_goals_write.writes_postgres():
            await pg_goals_write.set_goal(
                period_type, amount, is_custom, calculated, growth_factor,
                datetime.now(DEFAULT_TZ))
        else:
            async with self.connection() as conn:
                now = datetime.now(DEFAULT_TZ)
                conn.execute("""
                    INSERT INTO revenue_goals (period_type, goal_amount, is_custom, calculated_goal, growth_factor, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT (period_type) DO UPDATE SET
                        goal_amount = excluded.goal_amount,
                        is_custom = excluded.is_custom,
                        calculated_goal = excluded.calculated_goal,
                        growth_factor = excluded.growth_factor,
                        updated_at = excluded.updated_at
                """, [period_type, amount, is_custom, calculated, growth_factor, now])

        return {
            "periodType": period_type,
            "amount": amount,
            "isCustom": is_custom,
            "calculatedGoal": calculated,
            "growthFactor": growth_factor
        }

    async def reset_goal_to_auto(self, period_type: str, sales_type: str = "retail") -> Dict[str, Any]:
        """
        Reset a goal to auto-calculated value.

        Args:
            period_type: 'daily', 'weekly', or 'monthly'
            sales_type: 'retail', 'b2b', or 'all'

        Returns:
            Updated goal data
        """
        suggestions = await self.calculate_suggested_goals(sales_type)
        suggested = suggestions[period_type]["suggested"]

        return await self.set_goal(
            period_type=period_type,
            amount=suggested,
            is_custom=False,
            growth_factor=1.10
        )

    # ─── Smart Seasonality Methods ─────────────────────────────────────────────
    #
    # Two halves, and only the second one writes (OD-14 (i)).
    #
    # The calculators compute and return. Nothing they do stores anything, so
    # the GETs that call them — `/goals/seasonality`, `/growth`,
    # `/weekly-patterns`, and `/goals/smart` and `/goals/forecast` through
    # `generate_smart_goals` — cannot write: by construction now, rather than
    # by a `persist=False` each caller had to remember to pass. Until chain 7b
    # one of them did not have to: `generate_smart_goals` recomputed and
    # stored all three tables whenever `seasonal_indices` held fewer than
    # twelve rows, for any viewer and with whatever sales_type the viewer
    # asked for.
    #
    # `recalculate_goal_tables` is the one writer, reached from the Monday job
    # and from `POST /goals/recalculate`. It computes everything first and
    # then persists it in ONE transaction. They were two writers in a fixed
    # order, and the YoY half was twelve autocommitted UPDATEs after twelve
    # autocommitted upserts, so a crash or a refused read between them left
    # this week's indices beside last week's `yoy_growth` and `growth_metrics`.
    #
    # Every history read below reads Silver through `_goals_run`, after
    # `_refuse_a_retired_history()`; the body each hands its reader is a
    # module constant a walk can read. Until chain 7b-4 each had a second
    # body — the DN-12 bridge over DuckDB `orders` — chosen by
    # `KS_GOALS_HISTORY`.

    async def calculate_seasonality_indices(
        self, sales_type: str = "retail",
    ) -> Dict[int, Dict[str, Any]]:
        """Monthly seasonality indices from the whole history. Reads only.

        Analyzes all available historical data to determine how each month
        performs relative to the annual average. Stored only by
        `recalculate_goal_tables`.

        Args:
            sales_type: 'retail', 'b2b', 'internal', 'exhibition' or 'all'

        Returns:
            Dictionary mapping month (1-12) to seasonality data
        """
        _refuse_a_retired_history()
        where, params = _silver_history_where(sales_type)
        return _seasonality_answer(await self._goals_run(
            _SILVER_SEASONALITY_SQL.replace("{where}", where), params))

    async def calculate_yoy_growth(self, sales_type: str = "retail") -> Dict[str, Any]:
        """Year-over-year growth, overall and per month. Reads only.

        Compares revenue between consecutive full years to determine the
        growth trend. Stored only by `recalculate_goal_tables`.

        Args:
            sales_type: 'retail', 'b2b', 'internal', 'exhibition' or 'all'

        Returns:
            Growth metrics including overall YoY, monthly YoY, and trend slope
        """
        answer, _overall = await self._yoy_growth(sales_type)
        return answer

    async def _yoy_growth(self, sales_type: str) -> Tuple[Dict[str, Any], float]:
        """`(answer, overall)` — `overall` unrounded, as it is stored."""
        # Only full years (11 months with orders or more), and never the
        # current Kyiv year. The second half is what the comment here always
        # said and the query never did: from the first order dated 1 November
        # the running year has eleven months and counted as "full", a second
        # pair of years appeared, and the weighted average below — DuckDB
        # DECIMALs times float weights — raised TypeError. The Monday job
        # would have failed from 2026-11-02, and `GET /goals/growth` with it.
        this_year = date(datetime.now(DEFAULT_TZ).year, 1, 1)

        _refuse_a_retired_history()
        where, params = _silver_history_where(sales_type)
        yearly = await self._goals_run(
            _SILVER_YEARLY_SQL.replace("{where}", where), params + [this_year])
        monthly = await self._goals_run(
            _SILVER_MONTHLY_YOY_SQL.replace("{where}", where), params)
        return _yoy_answer(yearly, monthly)

    async def _history_bounds(self) -> Tuple[Optional[date], Optional[date]]:
        """The first and last Kyiv order date anywhere in the history —
        `growth_metrics.period_start`/`period_end`."""
        _refuse_a_retired_history()
        rows = await self._goals_run(_SILVER_BOUNDS_SQL)
        return (rows[0][0], rows[0][1]) if rows else (None, None)

    async def calculate_weekly_patterns(
        self, sales_type: str = "retail",
    ) -> Dict[int, Dict[int, float]]:
        """How revenue distributes across the weeks of each month. Reads only.

        Stored only by `recalculate_goal_tables(include_weekly=True)` —
        `POST /goals/recalculate`; the Monday job does not store them (OQ-2).

        Args:
            sales_type: 'retail', 'b2b', 'internal', 'exhibition' or 'all'

        Returns:
            Dictionary mapping month -> week_of_month -> weight (percentage)
        """
        patterns, _rows = await self._weekly_patterns(sales_type)
        return patterns

    async def _weekly_patterns(self, sales_type: str):
        """`(patterns, rows)` — `rows` are `[month, week, weight, sample_size]`
        as stored: the measured weeks only, the weight unrounded."""
        _refuse_a_retired_history()
        where, params = _silver_history_where(sales_type)
        return _weekly_answer(await self._goals_run(
            _SILVER_WEEKLY_SQL.replace("{where}", where), params))

    async def _compute_goal_tables(self, *, include_weekly: bool) -> Dict[str, Any]:
        """Every number the shared tables will hold, read and computed, and
        nothing written. Retail: the tables carry no sales_type, so the rows
        mean retail (`GOAL_TABLES_SALES_TYPE`)."""
        sales_type = GOAL_TABLES_SALES_TYPE
        seasonal = await self.calculate_seasonality_indices(sales_type)
        yoy, overall = await self._yoy_growth(sales_type)
        bounds = await self._history_bounds()
        weekly_rows = None
        if include_weekly:
            _patterns, weekly_rows = await self._weekly_patterns(sales_type)
        return {"seasonal": seasonal, "yoy": yoy, "yoy_overall": overall,
                "history_bounds": bounds, "weekly_rows": weekly_rows}

    async def _persist_goal_tables(
        self, tables: Dict[str, Any], now: Optional[datetime] = None,
    ) -> None:
        """Store what `_compute_goal_tables` computed, in one transaction.

        The statements are the ones the calculators ran when they stored for
        themselves, in the order the Monday job and the POST ran them — the
        indices, then the YoY over them, then the weekly weights — so the rows
        are what they were. What changed is that a reader sees the old set or
        the new one: a failure anywhere rolls the whole set back.

        Two rules carried over exactly. `yoy_growth` is set only for months
        with a pair of years, so a month without one keeps the value it had
        (the upsert of the indices never touches the column). And the 0.10
        overall placeholder — "no pair of full years", not a measurement — is
        written only where there is nothing better to keep: before DN-12 an
        emptied history wrote it over the measured rate, and the hourly full
        replace carried the guess into Postgres.
        """
        now = now or datetime.now(DEFAULT_TZ)
        seasonal = tables["seasonal"]
        yoy = tables["yoy"]
        start, end = tables["history_bounds"]
        seasonal_rows = [
            [month, data["seasonality_index"], data["sample_size"],
             data["avg_revenue"], data["min_revenue"], data["max_revenue"],
             data["confidence"], now]
            for month, data in seasonal.items()
        ]
        weekly_rows = [list(row) + [now] for row in (tables.get("weekly_rows") or [])]

        # Chain 7b-3 (`KS_WRITE_FORECAST`): the same rows, the same statements
        # and the same placeholder rule, in one Postgres transaction. Routed
        # here, where the Monday job and the POST both arrive, rather than in
        # either of them. Asked after everything is computed, so a flag nobody
        # can read raises with nothing written in either store.
        from core import pg_forecast_write

        if pg_forecast_write.writes_postgres():
            await pg_forecast_write.persist_goal_tables(
                [row[:7] for row in seasonal_rows],
                (tables["yoy_overall"], start, end, yoy["sample_size"]),
                yoy["monthly_yoy"],
                [row[:4] for row in weekly_rows],
                now)
            return

        async with self.connection() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                if seasonal_rows:
                    conn.executemany(_SEASONAL_UPSERT_SQL, seasonal_rows)

                has_row = conn.execute(_YOY_OVERALL_EXISTS_SQL).fetchone() is not None
                if yoy["sample_size"] or not has_row:
                    conn.execute(_YOY_OVERALL_UPSERT_SQL, [
                        tables["yoy_overall"], start, end, yoy["sample_size"], now])
                else:
                    logger.warning(
                        "YoY: no pair of full years in the %s history; keeping "
                        "the stored yoy_overall rather than overwriting it with "
                        "the %.2f fallback", GOAL_TABLES_SALES_TYPE,
                        tables["yoy_overall"],
                    )
                for month, value in yoy["monthly_yoy"].items():
                    conn.execute(_MONTHLY_YOY_UPDATE_SQL, [value, now, month])

                if weekly_rows:
                    conn.executemany(_WEEKLY_UPSERT_SQL, weekly_rows)
                conn.execute("COMMIT")
            except Exception:
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass
                raise

    async def recalculate_goal_tables(self, *, include_weekly: bool) -> Dict[str, Any]:
        """Recompute the shared goal tables and store them — the one writer.

        `include_weekly=False` is the Monday job (seasonality and YoY, what it
        has always stored); `True` is `POST /goals/recalculate`, which stores
        the weekly weights too. Everything is read before anything is written,
        so a read refused under `KS_READ_FALLBACK=off` — or any other failure
        while computing — leaves every table as it was.
        """
        tables = await self._compute_goal_tables(include_weekly=include_weekly)
        await self._persist_goal_tables(tables)
        return tables

    async def _forecast_actual_revenue(
        self, start: date, end: date, sales_type: str = "retail"
    ) -> float:
        """Gold revenue from `start` to `end` inclusive — the part of a month
        that has already happened.

        `retail` and `b2b` narrow to their own rows; any other value sums every
        row, which is what this read has always done (an `'all'` smart goal is
        the whole business). The dates go over as `date` objects: DuckDB
        coerces an ISO string, asyncpg refuses one.
        """
        if sales_type in ("retail", "b2b"):
            sales_filter, params = "sales_type = ?", [start, end, sales_type]
        else:
            sales_filter, params = "1=1", [start, end]
        rows = await self._goals_run(
            _FORECAST_ACTUAL_SQL.replace("{sales_filter}", sales_filter), params)
        return float(rows[0][0]) if rows and rows[0][0] is not None else 0.0

    async def _get_ml_forecast_total(
        self, target_year: int, target_month: int, sales_type: str = "retail"
    ) -> float:
        """Estimate full-month revenue using actual data + ML predictions.

        For past days (before today): actual revenue from Gold.
        For today and future days: ML predictions from revenue_predictions.
        Returns 0 if no predictions are available for the future portion, or
        if a read fails on the engine that finally answers it — the signal is
        then absent from the blend. A Postgres failure alone does not return
        0: that read falls back to DuckDB, and the sum may then take its two
        halves from two engines (see `_FORECAST_ACTUAL_SQL`). An unknown
        `KS_READ_GOALS` raises, as it does for every goal read. Under chain
        7b-3 the forecast half has no fallback, and its failure is a refusal
        that passes through like any other (`_goals_run`), never a 0.

        **Called without the store lock.** Both halves go through `_goals_run`,
        which takes that lock itself on the DuckDB path and the lock is not
        reentrant — so `generate_smart_goals` asks for this before it opens its
        own connection. It used to take that connection as an argument and read
        DuckDB Gold on it, which after step 13 would have been a frozen Gold.
        """
        from core import pg_goals_read

        first_day = date(target_year, target_month, 1)
        last_day = date(target_year, target_month, calendar.monthrange(target_year, target_month)[1])
        today = datetime.now(DEFAULT_TZ).date()

        # Asked here, outside the `try`, and only for its refusal: an unknown
        # `KS_READ_GOALS` raises, and every other goal read lets that stop it.
        # Inside the `try` the refusal became a missing signal, and
        # `/api/goals/forecast` answered 200 with a different goal.
        pg_goals_read.enabled()

        try:
            # Get actual revenue for past days of the month
            actual_revenue = 0.0
            if first_day < today:
                actual_end = min(today - timedelta(days=1), last_day)
                actual_revenue = await self._forecast_actual_revenue(
                    first_day, actual_end, sales_type)

            # Get predicted revenue for today onwards
            predict_start = max(first_day, today)
            predicted_rows = await self._goals_run(
                _FORECAST_PREDICTED_SQL, [sales_type, predict_start, last_day])
            predicted_revenue = (
                float(predicted_rows[0][0])
                if predicted_rows and predicted_rows[0][0] else 0.0
            )

            if predicted_revenue == 0.0:
                return 0.0  # No predictions available

            return actual_revenue + predicted_revenue
        except read_fallback.ReadUnavailable:
            # A fallback refused (DN-20b, `KS_READ_FALLBACK=off`) is not a
            # missing signal: it passes through, so the goal is not computed
            # from two signals while the third reads a store nobody writes.
            # It reaches the route, which answers 503 naming `goals`; DN-20a's
            # walk requires the decision to be written here, as for every
            # handler around a router.
            raise
        except Exception as exc:  # noqa: BLE001
            # What reaches here is a read that failed on the engine that
            # finally answered — the goal is then computed without this signal,
            # so it is said at WARNING, not DEBUG.
            logger.warning(
                "goals: ML forecast signal for %d-%02d unavailable, left out of "
                "the blend: %s", target_year, target_month, exc)
            return 0.0

    async def _dynamic_growth_cap(
        self, target_month: int, sales_type: str = "retail"
    ) -> float:
        """Calculate per-month dynamic growth cap from historical YoY variance.

        Returns max(0.10, min(0.50, avg_yoy + 1.5 * stddev_yoy)).
        Falls back to 0.35 if insufficient data.

        **Called without the store lock**, like `_get_ml_forecast_total`: it
        reads through `_goals_run`, which takes the lock itself on the DuckDB
        path, and that lock is not reentrant — so `generate_smart_goals` asks
        for it before it opens its own. It used to take that connection as an
        argument.
        """
        _refuse_a_retired_history()
        where, params = _silver_history_where(sales_type)
        rows = await self._goals_run(
            _SILVER_GROWTH_CAP_SQL.replace("{where}", where),
            [target_month] + params)
        return _growth_cap(rows[0] if rows else None)

    async def _last_year_month_revenue(
        self, target_year: int, target_month: int, sales_type: str,
    ) -> float:
        """The target month a year earlier — signal 1's baseline."""
        _refuse_a_retired_history()
        where, params = _silver_history_where(sales_type)
        first = date(target_year - 1, target_month, 1)
        rows = await self._goals_run(
            _SILVER_LAST_YEAR_SQL.replace("{where}", where),
            [first, _first_of_next_month(first)] + params)
        value = rows[0][0] if rows else None
        return float(value or 0) if value else 0

    async def _recent_three_month_average(self, sales_type: str) -> float:
        """The last three complete months with at least 25 days of orders —
        signal 2's baseline.

        "Complete" is bounded by the first of the current month **in Kyiv**,
        computed here rather than as `DATE_TRUNC('month', CURRENT_DATE)`: the
        engines disagree about what day `CURRENT_DATE` is for three hours out
        of twenty-four (`core/pg_goals_read.py`), and so does a test runner in
        UTC against the warehouse's Kyiv dates. The web container runs
        `TZ=Europe/Kyiv`, where the two are the same day — nothing moves in
        production.
        """
        month_start = datetime.now(DEFAULT_TZ).date().replace(day=1)
        _refuse_a_retired_history()
        where, params = _silver_history_where(sales_type)
        rows = await self._goals_run(
            _SILVER_RECENT_MONTHS_SQL.replace("{where}", where),
            params + [month_start])
        value = rows[0][0] if rows else None
        return float(value or 0) if value else 0

    async def generate_smart_goals(
        self,
        target_year: int,
        target_month: int,
        sales_type: str = "retail",
    ) -> Dict[str, Any]:
        """
        Generate smart goals for a target month using seasonality and growth.

        Reads only (OD-14 (i)). It used to recompute and store the three shared
        tables first whenever `seasonal_indices` held fewer than twelve rows, or
        when asked to with `recalculate=True` — for any viewer of
        `/goals/smart`, with the viewer's own sales_type. Storing them is
        `recalculate_goal_tables`' alone now; a table that is short is read as
        it stands, and each month without a row falls back the way it always
        did (index 1.0, YoY at the cap, confidence `low`, default weekly
        weights).

        Algorithm:
        1. Get last year's same month revenue as baseline
        2. Apply YoY growth rate
        3. Adjust using seasonality index
        4. Calculate weekly breakdown using weekly patterns

        Args:
            target_year: Year to generate goals for
            target_month: Month (1-12) to generate goals for
            sales_type: 'retail', 'b2b', 'internal', 'exhibition' or 'all'

        Returns:
            Smart goals with monthly total and weekly breakdown
        """
        # ── The revenue history, each read before the connection below: they
        # take the store lock themselves (or go through `_goals_run`, which
        # does on the DuckDB path), and it is not reentrant.
        ml_forecast_goal = await self._get_ml_forecast_total(
            target_year, target_month, sales_type)
        dynamic_cap = await self._dynamic_growth_cap(target_month, sales_type)
        last_year_revenue = await self._last_year_month_revenue(
            target_year, target_month, sales_type)
        recent_3_month_avg = await self._recent_three_month_average(sales_type)

        # ── The three shared tables, in one statement: their writer stores
        # them in one transaction, so this reads one set of them, from
        # whichever store chain 7b-3 says writes them. Asked after the
        # history reads above, which take DuckDB's store lock themselves on
        # their DuckDB path — and so does this one; it is not reentrant.
        shared = await self._goal_tables_run(
            _SHARED_GOAL_TABLES_SQL, [target_month, target_month])
        seasonality_result = next(
            ((r[2], r[3], r[4], r[5]) for r in shared if r[0] == "seasonal"), None)
        yoy_result = next(((r[2],) for r in shared if r[0] == "growth"), None)
        # Sorted here, not by the statement: a UNION's order is the engine's,
        # and the week order is part of the answer — the residual below goes
        # to `max(...)`, which breaks a tie by insertion order.
        weekly_patterns = sorted(
            ((int(r[1]), r[2]) for r in shared if r[0] == "weekly"),
            key=lambda row: row[0])

        if seasonality_result:
            seasonality_index = float(seasonality_result[0] or 1.0)
            historical_avg = float(seasonality_result[1] or 0)
            monthly_yoy = float(seasonality_result[2] or dynamic_cap)
            confidence = seasonality_result[3] or "low"
        else:
            seasonality_index = 1.0
            historical_avg = 0
            monthly_yoy = dynamic_cap
            confidence = "low"

        # Overall YoY growth
        overall_yoy = float(yoy_result[0] or dynamic_cap) if yoy_result else dynamic_cap

        # Apply growth rate with dynamic cap
        raw_growth_rate = monthly_yoy if monthly_yoy > 0 else overall_yoy
        growth_rate = min(raw_growth_rate, dynamic_cap)

        # ── Signal 1: YoY growth (last year same month × growth)
        yoy_goal = 0.0
        if last_year_revenue > 0:
            yoy_goal = last_year_revenue * (1 + growth_rate)

        # ── Signal 2: Recent baseline adjusted for seasonality
        recent_goal = 0.0
        if recent_3_month_avg > 0 and seasonality_index > 0:
            recent_goal = recent_3_month_avg * seasonality_index

        # ── Signal 3: ML forecast — read before the connection was taken.

        # ── Weighted blend (replaces MAX)
        # Confidence-based weight tables
        weight_tables = {
            "high":   {"yoy": 0.40, "recent": 0.30, "ml": 0.30},
            "medium": {"yoy": 0.30, "recent": 0.35, "ml": 0.35},
            "low":    {"yoy": 0.20, "recent": 0.40, "ml": 0.40},
        }
        base_weights = weight_tables.get(confidence, weight_tables["low"])

        # Build signal dict (only non-zero signals)
        signals = {}
        if yoy_goal > 0:
            signals["yoy"] = yoy_goal
        if recent_goal > 0:
            signals["recent"] = recent_goal
        if ml_forecast_goal > 0:
            signals["ml"] = ml_forecast_goal

        if signals:
            # Redistribute unavailable signal weights proportionally
            available_weight = sum(base_weights[k] for k in signals)
            blend_weights = {k: base_weights[k] / available_weight for k in signals}

            monthly_goal = sum(signals[k] * blend_weights[k] for k in signals)

            # Determine primary method
            dominant = max(blend_weights, key=lambda k: blend_weights[k] * signals[k])
            method_map = {"yoy": "yoy_growth", "recent": "recent_trend", "ml": "ml_forecast"}
            calculation_method = method_map[dominant]
        elif historical_avg > 0:
            monthly_goal = historical_avg * (1 + growth_rate)
            calculation_method = "historical_avg"
            blend_weights = {}
        else:
            monthly_goal = 3000000  # 3M UAH default
            growth_rate = dynamic_cap
            calculation_method = "fallback"
            blend_weights = {}

        # Round to nice number
        monthly_goal = round(monthly_goal / 100000) * 100000

        if weekly_patterns:
            weekly_weights = {int(row[0]): float(row[1]) for row in weekly_patterns}
        else:
            weekly_weights = dict(_DEFAULT_WEEKLY_WEIGHTS)

        # Normalize weights to sum to 1
        total_weight = sum(weekly_weights.values())
        if total_weight > 0:
            weekly_weights = {k: v / total_weight for k, v in weekly_weights.items()}

        # Calculate weekly goals with residual adjustment
        weekly_goals = {
            week: round(monthly_goal * weight / 10000) * 10000
            for week, weight in weekly_weights.items()
        }
        # Distribute residual so weekly goals sum exactly to monthly
        residual = monthly_goal - sum(weekly_goals.values())
        if residual != 0 and weekly_goals:
            # Add residual to the largest week
            largest_week = max(weekly_goals, key=lambda k: weekly_goals[k])
            weekly_goals[largest_week] += residual

        # Fix: use calendar.monthrange for correct days (handles leap years)
        days_in_month = calendar.monthrange(target_year, target_month)[1]
        daily_goal = round(monthly_goal / days_in_month / 10000) * 10000

        # Calculate weekly goal (monthly / actual weeks)
        weeks_in_month = days_in_month / 7.0
        weekly_goal = round(monthly_goal / weeks_in_month / 50000) * 50000

        return {
            "targetYear": target_year,
            "targetMonth": target_month,
            "monthly": {
                "goal": monthly_goal,
                "lastYearRevenue": round(last_year_revenue, 2),
                "recent3MonthAvg": round(recent_3_month_avg, 2),
                "historicalAvg": round(historical_avg, 2),
                "yoyGoal": round(yoy_goal, 2),
                "recentGoal": round(recent_goal, 2),
                "mlForecastGoal": round(ml_forecast_goal, 2),
                "growthRate": round(growth_rate, 4),
                "growthCap": round(dynamic_cap, 4),
                "seasonalityIndex": seasonality_index,
                "confidence": confidence,
                "calculationMethod": calculation_method,
                "blendWeights": {k: round(v, 4) for k, v in blend_weights.items()} if blend_weights else None,
            },
            "weekly": {
                "goal": weekly_goal,  # Average weekly goal
                "breakdown": weekly_goals,
                "weights": weekly_weights
            },
            "daily": {
                "goal": daily_goal,
                "daysInMonth": days_in_month
            },
            "metadata": {
                "overallYoY": round(overall_yoy, 4),
                "monthlyYoY": round(monthly_yoy, 4),
                "calculatedAt": datetime.now(DEFAULT_TZ).isoformat()
            }
        }

    async def get_smart_goals(
        self,
        sales_type: str = "retail",
        year: int | None = None,
        month: int | None = None,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Get smart goals for a specific or current month using seasonality.

        Args:
            sales_type: 'retail', 'b2b', or 'all'
            year: Target year (defaults to current)
            month: Target month 1-12 (defaults to current)

        Returns:
            Goals for daily, weekly, and monthly periods with calculation details
        """
        now = datetime.now(DEFAULT_TZ)
        target_year = year if year is not None else now.year
        target_month = month if month is not None else now.month

        # Generate smart goals for target month
        smart = await self.generate_smart_goals(target_year, target_month, sales_type)

        # Check for custom overrides
        # The same body as `get_goals`, narrowed in Python rather than in SQL:
        # one statement is one thing to keep identical between two engines, and
        # `is_custom` is a boolean in Postgres and an integer in DuckDB — a
        # predicate on it is exactly the sort of thing that would have needed a
        # dialect of its own.
        stored_goals = await self._goals_run(_STORED_GOALS_SQL)
        custom_goals = {row[0]: float(row[1]) for row in stored_goals if row[2]}

        return {
            "daily": {
                "amount": custom_goals.get("daily", smart["daily"]["goal"]),
                "isCustom": "daily" in custom_goals,
                "suggestedAmount": smart["daily"]["goal"],
                "basedOnAverage": smart["monthly"]["historicalAvg"] / smart["daily"]["daysInMonth"],
                "trend": smart["metadata"]["monthlyYoY"] * 100,
                "confidence": smart["monthly"]["confidence"]
            },
            "weekly": {
                "amount": custom_goals.get("weekly", smart["weekly"]["goal"]),
                "isCustom": "weekly" in custom_goals,
                "suggestedAmount": smart["weekly"]["goal"],
                "basedOnAverage": smart["monthly"]["historicalAvg"] / (smart["daily"]["daysInMonth"] / 7.0),
                "trend": smart["metadata"]["monthlyYoY"] * 100,
                "confidence": smart["monthly"]["confidence"],
                "weeklyBreakdown": smart["weekly"]["breakdown"]
            },
            "monthly": {
                "amount": custom_goals.get("monthly", smart["monthly"]["goal"]),
                "isCustom": "monthly" in custom_goals,
                "suggestedAmount": smart["monthly"]["goal"],
                "basedOnAverage": smart["monthly"]["historicalAvg"],
                "lastYearRevenue": smart["monthly"]["lastYearRevenue"],
                "recent3MonthAvg": smart["monthly"]["recent3MonthAvg"],
                "growthRate": smart["monthly"]["growthRate"],
                "seasonalityIndex": smart["monthly"]["seasonalityIndex"],
                "trend": smart["metadata"]["monthlyYoY"] * 100,
                "confidence": smart["monthly"]["confidence"],
                "calculationMethod": smart["monthly"]["calculationMethod"],
                "mlForecastGoal": smart["monthly"]["mlForecastGoal"],
                "blendWeights": smart["monthly"]["blendWeights"],
            },
            "metadata": smart["metadata"]
        }

    # ─── Stats Methods ────────────────────────────────────────────────────────

    async def store_predictions(
        self,
        predictions: List[Dict],
        sales_type: str = "retail",
        metrics: Optional[Dict[str, float]] = None,
    ) -> int:
        """Store revenue predictions where chain 7b-3 says they are written.

        DuckDB, or under `KS_WRITE_FORECAST` (`core/pg_forecast_write.py`)
        Postgres: the same DELETE over the range and INSERT, in one
        transaction, stamped with this call's clock.

        Args:
            predictions: List of dicts with 'date' and 'predicted_revenue' keys.
            sales_type: 'retail' or 'b2b'.
            metrics: Model metrics dict with 'mae', 'mape', and 'wape'.

        Returns:
            Number of predictions stored.
        """
        if not predictions:
            return 0

        from core import pg_forecast_write

        if pg_forecast_write.writes_postgres():
            from datetime import timezone

            return await pg_forecast_write.store_predictions(
                predictions, sales_type, metrics, datetime.now(timezone.utc))

        mae = metrics.get('mae', 0) if metrics else 0
        mape = metrics.get('mape', 0) if metrics else 0
        wape = metrics.get('wape', 0) if metrics else 0

        async with self.connection() as conn:
            dates = [p['date'] for p in predictions]
            min_date = min(dates)
            max_date = max(dates)

            # One transaction: as two autocommit statements, a kill between the
            # DELETE and the INSERT — at peak memory, right after training —
            # left the range empty, the month view lost its forecast, and boot
            # did not repair it because the model file was still there.
            conn.execute("BEGIN TRANSACTION")
            try:
                conn.execute(
                    """DELETE FROM revenue_predictions
                       WHERE sales_type = ? AND prediction_date >= ? AND prediction_date <= ?""",
                    [sales_type, min_date, max_date]
                )
                conn.executemany(
                    """INSERT INTO revenue_predictions
                       (prediction_date, sales_type, predicted_revenue, model_mae, model_mape, model_wape)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    [[pred['date'], sales_type, pred['predicted_revenue'], mae, mape, wape]
                     for pred in predictions]
                )
                conn.execute("COMMIT")
            except Exception:
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass
                raise

            logger.info(f"Stored {len(predictions)} predictions for {sales_type}")
            return len(predictions)

    async def get_predictions(
        self,
        start_date: date,
        end_date: date,
        sales_type: str = "retail",
    ) -> List[Dict]:
        """Stored revenue predictions for a date range, from whichever engine.

        The last read of this tab to move, and it could not until revision 0025
        gave `revenue_predictions` a Postgres home. `generate_smart_goals`,
        which the audit filed beside it, was not a read at all until chain
        7b-1: it recomputed the three seasonality tables in DuckDB and read
        them back. It reads only now, through `_goal_tables_run`, from the
        store chain 7b-3 says writes them. This read follows the same chain
        through `_goals_run`: Postgres with no fallback while it writes there,
        `KS_READ_GOALS` while it does not.
        """
        rows = await self._goals_run(
            _PREDICTIONS_SQL, [sales_type, start_date, end_date])

        return [
            {
                'date': row[0].isoformat() if hasattr(row[0], 'isoformat') else str(row[0]),
                'predicted_revenue': float(row[1]),
                'model_mae': float(row[2]) if row[2] else 0,
                'model_mape': float(row[3]) if row[3] else 0,
                'model_wape': float(row[4]) if row[4] else 0,
            }
            for row in rows
        ]

    async def get_daily_revenue_for_dates(
        self,
        dates: list,
        sales_type: str = "retail",
    ) -> Dict[date, float]:
        """Daily revenue for a list of specific dates, from Silver.

        Returns dict mapping date -> revenue total.
        Used for extending comparison data to cover forecast dates.
        """
        if not dates:
            return {}

        # The dates go over as `date` objects, not as ISO strings. DuckDB
        # coerces a string to a DATE without being asked; asyncpg encodes by
        # the type Postgres inferred and raises `invalid input for query
        # argument … 'str' object has no attribute 'toordinal'`. Under the
        # flag that fault is not visible: the read falls back to DuckDB, logs
        # an ERROR, and the method goes on answering — so the tab looks
        # switched and is not. Found by the differential test, which is the
        # only kind that can see it: a unit test mocks `fetch` and never meets
        # the driver.
        params: List[Any] = [
            d.date() if isinstance(d, datetime) else d for d in dates
        ]
        where = [f"s.order_date IN ({', '.join(['?'] * len(dates))})",
                 "NOT s.is_return", "s.is_active_source"]
        if sales_type != "all":
            where.append("s.sales_type = ?")
            params.append(sales_type)

        rows = await self._goals_run(
            _GOALS_DAILY_FOR_DATES_SQL.replace("{where}", " AND ".join(where)),
            params)
        return {row[0]: float(row[1]) for row in rows}

    # ═══════════════════════════════════════════════════════════════════════════
    # MANUAL EXPENSES CRUD
    # ═══════════════════════════════════════════════════════════════════════════
