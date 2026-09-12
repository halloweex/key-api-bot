"""DuckDBStore revenue and analytics methods."""
from __future__ import annotations

import logging
from contextlib import nullcontext
from datetime import date, timedelta
from typing import Optional, List, Dict, Any, Tuple

from core.duckdb_constants import UNKNOWN_BRAND, brand_where
from core.models import OrderStatus

logger = logging.getLogger(__name__)


# The category tree, one body for both engines. Six other methods still run it
# on a connection they already hold, and the three report methods run it
# through the router — so the text lives here rather than in either of them.
_CATEGORY_TREE_SQL = """
    WITH RECURSIVE category_tree AS (
        SELECT id FROM {categories} WHERE id = ?
        UNION ALL
        SELECT c.id FROM {categories} c
        JOIN category_tree ct ON c.parent_id = ct.id
    )
    SELECT id FROM category_tree
    -- Ordered because the result becomes an `IN (...)` list that is compared
    -- between engines; unordered, the two would build the same filter from a
    -- different list and a differential test would report a difference that
    -- is not one.
    ORDER BY id
"""


# ─── The filter bar's four dropdowns ────────────────────────────────────────
#
# Not a tab: the header, drawn on every page but `/traffic`. One body each,
# two engines, and the table names are the only thing that differs — every
# hole below already existed on `Dialect`, so this port needed no change to
# it. Why they share one flag, and why their `ORDER BY` is portable, is in
# `core/pg_lookups_read.py`.

_ROOT_CATEGORIES_SQL = """
    SELECT id, name FROM {categories}
    WHERE parent_id IS NULL
    ORDER BY name
"""

_CHILD_CATEGORIES_SQL = """
    SELECT id, name FROM {categories}
    WHERE parent_id = ?
    ORDER BY name
"""

_BRANDS_SQL = """
    SELECT DISTINCT brand FROM {products}
    WHERE brand IS NOT NULL AND brand != ''
    ORDER BY brand
"""

# Deliberately a second statement rather than a `UNION` onto the list above:
# the unknown bucket is a different question (does one exist?) with a
# different answer type, and folding it in would put its placement in the
# dropdown at the mercy of the sort. `BrandFilter` sorts by label anyway.
_UNBRANDED_EXISTS_SQL = """
    SELECT EXISTS (
        SELECT 1 FROM {products} WHERE brand IS NULL OR TRIM(brand) = ''
    )
"""

_PROMOCODES_SQL = """
    SELECT DISTINCT promocode FROM {silver_orders}
    WHERE promocode IS NOT NULL AND promocode != ''
    ORDER BY promocode
"""


def _at_least_the_root(category_id: int, found: List[int]) -> List[int]:
    """Never hand back an empty list, because the caller builds `IN (...)`.

    The anchor of the recursive term is `WHERE id = ?`, so an existing
    category always yields itself and this changes nothing. A category id that
    is *not* in the table yields nothing at all, and the seven callers then
    render `category_id IN ()` — a parser error on both engines, so
    `?category_id=99999` was a 500 rather than an empty report.

    Older than this port: `_get_category_with_children` used to open with
    `result = [category_id]` and then never use it, which reads as dead code
    and is really a guard somebody wrote and then wired up wrong. Found by the
    routing test, whose fixture database has no categories in it at all.

    Returning the root keeps the filter valid and means the right thing: no
    product carries a category that does not exist, so the report is empty.
    """
    return found or [category_id]


class RevenueMixin:

    # ── Which engine answers `/reports` ────────────────────────────────────
    #
    # Three methods, all reading Silver, which Postgres has had since 0005 and
    # 0011. No migration, no replication — only a choice of engine, made
    # BEFORE any connection is taken. That ordering is `/inventory`'s §34
    # invariant: a read bound for Postgres must not first queue behind
    # DuckDB's single writer, or the flag has moved the bottleneck rather than
    # left it behind.
    #
    # **Never call these from inside `self.connection()`.** The store lock is
    # not reentrant and the deadlock does not raise — it simply never returns.
    # That is why `_category_ids` exists beside the older
    # `_get_category_with_children`: the six unported callers hold a
    # connection and pass it in; these three hold nothing.

    async def _reports_run(
        self, sql: str, params: Optional[List[Any]] = None,
    ) -> List[Tuple]:
        """Run one report query against whichever engine the flag names."""
        from core.sql_dialect import DUCKDB, POSTGRES

        from core import pg_reports_read

        params = list(params or [])
        if pg_reports_read.enabled() and pg_reports_read.available():
            try:
                return await pg_reports_read.fetch(
                    self._render_report(sql, POSTGRES), params,
                )
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "reports: Postgres failed, falling back to DuckDB: %s",
                    exc, exc_info=True,
                )

        async with self.connection() as conn:
            return conn.execute(
                self._render_report(sql, DUCKDB), params,
            ).fetchall()

    @staticmethod
    def _render_report(sql: str, dialect, **extra) -> str:
        """The holes the report and marketing bodies use.

        All the table names already existed on `Dialect`, which is why the
        `/reports` port needed no change to it. `period_measures` is the one
        that is not a name: the two Golds differ in *shape* there, and
        `core.sql_dialect.marketing_period_measures` renders that difference
        from one place.
        """
        from core.sql_dialect import render_tables

        return render_tables(sql, dialect, **extra)

    async def _marketing_run(
        self, sql: str, params: Optional[List[Any]] = None, **extra,
    ) -> List[Tuple]:
        """`_reports_run`'s twin for `/marketing`, on its own flag.

        A separate switch rather than a shared one, because the two tabs can
        fail differently and a rollback should cost one of them. `/marketing`
        also reads Gold and the goals, where `/reports` reads only Silver — so
        the surfaces genuinely differ, and so does what going wrong looks like.
        """
        from core.sql_dialect import DUCKDB, POSTGRES

        from core import pg_marketing_read

        params = list(params or [])
        if pg_marketing_read.enabled() and pg_marketing_read.available():
            try:
                return await pg_marketing_read.fetch(
                    self._render_report(sql, POSTGRES, **extra), params,
                )
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "marketing: Postgres failed, falling back to DuckDB: %s",
                    exc, exc_info=True,
                )

        async with self.connection() as conn:
            return conn.execute(
                self._render_report(sql, DUCKDB, **extra), params,
            ).fetchall()

    async def _lookups_run(
        self, sql: str, params: Optional[List[Any]] = None,
    ) -> List[Tuple]:
        """`_reports_run`'s twin for the filter bar, on its own flag.

        Its own switch for the reason the others have theirs, sharpened: these
        four are drawn on *every* page, so a rollback here is the one that
        must not be tangled up with a tab's. It is also the only router in
        this file whose failure would be visible on nine tabs at once, which
        is why the fallback below is not negotiable.
        """
        from core.sql_dialect import DUCKDB, POSTGRES

        from core import pg_lookups_read

        params = list(params or [])
        if pg_lookups_read.enabled() and pg_lookups_read.available():
            try:
                return await pg_lookups_read.fetch(
                    self._render_report(sql, POSTGRES), params,
                )
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "lookups: Postgres failed, falling back to DuckDB: %s",
                    exc, exc_info=True,
                )

        async with self.connection() as conn:
            return conn.execute(
                self._render_report(sql, DUCKDB), params,
            ).fetchall()

    async def _category_ids(self, category_id: int) -> List[int]:
        """A category and its descendants, from whichever engine answers.

        The routed twin of `_get_category_with_children`, sharing its SQL. The
        older one keeps taking a connection because its six callers are inside
        one; this one must not, for the reentrancy reason above.
        """
        rows = await self._reports_run(_CATEGORY_TREE_SQL, [category_id])
        return _at_least_the_root(category_id, [row[0] for row in rows])

    # ── Step 1 of «Одна бронза»: the Gold primitives can read Postgres ──────
    #
    # Interception mirrors DuckDB's own routing exactly: a request DuckDB's
    # Gold cannot answer (a source without a channel column, a line-level
    # filter) never reaches the Postgres reader either, so KS_READ_GOLD
    # changes the engine and nothing else. A failed Postgres read falls back
    # to DuckDB with an ERROR in the log — the flag is обкатка, and the
    # dashboard staying up is what makes flipping it survivable.

    async def _pg_gold_summary(
        self,
        start_date: date,
        end_date: date,
        sales_type: str,
        source_id: Optional[int],
    ) -> Optional[Tuple[int, float, int, float]]:
        from core import pg_gold_read

        if source_id and source_id not in self._GOLD_SOURCE_COLUMNS:
            return None  # DuckDB answers zeros here; parity keeps it that way
        if not pg_gold_read.enabled():
            return None
        try:
            return await pg_gold_read.fetch_summary(
                start_date, end_date, sales_type, source_id
            )
        except Exception as e:
            logger.error(
                "KS_READ_GOLD=postgres but the summary read failed, "
                "falling back to DuckDB: %s", e,
            )
            return None

    @staticmethod
    def _comparison_window(
        start_date: date, end_date: date, compare_type: str,
    ) -> Tuple[date, date]:
        """The comparison period's bounds — one home, two callers (the
        pre-lock Postgres prefetch and the in-lock DuckDB path)."""
        if compare_type == "year_ago":
            from dateutil.relativedelta import relativedelta
            return start_date - relativedelta(years=1), end_date - relativedelta(years=1)
        if compare_type == "month_ago":
            from dateutil.relativedelta import relativedelta
            return start_date - relativedelta(months=1), end_date - relativedelta(months=1)
        period_days = (end_date - start_date).days + 1
        prev_end = start_date - timedelta(days=1)
        return prev_end - timedelta(days=period_days - 1), prev_end

    async def _pg_gold_series(
        self,
        start_date: date,
        end_date: date,
        sales_type: str,
        source_id: Optional[int],
    ) -> Optional[List[Tuple[date, float, int]]]:
        from core import pg_gold_read

        if source_id and source_id not in self._GOLD_SOURCE_COLUMNS:
            return None  # same guard as the summary twin — parity by routing
        if not pg_gold_read.enabled():
            return None
        try:
            return await pg_gold_read.fetch_series(
                start_date, end_date, sales_type, source_id
            )
        except Exception as e:
            logger.error(
                "KS_READ_GOLD=postgres but the series read failed, "
                "falling back to DuckDB: %s", e,
            )
            return None

    async def get_summary_stats(
        self,
        start_date: date,
        end_date: date,
        source_id: Optional[int] = None,
        category_id: Optional[int] = None,
        brand: Optional[str] = None,
        sales_type: str = "retail",
        promocode: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get summary statistics for a date range (from Gold/Silver layers)."""
        # Step 1 of «Одна бронза» — and it happens BEFORE the store lock, not
        # inside it: `self.connection()` serialises every DuckDB access in the
        # process, and holding that lock across a network round trip hands a
        # hung Postgres the entire dashboard plus the 2-minute refresh queue.
        # The rule is the scheduler's own: the store's lock is not held across
        # the network. The Gold-only branch needs no DuckDB at all when
        # Postgres answers, so on success the lock is never taken.
        if not (category_id or brand or promocode):
            pg_totals = await self._pg_gold_summary(
                start_date, end_date, sales_type, source_id
            )
            if pg_totals is not None:
                total_orders, total_revenue = int(pg_totals[0]), float(pg_totals[1])
                total_returns, returns_revenue = int(pg_totals[2]), float(pg_totals[3])
                avg_check = total_revenue / total_orders if total_orders > 0 else 0
                # The same seven keys the DuckDB tail builds — the API shape
                # must not change with the engine.
                return {
                    "totalOrders": total_orders,
                    "totalRevenue": round(total_revenue, 2),
                    "avgCheck": round(avg_check, 2),
                    "totalReturns": total_returns,
                    "returnsRevenue": round(returns_revenue, 2),
                    "startDate": start_date.isoformat(),
                    "endDate": end_date.isoformat(),
                }

        async with self.connection() as conn:
            if category_id or brand or promocode:
                # Use Silver layer with JOINs for correct distinct order counts
                # (gold_daily_products can't deduplicate orders with multiple matching products)
                params = [start_date, end_date]
                where_clauses = ["l.order_date BETWEEN ? AND ?", "NOT l.is_return", "l.is_active_source"]

                if sales_type != "all":
                    where_clauses.append("l.sales_type = ?")
                    params.append(sales_type)

                if source_id:
                    where_clauses.append("l.source_id = ?")
                    params.append(source_id)

                cat_ids = None
                if category_id:
                    cat_ids = await self._get_category_with_children(conn, category_id)
                    where_clauses.append(f"l.category_id IN ({','.join('?' * len(cat_ids))})")
                    params.extend(cat_ids)

                if brand:
                    where_clauses.append(brand_where(brand, params, "l"))

                if promocode:
                    where_clauses.append("UPPER(l.promocode) = UPPER(?)")
                    params.append(promocode)

                where_sql = " AND ".join(where_clauses)

                # The line level: distinct order counts stay correct because the
                # level keeps the order id beside every line.
                result = conn.execute(f"""
                    SELECT
                        COUNT(DISTINCT l.order_id) as total_orders,
                        COALESCE(SUM(l.line_amount), 0) as total_revenue
                    FROM silver_order_lines l
                    WHERE {where_sql}
                """, params).fetchone()

                total_orders = int(result[0] or 0)
                total_revenue = float(result[1] or 0)

                # Returns from silver_orders — consistent with the filtered orders query above
                # (Gold doesn't have category/brand/source breakdown for returns)
                ret_params = [start_date, end_date]
                ret_where = ["s.order_date BETWEEN ? AND ?", "s.is_return", "s.is_active_source"]
                if sales_type != "all":
                    ret_where.append("s.sales_type = ?")
                    ret_params.append(sales_type)
                if source_id:
                    ret_where.append("s.source_id = ?")
                    ret_params.append(source_id)
                ret_result = conn.execute(f"""
                    SELECT COUNT(DISTINCT s.id), COALESCE(SUM(s.grand_total), 0)
                    FROM silver_orders s
                    WHERE {" AND ".join(ret_where)}
                """, ret_params).fetchone()
                total_returns = int(ret_result[0])
                returns_revenue = float(ret_result[1])
            else:
                # Use gold_daily_revenue for non-product queries. (Postgres
                # was already given its chance above, before the lock.)
                params = [start_date, end_date]
                where_clauses = ["date BETWEEN ? AND ?"]

                if sales_type != "all":
                    where_clauses.append("sales_type = ?")
                    params.append(sales_type)

                where_sql = " AND ".join(where_clauses)

                if source_id:
                    # Source-specific: sum per-source columns for orders/revenue
                    source_col_map = {1: "instagram", 2: "telegram", 4: "shopify"}
                    src_name = source_col_map.get(source_id)
                    if src_name:
                        result = conn.execute(f"""
                            SELECT
                                SUM({src_name}_orders) as total_orders,
                                SUM({src_name}_revenue) as total_revenue
                            FROM gold_daily_revenue
                            WHERE {where_sql}
                        """, params).fetchone()
                        # Gold doesn't have per-source return columns — query Silver
                        ret_params = [start_date, end_date, source_id]
                        sales_filter = self._build_sales_type_filter(sales_type, table_alias="silver_orders")
                        ret_result = conn.execute(f"""
                            SELECT COUNT(DISTINCT id), COALESCE(SUM(grand_total), 0)
                            FROM silver_orders
                            WHERE order_date BETWEEN ? AND ?
                              AND is_return AND is_active_source AND source_id = ?
                              AND {sales_filter}
                        """, ret_params).fetchone()
                        result = (result[0], result[1], ret_result[0], ret_result[1])
                    else:
                        result = (0, 0, 0, 0)
                else:
                    result = conn.execute(f"""
                        SELECT
                            SUM(orders_count) as total_orders,
                            SUM(revenue) as total_revenue,
                            SUM(returns_count) as total_returns,
                            SUM(returns_revenue) as returns_revenue
                        FROM gold_daily_revenue
                        WHERE {where_sql}
                    """, params).fetchone()

                total_orders = int(result[0] or 0)
                total_revenue = float(result[1] or 0)
                total_returns = int(result[2] or 0)
                returns_revenue = float(result[3] or 0)

            avg_check = total_revenue / total_orders if total_orders > 0 else 0

            return {
                "totalOrders": total_orders,
                "totalRevenue": round(total_revenue, 2),
                "avgCheck": round(avg_check, 2),
                "totalReturns": total_returns,
                "returnsRevenue": round(returns_revenue, 2),
                "startDate": start_date.isoformat(),
                "endDate": end_date.isoformat()
            }

    async def get_return_orders(
        self,
        start_date: date,
        end_date: date,
        sales_type: str = "retail",
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Get list of return orders for a date range.

        Returns list of orders with id, date, amount, status, source.
        """
        # Derive status names from OrderStatus enum (single source of truth)
        STATUS_NAMES = {
            s.value: s.name.replace("_", " ").title()
            for s in OrderStatus.return_statuses()
        }
        # Ensure we have a mapping (safety net)
        STATUS_NAMES.setdefault(19, "Returned")

        async with self.connection() as conn:
            params: list = [start_date, end_date]
            where_clauses = [
                "s.order_date BETWEEN ? AND ?",
                "s.is_return = TRUE",
                "s.is_active_source = TRUE"
            ]

            if sales_type != "all":
                where_clauses.append("s.sales_type = ?")
                params.append(sales_type)

            where_sql = " AND ".join(where_clauses)
            params.append(limit)

            result = conn.execute(f"""
                SELECT
                    s.id,
                    s.order_date,
                    s.grand_total,
                    s.status_id,
                    s.source_name,
                    s.buyer_id,
                    b.full_name AS buyer_name,
                    b.phone AS buyer_phone,
                    s.manager_id,
                    m.name AS manager_name
                FROM silver_orders s
                LEFT JOIN buyers b ON s.buyer_id = b.id
                LEFT JOIN managers m ON s.manager_id = m.id
                WHERE {where_sql}
                ORDER BY s.order_date DESC, s.id DESC
                LIMIT ?
            """, params).fetchall()

            return [
                {
                    "id": row[0],
                    "date": row[1].isoformat() if row[1] else None,
                    "amount": float(row[2] or 0),
                    "statusId": row[3],
                    "statusName": STATUS_NAMES.get(row[3], f"Status {row[3]}"),
                    "source": row[4],
                    "buyerId": row[5],
                    "buyerName": row[6],
                    "buyerPhone": row[7],
                    "managerId": row[8],
                    "managerName": row[9],
                }
                for row in result
            ]

    def _build_gold_revenue_query(
        self,
        start_date: date,
        end_date: date,
        sales_type: str = "retail",
        source_id: Optional[int] = None,
    ) -> Tuple[str, list]:
        """Build a query against gold_daily_revenue for a date range.

        Returns (sql, params) tuple that SELECTs day, revenue, order_count.
        When sales_type='all', aggregates across sales types with GROUP BY.
        """
        params = [start_date, end_date]
        where_clauses = ["date BETWEEN ? AND ?"]
        needs_group_by = sales_type == "all"

        if sales_type != "all":
            where_clauses.append("sales_type = ?")
            params.append(sales_type)

        where_sql = " AND ".join(where_clauses)

        if source_id:
            source_col_map = {1: "instagram", 2: "telegram", 4: "shopify"}
            src = source_col_map.get(source_id)
            if src:
                if needs_group_by:
                    sql = f"""
                        SELECT date AS day, SUM({src}_revenue) AS revenue, SUM({src}_orders) AS order_count
                        FROM gold_daily_revenue
                        WHERE {where_sql}
                        GROUP BY date
                        ORDER BY date
                    """
                else:
                    sql = f"""
                        SELECT date AS day, {src}_revenue AS revenue, {src}_orders AS order_count
                        FROM gold_daily_revenue
                        WHERE {where_sql}
                        ORDER BY date
                    """
            else:
                # Gold holds no column for this source, so it cannot answer.
                # `params` is emptied with the SQL: the two used to part ways
                # here, and passing three bound values to a statement taking
                # none raised `Parameter argument/count mismatch` — a 500 on
                # every /api/revenue/trend?source_id=5.
                #
                # `get_revenue_trend` no longer routes an unanswerable source
                # here at all; it goes to `_build_silver_orders_revenue_query`,
                # which returns the real number. This stays consistent for any
                # future caller rather than staying a trap.
                sql = "SELECT NULL::DATE, 0, 0 WHERE FALSE"
                params = []
        else:
            if needs_group_by:
                sql = f"""
                    SELECT date AS day, SUM(revenue) AS revenue, SUM(orders_count) AS order_count
                    FROM gold_daily_revenue
                    WHERE {where_sql}
                    GROUP BY date
                    ORDER BY date
                """
            else:
                sql = f"""
                    SELECT date AS day, revenue, orders_count AS order_count
                    FROM gold_daily_revenue
                    WHERE {where_sql}
                    ORDER BY date
                """
        return sql, params

    # Which source ids `gold_daily_revenue` can answer for on its own. The Gold
    # row carries one column per source, so a source without a column cannot be
    # asked about here — it is not a missing filter but an absent column.
    _GOLD_SOURCE_COLUMNS = {1: "instagram", 2: "telegram", 4: "shopify"}

    def _build_silver_orders_revenue_query(
        self,
        start_date: date,
        end_date: date,
        sales_type: str = "retail",
        source_id: Optional[int] = None,
        promocode: Optional[str] = None,
    ) -> Tuple[str, list]:
        """Revenue at the ORDER grain, for order-level filters Gold cannot express.

        Same measure as `_build_gold_revenue_query` — `SUM(grand_total)` over
        non-return rows on an active source — computed from `silver_orders`
        instead of read from the pre-aggregate. Gold is the fast path for the
        shapes it holds a column for; this is the general one.

        Two filters need it, and both used to be a 500 rather than an answer.
        `promocode` has no Gold column at all. And a `source_id` Gold has no
        column for fell to a guard that builds
        `SELECT NULL::DATE, 0, 0 WHERE FALSE` while still returning three
        bound parameters, so DuckDB raised
        `Parameter argument/count mismatch` — that is what
        `/api/revenue/trend?source_id=5` did, on the Виставка source worth
        ₴266k that PR #101 had just finished putting back into the numbers.

        Deliberately NOT the line-grain query: an order-level filter selects
        whole orders, so the answer is the money those orders brought in. The
        line-grain query answers a different question and is only correct when
        the filter is line-level.
        """
        params: list = [start_date, end_date]
        where = [
            "order_date BETWEEN ? AND ?",
            "NOT is_return",
            "is_active_source",
        ]

        if sales_type != "all":
            where.append("sales_type = ?")
            params.append(sales_type)

        if source_id:
            where.append("source_id = ?")
            params.append(source_id)

        if promocode:
            where.append("UPPER(promocode) = UPPER(?)")
            params.append(promocode)

        sql = f"""
            SELECT order_date AS day,
                   COALESCE(SUM(grand_total), 0) AS revenue,
                   COUNT(*) AS order_count
            FROM silver_orders
            WHERE {" AND ".join(where)}
            GROUP BY order_date
            ORDER BY order_date
        """
        return sql, params

    def _build_silver_products_revenue_query(
        self,
        start_date: date,
        end_date: date,
        sales_type: str = "retail",
        source_id: Optional[int] = None,
        category_ids: Optional[List[int]] = None,
        brand: Optional[str] = None,
        promocode: Optional[str] = None,
    ) -> Tuple[str, list]:
        """Build a query against Silver layer for revenue trend with product filters.

        Reads `silver_order_lines`, which keeps distinct order counts correct.
        Returns (sql, params) that SELECTs day, revenue, order_count.
        """
        params: list = [start_date, end_date]
        where_clauses = ["l.order_date BETWEEN ? AND ?", "NOT l.is_return", "l.is_active_source"]

        if sales_type != "all":
            where_clauses.append("l.sales_type = ?")
            params.append(sales_type)

        if source_id:
            where_clauses.append("l.source_id = ?")
            params.append(source_id)

        if category_ids:
            where_clauses.append(f"l.category_id IN ({','.join('?' * len(category_ids))})")
            params.extend(category_ids)

        if brand:
            where_clauses.append(brand_where(brand, params, "l"))

        if promocode:
            # `l`, not `s`: the FROM clause below binds `silver_order_lines l`
            # and nothing else, so `s.promocode` raised
            # `Binder Error: Referenced table "s" not found!` — every request
            # to /api/revenue/trend carrying a promocode was a 500. The other
            # five promocode predicates in this file already say `l`.
            where_clauses.append("UPPER(l.promocode) = UPPER(?)")
            params.append(promocode)

        where_sql = " AND ".join(where_clauses)
        sql = f"""
            SELECT l.order_date AS day,
                   COALESCE(SUM(l.line_amount), 0) AS revenue,
                   COUNT(DISTINCT l.order_id) AS order_count
            FROM silver_order_lines l
            WHERE {where_sql}
            GROUP BY l.order_date
            ORDER BY l.order_date
        """
        return sql, params

    async def get_revenue_trend(
        self,
        start_date: date,
        end_date: date,
        source_id: Optional[int] = None,
        category_id: Optional[int] = None,
        brand: Optional[str] = None,
        include_comparison: bool = True,
        sales_type: str = "retail",
        compare_type: str = "previous_period",
        promocode: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get daily revenue trend for chart.

        **Two measures live here, and which one you get depends on the filter.**
        That is deliberate, and the response says which one it used.

        - A **line-level** filter — category or brand — selects part of an
          order, and `grand_total` cannot be split by it. The answer is the
          value of the goods sold: `SUM(price_sold * quantity)`. This is what
          brand analytics is for, and the owner ruled on 2026-08-23 that it is
          counted honestly — all goods of that brand across every order in the
          period, whatever the customer paid with.
        - An **order-level** filter — promocode, source, sales_type — selects
          whole orders, so the answer stays money that came in:
          `SUM(grand_total)`.

        The two do not add up to each other and are not meant to: retail goods
        come to ₴57.7M against ₴56.9M of revenue, and 99.4% of that ₴0.8M gap
        is gift certificates, whose goods were paid for when the certificate
        was sold. See `_headline_vs_line_items_check`.

        The bug this replaced was not that both exist. It was that
        `use_products` also switched on `promocode` — an order-level filter —
        so applying one silently changed the measure by ₴1.5M lifetime, with
        nothing in the response saying so.
        """
        # Line-level filters only. A promocode selects whole orders and
        # belongs on the order grain; it used to land here and change the
        # measure as a side effect.
        use_lines = bool(category_id or brand)
        gold_can_answer = (
            not promocode
            and (not source_id or source_id in self._GOLD_SOURCE_COLUMNS)
        )

        # Step 1 of «Одна бронза»: both Gold series — current and comparison —
        # are read from Postgres BEFORE the store lock, get_summary_stats'
        # reasoning: the lock serialises every DuckDB access in the process
        # and must not be held across the network.
        pg_series = prev_pg_series = None
        if not use_lines and gold_can_answer:
            pg_series = await self._pg_gold_series(
                start_date, end_date, sales_type, source_id
            )
            if include_comparison and pg_series is not None:
                prev_start, prev_end = self._comparison_window(
                    start_date, end_date, compare_type
                )
                # Same engine as the current period, or the growth number
                # becomes a ratio between two stores.
                prev_pg_series = await self._pg_gold_series(
                    prev_start, prev_end, sales_type, source_id
                )

        # When Postgres answered everything the method will ask for, the store
        # is not touched at all — not even an empty lock acquisition, which
        # would still serialise this request behind a running rebuild.
        fully_pg = (
            not use_lines
            and pg_series is not None
            and (not include_comparison or prev_pg_series is not None)
        )
        async with (nullcontext(None) if fully_pg else self.connection()) as conn:
            cat_ids = None
            if category_id:
                cat_ids = await self._get_category_with_children(conn, category_id)

            if use_lines:
                sql, params = self._build_silver_products_revenue_query(
                    start_date, end_date, sales_type, source_id, cat_ids, brand, promocode
                )
            elif gold_can_answer:
                # pg_series may already hold the Postgres answer (prefetched
                # above, before the lock); None falls through to DuckDB.
                sql, params = self._build_gold_revenue_query(
                    start_date, end_date, sales_type, source_id
                )
            else:
                sql, params = self._build_silver_orders_revenue_query(
                    start_date, end_date, sales_type, source_id, promocode
                )

            results = (
                pg_series if pg_series is not None
                else conn.execute(sql, params).fetchall()
            )
            daily_data = {row[0]: (float(row[1]), int(row[2])) for row in results}

            # Build labels and data
            labels = []
            data = []
            orders_data = []
            current = start_date
            while current <= end_date:
                labels.append(current.strftime("%d.%m"))
                revenue_val, orders_val = daily_data.get(current, (0, 0))
                data.append(round(revenue_val, 2))
                orders_data.append(orders_val)
                current += timedelta(days=1)

            datasets = [{
                "label": "This Period",
                "data": data,
                "borderColor": "#16A34A",
                "backgroundColor": "rgba(22, 163, 74, 0.1)",
                "fill": True,
                "tension": 0.3,
                "borderWidth": 2
            }]

            # Add previous period comparison
            if include_comparison:
                prev_start, prev_end = self._comparison_window(
                    start_date, end_date, compare_type
                )

                # The comparison period has to be measured the same way as the
                # current one, or the growth percentage is a ratio between two
                # different questions.
                if use_lines:
                    prev_sql, prev_params = self._build_silver_products_revenue_query(
                        prev_start, prev_end, sales_type, source_id, cat_ids, brand, promocode
                    )
                elif gold_can_answer:
                    # prev_pg_series was prefetched above, before the lock,
                    # from the same engine as the current period.
                    prev_sql, prev_params = self._build_gold_revenue_query(
                        prev_start, prev_end, sales_type, source_id
                    )
                else:
                    prev_sql, prev_params = self._build_silver_orders_revenue_query(
                        prev_start, prev_end, sales_type, source_id, promocode
                    )

                # Only need day + revenue for comparison
                prev_results = (
                    prev_pg_series if prev_pg_series is not None
                    else conn.execute(prev_sql, prev_params).fetchall()
                )
                prev_daily = {row[0]: float(row[1]) for row in prev_results}

                prev_data = []
                prev_current = prev_start
                while prev_current <= prev_end:
                    prev_data.append(round(prev_daily.get(prev_current, 0), 2))
                    prev_current += timedelta(days=1)

                datasets.append({
                    "label": "Previous Period",
                    "data": prev_data,
                    "borderColor": "#9CA3AF",
                    "backgroundColor": "rgba(156, 163, 175, 0.1)",
                    "fill": False,
                    "tension": 0.3,
                    "borderWidth": 2,
                    "borderDash": [5, 5]
                })

            # Build comparison object for v2 frontend
            comparison = None
            if include_comparison and len(datasets) > 1:
                prev_dataset = datasets[1]
                current_total = sum(data)
                prev_total = sum(prev_dataset["data"])
                growth_percent = ((current_total - prev_total) / prev_total * 100) if prev_total > 0 else 0

                comparison = {
                    "labels": labels,
                    "revenue": prev_dataset["data"],
                    "orders": [],
                    "period": {
                        "start": prev_start.isoformat(),
                        "end": prev_end.isoformat(),
                        "type": compare_type
                    },
                    "totals": {
                        "current": round(current_total, 2),
                        "previous": round(prev_total, 2),
                        "growth_percent": round(growth_percent, 1)
                    }
                }

            result = {
                "labels": labels,
                "revenue": data,
                "orders": orders_data,
                "datasets": datasets,
                # Which of the two measures the numbers above are. A caller
                # that draws them under one axis label without reading this
                # reintroduces the defect: the series steps by ~1.5% when a
                # brand filter goes on, and nothing on screen says why.
                "measure": "goods_value" if use_lines else "revenue",
            }
            if comparison:
                result["comparison"] = comparison
            return result

    async def get_sales_by_source(
        self,
        start_date: date,
        end_date: date,
        category_id: Optional[int] = None,
        brand: Optional[str] = None,
        promocode: Optional[str] = None,
        sales_type: str = "retail"
    ) -> Dict[str, Any]:
        """Get sales breakdown by source (from Gold/Silver layers)."""
        async with self.connection() as conn:
            source_names = {1: "Instagram", 2: "Telegram", 4: "Shopify"}
            source_colors = {1: "#7C3AED", 2: "#2563EB", 4: "#eb4200"}

            if category_id or brand or promocode:
                # Use Silver layer with JOINs for correct distinct order counts
                params = [start_date, end_date]
                where_clauses = ["l.order_date BETWEEN ? AND ?", "NOT l.is_return", "l.is_active_source"]

                if sales_type != "all":
                    where_clauses.append("l.sales_type = ?")
                    params.append(sales_type)

                if category_id:
                    cat_ids = await self._get_category_with_children(conn, category_id)
                    where_clauses.append(f"l.category_id IN ({','.join('?' * len(cat_ids))})")
                    params.extend(cat_ids)

                if brand:
                    where_clauses.append(brand_where(brand, params, "l"))

                if promocode:
                    where_clauses.append("UPPER(l.promocode) = UPPER(?)")
                    params.append(promocode)

                where_sql = " AND ".join(where_clauses)

                results = conn.execute(f"""
                    SELECT l.source_id,
                           COUNT(DISTINCT l.order_id) as orders,
                           COALESCE(SUM(l.line_amount), 0) as revenue
                    FROM silver_order_lines l
                    WHERE {where_sql}
                    GROUP BY l.source_id
                    ORDER BY revenue DESC
                """, params).fetchall()

                labels = []
                orders = []
                revenue = []
                colors = []
                for row in results:
                    sid = row[0]
                    if sid in source_names:
                        labels.append(source_names[sid])
                        orders.append(int(row[1]))
                        revenue.append(round(float(row[2]), 2))
                        colors.append(source_colors.get(sid, "#999999"))
            else:
                # Use gold_daily_revenue per-source columns
                params = [start_date, end_date]
                where_clauses = ["date BETWEEN ? AND ?"]

                if sales_type != "all":
                    where_clauses.append("sales_type = ?")
                    params.append(sales_type)

                where_sql = " AND ".join(where_clauses)

                result = conn.execute(f"""
                    SELECT
                        SUM(instagram_orders) as ig_orders, SUM(instagram_revenue) as ig_rev,
                        SUM(telegram_orders) as tg_orders, SUM(telegram_revenue) as tg_rev,
                        SUM(shopify_orders) as sh_orders, SUM(shopify_revenue) as sh_rev
                    FROM gold_daily_revenue
                    WHERE {where_sql}
                """, params).fetchone()

                # Build source list sorted by revenue desc
                source_data = [
                    (1, "Instagram", int(result[0] or 0), float(result[1] or 0)),
                    (2, "Telegram", int(result[2] or 0), float(result[3] or 0)),
                    (4, "Shopify", int(result[4] or 0), float(result[5] or 0)),
                ]
                source_data.sort(key=lambda x: x[3], reverse=True)

                labels = [s[1] for s in source_data if s[3] > 0 or s[2] > 0]
                orders = [s[2] for s in source_data if s[3] > 0 or s[2] > 0]
                revenue = [round(s[3], 2) for s in source_data if s[3] > 0 or s[2] > 0]
                colors = [source_colors[s[0]] for s in source_data if s[3] > 0 or s[2] > 0]

            return {
                "labels": labels,
                "orders": orders,
                "revenue": revenue,
                "backgroundColor": colors
            }

    async def get_top_products(
        self,
        start_date: date,
        end_date: date,
        source_id: Optional[int] = None,
        category_id: Optional[int] = None,
        brand: Optional[str] = None,
        promocode: Optional[str] = None,
        limit: int = 10,
        sales_type: str = "retail"
    ) -> Dict[str, Any]:
        """Get top products by quantity (from Gold layer)."""
        async with self.connection() as conn:
            if promocode:
                # Silver path: gold_daily_products lacks promocode
                silver_params = [start_date, end_date]
                silver_where = ["l.order_date BETWEEN ? AND ?", "NOT l.is_return", "l.is_active_source"]
                if sales_type != "all":
                    silver_where.append("l.sales_type = ?")
                    silver_params.append(sales_type)
                if source_id:
                    silver_where.append("l.source_id = ?")
                    silver_params.append(source_id)
                if category_id:
                    cat_ids = await self._get_category_with_children(conn, category_id)
                    silver_where.append(f"l.category_id IN ({','.join('?' * len(cat_ids))})")
                    silver_params.extend(cat_ids)
                if brand:
                    silver_where.append(brand_where(brand, silver_params, "l"))
                silver_where.append("UPPER(l.promocode) = UPPER(?)")
                silver_params.append(promocode)
                silver_params.append(limit)
                silver_sql = " AND ".join(silver_where)
                results = conn.execute(f"""
                    SELECT
                        ANY_VALUE(l.product_name) as product_name,
                        SUM(l.quantity) as total_qty
                    FROM silver_order_lines l
                    WHERE {silver_sql}
                    GROUP BY COALESCE(CAST(l.product_id AS VARCHAR), l.product_name)
                    ORDER BY total_qty DESC
                    LIMIT ?
                """, silver_params).fetchall()
            else:
                params = [start_date, end_date]
                where_clauses = ["g.date BETWEEN ? AND ?"]

                if sales_type != "all":
                    where_clauses.append("g.sales_type = ?")
                    params.append(sales_type)

                if source_id:
                    where_clauses.append("g.source_id = ?")
                    params.append(source_id)

                if category_id:
                    cat_ids = await self._get_category_with_children(conn, category_id)
                    where_clauses.append(f"g.category_id IN ({','.join('?' * len(cat_ids))})")
                    params.extend(cat_ids)

                if brand:
                    where_clauses.append(brand_where(brand, params, "g"))

                params.append(limit)
                where_sql = " AND ".join(where_clauses)

                results = conn.execute(f"""
                    SELECT
                        ANY_VALUE(g.product_name) as product_name,
                        SUM(g.quantity_sold) as total_qty
                    FROM gold_daily_products g
                    WHERE {where_sql}
                    GROUP BY COALESCE(CAST(g.product_id AS VARCHAR), g.product_name)
                    ORDER BY total_qty DESC
                    LIMIT ?
                """, params).fetchall()

            raw_labels = [row[0] or "Unknown" for row in results]
            labels = [self._wrap_label(row[0]) for row in results]
            data = [int(row[1]) for row in results]
            total = sum(data) if data else 1
            percentages = [round(d / total * 100, 1) for d in data]

            return {
                "labels": raw_labels,
                "wrappedLabels": labels,
                "data": data,
                "percentages": percentages,
                "backgroundColor": "#2563EB"
            }

    async def get_categories(self) -> List[Dict[str, Any]]:
        """Root categories for the filter dropdown, from whichever engine."""
        rows = await self._lookups_run(_ROOT_CATEGORIES_SQL)
        return [{"id": row[0], "name": row[1]} for row in rows]

    async def get_child_categories(self, parent_id: int) -> List[Dict[str, Any]]:
        """A parent's children, from whichever engine."""
        rows = await self._lookups_run(_CHILD_CATEGORIES_SQL, [parent_id])
        return [{"id": row[0], "name": row[1]} for row in rows]

    async def get_brands(self) -> List[Dict[str, str]]:
        """Every brand the filter can be set to, plus the unknown bucket.

        Brand analytics has always drawn a bucket for goods with no brand —
        `COALESCE(g.brand, 'Unknown')`, ₴3.5M of retail, fifth by revenue —
        and this list has always left it out, so it was the one slice on the
        chart a user could see and not click into.

        Offered only when there is something in it. Where it lands in the list
        is not decided here — `BrandFilter` sorts the whole list by label — so
        it reads alphabetically in the dropdown, which is where someone
        looking for it would look.
        """
        rows = await self._lookups_run(_BRANDS_SQL)
        brands = [{"name": row[0]} for row in rows]

        unbranded = await self._lookups_run(_UNBRANDED_EXISTS_SQL)
        if unbranded and unbranded[0][0]:
            brands.append({"name": UNKNOWN_BRAND})
        return brands

    async def get_promocodes(self) -> List[Dict[str, str]]:
        """Every promocode the filter can be set to, from whichever engine."""
        rows = await self._lookups_run(_PROMOCODES_SQL)
        return [{"name": row[0]} for row in rows]

    # ─── Helper Methods ───────────────────────────────────────────────────────

    async def _get_category_with_children(
        self,
        conn: duckdb.DuckDBPyConnection,
        category_id: int
    ) -> List[int]:
        """A category and its descendants, on a connection the caller holds.

        Shares `_CATEGORY_TREE_SQL` with `_category_ids`, which is the routed
        twin the three report methods use. Six methods still call this one
        from inside `self.connection()`, and the store lock is not reentrant,
        so they cannot use the router — hence two executors over one body
        rather than two bodies.

        (The `result = [category_id]` this used to open with looked dead — the
        recursive term already yields the root — but it was guarding something
        real; see `_at_least_the_root`.)
        """
        children = conn.execute(
            _CATEGORY_TREE_SQL.format(categories="categories"), [category_id],
        ).fetchall()
        return _at_least_the_root(category_id, [row[0] for row in children])

    @staticmethod
    def _wrap_label(text: str, max_chars: int = 25) -> List[str]:
        """Wrap long text for chart labels."""
        if not text or len(text) <= max_chars:
            return [text] if text else ["Unknown"]

        words = text.split()
        lines = []
        current_line = ""

        for word in words:
            if len(current_line) + len(word) + 1 <= max_chars:
                current_line = f"{current_line} {word}".strip()
            else:
                if current_line:
                    lines.append(current_line)
                current_line = word

        if current_line:
            lines.append(current_line)

        if len(lines) > 2:
            lines = [lines[0], lines[1][:max_chars-3] + "..."]

        return lines

    # ─── Advanced Analytics Methods ──────────────────────────────────────────────

    async def get_product_performance(
        self,
        start_date: date,
        end_date: date,
        source_id: Optional[int] = None,
        brand: Optional[str] = None,
        promocode: Optional[str] = None,
        sales_type: str = "retail"
    ) -> Dict[str, Any]:
        """Get product performance: top by revenue, category breakdown (from Gold layer)."""
        async with self.connection() as conn:
            if promocode:
                # Silver path: gold_daily_products lacks promocode
                silver_params = [start_date, end_date]
                silver_where = ["l.order_date BETWEEN ? AND ?", "NOT l.is_return", "l.is_active_source"]
                if sales_type != "all":
                    silver_where.append("l.sales_type = ?")
                    silver_params.append(sales_type)
                if source_id:
                    silver_where.append("l.source_id = ?")
                    silver_params.append(source_id)
                if brand:
                    silver_where.append(brand_where(brand, silver_params, "l"))
                silver_where.append("UPPER(l.promocode) = UPPER(?)")
                silver_params.append(promocode)
                silver_sql = " AND ".join(silver_where)

                top_results = conn.execute(f"""
                    SELECT
                        ANY_VALUE(l.product_name) as product_name,
                        SUM(l.line_amount) as revenue,
                        SUM(l.quantity) as quantity
                    FROM silver_order_lines l
                    WHERE {silver_sql}
                    GROUP BY COALESCE(CAST(l.product_id AS VARCHAR), l.product_name)
                    ORDER BY revenue DESC
                    LIMIT 10
                """, silver_params).fetchall()

                cat_results = conn.execute(f"""
                    SELECT
                        COALESCE(l.parent_category_name, l.category_name, 'Other') as category_name,
                        SUM(l.line_amount) as revenue,
                        SUM(l.quantity) as quantity
                    FROM silver_order_lines l
                    WHERE {silver_sql}
                    GROUP BY COALESCE(l.parent_category_name, l.category_name, 'Other')
                    ORDER BY revenue DESC
                """, silver_params).fetchall()
            else:
                params = [start_date, end_date]
                where_clauses = ["g.date BETWEEN ? AND ?"]

                if sales_type != "all":
                    where_clauses.append("g.sales_type = ?")
                    params.append(sales_type)

                if source_id:
                    where_clauses.append("g.source_id = ?")
                    params.append(source_id)

                if brand:
                    where_clauses.append(brand_where(brand, params, "g"))

                where_sql = " AND ".join(where_clauses)

                # Top products by revenue
                top_results = conn.execute(f"""
                    SELECT
                        g.product_name,
                        SUM(g.product_revenue) as revenue,
                        SUM(g.quantity_sold) as quantity
                    FROM gold_daily_products g
                    WHERE {where_sql}
                    GROUP BY g.product_name
                    ORDER BY revenue DESC
                    LIMIT 10
                """, params).fetchall()

                # Category breakdown (use parent_category_name, fall back to category_name)
                cat_results = conn.execute(f"""
                    SELECT
                        COALESCE(g.parent_category_name, g.category_name, 'Other') as category_name,
                        SUM(g.product_revenue) as revenue,
                        SUM(g.quantity_sold) as quantity
                    FROM gold_daily_products g
                    WHERE {where_sql}
                    GROUP BY COALESCE(g.parent_category_name, g.category_name, 'Other')
                    ORDER BY revenue DESC
                """, params).fetchall()

            category_colors = ["#7C3AED", "#2563EB", "#16A34A", "#F59E0B", "#eb4200", "#EC4899", "#8B5CF6", "#06B6D4"]
            category_breakdown = {
                "labels": [row[0] for row in cat_results],
                "revenue": [round(float(row[1]), 2) for row in cat_results],
                "quantity": [int(row[2]) for row in cat_results],
                "backgroundColor": category_colors[:len(cat_results)]
            }

            product_colors = "#7C3AED"
            top_by_revenue = {
                "labels": [row[0] for row in top_results],
                "data": [round(float(row[1]), 2) for row in top_results],
                "quantities": [int(row[2]) for row in top_results],
                "backgroundColor": product_colors
            }

            total_revenue = sum(float(row[1]) for row in top_results) if top_results else 0
            total_quantity = sum(int(row[2]) for row in top_results) if top_results else 0

            return {
                "topByRevenue": top_by_revenue,
                "categoryBreakdown": category_breakdown,
                "metrics": {
                    "totalProducts": len(top_results),
                    "totalRevenue": round(total_revenue, 2),
                    "totalQuantity": total_quantity,
                    "avgProductRevenue": round(total_revenue / len(top_results), 2) if top_results else 0
                }
            }

    async def get_subcategory_breakdown(
        self,
        start_date: date,
        end_date: date,
        parent_category_name: str,
        source_id: Optional[int] = None,
        brand: Optional[str] = None,
        promocode: Optional[str] = None,
        sales_type: str = "retail"
    ) -> Dict[str, Any]:
        """Get sales breakdown by subcategories for a given parent category."""
        async with self.connection() as conn:
            params = [start_date, end_date]
            # This was the last query in the file reading raw `orders`, with its
            # own date conversion, its own return-status list and its own copy of
            # the sales_type rule. The level answers all three, and answers them
            # the way Gold does: `is_return` prefers KeyCRM's status *group*,
            # which is what caught status 20 in July.
            where_clauses = [
                "l.order_date BETWEEN ? AND ?",
                "NOT l.is_return",
                "l.source_id IN (1, 2, 4)",  # Exclude Opencart (deprecated)
            ]

            if sales_type != "all":
                where_clauses.append("l.sales_type = ?")
                params.append(sales_type)

            if source_id:
                where_clauses.append("l.source_id = ?")
                params.append(source_id)

            where_sql = " AND ".join(where_clauses)

            # Build brand filter
            brand_filter = ""
            brand_params = []
            if brand:
                brand_filter = "AND " + brand_where(brand, brand_params, "l")

            promocode_filter = ""
            promocode_params = []
            if promocode:
                promocode_filter = "AND UPPER(l.promocode) = UPPER(?)"
                promocode_params.append(promocode)

            # Get subcategories for the parent category
            subcategory_sql = f"""
                SELECT
                    l.category_name as subcategory_name,
                    SUM(l.line_amount) as revenue,
                    SUM(l.quantity) as quantity
                FROM silver_order_lines l
                WHERE {where_sql}
                    AND (l.parent_category_name = ?
                         OR (l.category_name = ? AND l.parent_category_id IS NULL))
                    {brand_filter}
                    {promocode_filter}
                GROUP BY l.category_name
                ORDER BY revenue DESC
            """
            # Build final params: base params + parent_category (twice) + brand + promocode
            final_params = params + [parent_category_name, parent_category_name] + brand_params + promocode_params

            results = conn.execute(subcategory_sql, final_params).fetchall()

            category_colors = ["#7C3AED", "#2563EB", "#16A34A", "#F59E0B", "#eb4200", "#EC4899", "#8B5CF6", "#06B6D4"]

            return {
                "parentCategory": parent_category_name,
                "labels": [row[0] for row in results],
                "revenue": [round(float(row[1]), 2) for row in results],
                "quantity": [row[2] for row in results],
                "backgroundColor": category_colors[:len(results)]
            }

    async def get_brand_analytics(
        self,
        start_date: date,
        end_date: date,
        source_id: Optional[int] = None,
        sales_type: str = "retail"
    ) -> Dict[str, Any]:
        """Get brand analytics: top brands by revenue and quantity (from Gold layer)."""
        async with self.connection() as conn:
            params = [start_date, end_date]
            where_clauses = ["g.date BETWEEN ? AND ?"]

            if sales_type != "all":
                where_clauses.append("g.sales_type = ?")
                params.append(sales_type)

            if source_id:
                where_clauses.append("g.source_id = ?")
                params.append(source_id)

            where_sql = " AND ".join(where_clauses)

            # Brand stats from gold_daily_products
            brand_results = conn.execute(f"""
                SELECT
                    COALESCE(g.brand, 'Unknown') as brand_name,
                    SUM(g.product_revenue) as revenue,
                    SUM(g.quantity_sold) as quantity,
                    SUM(g.order_count) as orders
                FROM gold_daily_products g
                WHERE {where_sql}
                GROUP BY COALESCE(g.brand, 'Unknown')
                ORDER BY revenue DESC
            """, params).fetchall()

            brand_colors = ["#7C3AED", "#2563EB", "#16A34A", "#F59E0B", "#eb4200", "#EC4899", "#8B5CF6", "#06B6D4", "#14B8A6", "#EF4444"]

            # Top 10 by revenue
            top_by_revenue = brand_results[:10]
            top_brands_revenue = {
                "labels": [row[0] for row in top_by_revenue],
                "data": [round(float(row[1]), 2) for row in top_by_revenue],
                "quantities": [int(row[2]) for row in top_by_revenue],
                "orders": [int(row[3]) for row in top_by_revenue],
                "backgroundColor": brand_colors[:len(top_by_revenue)]
            }

            # Top 10 by quantity
            sorted_by_qty = sorted(brand_results, key=lambda x: x[2], reverse=True)[:10]
            top_brands_quantity = {
                "labels": [row[0] for row in sorted_by_qty],
                "data": [int(row[2]) for row in sorted_by_qty],
                "revenue": [round(float(row[1]), 2) for row in sorted_by_qty],
                "backgroundColor": brand_colors[:len(sorted_by_qty)]
            }

            total_revenue = sum(float(row[1]) for row in brand_results)
            total_quantity = sum(int(row[2]) for row in brand_results)
            unique_brands = len([b for b in brand_results if b[0] != "Unknown"])

            top_brand = brand_results[0][0] if brand_results else "N/A"
            top_brand_revenue = float(brand_results[0][1]) if brand_results else 0
            top_brand_share = (top_brand_revenue / total_revenue * 100) if total_revenue > 0 else 0

            return {
                "topByRevenue": top_brands_revenue,
                "topByQuantity": top_brands_quantity,
                "metrics": {
                    "totalBrands": unique_brands,
                    "topBrand": top_brand,
                    "topBrandShare": round(top_brand_share, 1),
                    "totalRevenue": round(total_revenue, 2),
                    "totalQuantity": total_quantity,
                    "avgBrandRevenue": round(total_revenue / unique_brands, 2) if unique_brands > 0 else 0
                }
            }

    # ─── Report Methods ──────────────────────────────────────────────────────

    async def get_report_summary(
        self,
        start_date: date,
        end_date: date,
        sales_type: str = "retail",
        source_id: Optional[int] = None,
        category_id: Optional[int] = None,
        brand: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get per-source breakdown report for a date range."""
        source_names = {1: "Instagram", 2: "Telegram", 4: "Shopify"}

        params: list = [start_date, end_date]
        where_clauses = ["s.order_date BETWEEN ? AND ?", "s.is_active_source"]

        if sales_type != "all":
            where_clauses.append("s.sales_type = ?")
            params.append(sales_type)

        if source_id:
            where_clauses.append("s.source_id = ?")
            params.append(source_id)

        if category_id:
            cat_ids = await self._category_ids(category_id)
            where_clauses.append(f"s.category_id IN ({','.join('?' * len(cat_ids))})")
            params.extend(cat_ids)

        if brand:
            where_clauses.append(brand_where(brand, params, "s"))

        where_sql = " AND ".join(where_clauses)

        need_product_filter = bool(category_id or brand)

        if need_product_filter:
            # Filter orders that contain matching products, then aggregate
            # The level, aliased `s` on purpose: this method shares one
            # predicate between a line-grain filter and an order-grain
            # aggregate, and every clause it can emit — dates, sales_type,
            # source, category, brand — exists on both relations under the
            # same name. The catalog clauses only appear when the product
            # filter is on, which is the only branch that reaches here.
            order_filter_sql = f"""
                SELECT DISTINCT s.order_id AS id
                FROM {{order_lines}} s
                WHERE {where_sql}
            """
            results = await self._reports_run(f"""
                WITH matching_orders AS ({order_filter_sql})
                SELECT
                    s.source_id,
                    COUNT(CASE WHEN NOT s.is_return THEN 1 END) as orders_count,
                    -- Still order-grain, and deliberately so: this counts
                    -- orders, and an order with no line items at all — 323
                    -- of them carry revenue — must still be counted. Rolling
                    -- the whole query onto the level would drop those, which
                    -- is the boundary of what a line level can replace.
                    COALESCE(SUM(CASE WHEN NOT s.is_return THEN (
                        SELECT SUM(op2.quantity) FROM {{order_products}} op2 WHERE op2.order_id = s.id
                    ) ELSE 0 END), 0) as products_sold,
                    COALESCE(SUM(CASE WHEN NOT s.is_return THEN s.grand_total ELSE 0 END), 0) as revenue,
                    COUNT(CASE WHEN s.is_return THEN 1 END) as returns_count
                FROM {{silver_orders}} s
                WHERE s.id IN (SELECT id FROM matching_orders)
                GROUP BY s.source_id
                -- `source_id` closes the sort: revenue can tie, and the
                -- caller renders these rows in order.
                ORDER BY revenue DESC, s.source_id
            """, params)
        else:
            # No product filter: aggregate at order level, products_sold via subquery
            results = await self._reports_run(f"""
                SELECT
                    s.source_id,
                    COUNT(CASE WHEN NOT s.is_return THEN 1 END) as orders_count,
                    COALESCE(SUM(CASE WHEN NOT s.is_return THEN (
                        SELECT SUM(op.quantity) FROM {{order_products}} op WHERE op.order_id = s.id
                    ) ELSE 0 END), 0) as products_sold,
                    COALESCE(SUM(CASE WHEN NOT s.is_return THEN s.grand_total ELSE 0 END), 0) as revenue,
                    COUNT(CASE WHEN s.is_return THEN 1 END) as returns_count
                FROM {{silver_orders}} s
                WHERE {where_sql}
                GROUP BY s.source_id
                ORDER BY revenue DESC, s.source_id
            """, params)

        sources = []
        total_orders = 0
        total_products = 0
        total_revenue = 0.0
        total_returns = 0

        for row in results:
            sid = row[0]
            if sid not in source_names:
                continue
            orders = int(row[1] or 0)
            products = int(row[2] or 0)
            revenue = float(row[3] or 0)
            returns = int(row[4] or 0)
            avg_check = revenue / orders if orders > 0 else 0
            return_rate = returns / (orders + returns) * 100 if (orders + returns) > 0 else 0

            sources.append({
                "source_id": sid,
                "source_name": source_names[sid],
                "orders_count": orders,
                "products_sold": products,
                "revenue": round(revenue, 2),
                "avg_check": round(avg_check, 2),
                "returns_count": returns,
                "return_rate": round(return_rate, 1),
            })

            total_orders += orders
            total_products += products
            total_revenue += revenue
            total_returns += returns

        total_avg_check = total_revenue / total_orders if total_orders > 0 else 0
        total_return_rate = total_returns / (total_orders + total_returns) * 100 if (total_orders + total_returns) > 0 else 0

        return {
            "sources": sources,
            "totals": {
                "orders_count": total_orders,
                "products_sold": total_products,
                "revenue": round(total_revenue, 2),
                "avg_check": round(total_avg_check, 2),
                "returns_count": total_returns,
                "return_rate": round(total_return_rate, 1),
            },
        }

    async def get_report_top_products(
        self,
        start_date: date,
        end_date: date,
        sales_type: str = "retail",
        source_id: Optional[int] = None,
        category_id: Optional[int] = None,
        brand: Optional[str] = None,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """Get top products report with rank, quantity, revenue, orders."""
        params: list = [start_date, end_date]
        where_clauses = ["l.order_date BETWEEN ? AND ?", "NOT l.is_return", "l.is_active_source"]

        if sales_type != "all":
            where_clauses.append("l.sales_type = ?")
            params.append(sales_type)

        if source_id:
            where_clauses.append("l.source_id = ?")
            params.append(source_id)

        if category_id:
            cat_ids = await self._category_ids(category_id)
            where_clauses.append(f"l.category_id IN ({','.join('?' * len(cat_ids))})")
            params.extend(cat_ids)

        if brand:
            where_clauses.append(brand_where(brand, params, "l"))

        params.append(limit)
        where_sql = " AND ".join(where_clauses)

        results = await self._reports_run(f"""
                SELECT
                    COALESCE(l.catalog_product_name, l.product_name, 'Unknown') as product_name,
                    COALESCE(l.sku, '') as sku,
                    SUM(l.quantity) as quantity,
                    COALESCE(SUM(l.line_amount), 0) as revenue,
                    COUNT(DISTINCT l.order_id) as orders_count
            FROM {{order_lines}} l
            WHERE {where_sql}
            GROUP BY COALESCE(l.catalog_product_name, l.product_name, 'Unknown'), COALESCE(l.sku, '')
            -- Ties decide who makes the top-N cut here, so they cannot be
            -- left to the plan. `sku` closes it: two products can share a
            -- name, and then the two engines would keep different rows.
            ORDER BY quantity DESC, product_name, sku
            LIMIT ?
        """, params)

        total_qty = sum(int(row[2] or 0) for row in results)

        return [
            {
                "rank": i + 1,
                "product_name": row[0],
                "sku": row[1],
                "quantity": int(row[2] or 0),
                "percentage": round(int(row[2] or 0) / total_qty * 100, 1) if total_qty > 0 else 0,
                "revenue": round(float(row[3] or 0), 2),
                "orders_count": int(row[4] or 0),
            }
            for i, row in enumerate(results)
        ]

    async def get_report_products_by_source(
        self,
        start_date: date,
        end_date: date,
        sales_type: str = "retail",
    ) -> Dict[str, Any]:
        """Get all products grouped by source (matches bot Excel format)."""
        source_names = {1: "Instagram", 2: "Telegram", 4: "Shopify"}

        params: list = [start_date, end_date]
        where_clauses = ["l.order_date BETWEEN ? AND ?", "NOT l.is_return", "l.is_active_source"]

        if sales_type != "all":
            where_clauses.append("l.sales_type = ?")
            params.append(sales_type)

        where_sql = " AND ".join(where_clauses)

        results = await self._reports_run(f"""
                SELECT
                    l.source_id,
                    COALESCE(l.catalog_product_name, l.product_name, 'Unknown') as product_name,
                    SUM(l.quantity) as quantity
            FROM {{order_lines}} l
            WHERE {where_sql}
            GROUP BY l.source_id, COALESCE(l.catalog_product_name, l.product_name, 'Unknown')
            -- product_name breaks ties: without it the order among equal
            -- quantities was whatever the plan happened to produce, so the
            -- same report could come out in a different order twice.
            ORDER BY l.source_id, quantity DESC, product_name
        """, params)

        # Group by source
        by_source: Dict[int, list] = {}
        for row in results:
            sid = int(row[0])
            if sid not in source_names:
                continue
            by_source.setdefault(sid, []).append({
                "product_name": row[1],
                "quantity": int(row[2] or 0),
            })

        return by_source

    # ─── Promocode Analytics ─────────────────────────────────────────────────

    async def get_promocode_analytics(
        self,
        start_date: date,
        end_date: date,
        sales_type: str = "retail",
    ) -> Dict[str, Any]:
        """Get promocode performance overview: top codes by revenue, orders, AOV."""
        params = [start_date, end_date]
        where_clauses = [
            "s.order_date BETWEEN ? AND ?",
            "NOT s.is_return",
            "s.is_active_source",
            "s.promocode IS NOT NULL",
            "s.promocode != ''",
        ]

        if sales_type != "all":
            where_clauses.append("s.sales_type = ?")
            params.append(sales_type)

        where_sql = " AND ".join(where_clauses)

        # Per-promocode stats
        results = await self._marketing_run(f"""
            SELECT
                s.promocode,
                COUNT(DISTINCT s.id) as orders,
                COALESCE(SUM(s.grand_total), 0) as revenue,
                COUNT(DISTINCT s.buyer_id) as unique_customers
            FROM {{silver_orders}} s
            WHERE {where_sql}
            GROUP BY s.promocode
            -- `promocode` closes the sort: codes can tie on revenue, and
            -- the two top-10 lists below are cut from this order.
            ORDER BY revenue DESC, s.promocode
        """, params)

        # Total orders (with and without promo) for share calculation
        total_params = [start_date, end_date]
        total_where = ["s.order_date BETWEEN ? AND ?", "NOT s.is_return", "s.is_active_source"]
        if sales_type != "all":
            total_where.append("s.sales_type = ?")
            total_params.append(sales_type)
        totals = (await self._marketing_run(f"""
            SELECT COUNT(DISTINCT s.id), COALESCE(SUM(s.grand_total), 0)
            FROM {{silver_orders}} s
            WHERE {" AND ".join(total_where)}
        """, total_params))[0]

        total_all_orders = int(totals[0] or 0)
        total_all_revenue = float(totals[1] or 0)

        promo_colors = [
            "#7C3AED", "#2563EB", "#16A34A", "#F59E0B", "#eb4200",
            "#EC4899", "#8B5CF6", "#06B6D4", "#14B8A6", "#EF4444",
        ]

        promo_total_orders = sum(int(row[1]) for row in results)
        promo_total_revenue = sum(float(row[2]) for row in results)
        promo_total_customers = sum(int(row[3]) for row in results)

        # Top 10 by revenue
        top_revenue = results[:10]
        top_by_revenue = {
            "labels": [row[0] for row in top_revenue],
            "data": [round(float(row[2]), 2) for row in top_revenue],
            "orders": [int(row[1]) for row in top_revenue],
            "backgroundColor": promo_colors[:len(top_revenue)],
        }

        # Top 10 by orders
        sorted_by_orders = sorted(results, key=lambda x: x[1], reverse=True)[:10]
        top_by_orders = {
            "labels": [row[0] for row in sorted_by_orders],
            "data": [int(row[1]) for row in sorted_by_orders],
            "revenue": [round(float(row[2]), 2) for row in sorted_by_orders],
            "backgroundColor": promo_colors[:len(sorted_by_orders)],
        }

        # Table: all codes with full metrics
        table = []
        for row in results:
            orders = int(row[1])
            revenue = float(row[2])
            table.append({
                "promocode": row[0],
                "orders": orders,
                "revenue": round(revenue, 2),
                "uniqueCustomers": int(row[3]),
                "aov": round(revenue / orders, 2) if orders > 0 else 0,
            })

        top_code = results[0][0] if results else "N/A"
        top_code_revenue = float(results[0][2]) if results else 0
        top_code_share = (top_code_revenue / total_all_revenue * 100) if total_all_revenue > 0 else 0
        promo_order_share = (promo_total_orders / total_all_orders * 100) if total_all_orders > 0 else 0

        return {
            "topByRevenue": top_by_revenue,
            "topByOrders": top_by_orders,
            "table": table,
            "metrics": {
                "totalCodes": len(results),
                "topCode": top_code,
                "topCodeShare": round(top_code_share, 1),
                "promoOrders": promo_total_orders,
                "promoRevenue": round(promo_total_revenue, 2),
                "promoOrderShare": round(promo_order_share, 1),
                "promoCustomers": promo_total_customers,
                "promoAov": round(promo_total_revenue / promo_total_orders, 2) if promo_total_orders > 0 else 0,
            },
        }

    # ─── Marketing Report ────────────────────────────────────────────────────

    async def get_marketing_report(
        self,
        year: int,
        month: int,
        sales_type: str = "retail",
    ) -> Dict[str, Any]:
        """Get monthly marketing report (legacy month-based)."""
        from calendar import monthrange
        start_date = date(year, month, 1)
        _, last_day = monthrange(year, month)
        end_date = date(year, month, last_day)
        return await self.get_marketing_report_by_dates(start_date, end_date, sales_type)

    async def get_marketing_report_by_dates(
        self,
        start_date: date,
        end_date: date,
        sales_type: str = "retail",
    ) -> Dict[str, Any]:
        """Get marketing report for arbitrary date range with previous period and YoY comparison."""
        from datetime import timedelta

        period_days = (end_date - start_date).days + 1

        # Previous period: same duration immediately before
        prev_end = start_date - timedelta(days=1)
        prev_start = prev_end - timedelta(days=period_days - 1)

        # Year ago: same dates shifted back 1 year
        yoy_start = start_date.replace(year=start_date.year - 1)
        yoy_end = end_date.replace(year=end_date.year - 1)

        sales_where = "sales_type = ?" if sales_type != "all" else "1=1"
        sales_params = [sales_type] if sales_type != "all" else []

        # The eleven period figures. `{period_measures}` is the one hole here
        # that is not a table name: DuckDB names a channel with a column,
        # Postgres with a `source_id` dimension, and the five scalars must be
        # read from the roll-up rows only — three of them are
        # `COUNT(DISTINCT buyer_id)`, and distinct counts do not add up.
        _MONTH_SQL = """
            SELECT
                {period_measures}
            FROM {gold_daily_revenue}
            WHERE date BETWEEN ? AND ? AND %s
        """ % sales_where

        async def _fetch_month(sd, ed):
                rows = await self._marketing_run(
                    _MONTH_SQL, [sd, ed] + sales_params,
                )
                row = rows[0]

                revenue = float(row[0])
                orders = int(row[1])
                customers = int(row[2])
                new_cust = int(row[3])
                ret_cust = int(row[4])

                return {
                    "revenue": round(revenue, 2),
                    "orders": orders,
                    "avg_check": round(revenue / orders, 2) if orders > 0 else 0,
                    "customers": customers,
                    "new_customers": new_cust,
                    "returning_customers": ret_cust,
                    "return_rate": round(ret_cust / customers * 100, 1) if customers > 0 else 0,
                }, {
                    "instagram": {"revenue": round(float(row[5]), 2), "orders": int(row[8])},
                    "telegram": {"revenue": round(float(row[6]), 2), "orders": int(row[9])},
                    "shopify": {"revenue": round(float(row[7]), 2), "orders": int(row[10])},
                }

        current, cur_sources = await _fetch_month(start_date, end_date)
        previous, _ = await _fetch_month(prev_start, prev_end)
        year_ago, _ = await _fetch_month(yoy_start, yoy_end)

        # Brands, from the order-line level rather than `gold_daily_products`.
        #
        # That table exists only in DuckDB, and deriving it here was the
        # obvious move and the wrong size: `core/duckdb_store.py` records that
        # the level reproduces it to the kopeck under Gold's own predicate —
        # ₴132,077,453.75 on both sides, 986 dates, zero disagreeing.
        #
        # The inner grouping is not decoration. `order_count` in that table is
        # `COUNT(DISTINCT id)` at the grain (date, sales_type, source, product,
        # name-as-sold, brand, category…), and the report *sums* it. A plain
        # `COUNT(DISTINCT order_id)` per brand is a different, smaller number —
        # correct-looking and not the one this report has always shown. So the
        # grain is reproduced first and summed after.
        brand_where = "l.sales_type = ?" if sales_type != "all" else "1=1"
        brand_params = [start_date, end_date] + sales_params
        brand_results = await self._marketing_run(f"""
            WITH product_days AS (
                SELECT
                    l.brand,
                    SUM(l.quantity) AS quantity_sold,
                    SUM(l.line_amount) AS product_revenue,
                    COUNT(DISTINCT l.order_id) AS order_count
                FROM {{order_lines}} l
                WHERE NOT l.is_return
                  AND l.is_active_source
                  AND l.order_date BETWEEN ? AND ?
                  AND {brand_where}
                GROUP BY
                    l.order_date, l.sales_type, l.source_id, l.product_id,
                    l.product_name, l.brand, l.category_id, l.category_name,
                    l.parent_category_name
            )
            SELECT
                COALESCE(brand, 'Unknown') AS brand_name,
                SUM(product_revenue) AS revenue,
                SUM(order_count) AS orders,
                SUM(quantity_sold) AS quantity
            FROM product_days
            GROUP BY COALESCE(brand, 'Unknown')
            -- Repeated rather than aliased: PostgreSQL does not see a select
            -- alias in HAVING, and `HAVING revenue > 0` is what the DuckDB
            -- original wrote.
            HAVING SUM(product_revenue) > 0
            -- Brands can tie on revenue and the page renders them in order.
            ORDER BY revenue DESC, brand_name
        """, brand_params)

        total_brand_revenue = sum(float(r[1]) for r in brand_results)
        brands = []
        for r in brand_results:
            rev = float(r[1])
            ord_count = int(r[2])
            brands.append({
                "brand": r[0],
                "revenue": round(rev, 2),
                "orders": ord_count,
                "avg_check": round(rev / ord_count, 2) if ord_count > 0 else 0,
                "share_pct": round(rev / total_brand_revenue * 100, 1) if total_brand_revenue > 0 else 0,
            })

        # The target line. Postgres holds an hourly read replica of the goals
        # (revision 0017); the goals API still writes DuckDB, so a target set
        # this hour appears here next hour. That is a display lag on a figure
        # changed a few times a year.
        goal_rows = await self._marketing_run(
            "SELECT goal_amount FROM {revenue_goals} WHERE period_type = 'monthly'"
        )
        monthly_goal = float(goal_rows[0][0]) if goal_rows else None

        # Build sources list
        total_orders = current["orders"]
        total_revenue = current["revenue"]
        sources = []
        for key, name in [("shopify", "Сайт"), ("telegram", "Telegram"), ("instagram", "Instagram")]:
            s = cur_sources[key]
            sources.append({
                "source_name": name,
                "orders": s["orders"],
                "revenue": round(s["revenue"], 2),
                "orders_pct": round(s["orders"] / total_orders * 100, 1) if total_orders > 0 else 0,
                "revenue_pct": round(s["revenue"] / total_revenue * 100, 1) if total_revenue > 0 else 0,
            })
        sources.sort(key=lambda x: x["revenue"], reverse=True)

        # Check if this is a full calendar month (for goal display)
        from calendar import monthrange
        is_full_month = (
            start_date.day == 1
            and end_date.day == monthrange(end_date.year, end_date.month)[1]
            and start_date.month == end_date.month
            and start_date.year == end_date.year
        )

        return {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "prev_start_date": prev_start.isoformat(),
            "prev_end_date": prev_end.isoformat(),
            "yoy_start_date": yoy_start.isoformat(),
            "yoy_end_date": yoy_end.isoformat(),
            "general_sales": {
                "current": current,
                "previous": previous,
                "year_ago": year_ago,
                "monthly_goal": monthly_goal if is_full_month else None,
            },
            "brands": brands,
            "sources": sources,
        }

    # ─── Expense Methods ─────────────────────────────────────────────────────
