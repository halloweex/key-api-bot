"""DuckDBStore expense methods."""
from __future__ import annotations

import logging
from datetime import timedelta
from typing import Optional, List, Dict, Any

from core.duckdb_constants import _date_in_kyiv
from core.models import OrderStatus

logger = logging.getLogger(__name__)


class ExpensesMixin:

    async def _expenses_run(
        self, sql: str, params: "list | None" = None, *, mode: str = "all",
    ):
        """Run one expense statement against whichever engine the flag names.

        `_fetch_one`/`_fetch_all` on the DuckDB side, not a bare
        `conn.execute` under `self.connection()`: they take the store lock
        themselves — so this must not, the lock is not reentrant and a nested
        acquisition hangs rather than raising — and they run the query in a
        thread bounded by `DEFAULT_QUERY_TIMEOUT`, interrupting it on expiry.
        The `/traffic` port lost that by writing the obvious thing, and it was
        worst exactly where it mattered: the DuckDB branch is what answers when
        Postgres is already down.

        The rendering happens here rather than at the call site, because a
        caller that picks the fragment from the flag renders one engine's shape
        into the other's fallback.
        """
        from core.sql_dialect import DUCKDB, POSTGRES, render_tables

        from core import pg_expenses_read

        params = list(params or [])
        # A statement that reads the mirrored landing needs its history to be
        # across; one that reads `manual_expenses` or the type dictionary does
        # not. Decided from the body rather than from a flag per method, so a
        # query that starts reading `{expenses}` tomorrow is gated the moment
        # it does.
        needs_history = "{expenses}" in sql
        if (pg_expenses_read.enabled() and pg_expenses_read.available()
                and (not needs_history or await pg_expenses_read.backfilled())):
            try:
                rows = await pg_expenses_read.fetch(
                    render_tables(sql, POSTGRES), params,
                )
                return (rows[0] if rows else None) if mode == "one" else rows
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "expenses: Postgres failed, falling back to DuckDB: %s",
                    exc, exc_info=True,
                )

        duck = render_tables(sql, DUCKDB)
        if mode == "one":
            return await self._fetch_one(duck, params)
        return await self._fetch_all(duck, params)

    async def upsert_expense_types(self, expense_types: List[Dict[str, Any]]) -> int:
        """Insert or update expense types from API response."""
        if not expense_types:
            return 0

        # Parsed once, in `core/landing_rows.py`, because Postgres receives the
        # same rows. The name cleanup — KeyCRM serves some names as
        # localisation keys — is a real transformation, and a second copy of it
        # would have the two stores disagreeing about what an expense is
        # *called*.
        from core.landing_rows import expense_type_rows

        rows = expense_type_rows(expense_types)

        async with self.connection() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                for row in rows:
                    conn.execute("""
                        INSERT OR REPLACE INTO expense_types (id, name, alias, is_active, synced_at)
                        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """, list(row))

                conn.execute("COMMIT")
                logger.info(f"Upserted {len(rows)} expense types to DuckDB")
                return len(rows)

            except Exception:
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass
                raise

    async def upsert_expenses(self, order_id: int, expenses: List[Dict[str, Any]]) -> int:
        """Insert or update expenses for an order."""
        if not expenses:
            return 0

        async with self.connection() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                count = 0
                for exp in expenses:
                    conn.execute("""
                        INSERT OR REPLACE INTO expenses
                        (id, order_id, expense_type_id, amount, description, status, payment_date, created_at, synced_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """, [
                        exp.get("id"),
                        order_id,
                        exp.get("expense_type_id"),
                        exp.get("amount", 0),
                        exp.get("description"),
                        exp.get("status"),
                        exp.get("payment_date"),
                        exp.get("created_at")
                    ])
                    count += 1

                conn.execute("COMMIT")
                return count

            except Exception:
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass
                raise

    async def upsert_expenses_batch(self, orders_with_expenses: List[Dict[str, Any]]) -> int:
        """
        Insert or update expenses for multiple orders in a single transaction.

        Args:
            orders_with_expenses: List of order dicts with 'id' and 'expenses' keys

        Returns:
            Total number of expenses upserted
        """
        # Flattened and parsed in one place, for `upsert_expense_types`' reason.
        from core.landing_rows import expense_rows

        rows = expense_rows(orders_with_expenses)
        if not rows:
            return 0

        async with self.connection() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                for row in rows:
                    conn.execute("""
                        INSERT OR REPLACE INTO expenses
                        (id, order_id, expense_type_id, amount, description, status, payment_date, created_at, synced_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """, list(row))

                conn.execute("COMMIT")
                return len(rows)

            except Exception:
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass
                raise

    async def get_expense_types(self) -> List[Dict[str, Any]]:
        """Get all expense types for filter dropdown."""
        results = await self._expenses_run("""
            SELECT id, name, alias, is_active
            FROM {expense_types}
            WHERE is_active = TRUE
            ORDER BY name, id
        """)
        return [{"id": row[0], "name": row[1], "alias": row[2]} for row in results]

    async def get_expense_summary(
        self,
        start_date: date,
        end_date: date,
        source_id: Optional[int] = None,
        expense_type_id: Optional[int] = None,
        sales_type: str = "retail"
    ) -> Dict[str, Any]:
        """Get expense summary for a date range."""
        params = [start_date, end_date]
        # `s.order_date` rather than `_date_in_kyiv(o.ordered_at)`: Silver
        # stores the Kyiv date, computed by the same expression, so reading
        # the column is the same answer without converting per row.
        where_clauses = ["s.order_date BETWEEN ? AND ?"]

        # The classification as a column, not an EXISTS against Silver from
        # raw orders. `_build_sales_type_filter` was already reading Silver
        # rather than re-deriving from `manager_id` — that subquery was the
        # fix for #101 — so this is the same fix taken one step further, and
        # it costs one join instead of two.
        if sales_type != "all":
            where_clauses.append("s.sales_type = ?")
            params.append(sales_type)

        if source_id:
            where_clauses.append("s.source_id = ?")
            params.append(source_id)

        if expense_type_id:
            where_clauses.append("e.expense_type_id = ?")
            params.append(expense_type_id)

        where_sql = " AND ".join(where_clauses)

        # These three sum `e.amount` only, never a column of the order, so the
        # join fans out harmlessly here — unlike `get_profit_analysis`, where
        # it did not.
        by_type_results = await self._expenses_run(f"""
            SELECT
                COALESCE(et.name, 'Other') as type_name,
                et.id as type_id,
                SUM(e.amount) as total_amount,
                COUNT(e.id) as expense_count
            FROM {{expenses}} e
            JOIN {{silver_orders}} s ON e.order_id = s.id
            LEFT JOIN {{expense_types}} et ON e.expense_type_id = et.id
            WHERE {where_sql}
            GROUP BY et.id, et.name
            ORDER BY total_amount DESC, et.id
        """, params)

        total_result = await self._expenses_run(f"""
            SELECT
                COALESCE(SUM(e.amount), 0) as total_expenses,
                COUNT(e.id) as expense_count,
                COUNT(DISTINCT e.order_id) as orders_with_expenses
            FROM {{expenses}} e
            JOIN {{silver_orders}} s ON e.order_id = s.id
            WHERE {where_sql}
        """, params, mode="one")

        trend_results = await self._expenses_run(f"""
            SELECT
                s.order_date as day,
                SUM(e.amount) as total_expenses
            FROM {{expenses}} e
            JOIN {{silver_orders}} s ON e.order_id = s.id
            WHERE {where_sql}
            GROUP BY s.order_date
            ORDER BY day
        """, params)
        daily_data = {row[0]: float(row[1]) for row in trend_results}

        # Build trend labels and data
        labels = []
        data = []
        current = start_date
        while current <= end_date:
            labels.append(current.strftime("%d.%m"))
            data.append(round(daily_data.get(current, 0), 2))
            current += timedelta(days=1)

        # Chart colors
        expense_colors = ["#EF4444", "#F59E0B", "#8B5CF6", "#06B6D4", "#EC4899", "#14B8A6", "#7C3AED", "#2563EB"]

        # By type breakdown for pie chart
        by_type_data = {
            "labels": [row[0] for row in by_type_results],
            "data": [round(float(row[2]), 2) for row in by_type_results],
            "counts": [row[3] for row in by_type_results],
            "backgroundColor": expense_colors[:len(by_type_results)]
        }

        total_expenses = float(total_result[0] or 0)
        expense_count = total_result[1] or 0
        orders_with_expenses = total_result[2] or 0

        return {
            "byType": by_type_data,
            "trend": {
                "labels": labels,
                "datasets": [{
                    "label": "Expenses (UAH)",
                    "data": data,
                    "borderColor": "#EF4444",
                    "backgroundColor": "rgba(239, 68, 68, 0.1)",
                    "fill": True,
                    "tension": 0.3,
                    "borderWidth": 2
                }]
            },
            "metrics": {
                "totalExpenses": round(total_expenses, 2),
                "expenseCount": expense_count,
                "ordersWithExpenses": orders_with_expenses,
                "avgExpensePerOrder": round(total_expenses / orders_with_expenses, 2) if orders_with_expenses > 0 else 0
            }
        }

    async def get_profit_analysis(
        self,
        start_date: date,
        end_date: date,
        source_id: Optional[int] = None,
        sales_type: str = "retail"
    ) -> Dict[str, Any]:
        """Get profit analysis: revenue vs expenses."""
        params = [start_date, end_date]
        # Silver carries all three: the Kyiv date as a column, the return flag,
        # and the classification. `NOT is_return` was checked against the old
        # `status_id NOT IN (15,18,19,21,22,23)` over the whole catalogue —
        # 47,338 orders, zero disagreements — so this is a rename, not a
        # redefinition.
        where_clauses = [
            "s.order_date BETWEEN ? AND ?",
            "NOT s.is_return",
        ]
        if sales_type != "all":
            where_clauses.append("s.sales_type = ?")
            params.append(sales_type)

        if source_id:
            where_clauses.append("s.source_id = ?")
            params.append(source_id)

        where_sql = " AND ".join(where_clauses)

        # THE EXPENSES ARE FOLDED BEFORE THE JOIN, AND THAT IS A BUG FIX.
        #
        # This read `FROM orders o LEFT JOIN expenses e` and then summed
        # `o.grand_total` across the joined rows — so an order carrying two
        # expenses had its revenue counted twice. 157 orders have more than
        # one, and the effect is not academic: measured on the production
        # catalogue, the old query reports ₴16,433,644.56 over 90 days where
        # the truth is ₴16,119,279.06 — **₴314,365.50 too much, and ₴62,000 on
        # 2026-07-28 alone.**
        #
        # Folding first gives one row per order, which reproduces the true
        # revenue and the true expense total to the kopeck on every one of
        # those 90 days. Fixed in the shared body, so both engines get it;
        # porting the fan-out faithfully would have meant keeping a wrong
        # number so that two stores could agree on it.
        results = await self._expenses_run(f"""
            SELECT
                s.order_date as day,
                SUM(s.grand_total) as revenue,
                COALESCE(SUM(x.amount), 0) as expenses
            FROM {{silver_orders}} s
            LEFT JOIN (
                SELECT order_id, SUM(amount) AS amount
                FROM {{expenses}}
                GROUP BY order_id
            ) x ON x.order_id = s.id
            WHERE {where_sql}
            GROUP BY s.order_date
            ORDER BY day
        """, params)
        daily_data = {row[0]: {"revenue": float(row[1]), "expenses": float(row[2])} for row in results}

        # Build chart data
        labels = []
        revenue_data = []
        expenses_data = []
        profit_data = []
        current = start_date
        while current <= end_date:
            labels.append(current.strftime("%d.%m"))
            day_data = daily_data.get(current, {"revenue": 0, "expenses": 0})
            revenue = day_data["revenue"]
            expenses = day_data["expenses"]
            revenue_data.append(round(revenue, 2))
            expenses_data.append(round(expenses, 2))
            profit_data.append(round(revenue - expenses, 2))
            current += timedelta(days=1)

        total_revenue = sum(revenue_data)
        total_expenses = sum(expenses_data)
        total_profit = total_revenue - total_expenses
        profit_margin = (total_profit / total_revenue * 100) if total_revenue > 0 else 0

        return {
            "chart": {
                "labels": labels,
                "datasets": [
                    {
                        "label": "Revenue",
                        "data": revenue_data,
                        "borderColor": "#16A34A",
                        "backgroundColor": "rgba(22, 163, 74, 0.1)",
                        "fill": False,
                        "tension": 0.3
                    },
                    {
                        "label": "Expenses",
                        "data": expenses_data,
                        "borderColor": "#EF4444",
                        "backgroundColor": "rgba(239, 68, 68, 0.1)",
                        "fill": False,
                        "tension": 0.3
                    },
                    {
                        "label": "Gross Profit",
                        "data": profit_data,
                        "borderColor": "#2563EB",
                        "backgroundColor": "rgba(37, 99, 235, 0.2)",
                        "fill": True,
                        "tension": 0.3
                    }
                ]
            },
            "metrics": {
                "totalRevenue": round(total_revenue, 2),
                "totalExpenses": round(total_expenses, 2),
                "grossProfit": round(total_profit, 2),
                "profitMargin": round(profit_margin, 1)
            }
        }

    # ─── Goal Methods ─────────────────────────────────────────────────────────

    async def add_expense(
        self,
        expense_date: date,
        category: str,
        expense_type: str,
        amount: float,
        currency: str = "UAH",
        note: Optional[str] = None,
        platform: Optional[str] = None
    ) -> Dict[str, Any]:
        """Add a manual expense.

        Args:
            expense_date: Date of the expense
            category: Category (marketing, salary, taxes, logistics, other)
            expense_type: Type (Facebook Ads, Google Ads, Salary, etc.)
            amount: Amount in specified currency
            currency: Currency code (default UAH)
            note: Optional note
            platform: Optional ad platform (facebook, tiktok, google)

        Returns:
            Created expense dict with id
        """
        async with self.connection() as conn:
            result = conn.execute("""
                INSERT INTO manual_expenses (expense_date, category, expense_type, amount, currency, note, platform)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                RETURNING id, expense_date, category, expense_type, amount, currency, note, created_at, platform
            """, [expense_date, category, expense_type, amount, currency, note, platform]).fetchone()

        # And on to Postgres now, not within the hour. The form writes this
        # store and the page it returns to reads the other once
        # KS_READ_EXPENSES=postgres, so without this the amount a human just
        # typed does not appear. Never raises — the DuckDB write has already
        # committed.
        from core.pg_operational import replicate_after_manual_expense

        await replicate_after_manual_expense(self)

        return {
            "id": result[0],
            "expense_date": result[1].isoformat() if result[1] else None,
            "category": result[2],
            "expense_type": result[3],
            "amount": float(result[4]),
            "currency": result[5],
            "note": result[6],
            "created_at": result[7].isoformat() if result[7] else None,
            "platform": result[8],
        }

    async def update_expense(
        self,
        expense_id: int,
        expense_date: Optional[date] = None,
        category: Optional[str] = None,
        expense_type: Optional[str] = None,
        amount: Optional[float] = None,
        currency: Optional[str] = None,
        note: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Update a manual expense.

        Args:
            expense_id: ID of expense to update
            Other args: Fields to update (None = keep existing)

        Returns:
            Updated expense dict or None if not found
        """
        async with self.connection() as conn:
            # Build dynamic update query
            updates = []
            params = []

            if expense_date is not None:
                updates.append("expense_date = ?")
                params.append(expense_date)
            if category is not None:
                updates.append("category = ?")
                params.append(category)
            if expense_type is not None:
                updates.append("expense_type = ?")
                params.append(expense_type)
            if amount is not None:
                updates.append("amount = ?")
                params.append(amount)
            if currency is not None:
                updates.append("currency = ?")
                params.append(currency)
            if note is not None:
                updates.append("note = ?")
                params.append(note)

            if not updates:
                return None

            updates.append("updated_at = CURRENT_TIMESTAMP")
            params.append(expense_id)

            result = conn.execute(f"""
                UPDATE manual_expenses
                SET {', '.join(updates)}
                WHERE id = ?
                RETURNING id, expense_date, category, expense_type, amount, currency, note, created_at, updated_at
            """, params).fetchone()

        if not result:
            return None

        # Same reason as `add_expense`: the page that opens next reads the
        # other store. Outside the `async with` above on purpose — this awaits
        # the network, and the store lock is global.
        from core.pg_operational import replicate_after_manual_expense

        await replicate_after_manual_expense(self)

        return {
            "id": result[0],
            "expense_date": result[1].isoformat() if result[1] else None,
            "category": result[2],
            "expense_type": result[3],
            "amount": float(result[4]),
            "currency": result[5],
            "note": result[6],
            "created_at": result[7].isoformat() if result[7] else None,
            "updated_at": result[8].isoformat() if result[8] else None
        }

    async def delete_expense(self, expense_id: int) -> bool:
        """Delete a manual expense.

        Args:
            expense_id: ID of expense to delete

        Returns:
            True if deleted, False if not found
        """
        async with self.connection() as conn:
            result = conn.execute("""
                DELETE FROM manual_expenses WHERE id = ? RETURNING id
            """, [expense_id]).fetchone()

        if result is None:
            # Nothing was deleted, so there is nothing to carry across; a
            # replication here would be 116 ms spent re-shipping seven tables
            # unchanged.
            return False

        # And on to Postgres now, not within the hour. The form writes this
        # store and the page it returns to reads the other once
        # KS_READ_EXPENSES=postgres, so without this the row a human just
        # removed is still on the page. Never raises — the DuckDB write has
        # already committed.
        from core.pg_operational import replicate_after_manual_expense

        await replicate_after_manual_expense(self)

        return True

    async def list_expenses(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        category: Optional[str] = None,
        platform: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """List manual expenses with optional filters.

        Args:
            start_date: Filter by start date
            end_date: Filter by end date
            category: Filter by category
            platform: Filter by ad platform (facebook, tiktok, google)
            limit: Max results

        Returns:
            List of expense dicts
        """
        conditions = []
        params = []

        if start_date:
            conditions.append("expense_date >= ?")
            params.append(start_date)
        if end_date:
            conditions.append("expense_date <= ?")
            params.append(end_date)
        if category:
            conditions.append("category = ?")
            params.append(category)
        if platform:
            conditions.append("platform = ?")
            params.append(platform)

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(limit)

        # `id DESC` closes the tie: two expenses entered on the same day with
        # the same `created_at` would otherwise come back in whichever order
        # each engine happened to produce, and under a LIMIT that is a row
        # missing from the page rather than a row out of order.
        rows = await self._expenses_run(f"""
            SELECT id, expense_date, category, expense_type, amount, currency, note, created_at, updated_at, platform
            FROM {{manual_expenses}}
            {where_clause}
            ORDER BY expense_date DESC, created_at DESC, id DESC
            LIMIT ?
        """, params)

        return [
            {
                "id": row[0],
                "expense_date": row[1].isoformat() if row[1] else None,
                "category": row[2],
                "expense_type": row[3],
                "amount": float(row[4]),
                "currency": row[5],
                "note": row[6],
                "created_at": row[7].isoformat() if row[7] else None,
                "updated_at": row[8].isoformat() if row[8] else None,
                "platform": row[9],
            }
            for row in rows
        ]

    async def get_ad_spend_by_platform(
        self,
        start_date: date,
        end_date: date
    ) -> Dict[str, Any]:
        """Get ad spend aggregated by platform for a date range.

        Args:
            start_date: Start date
            end_date: End date

        Returns:
            Dict with by_platform breakdown and total_spend
        """
        rows = await self._expenses_run("""
            SELECT platform, SUM(amount) as spend, COUNT(*) as entries
            FROM {manual_expenses}
            WHERE expense_date BETWEEN ? AND ?
              AND category = 'marketing'
              AND platform IS NOT NULL
            GROUP BY platform
            ORDER BY platform
        """, [start_date, end_date])

        by_platform = {}
        total_spend = 0.0
        for row in rows:
            platform, spend, entries = row
            spend_val = float(spend)
            by_platform[platform] = {"spend": round(spend_val, 2), "entries": int(entries)}
            total_spend += spend_val

        return {
            "by_platform": by_platform,
            "total_spend": round(total_spend, 2),
        }

    async def get_expenses_summary(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> Dict[str, Any]:
        """Get expenses summary with totals by category.

        Args:
            start_date: Filter by start date
            end_date: Filter by end date

        Returns:
            Summary dict with total and by-category breakdown
        """
        conditions = []
        params = []

        if start_date:
            conditions.append("expense_date >= ?")
            params.append(start_date)
        if end_date:
            conditions.append("expense_date <= ?")
            params.append(end_date)

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        total_row = await self._expenses_run(f"""
            SELECT COALESCE(SUM(amount), 0) as total, COUNT(*) as count
            FROM {{manual_expenses}}
            {where_clause}
        """, params, mode="one")

        category_rows = await self._expenses_run(f"""
            SELECT category, SUM(amount) as total, COUNT(*) as count
            FROM {{manual_expenses}}
            {where_clause}
            GROUP BY category
            ORDER BY total DESC, category
        """, params)

        return {
            "total": float(total_row[0]),
            "count": int(total_row[1]),
            "by_category": [
                {"category": row[0], "total": float(row[1]), "count": int(row[2])}
                for row in category_rows
            ]
        }

    # ═══════════════════════════════════════════════════════════════════════════
    # USER MANAGEMENT
    # ═══════════════════════════════════════════════════════════════════════════
