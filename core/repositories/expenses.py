"""DuckDBStore expense methods."""
from __future__ import annotations

import logging
from datetime import timedelta
from typing import Optional, List, Dict, Any

from core.duckdb_constants import _date_in_kyiv

logger = logging.getLogger(__name__)


class ExpensesMixin:

    async def upsert_expense_types(self, expense_types: List[Dict[str, Any]]) -> int:
        """Insert or update expense types from API response."""
        if not expense_types:
            return 0

        async with self.connection() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                count = 0
                for et in expense_types:
                    name = et.get("name", "Unknown")
                    alias = et.get("alias")

                    # Clean up localization keys (e.g., "dictionaries.expense_types.delivery" -> "Delivery")
                    if name.startswith("dictionaries.expense_types."):
                        # Use alias as display name, formatted nicely
                        if alias:
                            name = alias.replace("_", " ").title()
                        else:
                            name = name.replace("dictionaries.expense_types.", "").replace("_", " ").title()

                    conn.execute("""
                        INSERT OR REPLACE INTO expense_types (id, name, alias, is_active, synced_at)
                        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """, [
                        et.get("id"),
                        name,
                        alias,
                        et.get("is_active", True)
                    ])
                    count += 1

                conn.execute("COMMIT")
                logger.info(f"Upserted {count} expense types to DuckDB")
                return count

            except Exception:
                conn.execute("ROLLBACK")
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
                conn.execute("ROLLBACK")
                raise

    async def upsert_expenses_batch(self, orders_with_expenses: List[Dict[str, Any]]) -> int:
        """
        Insert or update expenses for multiple orders in a single transaction.

        Args:
            orders_with_expenses: List of order dicts with 'id' and 'expenses' keys

        Returns:
            Total number of expenses upserted
        """
        # Flatten all expenses with their order IDs
        all_expenses = []
        for order in orders_with_expenses:
            order_id = order.get("id")
            for exp in order.get("expenses", []):
                all_expenses.append((order_id, exp))

        if not all_expenses:
            return 0

        async with self.connection() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                for order_id, exp in all_expenses:
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

                conn.execute("COMMIT")
                return len(all_expenses)

            except Exception:
                conn.execute("ROLLBACK")
                raise

    async def get_expense_types(self) -> List[Dict[str, Any]]:
        """Get all expense types for filter dropdown."""
        async with self.connection() as conn:
            results = conn.execute("""
                SELECT id, name, alias, is_active
                FROM expense_types
                WHERE is_active = TRUE
                ORDER BY name
            """).fetchall()
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
        async with self.connection() as conn:
            params = [start_date, end_date]
            where_clauses = [f"{_date_in_kyiv('o.ordered_at')} BETWEEN ? AND ?"]

            # Add sales type filter
            where_clauses.append(self._build_sales_type_filter(sales_type))

            if source_id:
                where_clauses.append("o.source_id = ?")
                params.append(source_id)

            if expense_type_id:
                where_clauses.append("e.expense_type_id = ?")
                params.append(expense_type_id)

            where_sql = " AND ".join(where_clauses)

            # Total expenses by type
            by_type_sql = f"""
                SELECT
                    COALESCE(et.name, 'Other') as type_name,
                    et.id as type_id,
                    SUM(e.amount) as total_amount,
                    COUNT(e.id) as expense_count
                FROM expenses e
                JOIN orders o ON e.order_id = o.id
                LEFT JOIN expense_types et ON e.expense_type_id = et.id
                WHERE {where_sql}
                GROUP BY et.id, et.name
                ORDER BY total_amount DESC
            """
            by_type_results = conn.execute(by_type_sql, params).fetchall()

            # Total expenses summary
            total_sql = f"""
                SELECT
                    COALESCE(SUM(e.amount), 0) as total_expenses,
                    COUNT(e.id) as expense_count,
                    COUNT(DISTINCT e.order_id) as orders_with_expenses
                FROM expenses e
                JOIN orders o ON e.order_id = o.id
                WHERE {where_sql}
            """
            total_result = conn.execute(total_sql, params).fetchone()

            # Daily expense trend
            trend_sql = f"""
                SELECT
                    {_date_in_kyiv('o.ordered_at')} as day,
                    SUM(e.amount) as total_expenses
                FROM expenses e
                JOIN orders o ON e.order_id = o.id
                WHERE {where_sql}
                GROUP BY {_date_in_kyiv('o.ordered_at')}
                ORDER BY day
            """
            trend_results = conn.execute(trend_sql, params).fetchall()
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
        async with self.connection() as conn:
            return_statuses = tuple(int(s) for s in OrderStatus.return_statuses())
            params = [start_date, end_date]
            where_clauses = [
                f"{_date_in_kyiv('o.ordered_at')} BETWEEN ? AND ?",
                f"o.status_id NOT IN {return_statuses}",
                self._build_sales_type_filter(sales_type)
            ]

            if source_id:
                where_clauses.append("o.source_id = ?")
                params.append(source_id)

            where_sql = " AND ".join(where_clauses)

            # Daily revenue and expenses
            daily_sql = f"""
                SELECT
                    {_date_in_kyiv('o.ordered_at')} as day,
                    SUM(o.grand_total) as revenue,
                    COALESCE(SUM(e.amount), 0) as expenses
                FROM orders o
                LEFT JOIN expenses e ON o.id = e.order_id
                WHERE {where_sql}
                GROUP BY {_date_in_kyiv('o.ordered_at')}
                ORDER BY day
            """
            results = conn.execute(daily_sql, params).fetchall()
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
            return result is not None

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

        async with self.connection() as conn:
            rows = conn.execute(f"""
                SELECT id, expense_date, category, expense_type, amount, currency, note, created_at, updated_at, platform
                FROM manual_expenses
                {where_clause}
                ORDER BY expense_date DESC, created_at DESC
                LIMIT ?
            """, params).fetchall()

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
        async with self.connection() as conn:
            rows = conn.execute("""
                SELECT platform, SUM(amount) as spend, COUNT(*) as entries
                FROM manual_expenses
                WHERE expense_date BETWEEN ? AND ?
                  AND category = 'marketing'
                  AND platform IS NOT NULL
                GROUP BY platform
            """, [start_date, end_date]).fetchall()

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

        async with self.connection() as conn:
            # Total
            total_row = conn.execute(f"""
                SELECT COALESCE(SUM(amount), 0) as total, COUNT(*) as count
                FROM manual_expenses
                {where_clause}
            """, params).fetchone()

            # By category
            category_rows = conn.execute(f"""
                SELECT category, SUM(amount) as total, COUNT(*) as count
                FROM manual_expenses
                {where_clause}
                GROUP BY category
                ORDER BY total DESC
            """, params).fetchall()

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
