"""
Goals repository for revenue goals and seasonality calculations.

Handles smart goal-setting based on historical data.
"""
from datetime import date, datetime, timedelta
from typing import Optional, Dict, Any

from core.repositories.base import BaseRepository, _date_in_kyiv
from core.models import OrderStatus
from core.observability import get_logger

logger = get_logger(__name__)


class GoalsRepository(BaseRepository):
    """Repository for goals and seasonality analysis."""

    async def get_goals(self, sales_type: str = "retail") -> Dict[str, Dict[str, Any]]:
        """
        Get revenue goals for all periods.

        Returns:
            Dict with daily, weekly, monthly goals
        """
        async with self.connection() as conn:
            result = conn.execute("""
                SELECT
                    period_type,
                    goal_amount,
                    is_custom,
                    calculated_goal,
                    growth_factor
                FROM revenue_goals
                WHERE sales_type = ?
            """, [sales_type]).fetchall()

            goals = {}
            for row in result:
                goals[row[0]] = {
                    "goal": float(row[1] or 0),
                    "is_custom": row[2],
                    "calculated_goal": float(row[3] or 0) if row[3] else None,
                    "growth_factor": float(row[4] or 1.1)
                }

            # Ensure all periods exist
            for period in ["daily", "weekly", "monthly"]:
                if period not in goals:
                    goals[period] = {
                        "goal": 0,
                        "is_custom": False,
                        "calculated_goal": None,
                        "growth_factor": 1.1
                    }

            return goals

    async def set_goal(
        self,
        period_type: str,
        goal_amount: float,
        is_custom: bool = True,
        sales_type: str = "retail"
    ) -> Dict[str, Any]:
        """
        Set a revenue goal for a period.

        Args:
            period_type: 'daily', 'weekly', or 'monthly'
            goal_amount: Target revenue amount
            is_custom: Whether this is a manual override
            sales_type: 'retail', 'b2b', or 'all'

        Returns:
            Updated goal info
        """
        async with self.connection() as conn:
            conn.execute("""
                INSERT INTO revenue_goals (period_type, sales_type, goal_amount, is_custom, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT (period_type, sales_type) DO UPDATE SET
                    goal_amount = excluded.goal_amount,
                    is_custom = excluded.is_custom,
                    updated_at = CURRENT_TIMESTAMP
            """, [period_type, sales_type, goal_amount, is_custom])

            return {
                "period_type": period_type,
                "goal": goal_amount,
                "is_custom": is_custom
            }

    async def reset_goal_to_auto(
        self,
        period_type: str,
        sales_type: str = "retail"
    ) -> Dict[str, Any]:
        """
        Reset a goal to auto-calculated value.

        Returns:
            Updated goal info with calculated value
        """
        async with self.connection() as conn:
            # Get calculated goal
            result = conn.execute("""
                SELECT calculated_goal
                FROM revenue_goals
                WHERE period_type = ? AND sales_type = ?
            """, [period_type, sales_type]).fetchone()

            calculated = float(result[0]) if result and result[0] else 0

            # Update to use calculated value
            conn.execute("""
                UPDATE revenue_goals
                SET goal_amount = calculated_goal, is_custom = FALSE, updated_at = CURRENT_TIMESTAMP
                WHERE period_type = ? AND sales_type = ?
            """, [period_type, sales_type])

            return {
                "period_type": period_type,
                "goal": calculated,
                "is_custom": False
            }

    async def calculate_suggested_goals(
        self,
        growth_factor: float = 1.10,
        sales_type: str = "retail"
    ) -> Dict[str, float]:
        """
        Calculate suggested goals based on historical averages.

        Args:
            growth_factor: Target growth (1.10 = 10% growth)
            sales_type: Sales type filter

        Returns:
            Dict with daily, weekly, monthly suggested goals
        """
        async with self.connection() as conn:
            return_statuses = tuple(int(s) for s in OrderStatus.return_statuses())

            # Get average daily revenue for last 30 days
            thirty_days_ago = date.today() - timedelta(days=30)

            result = conn.execute(f"""
                SELECT
                    AVG(daily_revenue) as avg_daily,
                    SUM(daily_revenue) / 4 as avg_weekly,
                    SUM(daily_revenue) as monthly_total
                FROM (
                    SELECT
                        {_date_in_kyiv('ordered_at')} as order_date,
                        SUM(grand_total) as daily_revenue
                    FROM orders
                    WHERE {_date_in_kyiv('ordered_at')} >= ?
                      AND status_id NOT IN {return_statuses}
                      AND {self._build_sales_type_filter(sales_type)}
                    GROUP BY {_date_in_kyiv('ordered_at')}
                ) daily
            """, [thirty_days_ago]).fetchone()

            avg_daily = float(result[0] or 0) if result else 0
            avg_weekly = float(result[1] or 0) if result else 0
            monthly_total = float(result[2] or 0) if result else 0

            suggestions = {
                "daily": round(avg_daily * growth_factor, 2),
                "weekly": round(avg_weekly * growth_factor, 2),
                "monthly": round(monthly_total * growth_factor, 2)
            }

            # Store calculated goals
            for period, amount in suggestions.items():
                conn.execute("""
                    INSERT INTO revenue_goals (period_type, sales_type, goal_amount, calculated_goal, is_custom, growth_factor, updated_at)
                    VALUES (?, ?, ?, ?, FALSE, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT (period_type, sales_type) DO UPDATE SET
                        calculated_goal = excluded.calculated_goal,
                        growth_factor = excluded.growth_factor,
                        updated_at = CURRENT_TIMESTAMP
                """, [period, sales_type, amount, amount, growth_factor])

            return suggestions

    async def calculate_seasonality_indices(
        self,
        sales_type: str = "retail"
    ) -> Dict[int, Dict[str, Any]]:
        """
        Calculate monthly seasonality indices.

        Seasonality index > 1.0 means above average month,
        < 1.0 means below average.

        Returns:
            Dict mapping month (1-12) to seasonality data
        """
        async with self.connection() as conn:
            return_statuses = tuple(int(s) for s in OrderStatus.return_statuses())

            result = conn.execute(f"""
                WITH monthly_revenue AS (
                    SELECT
                        EXTRACT(YEAR FROM {_date_in_kyiv('ordered_at')}) as year,
                        EXTRACT(MONTH FROM {_date_in_kyiv('ordered_at')}) as month,
                        SUM(grand_total) as revenue
                    FROM orders
                    WHERE status_id NOT IN {return_statuses}
                      AND {self._build_sales_type_filter(sales_type)}
                    GROUP BY 1, 2
                ),
                avg_monthly AS (
                    SELECT AVG(revenue) as overall_avg FROM monthly_revenue
                )
                SELECT
                    m.month,
                    AVG(m.revenue) as avg_revenue,
                    MIN(m.revenue) as min_revenue,
                    MAX(m.revenue) as max_revenue,
                    COUNT(*) as sample_size,
                    AVG(m.revenue) / a.overall_avg as seasonality_index
                FROM monthly_revenue m
                CROSS JOIN avg_monthly a
                GROUP BY m.month, a.overall_avg
                ORDER BY m.month
            """).fetchall()

            indices = {}
            for row in result:
                month = int(row[0])
                sample_size = row[4]
                confidence = "high" if sample_size >= 3 else "medium" if sample_size >= 2 else "low"

                indices[month] = {
                    "avg_revenue": round(float(row[1] or 0), 2),
                    "min_revenue": round(float(row[2] or 0), 2),
                    "max_revenue": round(float(row[3] or 0), 2),
                    "sample_size": sample_size,
                    "seasonality_index": round(float(row[5] or 1.0), 4),
                    "confidence": confidence
                }

                # Store in database
                conn.execute("""
                    INSERT INTO seasonal_indices (month, sales_type, seasonality_index, sample_size, avg_revenue, min_revenue, max_revenue, confidence, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT (month, sales_type) DO UPDATE SET
                        seasonality_index = excluded.seasonality_index,
                        sample_size = excluded.sample_size,
                        avg_revenue = excluded.avg_revenue,
                        min_revenue = excluded.min_revenue,
                        max_revenue = excluded.max_revenue,
                        confidence = excluded.confidence,
                        updated_at = CURRENT_TIMESTAMP
                """, [
                    month, sales_type,
                    indices[month]["seasonality_index"],
                    sample_size,
                    indices[month]["avg_revenue"],
                    indices[month]["min_revenue"],
                    indices[month]["max_revenue"],
                    confidence
                ])

            return indices

    async def calculate_yoy_growth(
        self,
        sales_type: str = "retail"
    ) -> Dict[str, Any]:
        """
        Calculate year-over-year growth metrics.

        Returns:
            Dict with overall YoY growth and monthly breakdown
        """
        async with self.connection() as conn:
            return_statuses = tuple(int(s) for s in OrderStatus.return_statuses())
            today = date.today()

            # Get this year vs last year revenue
            this_year_start = date(today.year, 1, 1)
            last_year_start = date(today.year - 1, 1, 1)
            last_year_end = date(today.year - 1, 12, 31)

            result = conn.execute(f"""
                SELECT
                    SUM(CASE WHEN {_date_in_kyiv('ordered_at')} >= ? THEN grand_total ELSE 0 END) as this_year,
                    SUM(CASE WHEN {_date_in_kyiv('ordered_at')} BETWEEN ? AND ? THEN grand_total ELSE 0 END) as last_year
                FROM orders
                WHERE status_id NOT IN {return_statuses}
                  AND {self._build_sales_type_filter(sales_type)}
            """, [this_year_start, last_year_start, last_year_end]).fetchone()

            this_year = float(result[0] or 0)
            last_year = float(result[1] or 0)

            yoy_growth = ((this_year - last_year) / last_year * 100) if last_year > 0 else 0

            # Store growth metric
            conn.execute("""
                INSERT INTO growth_metrics (metric_type, sales_type, value, period_start, period_end, sample_size, updated_at)
                VALUES ('yoy_overall', ?, ?, ?, ?, 2, CURRENT_TIMESTAMP)
                ON CONFLICT (metric_type, sales_type) DO UPDATE SET
                    value = excluded.value,
                    period_start = excluded.period_start,
                    period_end = excluded.period_end,
                    updated_at = CURRENT_TIMESTAMP
            """, [sales_type, yoy_growth / 100, last_year_start, today])

            return {
                "this_year_revenue": round(this_year, 2),
                "last_year_revenue": round(last_year, 2),
                "yoy_growth_percent": round(yoy_growth, 2),
                "period": f"{today.year} vs {today.year - 1}"
            }

    async def get_smart_goals(
        self,
        sales_type: str = "retail"
    ) -> Dict[str, Dict[str, Any]]:
        """
        Get smart goals adjusted for seasonality.

        Returns:
            Dict with goals adjusted by current month's seasonality
        """
        # Get base goals
        goals = await self.get_goals(sales_type)

        # Get seasonality for current month
        async with self.connection() as conn:
            current_month = date.today().month

            result = conn.execute("""
                SELECT seasonality_index, confidence
                FROM seasonal_indices
                WHERE month = ? AND sales_type = ?
            """, [current_month, sales_type]).fetchone()

            seasonality_index = float(result[0]) if result else 1.0
            confidence = result[1] if result else "low"

        # Adjust goals by seasonality
        for period in goals:
            base_goal = goals[period]["goal"]
            goals[period]["adjusted_goal"] = round(base_goal * seasonality_index, 2)
            goals[period]["seasonality_index"] = seasonality_index
            goals[period]["seasonality_confidence"] = confidence

        return goals
