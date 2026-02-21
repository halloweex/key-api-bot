"""DuckDBStore goals and forecast methods."""
from __future__ import annotations

import logging
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any
from zoneinfo import ZoneInfo

from core.duckdb_constants import DEFAULT_TZ, _date_in_kyiv
from core.models import OrderStatus

logger = logging.getLogger(__name__)


class GoalsMixin:

    async def get_historical_revenue(
        self,
        period_type: str,
        weeks_back: int = 4,
        sales_type: str = "retail"
    ) -> Dict[str, Any]:
        """
        Get historical revenue data for goal calculation.

        Args:
            period_type: 'daily', 'weekly', or 'monthly'
            weeks_back: Number of weeks of history to analyze
            sales_type: 'retail', 'b2b', or 'all'

        Returns:
            Historical stats including average, min, max, and trend
        """
        async with self.connection() as conn:
            return_statuses = tuple(int(s) for s in OrderStatus.return_statuses())
            sales_filter = self._build_sales_type_filter(sales_type)

            if period_type == "daily":
                # Get daily averages for the same day of week over past N weeks
                sql = f"""
                    WITH daily_revenue AS (
                        SELECT
                            {_date_in_kyiv('o.ordered_at')} as day,
                            SUM(o.grand_total) as revenue
                        FROM orders o
                        WHERE {_date_in_kyiv('o.ordered_at')} >= CURRENT_DATE - INTERVAL ? DAY
                            AND {_date_in_kyiv('o.ordered_at')} < CURRENT_DATE
                            AND o.status_id NOT IN {return_statuses}
                            AND {sales_filter}
                        GROUP BY {_date_in_kyiv('o.ordered_at')}
                    )
                    SELECT
                        AVG(revenue) as avg_revenue,
                        MIN(revenue) as min_revenue,
                        MAX(revenue) as max_revenue,
                        COUNT(*) as days_count,
                        STDDEV(revenue) as std_dev
                    FROM daily_revenue
                """
                sql_params = [weeks_back * 7]
            elif period_type == "weekly":
                # Get weekly totals for past N weeks
                sql = f"""
                    WITH weekly_revenue AS (
                        SELECT
                            DATE_TRUNC('week', {_date_in_kyiv('o.ordered_at')}) as week_start,
                            SUM(o.grand_total) as revenue
                        FROM orders o
                        WHERE {_date_in_kyiv('o.ordered_at')} >= CURRENT_DATE - INTERVAL ? DAY
                            AND {_date_in_kyiv('o.ordered_at')} < DATE_TRUNC('week', CURRENT_DATE)
                            AND o.status_id NOT IN {return_statuses}
                            AND {sales_filter}
                        GROUP BY DATE_TRUNC('week', {_date_in_kyiv('o.ordered_at')})
                    )
                    SELECT
                        AVG(revenue) as avg_revenue,
                        MIN(revenue) as min_revenue,
                        MAX(revenue) as max_revenue,
                        COUNT(*) as weeks_count,
                        STDDEV(revenue) as std_dev
                    FROM weekly_revenue
                """
                sql_params = [weeks_back * 7]
            else:  # monthly
                # Get monthly totals for past N months
                months_back = max(3, weeks_back // 4)
                sql = f"""
                    WITH monthly_revenue AS (
                        SELECT
                            DATE_TRUNC('month', {_date_in_kyiv('o.ordered_at')}) as month_start,
                            SUM(o.grand_total) as revenue
                        FROM orders o
                        WHERE {_date_in_kyiv('o.ordered_at')} >= CURRENT_DATE - INTERVAL '{int(months_back)} months'
                            AND {_date_in_kyiv('o.ordered_at')} < DATE_TRUNC('month', CURRENT_DATE)
                            AND o.status_id NOT IN {return_statuses}
                            AND {sales_filter}
                        GROUP BY DATE_TRUNC('month', {_date_in_kyiv('o.ordered_at')})
                    )
                    SELECT
                        AVG(revenue) as avg_revenue,
                        MIN(revenue) as min_revenue,
                        MAX(revenue) as max_revenue,
                        COUNT(*) as months_count,
                        STDDEV(revenue) as std_dev
                    FROM monthly_revenue
                """
                sql_params = []

            result = conn.execute(sql, sql_params).fetchone()

            avg_revenue = float(result[0] or 0)
            min_revenue = float(result[1] or 0)
            max_revenue = float(result[2] or 0)
            period_count = result[3] or 0
            std_dev = float(result[4] or 0)

            # Calculate trend (compare recent vs older periods)
            trend = 0.0
            if period_type == "weekly" and period_count >= 4:
                trend_sql = f"""
                    WITH weekly_revenue AS (
                        SELECT
                            DATE_TRUNC('week', {_date_in_kyiv('o.ordered_at')}) as week_start,
                            SUM(o.grand_total) as revenue,
                            ROW_NUMBER() OVER (ORDER BY DATE_TRUNC('week', {_date_in_kyiv('o.ordered_at')}) DESC) as week_num
                        FROM orders o
                        WHERE {_date_in_kyiv('o.ordered_at')} >= CURRENT_DATE - INTERVAL ? DAY
                            AND {_date_in_kyiv('o.ordered_at')} < DATE_TRUNC('week', CURRENT_DATE)
                            AND o.status_id NOT IN {return_statuses}
                            AND {sales_filter}
                        GROUP BY DATE_TRUNC('week', {_date_in_kyiv('o.ordered_at')})
                    )
                    SELECT
                        AVG(CASE WHEN week_num <= 2 THEN revenue END) as recent_avg,
                        AVG(CASE WHEN week_num > 2 THEN revenue END) as older_avg
                    FROM weekly_revenue
                """
                trend_result = conn.execute(trend_sql, [weeks_back * 7]).fetchone()
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
        async with self.connection() as conn:
            # Get stored goals
            results = conn.execute("""
                SELECT period_type, goal_amount, is_custom, calculated_goal, growth_factor
                FROM revenue_goals
            """).fetchall()

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

    async def calculate_seasonality_indices(self, sales_type: str = "retail") -> Dict[int, Dict[str, Any]]:
        """
        Calculate monthly seasonality indices from historical data.

        Analyzes all available historical data to determine how each month
        performs relative to the annual average.

        Args:
            sales_type: 'retail', 'b2b', or 'all'

        Returns:
            Dictionary mapping month (1-12) to seasonality data
        """
        async with self.connection() as conn:
            return_statuses = tuple(int(s) for s in OrderStatus.return_statuses())
            sales_filter = self._build_sales_type_filter(sales_type)

            # Get monthly revenue totals for all available history (only complete months with 25+ days)
            sql = f"""
                WITH monthly_data AS (
                    SELECT
                        EXTRACT(YEAR FROM {_date_in_kyiv('o.ordered_at')}) as year,
                        EXTRACT(MONTH FROM {_date_in_kyiv('o.ordered_at')}) as month,
                        SUM(o.grand_total) as revenue,
                        COUNT(DISTINCT DATE({_date_in_kyiv('o.ordered_at')})) as days_with_orders
                    FROM orders o
                    WHERE o.status_id NOT IN {return_statuses}
                        AND {sales_filter}
                    GROUP BY
                        EXTRACT(YEAR FROM {_date_in_kyiv('o.ordered_at')}),
                        EXTRACT(MONTH FROM {_date_in_kyiv('o.ordered_at')})
                    HAVING COUNT(DISTINCT DATE({_date_in_kyiv('o.ordered_at')})) >= 20
                ),
                monthly_stats AS (
                    SELECT
                        month,
                        AVG(revenue) as avg_revenue,
                        MIN(revenue) as min_revenue,
                        MAX(revenue) as max_revenue,
                        COUNT(*) as sample_size,
                        STDDEV(revenue) as std_dev
                    FROM monthly_data
                    GROUP BY month
                ),
                overall_avg AS (
                    SELECT AVG(avg_revenue) as grand_avg FROM monthly_stats
                )
                SELECT
                    ms.month,
                    ms.avg_revenue,
                    ms.min_revenue,
                    ms.max_revenue,
                    ms.sample_size,
                    ms.std_dev,
                    ms.avg_revenue / oa.grand_avg as seasonality_index
                FROM monthly_stats ms, overall_avg oa
                ORDER BY ms.month
            """

            results = conn.execute(sql).fetchall()

            indices = {}
            for row in results:
                month = int(row[0])
                sample_size = row[4] or 0

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

            # Store in database for caching — batch upsert to avoid row-by-row lock hold
            now = datetime.now(DEFAULT_TZ)
            rows_to_upsert = [
                [month, data["seasonality_index"], data["sample_size"],
                 data["avg_revenue"], data["min_revenue"], data["max_revenue"],
                 data["confidence"], now]
                for month, data in indices.items()
            ]
            if rows_to_upsert:
                conn.executemany("""
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
                """, rows_to_upsert)

            logger.info(f"Calculated seasonality indices for {len(indices)} months")
            return indices

    async def calculate_yoy_growth(self, sales_type: str = "retail") -> Dict[str, Any]:
        """
        Calculate year-over-year growth rate.

        Compares revenue between consecutive years to determine growth trend.

        Args:
            sales_type: 'retail', 'b2b', or 'all'

        Returns:
            Growth metrics including overall YoY, monthly YoY, and trend slope
        """
        async with self.connection() as conn:
            return_statuses = tuple(int(s) for s in OrderStatus.return_statuses())
            sales_filter = self._build_sales_type_filter(sales_type)

            # Get yearly totals
            yearly_sql = f"""
                SELECT
                    EXTRACT(YEAR FROM {_date_in_kyiv('o.ordered_at')}) as year,
                    SUM(o.grand_total) as revenue
                FROM orders o
                WHERE o.status_id NOT IN {return_statuses}
                    AND {sales_filter}
                GROUP BY EXTRACT(YEAR FROM {_date_in_kyiv('o.ordered_at')})
                ORDER BY year
            """
            yearly_results = conn.execute(yearly_sql).fetchall()

            # Calculate YoY growth between consecutive years
            yoy_rates = []
            for i in range(1, len(yearly_results)):
                prev_year = yearly_results[i-1][1]
                curr_year = yearly_results[i][1]
                if prev_year > 0:
                    yoy_rate = (curr_year - prev_year) / prev_year
                    yoy_rates.append(yoy_rate)

            overall_yoy = sum(yoy_rates) / len(yoy_rates) if yoy_rates else 0.10

            # Calculate monthly YoY for each month
            monthly_yoy_sql = f"""
                WITH monthly_by_year AS (
                    SELECT
                        EXTRACT(YEAR FROM {_date_in_kyiv('o.ordered_at')}) as year,
                        EXTRACT(MONTH FROM {_date_in_kyiv('o.ordered_at')}) as month,
                        SUM(o.grand_total) as revenue
                    FROM orders o
                    WHERE o.status_id NOT IN {return_statuses}
                        AND {sales_filter}
                    GROUP BY
                        EXTRACT(YEAR FROM {_date_in_kyiv('o.ordered_at')}),
                        EXTRACT(MONTH FROM {_date_in_kyiv('o.ordered_at')})
                )
                SELECT
                    curr.month,
                    AVG((curr.revenue - prev.revenue) / NULLIF(prev.revenue, 0)) as avg_yoy_growth
                FROM monthly_by_year curr
                JOIN monthly_by_year prev
                    ON curr.month = prev.month
                    AND curr.year = prev.year + 1
                GROUP BY curr.month
                ORDER BY curr.month
            """
            monthly_yoy_results = conn.execute(monthly_yoy_sql).fetchall()

            monthly_yoy = {
                int(row[0]): round(float(row[1] or 0), 4)
                for row in monthly_yoy_results
            }

            # Store metrics
            min_date = conn.execute(f"SELECT MIN({_date_in_kyiv('ordered_at')}) FROM orders").fetchone()[0]
            max_date = conn.execute(f"SELECT MAX({_date_in_kyiv('ordered_at')}) FROM orders").fetchone()[0]
            now = datetime.now(DEFAULT_TZ)

            conn.execute("""
                INSERT INTO growth_metrics (metric_type, value, period_start, period_end, sample_size, updated_at)
                VALUES ('yoy_overall', ?, ?, ?, ?, ?)
                ON CONFLICT (metric_type) DO UPDATE SET
                    value = excluded.value,
                    period_start = excluded.period_start,
                    period_end = excluded.period_end,
                    sample_size = excluded.sample_size,
                    updated_at = excluded.updated_at
            """, [overall_yoy, min_date, max_date, len(yoy_rates), now])

            # Update seasonal_indices with monthly YoY
            for month, yoy in monthly_yoy.items():
                conn.execute("""
                    UPDATE seasonal_indices
                    SET yoy_growth = ?, updated_at = ?
                    WHERE month = ?
                """, [yoy, now, month])

            logger.info(f"Calculated YoY growth: {overall_yoy:.2%}")
            return {
                "overall_yoy": round(overall_yoy, 4),
                "monthly_yoy": monthly_yoy,
                "yearly_data": [
                    {"year": int(row[0]), "revenue": round(float(row[1]), 2)}
                    for row in yearly_results
                ],
                "sample_size": len(yoy_rates)
            }

    async def calculate_weekly_patterns(self, sales_type: str = "retail") -> Dict[int, Dict[int, float]]:
        """
        Calculate how revenue distributes across weeks within each month.

        Args:
            sales_type: 'retail', 'b2b', or 'all'

        Returns:
            Dictionary mapping month -> week_of_month -> weight (percentage)
        """
        async with self.connection() as conn:
            return_statuses = tuple(int(s) for s in OrderStatus.return_statuses())
            sales_filter = self._build_sales_type_filter(sales_type)

            # Calculate weekly revenue within each month instance
            sql = f"""
                WITH weekly_data AS (
                    SELECT
                        EXTRACT(YEAR FROM {_date_in_kyiv('o.ordered_at')}) as year,
                        EXTRACT(MONTH FROM {_date_in_kyiv('o.ordered_at')}) as month,
                        -- Week of month: 1-5 based on day of month
                        LEAST(5, CEIL(EXTRACT(DAY FROM {_date_in_kyiv('o.ordered_at')}) / 7.0)::int) as week_of_month,
                        SUM(o.grand_total) as revenue
                    FROM orders o
                    WHERE o.status_id NOT IN {return_statuses}
                        AND {sales_filter}
                    GROUP BY
                        EXTRACT(YEAR FROM {_date_in_kyiv('o.ordered_at')}),
                        EXTRACT(MONTH FROM {_date_in_kyiv('o.ordered_at')}),
                        LEAST(5, CEIL(EXTRACT(DAY FROM {_date_in_kyiv('o.ordered_at')}) / 7.0)::int)
                ),
                monthly_totals AS (
                    SELECT year, month, SUM(revenue) as month_total
                    FROM weekly_data
                    GROUP BY year, month
                ),
                weekly_weights AS (
                    SELECT
                        wd.month,
                        wd.week_of_month,
                        AVG(wd.revenue / NULLIF(mt.month_total, 0)) as avg_weight,
                        COUNT(*) as sample_size
                    FROM weekly_data wd
                    JOIN monthly_totals mt ON wd.year = mt.year AND wd.month = mt.month
                    GROUP BY wd.month, wd.week_of_month
                )
                SELECT month, week_of_month, avg_weight, sample_size
                FROM weekly_weights
                ORDER BY month, week_of_month
            """

            results = conn.execute(sql).fetchall()

            patterns = {}
            now = datetime.now(DEFAULT_TZ)
            pattern_rows = []
            for row in results:
                month = int(row[0])
                week = int(row[1])
                weight = float(row[2] or 0.25)  # Default to 25% if no data
                sample_size = int(row[3])

                if month not in patterns:
                    patterns[month] = {}
                patterns[month][week] = round(weight, 4)
                pattern_rows.append([month, week, weight, sample_size, now])

            # Batch upsert to avoid row-by-row lock hold
            if pattern_rows:
                conn.executemany("""
                    INSERT INTO weekly_patterns (month, week_of_month, weight, sample_size, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT (month, week_of_month) DO UPDATE SET
                        weight = excluded.weight,
                        sample_size = excluded.sample_size,
                        updated_at = excluded.updated_at
                """, pattern_rows)

            # Ensure all months have all 5 weeks (fill missing with equal distribution)
            for month in range(1, 13):
                if month not in patterns:
                    patterns[month] = {}
                for week in range(1, 6):
                    if week not in patterns[month]:
                        # Default: slightly more revenue in weeks 1-4, less in week 5
                        default_weights = {1: 0.23, 2: 0.23, 3: 0.23, 4: 0.23, 5: 0.08}
                        patterns[month][week] = default_weights[week]

            logger.info(f"Calculated weekly patterns for {len(patterns)} months")
            return patterns

    async def generate_smart_goals(
        self,
        target_year: int,
        target_month: int,
        sales_type: str = "retail",
        recalculate: bool = False
    ) -> Dict[str, Any]:
        """
        Generate smart goals for a target month using seasonality and growth.

        Algorithm:
        1. Get last year's same month revenue as baseline
        2. Apply YoY growth rate
        3. Adjust using seasonality index
        4. Calculate weekly breakdown using weekly patterns

        Args:
            target_year: Year to generate goals for
            target_month: Month (1-12) to generate goals for
            sales_type: 'retail', 'b2b', or 'all'
            recalculate: Force recalculation of indices

        Returns:
            Smart goals with monthly total and weekly breakdown
        """
        async with self.connection() as conn:
            # Recalculate indices if needed or not cached
            indices_exist = conn.execute(
                "SELECT COUNT(*) FROM seasonal_indices"
            ).fetchone()[0]

            if recalculate or indices_exist < 12:
                await self.calculate_seasonality_indices(sales_type)
                await self.calculate_yoy_growth(sales_type)
                await self.calculate_weekly_patterns(sales_type)

            # Cap growth rate to reasonable maximum (35%)
            # Used as default when no historical data available
            MAX_GROWTH_RATE = 0.35

            # Get seasonality index for target month
            seasonality_result = conn.execute("""
                SELECT seasonality_index, avg_revenue, yoy_growth, confidence
                FROM seasonal_indices
                WHERE month = ?
            """, [target_month]).fetchone()

            if seasonality_result:
                seasonality_index = float(seasonality_result[0] or 1.0)
                historical_avg = float(seasonality_result[1] or 0)
                monthly_yoy = float(seasonality_result[2] or MAX_GROWTH_RATE)
                confidence = seasonality_result[3] or "low"
            else:
                seasonality_index = 1.0
                historical_avg = 0
                monthly_yoy = MAX_GROWTH_RATE
                confidence = "low"

            # Get overall YoY growth
            yoy_result = conn.execute("""
                SELECT value FROM growth_metrics WHERE metric_type = 'yoy_overall'
            """).fetchone()
            overall_yoy = float(yoy_result[0] or MAX_GROWTH_RATE) if yoy_result else MAX_GROWTH_RATE

            # Get last year's same month revenue
            return_statuses = tuple(int(s) for s in OrderStatus.return_statuses())
            sales_filter = self._build_sales_type_filter(sales_type)

            last_year_sql = f"""
                SELECT SUM(o.grand_total) as revenue
                FROM orders o
                WHERE EXTRACT(YEAR FROM {_date_in_kyiv('o.ordered_at')}) = ?
                    AND EXTRACT(MONTH FROM {_date_in_kyiv('o.ordered_at')}) = ?
                    AND o.status_id NOT IN {return_statuses}
                    AND {sales_filter}
            """
            last_year_result = conn.execute(last_year_sql, [target_year - 1, target_month]).fetchone()
            last_year_revenue = float(last_year_result[0] or 0) if last_year_result[0] else 0

            # Get recent 3-month average (last 3 complete months with at least 25 days of data)
            recent_avg_sql = f"""
                WITH monthly_revenue AS (
                    SELECT
                        EXTRACT(YEAR FROM {_date_in_kyiv('o.ordered_at')}) as year,
                        EXTRACT(MONTH FROM {_date_in_kyiv('o.ordered_at')}) as month,
                        SUM(o.grand_total) as revenue,
                        COUNT(DISTINCT DATE({_date_in_kyiv('o.ordered_at')})) as days_with_orders
                    FROM orders o
                    WHERE o.status_id NOT IN {return_statuses}
                        AND {sales_filter}
                        AND {_date_in_kyiv('o.ordered_at')} < DATE_TRUNC('month', CURRENT_DATE)
                    GROUP BY
                        EXTRACT(YEAR FROM {_date_in_kyiv('o.ordered_at')}),
                        EXTRACT(MONTH FROM {_date_in_kyiv('o.ordered_at')})
                    HAVING COUNT(DISTINCT DATE({_date_in_kyiv('o.ordered_at')})) >= 25
                    ORDER BY year DESC, month DESC
                    LIMIT 3
                )
                SELECT AVG(revenue) as avg_revenue FROM monthly_revenue
            """
            recent_avg_result = conn.execute(recent_avg_sql).fetchone()
            recent_3_month_avg = float(recent_avg_result[0] or 0) if recent_avg_result[0] else 0

            # Calculate goals using both methods
            yoy_goal = 0
            recent_goal = 0

            # Apply growth rate cap
            raw_growth_rate = monthly_yoy if monthly_yoy > 0 else overall_yoy
            growth_rate = min(raw_growth_rate, MAX_GROWTH_RATE)

            # Method 1: YoY growth (last year same month × growth)
            if last_year_revenue > 0:
                yoy_goal = last_year_revenue * (1 + growth_rate)

            # Method 2: Recent baseline adjusted for seasonality
            # recent_3_month_avg × seasonality_index
            # seasonality_index < 1 means this month is typically below average
            # seasonality_index > 1 means this month is typically above average
            if recent_3_month_avg > 0 and seasonality_index > 0:
                recent_goal = recent_3_month_avg * seasonality_index

            # Take the MAX of both methods (never set goal below recent performance)
            if yoy_goal > 0 and recent_goal > 0:
                monthly_goal = max(yoy_goal, recent_goal)
                calculation_method = "yoy_growth" if yoy_goal >= recent_goal else "recent_trend"
            elif recent_goal > 0:
                monthly_goal = recent_goal
                calculation_method = "recent_trend"
            elif yoy_goal > 0:
                monthly_goal = yoy_goal
                calculation_method = "yoy_growth"
            elif historical_avg > 0:
                monthly_goal = historical_avg * (1 + growth_rate)
                calculation_method = "historical_avg"
            else:
                monthly_goal = 3000000  # 3M UAH default
                growth_rate = MAX_GROWTH_RATE
                calculation_method = "fallback"

            # Round to nice number
            monthly_goal = round(monthly_goal / 100000) * 100000

            # Get weekly patterns for this month
            weekly_patterns = conn.execute("""
                SELECT week_of_month, weight
                FROM weekly_patterns
                WHERE month = ?
                ORDER BY week_of_month
            """, [target_month]).fetchall()

            if weekly_patterns:
                weekly_weights = {int(row[0]): float(row[1]) for row in weekly_patterns}
            else:
                # Default distribution
                weekly_weights = {1: 0.23, 2: 0.23, 3: 0.23, 4: 0.23, 5: 0.08}

            # Normalize weights to sum to 1
            total_weight = sum(weekly_weights.values())
            if total_weight > 0:
                weekly_weights = {k: v / total_weight for k, v in weekly_weights.items()}

            # Calculate weekly goals
            weekly_goals = {
                week: round(monthly_goal * weight / 10000) * 10000
                for week, weight in weekly_weights.items()
            }

            # Calculate daily goal (monthly / ~22 working days or 30 days)
            days_in_month = 30 if target_month in [4, 6, 9, 11] else 31 if target_month != 2 else 28
            daily_goal = round(monthly_goal / days_in_month / 10000) * 10000

            # Calculate weekly goal (monthly / 4.3 weeks on average)
            weekly_goal = round(monthly_goal / 4.3 / 50000) * 50000

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
                    "growthRate": round(growth_rate, 4),
                    "seasonalityIndex": seasonality_index,
                    "confidence": confidence,
                    "calculationMethod": calculation_method
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

    async def get_smart_goals(self, sales_type: str = "retail") -> Dict[str, Dict[str, Any]]:
        """
        Get smart goals for the current period using seasonality.

        This is an enhanced version of get_goals() that uses the smart
        goal generation system with seasonality and growth factors.

        Args:
            sales_type: 'retail', 'b2b', or 'all'

        Returns:
            Goals for daily, weekly, and monthly periods with calculation details
        """
        now = datetime.now(DEFAULT_TZ)
        current_year = now.year
        current_month = now.month

        # Generate smart goals for current month
        smart = await self.generate_smart_goals(current_year, current_month, sales_type)

        # Check for custom overrides
        async with self.connection() as conn:
            stored_goals = conn.execute("""
                SELECT period_type, goal_amount, is_custom
                FROM revenue_goals
                WHERE is_custom = TRUE
            """).fetchall()
            custom_goals = {row[0]: float(row[1]) for row in stored_goals if row[2]}

        return {
            "daily": {
                "amount": custom_goals.get("daily", smart["daily"]["goal"]),
                "isCustom": "daily" in custom_goals,
                "suggestedAmount": smart["daily"]["goal"],
                "basedOnAverage": smart["monthly"]["historicalAvg"] / 30,
                "trend": smart["metadata"]["monthlyYoY"] * 100,
                "confidence": smart["monthly"]["confidence"]
            },
            "weekly": {
                "amount": custom_goals.get("weekly", smart["weekly"]["goal"]),
                "isCustom": "weekly" in custom_goals,
                "suggestedAmount": smart["weekly"]["goal"],
                "basedOnAverage": smart["monthly"]["historicalAvg"] / 4.3,
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
                "growthRate": smart["monthly"]["growthRate"],
                "seasonalityIndex": smart["monthly"]["seasonalityIndex"],
                "trend": smart["metadata"]["monthlyYoY"] * 100,
                "confidence": smart["monthly"]["confidence"],
                "calculationMethod": smart["monthly"]["calculationMethod"]
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
        """Store revenue predictions in DuckDB.

        Args:
            predictions: List of dicts with 'date' and 'predicted_revenue' keys.
            sales_type: 'retail' or 'b2b'.
            metrics: Model metrics dict with 'mae' and 'mape'.

        Returns:
            Number of predictions stored.
        """
        if not predictions:
            return 0

        mae = metrics.get('mae', 0) if metrics else 0
        mape = metrics.get('mape', 0) if metrics else 0

        async with self.connection() as conn:
            # Delete existing predictions for this sales_type in the date range
            dates = [p['date'] for p in predictions]
            min_date = min(dates)
            max_date = max(dates)

            conn.execute(
                """DELETE FROM revenue_predictions
                   WHERE sales_type = ? AND prediction_date >= ? AND prediction_date <= ?""",
                [sales_type, min_date, max_date]
            )

            # Batch insert new predictions
            conn.executemany(
                """INSERT INTO revenue_predictions
                   (prediction_date, sales_type, predicted_revenue, model_mae, model_mape)
                   VALUES (?, ?, ?, ?, ?)""",
                [[pred['date'], sales_type, pred['predicted_revenue'], mae, mape]
                 for pred in predictions]
            )

            logger.info(f"Stored {len(predictions)} predictions for {sales_type}")
            return len(predictions)

    async def get_predictions(
        self,
        start_date: date,
        end_date: date,
        sales_type: str = "retail",
    ) -> List[Dict]:
        """Get stored revenue predictions for a date range.

        Returns:
            List of dicts with date, predicted_revenue, model_mae, model_mape.
        """
        async with self.connection() as conn:
            rows = conn.execute(
                """SELECT prediction_date, predicted_revenue, model_mae, model_mape
                   FROM revenue_predictions
                   WHERE sales_type = ?
                     AND prediction_date >= ?
                     AND prediction_date <= ?
                   ORDER BY prediction_date""",
                [sales_type, start_date.isoformat(), end_date.isoformat()]
            ).fetchall()

        return [
            {
                'date': row[0].isoformat() if hasattr(row[0], 'isoformat') else str(row[0]),
                'predicted_revenue': float(row[1]),
                'model_mae': float(row[2]) if row[2] else 0,
                'model_mape': float(row[3]) if row[3] else 0,
            }
            for row in rows
        ]

    async def get_daily_revenue_for_dates(
        self,
        dates: list,
        sales_type: str = "retail",
    ) -> Dict[date, float]:
        """Get daily revenue for a list of specific dates.

        Returns dict mapping date -> revenue total.
        Used for extending comparison data to cover forecast dates.
        """
        if not dates:
            return {}

        return_statuses = tuple(int(s) for s in OrderStatus.return_statuses())
        placeholders = ", ".join(["?"] * len(dates))
        date_strs = [d.isoformat() for d in dates]

        async with self.connection() as conn:
            sales_filter = self._build_sales_type_filter(sales_type)
            rows = conn.execute(
                f"""SELECT {_date_in_kyiv('o.ordered_at')} as day,
                           SUM(o.grand_total) as revenue
                    FROM orders o
                    WHERE {_date_in_kyiv('o.ordered_at')} IN ({placeholders})
                      AND o.status_id NOT IN {return_statuses}
                      AND o.source_id IN (1, 2, 4)
                      AND {sales_filter}
                    GROUP BY day""",
                date_strs,
            ).fetchall()

        return {row[0]: float(row[1]) for row in rows}

    # ═══════════════════════════════════════════════════════════════════════════
    # MANUAL EXPENSES CRUD
    # ═══════════════════════════════════════════════════════════════════════════
