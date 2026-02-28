"""DuckDBStore customer insights methods."""
from __future__ import annotations

from datetime import date, timedelta
from typing import Optional, Dict, Any

from core.duckdb_constants import B2B_MANAGER_ID, RETAIL_MANAGER_IDS


class CustomersMixin:

    async def get_customer_insights(
        self,
        start_date: date,
        end_date: date,
        source_id: Optional[int] = None,
        brand: Optional[str] = None,
        sales_type: str = "retail"
    ) -> Dict[str, Any]:
        """Get customer insights: new vs returning, AOV trend (from Gold/Silver layers)."""
        async with self.connection() as conn:
            # ── Base metrics from gold_daily_revenue ──
            params = [start_date, end_date]
            where_clauses = ["date BETWEEN ? AND ?"]

            if sales_type != "all":
                where_clauses.append("sales_type = ?")
                params.append(sales_type)

            where_sql = " AND ".join(where_clauses)

            gold_result = conn.execute(f"""
                SELECT
                    SUM(unique_customers) as total_customers,
                    SUM(orders_count) as total_orders,
                    SUM(revenue) as total_revenue,
                    SUM(new_customers) as new_customers,
                    SUM(returning_customers) as returning_customers
                FROM gold_daily_revenue
                WHERE {where_sql}
            """, params).fetchone()

            total_customers = int(gold_result[0] or 0)
            total_orders = int(gold_result[1] or 0)
            total_revenue = float(gold_result[2] or 0)
            new_customers = int(gold_result[3] or 0)
            returning_customers = int(gold_result[4] or 0)

            # AOV trend from gold_daily_revenue
            aov_results = conn.execute(f"""
                SELECT date,
                       CASE WHEN orders_count > 0 THEN revenue / orders_count ELSE 0 END as aov
                FROM gold_daily_revenue
                WHERE {where_sql}
                ORDER BY date
            """, params).fetchall()
            aov_by_day = {row[0]: float(row[1]) for row in aov_results}

            labels = []
            aov_data = []
            current = start_date
            while current <= end_date:
                labels.append(current.strftime("%d.%m"))
                aov_data.append(round(aov_by_day.get(current, 0), 2))
                current += timedelta(days=1)

            overall_aov = total_revenue / total_orders if total_orders > 0 else 0

            # ── CLV metrics from silver_orders (need per-buyer aggregation) ──
            sales_where = "s.sales_type = ?" if sales_type != "all" else "1=1"
            clv_params = [sales_type] if sales_type != "all" else []

            clv_result = conn.execute(f"""
                WITH customer_stats AS (
                    SELECT
                        s.buyer_id,
                        COUNT(DISTINCT s.id) as order_count,
                        SUM(s.grand_total) as total_spent,
                        DATE_DIFF('day', MIN(s.ordered_at), MAX(s.ordered_at)) as lifespan_days
                    FROM silver_orders s
                    WHERE s.buyer_id IS NOT NULL
                      AND NOT s.is_return
                      AND s.is_active_source
                      AND {sales_where}
                    GROUP BY s.buyer_id
                    HAVING COUNT(DISTINCT s.id) > 1
                )
                SELECT
                    COUNT(*) as repeat_customer_count,
                    AVG(order_count) as avg_purchase_frequency,
                    AVG(lifespan_days) as avg_lifespan_days,
                    AVG(total_spent) as avg_customer_value
                FROM customer_stats
            """, clv_params).fetchone()

            repeat_customer_count = clv_result[0] or 0
            avg_purchase_frequency = float(clv_result[1] or 0)
            avg_lifespan_days = float(clv_result[2] or 0)
            avg_customer_value = float(clv_result[3] or 0)
            clv = avg_customer_value if repeat_customer_count > 0 else 0

            # Compute accurate unique customer counts from Silver
            # (Gold sums daily unique counts, double-counting multi-day buyers)
            pf_where = ["s.order_date BETWEEN ? AND ?", "NOT s.is_return", "s.is_active_source",
                        "s.buyer_id IS NOT NULL"]
            pf_params: list = [start_date, end_date]
            if sales_type != "all":
                pf_where.append("s.sales_type = ?")
                pf_params.append(sales_type)
            pf_result = conn.execute(f"""
                SELECT
                    COUNT(DISTINCT s.buyer_id),
                    COUNT(DISTINCT CASE WHEN s.is_new_customer THEN s.buyer_id END),
                    COUNT(DISTINCT CASE WHEN NOT s.is_new_customer THEN s.buyer_id END)
                FROM silver_orders s
                WHERE {" AND ".join(pf_where)}
            """, pf_params).fetchone()
            unique_buyers = int(pf_result[0] or 0)
            new_customers = int(pf_result[1] or 0)
            returning_customers = int(pf_result[2] or 0)
            total_customers = unique_buyers
            purchase_frequency = total_orders / unique_buyers if unique_buyers > 0 else 0

            # All-time repeat rate from silver_orders
            alltime_result = conn.execute(f"""
                WITH customer_orders AS (
                    SELECT
                        s.buyer_id,
                        COUNT(DISTINCT s.id) as order_count
                    FROM silver_orders s
                    WHERE s.buyer_id IS NOT NULL
                      AND NOT s.is_return
                      AND s.is_active_source
                      AND {sales_where}
                    GROUP BY s.buyer_id
                )
                SELECT
                    COUNT(*) as total_customers,
                    SUM(CASE WHEN order_count >= 2 THEN 1 ELSE 0 END) as repeat_customers,
                    AVG(order_count) as avg_orders_per_customer
                FROM customer_orders
            """, clv_params).fetchone()

            alltime_total_customers = alltime_result[0] or 0
            alltime_repeat_customers = alltime_result[1] or 0
            alltime_avg_orders = float(alltime_result[2] or 0)
            true_repeat_rate = (alltime_repeat_customers / alltime_total_customers * 100) if alltime_total_customers > 0 else 0

            return {
                "newVsReturning": {
                    "labels": ["New Customers", "Returning Customers"],
                    "data": [new_customers, returning_customers],
                    "backgroundColor": ["#2563EB", "#16A34A"]
                },
                "aovTrend": {
                    "labels": labels,
                    "datasets": [{
                        "label": "AOV (UAH)",
                        "data": aov_data,
                        "borderColor": "#F59E0B",
                        "backgroundColor": "rgba(245, 158, 11, 0.1)",
                        "fill": True,
                        "tension": 0.3
                    }]
                },
                "metrics": {
                    "totalCustomers": total_customers,
                    "newCustomers": new_customers,
                    "returningCustomers": returning_customers,
                    "totalOrders": total_orders,
                    "repeatRate": round((returning_customers / total_customers * 100) if total_customers > 0 else 0, 1),
                    "averageOrderValue": round(overall_aov, 2),
                    "customerLifetimeValue": round(clv, 2),
                    "avgPurchaseFrequency": round(avg_purchase_frequency, 2),
                    "avgCustomerLifespanDays": round(avg_lifespan_days, 0),
                    "purchaseFrequency": round(purchase_frequency, 2),
                    "totalCustomersAllTime": alltime_total_customers,
                    "repeatCustomersAllTime": alltime_repeat_customers,
                    "trueRepeatRate": round(true_repeat_rate, 1),
                    "avgOrdersPerCustomer": round(alltime_avg_orders, 2)
                }
            }

    async def get_cohort_retention(
        self,
        months_back: int = 12,
        retention_months: int = 6,
        sales_type: str = "retail"
    ) -> Dict[str, Any]:
        """
        Get cohort retention analysis.

        Shows what percentage of customers from each cohort (first purchase month)
        returned to make purchases in subsequent months.

        Args:
            months_back: How many months of cohorts to analyze
            retention_months: How many months of retention to track (M0 to Mn)
            sales_type: Filter by sales type (retail/b2b/all)

        Returns:
            Dict with cohorts, retention matrix, and summary metrics
        """
        async with self.connection() as conn:
            # Build sales type filter
            sales_type_filter = ""
            if sales_type == "retail":
                sales_type_filter = f"""
                    AND (o.manager_id IN ({','.join(map(str, RETAIL_MANAGER_IDS))})
                         OR (o.manager_id IS NULL AND o.source_id = 4))
                """
            elif sales_type == "b2b":
                sales_type_filter = f"AND o.manager_id = {B2B_MANAGER_ID}"

            query = f"""
            WITH customer_cohorts AS (
                -- Get each customer's first order month (their cohort)
                SELECT
                    o.buyer_id,
                    DATE_TRUNC('month', MIN(o.order_date)) AS cohort_month
                FROM silver_orders o
                WHERE o.buyer_id IS NOT NULL
                  AND NOT o.is_return
                  {sales_type_filter}
                GROUP BY o.buyer_id
            ),
            customer_orders AS (
                -- Get all order months per customer
                SELECT DISTINCT
                    o.buyer_id,
                    c.cohort_month,
                    DATEDIFF('month', c.cohort_month, DATE_TRUNC('month', o.order_date)) AS months_since
                FROM silver_orders o
                JOIN customer_cohorts c ON o.buyer_id = c.buyer_id
                WHERE NOT o.is_return
                  {sales_type_filter}
            ),
            cohort_sizes AS (
                SELECT cohort_month, COUNT(DISTINCT buyer_id) AS size
                FROM customer_cohorts
                GROUP BY cohort_month
            ),
            retention_data AS (
                SELECT
                    r.cohort_month,
                    r.months_since,
                    COUNT(DISTINCT r.buyer_id) AS retained_customers
                FROM customer_orders r
                WHERE r.months_since <= ?
                GROUP BY r.cohort_month, r.months_since
            )
            SELECT
                strftime(r.cohort_month, '%Y-%m') as cohort,
                s.size as cohort_size,
                r.months_since as month_number,
                r.retained_customers,
                ROUND(100.0 * r.retained_customers / s.size, 1) as retention_pct
            FROM retention_data r
            JOIN cohort_sizes s ON r.cohort_month = s.cohort_month
            WHERE r.cohort_month >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '{int(months_back)} months'
            ORDER BY r.cohort_month DESC, r.months_since
            """

            rows = conn.execute(query, [retention_months]).fetchall()

            # Build cohort data structure
            cohorts = {}
            for cohort, size, month_num, retained, pct in rows:
                if cohort not in cohorts:
                    cohorts[cohort] = {
                        "size": size,
                        "retention": {}
                    }
                cohorts[cohort]["retention"][month_num] = {
                    "count": retained,
                    "percent": pct
                }

            # Calculate summary metrics
            total_cohort_size = sum(c["size"] for c in cohorts.values())

            # Weighted average retention by month (weight = cohort size)
            avg_retention = {}
            for m in range(retention_months + 1):
                weighted_sum = 0
                total_weight = 0
                for c in cohorts.values():
                    entry = c["retention"].get(m)
                    if entry is not None:
                        pct = entry.get("percent", 0)
                        weighted_sum += pct * c["size"]
                        total_weight += c["size"]
                if total_weight > 0:
                    avg_retention[m] = round(weighted_sum / total_weight, 1)

            return {
                "cohorts": [
                    {
                        "month": cohort,
                        "size": data["size"],
                        "retention": [
                            data["retention"].get(m, {}).get("percent", None)
                            for m in range(retention_months + 1)
                        ]
                    }
                    for cohort, data in sorted(cohorts.items(), reverse=True)
                ],
                "retentionMonths": retention_months,
                "summary": {
                    "totalCohorts": len(cohorts),
                    "totalCustomers": total_cohort_size,
                    "avgRetention": avg_retention
                }
            }

    async def get_enhanced_cohort_retention(
        self,
        months_back: int = 12,
        retention_months: int = 6,
        sales_type: str = "retail",
        include_revenue: bool = True
    ) -> Dict[str, Any]:
        """
        Get enhanced cohort retention analysis with revenue tracking.

        Shows customer retention percentages AND revenue retention for each cohort.

        Args:
            months_back: How many months of cohorts to analyze
            retention_months: How many months of retention to track (M0 to Mn)
            sales_type: Filter by sales type (retail/b2b/all)
            include_revenue: Include revenue retention metrics

        Returns:
            Dict with cohorts, customer retention, revenue retention, and summary
        """
        async with self.connection() as conn:
            # Build sales type filter
            sales_type_filter = ""
            if sales_type == "retail":
                sales_type_filter = f"""
                    AND (o.manager_id IN ({','.join(map(str, RETAIL_MANAGER_IDS))})
                         OR (o.manager_id IS NULL AND o.source_id = 4))
                """
            elif sales_type == "b2b":
                sales_type_filter = f"AND o.manager_id = {B2B_MANAGER_ID}"

            query = f"""
            WITH customer_first_order AS (
                -- Get each customer's first order month (cohort)
                SELECT
                    o.buyer_id,
                    DATE_TRUNC('month', MIN(o.order_date)) AS cohort_month
                FROM silver_orders o
                WHERE o.buyer_id IS NOT NULL
                  AND NOT o.is_return
                  {sales_type_filter}
                GROUP BY o.buyer_id
            ),
            customer_cohorts AS (
                -- Add first month revenue per customer
                SELECT
                    c.buyer_id,
                    c.cohort_month,
                    COALESCE(SUM(o.grand_total), 0) AS first_month_revenue
                FROM customer_first_order c
                LEFT JOIN silver_orders o ON c.buyer_id = o.buyer_id
                    AND DATE_TRUNC('month', o.order_date) = c.cohort_month
                    AND NOT o.is_return
                GROUP BY c.buyer_id, c.cohort_month
            ),
            customer_orders AS (
                -- Get all order months per customer with revenue
                SELECT
                    o.buyer_id,
                    c.cohort_month,
                    DATEDIFF('month', c.cohort_month, DATE_TRUNC('month', o.order_date)) AS months_since,
                    o.grand_total AS revenue
                FROM silver_orders o
                JOIN customer_cohorts c ON o.buyer_id = c.buyer_id
                WHERE NOT o.is_return
                  {sales_type_filter}
            ),
            cohort_sizes AS (
                SELECT
                    cohort_month,
                    COUNT(DISTINCT buyer_id) AS size,
                    SUM(first_month_revenue) AS m0_revenue
                FROM customer_cohorts
                GROUP BY cohort_month
            ),
            retention_data AS (
                SELECT
                    r.cohort_month,
                    r.months_since,
                    COUNT(DISTINCT r.buyer_id) AS retained_customers,
                    SUM(r.revenue) AS period_revenue
                FROM customer_orders r
                WHERE r.months_since <= ?
                GROUP BY r.cohort_month, r.months_since
            )
            SELECT
                strftime(r.cohort_month, '%Y-%m') as cohort,
                s.size as cohort_size,
                s.m0_revenue,
                r.months_since as month_number,
                r.retained_customers,
                ROUND(100.0 * r.retained_customers / s.size, 1) as retention_pct,
                r.period_revenue,
                ROUND(100.0 * r.period_revenue / NULLIF(s.m0_revenue, 0), 1) as revenue_retention_pct
            FROM retention_data r
            JOIN cohort_sizes s ON r.cohort_month = s.cohort_month
            WHERE r.cohort_month >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '{int(months_back)} months'
            ORDER BY r.cohort_month DESC, r.months_since
            """

            rows = conn.execute(query, [retention_months]).fetchall()

            # Build cohort data structure
            cohorts = {}
            for cohort, size, m0_rev, month_num, retained, pct, rev, rev_pct in rows:
                if cohort not in cohorts:
                    cohorts[cohort] = {
                        "size": size,
                        "m0_revenue": float(m0_rev or 0),
                        "retention": {},
                        "revenue_retention": {},
                        "revenue": {}
                    }
                cohorts[cohort]["retention"][month_num] = float(pct) if pct is not None else None
                cohorts[cohort]["revenue_retention"][month_num] = float(rev_pct) if rev_pct is not None else None
                cohorts[cohort]["revenue"][month_num] = float(rev or 0)

            # Calculate summary metrics
            total_cohort_size = sum(c["size"] for c in cohorts.values())
            total_revenue = sum(c["m0_revenue"] for c in cohorts.values())

            # Weighted average retention by month (weight = cohort size)
            avg_customer_retention = {}
            avg_revenue_retention = {}
            for m in range(retention_months + 1):
                cust_weighted_sum = 0
                cust_total_weight = 0
                rev_weighted_sum = 0
                rev_total_weight = 0
                for c in cohorts.values():
                    cust_pct = c["retention"].get(m)
                    if cust_pct is not None:
                        cust_weighted_sum += cust_pct * c["size"]
                        cust_total_weight += c["size"]
                    rev_pct = c["revenue_retention"].get(m)
                    if rev_pct is not None:
                        rev_weighted_sum += rev_pct * c["size"]
                        rev_total_weight += c["size"]
                if cust_total_weight > 0:
                    avg_customer_retention[m] = round(cust_weighted_sum / cust_total_weight, 1)
                if rev_total_weight > 0:
                    avg_revenue_retention[m] = round(rev_weighted_sum / rev_total_weight, 1)

            # ── Compute insights ──────────────────────────────────────
            sorted_cohort_list = [
                {"month": k, **v}
                for k, v in sorted(cohorts.items())
            ]

            insights = self._compute_cohort_insights(
                sorted_cohort_list, avg_customer_retention, retention_months
            )

            return {
                "cohorts": [
                    {
                        "month": cohort,
                        "size": data["size"],
                        "retention": [
                            data["retention"].get(m)
                            for m in range(retention_months + 1)
                        ],
                        "revenueRetention": [
                            data["revenue_retention"].get(m)
                            for m in range(retention_months + 1)
                        ] if include_revenue else None,
                        "revenue": [
                            round(data["revenue"].get(m, 0), 2)
                            for m in range(retention_months + 1)
                        ] if include_revenue else None
                    }
                    for cohort, data in sorted(cohorts.items(), reverse=True)
                ],
                "retentionMonths": retention_months,
                "summary": {
                    "totalCohorts": len(cohorts),
                    "totalCustomers": total_cohort_size,
                    "avgCustomerRetention": avg_customer_retention,
                    "avgRevenueRetention": avg_revenue_retention if include_revenue else None,
                    "totalRevenue": round(total_revenue, 2) if include_revenue else None
                },
                "insights": insights
            }

    @staticmethod
    def _compute_cohort_insights(
        sorted_cohorts: list,
        avg_customer_retention: dict,
        retention_months: int
    ) -> dict:
        """Compute derived insights from cohort retention data."""
        if not sorted_cohorts:
            return {
                "retentionTrend": None,
                "cohortQualityTrend": None,
                "revenueImpact": None,
                "decayAnalysis": None,
            }

        # Helper: weighted average M1 for a subset of cohorts
        def weighted_m1(cohort_list):
            total_w, total_s = 0.0, 0
            for c in cohort_list:
                m1 = c["retention"].get(1)
                if m1 is not None and c["size"] > 0:
                    total_w += float(m1) * c["size"]
                    total_s += c["size"]
            return round(total_w / total_s, 1) if total_s > 0 else None

        # ── 5A.2 Retention trend ──
        cohorts_with_m1 = [c for c in sorted_cohorts if c["retention"].get(1) is not None]
        retention_trend = None
        if len(cohorts_with_m1) >= 4:
            recent = cohorts_with_m1[-3:]
            older = cohorts_with_m1[:3]
            recent_avg = weighted_m1(recent)
            older_avg = weighted_m1(older)
            if recent_avg is not None and older_avg is not None:
                delta = round(recent_avg - older_avg, 1)
                if abs(delta) < 1:
                    direction = "stable"
                elif delta > 0:
                    direction = "improving"
                else:
                    direction = "declining"
                retention_trend = {
                    "recentM1": recent_avg,
                    "olderM1": older_avg,
                    "delta": delta,
                    "direction": direction,
                }

        # ── 5A.3 Cohort quality trend ──
        quality_scores = []
        for c in sorted_cohorts:
            m1 = c["retention"].get(1)
            if m1 is None:
                continue
            m1 = float(m1)
            m3 = c["retention"].get(3)
            score = round(0.6 * m1 + 0.4 * float(m3), 1) if m3 is not None else round(m1, 1)
            quality_scores.append({"month": c["month"], "score": score})

        cohort_quality = None
        if quality_scores:
            best = max(quality_scores, key=lambda x: x["score"])
            worst = min(quality_scores, key=lambda x: x["score"])
            latest = quality_scores[-1]
            avg_score = round(sum(q["score"] for q in quality_scores) / len(quality_scores), 1)
            cohort_quality = {
                "bestCohort": {"month": best["month"], "score": best["score"]},
                "worstCohort": {"month": worst["month"], "score": worst["score"]},
                "latestScore": latest["score"],
                "avgScore": avg_score,
            }

        # ── 5A.4 Revenue impact ──
        revenue_impact = None
        cohorts_with_data = [
            c for c in sorted_cohorts
            if c["retention"].get(1) is not None and c["size"] > 0
        ]
        if cohorts_with_data:
            best_m1 = max(float(c["retention"].get(1, 0)) for c in cohorts_with_data)
            total_extra_customers = 0.0
            total_m1_revenue = 0.0
            total_m1_customers = 0
            for c in cohorts_with_data:
                current_m1 = float(c["retention"].get(1, 0) or 0)
                m1_rev = float(c.get("revenue", {}).get(1, 0) or 0)
                m1_cust_count = round(c["size"] * current_m1 / 100) if current_m1 > 0 else 0
                total_m1_revenue += m1_rev
                total_m1_customers += m1_cust_count
                if current_m1 < best_m1:
                    extra = c["size"] * (best_m1 - current_m1) / 100
                    total_extra_customers += extra

            avg_rev_per_cust = (
                total_m1_revenue / total_m1_customers
                if total_m1_customers > 0 else 0
            )
            potential = round(total_extra_customers * avg_rev_per_cust, 2)
            num_cohorts = len(cohorts_with_data)
            monthly_potential = round(potential / num_cohorts, 2) if num_cohorts > 0 else 0

            revenue_impact = {
                "bestM1": best_m1,
                "potentialExtraCustomers": round(total_extra_customers),
                "potentialExtraRevenue": potential,
                "monthlyPotential": monthly_potential,
            }

        # ── 5A.5 Decay analysis ──
        m1_avg = float(avg_customer_retention.get(1, 0) or 0)
        half_life = None
        if m1_avg > 0:
            for m in range(2, retention_months + 1):
                val = float(avg_customer_retention.get(m, 0) or 0)
                if val <= m1_avg / 2:
                    half_life = m
                    break

        stabilization_month = None
        for m in range(2, retention_months + 1):
            prev = float(avg_customer_retention.get(m - 1, 0) or 0)
            curr = float(avg_customer_retention.get(m, 0) or 0)
            if prev > 0 and abs(prev - curr) < 2:
                stabilization_month = m
                break

        terminal_raw = avg_customer_retention.get(retention_months)
        terminal = float(terminal_raw) if terminal_raw is not None else None
        m3_val = float(avg_customer_retention.get(3, 0) or 0)
        m1_to_m3_drop = round(m1_avg - m3_val, 1) if m1_avg > 0 and m3_val else 0

        decay_analysis = {
            "halfLifeMonth": half_life,
            "stabilizationMonth": stabilization_month,
            "terminalRetention": terminal,
            "m1ToM3Drop": m1_to_m3_drop,
        }

        return {
            "retentionTrend": retention_trend,
            "cohortQualityTrend": cohort_quality,
            "revenueImpact": revenue_impact,
            "decayAnalysis": decay_analysis,
        }

    async def get_days_to_second_purchase(
        self,
        months_back: int = 12,
        sales_type: str = "retail"
    ) -> Dict[str, Any]:
        """
        Analyze time between first and second purchase.

        Groups customers into buckets based on how many days it took them
        to make their second purchase. Useful for understanding repurchase cycles.

        Args:
            months_back: How many months of first-time customers to analyze
            sales_type: Filter by sales type (retail/b2b/all)

        Returns:
            Dict with buckets, customer counts, and summary statistics
        """
        async with self.connection() as conn:
            # Build sales type filter
            sales_type_filter = ""
            if sales_type == "retail":
                sales_type_filter = f"""
                    AND (o.manager_id IN ({','.join(map(str, RETAIL_MANAGER_IDS))})
                         OR (o.manager_id IS NULL AND o.source_id = 4))
                """
            elif sales_type == "b2b":
                sales_type_filter = f"AND o.manager_id = {B2B_MANAGER_ID}"

            query = f"""
            WITH customer_orders_ranked AS (
                SELECT
                    o.buyer_id,
                    o.order_date,
                    ROW_NUMBER() OVER (PARTITION BY o.buyer_id ORDER BY o.order_date) AS order_num
                FROM silver_orders o
                WHERE o.buyer_id IS NOT NULL
                  AND NOT o.is_return
                  {sales_type_filter}
            ),
            second_purchase AS (
                SELECT
                    c1.buyer_id,
                    DATEDIFF('day', c1.order_date, c2.order_date) AS days_to_second
                FROM customer_orders_ranked c1
                JOIN customer_orders_ranked c2
                    ON c1.buyer_id = c2.buyer_id
                    AND c1.order_num = 1
                    AND c2.order_num = 2
                WHERE c1.order_date >= CURRENT_DATE - INTERVAL '{int(months_back)} months'
            ),
            bucketed AS (
                SELECT
                    days_to_second,
                    CASE
                        WHEN days_to_second <= 30 THEN '0-30'
                        WHEN days_to_second <= 60 THEN '31-60'
                        WHEN days_to_second <= 90 THEN '61-90'
                        WHEN days_to_second <= 120 THEN '91-120'
                        WHEN days_to_second <= 180 THEN '121-180'
                        ELSE '180+'
                    END AS bucket,
                    CASE
                        WHEN days_to_second <= 30 THEN 1
                        WHEN days_to_second <= 60 THEN 2
                        WHEN days_to_second <= 90 THEN 3
                        WHEN days_to_second <= 120 THEN 4
                        WHEN days_to_second <= 180 THEN 5
                        ELSE 6
                    END AS bucket_order
                FROM second_purchase
            ),
            global_stats AS (
                SELECT
                    MEDIAN(days_to_second) AS median_days,
                    AVG(days_to_second) AS avg_days,
                    COUNT(*) AS total_count
                FROM second_purchase
            )
            SELECT
                b.bucket,
                COUNT(*) AS customers,
                ROUND(AVG(b.days_to_second), 1) AS avg_days,
                (SELECT median_days FROM global_stats) AS median_days,
                (SELECT avg_days FROM global_stats) AS avg_days_overall,
                (SELECT total_count FROM global_stats) AS total_count
            FROM bucketed b
            GROUP BY b.bucket, b.bucket_order
            ORDER BY b.bucket_order
            """

            rows = conn.execute(query).fetchall()

            # Extract global stats from first row
            median_days = rows[0][3] if rows else None
            avg_days_overall = rows[0][4] if rows else None

            # Calculate totals and percentages
            total_repeat = sum(row[1] for row in rows)
            buckets = []
            for row in rows:
                bucket, customers, avg_days = row[0], row[1], row[2]
                buckets.append({
                    "bucket": bucket,
                    "customers": customers,
                    "avgDays": avg_days,
                    "percentage": round(100.0 * customers / total_repeat, 1) if total_repeat > 0 else 0
                })

            return {
                "buckets": buckets,
                "summary": {
                    "totalRepeatCustomers": total_repeat,
                    "medianDays": round(median_days, 1) if median_days else None,
                    "avgDays": round(avg_days_overall, 1) if avg_days_overall else None
                }
            }

    async def get_cohort_ltv(
        self,
        months_back: int = 12,
        retention_months: int = 12,
        sales_type: str = "retail"
    ) -> Dict[str, Any]:
        """
        Get cumulative lifetime value by cohort.

        Shows how much revenue each cohort has generated over time,
        with cumulative totals per month since first purchase.

        Args:
            months_back: How many months of cohorts to analyze
            sales_type: Filter by sales type (retail/b2b/all)

        Returns:
            Dict with cohort LTV data and summary statistics
        """
        async with self.connection() as conn:
            # Build sales type filter
            sales_type_filter = ""
            if sales_type == "retail":
                sales_type_filter = f"""
                    AND (o.manager_id IN ({','.join(map(str, RETAIL_MANAGER_IDS))})
                         OR (o.manager_id IS NULL AND o.source_id = 4))
                """
            elif sales_type == "b2b":
                sales_type_filter = f"AND o.manager_id = {B2B_MANAGER_ID}"

            query = f"""
            WITH customer_cohorts AS (
                SELECT
                    o.buyer_id,
                    DATE_TRUNC('month', MIN(o.order_date)) AS cohort_month
                FROM silver_orders o
                WHERE o.buyer_id IS NOT NULL
                  AND NOT o.is_return
                  {sales_type_filter}
                GROUP BY o.buyer_id
            ),
            customer_revenue AS (
                SELECT
                    o.buyer_id,
                    c.cohort_month,
                    DATEDIFF('month', c.cohort_month, DATE_TRUNC('month', o.order_date)) AS months_since,
                    SUM(o.grand_total) AS revenue
                FROM silver_orders o
                JOIN customer_cohorts c ON o.buyer_id = c.buyer_id
                WHERE NOT o.is_return
                  {sales_type_filter}
                GROUP BY o.buyer_id, c.cohort_month, DATEDIFF('month', c.cohort_month, DATE_TRUNC('month', o.order_date))
            ),
            cohort_monthly AS (
                SELECT
                    cohort_month,
                    months_since,
                    SUM(revenue) AS total_revenue,
                    COUNT(DISTINCT buyer_id) AS active_customers
                FROM customer_revenue
                WHERE months_since <= ?
                GROUP BY cohort_month, months_since
            ),
            cohort_sizes AS (
                SELECT cohort_month, COUNT(DISTINCT buyer_id) AS cohort_size
                FROM customer_cohorts
                GROUP BY cohort_month
            )
            SELECT
                strftime(cm.cohort_month, '%Y-%m') AS cohort,
                cs.cohort_size,
                cm.months_since,
                cm.total_revenue,
                cm.active_customers
            FROM cohort_monthly cm
            JOIN cohort_sizes cs ON cm.cohort_month = cs.cohort_month
            WHERE cm.cohort_month >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '{int(months_back)} months'
            ORDER BY cm.cohort_month DESC, cm.months_since
            """

            rows = conn.execute(query, [retention_months]).fetchall()

            # Build cohort LTV structure with cumulative revenue
            cohorts = {}
            for cohort, size, months_since, revenue, active in rows:
                if cohort not in cohorts:
                    cohorts[cohort] = {
                        "size": size,
                        "monthly_revenue": {},
                        "cumulative": []
                    }
                cohorts[cohort]["monthly_revenue"][months_since] = revenue or 0

            # Calculate cumulative revenue for each cohort
            for cohort_data in cohorts.values():
                cumulative = 0
                cumulative_list = []
                for m in range(retention_months + 1):  # M0 to Mn
                    cumulative += cohort_data["monthly_revenue"].get(m, 0)
                    cumulative_list.append(round(cumulative, 2))
                cohort_data["cumulative"] = cumulative_list

            # Calculate weighted average LTV (weight = cohort size)
            total_rev = sum(c["cumulative"][-1] for c in cohorts.values())
            total_size = sum(c["size"] for c in cohorts.values())
            avg_ltv = round(total_rev / total_size, 2) if total_size > 0 else 0

            # Find best cohort
            best_cohort = max(
                cohorts.items(),
                key=lambda x: x[1]["cumulative"][-1] / x[1]["size"] if x[1]["size"] > 0 else 0,
                default=(None, {"cumulative": [0], "size": 1})
            )

            return {
                "cohorts": [
                    {
                        "month": cohort,
                        "customerCount": data["size"],
                        "cumulativeRevenue": data["cumulative"],
                        "avgLTV": round(data["cumulative"][-1] / data["size"], 2) if data["size"] > 0 else 0
                    }
                    for cohort, data in sorted(cohorts.items(), reverse=True)
                ],
                "summary": {
                    "avgLTV": avg_ltv,
                    "bestCohort": best_cohort[0],
                    "bestCohortLTV": round(best_cohort[1]["cumulative"][-1] / best_cohort[1]["size"], 2) if best_cohort[1]["size"] > 0 else 0
                }
            }

    async def get_at_risk_customers(
        self,
        days_threshold: int = 90,
        months_back: int = 12,
        sales_type: str = "retail"
    ) -> Dict[str, Any]:
        """
        Identify at-risk customers who haven't purchased recently.

        Segments customers by their cohort and identifies those who haven't
        made a purchase in the specified number of days.

        Args:
            days_threshold: Days since last purchase to consider "at risk"
            sales_type: Filter by sales type (retail/b2b/all)

        Returns:
            Dict with at-risk counts by cohort and summary statistics
        """
        async with self.connection() as conn:
            # Build sales type filter
            sales_type_filter = ""
            if sales_type == "retail":
                sales_type_filter = f"""
                    AND (o.manager_id IN ({','.join(map(str, RETAIL_MANAGER_IDS))})
                         OR (o.manager_id IS NULL AND o.source_id = 4))
                """
            elif sales_type == "b2b":
                sales_type_filter = f"AND o.manager_id = {B2B_MANAGER_ID}"

            churn_threshold = days_threshold * 2

            query = f"""
            WITH customer_activity AS (
                SELECT
                    o.buyer_id,
                    DATE_TRUNC('month', MIN(o.order_date)) AS cohort_month,
                    MAX(o.order_date) AS last_order_date,
                    DATEDIFF('day', MAX(o.order_date), CURRENT_DATE) AS days_since_last,
                    COUNT(*) AS total_orders,
                    SUM(o.grand_total) AS total_revenue
                FROM silver_orders o
                WHERE o.buyer_id IS NOT NULL
                  AND NOT o.is_return
                  {sales_type_filter}
                GROUP BY o.buyer_id
            )
            SELECT
                strftime(cohort_month, '%Y-%m') AS cohort,
                COUNT(*) AS total_customers,
                COUNT(*) FILTER (WHERE days_since_last > ? AND days_since_last <= ?) AS at_risk_count,
                ROUND(100.0 * COUNT(*) FILTER (WHERE days_since_last > ?) / COUNT(*), 1) AS at_risk_pct,
                SUM(total_revenue) FILTER (WHERE days_since_last > ?) AS at_risk_revenue,
                AVG(total_orders) FILTER (WHERE days_since_last > ?) AS avg_orders_at_risk,
                COUNT(*) FILTER (WHERE days_since_last > ?) AS churned_count
            FROM customer_activity
            WHERE cohort_month >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '{int(months_back)} months'
            GROUP BY cohort_month
            ORDER BY cohort_month DESC
            """

            rows = conn.execute(query, [
                days_threshold, churn_threshold,  # at_risk_count (between threshold and 2x)
                days_threshold,  # at_risk_pct (> threshold)
                days_threshold,  # at_risk_revenue
                days_threshold,  # avg_orders_at_risk
                churn_threshold,  # churned_count (> 2x threshold)
            ]).fetchall()

            cohorts = []
            total_at_risk = 0
            total_churned = 0
            total_customers = 0
            for cohort, total, at_risk, pct, revenue, avg_orders, churned in rows:
                cohorts.append({
                    "cohort": cohort,
                    "totalCustomers": total,
                    "atRiskCount": at_risk,
                    "atRiskPct": pct,
                    "atRiskRevenue": round(revenue, 2) if revenue else 0,
                    "avgOrdersAtRisk": round(avg_orders, 1) if avg_orders else 0,
                    "churnedCount": churned
                })
                total_at_risk += at_risk
                total_churned += churned
                total_customers += total

            return {
                "cohorts": cohorts,
                "daysThreshold": days_threshold,
                "summary": {
                    "totalAtRisk": total_at_risk,
                    "totalCustomers": total_customers,
                    "overallAtRiskPct": round(100.0 * total_at_risk / total_customers, 1) if total_customers > 0 else 0,
                    "totalChurned": total_churned,
                    "churnPct": round(100.0 * total_churned / total_customers, 1) if total_customers > 0 else 0
                }
            }
