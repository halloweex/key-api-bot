"""DuckDBStore customer insights methods."""
from __future__ import annotations

from datetime import date, timedelta
from typing import Optional, Dict, Any

from core.duckdb_constants import B2B_MANAGER_ID, RETAIL_MANAGER_IDS

# Tier cut-offs per LTV basis for get_sms_segments.
#
# The margin figures are calibrated so each tier selects roughly the same
# share of the base as its revenue counterpart (blended margin runs ~54%):
# on current data revenue >=10000/5000 picks 1325/3108 customers, margin
# >=5500/2750 picks 1346/3149. Switching basis therefore re-ranks who lands
# in a tier without silently resizing it.
SMS_LTV_BASES = ("revenue", "margin")

SMS_TIER_DEFAULTS = {
    "revenue": {"vip": 10000.0, "core": 5000.0},
    "margin": {"vip": 5500.0, "core": 2750.0},
}


class CustomersMixin:

    async def get_customer_insights(
        self,
        start_date: date,
        end_date: date,
        source_id: Optional[int] = None,
        brand: Optional[str] = None,
        sales_type: str = "retail",
        promocode: Optional[str] = None,
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
            if promocode:
                pf_where.append("UPPER(s.promocode) = UPPER(?)")
                pf_params.append(promocode)
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

    async def get_sms_segments(
        self,
        max_recency_days: int = 270,
        vip_ltv: Optional[float] = None,
        core_ltv: Optional[float] = None,
        core_min_orders: int = 2,
        reactivation_max_recency: int = 120,
        sales_type: str = "retail",
        ltv_basis: str = "revenue",
        holdout_pct: int = 10,
        campaign: str = "default",
        tier: Optional[str] = None,
        include_customers: bool = False,
        limit: int = 20000,
    ) -> Dict[str, Any]:
        """
        Build RFM-based SMS campaign segments with a deterministic holdout group.

        Customers are scored on recency / frequency / lifetime value and assigned
        to exactly one tier (most valuable wins):

        * ``VIP``          - ltv >= vip_ltv. High baseline repeat rate; discounting
          them mostly cannibalises purchases that would have happened anyway.
        * ``CORE``         - orders >= core_min_orders OR ltv >= core_ltv.
        * ``REACTIVATION`` - single-order buyers still inside
          ``reactivation_max_recency`` days. Lowest baseline, highest headroom.

        Everyone outside ``max_recency_days``, without a usable phone number, or
        a single-order buyer past ``reactivation_max_recency`` is dropped.

        ``ltv_basis`` picks which lifetime value drives the tiering:

        * ``revenue`` - lifetime revenue.
        * ``margin``  - lifetime contribution margin (revenue minus COGS from
          ``offer_stocks.purchased_price``). Preferred when margin varies by
          brand, since revenue ranking otherwise steers budget towards
          low-margin customers.

        Both figures are always returned, so the two bases can be compared on
        the same people. Margin can be negative (goods sold below cost) and is
        deliberately not clipped. Where a SKU has no cost, that line is left out
        of the margin but still counted in revenue — ``costCoverage`` reports
        the costed share so under-scored customers are visible.

        ``holdout_pct`` of each tier is marked ``holdout`` instead of ``target``
        so campaign uplift can be measured. The split is a hash of
        (buyer_id, campaign): stable across reruns of the same campaign, and
        re-drawn when ``campaign`` changes so the same people are not always
        withheld.

        Args:
            max_recency_days: Drop customers whose last order is older than this.
            vip_ltv: VIP cut-off; defaults per basis (see SMS_TIER_DEFAULTS).
            core_ltv: CORE cut-off without repeat orders; defaults per basis.
            core_min_orders: Order count that qualifies for CORE.
            reactivation_max_recency: Recency cap for single-order buyers.
            sales_type: retail / b2b / all.
            ltv_basis: revenue or margin — which LTV drives tier assignment.
            holdout_pct: Percent of each tier withheld as control (0 disables).
            campaign: Campaign label; also seeds the holdout split.
            tier: Return only this tier (VIP / CORE / REACTIVATION).
            include_customers: Include the customer rows, not just the summary.
            limit: Max customer rows returned when include_customers is set.

        Returns:
            Dict with per-tier summary, totals and (optionally) customer rows.

        Raises:
            ValueError: If ltv_basis is not one of SMS_LTV_BASES.
        """
        if ltv_basis not in SMS_LTV_BASES:
            raise ValueError(
                f"ltv_basis must be one of {', '.join(SMS_LTV_BASES)}, got {ltv_basis!r}"
            )

        defaults = SMS_TIER_DEFAULTS[ltv_basis]
        if vip_ltv is None:
            vip_ltv = defaults["vip"]
        if core_ltv is None:
            core_ltv = defaults["core"]

        if tier is not None:
            tier = tier.upper()

        async with self.connection() as conn:
            # Silver already classifies each order, so filter on the column
            # rather than re-deriving retail/b2b from manager_id here.
            sales_type_filter = "" if sales_type == "all" else "AND o.sales_type = ?"

            # Phones are stored as free text; normalise to digits and keep only
            # full Ukrainian MSISDNs (380 + 9 digits). Everything shorter is a
            # partial record that no SMS gateway will accept.
            # Which lifetime value drives tiering. Both are always computed.
            ltv_column = "revenue_ltv" if ltv_basis == "revenue" else "margin_ltv"

            query = f"""
            WITH line_items AS (
                SELECT
                    o.id AS order_id,
                    o.buyer_id,
                    o.order_date,
                    o.grand_total,
                    op.price_sold * op.quantity AS line_revenue,
                    CASE WHEN os.purchased_price > 0
                         THEN os.purchased_price * op.quantity END AS line_cogs
                FROM silver_orders o
                JOIN order_products op ON op.order_id = o.id
                LEFT JOIN products p ON p.id = op.product_id
                LEFT JOIN offer_stocks os ON os.sku = p.sku
                WHERE o.buyer_id IS NOT NULL
                  AND NOT o.is_return
                  -- Same revenue definition the Gold layer uses: deprecated
                  -- sources (Opencart et al.) must not inflate LTV or recency.
                  AND o.is_active_source
                  {sales_type_filter}
            ),
            allocated AS (
                -- Order-level discounts live in grand_total, not in the line
                -- prices (line totals run ~1.5% above grand_total), so spread
                -- each order's grand_total across its lines pro rata. That
                -- charges the discount to margin, which is where it belongs:
                -- a customer who only ever buys on discount is worth less.
                SELECT
                    buyer_id, order_id, order_date, line_cogs,
                    COALESCE(
                        grand_total * line_revenue
                            / NULLIF(SUM(line_revenue) OVER (PARTITION BY order_id), 0),
                        0
                    ) AS revenue
                FROM line_items
            ),
            cust AS (
                SELECT
                    buyer_id,
                    COUNT(DISTINCT order_id) AS orders,
                    SUM(revenue) AS revenue_ltv,
                    -- Uncosted lines drop out of margin but stay in revenue;
                    -- cost_coverage exposes how much of the customer is costed.
                    COALESCE(SUM(revenue - line_cogs) FILTER (WHERE line_cogs IS NOT NULL), 0)
                        AS margin_ltv,
                    COALESCE(SUM(revenue) FILTER (WHERE line_cogs IS NOT NULL), 0)
                        / NULLIF(SUM(revenue), 0) AS cost_coverage,
                    MAX(order_date) AS last_order_date,
                    MIN(order_date) AS first_order_date,
                    DATEDIFF('day', MAX(order_date), CURRENT_DATE) AS recency
                FROM allocated
                GROUP BY buyer_id
            ),
            scored AS (
                SELECT
                    c.*,
                    b.full_name,
                    b.city,
                    regexp_replace(COALESCE(b.phone, ''), '[^0-9]', '', 'g') AS phone,
                    CASE
                        WHEN c.{ltv_column} >= ? THEN 'VIP'
                        WHEN c.orders >= ? OR c.{ltv_column} >= ? THEN 'CORE'
                        WHEN c.recency <= ? THEN 'REACTIVATION'
                    END AS tier
                FROM cust c
                JOIN buyers b ON b.id = c.buyer_id
                WHERE c.recency <= ?
            ),
            eligible AS (
                SELECT *
                FROM scored
                WHERE tier IS NOT NULL
                  AND length(phone) = 12
                  AND phone LIKE '380%'
                -- One SMS per phone number: shared numbers across buyer records
                -- would otherwise be messaged twice.
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY phone ORDER BY {ltv_column} DESC, buyer_id
                ) = 1
            )
            SELECT
                buyer_id, full_name, phone, city, tier, orders,
                ROUND({ltv_column}, 2) AS ltv,
                ROUND({ltv_column} / orders, 2) AS aov,
                ROUND(revenue_ltv, 2) AS revenue_ltv,
                ROUND(margin_ltv, 2) AS margin_ltv,
                ROUND(100.0 * margin_ltv / NULLIF(revenue_ltv, 0), 1) AS margin_pct,
                ROUND(100.0 * COALESCE(cost_coverage, 0), 1) AS cost_coverage,
                recency, last_order_date, first_order_date,
                CASE WHEN hash(buyer_id::VARCHAR || '|' || ?) % 100 < ?
                     THEN 'holdout' ELSE 'target' END AS assignment
            FROM eligible
            {"WHERE tier = ?" if tier else ""}
            ORDER BY tier, ltv DESC, buyer_id
            """

            # Bound in textual order of the `?` placeholders above.
            params: list = []
            if sales_type != "all":
                params.append(sales_type)
            params += [
                vip_ltv, core_min_orders, core_ltv, reactivation_max_recency,
                max_recency_days, campaign, holdout_pct,
            ]
            if tier:
                params.append(tier)

            rows = conn.execute(query, params).fetchall()

        tiers: Dict[str, Dict[str, Any]] = {}
        customers = []
        for (buyer_id, full_name, phone, city, row_tier, orders, ltv, aov,
             revenue_ltv, margin_ltv, margin_pct, cost_coverage,
             recency, last_order, first_order, assignment) in rows:
            stats = tiers.setdefault(row_tier, {
                "tier": row_tier, "total": 0, "target": 0, "holdout": 0,
                "ltv": 0.0, "revenue": 0.0, "margin": 0.0,
                "_recency_sum": 0, "_orders_sum": 0,
            })
            stats["total"] += 1
            stats[assignment] += 1
            stats["ltv"] += float(ltv or 0)
            stats["revenue"] += float(revenue_ltv or 0)
            stats["margin"] += float(margin_ltv or 0)
            stats["_recency_sum"] += recency
            stats["_orders_sum"] += orders

            if include_customers and len(customers) < limit:
                customers.append({
                    "buyerId": buyer_id,
                    "fullName": full_name,
                    "phone": phone,
                    "city": city,
                    "tier": row_tier,
                    "orders": orders,
                    "ltv": float(ltv or 0),
                    "avgOrderValue": float(aov or 0),
                    "revenueLtv": float(revenue_ltv or 0),
                    "marginLtv": float(margin_ltv or 0),
                    "marginPct": float(margin_pct) if margin_pct is not None else None,
                    "costCoverage": float(cost_coverage or 0),
                    "recencyDays": recency,
                    "lastOrderDate": last_order.isoformat() if last_order else None,
                    "firstOrderDate": first_order.isoformat() if first_order else None,
                    "assignment": assignment,
                })

        tier_order = {"VIP": 0, "CORE": 1, "REACTIVATION": 2}
        summary = []
        for stats in sorted(tiers.values(), key=lambda s: tier_order.get(s["tier"], 9)):
            total = stats["total"]
            summary.append({
                "tier": stats["tier"],
                "total": total,
                "target": stats["target"],
                "holdout": stats["holdout"],
                "totalLtv": round(stats["ltv"], 2),
                "avgLtv": round(stats["ltv"] / total, 2) if total else 0,
                "totalRevenue": round(stats["revenue"], 2),
                "totalMargin": round(stats["margin"], 2),
                "marginPct": round(100.0 * stats["margin"] / stats["revenue"], 1)
                             if stats["revenue"] else None,
                "avgOrders": round(stats["_orders_sum"] / total, 2) if total else 0,
                "avgRecencyDays": round(stats["_recency_sum"] / total) if total else 0,
            })

        total_customers = sum(s["total"] for s in summary)
        return {
            "campaign": campaign,
            "salesType": sales_type,
            "ltvBasis": ltv_basis,
            "criteria": {
                "maxRecencyDays": max_recency_days,
                "ltvBasis": ltv_basis,
                "vipLtv": vip_ltv,
                "coreLtv": core_ltv,
                "coreMinOrders": core_min_orders,
                "reactivationMaxRecency": reactivation_max_recency,
                "holdoutPct": holdout_pct,
            },
            "segments": summary,
            "totals": {
                "customers": total_customers,
                "target": sum(s["target"] for s in summary),
                "holdout": sum(s["holdout"] for s in summary),
            },
            "customers": customers if include_customers else [],
            "truncated": include_customers and total_customers > limit,
        }
