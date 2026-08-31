"""The `/inventory` views learn whose day "today" is.

Revision ID: 0015_inventory_views_today
Revises: 0014_inventory_views
Created: 2026-08-31

Revision 0014 rendered `CURRENT_DATE` verbatim into eleven Postgres views, and
that is wrong here in a way that only shows up at night.

The production `web` container runs `TZ=Europe/Kyiv`, so DuckDB's
`CURRENT_DATE` is already the day the business is in. The Postgres server
answers in UTC. Measured on production, 2026-08-31: DuckDB `Europe/Kyiv`,
Postgres `UTC`. **Between 21:00 and midnight UTC the two name different days**,
and every `days_since_sale`, every aging bucket, every dead-stock threshold and
every 30/90-day window would move by one — for three hours, every night, on one
engine only, and only while `KS_READ_INVENTORY=postgres`.

The differential test could not have caught it: both engines run in the same
timezone under the gate, so both were right together. It was found by asking
the two production containers what day it was.

So the views take `Dialect.today` rather than the bare keyword. DuckDB keeps
`CURRENT_DATE`, which is why this changes nothing about the answers it gives;
Postgres is told the day rather than trusted to guess it.

WHY A NEW REVISION AND NOT AN EDIT TO 0014

0014 is deployed. A migration that has run somewhere is history, and history is
appended to. The eleven are dropped and recreated rather than replaced in
place: they depend on each other, so a single order — the same order 0014 uses
— is easier to be sure of than eleven independent `CREATE OR REPLACE`
statements. Alembic runs this in one transaction, so no reader sees the gap.
"""
from alembic import op

revision = "0015_inventory_views_today"
down_revision = "0014_inventory_views"
branch_labels = None
depends_on = None

# The Postgres rendering of core.sql_dialect.inventory_view_selects(POSTGRES).
# Frozen here; a test asserts this is the newest migration to hold a copy and
# that the copy is what the body renders today. ORDER IS LOAD-BEARING.
_VIEWS_IN_DEPENDENCY_ORDER = (
    "gold.v_sku_analysis",
    "gold.v_category_velocity",
    "gold.v_sku_status",
    "gold.v_inventory_summary",
    "gold.v_aging_buckets",
    "gold.v_sku_sell_through",
    "gold.v_abc_classification",
    "gold.v_abc_summary",
    "gold.v_recommended_actions",
    "gold.v_restock_alerts",
    "gold.v_sku_dead_stock_v2",
)


def _drop_all() -> None:
    for name in reversed(_VIEWS_IN_DEPENDENCY_ORDER):
        op.execute(f"DROP VIEW IF EXISTS {name}")


def upgrade() -> None:
    _drop_all()

    op.execute(
        """
        CREATE VIEW gold.v_sku_analysis AS
        SELECT
            s.*,
            c.name as category_name,
            s.quantity - s.reserve as available,
            s.quantity * s.price as stock_value,
            (s.quantity - s.reserve) * s.price as available_value,
            (now() AT TIME ZONE 'Europe/Kyiv')::date - s.last_sale_date as days_since_sale,
            (now() AT TIME ZONE 'Europe/Kyiv')::date - s.first_seen_at as days_in_stock
        FROM app.sku_inventory_status s
        LEFT JOIN bronze.categories c ON s.category_id = c.id
        """
    )

    op.execute(
        """
        CREATE VIEW gold.v_category_velocity AS
        SELECT
            category_id,
            category_name,
            COUNT(*) as sample_size,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY days_since_sale) as p50,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY days_since_sale) as p75,
            PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY days_since_sale) as p90,
            LEAST(GREATEST(
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY days_since_sale),
                90
            ), 365) as threshold_days
        FROM gold.v_sku_analysis
        WHERE last_sale_date IS NOT NULL AND quantity > 0
        GROUP BY category_id, category_name
        HAVING COUNT(*) >= 5
        """
    )

    op.execute(
        """
        CREATE VIEW gold.v_sku_status AS
        SELECT
            s.*,
            COALESCE(cv.threshold_days, 180) as threshold_days,
            CASE WHEN COALESCE(vel.qty_sold_90d, 0) > 0
                 THEN ROUND((s.quantity - s.reserve) * 90.0 / vel.qty_sold_90d, 0)
                 ELSE NULL
            END as days_of_supply,
            CASE
                WHEN s.last_sale_date IS NULL THEN 'never_sold'
                WHEN s.days_since_sale > COALESCE(cv.threshold_days, 180) THEN 'dead_stock'
                WHEN s.days_since_sale > COALESCE(cv.threshold_days, 180) * 0.7 THEN 'at_risk'
                WHEN COALESCE(vel.qty_sold_90d, 0) > 0
                     AND ROUND((s.quantity - s.reserve) * 90.0 / vel.qty_sold_90d, 0) > 90
                    THEN 'overstocked'
                ELSE 'healthy'
            END as status
        FROM gold.v_sku_analysis s
        LEFT JOIN gold.v_category_velocity cv ON s.category_id = cv.category_id
        LEFT JOIN (
            SELECT product_id, SUM(quantity) as qty_sold_90d
            FROM silver.order_lines
            WHERE NOT is_return AND is_active_source
              AND order_date >= (now() AT TIME ZONE 'Europe/Kyiv')::date - INTERVAL '90 days'
            GROUP BY product_id
        ) vel ON s.product_id = vel.product_id
        WHERE s.quantity > 0
        """
    )

    op.execute(
        """
        CREATE VIEW gold.v_inventory_summary AS
        SELECT
            status,
            COUNT(*) as sku_count,
            SUM(available) as total_units,
            SUM(available_value) as total_value,
            ROUND(100.0 * SUM(available_value) /
                NULLIF(SUM(SUM(available_value)) OVER (), 0), 1) as value_pct
        FROM gold.v_sku_status
        GROUP BY status
        """
    )

    op.execute(
        """
        CREATE VIEW gold.v_aging_buckets AS
        SELECT
            CASE
                WHEN days_since_sale IS NULL THEN '6. Never sold'
                WHEN days_since_sale <= 30 THEN '1. 0-30 days'
                WHEN days_since_sale <= 90 THEN '2. 31-90 days'
                WHEN days_since_sale <= 180 THEN '3. 91-180 days'
                WHEN days_since_sale <= 365 THEN '4. 181-365 days'
                ELSE '5. 365+ days'
            END as bucket,
            COUNT(*) as sku_count,
            SUM(available) as units,
            SUM(available_value) as value
        FROM gold.v_sku_analysis
        WHERE quantity > 0
        GROUP BY bucket
        ORDER BY bucket
        """
    )

    op.execute(
        """
        CREATE VIEW gold.v_sku_sell_through AS
        SELECT
            s.offer_id,
            s.product_id,
            s.sku,
            s.name,
            s.brand,
            s.category_id,
            s.category_name,
            s.available,
            s.available_value,
            s.price,
            s.purchased_price,
            s.days_since_sale,
            s.days_in_stock,
            COALESCE(g30.qty_sold_30d, 0) as qty_sold_30d,
            COALESCE(g30.revenue_30d, 0) as revenue_30d,
            COALESCE(g30.orders_30d, 0) as orders_30d,
            COALESCE(g90.qty_sold_90d, 0) as qty_sold_90d,
            COALESCE(g90.revenue_90d, 0) as revenue_90d,
            CASE WHEN (COALESCE(g30.qty_sold_30d, 0) + s.available) > 0
                 THEN ROUND(100.0 * COALESCE(g30.qty_sold_30d, 0) /
                      (COALESCE(g30.qty_sold_30d, 0) + s.available), 1)
                 ELSE 0
            END as sell_through_rate_30d,
            CASE WHEN COALESCE(g90.qty_sold_90d, 0) > 0
                 THEN ROUND(s.available * 90.0 / g90.qty_sold_90d, 0)
                 ELSE NULL
            END as days_of_supply,
            CASE WHEN COALESCE(g90.qty_sold_90d, 0) > 0
                 THEN ROUND(g90.qty_sold_90d / 90.0, 2)
                 ELSE 0
            END as avg_daily_sales
        FROM gold.v_sku_analysis s
        LEFT JOIN (
            SELECT product_id,
                   SUM(quantity) as qty_sold_30d,
                   SUM(quantity * price_sold) as revenue_30d,
                   COUNT(DISTINCT order_id) as orders_30d
            FROM silver.order_lines
            WHERE NOT is_return AND is_active_source
              AND order_date >= (now() AT TIME ZONE 'Europe/Kyiv')::date - INTERVAL '30 days'
            GROUP BY product_id
        ) g30 ON s.product_id = g30.product_id
        LEFT JOIN (
            SELECT product_id,
                   SUM(quantity) as qty_sold_90d,
                   SUM(quantity * price_sold) as revenue_90d
            FROM silver.order_lines
            WHERE NOT is_return AND is_active_source
              AND order_date >= (now() AT TIME ZONE 'Europe/Kyiv')::date - INTERVAL '90 days'
            GROUP BY product_id
        ) g90 ON s.product_id = g90.product_id
        WHERE s.quantity > 0
        """
    )

    op.execute(
        """
        CREATE VIEW gold.v_abc_classification AS
        WITH product_revenue AS (
            SELECT
                product_id,
                SUM(quantity * price_sold) as total_revenue,
                SUM(quantity) as total_qty_sold
            FROM silver.order_lines
            WHERE NOT is_return AND is_active_source
              AND order_date >= (now() AT TIME ZONE 'Europe/Kyiv')::date - INTERVAL '90 days'
            GROUP BY product_id
        ),
        ranked AS (
            SELECT
                s.offer_id,
                s.product_id,
                s.sku,
                s.name,
                s.brand,
                s.category_name,
                s.available,
                s.available_value,
                s.price,
                COALESCE(pr.total_revenue, 0) as revenue_90d,
                COALESCE(pr.total_qty_sold, 0) as qty_sold_90d,
                SUM(COALESCE(pr.total_revenue, 0)) OVER () as grand_total_revenue,
                SUM(COALESCE(pr.total_revenue, 0)) OVER (
                    ORDER BY COALESCE(pr.total_revenue, 0) DESC
                    ROWS UNBOUNDED PRECEDING
                ) as cumulative_revenue
            FROM gold.v_sku_analysis s
            LEFT JOIN product_revenue pr ON s.product_id = pr.product_id
            WHERE s.quantity > 0
        )
        SELECT
            *,
            CASE WHEN grand_total_revenue > 0
                 THEN ROUND(100.0 * cumulative_revenue / grand_total_revenue, 1)
                 ELSE 0
            END as cumulative_pct,
            CASE
                WHEN grand_total_revenue > 0
                     AND cumulative_revenue - COALESCE(revenue_90d, 0) < grand_total_revenue * 0.8
                    THEN 'A'
                WHEN grand_total_revenue > 0
                     AND cumulative_revenue - COALESCE(revenue_90d, 0) < grand_total_revenue * 0.95
                    THEN 'B'
                ELSE 'C'
            END as abc_class
        FROM ranked
        """
    )

    op.execute(
        """
        CREATE VIEW gold.v_abc_summary AS
        SELECT
            abc_class,
            COUNT(*) as sku_count,
            SUM(available) as total_units,
            SUM(available_value) as stock_value,
            SUM(revenue_90d) as revenue,
            ROUND(100.0 * SUM(available_value) /
                NULLIF(SUM(SUM(available_value)) OVER (), 0), 1) as stock_value_pct,
            ROUND(100.0 * SUM(revenue_90d) /
                NULLIF(SUM(SUM(revenue_90d)) OVER (), 0), 1) as revenue_pct
        FROM gold.v_abc_classification
        GROUP BY abc_class
        ORDER BY abc_class
        """
    )

    op.execute(
        """
        CREATE VIEW gold.v_recommended_actions AS
        SELECT
            offer_id,
            sku,
            name,
            brand,
            category_name,
            available as units,
            available_value as value,
            days_since_sale,
            days_in_stock,
            status,
            CASE
                WHEN status = 'never_sold' AND days_in_stock > 180 THEN 'Return to supplier'
                WHEN status = 'never_sold' AND days_in_stock > 90 THEN 'Deep discount (70%+)'
                WHEN status = 'dead_stock' AND available_value > 10000 THEN 'Discount 50%'
                WHEN status = 'dead_stock' THEN 'Bundle with bestsellers'
                WHEN status = 'at_risk' THEN 'Promote / Feature'
                ELSE NULL
            END as action
        FROM gold.v_sku_status
        WHERE status != 'healthy'
        ORDER BY available_value DESC
        """
    )

    op.execute(
        """
        CREATE VIEW gold.v_restock_alerts AS
        SELECT
            offer_id,
            sku,
            name,
            brand,
            available as units_left,
            days_since_sale,
            CASE
                WHEN available = 0 THEN 'OUT_OF_STOCK'
                WHEN available <= 3 THEN 'CRITICAL'
                WHEN available <= 10 THEN 'LOW'
            END as alert_level
        FROM gold.v_sku_analysis
        WHERE available <= 10
          AND (days_since_sale IS NULL OR days_since_sale <= 90)
        ORDER BY available ASC
        """
    )

    op.execute(
        """
        CREATE VIEW gold.v_sku_dead_stock_v2 AS
        WITH cost_ratio AS (
            -- Portfolio-wide cost-to-sale ratio for fallback when purchased_price is missing
            SELECT
                COALESCE(
                    SUM(quantity * NULLIF(purchased_price, 0)) /
                    NULLIF(SUM(quantity * NULLIF(price, 0)), 0),
                    0.5
                ) as ratio
            FROM bronze.offer_stocks
            WHERE quantity > 0
        ),
        sales_90 AS (
            SELECT product_id,
                   SUM(quantity) as qty_sold_90d,
                   SUM(quantity * price_sold) as revenue_90d
            FROM silver.order_lines
            WHERE NOT is_return AND is_active_source
              AND order_date >= (now() AT TIME ZONE 'Europe/Kyiv')::date - INTERVAL '90 days'
            GROUP BY product_id
        ),
        sales_30 AS (
            SELECT product_id,
                   SUM(quantity) as qty_sold_30d,
                   SUM(quantity * price_sold) as revenue_30d
            FROM silver.order_lines
            WHERE NOT is_return AND is_active_source
              AND order_date >= (now() AT TIME ZONE 'Europe/Kyiv')::date - INTERVAL '30 days'
            GROUP BY product_id
        ),
        base AS (
            SELECT
                s.offer_id,
                s.product_id,
                s.sku,
                s.name,
                s.brand,
                s.category_id,
                s.category_name,
                s.available,
                s.price,
                s.purchased_price,
                s.days_since_sale,
                s.days_in_stock,
                s.last_sale_date,
                COALESCE(
                    NULLIF(s.purchased_price, 0),
                    s.price * (SELECT ratio FROM cost_ratio)
                ) as effective_unit_cost,
                CASE
                    WHEN s.purchased_price IS NULL OR s.purchased_price = 0
                        THEN 'fallback'
                    ELSE 'actual'
                END as cost_quality,
                COALESCE(s90.qty_sold_90d, 0) as qty_sold_90d,
                COALESCE(s90.revenue_90d, 0) as revenue_90d,
                COALESCE(s30.qty_sold_30d, 0) as qty_sold_30d,
                COALESCE(s30.revenue_30d, 0) as revenue_30d,
                COALESCE(a.abc_class, 'C') as abc_class
            FROM gold.v_sku_analysis s
            LEFT JOIN sales_90 s90 ON s.product_id = s90.product_id
            LEFT JOIN sales_30 s30 ON s.product_id = s30.product_id
            LEFT JOIN gold.v_abc_classification a ON s.offer_id = a.offer_id
            WHERE s.quantity > 0
        )
        SELECT
            b.*,
            b.available * b.price as sale_value,
            b.available * b.effective_unit_cost as cost_basis,
            CASE WHEN b.qty_sold_90d > 0
                 THEN ROUND(b.available * 90.0 / b.qty_sold_90d, 0)
                 ELSE NULL
            END as days_of_supply,
            CASE WHEN b.qty_sold_90d > 0 THEN ROUND(b.qty_sold_90d / 90.0, 3) ELSE 0 END as avg_daily_sales_90d,
            CASE WHEN b.qty_sold_30d > 0 THEN ROUND(b.qty_sold_30d / 30.0, 3) ELSE 0 END as avg_daily_sales_30d,
            CASE
                WHEN b.qty_sold_90d = 0 THEN 'frozen'
                WHEN b.available * 90.0 / b.qty_sold_90d > 365 THEN 'frozen'
                WHEN b.available * 90.0 / b.qty_sold_90d > 180 THEN 'cold'
                WHEN b.available * 90.0 / b.qty_sold_90d > 90 THEN 'warm'
                WHEN b.available * 90.0 / b.qty_sold_90d > 30 THEN 'healthy'
                ELSE 'hot'
            END as velocity_tier,
            -- Velocity decay: 30d rate vs 90d rate. <0.7 = slowing, >1.3 = accelerating
            CASE WHEN b.qty_sold_90d > 0 AND (b.qty_sold_90d / 90.0) > 0
                 THEN ROUND(b.qty_sold_30d * 3.0 / b.qty_sold_90d, 2)
                 ELSE NULL
            END as velocity_ratio_30_90,
            -- Annualized gross profit per SKU (revenue × 4 × margin)
            CASE WHEN b.price > 0
                 THEN b.revenue_90d * 4.0 * ((b.price - b.effective_unit_cost) / b.price)
                 ELSE 0
            END as annual_gross_profit,
            -- GMROI annualized: gross profit / cost_basis (avg inventory proxy = current)
            CASE WHEN (b.available * b.effective_unit_cost) > 0 AND b.price > 0
                 THEN b.revenue_90d * 4.0 * ((b.price - b.effective_unit_cost) / b.price)
                      / (b.available * b.effective_unit_cost)
                 ELSE NULL
            END as gmroi
        FROM base b
        """
    )


def downgrade() -> None:
    """Back to 0014's bodies, which is to say back to the bare keyword.

    Not a bare drop: a downgrade has to leave the schema matching the revision
    it lands on, and 0014 claims eleven views exist. The statements below are
    the same body with `CURRENT_DATE` back in place of `TODAY_IN_KYIV`, and a
    test asserts they are byte-identical to what 0014 froze — so this copy
    cannot drift from the thing it is restoring.
    """
    _drop_all()

    op.execute(
        """
        CREATE VIEW gold.v_sku_analysis AS
        SELECT
            s.*,
            c.name as category_name,
            s.quantity - s.reserve as available,
            s.quantity * s.price as stock_value,
            (s.quantity - s.reserve) * s.price as available_value,
            CURRENT_DATE - s.last_sale_date as days_since_sale,
            CURRENT_DATE - s.first_seen_at as days_in_stock
        FROM app.sku_inventory_status s
        LEFT JOIN bronze.categories c ON s.category_id = c.id
        """
    )

    op.execute(
        """
        CREATE VIEW gold.v_category_velocity AS
        SELECT
            category_id,
            category_name,
            COUNT(*) as sample_size,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY days_since_sale) as p50,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY days_since_sale) as p75,
            PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY days_since_sale) as p90,
            LEAST(GREATEST(
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY days_since_sale),
                90
            ), 365) as threshold_days
        FROM gold.v_sku_analysis
        WHERE last_sale_date IS NOT NULL AND quantity > 0
        GROUP BY category_id, category_name
        HAVING COUNT(*) >= 5
        """
    )

    op.execute(
        """
        CREATE VIEW gold.v_sku_status AS
        SELECT
            s.*,
            COALESCE(cv.threshold_days, 180) as threshold_days,
            CASE WHEN COALESCE(vel.qty_sold_90d, 0) > 0
                 THEN ROUND((s.quantity - s.reserve) * 90.0 / vel.qty_sold_90d, 0)
                 ELSE NULL
            END as days_of_supply,
            CASE
                WHEN s.last_sale_date IS NULL THEN 'never_sold'
                WHEN s.days_since_sale > COALESCE(cv.threshold_days, 180) THEN 'dead_stock'
                WHEN s.days_since_sale > COALESCE(cv.threshold_days, 180) * 0.7 THEN 'at_risk'
                WHEN COALESCE(vel.qty_sold_90d, 0) > 0
                     AND ROUND((s.quantity - s.reserve) * 90.0 / vel.qty_sold_90d, 0) > 90
                    THEN 'overstocked'
                ELSE 'healthy'
            END as status
        FROM gold.v_sku_analysis s
        LEFT JOIN gold.v_category_velocity cv ON s.category_id = cv.category_id
        LEFT JOIN (
            SELECT product_id, SUM(quantity) as qty_sold_90d
            FROM silver.order_lines
            WHERE NOT is_return AND is_active_source
              AND order_date >= CURRENT_DATE - INTERVAL '90 days'
            GROUP BY product_id
        ) vel ON s.product_id = vel.product_id
        WHERE s.quantity > 0
        """
    )

    op.execute(
        """
        CREATE VIEW gold.v_inventory_summary AS
        SELECT
            status,
            COUNT(*) as sku_count,
            SUM(available) as total_units,
            SUM(available_value) as total_value,
            ROUND(100.0 * SUM(available_value) /
                NULLIF(SUM(SUM(available_value)) OVER (), 0), 1) as value_pct
        FROM gold.v_sku_status
        GROUP BY status
        """
    )

    op.execute(
        """
        CREATE VIEW gold.v_aging_buckets AS
        SELECT
            CASE
                WHEN days_since_sale IS NULL THEN '6. Never sold'
                WHEN days_since_sale <= 30 THEN '1. 0-30 days'
                WHEN days_since_sale <= 90 THEN '2. 31-90 days'
                WHEN days_since_sale <= 180 THEN '3. 91-180 days'
                WHEN days_since_sale <= 365 THEN '4. 181-365 days'
                ELSE '5. 365+ days'
            END as bucket,
            COUNT(*) as sku_count,
            SUM(available) as units,
            SUM(available_value) as value
        FROM gold.v_sku_analysis
        WHERE quantity > 0
        GROUP BY bucket
        ORDER BY bucket
        """
    )

    op.execute(
        """
        CREATE VIEW gold.v_sku_sell_through AS
        SELECT
            s.offer_id,
            s.product_id,
            s.sku,
            s.name,
            s.brand,
            s.category_id,
            s.category_name,
            s.available,
            s.available_value,
            s.price,
            s.purchased_price,
            s.days_since_sale,
            s.days_in_stock,
            COALESCE(g30.qty_sold_30d, 0) as qty_sold_30d,
            COALESCE(g30.revenue_30d, 0) as revenue_30d,
            COALESCE(g30.orders_30d, 0) as orders_30d,
            COALESCE(g90.qty_sold_90d, 0) as qty_sold_90d,
            COALESCE(g90.revenue_90d, 0) as revenue_90d,
            CASE WHEN (COALESCE(g30.qty_sold_30d, 0) + s.available) > 0
                 THEN ROUND(100.0 * COALESCE(g30.qty_sold_30d, 0) /
                      (COALESCE(g30.qty_sold_30d, 0) + s.available), 1)
                 ELSE 0
            END as sell_through_rate_30d,
            CASE WHEN COALESCE(g90.qty_sold_90d, 0) > 0
                 THEN ROUND(s.available * 90.0 / g90.qty_sold_90d, 0)
                 ELSE NULL
            END as days_of_supply,
            CASE WHEN COALESCE(g90.qty_sold_90d, 0) > 0
                 THEN ROUND(g90.qty_sold_90d / 90.0, 2)
                 ELSE 0
            END as avg_daily_sales
        FROM gold.v_sku_analysis s
        LEFT JOIN (
            SELECT product_id,
                   SUM(quantity) as qty_sold_30d,
                   SUM(quantity * price_sold) as revenue_30d,
                   COUNT(DISTINCT order_id) as orders_30d
            FROM silver.order_lines
            WHERE NOT is_return AND is_active_source
              AND order_date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY product_id
        ) g30 ON s.product_id = g30.product_id
        LEFT JOIN (
            SELECT product_id,
                   SUM(quantity) as qty_sold_90d,
                   SUM(quantity * price_sold) as revenue_90d
            FROM silver.order_lines
            WHERE NOT is_return AND is_active_source
              AND order_date >= CURRENT_DATE - INTERVAL '90 days'
            GROUP BY product_id
        ) g90 ON s.product_id = g90.product_id
        WHERE s.quantity > 0
        """
    )

    op.execute(
        """
        CREATE VIEW gold.v_abc_classification AS
        WITH product_revenue AS (
            SELECT
                product_id,
                SUM(quantity * price_sold) as total_revenue,
                SUM(quantity) as total_qty_sold
            FROM silver.order_lines
            WHERE NOT is_return AND is_active_source
              AND order_date >= CURRENT_DATE - INTERVAL '90 days'
            GROUP BY product_id
        ),
        ranked AS (
            SELECT
                s.offer_id,
                s.product_id,
                s.sku,
                s.name,
                s.brand,
                s.category_name,
                s.available,
                s.available_value,
                s.price,
                COALESCE(pr.total_revenue, 0) as revenue_90d,
                COALESCE(pr.total_qty_sold, 0) as qty_sold_90d,
                SUM(COALESCE(pr.total_revenue, 0)) OVER () as grand_total_revenue,
                SUM(COALESCE(pr.total_revenue, 0)) OVER (
                    ORDER BY COALESCE(pr.total_revenue, 0) DESC
                    ROWS UNBOUNDED PRECEDING
                ) as cumulative_revenue
            FROM gold.v_sku_analysis s
            LEFT JOIN product_revenue pr ON s.product_id = pr.product_id
            WHERE s.quantity > 0
        )
        SELECT
            *,
            CASE WHEN grand_total_revenue > 0
                 THEN ROUND(100.0 * cumulative_revenue / grand_total_revenue, 1)
                 ELSE 0
            END as cumulative_pct,
            CASE
                WHEN grand_total_revenue > 0
                     AND cumulative_revenue - COALESCE(revenue_90d, 0) < grand_total_revenue * 0.8
                    THEN 'A'
                WHEN grand_total_revenue > 0
                     AND cumulative_revenue - COALESCE(revenue_90d, 0) < grand_total_revenue * 0.95
                    THEN 'B'
                ELSE 'C'
            END as abc_class
        FROM ranked
        """
    )

    op.execute(
        """
        CREATE VIEW gold.v_abc_summary AS
        SELECT
            abc_class,
            COUNT(*) as sku_count,
            SUM(available) as total_units,
            SUM(available_value) as stock_value,
            SUM(revenue_90d) as revenue,
            ROUND(100.0 * SUM(available_value) /
                NULLIF(SUM(SUM(available_value)) OVER (), 0), 1) as stock_value_pct,
            ROUND(100.0 * SUM(revenue_90d) /
                NULLIF(SUM(SUM(revenue_90d)) OVER (), 0), 1) as revenue_pct
        FROM gold.v_abc_classification
        GROUP BY abc_class
        ORDER BY abc_class
        """
    )

    op.execute(
        """
        CREATE VIEW gold.v_recommended_actions AS
        SELECT
            offer_id,
            sku,
            name,
            brand,
            category_name,
            available as units,
            available_value as value,
            days_since_sale,
            days_in_stock,
            status,
            CASE
                WHEN status = 'never_sold' AND days_in_stock > 180 THEN 'Return to supplier'
                WHEN status = 'never_sold' AND days_in_stock > 90 THEN 'Deep discount (70%+)'
                WHEN status = 'dead_stock' AND available_value > 10000 THEN 'Discount 50%'
                WHEN status = 'dead_stock' THEN 'Bundle with bestsellers'
                WHEN status = 'at_risk' THEN 'Promote / Feature'
                ELSE NULL
            END as action
        FROM gold.v_sku_status
        WHERE status != 'healthy'
        ORDER BY available_value DESC
        """
    )

    op.execute(
        """
        CREATE VIEW gold.v_restock_alerts AS
        SELECT
            offer_id,
            sku,
            name,
            brand,
            available as units_left,
            days_since_sale,
            CASE
                WHEN available = 0 THEN 'OUT_OF_STOCK'
                WHEN available <= 3 THEN 'CRITICAL'
                WHEN available <= 10 THEN 'LOW'
            END as alert_level
        FROM gold.v_sku_analysis
        WHERE available <= 10
          AND (days_since_sale IS NULL OR days_since_sale <= 90)
        ORDER BY available ASC
        """
    )

    op.execute(
        """
        CREATE VIEW gold.v_sku_dead_stock_v2 AS
        WITH cost_ratio AS (
            -- Portfolio-wide cost-to-sale ratio for fallback when purchased_price is missing
            SELECT
                COALESCE(
                    SUM(quantity * NULLIF(purchased_price, 0)) /
                    NULLIF(SUM(quantity * NULLIF(price, 0)), 0),
                    0.5
                ) as ratio
            FROM bronze.offer_stocks
            WHERE quantity > 0
        ),
        sales_90 AS (
            SELECT product_id,
                   SUM(quantity) as qty_sold_90d,
                   SUM(quantity * price_sold) as revenue_90d
            FROM silver.order_lines
            WHERE NOT is_return AND is_active_source
              AND order_date >= CURRENT_DATE - INTERVAL '90 days'
            GROUP BY product_id
        ),
        sales_30 AS (
            SELECT product_id,
                   SUM(quantity) as qty_sold_30d,
                   SUM(quantity * price_sold) as revenue_30d
            FROM silver.order_lines
            WHERE NOT is_return AND is_active_source
              AND order_date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY product_id
        ),
        base AS (
            SELECT
                s.offer_id,
                s.product_id,
                s.sku,
                s.name,
                s.brand,
                s.category_id,
                s.category_name,
                s.available,
                s.price,
                s.purchased_price,
                s.days_since_sale,
                s.days_in_stock,
                s.last_sale_date,
                COALESCE(
                    NULLIF(s.purchased_price, 0),
                    s.price * (SELECT ratio FROM cost_ratio)
                ) as effective_unit_cost,
                CASE
                    WHEN s.purchased_price IS NULL OR s.purchased_price = 0
                        THEN 'fallback'
                    ELSE 'actual'
                END as cost_quality,
                COALESCE(s90.qty_sold_90d, 0) as qty_sold_90d,
                COALESCE(s90.revenue_90d, 0) as revenue_90d,
                COALESCE(s30.qty_sold_30d, 0) as qty_sold_30d,
                COALESCE(s30.revenue_30d, 0) as revenue_30d,
                COALESCE(a.abc_class, 'C') as abc_class
            FROM gold.v_sku_analysis s
            LEFT JOIN sales_90 s90 ON s.product_id = s90.product_id
            LEFT JOIN sales_30 s30 ON s.product_id = s30.product_id
            LEFT JOIN gold.v_abc_classification a ON s.offer_id = a.offer_id
            WHERE s.quantity > 0
        )
        SELECT
            b.*,
            b.available * b.price as sale_value,
            b.available * b.effective_unit_cost as cost_basis,
            CASE WHEN b.qty_sold_90d > 0
                 THEN ROUND(b.available * 90.0 / b.qty_sold_90d, 0)
                 ELSE NULL
            END as days_of_supply,
            CASE WHEN b.qty_sold_90d > 0 THEN ROUND(b.qty_sold_90d / 90.0, 3) ELSE 0 END as avg_daily_sales_90d,
            CASE WHEN b.qty_sold_30d > 0 THEN ROUND(b.qty_sold_30d / 30.0, 3) ELSE 0 END as avg_daily_sales_30d,
            CASE
                WHEN b.qty_sold_90d = 0 THEN 'frozen'
                WHEN b.available * 90.0 / b.qty_sold_90d > 365 THEN 'frozen'
                WHEN b.available * 90.0 / b.qty_sold_90d > 180 THEN 'cold'
                WHEN b.available * 90.0 / b.qty_sold_90d > 90 THEN 'warm'
                WHEN b.available * 90.0 / b.qty_sold_90d > 30 THEN 'healthy'
                ELSE 'hot'
            END as velocity_tier,
            -- Velocity decay: 30d rate vs 90d rate. <0.7 = slowing, >1.3 = accelerating
            CASE WHEN b.qty_sold_90d > 0 AND (b.qty_sold_90d / 90.0) > 0
                 THEN ROUND(b.qty_sold_30d * 3.0 / b.qty_sold_90d, 2)
                 ELSE NULL
            END as velocity_ratio_30_90,
            -- Annualized gross profit per SKU (revenue × 4 × margin)
            CASE WHEN b.price > 0
                 THEN b.revenue_90d * 4.0 * ((b.price - b.effective_unit_cost) / b.price)
                 ELSE 0
            END as annual_gross_profit,
            -- GMROI annualized: gross profit / cost_basis (avg inventory proxy = current)
            CASE WHEN (b.available * b.effective_unit_cost) > 0 AND b.price > 0
                 THEN b.revenue_90d * 4.0 * ((b.price - b.effective_unit_cost) / b.price)
                      / (b.available * b.effective_unit_cost)
                 ELSE NULL
            END as gmroi
        FROM base b
        """
    )

