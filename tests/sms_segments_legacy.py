"""The SMS audience query as it stood before audience groups — frozen.

Copied verbatim from `core/sql_dialect.py` and `get_sms_segments` on main at
5b4d1c61 (3.0.287), the last commit before an audience could be several groups.
It is the yardstick for one promise: **one group with today's parameters
selects exactly what the audience selected before groups existed**, so every
past campaign, built-in preset and saved preset stays reproducible.

Never edit it to make a test pass. A difference between this and the current
body over a one-group audience is the defect the comparison exists to find.
What it does not freeze is `SmsAudienceFilters.predicate`, which both sides
call: groups added parameters to it and changed nothing it rendered for the
filters that existed before them — a filter set without a period renders the
text and parameters it rendered then, which
`tests/unit/test_sms_audience_groups.py` holds.
"""
from __future__ import annotations

from typing import Any, List, Sequence, Tuple

from core.sql_dialect import TODAY_IN_KYIV, Dialect

LEGACY_SMS_SEGMENTS_BODY = """
            WITH line_items AS (
                SELECT
                    l.order_id,
                    l.buyer_id,
                    l.order_date,
                    l.order_grand_total AS grand_total,
                    l.line_amount AS line_revenue,
                    CASE WHEN os.purchased_price > 0
                         THEN os.purchased_price * l.quantity END AS line_cogs
                FROM {order_lines} l
                LEFT JOIN {offer_stocks} os ON os.sku = l.sku
                WHERE l.buyer_id IS NOT NULL
                  AND NOT l.is_return
                  -- Same revenue definition the Gold layer uses: deprecated
                  -- sources (Opencart et al.) must not inflate LTV or recency.
                  AND l.is_active_source
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
            order_totals AS (
                SELECT buyer_id, order_id, order_date, SUM(revenue) AS order_total
                FROM allocated
                GROUP BY buyer_id, order_id, order_date
            ),
            last_order AS (
                -- What the customer bought last: the hook an SMS is written
                -- around. A ranked subquery rather than QUALIFY, which only
                -- DuckDB has.
                SELECT buyer_id, order_id, order_total
                FROM (
                    SELECT buyer_id, order_id, order_total,
                           ROW_NUMBER() OVER (
                               PARTITION BY buyer_id ORDER BY order_date DESC, order_id DESC
                           ) AS rn
                    FROM order_totals
                ) ranked
                WHERE rn = 1
            ),
            last_order_items AS (
                -- Names run long (up to ~4k chars per order), so keep the three
                -- biggest lines, truncate each, and note how many were left out.
                SELECT
                    lo.buyer_id,
                    lo.order_id AS last_order_id,
                    lo.order_total AS last_order_total,
                    COUNT(*) AS last_order_item_count,
                    array_to_string(ARRAY(
                        SELECT CASE WHEN length(x) > 60
                                    THEN left(x, 57) || chr(8230) ELSE x END
                        FROM unnest(
                            (array_agg(l.product_name ORDER BY l.quantity DESC,
                                       l.product_name))[1:3]
                        ) AS t(x)
                    ), ' | ') AS last_order_items
                FROM last_order lo
                JOIN {order_lines} l ON l.order_id = lo.order_id
                GROUP BY lo.buyer_id, lo.order_id, lo.order_total
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
                    ({today} - MAX(order_date)) AS recency
                FROM allocated
                GROUP BY buyer_id
            ),
            scored AS (
                SELECT
                    c.*,
                    lo.last_order_id,
                    lo.last_order_total,
                    lo.last_order_item_count,
                    lo.last_order_items,
                    b.full_name,
                    b.city,
                    regexp_replace(COALESCE(b.phone, ''), '[^0-9]', '', 'g') AS phone,
                    g.gender,
                    g.confidence AS gender_confidence,
                    {tier_case} AS tier_level
                FROM cust c
                JOIN {buyers} b ON b.id = c.buyer_id
                LEFT JOIN last_order_items lo ON lo.buyer_id = c.buyer_id
                -- LEFT, never JOIN: 1.4% of buyers carry NULL because the
                -- classifier refused, and an inner join would drop them from
                -- every audience — including the ones that do not filter on
                -- gender at all.
                LEFT JOIN {buyer_gender} g ON g.buyer_id = c.buyer_id
                WHERE c.recency <= ?
            ),
            flagged AS (
                -- Each eligibility rule as its own column rather than a WHERE
                -- clause, so the same pass can both filter and report how many
                -- customers each rule removed.
                SELECT
                    *,
                    {arm_expr} AS tier,
                    {ok_tier_expr} AS ok_tier,
                    {filter_sql} AS ok_filters,
                    length(phone) = 12 AND phone LIKE '380%' AS ok_phone,
                    -- Opted out stays out. Matched on buyer AND on phone, because
                    -- the same number can reach us under a second buyer record.
                    NOT EXISTS (
                        SELECT 1 FROM {marketing_optouts} o
                        WHERE o.channel = 'sms'
                          AND (o.buyer_id = scored.buyer_id OR o.phone = scored.phone)
                    ) AS ok_subscribed
                FROM scored
            ),
            eligible AS (
                -- One SMS per phone number: shared numbers across buyer records
                -- would otherwise be messaged twice.
                SELECT * FROM (
                    SELECT f.*,
                           ROW_NUMBER() OVER (
                               PARTITION BY phone ORDER BY {ltv_column} DESC, buyer_id
                           ) AS phone_rn
                    FROM flagged f
                    WHERE ok_filters AND ok_tier AND ok_phone AND ok_subscribed
                ) deduped
                WHERE phone_rn = 1
            ),
            selected AS (
                -- Tier filter applies after de-duplication so asking for a
                -- subset cannot change which buyer wins a shared phone number.
                SELECT * FROM eligible
                {tier_subset}
            ),
            funnel AS (
                -- The selection, stage by stage. Counted in the order the rules
                -- are applied above, so each figure is "still in after this rule".
                SELECT
                    (SELECT COUNT(*) FROM cust) AS f_customers,
                    COUNT(*) AS f_in_window,
                    COUNT(*) FILTER (WHERE ok_filters) AS f_filtered,
                    COUNT(*) FILTER (WHERE ok_filters AND ok_tier) AS f_tiered,
                    COUNT(*) FILTER (WHERE ok_filters AND ok_tier AND ok_phone) AS f_phone,
                    COUNT(*) FILTER (WHERE ok_filters AND ok_tier AND ok_phone AND ok_subscribed)
                        AS f_subscribed,
                    (SELECT COUNT(*) FROM eligible) AS f_eligible
                FROM flagged
            )
            SELECT
                buyer_id, full_name, phone, city, tier, orders,
                ROUND(({ltv_column})::numeric, 2) AS ltv,
                ROUND(({ltv_column} / orders)::numeric, 2) AS aov,
                ROUND((revenue_ltv)::numeric, 2) AS revenue_ltv,
                ROUND((margin_ltv)::numeric, 2) AS margin_ltv,
                ROUND((100.0 * margin_ltv / NULLIF(revenue_ltv, 0))::numeric, 1) AS margin_pct,
                ROUND((100.0 * COALESCE(cost_coverage, 0))::numeric, 1) AS cost_coverage,
                recency, last_order_date, first_order_date,
                last_order_id,
                ROUND((last_order_total)::numeric, 2) AS last_order_total,
                last_order_item_count,
                CASE WHEN last_order_item_count > 3
                     THEN last_order_items || ' +' || (last_order_item_count - 3) || ' ещё'
                     ELSE last_order_items END AS last_order_items,
                f_customers, f_in_window, f_filtered, f_tiered, f_phone,
                f_subscribed, f_eligible
            -- RIGHT JOIN, not CROSS: when nothing survives the filters the
            -- funnel is the only thing left to explain why, so its single row
            -- has to come back regardless.
            FROM selected RIGHT JOIN funnel ON TRUE
            ORDER BY tier, ltv DESC, buyer_id
            """


def legacy_sms_segments_select(
    dialect: Dialect, *, ltv_column: str, sales_type_filter: str, tier_case: str,
    arm_expr: str, ok_tier_expr: str, filter_sql: str, tier_subset: str,
) -> str:
    """`sms_segments_select` as it was."""
    return LEGACY_SMS_SEGMENTS_BODY.format(
        today=TODAY_IN_KYIV,
        order_lines=dialect.order_lines,
        offer_stocks=dialect.offer_stocks,
        buyers=dialect.buyers,
        marketing_optouts=dialect.marketing_optouts,
        buyer_gender=dialect.buyer_gender,
        ltv_column=ltv_column,
        sales_type_filter=sales_type_filter,
        tier_case=tier_case,
        arm_expr=arm_expr,
        ok_tier_expr=ok_tier_expr,
        filter_sql=filter_sql,
        tier_subset=tier_subset,
    )


def legacy_render(
    dialect: Dialect, filters, *, ltv_basis: str = "revenue",
    sales_type: str = "retail", tiers: Sequence[str] = (), grouping: str = "single",
    max_recency_days: int = 270, vip_ltv: float = 10000.0, core_ltv: float = 5000.0,
    core_min_orders: int = 2, reactivation_max_recency: int = 120,
) -> Tuple[str, List[Any]]:
    """The query and parameters `get_sms_segments` built at 5b4d1c61."""
    ltv_column = "revenue_ltv" if ltv_basis == "revenue" else "margin_ltv"
    tier_case = f"""CASE
                    WHEN c.{ltv_column} >= ? THEN 'VIP'
                    WHEN c.orders >= ? OR c.{ltv_column} >= ? THEN 'CORE'
                    WHEN c.recency <= ? THEN 'REACTIVATION'
                END"""
    if grouping == "single":
        arm_expr, ok_tier_expr = "'ALL'", "TRUE"
    else:
        arm_expr, ok_tier_expr = "tier_level", "tier_level IS NOT NULL"
    filter_sql, filter_params = filters.predicate(
        ltv_column, sales_type, dialect.order_lines,
    )
    sql = legacy_sms_segments_select(
        dialect,
        ltv_column=ltv_column,
        sales_type_filter="" if sales_type == "all" else "AND l.sales_type = ?",
        tier_case=tier_case,
        arm_expr=arm_expr,
        ok_tier_expr=ok_tier_expr,
        filter_sql=filter_sql,
        tier_subset=(f"WHERE tier_level IN ({', '.join('?' * len(tiers))})"
                     if tiers else ""),
    )
    params: List[Any] = []
    if sales_type != "all":
        params.append(sales_type)
    params += [vip_ltv, core_min_orders, core_ltv, reactivation_max_recency]
    params.append(max_recency_days)
    params += filter_params
    params += list(tiers)
    return sql, params
