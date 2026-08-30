"""One Silver projection, two engines.

Step 05 ends with Silver computed inside Postgres. The naive way to get there
is to write the projection a second time in Postgres dialect, and that is
exactly the mistake charter rule 1 exists to prevent: a rule with two homes is
a rule that will differ. The shortest recorded time it took to differ in this
codebase is **under a day** — #101 updated `sales_type` in one copy of the
Silver projection and not the other.

The good news, found by reading rather than assumed: the projection is almost
entirely ordinary SQL. Between DuckDB and PostgreSQL it differs in exactly
**two** places.

    the Kyiv date   DATE(timezone('Europe/Kyiv', x))     DuckDB
                    (timezone('Europe/Kyiv', x))::date   PostgreSQL

    table names     orders                    bronze.orders
                    managers                  bronze.managers
                    manager_classifications   app.manager_classifications

Everything else — the CASE, the EXISTS subqueries, the status tuple, the
boolean expressions — is identical text. So the projection stays in one place
and takes a dialect.

`timezone(zone, timestamptz)` means the same thing in both engines, which is
the one thing here worth doubting and was checked against production: 1,007
orders have a Kyiv date different from their UTC one, and the two stores agree
on every one of them.

WHY A DIALECT OBJECT AND NOT AN `if postgres:`

Because the difference is data, and writing it as data makes it testable. A
test renders both and asserts they are the same string once the two known
substitutions are undone — which proves there is no *third* divergence, the
thing a branchy implementation could hide indefinitely.
"""
from __future__ import annotations

from dataclasses import dataclass

from core.duckdb_constants import DISPLAY_TIMEZONE


@dataclass(frozen=True)
class Dialect:
    """Where the Silver projection's tables live, and how it reads a date."""

    name: str
    orders: str
    managers: str
    classifications: str
    silver_orders: str
    # The order-lines level joins three more tables. Same rule as above: the
    # names are the *only* thing allowed to differ between the two renderings.
    order_products: str
    products: str
    categories: str
    # The SMS audience reads four more. `order_lines` is the *rendered* line
    # level — a view in DuckDB, a view in Postgres (revision 0011) — so the
    # audience query selects from a name in both engines and never re-derives
    # it. `offer_stocks` carries the cost side of margin, `marketing_optouts`
    # the promise not to message somebody.
    order_lines: str
    buyers: str
    offer_stocks: str
    marketing_optouts: str
    # A format template with one `{column}` hole. Not a function, so the whole
    # dialect stays comparable, printable and trivially frozen.
    date_template: str

    def kyiv_date(self, column: str) -> str:
        """The order's calendar date in the timezone the business reads."""
        return self.date_template.format(column=column, zone=DISPLAY_TIMEZONE)


DUCKDB = Dialect(
    name="duckdb",
    orders="orders",
    managers="managers",
    classifications="manager_classifications",
    silver_orders="silver_orders",
    order_products="order_products",
    products="products",
    categories="categories",
    order_lines="silver_order_lines",
    buyers="buyers",
    offer_stocks="offer_stocks",
    marketing_optouts="marketing_optouts",
    # Byte-for-byte what `core.duckdb_constants._date_in_kyiv` has always
    # emitted. Changing it here changes stored Silver on the next rebuild.
    date_template="DATE(timezone('{zone}', {column}))",
)

POSTGRES = Dialect(
    name="postgres",
    # `bronze` is landing from KeyCRM; `app` is what nothing can decide again.
    # `postgres/initdb/30-app.sql` draws that line — see revision 0005 for why
    # the classification sits on the other side of it from the managers.
    orders="bronze.orders",
    managers="bronze.managers",
    classifications="app.manager_classifications",
    silver_orders="silver.orders",
    order_products="bronze.order_products",
    products="bronze.products",
    categories="bronze.categories",
    order_lines="silver.order_lines",
    buyers="bronze.buyers",
    offer_stocks="bronze.offer_stocks",
    marketing_optouts="app.marketing_optouts",
    # `DATE(x)` also exists in PostgreSQL, but the cast is what the rest of
    # this repository's Postgres SQL uses, so it reads the same as its
    # neighbours in `core/reconciliation_io.py`.
    date_template="(timezone('{zone}', {column}))::date",
)


# ─── The order-lines level, one body for two engines ────────────────────────
#
# Step 2 of «Одна бронза» needs the line level in Postgres, and the line level
# already exists in DuckDB as a view. Same charter-rule-1 move as the Silver
# projection above: one body, the table names as the only difference. DuckDB
# renders it into `SILVER_ORDER_LINES_VIEW_SQL`; migration 0011 froze the
# Postgres rendering as `silver.order_lines`, and a test re-renders this
# template and asserts the two agree — the same contract revision 0010 has
# with `core/pg_order_versions.py`.

_ORDER_LINES_BODY = """SELECT
    op.id                                        AS line_id,
    op.order_id,
    op.product_id,
    op.name                                      AS product_name,  -- as sold
    op.quantity,
    op.price_sold,
    CAST(op.quantity * op.price_sold AS DECIMAL(14, 2)) AS line_amount,
    -- the order, denormalised: every predicate a page applies to
    -- revenue applies here too, and re-deriving them was the bug
    s.order_date,
    s.ordered_at,
    s.sales_type,
    s.source_id,
    s.source_name,
    s.is_return,
    s.is_active_source,
    s.buyer_id,
    s.manager_id,
    s.is_new_customer,
    s.buyer_first_order_date,
    s.promocode,
    s.grand_total                                AS order_grand_total,
    -- the catalog, denormalised. 8.2 % of lines carry no product_id and
    -- 31 % no category; both stay NULL rather than being dropped, which
    -- is the difference between a level and a filter.
    p.name                                       AS catalog_product_name,
    p.brand,
    p.sku,
    p.category_id,
    c.name                                       AS category_name,
    c.parent_id                                  AS parent_category_id,
    parent_c.name                                AS parent_category_name
FROM {order_products} op
JOIN {silver_orders} s ON s.id = op.order_id
LEFT JOIN {products} p ON p.id = op.product_id
LEFT JOIN {categories} c ON c.id = p.category_id
LEFT JOIN {categories} parent_c ON parent_c.id = c.parent_id"""


def order_lines_select(dialect: Dialect) -> str:
    """The order-lines SELECT, rendered for one engine."""
    return _ORDER_LINES_BODY.format(
        order_products=dialect.order_products,
        silver_orders=dialect.silver_orders,
        products=dialect.products,
        categories=dialect.categories,
    )


# ─── The SMS audience, one body for two engines ─────────────────────────────
#
# The segmentation was DuckDB-only text, and moving `/sms` off DuckDB meant
# either writing it a second time in Postgres dialect — charter rule 1's exact
# prohibition, and the projection above records that a duplicated rule here
# diverged in under a day — or finding a text both engines accept.
#
# Both engines accept one, and that was measured rather than hoped for. Six
# constructs looked like they would force a dialect, and every one of them has
# a portable spelling that DuckDB and PostgreSQL agree on:
#
#   QUALIFY rn = 1          -> a subquery with WHERE rn = 1
#   list_slice/list_transform -> ARRAY(SELECT … FROM unnest(agg[1:3]))
#   DATEDIFF('day', a, b)   -> (b - a)
#   FILTER (expr)           -> FILTER (WHERE expr)
#   ROUND(x, 2)             -> ROUND(x::numeric, 2)
#   RIGHT JOIN … ON TRUE    -> identical already
#
# The array one is the one worth doubting, because it nests an aggregate inside
# a scalar subquery under a GROUP BY. Checked on DuckDB 1.5.5 and PostgreSQL
# 17.2 against the same rows, including a name long enough to truncate: the two
# produce **byte-identical** strings, U+2026 included.
#
# So the table names are again the only difference, and a test renders both and
# asserts they are the same string once the names are undone — which is what
# proves there is no seventh divergence hiding.
_SMS_SEGMENTS_BODY = """
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
                    (CURRENT_DATE - MAX(order_date)) AS recency
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
                    {tier_case} AS tier_level
                FROM cust c
                JOIN {buyers} b ON b.id = c.buyer_id
                LEFT JOIN last_order_items lo ON lo.buyer_id = c.buyer_id
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


def sms_segments_select(
    dialect: Dialect,
    *,
    ltv_column: str,
    sales_type_filter: str,
    tier_case: str,
    arm_expr: str,
    ok_tier_expr: str,
    filter_sql: str,
    tier_subset: str,
) -> str:
    """The SMS audience query, rendered for one engine.

    Everything but `dialect` is a fragment the caller built from validated
    input — the level thresholds, the arm expression, the audience predicate.
    None of them carries user text: values travel as bound parameters, which is
    why the placeholders stay `?` here and are renumbered per driver.
    """
    return _SMS_SEGMENTS_BODY.format(
        order_lines=dialect.order_lines,
        offer_stocks=dialect.offer_stocks,
        buyers=dialect.buyers,
        marketing_optouts=dialect.marketing_optouts,
        ltv_column=ltv_column,
        sales_type_filter=sales_type_filter,
        tier_case=tier_case,
        arm_expr=arm_expr,
        ok_tier_expr=ok_tier_expr,
        filter_sql=filter_sql,
        tier_subset=tier_subset,
    )
