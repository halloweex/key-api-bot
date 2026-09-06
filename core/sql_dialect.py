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
    # The SMS tab's own state (revision 0013). Operational, and irreplaceable
    # in `stock_movements`' sense — a frozen roster cannot be recomputed.
    sms_campaigns: str
    sms_campaign_members: str
    sms_audience_presets: str
    sms_dlr_events: str
    # The `/inventory` layer. `sku_inventory_status` is replicated, not
    # mirrored — it is derived from state only DuckDB holds — which is why it
    # sits in `app` rather than `bronze` (revision 0008).
    sku_inventory_status: str
    # The daily stock snapshot behind the trend chart. Replicated, not
    # mirrored, and in `app` for `sku_inventory_status`' reason: the API
    # serves current stock only, so a day the snapshot job did not run is a
    # day that cannot be recovered from KeyCRM (revision 0008).
    inventory_history: str
    # `/inventory`'s turnover KPIs read the revenue Gold directly, and this is
    # the one place the two Golds are not the same shape. DuckDB grains on
    # (date, sales_type) and splits channels into columns; Postgres grains on
    # (date, sales_type, source_id) with `source_id IS NULL` as the roll-up.
    # So a bare `SUM(revenue)` reads correctly in DuckDB and **doubles** in
    # Postgres — measured on production over 30 days: 11,107,040.50 against a
    # true 5,553,520.25. The predicate is a hole rather than a name because
    # that is exactly what differs.
    gold_daily_revenue: str
    gold_revenue_rollup: str
    # A namespace, not a name: the eleven inventory views reference each other,
    # so one prefix does the work of eleven holes. Empty in DuckDB, which has
    # no schemas to speak of; `gold.` in Postgres.
    inventory_views: str
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
    sms_campaigns="sms_campaigns",
    sms_campaign_members="sms_campaign_members",
    sms_audience_presets="sms_audience_presets",
    sms_dlr_events="sms_dlr_events",
    sku_inventory_status="sku_inventory_status",
    inventory_history="inventory_history",
    gold_daily_revenue="gold_daily_revenue",
    # Every row is a roll-up here: this Gold has no source dimension.
    gold_revenue_rollup="TRUE",
    inventory_views="",
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
    sms_campaigns="app.sms_campaigns",
    sms_campaign_members="app.sms_campaign_members",
    sms_audience_presets="app.sms_audience_presets",
    sms_dlr_events="app.sms_dlr_events",
    sku_inventory_status="app.sku_inventory_status",
    inventory_history="app.inventory_history",
    gold_daily_revenue="gold.daily_revenue",
    gold_revenue_rollup="source_id IS NULL",
    inventory_views="gold.",
    # `DATE(x)` also exists in PostgreSQL, but the cast is what the rest of
    # this repository's Postgres SQL uses, so it reads the same as its
    # neighbours in `core/reconciliation_io.py`.
    date_template="(timezone('{zone}', {column}))::date",
)




# ─── The day the business is in ─────────────────────────────────────────────
#
# `CURRENT_DATE` is not the same day in both engines, and the difference is
# invisible for twenty-one hours out of twenty-four. The production `web`
# container runs `TZ=Europe/Kyiv`, so DuckDB's `CURRENT_DATE` is Kyiv's day;
# the Postgres server answers in UTC. Measured on production, 2026-08-31:
# DuckDB `Europe/Kyiv`, Postgres `UTC`. **Between 21:00 and midnight UTC they
# name different days**, and every `days_since_sale`, every aging bucket and
# every 30/90-day window moves by one on whichever engine is answering.
#
# A differential test cannot find this: under the gate both engines sit in the
# same timezone, so both are wrong together. It was found by asking the two
# production containers what day it was.
#
# It is **not** a dialect hole, because both engines accept this expression and
# mean the same thing by it — checked on DuckDB 1.5.5 and PostgreSQL 17.2,
# including from a session pinned to UTC+14, where `CURRENT_DATE` is already
# tomorrow and this is not. So the body keeps one text, and the answer stops
# depending on an environment variable in a container.
TODAY_IN_KYIV = f"(now() AT TIME ZONE '{DISPLAY_TIMEZONE}')::date"

# ─── How each driver spells a bound parameter ───────────────────────────────
#
# The shared bodies keep DuckDB's `?` and are renumbered on their way to
# asyncpg. That is a property of the drivers, not of any one query, so it
# lives beside the dialects rather than inside the first module that needed
# it — `/sms` wrote it, `/inventory` is the second caller, and an import of
# the SMS module from the inventory one would be a dependency that means
# nothing.

def numbered(sql: str) -> str:
    """`?` placeholders rewritten as `$1 … $n` for asyncpg.

    Both drivers bind positionally, so the order the caller assembled the
    parameters in is the order they bind in — there is no mapping to keep in
    step, which is the whole reason the shared body keeps `?` rather than
    growing a per-driver placeholder hole.

    **Comments and string literals are skipped, and that is not tidiness.** A
    plain scan reads a `?` inside a `--` comment as a placeholder, consumes a
    number for it, and shifts every real parameter after it by one — silently,
    into a query that still runs. It cost a debugging round here: a comment
    added to explain a cast contained the words `? + INTERVAL`, and the query
    then bound the attribution window to the wrong argument. Nothing about the
    failure pointed at the comment.

    The same hazard in the other direction is why literals are skipped too,
    though the bodies here bind every value rather than interpolating any.
    """
    out: List[str] = []
    n = 0
    in_line_comment = False
    in_literal = False
    i = 0
    while i < len(sql):
        char = sql[i]
        if in_line_comment:
            if char == "\n":
                in_line_comment = False
            out.append(char)
        elif in_literal:
            if char == "'":
                in_literal = False
            out.append(char)
        elif char == "-" and sql[i:i + 2] == "--":
            in_line_comment = True
            out.append(char)
        elif char == "'":
            in_literal = True
            out.append(char)
        elif char == "?":
            n += 1
            out.append(f"${n}")
        else:
            out.append(char)
        i += 1
    return "".join(out)

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
        today=TODAY_IN_KYIV,
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


# ─── The customer-analytics bodies, DuckDB and ClickHouse ───────────────────
#
# A second, smaller dialect, and the smallness is the point. The `Dialect`
# above carries eight table names because the Silver projection and the SMS
# audience touch eight tables; the cohort queries touch exactly one, and a
# ClickHouse rendering has no answer for `bronze.buyers` or
# `app.marketing_optouts` — those stores do not exist there and never will.
# Reusing the big dialect would mean inventing names for tables the engine
# cannot be asked about.
#
# `today` is a hole for **two** reasons, and the second was found the hard way.
#
# Syntax first: `CURRENT_DATE` has no spelling all three engines accept —
# measured, not assumed.
#
#     bare CURRENT_DATE     DuckDB ok   PostgreSQL ok   ClickHouse rejects
#     CURRENT_DATE()        DuckDB ok   PostgreSQL rejects   ClickHouse ok
#
# PostgreSQL treats it as a reserved keyword and refuses the parentheses;
# ClickHouse has no bare keyword and refuses their absence.
#
# **And meaning second: neither keyword names the same day on both engines.**
# The `keycrm-web` container runs `TZ=Europe/Kyiv`, so DuckDB's day is Kyiv's;
# `ks-clickhouse` answers in UTC. Measured 2026-08-31. Between 21:00 and
# midnight UTC the two are on different dates, which moved `days_since_last` in
# the at-risk query by a day for three hours every night, and — because the
# cohort windows are month-truncated — moved four cohort windows by a whole
# *month* during the last three hours of a month. `KS_READ_COHORTS=clickhouse`
# had been live for a day when this was found.
#
# A differential test could not have caught it: under the gate both engines sit
# in one timezone and are wrong together. It was found by asking the two
# production containers what day it was, while porting `/inventory`.
#
# So both renderings now name the timezone rather than inheriting one, and the
# hole stays only because the *spelling* still differs. Verified by execution
# on ClickHouse 24.8.14.39 — production's version — that `toTimeZone` survives
# `DATE_TRUNC`, `DATEDIFF` and `- INTERVAL n month` unchanged.
@dataclass(frozen=True)
class AnalyticsDialect:
    """Where the order facts live for one engine, and how it says "today"."""

    name: str
    silver_orders: str
    today: str


DUCKDB_ANALYTICS = AnalyticsDialect(
    name="duckdb", silver_orders="silver_orders", today=TODAY_IN_KYIV,
)

CLICKHOUSE_ANALYTICS = AnalyticsDialect(
    # `core/ch_silver.py` ships Silver under the same name Postgres uses.
    name="clickhouse", silver_orders="silver.orders",
    today=f"toDate(toTimeZone(now(), '{DISPLAY_TIMEZONE}'))",
)


# Everything else in this body was run on DuckDB 1.5.5 and ClickHouse
# 24.8.14.39 against the same rows: `DATE_TRUNC`, `DATEDIFF`, `median`,
# `FILTER (WHERE …)`, CTEs, `COUNT(DISTINCT …)` and `ROUND` all agree, and
# `substring(CAST(x AS VARCHAR), 1, 7)` returns the same month label on both —
# which is why it stands where DuckDB's own `strftime` used to.
_COHORT_RETENTION_BODY = """
            WITH customer_cohorts AS (
                -- Each customer's first order month is their cohort.
                SELECT
                    o.buyer_id,
                    DATE_TRUNC('month', MIN(o.order_date)) AS cohort_month
                FROM {silver_orders} o
                WHERE o.buyer_id IS NOT NULL
                  AND NOT o.is_return
                  {sales_type_filter}
                GROUP BY o.buyer_id
            ),
            customer_orders AS (
                SELECT DISTINCT
                    o.buyer_id,
                    c.cohort_month,
                    DATEDIFF('month', c.cohort_month,
                             DATE_TRUNC('month', o.order_date)) AS months_since
                FROM {silver_orders} o
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
                substring(CAST(r.cohort_month AS VARCHAR), 1, 7) AS cohort,
                s.size AS cohort_size,
                r.months_since AS month_number,
                r.retained_customers,
                ROUND(100.0 * r.retained_customers / s.size, 1) AS retention_pct
            FROM retention_data r
            JOIN cohort_sizes s ON r.cohort_month = s.cohort_month
            WHERE r.cohort_month
                  >= DATE_TRUNC('month', {today}) - INTERVAL '{months_back} months'
            ORDER BY r.cohort_month DESC, r.months_since
"""


def cohort_retention_select(
    dialect: AnalyticsDialect, *, sales_type_filter: str, months_back: int,
) -> str:
    """The retention matrix, rendered for one engine.

    `months_back` is interpolated rather than bound because an interval cannot
    be parameterised in either engine; it is an int by construction and never
    user text. The retention horizon *is* bound, as the single `?`.
    """
    return _COHORT_RETENTION_BODY.format(
        silver_orders=dialect.silver_orders,
        today=dialect.today,
        sales_type_filter=sales_type_filter,
        months_back=int(months_back),
    )


# ─── The other four analytics bodies ────────────────────────────────────────
#
# Same two holes as the retention body, same reason, and the same six
# constructs verified on DuckDB 1.5.5 and ClickHouse 24.8.14.39.

_ENHANCED_COHORT_RETENTION_SELECT_BODY = """            WITH customer_first_order AS (
                -- Get each customer's first order month (cohort)
                SELECT
                    o.buyer_id,
                    DATE_TRUNC('month', MIN(o.order_date)) AS cohort_month
                FROM {silver_orders} o
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
                LEFT JOIN {silver_orders} o ON c.buyer_id = o.buyer_id
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
                FROM {silver_orders} o
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
                substring(CAST(r.cohort_month AS VARCHAR), 1, 7) as cohort,
                s.size as cohort_size,
                s.m0_revenue,
                r.months_since as month_number,
                r.retained_customers,
                ROUND(100.0 * r.retained_customers / s.size, 1) as retention_pct,
                r.period_revenue,
                ROUND(100.0 * r.period_revenue / NULLIF(s.m0_revenue, 0), 1) as revenue_retention_pct
            FROM retention_data r
            JOIN cohort_sizes s ON r.cohort_month = s.cohort_month
            WHERE r.cohort_month >= DATE_TRUNC('month', {today}) - INTERVAL '{months_back} months'
            ORDER BY r.cohort_month DESC, r.months_since
"""


def enhanced_cohort_retention_select(
    dialect: AnalyticsDialect, *, sales_type_filter: str, months_back: int,
) -> str:
    """The retention matrix with revenue and order counts per cell.

    `months_back` is interpolated because an interval cannot be bound in
    either engine; it is an int by construction and never user text.
    """
    return _ENHANCED_COHORT_RETENTION_SELECT_BODY.format(
        silver_orders=dialect.silver_orders,
        today=dialect.today,
        sales_type_filter=sales_type_filter,
        months_back=int(months_back),
    )

_DAYS_TO_SECOND_PURCHASE_SELECT_BODY = """            WITH customer_orders_ranked AS (
                SELECT
                    o.buyer_id,
                    o.order_date,
                    ROW_NUMBER() OVER (PARTITION BY o.buyer_id ORDER BY o.order_date) AS order_num
                FROM {silver_orders} o
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
                WHERE c1.order_date >= {today} - INTERVAL '{months_back} months'
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
                    median(days_to_second) AS median_days,
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


def days_to_second_purchase_select(
    dialect: AnalyticsDialect, *, sales_type_filter: str, months_back: int,
) -> str:
    """How long the second purchase takes, bucketed, with the median.

    `months_back` is interpolated because an interval cannot be bound in
    either engine; it is an int by construction and never user text.
    """
    return _DAYS_TO_SECOND_PURCHASE_SELECT_BODY.format(
        silver_orders=dialect.silver_orders,
        today=dialect.today,
        sales_type_filter=sales_type_filter,
        months_back=int(months_back),
    )

_COHORT_LTV_SELECT_BODY = """            WITH customer_cohorts AS (
                SELECT
                    o.buyer_id,
                    DATE_TRUNC('month', MIN(o.order_date)) AS cohort_month
                FROM {silver_orders} o
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
                FROM {silver_orders} o
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
                substring(CAST(cm.cohort_month AS VARCHAR), 1, 7) AS cohort,
                cs.cohort_size,
                cm.months_since,
                cm.total_revenue,
                cm.active_customers
            FROM cohort_monthly cm
            JOIN cohort_sizes cs ON cm.cohort_month = cs.cohort_month
            WHERE cm.cohort_month >= DATE_TRUNC('month', {today}) - INTERVAL '{months_back} months'
            ORDER BY cm.cohort_month DESC, cm.months_since
"""


def cohort_ltv_select(
    dialect: AnalyticsDialect, *, sales_type_filter: str, months_back: int,
) -> str:
    """Cumulative lifetime value per cohort month.

    `months_back` is interpolated because an interval cannot be bound in
    either engine; it is an int by construction and never user text.
    """
    return _COHORT_LTV_SELECT_BODY.format(
        silver_orders=dialect.silver_orders,
        today=dialect.today,
        sales_type_filter=sales_type_filter,
        months_back=int(months_back),
    )

_AT_RISK_CUSTOMERS_SELECT_BODY = """            WITH customer_activity AS (
                SELECT
                    o.buyer_id,
                    DATE_TRUNC('month', MIN(o.order_date)) AS cohort_month,
                    MAX(o.order_date) AS last_order_date,
                    DATEDIFF('day', MAX(o.order_date), {today}) AS days_since_last,
                    COUNT(*) AS total_orders,
                    SUM(o.grand_total) AS total_revenue
                FROM {silver_orders} o
                WHERE o.buyer_id IS NOT NULL
                  AND NOT o.is_return
                  {sales_type_filter}
                GROUP BY o.buyer_id
            )
            SELECT
                substring(CAST(cohort_month AS VARCHAR), 1, 7) AS cohort,
                COUNT(*) AS total_customers,
                -- `COUNT(*) FILTER (WHERE …)` rather than the CASE below is
                -- what this said, and ClickHouse 24.8 rejects that spelling
                -- when the aggregate reads a CTE: it parses the FILTER as a
                -- second argument and complains that COUNT takes one. The
                -- same expression against a subquery is accepted, which is
                -- why this needed executing rather than reading. The CASE
                -- form is plain SQL and means the same on all three engines.
                COUNT(CASE WHEN days_since_last > ? AND days_since_last <= ?
                           THEN 1 END) AS at_risk_count,
                ROUND(100.0 * COUNT(CASE WHEN days_since_last > ? THEN 1 END)
                      / COUNT(*), 1) AS at_risk_pct,
                SUM(CASE WHEN days_since_last > ? THEN total_revenue END)
                    AS at_risk_revenue,
                AVG(CASE WHEN days_since_last > ? THEN total_orders END)
                    AS avg_orders_at_risk,
                COUNT(CASE WHEN days_since_last > ? THEN 1 END) AS churned_count
            FROM customer_activity
            WHERE cohort_month >= DATE_TRUNC('month', {today}) - INTERVAL '{months_back} months'
            GROUP BY cohort_month
            ORDER BY cohort_month DESC
"""


def at_risk_customers_select(
    dialect: AnalyticsDialect, *, sales_type_filter: str, months_back: int,
) -> str:
    """Customers whose recency has drifted past their cohort's habit.

    `months_back` is interpolated because an interval cannot be bound in
    either engine; it is an int by construction and never user text.
    """
    return _AT_RISK_CUSTOMERS_SELECT_BODY.format(
        silver_orders=dialect.silver_orders,
        today=dialect.today,
        sales_type_filter=sales_type_filter,
        months_back=int(months_back),
    )


# ─── What each body returns, once ───────────────────────────────────────────
#
# The column types live beside the body they describe, because they *are* part
# of it: ClickHouse answers in TabSeparated and hands every value back as text,
# so the reader needs this list to give the caller the shapes DuckDB would have
# returned. Keeping them here rather than at the call site is not tidiness —
# they were briefly in two places, the repository's copy said seven columns
# where the enhanced matrix has eight and six where the at-risk projection has
# seven, and the differential test could not see it because it carried a third
# copy of its own.
COHORT_RETENTION_TYPES: Tuple[str, ...] = (
    "text",   # cohort, 'YYYY-MM'
    "int",    # cohort_size
    "int",    # month_number
    "int",    # retained_customers
    "float",  # retention_pct
)

ENHANCED_RETENTION_TYPES: Tuple[str, ...] = (
    "text", "int", "float", "int", "int", "float", "float", "float",
)

DAYS_TO_SECOND_TYPES: Tuple[str, ...] = (
    "text",   # bucket label
    "int",    # customers
    "float",  # avg_days
    "float",  # median_days
    "float",  # avg_days_overall
    "int",    # total_count
)

COHORT_LTV_TYPES: Tuple[str, ...] = (
    "text",   # cohort
    "int",    # cohort_size
    "int",    # months_since
    "float",  # total_revenue
    "int",    # active_customers
)

AT_RISK_TYPES: Tuple[str, ...] = (
    "text",   # cohort
    "int",    # total_customers
    "int",    # at_risk_count
    "float",  # at_risk_pct
    "float",  # at_risk_revenue — NULL until a cohort has anyone at risk
    "float",  # avg_orders_at_risk — likewise
    "int",    # churned_count
)


# ─── The inventory analytics layer, one body for two engines ────────────────
#
# The `/inventory` tab reads no table directly. It reads eleven views, rooted
# on `v_sku_analysis`, and eight repository methods are their only consumers.
# Moving the tab to Postgres therefore means moving the views, and charter
# rule 1 applies here exactly as it did to the Silver projection and the SMS
# audience: one body, the table names the only difference.
#
# WHAT HAD TO CHANGE, AND WHY IT IS NOT A REWRITE
#
# Four of the eleven read `gold_daily_products`, which exists in DuckDB and has
# no Postgres counterpart. The reconnaissance that opened this work recorded
# the tab's sources as two tables, both already mirrored; that was wrong, and
# seven of the eight methods depend on the table it missed.
#
# Deriving `gold.daily_products` in Postgres was the obvious repair and is the
# wrong size. The views ask that 89,466-row table for one narrow thing: per
# `product_id`, over 30 and 90 days, quantity and revenue. The order-lines
# level already carries it in both engines, so the six subqueries read
# `{order_lines}` and re-apply Gold's own predicate (`NOT is_return AND
# is_active_source`) — which is what Gold applied when it materialised those
# rows in the first place.
#
# **The numbers do not move, and that is measured rather than argued.** On the
# production backup, the rollup from the line level and the rollup from
# `gold_daily_products` agree on every product in both windows:
#
#     30d   0 differing on quantity, 0 on revenue, 0 on order_count
#     90d   0 differing on quantity, 0 on revenue, 5 on order_count (max 9)
#
# Quantity and revenue are exact because nothing rounds on either path:
# `price_sold` is DECIMAL(12,2) and `quantity` is an integer, so the products
# and their sums stay exact at two decimal places in both engines.
#
# `order_count` is the one measure that differs, it differs by construction,
# and **nothing reads it**. Gold stores `COUNT(DISTINCT order_id)` per
# (date, sales_type, source_id, product) cell and the view sums those cells,
# so an order carrying two lines of the same product under different sold-names
# is counted twice on that day. `COUNT(DISTINCT order_id)` over the window is
# the answer that was wanted. It surfaces as `orders_30d` on
# `v_sku_sell_through`, and a search of the repository finds no consumer — not
# a method, not a route, not the frontend.
#
# WHY A NAMESPACE HOLE AND NOT ELEVEN NAME HOLES
#
# The views reference each other, so their own names need a hole too. One
# prefix hole (`{views}` → `` in DuckDB, `gold.` in Postgres) keeps the count
# at one instead of eleven, and keeps the test that undoes every substitution
# able to prove there is no twelfth divergence.
#
# WHY SCHEMA `gold` IN POSTGRES
#
# This is a derived reading layer over Silver and the replicated stock tables —
# the same kind of thing `gold.daily_revenue` is. `app` is where what nothing
# can decide again lives, and none of these eleven holds a fact: drop them all
# and one migration puts them back.
#
# ONE DIVISION, NOT TWO — AND THIS IS NOT TIDINESS
#
# `available / (qty_sold_90d / 90.0)` divides twice, and the engines part
# company on the second one. Measured, on 40 units against 20 sold in 90 days,
# which is exactly 180 days of supply:
#
#     DuckDB      20 / 90.0 -> DOUBLE 0.2222222222222222
#                 40 / that -> 180.0                     -> not > 180 -> warm
#     PostgreSQL  20 / 90.0 -> NUMERIC 0.22222222222222222222 (truncated)
#                 40 / that -> 180.00000000000000000180    ->     > 180 -> cold
#
# A SKU sitting on a tier boundary was classified differently by the two
# engines, and that flows into `velocity_tier`, the NPV decision, the quadrant
# matrix and the brand scorecard's frozen share. `available * 90.0 / qty` is
# the same number with one division, and both engines then agree.
#
# It also corrects DuckDB. On the production backup the rewrite moves exactly
# two values, both `velocity_ratio_30_90`: 10 sold in 30 days against 16 in 90
# is 1.875 exactly, and the double division returned 1.8749999999999998, which
# rounded to 1.87 instead of 1.88.
#
# ORDER IS LOAD-BEARING. `v_sku_analysis` is the root; `v_sku_status` needs
# `v_category_velocity`; `v_sku_dead_stock_v2` needs `v_abc_classification`.
# They are created in the order below and no other.
_INVENTORY_VIEWS: tuple[tuple[str, str], ...] = (
    # ── Layer 3: analytics ──────────────────────────────────────────────
    # The root. Current SKU state with the calculated fields everything
    # downstream reads; the only view that touches the stock table directly.
    (
        "v_sku_analysis",
        """SELECT
    s.*,
    c.name as category_name,
    s.quantity - s.reserve as available,
    s.quantity * s.price as stock_value,
    (s.quantity - s.reserve) * s.price as available_value,
    {today} - s.last_sale_date as days_since_sale,
    {today} - s.first_seen_at as days_in_stock
FROM {sku_inventory_status} s
LEFT JOIN {categories} c ON s.category_id = c.id""",
    ),
    # Category velocity, which is what makes the dead-stock threshold
    # dynamic instead of one number for a catalogue selling both cosmetics
    # and appliances.
    (
        "v_category_velocity",
        """SELECT
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
FROM {views}v_sku_analysis
WHERE last_sale_date IS NOT NULL AND quantity > 0
GROUP BY category_id, category_name
HAVING COUNT(*) >= 5""",
    ),
    # Dead stock and overstock classification. `days_of_supply` uses the
    # 90-day velocity rather than the 30-day one: more stable, and a single
    # promo week must not reclassify half the catalogue.
    (
        "v_sku_status",
        """SELECT
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
FROM {views}v_sku_analysis s
LEFT JOIN {views}v_category_velocity cv ON s.category_id = cv.category_id
LEFT JOIN (
    SELECT product_id, SUM(quantity) as qty_sold_90d
    FROM {order_lines}
    WHERE NOT is_return AND is_active_source
      AND order_date >= {today} - INTERVAL '90 days'
    GROUP BY product_id
) vel ON s.product_id = vel.product_id
WHERE s.quantity > 0""",
    ),
    # Summary by status.
    (
        "v_inventory_summary",
        """SELECT
    status,
    COUNT(*) as sku_count,
    SUM(available) as total_units,
    SUM(available_value) as total_value,
    ROUND(100.0 * SUM(available_value) /
        NULLIF(SUM(SUM(available_value)) OVER (), 0), 1) as value_pct
FROM {views}v_sku_status
GROUP BY status""",
    ),
    # Aging buckets.
    (
        "v_aging_buckets",
        """SELECT
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
FROM {views}v_sku_analysis
WHERE quantity > 0
GROUP BY bucket
ORDER BY bucket""",
    ),
    # ── Layer 3b: turnover and ABC ──────────────────────────────────────
    # Per-SKU sell-through: inventory joined to what actually sold. Read the
    # sales side from the order-lines level, not from `gold_daily_products` —
    # see the note at the top of this section for why, and for the
    # measurement that says the numbers are the same.
    (
        "v_sku_sell_through",
        """SELECT
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
FROM {views}v_sku_analysis s
LEFT JOIN (
    SELECT product_id,
           SUM(quantity) as qty_sold_30d,
           SUM(quantity * price_sold) as revenue_30d,
           COUNT(DISTINCT order_id) as orders_30d
    FROM {order_lines}
    WHERE NOT is_return AND is_active_source
      AND order_date >= {today} - INTERVAL '30 days'
    GROUP BY product_id
) g30 ON s.product_id = g30.product_id
LEFT JOIN (
    SELECT product_id,
           SUM(quantity) as qty_sold_90d,
           SUM(quantity * price_sold) as revenue_90d
    FROM {order_lines}
    WHERE NOT is_return AND is_active_source
      AND order_date >= {today} - INTERVAL '90 days'
    GROUP BY product_id
) g90 ON s.product_id = g90.product_id
WHERE s.quantity > 0""",
    ),
    # ABC classification by cumulative revenue (Pareto).
    (
        "v_abc_classification",
        """WITH product_revenue AS (
    SELECT
        product_id,
        SUM(quantity * price_sold) as total_revenue,
        SUM(quantity) as total_qty_sold
    FROM {order_lines}
    WHERE NOT is_return AND is_active_source
      AND order_date >= {today} - INTERVAL '90 days'
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
    FROM {views}v_sku_analysis s
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
FROM ranked""",
    ),
    # ABC summary, aggregated per class.
    (
        "v_abc_summary",
        """SELECT
    abc_class,
    COUNT(*) as sku_count,
    SUM(available) as total_units,
    SUM(available_value) as stock_value,
    SUM(revenue_90d) as revenue,
    ROUND(100.0 * SUM(available_value) /
        NULLIF(SUM(SUM(available_value)) OVER (), 0), 1) as stock_value_pct,
    ROUND(100.0 * SUM(revenue_90d) /
        NULLIF(SUM(SUM(revenue_90d)) OVER (), 0), 1) as revenue_pct
FROM {views}v_abc_classification
GROUP BY abc_class
ORDER BY abc_class""",
    ),
    # ── Layer 4: actions ────────────────────────────────────────────────
    # Actionable recommendations.
    (
        "v_recommended_actions",
        """SELECT
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
FROM {views}v_sku_status
WHERE status != 'healthy'
ORDER BY available_value DESC""",
    ),
    # Low stock alerts.
    (
        "v_restock_alerts",
        """SELECT
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
FROM {views}v_sku_analysis
WHERE available <= 10
  AND (days_since_sale IS NULL OR days_since_sale <= 90)
ORDER BY available ASC""",
    ),
    # ── Layer 5: dead stock v2 ──────────────────────────────────────────
    # Cost-basis ranking, velocity tiers, ABC and GMROI inputs. The NPV
    # decision itself stays in Python, so the carrying rate and the
    # liquidation discount can be tuned without rebuilding the view.
    (
        "v_sku_dead_stock_v2",
        """WITH cost_ratio AS (
    -- Portfolio-wide cost-to-sale ratio for fallback when purchased_price is missing
    SELECT
        COALESCE(
            SUM(quantity * NULLIF(purchased_price, 0)) /
            NULLIF(SUM(quantity * NULLIF(price, 0)), 0),
            0.5
        ) as ratio
    FROM {offer_stocks}
    WHERE quantity > 0
),
sales_90 AS (
    SELECT product_id,
           SUM(quantity) as qty_sold_90d,
           SUM(quantity * price_sold) as revenue_90d
    FROM {order_lines}
    WHERE NOT is_return AND is_active_source
      AND order_date >= {today} - INTERVAL '90 days'
    GROUP BY product_id
),
sales_30 AS (
    SELECT product_id,
           SUM(quantity) as qty_sold_30d,
           SUM(quantity * price_sold) as revenue_30d
    FROM {order_lines}
    WHERE NOT is_return AND is_active_source
      AND order_date >= {today} - INTERVAL '30 days'
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
    FROM {views}v_sku_analysis s
    LEFT JOIN sales_90 s90 ON s.product_id = s90.product_id
    LEFT JOIN sales_30 s30 ON s.product_id = s30.product_id
    LEFT JOIN {views}v_abc_classification a ON s.offer_id = a.offer_id
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
FROM base b""",
    ),
)


def inventory_view_selects(dialect: Dialect) -> tuple[tuple[str, str], ...]:
    """The eleven inventory views for one engine, in creation order.

    Returns (qualified view name, SELECT body). The caller decides between
    `CREATE OR REPLACE VIEW` (DuckDB, which rebuilds them on every connect)
    and `CREATE VIEW` (migration 0014, which freezes this rendering once).
    """
    return tuple(
        (
            f"{dialect.inventory_views}{name}",
            select.format(
                views=dialect.inventory_views,
                today=TODAY_IN_KYIV,
                sku_inventory_status=dialect.sku_inventory_status,
                categories=dialect.categories,
                offer_stocks=dialect.offer_stocks,
                order_lines=dialect.order_lines,
            ),
        )
        for name, select in _INVENTORY_VIEWS
    )
