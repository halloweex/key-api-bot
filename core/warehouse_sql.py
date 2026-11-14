"""The warehouse rule every engine renders: Silver's projection and Gold's measures.

One text, and once three engines. `silver_recompute_ctes` — `silver_select_sql`
and each buyer's first order, in one SELECT — is what Postgres inserts to
derive `silver.orders` (`core.pg_silver`) and what the row-values check
recomputes it with (`core.pg_warehouse_dq`). DuckDB ran `silver_select_sql`
and a second pass, an UPDATE (`silver_pass2_sql`), to derive `silver_orders`
until step 14's S14-15; Postgres ran the same two passes until the review's
A23 (2026-11) folded the UPDATE into the INSERT and deleted it. `GOLD_MEASURES` is rendered verbatim by Postgres
(`core.pg_gold`) and by ClickHouse (`core.ch_silver.derive_gold_sql`); DuckDB
rendered it into `core.duckdb_store.GOLD_REVENUE_SELECT_SQL` until S14-15.

They lived in `core/duckdb_store.py` until step 14's S14-01, so every
Postgres derivation tick imported the DuckDB store lazily — and with it the
`duckdb` driver and pandas — to render a string. They moved here unchanged
(`core.duckdb_store` re-exported each as the same object until S14-18), and
`tests/unit/test_warehouse_sql.py` holds every rendering to the bytes the
code before the move produced.

Every builder takes its dialect since step 14's S14-07, when the default —
`core.sql_dialect.DUCKDB` — stopped being one. The DuckDB renderings that
named `DUCKDB` at the call (the store's own rebuild, its row-values check, the
DuckDB half of `POST /api/warehouse/rebuild-silver`) went in S14-15 and S14-18;
nothing in `core/`, `web/` or `bot/` renders `DUCKDB` since. Nothing here
imports `duckdb`.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Dict

from core import duckdb_constants
from core.duckdb_constants import (
    B2B_MANAGER_ID, EXHIBITION_SOURCE_ID, RETAIL_MANAGER_IDS, REVENUE_SOURCE_IDS,
    not_own_entity,
)
from core.models import LOST_STATUS_GROUP_ID, OrderStatus

if TYPE_CHECKING:  # pragma: no cover — the annotation's name, no import cost
    from core.sql_dialect import Dialect


# ─── The one definition of a Silver row ──────────────────────────────────────
#
# Two copies of this existed: here and in `web/routes/api/admin.py`'s
# `rebuild-silver` endpoint. On 2026-08-20 the exhibition source was added to
# one of them and not the other, by someone who knew about the duplicate and
# was checking for exactly this — so `rebuild-silver` would have silently
# reverted the fix and dropped 178 orders back out of revenue. The divergence
# took under a day.
#
# `GOLD_MEASURES` below is the pattern done right: one home, imported by
# whoever needs it. These do the same for Silver. A test fails if a second
# copy appears.


def silver_sales_type_case(dialect: "Dialect") -> str:
    """`sales_type`, the one place it is decided.

    The exhibition branch is first on purpose: that fair was staffed by a
    retail manager, so a manager-based rule would quietly pull a one-off
    channel into the retail trend it would distort.

    **Retail is resolved as of the order's own date**, from
    `manager_classifications`, not from the manager's classification today.
    Reclassifying somebody used to rewrite their whole history on the next
    rebuild — the owner ruled that out on 2026-08-20. The two fallbacks below
    keep a manager nobody has ever classified behaving exactly as before: the
    first for a manager with no interval, the second for a database whose
    `managers` table has not synced yet.
    """
    classifications = dialect.classifications
    managers = dialect.managers
    manager_list = ",".join(str(m) for m in RETAIL_MANAGER_IDS)
    order_date = dialect.kyiv_date("o.ordered_at")
    return f"""CASE
            WHEN o.source_id = {EXHIBITION_SOURCE_ID} THEN 'exhibition'
            WHEN o.manager_id IS NULL THEN 'retail'
            WHEN o.manager_id = {B2B_MANAGER_ID} THEN 'b2b'
            WHEN EXISTS (
                     SELECT 1 FROM {classifications} mc
                     WHERE mc.manager_id = o.manager_id
                       AND mc.is_retail
                       AND {order_date} >= mc.valid_from
                       AND (mc.valid_to IS NULL OR {order_date} < mc.valid_to)
                 ) THEN 'retail'
            WHEN NOT EXISTS (SELECT 1 FROM {classifications} mc
                             WHERE mc.manager_id = o.manager_id)
                 AND o.manager_id IN (SELECT id FROM {managers} WHERE is_retail = TRUE) THEN 'retail'
            WHEN NOT EXISTS (SELECT 1 FROM {classifications})
                 AND NOT EXISTS (SELECT 1 FROM {managers} WHERE is_retail = TRUE)
                 AND o.manager_id IN ({manager_list}) THEN 'retail'
            ELSE 'internal'
        END"""


def silver_select_sql(dialect: "Dialect") -> str:
    """Every column of a Silver row, selected `FROM <dialect.orders> o`.

    `core.sql_dialect.POSTGRES` renders the rule against `bronze.orders`;
    `DUCKDB` rendered it against DuckDB's `orders` until step 14 (its callers
    went in S14-15 and S14-18). See that module for the two — and only two —
    places the dialects differ.

    **`is_active_source` means "counts as revenue"**, whatever its name says:
    an order on a revenue source (`REVENUE_SOURCE_IDS`) whose buyer is not
    the company's own legal entity (`OWN_ENTITY_BUYER_IDS`, the owner's
    decision of 2026-11-14 — a sale to ourselves is not revenue). The name
    stays because renaming the column would cost a revision, ClickHouse's DDL
    and the rollback to an image before step 14. Every Gold measure, the line
    level and the inventory views read the column rather than the rule, so
    this is the one place either half is decided. The buyer half is NULL-safe
    (`not_own_entity`): an order with no buyer stays revenue, and the column
    is never NULL.
    """
    # KeyCRM's own grouping decides when we have it; the id list covers rows
    # synced before the column existed. Verified equal for every status the
    # warehouse holds — see core/models.py.
    return_statuses = tuple(int(s) for s in OrderStatus.return_statuses())
    revenue_sources = ", ".join(str(s) for s in REVENUE_SOURCE_IDS)
    counts_as_revenue = f"o.source_id IN ({revenue_sources})"
    if duckdb_constants.OWN_ENTITY_BUYER_IDS:
        counts_as_revenue += f"\n                AND {not_own_entity('o')}"
    return f"""
            o.id, o.source_id, o.status_id, o.grand_total,
            o.ordered_at, o.buyer_id, o.manager_id,
            {dialect.kyiv_date('o.ordered_at')} AS order_date,
            CASE
                WHEN o.status_group_id IS NOT NULL
                    THEN o.status_group_id = {LOST_STATUS_GROUP_ID}
                ELSE o.status_id IN {return_statuses}
            END AS is_return,
            {silver_sales_type_case(dialect)} AS sales_type,
            {counts_as_revenue} AS is_active_source,
            CASE o.source_id
                WHEN 1 THEN 'Instagram'
                WHEN 2 THEN 'Telegram'
                WHEN 4 THEN 'Shopify'
                WHEN {EXHIBITION_SOURCE_ID} THEN 'Виставка'
                ELSE 'Other'
            END AS source_name,
            FALSE AS is_new_customer,
            NULL  AS buyer_first_order_date,
            o.promocode
    """


def silver_recompute_ctes(dialect: "Dialect") -> str:
    """Every Silver row as the rule writes it — as three CTEs, `pass1`,
    `baseline` and `recomputed`, for a caller that writes
    `WITH {this} ... SELECT ... FROM recomputed`.

    The rebuild inserts exactly this (`core.pg_silver.insert_sql`, since the
    review's A23), and the check that asks whether stored Silver is still what
    the rule says, `pg_silver_row_values` (chain 2, step 8b), recomputes with
    it — one text, so the two cannot state the rule two ways. (Until A23 the
    rebuild was this projection and then a full-table UPDATE, `silver_pass2_sql`,
    and these CTEs were its SELECT form, held equal to it by a fixture.)

    The first-order columns are computed from the projected rows, never from
    stored Silver: a baseline taken from what Silver holds would reproduce a
    stale one, and only a recompute from landing can tell. A buyer with no
    qualifying order (only returns) and an order with no buyer meet no
    `baseline` row, so the LEFT JOIN leaves `is_new_customer` FALSE and
    `buyer_first_order_date` NULL — exactly what the UPDATE, which joined
    those rows away, left standing.

    **The baseline deliberately does NOT filter `is_active_source`.** A
    purchase on a retired channel is still a purchase: with Opencart excluded
    from it, 419 buyers whose first order was placed there counted as brand
    new the next time they bought on Instagram — 422 orders and ₴1,081,979.59
    of repeat business booked as acquisition, overstating new customers by
    3.9% across 2025. Returns stay excluded: a cancelled first order is not a
    purchase, so the next one genuinely is their first. (This said so in an
    SQL comment inside the UPDATE until A23 deleted it; here it stays out of
    the text, which is pinned byte for byte.)
    """
    return f"""
        pass1 AS (
            SELECT {silver_select_sql(dialect)} FROM {dialect.orders} o
        ),
        baseline AS (
            SELECT buyer_id, MIN(order_date) AS first_order_date
            FROM pass1
            WHERE buyer_id IS NOT NULL AND NOT is_return
            GROUP BY buyer_id
        ),
        recomputed AS (
            SELECT p.id, p.source_id, p.status_id, p.grand_total, p.ordered_at,
                   p.buyer_id, p.manager_id, p.order_date, p.is_return,
                   p.sales_type, p.is_active_source, p.source_name,
                   CASE
                       WHEN p.buyer_id IS NOT NULL
                            AND NOT p.is_return
                            AND p.is_active_source
                            AND p.order_date = b.first_order_date
                       THEN TRUE ELSE FALSE
                   END AS is_new_customer,
                   b.first_order_date AS buyer_first_order_date,
                   p.promocode
            FROM pass1 p
            LEFT JOIN baseline b ON b.buyer_id = p.buyer_id
        )
    """


# ─── The one definition of a Gold revenue row ────────────────────────────────
#
# Same treatment as the Silver projection above, and for the same reason:
# Postgres computes Gold too, from revision 0007 onward, and a measure with two
# homes is a measure that will differ. #101 took under a day to prove that.
#
# What does *not* transfer is the shape. DuckDB's `gold_daily_revenue` splits
# the channels into columns — `instagram_revenue`, `telegram_revenue`,
# `shopify_revenue` — and a column per source cannot name a source that did not
# exist when the column was written. Source 5 (Виставка) arrived with #101, and
# `SUM(revenue) - SUM(the three columns)` is ₴266,059.00 across 177 orders,
# measured on production: money that is in the total and in no channel. So
# Postgres carries `source_id` as a *dimension* instead, and only the measures
# below are shared.

# The two row sets every measure is built from. Spelled once so a change to
# what counts as revenue cannot reach five of the eight expressions and miss
# the rest.
_GOLD_EARNING_ROWS = "NOT is_return AND is_active_source"
_GOLD_RETURNED_ROWS = "is_return AND is_active_source"

_GOLD_REVENUE_EXPR = (
    f"COALESCE(SUM(CASE WHEN {_GOLD_EARNING_ROWS} THEN grand_total END), 0)"
)
_GOLD_ORDERS_EXPR = (
    f"COUNT(DISTINCT CASE WHEN {_GOLD_EARNING_ROWS} THEN id END)"
)

# Engine-neutral by construction: every expression here is ordinary aggregate
# SQL that means the same thing in DuckDB and in PostgreSQL. A test asserts
# each one appears verbatim in both renderings.
GOLD_MEASURES: "Dict[str, str]" = {
    "revenue": _GOLD_REVENUE_EXPR,
    "orders_count": _GOLD_ORDERS_EXPR,
    "unique_customers":
        f"COUNT(DISTINCT CASE WHEN {_GOLD_EARNING_ROWS} AND buyer_id IS NOT NULL THEN buyer_id END)",
    "new_customers":
        f"COUNT(DISTINCT CASE WHEN {_GOLD_EARNING_ROWS} AND is_new_customer THEN buyer_id END)",
    "returning_customers":
        f"COUNT(DISTINCT CASE WHEN {_GOLD_EARNING_ROWS} AND NOT is_new_customer AND buyer_id IS NOT NULL THEN buyer_id END)",
    "returns_count":
        f"COUNT(DISTINCT CASE WHEN {_GOLD_RETURNED_ROWS} THEN id END)",
    "returns_revenue":
        f"COALESCE(SUM(CASE WHEN {_GOLD_RETURNED_ROWS} THEN grand_total END), 0)",
    # Written out rather than `revenue / orders_count` because the guard and
    # the division must read the same aggregate, and an alias is not available
    # to a sibling select item in either engine.
    "avg_order_value":
        "CASE\n"
        f"        WHEN {_GOLD_ORDERS_EXPR} > 0\n"
        f"        THEN {_GOLD_REVENUE_EXPR}\n"
        f"             / {_GOLD_ORDERS_EXPR}\n"
        "        ELSE 0\n"
        "    END",
}
