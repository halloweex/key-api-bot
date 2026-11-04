"""The warehouse rule every engine renders: Silver's projection and Gold's measures.

One text, three engines. `silver_select_sql` and `silver_pass2_sql` are what
Postgres runs to derive `silver.orders` (`core.pg_silver`), what DuckDB ran to
derive `silver_orders`, and `silver_recompute_ctes` is what the row-values
checks recompute it with (`core.pg_warehouse_dq`, `core.data_quality`).
`GOLD_MEASURES` is rendered verbatim by Postgres (`core.pg_gold`), by
ClickHouse (`core.ch_silver.derive_gold_sql`) and by DuckDB
(`core.duckdb_store.GOLD_REVENUE_SELECT_SQL`).

They lived in `core/duckdb_store.py` until step 14's S14-01, so every
Postgres derivation tick imported the DuckDB store lazily — and with it the
`duckdb` driver and pandas — to render a string. They moved here unchanged;
`core.duckdb_store` re-exports each one as the same object for one release,
and `tests/unit/test_warehouse_sql.py` holds every rendering to the bytes the
code before the move produced.

Every builder takes its dialect since step 14's S14-07, when the default —
`core.sql_dialect.DUCKDB` — stopped being one: the DuckDB renderings left (the
store's own rebuild, its row-values check, the DuckDB half of
`POST /api/warehouse/rebuild-silver`) name `DUCKDB` at the call, until the PRs
that delete them. Nothing here imports `duckdb`.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Dict

from core.duckdb_constants import (
    B2B_MANAGER_ID, EXHIBITION_SOURCE_ID, RETAIL_MANAGER_IDS, REVENUE_SOURCE_IDS,
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

    One text, two engines: `core.sql_dialect.POSTGRES` renders the rule
    against `bronze.orders`, and `DUCKDB`, named by its last callers, against
    DuckDB's `orders`. See that module for the two — and only two — places
    the dialects differ.
    """
    # KeyCRM's own grouping decides when we have it; the id list covers rows
    # synced before the column existed. Verified equal for every status the
    # warehouse holds — see core/models.py.
    return_statuses = tuple(int(s) for s in OrderStatus.return_statuses())
    revenue_sources = ", ".join(str(s) for s in REVENUE_SOURCE_IDS)
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
            o.source_id IN ({revenue_sources}) AS is_active_source,
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


def silver_pass2_sql(buyer_filter: str = "", *, dialect: "Dialect") -> str:
    """Recompute `is_new_customer` from each buyer's MIN(order_date).

    Empty `buyer_filter` runs on every Silver row (full mode); otherwise it is
    scoped to the affected buyers, keeping the write set bounded. `dialect`
    is keyword-only: every caller already named it so.
    """
    # Aliased on purpose. PostgreSQL cannot qualify the UPDATE target by its
    # schema in SET/WHERE — `silver.orders.buyer_id` is read as a column of a
    # column — so both dialects address the target through one short alias and
    # the text stays identical apart from the table name.
    target = dialect.silver_orders
    inner_filter = "buyer_id IS NOT NULL" if not buyer_filter else f"buyer_id IN ({buyer_filter})"
    outer_filter = "" if not buyer_filter else f"AND s.buyer_id IN ({buyer_filter})"
    return f"""
                UPDATE {target} AS s SET
                    buyer_first_order_date = fo.first_order_date,
                    is_new_customer = CASE
                        WHEN s.buyer_id IS NOT NULL
                             AND NOT s.is_return
                             AND s.is_active_source
                             AND s.order_date = fo.first_order_date
                        THEN TRUE ELSE FALSE
                    END
                FROM (
                    -- The baseline deliberately does NOT filter is_active_source.
                    -- A purchase on a retired channel is still a purchase: with
                    -- Opencart excluded here, 419 buyers whose first order was
                    -- placed there counted as brand new the next time they bought
                    -- on Instagram — 422 orders and ₴1,081,979.59 of repeat
                    -- business booked as acquisition, overstating new customers
                    -- by 3.9% across 2025.
                    -- Returns stay excluded: a cancelled first order is not a
                    -- purchase, so the next one genuinely is their first.
                    SELECT buyer_id, MIN(order_date) AS first_order_date
                    FROM {target}
                    WHERE {inner_filter}
                      AND NOT is_return
                    GROUP BY buyer_id
                ) fo
                WHERE s.buyer_id = fo.buyer_id
                  {outer_filter}
    """


def silver_recompute_ctes(dialect: "Dialect") -> str:
    """Every Silver row as pass 1 then pass 2 would write it — as three CTEs,
    `pass1`, `baseline` and `recomputed`, for a caller that writes
    `WITH {this} SELECT ... FROM recomputed r`.

    The two checks that ask whether stored Silver is what the rule says today
    read this one text: DuckDB's `silver_row_values` and its Postgres twin,
    `pg_silver_row_values` (chain 2, step 8b). It used to live inside the
    DuckDB check; a Postgres copy of it would have been a third statement of
    pass 2, and the first time pass 2 changed the twin would have asked a
    different question from the rebuild it watches.

    Pass 2 is recomputed from the recomputed rows rather than from stored
    Silver. That is the point: the real pass 2 takes its baseline from whatever
    Silver currently holds, so a stale baseline reproduces itself, and only a
    recompute from landing can tell. The LEFT JOIN reproduces pass 2's own
    semantics for a buyer with no qualifying order — the real UPDATE joins
    those rows away and leaves pass 1's `FALSE` / `NULL` standing.
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
