"""gold.daily_revenue — Gold computed in Postgres, at the grain DuckDB cannot hold

Revision ID: 0007_gold_daily_revenue
Revises: 0006_silver_orders
Created: 2026-08-27

Revision 0006 gave Postgres the first table it *derives*. This is the second,
and unlike Silver it is deliberately **not** the same shape as its DuckDB
counterpart.

WHY THE SHAPE CHANGES HERE AND DID NOT CHANGE FOR SILVER

`gold_daily_revenue` in DuckDB splits the channels into columns —
`instagram_revenue`, `telegram_revenue`, `shopify_revenue`, and three matching
order counts. A column per channel can only name the channels somebody wrote a
column for. Source 5 (Виставка) arrived with #101, and on production today:

    SUM(revenue) - SUM(the three revenue columns)  =  ₴266,059.00
    orders behind that gap                         =  177

All of it sits in the `exhibition` rows, whose three channel columns are
**empty while their revenue is not** — the money is in the warehouse and in no
channel, so any per-channel reading of Gold silently omits it. Nothing about
that is specific to source 5: the next channel needs a schema change, a
backfill and an edit to every reader, where a dimension needs none of the
three. The owner settled on the dimension on 2026-08-25, and this warehouse
already does it elsewhere — `gold_daily_products` has been grained
`(date, sales_type, source_id)` all along.

WHY THE TABLE ALSO HOLDS THE ROLL-UP, AND WHY THAT IS NOT REDUNDANCY

Three of the eight measures are `COUNT(DISTINCT buyer_id)`, and a distinct
count does not add up. Measured on production Silver, summing the per-source
rows instead of grouping without the source overstates:

    unique_customers      29 of 2,107 cells
    new_customers         12
    returning_customers   17

each by exactly one buyer — somebody who bought on two channels the same day.
The closing criterion for step 05 is zero, so an approximation is not
available, and the read switch cannot serve those columns from the fine rows
either.

ClickHouse answers this with `AggregateFunction(uniqExact)`, which merges. That
is a real answer and it is the one step 06 will use; PostgreSQL has no
equivalent. Its answer is `GROUPING SETS`, which computes both grains in one
pass over one snapshot, and that is what `core/pg_gold.py` writes.

    source_id IS NOT NULL   one channel on one day
    source_id IS NULL       every channel on that day, counted once

`silver.orders.source_id` is `NOT NULL` (0 of 46,649 rows), so NULL is
unambiguous here — it is the roll-up and nothing else. `UNIQUE NULLS NOT
DISTINCT` is what stops two roll-ups for one cell; plain `UNIQUE` treats NULLs
as distinct and would let them accumulate silently.

INACTIVE SOURCES GET A ROW, AND IT IS ALL ZEROES

Every measure filters `is_active_source`, so Opencart (source 3, 2,470 orders,
₴5.8M gross) produces a row of zeroes. That is not a claim it sold nothing —
it is `is_active_source` written out. DuckDB already does the same thing one
grain up: a day whose only orders were Opencart gets a zero row there too.
Keeping the behaviour identical is what lets the two cell sets be compared.

NO CHECK ON `sales_type`, NO FOREIGN KEYS

Revision 0006's reasons, unchanged: a constraint the other store lacks makes
this a filter rather than a computation, and turns the daily comparison into a
report about the schema.
"""
from __future__ import annotations

from alembic import op

revision = "0007_gold_daily_revenue"
down_revision = "0006_silver_orders"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE gold.daily_revenue (
            date                DATE          NOT NULL,
            sales_type          TEXT          NOT NULL,

            -- NULL is the all-sources roll-up for (date, sales_type), not an
            -- unknown source. See the module docstring: the three distinct
            -- counts below cannot be summed out of the per-source rows.
            source_id           INTEGER,

            revenue             NUMERIC(14,2) NOT NULL,
            orders_count        INTEGER       NOT NULL,
            unique_customers    INTEGER       NOT NULL,
            new_customers       INTEGER       NOT NULL,
            returning_customers INTEGER       NOT NULL,
            returns_count       INTEGER       NOT NULL,
            returns_revenue     NUMERIC(14,2) NOT NULL,

            -- The one column the two engines may legitimately differ on by a
            -- cent: DuckDB's DECIMAL division promotes to DOUBLE and rounds
            -- the result, PostgreSQL divides exactly and rounds once. The
            -- warehouse's own Gold check already allows exactly this — see
            -- MATERIAL_THRESHOLDS in core/data_quality.py.
            avg_order_value     NUMERIC(12,2) NOT NULL,

            CONSTRAINT gold_daily_revenue_cell
                UNIQUE NULLS NOT DISTINCT (date, sales_type, source_id)
        )
        """
    )

    # How the dashboard reads it after the switch, and how the daily comparison
    # pulls the roll-up on its own: `WHERE source_id IS NULL` against 2,107
    # rows out of 5,684.
    op.execute(
        "CREATE INDEX idx_gold_daily_revenue_rollup "
        "ON gold.daily_revenue (date, sales_type) WHERE source_id IS NULL"
    )

    op.execute(
        "COMMENT ON TABLE gold.daily_revenue IS "
        "'Derived, not mirrored: GOLD_MEASURES over silver.orders, at both "
        "grains. source_id IS NULL is the all-sources roll-up, which exists "
        "because COUNT(DISTINCT buyer_id) does not sum. Rebuilt whole; the "
        "watermark lives in meta.mirror_state.'"
    )
    op.execute(
        "COMMENT ON COLUMN gold.daily_revenue.source_id IS "
        "'NULL = roll-up over every source for this (date, sales_type).'"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS gold.daily_revenue")
