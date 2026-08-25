"""silver.orders — the first table Postgres will compute rather than receive

Revision ID: 0006_silver_orders
Revises: 0005_manager_classification
Created: 2026-08-25

Everything Postgres holds so far arrived from somewhere else: `bronze.*` is a
KeyCRM payload written twice, `bronze.managers` and `app.manager_classifications`
are a copy of a human's decision. This one is **derived** — the output of
`silver_select_sql(POSTGRES)` over `bronze.orders`, the same projection DuckDB
runs, rendered for this engine.

That difference decides most of what is below.

NO `mirrored_at`, AND NO PER-ROW BOOKKEEPING AT ALL

`bronze.products` and `bronze.orders` carry `mirrored_at` because each row
arrives on its own schedule and "when did this row last land" is a real
question. Silver is rebuilt **whole**, in one transaction, so the answer would
be the same 46,000 times. The table-level fact belongs in `meta.mirror_state`,
which already exists and is already what Reconciliation A reads.

TYPES MIRROR DUCKDB, INCLUDING THE ONES THAT LOOK LOOSE

`NUMERIC(12,2)` against DuckDB's `DECIMAL(12,2)`, so the daily comparison sees
values rather than representations — revision 0002's reason, unchanged.

`sales_type` and `source_name` are `TEXT` with **no CHECK**, though the set of
legal values is known (`KNOWN_SALES_TYPES`, plus `exhibition` since #101).
A constraint here would let Postgres reject a row DuckDB accepted, which makes
this a filter rather than a copy and turns the reconciliation into a report
about the schema. The same reasoning kept foreign keys out of 0002 and 0003.

`order_date` is `DATE NOT NULL` because the projection cannot produce NULL:
`ordered_at` is NOT NULL in practice — 0 of 46,530 orders lack it — and
`landed_orders` drops any order that does.

THE INDEXES ARE THE ONES THE NEXT TWO STEPS NEED

`(order_date, sales_type)` is how Gold groups, and Gold is the step after this.
`buyer_id` is what pass 2 groups by when it recomputes `is_new_customer` from
each buyer's first order. Neither is speculative; both are in
`GOLD_REVENUE_SELECT_SQL` and `silver_pass2_sql` today.

No index on `id` beyond the primary key, and — unlike the other store — no
reason to fear one: the ART pathology that makes a single index cost DuckDB
1.171 MB per rebuild cycle has no equivalent here.
"""
from __future__ import annotations

from alembic import op

revision = "0006_silver_orders"
down_revision = "0005_manager_classification"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE silver.orders (
            id                     INTEGER       PRIMARY KEY,
            source_id              INTEGER       NOT NULL,
            status_id              INTEGER       NOT NULL,
            grand_total            NUMERIC(12,2) NOT NULL,
            ordered_at             TIMESTAMPTZ,
            buyer_id               INTEGER,
            manager_id             INTEGER,

            -- The Kyiv calendar date. Both engines render it from the same
            -- template in `core/sql_dialect.py`; 1,007 orders have a Kyiv date
            -- different from their UTC one, so this is not cosmetic.
            order_date             DATE          NOT NULL,

            is_return              BOOLEAN       NOT NULL,
            -- retail | b2b | internal | exhibition. No CHECK — see the module
            -- docstring: a constraint the other store lacks makes this a
            -- filter rather than a copy.
            sales_type             TEXT          NOT NULL,
            is_active_source       BOOLEAN       NOT NULL,
            source_name            TEXT          NOT NULL,

            -- Written FALSE by pass 1 and recomputed by pass 2 from each
            -- buyer's first order. The default matches DuckDB's so a
            -- half-applied rebuild cannot invent acquisitions.
            is_new_customer        BOOLEAN       NOT NULL DEFAULT FALSE,
            buyer_first_order_date DATE,
            promocode              TEXT
        )
        """
    )

    op.execute(
        "CREATE INDEX idx_silver_orders_date_type "
        "ON silver.orders (order_date, sales_type)"
    )
    op.execute(
        "CREATE INDEX idx_silver_orders_buyer ON silver.orders (buyer_id)"
    )

    op.execute(
        "COMMENT ON TABLE silver.orders IS "
        "'Derived, not mirrored: the output of silver_select_sql(POSTGRES) "
        "over bronze.orders. Rebuilt whole; the watermark lives in "
        "meta.mirror_state.'"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS silver.orders")
