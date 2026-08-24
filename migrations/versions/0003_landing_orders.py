"""bronze.orders and bronze.order_products — landing's second and largest pair

Revision ID: 0003_landing_orders
Revises: 0002_landing_catalog
Created: 2026-08-24

The catalogue went first because a mistake on it costs one resync. These are
the tables the business is actually made of: 46,446 orders and 147,508 line
items, measured on the production backup of 2026-08-24, back to 2023-12-02.

TYPES MIRROR DUCKDB EXACTLY, AND ONE OF THEM IS NOT OBVIOUS

`NUMERIC(12,2)` for both money columns, matching DuckDB's `DECIMAL(12,2)`, so
the daily reconciliation compares values and not binary representations.

`order_products.id` is **BIGINT**, not INTEGER, and that is not caution — it is
what DuckDB declares. The id is synthetic: `order_id * 1000 + position`. The
largest today is 46,489,001, which fits an INTEGER comfortably, and would stop
fitting at order id ~2.1M. Matching DuckDB's width means the ceiling arrives on
both sides at once instead of on one.

`TEXT` rather than `VARCHAR(n)` throughout, for the reason revision 0002 gives:
DuckDB imposes no length, and a length invented here would let Postgres reject
a row DuckDB accepted. The longest line-item name today is 147 characters.

NO FOREIGN KEY, AND THIS TIME IT WOULD HOLD TOO

`order_products.order_id` against `orders.id`: **0** orphans on the production
backup of 2026-08-24. Still not declared, for revision 0002's reason — a mirror
that rejects a row DuckDB accepted is a filter, not a mirror — and for one more
that is specific to these two tables: they are written in one transaction but
the *backfill* walks them in chunks, and a chunk boundary that splits an order
from its line items is an ordinary intermediate state, not a fault.

WHAT `mirrored_at` IS FOR, AND WHAT IT IS NOT

This copy's bookkeeping. DuckDB keeps `synced_at`, `first_seen_at` and
`update_count` on `orders`; none of them are mirrored and none of them are
compared. Two correct copies differ on those by construction.

INDEXES

`order_products(order_id)` because every read of a line item starts from its
order, and because the mirror deletes by `order_id` on every write — without
it that DELETE is a sequential scan of 147,508 rows per synced order.

`orders(ordered_at)` because the reconciliation compares day by day, and a
per-day checksum over 46,446 rows is the one query this table exists to serve
until the read switches.
"""
from __future__ import annotations

from alembic import op

revision = "0003_landing_orders"
down_revision = "0002_landing_catalog"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE bronze.orders (
            id              INTEGER       PRIMARY KEY,
            source_id       INTEGER       NOT NULL,
            status_id       INTEGER       NOT NULL,
            -- KeyCRM's own grouping; 6 = lost/cancel. NULL on 44,231 of 46,446
            -- rows: the column arrived after they were synced, and
            -- OrderStatus.return_statuses() is the fallback for those.
            status_group_id INTEGER,
            grand_total     NUMERIC(12,2) NOT NULL,
            ordered_at      TIMESTAMPTZ,
            created_at      TIMESTAMPTZ,
            updated_at      TIMESTAMPTZ,
            buyer_id        INTEGER,
            manager_id      INTEGER,
            -- Carries UTM attribution for Shopify orders, which is why the
            -- mirror updates it as COALESCE(new, stored) exactly as DuckDB
            -- does: a payload that omits it must not erase it.
            manager_comment TEXT,
            promocode       TEXT,

            mirrored_at     TIMESTAMPTZ   NOT NULL DEFAULT now()
        )
        """
    )
    op.execute("CREATE INDEX idx_orders_ordered_at ON bronze.orders (ordered_at)")

    op.execute(
        """
        CREATE TABLE bronze.order_products (
            -- order_id * 1000 + position. BIGINT because DuckDB says BIGINT.
            id          BIGINT        PRIMARY KEY,
            order_id    INTEGER       NOT NULL,
            -- NULL on 11,988 of 147,508 rows: a line item whose offer carries
            -- no product, which KeyCRM permits.
            product_id  INTEGER,
            name        TEXT          NOT NULL,
            quantity    INTEGER       NOT NULL,
            price_sold  NUMERIC(12,2) NOT NULL,

            mirrored_at TIMESTAMPTZ   NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        "CREATE INDEX idx_order_products_order ON bronze.order_products (order_id)"
    )

    op.execute(
        "COMMENT ON TABLE bronze.orders IS "
        "'Mirror of DuckDB orders (step 05). DuckDB is the system of record "
        "until the read switches.'"
    )
    op.execute(
        "COMMENT ON TABLE bronze.order_products IS "
        "'Mirror of DuckDB order_products (step 05). Line items are replaced "
        "per order, never upserted: the ids are positional.'"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS bronze.order_products")
    op.execute("DROP TABLE IF EXISTS bronze.orders")
