"""bronze.categories and bronze.products — the first landing tables

Revision ID: 0002_landing_catalog
Revises: 0001_mirror_state
Created: 2026-08-24

The catalogue goes first, before orders, because charter rule 5 orders the
migration by what a mistake costs: both tables come back whole from KeyCRM, so
losing them costs one resync. 28 categories and 1,004 products is also small
enough that the whole mechanism — parse once, write twice, reconcile daily —
can be watched working before it touches 46,000 orders.

`bronze`, not `app`. `postgres/initdb/30-app.sql` draws the line and it is the
right one: `app` is for what nothing can decide again — authorised users,
goals, manager classifications. The catalogue is landing from KeyCRM and
rebuildable, which is what `bronze` is for.

TYPES MIRROR DUCKDB EXACTLY

`NUMERIC(12,2)` because DuckDB stores `DECIMAL(12,2)`: a float here would make
the daily reconciliation report differences that exist only in binary
representation, and the first days of a mirror are expensive enough to debug
without inventing discrepancies. `TEXT` rather than `VARCHAR(n)` because DuckDB
imposes no length and inventing one here would let Postgres reject a row DuckDB
accepted — the longest product name today is 147 characters, and nothing says
tomorrow's is shorter.

NO FOREIGN KEYS, DELIBERATELY

Both would hold right now. Measured on the production backup of 2026-08-24:
`products.category_id` has **0** rows pointing at a missing category, and
`categories.parent_id` has 0 as well.

They are still not declared, because a mirror that rejects a row DuckDB
accepted is not a mirror — it is a filter, and the reconciliation would then
report a difference caused by this schema rather than by the data. DuckDB
enforces no FK at all (its own bug, documented in CLAUDE.md), so the sync
happily stores a product whose category has not arrived yet; a delta sync that
carries products without their categories is an ordinary event, not a fault.

The measurement is recorded so that a later hardening step knows the constraint
is viable, and can add it `NOT VALID` after the backfill rather than guessing.

WHAT IS AND IS NOT MIRRORED

Only what KeyCRM said: the columns of `core.landing_rows.CategoryRow` and
`ProductRow`. `mirrored_at` is this store's own bookkeeping and is named
differently from DuckDB's `synced_at` on purpose — two correct copies differ on
those by construction, and a shared name would invite comparing them and
finding every row in disagreement.
"""
from __future__ import annotations

from alembic import op

revision = "0002_landing_catalog"
down_revision = "0001_mirror_state"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE bronze.categories (
            id          INTEGER     PRIMARY KEY,
            name        TEXT        NOT NULL,
            parent_id   INTEGER,

            -- This copy's bookkeeping, never compared against DuckDB's.
            mirrored_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        "CREATE INDEX idx_categories_parent ON bronze.categories (parent_id)"
    )

    op.execute(
        """
        CREATE TABLE bronze.products (
            id          INTEGER       PRIMARY KEY,
            name        TEXT          NOT NULL,
            category_id INTEGER,
            -- 400 of 1,004 products carry no brand: it is typed by a human
            -- into a KeyCRM custom field, and the dashboard's `Unknown` bucket
            -- is exactly this column being NULL.
            brand       TEXT,
            sku         TEXT,
            -- DECIMAL(12,2) in DuckDB. Same scale, so the reconciliation
            -- compares values rather than representations.
            price       NUMERIC(12,2),

            mirrored_at TIMESTAMPTZ   NOT NULL DEFAULT now()
        )
        """
    )

    op.execute(
        "COMMENT ON TABLE bronze.categories IS "
        "'Mirror of DuckDB categories (step 05). Landing from KeyCRM, "
        "rebuildable; DuckDB is the system of record until the read switches.'"
    )
    op.execute(
        "COMMENT ON TABLE bronze.products IS "
        "'Mirror of DuckDB products (step 05). Landing from KeyCRM, "
        "rebuildable; DuckDB is the system of record until the read switches.'"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS bronze.products")
    op.execute("DROP TABLE IF EXISTS bronze.categories")
