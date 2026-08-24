"""meta.mirror_state.backfilled_at — the gate Reconciliation A needs for orders

Revision ID: 0004_backfilled_at
Revises: 0003_landing_orders
Created: 2026-08-24

The catalogue never needed this. `mirror_products` ships all 1,003 rows every
hour, so `last_ok_at` proves what Postgres held at that instant, and a row
DuckDB has that Postgres does not is a row KeyCRM has retired — the rule
revision 0002's check is built on.

Orders break that rule. `upsert_orders` mirrors `updated_ids`, the delta,
because re-sending a page of 500 unchanged orders every minute is not a mirror,
it is a load generator. So `last_ok_at` proves only that the last delta landed;
it says nothing about the 46,487 orders that predate the mirror. Until the
backfill has walked the whole table, "missing from Postgres" means "not carried
across yet", and a check with a tolerance of zero would report tens of
thousands of CRITICAL findings for a job nobody has run.

One column answers it. `backfilled_at` is set by `core.pg_backfill` when a run
finishes with nothing remaining, and it is what lets the check say **"history
is present, so any difference is now a defect"** instead of guessing from the
size of the difference.

NULLABLE, AND NULL MEANS TWO DIFFERENT THINGS

For `bronze.orders` and `bronze.order_products`, NULL means the backfill has
not completed and the row-level comparison is suppressed. For the catalogue
tables it stays NULL forever and is never read — they have no history to carry,
and their check uses the watermark rule instead. Which reading applies is a
property of the table's spec in `core.mirror_reconciliation`, not of this
column.

Adding a nullable column to a five-row table is instant in Postgres. It is
worth saying out loud only because the sibling store's equivalent is not:
`ALTER TABLE ADD COLUMN ... DEFAULT x` in DuckDB rewrites the whole table to
materialise the default and OOMs on a 9 GB file, which is why nothing in this
repository ever writes one.
"""
from __future__ import annotations

from alembic import op

revision = "0004_backfilled_at"
down_revision = "0003_landing_orders"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE meta.mirror_state ADD COLUMN backfilled_at TIMESTAMPTZ")
    op.execute(
        "COMMENT ON COLUMN meta.mirror_state.backfilled_at IS "
        "'When a backfill last finished with nothing remaining. NULL means the "
        "history predating the mirror has not been carried across, and the "
        "row-level reconciliation is suppressed for tables that need one.'"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE meta.mirror_state DROP COLUMN IF EXISTS backfilled_at")
