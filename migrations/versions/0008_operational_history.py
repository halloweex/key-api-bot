"""app.* — the five tables nothing can reconstruct

Revision ID: 0008_operational_history
Revises: 0007_gold_daily_revenue
Created: 2026-08-27

Everything Postgres holds so far is recoverable. `bronze.*` is a KeyCRM payload
and KeyCRM still has it; `silver.orders` and `gold.daily_revenue` are computed
and can be computed again. These five cannot be. They are the reason the plan's
2026-08-22 amendment routed the warehouse tables to Postgres and **not** to
ClickHouse: a store you would rebuild from source is a cache, and none of these
has a source to rebuild from.

WHAT EACH ONE KNOWS THAT KEYCRM DOES NOT

`stock_movements` is the only record that a quantity ever changed. It is
computed as a delta against the *previous* contents of `offer_stocks` — KeyCRM
serves the current level and nothing else, so a movement not written at the
moment it happened is gone. 50,386 rows back to 2026-03-14.

`inventory_sku_history` is a per-SKU daily snapshot, written once a day and
never revisited. 143,274 rows back to 2026-01-27. `inventory_history` is the
same thing rolled up, 188 rows.

`sku_inventory_status` is current state and would be re-derivable but for
`first_seen_at`, which the rebuild carries forward out of the table's own
previous contents. Lose the table and that column restarts at today for every
SKU that has never been ordered.

`order_backfill_misses` records ids KeyCRM **could not supply** — the one fact
an API can never be asked for. Without it the gap backfill re-asks for the same
1,660 holes forever.

WHY `app` AND NOT `bronze`

`postgres/initdb/10-roles-and-schemas.sql` draws the line and revision 0005
follows it: `bronze` is landing from KeyCRM, `app` is what nothing can decide
again. All five are the second kind. They are **replicated**, not mirrored —
read out of DuckDB as they stand, exactly like `app.manager_classifications`
and for the same reason: derived from state only DuckDB holds, so a second
computation on this side would diverge the first time a sync was missed.

TWO SHIPPING SHAPES, AND THE SCHEMA REFLECTS BOTH

`order_backfill_misses`, `inventory_history` and `sku_inventory_status` are
replaced whole — 43, 188 and 887 rows, and the last one is a `DELETE`+`INSERT`
on the DuckDB side too, so a full replace is not a choice, it is the same
operation.

`inventory_sku_history` and `stock_movements` are **append-only** — verified,
not assumed: no `UPDATE` and no `DELETE` against either exists anywhere in the
repository. So they ship what is above what Postgres already holds, and the
watermark is `MAX(date)` / `MAX(id)` recomputed from the data rather than
stored. There is no cursor to corrupt and no state to resume from.

`stock_movements.id` is **carried from DuckDB, not generated here.** DuckDB
assigns it from `seq_stock_movements_id`, and the whole point of the daily
comparison is that the two stores agree row for row — a Postgres sequence would
hand the same movement a different name and the comparison would be meaningless
before it began. Hence a plain `BIGINT PRIMARY KEY` and no `GENERATED`.

NO CHECK CONSTRAINTS, NO FOREIGN KEYS

Revisions 0002, 0003 and 0006's reason, unchanged: a constraint the other store
lacks makes this a filter rather than a copy, and the reconciliation would then
report a difference the schema created. `movement_type` is one of three strings
and is left as `TEXT` for exactly that reason.
"""
from __future__ import annotations

from alembic import op

revision = "0008_operational_history"
down_revision = "0007_gold_daily_revenue"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── ids KeyCRM could not supply ──────────────────────────────────────────
    op.execute(
        """
        CREATE TABLE app.order_backfill_misses (
            order_id   BIGINT      PRIMARY KEY,
            checked_at TIMESTAMPTZ,
            reason     TEXT
        )
        """
    )

    # ── the rolled-up daily snapshot ─────────────────────────────────────────
    op.execute(
        """
        CREATE TABLE app.inventory_history (
            date           DATE          PRIMARY KEY,
            total_quantity INTEGER       NOT NULL,
            total_value    NUMERIC(14,2) NOT NULL,
            total_reserve  INTEGER,
            sku_count      INTEGER,
            recorded_at    TIMESTAMPTZ
        )
        """
    )

    # ── current state per SKU ────────────────────────────────────────────────
    op.execute(
        """
        CREATE TABLE app.sku_inventory_status (
            offer_id          INTEGER       PRIMARY KEY,
            product_id        INTEGER       NOT NULL,
            sku               TEXT          NOT NULL,
            name              TEXT,
            brand             TEXT,
            category_id       INTEGER,
            quantity          INTEGER       NOT NULL,
            reserve           INTEGER       NOT NULL,
            price             NUMERIC(12,2) NOT NULL,
            purchased_price   NUMERIC(12,2),
            last_sale_date    DATE,

            -- Carried forward by the DuckDB rebuild out of the table's own
            -- previous contents. This is the column that makes the table
            -- irreplaceable rather than merely expensive to rebuild.
            first_seen_at     DATE          NOT NULL,

            updated_at        TIMESTAMPTZ,
            last_stock_out_at DATE
        )
        """
    )

    # ── the per-SKU daily snapshot, append-only ──────────────────────────────
    op.execute(
        """
        CREATE TABLE app.inventory_sku_history (
            date     DATE          NOT NULL,
            offer_id INTEGER       NOT NULL,
            quantity INTEGER       NOT NULL,
            reserve  INTEGER       NOT NULL,
            price    NUMERIC(12,2) NOT NULL,
            PRIMARY KEY (date, offer_id)
        )
        """
    )
    # The comparison fingerprints this one date by date, and the shipper asks
    # for everything from `MAX(date)` onward. Both read the leading column of
    # the primary key, so neither needs an index of its own.

    # ── every change to a quantity, append-only ──────────────────────────────
    op.execute(
        """
        CREATE TABLE app.stock_movements (
            -- From DuckDB's sequence. See the module docstring: generating it
            -- here would give the same movement two names.
            id              BIGINT      PRIMARY KEY,

            offer_id        INTEGER     NOT NULL,
            product_id      INTEGER,
            movement_type   TEXT        NOT NULL,
            quantity_before INTEGER     NOT NULL,
            quantity_after  INTEGER     NOT NULL,
            delta           INTEGER     NOT NULL,
            reserve_before  INTEGER     NOT NULL,
            reserve_after   INTEGER     NOT NULL,
            recorded_at     TIMESTAMPTZ,
            source          TEXT
        )
        """
    )
    op.execute(
        "CREATE INDEX idx_stock_movements_offer "
        "ON app.stock_movements (offer_id, recorded_at DESC)"
    )

    for table, comment in (
        ("order_backfill_misses",
         "Replicated from DuckDB. Ids KeyCRM could not supply — the one fact "
         "an API cannot be asked for. Full replace."),
        ("inventory_history",
         "Replicated from DuckDB. Daily stock roll-up; KeyCRM keeps no "
         "history. Full replace."),
        ("sku_inventory_status",
         "Replicated from DuckDB. first_seen_at is carried forward by the "
         "rebuild and cannot be re-derived. Full replace."),
        ("inventory_sku_history",
         "Replicated from DuckDB. Per-SKU daily snapshot, append-only; "
         "shipped from MAX(date) onward."),
        ("stock_movements",
         "Replicated from DuckDB. Every quantity change ever recorded, "
         "append-only; id comes from DuckDB, shipped above MAX(id)."),
    ):
        op.execute(f"COMMENT ON TABLE app.{table} IS '{comment}'")


def downgrade() -> None:
    for table in (
        "stock_movements", "inventory_sku_history", "sku_inventory_status",
        "inventory_history", "order_backfill_misses",
    ):
        op.execute(f"DROP TABLE IF EXISTS app.{table}")
