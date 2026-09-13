"""The offer catalogue and the sync watermarks — the last two of stage 3.

Revision ID: 0029_offers_and_sync_metadata
Revises: 0028_quality_journal
Created: 2026-09-14

WITH THESE TWO, EVERY DUCKDB TABLE HAS A POSTGRES COUNTERPART

`offers` maps an offer id to its product and SKU — 1 057 rows, straight from
KeyCRM's `GET /offers`. `sync_metadata` is eleven rows of key/value: nine
`last_sync_*` watermarks, `dq_digest_last_sent`, and one thing that is not like
the others (below).

`bronze.offers` — REPLICATED, NOT MIRRORED, AND THAT IS THE INTERESTING CALL

It is landing data, so the obvious home is `core/pg_landing.py`: parse the
payload once in `core/landing_rows.py` and write the same tuple to both stores.
That is what the plan said and it is the wrong answer here, for the reason
`bronze.offer_stocks` is already in `core/pg_operational.py` and says so in its
own comment.

`offers` and `offer_stocks` are two halves of one inventory sync. They are read
together — `sku_inventory_status` is built `FROM offer_stocks LEFT JOIN offers`
— and `stock_movements` is a delta computed against the previous contents of
`offer_stocks` on the same tick. Sending one through the mirror and the other
through the replicator gives them two clocks, two failure modes and two ways to
stand down; the SMS switch already did exactly that to `offer_stocks` once, and
it silently stopped moving at 08:00:53 on 2026-09-06 with nothing to report it.
Both travel together here instead, in one call, on one clock.

`sync_metadata` — AND THE ONE KEY THAT IS NOT COPIED

Ten of the eleven keys are durable facts: when each entity last synced, and
when the digest last went out. They are what a writer moved to Postgres in
stage 4 will need in order to know where it left off, which is why this table
has to cross at all.

`warehouse_catalog_dirty` is not one of those. It is a **coordination flag**:
set when a catalogue sync changes something, read every two minutes by the
warehouse refresh, and deleted **conditionally on its own `updated_at`** — an
optimistic-concurrency token whose whole meaning is "a rebuild in this process
still has to happen". Postgres runs no warehouse rebuild, so a copy of it there
would assert something that cannot be true, and it would be wrong in a
specific, recurring way: the flag lives for about two minutes, the copy runs
hourly and the comparison runs at 07:30, so any window that catches it set at
the copy and cleared at the check reports an orphan the copy itself created.

So the Postgres copy is a **projection**, and the exclusion is written down in
one place (`SYNC_METADATA_SOURCE` in `core/pg_operational.py`) that both the
shipper and the comparison read, rather than in two places that can drift. The
DuckDB table keeps all eleven rows and `core/snapshot_validation.py` still sees
it whole.

TYPES

`value` is TEXT and unbounded on purpose: `warehouse_catalog_dirty` is excluded
but the shape it demonstrated is not hypothetical — DuckDB's VARCHAR has no
length, and a `VARCHAR(n)` here would raise on the first value that outgrew it,
inside the hourly transaction, taking sixteen other tables with it.

`offers.product_id` is NOT NULL, matching DuckDB. KeyCRM has never served an
offer without one; if it ever does, the DuckDB insert fails first and this
never sees it.
"""
from alembic import op


revision = "0029_offers_and_sync_metadata"
down_revision = "0028_quality_journal"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # `bronze`, beside `offer_stocks`: this is a KeyCRM payload and could be
    # re-fetched, which is what `bronze` means. That it is *replicated* rather
    # than mirrored is about which code path carries it, not about what it is.
    op.execute("""
        CREATE TABLE IF NOT EXISTS bronze.offers (
            id         INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            sku        VARCHAR,
            synced_at  TIMESTAMPTZ,
            PRIMARY KEY (id)
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_offers_product "
               "ON bronze.offers (product_id)")

    # `app`, not `bronze`: a sync watermark is this system's own bookkeeping.
    # KeyCRM has never heard of it and nothing can re-derive when we last
    # asked it a question.
    op.execute("""
        CREATE TABLE IF NOT EXISTS app.sync_metadata (
            key        VARCHAR     NOT NULL,
            value      TEXT,
            updated_at TIMESTAMPTZ,
            PRIMARY KEY (key)
        )
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS bronze.offers")
    op.execute("DROP TABLE IF EXISTS app.sync_metadata")
