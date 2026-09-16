"""Where a write chain keeps its sync watermarks once Postgres writes it.

Revision ID: 0032_chain_watermarks
Revises: 0031_manual_expense_ids
Created: 2026-09-16

Chain 1 moved its two watermarks, `last_sync_offers` and `last_sync_stocks`, to
Postgres — into `app.sync_metadata`. That was a defect, found by the chain-2
design review and confirmed by reading the code, and it had three parts:

- `set_last_sync_time` wrote the key to Postgres;
- `get_last_sync_time` read it back from DuckDB, where it had stopped moving;
- and `app.sync_metadata` is full-replaced out of DuckDB every hour, so even
  the Postgres copy was put back to the frozen value.

The flag was off, so nothing broke. But it meant the inventory chain was not
ready to switch.

WHY A TABLE OF ITS OWN, NOT A STAND-DOWN INSIDE THE OLD ONE

`app.sync_metadata` also holds keys DuckDB still owns — the warehouse dirty
flag, the Meilisearch watermarks — so the table cannot be stood down whole. A
key-level stand-down would have to change the hourly replace's DELETE, its
source projection and the Postgres side of the daily comparison: the shared
machinery that also guards the five irreplaceable tables. Excluding the keys
from the source alone, the obvious half, would be worse than the bug: the
unconditional DELETE would then remove them every hour and nothing would put
them back.

So a watermark owned by Postgres lives where no copy out of DuckDB reaches it.
The replication and the comparison are untouched: DuckDB's frozen copies of the
two keys keep being copied and compared, agree with themselves, and are read by
nothing. This is also what the chain-2 design recommends for the dirty flag.

Bookkeeping, so `meta`, beside `meta.mirror_state`.
"""
from alembic import op

revision = "0032_chain_watermarks"
down_revision = "0031_manual_expense_ids"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS meta.chain_watermarks (
            key        TEXT PRIMARY KEY,
            value      TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)
    op.execute("""
        COMMENT ON TABLE meta.chain_watermarks IS
        'Sync watermarks owned by a write chain that writes Postgres. Never a '
        'copy of DuckDB sync_metadata: see revision 0032.'
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS meta.chain_watermarks")
