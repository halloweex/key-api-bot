"""An allocator for `app.stock_movements.id`, for when Postgres becomes the writer.

Revision ID: 0030_stock_movement_ids
Revises: 0029_offers_and_sync_metadata
Created: 2026-09-14

THIS INVERTS AN INVARIANT, WHICH IS WHY IT IS ITS OWN REVISION

Revision 0008 says, at length, that `stock_movements.id` is **carried from
DuckDB and not generated here**, because "a Postgres sequence would give the
same movement two names and the daily comparison would be meaningless before
it began". That is exactly right — *while DuckDB is the writer*.

Stage 4 makes Postgres the writer of this chain, and then the same argument
points the other way: the store that writes has to be the store that names,
and a movement allocated in DuckDB would be the second name.

So the rule is not being abandoned, it is being restated with its condition
made explicit:

    exactly one store writes `stock_movements`, and that store allocates the id.

`KS_WRITE_INVENTORY` is what makes "exactly one" true. Under `duckdb` (the
default) the replication keeps shipping explicit ids and this sequence is never
consulted; under `postgres` the replication stands down for this table and the
sequence is the only allocator. The dangerous state is both at once, which is
why the flag gates the shipper and not just the writer.

WHY THE `setval` HERE IS NOT SUFFICIENT, AND WHAT IS

The sequence must begin above every id DuckDB ever allocated — 55,721 over
55,375 rows on production at the time of writing, the gap being DuckDB's own
sequence gaps. `START WITH` takes a constant, so the value is read rather than
typed; a literal would be stale the moment it was written.

But a migration runs against an **empty** table. On a fresh database the rows
arrive afterwards, carried from DuckDB by `replicate_operational`, and this
`setval` has already left the sequence at 1 — so the first allocated id
collides with a row that is now there. The verification harness caught exactly
that on its first run, which is worth recording because the migration reads as
if it were enough.

What actually guarantees the floor is `_ensure_movement_id_floor` in
`core/pg_inventory_write.py`: idempotent, monotone, and run inside the writing
transaction whenever there are movements to write. The `setval` below stays
because it is correct for the case it can see, and because a sequence created
without one starts at 1 silently rather than obviously.

THE DEFAULT IS SAFE TO ADD BEFORE THE WRITER EXISTS

`replicate_operational` names `id` in its INSERT, so the default is not reached
while DuckDB is still the writer. Adding it now rather than at the cutover
means the cutover is a flag and not a migration — which matters, because the
cutover is the step that has to be reversible in a hurry.
"""
from alembic import op


revision = "0030_stock_movement_ids"
down_revision = "0029_offers_and_sync_metadata"
branch_labels = None
depends_on = None

SEQUENCE = "app.stock_movements_id_seq"


def upgrade() -> None:
    op.execute(f"CREATE SEQUENCE IF NOT EXISTS {SEQUENCE}")
    # Above every id DuckDB has allocated. Read, not typed — see the docstring.
    op.execute(f"""
        SELECT setval(
            '{SEQUENCE}',
            COALESCE((SELECT MAX(id) FROM app.stock_movements), 0) + 1,
            false
        )
    """)
    # Owned by the column, so a future DROP TABLE takes the sequence with it
    # rather than leaving an orphan that a later CREATE would collide with.
    op.execute(f"ALTER SEQUENCE {SEQUENCE} OWNED BY app.stock_movements.id")
    op.execute(
        f"ALTER TABLE app.stock_movements "
        f"ALTER COLUMN id SET DEFAULT nextval('{SEQUENCE}')"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE app.stock_movements ALTER COLUMN id DROP DEFAULT")
    op.execute(f"DROP SEQUENCE IF EXISTS {SEQUENCE}")
