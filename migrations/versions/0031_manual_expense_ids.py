"""An allocator for `app.manual_expenses.id`, for when Postgres becomes the writer.

Revision ID: 0031_manual_expense_ids
Revises: 0030_stock_movement_ids
Created: 2026-09-16

Chain 8 of stage 4, and the same inversion revision 0030 made for stock
movements. `manual_expenses` is replicated out of DuckDB as a full replace, so
its `id` has always been CARRIED — and Postgres was built to receive it:
measured on production before writing this, the column has no default and no
owned sequence. The rule is 0030's, restated with its condition:

    exactly one store writes `manual_expenses`, and that store allocates the id.

`KS_WRITE_EXPENSES` makes "exactly one" true. Under `duckdb` the replication
names `id` in its INSERT and this sequence is never consulted; under `postgres`
the replication stands down for this table and the sequence is the only
allocator.

THE `setval` IS NOT THE FLOOR, FOR 0030'S REASON

A migration runs against the table as it is at that moment, and rows can arrive
afterwards, carried from DuckDB with their own ids. Production holds zero rows
today, which makes the seed below 1 — and the first expense typed before the
switch would then be shipped in with id 1, colliding with the first id this
sequence hands out. `_ensure_expense_id_floor` in `core/pg_expenses_write.py`
is what guarantees the floor, inside the writing transaction; the `setval` stays
because it is right for what it can see and a bare sequence starts at 1 silently.

THE DEFAULT IS SAFE TO ADD BEFORE THE WRITER EXISTS

The replication names `id`, so the default is unreachable while DuckDB writes.
Adding it now makes the cutover a flag rather than a migration — and the cutover
is the step that must be reversible in a hurry.

`created_at` and `currency` get no defaults here, deliberately: DuckDB defaults
them and the writer supplies both explicitly, which is what chain 1 did for
`stock_movements.recorded_at` and `.source`. A default added now would also
change what a row shipped with a NULL in those columns reads as.
"""
from alembic import op

revision = "0031_manual_expense_ids"
down_revision = "0030_stock_movement_ids"
branch_labels = None
depends_on = None

SEQUENCE = "app.manual_expenses_id_seq"


def upgrade() -> None:
    op.execute(f"CREATE SEQUENCE IF NOT EXISTS {SEQUENCE}")
    op.execute(f"""
        SELECT setval(
            '{SEQUENCE}',
            COALESCE((SELECT MAX(id) FROM app.manual_expenses), 0) + 1,
            false
        )
    """)
    # Owned by the column, so a future DROP TABLE takes the sequence with it.
    op.execute(f"ALTER SEQUENCE {SEQUENCE} OWNED BY app.manual_expenses.id")
    op.execute(
        f"ALTER TABLE app.manual_expenses "
        f"ALTER COLUMN id SET DEFAULT nextval('{SEQUENCE}')"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE app.manual_expenses ALTER COLUMN id DROP DEFAULT")
    op.execute(f"DROP SEQUENCE IF EXISTS {SEQUENCE}")
