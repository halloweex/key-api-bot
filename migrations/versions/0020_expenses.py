"""The order-level costs and the dictionary that names them.

Revision ID: 0020_expenses
Revises: 0019_drop_dead_utm_indexes
Created: 2026-09-07

Two tables, and the third the `/expenses` tab needs was already here:
`app.manual_expenses` came with revision 0018, because the ROAS calculation on
`/traffic` had to be able to join it. So half of this tab — the three methods
that read only what a human typed — moves with no migration at all.

MIRRORED, NOT REPLICATED

Both of these come from KeyCRM: `expenses` from `include=expenses` on the order
fetch, `expense_types` from its own dictionary endpoint. That puts them with
`bronze.products` and `bronze.orders` — parsed once in `core/landing_rows.py`,
written twice, and recoverable from the API if this store were ever lost. The
opposite of `app.manual_expenses`, which sits a schema away precisely because
nothing can re-derive a number a human typed.

THE PARSE THAT HAD TO MOVE FIRST

`expense_types` is the first landing table whose parse is a real
transformation rather than a lookup. KeyCRM serves some names as localisation
keys — `dictionaries.expense_types.delivery` — and the display name is built
from the alias in Title Case instead, falling back to the key's own tail. A
mirror that re-read the payload would either repeat that rule or drop it, and
the two stores would then disagree about what an expense is *called* — a
difference the daily comparison would report without being able to say which
store was right. So it moved to `core/landing_rows.py` before either store
gained a second writer, verified row-for-row against the old inline code on six
payloads including both fallbacks.

WHAT THE READS ACTUALLY NEED FROM `orders`

The two summary methods join expenses to raw `orders` and then narrow by
`sales_type` with an `EXISTS` against `silver_orders` — so the only facts they
take from the order are its Kyiv date and its source, both of which
`silver.orders` already carries as columns. The ported bodies read Silver
directly and drop the EXISTS; Silver covers every order (46,272 of 46,272,
verified 2026-08-20), so nothing is excluded that the join admitted.
"""
from alembic import op

revision = "0020_expenses"
down_revision = "0019_drop_dead_utm_indexes"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE bronze.expense_types (
            id          INTEGER PRIMARY KEY,

            -- The *display* name, after the localisation key is resolved —
            -- not the raw payload value. Both stores receive the same
            -- resolved string; see the module docstring.
            name        TEXT NOT NULL,
            alias       TEXT,
            is_active   BOOLEAN,

            -- Bookkeeping, deliberately not shared with DuckDB's `synced_at`:
            -- two correct copies differ on when each of them wrote the row.
            mirrored_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        "COMMENT ON TABLE bronze.expense_types IS "
        "'KeyCRM expense-type dictionary, 27 rows. Mirrored from the same "
        "parsed tuple DuckDB receives.'"
    )

    op.execute(
        """
        CREATE TABLE bronze.expenses (
            id              INTEGER PRIMARY KEY,

            -- No foreign key to bronze.orders, and not for DuckDB's reason
            -- (its FK/UPDATE bug, noted in the DuckDB DDL). Here it is the
            -- mirror's: expenses and orders arrive in separate statements, so
            -- a constraint would make the order of two independent mirrors
            -- load-bearing and turn a transient gap into a failed write.
            order_id        INTEGER NOT NULL,
            expense_type_id INTEGER,

            -- NUMERIC(12,2) against DuckDB's DECIMAL(12,2), revision 0017's
            -- rule: money is never `double precision` in this schema, and a
            -- different scale reports a change the rounding invented.
            amount          NUMERIC(12,2) NOT NULL,
            description     TEXT,
            status          TEXT,
            payment_date    TIMESTAMPTZ,
            created_at      TIMESTAMPTZ,

            mirrored_at     TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    # The reads group by type over a date window, and the window comes from
    # the order — so the join is driven from `silver.orders` and this is the
    # side that gets probed. Unlike revision 0018's two, this index serves a
    # predicate written as itself, with no COALESCE around it.
    op.execute("CREATE INDEX idx_expenses_order ON bronze.expenses (order_id)")
    op.execute(
        "COMMENT ON TABLE bronze.expenses IS "
        "'Order-level costs from KeyCRM include=expenses, ~15k rows. Mirrored "
        "from the same parsed tuple DuckDB receives.'"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS bronze.expenses")
    op.execute("DROP TABLE IF EXISTS bronze.expense_types")
