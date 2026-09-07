"""One column: which tabs a person may open.

Revision ID: 0021_user_allowed_features
Revises: 0020_expenses
Created: 2026-09-07

WHY A COLUMN AND NOT A TABLE

`_resolve_session` reads `app.dashboard_users` on **every** request to every
tab — that read is what revocation actually is here, since the session cookie
is a stateless signed token that cannot be withdrawn. A second table would
mean a second read on that same path, or a cache, and a cache on this path is
a revocation that takes effect later than the admin thinks it did. A column
travels in the SELECT that is already being made.

The rule the column carries, in one sentence: **the tab set decides what is
visible, the role decides what can be done inside it** (core/permissions.py,
`apply_feature_override`). So this is not a second permissions system beside
`role_permissions`; it is a per-person narrowing of the first one.

WHY NULL IS NOT AN EMPTY LIST

NULL means "as the role" and is what every one of the 24 existing rows means
the moment this lands: nobody's access changes, and the admin page shows them
as inheriting. An empty string means somebody deliberately ticked nothing —
a real state, reachable from the admin page, and the reason the parser
distinguishes them (`core/permissions.parse_features`).

WHY TEXT AND NOT text[]

DuckDB carries the same column, and `core/repositories/users.py` renders one
SQL text against both engines. An array type exists in both and renders
differently in neither's favour; a comma-separated list of keys from a fixed
vocabulary compares as itself on both sides, and the vocabulary is validated
in Python before it is stored.
"""
from alembic import op

revision = "0021_user_allowed_features"
down_revision = "0020_expenses"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE app.dashboard_users ADD COLUMN IF NOT EXISTS allowed_features TEXT"
    )
    op.execute(
        "COMMENT ON COLUMN app.dashboard_users.allowed_features IS "
        "'Comma-separated tab keys this account may open, or NULL for "
        "whatever the role grants. Vocabulary: core.permissions.TAB_FEATURE_KEYS.'"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE app.dashboard_users DROP COLUMN IF EXISTS allowed_features")
