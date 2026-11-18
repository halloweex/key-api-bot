"""The month's automatic goal, frozen on its first day: one table in `app`.

Revision ID: 0037_frozen_month_goals
Revises: 0036_analytics_captures
Created: 2026-11-18

WHAT IT CREATES, AND WHY

`GET /api/goals/smart` computed the month's automatic goal on every request
(`GoalsMixin.generate_smart_goals`), and 30-40 % of it is the ML signal: the
month's actual revenue so far plus the stored forecast of the rest, which
drifts toward the actual as the month goes. Measured: the goal moved within a
month by a median 4-6 %, and in July 2026 by about ₴0.5M. A past month's goal
changed after the fact as well, because "the last three complete months" are
counted from today and the ML term is 0 for a month that has ended.

The owner's decision: freeze the month's automatic goal on the 1st. This
revision adds the one table that holds it:

- `app.frozen_month_goals` — a row per (year, month, sales_type): the answer
  `generate_smart_goals` gave, whole, as JSONB, and when it was frozen.

One writer, `core.goal_freeze` (job `month_goal_freeze`, daily 00:30 Kyiv and
once a few minutes after a start), insert-only: the first freeze of a month
wins (`ON CONFLICT DO NOTHING`), nothing updates or deletes a row. The reader
is `GoalsMixin.smart_goals_of_month`: the frozen answer when the month has a
row, the live computation as before when it has none. A goal typed on /goals
(`app.revenue_goals.is_custom`) still overrides either. A GET never writes it.

BOUND

About one row per month per sales type: `KNOWN_SALES_TYPES` and `all` are six
today, so 72 rows a year, each a JSON document of about 1.5 KB — under 150 KB
a year. Append-only by design; the rows are the record of which goal each
month was measured against.

WHY THE DOWNGRADE DROPS NOTHING

The rows cannot be rebuilt: recomputing a month later gives a different
number, which is the drift this revision exists to stop. So, like 0036 (owner
decision D2), `alembic downgrade 0036_analytics_captures` moves the stamp and
executes nothing; the table and its rows stay. An image at 0036 neither reads
nor writes it and computes every goal live while it runs, and the next
`alembic upgrade head` is a no-op, because every statement here is
idempotent. `tests/unit/test_goal_freeze_revision.py` pins the empty
downgrade, and `tests/integration/test_goal_freeze_pg.py` runs it on a server.

NO GRANTS

`postgres/initdb/30-app.sql` sets `ALTER DEFAULT PRIVILEGES FOR ROLE ks_app IN
SCHEMA app GRANT SELECT ON TABLES TO ks_readonly`, and migrations run as
`ks_app`, so `ks_readonly` reads this table as it reads the rest of `app.*`.

LOCKS

A new table and its primary key only: nothing waits on this revision but DDL
on that name.
"""
from alembic import op

revision = "0037_frozen_month_goals"
down_revision = "0036_analytics_captures"
branch_labels = None
depends_on = None

STATEMENTS = (
    """
-- The month's automatic goal per sales type, as generate_smart_goals answered it when the month was
-- frozen (its 1st, or the deploy day for the month 0037 shipped in). Insert-only: the first freeze wins.
CREATE TABLE IF NOT EXISTS app.frozen_month_goals (
    year        SMALLINT     NOT NULL,
    month       SMALLINT     NOT NULL CHECK (month BETWEEN 1 AND 12),
    sales_type  VARCHAR(32)  NOT NULL,
    smart       JSONB        NOT NULL,     -- generate_smart_goals' answer, whole
    frozen_at   TIMESTAMPTZ  NOT NULL,     -- the writer's clock, the one that chose the month
    PRIMARY KEY (year, month, sales_type)
)""",
    """
COMMENT ON TABLE app.frozen_month_goals IS
  'Each month''s automatic goal per sales type, frozen on the month''s first day: what /api/goals/smart and /api/goals/forecast answer for that month. One writer, core.goal_freeze (job month_goal_freeze); insert-only, the first freeze wins.'""",
)

# What `upgrade()` creates — the one table the downgrade keeps.
TABLES = ("app.frozen_month_goals",)


def upgrade() -> None:
    for statement in STATEMENTS:
        op.execute(statement)


def downgrade() -> None:
    """Nothing: Alembic moves the stamp to 0036 and the table stays.

    A frozen goal cannot be recomputed — a month computed later is another
    number — so a downgrade that dropped the table would destroy what no
    upgrade can give back. The module docstring has the rest.
    """
