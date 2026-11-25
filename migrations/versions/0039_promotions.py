"""The promotions calendar, phase 1: three tables and a sequence in `app`.

Revision ID: 0039_promotions
Revises: 0038_order_discounts
Created: 2026-11-19

WHY

The revenue forecast cannot know a promotion before it happens. Found in the
daily retail revenue since December 2023, runs of days at 1.8x their weekday
norm and more add up to about 9 % of retail revenue, and Black Friday alone is
27-33 % of its November. The forecast's own calendar is a hard-coded list of
dates, and a promotion's size cannot be known in advance either. So the owner
approved (2026-11-19, the mockup "Календарь акций и рекламы", version 2)
recording what is known before a promotion starts, and keeping every edit as
a version: a later test can then ask what was known on the morning of any
day, and nothing about the past is rewritten by hindsight.

WHAT IT CREATES

- `app.promotion_versions`: one row per saved state of a promotion. The
  current state is the highest `version`; the state as of T is the highest
  version whose `recorded_at` is T or earlier. Cancelling writes a version
  with `status = 'cancelled'`, restoring one with `'planned'`. Nothing
  updates or deletes a row.
- `app.promotion_ids`: the sequence a new promotion takes its id from, so a
  promotion's id is not any one row's.
- `app.promotion_checkins`: the answers to the Monday question in the bot.
  "None" is an answer, and it is kept: a two-week window somebody said had no
  promotion is not a window nobody was asked about.
- `app.promotion_asks`: one row per ISO week the question went out, the
  ledger that keeps a daily job to one message a week
  (`app.weekly_report_sends`' pattern).

ONE WRITER EACH

`core/promotions.py` writes `promotion_versions`, under an advisory lock so
two saves of one promotion cannot both write version N+1 (the UNIQUE key is
the second line of defence). `core/promo_asks.py` holds the statement the
bot's adapter writes a checkin with, and records the ledger. The tests walk
the repository for each.

BOUND

Human-entered: a few dozen promotions a year, a handful of versions each,
so hundreds of rows a year at about 300 bytes. The checkins are at most one
row per recipient per week, and the ledger 52 rows a year. Append-only by
design; the arithmetic is the bound.

THE DOWNGRADE KEEPS THE TABLES

As 0036's, 0037's and 0038's do (owner decision D2), and for their reason:
what a person typed cannot be rebuilt from anything. `alembic downgrade
0038_order_discounts` moves the stamp and executes nothing; an image at 0038
neither reads nor writes these tables, and the next `upgrade head` is a no-op,
every statement here being IF NOT EXISTS.

NO GRANTS

`postgres/initdb/30-app.sql` sets `ALTER DEFAULT PRIVILEGES FOR ROLE ks_app IN
SCHEMA app GRANT SELECT ON TABLES TO ks_readonly`, and migrations run as
`ks_app`, so `ks_readonly` reads the three tables as it reads the rest of
`app.*`. The sequence it does not need.

LOCKS

New relations only: nothing waits on this revision but DDL on their names.
"""
from alembic import op

revision = "0039_promotions"
down_revision = "0038_order_discounts"
branch_labels = None
depends_on = None

STATEMENTS = (
    """
-- A promotion's id, taken once, when its first version is written.
CREATE SEQUENCE IF NOT EXISTS app.promotion_ids AS BIGINT""",
    """
-- One row per saved state of a promotion; the current state is the highest version.
CREATE TABLE IF NOT EXISTS app.promotion_versions (
    version_id    BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    promotion_id  BIGINT       NOT NULL,
    version       INTEGER      NOT NULL CHECK (version >= 1),
    recorded_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    recorded_by   BIGINT,
    source        TEXT         NOT NULL CHECK (source IN ('form', 'form_from_sms')),
    status        TEXT         NOT NULL CHECK (status IN ('planned', 'cancelled')),
    name          TEXT         NOT NULL CHECK (char_length(name) BETWEEN 1 AND 120),
    starts_on     DATE         NOT NULL,
    ends_on       DATE         NOT NULL,
    start_time    TIME,
    kind          TEXT         NOT NULL CHECK (kind IN ('black_friday', 'other')),
    discount_pct  NUMERIC(5,2) CHECK (discount_pct BETWEEN 0 AND 90),
    scope         TEXT         NOT NULL CHECK (scope IN ('store', 'brands', 'categories')),
    scope_items   TEXT[]       NOT NULL DEFAULT '{}',
    channels      TEXT[]       NOT NULL DEFAULT '{}',
    sms_campaign  TEXT,
    note          TEXT         CHECK (char_length(note) <= 500),
    UNIQUE (promotion_id, version),
    -- At most 62 days, the first and the last included.
    CHECK (ends_on >= starts_on AND ends_on - starts_on < 62),
    CHECK (cardinality(scope_items) <= 50 AND array_position(scope_items, NULL) IS NULL),
    CHECK (channels <@ ARRAY['sms', 'instagram', 'telegram', 'site']::TEXT[])
)""",
    """
COMMENT ON TABLE app.promotion_versions IS
  'Every saved state of a planned promotion, as it was known when recorded_at says. The current state is the highest version; as of T, the highest version recorded at T or earlier. Append-only: an edit, a cancel and a restore each write a version. One writer, core.promotions.'""",
    """
COMMENT ON COLUMN app.promotion_versions.recorded_at IS
  'When this state became known: the writer''s clock after it took the lock, never earlier than the previous version''s.'""",
    """
COMMENT ON COLUMN app.promotion_versions.recorded_by IS
  'The Telegram id of the person who saved it (the dashboard signs in through Telegram). Never served: the API says only whether it was the caller.'""",
    """
-- The answers to the Monday question; 'none' is an answer and is kept.
CREATE TABLE IF NOT EXISTS app.promotion_checkins (
    id           BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    period_from  DATE         NOT NULL,
    period_to    DATE         NOT NULL,
    answer       TEXT         NOT NULL CHECK (answer IN ('none')),
    answered_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    answered_by  BIGINT,
    CHECK (period_to >= period_from)
)""",
    """
COMMENT ON TABLE app.promotion_checkins IS
  'The answers to the bot''s Monday question about the next two weeks: none means somebody said no promotion is planned there. Append-only; written by the bot through core.promo_asks.'""",
    """
-- The ISO weeks the Monday question went out: one message a week from a daily job.
CREATE TABLE IF NOT EXISTS app.promotion_asks (
    week_start  DATE         PRIMARY KEY CHECK (extract(isodow FROM week_start) = 1),
    sent_at     TIMESTAMPTZ  NOT NULL,
    delivered   INTEGER      NOT NULL CHECK (delivered >= 0)
)""",
    """
COMMENT ON TABLE app.promotion_asks IS
  'One row per ISO week the bot asked about promotions (week_start is its Monday): what keeps the daily job promo_weekly_ask to one message a week. Insert-only, the first record wins; written by core.promo_asks.'""",
)

# What `upgrade()` creates — the relations the downgrade keeps.
TABLES = ("app.promotion_versions", "app.promotion_checkins", "app.promotion_asks")
SEQUENCES = ("app.promotion_ids",)


def upgrade() -> None:
    for statement in STATEMENTS:
        op.execute(statement)


def downgrade() -> None:
    """Nothing: Alembic moves the stamp to 0038 and the tables stay.

    What a person typed about a promotion cannot be recomputed from anything,
    so a downgrade that dropped these tables would destroy what no upgrade can
    give back. The module docstring has the rest.
    """
