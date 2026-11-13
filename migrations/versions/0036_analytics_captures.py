"""The analysts' captures: eleven tables in `app`, and a downgrade that keeps them.

Revision ID: 0036_analytics_captures
Revises: 0035_batch_e_chains
Created: 2026-11-13

WHAT IT CREATES, AND WHY NOW

Seven histories are destroyed on write today, and each loses a day for every
day it is not captured (the analysts' warehouse design, revision 3.1, §3.3):

- `offer_cost_daily` — `bronze.offer_stocks.purchased_price` is overwritten;
- `catalogue_versions` — a product's brand and category, edited in place;
- `revenue_prediction_vintages` — the stored forecast, replaced by every
  training;
- `revenue_goal_history` — `app.revenue_goals` keeps one row per period;
- `order_channel_verdicts` — `silver.order_utm` is upserted, and a reclassify
  overwrites every verdict (owner decision D12: this log lands before one);
- `expense_deletions` — chain 3 deletes what KeyCRM stopped listing (O2 (a));
- `ga4_funnel` — GA4 keeps a funnel for fourteen months.

And four key tables: `analytics_buyer_key`, and the person key's three
(`analytics_person_key`, `analytics_phone_excluded`, `analytics_person_merges`).
Owner decision D3 is yes, but only the tables come here: their writer comes
with the warehouse job in phase 2, and nothing in this build writes them
(`tests/unit/test_write_chains.py`, `TestTheCaptureTables`).

Each capture has one writer, named in `core.analytics_captures.CAPTURES` and
held to it by a walk of the repository. A capture that rides another writer's
transaction runs under a SAVEPOINT of its own, so it can never roll that writer
back; `KS_ANALYTICS_CAPTURES=off` stops every one of them.

The statements are the design's §3.6 verbatim, tested there twice in a row on
postgres:17.2 (`chdwh/rev3/out/t07_pg.out`, T1), but for the SQL comment over
the vintages, which said "every stored forecast as it was shown" and was
corrected: the forecast a past day was shown last is never replaced, so it
stays in `app.revenue_predictions` and never reaches the vintages. Every
statement is IF NOT EXISTS.

WHY THE DOWNGRADE DROPS NOTHING (owner decision D2)

Until Z3 the last image before step 14, `rollback-step14`, is a rollback
target, and it requires 0035. `require_revision` compares the stamp and
nothing else, so what that image needs is the stamp: `alembic downgrade
0035_batch_e_chains`, run with this revision's migrate image (an older one
does not know 0036), moves `meta.alembic_version` and executes nothing. The
eleven tables and every row in them stay. An image at 0035 neither reads nor
writes them, the captures stop while it runs (a gap in each history, the cost
D2 accepted), and the next `alembic upgrade head` is a no-op because every
statement here is IF NOT EXISTS.

Dropping them would be the one downgrade that destroys what it cannot give
back: each capture is the only record there will ever be of what it holds.
`tests/unit/test_analytics_captures_revision.py` pins the empty downgrade, and
`tests/integration/test_analytics_captures_pg.py` runs it on a server.

From this revision on, any rollback below it needs that downgrade first,
before the retag — every image before this one requires 0035, the previous
version and `rollback-step14` alike, and a revert's deploy builds the same
older migrate image. Its own migrate image cannot locate 0036 and its
`upgrade head` fails, so web and bot, which wait for migrate to succeed, are
stopped and not started again; and its `require_revision` refuses a database
at 0036. Measured 2026-11-13 with origin/main's migrate tree (CLAUDE.md,
"Rolling back below a revision", which also has the recovery when the move
was missed). Left new, keycrm-migrate re-runs `upgrade head` on `up -d` and
puts the stamp back.

NO GRANTS

`postgres/initdb/30-app.sql` sets `ALTER DEFAULT PRIVILEGES FOR ROLE ks_app IN
SCHEMA app GRANT SELECT ON TABLES TO ks_readonly`, and migrations run as
`ks_app`, so `ks_readonly` reads every table here as it reads the rest of
`app.*` (revision 0025 says the same). It gains nothing on the sequences.

LOCKS

New tables and their own indexes only: no statement touches an existing
table, so nothing waits on this revision but DDL on these names.
"""
from alembic import op

revision = "0036_analytics_captures"
down_revision = "0035_batch_e_chains"
branch_labels = None
depends_on = None

# The design's §3.6, verbatim, one statement each, in its order.
STATEMENTS = (
    """
-- 1. Pseudonymous keys. Random, stored, never derived: no secret to leak through view text or a query
--    log, covered by PITR, stable across loads. The map is as sensitive as bronze.buyers and never leaves.
CREATE TABLE IF NOT EXISTS app.analytics_buyer_key (
    buyer_id   INTEGER     PRIMARY KEY,
    buyer_key  BIGINT      NOT NULL UNIQUE
               DEFAULT (('x' || substr(replace(gen_random_uuid()::text, '-', ''), 1, 16))::bit(64)::bigint
                        & 9223372036854775807),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
)""",
    """
COMMENT ON TABLE app.analytics_buyer_key IS
  'buyer_id -> random 63-bit key shipped to the analysts'' warehouse instead of buyer_id. Never shipped itself.'""",
    """
-- Owner decision D3 (person key). Created empty unless the owner says yes; the job fills it.
CREATE TABLE IF NOT EXISTS app.analytics_person_key (
    buyer_id    INTEGER     PRIMARY KEY,
    person_key  BIGINT      NOT NULL,     -- the buyer_key of the cluster's earliest buyer record
    rule        TEXT        NOT NULL,     -- 'phone9': records sharing the last 9 digits of a phone
    assigned_at TIMESTAMPTZ NOT NULL DEFAULT now()
)""",
    """
-- Numbers the person key never links through: shared by more than PHONE_MAX_RECORDS (3) records, a
-- known placeholder, or not a Ukrainian mobile. Written by the job each run; read by the analyst guide.
CREATE TABLE IF NOT EXISTS app.analytics_phone_excluded (
    phone9      TEXT        PRIMARY KEY,
    records     INTEGER     NOT NULL,
    reason      TEXT        NOT NULL,     -- shared | placeholder | invalid
    seen_at     TIMESTAMPTZ NOT NULL DEFAULT now()
)""",
    """
CREATE TABLE IF NOT EXISTS app.analytics_person_merges (
    id              BIGSERIAL   PRIMARY KEY,
    from_person_key BIGINT      NOT NULL,
    to_person_key   BIGINT      NOT NULL,
    merged_at       TIMESTAMPTZ NOT NULL DEFAULT now()
)""",
    """
-- 2. Cost and price per offer per Kyiv day, written in the 01:00 inventory snapshot's transaction.
--    The only cost history there will ever be: offer_stocks.purchased_price is overwritten.
CREATE TABLE IF NOT EXISTS app.offer_cost_daily (
    date            DATE          NOT NULL,
    offer_id        INTEGER       NOT NULL,
    sku             TEXT,
    purchased_price NUMERIC(12,2),
    price           NUMERIC(12,2),
    PRIMARY KEY (date, offer_id)
)""",
    """
-- 3. Catalogue versions (SCD2 by content diff: "a version is a change, not an observation").
CREATE TABLE IF NOT EXISTS app.catalogue_versions (
    id          BIGSERIAL   PRIMARY KEY,
    product_id  INTEGER     NOT NULL,
    known_from  TIMESTAMPTZ NOT NULL,          -- the capture that first saw this content
    name        TEXT,
    brand       TEXT,
    category_id INTEGER,
    row_hash    BIGINT      NOT NULL,          -- 64-bit md5 prefix of the canonical row
    is_deleted  BOOLEAN     NOT NULL DEFAULT false,
    UNIQUE (product_id, known_from)
)""",
    """
-- 4. Every stored forecast a training replaced (pg_forecast_write replaces today on: DELETE + INSERT).
--    The forecast a past day was shown last is never replaced and stays in app.revenue_predictions;
--    the history of what was shown is this table with that one's rows dated before today.
--    made_at is the replaced rows' created_at, which app.revenue_predictions does NOT declare NOT NULL
--    (0025): a NULL there must not fail the forecast write, so made_at is nullable and captured_at is
--    the capture's own clock. No UNIQUE: two replacements of unstamped rows are two facts.
CREATE TABLE IF NOT EXISTS app.revenue_prediction_vintages (
    id                BIGSERIAL     PRIMARY KEY,
    made_at           TIMESTAMPTZ,                 -- the replaced rows' created_at (NULL if never stamped)
    captured_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),
    prediction_date   DATE          NOT NULL,
    sales_type        VARCHAR(32)   NOT NULL,
    predicted_revenue NUMERIC(12,2),
    model_mae         NUMERIC(10,2),
    model_mape        NUMERIC(6,2),
    model_wape        NUMERIC(6,2)
)""",
    """
CREATE INDEX IF NOT EXISTS idx_revenue_prediction_vintages ON app.revenue_prediction_vintages (sales_type, prediction_date)""",
    """
-- 5. Every goal ever set (app.revenue_goals keeps one row per period_type, upserted).
CREATE TABLE IF NOT EXISTS app.revenue_goal_history (
    id              BIGSERIAL     PRIMARY KEY,
    period_type     TEXT          NOT NULL,
    goal_amount     NUMERIC(12,2) NOT NULL,
    is_custom       BOOLEAN,
    calculated_goal NUMERIC(12,2),
    growth_factor   NUMERIC(4,2),
    change_kind     TEXT          NOT NULL,      -- set | recalculated | reset_to_auto
    set_at          TIMESTAMPTZ   NOT NULL DEFAULT now()
)""",
    """
-- 6. Channel verdicts, appended when an order's verdict changes (silver.order_utm is upserted and a
--    full parse DELETEs). rule_version names the classifier's rules; it changes with the golden file.
CREATE TABLE IF NOT EXISTS app.order_channel_verdicts (
    id           BIGSERIAL   PRIMARY KEY,
    order_id     INTEGER     NOT NULL,
    known_from   TIMESTAMPTZ NOT NULL,
    rule_version TEXT        NOT NULL,
    change_kind  TEXT        NOT NULL,           -- first | change | reclassify
    traffic_type TEXT,
    platform     TEXT,
    utm_source   TEXT,
    utm_medium   TEXT,
    utm_campaign TEXT,
    row_hash     BIGINT      NOT NULL
)""",
    """
CREATE INDEX IF NOT EXISTS idx_order_channel_verdicts_order ON app.order_channel_verdicts (order_id, known_from)""",
    """
-- 7. Expenses KeyCRM stopped listing, logged at the moment chain 3 deletes them (O2 (a)).
CREATE TABLE IF NOT EXISTS app.expense_deletions (
    expense_id      INTEGER       NOT NULL,
    order_id        INTEGER       NOT NULL,
    expense_type_id INTEGER,
    amount          NUMERIC(12,2) NOT NULL,
    deleted_at      TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (expense_id, deleted_at)
)""",
    """
-- 8. The GA4 site funnel per CLOSED period, captured once per period and never summed across grains
--    (GA4 counts a person once per range). Backfilled oldest-first for everything inside GA4's 14 months.
CREATE TABLE IF NOT EXISTS app.ga4_funnel (
    grain           TEXT        NOT NULL CHECK (grain IN ('day', 'week', 'month')),
    period_start    DATE        NOT NULL,
    period_end      DATE        NOT NULL,
    country_scope   TEXT        NOT NULL CHECK (country_scope IN ('ua', 'all')),
    breakdown       TEXT        NOT NULL CHECK (breakdown IN ('total', 'channel_group', 'device')),
    breakdown_value TEXT        NOT NULL,        -- '' for the total
    traffic_kind    TEXT        NOT NULL DEFAULT '',  -- cold | warm | other for a channel group, AS DECIDED AT CAPTURE
    traffic_rule    TEXT        NOT NULL DEFAULT '',  -- the version of that rule (ga4_funnel_service's cold/warm table)
    step            SMALLINT    NOT NULL CHECK (step BETWEEN 1 AND 5),
    active_users    INTEGER     NOT NULL,
    captured_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (grain, period_start, country_scope, breakdown, breakdown_value, step)
)""",
)

# What `upgrade()` creates, in order — the eleven the downgrade keeps.
TABLES = (
    "app.analytics_buyer_key", "app.analytics_person_key",
    "app.analytics_phone_excluded", "app.analytics_person_merges",
    "app.offer_cost_daily", "app.catalogue_versions",
    "app.revenue_prediction_vintages", "app.revenue_goal_history",
    "app.order_channel_verdicts", "app.expense_deletions", "app.ga4_funnel",
)


def upgrade() -> None:
    for statement in STATEMENTS:
        op.execute(statement)


def downgrade() -> None:
    """Nothing: Alembic moves the stamp to 0035 and every table stays.

    Owner decision D2: until Z3 `rollback-step14` is a rollback target, and it
    requires 0035 by the stamp alone. The tables hold the only record of what
    they capture, so a downgrade that dropped them would be the one step back
    that cannot be undone. The module docstring has the rest.
    """
