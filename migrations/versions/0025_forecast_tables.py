"""The Postgres twins of the four forecast tables.

Revision ID: 0025_forecast_tables
Revises: 0024_buyer_gender
Created: 2026-09-13

WHY THESE FOUR, AND WHY FIRST

Sixteen tables in DuckDB have no Postgres counterpart at all, and they are what
the rest of the migration is waiting on. These four are the smallest slice that
unblocks something concrete: `get_predictions` and `generate_smart_goals` are
the last two reads of the whole dashboard still tied to DuckDB, and they are
tied to it *only* because these tables are not here. 313 rows between them.

REPLICATED, NOT DERIVED — AND THAT IS THE INTERESTING HALF

Three of them could in principle be recomputed here: `seasonal_indices`,
`weekly_patterns` and `growth_metrics` are aggregates over orders, and Postgres
has the orders. Deriving them would be a second implementation of
`calculate_seasonality_indices` and its two siblings, and the moment either
copy is edited the two answers part company — `app.manager_classifications`'
argument exactly, and the Silver projection's before it. `core/pg_operational.py`
copies what DuckDB computed.

`revenue_predictions` is not even arguably derivable: it records what the model
predicted **on a given day**, and the model is retrained daily. Re-deriving it
tomorrow answers a different question.

WHY SCHEMA `app`

`bronze` is landing from KeyCRM and nothing here comes from a payload —
KeyCRM has no opinion about seasonality. `gold` is a derived reading layer that
one migration could put back; these cannot be put back, because the model that
made them has moved on. `app` is where what nothing can decide again lives.

SHAPES, AND WHY THEY DIFFER

`revenue_predictions` is a full replace because its DuckDB writer is a
`DELETE` followed by an `INSERT` — the same operation, so no decision is being
taken. The other three are written with `ON CONFLICT DO UPDATE` over a fixed
key space: twelve months, sixty month-weeks, one metric type. Replacing them
whole *is* a decision there, and it is taken deliberately — the space is fixed
and complete, so a whole replacement is the same set of rows, and it also
corrects a row that is present and wrong. An upsert would leave a stale one standing for ever.

NO GRANTS, NO DEFAULTS ON THE TIMESTAMPS

`initdb` already grants `ks_readonly` SELECT on everything `ks_app` creates in
this schema, and `ks_app` owns what it creates — see the note in `upgrade()`.



`updated_at` and `created_at` carry no `DEFAULT now()` here, unlike their
DuckDB originals. The value being copied is the one DuckDB stamped; a default
would quietly substitute the replication's clock for the model's, and the
column would then answer "when was this copied" while being read as "when was
this computed".
"""
from alembic import op


revision = "0025_forecast_tables"
down_revision = "0024_buyer_gender"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS app.revenue_predictions (
            prediction_date   DATE           NOT NULL,
            sales_type        VARCHAR(32)    NOT NULL,
            predicted_revenue NUMERIC(12, 2),
            model_mae         NUMERIC(10, 2),
            model_mape        NUMERIC(6, 2),
            model_wape        NUMERIC(6, 2),
            created_at        TIMESTAMPTZ,
            PRIMARY KEY (prediction_date, sales_type)
        )
    """)
    op.execute("""
        CREATE TABLE IF NOT EXISTS app.seasonal_indices (
            month             INTEGER        PRIMARY KEY,
            seasonality_index NUMERIC(6, 4),
            sample_size       INTEGER,
            avg_revenue       NUMERIC(12, 2),
            min_revenue       NUMERIC(12, 2),
            max_revenue       NUMERIC(12, 2),
            yoy_growth        NUMERIC(6, 4),
            confidence        VARCHAR(10),
            updated_at        TIMESTAMPTZ
        )
    """)
    op.execute("""
        CREATE TABLE IF NOT EXISTS app.weekly_patterns (
            month          INTEGER NOT NULL,
            week_of_month  INTEGER NOT NULL,
            weight         NUMERIC(6, 4),
            sample_size    INTEGER,
            updated_at     TIMESTAMPTZ,
            PRIMARY KEY (month, week_of_month)
        )
    """)
    op.execute("""
        CREATE TABLE IF NOT EXISTS app.growth_metrics (
            metric_type   VARCHAR(20) PRIMARY KEY,
            value         NUMERIC(8, 4),
            period_start  DATE,
            period_end    DATE,
            sample_size   INTEGER,
            updated_at    TIMESTAMPTZ
        )
    """)

    # The forecast series is read by date and by sales type; the other three are
    # small enough that their primary keys are the whole access path.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_revenue_predictions_date
            ON app.revenue_predictions (prediction_date)
    """)

    # NO GRANTS HERE, AND THAT IS NOT AN OVERSIGHT
    #
    # The first version of this revision granted to `ks_app`, which owns what
    # it creates — migrations run as that role, deliberately. And `ks_readonly`
    # needs nothing either: `postgres/initdb/30-app.sql` sets
    # `ALTER DEFAULT PRIVILEGES FOR ROLE ks_app IN SCHEMA app GRANT SELECT ON
    # TABLES TO ks_readonly`, so every table created here is readable by it
    # already. None of the twenty-four revisions before this one grants
    # anything; a GRANT would imply a privilege boundary that does not exist.


def downgrade() -> None:
    for table in ("growth_metrics", "weekly_patterns",
                  "seasonal_indices", "revenue_predictions"):
        op.execute(f"DROP TABLE IF EXISTS app.{table}")
