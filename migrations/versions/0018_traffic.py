"""The UTM classification and the ad spend, so `/traffic` can read Postgres.

Revision ID: 0018_traffic
Revises: 0017_revenue_goals
Created: 2026-09-07

Two tables, and the interesting part of this revision is the third one that is
**not** here.

WHY `gold.daily_traffic` IS ABSENT

DuckDB has a traffic Gold, rebuilt on every warehouse tick, and the obvious
move was to derive it here as `core/pg_gold.py` derives the revenue Gold. It
is not needed, and the reason is arithmetic rather than taste.

Both queries that read that Gold fold it: `SUM(orders_count)`, `SUM(revenue)`
grouped by a *subset* of its own primary key. `silver_order_utm` is keyed on
`order_id`, so it is strictly 1:1 with the order, and every order therefore
lands in exactly one Gold cell. A fold of cells is then the same arithmetic as
the aggregate over the rows underneath — `SUM(orders_count)` over the cells is
`COUNT(DISTINCT s.id)` over the join, with no double counting available.

That is identical **by construction**, not to the kopeck by measurement, which
is the stronger of the two claims `/marketing` had to choose between when its
brand table read the line level instead of `gold_daily_products`. So the reads
go to `silver.orders LEFT JOIN silver.order_utm` in both engines — one body,
rule 1 — and this store gains no fourth derived layer, no deriver inside the
Gold tick, and no comparison to keep it honest. A layer that does not exist
cannot go stale in one store and not the other.

The visible consequence is that DuckDB's `gold_daily_traffic` is left with no
reader. It is still rebuilt every warehouse tick. Whether to stop maintaining
it is the owner's call and not this revision's business.

WHY `silver.order_utm` IS REPLICATED AND NOT DERIVED

It is Silver, and every other Silver object here is computed from what this
store already holds — `silver.orders` from `bronze.orders`, `silver.order_lines`
a view whose body is the same text DuckDB uses. This one cannot be, because its
body is not SQL: `refresh_utm_silver_layer` parses UTM parameters and pixel
cookies out of `manager_comment` in Python and classifies each order with
`_classify_traffic`. Deriving it here would mean a second implementation of
that classifier, in a second language, drifting from the first the day either
is touched. `app.manager_classifications` made the same choice for the same
reason, one layer down.

So it is a real table, shipped whole from DuckDB, and — unlike the `app.*`
family — it is shipped on the **Silver tick** rather than the hourly
operational one. A stale UTM row does not read as missing data; it reads as a
different answer, moving an order from `paid_confirmed` to `organic` and
changing a number on a chart. Silver and its classification therefore land
together, which is what DuckDB already does one store over.

WHY `app.manual_expenses` IS HERE AT ALL

It holds **zero rows in production**, measured on the 2026-09-07 snapshot, so
the ROAS calculation's spend half has never had an input: `has_spend_data` is
false and `blended_roas` is null on every request. The table is created anyway
because a query cannot join a table that is not there, and the endpoint has to
return the same "no spend data" answer from either engine rather than an error
from one of them.

It is `app`, not `bronze`: an amount a human typed into the expenses form, that
KeyCRM has never heard of and no rebuild can produce — revision 0008's family,
and it rides `replicate_operational` with the rest of it.
"""
from alembic import op

revision = "0018_traffic"
down_revision = "0017_revenue_goals"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE silver.order_utm (
            -- 1:1 with the order, and load-bearing: it is what makes a fold of
            -- the traffic cells equal the aggregate over the orders, which is
            -- why this store needs no traffic Gold. A second row per order
            -- would silently double `orders_count` on every chart.
            order_id      INTEGER PRIMARY KEY,

            -- TEXT throughout, for revision 0009's reason: DuckDB's VARCHAR(n)
            -- limits are not repeated here, because a length limit the other
            -- store does not have turns the comparison into a filter and a
            -- truncated row reads as a difference the mirror invented.
            utm_source    TEXT,
            utm_medium    TEXT,
            utm_campaign  TEXT,
            utm_content   TEXT,
            utm_term      TEXT,
            utm_lang      TEXT,

            -- The pixel cookies. Evidence for the classification below, and
            -- rendered per transaction on the tab, so they are carried rather
            -- than dropped once `traffic_type` has been decided.
            fbp           TEXT,
            fbc           TEXT,
            ttp           TEXT,
            fbclid        TEXT,

            -- The classification itself: paid_confirmed | paid_likely |
            -- organic | pixel_only | unknown, and the platform it came from.
            -- Decided in Python by `_classify_traffic`; this store copies the
            -- verdict and never re-derives it.
            traffic_type  TEXT,
            platform      TEXT,

            -- DuckDB's own stamp, carried across rather than regenerated: it
            -- says when the parser last looked at the order, and a fresh
            -- `now()` here would erase that. Not compared for the same reason
            -- `mirrored_at` is not — but this one is not bookkeeping, it is
            -- a fact about the parse.
            parsed_at     TIMESTAMPTZ,

            mirrored_at   TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    # The two the reads filter and group on. `order_date` lives on
    # `silver.orders`, so the join drives from there and these carry the rest.
    op.execute("CREATE INDEX idx_order_utm_platform ON silver.order_utm (platform)")
    op.execute(
        "CREATE INDEX idx_order_utm_traffic_type ON silver.order_utm (traffic_type)"
    )
    op.execute(
        "COMMENT ON TABLE silver.order_utm IS "
        "'UTM parameters and the traffic classification, parsed in Python and "
        "shipped whole from DuckDB on the Silver tick. Not derived here: the "
        "parser is not SQL.'"
    )

    op.execute(
        """
        CREATE TABLE app.manual_expenses (
            -- Carried from DuckDB, never generated here — `stock_movements`'
            -- rule (revision 0008): a sequence of our own would give the same
            -- expense two names and the comparison would be meaningless
            -- before it began. So no GENERATED, no DEFAULT, no sequence.
            id            INTEGER PRIMARY KEY,

            expense_date  DATE NOT NULL,
            category      TEXT NOT NULL,
            expense_type  TEXT NOT NULL,

            -- NUMERIC(12,2) against DuckDB's DECIMAL(12,2), revision 0017's
            -- note: money is never `double precision` in this schema, and a
            -- different scale reports a change the rounding invented.
            amount        NUMERIC(12,2) NOT NULL,
            currency      TEXT,
            note          TEXT,
            created_at    TIMESTAMPTZ,
            updated_at    TIMESTAMPTZ,

            -- Added by a later DuckDB migration than the table itself
            -- (`core/migrations.py` backfills it from `expense_type`), which
            -- is why it is last here too: the column order follows DuckDB's,
            -- and the replication reads by name in that order.
            platform      TEXT,

            mirrored_at   TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        "CREATE INDEX idx_manual_expenses_date ON app.manual_expenses (expense_date)"
    )
    op.execute(
        "COMMENT ON TABLE app.manual_expenses IS "
        "'Ad spend and other costs a human typed. Irreplaceable — KeyCRM has "
        "no such record — and empty in production as of 2026-09-07.'"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS app.manual_expenses")
    op.execute("DROP TABLE IF EXISTS silver.order_utm")
