"""Step 2 of «Одна бронза»: the buyer landing, the line level, and the витрина.

Revision ID: 0011_buyers_lines_vitrina
Revises: 0010_order_versions
Created: 2026-08-28

Three additions, one step. The page's step 2 reads "витрина: строка на
клиента", and its own caveat is why this migration is three things: the
Postgres silver was one table, `orders`, and a customer row cannot be built
from it alone. So the enrichment lands first — the buyer landing and the
order-lines level — and the витрина is the small table at the end.

BUYERS ARE LANDING, NOT DERIVATION

`upsert_buyers` writes what the KeyCRM buyers API returned — one parsed batch
of Buyer models. That makes `bronze.buyers` a mirror in the exact sense of
`bronze.products`: the same parsed objects handed to two stores in the same
call (`core/pg_buyers.py`). The sync is a *delta* by construction — it only
ever fetches buyers referenced by orders and missing from the table — so the
mirror follows the orders pattern, not the catalogue one: ship what was
written, backfill history by an ids-diff with no cursor, gate the zero-
tolerance comparison on `backfilled_at`.

`bronze.buyer_contacts` drops DuckDB's sequence id on purpose. The natural
key `(buyer_id, contact_type, value)` is UNIQUE on the DuckDB side already,
and a sequence-generated id would give the same contact two names — the
`stock_movements` lesson, avoided this time by not having the column at all.

THE LINE LEVEL IS A VIEW, BECAUSE IT IS ONE IN DUCKDB

`silver_order_lines` has never been a table anywhere: it is a projection over
tables this schema already holds (bronze.order_products, silver.orders, the
catalogue). The body lives in `core.sql_dialect.order_lines_select` — one
body, two engines, charter rule 1 — and the text below is its Postgres
rendering, frozen the way a migration freezes things. A test re-renders the
template and asserts this file agrees, the same contract revision 0010 keeps
with `core/pg_order_versions.py`.

THE ВИТРИНА HOLDS FACTS, NOT POLICY

`app.customer_profile` is one row per retail customer: dates, counts, money.
Deliberately **no value tier**: the tier thresholds are campaign policy that
lives in the SMS endpoints, and a policy materialised into a table is a policy
that silently disagrees with the code the day either changes. Any reader can
compute a tier from these facts; none can un-bake a stale one.

Retail only, and the WHERE says so: the кабинет and офферы this feeds are a
retail product, and `internal` is admin-only by access rule — a customer row
that mixed them would leak numbers the dashboard itself gates.

Rebuilt whole from silver in the same scheduler tick as Gold, immediately
after it (`core/pg_vitrina.py`) — one floor, one tick, same reasoning as
revision 0007. Which Postgres it ultimately lives in — this one or the
shop's — is the owner's open question; until answered it is built here, and
being derived it moves for the cost of one rebuild elsewhere.
"""
from alembic import op


revision = "0011_buyers_lines_vitrina"
down_revision = "0010_order_versions"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── the buyer landing ────────────────────────────────────────────────────
    op.execute(
        """
        CREATE TABLE bronze.buyers (
            id                   INTEGER       PRIMARY KEY,
            full_name            TEXT,
            birthday             DATE,
            note                 TEXT,
            phone                TEXT,
            email                TEXT,
            manager_id           INTEGER,
            company_id           INTEGER,
            company_name         TEXT,
            city                 TEXT,
            region               TEXT,
            loyalty_program_name TEXT,
            loyalty_level_name   TEXT,
            loyalty_discount     NUMERIC(5, 2),
            loyalty_amount       NUMERIC(12, 2),
            created_at           TIMESTAMPTZ,
            updated_at           TIMESTAMPTZ,

            mirrored_at          TIMESTAMPTZ   NOT NULL DEFAULT now()
        )
        """
    )
    op.execute("CREATE INDEX idx_buyers_city ON bronze.buyers (city)")

    op.execute(
        """
        CREATE TABLE bronze.buyer_contacts (
            buyer_id     INTEGER     NOT NULL,
            contact_type TEXT        NOT NULL,
            value        TEXT        NOT NULL,
            is_primary   BOOLEAN     NOT NULL DEFAULT FALSE,

            mirrored_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

            PRIMARY KEY (buyer_id, contact_type, value)
        )
        """
    )

    op.execute(
        "COMMENT ON TABLE bronze.buyers IS "
        "'Mirror of DuckDB buyers: the same parsed Buyer batch handed to two "
        "stores in the same call (core/pg_buyers.py). Delta-fed like orders; "
        "history arrives via the ids-diff backfill, and the comparison is "
        "gated on backfilled_at.'"
    )

    # ── the line level ───────────────────────────────────────────────────────
    # The Postgres rendering of core.sql_dialect.order_lines_select(POSTGRES).
    # Frozen here; a test asserts the two agree.
    op.execute(
        """
        CREATE VIEW silver.order_lines AS
        SELECT
            op.id                                        AS line_id,
            op.order_id,
            op.product_id,
            op.name                                      AS product_name,  -- as sold
            op.quantity,
            op.price_sold,
            CAST(op.quantity * op.price_sold AS DECIMAL(14, 2)) AS line_amount,
            -- the order, denormalised: every predicate a page applies to
            -- revenue applies here too, and re-deriving them was the bug
            s.order_date,
            s.ordered_at,
            s.sales_type,
            s.source_id,
            s.source_name,
            s.is_return,
            s.is_active_source,
            s.buyer_id,
            s.manager_id,
            s.is_new_customer,
            s.buyer_first_order_date,
            s.promocode,
            s.grand_total                                AS order_grand_total,
            -- the catalog, denormalised. 8.2 % of lines carry no product_id and
            -- 31 % no category; both stay NULL rather than being dropped, which
            -- is the difference between a level and a filter.
            p.name                                       AS catalog_product_name,
            p.brand,
            p.sku,
            p.category_id,
            c.name                                       AS category_name,
            c.parent_id                                  AS parent_category_id,
            parent_c.name                                AS parent_category_name
        FROM bronze.order_products op
        JOIN silver.orders s ON s.id = op.order_id
        LEFT JOIN bronze.products p ON p.id = op.product_id
        LEFT JOIN bronze.categories c ON c.id = p.category_id
        LEFT JOIN bronze.categories parent_c ON parent_c.id = c.parent_id
        """
    )

    # ── the витрина ──────────────────────────────────────────────────────────
    op.execute(
        """
        CREATE TABLE app.customer_profile (
            buyer_id         INTEGER       PRIMARY KEY,
            first_order_date DATE,
            last_order_date  DATE,
            orders_count     INTEGER       NOT NULL,
            ltv              NUMERIC(14,2) NOT NULL,
            avg_order        NUMERIC(12,2) NOT NULL,
            returns_count    INTEGER       NOT NULL,
            city             TEXT,
            refreshed_at     TIMESTAMPTZ   NOT NULL
        )
        """
    )
    op.execute(
        "COMMENT ON TABLE app.customer_profile IS "
        "'Витрина: one row per retail customer, facts only — tiers are policy "
        "and stay in code. Rebuilt whole from silver.orders in the same tick "
        "as Gold (core/pg_vitrina.py). Derived: dropping it loses nothing.'"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS app.customer_profile")
    op.execute("DROP VIEW IF EXISTS silver.order_lines")
    op.execute("DROP TABLE IF EXISTS bronze.buyer_contacts")
    op.execute("DROP TABLE IF EXISTS bronze.buyers")
