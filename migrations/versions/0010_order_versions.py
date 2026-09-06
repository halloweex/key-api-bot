"""app.order_versions — the one fact this system loses every single day

Revision ID: 0010_order_versions
Revises: 0009_bot_state
Created: 2026-08-27

Step 0 of "Одна бронза": an append-only archive of every version of an order
header. Not a layer of the trunk — nothing downstream reads it, Silver keeps
feeding from the latest state in bronze, and the whole table could be dropped
without moving a single number on a dashboard. It is built first anyway, and
that is the entire argument for it.

WHY FIRST, WHEN NOTHING DEPENDS ON IT

Everything else on the schema can be rebuilt. `bronze.*` can be re-fetched from
KeyCRM; Silver and Gold are recomputed from bronze in 1.37 s and 235 ms. The
one thing that cannot is *yesterday's status*: KeyCRM serves current state and
has no history endpoint, so a transition nobody wrote down when it happened is
gone. That is the same class as `stock_movements` — a movement not recorded
when it occurred is not recoverable from the thing that moved.

At ~48 orders a day and a handful of transitions each, the honest cost is
around a hundred rows a day. The risk here is silence, not volume; see
`reconcile_order_versions`.

WHY THE HEADER ONLY, AND NOT THE LINE ITEMS

Because on the path that produces most of the writes, there are no line items
to see. `order_status_refresh` runs daily at 05:15 with `skip_products=True`,
so `core.landing_rows.landed_orders` is called with `with_products=False` and
returns an empty product list — it is a header-only refresh by design. An
archive that recorded line items would read that as "every item removed" on
~1,400 orders every morning, and would record the deletion of a basket nobody
touched.

Measured on production 2026-08-27: 1,594 orders inside that 30-day window,
1,388 of them force-written in the 05:00 hour on 26.08 alone.

Status transitions are what is being preserved, and they live in the header.
A basket that genuinely changes does so through the ordinary sync, which
carries its items and rewrites `bronze.order_products` — a table that is
mirrored, compared daily, and therefore not lost.

WHY `updated_at` IS STORED BUT NOT COMPARED

KeyCRM does not bump `updated_at` on a status change — that is the whole reason
`force_update` exists — so it cannot be the key. The subtler half is the
reverse: if `updated_at` moves and every column we hold is identical, then what
we hold has not changed, and writing a row would announce a change while
showing none. That is `bronze_order_events` in slow motion: it keyed on the
*observation* rather than the *change*, wrote ~150K rows a day and took the
database to 43 GB before it was turned off by default. The column is kept for
forensics and left out of the comparison.

WHY A BASELINE IS SEEDED HERE

An empty archive makes the first row about each order ambiguous: it is not a
transition, it is whichever state that order happened to be in the first time
something touched it. Seeding one `baseline` row per order at creation time
makes every later row mean exactly one thing — a change — and makes "the first
version" answerable. It costs one INSERT ... SELECT over `bronze.orders`
(46,685 rows on production, 0 in a fresh checkout).

APPEND-ONLY IS ENFORCED BY THERE BEING NO STATEMENT THAT IS NOT AN INSERT

Deliberately *not* by a REVOKE. Migrations run as `ks_app` (see the `migrate`
service in docker-compose.yml, which chose that over `postgres` on purpose), so
`ks_app` owns this table, and an owner can grant itself back anything it
revoked. A REVOKE here would look like a guarantee and be a speed bump.

The same answer the append-only inventory tables already use applies instead:
`tests/unit/test_order_versions.py` parses the repository for an UPDATE or a
DELETE against this table and fails on one.
"""
from alembic import op


revision = "0010_order_versions"
down_revision = "0009_bot_state"
branch_labels = None
depends_on = None


# The list of compared columns lives in `core.pg_order_versions`, not here — a
# migration is a frozen artefact and nothing running should import one. The DDL
# below spells them out, and a test asserts the two agree.


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE app.order_versions (
            -- BIGSERIAL and not INTEGER: this is the one table in the schema
            -- that only ever grows. At the measured rate it reaches ~550K rows
            -- in a decade, so INTEGER would in fact do — but a surrogate key on
            -- an append-only archive is the last place to leave a ceiling.
            id              BIGSERIAL     PRIMARY KEY,

            -- Matches bronze.orders.id. No foreign key: the archive outlives
            -- the order, and KeyCRM does retire rows — product 1055 is the
            -- standing proof one schema up. A version whose order is gone is
            -- the most interesting row in the table, not an integrity error.
            order_id        INTEGER       NOT NULL,

            -- When this application saw the change, not when KeyCRM made it.
            -- Those differ by up to the sync interval and the archive must not
            -- pretend otherwise.
            captured_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),

            -- baseline | create | change.
            --   baseline — seeded below; the state at the moment the archive
            --              began, which is not a transition and must not be
            --              counted as one.
            --   create   — first version of an order this store had not seen.
            --   change   — content differs from the previous version.
            -- No CHECK constraint: the writer is one statement in one module,
            -- and a constraint would only fail a deploy that the test suite
            -- already fails.
            kind            TEXT          NOT NULL,

            -- The header exactly as written to bronze.orders, which is not
            -- always what arrived: `manager_comment` is upserted as
            -- COALESCE(EXCLUDED, stored) because it carries UTM attribution a
            -- backfill restored, and 32,437 of 46,685 orders have a value in
            -- it. Comparing against the incoming payload instead of the stored
            -- row would report "comment removed" on two orders in three, on
            -- every tick, forever.
            source_id       INTEGER,
            status_id       INTEGER,
            status_group_id INTEGER,
            grand_total     NUMERIC(12,2),
            ordered_at      TIMESTAMPTZ,
            buyer_id        INTEGER,
            manager_id      INTEGER,
            manager_comment TEXT,
            promocode       TEXT,

            -- Stored, never compared. See the module docstring.
            updated_at      TIMESTAMPTZ
        )
        """
    )

    # The capture reads exactly one row per order — the latest version — for
    # every id in the batch. DESC on both so the index serves the ORDER BY of
    # the DISTINCT ON directly; `id` breaks the tie because several versions of
    # one order can share a `captured_at` when they land in the same second.
    op.execute(
        """
        CREATE INDEX idx_order_versions_latest
            ON app.order_versions (order_id, captured_at DESC, id DESC)
        """
    )

    # Answers "what happened yesterday", which is the question the table exists
    # for and the one a per-order index cannot serve.
    op.execute(
        "CREATE INDEX idx_order_versions_captured_at "
        "ON app.order_versions (captured_at)"
    )

    # ── the baseline ─────────────────────────────────────────────────────────
    # Every order the store holds, as it stands now. Empty in a fresh checkout,
    # 46,685 rows on production. `captured_at` defaults to now() rather than
    # being derived from `mirrored_at`: this row records when the archive
    # started watching, and dressing it up as an observation from the past
    # would be a lie the table cannot later distinguish from a real one.
    op.execute(
        """
        INSERT INTO app.order_versions (
            order_id, kind, source_id, status_id, status_group_id,
            grand_total, ordered_at, buyer_id, manager_id,
            manager_comment, promocode, updated_at)
        SELECT id, 'baseline', source_id, status_id, status_group_id,
               grand_total, ordered_at, buyer_id, manager_id,
               manager_comment, promocode, updated_at
        FROM bronze.orders
        """
    )

    # No apostrophes in this string on purpose: the first draft of revision 0009
    # put one inside a single-quoted SQL literal and the migration died at
    # COMMENT ON, on a real server, after four tables had already been created.
    op.execute(
        "COMMENT ON TABLE app.order_versions IS "
        "'Append-only archive of order header versions, written from "
        "updated_ids in the same transaction as the mirror. The only fact in "
        "this schema that cannot be re-fetched from KeyCRM: it serves current "
        "state and has no history. Nothing downstream reads this table.'"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS app.order_versions")
