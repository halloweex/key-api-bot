"""The SMS tab's own state, and the cost table its money is measured with.

Revision ID: 0013_sms_state
Revises: 0012_alert_archive
Created: 2026-08-30

Six tables so that `/sms` can be answered without DuckDB. Five of them are the
seventh thing in this system that nothing can rebuild; the sixth is a landing
copy that the other five are priced against.

WHY THE ROSTER IS IRREPLACEABLE, IN THE SAME SENSE `stock_movements` IS

The eligible population moves every day — people buy, recency slides, someone
opts out. Re-running the segmentation a week after a send returns a DIFFERENT
set of people, so "who was in the campaign" is not recomputable from anything;
it is only recorded. And it is recorded once, at the moment of export, for
target and holdout alike. Without those two lists there is no control group,
and without a control group the campaign has no measured lift at all — only a
before-and-after that every seasonal effect contaminates.

That puts `sms_campaigns` + `sms_campaign_members` in the category of
`app.stock_movements` and `app.order_versions`: derived from state only the
writing store holds, lost for good if the file holding them is lost. They are
REPLICATED, not mirrored — `core/pg_operational.py`'s distinction — because a
second computation here would diverge the first time either side missed a
tick, and there is no payload to re-parse.

`sms_dlr_events` is the same category for a different reason. TurboSMS signs
SHA1(secret + id) and nothing else, so the binding of an event id to the
message it first named is what stops one captured (id, signature) pair from
rewriting any recipient's delivery state forever. The scheme carries no nonce
and no expiry, so the binding must not expire either — which makes this table
a security control with a durability requirement, not a cache.

`marketing_optouts` is a promise made to a person. Segmentation reads only
purchases, so a lost opt-out is not a missing row: it is the same customer
re-selected by every future export, messaged again, after asking not to be.

`sms_audience_presets` is the one of the five that could be re-entered by
hand, and it still comes along: a preset is what makes the second campaign
measurable against the same population as the first.

WHY `bronze.offer_stocks` IS HERE AND WHY IT IS `bronze`

`ltv_basis=margin` — the basis this tab prefers, because ranking by revenue
steers budget at low-margin customers — is revenue minus COGS, and COGS comes
from `offer_stocks.purchased_price` joined by SKU. Without it the Postgres
side can compute one of the two lifetime values and the tab is not portable.

It is written straight from KeyCRM's offers/stocks payload
(`core/repositories/inventory.py`), so it is LANDING, and it goes to `bronze`
under `mirror_products`' contract: the catalogue re-ships whole, both stores
receive the same parsed tuple, and a tolerance of zero is therefore legitimate
rather than generous.

TWO DECISIONS THAT READ AS DETAILS AND ARE NOT

`criteria` stays TEXT, not JSONB. It holds the wizard's own form state as a
JSON string, and the comparison against DuckDB is column by column with
tolerance zero. JSONB normalises key order and whitespace on the way in, so a
faithful copy would come back differing from its source and the reconciliation
would spend every morning reporting a difference this migration created. The
column is never queried by key — `sms_audience_presets` is read whole and
parsed in Python, and a preset is deliberately never executed as a query.

`delivered` is BOOLEAN and NULL means "the operator has not reported yet",
which is a third state and not a false. `record_sms_delivery` writes it from
the gateway callback and campaign results exclude anyone not delivered to; an
undelivered number counted as messaged drags the measured lift down. Coercing
NULL to false here would silently move every pending recipient into the
"failed" bucket — the `notifications_enabled` trap of revision 0009, in a
table whose whole purpose is measuring a number.

NO FOREIGN KEY from members to campaigns, deliberately. DuckDB has none, the
replication replaces both tables in one transaction, and an FK would only
decide the order in which a full replace must run — a constraint that
constrains the copier rather than the data.
"""
from alembic import op

revision = "0013_sms_state"
down_revision = "0012_alert_archive"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── landing: the cost side of margin ─────────────────────────────────────
    op.execute(
        """
        CREATE TABLE bronze.offer_stocks (
            id              INTEGER       PRIMARY KEY,   -- offer_id from KeyCRM
            sku             TEXT,
            price           NUMERIC(12,2),
            purchased_price NUMERIC(12,2),
            quantity        INTEGER       NOT NULL DEFAULT 0,
            reserve         INTEGER       NOT NULL DEFAULT 0,

            mirrored_at     TIMESTAMPTZ   NOT NULL DEFAULT now()
        )
        """
    )
    # Joined by SKU, never by id: `silver.order_lines` carries the SKU the
    # product was catalogued under, and offers are per-SKU stock records.
    op.execute("CREATE INDEX idx_offer_stocks_sku ON bronze.offer_stocks(sku)")
    op.execute(
        "COMMENT ON TABLE bronze.offer_stocks IS "
        "'Landing mirror of KeyCRM offers/stocks: the same parsed payload "
        "handed to two stores. Carries purchased_price, which is the only "
        "source of COGS for margin-based LTV (core/pg_sms.py).'"
    )

    # ── the promise ──────────────────────────────────────────────────────────
    op.execute(
        """
        CREATE TABLE app.marketing_optouts (
            buyer_id     INTEGER     NOT NULL,
            channel      TEXT        NOT NULL DEFAULT 'sms',
            phone        TEXT,
            reason       TEXT,                            -- stoplist | manual | complaint
            source       TEXT,                            -- who/what recorded it
            opted_out_at TIMESTAMPTZ NOT NULL DEFAULT now(),

            PRIMARY KEY (buyer_id, channel)
        )
        """
    )
    # Matched on buyer AND on phone: the same number reaches us under a second
    # buyer record often enough that buyer_id alone re-selects an opted-out
    # person under their other identity.
    op.execute("CREATE INDEX idx_optouts_phone ON app.marketing_optouts(phone)")
    op.execute(
        "COMMENT ON TABLE app.marketing_optouts IS "
        "'A promise made to a person. Replicated from DuckDB; losing a row "
        "means messaging someone who asked not to be.'"
    )

    # ── saved audiences ──────────────────────────────────────────────────────
    op.execute(
        """
        CREATE TABLE app.sms_audience_presets (
            name       TEXT        PRIMARY KEY,
            criteria   TEXT        NOT NULL,              -- wizard form state, JSON as text
            created_by BIGINT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ
        )
        """
    )
    op.execute(
        "COMMENT ON TABLE app.sms_audience_presets IS "
        "'Saved wizard form state, never executed as a query. criteria is "
        "TEXT and not JSONB so a faithful copy compares equal to its source.'"
    )

    # ── the campaign, and who was in it ──────────────────────────────────────
    op.execute(
        """
        CREATE TABLE app.sms_campaigns (
            campaign       TEXT        PRIMARY KEY,
            ltv_basis      TEXT        NOT NULL,
            sales_type     TEXT        NOT NULL,
            holdout_pct    INTEGER     NOT NULL,
            criteria       TEXT        NOT NULL,          -- threshold snapshot, JSON as text
            promocode      TEXT,
            exported_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
            sent_at        TIMESTAMPTZ,                   -- set when the file actually goes out
            notes          TEXT,

            -- What the send cost, recorded as it happened rather than read
            -- from config at display time: changing the tariff must not
            -- restate a past campaign's economics.
            message_text   TEXT,
            message_parts  INTEGER,
            recipients_sent INTEGER,
            price_per_part NUMERIC(10,4),
            cost_total     NUMERIC(14,2)
        )
        """
    )
    op.execute(
        "COMMENT ON TABLE app.sms_campaigns IS "
        "'One row per frozen send. Replicated from DuckDB: the eligible "
        "population moves daily, so this is recorded, never recomputed.'"
    )

    op.execute(
        """
        CREATE TABLE app.sms_campaign_members (
            campaign              TEXT        NOT NULL,
            buyer_id              INTEGER     NOT NULL,
            phone                 TEXT        NOT NULL,
            tier                  TEXT        NOT NULL,
            assignment            TEXT        NOT NULL,   -- 'target' or 'holdout'

            -- State at export time. The campaign is measured against what
            -- these customers looked like when they were chosen, not against
            -- what they have become since.
            orders_at_export      INTEGER     NOT NULL,
            revenue_ltv_at_export NUMERIC(14,2),
            margin_ltv_at_export  NUMERIC(14,2),
            recency_at_export     INTEGER,

            -- Delivery, filled in from the gateway callback. NULL delivered
            -- is "not reported yet" and is a third state, not a false.
            message_id            TEXT,
            delivery_status       TEXT,                   -- gateway status, verbatim
            delivered             BOOLEAN,
            delivered_at          TIMESTAMPTZ,

            PRIMARY KEY (campaign, buyer_id)
        )
        """
    )
    op.execute(
        "CREATE INDEX idx_sms_members_campaign "
        "ON app.sms_campaign_members(campaign, assignment)"
    )
    op.execute("CREATE INDEX idx_sms_members_buyer ON app.sms_campaign_members(buyer_id)")
    # The delivery callback arrives knowing only the gateway's message id.
    op.execute(
        "CREATE INDEX idx_sms_members_message_id "
        "ON app.sms_campaign_members(message_id) WHERE message_id IS NOT NULL"
    )
    op.execute(
        "COMMENT ON TABLE app.sms_campaign_members IS "
        "'The frozen roster — target and holdout alike. Without the holdout "
        "there is no control group and the campaign has no measurable lift.'"
    )

    # ── the delivery-report binding ──────────────────────────────────────────
    op.execute(
        """
        CREATE TABLE app.sms_dlr_events (
            event_id      TEXT        PRIMARY KEY,
            message_id    TEXT        NOT NULL,
            first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    # Replicated above its own watermark, so the copier reads this back.
    op.execute(
        "CREATE INDEX idx_sms_dlr_first_seen ON app.sms_dlr_events(first_seen_at)"
    )
    op.execute(
        "COMMENT ON TABLE app.sms_dlr_events IS "
        "'What each delivery-report event first said it was about. A security "
        "control: TurboSMS signs SHA1(secret + id) only, so without this one "
        "captured pair rewrites any recipient''s delivery state forever. "
        "Append-only, never pruned — the captured pair never expires either.'"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS app.sms_dlr_events")
    op.execute("DROP TABLE IF EXISTS app.sms_campaign_members")
    op.execute("DROP TABLE IF EXISTS app.sms_campaigns")
    op.execute("DROP TABLE IF EXISTS app.sms_audience_presets")
    op.execute("DROP TABLE IF EXISTS app.marketing_optouts")
    op.execute("DROP TABLE IF EXISTS bronze.offer_stocks")
