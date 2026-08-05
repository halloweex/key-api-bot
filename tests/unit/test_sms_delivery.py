"""Tests for recording a send, delivery reports, and opt-outs.

The behaviours worth pinning are the ones that quietly corrupt a measurement:
messaging the control arm, re-selecting someone who opted out, and treating a
not-yet-reported delivery as a failure.
"""
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

from core.duckdb_store import DuckDBStore

SENT = datetime(2026, 8, 10, 9, 0)


async def _make_store(tmp_path: Path) -> DuckDBStore:
    store = DuckDBStore(db_path=tmp_path / "test.duckdb")
    await store.connect()
    return store


def _member(buyer_id: int, assignment: str, tier: str = "CORE") -> dict:
    return {
        "buyerId": buyer_id,
        "phone": f"3809{buyer_id:08d}",
        "tier": tier,
        "assignment": assignment,
        "orders": 2,
        "revenueLtv": 5000.0,
        "marginLtv": 3000.0,
        "recencyDays": 30,
    }


async def _freeze(store, members, campaign="aug"):
    await store.freeze_sms_campaign(
        campaign=campaign, customers=members, criteria={}, ltv_basis="margin",
        sales_type="retail", holdout_pct=10,
    )


# ─── who gets messaged ───────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_only_the_target_arm_is_returned_for_sending(tmp_path):
    """Messaging the control destroys the only way to measure the campaign."""
    store = await _make_store(tmp_path)
    await _freeze(store, [
        _member(1, "target"), _member(2, "target"), _member(3, "holdout"),
    ])

    targets = await store.get_sms_campaign_targets("aug")

    assert {t["buyerId"] for t in targets} == {1, 2}
    await store.close()


@pytest.mark.asyncio
async def test_sending_twice_is_refused(tmp_path):
    store = await _make_store(tmp_path)
    await _freeze(store, [_member(1, "target")])
    await store.record_sms_send("aug", {1: "mid-1"}, [], {}, SENT)

    with pytest.raises(ValueError, match="already sent"):
        await store.get_sms_campaign_targets("aug")

    await store.close()


@pytest.mark.asyncio
async def test_unknown_campaign_has_no_targets(tmp_path):
    store = await _make_store(tmp_path)
    with pytest.raises(ValueError, match="not frozen"):
        await store.get_sms_campaign_targets("nope")
    await store.close()


# ─── recording the gateway's answer ──────────────────────────────────────

@pytest.mark.asyncio
async def test_send_records_ids_stoplist_and_failures(tmp_path):
    store = await _make_store(tmp_path)
    await _freeze(store, [
        _member(1, "target"), _member(2, "target"), _member(3, "target"),
    ])

    summary = await store.record_sms_send(
        "aug", accepted={1: "mid-1"}, stoplisted=[2],
        failed={3: "INVALID_NUMBER"}, sent_at=SENT,
    )

    assert summary == {"campaign": "aug", "accepted": 1, "stoplisted": 1, "failed": 1}

    async with store.connection() as conn:
        rows = dict(conn.execute(
            "SELECT buyer_id, delivery_status FROM sms_campaign_members"
            " WHERE campaign='aug'"
        ).fetchall())
        mid = conn.execute(
            "SELECT message_id FROM sms_campaign_members"
            " WHERE campaign='aug' AND buyer_id=1"
        ).fetchone()[0]
        sent_at = conn.execute(
            "SELECT sent_at FROM sms_campaigns WHERE campaign='aug'"
        ).fetchone()[0]

    assert mid == "mid-1"
    assert rows[2] == "Stoplist"
    assert rows[3] == "INVALID_NUMBER"
    assert sent_at is not None, "recording a send stamps the campaign"

    await store.close()


@pytest.mark.asyncio
async def test_stoplisted_recipients_become_optouts(tmp_path):
    """The provider refuses delivery; we must also stop re-selecting them."""
    store = await _make_store(tmp_path)
    await _freeze(store, [_member(1, "target"), _member(2, "target")])

    await store.record_sms_send("aug", {1: "mid-1"}, [2], {}, SENT)

    async with store.connection() as conn:
        rows = conn.execute(
            "SELECT buyer_id, channel, phone, reason, source FROM marketing_optouts"
        ).fetchall()

    assert len(rows) == 1
    assert rows[0][0] == 2
    assert rows[0][1] == "sms"
    assert rows[0][2] == "3809" + f"{2:08d}", "the phone travels with the opt-out"
    assert rows[0][3] == "stoplist"
    assert rows[0][4] == "turbosms"

    await store.close()


# ─── delivery reports ────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_delivery_report_marks_the_member(tmp_path):
    store = await _make_store(tmp_path)
    await _freeze(store, [_member(1, "target")])
    await store.record_sms_send("aug", {1: "mid-1"}, [], {}, SENT)

    matched = await store.record_sms_delivery(
        "mid-1", "DELIVRD", True, datetime(2026, 8, 10, 9, 5),
    )
    assert matched is True

    async with store.connection() as conn:
        row = conn.execute(
            "SELECT delivery_status, delivered, delivered_at FROM sms_campaign_members"
            " WHERE message_id='mid-1'"
        ).fetchone()

    assert row[0] == "DELIVRD"
    assert row[1] is True
    assert row[2] is not None

    await store.close()


@pytest.mark.asyncio
async def test_non_final_status_leaves_the_flag_alone(tmp_path):
    """'Sent' is not 'failed' — guessing would understate the campaign."""
    store = await _make_store(tmp_path)
    await _freeze(store, [_member(1, "target")])
    await store.record_sms_send("aug", {1: "mid-1"}, [], {}, SENT)

    await store.record_sms_delivery("mid-1", "Sent", None)

    async with store.connection() as conn:
        row = conn.execute(
            "SELECT delivery_status, delivered FROM sms_campaign_members"
            " WHERE message_id='mid-1'"
        ).fetchone()

    assert row[0] == "Sent"
    assert row[1] is None, "still open, not a failure"

    await store.close()


@pytest.mark.asyncio
async def test_report_for_unknown_message_is_reported_as_unmatched(tmp_path):
    store = await _make_store(tmp_path)
    assert await store.record_sms_delivery("never-seen", "DELIVRD", True) is False
    await store.close()


# ─── opt-outs keep people out ────────────────────────────────────────────

@pytest.mark.asyncio
async def test_optout_excludes_the_customer_from_future_segments(tmp_path):
    """Segmentation reads purchases only, so without this they come straight back."""
    store = await _make_store(tmp_path)
    async with store.connection() as conn:
        conn.execute("INSERT INTO products (id, name, sku, price)"
                     " VALUES (1, 'Cream', 'SKU-1', 1000)")
        conn.execute("INSERT INTO offer_stocks (id, sku, price, purchased_price,"
                     " quantity) VALUES (1, 'SKU-1', 1000, 400, 10)")
        for bid in (1, 2):
            conn.execute("INSERT INTO buyers (id, full_name, phone, city)"
                         " VALUES (?, ?, ?, 'Kyiv')",
                         [bid, f"Buyer {bid}", f"3809{bid:08d}"])
            conn.execute(
                """
                INSERT INTO silver_orders (id, source_id, status_id, grand_total,
                    ordered_at, buyer_id, manager_id, order_date, is_return,
                    sales_type, is_active_source, source_name, is_new_customer)
                VALUES (?, 4, 1, '9000.00', ?, ?, NULL, ?, FALSE, 'retail', TRUE,
                        'Shopify', FALSE)
                """,
                [bid, date.today() - timedelta(days=10), bid,
                 date.today() - timedelta(days=10)],
            )
            conn.execute("INSERT INTO order_products (id, order_id, product_id,"
                         " name, quantity, price_sold) VALUES (?, ?, 1, 'Cream',"
                         " 9, '1000.00')", [bid, bid])

    before = await store.get_sms_segments(include_customers=True, holdout_pct=0)
    assert {c["buyerId"] for c in before["customers"]} == {1, 2}

    await store.add_marketing_optout(buyer_id=2, phone="380900000002")

    after = await store.get_sms_segments(include_customers=True, holdout_pct=0)
    assert {c["buyerId"] for c in after["customers"]} == {1}

    await store.close()


@pytest.mark.asyncio
async def test_optout_is_idempotent(tmp_path):
    store = await _make_store(tmp_path)
    await store.add_marketing_optout(buyer_id=7, reason="manual")
    result = await store.add_marketing_optout(buyer_id=7, reason="complaint")

    assert result["totalOptouts"] == 1, "the same person is not recorded twice"
    await store.close()


# ─── schema migration ────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_delivery_columns_are_added_to_a_pre_existing_table(tmp_path):
    """CREATE TABLE IF NOT EXISTS is a no-op on an existing table.

    A database created before the TurboSMS work has the roster but none of the
    delivery columns, so every delivery report fails. The migration has to add
    them; this reproduces that database and checks it recovers.
    """
    db = tmp_path / "legacy.duckdb"

    # A roster table in its pre-TurboSMS shape
    import duckdb
    con = duckdb.connect(str(db))
    con.execute(
        """
        CREATE TABLE sms_campaign_members (
            campaign VARCHAR NOT NULL,
            buyer_id INTEGER NOT NULL,
            phone VARCHAR NOT NULL,
            tier VARCHAR NOT NULL,
            assignment VARCHAR NOT NULL,
            orders_at_export INTEGER NOT NULL,
            revenue_ltv_at_export DECIMAL(14, 2),
            margin_ltv_at_export DECIMAL(14, 2),
            recency_at_export INTEGER,
            PRIMARY KEY (campaign, buyer_id)
        )
        """
    )
    con.execute(
        "INSERT INTO sms_campaign_members VALUES"
        " ('old', 1, '380961111111', 'CORE', 'target', 2, 100, 50, 30)"
    )
    con.close()

    store = DuckDBStore(db_path=db)
    await store.connect()

    async with store.connection() as conn:
        columns = {r[0] for r in conn.execute(
            "DESCRIBE sms_campaign_members"
        ).fetchall()}

    assert {"message_id", "delivery_status", "delivered", "delivered_at"} <= columns

    # And the existing row survived, so the migration did not rebuild the table
    async with store.connection() as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM sms_campaign_members WHERE campaign='old'"
        ).fetchone()[0] == 1

    await store.close()
