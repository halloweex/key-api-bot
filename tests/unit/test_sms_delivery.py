"""Tests for recording a send, delivery reports, and opt-outs.

The behaviours worth pinning are the ones that quietly corrupt a measurement:
messaging the control arm, re-selecting someone who opted out, and treating a
not-yet-reported delivery as a failure.
"""
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

from core.duckdb_store import DuckDBStore
from core.repositories.customers import DlrEventRebound

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

    assert summary == {"campaign": "aug", "accepted": 1, "stoplisted": 1,
                       "failed": 1, "notSent": 0}

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


@pytest.mark.asyncio
async def test_the_stoplist_completes_a_phoneless_optout(tmp_path):
    """The gateway is where a hand-filed opt-out usually gets its number."""
    store = await _make_store(tmp_path)
    await _freeze(store, [_member(1, "target")])
    await store.add_marketing_optout(buyer_id=1, reason="complaint", source="ops")

    await store.record_sms_send("aug", {}, [1], {}, SENT)

    async with store.connection() as conn:
        row = conn.execute(
            "SELECT phone, reason, source FROM marketing_optouts WHERE buyer_id = 1"
        ).fetchone()

    assert row[0] == "3809" + f"{1:08d}", "the roster's phone lands on the row"
    assert (row[1], row[2]) == ("complaint", "ops"), \
        "the gateway confirms a refusal it did not receive; it does not reattribute it"

    await store.close()


# ─── delivery reports ────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_delivery_report_marks_the_member(tmp_path):
    store = await _make_store(tmp_path)
    await _freeze(store, [_member(1, "target")])
    await store.record_sms_send("aug", {1: "mid-1"}, [], {}, SENT)

    matched = await store.record_sms_delivery(
        "mid-1", "DELIVRD", True, datetime(2026, 8, 10, 9, 5), event_id="evt-1",
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

    await store.record_sms_delivery("mid-1", "Sent", None, event_id="evt-1")

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
    assert await store.record_sms_delivery(
        "never-seen", "DELIVRD", True, event_id="evt-1") is False
    await store.close()


@pytest.mark.asyncio
async def test_a_confirmed_delivery_is_not_downgraded_by_a_later_failure(tmp_path):
    """Delivery is terminal. A later UNDELIV for a message already reported
    DELIVRD — a reordered or duplicate gateway callback, or a forged replay
    built from one captured (id, signature) pair, since the signature covers
    only the event id and never the delivery data — must not flip a real
    delivery to a failure and drag the measured lift down.

    The row still exists, so the report is still acknowledged (200): what is
    refused is the corrupting write, not the callback.
    """
    store = await _make_store(tmp_path)
    await _freeze(store, [_member(1, "target")])
    await store.record_sms_send("aug", {1: "mid-1"}, [], {}, SENT)

    assert await store.record_sms_delivery(
        "mid-1", "DELIVRD", True, datetime(2026, 8, 10, 9, 5),
        event_id="evt-1") is True
    # A contradicting report arrives afterwards, under its own event.
    assert await store.record_sms_delivery(
        "mid-1", "UNDELIV", False, datetime(2026, 8, 10, 9, 9),
        event_id="evt-2") is True

    async with store.connection() as conn:
        row = conn.execute(
            "SELECT delivery_status, delivered FROM sms_campaign_members"
            " WHERE message_id='mid-1'"
        ).fetchone()

    assert row[1] is True, "a confirmed delivery must stay delivered"
    assert row[0] == "DELIVRD", "and its status is not clobbered by the downgrade"
    await store.close()


@pytest.mark.asyncio
async def test_a_confirmed_delivery_is_not_reopened_by_a_later_nonfinal_status(tmp_path):
    """A stale 'Sent' arriving after 'DELIVRD' (reorder, retry, or replay) must
    not overwrite the ground-truth status of a delivered record."""
    store = await _make_store(tmp_path)
    await _freeze(store, [_member(1, "target")])
    await store.record_sms_send("aug", {1: "mid-1"}, [], {}, SENT)

    await store.record_sms_delivery("mid-1", "DELIVRD", True,
                                    datetime(2026, 8, 10, 9, 5), event_id="evt-1")
    await store.record_sms_delivery("mid-1", "Sent", None, event_id="evt-2")

    async with store.connection() as conn:
        row = conn.execute(
            "SELECT delivery_status, delivered FROM sms_campaign_members"
            " WHERE message_id='mid-1'"
        ).fetchone()

    assert row == ("DELIVRD", True)
    await store.close()


@pytest.mark.asyncio
async def test_an_open_report_can_still_progress_to_delivered(tmp_path):
    """The guard blocks downgrades only — a genuine Sent -> DELIVRD upgrade,
    and a re-confirmation of the same delivery, must both still apply."""
    store = await _make_store(tmp_path)
    await _freeze(store, [_member(1, "target")])
    await store.record_sms_send("aug", {1: "mid-1"}, [], {}, SENT)

    await store.record_sms_delivery("mid-1", "Sent", None, event_id="evt-1")
    await store.record_sms_delivery("mid-1", "DELIVRD", True,
                                    datetime(2026, 8, 10, 9, 5), event_id="evt-2")
    # Duplicate delivered callback (gateway retries the same event) is idempotent.
    await store.record_sms_delivery("mid-1", "READ", True,
                                    datetime(2026, 8, 10, 9, 6), event_id="evt-3")

    async with store.connection() as conn:
        row = conn.execute(
            "SELECT delivery_status, delivered FROM sms_campaign_members"
            " WHERE message_id='mid-1'"
        ).fetchone()

    assert row == ("READ", True)
    await store.close()


# ─── an event id names one message, for good ─────────────────────────────

@pytest.mark.asyncio
async def test_an_event_id_cannot_be_re_pointed_at_another_message(tmp_path):
    """The gateway signs the event id and nothing else, so the message_id in a
    callback is unproven. Binding the id to the message it first reported on is
    what stops a captured (id, signature) pair writing over somebody else."""
    store = await _make_store(tmp_path)
    await _freeze(store, [_member(1, "target"), _member(2, "target")])
    await store.record_sms_send("aug", {1: "mid-1", 2: "mid-2"}, [], {}, SENT)

    await store.record_sms_delivery("mid-1", "Sent", None, event_id="evt-1")

    with pytest.raises(DlrEventRebound) as caught:
        await store.record_sms_delivery("mid-2", "UNDELIV", False, event_id="evt-1")

    assert caught.value.bound_message_id == "mid-1"
    async with store.connection() as conn:
        row = conn.execute(
            "SELECT delivery_status, delivered FROM sms_campaign_members"
            " WHERE message_id='mid-2'"
        ).fetchone()
    assert row == ("Accepted", None), "the other recipient is untouched"

    await store.close()


@pytest.mark.asyncio
async def test_the_same_event_reported_nine_times_is_recorded_every_time(tmp_path):
    """The gateway retries each event nine times over 4.5 hours and offers no
    replay, so a refused callback is a delivery result lost for good. A retry
    carries the same id and the same data and must never read as a replay."""
    store = await _make_store(tmp_path)
    await _freeze(store, [_member(1, "target")])
    await store.record_sms_send("aug", {1: "mid-1"}, [], {}, SENT)

    for _ in range(9):
        assert await store.record_sms_delivery(
            "mid-1", "DELIVRD", True, datetime(2026, 8, 10, 9, 5),
            event_id="evt-1") is True

    async with store.connection() as conn:
        row = conn.execute(
            "SELECT delivery_status, delivered FROM sms_campaign_members"
            " WHERE message_id='mid-1'"
        ).fetchone()

    assert row == ("DELIVRD", True)
    await store.close()


@pytest.mark.asyncio
async def test_an_event_spent_on_an_unknown_message_is_still_spent(tmp_path):
    """An id burned against a message the roster does not hold must not stay
    re-pointable — that is the whole pair, still usable, one call later."""
    store = await _make_store(tmp_path)
    await _freeze(store, [_member(1, "target")])
    await store.record_sms_send("aug", {1: "mid-1"}, [], {}, SENT)

    assert await store.record_sms_delivery(
        "never-seen", "DELIVRD", True, event_id="evt-1") is False

    with pytest.raises(DlrEventRebound):
        await store.record_sms_delivery("mid-1", "UNDELIV", False, event_id="evt-1")

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


@pytest.mark.asyncio
async def test_a_later_optout_supplies_the_phone_the_first_one_lacked(tmp_path):
    """The refusal usually arrives before the number does.

    Someone phones in and is opted out by buyer id, because that is all the CRM
    screen shows. The number turns up afterwards — the gateway stoplists them on
    the next send, or the operator files it by hand. Until it lands, the phone
    column suppresses nothing, and the same person under a second buyer record
    (the one case that column exists for) keeps being messaged.
    """
    store = await _make_store(tmp_path)
    shared = "380900000001"
    async with store.connection() as conn:
        conn.execute("INSERT INTO products (id, name, sku, price)"
                     " VALUES (1, 'Cream', 'SKU-1', 1000)")
        conn.execute("INSERT INTO offer_stocks (id, sku, price, purchased_price,"
                     " quantity) VALUES (1, 'SKU-1', 1000, 400, 10)")
        # 1 and 2 are one person under two buyer records; 3 is somebody else.
        for bid, phone in ((1, shared), (2, shared), (3, "380900000003")):
            conn.execute("INSERT INTO buyers (id, full_name, phone, city)"
                         " VALUES (?, ?, ?, 'Kyiv')",
                         [bid, f"Buyer {bid}", phone])
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

    await store.add_marketing_optout(buyer_id=1, reason="manual")
    await store.add_marketing_optout(buyer_id=1, phone=shared, reason="stoplist")

    async with store.connection() as conn:
        stored = conn.execute(
            "SELECT phone, reason FROM marketing_optouts WHERE buyer_id = 1"
        ).fetchone()

    assert stored[0] == shared, "the phone must land on the row already there"
    assert stored[1] == "manual", \
        "the first refusal is the fact; a later one must not restate why"

    after = await store.get_sms_segments(include_customers=True, holdout_pct=0)
    assert {c["buyerId"] for c in after["customers"]} == {3}, \
        "the second buyer record carries the same number and must be suppressed too"

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

    # The message_id index has to be created here rather than alongside the
    # other indexes: at that point this database has no such column, and a
    # failed CREATE INDEX would take the whole schema init down with it.
    async with store.connection() as conn:
        indexes = {r[0] for r in conn.execute(
            "SELECT index_name FROM duckdb_indexes() "
            "WHERE table_name = 'sms_campaign_members'"
        ).fetchall()}

    assert "idx_sms_members_message_id" in indexes

    await store.close()


# ─── claiming ────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_asking_for_targets_claims_the_campaign(tmp_path):
    """The stamp has to land before the gateway call, not after it.

    It used to be written once the last batch had gone, leaving the campaign
    unclaimed for the whole length of a send — minutes, on a real roster. Two
    requests both read "not sent yet" and both went, which is how 5,550 people
    were messaged twice.
    """
    store = await _make_store(tmp_path)
    await _freeze(store, [_member(1, "target"), _member(2, "holdout")])

    first = await store.get_sms_campaign_targets("aug")
    assert [t["buyerId"] for t in first] == [1]

    with pytest.raises(ValueError, match="already sent"):
        await store.get_sms_campaign_targets("aug")

    await store.close()


@pytest.mark.asyncio
async def test_concurrent_sends_cannot_both_take_the_roster(tmp_path):
    import asyncio

    store = await _make_store(tmp_path)
    await _freeze(store, [_member(1, "target"), _member(2, "target")])

    results = await asyncio.gather(
        store.get_sms_campaign_targets("aug"),
        store.get_sms_campaign_targets("aug"),
        return_exceptions=True,
    )

    ok = [r for r in results if not isinstance(r, Exception)]
    refused = [r for r in results if isinstance(r, ValueError)]
    assert len(ok) == 1, "exactly one caller may send"
    assert len(refused) == 1

    await store.close()


@pytest.mark.asyncio
async def test_a_released_campaign_can_be_sent_again(tmp_path):
    """Nothing went out, so the claim must come back."""
    store = await _make_store(tmp_path)
    await _freeze(store, [_member(1, "target")])

    await store.get_sms_campaign_targets("aug")
    await store.release_sms_campaign("aug")

    assert [t["buyerId"] for t in await store.get_sms_campaign_targets("aug")] == [1]

    await store.close()


@pytest.mark.asyncio
async def test_members_the_gateway_never_answered_for_are_marked(tmp_path):
    """A send that dies partway leaves people behind, and they must not sit in
    the target arm unable to respond to a message they never got.

    The results already excluded this status; nothing wrote it. It existed only
    because a roster was repaired by hand, so the exclusion was dead code.
    """
    store = await _make_store(tmp_path)
    await _freeze(store, [
        _member(1, "target"), _member(2, "target"), _member(3, "target"),
        _member(4, "holdout"),
    ])

    # Only buyer 1 came back from the gateway before the send failed.
    summary = await store.record_sms_send("aug", {1: "m-1"}, [], {})

    assert summary["notSent"] == 2
    async with store.connection() as conn:
        rows = dict(conn.execute(
            "SELECT buyer_id, delivery_status FROM sms_campaign_members"
            " WHERE campaign = 'aug' ORDER BY buyer_id"
        ).fetchall())
    assert rows[1] == "Accepted"
    assert rows[2] == rows[3] == "NotSent"
    assert rows[4] is None, "the control was never sent to, so it is not NotSent"

    await store.close()


@pytest.mark.asyncio
async def test_a_complete_send_marks_nobody_as_unsent(tmp_path):
    store = await _make_store(tmp_path)
    await _freeze(store, [
        _member(1, "target"), _member(2, "target"), _member(3, "holdout"),
    ])

    summary = await store.record_sms_send(
        "aug", {1: "m-1"}, [], {2: "NOT_ALLOWED_NUMBER"},
    )

    assert summary["notSent"] == 0, "a refusal is an answer, not a silence"

    await store.close()
