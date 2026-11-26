"""Recording a send batch by batch, on the store the send writes.

The route writes each batch's answer as it lands (`final=False`) and the whole
answer once more at the end. The store has to make that safe:

* a batch write touches the members and nothing else, so a send still under
  way is never swept to NotSent or stamped with a cost;
* the final write fills only what is empty, so a delivery report that landed
  between the two keeps its status;
* a batch with no answer is 'OutcomeUnknown', counted as sent and in the cost,
  and never swept to NotSent, which is what a later "send to whoever was never
  messaged" would read;
* the progress the page polls is read off the same roster.

Skipped without `KS_PG_DSN`.
"""
import pytest
import pytest_asyncio

from tests.integration import pg_sms_seed
from tests.integration.pg_sms_seed import pg_conn


@pytest_asyncio.fixture(autouse=True)
async def _postgres(request, monkeypatch):
    async with pg_sms_seed.for_async_tests(request, monkeypatch):
        yield


def _member(buyer_id: int, assignment: str) -> dict:
    return {
        "buyerId": buyer_id, "phone": f"3809{buyer_id:08d}", "tier": "ALL",
        "assignment": assignment, "orders": 2, "revenueLtv": 5000.0,
        "marginLtv": 3000.0, "recencyDays": 30,
    }


@pytest_asyncio.fixture
async def store(tmp_path):
    s = pg_sms_seed.store(tmp_path)
    await s.freeze_sms_campaign(
        campaign="aug",
        customers=[_member(1, "target"), _member(2, "target"),
                   _member(3, "target"), _member(4, "holdout")],
        criteria={}, ltv_basis="margin", sales_type="retail", holdout_pct=10,
    )
    await s.get_sms_campaign_targets("aug")   # the claim, as the route takes it
    return s


async def _members() -> dict:
    async with pg_conn() as conn:
        rows = await conn.fetch(
            "SELECT buyer_id, message_id, delivery_status, delivered "
            "FROM app.sms_campaign_members WHERE campaign = 'aug'")
    return {r["buyer_id"]: (r["message_id"], r["delivery_status"], r["delivered"])
            for r in rows}


async def _campaign() -> tuple:
    async with pg_conn() as conn:
        row = await conn.fetchrow(
            "SELECT recipients_sent, cost_total, message_text "
            "FROM app.sms_campaigns WHERE campaign = 'aug'")
    return tuple(row)


@pytest.mark.asyncio
async def test_a_batch_write_touches_the_members_and_nothing_else(store):
    await store.record_sms_send("aug", {1: "m-1"}, [], {}, final=False)

    members = await _members()
    assert members[1] == ("m-1", "Accepted", None)
    assert members[2] == members[3] == (None, None, None), (
        "the rest of the roster is still on its way, not NotSent")
    assert await _campaign() == (None, None, None), "not stamped mid-send"


@pytest.mark.asyncio
async def test_the_final_write_keeps_a_delivery_report_that_landed_first(store):
    """The batch went, its report came back, then the send finished."""
    await store.record_sms_send("aug", {1: "m-1"}, [], {}, final=False)
    known = await store.record_sms_delivery("m-1", "DELIVRD", True, event_id="ev-1")
    assert known

    await store.record_sms_send("aug", {1: "m-1", 2: "m-2", 3: "m-3"}, [], {},
                                message_text="hi", message_parts=1,
                                price_per_part=1.28)

    members = await _members()
    assert members[1] == ("m-1", "DELIVRD", True), "the report is not erased"
    assert members[2] == ("m-2", "Accepted", None)


@pytest.mark.asyncio
async def test_an_unanswered_batch_is_uncertain_counted_and_never_swept(store):
    summary = await store.record_sms_send(
        "aug", {1: "m-1"}, [], {}, uncertain=[2],
        message_text="hi", message_parts=1, price_per_part=1.28,
    )

    members = await _members()
    assert members[2] == (None, "OutcomeUnknown", None)
    assert members[3][1] == "NotSent", "the batch that never went"
    assert members[4] == (None, None, None), "the control is untouched"
    assert summary["notSent"] == 1, "the uncertain one is not counted as unsent"
    recipients, cost, _ = await _campaign()
    assert recipients == 2
    assert float(cost) == 2 * 1.28, "the uncertain batch is in the cost"


@pytest.mark.asyncio
async def test_the_progress_follows_the_roster(store):
    before = await store.get_sms_send_progress("aug")
    assert (before["total"], before["done"], before["pending"]) == (3, 0, 3)

    await store.record_sms_send("aug", {1: "m-1"}, [], {}, final=False)
    middle = await store.get_sms_send_progress("aug")
    assert (middle["done"], middle["accepted"], middle["pending"]) == (1, 1, 2)

    await store.record_sms_send("aug", {1: "m-1"}, [], {}, uncertain=[2])
    after = await store.get_sms_send_progress("aug")
    assert (after["done"], after["pending"]) == (3, 0)
    assert (after["accepted"], after["uncertain"], after["notSent"]) == (1, 1, 1)
    assert after["failed"] == 0
    assert after["sentAt"] is not None


@pytest.mark.asyncio
async def test_the_progress_of_an_unknown_campaign_is_refused(store):
    with pytest.raises(ValueError):
        await store.get_sms_send_progress("nope")


# ─── sending the rest ─────────────────────────────────────────────────────

async def _statuses() -> dict:
    return {b: s for b, (_, s, _) in (await _members()).items()}


@pytest.mark.asyncio
async def test_the_rest_takes_only_the_never_sent(store):
    """1 went, 2 had no answer, 3 never left: only 3 is taken, and once."""
    await store.record_sms_send("aug", {1: "m-1"}, [], {}, uncertain=[2],
                                message_text="hi", message_parts=1, price_per_part=1.28)

    taken = await store.claim_sms_resend("aug")
    again = await store.claim_sms_resend("aug")

    assert [t["buyerId"] for t in taken] == [3]
    assert again == [], "a second press finds nobody left"
    statuses = await _statuses()
    assert statuses[2] == "OutcomeUnknown", "never taken: they may have the message"
    assert statuses[3] is None, "taken: on its way, as a send in progress"
    assert statuses[4] is None, "the control is never taken"


@pytest.mark.asyncio
async def test_a_rest_that_got_no_answer_goes_back_to_never_sent(store):
    await store.record_sms_send("aug", {1: "m-1"}, [], {}, message_text="hi",
                                message_parts=1, price_per_part=1.28)
    await store.claim_sms_resend("aug")

    await store.record_sms_send("aug", {}, [], {}, message_text="hi",
                                message_parts=1, price_per_part=1.28, resend=True)

    statuses = await _statuses()
    assert statuses[2] == statuses[3] == "NotSent", "sendable again"


@pytest.mark.asyncio
async def test_the_rest_adds_to_the_campaign_and_keeps_its_send_time(store):
    await store.record_sms_send("aug", {1: "m-1"}, [], {}, message_text="hi",
                                message_parts=1, price_per_part=1.28)
    before = await store.get_sms_campaign_message("aug")
    await store.claim_sms_resend("aug")

    await store.record_sms_send("aug", {2: "m-2", 3: "m-3"}, [], {},
                                message_text="hi", message_parts=1,
                                price_per_part=1.28, resend=True)

    recipients, cost, text = await _campaign()
    assert (recipients, float(cost), text) == (3, 3 * 1.28, "hi")
    assert (await store.get_sms_campaign_message("aug"))["sentAt"] == before["sentAt"]
    progress = await store.get_sms_send_progress("aug")
    assert (progress["accepted"], progress["notSent"], progress["pending"]) == (3, 0, 0)


@pytest.mark.asyncio
async def test_the_message_of_an_unknown_campaign_is_none(store):
    assert await store.get_sms_campaign_message("nope") is None
