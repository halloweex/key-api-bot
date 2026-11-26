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
