"""Can the gateway's export still be replayed after the event-id binding?

This matters more than its size suggests. ~48 % of TurboSMS delivery reports
are lost on every send to a capacity limit whose cause is still unknown, and
the only way anybody has ever got them back is to export the xlsx from the
gateway's panel and **replay it through the webhook** — which is the sole write
path for those columns. That recovery ran once, on 2026-08-27, as ad-hoc code.

On 2026-08-30 the webhook gained a binding: the first report under an event id
fixes which message that id may ever speak for. It is what stops one captured
`(id, signature)` pair from rewriting an arbitrary recipient's delivery state,
because the gateway signs `SHA1(secret + id)` and nothing else — the message id
being written is outside the proof.

The two interact, and the interaction is invisible until someone is repairing a
campaign under time pressure. These tests pin which replay shapes survive it:

* an event id **derived from the message id** replays, and replays again — the
  binding sees the same pair and allows it, so the repair is idempotent and a
  half-finished run can simply be re-run;
* a **positional** event id (row number, counter, timestamp) works once and
  then destroys itself the moment the export is re-sorted or a row is filtered
  out, because id 7 then names a different message than it did before.

`record_sms_delivery` is exercised against a real store rather than a mock: the
binding lives in SQL, and a mock would agree with whatever the test expected.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from core.duckdb_store import DuckDBStore
from core.repositories.customers import DlrEventRebound

UTC = timezone.utc


async def _store_with_roster(tmp_path: Path) -> DuckDBStore:
    """Two recipients of one campaign, each with the gateway's message id."""
    store = DuckDBStore(db_path=tmp_path / "dlr.duckdb")
    await store.connect()
    async with store.connection() as conn:
        conn.execute(
            "INSERT INTO sms_campaigns (campaign, ltv_basis, sales_type, "
            " holdout_pct, criteria) VALUES ('aug', 'revenue', 'retail', 10, '{}')"
        )
        for buyer, msg in ((1, "msg-aaa"), (2, "msg-bbb")):
            conn.execute(
                "INSERT INTO sms_campaign_members (campaign, buyer_id, phone, "
                " tier, assignment, orders_at_export, message_id, "
                " delivery_status) VALUES "
                f"('aug', {buyer}, '38050000000{buyer}', 'VIP', 'target', 3, "
                f"'{msg}', 'Accepted')"
            )
    return store


async def _replay(store, event_id: str, message_id: str, status: str = "DELIVRD"):
    return await store.record_sms_delivery(
        message_id=message_id,
        status=status,
        delivered=True,
        delivered_at=datetime(2026, 8, 27, 12, 0, tzinfo=UTC),
        event_id=event_id,
    )


async def _delivered(store, message_id: str):
    async with store.connection() as conn:
        return conn.execute(
            "SELECT delivered, delivery_status FROM sms_campaign_members "
            "WHERE message_id = ?", [message_id],
        ).fetchone()


class TestTheSupportedReplayShape:
    """Derive the event id from the message id and the repair is re-runnable."""

    @pytest.mark.asyncio
    async def test_a_message_derived_id_replays_and_replays_again(self, tmp_path):
        store = await _store_with_roster(tmp_path)
        try:
            for _attempt in range(2):
                for msg in ("msg-aaa", "msg-bbb"):
                    known = await _replay(store, f"replay-{msg}", msg)
                    assert known is True

            assert (await _delivered(store, "msg-aaa"))[0] is True
            assert (await _delivered(store, "msg-bbb"))[0] is True
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_it_survives_the_export_being_reordered(self, tmp_path):
        """The second run sends the same rows in the opposite order, which is
        what happens when someone re-sorts the sheet or filters it down."""
        store = await _store_with_roster(tmp_path)
        try:
            for msg in ("msg-aaa", "msg-bbb"):
                await _replay(store, f"replay-{msg}", msg)
            for msg in ("msg-bbb", "msg-aaa"):
                assert await _replay(store, f"replay-{msg}", msg) is True
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_live_callback_afterwards_is_not_refused(self, tmp_path):
        """The gateway's own ids never collide with `replay-` ones, so the
        repair does not spend the event ids real callbacks still need."""
        store = await _store_with_roster(tmp_path)
        try:
            await _replay(store, "replay-msg-aaa", "msg-aaa")
            assert await _replay(store, "gateway-event-1", "msg-aaa") is True
        finally:
            await store.close()


class TestTheShapeThatBreaks:
    """A positional id is fine once and lethal on the second run."""

    @pytest.mark.asyncio
    async def test_a_row_number_id_rebinds_when_the_export_is_reordered(
        self, tmp_path,
    ):
        store = await _store_with_roster(tmp_path)
        try:
            for n, msg in enumerate(("msg-aaa", "msg-bbb")):
                await _replay(store, f"row-{n}", msg)

            # Same rows, opposite order — `row-0` now names msg-bbb.
            with pytest.raises(DlrEventRebound) as caught:
                await _replay(store, "row-0", "msg-bbb")

            assert caught.value.bound_message_id == "msg-aaa"
            assert caught.value.message_id == "msg-bbb"
        finally:
            await store.close()


class TestTheBindingStillDoesItsJob:
    """The replay shape above must not have made the control toothless."""

    @pytest.mark.asyncio
    async def test_a_captured_pair_cannot_be_repointed(self, tmp_path):
        store = await _store_with_roster(tmp_path)
        try:
            await _replay(store, "captured-id", "msg-aaa")
            with pytest.raises(DlrEventRebound):
                await _replay(store, "captured-id", "msg-bbb", status="REJECTD")

            # and the other recipient's state was not touched
            assert (await _delivered(store, "msg-bbb"))[1] == "Accepted"
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_an_id_spent_on_an_unknown_message_stays_spent(self, tmp_path):
        """`record_sms_delivery` binds even when the roster does not know the
        message — otherwise an event aimed at nothing stays re-pointable."""
        store = await _store_with_roster(tmp_path)
        try:
            assert await _replay(store, "wasted", "msg-not-ours") is False
            with pytest.raises(DlrEventRebound):
                await _replay(store, "wasted", "msg-aaa")
        finally:
            await store.close()
