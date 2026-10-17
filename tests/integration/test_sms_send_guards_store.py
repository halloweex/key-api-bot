"""Who a campaign may reach, and who a follow-up is — asked of the store itself.

Two rules sit between a claimed campaign and the gateway, and one roster is
built from another campaign's:

* `find_sms_send_conflicts` — another campaign sent in the last day reached
  these phones. Its control arm is never negotiable; its target arm is, but
  only by a declared follow-up of it (or of the campaign it follows).
* `get_sms_follow_up_audience` — the people campaign X reached: its target
  arm, accepted by the gateway, less anyone opted out since. No control of
  its own; X's control is never in it.

Run against DuckDB always and against PostgreSQL wherever `KS_PG_DSN` names
one, through the real `_sms_run` / `_sms_tx` switch (`KS_SMS_STORE`), because
production SMS is Postgres and the two engines are one SQL text here.
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from core.duckdb_store import DuckDBStore

DSN = os.getenv("KS_PG_DSN")


@pytest_asyncio.fixture(params=["duckdb", "postgres"])
async def store(request, tmp_path, monkeypatch):
    """A store whose SMS statements run on one engine or the other."""
    duck = DuckDBStore(db_path=tmp_path / "guards.duckdb")
    await duck.connect()
    try:
        if request.param == "duckdb":
            monkeypatch.delenv("KS_SMS_STORE", raising=False)
            yield duck
            return

        if not DSN:
            pytest.skip("needs a live PostgreSQL at KS_PG_DSN")
        asyncpg = pytest.importorskip("asyncpg")
        # A real pool, not one connection: the concurrency test claims two
        # campaigns at once, and one connection would serialise them.
        pool = await asyncpg.create_pool(DSN, min_size=1, max_size=4)
        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    "TRUNCATE app.sms_campaigns, app.sms_campaign_members,"
                    " app.marketing_optouts, app.sms_dlr_events")
            monkeypatch.setenv("KS_SMS_STORE", "postgres")
            with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
                 patch("core.pg.require_revision", new=AsyncMock()):
                yield duck
        finally:
            await pool.close()
    finally:
        await duck.close()


def _phone(buyer_id: int) -> str:
    return f"38050{buyer_id:07d}"


def _member(buyer_id: int, assignment: str = "target", phone: str | None = None) -> dict:
    return {
        "buyerId": buyer_id, "phone": phone or _phone(buyer_id), "tier": "ALL",
        "assignment": assignment, "orders": 2, "revenueLtv": 1000.0,
        "marginLtv": 500.0, "recencyDays": 30,
    }


async def _freeze(store, campaign: str, members: list, follow_up_of: str | None = None):
    criteria = {"grouping": "single"}
    if follow_up_of:
        criteria["followUpOf"] = follow_up_of
    return await store.freeze_sms_campaign(
        campaign=campaign, customers=members, criteria=criteria,
        ltv_basis="margin", sales_type="retail",
        holdout_pct=0 if follow_up_of else 10,
    )


async def _send(store, campaign: str, hours_ago: float, refused: tuple = ()):
    """Claimed and recorded the way the send route does it, `hours_ago` ago.

    Buyers in `refused` are the ones the gateway did not accept.
    """
    targets = await store.get_sms_campaign_targets(campaign)
    accepted = {t["buyerId"]: f"m-{campaign}-{t['buyerId']}"
                for t in targets if t["buyerId"] not in refused}
    failed = {b: "Rejected" for b in refused}
    await store.record_sms_send(
        campaign, accepted, [], failed,
        sent_at=datetime.now() - timedelta(hours=hours_ago),
    )


async def _freeze_follow_up(store, campaign: str, root: str):
    """Frozen the way the route freezes one: the store's own audience."""
    audience = await store.get_sms_follow_up_audience(
        root, campaign=campaign, include_customers=True,
    )
    await store.freeze_sms_campaign(
        campaign=campaign, customers=audience["customers"],
        criteria=audience["criteria"], ltv_basis=audience["ltvBasis"],
        sales_type=audience["salesType"], holdout_pct=0,
    )
    return audience


# ─── Another campaign's people, in the last day ──────────────────────────────

class TestRecentCampaigns:

    @pytest.mark.asyncio
    async def test_a_target_overlap_an_hour_old_is_named(self, store):
        await _freeze(store, "first", [_member(1), _member(2), _member(3, "holdout")])
        await _send(store, "first", hours_ago=1)
        await _freeze(store, "second", [_member(2), _member(9)])
        await store.get_sms_campaign_targets("second")   # the claim comes first

        conflicts = await store.find_sms_send_conflicts("second")

        assert [(c["campaign"], c["arm"], c["people"]) for c in conflicts] == [
            ("first", "target", 1),
        ]
        assert conflicts[0]["sentAt"], "the other campaign's send time is named"

    @pytest.mark.asyncio
    async def test_twenty_five_hours_later_it_passes(self, store):
        await _freeze(store, "first", [_member(1), _member(2)])
        await _send(store, "first", hours_ago=25)
        await _freeze(store, "second", [_member(2)])
        await store.get_sms_campaign_targets("second")

        assert await store.find_sms_send_conflicts("second") == []

    @pytest.mark.asyncio
    async def test_a_control_overlap_is_named_as_control(self, store):
        await _freeze(store, "first", [_member(1), _member(2, "holdout")])
        await _send(store, "first", hours_ago=3)
        await _freeze(store, "second", [_member(2)])
        await store.get_sms_campaign_targets("second")

        conflicts = await store.find_sms_send_conflicts("second")

        assert [(c["campaign"], c["arm"]) for c in conflicts] == [("first", "control")]

    @pytest.mark.asyncio
    async def test_an_unsent_campaign_is_no_conflict(self, store):
        """Frozen and never claimed: nobody was messaged."""
        await _freeze(store, "first", [_member(1)])
        await _freeze(store, "second", [_member(1)])
        await store.get_sms_campaign_targets("second")

        assert await store.find_sms_send_conflicts("second") == []

    @pytest.mark.asyncio
    async def test_a_campaign_in_flight_counts(self, store):
        """Claimed, its gateway call still running: `sent_at` is stamped by the
        claim, before any message id exists, and that is what is read."""
        await _freeze(store, "first", [_member(1)])
        await store.get_sms_campaign_targets("first")      # claimed, not recorded
        await _freeze(store, "second", [_member(1)])
        await store.get_sms_campaign_targets("second")

        assert [c["campaign"] for c in await store.find_sms_send_conflicts("second")] \
            == ["first"]

    @pytest.mark.asyncio
    async def test_matched_on_the_phone_not_the_buyer_record(self, store):
        """One person under two buyer records still has one handset."""
        await _freeze(store, "first", [_member(1)])
        await _send(store, "first", hours_ago=1)
        await _freeze(store, "second", [_member(77, phone=_phone(1))])
        await store.get_sms_campaign_targets("second")

        assert [c["campaign"] for c in await store.find_sms_send_conflicts("second")] \
            == ["first"]

    @pytest.mark.asyncio
    async def test_two_overlapping_claims_at_once_both_see_the_other(self, store):
        """The check runs after the claim, so neither can slip past the other."""
        await _freeze(store, "left", [_member(1), _member(2)])
        await _freeze(store, "right", [_member(2), _member(3)])

        await asyncio.gather(
            store.get_sms_campaign_targets("left"),
            store.get_sms_campaign_targets("right"),
        )
        left, right = await asyncio.gather(
            store.find_sms_send_conflicts("left"),
            store.find_sms_send_conflicts("right"),
        )

        assert [c["campaign"] for c in left] == ["right"]
        assert [c["campaign"] for c in right] == ["left"]


# ─── A declared follow-up ───────────────────────────────────────────────────

class TestFollowUpOverlap:

    @pytest.mark.asyncio
    async def test_a_follow_up_may_reach_its_roots_targets(self, store):
        await _freeze(store, "root", [_member(1), _member(2), _member(3, "holdout")])
        await _send(store, "root", hours_ago=1)
        await _freeze_follow_up(store, "root-last-day", "root")
        await store.get_sms_campaign_targets("root-last-day")

        assert await store.find_sms_send_conflicts("root-last-day") == []

    @pytest.mark.asyncio
    async def test_two_follow_ups_of_one_root_an_hour_apart_pass(self, store):
        await _freeze(store, "root", [_member(1), _member(2)])
        await _send(store, "root", hours_ago=2)
        await _freeze_follow_up(store, "root-noon", "root")
        await _send(store, "root-noon", hours_ago=1)
        await _freeze_follow_up(store, "root-evening", "root")
        await store.get_sms_campaign_targets("root-evening")

        assert await store.find_sms_send_conflicts("root-evening") == []

    @pytest.mark.asyncio
    async def test_a_follow_up_still_refuses_a_different_campaigns_targets(self, store):
        await _freeze(store, "root", [_member(1), _member(2)])
        await _send(store, "root", hours_ago=3)
        await _freeze(store, "other", [_member(2), _member(5)])
        await _send(store, "other", hours_ago=1)
        await _freeze_follow_up(store, "root-last-day", "root")
        await store.get_sms_campaign_targets("root-last-day")

        conflicts = await store.find_sms_send_conflicts("root-last-day")

        assert [(c["campaign"], c["arm"], c["people"]) for c in conflicts] == [
            ("other", "target", 1),
        ]

    @pytest.mark.asyncio
    async def test_a_control_overlap_refuses_even_a_declared_follow_up(self, store):
        """No declaration buys back a control group. The roster here is built
        by hand — the store's own follow-up audience could never contain the
        root's control — to prove the rule does not rest on that alone."""
        await _freeze(store, "root", [_member(1), _member(2, "holdout")])
        await _send(store, "root", hours_ago=1)
        await _freeze(store, "forged", [_member(1), _member(2)], follow_up_of="root")
        await store.get_sms_campaign_targets("forged")

        conflicts = await store.find_sms_send_conflicts("forged")

        assert [(c["campaign"], c["arm"], c["people"]) for c in conflicts] == [
            ("root", "control", 1),
        ]


# ─── The follow-up's roster ─────────────────────────────────────────────────

class TestFollowUpAudience:

    @pytest.mark.asyncio
    async def test_it_is_the_roots_accepted_targets_less_opt_outs_since(self, store):
        await _freeze(store, "root", [
            _member(1), _member(2), _member(3), _member(4, "holdout"), _member(5),
        ])
        await _send(store, "root", hours_ago=2, refused=(5,))
        # Opted out after the first message: by buyer, and — under a second
        # buyer record — by phone.
        await store.add_marketing_optout(buyer_id=2)
        await store.add_marketing_optout(buyer_id=99, phone=_phone(3))

        audience = await store.get_sms_follow_up_audience(
            "root", campaign="root-last-day", include_customers=True,
        )

        assert [c["buyerId"] for c in audience["customers"]] == [1]
        assert all(c["assignment"] == "target" for c in audience["customers"])
        assert audience["totals"] == {"customers": 1, "target": 1, "holdout": 0}
        assert audience["criteria"]["followUpOf"] == "root"
        assert audience["criteria"]["holdoutPct"] == 0
        assert [(s["stage"], s["remaining"]) for s in audience["funnel"]] == [
            ("rootTarget", 4), ("accepted", 3), ("subscribed", 1),
        ]

    @pytest.mark.asyncio
    async def test_frozen_it_has_no_control_and_none_of_the_roots(self, store):
        await _freeze(store, "root", [_member(1), _member(2), _member(3, "holdout")])
        await _send(store, "root", hours_ago=2)

        await _freeze_follow_up(store, "root-last-day", "root")
        targets = await store.get_sms_campaign_targets("root-last-day")
        listed = {c["campaign"]: c for c in await store.list_sms_campaigns()}

        assert sorted(t["buyerId"] for t in targets) == [1, 2]
        assert listed["root-last-day"]["holdout"] == 0
        assert listed["root-last-day"]["holdoutPct"] == 0
        assert listed["root-last-day"]["followUpOf"] == "root"
        assert listed["root"]["followUpOf"] is None

    @pytest.mark.asyncio
    async def test_an_unsent_root_is_refused(self, store):
        await _freeze(store, "root", [_member(1)])

        with pytest.raises(ValueError, match="has not been sent"):
            await store.get_sms_follow_up_audience("root")

    @pytest.mark.asyncio
    async def test_a_follow_up_of_a_follow_up_is_refused(self, store):
        await _freeze(store, "root", [_member(1)])
        await _send(store, "root", hours_ago=2)
        await _freeze_follow_up(store, "root-noon", "root")
        await _send(store, "root-noon", hours_ago=1)

        with pytest.raises(ValueError, match="itself a follow-up of 'root'"):
            await store.get_sms_follow_up_audience("root-noon")

    @pytest.mark.asyncio
    async def test_an_unknown_root_is_not_frozen(self, store):
        with pytest.raises(ValueError, match="not frozen"):
            await store.get_sms_follow_up_audience("nobody")


class TestFollowUpResults:

    @pytest.mark.asyncio
    async def test_the_root_lists_its_follow_ups_and_the_follow_up_names_its_root(
        self, store,
    ):
        await _freeze(store, "root", [_member(1), _member(2), _member(3, "holdout")])
        await _send(store, "root", hours_ago=2)
        await _freeze_follow_up(store, "root-last-day", "root")
        await _send(store, "root-last-day", hours_ago=1)

        root = await store.get_sms_campaign_results("root")
        follow_up = await store.get_sms_campaign_results("root-last-day")

        assert root["followUpOf"] is None
        assert [(f["campaign"], f["recipients"]) for f in root["followUps"]] == [
            ("root-last-day", 2),
        ]
        assert root["followUps"][0]["sentAt"]
        assert follow_up["followUpOf"] == "root"
        assert follow_up["followUps"] == []
        assert follow_up["overall"]["comparison"] is None, \
            "a follow-up has no control, so nothing to compare against"
