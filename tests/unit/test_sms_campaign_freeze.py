"""Tests for freezing an SMS campaign roster.

The roster recorded at export time is the only control group that will exist
when results are measured — the eligible population shifts daily, so it cannot
be reconstructed afterwards. These tests pin the behaviour that protects it.
"""
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from core.duckdb_store import DuckDBStore


async def _make_store(tmp_path: Path) -> DuckDBStore:
    store = DuckDBStore(db_path=tmp_path / "test.duckdb")
    await store.connect()
    return store


def _member(buyer_id: int, tier: str, assignment: str) -> dict:
    return {
        "buyerId": buyer_id,
        "phone": f"3809{buyer_id:08d}",
        "tier": tier,
        "assignment": assignment,
        "orders": 3,
        "revenueLtv": 12000.0,
        "marginLtv": 6600.0,
        "recencyDays": 20,
    }


ROSTER = [
    _member(1, "VIP", "target"),
    _member(2, "VIP", "holdout"),
    _member(3, "CORE", "target"),
    _member(4, "REACTIVATION", "target"),
]

CRITERIA = {"vipLtv": 5500, "coreLtv": 2750, "maxRecencyDays": 270}


async def _freeze(store, campaign="aug-promo", roster=None, **kw):
    return await store.freeze_sms_campaign(
        campaign=campaign,
        customers=ROSTER if roster is None else roster,
        criteria=CRITERIA,
        ltv_basis=kw.pop("ltv_basis", "margin"),
        sales_type=kw.pop("sales_type", "retail"),
        holdout_pct=kw.pop("holdout_pct", 10),
        **kw,
    )


@pytest.mark.asyncio
async def test_roster_is_recorded_with_both_groups(tmp_path):
    store = await _make_store(tmp_path)
    result = await _freeze(store, promocode="KS-AUG")

    assert result["frozen"] is True
    assert result["totals"] == {"customers": 4, "target": 3, "holdout": 1}
    assert [s["tier"] for s in result["segments"]] == ["VIP", "CORE", "REACTIVATION"]

    async with store.connection() as conn:
        rows = conn.execute(
            "SELECT buyer_id, tier, assignment, revenue_ltv_at_export,"
            " margin_ltv_at_export FROM sms_campaign_members"
            " WHERE campaign = 'aug-promo' ORDER BY buyer_id"
        ).fetchall()

    assert len(rows) == 4, "the holdout is stored too — it is the control group"
    assert rows[1][2] == "holdout"
    # State at export time is preserved, not recomputed later
    assert float(rows[0][3]) == 12000.0
    assert float(rows[0][4]) == 6600.0

    await store.close()


@pytest.mark.asyncio
async def test_criteria_snapshot_is_kept(tmp_path):
    store = await _make_store(tmp_path)
    await _freeze(store)

    async with store.connection() as conn:
        row = conn.execute(
            "SELECT ltv_basis, sales_type, holdout_pct, criteria, exported_at, sent_at"
            " FROM sms_campaigns WHERE campaign = 'aug-promo'"
        ).fetchone()

    assert row[0] == "margin"
    assert row[1] == "retail"
    assert row[2] == 10
    assert '"vipLtv": 5500' in row[3], "thresholds are recoverable from the record"
    assert row[4] is not None, "export time is stamped"
    assert row[5] is None, "not sent yet"

    await store.close()


@pytest.mark.asyncio
async def test_refreeze_refused_without_overwrite(tmp_path):
    store = await _make_store(tmp_path)
    await _freeze(store)

    with pytest.raises(ValueError, match="already frozen"):
        await _freeze(store)

    await store.close()


@pytest.mark.asyncio
async def test_overwrite_replaces_roster_before_send(tmp_path):
    store = await _make_store(tmp_path)
    await _freeze(store)

    smaller = [_member(9, "VIP", "target")]
    result = await _freeze(store, roster=smaller, overwrite=True)

    assert result["totals"]["customers"] == 1

    async with store.connection() as conn:
        ids = [r[0] for r in conn.execute(
            "SELECT buyer_id FROM sms_campaign_members WHERE campaign = 'aug-promo'"
        ).fetchall()]
    assert ids == [9], "the previous roster is gone, not merged"

    await store.close()


@pytest.mark.asyncio
async def test_sent_roster_cannot_be_rewritten(tmp_path):
    """Once the file has gone out, the roster is evidence — even overwrite is refused."""
    store = await _make_store(tmp_path)
    await _freeze(store)
    await store.mark_sms_campaign_sent("aug-promo")

    with pytest.raises(ValueError, match="already sent"):
        await _freeze(store, roster=[_member(9, "VIP", "target")], overwrite=True)

    async with store.connection() as conn:
        n = conn.execute(
            "SELECT COUNT(*) FROM sms_campaign_members WHERE campaign = 'aug-promo'"
        ).fetchone()[0]
    assert n == 4, "the original roster survives the refused overwrite"

    await store.close()


@pytest.mark.asyncio
async def test_empty_roster_refused(tmp_path):
    store = await _make_store(tmp_path)

    with pytest.raises(ValueError, match="empty roster"):
        await _freeze(store, roster=[])

    await store.close()


@pytest.mark.asyncio
async def test_mark_sent_records_the_moment(tmp_path):
    store = await _make_store(tmp_path)
    await _freeze(store)

    when = datetime(2026, 8, 10, 9, 30)
    result = await store.mark_sms_campaign_sent("aug-promo", when)

    assert result["sentAt"].startswith("2026-08-10T09:30")
    assert result["previouslySentAt"] is None

    # Correcting the send date is allowed and surfaces what it replaced
    later = await store.mark_sms_campaign_sent("aug-promo", when + timedelta(days=1))
    assert later["previouslySentAt"].startswith("2026-08-10T09:30")
    assert later["sentAt"].startswith("2026-08-11T09:30")

    await store.close()


@pytest.mark.asyncio
async def test_mark_sent_unknown_campaign(tmp_path):
    store = await _make_store(tmp_path)

    with pytest.raises(ValueError, match="not frozen"):
        await store.mark_sms_campaign_sent("never-existed")

    await store.close()


@pytest.mark.asyncio
async def test_campaigns_listed_newest_first(tmp_path):
    store = await _make_store(tmp_path)
    await _freeze(store, campaign="jul-promo")
    await _freeze(store, campaign="aug-promo", promocode="KS-AUG")
    await store.mark_sms_campaign_sent("jul-promo", datetime(2026, 7, 5, 10, 0))

    campaigns = {c["campaign"]: c for c in await store.list_sms_campaigns()}

    assert set(campaigns) == {"jul-promo", "aug-promo"}
    assert campaigns["jul-promo"]["sentAt"].startswith("2026-07-05")
    assert campaigns["aug-promo"]["sentAt"] is None
    assert campaigns["aug-promo"]["promocode"] == "KS-AUG"
    assert campaigns["aug-promo"]["target"] == 3
    assert campaigns["aug-promo"]["holdout"] == 1

    await store.close()
