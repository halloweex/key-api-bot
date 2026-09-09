"""The first check here that reads meaning rather than fidelity.

On 2026-07-20/21 the shop's order-comment template changed and stopped
carrying `utm_source`, `utm_medium` and `utm_campaign`. Website campaign
coverage fell from 34% to 5.5% and stayed there five weeks. Nothing said so,
and nothing *could*: the reconciliations compare four stores against each
other and all four faithfully recorded the absence; `validation_passed`
checksums revenue, which never moved; and the platform chart counted
pixel-only orders as Facebook, so it did not move either.

These tests are the outage, replayed at both ends of it.
"""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pytest

from core.data_quality import Severity, _attribution_coverage_check
from core.duckdb_store import DuckDBStore

SHOPIFY = 4
INSTAGRAM = 1


async def _store(tmp_path: Path) -> DuckDBStore:
    s = DuckDBStore(db_path=tmp_path / "coverage.duckdb")
    await s.connect()
    return s


async def _week(
    store: DuckDBStore, *, days_ago: int, orders: int, tagged: int,
    source_id: int = SHOPIFY, first_id: int = 1,
) -> int:
    """`orders` website orders on one day, `tagged` of them with a campaign."""
    day = date.today() - timedelta(days=days_ago)
    async with store.connection() as conn:
        for i in range(orders):
            oid = first_id + i
            conn.execute(
                """
                INSERT INTO silver_orders (
                    id, source_id, status_id, grand_total, ordered_at, buyer_id,
                    manager_id, order_date, is_return, sales_type,
                    is_active_source, source_name, is_new_customer,
                    buyer_first_order_date, promocode
                ) VALUES (?, ?, 12, 100.0, NULL, NULL, NULL, ?, FALSE,
                          'retail', TRUE, 'Shopify', FALSE, NULL, NULL)
                """,
                [oid, source_id, day],
            )
            # Every order carries the pixels and the page language, exactly as
            # production does — the outage left those untouched, which is why
            # it looked like nothing had happened.
            conn.execute(
                """
                INSERT INTO silver_order_utm
                    (order_id, utm_source, utm_medium, utm_campaign, utm_lang,
                     fbp, ttp, traffic_type, platform, parsed_at)
                VALUES (?, ?, ?, ?, 'uk', 'fb.1', 'T',
                        ?, ?, CURRENT_TIMESTAMP)
                """,
                [oid,
                 'fbads' if i < tagged else None,
                 'cpc' if i < tagged else None,
                 f'camp_{oid}' if i < tagged else None,
                 'paid_confirmed' if i < tagged else 'pixel_only',
                 'facebook' if i < tagged else 'unattributed'],
            )
    return first_id + orders


async def _run(store, **kw):
    """The check takes the raw connection the store lends out."""
    async with store.connection() as conn:
        return _attribution_coverage_check(conn, **kw)


async def _integrity(store):
    from core.data_quality import check_internal_integrity

    async with store.connection() as conn:
        return check_internal_integrity(conn)


class TestTheOutageIsSeen:

    @pytest.mark.asyncio
    async def test_a_healthy_week_says_nothing(self, tmp_path):
        """June ran at 34% coverage for weeks. A check that fires there is a
        check nobody will read by August."""
        store = await _store(tmp_path)
        nxt = await _week(store, days_ago=3, orders=100, tagged=34)
        await _week(store, days_ago=20, orders=200, tagged=68, first_id=nxt)

        assert await _run(store) == []

    @pytest.mark.asyncio
    async def test_the_july_collapse_is_reported(self, tmp_path):
        """8% in the window against 30% before it — the second week of the
        real outage, which is when this should have spoken."""
        store = await _store(tmp_path)
        nxt = await _week(store, days_ago=3, orders=100, tagged=8)
        await _week(store, days_ago=20, orders=200, tagged=60, first_id=nxt)

        issues = await _run(store)
        assert len(issues) == 1
        issue = issues[0]
        assert issue.check_name == "attribution_coverage_website"
        assert issue.severity is Severity.WARN
        assert "8%" in issue.description
        assert "30%" in issue.description, "the reader needs the number it fell from"
        assert issue.count == 92, "the orders that arrived without a tag"

    @pytest.mark.asyncio
    async def test_a_halving_fires_before_it_reaches_the_floor(self, tmp_path):
        """34% → 16% is the same failure caught early, and a floor of 15%
        would sleep through it."""
        store = await _store(tmp_path)
        nxt = await _week(store, days_ago=3, orders=100, tagged=16)
        await _week(store, days_ago=20, orders=200, tagged=68, first_id=nxt)

        issues = await _run(store)
        assert len(issues) == 1
        assert "16%" in issues[0].description

    @pytest.mark.asyncio
    async def test_a_slow_slide_within_half_is_not_news(self, tmp_path):
        """30% → 24% happened over June and July and is not an incident."""
        store = await _store(tmp_path)
        nxt = await _week(store, days_ago=3, orders=100, tagged=24)
        await _week(store, days_ago=20, orders=200, tagged=60, first_id=nxt)

        assert await _run(store) == []


class TestItRefusesToGuess:

    @pytest.mark.asyncio
    async def test_too_few_orders_says_nothing(self, tmp_path):
        """A quiet week must not read as an outage: three untagged orders out
        of five is 40% of nothing."""
        store = await _store(tmp_path)
        await _week(store, days_ago=3, orders=5, tagged=0)

        assert await _run(store) == []

    @pytest.mark.asyncio
    async def test_no_baseline_still_honours_the_floor(self, tmp_path):
        """A new shop, or a database that has just been rebuilt, has no
        trailing month. The floor is absolute and still applies."""
        store = await _store(tmp_path)
        await _week(store, days_ago=3, orders=100, tagged=5)

        issues = await _run(store)
        assert len(issues) == 1
        assert "previous" not in issues[0].description, "no baseline to cite"

    @pytest.mark.asyncio
    async def test_hand_taken_orders_are_not_measured(self, tmp_path):
        """An order typed into the Instagram inbox cannot carry a tag and
        never will. Counting those would measure the channel mix and call a
        good week an outage."""
        store = await _store(tmp_path)
        # Website coverage is healthy; the Instagram orders outnumber it and
        # carry nothing.
        nxt = await _week(store, days_ago=3, orders=100, tagged=34)
        nxt = await _week(store, days_ago=20, orders=200, tagged=68, first_id=nxt)
        await _week(store, days_ago=3, orders=300, tagged=0,
                    source_id=INSTAGRAM, first_id=nxt)

        assert await _run(store) == []


class TestItIsWiredIn:

    @pytest.mark.asyncio
    async def test_the_integrity_run_includes_it(self, tmp_path):
        """A check nobody calls is a check that does not exist. This is the
        job that runs four times a day and feeds the 09:00 digest."""
        store = await _store(tmp_path)
        nxt = await _week(store, days_ago=3, orders=100, tagged=4)
        await _week(store, days_ago=20, orders=200, tagged=60, first_id=nxt)

        names = {i.check_name for i in await _integrity(store)}
        assert "attribution_coverage_website" in names

    def test_it_names_a_lever_that_is_not_in_this_repository(self):
        """`REMEDIATION` exists so a finding says what to do. The honest
        answer here is that the fix is on the website, and saying "run the
        warehouse refresh" would send somebody to rebuild a copy of the
        absence."""
        from core.data_quality import remediation_for

        lever = " ".join(remediation_for(["attribution_coverage_website"]))
        assert lever, "the check has no remediation entry"
        assert "utm_source" in lever
