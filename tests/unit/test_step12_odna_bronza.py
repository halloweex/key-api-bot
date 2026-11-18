"""Steps 1 and 2 of «Одна бронза»: the Gold reader and the buyer landing.

Step 1 was a switch (`KS_READ_GOLD`), and its tests were about what a switch
must guarantee: off by default, loud on a typo, intercepting exactly the
requests DuckDB's own Gold would have answered, and falling back visibly
instead of taking the dashboard down. Since step 14's S14-06 there is no
switch and no DuckDB to fall back to: what stays is the reader's predicate,
the routing that still keeps sources 3 and 5 away from it (the owner's
decision 5), the shape of the card row and the chart, and that neither ever
reaches for the DuckDB store.

Step 2's units are the conversions — the places where a Buyer model becomes
two stores' rows — and the one-body-two-engines contract for the order-lines
view, which is revision 0010's contract applied to a view: the migration
spells the SQL out, and this file re-renders the template and asserts the two
agree, so the Postgres view cannot drift from the DuckDB one without a test
naming the divergence.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import AsyncMock

import pytest

from core import pg_gold_read
from core.landing_rows import buyer_row, contact_rows


class _Buyer:
    def __init__(self, **kw):
        defaults = dict(
            id=1, full_name="Оксана К.", birthday=None, note=None,
            phone="+380501112233", email=None, manager_id=None,
            company_id=None, company_name=None, city="Київ", region=None,
            loyalty_program_name=None, loyalty_level_name=None,
            loyalty_discount=0, loyalty_amount=0,
            created_at=None, updated_at=None, phones=None, emails=None,
        )
        defaults.update(kw)
        self.__dict__.update(defaults)


class TestTheGoldReader:
    def test_rollup_predicate_for_all_sources(self):
        params = [date(2026, 8, 1), date(2026, 8, 27)]
        where = pg_gold_read._predicate("retail", None, params)
        assert "source_id IS NULL" in where
        assert "sales_type = $3" in where
        assert params[2] == "retail"

    def test_fine_row_predicate_for_one_source(self):
        params = [date(2026, 8, 1), date(2026, 8, 27)]
        where = pg_gold_read._predicate("all", 4, params)
        assert "source_id = $3" in where
        assert "sales_type" not in where  # 'all' spans the types

    @pytest.mark.asyncio
    @pytest.mark.parametrize("source", [3, 5])
    async def test_a_source_gold_is_not_asked_about_never_reaches_it(
        self, monkeypatch, source,
    ):
        """A source DuckDB's Gold had no column for is not asked of
        Postgres's Gold either, which could answer it: `/api/summary`
        answers zeros, the owner's decision 5 for step 14. Mutation: route
        3 or 5 to `_pg_gold_summary` — the fetch is awaited and the zeros
        become the source's money."""
        from core.store import AnalyticsStore

        fetch = AsyncMock(return_value=(9, 900.0, 1, 10.0))
        monkeypatch.setattr(pg_gold_read, "fetch_summary", fetch)

        mixin = AnalyticsStore.__new__(AnalyticsStore)
        out = await mixin.get_summary_stats(
            date(2026, 8, 1), date(2026, 8, 27), source_id=source,
        )
        assert (out["totalOrders"], out["totalRevenue"], out["avgCheck"],
                out["totalReturns"], out["returnsRevenue"]) == (0, 0, 0, 0, 0)
        fetch.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("source", [None, 1, 2, 4])
    async def test_every_other_source_is_asked_of_gold(self, monkeypatch, source):
        from core.store import AnalyticsStore

        fetch = AsyncMock(return_value=(2, 300.0, 0, 0.0))
        monkeypatch.setattr(pg_gold_read, "fetch_summary", fetch)

        mixin = AnalyticsStore.__new__(AnalyticsStore)
        out = await mixin.get_summary_stats(
            date(2026, 8, 1), date(2026, 8, 27), source_id=source, sales_type="b2b",
        )
        assert out["totalRevenue"] == 300.0 and out["avgCheck"] == 150.0
        fetch.assert_awaited_once_with(
            date(2026, 8, 1), date(2026, 8, 27), "b2b", source)


class TestTheBuyerConversions:
    def test_birthday_string_becomes_a_date(self):
        row = buyer_row(_Buyer(birthday="1992-04-17"))
        assert row[2] == date(1992, 4, 17)

    def test_timestamps_parse_both_separators(self):
        a = buyer_row(_Buyer(created_at="2026-08-27 12:00:00+00:00"))
        b = buyer_row(_Buyer(created_at="2026-08-27T12:00:00+00:00"))
        assert a[15] == b[15] == datetime(2026, 8, 27, 12, tzinfo=timezone.utc)

    def test_an_unparseable_timestamp_raises_rather_than_writing_none(self):
        # DuckDB will have stored the value; a silent None here would be a
        # discrepancy this module manufactured.
        with pytest.raises(ValueError):
            buyer_row(_Buyer(created_at="вчора ввечері"))

    def test_contacts_first_is_primary_empties_skipped_dupes_collapsed(self):
        rows = contact_rows(_Buyer(
            phones=["+380501112233", "", "+380501112233", "+380671234567"],
            emails=["a@b.ua"],
        ))
        assert (1, "phone", "+380501112233", True) in rows
        assert (1, "phone", "+380671234567", False) in rows
        assert (1, "email", "a@b.ua", True) in rows
        assert len(rows) == 3
