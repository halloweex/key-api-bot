"""An audience is groups of conditions, and a customer is in it if any holds.

The owner's campaign could not be said before this: "two or more purchases in
2026" OR "a purchase in the last three months". Filters were ANDed, the order
count was lifetime, and the recency window was one number for the whole
audience — so the second group's 92 days would have cut the first group down
to the same 92 days.

These tests hold four promises:

* a group means the same thing whatever groups stand beside it;
* everything common to the campaign — phone, opt-out, one SMS per phone — runs
  on the union, so a person in two groups is one recipient;
* "orders in a period" counts what the rest of the audience counts, and
  nothing else (no returns, no retired sources, the campaign's sales type);
* one group renders exactly the predicate it rendered before groups existed.

The store runs here on DuckDB. The same expectations against PostgreSQL, and
the comparison with the query as it stood before groups, are in
`tests/integration/test_sms_audience_groups_two_engines.py`.
"""
from __future__ import annotations

from datetime import date

import pytest
import pytest_asyncio

from core.duckdb_store import DuckDBStore
from core.repositories.customers import (
    MAX_AUDIENCE_GROUPS, SmsAudienceFilters, SmsAudienceGroup,
    audience_group_fragments,
)
from core.sql_dialect import DUCKDB, TODAY_IN_KYIV, sms_segments_select
from tests import sms_audience_fixture as fx


def group_a(window: int = 365) -> SmsAudienceGroup:
    """Two or more orders since `fx.period_from()`. A function where B is a
    constant because it carries a date, and a date built at import is the
    collection's day, not the query's (the fixture's docstring says why)."""
    return SmsAudienceGroup(
        window, SmsAudienceFilters(period_orders_min=2, period_from=fx.period_from()),
    )


GROUP_B = SmsAudienceGroup(92, SmsAudienceFilters())


class TestTheCondition:
    """Orders in a period, as a value."""

    def test_since_a_date_counts_from_that_date(self):
        sql, params = SmsAudienceFilters(
            period_orders_min=2, period_from=date(2026, 1, 1),
        ).period_count("period_orders_1")
        assert sql == ("COUNT(DISTINCT order_id) FILTER (WHERE order_date >= ?) "
                       "AS period_orders_1")
        assert params == [date(2026, 1, 1)]

    def test_within_n_days_is_spelled_as_recency_is(self):
        """`today - order_date <= N` is how `recency` itself is computed, so
        "an order within 92 days" and "last order at most 92 days ago" are one
        rule and agree on the 92nd day."""
        sql, params = SmsAudienceFilters(period_days=92).period_count("p")
        assert f"({TODAY_IN_KYIV} - order_date) <= ?" in sql
        assert params == [92]

    def test_a_period_without_a_count_means_at_least_one(self):
        sql, params = SmsAudienceFilters(period_days=92).predicate(
            "revenue_ltv", "retail", period_column="period_orders_1",
        )
        assert sql == "(period_orders_1 >= ?)"
        assert params == [1]

    @pytest.mark.parametrize("kwargs,message", [
        (dict(period_from=date(2026, 1, 1), period_days=30), "not both"),
        # The message names period_to since it exists; deliberately changed.
        (dict(period_orders_min=2), "needs period_from, period_to or period_days"),
        (dict(period_orders_min=0, period_days=30), "at least 1"),
        (dict(period_days=0), "at least 1"),
        (dict(period_to=date(2026, 1, 1), period_days=30), "not both"),
        (dict(period_from=date(2026, 3, 1), period_to=date(2026, 1, 1)),
         "period_from must not exceed period_to"),
        (dict(period_orders_max=0), "period_orders_max needs period_from, period_to"),
        (dict(period_orders_max=-1, period_days=30), "must not be negative"),
        (dict(period_orders_min=3, period_orders_max=2, period_days=30),
         "period_orders_min must not exceed period_orders_max"),
    ])
    def test_a_half_said_condition_cannot_be_built(self, kwargs, message):
        with pytest.raises(ValueError, match=message):
            SmsAudienceFilters(**kwargs)

    def test_since_a_date_and_within_n_days_render_what_they_rendered(self):
        """Byte for byte the text before `period_to` and `period_orders_max`
        existed — the from-alone and days-alone shapes, with and without a
        count. `TODAY_IN_KYIV` is the one shared spelling of today, pinned
        where it is defined."""
        since = SmsAudienceFilters(period_orders_min=2, period_from=date(2026, 1, 1))
        within = SmsAudienceFilters(period_days=92)
        assert since.period_count("period_orders_1") == (
            "COUNT(DISTINCT order_id) FILTER (WHERE order_date >= ?) AS period_orders_1",
            [date(2026, 1, 1)])
        assert within.period_count("period_orders_1") == (
            "COUNT(DISTINCT order_id) FILTER "
            f"(WHERE ({TODAY_IN_KYIV} - order_date) <= ?) AS period_orders_1",
            [92])
        assert since.predicate("revenue_ltv", "retail",
                               period_column="period_orders_1") == (
            "(period_orders_1 >= ?)", [2])
        assert within.predicate("revenue_ltv", "retail",
                                period_column="period_orders_1") == (
            "(period_orders_1 >= ?)", [1])

    def test_from_and_to_is_a_closed_range(self):
        sql, params = SmsAudienceFilters(
            period_from=date(2026, 1, 1), period_to=date(2026, 3, 31),
        ).period_count("p")
        assert sql == ("COUNT(DISTINCT order_id) FILTER "
                       "(WHERE order_date >= ? AND order_date <= ?) AS p")
        assert params == [date(2026, 1, 1), date(2026, 3, 31)]

    def test_to_alone_is_every_order_up_to_that_date(self):
        sql, params = SmsAudienceFilters(period_to=date(2026, 3, 31)).period_count("p")
        assert sql == "COUNT(DISTINCT order_id) FILTER (WHERE order_date <= ?) AS p"
        assert params == [date(2026, 3, 31)]

    def test_one_day_is_a_range(self):
        day = date(2026, 3, 8)
        f = SmsAudienceFilters(period_from=day, period_to=day)
        assert f.period_count("p")[1] == [day, day]

    def test_to_alone_is_a_period(self):
        f = SmsAudienceFilters(period_to=date(2026, 3, 31))
        assert f.has_period and not f.is_empty()
        assert f.predicate("revenue_ltv", "retail", period_column="p") == (
            "(p >= ?)", [1])

    @pytest.mark.parametrize("bounds,sql,params", [
        (dict(), "(p >= ?)", [1]),
        (dict(period_orders_min=2), "(p >= ?)", [2]),
        (dict(period_orders_max=3), "(p <= ?)", [3]),
        (dict(period_orders_max=0), "(p <= ?)", [0]),
        (dict(period_orders_min=2, period_orders_max=5), "(p >= ? AND p <= ?)", [2, 5]),
        (dict(period_orders_min=2, period_orders_max=2), "(p >= ? AND p <= ?)", [2, 2]),
    ], ids=["neither", "min", "max", "none-in-period", "both", "exactly"])
    def test_the_count_has_two_bounds(self, bounds, sql, params):
        """No bound is "at least one", as it always was; an upper bound alone
        takes the floor away, which is how `0` means "nothing in the period"."""
        f = SmsAudienceFilters(period_days=30, **bounds)
        assert f.predicate("revenue_ltv", "retail", period_column="p") == (sql, params)

    def test_the_new_fields_are_frozen_with_the_rest(self):
        f = SmsAudienceFilters(period_orders_max=0, period_from=date(2026, 1, 1),
                               period_to=date(2026, 3, 31))
        assert not f.is_empty()
        # Zero is a value, not "unset": dropping it would freeze the audience
        # as "at least one order in the period", its opposite.
        assert f.as_dict() == {"period_orders_max": 0, "period_from": "2026-01-01",
                               "period_to": "2026-03-31"}
        assert not SmsAudienceFilters(period_orders_max=0, period_days=30).is_empty()

    def test_it_needs_the_column_it_was_counted_in(self):
        with pytest.raises(ValueError, match="column"):
            SmsAudienceFilters(period_days=30).predicate("revenue_ltv", "retail")

    def test_it_is_a_filter_and_is_frozen_with_the_rest(self):
        f = SmsAudienceFilters(period_orders_min=2, period_from=date(2026, 1, 1))
        assert not f.is_empty()
        assert f.as_dict() == {"period_orders_min": 2, "period_from": "2026-01-01"}


class TestOneGroupRendersWhatItRenderedBefore:
    """The part of the promise that can be checked without running anything:
    with one group, the predicate and parameters are the filters' own,
    unchanged, and no group machinery reaches the predicate."""

    @pytest.mark.parametrize("filters", [
        SmsAudienceFilters(),
        SmsAudienceFilters(orders_min=2, cities=("Київ",)),
        SmsAudienceFilters(brands=("Cosrx",), bought_within_days=180),
        SmsAudienceFilters(recency_min_days=30, ltv_min=1000.0, genders=("f",)),
    ], ids=["empty", "aggregate", "content", "mixed"])
    def test_the_predicate_is_the_filters_own(self, filters):
        frag = audience_group_fragments(
            [SmsAudienceGroup(270, filters)], "revenue_ltv", "retail",
            "silver_order_lines",
        )
        own_sql, own_params = filters.predicate(
            "revenue_ltv", "retail", "silver_order_lines")
        assert frag["group_predicates"] == [own_sql]
        assert frag["group_params"] == own_params
        assert frag["window"] == 270
        assert frag["period_columns"] == [] and frag["period_params"] == []


class TestTheFragments:
    def test_only_a_narrower_group_carries_its_own_window(self):
        frag = audience_group_fragments(
            [group_a(), GROUP_B], "revenue_ltv", "retail", "silver_order_lines",
        )
        assert frag["window"] == 365
        a, b = frag["group_predicates"]
        assert not a.startswith("(recency <= ?")
        assert b == "(recency <= ? AND TRUE)"

    def test_parameters_follow_the_text(self):
        """The period counts bind in `cust`, the predicates in `flagged`, and
        within a group the window comes before its filters. A list built in
        any other order binds a date to a day count."""
        groups = [
            SmsAudienceGroup(365, SmsAudienceFilters(
                period_orders_min=2, period_from=date(2026, 1, 1), cities=("Київ",))),
            SmsAudienceGroup(92, SmsAudienceFilters(period_days=30, orders_min=3)),
        ]
        frag = audience_group_fragments(groups, "revenue_ltv", "retail",
                                        "silver_order_lines")
        assert frag["period_params"] == [date(2026, 1, 1), 30]
        assert frag["group_params"] == [
            "київ", 2,          # group 1: city, then its period count
            92, 3, 1,           # group 2: its window, orders, period count
        ]
        assert [c.split(" AS ")[-1] for c in frag["period_columns"]] == [
            "period_orders_1", "period_orders_2"]

    def test_a_range_binds_both_its_dates_in_order(self):
        """A closed range is two dates in `cust`, the start first; bounds on
        the count are the floor then the ceiling in `flagged`."""
        groups = [
            SmsAudienceGroup(3650, SmsAudienceFilters(
                period_orders_min=1, period_orders_max=2,
                period_from=date(2026, 1, 1), period_to=date(2026, 3, 31))),
            SmsAudienceGroup(365, SmsAudienceFilters(
                period_orders_max=0, period_to=date(2025, 6, 30))),
        ]
        frag = audience_group_fragments(groups, "revenue_ltv", "retail",
                                        "silver_order_lines")
        assert frag["window"] == 3650
        assert frag["period_params"] == [date(2026, 1, 1), date(2026, 3, 31),
                                         date(2025, 6, 30)]
        assert frag["group_params"] == [1, 2, 365, 0]
        assert frag["group_predicates"] == [
            "(period_orders_1 >= ? AND period_orders_1 <= ?)",
            "(recency <= ? AND (period_orders_2 <= ?))",
        ]

    def test_the_count_of_groups_is_bounded(self):
        with pytest.raises(ValueError, match="at least one"):
            audience_group_fragments([], "revenue_ltv", "retail", "x")
        with pytest.raises(ValueError, match="at most"):
            audience_group_fragments(
                [GROUP_B] * (MAX_AUDIENCE_GROUPS + 1), "revenue_ltv", "retail", "x",
            )

    def test_the_query_names_every_group_and_counts_each(self):
        frag = audience_group_fragments(
            [group_a(), GROUP_B], "revenue_ltv", "retail", DUCKDB.order_lines,
        )
        sql = sms_segments_select(
            DUCKDB, ltv_column="revenue_ltv", sales_type_filter="",
            tier_case="NULL", arm_expr="'ALL'", ok_tier_expr="TRUE",
            group_predicates=frag["group_predicates"],
            period_columns=frag["period_columns"], tier_subset="",
        )
        assert "AS in_group_1" in sql and "AS in_group_2" in sql
        assert "(in_group_1 OR in_group_2) AS ok_filters" in sql
        assert "CASE WHEN in_group_2 THEN 2 ELSE 0 END" in sql
        assert sql.count("AS f_group_") == 2
        assert "{" not in sql and "}" not in sql


# ─── On a real store ────────────────────────────────────────────────────────

@pytest_asyncio.fixture
async def store(tmp_path):
    s = DuckDBStore(db_path=tmp_path / "groups.duckdb")
    await s.connect()
    async with s.connection() as conn:
        fx.seed_duckdb_conn(conn)
    try:
        yield s
    finally:
        await s.close()


def _ids(result) -> set:
    return {c["buyerId"] for c in result["customers"]}


async def _audience(store, groups, **kwargs):
    kwargs.setdefault("grouping", "single")
    kwargs.setdefault("holdout_pct", 0)
    return await store.get_sms_segments(
        groups=groups, include_customers=True, **kwargs,
    )


class TestTheUnion:
    @pytest.mark.asyncio
    async def test_each_group_alone(self, store):
        assert _ids(await _audience(store, [group_a()])) == fx.A_ALONE
        assert _ids(await _audience(store, [GROUP_B])) == fx.B_ALONE

    @pytest.mark.asyncio
    async def test_the_owners_campaign(self, store):
        result = await _audience(store, [group_a(), GROUP_B])
        assert _ids(result) == fx.UNION
        assert result["totals"]["customers"] == len(fx.UNION)

    @pytest.mark.asyncio
    async def test_each_group_reports_its_share(self, store):
        result = await _audience(store, [group_a(), GROUP_B])
        a, b = result["groups"]
        assert a == {"group": 1, "matched": len(fx.A_MATCHED),
                     "recipients": fx.A_RECIPIENTS,
                     "onlyHere": fx.A_ONLY}
        assert b == {"group": 2, "matched": len(fx.B_MATCHED),
                     "recipients": fx.B_RECIPIENTS,
                     "onlyHere": fx.B_ONLY}

    @pytest.mark.asyncio
    async def test_the_funnel_counts_the_union(self, store):
        """`filtered` is "in any group"; the stages after it are the common
        rules on the union, as before."""
        result = await _audience(store, [group_a(), GROUP_B])
        stages = {s["stage"]: s["remaining"] for s in result["funnel"]}
        assert stages["customers"] == len(fx.BUYERS)
        assert stages["filtered"] == len(fx.A_MATCHED | fx.B_MATCHED)
        assert stages["phone"] == stages["filtered"] - 1          # buyer 11
        assert stages["subscribed"] == stages["phone"] - 1        # buyer 10
        assert stages["uniquePhone"] == len(fx.UNION)             # 8 and 9

    @pytest.mark.asyncio
    async def test_the_order_of_groups_changes_nobody(self, store):
        ab = await _audience(store, [group_a(), GROUP_B])
        ba = await _audience(store, [GROUP_B, group_a()])
        assert ab["customers"] == ba["customers"]
        assert ab["groups"][0]["recipients"] == ba["groups"][1]["recipients"]

    @pytest.mark.asyncio
    async def test_a_group_twice_is_the_group_once(self, store):
        once = await _audience(store, [GROUP_B])
        twice = await _audience(store, [GROUP_B, GROUP_B])
        assert once["customers"] == twice["customers"]
        assert twice["groups"][0]["onlyHere"] == 0

    @pytest.mark.asyncio
    async def test_a_shared_phone_is_one_recipient_across_groups(self, store):
        """8 is in A, 9 in B, one number between them: the union messages it
        once, to the more valuable of the two, which is how a single group
        has always settled it."""
        result = await _audience(store, [group_a(), GROUP_B])
        assert 9 in _ids(result) and 8 not in _ids(result)
        phones = [c["phone"] for c in result["customers"]]
        assert len(phones) == len(set(phones))

    @pytest.mark.asyncio
    async def test_an_empty_group_says_so(self, store):
        """An audience that a group leaves empty must still say which group:
        `matched` is zero for a group nobody fits."""
        nobody = SmsAudienceGroup(92, SmsAudienceFilters(orders_min=50))
        result = await _audience(store, [GROUP_B, nobody])
        assert result["groups"][1]["matched"] == 0
        assert result["groups"][1]["recipients"] == 0
        assert _ids(result) == fx.B_ALONE


class TestEachGroupKeepsItsMeaning:
    @pytest.mark.asyncio
    async def test_a_narrow_group_does_not_cut_a_wide_one(self, store):
        """B's 92 days used to be the window of the whole audience; 14's last
        order was 185 days ago, and it is in A."""
        result = await _audience(store, [group_a(), GROUP_B])
        assert 14 in _ids(result)

    @pytest.mark.asyncio
    async def test_a_wide_group_does_not_widen_a_narrow_one(self, store):
        """13 is inside A's 365 days and one day outside B's 92, and not in A:
        the union must not let it in through A's window."""
        result = await _audience(store, [group_a(), GROUP_B])
        assert 13 not in _ids(result)
        assert 4 not in _ids(result)

    @pytest.mark.asyncio
    async def test_a_groups_own_window_still_binds_it(self, store):
        narrow_a = group_a(180)
        result = await _audience(store, [narrow_a, GROUP_B])
        assert _ids(result) == fx.UNION - {14}


class TestOrdersInAPeriod:
    @pytest.mark.asyncio
    async def test_it_counts_the_period_not_the_lifetime(self, store):
        """4 has two orders, both before the period: a lifetime count of two
        selects them, the period count does not."""
        lifetime = await store.get_sms_segments(
            filters=SmsAudienceFilters(orders_min=2), max_recency_days=730,
            grouping="single", include_customers=True,
        )
        assert 4 in _ids(lifetime)
        assert 4 not in _ids(await _audience(store, [group_a()]))

    @pytest.mark.asyncio
    async def test_it_counts_what_the_audience_counts(self, store):
        """A return (5), a retired source (6) and a b2b order (7) are not
        purchases for the period any more than for the rest of the query."""
        ids = _ids(await _audience(store, [group_a()]))
        assert not ids & {5, 6, 7}

    @pytest.mark.asyncio
    async def test_the_sales_type_decides_what_counts(self, store):
        ids = _ids(await _audience(store, [group_a()], sales_type="all"))
        assert 7 in ids

    @pytest.mark.asyncio
    async def test_within_n_days_is_the_window_of_that_many_days(self, store):
        """B said as "an order in the last 92 days" is B said as a 92-day
        window — the edge included (12 at 92 days in, 13 at 93 out)."""
        as_period = SmsAudienceGroup(270, SmsAudienceFilters(period_days=92))
        a = await _audience(store, [as_period])
        b = await _audience(store, [GROUP_B])
        assert a["customers"] == b["customers"]
        assert 12 in _ids(a) and 13 not in _ids(a)


class TestThePeriodsShapesAndBounds:
    """From-and-to, up-to, exactly N, none in the period and the all-time
    window, each against `tests/sms_audience_fixture.py`'s oracle and its
    hand-written answer. The same cases run on PostgreSQL in
    `tests/integration/test_sms_audience_groups_two_engines.py`."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("label,window,fields,terms,answer", fx.PERIOD_CASES,
                             ids=[c[0] for c in fx.PERIOD_CASES])
    async def test_the_audience_is_the_oracles(self, store, label, window, fields,
                                               terms, answer):
        mine = fx.matched(window, **terms)
        assert fx.recipients(mine) == answer, f"{label}: the oracle disagrees with the hand"
        result = await _audience(
            store, [SmsAudienceGroup(window, SmsAudienceFilters(**fields()))])
        assert _ids(result) == answer
        assert result["groups"][0]["matched"] == len(mine)

    @pytest.mark.asyncio
    async def test_none_in_the_period_is_a_recency_floor_said_by_date(self, store):
        """"Nothing in the last 92 days" and "last order at least 93 days ago"
        are one audience: the two spellings of a lapsed customer agree."""
        by_period = await _audience(store, [SmsAudienceGroup(3650, SmsAudienceFilters(
            period_orders_max=0, period_days=92))])
        by_recency = await _audience(store, [SmsAudienceGroup(3650, SmsAudienceFilters(
            recency_min_days=93))])
        assert by_period["customers"] == by_recency["customers"]

    @pytest.mark.asyncio
    async def test_an_all_time_window_is_wider_than_two_years(self, store):
        two_years = _ids(await _audience(store, [SmsAudienceGroup(730)]))
        all_time = _ids(await _audience(store, [SmsAudienceGroup(3650)]))
        assert all_time - two_years == {15}

    @pytest.mark.asyncio
    async def test_the_new_bounds_are_frozen(self, store):
        result = await _audience(store, [SmsAudienceGroup(3650, SmsAudienceFilters(
            period_orders_max=0, period_from=fx.period_from(), period_to=fx.ago(10)))])
        assert result["criteria"]["maxRecencyDays"] == 3650
        assert result["criteria"]["filters"] == {
            "period_orders_max": 0, "period_from": fx.period_from().isoformat(),
            "period_to": fx.ago(10).isoformat()}


class TestWhatIsFrozen:
    @pytest.mark.asyncio
    async def test_the_criteria_record_every_group(self, store):
        result = await _audience(store, [group_a(), GROUP_B])
        criteria = result["criteria"]
        assert criteria["maxRecencyDays"] == 365
        assert criteria["groups"] == [
            {"maxRecencyDays": 365, "filters": {
                "period_orders_min": 2, "period_from": fx.period_from().isoformat()}},
            {"maxRecencyDays": 92, "filters": {}},
        ]
        assert criteria["filters"] == criteria["groups"][0]["filters"]

    @pytest.mark.asyncio
    async def test_one_group_is_recorded_as_it_always_was_plus_its_group(self, store):
        result = await store.get_sms_segments(
            filters=SmsAudienceFilters(orders_min=2), max_recency_days=180,
            grouping="single",
        )
        criteria = result["criteria"]
        assert criteria["maxRecencyDays"] == 180
        assert criteria["filters"] == {"orders_min": 2}
        assert criteria["groups"] == [
            {"maxRecencyDays": 180, "filters": {"orders_min": 2}}]


class TestTheCallerSaysItOnce:
    @pytest.mark.asyncio
    async def test_groups_beside_filters_is_refused(self, store):
        with pytest.raises(ValueError, match="not both"):
            await store.get_sms_segments(
                groups=[GROUP_B], filters=SmsAudienceFilters(),
            )
        with pytest.raises(ValueError, match="not both"):
            await store.get_sms_segments(groups=[GROUP_B], max_recency_days=92)


class TestTheControlIsDrawnOverTheUnion:
    @pytest.mark.asyncio
    async def test_a_persons_arm_does_not_depend_on_their_groups(self, store):
        """The holdout is a hash of (buyer, campaign) — reaching somebody
        through another group must not re-draw them."""
        alone = await _audience(store, [GROUP_B], holdout_pct=50, campaign="x1")
        union = await _audience(store, [group_a(), GROUP_B], holdout_pct=50,
                                campaign="x1")
        arm = {c["buyerId"]: c["assignment"] for c in union["customers"]}
        for c in alone["customers"]:
            assert arm[c["buyerId"]] == c["assignment"]
