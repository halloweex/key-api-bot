"""Audience groups on Postgres, against the query before groups and against
what DuckDB answered.

Two things are proved here by running SQL, which is what a rendered-text test
cannot do:

1. **One group selects what the audience selected before groups existed.**
   The query as it stood on main before this change is frozen in
   `tests/sms_segments_legacy.py`. Each case below runs it and the current
   query side by side over the same rows: the rows must be identical, funnel
   included. The current query is rendered by `sms_segments_fragments` — the
   function `get_sms_segments` executes — so the comparison is with what the
   store runs, not with a copy of it. That is the promise that keeps every
   past campaign, built-in preset and saved preset reproducible. It ran on
   DuckDB too until step 14's S14-08.

2. **Groups mean what they meant.** The owner's campaign and its edges — a
   shared phone across groups, a group narrower than another, the period
   counting only what the audience counts — answer what
   `tests/sms_audience_fixture.py` says by hand, through the real
   `get_sms_segments`. So do the period's shapes and bounds — a closed range,
   up to a date, exactly N, none in the period — and the all-time window,
   against the fixture's oracle. The whole result of each was compared with
   DuckDB's while there were two engines; it is compared now with DuckDB's
   answer recorded on this fixture before that side went
   (`tests/integration/golden/sms_audience_groups.json`, rule 9 of the step-14
   plan), its dates written back from today as the fixture's are.

Skipped without `KS_PG_DSN`, which CI always sets and fails on skipping.
"""
from __future__ import annotations

import os
from datetime import date

import pytest
import pytest_asyncio

from core.repositories.customers import (
    SMS_TIER_DEFAULTS, SmsAudienceFilters, SmsAudienceGroup, sms_segments_fragments,
)
from core.sql_dialect import POSTGRES, numbered, sms_segments_select
from tests import sms_audience_fixture as fx
from tests.integration import pg_sms_seed
from tests.integration.pg_sms_seed import golden, kyiv_today, relative, shifted
from tests.sms_segments_legacy import legacy_render

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(
    not DSN, reason="needs a live PostgreSQL at KS_PG_DSN",
)

GOLDEN = golden("sms_audience_groups")
# The Kyiv day the literals were recorded on: the one date below written out
# rather than counted back from today moves with the calendar from it.
RECORDED_ON = date(2026, 10, 28)
# What each whole-result comparison held to DuckDB's.
_KEYS = ("funnel", "groups", "segments", "totals", "customers", "criteria")


def group_a(window: int = 365) -> SmsAudienceGroup:
    """Two or more orders since `fx.period_from()`. A function where B is a
    constant because it carries a date, and a date built at import is the
    collection's day, not the query's (the fixture's docstring says why)."""
    return SmsAudienceGroup(
        window, SmsAudienceFilters(period_orders_min=2, period_from=fx.period_from()),
    )


GROUP_B = SmsAudienceGroup(92, SmsAudienceFilters())

# One group, every way it could be asked before groups existed. Each filter
# that existed then appears in at least one case, as does each grouping, both
# bases, a level subset and both sales types. A case's filters are a function,
# called when the test runs: pytest builds this table at collection, and
# "first order" carries dates.
LEGACY_CASES = [
    ("defaults", {}),
    ("narrow window", dict(window=92)),
    ("widest window", dict(window=730)),
    # Wider than the API allowed before groups, so no past campaign used it;
    # the query must still be the old one's at the new cap.
    ("all-time window", dict(window=3650)),
    ("shortest window", dict(window=30)),
    ("rfm on revenue", dict(grouping="rfm")),
    ("rfm on margin", dict(grouping="rfm", ltv_basis="margin")),
    ("one level", dict(grouping="rfm", tiers=("REACTIVATION",))),
    ("sales_type all", dict(sales_type="all", window=730)),
    ("lifetime orders", dict(
        filters=lambda: SmsAudienceFilters(orders_min=2, orders_max=5),
        window=730)),
    ("recency range", dict(
        filters=lambda: SmsAudienceFilters(recency_min_days=20,
                                           recency_max_days=160))),
    ("value and basket", dict(
        filters=lambda: SmsAudienceFilters(ltv_min=1500.0, aov_max=1200.0),
        window=730)),
    ("first order", dict(
        filters=lambda: SmsAudienceFilters(first_order_from=fx.ago(200),
                                           first_order_to=fx.ago(100)))),
    ("city", dict(filters=lambda: SmsAudienceFilters(cities=("київ",)))),
    ("brand ever", dict(
        filters=lambda: SmsAudienceFilters(brands=("Anua",)), window=730)),
    ("brand recently", dict(
        filters=lambda: SmsAudienceFilters(brands=("Cosrx",),
                                           bought_within_days=60))),
    ("category branch", dict(
        filters=lambda: SmsAudienceFilters(category_ids=(10,)))),
    ("source", dict(
        filters=lambda: SmsAudienceFilters(source_ids=(4,)), window=730)),
    ("promocode", dict(filters=lambda: SmsAudienceFilters(promocode="aug"))),
]


@pytest_asyncio.fixture
async def engine(tmp_path, monkeypatch):
    """The repository on Postgres, the fixture seeded, its DuckDB connection
    a trap; and the pool, for the statements the tests run themselves."""
    async with pg_sms_seed.on_postgres(monkeypatch) as pool:
        async with pool.acquire() as conn:
            await fx.seed_postgres(conn)
        yield pg_sms_seed.store(tmp_path, "groups.duckdb"), pool


async def _run(pool, sql, params):
    async with pool.acquire() as conn:
        return [tuple(r) for r in await conn.fetch(numbered(sql), *params)]


def _plain(rows):
    """Money as float at two decimals: asyncpg hands back Decimal."""
    return [tuple(round(float(v), 2) if hasattr(v, "as_tuple") else v for v in r)
            for r in rows]


def _filters(case) -> SmsAudienceFilters:
    build = case.get("filters")
    return build() if build else SmsAudienceFilters()


def _as_recorded(result) -> dict:
    """The keys the engines were compared on, dates written back from today."""
    return relative({k: result[k] for k in _KEYS}, kyiv_today())


class TestOneGroupIsTheAudienceBeforeGroups:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("label,case", LEGACY_CASES,
                             ids=[c[0] for c in LEGACY_CASES])
    async def test_the_rows_are_the_old_querys_rows(self, engine, label, case):
        _store, pool = engine
        dialect = POSTGRES
        basis = case.get("ltv_basis", "revenue")
        window = case.get("window", 270)
        filters = _filters(case)
        common = dict(
            ltv_basis=basis,
            sales_type=case.get("sales_type", "retail"),
            grouping=case.get("grouping", "single"),
            vip_ltv=SMS_TIER_DEFAULTS[basis]["vip"],
            core_ltv=SMS_TIER_DEFAULTS[basis]["core"],
            core_min_orders=2,
            reactivation_max_recency=min(120, window),
        )
        tiers = case.get("tiers", ())

        old_sql, old_params = legacy_render(
            dialect, filters, tiers=tiers, max_recency_days=window, **common,
        )
        fragments, new_params, _ = sms_segments_fragments(
            [SmsAudienceGroup(window, filters)], order_lines=dialect.order_lines,
            tiers=list(tiers) or None, **common,
        )
        new_sql = sms_segments_select(dialect, **fragments)

        old_rows = await _run(pool, old_sql, old_params)
        new_rows = await _run(pool, new_sql, new_params)

        assert old_rows, "the comparison ran over nothing"
        # The old projection is the first 26 columns; after it come the group
        # mask and one count per group, which the old query did not have.
        assert _plain([r[:26] for r in new_rows]) == _plain(old_rows), (
            f"{dialect.name}: one group ({label}) selects differently from the "
            f"audience before groups"
        )

    @pytest.mark.asyncio
    async def test_the_cases_are_not_all_the_same_audience(self, engine):
        """A pin that compared one roster eighteen times would prove one
        thing. The cases must actually select different people."""
        _store, pool = engine
        rosters = set()
        for _label, case in LEGACY_CASES:
            basis = case.get("ltv_basis", "revenue")
            window = case.get("window", 270)
            sql, params = legacy_render(
                POSTGRES, _filters(case),
                ltv_basis=basis, sales_type=case.get("sales_type", "retail"),
                tiers=case.get("tiers", ()), grouping=case.get("grouping", "single"),
                max_recency_days=window,
                vip_ltv=SMS_TIER_DEFAULTS[basis]["vip"],
                core_ltv=SMS_TIER_DEFAULTS[basis]["core"],
                reactivation_max_recency=min(120, window),
            )
            rows = await _run(pool, sql, params)
            rosters.add(frozenset(r[0] for r in rows if r[0] is not None))
        assert len(rosters) >= 10


class TestGroupsOnPostgres:
    @pytest.mark.asyncio
    async def test_the_owners_campaign(self, engine):
        store, _pool = engine
        result = await store.get_sms_segments(
            groups=[group_a(), GROUP_B],
            grouping="single", holdout_pct=0, include_customers=True,
        )
        assert {c["buyerId"] for c in result["customers"]} == fx.UNION
        a, b = result["groups"]
        assert (a["matched"], a["recipients"], a["onlyHere"]) == (
            len(fx.A_MATCHED), fx.A_RECIPIENTS, fx.A_ONLY)
        assert (b["matched"], b["recipients"], b["onlyHere"]) == (
            len(fx.B_MATCHED), fx.B_RECIPIENTS, fx.B_ONLY)

    @pytest.mark.asyncio
    async def test_each_group_alone(self, engine):
        store, _pool = engine
        for group, expected in ((group_a(), fx.A_ALONE), (GROUP_B, fx.B_ALONE)):
            result = await store.get_sms_segments(
                groups=[group], grouping="single", include_customers=True,
            )
            assert {c["buyerId"] for c in result["customers"]} == expected

    @pytest.mark.asyncio
    async def test_within_n_days_is_the_window_on_postgres_too(self, engine):
        store, _pool = engine
        as_period = SmsAudienceGroup(270, SmsAudienceFilters(period_days=92))
        a = await store.get_sms_segments(groups=[as_period],
                                         grouping="single", include_customers=True)
        b = await store.get_sms_segments(groups=[GROUP_B],
                                         grouping="single", include_customers=True)
        assert a["customers"] == b["customers"]


class TestThePeriodsShapes:
    """From-and-to, up-to, exactly N, none in the period and the all-time
    window: the store selects the oracle's answer
    (`tests/sms_audience_fixture.py`, which also writes it by hand), and the
    whole result — funnel and frozen criteria included — is the one DuckDB
    returned."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("label,window,fields,terms,answer", fx.PERIOD_CASES,
                             ids=[c[0] for c in fx.PERIOD_CASES])
    async def test_the_store_selects_the_oracles_answer(
        self, engine, label, window, fields, terms, answer,
    ):
        store, _pool = engine
        mine = fx.matched(window, **terms)
        assert fx.recipients(mine) == answer, f"{label}: the oracle disagrees with the hand"
        result = await store.get_sms_segments(
            groups=[SmsAudienceGroup(window, SmsAudienceFilters(**fields()))],
            grouping="single", holdout_pct=20, campaign="period-shapes",
            include_customers=True)
        assert {c["buyerId"] for c in result["customers"]} == answer, label
        assert result["groups"][0]["matched"] == len(mine), label
        assert _as_recorded(result) == GOLDEN[f"periods:{label}"], (
            f"{label}: Postgres's result is not the one DuckDB returned")


class TestTheEnginesAgree:
    """Each answer is the one DuckDB gave on the same fixture."""

    # Each case's arguments are a function, called when the test runs: they
    # carry dates, and pytest builds this table at collection.
    CASES = [
        ("the owner's campaign", lambda: dict(groups=[group_a(), GROUP_B])),
        ("a narrower A", lambda: dict(groups=[group_a(180), GROUP_B])),
        ("all sales types", lambda: dict(groups=[group_a(), GROUP_B],
                                         sales_type="all")),
        ("three groups, mixed", lambda: dict(groups=[
            group_a(),
            SmsAudienceGroup(120, SmsAudienceFilters(period_days=60,
                                                     cities=("Львів",))),
            SmsAudienceGroup(400, SmsAudienceFilters(brands=("Anua",),
                                                     period_orders_min=1,
                                                     # Written out, so moved with
                                                     # the calendar: its echo in
                                                     # the criteria is a literal.
                                                     period_from=shifted(
                                                         date(2000, 1, 1),
                                                         RECORDED_ON))),
        ])),
        ("split by level", lambda: dict(groups=[group_a(), GROUP_B],
                                        grouping="rfm", ltv_basis="margin")),
        ("one level of the union", lambda: dict(groups=[group_a(), GROUP_B],
                                                grouping="rfm",
                                                tier=["REACTIVATION"])),
        # The range, its bounds and the all-time window beside other groups:
        # "two in the range" OR "lapsed since the date, all time" OR B.
        ("ranges and bounds, ORed", lambda: dict(groups=[
            SmsAudienceGroup(365, SmsAudienceFilters(
                period_orders_min=2, period_orders_max=2,
                period_from=fx.range_from(), period_to=fx.range_to())),
            SmsAudienceGroup(3650, SmsAudienceFilters(
                period_orders_max=0, period_from=fx.period_from())),
            GROUP_B,
        ])),
        ("up to a date, margin levels", lambda: dict(groups=[
            SmsAudienceGroup(3650, SmsAudienceFilters(period_to=fx.period_from())),
            SmsAudienceGroup(92, SmsAudienceFilters(period_orders_max=0,
                                                    period_days=30)),
        ], grouping="rfm", ltv_basis="margin")),
    ]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("label,kwargs", CASES, ids=[c[0] for c in CASES])
    async def test_the_whole_result_is_the_one_duckdb_returned(self, engine,
                                                               label, kwargs):
        store, _pool = engine
        common = dict(include_customers=True, campaign="groups-compare",
                      holdout_pct=20, grouping="single")
        common.update(kwargs())
        postgres = await store.get_sms_segments(**common)
        assert postgres["totals"]["customers"] > 0, f"{label}: compared nobody"
        assert _as_recorded(postgres) == GOLDEN[f"agree:{label}"], (
            f"{label}: Postgres's result is not the one DuckDB returned")
