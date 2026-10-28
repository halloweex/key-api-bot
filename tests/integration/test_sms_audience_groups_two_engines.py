"""Audience groups on both engines, and against the query before groups.

Two things are proved here by running SQL, which is what a rendered-text test
cannot do:

1. **One group selects what the audience selected before groups existed.**
   The query as it stood on main before this change is frozen in
   `tests/sms_segments_legacy.py`. Each case below runs it and the current
   query side by side, on DuckDB and on PostgreSQL, over the same rows: the
   rows must be identical, funnel included. The current query is rendered by
   `sms_segments_fragments` — the function `get_sms_segments` executes — so
   the comparison is with what the store runs, not with a copy of it. That is
   the promise that keeps every past campaign, built-in preset and saved
   preset reproducible.

2. **Groups mean the same thing in both engines.** The owner's campaign and
   its edges — a shared phone across groups, a group narrower than another,
   the period counting only what the audience counts — answer what
   `tests/sms_audience_fixture.py` says by hand, through the real
   `get_sms_segments`, on PostgreSQL as on DuckDB. So do the period's shapes
   and bounds — a closed range, up to a date, exactly N, none in the period —
   and the all-time window, against the fixture's oracle.

Skipped without `KS_PG_DSN`, which CI always sets and fails on skipping.
"""
from __future__ import annotations

import os
from datetime import date
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from core.duckdb_store import DuckDBStore
from core.repositories.customers import (
    SMS_TIER_DEFAULTS, SmsAudienceFilters, SmsAudienceGroup, sms_segments_fragments,
)
from core.sql_dialect import DUCKDB, POSTGRES, numbered, sms_segments_select
from tests import sms_audience_fixture as fx
from tests.sms_segments_legacy import legacy_render


@pytest.fixture(autouse=True)
def _sms_store_follows_env(monkeypatch):
    """Since step 14's S14-00 the store switch decides nothing; this module's
    two engines are chosen as before it (`tests.conftest.sms_store_follows_env`)."""
    from tests.conftest import sms_store_follows_env

    sms_store_follows_env(monkeypatch)


asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(
    not DSN, reason="needs a live PostgreSQL at KS_PG_DSN",
)


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


class _PoolOf:
    def __init__(self, conn):
        self._conn = conn

    def acquire(self):
        conn = self._conn

        class _Ctx:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, *exc):
                return False

        return _Ctx()


@pytest_asyncio.fixture
async def engines(tmp_path):
    store = DuckDBStore(db_path=tmp_path / "groups.duckdb")
    await store.connect()
    conn = await asyncpg.connect(DSN)
    try:
        async with store.connection() as duck:
            fx.seed_duckdb_conn(duck)
        await fx.seed_postgres(conn)
        yield store, conn
    finally:
        await conn.close()
        await store.close()


async def _run(store, conn, dialect, sql, params):
    if dialect is DUCKDB:
        async with store.connection() as duck:
            return duck.execute(sql, params).fetchall()
    return [tuple(r) for r in await conn.fetch(numbered(sql), *params)]


def _plain(rows):
    """Money as float at two decimals: DuckDB DECIMAL, asyncpg Decimal."""
    return [tuple(round(float(v), 2) if hasattr(v, "as_tuple") else v for v in r)
            for r in rows]


async def _on_postgres(store, conn, monkeypatch, **kwargs):
    monkeypatch.setenv("KS_SMS_STORE", "postgres")
    try:
        with patch("core.pg.get_pool", new=AsyncMock(return_value=_PoolOf(conn))), \
             patch("core.pg.require_revision", new=AsyncMock()):
            return await store.get_sms_segments(**kwargs)
    finally:
        monkeypatch.delenv("KS_SMS_STORE", raising=False)


def _filters(case) -> SmsAudienceFilters:
    build = case.get("filters")
    return build() if build else SmsAudienceFilters()


async def _on_duckdb(store, monkeypatch, **kwargs):
    monkeypatch.delenv("KS_SMS_STORE", raising=False)
    return await store.get_sms_segments(**kwargs)


class TestOneGroupIsTheAudienceBeforeGroups:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("label,case", LEGACY_CASES,
                             ids=[c[0] for c in LEGACY_CASES])
    @pytest.mark.parametrize("dialect", (DUCKDB, POSTGRES), ids=("duckdb", "postgres"))
    async def test_the_rows_are_the_old_querys_rows(
        self, engines, dialect, label, case,
    ):
        store, conn = engines
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

        old_rows = await _run(store, conn, dialect, old_sql, old_params)
        new_rows = await _run(store, conn, dialect, new_sql, new_params)

        assert old_rows, "the comparison ran over nothing"
        # The old projection is the first 26 columns; after it come the group
        # mask and one count per group, which the old query did not have.
        assert _plain([r[:26] for r in new_rows]) == _plain(old_rows), (
            f"{dialect.name}: one group ({label}) selects differently from the "
            f"audience before groups"
        )

    @pytest.mark.asyncio
    async def test_the_cases_are_not_all_the_same_audience(self, engines):
        """A pin that compared one roster eighteen times would prove one
        thing. The cases must actually select different people."""
        store, conn = engines
        rosters = set()
        for _label, case in LEGACY_CASES:
            basis = case.get("ltv_basis", "revenue")
            window = case.get("window", 270)
            sql, params = legacy_render(
                DUCKDB, _filters(case),
                ltv_basis=basis, sales_type=case.get("sales_type", "retail"),
                tiers=case.get("tiers", ()), grouping=case.get("grouping", "single"),
                max_recency_days=window,
                vip_ltv=SMS_TIER_DEFAULTS[basis]["vip"],
                core_ltv=SMS_TIER_DEFAULTS[basis]["core"],
                reactivation_max_recency=min(120, window),
            )
            rows = await _run(store, conn, DUCKDB, sql, params)
            rosters.add(frozenset(r[0] for r in rows if r[0] is not None))
        assert len(rosters) >= 10


class TestGroupsOnPostgres:
    @pytest.mark.asyncio
    async def test_the_owners_campaign(self, engines, monkeypatch):
        store, conn = engines
        result = await _on_postgres(
            store, conn, monkeypatch, groups=[group_a(), GROUP_B],
            grouping="single", holdout_pct=0, include_customers=True,
        )
        assert {c["buyerId"] for c in result["customers"]} == fx.UNION
        a, b = result["groups"]
        assert (a["matched"], a["recipients"], a["onlyHere"]) == (
            len(fx.A_MATCHED), fx.A_RECIPIENTS, fx.A_ONLY)
        assert (b["matched"], b["recipients"], b["onlyHere"]) == (
            len(fx.B_MATCHED), fx.B_RECIPIENTS, fx.B_ONLY)

    @pytest.mark.asyncio
    async def test_each_group_alone(self, engines, monkeypatch):
        store, conn = engines
        for group, expected in ((group_a(), fx.A_ALONE), (GROUP_B, fx.B_ALONE)):
            result = await _on_postgres(
                store, conn, monkeypatch, groups=[group], grouping="single",
                include_customers=True,
            )
            assert {c["buyerId"] for c in result["customers"]} == expected

    @pytest.mark.asyncio
    async def test_within_n_days_is_the_window_on_postgres_too(
        self, engines, monkeypatch,
    ):
        store, conn = engines
        as_period = SmsAudienceGroup(270, SmsAudienceFilters(period_days=92))
        a = await _on_postgres(store, conn, monkeypatch, groups=[as_period],
                               grouping="single", include_customers=True)
        b = await _on_postgres(store, conn, monkeypatch, groups=[GROUP_B],
                               grouping="single", include_customers=True)
        assert a["customers"] == b["customers"]


class TestThePeriodsShapesOnBothEngines:
    """From-and-to, up-to, exactly N, none in the period and the all-time
    window: each engine selects the oracle's answer
    (`tests/sms_audience_fixture.py`, which also writes it by hand), and the
    two engines return one result, funnel and frozen criteria included."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("label,window,fields,terms,answer", fx.PERIOD_CASES,
                             ids=[c[0] for c in fx.PERIOD_CASES])
    async def test_both_engines_select_the_oracles_answer(
        self, engines, monkeypatch, label, window, fields, terms, answer,
    ):
        store, conn = engines
        mine = fx.matched(window, **terms)
        assert fx.recipients(mine) == answer, f"{label}: the oracle disagrees with the hand"
        kwargs = dict(groups=[SmsAudienceGroup(window, SmsAudienceFilters(**fields()))],
                      grouping="single", holdout_pct=20, campaign="period-shapes",
                      include_customers=True)
        duck = await _on_duckdb(store, monkeypatch, **kwargs)
        postgres = await _on_postgres(store, conn, monkeypatch, **kwargs)
        for name, result in (("duckdb", duck), ("postgres", postgres)):
            assert {c["buyerId"] for c in result["customers"]} == answer, (
                f"{name}: {label}")
            assert result["groups"][0]["matched"] == len(mine), f"{name}: {label}"
        for key in ("funnel", "groups", "segments", "totals", "customers",
                    "criteria"):
            assert duck[key] == postgres[key], f"{label}: {key} differs"


class TestTheEnginesAgree:
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
                                                     period_from=date(2000, 1, 1))),
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
    async def test_the_whole_result_is_the_same(self, engines, monkeypatch,
                                                label, kwargs):
        store, conn = engines
        common = dict(include_customers=True, campaign="groups-compare",
                      holdout_pct=20, grouping="single")
        common.update(kwargs())
        duck = await _on_duckdb(store, monkeypatch, **common)
        postgres = await _on_postgres(store, conn, monkeypatch, **common)
        assert duck["totals"]["customers"] > 0, f"{label}: compared nobody"
        for key in ("funnel", "groups", "segments", "totals", "customers",
                    "criteria"):
            assert duck[key] == postgres[key], f"{label}: {key} differs"
