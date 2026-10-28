"""The audience query, and the short `/sms` statements, answer in Postgres what
they answered in DuckDB.

`tests/unit/test_sms_segments_dialect.py` proves the two renderings are *one
text*. That is necessary and not sufficient: identical SQL can still mean
different things where the engines' semantics differ — integer division, NULL
ordering, how a window function breaks a tie, what `ROUND` does at .005.

So this ran the real thing on both: the same synthetic customers in a DuckDB
store and a live PostgreSQL, both renderings executed, the rosters compared
row by row.

ONE ENGINE SINCE STEP 14 (S14-08)

`/sms` is Postgres's alone. Every comparison is now with a literal
(`tests/integration/golden/sms_segments.json`): DuckDB's answer on this
fixture, recorded while this file still asserted Postgres answered the same
(rule 9 of the step-14 plan). The fixture's dates are written as they stood on
the day of the recording, `RECORDED_ON`, and move with the calendar — the
query counts recency from Kyiv's today — so the literal holds on any day; its
own dates are written relative to today (`pg_sms_seed.relative`).

The fixture is small and deliberately awkward, because a roster that agrees on
easy data proves nothing. It carries a product name long enough to be
truncated, two buyers sharing one phone number (only the higher-value one may
survive), an opted-out customer, a return, an order on a retired source, and a
b2b order that must be invisible unless `sales_type=all` asks for it.

Skipped without `KS_PG_DSN`, which CI always sets and fails on skipping.
"""
from __future__ import annotations

import asyncio
from datetime import date, datetime, time, timezone

import pytest
import pytest_asyncio

from core.repositories.customers import DlrEventRebound, SmsAudienceFilters
from core.sql_dialect import POSTGRES, numbered, sms_segments_select
from tests.integration import pg_sms_seed
from tests.integration.pg_sms_seed import (
    golden, kyiv_today, needs_postgres, relative, same_numbers, shifted,
)

pytestmark = needs_postgres

GOLDEN = golden("sms_segments")
# The Kyiv day the literals were recorded on. Every date below is written as
# it stood then and seeded `today − RECORDED_ON` days later (`_day`).
RECORDED_ON = date(2026, 10, 28)

LONG = "Очищувальна пінка з екстрактом зеленого чаю та центелою азійською 150мл"

# (id, buyer, date, grand_total, sales_type, is_return, is_active, source, promo)
ORDERS = [
    (1,  1, "2026-08-01", 6000, "retail", False, True, 1, None),
    (2,  1, "2026-07-01", 5000, "retail", False, True, 1, "AUG"),
    (3,  2, "2026-08-10", 12000, "retail", False, True, 2, None),
    (4,  3, "2026-06-01", 3000, "retail", False, True, 1, None),
    (5,  4, "2026-08-15", 900, "retail", False, True, 1, None),   # opted out
    (6,  5, "2026-08-05", 7000, "b2b", False, True, 1, None),     # wrong type
    (7,  6, "2026-08-05", 4000, "retail", True, True, 1, None),   # a return
    (8,  7, "2026-08-05", 4000, "retail", False, False, 3, None), # retired source
    (9,  8, "2026-08-12", 2500, "retail", False, True, 1, None),  # shares a phone
    (10, 9, "2026-08-12", 2600, "retail", False, True, 1, None),  # …and wins it
]
LINES = [
    (1, 1, 100, LONG, 3, 1000), (2, 1, 101, "Тонер", 2, 900),
    (3, 1, 102, "Сироватка", 1, 800), (4, 1, 103, "Патчі", 1, 700),
    (5, 2, 100, LONG, 2, 1200), (6, 3, 101, "Тонер", 5, 1500),
    (7, 4, 102, "Сироватка", 1, 1500),
    (8, 4, 104, "Крем без ціни", 1, 1500),   # no cost row: margin must skip it
    (9, 5, 103, "Патчі", 1, 900), (10, 6, 100, LONG, 4, 1000),
    (11, 7, 101, "Тонер", 2, 1000), (12, 8, 101, "Тонер", 2, 1000),
    (13, 9, 102, "Сироватка", 1, 2500), (14, 10, 102, "Сироватка", 1, 2600),
]
PRODUCTS = [(100, "Cosrx", "SKU-100", 10), (101, "Beauty", "SKU-101", 10),
            (102, "Cosrx", "SKU-102", 11), (103, "Beauty", "SKU-103", 11),
            (104, "Cosrx", "SKU-104", 11)]
CATEGORIES = [(10, "Очищення", None), (11, "Догляд", 10)]
STOCKS = [(1, "SKU-100", 400), (2, "SKU-101", 500), (3, "SKU-102", 900)]
BUYERS = [
    (1, "Анна", "Київ", "+38 (050) 111-11-11"),   # punctuation must normalise
    (2, "Богдан", "Львів", "380502222222"),
    (3, "Віра", "київ", "380503333333"),          # lower case city
    (4, "Галина", "Одеса", "380504444444"),
    (5, "Дмитро", "Київ", "380505555555"),
    (6, "Олена", "Київ", "380506666666"),
    (7, "Женя", "Київ", "380507777777"),
    (8, "Ігор", "Київ", "380508888888"),
    (9, "Ірина", "Київ", "380508888888"),
]
OPTOUTS = [(4, "sms", "380504444444")]

CASES = [
    ("rfm, revenue basis", SmsAudienceFilters(), "revenue_ltv", "retail", (), "rfm"),
    ("single arm, margin basis", SmsAudienceFilters(), "margin_ltv", "retail", (),
     "single"),
    ("brand filter (the content EXISTS)", SmsAudienceFilters(brands=("Cosrx",)),
     "revenue_ltv", "retail", (), "single"),
    ("category matches the whole branch", SmsAudienceFilters(category_ids=(10,)),
     "revenue_ltv", "retail", (), "single"),
    ("city and order count", SmsAudienceFilters(cities=("Київ",), orders_min=2),
     "revenue_ltv", "retail", (), "single"),
    ("one level only", SmsAudienceFilters(), "revenue_ltv", "retail", ("VIP",), "rfm"),
    ("promocode", SmsAudienceFilters(promocode="aug"), "revenue_ltv", "retail", (),
     "single"),
    ("sales_type=all lets b2b in", SmsAudienceFilters(), "revenue_ltv", "all", (),
     "single"),
]


def _day(text: str) -> date:
    """A fixture date, as far back from today as it stood from RECORDED_ON."""
    return shifted(date.fromisoformat(text), RECORDED_ON)


def _instant(text: str, hour: int) -> datetime:
    return datetime.combine(_day(text), time(hour), timezone.utc)


def _render(filters, ltv_column, sales_type, tiers, grouping):
    tier_case = f"""CASE
        WHEN c.{ltv_column} >= ? THEN 'VIP'
        WHEN c.orders >= ? OR c.{ltv_column} >= ? THEN 'CORE'
        WHEN c.recency <= ? THEN 'REACTIVATION' END"""
    filter_sql, filter_params = filters.predicate(
        ltv_column, sales_type, POSTGRES.order_lines,
    )
    sql = sms_segments_select(
        POSTGRES,
        ltv_column=ltv_column,
        sales_type_filter="" if sales_type == "all" else "AND l.sales_type = ?",
        tier_case=tier_case,
        arm_expr="'ALL'" if grouping == "single" else "tier_level",
        ok_tier_expr="TRUE" if grouping == "single" else "tier_level IS NOT NULL",
        group_predicates=[filter_sql],
        tier_subset=(f"WHERE tier_level IN ({', '.join('?' * len(tiers))})"
                     if tiers else ""),
    )
    params: list = []
    if sales_type != "all":
        params.append(sales_type)
    params += [5000.0, 2, 2000.0, 120, 270]
    params += filter_params
    params += list(tiers)
    return numbered(sql), params


def _comparable(rows):
    """Money as float at two decimals — the form the rows were compared in
    while there were two engines, and recorded in."""
    out = []
    for row in rows:
        out.append(tuple(
            round(float(v), 2) if hasattr(v, "as_tuple") else v for v in row
        ))
    return out


def _now(value):
    """`value` with its dates written against today, as the literals are."""
    return relative(value, kyiv_today())


async def _seed(conn):
    await conn.executemany(
        "INSERT INTO bronze.categories (id, name, parent_id) VALUES ($1,$2,$3)",
        CATEGORIES)
    await conn.executemany(
        "INSERT INTO bronze.products (id, name, brand, sku, category_id)"
        " VALUES ($1,$2,$3,$4,$5)",
        [(p, f"p{p}", b, s, c) for p, b, s, c in PRODUCTS])
    await conn.executemany(
        "INSERT INTO bronze.offer_stocks (id, sku, purchased_price)"
        " VALUES ($1,$2,$3)", STOCKS)
    await conn.executemany(
        "INSERT INTO bronze.buyers (id, full_name, city, phone)"
        " VALUES ($1,$2,$3,$4)", BUYERS)
    await conn.executemany(
        "INSERT INTO app.marketing_optouts (buyer_id, channel, phone)"
        " VALUES ($1,$2,$3)", OPTOUTS)
    await conn.executemany(
        "INSERT INTO silver.orders (id, source_id, status_id, grand_total,"
        " ordered_at, buyer_id, manager_id, order_date, is_return, sales_type,"
        " is_active_source, source_name, is_new_customer,"
        " buyer_first_order_date, promocode)"
        " VALUES ($1,$2,1,$3,$4,$5,NULL,$6,$7,$8,$9,$10,FALSE,$11,$12)",
        [(o, src, total, _instant(day, 10), buyer, _day(day), ret, stype,
          active, f"src{src}", _day(day), promo)
         for o, buyer, day, total, stype, ret, active, src, promo in ORDERS])
    await conn.executemany(
        "INSERT INTO bronze.order_products (id, order_id, product_id, name,"
        " quantity, price_sold) VALUES ($1,$2,$3,$4,$5,$6)", LINES)


@pytest_asyncio.fixture
async def engine(tmp_path, monkeypatch):
    """The repository on Postgres, the fixture seeded, its DuckDB connection
    a trap; and the pool, for the statements the tests run themselves. A real
    pool: the claim's race needs two callers that can genuinely race."""
    async with pg_sms_seed.on_postgres(monkeypatch) as pool:
        async with pool.acquire() as conn:
            await _seed(conn)
        yield pg_sms_seed.store(tmp_path, "segments.duckdb"), pool


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "label,filters,ltv_column,sales_type,tiers,grouping", CASES,
    ids=[c[0] for c in CASES],
)
async def test_the_roster_is_the_one_duckdb_returned(
    engine, label, filters, ltv_column, sales_type, tiers, grouping,
):
    _store, pool = engine
    sql, params = _render(filters, ltv_column, sales_type, tiers, grouping)

    async with pool.acquire() as conn:
        rows = [tuple(r) for r in await conn.fetch(sql, *params)]

    assert _now(_comparable(rows)) == GOLDEN[f"roster:{label}"], (
        f"Postgres disagrees with DuckDB's {label} audience"
    )


@pytest.mark.asyncio
async def test_the_fixture_actually_exercises_the_exclusions(engine):
    """A roster that agrees on easy data proves nothing, so this asserts the
    awkward rows really are being excluded — otherwise the agreement above
    could be both engines returning everybody."""
    _store, pool = engine
    sql, params = _render(SmsAudienceFilters(), "revenue_ltv", "retail", (), "single")
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *params)

    selected = {r[0] for r in rows if r[0] is not None}
    assert 4 not in selected, "an opted-out customer was messaged"
    assert 5 not in selected, "a b2b order leaked into a retail audience"
    assert 6 not in selected, "a return counted as a purchase"
    assert 7 not in selected, "a retired source counted"
    assert 8 not in selected and 9 in selected, (
        "the shared phone number was not de-duplicated to the higher-value buyer"
    )


@pytest.mark.asyncio
async def test_the_whole_result_through_the_store(engine):
    """The end-to-end shape, not just the rows: the real `get_sms_segments` —
    the level summaries, the funnel, the customer rows and the arm each
    customer landed in.

    The arms are the reason this is worth running separately. They are decided
    in Python (`core.sms_holdout`) precisely so the engines could not disagree,
    and an assertion that never looked at them would not notice if that
    stopped being true.
    """
    store, _pool = engine

    postgres = await store.get_sms_segments(
        grouping="single", include_customers=True, campaign="aug-compare",
    )

    assert _now(postgres) == GOLDEN["whole_result"]


class TestThePortedStatements:
    """The short statements, held to what DuckDB answered.

    They carry few rules, which is why they share one text rather than earning
    a dialect — but "few" is not "none": the campaign list counts arms and
    delivery states with `FILTER`, and it lost DuckDB's `GROUP BY ALL` on the
    way, so it is worth executing rather than eyeballing.
    """

    @pytest.mark.asyncio
    async def test_a_saved_preset_reads_back_as_it_did(self, engine):
        store, _pool = engine
        criteria = {"grouping": "single", "brands": ["Cosrx"], "city": "Київ"}

        await store.save_sms_audience_preset("осінь", criteria, created_by=7)
        listed = await store.list_sms_audience_presets()

        # `createdAt` is the store's own clock and is not compared.
        stripped = [
            {k: v for k, v in row.items() if k not in ("createdAt", "updatedAt")}
            for row in listed
        ]
        assert _now(stripped) == GOLDEN["presets"]
        saved = [r for r in listed if not r["builtin"]]
        assert saved and saved[0]["criteria"] == criteria

    @pytest.mark.asyncio
    async def test_an_optout_upserts(self, engine):
        store, _pool = engine
        first = await store.add_marketing_optout(77, phone=None)
        second = await store.add_marketing_optout(77, phone="380509999999")
        # The refusal usually arrives before the number does; the repeat
        # only ever fills a phone that was missing.
        assert first["buyerId"] == 77
        assert second["totalOptouts"] == first["totalOptouts"]

    @pytest.mark.asyncio
    async def test_the_campaign_list_counts_as_it_did(self, engine):
        """`GROUP BY ALL` was DuckDB-only and had to be spelled out. The arm
        and delivery counters are what would break if the grouping list drifted
        from the projection."""
        store, pool = engine
        # `exported_at` is set explicitly rather than left to `now()`, so the
        # column is compared with the rest.
        exported = _instant("2026-08-20", 9)
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO app.sms_campaigns (campaign, ltv_basis, sales_type,"
                " holdout_pct, criteria, promocode, exported_at)"
                " VALUES ('aug', 'revenue', 'retail', 10, '{}', NULL, $1)", exported)
            await conn.executemany(
                "INSERT INTO app.sms_campaign_members (campaign, buyer_id, phone,"
                " tier, assignment, orders_at_export, delivered)"
                " VALUES ('aug',$1,$2,'VIP',$3,1,$4)",
                [(1, "380500000001", "target", True),
                 (2, "380500000002", "target", False),
                 (3, "380500000003", "holdout", None)])

        rows = await store.list_sms_campaigns()

        assert _now(rows) == GOLDEN["campaign_list"]
        assert rows[0]["members"] == 3
        assert rows[0]["target"] == 2 and rows[0]["holdout"] == 1
        assert rows[0]["delivered"] == 1 and rows[0]["undelivered"] == 1


class TestTheClaimIsAtomic:
    """A campaign may be claimed once.

    This is the guard behind a real incident: a roster of 5,550 went out twice
    because a client-side timeout made the operator press send again while the
    first call was still running. It used to be a read followed by a write,
    safe only because DuckDB admits one writer and serialised the pair. A
    connection pool gives no such thing, so the claim became one conditional
    UPDATE — and the point of this test is that two callers racing on the
    pooled engine still produce exactly one winner.
    """

    @staticmethod
    async def _frozen(pool):
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO app.sms_campaigns (campaign, ltv_basis, sales_type,"
                " holdout_pct, criteria) VALUES ('race','revenue','retail',10,'{}')")
            await conn.execute(
                "INSERT INTO app.sms_campaign_members (campaign, buyer_id, phone,"
                " tier, assignment, orders_at_export)"
                " VALUES ('race',1,'380500000001','VIP','target',1)")

    @pytest.mark.asyncio
    async def test_two_racing_claims_produce_one_winner(self, engine):
        store, pool = engine
        await self._frozen(pool)

        results = await asyncio.gather(
            store.get_sms_campaign_targets("race"),
            store.get_sms_campaign_targets("race"),
            return_exceptions=True,
        )

        won = [r for r in results if not isinstance(r, Exception)]
        lost = [r for r in results if isinstance(r, ValueError)]
        assert len(won) == 1, f"the campaign was claimed {len(won)} times"
        assert len(lost) == 1
        assert "already sent" in str(lost[0])
        assert won[0] == [{"buyerId": 1, "phone": "380500000001", "tier": "VIP"}]

    @pytest.mark.asyncio
    async def test_a_claim_holds_and_a_release_hands_it_back(self, engine):
        store, pool = engine
        await self._frozen(pool)

        first = await store.get_sms_campaign_targets("race")
        assert first == [{"buyerId": 1, "phone": "380500000001", "tier": "VIP"}]
        with pytest.raises(ValueError, match="already sent"):
            await store.get_sms_campaign_targets("race")

        # …and handing it back makes it sendable again.
        await store.release_sms_campaign("race")
        assert await store.get_sms_campaign_targets("race") == first

    @pytest.mark.asyncio
    async def test_an_unknown_campaign_says_so_rather_than_claiming_nothing(
        self, engine,
    ):
        """The conditional UPDATE matches no row for a campaign that does not
        exist *and* for one already claimed. The two need different words."""
        store, _pool = engine
        with pytest.raises(ValueError, match="not frozen"):
            await store.get_sms_campaign_targets("nope")


class TestTheRosterAndTheDeliveryBinding:
    """The two write paths that carry real rules."""

    ROSTER = [
        {"buyerId": 1, "phone": "380500000001", "tier": "VIP",
         "assignment": "target", "orders": 3, "revenueLtv": 9000.0,
         "marginLtv": 4000.0, "recencyDays": 12},
        {"buyerId": 2, "phone": "380500000002", "tier": "VIP",
         "assignment": "holdout", "orders": 2, "revenueLtv": 7000.0,
         "marginLtv": 3000.0, "recencyDays": 30},
    ]

    @pytest.mark.asyncio
    async def test_a_frozen_roster_is_the_one_duckdb_froze(self, engine):
        store, _pool = engine
        criteria = {"grouping": "single", "maxRecencyDays": 270}

        postgres = await store.freeze_sms_campaign(
            "aug", self.ROSTER, criteria, "revenue", "retail", 10)

        assert _now(postgres) == GOLDEN["frozen_roster"]
        assert postgres["totals"] == {"customers": 2, "target": 1, "holdout": 1}

    @pytest.mark.asyncio
    async def test_re_freezing_is_refused(self, engine):
        """A roster that could be quietly rewritten after the send is a control
        group that cannot be trusted."""
        store, _pool = engine
        await store.freeze_sms_campaign(
            "aug", self.ROSTER, {}, "revenue", "retail", 10)
        with pytest.raises(ValueError, match="already frozen"):
            await store.freeze_sms_campaign(
                "aug", self.ROSTER, {}, "revenue", "retail", 10)

    @pytest.mark.asyncio
    async def test_the_delivery_binding_holds(self, engine):
        """The security control: one captured (id, signature) pair must not be
        re-pointable at another recipient."""
        store, _pool = engine
        await store.freeze_sms_campaign(
            "aug", self.ROSTER, {}, "revenue", "retail", 10)
        await store.record_sms_send(
            "aug", accepted={1: "msg-aaa", 2: "msg-bbb"},
            stoplisted=[], failed={})

        known = await store.record_sms_delivery(
            message_id="msg-aaa", status="DELIVRD", delivered=True,
            delivered_at=None, event_id="evt-1")
        assert known is True

        # The same event id, now naming somebody else's message.
        with pytest.raises(DlrEventRebound):
            await store.record_sms_delivery(
                message_id="msg-bbb", status="REJECTD", delivered=False,
                delivered_at=None, event_id="evt-1")

    @pytest.mark.asyncio
    async def test_a_send_marks_everyone_the_gateway_never_answered_for(
        self, engine,
    ):
        """A send that dies partway leaves targets behind, and they must not
        sit in the arm unable to respond to a message they never got."""
        store, _pool = engine
        await store.freeze_sms_campaign(
            "aug", self.ROSTER, {}, "revenue", "retail", 10)
        out = await store.record_sms_send(
            "aug", accepted={}, stoplisted=[], failed={})
        assert out["notSent"] == 1        # the one target, never answered for


class TestTheResultsMeasurement:
    """The last path, and the one whose numbers a decision rests on.

    A campaign's verdict is the target arm against the holdout, so a
    measurement that differed by engine was worse than one merely wrong: it
    would have changed its mind about whether the send worked, depending on
    which store answered. Held to DuckDB's measurement within a hair
    (`pg_sms_seed.same_numbers`: revenue is a pro-rata allocation summed in an
    order of the engine's choosing).
    """

    ROSTER = [
        {"buyerId": 1, "phone": "380500000001", "tier": "VIP",
         "assignment": "target", "orders": 3, "revenueLtv": 9000.0,
         "marginLtv": 4000.0, "recencyDays": 12},
        {"buyerId": 2, "phone": "380500000002", "tier": "VIP",
         "assignment": "holdout", "orders": 2, "revenueLtv": 7000.0,
         "marginLtv": 3000.0, "recencyDays": 30},
        {"buyerId": 3, "phone": "380500000003", "tier": "VIP",
         "assignment": "target", "orders": 1, "revenueLtv": 3000.0,
         "marginLtv": 1000.0, "recencyDays": 40},
    ]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("delivered_only", (False, True), ids=["all", "delivered"])
    async def test_the_campaign_is_measured_as_duckdb_measured_it(
        self, engine, delivered_only,
    ):
        store, _pool = engine
        sent = _instant("2026-07-01", 8)

        await store.freeze_sms_campaign(
            "res", self.ROSTER, {}, "revenue", "retail", 33, promocode="AUG")
        await store.record_sms_send(
            "res", accepted={1: "m-1", 3: "m-3"}, stoplisted=[], failed={},
            sent_at=sent)
        await store.record_sms_delivery(
            message_id="m-1", status="DELIVRD", delivered=True,
            delivered_at=None, event_id="e-1")
        postgres = await store.get_sms_campaign_results(
            "res", window_days=90, delivered_only=delivered_only)

        recorded = GOLDEN[f"results:{'delivered' if delivered_only else 'all'}"]
        same_numbers(recorded, _now(postgres))

    @pytest.mark.asyncio
    async def test_an_unsent_campaign_refuses(self, engine):
        """Without a send date the window would start anywhere, and a result
        measured over an arbitrary window is a result invented."""
        store, _pool = engine
        await store.freeze_sms_campaign(
            "unsent", self.ROSTER, {}, "revenue", "retail", 10)
        with pytest.raises(ValueError, match="no send date"):
            await store.get_sms_campaign_results("unsent")
