"""The audience query means the same thing in both engines.

`tests/unit/test_sms_segments_dialect.py` proves the two renderings are *one
text*. That is necessary and not sufficient: identical SQL can still mean
different things where the engines' semantics differ — integer division, NULL
ordering, how a window function breaks a tie, what `ROUND` does at .005.

So this runs the real thing. The same synthetic customers go into a DuckDB
store and a live PostgreSQL, both renderings execute, and the rosters are
compared row by row. Skipped without `KS_PG_DSN`, the same contract
`test_bot_database.py` has with its Postgres arm.

The fixture is small and deliberately awkward, because a roster that agrees on
easy data proves nothing. It carries a product name long enough to be
truncated, two buyers sharing one phone number (only the higher-value one may
survive), an opted-out customer, a return, an order on a retired source, and a
b2b order that must be invisible unless `sales_type=all` asks for it.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timezone

from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from core.duckdb_store import DuckDBStore
from core.repositories.customers import DlrEventRebound, SmsAudienceFilters
from core.sql_dialect import DUCKDB, POSTGRES, sms_segments_select

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(
    not DSN, reason="needs a live PostgreSQL at KS_PG_DSN",
)

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


def _render(dialect, filters, ltv_column, sales_type, tiers, grouping):
    tier_case = f"""CASE
        WHEN c.{ltv_column} >= ? THEN 'VIP'
        WHEN c.orders >= ? OR c.{ltv_column} >= ? THEN 'CORE'
        WHEN c.recency <= ? THEN 'REACTIVATION' END"""
    filter_sql, filter_params = filters.predicate(
        ltv_column, sales_type, dialect.order_lines,
    )
    sql = sms_segments_select(
        dialect,
        ltv_column=ltv_column,
        sales_type_filter="" if sales_type == "all" else "AND l.sales_type = ?",
        tier_case=tier_case,
        arm_expr="'ALL'" if grouping == "single" else "tier_level",
        ok_tier_expr="TRUE" if grouping == "single" else "tier_level IS NOT NULL",
        filter_sql=filter_sql,
        tier_subset=(f"WHERE tier_level IN ({', '.join('?' * len(tiers))})"
                     if tiers else ""),
    )
    params: list = []
    if sales_type != "all":
        params.append(sales_type)
    params += [5000.0, 2, 2000.0, 120, 270]
    params += filter_params
    params += list(tiers)
    return sql, params


def _numbered(sql: str) -> str:
    """`?` placeholders renumbered for asyncpg, which wants `$1 … $n`.

    Positional in both drivers, so the order the caller built them in is the
    order they bind in — no mapping to keep in step.
    """
    out, n = [], 0
    for char in sql:
        if char == "?":
            n += 1
            out.append(f"${n}")
        else:
            out.append(char)
    return "".join(out)


def _comparable(rows):
    """Money as float at two decimals: DuckDB hands back DECIMAL, asyncpg
    Decimal, and the roster is the thing being compared, not the type."""
    out = []
    for row in rows:
        out.append(tuple(
            round(float(v), 2) if hasattr(v, "as_tuple") else v for v in row
        ))
    return out


async def _seed_duckdb(store):
    async with store.connection() as conn:
        for pid, brand, sku, cat in PRODUCTS:
            conn.execute("INSERT INTO products (id, name, brand, sku, category_id)"
                         " VALUES (?,?,?,?,?)", [pid, f"p{pid}", brand, sku, cat])
        for cid, name, parent in CATEGORIES:
            conn.execute("INSERT INTO categories (id, name, parent_id)"
                         " VALUES (?,?,?)", [cid, name, parent])
        for oid, sku, cost in STOCKS:
            conn.execute("INSERT INTO offer_stocks (id, sku, purchased_price)"
                         " VALUES (?,?,?)", [oid, sku, cost])
        for bid, name, city, phone in BUYERS:
            conn.execute("INSERT INTO buyers (id, full_name, city, phone)"
                         " VALUES (?,?,?,?)", [bid, name, city, phone])
        for bid, channel, phone in OPTOUTS:
            conn.execute("INSERT INTO marketing_optouts (buyer_id, channel, phone)"
                         " VALUES (?,?,?)", [bid, channel, phone])
        for oid, buyer, day, total, stype, ret, active, src, promo in ORDERS:
            conn.execute(
                "INSERT INTO silver_orders (id, source_id, status_id, grand_total,"
                " ordered_at, buyer_id, manager_id, order_date, is_return,"
                " sales_type, is_active_source, source_name, is_new_customer,"
                " buyer_first_order_date, promocode)"
                " VALUES (?,?,1,?,?,?,NULL,?,?,?,?,?,FALSE,?,?)",
                [oid, src, total, f"{day} 10:00:00+00", buyer, day, ret, stype,
                 active, f"src{src}", day, promo])
        for lid, oid, pid, name, qty, price in LINES:
            conn.execute("INSERT INTO order_products (id, order_id, product_id,"
                         " name, quantity, price_sold) VALUES (?,?,?,?,?,?)",
                         [lid, oid, pid, name, qty, price])


async def _seed_postgres(conn):
    await conn.execute(
        "TRUNCATE silver.orders, bronze.order_products, bronze.products,"
        " bronze.categories, bronze.buyers, bronze.offer_stocks,"
        " app.marketing_optouts, app.sms_campaigns, app.sms_campaign_members,"
        " app.sms_audience_presets, app.sms_dlr_events")
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
        [(o, src, total,
          datetime.fromisoformat(f"{day} 10:00:00+00:00"), buyer,
          date.fromisoformat(day), ret, stype, active, f"src{src}",
          date.fromisoformat(day), promo)
         for o, buyer, day, total, stype, ret, active, src, promo in ORDERS])
    await conn.executemany(
        "INSERT INTO bronze.order_products (id, order_id, product_id, name,"
        " quantity, price_sold) VALUES ($1,$2,$3,$4,$5,$6)", LINES)


@pytest_asyncio.fixture
async def engines(tmp_path):
    store = DuckDBStore(db_path=tmp_path / "segments.duckdb")
    await store.connect()
    conn = await asyncpg.connect(DSN)
    try:
        await _seed_duckdb(store)
        await _seed_postgres(conn)
        yield store, conn
    finally:
        await conn.close()
        await store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "label,filters,ltv_column,sales_type,tiers,grouping", CASES,
    ids=[c[0] for c in CASES],
)
async def test_both_engines_return_the_same_roster(
    engines, label, filters, ltv_column, sales_type, tiers, grouping,
):
    store, conn = engines

    duck_sql, params = _render(DUCKDB, filters, ltv_column, sales_type, tiers, grouping)
    pg_sql, pg_params = _render(POSTGRES, filters, ltv_column, sales_type, tiers,
                                grouping)

    async with store.connection() as duck:
        duck_rows = duck.execute(duck_sql, params).fetchall()
    pg_rows = [tuple(r) for r in await conn.fetch(_numbered(pg_sql), *pg_params)]

    assert _comparable(duck_rows) == _comparable(pg_rows), (
        f"the two engines disagree about the {label} audience"
    )


@pytest.mark.asyncio
async def test_the_fixture_actually_exercises_the_exclusions(engines):
    """A roster that agrees on easy data proves nothing, so this asserts the
    awkward rows really are being excluded — otherwise the agreement above
    could be two engines both returning everybody."""
    store, _conn = engines
    sql, params = _render(DUCKDB, SmsAudienceFilters(), "revenue_ltv", "retail",
                          (), "single")
    async with store.connection() as duck:
        rows = duck.execute(sql, params).fetchall()

    selected = {r[0] for r in rows if r[0] is not None}
    assert 4 not in selected, "an opted-out customer was messaged"
    assert 5 not in selected, "a b2b order leaked into a retail audience"
    assert 6 not in selected, "a return counted as a purchase"
    assert 7 not in selected, "a retired source counted"
    assert 8 not in selected and 9 in selected, (
        "the shared phone number was not de-duplicated to the higher-value buyer"
    )


@pytest.mark.asyncio
async def test_the_whole_result_matches_through_the_store(engines, monkeypatch):
    """The end-to-end shape, not just the rows.

    Above, both renderings are executed side by side. This drives the real
    `get_sms_segments` twice — once per engine, chosen by `KS_SMS_STORE` — and
    compares what the API would actually return: the level summaries, the
    funnel, the customer rows and the arm each customer landed in.

    The arms are the reason this is worth running separately. They are decided
    in Python now (`core.sms_holdout`) precisely so the engines cannot
    disagree, and an assertion that never looked at them would not notice if
    that stopped being true.
    """
    store, conn = engines

    monkeypatch.delenv("KS_SMS_STORE", raising=False)
    duck = await store.get_sms_segments(
        grouping="single", include_customers=True, campaign="aug-compare",
    )

    monkeypatch.setenv("KS_SMS_STORE", "postgres")
    with patch("core.pg.get_pool", new=AsyncMock(return_value=_PoolOf(conn))), \
         patch("core.pg.require_revision", new=AsyncMock()):
        postgres = await store.get_sms_segments(
            grouping="single", include_customers=True, campaign="aug-compare",
        )

    assert duck["funnel"] == postgres["funnel"]
    assert duck["segments"] == postgres["segments"]
    assert duck["totals"] == postgres["totals"]
    assert [c["buyerId"] for c in duck["customers"]] == \
           [c["buyerId"] for c in postgres["customers"]]
    assert [c["assignment"] for c in duck["customers"]] == \
           [c["assignment"] for c in postgres["customers"]]
    assert duck["customers"] == postgres["customers"]


class _PoolOf:
    """The one connection the fixture already opened, shaped like a pool."""

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


class TestThePortedStatements:
    """The short statements, run against both stores and compared.

    They carry few rules, which is why they share one text rather than earning
    a dialect — but "few" is not "none": the campaign list counts arms and
    delivery states with `FILTER`, and it lost DuckDB's `GROUP BY ALL` on the
    way, so it is worth executing rather than eyeballing.
    """

    @staticmethod
    async def _both(store, conn, call, monkeypatch):
        monkeypatch.delenv("KS_SMS_STORE", raising=False)
        duck = await call()
        monkeypatch.setenv("KS_SMS_STORE", "postgres")
        with patch("core.pg.get_pool", new=AsyncMock(return_value=_PoolOf(conn))), \
             patch("core.pg.require_revision", new=AsyncMock()):
            postgres = await call()
        monkeypatch.delenv("KS_SMS_STORE", raising=False)
        return duck, postgres

    @pytest.mark.asyncio
    async def test_a_saved_preset_reads_back_the_same_from_either_store(
        self, engines, monkeypatch,
    ):
        store, conn = engines
        criteria = {"grouping": "single", "brands": ["Cosrx"], "city": "Київ"}

        # Written to each store in turn, then read back from each.
        monkeypatch.delenv("KS_SMS_STORE", raising=False)
        await store.save_sms_audience_preset("осінь", criteria, created_by=7)
        monkeypatch.setenv("KS_SMS_STORE", "postgres")
        with patch("core.pg.get_pool", new=AsyncMock(return_value=_PoolOf(conn))), \
             patch("core.pg.require_revision", new=AsyncMock()):
            await store.save_sms_audience_preset("осінь", criteria, created_by=7)
        monkeypatch.delenv("KS_SMS_STORE", raising=False)

        duck, postgres = await self._both(
            store, conn, store.list_sms_audience_presets, monkeypatch,
        )
        # `createdAt` is each store's own clock and must not be compared.
        strip = lambda rows: [   # noqa: E731
            {k: v for k, v in row.items() if k not in ("createdAt", "updatedAt")}
            for row in rows
        ]
        assert strip(duck) == strip(postgres)
        saved = [r for r in duck if not r["builtin"]]
        assert saved and saved[0]["criteria"] == criteria

    @pytest.mark.asyncio
    async def test_an_optout_upserts_the_same_way_in_both(self, engines, monkeypatch):
        store, conn = engines

        for flag in (None, "postgres"):
            if flag is None:
                monkeypatch.delenv("KS_SMS_STORE", raising=False)
                first = await store.add_marketing_optout(77, phone=None)
                second = await store.add_marketing_optout(77, phone="380509999999")
            else:
                monkeypatch.setenv("KS_SMS_STORE", flag)
                with patch("core.pg.get_pool",
                           new=AsyncMock(return_value=_PoolOf(conn))), \
                     patch("core.pg.require_revision", new=AsyncMock()):
                    first = await store.add_marketing_optout(77, phone=None)
                    second = await store.add_marketing_optout(77, phone="380509999999")
            # The refusal usually arrives before the number does; the repeat
            # only ever fills a phone that was missing.
            assert first["buyerId"] == 77
            assert second["totalOptouts"] == first["totalOptouts"]
        monkeypatch.delenv("KS_SMS_STORE", raising=False)

    @pytest.mark.asyncio
    async def test_the_campaign_list_counts_the_same(self, engines, monkeypatch):
        """`GROUP BY ALL` was DuckDB-only and had to be spelled out. The arm
        and delivery counters are what would break if the grouping list drifted
        from the projection."""
        store, conn = engines
        # `exported_at` is set explicitly rather than left to each store's
        # `now()`: two independently stamped defaults differ by construction,
        # and excluding the column would stop it being compared at all.
        exported = datetime.fromisoformat("2026-08-20 09:00:00+00:00")
        rows = [
            ("aug", "revenue", "retail", 10, "{}", None, exported),
        ]
        async with store.connection() as duck:
            for c, basis, stype, pct, crit, promo, when in rows:
                duck.execute(
                    "INSERT INTO sms_campaigns (campaign, ltv_basis, sales_type,"
                    " holdout_pct, criteria, promocode, exported_at)"
                    " VALUES (?,?,?,?,?,?,?)",
                    [c, basis, stype, pct, crit, promo, when])
            for buyer, arm, delivered in ((1, "target", True), (2, "target", False),
                                          (3, "holdout", None)):
                duck.execute(
                    "INSERT INTO sms_campaign_members (campaign, buyer_id, phone,"
                    " tier, assignment, orders_at_export, delivered)"
                    " VALUES ('aug',?,?,'VIP',?,1,?)",
                    [buyer, f"38050000000{buyer}", arm, delivered])
        await conn.executemany(
            "INSERT INTO app.sms_campaigns (campaign, ltv_basis, sales_type,"
            " holdout_pct, criteria, promocode, exported_at)"
            " VALUES ($1,$2,$3,$4,$5,$6,$7)", rows)
        await conn.executemany(
            "INSERT INTO app.sms_campaign_members (campaign, buyer_id, phone,"
            " tier, assignment, orders_at_export, delivered)"
            " VALUES ('aug',$1,$2,'VIP',$3,1,$4)",
            [(1, "380500000001", "target", True), (2, "380500000002", "target", False),
             (3, "380500000003", "holdout", None)])

        duck_rows, pg_rows = await self._both(
            store, conn, store.list_sms_campaigns, monkeypatch,
        )
        assert duck_rows == pg_rows
        assert duck_rows[0]["members"] == 3
        assert duck_rows[0]["target"] == 2 and duck_rows[0]["holdout"] == 1
        assert duck_rows[0]["delivered"] == 1 and duck_rows[0]["undelivered"] == 1


class TestTheClaimIsAtomic:
    """A campaign may be claimed once, on either engine.

    This is the guard behind a real incident: a roster of 5,550 went out twice
    because a client-side timeout made the operator press send again while the
    first call was still running. It used to be a read followed by a write,
    safe only because DuckDB admits one writer and serialised the pair. A
    connection pool gives no such thing, so the claim became one conditional
    UPDATE — and the point of this test is that two callers racing on the
    *pooled* engine still produce exactly one winner.
    """

    @staticmethod
    async def _frozen(store, conn):
        async with store.connection() as duck:
            duck.execute(
                "INSERT INTO sms_campaigns (campaign, ltv_basis, sales_type,"
                " holdout_pct, criteria) VALUES ('race','revenue','retail',10,'{}')")
            duck.execute(
                "INSERT INTO sms_campaign_members (campaign, buyer_id, phone,"
                " tier, assignment, orders_at_export)"
                " VALUES ('race',1,'380500000001','VIP','target',1)")
        await conn.execute(
            "INSERT INTO app.sms_campaigns (campaign, ltv_basis, sales_type,"
            " holdout_pct, criteria) VALUES ('race','revenue','retail',10,'{}')")
        await conn.execute(
            "INSERT INTO app.sms_campaign_members (campaign, buyer_id, phone,"
            " tier, assignment, orders_at_export)"
            " VALUES ('race',1,'380500000001','VIP','target',1)")

    @pytest.mark.asyncio
    async def test_two_racing_claims_produce_one_winner_on_postgres(
        self, engines, monkeypatch,
    ):
        import asyncio

        store, conn = engines
        await self._frozen(store, conn)

        # A real pool, not the single-connection stand-in the other tests use:
        # asyncpg forbids two queries on one connection, so a shared one would
        # make the loser fail for the wrong reason and prove nothing about the
        # claim. Two callers must be able to genuinely race.
        pool = await asyncpg.create_pool(DSN, min_size=2, max_size=4)
        monkeypatch.setenv("KS_SMS_STORE", "postgres")
        try:
            with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
                 patch("core.pg.require_revision", new=AsyncMock()):
                results = await asyncio.gather(
                    store.get_sms_campaign_targets("race"),
                    store.get_sms_campaign_targets("race"),
                    return_exceptions=True,
                )
        finally:
            await pool.close()
            monkeypatch.delenv("KS_SMS_STORE", raising=False)

        won = [r for r in results if not isinstance(r, Exception)]
        lost = [r for r in results if isinstance(r, ValueError)]
        assert len(won) == 1, f"the campaign was claimed {len(won)} times"
        assert len(lost) == 1
        assert "already sent" in str(lost[0])
        assert won[0] == [{"buyerId": 1, "phone": "380500000001", "tier": "VIP"}]

    @pytest.mark.asyncio
    async def test_the_same_holds_on_duckdb(self, engines, monkeypatch):
        store, conn = engines
        await self._frozen(store, conn)
        monkeypatch.delenv("KS_SMS_STORE", raising=False)

        first = await store.get_sms_campaign_targets("race")
        assert first == [{"buyerId": 1, "phone": "380500000001", "tier": "VIP"}]
        with pytest.raises(ValueError, match="already sent"):
            await store.get_sms_campaign_targets("race")

        # …and handing it back makes it sendable again.
        await store.release_sms_campaign("race")
        assert await store.get_sms_campaign_targets("race") == first

    @pytest.mark.asyncio
    async def test_an_unknown_campaign_says_so_rather_than_claiming_nothing(
        self, engines, monkeypatch,
    ):
        """The conditional UPDATE matches no row for a campaign that does not
        exist *and* for one already claimed. The two need different words."""
        store, conn = engines
        monkeypatch.delenv("KS_SMS_STORE", raising=False)
        with pytest.raises(ValueError, match="not frozen"):
            await store.get_sms_campaign_targets("nope")


class TestTheRosterAndTheDeliveryBinding:
    """The two write paths that carry real rules, on both engines."""

    ROSTER = [
        {"buyerId": 1, "phone": "380500000001", "tier": "VIP",
         "assignment": "target", "orders": 3, "revenueLtv": 9000.0,
         "marginLtv": 4000.0, "recencyDays": 12},
        {"buyerId": 2, "phone": "380500000002", "tier": "VIP",
         "assignment": "holdout", "orders": 2, "revenueLtv": 7000.0,
         "marginLtv": 3000.0, "recencyDays": 30},
    ]

    @staticmethod
    def _pg(conn):
        return patch("core.pg.get_pool", new=AsyncMock(return_value=_PoolOf(conn)))

    @pytest.mark.asyncio
    async def test_a_frozen_roster_is_identical_in_both(self, engines, monkeypatch):
        store, conn = engines
        criteria = {"grouping": "single", "maxRecencyDays": 270}

        monkeypatch.delenv("KS_SMS_STORE", raising=False)
        duck = await store.freeze_sms_campaign(
            "aug", self.ROSTER, criteria, "revenue", "retail", 10)

        monkeypatch.setenv("KS_SMS_STORE", "postgres")
        with self._pg(conn), patch("core.pg.require_revision", new=AsyncMock()):
            postgres = await store.freeze_sms_campaign(
                "aug", self.ROSTER, criteria, "revenue", "retail", 10)
        monkeypatch.delenv("KS_SMS_STORE", raising=False)

        assert duck == postgres
        assert duck["totals"] == {"customers": 2, "target": 1, "holdout": 1}

    @pytest.mark.asyncio
    async def test_re_freezing_is_refused_the_same_way(self, engines, monkeypatch):
        """A roster that could be quietly rewritten after the send is a control
        group that cannot be trusted."""
        store, conn = engines
        monkeypatch.delenv("KS_SMS_STORE", raising=False)
        await store.freeze_sms_campaign(
            "aug", self.ROSTER, {}, "revenue", "retail", 10)

        with pytest.raises(ValueError, match="already frozen"):
            await store.freeze_sms_campaign(
                "aug", self.ROSTER, {}, "revenue", "retail", 10)

        monkeypatch.setenv("KS_SMS_STORE", "postgres")
        with self._pg(conn), patch("core.pg.require_revision", new=AsyncMock()):
            await store.freeze_sms_campaign(
                "aug", self.ROSTER, {}, "revenue", "retail", 10)
            with pytest.raises(ValueError, match="already frozen"):
                await store.freeze_sms_campaign(
                    "aug", self.ROSTER, {}, "revenue", "retail", 10)
        monkeypatch.delenv("KS_SMS_STORE", raising=False)

    @pytest.mark.asyncio
    async def test_the_delivery_binding_holds_on_postgres_too(
        self, engines, monkeypatch,
    ):
        """The security control, on the engine it is moving to: one captured
        (id, signature) pair must not be re-pointable at another recipient."""
        store, conn = engines
        monkeypatch.setenv("KS_SMS_STORE", "postgres")
        try:
            with self._pg(conn), patch("core.pg.require_revision", new=AsyncMock()):
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
        finally:
            monkeypatch.delenv("KS_SMS_STORE", raising=False)

    @pytest.mark.asyncio
    async def test_a_send_marks_everyone_the_gateway_never_answered_for(
        self, engines, monkeypatch,
    ):
        """A send that dies partway leaves targets behind, and they must not
        sit in the arm unable to respond to a message they never got."""
        store, conn = engines
        monkeypatch.setenv("KS_SMS_STORE", "postgres")
        try:
            with self._pg(conn), patch("core.pg.require_revision", new=AsyncMock()):
                await store.freeze_sms_campaign(
                    "aug", self.ROSTER, {}, "revenue", "retail", 10)
                out = await store.record_sms_send(
                    "aug", accepted={}, stoplisted=[], failed={})
            assert out["notSent"] == 1        # the one target, never answered for
        finally:
            monkeypatch.delenv("KS_SMS_STORE", raising=False)


def _same_numbers(a, b, *, tol=1e-6, path="result"):
    """Compare two result trees, allowing floats to differ by a hair.

    Not laxity — the documented behaviour of two engines summing the same rows.
    Revenue here is a pro-rata allocation summed per buyer, float addition is
    not associative, and the two databases return rows in different orders, so
    the last bit disagrees: `6433.333333333334` against `6433.333333333333`.
    The Gold reconciliation carries the same tolerance for the same reason —
    exact equality there would buy a daily discrepancy of two nanohryvnia.

    Everything that is not a float is still compared exactly, which is what
    keeps counts, arms and delivery states honest.
    """
    if isinstance(a, dict) and isinstance(b, dict):
        assert a.keys() == b.keys(), f"{path}: different keys"
        for key in a:
            _same_numbers(a[key], b[key], tol=tol, path=f"{path}.{key}")
    elif isinstance(a, list) and isinstance(b, list):
        assert len(a) == len(b), f"{path}: {len(a)} vs {len(b)} items"
        for i, (x, y) in enumerate(zip(a, b)):
            _same_numbers(x, y, tol=tol, path=f"{path}[{i}]")
    elif isinstance(a, float) or isinstance(b, float):
        assert a == pytest.approx(b, abs=tol), f"{path}: {a} vs {b}"
    else:
        assert a == b, f"{path}: {a!r} vs {b!r}"



class TestTheResultsMeasurement:
    """The last path, and the one whose numbers a decision rests on.

    A campaign's verdict is the target arm against the holdout, so a
    measurement that differs by engine is worse than one that is merely wrong:
    it would change its mind about whether the send worked, depending on which
    store answered.
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
    async def test_both_engines_measure_the_campaign_the_same(
        self, engines, monkeypatch, delivered_only,
    ):
        store, conn = engines
        sent = datetime.fromisoformat("2026-07-01 08:00:00+00:00")

        # The same campaign in both stores, sent at the same instant, so the
        # attribution window opens on the same moment.
        monkeypatch.delenv("KS_SMS_STORE", raising=False)
        await store.freeze_sms_campaign(
            "res", self.ROSTER, {}, "revenue", "retail", 33, promocode="AUG")
        await store.record_sms_send(
            "res", accepted={1: "m-1", 3: "m-3"}, stoplisted=[], failed={},
            sent_at=sent)
        await store.record_sms_delivery(
            message_id="m-1", status="DELIVRD", delivered=True,
            delivered_at=None, event_id="e-1")

        with patch("core.pg.get_pool", new=AsyncMock(return_value=_PoolOf(conn))), \
             patch("core.pg.require_revision", new=AsyncMock()):
            monkeypatch.setenv("KS_SMS_STORE", "postgres")
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

        monkeypatch.delenv("KS_SMS_STORE", raising=False)
        duck = await store.get_sms_campaign_results(
            "res", window_days=90, delivered_only=delivered_only)

        _same_numbers(duck, postgres)

    @pytest.mark.asyncio
    async def test_an_unsent_campaign_refuses_on_both(self, engines, monkeypatch):
        """Without a send date the window would start anywhere, and a result
        measured over an arbitrary window is a result invented."""
        store, conn = engines
        monkeypatch.delenv("KS_SMS_STORE", raising=False)
        await store.freeze_sms_campaign(
            "unsent", self.ROSTER, {}, "revenue", "retail", 10)
        with pytest.raises(ValueError, match="no send date"):
            await store.get_sms_campaign_results("unsent")

        monkeypatch.setenv("KS_SMS_STORE", "postgres")
        try:
            with patch("core.pg.get_pool", new=AsyncMock(return_value=_PoolOf(conn))), \
                 patch("core.pg.require_revision", new=AsyncMock()):
                await store.freeze_sms_campaign(
                    "unsent", self.ROSTER, {}, "revenue", "retail", 10)
                with pytest.raises(ValueError, match="no send date"):
                    await store.get_sms_campaign_results("unsent")
        finally:
            monkeypatch.delenv("KS_SMS_STORE", raising=False)
