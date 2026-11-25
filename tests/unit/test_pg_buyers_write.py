"""Chain 4: its shape, and what is refused before the latch.

`TestTheMigratedSchema` holds the refusals to the schema as migrated, on the
test Postgres: the loyalty figures' NUMERIC bounds and the verdicts' CHECKs.

The writers themselves are exercised against a live Postgres in
`tests/integration/test_buyers_chain_writer.py` and, for the latch, in
`tests/integration/test_latch_waits_for_a_connection.py`. Until step 14's
S14-11 this file also pinned the decision every consumer read (`mode()`,
`writes_postgres()`), a constant since S14-00; S14-11 deleted it with
`KS_WRITE_BUYERS`, and `tests/unit/test_write_flags_are_not_read.py` holds
that nothing reads the flag.
"""
from __future__ import annotations

import os
from datetime import datetime
from decimal import Decimal

import pytest
import pytest_asyncio

from core import chain_latch, pg_buyers_write as chain
from core import pg_chain_invariants as inv
from tests.integration.pg_tab_seed import needs_postgres

@pytest.fixture
def flags(monkeypatch):
    """No flag of this chain's, so a test moves exactly what it means to."""
    monkeypatch.delenv(chain.WRITE_ENV, raising=False)
    return monkeypatch


class TestTheShape:
    def test_the_tables_and_the_key(self):
        assert chain.CHAIN == "pg_buyers_write"
        assert chain.CHAIN_TABLES == (
            "bronze.buyers", "bronze.buyer_contacts", "app.buyer_gender")
        assert chain.CHAIN_SYNC_KEYS == ("last_sync_buyers",)

    def test_its_bronze_tables_are_the_writers_whole_unit(self):
        """Whoever takes the buyers takes their contacts: their one writer
        (`core.pg_buyer_rows`) writes both in one transaction. The mirror's
        `pg_buyers.BUYER_UNIT` said so until step 14's S14-13 deleted it."""
        assert {"bronze.buyers", "bronze.buyer_contacts"} <= set(chain.CHAIN_TABLES)

    def test_its_watermark_is_left_to_the_canary(self):
        """`buyer_sync_stalled` already pages on the step's own state at 90
        minutes; judging the stamp the same step writes would page twice."""
        assert chain.CHAIN_WATERMARK_MAX_AGE_MIN is None
        assert inv.watermark_limit_min(chain) is None


# ─── The migrated schema the writer answers to ──────────────────────────────

# Where KeyCRM sends each NUMERIC column of `bronze.buyers`: a key of the
# buyer's first loyalty programme (`Buyer.from_api`). A NUMERIC column this
# does not name fails the test below: its refusal has to be decided.
LOYALTY_KEY = {"loyalty_discount": "discount", "loyalty_amount": "amount"}
TEST_BUYER_IDS = range(990_000_001, 990_000_100)


@pytest_asyncio.fixture
async def buyers_pg():
    """A pool on the test Postgres that `core.pg.get_pool` answers with, the
    chain's tables and owner rows empty before and after."""
    import asyncpg
    from unittest.mock import AsyncMock, patch

    pool = await asyncpg.create_pool(os.environ["KS_PG_DSN"], min_size=1, max_size=2)

    async def clean():
        async with pool.acquire() as conn:
            for table in ("bronze.buyer_contacts", "app.buyer_gender", "bronze.buyers"):
                await conn.execute(f"DELETE FROM {table}")
            await conn.execute(
                "DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%'")

    await clean()
    try:
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)):
            yield pool
    finally:
        await clean()
        await pool.close()


@needs_postgres
class TestTheMigratedSchema:
    @pytest.mark.asyncio
    async def test_each_numeric_column_holds_what_the_writer_lets_through(self, buyers_pg):
        """The refusal before the latch must agree with the columns as
        migrated: one unit over a column's largest value is refused before
        the latch (Postgres would refuse it after, leaving a marker with no
        owner row), and the largest value itself lands, exactly. A later
        migration that widens or narrows a column without the writer
        following fails here."""
        from core.models import Buyer
        from core.store import AnalyticsStore

        async with buyers_pg.acquire() as conn:
            columns = await conn.fetch(
                "SELECT column_name, numeric_precision, numeric_scale "
                "FROM information_schema.columns WHERE table_schema = 'bronze' "
                "AND table_name = 'buyers' AND data_type = 'numeric' "
                "ORDER BY column_name")
        assert columns, "bronze.buyers has no NUMERIC column"
        for c in columns:
            assert c["column_name"] in LOYALTY_KEY, (
                f"{c['column_name']}: which KeyCRM field lands here?")

        def buyer(buyer_id, column, value):
            return Buyer.from_api({"id": buyer_id, "full_name": "Олена Петренко",
                                   "loyalty": [{LOYALTY_KEY[column]: float(value)}]})

        # NUMERIC(p, s) holds values under 10^(p-s), in steps of 10^-s.
        bound, largest = {}, {}
        for c in columns:
            name = c["column_name"]
            bound[name] = Decimal(10) ** (c["numeric_precision"] - c["numeric_scale"])
            largest[name] = bound[name] - Decimal(1).scaleb(-c["numeric_scale"])

        store = AnalyticsStore()
        ids = iter(TEST_BUYER_IDS)
        for column, over in bound.items():
            over_id, skipped = next(ids), []
            assert await store.upsert_buyers(
                [buyer(over_id, column, over)], skipped_out=skipped) == 0, column
            assert skipped == [over_id], column
        assert not chain_latch.marker_path(chain.CHAIN).exists(), (
            "a value the column refuses reached Postgres after the latch")

        landing = {column: next(ids) for column in largest}
        assert await store.upsert_buyers(
            [buyer(landing[column], column, top) for column, top in largest.items()]
        ) == len(largest)
        async with buyers_pg.acquire() as conn:
            for column, top in largest.items():
                stored = await conn.fetchval(
                    f"SELECT {column} FROM bronze.buyers WHERE id = $1", landing[column])
                assert stored == top, column

    @pytest.mark.asyncio
    async def test_every_verdict_the_classifier_can_give_is_one_the_table_takes(
            self, buyers_pg):
        """A verdict `app.buyer_gender` refuses costs the portion's verdicts,
        every tick, and nothing says why but a log line. Every gender and
        confidence the classifier declares, and NULL for each, is stored."""
        from core.gender import CONFIDENCE_LADDER, GENDERS, RULES_VERSION

        pairs = [(g, c) for g in (*GENDERS, None) for c in (*CONFIDENCE_LADDER, None)]
        rows = [(buyer_id, g, c, RULES_VERSION)
                for buyer_id, (g, c) in zip(TEST_BUYER_IDS, pairs)]
        async with buyers_pg.acquire() as conn:
            await conn.executemany(
                "INSERT INTO app.buyer_gender (buyer_id, gender, method, confidence, "
                "rules_version) VALUES ($1, $2, 'test', $3, $4)", rows)
            stored = await conn.fetch(
                "SELECT gender, confidence FROM app.buyer_gender "
                "WHERE buyer_id = ANY($1::int[])", [r[0] for r in rows])
        assert sorted(map(tuple, stored), key=repr) == sorted(pairs, key=repr)


def _row(**kw):
    from core.landing_rows import BuyerRow

    base = {f: None for f in BuyerRow._fields}
    base.update(id=1, full_name="Олена")
    base.update(kw)
    return BuyerRow(**base)


def _contact(buyer_id=1, value="+380500000001", kind="phone"):
    from core.landing_rows import ContactRow

    return ContactRow(buyer_id, kind, value, True)


class TestWhatIsRefusedBeforeTheLatch:
    """A value Postgres refuses raises inside the write, after the marker is on
    disk: a first batch refused whole would latch the chain with no owner row
    behind it. So the writer asks first."""

    def test_a_loyalty_figure_over_its_numeric(self):
        assert chain._refusal(_row(loyalty_discount=1000.0), ()) is not None
        assert chain._refusal(_row(loyalty_discount=999.99), ()) is None
        assert chain._refusal(_row(loyalty_amount=1e10), ()) is not None
        assert chain._refusal(_row(loyalty_amount=float("nan")), ()) is not None

    def test_an_id_that_is_not_an_integer(self):
        assert chain._refusal(_row(id=None), ()) is not None
        assert chain._refusal(_row(id=True), ()) is not None

    def test_a_contact_naming_another_buyer(self):
        assert chain._refusal(_row(id=1), (_contact(buyer_id=2),)) is not None
        assert chain._refusal(_row(id=1), (_contact(),)) is None

    @pytest.mark.parametrize("field,value", [
        ("id", 2 ** 31), ("manager_id", -2 ** 31 - 1), ("company_id", "7"),
        ("full_name", "Олена \ud800"), ("note", 5), ("city", b"Kyiv"),
        ("birthday", "1990-01-05"), ("updated_at", "2026-09-30"),
    ])
    def test_what_the_driver_would_refuse(self, field, value):
        """Everything asyncpg or the table refuses as a DataError, which after
        the latch would leave a first batch with the marker and no owner row
        (review of PR-3). The reason names the column, never the value."""
        why = chain._refusal(_row(**{field: value}), ())
        assert why and field in why, why
        assert str(value) not in why

    def test_a_contact_that_is_not_utf8(self):
        assert "UTF-8" in chain._refusal(_row(), (_contact(value="+380 \ud800"),))

    def test_an_ordinary_buyer_passes(self):
        from datetime import date

        assert chain._refusal(_row(
            manager_id=4, company_id=None, birthday=date(1990, 1, 5),
            updated_at=datetime(2026, 9, 30, 12, 0), loyalty_discount=5.0,
            loyalty_amount=120.5, city="Київ"), (_contact(),)) is None

    @pytest.mark.asyncio
    async def test_a_batch_refused_whole_never_reaches_the_latch(self, flags, monkeypatch):
        from unittest.mock import AsyncMock

        from core.models import Buyer

        pool = AsyncMock(side_effect=AssertionError("asked for a pool"))
        monkeypatch.setattr(chain, "_pool", pool)
        skipped = []
        bad = Buyer.from_api({"id": 5, "full_name": "Олена",
                              "loyalty": [{"discount": 5000}]})
        assert bad.loyalty_discount == 5000.0, "the fixture must carry the value"
        assert await chain.upsert_buyers([bad], skipped_out=skipped) == 0
        assert skipped == [5]
        pool.assert_not_awaited()
        assert not chain_latch.latched(chain.CHAIN)

    @pytest.mark.asyncio
    async def test_an_empty_batch_asks_nothing(self, monkeypatch):
        from unittest.mock import AsyncMock

        pool = AsyncMock(side_effect=AssertionError("asked for a pool"))
        monkeypatch.setattr(chain, "_pool", pool)
        assert await chain.upsert_buyers([]) == 0
        pool.assert_not_awaited()


class TestTheDerivationNeverRaises:
    @pytest.mark.asyncio
    async def test_a_pool_it_cannot_get_is_a_result(self, monkeypatch):
        from unittest.mock import AsyncMock

        monkeypatch.setattr(chain, "_pool",
                            AsyncMock(side_effect=OSError("connection refused")))
        out = await chain.derive_gender_pg()
        assert out["error_class"] == "OSError" and out["written"] == 0
        assert not chain_latch.latched(chain.CHAIN)

    @pytest.mark.asyncio
    async def test_a_cancellation_still_goes_through(self, monkeypatch):
        import asyncio
        from unittest.mock import AsyncMock

        monkeypatch.setattr(chain, "_pool",
                            AsyncMock(side_effect=asyncio.CancelledError()))
        with pytest.raises(asyncio.CancelledError):
            await chain.derive_gender_pg()

    def test_its_clock_is_utc_and_the_web_processs(self):
        stamp = chain._now_utc()
        assert isinstance(stamp, datetime) and stamp.utcoffset().total_seconds() == 0


class TestTheHealthBlockCarriesAClockThatSurvivesARestart:
    @pytest.mark.asyncio
    async def test_under_the_chain_it_publishes_the_watermarks_age(self, flags, monkeypatch):
        from unittest.mock import AsyncMock

        from core import pg_buyer_sync_read
        from web.routes.api import health

        flags.setenv(chain.WRITE_ENV, "junk-s14-11")    # read by nothing
        monkeypatch.setattr(health, "_buyer_sync", lambda: {"last_ok_age_s": 60})
        monkeypatch.setattr(health, "_buyer_watermark_cache", {"age": None, "expires_at": 0.0})
        monkeypatch.setattr(pg_buyer_sync_read, "watermark_age_s", AsyncMock(return_value=12_000))
        block = await health._buyer_sync_block()
        assert block == {"last_ok_age_s": 60, "watermark_age_s": 12_000}

    @pytest.mark.asyncio
    async def test_an_unreadable_store_publishes_none(self, flags, monkeypatch):
        from unittest.mock import AsyncMock

        from core import pg_buyer_sync_read
        from web.routes.api import health

        flags.setenv(chain.WRITE_ENV, "postgres")
        monkeypatch.setattr(health, "_buyer_sync", lambda: {"last_ok_age_s": 60})
        monkeypatch.setattr(health, "_buyer_watermark_cache", {"age": None, "expires_at": 0.0})
        monkeypatch.setattr(pg_buyer_sync_read, "watermark_age_s",
                            AsyncMock(side_effect=OSError("down")))
        assert (await health._buyer_sync_block())["watermark_age_s"] is None

    @pytest.mark.asyncio
    async def test_the_age_is_read_from_the_stored_value(self, monkeypatch):
        from datetime import timezone

        from core import pg_buyer_sync_read
        from unittest.mock import AsyncMock

        now = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)
        for raw, age in (("2026-09-30T14:00:00+03:00", 3600), ("", None), ("junk", None)):
            monkeypatch.setattr("core.pg_chain_watermarks.read_values",
                                AsyncMock(return_value={"last_sync_buyers": raw} if raw else {}))
            assert await pg_buyer_sync_read.watermark_age_s(now=now) == age, raw
