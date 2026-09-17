"""The Postgres twins' facts, read from a real Postgres (chain 2, step 8a).

The case the design's critics insisted on is first: Silver's own watermark
frozen hours in the past, and a bronze order with no Silver row still reported —
a grace relative to that watermark would report zero exactly when Silver stopped
being built.
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

KYIV = ZoneInfo("Europe/Kyiv")
IDS = list(range(980001, 980080))
SILVER_COLS = ("id, source_id, status_id, grand_total, ordered_at, buyer_id, manager_id,"
               " order_date, is_return, sales_type, is_active_source, source_name,"
               " is_new_customer, buyer_first_order_date, promocode")


async def _clean(pool):
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM bronze.order_products WHERE order_id = ANY($1::int[])", IDS)
        await conn.execute("DELETE FROM silver.order_utm WHERE order_id = ANY($1::int[])", IDS)
        await conn.execute("DELETE FROM silver.orders WHERE id = ANY($1::int[])", IDS)
        await conn.execute("DELETE FROM bronze.orders WHERE id = ANY($1::int[])", IDS)


async def _bronze(conn, oid, *, total=100, source=1, minutes_ago=0):
    at = datetime.now(timezone.utc) - timedelta(days=2)
    await conn.execute(
        "INSERT INTO bronze.orders (id, source_id, status_id, grand_total, ordered_at,"
        " created_at, updated_at, mirrored_at) VALUES ($1,$2,1,$3,$4,$4,$4,"
        " now() - make_interval(mins => $5))", oid, source, total, at, minutes_ago)


async def _silver(conn, oid, *, total=100, source=4, days_ago=2, sales_type="retail"):
    day = (datetime.now(timezone.utc).astimezone(KYIV) - timedelta(days=days_ago)).date()
    await conn.execute(
        f"INSERT INTO silver.orders ({SILVER_COLS}) VALUES "
        "($1,$2,1,$3,$4,NULL,NULL,$5,false,$6,true,'x',false,NULL,NULL)",
        oid, source, total, datetime.now(timezone.utc) - timedelta(days=days_ago), day, sales_type)


@pytest_asyncio.fixture
async def pool(monkeypatch):
    monkeypatch.setenv("KS_PG_DSN", DSN)
    monkeypatch.delenv("KS_PG_SILVER_INTERVAL_S", raising=False)
    p = await asyncpg.create_pool(DSN, min_size=1, max_size=4)
    await _clean(p)
    async with p.acquire() as conn:
        saved = await conn.fetch("SELECT * FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
                                 ["bronze.orders", "silver.orders"])
        await conn.execute(
            "INSERT INTO meta.mirror_state (table_name, last_attempted_at, last_ok_at, failures_since_ok, backfilled_at)"
            " VALUES ('bronze.orders', now(), now(), 0, now())"
            " ON CONFLICT (table_name) DO UPDATE SET last_attempted_at = now(), last_ok_at = now(),"
            " failures_since_ok = 0, backfilled_at = now(), last_error = NULL")
        # Silver's watermark frozen three hours ago: the twin must not care.
        await conn.execute(
            "INSERT INTO meta.mirror_state (table_name, last_attempted_at, last_ok_at, failures_since_ok)"
            " VALUES ('silver.orders', now() - interval '3 hours', now() - interval '3 hours', 0)"
            " ON CONFLICT (table_name) DO UPDATE SET last_attempted_at = now() - interval '3 hours',"
            " last_ok_at = now() - interval '3 hours'")
    with patch("core.pg.get_pool", new=AsyncMock(return_value=p)), \
         patch("core.pg.require_revision", new=AsyncMock()):
        yield p
    await _clean(p)
    async with p.acquire() as conn:
        await conn.execute("DELETE FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
                           ["bronze.orders", "silver.orders"])
        for r in saved:
            await conn.execute(
                "INSERT INTO meta.mirror_state (table_name, last_attempted_at, last_ok_at,"
                " failures_since_ok, last_error, last_rows, backfilled_at)"
                " VALUES ($1,$2,$3,$4,$5,$6,$7)",
                r["table_name"], r["last_attempted_at"], r["last_ok_at"], r["failures_since_ok"],
                r["last_error"], r["last_rows"], r["backfilled_at"])
    await p.close()


def _judge(facts):
    from core import pg_warehouse_dq

    return {i.check_name: i for i in pg_warehouse_dq.check_pg_warehouse(facts)}


class TestTheSilverArc:
    @pytest.mark.asyncio
    async def test_a_row_missing_past_the_repair_window_pages_with_silvers_watermark_frozen(self, pool):
        from core.data_quality import Severity
        from core.pg_warehouse_dq import read_facts

        async with pool.acquire() as conn:
            await _bronze(conn, IDS[0], total=321, minutes_ago=120)
        facts = await read_facts(pool=pool)
        assert facts.silver_arc.missing >= 1 and facts.silver_arc.past_repair >= 1
        issue = _judge(facts)["pg_silver_missing_rows"]
        assert issue.severity == Severity.CRITICAL and IDS[0] in issue.sample_ids

    @pytest.mark.asyncio
    async def test_inside_the_window_it_warns_and_in_flight_it_is_silent(self, pool):
        from core.data_quality import Severity
        from core.pg_warehouse_dq import read_facts

        async with pool.acquire() as conn:
            await _bronze(conn, IDS[0], minutes_ago=30)
            await _bronze(conn, IDS[1], minutes_ago=5)
        facts = await read_facts(pool=pool)
        issue = _judge(facts)["pg_silver_missing_rows"]
        assert issue.severity == Severity.WARN
        assert IDS[0] in issue.sample_ids and IDS[1] not in issue.sample_ids
        assert facts.watermark.in_flight >= 1

    @pytest.mark.asyncio
    async def test_an_orphan(self, pool):
        from core.pg_warehouse_dq import read_facts

        async with pool.acquire() as conn:
            await _silver(conn, IDS[2], total=77)
        issue = _judge(await read_facts(pool=pool))["pg_silver_orphan_rows"]
        assert IDS[2] in issue.sample_ids


class TestAttributionAndLineItems:
    @pytest.mark.asyncio
    async def test_website_orders_without_tags_are_named_in_kyivs_week(self, pool):
        from core.pg_warehouse_dq import read_facts

        async with pool.acquire() as conn:
            for i, oid in enumerate(IDS[10:60]):           # 50 website orders, 2 days ago
                await _bronze(conn, oid, source=4, minutes_ago=120)
                await _silver(conn, oid, source=4, days_ago=2)
                if i < 3:
                    await conn.execute(
                        "INSERT INTO silver.order_utm (order_id, utm_campaign) VALUES ($1, 'x')", oid)
        facts = await read_facts(pool=pool)
        assert facts.watermark.today == datetime.now(timezone.utc).astimezone(KYIV).date()
        assert facts.attribution.orders >= 50 and facts.attribution.tagged >= 3
        issue = _judge(facts)["pg_attribution_coverage_website"]
        assert issue.count >= 47

    @pytest.mark.asyncio
    async def test_a_zero_total_order_with_line_items(self, pool):
        from core.pg_warehouse_dq import read_facts

        async with pool.acquire() as conn:
            await _bronze(conn, IDS[70], total=0, minutes_ago=120)
            await _silver(conn, IDS[70], total=0)
            await conn.execute(
                "INSERT INTO bronze.order_products (id, order_id, product_id, name, quantity, price_sold)"
                " VALUES ($1, $2, NULL, 'x', 2, 150)", IDS[70] * 1000 + 1, IDS[70])
        facts = await read_facts(pool=pool)
        assert IDS[70] in facts.line_items.headline_sample
        assert "pg_headline_vs_line_items" in _judge(facts)   # DuckDB did not look here


class TestBlindness:
    @pytest.mark.asyncio
    async def test_a_held_layer_lock_blinds_the_snapshot_quickly(self, pool, monkeypatch):
        from core import pg_warehouse_dq
        from core.pg_silver import PG_LAYER_LOCK

        monkeypatch.setattr(pg_warehouse_dq, "LOCK_WAIT_S", 1)
        async with PG_LAYER_LOCK:
            facts = await asyncio.wait_for(pg_warehouse_dq.read_facts(pool=pool), timeout=10)
        assert facts.whole is not None and "PG_LAYER_LOCK" in facts.whole.reason
        assert list(_judge(facts)) == ["pg_warehouse_unwatched"]

    @pytest.mark.asyncio
    async def test_one_broken_group_does_not_blind_the_others(self, pool, monkeypatch):
        from core import pg_warehouse_dq

        monkeypatch.setattr(pg_warehouse_dq, "_ATTRIBUTION_SQL", "SELECT no_such_column FROM silver.orders")
        async with pool.acquire() as conn:
            await _bronze(conn, IDS[0], minutes_ago=120)
        facts = await pg_warehouse_dq.read_facts(pool=pool)
        assert isinstance(facts.attribution, pg_warehouse_dq.Unwatched)
        assert facts.silver_arc.missing >= 1                  # read in the same snapshot
        judged = _judge(facts)
        assert "pg_attribution_coverage_unwatched" in judged and "pg_silver_missing_rows" in judged

    @pytest.mark.asyncio
    async def test_history_not_backfilled_blinds_the_whole_history_groups(self, pool):
        from core.pg_warehouse_dq import read_facts

        async with pool.acquire() as conn:
            await conn.execute("UPDATE meta.mirror_state SET backfilled_at = NULL WHERE table_name = 'bronze.orders'")
        facts = await read_facts(pool=pool)
        assert list(_judge(facts)) == ["pg_warehouse_unwatched"]   # attribution + line items

    @pytest.mark.asyncio
    async def test_no_dsn(self, pool, monkeypatch):
        from core.pg_warehouse_dq import read_facts

        monkeypatch.delenv("KS_PG_DSN", raising=False)
        facts = await read_facts(pool=pool)
        assert facts.whole is not None and "KS_PG_DSN" in facts.whole.reason


class TestTheJobEndToEnd:
    """The integrity job with KS_DQ_PG_WAREHOUSE=on, against a clean DuckDB and a
    Postgres Silver that has lost an order: the twin's CRITICAL is journalled in
    the run and pages, and the DuckDB half is untouched."""

    @pytest_asyncio.fixture
    async def job(self, pool, tmp_path, monkeypatch):
        from core.duckdb_store import DuckDBStore
        from core.scheduler import BackgroundScheduler

        store = DuckDBStore(db_path=tmp_path / "integrity.duckdb")
        await store.connect()
        scheduler = BackgroundScheduler()
        sent = AsyncMock(return_value=True)
        monkeypatch.setattr(scheduler, "_send_dq_alert_throttled", sent)
        monkeypatch.setattr(scheduler, "_resolve_dq_layer", AsyncMock())
        with patch("core.duckdb_store.get_store", new=AsyncMock(return_value=store)):
            yield scheduler, store, sent
        await store.close()

    async def _issues(self, store, run_id):
        from core.data_quality import fetch_run_issues

        async with store.connection() as conn:
            return {r["check_name"]: r for r in fetch_run_issues(conn, run_id)}

    @pytest.mark.asyncio
    async def test_on_a_lost_postgres_row_is_journalled_and_pages(self, pool, job, monkeypatch):
        scheduler, store, sent = job
        monkeypatch.setenv("KS_DQ_PG_WAREHOUSE", "on")
        async with pool.acquire() as conn:
            await _bronze(conn, IDS[0], total=555, minutes_ago=150)

        result = await scheduler._run_dq_integrity()

        issues = await self._issues(store, result["run_id"])
        assert issues["pg_silver_missing_rows"]["severity"] == "CRITICAL"
        assert not any(name.startswith("silver_") for name in issues)   # DuckDB is clean
        assert sent.await_count == 1
        assert "pg_silver_missing_rows" in sent.await_args.kwargs["conditions"]
        unverified = scheduler._resolve_dq_layer.await_args.kwargs["unverified"]
        assert not any(c.startswith("pg_") for c in unverified)          # the twins looked

    @pytest.mark.asyncio
    async def test_off_the_twins_do_not_run_and_hold_their_conditions(self, pool, job, monkeypatch):
        from core import pg_warehouse_dq

        scheduler, store, sent = job
        monkeypatch.delenv("KS_DQ_PG_WAREHOUSE", raising=False)
        async with pool.acquire() as conn:
            await _bronze(conn, IDS[0], total=555, minutes_ago=150)

        with patch.object(pg_warehouse_dq, "read_facts", AsyncMock()) as read:
            result = await scheduler._run_dq_integrity()

        assert read.await_count == 0
        issues = await self._issues(store, result["run_id"])
        assert not any(name.startswith("pg_") for name in issues)
        assert sent.await_count == 0
        unverified = set(scheduler._resolve_dq_layer.await_args.kwargs["unverified"])
        assert pg_warehouse_dq.all_conditions() <= unverified
