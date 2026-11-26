"""The admin's warehouse levers and the status page, over a real Postgres.

`POST /api/warehouse/refresh` and `POST /api/warehouse/rebuild-silver` are the
lever every Silver/Gold alert names: they run the Postgres derivation now,
past the 600-second floor a scheduled tick waits for. `GET
/api/warehouse/status` says whether a rebuild is owed and what the last run
did. All three sit on the admin-only router.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from tests.helpers.app_client import signed_in
from tests.integration.pg_tab_seed import DSN, needs_postgres, seed, tab_in_postgres

asyncpg = pytest.importorskip("asyncpg")

pytestmark = needs_postgres

DAY = date(2026, 3, 10)
AT = datetime(2026, 3, 10, 9, 0, tzinfo=timezone.utc)
# Two retail orders: Instagram and the website, no manager.
# `(id, source_id, status_id, grand_total, ordered_at, buyer_id, manager_id, promocode)`
ORDERS = [
    (96100001, 1, 1, 120, AT, None, None, None),
    (96100002, 4, 1, 80, AT, None, None, None),
]
LANDED_LATER = 96100003
IDS = [row[0] for row in ORDERS] + [LANDED_LATER]


async def _restore(signal, last_run: int, stamps) -> None:
    conn = await asyncpg.connect(DSN)
    try:
        await conn.execute("DELETE FROM app.order_versions WHERE order_id = ANY($1::int[])", IDS)
        await conn.execute(
            "UPDATE meta.derivation_signal SET requested = $1, built = $2,"
            " requested_at = $3, built_at = $4 WHERE layer = 'warehouse'", *signal)
        await conn.execute("DELETE FROM meta.derivation_runs WHERE id > $1", last_run)
        await conn.execute("DELETE FROM meta.mirror_state")
        for row in stamps:
            columns = list(row.keys())
            await conn.execute(
                f"INSERT INTO meta.mirror_state ({', '.join(columns)}) VALUES "
                f"({', '.join(f'${i}' for i in range(1, len(columns) + 1))})",
                *row.values())
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def held_at_the_floor(monkeypatch):
    """Two orders landed in Bronze and nothing derived from them, a rebuild
    owed, and a run journalled a moment ago, so a scheduled tick waits out
    its floor. The scheduler is a fresh, unstarted one, as in a web process
    past its first tick. The signal, the journal, every watermark and the
    order versions are put back afterwards."""
    from core import pg_derivation, scheduler as scheduler_module
    from core.scheduler import BackgroundScheduler

    monkeypatch.delenv("KS_PG_SILVER_INTERVAL_S", raising=False)   # the 600 s floor
    monkeypatch.setattr(scheduler_module, "_scheduler", BackgroundScheduler())
    monkeypatch.setattr(BackgroundScheduler, "_pg_derive_ran", True)
    monkeypatch.setattr(BackgroundScheduler, "_pg_derive_started", None)
    monkeypatch.setattr(pg_derivation, "_signal_failures", 0)

    conn = await asyncpg.connect(DSN)
    try:
        signal = tuple(await conn.fetchrow(
            "SELECT requested, built, requested_at, built_at FROM meta.derivation_signal"
            " WHERE layer = 'warehouse'"))
        last_run = await conn.fetchval("SELECT COALESCE(max(id), 0) FROM meta.derivation_runs")
        stamps = await conn.fetch("SELECT * FROM meta.mirror_state")
    finally:
        await conn.close()

    async def populate(conn, _pool):
        await seed(conn, orders=ORDERS)
        await conn.execute(
            "UPDATE meta.derivation_signal SET requested = built + 1 WHERE layer = 'warehouse'")
        now = datetime.now(timezone.utc)
        await pg_derivation.record_run(conn, trigger="signal", started_at=now, ended_at=now)

    try:
        # The alert journal and Telegram are not what is under test.
        with patch("core.alerting.raise_alert", AsyncMock(return_value=1)), \
             patch("core.alerting.resolve_group", AsyncMock(return_value=0)):
            async with tab_in_postgres(DSN, monkeypatch, populate) as pool:
                yield pool
    finally:
        await _restore(signal, last_run, stamps)


async def _signal(pool):
    async with pool.acquire() as conn:
        return tuple(await conn.fetchrow(
            "SELECT requested, built FROM meta.derivation_signal WHERE layer = 'warehouse'"))


class TestTheLeverDerivesPastTheFloor:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("path, besides", [
        pytest.param("/api/warehouse/refresh", {"trigger": "manual"}, id="refresh"),
        pytest.param("/api/warehouse/rebuild-silver", {}, id="rebuild-silver"),
    ])
    async def test_it_rebuilds_silver_and_gold_now_and_journals_a_manual_run(
            self, held_at_the_floor, path, besides):
        from core.scheduler import BackgroundScheduler

        pool = held_at_the_floor
        async with pool.acquire() as conn:
            runs_before = await conn.fetchval("SELECT COALESCE(max(id), 0) FROM meta.derivation_runs")
        # The premise: the scheduled tick would wait.
        assert await BackgroundScheduler()._run_pg_derivation() == {
            "skipped": True, "reason": "floor"}

        async with signed_in("admin") as admin:
            response = await admin.post(path)

        assert response.status_code == 200, response.text
        body = response.json()
        assert {k: v for k, v in body.items() if k != "postgres"} == besides
        derived = body["postgres"]
        assert (derived["status"], derived["trigger"], derived["validation_passed"],
                derived["silver_rows"]) == ("success", "manual", True, 2)

        async with pool.acquire() as conn:
            silver = {r["id"] for r in await conn.fetch("SELECT id FROM silver.orders")}
            revenue = await conn.fetchval(
                "SELECT revenue FROM gold.daily_revenue WHERE date = $1"
                " AND sales_type = 'retail' AND source_id IS NULL", DAY)
            runs = await conn.fetch(
                "SELECT trigger, validation_passed, error FROM meta.derivation_runs"
                " WHERE id > $1 ORDER BY id", runs_before)
        assert silver == {96100001, 96100002}
        assert revenue == 200
        assert [tuple(r) for r in runs] == [("manual", True, None)]
        requested, built = await _signal(pool)
        assert requested == built, "the run left the debt owed"


class TestTheStatusPage:
    @pytest.mark.asyncio
    async def test_it_says_owed_once_an_order_lands_after_a_run(self, held_at_the_floor):
        from core.landing_rows import OrderRow
        from core.pg_landing import write_orders

        async with signed_in("admin") as admin:
            assert (await admin.post("/api/warehouse/refresh")).status_code == 200
            after_run = (await admin.get("/api/warehouse/status")).json()["postgres"]
            await write_orders([OrderRow(
                id=LANDED_LATER, source_id=1, status_id=1, status_group_id=1,
                grand_total=50.0, ordered_at=AT, created_at=AT, updated_at=AT,
                buyer_id=None, manager_id=None, manager_comment=None, promocode=None,
            )], [], replace_products=False)
            after_mark = (await admin.get("/api/warehouse/status")).json()["postgres"]

        assert after_run["owed"] is False
        assert after_run["requested"] == after_run["built"]
        assert after_run["built_at"] is not None
        assert (after_run["last_run"]["trigger"], after_run["last_run"]["validation_passed"],
                after_run["last_run"]["error"]) == ("manual", True, None)

        assert after_mark["owed"] is True
        assert after_mark["requested"] == after_run["requested"] + 1
        assert after_mark["built"] == after_run["built"]
        assert after_mark["last_run"] == after_run["last_run"]
