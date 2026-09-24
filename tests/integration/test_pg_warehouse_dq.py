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
        # Silver's watermark frozen three hours ago: the arc twin must not care.
        # (The row-values twin reads it as the last rebuild; `rv` moves it.)
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


class TestExactCounts:
    """Exact, not `>=`: found reviewing #215, where inflating in_flight or
    moving a window edge would have survived every assertion."""

    @pytest.mark.asyncio
    async def test_in_flight_counts_only_orders_inside_the_grace(self, pool):
        from core.pg_warehouse_dq import read_facts

        before = (await read_facts(pool=pool)).watermark.in_flight
        async with pool.acquire() as conn:
            await _bronze(conn, IDS[0], minutes_ago=5)
            await _bronze(conn, IDS[1], minutes_ago=30)
            await _bronze(conn, IDS[2], minutes_ago=120)
        after = (await read_facts(pool=pool)).watermark.in_flight
        assert after - before == 1

    @pytest.mark.asyncio
    async def test_the_attribution_windows_edge_by_edge(self, pool):
        """A date on which no real row lives, so every count compares with ==.
        Current window [today-7, today), baseline [today-35, today-7)."""
        from datetime import date

        from core.pg_warehouse_dq import Attribution, read_facts

        today = date(2001, 3, 10)

        async def website(oid, days, *, tagged, source=4, is_return=False):
            day = today - timedelta(days=days)
            async with pool.acquire() as conn:
                await conn.execute(
                    f"INSERT INTO silver.orders ({SILVER_COLS}) VALUES "
                    "($1,$2,1,100,$3,NULL,NULL,$4,$5,'retail',true,'x',false,NULL,NULL)",
                    oid, source, datetime(day.year, day.month, day.day, 12, tzinfo=KYIV),
                    day, is_return)
                await conn.execute(
                    "INSERT INTO silver.order_utm (order_id, utm_source, utm_campaign) VALUES ($1, 's', $2)",
                    oid, "c" if tagged else None)

        await website(IDS[20], 0, tagged=True)             # today — excluded
        await website(IDS[21], 1, tagged=True)             # current
        await website(IDS[22], 7, tagged=False)            # current edge
        await website(IDS[23], 8, tagged=True)             # baseline
        await website(IDS[24], 35, tagged=False)           # baseline edge
        await website(IDS[25], 36, tagged=True)            # excluded
        await website(IDS[26], 2, tagged=True, is_return=True)    # a return — excluded
        await website(IDS[27], 2, tagged=True, source=1)          # not the website — excluded
        await website(IDS[28], 3, tagged=False)            # utm_source but no campaign — untagged

        facts = await read_facts(pool=pool, today=today)
        assert facts.attribution == Attribution(orders=3, tagged=1, base_orders=2, base_tagged=1)


class TestBlindness:
    @pytest.mark.asyncio
    async def test_a_held_layer_lock_blinds_the_snapshot_quickly(self, pool, monkeypatch):
        """A lock of this test's own. Waiting on the module's lock would bind it
        to this test's event loop for good, and the next test to contend for it
        in another loop — test_order_utm_shipping's — would fail on it. Found
        reviewing #215."""
        from core import pg_silver, pg_warehouse_dq

        lock = asyncio.Lock()
        monkeypatch.setattr(pg_silver, "PG_LAYER_LOCK", lock)
        monkeypatch.setattr(pg_warehouse_dq, "LOCK_WAIT_S", 1)
        async with lock:
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


ROW_IDS = IDS[30:40]
RULE_IDS = IDS[40:60]
MANAGER = 979901
# The managers `test_every_branch_of_the_rule_...` classifies, one per branch of
# `silver_sales_type_case` that reads a table. MANAGER is the first of them.
WAS_RETAIL, UNCLASSIFIED_RETAIL, UNCLASSIFIED = 979902, 979903, 979904
TEST_MANAGERS = [MANAGER, WAS_RETAIL, UNCLASSIFIED_RETAIL, UNCLASSIFIED]


async def _landed(conn, oid, *, buyer=None, manager=None, days_ago=2, minutes_ago=180,
                  status=1, group=1, total=100, source=1, promocode=None, at=None):
    at = at or datetime.now(timezone.utc).replace(hour=12, minute=0, second=0,
                                                  microsecond=0) - timedelta(days=days_ago)
    await conn.execute(
        "INSERT INTO bronze.orders (id, source_id, status_id, status_group_id, grand_total,"
        " ordered_at, created_at, updated_at, buyer_id, manager_id, promocode, mirrored_at)"
        " VALUES ($1, $9, $2, $3, $4, $5, $5, $5, $6, $7, $10,"
        " now() - make_interval(mins => $8))",
        oid, status, group, total, at, buyer, manager, minutes_ago, source, promocode)


async def _touch(conn, oid, *, minutes_ago=None, at=None, **changes):
    """Change a landed order the way a mirror would: new values, new stamp —
    `minutes_ago` by Postgres' clock, or exactly `at`."""
    sets = [f"{column} = ${i}" for i, column in enumerate(changes, start=2)]
    n = len(changes) + 2
    if at is None:
        stamp, value = f"now() - make_interval(secs => ${n})", float(minutes_ago * 60)
    else:
        stamp, value = f"${n}::timestamptz", at
    await conn.execute(
        f"UPDATE bronze.orders SET {', '.join(sets + [f'mirrored_at = {stamp}'])} WHERE id = $1",
        oid, *changes.values(), value)


async def _journal(conn, *, minutes_ago=0, error=None, layer="warehouse"):
    """One derivation run as `_derive_pg_layers` records it, started
    `minutes_ago` by Postgres' clock. Returns its `started_at`."""
    from core.pg_derivation import record_run

    started = await conn.fetchval("SELECT now() - make_interval(secs => $1)",
                                  float(minutes_ago * 60))
    await record_run(conn, trigger="signal", started_at=started, ended_at=started,
                     error=error, layer=layer)
    return started


async def _rebuilt(conn, *, minutes_ago):
    """Date Silver's last rebuild `minutes_ago` by Postgres' clock: the
    watermark `rebuild_silver` would have stamped then. A test that runs the
    real rebuild and then moves the clock uses this, since the rebuild stamps
    the moment it actually ran."""
    await conn.execute(
        "UPDATE meta.mirror_state SET last_attempted_at = now() - make_interval(secs => $1),"
        " last_ok_at = now() - make_interval(secs => $1) WHERE table_name = 'silver.orders'",
        float(minutes_ago * 60))


@pytest_asyncio.fixture
async def rv(pool, monkeypatch):
    from core import pg_silver
    from core.pg_silver import rebuild_silver

    # The process loaded the Silver rule four hours ago — before anything the
    # tests date — so the rule's load time decides nothing unless a test moves it.
    monkeypatch.setattr(pg_silver, "RULE_LOADED_AT",
                        datetime.now(timezone.utc) - timedelta(hours=4))

    async def reset():
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM meta.derivation_runs")
            await conn.execute("DELETE FROM app.manager_classifications"
                               " WHERE manager_id = ANY($1::int[])", TEST_MANAGERS)
            await conn.execute("DELETE FROM bronze.managers WHERE id = ANY($1::int[])",
                               TEST_MANAGERS)

    await reset()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO bronze.managers (id, name, is_retail, mirrored_at)"
            " VALUES ($1, 'dn13', true, now() - interval '3 hours')", MANAGER)
        await conn.execute(
            "INSERT INTO app.manager_classifications (manager_id, is_retail, valid_from, mirrored_at)"
            " VALUES ($1, true, DATE '1970-01-01', now() - interval '3 hours')", MANAGER)
        a, b, c, d, e, f, g = ROW_IDS[:7]
        await _landed(conn, a)                                   # the plain row
        await _landed(conn, b, buyer=97001, days_ago=10)         # buyer 1's first
        await _landed(conn, c, buyer=97001, days_ago=2)          # buyer 1's second
        await _landed(conn, d, buyer=97002, days_ago=12)         # buyer 2's first
        await _landed(conn, e, buyer=97002, days_ago=5)          # buyer 2's second
        await _landed(conn, f, manager=MANAGER)                  # retail by classification
        await _landed(conn, g)
    await rebuild_silver(pool)
    async with pool.acquire() as conn:
        await _rebuilt(conn, minutes_ago=120)
        await _journal(conn, minutes_ago=120)
    yield pool
    await reset()


class TestTheRowValues:
    """Step 8b against a real Postgres: bronze landed three hours ago, Silver
    rebuilt by the real `rebuild_silver`, dated two hours ago in its watermark
    and by one error-free derivation — then a row broken in exactly one way
    per test.

    `meta.derivation_runs` and the one test manager are this class's to empty,
    the way `test_pg_own_derivation` empties the journal."""

    async def _read(self, pool):
        from core.pg_warehouse_dq import read_facts

        facts = await read_facts(pool=pool)
        return facts.silver_row_values, _judge(facts)

    @pytest.mark.asyncio
    async def test_the_rebuilds_own_silver_files_nothing(self, rv):
        values, judged = await self._read(rv)
        assert (values.reported, values.in_flight) == (0, 0)
        assert values.compared >= len(ROW_IDS[:7]) and values.rebuilt_at is not None
        assert "pg_silver_row_values" not in judged

    @pytest.mark.asyncio
    async def test_every_branch_of_the_rule_recomputes_to_what_the_rebuild_wrote(self, rv):
        """The recompute's pass 2 is a SELECT, the rebuild's an UPDATE
        (`silver_pass2_sql`): two statements of one rule, and nothing ties them
        but a fixture on which both run. So this one takes every branch either
        can take — each sales_type, a return by group and by the legacy status
        list, an inactive source first in a buyer's history, a return on the
        buyer's first day, no buyer, a promocode — and the real rebuild must
        recompute to itself with nothing reported and nothing in flight.

        Then the fixture proves its own coverage from what the rebuild wrote, so
        an edit that stops exercising a branch fails here rather than going
        quiet."""
        from core.duckdb_constants import B2B_MANAGER_ID, KNOWN_SALES_TYPES
        from core.pg_silver import rebuild_silver

        r = RULE_IDS
        noon = datetime.now(timezone.utc).replace(hour=12, minute=0, second=0, microsecond=0)
        async with rv.acquire() as conn:
            await conn.executemany(
                "INSERT INTO bronze.managers (id, name, is_retail, mirrored_at)"
                " VALUES ($1, 'dn13', $2, now() - interval '3 hours')",
                [(WAS_RETAIL, True), (UNCLASSIFIED_RETAIL, True), (UNCLASSIFIED, False)])
            await conn.execute(
                "INSERT INTO app.manager_classifications"
                " (manager_id, is_retail, valid_from, valid_to, mirrored_at)"
                " VALUES ($1, true, DATE '1970-01-01', $2, now() - interval '3 hours')",
                WAS_RETAIL, (noon - timedelta(days=5)).date())
            await _landed(conn, r[0], source=5, buyer=97101, days_ago=20)        # exhibition, first
            await _landed(conn, r[1], manager=B2B_MANAGER_ID, buyer=97101, days_ago=3)  # b2b
            await _landed(conn, r[2], source=3, buyer=97102, days_ago=30)        # Opencart, first
            await _landed(conn, r[3], buyer=97102, days_ago=4)                   # not new: the trap
            await _landed(conn, r[4], source=3, buyer=97103, days_ago=6)         # first, inactive
            await _landed(conn, r[5], buyer=97104, days_ago=15, status=19, group=6)  # a return, earliest
            await _landed(conn, r[6], buyer=97104, days_ago=9, status=19, group=6)   # return, first day
            await _landed(conn, r[7], buyer=97104, days_ago=9)                   # the real first
            await _landed(conn, r[8], buyer=97104, days_ago=2)
            await _landed(conn, r[9], source=2, buyer=97105, days_ago=7, status=19, group=None)
            await _landed(conn, r[10], manager=WAS_RETAIL, days_ago=8)          # inside the interval
            await _landed(conn, r[11], manager=WAS_RETAIL, days_ago=2)          # after it closed
            await _landed(conn, r[12], source=4, manager=UNCLASSIFIED_RETAIL)
            await _landed(conn, r[13], source=4, manager=UNCLASSIFIED)
            await _landed(conn, r[14], promocode="DN13", total=1234.56)          # no buyer
            await _landed(conn, r[15], buyer=97106,                              # a Kyiv midnight
                          at=noon - timedelta(days=3) + timedelta(hours=9, minutes=30))
            await _landed(conn, r[16], buyer=97106, days_ago=2)
        await rebuild_silver(rv)

        # The verdict first: a rebuild that drifts from the rule is what this
        # check exists to see, and it should say so before the coverage below
        # does.
        values, judged = await self._read(rv)
        assert (values.reported, values.in_flight) == (0, 0), (values.columns, values.sample)
        assert "pg_silver_row_values" not in judged

        async with rv.acquire() as conn:
            s = {row["id"]: row for row in await conn.fetch(
                f"SELECT {SILVER_COLS} FROM silver.orders WHERE id = ANY($1::int[])", r[:17])}
        assert len(s) == 17
        assert {row["sales_type"] for row in s.values()} == set(KNOWN_SALES_TYPES)
        assert (s[r[10]]["sales_type"], s[r[11]]["sales_type"]) == ("retail", "internal")
        assert (s[r[12]]["sales_type"], s[r[13]]["sales_type"]) == ("retail", "internal")
        # an inactive source is still a purchase: the buyer's later order is not new
        assert not s[r[2]]["is_active_source"] and not s[r[3]]["is_new_customer"]
        assert s[r[3]]["buyer_first_order_date"] == s[r[2]]["order_date"]
        # first and only purchase, on an inactive source: not new, for that reason alone
        assert not s[r[4]]["is_new_customer"]
        assert s[r[4]]["buyer_first_order_date"] == s[r[4]]["order_date"]
        # a return on the first day is not new; the purchase beside it is
        assert s[r[6]]["is_return"] and not s[r[6]]["is_new_customer"]
        assert s[r[6]]["order_date"] == s[r[7]]["buyer_first_order_date"] == s[r[7]]["order_date"]
        assert s[r[7]]["is_new_customer"] and not s[r[8]]["is_new_customer"]
        assert s[r[9]]["is_return"] and s[r[9]]["buyer_first_order_date"] is None
        assert s[r[14]]["buyer_id"] is None and s[r[14]]["promocode"] == "DN13"

    @pytest.mark.asyncio
    async def test_an_old_rows_grand_total_changed_under_a_later_rebuild_is_critical_with_the_id(self, rv):
        """Changed half an hour ago, and an error-free derivation began ten
        minutes ago: that snapshot held the change, and Silver still lacks it."""
        from core.data_quality import Severity

        a = ROW_IDS[0]
        async with rv.acquire() as conn:
            await _touch(conn, a, minutes_ago=30, grand_total=150)
            await _journal(conn, minutes_ago=10)
        values, judged = await self._read(rv)
        assert (values.reported, values.covered, values.abandoned) == (1, 1, 0)
        assert values.columns == (("grand_total", 1),)
        issue = judged["pg_silver_row_values"]
        assert issue.severity == Severity.CRITICAL and issue.sample_ids == (a,)

    @pytest.mark.asyncio
    async def test_a_row_mirrored_after_the_last_rebuild_warns_inside_the_repair_window(self, rv):
        from core.data_quality import Severity

        a = ROW_IDS[0]
        async with rv.acquire() as conn:
            await _touch(conn, a, minutes_ago=30, grand_total=150)
        values, judged = await self._read(rv)
        assert (values.reported, values.covered, values.abandoned) == (1, 0, 0)
        assert judged["pg_silver_row_values"].severity == Severity.WARN

    @pytest.mark.asyncio
    async def test_past_the_repair_window_with_no_rebuild_since_it_is_critical(self, rv):
        """The absolute bound: a derivation that stopped must not leave a
        changed row in the repair window for ever."""
        from core.data_quality import Severity

        a = ROW_IDS[0]
        async with rv.acquire() as conn:
            await _touch(conn, a, minutes_ago=100, grand_total=150)
        values, judged = await self._read(rv)
        assert (values.reported, values.covered, values.abandoned) == (1, 0, 1)
        assert judged["pg_silver_row_values"].severity == Severity.CRITICAL

    @pytest.mark.asyncio
    async def test_inside_the_settle_grace_it_is_in_flight_and_not_filed(self, rv):
        async with rv.acquire() as conn:
            await _touch(conn, ROW_IDS[0], minutes_ago=5, grand_total=150)
        values, judged = await self._read(rv)
        assert (values.reported, values.in_flight) == (0, 1)
        assert "pg_silver_row_values" not in judged

    @pytest.mark.asyncio
    async def test_a_change_inside_the_margin_before_a_rebuild_began_is_not_covered(self, rv):
        """Ten seconds before a derivation's `started_at` may still have
        committed after its snapshot; thirty is the margin."""
        from core.data_quality import Severity

        async with rv.acquire() as conn:
            started = await _journal(conn, minutes_ago=30)
            await _touch(conn, ROW_IDS[0], at=started - timedelta(seconds=10), grand_total=150)
        values, judged = await self._read(rv)
        assert (values.covered, values.reported) == (0, 1)
        assert judged["pg_silver_row_values"].severity == Severity.WARN

    @pytest.mark.asyncio
    async def test_a_failed_derivation_covers_nothing(self, rv):
        """Failed before Silver committed: the journal says error and the
        watermark did not move, so nothing was rebuilt."""
        from core.data_quality import Severity

        async with rv.acquire() as conn:
            await _touch(conn, ROW_IDS[0], minutes_ago=30, grand_total=150)
            await _journal(conn, minutes_ago=10, error="silver.orders: RuntimeError: boom")
            await _journal(conn, minutes_ago=10, layer="not-the-warehouse")
        values, judged = await self._read(rv)
        assert values.covered == 0
        assert judged["pg_silver_row_values"].severity == Severity.WARN

    @pytest.mark.asyncio
    async def test_under_piggyback_a_faulty_rebuild_is_covered_by_its_watermark(self, rv, monkeypatch):
        """`KS_PG_DERIVE=piggyback`, production's mode, writes no journal row:
        `_rebuild_pg_layers` calls `rebuild_silver` on DuckDB's tick, and
        Silver's watermark is all a rebuild leaves. One that ran seconds ago
        and still wrote the wrong value is covered — a fault in the rebuild —
        and never "no rebuild is coming", whose lever would only run it again.
        Found reviewing DN-13: dated by the journal alone, this read as
        abandoned."""
        from core import pg_silver
        from core.data_quality import Severity

        x = RULE_IDS[0]
        async with rv.acquire() as conn:
            await conn.execute("DELETE FROM meta.derivation_runs")
            await _landed(conn, x, source=3, buyer=97301, days_ago=6)   # first, inactive source
        real = pg_silver.pass2_sql
        monkeypatch.setattr(pg_silver, "pass2_sql",
                            lambda: real().replace("AND s.is_active_source", "", 1))
        await pg_silver.rebuild_silver(rv)                               # the piggyback rebuild

        values, judged = await self._read(rv)
        assert (values.reported, values.covered, values.abandoned) == (1, 1, 0)
        assert values.columns == (("is_new_customer", 1),)
        issue = judged["pg_silver_row_values"]
        assert issue.severity == Severity.CRITICAL and issue.sample_ids == (x,)
        assert "another would repeat it" in issue.description
        assert "no rebuild is coming" not in issue.description

    @pytest.mark.asyncio
    async def test_a_rule_changed_by_a_deploy_is_owed_a_rebuild_not_blamed_on_one(self, rv, monkeypatch):
        """Silver was rebuilt two hours ago by the code before a deploy, and the
        deploy dropped Instagram from the revenue sources. Every row the new
        rule moves differs from the recompute, nothing moved in bronze, and the
        rebuild that would "cover" them ran the old rule — so they are owed this
        process's first rebuild, not a fault in it. Found reviewing DN-13: they
        read as covered CRITICAL, "another would repeat it"."""
        from core import duckdb_store, pg_silver
        from core.data_quality import Severity

        monkeypatch.setattr(duckdb_store, "REVENUE_SOURCE_IDS",
                            tuple(s for s in duckdb_store.REVENUE_SOURCE_IDS if s != 1))
        now = datetime.now(timezone.utc)

        # Loaded two minutes ago: inside the settle grace, in flight, not filed.
        monkeypatch.setattr(pg_silver, "RULE_LOADED_AT", now - timedelta(minutes=2))
        values, judged = await self._read(rv)
        assert (values.reported, values.in_flight) == (0, 7)       # every Instagram row
        assert "pg_silver_row_values" not in judged

        # Half an hour and still no rebuild: WARN, saying what dated them.
        monkeypatch.setattr(pg_silver, "RULE_LOADED_AT", now - timedelta(minutes=30))
        values, judged = await self._read(rv)
        assert (values.reported, values.covered, values.dated_by_rule) == (7, 0, 7)
        issue = judged["pg_silver_row_values"]
        assert issue.severity == Severity.WARN
        assert "loading the Silver rule" in issue.description

        # This process's first rebuild runs the new rule, and nothing is left.
        await pg_silver.rebuild_silver(rv)
        values, judged = await self._read(rv)
        assert (values.reported, values.in_flight) == (0, 0)
        assert "pg_silver_row_values" not in judged

    @pytest.mark.asyncio
    async def test_a_flipped_is_new_customer_is_caught(self, rv):
        """Bronze untouched, Silver's pass-2 column wrong: only a recompute of
        pass 2 from landing can see it, since the real pass 2 would take its
        baseline from the Silver that is wrong."""
        from core.data_quality import Severity

        b = ROW_IDS[1]
        async with rv.acquire() as conn:
            await conn.execute(
                "UPDATE silver.orders SET is_new_customer = NOT is_new_customer WHERE id = $1", b)
        values, judged = await self._read(rv)
        assert values.columns == (("is_new_customer", 1),) and values.covered == 1
        issue = judged["pg_silver_row_values"]
        assert issue.severity == Severity.CRITICAL and issue.sample_ids == (b,)

    @pytest.mark.asyncio
    async def test_a_cancelled_first_order_leaves_the_buyers_later_orders_in_flight(self, rv):
        """Buyer 1's first order is cancelled two minutes ago. Their second
        becomes their first, so `is_new_customer` and `buyer_first_order_date`
        both move; their third moves `buyer_first_order_date` alone. Both own
        stamps are three hours old, before the last rebuild: judged by them,
        both would be covered CRITICALs a rebuild away from clean. The third
        is the trigger on `buyer_first_order_date` alone, which the round-3
        review found no test exercised."""
        from core.pg_silver import rebuild_silver

        b, third = ROW_IDS[1], ROW_IDS[7]
        async with rv.acquire() as conn:
            await _landed(conn, third, buyer=97001, days_ago=1)
        await rebuild_silver(rv)
        async with rv.acquire() as conn:
            await _rebuilt(conn, minutes_ago=60)
            await _touch(conn, b, minutes_ago=2, status_id=19, status_group_id=6)
        values, judged = await self._read(rv)
        assert (values.reported, values.in_flight) == (0, 3)
        assert "pg_silver_row_values" not in judged

    @pytest.mark.asyncio
    async def test_the_sample_leads_with_the_rows_that_page(self, rv):
        """One row a rebuild covered and one still inside the repair window,
        the covered one with the lower id. It comes first: the ids a reader
        opens are the ones the CRITICAL is about, and waiting rows cannot push
        it out of the ten."""
        from core.data_quality import Severity

        covered, waiting = ROW_IDS[0], ROW_IDS[6]
        async with rv.acquire() as conn:
            await _touch(conn, covered, minutes_ago=40, grand_total=150)
            await _rebuilt(conn, minutes_ago=25)
            await _touch(conn, waiting, minutes_ago=22, grand_total=150)
        values, judged = await self._read(rv)
        assert (values.reported, values.covered) == (2, 1)
        issue = judged["pg_silver_row_values"]
        assert issue.severity == Severity.CRITICAL and issue.sample_ids == (covered, waiting)

    @pytest.mark.asyncio
    async def test_half_a_hryvnia_is_a_difference(self, rv):
        """`grand_total`'s tolerance is DuckDB's 0.01 — an allowance for float
        arithmetic, not a threshold of materiality."""
        from decimal import Decimal

        a = ROW_IDS[0]
        async with rv.acquire() as conn:
            await _touch(conn, a, minutes_ago=30, grand_total=Decimal("100.50"))
        values, _ = await self._read(rv)
        assert (values.reported, values.columns) == (1, (("grand_total", 1),))

    @pytest.mark.asyncio
    async def test_an_order_moved_to_another_buyer_is_a_recent_change_for_the_old_one(self, rv):
        """Buyer 2's first order moves to a new buyer: in bronze buyer 2 no
        longer holds it, so only the stored Silver row says it was theirs."""
        d, e = ROW_IDS[3], ROW_IDS[4]
        async with rv.acquire() as conn:
            await _touch(conn, d, minutes_ago=2, buyer_id=97003)
        values, judged = await self._read(rv)
        assert (values.reported, values.in_flight) == (0, 2)
        assert "pg_silver_row_values" not in judged

    @pytest.mark.asyncio
    async def test_a_reclassified_manager_is_in_flight_until_a_rebuild_begins_after_it(self, rv):
        from core.data_quality import Severity

        f = ROW_IDS[5]
        async with rv.acquire() as conn:
            for table, key in (("app.manager_classifications", "manager_id"), ("bronze.managers", "id")):
                await conn.execute(
                    f"UPDATE {table} SET is_retail = false, mirrored_at = now() - interval '1 minute'"
                    f" WHERE {key} = $1", MANAGER)
        values, judged = await self._read(rv)
        assert (values.reported, values.in_flight) == (0, 1)

        async with rv.acquire() as conn:
            await _journal(conn, minutes_ago=0)
        values, judged = await self._read(rv)
        assert (values.covered, values.columns) == (1, (("sales_type", 1),))
        issue = judged["pg_silver_row_values"]
        assert issue.severity == Severity.CRITICAL and issue.sample_ids == (f,)

    @pytest.mark.asyncio
    async def test_a_budget_spent_on_the_recompute_blinds_it_alone(self, rv, monkeypatch):
        """Read last, so the three cheap groups keep their verdicts."""
        from core import pg_warehouse_dq

        monkeypatch.setattr(pg_warehouse_dq, "HOLD_BUDGET_S", 2)
        monkeypatch.setattr(pg_warehouse_dq, "_row_values_sql", lambda: (
            "SELECT pg_sleep(10), $1::int, $2::int, $3::interval, $4::text, $5::text,"
            " $6::timestamptz"))
        facts = await asyncio.wait_for(pg_warehouse_dq.read_facts(pool=rv), timeout=30)
        assert isinstance(facts.silver_row_values, pg_warehouse_dq.Unwatched)
        assert "hold budget" in facts.silver_row_values.reason
        for group in ("silver_arc", "attribution", "line_items"):
            assert not isinstance(getattr(facts, group), pg_warehouse_dq.Unwatched), group
        held = []
        issues = pg_warehouse_dq.check_pg_warehouse(facts, held_out=held)
        names = {i.check_name for i in issues}
        assert "pg_silver_row_values_unwatched" in names and "pg_warehouse_unwatched" not in names
        assert held == ["pg_silver_row_values"]

    @pytest.mark.asyncio
    async def test_the_snapshot_runs_without_jit(self, rv, monkeypatch):
        from core import pg_warehouse_dq

        seen = {}

        async def spy(conn, grace, page_after):
            seen["jit"] = await conn.fetchval("SELECT current_setting('jit')")
            return await real(conn, grace, page_after)

        real = pg_warehouse_dq._read_row_values
        monkeypatch.setattr(pg_warehouse_dq, "_read_row_values", spy)
        await pg_warehouse_dq.read_facts(pool=rv)
        async with rv.acquire() as conn:
            default = await conn.fetchval("SELECT current_setting('jit')")
        assert seen["jit"] == "off", f"server default is {default}"


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
        assert not unverified & pg_warehouse_dq.all_conditions()   # stood down, not blind

    @pytest.mark.asyncio
    async def test_a_flag_not_understood_is_filed_and_holds_everything(self, pool, job, monkeypatch):
        from core import pg_warehouse_dq

        scheduler, store, _sent = job
        monkeypatch.setenv("KS_DQ_PG_WAREHOUSE", "yes")
        result = await scheduler._run_dq_integrity()
        assert "pg_warehouse_dq_flag_invalid" in await self._issues(store, result["run_id"])
        unverified = set(scheduler._resolve_dq_layer.await_args.kwargs["unverified"])
        assert pg_warehouse_dq.all_conditions() <= unverified

    @pytest.mark.asyncio
    async def test_the_duckdb_half_failing_still_pages_a_postgres_critical_and_resolves_the_twins(
        self, pool, job, monkeypatch,
    ):
        scheduler, store, sent = job
        monkeypatch.setenv("KS_DQ_PG_WAREHOUSE", "on")
        async with pool.acquire() as conn:
            await _bronze(conn, IDS[0], total=555, minutes_ago=150)
        resolve = AsyncMock(return_value=0)
        with patch("core.data_quality.check_internal_integrity", side_effect=RuntimeError("duckdb")), \
             patch("core.alerting.resolve_group", resolve):
            result = await scheduler._run_dq_integrity()

        assert result["error"]
        assert sent.await_count == 1
        assert sent.await_args.kwargs["conditions"] == ["pg_silver_missing_rows"]
        (call,) = resolve.await_args_list
        assert call.kwargs["only_prefix"] == "pg_"
        assert "pg_silver_missing_rows" in call.kwargs["still_firing"]

    @pytest.mark.asyncio
    async def test_the_duckdb_half_failing_with_only_postgres_warnings_pages_nobody(
        self, pool, job, monkeypatch,
    ):
        scheduler, store, sent = job
        monkeypatch.setenv("KS_DQ_PG_WAREHOUSE", "on")
        async with pool.acquire() as conn:
            await _bronze(conn, IDS[0], total=555, minutes_ago=30)   # inside the repair window
        with patch("core.data_quality.check_internal_integrity", side_effect=RuntimeError("duckdb")), \
             patch("core.alerting.resolve_group", AsyncMock(return_value=0)):
            await scheduler._run_dq_integrity()
        assert sent.await_count == 0

    @pytest.mark.asyncio
    async def test_a_duckdb_line_items_check_that_raised_is_stood_in_for_not_compared(
        self, pool, job, monkeypatch,
    ):
        """DuckDB's headline guard raised: the twin files the standing finding
        and does not report DuckDB's count as 0 against its own."""
        from core import data_quality

        scheduler, store, _sent = job
        monkeypatch.setenv("KS_DQ_PG_WAREHOUSE", "on")
        async with pool.acquire() as conn:
            await _bronze(conn, IDS[70], total=0, minutes_ago=150)
            await _silver(conn, IDS[70], total=0)
            await conn.execute(
                "INSERT INTO bronze.order_products (id, order_id, product_id, name, quantity, price_sold)"
                " VALUES ($1, $2, NULL, 'x', 2, 150)", IDS[70] * 1000 + 1, IDS[70])

        real = data_quality.check_internal_integrity

        def raising_headline(conn, **kw):
            out = real(conn, **kw)
            kw["raised_out"].append("headline_vs_line_items")
            return [i for i in out if i.check_name != "headline_vs_line_items"]

        with patch("core.data_quality.check_internal_integrity", side_effect=raising_headline):
            result = await scheduler._run_dq_integrity()
        issues = await self._issues(store, result["run_id"])
        assert "pg_headline_vs_line_items" in issues
        assert "pg_line_items_disagree" not in issues

    @pytest.mark.asyncio
    async def test_on_a_stale_postgres_silver_row_is_journalled_and_pages(
        self, rv, job, monkeypatch,
    ):
        """Step 8b through the job: a row a later rebuild covered and still did
        not carry is a CRITICAL in the run, and it pages."""
        scheduler, store, sent = job
        monkeypatch.setenv("KS_DQ_PG_WAREHOUSE", "on")
        async with rv.acquire() as conn:
            await _touch(conn, ROW_IDS[0], minutes_ago=30, grand_total=150)
            await _journal(conn, minutes_ago=10)

        result = await scheduler._run_dq_integrity()

        issues = await self._issues(store, result["run_id"])
        assert issues["pg_silver_row_values"]["severity"] == "CRITICAL"
        assert "pg_silver_row_values" in sent.await_args.kwargs["conditions"]
        unverified = scheduler._resolve_dq_layer.await_args.kwargs["unverified"]
        assert "pg_silver_row_values" not in unverified                 # it looked

    @pytest.mark.asyncio
    async def test_off_a_stale_postgres_silver_row_is_not_read(self, rv, job, monkeypatch):
        """Flag off: the recompute is never run, not run and discarded."""
        from core import pg_warehouse_dq

        scheduler, store, sent = job
        monkeypatch.delenv("KS_DQ_PG_WAREHOUSE", raising=False)
        async with rv.acquire() as conn:
            await _touch(conn, ROW_IDS[0], minutes_ago=30, grand_total=150)
            await _journal(conn, minutes_ago=10)

        with patch.object(pg_warehouse_dq, "read_facts",
                          AsyncMock(wraps=pg_warehouse_dq.read_facts)) as read, \
             patch.object(pg_warehouse_dq, "_read_row_values",
                          AsyncMock(wraps=pg_warehouse_dq._read_row_values)) as recompute:
            result = await scheduler._run_dq_integrity()

        assert read.await_count == 0 and recompute.await_count == 0
        assert "pg_silver_row_values" not in await self._issues(store, result["run_id"])
        assert sent.await_count == 0
