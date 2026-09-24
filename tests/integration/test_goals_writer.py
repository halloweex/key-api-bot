"""Chain 7a: the revenue goals a human types, against a real Postgres.

DN-25. `POST /api/goals` wrote DuckDB while `GET /api/goals` read Postgres
under `KS_READ_GOALS`, and `app.revenue_goals` was an hourly full-replace
copy in between — so a typed goal was invisible for up to an hour. Under
`KS_WRITE_GOALS=postgres` the write lands where the page reads. What that has
to survive, and what has to stay true around it, is what these tests pin:

- the typed goal is on the page at once and survives the hourly replace,
  which stands down for its table — and a control proves the replace would
  otherwise have wiped it;
- the first write latches the chain, so putting the flag back without the
  copy-back changes nothing (OD-19 (a)): the writes stay in Postgres, the
  replace stays stood down, and the disagreement is published;
- the copy-back carries the goals into DuckDB, compares them at zero, releases
  the latch, and the hourly copy resumes shipping exactly what came back.

Everything goes through the repository methods — the path both the POST and
the DELETE (reset to auto) take — because the routing lives there.
"""
from __future__ import annotations

import os
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
import pytest_asyncio

from core import chain_latch, pg_goals_write
from core.chain_transfer import CopyBackRefused, copy_back, handover_check

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

GOALS = "app.revenue_goals"
CHAIN = pg_goals_write.CHAIN


async def _clean(pool):
    async with pool.acquire() as conn:
        await conn.execute(f"DELETE FROM {GOALS}")
        # The owner rows the latch writes on its first Postgres write. Left
        # behind, they read in the next module as a chain owning a table with
        # no local marker — a real CRITICAL, manufactured by a fixture.
        await conn.execute("DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%'")
        await conn.execute("DELETE FROM meta.mirror_state WHERE table_name = $1", GOALS)


@pytest_asyncio.fixture
async def stores(tmp_path, monkeypatch):
    """A real DuckDB with the whole schema and an empty goals table, a live
    Postgres, and the page reading Postgres — the production read mode this
    chain presupposes (see `core/pg_goals_write.py`)."""
    from core.duckdb_store import DuckDBStore

    for env in ("KS_WRITE_INVENTORY", "KS_WRITE_EXPENSES", "KS_WRITE_GOALS"):
        monkeypatch.delenv(env, raising=False)
    monkeypatch.setenv("KS_PG_DSN", DSN)
    monkeypatch.setenv("KS_READ_GOALS", "postgres")
    store = DuckDBStore(db_path=tmp_path / "goals.duckdb")
    await store.connect()
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    await _clean(pool)
    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
         patch("core.pg.require_revision", new=AsyncMock()):
        yield store, pool, monkeypatch
    await _clean(pool)
    await pool.close()
    await store.close()


async def _pg_goals(pool):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"SELECT period_type, goal_amount, is_custom, calculated_goal, "
            f"growth_factor, updated_at FROM {GOALS} ORDER BY period_type")
    return {r["period_type"]: tuple(r) for r in rows}


async def _duck_goals(store):
    async with store.connection() as conn:
        rows = conn.execute(
            "SELECT period_type, goal_amount, is_custom, calculated_goal, "
            "growth_factor, updated_at FROM revenue_goals ORDER BY period_type"
        ).fetchall()
    return {r[0]: tuple(r) for r in rows}


class TestUnderTheFlag:
    @pytest.mark.asyncio
    async def test_the_typed_goal_is_on_the_page_at_once_and_not_in_duckdb(self, stores):
        store, pool, env = stores
        env.setenv("KS_WRITE_GOALS", "postgres")

        out = await store.set_goal("monthly", 1_234_500.0)

        # The caller's shape is the one the DuckDB branch returns.
        assert set(out) == {"periodType", "amount", "isCustom", "calculatedGoal",
                            "growthFactor"}
        assert out["amount"] == 1_234_500.0 and out["isCustom"] is True
        row = (await _pg_goals(pool))["monthly"]
        assert float(row[1]) == 1_234_500.0 and row[2] is True
        # Supplied by the writer: the Postgres column has no default, and it
        # is the clock the copy-back's handover orders versions by.
        assert row[5] is not None
        assert await _duck_goals(store) == {}, "the write reached DuckDB"

        # GET /api/goals is `get_goals`, reading Postgres under KS_READ_GOALS:
        # no hour of invisibility any more.
        goals = await store.get_goals()
        assert goals["monthly"]["amount"] == 1_234_500.0
        assert goals["monthly"]["isCustom"] is True

    @pytest.mark.asyncio
    async def test_it_latches_and_claims_inside_the_write(self, stores):
        store, pool, env = stores
        env.setenv("KS_WRITE_GOALS", "postgres")

        await store.set_goal("daily", 60_000.0)

        assert chain_latch.latched(CHAIN)
        owners = await chain_latch.read_owners(pool)
        assert owners == {GOALS: chain_latch.latched_at(CHAIN)}

    @pytest.mark.asyncio
    async def test_a_second_write_to_one_period_rewrites_it_in_place(self, stores):
        store, pool, env = stores
        env.setenv("KS_WRITE_GOALS", "postgres")

        await store.set_goal("weekly", 300_000.0)
        first = (await _pg_goals(pool))["weekly"]
        await store.set_goal("weekly", 350_000.0, growth_factor=1.2)

        goals = await _pg_goals(pool)
        assert list(goals) == ["weekly"]
        assert float(goals["weekly"][1]) == 350_000.0
        assert float(goals["weekly"][4]) == 1.2
        assert goals["weekly"][5] >= first[5]

    @pytest.mark.asyncio
    async def test_reset_to_auto_goes_through_the_same_writer(self, stores):
        """`DELETE /api/goals/{period}` never names `set_goal` in the route —
        it reaches it through `reset_goal_to_auto`, which is why the routing
        lives in the repository."""
        store, pool, env = stores
        env.setenv("KS_WRITE_GOALS", "postgres")

        await store.set_goal("daily", 99_000.0)
        out = await store.reset_goal_to_auto("daily")

        row = (await _pg_goals(pool))["daily"]
        assert row[2] is False and float(row[1]) == out["amount"]
        assert await _duck_goals(store) == {}

    @pytest.mark.asyncio
    async def test_an_unknown_value_refuses_the_write_in_both_stores(self, stores):
        store, pool, env = stores
        env.setenv("KS_WRITE_GOALS", "postgrse")

        with pytest.raises(RuntimeError, match="KS_WRITE_GOALS"):
            await store.set_goal("monthly", 1_000_000.0)
        assert await _pg_goals(pool) == {} and await _duck_goals(store) == {}
        assert not chain_latch.latched(CHAIN)


class TestTheHourlyReplaceCannotRollItBack:
    """The most dangerous state for a write chain, proven end to end, and the
    control that gives the survival its meaning — `replicate_operational`
    never raises, so "the goal survived" alone could pass for a run that
    quietly did nothing."""

    @pytest.mark.asyncio
    async def test_under_the_flag_the_typed_goal_survives(self, stores):
        from core.pg_operational import replicate_operational

        store, pool, env = stores
        env.setenv("KS_WRITE_GOALS", "postgres")
        await store.set_goal("monthly", 2_000_000.0)

        result = await replicate_operational(store)

        assert "error" not in result, result
        assert GOALS in result.get("stood_down", []), result
        assert GOALS not in result["replaced"], result
        assert float((await _pg_goals(pool))["monthly"][1]) == 2_000_000.0
        goals = await store.get_goals()
        assert goals["monthly"]["amount"] == 2_000_000.0

    @pytest.mark.asyncio
    async def test_control_released_and_unflagged_the_same_replace_wipes_it(self, stores):
        from core.pg_operational import replicate_operational

        store, pool, env = stores
        env.setenv("KS_WRITE_GOALS", "postgres")
        await store.set_goal("monthly", 2_000_000.0)
        env.delenv("KS_WRITE_GOALS")
        chain_latch.release(CHAIN)
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%'")

        result = await replicate_operational(store)

        assert "error" not in result, result
        assert GOALS not in result.get("stood_down", [])
        assert result["replaced"][GOALS] == 0
        assert await _pg_goals(pool) == {}, "the replace out of an empty DuckDB did not run"


class TestTheFlagGoesBackWithoutTheCopyBack:
    """OD-19 (a): once a goal has been written to Postgres, `KS_WRITE_GOALS`
    back to `duckdb` is a disagreement, not a rollback."""

    @pytest.mark.asyncio
    async def test_the_writes_stay_in_postgres_and_the_replace_stays_down(self, stores):
        from core.pg_operational import replicate_operational
        from web.routes.api.health import _write_chains

        store, pool, env = stores
        env.setenv("KS_WRITE_GOALS", "postgres")
        await store.set_goal("monthly", 1_500_000.0)
        env.setenv("KS_WRITE_GOALS", "duckdb")          # the flag goes back

        await store.set_goal("weekly", 400_000.0)

        goals = await _pg_goals(pool)
        assert set(goals) == {"monthly", "weekly"}, "the flag moved the write back"
        assert await _duck_goals(store) == {}, "a second writer started in DuckDB"
        assert (await store.get_goals())["weekly"]["amount"] == 400_000.0

        result = await replicate_operational(store)
        assert "error" not in result, result
        assert GOALS in result["stood_down"]
        assert list(result["chain_flag_mismatch"]) == [CHAIN]
        assert set(await _pg_goals(pool)) == {"monthly", "weekly"}
        async with pool.acquire() as conn:
            state = await conn.fetchrow(
                "SELECT failures_since_ok, last_error FROM meta.mirror_state "
                "WHERE table_name = $1", GOALS)
        assert state["failures_since_ok"] >= 1
        assert "owned by Postgres since" in state["last_error"]

        block = _write_chains()[CHAIN]
        assert block["mode"] == "postgres" and block["latched"] is True
        assert block["mismatch"] is True


class TestTheCopyBack:
    """DN-08's machinery, derived for this chain rather than written for it."""

    @pytest.mark.asyncio
    async def test_the_goals_arrive_compare_at_zero_and_the_copy_resumes(self, stores):
        from core.pg_operational import replicate_operational

        store, pool, env = stores
        env.setenv("KS_WRITE_GOALS", "postgres")
        await store.set_goal("monthly", 1_750_000.0)
        await store.set_goal("daily", 55_000.0)
        typed = await _pg_goals(pool)

        result = await copy_back(store, pg_goals_write, dry_run=False)

        assert result["findings"] == [], result["findings"]
        assert result["released"] is True
        assert result["rows"] == {GOALS: 2}
        assert result["sequences"] == {} and result["sync_keys"] == {}
        assert await _duck_goals(store) == typed

        # Both copies of the latch, and only on a clean comparison.
        assert not chain_latch.latched(CHAIN)
        assert await chain_latch.read_owners(pool) == {}

        # The flag decides again, so the hourly copy resumes — and writes
        # exactly what came back, which is the whole claim.
        env.setenv("KS_WRITE_GOALS", "duckdb")
        shipped = await replicate_operational(store)
        assert "error" not in shipped, shipped
        assert shipped["replaced"][GOALS] == 2
        assert GOALS not in shipped.get("stood_down", [])
        assert await _pg_goals(pool) == typed

        # And the next goal is DuckDB's to write again.
        await store.set_goal("weekly", 320_000.0)
        assert set(await _duck_goals(store)) == {"daily", "monthly", "weekly"}
        assert "weekly" not in await _pg_goals(pool)
        assert not chain_latch.latched(CHAIN)

    @pytest.mark.asyncio
    async def test_a_dry_run_writes_nothing_and_releases_nothing(self, stores):
        store, pool, env = stores
        env.setenv("KS_WRITE_GOALS", "postgres")
        await store.set_goal("monthly", 1_750_000.0)

        result = await copy_back(store, pg_goals_write)

        assert result["executed"] is False and result["rows"] == {GOALS: 1}
        assert await _duck_goals(store) == {}
        assert chain_latch.latched(CHAIN)

    @pytest.mark.asyncio
    async def test_an_unlatched_chain_is_refused(self, stores):
        store, _pool, env = stores
        env.setenv("KS_WRITE_GOALS", "postgres")        # the flag is not the latch

        with pytest.raises(CopyBackRefused, match="not latched"):
            await copy_back(store, pg_goals_write, dry_run=False)


class TestTheHandoverBeforeAFlip:
    """`--handover` is the gate: a goal typed in DuckDB after the last hourly
    shipment would be stranded by the flip, because the replace stands down
    the moment the chain routes to Postgres and this table has no backfill."""

    @pytest.mark.asyncio
    async def test_a_goal_only_duckdb_holds_fails_it(self, stores):
        store, _pool, _env = stores
        await store.set_goal("monthly", 1_100_000.0)     # DuckDB, flag off

        issues = await handover_check(store, pg_goals_write)

        assert [(i.check_name, i.severity.value, i.table_name, i.count)
                for i in issues] == [
            ("handover_rows_missing", "CRITICAL", GOALS, 1)]
        # No sample: `IntegrityIssue.sample_ids` carries ints and this table is
        # keyed on `period_type`, so the finding names the table and the count
        # and the operator reads the three rows. Pinned so that a change to
        # `_sample` that starts rendering text keys is noticed here.
        assert issues[0].sample_ids == ()

    @pytest.mark.asyncio
    async def test_once_shipped_it_passes(self, stores):
        from core.pg_operational import replicate_operational

        store, _pool, _env = stores
        await store.set_goal("monthly", 1_100_000.0)
        shipped = await replicate_operational(store)
        assert shipped["replaced"][GOALS] == 1

        assert await handover_check(store, pg_goals_write) == []
