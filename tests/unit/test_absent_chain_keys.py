"""What a chain-3, -6 or -6a sync key absent from `meta.chain_watermarks` does.

Step 14's S14-09 took `CHAIN_WATERMARK_INHERITS_DUCKDB` off chains 6 and 6a
and kept it on chain 3. Production held every one of these keys on 2026-10-29
(the plan's H6), so nothing reads the stand-in today; what is pinned here is
what a key that went missing would cost — a row deleted by hand, a restore —
because for `last_sync_orders` the answer "absent means an hour ago" was a
real defect once: the order step's window starts 24 h before the watermark,
so a None opened a 25-hour window and every order KeyCRM updated before it
was never fetched by the incremental sync again (the chain-3 review).

- **Chain 3 keeps the stand-in**: `last_sync_orders` and the search index's
  `last_sync_meilisearch_pg`, absent from Postgres, are answered from DuckDB's
  `sync_metadata` as Postgres copied it (`app.sync_metadata`,
  `core.pg_frozen_metadata`), so the window starts where the sync stopped and
  the index goes on incrementally. Present, Postgres wins.
- **Chain 6's `last_sync_products`** is read by one path, the hourly products
  step, and absent it is simply due: the whole catalogue is synced at once and
  the key written. Nothing is lost by a missing stand-in.
- **`last_sync_categories` and `last_sync_expense_types`** are read back by no
  sync path at all; only the freshness check judges them, and absent it says
  "never synced" — the same check, at the same severity, as the stale stamp
  the stand-in would have offered.
"""
from __future__ import annotations

import ast
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from core import pg_catalogue_write, pg_expense_types_write, pg_orders_write, write_chains

ROOT = Path(__file__).resolve().parents[2]
UTC = timezone.utc
DSN = os.getenv("KS_PG_DSN")


def test_only_chain_3_declares_the_stand_in():
    """Kills: the declaration put back on 6 or 6a (S14-09 took it off), or
    taken off chain 3, whose absent key opens the 25-hour window."""
    declaring = {write_chains.chain_name(c) for c in write_chains.WRITE_CHAINS
                 if getattr(c, "CHAIN_WATERMARK_INHERITS_DUCKDB", False)}
    assert declaring == {pg_orders_write.CHAIN}
    assert not hasattr(pg_catalogue_write, "CHAIN_WATERMARK_INHERITS_DUCKDB")
    assert not hasattr(pg_expense_types_write, "CHAIN_WATERMARK_INHERITS_DUCKDB")


@pytest.fixture
def off_store(monkeypatch, tmp_path):
    """The store as web holds it, its DuckDB connection a trap."""
    from core.duckdb_store import DuckDBStore

    store = DuckDBStore()
    monkeypatch.setattr(store, "connection", MagicMock(
        side_effect=AssertionError("the getter opened DuckDB")))
    return store


@pytest.fixture
def postgres_holds(monkeypatch):
    """`meta.chain_watermarks` and `app.sync_metadata`, as dicts."""
    from core import pg_chain_watermarks, pg_frozen_metadata

    live, frozen = {}, {}

    async def get_value(key):
        raw = live.get(key)
        return datetime.fromisoformat(raw) if raw else None

    async def frozen_stamp(key):
        raw = frozen.get(key)
        return datetime.fromisoformat(raw) if raw else None

    monkeypatch.setattr(pg_chain_watermarks, "get_value", get_value)
    monkeypatch.setattr(pg_frozen_metadata, "frozen_stamp", frozen_stamp)
    return live, frozen


class TestChain3KeepsTheStandIn:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("key", ["orders", "meilisearch_pg"])
    async def test_an_absent_key_is_the_copy_s_stamp_never_none(
            self, off_store, postgres_holds, key):
        live, frozen = postgres_holds
        stamp = datetime(2026, 10, 15, 8, 0, 44, tzinfo=UTC)
        frozen[f"last_sync_{key}"] = stamp.isoformat()
        assert await off_store.get_last_sync_time(key) == stamp

    @pytest.mark.asyncio
    async def test_a_present_key_is_postgres_s(self, off_store, postgres_holds):
        live, frozen = postgres_holds
        frozen["last_sync_orders"] = "2026-10-15T08:00:44+00:00"
        live["last_sync_orders"] = "2026-10-29T13:21:02+00:00"
        assert await off_store.get_last_sync_time("orders") == datetime(
            2026, 10, 29, 13, 21, 2, tzinfo=UTC)

    @pytest.mark.asyncio
    async def test_the_window_starts_where_the_sync_stopped(
            self, off_store, postgres_holds, monkeypatch):
        """The order step asks KeyCRM from the copy's stamp less 24 h, not
        from 25 h back. Kills: the stand-in dropped from the getter, or the
        declaration from chain 3 — the window then starts an hour ago less
        24 h, and three days of updated orders are never fetched."""
        from core.sync_service import DEFAULT_TZ, SyncService

        _live, frozen = postgres_holds
        stopped = datetime.now(UTC).replace(microsecond=0) - timedelta(days=3)
        frozen["last_sync_orders"] = stopped.isoformat()
        asked = []

        async def fetch(self, client, start, end, *args, **kwargs):
            asked.append(start)
            return []

        monkeypatch.setattr(SyncService, "_fetch_orders_with_date_filter", fetch)
        await SyncService(off_store)._orders_step(MagicMock(), {"orders": 0, "expenses": 0})
        expected = (stopped - timedelta(hours=24)).astimezone(DEFAULT_TZ)
        assert asked == [expected.strftime("%Y-%m-%d %H:%M:%S")]


class TestChain6sProductsKeyIsSimplyDue:
    @pytest.mark.asyncio
    async def test_an_absent_key_reads_none(self, off_store, postgres_holds):
        """No stand-in: a copy that holds the key is not consulted."""
        _live, frozen = postgres_holds
        frozen["last_sync_products"] = "2026-10-09T12:43:22+03:00"
        assert await off_store.get_last_sync_time("products") is None

    @pytest.mark.asyncio
    async def test_and_the_hourly_step_syncs_the_catalogue_at_once(self, monkeypatch):
        """Kills: a step that read an absent key as "not due" — the catalogue
        would then never sync again until a full sync."""
        from core.sync_service import SyncService

        store = MagicMock()
        store.get_last_sync_time = AsyncMock(return_value=None)
        store.upsert_products = AsyncMock(return_value=3)
        store.set_last_sync_time = AsyncMock()
        client = MagicMock()

        async def pages(*args, **kwargs):
            yield [{"id": 1}, {"id": 2}, {"id": 3}]

        client.paginate = pages
        monkeypatch.setattr("core.sync_service.events.emit", AsyncMock())
        stats: dict = {}
        await SyncService(store)._catalogue_step_postgres(client, stats)
        store.upsert_products.assert_awaited_once_with([{"id": 1}, {"id": 2}, {"id": 3}])
        store.set_last_sync_time.assert_awaited_once_with("products")
        assert stats["products"] == 3


def _keys_read_back() -> set:
    """Every literal key passed to `get_last_sync_time` in core/, web/, bot/
    and scripts/."""
    keys = set()
    for root in ("core", "web", "bot", "scripts"):
        for path in (ROOT / root).rglob("*.py"):
            for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
                if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                        and node.func.attr == "get_last_sync_time" and node.args
                        and isinstance(node.args[0], ast.Constant)):
                    keys.add(node.args[0].value)
    return keys


class TestCategoriesAndExpenseTypesAreReadByNoSync:
    def test_no_path_reads_them_back(self):
        """Kills: a sync that starts gating on either — an absent key would
        then be a decision, and this module's argument would no longer hold."""
        read = _keys_read_back()
        assert {"orders", "products"} <= read, read          # the walk sees calls
        assert not {"categories", "expense_types"} & read, read

    @pytest.mark.parametrize("entity", ["categories", "expense_types", "products"])
    def test_absent_the_freshness_check_says_never_synced_at_the_same_severity(self, entity):
        """What the stand-in offered was DuckDB's stamp, frozen at the flip and
        days old: the same check at the same severity, "stalled". Absent, it
        is "never synced" — true, and no quieter. Kills: an absent moved key
        that the check skips."""
        from core.data_quality import FRESHNESS_THRESHOLDS, _judge_freshness

        now = datetime(2026, 10, 30, 6, 0, tzinfo=UTC)
        moved = {f"last_sync_{e}": (now - timedelta(minutes=5)).isoformat()
                 for e in FRESHNESS_THRESHOLDS if e != entity}
        copy = {f"last_sync_{entity}": (now - timedelta(days=20)).isoformat()}
        issues = [i for i in _judge_freshness(copy, now, chain_watermarks=moved,
                                              from_the_copy=True)
                  if i.check_name == f"freshness_{entity}"]
        (issue,) = issues
        assert "never synced" in issue.description
        assert issue.severity == FRESHNESS_THRESHOLDS[entity][1]


needs_pg = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")


@needs_pg
class TestOnARealPostgres:
    """The getter against the tables themselves: `meta.chain_watermarks`
    without the key, `app.sync_metadata` with it."""

    KEYS = ("last_sync_orders", "last_sync_products")

    @pytest.mark.asyncio
    async def test_orders_inherit_and_products_do_not(self, off_store, monkeypatch):
        import asyncpg

        from core import pg

        monkeypatch.setenv("KS_PG_DSN", DSN)
        pool = await asyncpg.create_pool(DSN, min_size=1, max_size=2)
        monkeypatch.setattr(pg, "get_pool", AsyncMock(return_value=pool))
        monkeypatch.setattr(pg, "require_revision", AsyncMock())
        try:
            async with pool.acquire() as conn:
                saved = await conn.fetch(
                    "SELECT key, value, updated_at FROM meta.chain_watermarks "
                    "WHERE key = ANY($1::text[])", list(self.KEYS))
                copied = await conn.fetch(
                    "SELECT key, value, updated_at FROM app.sync_metadata "
                    "WHERE key = ANY($1::text[])", list(self.KEYS))
                await conn.execute("DELETE FROM meta.chain_watermarks "
                                   "WHERE key = ANY($1::text[])", list(self.KEYS))
                await conn.execute("DELETE FROM app.sync_metadata "
                                   "WHERE key = ANY($1::text[])", list(self.KEYS))
                for key in self.KEYS:
                    await conn.execute(
                        "INSERT INTO app.sync_metadata (key, value, updated_at) "
                        "VALUES ($1, '2026-10-15T08:00:44+00:00', now())", key)
            try:
                assert await off_store.get_last_sync_time("orders") == datetime(
                    2026, 10, 15, 8, 0, 44, tzinfo=UTC)
                assert await off_store.get_last_sync_time("products") is None
            finally:
                async with pool.acquire() as conn:
                    await conn.execute("DELETE FROM app.sync_metadata "
                                       "WHERE key = ANY($1::text[])", list(self.KEYS))
                    for rows, table in ((saved, "meta.chain_watermarks"),
                                        (copied, "app.sync_metadata")):
                        for r in rows:
                            await conn.execute(
                                f"INSERT INTO {table} (key, value, updated_at) "
                                "VALUES ($1, $2, $3) ON CONFLICT (key) DO UPDATE "
                                "SET value = EXCLUDED.value", r["key"], r["value"],
                                r["updated_at"])
        finally:
            await pool.close()
