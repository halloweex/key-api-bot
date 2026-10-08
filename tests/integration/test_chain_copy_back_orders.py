"""Chain 3's way back, against a real Postgres and a real DuckDB (design §6, C).

The chain is the registered one, `core.pg_orders_write`, and it is latched the
way production would latch it: its own writer's first write takes the marker
and claims the four owner rows in the transaction that writes the orders.
Before that, both stores hold the same orders because DuckDB's own path wrote
them and the sync's real mirrors shipped them.

What they prove:

1. a clean pre-flip handover says nothing, and an order only DuckDB holds is
   refused with the backfill that ships it;
2. after a latch that created orders, moved a status, shrank a basket 3 → 2 and
   recorded a miss, `--execute` copies, compares and releases — DuckDB then
   equals Postgres on every written column, the watermark moved home, the
   latch is gone;
3. with the flag back at duckdb, the next DuckDB write of an order the chain
   wrote is archived as one `'change'` — the archive never left Postgres, so
   there is no second `'create'` and no gap;
4. a DuckDB version KeyCRM dates later refuses, and nothing is written; so
   does an order only DuckDB holds after the latch, and a line item of an
   order the chain never rewrote.

No NULL case: in these four tables every column DuckDB declares NOT NULL is
NOT NULL in Postgres too (revisions 0003, 0008, 0020), so
`handover_rows_unwritable` has nothing to find here — the unit test of the
specs reads DuckDB's catalogue for chain 4's case.

The copy-back reads WHOLE tables, so the four chain tables are emptied around
each test, `test_chain_copy_back_buyers`' precedent on this shared database.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

asyncpg = pytest.importorskip("asyncpg")

from core import chain_latch, pg_orders_write as chain, write_chains  # noqa: E402

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

ORDERS, LINES, EXPENSES, MISSES = chain.CHAIN_TABLES
BASE = 990_500
T0 = datetime(2026, 9, 20, 12, 0, tzinfo=timezone.utc)


def _payload(oid: int, *, status: int = 12, updated: datetime = T0, items: int = 2,
             comment="utm_source=ig") -> dict:
    return {
        "id": oid, "source_id": 1, "status_id": status, "status_group_id": 4,
        "grand_total": "100.00", "ordered_at": T0.isoformat(),
        "created_at": T0.isoformat(), "updated_at": updated.isoformat(),
        "buyer": {"id": 5501}, "manager": {"id": 4},
        "manager_comment": comment, "promocode": None,
        "products": [{"name": f"Товар {i}", "quantity": 1, "price_sold": "50.00",
                      "offer": {"product_id": 700 + i}} for i in range(items)],
        "expenses": [{"id": oid * 10, "expense_type_id": 1, "amount": 10.5,
                      "status": "paid"}],
    }


async def _clean(pool):
    async with pool.acquire() as conn:
        for table in (LINES, EXPENSES, ORDERS, MISSES):
            await conn.execute(f"DELETE FROM {table}")
        await conn.execute(
            "DELETE FROM app.order_versions WHERE order_id BETWEEN $1 AND $2",
            BASE, BASE + 99)
        await conn.execute("DELETE FROM meta.chain_watermarks")
        await conn.execute(
            "DELETE FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
            [ORDERS, LINES, EXPENSES, MISSES])


@pytest_asyncio.fixture
async def stores(tmp_path, monkeypatch):
    from core.duckdb_store import DuckDBStore

    for c in write_chains.WRITE_CHAINS:
        monkeypatch.delenv(c.WRITE_ENV, raising=False)
    monkeypatch.setenv("KS_PG_DSN", DSN)
    monkeypatch.setattr(chain, "unmet_precondition", lambda: None)
    store = DuckDBStore(db_path=tmp_path / "orders-back.duckdb")
    await store.connect()
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    await _clean(pool)
    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
         patch("core.pg.require_revision", new=AsyncMock()):
        yield store, pool, monkeypatch
    await _clean(pool)
    await pool.close()
    await store.close()


async def _sync(store, payloads, **kw):
    """The sync's one order call site: DuckDB and its mirrors before the flag,
    the chain after it."""
    from core.sync_service import SyncService

    return await SyncService(store)._upsert_orders_with_expenses(payloads, **kw)


async def _both(store, pool, payloads):
    """The same orders and expenses in both stores, as the sync leaves them
    with the chain on DuckDB."""
    assert not chain.writes_postgres()
    await _sync(store, payloads)


async def _duck(store, sql, params=None):
    async with store.connection() as conn:
        return conn.execute(sql, params or []).fetchall()


async def _pg(pool, sql, *params):
    async with pool.acquire() as conn:
        return await conn.fetch(sql, *params)


def _critical(issues):
    return {(i.check_name, i.table_name) for i in issues if i.severity.value == "CRITICAL"}


async def _flip(env):
    env.setenv(chain.WRITE_ENV, "postgres")
    assert chain.writes_postgres()


class TestBeforeTheFlip:
    @pytest.mark.asyncio
    async def test_a_clean_handover_says_nothing(self, stores):
        from core.chain_transfer import handover_check

        store, pool, _env = stores
        await _both(store, pool, [_payload(BASE), _payload(BASE + 1, items=3)])
        assert await handover_check(store, chain) == []

    @pytest.mark.asyncio
    async def test_an_order_only_duckdb_holds_names_the_backfill(self, stores):
        from core.chain_transfer import handover_check

        store, pool, env = stores
        await _both(store, pool, [_payload(BASE)])
        env.setenv("KS_MIRROR_LANDING", "0")        # it reaches DuckDB alone
        await _sync(store, [_payload(BASE + 1)])
        env.delenv("KS_MIRROR_LANDING")
        issues = await handover_check(store, chain)
        assert ("handover_rows_missing", ORDERS) in _critical(issues)
        said = " ".join(i.description for i in issues if i.table_name == ORDERS)
        assert "/api/mirror/backfill/orders" in said


class TestTheWayBack:
    @pytest.mark.asyncio
    async def test_the_copy_back_carries_the_chains_writes_and_releases(self, stores):
        from core.chain_transfer import copy_back, handover_check

        store, pool, env = stores
        await _both(store, pool, [_payload(BASE, items=3), _payload(BASE + 1)])
        await _flip(env)
        # The chain's writes: a status moved and the basket shrank 3 → 2 on
        # BASE, two new orders, a miss and the watermark.
        await _sync(store, [_payload(BASE, status=20, items=2,
                                     updated=T0 + timedelta(hours=1)),
                            _payload(BASE + 2), _payload(BASE + 3)])
        await store.record_backfill_misses({BASE + 4: "not_in_keycrm"})
        stamp = T0 + timedelta(hours=2)
        await store.set_last_sync_time("orders", stamp)
        assert await _pg(pool, "SELECT value FROM meta.chain_watermarks "
                               "WHERE key = 'last_sync_orders'")
        # The search index's Postgres cursor moves with the chain (OD-15):
        # written to Postgres under it, carried back and released with it.
        cursor = T0 + timedelta(hours=3)
        await store.set_last_sync_time("meilisearch_pg", cursor)
        assert await _pg(pool, "SELECT value FROM meta.chain_watermarks "
                               "WHERE key = 'last_sync_meilisearch_pg'")
        assert chain_latch.latched(chain.CHAIN)

        preview = await handover_check(store, chain)
        assert _critical(preview) == set(), preview

        plan = await copy_back(store, chain, dry_run=False)

        assert plan["executed"] and plan["committed"] and plan["released"], plan
        assert plan["findings"] == []
        assert sorted(tuple(r) for r in await _duck(
            store, "SELECT id, status_id FROM orders ORDER BY id")) == [
            (BASE, 20), (BASE + 1, 12), (BASE + 2, 12), (BASE + 3, 12)]
        assert [r[0] for r in await _duck(
            store, "SELECT id FROM order_products WHERE order_id = ? ORDER BY id",
            [BASE])] == [BASE * 1000, BASE * 1000 + 1]
        assert sorted(r[0] for r in await _duck(store, "SELECT id FROM expenses")) == [
            BASE * 10, (BASE + 1) * 10, (BASE + 2) * 10, (BASE + 3) * 10]
        assert [r[0] for r in await _duck(
            store, "SELECT order_id FROM order_backfill_misses")] == [BASE + 4]
        stored = (await _duck(store, "SELECT value FROM sync_metadata "
                                     "WHERE key = 'last_sync_orders'"))[0][0]
        assert datetime.fromisoformat(stored) == stamp
        carried = (await _duck(store, "SELECT value FROM sync_metadata "
                                      "WHERE key = 'last_sync_meilisearch_pg'"))[0][0]
        assert datetime.fromisoformat(carried) == cursor
        assert plan["sync_keys"].keys() == {"last_sync_orders", "last_sync_meilisearch_pg"}
        assert await _pg(pool, "SELECT key FROM meta.chain_watermarks "
                               "WHERE key LIKE 'owner:%' OR key LIKE 'last_sync_%'") == []
        assert chain_latch.latched_at(chain.CHAIN) is None
        # The release hands the writes back to the flag; put it back as the
        # runbook does and DuckDB writes again.
        env.setenv(chain.WRITE_ENV, "duckdb")
        assert chain.writes_postgres() is False

    @pytest.mark.asyncio
    async def test_after_the_way_back_the_archive_goes_on_without_a_second_create(
            self, stores):
        from core.chain_transfer import copy_back

        store, pool, env = stores
        await _both(store, pool, [_payload(BASE)])
        await _flip(env)
        await _sync(store, [_payload(BASE + 1)])          # created by the chain
        assert (await copy_back(store, chain, dry_run=False))["released"]
        env.setenv(chain.WRITE_ENV, "duckdb")

        # DuckDB's path again, its mirror shipping through the one writer.
        await _sync(store, [_payload(BASE + 1, status=19,
                                     updated=T0 + timedelta(hours=1))])

        kinds = [r[0] for r in await _pg(
            pool, "SELECT kind FROM app.order_versions WHERE order_id = $1 "
                  "ORDER BY id", BASE + 1)]
        assert kinds == ["create", "change"]
        assert (await _pg(pool, "SELECT status_id FROM bronze.orders WHERE id = $1",
                          BASE + 1))[0][0] == 19


class TestTheRefusals:
    async def _refused(self, store, check, table):
        from core.chain_transfer import CopyBackRefused, copy_back, handover_check

        assert (check, table) in _critical(await handover_check(store, chain))
        for dry_run in (True, False):
            with pytest.raises(CopyBackRefused) as refused:
                await copy_back(store, chain, dry_run=dry_run)
            assert (check, table) in {(i.check_name, i.table_name)
                                      for i in refused.value.issues}

    @pytest.mark.asyncio
    async def test_a_duckdb_version_keycrm_dates_later(self, stores):
        """A DuckDB write after the latch — an image older than the chain —
        that the rewrite clock alone would read as the chain's."""
        store, pool, env = stores
        await _both(store, pool, [_payload(BASE)])
        await _flip(env)
        await _sync(store, [_payload(BASE, status=20, updated=T0 + timedelta(hours=1))])
        later = T0 + timedelta(hours=5)
        async with store.connection() as conn:
            conn.execute("UPDATE orders SET status_id = 19, updated_at = ? WHERE id = ?",
                         [later, BASE])
        await self._refused(store, "handover_rows_newer_in_duckdb", ORDERS)
        # Nothing was written: DuckDB still holds its own version.
        assert (await _duck(store, "SELECT status_id FROM orders WHERE id = ?",
                            [BASE]))[0][0] == 19

    @pytest.mark.asyncio
    async def test_an_order_only_duckdb_holds_after_the_latch(self, stores):
        """The chain's writer never deletes an order, so one only DuckDB holds
        was written there after the flip, and a copy would delete it."""
        store, pool, env = stores
        await _both(store, pool, [_payload(BASE)])
        await _flip(env)
        await _sync(store, [_payload(BASE + 1)])
        async with store.connection() as conn:
            conn.execute(
                "INSERT INTO orders (id, source_id, status_id, grand_total) "
                "VALUES (?, 1, 12, 5.00)", [BASE + 9])
        await self._refused(store, "handover_rows_missing", ORDERS)

    @pytest.mark.asyncio
    async def test_a_line_item_of_an_order_the_chain_never_rewrote(self, stores):
        """Only a rewrite of its order explains a line item one store lacks."""
        store, pool, env = stores
        await _both(store, pool, [_payload(BASE), _payload(BASE + 1)])
        await _flip(env)
        await _sync(store, [_payload(BASE + 1, status=20,
                                     updated=T0 + timedelta(hours=1))])  # BASE untouched
        async with store.connection() as conn:
            conn.execute(
                "INSERT INTO order_products (id, order_id, product_id, name, quantity, "
                "price_sold) VALUES (?, ?, 799, 'Зайвий', 1, 1.00)",
                [BASE * 1000 + 7, BASE])
        await self._refused(store, "handover_rows_missing", LINES)


class TestALineItemIsDatedByItsOwnWrite:
    """A line item is the chain's only when the chain replaced its order's
    line items, never because its order's HEADER moved: the 05:15 refresh
    (`force_update=True, skip_products=True`) and the comment restore write
    headers alone, every day, and each moves `bronze.orders.mirrored_at`. Dated
    by the header, a line only DuckDB holds under any order refreshed since
    the latch read as a basket the chain shrank, and `--execute` deleted it
    (the chain-3 review). Mutation: `_REWRITE_STAMP[LINES]` back to
    `("bronze.orders", "id")`."""

    async def _refused(self, store, check, table):
        await TestTheRefusals._refused(self, store, check, table)

    @pytest.mark.asyncio
    async def test_a_rogue_line_under_a_header_only_refresh_is_refused(self, stores):
        store, pool, env = stores
        await _both(store, pool, [_payload(BASE, items=3), _payload(BASE + 1)])
        await _flip(env)
        # The chain shrinks BASE's basket, and refreshes BASE + 1's header only.
        await _sync(store, [_payload(BASE, status=20, items=2,
                                     updated=T0 + timedelta(hours=1))])
        await _sync(store, [_payload(BASE + 1, status=19)],
                    force_update=True, skip_products=True)
        assert (await _pg(pool, "SELECT status_id FROM bronze.orders WHERE id = $1",
                          BASE + 1))[0][0] == 19
        async with store.connection() as conn:
            conn.execute(
                "INSERT INTO order_products (id, order_id, product_id, name, quantity, "
                "price_sold) VALUES (?, ?, 799, 'Зайвий', 1, 1.00)",
                [(BASE + 1) * 1000 + 7, BASE + 1])
        from core.chain_transfer import handover_check

        issues = await handover_check(store, chain)
        on_lines = {(i.severity.value, tuple(i.sample_ids)) for i in issues
                    if i.table_name == LINES and i.check_name == "handover_rows_missing"}
        # The shrunk basket is still the chain's work; the rogue line is not.
        assert ("INFO", (BASE * 1000 + 2,)) in on_lines, on_lines
        assert ("CRITICAL", ((BASE + 1) * 1000 + 7,)) in on_lines, on_lines
        await self._refused(store, "handover_rows_missing", LINES)
        # Nothing was written: the line DuckDB holds is still there.
        assert await _duck(store, "SELECT id FROM order_products WHERE id = ?",
                           [(BASE + 1) * 1000 + 7])

    @pytest.mark.asyncio
    async def test_a_line_that_differs_under_a_comment_restore_is_refused(self, stores):
        store, pool, env = stores
        await _both(store, pool, [_payload(BASE, comment=None)])
        await _flip(env)
        assert await chain.restore_manager_comments({BASE: "utm_source=ig"}) == [BASE]
        async with store.connection() as conn:
            conn.execute("UPDATE order_products SET quantity = 9 WHERE id = ?",
                         [BASE * 1000])
        await self._refused(store, "handover_rows_differ", LINES)

    @pytest.mark.asyncio
    async def test_a_basket_the_chain_emptied_is_refused_and_says_so(self, stores):
        """Knowingly too strict: an emptied basket leaves no Postgres line to
        date, so its DuckDB lines cannot be told from stranded ones. The
        refusal names the case and the per-id decision."""
        from core.chain_transfer import handover_check

        store, pool, env = stores
        await _both(store, pool, [_payload(BASE)])
        await _flip(env)
        await _sync(store, [_payload(BASE, items=0, updated=T0 + timedelta(hours=1))])
        assert await _pg(pool, "SELECT id FROM bronze.order_products "
                               "WHERE order_id = $1", BASE) == []
        await self._refused(store, "handover_rows_missing", LINES)
        said = " ".join(i.description for i in await handover_check(store, chain)
                        if i.table_name == LINES)
        assert "EMPTIED" in said and "delete its line items from DuckDB" in said
