"""A restored `manager_comment` reaches Postgres, and is labelled for it.

DN-17. Both `manager_comment` backfills — the admin endpoint and the CLI —
UPDATE DuckDB directly, and until now stopped there. Nothing else could carry
the result: the hourly ids-diff ships the orders Postgres is *missing*, and
these exist on both sides and merely differ.

**What that costs is not the /traffic tab.** That reads `silver.orders LEFT
JOIN silver.order_utm`, and both backfills have shipped the parsed UTM rows
through `ship_after_reparse` since DN-04, so the screen already saw the
result. What a comment stranded in DuckDB costs is the daily `mirror_landing`
orders fingerprint, which compares `manager_comment` between the stores and
so reported every restore as a disagreeing bucket, and step 9's Postgres UTM
parser, which reads `bronze.orders.manager_comment` directly — which is why
DN-17 lands before that parser is wired up.

`manager_comment` is also a versioned column, which is what made this need an
owner's decision rather than a patch. OD-20 (b), 2026-09-17: the row is
archived like any other, under `kind = 'backfill'`, and the two liveness
checks exclude that kind as they exclude the migration's `'baseline'`. Nothing
is skipped and nothing is renamed — the label is what lets the checks tell an
operator's run from the writer's output.

Executed against a real PostgreSQL and a real DuckDB file, because every claim
here is a claim about what two databases do to real rows.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from core.duckdb_store import DuckDBStore

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

# Far above anything the rest of the suite uses, so the rows this test writes
# and deletes cannot be another module's.
IDS = (971001, 971002, 971003)
WHEN = "2026-08-20T12:00:00+00:00"


def _payload(order_id: int, comment=None) -> dict:
    return {
        "id": order_id, "source_id": 1, "status_id": 12, "status_group_id": 4,
        "grand_total": "100.00", "ordered_at": WHEN, "created_at": WHEN,
        "updated_at": WHEN, "buyer": {"id": 500}, "manager": {"id": 4},
        "manager_comment": comment, "promocode": None,
        "products": [{"name": "Товар", "quantity": 1, "price_sold": "100.00",
                      "offer": {"product_id": 701}}],
    }


async def _clean(pool):
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM app.order_versions WHERE order_id = ANY($1::int[])", list(IDS))
        await conn.execute(
            "DELETE FROM bronze.order_products WHERE order_id = ANY($1::int[])", list(IDS))
        await conn.execute(
            "DELETE FROM bronze.orders WHERE id = ANY($1::int[])", list(IDS))


@pytest_asyncio.fixture
async def stores(tmp_path, monkeypatch):
    """Postgres holding three orders with no comment, written by the sync's
    one order call site — chain 3's writer, which since step 14's S14-00 is
    the only one. (Until then DuckDB held them and `ship_orders_by_id`
    carried them across; that ship stands down under the chain, and its tests
    went with it. Chain 3's own restore, `restore_manager_comments`, is
    proved in `tests/integration/test_orders_chain_writer.py`.)"""
    from core import pg_derivation

    monkeypatch.setenv("KS_PG_DSN", DSN)
    monkeypatch.setenv("KS_PG_DERIVE", "own")
    monkeypatch.delenv("KS_MIRROR_LANDING", raising=False)
    before_mode = (pg_derivation._mode, pg_derivation._mode_error)
    pg_derivation.configure_mode()

    store = DuckDBStore(db_path=tmp_path / "backfill.duckdb")
    await store.connect()
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    await _clean(pool)

    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
         patch("core.pg.require_revision", new=AsyncMock()):
        from core.sync_service import SyncService

        await SyncService(store)._upsert_orders_with_expenses(
            [_payload(i) for i in IDS])
        yield store, pool

    await _clean(pool)
    await pool.close()
    await store.close()
    pg_derivation._mode, pg_derivation._mode_error = before_mode


class TestTheRestoredCommentReachesPostgres:
    @pytest.mark.asyncio
    async def test_a_label_the_checks_do_not_know_is_refused(self, stores):
        """Before the transaction opens, so a typo cannot roll back a header
        upsert that `mirror_orders` would then swallow."""
        from core.pg_landing import write_orders

        with pytest.raises(ValueError, match="version_kind"):
            await write_orders([], [], replace_products=False,
                               version_kind="backfil")


class TestWhatTheLivenessChecksMakeOfIt:
    """1,500 rows in a day is the shape of the writer having stopped
    discriminating — and it is also a shape a backfill run could take. The
    kind is what tells them apart, and it has to, because no threshold can:
    the two are the same number.

    Not today's number. Measured on production 2026-09-17, one run would be
    ~26 rows: **0** orders diverge between the stores right now, and of the
    10,192 NULL comments in DuckDB's 730-day window only 26 are website
    orders, which are the only ones a KeyCRM re-fetch could fill. 1,500 is
    chosen here because it is the first number above the threshold, not
    because it is expected."""

    @staticmethod
    async def _insert(pool, kind: str, count: int):
        async with pool.acquire() as conn:
            await conn.executemany(
                "INSERT INTO app.order_versions (order_id, kind) VALUES ($1, $2)",
                [(IDS[0], kind)] * count,
            )

    @staticmethod
    def _flooding(issues):
        return [i for i in issues if i.check_name == "order_versions_flooding"]

    @pytest.mark.asyncio
    async def test_a_backfill_of_fifteen_hundred_orders_is_not_a_flood(self, stores):
        from core.mirror_reconciliation import (
            ORDER_VERSIONS_FLOOD_PER_DAY,
            ORDER_VERSIONS_RECENT_SQL,
            reconcile_order_versions,
        )

        _store, pool = stores
        async with pool.acquire() as conn:
            base = await conn.fetchval(
                ORDER_VERSIONS_RECENT_SQL,
                datetime.now(timezone.utc) - timedelta(hours=24))
        assert base < ORDER_VERSIONS_FLOOD_PER_DAY, (
            "another module left a day's worth of versions behind; this test "
            "measures a delta and cannot start above the threshold")

        await self._insert(pool, "backfill", 1500)
        assert not self._flooding(await reconcile_order_versions())

    @pytest.mark.asyncio
    async def test_fifteen_hundred_changes_still_is(self, stores):
        from core.mirror_reconciliation import reconcile_order_versions

        _store, pool = stores
        await self._insert(pool, "change", 1500)
        found = self._flooding(await reconcile_order_versions())
        assert found and found[0].count >= 1500

    @pytest.mark.asyncio
    async def test_a_backfill_does_not_answer_for_the_writer_being_alive(self, stores):
        """The stall check reads the newest version, and a backfill must not
        be it: a run on a Tuesday would otherwise certify the sync as alive
        until Wednesday, which is the one day nobody is looking."""
        from core.mirror_reconciliation import ORDER_VERSIONS_NEWEST_SQL

        _store, pool = stores
        async with pool.acquire() as conn:
            before = await conn.fetchval(ORDER_VERSIONS_NEWEST_SQL)
            assert before is not None, (
                "the fixture's own ship should have written versions; an empty "
                "archive here means the helper is not writing at all")
            await conn.execute(
                "INSERT INTO app.order_versions (order_id, kind) VALUES ($1, 'backfill')",
                IDS[0])
            assert await conn.fetchval(ORDER_VERSIONS_NEWEST_SQL) == before
            await conn.execute(
                "INSERT INTO app.order_versions (order_id, kind) VALUES ($1, 'change')",
                IDS[0])
            assert await conn.fetchval(ORDER_VERSIONS_NEWEST_SQL) > before
