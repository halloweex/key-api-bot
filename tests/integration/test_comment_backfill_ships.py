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
    """A DuckDB holding three orders with no comment, mirrored into Postgres.

    `KS_PG_DERIVE=own` throughout, because one of the things under test is
    that the ship owes Postgres a rebuild — the helper goes through
    `write_orders`, so it inherits the mark, and a helper that wrote the rows
    by some other route would leave Silver describing the previous comment.
    """
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
        # DuckDB first, with the mirror stubbed: the seeding of Postgres is
        # the next line, and doing it through the helper keeps this test's
        # starting state built the same way production's is.
        with patch("core.pg_landing.mirror_orders", new=AsyncMock()):
            await store.upsert_orders([_payload(i) for i in IDS])
        from core.pg_backfill import ship_orders_by_id
        from core.pg_order_versions import CHANGE

        await ship_orders_by_id(store, IDS, version_kind=CHANGE)
        yield store, pool

    await _clean(pool)
    await pool.close()
    await store.close()
    pg_derivation._mode, pg_derivation._mode_error = before_mode


async def _comments(pool):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, manager_comment FROM bronze.orders "
            "WHERE id = ANY($1::int[]) ORDER BY id", list(IDS))
    return {int(r["id"]): r["manager_comment"] for r in rows}


async def _versions(pool):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT order_id, kind, manager_comment FROM app.order_versions "
            "WHERE order_id = ANY($1::int[]) ORDER BY id", list(IDS))
    return [(int(r["order_id"]), r["kind"], r["manager_comment"]) for r in rows]


async def _restore_in_duckdb(store, mapping):
    """What both backfills do to DuckDB: fill the NULLs, nothing else."""
    async with store.connection() as conn:
        for order_id, comment in mapping.items():
            conn.execute(
                "UPDATE orders SET manager_comment = ? "
                "WHERE id = ? AND manager_comment IS NULL",
                [comment, order_id],
            )


class TestTheRestoredCommentReachesPostgres:
    @pytest.mark.asyncio
    async def test_bronze_ends_up_holding_what_duckdb_holds(self, stores):
        store, pool = stores
        restored = {IDS[0]: "utm_source=fb|utm_campaign=spring",
                    IDS[1]: "utm_source=tg"}
        await _restore_in_duckdb(store, restored)

        from core.pg_backfill import ship_orders_by_id
        from core.pg_order_versions import BACKFILL

        result = await ship_orders_by_id(
            store, list(restored), version_kind=BACKFILL)

        assert result["orders_shipped"] == 2
        async with store.connection() as conn:
            duck = {int(r[0]): r[1] for r in conn.execute(
                "SELECT id, manager_comment FROM orders ORDER BY id").fetchall()}
        assert await _comments(pool) == duck
        assert duck[IDS[2]] is None, "the order nobody restored must stay NULL"

    @pytest.mark.asyncio
    async def test_one_backfill_version_per_order_that_actually_changed(self, stores):
        store, pool = stores
        await _restore_in_duckdb(store, {IDS[0]: "utm_source=fb"})

        from core.pg_backfill import ship_orders_by_id
        from core.pg_order_versions import BACKFILL

        # All three ids offered, as the route offers whatever KeyCRM returned.
        await ship_orders_by_id(store, IDS, version_kind=BACKFILL)

        versions = await _versions(pool)
        assert [(o, k) for o, k, _c in versions] == [
            (IDS[0], "create"), (IDS[1], "create"), (IDS[2], "create"),
            (IDS[0], BACKFILL),
        ]
        assert versions[-1][2] == "utm_source=fb"

    @pytest.mark.asyncio
    async def test_shipping_the_same_rows_again_writes_nothing(self, stores):
        """The comparison does not change with the label: an order whose
        header has not moved since its last version writes no row, however the
        caller would have named one."""
        store, pool = stores
        await _restore_in_duckdb(store, {IDS[0]: "utm_source=fb"})

        from core.pg_backfill import ship_orders_by_id
        from core.pg_order_versions import BACKFILL

        await ship_orders_by_id(store, IDS, version_kind=BACKFILL)
        before = await _versions(pool)
        await ship_orders_by_id(store, IDS, version_kind=BACKFILL)
        assert await _versions(pool) == before

    @pytest.mark.asyncio
    async def test_it_owes_postgres_a_rebuild(self, stores):
        """Silver reads `bronze.orders`, and `silver.order_utm` is parsed from
        this very column. A ship that did not raise the signal would leave the
        tab reading a classification of the comment it replaced."""
        from core.pg_backfill import ship_orders_by_id
        from core.pg_order_versions import BACKFILL

        store, pool = stores
        await _restore_in_duckdb(store, {IDS[0]: "utm_source=fb"})

        async def requested():
            async with pool.acquire() as conn:
                return await conn.fetchval(
                    "SELECT requested FROM meta.derivation_signal "
                    "WHERE layer = 'warehouse'")

        before = await requested()
        await ship_orders_by_id(store, [IDS[0]], version_kind=BACKFILL)
        assert await requested() == before + 1

    @pytest.mark.asyncio
    async def test_the_line_item_watermark_does_not_move(self, stores):
        """Headers only. `skip_products=True` follows the same rule at 05:15:
        stamping the line items as shipped here would tell the reconciliation
        they are current when nothing looked at them."""
        from core.pg_backfill import ship_orders_by_id
        from core.pg_landing import ORDER_PRODUCTS_TABLE
        from core.pg_order_versions import BACKFILL

        store, pool = stores
        await _restore_in_duckdb(store, {IDS[0]: "utm_source=fb"})

        async def stamp():
            async with pool.acquire() as conn:
                return await conn.fetchval(
                    "SELECT last_ok_at FROM meta.mirror_state WHERE table_name = $1",
                    ORDER_PRODUCTS_TABLE)

        before = await stamp()
        await ship_orders_by_id(store, [IDS[0]], version_kind=BACKFILL)
        assert await stamp() == before

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
