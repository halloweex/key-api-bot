"""The catalogue's history (`app.catalogue_versions`, revision 0036) against a
real PostgreSQL, through the chain's own write path.

Chain 6 writes the whole product catalogue every hour and overwrites a
product's name, brand and category in place. After each products write
`core.pg_catalogue_write` keeps the history the analysts' warehouse reads:
SCD2 by content diff, in a transaction of its own (design 3.1, 3.3). Each case
below is named by the mutation it kills:

- the first run seeds one version per product, all at one `known_from`, read
  after the write committed; a run with nothing changed writes nothing and
  still stamps;
- a change writes exactly one version, the old one stays; the row hash is the
  canonical row's 64-bit md5 prefix, brand verbatim (`''` is not NULL);
- a product one write leaves out is not retired; two writes leave it out and
  it is tombstoned with its content; served again, it is live again; a row a
  human deletes from bronze is tombstoned with its last version's content; a
  product first seen already retired is seeded as a tombstone;
- the mass-delete guard refuses more than max(50, 1 %) tombstones whole —
  the write it follows committed, nothing of the history moved, the stamp
  says `self_check:` and the watch files `capture_self_check_failed`; 50 pass;
  `guard=False` is the lever;
- the self-check refuses a history that disagrees with bronze, and a hash
  that is not its row's;
- a capture that fails never fails the write, and is stamped as failed; with
  the switch off nothing is written or stamped;
- two captures at once write one change once.

The catalogue, the history and their stamps are emptied around each test.
"""
from __future__ import annotations

import asyncio
import functools
import hashlib
import os
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from core import analytics_captures as cap
from core import pg_catalogue_write as chain
from core import write_chains

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

TABLE = chain.VERSIONS
STAMP_ROW = cap.state_row(TABLE)


def _product(pid, name=None, brand="default", category_id=10):
    """A KeyCRM product payload. `brand="default"` is a brand per id; None
    is no custom field (bronze NULL); `""` an emptied one (bronze '')."""
    fields = []
    if brand == "default":
        fields = [{"uuid": "CT_1001", "value": [f"Brand {pid % 3}"]}]
    elif brand is not None:
        fields = [{"uuid": "CT_1001", "value": [brand]}]
    return {"id": pid, "name": name or f"Product {pid}", "category_id": category_id,
            "sku": f"S-{pid}", "price": 100, "custom_fields": fields}


PAYLOAD = [_product(1), _product(2), _product(3)]


async def _clean(pool):
    async with pool.acquire() as conn:
        await conn.execute(f"DELETE FROM {TABLE}")
        for table in chain.CHAIN_TABLES:
            await conn.execute(f"DELETE FROM {table}")
        await conn.execute(
            "DELETE FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
            list(chain.CHAIN_TABLES) + [STAMP_ROW])
        await conn.execute(
            "DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%' "
            "OR key LIKE $2 OR key = ANY($1::text[])", list(chain.CHAIN_SYNC_KEYS),
            chain.RECORD_PREFIX + "%")


@pytest_asyncio.fixture
async def stores(monkeypatch):
    from core.store import AnalyticsStore

    for c in write_chains.WRITE_CHAINS:
        monkeypatch.delenv(c.WRITE_ENV, raising=False)
    monkeypatch.delenv(cap.SWITCH_ENV, raising=False)
    monkeypatch.setenv("KS_PG_DSN", DSN)
    store = AnalyticsStore()
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=4)
    await _clean(pool)
    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
         patch("core.pg.require_revision", new=AsyncMock()):
        yield store, pool, monkeypatch
    await _clean(pool)
    await pool.close()


async def _pg(pool, sql, *params):
    async with pool.acquire() as conn:
        return await conn.fetch(sql, *params)


async def _versions(pool, pid=None):
    sql = (f"SELECT id, product_id, known_from, name, brand, category_id, row_hash, "
           f"is_deleted FROM {TABLE}")
    if pid is None:
        return await _pg(pool, sql + " ORDER BY product_id, known_from, id")
    return await _pg(pool, sql + " WHERE product_id = $1 ORDER BY known_from, id", pid)


async def _stamp(pool):
    rows = await _pg(pool, "SELECT last_ok_at, last_rows, failures_since_ok, last_error "
                           "FROM meta.mirror_state WHERE table_name = $1", STAMP_ROW)
    return rows[0] if rows else None


async def _capture(**kw):
    """One run of the capture, as the writer runs it, outside any write."""
    if kw:
        return await cap.run_own_transaction(
            TABLE, functools.partial(chain._capture_versions, **kw))
    return await cap.run_own_transaction(TABLE, chain._capture_versions)


def _expected_hash(name, brand, category_id) -> int:
    """The canonical row hashed in Python, independently of the SQL: `\\N`
    for NULL, chr(31) between columns, the first 64 bits of md5, signed."""
    def text(v):
        return "\\N" if v is None else str(v)

    canon = chr(31).join((text(name), text(brand), text(category_id)))
    value = int(hashlib.md5(canon.encode("utf-8")).hexdigest()[:16], 16)
    return value - (1 << 64) if value >= 1 << 63 else value


def _watch(issues, table=TABLE):
    return {(i.check_name, i.severity.value) for i in issues if i.table_name == table}


def _refusals(monkeypatch):
    """Every `CaptureRefused` the capture raises, as the writer runs it: the
    numbers a refusal was written from, read without parsing its sentence."""
    seen = []
    real = chain._capture_versions

    async def spy(conn, **kw):
        try:
            return await real(conn, **kw)
        except cap.CaptureRefused as exc:
            seen.append(exc)
            raise

    monkeypatch.setattr(chain, "_capture_versions", spy)
    return seen


class TestTheFirstRunAndAQuietOne:
    @pytest.mark.asyncio
    async def test_the_first_run_seeds_every_product_at_one_instant(self, stores):
        store, pool, _env = stores
        assert await store.upsert_products(PAYLOAD) == 3

        versions = await _versions(pool)
        bronze = {r["id"]: r for r in await _pg(
            pool, "SELECT id, name, brand, category_id, mirrored_at FROM bronze.products")}
        assert [v["product_id"] for v in versions] == [1, 2, 3]
        assert not any(v["is_deleted"] for v in versions)
        for v in versions:
            b = bronze[v["product_id"]]
            assert (v["name"], v["brand"], v["category_id"]) == (
                b["name"], b["brand"], b["category_id"])
        # One run, one knowledge time — read after the write committed.
        (known_from,) = {v["known_from"] for v in versions}
        assert known_from > max(b["mirrored_at"] for b in bronze.values())

        stamp = await _stamp(pool)
        assert stamp["last_rows"] == 3 and stamp["failures_since_ok"] == 0
        assert stamp["last_error"] is None

    @pytest.mark.asyncio
    async def test_a_write_that_changed_nothing_writes_nothing_and_still_stamps(self, stores):
        store, pool, _env = stores
        await store.upsert_products(PAYLOAD)
        first = await _stamp(pool)

        await store.upsert_products(PAYLOAD)

        assert len(await _versions(pool)) == 3
        stamp = await _stamp(pool)
        assert stamp["last_ok_at"] > first["last_ok_at"] and stamp["last_rows"] == 0


class TestAChangeIsAVersion:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("edit", [
        {"name": "Product 2 renamed"},
        {"brand": "Another brand"},
        {"category_id": 11},
    ], ids=["name", "brand", "category"])
    async def test_one_edit_is_one_new_version_and_the_old_one_stays(self, stores, edit):
        store, pool, _env = stores
        await store.upsert_products(PAYLOAD)
        await store.upsert_products([PAYLOAD[0], _product(2, **edit), PAYLOAD[2]])

        assert [len(await _versions(pool, pid)) for pid in (1, 2, 3)] == [1, 2, 1]
        old, new = await _versions(pool, 2)
        assert old["known_from"] < new["known_from"]
        assert (old["name"], old["brand"], old["category_id"]) == ("Product 2", "Brand 2", 10)
        bronze = (await _pg(pool, "SELECT name, brand, category_id FROM bronze.products "
                                  "WHERE id = 2"))[0]
        assert (new["name"], new["brand"], new["category_id"]) == tuple(bronze)
        assert not new["is_deleted"] and new["row_hash"] != old["row_hash"]

    @pytest.mark.asyncio
    async def test_the_hash_is_the_canonical_row_and_brand_is_kept_verbatim(self, stores):
        """`''` is what bronze holds for an emptied custom field, NULL for no
        field. Both mean "no brand" to a reader (`NULLIF(brand, '')`), and both
        are kept as bronze holds them, so a flip is one version."""
        store, pool, _env = stores
        await store.upsert_products([_product(1, brand=""), _product(2, brand=None),
                                     _product(3, category_id=None)])
        rows = {v["product_id"]: v for v in await _versions(pool)}
        assert rows[1]["brand"] == "" and rows[2]["brand"] is None
        for v in rows.values():
            assert v["row_hash"] == _expected_hash(v["name"], v["brand"], v["category_id"])
        assert rows[1]["row_hash"] != _expected_hash("Product 1", None, 10)

        await store.upsert_products([_product(1, brand=None), _product(2, brand=None),
                                     _product(3, category_id=None)])
        flipped = await _versions(pool, 1)
        assert [v["brand"] for v in flipped] == ["", None]
        assert len(await _versions(pool)) == 4


class TestWhatLeavingTheCatalogueIs:
    @pytest.mark.asyncio
    async def test_one_short_write_retires_nothing(self, stores):
        """A truncated catalogue is put right by the next write; one write that
        left a product out is not KeyCRM retiring it."""
        store, pool, _env = stores
        await store.upsert_products(PAYLOAD)
        await store.upsert_products(PAYLOAD[:2])      # 3 left out once
        await store.upsert_products(PAYLOAD)          # and served again

        assert len(await _versions(pool)) == 3
        assert not any(v["is_deleted"] for v in await _versions(pool))

    @pytest.mark.asyncio
    async def test_two_writes_without_it_tombstone_it_and_serving_it_revives_it(self, stores):
        store, pool, _env = stores
        await store.upsert_products(PAYLOAD)
        await store.upsert_products(PAYLOAD[:2])
        assert len(await _versions(pool, 3)) == 1

        await store.upsert_products(PAYLOAD[:2])      # left out of both of the last two
        seed, tomb = await _versions(pool, 3)
        assert tomb["is_deleted"] and not seed["is_deleted"]
        # The tombstone carries the content bronze still holds, so a brand
        # asked of a date after it still answers.
        assert (tomb["name"], tomb["brand"], tomb["category_id"], tomb["row_hash"]) == (
            seed["name"], seed["brand"], seed["category_id"], seed["row_hash"])
        assert (await _pg(pool, "SELECT count(*) AS n FROM bronze.products "
                                "WHERE id = 3"))[0]["n"] == 1, "the writer deleted it"

        await store.upsert_products(PAYLOAD[:2])      # still out: nothing more
        assert len(await _versions(pool, 3)) == 2

        await store.upsert_products(PAYLOAD)          # served again
        *_, back = await _versions(pool, 3)
        assert not back["is_deleted"] and back["row_hash"] == seed["row_hash"]
        assert len(await _versions(pool)) == 5

    @pytest.mark.asyncio
    async def test_a_row_deleted_from_bronze_is_tombstoned_with_its_last_content(self, stores):
        store, pool, _env = stores
        await store.upsert_products(PAYLOAD)
        await store.upsert_products([PAYLOAD[0], PAYLOAD[1], _product(3, name="Renamed 3")])
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM bronze.products WHERE id = 3")

        await store.upsert_products(PAYLOAD[:2])

        *_, last, tomb = await _versions(pool, 3)
        assert tomb["is_deleted"] and not last["is_deleted"]
        assert (tomb["name"], tomb["row_hash"]) == ("Renamed 3", last["row_hash"])
        await _capture()
        assert len(await _versions(pool, 3)) == 3, "a tombstone is written once"

    @pytest.mark.asyncio
    async def test_a_product_first_seen_already_retired_is_seeded_as_a_tombstone(
            self, stores):
        store, pool, env = stores
        env.setenv(cap.SWITCH_ENV, "off")
        await store.upsert_products(PAYLOAD)
        await store.upsert_products(PAYLOAD[:2])
        await store.upsert_products(PAYLOAD[:2])
        assert await _versions(pool) == []
        env.delenv(cap.SWITCH_ENV)

        await store.upsert_products(PAYLOAD[:2])

        rows = {v["product_id"]: v["is_deleted"] for v in await _versions(pool)}
        assert rows == {1: False, 2: False, 3: True}
        assert (await _stamp(pool))["last_rows"] == 3


async def _seed(store, n):
    payload = [_product(i) for i in range(1, n + 1)]
    await store.upsert_products(payload)
    return payload


class TestTheMassDeleteGuard:
    @pytest.mark.asyncio
    async def test_more_than_the_limit_is_refused_whole_and_the_write_stands(self, stores):
        store, pool, env = stores
        payload = await _seed(store, 120)             # limit: max(50, 1) = 50
        refused = _refusals(env)
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM bronze.products WHERE id > 69")   # 51 gone
        before = await _versions(pool)

        # The write itself renames product 1 — a change the refused run must
        # not keep either.
        renamed = [_product(1, name="Renamed 1")] + payload[1:69]
        assert await store.upsert_products(renamed) == 69

        assert await _versions(pool) == before
        assert (await _pg(pool, "SELECT name FROM bronze.products WHERE id = 1"))[0][
            "name"] == "Renamed 1", "the write the capture follows did not stand"
        stamp = await _stamp(pool)
        assert stamp["failures_since_ok"] == 1
        assert stamp["last_error"].startswith(cap.SELF_CHECK_MARK + "CaptureRefused: ")
        (refusal,) = refused
        assert (refusal.facts["tombstoned"], refusal.facts["live"],
                refusal.facts["limit"]) == (51, 120, 50)
        assert refusal.facts["sample_ids"] == tuple(range(70, 70 + chain.CAPTURE_SAMPLE))

        issues = cap.judge(await cap.read_facts(pool=pool))
        assert _watch(issues) == {
            (cap.condition(cap.SELF_CHECK_FAILED, TABLE), "WARN")}

        # The rows are back: the next run passes and versions the rename.
        assert await store.upsert_products([_product(1, name="Renamed 1")] + payload[1:]) == 120
        first, renamed_version = await _versions(pool, 1)
        assert renamed_version["name"] == "Renamed 1"
        assert not any(v["is_deleted"] for v in await _versions(pool))
        assert (await _stamp(pool))["failures_since_ok"] == 0
        assert _watch(cap.judge(await cap.read_facts(pool=pool))) == set()

    @pytest.mark.asyncio
    async def test_the_limit_itself_passes(self, stores):
        store, pool, _env = stores
        payload = await _seed(store, 120)
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM bronze.products WHERE id > 70")   # 50 gone

        await store.upsert_products(payload[:70])

        tombs = [v for v in await _versions(pool) if v["is_deleted"]]
        assert sorted(v["product_id"] for v in tombs) == list(range(71, 121))
        assert (await _stamp(pool))["failures_since_ok"] == 0

    @pytest.mark.asyncio
    async def test_guard_false_is_the_lever_for_a_confirmed_retirement(self, stores):
        store, pool, _env = stores
        payload = await _seed(store, 120)
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM bronze.products WHERE id > 60")
        await store.upsert_products(payload[:60])
        assert not [v for v in await _versions(pool) if v["is_deleted"]]

        assert await _capture(guard=False) is True

        assert len([v for v in await _versions(pool) if v["is_deleted"]]) == 60
        assert (await _stamp(pool))["failures_since_ok"] == 0

    @pytest.mark.asyncio
    async def test_a_seed_of_retired_products_is_not_a_mass_delete(self, stores):
        """The guard counts live versions turning into tombstones. A first run
        that finds products already retired removes nothing anybody held."""
        store, pool, env = stores
        env.setenv(cap.SWITCH_ENV, "off")
        payload = await _seed(store, 120)
        await store.upsert_products(payload[:10])
        await store.upsert_products(payload[:10])
        env.delenv(cap.SWITCH_ENV)

        await store.upsert_products(payload[:10])

        assert len([v for v in await _versions(pool) if v["is_deleted"]]) == 110
        assert (await _stamp(pool))["failures_since_ok"] == 0


class TestTheSelfCheck:
    @pytest.mark.asyncio
    async def test_a_history_that_disagrees_with_bronze_is_refused(self, stores):
        """A latest version edited by hand, hash left alone: its hash is not
        its row's, and the run refuses before it diffs, rather than versioning
        bronze's content over it as a change nobody made."""
        store, pool, env = stores
        await store.upsert_products(PAYLOAD)
        refused = _refusals(env)
        async with pool.acquire() as conn:
            await conn.execute(f"UPDATE {TABLE} SET brand = 'Edited' WHERE product_id = 2")
        await store.upsert_products([_product(1, name="Renamed 1")] + PAYLOAD[1:])

        assert len(await _versions(pool)) == 3, "a refused run kept a version"
        stamp = await _stamp(pool)
        assert stamp["last_error"].startswith(cap.SELF_CHECK_MARK)
        assert [r.facts for r in refused] == [{"mis_hashed_before": 1}]
        assert _watch(cap.judge(await cap.read_facts(pool=pool))) == {
            (cap.condition(cap.SELF_CHECK_FAILED, TABLE), "WARN")}

    @pytest.mark.asyncio
    async def test_a_hash_that_is_not_its_rows_is_refused(self, stores, monkeypatch):
        store, pool, _env = stores
        refused = _refusals(monkeypatch)
        monkeypatch.setattr(chain, "_VERSION_INSERT_SQL",
                            chain._VERSION_INSERT_SQL.replace("$6", "($6::bigint # 1)"))
        await store.upsert_products(PAYLOAD)

        assert await _versions(pool) == []
        assert (await _stamp(pool))["last_error"].startswith(cap.SELF_CHECK_MARK)
        assert [r.facts for r in refused] == [
            {"differ": 0, "orphaned": 0, "mis_hashed": 3}]

    @pytest.mark.asyncio
    async def test_contents_that_hash_alike_are_still_two_versions(self, stores):
        """The canonical row is not injective: NULL and the text `\\N`
        render alike. The diff compares columns, as the self-check does, so
        the second content is versioned rather than every run after refused.
        Kills: the diff deciding by `row_hash` — the self-check would then
        refuse every hourly run, and the whole catalogue history would stop."""
        store, pool, _env = stores
        await store.upsert_products([_product(1, brand=None)])
        (seed,) = await _versions(pool, 1)
        assert seed["brand"] is None
        async with pool.acquire() as conn:
            await conn.execute("UPDATE bronze.products SET brand = $1 WHERE id = 1", "\\N")

        assert await _capture() is True
        assert await _capture() is True

        versions = await _versions(pool, 1)
        assert [v["brand"] for v in versions] == [None, "\\N"]
        assert versions[0]["row_hash"] == versions[1]["row_hash"]
        assert (await _stamp(pool))["failures_since_ok"] == 0


class TestTheCaptureNeverFailsTheWrite:
    @pytest.mark.asyncio
    async def test_a_capture_that_raises_leaves_the_write_and_is_stamped_failed(
            self, stores, monkeypatch):
        store, pool, _env = stores
        monkeypatch.setattr(chain, "_VERSIONS_DIFF_SQL",
                            "SELECT * FROM no_such_table WHERE $1::bigint IS NULL")

        assert await store.upsert_products(PAYLOAD) == 3

        assert len(await _pg(pool, "SELECT id FROM bronze.products")) == 3
        assert await _versions(pool) == []
        stamp = await _stamp(pool)
        assert stamp["failures_since_ok"] == 1 and stamp["last_ok_at"] is None
        assert stamp["last_error"].startswith("UndefinedTableError")
        assert _watch(cap.judge(await cap.read_facts(pool=pool))) == {
            (cap.condition(cap.FAILED, TABLE), "WARN")}

    @pytest.mark.asyncio
    async def test_with_the_switch_off_nothing_is_written_or_stamped(self, stores):
        store, pool, env = stores
        env.setenv(cap.SWITCH_ENV, "off")

        assert await store.upsert_products(PAYLOAD) == 3

        assert await _versions(pool) == []
        assert await _stamp(pool) is None


class _WriteBeforeTheSelfCheck:
    """The capture's connection, except that a products edit commits on
    another connection just before the self-check reads — a products write
    landing in the middle of a capture."""

    def __init__(self, conn, pool):
        self._conn, self._pool = conn, pool

    async def fetchrow(self, sql, *args):
        async with self._pool.acquire() as other:
            await other.execute("UPDATE bronze.products SET name = 'Edited mid-run' "
                                "WHERE id = 2")
        return await self._conn.fetchrow(sql, *args)

    def __getattr__(self, name):
        return getattr(self._conn, name)


class TestOneSnapshot:
    @pytest.mark.asyncio
    async def test_a_write_landing_mid_capture_is_the_next_runs(self, stores):
        """The diff and the self-check read one snapshot, so an edit committed
        between them is neither a refusal now nor lost: the next run versions
        it. Kills: the capture at read committed."""
        store, pool, _env = stores
        await store.upsert_products(PAYLOAD)

        async def capture(conn):
            return await chain._capture_versions(_WriteBeforeTheSelfCheck(conn, pool))

        assert await cap.run_own_transaction(TABLE, capture) is True
        assert (await _stamp(pool))["failures_since_ok"] == 0
        assert len(await _versions(pool, 2)) == 1

        await _capture()
        assert [v["name"] for v in await _versions(pool, 2)] == ["Product 2", "Edited mid-run"]


class TestTwoCapturesAtOnce:
    @pytest.mark.asyncio
    async def test_they_queue_on_the_lock_and_write_one_change_once(self, stores):
        store, pool, _env = stores
        await store.upsert_products(PAYLOAD)
        async with pool.acquire() as conn:
            await conn.execute("UPDATE bronze.products SET name = 'Edited 2' WHERE id = 2")

        async with pool.acquire() as holder:
            tx = holder.transaction()
            await tx.start()
            runs = None
            try:
                await holder.execute(chain._VERSIONS_LOCK_SQL)
                runs = asyncio.gather(_capture(), _capture())
                for _ in range(200):
                    (waiting,) = await _pg(
                        pool, "SELECT count(*) AS n FROM pg_stat_activity "
                              "WHERE datname = current_database() "
                              "AND wait_event_type = 'Lock'")
                    if waiting["n"] >= 2:
                        break
                    await asyncio.sleep(0.05)
                else:
                    pytest.fail("the two captures never came to wait on the lock")
            finally:
                await tx.rollback()
            assert await asyncio.wait_for(runs, timeout=30) == [True, True]

        assert [v["name"] for v in await _versions(pool, 2)] == ["Product 2", "Edited 2"]
        assert len(await _versions(pool)) == 4
