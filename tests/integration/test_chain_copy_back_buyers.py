"""Chain 4's way back, against a real Postgres and a real DuckDB (PR-2, 2.5).

PR-2 registers no chain, so these register a fake one declaring chain 4's
three tables — `bronze.buyers`, `bronze.buyer_contacts`, `app.buyer_gender` —
and latch it the way a chain writer does: the marker on disk, the owner rows
claimed inside the same Postgres transaction that writes the rows. The rows
themselves go through `core.pg_buyer_rows`, the writer the chain will run.

What they prove, in the plan's numbering:

1. a clean pre-flip handover says nothing;
2. after a latch that added a buyer, shrank a contact list 3 → 2 and changed a
   verdict with `override_by_human`, `--execute` copies, compares and releases;
3. DuckDB then equals Postgres, the override and `decided_at` included, the
   contacts' allocator sits above every id, and the watermark moved home;
4. the hourly copy afterwards leaves the override alone — and 5, the control:
   without the copy-back the same release and copy wipe it;
6. the refusals: a NULL name, a buyer only DuckDB holds, a contact of a buyer
   the chain never rewrote, and before a flip a contact only Postgres holds;
7. a whitespace name is written back without complaint;
8. the reship clears what the pre-flip handover refused on — a number of a
   buyer DuckDB holds with none included (F2);
9. the reship's loop keeps its docstring's promises: a guard that says stop
   stops it, it ships in portions of `chunk`, a latch taken between portions
   stops the next one, a mirror switched off refuses, and its writer takes
   contacts as any iterable (F9, review of #265);
10. the handover's two conservative branches: buyers are judged before their
   contacts whatever order the chain declares them in, and with no
   `owner:bronze.buyers` row no buyer counts as the chain's write (F10).

The copy-back reads WHOLE tables, so the three tables are emptied around each
test — `test_buyer_writes_mirror`'s precedent on this shared database.
"""
from __future__ import annotations

import os
import types
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from core.landing_rows import parse_buyers
from core.models import Buyer

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

BUYERS = "bronze.buyers"
CONTACTS = "bronze.buyer_contacts"
GENDER = "app.buyer_gender"
TABLES = (BUYERS, CONTACTS, GENDER)
NAME = "pg_buyers_fake_write"
ENV = "KS_WRITE_BUYERS_FAKE"
DECIDED = datetime(2026, 9, 20, 8, 0, tzinfo=timezone.utc)


def _buyer(bid, *, name=None, phones=None):
    return Buyer.from_api({
        "id": bid, "full_name": name or f"Покупець {bid}",
        "created_at": "2026-09-01 10:00:00+00:00",
        "updated_at": "2026-09-02T11:30:00Z",
        "phone": phones if phones is not None else [f"+38050{bid:07d}"],
        "email": [],
    })


def _fake_chain():
    fake = types.ModuleType(f"core.{NAME}")
    fake.CHAIN = NAME
    fake.WRITE_ENV = ENV
    fake.CHAIN_TABLES = TABLES
    fake.CHAIN_SYNC_KEYS = ("last_sync_buyers",)
    fake.env_writes_postgres = lambda: False
    return fake


async def _clean(pool):
    async with pool.acquire() as conn:
        for table in (GENDER, CONTACTS, BUYERS):
            await conn.execute(f"DELETE FROM {table}")
        await conn.execute("DELETE FROM meta.chain_watermarks")
        await conn.execute(
            "DELETE FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
            list(TABLES))


@pytest_asyncio.fixture
async def stores(tmp_path, monkeypatch):
    from core import write_chains
    from core.duckdb_store import DuckDBStore

    monkeypatch.setenv("KS_PG_DSN", DSN)
    chain = _fake_chain()
    monkeypatch.setattr(write_chains, "WRITE_CHAINS", write_chains.WRITE_CHAINS + (chain,))
    store = DuckDBStore(db_path=tmp_path / "buyers-back.duckdb")
    await store.connect()
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    await _clean(pool)
    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
         patch("core.pg.require_revision", new=AsyncMock()):
        yield store, pool, chain, monkeypatch
    await _clean(pool)
    await pool.close()
    await store.close()


async def _verdict_both(store, pool, bid, *, gender="f", override=False):
    """The same verdict in both stores, as the hourly copy leaves them."""
    row = (bid, gender, "dictionary", "high", "given", 1, override, DECIDED)
    async with store.connection() as conn:
        conn.execute(
            "INSERT INTO buyer_gender (buyer_id, gender, method, confidence, "
            "decided_from, rules_version, override_by_human, decided_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)", list(row))
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO app.buyer_gender (buyer_id, gender, method, confidence, "
            "decided_from, rules_version, override_by_human, decided_at) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", *row)


async def _mirrored(store, pool, buyers):
    """Buyers into DuckDB and, through the real mirror, into Postgres; each
    with a verdict in both stores."""
    await store.upsert_buyers(buyers)
    for b in buyers:
        await _verdict_both(store, pool, b.id)


async def _latch_and_write(pool, buyers=(), *, sync_value="2026-09-20T09:00:00+00:00"):
    """What a chain writer's first write does: marker, then one transaction
    that claims the owner rows and writes the rows through the one writer."""
    from core import chain_latch
    from core.pg_buyer_rows import _write_buyer_rows

    chain_latch.latch(NAME, ENV)
    parsed = parse_buyers(list(buyers))
    async with pool.acquire() as conn:
        async with conn.transaction():
            await chain_latch.claim(conn, TABLES, datetime.now(timezone.utc).isoformat())
            await _write_buyer_rows(conn, parsed.rows, parsed.contacts)
            await conn.execute(
                "INSERT INTO meta.chain_watermarks (key, value, updated_at) "
                "VALUES ('last_sync_buyers', $1, now()) "
                "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value", sync_value)


def _critical(issues):
    return {(i.check_name, i.table_name) for i in issues if i.severity.value == "CRITICAL"}


async def _duck(store, sql, params=None):
    async with store.connection() as conn:
        return conn.execute(sql, params or []).fetchall()


async def _pg(pool, sql, *params):
    async with pool.acquire() as conn:
        return await conn.fetch(sql, *params)


class TestTheWayBack:
    @pytest.mark.asyncio
    async def test_a_clean_pre_flip_handover_says_nothing(self, stores):
        from core.chain_transfer import handover_check

        store, pool, chain, _env = stores
        await _mirrored(store, pool, [_buyer(1, phones=["+380501", "+380502"]), _buyer(2)])

        assert await handover_check(store, chain) == []

    @pytest.mark.asyncio
    async def test_the_copy_back_carries_the_chains_writes_and_releases(self, stores):
        from core import chain_latch
        from core.chain_transfer import copy_back, handover_check
        from core.duckdb_sequences import next_value

        store, pool, chain, _env = stores
        await _mirrored(store, pool, [
            _buyer(1, phones=["+380501", "+380502", "+380503"]), _buyer(2)])
        # The chain's first write: buyer 1's list shrinks to two, buyer 3 is new.
        await _latch_and_write(pool, [
            _buyer(1, phones=["+380501", "+380503"]), _buyer(3)])
        # A person overrides buyer 1's verdict after the latch.
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE app.buyer_gender SET gender = 'm', override_by_human = TRUE, "
                "decided_at = $2 WHERE buyer_id = $1", 1, DECIDED + timedelta(hours=2))

        preview = await handover_check(store, chain)
        assert _critical(preview) == set(), preview

        plan = await copy_back(store, chain, dry_run=False)

        assert plan["executed"] and plan["committed"] and plan["released"], plan
        assert plan["findings"] == []
        # 3. DuckDB equals Postgres, the override and decided_at included.
        assert sorted(tuple(r) for r in await _duck(
            store, "SELECT id, full_name FROM buyers ORDER BY id")) == [
            (1, "Покупець 1"), (2, "Покупець 2"), (3, "Покупець 3")]
        assert [r[0] for r in await _duck(
            store, "SELECT value FROM buyer_contacts WHERE buyer_id = 1 ORDER BY value")] == [
            "+380501", "+380503"]
        gender = await _duck(store, "SELECT gender, override_by_human, decided_at "
                                    "FROM buyer_gender WHERE buyer_id = 1")
        assert gender[0][0] == "m" and gender[0][1] is True
        # An instant, compared as one: DuckDB hands TIMESTAMPTZ back in the
        # process's zone, so re-labelling it UTC passed on a UTC laptop and
        # failed under Europe/Kyiv on the same, correct value.
        stamp = gender[0][2]
        stamp = stamp if stamp.tzinfo else stamp.replace(tzinfo=timezone.utc)
        assert stamp == DECIDED + timedelta(hours=2)
        async with store.connection() as conn:
            top = conn.execute("SELECT MAX(id) FROM buyer_contacts").fetchone()[0]
            assert next_value(conn, "seq_buyer_contacts_id") > top
        assert await _duck(store, "SELECT value FROM sync_metadata "
                                  "WHERE key = 'last_sync_buyers'") == [
            ("2026-09-20T09:00:00+00:00",)]
        assert await _pg(pool, "SELECT key FROM meta.chain_watermarks") == []
        assert chain_latch.latched_at(NAME) is None

    @pytest.mark.asyncio
    async def test_the_hourly_copy_then_keeps_the_override(self, stores):
        """4. After the copy-back DuckDB holds the override, so the replace
        out of it puts the override back rather than removing it."""
        from core.chain_transfer import copy_back
        from core.pg_operational import replicate_operational

        store, pool, chain, _env = stores
        await _mirrored(store, pool, [_buyer(1)])
        await _latch_and_write(pool, [_buyer(1, name="Покупець Один")])
        async with pool.acquire() as conn:
            await conn.execute("UPDATE app.buyer_gender SET override_by_human = TRUE, "
                               "decided_at = $2 WHERE buyer_id = $1",
                               1, DECIDED + timedelta(hours=1))
        assert (await copy_back(store, chain, dry_run=False))["released"]

        result = await replicate_operational(store)

        assert "error" not in result, result
        assert GENDER in result["replaced"], result
        assert (await _pg(pool, "SELECT override_by_human FROM app.buyer_gender "
                                "WHERE buyer_id = 1"))[0][0] is True

    @pytest.mark.asyncio
    async def test_control_without_the_copy_back_the_same_copy_wipes_it(self, stores):
        """5. Release by hand and let the hourly copy run: DuckDB never learnt
        of the override, so the full replace removes it. What the test above
        keeps is the copy-back's doing."""
        from core.chain_transfer import release_chain
        from core.pg_operational import replicate_operational

        store, pool, chain, _env = stores
        await _mirrored(store, pool, [_buyer(1)])
        await _latch_and_write(pool, [_buyer(1)])
        async with pool.acquire() as conn:
            await conn.execute("UPDATE app.buyer_gender SET override_by_human = TRUE "
                               "WHERE buyer_id = 1")
        await release_chain(pool, chain)

        result = await replicate_operational(store)

        assert "error" not in result, result
        assert (await _pg(pool, "SELECT override_by_human FROM app.buyer_gender "
                                "WHERE buyer_id = 1"))[0][0] is False

    @pytest.mark.asyncio
    async def test_a_whitespace_name_goes_back_as_it_is(self, stores):
        """7. DuckDB's NOT NULL takes it; only NULL is refused."""
        from core.chain_transfer import copy_back

        store, pool, chain, _env = stores
        await _mirrored(store, pool, [_buyer(1)])
        await _latch_and_write(pool, [_buyer(1)])
        async with pool.acquire() as conn:
            await conn.execute("UPDATE bronze.buyers SET full_name = '   ', "
                               "mirrored_at = now() WHERE id = 1")

        plan = await copy_back(store, chain, dry_run=False)

        assert plan["released"], plan
        assert (await _duck(store, "SELECT full_name FROM buyers WHERE id = 1"))[0][0] == "   "


class TestTheRefusals:
    """6. Each is a CRITICAL in `--handover` and a refusal before anything is
    written, in both the dry run and the execution."""

    async def _refused(self, store, chain, check, table):
        from core.chain_transfer import CopyBackRefused, copy_back, handover_check

        assert (check, table) in _critical(await handover_check(store, chain))
        for dry_run in (True, False):
            with pytest.raises(CopyBackRefused) as refused:
                await copy_back(store, chain, dry_run=dry_run)
            assert (check, table) in {(i.check_name, i.table_name)
                                      for i in refused.value.issues}

    @pytest.mark.asyncio
    async def test_a_null_name(self, stores):
        store, pool, chain, _env = stores
        await _mirrored(store, pool, [_buyer(1)])
        await _latch_and_write(pool, [_buyer(1)])
        async with pool.acquire() as conn:
            await conn.execute("UPDATE bronze.buyers SET full_name = NULL, "
                               "mirrored_at = now() WHERE id = 1")
        await self._refused(store, chain, "handover_rows_unwritable", BUYERS)
        # Nothing was written: DuckDB still holds the old name.
        assert (await _duck(store, "SELECT full_name FROM buyers WHERE id = 1"))[0][0] == "Покупець 1"

    @pytest.mark.asyncio
    async def test_a_buyer_only_duckdb_holds(self, stores):
        store, pool, chain, _env = stores
        await _mirrored(store, pool, [_buyer(1)])
        await _latch_and_write(pool, [_buyer(1)])
        # After the latch the mirror stands down, so this reaches DuckDB alone.
        await store.upsert_buyers([_buyer(9)])
        await self._refused(store, chain, "handover_rows_missing", BUYERS)

    @pytest.mark.asyncio
    async def test_a_contact_of_a_buyer_the_chain_never_rewrote(self, stores):
        store, pool, chain, _env = stores
        await _mirrored(store, pool, [_buyer(1), _buyer(2)])
        await _latch_and_write(pool, [_buyer(1)])     # buyer 2 untouched
        async with store.connection() as conn:
            conn.execute("INSERT INTO buyer_contacts (buyer_id, contact_type, value, "
                         "is_primary) VALUES (2, 'phone', '+380599', FALSE)")
        await self._refused(store, chain, "handover_rows_missing", CONTACTS)

    @pytest.mark.asyncio
    async def test_before_a_flip_a_contact_only_postgres_holds(self, stores):
        from core.chain_transfer import handover_check

        store, pool, chain, _env = stores
        await _mirrored(store, pool, [_buyer(1)])
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO bronze.buyer_contacts (buyer_id, "
                               "contact_type, value, is_primary) "
                               "VALUES (1, 'phone', '+380598', FALSE)")
        assert ("handover_rows_ahead", CONTACTS) in _critical(await handover_check(store, chain))


class TestTheReship:
    @pytest.mark.asyncio
    async def test_it_clears_what_the_pre_flip_handover_refused(self, stores):
        """8. A contact only Postgres holds and a buyer that differs: both
        CRITICAL before a flip, both gone after the reship."""
        from core.chain_transfer import handover_check
        from core.pg_buyers import reship_buyers

        store, pool, chain, _env = stores
        await _mirrored(store, pool, [_buyer(1), _buyer(2)])
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO bronze.buyer_contacts (buyer_id, "
                               "contact_type, value, is_primary) "
                               "VALUES (1, 'phone', '+380597', FALSE)")
            await conn.execute("UPDATE bronze.buyers SET full_name = 'Інше' WHERE id = 2")
        assert _critical(await handover_check(store, chain)) == {
            ("handover_rows_ahead", CONTACTS), ("handover_rows_differ", BUYERS)}

        result = await reship_buyers(store, chunk=1)

        assert result["status"] == "done" and result["buyers_shipped"] == 2, result
        assert await handover_check(store, chain) == []

    @pytest.mark.asyncio
    async def test_it_clears_a_number_of_a_buyer_duckdb_holds_with_none(self, stores):
        """F2 (review of #265). DuckDB holds buyer 1 with no contact at all;
        Postgres still holds a number for it. The reship hands `(1, [])`, and
        the contacts DELETE must run for an empty list too, or `--handover`
        stays CRITICAL after the very lever it names. Mutation killed: the
        DELETE moved under `if buyer_contacts:` in `core.pg_buyer_rows`."""
        from core.chain_transfer import handover_check
        from core.pg_buyers import reship_buyers

        store, pool, chain, _env = stores
        await _mirrored(store, pool, [_buyer(1, phones=[])])
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO bronze.buyer_contacts (buyer_id, "
                               "contact_type, value, is_primary) "
                               "VALUES (1, 'phone', '+380597', FALSE)")
        assert _critical(await handover_check(store, chain)) == {
            ("handover_rows_ahead", CONTACTS)}

        result = await reship_buyers(store, chunk=1)

        assert (result["status"], result["buyers_shipped"],
                result["contacts_shipped"]) == ("done", 1, 0), result
        assert await handover_check(store, chain) == []

    @pytest.mark.asyncio
    async def test_it_refuses_once_the_chain_is_latched(self, stores):
        from core.pg_buyers import reship_buyers

        store, pool, _chain, _env = stores
        await _mirrored(store, pool, [_buyer(1)])
        await _latch_and_write(pool, [_buyer(1)])
        with pytest.raises(RuntimeError, match="is written by a write chain"):
            await reship_buyers(store)

    # ── The loop the docstring promises (F9, review of #265) ──
    #
    # Every case above used the default guard, chunk=1 over two buyers, and a
    # latch already standing before the first portion; six mutations of the
    # loop passed all of them. Postgres's names are changed first, so a buyer
    # the reship did not reach still says so afterwards.

    @staticmethod
    def _guard(calls, *, answers=(), on_entry=None):
        """A portion guard that counts its entries, answers `answers[n]` on
        the n-th (True past the end), and runs `on_entry(n)` first."""
        import contextlib

        @contextlib.asynccontextmanager
        async def guard():
            calls.append(len(calls))
            n = calls[-1]
            if on_entry is not None:
                await on_entry(n)
            yield answers[n] if n < len(answers) else True

        return guard

    @staticmethod
    async def _names(pool):
        return {r["id"]: r["full_name"] for r in await _pg(
            pool, "SELECT id, full_name FROM bronze.buyers ORDER BY id")}

    @staticmethod
    async def _renamed_in_postgres(pool):
        async with pool.acquire() as conn:
            await conn.execute("UPDATE bronze.buyers SET full_name = 'PG-' || id")

    @pytest.mark.asyncio
    async def test_a_guard_that_says_stop_stops_the_run(self, stores):
        """Mutation killed: `if not go:` that never fires — the run writing on
        without the lock it was told it could not have."""
        from core.pg_buyers import reship_buyers

        store, pool, _chain, _env = stores
        await _mirrored(store, pool, [_buyer(1), _buyer(2), _buyer(3)])
        await self._renamed_in_postgres(pool)
        calls = []

        result = await reship_buyers(
            store, chunk=1, portion_guard=self._guard(calls, answers=(True, False)))

        assert (result["status"], result["buyers_shipped"], result["buyers_total"]) == (
            "stopped", 1, 3), result
        assert len(calls) == 2
        assert await self._names(pool) == {1: "Покупець 1", 2: "PG-2", 3: "PG-3"}

    @pytest.mark.asyncio
    async def test_it_ships_in_portions_of_chunk(self, stores):
        """Five buyers at chunk=2 are three portions, each behind the guard,
        and the counts are what was written. Mutations killed: `chunk`
        ignored (one portion), and `contacts_shipped` not counted."""
        from core.pg_buyers import reship_buyers

        store, pool, _chain, _env = stores
        await _mirrored(store, pool, [_buyer(i) for i in range(1, 6)])
        await self._renamed_in_postgres(pool)
        calls = []

        result = await reship_buyers(store, chunk=2, portion_guard=self._guard(calls))

        assert (result["status"], result["buyers_shipped"],
                result["contacts_shipped"]) == ("done", 5, 5), result
        assert len(calls) == 3
        assert await self._names(pool) == {i: f"Покупець {i}" for i in range(1, 6)}

    @pytest.mark.asyncio
    async def test_a_latch_taken_mid_run_stops_the_next_portion(self, stores):
        """"Asked before the first portion and again before each": a chain
        that takes the buyers between two portions — its owner rows claimed,
        as a second process's first write would — stops the run before it
        writes over them. Mutation killed: the in-loop `refuse_if_owned()`
        removed."""
        from core import chain_latch
        from core.pg_buyers import reship_buyers

        store, pool, _chain, _env = stores
        await _mirrored(store, pool, [_buyer(1), _buyer(2)])
        await self._renamed_in_postgres(pool)

        async def latched_before_the_second(n):
            if n == 1:
                async with pool.acquire() as conn:
                    async with conn.transaction():
                        await chain_latch.claim(
                            conn, TABLES, datetime.now(timezone.utc).isoformat())

        with pytest.raises(RuntimeError, match="is written by a write chain"):
            await reship_buyers(store, chunk=1, portion_guard=self._guard(
                [], on_entry=latched_before_the_second))
        assert await self._names(pool) == {1: "Покупець 1", 2: "PG-2"}

    @pytest.mark.asyncio
    async def test_with_the_mirror_off_it_refuses_and_writes_nothing(self, stores):
        """Mutation killed: the `pg_landing.enabled()` refusal dropped."""
        from core import pg_landing
        from core.pg_buyers import reship_buyers

        store, pool, _chain, env = stores
        await _mirrored(store, pool, [_buyer(1)])
        await self._renamed_in_postgres(pool)
        env.setenv(pg_landing.MIRROR_ENV, "0")

        with pytest.raises(RuntimeError, match="KS_MIRROR_LANDING is off"):
            await reship_buyers(store)
        assert await self._names(pool) == {1: "PG-1"}

    @pytest.mark.asyncio
    async def test_the_writer_takes_contacts_as_any_iterable(self, stores):
        """`_write` counts the contacts and then hands them on, so it lists
        them first: a generator would be spent by the count and its contacts
        silently never written. Mutation killed:
        `contacts_by_buyer = list(contacts_by_buyer)` removed."""
        from core import pg_buyers

        _store, pool, _chain, _env = stores
        parsed = parse_buyers([_buyer(1, phones=["+380501", "+380502"])])

        await pg_buyers._write(parsed.rows, (pair for pair in parsed.contacts))

        assert sorted(r["value"] for r in await _pg(
            pool, "SELECT value FROM bronze.buyer_contacts WHERE buyer_id = 1")) == [
            "+380501", "+380502"]
        (rows,) = [r["last_rows"] for r in await _pg(
            pool, "SELECT last_rows FROM meta.mirror_state WHERE table_name = $1",
            CONTACTS)]
        assert rows == 2


class TestTheReviewsCases:
    """What the PR-2 review found the rewrite clock alone could not see."""

    @pytest.mark.asyncio
    async def test_a_newer_duckdb_version_after_the_marker_was_lost_refuses(self, stores):
        """The chain wrote buyer 5 after the latch; then the marker was lost
        with the flag at duckdb, and the DuckDB buyers step fetched a later
        version of it from KeyCRM. Buyer 5 reads as rewritten by the chain,
        yet DuckDB's copy is the newer one: overwriting it, and deleting its
        new number as a contact the chain "dropped", would lose KeyCRM's
        latest. KeyCRM's own updated_at says so."""
        from core import chain_latch
        from core.chain_transfer import CopyBackRefused, copy_back, handover_check

        store, pool, chain, _env = stores
        await _mirrored(store, pool, [_buyer(5, phones=["+380501"])])
        await _latch_and_write(pool, [_buyer(5, phones=["+380501"])])
        chain_latch.release(NAME)                       # the marker is lost
        newer = Buyer.from_api({
            "id": 5, "full_name": "Покупець 5", "phone": ["+380502"], "email": [],
            "created_at": "2026-09-01 10:00:00+00:00",
            "updated_at": "2026-09-24T09:00:00Z"})
        await store.upsert_buyers([newer])              # DuckDB only: the mirror stands down

        found = _critical(await handover_check(store, chain))
        assert ("handover_rows_newer_in_duckdb", BUYERS) in found, found
        assert ("handover_rows_missing", CONTACTS) in found, found
        with pytest.raises(CopyBackRefused):
            await copy_back(store, chain, dry_run=False)
        assert [r[0] for r in await _duck(
            store, "SELECT value FROM buyer_contacts WHERE buyer_id = 5")] == ["+380502"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("order", [(BUYERS, CONTACTS, GENDER),
                                       (CONTACTS, BUYERS, GENDER)],
                             ids=["buyers-first", "contacts-first"])
    async def test_buyers_are_judged_before_their_contacts_whatever_the_order(
            self, stores, order):
        """F10 (review of #265). The case above, with the chain's tables
        declared in either order. A buyer DuckDB holds in a later version is
        taken out of the rewritten set before its contacts are judged, so its
        new number is a stranded contact (CRITICAL), not one the chain
        "dropped" (INFO) that the copy-back would delete. That needs the
        buyers judged first, which `_handover_issues` sorts for rather than
        trusting `CHAIN_TABLES`. Mutation killed: `ordered = list(specs)` —
        with the contacts first, `handover_rows_missing` disappeared."""
        from core import chain_latch
        from core.chain_transfer import handover_check

        store, pool, chain, env = stores
        env.setattr(chain, "CHAIN_TABLES", order)
        await _mirrored(store, pool, [_buyer(5, phones=["+380501"])])
        await _latch_and_write(pool, [_buyer(5, phones=["+380501"])])
        chain_latch.release(NAME)                       # the marker is lost
        await store.upsert_buyers([Buyer.from_api({
            "id": 5, "full_name": "Покупець 5", "phone": ["+380502"], "email": [],
            "created_at": "2026-09-01 10:00:00+00:00",
            "updated_at": "2026-09-24T09:00:00Z"})])

        assert _critical(await handover_check(store, chain)) == {
            ("handover_rows_newer_in_duckdb", BUYERS),
            ("handover_rows_missing", CONTACTS),
            ("handover_rows_ahead", CONTACTS),
        }

    @pytest.mark.asyncio
    async def test_without_the_buyers_owner_row_nothing_is_the_chains(self, stores):
        """F10 (review of #265). The chain holds an owner row — so Postgres
        has moved on — but not `owner:bronze.buyers`: a row deleted by hand,
        or an image older than the chain. Then no buyer can be shown to be
        the chain's write, and a buyer that differs stays CRITICAL however
        recently Postgres stamped it; the copy-back refuses and DuckDB keeps
        its name. Mutation killed: `_rewritten_since_latch` answering every
        buyer Postgres holds when the owner row is absent — the review saw
        the difference read INFO, the copy-back release, and DuckDB's name
        overwritten."""
        from core import chain_latch
        from core.chain_transfer import CopyBackRefused, copy_back, handover_check

        store, pool, chain, _env = stores
        await _mirrored(store, pool, [_buyer(1)])
        chain_latch.latch(NAME, ENV)
        async with pool.acquire() as conn:
            async with conn.transaction():
                await chain_latch.claim(conn, (GENDER,),
                                        datetime.now(timezone.utc).isoformat())
            await conn.execute(
                "UPDATE bronze.buyers SET full_name = 'Інше ім''я', "
                "mirrored_at = now() + interval '1 second' WHERE id = 1")

        found = await handover_check(store, chain)

        assert {(i.check_name, i.table_name, i.severity.value) for i in found
                if i.table_name == BUYERS} == {
            ("handover_rows_differ", BUYERS, "CRITICAL")}, found
        with pytest.raises(CopyBackRefused):
            await copy_back(store, chain, dry_run=False)
        assert [r[0] for r in await _duck(
            store, "SELECT full_name FROM buyers WHERE id = 1")] == ["Покупець 1"]
        assert chain_latch.latched_at(NAME) is not None

    @pytest.mark.asyncio
    async def test_an_orphan_contact_in_either_store_blocks_nothing(self, stores):
        """A contact whose buyer its own store does not hold is outside the
        landing: the daily check never read DuckDB's, the reship cannot ship
        it, and so the copy-back neither refuses on it nor carries it."""
        from core.chain_transfer import copy_back, handover_check

        store, pool, chain, _env = stores
        await _mirrored(store, pool, [_buyer(1)])
        async with store.connection() as conn:
            conn.execute("INSERT INTO buyer_contacts (buyer_id, contact_type, value, "
                         "is_primary) VALUES (77, 'phone', '+380577', FALSE)")
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO bronze.buyer_contacts (buyer_id, "
                               "contact_type, value, is_primary) "
                               "VALUES (78, 'phone', '+380578', FALSE)")

        assert await handover_check(store, chain) == []

        await _latch_and_write(pool, [_buyer(1)])
        plan = await copy_back(store, chain, dry_run=False)

        assert plan["released"], plan
        assert await _duck(store, "SELECT buyer_id FROM buyer_contacts "
                                  "WHERE buyer_id IN (77, 78)") == []
