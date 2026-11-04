"""Chain 5 — the manager classification and stats, without a database.

What is pinned here, each with the mutation it must kill:

- the payload is read once, for both stores (`core.landing_rows.manager_row`);
- the stats body is one text, and its date is Kyiv's in both engines rather
  than the process's — a laptop or CI under UTC used to store a day early.

The copy-back's fourth source — chain 5's two tables derived from what the
replicator ships and the daily comparison compares — its handover and its
full rebuild went with the copy-back in step 14's S14-04.

Everything that needs Postgres is in
`tests/integration/test_managers_chain_writer.py`.

Since step 14's S14-00 the chain writes Postgres whatever its flag says and
asks no precondition, and step 13's way back is not this code's: the tests
of the flag, of each precondition, of the preflight before the flip and of
the refused way back went with them, as did those of DuckDB's own stats and
boot seed.
"""
from __future__ import annotations

import ast
import inspect
import textwrap
from datetime import date, datetime, timezone

import pytest

from core import landing_rows, sql_dialect
from core.sql_dialect import DUCKDB, POSTGRES


# ─── The one reading of a KeyCRM user ───────────────────────────────────────


class TestTheManagerRow:
    @pytest.mark.parametrize("payload,expected", [
        ({"id": 1, "name": "Олена", "email": "o@x", "status": "active"},
         (1, "Олена", "o@x", "active", False)),
        # `name` empty, `full_name` there: the fallback.
        ({"id": 2, "name": "", "full_name": "Петро"}, (2, "Петро", None, None, False)),
        # Neither: 'Unknown'.
        ({"id": 3}, (3, "Unknown", None, None, False)),
        # `full_name` present and null stays null — DuckDB's expression, which
        # the writers then refuse rather than invent a name.
        ({"id": 5, "name": None, "full_name": None}, (5, None, None, None, False)),
        # A listed id seeds retail; an unlisted one does not.
        ({"id": 4, "name": "R"}, (4, "R", None, None, True)),
    ])
    def test_every_edge_reads_as_it_always_did(self, payload, expected):
        """Mutation: change a fallback, or the seed, in the shared function."""
        assert tuple(landing_rows.manager_row(payload)) == expected

    def test_the_seed_is_the_constant(self):
        from core.duckdb_constants import RETAIL_MANAGER_IDS

        for mid in RETAIL_MANAGER_IDS:
            assert landing_rows.manager_row({"id": mid, "name": "x"}).is_retail is True

    def test_a_batch_keeps_keycrms_order(self):
        rows = landing_rows.manager_rows([{"id": 9, "name": "a"}, {"id": 1, "name": "b"},
                                          {"id": 9, "name": "c"}])
        assert [(r.id, r.name) for r in rows] == [(9, "a"), (1, "b"), (9, "c")]

    def test_duckdbs_writer_reads_it_through_the_shared_function(self):
        """Mutation: spell the row inline in DuckDB again — the two stores
        would then be one edit away from naming a manager differently."""
        from core.duckdb_store import DuckDBStore

        tree = ast.parse(textwrap.dedent(inspect.getsource(DuckDBStore.upsert_managers)))
        called = {getattr(n.func, "id", getattr(n.func, "attr", ""))
                  for n in ast.walk(tree) if isinstance(n, ast.Call)}
        assert "manager_rows" in called
        # Parsed, not grepped: the docstring names the constant on purpose.
        names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
        assert "RETAIL_MANAGER_IDS" not in names
        reads = [n for n in ast.walk(tree) if isinstance(n, ast.Constant)
                 and n.value == "full_name"]
        assert not reads, "the payload is read in DuckDB's writer again"


# ─── The stats body ─────────────────────────────────────────────────────────


class TestTheStatsBody:
    def test_one_text_for_both_engines(self):
        """The two renders differ only by the names and the date's spelling."""
        duck, pg = (sql_dialect.manager_stats_sql(DUCKDB),
                    sql_dialect.manager_stats_sql(POSTGRES))
        assert "UPDATE managers m" in duck and "FROM orders" in duck
        assert "UPDATE bronze.managers m" in pg and "FROM bronze.orders" in pg
        undone = (pg.replace("bronze.managers", "managers")
                    .replace("bronze.orders", "orders")
                    .replace(POSTGRES.kyiv_date("ordered_at"), DUCKDB.kyiv_date("ordered_at")))
        assert undone == duck

    @pytest.mark.parametrize("dialect", [DUCKDB, POSTGRES], ids=lambda d: d.name)
    def test_the_date_is_kyivs_in_both(self, dialect):
        """Mutation: `DATE(ordered_at)` / `ordered_at::date` — the process's
        or the session's timezone, a day early for every order between 00:00
        and 03:00 Kyiv."""
        sql = sql_dialect.manager_stats_sql(dialect)
        assert sql.count("timezone('Europe/Kyiv', ordered_at)") == 2
        assert "DATE(ordered_at)" not in sql and "ordered_at::date" not in sql


def _utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


T0 = _utc(2026, 10, 7, 9)


# ─── The chain: flag, preconditions, registry ───────────────────────────────


@pytest.fixture
def flags(monkeypatch):
    from core import pg_managers_write

    monkeypatch.delenv(pg_managers_write.WRITE_ENV, raising=False)
    monkeypatch.setattr(pg_managers_write, "_unmet_warned", {})
    return monkeypatch


@pytest.fixture
def every_precondition_met(flags):
    """The facts chain 5 waited on. Since step 14's S14-00 it waits on none:
    it writes Postgres whatever its flag says."""
    return flags


class TestTheFlag:


    def test_the_latch_outranks_any_flag(self, flags):
        from core import chain_latch, pg_managers_write

        flags.setenv(pg_managers_write.WRITE_ENV, "duckdb")
        chain_latch.latch(pg_managers_write.CHAIN, pg_managers_write.WRITE_ENV)
        assert pg_managers_write.writes_postgres() is True
        assert pg_managers_write.mode() == "postgres"
        assert pg_managers_write.reads_postgres() is True


# ─── The writers, as code ───────────────────────────────────────────────────


def _fn_tree(fn):
    return ast.parse(textwrap.dedent(inspect.getsource(fn)))


def _transaction_bodies(tree):
    return [n for n in ast.walk(tree) if isinstance(n, ast.AsyncWith)
            and any(isinstance(i.context_expr, ast.Call)
                    and getattr(i.context_expr.func, "attr", "") == "transaction"
                    for i in n.items)]


def _strings(node):
    return [n.value for n in ast.walk(node)
            if isinstance(n, ast.Constant) and isinstance(n.value, str)]


class TestTheUpsert:
    def test_is_retail_is_never_in_the_conflict_update(self):
        """Measured on 17.2 (design, M4): writing the seed in DO UPDATE puts it
        over every classification a human made. Mutation: add
        `is_retail = EXCLUDED.is_retail`."""
        import re

        from core import pg_managers_write

        (sql,) = [s for s in _strings(_fn_tree(pg_managers_write.upsert_managers))
                  if s.startswith("INSERT INTO bronze.managers")]
        head, _, update = sql.partition("DO UPDATE SET")
        assert "is_retail" in head, "the seed is inserted for a new manager"
        targets = re.findall(r"(\w+)\s*=\s*EXCLUDED\.\w+", update)
        assert targets == ["name", "email", "status", "mirrored_at"]

    def test_it_seeds_every_manager_without_an_interval_not_only_the_batch(self):
        """`_m0006`'s twin. Mutation: narrow the seed to the batch's ids — a
        manager the replica carried without a baseline would never get one."""
        from core import pg_managers_write

        (seed,) = [s for s in _strings(_fn_tree(pg_managers_write.upsert_managers))
                   if s.startswith("INSERT INTO app.manager_classifications")]
        assert "FROM bronze.managers m" in seed and "NOT EXISTS" in seed
        assert "$2" not in seed and "ANY(" not in seed
        assert "DATE '1970-01-01'" in seed


class TestTheDerivationSignal:
    def test_the_two_that_change_what_silver_reads_mark_inside_their_transaction(self):
        """Mutation: drop either mark."""
        from core import pg_managers_write

        for fn in (pg_managers_write.upsert_managers,
                   pg_managers_write.set_manager_retail_status):
            marks = [n for t in _transaction_bodies(_fn_tree(fn)) for n in ast.walk(t)
                     if isinstance(n, ast.Call) and getattr(n.func, "id", "") == "mark_if_owned"]
            assert marks, fn.__name__

    def test_the_stats_do_not(self):
        """Silver reads none of the three stats columns, so the stats owe no
        rebuild — the one daily Postgres Silver rebuild today's replicate
        causes goes. Mutation: mark there without revisiting this: a Silver
        that starts reading the columns must come back here."""
        from core import pg_managers_write
        from core.duckdb_store import silver_select_sql

        calls = {getattr(n.func, "id", getattr(n.func, "attr", ""))
                 for n in ast.walk(_fn_tree(pg_managers_write.update_manager_stats))
                 if isinstance(n, ast.Call)}
        assert "mark_if_owned" not in calls and "mark" not in calls
        import re

        silver = silver_select_sql()
        for column in ("first_order_date", "last_order_date", "order_count"):
            # `buyer_first_order_date` is the buyer's, not the manager's.
            assert not re.search(rf"\b{column}\b", silver), column


class TestOneLockForAllThree:
    WRITERS = ("upsert_managers", "update_manager_stats", "set_manager_retail_status")

    @pytest.mark.parametrize("name", WRITERS)
    def test_the_advisory_lock_comes_before_the_claim_and_every_table(self, name):
        """M5: without it two classifications of one manager leave two open
        intervals. Mutation: drop the lock, or take it after a statement."""
        from core import pg_managers_write

        tree = _fn_tree(getattr(pg_managers_write, name))
        (tx,) = _transaction_bodies(tree)
        order = []
        for n in ast.walk(tx):
            if isinstance(n, ast.Call):
                what = getattr(n.func, "attr", getattr(n.func, "id", ""))
                text = " ".join(_strings(n))
                if "pg_advisory_xact_lock" in text:
                    order.append(("lock", n.lineno))
                elif what == "claim":
                    order.append(("claim", n.lineno))
                elif "bronze." in text or "app." in text:
                    order.append(("table", n.lineno))
        order.sort(key=lambda o: o[1])
        assert order and order[0][0] == "lock", order
        locks = [n for n in ast.walk(tx) if isinstance(n, ast.Call)
                 and any(isinstance(a, ast.Name) and a.id == "CHAIN_LOCK_KEY"
                         for a in n.args)]
        assert locks, f"{name} does not lock CHAIN_LOCK_KEY"

    def test_the_key_is_no_other_chains(self):
        """Mutation: reuse chain 1's key — the two chains would serialise
        against each other for nothing."""
        import pkgutil
        import importlib

        import core
        from core import pg_managers_write

        keys = {}
        for info in pkgutil.iter_modules(core.__path__):
            if not info.name.startswith("pg_"):
                continue
            module = importlib.import_module(f"core.{info.name}")
            for attr in dir(module):
                if attr.endswith("LOCK_KEY") and isinstance(getattr(module, attr), int):
                    keys[f"{info.name}.{attr}"] = getattr(module, attr)
        mine = keys.pop("pg_managers_write.CHAIN_LOCK_KEY")
        assert mine == pg_managers_write.CHAIN_LOCK_KEY
        assert mine not in keys.values(), keys


# ─── The writers, against a recording connection ────────────────────────────


class _Tx:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        self.conn.calls.append(("BEGIN", None))
        return self

    async def __aexit__(self, exc_type, *exc):
        self.conn.calls.append(("ROLLBACK" if exc_type else "COMMIT", None))
        return False


class _PgConn:
    """What a writer asks of its connection, recorded; reads answer from
    `answers` by the first words of the statement."""

    def __init__(self, answers=None):
        self.calls = []
        self.answers = answers or {}

    def transaction(self, **kw):
        return _Tx(self)

    async def execute(self, sql, *args):
        self.calls.append((sql, args))
        return "UPDATE 3" if sql.lstrip().startswith("UPDATE") else "OK"

    async def executemany(self, sql, rows):
        self.calls.append((sql, list(rows)))

    def _answer(self, sql):
        for head, value in self.answers.items():
            if head in sql:
                return value
        return None

    async def fetchval(self, sql, *args):
        self.calls.append((sql, args))
        return self._answer(sql)

    async def fetchrow(self, sql, *args, **kw):
        self.calls.append((sql, args))
        return self._answer(sql)


class _PgPool:
    def __init__(self, conn):
        self.conn = conn
        self.acquired = 0

    def acquire(self, *args, **kwargs):
        pool = self

        class _Ctx:
            async def __aenter__(self_inner):
                pool.acquired += 1
                return pool.conn

            async def __aexit__(self_inner, *exc):
                return False
        return _Ctx()


@pytest.fixture
def pool(monkeypatch):
    from unittest.mock import AsyncMock

    conn = _PgConn()
    fake = _PgPool(conn)
    monkeypatch.setattr("core.pg.get_pool", AsyncMock(return_value=fake))
    monkeypatch.setattr("core.pg.require_revision", AsyncMock())
    return fake


def _row(mid, name="M", email=None, status="active", retail=False):
    return landing_rows.ManagerRow(mid, name, email, status, retail)


class TestWhatPostgresWouldRefuse:
    @pytest.mark.asyncio
    async def test_a_refused_manager_is_skipped_before_the_latch(self, pool):
        """Mutation: move the refusal after `_latch` — a first batch refused
        whole would latch the chain with no owner row behind it."""
        from core import chain_latch, pg_managers_write

        rows = [_row(1), _row(2, name="a\x00b"), _row(2 ** 31), _row(3, email="\ud800")]
        written = await pg_managers_write.upsert_managers(rows, set_at=T0)
        assert written == 1
        (inserted,) = [args for sql, args in pool.conn.calls
                       if sql.startswith("INSERT INTO bronze.managers")]
        assert [r[0] for r in inserted] == [1]
        assert chain_latch.latched(pg_managers_write.CHAIN)

    @pytest.mark.asyncio
    async def test_a_batch_of_nothing_but_refusals_raises_and_latches_nothing(self, pool):
        from core import chain_latch, pg_managers_write

        with pytest.raises(pg_managers_write.ManagersRefused):
            await pg_managers_write.upsert_managers(
                [_row(2 ** 31), _row(4, name=None)], set_at=T0)
        assert pool.acquired == 0
        assert not chain_latch.marker_path(pg_managers_write.CHAIN).exists()

    @pytest.mark.asyncio
    async def test_an_empty_batch_asks_nothing_and_latches_nothing(self, pool):
        from core import chain_latch, pg_managers_write

        assert await pg_managers_write.upsert_managers([], set_at=T0) == 0
        assert pool.acquired == 0
        assert not chain_latch.marker_path(pg_managers_write.CHAIN).exists()

    @pytest.mark.asyncio
    async def test_a_manager_served_twice_is_written_once_in_id_order(self, pool):
        from core import pg_managers_write

        await pg_managers_write.upsert_managers(
            [_row(9, name="old"), _row(1), _row(9, name="new")], set_at=T0)
        (inserted,) = [args for sql, args in pool.conn.calls
                       if sql.startswith("INSERT INTO bronze.managers")]
        assert [(r[0], r[1]) for r in inserted] == [(1, "M"), (9, "new")]

    @pytest.mark.asyncio
    async def test_the_copy_back_clock_is_the_callers(self, pool):
        """`set_at` is the web container's clock, never Postgres's `now()`."""
        from core import pg_managers_write

        await pg_managers_write.upsert_managers([_row(1)], set_at=T0)
        (seed,) = [args for sql, args in pool.conn.calls
                   if sql.startswith("INSERT INTO app.manager_classifications")]
        assert seed == (T0,)
        with pytest.raises(ValueError, match="set_at"):
            await pg_managers_write.upsert_managers([_row(1)], set_at=None)


class TestAClassificationThatCannotLand:
    @pytest.mark.asyncio
    async def test_a_backdate_behind_the_latest_change_is_refused_before_the_latch(self, pool):
        """OD-C5-1 (a). DuckDB writes two open intervals here (design, M1).
        Mutation: drop the guard."""
        from core import chain_latch, pg_managers_write

        pool.conn.answers = {"EXISTS": {"present": True, "latest": date(2026, 3, 1)}}
        with pytest.raises(pg_managers_write.BackdateBehindLatest) as raised:
            await pg_managers_write.set_manager_retail_status(
                7, True, date(2026, 1, 1), 1, None, set_at=T0)
        assert raised.value.latest == date(2026, 3, 1)
        assert not chain_latch.marker_path(pg_managers_write.CHAIN).exists()
        assert not [c for c in pool.conn.calls if c[0] == "BEGIN"]

    @pytest.mark.asyncio
    async def test_the_same_date_is_a_replacement_not_a_backdate(self, pool):
        from core import pg_managers_write

        pool.conn.answers = {"EXISTS": {"present": True, "latest": date(2026, 3, 1)},
                             "SELECT MAX(valid_from)": date(2026, 3, 1)}
        await pg_managers_write.set_manager_retail_status(
            7, True, date(2026, 3, 1), 1, "same day", set_at=T0)
        statements = [sql for sql, _a in pool.conn.calls]
        assert any(s.startswith("DELETE FROM app.manager_classifications") for s in statements)
        assert ("COMMIT", None) in pool.conn.calls

    @pytest.mark.asyncio
    async def test_a_manager_postgres_does_not_hold_is_refused_before_the_latch(self, pool):
        from core import chain_latch, pg_managers_write

        pool.conn.answers = {"EXISTS": {"present": False, "latest": None}}
        with pytest.raises(pg_managers_write.ManagerNotFound):
            await pg_managers_write.set_manager_retail_status(
                99, True, date(2026, 5, 1), 1, None, set_at=T0)
        assert not chain_latch.marker_path(pg_managers_write.CHAIN).exists()

    @pytest.mark.asyncio
    async def test_a_race_inside_the_lock_rolls_the_whole_write_back(self, pool):
        """A classification that landed between the read and the lock: the
        re-check inside raises, and the transaction rolls back."""
        from core import pg_managers_write

        pool.conn.answers = {"EXISTS": {"present": True, "latest": date(2026, 1, 1)},
                             "SELECT MAX(valid_from)": date(2026, 6, 1)}
        with pytest.raises(pg_managers_write.BackdateBehindLatest):
            await pg_managers_write.set_manager_retail_status(
                7, True, date(2026, 5, 1), 1, None, set_at=T0)
        assert ("ROLLBACK", None) in pool.conn.calls
        assert not [sql for sql, _a in pool.conn.calls
                    if sql.startswith(("DELETE", "UPDATE app", "UPDATE bronze"))]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("args", [
        (2 ** 31, True, date(2026, 5, 1), 1, None),
        (7, "yes", date(2026, 5, 1), 1, None),
        (7, True, None, 1, None),
        (7, True, date(2026, 5, 1), 1, "a\x00b"),
    ])
    async def test_what_postgres_would_refuse_is_refused_first(self, pool, args):
        """As `ClassificationRefused`, which the route answers 422: a bad
        request, not a Postgres that did not answer."""
        from core import pg_managers_write

        with pytest.raises(pg_managers_write.ClassificationRefused):
            await pg_managers_write.set_manager_retail_status(*args, set_at=T0)
        assert pool.acquired == 0


# ─── DuckDB stands down where the chain took over ───────────────────────────


class TestTheStoreRoutes:
    @pytest.mark.asyncio
    async def test_get_all_managers_reads_postgres_under_the_chain_and_never_duckdb(self, flags):
        """No fallback: DuckDB's managers are frozen after the flip.
        Mutation: drop the routing in `get_all_managers`."""
        from unittest.mock import AsyncMock, patch

        from core import chain_latch, pg_managers_write
        from core.duckdb_store import DuckDBStore

        chain_latch.latch(pg_managers_write.CHAIN)
        store = DuckDBStore(db_path="/nonexistent/never-opened.duckdb")
        rows = [{"id": 1, "name": "M", "synced_at": T0}]
        with patch("core.pg_managers_read.fetch_all_managers",
                   new=AsyncMock(return_value=rows)) as read:
            assert await store.get_all_managers() == rows
        read.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_classification_marks_whichever_engine_derives(self, flags):
        from unittest.mock import AsyncMock, patch

        from core import chain_latch, pg_managers_write
        from core.duckdb_store import DuckDBStore

        chain_latch.latch(pg_managers_write.CHAIN)
        store = DuckDBStore(db_path="/nonexistent/never-opened.duckdb")
        store.mark_warehouse_dirty = AsyncMock()
        with patch.object(pg_managers_write, "set_manager_retail_status",
                          new=AsyncMock()) as write:
            await store.set_manager_retail_status(7, True, date(2026, 5, 1), 3, "n")
        write.assert_awaited_once()
        assert write.await_args.args == (7, True, date(2026, 5, 1), 3, "n")
        assert write.await_args.kwargs["set_at"].tzinfo is not None
        store.mark_warehouse_dirty.assert_awaited_once_with(None)

    @pytest.mark.asyncio
    async def test_the_stats_and_the_upsert_route_too(self, flags):
        from unittest.mock import AsyncMock, patch

        from core import chain_latch, pg_managers_write
        from core.duckdb_store import DuckDBStore

        chain_latch.latch(pg_managers_write.CHAIN)
        store = DuckDBStore(db_path="/nonexistent/never-opened.duckdb")
        with patch.object(pg_managers_write, "update_manager_stats",
                          new=AsyncMock(return_value=4)), \
                patch.object(pg_managers_write, "upsert_managers",
                             new=AsyncMock(return_value=2)) as upsert, \
                patch("core.pg_replication.replicate_managers",
                      new=AsyncMock(side_effect=AssertionError("no replica"))):
            assert await store.update_manager_stats() == 4
            assert await store.upsert_managers(
                [{"id": 1, "name": "A"}, {"id": 2, "full_name": "B"}]) == 2
        rows = upsert.await_args.args[0]
        assert [tuple(r) for r in rows] == [(1, "A", None, None, False),
                                           (2, "B", None, None, False)]


# ─── The sync step: contained under the chain, untouched without it ────────


def _tick(monkeypatch, *, upsert=None, managers_due=True):
    """The REAL `SyncService.incremental_sync`, with the store and KeyCRM
    faked around it (tests/unit/test_buyers_step_containment.py's shape)."""
    from datetime import timedelta
    from unittest.mock import AsyncMock, MagicMock
    from zoneinfo import ZoneInfo

    from bot.config import DEFAULT_TIMEZONE
    from core import sync_service as mod
    from core.sync_service import SyncService

    now = datetime.now(ZoneInfo(DEFAULT_TIMEZONE))
    recent, stale = now - timedelta(minutes=1), now - timedelta(days=2)
    due = {"orders": recent, "products": recent, "buyers": recent,
           "managers": stale if managers_due else recent}

    store = MagicMock()
    store.get_last_sync_time = AsyncMock(side_effect=lambda k: due.get(k, stale))
    store.set_last_sync_time = AsyncMock()
    store.upsert_managers = upsert or AsyncMock(return_value=3)
    store.update_manager_stats = AsyncMock(return_value=3)
    store.mark_warehouse_dirty = AsyncMock()
    store.refresh_sku_inventory_status = AsyncMock(return_value=0)
    store.record_sku_inventory_snapshot = AsyncMock()
    store.record_inventory_snapshot = AsyncMock()

    async def users(*a, **kw):
        yield [{"id": 1, "name": "M"}]

    client = MagicMock()
    client.paginate = users
    monkeypatch.setattr(mod, "get_async_client", AsyncMock(return_value=client))

    svc = SyncService(store=store)
    svc._should_skip_sync = lambda: (False, None)
    svc._fetch_orders_with_date_filter = AsyncMock(return_value=[])
    svc.sync_offers = AsyncMock(return_value=5)
    svc.sync_stocks = AsyncMock(return_value=7)
    # Since step 14's S14-00 every chain writes Postgres, a constant; the
    # tick's other steps are faked on the store here, and keep its paths.
    from core import pg_buyers_write, pg_catalogue_write, pg_inventory_write, pg_orders_write
    from tests.conftest import route_chains_to_duckdb

    route_chains_to_duckdb(monkeypatch, pg_orders_write, pg_buyers_write,
                           pg_inventory_write, pg_catalogue_write)
    return svc, store


def _stamped(store, key="managers") -> bool:
    return any(c.args[:1] == (key,) for c in store.set_last_sync_time.await_args_list)


class TestTheManagersStep:
    @pytest.mark.asyncio
    async def test_under_the_chain_a_failure_is_contained_and_held(
            self, monkeypatch, every_precondition_met):
        """Mutation: remove the containment — the raise skipped the buyers,
        offers and stocks steps for the tick — or stamp on failure."""
        from unittest.mock import AsyncMock

        from core import pg_managers_write

        every_precondition_met.setenv(pg_managers_write.WRITE_ENV, "postgres")
        svc, store = _tick(monkeypatch,
                           upsert=AsyncMock(side_effect=OSError("postgres gone")))

        stats = await svc.incremental_sync()       # does not raise

        svc.sync_offers.assert_awaited_once()
        svc.sync_stocks.assert_awaited_once()
        assert not _stamped(store), "a failed step moved its watermark"
        assert stats["managers"] == 0
        assert all(isinstance(v, int) for v in stats.values()), stats
        health = svc.managers_step_health()
        assert (health["failures_since_ok"], health["last_error"]) == (1, "OSError")
        assert 590 <= health["retry_in_s"] <= 601

        # Inside the retry window nothing is asked of anyone.
        store.get_last_sync_time.reset_mock()
        await svc.incremental_sync()
        assert "managers" not in [c.args[0] for c in store.get_last_sync_time.await_args_list]
        assert store.upsert_managers.await_count == 1

    @pytest.mark.asyncio
    async def test_a_watermark_that_cannot_be_read_is_contained_too(
            self, monkeypatch, every_precondition_met):
        """Under the chain the getter reads `meta.chain_watermarks`."""
        from unittest.mock import AsyncMock

        from core import pg_managers_write

        every_precondition_met.setenv(pg_managers_write.WRITE_ENV, "postgres")
        svc, store = _tick(monkeypatch)
        others = store.get_last_sync_time.side_effect

        async def getter(key):
            if key == "managers":
                raise ConnectionError("pool gone")
            return others(key)

        store.get_last_sync_time = AsyncMock(side_effect=getter)
        await svc.incremental_sync()
        svc.sync_offers.assert_awaited()
        assert svc.managers_step_health()["last_error"] == "ConnectionError"


    @pytest.mark.asyncio
    async def test_a_success_under_the_chain_stamps_and_is_published(
            self, monkeypatch, every_precondition_met):
        from core import pg_managers_write

        every_precondition_met.setenv(pg_managers_write.WRITE_ENV, "postgres")
        svc, store = _tick(monkeypatch)
        stats = await svc.incremental_sync()
        assert stats["managers"] == 3 and _stamped(store)
        health = svc.managers_step_health()
        assert health["failures_since_ok"] == 0 and health["last_ok_at"]

    @pytest.mark.asyncio
    async def test_the_full_sync_is_not_taken_down_either(
            self, monkeypatch, every_precondition_met):
        """`full_sync` calls `sync_managers` first and directly, not through
        the tick's step: one classification store must not cost a week's
        orders. Mutation: let `sync_managers` re-raise under the chain."""
        from unittest.mock import AsyncMock

        from core import pg_managers_write

        every_precondition_met.setenv(pg_managers_write.WRITE_ENV, "postgres")
        svc, store = _tick(monkeypatch,
                           upsert=AsyncMock(side_effect=OSError("postgres gone")))
        assert await svc.sync_managers() == 0
        assert not _stamped(store)
        assert svc.managers_step_health()["last_error"] == "OSError"


# ─── Every read of the two tables follows the chain ─────────────────────────


def _read_walk():
    """`(routers that ask, [(file, line, call) a hole statement is handed to],
    [stray (file, line)])` over `core/` and `web/`.

    A statement is a string constant carrying one of the chain's holes, not in
    an f-string (the Silver CASE renders its names, it is not routed) and not a
    write: the stats UPDATE is the writer's own body. Chain 7a's walk
    (`test_goals_reads_follow_chain.py`) in miniature.
    """
    import pathlib

    from core import pg_managers_write

    repo = pathlib.Path(__file__).resolve().parents[2]
    holes = pg_managers_write.TABLE_HOLES
    trees = []
    for top in ("core", "web"):
        for path in sorted((repo / top).rglob("*.py")):
            rel = str(path.relative_to(repo))
            trees.append((rel, ast.parse(path.read_text(encoding="utf-8"))))

    def own_calls(fn):
        out, stack = set(), list(ast.iter_child_nodes(fn))
        while stack:
            node = stack.pop()
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
                continue
            if isinstance(node, ast.Call):
                out.add(getattr(node.func, "attr", getattr(node.func, "id", "")))
            stack.extend(ast.iter_child_nodes(node))
        return out

    routers = {fn.name for _r, tree in trees for fn in ast.walk(tree)
               if isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef))
               and "reads_the_chain" in own_calls(fn)}

    def is_statement(node):
        return (isinstance(node, ast.Constant) and isinstance(node.value, str)
                and any(h in node.value for h in holes)
                and not node.value.lstrip().upper().startswith(("UPDATE", "INSERT", "DELETE")))

    sites, stray = [], []
    for rel, tree in trees:
        in_fstrings = {id(v) for n in ast.walk(tree) if isinstance(n, ast.JoinedStr)
                       for v in ast.walk(n)}
        docs = {id(n.body[0].value) for n in ast.walk(tree)
                if isinstance(n, (ast.Module, ast.ClassDef, ast.FunctionDef,
                                  ast.AsyncFunctionDef))
                and n.body and isinstance(n.body[0], ast.Expr)
                and isinstance(n.body[0].value, ast.Constant)}
        constants = {}
        for node in tree.body:
            if isinstance(node, ast.Assign) and is_statement(node.value):
                for t in node.targets:
                    if isinstance(t, ast.Name):
                        constants[t.id] = node.value
        literals = [n for n in ast.walk(tree) if is_statement(n)
                    and id(n) not in in_fstrings and id(n) not in docs
                    and not (rel == "core/pg_managers_write.py")]
        handed = set()
        literal_ids = {id(lit) for lit in literals}
        for call in [n for n in ast.walk(tree) if isinstance(n, ast.Call)]:
            for arg in list(call.args) + [k.value for k in call.keywords]:
                # The statement as it is handed over: a literal, a module
                # constant, or either through a string method
                # (`_RETURN_ORDERS_SQL.replace(...)`).
                names = [n.id for n in ast.walk(arg)
                         if isinstance(n, ast.Name) and n.id in constants]
                if names or any(id(n) in literal_ids for n in ast.walk(arg)):
                    sites.append((rel, call.lineno,
                                  getattr(call.func, "attr", getattr(call.func, "id", ""))))
                    handed |= {id(n) for n in ast.walk(arg)}
                    handed |= {id(constants[name]) for name in names}
        for lit in literals:
            if id(lit) not in handed:
                stray.append((rel, lit.lineno))
    return routers, sites, stray


class TestEveryReadOfTheTablesFollowsTheChain:
    def test_the_walk_finds_the_reads_there_are(self):
        """Non-vacuity: the dashboard's returns list joins `{managers}`."""
        routers, sites, _stray = _read_walk()
        assert "_dashboard_run" in routers
        assert ("core/repositories/revenue.py", "_dashboard_run") in {
            (rel, name) for rel, _line, name in sites}

    def test_each_is_handed_to_a_router_that_asks(self):
        """Mutation: drop `reads_the_chain` from `_dashboard_run` — after the
        flip that join would name managers out of a DuckDB nothing writes."""
        routers, sites, _stray = _read_walk()
        wrong = [s for s in sites if s[2] not in routers]
        assert not wrong, wrong

    def test_none_is_left_where_the_walk_cannot_follow(self):
        _routers, _sites, stray = _read_walk()
        assert not stray, stray

    @pytest.mark.asyncio
    async def test_the_dashboard_join_reads_postgres_under_the_chain_without_a_fallback(
            self, flags):
        """`_RETURN_ORDERS_SQL` under the chain, with `KS_READ_DASHBOARD` at
        duckdb: Postgres, and a Postgres failure raises."""
        from unittest.mock import AsyncMock, patch

        from core import chain_latch, pg_managers_write
        from core.duckdb_store import DuckDBStore
        from core.repositories.revenue import _RETURN_ORDERS_SQL

        flags.setenv("KS_READ_DASHBOARD", "duckdb")
        chain_latch.latch(pg_managers_write.CHAIN)
        store = DuckDBStore(db_path="/nonexistent/never-opened.duckdb")
        with patch("core.pg_dashboard_read.fetch",
                   new=AsyncMock(return_value=[(1,)])) as fetch:
            assert await store._dashboard_run(_RETURN_ORDERS_SQL.replace(
                "{where}", "TRUE"), [5]) == [(1,)]
        assert "bronze.managers" in fetch.await_args.args[0]
        with patch("core.pg_dashboard_read.fetch",
                   new=AsyncMock(side_effect=OSError("down"))):
            with pytest.raises(OSError):
                await store._dashboard_run("SELECT name FROM {managers}", [])


# ─── The route ──────────────────────────────────────────────────────────────


class _RouteStore:
    def __init__(self, raises=None):
        self.raises = raises
        self.set_calls = []

    async def get_all_managers(self):
        return [{"id": 34, "name": "M", "is_retail": False}]

    async def set_manager_retail_status(self, manager_id, is_retail, **kw):
        self.set_calls.append((manager_id, is_retail, kw))
        if self.raises:
            raise self.raises


@pytest.fixture
def route(monkeypatch, flags):
    import time as _time

    from fastapi.testclient import TestClient

    from core.permissions import ADMIN_USER_IDS
    from web.main import app
    from web.routes.api._deps import limiter
    from web.routes.auth import SESSION_COOKIE, create_session_data, session_serializer

    limiter.reset()
    admin = sorted(ADMIN_USER_IDS)[0]
    client = TestClient(app)
    client.cookies.set(SESSION_COOKIE, session_serializer.dumps(create_session_data(
        {"id": str(admin), "first_name": "T", "last_name": "U", "username": "t",
         "auth_date": str(int(_time.time()))}, role="admin")))

    def with_store(store):
        async def get():
            return store
        monkeypatch.setattr("web.routes.api.admin.get_store", get)
        return client
    yield with_store
    limiter.reset()


class TestTheRoute:
    PATH = "/api/managers/34/retail-status?is_retail=true"


    def test_a_backdate_is_409_naming_the_latest_change(self, route, flags):
        from core import chain_latch, pg_managers_write

        chain_latch.latch(pg_managers_write.CHAIN)
        store = _RouteStore(pg_managers_write.BackdateBehindLatest(
            34, date(2026, 1, 1), date(2026, 3, 1)))
        res = route(store).post(self.PATH + "&effective_from=2026-01-01")
        assert res.status_code == 409 and "2026-03-01" in res.json()["detail"]

    def test_a_postgres_failure_under_the_chain_is_503(self, route, flags):
        from unittest.mock import AsyncMock, patch

        from core import chain_latch, pg_managers_write

        chain_latch.latch(pg_managers_write.CHAIN)
        store = _RouteStore(OSError("connection refused"))
        with patch("core.pg_replication.replicate_managers", new=AsyncMock()):
            res = route(store).post(self.PATH)
        assert res.status_code == 503 and "OSError" in res.json()["detail"]
        assert "connection refused" not in res.text


    def test_under_the_chain_the_replica_says_it_stood_down(self, route, flags, monkeypatch):
        from core import chain_latch, pg_managers_write

        monkeypatch.setenv("KS_MIRROR_LANDING", "on")
        chain_latch.latch(pg_managers_write.CHAIN)
        store = _RouteStore()
        res = route(store).post(self.PATH)
        assert res.status_code == 200, res.text
        assert "stood down" in (res.json()["replica"]["skipped"] or "")


# ─── The standing watch, judged ─────────────────────────────────────────────


def _facts(**managers):
    from core import pg_chain_invariants as inv

    base = dict(nulls=inv.Nulls(table="app.manager_classifications",
                                counts={"set_at": 0}),
                managers=40, intervals=44)
    base.update(managers)
    return inv.Facts(watched=("pg_managers_write",), now=T0,
                     managers=inv.Managers(**base))


class TestTheStandingWatch:
    def _checks(self, facts):
        from core import pg_chain_invariants as inv

        return {(i.check_name, i.severity.value): i
                for i in inv.check_chain_invariants(facts)}

    def test_a_clean_shape_is_quiet(self):
        assert self._checks(_facts()) == {}

    @pytest.mark.parametrize("field,check,severity", [
        ("open_wrong", "chain_manager_open_interval", "CRITICAL"),
        ("broken", "chain_manager_intervals_broken", "WARN"),
        ("disagree", "chain_manager_retail_disagrees", "WARN"),
    ])
    def test_each_shape_is_its_own_finding(self, field, check, severity):
        """Mutation: remove or invert any predicate."""
        found = self._checks(_facts(**{field: (4, 7)}))
        assert set(found) == {(check, severity)}
        assert found[(check, severity)].sample_ids == (4, 7)

    def test_an_empty_table_is_critical(self):
        assert ("chain_managers_empty", "CRITICAL") in self._checks(_facts(managers=0))

    def test_unclassified_is_judged_only_from_the_latch(self):
        """Before it the replica carries such managers until DuckDB's next
        boot (M2); after it the writer seeds in the same transaction."""
        assert self._checks(_facts(unclassified=(5,))) == {}
        found = self._checks(_facts(unclassified=(5,), latched_at=T0))
        assert set(found) == {("chain_manager_unclassified", "WARN")}

    def test_a_missing_set_at_is_the_required_column(self):
        from core import pg_chain_invariants as inv

        found = self._checks(_facts(nulls=inv.Nulls(
            table="app.manager_classifications", counts={"set_at": 2})))
        assert ("chain_required_column_null", "CRITICAL") in found

    def test_a_group_that_could_not_be_read_is_unwatched_and_holds(self):
        from core import pg_chain_invariants as inv

        facts = inv.Facts(watched=("pg_managers_write",), now=T0,
                          managers=inv.Unwatched("ConnectionError: gone"))
        issues = inv.check_chain_invariants(facts)
        assert [i.check_name for i in issues] == [inv.UNWATCHED]
        assert set(inv.unverified_conditions(issues)) >= {
            inv.MANAGERS_EMPTY, inv.MANAGER_OPEN_INTERVAL, inv.MANAGER_UNCLASSIFIED,
            inv.MANAGER_INTERVALS_BROKEN, inv.MANAGER_RETAIL_DISAGREES}

    def test_every_condition_is_in_the_registry_and_has_a_lever(self):
        from core import pg_chain_invariants as inv
        from core.alerting import REGISTRY
        from core.data_quality import REMEDIATION

        levers = {prefix for prefix, _line in REMEDIATION}
        for name in (inv.MANAGERS_EMPTY, inv.MANAGER_OPEN_INTERVAL,
                     inv.MANAGER_UNCLASSIFIED, inv.MANAGER_INTERVALS_BROKEN,
                     inv.MANAGER_RETAIL_DISAGREES):
            assert name in REGISTRY and name in levers and name.startswith(inv.PREFIX)


# ─── The preflight and /api/health ──────────────────────────────────────────


_CLEAN_SHAPE = {"managers": 40, "intervals": 44, "unclassified": [], "open_wrong": [],
                "broken": [], "disagree": [], "set_at_null": 0, "orphans": []}


class TestThePreflight:


    @pytest.mark.asyncio
    async def test_the_question_is_over_once_the_chain_writes_postgres(self, flags):
        from core import chain_latch, pg_managers_write

        chain_latch.latch(pg_managers_write.CHAIN)
        assert await pg_managers_write.preflight() == {"ok": None, "reasons": []}


class TestHealthPublishesIt:
    @pytest.mark.asyncio
    async def test_preflight_and_sync_step_sit_under_chain_5s_entry(self, flags, monkeypatch):
        from unittest.mock import AsyncMock

        from core import pg_inventory_write, pg_managers_write
        from web.routes.api import health

        health._managers_preflight_cache.update(data=None, expires_at=0)
        health._preflight_cache.update(data=None, expires_at=0)
        monkeypatch.setattr(pg_managers_write, "preflight", AsyncMock(
            return_value={"ok": False, "reasons": ["step13: x"]}))
        monkeypatch.setattr(pg_inventory_write, "preflight", AsyncMock(
            return_value={"ok": True, "reasons": []}))
        monkeypatch.setattr(health, "_managers_sync_step", AsyncMock(
            return_value={"failures_since_ok": 0}))
        try:
            block = await health._write_chains_block()
        finally:
            health._managers_preflight_cache.update(data=None, expires_at=0)
            health._preflight_cache.update(data=None, expires_at=0)
        entry = block["pg_managers_write"]
        assert entry["preflight"] == {"ok": False, "reasons": ["step13: x"]}
        assert entry["sync_step"] == {"failures_since_ok": 0}
        assert entry["mode"] == "postgres" and entry["unmet_precondition"] is None
        assert block["pg_inventory_write"]["preflight"]["ok"] is True
        assert "preflight" not in block["pg_goals_write"]

    @pytest.mark.asyncio
    async def test_the_two_preflights_cost_one_bound_not_two(self, flags, monkeypatch):
        """Concurrently: two hung preflights in a row would cost the canary's
        whole HEALTH_TIMEOUT_S."""
        import asyncio
        import time as _time

        from core import pg_inventory_write, pg_managers_write
        from web.routes.api import health

        async def hangs(*a, **kw):
            await asyncio.sleep(60)

        health._managers_preflight_cache.update(data=None, expires_at=0)
        health._preflight_cache.update(data=None, expires_at=0)
        monkeypatch.setattr(pg_managers_write, "preflight", hangs)
        monkeypatch.setattr(pg_inventory_write, "preflight", hangs)
        monkeypatch.setattr(health, "_PREFLIGHT_TIMEOUT_S", 0.3)
        started = _time.monotonic()
        try:
            block = await health._write_chains_block()
        finally:
            health._managers_preflight_cache.update(data=None, expires_at=0)
            health._preflight_cache.update(data=None, expires_at=0)
        assert _time.monotonic() - started < 0.55
        assert "did not answer" in block["pg_managers_write"]["preflight"]["reasons"][0]


# ─── The review of chain 5 (2026-10-08) ─────────────────────────────────────


class TestTheRouteAnswersWhatHappened:
    PATH = TestTheRoute.PATH

    def test_a_manager_gone_between_the_list_and_the_write_is_404(self, route, flags):
        """Mutation: drop the `ManagerNotFound` mapping — a 503 naming
        Postgres for a manager that is not there."""
        from core import chain_latch, pg_managers_write

        chain_latch.latch(pg_managers_write.CHAIN)
        store = _RouteStore(pg_managers_write.ManagerNotFound("manager 34"))
        res = route(store).post(self.PATH)
        assert res.status_code == 404 and "34" in res.json()["detail"]

    def test_a_refusal_postgres_never_saw_is_422_not_503(self, route, flags, monkeypatch):
        """A note carrying NUL, refused by the writer before Postgres is asked
        (the review's reproduction answered 503 "Postgres did not answer").
        Handed to the writer here, because a NUL in the query string no longer
        reaches the route: the edge answers it 400 (the test below).
        Mutation: drop the route's `ClassificationRefused` mapping."""
        from unittest.mock import AsyncMock

        from core import chain_latch, pg_managers_write

        chain_latch.latch(pg_managers_write.CHAIN)
        get_pool = AsyncMock(side_effect=AssertionError("Postgres was asked"))
        monkeypatch.setattr("core.pg.get_pool", get_pool)

        class _Writing(_RouteStore):
            async def set_manager_retail_status(self, manager_id, is_retail, **kw):
                await pg_managers_write.set_manager_retail_status(
                    manager_id, is_retail, kw["effective_from"], kw["set_by"],
                    "a\x00b", set_at=T0)

        res = route(_Writing()).post(self.PATH + "&effective_from=2026-10-07&note=ok")
        assert res.status_code == 422, res.text
        assert "note carries NUL" in res.json()["detail"]
        assert "a\x00b" not in res.text
        get_pool.assert_not_awaited()

    def test_a_nul_in_the_query_string_is_refused_at_the_edge(self, route, flags, monkeypatch):
        """`RejectNulMiddleware` answers it 400 before routing: neither the
        writer nor Postgres is asked."""
        from unittest.mock import AsyncMock

        from core import chain_latch, pg_managers_write

        chain_latch.latch(pg_managers_write.CHAIN)
        get_pool = AsyncMock(side_effect=AssertionError("Postgres was asked"))
        monkeypatch.setattr("core.pg.get_pool", get_pool)

        class _Unreached(_RouteStore):
            async def set_manager_retail_status(self, manager_id, is_retail, **kw):
                raise AssertionError("the writer was asked")

        res = route(_Unreached()).post(self.PATH + "&effective_from=2026-10-07&note=a%00b")
        assert res.status_code == 400, res.text
        assert res.json()["detail"] == "query: Must not contain a NUL character"
        get_pool.assert_not_awaited()


# ─── Nothing reads the frozen DuckDB copies by name ─────────────────────────

# DuckDB's own `managers` and `manager_classifications` spelled bare in a
# statement — a read or a write — outside the `{managers}`/`{classifications}`
# holes `_read_walk` follows. Not after a `.`: `bronze.managers` is Postgres's.
_BARE = r"\b(?:FROM|JOIN|UPDATE|INTO)\s+(?:managers|manager_classifications)(?![.\w])"

# Every function that names them, with the function that routes it and the
# question that function asks: the DuckDB half of a store method behind the
# chain's own answer, the boot seed behind the latch, and the replica's read
# behind the stand-down. A new site fails until it is added here with its
# router — the review's probe (`FROM managers` in a new module) is one.
_BARE_SITES = {
    ("core/duckdb_store.py", "upsert_managers"): ("upsert_managers", "writes_postgres"),
    ("core/duckdb_store.py", "set_manager_retail_status"):
        ("set_manager_retail_status", "writes_postgres"),
    ("core/duckdb_store.py", "get_all_managers"): ("get_all_managers", "reads_postgres"),
    ("core/migrations.py", "_m0006_seed_manager_classifications"):
        ("_m0006_seed_manager_classifications", "latched"),
    ("core/pg_replication.py", "read_managers"): ("replicate_managers", "tables_stood_down"),
}


def _bare_walk():
    """`({(file, function) naming a bare table}, {(file, function): its
    calls}, {called name: {(file, function) calling it}})` over `core/`,
    `web/`, `scripts/`, `bot/` and `deploy/`. A module-level constant counts
    at every function of its module that loads it."""
    import pathlib
    import re

    repo = pathlib.Path(__file__).resolve().parents[2]
    bare = re.compile(_BARE, re.I)
    found, calls, callers = set(), {}, {}
    for top in ("core", "web", "scripts", "bot", "deploy"):
        for path in sorted((repo / top).rglob("*.py")):
            rel = str(path.relative_to(repo))
            tree = ast.parse(path.read_text(encoding="utf-8"))
            docs = {id(n.body[0].value) for n in ast.walk(tree)
                    if isinstance(n, (ast.Module, ast.ClassDef, ast.FunctionDef,
                                      ast.AsyncFunctionDef))
                    and n.body and isinstance(n.body[0], ast.Expr)
                    and isinstance(n.body[0].value, ast.Constant)}
            functions = [n for n in ast.walk(tree)
                         if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]
            owner = {}
            for fn in functions:
                for node in ast.walk(fn):
                    owner.setdefault(id(node), fn.name)
            for fn in functions:
                names = {getattr(c.func, "attr", getattr(c.func, "id", ""))
                         for c in ast.walk(fn) if isinstance(c, ast.Call)}
                calls.setdefault((rel, fn.name), set()).update(names)
                for name in names:
                    callers.setdefault(name, set()).add((rel, fn.name))
            constants = {}
            for node in ast.walk(tree):
                if not (isinstance(node, ast.Constant) and isinstance(node.value, str)
                        and id(node) not in docs and bare.search(node.value)):
                    continue
                if id(node) in owner:
                    found.add((rel, owner[id(node)]))
                    continue
                for stmt in tree.body:
                    if isinstance(stmt, ast.Assign) and any(
                            n is node for n in ast.walk(stmt.value)):
                        constants.update({t.id: node for t in stmt.targets
                                          if isinstance(t, ast.Name)})
                else:
                    found.add((rel, "<module>"))
            for fn in functions:
                if any(isinstance(n, ast.Name) and n.id in constants
                       for n in ast.walk(fn)):
                    found.add((rel, fn.name))
    return found, calls, callers


class TestNothingReadsTheFrozenCopiesByName:
    def test_every_site_is_known(self):
        """The review's finding: a statement written `FROM managers` instead
        of `FROM {managers}` passed every walk. Mutation: add one."""
        found, _calls, _callers = _bare_walk()
        assert found == set(_BARE_SITES), sorted(found ^ set(_BARE_SITES))

    def test_each_is_routed_by_the_chain(self):
        """Mutation: drop the routing from any of them — `get_all_managers`'
        `reads_postgres()`, say."""
        _found, calls, callers = _bare_walk()
        for (rel, function), (router, asks) in _BARE_SITES.items():
            assert asks in calls[(rel, router)], (rel, router, asks)
            if function != router:
                assert callers[function] == {(rel, router)}, (function, callers[function])

    def test_it_sees_a_statement_written_bare(self):
        """Non-vacuity, on the review's own probe."""
        import re

        bare = re.compile(_BARE, re.I)
        for sql, seen in (("SELECT id, name, is_retail FROM managers ORDER BY id", True),
                          ("SELECT manager_id FROM manager_classifications", True),
                          ("SELECT 1 FROM orders o JOIN managers m ON m.id = o.manager_id", True),
                          ("SELECT 1 FROM bronze.managers", False),
                          ("SELECT 1 FROM {managers}", False),
                          ("'baseline frozen from managers.is_retail'", False)):
            assert bool(bare.search(sql)) is seen, sql
