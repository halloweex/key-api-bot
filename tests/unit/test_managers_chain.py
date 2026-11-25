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

from core import landing_rows


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


# ─── The stats body ─────────────────────────────────────────────────────────


def _utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


T0 = _utc(2026, 10, 7, 9)


# ─── The chain: flag, preconditions, registry ───────────────────────────────


@pytest.fixture
def flags(monkeypatch):
    from core import pg_managers_write

    monkeypatch.delenv(pg_managers_write.WRITE_ENV, raising=False)
    return monkeypatch


@pytest.fixture
def every_precondition_met(flags):
    """The facts chain 5 waited on. Since step 14's S14-00 it waits on none:
    it writes Postgres whatever its flag says."""
    return flags


# `TestTheFlag` stood here until step 14's S14-11: the latch outranking any
# flag in `writes_postgres()`, `mode()` and `reads_postgres()`, which went
# with `KS_WRITE_MANAGERS`.


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
    # The other chains' steps, each the one entry the tick reaches it by
    # since step 14's S14-11 (every chain writes Postgres, asking nothing).
    svc._orders_step = AsyncMock(return_value=[])
    svc._catalogue_step_postgres = AsyncMock()
    svc._inventory_step_postgres = AsyncMock()
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

        svc._inventory_step_postgres.assert_awaited_once()
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
        svc._inventory_step_postgres.assert_awaited()
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
        from core import chain_latch, pg_managers_write

        chain_latch.latch(pg_managers_write.CHAIN)
        store = _RouteStore(OSError("connection refused"))
        res = route(store).post(self.PATH)
        assert res.status_code == 503 and "OSError" in res.json()["detail"]
        assert "connection refused" not in res.text


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
