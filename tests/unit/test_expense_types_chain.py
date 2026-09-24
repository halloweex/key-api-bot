"""Chain 6a, the expense-type dictionary, without a database (DN-26).

The real-Postgres half is `tests/integration/test_expense_types_writer.py` and
it proves the statements; this proves the routing around them: that the flag
off changes nothing, that the flag on hands the writer the rows the shared
parse produced and never touches DuckDB, that a fault in this chain stops this
chain and not the full sync it rides in, and that the standing watch, the
shipper, the comparison and the copy-back all see the chain.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from core import chain_latch, pg_chain_invariants as inv, pg_expense_types_write as chain
from core import sync_service as sync_module
from core import write_chains
from core.landing_rows import expense_type_rows

PAYLOAD = [
    {"id": 1, "name": "Доставка", "alias": "delivery", "is_active": True},
    {"id": 2, "name": "dictionaries.expense_types.commission", "alias": "bank_commission"},
    {"id": 3, "name": "dictionaries.expense_types.packing_materials", "alias": None,
     "is_active": False},
]


@pytest.fixture
def flags(monkeypatch):
    """No chain's variable set — and the read flag chain 6a needs before its
    own can move anything already on, so a test that sets
    KS_WRITE_EXPENSE_TYPES=postgres means it. `TestTheReadFlagComesFirst`
    takes it away."""
    for c in write_chains.WRITE_CHAINS:
        monkeypatch.delenv(c.WRITE_ENV, raising=False)
    monkeypatch.setenv("KS_READ_EXPENSES", "postgres")
    return monkeypatch


class _NoDuckDB:
    """A store whose DuckDB must not be reached."""

    def connection(self):
        raise AssertionError("the dictionary reached DuckDB under KS_WRITE_EXPENSE_TYPES=postgres")


class TestTheRepositoryRoutes:
    @pytest.mark.asyncio
    async def test_under_the_flag_the_writer_gets_the_parsed_rows_and_duckdb_nothing(self, flags):
        from core.repositories.expenses import ExpensesMixin

        flags.setenv(chain.WRITE_ENV, "postgres")
        writer = AsyncMock(return_value=3)
        with patch.object(chain, "upsert_expense_types", new=writer):
            written = await ExpensesMixin.upsert_expense_types(_NoDuckDB(), PAYLOAD)

        assert written == 3
        (rows,), _ = writer.await_args
        # The names the shared parse resolves, never the payload's keys: the
        # routing is AFTER `expense_type_rows`, so both stores get these.
        assert rows == expense_type_rows(PAYLOAD)
        assert [r.name for r in rows] == ["Доставка", "Bank Commission", "Packing Materials"]

    @pytest.mark.asyncio
    async def test_with_the_flag_off_the_postgres_writer_is_never_called(self, flags):
        from core.repositories.expenses import ExpensesMixin

        class _Store:
            def __init__(self):
                self.statements = []

            def connection(self):
                store = self

                class _Ctx:
                    async def __aenter__(self_inner):
                        class _Conn:
                            def execute(self_c, sql, params=None):
                                store.statements.append((sql.strip().split()[0], params))
                        return _Conn()

                    async def __aexit__(self_inner, *exc):
                        return False
                return _Ctx()

        store = _Store()
        writer = AsyncMock(side_effect=AssertionError("Postgres written with the flag off"))
        with patch.object(chain, "upsert_expense_types", new=writer):
            assert await ExpensesMixin.upsert_expense_types(store, PAYLOAD) == 3
        inserts = [p for verb, p in store.statements if verb == "INSERT"]
        assert inserts == [list(r) for r in expense_type_rows(PAYLOAD)]

    @pytest.mark.asyncio
    async def test_an_empty_payload_writes_nowhere_and_latches_nothing(self, flags):
        flags.setenv(chain.WRITE_ENV, "postgres")
        assert await chain.upsert_expense_types([]) == 0
        assert not chain_latch.latched(chain.CHAIN)


class TestTheFlag:
    def test_off_by_default(self, flags):
        assert chain.writes_postgres() is False

    def test_an_unknown_value_raises(self, flags):
        flags.setenv(chain.WRITE_ENV, "postgrse")
        with pytest.raises(RuntimeError, match=chain.WRITE_ENV):
            chain.writes_postgres()

    def test_the_latch_outranks_it(self, flags):
        chain_latch.latch(chain.CHAIN, chain.WRITE_ENV)
        flags.setenv(chain.WRITE_ENV, "duckdb")
        assert chain.writes_postgres() is True


class TestTheRegistrySeesIt:
    def test_the_table_stands_down_under_the_flag_and_only_then(self, flags):
        assert "bronze.expense_types" not in write_chains.stood_down_tables()
        flags.setenv(chain.WRITE_ENV, "postgres")
        assert write_chains.stood_down_tables() == frozenset({"bronze.expense_types"})

    def test_the_watermark_is_this_chains_and_moves_with_it(self, flags):
        assert write_chains.chain_for_sync_key("last_sync_expense_types") is chain
        flags.setenv(chain.WRITE_ENV, "postgres")
        assert write_chains.stood_down_sync_keys() == frozenset({"last_sync_expense_types"})

    def test_the_table_is_one_the_shipper_replaces_and_the_comparison_reads(self):
        """Standing down a table neither touches would be a no-op that reads as
        a guarantee."""
        from core.mirror_reconciliation import OPERATIONAL_TABLES
        from core.pg_operational import _FULL_REPLACE

        assert set(chain.CHAIN_TABLES) <= {pg for pg, _d, _c, _o in _FULL_REPLACE}
        assert set(chain.CHAIN_TABLES) <= {s.pg_table for s in OPERATIONAL_TABLES}

    def test_a_typo_in_it_stands_down_only_its_own_table(self, flags):
        flags.setenv(chain.WRITE_ENV, "yes")
        tables, errors = write_chains.stood_down_tables_checked()
        assert tables == frozenset({"bronze.expense_types"})
        assert list(errors) == [chain.CHAIN]


class TestTheCopyBackKnowsIt:
    def test_one_full_replace_spec_with_no_clock(self):
        """Derived from the shipper and the comparison, never written out. No
        shared clock: DuckDB's `synced_at` and Postgres's `mirrored_at` are
        each store's own bookkeeping, so after the latch a difference is taken
        as Postgres being newer."""
        from core import chain_transfer

        (spec,) = chain_transfer.chain_specs(chain)
        assert (spec.pg_table, spec.dk_table, spec.is_append, spec.clock) == (
            "bronze.expense_types", "expense_types", False, ())
        assert chain_transfer.chain_sequences(chain) == ()

    def test_the_operator_types_its_short_name(self):
        from core.chain_transfer import resolve_chain

        assert resolve_chain("expense_types") is chain

    def test_the_runbook_sends_it_to_its_own_tables_not_inventorys(self):
        from core.chain_transfer import _runbook

        said = " ".join(_runbook(chain, executed=True, released=True))
        assert "KS_WRITE_EXPENSE_TYPES=duckdb" in said
        assert "bronze.expense_types" in said
        assert "I1" not in said and "E1" not in said


# ─── full_sync: a fault in this chain stops this chain ─────────────────────


class _Client:
    async def paginate(self, endpoint, params=None, page_size=50):
        if endpoint == "order/expense-type":
            yield PAYLOAD
        return


class _SyncStore:
    """Everything `full_sync` asks of the store, recorded; the dictionary's
    write raises what the test says."""

    def __init__(self, raises: BaseException):
        self.raises = raises
        self.stamped = []

    async def upsert_categories(self, rows):
        return len(rows)

    async def upsert_expense_types(self, rows):
        raise self.raises

    async def upsert_products(self, rows):
        return len(rows)

    async def set_last_sync_time(self, key, timestamp=None):
        self.stamped.append(key)

    async def refresh_sku_inventory_status(self):
        return 0

    async def record_sku_inventory_snapshot(self):
        return False

    async def refresh_warehouse_layers(self, trigger=None):
        return None

    async def get_latest_order_time(self):
        return datetime(2026, 9, 20, tzinfo=timezone.utc)

    async def checkpoint(self):
        return None


async def _full_sync(store):
    service = sync_module.SyncService(store)
    service.sync_managers = AsyncMock(return_value=0)
    service.sync_offers = AsyncMock(return_value=0)
    service.sync_stocks = AsyncMock(return_value=0)
    service._fetch_orders_with_date_filter = AsyncMock(return_value=[])

    async def _client():
        return _Client()

    with patch.object(sync_module, "get_async_client", new=_client), \
         patch.object(sync_module, "mirror_categories", new=AsyncMock()), \
         patch.object(sync_module, "mirror_products", new=AsyncMock()):
        return await service.full_sync(days_back=30)


class TestFullSyncContainsThisChainOnly:
    @pytest.mark.asyncio
    async def test_under_the_flag_a_postgres_failure_leaves_the_watermark_and_the_sync_goes_on(
        self, flags,
    ):
        flags.setenv(chain.WRITE_ENV, "postgres")
        store = _SyncStore(ConnectionRefusedError("postgres is down"))

        stats = await _full_sync(store)

        assert "ConnectionRefusedError" in stats["expense_types_error"]
        assert "expense_types" not in store.stamped, "the watermark moved over nothing"
        # Products and orders still synced: one dictionary did not cost them.
        assert store.stamped == ["categories", "products", "orders"]

    @pytest.mark.asyncio
    async def test_a_flag_nobody_can_read_is_contained_the_same_way(self, flags):
        """DN-01's rule on the one path the registry does not cover: a typo in
        KS_WRITE_EXPENSE_TYPES raises inside the repository, and must not stop
        the orders of a full sync — or, on an empty DuckDB, the boot sync."""
        flags.setenv(chain.WRITE_ENV, "postgrse")
        store = _SyncStore(RuntimeError(
            "KS_WRITE_EXPENSE_TYPES='postgrse' is not understood"))

        stats = await _full_sync(store)

        assert "postgrse" in stats["expense_types_error"]
        assert store.stamped == ["categories", "products", "orders"]

    @pytest.mark.asyncio
    async def test_with_the_flag_off_a_failure_raises_exactly_as_it_always_has(self, flags):
        store = _SyncStore(RuntimeError("duckdb said no"))

        with pytest.raises(RuntimeError, match="duckdb said no"):
            await _full_sync(store)
        assert store.stamped == ["categories"]


# ─── the standing watch (DN-07) ──────────────────────────────────────────────


class TestTheStandingWatch:
    def test_the_chain_has_a_reader(self):
        assert inv._reader_groups()[chain.CHAIN] == "expense_types"

    def test_a_full_dictionary_of_display_names_is_clean(self):
        facts = inv.Facts(watched=(chain.CHAIN,),
                          expense_types=inv.ExpenseTypes(rows=27))
        assert inv.check_chain_invariants(facts) == []

    def test_an_empty_dictionary_is_critical(self):
        facts = inv.Facts(watched=(chain.CHAIN,),
                          expense_types=inv.ExpenseTypes(rows=0))
        (issue,) = inv.check_chain_invariants(facts)
        assert (issue.check_name, issue.severity.value) == (inv.DICTIONARY_EMPTY, "CRITICAL")
        assert "Other" in issue.description

    def test_a_localisation_key_standing_as_a_name_is_a_warning_with_ids(self):
        facts = inv.Facts(watched=(chain.CHAIN,), expense_types=inv.ExpenseTypes(
            rows=27, unresolved=2, unresolved_sample=(4, 9)))
        (issue,) = inv.check_chain_invariants(facts)
        assert (issue.check_name, issue.severity.value) == (inv.NAME_UNRESOLVED, "WARN")
        assert issue.sample_ids == (4, 9) and issue.count == 2

    def test_an_unreadable_dictionary_is_unwatched_not_clean(self):
        facts = inv.Facts(watched=(chain.CHAIN,),
                          expense_types=inv.Unwatched("relation does not exist"))
        (issue,) = inv.check_chain_invariants(facts)
        assert issue.check_name == inv.UNWATCHED and chain.CHAIN in issue.description

    def test_both_conditions_are_held_by_a_blind_run(self):
        assert {inv.DICTIONARY_EMPTY, inv.NAME_UNRESOLVED} <= set(inv.CONDITIONS)
        assert all(c.startswith(inv.PREFIX) for c in (inv.DICTIONARY_EMPTY, inv.NAME_UNRESOLVED))


class _Conn:
    def __init__(self):
        self.asked = []

    async def fetch(self, sql, keys):
        self.asked.append(tuple(keys))
        return [{"key": k, "value": "2026-09-13T02:05:00+03:00"} for k in keys]


class TestItsWatermarkIsNotJudgedAtNinetyMinutes:
    """`last_sync_expense_types` moves on the Sunday full sync. At the
    inventory chain's 90 minutes it would page every run of the week."""

    def test_the_chain_opts_out_and_the_others_keep_the_default(self):
        from core import pg_expenses_write, pg_inventory_write

        assert inv.watermark_limit_min(chain) is None
        assert inv.watermark_limit_min(pg_inventory_write) == inv.WATERMARK_MAX_AGE_MIN
        assert inv.watermark_limit_min(pg_expenses_write) == inv.WATERMARK_MAX_AGE_MIN

    @pytest.mark.asyncio
    async def test_its_key_is_not_read_while_chain_1s_still_are(self):
        from core import pg_inventory_write

        conn = _Conn()
        now = datetime(2026, 9, 20, 12, tzinfo=timezone.utc)
        marks = await inv._read_watermarks(
            conn, {chain.CHAIN: None, pg_inventory_write.CHAIN: None}, now)

        assert conn.asked == [tuple(pg_inventory_write.CHAIN_SYNC_KEYS)]
        assert {m.key for m in marks} == set(pg_inventory_write.CHAIN_SYNC_KEYS)
        assert all(m.limit_min == inv.WATERMARK_MAX_AGE_MIN for m in marks)

    @pytest.mark.asyncio
    async def test_alone_it_reads_nothing_at_all(self):
        conn = _Conn()
        now = datetime(2026, 9, 20, 12, tzinfo=timezone.utc)
        assert await inv._read_watermarks(conn, {chain.CHAIN: None}, now) == ()
        assert conn.asked == []

    def test_the_verdict_uses_the_limit_each_mark_carries(self):
        """Carried with the reading, so a chain that declares its own number
        is judged by it and not by chain 1's."""
        mark = inv.WatermarkAge("some_chain", "last_sync_x", "2026-09-20T00:00:00+00:00",
                                age_s=150 * 60, limit_min=180)
        assert inv._watermark_issues((mark,)) == []
        (issue,) = inv._watermark_issues(
            (inv.WatermarkAge("some_chain", "last_sync_x", "x", age_s=150 * 60),))
        assert issue.check_name == inv.WATERMARK_STALE
        assert f"{inv.WATERMARK_MAX_AGE_MIN}-minute" in issue.description


class TestTheFreshnessCheckBridgesTheWeekAfterTheFlip:
    """The key moves on the Sunday full sync, so after a mid-week flip
    `meta.chain_watermarks` has none for up to a week. It used to read as
    "entity never synced" on every integrity run until Sunday (review of
    DN-26); DuckDB's frozen stamp now stands in, at the same 192 h."""

    NOW = datetime(2026, 9, 24, 9, 0, tzinfo=timezone.utc)   # a Thursday

    def _duckdb(self, expense_types_age_h):
        import duckdb
        from datetime import timedelta

        conn = duckdb.connect()
        conn.execute("CREATE TABLE sync_metadata "
                     "(key VARCHAR PRIMARY KEY, value VARCHAR, updated_at TIMESTAMP)")
        # Everything else fresh, so the only thing that can be said is about
        # the dictionary.
        for entity in ("orders", "products", "buyers", "offers", "stocks",
                       "managers", "categories"):
            conn.execute("INSERT INTO sync_metadata VALUES (?, ?, now())",
                         [f"last_sync_{entity}", (self.NOW - timedelta(minutes=5)).isoformat()])
        if expense_types_age_h is not None:
            conn.execute("INSERT INTO sync_metadata VALUES (?, ?, now())",
                         ["last_sync_expense_types",
                          (self.NOW - timedelta(hours=expense_types_age_h)).isoformat()])
        return conn

    def _judge(self, flags, conn, marks):
        from core.data_quality import _freshness_check

        flags.setenv(chain.WRITE_ENV, "postgres")
        return [(i.check_name, i.severity.value, i.count, i.description)
                for i in _freshness_check(conn, self.NOW, chain_watermarks=marks)]

    def test_the_chain_declares_it_and_chain_1_does_not(self):
        from core import pg_inventory_write

        assert chain.CHAIN_WATERMARK_INHERITS_DUCKDB is True
        assert not getattr(pg_inventory_write, "CHAIN_WATERMARK_INHERITS_DUCKDB", False)

    def test_a_mid_week_flip_is_quiet_about_a_dictionary_synced_on_sunday(self, flags):
        # 103 h: Sunday 02:00 Kyiv to Thursday 09:00 UTC. `{}` is what
        # `read_values` returns for a key Postgres does not hold yet.
        assert self._judge(flags, self._duckdb(103), {}) == []

    def test_the_frozen_stamp_still_pages_at_the_same_threshold(self, flags):
        """A first sync under the flag that failed must page when a failed
        DuckDB one would have — the stand-in is not a pass."""
        (issue,) = self._judge(flags, self._duckdb(200), {})
        name, severity, count, description = issue
        assert (name, severity, count) == ("freshness_expense_types", "WARN", 200)
        assert "threshold 192" in description and "before the switch" in description

    def test_once_postgres_holds_the_key_it_wins(self, flags):
        """The stand-in is only for an absent key: a stale Postgres stamp
        beside a fresh DuckDB one is a stall."""
        from datetime import timedelta

        stale = (self.NOW - timedelta(hours=200)).isoformat()
        (issue,) = self._judge(flags, self._duckdb(10),
                               {"last_sync_expense_types": stale})
        assert issue[0] == "freshness_expense_types" and issue[2] == 200
        assert "before the switch" not in issue[3]

    def test_with_no_stamp_anywhere_it_is_still_never_synced(self, flags):
        (issue,) = self._judge(flags, self._duckdb(None), {})
        assert issue[0] == "freshness_expense_types"
        assert "never synced" in issue[3]


class TestTheReadFlagComesFirst:
    """`KS_WRITE_EXPENSE_TYPES=postgres` without `KS_READ_EXPENSES=postgres`
    used to write Postgres while every reader of the dictionary read DuckDB,
    so a type KeyCRM added after the flip showed as "Other" and nothing said
    so (review of DN-26). DN-27's rule now: the chain runs as duckdb and says
    why."""

    def _off(self, flags, value=None):
        flags.setenv(chain.WRITE_ENV, "postgres")
        if value is None:
            flags.delenv("KS_READ_EXPENSES", raising=False)
        else:
            flags.setenv("KS_READ_EXPENSES", value)

    def test_the_chain_stays_on_duckdb_and_every_consumer_agrees(self, flags):
        self._off(flags)
        state = write_chains.chain_modes()[chain.CHAIN]

        assert chain.writes_postgres() is False
        assert state["mode"] == "duckdb" and state["error"] is None
        assert "KS_READ_EXPENSES" in state["unmet_precondition"]
        # The hourly copy keeps shipping the table DuckDB still writes, and
        # the watermark stays where the writer puts it.
        assert "bronze.expense_types" not in write_chains.stood_down_tables()
        assert "last_sync_expense_types" not in write_chains.stood_down_sync_keys()
        assert chain.CHAIN not in inv.watched_chains()

    def test_a_read_flag_nobody_can_parse_is_unmet_and_raises_nothing(self, flags):
        self._off(flags, "postgrse")
        assert chain.writes_postgres() is False
        state = write_chains.chain_modes()[chain.CHAIN]
        assert state["mode"] == "duckdb"
        assert "not understood" in state["unmet_precondition"]

    def test_with_the_read_flag_on_nothing_is_unmet(self, flags):
        flags.setenv(chain.WRITE_ENV, "postgres")
        assert chain.unmet_precondition() is None
        assert chain.writes_postgres() is True
        assert write_chains.chain_modes()[chain.CHAIN]["unmet_precondition"] is None

    def test_a_latched_chain_keeps_writing_postgres_and_says_its_readers_are_not(self, flags):
        """OD-19 (a): the latch is not undone by a read flag either — routing
        back would start a second writer. What changes is that it is said."""
        chain_latch.latch(chain.CHAIN, chain.WRITE_ENV)
        self._off(flags)
        state = write_chains.chain_modes()[chain.CHAIN]
        assert chain.writes_postgres() is True
        assert state["mode"] == "postgres" and state["mismatch"] is False
        assert "KS_READ_EXPENSES" in state["unmet_precondition"]

    def test_with_the_write_flag_off_it_is_not_even_asked(self, flags):
        flags.delenv("KS_READ_EXPENSES", raising=False)
        assert write_chains.chain_modes()[chain.CHAIN]["unmet_precondition"] is None

    @pytest.mark.parametrize("write", [None, "duckdb", "postgres"])
    @pytest.mark.parametrize("read", [None, "duckdb", "postgres", "yes"])
    @pytest.mark.parametrize("latched", [False, True])
    def test_the_registry_and_the_writer_give_one_answer(self, flags, write, read, latched):
        """Two homes for the rule — `_chain_state` and `writes_postgres` — so
        the whole matrix is asked of both. A disagreement is the shipper
        overwriting what the writer just wrote, once an hour."""
        for env, value in ((chain.WRITE_ENV, write), ("KS_READ_EXPENSES", read)):
            if value is None:
                flags.delenv(env, raising=False)
            else:
                flags.setenv(env, value)
        if latched:
            chain_latch.latch(chain.CHAIN, chain.WRITE_ENV)
        mode = write_chains.chain_modes()[chain.CHAIN]["mode"]
        assert mode == ("postgres" if chain.writes_postgres() else "duckdb")

    @pytest.mark.asyncio
    async def test_the_repository_writes_duckdb_and_the_page_sees_the_new_type(
        self, flags, tmp_path,
    ):
        """The review's reproduction, run to the reader: a type added after
        the flip reaches the store the page reads."""
        from core.duckdb_store import DuckDBStore

        store = DuckDBStore(db_path=tmp_path / "held.duckdb")
        await store.connect()
        try:
            flags.delenv("KS_READ_EXPENSES", raising=False)
            await store.upsert_expense_types([{"id": 1, "name": "Delivery"}])
            flags.setenv(chain.WRITE_ENV, "postgres")
            writer = AsyncMock(side_effect=AssertionError(
                "Postgres written while the page reads DuckDB"))
            with patch.object(chain, "upsert_expense_types", new=writer):
                await store.upsert_expense_types([{"id": 1, "name": "Delivery"},
                                                  {"id": 32, "name": "Новий тип"}])
            assert [r["id"] for r in await store.get_expense_types()] == [1, 32]
            assert not chain_latch.latched(chain.CHAIN)
        finally:
            await store.close()


class TestTheRegistryAsksAChainsPrecondition:
    def test_a_chain_that_names_none_has_none(self):
        from core import pg_expenses_write

        assert write_chains._unmet_precondition(pg_expenses_write) is None

    def test_a_check_that_raises_is_unmet_rather_than_raising(self):
        import types

        def _boom():
            raise RuntimeError("cannot tell")

        fake = types.SimpleNamespace(unmet_precondition=_boom)
        assert "cannot tell" in write_chains._unmet_precondition(fake)


class TestTheCanaryWarnsOnIt:
    def _block(self, **state):
        base = {"env": chain.WRITE_ENV, "mode": "duckdb", "error": None,
                "latched": False, "latched_at": None, "mismatch": False,
                "unmet_precondition": None}
        base.update(state)
        return {"write_chains": {chain.CHAIN: base}}

    def test_held_on_duckdb_is_named_as_held(self):
        from bot import canary

        (key, message), = canary.check_write_chain_precondition(
            self._block(unmet_precondition="KS_READ_EXPENSES is not postgres"))
        assert key == "write_chain_precondition_unmet"
        assert "held on DuckDB" in message and "KS_READ_EXPENSES" in message

    def test_latched_it_says_the_writes_go_to_postgres(self):
        from bot import canary

        (_key, message), = canary.check_write_chain_precondition(self._block(
            mode="postgres", latched=True,
            unmet_precondition="KS_READ_EXPENSES is not postgres"))
        assert "writes Postgres" in message

    def test_nothing_unmet_and_an_older_web_are_quiet(self):
        from bot import canary

        assert canary.check_write_chain_precondition(self._block()) == []
        assert canary.check_write_chain_precondition({}) == []
        stale = self._block()
        del stale["write_chains"][chain.CHAIN]["unmet_precondition"]
        assert canary.check_write_chain_precondition(stale) == []

    @pytest.mark.asyncio
    async def test_run_canary_warns_and_names_the_lever(self):
        import httpx
        from datetime import timedelta

        from bot import canary
        from tests.unit.test_canary import DASHBOARD, _healthy_payload, _mock_transport

        payload = _healthy_payload()
        payload.update(self._block(unmet_precondition="KS_READ_EXPENSES is not postgres"))
        future = datetime.now(timezone.utc) + timedelta(days=60)
        cert = {"notAfter": future.strftime("%b %d %H:%M:%S %Y GMT")}
        async with _mock_transport(lambda request: httpx.Response(200, json=payload)) as client:
            with patch.object(canary, "_fetch_peer_cert", return_value=cert):
                result = await canary.run_canary(DASHBOARD, client=client)

        assert result.severity == "warn"
        assert "write_chain_precondition_unmet" in result.failure_keys
        assert "read flag" in canary.format_alert(result, DASHBOARD)

    def test_health_publishes_it(self, flags):
        from web.routes.api.health import _write_chains

        flags.setenv(chain.WRITE_ENV, "postgres")
        flags.delenv("KS_READ_EXPENSES", raising=False)
        assert "KS_READ_EXPENSES" in _write_chains()[chain.CHAIN]["unmet_precondition"]
