"""A failed buyer selection holds the watermark and costs nothing else."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest


class TestAFailedSelectionIsContained:
    @pytest.mark.asyncio
    async def test_it_returns_without_moving_the_watermark(self):
        from core.sync_service import SyncService

        store = MagicMock()
        store.get_missing_buyer_ids = AsyncMock(side_effect=OSError("pool closed"))
        store.set_last_sync_time = AsyncMock()

        assert await SyncService(store=store).sync_missing_buyers() == 0
        store.set_last_sync_time.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_empty_selection_still_moves_it(self):
        """The control: "nothing missing" is a completed sync and is stamped."""
        from core.sync_service import SyncService

        store = MagicMock()
        store.get_missing_buyer_ids = AsyncMock(return_value=[])
        store.set_last_sync_time = AsyncMock()

        assert await SyncService(store=store).sync_missing_buyers() == 0
        store.set_last_sync_time.assert_awaited_once_with("buyers")

    def test_an_unknown_engine_raises(self, monkeypatch):
        from core import pg_buyer_sync_read
        monkeypatch.setenv("KS_READ_BUYER_SYNC", "postgre")
        with pytest.raises(ValueError):
            pg_buyer_sync_read.enabled()


class TestTheTiebreak:
    def test_the_order_ends_on_the_id(self):
        """Structural, for the chat tools' reason: a two-engine test of a tie
        passes whenever both engines happen to emit level rows in the same
        order. The ORDER BY is what makes the cut at LIMIT deterministic."""
        import ast
        import inspect
        import textwrap

        from core.duckdb_store import DuckDBStore

        tree = ast.parse(textwrap.dedent(inspect.getsource(DuckDBStore.get_missing_buyer_ids)))
        doc = ast.get_docstring(tree.body[0], clean=False)
        sql = "".join(n.value for n in ast.walk(tree)
                      if isinstance(n, ast.Constant) and isinstance(n.value, str)
                      and n.value != doc)
        order_by = sql[sql.index("ORDER BY"):sql.index("LIMIT")]
        assert order_by.rstrip().endswith("buyer_id DESC"), order_by


class TestUnderChain4TheSelectionFollowsTheWrites:
    """Once the buyers are written in Postgres, DuckDB's copy has stopped, and
    a selection read from it would ask KeyCRM for the same buyers every hour
    for ever. The chain's own answer decides, before the read switch — whose
    typo must not stop the step once it is not the one deciding."""

    READERS = ("KS_SMS_STORE", "KS_READ_SEARCH_INDEX", "KS_READ_DASHBOARD")

    @pytest.fixture
    def store(self, tmp_path):
        """A real store whose DuckDB fails the test if the selection reads it."""
        from core.duckdb_store import DuckDBStore

        s = DuckDBStore(db_path=tmp_path / "selection.duckdb")

        def refused():
            raise AssertionError("the selection read DuckDB")

        s.connection = refused
        return s

    @pytest.fixture
    def fetch(self, monkeypatch):
        from core import pg_buyer_sync_read

        fake = AsyncMock(return_value=[(7,), (5,)])
        monkeypatch.setattr(pg_buyer_sync_read, "fetch", fake)
        return fake

    def _flip(self, monkeypatch):
        monkeypatch.setenv("KS_WRITE_BUYERS", "postgres")
        for reader in self.READERS:
            monkeypatch.setenv(reader, "postgres")

    @pytest.mark.asyncio
    async def test_a_flagged_chain_selects_from_postgres(self, monkeypatch, store, fetch):
        self._flip(monkeypatch)
        monkeypatch.delenv("KS_READ_BUYER_SYNC", raising=False)
        assert await store.get_missing_buyer_ids(10) == [7, 5]
        (sql, params), _ = fetch.await_args
        assert "bronze.buyers" in sql and params == [10]

    @pytest.mark.asyncio
    async def test_the_read_switchs_typo_does_not_stop_it(self, monkeypatch, store, fetch):
        """Decision 11: `KS_READ_BUYER_SYNC` is not what decides under the
        chain, so a value nobody can read there must not raise."""
        self._flip(monkeypatch)
        monkeypatch.setenv("KS_READ_BUYER_SYNC", "postgre")
        assert await store.get_missing_buyer_ids(10) == [7, 5]

    @pytest.mark.asyncio
    async def test_a_chain_flag_nobody_can_read_selects_from_postgres(
            self, monkeypatch, store, fetch):
        monkeypatch.setenv("KS_WRITE_BUYERS", "postgre")
        assert await store.get_missing_buyer_ids(10) == [7, 5]

    @pytest.mark.asyncio
    async def test_a_latched_chain_routes_both_halves_with_the_flag_off(
            self, monkeypatch, store, fetch):
        """Carried from PR-2's review (#13): the copy-back's post-latch levers
        say the buyers step fetches a buyer Postgres lacks "through the chain"
        and sync-all rewrites buyers "in Postgres after the latch". True only
        while a latched chain routes the selection AND the write to Postgres,
        whatever the flag says — pinned here so the levers cannot outlive the
        routing they describe."""
        from core import chain_latch, pg_buyers_write
        from core.models import Buyer

        chain_latch.latch(pg_buyers_write.CHAIN, pg_buyers_write.WRITE_ENV)
        monkeypatch.setenv("KS_WRITE_BUYERS", "duckdb")
        assert await store.get_missing_buyer_ids(10) == [7, 5]
        writer = AsyncMock(return_value=1)
        monkeypatch.setattr(pg_buyers_write, "upsert_buyers", writer)
        assert await store.upsert_buyers([Buyer.from_api({"id": 1, "full_name": "Олена"})]) == 1
        writer.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_with_the_chain_on_duckdb_the_read_switch_still_decides(
            self, monkeypatch, fetch, tmp_path):
        """The control: nothing moved, so today's routing — and its typo — stand."""
        from core.duckdb_store import DuckDBStore

        monkeypatch.delenv("KS_WRITE_BUYERS", raising=False)
        monkeypatch.setenv("KS_READ_BUYER_SYNC", "postgre")
        s = DuckDBStore(db_path=tmp_path / "control.duckdb")
        with pytest.raises(ValueError):
            await s.get_missing_buyer_ids(10)
        fetch.assert_not_awaited()


class TestAChainFlagNobodyCanReadFetchesNothing:
    @pytest.mark.asyncio
    async def test_the_step_refuses_before_kycrm_is_asked(self, monkeypatch):
        """`POST /duckdb/sync-buyers` calls the step directly; with a flag
        nobody can read the write would raise only after up to 500 GETs."""
        from core import sync_service as mod
        from core.sync_service import SyncService

        monkeypatch.setenv("KS_WRITE_BUYERS", "postgre")
        client = AsyncMock()
        monkeypatch.setattr(mod, "get_async_client", AsyncMock(return_value=client))
        store = MagicMock()
        store.get_missing_buyer_ids = AsyncMock(return_value=[1])
        store.set_last_sync_time = AsyncMock()
        svc = SyncService(store=store)

        assert await svc.sync_missing_buyers() == 0
        store.get_missing_buyer_ids.assert_not_awaited()
        client.fetch_buyers_by_ids.assert_not_awaited()
        store.set_last_sync_time.assert_not_awaited()
        state = svc.buyer_sync_state
        assert state.last_error_class == "RuntimeError"
        assert state.consecutive_failures == 1, "a writer with nowhere to write is a failure"


class TestASkippedBuyerIsCounted:
    @pytest.mark.asyncio
    async def test_the_step_publishes_how_many_postgres_refused(self, monkeypatch):
        from core import sync_service as mod
        from core.sync_service import SyncService

        class _B:
            def __init__(self, i):
                self.id, self.birthday = i, None

        client = MagicMock()
        client.fetch_buyers_by_ids = AsyncMock(return_value=[_B(1), _B(2)])
        monkeypatch.setattr(mod, "get_async_client", AsyncMock(return_value=client))

        async def upsert(buyers, *, skipped_out=None):
            skipped_out.append(2)
            return 1

        store = MagicMock()
        store.get_missing_buyer_ids = AsyncMock(return_value=[1, 2])
        store.set_last_sync_time = AsyncMock()
        store.upsert_buyers = upsert
        svc = SyncService(store=store)

        assert await svc.sync_missing_buyers() == 1
        block = svc.buyer_sync_state.published(retry_in_s=None)
        assert block["last_skipped_bad"] == 1 and block["last_written"] == 1
        store.set_last_sync_time.assert_awaited_once_with("buyers")
