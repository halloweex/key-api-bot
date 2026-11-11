"""Chain 3: the orders writer, its decision and the paths that reach it.

`core/pg_orders_write.py` writes the orders, their line items, their expenses
and the backfill-miss ledger to Postgres once `KS_WRITE_ORDERS=postgres` and
every precondition holds (OD-13 (a)). Production has the flag off; what is
pinned here is that off means today's behaviour, that on means Postgres and
nothing else, and that the flag cannot move the writes before step 13,
chain 1 and the backups say it may (the goal bridge went in chain 7b-4).
Against a real Postgres the writer is proved in
`tests/integration/test_orders_chain_writer.py`.

Each test names the mutation it exists to fail on.

Since step 14's S14-00 the chain writes Postgres whatever its flag says, with
no precondition and no start hold, so the tests of the flag off, of a typo,
of each precondition and of the hold went with them. S14-09 deleted those
answers, the preflight and DuckDB's own order writers: DuckDB's decision is
pinned as a literal of what it answered (`TestTheDecisionIsDuckDBs`), and what
stays is the writer, its decision, what Postgres would refuse, the paths that
reach it and the order step's containment.
"""
from __future__ import annotations

import asyncio
import itertools
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

from core import chain_latch, pg_orders_write as pow_, write_chains
from core.landing_rows import landed_orders

UTC = timezone.utc
T0 = datetime(2026, 9, 20, 10, 0, tzinfo=UTC)


def _payload(oid, *, updated=T0, comment=None, total="100.00", products=1,
             expenses=(), ordered=T0, source=1):
    return {
        "id": oid, "source_id": source, "status_id": 12, "status_group_id": 4,
        "grand_total": total, "ordered_at": ordered.isoformat() if ordered else None,
        "created_at": T0.isoformat(),
        "updated_at": updated.isoformat() if updated else None,
        "buyer": {"id": 500 + oid}, "manager": {"id": 4},
        "manager_comment": comment, "promocode": None,
        "products": [{"name": f"Товар {i}", "quantity": 1, "price_sold": "50.00",
                      "offer": {"product_id": 700 + i}} for i in range(products)],
        "expenses": [{"id": e, "expense_type_id": 1, "amount": "10.00",
                      "status": "paid"} for e in expenses],
    }


@pytest.fixture
def flags(monkeypatch):
    for chain in write_chains.WRITE_CHAINS:
        monkeypatch.delenv(chain.WRITE_ENV, raising=False)
    return monkeypatch


@pytest.fixture
def met(flags):
    """Every precondition held. Since step 14's S14-00 there is none to hold:
    the chain writes Postgres whatever its flag says."""
    return flags


# ─── G1: the flag, the latch, the declaration ────────────────────────────────


class TestTheRoutingIsGone:
    """S14-09 deleted what routed the chain: the flag's answer as a decision,
    the preconditions, the start hold, the preflight, `mode()` and DuckDB's
    own order writers. Kills: any of them put back, which would make a later
    reader believe the chain could still be on DuckDB."""

    @pytest.mark.parametrize("name", [
        "unmet_precondition", "unmet_reasons", "settle_hold", "_held", "HELD_KEY",
        "preflight", "LOCKOUT_REVISION", "mode", "ChainOwnsOrders",
        "UNMET_WARN_EVERY_S", "_unmet_warned", "_warn_unmet", "_BACKFILLED"])
    def test_the_chain_module_carries_none_of_it(self, name):
        assert not hasattr(pow_, name)

    @pytest.mark.parametrize("name", ["upsert_orders", "upsert_expenses_batch"])
    def test_the_store_has_no_order_writer_of_its_own(self, name):
        from core.duckdb_store import DuckDBStore

        assert not hasattr(DuckDBStore, name)

    @pytest.mark.parametrize("name", ["writes_postgres", "env_writes_postgres"])
    def test_and_s14_11_took_what_stayed(self, name):
        """`writes_postgres` stayed for the registry's sync-key routing and
        `env_writes_postgres` for its published `error` until S14-11, which
        deleted both with the flags."""
        assert not hasattr(pow_, name)


class TestTheDeclaration:
    def test_the_four_tables_and_the_two_keys(self):
        """Mutation: drop `app.order_backfill_misses` — `replicate_operational`
        would go on replacing it out of a frozen DuckDB every hour, rolling
        back every miss the chain recorded. And the search index's Postgres
        cursor (OD-15, `TestTheSearchCursorMovesWithTheChain`)."""
        assert pow_.CHAIN == "pg_orders_write"
        assert pow_.WRITE_ENV == "KS_WRITE_ORDERS"
        assert pow_.CHAIN_TABLES == ("bronze.orders", "bronze.order_products",
                                     "bronze.expenses", "app.order_backfill_misses")
        assert pow_.CHAIN_SYNC_KEYS == ("last_sync_orders", "last_sync_meilisearch_pg")
        assert pow_.CHAIN_WATERMARK_MAX_AGE_MIN is None
        # The order step's window starts at this key (the chain-3 review;
        # `TestTheFirstTickAfterTheFlip`), kept by S14-09
        # (`tests/unit/test_absent_chain_keys.py`).
        assert pow_.CHAIN_WATERMARK_INHERITS_DUCKDB is True

    def test_the_archive_is_not_a_chain_table(self):
        """`app.order_versions` has one writer in both modes and no DuckDB
        copy (declaring it made the copy-back's `chain_specs` raise, until
        step 14's S14-04 deleted it)."""
        assert "app.order_versions" not in pow_.CHAIN_TABLES
        assert "bronze.expense_types" not in pow_.CHAIN_TABLES   # chain 6a's

    def test_registered_and_found_by_its_sync_key(self):
        """It was resolved by its short name too, by the copy-back that step
        14's S14-04 deleted."""
        assert pow_ in write_chains.WRITE_CHAINS
        assert write_chains.chain_for_sync_key("last_sync_orders") is pow_


# ─── G2: the preconditions ───────────────────────────────────────────────────


# ─── G3: the decision is DuckDB's ────────────────────────────────────────────


@pytest_asyncio.fixture
async def duck(tmp_path, flags):
    from core.duckdb_store import DuckDBStore

    store = DuckDBStore()
    flags.setenv("KS_MIRROR_LANDING", "0")
    yield store


class TestTheDecisionIsDuckDBs:
    """`decide` against what `DuckDBStore.upsert_orders` itself answered, on
    a grid of stored and incoming stamps, force and skip_products. Until step
    14's S14-09 the grid ran DuckDB's writer beside the decider; S14-09
    deleted the writer, and `DUCKDB_ANSWERED` is its answer on this grid,
    recorded on the commit before (`d544144c`), where both were asserted
    equal in the same run (rule 9) — written, skipped as unchanged, or
    deferred to a full writer. Mutations: drop the header-only deferral;
    compare without reading a naive stamp as UTC; write a stored order whose
    payload has no stamp."""

    STORED = [None, T0 - timedelta(hours=1), T0, T0 + timedelta(hours=1)]
    INCOMING = [T0, None]
    # (stored index, incoming index, force_update, skip_products) → verdict.
    DUCKDB_ANSWERED = {
        (0, 0, False, False): 'written',
        (0, 0, False, True): 'deferred',
        (0, 0, True, False): 'written',
        (0, 0, True, True): 'deferred',
        (0, 1, False, False): 'written',
        (0, 1, False, True): 'deferred',
        (0, 1, True, False): 'written',
        (0, 1, True, True): 'deferred',
        (1, 0, False, False): 'written',
        (1, 0, False, True): 'written',
        (1, 0, True, False): 'written',
        (1, 0, True, True): 'written',
        (1, 1, False, False): 'skipped',
        (1, 1, False, True): 'skipped',
        (1, 1, True, False): 'skipped',
        (1, 1, True, True): 'skipped',
        (2, 0, False, False): 'skipped',
        (2, 0, False, True): 'skipped',
        (2, 0, True, False): 'written',
        (2, 0, True, True): 'written',
        (2, 1, False, False): 'skipped',
        (2, 1, False, True): 'skipped',
        (2, 1, True, False): 'skipped',
        (2, 1, True, True): 'skipped',
        (3, 0, False, False): 'skipped',
        (3, 0, False, True): 'skipped',
        (3, 0, True, False): 'skipped',
        (3, 0, True, True): 'skipped',
        (3, 1, False, False): 'skipped',
        (3, 1, False, True): 'skipped',
        (3, 1, True, False): 'skipped',
        (3, 1, True, True): 'skipped',
    }

    def test_the_grid_is_whole(self):
        assert set(self.DUCKDB_ANSWERED) == set(itertools.product(
            range(len(self.STORED)), range(len(self.INCOMING)), [False, True], [False, True]))

    @pytest.mark.parametrize("case", sorted(DUCKDB_ANSWERED))
    def test_on_a_grid(self, case):
        si, ii, force, skip = case
        oid = 1000
        stored = self.STORED[si]
        existing = {} if stored is None else {oid: stored}
        rows = landed_orders([_payload(oid, updated=self.INCOMING[ii])],
                             with_products=not skip).orders
        to_write, skipped, deferred = pow_.decide(
            rows, existing, force_update=force, skip_products=skip)
        verdict = ("written" if to_write else "skipped" if skipped
                   else "deferred" if deferred else "nothing")
        assert (len(to_write), len(skipped), deferred) in ((1, 0, 0), (0, 1, 0), (0, 0, 1))
        assert verdict == self.DUCKDB_ANSWERED[case], (stored, self.INCOMING[ii], force, skip)

    def test_a_payload_without_a_stamp_never_rewrites_a_stored_order(self):
        """DuckDB reads the missing stamp as pandas' NaT, which no comparison
        passes — under force too. Mutation: follow `should_update_order`'s
        own None rule, which would rewrite the order."""
        (row,) = landed_orders([_payload(1, updated=None)]).orders
        for force in (False, True):
            to_write, skipped, _d = pow_.decide(
                [row], {1: T0}, force_update=force, skip_products=False)
            assert (to_write, skipped) == ([], [1])
        to_write, _s, _d = pow_.decide([row], {1: None}, force_update=False,
                                       skip_products=False)
        assert [r.id for r in to_write] == [1]

    def test_a_naive_stamp_is_read_as_utc(self):
        rows = landed_orders([_payload(1, updated=T0)]).orders
        naive = (T0 + timedelta(minutes=1)).replace(tzinfo=None)
        _w, skipped, _d = pow_.decide(rows, {1: naive}, force_update=False,
                                      skip_products=False)
        assert skipped == [1]


# ─── G4: what Postgres would refuse is refused before the latch ─────────────


class TestWhatPostgresWouldRefuse:
    def _row(self, **changes):
        (row,) = landed_orders([_payload(1)]).orders
        return row._replace(**changes)

    @pytest.mark.parametrize("changes,why", [
        ({"manager_comment": "utm\x00x"}, "NUL"),
        ({"promocode": "\ud800"}, "UTF-8"),
        ({"grand_total": 1e10}, "NUMERIC"),
        ({"id": 2 ** 31}, "range"),
        ({"source_id": None}, "NULL"),
        ({"status_id": None}, "NULL"),
        ({"buyer_id": "7"}, "not an integer"),
    ])
    def test_an_order(self, changes, why):
        assert why in pow_.order_refusal(self._row(**changes))

    def test_a_clean_order_is_not_refused(self):
        assert pow_.order_refusal(self._row()) is None

    def test_a_line_item_id_is_a_bigint(self):
        (_o,), (p,) = landed_orders([_payload(1)])
        assert pow_.product_refusal(p._replace(id=2 ** 40)) is None
        assert "NUL" in pow_.product_refusal(p._replace(name="x\x00"))

    def test_an_expense(self):
        from core.landing_rows import expense_rows

        (e,) = expense_rows([_payload(1, expenses=[9])])
        # Text, as KeyCRM may serve it: both stores take a numeric string.
        assert e.amount == "10.00" and pow_.expense_refusal(e) is None
        assert "not a number" in pow_.expense_refusal(e._replace(amount="ten"))
        assert "NULL" in pow_.expense_refusal(e._replace(amount=None))
        assert "NUMERIC" in pow_.expense_refusal(e._replace(amount=Decimal("1e12")))

    @pytest.mark.asyncio
    async def test_a_batch_refused_whole_never_reaches_postgres(self, met):
        """Refused before the latch, so a first batch Postgres would refuse
        whole leaves no marker behind (chain 4's reason). Mutation: drop the
        NUL rule — the batch reaches the pool."""
        met.setenv("KS_WRITE_ORDERS", "postgres")
        with patch("core.pg.get_pool", new=AsyncMock(side_effect=AssertionError("reached"))):
            result, expenses = await pow_.upsert_orders_with_expenses(
                [_payload(1, comment="a\x00b")])
        assert result.failed == 1 and result.count == 0 and expenses == 0
        assert not chain_latch.marker_path(pow_.CHAIN).exists()


# ─── G5: the routing ─────────────────────────────────────────────────────────


class TestTheRouting:
    @pytest.mark.asyncio
    async def test_the_sync_s_one_write_goes_to_postgres(self, met, duck):
        """The sync's one order write is the chain's, and DuckDB is handed
        nothing. Mutation: hand `_upsert_orders_with_expenses` anything but
        the chain's writer."""
        from core.pg_orders_write import UpsertResult
        from core.sync_service import SyncService

        met.setenv("KS_WRITE_ORDERS", "postgres")
        writer = AsyncMock(return_value=(UpsertResult(
            count=1, changed_ids=[1], skipped_unchanged=0, failed=0), 1))
        met.setattr(pow_, "upsert_orders_with_expenses", writer)
        out: list = []
        counts = await SyncService(duck)._upsert_orders_with_expenses(
            [_payload(1, expenses=[9])], force_update=True, skip_products=True,
            changed_ids_out=out)
        assert counts == (1, 1) and out == [1]
        writer.assert_awaited_once()
        assert writer.await_args.kwargs == {"force_update": True, "skip_products": True}

    @pytest.mark.asyncio
    async def test_the_misses_go_where_the_scans_read_them(self, met, duck):
        met.setenv("KS_WRITE_ORDERS", "postgres")
        writer = AsyncMock(return_value=2)
        met.setattr(pow_, "record_backfill_misses", writer)
        assert await duck.record_backfill_misses({1: "gone", 2: "empty"}) == 2
        writer.assert_awaited_once_with({1: "gone", 2: "empty"})


# ─── G8: the selections follow the writes ────────────────────────────────────


class TestTheSelectionsFollow:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("method,reader,args,answer", [
        ("find_order_id_gaps", "order_id_gaps", (50,), [7]),
        ("find_backdated_order_ids", "backdated_order_ids", (T0.date(), 5), [8]),
        ("get_latest_order_time", "latest_order_time", (), T0),
        ("find_halfwritten_orders", "halfwritten_candidates", (10,), [9]),
        ("halfwritten_among", "halfwritten_among", ([9, 10],), [9]),
        ("orders_held", "order_count", (), 46_000),
    ])
    async def test_each_reads_postgres_under_the_chain(
            self, met, duck, method, reader, args, answer):
        """A selection read from a frozen DuckDB never sees the repair it
        caused: the gap scan would re-fetch the same 200 ids every hour.
        Mutation: route one reader at DuckDB — its row fails."""
        from core import pg_orders_read

        met.setenv("KS_WRITE_ORDERS", "postgres")
        read = AsyncMock(return_value=answer)
        met.setattr(pg_orders_read, reader, read)
        met.setattr(duck, "connection", MagicMock(side_effect=AssertionError("DuckDB read")))
        assert await getattr(duck, method)(*args) == answer
        read.assert_awaited_once()


    @pytest.mark.asyncio
    async def test_the_boot_does_not_pull_730_days_over_an_empty_duckdb(self, met, monkeypatch):
        """A latched host with a fresh DuckDB: DuckDB's count is 0 for ever.
        Mutation: gate the boot's full sync on `get_stats()["orders"]`."""
        from core import sync_service as mod

        met.setenv("KS_WRITE_ORDERS", "postgres")
        store = MagicMock()
        store.get_stats = AsyncMock(return_value={"orders": 0, "products": 0})
        store.orders_held = AsyncMock(return_value=46_000)
        store.refresh_sku_inventory_status = AsyncMock(return_value=0)
        service = MagicMock()
        service.full_sync = AsyncMock()
        service.incremental_sync = AsyncMock()
        monkeypatch.setattr(mod, "get_store", AsyncMock(return_value=store))
        monkeypatch.setattr(mod, "get_sync_service", AsyncMock(return_value=service))
        monkeypatch.setattr(mod, "init_meilisearch", AsyncMock(return_value=False))
        await mod.init_and_sync()
        service.full_sync.assert_not_awaited()
        service.incremental_sync.assert_awaited_once()


# ─── G7: the order step is contained under the chain ─────────────────────────


def _tick_service(monkeypatch, store):
    from core import sync_service as mod
    from core.sync_service import SyncService

    client = MagicMock()
    monkeypatch.setattr(mod, "get_async_client", AsyncMock(return_value=client))
    monkeypatch.setattr(mod, "emit_sync_started", AsyncMock())
    monkeypatch.setattr(mod, "emit_sync_completed", AsyncMock())
    monkeypatch.setattr(mod, "emit_sync_failed", AsyncMock())
    svc = SyncService(store=store)
    # Everything after the order step is quiet but reached.
    svc.sync_missing_buyers = AsyncMock(return_value=0)
    svc._inventory_step_postgres = AsyncMock()
    svc.sync_offers = AsyncMock(return_value=0)
    svc.sync_stocks = AsyncMock(return_value=0)
    return svc


def _quiet_store():
    store = MagicMock()
    recent = datetime.now(UTC)
    store.get_last_sync_time = AsyncMock(return_value=recent)
    store.set_last_sync_time = AsyncMock()
    store.mark_warehouse_dirty = AsyncMock()
    store.refresh_sku_inventory_status = AsyncMock(return_value=0)
    store.record_sku_inventory_snapshot = AsyncMock()
    store.record_inventory_snapshot = AsyncMock()
    return store


class TestTheOrderStepIsContained:
    @pytest.mark.asyncio
    async def test_a_postgres_failure_costs_the_order_step_alone(self, met, monkeypatch):
        """Mutation: re-raise in the tick's handler — the rest of the tick,
        products and buyers and inventory, would stop with Postgres."""
        met.setenv("KS_WRITE_ORDERS", "postgres")
        store = _quiet_store()
        svc = _tick_service(monkeypatch, store)
        svc._orders_step = AsyncMock(side_effect=ConnectionRefusedError("pg down"))
        svc._record_orders_failure = AsyncMock()
        stats = await svc.incremental_sync()
        assert "skipped" not in stats
        assert svc.orders_step.consecutive_failures == 1
        assert svc.orders_step.last_error_class == "ConnectionRefusedError"
        svc._record_orders_failure.assert_awaited_once()
        store.set_last_sync_time.assert_not_awaited()       # the watermark is held
        published = svc.orders_step_health()
        assert published["consecutive_failures"] == 1 and published["ever_ok"] is False
        assert "pg down" not in str(published)              # the class, never the text

    @pytest.mark.asyncio
    async def test_a_keycrm_error_still_ends_the_tick(self, met, monkeypatch):
        from core.exceptions import KeyCRMAPIError

        met.setenv("KS_WRITE_ORDERS", "postgres")
        svc = _tick_service(monkeypatch, _quiet_store())
        svc._orders_step = AsyncMock(side_effect=KeyCRMAPIError("429", status_code=429))
        await svc.incremental_sync()
        svc.sync_missing_buyers.assert_not_awaited()
        assert svc.orders_step.consecutive_failures == 0


    @pytest.mark.asyncio
    async def test_a_success_clears_the_streak(self, met, monkeypatch):
        met.setenv("KS_WRITE_ORDERS", "postgres")
        svc = _tick_service(monkeypatch, _quiet_store())
        svc.orders_step.failed(RuntimeError("x"))
        svc._orders_step = AsyncMock(return_value=[])
        await svc.incremental_sync()
        assert svc.orders_step.consecutive_failures == 0
        assert svc.orders_step_health()["ever_ok"] is True

    @pytest.mark.asyncio
    async def test_a_watermark_read_that_hangs_is_a_failure_not_a_stall(self, met, monkeypatch):
        """Under the chain `last_sync_orders` is a Postgres read; a store that
        neither answers nor refuses must fail the step within its bound."""
        from core import sync_service as mod

        met.setenv("KS_WRITE_ORDERS", "postgres")
        monkeypatch.setattr(mod, "ORDERS_WATERMARK_TIMEOUT_S", 0.05)
        store = _quiet_store()

        async def hang(_key):
            await asyncio.sleep(30)

        store.get_last_sync_time = AsyncMock(side_effect=hang)
        svc = _tick_service(monkeypatch, store)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(svc._orders_step(MagicMock(), {}), 5)


def _duckdbs_own_stamp(monkeypatch, key: str, value) -> None:
    """The stamp DuckDB's `sync_metadata` froze at the flip, as Postgres's
    copy of that table holds it (`app.sync_metadata`, read by
    `core.pg_frozen_metadata`). Since step 14's S14-11 the store's getter
    inherits from that copy alone; until then a process under `KS_DUCKDB=on`
    read DuckDB's own row, which this wrote."""
    async def frozen_value(k):
        return value.isoformat() if k == f"last_sync_{key}" else None

    monkeypatch.setattr("core.pg_frozen_metadata.frozen_value", frozen_value)


class TestTheFirstTickAfterTheFlip:
    """The chain-3 review: `last_sync_orders` is where the order step's window
    STARTS, and Postgres holds no such key until the first tick under the flag
    writes one. Read as absent it became "an hour ago", so the first tick asked
    KeyCRM from 25 h back — and with DuckDB's last stamp three days old (a
    maintenance window, a day of KeyCRM failing before the flip) two days of
    updated orders were never fetched by the incremental sync at all."""

    @staticmethod
    def _recording_fetch(monkeypatch):
        from core.sync_service import SyncService

        asked = []

        async def fetch(self, client, start, end, *args, **kwargs):
            asked.append(start)
            return []

        monkeypatch.setattr(SyncService, "_fetch_orders_with_date_filter", fetch)
        return asked

    @staticmethod
    def _window_start(watermark):
        from core.sync_service import DEFAULT_TZ

        return ((watermark - timedelta(hours=24)).astimezone(DEFAULT_TZ)
                .strftime("%Y-%m-%d %H:%M:%S"))

    @pytest.mark.asyncio
    async def test_the_window_starts_where_duckdb_stopped(self, met, duck, monkeypatch):
        """Mutations: declare `CHAIN_WATERMARK_INHERITS_DUCKDB = False`; drop
        the inheritance from `DuckDBStore.get_last_sync_time`."""
        from core import pg_chain_watermarks
        from core.sync_service import SyncService

        frozen = datetime.now(UTC).replace(microsecond=0) - timedelta(days=3)
        _duckdbs_own_stamp(monkeypatch, "orders", frozen)
        met.setenv("KS_WRITE_ORDERS", "postgres")
        monkeypatch.setattr(pg_chain_watermarks, "get_value", AsyncMock(return_value=None))
        asked = self._recording_fetch(monkeypatch)

        assert await duck.get_last_sync_time("orders") == frozen
        await SyncService(duck)._orders_step(MagicMock(), {"orders": 0, "expenses": 0})
        assert asked == [self._window_start(frozen)]

    @pytest.mark.asyncio
    async def test_once_postgres_holds_the_key_it_wins(self, met, duck, monkeypatch):
        """DuckDB's stamp stands in for an ABSENT key only. Mutation: read
        DuckDB's whatever Postgres holds — every tick under the chain would
        start from the frozen stamp, a window growing by a day a day."""
        from core import pg_chain_watermarks
        from core.sync_service import SyncService

        frozen = datetime.now(UTC).replace(microsecond=0) - timedelta(days=3)
        live = frozen + timedelta(days=2, hours=23)
        _duckdbs_own_stamp(monkeypatch, "orders", frozen)
        met.setenv("KS_WRITE_ORDERS", "postgres")
        monkeypatch.setattr(pg_chain_watermarks, "get_value", AsyncMock(return_value=live))
        asked = self._recording_fetch(monkeypatch)

        await SyncService(duck)._orders_step(MagicMock(), {"orders": 0, "expenses": 0})
        assert asked == [self._window_start(live)]

    @pytest.mark.asyncio
    async def test_a_chain_that_does_not_declare_it_still_reads_absent(self, flags, duck,
                                                                      monkeypatch):
        """Opt-in, per chain: chain 1's keys move every hour, and its absence
        past one tick is the finding (`_freshness_check`). Mutation: inherit
        for every chain."""
        from core import pg_chain_watermarks, pg_inventory_write

        assert not getattr(pg_inventory_write, "CHAIN_WATERMARK_INHERITS_DUCKDB", False)
        _duckdbs_own_stamp(monkeypatch, "offers", datetime.now(UTC) - timedelta(minutes=5))
        monkeypatch.setattr(pg_chain_watermarks, "get_value", AsyncMock(return_value=None))
        assert await duck.get_last_sync_time("offers") is None


class TestTheSearchCursorMovesWithTheChain:
    """OD-15: `last_sync_meilisearch_pg`, the cursor of the search index built
    from Postgres, moves with chain 3. Its value is a Postgres clock
    (`MAX(mirrored_at)`), and once the chain writes Postgres it lives in
    `meta.chain_watermarks` with `last_sync_orders` — carried into DuckDB and
    released by the copy-back with it."""

    @pytest.mark.asyncio
    async def test_under_the_chain_it_is_postgres_s_and_inherited_until_written(
            self, met, duck, monkeypatch):
        """The first index tick after the flip finds no cursor in Postgres;
        DuckDB's frozen one stands in, so it goes on incrementally rather
        than re-indexing everything, and its stamp lands in Postgres.
        Mutation: drop the key from `CHAIN_SYNC_KEYS` — the setter writes
        DuckDB, and nothing set below reaches `set_value`."""
        from core import pg_chain_watermarks
        from core.pg_search_index_read import WATERMARK_KEY

        frozen = datetime.now(UTC).replace(microsecond=0) - timedelta(hours=2)
        _duckdbs_own_stamp(monkeypatch, WATERMARK_KEY, frozen)
        met.setenv("KS_WRITE_ORDERS", "postgres")
        written = {}

        async def set_value(key, value):
            written[key] = value

        monkeypatch.setattr(pg_chain_watermarks, "get_value", AsyncMock(return_value=None))
        monkeypatch.setattr(pg_chain_watermarks, "set_value", set_value)
        assert await duck.get_last_sync_time(WATERMARK_KEY) == frozen

        later = frozen + timedelta(hours=1)
        await duck.set_last_sync_time(WATERMARK_KEY, later)
        assert written == {"last_sync_meilisearch_pg": later.isoformat()}

    @pytest.mark.asyncio
    async def test_the_freshness_check_does_not_call_it_unwatched(self, flags, duck):
        """It has no threshold, so it is watched in no mode; a run that could
        not read the moved watermarks names only what it would have judged.
        Mutation: drop `blind &= set(FRESHNESS_THRESHOLDS)` (or the matching
        filter in `unverified_conditions`) — every such run would say a stall
        of the search cursor went unwatched, and hold a condition no check
        fires."""
        from core import data_quality as dq

        chain_latch.latch(pow_.CHAIN)
        now = datetime.now(UTC)
        rows = {f"last_sync_{key}": (now - timedelta(minutes=5)).isoformat()
                for key in ("orders", "meilisearch_pg")}
        issues = dq._freshness_check(now, frozen_rows=rows)   # no chain_watermarks
        (unwatched,) = [i for i in issues if i.check_name == "sync_watermarks_unwatched"]
        # Since S14-00 every chain's keys are moved, so every one with a
        # threshold is named — the search cursor still never is.
        assert "orders" in unwatched.description
        assert "meilisearch" not in unwatched.description
        conditions = dq.unverified_conditions([], [unwatched])
        assert "freshness_orders" in conditions
        assert not [c for c in conditions if "meilisearch" in c]


class TestTheTickSaysWhenItWaitsForTheHeavyLock:
    @pytest.mark.asyncio
    async def test_the_wait_is_published_and_ends_when_the_tick_runs(self, flags, monkeypatch):
        """What the canary's allowance reads. Mutations: drop `waiting()` —
        the wait is never published and a heavy hold pages; drop the
        `done_waiting()` taken with the lock — a tick stuck INSIDE the lock
        reads as waiting, and is excused for 90 minutes instead of 20."""
        from core import sync_service as sync_mod
        from core.scheduler import BackgroundScheduler
        from core.sync_service import SyncService

        svc = SyncService(_quiet_store())
        seen_inside = []

        async def tick():
            seen_inside.append(svc.orders_step_health()["lock_wait_s"])
            return {"orders": 0}

        svc.incremental_sync = tick
        monkeypatch.setattr(sync_mod, "get_sync_service", AsyncMock(return_value=svc))
        scheduler = BackgroundScheduler()
        assert svc.orders_step_health()["lock_wait_s"] is None

        await scheduler._heavy_job_lock.acquire()              # the 05:15 refresh
        job = asyncio.ensure_future(scheduler._run_incremental_sync())
        for _ in range(50):
            if svc.orders_step.waiting_since is not None:
                break
            await asyncio.sleep(0.01)
        assert svc.orders_step_health()["lock_wait_s"] is not None
        assert not job.done()
        scheduler._heavy_job_lock.release()
        assert await asyncio.wait_for(job, 5) == {"orders": 0}

        assert seen_inside == [None]
        assert svc.orders_step_health()["lock_wait_s"] is None

    @pytest.mark.asyncio
    async def test_a_tick_that_raises_stops_waiting(self, flags, monkeypatch):
        from core import sync_service as sync_mod
        from core.scheduler import BackgroundScheduler
        from core.sync_service import SyncService

        svc = SyncService(_quiet_store())
        svc.incremental_sync = AsyncMock(side_effect=RuntimeError("tick"))
        monkeypatch.setattr(sync_mod, "get_sync_service", AsyncMock(return_value=svc))
        with pytest.raises(RuntimeError):
            await BackgroundScheduler()._run_incremental_sync()
        assert svc.orders_step.waiting_since is None


# ─── G13: the canary pages a step that stopped, under the chain only ─────────


class TestTheCanary:
    @staticmethod
    def _payload(mode="postgres", **step):
        base = {"consecutive_failures": 0, "last_ok_age_s": 60, "ever_ok": True,
                "last_attempt_age_s": 60, "last_error_class": None, "last_refused": 0}
        base.update(step)
        return {"write_chains": {"pg_orders_write": {"mode": mode, "sync_step": base}}}

    def test_the_name_is_the_chain_s(self):
        from bot import canary

        assert canary.ORDERS_CHAIN == pow_.CHAIN

    @pytest.mark.parametrize("step,fires", [
        ({}, False),
        ({"consecutive_failures": 3, "last_error_class": "ConnectionRefusedError"}, True),
        ({"consecutive_failures": 2}, False),
        ({"last_ok_age_s": 16 * 60}, True),
        ({"last_ok_age_s": 14 * 60}, False),
        ({"last_ok_age_s": 25 * 60, "last_attempt_age_s": 25 * 60}, True),
        ({"last_ok_age_s": 25 * 60, "last_attempt_age_s": None}, True),
        ({"lock_wait_s": None}, False),
    ])
    def test_it_fires_on_a_streak_a_stale_success_or_a_step_not_reached(self, step, fires):
        from bot.canary import check_orders_sync_chain

        found = check_orders_sync_chain(self._payload(**step))
        assert bool(found) is fires
        if fires:
            (key, message), = found
            assert key == "orders_sync_failing" and "chain 3" in message

    @pytest.mark.parametrize("step,fires,says", [
        # The review's shape: a 22-minute heavy hold right after a success —
        # the tick queued behind the lock, nothing at fault. Mutation: ignore
        # `lock_wait_s`.
        ({"last_ok_age_s": 22 * 60, "last_attempt_age_s": 22 * 60,
          "lock_wait_s": 21 * 60 + 30}, False, None),
        ({"last_ok_age_s": 80 * 60, "last_attempt_age_s": 80 * 60,
          "lock_wait_s": 79 * 60}, False, None),
        # Stale BEFORE the wait began: the wait excuses its own length and
        # nothing more. Mutation: answer "not stale" for any wait.
        ({"last_ok_age_s": 40 * 60, "last_attempt_age_s": 40 * 60,
          "lock_wait_s": 10 * 60}, True, "not reached"),
        ({"last_ok_age_s": 30 * 60, "last_attempt_age_s": 12 * 60,
          "lock_wait_s": 10 * 60}, True, "no success"),
        # A wait past chain 4's bound: whatever holds the lock is stuck.
        # Mutation: no bound on the wait.
        ({"last_ok_age_s": 92 * 60, "last_attempt_age_s": 92 * 60,
          "lock_wait_s": 91 * 60}, True, "heavy-job lock"),
        # A streak pages whatever the tick is doing now.
        ({"consecutive_failures": 3, "lock_wait_s": 60}, True, "failures in a row"),
    ])
    def test_a_tick_queued_behind_the_heavy_lock_is_not_a_stopped_one(self, step, fires, says):
        """The chain-3 review: the full sync, training, the backup and the
        05:15 refresh hold `_heavy_job_lock`, the tick waits behind them, and
        a hold past 20 minutes paged CRITICAL with no fault behind it."""
        from bot.canary import check_orders_sync_chain

        found = check_orders_sync_chain(self._payload(**step))
        assert bool(found) is fires, found
        if fires:
            (key, message), = found
            assert key == "orders_sync_failing" and says in message

    @pytest.mark.parametrize("mode", ["duckdb", None])
    def test_whatever_an_entry_says_of_its_mode(self, mode):
        """Since step 14's S14-11 the step is judged on its own: chain 3 is
        the only writer of orders, a constant, and no entry publishes a
        `mode` (until then one that said duckdb was not judged). Mutation:
        gate the check on `mode` again."""
        from bot.canary import check_orders_sync_chain

        (key, _), = check_orders_sync_chain(self._payload(mode, consecutive_failures=9))
        assert key == "orders_sync_failing"
        assert check_orders_sync_chain({}) == []

    def test_the_text_never_reaches_the_page(self):
        from bot.canary import check_orders_sync_chain

        (_, message), = check_orders_sync_chain(self._payload(
            consecutive_failures=3, last_error_class="InterfaceError"))
        assert "InterfaceError" in message

    def test_it_is_a_critical_with_a_lever(self):
        from bot import canary
        from core.alerting import REGISTRY

        assert "orders_sync_failing" in REGISTRY
        assert any(prefix == "orders_sync_failing" for prefix, _ in canary._ACTIONS)


class TestThePreflight:
    """Chain 3's preflight went in step 14's S14-09 and chains 1's and 5's
    in S14-11, with the bound they shared. What stays is the order step
    under the chain's entry, which the canary judges."""

    @staticmethod
    def _quiet_steps(monkeypatch):
        from web.routes.api import health

        monkeypatch.setattr(health, "_inventory_sync_step", AsyncMock(return_value=None))
        monkeypatch.setattr(health, "_managers_sync_step", AsyncMock(return_value=None))
        monkeypatch.setattr(health, "_catalogue_sync_step", AsyncMock(return_value=None))

    @pytest.mark.asyncio
    async def test_health_publishes_the_step_and_no_preflight_under_the_entry(
            self, flags, monkeypatch):
        """Mutation: a preflight put back under chain 3's entry, or the step
        taken away — the canary would no longer judge the only writer of
        orders (`orders_sync_failing`)."""
        from web.routes.api import health

        self._quiet_steps(monkeypatch)
        monkeypatch.setattr(health, "_orders_sync_step", AsyncMock(
            return_value={"consecutive_failures": 0}))
        block = await health._write_chains_block()
        assert block["pg_orders_write"] == {"sync_step": {"consecutive_failures": 0}}


class TestTheCanaryReadsWhatHealthNowPublishes:
    """S14-09 changed the shape of chain 3's `/api/health` entry (its
    preflight went): the CRITICAL that is the only page for "nobody writes
    the orders" must still read it. End to end, through the real block and
    the real check. Since S14-11 the entry is `{sync_step}` alone. Mutation:
    drop `sync_step` from the entry — the canary then judges nothing."""

    @staticmethod
    async def _block(monkeypatch, step_state):
        from types import SimpleNamespace

        from core import sync_service as sync_mod
        from web.routes.api import health

        TestThePreflight._quiet_steps(monkeypatch)
        service = SimpleNamespace(orders_step_health=step_state.published)
        monkeypatch.setattr(sync_mod, "get_sync_service", AsyncMock(return_value=service))
        return {"write_chains": await health._write_chains_block()}

    @pytest.mark.asyncio
    async def test_a_failing_step_pages(self, flags, monkeypatch):
        from bot import canary
        from core.sync_service import OrdersStepState

        state = OrdersStepState()
        for _ in range(3):
            state.attempted()
            state.failed(ConnectionRefusedError("down"))
        payload = await self._block(monkeypatch, state)
        (key, message), = canary.check_orders_sync_chain(payload)
        assert key == "orders_sync_failing" and "ConnectionRefusedError" in message
        assert "orders_sync_failing" not in canary.unjudged_keys(payload)

    @pytest.mark.asyncio
    async def test_a_healthy_step_is_quiet_and_judged(self, flags, monkeypatch):
        from bot import canary
        from core.sync_service import OrdersStepState

        state = OrdersStepState()
        state.attempted()
        state.succeeded()
        payload = await self._block(monkeypatch, state)
        assert canary.check_orders_sync_chain(payload) == []
        assert "orders_sync_failing" not in canary.unjudged_keys(payload)

    @pytest.mark.asyncio
    async def test_no_step_holds_the_key(self, flags, monkeypatch):
        from bot import canary
        from core import sync_service as sync_mod
        from web.routes.api import health

        TestThePreflight._quiet_steps(monkeypatch)
        monkeypatch.setattr(sync_mod, "get_sync_service",
                            AsyncMock(side_effect=RuntimeError("no store")))
        payload = {"write_chains": await health._write_chains_block()}
        assert payload["write_chains"][pow_.CHAIN]["sync_step"] is None
        assert "orders_sync_failing" in canary.unjudged_keys(payload)


class TestTheDefaultShipsWhatMainShipped:
    """With the flag at its default every order shipment out of DuckDB runs
    `pg_landing._write_order_rows`, which chain 3 split out of `write_orders`.
    The data was identical, the statements were not: the savepoint around the
    derivation mark was opened whether or not derivation is owned (the
    chain-3 review). Since step 14's S14-15 derivation is always owned — the
    `owns()` the writer asked went with KS_PG_DERIVE — so the mark and its
    savepoint are always there, once."""

    @pytest.mark.asyncio
    async def test_the_mark_runs_in_one_savepoint_of_its_own(self, monkeypatch):
        """Mutation: drop the savepoint, or mark twice."""
        from core import pg_derivation, pg_landing, pg_order_versions

        marked = []

        async def mark(conn, layer=pg_derivation.LAYER):
            marked.append(layer)
            return True

        monkeypatch.setattr(pg_derivation, "mark", mark)
        monkeypatch.setattr(pg_order_versions, "capture_versions", AsyncMock(return_value=0))

        class Conn:
            savepoints = 0

            async def executemany(self, sql, rows):
                pass

            async def execute(self, sql, *args):
                pass

            def transaction(self):
                conn = self

                class Savepoint:
                    async def __aenter__(self):
                        conn.savepoints += 1

                    async def __aexit__(self, *exc):
                        return False

                return Savepoint()

        landed = landed_orders([_payload(1, products=2)], with_products=True)
        conn = Conn()
        await pg_landing._write_order_rows(
            conn, [tuple(r) for r in landed.orders], [tuple(p) for p in landed.products],
            replace_products=True, version_kind="change")
        assert conn.savepoints == 1
        assert marked == [pg_derivation.LAYER]
