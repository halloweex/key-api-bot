"""Chain 3: the orders writer, its preconditions, its decision and its routing.

`core/pg_orders_write.py` writes the orders, their line items, their expenses
and the backfill-miss ledger to Postgres once `KS_WRITE_ORDERS=postgres` and
every precondition holds (OD-13 (a)). Production has the flag off; what is
pinned here is that off means today's behaviour, that on means Postgres and
nothing else, and that the flag cannot move the writes before the goal
bridge, step 13, chain 1 and the backups say it may. Against a real Postgres
the writer is proved in `tests/integration/test_orders_chain_writer.py`.

Each test names the mutation it exists to fail on.
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
    pow_._unmet_warned.clear()
    return monkeypatch


@pytest.fixture
def met(flags):
    """Every precondition held: the chain moves on its flag alone."""
    for name in ("_goals_bridge_unmet", "_step13_unmet", "_chain1_unmet",
                 "_landing_pages_unmet"):
        flags.setattr(pow_, name, lambda: None)
    flags.setattr(pow_, "_backup_unmet", lambda: [])
    return flags


# ─── G1: the flag, the latch, the declaration ────────────────────────────────


class TestTheFlag:
    def test_off_by_default_is_duckdb(self, flags):
        assert pow_.env_writes_postgres() is False
        assert pow_.writes_postgres() is False
        assert pow_.mode() == "duckdb"

    def test_a_typo_raises_in_the_writer_and_is_no_mode(self, flags):
        """OD-18 (a): a value nobody can read stops this chain and nothing
        else. Mutation: read an unknown value as duckdb."""
        flags.setenv("KS_WRITE_ORDERS", "postgrse")
        with pytest.raises(RuntimeError, match="postgrse"):
            pow_.writes_postgres()
        assert pow_.mode() is None
        assert write_chains.chain_modes()["pg_orders_write"]["error"]

    def test_the_flag_moves_the_writes_only_with_every_precondition(self, met):
        met.setenv("KS_WRITE_ORDERS", "postgres")
        assert pow_.writes_postgres() is True and pow_.mode() == "postgres"
        met.setattr(pow_, "_step13_unmet", lambda: "step13: not in force")
        assert pow_.writes_postgres() is False and pow_.mode() == "duckdb"

    def test_the_latch_outranks_an_unmet_precondition(self, flags):
        """OD-19 (a): a latched chain keeps writing Postgres — a stale drill
        is no reason to start a second writer in DuckDB."""
        chain_latch.latch(pow_.CHAIN)
        assert pow_.writes_postgres() is True
        assert pow_.unmet_precondition()            # still published
        assert write_chains.chain_modes()[pow_.CHAIN]["unmet_precondition"]

    def test_a_precondition_that_comes_good_mid_process_does_not_flip(self, met):
        """The chain-3 review: with the flag set while the PITR drill marker
        was stale, the Monday drill's fresh marker flipped a RUNNING web on
        its next write — no stopped window, no `--handover`. Held is held
        until the process ends, in the writer and in the registry alike.
        Mutation: answer the live preconditions alone (drop `_held`)."""
        met.setenv("KS_WRITE_ORDERS", "postgres")
        met.setattr(pow_, "_backup_unmet", lambda: ["pitr_drill: 9 days old"])
        assert pow_.writes_postgres() is False
        met.setattr(pow_, "_backup_unmet", lambda: [])         # 08:44, the drill passed
        assert pow_.writes_postgres() is False
        assert pow_.mode() == "duckdb"
        why = pow_.unmet_precondition()
        assert why.startswith(pow_.HELD_KEY) and "pitr_drill" in why
        assert "--handover" in why
        assert write_chains.chain_modes()[pow_.CHAIN]["unmet_precondition"] == why
        met.setattr(pow_, "_held", None)                         # a restart
        assert pow_.writes_postgres() is True

    def test_the_start_takes_the_verdict_before_any_consumer_asks(self, met):
        """A precondition unmet when the process starts holds it, even if it
        comes good before the first write asks. Mutation: drop
        `settle_hold()` from `configure_modes()`."""
        from core.runtime_modes import configure_modes

        met.setenv("KS_WRITE_ORDERS", "postgres")
        met.setattr(pow_, "_backup_unmet", lambda: ["remote_restore: never"])
        configure_modes()
        met.setattr(pow_, "_backup_unmet", lambda: [])
        assert pow_.writes_postgres() is False
        assert pow_.unmet_precondition().startswith(pow_.HELD_KEY)

    def test_with_the_flag_off_the_start_reads_nothing_and_holds_nothing(self, flags):
        """Production today: the start verdict costs nothing and records
        nothing, so a later process — the only place a flag can change —
        decides for itself. Mutation: record the hold whatever the flag."""
        def boom():
            raise AssertionError("read with the flag off")

        live = pow_._live_unmet
        flags.setattr(pow_, "_live_unmet", boom)
        assert pow_.settle_hold() is None and pow_._held is None
        flags.setattr(pow_, "_live_unmet", live)
        assert pow_.unmet_precondition()                # unmet today, flag off
        assert pow_._held is None

    def test_a_latched_chain_is_never_held(self, met):
        """The latch outranks the hold as it outranks every precondition."""
        met.setenv("KS_WRITE_ORDERS", "postgres")
        met.setattr(pow_, "_held", ("2026-10-01T08:00:00+00:00", "pitr_drill: old"))
        chain_latch.latch(pow_.CHAIN)
        assert pow_.writes_postgres() is True and pow_.mode() == "postgres"

    @pytest.mark.parametrize("state", ["off", "flag", "latched", "typo", "held"])
    def test_mode_is_the_registry_s_answer_in_every_state(self, met, state):
        """`mode()` and `chain_modes()` are one answer. Mutation: compute
        `mode()` from the environment alone."""
        if state == "flag":
            met.setenv("KS_WRITE_ORDERS", "postgres")
        elif state == "latched":
            chain_latch.latch(pow_.CHAIN)
        elif state == "typo":
            met.setenv("KS_WRITE_ORDERS", "pg")
        elif state == "held":
            met.setenv("KS_WRITE_ORDERS", "postgres")
            met.setattr(pow_, "_chain1_unmet", lambda: "chain1: not postgres")
        assert pow_.mode() == write_chains.chain_modes()[pow_.CHAIN]["mode"]


class TestTheDeclaration:
    def test_the_four_tables_and_the_one_key(self):
        """Mutation: drop `app.order_backfill_misses` — `replicate_operational`
        would go on replacing it out of a frozen DuckDB every hour, rolling
        back every miss the chain recorded."""
        assert pow_.CHAIN == "pg_orders_write"
        assert pow_.WRITE_ENV == "KS_WRITE_ORDERS"
        assert pow_.CHAIN_TABLES == ("bronze.orders", "bronze.order_products",
                                     "bronze.expenses", "app.order_backfill_misses")
        assert pow_.CHAIN_SYNC_KEYS == ("last_sync_orders",)
        assert pow_.CHAIN_WATERMARK_MAX_AGE_MIN is None
        # The order step's window starts at this key (the chain-3 review;
        # `TestTheFirstTickAfterTheFlip`).
        assert pow_.CHAIN_WATERMARK_INHERITS_DUCKDB is True

    def test_the_archive_is_not_a_chain_table(self):
        """`app.order_versions` has one writer in both modes and no DuckDB
        copy; declaring it would make `chain_specs` raise."""
        assert "app.order_versions" not in pow_.CHAIN_TABLES
        assert "bronze.expense_types" not in pow_.CHAIN_TABLES   # chain 6a's

    def test_registered_and_resolved_by_its_short_name(self):
        from core.chain_transfer import resolve_chain

        assert pow_ in write_chains.WRITE_CHAINS
        assert resolve_chain("orders") is pow_
        assert write_chains.chain_for_sync_key("last_sync_orders") is pow_

    def test_with_the_flag_off_nothing_stands_down(self, flags):
        """Production's state after this merges: registered, off, and the
        order shippers still ship."""
        from core.pg_landing import order_tables_stood_down

        assert order_tables_stood_down() == frozenset()
        assert not write_chains.stood_down_tables() & set(pow_.CHAIN_TABLES)
        assert "last_sync_orders" not in write_chains.stood_down_sync_keys()


# ─── G2: the preconditions ───────────────────────────────────────────────────


class TestThePreconditions:
    def test_all_met_is_none(self, met):
        assert pow_.unmet_precondition() is None

    @pytest.mark.parametrize("name,key", [
        ("_goals_bridge_unmet", "goals_bridge"), ("_step13_unmet", "step13"),
        ("_chain1_unmet", "chain1"), ("_landing_pages_unmet", "landing_pages_clear"),
    ])
    def test_each_one_unmet_alone_is_named(self, met, name, key):
        """Mutation: drop any one check from `unmet_precondition` — its row
        here fails."""
        met.setattr(pow_, name, lambda: f"{key}: unmet in this test")
        assert pow_.unmet_precondition().startswith(key)

    def test_the_backup_evidence_is_named(self, met):
        met.setattr(pow_, "_backup_unmet", lambda: ["pitr_drill: never", "pg_offsite: old"])
        assert pow_.unmet_precondition() == "pitr_drill: never; pg_offsite: old"

    def test_a_fact_that_cannot_be_read_is_unmet(self, met):
        def broken():
            raise OSError("marker directory gone")

        met.setattr(pow_, "_step13_unmet", broken)
        assert "could not be read" in pow_.unmet_precondition()

    def test_it_asks_postgres_nothing(self, flags, tmp_path):
        """`/api/health` reads it through the registry and must answer with
        Postgres down. Mutation: read an owner row or a run journal here."""
        with patch("core.pg.get_pool", new=AsyncMock(side_effect=AssertionError("asked"))):
            assert pow_.unmet_precondition()   # unmet today, and answered

    def test_the_goals_bridge_holds_it_today_and_goes_with_the_bridge(self, flags):
        from core.repositories import goals

        assert pow_._goals_bridge_unmet().startswith("goals_bridge")
        flags.delattr(goals, "SALES_TYPE_BRIDGE_TABLES")
        assert pow_._goals_bridge_unmet() is None

    def test_step13_is_the_cutover_s_cached_verdict(self, flags):
        from core import warehouse_cutover

        flags.setattr(warehouse_cutover, "_mode", warehouse_cutover.DUCKDB)
        assert pow_._step13_unmet().startswith("step13")
        flags.setattr(warehouse_cutover, "_mode", warehouse_cutover.POSTGRES)
        assert pow_._step13_unmet() is None

    def test_chain1_is_its_own_answer(self, flags):
        assert pow_._chain1_unmet().startswith("chain1")
        flags.setenv("KS_WRITE_INVENTORY", "postgres")
        assert pow_._chain1_unmet() is None
        flags.setenv("KS_WRITE_INVENTORY", "pg")
        assert "not understood" in pow_._chain1_unmet()

    @pytest.mark.parametrize("delivered,blocked", [
        ({}, False),
        ({"fk_orphan_order_products_order_id": "dq:integrity"}, True),
        ({"status_group_vs_return_list": "dq:integrity"}, True),
        ({"order_versions_stalled": "dq:mirror_landing"}, False),
        ({"recon_missing": "dq:reconciliation"}, True),
    ])
    def test_a_page_the_flip_retires_holds_it(self, flags, delivered, blocked):
        """The first run after the flip would announce such a page resolved
        with no check looking — step 13's `retired_conditions_clear`, for
        landing. Mutation: read `retired_conditions` as guard names (the
        bare checks' names are their own conditions, `status_group_agreement`
        reports under another)."""
        with patch("core.alerting.delivered_conditions", return_value=delivered):
            why = pow_._landing_pages_unmet()
        assert bool(why) is blocked
        if blocked:
            assert why.startswith("landing_pages_clear")

    def test_the_backup_evidence_is_the_marker_files(self, flags, tmp_path):
        from core import backup_evidence

        flags.setattr(backup_evidence, "DB_DIR", tmp_path)
        keys = sorted(line.split(":", 1)[0] for line in pow_._backup_unmet())
        assert keys == ["pg_offsite", "pitr_drill", "remote_restore"]

    def test_the_unmet_warning_is_rate_limited(self, flags, caplog):
        """The order step asks every minute. Mutation: warn on every call."""
        flags.setenv("KS_WRITE_ORDERS", "postgres")
        with caplog.at_level("WARNING", logger="core.pg_orders_write"):
            for _ in range(5):
                assert pow_.writes_postgres() is False
        assert len([r for r in caplog.records if "stays on DuckDB" in r.message]) == 1


# ─── G3: the decision is DuckDB's ────────────────────────────────────────────


@pytest_asyncio.fixture
async def duck(tmp_path, flags):
    from core.duckdb_store import DuckDBStore

    store = DuckDBStore(db_path=tmp_path / "orders.duckdb")
    await store.connect()
    flags.setenv("KS_MIRROR_LANDING", "0")
    yield store
    await store.close()


class TestTheDecisionIsDuckDBs:
    """`decide` against `DuckDBStore.upsert_orders` itself, on a grid of
    stored and incoming stamps, force and skip_products. Mutations: drop the
    header-only deferral; compare without reading a naive stamp as UTC."""

    STORED = [None, T0 - timedelta(hours=1), T0, T0 + timedelta(hours=1)]
    INCOMING = [T0, None]

    @pytest.mark.asyncio
    async def test_on_a_grid(self, duck):
        cases = list(itertools.product(range(len(self.STORED)), range(len(self.INCOMING)),
                                       [False, True], [False, True]))
        for n, (si, ii, force, skip) in enumerate(cases):
            oid = 1000 + n
            stored = self.STORED[si]
            if stored is not None:
                await duck.upsert_orders([_payload(oid, updated=stored)])
            incoming = _payload(oid, updated=self.INCOMING[ii])
            existing = {}
            if stored is not None:
                async with duck.connection() as conn:
                    existing = {r[0]: r[1] for r in conn.execute(
                        "SELECT id, updated_at FROM orders WHERE id = ?", [oid]).fetchall()}
            rows = landed_orders([incoming], with_products=not skip).orders
            to_write, skipped, deferred = pow_.decide(
                rows, existing, force_update=force, skip_products=skip)
            result = await duck.upsert_orders([incoming], force_update=force,
                                              skip_products=skip)
            case = (stored, self.INCOMING[ii], force, skip)
            assert [r.id for r in to_write] == result.changed_ids, case
            assert len(skipped) == result.skipped_unchanged, case
            assert deferred == result.deferred_to_full_sync, case

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
        """Mutation: remove the branch in `_upsert_orders_with_expenses` —
        DuckDB's writer then refuses, and the order step fails."""
        from core.duckdb_store import UpsertResult
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
        async with duck.connection() as conn:
            assert conn.execute("SELECT count(*) FROM orders").fetchone()[0] == 0
            assert conn.execute("SELECT count(*) FROM expenses").fetchone()[0] == 0

    @pytest.mark.asyncio
    @pytest.mark.parametrize("state", ["flag", "latched", "typo"])
    async def test_duckdb_s_order_writers_refuse(self, met, duck, state):
        """Mutation: remove the refusal from `upsert_orders` or
        `upsert_expenses_batch` — a caller that goes round the sync would
        write the store nobody reads."""
        if state == "flag":
            met.setenv("KS_WRITE_ORDERS", "postgres")
        elif state == "latched":
            chain_latch.latch(pow_.CHAIN)
        else:
            met.setenv("KS_WRITE_ORDERS", "pgsql")
        with pytest.raises(pow_.ChainOwnsOrders):
            await duck.upsert_orders([_payload(1)])
        with pytest.raises(pow_.ChainOwnsOrders):
            await duck.upsert_expenses_batch([_payload(1, expenses=[9])])
        with pytest.raises(pow_.ChainOwnsOrders):
            await duck.upsert_orders([])                     # not payload-dependent

    @pytest.mark.asyncio
    async def test_the_misses_go_where_the_scans_read_them(self, met, duck):
        met.setenv("KS_WRITE_ORDERS", "postgres")
        writer = AsyncMock(return_value=2)
        met.setattr(pow_, "record_backfill_misses", writer)
        assert await duck.record_backfill_misses({1: "gone", 2: "empty"}) == 2
        writer.assert_awaited_once_with({1: "gone", 2: "empty"})
        async with duck.connection() as conn:
            assert conn.execute(
                "SELECT count(*) FROM order_backfill_misses").fetchone()[0] == 0


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
    async def test_off_they_read_duckdb(self, flags, duck):
        await duck.upsert_orders([_payload(5), _payload(9, products=0)])
        assert await duck.find_order_id_gaps(10) == [6, 7, 8]
        assert await duck.find_halfwritten_orders(10) == [9]
        assert await duck.halfwritten_among([5, 9]) == [9]
        assert await duck.orders_held() == 2

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
        monkeypatch.setattr(mod.warehouse_cutover, "settle_writer", AsyncMock())
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
    async def test_on_duckdb_a_failure_leaves_the_tick_as_it_always_has(self, flags, monkeypatch):
        svc = _tick_service(monkeypatch, _quiet_store())
        svc._orders_step = AsyncMock(side_effect=RuntimeError("duckdb"))
        with pytest.raises(RuntimeError):
            await svc.incremental_sync()

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
        await duck.set_last_sync_time("orders", frozen)         # flag off: DuckDB's
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
        await duck.set_last_sync_time("orders", frozen)
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
        await duck.set_last_sync_time("offers", datetime.now(UTC) - timedelta(minutes=5))
        flags.setenv("KS_WRITE_INVENTORY", "postgres")
        assert pg_inventory_write.writes_postgres()
        monkeypatch.setattr(pg_chain_watermarks, "get_value", AsyncMock(return_value=None))
        assert await duck.get_last_sync_time("offers") is None


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
    def test_not_under_duckdb(self, mode):
        """Mutation: ignore `mode` — a web on DuckDB, whose orders also ship
        through the mirror, would page about a step that is not the only
        writer of anything."""
        from bot.canary import check_orders_sync_chain

        assert check_orders_sync_chain(self._payload(mode, consecutive_failures=9)) == []
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
    @pytest.mark.asyncio
    async def test_before_the_flip_it_names_every_precondition(self, flags):
        """The registry asks the preconditions only under the flag; an operator
        about to flip needs them before. Mutation: return early on the flag."""
        with patch("core.pg.get_pool", new=AsyncMock(side_effect=OSError("no pg"))):
            out = await pow_.preflight()
        assert out["ok"] is False
        keys = {r.split(":", 1)[0] for r in out["reasons"]}
        assert {"goals_bridge", "step13", "chain1", "postgres"} <= keys

    @pytest.mark.asyncio
    async def test_it_asks_postgres_for_the_landing_it_inherits(self, met):
        class _Conn:
            async def fetch(self, sql, tables):
                assert "meta.mirror_state" in sql
                return [{"table_name": "bronze.orders", "backfilled_at": T0,
                         "failures_since_ok": 0},
                        {"table_name": "bronze.expenses", "backfilled_at": T0,
                         "failures_since_ok": 2}]

        class _Acquire:
            async def __aenter__(self):
                return _Conn()

            async def __aexit__(self, *a):
                return False

        class _Pool:
            def acquire(self, timeout=None):
                return _Acquire()

        with patch("core.pg.get_pool", new=AsyncMock(return_value=_Pool())), \
                patch("core.pg.require_revision", new=AsyncMock()):
            out = await pow_.preflight()
        assert out["ok"] is False
        assert any(r.startswith("backfill: bronze.order_products") for r in out["reasons"])
        assert any(r.startswith("mirror: bronze.expenses") for r in out["reasons"])
        assert not any("bronze.orders " in r for r in out["reasons"])

    @pytest.mark.asyncio
    async def test_once_the_chain_writes_postgres_the_question_is_over(self, flags):
        chain_latch.latch(pow_.CHAIN)
        assert await pow_.preflight() == {"ok": None, "reasons": []}

    @pytest.mark.asyncio
    async def test_health_publishes_it_and_the_step_under_the_chain_s_entry(self, flags):
        from web.routes.api import health

        with patch.object(health, "_orders_preflight",
                          new=AsyncMock(return_value={"ok": False, "reasons": ["x"]})), \
                patch.object(health, "_orders_sync_step",
                             new=AsyncMock(return_value={"consecutive_failures": 0})), \
                patch.object(health, "_inventory_preflight", new=AsyncMock(return_value={})), \
                patch.object(health, "_inventory_sync_step", new=AsyncMock(return_value=None)):
            block = await health._write_chains_block()
        entry = block["pg_orders_write"]
        assert entry["preflight"] == {"ok": False, "reasons": ["x"]}
        assert entry["sync_step"] == {"consecutive_failures": 0}
