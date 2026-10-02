"""Chain 6, the catalogue (products and categories), without a database.

The real-Postgres half is `tests/integration/test_catalogue_writer.py` and it
proves the statements and the one-clock rule; this proves what is around them:
that the flag off changes nothing, that the flag on hands the writer the rows
the shared parse produced and never touches DuckDB, that the chain is held on
DuckDB until its readers have moved, that a fault in it costs the catalogue
and not the tick or the full sync it rides in, that only the two
full-catalogue sync sites reach the writer, and that the standing watch, the
registry and the copy-back all see it.
"""
from __future__ import annotations

import ast
import pathlib
import time
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

from core import chain_latch, pg_catalogue_write as chain
from core import pg_chain_invariants as inv
from core import read_fallback, warehouse_cutover, write_chains
from core.landing_rows import CategoryRow, ProductRow, category_rows, product_rows

ROOT = pathlib.Path(__file__).resolve().parents[2]
UTC = timezone.utc

PRODUCTS = [
    {"id": 1, "name": "Toner", "category_id": 10, "sku": "T-1", "min_price": 250,
     "custom_fields": [{"uuid": "CT_1001", "value": ["COSRX"]}]},
    {"id": 2, "name": "Serum", "category_id": 11, "sku": "S-2", "price": 410.5},
]
CATEGORIES = [{"id": 10, "name": "Care", "parent_id": None},
              {"id": 11, "name": "Serums", "parent_id": 10}]


@pytest.fixture
def flags(monkeypatch):
    """No chain's variable set, nothing latched (conftest points the marker
    directory at this test's tmp_path), and the read fallback as it is in
    production today."""
    for c in write_chains.WRITE_CHAINS:
        monkeypatch.delenv(c.WRITE_ENV, raising=False)
    for name in warehouse_cutover.WAREHOUSE_READERS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(read_fallback, "_mode", "duckdb")
    chain._unmet_warned.clear()
    return monkeypatch


@pytest.fixture
def met(flags):
    """Every precondition of the flip met, as the environment says it: chain 1
    on Postgres, the fallback off, every warehouse reader on Postgres."""
    flags.setenv("KS_WRITE_INVENTORY", "postgres")
    flags.setattr(read_fallback, "_mode", "off")
    for name in warehouse_cutover.WAREHOUSE_READERS:
        flags.setenv(name, "postgres")
    return flags


@pytest.fixture
def flagged(met):
    met.setenv(chain.WRITE_ENV, "postgres")
    return met


@pytest.fixture
def flagged_alone(flags):
    """The flag on and its precondition taken as met, with every other chain
    and reader as it is today — so the tick's other steps keep their own
    paths and only the catalogue's moves."""
    flags.setenv(chain.WRITE_ENV, "postgres")
    flags.setattr(chain, "unmet_precondition", lambda: None)
    return flags


class _NoDuckDB:
    """A store whose DuckDB must not be reached."""

    def connection(self):
        raise AssertionError("the catalogue reached DuckDB under KS_WRITE_CATALOGUE=postgres")


class _Recorder:
    """A DuckDB connection that records what it is handed."""

    def __init__(self):
        self.statements = []

    def connection(self):
        rec = self

        class _Ctx:
            async def __aenter__(self_inner):
                class _Conn:
                    def execute(self_c, sql, params=None):
                        rec.statements.append((sql.strip().split()[0], params))
                return _Conn()

            async def __aexit__(self_inner, *exc):
                return False
        return _Ctx()


# ─── the repository routes (T-1) ─────────────────────────────────────────────


class TestTheRepositoryRoutes:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("method,payload,parse", [
        ("upsert_products", PRODUCTS, product_rows),
        ("upsert_categories", CATEGORIES, category_rows),
    ])
    async def test_under_the_flag_the_writer_gets_the_parsed_rows_and_duckdb_nothing(
            self, flagged, method, payload, parse):
        from core.duckdb_store import DuckDBStore

        writer = AsyncMock(return_value=len(payload))
        with patch.object(chain, method, new=writer):
            written = await getattr(DuckDBStore, method)(_NoDuckDB(), payload)

        assert written == len(payload)
        (rows,), _ = writer.await_args
        # After the shared parse, so both stores read a product the same way:
        # brand from the custom field, `min_price or price`.
        assert rows == parse(payload)

    def test_the_parse_is_the_one_that_reads_the_brand(self):
        rows = product_rows(PRODUCTS)
        assert rows[0].brand == "COSRX" and rows[0].price == 250
        assert rows[1].brand is None and rows[1].price == 410.5

    @pytest.mark.asyncio
    @pytest.mark.parametrize("method,payload", [
        ("upsert_products", PRODUCTS), ("upsert_categories", CATEGORIES)])
    async def test_with_the_flag_off_the_postgres_writer_is_never_called(
            self, flags, method, payload):
        from core.duckdb_store import DuckDBStore

        store = _Recorder()
        writer = AsyncMock(side_effect=AssertionError("Postgres written with the flag off"))
        with patch.object(chain, method, new=writer):
            assert await getattr(DuckDBStore, method)(store, payload) == len(payload)
        assert [v for v, _ in store.statements].count("INSERT") == len(payload)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("method,payload", [
        ("upsert_products", PRODUCTS), ("upsert_categories", CATEGORIES)])
    async def test_held_by_its_precondition_it_writes_duckdb(self, flags, method, payload):
        """The flag alone moves nothing: chain 1 is off here."""
        from core.duckdb_store import DuckDBStore

        flags.setenv(chain.WRITE_ENV, "postgres")
        store = _Recorder()
        writer = AsyncMock(side_effect=AssertionError("written while held"))
        with patch.object(chain, method, new=writer):
            assert await getattr(DuckDBStore, method)(store, payload) == len(payload)

    @pytest.mark.asyncio
    async def test_an_empty_payload_writes_nowhere_latches_nothing_stamps_nothing(
            self, flagged):
        get_pool = AsyncMock(side_effect=AssertionError("asked for a pool"))
        with patch("core.pg.get_pool", new=get_pool):
            assert await chain.upsert_products([]) == 0
            assert await chain.upsert_categories([]) == 0
        assert not chain_latch.latched(chain.CHAIN)


# ─── the flag (T-2) ──────────────────────────────────────────────────────────


class TestTheFlag:
    def test_off_by_default(self, flags):
        assert chain.writes_postgres() is False
        assert chain.mode() == "duckdb"

    def test_an_unknown_value_raises_and_mode_does_not(self, flags):
        flags.setenv(chain.WRITE_ENV, "postgrse")
        with pytest.raises(RuntimeError, match=chain.WRITE_ENV):
            chain.writes_postgres()
        assert chain.mode() is None

    def test_the_latch_outranks_it(self, flags):
        chain_latch.latch(chain.CHAIN, chain.WRITE_ENV)
        flags.setenv(chain.WRITE_ENV, "duckdb")
        assert chain.writes_postgres() is True
        assert chain.mode() == "postgres"

    def test_the_latch_outranks_a_value_nobody_can_read(self, flags):
        chain_latch.latch(chain.CHAIN, chain.WRITE_ENV)
        flags.setenv(chain.WRITE_ENV, "yes")
        assert chain.writes_postgres() is True

    @pytest.mark.parametrize("env,met_,latched,expected", [
        (None, False, False, "duckdb"),
        ("postgres", True, False, "postgres"),
        ("postgres", False, False, "duckdb"),
        ("postgrse", True, False, None),
        ("duckdb", False, True, "postgres"),
    ])
    def test_mode_is_the_registrys_answer(self, flags, env, met_, latched, expected):
        if env:
            flags.setenv(chain.WRITE_ENV, env)
        if met_:
            flags.setattr(chain, "unmet_precondition", lambda: None)
        if latched:
            chain_latch.latch(chain.CHAIN, chain.WRITE_ENV)
        assert chain.mode() == expected
        assert write_chains.chain_modes()[chain.CHAIN]["mode"] == expected


# ─── the precondition (T-3) ──────────────────────────────────────────────────


class TestThePrecondition:
    def test_all_met_is_none(self, met):
        assert chain.unmet_precondition() is None

    def test_chain_1_on_duckdb_is_named(self, met):
        met.setenv("KS_WRITE_INVENTORY", "duckdb")
        assert "KS_WRITE_INVENTORY is not postgres" in chain.unmet_precondition()

    def test_a_chain_1_typo_is_unmet_and_named_without_its_value(self, met):
        met.setenv("KS_WRITE_INVENTORY", "s3cr3t-typo")
        why = chain.unmet_precondition()
        assert "KS_WRITE_INVENTORY is not understood" in why
        assert "s3cr3t-typo" not in why, "the public endpoint must not quote a value"

    def test_chain_1_latched_counts_as_moved(self, met):
        met.setenv("KS_WRITE_INVENTORY", "duckdb")
        chain_latch.latch("pg_inventory_write", "KS_WRITE_INVENTORY")
        assert chain.unmet_precondition() is None

    def test_the_fallback_still_answering_from_duckdb_is_named(self, met):
        met.setattr(read_fallback, "_mode", "duckdb")
        assert "KS_READ_FALLBACK is not off" in chain.unmet_precondition()

    @pytest.mark.parametrize("reader", warehouse_cutover.WAREHOUSE_READERS)
    def test_every_warehouse_reader_left_on_duckdb_is_named(self, met, reader):
        """Parametrised over the switch's own list, which a walk of every
        `KS_READ_*` name in the tree keeps complete — never a hand-typed
        subset."""
        met.setenv(reader, "duckdb")
        why = chain.unmet_precondition()
        assert why and reader in why

    @pytest.mark.parametrize("reader", warehouse_cutover.WAREHOUSE_READERS[:2])
    def test_a_reader_typo_is_unmet(self, met, reader):
        met.setenv(reader, "postgress")
        assert reader in chain.unmet_precondition()

    def test_it_asks_postgres_nothing(self, met):
        met.setenv("KS_WRITE_INVENTORY", "duckdb")
        with patch("core.pg.get_pool", new=AsyncMock(side_effect=AssertionError("pool"))):
            assert chain.unmet_precondition()

    def test_held_on_duckdb_while_unmet_and_published(self, flags):
        flags.setenv(chain.WRITE_ENV, "postgres")
        state = write_chains.chain_modes()[chain.CHAIN]
        assert state["mode"] == "duckdb"
        assert "KS_WRITE_INVENTORY" in state["unmet_precondition"]
        assert chain.writes_postgres() is False

    def test_latched_and_unmet_keeps_writing_postgres_and_says_so(self, flags):
        chain_latch.latch(chain.CHAIN, chain.WRITE_ENV)
        flags.setenv(chain.WRITE_ENV, "postgres")
        state = write_chains.chain_modes()[chain.CHAIN]
        assert state["mode"] == "postgres" and state["unmet_precondition"]

    def test_the_warning_is_logged_once_an_hour_per_reason(self, flags, caplog):
        """The tick reads the products watermark every minute; chain 6a's
        warn-per-call would log once a minute while the chain is held."""
        flags.setenv(chain.WRITE_ENV, "postgres")
        with caplog.at_level("WARNING", logger=chain.__name__):
            for _ in range(5):
                assert chain.writes_postgres() is False
        assert len([r for r in caplog.records if "stays on DuckDB" in r.message]) == 1

    def test_it_reads_the_switchs_one_helper(self, met, monkeypatch):
        """`readers_not_on_postgres` is what the switch files as `reader:*`;
        the chain reads it rather than a list of its own."""
        monkeypatch.setattr(warehouse_cutover, "readers_not_on_postgres",
                            lambda env: ("KS_READ_SOMETHING_NEW",))
        assert "KS_READ_SOMETHING_NEW" in chain.unmet_precondition()


# ─── the registry (T-4, T-5) ─────────────────────────────────────────────────


class TestTheRegistrySeesIt:
    def test_it_declares_both_tables_and_both_watermarks(self):
        assert chain.CHAIN_TABLES == ("bronze.products", "bronze.categories")
        assert chain.CHAIN_SYNC_KEYS == ("last_sync_products", "last_sync_categories")
        assert chain in write_chains.WRITE_CHAINS
        assert write_chains.chain_name(chain) == chain.CHAIN

    def test_the_tables_stand_down_under_the_flag_and_only_then(self, met):
        assert not set(chain.CHAIN_TABLES) & write_chains.stood_down_tables()
        met.setenv(chain.WRITE_ENV, "postgres")
        assert set(chain.CHAIN_TABLES) <= write_chains.stood_down_tables()

    def test_the_watermarks_are_this_chains_and_move_with_it(self, met):
        for key in chain.CHAIN_SYNC_KEYS:
            assert write_chains.chain_for_sync_key(key) is chain
        met.setenv(chain.WRITE_ENV, "postgres")
        assert set(chain.CHAIN_SYNC_KEYS) <= write_chains.stood_down_sync_keys()

    def test_a_typo_stands_down_only_its_own_tables(self, flags):
        flags.setenv(chain.WRITE_ENV, "yes")
        tables, errors = write_chains.stood_down_tables_checked()
        assert tables == frozenset(chain.CHAIN_TABLES)
        assert list(errors) == [chain.CHAIN]

    def test_the_order_paths_never_ask_it(self, flags):
        from core.pg_landing import ORDER_UNIT

        flags.setenv(chain.WRITE_ENV, "postgrse")
        with patch.object(chain, "env_writes_postgres",
                          side_effect=AssertionError("asked about the orders")):
            assert write_chains.stood_down_among(ORDER_UNIT) == frozenset()

    def test_each_table_is_a_unit_of_its_own(self):
        """`_mirror` ships each alone, so neither drags another table down."""
        from core.pg_landing import unit_of

        for table in chain.CHAIN_TABLES:
            assert unit_of(table) == (table,)

    def test_the_mirror_stands_down_for_both(self, flagged):
        import asyncio

        from core import pg_landing

        flagged.setenv(pg_landing.MIRROR_ENV, "1")
        out = asyncio.run(pg_landing.mirror_products(PRODUCTS))
        assert out.skipped and "bronze.products" in out.skipped
        out = asyncio.run(pg_landing.mirror_categories(CATEGORIES))
        assert out.skipped and "bronze.categories" in out.skipped


class TestTheSearchCursorIsNotTheCatalogues:
    """OD-15: `last_sync_meilisearch_pg` moves with whichever of chains 3 and
    6 lands last, and that is chain 3. Declared here, a catalogue rollback
    would carry and release a search-index cursor, and the flip would read it
    as absent and re-index everything."""

    SEARCH_KEYS = {"last_sync_meilisearch_pg", "last_sync_meilisearch"}

    def test_no_chain_declares_it_unless_it_owns_an_order_table(self):
        from core.pg_landing import ORDER_UNIT

        for c in write_chains.WRITE_CHAINS:
            declared = set(getattr(c, "CHAIN_SYNC_KEYS", ())) & self.SEARCH_KEYS
            if declared:
                assert set(ORDER_UNIT) & set(c.CHAIN_TABLES), (
                    write_chains.chain_name(c), declared)

    def test_the_catalogue_does_not(self):
        assert not set(chain.CHAIN_SYNC_KEYS) & self.SEARCH_KEYS


# ─── the refusal comes before the latch (T-7) ────────────────────────────────


class TestARefusedCatalogueLatchesNothing:
    @pytest.mark.parametrize("table,rows,named", [
        ("products", [ProductRow(1, None, 1, None, None, None)], "name"),
        ("products", [ProductRow(1, "x", 1, None, "SKU\x00", None)], "sku"),
        ("products", [ProductRow(1, "x", 1, None, None, 1e11)], "price"),
        ("products", [ProductRow(None, "x", 1, None, None, None)], "id"),
        ("products", [ProductRow(True, "x", 1, None, None, None)], "id"),
        ("products", [ProductRow(1, "x", 2 ** 31, None, None, None)], "category_id"),
        ("products", [ProductRow(1, "x", None, "\ud800", None, None)], "brand"),
        ("products", [ProductRow(1, "x", None, None, None, "12.5")], "price"),
        ("categories", [CategoryRow(1, None, None)], "name"),
        ("categories", [CategoryRow(1, "x", "7")], "parent_id"),
    ])
    @pytest.mark.asyncio
    async def test_refused_whole_before_any_connection(self, flagged, table, rows, named):
        get_pool = AsyncMock(side_effect=AssertionError("asked for a pool"))
        good = (ProductRow(5, "fine", None, None, None, 1.0) if table == "products"
                else CategoryRow(5, "fine", None))
        with patch("core.pg.get_pool", new=get_pool):
            with pytest.raises(chain.CatalogueRefused, match=named):
                await getattr(chain, f"upsert_{table}")([good, *rows])
        get_pool.assert_not_called()
        assert not chain_latch.latched(chain.CHAIN)
        assert not chain_latch.marker_path(chain.CHAIN).exists()

    def test_the_refusal_names_the_id_and_column_never_the_value(self):
        why = chain._refusal(chain.PRODUCTS, [ProductRow(7, "x", None, None, "s3cr3t\x00", None)])
        assert "id 7" in why and "sku" in why and "s3cr3t" not in why

    def test_a_price_on_the_boundary_is_postgres_own_rule(self):
        ok = [ProductRow(1, "x", None, None, None, 9999999999.99)]
        over = [ProductRow(1, "x", None, None, None, 9999999999.995)]
        assert chain._refusal(chain.PRODUCTS, ok) is None
        assert "price" in chain._refusal(chain.PRODUCTS, over)


class TestDeduplication:
    def test_last_wins_and_sorted_by_id(self):
        rows = [ProductRow(3, "c", None, None, None, None),
                ProductRow(1, "a", None, None, None, None),
                ProductRow(3, "c2", None, None, None, None)]
        assert chain._dedupe(rows) == [(1, "a", None, None, None, None),
                                       (3, "c2", None, None, None, None)]


class TestTheRecordOfTheChainsWrites:
    """What `writes:<table>` holds and who may read it. The statements that
    write it are proved on a real Postgres (`test_catalogue_writer.py`)."""

    def test_it_reads_what_the_writer_stores(self):
        record = chain.parse_record('{"stamps": [3, 1, 2], "previous": 7}')
        assert record == chain.WriteRecord(stamps=frozenset({1, 2, 3}), previous=7)
        assert chain.parse_record('{"stamps": [], "previous": null}') == chain.WriteRecord()
        # The empty record the writer creates is one it can read back.
        assert chain.parse_record(chain._EMPTY_RECORD_TEXT) == chain.WriteRecord()

    @pytest.mark.parametrize("value", [
        "not json", "[]", "null", '{"previous": 1}', '{"stamps": "1,2"}',
        '{"stamps": [1, "2"]}', '{"stamps": [true]}', '{"stamps": [1.5]}',
        '{"stamps": [], "previous": "1"}', '{"stamps": [], "previous": false}',
    ])
    def test_anything_else_is_unreadable_never_a_guess(self, value):
        with pytest.raises(chain.WriteRecordUnreadable):
            chain.parse_record(value)

    def test_one_key_per_table_beside_the_owner_rows(self):
        keys = {chain.record_key(t) for t in chain.CHAIN_TABLES}
        assert keys == {"writes:bronze.products", "writes:bronze.categories"}
        assert not keys & {chain_latch.owner_key(t) for t in chain.CHAIN_TABLES}
        assert not keys & set(chain.CHAIN_SYNC_KEYS)

    def test_the_copy_back_releases_it_with_the_latch(self):
        assert set(chain.CHAIN_RELEASED_KEYS) == {
            chain.record_key(t) for t in chain.CHAIN_TABLES}

    def test_the_copy_back_reads_it_for_both_tables_and_the_buyers_do_not(self):
        from core import chain_transfer

        assert chain_transfer._RECORDED_CLOCKS == frozenset(chain.CHAIN_TABLES)
        assert chain_transfer._REWRITE_CLOCK_TABLE not in chain_transfer._RECORDED_CLOCKS


# ─── only the two full-catalogue sync sites reach the writer (T-6) ──────────


def _calls(tree, attr):
    """`{function name: node}` of every function whose body calls `.attr(`."""
    found = {}
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for sub in ast.walk(node):
            if (isinstance(sub, ast.Call) and isinstance(sub.func, ast.Attribute)
                    and sub.func.attr == attr):
                found.setdefault(node.name, []).append(sub)
    return found


def _owner(func_call) -> str:
    target = func_call.func.value
    return ast.unparse(target)


def _walk_callers(attr, *, on=None):
    """`{(file, function)}` calling `<x>.attr(...)` in core/, web/, scripts/,
    `on` restricting `<x>` to that expression when given."""
    out = set()
    for base in ("core", "web", "scripts"):
        for path in (ROOT / base).rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for name, calls in _calls(tree, attr).items():
                for call in calls:
                    if on is None or _owner(call) == on:
                        out.add((str(path.relative_to(ROOT)), name))
    return out


class TestTheFullCatalogueContract:
    """Every write stamps the watermark that says "the whole catalogue landed
    at this instant", so a writer handed part of it would make every row it
    left out read as retired. Walked, never listed: a new caller fails here."""

    @pytest.mark.parametrize("attr", ["upsert_products", "upsert_categories"])
    def test_the_writer_is_reached_from_the_repository_alone(self, attr):
        assert _walk_callers(attr, on="pg_catalogue_write") == {
            ("core/duckdb_store.py", attr)}

    @pytest.mark.parametrize("attr,sites", [
        ("upsert_products", {("core/sync_service.py", "full_sync"),
                             ("core/sync_service.py", "incremental_sync"),
                             ("core/sync_service.py", "_catalogue_step_postgres")}),
        ("upsert_categories", {("core/sync_service.py", "full_sync")}),
    ])
    def test_the_repository_is_reached_from_the_full_catalogue_sites_alone(self, attr, sites):
        callers = _walk_callers(attr) - {("core/duckdb_store.py", attr)}
        assert callers == sites, callers


# ─── full_sync contains this chain only (T-8) ────────────────────────────────


class _Client:
    async def paginate(self, endpoint, params=None, page_size=50):
        if endpoint == "products":
            yield PRODUCTS
        elif endpoint == "products/categories":
            yield CATEGORIES
        return


class _SyncStore:
    """Everything `full_sync` asks of the store, recorded; the catalogue's
    writes raise what the test says."""

    def __init__(self, *, categories=None, products=None):
        self.raises = {"categories": categories, "products": products}
        self.stamped = []

    async def upsert_categories(self, rows):
        if self.raises["categories"]:
            raise self.raises["categories"]
        return len(rows)

    async def upsert_expense_types(self, rows):
        return len(rows)

    async def upsert_products(self, rows):
        if self.raises["products"]:
            raise self.raises["products"]
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
        return datetime(2026, 9, 20, tzinfo=UTC)

    async def checkpoint(self):
        return None


async def _full_sync(store):
    from core import sync_service as sync_module

    service = sync_module.SyncService(store)
    service.sync_managers = AsyncMock(return_value=0)
    service.sync_offers = AsyncMock(return_value=0)
    service.sync_stocks = AsyncMock(return_value=0)
    service._fetch_orders_with_date_filter = AsyncMock(return_value=[])

    async def _client():
        return _Client()

    with patch.object(sync_module, "get_async_client", new=_client), \
         patch.object(sync_module, "mirror_categories", new=AsyncMock()), \
         patch.object(sync_module, "mirror_products", new=AsyncMock()), \
         patch("core.pg_landing._record_failure", new=AsyncMock(return_value=True)) as rec:
        return await service.full_sync(days_back=30), rec


class TestFullSyncContainsThisChainOnly:
    @pytest.mark.asyncio
    async def test_under_the_flag_a_categories_failure_leaves_its_watermark(self, flagged):
        store = _SyncStore(categories=ConnectionRefusedError("postgres is down"))
        stats, rec = await _full_sync(store)
        assert "ConnectionRefusedError" in stats["categories_error"]
        assert store.stamped == ["expense_types", "products", "orders"]
        rec.assert_awaited_once()
        assert rec.await_args.args[0] == "bronze.categories"

    @pytest.mark.asyncio
    async def test_under_the_flag_a_products_failure_leaves_its_watermark(self, flagged):
        store = _SyncStore(products=chain.CatalogueRefused("bronze.products id 3: name is NULL"))
        stats, _ = await _full_sync(store)
        assert "CatalogueRefused" in stats["products_error"]
        assert store.stamped == ["categories", "expense_types", "orders"]

    @pytest.mark.asyncio
    async def test_a_flag_nobody_can_read_is_contained_the_same_way(self, flags):
        flags.setenv(chain.WRITE_ENV, "postgrse")
        store = _SyncStore(
            categories=RuntimeError("KS_WRITE_CATALOGUE='postgrse' is not understood"),
            products=RuntimeError("KS_WRITE_CATALOGUE='postgrse' is not understood"))
        stats, _ = await _full_sync(store)
        assert "postgrse" in stats["categories_error"] and "postgrse" in stats["products_error"]
        assert store.stamped == ["expense_types", "orders"]

    @pytest.mark.asyncio
    async def test_with_the_flag_off_a_failure_raises_exactly_as_it_always_has(self, flags):
        store = _SyncStore(products=RuntimeError("duckdb said no"))
        with pytest.raises(RuntimeError, match="duckdb said no"):
            await _full_sync(store)
        assert store.stamped == ["categories", "expense_types"]

    @pytest.mark.asyncio
    async def test_with_the_flag_off_a_categories_failure_raises_too(self, flags):
        store = _SyncStore(categories=RuntimeError("duckdb said no to categories"))
        with pytest.raises(RuntimeError, match="categories"):
            await _full_sync(store)
        assert store.stamped == []


# ─── the incremental tick survives the chain (T-9) ──────────────────────────


@pytest_asyncio.fixture
async def real_store(tmp_path):
    from core.duckdb_store import DuckDBStore

    s = DuckDBStore(db_path=tmp_path / "c6.duckdb")
    await s.connect()
    yield s
    await s.close()


def _tick(monkeypatch, store, *, upsert_products=None):
    """The REAL `incremental_sync` over a real DuckDB, with KeyCRM, the other
    steps and the catalogue writer faked. Orders, managers, buyers and
    inventory are all due, so each can be seen to run."""
    from core import sync_service as mod
    from core.sync_service import SyncService

    endpoints = []

    class _KeyCRM:
        async def paginate(self, endpoint, params=None, page_size=50):
            endpoints.append(endpoint)
            yield PRODUCTS

    monkeypatch.setattr(mod, "get_async_client", AsyncMock(return_value=_KeyCRM()))
    monkeypatch.setattr(mod, "mirror_products", AsyncMock())
    svc = SyncService(store=store)
    svc._should_skip_sync = lambda: (False, None)
    svc._fetch_orders_with_date_filter = AsyncMock(return_value=[])
    svc.sync_managers = AsyncMock(return_value=0)
    svc.sync_missing_buyers = AsyncMock(return_value=0)
    svc.sync_offers = AsyncMock(return_value=5)
    svc.sync_stocks = AsyncMock(return_value=7)
    monkeypatch.setattr(store, "refresh_sku_inventory_status", AsyncMock(return_value=0))
    monkeypatch.setattr(store, "record_sku_inventory_snapshot", AsyncMock())
    monkeypatch.setattr(store, "record_inventory_snapshot", AsyncMock())
    writer = upsert_products or AsyncMock(return_value=2)
    monkeypatch.setattr(chain, "upsert_products", writer)
    return svc, endpoints, writer


def _rest_of_the_tick_ran(svc):
    svc._fetch_orders_with_date_filter.assert_awaited()
    svc.sync_managers.assert_awaited()
    svc.sync_missing_buyers.assert_awaited()
    svc.sync_offers.assert_awaited()
    svc.sync_stocks.assert_awaited()


class TestTheTickSurvivesTheChain:
    @pytest.mark.asyncio
    async def test_a_typo_costs_the_products_step_and_nothing_else(
            self, flags, monkeypatch, real_store):
        flags.setenv(chain.WRITE_ENV, "postgrse")
        svc, endpoints, writer = _tick(monkeypatch, real_store)

        stats = await svc.incremental_sync()

        _rest_of_the_tick_ran(svc)
        assert "products" not in endpoints, "KeyCRM was asked for a catalogue nobody can write"
        writer.assert_not_called()
        assert all(isinstance(v, int) for v in stats.values()), stats
        step = svc.catalogue_step
        assert (step.failures_since_ok, step.last_error) == (1, "RuntimeError")
        assert svc._catalogue_retry_in() is not None

    @pytest.mark.asyncio
    async def test_postgres_down_at_the_watermark_costs_the_step_alone(
            self, flagged_alone, monkeypatch, real_store):
        svc, endpoints, writer = _tick(monkeypatch, real_store)
        with patch("core.pg_chain_watermarks.get_value",
                   new=AsyncMock(side_effect=OSError("connection refused"))):
            await svc.incremental_sync()

        _rest_of_the_tick_ran(svc)
        assert "products" not in endpoints
        assert svc.catalogue_step.last_error == "OSError"

    @pytest.mark.asyncio
    async def test_a_failed_write_holds_the_watermark_and_stamps_the_mirror_state(
            self, flagged_alone, monkeypatch, real_store):
        svc, endpoints, writer = _tick(
            monkeypatch, real_store,
            upsert_products=AsyncMock(side_effect=ConnectionRefusedError("gone")))
        setter = AsyncMock()
        with patch("core.pg_chain_watermarks.get_value", new=AsyncMock(return_value=None)), \
             patch("core.pg_chain_watermarks.set_value", new=setter), \
             patch("core.pg_landing._record_failure", new=AsyncMock(return_value=True)) as rec:
            await svc.incremental_sync()

        _rest_of_the_tick_ran(svc)
        assert endpoints == ["products"]
        assert not [c for c in setter.await_args_list if c.args[0] == "last_sync_products"]
        rec.assert_awaited_once()
        assert rec.await_args.args[0] == "bronze.products"
        assert svc.catalogue_step.last_error == "ConnectionRefusedError"

    @pytest.mark.asyncio
    async def test_inside_the_window_it_asks_keycrm_and_postgres_nothing(
            self, flagged_alone, monkeypatch, real_store):
        svc, endpoints, _ = _tick(monkeypatch, real_store)
        getter = AsyncMock(side_effect=OSError("connection refused"))
        with patch("core.pg_chain_watermarks.get_value", new=getter):
            await svc.incremental_sync()
            assert getter.await_count == 1
            await svc.incremental_sync()
        assert getter.await_count == 1, "the retry window let the watermark read through"
        assert "products" not in endpoints
        assert svc.sync_offers.await_count == 2      # the rest of each tick ran

    @pytest.mark.asyncio
    async def test_after_the_window_a_success_writes_stamps_and_closes_it(
            self, flagged_alone, monkeypatch, real_store):
        svc, endpoints, writer = _tick(monkeypatch, real_store)
        setter = AsyncMock()
        with patch("core.pg_chain_watermarks.get_value",
                   new=AsyncMock(side_effect=[OSError("down"), None])), \
             patch("core.pg_chain_watermarks.set_value", new=setter):
            await svc.incremental_sync()
            svc._catalogue_retry_at = time.monotonic() - 1
            await svc.incremental_sync()

        writer.assert_awaited_once()
        (rows,), _ = writer.await_args
        assert rows == product_rows(PRODUCTS)
        assert [c.args[0] for c in setter.await_args_list].count("last_sync_products") == 1
        assert svc.catalogue_step.failures_since_ok == 0
        assert svc._catalogue_retry_in() is None

    @pytest.mark.asyncio
    async def test_not_due_it_fetches_nothing(self, flagged_alone, monkeypatch, real_store):
        svc, endpoints, writer = _tick(monkeypatch, real_store)
        recent = datetime.now(UTC) - timedelta(minutes=5)
        with patch("core.pg_chain_watermarks.get_value", new=AsyncMock(return_value=recent)):
            await svc.incremental_sync()
        assert endpoints == [] and not writer.await_count
        assert svc.catalogue_step.failures_since_ok == 0

    @pytest.mark.asyncio
    async def test_with_the_flag_off_the_read_comes_before_the_orders_as_always(
            self, flags, monkeypatch, real_store):
        svc, endpoints, writer = _tick(monkeypatch, real_store)
        asked = []
        real = real_store.get_last_sync_time

        async def spy(key="orders"):
            asked.append(key)
            return await real(key)

        monkeypatch.setattr(real_store, "get_last_sync_time", spy)
        order_fetch = svc._fetch_orders_with_date_filter

        async def fetch(*a, **kw):
            asked.append("<orders fetched>")
            return await order_fetch(*a, **kw)

        svc._fetch_orders_with_date_filter = fetch
        await svc.incremental_sync()
        assert asked[:3] == ["orders", "products", "<orders fetched>"]
        assert endpoints == ["products"]
        writer.assert_not_called()      # DuckDB wrote it
        assert svc.catalogue_step.failures_since_ok == 0


# ─── the standing watch (T-10) ───────────────────────────────────────────────

W = datetime(2026, 9, 30, 8, tzinfo=UTC)
LATCHED = datetime(2026, 9, 29, 9, tzinfo=UTC)


def _table(name="bronze.products", **kw):
    base = dict(table=name, rows=1004, last_ok_at=W, last_rows=1003, current=1003,
                retired=1, around=0, retired_sample=(1055,))
    base.update(kw)
    return inv.CatalogueTable(**base)


def _facts(*tables, latched_at=LATCHED):
    return inv.Facts(watched=(chain.CHAIN,),
                     catalogue=inv.Catalogue(tables=tables, latched_at=latched_at))


def _names(issues):
    return {(i.check_name, i.severity.value) for i in issues}


class TestTheStandingWatch:
    def test_the_chain_has_a_reader_and_a_field(self):
        assert inv._reader_groups()[chain.CHAIN] == "catalogue"

    def test_production_today_is_one_retired_product_and_nothing_else(self):
        issues = inv.check_chain_invariants(_facts(
            _table(), _table("bronze.categories", rows=28, last_rows=28, current=28,
                             retired=0, retired_sample=())))
        assert _names(issues) == {(inv.CATALOGUE_RETIRED, "INFO")}
        (issue,) = issues
        assert issue.sample_ids == (1055,) and issue.count == 1

    def test_an_empty_table_is_critical(self):
        (issue,) = inv.check_chain_invariants(_facts(_table(rows=0, current=0, retired=0)))
        assert (issue.check_name, issue.severity.value) == (inv.CATALOGUE_EMPTY, "CRITICAL")

    def test_a_row_written_after_the_last_full_write_is_critical_with_ids(self):
        issues = inv.check_chain_invariants(_facts(
            _table(current=1002, around=1, around_sample=(7,), retired=1)))
        assert (inv.CATALOGUE_WRITTEN_AROUND, "CRITICAL") in _names(issues)
        (around,) = [i for i in issues if i.check_name == inv.CATALOGUE_WRITTEN_AROUND]
        assert around.sample_ids == (7,)
        assert "later than the last full write" in around.description

    def test_once_the_chain_wrote_it_names_the_record_not_the_watermark(self):
        (around,) = [i for i in inv.check_chain_invariants(_facts(
            _table(current=1002, around=1, around_sample=(777,), recorded=True)))
            if i.check_name == inv.CATALOGUE_WRITTEN_AROUND]
        assert "none of the chain's recorded writes" in around.description
        assert "stands until" in around.description

    def test_lost_counts_the_rows_written_round_it_as_still_there(self):
        """The stray-update probe: one served row re-stamped by somebody else
        is `around`, not lost. Counting lost as `last_rows - current` would
        call it lost too."""
        issues = inv.check_chain_invariants(_facts(
            _table(current=1002, around=1, around_sample=(7,))))
        assert inv.CATALOGUE_ROWS_LOST not in {i.check_name for i in issues}

    def test_a_deleted_served_row_is_lost_once_the_chain_wrote(self):
        issues = inv.check_chain_invariants(_facts(_table(rows=1003, current=1002)))
        (lost,) = [i for i in issues if i.check_name == inv.CATALOGUE_ROWS_LOST]
        assert (lost.severity.value, lost.count) == ("CRITICAL", 1)

    def test_lost_is_not_judged_against_a_write_older_than_the_handover(self):
        """The mirror's `last_rows` counts a repeated payload id twice."""
        issues = inv.check_chain_invariants(_facts(
            _table(rows=1003, current=1002), latched_at=W + timedelta(minutes=1)))
        assert inv.CATALOGUE_ROWS_LOST not in {i.check_name for i in issues}

    def test_lost_is_not_judged_before_the_first_write(self):
        issues = inv.check_chain_invariants(_facts(_table(rows=1003, current=1002),
                                                   latched_at=None))
        assert inv.CATALOGUE_ROWS_LOST not in {i.check_name for i in issues}

    def test_retired_is_strictly_before_the_last_write(self):
        """A row that carries the instant exactly is current, not retired:
        the boundary is `<`, judged by the SQL, and the verdict takes its
        count."""
        assert inv.check_chain_invariants(_facts(_table(retired=0, retired_sample=()))) == []

    def test_a_short_write_is_a_warning(self):
        issues = inv.check_chain_invariants(_facts(
            _table(rows=1004, current=900, last_rows=900, retired=104,
                   retired_sample=tuple(range(10)))))
        assert (inv.CATALOGUE_SHORT_WRITE, "WARN") in _names(issues)

    def test_a_few_retired_categories_are_not_a_short_write(self):
        """Two of 28 is 7% and ordinary: the floor in rows keeps the small
        table from warning for ever."""
        issues = inv.check_chain_invariants(_facts(_table(
            "bronze.categories", rows=28, last_rows=26, current=26, retired=2,
            retired_sample=(3, 4))))
        assert _names(issues) == {(inv.CATALOGUE_RETIRED, "INFO")}

    def test_no_full_write_recorded_is_unwatched_not_clean(self):
        (issue,) = inv.check_chain_invariants(_facts(_table(last_ok_at=None, last_rows=None)))
        assert issue.check_name == inv.UNWATCHED and "bronze.products" in issue.description
        assert inv.unverified_conditions([issue]) == sorted(inv.CONDITIONS)

    def test_an_unreadable_group_is_unwatched_and_holds_every_condition(self):
        facts = inv.Facts(watched=(chain.CHAIN,), catalogue=inv.Unwatched("no table"))
        (issue,) = inv.check_chain_invariants(facts)
        assert issue.check_name == inv.UNWATCHED and chain.CHAIN in issue.description

    def test_every_condition_is_held_by_a_blind_run_and_carries_the_prefix(self):
        mine = {inv.CATALOGUE_EMPTY, inv.CATALOGUE_WRITTEN_AROUND, inv.CATALOGUE_ROWS_LOST,
                inv.CATALOGUE_RETIRED, inv.CATALOGUE_SHORT_WRITE}
        assert mine <= set(inv.CONDITIONS)
        assert all(c.startswith(inv.PREFIX) for c in mine)

    def test_its_watermarks_are_left_to_the_freshness_check(self):
        assert inv.watermark_limit_min(chain) is None
        assert chain.CHAIN_WATERMARK_INHERITS_DUCKDB is True


# ─── the copy-back (T-11) ────────────────────────────────────────────────────


class TestTheCopyBackKnowsIt:
    def test_two_mirrored_specs_each_its_own_clock_compared_whole(self):
        from core import chain_transfer

        specs = chain_transfer.chain_specs(chain)
        assert [s.pg_table for s in specs] == ["bronze.products", "bronze.categories"]
        for spec in specs:
            assert spec.is_mirrored and not spec.is_append
            assert spec.clock == ()
            assert spec.clock_table == spec.pg_table
            assert spec.rewritten_by == "id"
            assert spec.texts is not None
            # A key on one side only is a failed copy in this direction,
            # whatever the daily spec forgives as retired.
            assert spec.compare.full_replace is True
            assert spec.compare.synced_column is None
        assert chain_transfer.chain_sequences(chain) == ()

    def test_the_buyers_keep_their_clock_and_texts(self):
        from core import chain_transfer, pg_buyers_write

        for spec in chain_transfer.chain_specs(pg_buyers_write):
            if spec.is_mirrored:
                assert spec.clock_table is None and spec.texts is None
                assert chain_transfer._clock_table(spec) == "bronze.buyers"

    def test_the_operator_types_its_short_name(self):
        from core.chain_transfer import resolve_chain

        assert resolve_chain("catalogue") is chain

    def test_the_runbook_sends_it_to_its_own_sync_not_another_chains(self):
        from core.chain_transfer import _marker_steps, _runbook

        said = " ".join(_runbook(chain, executed=True, released=True))
        assert "KS_WRITE_CATALOGUE=duckdb" in said
        assert "bronze.products" in said and "bronze.categories" in said
        assert "hourly products sync" in said and "full_sync_weekly" in said
        for elsewhere in ("replicate_operational", "backfill/buyers", "buyers mirror", "I1"):
            assert elsewhere not in said, elsewhere
        steps = " ".join(_marker_steps(chain, chain.CHAIN))
        assert "backfill/buyers" not in steps and "hourly products sync" in steps

    @pytest.mark.asyncio
    async def test_an_unknown_rewrite_clock_is_refused(self):
        from core import chain_transfer

        with pytest.raises(LookupError):
            await chain_transfer._rewritten_since_latch(object(), "app.manual_expenses")


def _spec(table="bronze.products"):
    from core import chain_transfer

    return {s.pg_table: s for s in chain_transfer.chain_specs(chain)}[table]


def _row(spec, **values):
    return tuple(values.get(c) for c in spec.compare.columns)


def _classify(spec, dk, pg, *, moved_on, rewritten=None):
    from core.chain_transfer import classify_handover

    return classify_handover(spec, dk, pg, moved_on=moved_on, rewritten=rewritten)


class TestTheHandoverOfTheCatalogue:
    """The texts and the rule for a catalogue table, both sides already read."""

    def _p(self, pid, name="x"):
        spec = _spec()
        return pid, _row(spec, id=pid, name=name)

    def test_before_the_flip_a_product_only_duckdb_holds_names_the_carry(self):
        spec = _spec()
        k, r = self._p(1055)
        (issue,) = _classify(spec, {k: r}, {}, moved_on=False)
        assert (issue.check_name, issue.severity.value) == ("handover_rows_missing", "CRITICAL")
        assert "/api/mirror/backfill/catalogue" in issue.description
        assert "backfill/buyers" not in issue.description

    def test_before_the_flip_a_differing_product_names_the_hourly_sync(self):
        spec = _spec()
        k, r = self._p(1)
        _, r2 = self._p(1, "other")
        (issue,) = _classify(spec, {k: r}, {k: r2}, moved_on=False)
        assert (issue.check_name, issue.severity.value) == ("handover_rows_differ", "CRITICAL")
        assert "hourly products sync" in issue.description
        assert "backfill/buyers" not in issue.description

    def test_before_the_flip_a_category_only_postgres_holds_names_the_full_sync(self):
        spec = _spec("bronze.categories")
        row = _row(spec, id=9, name="x")
        (issue,) = _classify(spec, {}, {9: row}, moved_on=False)
        assert (issue.check_name, issue.severity.value) == ("handover_rows_ahead", "CRITICAL")
        assert "full_sync_weekly" in issue.description

    def test_after_the_latch_a_re_stamped_difference_is_the_copys_work(self):
        spec = _spec()
        k, r = self._p(1)
        _, r2 = self._p(1, "renamed")
        (issue,) = _classify(spec, {k: r}, {k: r2}, moved_on=True, rewritten=frozenset({1}))
        assert (issue.check_name, issue.severity.value) == ("handover_rows_differ", "INFO")

    def test_after_the_latch_a_retired_row_that_differs_is_critical(self):
        spec = _spec()
        k, r = self._p(1055)
        _, r2 = self._p(1055, "edited")
        (issue,) = _classify(spec, {k: r}, {k: r2}, moved_on=True, rewritten=frozenset({1}))
        assert (issue.check_name, issue.severity.value) == ("handover_rows_differ", "CRITICAL")
        assert "retired" in issue.description

    def test_after_the_latch_a_product_only_duckdb_holds_always_refuses(self):
        spec = _spec()
        k, r = self._p(77)
        (issue,) = _classify(spec, {k: r}, {}, moved_on=True, rewritten=frozenset({77}))
        assert (issue.check_name, issue.severity.value) == ("handover_rows_missing", "CRITICAL")
        assert "never deletes" in issue.description

    def test_after_the_latch_a_new_product_is_the_copys_work_and_an_old_one_is_not(self):
        spec = _spec()
        new, old = self._p(2001), self._p(3)
        issues = _classify(spec, {}, dict([new, old]), moved_on=True,
                           rewritten=frozenset({2001}))
        assert {(i.check_name, i.severity.value, i.sample_ids) for i in issues} == {
            ("handover_rows_ahead", "INFO", (2001,)),
            ("handover_rows_ahead", "CRITICAL", (3,)),
        }

    def test_equal_stores_say_nothing(self):
        spec = _spec()
        k, r = self._p(1)
        assert _classify(spec, {k: r}, {k: r}, moved_on=False) == []
        assert _classify(spec, {k: r}, {k: r}, moved_on=True, rewritten=frozenset()) == []


# ─── /api/health publishes the step (§3.11) ──────────────────────────────────

@pytest.fixture
def health(flags):
    """The health module with chain 1's preflight answered locally, so the
    block is the registry's state and the two steps' recorders alone."""
    from core import pg_inventory_write
    from web.routes.api import health as module

    async def preflight(*a, **kw):
        return {"ok": False, "reasons": ["not asked here"]}

    module._preflight_cache.update(data=None, expires_at=0)
    flags.setattr(pg_inventory_write, "preflight", preflight)
    yield module
    module._preflight_cache.update(data=None, expires_at=0)


class TestHealthPublishesTheStep:
    @pytest.mark.asyncio
    async def test_it_sits_under_chain_6s_entry_with_the_class_never_the_text(
            self, health, monkeypatch):
        from types import SimpleNamespace

        from core import sync_service as sync_mod

        mine = sync_mod.InventoryStepState()
        mine.failed("products", OSError("password authentication failed for user ks_app"))
        theirs = sync_mod.InventoryStepState()
        service = SimpleNamespace(catalogue_step_health=lambda: mine.published(599),
                                  inventory_step_health=lambda: theirs.published(None))
        monkeypatch.setattr(sync_mod, "get_sync_service", AsyncMock(return_value=service))

        block = await health._write_chains_block()

        step = block[chain.CHAIN]["sync_step"]
        assert step["failures_since_ok"] == 1 and step["last_failed_step"] == "products"
        assert step["last_error"] == "OSError" and step["retry_in_s"] == 599
        assert "password" not in str(block)                 # the class, never the text
        # Each chain reads its own recorder: chain 1's stays clean.
        assert block["pg_inventory_write"]["sync_step"]["failures_since_ok"] == 0
        # The registry's half is untouched: the canary reads `mode` and `error` there.
        assert block[chain.CHAIN]["mode"] == "duckdb"
        assert "preflight" not in block[chain.CHAIN]

    @pytest.mark.asyncio
    async def test_no_sync_service_is_null_not_empty(self, health, monkeypatch):
        from core import sync_service as sync_mod

        monkeypatch.setattr(sync_mod, "get_sync_service",
                            AsyncMock(side_effect=RuntimeError("no store")))
        assert (await health._write_chains_block())[chain.CHAIN]["sync_step"] is None

    @pytest.mark.asyncio
    async def test_the_service_records_what_the_step_publishes(self):
        from core import sync_service as sync_mod

        svc = sync_mod.SyncService.__new__(sync_mod.SyncService)
        svc.catalogue_step = sync_mod.InventoryStepState()
        svc._catalogue_retry_at = 0.0
        assert svc.catalogue_step_health()["retry_in_s"] is None
        svc._catalogue_step_failed(ConnectionRefusedError("refused"))
        published = svc.catalogue_step_health()
        assert published["last_error"] == "ConnectionRefusedError"
        assert 0 < published["retry_in_s"] <= sync_mod.CATALOGUE_RETRY_AFTER_S + 1
