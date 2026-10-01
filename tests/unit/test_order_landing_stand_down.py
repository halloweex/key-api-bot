"""Chain 3 stands DuckDB's landing checks down, and DN-23's twins stand in (D15).

Once the orders are written to Postgres, DuckDB's `orders` and
`order_products` freeze. The integrity scan's checks over them would compare a
frozen copy — and the Postgres twins, which compare while DuckDB looks, would
file `pg_order_landing_disagree` for every order created since the flip. So the
ten checks in `data_quality.ORDER_LANDING_CHECKS` join
`warehouse_cutover.stood_down_duckdb_checks()` under the chain: the scan skips
them, and `duckdb_looked` (the scheduler subtracts that same set) leaves them
out, so the twins stand in alone.

The set is read off the scan, not remembered: every check the scan runs over
the order tables must sit behind `landing(...)` or a guard, and every name the
constant holds must be one the scan skips under.

Each test names the mutation it exists to fail on.
"""
from __future__ import annotations

import ast
import inspect
import re

import pytest
import pytest_asyncio

from core import chain_latch, data_quality, pg_orders_write, pg_warehouse_dq, write_chains
from core import warehouse_cutover as wc

ORDER_TABLES = {"orders", "order_products"}
LANDING = data_quality.ORDER_LANDING_CHECKS


@pytest.fixture
def flags(monkeypatch):
    for chain in write_chains.WRITE_CHAINS:
        monkeypatch.delenv(chain.WRITE_ENV, raising=False)
    monkeypatch.delenv("KS_WRITE_WAREHOUSE", raising=False)
    return monkeypatch


@pytest.fixture
def latched(flags):
    """Chain 3 writes Postgres by its latch, whatever the flag says."""
    chain_latch.latch(pg_orders_write.CHAIN, pg_orders_write.WRITE_ENV)
    chain_latch._latched = None
    assert pg_orders_write.mode() == "postgres"
    yield flags
    chain_latch.release(pg_orders_write.CHAIN)
    chain_latch._latched = None


class TestTheUnion:
    def test_under_the_chain_every_landing_check_stands_down(self, latched):
        """Mutation: `landing_checks_stood_down` returning nothing — the scan
        compares a frozen DuckDB and the twins file every new order."""
        assert LANDING <= wc.stood_down_duckdb_checks()

    def test_off_the_chain_none_does(self, flags):
        assert pg_orders_write.mode() == "duckdb"
        assert not (LANDING & wc.stood_down_duckdb_checks())

    def test_every_bare_check_a_twin_pairs_with_is_a_landing_check(self):
        """The twins count a bare DuckDB check as looked whenever the scan
        finished; a bare one left off the landing list would stay "looked"
        under the chain and pair a twin with a frozen count."""
        assert pg_warehouse_dq.DUCKDB_BARE_CHECKS <= LANDING


_SQL_OVER_ORDERS = re.compile(r"\b(?:FROM|JOIN)\s+(?:orders|order_products)\b", re.I)
_MODULE = ast.parse(inspect.getsource(data_quality))
_FUNCS = {n.name: n for n in _MODULE.body
          if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}


def _reads_order_tables(node, seen=None) -> bool:
    """Whether a call reads `orders`/`order_products`: by a table name handed
    to a generic helper (`_pk_uniqueness_check(conn, "orders", "id")`), or by
    SQL in the body of a function of the module it calls, to a fixed point."""
    seen = set() if seen is None else seen
    for n in ast.walk(node):
        if isinstance(n, ast.Constant) and isinstance(n.value, str):
            if n.value in ORDER_TABLES or _SQL_OVER_ORDERS.search(n.value):
                return True
        if (isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
                and n.func.id in _FUNCS and n.func.id not in seen):
            seen.add(n.func.id)
            if _reads_order_tables(_FUNCS[n.func.id], seen):
                return True
    return False


def _scan_calls():
    """`(wrapper, name, lambda)` for every `landing(...)`/`guarded(...)` call in
    `check_internal_integrity`, and the check calls outside them."""
    tree = _FUNCS["check_internal_integrity"]
    wrapped, inside = [], set()
    for node in ast.walk(tree):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id in {"landing", "guarded"}
                and node.args and isinstance(node.args[0], ast.Constant)):
            wrapped.append((node.func.id, node.args[0].value, node.args[1]))
            inside.update(id(n) for n in ast.walk(node.args[1]))
    bare = [n for n in ast.walk(tree)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
            and n.func.id in _FUNCS and n.func.id.endswith("_check")
            and id(n) not in inside]
    return wrapped, bare


class TestTheListIsTheScans:
    def test_no_check_over_the_order_tables_runs_bare(self):
        """A check over `orders`/`order_products` added beside the others
        without `landing(...)` would run against the frozen copy. Mutation:
        unwrap `orders_without_line_items`."""
        _wrapped, bare = _scan_calls()
        assert bare, "the walk found no check calls at all"
        offenders = [ast.unparse(c) for c in bare if _reads_order_tables(c)]
        assert offenders == []

    def test_every_check_over_the_order_tables_stands_down_under_the_chain(self):
        """Behind `landing` (chain 3) or a guard step 13 stands down — and
        step 13 in force is a precondition of chain 3 (`_step13_unmet`)."""
        wrapped, _bare = _scan_calls()
        reading = {name for _kind, name, fn in wrapped if _reads_order_tables(fn)}
        assert reading, "the walk found no check over the order tables"
        assert reading <= LANDING | wc.STOOD_DOWN_WHEN_POSTGRES, (
            reading - LANDING - wc.STOOD_DOWN_WHEN_POSTGRES)

    def test_every_listed_name_is_one_the_scan_skips_and_reads_the_tables(self):
        wrapped, _bare = _scan_calls()
        by_name = {name: fn for _kind, name, fn in wrapped}
        assert LANDING <= set(by_name), LANDING - set(by_name)
        assert all(_reads_order_tables(by_name[name]) for name in LANDING), [
            name for name in LANDING if not _reads_order_tables(by_name[name])]


@pytest_asyncio.fixture
async def planted(tmp_path, flags):
    """A DuckDB with a defect under four of the landing checks: a line item
    whose order is missing, an order with no `ordered_at`, a status nobody
    knows, and an order with revenue and no line items."""
    from core.duckdb_store import DuckDBStore

    flags.setenv("KS_MIRROR_LANDING", "0")
    store = DuckDBStore(db_path=tmp_path / "landing.duckdb")
    await store.connect()
    async with store.connection() as conn:
        conn.execute("INSERT INTO orders (id, source_id, status_id, grand_total, "
                     "ordered_at) VALUES (1, 1, 999, 100.00, NULL)")
        conn.execute("INSERT INTO order_products (id, order_id, product_id, name, "
                     "quantity, price_sold) VALUES (5001, 5, 700, 'x', 1, 10.00)")
    yield store
    await store.close()


class TestTheScanSkipsThem:
    @pytest.mark.asyncio
    async def test_they_file_on_duckdb_and_nothing_under_the_chain(self, planted, flags):
        async with planted.connection() as conn:
            filed = {i.check_name for i in data_quality.check_internal_integrity(conn)}
        assert {"fk_orphan_order_products_order_id", "not_null_orders_ordered_at",
                "value_domain_orders_status_id", "orders_without_line_items"} <= filed

        chain_latch.latch(pg_orders_write.CHAIN, pg_orders_write.WRITE_ENV)
        chain_latch._latched = None
        try:
            async with planted.connection() as conn:
                filed = {i.check_name for i in data_quality.check_internal_integrity(conn)}
        finally:
            chain_latch.release(pg_orders_write.CHAIN)
            chain_latch._latched = None
        assert not (filed & LANDING), filed & LANDING
