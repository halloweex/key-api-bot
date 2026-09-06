"""The five customer-analytics methods actually run.

Written after a refactor deadlocked `get_cohort_retention` and the whole suite
stayed green: the method held DuckDB's store lock and the helper it now calls
took the same non-reentrant lock again. Nothing failed — it simply never
returned, which in production is a dashboard tab that spins forever while the
warehouse rebuild queues behind it.

Two tests' worth of coverage across five methods was what let that happen, and
none of the two called any of them. So this calls each one against a real
store, with a timeout: a deadlock has to fail the suite rather than hang it.

Deliberately a smoke test, not an assertion about numbers. It answers "does
this execute and come back shaped right" — the question the suite could not
answer at all — and leaves what the numbers should be to
`tests/integration/test_cohorts_two_engines.py`, which compares them against a
second engine.
"""
from __future__ import annotations

import asyncio

import pytest

from core.duckdb_store import DuckDBStore

# (method, kwargs, keys the result must carry)
CALLS = (
    ("get_cohort_retention", {}, ("cohorts", "summary")),
    ("get_enhanced_cohort_retention", {}, ("cohorts",)),
    ("get_days_to_second_purchase", {}, ("buckets",)),
    ("get_cohort_ltv", {}, ("cohorts",)),
    ("get_at_risk_customers", {}, ()),
)

TIMEOUT_S = 10


@pytest.fixture
async def store(tmp_path):
    st = DuckDBStore(db_path=tmp_path / "analytics.duckdb")
    await st.connect()
    yield st
    await st.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("name,kwargs,keys", CALLS, ids=[c[0] for c in CALLS])
async def test_it_returns_rather_than_hanging(tmp_path, name, kwargs, keys):
    st = DuckDBStore(db_path=tmp_path / f"{name}.duckdb")
    await st.connect()
    try:
        method = getattr(st, name)
        try:
            result = await asyncio.wait_for(method(**kwargs), timeout=TIMEOUT_S)
        except asyncio.TimeoutError:
            pytest.fail(
                f"{name} did not return within {TIMEOUT_S}s — the store lock is "
                f"not reentrant, so this is almost certainly a nested "
                f"`self.connection()`"
            )
        for key in keys:
            assert key in result, f"{name} returned no {key!r}"
    finally:
        await st.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("sales_type", ("retail", "b2b", "all"))
async def test_every_sales_type_is_accepted(tmp_path, sales_type):
    """`all` must stay a no-op filter rather than becoming a predicate, and
    `internal` orders belong in it — the admin-only category is gated at the
    API, not here."""
    st = DuckDBStore(db_path=tmp_path / f"st-{sales_type}.duckdb")
    await st.connect()
    try:
        out = await asyncio.wait_for(
            st.get_cohort_retention(sales_type=sales_type), timeout=TIMEOUT_S,
        )
        assert "cohorts" in out
    finally:
        await st.close()


@pytest.mark.asyncio
async def test_the_analytics_helper_owns_the_connection(tmp_path):
    """The regression, stated directly: calling the helper from inside a held
    connection is the deadlock, so no analytics method may do it."""
    import ast
    import inspect

    from core.repositories import customers as module

    tree = ast.parse(inspect.getsource(module))
    for node in ast.walk(tree):
        if not isinstance(node, ast.AsyncFunctionDef):
            continue

        uses_helper = any(
            getattr(getattr(c, "func", None), "attr", None) == "_analytics_rows"
            for c in ast.walk(node) if isinstance(c, ast.Call)
        )
        if not uses_helper:
            continue

        opens_its_own = any(
            getattr(getattr(item.context_expr, "func", None), "attr", None)
            == "connection"
            for inner in ast.walk(node) if isinstance(inner, ast.AsyncWith)
            for item in inner.items
        )
        assert not opens_its_own, (
            f"{node.name} calls the analytics helper from inside its own "
            f"self.connection(); the helper takes the same non-reentrant lock "
            f"and the call never returns"
        )
