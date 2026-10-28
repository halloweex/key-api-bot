"""`/expenses` answers in Postgres what DuckDB answered.

Six read methods across two shapes: three that read only what a human typed —
`app.manual_expenses`, here since revision 0018 — and three that read the
order-level costs KeyCRM serves, which revision 0020 mirrors.

ONE ENGINE SINCE STEP 14 (S14-07)

`/expenses` reads Postgres and nothing else. Every call below is compared with
a literal (`tests/integration/golden/expenses.json`): DuckDB's answer on this
file's fixture, pinned to 2026-09-15, recorded while the same run asserted
Postgres answered each one equally (rule 9 of the step-14 plan; all fourteen
agreed). Postgres derives Silver from the seeded Bronze — the production path
— and the expenses and their types go through the shared parse
(`core.landing_rows`), as the sync lands them. The store's DuckDB connection
is a trap, so an answer can only be Postgres's.

The fixture is built to attack the two things the port changed in both
engines:

* **the fan-out.** Order 1 carries three expenses and order 2 carries two, so
  a query that sums `grand_total` across the join reports their revenue three
  and two times over. That was live on production — ₴314,365.50 over 90 days.
* **the move to Silver.** The old bodies joined raw `orders` and narrowed with
  an `EXISTS` against Silver; these read Silver directly. A return and a b2b
  order are here so the flags and the classification are actually exercised.

`tests/unit/test_pg_expenses_read.py` proves the wiring and the history gate.

Skipped without `KS_PG_DSN`; CI and `deploy/gate_with_stores.sh` supply one.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone

import pytest
import pytest_asyncio

from tests.integration.pg_tab_seed import (
    call_key, comparable, derive, golden, seed, store_that_never_reaches_duckdb,
    tab_in_postgres,
)

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(
    not DSN, reason="needs a live PostgreSQL at KS_PG_DSN",
)

# Pinned: the answers are literals taken from DuckDB on this fixture, so the
# calendar under them cannot move with the run. No body here reads the clock
# in SQL; every window is a parameter.
TODAY = date(2026, 9, 15)
NOW = datetime(2026, 9, 15, 9, 0, tzinfo=timezone.utc)   # midday Kyiv
W = (TODAY - timedelta(days=25), TODAY)

# (id, source_id, grand_total, days_ago, status_id, manager_id)
# status 19 is KeyCRM's lost/cancel group. Manager 15 is `B2B_MANAGER_ID`, and
# the Silver CASE reads that constant rather than the `managers` table, so
# order 4 classifies as b2b with no manager row to seed.
ORDERS = [
    (1, 1, 1000.0, 2, 1, None),
    (2, 1, 2000.0, 3, 1, None),
    (3, 2, 500.0, 4, 1, None),
    (4, 1, 9000.0, 5, 1, 15),
    (5, 1, 700.0, 6, 19, None),
]
TYPES = [
    (1, "dictionaries.expense_types.delivery", "delivery", True),
    (2, "Комісія", "commission", True),
    (3, "Retired", "retired", False),
]
# (id, order_id, expense_type_id, amount)
EXPENSES = [
    (10, 1, 1, 100.0), (11, 1, 2, 50.0), (12, 1, 1, 25.0),   # three on one order
    (13, 2, 1, 200.0), (14, 2, 2, 30.0),                      # two on another
    (15, 3, 1, 40.0),
    (16, 4, 1, 500.0),                                        # b2b
    (17, 5, 1, 60.0),                                         # on a return
]
SPEND = [
    (1, 3, "marketing", "Facebook Ads", 500.0, "facebook"),
    (2, 4, "marketing", "TikTok Ads", 250.0, "tiktok"),
    (3, 5, "salary", "Salary", 9999.0, None),
]


async def _populate(conn, pool):
    from core.landing_rows import (
        EXPENSE_COLUMNS, EXPENSE_TYPE_COLUMNS, expense_rows, expense_type_rows,
    )

    await seed(conn, orders=[
        (oid, src, status, total, NOW - timedelta(days=days), oid, mgr, None)
        for oid, src, total, days, status, mgr in ORDERS])
    await derive(pool)
    # The shared parse, as the sync lands them: the localisation-key name is
    # resolved there, not here.
    types = expense_type_rows([
        {"id": i, "name": n, "alias": a, "is_active": act} for i, n, a, act in TYPES])
    await conn.executemany(
        f"INSERT INTO bronze.expense_types ({', '.join(EXPENSE_TYPE_COLUMNS)}) "
        f"VALUES ($1,$2,$3,$4)", [tuple(r) for r in types])
    expenses = expense_rows([
        {"id": oid, "expenses": [
            {"id": eid, "expense_type_id": tid, "amount": amt,
             "description": f"exp {eid}", "status": "paid", "payment_date": None,
             "created_at": NOW - timedelta(days=1)}
            for eid, o, tid, amt in EXPENSES if o == oid]}
        for oid in sorted({o for _, o, _, _ in EXPENSES})])
    await conn.executemany(
        f"INSERT INTO bronze.expenses ({', '.join(EXPENSE_COLUMNS)}) "
        f"VALUES ({', '.join(f'${i}' for i in range(1, len(EXPENSE_COLUMNS) + 1))})",
        [tuple(r) for r in expenses])
    await conn.executemany(
        "INSERT INTO app.manual_expenses (id, expense_date, category, expense_type,"
        " amount, currency, note, created_at, platform)"
        " VALUES ($1,$2,$3,$4,$5,'UAH',NULL,$6,$7)",
        [(eid, TODAY - timedelta(days=days), cat, etype, amt,
          NOW - timedelta(days=days), plat)
         for eid, days, cat, etype, amt, plat in SPEND])
    # A backfilled mirror, the only state in which the two methods that read
    # the landing are answered at all (`_expenses_run`'s history gate).
    # Replaced whole: a row another test left could carry a `last_attempted_at`
    # the check constraint would hold a fresh `last_ok_at` against.
    await conn.execute(
        "DELETE FROM meta.mirror_state WHERE table_name = 'bronze.expenses'")
    await conn.execute(
        """
        INSERT INTO meta.mirror_state (table_name, last_ok_at, backfilled_at)
        VALUES ('bronze.expenses', now(), now())
        """
    )


@pytest_asyncio.fixture
async def in_postgres(tmp_path, monkeypatch):
    """The tab's store, never connected: its DuckDB connection is a trap, so
    an answer below can only be Postgres's."""
    from core import pg_expenses_read

    # Module state, and a latch that only closes forward.
    monkeypatch.setattr(pg_expenses_read, "_backfilled", False)
    monkeypatch.setattr(pg_expenses_read, "_checked_at", 0.0)
    monkeypatch.setattr(pg_expenses_read, "_check_failed", None)
    async with tab_in_postgres(DSN, monkeypatch, _populate) as pool:
        try:
            yield store_that_never_reaches_duckdb(tmp_path, monkeypatch, "/expenses")
        finally:
            async with pool.acquire() as conn:
                await conn.execute(
                    "DELETE FROM meta.mirror_state WHERE table_name = 'bronze.expenses'")


GOLDEN = golden("expenses")

CALLS = (
    ("get_expense_types", (), {}),
    ("get_expense_summary", W, {}),
    ("get_expense_summary", W, {"sales_type": "all"}),
    ("get_expense_summary", W, {"sales_type": "b2b"}),
    ("get_expense_summary", W, {"source_id": 1}),
    ("get_expense_summary", W, {"expense_type_id": 1}),
    ("get_profit_analysis", W, {}),
    ("get_profit_analysis", W, {"sales_type": "all"}),
    ("get_profit_analysis", W, {"source_id": 2}),
    ("list_expenses", (), {}),
    ("list_expenses", (), {"limit": 2}),
    ("list_expenses", (), {"category": "marketing"}),
    ("get_ad_spend_by_platform", W, {}),
    ("get_expenses_summary", (), {}),
)


def _key(name, args, kwargs):
    return call_key(name, [[a.isoformat() for a in args], kwargs])


class TestPostgresAnswersWhatDuckDBAnswered:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "name,args,kwargs", CALLS,
        ids=[f"{n}{tuple(k.values()) if k else ''}" for n, _, k in CALLS],
    )
    async def test_the_whole_result_is_the_literal(self, in_postgres, name, args, kwargs):
        answer = await getattr(in_postgres, name)(*args, **kwargs)
        assert comparable(answer) == GOLDEN[_key(name, args, kwargs)], (
            f"{name}{args}{kwargs}: {answer!r}")

    def test_every_literal_is_a_call_and_every_call_has_one(self):
        assert set(GOLDEN) == {_key(n, a, k) for n, a, k in CALLS}


@pytest.mark.asyncio
async def test_revenue_is_not_multiplied_by_the_expense_count(in_postgres):
    """Order 1 has three expenses and order 2 has two. Under the old join
    their ₴1 000 and ₴2 000 were counted three and two times; the truth is
    each once."""
    out = await in_postgres.get_profit_analysis(*W)
    retail = [o for o in ORDERS if o[5] is None and o[4] != 19]
    expected = round(sum(o[2] for o in retail), 2)
    revenue = round(sum(out["chart"]["datasets"][0]["data"]), 2)
    assert revenue == expected, (
        f"revenue {revenue} against a true {expected} — the join fans out")


@pytest.mark.asyncio
async def test_the_expense_total_survives_the_fix(in_postgres):
    """Folding the expenses before the join must not lose any of them."""
    out = await in_postgres.get_profit_analysis(*W)
    retail_ids = {o[0] for o in ORDERS if o[5] is None and o[4] != 19}
    expected = round(sum(a for _, o, _, a in EXPENSES if o in retail_ids), 2)
    assert round(sum(out["chart"]["datasets"][1]["data"]), 2) == expected


@pytest.mark.asyncio
async def test_the_localisation_key_arrives_resolved(in_postgres):
    """The parse that had to move before the table gained a second writer."""
    names = {t["name"] for t in await in_postgres.get_expense_types()}
    assert "Delivery" in names
    assert not any(n.startswith("dictionaries.") for n in names)


@pytest.mark.asyncio
async def test_an_inactive_type_is_absent(in_postgres):
    assert "Retired" not in {t["name"] for t in await in_postgres.get_expense_types()}
