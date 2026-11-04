"""Python against Rust on the expenses router's four GET routes, by
contract: `/api/expense-types`, `/api/expenses/summary`,
`/api/expenses/profit` and the manual list `GET /api/expenses`.

`web.main.app` in this process under `KS_READ_EXPENSES=postgres`, as
production runs it, and the pilot over HTTP, on one Postgres seeded through
production's derivations (`tests/rust_wave1_fixture.py`) plus what these
routes read on top: the type dictionary, the order-level expenses with
their history marked as carried across, and the hand-typed expenses. The
machinery is `tests/rust_wave1.py`'s.

THE CONTRACT: status, Content-Type and body, byte for byte.

What the frontend can send, read out of the TypeScript: the summary's and
the profit analysis' hooks (`frontend_contract.expenses()` — mounted on no
page today, so what they would send from any page but /traffic); the type
list, asked bare; and the manual list, built by hand on the dashboard (the
store's period as it stands, `custom` included, a category filter, 50) and
on /traffic (the period, `marketing`, 20). Then the Kyiv midnights — the
manual list reads its own clock, which is pinned here too — edges beyond the
contract the bytes still settle, the 503s, and the three limits.

THE REST: the status alone, against DIVERGENCES.

Skipped without the pilot or a Postgres; `.github/workflows/rust-pilot.yml`
runs it with both. With `KS_RUST_EXPENSES_REPORT` set, the counts go there.
"""
from __future__ import annotations

import asyncio
import datetime as datetime_module
import json
import os
import random
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, List, Tuple
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio

from tests import frontend_contract as fc
from tests import rust_wave1
from tests.rust_wave1 import PICKER_RANGES, Divergence

asyncpg = pytest.importorskip("asyncpg")
httpx = pytest.importorskip("httpx")

PILOT = os.getenv("KS_RUST_PILOT_URL", "").rstrip("/")
DSN = os.getenv("KS_PG_DSN")
REPORT_PATH = os.getenv("KS_RUST_EXPENSES_REPORT")

pytestmark = [
    pytest.mark.skipif(not PILOT, reason="needs the Rust pilot service at KS_RUST_PILOT_URL"),
    pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN"),
]

KYIV = ZoneInfo("Europe/Kyiv")
CONTRACT = fc.expenses()
ENV = {"KS_READ_EXPENSES": "postgres", "KS_READ_LOOKUPS": "postgres"}
SUMMARY, PROFIT = CONTRACT.endpoints
TYPES = fc.expense_types_path()
MANUAL = "/api/expenses"
FETCHES = fc.manual_expenses_fetches()
# The filter store's period, which both fetches send whenever it is set.
STORE_PERIODS = (*CONTRACT.periods, "custom")

# The type dictionary: a name twice (the sort's tie on `id`), an inactive
# type and one whose flag is NULL (`is_active = TRUE` keeps neither), and no
# row at all for type 99, which expenses carry (`COALESCE(et.name, 'Other')`).
EXPENSE_TYPES = (
    (1, "Доставка", "delivery", True), (2, "Комісія", "commission", True),
    (3, "Packaging", None, True), (4, "Packaging", "packaging-2", True),
    (5, "Old type", "old", False), (6, "Unflagged", "unflagged", None),
    (7, "Реклама", "ads", True),
)
EXPENSE_TYPE_IDS = (1, 2, 3, 4, 5, 6, 7, 99)
CATEGORIES = (*fc.expense_categories(), "misc")    # one the filter never offers
SEEDED_TABLES = ("bronze.expenses", "bronze.expense_types", "app.manual_expenses")


def _expenses(order_ids: List[int], rng: random.Random) -> List[tuple]:
    """One order in three carries expenses, some two or three — the fan-out
    the profit analysis folds before its join."""
    rows, next_id = [], 1
    for oid in order_ids:
        if oid % 3:
            continue
        for _ in range(rng.choice((1, 1, 1, 2, 3))):
            amount = Decimal(rng.randint(100, 90_000)) / 100
            rows.append((next_id, oid, rng.choice(EXPENSE_TYPE_IDS), amount,
                         rng.choice(("", "Нова пошта", None)), "paid", None, None))
            next_id += 1
    return rows


def _manual(rng: random.Random) -> List[tuple]:
    """Typed expenses around the pinned clock's Wednesday, 2026-10-21: today,
    the week, the month, last month and before; one created_at shared by two
    rows of one day (the sort's tie on `id`), stamps with and without
    microseconds, and a NULL `updated_at`, `note`, `currency`, `platform`."""
    anchor = date(2026, 10, 21)
    base = datetime(2026, 10, 21, 7, 30, tzinfo=timezone.utc)
    rows = []
    for i in range(1, 61):
        day = anchor - timedelta(days=rng.choice((0, 0, 1, 2, 3, 6, 9, 15, 25, 35, 50, 80, 400)))
        created = base - timedelta(days=(anchor - day).days, seconds=i * 61,
                                   microseconds=0 if i % 4 else 250_500)
        if i in (7, 8):
            day, created = anchor, base        # the same day, the same stamp
        rows.append((
            i, day, rng.choice(CATEGORIES), rng.choice(("Facebook Ads", "Зарплата", "Податки")),
            Decimal(rng.randint(1_000, 5_000_000)) / 100,
            rng.choice(("UAH", "UAH", "USD", None)),
            rng.choice((None, "", "квітень, частина", 'a "quoted" note')),
            created, None if i % 3 else created + timedelta(hours=5, microseconds=7),
            rng.choice(("facebook", "tiktok", "google", None)),
        ))
    return rows


async def _seed(pool) -> Dict[str, object]:
    from tests import rust_wave1_fixture

    seed = await rust_wave1_fixture.seed(pool)
    rng = random.Random(20261029)
    async with pool.acquire() as conn:
        for table in SEEDED_TABLES:
            await conn.execute(f"DELETE FROM {table}")
        await conn.copy_records_to_table(
            "expense_types", schema_name="bronze",
            columns=("id", "name", "alias", "is_active"), records=EXPENSE_TYPES)
        order_ids = [r["id"] for r in await conn.fetch("SELECT id FROM bronze.orders ORDER BY id")]
        expenses = _expenses(order_ids, rng)
        await conn.copy_records_to_table(
            "expenses", schema_name="bronze",
            columns=("id", "order_id", "expense_type_id", "amount", "description", "status",
                     "payment_date", "created_at"), records=expenses)
        manual = _manual(rng)
        await conn.copy_records_to_table(
            "manual_expenses", schema_name="app",
            columns=("id", "expense_date", "category", "expense_type", "amount", "currency",
                     "note", "created_at", "updated_at", "platform"), records=manual)
        previous = await conn.fetchval(
            "SELECT backfilled_at FROM meta.mirror_state WHERE table_name = 'bronze.expenses'")
        await conn.execute(
            "INSERT INTO meta.mirror_state (table_name, backfilled_at) "
            "VALUES ('bronze.expenses', now()) "
            "ON CONFLICT (table_name) DO UPDATE SET backfilled_at = EXCLUDED.backfilled_at")
    return {**seed, "expenses": len(expenses), "manual_expenses": len(manual),
            "expense_types": len(EXPENSE_TYPES), "_backfilled_before": previous}


async def _clear(pool, previous) -> None:
    from tests import rust_wave1_fixture

    async with pool.acquire() as conn:
        for table in SEEDED_TABLES:
            await conn.execute(f"DELETE FROM {table}")
        await conn.execute(
            "UPDATE meta.mirror_state SET backfilled_at = $1 WHERE table_name = 'bronze.expenses'",
            previous)
    await rust_wave1_fixture.clear(pool)


REPORT = rust_wave1.Report()


@pytest.fixture(scope="module")
def seeded():
    async def _with_pool(work, *args):
        pool = await asyncpg.create_pool(DSN, min_size=1, max_size=2)
        try:
            return await work(pool, *args)
        finally:
            await pool.close()

    seed = asyncio.run(_with_pool(_seed))
    previous = seed.pop("_backfilled_before")
    REPORT.data["seed"] = seed
    yield
    asyncio.run(_with_pool(_clear, previous))
    REPORT.write(REPORT_PATH)


def _relatch(mp) -> None:
    """`backfilled()` latches true for the process and holds a false for a
    minute; each phase asks it afresh, as a new process would."""
    from core import pg_expenses_read

    mp.setattr(pg_expenses_read, "_backfilled", False)
    mp.setattr(pg_expenses_read, "_checked_at", 0.0)
    mp.setattr(pg_expenses_read, "_check_failed", None)


@pytest_asyncio.fixture
async def pair(seeded, duckdb_off):
    # The manual list reads `datetime.now(...)` through an import inside the
    # route; the module attribute is what that import finds.
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(datetime_module, "datetime", rust_wave1.Clock)
        _relatch(mp)
        async with rust_wave1.services(DSN, PILOT, ENV) as both:
            both.relatch = lambda: _relatch(mp)
            yield both


def _windows() -> List[Dict[str, str]]:
    windows = [{"period": p} for p in CONTRACT.periods]
    windows += [{"period": "custom", "start_date": s, "end_date": e} for s, e in PICKER_RANGES]
    return windows


def _manual_requests() -> List[str]:
    return [fc.manual_expenses_query(period=period, category=category, limit=limit)
            for _, categories, limit in FETCHES
            for period in STORE_PERIODS for category in categories]


def _contract(filters: Dict[str, list]) -> List[Tuple[str, str]]:
    cases: List[Tuple[str, str]] = [(TYPES, "")]
    chosen: List[Dict[str, object]] = [
        {name: value} for name, values in filters.items() for value in values]
    chosen.append({name: values[-1] for name, values in filters.items()})
    for path in CONTRACT.endpoints:
        for window in _windows():
            for sales_type in CONTRACT.sales_types:
                cases.append((path, fc.query(CONTRACT, sales_type=sales_type, **window)))
        for period in CONTRACT.periods:
            for header in chosen:
                cases.append((path, fc.query(CONTRACT, period=period, sales_type="retail",
                                             **header)))
    cases += [(MANUAL, query) for query in _manual_requests()]
    return list(dict.fromkeys(cases))


def _has_rows(path: str, body: bytes) -> bool:
    answer = json.loads(body)
    if path == SUMMARY:
        return bool(answer["metrics"]["expenseCount"])
    if path == PROFIT:
        return bool(answer["metrics"]["totalRevenue"])
    if path == MANUAL:
        return bool(answer["expenses"])
    return bool(answer)


@pytest.mark.asyncio
async def test_the_contract_answers_byte_for_byte(pair):
    from core import duckdb_switch, read_fallback

    filters = await rust_wave1.header_filters(pair.python)
    REPORT.data["lookups"] = filters
    rows, with_rows = [], 0
    for path, query in _contract(filters):
        py, rs = await pair.ask(path, query)
        found = rust_wave1.difference(py, rs)
        rows.append((f"{path}?{query}", py.status_code, found))
        with_rows += not found and py.status_code == 200 and _has_rows(path, py.content)
    differences = REPORT.phase("contract", rows)
    REPORT.data["phases"]["contract"]["with_rows"] = with_rows
    assert not differences, "\n".join(differences[:20])
    assert {200, 400} <= {status for _, status, _ in rows}
    assert with_rows > len(rows) // 2, (with_rows, len(rows))
    assert duckdb_switch.opened() == {}
    assert read_fallback.counts() == {} and read_fallback.refusals() == {}


@pytest.mark.asyncio
async def test_the_shortcuts_turn_at_kyivs_midnights(pair):
    asks = [(path, fc.query(CONTRACT, period=period, sales_type="retail"))
            for path in CONTRACT.endpoints for period in CONTRACT.periods]
    asks += [(MANUAL, fc.manual_expenses_query(period=period, category=None, limit=50))
             for period in STORE_PERIODS]
    rows = []
    for now in rust_wave1.KYIV_INSTANTS:
        for path, query in asks:
            py, rs = await pair.ask(path, query, now=now)
            rows.append((f"{now} {path}?{query}", py.status_code,
                         rust_wave1.difference(py, rs)))
    differences = REPORT.phase("midnights", rows)
    assert not differences, "\n".join(differences[:20])


@pytest.mark.asyncio
async def test_today_on_the_real_clock(pair):
    for path, query in ((SUMMARY, "period=today&sales_type=all"),
                        (MANUAL, "period=week&limit=50")):
        for _ in range(3):
            before = datetime.now(KYIV).date()
            py, rs = await pair.ask(path, query, now=None)
            if datetime.now(KYIV).date() == before:
                break
        assert rust_wave1.difference(py, rs) is None, rust_wave1.difference(py, rs)
        assert py.status_code == 200


# Ten years and a week, to the day: the longest range a page may ask for.
LONGEST = (date(2026, 10, 21) - timedelta(days=3659)).isoformat()
TOO_LONG = (date(2026, 10, 21) - timedelta(days=3660)).isoformat()

# Beyond the contract, and still settled by the bytes.
EDGES: Tuple[Tuple[str, str], ...] = (
    (SUMMARY, "period=last_month&sales_type=all&expense_type_id=1"),
    (SUMMARY, "period=last_month&sales_type=all&expense_type_id=99"),   # no such type
    (SUMMARY, "period=last_month&sales_type=all&expense_type_id=0"),    # falsy: no filter
    (SUMMARY, "period=last_month&sales_type=all&expense_type_id=-3"),
    (SUMMARY, f"start_date={LONGEST}&end_date=2026-10-21&sales_type=all"),
    (PROFIT, f"start_date={LONGEST}&end_date=2026-10-21&sales_type=all"),
    (SUMMARY, "start_date=2026-03-01&end_date=2026-02-01&sales_type=retail"),
    (PROFIT, "start_date=2026-03-01&end_date=2026-02-01&sales_type=retail"),
    (PROFIT, "period=month&sales_type=all&source_id=3"),
    (MANUAL, "limit=1"), (MANUAL, "limit=200"), (MANUAL, "category=misc&limit=50"),
    (MANUAL, "period=last_week&category=&limit=50"), (MANUAL, ""),
    (MANUAL, "period=TODAY&limit=50"), (TYPES, "period=xyz&limit=0"),
)


@pytest.mark.asyncio
async def test_the_edges_answer_byte_for_byte(pair):
    rows = []
    for path, query in EDGES:
        py, rs = await pair.ask(path, query)
        rows.append((f"{path}?{query}", py.status_code, rust_wave1.difference(py, rs)))
    differences = REPORT.phase("edges", rows)
    assert not differences, "\n".join(differences)
    assert {status for _, status, _ in rows} == {200}


@pytest.mark.asyncio
async def test_a_postgres_that_cannot_answer_is_the_same_503(pair):
    """A schema this build does not know; the history of `bronze.expenses`
    not carried across; the type dictionary gone."""
    month = "period=month&sales_type=retail"
    asks = ((TYPES, ""), (SUMMARY, month), (PROFIT, month), (MANUAL, "period=month&limit=50"))
    async with pair.pool.acquire() as conn:
        revision = await conn.fetchval("SELECT version_num FROM meta.alembic_version")
        carried = await conn.fetchval(
            "SELECT backfilled_at FROM meta.mirror_state WHERE table_name = 'bronze.expenses'")
    breaks = (
        ("revision", "UPDATE meta.alembic_version SET version_num = 'wave1_moved'",
         f"UPDATE meta.alembic_version SET version_num = '{revision}'"),
        ("history", "UPDATE meta.mirror_state SET backfilled_at = NULL "
                    "WHERE table_name = 'bronze.expenses'", None),
        ("types", "ALTER TABLE bronze.expense_types RENAME TO expense_types_away",
         "ALTER TABLE bronze.expense_types_away RENAME TO expense_types"),
    )
    rows, statuses = [], {}
    for name, broken, mended in breaks:
        async with pair.pool.acquire() as conn:
            await conn.execute(broken)
        pair.relatch()
        try:
            for path, query in asks:
                py, rs = await pair.ask(path, query)
                rows.append((f"{name} {path}?{query}", py.status_code,
                             rust_wave1.difference(py, rs)))
                statuses[(name, path)] = py.status_code
        finally:
            async with pair.pool.acquire() as conn:
                if mended:
                    await conn.execute(mended)
                else:
                    await conn.execute(
                        "UPDATE meta.mirror_state SET backfilled_at = $1 "
                        "WHERE table_name = 'bronze.expenses'", carried)
            pair.relatch()
    differences = REPORT.phase("unreadable", rows, may_hand_over=True)
    assert not differences, "\n".join(differences)
    # Another revision is a migration window: Rust hands every request to
    # Python, whose image is the schema's. A table neither can read is the
    # same answer from both.
    assert rust_wave1.handed_over(rows) == [r for r, _, _ in rows if r.startswith("revision ")]
    assert {statuses[("revision", path)] for path, _ in asks} == {503}
    # Only the two statements over `bronze.expenses` ask for its history,
    # and only the type list and the summary read the type dictionary.
    assert [statuses[("history", path)] for path, _ in asks] == [200, 503, 503, 200]
    assert [statuses[("types", path)] for path, _ in asks] == [503, 503, 200, 200]
    py, rs = await pair.ask(SUMMARY, month)
    assert py.status_code == 200 and rust_wave1.difference(py, rs) is None


@pytest.mark.asyncio
async def test_each_limit_counts_what_it_counts_on_both(pair):
    """60 a minute on the type list, 30 on the others: a 422 is never
    counted, a 400 the route raises always is."""
    plans = (
        (SUMMARY, 30, ["source_id=abc"] * 3 + ["period=xyz&sales_type=retail"] * 5
         + ["period=month&sales_type=retail"] * 26),
        (MANUAL, 30, ["limit=0"] * 3 + ["period=month&limit=50"] * 31),
        (TYPES, 60, [""] * 61),
    )
    rows = []
    for plan, (path, per_minute, asks) in enumerate(plans):
        key = f"expenses-limit-{pair.run}-{plan}"
        py_statuses, rs_statuses = [], []
        for query in asks:
            py, rs = await pair.ask(path, query, key=key)
            py_statuses.append(py.status_code)
            rs_statuses.append(rs.status_code)
            rows.append((f"{path}?{query}", py.status_code,
                         None if py.status_code == rs.status_code
                         else f"{py.status_code} against {rs.status_code}"))
        refused = sum(1 for s in py_statuses if s == 400)
        unparsed = sum(1 for s in py_statuses if s == 422)
        assert py_statuses == ([422] * unparsed + [400] * refused
                               + [200] * (per_minute - refused) + [429]), (path, py_statuses)
        assert rs_statuses == py_statuses, path
        assert rust_wave1.difference(py, rs) is None, rust_wave1.difference(py, rs)
    REPORT.phase("limit", rows)


# Hostile input, by class: (endpoint, query). The status alone is compared.
HOSTILE: Dict[str, List[Tuple[str, str]]] = {
    "source_id out of range": [(SUMMARY, "source_id=0"), (PROFIT, "source_id=5"),
                               (SUMMARY, "source_id=-1")],
    "source_id not an integer": [(SUMMARY, "source_id=abc"), (PROFIT, "source_id="),
                                 (SUMMARY, "source_id=1.5")],
    "source_id as pydantic reads it": [(SUMMARY, "source_id=1.0"), (PROFIT, "source_id=%204"),
                                       (SUMMARY, "source_id=0_2")],
    "source_id past 64 bits": [(SUMMARY, "source_id=99999999999999999999")],
    "expense_type_id not an integer": [(SUMMARY, "expense_type_id=abc"),
                                       (SUMMARY, "expense_type_id=")],
    "expense_type_id as pydantic reads it": [(SUMMARY, "expense_type_id=1.0"),
                                             (SUMMARY, "expense_type_id=%202")],
    "expense_type_id past int32": [(SUMMARY, "expense_type_id=3000000000")],
    "expense_type_id past 64 bits": [(SUMMARY, "expense_type_id=99999999999999999999")],
    "limit out of bounds": [(MANUAL, "limit=0"), (MANUAL, "limit=201"), (MANUAL, "limit=-1")],
    "limit not an integer": [(MANUAL, "limit=abc"), (MANUAL, "limit="), (MANUAL, "limit=5.5")],
    "limit as pydantic reads it": [(MANUAL, "limit=5.0"), (MANUAL, "limit=%205"),
                                   (MANUAL, "limit=1_0")],
    "limit past 64 bits": [(MANUAL, "limit=99999999999999999999")],
    "category any text": [(MANUAL, "category=%3Cscript%3E"), (MANUAL, "category=%20marketing")],
    "category with a NUL": [(MANUAL, "category=a%00b")],
    "another route's parameter": [(TYPES, "period=xyz&sales_type=bogus"),
                                  (MANUAL, "source_id=abc&sales_type=bogus&start_date=x"),
                                  (PROFIT, "expense_type_id=abc&limit=x&category=y")],
    "period unknown": [(SUMMARY, "period=xyz"), (PROFIT, "period=custom"),
                       (MANUAL, "period=xyz"), (MANUAL, "period=last_28_days")],
    "period empty": [(SUMMARY, "period="), (MANUAL, "period=")],
    "period respelled": [(PROFIT, "period=TODAY"), (MANUAL, "period=%20today")],
    "period repeated": [(SUMMARY, "period=xyz&period=month"),
                        (MANUAL, "period=month&period=xyz")],
    "no window": [(SUMMARY, ""), (PROFIT, "sales_type=all")],
    "sales type unknown": [(SUMMARY, "sales_type=bogus"), (PROFIT, "sales_type=wholesale")],
    "sales type respelled": [(SUMMARY, "sales_type=%20all%20"), (PROFIT, "sales_type=B2B"),
                             (SUMMARY, "sales_type=")],
    "dates malformed": [(SUMMARY, "start_date=2026-02-30&end_date=2026-03-01"),
                        (PROFIT, "start_date=x&end_date=2026-03-01")],
    "dates strptime forgives": [(SUMMARY, "start_date=2026-1-5&end_date=2026-01-29")],
    "dates reversed": [(PROFIT, "start_date=2026-03-01&end_date=2026-02-01")],
    "range past the cap": [(SUMMARY, f"start_date={TOO_LONG}&end_date=2026-10-21"),
                           (PROFIT, "start_date=1999-12-31&end_date=2000-01-02")],
    "one date alone": [(SUMMARY, "start_date=2026-03-01"), (PROFIT, "end_date=2026-03-01")],
    "validators in their order": [
        (SUMMARY, "period=xyz&source_id=9"), (PROFIT, "source_id=9&sales_type=x"),
        (SUMMARY, "sales_type=x&start_date=2026-02-30&end_date=2026-03-01")],
    "a parameter repeated": [(MANUAL, "limit=0&limit=5"), (SUMMARY, "source_id=9&source_id=1")],
}

# Where the two statuses differ on hostile input, and why Rust is not made to
# agree. The pages send no `expense_type_id` at all, a `source_id` from the
# header's four, and a `limit` of 50 or 20.
_WIDTH = "Python's int has no width{}; Rust's i64 does not parse it"
DIVERGENCES: Dict[str, Divergence] = {
    "source_id past 64 bits": Divergence(400, 422, _WIDTH.format(
        ", so validate_source_id refuses it as no source")),
    # Not listed since #335: an expense_type_id past int32, or past 64 bits,
    # is validate_db_int's 400 on both sides; BAD_INPUT compares it byte for
    # byte.
}


@pytest.mark.asyncio
async def test_hostile_input_differs_only_where_listed(pair):
    answers = []
    for kind, requests in HOSTILE.items():
        for path, query in requests:
            py, rs = await pair.ask(path, query)
            answers.append((kind, f"{path}?{query}", py.status_code, rs.status_code))
    REPORT.phase("hostile", [(request, py, None if py == rs else f"{py} against {rs}")
                             for _, request, py, rs in answers])
    REPORT.data["divergences"] = {kind: {"python": d.python, "rust": d.rust, "why": d.why}
                                  for kind, d in DIVERGENCES.items()}
    unlisted, stale = rust_wave1.judge_hostile(answers, DIVERGENCES)
    assert not unlisted, "differences no entry lists:\n" + "\n".join(unlisted)
    assert not stale, f"entries that no longer hold: {stale}"


# #335's 400s, byte for byte: an expense type past `bronze.expenses`'
# int32, either way, and past 64 bits, in the summary's order of checks;
# and a NUL anywhere in a query, which the middleware refuses before routing.
BAD_INPUT = ((SUMMARY, "expense_type_id=2147483648"), (SUMMARY, "expense_type_id=-2147483649"),
             (SUMMARY, "expense_type_id=99999999999999999999&sales_type=x"),
             (SUMMARY, "source_id=1&expense_type_id=-99999999999999999999"),
             (MANUAL, "category=a%00b"), (PROFIT, "period=%00"), (TYPES, "x=%00"))


@pytest.mark.asyncio
async def test_bad_input_is_the_same_400_byte_for_byte(pair):
    rows = []
    for path, query in BAD_INPUT:
        py, rs = await pair.ask(path, query)
        rows.append((f"{path}?{query}", py.status_code, rust_wave1.difference(py, rs)))
    differences = REPORT.phase("bad_input", rows)
    assert not differences, "\n".join(differences)
    assert {status for _, status, _ in rows} == {400}
