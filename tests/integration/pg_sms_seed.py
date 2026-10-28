"""`/sms` and the dashboard's access lists, in Postgres alone, for the tests.

Step 14's S14-08 took the DuckDB branches of the SMS store (`_sms_tx`,
`_sms_run`, the audience query) and of the access routers (`_users_run`,
`_perms_run`) away: production has run both on Postgres since 2026-09-06 and
2026-09-07, a constant since S14-00. The tests that proved the tab and the
access lists on a DuckDB fixture — the audience, the freeze, the send's
guards, the delivery reports, the results, approvals and the role matrix —
seed the same rows into the Postgres tables the production reads name
(`silver.orders`, `bronze.*`, `app.*`) and call the same store methods.

`on_postgres` is the whole arrangement: a pool on `KS_PG_DSN`, the tables
emptied before and after, `core.pg.get_pool` answering with that pool, and
the store's DuckDB connection a trap — so an answer a test reads can only be
Postgres's, and a path that reached for DuckDB fails loudly instead of
reading an empty file. `require_revision` is the real one: the suite's
database is migrated to `core.pg.REQUIRED_REVISION`, as CI's is.

The goldens for the two-engine files (rule 9 of the step-14 plan) are in
`tests/integration/golden/`; `relative()` is how their dates are written down,
so a literal recorded on one day reads the same on every other.
"""
from __future__ import annotations

import json
import os
import pathlib
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from typing import Any, Iterable, Mapping, Sequence
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

import pytest

DSN = os.getenv("KS_PG_DSN")
# The skip CI's last step greps for: it fails the job if any test takes it.
needs_postgres = pytest.mark.skipif(
    not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

# What `/sms` reads and writes. The audience reads Silver's orders through
# `silver.order_lines` (a view over orders, lines, products and categories),
# the buyers, the costs, the opt-outs and the buyers' gender; the campaigns
# are the tab's own state.
SMS_TABLES = (
    "app.sms_dlr_events", "app.sms_campaign_members", "app.sms_campaigns",
    "app.sms_audience_presets", "app.marketing_optouts",
    "silver.orders", "bronze.order_products", "bronze.products",
    "bronze.categories", "bronze.buyers", "bronze.offer_stocks",
    "app.buyer_gender",
)
# Who may open the dashboard, and what each level may do there.
ACCESS_TABLES = ("app.dashboard_users", "app.role_permissions")

GOLDEN = pathlib.Path(__file__).resolve().parent / "golden"
KYIV = ZoneInfo("Europe/Kyiv")


def kyiv_today() -> date:
    """The day the audience query counts from (`TODAY_IN_KYIV`)."""
    return datetime.now(KYIV).date()


async def clear(conn, tables: Sequence[str] = SMS_TABLES) -> None:
    await conn.execute(f"TRUNCATE {', '.join(tables)}")


def _trap(what: str):
    def _no_duckdb(*_a, **_k):
        raise AssertionError(f"{what} reached for DuckDB")
    return _no_duckdb


@asynccontextmanager
async def on_postgres(monkeypatch, tables: Sequence[str] = SMS_TABLES, *,
                      what: str = "/sms", max_size: int = 4):
    """A pool on `KS_PG_DSN`, `tables` emptied, `core.pg.get_pool` answering
    with it and every `DuckDBStore`'s connection a trap — for one test.

    A real pool, not one connection: the claim's race and two overlapping
    claims run statements at once, and one connection would serialise them
    and prove nothing."""
    import asyncpg

    from core.duckdb_store import DuckDBStore

    monkeypatch.setattr(DuckDBStore, "connection", _trap(what))
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=max_size)
    try:
        async with pool.acquire() as conn:
            await clear(conn, tables)
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)):
            yield pool
    finally:
        try:
            async with pool.acquire() as conn:
                await clear(conn, tables)
        finally:
            await pool.close()


@asynccontextmanager
async def for_async_tests(request, monkeypatch, tables: Sequence[str] = SMS_TABLES,
                          **kwargs):
    """`on_postgres` for the async tests of a module that also holds plain
    ones: a synchronous test reads no store, so it gets nothing and is not
    skipped without `KS_PG_DSN`."""
    if request.node.get_closest_marker("asyncio") is None:
        yield None
        return
    if not DSN:
        pytest.skip("needs a live PostgreSQL at KS_PG_DSN")
    async with on_postgres(monkeypatch, tables, **kwargs) as pool:
        yield pool


def store(tmp_path, name: str = "never.duckdb"):
    """The repository, on a store that is never connected. Its connection is
    a trap for as long as `on_postgres` is open."""
    from core.duckdb_store import DuckDBStore

    return DuckDBStore(db_path=tmp_path / name)


@asynccontextmanager
async def pg_conn():
    """A connection from the pool `on_postgres` installed."""
    from core.pg import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        yield conn


async def insert(conn, table: str, rows: Iterable[Mapping[str, Any]]) -> None:
    """Rows into `table`, each a mapping of column to value — the column list
    of the first row for all of them."""
    rows = list(rows)
    if not rows:
        return
    columns = list(rows[0])
    await conn.executemany(
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES "
        f"({', '.join(f'${i}' for i in range(1, len(columns) + 1))})",
        [tuple(row[c] for c in columns) for row in rows])


def order_row(*, oid: int, buyer_id, day: date, total, sales_type: str = "retail",
              is_return: bool = False, is_active_source: bool = True,
              source_id: int = 4, source_name: str = "Shopify",
              promocode=None, ordered_at=None, first_order_date=None,
              status_id: int = 1, is_new_customer: bool = False) -> dict:
    """One `silver.orders` row, as the derivation would leave it."""
    return dict(
        id=oid, source_id=source_id, status_id=status_id, grand_total=total,
        ordered_at=ordered_at if ordered_at is not None else day,
        buyer_id=buyer_id, manager_id=None, order_date=day, is_return=is_return,
        sales_type=sales_type, is_active_source=is_active_source,
        source_name=source_name, is_new_customer=is_new_customer,
        buyer_first_order_date=first_order_date, promocode=promocode)


# ─── goldens ─────────────────────────────────────────────────────────────────


def golden(name: str) -> dict:
    """The literal answers for one two-engine file: DuckDB's, on that file's
    fixture, recorded at step 14's S14-08 by a run that asserted Postgres
    answered each one equally (rule 9 of the step-14 plan)."""
    doc = json.loads((GOLDEN / f"{name}.json").read_text(encoding="utf-8"))
    return doc["answers"]


def relative(value: Any, today: date) -> Any:
    """`value` with every date and instant written against `today`, money to
    six places, tuples as lists — the form the goldens are recorded in.

    A date becomes `"D-150"`, an instant `"D-150T08:00:00Z"` (in UTC, so the
    process's timezone cannot move it): the fixtures here count their days
    back from Kyiv's today, so the answer about a buyer who ordered 150 days
    ago is the same answer whichever day the suite runs. An ISO string is
    read as the date or instant it spells; any other string stays as it is.
    """
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, dict):
        return {str(k): relative(v, today) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [relative(v, today) for v in value]
    if isinstance(value, datetime):
        return _instant(value, today)
    if isinstance(value, date):
        return _day(value, today)
    if isinstance(value, str):
        return _iso_text(value, today)
    if hasattr(value, "as_tuple") or isinstance(value, float):
        return round(float(value), 6)
    return value


def _day(d: date, today: date) -> str:
    return f"D{(d - today).days:+d}"


def _instant(dt: datetime, today: date) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    utc = dt.astimezone(timezone.utc)
    return f"{_day(utc.date(), today)}T{utc.strftime('%H:%M:%S')}Z"


def _iso_text(text: str, today: date) -> str:
    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        try:
            return _day(date.fromisoformat(text), today)
        except ValueError:
            return text
    if len(text) >= 19 and text[4] == "-" and text[10] in "T ":
        try:
            return _instant(datetime.fromisoformat(text), today)
        except ValueError:
            return text
    return text


def same_numbers(a, b, *, tol=1e-6, path="result"):
    """Compare two result trees, allowing floats to differ by a hair.

    Not laxity — the documented behaviour of an engine summing rows in an
    order of its choosing. Revenue is a pro-rata allocation summed per buyer,
    float addition is not associative, and the last bit can disagree:
    `6433.333333333334` against `6433.333333333333`. Everything that is not a
    float is compared exactly, which keeps counts, arms and delivery states
    honest."""
    if isinstance(a, dict) and isinstance(b, dict):
        assert a.keys() == b.keys(), f"{path}: different keys {sorted(set(a) ^ set(b))}"
        for key in a:
            same_numbers(a[key], b[key], tol=tol, path=f"{path}.{key}")
    elif isinstance(a, list) and isinstance(b, list):
        assert len(a) == len(b), f"{path}: {len(a)} vs {len(b)} items"
        for i, (x, y) in enumerate(zip(a, b)):
            same_numbers(x, y, tol=tol, path=f"{path}[{i}]")
    elif (isinstance(a, float) or isinstance(b, float)) and not (
            isinstance(a, bool) or isinstance(b, bool)):
        assert a == pytest.approx(b, abs=tol), f"{path}: {a} vs {b}"
    else:
        assert a == b, f"{path}: {a!r} vs {b!r}"


def shifted(day: date, recorded_on: date) -> date:
    """A fixture's day, moved so it stands as far back from Kyiv's today as it
    stood from `recorded_on`, the day its golden was recorded."""
    return day + (kyiv_today() - recorded_on)


__all__ = [
    "ACCESS_TABLES", "DSN", "SMS_TABLES", "clear", "for_async_tests",
    "golden", "insert",
    "kyiv_today", "needs_postgres", "on_postgres", "order_row", "pg_conn",
    "relative", "same_numbers", "shifted", "store",
]
