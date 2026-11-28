"""К2's harness, step 1: chain 6a's dictionary written by Python and by the
Rust worker from the same bytes, compared with tolerance zero (the plan,
revision 4).

Python writes what it writes in production, `bronze.expense_types`, through
`AnalyticsStore.upsert_expense_types`, the one path the full sync takes. The
Rust worker writes its shadow, `bronze.expense_types_rust`, and the shadow's
row `rust_shadow.bronze.expense_types` in `meta.mirror_state` — as its own
login where the harness made one (`deploy/rust/ks_rust_worker.sql`), so a
grant it lacks fails here. Each batch is one file: Python reads it with
`json`, the worker with serde_json, so both start from the same bytes. Then
the two tables are compared row for row, with which rows the batch touched
and whether each side wrote or refused.

Two batches are expected to differ, and are named: the Rust port refuses
`str.title()` of text that is not ASCII, and what Python's `json` reads and
JSON does not (`NaN`). Both are refusals — the shadow keeps what it had —
never a different row.

The shadow table and its grants are made here, by the statements the
revision that brings the shadow to production will run. Until that revision
(after Z3) nothing in production holds them.

Runs where `rust/harness/k2-expense-types.sh` or CI's `k2` job starts it: a
throwaway Postgres 17.2 (`postgres/initdb`, `alembic upgrade head`) and the
worker built from `rust/`. It empties the tables it uses. Every name in it is
synthetic: the repository is public.
"""
from __future__ import annotations

import json
import os
import subprocess
from typing import Any, Dict, List, Tuple
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
import pytest_asyncio

DSN = os.getenv("KS_PG_DSN")
# Two markers, as К1's harness has: CI's suite has a Postgres and no worker,
# so the store's skip says "needs a live PostgreSQL" (the CI step that refuses
# store skips reads that phrase) and the worker's must not name Postgres.
pytestmark = [
    pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL (KS_PG_DSN)"),
    pytest.mark.skipif(
        not os.getenv("KS_RUST_WORKER_BIN"),
        reason="needs the Rust worker (KS_RUST_WORKER_BIN): "
               "rust/harness/k2-expense-types.sh runs it",
    ),
]

LIVE = "bronze.expense_types"
SHADOW = "bronze.expense_types_rust"
STATE = "rust_shadow.bronze.expense_types"
OWNER = "owner:bronze.expense_types"
WORKER = "ks_rust_worker"
KEY = "dictionaries.expense_types."
ANSWER, ERROR = "expense_types", "expense_types_error"

# What the revision that brings the shadow to production runs; until then,
# this harness.
SHADOW_DDL = f"CREATE TABLE IF NOT EXISTS {SHADOW} (LIKE {LIVE} INCLUDING ALL)"
SHADOW_GRANTS = (
    f"GRANT USAGE ON SCHEMA bronze TO {WORKER}",
    f"GRANT SELECT, INSERT, UPDATE ON {SHADOW} TO {WORKER}",
)

# The dictionary as an earlier sync left it, in both tables.
SEEDED = [
    (1, "Стара назва", "old_alias", True),
    (2, "Unchanged", None, False),
    (900, "Kept, though no longer served", "kept", None),
]
SEEDED_AT = "2001-01-01 00:00:00+00"


def _dictionary() -> List[Dict[str, Any]]:
    """Production's size and shapes, as the chain's own writer test builds
    them: a display name, a key with an alias, a key without one."""
    rows = []
    for i in range(1, 28):
        if i % 3 == 1:
            row: Dict[str, Any] = {"id": i, "name": f"Витрата {i}", "alias": f"cost_{i}"}
        elif i % 3 == 2:
            row = {"id": i, "name": f"{KEY}kind_{i}", "alias": f"alias_for_{i}"}
        else:
            row = {"id": i, "name": f"{KEY}bare_kind_{i}"}
        if i % 5 == 0:
            row["is_active"] = False
        elif i % 7 == 0:
            row["is_active"] = True
        rows.append(row)
    return rows


def _text(payloads: Any) -> str:
    return json.dumps(payloads, ensure_ascii=False)


VALID = {"id": 11, "name": "Valid"}

# Each after a valid payload, so a refusal that wrote half is caught.
REFUSED: List[Tuple[str, Any]] = [
    ("a null name", {"id": 12, "name": None}),
    ("a number for a name", {"id": 12, "name": 5}),
    ("a truthy alias on a key that is not text", {"id": 12, "name": KEY + "x", "alias": 7}),
    ("a stored alias that is not text", {"id": 12, "name": "x", "alias": 7}),
    ("a falsy alias that is not text", {"id": 12, "name": KEY + "x", "alias": 0}),
    ("is_active a number", {"id": 12, "name": "x", "is_active": 1}),
    ("is_active text", {"id": 12, "name": "x", "is_active": "true"}),
    ("a string id", {"id": "12", "name": "x"}),
    ("no id", {"name": "x"}),
    ("an id past int4", {"id": 2147483648, "name": "x"}),
    ("a huge id", {"id": 10 ** 20, "name": "x"}),
    ("a payload that is not an object", "delivery"),
]

WRITES, REFUSES, GAP = "writes", "refuses", "gap"
BATCHES: List[Tuple[str, str, str]] = [
    ("the production shape", _text(_dictionary()), WRITES),
    ("a rename in place, and a type no longer served is kept",
     _text([{"id": 1, "name": "Доставка Нова пошта", "alias": "np"},
            {"id": 2, "name": KEY + "bank_fee"}]), WRITES),
    ("one id twice: the later payload wins",
     _text([{"id": 3, "name": "First"}, {"id": 3, "name": "Second", "alias": "second"}]), WRITES),
    ("one key twice in an object: the later value wins",
     '[{"id": 4, "name": "First", "name": "Second"}]', WRITES),
    ("float ids truncate and true is 1",
     _text([{"id": 5.0, "name": "Five"}, {"id": 6.9, "name": "Six"},
            {"id": -7.5, "name": "Minus seven"}, {"id": True, "name": "One"}]), WRITES),
    ("the edges of int4",
     _text([{"id": 2147483647, "name": "Max"}, {"id": -2147483648, "name": "Min"}]), WRITES),
    ("defaults and falsy aliases",
     _text([{"id": 8}, {"id": 9, "name": "x", "is_active": None},
            {"id": 10, "name": KEY + "return_shipping", "alias": ""}]), WRITES),
    ("an empty batch", "[]", WRITES),
    *[(f"refused: {label}", _text([VALID, bad]), REFUSES) for label, bad in REFUSED],
    ("refused: a NUL in a name", _text([VALID]).rstrip("]") + ', {"id": 12, "name": "a\\u0000b"}]',
     REFUSES),
    ("refused: a lone surrogate",
     _text([VALID]).rstrip("]") + ', {"id": 12, "name": "a\\ud800b"}]', REFUSES),
    ("refused: NaN for an id", _text([VALID]).rstrip("]") + ', {"id": NaN, "name": "x"}]', REFUSES),
    # The two named differences.
    ("gap: titlecasing text that is not ASCII",
     _text([{"id": 13, "name": KEY + "x", "alias": "доставка_курʼєром"}]), GAP),
    ("gap: NaN where nothing reads it", '[{"id": 14, "name": "x", "extra": NaN}]', GAP),
]


async def _clean(pool) -> None:
    async with pool.acquire() as conn:
        for table in (LIVE, SHADOW):
            await conn.execute(f"DELETE FROM {table}")
        await conn.execute("DELETE FROM meta.mirror_state WHERE table_name = $1", STATE)
        await conn.execute("DELETE FROM meta.chain_watermarks WHERE key = $1", OWNER)


async def _seed(pool) -> None:
    await _clean(pool)
    async with pool.acquire() as conn:
        for table in (LIVE, SHADOW):
            await conn.executemany(
                f"INSERT INTO {table} (id, name, alias, is_active, mirrored_at) "
                f"VALUES ($1, $2, $3, $4, TIMESTAMPTZ '{SEEDED_AT}')", SEEDED)


@pytest_asyncio.fixture
async def stores():
    """A live Postgres with the shadow and its grants, and the Python
    writer's pool pointed at it."""
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    async with pool.acquire() as conn:
        await conn.execute(SHADOW_DDL)
        if await conn.fetchval("SELECT 1 FROM pg_roles WHERE rolname = $1", WORKER):
            for grant in SHADOW_GRANTS:
                await conn.execute(grant)
    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
            patch("core.pg.require_revision", new=AsyncMock()):
        yield pool
    await _clean(pool)
    await pool.close()


async def _python(path) -> Dict[str, Any]:
    """What `full_sync` does with the payloads, and what it records."""
    from core.store import AnalyticsStore

    try:
        payloads = json.loads(path.read_text(encoding="utf-8"))
        return {"written": await AnalyticsStore().upsert_expense_types(payloads)}
    except Exception as exc:  # `full_sync`'s `expense_types_error`
        return {"error": f"{type(exc).__name__}: {exc}"}


def _rust(path) -> Tuple[Dict[str, Any], int, str]:
    env = dict(os.environ)
    if os.getenv("KS_K2_WORKER_PG_DSN"):
        env["KS_PG_DSN"] = os.environ["KS_K2_WORKER_PG_DSN"]
    process = subprocess.run(
        [os.environ["KS_RUST_WORKER_BIN"], "expense-types", "--payloads", str(path)],
        capture_output=True, text=True, timeout=120, env=env,
    )
    assert process.stdout.strip(), process.stderr[-4000:]
    return json.loads(process.stdout), process.returncode, process.stderr


async def _rows(pool, table: str) -> List[Tuple[Any, ...]]:
    async with pool.acquire() as conn:
        return [tuple(r) for r in await conn.fetch(
            f"SELECT id, name, alias, is_active FROM {table} ORDER BY id")]


async def _touched(pool, table: str) -> Tuple[List[int], int]:
    """The rows a batch wrote, and how many stamps they carry: one, because a
    batch is one transaction and `now()` is the transaction's."""
    async with pool.acquire() as conn:
        ids = [r[0] for r in await conn.fetch(
            f"SELECT id FROM {table} WHERE mirrored_at > TIMESTAMPTZ '{SEEDED_AT}' ORDER BY id")]
        stamps = await conn.fetchval(
            f"SELECT count(DISTINCT mirrored_at) FROM {table} "
            f"WHERE mirrored_at > TIMESTAMPTZ '{SEEDED_AT}'")
    return ids, stamps


async def _state(pool) -> Tuple[Any, ...]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT failures_since_ok, last_error IS NOT NULL, last_rows, "
            "last_ok_at IS NOT NULL FROM meta.mirror_state WHERE table_name = $1", STATE)
    return tuple(row) if row else ()


@pytest.mark.asyncio
@pytest.mark.parametrize("label, text, expected", BATCHES, ids=[b[0] for b in BATCHES])
async def test_python_and_rust_write_the_same_dictionary(stores, tmp_path, label, text,
                                                         expected):
    pool = stores
    await _seed(pool)
    path = tmp_path / "payloads.json"
    path.write_text(text, encoding="utf-8")

    python = await _python(path)
    rust, code, stderr = _rust(path)
    live, shadow = await _rows(pool, LIVE), await _rows(pool, SHADOW)
    state = await _state(pool)

    if expected == GAP:
        assert "error" not in python, python
        assert ERROR in rust and code == 1, (rust, stderr[-2000:])
        assert shadow == sorted(SEEDED), f"{label}: a refused batch wrote the shadow"
        assert state[:2] == (1, True), state
        return

    # Python first, so a batch both sides refuse cannot pass for one both
    # write, nor the other way round.
    assert ("error" in python) == (expected == REFUSES), f"{label}: Python {python}"
    assert ("error" in python) == (ERROR in rust), (python, rust)
    assert code == (1 if "error" in python else 0), stderr[-2000:]
    assert shadow == live, f"{label}: the shadow is not the dictionary"
    live_touched, live_stamps = await _touched(pool, LIVE)
    shadow_touched, shadow_stamps = await _touched(pool, SHADOW)
    assert shadow_touched == live_touched, f"{label}: the batch touched other rows"
    assert live_stamps <= 1 and shadow_stamps <= 1, "a batch is one transaction"
    if "error" in python:
        assert live == sorted(SEEDED), f"{label}: Python's refusal wrote something"
        assert state[:2] == (1, True), state
    else:
        assert rust == {ANSWER: python["written"]}
        assert state == (0, False, python["written"], True)


@pytest.mark.asyncio
async def test_a_refusal_counts_and_a_success_starts_over(stores, tmp_path):
    """The shadow's row says what К1's say: failures in a row since the last
    success, the last error, the rows of the last success."""
    pool = stores
    await _seed(pool)
    bad, good = tmp_path / "bad.json", tmp_path / "good.json"
    bad.write_text(_text([{"id": 12, "name": None}]), encoding="utf-8")
    good.write_text(_text(_dictionary()), encoding="utf-8")

    for _ in range(2):
        assert ERROR in _rust(bad)[0]
    assert (await _state(pool))[:2] == (2, True)
    assert _rust(good)[0] == {ANSWER: 27}
    assert await _state(pool) == (0, False, 27, True)
    assert ERROR in _rust(bad)[0]
    assert await _state(pool) == (1, True, 27, True), "a failure keeps the last success"


@pytest.mark.asyncio
async def test_the_worker_writes_the_shadow_and_cannot_touch_the_dictionary(stores):
    """The login the shadow ships as holds exactly what it writes."""
    async with stores.acquire() as conn:
        if not await conn.fetchval("SELECT 1 FROM pg_roles WHERE rolname = $1", WORKER):
            pytest.fail(f"{WORKER} is not here: the harness makes it from "
                        "deploy/rust/ks_rust_worker.sql")
        can = {
            (table, privilege): await conn.fetchval(
                "SELECT has_table_privilege($1, $2, $3)", WORKER, table, privilege)
            for table in (LIVE, SHADOW)
            for privilege in ("SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE")
        }
    assert {key for key, granted in can.items() if granted} == {
        (SHADOW, "SELECT"), (SHADOW, "INSERT"), (SHADOW, "UPDATE")}
