"""К1's harness: Python's `ch_sync` and the Rust worker's, on one Postgres
and one ClickHouse, compared with tolerance zero (plan, revision 4).

Python writes the tables it writes in production — `silver.orders`,
`gold.daily_revenue`, `history.order_versions` and their `clickhouse.*` rows
in `meta.mirror_state`. The Rust worker writes its shadow: the same names
with `_rust` in the same databases, and the same state rows prefixed
`rust_shadow.`. Every run is then compared four ways:

1. every statement sent to ClickHouse, in order, text and rows byte for byte
   (Python's with its table names mapped to the shadow's);
2. what ClickHouse holds afterwards, table for table, as ClickHouse prints it;
3. the state rows — rows shipped, failures counted, an error or none;
4. the answer, in `_run_ch_sync`'s keys.

And the shadow is judged as Python's daily check judges the live copy:
its Silver read back equals Postgres' Silver, and its Gold equals Postgres'
Gold under `compare_gold_cells` — tolerance zero but the documented kopeck
on `avg_order_value`.

Runs where `rust/harness/k1-ch-sync.sh` starts it: a throwaway Postgres 17.2
(`postgres/initdb`, `alembic upgrade head`), ClickHouse 24.8, the worker
built from `rust/`. It empties the tables it uses. With `KS_K1_REPORT` set it
appends what it measured, one JSON object a line.
"""
from __future__ import annotations

import json
import os
import subprocess
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import patch

import pytest
import pytest_asyncio

from tests import rust_k1_fixture as fixture

pytestmark = pytest.mark.skipif(
    not all(os.getenv(v) for v in ("KS_PG_DSN", "KS_CH_URL", "KS_RUST_WORKER_BIN")),
    reason="needs a throwaway Postgres and ClickHouse and the Rust worker "
           "(KS_RUST_WORKER_BIN): rust/harness/k1-ch-sync.sh runs it",
)

SHADOW = (
    ("silver.orders", "silver.orders_rust"),
    ("gold.daily_revenue", "gold.daily_revenue_rust"),
    ("history.order_versions", "history.order_versions_rust"),
)
ORDER_BY = {
    "silver.orders": "id",
    "gold.daily_revenue": "date, sales_type, source_id",
    "history.order_versions": "id",
}
STATES = ("clickhouse.silver_orders", "clickhouse.gold_daily_revenue",
          "clickhouse.order_versions")
SHADOW_STATE = "rust_shadow."
NO_GOLD_USER, NO_GOLD_KEY = "ks_k1_nogold", "k1-nogold-only"

Sent = List[Tuple[str, Optional[bytes]]]


def _report(**facts: Any) -> None:
    path = os.getenv("KS_K1_REPORT")
    if path:
        with open(path, "a", encoding="utf-8") as out:
            out.write(json.dumps(facts, ensure_ascii=False, default=str) + "\n")


@pytest_asyncio.fixture
async def stores():
    from core import ch_history, pg

    await pg.close_pool()
    ch_history.last_heal.clear()
    pool = await pg.get_pool()
    yield pool
    await pg.close_pool()


async def _ch(sql: str, *, body: Optional[bytes] = None) -> str:
    from core.ch_common import execute

    return await execute(sql, body=body)


async def _fresh(pool) -> Dict[str, int]:
    """The fixture in Postgres; no copy of either side in ClickHouse."""
    counts = await fixture.seed(pool)
    for live, shadow in SHADOW:
        for name in (live, shadow):
            for table in (name, f"{name}_staging"):
                await _ch(f"DROP TABLE IF EXISTS {table}")
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM meta.mirror_state WHERE table_name LIKE '%clickhouse.%'")
    return counts


async def _python(chunk: Optional[int] = None) -> Tuple[Dict[str, Any], Sent, float]:
    """`_run_ch_sync`, recording what it sends."""
    from core import ch_common, ch_history, ch_silver

    sent: Sent = []
    real = ch_common.execute

    async def recording(sql, *, body=None):
        sent.append((sql, body))
        return await real(sql, body=body)

    started = time.perf_counter()
    with patch.object(ch_silver, "_execute", recording), \
            patch.object(ch_history, "_execute", recording):
        result = await ch_silver.ch_silver_sync()
        result["history"] = await (
            ch_history.ship_history(chunk=chunk) if chunk else ch_history.ship_history())
    return result, sent, time.perf_counter() - started


def _rust(tmp_path, *args: str, env: Optional[Dict[str, str]] = None):
    """The worker's shadow run: its answer, what it sent, what it found."""
    dump = tmp_path / f"rust-{uuid.uuid4().hex[:8]}"
    started = time.perf_counter()
    process = subprocess.run(
        [os.environ["KS_RUST_WORKER_BIN"], "ch-sync", "--target", "shadow",
         "--dump", str(dump), *args],
        capture_output=True, text=True, timeout=600, env={**os.environ, **(env or {})},
    )
    elapsed = time.perf_counter() - started
    assert process.stdout.strip(), process.stderr[-4000:]
    result = json.loads(process.stdout)
    assert process.returncode == (1 if _failed(result) else 0), process.stderr[-4000:]
    sent: Sent = []
    for line in (dump / "statements.jsonl").read_text(encoding="utf-8").splitlines():
        record = json.loads(line)
        body = record["body"]
        sent.append((record["sql"], None if body is None else body.encode("utf-8")))
    findings = [json.loads(line) for line in
                (dump / "findings.jsonl").read_text(encoding="utf-8").splitlines()]
    return result, sent, findings, elapsed


def _failed(result: Dict[str, Any]) -> bool:
    return "error" in result or "error" in result.get("history", {})


def _shadowed(sent: Sent) -> Sent:
    out: Sent = []
    for sql, body in sent:
        for live, shadow in SHADOW:
            sql = sql.replace(live, shadow)
        out.append((sql, body))
    return out


def _assert_same_statements(python: Sent, rust: Sent) -> None:
    for i, (a, b) in enumerate(zip(python, rust)):
        assert a[0] == b[0], f"statement {i} differs:\nPython: {a[0][:600]!r}\nRust:   {b[0][:600]!r}"
        if a[1] != b[1]:
            at = next((j for j, (x, y) in enumerate(zip(a[1] or b"", b[1] or b"")) if x != y),
                      min(len(a[1] or b""), len(b[1] or b"")))
            pytest.fail(
                f"statement {i} ({a[0][:80]!r}) sent other rows: Python {len(a[1] or b'')} "
                f"bytes, Rust {len(b[1] or b'')}; first difference at byte {at}: "
                f"{(a[1] or b'')[max(0, at - 80):at + 80]!r} vs {(b[1] or b'')[max(0, at - 80):at + 80]!r}")
    assert len(python) == len(rust), (
        f"Python sent {len(python)} statement(s), Rust {len(rust)}; "
        f"the first extra: {(python[len(rust):] or rust[len(python):])[0][0][:200]!r}")


async def _assert_same_tables() -> Dict[str, int]:
    sizes = {}
    for live, shadow in SHADOW:
        texts = []
        for name in (live, shadow):
            texts.append(await _ch(
                f"SELECT * FROM {name} ORDER BY {ORDER_BY[live]} FORMAT TabSeparated"))
        assert texts[0] == texts[1], f"{shadow} is not {live} ({len(texts[1])} vs {len(texts[0])} bytes)"
        sizes[live] = texts[0].count("\n")
    return sizes


async def _states(pool) -> Dict[str, Tuple[Any, ...]]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT table_name, failures_since_ok, last_error IS NOT NULL AS failing, "
            "last_rows, last_ok_at IS NOT NULL AS succeeded FROM meta.mirror_state "
            "WHERE table_name LIKE '%clickhouse.%'")
    return {r["table_name"]: tuple(r)[1:] for r in rows}


async def _assert_same_states(pool) -> Dict[str, Tuple[Any, ...]]:
    states = await _states(pool)
    for live in STATES:
        assert states.get(live) == states.get(SHADOW_STATE + live), (
            f"{live}: Python {states.get(live)}, Rust {states.get(SHADOW_STATE + live)} "
            f"(failures_since_ok, failing, last_rows, succeeded)")
    return states


async def _judge_the_shadow() -> Tuple[int, int]:
    """Python's own daily check, pointed at the shadow."""
    from core import ch_gold, ch_silver
    from core.pg_gold import GOLD_COLUMNS
    from core.pg_silver import SILVER_COLUMNS

    readback = ch_silver.parse_tsv(await _ch(
        f"SELECT {', '.join(SILVER_COLUMNS)} FROM silver.orders_rust FORMAT TabSeparated"))
    shipped = await ch_silver._fetch_pg_silver()
    assert sorted(readback, key=lambda r: r[0]) == sorted(shipped, key=lambda r: r[0])
    cells = ch_gold.parse_tsv(await _ch(
        f"SELECT {', '.join(GOLD_COLUMNS)} FROM gold.daily_revenue_rust FORMAT TabSeparated"))
    pg_cells = await ch_silver._fetch_pg_gold_cells()
    assert ch_silver.compare_gold_cells(pg_cells, cells) == []
    return len(readback), len(cells)


@pytest.mark.asyncio
async def test_a_first_run_ships_the_same_bytes(stores, tmp_path):
    counts = await _fresh(stores)
    python, python_sent, python_s = await _python()
    rust, rust_sent, findings, rust_s = _rust(tmp_path)

    assert not _failed(python), python
    assert rust == python
    _assert_same_statements(_shadowed(python_sent), rust_sent)
    sizes = await _assert_same_tables()
    await _assert_same_states(stores)
    silver_rows, gold_cells = await _judge_the_shadow()
    assert findings == []
    assert sizes["history.order_versions"] == counts["versions"]
    _report(test="first_run", **counts, answer=python, statements=len(rust_sent),
            bytes_sent=sum(len(b or b"") for _, b in rust_sent),
            silver_readback=silver_rows, gold_cells=gold_cells,
            python_s=round(python_s, 2), rust_s=round(rust_s, 2))


@pytest.mark.asyncio
async def test_the_archive_appends_and_heals_the_same(stores, tmp_path):
    from core import ch_history

    await _fresh(stores)
    await _python()
    _rust(tmp_path)

    # Late commits into the gaps below the copy's top, new versions above it,
    # versions both copies lost, and a Silver the derivation moved.
    _, versions = fixture.build()
    late = fixture.gaps(versions)[:50]
    top = max(v[0] for v in versions)
    template = versions[0]
    added = [(i,) + template[1:] for i in late + list(range(top + 1, top + 301))]
    async with stores.acquire() as conn:
        await conn.copy_records_to_table(
            "order_versions", schema_name="app", columns=fixture.VERSION_COLUMNS,
            records=added)
        await conn.execute(
            "UPDATE bronze.orders SET grand_total = grand_total + 1 WHERE id % 97 = 0")
    from unittest.mock import AsyncMock

    from core.pg_gold import rebuild_gold
    from core.pg_silver import rebuild_silver
    with patch("core.pg.require_revision", new=AsyncMock()):
        await rebuild_silver(pool=stores)
        await rebuild_gold(pool=stores)
    lost = [v[0] for v in versions[1000:40_000:1000]]
    for table in ("history.order_versions", "history.order_versions_rust"):
        await _ch(f"ALTER TABLE {table} DELETE WHERE id IN ({', '.join(map(str, lost))}) "
                  f"SETTINGS mutations_sync = 2")

    python, python_sent, python_s = await _python(chunk=7_000)
    rust, rust_sent, findings, rust_s = _rust(tmp_path, "--history-chunk", "7000")

    assert not _failed(python), python
    assert rust == python
    assert python["history"] == {"appended": len(added) + len(lost)}
    _assert_same_statements(_shadowed(python_sent), rust_sent)
    await _assert_same_tables()
    await _assert_same_states(stores)
    await _judge_the_shadow()
    healed = len(late) + len(lost)
    assert ch_history.last_heal.get("shipped") == healed
    assert [(f["key"], f["count"]) for f in findings] == [("ch_history_selfhealed", healed)]
    inserts = sum(1 for sql, _ in rust_sent if sql.startswith("INSERT INTO history."))
    assert inserts == 1  # 390 versions: one chunk of 7 000
    _report(test="appends_and_heals", appended=python["history"]["appended"], healed=healed,
            python_s=round(python_s, 2), rust_s=round(rust_s, 2))


@pytest.mark.asyncio
async def test_a_whole_archive_in_chunks_is_the_same(stores, tmp_path):
    await _fresh(stores)
    python, python_sent, _ = await _python(chunk=7_000)
    rust, rust_sent, _, _ = _rust(tmp_path, "--history-chunk", "7000")
    assert rust == python
    _assert_same_statements(_shadowed(python_sent), rust_sent)
    await _assert_same_tables()
    inserts = sum(1 for sql, _ in rust_sent if sql.startswith("INSERT INTO history."))
    assert inserts == -(-python["history"]["appended"] // 7_000) > 1
    _report(test="chunked", history_inserts=inserts)


@pytest.mark.asyncio
async def test_an_unreachable_clickhouse_fails_the_same_rows(stores, tmp_path):
    await _fresh(stores)
    nowhere = {"KS_CH_URL": "http://127.0.0.1:9"}
    with patch.dict(os.environ, nowhere):
        python, python_sent, _ = await _python()
    rust, rust_sent, _, _ = _rust(tmp_path, env=nowhere)

    assert set(python) == set(rust) == {"error", "history"}
    assert "error" in python["history"] and "error" in rust["history"]
    _assert_same_statements(_shadowed(python_sent), rust_sent)
    states = await _assert_same_states(stores)
    assert states["clickhouse.silver_orders"] == (1, True, None, False)
    assert "clickhouse.gold_daily_revenue" not in states
    assert states["clickhouse.order_versions"] == (1, True, None, False)
    _report(test="unreachable", python=python, rust=rust)


@pytest.mark.asyncio
async def test_a_gold_refused_fails_against_gold_not_silver(stores, tmp_path):
    """Production's grant shape: no global CREATE DATABASE, so every
    `CREATE DATABASE IF NOT EXISTS` is denied and tolerated — and here no
    right on `gold` either, so Silver ships and Gold fails."""
    await _fresh(stores)
    await _ch(f"DROP USER IF EXISTS {NO_GOLD_USER}")
    await _ch(f"CREATE USER {NO_GOLD_USER} IDENTIFIED WITH plaintext_password BY '{NO_GOLD_KEY}'")
    for database in ("silver", "history"):
        await _ch(f"GRANT SELECT, INSERT, ALTER, CREATE TABLE, DROP TABLE, TRUNCATE, OPTIMIZE "
                  f"ON {database}.* TO {NO_GOLD_USER}")
    narrowed = {"KS_CH_USER": NO_GOLD_USER, "KS_CH_PASSWORD": NO_GOLD_KEY}
    with patch.dict(os.environ, narrowed):
        python, python_sent, _ = await _python()
    rust, rust_sent, _, _ = _rust(tmp_path, env=narrowed)

    assert "Code: 497" in python["error"] and "Code: 497" in rust["error"], (python, rust)
    assert python["history"] == rust["history"] and "appended" in rust["history"]
    _assert_same_statements(_shadowed(python_sent), rust_sent)
    states = await _assert_same_states(stores)
    assert states["clickhouse.silver_orders"][:2] == (0, False)
    assert states["clickhouse.gold_daily_revenue"] == (1, True, None, False)
    _report(test="gold_refused", python_error=python["error"][:300], rust_error=rust["error"][:300])


@pytest.mark.asyncio
async def test_an_empty_silver_ships_empty_the_same(stores, tmp_path):
    await _fresh(stores)
    async with stores.acquire() as conn:
        await conn.execute("DELETE FROM gold.daily_revenue")
        await conn.execute("DELETE FROM silver.orders")
    python, python_sent, _ = await _python()
    rust, rust_sent, _, _ = _rust(tmp_path)
    assert python["silver_rows"] == python["gold_rows"] == 0
    assert rust == python
    _assert_same_statements(_shadowed(python_sent), rust_sent)
    await _assert_same_tables()
    await _assert_same_states(stores)
