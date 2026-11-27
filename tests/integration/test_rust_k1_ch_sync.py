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

# Two markers, not one: CI has a Postgres and no ClickHouse, so the store's
# skip must say "needs a live PostgreSQL" (the CI step that refuses store
# skips reads that phrase) and the other skip must not name Postgres at all.
pytestmark = [
    pytest.mark.skipif(not os.getenv("KS_PG_DSN"), reason="needs a live PostgreSQL (KS_PG_DSN)"),
    pytest.mark.skipif(
        not all(os.getenv(v) for v in ("KS_CH_URL", "KS_RUST_WORKER_BIN")),
        reason="needs a throwaway ClickHouse and the Rust worker (KS_RUST_WORKER_BIN): "
               "rust/harness/k1-ch-sync.sh runs it",
    ),
]

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


def _rust(tmp_path, *args: str, env: Optional[Dict[str, str]] = None,
          target: str = "shadow"):
    """The worker's run, the shadow unless told: its answer, what it sent,
    what it found."""
    dump = tmp_path / f"rust-{uuid.uuid4().hex[:8]}"
    started = time.perf_counter()
    process = subprocess.run(
        [os.environ["KS_RUST_WORKER_BIN"], "ch-sync", "--target", target,
         "--dump", str(dump), *args],
        capture_output=True, text=True, timeout=600,
        env={**os.environ, **_worker_logins(target), **(env or {})},
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


def _worker_logins(target: str) -> Dict[str, str]:
    """The worker's own logins, where the harness made them
    (deploy/rust/ks_rust_worker*.sql): Postgres for every run, ClickHouse for
    the shadow's — the live tables are not that user's to write."""
    logins = {}
    if os.getenv("KS_K1_WORKER_PG_DSN"):
        logins["KS_PG_DSN"] = os.environ["KS_K1_WORKER_PG_DSN"]
    if target == "shadow" and os.getenv("KS_K1_WORKER_CH_USER"):
        logins["KS_CH_USER"] = os.environ["KS_K1_WORKER_CH_USER"]
        logins["KS_CH_PASSWORD"] = os.environ.get("KS_K1_WORKER_CH_PASSWORD", "")
    return logins


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
    # The worker's peak memory, for docker-compose.yml's mem_limit: the only
    # children this process has started are the worker's runs.
    import resource
    _report(test="first_run", **counts, answer=python, statements=len(rust_sent),
            bytes_sent=sum(len(b or b"") for _, b in rust_sent),
            silver_readback=silver_rows, gold_cells=gold_cells,
            python_s=round(python_s, 2), rust_s=round(rust_s, 2),
            rust_worker_max_rss_kb=resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss)


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
    # The heal's durable trace (core.ch_history.HEAL_ROW), stamped by both.
    states = await _states(stores)
    heal = states.get(ch_history.HEAL_ROW)
    assert heal == (0, False, healed, True), heal
    assert states.get(SHADOW_STATE + ch_history.HEAL_ROW) == heal
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


# К0's contract (`core/job_lease.py`): the key is computed by Postgres here,
# from the formula's own text, and handed to a session that holds it, so the
# worker refusing to run proves it asked for the same number.
LEASE_KEY_SQL = (
    "SELECT ('x' || '6b734a4c' || "
    "left(encode(sha256(convert_to($1, 'UTF8')), 'hex'), 8))::bit(64)::bigint"
)
SWITCH_KEY = "executor:ch_sync"


async def _switch(pool, value: Optional[str]) -> None:
    async with pool.acquire() as conn:
        if value is None:
            await conn.execute("DELETE FROM meta.chain_watermarks WHERE key = $1", SWITCH_KEY)
        else:
            await conn.execute(
                "INSERT INTO meta.chain_watermarks (key, value) VALUES ($1, $2) "
                "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()",
                SWITCH_KEY, value)


async def _live_bytes() -> Dict[str, str]:
    return {live: await _ch(
        f"SELECT * FROM {live} ORDER BY {ORDER_BY[live]} FORMAT TabSeparated")
        for live, _ in SHADOW}


@pytest.mark.asyncio
async def test_the_switch_and_the_lease_decide_who_writes_the_live_tables(stores, tmp_path):
    """The live tables are the worker's only when `executor:ch_sync` is
    exactly `rust`, and only under ch_sync's lease; the shadow's lease is its
    own. Run live, Rust leaves the tables as Python's run left them."""
    import asyncpg

    await _fresh(stores)
    try:
        python, _, _ = await _python()
        assert not _failed(python), python
        live = await _live_bytes()
        before = await _states(stores)

        for value in (None, "Rust", "RUST", "rust ", " rust", "python", ""):
            await _switch(stores, value)
            result, sent, _, _ = _rust(tmp_path, target="live")
            assert result == {"skipped": True, "reason": "executed_by_python",
                              "executor": "python"}, (value, result)
            assert sent == [], value
        assert await _states(stores) == before

        await _switch(stores, "rust")
        async with stores.acquire() as conn:
            key = await conn.fetchval(LEASE_KEY_SQL, "ch_sync")
            shadow_key = await conn.fetchval(LEASE_KEY_SQL, "ch_sync:shadow")
        assert (key, shadow_key) == (7742613876423907721, 7742613876521340087)

        holder = await asyncpg.connect(os.environ["KS_PG_DSN"])
        try:
            assert await holder.fetchval("SELECT pg_try_advisory_lock($1)", key)
            result, sent, _, _ = _rust(tmp_path, target="live")
            assert result == {"skipped": True, "reason": "lease_held",
                              "lease": "ch_sync", "lease_key": str(key)}, result
            assert sent == []
            # The shadow takes its own lease, so it runs while ch_sync's is held.
            shadow, shadow_sent, _, _ = _rust(tmp_path)
            assert "skipped" not in shadow and not _failed(shadow), shadow
            assert shadow_sent
            assert await holder.fetchval("SELECT pg_advisory_unlock($1)", key)
            assert await holder.fetchval("SELECT pg_try_advisory_lock($1)", shadow_key)
            result, sent, _, _ = _rust(tmp_path)
            assert result == {"skipped": True, "reason": "lease_held",
                              "lease": "ch_sync:shadow", "lease_key": str(shadow_key)}, result
            assert sent == []
            assert await holder.fetchval("SELECT pg_advisory_unlock($1)", shadow_key)
        finally:
            await holder.close()

        rust, rust_sent, _, _ = _rust(tmp_path, target="live")
        assert rust == {"silver_rows": python["silver_rows"], "gold_rows": python["gold_rows"],
                        "history": {"appended": 0}}, rust
        assert rust_sent and not any("_rust" in sql for sql, _ in rust_sent)
        assert await _live_bytes() == live
        after = await _states(stores)
        for row, rows in (("clickhouse.silver_orders", python["silver_rows"]),
                          ("clickhouse.gold_daily_revenue", python["gold_rows"]),
                          ("clickhouse.order_versions", 0)):
            assert after[row] == (0, False, rows, True), (row, after[row])
        _report(test="switch_and_lease", lease_key=key, shadow_key=shadow_key,
                switch_values_refused=7)
    finally:
        await _switch(stores, None)


# ─── The nightly judge on real stores (core/ch_rust_shadow.py) ───────────────
#
# The same judge the scheduler runs at night, against what the two sides
# actually shipped: a clean shadow compares clean in every part, a wrong row,
# cell or version in the shadow alone is named, and a Silver rebuilt after the
# ships is a wait, not a difference.

async def _rebuilt_before_the_ships(pool) -> None:
    """Move Postgres' two rebuild stamps an hour back — a night on which
    Silver was last rebuilt before either side shipped — and forget every
    night the judge has journalled."""
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE meta.mirror_state SET last_ok_at = last_ok_at - interval '1 hour' "
            "WHERE table_name IN ('silver.orders', 'gold.daily_revenue')")
        await conn.execute(
            "DELETE FROM app.data_quality_issues WHERE run_id IN "
            "(SELECT run_id FROM app.data_quality_runs WHERE layer = 'rust_shadow')")
        await conn.execute("DELETE FROM app.data_quality_runs WHERE layer = 'rust_shadow'")


def _findings(verdict) -> Dict[Tuple[str, str], Any]:
    return {(i.check_name, i.table_name): i for i in verdict.issues}


EVERY_PART = {"silver", "gold", "live silver", "live gold", "archive", "live archive"}


@pytest.mark.asyncio
async def test_the_night_finds_a_clean_shadow_clean(stores, tmp_path, monkeypatch):
    from core import ch_rust_shadow as shadow

    await _fresh(stores)
    await _python()
    _rust(tmp_path)
    await _rebuilt_before_the_ships(stores)

    verdict = await shadow.judge()
    assert verdict.issues == [], [(i.check_name, i.description) for i in verdict.issues]
    assert verdict.unwatched == []
    assert set(verdict.compared) == EVERY_PART

    monkeypatch.setenv(shadow.SHADOW_ENV, "ch_sync")
    first = await shadow.run_nightly(final=False)
    assert first["findings"] == [] and first["run_id"], first
    assert await shadow.run_nightly(final=False) == {
        "skipped": "tonight's comparison is journalled"}
    async with stores.acquire() as conn:
        runs = await conn.fetch(
            "SELECT status, error_message, integrity_issues_count FROM app.data_quality_runs "
            "WHERE layer = 'rust_shadow'")
    assert [tuple(r) for r in runs] == [("PASS", None, 0)]
    _report(test="shadow_judged_clean", compared=sorted(verdict.compared))


@pytest.mark.asyncio
async def test_the_night_names_what_the_shadow_alone_got_wrong(stores, tmp_path):
    from core import ch_rust_shadow as shadow

    await _fresh(stores)
    await _python()
    _rust(tmp_path)
    await _rebuilt_before_the_ships(stores)
    async with stores.acquire() as conn:
        order = await conn.fetchval("SELECT min(id) FROM silver.orders")
        day = await conn.fetchval("SELECT min(date) FROM gold.daily_revenue")
        version = await conn.fetchval("SELECT min(id) FROM app.order_versions")

    # A Silver row: named, and Gold and the live copy wait for an equal Silver.
    await _ch(f"ALTER TABLE silver.orders_rust UPDATE grand_total = grand_total + "
              f"toDecimal64(1, 2) WHERE id = {order} SETTINGS mutations_sync = 2")
    verdict = await shadow.judge()
    found = _findings(verdict)
    assert set(found) == {("rust_shadow_silver_differs", "silver.orders_rust")}, (
        found, verdict.compared, verdict.unwatched)
    assert found["rust_shadow_silver_differs", "silver.orders_rust"].sample_ids == (order,)
    assert "live silver" not in verdict.compared and "gold" not in verdict.compared

    # A Gold cell, after a fresh ship: named against Postgres and against the live copy.
    _rust(tmp_path)
    await _ch(f"ALTER TABLE gold.daily_revenue_rust UPDATE revenue = revenue + "
              f"toDecimal64(1, 2) WHERE date = '{day}' AND source_id IS NULL "
              f"SETTINGS mutations_sync = 2")
    verdict = await shadow.judge()
    found = _findings(verdict)
    assert set(found) == {("rust_shadow_gold_differs", "gold.daily_revenue_rust"),
                          ("rust_shadow_gold_differs", "gold.daily_revenue")}, (
        found, verdict.compared, verdict.unwatched)
    sample = day.year * 10000 + day.month * 100 + day.day
    assert all(i.sample_ids == (sample,) for i in found.values())

    # A version lost from the shadow archive below the cut, after a fresh ship.
    _rust(tmp_path)
    await _ch(f"ALTER TABLE history.order_versions_rust DELETE WHERE id = {version} "
              f"SETTINGS mutations_sync = 2")
    verdict = await shadow.judge()
    found = _findings(verdict)
    assert set(found) == {("rust_shadow_history_differs", "history.order_versions_rust"),
                          ("rust_shadow_history_differs", "history.order_versions")}, (
        found, verdict.compared, verdict.unwatched)
    assert found["rust_shadow_history_differs", "history.order_versions"].sample_ids == (version,)
    assert verdict.differs and set(verdict.compared) == EVERY_PART
    _report(test="shadow_judged_wrong", order=order, day=str(day), version=version)


@pytest.mark.asyncio
async def test_the_night_waits_for_a_silver_the_shadow_has_not_shipped(stores, tmp_path,
                                                                       monkeypatch):
    from unittest.mock import AsyncMock

    from core import ch_rust_shadow as shadow
    from core.pg_gold import rebuild_gold
    from core.pg_silver import rebuild_silver

    await _fresh(stores)
    await _python()
    _rust(tmp_path)
    await _rebuilt_before_the_ships(stores)
    async with stores.acquire() as conn:
        await conn.execute(
            "UPDATE bronze.orders SET grand_total = grand_total + 1 WHERE id % 97 = 0")
    with patch("core.pg.require_revision", new=AsyncMock()):
        await rebuild_silver(pool=stores)
        await rebuild_gold(pool=stores)

    verdict = await shadow.judge()
    assert not verdict.differs, [(i.check_name, i.description) for i in verdict.issues]
    assert any(reason.startswith("Silver:") for reason in verdict.unwatched)
    assert set(verdict.compared) == {"archive", "live archive"}

    monkeypatch.setenv(shadow.SHADOW_ENV, "ch_sync")
    assert "pending" in await shadow.run_nightly(final=False)
    last = await shadow.run_nightly(final=True)
    assert last["findings"] == ["rust_shadow_unwatched"], last
    _report(test="shadow_waits", unwatched=verdict.unwatched)


@pytest.mark.asyncio
async def test_the_workers_clickhouse_login_cannot_write_the_live_tables(stores, tmp_path):
    """deploy/rust/ks_rust_worker_clickhouse.sql grants the shadow alone. Run
    live as that login — the switch saying rust — and every write is refused,
    counted against the live rows, and the live tables keep what Python
    shipped: what the morning check would see, with nothing written wrong."""
    if not os.getenv("KS_K1_WORKER_CH_USER"):
        pytest.skip("the worker's ClickHouse login is made by rust/harness/k1-ch-sync.sh")
    await _fresh(stores)
    python, _, _ = await _python()
    assert not _failed(python), python
    live = await _live_bytes()
    await _switch(stores, "rust")
    try:
        worker = {"KS_CH_USER": os.environ["KS_K1_WORKER_CH_USER"],
                  "KS_CH_PASSWORD": os.environ.get("KS_K1_WORKER_CH_PASSWORD", "")}
        result, _, _, _ = _rust(tmp_path, target="live", env=worker)
    finally:
        await _switch(stores, None)
    assert "Code: 497" in result.get("error", ""), result
    assert "Code: 497" in result["history"].get("error", ""), result
    assert await _live_bytes() == live
