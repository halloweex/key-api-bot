"""The UTM verdicts' doors over the test Postgres: ten website orders with a
paid comment, parsed, and everything a door's parse writes put back.

A full parse reads every commented order in `bronze.orders` and replaces
`silver.order_utm` whole, so a door's test owns both tables while it runs:
they are emptied before and after, as `tests/integration/pg_tab_seed.py`
empties its tables. What the parses and their verdict log (revision 0036)
write beside them — `app.order_channel_verdicts` above the id the test found,
and the two `meta.mirror_state` rows — is put back as it was.

The verdict log is the real one, not stood in for: a full parse whose log did
not first keep what it would replace is refused (owner decision D12), and a
door's test runs the door as production runs it.

`core.pg.get_pool` answers with a connection per acquire, so the app under
`TestClient` and a script's own event loop can both reach the database.
"""
from __future__ import annotations

import asyncio
import runpy
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Tuple
from unittest.mock import AsyncMock, patch

from core.analytics_captures import SWITCH_ENV, state_row
from core.pg_utm_parse import UTM_COLUMNS, UTM_TABLE, VERDICTS_TABLE
from tests.helpers.app_client import ConnectionPerAcquire

UPDATED = datetime(2026, 9, 7, 9, 0, tzinfo=timezone.utc)
PAID = "UTM: utm_source: fbads; utm_medium: cpc; utm_campaign: spring"
PAID_VERDICT = ("paid_confirmed", "facebook")
# What an earlier rule might have stored for the same comment.
STALE_VERDICT = ("unknown", "other")
IDS = tuple(range(973_001, 973_011))
NEW = 973_011
STAMPS = (UTM_TABLE, state_row(VERDICTS_TABLE))
BACKFILL_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "backfill_utm.py"


async def commented(conn, oid, comment=PAID, updated_at=UPDATED):
    """A website order in Bronze, as chain 3 lands it."""
    await conn.execute(
        "INSERT INTO bronze.orders"
        " (id, source_id, status_id, grand_total, updated_at, manager_comment)"
        " VALUES ($1, 4, 1, 100, $2, $3)"
        " ON CONFLICT (id) DO UPDATE SET updated_at = EXCLUDED.updated_at,"
        " manager_comment = EXCLUDED.manager_comment",
        oid, updated_at, comment)


async def verdicts(conn) -> Dict[int, Tuple[str, str]]:
    """`(traffic_type, platform)` per stored order."""
    return {r["order_id"]: (r["traffic_type"], r["platform"]) for r in await conn.fetch(
        f"SELECT order_id, traffic_type, platform FROM {UTM_TABLE}")}


async def stored_rows(conn) -> Dict[int, tuple]:
    """Every stored row, every column the parse writes."""
    return {r["order_id"]: tuple(r) for r in await conn.fetch(
        f"SELECT {', '.join(UTM_COLUMNS)} FROM {UTM_TABLE}")}


async def utm_failures(conn) -> int:
    """`silver.order_utm`'s watermark: failures since its last success."""
    return await conn.fetchval(
        "SELECT failures_since_ok FROM meta.mirror_state WHERE table_name = $1",
        UTM_TABLE)


async def age_the_rules(conn) -> None:
    """The state a rule change leaves: the first order's verdict as an earlier
    rule stored it, its order unchanged since (so no incremental parse looks
    at it again), and the last order's comment emptied in KeyCRM since it was
    parsed."""
    await conn.execute(
        f"UPDATE {UTM_TABLE} SET traffic_type = $1, platform = $2 WHERE order_id = $3",
        *STALE_VERDICT, IDS[0])
    await conn.execute(
        "UPDATE bronze.orders SET manager_comment = NULL, updated_at = $1 WHERE id = $2",
        UPDATED + timedelta(minutes=5), IDS[-1])


async def lose_comments(conn) -> None:
    """Every comment but the first order's gone from Bronze — the state the
    July 2026 template break left, which the backfill CLI exists to repair:
    a full parse of it yields one verdict of the ten held."""
    await conn.execute(
        "UPDATE bronze.orders SET manager_comment = NULL WHERE id <> $1", IDS[0])


async def _empty(conn) -> None:
    await conn.execute(f"DELETE FROM {UTM_TABLE}")
    await conn.execute("DELETE FROM bronze.orders")


@asynccontextmanager
async def ten_parsed_orders(dsn: str, monkeypatch):
    """Ten website orders (`IDS`) with a paid comment, parsed by the
    incremental parse; a `PG_LAYER_LOCK` of the test's own; the verdict log
    switched on. Yields a connection to read the tables with."""
    import asyncpg

    from core.pg_utm_parse import parse_incremental

    monkeypatch.setenv("KS_PG_DSN", dsn)
    monkeypatch.delenv(SWITCH_ENV, raising=False)
    conn = await asyncpg.connect(dsn)
    try:
        top = await conn.fetchval(f"SELECT COALESCE(max(id), 0) FROM {VERDICTS_TABLE}")
        kept = await conn.fetch(
            "SELECT * FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
            list(STAMPS))
        try:
            await _empty(conn)
            for oid in IDS:
                await commented(conn, oid)
            with patch("core.pg.get_pool",
                       new=AsyncMock(return_value=ConnectionPerAcquire(dsn))), \
                 patch("core.pg_silver.PG_LAYER_LOCK", asyncio.Lock()):
                await parse_incremental()
                yield conn
        finally:
            await _empty(conn)
            await conn.execute(f"DELETE FROM {VERDICTS_TABLE} WHERE id > $1", top)
            await conn.execute(
                "DELETE FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
                list(STAMPS))
            for row in kept:
                columns = list(row.keys())
                await conn.execute(
                    f"INSERT INTO meta.mirror_state ({', '.join(columns)}) VALUES "
                    f"({', '.join(f'${i}' for i in range(1, len(columns) + 1))})",
                    *row.values())
    finally:
        await conn.close()


async def run_backfill_cli(monkeypatch, *flags: str) -> None:
    """`python scripts/backfill_utm.py --days 30 [flags]`, as an operator runs
    it: its own argument parser and its own event loop (in a thread), the walk
    over KeyCRM served no orders at the transport, then its re-parse."""
    import httpx

    from core import keycrm

    def keycrm_answers(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"data": []})

    client = keycrm.KeyCRMClient(api_key="test-key", base_url="https://keycrm.test/v1")
    client._client = httpx.AsyncClient(
        transport=httpx.MockTransport(keycrm_answers), headers=client.headers)
    monkeypatch.setattr(keycrm, "_client_instance", client)
    monkeypatch.setattr(sys, "argv", [str(BACKFILL_SCRIPT), "--days", "30", *flags])
    monkeypatch.setattr(sys, "path", list(sys.path))   # the script inserts the repo root
    try:
        await asyncio.to_thread(runpy.run_path, str(BACKFILL_SCRIPT), run_name="__main__")
    finally:
        await client._client.aclose()
