"""A whole Postgres derivation tick loads no DuckDB module (step 14, S14-01).

Until S14-01 the tick's first rendering, `pg_silver.pass1_sql`, imported
`core.duckdb_store` lazily for `silver_select_sql`, and with it the `duckdb`
driver and pandas — measured in a fresh interpreter on S14-00's tree, the
stack was `_run_pg_derivation → _derive_and_journal_pg_layers → rebuild_silver
→ pass1_sql`. The unit test beside the move (`tests/unit/test_warehouse_sql.py`)
renders every statement; this runs the tick itself — the signal read, Silver,
Gold, the customer profile, the validation, the journal row and the UTM parse
— against a real Postgres, in a fresh interpreter, and looks at what it
loaded.

It writes what a tick writes, so it resets what
`tests/integration/test_pg_own_derivation.py` resets, before and after.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

ROOT = Path(__file__).resolve().parents[2]
DERIVED = ("silver.orders", "gold.daily_revenue", "app.customer_profile")

PROGRAM = r"""
import asyncio, json, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
tmp = Path(sys.argv[2])

from core import chain_latch, report_ledger, warehouse_cutover
# Never the checkout's own data directory: what the tick reads or writes
# there goes to this test's tmp_path instead.
chain_latch.MARKER_DIR = tmp / "write-chain-owners"
report_ledger.SPOOL_DIR = tmp / "report-ledger-pending"
warehouse_cutover.UTM_MARKER = tmp / "warehouse-utm-reparsed-at"

from core.runtime_modes import configure_modes
from core.scheduler import BackgroundScheduler


async def main():
    configure_modes(web=True)
    try:
        return await BackgroundScheduler()._run_pg_derivation(trigger="probe", force=True)
    finally:
        from core.pg import close_pool
        await close_pool()

result = asyncio.run(main())
watched = ("duckdb", "core.duckdb_store", "pandas")
print(json.dumps({"status": result.get("status"), "silver_rows": result.get("silver_rows"),
                  "loaded": sorted(m for m in watched if m in sys.modules)}))
"""


def _reset():
    import asyncio

    async def go():
        conn = await asyncpg.connect(DSN)
        try:
            for table in DERIVED:
                await conn.execute(f"TRUNCATE {table}")
            await conn.execute(
                "UPDATE meta.derivation_signal SET requested = 1, built = 0, built_at = NULL"
                " WHERE layer = 'warehouse'")
            await conn.execute("DELETE FROM meta.derivation_runs")
            await conn.execute(
                "DELETE FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
                ["silver.orders", "gold.daily_revenue", "app.customer_profile",
                 "meta.derivation_signal", "meta.derivation_runs", "silver.order_utm"])
        finally:
            await conn.close()

    asyncio.run(go())


@pytest.fixture
def clean():
    _reset()
    yield
    _reset()


def test_a_derivation_tick_loads_no_duckdb_module(clean, tmp_path):
    """Mutation: put `from core.duckdb_store import silver_select_sql` back in
    `pg_silver.pass1_sql` — `loaded` is the driver, the store and pandas."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("KS_")}
    env.update({"KS_PG_DSN": DSN or "", "KS_ALERTS_DISABLED": "1",
                "KS_ALERT_GATE_STATE_DIR": str(tmp_path / "gate"),
                "KS_INSTANCE": "s14-01-test", "TZ": "UTC"})
    done = subprocess.run(
        [sys.executable, "-I", "-c", PROGRAM, str(ROOT), str(tmp_path)],
        capture_output=True, text=True, timeout=180, cwd=str(tmp_path), env=env)
    assert done.returncode == 0, done.stderr[-3000:]
    seen = json.loads(done.stdout.strip().splitlines()[-1])
    assert seen["status"] == "success", (seen, done.stderr[-3000:])
    assert seen["loaded"] == [], seen
