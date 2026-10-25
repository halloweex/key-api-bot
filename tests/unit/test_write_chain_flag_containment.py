"""A KS_WRITE_* value no chain understands stops that chain and nothing else.

DN-01 of the stage-4 prep plan (2026-09-17). The registry evaluated every
chain's flag for every question, so a typo in KS_WRITE_EXPENSES raised inside
`get_last_sync_time("orders")` — the first call of the incremental sync — and
order intake stopped silently for the eight hours the canary allows. It also
broke the hourly copy of every stage-3 table and the freshness check.

Since step 14's S14-00 a flag routes nothing — every chain writes Postgres,
a constant — so what a typo stopped is gone with the tests that held it (the
orders watermark beside an unrelated typo, chain 4's typo). What stays is
that the registry never raises over one and publishes it, so a rollback to
an image from before step 14, which still routes by the flags, is not taken
blind.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import httpx
import pytest
import pytest_asyncio

from core import write_chains
from core.duckdb_store import DuckDBStore


@pytest.fixture
def flags(monkeypatch):
    for env in ("KS_WRITE_INVENTORY", "KS_WRITE_EXPENSES", "KS_WRITE_GOALS",
                "KS_WRITE_CATALOGUE"):
        monkeypatch.delenv(env, raising=False)
    return monkeypatch


@pytest_asyncio.fixture
async def store(tmp_path):
    s = DuckDBStore(db_path=tmp_path / "chains.duckdb")
    await s.connect()
    yield s
    await s.close()


class TestTheRegistryNeverRaises:
    def test_an_expenses_typo_stands_its_table_down_and_names_itself(self, flags):
        flags.setenv("KS_WRITE_EXPENSES", "postgrse")
        tables, errors = write_chains.stood_down_tables_checked()
        assert "app.manual_expenses" in tables
        assert list(errors) == ["pg_expenses_write"] and "postgrse" in errors["pg_expenses_write"]
        assert write_chains.stood_down_tables() == tables      # the plain form does not raise

    def test_an_inventory_typo_stands_its_keys_down_and_names_itself(self, flags):
        flags.setenv("KS_WRITE_INVENTORY", "on")
        keys, errors = write_chains.stood_down_sync_keys_checked()
        assert {"last_sync_offers", "last_sync_stocks"} <= keys
        assert list(errors) == ["pg_inventory_write"]

    def test_modes_are_published_with_the_error(self, flags):
        flags.setenv("KS_WRITE_EXPENSES", "postgres")
        flags.setenv("KS_WRITE_INVENTORY", "yes")
        modes = write_chains.chain_modes()
        # The latch fields are False/None here: nothing has written Postgres,
        # which is production's state today and the only one in which a flag
        # still decides anything (DN-06).
        assert modes["pg_expenses_write"] == {
            "env": "KS_WRITE_EXPENSES", "mode": "postgres", "error": None,
            "latched": False, "latched_at": None, "mismatch": False,
            "unmet_precondition": None,
            # The chain's shape, not its state: chain 8 freezes DuckDB
            # (OD-02 (c) shadows only chains 9, 10 and 11).
            "shadow": False}
        # Since S14-00 the mode is Postgres whatever the flag says; the typo is
        # still published, for a rollback to read.
        assert modes["pg_inventory_write"]["mode"] == "postgres"
        assert "yes" in modes["pg_inventory_write"]["error"]


class TestTheFreshnessCheckSaysItCannotJudge:
    @pytest.mark.asyncio
    async def test_an_inventory_typo_leaves_offers_and_stocks_unwatched(self, flags, store):
        from core.data_quality import _freshness_check

        now = datetime.now(timezone.utc)
        async with store.connection() as conn:
            for entity in ("orders", "offers", "stocks"):
                conn.execute(
                    "INSERT OR REPLACE INTO sync_metadata (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
                    [f"last_sync_{entity}", (now - timedelta(hours=60)).isoformat()])
            flags.setenv("KS_WRITE_INVENTORY", "postgrse")
            issues = _freshness_check(conn, now, chain_watermarks={})
        names = {i.check_name for i in issues}
        assert "freshness_offers" not in names and "freshness_stocks" not in names
        (unwatched,) = [i for i in issues if i.check_name == "sync_watermarks_unwatched"]
        assert "KS_WRITE_INVENTORY" in unwatched.description
        assert "freshness_orders" in names                       # the rest is still judged


class TestItIsSeen:
    def test_health_publishes_the_chains(self, flags):
        from web.routes.api.health import _write_chains

        flags.setenv("KS_WRITE_EXPENSES", "postgrse")
        block = _write_chains()
        assert block["pg_expenses_write"]["error"]
        assert block["pg_expenses_write"]["mode"] == "postgres"     # S14-00

    def test_the_canary_pages_on_a_chain_flag_error(self):
        from bot import canary

        payload = {"write_chains": {"pg_expenses_write": {"env": "KS_WRITE_EXPENSES", "mode": None,
                                                          "error": "KS_WRITE_EXPENSES='postgrse' is not understood"}}}
        assert [k for k, _ in canary.check_write_chains(payload)] == ["write_chain_flag_invalid"]
        assert canary.check_write_chains({"write_chains": {"x": {"mode": "postgres", "error": None}}}) == []
        assert canary.check_write_chains({}) == []

    @pytest.mark.asyncio
    async def test_run_canary_escalates_to_critical(self):
        from tests.unit.test_canary import DASHBOARD, _healthy_payload, _mock_transport
        from bot import canary

        payload = _healthy_payload()
        payload["write_chains"] = {"pg_expenses_write": {"env": "KS_WRITE_EXPENSES", "mode": None,
                                                         "error": "not understood"}}

        def handler(request):
            return httpx.Response(200, json=payload)

        future = datetime.now(timezone.utc) + timedelta(days=60)
        cert = {"notAfter": future.strftime("%b %d %H:%M:%S %Y GMT")}
        async with _mock_transport(handler) as client:
            with patch.object(canary, "_fetch_peer_cert", return_value=cert):
                result = await canary.run_canary(DASHBOARD, client=client)
        assert result.severity == "critical" and "write_chain_flag_invalid" in result.failure_keys


class TestTheDailyComparisonFilesIt:
    def test_reconcile_operational_files_a_critical_for_each_chain_error(self):
        """The finding is built by a pure helper the comparison calls, so this
        runs it. It used to be appended at the end of `reconcile_operational`,
        after the early return taken whenever no fingerprint disagrees — which
        is every healthy run, so it was unreachable in practice (found by
        DN-06, which had to file two more findings in the same place)."""
        import ast
        import inspect
        import textwrap

        from core.mirror_reconciliation import _chain_flag_findings, reconcile_operational
        from core.data_quality import Severity

        assert _chain_flag_findings({}) == []
        (issue,) = _chain_flag_findings({"pg_expenses_write": "KS_WRITE_EXPENSES='postgrse'"})
        assert issue.check_name == "write_chain_flag_invalid"
        assert issue.severity is Severity.CRITICAL and "postgrse" in issue.description

        tree = ast.parse(textwrap.dedent(inspect.getsource(reconcile_operational)))
        called = {n.func.id for n in ast.walk(tree)
                  if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
        assert "stood_down_tables_checked" in called
        assert "_chain_flag_findings" in called

    def test_the_chain_findings_are_built_before_the_early_return(self):
        """The drill-down returns as soon as no bucket disagrees, so anything
        filed after it is filed only on a run that already found something
        else."""
        import ast
        import inspect
        import textwrap

        from core.mirror_reconciliation import reconcile_operational

        tree = ast.parse(textwrap.dedent(inspect.getsource(reconcile_operational)))
        built = [n.lineno for n in ast.walk(tree)
                 if isinstance(n, ast.Call) and getattr(n.func, "id", "")
                 in ("_chain_flag_findings", "_chain_latch_findings")]
        # Every `return issues` — the early one and the last one. Anything
        # built after the first of them is filed only sometimes.
        returns = [n.lineno for n in ast.walk(tree)
                   if isinstance(n, ast.Return) and isinstance(n.value, ast.Name)
                   and n.value.id == "issues"]
        assert len(built) == 2, "both chain-level findings must be built here"
        assert len(returns) >= 2, "the early return moved — this test no longer guards it"
        assert max(built) < min(returns), "a chain finding is built after a return"


