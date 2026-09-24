"""Chain 1's pre-flip hardening (DN-24), the parts that need no database.

KS_WRITE_INVENTORY stays off; every guard here is about what happens the day
it is switched on. The same properties against a real Postgres are in
`tests/integration/test_inventory_preflip.py`.
"""
from __future__ import annotations

import ast
import asyncio
import inspect
import pathlib
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import pytest

from core import pg_inventory_write

# The tables whose rebuild or daily photograph two callers can race on. The
# stock upsert and the offer upsert are not here: both run only in the stock
# step, under the scheduler's heavy lock, and nothing else calls them.
_REBUILT_OR_PHOTOGRAPHED = (
    "app.sku_inventory_status",
    "app.inventory_sku_history",
    "app.inventory_history",
)


def _module_tree() -> ast.Module:
    return ast.parse(pathlib.Path(inspect.getfile(pg_inventory_write)).read_text(
        encoding="utf-8"))


def _writes_one_of_them(node: ast.AsyncFunctionDef) -> bool:
    """Whether the function writes a rebuilt or photographed table: a literal
    INSERT or DELETE naming it, or the shared rebuild body, which is composed
    elsewhere and never appears here as text."""
    for n in ast.walk(node):
        if isinstance(n, ast.Constant) and isinstance(n.value, str):
            text = n.value.strip().upper()
            if text.startswith(("INSERT", "DELETE")) and any(
                    t.upper() in text for t in _REBUILT_OR_PHOTOGRAPHED):
                return True
        if isinstance(n, ast.Call) and getattr(n.func, "id", "") == "sku_status_rebuild_select":
            return True
    return False


def _transactions(node: ast.AsyncFunctionDef):
    """Every `async with <conn>.transaction():` body in the function."""
    for n in ast.walk(node):
        if isinstance(n, ast.AsyncWith) and any(
                isinstance(item.context_expr, ast.Call)
                and getattr(item.context_expr.func, "attr", "") == "transaction"
                for item in n.items):
            yield n


def _first_await(body) -> str:
    first = body[0]
    if isinstance(first, ast.Expr) and isinstance(first.value, ast.Await):
        call = first.value.value
        if isinstance(call, ast.Call):
            return getattr(call.func, "id", getattr(call.func, "attr", ""))
    return ""


def _rebuilders() -> dict:
    return {node.name: node for node in _module_tree().body
            if isinstance(node, ast.AsyncFunctionDef) and _writes_one_of_them(node)}


class TestEveryRebuildAndSnapshotTakesTheChainLock:
    def test_the_walk_finds_the_three_that_exist(self):
        # Not vacuous: a walk that found nothing would pass the test below.
        assert set(_rebuilders()) == {
            "rebuild_sku_inventory_status",
            "record_sku_inventory_snapshot",
            "record_inventory_snapshot",
        }

    def test_the_lock_is_the_first_statement_of_each_transaction(self):
        """First, before the owner rows and before any read: the second of two
        callers must read what the first committed. Taken after the
        `first_seen_at` carry-forward, the rebuild would still copy a state
        the other one was about to replace."""
        for name, node in _rebuilders().items():
            blocks = list(_transactions(node))
            assert blocks, f"{name} writes outside a transaction"
            for block in blocks:
                assert _first_await(block.body) == "_chain_lock", (
                    f"{name} does not take the chain-1 advisory lock first")

    def test_it_is_a_transaction_lock_on_the_chains_own_key(self):
        """`_xact_`, so COMMIT or ROLLBACK releases it and a writer that dies
        cannot leave it held; and a bigint Postgres accepts."""
        source = inspect.getsource(pg_inventory_write._chain_lock)
        assert "pg_advisory_xact_lock(" in source and "CHAIN_LOCK_KEY" in source
        assert 0 < pg_inventory_write.CHAIN_LOCK_KEY < 2 ** 63
        assert pg_inventory_write.CHAIN_LOCK_KEY & 0xFFFF_FFFF == 1   # chain 1


# ─── preflight() ─────────────────────────────────────────────────────────────

NOW = datetime(2026, 9, 24, 10, 0, tzinfo=timezone.utc)


class _Conn:
    """Answers the preflight's four reads from a dict of facts."""

    def __init__(self, facts):
        self.facts = facts

    async def fetchrow(self, sql, *args):
        if "has_database_privilege" in sql:
            return ("ks_app", "ks", self.facts["temporary"])
        if "FROM app.data_quality_runs" in sql:
            assert args == ("mirror_landing",)
            return self.facts["run"]
        raise AssertionError(f"unexpected fetchrow: {sql}")

    async def fetch(self, sql, *args):
        if "FROM meta.mirror_state" in sql:
            wanted = set(args[0])
            return [{"table_name": t, **m} for t, m in self.facts["marks"].items()
                    if t in wanted]
        if "FROM app.data_quality_issues" in sql:
            run_id, tables = args
            assert set(tables) == {*pg_inventory_write.CHAIN_TABLES, "pg_inventory_write"}
            return self.facts["findings"]
        raise AssertionError(f"unexpected fetch: {sql}")


class _Pool:
    def __init__(self, facts=None, error=None):
        self.facts, self.error, self.acquired = facts, error, 0

    @asynccontextmanager
    async def acquire(self):
        self.acquired += 1
        if self.error:
            raise self.error
        yield _Conn(self.facts)


def _mark(minutes_ago, failures=0):
    return {"last_ok_at": NOW - timedelta(minutes=minutes_ago) if minutes_ago is not None else None,
            "failures_since_ok": failures}


def _facts(**over):
    facts = {
        "temporary": True,
        "marks": {**{t: _mark(20) for t in pg_inventory_write.CHAIN_TABLES},
                  "app.data_quality_runs": _mark(20)},
        "run": {"run_id": 700, "started_at": NOW - timedelta(hours=2, minutes=30),
                "error_message": None},
        "findings": [],
    }
    facts.update(over)
    return facts


@pytest.fixture
def flag_off(monkeypatch):
    monkeypatch.delenv("KS_WRITE_INVENTORY", raising=False)
    return monkeypatch


async def _ask(facts=None, **kw):
    return await pg_inventory_write.preflight(_Pool(facts or _facts(), **kw), now=NOW)


class TestPreflight:
    @pytest.mark.asyncio
    async def test_a_ready_chain_is_ok(self, flag_off):
        result = await _ask()
        assert result["ok"] is True and result["reasons"] == []
        assert result["temporary"] is True
        assert set(result["replicated"]) == set(pg_inventory_write.CHAIN_TABLES)
        assert result["operational_run"] == {
            "run_id": 700, "age_s": 9000, "failed": False, "findings": 0}

    @pytest.mark.asyncio
    async def test_no_temporary_privilege_is_not_ok_and_says_the_grant(self, flag_off):
        result = await _ask(_facts(temporary=False))
        assert result["ok"] is False and result["temporary"] is False
        (reason,) = result["reasons"]
        assert "GRANT TEMPORARY ON DATABASE ks TO ks_app" in reason

    @pytest.mark.asyncio
    @pytest.mark.parametrize("mark, words", [
        (_mark(50), "last copied 50 min ago (limit 50)"),
        (_mark(10, failures=2), "failing (2 in a row)"),
        (_mark(None), "never been copied"),
        (None, "never been copied"),
    ])
    async def test_a_table_not_copied_within_50_minutes_is_not_ok(self, flag_off, mark, words):
        facts = _facts()
        if mark is None:
            del facts["marks"]["app.stock_movements"]
        else:
            facts["marks"]["app.stock_movements"] = mark
        result = await _ask(facts)
        assert result["ok"] is False
        (reason,) = result["reasons"]
        assert "app.stock_movements" in reason
        assert words in reason

    @pytest.mark.asyncio
    async def test_49_minutes_is_inside_the_limit(self, flag_off):
        facts = _facts()
        facts["marks"]["bronze.offers"] = _mark(49)
        assert (await _ask(facts))["ok"] is True

    @pytest.mark.asyncio
    @pytest.mark.parametrize("mark, words", [
        (_mark(75), "75 min old (limit 75)"),
        (_mark(5, failures=1), "failing (1 in a row)"),
        (_mark(None), "never been copied"),
    ])
    async def test_a_stale_journal_copy_cannot_vouch_for_no_findings(self, flag_off, mark, words):
        facts = _facts()
        facts["marks"]["app.data_quality_runs"] = mark
        result = await _ask(facts)
        assert result["ok"] is False
        (reason,) = result["reasons"]
        assert "quality journal" in reason and words in reason

    @pytest.mark.asyncio
    async def test_no_operational_run_is_not_ok(self, flag_off):
        result = await _ask(_facts(run=None))
        assert result["ok"] is False and result["operational_run"] is None
        assert result["reasons"] == ["no mirror_landing run in the quality journal"]

    @pytest.mark.asyncio
    async def test_a_verdict_older_than_the_canarys_limit_is_not_todays(self, flag_off):
        run = {"run_id": 700, "started_at": NOW - timedelta(hours=30), "error_message": None}
        result = await _ask(_facts(run=run))
        assert result["ok"] is False
        assert "30 h old (limit 30)" in result["reasons"][0]

    @pytest.mark.asyncio
    async def test_a_failed_run_compared_nothing(self, flag_off):
        run = {"run_id": 701, "started_at": NOW - timedelta(hours=1),
               "error_message": "SchemaVersionError: revision mismatch"}
        result = await _ask(_facts(run=run))
        assert result["ok"] is False and result["operational_run"]["failed"] is True
        (reason,) = result["reasons"]
        assert "failed and compared nothing" in reason
        assert "SchemaVersionError" not in reason       # the journal's text stays in the journal

    @pytest.mark.asyncio
    async def test_an_open_finding_on_the_chain_is_not_ok_and_is_named(self, flag_off):
        findings = [{"check_name": "mirror_buckets_disagree", "table_name": "app.stock_movements"}]
        result = await _ask(_facts(findings=findings))
        assert result["ok"] is False and result["operational_run"]["findings"] == 1
        (reason,) = result["reasons"]
        assert "mirror_buckets_disagree on app.stock_movements" in reason

    @pytest.mark.asyncio
    async def test_many_findings_are_counted_not_all_named(self, flag_off):
        findings = [{"check_name": f"c{i}", "table_name": "bronze.offers"} for i in range(8)]
        (reason,) = (await _ask(_facts(findings=findings)))["reasons"]
        assert reason.startswith("8 open finding(s)") and reason.endswith("and 3 more")
        assert "c5" not in reason

    @pytest.mark.asyncio
    async def test_every_reason_is_reported_not_just_the_first(self, flag_off):
        facts = _facts(temporary=False, run=None)
        facts["marks"]["app.inventory_history"] = _mark(90)
        assert len((await _ask(facts))["reasons"]) == 3

    @pytest.mark.asyncio
    async def test_an_unreadable_postgres_is_named_by_class_only(self, flag_off):
        result = await _ask(error=OSError("could not connect to 10.0.0.7:5432 as ks_app"))
        assert result["ok"] is False
        assert result["reasons"] == ["Postgres could not be read (OSError)"]

    @pytest.mark.asyncio
    async def test_once_the_chain_writes_postgres_it_is_not_the_question(self, flag_off):
        flag_off.setenv("KS_WRITE_INVENTORY", "postgres")
        pool = _Pool(_facts())
        result = await pg_inventory_write.preflight(pool, now=NOW)
        assert result["ok"] is None and pool.acquired == 0

    @pytest.mark.asyncio
    async def test_a_latched_chain_is_not_the_question_either(self, flag_off):
        from core import chain_latch

        chain_latch.latch("pg_inventory_write")
        pool = _Pool(_facts())
        assert (await pg_inventory_write.preflight(pool, now=NOW))["ok"] is None

    @pytest.mark.asyncio
    async def test_a_misspelt_flag_is_still_judged(self, flag_off):
        """Not "writes Postgres": the chain is stood down, nothing is copied,
        and the checks are what say so."""
        flag_off.setenv("KS_WRITE_INVENTORY", "postgrse")
        assert (await _ask())["ok"] is True

    def test_the_verdict_age_is_the_canarys_own_limit(self):
        from bot import canary

        assert (pg_inventory_write.PREFLIGHT_VERDICT_WITHIN.total_seconds()
                == canary.DQ_MAX_AGE_S[pg_inventory_write.OPERATIONAL_LAYER])


# ─── /api/health publishes it ────────────────────────────────────────────────

@pytest.fixture
def health(monkeypatch):
    """The health module with an empty preflight cache and a preflight that
    counts its calls."""
    from web.routes.api import health as module

    calls = []

    async def fake(*a, **kw):
        calls.append(1)
        return {"ok": False, "reasons": ["app.stock_movements was last copied 70 min ago (limit 50)"]}

    module._preflight_cache.update(data=None, expires_at=0)
    monkeypatch.setattr(pg_inventory_write, "preflight", fake)
    monkeypatch.delenv("KS_WRITE_INVENTORY", raising=False)
    yield module, calls
    module._preflight_cache.update(data=None, expires_at=0)


class TestHealthPublishesThePreflight:
    @pytest.mark.asyncio
    async def test_it_sits_under_chain_1s_entry_and_nowhere_else(self, health):
        module, _ = health
        block = await module._write_chains_block()
        assert block["pg_inventory_write"]["preflight"]["ok"] is False
        assert "preflight" not in block["pg_expenses_write"]
        # The local half is untouched: the canary still reads `error` and `mismatch` there.
        assert block["pg_inventory_write"]["mode"] == "duckdb"
        assert block["pg_inventory_write"]["mismatch"] is False

    @pytest.mark.asyncio
    async def test_it_is_asked_once_a_minute_not_once_a_probe(self, health):
        module, calls = health
        await module._write_chains_block()
        await module._write_chains_block()
        assert len(calls) == 1

    @pytest.mark.asyncio
    async def test_a_postgres_that_does_not_answer_is_an_answer(self, health, monkeypatch):
        module, _ = health

        async def hangs(*a, **kw):
            await asyncio.sleep(60)

        monkeypatch.setattr(pg_inventory_write, "preflight", hangs)
        monkeypatch.setattr(module, "_PREFLIGHT_TIMEOUT_S", 0.05)
        preflight = (await module._write_chains_block())["pg_inventory_write"]["preflight"]
        assert preflight["ok"] is False and "did not answer" in preflight["reasons"][0]

    def test_the_endpoint_returns_it(self, health):
        from fastapi.testclient import TestClient

        from web.main import app
        from web.ratelimit import limiter

        limiter.reset()
        try:
            body = TestClient(app).get("/api/health").json()
        finally:
            limiter.reset()
        preflight = body["write_chains"]["pg_inventory_write"]["preflight"]
        assert preflight == {"ok": False, "reasons": [
            "app.stock_movements was last copied 70 min ago (limit 50)"]}
