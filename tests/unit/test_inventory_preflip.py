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
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from core import pg_inventory_write
from core import sync_service as sync_mod
from core.exceptions import KeyCRMConnectionError
from core.sync_service import SyncService

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


# ─── The stock step cannot take the tick down (on the Postgres path) ─────────

STALE, FRESH = timedelta(hours=2), timedelta(minutes=1)


def _tick_service(monkeypatch, *, offers_due=True, stocks_due=True):
    """A SyncService whose tick reaches the inventory steps and nothing else:
    no orders in the window, and every other hourly/daily branch fresh.
    `calls` records the order the store is written in."""
    calls = []
    now = datetime.now(timezone.utc)
    due = {"offers": offers_due, "stocks": stocks_due}

    async def last_sync(kind):
        return now - (STALE if due.get(kind) else FRESH)

    def recorder(name, result=None):
        async def fn(*args, **kwargs):
            calls.append((name, *args))
            return result
        return AsyncMock(side_effect=fn)

    store = SimpleNamespace(
        get_last_sync_time=AsyncMock(side_effect=last_sync),
        set_last_sync_time=recorder("set_last_sync_time"),
        upsert_offers=recorder("upsert_offers", 2),
        upsert_stocks=recorder("upsert_stocks", 3),
        refresh_sku_inventory_status=recorder("refresh_sku_inventory_status", 3),
        record_sku_inventory_snapshot=recorder("record_sku_inventory_snapshot", True),
        record_inventory_snapshot=recorder("record_inventory_snapshot", True),
        mark_warehouse_dirty=AsyncMock(),
    )
    client = SimpleNamespace(
        fetch_all_offers=AsyncMock(return_value=[{"id": 1}, {"id": 2}]),
        fetch_all_stocks=AsyncMock(return_value=[{"id": 1}, {"id": 2}, {"id": 3}]),
    )
    monkeypatch.setattr(sync_mod, "get_async_client", AsyncMock(return_value=client))
    service = SyncService(store=store)
    service._should_skip_sync = lambda: (False, "")
    service._fetch_orders_with_date_filter = AsyncMock(return_value=[])
    return service, store, client, calls


@pytest.fixture
def on_postgres(monkeypatch):
    monkeypatch.setenv("KS_WRITE_INVENTORY", "postgres")
    return monkeypatch


def _stock_watermark_moved(calls) -> bool:
    return ("set_last_sync_time", "stocks") in calls


class TestTheStockStepOnPostgres:
    @pytest.mark.asyncio
    async def test_a_raising_rebuild_is_recorded_and_the_tick_returns(self, on_postgres):
        """The missing-TEMPORARY case: the upsert committed, the rebuild
        raised. Before DN-24 that left `incremental_sync` altogether."""
        service, store, _client, calls = _tick_service(on_postgres)
        store.refresh_sku_inventory_status.side_effect = RuntimeError(
            "the /inventory status rebuild needs one temporary table")

        stats = await service.incremental_sync()

        assert "skipped" not in stats and stats["stocks"] == 3
        assert not _stock_watermark_moved(calls), "last_sync_stocks moved past a failed rebuild"
        step = service.inventory_step_health()
        assert step["failures_since_ok"] == 1 and step["last_failed_step"] == "stocks"
        assert step["last_error"] == "RuntimeError" and step["last_failure_at"]
        assert 0 < step["retry_in_s"] <= sync_mod.INVENTORY_RETRY_AFTER_S
        assert service._last_sync_time is not None, "the tick ended before the backoff"
        store.record_sku_inventory_snapshot.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_raising_upsert_is_recorded_the_same_way(self, on_postgres):
        service, store, _client, calls = _tick_service(on_postgres)
        store.upsert_stocks.side_effect = OSError("connection refused")

        await service.incremental_sync()

        assert not _stock_watermark_moved(calls)
        assert service.inventory_step_health()["last_error"] == "OSError"
        store.refresh_sku_inventory_status.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_the_watermark_moves_last(self, on_postgres):
        service, _store, _client, calls = _tick_service(on_postgres)

        stats = await service.incremental_sync()

        assert [c[0] for c in calls] == [
            "upsert_offers", "set_last_sync_time",                    # offers, as before
            "upsert_stocks", "refresh_sku_inventory_status",
            "record_sku_inventory_snapshot", "record_inventory_snapshot",
            "set_last_sync_time"]
        assert calls[1] == ("set_last_sync_time", "offers")
        assert calls[-1] == ("set_last_sync_time", "stocks")
        assert (stats["offers"], stats["stocks"]) == (2, 3)
        step = service.inventory_step_health()
        assert step["failures_since_ok"] == 0 and step["last_ok_at"] and step["retry_in_s"] is None

    @pytest.mark.asyncio
    async def test_a_success_clears_the_count(self, on_postgres):
        service, store, _client, _calls = _tick_service(on_postgres)
        store.record_inventory_snapshot.side_effect = [OSError("gone"), True]
        await service.incremental_sync()
        assert service.inventory_step_health()["failures_since_ok"] == 1

        service._inventory_retry_at = 0.0                         # the hold has passed
        await service.incremental_sync()
        assert service.inventory_step_health()["failures_since_ok"] == 0

    @pytest.mark.asyncio
    async def test_the_next_attempt_waits_for_the_hold(self, on_postgres):
        """With the watermark held the step is due again on the next tick; it
        must not fetch every stock from KeyCRM once a minute while it fails."""
        service, store, client, _calls = _tick_service(on_postgres)
        store.refresh_sku_inventory_status.side_effect = RuntimeError("no TEMPORARY")
        await service.incremental_sync()
        await service.incremental_sync()
        assert client.fetch_all_stocks.await_count == 1
        asked = [c.args for c in store.get_last_sync_time.await_args_list]
        assert asked.count(("stocks",)) == 1, "the held step read its watermark again"

        service._inventory_retry_at = 0.0
        await service.incremental_sync()
        assert client.fetch_all_stocks.await_count == 2

    @pytest.mark.asyncio
    async def test_an_unreadable_watermark_is_recorded_too(self, on_postgres):
        """Under this flag the watermarks live in `meta.chain_watermarks`, so
        a Postgres that is down fails the read before any write."""
        service, store, client, _calls = _tick_service(on_postgres)
        real = store.get_last_sync_time.side_effect

        async def last_sync(kind):
            if kind == "offers":
                raise OSError("connection refused")
            return await real(kind)

        store.get_last_sync_time.side_effect = last_sync
        await service.incremental_sync()
        assert service.inventory_step_health()["last_failed_step"] == "offers"
        client.fetch_all_stocks.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_failed_offers_step_skips_the_stocks_step(self, on_postgres):
        """Stocks read the offer→product map: written ahead of their offers,
        the new SKUs' movements would be recorded with no product."""
        service, store, client, calls = _tick_service(on_postgres)
        store.upsert_offers.side_effect = OSError("connection refused")

        await service.incremental_sync()

        assert service.inventory_step_health()["last_failed_step"] == "offers"
        client.fetch_all_stocks.assert_not_awaited()
        assert calls == []

    @pytest.mark.asyncio
    async def test_a_keycrm_failure_rebuilds_but_holds_the_watermark(self, on_postgres):
        """As on the DuckDB path: nothing fetched is nothing to stamp, and the
        rebuild and the snapshots still run on the stock already held."""
        service, store, client, calls = _tick_service(on_postgres, offers_due=False)
        client.fetch_all_stocks.side_effect = KeyCRMConnectionError("timeout")

        stats = await service.incremental_sync()

        assert stats["stocks"] == 0 and not _stock_watermark_moved(calls)
        store.record_inventory_snapshot.assert_awaited_once()
        assert service.inventory_step_health()["failures_since_ok"] == 0

    @pytest.mark.asyncio
    async def test_nothing_due_is_not_a_success(self, on_postgres):
        service, _store, _client, calls = _tick_service(
            on_postgres, offers_due=False, stocks_due=False)
        await service.incremental_sync()
        assert calls == [] and service.inventory_step_health()["last_ok_at"] is None


class TestTheDuckDBPathIsUnchanged:
    @pytest.mark.asyncio
    async def test_a_raising_rebuild_still_leaves_the_tick(self, monkeypatch):
        """Production today. DN-24 changes nothing here, the escape included."""
        monkeypatch.delenv("KS_WRITE_INVENTORY", raising=False)
        service, store, _client, calls = _tick_service(monkeypatch)
        store.refresh_sku_inventory_status.side_effect = RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            await service.incremental_sync()
        # ...and the DuckDB path still stamps straight after the upsert.
        assert _stock_watermark_moved(calls)
        assert service.inventory_step_health()["failures_since_ok"] == 0

    def test_a_misspelt_flag_takes_the_path_it_always_took(self, monkeypatch):
        """Where the chain's own watermark read raises on it (DN-01)."""
        monkeypatch.setenv("KS_WRITE_INVENTORY", "postgrse")
        assert sync_mod._inventory_writes_postgres() is False

    def test_a_latched_chain_takes_the_postgres_path_whatever_the_flag(self, monkeypatch):
        from core import chain_latch

        monkeypatch.delenv("KS_WRITE_INVENTORY", raising=False)
        chain_latch.latch("pg_inventory_write")
        assert sync_mod._inventory_writes_postgres() is True


class TestHealthPublishesTheSyncStep:
    @pytest.mark.asyncio
    async def test_it_sits_beside_the_preflight(self, health, monkeypatch):
        module, _ = health
        state = sync_mod.InventoryStepState()
        state.failed("stocks", RuntimeError("the /inventory status rebuild needs one temporary table"))
        service = SimpleNamespace(inventory_step_health=lambda: state.published(599))
        monkeypatch.setattr(sync_mod, "get_sync_service", AsyncMock(return_value=service))

        step = (await module._write_chains_block())["pg_inventory_write"]["sync_step"]

        assert step["failures_since_ok"] == 1 and step["last_failed_step"] == "stocks"
        assert step["last_error"] == "RuntimeError" and step["retry_in_s"] == 599
        assert "temporary" not in str(step)                   # the class, never the text

    @pytest.mark.asyncio
    async def test_no_sync_service_is_null_not_empty(self, health, monkeypatch):
        module, _ = health
        monkeypatch.setattr(sync_mod, "get_sync_service",
                            AsyncMock(side_effect=RuntimeError("no store")))
        assert (await module._write_chains_block())["pg_inventory_write"]["sync_step"] is None
