"""Stage 5's PR-2: `KS_DUCKDB=off`'s coherence verdict (`core/duckdb_off.py`).

Off is a claim — nothing web does still needs the DuckDB file — and the
verdict checks it at web's start: every key, one at a time, holds web on `on`
and is published by name (OD-S5-1 (a)); under `on` nothing is read; the chain
keys are derived from the write-chain registry; the verdict is web's alone
and kept for the life of the process; and the pages a check off stands down
would leave unseen hold it, the shared `mirror_*` ones through a mark the
mirror job sets under `on`.
"""
from __future__ import annotations

import ast
import asyncio
import logging
import time
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from core import duckdb_off, duckdb_switch, warehouse_cutover as wc
from core.write_chains import WRITE_CHAINS, chain_name
from tests.unit.test_read_fallback_sites import fresh_read_fallback  # noqa: F401 — fixture

REPO = Path(__file__).resolve().parents[2]

RESOLVED = {"writer": "postgres", "since": "2026-10-01T06:00:00+00:00",
            "resolved": True, "resolved_at": "2026-10-01T06:00:01+00:00"}
NOW = datetime(2026, 10, 20, 9, 0, tzinfo=timezone.utc)
MET_ENV = {"KS_USER_STORE": "postgres"}


def met_chains() -> dict:
    return {chain_name(c): {"env": "postgres", "mode": "postgres", "error": None,
                            "latched": True, "mismatch": False,
                            "unmet_precondition": None}
            for c in WRITE_CHAINS}


def met(**over) -> duckdb_off.Facts:
    """Every key met; `over` breaks the ones a test names."""
    facts = dict(writes_postgres=True, warehouse_mode="postgres", chains=met_chains(),
                 open_retired={}, pg_asked=True, writer=dict(RESOLVED),
                 copied_at=NOW, latched_at=NOW - timedelta(hours=1))
    facts.update(over)
    return duckdb_off.Facts(**facts)


def keys_of(unmet) -> list:
    return [u.key for u in unmet]


def frozen_met() -> dict:
    return {"writer": dict(RESOLVED), "copied_at": NOW, "latched_at": NOW - timedelta(hours=1)}


@pytest.fixture
def world(monkeypatch):
    """A web start whose every fact is met: step 13 in force, every chain on
    Postgres, an empty Alert Gate, the user list on Postgres, and Postgres
    answering off's two facts. Returns the mock reading them — each test
    breaks what it is about. `KS_DUCKDB=off` read, no verdict yet."""
    monkeypatch.setattr(wc, "_mode", wc.POSTGRES)
    monkeypatch.setattr(wc, "_unmet", ())
    monkeypatch.setattr("core.write_chains.chain_modes", met_chains)
    monkeypatch.setenv("KS_USER_STORE", "postgres")
    monkeypatch.setenv("KS_PG_DSN", "postgresql://nobody@127.0.0.1:1/ks")
    monkeypatch.setattr(wc, "REVISION_RETRY_DELAYS_S", (0.0, 0.0))
    read = AsyncMock(return_value=frozen_met())
    monkeypatch.setattr(duckdb_off, "_frozen_facts_on_its_own_connection", read)
    monkeypatch.setenv(duckdb_switch.ENV, "off")
    duckdb_switch.configure_mode()
    return read


# ─── The keys ────────────────────────────────────────────────────────────────

class TestTheKeys:
    def test_the_chain_keys_are_the_registry(self):
        """Derived, never listed: one per chain in `WRITE_CHAINS`, in its
        order, between step 13 and the user list."""
        keys = duckdb_off.keys()
        chains = [k for k in keys if k.startswith(duckdb_off.CHAIN_PREFIX)]
        assert chains == [f"chain:{chain_name(c)}" for c in WRITE_CHAINS]
        assert len(chains) >= 10, "the registry walk read nothing"
        assert keys == (duckdb_off.WAREHOUSE_POSTGRES, *chains, duckdb_off.USER_STORE,
                        duckdb_off.WRITER_RECORD, duckdb_off.SYNC_METADATA_COPIED,
                        duckdb_off.RETIRED_PAGES_CLEAR)

    def test_a_chain_registered_later_is_a_key_and_holds_off(self, monkeypatch):
        """Mutation: spell the chain keys out — the new chain is neither a key
        nor a reason to hold."""
        new = types.ModuleType("core.pg_new_write")
        new.WRITE_ENV, new.CHAIN_TABLES = "KS_WRITE_NEW", ("app.new",)
        monkeypatch.setattr("core.write_chains.WRITE_CHAINS", (*WRITE_CHAINS, new))
        assert "chain:pg_new_write" in duckdb_off.keys()
        assert keys_of(duckdb_off.evaluate(MET_ENV, met())) == ["chain:pg_new_write"]

    def test_every_key_unmet_is_reported_in_order(self):
        facts = met(writes_postgres=False, warehouse_mode="duckdb",
                    chains={}, writer=None, copied_at=None,
                    open_retired={"pk_uniqueness_products": "dq:integrity"})
        assert keys_of(duckdb_off.evaluate({}, facts)) == list(duckdb_off.keys())


# ─── One key at a time ───────────────────────────────────────────────────────

CHAIN_KEYS = [f"chain:{chain_name(c)}" for c in WRITE_CHAINS]


class TestEachKeyHoldsOffAlone:
    def test_all_met_is_off(self):
        assert duckdb_off.evaluate(MET_ENV, met()) == []

    def test_warehouse_postgres(self):
        unmet = duckdb_off.evaluate(MET_ENV, met(
            writes_postgres=False, warehouse_mode="duckdb", warehouse_unmet=("pg_revision",)))
        assert keys_of(unmet) == ["warehouse_postgres"]
        assert "pg_revision" in unmet[0].detail

    @pytest.mark.parametrize("key", CHAIN_KEYS)
    def test_each_chain(self, key):
        chains = met_chains()
        chains[key.split(":", 1)[1]].update(mode="duckdb", latched=False,
                                            unmet_precondition="a reason")
        unmet = duckdb_off.evaluate(MET_ENV, met(chains=chains))
        assert keys_of(unmet) == [key]
        assert "a reason" in unmet[0].detail

    def test_a_chain_web_cannot_read_is_unmet_not_skipped(self):
        chains = met_chains()
        name = chain_name(WRITE_CHAINS[0])
        chains[name] = {"env": "postgress", "mode": None, "error": "not understood"}
        assert keys_of(duckdb_off.evaluate(MET_ENV, met(chains=chains))) == [
            f"chain:{name}"]

    def test_the_registry_unreadable_holds_every_chain(self):
        unmet = duckdb_off.evaluate(MET_ENV, met(chains=None, chains_error="KeyError"))
        assert keys_of(unmet) == CHAIN_KEYS

    @pytest.mark.parametrize("value", [None, "duckdb", "postgress", ""])
    def test_user_store(self, value):
        env = {} if value is None else {"KS_USER_STORE": value}
        assert keys_of(duckdb_off.evaluate(env, met())) == ["user_store"]

    @pytest.mark.parametrize("writer, words", [
        (None, "holds no warehouse_writer"),
        ({"writer": "duckdb", "since": "x"}, "says 'duckdb'"),
        ({"writer": "postgres", "since": "x", "resolved": False}, "not resolved"),
        ({"writer": "postgres", "since": "x"}, "not resolved"),
        ({"writer": "postgres", "resolved": False, "unreadable": True}, "did not write"),
    ])
    def test_writer_record(self, writer, words):
        unmet = duckdb_off.evaluate(MET_ENV, met(writer=writer))
        assert keys_of(unmet) == ["writer_record"]
        assert words in unmet[0].detail

    @pytest.mark.parametrize("copied, latched, ok", [
        (None, NOW, False),
        (NOW - timedelta(hours=2), NOW - timedelta(hours=1), False),
        (NOW, NOW, False),
        (NOW, NOW - timedelta(seconds=1), True),
        (NOW, None, True),
        # Naive instants read as UTC, as asyncpg hands `timestamp` back.
        (NOW.replace(tzinfo=None), (NOW - timedelta(hours=1)).replace(tzinfo=None), True),
    ])
    def test_sync_metadata_copied(self, copied, latched, ok):
        unmet = duckdb_off.evaluate(MET_ENV, met(copied_at=copied, latched_at=latched))
        assert keys_of(unmet) == ([] if ok else ["sync_metadata_copied"])

    def test_a_postgres_read_that_failed_holds_both_postgres_keys(self):
        unmet = duckdb_off.evaluate(MET_ENV, met(
            pg_error="ConnectionRefusedError", writer=None, copied_at=None, latched_at=None))
        assert keys_of(unmet) == ["writer_record", "sync_metadata_copied"]
        assert all("ConnectionRefusedError" in u.detail for u in unmet)

    def test_postgres_not_asked_judges_neither_postgres_key(self):
        """Asked only once step 13 is in force: a start that could not reach
        Postgres names `warehouse_postgres` alone."""
        unmet = duckdb_off.evaluate(MET_ENV, met(
            writes_postgres=False, warehouse_unmet=("pg_revision",),
            pg_asked=False, writer=None, copied_at=None))
        assert keys_of(unmet) == ["warehouse_postgres"]

    def test_retired_pages_clear(self):
        unmet = duckdb_off.evaluate(MET_ENV, met(
            open_retired={"pk_uniqueness_buyers": "dq:integrity"}))
        assert keys_of(unmet) == ["retired_pages_clear"]
        assert "pk_uniqueness_buyers (dq:integrity)" in unmet[0].detail

    def test_an_alert_gate_it_cannot_read_holds(self):
        assert keys_of(duckdb_off.evaluate(MET_ENV, met(
            open_retired=None, open_retired_error="OSError"))) == ["retired_pages_clear"]


# ─── Through the start ───────────────────────────────────────────────────────

ALL_BROKEN = {
    "warehouse_postgres": lambda mp, read: mp.setattr(wc, "_mode", wc.DUCKDB),
    "user_store": lambda mp, read: mp.setenv("KS_USER_STORE", "duckdb"),
    "writer_record": lambda mp, read: setattr(read, "return_value", {
        **frozen_met(), "writer": None}),
    "sync_metadata_copied": lambda mp, read: setattr(read, "return_value", {
        **frozen_met(), "copied_at": None}),
    "retired_pages_clear": lambda mp, read: __import__(
        "core.alerting", fromlist=["_gate"])._gate.note_delivered_conditions(
            ["pk_uniqueness_categories"], "dq:integrity"),
}


def _break_chain(name):
    def breaks(mp, read):
        chains = met_chains()
        chains[name]["mode"] = "duckdb"
        mp.setattr("core.write_chains.chain_modes", lambda: chains)
    return breaks


ALL_BROKEN.update({f"chain:{chain_name(c)}": _break_chain(chain_name(c))
                   for c in WRITE_CHAINS})


class TestTheStart:
    def test_every_key_breaks_alone_through_the_real_start(self):
        """Nothing in the table above is listed by hand twice: it must name
        every key the verdict has."""
        assert set(ALL_BROKEN) == set(duckdb_off.keys())

    @pytest.mark.parametrize("key", sorted(ALL_BROKEN))
    def test_one_unmet_key_holds_web_on_and_publishes_it(self, key, world, monkeypatch):
        ALL_BROKEN[key](monkeypatch, world)
        assert duckdb_off.settle() == "on"
        assert duckdb_switch.mode() == "on" and not duckdb_switch.is_off()
        assert duckdb_switch.held_back() == (key,)
        assert keys_of(duckdb_off.unmet()) == [key]
        block = duckdb_switch.health_block()
        assert block["held_back"] == [key] and block["value"] == "off"

    def test_met_runs_off(self, world, caplog):
        with caplog.at_level(logging.WARNING, logger="core.duckdb_off"):
            assert duckdb_off.settle() == "off"
        assert duckdb_switch.is_off() and duckdb_switch.held_back() == ()
        assert duckdb_switch.health_block()["held_back"] == []
        assert "every precondition of off holds" in caplog.text
        world.assert_awaited_once()

    def test_held_back_is_logged_with_its_details_and_published_without(
        self, world, monkeypatch, caplog,
    ):
        monkeypatch.setenv("KS_USER_STORE", "duckdb")
        with caplog.at_level(logging.CRITICAL, logger="core.duckdb_off"):
            duckdb_off.settle()
        assert "KS_USER_STORE is 'duckdb'" in caplog.text
        assert "runs as on" in caplog.text
        assert "KS_USER_STORE is" not in repr(duckdb_switch.health_block())

    def test_kept_for_the_life_of_the_process(self, world, monkeypatch):
        """The scheduler configures again; a held web stays on, and nothing is
        read a second time. Mutation: drop the `held_back() is not None`
        return in `settle`."""
        monkeypatch.setenv("KS_USER_STORE", "duckdb")
        duckdb_off.settle()
        monkeypatch.setenv("KS_USER_STORE", "postgres")
        assert duckdb_switch.configure_mode() == "on"
        assert duckdb_off.settle() == "on"
        assert duckdb_switch.held_back() == ("user_store",)
        world.assert_awaited_once()

    def test_an_off_that_held_stays_off(self, world, monkeypatch):
        duckdb_off.settle()
        monkeypatch.setenv("KS_USER_STORE", "duckdb")
        assert duckdb_switch.configure_mode() == "off"
        assert duckdb_off.settle() == "off"
        world.assert_awaited_once()

    def test_a_value_other_than_off_forgets_the_verdict(self, world, monkeypatch):
        monkeypatch.setenv("KS_USER_STORE", "duckdb")
        duckdb_off.settle()
        monkeypatch.setenv(duckdb_switch.ENV, "on")
        assert duckdb_switch.configure_mode() == "on"
        assert duckdb_switch.held_back() is None

    @pytest.mark.parametrize("value", [None, "on", "of"])
    def test_anything_but_off_reads_nothing(self, value, monkeypatch):
        """Under `on`, today, and under a typo, the verdict asks nothing — not
        Postgres, not the chains, not the Alert Gate — and /api/health
        publishes what it always did. Mutation: gather the facts first."""
        if value is None:
            monkeypatch.delenv(duckdb_switch.ENV, raising=False)
        else:
            monkeypatch.setenv(duckdb_switch.ENV, value)
        boom = MagicMock(side_effect=AssertionError("read under on"))
        for name in ("gather_facts", "_local_facts", "_read_postgres_in_a_worker",
                     "open_retired_pages"):
            monkeypatch.setattr(duckdb_off, name, boom)
        monkeypatch.setattr("core.write_chains.chain_modes", boom)
        assert duckdb_off.settle() == "on"
        assert duckdb_switch.held_back() is None
        assert set(duckdb_switch.health_block()) == {
            "mode", "value", "error", "opened_while_off"}
        boom.assert_not_called()

    def test_postgres_is_asked_only_once_step_13_is_in_force(self, world, monkeypatch):
        monkeypatch.setattr(wc, "_mode", wc.DUCKDB)
        duckdb_off.settle()
        world.assert_not_awaited()
        assert duckdb_switch.held_back() == ("warehouse_postgres",)

    def test_a_failed_read_is_asked_again_and_an_answer_is_not(self, world):
        world.side_effect = [OSError("the database system is starting up"), frozen_met()]
        assert duckdb_off.settle() == "off"
        assert world.await_count == 2

    def test_an_answer_is_not_asked_again(self, world):
        world.return_value = {**frozen_met(), "writer": None}
        duckdb_off.settle()
        assert world.await_count == 1
        assert duckdb_switch.held_back() == ("writer_record",)

    def test_three_failed_reads_hold_and_say_why(self, world):
        world.side_effect = OSError("refused")
        duckdb_off.settle()
        assert world.await_count == wc.REVISION_READ_ATTEMPTS
        assert duckdb_switch.held_back() == ("writer_record", "sync_metadata_copied")
        assert all("OSError" in u.detail for u in duckdb_off.unmet())

    def test_a_read_that_hangs_is_bounded(self, world, monkeypatch):
        async def hang(_dsn):
            await asyncio.sleep(30)

        monkeypatch.setattr(duckdb_off, "_frozen_facts_on_its_own_connection", hang)
        monkeypatch.setattr(wc, "REVISION_READ_TIMEOUT_S", 0.05)
        started = time.monotonic()
        duckdb_off.settle()
        assert time.monotonic() - started < 5
        assert "did not answer" in duckdb_off.unmet()[0].detail

    def test_it_never_raises_and_a_verdict_it_cannot_reach_holds(self, world, monkeypatch):
        monkeypatch.setattr(duckdb_off, "evaluate", MagicMock(side_effect=KeyError("x")))
        assert duckdb_off.settle() == "on"
        assert duckdb_switch.held_back() == duckdb_off.keys()


@pytest.fixture
def configured(monkeypatch, fresh_read_fallback):  # noqa: F811
    """`configure_modes()` fills process-wide caches; these are put back."""
    from core import pg_utm_parse

    for name in ("_mode", "_mode_error"):
        monkeypatch.setattr(pg_utm_parse, name, getattr(pg_utm_parse, name))


class TestWhoReachesIt:
    def _spy(self, monkeypatch):
        calls = []
        real = duckdb_off.settle

        def settle(*a, **k):
            calls.append("off")
            return real(*a, **k)

        monkeypatch.setattr(duckdb_off, "settle", settle)
        return calls

    def test_web_reaches_it_last_and_the_mapping_says_what_runs(
        self, world, configured, monkeypatch,
    ):
        """After step 13's verdict and chain 3's hold, which it reads.
        Mutation: settle before `settle_hold`."""
        from core import pg_orders_write
        from core.runtime_modes import configure_modes

        order = self._spy(monkeypatch)
        real_hold, real_wc = pg_orders_write.settle_hold, wc.configure_mode
        monkeypatch.setattr(pg_orders_write, "settle_hold",
                            lambda: (order.append("chain 3"), real_hold())[1])
        monkeypatch.setattr(wc, "configure_mode",
                            lambda: (order.append("step 13"), real_wc())[1])
        monkeypatch.setattr("core.write_chains.chain_modes", met_chains)
        modes = configure_modes(web=True)
        assert order == ["step 13", "chain 3", "off"]
        # KS_WRITE_WAREHOUSE is unset here, so step 13 is not in force.
        assert modes[duckdb_switch.ENV] == "on"
        assert duckdb_switch.held_back() == ("warehouse_postgres",)

    def test_a_script_reaches_none_and_stays_off(self, world, configured, monkeypatch):
        """A one-off is not the syncer, and a blip in its own read must not
        open the file in a week web keeps silent. Mutation: reach the verdict
        whatever `web` says."""
        from core.runtime_modes import configure_modes

        calls = self._spy(monkeypatch)
        assert configure_modes()[duckdb_switch.ENV] == "off"
        assert calls == [] and duckdb_switch.held_back() is None
        assert duckdb_switch.is_off()

    @pytest.mark.parametrize("value", [None, "on"])
    def test_under_on_web_configures_as_it_always_did(
        self, value, configured, monkeypatch,
    ):
        """The mapping, the switch and its published block are the same
        whether web says it is web or not — `on` reads no verdict. Mutation:
        hand `on` a verdict."""
        from core.runtime_modes import configure_modes

        if value is None:
            monkeypatch.delenv(duckdb_switch.ENV, raising=False)
        else:
            monkeypatch.setenv(duckdb_switch.ENV, value)
        script = configure_modes()
        block = duckdb_switch.health_block()
        assert configure_modes(web=True) == script
        assert duckdb_switch.health_block() == block == {
            "mode": "on", "value": value, "error": None, "opened_while_off": {}}
        assert duckdb_switch.held_back() is None

    @pytest.mark.parametrize("path, function", [
        ("web/main.py", "startup_event"),
        ("core/scheduler.py", "start"),
    ])
    def test_web_s_two_calls_say_they_are_web(self, path, function):
        tree = ast.parse((REPO / path).read_text(encoding="utf-8"))
        [fn] = [n for n in ast.walk(tree)
                if isinstance(n, ast.AsyncFunctionDef) and n.name == function]
        calls = [n for n in ast.walk(fn) if isinstance(n, ast.Call)
                 and getattr(n.func, "id", getattr(n.func, "attr", None)) == "configure_modes"]
        assert calls and all(
            any(k.arg == "web" and isinstance(k.value, ast.Constant) and k.value.value is True
                for k in c.keywords) for c in calls), (path, function)

    def test_no_script_says_it_is_web(self):
        for path in (REPO / "scripts").rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for n in ast.walk(tree):
                if isinstance(n, ast.Call) and getattr(
                        n.func, "id", getattr(n.func, "attr", None)) == "configure_modes":
                    assert not n.keywords, path


# ─── What off stands down, and its pages ─────────────────────────────────────

def _module_functions(path: str) -> dict:
    tree = ast.parse((REPO / path).read_text(encoding="utf-8"))
    return {n.name: n for n in ast.walk(tree)
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}


class TestWhatIsRetired:
    def test_the_pk_tables_are_the_scan_s_bare_checks(self):
        """`_pk_uniqueness_check` calls in the integrity scan that no chain's
        stand-down wraps (`landing(...)`): read out of the scan. Mutation: add
        a fourth bare call, or wrap one."""
        scan = _module_functions("core/data_quality.py")["check_internal_integrity"]
        wrapped = {id(n) for lam in ast.walk(scan) if isinstance(lam, ast.Lambda)
                   for n in ast.walk(lam)}
        bare = [n for n in ast.walk(scan) if isinstance(n, ast.Call)
                and getattr(n.func, "id", None) == "_pk_uniqueness_check"
                and id(n) not in wrapped]
        assert tuple(n.args[1].value for n in bare) == duckdb_off.PK_TABLES

    def test_the_shadow_conditions_are_compare_shadow_s(self):
        from tests.unit.test_warehouse_writer import _check_names_reached
        from tests.unit.test_warehouse_writer import _module_functions as top_level

        fns = top_level("core/mirror_reconciliation.py")
        assert _check_names_reached(fns, "compare_shadow") == duckdb_off.SHADOW_CONDITIONS

    def test_every_retired_condition_is_registered(self):
        from core.alerting import REGISTRY

        assert duckdb_off.retired_conditions() <= set(REGISTRY)

    def test_the_history_tables_are_the_three_no_chain_owns(self):
        """Derived from the comparison's registries; pinned here so a fourth
        table copied out of DuckDB with no chain is a decision, not a quiet
        addition to what off freezes."""
        from core.pg_operational import (
            RECONCILIATION_LOG_TABLE, REFRESHES_TABLE, SYNC_METADATA_TABLE,
        )

        assert sorted(duckdb_off.history_tables()) == sorted(
            (SYNC_METADATA_TABLE, REFRESHES_TABLE, RECONCILIATION_LOG_TABLE))

    def test_step_13_s_mark_and_off_s_are_different_marks(self):
        assert duckdb_off.HISTORY_MARK != wc.COMPARISON_MARK


def _critical(name, table):
    from core.data_quality import IntegrityIssue, Severity

    return IntegrityIssue(check_name=name, table_name=table,
                          severity=Severity.CRITICAL, count=1)


def _mirror_run(outcomes):
    from tests.unit.test_mirror_landing_isolation import _run

    return asyncio.run(_run(outcomes))


class TestTheHistoryPages:
    """`mirror_*` is every comparison's name, so a page of the three history
    tables is marked by the mirror job under `on`, and the mark holds off."""

    GROUP = "dq:mirror_landing"

    def test_a_page_the_history_comparison_reports_holds_off(self):
        from core.alerting import _gate

        _gate.note_delivered_conditions(["mirror_row_values"], self.GROUP)
        _mirror_run({"reconcile_operational": [
            _critical("mirror_row_values", "app.sync_metadata")]})
        assert duckdb_off.open_retired_pages() == {"mirror_row_values": self.GROUP}
        assert keys_of(duckdb_off.evaluate(MET_ENV, met(
            open_retired=duckdb_off.open_retired_pages()))) == ["retired_pages_clear"]

    def test_the_same_name_from_another_table_does_not(self):
        """Mutation: mark every CRITICAL `reconcile_operational` reports."""
        from core.alerting import _gate

        _gate.note_delivered_conditions(["mirror_row_values"], self.GROUP)
        _mirror_run({"reconcile_operational": [
            _critical("mirror_row_values", "app.stock_movements")]})
        assert duckdb_off.open_retired_pages() == {}

    def test_a_clean_run_takes_the_mark_off(self):
        from core.alerting import _gate

        _gate.note_delivered_conditions(["mirror_row_values"], self.GROUP)
        _mirror_run({"reconcile_operational": [
            _critical("mirror_row_values", "app.warehouse_refreshes")]})
        _mirror_run({"reconcile_operational": [
            _critical("mirror_row_values", "bronze.offers")]})
        assert duckdb_off.open_retired_pages() == {}

    def test_a_run_whose_comparison_raised_changes_nothing(self):
        from core.alerting import _gate

        _gate.note_delivered_conditions(["mirror_row_values"], self.GROUP)
        _mirror_run({"reconcile_operational": [
            _critical("mirror_row_values", "app.reconciliation_log")]})
        _mirror_run({"reconcile_operational": RuntimeError("down")})
        assert duckdb_off.open_retired_pages() == {"mirror_row_values": self.GROUP}

    def test_under_off_nothing_is_marked_or_unmarked(self, duckdb_off_dir):
        """A run under off did not compare them (PR-7 stands them down), so it
        knows nothing about their pages. Mutation: drop the `is_off()` guard."""
        from core.alerting import _gate

        _gate.note_delivered_conditions(["mirror_row_values"], self.GROUP)
        _mirror_run({"reconcile_operational": [
            _critical("mirror_row_values", "app.sync_metadata")]})
        assert duckdb_off.open_retired_pages() == {}

    def test_under_on_the_mark_moves_nothing_else(self):
        """What `on` gains is a mark on a delivered entry; what is delivered,
        what resolves, and step 13's retired pages read the same."""
        from core.alerting import _gate, delivered_conditions

        _gate.note_delivered_conditions(["mirror_row_values"], self.GROUP)
        before = (delivered_conditions(), wc.open_retired_conditions())
        _, paged, resolved = _mirror_run({"reconcile_operational": [
            _critical("mirror_row_values", "app.sync_metadata")]})
        assert (delivered_conditions(), wc.open_retired_conditions()) == before
        assert paged == [["mirror_row_values"]]
        assert resolved == [["mirror_row_values"]]

    def test_a_retired_name_holds_off_whatever_group_delivered_it(self):
        from core.alerting import _gate

        _gate.note_delivered_conditions(["shadow_row_values"], self.GROUP)
        _gate.note_delivered_conditions(["pk_uniqueness_products"], "dq:integrity")
        _gate.note_delivered_conditions(["mirror_failing"], self.GROUP)
        assert duckdb_off.open_retired_pages() == {
            "shadow_row_values": self.GROUP, "pk_uniqueness_products": "dq:integrity"}


@pytest.fixture
def duckdb_off_dir(monkeypatch, tmp_path):
    """`KS_DUCKDB=off` with no verdict — what the mirror job reads — and the
    store's directory somewhere nothing is."""
    monkeypatch.setattr("core.duckdb_store.DB_DIR", tmp_path / "data")
    monkeypatch.setattr("core.duckdb_store.DB_PATH", tmp_path / "data" / "x.duckdb")
    monkeypatch.setenv(duckdb_switch.ENV, "off")
    duckdb_switch.configure_mode()


# ─── Published ───────────────────────────────────────────────────────────────

class TestPublished:
    def test_the_public_endpoint_carries_the_keys(self, world, monkeypatch):
        from fastapi.testclient import TestClient

        from web.main import app
        from web.ratelimit import limiter

        monkeypatch.setenv("KS_USER_STORE", "duckdb")
        duckdb_off.settle()
        limiter.reset()
        try:
            block = TestClient(app).get("/api/health").json()["duckdb_switch"]
        finally:
            limiter.reset()
        assert block["held_back"] == ["user_store"] and block["mode"] == "on"
        assert "KS_USER_STORE" not in repr(block)

    def test_the_canary_reads_what_web_publishes(self, world, monkeypatch):
        from bot import canary

        monkeypatch.setenv("KS_USER_STORE", "duckdb")
        duckdb_off.settle()
        payload = {"duckdb_switch": duckdb_switch.health_block()}
        [(key, line)] = canary.check_duckdb_off_held_back(payload)
        assert key == "duckdb_off_held_back" and line.endswith("user_store")
        assert canary.duckdb_silent(payload) is None, "held on, the week's watch is not written"
