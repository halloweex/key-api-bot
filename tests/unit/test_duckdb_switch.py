"""`KS_DUCKDB` and its tripwire — the week of silence's first detector (OD-17 (a)).

Stage 5 may begin only after seven days in which nothing opened the DuckDB
file, and that has to be shown by running web with `KS_DUCKDB=off`, not by
reading the code (core/duckdb_switch.py). What is held here:

- the mode: unset is `on`, today; a value nobody understands runs `on` and
  says so; a process that never configured reads it on its first open;
- the opener: under `off` it refuses before the driver runs — the file is not
  created — counts at the raise, names the site, bounds the sites, and
  publishes no exception text;
- the walk: `open_file` is the only reference to `duckdb.connect` in `core/`,
  `web/` and `bot/`, and the host-side tools that connect directly are exactly
  the exemptions written down here;
- web's startup survives the refusal, `/api/health` publishes it, and the
  canary pages it CRITICAL and keeps the watch only while web runs `off`.

Each guard names the mutation that makes it fail.
"""
from __future__ import annotations

import ast
import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from bot import canary
from core import duckdb_switch
from core.alerting import REGISTRY, Kind
from tests.unit.test_canary import DASHBOARD, _healthy_payload, _mock_transport
from tests.unit.test_read_fallback_sites import admin_client  # noqa: F401  (fixture)

REPO = Path(__file__).resolve().parents[2]


def _off(monkeypatch):
    monkeypatch.setenv(duckdb_switch.ENV, "off")
    duckdb_switch.configure_mode()


# ─── The mode ────────────────────────────────────────────────────────────────

class TestTheMode:
    def test_unset_is_on_and_says_nothing(self, caplog):
        with caplog.at_level(logging.WARNING, logger="core.duckdb_switch"):
            assert duckdb_switch.configure_mode() == "on"
        assert duckdb_switch.value() is None and duckdb_switch.mode_error() is None
        assert caplog.text == ""

    @pytest.mark.parametrize("raw, mode", [("on", "on"), ("off", "off"),
                                           (" OFF ", "off"), ("On", "on")])
    def test_the_two_values_read_the_way_every_switch_reads(self, monkeypatch, raw, mode):
        monkeypatch.setenv(duckdb_switch.ENV, raw)
        assert duckdb_switch.configure_mode() == mode
        assert duckdb_switch.mode_error() is None

    def test_off_says_so_once_at_critical(self, monkeypatch, caplog):
        monkeypatch.setenv(duckdb_switch.ENV, "off")
        with caplog.at_level(logging.CRITICAL, logger="core.duckdb_switch"):
            duckdb_switch.configure_mode()
            duckdb_switch.configure_mode()
        assert caplog.text.count("must not open the DuckDB file") == 1

    def test_a_value_nobody_understands_runs_on_and_is_published(self, monkeypatch, caplog):
        """OD-09: web is the only syncer, so a typo must not stop it — and
        whoever typed `of` believes the week is running, so it is said."""
        monkeypatch.setenv(duckdb_switch.ENV, "of")
        with caplog.at_level(logging.ERROR, logger="core.duckdb_switch"):
            assert duckdb_switch.configure_mode() == "on"
            duckdb_switch.configure_mode()
        assert duckdb_switch.value() == "of"
        assert "KS_DUCKDB='of'" in duckdb_switch.mode_error()
        assert caplog.text.count("is not one of") == 1
        assert duckdb_switch.health_block()["error"] == duckdb_switch.mode_error()

    def test_an_unconfigured_process_reads_the_switch_on_its_first_open(
        self, monkeypatch, tmp_path,
    ):
        """A script that never called `configure_modes()` must not be the one
        process that opens the file under `off`.
        Mutation: make `mode()` return `_mode or ON` without configuring."""
        monkeypatch.setenv(duckdb_switch.ENV, "off")
        assert duckdb_switch._mode is None  # the conftest fixture forgot it
        with pytest.raises(duckdb_switch.DuckDBOpenedWhileOff):
            duckdb_switch.open_file(tmp_path / "never.duckdb")
        assert not (tmp_path / "never.duckdb").exists()

    def test_configure_modes_reads_it_before_the_boot_sync(self, monkeypatch):
        from core.runtime_modes import configure_modes

        monkeypatch.setenv(duckdb_switch.ENV, "off")
        modes = configure_modes()
        assert modes[duckdb_switch.ENV] == "off"
        assert list(modes)[0] == duckdb_switch.ENV, (
            "first: the boot sync's first act is to open the file")


# ─── The opener ──────────────────────────────────────────────────────────────

class TestTheOpener:
    def test_on_opens_the_file_as_ever(self, tmp_path):
        con = duckdb_switch.open_file(tmp_path / "a.duckdb")
        try:
            assert con.execute("SELECT 42").fetchone() == (42,)
        finally:
            con.close()
        assert duckdb_switch.opened() == {}

    def test_off_refuses_before_the_driver_and_creates_nothing(self, monkeypatch, tmp_path):
        """Mutation: delete `guard()` from `open_file`."""
        _off(monkeypatch)
        target = tmp_path / "a.duckdb"
        with pytest.raises(duckdb_switch.DuckDBOpenedWhileOff) as raised:
            duckdb_switch.open_file(target, read_only=True)
        assert not target.exists() and list(tmp_path.iterdir()) == []
        site = f"{__name__}:test_off_refuses_before_the_driver_and_creates_nothing"
        assert raised.value.site == site and raised.value.count == 1
        assert list(duckdb_switch.opened()) == [site]

    def test_a_refusal_the_caller_swallows_is_still_counted(self, monkeypatch, tmp_path):
        """Dozens of callers wrap `get_store()` in `except Exception`; the
        count is taken at the raise so none of them can hide an open.
        Mutation: move `_tally` after the raise in `guard()`."""
        _off(monkeypatch)
        for _ in range(3):
            try:
                duckdb_switch.open_file(tmp_path / "a.duckdb")
            except Exception:
                pass
        [(site, entry)] = duckdb_switch.opened().items()
        assert entry["count"] == 3
        assert datetime.fromisoformat(entry["last_at"]) <= datetime.now(timezone.utc)

    def test_it_logs_critical_naming_the_site(self, monkeypatch, tmp_path, caplog):
        _off(monkeypatch)
        with caplog.at_level(logging.CRITICAL, logger="core.duckdb_switch"):
            with pytest.raises(duckdb_switch.DuckDBOpenedWhileOff):
                duckdb_switch.open_file(tmp_path / "a.duckdb")
        assert "DuckDB opened while KS_DUCKDB=off" in caplog.text
        assert "test_it_logs_critical_naming_the_site" in caplog.text

    def test_the_store_is_plumbing_and_the_caller_is_the_site(self, monkeypatch, tmp_path):
        """`DuckDBStore.connect()` is on every path, so naming it would name
        nothing: the site is whoever asked for the store."""
        from core.duckdb_store import DuckDBStore

        _off(monkeypatch)
        target = tmp_path / "store.duckdb"

        async def asks_for_the_store():
            await DuckDBStore(db_path=target).connect()

        with pytest.raises(duckdb_switch.DuckDBOpenedWhileOff):
            asyncio.run(asks_for_the_store())
        assert not target.exists()
        assert list(duckdb_switch.opened()) == [f"{__name__}:asks_for_the_store"]

    def test_the_singleton_is_not_left_half_built(self, monkeypatch):
        import core.duckdb_store as store_module

        _off(monkeypatch)
        with pytest.raises(duckdb_switch.DuckDBOpenedWhileOff):
            asyncio.run(store_module.get_store())
        assert store_module._store_instance is None
        assert not Path(store_module.DB_PATH).exists()

    def test_sites_are_bounded_and_the_rest_are_other(self, monkeypatch, tmp_path):
        """Published, so bounded: a counter keyed on anything says where it
        stops. Mutation: drop the `len(_opened) >= MAX_SITES` branch."""
        _off(monkeypatch)
        extra = 5
        for i in range(duckdb_switch.MAX_SITES + extra):
            scope = {"__name__": f"fake.module{i}", "open_file": duckdb_switch.open_file,
                     "path": tmp_path / "x.duckdb"}
            exec(f"def caller{i}():\n    open_file(path)\n", scope)
            with pytest.raises(duckdb_switch.DuckDBOpenedWhileOff):
                scope[f"caller{i}"]()
        opened = duckdb_switch.opened()
        assert len(opened) == duckdb_switch.MAX_SITES + 1
        assert opened[duckdb_switch.OTHER]["count"] == extra

    def test_the_health_block_carries_no_exception_text(self, monkeypatch, tmp_path):
        _off(monkeypatch)
        with pytest.raises(duckdb_switch.DuckDBOpenedWhileOff) as raised:
            duckdb_switch.open_file(tmp_path / "a.duckdb")
        block = duckdb_switch.health_block()
        assert set(block) == {"mode", "value", "error", "opened_while_off"}
        assert block["mode"] == "off" and block["value"] == "off"
        for entry in block["opened_while_off"].values():
            assert set(entry) == {"count", "last_at"}
        assert str(raised.value) not in json.dumps(block)
        assert str(tmp_path) not in json.dumps(block)

    def test_reset_forgets_everything(self, monkeypatch, tmp_path):
        _off(monkeypatch)
        with pytest.raises(duckdb_switch.DuckDBOpenedWhileOff):
            duckdb_switch.open_file(tmp_path / "a.duckdb")
        duckdb_switch.reset()
        monkeypatch.delenv(duckdb_switch.ENV)
        assert duckdb_switch.opened() == {} and duckdb_switch.mode() == "on"


# ─── The walk: one opener ────────────────────────────────────────────────────

def connect_references(source: str) -> list:
    """Every reference to `duckdb.connect` in a module, as the dotted name of
    the function holding it (`<module>` at top level). References, not only
    calls: `opener = duckdb.connect` hands the driver on just as well. Reads
    `import duckdb [as x]`, `import a, duckdb`, `from duckdb import connect
    [as c]` and `from duckdb import *`, wherever they sit in the module."""
    tree = ast.parse(source)
    parents = {child: node for node in ast.walk(tree)
               for child in ast.iter_child_nodes(node)}
    modules, connects = set(), set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules |= {a.asname or a.name for a in node.names if a.name == "duckdb"}
        elif isinstance(node, ast.ImportFrom) and node.module == "duckdb":
            for alias in node.names:
                if alias.name == "*":
                    connects.add("connect")
                elif alias.name == "connect":
                    connects.add(alias.asname or "connect")
    found = []
    for node in ast.walk(tree):
        attribute = (isinstance(node, ast.Attribute) and node.attr == "connect"
                     and isinstance(node.value, ast.Name) and node.value.id in modules)
        by_getattr = (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                      and node.func.id == "getattr" and len(node.args) >= 2
                      and isinstance(node.args[0], ast.Name)
                      and node.args[0].id in modules
                      and isinstance(node.args[1], ast.Constant)
                      and node.args[1].value == "connect")
        bare = (isinstance(node, ast.Name) and node.id in connects
                and isinstance(node.ctx, ast.Load))
        if attribute or by_getattr or bare:
            scope, names = node, []
            while scope in parents:
                scope = parents[scope]
                if isinstance(scope, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                    names.append(scope.name)
            found.append(".".join(reversed(names)) or "<module>")
    return found


def _walk(*trees: str) -> dict:
    out: dict = {}
    for top in trees:
        for path in sorted((REPO / top).rglob("*.py")):
            rel = path.relative_to(REPO).as_posix()
            for scope in connect_references(path.read_text(encoding="utf-8")):
                out.setdefault((rel, scope), 0)
                out[(rel, scope)] += 1
    return out


# The one opener in the application.
OPENER = ("core/duckdb_switch.py", "open_file")

# Host-side tools that connect directly. They do not run in web, so the switch
# — which is about what web opens — does not cover them; what covers them is
# the hourly sha256 of the file (deploy/duckdb_silence_check.sh), which sees
# any of them that writes. Each key must still be found by the walk, and the
# walk must find nothing else.
HOST_TOOLS = {
    ("scripts/compact_duckdb.py", "phase1_export"):
        "the weekly compaction reads the file read-only to export it; its phase 2 "
        "opens through the store and is refused under off, so it never reaches "
        "phase 3. Retired at the start of the week of silence",
    ("scripts/compact_duckdb.py", "phase3_validate"):
        "validates the NEW file the compaction built, never the live one; "
        "unreachable under off (phase 2 is refused first)",
    ("scripts/weekly_report_preview.py", "live_report"):
        "an operator's preview on a laptop copy, read-only",
    ("deploy/ark_freeze.py", "freeze"):
        "the Ark is frozen from the file read-only, before the week starts",
    ("deploy/ark_freeze.py", "verify"):
        "verifies the frozen copy and an in-memory probe, never the live file",
    ("deploy/dq_history.py", "<module>"):
        "reads a backup under data/backups, read-only",
}


class TestOneOpener:
    def test_the_application_has_one_opener(self):
        """`core/`, `web/` and `bot/` run in the two containers; every DuckDB
        connection they make goes through `open_file`, or the switch has a
        hole nobody can see. Mutation: in `DuckDBStore.connect`, open with
        `duckdb.connect(str(self.db_path))` again."""
        found = _walk("core", "web", "bot")
        assert found == {OPENER: 1}, found

    def test_the_host_tools_are_exactly_the_written_exemptions(self):
        """Mutation: delete one entry, or add `duckdb.connect` to another
        script — either way the two sets differ."""
        found = _walk("scripts", "deploy")
        assert set(found) == set(HOST_TOOLS), (
            f"new: {sorted(set(found) - set(HOST_TOOLS))}, "
            f"gone: {sorted(set(HOST_TOOLS) - set(found))}")

    @pytest.mark.parametrize("source, expected", [
        ("import duckdb\nduckdb.connect('x')", ["<module>"]),
        ("import duckdb as d\ndef f():\n    return d.connect('x')", ["f"]),
        ("import glob, duckdb\nc = duckdb.connect('x')", ["<module>"]),
        ("from duckdb import connect\nconnect('x')", ["<module>"]),
        ("from duckdb import connect as c\nclass A:\n    def m(self):\n        c('x')",
         ["A.m"]),
        ("from duckdb import *\ndef f():\n    return connect", ["f"]),
        ("import duckdb\nopener = duckdb.connect", ["<module>"]),
        ("import duckdb\nopener = getattr(duckdb, 'connect')", ["<module>"]),
        ("def f():\n    import duckdb\n    return duckdb.connect(':memory:')", ["f"]),
        ("import sqlite3\nsqlite3.connect('x')", []),
        ("import duckdb\nduckdb.sql('SELECT 1')", []),
    ])
    def test_the_walk_reads_every_spelling(self, source, expected):
        assert connect_references(source) == expected

    def test_the_backup_validation_goes_through_the_opener(self):
        """The one read-only open in the store, of a copy: still DuckDB, still
        refused under off."""
        source = (REPO / "core" / "duckdb_store.py").read_text(encoding="utf-8")
        assert source.count("duckdb_switch.open_file(") == 2


# ─── Web: it starts, and it says so ──────────────────────────────────────────

class TestTheStartup:
    def _stub(self, monkeypatch):
        import web.main as main

        monkeypatch.setattr(main, "validate_config", MagicMock())
        monkeypatch.setattr(main, "init_database", MagicMock())
        monkeypatch.setattr(main, "start_scheduler", AsyncMock())
        monkeypatch.setattr(main, "_register_event_handlers", MagicMock())
        monkeypatch.setattr("core.prediction_service.get_prediction_service",
                            MagicMock(return_value=MagicMock(is_ready=True)))
        return main

    def test_off_starts_with_no_store_and_counts_the_boot(self, monkeypatch):
        """The boot sync's first act opens the file. Under off web must not
        die of the refusal — only a web that answers /api/health lets the
        canary page it. Mutation: delete the `except
        duckdb_switch.DuckDBOpenedWhileOff` branch in `startup_event` (the
        generic one asks for the store again, and the second refusal ends the
        startup)."""
        import core.duckdb_store as store_module

        main = self._stub(monkeypatch)
        monkeypatch.setenv(duckdb_switch.ENV, "off")

        async def boot_sync(**_):
            await store_module.get_store()

        monkeypatch.setattr(main, "init_and_sync", boot_sync)
        # web's logger does not propagate (core.observability), so it is read
        # where it is called.
        monkeypatch.setattr(main, "logger", MagicMock())
        asyncio.run(main.startup_event())
        main.start_scheduler.assert_awaited_once()
        said = " ".join(str(c.args[0]) for c in main.logger.critical.call_args_list)
        assert "web starts with no DuckDB store" in said
        assert list(duckdb_switch.opened()) == [f"{__name__}:boot_sync"]
        assert not Path(store_module.DB_PATH).exists()

    def test_on_is_the_path_it_always_was(self, monkeypatch):
        main = self._stub(monkeypatch)
        store = MagicMock()
        store.get_stats = AsyncMock(return_value={
            "orders": 1, "products": 1, "categories": 1, "db_size_mb": 1})
        store.backfill_sms_campaign_record = AsyncMock(return_value=False)
        monkeypatch.setattr(main, "init_and_sync", AsyncMock())
        monkeypatch.setattr(main, "get_store", AsyncMock(return_value=store))
        asyncio.run(main.startup_event())
        store.backfill_sms_campaign_record.assert_awaited_once()
        assert duckdb_switch.opened() == {}


class TestWhatWebPublishes:
    def test_on_publishes_the_mode_and_nothing_opened(self, admin_client):  # noqa: F811
        health = admin_client.get("/api/health").json()
        assert health["duckdb_switch"] == {"mode": "on", "value": None,
                                           "error": None, "opened_while_off": {}}
        assert canary.check_duckdb_switch(health) == []
        assert canary.check_duckdb_mode(health) == []
        assert canary.duckdb_silent(health) is None

    def test_off_publishes_every_site_that_reached_for_the_file(
        self, admin_client, monkeypatch,  # noqa: F811
    ):
        """Health itself asks for the store, so under off today its first
        answer already carries a site — the tripwire proving itself. Mutation:
        drop `duckdb_switch` from `health_check`'s answer."""
        _off(monkeypatch)
        health = admin_client.get("/api/health").json()
        block = health["duckdb_switch"]
        assert block["mode"] == "off"
        assert any(site.startswith("web.routes.api.health:")
                   for site in block["opened_while_off"]), block
        assert "tried to open" not in json.dumps(block)
        [(key, _line)] = canary.check_duckdb_switch(health)
        assert key == "duckdb_opened_while_off"
        assert canary.duckdb_silent(health) is False


# ─── The canary ──────────────────────────────────────────────────────────────

def _entry(count=1):
    return {"count": count,
            "last_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}


async def _probe(payload):
    def handler(request):
        return httpx.Response(200, json=payload)

    future = datetime.now(timezone.utc) + timedelta(days=60)
    cert = {"notAfter": future.strftime("%b %d %H:%M:%S %Y GMT")}
    async with _mock_transport(handler) as client:
        with patch.object(canary, "_fetch_peer_cert", return_value=cert):
            return await canary.run_canary(DASHBOARD, client=client)


class TestTheCanary:
    def test_both_keys_are_registered_conditions(self):
        for key in ("duckdb_opened_while_off", "duckdb_mode_invalid"):
            assert REGISTRY[key].kind is Kind.CONDITION, key

    def test_an_absent_or_empty_block_says_nothing(self):
        for payload in (None, {}, {"duckdb_switch": None},
                        {"duckdb_switch": {"mode": "off", "opened_while_off": {}}}):
            assert canary.check_duckdb_switch(payload) == []

    def test_an_open_under_off_pages_naming_the_sites(self):
        payload = {"duckdb_switch": {"mode": "off", "opened_while_off": {
            "web.routes.api.health:health_check": _entry(4)}}}
        [(key, line)] = canary.check_duckdb_switch(payload)
        assert key == "duckdb_opened_while_off"
        assert line.startswith(canary.DUCKDB_OPENED_LINE)
        assert "web.routes.api.health:health_check ×4" in line

    @pytest.mark.asyncio
    async def test_it_pages_critical_with_its_own_title_and_lever_first(self):
        """Mutation: make the severity `warn` in `run_canary`."""
        payload = _healthy_payload()
        payload["duckdb_switch"] = {"mode": "off", "value": "off", "error": None,
                                    "opened_while_off": {"core.x:y": _entry()}}
        result = await _probe(payload)
        assert result.severity == "critical"
        assert result.failure_keys == ["duckdb_opened_while_off"]
        assert canary._title(result) == "DuckDB opened while off"
        assert canary._what_to_do(result).startswith("KS_DUCKDB=off and web opened DuckDB")
        assert result.duckdb_silent is False

    @pytest.mark.asyncio
    async def test_its_lever_outranks_the_outage_it_causes(self):
        """Under off today web cannot work, so health is degraded too — the
        cause is the switch, and its lever comes first."""
        payload = _healthy_payload()
        payload["status"] = "degraded"
        payload["duckdb_switch"] = {"mode": "off", "value": "off", "error": None,
                                    "opened_while_off": {"core.x:y": _entry()}}
        result = await _probe(payload)
        assert "duckdb_opened_while_off" in result.failure_keys
        assert canary._what_to_do(result).startswith("KS_DUCKDB=off")

    @pytest.mark.asyncio
    async def test_a_typo_warns_and_is_not_silence(self):
        payload = _healthy_payload()
        payload["duckdb_switch"] = {"mode": "on", "value": "of",
                                    "error": "KS_DUCKDB='of' is not one of ('on', 'off')",
                                    "opened_while_off": {}}
        result = await _probe(payload)
        assert result.severity == "warn"
        assert result.failure_keys == ["duckdb_mode_invalid"]
        assert result.duckdb_silent is None

    @pytest.mark.parametrize("block, silent", [
        (None, None),
        ({"mode": "on", "opened_while_off": {}}, None),
        ({"mode": "off", "opened_while_off": {}}, True),
        ({"mode": "off", "opened_while_off": {"a:b": {"count": 1}}}, False),
    ])
    def test_the_watch_is_said_only_under_off(self, block, silent):
        payload = {} if block is None else {"duckdb_switch": block}
        assert canary.duckdb_silent(payload) is silent

    @pytest.mark.asyncio
    async def test_production_today_writes_no_watch(self):
        """`on`, the default: the probe reads the block and has nothing to say
        about the week. Mutation: make `duckdb_silent` answer for `on`."""
        payload = _healthy_payload()
        payload["duckdb_switch"] = {"mode": "on", "value": None, "error": None,
                                    "opened_while_off": {}}
        result = await _probe(payload)
        assert result.ok and result.duckdb_silent is None
        assert "duckdb_opened_while_off" not in result.unjudged_keys

    def test_the_lever_is_one_line_of_at_most_150(self):
        actions = dict(canary._ACTIONS)
        for key in ("duckdb_opened_while_off", "duckdb_mode_invalid"):
            assert "\n" not in actions[key] and len(actions[key]) <= 150, key


def _canary_job() -> ast.AsyncFunctionDef:
    tree = ast.parse((REPO / "bot" / "main.py").read_text(encoding="utf-8"))
    [job] = [n for n in ast.walk(tree)
             if isinstance(n, ast.AsyncFunctionDef) and n.name == "canary_job"]
    return job


class TestTheBotWritesTheWatch:
    def test_once_per_probe_and_only_when_web_runs_off(self):
        """Parsed: one `record_watch` under the week's key, with the probe's
        reading, the canary's gap and web's uptime, guarded by `is not None`
        and nothing else. Mutation: drop the guard (a web running `on` would
        write the row every probe)."""
        job = _canary_job()
        calls = [n for n in ast.walk(job) if isinstance(n, ast.Call)
                 and isinstance(n.func, ast.Name) and n.func.id == "record_watch"
                 and n.args and ast.unparse(n.args[0]) == "DUCKDB_SWITCH_WATCH_KEY"]
        assert len(calls) == 1
        kwargs = {k.arg: ast.unparse(k.value) for k in calls[0].keywords}
        assert kwargs == {"clean": "result.duckdb_silent",
                          "gap_s": "DUCKDB_SWITCH_WATCH_GAP_S",
                          "web_uptime_s": "result.web_uptime_s"}
        parents = {c: n for n in ast.walk(job) for c in ast.iter_child_nodes(n)}
        node, guards = calls[0], []
        while node is not job:
            node = parents[node]
            if isinstance(node, ast.If):
                guards.append(ast.unparse(node.test))
        assert guards == ["result.duckdb_silent is not None"], guards

    def test_the_watch_key_is_not_an_alert(self):
        assert canary.DUCKDB_SWITCH_WATCH_KEY.startswith("watch:")
        assert canary.DUCKDB_SWITCH_WATCH_KEY not in REGISTRY
        assert canary.DUCKDB_SWITCH_WATCH_GAP_S == canary.READ_FALLBACK_WATCH_GAP_S
