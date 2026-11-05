"""Step 14's S14-11: no `KS_WRITE_*` is read, and nothing that judged one
remains.

Until S14-11 each of the thirteen write chains read its flag
(`env_writes_postgres`, `writes_postgres`, `mode`, `unmet_precondition`, the
registry's `chain_modes`), and `/api/health` published what it found, judged
by the canary as `write_chain_flag_invalid`, `write_chain_flag_mismatch` and
`write_chain_precondition_unmet`. S14-00 made every answer a constant —
every chain writes Postgres — and S14-11 deleted the answers with the flags.
What this holds:

- rule 3: junk in any of the thirteen, or in all at once, imports web, runs
  web's start (`configure_modes()`) and answers `/api/health` 200 —
  in this process and in a fresh interpreter;
- the `write_chains` block is `{chain: {sync_step?, pending?}}` for every
  registered chain, whatever the flags hold;
- no module under `core/`, `web/`, `bot/` or `scripts/` reads a chain's
  `WRITE_ENV` from the environment;
- the two CRITICALs that were gated on the chains' `mode` — chain 3's
  `orders_sync_failing` and chain 4's `buyer_sync_stalled_chain` — are
  judged on the step alone: a health stub without `sync_step` holds chain
  3's key unjudged, and a step with the old error pages with no `mode` in
  sight.
"""
from __future__ import annotations

import ast
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]

# The thirteen flags, written out: a chain registered later is not one of
# them, and the registry is held to this list below.
FLAGS = (
    "KS_WRITE_INVENTORY", "KS_WRITE_EXPENSES", "KS_WRITE_GOALS",
    "KS_WRITE_EXPENSE_TYPES", "KS_WRITE_BUYERS", "KS_WRITE_ORDERS",
    "KS_WRITE_MANAGERS", "KS_WRITE_CATALOGUE", "KS_WRITE_FORECAST",
    "KS_WRITE_DQ_JOURNAL", "KS_WRITE_WATCHDOGS", "KS_WRITE_WEEKLY_LEDGER",
    "KS_WRITE_TRAFFIC_LEDGER",
)
JUNK = "junk-s14-11"
# What an entry may carry since S14-11.
ENTRY_FIELDS = {"sync_step", "pending"}


def test_the_thirteen_are_the_registrys_names():
    """`WRITE_ENV` stays a name on every chain module (revisions 0034 and
    0035 name them, and an image from before step 14 reads them); nothing
    reads its value."""
    from core.write_chains import WRITE_CHAINS

    assert sorted(c.WRITE_ENV for c in WRITE_CHAINS) == sorted(FLAGS)


def test_no_chain_answers_where_it_writes():
    """The answers went with the flags. Mutation: put `writes_postgres` or
    `mode` back on a chain."""
    from core import chain_latch, write_chains

    gone = ("env_writes_postgres", "writes_postgres", "mode", "reads_postgres",
            "reads_the_chain", "unmet_precondition", "unmet_reasons", "preflight",
            "duckdb_orders_frozen", "LOCKOUT_REVISION", "_routes_postgres")
    for chain in write_chains.WRITE_CHAINS:
        left = [name for name in gone if hasattr(chain, name)]
        assert not left, (chain.__name__, left)
    for name in ("chain_modes", "is_shadow", "compared_in_shadow", "mismatched_chains",
                 "stood_down_tables", "stood_down_tables_checked",
                 "stood_down_sync_keys", "stood_down_sync_keys_checked",
                 "stood_down_among"):
        assert not hasattr(write_chains, name), name
    assert not hasattr(chain_latch, "lockout_unmet")


# ─── Rule 3: junk starts web ─────────────────────────────────────────────────


@pytest.fixture
def web_with(monkeypatch):
    """Set the flags, run web's start, and ask `/api/health`."""
    def ask(values: dict):
        from fastapi.testclient import TestClient

        from core.runtime_modes import configure_modes
        from web.main import app
        from web.routes.api import health

        for name in FLAGS:
            monkeypatch.delenv(name, raising=False)
        for name, value in values.items():
            monkeypatch.setenv(name, value)
        configure_modes()
        configure_modes()   # the scheduler's call, after the startup's
        for value in vars(health).values():
            if isinstance(value, dict) and "expires_at" in value:
                monkeypatch.setitem(value, "expires_at", 0)
        return TestClient(app).get("/api/health")

    return ask


def _assert_the_block(response) -> None:
    from core.write_chains import WRITE_CHAINS, chain_name

    assert response.status_code == 200, response.text
    block = response.json()["write_chains"]
    assert set(block) == {chain_name(c) for c in WRITE_CHAINS}
    for name, entry in block.items():
        assert set(entry) <= ENTRY_FIELDS, (name, sorted(entry))
        assert JUNK not in str(entry), name


@pytest.mark.parametrize("flag", FLAGS)
def test_junk_in_one_flag_starts_web_and_health_answers(web_with, flag):
    """Mutation: read the flag again anywhere `configure_modes` or
    `/api/health` reaches."""
    _assert_the_block(web_with({flag: JUNK}))


@pytest.mark.parametrize("value", [JUNK, "", "duckdb", "postgres"])
def test_any_value_in_all_thirteen_at_once(web_with, value):
    _assert_the_block(web_with({flag: value for flag in FLAGS}))


def test_a_fresh_interpreter_imports_and_starts_with_junk_in_all_thirteen(tmp_path):
    """What is read at import time is read there. The child gets the suite's
    test signing secrets and runs from `tmp_path`: under `-c` `load_dotenv()`
    walks up from the cwd, and from a worktree that reaches the main
    checkout's real `.env`; the child asserts it found none."""
    modules = sorted({c.__name__ for c in __import__(
        "core.write_chains", fromlist=["WRITE_CHAINS"]).WRITE_CHAINS})
    code = (
        "import importlib, sys\n"
        f"sys.path.insert(0, {str(ROOT)!r})\n"
        "from dotenv import find_dotenv\n"
        "found = find_dotenv(usecwd=True)\n"
        "assert not found, 'a .env above the cwd would be loaded: ' + found\n"
        "import web.main, bot.canary, scripts.backfill_gender\n"
        f"for m in {modules!r}:\n"
        "    importlib.import_module(m)\n"
        "from core.runtime_modes import configure_modes\n"
        "configure_modes()\n"
        "configure_modes()\n"
        "print('ok')\n"
    )
    env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": str(tmp_path),
           "TZ": "UTC",
           "DASHBOARD_SECRET_KEY": os.environ["DASHBOARD_SECRET_KEY"],
           "BOT_TOKEN": os.environ["BOT_TOKEN"],
           **{flag: JUNK for flag in FLAGS}}
    run = subprocess.run([sys.executable, "-I", "-c", code], cwd=tmp_path, env=env,
                         capture_output=True, text=True, timeout=180)
    assert run.returncode == 0, run.stderr[-3000:]
    assert run.stdout.strip().endswith("ok")


# ─── Nothing reads one ───────────────────────────────────────────────────────


def _os_bindings(tree: ast.AST) -> tuple:
    """Names bound to `os.getenv` and `os.environ` by `from os import ...`,
    aliases included: a bare `getenv(X)` reads the environment too."""
    getenvs, environs = set(), set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "os":
            for alias in node.names:
                bound = alias.asname or alias.name
                if alias.name == "getenv":
                    getenvs.add(bound)
                elif alias.name == "environ":
                    environs.add(bound)
    return getenvs, environs


def _reads_of_the_environment(tree: ast.AST):
    """Every `os.getenv(X)`, `os.environ.get(X)` and `os.environ[X]` — and the
    same through `from os import getenv, environ` under any alias — with X."""
    getenvs, environs = _os_bindings(tree)

    def is_environ(node) -> bool:
        return (ast.unparse(node).endswith("environ")
                or (isinstance(node, ast.Name) and node.id in environs))

    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and node.args:
            func = node.func
            if isinstance(func, ast.Attribute) and func.attr in ("getenv", "get"):
                if func.attr == "getenv" or is_environ(func.value):
                    yield node.args[0]
            elif isinstance(func, ast.Name) and func.id in getenvs:
                yield node.args[0]
        elif isinstance(node, ast.Subscript) and is_environ(node.value):
            yield node.slice


def _names_a_flag(text: str) -> bool:
    return "WRITE_ENV" in text or "KS_WRITE_" in text or any(f in text for f in FLAGS)


def _bound_to_a_flag(tree: ast.AST, name: str) -> bool:
    """Whether `name` is assigned, anywhere in the module, from an expression
    that names a chain's flag — `name = chain.WRITE_ENV; os.getenv(name)`."""
    for node in ast.walk(tree):
        targets, value = (), None
        if isinstance(node, ast.Assign):
            targets, value = node.targets, node.value
        elif isinstance(node, (ast.AnnAssign, ast.NamedExpr)) and node.value is not None:
            targets, value = (node.target,), node.value
        elif isinstance(node, (ast.For, ast.comprehension)):
            targets, value = (node.target,), node.iter
        for target in targets:
            bound = {n.id for n in ast.walk(target) if isinstance(n, ast.Name)}
            if name in bound and _names_a_flag(ast.unparse(value)):
                return True
    return False


def test_nothing_reads_a_write_flag():
    """Static: no read of the environment names a chain's flag — by
    `WRITE_ENV` (bare or as an attribute), by its literal, or through a local
    name bound to either. Mutation: an `os.getenv(WRITE_ENV)` left in a chain
    module, an `os.environ.get("KS_WRITE_BUYERS")` anywhere, `from os import
    getenv; getenv(WRITE_ENV)`, or `name = chain.WRITE_ENV; os.getenv(name)`."""
    found = []
    for top in ("core", "web", "bot", "scripts"):
        for path in sorted((ROOT / top).rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for arg in _reads_of_the_environment(tree):
                text = ast.unparse(arg)
                via = {n.id for n in ast.walk(arg) if isinstance(n, ast.Name)}
                if _names_a_flag(text) or any(_bound_to_a_flag(tree, n) for n in via):
                    found.append(f"{path.relative_to(ROOT)}:{arg.lineno}: {text}")
    assert not found, found


@pytest.mark.parametrize("source", [
    "from os import getenv\nWRITE_ENV = 'KS_WRITE_BUYERS'\ngetenv(WRITE_ENV)\n",
    "from os import getenv as ge\nge('KS_WRITE_ORDERS')\n",
    "from os import environ\nenviron.get('KS_WRITE_ORDERS')\n",
    "from os import environ as env\nenv['KS_WRITE_ORDERS']\n",
    "import os\ndef f(chain):\n    name = chain.WRITE_ENV\n    return os.getenv(name)\n",
    "import os\nfor chain in CHAINS:\n    os.environ.get(chain.WRITE_ENV)\n",
])
def test_the_walk_sees_every_spelling(source):
    """The review's two spellings, and their neighbours: each is a read the
    walk above must find. Mutation: drop the `from os import` bindings, or
    the local-name resolution."""
    tree = ast.parse(source)
    hits = [arg for arg in _reads_of_the_environment(tree)
            if _names_a_flag(ast.unparse(arg)) or any(
                _bound_to_a_flag(tree, n.id) for n in ast.walk(arg)
                if isinstance(n, ast.Name))]
    assert hits, source


def test_the_walk_does_not_see_an_unrelated_read():
    tree = ast.parse("import os\nname = 'KS_PG_DSN'\nos.getenv(name)\n")
    assert not [arg for arg in _reads_of_the_environment(tree)
                if any(_bound_to_a_flag(tree, n.id) for n in ast.walk(arg)
                       if isinstance(n, ast.Name))]


# ─── The two CRITICALs judged on the step alone ──────────────────────────────


def _payload(**blocks) -> dict:
    return {"read_fallbacks": {}, "read_fallback_mode": {},
            "duckdb_switch": {"mode": "off", "opened_while_off": {}},
            "backups": {}, "buyer_sync": {"last_ok_age_s": 60}, **blocks}


class TestTheOrderStep:
    def test_a_stub_without_sync_step_holds_the_key_unjudged(self):
        """The page stands through a probe that cannot judge it: an entry
        with no step, an older web's entry whose `mode` said postgres, and
        one whose `mode` said duckdb (S14-11: `mode` decides nothing).
        Mutation: gate the hold on `mode` again."""
        from bot import canary

        for entry in ({}, {"mode": "postgres"}, {"mode": "duckdb"}, {"sync_step": None}):
            payload = _payload(write_chains={canary.ORDERS_CHAIN: entry})
            assert "orders_sync_failing" in canary.unjudged_keys(payload), entry
            assert canary.check_orders_sync_chain(payload) == [], entry

    @pytest.mark.parametrize("mode", [None, "postgres", "duckdb"])
    def test_the_old_error_pages_whatever_mode_says(self, mode):
        """Three failures in a row — the order step's error before S14-11 —
        page CRITICAL with no `mode` in the entry, and with an older web's
        `mode` beside it. Mutation: skip the entry unless `mode` is
        postgres."""
        from bot import canary

        entry = {"sync_step": {"consecutive_failures": canary.ORDERS_SYNC_FAILURES,
                               "last_ok_age_s": 3600, "last_attempt_age_s": 60,
                               "last_error_class": "TimeoutError"}}
        if mode is not None:
            entry["mode"] = mode
        payload = _payload(write_chains={canary.ORDERS_CHAIN: entry})
        assert "orders_sync_failing" not in canary.unjudged_keys(payload)
        [(key, message)] = canary.check_orders_sync_chain(payload)
        assert key == "orders_sync_failing" and "TimeoutError" in message


class TestTheBuyersStep:
    def test_a_stall_pages_with_no_chain_entry(self):
        """Chain 4's CRITICAL is judged on `buyer_sync` alone. Mutation:
        require a `write_chains` entry, or its `mode`, again."""
        from bot import canary

        stale = canary.BUYER_SYNC_CHAIN_STALE_S + 600
        payload = _payload(buyer_sync={"last_ok_age_s": stale, "watermark_age_s": stale,
                                       "last_attempt_age_s": 60,
                                       "consecutive_failures": 0})
        payload.pop("write_chains", None)
        [(key, _)] = canary.check_buyer_sync_chain(payload)
        assert key == "buyer_sync_stalled_chain"
        assert "buyer_sync_stalled_chain" not in canary.unjudged_keys(payload)

    def test_no_buyer_sync_block_holds_both_keys(self):
        from bot import canary

        payload = _payload(buyer_sync=None)
        held = canary.unjudged_keys(payload)
        assert {"buyer_sync_stalled", "buyer_sync_stalled_chain"} <= set(held)


def test_the_three_flag_conditions_are_gone():
    """Rule 8: REGISTRY, emitters, REMEDIATION and the canary's actions
    together. Mutation: leave any one of them behind."""
    from bot import canary
    from core import alerting, data_quality

    gone = ("write_chain_flag_invalid", "write_chain_flag_mismatch",
            "write_chain_precondition_unmet")
    registry = alerting.REGISTRY
    for key in gone:
        assert key not in registry, key
        assert key not in data_quality.HUMAN_CHECK_NAMES, key
        assert not any(prefix == key for prefix, _ in data_quality.REMEDIATION), key
    actions = getattr(canary, "_ACTIONS")
    assert not any(prefix in gone for prefix, _ in actions)
    for name in ("check_write_chains", "check_write_chain_latch",
                 "check_write_chain_precondition"):
        assert not hasattr(canary, name), name
    # And nothing under core/, web/ or bot/ emits one.
    for top in ("core", "web", "bot"):
        for path in (ROOT / top).rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if isinstance(node, ast.Constant) and node.value in gone:
                    pytest.fail(f"{path.relative_to(ROOT)}:{node.lineno} names {node.value}")
