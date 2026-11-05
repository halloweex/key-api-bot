"""Shared fixtures, and the environment the suite is entitled to assume.

The fixtures here are autouse guards rather than conveniences: they stop the
suite from reaching something real — the Telegram Bot API, the production
database, and the write-chain latch that sits beside it. Markers and collection
settings live in pytest.ini.

THE ENVIRONMENT BLOCK BELOW RUNS BEFORE ANY TEST MODULE IS IMPORTED

`core/config.py` calls `load_dotenv()` at import and reads every setting from
the environment, and `web/routes/auth.py` **raises at import time** when no
session signing key is present. On a developer's machine `.env` supplies one,
so the suite passed for as long as anyone had checked. In a clean checkout —
which is what CI has, because `.env` is gitignored — five integration modules
died during collection and the whole run exited 2.

That was found by the first CI run ever executed on this repository, on the
pull request that added CI. It is worth recording because the pre-flight that
missed it looked convincing: running the suite under `env -i` proved nothing,
since the values come from a *file*, not from the environment.

Set here rather than in the workflow for two reasons. A `BOT_TOKEN:` line in
`ci.yml` is a fake secret in a public repository, and it would fix CI while
leaving `git clone && pytest` broken for the next person. This makes the suite
self-sufficient wherever it runs.

`os.environ` is assigned, not `setdefault`-ed, and that is deliberate:
`load_dotenv()` does not override variables already present, so these win over
`.env` even on a machine that has one. A test must never sign a session with
the production key or hold the real bot token — the autouse guard below stops
the requests, and this stops the credentials from being there at all.
"""
import os

# Credentials of exactly the shape the real ones have, and unmistakably not
# them. Every one of these is required by something that raises on absence:
# `web/routes/auth.py` at import, and `core/keycrm.py:119` in the constructor —
# sixteen tests build a real client with mocked transport and died on the key
# they never use.
os.environ["DASHBOARD_SECRET_KEY"] = "test-signing-key-not-a-real-secret"
os.environ["BOT_TOKEN"] = "123456:test-bot-token-not-a-real-secret"
os.environ["KEYCRM_API_KEY"] = "test-keycrm-key-not-a-real-secret"

# The dev kill switch is a property of a *machine*, not of the suite, and a
# developer laptop sets it in `.env`. Left in place it silences the transports
# before they are reached, so nine tests that assert an alert was delivered
# passed on CI and failed on the machine that had switched alerts off — the
# opposite of what a kill switch should cost. Neutralised here rather than in
# each test: what stops the suite reaching Telegram is the autouse fixture
# below, never this variable. A test that wants suppression sets it with
# monkeypatch.
#
# Assigned "0" rather than deleted, for the reason the block above turns on:
# `load_dotenv()` only declines to overwrite a name that is *present*, so
# popping this one just clears the way for `.env` to put it back.
os.environ["KS_ALERTS_DISABLED"] = "0"

# Same shape, different reason: every message signs itself with the instance
# name, which without this is the developer's hostname and makes any assertion
# about a rendered message machine-dependent.
os.environ["KS_INSTANCE"] = "test-instance"

# The Gate persists its decision state under data/ by default; two thousand
# tests taking turns rewriting a real file would be both slow and a way for
# one run to poison the next. Empty means disabled; tests that exercise
# persistence construct AlertGate(state_path=tmp_path/...) explicitly.
os.environ["KS_ALERT_GATE_STATE_DIR"] = ""

import pytest  # noqa: E402  — must follow the environment block above


@pytest.fixture(autouse=True)
def _no_telegram_from_tests(monkeypatch):
    """Nothing in the suite may reach Telegram. Ever.

    `.env` holds a real BOT_TOKEN, and `bot.main.send_admin_message` falls back
    to the HTTP Bot API whenever no Application is running — which is every
    test process. So a test that deliberately fails warehouse validation, as
    the cell-guard tests must, sent a real alert to real admins:

        ⚠️ Warehouse validation failed — full retry scheduled (attempt 1/3).
        rows=2→2 (match=True), cells: 1 missing/0 orphaned, revenue=1000.00

    Two orders and a thousand hryvnia — unmistakably a fixture, delivered to a
    phone at midday. Individual tests stubbing `_send_warehouse_alert` is not
    protection; it only covers the paths someone remembered.

    Autouse and function-scoped, so a test that wants to exercise delivery can
    still patch the same name itself and see its own mock.
    """
    async def _blocked(*args, **kwargs):
        return 0

    for name in ("send_admin_message_http", "send_admin_photo_http",
                 "send_rich_message_http"):
        monkeypatch.setattr(
            f"core.telegram_alerts.{name}", _blocked, raising=False,
        )
    # The bot's document sender behind `POST /api/me/send-file`: the same
    # token, to whichever id the test signed in as.
    monkeypatch.setattr(
        "core.telegram_user_files.send_user_document_http", _blocked, raising=False,
    )


@pytest.fixture(autouse=True)
def _fresh_alert_gate():
    """The Gate is a module singleton with per-bucket cooldown state; without
    this, one test's alert buys thirty minutes of silence in the next."""
    from core.alerting import reset_gate

    reset_gate()
    yield
    reset_gate()


@pytest.fixture(autouse=True)
def _never_the_production_database(monkeypatch, tmp_path):
    """No test may open the real analytics database. Ever.

    `DuckDBStore()` with no argument opens DB_PATH, and `get_store()` — which
    every route reaches through Depends — builds exactly that. So a single
    `TestClient(app)` request was enough: tests/integration/test_internal_sales_type.py
    opened the 8.7 GB production file on every run, and the only reason it did
    no damage is that this machine holds a copy rather than the live one. On a
    host where data/ is the real volume, running the suite would have opened
    the database the bot and the web container are using.

    Redirecting DB_PATH is only possible because __init__ resolves it at call
    time; it used to be a default argument, frozen at import.

    Function-scoped against tmp_path, so each test gets an empty database and
    none of them can see another's writes. A test that wants a specific file
    still passes db_path= explicitly and is unaffected.
    """
    monkeypatch.setattr(
        "core.duckdb_store.DB_PATH", tmp_path / "test-analytics.duckdb",
    )
    # get_store() caches a singleton, and one built before this fixture ran
    # would keep the old path for the rest of the session.
    monkeypatch.setattr("core.duckdb_store._store_instance", None, raising=False)


# `_no_write_chain_flag_from_outside`, which cleared every chain's KS_WRITE_*
# before each test, stood here until step 14's S14-11: a flag inherited from a
# developer's `.env` decided where a chain's writers wrote. Nothing reads one
# since (`tests/unit/test_write_flags_are_not_read.py`).


@pytest.fixture(autouse=True)
def _duckdb_switch_on_and_forgotten(monkeypatch):
    """Every test starts with KS_DUCKDB unset and the switch's cache and counts
    empty (core/duckdb_switch.py), and leaves them so.

    A developer's shell or `.env` carrying `KS_DUCKDB=off` would refuse every
    store the suite opens, and a test that switched it off must not hand the
    refusal to the next one. Tests that mean `off` set it after this ran.
    `reset()` also forgets that a test configured the switch as web's
    (`configure_modes(web=True)`), which is `off` whatever the variable says
    since step 14's S14-00."""
    from core import duckdb_switch

    monkeypatch.delenv(duckdb_switch.ENV, raising=False)
    duckdb_switch.reset()
    yield
    duckdb_switch.reset()


@pytest.fixture
def duckdb_off(monkeypatch, tmp_path):
    """`KS_DUCKDB=off`, read the way web reads it, with the store's directory
    and file redirected to a directory that does not exist yet. Returns that
    directory, so a test can say what it expects to find there — under off,
    usually nothing at all.

    `_never_the_production_database` above redirects the file and not its
    directory, and `DuckDBStore.connect()` used to make that directory before
    the switch refused; with the real `data/` already there, a test could not
    have seen it. Opt-in: a test under `on` keeps the autouse arrangement."""
    from core import duckdb_switch

    data = tmp_path / "data"
    monkeypatch.setattr("core.duckdb_store.DB_DIR", data)
    monkeypatch.setattr("core.duckdb_store.DB_PATH", data / "analytics.duckdb")
    monkeypatch.setenv(duckdb_switch.ENV, "off")
    duckdb_switch.configure_mode()
    return data


@pytest.fixture(autouse=True)
def _never_the_real_chain_latch(monkeypatch, tmp_path):
    """No test may latch a write chain in the real data directory.

    The latch marker beside the database decides, for the life of the machine,
    that a chain writes Postgres and no `KS_WRITE_*` may move it back
    (`core/chain_latch.py`). A test that wrote one into `data/` would hand that
    decision to whatever runs next on the developer's machine — and the suite
    reaches the writers through the repository methods, every one of which
    writes Postgres and takes the latch on its first write.

    Redirected and reset per test, `_never_the_production_database`'s
    arrangement: each test starts with nothing latched, and no test can see
    another's marker.
    """
    from core import chain_latch

    monkeypatch.setattr(chain_latch, "MARKER_DIR", tmp_path / "write-chain-owners")
    monkeypatch.setattr(chain_latch, "_latched", None, raising=False)
    # The report ledgers' spool (chains 11a/11b) is a decision of the same
    # kind — a spooled week reads as sent — so it is redirected the same way.
    from core import report_ledger

    monkeypatch.setattr(report_ledger, "SPOOL_DIR", tmp_path / "report-ledger-pending")


@pytest.fixture(autouse=True)
def _never_the_real_data_directory(monkeypatch, tmp_path):
    """No test may sample, or leave a heartbeat in, the real data directory.

    The disk watchdog walks the data directory and writes
    `health/watchdog_last_sample` into it, which host cron reads as proof the
    watchdog is alive. Until step 14's S14-01 it found that directory as the
    DuckDB store's parent, so `_never_the_production_database`'s redirect of
    the store kept every test out of `data/`; it reads `core.paths.DATA_DIR`
    now, so the redirect is made here, at the one name the scheduler reads.
    `tmp_path` itself, the directory a test's own store usually sits in, so a
    test that builds its store there and expects the heartbeat beside it
    still finds it.

    The scheduler's name and not `core.paths.DATA_DIR`: the modules that read
    the directory bind it at import, so patching `core.paths` would bind this
    test's tmp_path into whichever of them was first imported inside it, for
    the rest of the session."""
    monkeypatch.setattr("core.scheduler.DATA_DIR", tmp_path)


@pytest.fixture(autouse=True)
def _a_fresh_sync_service(monkeypatch):
    """Every test starts with no `SyncService` singleton.

    Its step states floor their ages at the instance's creation, so one made
    by an earlier test reads, half an hour later, as a tick that never
    reached the order step — `orders_sync_failing`, CRITICAL, on any test
    that judges a live /api/health with the canary. Since S14-00 chain 3 is
    always on Postgres, so the canary judges that step in every test; the
    store-backed gate, which runs the suite for over an hour, failed
    `test_duckdb_fatal_reconnect` on exactly this. Only reset when the module
    is loaded: before that there is no singleton to leak."""
    import sys

    module = sys.modules.get("core.sync_service")
    if module is not None:
        monkeypatch.setattr(module, "_sync_service", None)


# `_a_fresh_warehouse_writer` stood here until step 14's S14-15: it reset the
# per-process caches of `core/warehouse_cutover.py` (the mode, the verdict on
# its preconditions, what `settle_writer` found, the way back's hold) and
# redirected its UTM re-parse marker into `tmp_path`, so one test's start
# never became the next one's. The module is deleted, and nothing in the
# application writes `data/warehouse-utm-reparsed-at` any more.


# `route_chains_to_duckdb` and `chains_on_duckdb` stood here until step 14's
# S14-11: they patched a chain's `writes_postgres()`/`reads_postgres()`/
# `mode()` back to DuckDB for one test or one block. Those answers went with
# the flags, and with them the DuckDB branches the helpers reached.
