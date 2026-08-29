"""Shared fixtures, and the environment the suite is entitled to assume.

Both fixtures here are autouse guards rather than conveniences: they stop the
suite from reaching something real — the Telegram Bot API and the production
database. Markers and collection settings live in pytest.ini.

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

    for name in ("send_admin_message_http", "send_admin_photo_http"):
        monkeypatch.setattr(
            f"core.telegram_alerts.{name}", _blocked, raising=False,
        )


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


