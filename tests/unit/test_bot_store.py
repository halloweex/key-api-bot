"""The seam itself: the engine is one object, and nothing reaches round it.

`tests/unit/test_bot_database.py` runs every function `bot/database.py`
exposes against both engines and checks what each one answers. A function
left holding its own `sqlite3` connection would keep working on SQLite and
answer from an empty file under Postgres, so it is caught there, by its
result. This file says the *shape* did what it was for — that the engine can
be replaced by installing one object, and that nothing quietly reaches round
it.
"""
from __future__ import annotations

import inspect
import re
from pathlib import Path
from types import SimpleNamespace

import pytest

from bot import store_sqlite
from core.bot_store import (
    AccessControl,
    BotStore,
    DashboardTabs,
    Milestones,
    Preferences,
    ShortLivedCache,
    get_bot_store,
    use_bot_store,
)

REPO = Path(__file__).resolve().parents[2]


# ── the registry ─────────────────────────────────────────────────────────────


class TestInstallingAStore:
    def test_the_default_is_sqlite(self):
        use_bot_store(None)
        assert isinstance(get_bot_store(), store_sqlite.SqliteBotStore)

    def test_installing_one_returns_the_one_it_replaced(self):
        first, second = SimpleNamespace(), SimpleNamespace()
        try:
            use_bot_store(first)
            assert use_bot_store(second) is first
            assert get_bot_store() is second
        finally:
            use_bot_store(None)

    def test_none_restores_the_default(self):
        use_bot_store(SimpleNamespace())
        use_bot_store(None)
        assert isinstance(get_bot_store(), store_sqlite.SqliteBotStore)

    def test_it_is_lazy(self):
        """Importing the port must not create a database file. A test that
        never touches the bot's state should leave no trace of one."""
        source = inspect.getsource(get_bot_store)
        assert "from bot.store_sqlite import SqliteBotStore" in source


class TestTheAdapterIsTheShapeThePortDeclares:
    @pytest.mark.parametrize("attribute,protocol", [
        ("access", AccessControl),
        ("preferences", Preferences),
        ("milestones", Milestones),
        ("cache", ShortLivedCache),
        ("dashboard", DashboardTabs),
    ])
    def test_each_aggregate_satisfies_its_protocol(self, attribute, protocol):
        store = store_sqlite.SqliteBotStore()
        assert isinstance(getattr(store, attribute), protocol)

    def test_the_store_satisfies_the_bundle(self):
        assert isinstance(store_sqlite.SqliteBotStore(), BotStore)

    def test_no_port_method_is_missing_from_the_adapter(self):
        """`runtime_checkable` only checks that the names exist, so the
        signatures are compared here. A method the adapter spells differently
        passes `isinstance` and fails at the call."""
        store = store_sqlite.SqliteBotStore()
        for attribute, protocol in (
            ("access", AccessControl), ("preferences", Preferences),
            ("milestones", Milestones), ("cache", ShortLivedCache),
            ("dashboard", DashboardTabs),
        ):
            implementation = getattr(store, attribute)
            for name, declared in inspect.getmembers(
                protocol, predicate=inspect.isfunction,
            ):
                if name.startswith("_"):
                    continue
                actual = getattr(implementation, name, None)
                assert actual is not None, f"{attribute}.{name} is missing"
                expected_args = list(
                    inspect.signature(declared).parameters
                )[1:]   # drop `self`
                assert list(
                    inspect.signature(actual).parameters
                ) == expected_args, f"{attribute}.{name}"


# ── nothing reaches round the seam ───────────────────────────────────────────


class TestNothingReachesRoundIt:
    def test_only_the_adapter_opens_sqlite_inside_bot(self):
        """Inside `bot/`, the adapter is the only place a connection is
        allowed. (`core/bot_prefs.py` and the copy in `core/pg_bot_state.py`
        opened the same file read-only from the *web* container, as separate
        readers by design; the first reads through the port since 2026-09-06,
        and the second was deleted on 2026-11-19.)"""
        offenders = [
            path.name for path in (REPO / "bot").rglob("*.py")
            if path.name != "store_sqlite.py"
            and re.search(r"^\s*import sqlite3", path.read_text(encoding="utf-8"), re.M)
        ]
        assert offenders == [], offenders

    def test_the_facade_holds_no_sql(self):
        text = (REPO / "bot" / "database.py").read_text(encoding="utf-8")
        for keyword in ("SELECT ", "INSERT ", "UPDATE ", "DELETE ", "CREATE "):
            assert keyword not in text, keyword


class TestThePortIsSynchronous:
    def test_no_protocol_method_is_a_coroutine(self):
        """The decision, pinned. 38 of 46 call sites would take an `await`
        without complaint; the eight that would not include
        `bot/handlers_legacy._lang()`, which CLAUDE.md keeps callable at the
        point of use rather than threaded through forty signatures. See
        `core/bot_store.py` for the cost this accepts."""
        for protocol in (AccessControl, Preferences, Milestones, ShortLivedCache):
            for name, function in inspect.getmembers(
                protocol, predicate=inspect.isfunction,
            ):
                assert not inspect.iscoroutinefunction(function), (
                    f"{protocol.__name__}.{name}"
                )
