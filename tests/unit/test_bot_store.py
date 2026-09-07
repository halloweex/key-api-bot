"""The seam itself: does everything actually go through it?

`tests/unit/test_bot_database.py` says the behaviour survived the move. This
file says the *shape* did what it was for — that the engine can be replaced by
installing one object, and that nothing quietly reaches round it.

The valuable test here is `TestEverythingGoesThroughTheStore`: it installs a
store that records instead of storing, calls every function `bot/database.py`
exposes, and fails on any that never reached the port. A function left holding
its own `sqlite3` connection would keep working today and read an empty
database on the day the engine changes — which is exactly the failure a seam
exists to prevent, and exactly the one that is invisible until then.
"""
from __future__ import annotations

import inspect
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytest

from bot import database, store_sqlite
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


# ── a store that records instead of storing ──────────────────────────────────


class _Recorder:
    def __init__(self, calls: List[Tuple[str, tuple, dict]], prefix: str):
        self._calls = calls
        self._prefix = prefix

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)

        def record(*args, **kwargs):
            self._calls.append((f"{self._prefix}.{name}", args, kwargs))
            return _RETURNS.get(f"{self._prefix}.{name}")

        return record


# What each port method must hand back for the facade not to crash on it.
_RETURNS: Dict[str, Any] = {
    "access.status": {"status": "pending"},
    "access.is_approved": True,
    "access.is_frozen": False,
    "access.request": True,
    "access.approve": True,
    "access.deny": (True, False),
    "access.unfreeze": True,
    "access.reset_to_pending": (True, False),
    "access.sweep_inactive": 3,
    "access.approved": [],
    "access.pending": [],
    "access.frozen": [],
    "preferences.get": {"language": "uk"},
    "milestones.already_celebrated": False,
    "milestones.celebrate": True,
    "milestones.recent": [],
    "cache.get": None,
    "cache.sweep": 7,
    "dashboard.available": False,
    "dashboard.get": None,
    "dashboard.grant": True,
    "dashboard.set_features": True,
    "stats": {"users": 0},
}


class _RecordingStore:
    def __init__(self):
        self.calls: List[Tuple[str, tuple, dict]] = []
        self.access = _Recorder(self.calls, "access")
        self.preferences = _Recorder(self.calls, "preferences")
        self.milestones = _Recorder(self.calls, "milestones")
        self.cache = _Recorder(self.calls, "cache")
        self.dashboard = _Recorder(self.calls, "dashboard")

    def initialise(self) -> None:
        self.calls.append(("initialise", (), {}))

    def stats(self) -> Dict[str, int]:
        self.calls.append(("stats", (), {}))
        return _RETURNS["stats"]


@pytest.fixture
def recording():
    store = _RecordingStore()
    previous = use_bot_store(store)
    yield store
    use_bot_store(previous)


# ── the registry ─────────────────────────────────────────────────────────────


class TestInstallingAStore:
    def test_the_default_is_sqlite(self):
        use_bot_store(None)
        assert isinstance(get_bot_store(), store_sqlite.SqliteBotStore)

    def test_installing_one_returns_the_one_it_replaced(self):
        first, second = _RecordingStore(), _RecordingStore()
        try:
            use_bot_store(first)
            assert use_bot_store(second) is first
            assert get_bot_store() is second
        finally:
            use_bot_store(None)

    def test_none_restores_the_default(self):
        use_bot_store(_RecordingStore())
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


# ── the seam ─────────────────────────────────────────────────────────────────


class TestEverythingGoesThroughTheStore:
    """One call each, and every one must reach the port.

    A function that kept its own connection works today and reads an empty
    database on the day the engine changes.
    """

    CALLS = [
        ("init_database", (), "initialise"),
        ("get_database_stats", (), "stats"),
        ("get_user_auth_status", (1,), "access.status"),
        ("is_user_authorized", (1,), "access.is_approved"),
        ("has_pending_request", (1,), "access.status"),
        ("is_user_frozen", (1,), "access.is_frozen"),
        ("unfreeze_user", (1, 9), "access.unfreeze"),
        ("request_access", (1,), "access.request"),
        ("approve_user", (1, 9), "access.approve"),
        ("deny_user", (1, 9), "access.deny"),
        ("revoke_user", (1, 9), "access.deny"),
        ("reset_user_to_pending", (1,), "access.reset_to_pending"),
        ("update_last_activity", (1,), "access.touch"),
        ("revoke_inactive_users", (45,), "access.sweep_inactive"),
        ("get_pending_requests", (), "access.pending"),
        ("get_all_authorized_users", (), "access.approved"),
        ("get_frozen_users", (), "access.frozen"),
        ("get_user_preferences", (1,), "preferences.get"),
        ("get_user_language", (1,), "preferences.get"),
        ("save_user_preferences", (1,), "preferences.save"),
        ("update_user_preference", (1, "timezone", "UTC"), "preferences.set"),
        ("cache_get", ("k",), "cache.get"),
        ("cache_set", ("k", 1), "cache.set"),
        ("cache_delete", ("k",), "cache.delete"),
        ("cache_cleanup", (), "cache.sweep"),
        ("is_milestone_celebrated", ("m", "2026-08", 1), "milestones.already_celebrated"),
        ("mark_milestone_celebrated", ("m", "2026-08", 1, 1.0), "milestones.celebrate"),
        ("get_celebrated_milestones", (), "milestones.recent"),
        ("dashboard_access_available", (), "dashboard.available"),
        ("get_dashboard_access", (1,), "dashboard.get"),
        ("grant_dashboard_access", (1, 9), "dashboard.grant"),
        ("set_dashboard_features", (1, ["traffic"], 9), "dashboard.set_features"),
    ]

    @pytest.mark.parametrize("function,args,expected", CALLS)
    def test_it_reaches_the_port(self, recording, function, args, expected):
        getattr(database, function)(*args)
        assert expected in [name for name, _a, _k in recording.calls], (
            f"{function} did not reach {expected} — it is holding its own "
            "connection and will read an empty database when the engine moves"
        )

    def test_the_list_covers_every_public_function(self):
        """So that a function added later cannot escape the check above by
        simply not being listed."""
        covered = {name for name, _a, _e in self.CALLS}
        # `generate_cache_key` is pure and touches no store.
        exempt = {"generate_cache_key"}
        public = {
            name for name in database.__all__
            if callable(getattr(database, name, None))
        }
        assert public - covered - exempt == set()


class TestNothingReachesRoundIt:
    def test_only_the_adapter_opens_sqlite_inside_bot(self):
        """`core/bot_prefs.py` and `core/pg_bot_state.py` open the same file
        read-only from the *web* container and are not going round anything —
        they are separate readers by design. Inside `bot/`, the adapter is the
        only place a connection is allowed."""
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
