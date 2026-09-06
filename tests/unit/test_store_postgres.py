"""The Postgres adapter, in the parts that do not need a Postgres.

The parts that do are covered by running the whole of
`tests/unit/test_bot_database.py` against both engines — 44 assertions each,
which is the only real proof that a caller cannot tell them apart. That needs
`KS_PG_DSN` and is skipped on a laptop; verified on the runtime, 87 passed and
1 skipped, the skip being a failure mode `TIMESTAMPTZ` makes impossible.

What is here instead: the pure conversions, the command-tag parsing, the engine
switch, and the two guards that stop the old copy overwriting the new writer.
That last pair is the most dangerous code in this change — an hourly full
replace pointed at a database the bot is now writing would roll back every
approval, once an hour, and look fine in between.
"""
from __future__ import annotations

import inspect
from datetime import datetime, timezone

import pytest

from bot import store_postgres
from bot.store_postgres import (
    PostgresAccessControl,
    PostgresBotStore,
    PostgresMilestones,
    PostgresPreferences,
    _as_sqlite_text,
    _row,
    _rowcount,
    _written,
)
from core.bot_store import (
    AccessControl,
    ENGINE_ENV,
    Milestones,
    Preferences,
    get_bot_store,
    use_bot_store,
)


@pytest.fixture(autouse=True)
def _no_store_left_installed():
    yield
    use_bot_store(None)


# ── the shapes a caller expects ──────────────────────────────────────────────


class TestItReturnsSqliteShapes:
    """46 call sites were left untouched on the promise that the engine can
    change without them noticing. Postgres disagrees with SQLite on exactly two
    things, and both would have shipped silently."""

    def test_a_moment_comes_back_as_the_text_sqlite_stored(self):
        """The admin screens interpolate these straight into a message, so a
        `datetime` reads as `2026-08-01 10:00:00+00:00` on somebody's phone."""
        aware = datetime(2026, 8, 1, 10, 0, tzinfo=timezone.utc)
        assert _as_sqlite_text(aware) == "2026-08-01 10:00:00"

    def test_a_moment_in_another_zone_is_rendered_in_utc(self):
        """SQLite's `CURRENT_TIMESTAMP` was UTC, so the text must be too —
        otherwise the switch moves every displayed time by an offset."""
        from datetime import timedelta

        kyiv = timezone(timedelta(hours=3))
        assert _as_sqlite_text(
            datetime(2026, 8, 1, 13, 0, tzinfo=kyiv)
        ) == "2026-08-01 10:00:00"

    def test_a_naive_moment_is_left_where_it_is(self):
        assert _as_sqlite_text(
            datetime(2026, 8, 1, 10, 0)
        ) == "2026-08-01 10:00:00"

    def test_everything_else_passes_through(self):
        for value in (None, 7, "text", 1.5, True):
            assert _as_sqlite_text(value) is value

    def test_notifications_enabled_comes_back_as_one_and_zero(self):
        """`core/bot_prefs.py` compares it against `1` in SQL, and
        `bot/handlers_legacy.py` writes `1 if enabled else 0`."""
        assert _row({"notifications_enabled": True})["notifications_enabled"] == 1
        assert _row({"notifications_enabled": False})["notifications_enabled"] == 0

    def test_a_count_that_happens_to_be_a_one_is_not_converted(self):
        """Which is why the conversion is named per column rather than
        inferred: `denial_count` is a count and this is a decision."""
        assert _row({"denial_count": 1})["denial_count"] == 1

    def test_a_missing_row_is_none_not_an_empty_dict(self):
        """`get_user_auth_status` returning `{}` would make
        `if not auth_status` behave the same and `auth_status['status']`
        explode one line later."""
        assert _row(None) is None


class TestReadingAsyncpgsCommandTag:
    """asyncpg hands back `'UPDATE 3'`, not a cursor with a `rowcount`."""

    @pytest.mark.parametrize("tag,expected", [
        ("UPDATE 1", 1), ("UPDATE 0", 0), ("UPDATE 17", 17),
        ("INSERT 0 1", 1), ("DELETE 4", 4),
    ])
    def test_it_reads_the_count(self, tag, expected):
        assert _rowcount(tag) == expected

    @pytest.mark.parametrize("tag", ["", "UPDATE", "nonsense", None])
    def test_anything_it_cannot_read_is_zero_written(self, tag):
        """Fail closed: an unparseable tag must not be reported as a
        successful approval."""
        assert _rowcount(tag) == 0
        assert _written(tag) is False

    def test_one_row_is_written_and_none_is_not(self):
        assert _written("UPDATE 1") is True
        assert _written("UPDATE 0") is False


# ── the shape of the adapter ─────────────────────────────────────────────────


class TestItImplementsThePort:
    """Checked without a database, because `isinstance` needs an instance and
    an instance needs a connection."""

    @pytest.mark.parametrize("implementation,protocol", [
        (PostgresAccessControl, AccessControl),
        (PostgresPreferences, Preferences),
        (PostgresMilestones, Milestones),
    ])
    def test_every_method_matches_the_port_signature(self, implementation, protocol):
        for name, declared in inspect.getmembers(
            protocol, predicate=inspect.isfunction,
        ):
            if name.startswith("_"):
                continue
            actual = getattr(implementation, name, None)
            assert actual is not None, f"{implementation.__name__}.{name} missing"
            expected = list(inspect.signature(declared).parameters)
            assert list(inspect.signature(actual).parameters) == expected, name

    def test_the_cache_is_not_reimplemented(self):
        """Revision 0009 left the cache out of Postgres, so this engine keeps
        using the local one: a network round trip to fetch the thing that
        exists to avoid a network round trip is the wrong shape twice over."""
        source = inspect.getsource(PostgresBotStore.__init__)
        assert "SqliteCache()" in source

    def test_the_pool_is_opened_inside_a_coroutine(self):
        """`asyncpg.create_pool()` returns a `Pool` — awaitable, not a
        coroutine — and `run_coroutine_threadsafe` raises `TypeError` on it.
        Found by hitting it."""
        assert inspect.iscoroutinefunction(PostgresBotStore._open_pool)


class TestItRefusesToGuess:
    def test_no_dsn_is_a_refusal_not_a_localhost(self, monkeypatch):
        """`core/pg.py` refuses for the same reason: guessing a database is how
        you migrate the wrong one."""
        monkeypatch.delenv("KS_PG_DSN", raising=False)
        with pytest.raises(RuntimeError, match="KS_PG_DSN"):
            PostgresBotStore()


# ── the switch ───────────────────────────────────────────────────────────────


class TestChoosingTheEngine:
    def test_unset_is_sqlite(self, monkeypatch):
        from bot.store_sqlite import SqliteBotStore

        monkeypatch.delenv(ENGINE_ENV, raising=False)
        use_bot_store(None)
        assert isinstance(get_bot_store(), SqliteBotStore)

    def test_sqlite_is_sqlite(self, monkeypatch):
        from bot.store_sqlite import SqliteBotStore

        monkeypatch.setenv(ENGINE_ENV, "SQLite")
        use_bot_store(None)
        assert isinstance(get_bot_store(), SqliteBotStore)

    def test_a_typo_stops_the_bot_rather_than_falling_back(self, monkeypatch):
        """The one variable that decides where the approval list lives. A
        misspelling that quietly pointed at the old copy would look like the
        switch worked."""
        monkeypatch.setenv(ENGINE_ENV, "postgress")
        use_bot_store(None)
        with pytest.raises(RuntimeError, match="not a store this code knows"):
            get_bot_store()

    def test_postgres_is_only_built_when_asked_for(self, monkeypatch):
        """Lazy: importing the port must not open a connection."""
        monkeypatch.setenv(ENGINE_ENV, "sqlite")
        use_bot_store(None)
        source = inspect.getsource(get_bot_store)
        assert "from bot.store_postgres import PostgresBotStore" in source
        assert source.index("engine ==") < source.index("PostgresBotStore")


# ── the guards ───────────────────────────────────────────────────────────────


class TestTheOldCopyStandsDownWhenTheBotTakesOver:
    """The most dangerous code in this change.

    `replicate_bot_state` is a full replace from `data/bot.db`. Once the bot
    writes Postgres, running it would roll back every approval made since the
    switch — and only once an hour, so it would look like the switch worked in
    between.
    """

    @pytest.mark.asyncio
    async def test_the_copy_refuses_under_the_postgres_engine(self, monkeypatch):
        from core.pg_bot_state import replicate_bot_state

        monkeypatch.setenv(ENGINE_ENV, "postgres")
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(
                "core.mirror_reconciliation.configured", lambda: True,
            )
            result = await replicate_bot_state()
        assert "skipped" in result
        assert "the bot owns these tables" in result["skipped"]

    @pytest.mark.asyncio
    async def test_the_comparison_stands_down_too(self, monkeypatch):
        """`bot.db` becomes a frozen artefact, so every approval since the
        switch would read as a discrepancy — a check reporting a difference it
        created is worse than no check."""
        from core.mirror_reconciliation import reconcile_bot_state

        monkeypatch.setenv(ENGINE_ENV, "postgres")
        assert await reconcile_bot_state() == []

    @pytest.mark.asyncio
    async def test_both_still_run_under_sqlite(self, monkeypatch, tmp_path):
        from core.pg_bot_state import replicate_bot_state

        monkeypatch.setenv(ENGINE_ENV, "sqlite")
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(
                "core.mirror_reconciliation.configured", lambda: True,
            )
            result = await replicate_bot_state(tmp_path / "absent.db")
        # Skipped for the *file*, not for the engine — which is the point.
        assert "does not exist" in result["skipped"]

    def test_the_guard_names_the_same_variable_the_switch_reads(self):
        """Two spellings of the environment variable would be a switch that
        turns the bot over and leaves the copy running."""
        from core import pg_bot_state

        assert "ENGINE_ENV" in inspect.getsource(pg_bot_state.replicate_bot_state)
        assert store_postgres.__doc__ is not None
