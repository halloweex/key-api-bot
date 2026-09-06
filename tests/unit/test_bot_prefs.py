"""Reading the bot's approvals and language choices from the web container.

The weekly report is built where DuckDB lives; who reads it and in what
language is decided where the bot runs. Since 2026-08-27 that is Postgres, and
a reader that still opened the SQLite file was reading a frozen copy: people
revoked since kept receiving the report, people approved since never did.
These pin that both questions go through the bot store port — the same engine
the bot writes — and that every way the read can fail has the same answer:
the default, and a report that still goes out.
"""
from __future__ import annotations

import pathlib
import sqlite3

import pytest

from core.bot_prefs import (
    default_language_for,
    group_by_language,
    read_approved_user_ids,
    read_language,
    read_user_languages,
    write_language,
)
from core.i18n import DEFAULT_LANGUAGE, EN, RU, UK

ADMIN = 999


@pytest.fixture
def bot_store(tmp_path, monkeypatch):
    """The real SQLite adapter on a throwaway file, installed as the active store.

    Seeded through the port, not with hand-written SQL: what is being tested
    is that the readers ask the store the bot writes, so the fixture writes
    the way the bot does.
    """
    from bot.store_sqlite import SqliteBotStore

    monkeypatch.setattr("bot.store_sqlite.DB_PATH", tmp_path / "bot.db")
    store = SqliteBotStore()
    store.initialise()
    monkeypatch.setattr("core.bot_store._store", store)
    return store


@pytest.fixture
def bot_db(bot_store):
    """Four users: two choices, one who never chose, one with a dropped language."""
    bot_store.preferences.set(1, "language", "uk")
    bot_store.preferences.set(2, "language", "ru")
    bot_store.preferences.set(4, "language", "de")
    return bot_store


class _Unreachable:
    """A store whose every aggregate raises — Postgres down, pool closed."""

    def __getattr__(self, name):
        raise RuntimeError("bot store unreachable")


class TestReadUserLanguages:
    def test_it_reads_what_each_user_chose(self, bot_db):
        assert read_user_languages([1, 2]) == {1: UK, 2: RU}

    def test_a_user_with_no_choice_gets_the_default(self, bot_db):
        assert read_user_languages([3]) == {3: DEFAULT_LANGUAGE}

    def test_a_language_no_longer_supported_falls_back(self, bot_db):
        """Rows outlive code. A dropped language must not reach the renderer."""
        assert read_user_languages([4]) == {4: DEFAULT_LANGUAGE}

    def test_a_user_the_bot_has_never_seen_still_gets_an_entry(self, bot_db):
        """Callers should never have to decide what a missing key means."""
        assert read_user_languages([1, 99]) == {1: UK, 99: DEFAULT_LANGUAGE}

    def test_no_ids_asks_nothing(self, bot_db):
        assert read_user_languages([]) == {}

    def test_duplicate_ids_are_asked_about_once(self, bot_db):
        assert read_user_languages([1, 1, 1]) == {1: UK}


class TestDegradation:
    def test_an_unreachable_store_is_not_an_error(self, monkeypatch):
        """Postgres down at 09:30 must cost the languages, not the report."""
        monkeypatch.setattr("core.bot_store._store", _Unreachable())
        assert read_user_languages([1, 2]) == {
            1: DEFAULT_LANGUAGE, 2: DEFAULT_LANGUAGE,
        }

    def test_a_missing_database_is_not_an_error(self, tmp_path, monkeypatch):
        """The web container can start before the bot has ever run."""
        from bot.store_sqlite import SqliteBotStore

        monkeypatch.setattr("bot.store_sqlite.DB_PATH", tmp_path / "nope.db")
        monkeypatch.setattr("core.bot_store._store", SqliteBotStore())
        assert read_user_languages([1, 2]) == {
            1: DEFAULT_LANGUAGE, 2: DEFAULT_LANGUAGE,
        }

    def test_a_database_without_the_column_is_not_an_error(self, tmp_path, monkeypatch):
        """An ordinary state mid-deploy: web restarted, bot has not yet."""
        from bot.store_sqlite import SqliteBotStore

        path = tmp_path / "old.db"
        with sqlite3.connect(path) as conn:
            conn.execute("CREATE TABLE user_preferences (user_id INTEGER PRIMARY KEY)")
            conn.execute("INSERT INTO user_preferences VALUES (1)")
        monkeypatch.setattr("bot.store_sqlite.DB_PATH", path)
        monkeypatch.setattr("core.bot_store._store", SqliteBotStore())
        assert read_user_languages([1]) == {1: DEFAULT_LANGUAGE}

    def test_a_file_that_is_not_a_database_is_not_an_error(self, tmp_path, monkeypatch):
        from bot.store_sqlite import SqliteBotStore

        path = tmp_path / "junk.db"
        path.write_bytes(b"not sqlite, not even close")
        monkeypatch.setattr("bot.store_sqlite.DB_PATH", path)
        monkeypatch.setattr("core.bot_store._store", SqliteBotStore())
        assert read_user_languages([1]) == {1: DEFAULT_LANGUAGE}

    def test_reading_does_not_modify_the_bot_s_database(self, bot_db, tmp_path):
        path = tmp_path / "bot.db"
        before = path.read_bytes()
        read_user_languages([1, 2, 3])
        read_approved_user_ids()
        assert path.read_bytes() == before
        assert not (path.parent / "bot.db-journal").exists()


class TestDefaultLanguage:
    def test_staff_get_ukrainian_and_admins_get_english(self):
        assert default_language_for(500, admin_ids=[1, 2]) == UK
        assert default_language_for(1, admin_ids=[1, 2]) == EN

    def test_admin_ids_may_arrive_as_strings(self):
        """`.env` parsing has produced both over the life of this project."""
        assert default_language_for(1, admin_ids=["1", "2"]) == EN

    def test_a_stored_choice_beats_the_default(self, bot_db):
        """The whole point: choose once, and it sticks."""
        defaults = {1: EN, 2: EN, 3: EN}
        languages = read_user_languages([1, 2, 3], defaults=defaults)
        assert languages[1] == UK   # stored 'uk' wins over the English default
        assert languages[2] == RU
        assert languages[3] == EN   # nothing stored, so the default stands


class TestApprovedUsers:
    @pytest.fixture
    def users(self, bot_store):
        """Approved 1, 2, 5; denied 3; pending 4 — written the way the bot does."""
        for uid in (1, 2, 3, 4, 5):
            bot_store.access.request(uid, f"user{uid}", "First", "Last")
        for uid in (1, 2, 5):
            bot_store.access.approve(uid, ADMIN)
        bot_store.access.deny(3, ADMIN)
        bot_store.preferences.set(1, "notifications_enabled", 1)
        bot_store.preferences.set(5, "notifications_enabled", 0)
        return bot_store

    def test_only_approved_users_are_written_to(self, users):
        assert sorted(read_approved_user_ids()) == [1, 2]

    def test_muting_notifications_is_respected(self, users):
        """A toggle some messages ignore is worse than no toggle at all."""
        assert 5 not in read_approved_user_ids()

    def test_a_user_who_never_opened_settings_is_included(self, users):
        """No preferences row at all — the column defaults to on."""
        assert 2 in read_approved_user_ids()

    def test_a_revocation_reaches_the_next_report(self, users):
        """The defect this replaces: revoked on 28.08, still on the list on
        every Monday since, because the list was read from a frozen file."""
        users.access.deny(2, ADMIN)   # what revoke_user does
        assert 2 not in read_approved_user_ids()

    def test_an_approval_reaches_the_next_report(self, users):
        users.access.request(6, "newcomer", "N", "C")
        users.access.approve(6, ADMIN)
        assert 6 in read_approved_user_ids()

    def test_an_unreachable_store_yields_nobody(self, monkeypatch):
        """The caller falls back to the admins rather than to silence."""
        monkeypatch.setattr("core.bot_store._store", _Unreachable())
        assert read_approved_user_ids() == []


class TestGrouping:
    def test_it_buckets_so_each_language_is_rendered_once(self, bot_db):
        buckets = group_by_language([1, 2, 3])
        assert buckets == {UK: [1], RU: [2], DEFAULT_LANGUAGE: [3]}

    def test_admins_sharing_a_language_are_one_bucket(self, bot_store):
        """The common case: one render, one caption, two recipients."""
        bot_store.preferences.set(1, "language", "uk")
        bot_store.preferences.set(2, "language", "uk")
        assert group_by_language([1, 2]) == {UK: [1, 2]}


class TestOneHomeForTheChoice:
    """The dashboard has stored this in the wrong place twice.

    First in DuckDB's own `user_preferences`, a table the bot and the weekly
    report never read. Then, after the bot moved to Postgres, in the SQLite
    file the bot no longer reads. Whichever interface you set it in, the other
    never saw it. These pin the single home: the store the bot writes.
    """

    def test_a_choice_written_here_is_read_back_here(self, bot_store):
        assert write_language(7, "uk") == UK
        assert read_language(7) == UK

    def test_it_reaches_the_readers_the_weekly_report_uses(self, bot_store):
        """The end-to-end shape: set it, then ask the question the report asks."""
        write_language(1, "ru")
        assert read_user_languages([1]) == {1: RU}
        assert group_by_language([1, 2]) == {RU: [1], DEFAULT_LANGUAGE: [2]}

    def test_it_is_what_the_bot_itself_reads(self, bot_store):
        """Same row the bot's settings screen shows."""
        write_language(1, "ru")
        assert bot_store.preferences.get(1)["language"] == RU

    def test_a_user_with_no_row_yet_gets_one(self, bot_store):
        """Anyone who has never opened the bot's settings has no row at all."""
        assert write_language(404, "en") == EN
        assert read_language(404) == EN

    def test_an_unsupported_language_is_normalised_not_stored_raw(self, bot_store):
        """Rows outlive code, so nothing unsupported should get in either."""
        assert write_language(8, "de") == DEFAULT_LANGUAGE
        assert read_language(8) == DEFAULT_LANGUAGE

    def test_an_unreachable_store_raises_instead_of_pretending(self, monkeypatch):
        """Readers here fall back on purpose. A writer must not: a settings
        screen that reports success and stores nothing is the original bug."""
        monkeypatch.setattr("core.bot_store._store", _Unreachable())
        with pytest.raises(RuntimeError):
            write_language(1, "uk")


class TestTheReadersAskTheStoreTheBotWrites:
    """Structural, because the defect was structural: the readers opened a
    file, every functional test of them seeded that file, and all of them
    passed while production read a copy frozen on 2026-08-27."""

    SOURCE = pathlib.Path(__file__).resolve().parents[2] / "core" / "bot_prefs.py"

    def test_bot_prefs_does_not_open_sqlite(self):
        source = self.SOURCE.read_text()
        assert "import sqlite3" not in source
        assert "sqlite3.connect" not in source
        assert "get_bot_store" in source

    def test_me_endpoint_does_not_reach_for_duckdb(self):
        source = (
            self.SOURCE.parent.parent / "web" / "routes" / "api" / "me.py"
        ).read_text()

        assert "FROM user_preferences" not in source, (
            "the endpoint is querying a preferences table directly again"
        )
        assert "INSERT INTO user_preferences" not in source
        assert "get_store" not in source, "preferences do not live in DuckDB"
        assert "write_language" in source and "read_language" in source
