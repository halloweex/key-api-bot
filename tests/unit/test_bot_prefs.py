"""Reading the bot's language choice from the container that does not own it.

The weekly report is built where DuckDB lives and the language is recorded
where the settings screen runs — two containers, one bind-mounted file. Every
way that read can fail has the same answer: the default language, and a report
that still goes out.
"""
from __future__ import annotations

import sqlite3

import pytest

from core.bot_prefs import (
    default_language_for,
    group_by_language,
    read_approved_user_ids,
    read_user_languages,
)
from core.i18n import DEFAULT_LANGUAGE, EN, RU, UK


@pytest.fixture
def bot_db(tmp_path):
    """A stand-in for the bot's SQLite, shaped like bot/database.py builds it."""
    path = tmp_path / "bot.db"
    with sqlite3.connect(path) as conn:
        conn.execute("""
            CREATE TABLE user_preferences (
                user_id INTEGER PRIMARY KEY,
                timezone TEXT DEFAULT 'Europe/Kyiv',
                language TEXT DEFAULT 'en'
            )
        """)
        conn.executemany(
            "INSERT INTO user_preferences (user_id, language) VALUES (?, ?)",
            [(1, "uk"), (2, "ru"), (3, None), (4, "de")],
        )
    return path


class TestReadUserLanguages:
    def test_it_reads_what_each_user_chose(self, bot_db):
        assert read_user_languages([1, 2], bot_db) == {1: UK, 2: RU}

    def test_a_user_with_no_choice_gets_the_default(self, bot_db):
        assert read_user_languages([3], bot_db) == {3: DEFAULT_LANGUAGE}

    def test_a_language_no_longer_supported_falls_back(self, bot_db):
        """Rows outlive code. A dropped language must not reach the renderer."""
        assert read_user_languages([4], bot_db) == {4: DEFAULT_LANGUAGE}

    def test_a_user_the_bot_has_never_seen_still_gets_an_entry(self, bot_db):
        """Callers should never have to decide what a missing key means."""
        assert read_user_languages([1, 99], bot_db) == {1: UK, 99: DEFAULT_LANGUAGE}

    def test_no_ids_asks_nothing(self, bot_db):
        assert read_user_languages([], bot_db) == {}

    def test_duplicate_ids_are_asked_about_once(self, bot_db):
        assert read_user_languages([1, 1, 1], bot_db) == {1: UK}


class TestDegradation:
    def test_a_missing_database_is_not_an_error(self, tmp_path):
        """The web container can start before the bot has ever run."""
        assert read_user_languages([1, 2], tmp_path / "nope.db") == {
            1: DEFAULT_LANGUAGE, 2: DEFAULT_LANGUAGE,
        }

    def test_a_database_without_the_column_is_not_an_error(self, tmp_path):
        """An ordinary state mid-deploy: web restarted, bot has not yet."""
        path = tmp_path / "old.db"
        with sqlite3.connect(path) as conn:
            conn.execute("CREATE TABLE user_preferences (user_id INTEGER PRIMARY KEY)")
            conn.execute("INSERT INTO user_preferences VALUES (1)")
        assert read_user_languages([1], path) == {1: DEFAULT_LANGUAGE}

    def test_a_file_that_is_not_a_database_is_not_an_error(self, tmp_path):
        path = tmp_path / "junk.db"
        path.write_bytes(b"not sqlite, not even close")
        assert read_user_languages([1], path) == {1: DEFAULT_LANGUAGE}

    def test_it_never_writes_to_the_bot_s_database(self, bot_db):
        """Opened mode=ro, so it cannot create, migrate or lock anything."""
        before = bot_db.read_bytes()
        read_user_languages([1, 2, 3], bot_db)
        assert bot_db.read_bytes() == before
        assert not (bot_db.parent / "bot.db-journal").exists()


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
        languages = read_user_languages([1, 2, 3], bot_db, defaults=defaults)
        assert languages[1] == UK   # stored 'uk' wins over the English default
        assert languages[2] == RU
        assert languages[3] == EN   # nothing stored, so the default stands


class TestApprovedUsers:
    @pytest.fixture
    def users_db(self, tmp_path):
        path = tmp_path / "bot.db"
        with sqlite3.connect(path) as conn:
            conn.execute("CREATE TABLE authorized_users "
                         "(user_id INTEGER PRIMARY KEY, status TEXT)")
            conn.executemany("INSERT INTO authorized_users VALUES (?, ?)", [
                (1, "approved"), (2, "approved"), (3, "denied"),
                (4, "pending"), (5, "approved"),
            ])
            conn.execute("CREATE TABLE user_preferences "
                         "(user_id INTEGER PRIMARY KEY, notifications_enabled INTEGER)")
            conn.executemany("INSERT INTO user_preferences VALUES (?, ?)",
                             [(1, 1), (5, 0)])
        return path

    def test_only_approved_users_are_written_to(self, users_db):
        assert sorted(read_approved_user_ids(users_db)) == [1, 2]

    def test_muting_notifications_is_respected(self, users_db):
        """A toggle some messages ignore is worse than no toggle at all."""
        assert 5 not in read_approved_user_ids(users_db)

    def test_a_user_who_never_opened_settings_is_included(self, users_db):
        """No preferences row at all — the column defaults to on."""
        assert 2 in read_approved_user_ids(users_db)

    def test_an_unreadable_database_yields_nobody(self, tmp_path):
        """The caller falls back to the admins rather than to silence."""
        assert read_approved_user_ids(tmp_path / "nope.db") == []


class TestGrouping:
    def test_it_buckets_so_each_language_is_rendered_once(self, bot_db, monkeypatch):
        monkeypatch.setattr("core.bot_prefs.BOT_DB_PATH", bot_db)
        buckets = group_by_language([1, 2, 3])
        assert buckets == {UK: [1], RU: [2], DEFAULT_LANGUAGE: [3]}

    def test_admins_sharing_a_language_are_one_bucket(self, tmp_path, monkeypatch):
        """The common case: one render, one caption, two recipients."""
        path = tmp_path / "bot.db"
        with sqlite3.connect(path) as conn:
            conn.execute("CREATE TABLE user_preferences "
                         "(user_id INTEGER PRIMARY KEY, language TEXT)")
            conn.executemany("INSERT INTO user_preferences VALUES (?, ?)",
                             [(1, "uk"), (2, "uk")])
        monkeypatch.setattr("core.bot_prefs.BOT_DB_PATH", path)
        assert group_by_language([1, 2]) == {UK: [1, 2]}
