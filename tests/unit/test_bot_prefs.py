"""Reading the bot's language choice from the container that does not own it.

The weekly report is built where DuckDB lives and the language is recorded
where the settings screen runs — two containers, one bind-mounted file. Every
way that read can fail has the same answer: the default language, and a report
that still goes out.
"""
from __future__ import annotations

import sqlite3

import pytest

from core.bot_prefs import group_by_language, read_user_languages
from core.i18n import DEFAULT_LANGUAGE, RU, UK


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
