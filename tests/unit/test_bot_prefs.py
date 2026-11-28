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
from tests.helpers.access_lists import bot_store_on_postgres
from tests.integration.pg_tab_seed import needs_postgres

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


@needs_postgres
class TestTheReadersAskTheStoreTheBotWrites:
    """The defect: the readers opened the bot's SQLite file, every functional
    test of them seeded that file, and all of them passed while production
    read a copy frozen on 2026-08-27. Here the bot's store is Postgres, as
    production runs it, and the SQLite file where the adapter would look
    holds the frozen day's contrary answers, so a reader that opened it says
    them. (The dashboard's door to the same row is held over Postgres in
    `tests/integration/test_auth_identity.py::TestMeTouchesOnlyTheSessionsOwnRow`.)"""

    REVOKED, APPROVED, MUTED = 7_100_000_001, 7_100_000_002, 7_100_000_003

    @pytest.mark.asyncio
    async def test_the_weekly_reports_questions_follow_the_live_store(
            self, monkeypatch, tmp_path):
        from bot.store_sqlite import SqliteBotStore

        async with bot_store_on_postgres(monkeypatch, tmp_path) as live:
            frozen = SqliteBotStore()
            frozen.initialise()
            for uid in (self.REVOKED, self.MUTED):
                frozen.access.request(uid, f"user{uid}", "First", "Last")
                frozen.access.approve(uid, ADMIN)
            frozen.preferences.set(self.REVOKED, "language", "en")
            frozen.preferences.set(self.MUTED, "notifications_enabled", 1)

            # Since the freeze, through the port, as the bot writes.
            for uid in (self.REVOKED, self.APPROVED, self.MUTED):
                live.access.request(uid, f"user{uid}", "First", "Last")
                live.access.approve(uid, ADMIN)
            live.access.deny(self.REVOKED, ADMIN)
            live.preferences.set(self.REVOKED, "language", "ru")
            live.preferences.set(self.APPROVED, "language", "en")
            live.preferences.set(self.MUTED, "notifications_enabled", 0)

            assert read_approved_user_ids() == [self.APPROVED]
            assert read_user_languages([self.REVOKED, self.APPROVED]) == {
                self.REVOKED: RU, self.APPROVED: EN}
