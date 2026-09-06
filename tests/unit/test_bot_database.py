"""What `bot/database.py` actually does — pinned before anything moves.

This module holds the bot's access control: who may use it, who was denied,
who is frozen and for how long. It had **no tests at all** until this file, and
it is about to be put behind a port so the engine can change underneath it.
Moving 717 lines of approve/deny/freeze logic with nothing watching is how a
refactor locks people out or lets a denied person back in.

So these are characterisation tests first and specification second: they say
what the code does today, including the parts that are surprising. Where a
behaviour is a defect it is marked and fixed rather than pinned — there is
exactly one, and it is the same timezone trap that `core/pg_bot_state.py` had
to close from the other side.

The surprise worth reading before the rest: **`revoke_user` is `deny_user`**,
so revoking somebody's access increments their denial count and can freeze
them out for thirty days. That is the current behaviour and it is pinned here,
not endorsed.
"""
from __future__ import annotations

import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone

import pytest

from bot import database, store_sqlite
from core.bot_store import use_bot_store

# The columns whose values these tests set by hand. SQLite takes the text
# straight; Postgres needs a moment, and handing it a string is the mistake the
# adapter exists to make impossible for everybody else.
_TIMESTAMP_COLUMNS = frozenset({"requested_at", "reviewed_at", "last_activity"})

DSN = os.getenv("KS_PG_DSN", "").strip()


class _SqliteHarness:
    """Reach past the store and look at what SQLite actually holds."""

    engine = "sqlite"

    def __init__(self, path: Path):
        self.path = path

    def row(self, user_id):
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        try:
            row = conn.execute(
                "SELECT * FROM authorized_users WHERE user_id = ?", (user_id,)
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def set(self, user_id, **values):
        conn = sqlite3.connect(self.path)
        try:
            assignments = ", ".join(f"{k} = ?" for k in values)
            conn.execute(
                f"UPDATE authorized_users SET {assignments} WHERE user_id = ?",
                (*values.values(), user_id),
            )
            conn.commit()
        finally:
            conn.close()


class _PostgresHarness:
    """The same two reaches, against `app.authorized_users`.

    `row` returns SQLite's shapes — text timestamps, 1/0 — because the whole
    claim under test is that a caller cannot tell the two engines apart.
    """

    engine = "postgres"

    def __init__(self, store):
        self._store = store

    def _run(self, coro):
        return self._store._loop.run(coro)

    def row(self, user_id):
        from bot.store_postgres import _row as to_sqlite_shapes

        async def fetch():
            async with self._store._pool.acquire() as conn:
                return await conn.fetchrow(
                    "SELECT * FROM app.authorized_users WHERE user_id = $1",
                    user_id,
                )

        return to_sqlite_shapes(self._run(fetch()))

    def set(self, user_id, **values):
        prepared = {}
        for key, value in values.items():
            if key in _TIMESTAMP_COLUMNS and isinstance(value, str):
                try:
                    value = datetime.strptime(
                        value, "%Y-%m-%d %H:%M:%S",
                    ).replace(tzinfo=timezone.utc)
                except ValueError:
                    # A value Postgres cannot hold. The column type is what
                    # makes that impossible, which is the point of the skip on
                    # the one test that needs it.
                    raise
            prepared[key] = value

        async def write():
            assignments = ", ".join(
                f"{k} = ${i}" for i, k in enumerate(prepared, start=1)
            )
            async with self._store._pool.acquire() as conn:
                await conn.execute(
                    f"UPDATE app.authorized_users SET {assignments} "
                    f"WHERE user_id = ${len(prepared) + 1}",
                    *prepared.values(), user_id,
                )

        self._run(write())

    def truncate(self):
        async def wipe():
            async with self._store._pool.acquire() as conn:
                await conn.execute(
                    "TRUNCATE app.authorized_users, app.user_preferences, "
                    "app.celebrated_milestones, app.report_history"
                )

        self._run(wipe())


@pytest.fixture(params=["sqlite", "postgres"], autouse=True)
def db(request, tmp_path, monkeypatch):
    """The same tests, against whichever engine is available.

    This is the file's whole reason for existing twice. 46 call sites were left
    untouched by the port on the promise that the engine can change without
    them noticing, and the only way to check a promise like that is to run the
    same assertions against both implementations.

    The Postgres half needs `KS_PG_DSN` and is skipped without one, so a
    laptop runs 44 tests and a runtime with a database runs 88. That asymmetry
    is deliberate and is what `pytest -m external` does elsewhere in this
    suite; the difference is that here the skip is silent on purpose — a
    developer should not have to run Postgres to work on the bot.
    """
    use_bot_store(None)

    if request.param == "sqlite":
        path = tmp_path / "bot.db"
        monkeypatch.setattr(store_sqlite, "DB_PATH", path)
        # The cache lives in SQLite under both engines, so it needs this path
        # whichever branch runs.
        database.init_database()
        yield _SqliteHarness(path)
        use_bot_store(None)
        return

    if not DSN:
        pytest.skip("KS_PG_DSN is not set — the Postgres half needs a database")

    from bot.store_postgres import PostgresBotStore

    monkeypatch.setattr(store_sqlite, "DB_PATH", tmp_path / "bot.db")
    store = PostgresBotStore(DSN)
    use_bot_store(store)
    # The real path, not a shortcut: `initialise` is also what creates the
    # local cache table this engine still needs.
    database.init_database()
    harness = _PostgresHarness(store)
    harness.truncate()
    try:
        yield harness
    finally:
        harness.truncate()
        use_bot_store(None)
        store.close()


def _row(db, user_id):
    return db.row(user_id)


def _set(db, user_id, **values):
    db.set(user_id, **values)


@pytest.fixture
def far_from_utc(monkeypatch):
    """Run the body in a timezone that is not UTC.

    Without this the clock tests prove nothing on a UTC host — they would pass
    against the broken code and fail only on somebody's laptop, which is the
    trap this repository has been caught by from the other direction
    (`time.monotonic()` seeded with 0.0, invisible here and immediate on CI).
    Production runs `TZ=Europe/Kyiv`; so does this.
    """
    monkeypatch.setenv("TZ", "Europe/Kyiv")
    time.tzset()
    yield
    monkeypatch.undo()
    time.tzset()


def _utc_ago(**delta) -> str:
    """A timestamp in the format SQLite writes, which is UTC."""
    moment = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(**delta)
    return moment.strftime("%Y-%m-%d %H:%M:%S")


# ── asking for access ────────────────────────────────────────────────────────


class TestRequestingAccess:
    def test_a_new_person_gets_a_pending_row(self, db):
        assert database.request_access(1, "ann", "Ann", "A") is True
        row = _row(db, 1)
        assert row["status"] == database.STATUS_PENDING
        assert row["username"] == "ann"
        assert row["denial_count"] == 0

    def test_asking_twice_changes_nothing(self, db):
        database.request_access(1, "ann")
        assert database.request_access(1, "someone-else") is False
        assert _row(db, 1)["username"] == "ann"

    def test_a_denied_person_cannot_ask_again_through_this_door(self, db):
        """`request_access` refuses anyone who already has a row, whatever it
        says. Coming back is `reset_user_to_pending`'s job, and that one knows
        about the freeze."""
        database.request_access(1)
        database.deny_user(1, admin_id=9)
        assert database.request_access(1) is False
        assert _row(db, 1)["status"] == database.STATUS_DENIED


class TestApproval:
    def test_approving_clears_the_denial_count(self, db):
        database.request_access(1)
        database.deny_user(1, 9)
        assert _row(db, 1)["denial_count"] == 1
        assert database.approve_user(1, 9) is True
        row = _row(db, 1)
        assert row["status"] == database.STATUS_APPROVED
        assert row["denial_count"] == 0
        assert row["reviewed_by"] == 9

    def test_approving_somebody_who_never_asked_does_nothing(self, db):
        assert database.approve_user(404, 9) is False

    def test_is_user_authorized_reads_the_status_and_nothing_else(self, db):
        assert database.is_user_authorized(1) is False
        database.request_access(1)
        assert database.is_user_authorized(1) is False   # pending
        database.approve_user(1, 9)
        assert database.is_user_authorized(1) is True


class TestDenialAndTheFreeze:
    def test_each_denial_counts(self, db):
        database.request_access(1)
        for expected in (1, 2, 3, 4):
            success, frozen = database.deny_user(1, 9)
            assert (success, frozen) == (True, False)
            assert _row(db, 1)["denial_count"] == expected

    def test_the_fifth_denial_freezes(self, db):
        database.request_access(1)
        for _ in range(database.MAX_DENIAL_COUNT - 1):
            database.deny_user(1, 9)
        success, frozen = database.deny_user(1, 9)
        assert (success, frozen) == (True, True)
        assert _row(db, 1)["status"] == database.STATUS_FROZEN

    def test_revoking_access_is_a_denial_and_counts_as_one(self, db):
        """Pinned, not endorsed. `revoke_user` calls `deny_user`, so taking
        access away from an approved person increments their denial count —
        five revocations freeze them out for thirty days."""
        database.request_access(1)
        database.approve_user(1, 9)
        assert database.revoke_user(1, 9) is True
        row = _row(db, 1)
        assert row["status"] == database.STATUS_DENIED
        assert row["denial_count"] == 1

    def test_a_frozen_person_cannot_come_back_on_their_own(self, db):
        database.request_access(1)
        for _ in range(database.MAX_DENIAL_COUNT):
            database.deny_user(1, 9)
        assert database.reset_user_to_pending(1) == (False, True)
        assert _row(db, 1)["status"] == database.STATUS_FROZEN

    def test_a_denied_person_can(self, db):
        database.request_access(1)
        database.deny_user(1, 9)
        assert database.reset_user_to_pending(1) == (True, False)
        row = _row(db, 1)
        assert row["status"] == database.STATUS_PENDING
        assert row["reviewed_at"] is None
        assert row["reviewed_by"] is None
        # The count survives a reset: coming back does not wipe the history.
        assert row["denial_count"] == 1

    def test_an_admin_can_unfreeze_and_that_does_wipe_the_count(self, db):
        database.request_access(1)
        for _ in range(database.MAX_DENIAL_COUNT):
            database.deny_user(1, 9)
        assert database.unfreeze_user(1, 9) is True
        row = _row(db, 1)
        assert row["status"] == database.STATUS_DENIED
        assert row["denial_count"] == 0

    def test_unfreezing_somebody_who_is_not_frozen_does_nothing(self, db):
        database.request_access(1)
        assert database.unfreeze_user(1, 9) is False


class TestTheFreezeExpires:
    def test_it_is_still_on_before_thirty_days(self, db):
        database.request_access(1)
        for _ in range(database.MAX_DENIAL_COUNT):
            database.deny_user(1, 9)
        _set(db, 1, reviewed_at=_utc_ago(days=database.FREEZE_DURATION_DAYS - 1))
        assert database.is_user_frozen(1) is True

    def test_it_lifts_itself_after_thirty_days(self, db):
        database.request_access(1)
        for _ in range(database.MAX_DENIAL_COUNT):
            database.deny_user(1, 9)
        _set(db, 1, reviewed_at=_utc_ago(days=database.FREEZE_DURATION_DAYS + 1))

        assert database.is_user_frozen(1) is False
        row = _row(db, 1)
        assert row["status"] == database.STATUS_DENIED
        assert row["denial_count"] == 0

    def test_the_clock_is_utc_on_both_sides(self, db, far_from_utc):
        """The defect this file found. SQLite writes `CURRENT_TIMESTAMP` in
        UTC; the container runs `TZ=Europe/Kyiv`, so `datetime.now()` was three
        hours ahead of every value it was compared against and the freeze
        expired three hours early.

        Three hours out of thirty days is immaterial today. It stops being
        immaterial when the engine moves: an aware `timestamptz` compared
        against a naive local `now()` does not skew, it raises.
        """
        database.request_access(1)
        for _ in range(database.MAX_DENIAL_COUNT):
            database.deny_user(1, 9)
        # Exactly at the boundary, measured in UTC. Read as Kyiv time this is
        # 30 days and 3 hours old and the freeze would already have lifted.
        _set(db, 1, reviewed_at=_utc_ago(days=database.FREEZE_DURATION_DAYS,
                                         minutes=-30))
        assert database.is_user_frozen(1) is True

    def test_an_unparseable_timestamp_leaves_the_freeze_on(self, db):
        """Fail closed: a value nobody can read is not a licence to come back.

        SQLite only, and that is the finding rather than the limitation: the
        column is `TEXT` there and `TIMESTAMPTZ` in Postgres, so this failure
        mode does not exist on the other engine. The type removes it.
        """
        if db.engine != "sqlite":
            pytest.skip("TIMESTAMPTZ cannot hold an unparseable value")
        database.request_access(1)
        for _ in range(database.MAX_DENIAL_COUNT):
            database.deny_user(1, 9)
        _set(db, 1, reviewed_at="not a date")
        assert database.is_user_frozen(1) is True


class TestTheInactivitySweep:
    def test_it_revokes_somebody_who_stopped_using_the_bot(self, db):
        database.request_access(1)
        database.approve_user(1, 9)
        _set(db, 1, last_activity=_utc_ago(days=60))
        assert database.revoke_inactive_users(days=45) == 1
        assert _row(db, 1)["status"] == database.STATUS_DENIED

    def test_it_leaves_an_active_person_alone(self, db):
        database.request_access(1)
        database.approve_user(1, 9)
        _set(db, 1, last_activity=_utc_ago(days=1))
        assert database.revoke_inactive_users(days=45) == 0
        assert _row(db, 1)["status"] == database.STATUS_APPROVED

    def test_somebody_who_never_did_anything_is_judged_by_their_approval(self, db):
        database.request_access(1)
        database.approve_user(1, 9)
        _set(db, 1, last_activity=None, reviewed_at=_utc_ago(days=60))
        assert database.revoke_inactive_users(days=45) == 1

    def test_it_does_not_count_as_a_denial(self, db):
        """Unlike `revoke_user`. Going quiet is not the same as being refused,
        and the sweep does not push anybody towards the freeze."""
        database.request_access(1)
        database.approve_user(1, 9)
        _set(db, 1, last_activity=_utc_ago(days=60))
        database.revoke_inactive_users(days=45)
        assert _row(db, 1)["denial_count"] == 0

    def test_the_cutoff_is_utc_like_the_column_it_compares(self, db, far_from_utc):
        """The same defect as the freeze, in the other direction: a Kyiv `now()`
        put the cutoff three hours late and revoked people three hours early."""
        database.request_access(1)
        database.approve_user(1, 9)
        _set(db, 1, last_activity=_utc_ago(days=45, minutes=-30))
        assert database.revoke_inactive_users(days=45) == 0

    def test_somebody_active_later_on_the_cutoff_day_is_kept(self, db):
        """The bigger of the two clock defects, and the one that has nothing to
        do with timezones.

        The comparison is a **string** comparison. `cutoff.isoformat()` writes
        `'2026-07-13T10:00:00'` and SQLite stores `'2026-07-13 23:00:00'`; a
        space (0x20) sorts before a `'T'` (0x54), so every row whose activity
        fell on the cutoff date read as older than the cutoff whatever the time
        of day. Somebody who used the bot that morning lost their access.
        """
        database.request_access(1)
        database.approve_user(1, 9)
        # Same calendar day as the cutoff, but twelve hours the safe side of it.
        _set(db, 1, last_activity=_utc_ago(days=45, hours=-12))
        assert database.revoke_inactive_users(days=45) == 0
        assert _row(db, 1)["status"] == database.STATUS_APPROVED

    def test_only_approved_people_are_swept(self, db):
        database.request_access(1)
        _set(db, 1, last_activity=_utc_ago(days=99))
        assert database.revoke_inactive_users(days=45) == 0
        assert _row(db, 1)["status"] == database.STATUS_PENDING


class TestActivity:
    def test_it_is_recorded_for_an_approved_person(self, db):
        database.request_access(1)
        database.approve_user(1, 9)
        assert _row(db, 1)["last_activity"] is None
        database.update_last_activity(1)
        assert _row(db, 1)["last_activity"] is not None

    def test_it_is_not_recorded_for_anybody_else(self, db):
        """A pending or denied person interacting with the bot does not get a
        heartbeat that would keep the sweep off them later."""
        database.request_access(1)
        database.update_last_activity(1)
        assert _row(db, 1)["last_activity"] is None


class TestTheLists:
    def test_approved_and_frozen_are_listed_separately(self, db):
        for user_id in (1, 2, 3):
            database.request_access(user_id)
        database.approve_user(1, 9)
        for _ in range(database.MAX_DENIAL_COUNT):
            database.deny_user(2, 9)

        assert [u["user_id"] for u in database.get_all_authorized_users()] == [1]
        assert [u["user_id"] for u in database.get_frozen_users()] == [2]

    def test_a_pending_person_is_in_neither(self, db):
        database.request_access(3)
        assert database.get_all_authorized_users() == []
        assert database.get_frozen_users() == []


# ── preferences ──────────────────────────────────────────────────────────────


class TestPreferences:
    def test_somebody_who_never_opened_settings_has_no_row(self, db):
        assert database.get_user_preferences(1) is None

    def test_updating_one_setting_creates_the_row_first(self, db):
        database.update_user_preference(1, "timezone", "Europe/Warsaw")
        assert database.get_user_preferences(1)["timezone"] == "Europe/Warsaw"

    def test_only_known_settings_can_be_written(self, db):
        """The allowlist is load-bearing: the column name goes into the SQL by
        f-string, so it is the only thing between a caller and an injection."""
        database.update_user_preference(1, "timezone", "Europe/Warsaw")
        database.update_user_preference(1, "status = 'approved' --", "x")
        assert database.get_user_preferences(1)["timezone"] == "Europe/Warsaw"

    def test_saving_defaults_writes_the_documented_ones(self, db):
        database.save_user_preferences(1)
        prefs = database.get_user_preferences(1)
        assert prefs["default_report_type"] == "summary"
        assert prefs["timezone"] == "Europe/Kyiv"
        assert prefs["default_date_range"] == "week"
        assert prefs["notifications_enabled"] == 1

    def test_saving_twice_updates_rather_than_failing(self, db):
        database.save_user_preferences(1, timezone="Europe/Kyiv")
        database.save_user_preferences(1, timezone="UTC")
        assert database.get_user_preferences(1)["timezone"] == "UTC"


class TestLanguage:
    def test_a_stored_choice_wins(self, db):
        database.update_user_preference(1, "language", "ru")
        assert database.get_user_language(1) == "ru"

    def test_without_one_it_is_ukrainian_for_an_ordinary_person(self, db, monkeypatch):
        monkeypatch.setattr("bot.config.ADMIN_USER_IDS", [999])
        assert database.get_user_language(1) == "uk"

    def test_and_english_for_an_admin(self, db, monkeypatch):
        monkeypatch.setattr("bot.config.ADMIN_USER_IDS", [1])
        assert database.get_user_language(1) == "en"

    def test_a_stored_choice_wins_for_an_admin_too(self, db, monkeypatch):
        monkeypatch.setattr("bot.config.ADMIN_USER_IDS", [1])
        database.update_user_preference(1, "language", "uk")
        assert database.get_user_language(1) == "uk"


# ── milestones ───────────────────────────────────────────────────────────────


class TestMilestones:
    def test_a_milestone_is_celebrated_once(self, db):
        assert database.is_milestone_celebrated("monthly", "2026-08", 1000000) is False
        assert database.mark_milestone_celebrated("monthly", "2026-08", 1000000, 1_050_000.0) is True
        assert database.is_milestone_celebrated("monthly", "2026-08", 1000000) is True

    def test_marking_it_again_is_refused_rather_than_duplicated(self, db):
        """The `UNIQUE` constraint is the mechanism, and the second call reads
        the IntegrityError as "already announced" rather than as a failure."""
        database.mark_milestone_celebrated("monthly", "2026-08", 1000000, 1.0)
        assert database.mark_milestone_celebrated("monthly", "2026-08", 1000000, 2.0) is False

    def test_the_same_amount_in_another_period_is_a_different_milestone(self, db):
        database.mark_milestone_celebrated("monthly", "2026-08", 1000000, 1.0)
        assert database.is_milestone_celebrated("monthly", "2026-09", 1000000) is False
        assert database.mark_milestone_celebrated("monthly", "2026-09", 1000000, 1.0) is True


# ── cache ────────────────────────────────────────────────────────────────────


class TestCache:
    def test_a_value_comes_back(self, db):
        database.cache_set("k", {"a": 1}, ttl_minutes=10)
        assert database.cache_get("k") == {"a": 1}

    def test_a_missing_key_is_none(self, db):
        assert database.cache_get("nothing") is None

    def test_an_expired_value_is_not_returned(self, db):
        database.cache_set("k", {"a": 1}, ttl_minutes=-1)
        assert database.cache_get("k") is None

    def test_the_sweep_removes_expired_entries_and_says_how_many(self, db):
        database.cache_set("stale", 1, ttl_minutes=-1)
        database.cache_set("fresh", 2, ttl_minutes=10)
        assert database.cache_cleanup() == 1
        assert database.cache_get("fresh") == 2


# ── a verdict is for a pending request ───────────────────────────────────────

class TestAVerdictIsForThePendingRequest:
    """Two admins tapping Approve and Deny on the same request in the same
    second used to be last-write-wins, with the person told both. The buttons
    now answer the *request*: a verdict lands only while the row is pending."""

    def test_the_second_verdict_is_refused(self, db):
        database.request_access(1, "u", "F", "L")
        assert database.approve_user(1, 999, expected_status="pending") is True
        assert database.deny_user(1, 999, expected_status="pending") == (False, False)
        assert db.row(1)["status"] == "approved"
        assert db.row(1)["denial_count"] == 0

    def test_deny_then_approve_is_refused_too(self, db):
        database.request_access(1, "u", "F", "L")
        assert database.deny_user(1, 999, expected_status="pending") == (True, False)
        assert database.approve_user(1, 999, expected_status="pending") is False
        assert db.row(1)["status"] == "denied"

    def test_without_an_expectation_the_write_is_unconditional(self, db):
        """`revoke_user` is a deny on an approved row, and the admin
        auto-approval re-approves a swept admin; both must keep working."""
        database.request_access(1, "u", "F", "L")
        database.approve_user(1, 999)
        assert database.revoke_user(1, 999) is True
        assert db.row(1)["status"] == "denied"
        assert database.approve_user(1, 999) is True


class TestRequestAgainStartsFromADenial:
    def test_a_denied_person_may_ask_again(self, db):
        database.request_access(1, "u", "F", "L")
        database.deny_user(1, 999)
        assert database.reset_user_to_pending(1) == (True, False)
        assert db.row(1)["status"] == "pending"

    def test_a_stale_button_does_not_demote_an_approved_person(self, db):
        """The button lives on an old denial message; pressed after an
        approval it used to put the person back to pending and page the
        admins again."""
        database.request_access(1, "u", "F", "L")
        database.deny_user(1, 999)
        database.approve_user(1, 999)
        assert database.reset_user_to_pending(1) == (False, False)
        assert db.row(1)["status"] == "approved"


class TestTheFirstSettingDoesNotChooseALanguage:
    def test_a_new_row_carries_no_language(self, db, monkeypatch):
        """Changing a timezone used to create the row with the column's
        DEFAULT 'en', which the reader then treated as a choice — switching
        a Ukrainian-default user to English in the bot and the report."""
        monkeypatch.setattr("bot.config.ADMIN_USER_IDS", [999])
        database.update_user_preference(1, "timezone", "UTC")
        assert database.get_user_preferences(1)["timezone"] == "UTC"
        assert database.get_user_preferences(1)["language"] is None
        assert database.get_user_language(1) == "uk"

    def test_save_does_not_choose_either(self, db, monkeypatch):
        monkeypatch.setattr("bot.config.ADMIN_USER_IDS", [999])
        database.save_user_preferences(1, timezone="Europe/Kyiv")
        assert database.get_user_language(1) == "uk"

    def test_a_choice_survives_a_later_save(self, db):
        database.update_user_preference(1, "language", "ru")
        database.save_user_preferences(1, timezone="UTC")
        assert database.get_user_language(1) == "ru"
