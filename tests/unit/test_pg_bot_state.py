"""The bot's fifty rows, and the two type gaps between SQLite and Postgres.

`data/bot.db` is the third store in this system and the only one in no backup:
`data/backups/` and `deploy/daily_offsite.sh` both glob `analytics-*.duckdb`,
and the Ark froze the warehouse. That is the whole argument for copying it, and
it is checked here rather than asserted — `TestItIsReallyUnbacked` reads the
off-site script.

The interesting failures are not "did the rows arrive". They are the two gaps
SQLite has and Postgres does not: no boolean, and no timezone. An unconverted
`1` never equals `True`, and a naive string read as local time moves every
approval by the container's offset — silently, and only the daily comparison
would ever say so.
"""
from __future__ import annotations

import inspect
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from core.mirror_reconciliation import (
    BOT_DB_GRACE_MINUTES,
    BOT_STATE_TABLES,
    OPERATIONAL_GRACE_MINUTES,
    compare_table,
    fetch_sqlite_rows,
)
from core.pg_bot_state import (
    AUTHORIZED_COLUMNS,
    MILESTONE_COLUMNS,
    PREFERENCE_COLUMNS,
    REPORT_HISTORY_COLUMNS,
    TABLES,
    _convert,
    as_utc,
    read_bot_state,
    replicate_bot_state,
)

REPO = Path(__file__).resolve().parents[2]
_MIGRATION = REPO / "migrations" / "versions" / "0009_bot_state.py"

NOW = datetime(2026, 8, 27, 12, 0, tzinfo=timezone.utc)
OLD = "2026-08-01 10:00:00"


# ── fixtures ─────────────────────────────────────────────────────────────────


SCHEMA = """
CREATE TABLE authorized_users (
    user_id INTEGER PRIMARY KEY, username TEXT, first_name TEXT,
    last_name TEXT, status TEXT DEFAULT 'pending',
    requested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, reviewed_at TIMESTAMP,
    reviewed_by INTEGER, last_activity TIMESTAMP, denial_count INTEGER DEFAULT 0
);
CREATE TABLE user_preferences (
    user_id INTEGER PRIMARY KEY, default_source TEXT,
    default_report_type TEXT DEFAULT 'summary',
    timezone TEXT DEFAULT 'Europe/Kyiv',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    default_date_range TEXT DEFAULT 'week',
    notifications_enabled INTEGER DEFAULT 1, language TEXT DEFAULT 'en'
);
CREATE TABLE celebrated_milestones (
    id INTEGER PRIMARY KEY AUTOINCREMENT, period_type TEXT NOT NULL,
    period_key TEXT NOT NULL, milestone_amount INTEGER NOT NULL,
    revenue REAL NOT NULL, celebrated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE report_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
    report_type TEXT NOT NULL, start_date TEXT NOT NULL, end_date TEXT NOT NULL,
    source TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE cache (
    cache_key TEXT PRIMARY KEY, value TEXT NOT NULL,
    expires_at TIMESTAMP NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


@pytest.fixture
def bot_db(tmp_path) -> Path:
    """A `bot.db` shaped exactly like production's."""
    path = tmp_path / "bot.db"
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    conn.execute(
        "INSERT INTO authorized_users (user_id, username, first_name, status, "
        "requested_at, reviewed_at, reviewed_by, last_activity, denial_count) "
        f"VALUES (7000000001, 'ann', 'Ann', 'approved', '{OLD}', '{OLD}', 1, "
        f"'{OLD}', 0)"
    )
    conn.execute(
        "INSERT INTO authorized_users (user_id, status, requested_at) "
        f"VALUES (2, 'pending', '{OLD}')"
    )
    conn.execute(
        "INSERT INTO user_preferences (user_id, language, notifications_enabled,"
        f" created_at, updated_at) VALUES (7000000001, 'uk', 0, '{OLD}', '{OLD}')"
    )
    conn.execute(
        "INSERT INTO celebrated_milestones (id, period_type, period_key, "
        "milestone_amount, revenue, celebrated_at) "
        f"VALUES (1, 'monthly', '2026-03', 1000000, 1234567.89, '{OLD}')"
    )
    conn.execute(
        "INSERT INTO report_history (id, user_id, report_type, start_date, "
        f"end_date, source, created_at) VALUES (1, 7000000001, 'summary', "
        f"'2026-08-01', '2026-08-07', NULL, '{OLD}')"
    )
    conn.execute(
        "INSERT INTO cache (cache_key, value, expires_at) "
        "VALUES ('k', '{}', '2099-01-01 00:00:00')"
    )
    conn.commit()
    conn.close()
    return path


def _migration_columns(table: str) -> list[str]:
    ddl = _MIGRATION.read_text(encoding="utf-8")
    body = ddl.split(f"CREATE TABLE app.{table} (", 1)[1]
    columns = []
    for raw in body.splitlines():
        line = raw.split("--")[0].strip()
        if not line or line.startswith(")") or line.startswith("CONSTRAINT"):
            if line.startswith(")"):
                break
            continue
        match = re.match(
            r"^([a-z_]+)\s+(BIGINT|TEXT|INTEGER|BOOLEAN|TIMESTAMPTZ|DOUBLE)", line,
        )
        if match:
            columns.append(match.group(1))
    return columns


# ── the reason this exists ───────────────────────────────────────────────────


class TestItIsReallyUnbacked:
    """The argument for copying these fifty rows, checked rather than repeated."""

    def test_the_offsite_job_only_ever_takes_the_warehouse(self):
        script = (REPO / "deploy" / "daily_offsite.sh").read_text(encoding="utf-8")
        # Every path it selects for shipping. If `bot.db` is ever added here,
        # this test should fail and the argument in revision 0009 be revisited.
        selected = re.findall(r'ls -t "\$BACKUP_DIR"/(\S+)', script)
        assert selected, "found nothing being selected for the off-site copy"
        assert all("analytics" in pattern for pattern in selected), selected
        assert "bot.db" not in script


# ── the two type gaps ────────────────────────────────────────────────────────


class TestTheTimezoneGap:
    """SQLite's `CURRENT_TIMESTAMP` is UTC and says so nowhere."""

    def test_a_naive_sqlite_stamp_is_read_as_utc(self):
        got = as_utc("2026-08-01 10:00:00")
        assert got == datetime(2026, 8, 1, 10, 0, tzinfo=timezone.utc)

    def test_it_is_not_read_as_local_time(self):
        """The failure mode: silent, and it moves every approval by the
        container's offset. Only the daily comparison would ever say so."""
        assert as_utc("2026-08-01 10:00:00").utcoffset() == timedelta(0)

    @pytest.mark.parametrize("text,expected_micros", [
        ("2026-08-01 10:00:00", 0),
        ("2026-08-01 10:00:00.123456", 123456),
        ("2026-08-01T10:00:00", 0),
        ("2026-08-01T10:00:00.123456Z", 123456),
    ])
    def test_every_shape_sqlite_writes_is_understood(self, text, expected_micros):
        got = as_utc(text)
        assert got.microsecond == expected_micros
        assert got.tzinfo is not None

    def test_an_aware_value_is_left_alone(self):
        aware = datetime(2026, 8, 1, 10, tzinfo=timezone.utc)
        assert as_utc(aware) is aware

    def test_none_and_empty_are_none_not_epoch(self):
        assert as_utc(None) is None
        assert as_utc("") is None

    def test_nonsense_becomes_none_rather_than_raising(self):
        """One NULL timestamp on a copied row is a smaller loss than the row
        not travelling at all, and the comparison reports it either way."""
        assert as_utc("not a date") is None


class TestTheBooleanGap:
    def test_notifications_enabled_becomes_a_real_boolean(self):
        row = _convert("user_preferences", ("user_id", "notifications_enabled"), (1, 0))
        assert row[1] is False
        row = _convert("user_preferences", ("user_id", "notifications_enabled"), (1, 1))
        assert row[1] is True

    def test_null_stays_null_and_does_not_become_false(self):
        """`COALESCE(p.notifications_enabled, 1) = 1` in core/bot_prefs.py reads
        NULL as *on*. Turning it into False here would mute everybody who has
        never opened settings."""
        row = _convert("user_preferences", ("user_id", "notifications_enabled"), (1, None))
        assert row[1] is None

    def test_a_count_that_happens_to_be_one_is_not_a_boolean(self):
        """Which is why the conversion is named per column and not guessed
        from the value."""
        row = _convert("authorized_users", ("user_id", "denial_count"), (1, 1))
        assert row[1] == 1
        assert row[1] is not True


# ── reading ──────────────────────────────────────────────────────────────────


class TestReadingTheSqliteSide:
    def test_all_four_tables_come_back(self, bot_db):
        state = read_bot_state(bot_db)
        assert set(state) == {t for t, _s, _c, _o in TABLES}
        assert len(state["app.authorized_users"]) == 2
        assert len(state["app.user_preferences"]) == 1

    def test_the_cache_stays_behind(self, bot_db):
        """A ten-minute TTL cache holds no fact that can be lost, and copying
        it would report a discrepancy every time an entry expired between the
        copy and the comparison."""
        assert "app.cache" not in read_bot_state(bot_db)
        assert not any(sq == "cache" for _p, sq, _c, _o in TABLES)

    def test_a_telegram_id_past_two_billion_survives(self, bot_db):
        """BIGINT, not INTEGER: Telegram ids passed 2^31 in 2021."""
        rows = read_bot_state(bot_db)["app.authorized_users"]
        assert 7000000001 in {r[0] for r in rows}
        declared = _MIGRATION.read_text(encoding="utf-8")
        block = declared.split("CREATE TABLE app.authorized_users (", 1)[1]
        assert re.search(r"user_id\s+BIGINT PRIMARY KEY", block)

    def test_it_opens_the_bot_s_live_database_read_only(self):
        """The web container has no business being able to write it."""
        source = inspect.getsource(read_bot_state)
        assert "mode=ro" in source
        assert "uri=True" in source


class TestTheWriterAndTheMigrationAgree:
    @pytest.mark.parametrize("table,columns", [
        ("authorized_users", AUTHORIZED_COLUMNS),
        ("user_preferences", PREFERENCE_COLUMNS),
        ("celebrated_milestones", MILESTONE_COLUMNS),
        ("report_history", REPORT_HISTORY_COLUMNS),
    ])
    def test_every_copied_column_is_declared(self, table, columns):
        declared = _migration_columns(table)
        assert declared, f"parsed no columns out of app.{table}"
        assert set(columns) <= set(declared), set(columns) - set(declared)

    def test_the_ids_are_carried_not_generated(self):
        ddl = _MIGRATION.read_text(encoding="utf-8")
        assert "GENERATED" not in ddl
        assert "serial" not in ddl.lower()

    def test_the_uniqueness_that_stops_a_second_celebration_travels(self):
        """SQLite declares it. A copy that dropped it would accept a duplicate
        the source would have refused."""
        ddl = _MIGRATION.read_text(encoding="utf-8")
        assert "UNIQUE (period_type, period_key, milestone_amount)" in ddl


# ── the comparison ───────────────────────────────────────────────────────────


def _compare(bot_db, pg_rows, *, now=NOW):
    """One table through the real comparator, against a Postgres side given
    as a plain dict."""
    spec = BOT_STATE_TABLES[0]
    with sqlite3.connect(f"file:{bot_db}?mode=ro", uri=True) as conn:
        rows, synced = fetch_sqlite_rows(conn, spec)
    return compare_table(
        spec, rows, synced, pg_rows,
        {"last_ok_at": now - timedelta(hours=2), "failures_since_ok": 0},
        now=now, grace_minutes=BOT_DB_GRACE_MINUTES,
    )


class TestComparingSqliteAgainstPostgres:
    def test_identical_sides_report_nothing(self, bot_db):
        with sqlite3.connect(f"file:{bot_db}?mode=ro", uri=True) as conn:
            rows, _ = fetch_sqlite_rows(conn, BOT_STATE_TABLES[0])
        assert _compare(bot_db, rows) == []

    def test_a_lost_approval_is_critical(self, bot_db):
        with sqlite3.connect(f"file:{bot_db}?mode=ro", uri=True) as conn:
            rows, _ = fetch_sqlite_rows(conn, BOT_STATE_TABLES[0])
        rows.pop(7000000001)
        issues = _compare(bot_db, rows)
        assert [i.check_name for i in issues] == ["mirror_missing_rows"]
        assert issues[0].sample_ids == (7000000001,)

    def test_a_ghost_approval_is_reported(self, bot_db):
        """`deny_user` and `revoke_user` really delete, so a row Postgres has
        and SQLite does not is somebody reading as approved who is not."""
        with sqlite3.connect(f"file:{bot_db}?mode=ro", uri=True) as conn:
            rows, _ = fetch_sqlite_rows(conn, BOT_STATE_TABLES[0])
        rows[999] = rows[7000000001]
        assert "mirror_orphan_rows" in {i.check_name for i in _compare(bot_db, rows)}

    def test_a_changed_status_is_reported_with_the_column(self, bot_db):
        with sqlite3.connect(f"file:{bot_db}?mode=ro", uri=True) as conn:
            rows, _ = fetch_sqlite_rows(conn, BOT_STATE_TABLES[0])
        at = BOT_STATE_TABLES[0].columns.index("status")
        changed = list(rows[7000000001])
        changed[at] = "revoked"
        rows[7000000001] = tuple(changed)
        issues = [i for i in _compare(bot_db, rows) if i.check_name == "mirror_row_values"]
        assert issues and "status" in issues[0].description

    def test_someone_approved_minutes_ago_is_in_flight_not_lost(self, tmp_path):
        """The copy runs hourly, so a fresh row legitimately has not travelled."""
        path = tmp_path / "bot.db"
        conn = sqlite3.connect(path)
        conn.executescript(SCHEMA)
        fresh = (NOW - timedelta(minutes=4)).strftime("%Y-%m-%d %H:%M:%S")
        conn.execute(
            "INSERT INTO authorized_users (user_id, status, requested_at) "
            f"VALUES (5, 'approved', '{fresh}')"
        )
        conn.commit()
        conn.close()
        assert _compare(path, {}) == []

    def test_the_same_row_is_reported_once_the_window_has_passed(self, bot_db):
        assert "mirror_missing_rows" in {i.check_name for i in _compare(bot_db, {})}

    def test_the_clock_falls_back_when_a_row_has_never_been_touched(self, bot_db):
        """`last_activity` and `reviewed_at` are NULL for a pending request;
        `requested_at` never is. Without the COALESCE a brand-new request has
        no clock and is CRITICAL the moment it is made."""
        spec = BOT_STATE_TABLES[0]
        assert "COALESCE(" in spec.synced_column
        with sqlite3.connect(f"file:{bot_db}?mode=ro", uri=True) as conn:
            _rows, synced = fetch_sqlite_rows(conn, spec)
        assert synced[2] is not None      # the pending user, never active


class TestTheSpecs:
    def test_all_four_are_full_replace(self):
        """`deny_user` and `revoke_user` delete. A watermark would leave a
        ghost that reads as an approved person who is not."""
        assert all(spec.full_replace for spec in BOT_STATE_TABLES)

    def test_the_compared_columns_are_the_copied_columns(self):
        copied = {t: c for t, _s, c, _o in TABLES}
        for spec in BOT_STATE_TABLES:
            assert set(spec.columns) == set(copied[spec.pg_table]), spec.pg_table

    def test_the_grace_window_is_the_replication_job_s(self):
        """Both copies ride the same hourly job, so one window covers them."""
        assert BOT_DB_GRACE_MINUTES == OPERATIONAL_GRACE_MINUTES

    def test_a_finding_says_what_this_table_actually_is(self):
        """Found by reading a real finding on the runtime: a changed `language`
        was reported as "written from the same parsed tuple in the same call",
        which is the landing mirror's story and sends the reader hunting a
        shared write path that does not exist here."""
        for spec in BOT_STATE_TABLES:
            assert "bot.db" in spec.origin_note
            assert "same parsed tuple" not in spec.origin_note
            assert "_convert" in spec.origin_note

    def test_the_conversion_has_one_home(self):
        """SQLite has no boolean and no timezone. Two conversions would have to
        agree, and a rule with two homes is a rule that will differ."""
        source = inspect.getsource(fetch_sqlite_rows)
        assert "from core.pg_bot_state import _convert" in source


# ── the copy ─────────────────────────────────────────────────────────────────


class TestReplication:
    @pytest.mark.asyncio
    async def test_no_postgres_configured_is_a_skip(self):
        with patch("core.mirror_reconciliation.configured", return_value=False):
            assert "skipped" in await replicate_bot_state()

    @pytest.mark.asyncio
    async def test_a_checkout_without_a_bot_db_is_a_skip_not_an_error(self, tmp_path):
        """A developer's machine where the bot has simply never run. Not
        something to file a finding about every hour."""
        with patch("core.mirror_reconciliation.configured", return_value=True):
            result = await replicate_bot_state(tmp_path / "absent.db")
        assert "skipped" in result

    @pytest.mark.asyncio
    async def test_a_locked_database_is_returned_not_thrown(self, bot_db):
        """The job this rides in also copies the operational history, and a
        lock on the bot's file must not take that down with it."""
        with patch("core.mirror_reconciliation.configured", return_value=True), \
             patch("core.pg.get_pool",
                   new=AsyncMock(side_effect=RuntimeError("database is locked"))):
            result = await replicate_bot_state(bot_db)
        assert "database is locked" in result["error"]

    @pytest.mark.asyncio
    async def test_all_four_tables_go_in_one_transaction(self):
        """The weekly report joins approvals to mute settings to decide who is
        written to. A half-applied pair computes the audience from one
        snapshot's approvals and another's settings."""
        source = inspect.getsource(replicate_bot_state)
        body = source[source.index("async with conn.transaction():"):]
        assert body.count("DELETE FROM") == 1      # one loop over all four
        assert "for pg_table, _sq, columns, _o in TABLES:" in body


class TestTheJob:
    def test_the_hourly_job_copies_the_bot_state_too(self):
        from core.scheduler import BackgroundScheduler

        source = inspect.getsource(
            BackgroundScheduler._run_replicate_operational,
        )
        assert "await replicate_bot_state()" in source

    def test_the_reconciliation_runs_it(self):
        from core.scheduler import BackgroundScheduler

        source = inspect.getsource(BackgroundScheduler._run_dq_mirror_landing)
        assert source.index("await reconcile_operational(store)") < source.index(
            "await reconcile_bot_state()"
        )

    def test_the_bot_container_is_not_touched(self):
        """Step 03's first half needs no change to the bot: no environment
        variable, no asyncpg on its path, no new way for it to fail. It reads
        the file the web container already opens for the weekly report."""
        offenders = []
        for path in (REPO / "bot").rglob("*.py"):
            text = path.read_text(encoding="utf-8")
            if re.search(r"^\s*(from|import)\s+core\.pg", text, re.MULTILINE):
                offenders.append(path.name)
        assert offenders == [], offenders
