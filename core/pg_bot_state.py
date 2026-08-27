"""Copying the bot's SQLite into Postgres.

Step 03. `core/pg_operational.py` copies what only DuckDB holds; this copies
what only `data/bot.db` holds, and the case for it is simpler than any of them:
**that file is in no backup.** `data/backups/` takes `analytics-*.duckdb`,
`deploy/daily_offsite.sh` globs the same pattern, and the Ark froze the
warehouse. One file, one disk, no copy — 24 approvals, 25 celebrated milestones
and everyone's language choice.

So this is worth having before any question of switching the writer. Landing the
rows in Postgres puts them inside step 01's WAL archiving and nightly dump.

IT RUNS IN THE WEB CONTAINER AND DOES NOT TOUCH THE BOT

`core/bot_prefs.py` already reads this file read-only from the web container —
both containers bind-mount `./data`, and the weekly report has depended on that
since it was written. This does the same thing for the same reason, which means
step 03's first half needs **no change to the bot at all**: no new environment
variable there, no asyncpg on its path, no new way for it to fail. The bot goes
on writing SQLite exactly as it does today.

That is also why the copy is one-directional and Postgres is not yet read from.
Two writers to one logical table is the thing that has to be avoided; one
writer and one copy is not.

FULL REPLACE, AND NOT EVEN A CHOICE

Fifty-one rows. An incremental scheme would be more code than the data.
`authorized_users` also genuinely loses rows — `deny_user` and `revoke_user`
delete — so a watermark would leave ghosts that read as approved people who are
not, which is the worst shape available for this particular table.

FOUR TABLES, NOT FIVE

`cache` stays behind: a ten-minute TTL cache holds no fact that can be lost, and
copying it would report a discrepancy every time an entry expired between the
copy and the comparison. Revision 0009 has the argument.

TWO CONVERSIONS, BOTH OF THEM TRAPS

SQLite has no boolean and no timezone, and both gaps have to be closed on the
way out rather than left for whoever reads the table next:

* `notifications_enabled` is 1/0 and becomes a real boolean.
* Timestamps are `'YYYY-MM-DD HH:MM:SS'` with no zone, written by SQLite's
  `CURRENT_TIMESTAMP`, **which is UTC by definition**. Read as anything else,
  24 approvals move by three hours. The shop's port hit the same trap from the
  other side and it is recorded in its point 4.

IT STOPS THE MOMENT THE BOT WRITES POSTGRES ITSELF

This is a **one-way** copy and a full replace. Once `KS_BOT_STORE=postgres` the
bot writes `app.*` directly, and an hourly replace from a `bot.db` nobody is
updating any more would silently roll every approval back to whatever SQLite
last held — the worst possible shape, because it would look like the switch
worked for fifty-nine minutes at a time.

So the copy refuses to run under that setting rather than trusting whoever
flips it to also remember this file. The guard is here rather than in the
adapter because it is this code that would do the damage.

FAILURE POLICY

Never raises. It is called from a scheduler job beside the operational copy, and
a missing or locked `bot.db` must not take that job down — `core/bot_prefs.py`
has forgiven exactly those two conditions since it was written, on the grounds
that one odd line in a report beats no report. Here the equivalent is: a copy
that did not happen leaves the watermark where it was, and Reconciliation A says
so in the morning.
"""
from __future__ import annotations

import logging
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

AUTHORIZED_TABLE = "app.authorized_users"
PREFERENCES_TABLE = "app.user_preferences"
MILESTONES_TABLE = "app.celebrated_milestones"
REPORT_HISTORY_TABLE = "app.report_history"

AUTHORIZED_COLUMNS: Tuple[str, ...] = (
    "user_id", "username", "first_name", "last_name", "status",
    "requested_at", "reviewed_at", "reviewed_by", "last_activity",
    "denial_count",
)

PREFERENCE_COLUMNS: Tuple[str, ...] = (
    "user_id", "default_source", "default_report_type", "timezone",
    "created_at", "updated_at", "default_date_range", "notifications_enabled",
    "language",
)

MILESTONE_COLUMNS: Tuple[str, ...] = (
    "id", "period_type", "period_key", "milestone_amount", "revenue",
    "celebrated_at",
)

REPORT_HISTORY_COLUMNS: Tuple[str, ...] = (
    "id", "user_id", "report_type", "start_date", "end_date", "source",
    "created_at",
)

# Which SQLite columns are a moment, and which is a decision stored as 1/0.
# Named per table rather than guessed from the value, because a guess would
# read `denial_count = 1` as True and `language = '2026-01-01'` as a timestamp.
_TIMESTAMPS: Dict[str, Tuple[str, ...]] = {
    "authorized_users": ("requested_at", "reviewed_at", "last_activity"),
    "user_preferences": ("created_at", "updated_at"),
    "celebrated_milestones": ("celebrated_at",),
    "report_history": ("created_at",),
}
_BOOLEANS: Dict[str, Tuple[str, ...]] = {
    "user_preferences": ("notifications_enabled",),
}

# (Postgres table, SQLite table, columns, ORDER BY). `cache` is absent on
# purpose — see the module docstring.
TABLES: Tuple[Tuple[str, str, Tuple[str, ...], str], ...] = (
    (AUTHORIZED_TABLE, "authorized_users", AUTHORIZED_COLUMNS, "user_id"),
    (PREFERENCES_TABLE, "user_preferences", PREFERENCE_COLUMNS, "user_id"),
    (MILESTONES_TABLE, "celebrated_milestones", MILESTONE_COLUMNS, "id"),
    (REPORT_HISTORY_TABLE, "report_history", REPORT_HISTORY_COLUMNS, "id"),
)

# Short, like `core/bot_prefs.py`'s. Nothing here is worth making a scheduler
# job wait on the bot's lock.
BUSY_TIMEOUT_SECONDS = 2.0

# Formats SQLite writes. `CURRENT_TIMESTAMP` produces the first; Python's
# `datetime.isoformat()` through the sqlite3 adapter produces the others.
_TIMESTAMP_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
)


def as_utc(value: Any) -> Optional[datetime]:
    """One SQLite timestamp as an aware moment, or None.

    SQLite's `CURRENT_TIMESTAMP` is UTC by definition and carries no zone, so a
    naive value is read as UTC rather than as local time. Getting this wrong
    does not fail — it moves every approval by whatever the container's offset
    happens to be, silently, and only the daily comparison would ever say so.

    An unparseable string yields None rather than raising: one NULL timestamp
    on a copied row is a smaller loss than the row not travelling at all, and
    Reconciliation A reports the difference either way.
    """
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1]
    for fmt in _TIMESTAMP_FORMATS:
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        logger.info("Unparseable bot.db timestamp %r — copied as NULL", value)
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _convert(sqlite_table: str, columns: Sequence[str], row: Sequence[Any]) -> tuple:
    """One SQLite row in the types Postgres declares."""
    stamps = _TIMESTAMPS.get(sqlite_table, ())
    flags = _BOOLEANS.get(sqlite_table, ())
    out: List[Any] = []
    for column, value in zip(columns, row):
        if column in stamps:
            out.append(as_utc(value))
        elif column in flags:
            out.append(None if value is None else bool(value))
        else:
            out.append(value)
    return tuple(out)


def read_bot_state(db_path: Path) -> Dict[str, List[tuple]]:
    """Every copied table out of `bot.db`, converted, keyed by Postgres name.

    Read-only through a URI, exactly as `core/bot_prefs.py` opens it: this is
    the bot's live database and the web container has no business being able to
    write it.
    """
    out: Dict[str, List[tuple]] = {}
    with sqlite3.connect(
        f"file:{db_path}?mode=ro", uri=True, timeout=BUSY_TIMEOUT_SECONDS,
    ) as conn:
        for pg_table, sqlite_table, columns, order_by in TABLES:
            rows = conn.execute(
                f"SELECT {', '.join(columns)} FROM {sqlite_table} "
                f"ORDER BY {order_by}"
            ).fetchall()
            out[pg_table] = [_convert(sqlite_table, columns, r) for r in rows]
    return out


def _insert(table: str, columns: Sequence[str]) -> str:
    values = ", ".join(f"${i}" for i in range(1, len(columns) + 1))
    return f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({values})"


async def replicate_bot_state(db_path: Optional[Path] = None) -> Dict[str, Any]:
    """Copy `bot.db` into `app.*`. Never raises.

    All four tables in one transaction. They are read together — the weekly
    report joins `authorized_users` to `user_preferences` to decide who is
    written to — so a half-applied pair is a window in which the audience is
    computed from one snapshot's approvals and another's mute settings.
    """
    from core.bot_prefs import BOT_DB_PATH
    from core.bot_store import ENGINE_ENV
    from core.mirror_reconciliation import configured

    if not configured():
        return {"skipped": "KS_PG_DSN is not set"}

    engine = os.getenv(ENGINE_ENV, "sqlite").strip().lower()
    if engine == "postgres":
        # The bot is the writer now. Replacing `app.*` from SQLite here would
        # undo every approval made since the switch, once an hour.
        return {"skipped": f"{ENGINE_ENV}=postgres — the bot owns these tables"}

    path = Path(db_path) if db_path is not None else BOT_DB_PATH
    if not path.exists():
        # A developer's checkout, and the bot has simply never run here. Not an
        # error, and not something to file a finding about every hour.
        return {"skipped": f"{path} does not exist"}

    started = time.monotonic()
    try:
        from core.pg import get_pool, require_revision
        from core.pg_landing import _WATERMARK_OK

        pool = await get_pool()
        await require_revision()

        state = read_bot_state(path)

        async with pool.acquire() as conn:
            async with conn.transaction():
                # Deleted in the reverse of the order they are listed, so that
                # if a foreign key is ever added the teardown still works.
                for pg_table, _sq, _c, _o in reversed(TABLES):
                    await conn.execute(f"DELETE FROM {pg_table}")
                for pg_table, _sq, columns, _o in TABLES:
                    rows = state[pg_table]
                    if rows:
                        await conn.executemany(
                            _insert(pg_table, columns), rows,
                        )
                    await conn.execute(_WATERMARK_OK, pg_table, len(rows))

        result = {
            "rows": {t: len(state[t]) for t, _s, _c, _o in TABLES},
            "duration_ms": int((time.monotonic() - started) * 1000),
        }
        logger.info("Bot state replicated: %s", result)
        return result
    except Exception as e:
        # ERROR, not DEBUG, and returned rather than raised: the job this rides
        # in must survive a locked bot.db, and the watermark it did not move is
        # what Reconciliation A reads as "not replicated yet".
        logger.error("Bot state replication failed: %s", e, exc_info=True)
        return {"error": f"{type(e).__name__}: {e}"}
