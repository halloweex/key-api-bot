"""Reading the bot's settings from the container that does not own them.

The weekly report is built in the *web* container, because that is where
DuckDB lives. Who may read it, and in what language, is recorded in the *bot*
container's `data/bot.db`, because that is where the approval flow and the
settings screen run. Both containers bind-mount `./data`, so the file is simply
there — this module opens it read-only and asks it two questions.

Read-only and forgiving on purpose. The bot's SQLite runs in its default
rollback-journal mode, so a reader that arrives mid-write gets SQLITE_BUSY;
a locked database, a missing file, a column that predates this feature, and a
container started before the bot has ever run all mean the same thing here —
fall back. An unreadable list falls back to the admins and an unreadable
language to the caller's default. A report reaching fewer people, or one line
in the wrong language, is a blemish. A report that does not go out is a
failure.
"""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional

from core.i18n import DEFAULT_LANGUAGE, EN, UK, normalize

logger = logging.getLogger(__name__)

# Same path bot/database.py writes to, resolved from this file so it holds in
# the container (/app/data/bot.db) and in a checkout alike.
BOT_DB_PATH = Path(__file__).parent.parent / "data" / "bot.db"

# Short. Nothing here is worth making a scheduler job wait on a lock.
BUSY_TIMEOUT_SECONDS = 2.0


def read_approved_user_ids(db_path: Optional[Path] = None) -> List[int]:
    """Everyone the bot has approved and who has not muted notifications.

    `notifications_enabled` is an existing setting whose entire purpose is
    this, so a user who switched it off is not written to. A toggle that some
    messages ignore is worse than no toggle. Users who have never opened
    settings have no preferences row at all and are included — the column
    defaults to on.

    Read-only and forgiving, like everything else here: a database that cannot
    be read yields nobody, and the caller falls back to the admins.
    """
    path = Path(db_path) if db_path is not None else BOT_DB_PATH
    if not path.exists():
        logger.debug("Bot preferences DB not found at %s", path)
        return []

    try:
        with sqlite3.connect(f"file:{path}?mode=ro", uri=True,
                             timeout=BUSY_TIMEOUT_SECONDS) as conn:
            rows = conn.execute("""
                SELECT a.user_id
                FROM authorized_users a
                LEFT JOIN user_preferences p ON p.user_id = a.user_id
                WHERE a.status = 'approved'
                  AND COALESCE(p.notifications_enabled, 1) = 1
            """).fetchall()
    except sqlite3.Error as exc:
        logger.info("Could not read approved users from bot DB: %s", exc)
        return []

    return [int(row[0]) for row in rows]


def read_user_languages(
    user_ids: Iterable[int],
    db_path: Optional[Path] = None,
    defaults: Optional[Mapping[int, str]] = None,
) -> Dict[int, str]:
    """Language per user id, falling back per user where nothing is stored.

    `defaults` carries the per-user fallback — Ukrainian for staff, English for
    admins — so the policy lives with the caller that knows who is who, and
    this function stays a reader. A stored choice always wins over it.

    Always returns an entry for every id asked about, so callers never have to
    decide what a missing key means.
    """
    ids = list(dict.fromkeys(int(uid) for uid in user_ids))
    defaults = defaults or {}
    languages = {
        uid: normalize(defaults.get(uid, DEFAULT_LANGUAGE)) for uid in ids
    }
    if not ids:
        return languages

    path = Path(db_path) if db_path is not None else BOT_DB_PATH
    if not path.exists():
        logger.debug("Bot preferences DB not found at %s", path)
        return languages

    try:
        # mode=ro so this can never create, migrate or lock the bot's database.
        uri = f"file:{path}?mode=ro"
        with sqlite3.connect(uri, uri=True, timeout=BUSY_TIMEOUT_SECONDS) as conn:
            placeholders = ",".join("?" * len(ids))
            rows = conn.execute(
                f"SELECT user_id, language FROM user_preferences "
                f"WHERE user_id IN ({placeholders})",
                ids,
            ).fetchall()
    except sqlite3.Error as exc:
        # Includes "no such column: language" on a bot that has not restarted
        # into the migration yet — an ordinary state during a rolling deploy.
        logger.info("Could not read languages from bot DB: %s", exc)
        return languages

    for user_id, language in rows:
        if language:
            languages[int(user_id)] = normalize(language)
    return languages


def default_language_for(user_id: int, admin_ids: Iterable[int]) -> str:
    """Ukrainian for everyone, English for admins — until they choose.

    The one place this policy is written down. A stored choice overrides it
    everywhere, in the bot's interface and in the weekly report alike.
    """
    return EN if int(user_id) in {int(a) for a in admin_ids} else UK


def group_by_language(
    user_ids: Iterable[int],
    defaults: Optional[Mapping[int, str]] = None,
) -> Dict[str, List[int]]:
    """The same ids, bucketed by language, for one render per language.

    Most readers share a language, so this usually yields one or two buckets
    however many people are on the list — the grouping exists so that nobody
    silently gets someone else's.
    """
    languages = read_user_languages(user_ids, defaults=defaults)
    buckets: Dict[str, List[int]] = {}
    for user_id, language in languages.items():
        buckets.setdefault(language, []).append(user_id)
    return buckets
