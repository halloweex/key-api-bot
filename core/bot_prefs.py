"""Reading the bot's settings from the container that does not own them.

The weekly report is built in the *web* container, because that is where
DuckDB lives. The language each admin chose is recorded in the *bot* container,
in `data/bot.db`, because that is where the settings screen runs. Both
containers bind-mount `./data`, so the file is simply there — this module opens
it read-only and asks one question.

Read-only and forgiving on purpose. The bot's SQLite runs in its default
rollback-journal mode, so a reader that arrives mid-write gets SQLITE_BUSY;
a locked database, a missing file, a column that predates this feature, and a
container started before the bot has ever run all mean the same thing here —
fall back to the default language. The report going out in English is a
blemish. The report not going out is a failure.
"""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Dict, Iterable, Optional

from core.i18n import DEFAULT_LANGUAGE, normalize

logger = logging.getLogger(__name__)

# Same path bot/database.py writes to, resolved from this file so it holds in
# the container (/app/data/bot.db) and in a checkout alike.
BOT_DB_PATH = Path(__file__).parent.parent / "data" / "bot.db"

# Short. Nothing here is worth making a scheduler job wait on a lock.
BUSY_TIMEOUT_SECONDS = 2.0


def read_user_languages(
    user_ids: Iterable[int], db_path: Optional[Path] = None,
) -> Dict[int, str]:
    """Language per user id, defaulting anyone without a stored choice.

    Always returns an entry for every id asked about, so callers never have to
    decide what a missing key means.
    """
    ids = list(dict.fromkeys(int(uid) for uid in user_ids))
    languages = {uid: DEFAULT_LANGUAGE for uid in ids}
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


def group_by_language(user_ids: Iterable[int]) -> Dict[str, list]:
    """The same ids, bucketed by language, for one render per language.

    Admins mostly share a language, so this usually yields a single bucket and
    the report is rendered once — the grouping exists so that the day they do
    not, nobody silently gets the wrong one.
    """
    languages = read_user_languages(user_ids)
    buckets: Dict[str, list] = {}
    for user_id, language in languages.items():
        buckets.setdefault(language, []).append(user_id)
    return buckets
