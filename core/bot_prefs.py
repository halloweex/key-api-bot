"""The bot's settings, read from the container that does not own them.

The weekly report is built in the *web* container, because that is where
DuckDB lives. Who may read it, and in what language, is decided in the *bot*
container — the approval flow and the settings screen. Both questions go
through the bot store port (`core.bot_store.get_bot_store`), so this module
asks the same engine the bot writes to: Postgres under `KS_BOT_STORE=postgres`,
the SQLite file otherwise. Web instantiates that store at startup already.

Until 2026-09-06 this module opened `data/bot.db` directly, read-only. That
was right while the file *was* the bot's store, and silently wrong from the
day the bot switched to Postgres (2026-08-27): the file froze at that day's
contents, so the weekly report kept going to people revoked since, never
reached people approved since, ignored notifications muted since, and a
language chosen on the dashboard reached nothing the bot reads. `BOT_DB_PATH`
stays here only as the file's location, for the replication and comparison
that copy it while SQLite is still the engine.

Readers are forgiving on purpose: a store that cannot be reached yields nobody
or the default language, and the caller falls back to the admins. A report
reaching fewer people, or one line in the wrong language, is a blemish. A
report that does not go out is a failure. `write_language` is the exception
and raises: a settings screen that reports success while storing nothing is
the bug it replaced.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

from core.i18n import DEFAULT_LANGUAGE, EN, UK, normalize

logger = logging.getLogger(__name__)

# Where the bot's SQLite lives, resolved from this file so it holds in the
# container (/app/data/bot.db) and in a checkout alike. Not read here any more;
# `core/pg_bot_state.py` and the bot-state comparison import it to copy the
# file while SQLite is the engine.
BOT_DB_PATH = Path(__file__).parent.parent / "data" / "bot.db"


def _store():
    from core.bot_store import get_bot_store

    return get_bot_store()


def _notifications_on(value: Any) -> bool:
    """NULL means on: the column defaults to on, and a user who has never
    opened settings has no preferences row at all. Both adapters hand the
    value back in SQLite's shape, 1/0, so `bool` reads either engine."""
    return value is None or bool(value)


def read_approved_user_ids() -> List[int]:
    """Everyone the bot has approved and who has not muted notifications.

    `notifications_enabled` is an existing setting whose entire purpose is
    this, so a user who switched it off is not written to. A toggle that some
    messages ignore is worse than no toggle. Users who have never opened
    settings have no preferences row at all and are included.

    Read through the port, so a revocation or an approval made in the bot a
    minute ago is what this returns. Forgiving: a store that cannot be read
    yields nobody, and the caller falls back to the admins.
    """
    try:
        store = _store()
        ids: List[int] = []
        for row in store.access.approved():
            uid = int(row["user_id"])
            prefs = store.preferences.get(uid)
            if prefs is None or _notifications_on(prefs.get("notifications_enabled")):
                ids.append(uid)
        return ids
    except Exception as exc:  # noqa: BLE001 — a reader here must never take the report down
        logger.info("Could not read approved users from the bot store: %s", exc)
        return []


def read_user_languages(
    user_ids: Iterable[int],
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

    try:
        prefs = _store().preferences
        for uid in ids:
            row = prefs.get(uid)
            language = row.get("language") if row else None
            if language:
                languages[uid] = normalize(language)
    except Exception as exc:  # noqa: BLE001 — see read_approved_user_ids
        logger.info("Could not read languages from the bot store: %s", exc)
    return languages


def read_language(user_id: int, default: str = DEFAULT_LANGUAGE) -> str:
    """One user's language, with a per-user fallback. See `read_user_languages`."""
    uid = int(user_id)
    return read_user_languages([uid], defaults={uid: default})[uid]


def write_language(user_id: int, language: str) -> str:
    """Store a user's language choice where every reader already looks.

    The dashboard used to write this into DuckDB's own `user_preferences` — a
    second table with the same name and columns that the bot and the weekly
    report never read — and then into the SQLite file after the bot had moved
    to Postgres, which was the same defect in a new shape. The port is the one
    home, whichever engine is behind it.

    Unlike the readers here, this raises. A settings screen that reports success
    while storing nothing is the exact failure this function was written to end.
    """
    normalized = normalize(language)
    _store().preferences.set(int(user_id), "language", normalized)
    return normalized


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
    ids = list(dict.fromkeys(int(uid) for uid in user_ids))
    languages = read_user_languages(ids, defaults=defaults)
    buckets: Dict[str, List[int]] = {}
    for uid in ids:
        buckets.setdefault(languages[uid], []).append(uid)
    return buckets
