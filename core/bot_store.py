"""The seam between the bot's state and whatever stores it.

`bot/database.py` was 717 lines of `sqlite3` with 46 call sites, and step 03
needs the engine underneath it to change. This is the port that makes that a
new file rather than a rewrite of the bot.

WHY IT IS SYNCHRONOUS, WHICH IS THE ONE DECISION HERE WORTH ARGUING

Measured before choosing: 38 of the 46 call sites are already inside `async
def` and would take an `await` without complaint. The other 8 would not, and
one of them decides it — `bot/handlers_legacy._lang()`, which resolves the
reader's language and is called from about forty places. CLAUDE.md records why
it is shaped that way: handlers resolve the language "at the point of use
rather than threading a parameter through forty signatures". Making the port
async makes `_lang` async and undoes that on purpose.

Against that: sync SQLite access from async code is already the established
pattern for these exact tables — `core/bot_prefs.py` reads them that way from
the *web* container, and `core/pg_bot_state.py` copies them that way. The
tables hold 51 rows.

**The cost is real and is written down here so it is not discovered later.** A
synchronous port cannot be backed by asyncpg directly, so a Postgres adapter
needs either `psycopg` — a second driver in a codebase that chose asyncpg to
have one — or asyncpg on a background loop. That choice belongs with the
adapter, on a measurement, and is deliberately not made here.

WHAT IS IN THE PORT AND WHAT IS NOT

Storage only. `get_user_language` reads a preference *and then* applies a
company rule about admins and Ukrainian; the rule stays in `bot/database.py`
and only the read is a port method, so an adapter never has to know who the
admins are. `generate_cache_key` is a pure function and is not here at all.

`revoke_user` is not here either, and its absence is the point. It is
implemented as `deny`, so revoking somebody's access increments their denial
count and five revocations freeze them — surprising enough that
`tests/unit/test_bot_database.py` pins it explicitly. Keeping it out of the
port means that if the owner ever decides revoking is not a denial, the port
gains a method instead of quietly changing the meaning of one.

TWO CLOCKS, AND THEY ARE NOT INTERCHANGEABLE

`authorized_users` and the rest carry timestamps written by SQLite's
`CURRENT_TIMESTAMP`, which is **UTC** in `'YYYY-MM-DD HH:MM:SS'`. The cache
writes `expires_at` itself, with Python's local `datetime.now().isoformat()`,
and reads it back the same way — self-consistent, and a different convention.
An adapter that unified them would expire the cache by the wrong clock. Both
conventions are the adapter's business; the port passes moments as `datetime`
and counts as `int`.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Protocol, Tuple, runtime_checkable


@runtime_checkable
class AccessControl(Protocol):
    """Who may use the bot: the request, the verdict, and the freeze."""

    def status(self, user_id: int) -> Optional[Dict[str, Any]]:
        """The whole row, or None for somebody who has never asked."""

    def is_approved(self, user_id: int) -> bool: ...

    def is_frozen(self, user_id: int) -> bool:
        """True while the freeze holds.

        Not a pure read: an expired freeze is lifted here, because this is the
        only moment anything looks at it. An adapter that made this pure would
        need a job to do the lifting.
        """

    def request(
        self, user_id: int, username: Optional[str] = None,
        first_name: Optional[str] = None, last_name: Optional[str] = None,
    ) -> bool:
        """Record a first request. False when the person already has a row —
        of any status. Coming back after a refusal is `reset_to_pending`."""

    def approve(self, user_id: int, admin_id: int) -> bool: ...

    def deny(self, user_id: int, admin_id: int) -> Tuple[bool, bool]:
        """`(written, now_frozen)`. The count is what freezes, not the verdict."""

    def unfreeze(self, user_id: int, admin_id: int) -> bool:
        """An admin lifting a freeze, which also forgives the count."""

    def reset_to_pending(self, user_id: int) -> Tuple[bool, bool]:
        """`(written, was_frozen)`. A frozen person cannot let themselves back in."""

    def touch(self, user_id: int) -> None:
        """Record activity — for approved people only, so that a pending or
        denied person interacting with the bot does not earn a heartbeat that
        keeps the sweep off them later."""

    def sweep_inactive(self, days: int) -> int:
        """Revoke the quiet. Returns how many. Not a denial: going quiet is
        not being refused, and this must not push anybody towards the freeze."""

    def approved(self) -> List[Dict[str, Any]]: ...
    def pending(self) -> List[Dict[str, Any]]: ...
    def frozen(self) -> List[Dict[str, Any]]: ...


@runtime_checkable
class Preferences(Protocol):
    """What each person chose. Storage only — the language *default* is a rule
    about this company and lives with the caller."""

    def get(self, user_id: int) -> Optional[Dict[str, Any]]:
        """None for somebody who has never opened settings, which is not the
        same as a row full of defaults: the weekly report reads a missing
        `notifications_enabled` as *on*."""

    def save(
        self, user_id: int, default_source: Optional[str] = None,
        default_report_type: str = "summary", timezone: str = "Europe/Kyiv",
        default_date_range: str = "week", notifications_enabled: bool = True,
    ) -> None: ...

    def set(self, user_id: int, key: str, value: Any) -> None:
        """One setting. The adapter is responsible for refusing a key that is
        not a real column — the name reaches the SQL, so the allowlist is the
        only thing between a caller and an injection."""


@runtime_checkable
class Milestones(Protocol):
    """Which revenue milestones have already been announced."""

    def already_celebrated(
        self, period_type: str, period_key: str, amount: int,
    ) -> bool: ...

    def celebrate(
        self, period_type: str, period_key: str, amount: int, revenue: float,
    ) -> bool:
        """True if this is the first time. The uniqueness is the mechanism —
        a second call is refused, not duplicated."""

    def recent(
        self, period_type: Optional[str] = None, limit: int = 10,
    ) -> List[Dict[str, Any]]: ...


@runtime_checkable
class ShortLivedCache(Protocol):
    """A TTL cache. The only aggregate here holding nothing that can be lost —
    which is why revision 0009 leaves it out of Postgres entirely."""

    def get(self, key: str) -> Optional[Any]: ...
    def set(self, key: str, value: Any, ttl_minutes: int = 10) -> None: ...
    def delete(self, key: str) -> None: ...
    def sweep(self) -> int: ...


@runtime_checkable
class BotStore(Protocol):
    """The four aggregates, which are the four tables plus the cache."""

    access: AccessControl
    preferences: Preferences
    milestones: Milestones
    cache: ShortLivedCache

    def initialise(self) -> None:
        """Make the store ready to be written to. Idempotent."""

    def stats(self) -> Dict[str, int]:
        """Row counts, for the operator. Not a port method with a reason —
        it exists because `get_database_stats` did."""


_store: Optional[BotStore] = None


def get_bot_store() -> BotStore:
    """The active store, defaulting to SQLite.

    Lazy, so importing this module costs nothing and a test that never touches
    the bot's state never creates a file. The default is SQLite because that is
    what production runs; the Postgres adapter arrives as an argument to
    `use_bot_store`, not as a change here.
    """
    global _store
    if _store is None:
        from bot.store_sqlite import SqliteBotStore

        _store = SqliteBotStore()
    return _store


def use_bot_store(store: Optional[BotStore]) -> Optional[BotStore]:
    """Install a store, returning the one it replaced.

    `None` restores the lazy default. Returning the previous one is what lets a
    caller put it back without knowing what it was — which is the whole
    interface a test or a rollback needs.
    """
    global _store
    previous, _store = _store, store
    return previous
