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

from typing import (
    Any, Dict, List, Optional, Protocol, Sequence, Tuple, runtime_checkable,
)


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

    def approve(
        self, user_id: int, admin_id: int, *, expected_status: Optional[str] = None,
    ) -> bool:
        """`expected_status` makes the verdict conditional on the row still
        being in that state — the admin's Approve button answers a *pending*
        request, and two admins tapping Approve and Deny on the same request
        in the same second used to be last-write-wins, with the person told
        both. Left None, the write is unconditional: the auto-approval of an
        admin, and `deny` as a revocation, act on whatever state is there."""

    def deny(
        self, user_id: int, admin_id: int, *, expected_status: Optional[str] = None,
    ) -> Tuple[bool, bool]:
        """`(written, now_frozen)`. The count is what freezes, not the verdict.
        `expected_status` as for `approve`."""

    def unfreeze(self, user_id: int, admin_id: int) -> bool:
        """An admin lifting a freeze, which also forgives the count."""

    def reset_to_pending(self, user_id: int) -> Tuple[bool, bool]:
        """`(written, was_frozen)`. A frozen person cannot let themselves back
        in, and only a *denied* person is let back to pending: the "Request
        again" button lives on an old denial message, and pressed after an
        approval it used to demote the person to pending and re-page the
        admins. Deny, the inactivity sweep and the auto-unfreeze all land on
        `denied`, so that is the one state a re-request starts from."""

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
class DashboardTabs(Protocol):
    """Which tabs a person may open on the **dashboard** — not on the bot.

    A different list from `AccessControl`, and deliberately so: `app.authorized_users`
    is who may talk to the bot, `app.dashboard_users` is who may open the web
    dashboard and as what, and on the day the second one was created the two
    disagreed about twelve of sixteen people (migration 0016 has the numbers).
    Merging them is the owner's decision and is not taken here.

    It is in this port because the bot writes it at exactly one moment — an
    admin approving an access request also chooses the tabs — and that write
    must go through the same pool and the same loop thread as everything else
    the bot stores. A second, unmanaged path to the database from inside the
    bot is what `tests/unit/test_pg_bot_state.py` watches for, and it would be
    right to.

    `available()` is the honest half. DuckDB takes a single writer and the web
    container holds it, so under `KS_USER_STORE=duckdb` no bot process can
    reach the dashboard's user list at all; the adapter says so and the
    approval still happens, with the tabs set from the admin page instead.
    """

    def available(self) -> bool:
        """Can this store reach the dashboard's user list at all?"""

    def get(self, user_id: int) -> Optional[Dict[str, Any]]:
        """The row, with `allowed_features` already parsed to a list of tab
        keys — or None for "as the role". None for the whole row means the
        person has never opened the dashboard and has no record there yet."""

    def grant(
        self, user_id: int, admin_id: int,
        features: Optional[Sequence[str]] = None,
        username: Optional[str] = None,
        first_name: Optional[str] = None,
        last_name: Optional[str] = None,
    ) -> bool:
        """Approve dashboard access and set the tab set in one statement.

        Not two: between an insert and an update the person is approved with
        somebody else's tabs, and the bot tells them they have access in that
        same second. The role is never touched — approving is not a statement
        about what somebody is."""

    def set_features(
        self, user_id: int, features: Optional[Sequence[str]], admin_id: int,
    ) -> bool:
        """Change the tab set of an existing row. False if there is none."""

    def permissions(self, user_id: int) -> Optional[Dict[str, Dict[str, bool]]]:
        """What this person may do, as the dashboard would answer it.

        The role's stored matrix narrowed by their tab set — the same two
        halves and the same pure rule (`core.permissions.apply_feature_override`)
        the web container combines on every request. It is here because the bot
        hands out the same numbers: a summary report is the dashboard's revenue
        in a Telegram message, and somebody narrowed to /traffic must not
        receive it just because the door they came through is different.

        **None means "nothing to narrow", not "denied".** Three cases return
        it, and all three mean the bot should behave exactly as it did before
        tabs existed: this store cannot be reached at all (`available()` is
        false), the person has no dashboard row, or their row carries no tab
        override. Failing closed here would lock every approved person out of
        the bot the first time the database hiccuped, to protect a narrowing
        that in that moment nobody has.
        """


@runtime_checkable
class AlertActions(Protocol):
    """The button on an alert: recording that a human authorised something.

    In this port for `DashboardTabs`' reason — the bot writes it at exactly one
    moment, and that write must go through the same pool and the same loop
    thread as everything else the bot stores. `tests/unit/test_pg_bot_state.py`
    watches for a second, unmanaged path to the database from inside the bot,
    and it would be right to.

    It records an *intent*, and never executes one. The tap arrives here and
    the work happens in the web container, which is where the scheduler and
    DuckDB are; the row is the handover. That split is not an inconvenience to
    route around — it is why a new internal credential was not needed.

    `available()` is false on SQLite, and honestly: the row has to be visible
    to the other container, and a file this process holds is not.
    """

    def available(self) -> bool:
        ...

    def request(self, *, action: str, subject: str, condition_key: str,
                by_user_id: int) -> "Optional[int]":
        """Record the authorisation. Returns the request id, or None."""
        ...


@runtime_checkable
class BotStore(Protocol):
    """The five aggregates: four tables, the cache, and the dashboard's list."""

    access: AccessControl
    preferences: Preferences
    milestones: Milestones
    cache: ShortLivedCache
    dashboard: DashboardTabs

    def initialise(self) -> None:
        """Make the store ready to be written to. Idempotent."""

    def stats(self) -> Dict[str, int]:
        """Row counts, for the operator. Not a port method with a reason —
        it exists because `get_database_stats` did."""


_store: Optional[BotStore] = None


# Which engine holds the bot's state. SQLite unless somebody says otherwise,
# because that is what production has run since the beginning and a default
# that changes under a deploy is not a switch, it is a surprise.
ENGINE_ENV = "KS_BOT_STORE"


def get_bot_store() -> BotStore:
    """The active store, chosen by `KS_BOT_STORE` and defaulting to SQLite.

    Lazy, so importing this module costs nothing and a test that never touches
    the bot's state never creates a file — nor, under `postgres`, opens a
    connection.

    An unknown value is refused rather than quietly treated as SQLite: a typo
    in the one variable that decides where the approval list lives should stop
    the bot, not silently point it at the old copy.
    """
    global _store
    if _store is None:
        import os

        engine = os.getenv(ENGINE_ENV, "sqlite").strip().lower()
        if engine in ("", "sqlite"):
            from bot.store_sqlite import SqliteBotStore

            _store = SqliteBotStore()
        elif engine == "postgres":
            from bot.store_postgres import PostgresBotStore

            _store = PostgresBotStore()
        else:
            raise RuntimeError(
                f"{ENGINE_ENV}={engine!r} is not a store this code knows. "
                "Expected 'sqlite' or 'postgres'."
            )
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
