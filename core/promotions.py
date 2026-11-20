"""Promotions planned in advance, every edit a version (revision 0039).

WHY

The revenue forecast cannot know a promotion before it happens, and the size
of one cannot be known in advance by anybody. So what is recorded is what is
known before a promotion starts — its days, its kind, its discount, what it
covers and where it is announced — and every edit is kept as a version of its
own: a later test can then ask what was known on the morning of any day, and
nothing about a promotion's past is rewritten by hindsight. The owner approved
it on 2026-11-19 (the mockup "Календарь акций и рекламы", version 2).

WHAT A PROMOTION IS

Rows of `app.promotion_versions` sharing a `promotion_id`, numbered 1, 2, ….
The current state is the highest version (`current`); the state as of T is
the highest version whose `recorded_at` is T or earlier (`as_of`). Cancelling
writes a version with `status = 'cancelled'` and the content it had; restoring
writes one with `'planned'`. A save that changes nothing writes nothing: a
version is a change, not an observation — `app.order_versions`' rule.

ONE WRITER

`_write_version` is the table's only writer — `tests/unit/test_promotions.py`
walks the repository for it — and `create`, `update`, `cancel` and `restore`
its only callers. Each is one INSERT in one transaction, after
`pg_advisory_xact_lock(PROMOTIONS_LOCK_KEY)`: the latest version is read
under the lock, so two saves of one promotion cannot both write version N+1,
and a save waits for the other to commit and then builds on it. One key for
every promotion, because saves are typed by people and a few a week:
serialising them costs nothing, and it keeps `recorded_at` in the order the
versions were written.

`recorded_at` is the writer's clock after it took the lock
(`clock_timestamp()`), never earlier than the previous version's, so "as of T"
and "the highest version" agree. `now()` would be the transaction's start,
which a save that waited behind another would give a version older than the
one it follows.

THE CHECKS

`parse` holds every field to what the table holds it to, and says which field
and why in English — the API answers that as a 400. The table's CHECKs are the
second line: a row this module writes never meets one.

Reads and writes ask `require_revision()` first, as every Postgres door here
does.
"""
from __future__ import annotations

import math
import re
from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Callable, Dict, List, Mapping, Optional, Set, Tuple

from core.duckdb_constants import DEFAULT_TZ
from core.exceptions import ValidationError
from core.filters import EARLIEST_DATE, LATEST_DATE

TABLE = "app.promotion_versions"

PLANNED = "planned"
CANCELLED = "cancelled"
KINDS: Tuple[str, ...] = ("black_friday", "other")
SCOPES: Tuple[str, ...] = ("store", "brands", "categories")
# In this order on the way out, whatever order they came in.
CHANNELS: Tuple[str, ...] = ("sms", "instagram", "telegram", "site")
SOURCES: Tuple[str, ...] = ("form", "form_from_sms")

NAME_MAX = 120
SPAN_MAX_DAYS = 62           # the first and the last day included
DISCOUNT_MAX = Decimal("90")
ITEMS_MAX = 50
ITEM_MAX = 80
NOTE_MAX = 500
SMS_CAMPAIGN_MAX = 200

# What `GET /api/promotions` answers without a window: the last 90 days, for
# "Минулі", and a year ahead.
WINDOW_BACK_DAYS = 90
WINDOW_AHEAD_DAYS = 365

# Every save of every promotion, serialised. Beside the chains' keys
# (`0x6B73_…_0001` and the rest); `tests/unit/test_dq_journal.py` walks
# `core/` for every `*_LOCK_KEY` and fails on a reuse.
PROMOTIONS_LOCK_KEY = 0x6B73_0000_0000_0A01

_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_CLOCK = re.compile(r"^(\d{2}):(\d{2})(?::(\d{2}))?$")


# ─── What a save carries ─────────────────────────────────────────────────────

@dataclass(frozen=True)
class PromotionIn:
    """A promotion's content, as `parse` accepted it: what one version holds
    besides its number, status and stamps."""

    name: str
    starts_on: date
    ends_on: date
    kind: str
    scope: str
    start_time: Optional[time] = None
    discount_pct: Optional[Decimal] = None
    scope_items: Tuple[str, ...] = ()
    channels: Tuple[str, ...] = ()
    sms_campaign: Optional[str] = None
    note: Optional[str] = None
    source: str = "form"

    def same_content(self, other: "PromotionIn") -> bool:
        """Whether a save of `other` over this would change anything a
        reader sees. `source` says how a state was typed, not what it is."""
        return replace(self, source=other.source) == other


def _text(payload: Mapping[str, Any], field: str, limit: int, *,
          required: bool = False) -> Optional[str]:
    value = payload.get(field)
    if value is None:
        if required:
            raise ValidationError(field, "Is required")
        return None
    if not isinstance(value, str):
        raise ValidationError(field, "Must be text")
    if "\x00" in value:
        raise ValidationError(field, "Must not contain a NUL character")
    value = value.strip()
    if not value:
        if required:
            raise ValidationError(field, "Must not be empty")
        return None
    if len(value) > limit:
        raise ValidationError(field, f"Must be at most {limit} characters")
    return value


def _day(payload: Mapping[str, Any], field: str) -> date:
    return parse_day(payload.get(field), field)


def parse_day(value: Any, field: str) -> date:
    """A Kyiv date as `YYYY-MM-DD`, inside the dashboard's window
    (2000-01-01..2099-12-31), or `ValidationError(field, reason)`."""
    if value is None:
        raise ValidationError(field, "Is required")
    if not isinstance(value, str) or not _ISO_DATE.match(value):
        raise ValidationError(field, "Must be a date (YYYY-MM-DD)")
    try:
        day = date.fromisoformat(value)
    except ValueError:
        raise ValidationError(field, "Not a valid date (YYYY-MM-DD)") from None
    if not EARLIEST_DATE <= day <= LATEST_DATE:
        raise ValidationError(field, f"Date must be between {EARLIEST_DATE} and {LATEST_DATE}")
    return day


def _clock(payload: Mapping[str, Any], field: str) -> Optional[time]:
    value = payload.get(field)
    if value is None or value == "":
        return None
    match = _CLOCK.match(value) if isinstance(value, str) else None
    if match is None:
        raise ValidationError(field, "Must be a time (HH:MM)")
    hour, minute = int(match.group(1)), int(match.group(2))
    if hour > 23 or minute > 59 or (match.group(3) and int(match.group(3)) > 59):
        raise ValidationError(field, "Must be a time (HH:MM)")
    # Minutes are what the form says and what the page shows.
    return time(hour, minute)


def _choice(payload: Mapping[str, Any], field: str, allowed: Tuple[str, ...],
            default: Optional[str] = None) -> str:
    value = payload.get(field)
    if value is None and default is not None:
        return default
    if value not in allowed:
        raise ValidationError(field, f"Must be one of {', '.join(allowed)}")
    return value


def _percent(payload: Mapping[str, Any], field: str) -> Optional[Decimal]:
    value = payload.get(field)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError(field, "Must be a number")
    if isinstance(value, float) and not math.isfinite(value):
        raise ValidationError(field, "Must be a number")
    raw = Decimal(str(value))
    # Bounded before it is rounded: `quantize` raises on a number with more
    # digits than the context holds (1e308), which would be a 500.
    if not Decimal(-1) < raw < DISCOUNT_MAX + 1:
        raise ValidationError(field, f"Must be between 0 and {DISCOUNT_MAX}")
    # Two places, as NUMERIC(5,2) holds it, rounded once here so the page
    # reads back what was stored.
    pct = raw.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    if not Decimal(0) <= pct <= DISCOUNT_MAX:
        raise ValidationError(field, f"Must be between 0 and {DISCOUNT_MAX}")
    return pct


def _texts(payload: Mapping[str, Any], field: str) -> List[str]:
    """A list of strings, each stripped; duplicates dropped, order kept."""
    value = payload.get(field)
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValidationError(field, "Must be a list")
    out: List[str] = []
    for item in value:
        if not isinstance(item, str):
            raise ValidationError(field, "Must be a list of text")
        if "\x00" in item:
            raise ValidationError(field, "Must not contain a NUL character")
        item = item.strip()
        if not item:
            raise ValidationError(field, "Must not hold an empty item")
        if item not in out:
            out.append(item)
    return out


def parse(payload: Any) -> PromotionIn:
    """`PromotionIn` out of a request body, or `ValidationError(field,
    reason)` for the first field that is wrong. Keys it does not know are
    ignored: the page may send a promotion back whole."""
    if not isinstance(payload, Mapping):
        raise ValidationError("body", "Must be a JSON object")
    name = _text(payload, "name", NAME_MAX, required=True)
    starts_on = _day(payload, "starts_on")
    ends_on = _day(payload, "ends_on")
    if ends_on < starts_on:
        raise ValidationError("ends_on", "Must not be before starts_on")
    if (ends_on - starts_on).days + 1 > SPAN_MAX_DAYS:
        raise ValidationError("ends_on", f"A promotion lasts at most {SPAN_MAX_DAYS} days")
    start_time = _clock(payload, "start_time")
    kind = _choice(payload, "kind", KINDS)
    discount_pct = _percent(payload, "discount_pct")
    scope = _choice(payload, "scope", SCOPES)

    items = _texts(payload, "scope_items")
    if scope == "store" and items:
        raise ValidationError("scope_items", "Must be empty when scope is store")
    if scope != "store" and not items:
        raise ValidationError("scope_items", f"Must name at least one item when scope is {scope}")
    if len(items) > ITEMS_MAX:
        raise ValidationError("scope_items", f"Must hold at most {ITEMS_MAX} items")
    if any(len(item) > ITEM_MAX for item in items):
        raise ValidationError("scope_items", f"Each item must be at most {ITEM_MAX} characters")

    channels = _texts(payload, "channels")
    unknown = [c for c in channels if c not in CHANNELS]
    if unknown:
        raise ValidationError("channels", f"Must be among {', '.join(CHANNELS)}")

    assert name is not None   # required above
    return PromotionIn(
        name=name, starts_on=starts_on, ends_on=ends_on, kind=kind, scope=scope,
        start_time=start_time, discount_pct=discount_pct,
        scope_items=tuple(items),
        channels=tuple(c for c in CHANNELS if c in channels),
        sms_campaign=_text(payload, "sms_campaign", SMS_CAMPAIGN_MAX),
        note=_text(payload, "note", NOTE_MAX),
        source=_choice(payload, "source", SOURCES, default="form"),
    )


# ─── What is stored ──────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Promotion:
    """One version of a promotion, with when its first version was saved."""

    id: int
    version: int
    status: str
    recorded_at: datetime
    recorded_by: Optional[int]
    created_at: datetime
    data: PromotionIn


class PromotionNotFound(LookupError):
    """No promotion has this id."""

    def __init__(self, promotion_id: int):
        super().__init__(f"No promotion {promotion_id}")
        self.promotion_id = promotion_id


class PromotionConflict(Exception):
    """The save asks for a state the promotion's current status refuses: an
    edit or a cancel of a cancelled promotion, a restore of a planned one."""

    def __init__(self, promotion_id: int, message: str):
        super().__init__(f"Promotion {promotion_id} {message}")
        self.promotion_id = promotion_id


def _kyiv(stamp: datetime) -> str:
    return stamp.astimezone(DEFAULT_TZ).isoformat(timespec="seconds")


def _number(value: Optional[Decimal]) -> Optional[float]:
    """A percent as JSON shows it: 20, not 20.00; 12.5 stays 12.5."""
    if value is None:
        return None
    return int(value) if value == value.to_integral_value() else float(value)


def to_json(promotion: Promotion) -> Dict[str, Any]:
    """The API's `Promotion`: the state this version holds, its first
    version's time as `created_at` and its own as `updated_at`, in Kyiv.
    `recorded_by` is not served — who typed a promotion is personal data,
    and the page needs none of it."""
    data = promotion.data
    return {
        "id": promotion.id,
        "version": promotion.version,
        "name": data.name,
        "starts_on": data.starts_on.isoformat(),
        "ends_on": data.ends_on.isoformat(),
        "start_time": data.start_time.strftime("%H:%M") if data.start_time else None,
        "kind": data.kind,
        "discount_pct": _number(data.discount_pct),
        "scope": data.scope,
        "scope_items": list(data.scope_items),
        "channels": list(data.channels),
        "sms_campaign": data.sms_campaign,
        "note": data.note,
        "status": promotion.status,
        "created_at": _kyiv(promotion.created_at),
        "updated_at": _kyiv(promotion.recorded_at),
        "source": data.source,
    }


def version_json(promotion: Promotion, viewer_id: Optional[int]) -> Dict[str, Any]:
    """One entry of a promotion's history: the state as of that version, when
    it was recorded, and whether the one asking recorded it."""
    return {
        **to_json(promotion),
        "recorded_at": _kyiv(promotion.recorded_at),
        "recorded_by_me": (viewer_id is not None
                           and promotion.recorded_by is not None
                           and int(promotion.recorded_by) == int(viewer_id)),
    }


def today() -> date:
    """Today in Kyiv, the calendar every date here is in."""
    return datetime.now(DEFAULT_TZ).date()


def default_window(day: Optional[date] = None) -> Tuple[date, date]:
    """`GET /api/promotions`' window when the request names none."""
    day = day or today()
    return day - timedelta(days=WINDOW_BACK_DAYS), day + timedelta(days=WINDOW_AHEAD_DAYS)


# ─── The statements ──────────────────────────────────────────────────────────

_COLUMNS = (
    "promotion_id, version, recorded_at, recorded_by, source, status, name, "
    "starts_on, ends_on, start_time, kind, discount_pct, scope, scope_items, "
    "channels, sms_campaign, note"
)

_LOCK_SQL = "SELECT pg_advisory_xact_lock($1)"

_NEXT_ID_SQL = "SELECT nextval('app.promotion_ids')"

# The promotion's latest version and its first version's time. Read after
# the lock, in a statement of its own: under READ COMMITTED it sees the
# version a save that held the lock before this one committed.
_LATEST_SQL = (
    f"SELECT {_COLUMNS}, "
    "(SELECT f.recorded_at FROM app.promotion_versions f "
    " WHERE f.promotion_id = v.promotion_id AND f.version = 1) AS created_at "
    "FROM app.promotion_versions v WHERE v.promotion_id = $1 "
    "ORDER BY v.version DESC LIMIT 1"
)

# One version. `recorded_at` is the clock after the lock, never earlier than
# the version before it ($3, NULL for a first version: GREATEST skips it).
_INSERT_SQL = (
    "INSERT INTO app.promotion_versions "
    "(promotion_id, version, recorded_at, recorded_by, source, status, name, "
    "starts_on, ends_on, start_time, kind, discount_pct, scope, scope_items, "
    "channels, sms_campaign, note) "
    "VALUES ($1, $2, GREATEST(clock_timestamp(), $3::timestamptz), $4, $5, $6, $7, "
    "$8, $9, $10, $11, $12, $13, $14, $15, $16, $17) "
    "RETURNING recorded_at"
)

# Each promotion's latest version recorded at $1 or earlier — every version
# when $1 is NULL — whose days overlap $2..$3; the cancelled too when $4.
_STATES_SQL = (
    "WITH latest AS ("
    f" SELECT DISTINCT ON (promotion_id) {_COLUMNS}"
    " FROM app.promotion_versions"
    " WHERE $1::timestamptz IS NULL OR recorded_at <= $1"
    " ORDER BY promotion_id, version DESC) "
    "SELECT l.*, f.recorded_at AS created_at FROM latest l "
    "JOIN app.promotion_versions f ON f.promotion_id = l.promotion_id AND f.version = 1 "
    "WHERE l.starts_on <= $3 AND l.ends_on >= $2 AND ($4 OR l.status = 'planned') "
    "ORDER BY l.starts_on, l.promotion_id"
)

_VERSIONS_SQL = (
    f"SELECT {', '.join('v.' + c.strip() for c in _COLUMNS.split(','))}, "
    "f.recorded_at AS created_at FROM app.promotion_versions v "
    "JOIN app.promotion_versions f ON f.promotion_id = v.promotion_id AND f.version = 1 "
    "WHERE v.promotion_id = $1 ORDER BY v.version DESC"
)

_LATEST_STAMP_SQL = "SELECT max(recorded_at) FROM app.promotion_versions"

_CAMPAIGNS_NAMED_SQL = (
    "SELECT DISTINCT sms_campaign FROM app.promotion_versions "
    "WHERE sms_campaign IS NOT NULL"
)


async def _pool():
    from core.pg import get_pool, require_revision

    pool = await get_pool()
    await require_revision()
    return pool


def _promotion(row: Mapping[str, Any]) -> Promotion:
    return Promotion(
        id=int(row["promotion_id"]),
        version=int(row["version"]),
        status=row["status"],
        recorded_at=row["recorded_at"],
        recorded_by=row["recorded_by"],
        created_at=row["created_at"],
        data=PromotionIn(
            name=row["name"],
            starts_on=row["starts_on"],
            ends_on=row["ends_on"],
            kind=row["kind"],
            scope=row["scope"],
            start_time=row["start_time"],
            discount_pct=row["discount_pct"],
            scope_items=tuple(row["scope_items"] or ()),
            channels=tuple(row["channels"] or ()),
            sms_campaign=row["sms_campaign"],
            note=row["note"],
            source=row["source"],
        ),
    )


# ─── The writer ──────────────────────────────────────────────────────────────

# What a save decides from the current version (None for a new promotion):
# the next state as (status, content), None to write nothing, or it raises
# `PromotionConflict`.
_Decision = Callable[[Optional[Promotion]], Optional[Tuple[str, PromotionIn]]]


async def _write_version(promotion_id: Optional[int], decide: _Decision,
                         by: Optional[int]) -> Promotion:
    """The one writer: lock, read the latest version, decide, insert the next.

    A new promotion (`promotion_id` None) takes its id from `app.promotion_ids`
    and writes version 1. Returns the promotion as this save left it — the
    current version unchanged when `decide` says there is nothing to write.
    Raises `PromotionNotFound`, or what `decide` raises, with nothing written.
    """
    pool = await _pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(_LOCK_SQL, PROMOTIONS_LOCK_KEY)
            current: Optional[Promotion] = None
            if promotion_id is None:
                promotion_id = int(await conn.fetchval(_NEXT_ID_SQL))
            else:
                row = await conn.fetchrow(_LATEST_SQL, promotion_id)
                if row is None:
                    raise PromotionNotFound(promotion_id)
                current = _promotion(row)
            decided = decide(current)
            if decided is None:
                assert current is not None
                return current
            status, data = decided
            version = current.version + 1 if current else 1
            recorded_at = await conn.fetchval(
                _INSERT_SQL, promotion_id, version,
                current.recorded_at if current else None, by, data.source,
                status, data.name, data.starts_on, data.ends_on, data.start_time,
                data.kind, data.discount_pct, data.scope, list(data.scope_items),
                list(data.channels), data.sms_campaign, data.note)
            return Promotion(
                id=promotion_id, version=version, status=status,
                recorded_at=recorded_at, recorded_by=by,
                created_at=current.created_at if current else recorded_at,
                data=data)


async def create(data: PromotionIn, *, by: Optional[int]) -> Promotion:
    """A new promotion, planned: its version 1."""
    return await _write_version(None, lambda _current: (PLANNED, data), by)


async def update(promotion_id: int, data: PromotionIn, *, by: Optional[int]) -> Promotion:
    """The promotion with `data` as its content, in a version of its own —
    none when nothing changed. A cancelled promotion refuses an edit:
    restoring it is a decision of its own."""

    def decide(current: Optional[Promotion]):
        assert current is not None
        if current.status == CANCELLED:
            raise PromotionConflict(promotion_id, "is cancelled: restore it first")
        if current.data.same_content(data):
            return None
        return PLANNED, data

    return await _write_version(promotion_id, decide, by)


async def cancel(promotion_id: int, *, by: Optional[int]) -> Promotion:
    """A version saying the promotion will not happen, its content kept."""

    def decide(current: Optional[Promotion]):
        assert current is not None
        if current.status == CANCELLED:
            raise PromotionConflict(promotion_id, "is already cancelled")
        return CANCELLED, current.data

    return await _write_version(promotion_id, decide, by)


async def restore(promotion_id: int, *, by: Optional[int]) -> Promotion:
    """A version saying a cancelled promotion is planned again, as it was."""

    def decide(current: Optional[Promotion]):
        assert current is not None
        if current.status != CANCELLED:
            raise PromotionConflict(promotion_id, "is not cancelled")
        return PLANNED, current.data

    return await _write_version(promotion_id, decide, by)


# ─── The reads ───────────────────────────────────────────────────────────────

async def _states(at: Optional[datetime], window_from: date, window_to: date,
                  include_cancelled: bool) -> List[Promotion]:
    pool = await _pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(_STATES_SQL, at, window_from, window_to,
                                bool(include_cancelled))
    return [_promotion(row) for row in rows]


async def current(window_from: date, window_to: date,
                  include_cancelled: bool = False) -> List[Promotion]:
    """Every promotion's current state whose days overlap the window, by its
    first day and then its id; the cancelled only when asked for."""
    return await _states(None, window_from, window_to, include_cancelled)


async def as_of(at: datetime, window_from: date, window_to: date,
                include_cancelled: bool = False) -> List[Promotion]:
    """The calendar as it was known at `at`: each promotion's highest version
    recorded then or earlier, whose days overlap the window. A promotion
    first saved after `at` is not in it."""
    if at.tzinfo is None:
        raise ValueError("as_of needs an aware moment")
    return await _states(at, window_from, window_to, include_cancelled)


async def versions(promotion_id: int) -> List[Promotion]:
    """Every version of one promotion, newest first; [] for an unknown id."""
    pool = await _pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(_VERSIONS_SQL, promotion_id)
    return [_promotion(row) for row in rows]


async def latest_recorded_at() -> Optional[datetime]:
    """When any promotion was last saved, or None before the first: what the
    forecast's cache keys on, so an edit shows on the next request."""
    pool = await _pool()
    async with pool.acquire() as conn:
        return await conn.fetchval(_LATEST_STAMP_SQL)


async def campaigns_named() -> Set[str]:
    """Every SMS campaign some version of some promotion names."""
    pool = await _pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(_CAMPAIGNS_NAMED_SQL)
    return {row["sms_campaign"] for row in rows}
