"""The Monday question: which promotions do the next two weeks hold?

People enter what they are asked for and see used. So the bot asks every
week — admins plus a named list — about the two weeks from that week's
Monday, lists what is planned in them already and the SMS campaigns of the
last seven days no promotion names, and offers two buttons: [Немає], which
is an answer and is stored (`app.promotion_checkins`), and [Додати], which
opens the dashboard's calendar (`/marketing#promotions`). The owner approved
it on 2026-11-19 with the promotions calendar (revision 0039,
`core/promotions.py`).

ONCE A WEEK FROM A DAILY JOB

The job `promo_weekly_ask` ticks daily at `RUN_AT` (10:05 Kyiv, clear of the
windows the schedule keeps) and asks about the ISO week the day is in, once:
`app.promotion_asks` records each week the question reached somebody, as
`weekly_report_sends` does for the sales report. A Monday the process was
down for becomes a Tuesday question, never a week nobody was asked. The record
is written after the delivery; a delivery whose record then fails is asked
again the next day, which is the residual, and a run that reached nobody
records nothing and tries again tomorrow. `KS_ALERTS_DISABLED` silences it
with the rest of the outgoing Telegram (`send_admin_message_http`).

WHO IS ASKED

The admins plus `KS_PROMO_ASK_RECIPIENTS`, a comma-separated list of Telegram
ids, empty by default — `core.traffic_report.audience`'s shape and reason: a
list typed by a person, never a rule derived from who holds the marketing
tab. A malformed entry is dropped with a warning. Each is written to in their
language by the system rule (`core.bot_prefs.default_language_for` and the
stored choice), one render per language. `tests/unit/test_alert_recipients.py`
names this send as the third `chat_ids` override that widens an audience.

WHAT THE BUTTON CARRIES

`pask:none:<period_from>` — a colon, for `afix:`'s and `atab:`'s reason —
matched strictly by `parse_callback`: the shape, a real Monday, nothing else.
The tap arrives at the bot, which writes the answer through its store port
(`core.bot_store.PromoAsks`) with `write_none_answer` below: the statement
lives here, once, for any adapter. Only an approved recipient may answer
(`bot/handlers_legacy.promo_ask_callback`).

Dates are numeric (`23.11 – 06.12.2026`), as everywhere in the bot's
messages: a month after a day number takes the genitive in Ukrainian and
Russian, and `core.i18n.fmt_window` cannot decline one wrong.
"""
from __future__ import annotations

import html
import logging
import os
import re
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from core.duckdb_constants import DEFAULT_TZ
from core.filters import EARLIEST_DATE, LATEST_DATE
from core.i18n import fmt_window, t

logger = logging.getLogger(__name__)

# When the daily tick fires, in Kyiv: after the 09:00 digest and the 09:30 and
# 09:45 reports, outside 08:55-09:55 and every other window the schedule keeps
# clear (`tests/unit/test_promo_asks.py` reads a year of firings).
RUN_AT = time(10, 5)

# The question covers the two weeks from the Monday of the week it is asked in.
PERIOD_DAYS = 14

# The SMS campaigns the question names: sent or frozen this many days back.
SMS_LOOKBACK_DAYS = 7

# Where [Додати] opens the dashboard: the calendar's section on /marketing.
ADD_PATH = "/marketing#promotions"

RECIPIENTS_ENV = "KS_PROMO_ASK_RECIPIENTS"

CALLBACK_PREFIX = "pask"
# For the bot's `CallbackQueryHandler`; `parse_callback` is the strict reader.
CALLBACK_PATTERN = r"^pask:none:\d{4}-\d{2}-\d{2}$"
_CALLBACK = re.compile(r"pask:none:(\d{4}-\d{2}-\d{2})")
# Telegram's ceiling on callback_data, as `core.alert_actions` checks it.
CALLBACK_MAX_BYTES = 64

# The answer the bot records for [Немає]. One statement for any adapter.
RECORD_NONE_SQL = (
    "INSERT INTO app.promotion_checkins (period_from, period_to, answer, answered_by) "
    "VALUES ($1, $2, 'none', $3) RETURNING id"
)

_ASKED_SQL = "SELECT 1 FROM app.promotion_asks WHERE week_start = $1"

# The first record of a week wins: a manual trigger beside the tick cannot
# move it.
_MARK_ASKED_SQL = (
    "INSERT INTO app.promotion_asks (week_start, sent_at, delivered) "
    "VALUES ($1, $2, $3) ON CONFLICT (week_start) DO NOTHING"
)

_RECENT_SMS_SQL = (
    "SELECT campaign, promocode FROM app.sms_campaigns "
    "WHERE exported_at >= $1 OR sent_at >= $1 "
    "ORDER BY COALESCE(sent_at, exported_at), campaign"
)


# ─── The window and the button ───────────────────────────────────────────────

def week_start(day: date) -> date:
    """The Monday of `day`'s ISO week."""
    return day - timedelta(days=day.isoweekday() - 1)


def period_of(monday: date) -> Tuple[date, date]:
    """The two weeks a question asked in `monday`'s week covers."""
    return monday, monday + timedelta(days=PERIOD_DAYS - 1)


def callback_for(period_from: date) -> str:
    """[Немає]'s callback_data, or raise: a keyboard Telegram would refuse
    costs the whole message, not the button."""
    data = f"{CALLBACK_PREFIX}:none:{period_from.isoformat()}"
    if len(data.encode("utf-8")) > CALLBACK_MAX_BYTES:
        raise ValueError(f"callback_data too long: {data!r}")
    return data


def parse_callback(data: Any) -> Optional[Tuple[date, date]]:
    """`pask:none:2026-11-23` → (2026-11-23, 2026-12-06), or None.

    Strict: the whole string, a real date, a Monday, inside the dashboard's
    date window. Anything else did not come from `callback_for`, and the bot
    says so rather than recording a window nobody was asked about."""
    if not isinstance(data, str):
        return None
    match = _CALLBACK.fullmatch(data)
    if match is None:
        return None
    try:
        start = date.fromisoformat(match.group(1))
    except ValueError:
        return None
    if start.isoweekday() != 1:
        return None
    end = start + timedelta(days=PERIOD_DAYS - 1)
    if start < EARLIEST_DATE or end > LATEST_DATE:
        return None
    return start, end


def add_url(dashboard_url: Optional[str]) -> Optional[str]:
    """Where [Додати] goes, or None without a dashboard address."""
    base = (dashboard_url or "").strip().rstrip("/")
    return f"{base}{ADD_PATH}" if base else None


def keyboard(lang: str, period_from: date, dashboard_url: Optional[str]) -> Dict[str, Any]:
    """[Немає] and [Додати], as the Bot API's `reply_markup`.

    [Додати] opens the dashboard as the bot's menu button does, as a Web App
    (`MenuButtonWebApp`), so the calendar opens inside Telegram, signed in.
    Telegram takes a Web App only over https; any other address is a plain
    link, and no address no button."""
    row: List[Dict[str, Any]] = [
        {"text": t("promo_ask.btn_none", lang), "callback_data": callback_for(period_from)},
    ]
    url = add_url(dashboard_url)
    if url:
        button: Dict[str, Any] = {"text": t("promo_ask.btn_add", lang)}
        if url.startswith("https://"):
            button["web_app"] = {"url": url}
        else:
            button["url"] = url
        row.append(button)
    return {"inline_keyboard": [row]}


# ─── Who is asked ────────────────────────────────────────────────────────────

def extra_recipients() -> List[int]:
    """Telegram ids from `KS_PROMO_ASK_RECIPIENTS`, comma-separated.

    A value that is not a number is dropped with a warning rather than
    raising: one bad character must not cost everybody else the question."""
    out: List[int] = []
    for chunk in os.getenv(RECIPIENTS_ENV, "").replace(";", ",").split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        try:
            out.append(int(chunk))
        except ValueError:
            logger.warning("%s: %r is not a Telegram id, skipping", RECIPIENTS_ENV, chunk)
    return out


def audience(admin_ids: Iterable[int]) -> List[int]:
    """Everyone the question is written to, in a stable order: the admins,
    then the named list."""
    ids = sorted(int(a) for a in admin_ids) + extra_recipients()
    return list(dict.fromkeys(ids))


# ─── The message ─────────────────────────────────────────────────────────────

def span(first: date, last: date) -> str:
    """A promotion's days, numerically: `26.11`, `26–28.11`, `30.11–02.12`."""
    if first == last:
        return first.strftime("%d.%m")
    if (first.year, first.month) == (last.year, last.month):
        return f"{first.day:02d}–{last.strftime('%d.%m')}"
    return f"{first.strftime('%d.%m')}–{last.strftime('%d.%m')}"


def render(
    lang: str,
    period_from: date,
    period_to: date,
    planned: Sequence[Any],
    campaigns: Sequence[Tuple[str, Optional[str]]],
) -> str:
    """The question in `lang`, as HTML: the window, then what is planned in
    it already (`core.promotions.Promotion`s), then last week's SMS campaigns
    no promotion names, as (campaign, promocode)."""
    esc = html.escape
    lines = [f"<b>{esc(t('promo_ask.question', lang, window=fmt_window(period_from, period_to)))}</b>"]
    if planned:
        items = "; ".join(
            t("promo_ask.planned_item", lang, name=esc(p.data.name),
              dates=span(p.data.starts_on, p.data.ends_on))
            for p in planned)
        lines += ["", t("promo_ask.planned", lang, items=items)]
    if campaigns:
        items = ", ".join(
            t("promo_ask.sms_item_code", lang, name=esc(name), code=esc(code)) if code
            else t("promo_ask.sms_item", lang, name=esc(name))
            for name, code in campaigns)
        lines += ["", t("promo_ask.sms", lang, items=items)]
    return "\n".join(lines)


def recorded_line(lang: str, period_from: date, period_to: date) -> str:
    """What the bot says once [Немає] is recorded."""
    return t("promo_ask.recorded_none", lang, window=fmt_window(period_from, period_to))


# ─── The statements ──────────────────────────────────────────────────────────

async def write_none_answer(conn, *, period_from: date, period_to: date,
                            by_user_id: Optional[int]) -> int:
    """Record that `by_user_id` said no promotion is planned in the window.
    Returns the answer's id. Run by the bot's adapter on its own connection."""
    return int(await conn.fetchval(RECORD_NONE_SQL, period_from, period_to, by_user_id))


async def _pool():
    from core.pg import get_pool, require_revision

    pool = await get_pool()
    await require_revision()
    return pool


async def _already_asked(pool, week: date) -> bool:
    async with pool.acquire() as conn:
        return await conn.fetchval(_ASKED_SQL, week) is not None


async def _mark_asked(pool, week: date, sent_at: datetime, delivered: int) -> None:
    """The ledger's one writer: the week asked, once."""
    async with pool.acquire() as conn:
        await conn.execute(_MARK_ASKED_SQL, week, sent_at, delivered)


async def _recent_campaigns(pool, since: datetime,
                            named: Set[str]) -> List[Tuple[str, Optional[str]]]:
    """SMS campaigns sent or frozen since `since` that no promotion names."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(_RECENT_SMS_SQL, since)
    return [(row["campaign"], row["promocode"]) for row in rows
            if row["campaign"] not in named]


# ─── The job ─────────────────────────────────────────────────────────────────

async def run_weekly_ask(*, now: Optional[datetime] = None) -> Dict[str, Any]:
    """Ask about the two weeks from this ISO week's Monday, once a week.

    Returns what it did, for `/api/jobs`: `sent`, and otherwise the `reason`
    (`already_asked`, `not_delivered`). Stands down without `KS_PG_DSN`."""
    if not os.getenv("KS_PG_DSN", "").strip():
        return {"skipped": "KS_PG_DSN is not set"}
    from core import promotions
    from core.bot_prefs import default_language_for, group_by_language
    from core.config import ADMIN_USER_IDS, DASHBOARD_URL
    from core.telegram_alerts import send_admin_message_http

    at = (now or datetime.now(DEFAULT_TZ)).astimezone(DEFAULT_TZ)
    week = week_start(at.date())
    period_from, period_to = period_of(week)
    result: Dict[str, Any] = {"week": week.isoformat()}

    pool = await _pool()
    if await _already_asked(pool, week):
        return {**result, "sent": False, "reason": "already_asked"}

    planned = await promotions.current(period_from, period_to)
    campaigns = await _recent_campaigns(
        pool, at - timedelta(days=SMS_LOOKBACK_DAYS), await promotions.campaigns_named())

    # The admins and whoever the owner listed, each in their own language.
    readers = audience(ADMIN_USER_IDS)
    defaults = {uid: default_language_for(uid, ADMIN_USER_IDS) for uid in readers}
    delivered = 0
    for lang, recipients in group_by_language(readers, defaults).items():
        delivered += await send_admin_message_http(
            render(lang, period_from, period_to, planned, campaigns),
            chat_ids=recipients,
            reply_markup=keyboard(lang, period_from, DASHBOARD_URL),
        )
    if not delivered:
        logger.warning("Promotions question for %s reached nobody", week)
        return {**result, "sent": False, "reason": "not_delivered"}

    await _mark_asked(pool, week, datetime.now(timezone.utc), delivered)
    result.update(sent=True, delivered=delivered, recipients=len(readers),
                  planned=len(planned), sms=len(campaigns))
    logger.info("Promotions question sent", extra=result)
    return result
