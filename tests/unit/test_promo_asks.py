"""The Monday question about promotions (revision 0039), without a database.

Pinned here, each named by the mutation it kills:

- the window is the two weeks from the Monday of the week asked in;
- the button's callback is matched strictly, and fits Telegram's ceiling;
- who is asked: the admins and `KS_PROMO_ASK_RECIPIENTS`, a bad entry
  dropped with a warning;
- the message in three languages: the window numerically, what is planned,
  last week's SMS no promotion names, every name escaped; the two buttons;
- the job: registered daily at 10:05, clear of every window the schedule
  keeps; once a week against its ledger; one render per language; nothing
  recorded when nobody was reached;
- the bot's handler: approved, a recipient, a real callback, the store's
  answer — or nothing recorded; the keyboard goes once used;
- the ledger and the checkins have one writer each.

The server's half — the ledger and a checkin through the bot's adapter on a
real database — is `tests/integration/test_promotions_pg.py`.
"""
from __future__ import annotations

import asyncio
import importlib
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock
from zoneinfo import ZoneInfo

import pytest
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from core import promo_asks
from core.i18n import LANGUAGES, t

KYIV = ZoneInfo("Europe/Kyiv")
MONDAY = date(2026, 11, 23)


# ─── The window and the button ───────────────────────────────────────────────

class TestTheWindow:
    @pytest.mark.parametrize("offset", range(7))
    def test_every_day_of_a_week_asks_from_its_monday(self, offset):
        assert promo_asks.week_start(MONDAY + timedelta(days=offset)) == MONDAY

    def test_it_covers_two_weeks_from_the_monday(self):
        assert promo_asks.period_of(MONDAY) == (MONDAY, date(2026, 12, 6))


class TestTheButton:
    def test_it_round_trips(self):
        data = promo_asks.callback_for(MONDAY)
        assert data == "pask:none:2026-11-23"
        assert promo_asks.parse_callback(data) == (MONDAY, date(2026, 12, 6))

    def test_the_handlers_pattern_matches_what_is_sent(self):
        """The bot registers `CALLBACK_PATTERN`. Kills: a pattern that misses
        the button — a tap that dies in silence."""
        import re

        assert re.match(promo_asks.CALLBACK_PATTERN, promo_asks.callback_for(MONDAY))

    def test_it_fits_telegrams_ceiling(self):
        assert len(promo_asks.callback_for(date(2099, 12, 21)).encode()) <= 64

    @pytest.mark.parametrize("data", [
        "pask:none:2026-11-24",          # a Tuesday: no question starts there
        "pask:none:2026-02-30",          # no such day
        "pask:none:2026-11-23\n",        # what `$` would let through
        "pask:none:2026-11-23:x",
        "pask:yes:2026-11-23",
        "afix:none:2026-11-23",
        "pask:none:1999-12-27",          # before the dashboard's window
        "pask:none:2099-12-28",          # a window ending past it
        "", None, 7,
    ])
    def test_anything_else_is_refused(self, data):
        assert promo_asks.parse_callback(data) is None


# ─── Who is asked ────────────────────────────────────────────────────────────

class TestTheAudience:
    def test_the_admins_alone_by_default(self, monkeypatch):
        monkeypatch.delenv(promo_asks.RECIPIENTS_ENV, raising=False)
        assert promo_asks.audience({20, 10}) == [10, 20]

    def test_the_list_is_added_once(self, monkeypatch):
        monkeypatch.setenv(promo_asks.RECIPIENTS_ENV, " 30 ; 10,40 ")
        assert promo_asks.audience({10, 20}) == [10, 20, 30, 40]

    def test_a_bad_entry_is_dropped_with_a_warning(self, monkeypatch, caplog):
        """Kills: one typo costing everybody the question."""
        monkeypatch.setenv(promo_asks.RECIPIENTS_ENV, "30,@marketer,40")
        with caplog.at_level("WARNING", logger="core.promo_asks"):
            assert promo_asks.audience({10}) == [10, 30, 40]
        assert "@marketer" in caplog.text


# ─── The message ─────────────────────────────────────────────────────────────

def _planned(name, first, last):
    return SimpleNamespace(data=SimpleNamespace(name=name, starts_on=first, ends_on=last))


class TestTheMessage:
    def test_the_question_alone_when_there_is_nothing_to_say(self):
        text = promo_asks.render("uk", MONDAY, date(2026, 12, 6), [], [])
        assert text == "<b>Акції на 23.11 – 06.12.2026?</b>"

    def test_what_is_planned_and_last_weeks_sms(self):
        text = promo_asks.render(
            "uk", MONDAY, date(2026, 12, 6),
            [_planned("Весняний розпродаж", date(2026, 11, 26), date(2026, 11, 28)),
             _planned("Грудень", date(2026, 11, 30), date(2026, 12, 2))],
            [("Black Friday SMS", "BF25"), ("Нагадування", None)])
        assert text.split("\n") == [
            "<b>Акції на 23.11 – 06.12.2026?</b>",
            "",
            "Вже заплановано: «Весняний розпродаж», 26–28.11; «Грудень», 30.11–02.12.",
            "",
            "SMS минулого тижня: «Black Friday SMS» (промокод BF25), «Нагадування». "
            "Якщо це була акція, додайте її.",
        ]

    @pytest.mark.parametrize("lang", LANGUAGES)
    def test_every_language_says_it(self, lang):
        text = promo_asks.render(lang, MONDAY, date(2026, 12, 6),
                                 [_planned("X", MONDAY, MONDAY)], [("Y", "Z")])
        assert "23.11 – 06.12.2026" in text and "23.11" in text and "Z" in text
        assert t("promo_ask.question", lang, window="") != "promo_ask.question"

    def test_names_are_escaped(self):
        """A name is typed by a person and the message is HTML. Kills: a
        promotion called `<b>` turning the question into a parse error — the
        transport would then send it as plain text with the tags showing."""
        text = promo_asks.render("en", MONDAY, date(2026, 12, 6),
                                 [_planned("A & <b>", MONDAY, MONDAY)], [("<i>", "1&2")])
        assert "A &amp; &lt;b&gt;" in text and "&lt;i&gt;" in text and "1&amp;2" in text

    def test_the_dates_are_numeric(self):
        """A month after a day number takes the genitive in Ukrainian and
        Russian (`core.i18n.fmt_window`)."""
        for lang in ("uk", "ru"):
            text = promo_asks.render(lang, MONDAY, date(2026, 12, 6), [], [])
            assert "листопада" not in text and "ноября" not in text

    @pytest.mark.parametrize("first,last,expected", [
        (date(2026, 11, 26), date(2026, 11, 26), "26.11"),
        (date(2026, 11, 26), date(2026, 11, 28), "26–28.11"),
        (date(2026, 12, 30), date(2027, 1, 2), "30.12–02.01"),
    ])
    def test_a_promotions_days(self, first, last, expected):
        assert promo_asks.span(first, last) == expected

    def test_the_answer_says_the_window(self):
        assert promo_asks.recorded_line("uk", MONDAY, date(2026, 12, 6)) == \
            "Записано: акцій на 23.11 – 06.12.2026 немає"


class TestTheKeyboard:
    def test_none_and_add_as_a_web_app(self):
        """[Додати] opens the dashboard as the bot's menu button does, signed
        in, at the calendar. Kills: a plain link, which opens a browser that
        holds no session."""
        markup = promo_asks.keyboard("uk", MONDAY, "https://dash.example/")
        assert markup == {"inline_keyboard": [[
            {"text": "Немає", "callback_data": "pask:none:2026-11-23"},
            {"text": "Додати", "web_app": {"url": "https://dash.example/marketing#promotions"}},
        ]]}

    def test_an_address_telegram_takes_no_web_app_at_is_a_link(self):
        markup = promo_asks.keyboard("en", MONDAY, "http://localhost:8080")
        assert markup["inline_keyboard"][0][1] == {
            "text": "Add", "url": "http://localhost:8080/marketing#promotions"}

    def test_no_dashboard_no_add(self):
        markup = promo_asks.keyboard("ru", MONDAY, "")
        assert markup == {"inline_keyboard": [[
            {"text": "Нет", "callback_data": "pask:none:2026-11-23"}]]}


# ─── The job ─────────────────────────────────────────────────────────────────

def _registered():
    from core.scheduler import BackgroundScheduler, SCHEDULER_TIMEZONE

    s = BackgroundScheduler()
    s._scheduler = AsyncIOScheduler(timezone=SCHEDULER_TIMEZONE)
    asyncio.run(s._register_jobs())
    return s


def _in_kyiv() -> CronTrigger:
    """The registered trigger's fields in Kyiv's zone, as
    `tests/unit/test_capture_ga4.py` reads its own: the process's zone is
    UTC in CI."""
    from core.scheduler import SCHEDULER_TIMEZONE

    job = _registered()._scheduler.get_job("promo_weekly_ask")
    fields = {f.name: str(f) for f in job.trigger.fields}
    return CronTrigger(hour=fields["hour"], minute=fields["minute"],
                       day=fields["day"], day_of_week=fields["day_of_week"],
                       timezone=SCHEDULER_TIMEZONE)


class TestTheSchedule:
    def test_it_is_registered_daily_at_1005(self):
        job = _registered()._scheduler.get_job("promo_weekly_ask")
        assert job is not None and isinstance(job.trigger, CronTrigger)
        fields = {f.name: str(f) for f in job.trigger.fields}
        assert (fields["hour"], fields["minute"], fields["day"], fields["day_of_week"]) == (
            "10", "5", "*", "*")
        assert job.func.__wrapped__.__name__ == "_run_promo_weekly_ask"

    def test_a_year_of_firings_misses_every_busy_window(self):
        """Read in Kyiv across both clock changes, against the windows the
        GA4 capture and the goal freeze keep clear. Kills: a time moved into
        one of them, or into the DST hours."""
        from core.scheduler import SCHEDULER_TIMEZONE
        from tests.unit.test_goal_freeze import BUSY

        trigger = _in_kyiv()
        at = datetime(2026, 3, 1, tzinfo=SCHEDULER_TIMEZONE)
        fired = []
        for _ in range(400):
            at = trigger.get_next_fire_time(None, at + timedelta(minutes=1))
            fired.append(at.astimezone(SCHEDULER_TIMEZONE))
        starts = {f.hour * 60 + f.minute for f in fired}
        assert starts == {10 * 60 + 5}
        assert not [w for w in BUSY for m in starts if w[0] <= m <= w[1]]
        assert len({f.date() for f in fired}) == 400


@pytest.fixture
def job(monkeypatch):
    """`run_weekly_ask` over an in-memory ledger, calendar and transport."""
    state: Dict[str, Any] = {"asked": set(), "marked": [], "sent": [], "delivered": 1,
                             "planned": [], "named": set(), "sms": [],
                             "languages": {}}
    monkeypatch.setenv("KS_PG_DSN", "postgresql://nobody@127.0.0.1:1/none")
    monkeypatch.delenv(promo_asks.RECIPIENTS_ENV, raising=False)

    async def pool():
        return "pool"

    async def already(_pool, week):
        return week in state["asked"]

    async def mark(_pool, week, sent_at, delivered):
        state["marked"].append((week, sent_at, delivered))

    async def recent(_pool, since, named):
        state["since"] = since
        return [c for c in state["sms"] if c[0] not in named]

    async def current(first, last, include_cancelled=False):
        state["window"] = (first, last)
        return state["planned"]

    async def named():
        return state["named"]

    async def send(text, parse_mode="HTML", *, token=None, chat_ids=None, reply_markup=None):
        state["sent"].append({"text": text, "chat_ids": list(chat_ids or ()),
                              "reply_markup": reply_markup})
        return state["delivered"] * len(list(chat_ids or ()))

    def languages(ids, defaults=None):
        return {uid: state["languages"].get(uid, (defaults or {}).get(uid, "en"))
                for uid in ids}

    monkeypatch.setattr(promo_asks, "_pool", pool)
    monkeypatch.setattr(promo_asks, "_already_asked", already)
    monkeypatch.setattr(promo_asks, "_mark_asked", mark)
    monkeypatch.setattr(promo_asks, "_recent_campaigns", recent)
    monkeypatch.setattr("core.promotions.current", current)
    monkeypatch.setattr("core.promotions.campaigns_named", named)
    monkeypatch.setattr("core.telegram_alerts.send_admin_message_http", send)
    monkeypatch.setattr("core.bot_prefs.read_user_languages", languages)
    config = importlib.import_module("core.config")
    monkeypatch.setattr(config, "ADMIN_USER_IDS", {10, 20})
    monkeypatch.setattr(config, "DASHBOARD_URL", "https://dash.example")
    return state


NOW = datetime(2026, 11, 25, 10, 5, tzinfo=KYIV)       # a Wednesday


class TestTheJob:
    @pytest.mark.asyncio
    async def test_it_asks_the_week_from_its_monday_once_and_records_it(self, job):
        result = await promo_asks.run_weekly_ask(now=NOW)
        assert result == {"week": "2026-11-23", "sent": True, "delivered": 2,
                          "recipients": 2, "planned": 0, "sms": 0}
        assert job["window"] == (MONDAY, date(2026, 12, 6))
        assert [(w, d) for w, _at, d in job["marked"]] == [(MONDAY, 2)]
        assert job["since"] == NOW - timedelta(days=7)

    @pytest.mark.asyncio
    async def test_a_week_asked_already_is_quiet(self, job):
        """Kills: a question every day of the week."""
        job["asked"].add(MONDAY)
        result = await promo_asks.run_weekly_ask(now=NOW)
        assert result == {"week": "2026-11-23", "sent": False, "reason": "already_asked"}
        assert job["sent"] == [] and job["marked"] == []

    @pytest.mark.asyncio
    async def test_one_render_per_language_and_a_named_reader_added(self, job, monkeypatch):
        """Admins in English, the named list in Ukrainian, a stored choice
        over either — `core.bot_prefs.default_language_for`."""
        monkeypatch.setenv(promo_asks.RECIPIENTS_ENV, "30,40")
        job["languages"] = {40: "ru"}
        await promo_asks.run_weekly_ask(now=NOW)
        by_language = {s["text"].split("?")[0]: s["chat_ids"] for s in job["sent"]}
        assert by_language == {"<b>Promotions for 23.11 – 06.12.2026": [10, 20],
                               "<b>Акції на 23.11 – 06.12.2026": [30],
                               "<b>Акции на 23.11 – 06.12.2026": [40]}
        for sent in job["sent"]:
            row = sent["reply_markup"]["inline_keyboard"][0]
            assert row[0]["callback_data"] == "pask:none:2026-11-23"
            assert row[1]["web_app"]["url"] == "https://dash.example/marketing#promotions"

    @pytest.mark.asyncio
    async def test_it_names_what_is_planned_and_the_sms_no_promotion_names(self, job):
        job["planned"] = [_planned("Знижки", date(2026, 11, 27), date(2026, 11, 29))]
        job["sms"] = [("BF", "BF25"), ("Уже в акції", None)]
        job["named"] = {"Уже в акції"}
        result = await promo_asks.run_weekly_ask(now=NOW)
        (text,) = {s["text"] for s in job["sent"]}
        assert "“Знижки”, 27–29.11" in text
        assert "“BF” (promo code BF25)" in text and "Уже в акції" not in text
        assert (result["planned"], result["sms"]) == (1, 1)

    @pytest.mark.asyncio
    async def test_nobody_reached_records_nothing(self, job):
        """`KS_ALERTS_DISABLED`, Telegram down: the next day asks again.
        Kills: a ledger row for a question nobody saw."""
        job["delivered"] = 0
        result = await promo_asks.run_weekly_ask(now=NOW)
        assert result == {"week": "2026-11-23", "sent": False, "reason": "not_delivered"}
        assert job["marked"] == []

    @pytest.mark.asyncio
    async def test_without_postgres_it_stands_down(self, job, monkeypatch):
        monkeypatch.delenv("KS_PG_DSN")
        assert await promo_asks.run_weekly_ask(now=NOW) == {"skipped": "KS_PG_DSN is not set"}
        assert job["sent"] == []

    @pytest.mark.asyncio
    async def test_the_scheduler_runs_it(self, job, monkeypatch):
        from core.scheduler import BackgroundScheduler

        monkeypatch.setattr(promo_asks, "run_weekly_ask",
                            AsyncMock(return_value={"sent": True}))
        assert await BackgroundScheduler()._run_promo_weekly_ask() == {"sent": True}


# ─── The bot's handler ──────────────────────────────────────────────────────

ADMIN, NAMED, STRANGER = 101, 202, 303


@pytest.fixture
def bot(monkeypatch):
    """`promo_ask_callback` as an approved person, with a store recording
    what it is asked to write."""
    from bot import handlers_legacy as h

    recorded: List[dict] = []

    class Asks:
        ok = True
        raises = None

        def available(self):
            return self.ok

        def record_none(self, **kwargs):
            if self.raises:
                raise self.raises
            recorded.append(kwargs)
            return 77

    store = SimpleNamespace(promo_asks=Asks())
    status = {"status": h.database.STATUS_APPROVED}
    monkeypatch.setattr(h, "is_admin", lambda uid: uid == ADMIN)
    monkeypatch.setattr(h, "ADMIN_USER_IDS", {ADMIN})
    monkeypatch.setattr(h.database, "get_user_auth_status", lambda uid: status)
    monkeypatch.setattr(h.database, "is_user_authorized", lambda uid: True)
    monkeypatch.setattr(h.database, "update_last_activity", lambda uid: None)
    monkeypatch.setattr(h, "_lang", lambda update: "uk")
    monkeypatch.setattr("core.bot_store.get_bot_store", lambda: store)
    monkeypatch.setenv(promo_asks.RECIPIENTS_ENV, str(NAMED))
    return SimpleNamespace(h=h, store=store, recorded=recorded, status=status)


def _tap(user_id, data="pask:none:2026-11-23", text="<b>Акції на 23.11 – 06.12.2026?</b>"):
    from telegram import Chat, Message

    message = Message(message_id=1, date=datetime(2026, 11, 23, 8, 5, tzinfo=timezone.utc),
                      chat=Chat(id=user_id, type="private"),
                      text="Акції на 23.11 – 06.12.2026?")
    query = MagicMock()
    query.data = data
    query.from_user = MagicMock(id=user_id)
    query.message = message
    query.answer = AsyncMock()
    query.edit_message_text = AsyncMock()
    query.edit_message_reply_markup = AsyncMock()
    update = MagicMock()
    update.callback_query = query
    update.effective_user = MagicMock(id=user_id, username="u", first_name="U", last_name="")
    update.message = None
    return update, query


class TestTheHandler:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("who", [ADMIN, NAMED])
    async def test_a_recipient_records_none_and_the_keyboard_goes(self, bot, who):
        update, query = _tap(who)
        await bot.h.promo_ask_callback(update, None)
        assert bot.recorded == [{"period_from": MONDAY, "period_to": date(2026, 12, 6),
                                 "by_user_id": who}]
        query.answer.assert_awaited_once_with("Записано: акцій на 23.11 – 06.12.2026 немає")
        text = query.edit_message_text.await_args.args[0]
        assert text == ("Акції на 23.11 – 06.12.2026?\n\n"
                        "✅ Записано: акцій на 23.11 – 06.12.2026 немає")
        assert query.edit_message_text.await_args.kwargs["reply_markup"] is None

    @pytest.mark.asyncio
    async def test_an_approved_person_who_was_not_asked_records_nothing(self, bot):
        """A forwarded question. Kills: anybody approved answering for the
        people the owner listed."""
        update, query = _tap(STRANGER)
        await bot.h.promo_ask_callback(update, None)
        assert bot.recorded == []
        assert query.answer.await_args.kwargs.get("show_alert") is True
        query.edit_message_text.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_recipient_whose_access_is_pending_records_nothing(self, bot):
        """`@authorized` before anything: the list names a Telegram id, and
        approval is still the bot's to give."""
        bot.status["status"] = bot.h.database.STATUS_PENDING
        update, query = _tap(NAMED)
        await bot.h.promo_ask_callback(update, None)
        assert bot.recorded == []

    @pytest.mark.asyncio
    async def test_a_callback_it_did_not_send_records_nothing(self, bot):
        update, query = _tap(ADMIN, data="pask:none:2026-11-24")
        await bot.h.promo_ask_callback(update, None)
        assert bot.recorded == []
        assert query.answer.await_args.args[0] == t("promo_ask.unknown", "uk")

    @pytest.mark.asyncio
    async def test_sqlite_says_so_instead_of_recording_into_the_void(self, bot):
        bot.store.promo_asks.ok = False
        update, query = _tap(ADMIN)
        await bot.h.promo_ask_callback(update, None)
        assert bot.recorded == []
        assert query.answer.await_args.args[0] == t("promo_ask.unavailable", "uk")

    @pytest.mark.asyncio
    async def test_a_failed_write_keeps_the_button(self, bot):
        """Kills: the keyboard taken away though nothing was recorded."""
        bot.store.promo_asks.raises = RuntimeError("pool closed")
        update, query = _tap(ADMIN)
        await bot.h.promo_ask_callback(update, None)
        assert query.answer.await_args.args[0] == t("promo_ask.failed", "uk")
        query.edit_message_text.assert_not_awaited()
        query.edit_message_reply_markup.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_message_it_cannot_edit_still_loses_its_keyboard(self, bot):
        update, query = _tap(ADMIN)
        query.edit_message_text.side_effect = RuntimeError("message is too old")
        await bot.h.promo_ask_callback(update, None)
        assert len(bot.recorded) == 1
        query.edit_message_reply_markup.assert_awaited_once_with(reply_markup=None)

    def test_the_bot_registers_it_by_the_pattern(self):
        """Kills: the handler written and never registered, or registered on
        a pattern of its own."""
        import ast
        from pathlib import Path

        source = (Path(__file__).resolve().parents[2] / "bot" / "main.py").read_text()
        tree = ast.parse(source)

        def handler_of(call):
            """The handler a `CallbackQueryHandler(...)` registers, through
            the `cast(Any, ...)` the type checker needs."""
            first = call.args[0] if call.args else None
            if isinstance(first, ast.Call) and getattr(first.func, "id", "") == "cast":
                first = first.args[1]
            return first

        registered = [
            node for node in ast.walk(tree)
            if isinstance(node, ast.Call) and getattr(node.func, "id", "") == "CallbackQueryHandler"
            and isinstance(handler_of(node), ast.Attribute)
            and handler_of(node).attr == "promo_ask_callback"]
        assert len(registered) == 1
        (call,) = registered
        assert {k.arg: getattr(k.value, "id", None) for k in call.keywords} == {
            "pattern": "PROMO_ASK_PATTERN"}
        assert "from core.promo_asks import CALLBACK_PATTERN as PROMO_ASK_PATTERN" in source


class TestTheAdapters:
    def test_sqlite_has_no_answer_to_give(self):
        from bot.store_sqlite import SqliteBotStore, SqlitePromoAsks

        asks = SqliteBotStore().promo_asks
        assert isinstance(asks, SqlitePromoAsks)
        assert asks.available() is False
        assert asks.record_none(period_from=MONDAY, period_to=MONDAY, by_user_id=1) is None

    def test_both_satisfy_the_port(self):
        from bot.store_postgres import PostgresPromoAsks
        from bot.store_sqlite import SqlitePromoAsks
        from core.bot_store import PromoAsks

        assert isinstance(SqlitePromoAsks(), PromoAsks)
        assert isinstance(PostgresPromoAsks(lambda c: c, None), PromoAsks)

    @pytest.mark.asyncio
    async def test_the_statement_is_the_one_the_adapter_runs(self):
        conn = MagicMock()
        conn.fetchval = AsyncMock(return_value=5)
        assert await promo_asks.write_none_answer(conn, period_from=MONDAY,
                                                  period_to=date(2026, 12, 6), by_user_id=9) == 5
        conn.fetchval.assert_awaited_once_with(
            promo_asks.RECORD_NONE_SQL, MONDAY, date(2026, 12, 6), 9)


# ─── One writer each ────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def walk():
    from tests.unit.test_write_chains import _CAPTURE_WALKED, _Walk

    return _Walk(_CAPTURE_WALKED)


class TestOneWriterEach:
    def test_the_checkins_are_written_by_the_shared_statement_from_the_bot(self, walk):
        writers = {key for key, targets in walk.writers().items()
                   if "app.promotion_checkins" in targets}
        assert writers == {("core/promo_asks.py", "write_none_answer")}
        assert walk.callers.get(("core/promo_asks.py", "write_none_answer")) == {
            ("bot/store_postgres.py", "_record_none")}

    def test_the_ledger_is_written_by_the_job_alone(self, walk):
        writers = {key for key, targets in walk.writers().items()
                   if "app.promotion_asks" in targets}
        assert writers == {("core/promo_asks.py", "_mark_asked")}
        assert walk.callers.get(("core/promo_asks.py", "_mark_asked")) == {
            ("core/promo_asks.py", "run_weekly_ask")}
        assert walk.callers.get(("core/promo_asks.py", "run_weekly_ask")) == {
            ("core/scheduler.py", "_run_promo_weekly_ask")}
