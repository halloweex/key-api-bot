"""The bot started as its container starts it, and a tap routed the way
Telegram's dispatcher routes it.

`start_the_bot` runs `bot.main.main()` up to the moment it would poll
Telegram, on an Application that only keeps what is registered on it — the
shape `tests/unit/test_handlers_facade.py` starts it in, copied here because
no test module imports another. `handler_for` asks the registered handlers,
group by group and in order, which one would take an update: the first whose
`check_update` answers, as `Application.process_update` picks it.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, List, Optional


class _JobQueue:
    def __init__(self):
        self.jobs = []

    def run_once(self, callback, when, **kwargs):
        self.jobs.append(callback)

    def run_daily(self, callback, time, **kwargs):
        self.jobs.append(callback)

    def run_repeating(self, callback, interval, **kwargs):
        self.jobs.append(callback)


class StartedBot:
    """What `Application.builder().token(...).build()` hands `main()`: it
    keeps the handlers registered on it, by group, and polls nothing."""

    def __init__(self):
        self.handlers: Dict[int, List] = {}
        self.job_queue = _JobQueue()
        self.polling = None

    def add_handler(self, handler, group=0):
        self.handlers.setdefault(group, []).append(handler)

    def run_polling(self, **kwargs):
        self.polling = kwargs


class _Builder:
    def __init__(self, application):
        self._application = application

    def token(self, _token):
        return self

    def build(self):
        return self._application


def start_the_bot(monkeypatch, tmp_path) -> StartedBot:
    """`bot.main.main()`, run to the start of polling; the Application it
    registered its handlers on. The bot's local store is in `tmp_path`, and
    what `main()` installs for the life of the process is put back after."""
    from telegram.ext import Application

    import bot.main as main
    from bot import handlers_legacy, store_sqlite
    from core.bot_store import use_bot_store

    application = StartedBot()

    def _not_this_one(*_a, **_k):
        raise AssertionError("main() built an Application this test did not hand it")

    monkeypatch.setattr(Application, "builder", lambda: _Builder(application))
    monkeypatch.setattr(Application, "run_polling", _not_this_one)
    # Read by `validate_config` at the call; a start that fails validation
    # returns before it registers anything.
    monkeypatch.setenv("ADMIN_USER_IDS", "1")
    monkeypatch.delenv("KS_BOT_STORE", raising=False)
    monkeypatch.setattr(main, "_application", main._application)
    monkeypatch.setattr(handlers_legacy, "report_service", handlers_legacy.report_service)
    monkeypatch.setattr(store_sqlite, "DB_PATH", tmp_path / "bot.db")
    previous = use_bot_store(store_sqlite.SqliteBotStore())
    try:
        main.main()
    finally:
        use_bot_store(previous)
    assert application.polling is not None, "main() returned before it started polling"
    return application


def tap(data: str, *, user_id: int = 1):
    """The update Telegram sends when `user_id` taps an inline button
    carrying `data`, in their private chat with the bot."""
    from telegram import CallbackQuery, Chat, Message, Update, User

    user = User(id=user_id, first_name="Test", is_bot=False)
    message = Message(message_id=1, date=datetime.now(timezone.utc),
                      chat=Chat(id=user_id, type="private"))
    return Update(update_id=1, callback_query=CallbackQuery(
        id="1", from_user=user, chat_instance="1", data=data, message=message))


def handler_for(application: StartedBot, update) -> Optional[object]:
    """The first handler that would take `update`, asked in the order the
    dispatcher asks them, or None when none would."""
    for group in sorted(application.handlers):
        for handler in application.handlers[group]:
            check = handler.check_update(update)
            if check is not None and check is not False:
                return handler
    return None
