"""`bot/main.py` reaches its handlers through the facade, so the facade decides.

`bot/handlers/__init__.py` re-exports `bot/handlers_legacy.py` and adds nothing,
which makes it easy to forget: a handler written in the legacy module works
under every test that imports that module directly, and does not exist at all
for `bot/main.py`, which says `handlers.<name>`.

That is not theoretical. On 2026-09-11 `alert_action_callback` shipped without
its re-export and the bot crash-looped in production on

    AttributeError: module 'bot.handlers' has no attribute 'alert_action_callback'

— after a green suite, because every test for it imported `bot.handlers_legacy`.
So this starts the bot: `main()` runs as the container runs it, up to the
moment it would start polling Telegram, on an Application that only keeps what
is registered on it. A name the facade lacks fails the start here exactly as
it failed it there.

What a start does not look up is a name used only inside a job's body — the
session sweep's `handlers.cleanup_expired_sessions` — which is resolved when
the job first runs, ten minutes in, and would not stop the bot starting.
"""
import bot.handlers as facade


class _JobQueue:
    def __init__(self):
        self.jobs = []

    def run_once(self, callback, when, **kwargs):
        self.jobs.append(callback)

    def run_daily(self, callback, time, **kwargs):
        self.jobs.append(callback)

    def run_repeating(self, callback, interval, **kwargs):
        self.jobs.append(callback)


class _Application:
    """What `Application.builder().token(...).build()` hands `main()`: it keeps
    what is registered on it, and polls nothing."""

    def __init__(self):
        self.handlers = []
        self.job_queue = _JobQueue()
        self.polling = None

    def add_handler(self, handler, group=0):
        self.handlers.append(handler)

    def run_polling(self, **kwargs):
        self.polling = kwargs


class _Builder:
    def __init__(self, application):
        self._application = application

    def token(self, _token):
        return self

    def build(self):
        return self._application


def _start_the_bot(monkeypatch, tmp_path) -> _Application:
    """`bot.main.main()`, run to the start of polling; the Application it built."""
    from telegram.ext import Application

    import bot.main as main
    from bot import handlers_legacy, store_sqlite
    from core.bot_store import use_bot_store

    application = _Application()

    def _not_this_one(*_a, **_k):
        raise AssertionError("main() built an Application this test did not hand it")

    monkeypatch.setattr(Application, "builder", lambda: _Builder(application))
    # Should a start ever build its Application another way, it must not poll
    # Telegram from a test: the real one refuses, and the start fails below.
    monkeypatch.setattr(Application, "run_polling", _not_this_one)
    # The one setting `validate_config` reads at the call, not at import; a
    # start that fails validation returns before it registers anything.
    monkeypatch.setenv("ADMIN_USER_IDS", "1")
    # What `main()` installs for the life of the process, put back afterwards.
    monkeypatch.setattr(main, "_application", main._application)
    monkeypatch.setattr(handlers_legacy, "report_service", handlers_legacy.report_service)
    monkeypatch.setattr(store_sqlite, "DB_PATH", tmp_path / "bot.db")
    previous = use_bot_store(store_sqlite.SqliteBotStore())
    try:
        main.main()
    finally:
        use_bot_store(previous)
    return application


def test_the_bot_starts(monkeypatch, tmp_path):
    """Every handler `main()` registers is looked up on the facade as it is
    registered, so a missing re-export raises out of `main()` — the start
    that crash-looped on 2026-09-11 — and fails this test with it."""
    application = _start_the_bot(monkeypatch, tmp_path)

    assert application.polling is not None, (
        "main() returned before it started polling, so nothing was registered")
    assert application.handlers, "a start that registers nothing answers nothing"


def test_the_facade_exports_what_it_declares():
    """`__all__` and the import list are two lists of the same thing, and a
    name in one only is the same bug wearing a different hat."""
    missing = [n for n in getattr(facade, "__all__", []) if not hasattr(facade, n)]
    assert not missing, f"__all__ names what is not imported: {missing}"
