"""web's real `startup_event`, with what it would start outside the process
faked, and a record of what it started.

For the tests of the start itself: the guard on the session signing key
(tests/integration/test_auth_identity.py) and the boot sync that must not end
the start (tests/unit/test_web_boot.py).

Faked: the boot sync (KeyCRM and Postgres), the scheduler (its jobs reach
both), the event subscriptions (process-wide), the forecast model (a
training on a background task). Postgres refuses every connection, so the
one write the start makes beside them, the August campaign's restore, fails
the way it does when Postgres is down. The bot's local store is initialised
in `tmp_path`. The configuration check is the real one.
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import List, Optional
from unittest.mock import AsyncMock

BOOT_SYNC = "boot sync"
SCHEDULER = "scheduler"


def run_startup(monkeypatch, tmp_path, started: Optional[List[str]] = None, *,
                boot_sync_fails_with: Optional[BaseException] = None) -> List[str]:
    """Run `web.main.startup_event()` once and return what it started, in
    order: `BOOT_SYNC` when it asked for the boot sync, `SCHEDULER` when it
    started the scheduler. Whatever the start raises is raised here; pass
    `started` to read the record after a start that raised."""
    import web.main as main
    from bot import store_sqlite
    from core.bot_store import use_bot_store
    from core.store import close_store

    started = [] if started is None else started

    async def boot_sync(**_kwargs):
        started.append(BOOT_SYNC)
        if boot_sync_fails_with is not None:
            raise boot_sync_fails_with

    async def start_scheduler():
        started.append(SCHEDULER)

    monkeypatch.setattr(main, "init_and_sync", boot_sync)
    monkeypatch.setattr(main, "start_scheduler", start_scheduler)
    monkeypatch.setattr(main, "_register_event_handlers", lambda: None)
    monkeypatch.setattr("core.prediction_service.get_prediction_service",
                        lambda: SimpleNamespace(is_ready=True))
    monkeypatch.setattr("core.pg.get_pool",
                        AsyncMock(side_effect=OSError("Postgres is not reached from this test")))
    monkeypatch.delenv("KS_BOT_STORE", raising=False)
    monkeypatch.setattr(store_sqlite, "DB_PATH", tmp_path / "bot.db")

    async def run():
        try:
            await main.startup_event()
        finally:
            await close_store()

    previous = use_bot_store(None)
    try:
        asyncio.run(run())
    finally:
        use_bot_store(previous)
    return started
