"""An outbox in place of the digest's Telegram transport."""
from __future__ import annotations

import sys
import types


class _Outbox:
    """Stands in for bot.main.send_admin_message, which the job imports late."""

    def __init__(self, fail: bool = False):
        self.messages: list[str] = []
        self.fail = fail

    async def __call__(self, text, key=None, **kw):
        if self.fail:
            raise RuntimeError("telegram is down")
        self.messages.append(text)
        return 1


def _install_outbox(monkeypatch, *, fail: bool = False) -> _Outbox:
    box = _Outbox(fail=fail)
    module = types.ModuleType("bot.main")
    module.send_admin_message = box
    monkeypatch.setitem(sys.modules, "bot.main", module)
    return box
