"""The bot's per-tab gate, driven as a person who is not an admin.

`@authorized(surface=...)` looks up the surface's tab with `surface_feature`,
and from 2026-09-08 to 2026-09-18 that name was never imported into
`bot/handlers_legacy.py`. Admins return before the lookup, and every test of
the gate was structural, so nothing executed it: every approved person who is
not an admin got a NameError instead of a summary, a TOP-10, an Excel file or
a search result.

These tests run the real wrapper as that person.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from core.permissions import BOT_SURFACES


@pytest.fixture
def gate(monkeypatch):
    from bot import handlers_legacy as h

    asked = []

    def may_use(user_id, feature):
        asked.append((user_id, feature))
        return gate.allowed

    monkeypatch.setattr(h, "is_admin", lambda uid: False)
    monkeypatch.setattr(h.database, "get_user_auth_status",
                        lambda uid: {"status": h.database.STATUS_APPROVED})
    monkeypatch.setattr(h.database, "may_use", may_use)
    monkeypatch.setattr(h.database, "update_last_activity", lambda uid: None)
    monkeypatch.setattr(h, "_lang", lambda update: "en")
    gate.allowed = True
    gate.asked = asked
    gate.h = h
    return gate


def _update(user_id=555):
    update = MagicMock()
    update.effective_user.id = user_id
    update.callback_query = None
    update.message.reply_text = MagicMock(side_effect=_async_none)
    return update


async def _async_none(*a, **kw):
    return None


def _decorated(h, surface, ran):
    @h.authorized(surface=surface)
    async def handler(update, context):
        ran.append(surface)
        return "report"

    return handler


@pytest.mark.parametrize("surface", sorted(BOT_SURFACES))
@pytest.mark.asyncio
async def test_a_person_holding_the_tab_gets_the_report(gate, surface):
    ran = []
    result = await _decorated(gate.h, surface, ran)(_update(), MagicMock())

    assert result == "report"
    assert ran == [surface]
    assert gate.asked == [(555, BOT_SURFACES[surface])]


@pytest.mark.parametrize("surface", sorted(BOT_SURFACES))
@pytest.mark.asyncio
async def test_a_person_without_the_tab_is_refused(gate, surface):
    gate.allowed = False
    ran = []
    update = _update()
    result = await _decorated(gate.h, surface, ran)(update, MagicMock())

    assert ran == []
    assert result == gate.h.ConversationHandler.END
    update.message.reply_text.assert_called_once()
