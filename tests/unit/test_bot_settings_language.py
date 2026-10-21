"""Choosing a language in the bot's settings, driven through the handler.

#76 (2026-08-12) taught the whole bot the reader's language and, in passing,
appended `lang=_lang(update)` to the confirmation's
`Keyboards.settings_menu(prefs, chosen)` — whose second positional parameter
*is* `lang`. From then on picking a language stored the choice and raised
`TypeError: settings_menu() got multiple values for argument 'lang'` before
the message was redrawn: the person tapped, nothing changed on screen, and
tapping again did the same. Nothing executed the branch; the type checker
named it.

The menu is drawn in the language just chosen, which is what makes the
confirmation the proof that it took.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from core.i18n import LANGUAGES


@pytest.fixture
def handlers():
    from bot import handlers_legacy

    return handlers_legacy


@pytest.mark.asyncio
@pytest.mark.parametrize("chosen", LANGUAGES)
async def test_choosing_a_language_redraws_the_menu_in_it(handlers, monkeypatch, chosen):
    """Mutation: pass the language twice again — TypeError, no redraw."""
    stored = {"language": "uk"}
    monkeypatch.setattr(handlers.database, "get_user_language",
                        lambda uid: stored["language"])
    monkeypatch.setattr(handlers.database, "update_user_preference",
                        lambda uid, key, value: stored.__setitem__(key, value))
    monkeypatch.setattr(handlers.database, "get_user_preferences",
                        lambda uid: dict(stored))
    query = SimpleNamespace(
        data=f"set_lang_{chosen}", answer=AsyncMock(), edit_message_text=AsyncMock())
    update = SimpleNamespace(
        callback_query=query, effective_user=SimpleNamespace(id=7, language_code="en"))

    await handlers.settings_callback(update, MagicMock())

    assert stored["language"] == chosen
    query.edit_message_text.assert_awaited_once()
    text = query.edit_message_text.await_args.args[0]
    markup = query.edit_message_text.await_args.kwargs["reply_markup"]
    assert text.startswith("✅")
    assert markup.to_dict() == handlers.Keyboards.settings_menu(
        {"language": chosen}, chosen).to_dict()
