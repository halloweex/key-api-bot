"""A half-finished flow must not wedge the menu.

Reported as "the Settings button does not work", and that is exactly how it
presents. `/report`, `/search` and `/settings` — and the reply-keyboard buttons
that stand in for them — are all ConversationHandler *entry points*, and entry
points are only checked for users who are not already in a conversation. Tap
"📊 Report", pick a report type, walk away, and the conversation is parked in
SELECTING_DATE_RANGE, whose handlers are all CallbackQueryHandlers. A text
button matches none of them and no entry point is allowed to fire, so the tap
reaches nothing at all.

"ℹ️ Help" and "📈 Dashboard" keep working the whole time, because they are
registered outside the conversation — which is why the failure reads as one
broken button rather than a stuck bot. Nothing re-armed it either: no timeout,
and only /cancel cleared the state.
"""
from __future__ import annotations

import datetime as dt
import warnings

import pytest
from telegram import Chat, Message, Update, User

from bot.config import ConversationState
from core.i18n import LANGUAGES, all_translations

# The handler is built with per_message=False, which PTB warns about by design.
warnings.filterwarnings("ignore", message=".*per_message.*")

USER = User(id=777, first_name="T", is_bot=False)
CHAT = Chat(id=777, type="private")

# The three top-level buttons that are entry points, and therefore the three
# that went dead — named by key, in every language they are drawn in.
MENU_KEYS = ("btn.settings", "btn.report", "btn.search")
MENU_BUTTONS = tuple(
    label for key in MENU_KEYS for label in all_translations(key)
)


@pytest.fixture
def conversation():
    import importlib
    return importlib.import_module("bot.main").create_conversation_handler()


def _message(text: str) -> Update:
    return Update(update_id=1, message=Message(
        message_id=1, date=dt.datetime.now(dt.timezone.utc),
        chat=CHAT, from_user=USER, text=text,
    ))


def _park(conversation, state) -> None:
    """Leave this user mid-flow, the way tapping away from /report does."""
    conversation._conversations[conversation._get_key(_message("x"))] = state


class TestMenuButtonsAlwaysWork:
    @pytest.mark.parametrize("label", MENU_BUTTONS)
    def test_a_fresh_user_can_press_them(self, conversation, label):
        assert conversation.check_update(_message(label))

    @pytest.mark.parametrize("label", MENU_BUTTONS)
    @pytest.mark.parametrize("state", [
        ConversationState.SELECTING_REPORT_TYPE,
        ConversationState.SELECTING_DATE_RANGE,
        ConversationState.SELECTING_CUSTOM_START_YEAR,
        ConversationState.SEARCH_WAITING_QUERY,
        ConversationState.SETTINGS_MENU,
    ])
    def test_a_parked_user_can_still_press_them(self, conversation, label, state):
        """The regression itself: these returned nothing from every state."""
        _park(conversation, state)
        assert conversation.check_update(_message(label)), (
            f"{label} is unreachable while parked in {state}"
        )

    @pytest.mark.parametrize("lang", LANGUAGES)
    def test_every_button_the_keyboard_draws_reaches_a_handler(self, conversation, lang):
        """The matcher and the labels must stay two views of one table.

        Telegram sends a tap back as plain text, so a label the matcher does
        not recognise is a button that dies in silence — which happened here
        once over a single U+FE0F variation selector. Drawing the keyboard and
        feeding every label back through the handler is the only check that
        cannot be fooled by a translation added on one side only.
        """
        from bot.keyboards import ReplyKeyboards

        for row in ReplyKeyboards.main_menu(lang).keyboard:
            for button in row:
                update = _message(button.text)
                reachable = bool(conversation.check_update(update))
                # Help and Dashboard live outside the conversation; the other
                # three are entry points and must be matched by it.
                if not reachable:
                    from bot.main import button_filter
                    assert (button_filter("btn.help").check_update(update)
                            or button_filter("btn.dashboard").check_update(update)), (
                        f"{button.text!r} ({lang}) reaches nothing"
                    )


class TestLogsKeepTheToken:
    def test_httpx_does_not_log_request_urls(self):
        """Every Telegram URL carries BOT_TOKEN in its path.

        httpx logs each request at INFO, so a ten-second long poll wrote the
        token into the container log a few hundred times an hour — kept for
        250 MB per service and readable by anyone with host access.
        `core/observability.py` has silenced it in the web container all
        along; the bot configures logging itself and never got it.
        """
        import importlib
        import logging

        importlib.import_module("bot.main")
        assert logging.getLogger("httpx").level >= logging.WARNING


class TestConversationSettings:
    def test_reentry_is_allowed(self, conversation):
        """What actually fixes it — entry points fire mid-conversation."""
        assert conversation.allow_reentry is True

    def test_an_abandoned_conversation_expires(self, conversation):
        """Otherwise a state nobody remembers entering lives until restart."""
        assert conversation.conversation_timeout == 30 * 60
