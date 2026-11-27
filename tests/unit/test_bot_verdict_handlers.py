"""The admin's Approve/Deny buttons answer the request they were shown for.

Every admin gets the same request card. Two of them tapping Approve and Deny
on it in the same second used to be last-write-wins, with the person told
both. A verdict now lands only while the request is pending: the later admin
is told it was already decided, and the person hears once. The taps go
through the bot as it is started, to a Bot API answered at the transport
(`tests/helpers/ci_bot_scheduler_telegram.py`).
"""
import inspect

import pytest

from bot import database, handlers_legacy as h
from core.i18n import t
from tests.helpers.ci_bot_scheduler_telegram import bot_world, buttons

ADMIN_A, ADMIN_B = 900_000_001, 900_000_002
PERSON = 900_000_101

# The bot's own conversation handler warns about its per_* settings by design.
pytestmark = pytest.mark.filterwarnings("ignore:If 'per_message=False'")


@pytest.fixture
def world(monkeypatch, tmp_path):
    """Both admins, and a person waiting in the queue as `/start` leaves
    them."""
    with bot_world(monkeypatch, tmp_path, admins=(ADMIN_A, ADMIN_B)) as w:
        database.request_access(PERSON, "asker", "Ask", "Er")
        yield w


def _verdict():
    row = database.get_user_auth_status(PERSON)
    return row["status"], row["denial_count"], row["reviewed_by"]


class TestTheLaterVerdictOnOneRequestIsRefused:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("first, later, stands", [
        ("approve", "deny", ("approved", 0, ADMIN_A)),
        ("deny", "approve", ("denied", 1, ADMIN_A)),
    ], ids=["deny_after_approve", "approve_after_deny"])
    async def test_the_first_verdict_stands_and_the_person_hears_once(
            self, world, first, later, stands):
        """The later admin is told why nothing happened, in their own
        language, and their card is left as it was."""
        database.update_user_preference(ADMIN_B, "language", "uk")
        await world.tap(ADMIN_A, f"auth_{first}_{PERSON}")
        told = world.api.messages_to(PERSON)

        tap = await world.tap(ADMIN_B, f"auth_{later}_{PERSON}")

        assert _verdict() == stands
        assert len(told) == 1 and world.api.messages_to(PERSON) == told
        answer = world.api.answer_to(tap)
        assert answer is not None and answer.get("show_alert") == "true"
        assert answer["text"] == t("admin.already_decided", "uk")
        assert world.api.edits_in(ADMIN_B) == []


class TestTheDeniedPersonIsToldInTheirOwnLanguage:
    """The denial used to reach the person as the raw i18n key."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("denied_before, said, may_ask_again", [
        (0, "access.denied", True),
        (database.MAX_DENIAL_COUNT - 1, "access.frozen", False),
    ], ids=["denied", "frozen"])
    async def test_in_russian_with_a_way_back_unless_frozen(
            self, world, denied_before, said, may_ask_again):
        database.update_user_preference(PERSON, "language", "ru")
        for _ in range(denied_before):
            database.deny_user(PERSON, ADMIN_A)
            database.reset_user_to_pending(PERSON)

        await world.tap(ADMIN_A, f"auth_deny_{PERSON}")

        (message,) = world.api.messages_to(PERSON)
        assert t(said, "ru") in message["text"]
        assert said not in message["text"]
        again = buttons(message)
        if may_ask_again:
            assert again, "a denied person is left no way to ask again"
            for button in again:
                assert world.handler_of(PERSON, button["callback_data"]) is not None, (
                    f"{button['callback_data']!r} reaches nothing")
        else:
            assert again == []


def test_the_list_refresh_no_longer_reaches_for_a_missing_update():
    src = inspect.getsource(h.show_updated_user_list)
    assert "_lang(update)" not in src
    assert "lang" in inspect.signature(h.show_updated_user_list).parameters
    for caller in (h.admin_revoke_user, h.admin_unfreeze_user):
        assert "show_updated_user_list(query, admin.id, _lang(update))" in inspect.getsource(caller)
