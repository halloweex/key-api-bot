"""Every outbound Telegram to an admin says which machine produced it.

Rule 6 of the alerts charter. `KS_ALERTS_DISABLED` stops a dev instance from
reaching anyone, but it is opt-in and therefore forgettable; the signature
answers the question that comes *after* a message has already arrived. Two
phantom alerts from a laptop running on a copy of the production backup are
why the question exists.

Only an admin can act on that answer. The weekly report and the milestone
broadcast reach every approved user through the same transports, and to them
`· prod-vps` under a sales figure is noise — the owner's call, 2026-09-07. So
the transports sign per recipient, and the rule is pinned here on both sides:
an admin gets the line, a non-admin never does.
"""
import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from core import telegram_alerts
from core.telegram_alerts import instance_name, sign, sign_for

# Bound at import, before conftest's autouse guard replaces both transports
# with a stub that returns 0 — these tests are *about* the transports, so they
# have to call the real ones. Nothing reaches the network: every test below
# either fakes httpx.AsyncClient or asserts that no client is built at all.
_REAL_SEND = telegram_alerts.send_admin_message_http
_REAL_PHOTO = telegram_alerts.send_admin_photo_http

# `core.config` the attribute is the AppConfig object (core/__init__ exports
# it), so the *module* the transports import from has to be fetched by name.
_CONFIG = importlib.import_module("core.config")


class TestInstanceName:
    def test_env_wins(self, monkeypatch):
        monkeypatch.setenv(telegram_alerts.INSTANCE_ENV, "prod-vps")
        assert instance_name() == "prod-vps"

    def test_falls_back_to_hostname(self, monkeypatch):
        monkeypatch.delenv(telegram_alerts.INSTANCE_ENV, raising=False)
        monkeypatch.setattr(telegram_alerts.socket, "gethostname", lambda: "laptop")
        assert instance_name() == "laptop"

    def test_blank_env_is_not_a_name(self, monkeypatch):
        monkeypatch.setenv(telegram_alerts.INSTANCE_ENV, "   ")
        monkeypatch.setattr(telegram_alerts.socket, "gethostname", lambda: "laptop")
        assert instance_name() == "laptop"

    def test_a_multiline_value_stays_one_line(self, monkeypatch):
        """Otherwise the signature reads as message body."""
        monkeypatch.setenv(telegram_alerts.INSTANCE_ENV, "prod\nvps")
        assert "\n" not in instance_name()


class TestSign:
    def test_appends_the_name(self, monkeypatch):
        monkeypatch.setenv(telegram_alerts.INSTANCE_ENV, "prod-vps")
        assert sign("боль").endswith("· prod-vps")
        assert sign("боль").startswith("боль")

    def test_is_idempotent_so_two_paths_cannot_double_sign(self, monkeypatch):
        monkeypatch.setenv(telegram_alerts.INSTANCE_ENV, "prod-vps")
        once = sign("боль")
        assert sign(once) == once

    def test_carries_no_markup(self, monkeypatch):
        """Two parse modes ride this channel — HTML from the canary and the
        bot, Markdown from the data-quality formatter. A transport cannot know
        which one it landed in, so the signature may not carry either."""
        monkeypatch.setenv(telegram_alerts.INSTANCE_ENV, "prod-vps")
        line = sign("x").splitlines()[-1]
        assert not any(c in line for c in "<>*_`[]")

    def test_empty_text_still_gets_a_name(self, monkeypatch):
        monkeypatch.setenv(telegram_alerts.INSTANCE_ENV, "prod-vps")
        assert sign("") == "· prod-vps"


class TestSignFor:
    def test_admin_gets_the_line(self, monkeypatch):
        monkeypatch.setenv(telegram_alerts.INSTANCE_ENV, "prod-vps")
        monkeypatch.setattr(_CONFIG, "ADMIN_USER_IDS", {1})
        assert sign_for("отчёт", 1) == sign("отчёт")

    def test_non_admin_gets_the_text_untouched(self, monkeypatch):
        monkeypatch.setenv(telegram_alerts.INSTANCE_ENV, "prod-vps")
        monkeypatch.setattr(_CONFIG, "ADMIN_USER_IDS", {1})
        assert sign_for("отчёт", 2) == "отчёт"

    def test_compares_ids_as_integers(self, monkeypatch):
        """Admin ids come out of the env as ints; chat ids arrive from the
        store as ints, and from the photo transport as strings. A string
        against an int set would silently sign for nobody."""
        monkeypatch.setenv(telegram_alerts.INSTANCE_ENV, "prod-vps")
        monkeypatch.setattr(_CONFIG, "ADMIN_USER_IDS", {"1"})
        assert sign_for("x", "1") == sign("x")
        assert sign_for("x", 1) == sign("x")

    def test_an_unparseable_id_is_not_an_admin(self, monkeypatch):
        monkeypatch.setattr(_CONFIG, "ADMIN_USER_IDS", {1})
        assert sign_for("x", None) == "x"


class TestBothTransportsSign:
    @pytest.mark.asyncio
    async def test_message_transport_signs_admins_only(self, monkeypatch):
        monkeypatch.delenv(telegram_alerts.DISABLE_ENV, raising=False)
        monkeypatch.setenv(telegram_alerts.INSTANCE_ENV, "prod-vps")
        monkeypatch.setattr(_CONFIG, "ADMIN_USER_IDS", {1})
        sent: list = []

        class FakeClient:
            async def __aenter__(self): return self
            async def __aexit__(self, *a): return False
            async def post(self, url, json=None, **kw):
                sent.append(json)
                return type("R", (), {"status_code": 200, "text": "ok",
                                      "raise_for_status": lambda self: None})()

        monkeypatch.setattr(telegram_alerts.httpx, "AsyncClient",
                            lambda **kw: FakeClient())
        delivered = await _REAL_SEND("тревога", token="t", chat_ids=[1, 2])
        assert delivered == 2
        by_chat = {m["chat_id"]: m["text"] for m in sent}
        assert by_chat[1].endswith("· prod-vps")
        assert by_chat[2] == "тревога"

    @pytest.mark.asyncio
    async def test_plain_text_retry_keeps_the_recipient_rule(self, monkeypatch):
        """The parse-rejection resend must send the same body it just tried,
        not the unsigned template for an admin or a signed one for a user."""
        monkeypatch.delenv(telegram_alerts.DISABLE_ENV, raising=False)
        monkeypatch.setenv(telegram_alerts.INSTANCE_ENV, "prod-vps")
        monkeypatch.setattr(_CONFIG, "ADMIN_USER_IDS", {1})
        sent: list = []

        class FakeClient:
            async def __aenter__(self): return self
            async def __aexit__(self, *a): return False
            async def post(self, url, json=None, **kw):
                sent.append(json)
                rejected = "parse_mode" in json
                return type("R", (), {
                    "status_code": 400 if rejected else 200,
                    "text": "Bad Request: can't parse entities" if rejected else "ok",
                    "raise_for_status": lambda self: None,
                })()

        monkeypatch.setattr(telegram_alerts.httpx, "AsyncClient",
                            lambda **kw: FakeClient())
        await _REAL_SEND("<b", token="t", chat_ids=[1, 2])
        retries = [m for m in sent if "parse_mode" not in m]
        assert {m["chat_id"]: m["text"] for m in retries} == {
            1: sign("<b"), 2: "<b",
        }

    @pytest.mark.asyncio
    async def test_photo_caption_signs_admins_only(self, monkeypatch):
        monkeypatch.delenv(telegram_alerts.DISABLE_ENV, raising=False)
        monkeypatch.setenv(telegram_alerts.INSTANCE_ENV, "prod-vps")
        monkeypatch.setattr(_CONFIG, "ADMIN_USER_IDS", {1})
        sent: list = []

        class FakeClient:
            async def __aenter__(self): return self
            async def __aexit__(self, *a): return False
            async def post(self, url, data=None, files=None, **kw):
                sent.append(data)
                return type("R", (), {"raise_for_status": lambda self: None})()

        monkeypatch.setattr(telegram_alerts.httpx, "AsyncClient",
                            lambda **kw: FakeClient())
        delivered = await _REAL_PHOTO(b"png", "отчёт", token="t", chat_ids=[1, 2])
        assert delivered == 2
        by_chat = {m["chat_id"]: m["caption"] for m in sent}
        assert by_chat["1"].endswith("· prod-vps")
        assert by_chat["2"] == "отчёт"

    @pytest.mark.asyncio
    async def test_message_transport_signs(self, monkeypatch):
        monkeypatch.delenv(telegram_alerts.DISABLE_ENV, raising=False)
        monkeypatch.setenv(telegram_alerts.INSTANCE_ENV, "prod-vps")
        monkeypatch.setattr(_CONFIG, "ADMIN_USER_IDS", {1})
        sent: list = []

        class FakeClient:
            async def __aenter__(self): return self
            async def __aexit__(self, *a): return False
            async def post(self, url, json=None, **kw):
                sent.append(json)
                return type("R", (), {"status_code": 200, "text": "ok",
                                      "raise_for_status": lambda self: None})()

        monkeypatch.setattr(telegram_alerts.httpx, "AsyncClient",
                            lambda **kw: FakeClient())
        delivered = await _REAL_SEND("тревога", token="t", chat_ids=[1])
        assert delivered == 1
        assert sent[0]["text"].endswith("· prod-vps")

    @pytest.mark.asyncio
    async def test_photo_caption_signs(self, monkeypatch):
        monkeypatch.delenv(telegram_alerts.DISABLE_ENV, raising=False)
        monkeypatch.setenv(telegram_alerts.INSTANCE_ENV, "prod-vps")
        monkeypatch.setattr(_CONFIG, "ADMIN_USER_IDS", {1})
        sent: list = []

        class FakeClient:
            async def __aenter__(self): return self
            async def __aexit__(self, *a): return False
            async def post(self, url, data=None, files=None, **kw):
                sent.append(data)
                return type("R", (), {"raise_for_status": lambda self: None})()

        monkeypatch.setattr(telegram_alerts.httpx, "AsyncClient",
                            lambda **kw: FakeClient())
        delivered = await _REAL_PHOTO(b"png", "отчёт", token="t", chat_ids=[1])
        assert delivered == 1
        assert sent[0]["caption"].endswith("· prod-vps")

    @pytest.mark.asyncio
    async def test_caption_budget_is_measured_after_signing(self, monkeypatch):
        """The signature spends caption budget, so the limit has to be checked
        against what is actually sent — otherwise a caption one character under
        the limit loses its picture with no explanation. Measured against the
        signed form even when no recipient is an admin: one verdict per send,
        never a picture for some readers and text for the rest."""
        monkeypatch.delenv(telegram_alerts.DISABLE_ENV, raising=False)
        monkeypatch.setenv(telegram_alerts.INSTANCE_ENV, "prod-vps")
        monkeypatch.setattr(_CONFIG, "ADMIN_USER_IDS", {1})

        def forbidden(*a, **kw):
            raise AssertionError("an over-budget caption was sent anyway")

        monkeypatch.setattr(telegram_alerts.httpx, "AsyncClient", forbidden)
        caption = "x" * telegram_alerts.TELEGRAM_CAPTION_LIMIT
        assert await _REAL_PHOTO(b"png", caption, token="t", chat_ids=[1]) == 0
        assert await _REAL_PHOTO(b"png", caption, token="t", chat_ids=[2]) == 0

    @pytest.mark.asyncio
    async def test_suppressed_alerts_are_not_signed_into_existence(self, monkeypatch):
        """The kill switch still wins: signing must not resurrect a send."""
        monkeypatch.setenv(telegram_alerts.DISABLE_ENV, "1")

        def forbidden(*a, **kw):
            raise AssertionError("suppressed alert opened an HTTP client")

        monkeypatch.setattr(telegram_alerts.httpx, "AsyncClient", forbidden)
        assert await _REAL_SEND("боль") == 0


class TestTheBotApplicationPathSigns:
    """The Application path never reaches core/telegram_alerts, so it is the
    one place outside the transports that has to sign for itself — and it must
    not sign twice when it falls back to HTTP."""

    @pytest.mark.asyncio
    async def test_application_path_signs_once(self, monkeypatch):
        from bot import main as bot_main

        monkeypatch.delenv(telegram_alerts.DISABLE_ENV, raising=False)
        monkeypatch.setenv(telegram_alerts.INSTANCE_ENV, "prod-vps")
        telegram_alerts.reset_throttle()

        app = type("App", (), {})()
        app.bot = type("Bot", (), {})()
        app.bot.send_message = AsyncMock()
        monkeypatch.setattr(bot_main, "_application", app)
        monkeypatch.setattr(bot_main, "ADMIN_USER_IDS", [7])

        await bot_main.send_admin_message("тревога", key="t:1")

        text = app.bot.send_message.call_args.kwargs["text"]
        assert text.count("· prod-vps") == 1

    @pytest.mark.asyncio
    async def test_http_fallback_signs_exactly_once(self, monkeypatch):
        from bot import main as bot_main

        monkeypatch.delenv(telegram_alerts.DISABLE_ENV, raising=False)
        monkeypatch.setenv(telegram_alerts.INSTANCE_ENV, "prod-vps")
        telegram_alerts.reset_throttle()
        monkeypatch.setattr(bot_main, "_application", None)

        with patch.object(telegram_alerts, "send_admin_message_http",
                          new=AsyncMock(return_value=1)) as http:
            await bot_main.send_admin_message("тревога", key="t:2")

        # The transport receives it unsigned and signs on its way out; what
        # must never happen is two signatures on one message.
        assert http.await_args.args[0].count("· prod-vps") <= 1
        assert sign(http.await_args.args[0]).count("· prod-vps") == 1

    @pytest.mark.asyncio
    async def test_signature_does_not_become_the_throttle_key(self, monkeypatch):
        """Signing happens after the throttle decision, so an unkeyed alert is
        still recognised as a repeat of itself."""
        from bot import main as bot_main

        monkeypatch.delenv(telegram_alerts.DISABLE_ENV, raising=False)
        monkeypatch.setenv(telegram_alerts.INSTANCE_ENV, "prod-vps")
        telegram_alerts.reset_throttle()
        monkeypatch.setattr(bot_main, "_application", None)

        with patch.object(telegram_alerts, "send_admin_message_http",
                          new=AsyncMock(return_value=1)) as http:
            await bot_main.send_admin_message("одно и то же")
            await bot_main.send_admin_message("одно и то же")

        assert http.await_count == 1


class TestTheMilestoneBroadcast:
    """The one message the bot itself sends to every approved user. Same
    rule, driven through the real handler: the admin's copy is signed, the
    user's is not."""

    @pytest.mark.asyncio
    async def test_signs_admins_only(self, monkeypatch):
        from bot import handlers_legacy as h

        monkeypatch.delenv(telegram_alerts.DISABLE_ENV, raising=False)
        monkeypatch.setenv(telegram_alerts.INSTANCE_ENV, "prod-vps")
        monkeypatch.setattr(_CONFIG, "ADMIN_USER_IDS", {1})
        monkeypatch.setattr(h, "REVENUE_MILESTONES", {
            "daily": [{"amount": 1, "message": "Day", "emoji": "🎉"}],
            "weekly": [], "monthly": [],
        })
        monkeypatch.setattr(h.database, "get_all_authorized_users",
                            lambda: [{"user_id": 1}, {"user_id": 2}])
        monkeypatch.setattr(h.database, "is_milestone_celebrated",
                            lambda *a: False)
        monkeypatch.setattr(h.database, "mark_milestone_celebrated",
                            lambda *a: True)
        monkeypatch.setattr(h, "report_service", SimpleNamespace(
            aggregate_sales_data=lambda **kw: ({}, {}, 3, {1: 500.0}, {}),
        ))
        bot = SimpleNamespace(send_message=AsyncMock())

        await h.check_and_broadcast_milestones(SimpleNamespace(bot=bot))

        texts = {c.kwargs["chat_id"]: c.kwargs["text"]
                 for c in bot.send_message.call_args_list}
        assert set(texts) == {1, 2}
        assert texts[1].endswith("· prod-vps")
        assert "prod-vps" not in texts[2]
        assert texts[2] == texts[1].replace("\n\n· prod-vps", "")
