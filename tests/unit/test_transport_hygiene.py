"""Step 00 of the alerts rework: delivery outranks typography.

Three properties the transports now guarantee, each born of a defect found
29.08: a message that Telegram rejects as unparseable is resent as plain text
instead of dying (the certificate alert died exactly this way); a message over
the 4 096-unit limit is clamped instead of rejected (an incident-morning
digest is ~10 000 characters); and every caller can learn how many admins a
message actually reached, because `send_admin_message` returning None is how
the digest advanced its beat on undelivered sends.
"""
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from core import telegram_alerts
from core.telegram_alerts import (
    TELEGRAM_MESSAGE_LIMIT,
    _utf16_units,
    clamp_message,
    reset_throttle,
)

bot_main = __import__("importlib").import_module("bot.main")

# The conftest autouse guard stubs the HTTP transports; these tests are about
# the real ones. Nothing reaches the network — every test fakes the client.
_REAL_SEND = telegram_alerts.send_admin_message_http


class _Resp:
    def __init__(self, status_code=200, text="ok"):
        self.status_code = status_code
        self.text = text

    def raise_for_status(self):
        if self.status_code >= 400:
            raise httpx.HTTPStatusError(self.text, request=None, response=None)


def _client(posts):
    class FakeClient:
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False
        async def post(self, url, json=None, **kw):
            posts.append(json)
            if json.get("parse_mode") and "(<14)" in json.get("text", ""):
                return _Resp(400, 'Bad Request: can\'t parse entities: '
                                  'Unsupported start tag "14)" at byte offset 20')
            return _Resp()
    return FakeClient()


class TestParseRejectionFallback:
    @pytest.mark.asyncio
    async def test_unparseable_markup_degrades_to_plain_text(self, monkeypatch):
        """The certificate-alert shape: `(<14)` under parse_mode=HTML."""
        posts = []
        monkeypatch.setattr(telegram_alerts.httpx, "AsyncClient",
                            lambda **kw: _client(posts))
        delivered = await _REAL_SEND(
            "cert expires in 13d (<14)", token="t", chat_ids=[1],
        )
        assert delivered == 1
        assert len(posts) == 2
        assert posts[0].get("parse_mode") == "HTML"
        assert "parse_mode" not in posts[1]
        assert posts[1]["text"] == posts[0]["text"]

    @pytest.mark.asyncio
    async def test_other_400s_are_not_retried(self, monkeypatch):
        """A bad chat id fails identically in plain text — one attempt only."""
        posts = []

        class FakeClient:
            async def __aenter__(self): return self
            async def __aexit__(self, *a): return False
            async def post(self, url, json=None, **kw):
                posts.append(json)
                return _Resp(400, "Bad Request: chat not found")

        monkeypatch.setattr(telegram_alerts.httpx, "AsyncClient",
                            lambda **kw: FakeClient())
        delivered = await _REAL_SEND("боль", token="t", chat_ids=[1])
        assert delivered == 0
        assert len(posts) == 1

    @pytest.mark.asyncio
    async def test_application_path_degrades_too(self, monkeypatch):
        """The bot's own path is the one that ate the certificate alert."""
        monkeypatch.setenv(telegram_alerts.INSTANCE_ENV, "test-instance")
        reset_throttle()
        calls = []

        async def send_message(chat_id, text, **kw):
            calls.append(kw)
            if "parse_mode" in kw:
                raise Exception("Bad Request: can't parse entities: "
                                "Unsupported start tag")

        app = type("App", (), {})()
        app.bot = type("Bot", (), {})()
        app.bot.send_message = send_message
        monkeypatch.setattr(bot_main, "_application", app)
        monkeypatch.setattr(bot_main, "ADMIN_USER_IDS", [7])

        delivered = await bot_main.send_admin_message("x (<14)", key="t:pf")
        assert delivered == 1
        assert len(calls) == 2 and "parse_mode" not in calls[1]


class TestMessageClamp:
    def test_over_limit_is_cut_to_fit(self):
        out = clamp_message("я" * 6000)
        assert _utf16_units(out) <= TELEGRAM_MESSAGE_LIMIT
        assert out.endswith("… (truncated)")

    def test_units_are_utf16_because_telegram_counts_that_way(self):
        """An emoji is two units; a char-count clamp would overshoot."""
        out = clamp_message("🚨" * 3000)
        assert _utf16_units(out) <= TELEGRAM_MESSAGE_LIMIT

    def test_short_text_passes_untouched(self):
        assert clamp_message("боль") == "боль"

    @pytest.mark.asyncio
    async def test_the_signature_survives_the_cut(self, monkeypatch):
        monkeypatch.delenv(telegram_alerts.DISABLE_ENV, raising=False)
        monkeypatch.setenv(telegram_alerts.INSTANCE_ENV, "test-instance")
        posts = []
        monkeypatch.setattr(telegram_alerts.httpx, "AsyncClient",
                            lambda **kw: _client(posts))
        delivered = await _REAL_SEND("я" * 6000, token="t", chat_ids=[1])
        assert delivered == 1
        sent = posts[0]["text"]
        assert _utf16_units(sent) <= TELEGRAM_MESSAGE_LIMIT
        assert sent.endswith("· test-instance")


class TestDeliveredCount:
    def setup_method(self):
        reset_throttle()

    def teardown_method(self):
        reset_throttle()

    @pytest.mark.asyncio
    async def test_http_fallback_count_is_passed_through(self, monkeypatch):
        monkeypatch.setattr(bot_main, "_application", None)
        with patch("core.telegram_alerts.send_admin_message_http",
                   new=AsyncMock(return_value=2)):
            assert await bot_main.send_admin_message("тревога", key="c:1") == 2

    @pytest.mark.asyncio
    async def test_kill_switch_reports_zero(self, monkeypatch):
        monkeypatch.setenv(telegram_alerts.DISABLE_ENV, "1")
        assert await bot_main.send_admin_message("тревога", key="c:2") == 0

    @pytest.mark.asyncio
    async def test_throttled_repeat_reports_zero(self, monkeypatch):
        monkeypatch.setattr(bot_main, "_application", None)
        with patch("core.telegram_alerts.send_admin_message_http",
                   new=AsyncMock(return_value=2)):
            assert await bot_main.send_admin_message("тревога", key="c:3") == 2
            assert await bot_main.send_admin_message("тревога", key="c:3") == 0

    @pytest.mark.asyncio
    async def test_application_path_counts_real_deliveries(self, monkeypatch):
        app = type("App", (), {})()
        app.bot = type("Bot", (), {})()

        async def flaky(chat_id, **kw):
            if chat_id == 1:
                raise Exception("blocked by user")

        app.bot.send_message = flaky
        monkeypatch.setattr(bot_main, "_application", app)
        monkeypatch.setattr(bot_main, "ADMIN_USER_IDS", [1, 2])
        assert await bot_main.send_admin_message("тревога", key="c:4") == 1


class TestDiskThrottleIsKeyed:
    """A WARN at 06:00 must not mute the escalation to CRITICAL at 12:00.

    Step 00 fixed this by turning the shared float into a per-key dict;
    step 02 moved the policy into the Gate, which keys per bucket by
    construction — so the assertion is now that the private state is gone."""

    def test_the_private_disk_cooldown_is_gone(self):
        from core.scheduler import BackgroundScheduler

        assert not hasattr(BackgroundScheduler, "_disk_alert_last_sent")
        assert not hasattr(BackgroundScheduler, "_bronze_invariant_last_alert")
        assert not hasattr(BackgroundScheduler, "_dq_last_alert")


class TestTheFourthTransportIsGone:
    def test_no_raw_telegram_post_in_scheduler(self):
        """The memory monitor used to hold a private transport that bypassed
        the kill switch, the throttle stack and the signature — the exact
        phantom-from-a-laptop scenario. It now rides the shared path."""
        import pathlib

        src = pathlib.Path("core/scheduler.py").read_text()
        assert "api.telegram.org" not in src
        assert "_send_admin_telegram" not in src
