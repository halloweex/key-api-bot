"""Admin alerts must leave the web container, where no bot Application exists.

Regression cover for 2026-08-02: warehouse validation failed for five days and
every CRITICAL alert was logged as "dropping" instead of being delivered.
"""
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from core.telegram_alerts import send_admin_message_http

# `bot/__init__.py` re-exports a `main` callable, so `import bot.main as ...`
# binds that function instead of the module. Go through importlib.
bot_main = __import__("importlib").import_module("bot.main")


def _client_returning(response_factory):
    """Build an AsyncClient stand-in whose .post uses response_factory."""
    client = MagicMock()
    client.post = AsyncMock(side_effect=response_factory)
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=False)
    return client


def _ok_response(*_args, **_kwargs):
    response = MagicMock()
    response.raise_for_status = MagicMock()
    return response


class TestHttpTransport:
    @pytest.mark.asyncio
    async def test_posts_to_every_admin(self):
        client = _client_returning(_ok_response)
        with patch("httpx.AsyncClient", return_value=client):
            delivered = await send_admin_message_http(
                "boom", token="123:ABC", chat_ids=[111, 222],
            )

        assert delivered == 2
        urls = {call.args[0] for call in client.post.call_args_list}
        assert urls == {"https://api.telegram.org/bot123:ABC/sendMessage"}
        chat_ids = [call.kwargs["json"]["chat_id"] for call in client.post.call_args_list]
        assert chat_ids == [111, 222]
        assert client.post.call_args_list[0].kwargs["json"]["text"] == "boom"

    @pytest.mark.asyncio
    async def test_one_failing_admin_does_not_block_the_others(self):
        def flaky(url, **kwargs):
            if kwargs["json"]["chat_id"] == 111:
                raise httpx.ConnectError("nope")
            return _ok_response()

        client = _client_returning(flaky)
        with patch("httpx.AsyncClient", return_value=client):
            delivered = await send_admin_message_http(
                "boom", token="123:ABC", chat_ids=[111, 222],
            )

        assert delivered == 1

    @pytest.mark.asyncio
    async def test_reports_zero_without_token_or_admins(self):
        with patch("httpx.AsyncClient") as client_cls:
            assert await send_admin_message_http("x", token="", chat_ids=[111]) == 0
            assert await send_admin_message_http("x", token="123:ABC", chat_ids=[]) == 0
        client_cls.assert_not_called()

    @pytest.mark.asyncio
    async def test_transport_failure_never_raises(self):
        with patch("httpx.AsyncClient", side_effect=RuntimeError("no network")):
            assert await send_admin_message_http("x", token="123:ABC", chat_ids=[111]) == 0


class TestFallbackWiring:
    @pytest.mark.asyncio
    async def test_falls_back_to_http_when_no_application(self, monkeypatch):
        monkeypatch.setattr(bot_main, "_application", None)
        sender = AsyncMock(return_value=1)
        with patch("core.telegram_alerts.send_admin_message_http", sender):
            await bot_main.send_admin_message("warehouse is broken")

        sender.assert_awaited_once_with("warehouse is broken", "HTML")

    @pytest.mark.asyncio
    async def test_uses_the_application_when_one_is_running(self, monkeypatch):
        application = MagicMock()
        application.bot.send_message = AsyncMock()
        monkeypatch.setattr(bot_main, "_application", application)
        monkeypatch.setattr(bot_main, "ADMIN_USER_IDS", {777})
        sender = AsyncMock()
        with patch("core.telegram_alerts.send_admin_message_http", sender):
            await bot_main.send_admin_message("hello")

        sender.assert_not_awaited()
        application.bot.send_message.assert_awaited_once()
        assert application.bot.send_message.await_args.kwargs["chat_id"] == 777

    @pytest.mark.asyncio
    async def test_warehouse_alert_reaches_the_transport(self, monkeypatch):
        """The path that stayed silent in production, end to end."""
        from core.duckdb_store import DuckDBStore

        monkeypatch.setattr(bot_main, "_application", None)
        sender = AsyncMock(return_value=1)
        store = DuckDBStore.__new__(DuckDBStore)
        with patch("core.telegram_alerts.send_admin_message_http", sender):
            await store._send_warehouse_alert("🚨 CRITICAL: validation failed 4x")

        sender.assert_awaited_once()
        assert "CRITICAL" in sender.await_args.args[0]


class TestTheSuiteCannotReachTelegram:
    """`.env` holds a real BOT_TOKEN and `send_admin_message` falls back to the
    HTTP Bot API whenever no Application is running — which is every test
    process. The cell-guard tests, which must fail validation on purpose, sent
    a real alert to real admins before the conftest guard existed."""

    @pytest.mark.asyncio
    async def test_the_transport_is_blocked_by_default(self, monkeypatch):
        import core.telegram_alerts as alerts

        monkeypatch.setattr(bot_main, "_application", None)
        delivered = await alerts.send_admin_message_http("this must not go out")

        assert delivered == 0, "the autouse guard in conftest is missing"

