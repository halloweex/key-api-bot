"""The customer bot's activity, asked of the bot over the docker network.

The bot is a separate project; these tests stand in for it with
`httpx.MockTransport` and are about the seams on this side: the token goes out,
a bot that is restarting costs the page its freshness and not its numbers, a
bot that answers something else is refused rather than drawn, and the route is
an admin's.
"""
from __future__ import annotations

import httpx
import pytest
from fastapi.testclient import TestClient

from web.services import tg_bot_service as svc

TOKEN = "t" * 64
REPORT = {"version": 1, "days": 90, "wau": 37, "mau": 153, "daily": []}


@pytest.fixture(autouse=True)
def _fresh(monkeypatch):
    svc._reset_cache()
    monkeypatch.setenv("KS_TG_BOT_TOKEN", TOKEN)
    monkeypatch.setenv("KS_TG_BOT_URL", "http://ks-tg-bot:8081")
    yield
    svc._reset_cache()


class _Bot:
    """A bot that answers what it is told to, and remembers being asked."""

    def __init__(self, *responses):
        self.responses = list(responses)
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        answer = self.responses.pop(0) if len(self.responses) > 1 else self.responses[0]
        if isinstance(answer, Exception):
            raise answer
        return answer


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


async def _get(bot: _Bot, days: int = 90, clock=None):
    async with httpx.AsyncClient(transport=httpx.MockTransport(bot)) as client:
        return await svc.get_activity(days, client=client,
                                      clock=clock or _Clock())


# --- the request ----------------------------------------------------------------

@pytest.mark.asyncio
async def test_it_asks_with_the_token_and_the_window_and_passes_the_counts_on():
    bot = _Bot(httpx.Response(200, json=REPORT))
    result = await _get(bot, days=30)

    [request] = bot.requests
    assert request.url.path == "/internal/activity"
    assert request.url.params["days"] == "30"
    assert request.headers["Authorization"] == f"Bearer {TOKEN}"
    assert result["wau"] == 37 and result["stale"] is False
    assert result["fetched_at"].endswith("Z")


@pytest.mark.asyncio
async def test_without_a_token_nothing_is_asked(monkeypatch):
    monkeypatch.delenv("KS_TG_BOT_TOKEN")
    bot = _Bot(httpx.Response(200, json=REPORT))
    with pytest.raises(svc.BotUnavailable, match="not configured"):
        await _get(bot)
    assert bot.requests == []


@pytest.mark.asyncio
async def test_a_window_the_page_does_not_offer_is_refused():
    with pytest.raises(ValueError):
        await _get(_Bot(httpx.Response(200, json=REPORT)), days=45)


# --- the cache, which is for the bot -------------------------------------------

@pytest.mark.asyncio
async def test_a_second_look_inside_the_ttl_does_not_ask_again():
    clock = _Clock()
    bot = _Bot(httpx.Response(200, json=REPORT))
    await _get(bot, clock=clock)
    clock.now += svc.TTL_S - 1
    await _get(bot, clock=clock)
    assert len(bot.requests) == 1


@pytest.mark.asyncio
async def test_a_bot_restarting_costs_freshness_and_not_the_numbers():
    clock = _Clock()
    bot = _Bot(httpx.Response(200, json=REPORT), httpx.ConnectError("refused"))
    await _get(bot, clock=clock)
    clock.now += svc.TTL_S + 1
    result = await _get(bot, clock=clock)
    assert len(bot.requests) == 2
    assert result["stale"] is True and result["wau"] == 37


@pytest.mark.asyncio
async def test_with_no_earlier_answer_an_unreachable_bot_is_an_error():
    with pytest.raises(svc.BotUnavailable, match="unreachable"):
        await _get(_Bot(httpx.ConnectError("refused")))


@pytest.mark.asyncio
async def test_windows_are_cached_apart():
    bot = _Bot(httpx.Response(200, json=REPORT))
    await _get(bot, days=30)
    await _get(bot, days=90)
    assert len(bot.requests) == 2
    assert set(svc._cache) <= set(svc.WINDOWS)


# --- answers that must not be drawn ----------------------------------------------

@pytest.mark.asyncio
async def test_a_refusal_is_an_error_that_does_not_repeat_the_bots_body():
    bot = _Bot(httpx.Response(401, text="unauthorized: token abc"))
    with pytest.raises(svc.BotUnavailable) as caught:
        await _get(bot)
    assert "401" in str(caught.value) and "abc" not in str(caught.value)


@pytest.mark.asyncio
async def test_another_contract_version_is_refused_rather_than_misdrawn():
    bot = _Bot(httpx.Response(200, json={**REPORT, "version": 2}))
    with pytest.raises(svc.BotUnavailable, match="version 2"):
        await _get(bot)


@pytest.mark.asyncio
async def test_something_that_is_not_json_is_an_error():
    with pytest.raises(svc.BotUnavailable, match="not JSON"):
        await _get(_Bot(httpx.Response(200, text="<html>")))


# --- the route --------------------------------------------------------------------

def test_the_route_belongs_to_the_tgbot_tab_and_the_marketers_hold_it():
    """Admin-only on the day it shipped; opened to the marketers the same day.
    A tab is how that is said, and the two marketing presets carry it."""
    from core import permissions as perm
    from tests.routes_helper import route_dependencies
    from web.main import app
    from web.routes.auth import require_admin

    # Who gets a 403 and who does not is tests/integration/test_tab_permissions.py,
    # which walks every tab's endpoint; here only that the admin gate is gone.
    assert require_admin not in set(route_dependencies(app, "/api/tg-bot/activity", "GET"))
    assert "tgbot" in perm.TAB_FEATURE_KEYS
    for preset in ("marketer", "marketing"):
        assert "tgbot" in perm.ACCESS_PRESETS[preset], preset
    for role in ("viewer", "editor"):
        assert "tgbot" not in (perm.DEFAULT_TABS[role] or ()), role


def _admin_client() -> TestClient:
    from tests.unit.duckdb_off_sweep_doors import hardcoded_admin, session_cookie
    from web.main import app

    client = TestClient(app)
    name, value = session_cookie(hardcoded_admin(), "admin").split("=", 1)
    client.cookies.set(name, value)
    return client


def test_unconfigured_the_route_answers_503_and_says_why(monkeypatch):
    monkeypatch.delenv("KS_TG_BOT_TOKEN")
    response = _admin_client().get("/api/tg-bot/activity")
    assert response.status_code == 503
    assert "not configured" in response.json()["detail"]


def test_a_window_outside_the_offer_is_a_400():
    assert _admin_client().get("/api/tg-bot/activity?days=45").status_code == 400
