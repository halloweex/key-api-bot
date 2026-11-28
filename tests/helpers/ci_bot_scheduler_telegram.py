"""The bot's handlers against a Bot API answered here, at the transport.

The handlers run with a real `telegram.ext.Application`, whose bot posts to
an `httpx.MockTransport`: every Bot API request they make — answering a tap,
editing the card it was on, writing to a person — is recorded with its
parameters and answered as Telegram would. A tap is the update Telegram
sends, routed to the handler `bot.main.main()` registered for it
(`tests/helpers/access_api_bot.py`), so a test reads what reached whom and
never which function ran.

    with bot_world(monkeypatch, tmp_path, admins=(A, B)) as world:
        query_id = await world.tap(A, f"auth_approve_{person}")
        world.api.messages_to(person)       # what the person was sent
        world.api.answer_to(query_id)       # what A saw on tapping

The bot's store is SQLite in `tmp_path`, as `KS_BOT_STORE` unset gives.
"""
from __future__ import annotations

import itertools
import json
import urllib.parse
from contextlib import contextmanager
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

import httpx

TOKEN = "123456:test-bot-token-not-a-real-secret"


class BotApi:
    """Telegram's side of the conversation: every request recorded and
    answered. Parameters are kept as the Bot API received them, strings."""

    def __init__(self) -> None:
        self.calls: List[Tuple[str, Dict[str, str]]] = []

    def handle(self, request: httpx.Request) -> httpx.Response:
        method = request.url.path.rsplit("/", 1)[-1]
        body = request.content.decode() if request.content else ""
        if "json" in request.headers.get("content-type", ""):
            params = {k: v if isinstance(v, str) else json.dumps(v)
                      for k, v in json.loads(body or "{}").items()}
        else:
            params = dict(urllib.parse.parse_qsl(body, keep_blank_values=True))
        self.calls.append((method, params))
        if method in ("sendMessage", "editMessageText"):
            return httpx.Response(200, json={"ok": True, "result": {
                "message_id": len(self.calls), "date": 0, "text": params.get("text", ""),
                "chat": {"id": int(params.get("chat_id") or 0), "type": "private"}}})
        return httpx.Response(200, json={"ok": True, "result": True})

    def _of(self, method: str) -> List[Dict[str, str]]:
        return [p for m, p in self.calls if m == method]

    def messages_to(self, chat_id: int) -> List[Dict[str, str]]:
        """Every message sent to `chat_id`, in order."""
        return [p for p in self._of("sendMessage") if int(p["chat_id"]) == chat_id]

    def edits_in(self, chat_id: int) -> List[Dict[str, str]]:
        """Every edit of a message in `chat_id`'s chat."""
        return [p for p in self._of("editMessageText")
                if int(p.get("chat_id") or 0) == chat_id]

    def answer_to(self, query_id: str) -> Optional[Dict[str, str]]:
        """What the person who made tap `query_id` was shown, if anything."""
        found = [p for p in self._of("answerCallbackQuery")
                 if p["callback_query_id"] == query_id]
        assert len(found) <= 1, f"tap {query_id} was answered {len(found)} times"
        return found[0] if found else None


def buttons(message: Dict[str, str]) -> List[dict]:
    """The inline buttons under a sent message."""
    markup = json.loads(message.get("reply_markup") or "{}")
    return [b for row in markup.get("inline_keyboard", []) for b in row]


class BotWorld:
    def __init__(self, api: BotApi, application, started) -> None:
        self.api = api
        self.application = application
        self._started = started
        self._ids = itertools.count(1)

    def _update(self, user_id: int, data: str):
        from telegram import Update

        n = next(self._ids)
        return Update.de_json({"update_id": n, "callback_query": {
            "id": f"tap-{n}", "data": data, "chat_instance": str(user_id),
            "from": {"id": user_id, "is_bot": False, "first_name": "Test"},
            "message": {"message_id": n, "date": 1_700_000_000, "text": "card",
                        "chat": {"id": user_id, "type": "private"}},
        }}, self.application.bot)

    def handler_of(self, user_id: int, data: str):
        """The handler a tap of `data` by `user_id` reaches, or None."""
        from tests.helpers.access_api_bot import handler_for

        return handler_for(self._started, self._update(user_id, data))

    async def tap(self, user_id: int, data: str) -> str:
        """`user_id` taps a button carrying `data`; the handler it reaches
        runs to the end. Returns the tap's id, for `BotApi.answer_to`."""
        from tests.helpers.access_api_bot import handler_for

        update = self._update(user_id, data)
        handler = handler_for(self._started, update)
        assert handler is not None, f"no handler takes a tap of {data!r}"
        context = self.application.context_types.context.from_update(
            update, self.application)
        await handler.callback(update, context)
        return update.callback_query.id


@contextmanager
def bot_world(monkeypatch, tmp_path, *, admins: Sequence[int]) -> Iterator[BotWorld]:
    """The bot started as its container starts it, `admins` its admins, its
    store SQLite in `tmp_path`; the store it replaced is put back after."""
    import bot.config as bot_config
    from telegram.ext import Application
    from telegram.request import HTTPXRequest

    from bot import database, store_sqlite
    from core.bot_store import use_bot_store
    from tests.helpers.access_api_bot import start_the_bot

    api = BotApi()
    # Built before `start_the_bot`, which replaces `Application.builder`.
    application = (Application.builder().token(TOKEN)
                   .request(HTTPXRequest(httpx_kwargs={
                       "transport": httpx.MockTransport(api.handle)}))
                   .updater(None).job_queue(None).build())
    started = start_the_bot(monkeypatch, tmp_path)
    monkeypatch.setattr(bot_config, "ADMIN_USER_IDS", set(admins))
    # `start_the_bot` pointed the store's file into `tmp_path`.
    previous = use_bot_store(store_sqlite.SqliteBotStore())
    try:
        database.init_database()
        yield BotWorld(api, application, started)
    finally:
        use_bot_store(previous)


__all__ = ["BotApi", "BotWorld", "TOKEN", "bot_world", "buttons"]
