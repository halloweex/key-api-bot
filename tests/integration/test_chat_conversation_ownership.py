"""A chat conversation belongs to the user who created it.

Conversations live in a process-global dict (`ChatService._conversations`)
keyed by a client-supplied `conversation_id`. Nothing tied that id to a user,
so any admin who presented another admin's `conversation_id` continued that
other person's conversation: their message was appended to the foreign history,
and the model's reply was generated with the foreign history in context — a
horizontal (IDOR) access to another user's chat, which carries customer PII and
business questions pulled through the chat tools.

These tests drive the real router (`/api/chat`, `/api/chat/stream`) with two
distinct admin sessions and a stub LLM that echoes the earliest user message,
so a leak of one user's history into another user's response is directly
observable.
"""
import time

import pytest
from fastapi.testclient import TestClient

import web.services.chat_service as chat_service_module
from web.services.chat_service import ChatService
from web.main import app
from web.routes.auth import (
    SESSION_COOKIE,
    create_session_data,
    session_serializer,
)

USER_A = 111_000_111
USER_B = 222_000_222


# ─── Stub LLM ────────────────────────────────────────────────────────────────

class _FakeLLM:
    """Deterministic, offline stand-in for the Anthropic client.

    Its reply echoes the earliest plain-text user message in the history it is
    handed. That makes cross-user leakage visible: if user B's request is run
    against user A's conversation, the history begins with A's message and B's
    reply contains it.
    """

    @property
    def is_available(self) -> bool:
        return True

    @staticmethod
    def _earliest_user_text(messages) -> str:
        for m in messages:
            if m.get("role") == "user" and isinstance(m.get("content"), str):
                return m["content"]
        return ""

    async def chat(self, messages, tools=None, max_tokens=1000):
        return {
            "content": f"echo:{self._earliest_user_text(messages)}",
            "usage": {"input_tokens": 1, "output_tokens": 1},
        }

    async def chat_stream(self, messages, tools=None, max_tokens=1000):
        yield {"type": "text", "text": f"echo:{self._earliest_user_text(messages)}"}
        yield {"type": "end", "usage": {"input_tokens": 1, "output_tokens": 1}}


# ─── Fixtures ────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _reset_rate_limiter():
    from web.ratelimit import limiter
    limiter.reset()
    yield
    limiter.reset()


@pytest.fixture(autouse=True)
def _fresh_chat_service(monkeypatch):
    """Give every test a clean, offline chat service.

    `_conversations` is a class attribute, so it survives across instances and
    tests; reset it. Swap the LLM for the stub and rebuild the singleton so the
    route's `get_chat_service()` picks it up.
    """
    ChatService._conversations = {}
    monkeypatch.setattr(chat_service_module, "get_llm_client", lambda: _FakeLLM())
    monkeypatch.setattr(chat_service_module, "_chat_service", None, raising=False)
    yield
    ChatService._conversations = {}
    monkeypatch.setattr(chat_service_module, "_chat_service", None, raising=False)


@pytest.fixture(autouse=True)
def _sessions_from_cookie(monkeypatch):
    """Resolve the session straight from the signed cookie, as an admin.

    The real resolver would consult DuckDB/SQLite for a role; here the cookie
    carries the user_id and both test users are treated as admins, which is the
    exact population these endpoints are restricted to.
    """
    async def _resolve(session):
        if not session:
            return None
        try:
            data = session_serializer.loads(session, max_age=7 * 24 * 3600)
        except Exception:
            return None
        return {"user_id": data["user_id"], "role": "admin"}

    monkeypatch.setattr("web.routes.auth._resolve_session", _resolve)


def _cookie(user_id: int) -> str:
    return session_serializer.dumps(create_session_data(
        {
            "id": str(user_id),
            "first_name": "Test",
            "last_name": "User",
            "username": "tester",
            "auth_date": str(int(time.time())),
        },
        role="admin",
    ))


def _client(user_id: int) -> TestClient:
    c = TestClient(app)
    c.cookies.set(SESSION_COOKIE, _cookie(user_id))
    return c


# ─── The IDOR ────────────────────────────────────────────────────────────────

def test_user_b_cannot_continue_user_a_conversation():
    """User B presenting A's conversation_id must not reach A's history."""
    a = _client(USER_A)
    b = _client(USER_B)

    created = a.post("/api/chat", json={"message": "A_SECRET_ABC"})
    assert created.status_code == 200, created.text
    conv_id = created.json()["conversation_id"]
    # Sanity: A's own reply reflects A's own message.
    assert "A_SECRET_ABC" in created.json()["content"]

    res = b.post("/api/chat", json={"message": "hello", "conversation_id": conv_id})

    # B must not receive a 200 carrying A's history, and must not have A's
    # secret reflected back.
    assert "A_SECRET_ABC" not in res.text, (
        "user B received user A's conversation history — IDOR"
    )
    assert res.status_code == 404, (
        f"foreign conversation_id must be rejected, got {res.status_code}: {res.text}"
    )

    # A's conversation must be untouched by B (no injected message).
    conv = ChatService._conversations[conv_id]
    assert all(
        m.get("content") != "hello" for m in conv.messages
    ), "user B's message was injected into user A's conversation"


def test_user_b_cannot_continue_user_a_conversation_via_stream():
    """Same rule on the SSE endpoint, where conversation_id is a query param."""
    a = _client(USER_A)
    b = _client(USER_B)

    created = a.post("/api/chat", json={"message": "A_SECRET_STREAM"})
    assert created.status_code == 200, created.text
    conv_id = created.json()["conversation_id"]

    res = b.get(f"/api/chat/stream?message=hi&conversation_id={conv_id}")
    body = res.text
    assert "A_SECRET_STREAM" not in body, (
        "user B streamed user A's conversation history — IDOR"
    )


def test_unknown_conversation_id_is_rejected_not_silently_created():
    """An explicit id that resolves to nothing is a 404, not a silent new chat."""
    b = _client(USER_B)
    res = b.post(
        "/api/chat",
        json={"message": "hello", "conversation_id": "conv_deadbeefdead"},
    )
    assert res.status_code == 404, res.text


# ─── The stream must refuse *before* it is a 200 ─────────────────────────────
#
# The service re-checks ownership inside `chat_stream`, but that body is an
# async generator: it does not run until the route's `async for` pulls the first
# event, by which point the response is already a 200 SSE stream and the refusal
# can only be an in-band `error` event. The route's pre-check is what turns that
# into a status code, and the test above cannot see the difference — it asserts
# only that the foreign history did not leak, which stays true either way.
# Removing the pre-check therefore left the suite green while reintroducing
# exactly the failure the route comment says it fixed. These pin the status.

def test_stream_refuses_foreign_conversation_with_a_status_not_an_event():
    a = _client(USER_A)
    b = _client(USER_B)

    created = a.post("/api/chat", json={"message": "A_SECRET_STATUS"})
    conv_id = created.json()["conversation_id"]

    res = b.get(f"/api/chat/stream?message=hi&conversation_id={conv_id}")

    assert res.status_code == 404, (
        "a foreign conversation_id must fail before the SSE stream opens; got "
        f"{res.status_code} with body {res.text!r}"
    )
    assert "event: error" not in res.text, (
        "refusal arrived in-band on a 200 stream — the client cannot act on it"
    )


def test_stream_refuses_unknown_conversation_with_a_status_not_an_event():
    b = _client(USER_B)

    res = b.get("/api/chat/stream?message=hi&conversation_id=conv_deadbeefdead")

    assert res.status_code == 404, res.text
    assert "event: error" not in res.text


def test_stream_serves_the_owner_a_real_stream():
    """Non-vacuity guard for the two tests above.

    Both assert on a refusal, which a service that were merely unavailable would
    also produce. This proves the same endpoint, in the same fixture, streams
    the stubbed model's output for the conversation's owner — so the 404s are
    the ownership check firing, not the endpoint being dead.
    """
    a = _client(USER_A)

    created = a.post("/api/chat", json={"message": "OWNER_MESSAGE"})
    conv_id = created.json()["conversation_id"]

    res = a.get(f"/api/chat/stream?message=hi&conversation_id={conv_id}")

    assert res.status_code == 200, res.text
    assert "event: chunk" in res.text
    assert "OWNER_MESSAGE" in res.text


# ─── An unowned conversation belongs to nobody, not to everybody ─────────────

def test_unowned_conversation_is_not_resolvable_by_an_anonymous_caller():
    """`None == None` must not be an ownership match.

    `Conversation.user_id` defaults to None, so a conversation created before
    ownership existed — or by any future caller that omits `user_id` — carries
    no owner. Comparing owners with `==` made that row match a caller whose own
    id was None, which is the fail-open direction. Nothing routes there today
    (`_resolve_session` refuses a session without a truthy user_id, so
    `user.get("user_id")` is never None at either endpoint), but that guarantee
    lives in another module and is one edit away from not holding.
    """
    service = ChatService()
    conv_id = service.create_conversation()

    assert ChatService._conversations[conv_id].user_id is None

    with pytest.raises(chat_service_module.ConversationAccessError):
        service.resolve_owned(conv_id, None)

    with pytest.raises(chat_service_module.ConversationAccessError):
        service.resolve_owned(conv_id, USER_A)


def test_owner_is_compared_as_a_string_so_int_and_str_ids_are_one_user():
    """The session carries `user_id` as an int; a query string would carry text.

    Both must name the same owner, or the same person would be locked out of
    their own conversation depending on which entry point they arrived through.
    """
    service = ChatService()
    conv_id = service.create_conversation(user_id=USER_A)

    assert service.resolve_owned(conv_id, USER_A).id == conv_id
    assert service.resolve_owned(conv_id, str(USER_A)).id == conv_id

    with pytest.raises(chat_service_module.ConversationAccessError):
        service.resolve_owned(conv_id, USER_B)


# ─── The legitimate flow still works ─────────────────────────────────────────

def test_owner_can_continue_their_own_conversation():
    a = _client(USER_A)

    first = a.post("/api/chat", json={"message": "first message"})
    assert first.status_code == 200, first.text
    conv_id = first.json()["conversation_id"]

    second = a.post(
        "/api/chat",
        json={"message": "second message", "conversation_id": conv_id},
    )
    assert second.status_code == 200, second.text
    assert second.json()["conversation_id"] == conv_id

    conv = ChatService._conversations[conv_id]
    user_texts = [m["content"] for m in conv.messages if m.get("role") == "user"]
    assert "first message" in user_texts
    assert "second message" in user_texts


def test_new_conversation_without_id_is_created_for_caller():
    a = _client(USER_A)
    res = a.post("/api/chat", json={"message": "hello"})
    assert res.status_code == 200, res.text
    assert res.json()["conversation_id"].startswith("conv_")
