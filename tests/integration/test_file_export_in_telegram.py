"""Exports inside Telegram: the bot sends the file to the requester's own chat.

Telegram's Mini App WebView cannot save a `blob:` download, and hands
`window.open` to an external browser with no session, so every export button
did nothing there. The page now fetches the CSV as it would anyway and posts
it here; the bot sends it as a document to the session's own Telegram id and
nobody else. Plus the guard that keeps every button on that one path.
"""
import re
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from core import telegram_user_files
from web.main import app
from web.routes.api._deps import limiter

# The real sender, taken before `tests/conftest.py` stubs it for every test,
# for the two tests about the sender itself.
_REAL_SEND = telegram_user_files.send_user_document_http
from tests.integration.test_tab_permissions import (
    PLAIN_ID,
    _cookie,
    _install_store,
    _FakeStore,
    _fresh_permissions,  # noqa: F401  (autouse fixture)
)

URL = "/api/me/send-file"


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture(autouse=True)
def _no_rate_limit():
    limiter.reset()
    yield
    limiter.reset()


@pytest.fixture
def sent(monkeypatch):
    calls = []

    async def _fake(chat_id, content, filename, caption="", **kw):
        calls.append((chat_id, content, filename))
        return True

    monkeypatch.setattr("core.telegram_user_files.send_user_document_http", _fake)
    return calls


def _login(monkeypatch, allowed_features=None):
    _install_store(monkeypatch, _FakeStore("viewer", allowed_features))
    return _cookie(PLAIN_ID, "viewer")


def test_sends_to_the_requesters_own_chat(client, monkeypatch, sent):
    headers = _login(monkeypatch)
    r = client.post(URL, json={"filename": "sku-all-2026-10-14.csv",
                               "content": "SKU,Name\n1,Крем"}, headers=headers)
    assert r.status_code == 200, r.text
    assert len(sent) == 1
    chat_id, content, filename = sent[0]
    assert chat_id == PLAIN_ID
    assert filename == "sku-all-2026-10-14.csv"
    # One BOM, so Excel reads Cyrillic, and the text intact.
    assert content.decode("utf-8") == "﻿SKU,Name\n1,Крем"


def test_a_bom_from_the_page_is_not_doubled(client, monkeypatch, sent):
    headers = _login(monkeypatch)
    client.post(URL, json={"filename": "a.csv", "content": "﻿x"}, headers=headers)
    assert sent[0][1].decode("utf-8") == "﻿x"


@pytest.mark.parametrize("name", ["../etc/passwd.csv", "a.exe", "a b.csv", ".csv"])
def test_refuses_a_filename_that_is_not_a_plain_csv(client, monkeypatch, sent, name):
    headers = _login(monkeypatch)
    r = client.post(URL, json={"filename": name, "content": "x"}, headers=headers)
    assert r.status_code == 400
    assert sent == []


def test_a_failed_send_is_not_reported_as_sent(client, monkeypatch):
    headers = _login(monkeypatch)

    async def _fail(*a, **k):
        return False

    monkeypatch.setattr("core.telegram_user_files.send_user_document_http", _fail)
    r = client.post(URL, json={"filename": "a.csv", "content": "x"}, headers=headers)
    assert r.status_code == 502


def test_a_narrowed_account_may_send_itself_what_it_was_served(client, monkeypatch, sent):
    """No tab gate: the server reads nothing, the content came from a route
    that already applied this person's tabs, and it goes back to them alone."""
    headers = _login(monkeypatch, allowed_features="traffic")
    r = client.post(URL, json={"filename": "a.csv", "content": "x"}, headers=headers)
    assert r.status_code == 200
    assert [c[0] for c in sent] == [PLAIN_ID]


def test_needs_a_session(client, sent):
    r = client.post(URL, json={"filename": "a.csv", "content": "x"})
    assert r.status_code == 401
    assert sent == []


@pytest.mark.asyncio
async def test_the_kill_switch_silences_it(monkeypatch):
    monkeypatch.setenv("KS_ALERTS_DISABLED", "1")

    class _Boom:
        def __init__(self, *a, **k):
            raise AssertionError("reached the network")

    monkeypatch.setattr(telegram_user_files.httpx, "AsyncClient", _Boom)
    assert await _REAL_SEND(1, b"x", "a.csv") is False


def test_the_suite_cannot_send_a_real_document(client, monkeypatch):
    """`.env` holds the real bot token and the suite runs with the kill switch
    off, so a test reaching the route without its own stub would post to a real
    chat. `tests/conftest.py` stubs the sender for every test; this one has
    none of its own and must reach no client."""
    opened = []
    monkeypatch.setattr(telegram_user_files.httpx, "AsyncClient",
                        lambda *a, **k: opened.append(a) or pytest.fail("reached httpx"))
    headers = _login(monkeypatch)
    r = client.post(URL, json={"filename": "a.csv", "content": "x"}, headers=headers)
    assert r.status_code == 502
    assert opened == []
    assert telegram_user_files.send_user_document_http is not _REAL_SEND


FRONTEND = Path(__file__).resolve().parents[2] / "web" / "frontend" / "src"
# A file handover that breaks inside Telegram: a blob link, or a new window
# Telegram gives to a browser with no session.
_BROKEN_HANDOVER = re.compile(r"createObjectURL|window\.open\(|\.download\s*=")
# A JS dialog Telegram Desktop's WebView may never show: `confirm()` returns
# false there and the button does nothing.
_JS_DIALOG = re.compile(r"(?<![\w.])(?:window\.)?(?:confirm|alert|prompt)\(")
# The SMS roster (names and phone numbers) is deliberately not put in a chat
# without the owner's decision; it keeps the browser link for now.
_ALLOWED = {"utils/deliverFile.ts", "components/SmsCampaignWizard.tsx"}


def test_every_file_handover_goes_through_deliver_file():
    offenders = []
    for path in FRONTEND.rglob("*.ts*"):
        rel = path.relative_to(FRONTEND).as_posix()
        if rel in _ALLOWED or ".stories." in rel or "__tests__" in rel:
            continue
        for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if line.lstrip().startswith("//"):
                continue
            if _BROKEN_HANDOVER.search(line) or _JS_DIALOG.search(line):
                offenders.append(f"{rel}:{n}: {line.strip()}")
    assert offenders == [], "\n".join(offenders)


# Telegram is detected, and its SDK loaded, in one module. The exports and the
# wizard's native buttons each had a test of their own once, and they differed
# on a same-origin frame: Storybook was Telegram for the exports alone.
_DETECTS_TELEGRAM = re.compile(
    r"TelegramWebviewProxy|telegram-web-app\.js|function\s+insideTelegram\b")
_DETECTOR = "utils/telegramMiniApp.ts"


def _code_lines(path: Path):
    for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.lstrip().startswith(("//", "*", "/*")):
            yield n, line


def test_telegram_is_detected_in_one_module():
    offenders = []
    for path in FRONTEND.rglob("*.ts*"):
        rel = path.relative_to(FRONTEND).as_posix()
        if rel == _DETECTOR or ".stories." in rel or "__tests__" in rel:
            continue
        offenders += [f"{rel}:{n}: {line.strip()}" for n, line in _code_lines(path)
                      if _DETECTS_TELEGRAM.search(line)]
    assert offenders == [], "\n".join(offenders)
    assert any(_DETECTS_TELEGRAM.search(line)
               for _, line in _code_lines(FRONTEND / _DETECTOR)), "the detector moved"


def test_the_exports_ask_the_same_detector():
    imports = [line for _, line in _code_lines(FRONTEND / "utils/deliverFile.ts")
               if line.startswith("import ")]
    assert any(re.search(r"\{[^}]*\binsideTelegram\b[^}]*\}\s*from\s*'\./telegramMiniApp'", line)
               for line in imports), imports
