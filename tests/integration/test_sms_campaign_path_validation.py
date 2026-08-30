"""The `{campaign}` path parameter must be validated like the query variant.

`_sms_segment_params` constrains the campaign name with `_CAMPAIGN_PATTERN`
(`^[A-Za-z0-9][A-Za-z0-9_-]{0,39}$`) wherever the name arrives as a *query*
parameter — creating a campaign, exporting a CSV. The three name-addressed
routes take it as a *path* parameter instead:

* POST /api/customers/sms-campaigns/{campaign}/sent
* POST /api/customers/sms-campaigns/{campaign}/send   (spends money, irreversible)
* GET  /api/customers/sms-campaigns/{campaign}/results

If those path parameters carry no pattern, a name that the create path could
never mint — arbitrary length, spaces, control/percent characters — sails
straight into the handler and the store lookup. The contract is meant to be one
charset for the campaign name everywhere; this pins it.
"""
import time
from urllib.parse import quote

import pytest
from fastapi.testclient import TestClient

from web.main import app
from web.routes.auth import session_serializer, create_session_data, SESSION_COOKIE
from core.permissions import ADMIN_USER_IDS

ADMIN_ID = sorted(ADMIN_USER_IDS)[0]


def _headers() -> dict:
    data = create_session_data(
        {
            "id": str(ADMIN_ID),
            "first_name": "Test",
            "username": "tester",
            "auth_date": str(int(time.time())),
        },
        role="admin",
    )
    return {"Cookie": f"{SESSION_COOKIE}={session_serializer.dumps(data)}"}


class _FakeStore:
    """Records which store method a request reached, then behaves like a miss."""

    def __init__(self):
        self.reached = []

    async def mark_sms_campaign_sent(self, campaign, sent_at=None):
        self.reached.append(("sent", campaign))
        raise ValueError(f"campaign {campaign!r} not found")

    async def get_sms_campaign_targets(self, campaign):
        self.reached.append(("send", campaign))
        raise ValueError(f"campaign {campaign!r} not frozen")

    async def get_sms_campaign_results(self, campaign, window_days=30, delivered_only=False):
        self.reached.append(("results", campaign))
        raise ValueError(f"campaign {campaign!r} not frozen")


@pytest.fixture(autouse=True)
def reset_rate_limiter():
    from web.ratelimit import limiter

    limiter.reset()
    yield
    limiter.reset()


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def store(monkeypatch):
    fake = _FakeStore()

    async def _fake_get_store():
        return fake

    monkeypatch.setattr("web.routes.api.customers.get_store", _fake_get_store)
    return fake


# Each entry: (http method, url template with {c}, the *other* required params).
# `send` needs a valid `text`, otherwise a missing-text 422 would mask whether
# the campaign name itself was rejected.
_ROUTES = [
    ("post", "/api/customers/sms-campaigns/{c}/sent", {}),
    ("post", "/api/customers/sms-campaigns/{c}/send", {"text": "hello"}),
    ("get", "/api/customers/sms-campaigns/{c}/results", {}),
]

# Names the create path (which does enforce _CAMPAIGN_PATTERN) could never mint.
_INVALID_NAMES = [
    "a" * 41,            # too long (pattern caps at 40 chars)
    "has space",         # space is not in the charset
    "bad$name",          # punctuation outside [-_]
    "_leading",          # first char must be alphanumeric
    "drop;table",        # semicolon
]


def _do(client, method, url, params):
    return getattr(client, method)(url, params=params, headers=_headers())


@pytest.mark.parametrize("method,tmpl,extra", _ROUTES)
@pytest.mark.parametrize("name", _INVALID_NAMES)
def test_invalid_campaign_name_in_path_is_rejected(client, store, method, tmpl, extra, name):
    url = tmpl.format(c=quote(name, safe=""))
    r = _do(client, method, url, extra)

    assert r.status_code == 422, (
        f"{method.upper()} {url} accepted invalid campaign name {name!r} "
        f"(got {r.status_code}, reached store: {store.reached})"
    )
    # And validation must reject it *before* the handler touches the store.
    assert store.reached == [], (
        f"invalid name {name!r} reached the store: {store.reached}"
    )


@pytest.mark.parametrize("method,tmpl,extra", _ROUTES)
def test_valid_campaign_name_in_path_is_accepted(client, store, method, tmpl, extra):
    """A name the create path could mint must pass validation and reach the store."""
    url = tmpl.format(c="sep-brand_2026")
    r = _do(client, method, url, extra)

    assert r.status_code != 422, r.text
    assert store.reached, "a valid campaign name never reached the store"
    assert store.reached[-1][1] == "sep-brand_2026"
