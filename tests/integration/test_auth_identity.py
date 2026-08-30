"""The identity foundation: how a caller's Telegram id is established, and what
the session cookie is trusted for afterwards.

Every ownership and role check in this application resolves to "which user id
is this session". If that id can be chosen by the caller, nothing downstream
matters — so the two unauthenticated login endpoints and the session resolver
are pinned here, separately from the broad surface sweep in
tests/integration/test_security_hardening.py.

The bot token used throughout is built in-process and is not the real one; the
suite's conftest already replaces the environment's credentials before any test
module is imported.
"""
import hashlib
import hmac
import json
import time
from urllib.parse import quote

import pytest
from fastapi.testclient import TestClient

from web.main import app

# Obviously fake, and shaped like the real thing (digits, colon, secret).
FAKE_BOT_TOKEN = "123456:AAH-fake-token-for-tests-only-not-real"


@pytest.fixture
def client():
    """TestClient without startup events (no DuckDB / scheduler).

    `raise_server_exceptions=False` so an unhandled exception in a handler
    arrives as the 500 a real caller would receive, rather than being re-raised
    into the test — the difference between "this endpoint answers wrongly" and
    "this test crashed".
    """
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture(autouse=True)
def fake_token(monkeypatch):
    """Both verifiers read the module-level binding, not bot.config."""
    monkeypatch.setattr("web.services.auth_service.BOT_TOKEN", FAKE_BOT_TOKEN)


# ─── Fixture builders: the two signatures, exactly as Telegram specifies ──────

def signed_widget_data(token: str = FAKE_BOT_TOKEN, **overrides) -> dict:
    """Login Widget callback params, signed with secret_key = SHA256(token).

    https://core.telegram.org/widgets/login-legacy — data_check_string is every
    received field except `hash`, sorted, `key=value`, newline-separated.
    """
    data = {
        "id": "12345",
        "first_name": "Valid",
        "username": "validuser",
        "auth_date": str(int(time.time())),
    }
    data.update(overrides)
    secret = hashlib.sha256(token.encode()).digest()
    check = "\n".join(f"{k}={data[k]}" for k in sorted(data))
    data["hash"] = hmac.new(secret, check.encode(), hashlib.sha256).hexdigest()
    return data


def signed_init_data(token: str = FAKE_BOT_TOKEN, secret_mode: str = "webapp", **overrides) -> str:
    """WebApp initData, signed with secret_key = HMAC_SHA256("WebAppData", token).

    `secret_mode="widget"` derives the key the *other* way (SHA256(token)) to
    exercise the classic mix-up: using the Login Widget construction to verify
    initData, or the reverse, must reject.
    """
    fields = {
        "auth_date": str(int(time.time())),
        "query_id": "AAF-test",
        "user": json.dumps({"id": 12345, "first_name": "Valid", "username": "validuser"}),
    }
    fields.update(overrides)
    if secret_mode == "widget":
        secret = hashlib.sha256(token.encode()).digest()
    else:
        secret = hmac.new(b"WebAppData", token.encode(), hashlib.sha256).digest()
    check = "\n".join(f"{k}={fields[k]}" for k in sorted(fields))
    fields["hash"] = hmac.new(secret, check.encode(), hashlib.sha256).hexdigest()
    return "&".join(f"{k}={quote(fields[k], safe='')}" for k in fields)


# ─── The received hash is attacker-controlled and reaches compare_digest ──────

class TestMalformedHashIsRejectedNotRaised:
    """`hmac.compare_digest` raises TypeError when either str holds a non-ASCII
    character, and the hash being compared arrived in a query string. A verifier
    that raises where it should return False turns "invalid authentication data"
    into an unhandled 500 on the two endpoints that, by design, anyone on the
    internet may call without a session.

    No privilege is gained either way — nothing mints a session down this path.
    What is gained is an unauthenticated remote 500 plus an ERROR-level
    traceback per request, on the login path, from a single character.
    """

    NON_ASCII_HASH = "é" * 64

    def test_widget_verifier_returns_false(self):
        from web.services.auth_service import verify_telegram_auth

        data = signed_widget_data()
        data["hash"] = self.NON_ASCII_HASH
        assert verify_telegram_auth(data) is False

    def test_webapp_verifier_returns_none(self):
        from web.services.auth_service import verify_webapp_auth

        init_data = signed_init_data()
        init_data = init_data.rsplit("&hash=", 1)[0] + "&hash=" + quote(self.NON_ASCII_HASH)
        assert verify_webapp_auth(init_data) is None

    def test_callback_redirects_to_login(self, client):
        data = signed_widget_data()
        data["hash"] = self.NON_ASCII_HASH
        r = client.get("/auth/telegram/callback", params=data, follow_redirects=False)
        assert r.status_code == 302, \
            f"a malformed hash answered {r.status_code}; invalid auth data is a " \
            f"redirect back to /login, not a server error"
        assert "/login" in r.headers["location"]

    def test_webapp_answers_401(self, client):
        init_data = signed_init_data()
        init_data = init_data.rsplit("&hash=", 1)[0] + "&hash=" + quote(self.NON_ASCII_HASH)
        r = client.post("/auth/webapp", json={"initData": init_data})
        assert r.status_code == 401, \
            f"a malformed hash answered {r.status_code}; unverifiable initData " \
            f"is 401, not a server error"

    @pytest.mark.parametrize(
        "bad_hash",
        ["", "not-hex", "0" * 63, "0" * 65, "ZZ" * 32],
        ids=["empty", "not-hex", "short", "long", "non-hex-chars"],
    )
    def test_other_malformed_hashes_are_rejected(self, bad_hash):
        """The ASCII ones already answered False — pinned so the fix for the
        non-ASCII case cannot accidentally start accepting one of these."""
        from web.services.auth_service import verify_telegram_auth

        data = signed_widget_data()
        data["hash"] = bad_hash
        assert verify_telegram_auth(data) is False


# ─── Can a caller choose someone else's Telegram id? ─────────────────────────

class TestTelegramIdCannotBeForged:
    """The id that becomes `session['user_id']` is `auth_data['id']` — a query
    parameter. Everything that makes it unforgeable is the HMAC, so the two
    constructions are pinned against the published spec rather than against
    themselves: a test that signs with the same helper the code uses would pass
    for any construction, correct or not.
    """

    def test_widget_accepts_a_correctly_signed_id(self):
        from web.services.auth_service import verify_telegram_auth

        assert verify_telegram_auth(signed_widget_data()) is True

    @pytest.mark.parametrize("field", ["id", "auth_date", "username", "first_name"])
    def test_widget_rejects_any_field_changed_after_signing(self, field):
        from web.services.auth_service import verify_telegram_auth

        data = signed_widget_data()
        data[field] = "99999"
        assert verify_telegram_auth(data) is False

    def test_widget_rejects_the_webapp_key_derivation(self):
        """secret_key = SHA256(token) here, HMAC(token, key="WebAppData") there.

        Using one construction to verify the other's data is the classic bug in
        this pair, and it is silent: both are HMAC-SHA256 over the same string.
        """
        from web.services.auth_service import verify_telegram_auth

        data = signed_widget_data()
        wrong_secret = hmac.new(b"WebAppData", FAKE_BOT_TOKEN.encode(), hashlib.sha256).digest()
        check = "\n".join(f"{k}={data[k]}" for k in sorted(data) if k != "hash")
        data["hash"] = hmac.new(wrong_secret, check.encode(), hashlib.sha256).hexdigest()
        assert verify_telegram_auth(data) is False

    def test_widget_rejects_a_different_bot_token(self):
        from web.services.auth_service import verify_telegram_auth

        assert verify_telegram_auth(signed_widget_data(token="999:another-fake-token")) is False

    def test_extra_query_params_cannot_ride_along_unsigned(self):
        """The callback hands `dict(request.query_params)` to the verifier, so
        an attacker may append anything. Every appended field enters the
        data-check-string, so the signature covers the whole parameter set —
        there is no field that travels unsigned.
        """
        from web.services.auth_service import verify_telegram_auth

        data = signed_widget_data()
        data["role"] = "admin"
        assert verify_telegram_auth(data) is False

    @pytest.mark.parametrize(
        "missing", ["id", "hash", "auth_date"], ids=["no-id", "no-hash", "no-auth_date"],
    )
    def test_widget_requires_the_three_load_bearing_fields(self, missing):
        from web.services.auth_service import verify_telegram_auth

        data = signed_widget_data()
        del data[missing]
        assert verify_telegram_auth(data) is False

    def test_widget_rejects_stale_auth_data(self):
        """A captured callback URL replays until AUTH_DATA_MAX_AGE and no longer."""
        from web.services.auth_service import verify_telegram_auth, AUTH_DATA_MAX_AGE

        stale = str(int(time.time()) - AUTH_DATA_MAX_AGE - 60)
        assert verify_telegram_auth(signed_widget_data(auth_date=stale)) is False


class TestWebAppIdCannotBeForged:
    """The WebApp path had no test of any kind. It reaches the same
    `create_session_data`, so a forgeable id here is a forgeable id everywhere.
    """

    def test_valid_init_data_yields_the_signed_user(self):
        from web.services.auth_service import verify_webapp_auth

        user = verify_webapp_auth(signed_init_data())
        assert user is not None
        assert user["id"] == 12345

    def test_rejects_the_widget_key_derivation(self):
        from web.services.auth_service import verify_webapp_auth

        assert verify_webapp_auth(signed_init_data(secret_mode="widget")) is None

    def test_rejects_a_tampered_user_field(self):
        from web.services.auth_service import verify_webapp_auth

        init_data = signed_init_data()
        forged = quote(json.dumps({"id": 999, "first_name": "Mallory"}), safe="")
        head, _, tail = init_data.partition("&user=")
        original, _, rest = tail.partition("&")
        assert verify_webapp_auth(f"{head}&user={forged}&{rest}") is None

    def test_a_second_user_field_cannot_override_the_signed_one(self):
        """`parse_qs` returns a list per key and the verifier takes `[0]` for
        both the data-check-string and the JSON it parses. Taking different
        elements in those two places is how a signed payload gets verified and
        an unsigned one gets used; the same index in both is what forbids it.
        """
        from web.services.auth_service import verify_webapp_auth

        forged = quote(json.dumps({"id": 999, "first_name": "Mallory"}), safe="")
        user = verify_webapp_auth(signed_init_data() + f"&user={forged}")
        assert user is not None
        assert user["id"] == 12345, "the unsigned duplicate was used to build the session"

    def test_user_json_is_only_trusted_after_the_hmac(self):
        """Unverifiable initData must not yield a user dict even when its
        `user` field is perfectly well-formed JSON."""
        from web.services.auth_service import verify_webapp_auth

        init_data = signed_init_data(token="999:another-fake-token")
        assert verify_webapp_auth(init_data) is None

    def test_rejects_stale_init_data(self):
        from web.services.auth_service import verify_webapp_auth, WEBAPP_DATA_MAX_AGE

        stale = str(int(time.time()) - WEBAPP_DATA_MAX_AGE - 60)
        assert verify_webapp_auth(signed_init_data(auth_date=stale)) is None

    @pytest.mark.parametrize("blank", ["", "   "], ids=["empty", "whitespace"])
    def test_rejects_blank_init_data(self, blank):
        from web.services.auth_service import verify_webapp_auth

        assert verify_webapp_auth(blank) is None


# ─── What the cookie is trusted for ──────────────────────────────────────────

def make_cookie(user_id: int, role: str = "admin") -> str:
    from web.routes.auth import create_session_data, session_serializer

    return session_serializer.dumps(create_session_data(
        {
            "id": str(user_id),
            "first_name": "Test",
            "username": "tester",
            "auth_date": str(int(time.time())),
        },
        role=role,
    ))


def install_store(monkeypatch, status: str = "approved", role: str = "viewer"):
    """Answer `_resolve_session`'s DuckDB lookup without a database.

    `_resolve_session` imports `get_store` from `core.duckdb_store` at call
    time, so the module attribute is the patch target.
    """
    class _Store:
        async def get_user(self, uid):
            return None if status is None else {"user_id": uid, "status": status, "role": role}

    async def _get_store():
        return _Store()

    monkeypatch.setattr("core.duckdb_store.get_store", _get_store)


class TestOnlyTheUserIdIsTakenFromTheCookie:
    """`_resolve_session` re-reads role and status per request. The cookie is a
    signed assertion of *identity* and nothing else — which is the property the
    whole role model rests on, and it had no test.
    """

    ORDINARY_ID = 555_000_111

    @pytest.fixture(autouse=True)
    def _not_a_hardcoded_admin(self):
        from core.permissions import ADMIN_USER_IDS

        assert self.ORDINARY_ID not in ADMIN_USER_IDS, \
            "this id must be an ordinary account for these tests to mean anything"

    @pytest.mark.asyncio
    async def test_a_forged_admin_role_resolves_to_the_stored_one(self, monkeypatch):
        from web.routes.auth import _resolve_session

        install_store(monkeypatch, status="approved", role="viewer")
        session = await _resolve_session(make_cookie(self.ORDINARY_ID, role="admin"))
        assert session is not None
        assert session["role"] == "viewer", \
            "the cookie's own role was honoured — anyone who can sign a cookie is an admin"

    def test_a_forged_admin_role_is_403_at_an_admin_endpoint(self, client, monkeypatch):
        """The structural claim, read as an answer rather than a return value."""
        install_store(monkeypatch, status="approved", role="viewer")
        r = client.get(
            "/api/jobs",
            headers={"Cookie": f"dashboard_session={make_cookie(self.ORDINARY_ID, role='admin')}"},
        )
        assert r.status_code == 403

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", ["denied", "frozen", "pending", "unknown"])
    async def test_a_valid_cookie_dies_with_the_account(self, monkeypatch, status):
        """`/logout` only clears the cookie client-side and nothing revokes a
        signed one, so revocation is entirely this re-read. A cookie stays
        valid for seven days; the account behind it must not.
        """
        from web.routes.auth import _resolve_session

        install_store(monkeypatch, status=status, role="viewer")
        assert await _resolve_session(make_cookie(self.ORDINARY_ID, role="viewer")) is None

    @pytest.mark.asyncio
    async def test_a_deleted_account_falls_back_and_is_never_admin(self, monkeypatch):
        """No DuckDB row sends the resolver to the SQLite path, which cannot
        supply a fresh role — so it must downgrade rather than honour the
        cookie's. A demoted admin keeps a signed `role=admin` cookie for a week.
        """
        from web.routes.auth import _resolve_session

        install_store(monkeypatch, status=None)
        monkeypatch.setattr(
            "web.routes.auth.check_user_access",
            lambda uid: {"authorized": True, "status": "approved", "role": "admin"},
        )
        session = await _resolve_session(make_cookie(self.ORDINARY_ID, role="admin"))
        assert session is not None
        assert session["role"] == "viewer"

    @pytest.mark.asyncio
    async def test_an_unauthorized_fallback_resolves_to_none(self, monkeypatch):
        from web.routes.auth import _resolve_session

        install_store(monkeypatch, status=None)
        monkeypatch.setattr(
            "web.routes.auth.check_user_access",
            lambda uid: {"authorized": False, "status": "denied"},
        )
        assert await _resolve_session(make_cookie(self.ORDINARY_ID)) is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "cookie",
        ["", "not-a-session", "eyJ1c2VyX2lkIjo5OTk5fQ.aaaaaa.bbbbbbbbbbbbbbbbbbbbbbbbbbbb"],
        ids=["empty", "garbage", "unsigned-payload"],
    )
    async def test_an_unsigned_cookie_is_refused(self, monkeypatch, cookie):
        from web.routes.auth import _resolve_session

        install_store(monkeypatch, status="approved", role="admin")
        assert await _resolve_session(cookie) is None

    @pytest.mark.asyncio
    async def test_a_cookie_signed_with_another_key_is_refused(self, monkeypatch):
        """The signing key is the whole boundary — pinned so that a change to
        how `SECRET_KEY` is chosen has to face this test.
        """
        from itsdangerous import URLSafeTimedSerializer
        from web.routes.auth import _resolve_session

        install_store(monkeypatch, status="approved", role="admin")
        foreign = URLSafeTimedSerializer("a-different-key-entirely")
        forged = foreign.dumps({"user_id": self.ORDINARY_ID, "role": "admin"})
        assert await _resolve_session(forged) is None


class TestSessionSigningKeyIsNotTheBotToken:
    """`web/routes/auth.py` falls back to BOT_TOKEN when DASHBOARD_SECRET_KEY is
    unset, and the token is also the Telegram HMAC key, lives in the bot
    process, and has appeared in logs. With the fallback in effect, anyone
    holding it can mint a cookie for any approved id.

    What contains that is `validate_config(require_secret_key=True)` at web
    startup, which raises in production — the fallback is a development
    convenience that cannot reach a production container. That containment is
    one `if` in another module with no test, and it is the only thing standing
    between a known-leaked token and arbitrary session forgery.
    """

    def _config_with(self, monkeypatch, url: str, secret: str | None):
        # `core/__init__.py` does `from core.config import config`, which
        # rebinds the package attribute `core.config` from the submodule to the
        # AppConfig instance — so `import core.config as x` hands back the
        # instance. sys.modules is the only way to reach the module itself.
        import sys
        import core.config  # noqa: F401 — ensures the entry exists

        core_config = sys.modules["core.config"]

        monkeypatch.setenv("DASHBOARD_URL", url)
        if secret is None:
            monkeypatch.delenv("DASHBOARD_SECRET_KEY", raising=False)
        else:
            monkeypatch.setenv("DASHBOARD_SECRET_KEY", secret)
        monkeypatch.setattr(core_config, "config", core_config.AppConfig())

    def test_production_refuses_to_start_without_a_dedicated_key(self, monkeypatch):
        from core.config import validate_config, ConfigurationError

        self._config_with(monkeypatch, "https://dashboard.invalid", None)
        with pytest.raises(ConfigurationError, match="DASHBOARD_SECRET_KEY"):
            validate_config(require_bot=False, require_api=True, require_secret_key=True)

    def test_production_starts_with_one(self, monkeypatch):
        from core.config import validate_config

        self._config_with(monkeypatch, "https://dashboard.invalid", "a-dedicated-signing-key")
        validate_config(require_bot=False, require_api=True, require_secret_key=True)

    def test_the_guard_keys_on_the_dashboard_url(self, monkeypatch):
        """Named rather than discovered later: the production test is
        `DASHBOARD_URL` starting https, so an unset or http URL disengages the
        guard and restores the BOT_TOKEN fallback silently.
        """
        from core.config import validate_config

        self._config_with(monkeypatch, "http://localhost:8080", None)
        validate_config(require_bot=False, require_api=True, require_secret_key=True)

    def test_the_web_startup_asks_for_the_guard(self, monkeypatch):
        """The guard is opt-in per service; the web app is the service that
        signs sessions, so it is the one that must pass require_secret_key.
        """
        import inspect
        import web.main

        source = inspect.getsource(web.main)
        assert "require_secret_key=True" in source
        assert "SystemExit(1)" in source, \
            "a logged-and-ignored ConfigurationError would boot with the fallback key"


class TestSessionCookieAttributes:
    """A stolen cookie is a full session for seven days, so the transport
    attributes are load-bearing rather than hygiene.
    """

    def test_callback_sets_an_httponly_lax_cookie(self, client, monkeypatch):
        from web.routes.auth import COOKIE_SECURE

        async def _access(user_id, auth_data=None):
            return {"authorized": True, "status": "approved", "role": "viewer"}

        monkeypatch.setattr("web.routes.auth.check_user_access_async", _access)
        r = client.get(
            "/auth/telegram/callback",
            params=signed_widget_data(),
            follow_redirects=False,
        )
        assert r.status_code == 302
        cookie = r.headers["set-cookie"]
        assert "HttpOnly" in cookie, "JS could read the session"
        assert "SameSite=lax" in cookie
        assert ("Secure" in cookie) is COOKIE_SECURE

    def test_webapp_never_returns_the_session_in_the_body(self, client, monkeypatch):
        """It used to, and the page set it with `document.cookie` — which
        defeats HttpOnly entirely. The cookie is set server-side now.
        """
        async def _access(user_id, auth_data=None):
            return {"authorized": True, "status": "approved", "role": "viewer"}

        monkeypatch.setattr("web.routes.auth.check_user_access_async", _access)
        r = client.post("/auth/webapp", json={"initData": signed_init_data()})
        assert r.status_code == 200
        assert r.json() == {"success": True}
        assert "HttpOnly" in r.headers["set-cookie"]


class TestCallbackRedirectIsNotCallerControlled:
    """The callback redirects to `/?welcome=<first_name>`, and `first_name`
    reaches it from the query string. `quote()` leaves `/` unescaped by
    default, so the check is that the value stays inside the query component
    and cannot become a host.
    """

    @pytest.mark.parametrize(
        "first_name",
        ["/evil.example", "//evil.example", "\\evil.example", "x?next=//evil.example", "x#@evil.example"],
    )
    def test_a_hostile_first_name_cannot_move_the_redirect_off_site(
        self, client, monkeypatch, first_name
    ):
        async def _access(user_id, auth_data=None):
            return {"authorized": True, "status": "approved", "role": "viewer"}

        monkeypatch.setattr("web.routes.auth.check_user_access_async", _access)
        r = client.get(
            "/auth/telegram/callback",
            params=signed_widget_data(first_name=first_name),
            follow_redirects=False,
        )
        location = r.headers["location"]
        assert location.startswith("/?welcome=") or location == "/", location
        assert not location.startswith("//"), f"protocol-relative redirect: {location}"


class TestMeTouchesOnlyTheSessionsOwnRow:
    """`/api/me` and `PATCH /api/me/preferences` take no identifier at all —
    the user id comes from the signed cookie, and `write_language` binds it.

    An IDOR pass judged these endpoints exempt on that reading. What makes the
    judgement worth a test is how easily it stops being true: a `user_id`
    parameter added here so an admin can fix somebody's language would turn the
    one endpoint every approved user may write through into a way to write
    everyone's row, and nothing else in the file would notice.
    """

    ORDINARY_ID = 555_000_666
    SOMEONE_ELSE = 555_000_777

    @pytest.fixture
    def recorded(self, monkeypatch):
        """The two bot_prefs functions, recording which user they were asked
        about. Bound into `web.routes.api.me` at import, so that is the target;
        the real ones would open the bot's SQLite.
        """
        calls = []

        def _write(user_id, language):
            calls.append(("write", user_id, language))
            return language

        def _read(user_id, default="uk"):
            calls.append(("read", user_id))
            return default

        monkeypatch.setattr("web.routes.api.me.write_language", _write)
        monkeypatch.setattr("web.routes.api.me.read_language", _read)
        return calls

    def _headers(self):
        return {"Cookie": f"dashboard_session={make_cookie(self.ORDINARY_ID, role='viewer')}"}

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"json": {"language": "ru", "user_id": SOMEONE_ELSE}},
            {"json": {"language": "ru", "id": SOMEONE_ELSE, "user": {"id": SOMEONE_ELSE}}},
            {"json": {"language": "ru"}, "params": {"user_id": SOMEONE_ELSE}},
            {"json": {"language": "ru"}, "headers": {"X-User-Id": str(SOMEONE_ELSE)}},
        ],
        ids=["body-user_id", "body-nested-id", "query-param", "header"],
    )
    def test_no_supplied_identifier_redirects_the_write(
        self, client, monkeypatch, recorded, kwargs
    ):
        install_store(monkeypatch, status="approved", role="viewer")
        headers = {**self._headers(), **kwargs.pop("headers", {})}

        r = client.patch("/api/me/preferences", headers=headers, **kwargs)

        assert r.status_code == 200
        assert recorded == [("write", self.ORDINARY_ID, "ru")]

    def test_the_read_is_the_sessions_own_row_too(self, client, monkeypatch, recorded):
        install_store(monkeypatch, status="approved", role="viewer")

        r = client.get("/api/me", headers=self._headers(), params={"user_id": self.SOMEONE_ELSE})

        assert r.status_code == 200
        assert r.json()["user"]["id"] == self.ORDINARY_ID
        assert [c for c in recorded if c[0] == "read"] == [("read", self.ORDINARY_ID)]

    def test_an_unsupported_language_is_refused_before_any_write(
        self, client, monkeypatch, recorded
    ):
        install_store(monkeypatch, status="approved", role="viewer")

        r = client.patch(
            "/api/me/preferences", headers=self._headers(), json={"language": "de"},
        )

        assert r.status_code == 400
        assert recorded == []
