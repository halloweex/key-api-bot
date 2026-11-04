"""The send's guards and the follow-up, through the HTTP routes.

The send used to have one guard of its own (the claim) and one in the browser
(the campaign name typed back). These pin the rest, at the route:

* the typed name, checked by the server, before anything is claimed;
* the balance, read from the gateway after the claim, covering the whole send
  at the configured tariff plus 2% — or the claim is handed back;
* another campaign sent in the last day to these phones — handed back too.

The gateway is the real `TurboSmsClient` over an injected `httpx.MockTransport`,
so the balance is parsed the way production parses it and nothing here can
reach the live gateway. The follow-up routes, the named refusals and the
guards end to end run over a real store on Postgres — `/sms`'s only one since
step 14's S14-08 (they ran over DuckDB until then;
`tests/integration/pg_sms_seed.py`) — and are skipped without `KS_PG_DSN`.
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timedelta

import httpx
import pytest
import pytest_asyncio
from fastapi.testclient import TestClient

from core.permissions import ADMIN_USER_IDS
from core.turbosms import TurboSmsClient, TurboSmsConfig
from tests.integration import pg_sms_seed
from web.main import app
from web.routes.auth import SESSION_COOKIE, create_session_data, session_serializer


ADMIN_ID = sorted(ADMIN_USER_IDS)[0]
SEND = "/api/customers/sms-campaigns/aug/send"
CHANNELS = "/api/customers/sms/channels"
SEGMENTS = "/api/customers/sms-segments"
CREATE = "/api/customers/sms-campaigns"
CSV = "/api/customers/sms-segments/export/csv"


def _headers() -> dict:
    data = create_session_data(
        {"id": str(ADMIN_ID), "first_name": "Test", "username": "tester",
         "auth_date": str(int(time.time()))},
        role="admin",
    )
    return {"Cookie": f"{SESSION_COOKIE}={session_serializer.dumps(data)}"}


@pytest.fixture(autouse=True)
def reset_rate_limiter():
    from web.ratelimit import limiter

    limiter.reset()
    yield
    limiter.reset()


@pytest.fixture
def client():
    return TestClient(app)


# ─── The gateway ────────────────────────────────────────────────────────────

class _Gateway:
    """TurboSMS over a mock transport: a balance, and whatever is sent."""

    def __init__(self):
        self.balance: object = 1_000_000.0
        self.balance_status = 200
        self.calls: list = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path.strip("/")
        self.calls.append(path)
        if path == "user/balance.json":
            if self.balance_status != 200:
                return httpx.Response(self.balance_status, text="upstream error")
            result = {} if self.balance is None else {"balance": self.balance}
            return httpx.Response(200, json={
                "response_code": 0, "response_status": "OK", "response_result": result,
            })
        if path == "message/send.json":
            body = json.loads(request.content)
            return httpx.Response(200, json={
                "response_code": 0, "response_status": "OK",
                "response_result": [
                    {"phone": p, "response_code": 0, "message_id": f"m-{p}",
                     "response_status": "OK"}
                    for p in body["recipients"]
                ],
            })
        return httpx.Response(404, json={"response_code": 404})

    @property
    def sends(self) -> int:
        return self.calls.count("message/send.json")


@pytest.fixture
def gateway(monkeypatch):
    gw = _Gateway()
    config = TurboSmsConfig(token="test-token-not-real", sender="KoreanStory",
                            viber_sender="KoreanStory", price_per_part=1.28)

    def factory(*_a, **_kw):
        return TurboSmsClient(config, transport=httpx.MockTransport(gw.handler))

    monkeypatch.setattr("web.routes.api.customers.TurboSmsClient", factory)
    monkeypatch.setattr("web.routes.api.customers.TurboSmsConfig", lambda: config)
    return gw


# ─── A store that claims the way the real one does ──────────────────────────

class _ClaimingStore:
    def __init__(self, recipients: int = 100):
        self.targets = [{"buyerId": i, "phone": f"38050{i:07d}", "tier": "ALL"}
                        for i in range(1, recipients + 1)]
        self.claimed = False
        self.claims = 0
        self.releases = 0
        self.conflicts: list = []
        self.conflict_error: Exception | None = None
        self.recorded = None

    async def get_sms_campaign_targets(self, campaign):
        if self.claimed:
            raise ValueError(f"campaign {campaign!r} was already sent")
        self.claimed = True
        self.claims += 1
        return list(self.targets)

    async def release_sms_campaign(self, campaign):
        self.claimed = False
        self.releases += 1

    async def find_sms_send_conflicts(self, campaign):
        if self.conflict_error:
            raise self.conflict_error
        return list(self.conflicts)

    async def record_sms_send(self, campaign, accepted, stoplisted, failed, **kw):
        self.recorded = {"accepted": accepted, **kw}
        return {"campaign": campaign, "accepted": len(accepted),
                "stoplisted": len(stoplisted), "failed": len(failed), "notSent": 0}


@pytest.fixture
def store(monkeypatch):
    fake = _ClaimingStore()

    async def _get_store():
        return fake

    monkeypatch.setattr("web.routes.api.customers.get_store", _get_store)
    return fake


def _send(client, text="Знижка 20%", **params):
    return client.post(
        SEND, params={"text": text, "confirm_campaign": "aug", **params},
        headers=_headers(),
    )


# ─── C. The typed name ──────────────────────────────────────────────────────

class TestTheTypedName:

    def test_missing_is_refused_before_the_claim(self, client, store, gateway):
        r = client.post(SEND, params={"text": "hi"}, headers=_headers())

        assert r.status_code == 409
        assert "confirm" in r.json()["detail"]
        # Named, so the page can say it in its reader's language; `detail`
        # stays the English sentence API callers read.
        assert r.json()["reason"] == "confirm_mismatch"
        assert r.json()["campaign"] == "aug"
        assert store.claims == 0, "nothing may be claimed for an unconfirmed send"
        assert gateway.calls == []

    @pytest.mark.parametrize("typed", ["au", "aug2", "AUG", "", "agu"])
    def test_wrong_is_refused_before_the_claim(self, client, store, gateway, typed):
        r = client.post(SEND, params={"text": "hi", "confirm_campaign": typed},
                        headers=_headers())

        assert r.status_code == 409
        assert r.json()["reason"] == "confirm_mismatch"
        assert store.claims == 0
        assert gateway.calls == []

    def test_surrounding_spaces_are_forgiven(self, client, store, gateway):
        r = client.post(SEND, params={"text": "hi", "confirm_campaign": "  aug "},
                        headers=_headers())

        assert r.status_code == 200, r.text
        assert gateway.sends == 1


# ─── A. The balance ─────────────────────────────────────────────────────────

class TestTheBalance:

    def test_a_balance_that_covers_the_send_lets_it_go(self, client, store, gateway):
        gateway.balance = 130.56       # 100 × 1 × 1.28 × 1.02, exactly

        r = _send(client)

        assert r.status_code == 200, r.text
        assert gateway.calls == ["user/balance.json", "message/send.json"]
        assert store.releases == 0

    def test_a_short_balance_refuses_and_hands_the_claim_back(
        self, client, store, gateway,
    ):
        gateway.balance = 100.0

        r = _send(client)

        assert r.status_code == 409
        body = r.json()
        assert body["reason"] == "balance_short"
        assert body["balance"] == 100.0
        assert body["cost"] == 130.56
        assert body["shortfall"] == 30.56
        assert "100.00" in body["detail"] and "130.56" in body["detail"]
        assert gateway.sends == 0, "nothing may reach the gateway"
        assert (store.claims, store.releases, store.claimed) == (1, 1, False)
        assert store.recorded is None

    def test_two_segments_double_the_cost(self, client, store, gateway):
        # 71 Cyrillic characters is two UCS-2 parts — twice the bill for every
        # recipient, which is the cliff nobody sees while typing.
        gateway.balance = 200.0          # covers one part (130.56), not two

        r = _send(client, text="я" * 71)

        assert r.status_code == 409
        assert r.json()["parts"] == 2
        assert r.json()["cost"] == 261.12
        assert gateway.sends == 0
        assert store.releases == 1

    def test_an_unreadable_balance_refuses(self, client, store, gateway):
        gateway.balance_status = 503

        r = _send(client)

        assert r.status_code == 409
        assert r.json()["reason"] == "balance_unreadable"
        assert r.json()["balance"] is None
        assert r.json()["cost"] == 130.56
        assert gateway.sends == 0
        assert (store.releases, store.claimed) == (1, False)

    def test_a_balance_with_no_figure_refuses(self, client, store, gateway):
        gateway.balance = None

        r = _send(client)

        assert r.status_code == 409
        assert r.json()["reason"] == "balance_unreadable"
        assert gateway.sends == 0
        assert store.releases == 1

    def test_viber_is_priced_as_sms_for_everyone(self, client, store, gateway):
        """The hybrid falls back to SMS for whoever Viber misses, which is not
        known until after — so the worst case is the one that must be covered."""
        gateway.balance = 130.0

        r = _send(client, channel="viber_sms")

        assert r.status_code == 409
        assert r.json()["cost"] == 130.56
        assert gateway.sends == 0

    def test_a_refused_send_can_go_once_the_balance_is_topped_up(
        self, client, store, gateway,
    ):
        gateway.balance = 10.0
        assert _send(client).status_code == 409

        gateway.balance = 1_000.0
        r = _send(client)

        assert r.status_code == 200, r.text
        assert gateway.sends == 1


# ─── B. Another campaign's people ───────────────────────────────────────────

class TestRecentCampaigns:

    def test_an_overlap_refuses_before_the_gateway_is_asked(
        self, client, store, gateway,
    ):
        store.conflicts = [
            {"campaign": "oct-vip", "sentAt": "2026-10-17T09:00:00+03:00",
             "arm": "control", "people": 3},
            {"campaign": "oct-all", "sentAt": "2026-10-17T08:00:00+03:00",
             "arm": "target", "people": 120},
        ]

        r = _send(client)

        assert r.status_code == 409
        body = r.json()
        assert body["reason"] == "recent_campaign"
        assert body["conflicts"] == store.conflicts
        assert body["hours"] == 24, "the page says how far back it looked"
        assert "oct-vip" in body["detail"] and "control" in body["detail"]
        assert "120" in body["detail"] and "follow_up_of" in body["detail"]
        assert gateway.calls == [], "not even the balance is asked"
        assert (store.releases, store.claimed) == (1, False)

    def test_a_guard_that_fails_hands_the_claim_back(self, store, gateway):
        store.conflict_error = RuntimeError("database went away")
        client = TestClient(app, raise_server_exceptions=False)

        r = _send(client)

        assert r.status_code == 500
        assert gateway.calls == []
        assert (store.releases, store.claimed) == (1, False), \
            "a campaign nothing was sent for must stay sendable"


# ─── The balance on the page ────────────────────────────────────────────────

class TestTheChannelsBalance:

    def test_asked_for_it_reads_the_gateway(self, client, gateway):
        gateway.balance = 886.0

        r = client.get(CHANNELS, params={"include_balance": "true"}, headers=_headers())

        assert r.status_code == 200
        assert r.json()["balance"] == 886.0
        assert r.json()["pricePerPart"] == 1.28

    def test_not_asked_it_leaves_the_gateway_alone(self, client, gateway):
        r = client.get(CHANNELS, headers=_headers())

        assert r.json()["balance"] is None
        assert gateway.calls == []

    def test_an_unreadable_balance_is_null_not_an_error(self, client, gateway):
        gateway.balance_status = 503

        r = client.get(CHANNELS, params={"include_balance": "true"}, headers=_headers())

        assert r.status_code == 200
        assert r.json()["balance"] is None


# ─── D. The follow-up, over a real store ────────────────────────────────────

@pytest_asyncio.fixture
async def real_store(tmp_path, monkeypatch):
    """The repository on Postgres, its DuckDB connection a trap, handed to the
    routes as `get_store()` answers it."""
    if not pg_sms_seed.DSN:
        pytest.skip("needs a live PostgreSQL at KS_PG_DSN")
    async with pg_sms_seed.on_postgres(monkeypatch):
        store = pg_sms_seed.store(tmp_path, "follow-up.duckdb")

        async def _get_store():
            return store

        monkeypatch.setattr("web.routes.api.customers.get_store", _get_store)
        yield store


@pytest_asyncio.fixture
async def aclient():
    """The app in this test's own event loop, so the pool the store holds is
    used from the loop that made it. `TestClient` runs the app on a loop of
    its own, which asyncpg refuses."""
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                 base_url="http://testserver") as ac:
        yield ac


def _member(buyer_id: int, assignment: str = "target") -> dict:
    return {"buyerId": buyer_id, "phone": f"38050{buyer_id:07d}", "tier": "ALL",
            "assignment": assignment, "orders": 2, "revenueLtv": 1000.0,
            "marginLtv": 500.0, "recencyDays": 30}


async def _sent_root(store, refused=(5,)):
    """`root`: four targets and a control, sent two hours ago; buyer 5's
    message was refused by the gateway, and buyer 2 opted out since."""
    await store.freeze_sms_campaign(
        campaign="root",
        customers=[_member(1), _member(2), _member(3), _member(4, "holdout"),
                   _member(5)],
        criteria={"grouping": "single", "holdoutPct": 10},
        ltv_basis="margin", sales_type="retail", holdout_pct=10,
    )
    targets = await store.get_sms_campaign_targets("root")
    await store.record_sms_send(
        "root",
        {t["buyerId"]: f"m-{t['buyerId']}" for t in targets
         if t["buyerId"] not in refused},
        [], {b: "Rejected" for b in refused},
        sent_at=datetime.now() - timedelta(hours=2),
    )
    await store.add_marketing_optout(buyer_id=2)


class TestFollowUpRoutes:

    @pytest.mark.asyncio
    async def test_preview_and_freeze_are_the_same_roster(self, aclient, real_store):
        await _sent_root(real_store)

        preview = await aclient.get(
            SEGMENTS, params={"follow_up_of": "root", "include_customers": "true"},
            headers=_headers(),
        )
        created = await aclient.post(
            CREATE, params={"campaign": "root-last-day", "follow_up_of": "root"},
            headers=_headers(),
        )

        assert preview.status_code == 200, preview.text
        assert created.status_code == 200, created.text
        shown = sorted(c["buyerId"] for c in preview.json()["customers"])
        assert shown == [1, 3], "accepted targets, less the opt-out; no control"
        assert created.json()["totals"] == {"customers": 2, "target": 2, "holdout": 0}
        assert created.json()["criteria"]["followUpOf"] == "root"

        listed = {c["campaign"]: c for c in await real_store.list_sms_campaigns()}
        frozen = listed["root-last-day"]
        assert (frozen["target"], frozen["holdout"], frozen["holdoutPct"]) == (2, 0, 0)
        assert frozen["followUpOf"] == "root"
        assert frozen["ltvBasis"] == "margin", "the root's basis, not the query default"
        members = await real_store.get_sms_campaign_targets("root-last-day")
        assert sorted(m["buyerId"] for m in members) == [1, 3]
        assert 4 not in {m["buyerId"] for m in members}, "the root's control is never in it"

    @pytest.mark.parametrize("extra", [
        {"brand": "Anua"}, {"grouping": "single"}, {"tier": "VIP"},
        {"max_recency_days": 365}, {"ltv_basis": "revenue"}, {"orders_min": 2},
    ])
    @pytest.mark.asyncio
    async def test_audience_parameters_beside_it_are_refused(
        self, aclient, real_store, extra,
    ):
        await _sent_root(real_store)

        r = await aclient.post(
            CREATE, params={"campaign": "root-last-day", "follow_up_of": "root", **extra},
            headers=_headers(),
        )

        assert r.status_code == 400
        assert next(iter(extra)) in r.json()["detail"]
        assert r.json()["reason"] == "follow_up_narrowed"
        assert r.json()["root"] == "root"
        assert r.json()["params"] == [next(iter(extra))]
        assert "root-last-day" not in {
            c["campaign"] for c in await real_store.list_sms_campaigns()}

    @pytest.mark.asyncio
    async def test_a_holdout_of_its_own_is_refused(self, aclient, real_store):
        await _sent_root(real_store)

        r = await aclient.post(
            CREATE, params={"campaign": "f", "follow_up_of": "root", "holdout_pct": 10},
            headers=_headers(),
        )

        assert r.status_code == 400
        assert "holdout" in r.json()["detail"]
        assert r.json()["reason"] == "follow_up_holdout"

    @pytest.mark.asyncio
    async def test_a_holdout_of_zero_is_what_it_is_anyway(self, aclient, real_store):
        await _sent_root(real_store)

        r = await aclient.post(
            CREATE, params={"campaign": "f", "follow_up_of": "root", "holdout_pct": 0},
            headers=_headers(),
        )

        assert r.status_code == 200, r.text

    @pytest.mark.asyncio
    async def test_an_unsent_root_is_refused(self, aclient, real_store):
        await real_store.freeze_sms_campaign(
            campaign="root", customers=[_member(1)], criteria={},
            ltv_basis="margin", sales_type="retail", holdout_pct=10,
        )

        r = await aclient.post(CREATE, params={"campaign": "f", "follow_up_of": "root"},
                        headers=_headers())

        assert r.status_code == 409
        assert "has not been sent" in r.json()["detail"]
        assert r.json()["reason"] == "follow_up_root_unsent"
        assert r.json()["root"] == "root"

    @pytest.mark.asyncio
    async def test_a_follow_up_of_a_follow_up_is_refused(self, aclient, real_store):
        await _sent_root(real_store)
        assert (await aclient.post(
            CREATE, params={"campaign": "noon", "follow_up_of": "root"},
            headers=_headers(),
        )).status_code == 200
        targets = await real_store.get_sms_campaign_targets("noon")
        await real_store.record_sms_send(
            "noon", {t["buyerId"]: f"n-{t['buyerId']}" for t in targets}, [], {},
        )

        r = await aclient.post(CREATE, params={"campaign": "evening", "follow_up_of": "noon"},
                        headers=_headers())

        assert r.status_code == 409
        assert "'root'" in r.json()["detail"], "the refusal names the root to use"
        assert r.json()["reason"] == "follow_up_of_follow_up"
        assert (r.json()["root"], r.json()["parent"]) == ("noon", "root")

    @pytest.mark.asyncio
    async def test_an_unknown_root_is_404(self, aclient, real_store):
        r = await aclient.get(SEGMENTS, params={"follow_up_of": "nobody"}, headers=_headers())

        assert r.status_code == 404
        assert r.json()["reason"] == "not_frozen"
        assert r.json()["campaign"] == "nobody"

    @pytest.mark.asyncio
    async def test_another_sales_type_must_be_named(self, aclient, real_store):
        await real_store.freeze_sms_campaign(
            campaign="root", customers=[_member(1)], criteria={},
            ltv_basis="margin", sales_type="all", holdout_pct=10,
        )
        targets = await real_store.get_sms_campaign_targets("root")
        await real_store.record_sms_send(
            "root", {t["buyerId"]: "m" for t in targets}, [], {})

        refused = await aclient.get(SEGMENTS, params={"follow_up_of": "root"},
                             headers=_headers())
        named = await aclient.get(SEGMENTS, params={"follow_up_of": "root", "sales_type": "all"},
                           headers=_headers())

        assert refused.status_code == 409
        assert "sales_type=all" in refused.json()["detail"]
        assert refused.json()["reason"] == "follow_up_sales_type"
        assert refused.json()["salesType"] == "all"
        assert named.status_code == 200, named.text

    @pytest.mark.asyncio
    async def test_it_is_not_exported(self, aclient, real_store):
        await _sent_root(real_store)

        r = await aclient.get(CSV, params={"follow_up_of": "root", "campaign": "f"},
                       headers=_headers())

        assert r.status_code == 400

    @pytest.mark.asyncio
    async def test_a_campaign_cannot_follow_itself(self, aclient, real_store):
        r = await aclient.post(CREATE, params={"campaign": "root", "follow_up_of": "root"},
                        headers=_headers())

        assert r.status_code == 400
        assert r.json()["reason"] == "follow_up_self"


# ─── E. Every refusal is named, over a real store ───────────────────────────
#
# The page says a refusal in its reader's language from `reason` and the
# figures beside it (`core/sms_refusals.py`); `detail` stays the English
# sentence for API callers. These are the refusals that only a real store
# can produce: the state of a campaign, not a guard of the route.

class TestRefusalsAreNamed:

    @pytest.mark.asyncio
    async def test_a_second_send_is_already_sent(self, aclient, real_store, gateway):
        await _sent_root(real_store)

        r = await aclient.post("/api/customers/sms-campaigns/root/send",
                        params={"text": "hi", "confirm_campaign": "root"},
                        headers=_headers())

        assert r.status_code == 409
        assert "already sent" in r.json()["detail"]
        assert r.json()["reason"] == "already_sent"
        assert r.json()["campaign"] == "root"
        assert r.json()["sentAt"], "the page says when it went"
        assert gateway.sends == 0

    @pytest.mark.asyncio
    async def test_an_unknown_campaign_is_not_frozen(self, aclient, real_store, gateway):
        r = await aclient.post("/api/customers/sms-campaigns/nobody/send",
                        params={"text": "hi", "confirm_campaign": "nobody"},
                        headers=_headers())

        assert r.status_code == 404
        assert r.json()["reason"] == "not_frozen"
        assert r.json()["campaign"] == "nobody"

    @pytest.mark.asyncio
    async def test_the_placeholder_name_is_refused(self, aclient, real_store):
        r = await aclient.post(CREATE, params={"campaign": "default", "grouping": "single"},
                        headers=_headers())

        assert r.status_code == 400
        assert r.json()["reason"] == "name_placeholder"
        assert "default" in r.json()["detail"]

    @pytest.mark.asyncio
    async def test_a_taken_name_is_already_frozen(self, aclient, real_store):
        await _sent_root(real_store)
        await real_store.freeze_sms_campaign(
            campaign="taken", customers=[_member(1)], criteria={},
            ltv_basis="margin", sales_type="retail", holdout_pct=10,
        )

        r = await aclient.post(CREATE, params={"campaign": "taken", "follow_up_of": "root"},
                        headers=_headers())

        assert r.status_code == 409
        assert r.json()["reason"] == "already_frozen"
        assert r.json()["campaign"] == "taken"

    @pytest.mark.asyncio
    async def test_an_empty_audience_is_refused(self, aclient, real_store):
        r = await aclient.post(CREATE, params={"campaign": "nobody-here", "grouping": "single"},
                        headers=_headers())

        assert r.status_code == 400
        assert r.json()["reason"] == "audience_empty"

    def test_a_gateway_refusal_is_named_and_hands_the_claim_back(
        self, client, store, gateway, monkeypatch,
    ):
        from core.turbosms import TurboSmsError

        class _Refusing:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            async def balance(self):
                return 1_000_000.0

            async def send(self, *a, **kw):
                raise TurboSmsError("sender name is not approved", unsent=True)

        monkeypatch.setattr("web.routes.api.customers.TurboSmsClient", lambda *a, **k: _Refusing())

        r = _send(client)

        assert r.status_code == 502
        assert r.json()["reason"] == "gateway_refused"
        assert "sender name is not approved" in r.json()["error"]
        assert store.releases == 1


# ─── F. Every guard, end to end, over the store production runs ─────────────
#
# The guards above are proved at the route against a store that claims the
# way the real one does. These prove each of them again with nothing stood
# in for: the route, the real repository and its claim — the conditional
# UPDATE on `app.sms_campaigns` — on Postgres, `/sms`'s only store since
# step 14's S14-08. What a refusal must leave behind is read back out of
# Postgres: a campaign nothing was sent for, still sendable. The gateway is
# the mock transport throughout; nothing here can reach TurboSMS.

async def _claim_state(campaign: str):
    """`sent_at` of the campaign as Postgres holds it: NULL is unclaimed."""
    async with pg_sms_seed.pg_conn() as conn:
        return await conn.fetchval(
            "SELECT sent_at FROM app.sms_campaigns WHERE campaign = $1", campaign)


async def _frozen(store, campaign: str, members: list) -> None:
    await store.freeze_sms_campaign(
        campaign=campaign, customers=members, criteria={"grouping": "single"},
        ltv_basis="margin", sales_type="retail", holdout_pct=0,
    )


def _send_to(aclient, campaign: str, confirm: "str | None" = None, text="Знижка 20%"):
    params = {"text": text}
    if confirm is not None:
        params["confirm_campaign"] = confirm
    return aclient.post(f"/api/customers/sms-campaigns/{campaign}/send",
                        params=params, headers=_headers())


class TestTheGuardsOverAPostgresStore:

    @pytest.mark.asyncio
    async def test_a_confirmed_send_goes_once_and_is_recorded(
        self, aclient, real_store, gateway,
    ):
        await _frozen(real_store, "oct", [_member(1), _member(2)])

        r = await _send_to(aclient, "oct", confirm="oct")

        assert r.status_code == 200, r.text
        assert gateway.sends == 1
        assert await _claim_state("oct") is not None, "the send stamps the claim"
        listed = {c["campaign"]: c for c in await real_store.list_sms_campaigns()}
        assert listed["oct"]["recipientsSent"] == 2

    @pytest.mark.asyncio
    async def test_the_typed_name_refuses_before_the_claim(
        self, aclient, real_store, gateway,
    ):
        await _frozen(real_store, "oct", [_member(1)])

        missing = await _send_to(aclient, "oct")
        wrong = await _send_to(aclient, "oct", confirm="ocT")

        assert (missing.status_code, missing.json()["reason"]) == (409, "confirm_mismatch")
        assert (wrong.status_code, wrong.json()["reason"]) == (409, "confirm_mismatch")
        assert gateway.calls == []
        assert await _claim_state("oct") is None, "nothing may be claimed"

    @pytest.mark.asyncio
    async def test_the_claim_lets_one_of_two_sends_through(
        self, aclient, real_store, gateway,
    ):
        """The 5 550 sent twice: a second press while the first was running.
        Two at once over the pool, and the conditional UPDATE lets one by."""
        import asyncio

        await _frozen(real_store, "oct", [_member(1), _member(2)])

        first, second = await asyncio.gather(
            _send_to(aclient, "oct", confirm="oct"),
            _send_to(aclient, "oct", confirm="oct"),
        )

        assert sorted([first.status_code, second.status_code]) == [200, 409]
        loser = first if first.status_code == 409 else second
        assert loser.json()["reason"] == "already_sent"
        assert gateway.sends == 1, "the roster went to the gateway once"

    @pytest.mark.asyncio
    async def test_a_recent_overlap_refuses_and_hands_the_claim_back(
        self, aclient, real_store, gateway,
    ):
        await _frozen(real_store, "first", [_member(1), _member(2)])
        targets = await real_store.get_sms_campaign_targets("first")
        await real_store.record_sms_send(
            "first", {t["buyerId"]: f"m-{t['buyerId']}" for t in targets}, [], {},
            sent_at=datetime.now() - timedelta(hours=1),
        )
        await _frozen(real_store, "second", [_member(2), _member(9)])

        r = await _send_to(aclient, "second", confirm="second")

        assert r.status_code == 409
        assert r.json()["reason"] == "recent_campaign"
        assert [(c["campaign"], c["arm"], c["people"]) for c in r.json()["conflicts"]] \
            == [("first", "target", 1)]
        assert gateway.calls == [], "not even the balance is asked"
        assert await _claim_state("second") is None, "handed back, still sendable"

    @pytest.mark.asyncio
    async def test_a_short_balance_refuses_and_hands_the_claim_back(
        self, aclient, real_store, gateway,
    ):
        await _frozen(real_store, "oct", [_member(i) for i in range(1, 11)])
        gateway.balance = 5.0      # ten recipients × 1.28 ₴ × 1.02 is 13.06 ₴

        r = await _send_to(aclient, "oct", confirm="oct")

        assert r.status_code == 409
        assert r.json()["reason"] == "balance_short"
        assert gateway.sends == 0
        assert await _claim_state("oct") is None

        gateway.balance = 1_000_000.0
        again = await _send_to(aclient, "oct", confirm="oct")
        assert again.status_code == 200, again.text
        assert gateway.sends == 1

    @pytest.mark.asyncio
    async def test_an_unreadable_balance_refuses_and_hands_the_claim_back(
        self, aclient, real_store, gateway,
    ):
        await _frozen(real_store, "oct", [_member(1)])
        gateway.balance_status = 503

        r = await _send_to(aclient, "oct", confirm="oct")

        assert r.status_code == 409
        assert r.json()["reason"] == "balance_unreadable"
        assert gateway.sends == 0
        assert await _claim_state("oct") is None

    @pytest.mark.asyncio
    async def test_a_guard_that_raises_hands_the_claim_back(
        self, real_store, gateway, monkeypatch,
    ):
        await _frozen(real_store, "oct", [_member(1)])

        async def broken(campaign):
            raise RuntimeError("database went away")

        monkeypatch.setattr(real_store, "find_sms_send_conflicts", broken)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
            base_url="http://testserver",
        ) as ac:
            r = await _send_to(ac, "oct", confirm="oct")

        assert r.status_code == 500
        assert gateway.calls == []
        assert await _claim_state("oct") is None, \
            "a campaign nothing was sent for must stay sendable"
