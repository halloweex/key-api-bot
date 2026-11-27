"""The SMS audience routes over the test Postgres, for the route tests.

`audience_on_postgres(monkeypatch)` empties the `/sms` tables, seeds
`tests/sms_audience_fixture.py` and an inferred gender for some of its buyers
(`GENDERS`), and leaves `core.pg.get_pool` on that database. Nest
`tests.helpers.app_client.signed_in(...)` inside it to call the routes as a
visitor; the tables are emptied again on the way out.

The fixture's audience at the defaults (one arm, the 270-day window, retail)
is `BASE`: everybody whose last counted order is inside 270 days, less 11 (an
unusable phone), 10 (opted out) and 8 (shares a phone with 9, who is worth
more). Every expected set in the route tests is written by hand against it.
"""
from __future__ import annotations

from contextlib import asynccontextmanager

from tests import sms_audience_fixture as fx
from tests.integration import pg_sms_seed

SEGMENTS = "/api/customers/sms-segments"
EXPORT = "/api/customers/sms-segments/export/csv"
CAMPAIGNS = "/api/customers/sms-campaigns"
PRESETS = "/api/customers/sms-audience-presets"

BASE = {1, 2, 3, 4, 5, 6, 7, 9, 12, 13, 14}

# (buyer, gender, confidence). 6 is a refused verdict (a row whose gender is
# NULL); 7, 8 and 10-15 have no row at all.
GENDERS = [
    (1, "f", "certain"), (2, "f", "certain"), (3, "f", "medium"),
    (4, "m", "high"), (5, "m", "medium"), (9, "m", "high"), (6, None, None),
]


async def seed_genders(conn, rows=GENDERS) -> None:
    await conn.executemany(
        "INSERT INTO app.buyer_gender (buyer_id, gender, method, confidence,"
        " rules_version) VALUES ($1, $2, $3, $4, 1)",
        [(b, g, "test" if g else "refused", c) for b, g, c in rows])


@asynccontextmanager
async def audience_on_postgres(monkeypatch):
    """The fixture's buyers, orders and genders in Postgres, for one test."""
    async with pg_sms_seed.on_postgres(monkeypatch) as pool:
        async with pool.acquire() as conn:
            await fx.seed_postgres(conn)
            await seed_genders(conn)
        yield pool


def buyer_ids(response) -> set:
    """The buyers a preview answered with (`include_customers=true`)."""
    return {c["buyerId"] for c in response.json()["customers"]}


def arms(response) -> dict:
    """buyer → `target` / `holdout`, as a preview answered."""
    return {c["buyerId"]: c["assignment"] for c in response.json()["customers"]}


async def frozen_members(campaign: str) -> dict:
    """buyer → assignment, as `app.sms_campaign_members` holds the roster."""
    async with pg_sms_seed.pg_conn() as conn:
        rows = await conn.fetch(
            "SELECT buyer_id, assignment FROM app.sms_campaign_members"
            " WHERE campaign = $1", campaign)
    return {r["buyer_id"]: r["assignment"] for r in rows}


async def frozen_campaign(campaign: str):
    """The campaign's row in `app.sms_campaigns`, or None."""
    async with pg_sms_seed.pg_conn() as conn:
        return await conn.fetchrow(
            "SELECT campaign, holdout_pct, ltv_basis, sales_type, criteria, sent_at"
            " FROM app.sms_campaigns WHERE campaign = $1", campaign)
