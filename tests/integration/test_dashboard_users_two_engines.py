"""The dashboard's user list answers in Postgres what it answered in DuckDB.

`tests/unit/test_dashboard_users_store.py` proves the ten statements are one
text and that nothing goes round the router. Necessary and not sufficient:
identical SQL still means different things where the engines differ — what
`RETURNING` gives back on a no-op update, how an `ON CONFLICT` treats a NULL,
where a tie lands under `LIMIT`, and what a timestamp looks like on the way out.

So this ran the real thing on both: the same people in a DuckDB store and a
live PostgreSQL, every method called on each, the **whole result** compared.

ONE ENGINE SINCE STEP 14 (S14-08)

The list is Postgres's alone. Every call is compared with a literal
(`tests/integration/golden/dashboard_users.json`): DuckDB's answer on this
fixture, recorded while this file still asserted Postgres answered the same
(rule 9 of the step-14 plan). The copy of the list out of DuckDB and its
daily comparison stand down under the constant (S14-00), so their tests went
with DuckDB's side.

WHY THE WRITES MATTER MORE THAN THE READS HERE

A read that disagrees shows a wrong name on an admin page. A write that
disagrees changes who can open the dashboard, and the two directions are not
equally visible: locking somebody out is loud, and leaving a revoked account
approved is not. So each write is held to the *resulting row*, not just its
return value.

Skipped without `KS_PG_DSN`; `deploy/gate_with_stores.sh` supplies one.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio

from core.pg_dashboard_users import USER_COLUMNS
from tests.integration import pg_sms_seed

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(
    not DSN, reason="needs a live PostgreSQL at KS_PG_DSN",
)

GOLDEN = pg_sms_seed.golden("dashboard_users")

NOW = datetime(2026, 9, 7, 9, 0, tzinfo=timezone.utc)


def _t(days_ago: int) -> datetime:
    return NOW - timedelta(days=days_ago)


# (user_id, username, first, last, photo, role, status, requested, reviewed,
#  reviewed_by, last_activity, denial_count, created, allowed_features)
#
# Deliberately awkward: a NULL username, a NULL photo, every status, every
# role, a denial count one below the freeze, and two rows sharing a
# `requested_at` so the list's ORDER BY has a tie to break.
#
# `allowed_features` carries all three of its states, because revision 0021
# makes NULL and "" mean different things and a fixture of NULLs would prove
# only that the column exists: 11 inherits (NULL), 12 is narrowed to two tabs,
# 13 is deliberately given none ("").
USERS = [
    (11, "alice",  "Alice", "A",  "http://p/1", "admin",    "approved", _t(30), _t(29), 1, _t(1), 0, _t(30), None),
    (12, "bob",    "Bob",   None, None,         "viewer",   "approved", _t(20), _t(19), 1, _t(2), 0, _t(20), "dashboard,products"),
    (13, None,     "Carol", None, None,         "marketer", "approved", _t(20), _t(18), 1, None,  0, _t(20), ""),
    (14, "dan",    "Dan",   "D",  None,         "viewer",   "pending",  _t(5),  None,   None, None, 0, _t(5), None),
    (15, "erin",   "Erin",  None, None,         "viewer",   "denied",   _t(9),  _t(8),  1, None,  4, _t(9), None),
    (16, "frank",  "Frank", None, None,         "editor",   "frozen",   _t(40), _t(35), 1, None,  5, _t(40), None),
]


@pytest_asyncio.fixture
async def store(tmp_path, monkeypatch):
    """The repository on Postgres, holding the list; its DuckDB connection
    is a trap."""
    async with pg_sms_seed.on_postgres(monkeypatch, pg_sms_seed.ACCESS_TABLES,
                                       what="the access path"):
        async with pg_sms_seed.pg_conn() as conn:
            await conn.executemany(
                f"INSERT INTO app.dashboard_users ({', '.join(USER_COLUMNS)}) "
                f"VALUES ({', '.join(f'${i}' for i in range(1, len(USER_COLUMNS) + 1))})",
                USERS,
            )
        yield pg_sms_seed.store(tmp_path, "users.duckdb")


def _plain(value):
    """JSON's shape, which the literals were recorded in: tuples as lists."""
    if isinstance(value, dict):
        return {str(k): _plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(v) for v in value]
    return value


READS = (
    ("get_user", {"user_id": 11}),
    ("get_user", {"user_id": 13}),          # NULL username, NULL last_activity
    ("get_user", {"user_id": 999}),         # absent
    ("get_user_by_status", {"status": "approved"}),
    ("get_user_by_status", {"status": "frozen"}),
    ("list_users", {}),
    ("list_users", {"status": "approved"}),
    ("list_users", {"role": "viewer"}),
    ("list_users", {"limit": 2, "offset": 0}),
    ("list_users", {"limit": 2, "offset": 2}),
    ("is_user_authorized", {"user_id": 12}),
    ("is_user_authorized", {"user_id": 15}),
    ("is_user_authorized", {"user_id": 999}),
    ("get_pending_users", {}),
    ("get_approved_users", {}),
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "name,kwargs", READS,
    ids=[f"{n}{tuple(k.values()) if k else ''}" for n, k in READS],
)
async def test_the_reads_agree(store, name, kwargs):
    answer = await getattr(store, name)(**kwargs)
    assert _plain(answer) == GOLDEN[f"read:{name}{json.dumps(kwargs, sort_keys=True)}"]


@pytest.mark.asyncio
async def test_the_fixture_reaches_the_awkward_rows(store):
    """A comparison of two empty answers agrees about nothing."""
    approved = await store.get_user_by_status("approved")
    assert len(approved) == 3
    assert any(u["username"] is None for u in approved), "the NULL username is not reached"
    assert {u["role"] for u in approved} == {"admin", "viewer", "marketer"}

    page_one = await store.list_users(limit=2, offset=0)
    page_two = await store.list_users(limit=2, offset=2)
    assert len({u["user_id"] for u in page_one} & {u["user_id"] for u in page_two}) == 0, (
        "the pages overlap, so the ORDER BY is not deciding the same thing twice"
    )


@pytest.mark.asyncio
async def test_the_timestamp_leaves_as_the_same_string(store):
    """DuckDB rendered a TIMESTAMPTZ in the session timezone and asyncpg in
    UTC. Same instant, two strings — the defect the `/inventory` port hit with
    `lastSync`, and it would land here on every date the admin page shows."""
    postgres = await store.get_user(11)
    recorded = GOLDEN['read:get_user{"user_id": 11}']["requested_at"]
    assert postgres["requested_at"] == recorded
    assert postgres["requested_at"].endswith("+00:00"), postgres["requested_at"]


# ─── the writes, held to what they leave behind ─────────────────────────────


# The clocks a write sets itself. Each engine stamped its own `now()` at the
# moment its copy of the call ran, so the values were never compared — only
# that they are *set at all*, which is where the real defect showed up:
# Postgres left them NULL until the migration learned DuckDB's defaults.
SELF_SET_CLOCKS = ("reviewed_at", "last_activity", "created_at", "requested_at")


def _comparable(row):
    """Everything but the clocks the write sets itself."""
    if row is None:
        return None
    if not isinstance(row, dict):
        return row
    return {k: v for k, v in row.items() if k not in SELF_SET_CLOCKS}


@pytest.mark.asyncio
@pytest.mark.parametrize("method,kwargs,user_id", (
    ("update_user_status", {"status": "approved", "reviewed_by": 11}, 14),
    ("update_user_status", {"status": "denied", "reviewed_by": 11}, 12),
    ("update_user_role", {"role": "editor", "changed_by": 11}, 12),
    ("update_user_activity", {}, 12),
    ("deny_user", {"admin_id": 11}, 12),
    ("deny_user", {"admin_id": 11}, 15),      # 4 denials → this one freezes
    ("deny_user", {"admin_id": 11}, 999),     # absent
    ("create_user", {"username": "new", "first_name": "New"}, 77),
    ("create_user", {"username": None, "first_name": None}, 11),   # COALESCE keeps
))
async def test_the_writes_leave_the_same_row(store, method, kwargs, user_id):
    call = dict(kwargs, user_id=user_id)
    recorded = GOLDEN[f"write:{method}{json.dumps(call, sort_keys=True)}"]

    returned = await getattr(store, method)(**call)
    row = await store.get_user(user_id)

    assert _plain(_comparable(returned)) == recorded["returned"]
    assert _plain(_comparable(row)) == recorded["row"]

    # Whatever the write touched, it must not have left a clock empty where
    # DuckDB filled it, or the other way round — that asymmetry is the shape
    # of the defect this file found.
    if recorded["clocks_set"] is None:
        assert row is None
    else:
        assert {c: row.get(c) is not None for c in SELF_SET_CLOCKS} == \
            recorded["clocks_set"]


@pytest.mark.asyncio
async def test_a_created_user_gets_its_dates(store):
    """The defect this file was written to catch.

    DuckDB defaults `requested_at` and `created_at`; the new Postgres table did
    not, and `create_user` passes neither. A person who first signed in while
    the switch was on carried no request date at all — a blank on the admin
    page, and a NULL in `COALESCE(reviewed_at, created_at)`, which is the clock
    the daily comparison forgives a row in flight by.

    The value itself is `now()` and is not compared; what must hold is that
    it is there.
    """
    row = await store.create_user(user_id=82, username="u82")
    assert row["requested_at"], "postgres left requested_at empty"
    assert row["created_at"], "postgres left created_at empty"
    assert row["status"] == "pending" and row["role"] == "viewer"


@pytest.mark.asyncio
async def test_the_fifth_refusal_freezes(store):
    """User 15 arrives on four refusals. The fifth is the freeze, and the
    count comes back from the database rather than being computed here —
    which is what stops two admins refusing at once from losing one."""
    ok, frozen = await store.deny_user(15, admin_id=11)
    assert (ok, frozen) == (True, True)
    assert (await store.get_user(15))["status"] == "frozen"


@pytest.mark.asyncio
async def test_approving_clears_the_denial_count(store):
    """Otherwise a re-approved person stands one refusal from a thirty-day
    freeze, which is a trap nobody would connect to their own approval."""
    await store.update_user_status(16, "approved", reviewed_by=11)
    row = await store.get_user(16)
    assert row["status"] == "approved"
    assert row["denial_count"] == 0
