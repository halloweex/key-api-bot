"""The dashboard's user list means the same thing in DuckDB and Postgres.

`tests/unit/test_dashboard_users_store.py` proves the ten statements are one
text and that nothing goes round the router. Necessary and not sufficient:
identical SQL still means different things where the engines differ — what
`RETURNING` gives back on a no-op update, how an `ON CONFLICT` treats a NULL,
where a tie lands under `LIMIT`, and what a timestamp looks like on the way out.

So this runs the real thing. The same twelve people go into a DuckDB store and
a live PostgreSQL, every method is called twice — once with `KS_USER_STORE`
unset and once set to `postgres` — and the **whole result** is compared.

WHY THE WRITES MATTER MORE THAN THE READS HERE

A read that disagrees shows a wrong name on an admin page. A write that
disagrees changes who can open the dashboard, and the two directions are not
equally visible: locking somebody out is loud, and leaving a revoked account
approved is not. So each write is run on both engines and the *resulting row*
is compared, not just its return value.

Skipped without `KS_PG_DSN`; `deploy/gate_with_stores.sh` supplies one.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, patch

from core.duckdb_store import DuckDBStore
from core.pg_dashboard_users import USER_COLUMNS

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(
    not DSN, reason="needs a live PostgreSQL at KS_PG_DSN",
)

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


async def _seed_duckdb(store):
    async with store.connection() as conn:
        conn.execute("DELETE FROM users")
        for row in USERS:
            conn.execute(
                f"INSERT INTO users ({', '.join(USER_COLUMNS)}) "
                f"VALUES ({', '.join(['?'] * len(USER_COLUMNS))})",
                list(row),
            )


async def _seed_postgres(conn):
    await conn.execute("DELETE FROM app.dashboard_users")
    await conn.executemany(
        f"INSERT INTO app.dashboard_users ({', '.join(USER_COLUMNS)}) "
        f"VALUES ({', '.join(f'${i}' for i in range(1, len(USER_COLUMNS) + 1))})",
        USERS,
    )


@pytest_asyncio.fixture
async def both_engines(tmp_path, monkeypatch):
    """One store each, holding the identical list.

    The pool is built here and `core.pg.get_pool` patched to hand it over —
    the module-level one is bound to whichever loop first asked for it, and
    closing it after pytest-asyncio has retired that loop raises. Same route
    as the inventory and SMS two-engine tests.
    """
    monkeypatch.setenv("KS_PG_DSN", DSN)
    store = DuckDBStore(db_path=tmp_path / "users.duckdb")
    await store.connect()
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    try:
        async with pool.acquire() as conn:
            await _seed_postgres(conn)
        await _seed_duckdb(store)
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)):
            yield store
    finally:
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM app.dashboard_users")
        await pool.close()
        await store.close()


async def _both(store, monkeypatch, name, kwargs):
    """The same call on each engine, DuckDB first."""
    monkeypatch.delenv("KS_USER_STORE", raising=False)
    duck = await getattr(store, name)(**kwargs)
    monkeypatch.setenv("KS_USER_STORE", "postgres")
    postgres = await getattr(store, name)(**kwargs)
    monkeypatch.delenv("KS_USER_STORE", raising=False)
    return duck, postgres


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
async def test_the_reads_agree(both_engines, monkeypatch, name, kwargs):
    duck, postgres = await _both(both_engines, monkeypatch, name, kwargs)
    assert postgres == duck


@pytest.mark.asyncio
async def test_the_fixture_reaches_the_awkward_rows(both_engines, monkeypatch):
    """A comparison of two empty answers agrees about nothing."""
    approved, _ = await _both(
        both_engines, monkeypatch, "get_user_by_status", {"status": "approved"})
    assert len(approved) == 3
    assert any(u["username"] is None for u in approved), "the NULL username is not reached"
    assert {u["role"] for u in approved} == {"admin", "viewer", "marketer"}

    page_one, _ = await _both(both_engines, monkeypatch, "list_users",
                              {"limit": 2, "offset": 0})
    page_two, _ = await _both(both_engines, monkeypatch, "list_users",
                              {"limit": 2, "offset": 2})
    assert len({u["user_id"] for u in page_one} & {u["user_id"] for u in page_two}) == 0, (
        "the pages overlap, so the ORDER BY is not deciding the same thing twice"
    )


@pytest.mark.asyncio
async def test_the_timestamp_leaves_as_the_same_string(both_engines, monkeypatch):
    """DuckDB renders a TIMESTAMPTZ in the session timezone and asyncpg in
    UTC. Same instant, two strings — the defect the `/inventory` port hit with
    `lastSync`, and it would land here on every date the admin page shows."""
    duck, postgres = await _both(both_engines, monkeypatch, "get_user", {"user_id": 11})
    assert duck["requested_at"] == postgres["requested_at"]
    assert duck["requested_at"].endswith("+00:00"), duck["requested_at"]


# ─── the writes, compared by what they leave behind ──────────────────────────


async def _row(store, monkeypatch, user_id):
    monkeypatch.delenv("KS_USER_STORE", raising=False)
    return await store.get_user(user_id)


async def _row_pg(store, monkeypatch, user_id):
    monkeypatch.setenv("KS_USER_STORE", "postgres")
    row = await store.get_user(user_id)
    monkeypatch.delenv("KS_USER_STORE", raising=False)
    return row


# The clocks a write sets itself. Each engine stamps its own `now()` at the
# moment its copy of the call runs — measured 13 ms apart on the gate — so
# comparing the values would be comparing two clocks, not two behaviours. That
# they are *set at all* is asserted by
# `test_a_created_user_gets_its_dates_on_both`, which is where the real defect
# showed up: Postgres left them NULL until the migration learned DuckDB's
# defaults.
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
async def test_the_writes_leave_the_same_row(
    both_engines, monkeypatch, method, kwargs, user_id,
):
    store = both_engines
    call = dict(kwargs, user_id=user_id)

    monkeypatch.delenv("KS_USER_STORE", raising=False)
    duck_returned = await getattr(store, method)(**call)
    duck_row = await _row(store, monkeypatch, user_id)

    monkeypatch.setenv("KS_USER_STORE", "postgres")
    pg_returned = await getattr(store, method)(**call)
    pg_row = await _row_pg(store, monkeypatch, user_id)

    assert _comparable(pg_returned) == _comparable(duck_returned)
    assert _comparable(pg_row) == _comparable(duck_row)

    # Whatever the write touched, it must not have left a clock empty on one
    # engine and filled on the other — that asymmetry is the shape of the
    # defect this file found.
    if duck_row is not None:
        for column in SELF_SET_CLOCKS:
            assert (duck_row.get(column) is None) == (pg_row.get(column) is None), (
                f"{column} is set on one engine and not the other"
            )


@pytest.mark.asyncio
async def test_a_created_user_gets_its_dates_on_both(both_engines, monkeypatch):
    """The defect this file was written to catch.

    DuckDB defaults `requested_at` and `created_at`; the new Postgres table did
    not, and `create_user` passes neither. A person who first signed in while
    the switch was on carried no request date at all — a blank on the admin
    page, and a NULL in `COALESCE(reviewed_at, created_at)`, which is the clock
    the daily comparison forgives a row in flight by.

    The values themselves are two different `now()`s and are not compared;
    what must match is that both are there.
    """
    store = both_engines
    for user_id, engine in ((81, "duckdb"), (82, "postgres")):
        if engine == "postgres":
            monkeypatch.setenv("KS_USER_STORE", "postgres")
        else:
            monkeypatch.delenv("KS_USER_STORE", raising=False)
        row = await store.create_user(user_id=user_id, username=f"u{user_id}")
        assert row["requested_at"], f"{engine} left requested_at empty"
        assert row["created_at"], f"{engine} left created_at empty"
        assert row["status"] == "pending" and row["role"] == "viewer"
    monkeypatch.delenv("KS_USER_STORE", raising=False)


@pytest.mark.asyncio
async def test_the_fifth_refusal_freezes_on_both(both_engines, monkeypatch):
    """User 15 arrives on four refusals. The fifth is the freeze, and the
    count now comes back from the database rather than being computed here —
    which is what stops two admins refusing at once from losing one."""
    store = both_engines
    monkeypatch.delenv("KS_USER_STORE", raising=False)
    ok, frozen = await store.deny_user(15, admin_id=11)
    assert (ok, frozen) == (True, True)
    assert (await store.get_user(15))["status"] == "frozen"

    monkeypatch.setenv("KS_USER_STORE", "postgres")
    ok, frozen = await store.deny_user(15, admin_id=11)
    assert (ok, frozen) == (True, True)
    assert (await store.get_user(15))["status"] == "frozen"
    monkeypatch.delenv("KS_USER_STORE", raising=False)


@pytest.mark.asyncio
async def test_approving_clears_the_denial_count_on_both(both_engines, monkeypatch):
    """Otherwise a re-approved person stands one refusal from a thirty-day
    freeze, which is a trap nobody would connect to their own approval."""
    store = both_engines
    for expected_store in ("duckdb", "postgres"):
        if expected_store == "postgres":
            monkeypatch.setenv("KS_USER_STORE", "postgres")
        else:
            monkeypatch.delenv("KS_USER_STORE", raising=False)
        await store.update_user_status(16, "approved", reviewed_by=11)
        row = await store.get_user(16)
        assert row["status"] == "approved"
        assert row["denial_count"] == 0, expected_store
    monkeypatch.delenv("KS_USER_STORE", raising=False)


@pytest.mark.asyncio
async def test_the_replication_carries_the_list_faithfully(both_engines, monkeypatch):
    """The copy that has to run before the switch can be flipped."""
    from core.pg_dashboard_users import replicate_dashboard_users

    store = both_engines
    monkeypatch.delenv("KS_USER_STORE", raising=False)
    async with store.connection() as conn:
        conn.execute("UPDATE users SET role = 'editor' WHERE user_id = 12")

    with patch("core.mirror_reconciliation.configured", return_value=True):
        out = await replicate_dashboard_users(store)
    assert out.get("rows") == len(USERS), out

    duck = await store.get_user(12)
    monkeypatch.setenv("KS_USER_STORE", "postgres")
    postgres = await store.get_user(12)
    monkeypatch.delenv("KS_USER_STORE", raising=False)
    assert postgres == duck


@pytest.mark.asyncio
async def test_the_comparison_is_clean_on_a_faithful_copy(both_engines, monkeypatch):
    from core.mirror_reconciliation import reconcile_dashboard_users
    from core.pg_dashboard_users import replicate_dashboard_users

    store = both_engines
    monkeypatch.delenv("KS_USER_STORE", raising=False)
    with patch("core.mirror_reconciliation.configured", return_value=True):
        await replicate_dashboard_users(store)
    with patch("core.pg_landing.enabled", return_value=True):
        assert await reconcile_dashboard_users(store) == []


@pytest.mark.asyncio
async def test_a_lost_approval_is_reported(both_engines, monkeypatch):
    """The finding that matters: somebody who can open the dashboard on one
    store and not on the other."""
    from core.mirror_reconciliation import reconcile_dashboard_users
    from core.pg_dashboard_users import replicate_dashboard_users

    store = both_engines
    monkeypatch.delenv("KS_USER_STORE", raising=False)
    with patch("core.mirror_reconciliation.configured", return_value=True):
        await replicate_dashboard_users(store)

    pool = await __import__("core.pg", fromlist=["pg"]).get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM app.dashboard_users WHERE user_id = 11")

    with patch("core.pg_landing.enabled", return_value=True):
        issues = await reconcile_dashboard_users(store)
    assert issues, "an approved admin vanished from one store and nothing said so"
    assert any("11" in str(i.sample_ids) for i in issues), issues
