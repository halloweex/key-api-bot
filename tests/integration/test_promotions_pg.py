"""The promotions calendar against a real PostgreSQL (revision 0039).

The suite's database is at the head. The revision's statements and Alembic's
own rendering of the downgrade and the upgrade run inside a transaction that
is always rolled back; the three tables are cleared before and after every
other test here, and `tests/integration/pg_tab_seed.clear` clears them for
every tab test.

Pinned, each named by the mutation it kills:

- the tables as the catalogue reports them, every CHECK refusing what the
  writer refuses, `ks_readonly` reading them and writing nothing, the
  revision applying twice and its downgrade keeping every row;
- the writer: versions in order, a save that changes nothing writing nothing,
  cancel and restore, concurrent saves each getting a version of their own
  and a save waiting under the lock building on the one before it, `as_of`;
- the API through the app: the contract's JSON, 400/404/409, the
  `marketing` tab, and the SMS link the `sms` tab's;
- a checkin written through the bot's adapter, once per window and person,
  and the Monday job's ledger;
- `planned_promotions` end to end: Bronze seeded, Silver and Gold derived,
  the forecast stored, promotions saved, `/api/revenue/forecast` answered.
"""
from __future__ import annotations

import asyncio
import os
import time as _time
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, Dict, List
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

import asyncpg
import pytest
import pytest_asyncio

from core import pg, promo_asks, promotions
from tests import migration_replay as replay
from tests.unit.test_goals_read_paths_do_not_write import _ConnectPerAcquire
from tests.unit.test_read_fallback_sites import (  # noqa: F401 — fixtures
    admin_client,
    fresh_read_fallback,
)

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

REV = replay.load(replay.VERSIONS / "0039_promotions.py")
BEFORE = "0038_order_discounts"
CURRENT = pg.REQUIRED_REVISION
TABLES = ("app.promotion_versions", "app.promotion_checkins", "app.promotion_asks")
KYIV = ZoneInfo("Europe/Kyiv")


def _payload(**over) -> Dict[str, Any]:
    body = {"name": "Весняний розпродаж", "starts_on": "2027-03-05",
            "ends_on": "2027-03-08", "start_time": "18:00", "kind": "other",
            "discount_pct": 20, "scope": "brands", "scope_items": ["COSRX"],
            "channels": ["sms", "instagram"]}
    body.update(over)
    return body


def _data(**over) -> promotions.PromotionIn:
    return promotions.parse(_payload(**over))


async def _clear(conn) -> None:
    for table in TABLES:
        await conn.execute(f"DELETE FROM {table}")


async def _stamp(conn) -> str:
    return await conn.fetchval("SELECT version_num FROM meta.alembic_version")


# ─── The revision on a server ────────────────────────────────────────────────

class TestTheRevisionOnAServer:
    @pytest.mark.asyncio
    async def test_the_tables_are_there_as_the_catalogue_reads_them(self):
        """Kills: a column renamed, a NOT NULL lost, a type moved."""
        conn = await asyncpg.connect(DSN)
        try:
            assert await _stamp(conn) == CURRENT
            columns = {(r["table_name"], r["column_name"]): (r["data_type"], r["is_nullable"])
                       for r in await conn.fetch(
                           "SELECT table_name, column_name, data_type, is_nullable "
                           "FROM information_schema.columns WHERE table_schema = 'app' "
                           "AND table_name LIKE 'promotion\\_%'")}
            v = "promotion_versions"
            assert {c: t for (table, c), t in columns.items() if table == v} == {
                "version_id": ("bigint", "NO"), "promotion_id": ("bigint", "NO"),
                "version": ("integer", "NO"),
                "recorded_at": ("timestamp with time zone", "NO"),
                "recorded_by": ("bigint", "YES"), "source": ("text", "NO"),
                "status": ("text", "NO"), "name": ("text", "NO"),
                "starts_on": ("date", "NO"), "ends_on": ("date", "NO"),
                "start_time": ("time without time zone", "YES"), "kind": ("text", "NO"),
                "discount_pct": ("numeric", "YES"), "scope": ("text", "NO"),
                "scope_items": ("ARRAY", "NO"), "channels": ("ARRAY", "NO"),
                "sms_campaign": ("text", "YES"), "note": ("text", "YES")}
            assert {c for (table, c) in columns if table == "promotion_checkins"} == {
                "id", "period_from", "period_to", "answer", "answered_at", "answered_by"}
            assert {c for (table, c) in columns if table == "promotion_asks"} == {
                "week_start", "sent_at", "delivered"}
            for table in TABLES:
                assert await conn.fetchval("SELECT obj_description($1::regclass)", table)
            assert await conn.fetchval("SELECT to_regclass('app.promotion_ids')") is not None
        finally:
            await conn.close()

    @pytest.mark.parametrize("statement", [
        # A status, kind, scope or source the writer does not know.
        "INSERT INTO app.promotion_versions (promotion_id, version, source, status, name, "
        "starts_on, ends_on, kind, scope) VALUES (1, 1, 'form', 'done', 'x', "
        "'2027-01-01', '2027-01-01', 'other', 'store')",
        "INSERT INTO app.promotion_versions (promotion_id, version, source, status, name, "
        "starts_on, ends_on, kind, scope) VALUES (1, 1, 'csv', 'planned', 'x', "
        "'2027-01-01', '2027-01-01', 'other', 'store')",
        # 63 days, the first and last included; ends before it starts.
        "INSERT INTO app.promotion_versions (promotion_id, version, source, status, name, "
        "starts_on, ends_on, kind, scope) VALUES (1, 1, 'form', 'planned', 'x', "
        "'2027-01-01', '2027-03-04', 'other', 'store')",
        "INSERT INTO app.promotion_versions (promotion_id, version, source, status, name, "
        "starts_on, ends_on, kind, scope) VALUES (1, 1, 'form', 'planned', 'x', "
        "'2027-01-02', '2027-01-01', 'other', 'store')",
        # A discount past 90, a name past 120, an empty name.
        "INSERT INTO app.promotion_versions (promotion_id, version, source, status, name, "
        "starts_on, ends_on, kind, scope, discount_pct) VALUES (1, 1, 'form', 'planned', "
        "'x', '2027-01-01', '2027-01-01', 'other', 'store', 90.01)",
        "INSERT INTO app.promotion_versions (promotion_id, version, source, status, name, "
        "starts_on, ends_on, kind, scope) VALUES (1, 1, 'form', 'planned', repeat('x', 121), "
        "'2027-01-01', '2027-01-01', 'other', 'store')",
        "INSERT INTO app.promotion_versions (promotion_id, version, source, status, name, "
        "starts_on, ends_on, kind, scope) VALUES (1, 1, 'form', 'planned', '', "
        "'2027-01-01', '2027-01-01', 'other', 'store')",
        # A channel it does not know; an item that is NULL; 51 items.
        "INSERT INTO app.promotion_versions (promotion_id, version, source, status, name, "
        "starts_on, ends_on, kind, scope, channels) VALUES (1, 1, 'form', 'planned', 'x', "
        "'2027-01-01', '2027-01-01', 'other', 'store', ARRAY['fax'])",
        "INSERT INTO app.promotion_versions (promotion_id, version, source, status, name, "
        "starts_on, ends_on, kind, scope, scope_items) VALUES (1, 1, 'form', 'planned', 'x', "
        "'2027-01-01', '2027-01-01', 'other', 'brands', ARRAY['a', NULL])",
        "INSERT INTO app.promotion_versions (promotion_id, version, source, status, name, "
        "starts_on, ends_on, kind, scope, scope_items) VALUES (1, 1, 'form', 'planned', 'x', "
        "'2027-01-01', '2027-01-01', 'other', 'brands', "
        "(SELECT array_agg(g::text) FROM generate_series(1, 51) g))",
        # A version 0; an answer that is not 'none'; a ledger week not a Monday.
        "INSERT INTO app.promotion_versions (promotion_id, version, source, status, name, "
        "starts_on, ends_on, kind, scope) VALUES (1, 0, 'form', 'planned', 'x', "
        "'2027-01-01', '2027-01-01', 'other', 'store')",
        "INSERT INTO app.promotion_checkins (period_from, period_to, answer) "
        "VALUES ('2026-11-23', '2026-12-06', 'yes')",
        "INSERT INTO app.promotion_asks (week_start, sent_at, delivered) "
        "VALUES ('2026-11-24', now(), 1)",
    ])
    @pytest.mark.asyncio
    async def test_every_check_refuses(self, statement):
        conn = await asyncpg.connect(DSN)
        tx = conn.transaction()
        await tx.start()
        try:
            with pytest.raises(asyncpg.CheckViolationError):
                await conn.execute(statement)
        finally:
            await tx.rollback()
            await conn.close()

    @pytest.mark.asyncio
    async def test_a_version_number_is_taken_once(self):
        """The second line behind the lock. Kills: the UNIQUE key dropped."""
        conn = await asyncpg.connect(DSN)
        tx = conn.transaction()
        await tx.start()
        try:
            insert = ("INSERT INTO app.promotion_versions (promotion_id, version, source, "
                      "status, name, starts_on, ends_on, kind, scope) VALUES (-5, 1, 'form', "
                      "'planned', 'x', '2027-01-01', '2027-01-01', 'other', 'store')")
            await conn.execute(insert)
            with pytest.raises(asyncpg.UniqueViolationError):
                await conn.execute(insert)
        finally:
            await tx.rollback()
            await conn.close()

    @pytest.mark.asyncio
    async def test_ks_readonly_reads_them_and_writes_nothing(self):
        """The default privileges in `postgres/initdb/30-app.sql`, not a GRANT
        in the revision; the sequence gives it nothing."""
        conn = await asyncpg.connect(DSN)
        try:
            for table in TABLES:
                assert await conn.fetchval(
                    "SELECT has_table_privilege('ks_readonly', $1, 'SELECT')", table)
                for right in ("INSERT", "UPDATE", "DELETE", "TRUNCATE"):
                    assert not await conn.fetchval(
                        "SELECT has_table_privilege('ks_readonly', $1, $2)", table, right)
            for right in ("USAGE", "SELECT", "UPDATE"):
                assert not await conn.fetchval(
                    "SELECT has_sequence_privilege('ks_readonly', 'app.promotion_ids', $1)",
                    right), right
        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_it_applies_twice(self):
        """Every statement again, then Alembic's step from 0038. Kills: an IF
        NOT EXISTS dropped, or a COMMENT that does not parse on a server."""
        from tests.integration.test_analytics_captures_pg import _alembic_sql

        conn = await asyncpg.connect(DSN)
        tx = conn.transaction()
        await tx.start()
        try:
            for statement in REV.STATEMENTS:
                await conn.execute(statement)
            await conn.execute("UPDATE meta.alembic_version SET version_num = $1", BEFORE)
            await conn.execute(_alembic_sql("upgrade", f"{BEFORE}:{CURRENT}"))
            assert await _stamp(conn) == CURRENT
        finally:
            await tx.rollback()
            await conn.close()

    @pytest.mark.asyncio
    async def test_the_downgrade_moves_the_stamp_and_keeps_every_row(self):
        """Down to 0038 the stamp is 0038, an image at 0038 passes its check,
        and every row is still there; up again, nothing moved. Kills: a DROP
        in the downgrade, or a re-upgrade that is not a no-op."""
        from tests.integration.test_analytics_captures_pg import _alembic_sql

        conn = await asyncpg.connect(DSN)
        tx = conn.transaction()
        await tx.start()
        try:
            await conn.execute(
                "INSERT INTO app.promotion_versions (promotion_id, version, source, status, "
                "name, starts_on, ends_on, kind, scope) VALUES (-7, 1, 'form', 'planned', "
                "'x', '2027-01-01', '2027-01-01', 'other', 'store')")
            await conn.execute("INSERT INTO app.promotion_checkins (period_from, period_to, "
                               "answer) VALUES ('2026-11-23', '2026-12-06', 'none')")
            before = {t: await conn.fetch(f"SELECT * FROM {t} ORDER BY 1") for t in TABLES}
            await conn.execute(_alembic_sql("downgrade", f"{CURRENT}:{BEFORE}"))
            assert await _stamp(conn) == BEFORE
            await pg.require_revision(BEFORE, conn=conn)
            assert {t: await conn.fetch(f"SELECT * FROM {t} ORDER BY 1") for t in TABLES} == before
            await conn.execute(_alembic_sql("upgrade", f"{BEFORE}:{CURRENT}"))
            assert await _stamp(conn) == CURRENT
            assert {t: await conn.fetch(f"SELECT * FROM {t} ORDER BY 1") for t in TABLES} == before
        finally:
            await tx.rollback()
            await conn.close()


# ─── The writer ──────────────────────────────────────────────────────────────

@pytest_asyncio.fixture
async def db():
    """A pool `core.pg.get_pool` answers with, the three tables empty before
    and after."""
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=10)
    try:
        async with pool.acquire() as conn:
            await _clear(conn)
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)):
            yield pool
    finally:
        try:
            async with pool.acquire() as conn:
                await _clear(conn)
        finally:
            await pool.close()


async def _rows(pool, pid) -> List[asyncpg.Record]:
    async with pool.acquire() as conn:
        return await conn.fetch("SELECT * FROM app.promotion_versions "
                                "WHERE promotion_id = $1 ORDER BY version", pid)


class TestTheWriter:
    @pytest.mark.asyncio
    async def test_every_save_is_a_version_and_nothing_else_moves(self, db):
        created = await promotions.create(_data(), by=111)
        edited = await promotions.update(created.id, _data(discount_pct=25), by=222)
        same = await promotions.update(created.id, _data(discount_pct=25.0,
                                                         source="form_from_sms"), by=333,
                                       sms_view=True)
        cancelled = await promotions.cancel(created.id, by=111)
        restored = await promotions.restore(created.id, by=222)

        rows = await _rows(db, created.id)
        assert [(r["version"], r["status"], r["recorded_by"]) for r in rows] == [
            (1, "planned", 111), (2, "planned", 222), (3, "cancelled", 111),
            (4, "planned", 222)]
        assert same == edited                      # nothing written for it
        assert rows[1]["discount_pct"] == Decimal("25.00")
        assert [r["recorded_at"] for r in rows] == sorted(r["recorded_at"] for r in rows)
        assert (cancelled.version, restored.version) == (3, 4)
        assert restored.created_at == created.recorded_at == rows[0]["recorded_at"]
        assert rows[0]["start_time"] == time(18, 0)
        assert rows[0]["scope_items"] == ["COSRX"]
        assert rows[0]["channels"] == ["sms", "instagram"]

    @pytest.mark.asyncio
    async def test_the_status_refuses_with_nothing_written(self, db):
        created = await promotions.create(_data(), by=1)
        with pytest.raises(promotions.PromotionConflict):
            await promotions.restore(created.id, by=1)
        await promotions.cancel(created.id, by=1)
        with pytest.raises(promotions.PromotionConflict):
            await promotions.cancel(created.id, by=1)
        with pytest.raises(promotions.PromotionConflict):
            await promotions.update(created.id, _data(name="Y"), by=1)
        with pytest.raises(promotions.PromotionNotFound):
            await promotions.update(created.id + 1000, _data(), by=1)
        assert len(await _rows(db, created.id)) == 2

    @pytest.mark.asyncio
    async def test_concurrent_saves_each_get_a_version_of_their_own(self, db):
        """Eight edits at once: versions 2..9, each once, none refused.
        Kills: the latest version read outside the lock — two saves would
        write the same N+1 and one would die on the UNIQUE key."""
        created = await promotions.create(_data(), by=1)
        saved = await asyncio.gather(*(
            promotions.update(created.id, _data(name=f"Назва {i}"), by=i)
            for i in range(8)))
        assert sorted(p.version for p in saved) == list(range(2, 10))
        rows = await _rows(db, created.id)
        assert [r["version"] for r in rows] == list(range(1, 10))
        assert sorted(r["name"] for r in rows[1:]) == sorted(f"Назва {i}" for i in range(8))
        stamps = [r["recorded_at"] for r in rows]
        assert stamps == sorted(stamps)

    @pytest.mark.asyncio
    async def test_concurrent_creates_get_ids_of_their_own(self, db):
        made = await asyncio.gather(*(promotions.create(_data(name=f"A{i}"), by=1)
                                      for i in range(5)))
        assert len({p.id for p in made}) == 5 and {p.version for p in made} == {1}

    @pytest.mark.asyncio
    async def test_a_save_waits_under_the_lock_and_builds_on_the_one_before(self, db):
        """Another save holds the lock and writes version 2; ours waits, then
        writes version 3 over it. Kills: a read before the lock."""
        created = await promotions.create(_data(), by=1)
        async with db.acquire() as conn:
            tx = conn.transaction()
            await tx.start()
            await conn.execute("SELECT pg_advisory_xact_lock($1)",
                               promotions.PROMOTIONS_LOCK_KEY)
            await conn.execute(
                "INSERT INTO app.promotion_versions (promotion_id, version, recorded_by, "
                "source, status, name, starts_on, ends_on, kind, scope, scope_items) "
                "VALUES ($1, 2, 9, 'form', 'planned', 'Інша', '2027-03-05', '2027-03-08', "
                "'other', 'brands', ARRAY['COSRX'])", created.id)
            waiting = asyncio.create_task(
                promotions.update(created.id, _data(name="Третя"), by=1))
            await asyncio.sleep(0.5)
            assert not waiting.done()
            await tx.commit()
        third = await waiting
        assert third.version == 3
        assert [r["name"] for r in await _rows(db, created.id)] == [
            "Весняний розпродаж", "Інша", "Третя"]

    @pytest.mark.asyncio
    async def test_the_calendar_now_and_as_it_was(self, db):
        """`current` is each promotion's latest version, `as_of(T)` its latest
        recorded at T or earlier; the window is an overlap of days. Kills: a
        later version leaking into the past, or a promotion saved after T
        appearing in T's calendar."""
        a = await promotions.create(_data(name="A", starts_on="2027-03-01",
                                          ends_on="2027-03-03"), by=1)
        a2 = await promotions.update(a.id, _data(name="A2", starts_on="2027-03-01",
                                                 ends_on="2027-03-03"), by=1)
        b = await promotions.create(_data(name="B", starts_on="2027-03-10",
                                          ends_on="2027-03-10"), by=1)
        b_cancelled = await promotions.cancel(b.id, by=1)

        window = (date(2027, 3, 1), date(2027, 3, 31))
        assert [(p.id, p.data.name) for p in await promotions.current(*window)] == [
            (a.id, "A2")]
        assert [(p.id, p.status) for p in await promotions.current(
            *window, include_cancelled=True)] == [(a.id, "planned"), (b.id, "cancelled")]
        # The edges: the last day before the window, the first day after.
        assert await promotions.current(date(2027, 3, 4), date(2027, 3, 9)) == []
        assert [p.id for p in await promotions.current(date(2027, 3, 3), date(2027, 3, 3))] == [a.id]

        between = a.recorded_at + (a2.recorded_at - a.recorded_at) / 2
        assert [(p.data.name, p.version) for p in await promotions.as_of(between, *window)] == [
            ("A", 1)]
        assert [(p.data.name, p.status) for p in await promotions.as_of(
            b.recorded_at, *window, include_cancelled=True)] == [
            ("A2", "planned"), ("B", "planned")]
        assert [p.data.name for p in await promotions.as_of(
            b_cancelled.recorded_at, *window)] == ["A2"]
        assert await promotions.as_of(a.recorded_at - timedelta(seconds=1), *window) == []
        with pytest.raises(ValueError):
            await promotions.as_of(datetime(2027, 1, 1), *window)

    @pytest.mark.asyncio
    async def test_the_history_and_the_campaigns_named(self, db):
        a = await promotions.create(_data(sms_campaign="Весна SMS"), by=5, sms_view=True)
        await promotions.update(a.id, _data(name="Інша"), by=6)
        history = await promotions.versions(a.id)
        assert [(p.version, p.recorded_by) for p in history] == [(2, 6), (1, 5)]
        assert all(p.created_at == a.recorded_at for p in history)
        assert await promotions.versions(a.id + 1000) == []
        assert await promotions.campaigns_named() == {"Весна SMS"}


# ─── The API through the app ─────────────────────────────────────────────────

@pytest.fixture
def through_the_app(db):
    """`core.pg.get_pool` answering every acquire with a connection of its
    own, in whichever loop asks: the app runs in the client's portal."""
    with patch("core.pg.get_pool", new=AsyncMock(return_value=_ConnectPerAcquire(DSN))):
        yield db


class TestTheApi:
    @pytest.mark.asyncio
    async def test_the_contracts_json_end_to_end(self, through_the_app, admin_client):  # noqa: F811
        response = admin_client.post("/api/promotions", json=_payload())
        assert response.status_code == 201, response.text
        made = response.json()
        rows = await _rows(through_the_app, made["id"])
        stamp = rows[0]["recorded_at"].astimezone(KYIV).isoformat(timespec="seconds")
        assert made == {
            "id": made["id"], "version": 1, "name": "Весняний розпродаж",
            "starts_on": "2027-03-05", "ends_on": "2027-03-08", "start_time": "18:00",
            "kind": "other", "discount_pct": 20, "scope": "brands",
            "scope_items": ["COSRX"], "channels": ["sms", "instagram"],
            "sms_campaign": None, "note": None, "status": "planned",
            "created_at": stamp, "updated_at": stamp, "source": "form"}

        listed = admin_client.get("/api/promotions", params={
            "from": "2027-03-01", "to": "2027-03-31"}).json()
        assert listed == {"promotions": [made]}

        pid = made["id"]
        edited = admin_client.put(f"/api/promotions/{pid}", json=_payload(discount_pct=12.5))
        assert edited.status_code == 200 and edited.json()["version"] == 2
        assert edited.json()["discount_pct"] == 12.5
        assert edited.json()["created_at"] == stamp

        assert admin_client.post(f"/api/promotions/{pid}/cancel").json()["status"] == "cancelled"
        assert admin_client.put(f"/api/promotions/{pid}", json=_payload()).status_code == 409
        assert admin_client.post(f"/api/promotions/{pid}/cancel").status_code == 409
        assert admin_client.post(f"/api/promotions/{pid}/restore").json()["version"] == 4
        assert admin_client.post(f"/api/promotions/{pid}/restore").status_code == 409
        assert admin_client.put("/api/promotions/987654321", json=_payload()).status_code == 404
        assert admin_client.get("/api/promotions/987654321/versions").status_code == 404

        bad = admin_client.post("/api/promotions", json=_payload(channels=["fax"]))
        assert bad.status_code == 400
        assert bad.json() == {"detail": "channels: Must be among sms, instagram, telegram, site"}

        history = admin_client.get(f"/api/promotions/{pid}/versions").json()["versions"]
        assert [(v["version"], v["status"]) for v in history] == [
            (4, "planned"), (3, "cancelled"), (2, "planned"), (1, "planned")]
        assert all(v["recorded_by_me"] is True for v in history)
        assert all("recorded_by" not in v for v in history)
        assert history[-1]["recorded_at"] == stamp

    @pytest.mark.asyncio
    async def test_a_viewer_of_the_tab_reads_and_may_not_save(
            self, through_the_app, monkeypatch):
        from fastapi.testclient import TestClient

        from core import permissions
        from web.main import app
        from web.ratelimit import limiter
        from web.routes.auth import SESSION_COOKIE, create_session_data, session_serializer

        person = 555_000_778

        async def resolve(session):
            return {"user_id": person, "role": "viewer", "allowed_features": ["marketing"]}

        async def matrix(role):
            return permissions.get_permissions_for_role(role)

        monkeypatch.setattr("web.routes.auth._resolve_session", resolve)
        monkeypatch.setattr(permissions, "get_permissions_for_role_async", matrix)
        limiter.reset()
        client = TestClient(app)
        client.cookies.set(SESSION_COOKIE, session_serializer.dumps(create_session_data(
            {"id": str(person), "first_name": "T", "last_name": "U", "username": "t",
             "auth_date": str(int(_time.time()))}, role="viewer")))
        made = await promotions.create(_data(), by=111)
        assert client.get("/api/promotions", params={"from": "2027-03-01"}).status_code == 200
        history = client.get(f"/api/promotions/{made.id}/versions").json()["versions"]
        assert history[0]["recorded_by_me"] is False
        assert client.post("/api/promotions", json=_payload()).status_code == 403
        assert len(await _rows(through_the_app, made.id)) == 1
        limiter.reset()

    @pytest.mark.asyncio
    async def test_the_sms_link_is_the_sms_tabs(self, through_the_app, monkeypatch):
        """An editor holding marketing and not sms: the campaign is null in
        every answer, the promotion sent back whole keeps it in the next
        version, and naming another one is a 403 with nothing written.
        Kills: the link changed, or cleared, by somebody who may not open
        /sms, and its name served to them."""
        from fastapi.testclient import TestClient

        from core import permissions
        from web.main import app
        from web.ratelimit import limiter
        from web.routes.auth import SESSION_COOKIE, create_session_data, session_serializer

        person = 555_000_779

        async def resolve(session):
            return {"user_id": person, "role": "editor", "allowed_features": ["marketing"]}

        async def matrix(role):
            return permissions.get_permissions_for_role(role)

        monkeypatch.setattr("web.routes.auth._resolve_session", resolve)
        monkeypatch.setattr(permissions, "get_permissions_for_role_async", matrix)
        limiter.reset()
        client = TestClient(app)
        client.cookies.set(SESSION_COOKIE, session_serializer.dumps(create_session_data(
            {"id": str(person), "first_name": "T", "last_name": "U", "username": "t",
             "auth_date": str(int(_time.time()))}, role="editor")))
        made = await promotions.create(_data(sms_campaign="Весна SMS"), by=111, sms_view=True)
        window = {"from": "2027-03-01", "to": "2027-03-31"}

        (shown,) = client.get("/api/promotions", params=window).json()["promotions"]
        assert shown["sms_campaign"] is None
        history = client.get(f"/api/promotions/{made.id}/versions").json()["versions"]
        assert [v["sms_campaign"] for v in history] == [None]

        edited = client.put(f"/api/promotions/{made.id}", json={**shown, "name": "Інша"})
        assert edited.status_code == 200, edited.text
        assert (edited.json()["version"], edited.json()["sms_campaign"]) == (2, None)
        rows = await _rows(through_the_app, made.id)
        assert [(r["name"], r["sms_campaign"]) for r in rows] == [
            ("Весняний розпродаж", "Весна SMS"), ("Інша", "Весна SMS")]

        refused = client.put(f"/api/promotions/{made.id}",
                             json={**shown, "sms_campaign": "Осінь SMS"})
        assert (refused.status_code, refused.json()) == (403, {"detail": "No view access to sms"})
        assert client.post("/api/promotions",
                           json=_payload(source="form_from_sms")).status_code == 403
        assert len(await _rows(through_the_app, made.id)) == 2
        async with through_the_app.acquire() as conn:
            assert await conn.fetchval("SELECT count(DISTINCT promotion_id) "
                                       "FROM app.promotion_versions") == 1
        limiter.reset()


# ─── The bot's answer and the job's ledger ──────────────────────────────────

class TestTheMondayQuestion:
    @pytest.mark.asyncio
    async def test_a_checkin_goes_through_the_bots_adapter(self, db, tmp_path, monkeypatch):
        """The bot's own pool and loop thread, the shared statement. Kills:
        the adapter writing somewhere the web container does not read."""
        from bot import store_sqlite
        from bot.store_postgres import PostgresBotStore

        monkeypatch.setattr(store_sqlite, "DB_PATH", tmp_path / "bot.db")
        store = PostgresBotStore(DSN)
        try:
            store.initialise()
            assert store.promo_asks.available() is True
            answer_id = store.promo_asks.record_none(
                period_from=date(2026, 11, 23), period_to=date(2026, 12, 6), by_user_id=4242)
        finally:
            store.close()
        async with db.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM app.promotion_checkins WHERE id = $1",
                                      answer_id)
        assert (row["period_from"], row["period_to"], row["answer"], row["answered_by"]) == (
            date(2026, 11, 23), date(2026, 12, 6), "none", 4242)
        assert row["answered_at"] is not None

    @pytest.mark.asyncio
    async def test_one_answer_per_window_and_person(self, db, tmp_path, monkeypatch):
        """`RECORD_NONE_SQL` on a server: a second answer of one person for
        one window inserts nothing and returns no id; another person, or the
        next window, is an answer of its own. Kills: a duplicate row for a
        double tap, or a statement whose parameters the server cannot type."""
        from bot import store_sqlite
        from bot.store_postgres import PostgresBotStore

        monkeypatch.setattr(store_sqlite, "DB_PATH", tmp_path / "bot.db")
        monday, sunday = date(2026, 11, 23), date(2026, 12, 6)
        later = (monday + timedelta(days=7), sunday + timedelta(days=7))
        store = PostgresBotStore(DSN)
        try:
            store.initialise()
            asks = store.promo_asks
            first = asks.record_none(period_from=monday, period_to=sunday, by_user_id=4242)
            again = asks.record_none(period_from=monday, period_to=sunday, by_user_id=4242)
            other = asks.record_none(period_from=monday, period_to=sunday, by_user_id=4343)
            next_week = asks.record_none(period_from=later[0], period_to=later[1],
                                         by_user_id=4242)
        finally:
            store.close()
        assert isinstance(first, int) and again is None
        assert isinstance(other, int) and isinstance(next_week, int)
        async with db.acquire() as conn:
            rows = await conn.fetch("SELECT period_from, answered_by FROM app.promotion_checkins "
                                    "ORDER BY id")
        assert [(r["period_from"], r["answered_by"]) for r in rows] == [
            (monday, 4242), (monday, 4343), (later[0], 4242)]

    @pytest.mark.asyncio
    async def test_the_job_asks_once_a_week_and_names_last_weeks_sms(self, db, monkeypatch):
        sent: List[dict] = []

        async def send(text, parse_mode="HTML", *, token=None, chat_ids=None,
                       reply_markup=None):
            sent.append({"text": text, "chat_ids": list(chat_ids or ())})
            return len(list(chat_ids or ()))

        import importlib

        config = importlib.import_module("core.config")
        monkeypatch.setenv("KS_PG_DSN", DSN)
        monkeypatch.delenv(promo_asks.RECIPIENTS_ENV, raising=False)
        monkeypatch.setattr(config, "ADMIN_USER_IDS", {10})
        monkeypatch.setattr("core.telegram_alerts.send_admin_message_http", send)
        monkeypatch.setattr("core.bot_prefs.read_user_languages",
                            lambda ids, defaults=None: {i: "uk" for i in ids})
        now = datetime.now(KYIV)
        week = promo_asks.week_start(now.date())
        campaigns = ("ks-test-promo-sms-a", "ks-test-promo-sms-b")
        async with db.acquire() as conn:
            await conn.execute("DELETE FROM app.sms_campaigns WHERE campaign = ANY($1)",
                               list(campaigns))
            await conn.executemany(
                "INSERT INTO app.sms_campaigns (campaign, ltv_basis, sales_type, holdout_pct, "
                "criteria, promocode, exported_at) VALUES ($1, 'net', 'retail', 0, '{}', $2, "
                "now() - interval '2 days')",
                [(campaigns[0], "AUTUMN10"), (campaigns[1], None)])
        await promotions.create(_data(sms_campaign=campaigns[1]), by=1, sms_view=True)
        try:
            first = await promo_asks.run_weekly_ask()
            second = await promo_asks.run_weekly_ask()
        finally:
            async with db.acquire() as conn:
                await conn.execute("DELETE FROM app.sms_campaigns WHERE campaign = ANY($1)",
                                   list(campaigns))
        assert first["sent"] is True and first["week"] == week.isoformat()
        assert second == {"week": week.isoformat(), "sent": False, "reason": "already_asked"}
        (message,) = sent
        assert message["chat_ids"] == [10]
        assert "«ks-test-promo-sms-a» (промокод AUTUMN10)" in message["text"]
        assert campaigns[1] not in message["text"]
        async with db.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM app.promotion_asks WHERE week_start = $1",
                                      week)
        assert row["delivered"] == 1


# ─── planned_promotions, end to end ─────────────────────────────────────────

RUNS = ((700, 3), (610, 2), (520, 3), (430, 2), (340, 3), (250, 2), (160, 3), (70, 2))


def _shop(today: date) -> Dict[date, float]:
    """830 days to yesterday: a weekday pattern and eight runs of 2-3 days at
    ×2.5 with a day at ×0.8 after each, the most recent 70 days back."""
    weekday = (1.10, 1.05, 1.00, 1.00, 1.08, 0.85, 0.80)
    revenue = {}
    for back in range(830, 0, -1):
        day = today - timedelta(days=back)
        revenue[day] = round(100_000.0 * weekday[day.weekday()], 2)
    for back, length in RUNS:
        for k in range(length):
            revenue[today - timedelta(days=back - k)] *= 2.5
        revenue[today - timedelta(days=back - length)] *= 0.8
    return {d: round(v, 2) for d, v in revenue.items()}


class TestPlannedPromotionsEndToEnd:
    @pytest.mark.asyncio
    async def test_the_forecast_lists_this_months_promotions(
            self, admin_client, monkeypatch):  # noqa: F811
        """Bronze seeded, Silver and Gold derived by the production path, the
        forecast of every day left stored, promotions saved by the writer:
        the forecast's answer lists the two planned for the rest of this
        month, the ordinary one with the typical effect of the same history,
        Black Friday's with none. The cancelled, the past and next month's
        are not there. Kills: a wrong window, a cancelled promotion counted,
        an extra that is not `typical_promotion`'s on Gold."""
        import core.prediction_service as ps
        from core import forecast_range
        from tests.integration.pg_tab_seed import derive, seed, tab_in_postgres

        today = datetime.now(KYIV).date()
        month_end = forecast_range.month_end(today)
        shop = _shop(today)

        async def populate(conn, pool):
            await seed(conn, orders=[
                (900_000 + i, 1, 1, value,
                 datetime.combine(day, time(12, 0), tzinfo=KYIV), 1, None, None)
                for i, (day, value) in enumerate(sorted(shop.items()))])
            await derive(pool)
            await conn.executemany(
                "INSERT INTO app.revenue_predictions (prediction_date, sales_type, "
                "predicted_revenue, model_mae, model_mape, model_wape, created_at) "
                "VALUES ($1, 'retail', 120000, 1, 1, 1, now())",
                [(today + timedelta(days=n),) for n in range(61)])

        monkeypatch.setattr(ps, "_today_kyiv", lambda: today)
        monkeypatch.setattr(ps, "_service", None)
        async with tab_in_postgres(DSN, monkeypatch, populate) as pool:
            with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)):
                ordinary = await promotions.create(_data(
                    name="Звичайна", starts_on=today.isoformat(),
                    ends_on=today.isoformat()), by=1)
                black = await promotions.create(_data(
                    name="Чорна п'ятниця", kind="black_friday",
                    starts_on=month_end.isoformat(), ends_on=month_end.isoformat()), by=1)
                gone = await promotions.create(_data(
                    name="Скасована", starts_on=today.isoformat(),
                    ends_on=today.isoformat()), by=1)
                await promotions.cancel(gone.id, by=1)
                await promotions.create(_data(
                    name="Минула", starts_on=(today - timedelta(days=3)).isoformat(),
                    ends_on=(today - timedelta(days=1)).isoformat()), by=1)
                await promotions.create(_data(
                    name="Наступного місяця",
                    starts_on=(month_end + timedelta(days=1)).isoformat(),
                    ends_on=(month_end + timedelta(days=2)).isoformat()), by=1)

            with patch("core.pg.get_pool", new=AsyncMock(return_value=_ConnectPerAcquire(DSN))):
                response = admin_client.get("/api/revenue/forecast")
            assert response.status_code == 200, response.text
            answer = response.json()

        rows = [(d, v) for d, v in sorted(shop.items()) if d < today]
        typical = forecast_range.promotion_outlook(rows, today)
        assert typical is not None and typical.runs == len(RUNS)
        assert answer["planned_promotions"] == [
            {"id": ordinary.id, "name": "Звичайна", "starts_on": today.isoformat(),
             "ends_on": today.isoformat(), "kind": "other",
             "extra": round(typical.extra, 2)},
            {"id": black.id, "name": "Чорна п'ятниця", "starts_on": month_end.isoformat(),
             "ends_on": month_end.isoformat(), "kind": "black_friday", "extra": None},
        ]
        assert answer["month_range"] is not None
