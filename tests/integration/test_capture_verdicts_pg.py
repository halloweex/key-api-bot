"""The channel-verdict log against a real Postgres (revision 0036).

`core/pg_utm_parse.py` appends to `app.order_channel_verdicts` once a parse
has committed, in a transaction of its own: a content diff of the whole of
`silver.order_utm` against each order's latest captured verdict, then a
self-check at zero tolerance. Every claim here is about what a server does —
which rows the diff selects, what a refusal leaves, which session waits — so
it is proved on one.

HOW A SCENARIO IS ISOLATED
The diff reads whole tables, so most scenarios run inside one transaction that
empties `bronze.orders`, `silver.order_utm` and the log and is rolled back, as
`tests/integration/test_pg_utm_parse.py` does. The capture refuses a
connection that is inside a transaction, so the scenario hands it a view of
its connection that says it is not (`_Outside`): its own transaction then
becomes a savepoint of the scenario's, which is how a failure inside it is
seen to roll back alone. The last class runs on real connections, committing,
under an order id of its own, and removes what it wrote.

Skipped without `KS_PG_DSN`; `deploy/gate_with_stores.sh` and CI supply one.
"""
from __future__ import annotations

import asyncio
import hashlib
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from core import analytics_captures as captures
from core import pg_utm_parse as parse
from core.pg_utm_parse import UTM_TABLE, VERDICT_COLUMNS, VERDICTS_TABLE
from core.utm_classify import RULE_VERSION, utm_columns

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(
    not DSN, reason="needs a live PostgreSQL at KS_PG_DSN",
)

STAMP = captures.state_row(VERDICTS_TABLE)
UPDATED = datetime(2026, 11, 2, 9, 0, 0, 123457, tzinfo=timezone.utc)
PAID = "UTM: utm_source: fbads; utm_medium: cpc; utm_campaign: spring"
EDITED = "UTM: utm_source: fbads; utm_medium: cpc; utm_campaign: autumn"
PIXEL = "UTM: _fbp: fb.1.123.456"
PLAIN = "Передзвонити після 18:00"           # no tracking data: a row of NULLs
NONE5 = (None,) * len(VERDICT_COLUMNS)


def verdict_of(comment):
    """The five columns the log keeps, as the parser decides them."""
    row = dict(zip(parse.UTM_COLUMNS[1:-1], utm_columns(comment)))
    return tuple(row[c] for c in VERDICT_COLUMNS)


def py_hash(values) -> int:
    """§3.4's row hash computed outside Postgres: the first 15 hex digits of
    the md5 of the canonical row (`\\N` for NULL, chr(31) between)."""
    canon = chr(31).join("\\N" if v is None else v for v in values)
    return int(hashlib.md5(canon.encode("utf-8")).hexdigest()[:15], 16)


class _Outside:
    """The scenario's connection, telling the capture it is outside a
    transaction. Its `transaction()` is the real one's, so the capture's
    transaction is a savepoint of the scenario's — rolled back with it here,
    the connection's own in production."""

    def __init__(self, conn):
        self._conn = conn

    def is_in_transaction(self):
        return False

    def __getattr__(self, name):
        return getattr(self._conn, name)


class _OneConnectionPool:
    def __init__(self, conn):
        self._conn = conn

    @asynccontextmanager
    async def acquire(self, **_kw):
        yield self._conn


@pytest.fixture(autouse=True)
def captures_on_and_a_fresh_lock():
    env = {k: v for k, v in os.environ.items() if k != captures.SWITCH_ENV}
    with patch.dict(os.environ, env, clear=True), \
         patch("core.pg_silver.PG_LAYER_LOCK", asyncio.Lock()):
        yield


@pytest_asyncio.fixture
async def conn():
    c = await asyncpg.connect(DSN)
    tx = c.transaction()
    await tx.start()
    try:
        await c.execute("SET LOCAL lock_timeout = '5s'")
        await c.execute("SET LOCAL statement_timeout = '60s'")
        await c.execute(f"DELETE FROM {VERDICTS_TABLE}")
        await c.execute(f"DELETE FROM {UTM_TABLE}")
        await c.execute("DELETE FROM bronze.orders")
        await c.execute("DELETE FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
                        [UTM_TABLE, STAMP])
        pool = _OneConnectionPool(_Outside(c))
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
             patch("core.pg.require_revision", new=AsyncMock()):
            yield c
    finally:
        await tx.rollback()
        await c.close()


async def order(conn, oid, *, comment=PAID, updated_at=UPDATED):
    await conn.execute(
        """
        INSERT INTO bronze.orders
               (id, source_id, status_id, grand_total, updated_at, manager_comment)
        VALUES ($1, 4, 1, 100, $2, $3)
        ON CONFLICT (id) DO UPDATE SET updated_at = EXCLUDED.updated_at,
                                       manager_comment = EXCLUDED.manager_comment
        """, oid, updated_at, comment)


async def log(conn, where="TRUE"):
    return [dict(r) for r in await conn.fetch(
        f"SELECT id, order_id, known_from, rule_version, change_kind, "
        f"{', '.join(VERDICT_COLUMNS)}, row_hash FROM {VERDICTS_TABLE} "
        f"WHERE {where} ORDER BY id")]


def five(row):
    return tuple(row[c] for c in VERDICT_COLUMNS)


async def silver_five(conn):
    return {r["order_id"]: five(r) for r in await conn.fetch(
        f"SELECT order_id, {', '.join(VERDICT_COLUMNS)} FROM {UTM_TABLE}")}


async def stamp(conn):
    return await conn.fetchrow(
        "SELECT last_ok_at, failures_since_ok, last_error, last_rows "
        "FROM meta.mirror_state WHERE table_name = $1", STAMP)


def judged(row):
    """What the integrity job files for this capture's stamp (the other
    captures' stamps are not this test's)."""
    facts = captures.Facts(now=datetime.now(timezone.utc), stamps={
        VERDICTS_TABLE: captures.Stamp(
            row["last_ok_at"], row["failures_since_ok"], row["last_error"])})
    return [f.check_name for f in captures.judge(facts) if f.table_name == VERDICTS_TABLE]


def refusals(monkeypatch):
    """Every `CaptureRefused` the capture raises: the numbers and ids a
    refusal was written from, read without parsing its sentence."""
    seen = []
    real = parse._append_verdicts

    async def spy(conn, kind):
        try:
            return await real(conn, kind)
        except captures.CaptureRefused as exc:
            seen.append(exc)
            raise

    monkeypatch.setattr(parse, "_append_verdicts", spy)
    return seen


async def stored_verdict(conn, oid, verdict, *, comment):
    """A verdict as an earlier rule stored it: the order, and its row in
    `silver.order_utm`, with no parse since."""
    await order(conn, oid, comment=comment)
    await conn.execute(
        f"INSERT INTO {UTM_TABLE} (order_id, {', '.join(VERDICT_COLUMNS)}, parsed_at) "
        f"VALUES ($1, $2, $3, $4, $5, $6, $7)", oid, *verdict, UPDATED)


async def utm_watermark(conn):
    return await conn.fetchrow(
        "SELECT failures_since_ok, last_error FROM meta.mirror_state "
        "WHERE table_name = $1", UTM_TABLE)


# ── the first run ─────────────────────────────────────────────────────────────

class TestTheFirstRun:
    @pytest.mark.asyncio
    async def test_it_seeds_first_for_every_verdict_the_table_holds(self, conn):
        """A row of NULLs included: it is a verdict the parse stored — the
        comment carries no tracking data — and the readers' fallback."""
        for oid, comment in ((1, PAID), (2, PLAIN), (3, PIXEL)):
            await order(conn, oid, comment=comment)

        await parse.parse_incremental()

        rows = await log(conn)
        assert [(r["order_id"], r["change_kind"], r["rule_version"]) for r in rows] == [
            (1, "first", RULE_VERSION), (2, "first", RULE_VERSION),
            (3, "first", RULE_VERSION)]
        assert {r["order_id"]: five(r) for r in rows} == await silver_five(conn)
        assert five(rows[1]) == NONE5
        assert five(rows[0]) == verdict_of(PAID) == (
            "paid_confirmed", "facebook", "fbads", "cpc", "spring")
        # One capture, one clock.
        assert len({r["known_from"] for r in rows}) == 1
        s = await stamp(conn)
        assert s["failures_since_ok"] == 0 and s["last_ok_at"] is not None
        assert s["last_rows"] == 3

    @pytest.mark.asyncio
    async def test_a_seed_verdict_todays_rules_do_not_make_says_so(self, conn):
        """The review's case: `silver.order_utm` still holds a verdict an
        earlier rule decided (`pixel_only/facebook`, before 2026-09-08). The
        seed keeps it as it was, under `pre-r1` — never `r1`, which never
        makes it — and a verdict today's rules do make, under `r1`. Kills: a
        reclassify's move of the 3 769 read as the next version's drift."""
        old = ("pixel_only", "facebook", None, None, None)
        await stored_verdict(conn, 1, old, comment=PIXEL)
        await stored_verdict(conn, 2, verdict_of(PAID), comment=PAID)

        await parse.parse_incremental()

        assert [(r["order_id"], r["rule_version"], five(r)) for r in await log(conn)] == [
            (1, parse.earlier_rules(), old), (2, RULE_VERSION, verdict_of(PAID))]

    @pytest.mark.asyncio
    async def test_the_row_hash_is_the_designs(self, conn):
        """Computed by Postgres, equal to the md5 prefix computed outside it —
        what lets ClickHouse check a copy of the log against its own rows —
        and never negative."""
        comments = (PAID, PLAIN, PIXEL, "UTM: utm_source: ig; utm_campaign: Весна ✓")
        for oid, comment in enumerate(comments, start=1):
            await order(conn, oid, comment=comment)
        await parse.parse_incremental()

        rows = await log(conn)
        assert len(rows) == len(comments)
        for r in rows:
            assert r["row_hash"] == py_hash(five(r)) and r["row_hash"] >= 0
        assert len({r["row_hash"] for r in rows}) == len(rows)


# ── ordinary parses ───────────────────────────────────────────────────────────

class TestTheIncrementalParse:
    @pytest.mark.asyncio
    async def test_an_edited_comment_appends_one_change(self, conn):
        await order(conn, 1)
        await order(conn, 2, comment=PLAIN)
        await parse.parse_incremental()

        await order(conn, 1, comment=EDITED, updated_at=UPDATED + timedelta(minutes=5))
        assert (await parse.parse_incremental())["parsed"] == 1

        rows = await log(conn, "order_id = 1")
        assert [r["change_kind"] for r in rows] == ["first", "change"]
        assert rows[1]["utm_campaign"] == "autumn"
        assert rows[1]["known_from"] > rows[0]["known_from"]
        assert rows[1]["row_hash"] != rows[0]["row_hash"]
        assert len(await log(conn, "order_id = 2")) == 1
        assert (await stamp(conn))["last_rows"] == 1

    @pytest.mark.asyncio
    async def test_a_quiet_run_appends_nothing_and_still_stamps(self, conn):
        await order(conn, 1)
        await parse.parse_incremental()
        before = await log(conn)

        assert (await parse.parse_incremental())["parsed"] == 0

        assert await log(conn) == before
        s = await stamp(conn)
        assert s["failures_since_ok"] == 0 and s["last_rows"] == 0

    @pytest.mark.asyncio
    async def test_a_reparse_to_the_same_verdict_is_no_change(self, conn):
        """A version is a change, not an observation: `updated_at` moved and
        the comment is re-parsed, but nothing the log keeps moved."""
        await order(conn, 1)
        await parse.parse_incremental()
        tagged = "UTM: utm_source: fbads; utm_medium: cpc; utm_campaign: spring; utm_term: x"
        await order(conn, 1, comment=tagged, updated_at=UPDATED + timedelta(minutes=5))

        assert (await parse.parse_incremental())["parsed"] == 1

        assert [r["change_kind"] for r in await log(conn)] == ["first"]


# ── the reclassify ────────────────────────────────────────────────────────────

def _old_rules(comment):
    """A classifier that called every paid Facebook order "other"."""
    row = list(utm_columns(comment))
    if row[-1] == "facebook":
        row[-2:] = ["unknown", "other"]
    return tuple(row)


class TestTheFullParse:
    @pytest.mark.asyncio
    async def test_what_the_rules_moved_is_a_reclassify(self, conn):
        """The case D12 lands the log for: verdicts stored under the old rules,
        logged first, then every one the new rules move, as `reclassify`."""
        await order(conn, 1)                       # paid Facebook: moves
        await order(conn, 2, comment=PLAIN)        # NULLs: stays
        await order(conn, 3, comment="UTM: utm_source: ig")   # organic: stays
        with patch("core.pg_utm_parse.utm_columns", side_effect=_old_rules):
            await parse.parse_incremental()
        assert five((await log(conn, "order_id = 1"))[0])[:2] == ("unknown", "other")

        result = await parse.parse_full()

        assert result["verdicts_logged"] is True
        rows = await log(conn)
        assert [(r["order_id"], r["change_kind"]) for r in rows] == [
            (1, "first"), (2, "first"), (3, "first"), (1, "reclassify")]
        assert five(rows[-1]) == verdict_of(PAID)
        assert {r["order_id"]: five(r) for r in rows} == await silver_five(conn)

    @pytest.mark.asyncio
    async def test_through_the_reclassify_door(self, conn):
        await order(conn, 1)
        with patch("core.pg_utm_parse.utm_columns", side_effect=_old_rules):
            await parse.parse_incremental()

        with patch("core.mirror_reconciliation.configured", return_value=True):
            assert "rows" in await parse.reparse_router(full=True)

        assert [r["change_kind"] for r in await log(conn)] == ["first", "reclassify"]

    @pytest.mark.asyncio
    async def test_a_new_order_in_a_full_parse_is_first(self, conn):
        await order(conn, 1)
        await parse.parse_incremental()
        await order(conn, 2, comment=PIXEL)

        await parse.parse_full()

        assert [(r["order_id"], r["change_kind"]) for r in await log(conn)] == [
            (1, "first"), (2, "first")]

    @pytest.mark.asyncio
    async def test_a_row_that_left_the_table_is_recorded_as_no_verdict(self, conn):
        """A comment KeyCRM emptied: the full parse drops the order's row, and
        the readers fall back exactly as for a row of NULLs. So the log says
        NULLs — once — and an order whose verdict was already NULLs gains no
        row at all."""
        for oid in range(201, 220):
            await order(conn, oid)
        await order(conn, 220, comment=PLAIN)
        await parse.parse_incremental()
        await conn.execute(
            "UPDATE bronze.orders SET manager_comment = NULL WHERE id IN (219, 220)")

        result = await parse.parse_full()          # 18 of 20: on the floor

        assert result["rows"] == 18
        dropped = await log(conn, "order_id IN (219, 220)")
        assert [(r["order_id"], r["change_kind"]) for r in dropped] == [
            (219, "first"), (220, "first"), (219, "reclassify")]
        assert five(dropped[-1]) == NONE5
        assert dropped[-1]["row_hash"] == py_hash(NONE5) == dropped[1]["row_hash"]

        before = await log(conn)
        await parse.parse_incremental()
        await parse.parse_full()
        assert await log(conn) == before, "NULLs after NULLs is no change"

    @pytest.mark.asyncio
    async def test_a_reclassify_against_an_empty_log_keeps_what_it_overwrote(self, conn):
        """D12, the review's case: verdicts stored under an earlier rule, no
        capture yet, then the reclassify. The verdict it overwrites is in the
        log as `first` — under `pre-r1`, since today's rules do not make it —
        and the new one as `reclassify`. Kills: a full parse that logs only
        after it has replaced the table."""
        old = ("pixel_only", "facebook", None, None, None)
        await stored_verdict(conn, 1, old, comment=PIXEL)
        assert verdict_of(PIXEL) != old

        with patch("core.mirror_reconciliation.configured", return_value=True):
            result = await parse.reparse_router(full=True)

        assert result["verdicts_logged"] is True
        rows = await log(conn)
        assert [(r["change_kind"], r["rule_version"], five(r)) for r in rows] == [
            ("first", parse.earlier_rules(), old),
            ("reclassify", RULE_VERSION, verdict_of(PIXEL))]

    @pytest.mark.asyncio
    async def test_a_change_no_capture_kept_is_logged_before_the_reclassify(self, conn):
        """An incremental parse whose capture failed left a `change` owed; the
        full parse logs it as `change` before overwriting it."""
        await order(conn, 1)
        await parse.parse_incremental()
        await order(conn, 1, comment=EDITED, updated_at=UPDATED + timedelta(minutes=5))
        with patch.object(parse, "VERDICT_CHECK_SQL", "SELECT 1/0 AS differing"):
            await parse.parse_incremental()
        assert [r["change_kind"] for r in await log(conn)] == ["first"]

        with patch("core.pg_utm_parse.utm_columns", side_effect=_old_rules):
            await parse.parse_full()

        rows = await log(conn)
        assert [r["change_kind"] for r in rows] == ["first", "change", "reclassify"]
        assert rows[1]["utm_campaign"] == "autumn"

    @pytest.mark.asyncio
    async def test_with_the_switch_off_a_reclassify_is_refused(self, conn):
        """D12: no reclassify without the log. Kills: a full parse that
        replaces the table while nothing keeps what it overwrote."""
        await order(conn, 1)
        with patch("core.pg_utm_parse.utm_columns", side_effect=_old_rules):
            await parse.parse_incremental()
        before = await silver_five(conn)

        with patch.dict(os.environ, {captures.SWITCH_ENV: "off"}):
            result = await parse.parse_full()

        assert result["refused"] == parse.UNLOGGED_REFUSAL
        assert await silver_five(conn) == before
        assert (await utm_watermark(conn))["last_error"] == parse.UNLOGGED_REFUSAL

    @pytest.mark.asyncio
    async def test_a_refused_full_parse_logs_nothing(self, conn):
        for oid in range(201, 211):
            await order(conn, oid)
        await parse.parse_incremental()
        before = await log(conn)
        await conn.execute("DELETE FROM bronze.orders WHERE id > 201")

        assert "refused" in await parse.parse_full()

        assert await log(conn) == before


# ── a capture never fails the parse ───────────────────────────────────────────

class TestTheParseNeverPaysForTheCapture:
    @pytest.mark.asyncio
    async def test_a_capture_failing_after_its_insert_is_rolled_back_alone(self, conn):
        """The INSERT ran and the next statement failed: the capture's rows go,
        the parse's verdicts stay, the parse's watermark is clean, the
        capture's stamp counts the failure — and the next run catches up."""
        await order(conn, 1)
        with patch.object(parse, "VERDICT_CHECK_SQL", "SELECT 1/0 AS differing"):
            result = await parse.parse_incremental()

        assert result["parsed"] == 1
        assert set(await silver_five(conn)) == {1}
        assert (await utm_watermark(conn))["failures_since_ok"] == 0
        assert await log(conn) == []
        s = await stamp(conn)
        assert s["failures_since_ok"] == 1
        assert s["last_error"].startswith("DivisionByZeroError")
        assert judged(s) == [f"capture_failed:{VERDICTS_TABLE}"]

        await parse.parse_incremental()

        assert [(r["order_id"], r["change_kind"]) for r in await log(conn)] == [(1, "first")]
        assert (await stamp(conn))["failures_since_ok"] == 0

    @pytest.mark.asyncio
    async def test_a_reclassify_whose_capture_before_fails_is_refused(self, conn):
        """D12: what the reclassify would overwrite was not kept, so it does
        not run; the table keeps its verdicts and the 409 says why. Forced,
        it runs and says the log did not keep it."""
        await order(conn, 1)
        with patch("core.pg_utm_parse.utm_columns", side_effect=_old_rules):
            await parse.parse_incremental()
        before = await silver_five(conn)
        with patch.object(parse, "VERDICT_CHECK_SQL", "SELECT 1/0 AS differing"):
            refused = await parse.parse_full()
            assert refused["refused"] == parse.UNLOGGED_REFUSAL
            assert await silver_five(conn) == before

            forced = await parse.parse_full(force=True)

        assert forced["rows"] == 1 and forced["verdicts_logged"] is False
        assert (await silver_five(conn))[1] == verdict_of(PAID)
        assert [r["change_kind"] for r in await log(conn)] == ["first"]

    @pytest.mark.asyncio
    async def test_a_reclassify_whose_capture_after_fails_replaces_the_table_and_says_so(
        self, conn,
    ):
        """The reclassify is the run D12 lands the log for, so its operator
        reads the outcome on the parse's own log line; the next capture fills
        the gap, as `change`."""
        await order(conn, 1)
        with patch("core.pg_utm_parse.utm_columns", side_effect=_old_rules):
            await parse.parse_incremental()
        real = parse._append_verdicts

        async def fails_after(c, kind):
            if kind == parse.RECLASSIFY:
                raise RuntimeError("the capture after the parse")
            return await real(c, kind)

        with patch.object(parse, "_append_verdicts", fails_after):
            result = await parse.parse_full()

        assert result["rows"] == 1 and result["verdicts_logged"] is False
        assert (await silver_five(conn))[1] == verdict_of(PAID)
        assert [r["change_kind"] for r in await log(conn)] == ["first"]

        await parse.parse_incremental()

        assert [r["change_kind"] for r in await log(conn)] == ["first", "change"]

    @pytest.mark.asyncio
    async def test_the_switch_off_logs_nothing_and_stamps_nothing(self, conn):
        await order(conn, 1)
        with patch.dict(os.environ, {captures.SWITCH_ENV: "off"}):
            result = await parse.parse_incremental()

        assert result["parsed"] == 1 and set(await silver_five(conn)) == {1}
        assert await log(conn) == [] and await stamp(conn) is None


# ── the self-check ────────────────────────────────────────────────────────────

class TestTheSelfCheck:
    @pytest.mark.asyncio
    async def test_a_stored_hash_that_is_not_its_columns_refuses_everything(
            self, conn, monkeypatch):
        """A hash renderer changed without rewriting the stored hashes would
        diff every order on the next run. The check refuses instead — nothing
        appended, a change that was due included — and says which."""
        await order(conn, 1)
        await order(conn, 2, comment=PIXEL)
        await parse.parse_incremental()
        refused = refusals(monkeypatch)
        await conn.execute(f"UPDATE {VERDICTS_TABLE} SET row_hash = 1 WHERE order_id = 1")
        await order(conn, 2, comment=EDITED, updated_at=UPDATED + timedelta(minutes=5))

        await parse.parse_incremental()

        assert len(await log(conn)) == 2
        s = await stamp(conn)
        assert s["failures_since_ok"] == 1
        assert s["last_error"].startswith(captures.SELF_CHECK_MARK + "CaptureRefused")
        assert [r.facts for r in refused] == [{
            "differing": 0, "differing_ids": (), "mishashed": 1, "mishashed_ids": (1,)}]
        assert judged(s) == [f"capture_self_check_failed:{VERDICTS_TABLE}"]

        await conn.execute(
            f"UPDATE {VERDICTS_TABLE} SET row_hash = $1 WHERE order_id = 1",
            py_hash(verdict_of(PAID)))
        await parse.parse_incremental()

        assert [(r["order_id"], r["change_kind"]) for r in await log(conn)] == [
            (1, "first"), (2, "first"), (2, "change")]
        assert (await stamp(conn))["failures_since_ok"] == 0

    @pytest.mark.asyncio
    async def test_a_latest_row_it_cannot_supersede_refuses(self, conn, monkeypatch):
        """A row whose `known_from` is later than the capture's clock stays the
        latest whatever the capture appends, so the log would go on disagreeing
        with the table. Zero tolerance: refused, nothing appended."""
        await order(conn, 1)
        await parse.parse_incremental()
        refused = refusals(monkeypatch)
        await conn.execute(
            f"INSERT INTO {VERDICTS_TABLE} (order_id, known_from, rule_version, "
            f"change_kind, traffic_type, platform, row_hash) "
            f"VALUES (1, clock_timestamp() + interval '1 day', 'r0', 'change', "
            f"'unknown', 'other', $1)",
            py_hash(("unknown", "other", None, None, None)))

        await parse.parse_incremental()

        assert len(await log(conn)) == 2
        s = await stamp(conn)
        assert s["last_error"].startswith(captures.SELF_CHECK_MARK)
        (refusal,) = refused
        assert (refusal.facts["differing"], refusal.facts["differing_ids"]) == (1, (1,))


# ── on real connections ───────────────────────────────────────────────────────

ORDER = -424242      # an id no fixture and no production order uses


@pytest_asyncio.fixture
async def committed():
    """A real connection, outside any transaction, and a clean-up of every
    row this test's captures commit: the log above the id it found, the
    stamp as it was, and this test's order."""
    c = await asyncpg.connect(DSN)
    top = await c.fetchval(f"SELECT COALESCE(max(id), 0) FROM {VERDICTS_TABLE}")
    kept = await c.fetchrow("SELECT * FROM meta.mirror_state WHERE table_name = $1", STAMP)
    try:
        yield c
    finally:
        await c.execute(f"DELETE FROM {VERDICTS_TABLE} WHERE id > $1", top)
        await c.execute(f"DELETE FROM {UTM_TABLE} WHERE order_id = $1", ORDER)
        await c.execute("DELETE FROM bronze.orders WHERE id = $1", ORDER)
        await c.execute("DELETE FROM meta.mirror_state WHERE table_name = $1", STAMP)
        if kept is not None:
            await c.execute(
                "INSERT INTO meta.mirror_state (table_name, last_attempted_at, "
                "last_ok_at, failures_since_ok, last_error, last_rows) "
                "VALUES ($1, $2, $3, $4, $5, $6)",
                STAMP, kept["last_attempted_at"], kept["last_ok_at"],
                kept["failures_since_ok"], kept["last_error"], kept["last_rows"])
        await c.close()


class TestOnARealConnection:
    @pytest.mark.asyncio
    async def test_it_commits_in_a_transaction_of_its_own(self, committed):
        comment = "UTM: utm_source: ig"
        assert verdict_of(comment) == ("organic", "instagram", "ig", None, None)
        await order(committed, ORDER, comment=comment)
        await committed.execute(
            f"INSERT INTO {UTM_TABLE} (order_id, utm_source, traffic_type, platform, "
            f"parsed_at) VALUES ($1, 'ig', 'organic', 'instagram', now())", ORDER)

        assert await parse.capture_verdicts(committed, full=False) is True

        assert not committed.is_in_transaction()
        other = await asyncpg.connect(DSN)
        try:
            row = await other.fetchrow(
                f"SELECT change_kind, rule_version, traffic_type, platform, utm_source "
                f"FROM {VERDICTS_TABLE} WHERE order_id = $1", ORDER)
            ok = await other.fetchrow(
                "SELECT failures_since_ok, last_ok_at FROM meta.mirror_state "
                "WHERE table_name = $1", STAMP)
        finally:
            await other.close()
        assert tuple(row) == ("first", RULE_VERSION, "organic", "instagram", "ig")
        assert ok["failures_since_ok"] == 0 and ok["last_ok_at"] is not None

    @pytest.mark.asyncio
    async def test_it_waits_for_a_parse_holding_the_key_and_gives_up_bounded(
        self, committed,
    ):
        """Another process's parse holds `ADVISORY_LOCK_KEY` (the CLI's full
        parse): the capture waits `lock_timeout`, then fails on its own — a
        stamped failure naming the lock, False to the parse, nothing raised."""
        holder = await asyncpg.connect(DSN)
        try:
            await holder.execute("SELECT pg_advisory_lock($1)", parse.ADVISORY_LOCK_KEY)
            with patch.object(parse, "PG_LOCK_WAIT_S", 0.2):
                assert await parse.capture_verdicts(committed, full=False) is False
        finally:
            await holder.execute("SELECT pg_advisory_unlock($1)", parse.ADVISORY_LOCK_KEY)
            await holder.close()

        failed = await committed.fetchrow(
            "SELECT failures_since_ok, last_error FROM meta.mirror_state "
            "WHERE table_name = $1", STAMP)
        assert failed["failures_since_ok"] >= 1
        assert failed["last_error"].startswith("LockNotAvailableError")
