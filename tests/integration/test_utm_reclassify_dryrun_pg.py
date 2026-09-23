"""The reclassify dry run against a real PostgreSQL (DN-15).

What the unit tests cannot show is the server's side of "writes nothing": that
the transaction the script opens really is read-only to PostgreSQL, and that
the transaction-id check really does catch a write the declaration missed.
Both are shown here by making the script write — a statement swapped into its
read, and then the same with the read-only declaration taken away — and
watching the row stay as it was.

Also here: the reads against the real schema (column names, joins, the
source-based fallback as Postgres evaluates it), the snapshot against the table
it copies, and the refusal of the login CI actually has. That login is
`ks_app`, which can write, so the script's front door refuses it by design;
the tests below go through `read_state` behind that door, which is where every
remaining guarantee lives. What the door asks of `ks_readonly` is asked in CI
too, by name from the `ks_app` login, so a revision that grants the read-only
role a write fails here rather than in front of the owner. Each kind of write
privilege the door looks for is granted for real, inside a transaction that
is rolled back, and has to be named.

Two tests need what `ks_app` cannot make: a login for `ks_readonly` (the
front door walked as production walks it) and a NOINHERIT member of `ks_app`
(making a role takes CREATEROLE). `.github/workflows/ci.yml` provisions both
as the superuser, and their skip reasons carry the sentence its "no store test
may have skipped" step greps for — so in CI they run or the job fails. Locally,
against the recipe in `.claude/CLAUDE.md`, they skip unless provisioned the
same way.

Seeded rows use ids from 991 500 001 and are removed after each test, and every
assertion is about those ids: the database is shared with the rest of the
suite, so the whole-table numbers are whatever other tests left behind.
"""
from __future__ import annotations

import ast
import asyncio
import csv
import gzip
import os
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from urllib.parse import urlsplit

import pytest
import pytest_asyncio

asyncpg = pytest.importorskip("asyncpg")

from core.repositories.traffic import TrafficMixin  # noqa: E402
from core.utm_classify import UTM_VERDICT_COLUMNS, utm_columns  # noqa: E402
from scripts import utm_reclassify_dryrun as script  # noqa: E402
from tests.unit.test_utm_reclassify_dryrun import (  # noqa: E402
    POISONED_PG_DSN,
    UNSEEN_WRITERS,
)
from tests.write_audit import run_main_audited  # noqa: E402

DSN = os.getenv("KS_PG_DSN")
READONLY_DSN = os.getenv("KS_PG_READONLY_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

BASE = 991_500_000
# Created by CI as the superuser: NOINHERIT, a member of ks_app.
NOINHERIT_ROLE = "ks_ci_noinherit"
PROVISIONED = ("needs a live PostgreSQL provisioned as .github/workflows/ci.yml "
               "provisions it")
STAMP = datetime(2026, 9, 1, 9, 0, tzinfo=timezone.utc)
DAY = date(2026, 9, 1)
WINDOW = (date(2026, 6, 29), date(2026, 9, 20))

# id offset → (source_id, comment, stored verdict or None, parsed_at shift)
PIXEL = "_fbp: fb.1.1700000000.42"
SEED = {
    1: (4, PIXEL, "stale", timedelta(0)),                         # rule_change
    2: (4, "UTM: utm_source: fbads; utm_medium: cpc", "current", timedelta(0)),
    3: (1, "UTM: utm_source: ig", None, None),                    # unparsed
    4: (2, "", "orphan", timedelta(0)),                           # orphaned
    5: (4, "UTM: utm_source: google; utm_medium: cpc", "stale_campaign",
        timedelta(minutes=-5)),                                   # pending
    6: (1, "Доставка кур'єром", "current", timedelta(0)),         # a row of NULLs
    7: (4, None, None, None),                                     # untouched
}


def _stored(kind, comment):
    row = list(utm_columns(comment)) if comment else [None] * len(UTM_VERDICT_COLUMNS)
    if kind == "stale":
        row[UTM_VERDICT_COLUMNS.index("platform")] = "facebook"   # the pre-09-09 rule
    elif kind == "orphan":
        row = list(utm_columns("UTM: utm_source: fbads"))
    elif kind == "stale_campaign":
        row[UTM_VERDICT_COLUMNS.index("utm_campaign")] = "old"
    return tuple(row)


async def _clean(conn):
    await conn.execute("DELETE FROM silver.order_utm WHERE order_id > $1 AND order_id < $2",
                       BASE, BASE + 1000)
    await conn.execute("DELETE FROM silver.orders WHERE id > $1 AND id < $2", BASE, BASE + 1000)
    await conn.execute("DELETE FROM bronze.orders WHERE id > $1 AND id < $2", BASE, BASE + 1000)


@pytest_asyncio.fixture
async def seeded():
    conn = await asyncpg.connect(DSN)
    try:
        await _clean(conn)
        for offset, (source, comment, kind, shift) in SEED.items():
            oid = BASE + offset
            await conn.execute(
                "INSERT INTO bronze.orders (id, source_id, status_id, grand_total, "
                "ordered_at, created_at, updated_at, buyer_id, manager_comment) "
                "VALUES ($1, $2, 1, $3, $4, $4, $4, 10, $5)",
                oid, source, Decimal(100 * offset), STAMP, comment)
            await conn.execute(
                "INSERT INTO silver.orders (id, source_id, status_id, grand_total, "
                "ordered_at, buyer_id, order_date, is_return, sales_type, "
                "is_active_source, source_name) "
                "VALUES ($1, $2, 1, $3, $4, 10, $5, false, 'retail', true, 'x')",
                oid, source, Decimal(100 * offset), STAMP, DAY)
            if kind is not None:
                await conn.execute(
                    "INSERT INTO silver.order_utm (order_id, "
                    + ", ".join(UTM_VERDICT_COLUMNS) + ", parsed_at) VALUES ("
                    + ", ".join(f"${i}" for i in range(1, len(UTM_VERDICT_COLUMNS) + 3))
                    + ")",
                    oid, *_stored(kind, comment), STAMP + shift)
        yield conn
    finally:
        await _clean(conn)
        await conn.close()


def _ours(state):
    return [o for o in state.orders if BASE < o.order_id < BASE + 1000]


async def _session(dsn=DSN):
    """The script's own session, opened behind its role check."""
    return await script.connect(SimpleNamespace(dsn=dsn))


class TestTheFrontDoor:
    @pytest.mark.asyncio
    async def test_ks_app_is_seen_to_be_able_to_write(self):
        conn = await _session()
        try:
            found = await script.write_privileges(conn)
        finally:
            await conn.close()
        assert any(f.startswith("INSERT/UPDATE/DELETE/TRUNCATE on ") for f in found)
        assert "CREATE on schema bronze" in found

    @pytest.mark.asyncio
    async def test_ks_readonly_as_the_migrations_leave_it_holds_nothing(self):
        """The role the script is for, asked the script's own questions on
        the schema at head — from ks_app, because CI has no ks_readonly
        login, but the privilege functions answer for any role by name. What
        this pins is that no revision since 0001 has granted the read-only
        role a write (or a sequence, or a schema): the day one does, the
        script would refuse production's login, and this fails first."""
        conn = await _session()
        try:
            assert await script.write_privileges(conn, script.READONLY_ROLE) == []
            # And the questions are able to say yes: the same statements,
            # asked of the owner, find its tables, its sequences and its
            # schemas, so an empty answer above is not a query that finds
            # nothing for anybody.
            owner = await script.write_privileges(conn, "ks_app")
        finally:
            await conn.close()
        assert any(f.startswith("USAGE/UPDATE on sequence ") for f in owner), owner
        assert any(f.startswith("INSERT/UPDATE/DELETE/TRUNCATE on ") for f in owner), owner
        assert "CREATE on schema silver" in owner, owner

    @pytest.mark.asyncio
    async def test_the_roles_asked_about_are_every_role_the_login_can_become(self):
        """A NOINHERIT member of a writer holds nothing itself, so the door
        asks its questions of every role the login may SET ROLE to. Making
        such a member takes CREATEROLE, which CI's login does not have; a
        built-in role stands in. `pg_monitor` is a member of three others in
        every cluster since PostgreSQL 10, so the statement that lists "what
        this login can become" must name all four, itself first."""
        conn = await _session()
        try:
            roles = [r["name"] for r in await conn.fetch(script.ROLES_SQL, "pg_monitor")]
        finally:
            await conn.close()
        assert roles == ["pg_monitor", "pg_read_all_settings", "pg_read_all_stats",
                         "pg_stat_scan_tables"]

    @pytest.mark.asyncio
    async def test_a_noinherit_member_of_the_writer_is_refused_through_it(self):
        """The hole the membership walk closes, on a real one. A NOINHERIT
        member of ks_app holds no privilege itself — every privilege function
        says no for it — and holds all of ks_app's one `SET ROLE` later. Only
        MEMBER sees that membership; USAGE, which is what inheritance
        follows, does not, so a door asking USAGE would let it in."""
        conn = await _session()
        try:
            if not await conn.fetchval(
                    "SELECT 1 FROM pg_roles WHERE rolname = $1", NOINHERIT_ROLE):
                pytest.skip(f"{PROVISIONED}: role {NOINHERIT_ROLE}")
            # The premise, so a role provisioned wrongly fails loudly here
            # rather than passing for the wrong reason.
            assert not await conn.fetchval(
                "SELECT has_table_privilege($1, 'bronze.orders', 'INSERT')",
                NOINHERIT_ROLE)
            assert await conn.fetchval(
                "SELECT pg_has_role($1, 'ks_app', 'MEMBER') "
                "AND NOT pg_has_role($1, 'ks_app', 'USAGE')", NOINHERIT_ROLE)
            roles = [r["name"] for r in await conn.fetch(script.ROLES_SQL, NOINHERIT_ROLE)]
            found = await script.write_privileges(conn, NOINHERIT_ROLE)
        finally:
            await conn.close()
        assert roles == [NOINHERIT_ROLE, "ks_app"]
        through = f" (through ks_app, which {NOINHERIT_ROLE} can SET ROLE to)"
        assert any(f.endswith(through) and f.startswith("INSERT/UPDATE/DELETE/TRUNCATE on ")
                   for f in found), found
        # Nothing is found for the role itself, which is the whole trap.
        assert all(f.endswith(through) for f in found), found

    @pytest.mark.asyncio
    async def test_role_attributes_are_read_from_the_catalogue(self):
        """CREATEROLE and CREATEDB change the cluster without a table
        privilege. The bootstrap superuser (oid 10) holds both in every
        cluster, so it is the one role whose answer is known without making
        one; the owner's TEMPORARY is the database privilege."""
        conn = await _session()
        try:
            bootstrap = await conn.fetchval("SELECT rolname FROM pg_roles WHERE oid = 10")
            found = await script.write_privileges(conn, bootstrap)
            owner = await script.write_privileges(conn, "ks_app")
        finally:
            await conn.close()
        assert {f"{bootstrap} is a superuser", "CREATEROLE", "CREATEDB"} <= set(found)
        assert "CREATE or TEMPORARY on the database" in owner, owner

    # Each relation kind a write can pass through, each granted a different
    # one of the four table privileges, and both sequence privileges.
    PROBES = [
        ("CREATE TABLE {} (x int)", "GRANT DELETE ON {} TO ks_readonly",
         "INSERT/UPDATE/DELETE/TRUNCATE on"),
        ("CREATE TABLE {} (x int) PARTITION BY RANGE (x)",
         "GRANT TRUNCATE ON {} TO ks_readonly", "INSERT/UPDATE/DELETE/TRUNCATE on"),
        ("CREATE VIEW {} AS SELECT 1 AS x", "GRANT INSERT ON {} TO ks_readonly",
         "INSERT/UPDATE/DELETE/TRUNCATE on"),
        ("CREATE MATERIALIZED VIEW {} AS SELECT 1 AS x",
         "GRANT UPDATE ON {} TO ks_readonly", "INSERT/UPDATE/DELETE/TRUNCATE on"),
        ("CREATE SEQUENCE {}", "GRANT USAGE ON SEQUENCE {} TO ks_readonly",
         "USAGE/UPDATE on sequence"),
        ("CREATE SEQUENCE {}", "GRANT UPDATE ON SEQUENCE {} TO ks_readonly",
         "USAGE/UPDATE on sequence"),
    ]

    @pytest.mark.parametrize("create, grant, finding", PROBES,
                             ids=["table", "partitioned", "view", "matview",
                                  "sequence-usage", "sequence-update"])
    @pytest.mark.asyncio
    async def test_each_write_privilege_granted_to_the_read_only_role_is_named(
            self, create, grant, finding):
        """Granted for real, as the owner, inside a transaction that is
        rolled back — the privilege functions answer inside it. A foreign
        table, the one kind not here, needs a foreign-data wrapper only a
        superuser can install; the unit test holds the kind list whole."""
        name = "silver.dn15_probe"
        conn = await asyncpg.connect(DSN)
        probe = conn.transaction()
        await probe.start()
        try:
            await conn.execute(create.format(name))
            await conn.execute(grant.format(name))
            found = await script.write_privileges(conn, script.READONLY_ROLE)
        finally:
            await probe.rollback()
            await conn.close()
        assert found == [f"{finding} {name}"]

    def test_ks_app_is_refused_before_a_row_is_read(self, tmp_path, capsys):
        path = tmp_path / "s.csv.gz"
        assert script.main(["--dsn", DSN, "--snapshot", str(path)]) == 2
        assert "this login can write" in capsys.readouterr().err
        assert not path.exists()


class TestTheRead:
    @pytest.mark.asyncio
    async def test_the_session_is_read_only_from_its_first_statement(self):
        conn = await _session()
        try:
            assert await conn.fetchval("SHOW default_transaction_read_only") == "on"
            assert await conn.fetchval("SHOW TimeZone") == "UTC"
            with pytest.raises(asyncpg.exceptions.ReadOnlySQLTransactionError):
                await conn.execute(
                    "UPDATE bronze.orders SET promocode = 'x' WHERE id = $1", BASE + 1)
        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_every_seeded_order_is_filed_under_its_cause(self, seeded, tmp_path):
        conn = await _session()
        try:
            state = await script.read_state(conn, tmp_path / "s.csv.gz")
        finally:
            await conn.close()
        ours = _ours(state)
        assert [o.order_id - BASE for o in ours] == sorted(SEED)

        report = script.diff_verdicts(ours, WINDOW)
        assert dict(report.rewrites) == {
            "rule_change": 1, "orphaned": 1, "pending": 1, "unparsed": 1}
        # The unparsed Instagram order lands where the fallback already put
        # it, and the pending one changes a campaign and not a verdict, so
        # both are rewrites without a transition.
        moves = {(t.cause, t.before, t.after): t.orders for t in report.transitions}
        assert moves == {
            ("rule_change", ("pixel_only", "facebook"), ("pixel_only", "unattributed")): 1,
            ("orphaned", ("paid_confirmed", "facebook"), ("organic", "telegram")): 1,
        }
        (stale,) = [t for t in report.transitions if t.cause == "rule_change"]
        assert stale.window_orders == {"retail": 1, "all": 1}
        assert stale.window_revenue["retail"] == Decimal("100.00")
        assert report.column_changes["utm_campaign"] == 1   # the pending one

    @pytest.mark.asyncio
    async def test_the_fallback_is_what_postgres_says(self, seeded):
        """The Python spelling of the tab's COALESCE, held to the SQL as
        Postgres evaluates it on the seeded rows — the unit test holds it to
        DuckDB's."""
        rows = await seeded.fetch(
            f"SELECT s.id, {TrafficMixin._TRAFFIC_TYPE_EXPR} AS t, "
            f"       {TrafficMixin._PLATFORM_EXPR} AS p "
            "FROM silver.orders s LEFT JOIN silver.order_utm u ON u.order_id = s.id "
            "WHERE s.id > $1 AND s.id < $2", BASE, BASE + 1000)
        conn = await _session()
        try:
            state = await script.read_state(conn)
        finally:
            await conn.close()
        shown = {o.order_id: script.shown_verdict(o.stored, o.source_id) for o in _ours(state)}
        assert {r["id"]: (r["t"], r["p"]) for r in rows} == shown


class TestTheSnapshot:
    @pytest.mark.asyncio
    async def test_it_is_the_table_row_for_row(self, seeded, tmp_path):
        path = tmp_path / "s.csv.gz"
        conn = await _session()
        try:
            state = await script.read_state(conn, path)
        finally:
            await conn.close()
        with gzip.open(path, "rt", encoding="utf-8", newline="") as text:
            header, *rows = list(csv.reader(text))
        assert header[:len(UTM_VERDICT_COLUMNS) + 1] == ["order_id", *UTM_VERDICT_COLUMNS]
        assert "parsed_at" in header and "mirrored_at" in header
        assert len(rows) == state.utm_rows == state.snapshot.rows
        ours = {int(r[0]): r for r in rows if BASE < int(r[0]) < BASE + 1000}
        assert sorted(o - BASE for o in ours) == [1, 2, 4, 5, 6]
        platform = header.index("platform")
        assert ours[BASE + 1][platform] == "facebook"   # the old verdict, saved
        assert ours[BASE + 6][platform] == ""           # a NULL is an empty field
        parsed_at = header.index("parsed_at")
        assert ours[BASE + 1][parsed_at] == "2026-09-01 09:00:00+00"


async def _until_the_read_waits_on_a_lock(conn, timeout=8.0):
    """Until the script's session is queued behind a lock — well inside the
    session's own 10 s `lock_timeout`, after which it would give up."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if await conn.fetchval(
                "SELECT count(*) FROM pg_stat_activity "
                "WHERE application_name = $1 AND wait_event_type = 'Lock'",
                script.SESSION["application_name"]):
            return
        await asyncio.sleep(0.05)
    raise AssertionError("the read never queued behind the tick's lock")


class TestASilverTickDuringTheRead:
    """Every Silver tick TRUNCATEs `silver.orders` and `silver.order_utm` and
    refills them in one transaction. A TRUNCATE is not MVCC-safe: a read whose
    snapshot was fixed before the tick committed, and which then waited for
    it, sees the new file and reads the table as empty. Measured before the
    fix at production size: rule_change 0 where 2,043 were true, every 12-week
    table "no orders", exit 0 — and a 0-row snapshot "checked against the
    table in the same snapshot"."""

    @pytest.mark.parametrize("table", ["silver.order_utm", "silver.orders"])
    @pytest.mark.asyncio
    async def test_a_tick_committed_while_the_read_waits_is_read_after_it(
            self, seeded, table):
        conn = await _session()
        try:
            undisturbed = await script.read_state(conn)
        finally:
            await conn.close()

        # The tick's shape, as `rebuild_silver` and `ship_order_utm` write
        # it: TRUNCATE, then the rows again, in one transaction.
        writer = await asyncpg.connect(DSN)
        tick = writer.transaction()
        await tick.start()
        conn = await _session()
        read = None
        try:
            await writer.execute(
                f"CREATE TEMP TABLE dn15_tick ON COMMIT DROP AS SELECT * FROM {table}")
            await writer.execute(f"TRUNCATE {table}")
            await writer.execute(f"INSERT INTO {table} SELECT * FROM dn15_tick")
            read = asyncio.create_task(script.read_state(conn))
            await _until_the_read_waits_on_a_lock(seeded)
            committing = await seeded.fetchval("SELECT clock_timestamp()")
            await tick.commit()
            raced = await read
        finally:
            # Closing a connection with its transaction open rolls it back,
            # which is the cleanup for every way this can fail half-way.
            await writer.close()
            if read is not None and not read.done():
                read.cancel()
            await conn.close()

        assert _ours(raced) == _ours(undisturbed)
        assert (raced.utm_rows, raced.orphan_rows) == (
            undisturbed.utm_rows, undisturbed.orphan_rows)
        assert raced.utm_rows == await seeded.fetchval(
            "SELECT count(*) FROM silver.order_utm")
        # And the instant the report prints is the snapshot's, after the
        # tick — not BEGIN's, which was before the wait.
        assert raced.taken_at > committing


class _ReadWrite:
    """A connection whose transactions ignore `readonly=True` — the mutation
    the transaction-id check exists for, as a wrapper because asyncpg's
    connection does not let an attribute be replaced."""

    def __init__(self, conn):
        self._conn = conn

    def transaction(self, **options):
        return self._conn.transaction(**{**options, "readonly": False})

    def __getattr__(self, name):
        return getattr(self._conn, name)


class TestTheServerRefusesToBeWrittenThrough:
    """The script made to write, two ways. Nothing it tries survives."""

    UPDATE = ("UPDATE bronze.orders SET promocode = 'dryrun-wrote' "
              f"WHERE id = {BASE + 2} RETURNING 1")

    async def _promocode(self, conn):
        return await conn.fetchval("SELECT promocode FROM bronze.orders WHERE id = $1",
                                   BASE + 2)

    @pytest.mark.asyncio
    async def test_a_write_inside_the_read_is_refused(self, seeded, monkeypatch, tmp_path):
        monkeypatch.setattr(script, "ORPHAN_ROWS_SQL", self.UPDATE)
        conn = await _session()
        try:
            with pytest.raises(asyncpg.exceptions.ReadOnlySQLTransactionError):
                await script.read_state(conn, tmp_path / "s.csv.gz")
        finally:
            await conn.close()
        assert await self._promocode(seeded) is None
        assert not (tmp_path / "s.csv.gz").exists()

    @pytest.mark.asyncio
    async def test_without_the_declaration_the_transaction_id_rolls_it_back(
            self, seeded, monkeypatch, tmp_path):
        """Both read-only declarations removed — a plain session and a
        transaction opened read-write — so the swapped-in UPDATE succeeds, and
        the only thing left between it and the table is the question whether
        the server assigned a transaction id. It did; the run fails, the
        UPDATE is rolled back, and the snapshot taken in the same transaction
        is removed with it."""
        monkeypatch.setattr(script, "ORPHAN_ROWS_SQL", self.UPDATE)
        conn = await asyncpg.connect(DSN)
        path = tmp_path / "s.csv.gz"
        try:
            with pytest.raises(script.WroteSomething):
                await script.read_state(_ReadWrite(conn), path)
        finally:
            await conn.close()
        assert await self._promocode(seeded) is None
        assert not path.exists()


@pytest.mark.skipif(not READONLY_DSN,
                    reason=f"{PROVISIONED}: a ks_readonly login at KS_PG_READONLY_DSN")
class TestAsKsReadonlyUnderAudit:
    def test_the_whole_command_writes_only_the_snapshot(self, seeded, tmp_path):
        """The front door, as production runs it: `ks_readonly`, a real
        server, the real `main()`, in a fresh interpreter under the audit
        hook. One file written, and one kind of connection — to the database.

        Seeded, because the database is shared: an emptied Silver beside a
        Bronze other tests left behind would be refused, rightly, and this
        is about the door and the writes, not about their leftovers."""
        path = tmp_path / "order_utm.csv.gz"
        run = run_main_audited(
            "scripts.utm_reclassify_dryrun",
            ["--today", "2026-09-23", "--snapshot", str(path)],
            env={"KS_PG_READONLY_DSN": READONLY_DSN, "KS_PG_DSN": POISONED_PG_DSN},
            cwd=tmp_path)
        assert run.code == 0, run.stderr
        assert run.writes == [str(path)]
        assert run.mutations == [] and run.commands == []
        # Every connection is to the read-only DSN's port. `KS_PG_DSN` is
        # poisoned with another, because the real one names the same server
        # and port as the read-only login and a connection through it would
        # otherwise pass this check.
        target = urlsplit(READONLY_DSN)
        ports = {ast.literal_eval(c)[1] for c in run.connects}
        assert ports == {target.port}, run.connects
        assert not set(UNSEEN_WRITERS) & set(run.modules)
        assert "the server assigned it no transaction id" in run.stdout
