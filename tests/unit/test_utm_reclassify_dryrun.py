"""The reclassify dry run: what it computes, and that it writes nothing (DN-15).

`scripts/utm_reclassify_dryrun.py` runs against production to answer OD-06, so
the property that matters most is the one a docstring cannot prove: that it
changes nothing. These tests hold it from four sides —

- the environment names it reads, and the statements it can send, by walking
  its tree (it never names `KS_PG_DSN`, and it has no `execute`);
- the transaction it declares, and the refusal of a login that could write,
  against a recording connection;
- a whole run in a fresh interpreter under an audit hook, which fails on any
  file opened for writing other than the snapshot, any filesystem change, any
  network connection and any command;
- the server's own word, `tests/integration/test_utm_reclassify_dryrun_pg.py`,
  where a write inside the read is refused and one that got through is rolled
  back by the transaction-id check.

The diff itself is tested on hand-built orders with a stand-in parser, so a
test here says what the arithmetic does and not what today's rules are; the
rules are `test_utm_classify_golden.py`'s business.
"""
from __future__ import annotations

import ast
import csv
import gzip
import hashlib
import json
import re
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import utm_reclassify_dryrun as script
from scripts.utm_reclassify_dryrun import (
    CAUSES,
    Order,
    UTM_VERDICT_COLUMNS,
    diff_verdicts,
    fallback_verdict,
    report_window,
    shown_verdict,
)
from tests.write_audit import run_main_audited

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "utm_reclassify_dryrun.py"
NOW = datetime(2026, 9, 23, 8, 0, tzinfo=timezone.utc)
WINDOW = (date(2026, 6, 29), date(2026, 9, 20))
NULL_ROW = (None,) * len(UTM_VERDICT_COLUMNS)


def verdict_row(traffic_type, platform, campaign=None):
    """A stored or reparsed row carrying only what these tests look at."""
    row = [None] * len(UTM_VERDICT_COLUMNS)
    row[UTM_VERDICT_COLUMNS.index("utm_campaign")] = campaign
    row[UTM_VERDICT_COLUMNS.index("traffic_type")] = traffic_type
    row[UTM_VERDICT_COLUMNS.index("platform")] = platform
    return tuple(row)


def order(order_id, *, comment="c", stored=None, source_id=4, updated=None,
          parsed=None, day=date(2026, 9, 1), total="100.00", sales_type="retail",
          counted=True):
    stamp = NOW - timedelta(days=30)
    return Order(
        order_id=order_id, source_id=source_id, comment=comment,
        updated_at=updated or stamp, stored=stored, parsed_at=parsed or stamp,
        order_date=day, grand_total=Decimal(total), sales_type=sales_type,
        counted=counted,
    )


# ─── the window ───────────────────────────────────────────────────────────────

class TestTheWindow:
    @pytest.mark.parametrize("today", [date(2026, 9, 21), date(2026, 9, 23),
                                       date(2026, 9, 26)])
    def test_twelve_complete_weeks_before_today(self, today):
        start, end = report_window(today)
        assert (start, end) == WINDOW
        assert start.weekday() == 0 and end.weekday() == 6
        assert (end - start).days + 1 == 12 * 7

    def test_sunday_is_not_yet_a_complete_week(self):
        """The Monday message's own rule: a week is complete once its Sunday
        has ended, so on that Sunday it is the week before that counts."""
        assert report_window(date(2026, 9, 20)) == (date(2026, 6, 22), date(2026, 9, 13))


# ─── the fallback is the tab's ────────────────────────────────────────────────

class TestTheFallbackIsTheTabs:
    """`fallback_verdict` is a Python spelling of two SQL expressions on the
    mixin. Held to them by evaluating the SQL, not by reading it."""

    @staticmethod
    def _sql(source_id, traffic_type=None, platform=None):
        import duckdb

        from core.repositories.traffic import TrafficMixin

        row = duckdb.connect().execute(
            f"SELECT {TrafficMixin._TRAFFIC_TYPE_EXPR}, {TrafficMixin._PLATFORM_EXPR} "
            "FROM (SELECT CAST(? AS VARCHAR) AS traffic_type, "
            "             CAST(? AS VARCHAR) AS platform) u, "
            "     (SELECT CAST(? AS INTEGER) AS source_id) s",
            [traffic_type, platform, source_id],
        ).fetchone()
        return tuple(row)

    @pytest.mark.parametrize("source_id", [1, 2, 3, 4, 5, None])
    def test_no_row_is_placed_where_the_tab_places_it(self, source_id):
        assert fallback_verdict(source_id) == self._sql(source_id)
        assert shown_verdict(None, source_id) == self._sql(source_id)
        assert shown_verdict(NULL_ROW, source_id) == self._sql(source_id)

    @pytest.mark.parametrize("traffic_type, platform", [
        ("paid_confirmed", None), (None, "facebook"), ("organic", "email"),
    ])
    @pytest.mark.parametrize("source_id", [1, 4])
    def test_each_column_falls_back_on_its_own(self, source_id, traffic_type, platform):
        assert shown_verdict(verdict_row(traffic_type, platform), source_id) \
            == self._sql(source_id, traffic_type, platform)


# ─── the diff ─────────────────────────────────────────────────────────────────

def _fake_parse(table):
    return lambda comment: table[comment]


class TestTheDiff:
    def test_an_unchanged_verdict_is_not_a_transition(self):
        row = verdict_row("paid_confirmed", "facebook", "spring")
        report = diff_verdicts([order(1, comment="a", stored=row)], WINDOW,
                               reclassify=_fake_parse({"a": row}))
        assert report.transitions == [] and sum(report.rewrites.values()) == 0

    def test_a_rule_change_is_filed_with_its_money(self):
        old = verdict_row("pixel_only", "facebook")
        new = verdict_row("pixel_only", "unattributed")
        orders = [
            order(1, comment="p", stored=old, total="250.50"),
            order(2, comment="p", stored=old, total="100.00", sales_type="b2b"),
            order(3, comment="p", stored=old, day=date(2026, 6, 28)),   # before the window
            order(4, comment="p", stored=old, counted=False),           # a return
        ]
        report = diff_verdicts(orders, WINDOW, reclassify=_fake_parse({"p": new}))

        (t,) = report.transitions
        assert (t.cause, t.before, t.after) == (
            "rule_change", ("pixel_only", "facebook"), ("pixel_only", "unattributed"))
        assert t.orders == 4
        assert t.window_orders == {"retail": 1, "all": 2}
        assert t.window_revenue == {"retail": Decimal("250.50"), "all": Decimal("350.50")}
        assert report.rewrites["rule_change"] == 4
        assert report.column_changes == {"platform": 4}

    def test_a_row_changed_since_it_was_parsed_is_pending_not_a_rule_change(self):
        """The next tick reparses it whatever anybody decides, so it must not
        swell the number OD-06 is about."""
        parsed = NOW - timedelta(days=3)
        old = verdict_row("paid_confirmed", "facebook")
        new = verdict_row("paid_confirmed", "tiktok")
        orders = [
            order(1, comment="x", stored=old, parsed=parsed, updated=parsed + timedelta(seconds=1)),
            order(2, comment="x", stored=old, parsed=parsed, updated=parsed),
        ]
        report = diff_verdicts(orders, WINDOW, reclassify=_fake_parse({"x": new}))
        assert report.rewrites == {"pending": 1, "rule_change": 1}
        assert [t.cause for t in report.transitions] == ["rule_change", "pending"]

    def test_a_comment_with_no_row_is_unparsed_and_starts_from_the_fallback(self):
        new = verdict_row("paid_confirmed", "google")
        report = diff_verdicts([order(1, comment="g", source_id=1)], WINDOW,
                               reclassify=_fake_parse({"g": new}))
        (t,) = report.transitions
        assert (t.cause, t.before, t.after) == (
            "unparsed", ("organic", "instagram"), ("paid_confirmed", "google"))

    @pytest.mark.parametrize("comment", [None, ""])
    def test_a_row_whose_comment_is_gone_is_orphaned(self, comment):
        """The reclassify DELETEs it and the parse never puts it back, because
        the parse selects only non-empty comments."""
        old = verdict_row("paid_confirmed", "facebook")
        report = diff_verdicts([order(1, comment=comment, stored=old, source_id=2)],
                               WINDOW, reclassify=_fake_parse({}))
        (t,) = report.transitions
        assert (t.cause, t.before, t.after) == (
            "orphaned", ("paid_confirmed", "facebook"), ("organic", "telegram"))
        assert report.column_changes == {"traffic_type": 1, "platform": 1}

    def test_a_rewrite_that_moves_no_verdict_is_counted_but_not_listed(self):
        old = verdict_row("paid_confirmed", "facebook", "spring")
        new = verdict_row("paid_confirmed", "facebook", "autumn")
        report = diff_verdicts([order(1, comment="c", stored=old)], WINDOW,
                               reclassify=_fake_parse({"c": new}))
        assert report.rewrites == {"rule_change": 1}
        assert report.verdict_moves == {}
        assert report.transitions == []
        assert report.column_changes == {"utm_campaign": 1}

    def test_a_null_row_and_no_data_are_the_same_answer(self):
        """A comment with no tracking data is stored as NULLs; reparsed, it is
        NULLs again, and the tab shows the fallback both times."""
        report = diff_verdicts([order(1, comment="n", stored=NULL_ROW, source_id=1)],
                               WINDOW, reclassify=_fake_parse({"n": NULL_ROW}))
        assert report.transitions == [] and sum(report.rewrites.values()) == 0

    def test_the_platform_table_covers_every_counted_order(self):
        """Including those the reclassify does not touch, or "facebook: 600 →
        540" would read as the whole chart when it is only the part that moves."""
        old = verdict_row("pixel_only", "facebook")
        new = verdict_row("pixel_only", "unattributed")
        orders = [
            order(1, comment="p", stored=old, total="10.00"),
            order(2, comment=None, source_id=1, total="5.00"),              # fallback only
            order(3, comment="p", stored=old, total="7.00", sales_type="internal"),
        ]
        report = diff_verdicts(orders, WINDOW, reclassify=_fake_parse({"p": new}))
        assert report.platforms["retail"] == {
            "facebook": [1, Decimal("10.00"), 0, Decimal("0")],
            "unattributed": [0, Decimal("0"), 1, Decimal("10.00")],
            "instagram": [1, Decimal("5.00"), 1, Decimal("5.00")],
        }
        assert report.platforms["all"]["facebook"] == [2, Decimal("17.00"), 0, Decimal("0")]
        assert report.traffic_types["retail"]["organic"] == [1, Decimal("5.00"), 1, Decimal("5.00")]

    def test_causes_come_out_in_order_and_largest_first(self):
        a = verdict_row("organic", "instagram")
        b = verdict_row("organic", "tiktok")
        c = verdict_row("organic", "google")
        orders = [order(1, comment="c", stored=a), order(2, comment="b", stored=a),
                  order(3, comment="b", stored=a), order(4, comment="c")]
        report = diff_verdicts(orders, WINDOW, reclassify=_fake_parse({"b": b, "c": c}))
        assert [(t.cause, t.after[1], t.orders) for t in report.transitions] == [
            ("rule_change", "tiktok", 2), ("rule_change", "google", 1),
            ("unparsed", "google", 1)]
        assert list(CAUSES) == ["rule_change", "orphaned", "pending", "unparsed"]

    def test_the_default_parser_is_the_parse(self):
        """Without a stand-in the diff runs `utm_columns` — the row the DuckDB
        parse writes — so the dry run cannot drift from the thing it predicts."""
        import inspect

        default = inspect.signature(diff_verdicts).parameters["reclassify"].default
        from core.utm_classify import utm_columns
        assert default is utm_columns


# ─── a recording connection ──────────────────────────────────────────────────

class FakeRecord(dict):
    """What asyncpg hands back, as far as the script uses it: lookup by name."""


def fake_records():
    """Four orders, one of each cause, and one that moves nothing."""
    stamp = NOW - timedelta(days=30)
    empty = {c: None for c in UTM_VERDICT_COLUMNS}

    def rec(order_id, comment, stored=None, **extra):
        values = dict(empty)
        if stored:
            values.update(stored)
        base = dict(
            id=order_id, source_id=4, manager_comment=comment, updated_at=stamp,
            has_row=stored is not None, parsed_at=stamp if stored else None,
            order_date=date(2026, 9, 1), grand_total=Decimal("100.00"),
            sales_type="retail", counted=True,
        )
        base.update(values)
        base.update(extra)
        return FakeRecord(base)

    return [
        rec(1, "_fbp: fb.1.2", {"fbp": "fb.1.2", "traffic_type": "pixel_only",
                                "platform": "facebook"}),
        rec(2, "UTM: utm_source: fbads", {"utm_source": "fbads",
                                          "traffic_type": "paid_confirmed",
                                          "platform": "facebook"}),
        rec(3, "UTM: utm_source: ig", None),
        rec(4, "", {"utm_source": "fbads", "traffic_type": "paid_confirmed",
                    "platform": "facebook"}),
    ]


def snapshot_csv(n_rows):
    lines = ["order_id,platform"] + [f"{i},facebook" for i in range(1, n_rows + 1)]
    return ("\n".join(lines) + "\n").encode()


class FakeConn:
    """Answers the script's reads and records what it was asked."""

    def __init__(self, records=None, *, utm_rows=3, csv_rows=None, xid=None,
                 privileges=None, copy_error=None):
        self.records = fake_records() if records is None else records
        self.utm_rows = utm_rows
        self.csv_rows = utm_rows if csv_rows is None else csv_rows
        self.xid = xid
        self.privileges = privileges or {}
        self.copy_error = copy_error
        self.statements = []
        self.transactions = []
        self.closed = False

    def _log(self, sql):
        self.statements.append(sql)

    async def fetchrow(self, sql, *args):
        self._log(sql)
        assert sql == script.ROLE_SQL, sql
        return FakeRecord(role="ks_readonly",
                          superuser=self.privileges.get("superuser", False),
                          db_write=self.privileges.get("db_write", False))

    async def fetch(self, sql, *args):
        self._log(sql)
        if sql == script.ORDERS_SQL:
            return self.records
        if sql == script.WRITABLE_TABLES_SQL:
            return [FakeRecord(name=n) for n in self.privileges.get("tables", [])]
        if sql == script.CREATABLE_SCHEMAS_SQL:
            return [FakeRecord(name=n) for n in self.privileges.get("schemas", [])]
        raise AssertionError(f"unexpected fetch: {sql}")

    async def fetchval(self, sql, *args):
        self._log(sql)
        answers = {
            script.NOW_SQL: NOW,
            "SELECT current_user": "ks_readonly",
            script.UTM_ROWS_SQL: self.utm_rows,
            script.ORPHAN_ROWS_SQL: 0,
            script.XID_SQL: self.xid,
        }
        assert sql in answers, f"unexpected fetchval: {sql}"
        return answers[sql]

    async def copy_from_query(self, sql, *, output, format, header):
        self._log(sql)
        assert (format, header) == ("csv", True)
        data = snapshot_csv(self.csv_rows)
        await output(data[: len(data) // 2])
        if self.copy_error:
            raise self.copy_error
        await output(data[len(data) // 2:])

    def transaction(self, **options):
        self.transactions.append(options)

        @asynccontextmanager
        async def _tx():
            yield

        return _tx()

    async def close(self):
        self.closed = True


def _args(**overrides):
    base = dict(dsn=None, snapshot=None, today=date(2026, 9, 23), json=False)
    base.update(overrides)
    return SimpleNamespace(**base)


def _run_main(monkeypatch, conn, argv):
    async def fake_connect(args):
        return conn

    monkeypatch.setattr(script, "connect", fake_connect)
    return script.main(argv)


# ─── nothing is written ───────────────────────────────────────────────────────

def _tree():
    return ast.parse(SCRIPT.read_text(encoding="utf-8"))


def _module_constants(tree):
    return {
        t.id: node.value.value
        for node in tree.body if isinstance(node, ast.Assign)
        for t in node.targets
        if isinstance(t, ast.Name) and isinstance(node.value, ast.Constant)
    }


class TestNothingIsWritten:
    def test_it_reads_only_the_read_only_names_from_the_environment(self):
        """Walked, not grepped: the docstring names `KS_PG_DSN` to say it is
        never read, and a text search cannot tell the two apart. Every access
        to `os.environ` must be a `.get` of a name that resolves here."""
        tree = _tree()
        constants = _module_constants(tree)
        names, environ, read_by_get = set(), [], set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and node.attr == "environ":
                environ.append(node)
            if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                    and node.func.attr in ("get", "getenv")):
                target = node.func.value
                is_env = (isinstance(target, ast.Attribute) and target.attr == "environ") \
                    or (isinstance(target, ast.Name) and target.id == "os"
                        and node.func.attr == "getenv")
                if not is_env:
                    continue
                arg = node.args[0]
                names.add(constants.get(arg.id) if isinstance(arg, ast.Name) else arg.value)
                read_by_get.add(id(target))
        stray = [n.lineno for n in environ if id(n) not in read_by_get]
        assert stray == [], f"os.environ used other than by .get() at lines {stray}"
        assert names == {"KS_PG_READONLY_DSN", "KS_READONLY_PASSWORD"}

    def test_it_has_no_way_to_execute_a_statement(self):
        """The connection methods it calls are the four that read and the two
        that frame a read. No `execute`, `executemany` or `copy_to_table`, and
        every statement is a module constant ending `_SQL` or a SELECT
        literal — so the list below is the whole of what it can send."""
        allowed = {"fetch", "fetchval", "fetchrow", "copy_from_query",
                   "transaction", "close"}
        tree = _tree()
        used, sent = set(), []
        for node in ast.walk(tree):
            if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                    and isinstance(node.func.value, ast.Name)
                    and node.func.value.id == "conn"):
                used.add(node.func.attr)
                if node.args:
                    sent.append(node.args[0])
        assert used <= allowed, sorted(used - allowed)
        for arg in sent:
            if isinstance(arg, ast.Name):
                assert arg.id.endswith("_SQL"), arg.id
            else:
                assert isinstance(arg, ast.Constant) and arg.value.startswith("SELECT"), \
                    ast.unparse(arg)

    def test_every_statement_it_holds_is_a_read(self):
        verbs = ("insert", "update", "delete", "truncate", "alter", "create",
                 "drop", "grant", "revoke", "merge", "lock", "nextval", "setval",
                 "set_config", "vacuum", "cluster", "reindex", "comment", "call",
                 "do", "copy")
        statements = {n: getattr(script, n) for n in dir(script) if n.endswith("_SQL")}
        assert len(statements) >= 8
        for name, sql in statements.items():
            code = sql.strip().lower()
            assert code.startswith("select"), name
            words = set(re.findall(r"[a-z_]+", re.sub(r"'[^']*'", "''", code)))
            assert not words & set(verbs), (name, sorted(words & set(verbs)))

    @pytest.mark.asyncio
    async def test_the_read_is_one_declared_read_only_transaction(self, tmp_path):
        conn = FakeConn()
        state = await script.read_state(conn, tmp_path / "s.csv.gz")
        assert conn.transactions == [{"isolation": "repeatable_read", "readonly": True}]
        # The transaction-id question is the last thing asked inside it.
        assert conn.statements[-1] == script.XID_SQL
        assert script.SESSION["default_transaction_read_only"] == "on"
        assert state.snapshot is not None and state.snapshot.rows == 3

    @pytest.mark.asyncio
    async def test_a_transaction_that_was_given_an_id_fails_and_keeps_no_snapshot(
            self, tmp_path):
        """What the server's answer means: an id is assigned on the first
        write. The run fails, and a snapshot from it is not left looking good."""
        path = tmp_path / "s.csv.gz"
        with pytest.raises(script.WroteSomething):
            await script.read_state(FakeConn(xid="7331"), path)
        assert not path.exists()

    def test_a_whole_run_writes_only_the_snapshot_and_reaches_nothing(self, tmp_path):
        """The script's `main()` in a fresh interpreter under an audit hook,
        its connection a recording fake: one file opened for writing, the
        snapshot, and not one network connection, filesystem change or
        command."""
        path = tmp_path / "order_utm.csv.gz"
        setup = (
            "from tests.unit.test_utm_reclassify_dryrun import FakeConn\n"
            "async def _connect(args):\n"
            "    return FakeConn()\n"
            "script.connect = _connect\n"
        )
        run = run_main_audited(
            "scripts.utm_reclassify_dryrun",
            ["--today", "2026-09-23", "--snapshot", str(path)],
            setup=setup, cwd=tmp_path)
        assert run.code == 0, run.stderr
        assert run.writes == [str(path)]
        assert run.mutations == [] and run.connects == [] and run.commands == []
        assert path.exists() and "rule_change" in run.stdout

        bare = run_main_audited(
            "scripts.utm_reclassify_dryrun", ["--today", "2026-09-23", "--json"],
            setup=setup, cwd=tmp_path)
        assert bare.code == 0, bare.stderr
        assert bare.events == []
        assert json.loads(bare.stdout)["wrote_nothing"] is True


# ─── the role ─────────────────────────────────────────────────────────────────

class TestTheRole:
    @pytest.mark.asyncio
    async def test_each_kind_of_write_privilege_is_named(self):
        found = await script.write_privileges(FakeConn(privileges={
            "superuser": True, "db_write": True, "schemas": ["bronze"],
            "tables": ["silver.order_utm"]}))
        assert found == [
            "ks_readonly is a superuser",
            "CREATE or TEMPORARY on the database",
            "CREATE on schema bronze",
            "INSERT/UPDATE/DELETE/TRUNCATE on silver.order_utm",
        ]
        assert await script.write_privileges(FakeConn()) == []

    def test_a_login_that_can_write_is_refused_before_a_row_is_read(
            self, monkeypatch, tmp_path, capsys):
        conn = FakeConn(privileges={"tables": ["bronze.orders"]})
        path = tmp_path / "s.csv.gz"
        code = _run_main(monkeypatch, conn, ["--snapshot", str(path)])
        assert code == 2
        assert "bronze.orders" in capsys.readouterr().err
        assert script.ORDERS_SQL not in conn.statements
        assert conn.transactions == [] and conn.closed
        assert not path.exists()

    def test_no_login_named_is_a_refusal(self, monkeypatch, capsys):
        monkeypatch.delenv(script.DSN_ENV, raising=False)
        monkeypatch.delenv(script.PASSWORD_ENV, raising=False)
        monkeypatch.setenv("KS_PG_DSN", "postgresql://ks_app:x@127.0.0.1:1/ks")
        assert script.main([]) == 2
        assert "no read-only login" in capsys.readouterr().err


# ─── the snapshot ─────────────────────────────────────────────────────────────

class TestTheSnapshot:
    def test_an_existing_file_is_refused_before_connecting(self, monkeypatch, tmp_path):
        path = tmp_path / "earlier.csv.gz"
        path.write_bytes(b"the only record of last month's verdicts")
        conn = FakeConn()
        assert _run_main(monkeypatch, conn, ["--snapshot", str(path)]) == 2
        assert conn.statements == []
        assert path.read_bytes() == b"the only record of last month's verdicts"

    def test_a_missing_directory_is_refused(self, monkeypatch, tmp_path):
        conn = FakeConn()
        assert _run_main(monkeypatch, conn,
                         ["--snapshot", str(tmp_path / "nope" / "s.csv.gz")]) == 2
        assert conn.statements == []

    @pytest.mark.asyncio
    async def test_a_file_that_appears_meanwhile_is_neither_overwritten_nor_removed(
            self, tmp_path):
        """The pre-check and the open are two moments; exclusive create is what
        holds between them, and the cleanup must not delete what it did not
        create."""
        path = tmp_path / "s.csv.gz"
        path.write_bytes(b"somebody else's")
        with pytest.raises(FileExistsError):
            await script.write_snapshot(FakeConn(), path, 3)
        assert path.read_bytes() == b"somebody else's"

    @pytest.mark.asyncio
    async def test_a_count_that_disagrees_removes_the_file(self, tmp_path):
        path = tmp_path / "s.csv.gz"
        with pytest.raises(script.SnapshotMismatch):
            await script.write_snapshot(FakeConn(csv_rows=2), path, 3)
        assert not path.exists()

    @pytest.mark.asyncio
    async def test_a_copy_that_fails_halfway_removes_the_file(self, tmp_path):
        path = tmp_path / "s.csv.gz"
        with pytest.raises(ConnectionResetError):
            await script.read_state(FakeConn(copy_error=ConnectionResetError("gone")), path)
        assert not path.exists()

    @pytest.mark.asyncio
    async def test_the_same_rows_give_the_same_bytes(self, tmp_path):
        """No name and no time in the gzip header, so a sha256 identifies the
        table's contents rather than the moment of the copy."""
        one = await script.write_snapshot(FakeConn(), tmp_path / "a.csv.gz", 3)
        two = await script.write_snapshot(FakeConn(), tmp_path / "b.csv.gz", 3)
        assert one.sha256 == two.sha256
        # Read from the header itself, because two copies made within the same
        # second would agree even with a clock in it (RFC 1952: bytes 4-7 are
        # MTIME; FLG bit 3 says a file name follows).
        header = (tmp_path / "a.csv.gz").read_bytes()[:10]
        assert header[4:8] == b"\x00\x00\x00\x00" and not header[3] & 0x08
        assert one.sha256 == hashlib.sha256((tmp_path / "a.csv.gz").read_bytes()).hexdigest()
        with gzip.open(tmp_path / "a.csv.gz", "rt", newline="") as text:
            assert list(csv.reader(text))[0] == ["order_id", "platform"]

    def test_the_printed_line_is_what_sha256sum_reads(self, monkeypatch, tmp_path, capsys):
        path = tmp_path / "s.csv.gz"
        assert _run_main(monkeypatch, FakeConn(), ["--today", "2026-09-23",
                                                   "--snapshot", str(path)]) == 0
        last = capsys.readouterr().out.rstrip().splitlines()[-1]
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        assert last == f"{digest}  {path.name}"


# ─── the report ───────────────────────────────────────────────────────────────

class TestTheReport:
    def test_the_four_fake_orders_land_where_they_should(self, monkeypatch, capsys):
        assert _run_main(monkeypatch, FakeConn(), ["--today", "2026-09-23", "--json"]) == 0
        out = json.loads(capsys.readouterr().out)
        assert out["rewrites"] == {"rule_change": 1, "orphaned": 1, "pending": 0,
                                   "unparsed": 1}
        assert out["window"] == ["2026-06-29", "2026-09-20"]
        moves = {(t["cause"], tuple(t["before"]), tuple(t["after"])) for t in out["transitions"]}
        assert moves == {
            ("rule_change", ("pixel_only", "facebook"), ("pixel_only", "unattributed")),
            ("orphaned", ("paid_confirmed", "facebook"), ("unknown", "unattributed")),
            ("unparsed", ("unknown", "unattributed"), ("organic", "instagram")),
        }
        assert out["platforms"]["retail"]["facebook"] == {
            "orders_now": 3, "revenue_now": "300.00",
            "orders_after": 1, "revenue_after": "100.00"}

    def test_the_text_report_names_every_cause(self, monkeypatch, capsys):
        assert _run_main(monkeypatch, FakeConn(), ["--today", "2026-09-23"]) == 0
        text = capsys.readouterr().out
        for cause in CAUSES:
            assert cause in text
        assert "nothing was written" in text
