"""The channel-verdict log's pure half (revision 0036, `core/pg_utm_parse.py`).

Everything a server decides — the diff, the self-check, what commits — is in
`tests/integration/test_capture_verdicts_pg.py`. What is here:

- `utm_classify.RULE_VERSION` moves with the golden: each version is pinned to
  a hash of the frozen tuples it was written against;
- the capture runs after the parse's transaction has closed, on the parse's
  connection, never inside it, and only after a parse that wrote;
- a full parse's changes are `reclassify`, an incremental's `change`;
- the row hash is the design's (§3.4), computable outside Postgres.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import re
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

import pytest

from core import pg_utm_parse as parse
from core import utm_classify
from tests.unit.test_utm_classify_golden import CLASSIFY_GOLDEN, PARSE_GOLDEN

# ─── The rules' version, pinned to the golden ─────────────────────────────────
#
# Append-only: one entry per version, never edited. A rule change edits a frozen
# tuple in `test_utm_classify_golden.py` (OD-06 makes that the place it is
# argued), and then `test_the_version_names_the_golden` fails until
# `RULE_VERSION` names a new version and its hash is added here. Editing an
# existing entry instead would let two sets of rules write one `rule_version`
# into `app.order_channel_verdicts`, which is the thing the column exists to
# prevent. A fixture added for a rule that did not change moves the hash too;
# that is the one case for replacing the current entry, and its commit says so.
GOLDEN_SHA256 = {
    "r1": "59ed4e1c8b7ea919bea114f1b9ac382fc98c07b5081328486a56809ab280a843",
}


def _golden_sha256() -> str:
    """The frozen verdicts — every (comment, parse, row) and (tags, verdict) —
    as canonical JSON: keys sorted, so a dict written in another order is the
    same golden."""
    text = json.dumps({"parse": PARSE_GOLDEN, "classify": CLASSIFY_GOLDEN},
                      ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class TestTheRuleVersion:
    def test_the_version_names_the_golden(self):
        current = _golden_sha256()
        assert GOLDEN_SHA256.get(utm_classify.RULE_VERSION) == current, (
            "the golden's verdicts moved: a rule change names a new "
            "utm_classify.RULE_VERSION and adds "
            f"{{'r<n+1>': '{current}'}} to GOLDEN_SHA256 in the same commit")

    def test_the_current_version_is_the_newest(self):
        """Kills: a rule change that went back to an old name, so that rows of
        two rule sets read as one."""
        assert all(re.fullmatch(r"r[1-9][0-9]*", v) for v in GOLDEN_SHA256)
        newest = max(GOLDEN_SHA256, key=lambda v: int(v[1:]))
        assert utm_classify.RULE_VERSION == newest

    def test_no_two_versions_share_a_golden(self):
        """Kills: a new name for rules that did not change, which splits one
        rule set's verdicts across two `rule_version`s."""
        assert len(set(GOLDEN_SHA256.values())) == len(GOLDEN_SHA256)

    def test_the_log_writes_this_version(self):
        assert parse.RULE_VERSION is utm_classify.RULE_VERSION


# ─── Where the capture runs ───────────────────────────────────────────────────

class _FakeConn:
    """Enough of asyncpg for the parse to run on nothing: every read empty,
    every transaction counted, so a capture can say where it ran."""

    def __init__(self, events):
        self.depth = 0
        self.events = events

    def is_in_transaction(self):
        return self.depth > 0

    @asynccontextmanager
    async def transaction(self):
        self.depth += 1
        self.events.append("parse: begin")
        try:
            yield
        finally:
            self.depth -= 1
            self.events.append("parse: end")

    async def execute(self, *_a):
        return "SELECT 1"

    async def executemany(self, *_a):
        return None

    async def fetch(self, *_a):
        return []

    async def fetchval(self, *_a):
        return 0


class _FakePool:
    def __init__(self, conn):
        self.conn = conn

    @asynccontextmanager
    async def acquire(self, **_kw):
        yield self.conn


def _run_with_spy(coro_factory, *, kept=True):
    """Run a parse on a fake connection, with `run_own_transaction` replaced by
    a spy that records the kind it would capture and whether a transaction was
    open on the connection when it was called. `kept` is what the spy says
    the capture did."""
    events: list = []
    conn = _FakeConn(events)
    seen: list = []

    async def spy(table, capture, *, conn=None, **_kw):
        events.append("capture")
        recorder = AsyncMock()
        # What the capture would execute: its kind is the third argument.
        recorder.execute = AsyncMock(return_value="INSERT 0 0")
        recorder.fetch = AsyncMock(return_value=[])
        recorder.fetchrow = AsyncMock(return_value={
            "differing": 0, "differing_ids": None, "mishashed": 0, "mishashed_ids": None})
        recorder.fetchval = AsyncMock(return_value=None)
        await capture(recorder)
        insert = [c for c in recorder.execute.await_args_list
                  if c.args and c.args[0] == parse.VERDICT_INSERT_SQL]
        seen.append({"table": table, "conn": conn,
                     "in_transaction": conn.is_in_transaction(),
                     "kind": insert[0].args[3] if insert else None})
        return kept

    async def run():
        with patch("core.pg.get_pool", new=AsyncMock(return_value=_FakePool(conn))), \
             patch("core.pg.require_revision", new=AsyncMock()), \
             patch("core.pg_landing._record_failure", new=AsyncMock()), \
             patch("core.analytics_captures.run_own_transaction", new=spy), \
             patch("core.pg_silver.PG_LAYER_LOCK", asyncio.Lock()):
            try:
                return await coro_factory()
            except Exception as exc:  # noqa: BLE001 — the test reads it
                return exc

    result = asyncio.run(run())
    return result, events, seen, conn


class TestWhereTheCaptureRuns:
    def test_after_the_incremental_has_committed_on_its_connection(self):
        result, events, seen, conn = _run_with_spy(lambda: parse.parse_incremental())
        assert result["parsed"] == 0
        assert events == ["parse: begin", "parse: end", "capture"]
        assert seen == [{"table": "app.order_channel_verdicts", "conn": conn,
                         "in_transaction": False, "kind": parse.CHANGE}]

    def test_the_full_parse_logs_what_it_replaces_first_and_its_own_after(self):
        """D12: the verdicts a reclassify overwrites are in the log before it
        overwrites them — an incremental capture before the parse's
        transaction — and what it moved is `reclassify` after it. Kills: the
        capture after the replacement only, which loses every verdict it
        overwrote that no capture had kept yet (the seed's, above all)."""
        result, events, seen, conn = _run_with_spy(lambda: parse.parse_full())
        assert result["rows"] == 0 and result["verdicts_logged"] is True
        assert events == ["capture", "parse: begin", "parse: end", "capture"]
        assert seen == [
            {"table": "app.order_channel_verdicts", "conn": conn,
             "in_transaction": False, "kind": kind}
            for kind in (parse.CHANGE, parse.RECLASSIFY)]

    def test_a_full_parse_whose_verdicts_were_not_logged_is_refused(self):
        """The switch off or the capture failing: D12 says the log lands
        before any reclassify, so nothing is replaced. Kills: a reclassify
        that runs, reports success and leaves the overwritten verdicts in no
        log."""
        result, events, seen, _conn = _run_with_spy(lambda: parse.parse_full(), kept=False)
        assert result["refused"] == parse.UNLOGGED_REFUSAL
        assert events == ["capture", "parse: begin", "parse: end"]
        assert [s["kind"] for s in seen] == [parse.CHANGE]

    def test_forced_it_runs_and_says_the_log_did_not_keep_it(self):
        result, events, _seen, _conn = _run_with_spy(
            lambda: parse.parse_full(force=True), kept=False)
        assert "refused" not in result and result["forced"] is True
        assert result["verdicts_logged"] is False
        assert events == ["capture", "parse: begin", "parse: end", "capture"]

    def test_a_refused_full_parse_captures_nothing_after(self):
        """It replaced nothing, so there is nothing new to log."""
        with patch.object(parse, "refusal", return_value="refused: test"):
            result, events, seen, _conn = _run_with_spy(lambda: parse.parse_full())
        assert "refused" in result
        assert events == ["capture", "parse: begin", "parse: end"]
        assert [s["kind"] for s in seen] == [parse.CHANGE]

    @pytest.mark.parametrize("which", ["incremental", "full"])
    def test_a_parse_that_raises_captures_nothing_after(self, which):
        """A failed parse left the table as it was; the next run's capture
        diffs whatever the next parse commits. The full parse's capture
        before it ran, as it always does."""
        run = parse.parse_incremental if which == "incremental" else parse.parse_full
        with patch.object(parse, "parse_rows", side_effect=RuntimeError("bad comment")):
            result, events, seen, _conn = _run_with_spy(lambda: run())
        assert isinstance(result, RuntimeError)
        before = ["capture"] if which == "full" else []
        assert events == before + ["parse: begin", "parse: end"]
        assert [s["kind"] for s in seen] == [parse.CHANGE] * len(before)

    def test_the_capture_is_run_own_transaction_with_the_table(self):
        """The table the helper is told is the one the INSERT names, and
        `CAPTURES` names this module its writer."""
        from core.analytics_captures import CAPTURES, OWN_TRANSACTION

        spec = CAPTURES[parse.VERDICTS_TABLE]
        assert spec.writer == parse.__name__ and spec.kind == OWN_TRANSACTION
        assert spec.self_checked
        assert f"INSERT INTO {parse.VERDICTS_TABLE}" in parse.VERDICT_INSERT_SQL


class TestTheKinds:
    def test_the_three_kinds_are_the_tables(self):
        """Revision 0036 comments the column `first | change | reclassify`."""
        assert (parse.FIRST, parse.CHANGE, parse.RECLASSIFY) == (
            "first", "change", "reclassify")
        assert f"THEN '{parse.FIRST}'" in parse.VERDICT_INSERT_SQL


class TestTheEarlierRules:
    """A `first` verdict the current rules do not make of its comment was
    decided under earlier ones (the seed's 3 769 of the 2026-08-31 dry run),
    and says `pre-r1`. Labelled `r1`, a reclassify's move of them would read
    as the next version's drift (the design's C6)."""

    def _record(self, oid, comment, verdict):
        return {"order_id": oid, "manager_comment": comment,
                **dict(zip(parse.VERDICT_COLUMNS, verdict))}

    def _today(self, comment):
        row = dict(zip(parse.UTM_COLUMNS[1:-1], parse.utm_columns(comment)))
        return tuple(row[c] for c in parse.VERDICT_COLUMNS)

    def test_what_the_rules_still_make_is_theirs_and_the_rest_is_earlier(self):
        pixel = "UTM: _fbp: fb.1.123.456"
        paid = "UTM: utm_source: fbads; utm_medium: cpc; utm_campaign: spring"
        old = ("pixel_only", "facebook", None, None, None)
        assert self._today(pixel) != old, "the r1 rule this case is about"
        records = [self._record(1, paid, self._today(paid)),
                   self._record(2, pixel, old),
                   self._record(3, pixel, self._today(pixel))]
        assert parse.not_these_rules(records) == [2]

    def test_the_label_names_the_version_it_precedes(self):
        assert parse.earlier_rules() == f"pre-{utm_classify.RULE_VERSION}"
        assert parse.earlier_rules() not in GOLDEN_SHA256


class TestTheRowHash:
    def test_it_reads_the_five_columns_in_order_with_the_canonical_null(self):
        sql = parse.verdict_hash_sql("d")
        order = [sql.index(f"d.{c}") for c in parse.VERDICT_COLUMNS]
        assert order == sorted(order)
        assert sql.count("'\\N'") == len(parse.VERDICT_COLUMNS)
        assert sql.count("chr(31)") == len(parse.VERDICT_COLUMNS) - 1
        assert "substr(md5(" in sql and "::bit(60)::bigint" in sql

    def test_the_verdict_columns_are_the_tables(self):
        """The five the log keeps are columns of `silver.order_utm` too."""
        assert set(parse.VERDICT_COLUMNS) <= set(parse.UTM_COLUMNS)
        assert parse.VERDICT_COLUMNS[:2] == ("traffic_type", "platform")
