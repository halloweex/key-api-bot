"""The Postgres path of the audience read, and the lock it must not take.

Two things here, and the second is the one that would go unnoticed.

`numbered` rewrites `?` into `$n` for asyncpg. It is a plain scan, which is
only safe while the body carries no `?` inside a string literal — true by
construction, since every value travels bound, and asserted rather than
assumed.

The other is §34's invariant: **a Postgres read must not touch DuckDB's
connection.** The store admits one process and every other reader queues behind
it, so a read that takes the lock on its way to another engine has moved the
bottleneck rather than left it. Nothing about the answer would look wrong — the
roster comes back correct, just behind the warehouse rebuild — so the test
makes the lock raise and requires the read to succeed anyway.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from core.pg_sms_read import numbered, render
from core.repositories.customers import SmsAudienceFilters
from core.sql_dialect import DUCKDB, POSTGRES, sms_segments_select

FRAGMENTS = dict(
    ltv_column="revenue_ltv",
    sales_type_filter="AND l.sales_type = ?",
    tier_case=("CASE WHEN c.revenue_ltv >= ? THEN 'VIP' "
               "WHEN c.orders >= ? OR c.revenue_ltv >= ? THEN 'CORE' "
               "WHEN c.recency <= ? THEN 'REACTIVATION' END"),
    arm_expr="tier_level",
    ok_tier_expr="tier_level IS NOT NULL",
    filter_sql="TRUE",
    tier_subset="",
)


class TestPlaceholders:
    def test_every_question_mark_becomes_its_own_number(self):
        assert numbered("a = ? AND b = ? AND c = ?") == "a = $1 AND b = $2 AND c = $3"

    def test_a_question_mark_in_a_comment_is_not_a_placeholder(self):
        """This cost a debugging round. A comment explaining a cast contained
        the words `? + INTERVAL`; the scan counted it, and every real parameter
        after it bound one argument to the left — into a query that still ran,
        with the attribution window silently taken from the wrong value."""
        sql = "a = ?\n-- see `? + INTERVAL` above\nAND b = ?"
        assert numbered(sql) == "a = $1\n-- see `? + INTERVAL` above\nAND b = $2"

    def test_a_question_mark_in_a_literal_is_not_a_placeholder(self):
        assert numbered("a = ? AND note = 'why?'") == "a = $1 AND note = 'why?'"

    def test_a_comment_ends_at_its_line(self):
        """Otherwise the first `--` would swallow the rest of the statement."""
        assert numbered("-- ?\nx = ?") == "-- ?\nx = $1"

    def test_the_body_has_no_question_mark_in_a_literal(self):
        """A plain scan is only correct while this holds. It holds because
        every value the audience filters on is bound, never interpolated.

        Comments come off first, and that is not tidiness: the body contains
        `-- each order's grand_total`, and one apostrophe inside a comment
        desynchronises every quote-counting parser for the rest of the file.
        The first version of this test failed on exactly that.
        """
        duck = sms_segments_select(DUCKDB, **FRAGMENTS)
        code = "\n".join(line.split("--")[0] for line in duck.splitlines())

        outside, in_literal = [], False
        for char in code:
            if char == "'":
                in_literal = not in_literal
            elif not in_literal:
                outside.append(char)
        assert code.count("?") == "".join(outside).count("?")

    def test_the_rendering_numbers_from_one_and_skips_nothing(self):
        sql = render(**FRAGMENTS)
        assert "?" not in sql
        highest = sms_segments_select(POSTGRES, **FRAGMENTS).count("?")
        assert f"${highest}" in sql
        for n in range(1, highest + 1):
            assert f"${n}" in sql, f"placeholder ${n} went missing"

    def test_the_filter_predicate_keeps_its_place_in_the_order(self):
        """The predicate contributes its own `?`s in the middle of the query,
        so a renumbering that ran per-fragment rather than over the whole
        string would bind the audience filters to the level thresholds."""
        filters = SmsAudienceFilters(cities=("Київ",), brands=("Cosrx",))
        filter_sql, filter_params = filters.predicate(
            "revenue_ltv", "retail", POSTGRES.order_lines,
        )
        sql = render(**{**FRAGMENTS, "filter_sql": filter_sql})
        assert "?" not in sql
        # Cities are lower-cased on the way in, because KeyCRM's are free text.
        assert filter_params == ["київ", "retail", "Cosrx"]


class TestItDoesNotTakeTheDuckDbLock:
    """§34's invariant, tested by making the lock fatal."""

    @pytest.mark.asyncio
    async def test_a_postgres_read_answers_while_the_store_lock_raises(
        self, tmp_path, monkeypatch,
    ):
        from core.duckdb_store import DuckDBStore

        monkeypatch.setenv("KS_SMS_STORE", "postgres")
        store = DuckDBStore(db_path=tmp_path / "locked.duckdb")
        await store.connect()

        def _explode(*_a, **_k):
            raise AssertionError(
                "the Postgres read reached for DuckDB's connection"
            )

        # One funnel row, no customers — the shape a RIGHT JOIN returns when
        # nothing survives, which is enough to prove the path ran.
        funnel_only = [(None,) * 19 + (0, 0, 0, 0, 0, 0, 0)]
        try:
            monkeypatch.setattr(type(store), "connection", _explode)
            with patch("core.pg_sms_read.fetch_segments",
                       new=AsyncMock(return_value=funnel_only)) as fetch:
                result = await store.get_sms_segments(grouping="single")

            assert fetch.await_count == 1
            assert result["funnel"][0] == {"stage": "customers", "remaining": 0}
        finally:
            monkeypatch.undo()
            await store.close()

    @pytest.mark.asyncio
    async def test_the_duckdb_path_still_uses_the_store(self, tmp_path, monkeypatch):
        """The mirror image, so the test above cannot pass by the routing
        being broken in both directions."""
        from core.duckdb_store import DuckDBStore

        monkeypatch.delenv("KS_SMS_STORE", raising=False)
        store = DuckDBStore(db_path=tmp_path / "used.duckdb")
        await store.connect()
        try:
            with patch("core.pg_sms_read.fetch_segments",
                       new=AsyncMock()) as fetch:
                result = await store.get_sms_segments(grouping="single")
            assert fetch.await_count == 0
            assert result["funnel"][0] == {"stage": "customers", "remaining": 0}
        finally:
            await store.close()


class TestTheSwitchIsWhole:
    """Nothing is left half-switched, and the guard that proved it still works.

    `UNPORTED` is empty: every path `/sms` uses runs against whichever store
    `KS_SMS_STORE` names. The mechanism stays rather than being deleted the day
    it first reached empty — the next thing to move onto two engines wants the
    same protection, and a guard removed at that moment is one nobody rebuilds
    in time.
    """

    def test_nothing_is_left_behind(self):
        from core.pg_sms import UNPORTED

        assert UNPORTED == (), (
            f"still answered by DuckDB alone: {UNPORTED} — the flag would move "
            f"the read and leave these writing to the other store"
        )

    def test_no_method_still_calls_the_guard(self):
        """The list and the code must not drift apart in either direction.
        Parsed, not grepped: a comment naming the function satisfies a grep."""
        import ast
        import inspect

        from core.pg_sms import UNPORTED
        from core.repositories import customers as module

        tree = ast.parse(inspect.getsource(module))
        guarded = set()
        for node in ast.walk(tree):
            if not isinstance(node, ast.AsyncFunctionDef):
                continue
            for call in ast.walk(node):
                if (isinstance(call, ast.Call)
                        and getattr(call.func, "id", None) == "refuse_while_unported"):
                    guarded.add(node.name)

        assert guarded == set(UNPORTED), (
            f"guarded but not listed: {guarded - set(UNPORTED)}; "
            f"listed but not guarded: {set(UNPORTED) - guarded}"
        )

    def test_the_guard_still_refuses_when_something_is_listed(self, monkeypatch):
        """The mechanism itself, exercised on a name that is not real — so the
        test keeps working after every method has moved."""
        from core import pg_sms

        monkeypatch.setenv("KS_SMS_STORE", "postgres")
        with pytest.raises(NotImplementedError, match="KS_SMS_STORE"):
            pg_sms.refuse_while_unported("some_future_half_ported_thing")

    def test_it_stays_silent_on_the_default_engine(self, monkeypatch):
        from core import pg_sms

        monkeypatch.delenv("KS_SMS_STORE", raising=False)
        pg_sms.refuse_while_unported("anything")
