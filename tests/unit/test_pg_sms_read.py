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


class TestTheHalfSwitchRefuses:
    """`KS_SMS_STORE=postgres` moves the audience read and nothing else yet.

    A flag that moved the read without the roster would compute an audience
    from Postgres and freeze the campaign it produced into DuckDB — the two
    would drift apart in silence, and a campaign whose roster is not the
    audience it was built from cannot be measured at all. So the unported half
    refuses out loud instead of writing where the reader will not look.
    """

    def test_every_unported_name_is_actually_guarded(self):
        """The list and the code must not drift. Parsed, not grepped: a
        comment naming the function would satisfy a grep."""
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

    def test_every_unported_name_exists_on_the_store(self):
        from core.duckdb_store import DuckDBStore
        from core.pg_sms import UNPORTED

        for name in UNPORTED:
            assert hasattr(DuckDBStore, name), f"{name} is not a store method"

    @pytest.mark.asyncio
    async def test_freezing_a_campaign_refuses_while_the_flag_is_on(
        self, tmp_path, monkeypatch,
    ):
        from core.duckdb_store import DuckDBStore

        monkeypatch.setenv("KS_SMS_STORE", "postgres")
        store = DuckDBStore(db_path=tmp_path / "half.duckdb")
        await store.connect()
        try:
            with pytest.raises(NotImplementedError, match="KS_SMS_STORE"):
                await store.get_sms_campaign_results("aug")
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_it_stays_out_of_the_way_by_default(self, tmp_path, monkeypatch):
        from core.duckdb_store import DuckDBStore

        monkeypatch.delenv("KS_SMS_STORE", raising=False)
        store = DuckDBStore(db_path=tmp_path / "whole.duckdb")
        await store.connect()
        try:
            # Reaches the store and answers on its own terms — an unknown
            # campaign is its own error, not a refusal to use this engine.
            with pytest.raises(ValueError):
                await store.get_sms_campaign_results("aug")
        finally:
            await store.close()
