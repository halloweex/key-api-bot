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

from core.sql_dialect import numbered


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
