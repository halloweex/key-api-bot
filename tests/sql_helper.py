"""Reading SQL structurally, for tests that scan the repository's queries.

Prose is not structure. A comment naming a table makes a scan report a
dependency the query does not have, and a scanner tripping over an apostrophe
inside one has already cost this repository two debugging rounds — see
`core.pg_sms_read.numbered`, where `? + INTERVAL` inside a comment was counted
as a placeholder and shifted every parameter left, into a query that still ran.

So the tests that scan SQL share one stripper rather than each writing their
own, which is the same rule the queries themselves follow.
"""
from __future__ import annotations


def strip_comments_and_literals(sql: str) -> str:
    """SQL with `--` comments and `'…'` literals blanked out."""
    out: list[str] = []
    in_comment = in_literal = False
    i = 0
    while i < len(sql):
        char = sql[i]
        if in_comment:
            if char == "\n":
                in_comment = False
                out.append(char)
        elif in_literal:
            if char == "'":
                in_literal = False
        elif sql[i:i + 2] == "--":
            in_comment = True
        elif char == "'":
            in_literal = True
        else:
            out.append(char)
        i += 1
    return "".join(out)
