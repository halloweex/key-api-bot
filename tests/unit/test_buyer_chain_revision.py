"""Revision 0034, chain 4's lock: what it writes and what it puts back.

It changes no schema — three table comments — and exists so that an image
without chain 4 refuses a database that has it (`REQUIRED_REVISION`). Its
downgrade must put back exactly what 0011 and 0024 wrote, and clear the one
table that never had a comment, or a round trip would leave a comment no
revision owns. Parsed from the files, not restated.
"""
from __future__ import annotations

import re
from pathlib import Path

VERSIONS = Path(__file__).resolve().parents[2] / "migrations" / "versions"


REV = VERSIONS / "0034_buyer_chain.py"


def _ancestry(head: str) -> list:
    """Every revision from `head` back to the base, parsed from the files."""
    parents = {}
    for path in VERSIONS.glob("*.py"):
        src = path.read_text(encoding="utf-8")
        rev = re.search(r'^revision = "([^"]+)"$', src, re.M).group(1)
        down = re.search(r'^down_revision = (?:"([^"]+)"|None)$', src, re.M)
        parents[rev] = down.group(1) if down else None
    out, cursor = [], head
    while cursor:
        out.append(cursor)
        cursor = parents[cursor]
    return out


class TestTheLock:
    def test_it_is_still_in_force_under_the_head_the_code_requires(self):
        """0034 was the head while it was the lock; since 0035 (the batch-E
        chains' lock) it is an ancestor, and the lock holds for as long as it
        stays one: an image older than chain 4 requires 0033 or less, and any
        mismatch refuses. Kills: a later revision branching round 0034, or the
        pin moving to a head that does not descend from it."""
        from core import pg

        assert "0034_buyer_chain" in _ancestry(pg.REQUIRED_REVISION)
        src = REV.read_text(encoding="utf-8")
        assert re.search(r'^revision = "0034_buyer_chain"$', src, re.M)
        assert re.search(r'^down_revision = "0033_derivation_signal"$', src, re.M)
