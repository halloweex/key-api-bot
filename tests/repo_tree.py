"""Whether this run holds the repository's own tree.

A few guards read the checkout itself — `git ls-files`, `.gitignore`, the
agents' instructions under `.claude/`, CLAUDE.md — to prove that no file,
tracked or documented, opens DuckDB the wrong way. The VPS gate runs the
suite inside the production image, which holds none of that (no `.git`, no
`git`, no `.claude/`), so there those guards could only fail on the
environment. CI runs on the checkout and runs them; the gate skips them by
name and says why, so a skip there never hides a defect the checkout would
show.
"""
from __future__ import annotations

import shutil
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]


def has_checkout() -> bool:
    return ((REPO / ".git").exists() and shutil.which("git") is not None
            and (REPO / ".claude" / "CLAUDE.md").is_file())


needs_checkout = pytest.mark.skipif(
    not has_checkout(),
    reason="reads the repository's own tree (git, .gitignore, .claude/), which the "
           "production image the VPS gate runs in does not hold; CI runs it on the checkout")
