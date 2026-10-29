"""A host tool the documentation runs by its path carries the execute bit.

The host gets this tree through `git reset --hard`, which writes each file
with the mode git records — 100644 or 100755 — and nothing else. So a tool
committed 100644 and documented as `deploy/x.py snap …` answers `permission
denied` on the host, at the moment somebody is following a runbook: S14-02's
`deploy/api_fingerprint.py` was committed that way, with every command in its
own header, in CLAUDE.md and in the PR body written to run it directly.

The subjects are derived, not named: every file under `deploy/` with a `#!`
line that some command runs by its path. A command is
  * a line of the tool's own header that starts with its path — the header
    is where its commands are written to be copied — and
  * a line of a fenced block in a Markdown document of this repository.
Prose that merely names a tool (`deploy/x.sh` reads …) is neither, and a
tool run through an interpreter (`python3 deploy/x.py`, `bash deploy/x.sh`,
`source deploy/x.sh`) needs no bit and is not counted.
"""
from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
DEPLOY = REPO / "deploy"

# The path at command position: after indentation, a shell prompt, a comment
# hash (a shell header), `cd <dir> &&`, or the host checkout's absolute path.
_RUN_BY_PATH = (r"^[ \t]*(?:#[ \t]*)?(?:\$[ \t]+)?(?:cd \S+ && )?"
                r"(?:\./|/opt/key-api-bot/)?{path}(?=[ \t]|$)")

_NOT_DOCUMENTS = {".git", ".planning", "node_modules", "worktrees"}


def _pattern(rel: str) -> "re.Pattern[str]":
    return re.compile(_RUN_BY_PATH.format(path=re.escape(rel)), re.M)


def _tools():
    return sorted(p for p in DEPLOY.rglob("*")
                  if p.is_file() and "runbooks" not in p.parts
                  and p.read_bytes()[:2] == b"#!")


def _fenced(text: str) -> str:
    """The lines inside ``` fences, joined: the commands of a document."""
    out, inside = [], False
    for line in text.splitlines():
        if line.lstrip().startswith("```"):
            inside = not inside
            continue
        if inside:
            out.append(line)
    return "\n".join(out)


def _documents():
    return sorted(p for p in REPO.rglob("*.md")
                  if not (_NOT_DOCUMENTS & set(p.relative_to(REPO).parts)))


def _run_by_path():
    """{tool: [where a command runs it by its path]}."""
    docs = [(d, _fenced(d.read_text(encoding="utf-8", errors="replace")))
            for d in _documents()]
    found = {}
    for tool in _tools():
        rel = tool.relative_to(REPO).as_posix()
        pattern = _pattern(rel)
        where = []
        if pattern.search(tool.read_text(encoding="utf-8", errors="replace")):
            where.append(f"{rel}'s own header")
        where += [d.relative_to(REPO).as_posix() for d, text in docs if pattern.search(text)]
        if where:
            found[tool] = where
    return found


def _git_mode(path: Path):
    """The mode git records for `path` — what `git reset --hard` writes on
    the host — or None where there is no git to ask."""
    try:
        done = subprocess.run(["git", "-C", str(REPO), "ls-files", "-s", "--",
                               path.relative_to(REPO).as_posix()],
                              capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.SubprocessError):
        return None
    line = done.stdout.strip()
    return line.split()[0] if done.returncode == 0 and line else None


def test_a_tool_run_by_its_path_is_executable():
    """Mutation: `git update-index --chmod=-x deploy/api_fingerprint.py`, or
    commit a new tool whose header shows `deploy/new_tool.py --flag` as
    100644."""
    problems = []
    for tool, where in _run_by_path().items():
        rel = tool.relative_to(REPO).as_posix()
        mode = _git_mode(tool)
        if mode is not None and mode != "100755":
            problems.append(f"{rel} is {mode} in git, and {', '.join(where)} "
                            f"run it by its path")
        elif not os.access(tool, os.X_OK):
            problems.append(f"{rel} is not executable here, and {', '.join(where)} "
                            f"run it by its path")
    assert not problems, "\n".join(problems)


def test_the_walk_sees_the_tools_it_guards():
    """If the derivation found nothing, the test above would pass by reading
    nothing. Each of these is run by its path in its own header or in a
    runbook today. `restore-test.sh` is run by its path only in
    docs/offsite_backup_setup.md, which the store-backed gate does not mount,
    so it is required only where that document is."""
    seen = {t.relative_to(REPO).as_posix() for t in _run_by_path()}
    expected = {"deploy/api_fingerprint.py", "deploy/ark_ship.sh", "deploy/stage4_soak.sh"}
    if (REPO / "docs" / "offsite_backup_setup.md").is_file():
        expected.add("deploy/restore-test.sh")
    assert expected <= seen, seen


@pytest.mark.parametrize("line, counted", [
    ("    deploy/api_fingerprint.py snap --base https://x --out a.json", True),
    ("#   deploy/api_fingerprint.py compare a.json b.json", True),
    ("cd /opt/key-api-bot && deploy/api_fingerprint.py list", True),
    ("$ ./deploy/api_fingerprint.py list", True),
    ("/opt/key-api-bot/deploy/api_fingerprint.py list", True),
    ("deploy/api_fingerprint.py", True),
    ("    python3 deploy/api_fingerprint.py list", False),
    ("source deploy/api_fingerprint.py", False),
    ("deploy/api_fingerprint.py.bak is another file", False),
    ("`deploy/api_fingerprint.py` is how a read PR shows it", False),
])
def test_what_counts_as_running_it_by_its_path(line, counted):
    assert bool(_pattern("deploy/api_fingerprint.py").search(line)) is counted, line
