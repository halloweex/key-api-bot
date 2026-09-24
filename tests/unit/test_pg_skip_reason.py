"""Every test that skips for want of PostgreSQL says so in the words CI counts.

CI runs the suite against a real PostgreSQL and fails the job if any store test
still skipped — the arrangement breaking silently is the state it replaced. It
finds those skips by the phrase "needs a live PostgreSQL" in the skip reason
(`.github/workflows/ci.yml`, "No store test may have skipped"). A skip spelled
any other way is invisible to that step: if CI ever lost its database, that
test would stop running and nobody would be told.

Two files were spelled otherwise — `test_order_versions.py`, which guards the
archive of order versions that nothing can rebuild, and `test_bot_database.py`
— and a third copied the first on 2026-09-23 before review caught it. So this
walks `tests/` rather than naming files: a guard that names its subjects only
guards the ones already thought of.
"""
from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PHRASE = "needs a live PostgreSQL"
# What makes a skip a Postgres skip: its condition reads the DSN, or its words
# name the database.
POSTGRES_WORDS = ("KS_PG_DSN", "PostgreSQL", "Postgres")


def _text(node) -> str:
    """Every string constant inside `node`, joined — f-strings included."""
    return " ".join(n.value for n in ast.walk(node)
                    if isinstance(n, ast.Constant) and isinstance(n.value, str))


def _names(node) -> set:
    return {n.id for n in ast.walk(node) if isinstance(n, ast.Name)} | \
           {n.attr for n in ast.walk(node) if isinstance(n, ast.Attribute)}


def _is(call: ast.Call, *dotted: str) -> bool:
    """`call.func` spells one of `dotted`, e.g. 'pytest.skip' or 'mark.skipif'."""
    parts, f = [], call.func
    while isinstance(f, ast.Attribute):
        parts.append(f.attr)
        f = f.value
    if isinstance(f, ast.Name):
        parts.append(f.id)
    spelled = ".".join(reversed(parts))
    return any(spelled == d or spelled.endswith("." + d) for d in dotted)


def postgres_skips_with_the_wrong_words(root: Path = ROOT / "tests"):
    """(path:line, reason) for every Postgres skip CI cannot see."""
    bad = []
    for path in sorted(root.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for call in ast.walk(tree):
            if not isinstance(call, ast.Call):
                continue
            if _is(call, "mark.skipif", "skipif"):
                reason = next((k.value for k in call.keywords if k.arg == "reason"), None)
                words = _text(reason) if reason is not None else ""
                condition = call.args[0] if call.args else None
                reads_dsn = condition is not None and any(
                    "DSN" in name for name in _names(condition))
                about_pg = reads_dsn or any(w in words for w in POSTGRES_WORDS)
            elif _is(call, "pytest.skip"):
                words = _text(call)
                about_pg = any(w in words for w in POSTGRES_WORDS)
            else:
                continue
            if about_pg and PHRASE not in words:
                where = path.relative_to(ROOT) if ROOT in path.parents else path.name
                bad.append((f"{where}:{call.lineno}", words))
    return bad


def test_every_postgres_skip_is_one_ci_can_see():
    assert postgres_skips_with_the_wrong_words() == []


def test_the_phrase_is_the_one_ci_counts():
    ci = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    assert f'grep -c "{PHRASE}"' in ci, "CI no longer counts skips by this phrase"


def test_it_would_catch_both_spellings(tmp_path):
    """The mutations, kept: the two shapes the blind files had."""
    (tmp_path / "test_x.py").write_text(
        'import os, pytest\n'
        'DSN = os.getenv("KS_PG_DSN")\n'
        'needs = pytest.mark.skipif(not DSN, reason="KS_PG_DSN is not set")\n'
        'def test_y():\n'
        '    pytest.skip("KS_PG_DSN is not set — the Postgres half needs a database")\n',
        encoding="utf-8")
    found = postgres_skips_with_the_wrong_words(tmp_path)
    assert [loc.rsplit(":", 1)[1] for loc, _ in found] == ["3", "5"]


def test_the_canonical_spelling_passes(tmp_path):
    (tmp_path / "test_x.py").write_text(
        'import os, pytest\n'
        'DSN = os.getenv("KS_PG_DSN")\n'
        'needs = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")\n'
        'other = pytest.mark.skipif(not os.getenv("KS_CH_URL"), reason="needs ClickHouse")\n',
        encoding="utf-8")
    assert postgres_skips_with_the_wrong_words(tmp_path) == []
