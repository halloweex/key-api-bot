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


def _text(node, consts=None) -> str:
    """Every string inside `node`, joined — f-string parts included, and the
    value of any module-level string constant it names. A reason is often
    built as `f"{PROVISIONED}: ..."` with the phrase living in the constant
    (test_utm_reclassify_dryrun_pg.py does exactly that); reading only the
    literal parts reported a skip CI counts as one it cannot see."""
    consts = consts or {}
    parts = []
    for n in ast.walk(node):
        if isinstance(n, ast.Constant) and isinstance(n.value, str):
            parts.append(n.value)
        elif isinstance(n, ast.Name) and n.id in consts:
            parts.append(consts[n.id])
    return " ".join(parts)


def _module_strings(tree) -> dict:
    """Top-level `NAME = "..."` assignments, the parenthesised multi-line
    literal included (Python joins it into one constant)."""
    out = {}
    for stmt in tree.body:
        if (isinstance(stmt, ast.Assign) and len(stmt.targets) == 1
                and isinstance(stmt.targets[0], ast.Name)
                and isinstance(stmt.value, ast.Constant)
                and isinstance(stmt.value.value, str)):
            out[stmt.targets[0].id] = stmt.value.value
    return out


def _names(node) -> set:
    return {n.id for n in ast.walk(node) if isinstance(n, ast.Name)} | \
           {n.attr for n in ast.walk(node) if isinstance(n, ast.Attribute)}


def _names_postgres(words: str) -> bool:
    low = words.lower()
    return any(w.lower() in low for w in POSTGRES_WORDS)


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
        consts = _module_strings(tree)
        for call in ast.walk(tree):
            if not isinstance(call, ast.Call):
                continue
            if _is(call, "mark.skipif", "skipif"):
                reason = next((k.value for k in call.keywords if k.arg == "reason"), None)
                words = _text(reason, consts) if reason is not None else ""
                condition = call.args[0] if call.args else None
                # The DSN read through a name (`not DSN`) or a literal
                # (`not os.getenv("KS_PG_DSN")` — the ClickHouse test's shape,
                # so the likeliest copy for a new Postgres one).
                reads_dsn = condition is not None and (
                    any("DSN" in name for name in _names(condition))
                    or "PG_DSN" in _text(condition))
                about_pg = reads_dsn or _names_postgres(words)
            elif _is(call, "pytest.skip"):
                words = _text(call, consts)
                about_pg = _names_postgres(words)
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


def test_it_sees_the_dsn_read_through_a_literal_and_any_case(tmp_path):
    """Two shapes PR-1's review found the first walker blind to."""
    (tmp_path / "test_x.py").write_text(
        'import os, pytest\n'
        'a = pytest.mark.skipif(not os.getenv("KS_PG_DSN"), reason="needs a database")\n'
        'b = pytest.mark.skipif(True, reason="no postgres here")\n',
        encoding="utf-8")
    found = postgres_skips_with_the_wrong_words(tmp_path)
    assert [loc.rsplit(":", 1)[1] for loc, _ in found] == ["2", "3"]


def test_the_canonical_spelling_passes(tmp_path):
    (tmp_path / "test_x.py").write_text(
        'import os, pytest\n'
        'DSN = os.getenv("KS_PG_DSN")\n'
        'needs = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")\n'
        'other = pytest.mark.skipif(not os.getenv("KS_CH_URL"), reason="needs ClickHouse")\n',
        encoding="utf-8")
    assert postgres_skips_with_the_wrong_words(tmp_path) == []


def test_a_reason_built_from_a_module_constant_is_read_whole(tmp_path):
    """`f"{PROVISIONED}: ..."` carries the phrase in the constant. Found the
    first time this walker ran over main after DN-15 landed."""
    (tmp_path / "test_x.py").write_text(
        'import os, pytest\n'
        'DSN = os.getenv("KS_PG_DSN")\n'
        'PROVISIONED = ("needs a live PostgreSQL provisioned "\n'
        '               "as CI provisions it")\n'
        'BLIND = "no database here"\n'
        'a = pytest.mark.skipif(not DSN, reason=f"{PROVISIONED}: a role")\n'
        'b = pytest.mark.skipif(not DSN, reason=PROVISIONED)\n'
        'c = pytest.mark.skipif(not DSN, reason=f"{BLIND}: a role")\n',
        encoding="utf-8")
    found = postgres_skips_with_the_wrong_words(tmp_path)
    assert [loc.rsplit(":", 1)[1] for loc, _ in found] == ["8"]

