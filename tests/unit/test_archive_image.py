"""The DuckDB archive tools run on a pinned image (step 14, S14-02).

Step 14 takes DuckDB out of main, and at S14-19 out of the images. The
archives stay, and what reads them is DuckDB 1.5.5 with the restore code they
were proven against — both in every image built before step 14, and the host
tags the last of them `rollback-step14`, pinned at S14-00's deploy (the image
production runs then; 3.0.292 was the tag's first target and is kept as
`rollback-step14-3.0.292`). So `deploy/restore-test.sh`,
`deploy/daily_offsite.sh` and `deploy/ark_ship.sh` take their image from a
variable that must be set, and ask it before using it
(`deploy/archive_image.sh`). Two halves are held here:

* the library itself, run against a fake `docker`: an unset, moving or
  untagged image is refused before docker is asked anything; an absent one,
  one without DuckDB and one with another version are refused after asking;
  and the program it runs in the image really prints DuckDB's version;
* the host files those scripts mount into that image. They are this
  checkout's, run inside an image from before step 14, so a name main gains
  after that image is a name the container does not have — and a step-14 PR
  deleting from main a module the image still carries changes nothing for
  them. The mounts are derived
  from the scripts and the recipes in the tools' headers by
  `tests/archive_mounts.py`, which reads every mount the way the shell does —
  `${PWD}`, `$(pwd)`, variables, function arguments, the `DOCKER_RUN` array —
  and every source must be a listed file, a path under `data/` or a listed
  place outside the checkout. Every import in a mounted file is held to a
  FROZEN list of what the pinned image carries. Frozen on purpose: read from
  today's tree, it would follow main away from the image.

  The list was read from 3.0.292's code (`a9595abf`, 2026-10-21) and checked
  again, name by name, against `6ac33044` (origin/main on 2026-10-25: the code
  of 3.0.298, which production ran that day, plus the version bump): every
  name it holds is defined in both, and `scripts/compact_duckdb.py`,
  `core/duckdb_switch.py` and `requirements.lock` (`duckdb==1.5.5`) are
  byte-identical between the two commits. The tag is pinned to whatever image
  production runs when S14-00 deploys; if that image was built from a later
  commit, read the list against that commit's `scripts/compact_duckdb.py` and
  `core/duckdb_switch.py` before the host steps.

The scripts' own behaviour around the library is run in
`test_ark_ship_script.py` and `test_week_of_silence_host.py`, and the rule
that every archive tool sources it in `test_backup_script_guards.py`.
"""
from __future__ import annotations

import ast
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from tests.archive_mounts import walk

REPO = Path(__file__).resolve().parents[2]
DEPLOY = REPO / "deploy"
LIBRARY = DEPLOY / "archive_image.sh"
PINNED = "halloweex/keycrm-web:rollback-step14"
ARCHIVE_SCRIPTS = ("restore-test.sh", "daily_offsite.sh", "ark_ship.sh")

pytestmark = pytest.mark.skipif(not shutil.which("bash"), reason="needs bash")


# ── the library ───────────────────────────────────────────────────────────────

FAKE_DOCKER = r"""#!/usr/bin/env bash
printf '%s\n' "$*" >> "$FAKE_CALLS"
case " $* " in
    *" image inspect "*) [ -n "${FAKE_NO_IMAGE:-}" ] && exit 1; exit 0 ;;
    *"import duckdb"*)
        case "${FAKE_DUCKDB:-1.5.5}" in
            absent) echo "ModuleNotFoundError: No module named 'duckdb'" >&2; exit 1 ;;
            noisy) echo "a warning on stdout"; echo "1.5.5" ;;
            silent) ;;
            *) echo "${FAKE_DUCKDB:-1.5.5}" ;;
        esac
        exit 0 ;;
esac
exit 3
"""


@pytest.fixture
def lib(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "docker").write_text(FAKE_DOCKER)
    (bin_dir / "docker").chmod(0o755)
    calls = tmp_path / "calls.log"

    def run(function: str, var: str, value: str, **fake: str):
        calls.write_text("")
        env = {k: v for k, v in os.environ.items() if not k.startswith(("FAKE_", "BACKUP_"))}
        env.update({"PATH": f"{bin_dir}{os.pathsep}{env.get('PATH', '')}",
                    "FAKE_CALLS": str(calls), **fake})
        done = subprocess.run(
            ["bash", "-c", 'set -Eeuo pipefail; source "$1"; "$2" "$3" "$4"', "_",
             str(LIBRARY), function, var, value],
            env=env, capture_output=True, text=True, timeout=30)
        return done.returncode, done.stdout, done.stderr, calls.read_text().splitlines()

    return run


class TestRequire:
    @pytest.mark.parametrize("value", [
        PINNED, "halloweex/keycrm-web:3.0.292", "registry.local:5000/keycrm-web:rollback-step14",
        "halloweex/keycrm-web@sha256:" + "a" * 64,
    ])
    def test_a_tag_that_does_not_move_passes_silently(self, lib, value):
        code, out, err, calls = lib("archive_image_require", "BACKUP_RESTORE_IMAGE", value)
        assert (code, out, err, calls) == (0, "", "", [])

    @pytest.mark.parametrize("value, words", [
        ("", "BACKUP_RESTORE_IMAGE is not set, and the archive tools no longer default to :latest"),
        ("halloweex/keycrm-web:latest", "moves with every deploy"),
        ("halloweex/keycrm-web", "names no tag, which is :latest"),
        ("registry.local:5000/keycrm-web", "names no tag, which is :latest"),
    ], ids=["unset", "latest", "untagged", "registry-port-untagged"])
    def test_a_moving_image_is_refused_before_docker_is_asked(self, lib, value, words):
        """A registry's host:port is not a tag: the tag is after the last ':'
        of the last path component. Mutation: read the tag off the whole
        string."""
        code, out, _, calls = lib("archive_image_require", "BACKUP_RESTORE_IMAGE", value)
        assert code == 2 and words in out, out
        assert f"BACKUP_RESTORE_IMAGE={PINNED} in deploy/backup.env" in out, out
        assert "rollback-step14" in out and "1.5.5" in out
        assert calls == []


class TestProbe:
    def test_the_pinned_image_with_duckdb_1_5_5_passes_silently(self, lib):
        code, out, err, calls = lib("archive_image_ready", "BACKUP_ARK_IMAGE", PINNED)
        assert (code, out, err) == (0, "", ""), (out, err)
        inspect, run = calls
        assert inspect == f"image inspect {PINNED}"
        # A throwaway container: never pulled, no network, no environment.
        assert run.startswith("run --rm --pull never --network none --entrypoint python3 "
                              f"{PINNED} -c "), run
        assert "--env-file" not in run and " -e " not in run and " -v " not in run

    def test_the_last_line_is_the_version(self, lib):
        code, out, _, _ = lib("archive_image_probe", "BACKUP_ARK_IMAGE", PINNED,
                              FAKE_DUCKDB="noisy")
        assert code == 0, out

    @pytest.mark.parametrize("fake, words", [
        ({"FAKE_NO_IMAGE": "1"}, f"no image {PINNED} on this host, and this does not pull"),
        ({"FAKE_DUCKDB": "absent"},
         f"{PINNED} cannot import duckdb (ModuleNotFoundError: No module named 'duckdb')"),
        ({"FAKE_DUCKDB": "1.4.3"}, f"{PINNED} carries DuckDB 1.4.3, not 1.5.5"),
        ({"FAKE_DUCKDB": "silent"}, f"{PINNED} carries DuckDB ?, not 1.5.5"),
    ], ids=["absent", "no-duckdb", "another-version", "silent"])
    def test_an_image_that_cannot_read_the_archives_is_refused(self, lib, fake, words):
        code, out, _, _ = lib("archive_image_ready", "BACKUP_ARK_IMAGE", PINNED, **fake)
        assert code == 2 and words in out, out
        assert f"BACKUP_ARK_IMAGE={PINNED}" in out

    def test_require_comes_first(self, lib):
        """`ready` on a moving tag asks docker nothing. Mutation: probe first."""
        code, _, _, calls = lib("archive_image_ready", "BACKUP_ARK_IMAGE",
                                "halloweex/keycrm-web:latest")
        assert code == 2 and calls == []

    def test_the_program_it_runs_prints_duckdbs_version(self):
        """The one line of Python the probe runs in the image, run here: it
        prints exactly what `duckdb.__version__` is."""
        duckdb = pytest.importorskip("duckdb")
        [program] = re.findall(r"-c '([^']*)'", LIBRARY.read_text())
        done = subprocess.run([sys.executable, "-c", program], capture_output=True,
                              text=True, timeout=60)
        assert done.returncode == 0, done.stderr
        assert done.stdout.strip().splitlines()[-1] == duckdb.__version__

    def test_the_version_is_the_one_the_lock_installs_today(self):
        """1.5.5 wrote every archive; the lock still installs it until step
        14's S14-19. Mutation: bump one without the other while both exist."""
        text = LIBRARY.read_text()
        assert re.search(r'^ARCHIVE_DUCKDB_VERSION="1\.5\.5"$', text, re.M)
        assert re.search(rf'^ARCHIVE_PINNED_IMAGE="{re.escape(PINNED)}"$', text, re.M)
        lock = (REPO / "requirements.lock").read_text()
        pinned = re.findall(r"^duckdb==(\S+)$", lock, re.M)
        assert pinned in ([], ["1.5.5"]), pinned


# ── what is mounted into the pinned image ────────────────────────────────────

# The files of this checkout that run inside the pinned image: the container
# that mounts each, and how much of it runs there — a tool runs whole, a
# library mounted beside one its module level (the fate check never calls the
# functions whose imports reach the rest of the application).
MOUNTED = {
    "deploy/restore_from_export.py": ("restore-test.sh", "whole"),
    "deploy/snapshot_export.py": ("daily_offsite.sh", "whole"),
    "core/snapshot_validation.py": ("daily_offsite.sh", "whole"),
    "deploy/ark_freeze.py": ("ark_ship.sh", "whole"),
    "deploy/duckdb_table_fates_check.py": ("duckdb_table_fates_check.py", "whole"),
    "core/duckdb_table_fates.py": ("duckdb_table_fates_check.py", "module"),
}

# FROZEN: what the pinned image (rollback-step14, the last image before step
# 14, pinned at S14-00's deploy) carries that the mounted files use, beyond the
# standard library and duckdb — read from 3.0.292's code (a9595abf) on
# 2026-10-21 and checked against 6ac33044, 3.0.298's code, on 2026-10-25
# (`scripts/compact_duckdb.py` on its /app/scripts, `core/duckdb_switch.py` on
# /app; both unchanged between the two). Never derived from today's tree: step
# 14 deletes both from main, and the image keeps them.
PINNED_IMAGE = {
    "compact_duckdb": frozenset({
        "section", "log", "MANIFEST_PATH", "NEW_DB", "SOURCE_DB",
        "phase1_export", "phase2_import", "phase3_validate"}),
    "core.duckdb_switch": frozenset({"DuckDBOpenedWhileOff", "HELD_BACK_NOTE"}),
}
THIRD_PARTY = frozenset({"duckdb"})


def _module_name(path: str) -> str:
    return path[:-3].replace("/", ".")


# Every mount source that is not this checkout, as the script writes it, and
# what it is. A new one fails until it is written here: a source the walk
# cannot resolve lands in this list too, so a spelling of the checkout the
# walk does not know is a question, never a pass.
OUTSIDE = {
    ("restore-test.sh", "$TMP/data"): "the drill's scratch directory, a mktemp",
    ("ark_ship.sh", "$SOURCE"): "the DuckDB file --source names, alone and read-only",
    ("ark_ship.sh", "$OUT_ROOT"): "where the Ark is written: --out, or BACKUP_ARK_DIR",
    ("ark_ship.sh", "$ARK_DIR"): "the Ark being verified, a stamp under that root",
    ("ark_ship.sh", "$back/x/$STAMP"): "the Ark downloaded back, in a mktemp",
    ("ark_ship.sh", "$back/x/ark_freeze.py"): (
        "the verifier that came back with the Ark: its own copy, the one a stranger "
        "will have, compared with this checkout's whenever this run sent it"),
    ("restore_from_export.py", "/tmp/drill"): "the real restore's scratch directory",
}


def _recipes():
    """Every host tool whose header gives a `docker run` to copy: the
    commands a person runs on the pinned image by hand."""
    out = {}
    for path in sorted(DEPLOY.rglob("*.py")):
        doc = ast.get_docstring(ast.parse(path.read_text())) or ""
        if "docker run" in doc:
            out[path.name] = doc
    return out


def _is_checkout_file(rel: str) -> bool:
    return (REPO / rel).is_file()


def _walks(scripts=None, recipes=None) -> dict:
    """{site: Mounts} for the three scripts and every recipe."""
    scripts = scripts if scripts is not None else {
        name: (DEPLOY / name).read_text() for name in ARCHIVE_SCRIPTS}
    recipes = recipes if recipes is not None else _recipes()
    return {site: walk(text, site, is_checkout_file=_is_checkout_file)
            for site, text in {**scripts, **recipes}.items()}


def _derived_mounts(walks=None) -> dict:
    """Every `.py` of this checkout a script or the table-fate check's recipe
    mounts into the image, and which one mounts it — the containers whose
    files sit beside each other in `MOUNTED`."""
    walks = walks if walks is not None else _walks()
    found = {}
    for site in (*ARCHIVE_SCRIPTS, "duckdb_table_fates_check.py"):
        found.update(walks[site].found)
    return found


def test_the_mounts_are_the_listed_ones():
    """A new mount is a new file to hold to the image: add it here, and to
    `PINNED_IMAGE` whatever it needs. Every source is accounted for — a file
    of `MOUNTED`, a path under `data/`, or one of `OUTSIDE` — and a recipe
    mounts nothing `MOUNTED` does not hold. Mutation: mount another file, in
    any spelling `test_the_walk_reads_every_spelling` lists."""
    walks = _walks()
    assert [p for w in walks.values() for p in w.problems] == []
    assert _derived_mounts(walks) == {path: where for path, (where, _) in MOUNTED.items()}
    for site, w in walks.items():
        assert set(w.found) <= set(MOUNTED), (site, w.found)
    outside = {(site, expr) for site, w in walks.items() for expr in w.outside}
    assert outside == set(OUTSIDE)
    assert {"restore_from_export.py", "duckdb_table_fates_check.py"} <= set(_recipes())


_SNAPSHOT_ANCHOR = '-v "$PWD/deploy/snapshot_export.py:'


def _with_line(line: str, script: str = "daily_offsite.sh", anchor: str = _SNAPSHOT_ANCHOR):
    """The script's text with `line` inserted before the line carrying
    `anchor`, at its indentation, as one more argument of that command."""
    text = (DEPLOY / script).read_text()
    at = text.index(anchor)
    start = text.rindex("\n", 0, at) + 1
    indent = text[start:at]
    return text[:start] + indent + line + " \\\n" + text[start:]


@pytest.mark.parametrize("line", [
    '-v "${PWD}/core/write_chains.py:/app/core/write_chains.py:ro"',
    '-v "$(pwd)/core/write_chains.py:/app/core/write_chains.py:ro"',
    '-v "$(pwd -P)/core/write_chains.py:/app/core/write_chains.py:ro"',
    '-v "/opt/key-api-bot/core/write_chains.py:/app/core/write_chains.py:ro"',
    '-v $PWD/core/write_chains.py:/app/core/write_chains.py:ro',
    '--volume="$PWD/core/write_chains.py:/app/core/write_chains.py:ro"',
    '--mount type=bind,source=$PWD/core/write_chains.py,target=/app/core/write_chains.py',
], ids=["braces", "pwd", "pwd-P", "absolute", "unquoted", "long-form", "mount"])
def test_the_walk_reads_every_spelling(line):
    """Each is one mount — a file from main inside the pinned image — in another
    spelling. The first walk read `-v "$PWD/…"` alone and passed the
    `${PWD}` one. Mutation: go back to a regex over one spelling."""
    walks = _walks(scripts={"daily_offsite.sh": _with_line(line)}, recipes={})
    assert walks["daily_offsite.sh"].found.get("core/write_chains.py") == "daily_offsite.sh"


def test_the_walk_follows_a_variable_and_a_function_argument():
    """A variable assigned from the checkout, in any spelling, and the
    checkout passed to a function that mounts its argument."""
    text = _with_line('-v "$CHAIN:/app/core/write_chains.py:ro"')
    text = text.replace('WORK="data/.snapshot_tmp"',
                        'WORK="data/.snapshot_tmp"\nHERE="$(pwd)"\n'
                        'CHAIN="${HERE}/core/write_chains.py"')
    found = _walks(scripts={"daily_offsite.sh": text}, recipes={})["daily_offsite.sh"].found
    assert found.get("core/write_chains.py") == "daily_offsite.sh"

    ark = (DEPLOY / "ark_ship.sh").read_text().replace(
        'verify_in_container "$ARK_DIR" "$FREEZER"',
        'verify_in_container "$ARK_DIR" "$REPO/core/write_chains.py"')
    found = _walks(scripts={"ark_ship.sh": ark}, recipes={})["ark_ship.sh"].found
    assert found.get("core/write_chains.py") == "ark_ship.sh"

    # Inside the DOCKER_RUN array every container of the Ark starts from.
    ark = (DEPLOY / "ark_ship.sh").read_text().replace(
        "DOCKER_RUN=(docker run --rm --pull never",
        'DOCKER_RUN=(docker run --rm --pull never -v "$REPO/core/write_chains.py:/w.py:ro"')
    assert ark != (DEPLOY / "ark_ship.sh").read_text()
    found = _walks(scripts={"ark_ship.sh": ark}, recipes={})["ark_ship.sh"].found
    assert found.get("core/write_chains.py") == "ark_ship.sh"


@pytest.mark.parametrize("line, words", [
    ('-v "$PWD/core:/app/core:ro"', "mounts core "),
    ('-v "${PWD}:/app:ro"', "the checkout itself"),
    ('-v "$PWD/scripts/compact_duckdb.sh:/x:ro"', "mounts scripts/compact_duckdb.sh"),
], ids=["directory", "root", "not-python"])
def test_a_checkout_directory_or_other_file_is_a_problem(line, words):
    """A directory of main's code mounted over the image's would run it all
    with no import held to anything. Mutation: accept a directory."""
    problems = _walks(scripts={"daily_offsite.sh": _with_line(line)},
                      recipes={})["daily_offsite.sh"].problems
    assert any(words in p for p in problems), problems


def test_a_source_the_walk_cannot_resolve_is_outside_and_unlisted():
    """`$(dirname "$0")/../core/x.py` is the checkout too, in a spelling the
    walk does not resolve: it is reported as written, and `OUTSIDE` does not
    hold it, so the mounts test fails on it."""
    line = '-v "$(dirname "$0")/../core/write_chains.py:/app/core/write_chains.py:ro"'
    walks = _walks(scripts={"daily_offsite.sh": _with_line(line)}, recipes={})
    unlisted = {e for e in walks["daily_offsite.sh"].outside
                if ("daily_offsite.sh", e) not in OUTSIDE}
    assert unlisted, walks["daily_offsite.sh"]


def test_every_recipe_names_the_pinned_image():
    """The two docstrings a person copies a command from. Mutation: put
    :latest back in either."""
    for path in ("deploy/restore_from_export.py", "deploy/duckdb_table_fates_check.py"):
        doc = ast.get_docstring(ast.parse((REPO / path).read_text())) or ""
        assert PINNED in doc and "--pull never" in doc, path
        assert "keycrm-web:latest" not in doc, path


def _imports(tree: ast.AST, *, module_level_only: bool):
    """(module, names, aliases) for every import, with the alias each binds
    a module to. Module level is what the interpreter executes on import:
    the body, and the bodies of `if`/`try` around it."""
    nodes = []
    if module_level_only:
        pending = list(tree.body)
        while pending:
            node = pending.pop()
            nodes.append(node)
            if isinstance(node, (ast.If, ast.Try)):
                pending += node.body + node.orelse + getattr(node, "finalbody", [])
                pending += [n for h in getattr(node, "handlers", []) for n in h.body]
    else:
        nodes = list(ast.walk(tree))
    for node in nodes:
        if isinstance(node, ast.Import):
            for a in node.names:
                yield a.name, (), a.asname or a.name.split(".")[0]
        elif isinstance(node, ast.ImportFrom) and node.level == 0:
            yield node.module, tuple(a.name for a in node.names), None
        elif (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
              and node.func.attr == "import_module" and node.args
              and isinstance(node.args[0], ast.Constant)):
            yield node.args[0].value, (), None


def _top_level_names(path: str) -> set:
    tree = ast.parse((REPO / path).read_text())
    names = set()
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            names |= {t.id for t in node.targets if isinstance(t, ast.Name)}
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
        elif isinstance(node, ast.ImportFrom):
            names |= {a.asname or a.name for a in node.names}
    return names


def _allowed(module: str, names, beside: dict) -> "str | None":
    """None when the pinned image (with the files mounted beside) has it."""
    top = module.split(".")[0]
    if top == "__future__" or top in sys.stdlib_module_names or module in THIRD_PARTY:
        return None
    if module in beside:
        missing = set(names) - beside[module]
        return f"{module} has no {sorted(missing)}" if missing else None
    if module in PINNED_IMAGE:
        missing = set(names) - PINNED_IMAGE[module]
        return f"the pinned image's {module} has no {sorted(missing)}" if missing else None
    if module == "core":   # `from core import x` is the submodule core.x
        missing = [n for n in names if f"core.{n}" not in PINNED_IMAGE
                   and f"core.{n}" not in beside]
        return f"the pinned image has no core.{missing}" if missing else None
    return f"{module} is not in the pinned image"


@pytest.mark.parametrize("path", sorted(MOUNTED))
def test_a_mounted_file_imports_only_what_the_pinned_image_carries(path):
    """Mutation: import a module main gained after the pinned image — or one
    step 14 is about to delete — into any mounted file, or use a
    `compact_duckdb` name the pinned image's does not define."""
    tree = ast.parse((REPO / path).read_text())
    where, mode = MOUNTED[path]
    beside = {_module_name(p): _top_level_names(p) for p, (w, _) in MOUNTED.items()
              if w == where and p != path}
    problems, modules = [], {}
    for module, names, alias in _imports(tree, module_level_only=mode == "module"):
        why = _allowed(module, names, beside)
        if why:
            problems.append(why)
        if alias:
            modules[alias] = module
        if module == "core":
            modules.update({n: f"core.{n}" for n in names})
        for n in names:
            if f"{module}.{n}" in PINNED_IMAGE:
                modules[n] = f"{module}.{n}"
    # `compact.phase2_import` and the like: an attribute of a module bound
    # above must be one the pinned image's module defines.
    for node in ast.walk(tree):
        if (isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name)
                and modules.get(node.value.id) in PINNED_IMAGE
                and node.attr not in PINNED_IMAGE[modules[node.value.id]]):
            problems.append(f"the pinned image's {modules[node.value.id]} has no {node.attr}")
    assert not problems, (path, problems)


def test_the_walk_sees_what_it_guards():
    """If the walk found nothing to hold, every file above would pass by
    reading nothing."""
    seen = set()
    for path, (_, mode) in MOUNTED.items():
        tree = ast.parse((REPO / path).read_text())
        seen |= {m for m, _, _ in _imports(tree, module_level_only=mode == "module")}
    assert {"compact_duckdb", "core.duckdb_switch", "core.snapshot_validation",
            "core.duckdb_table_fates", "duckdb"} <= seen, seen
    assert _allowed("core.pg_orders_write", ("x",), {}) is not None
    assert _allowed("compact_duckdb", ("phase9",), {}) is not None
    assert _allowed("pandas", (), {}) is not None
