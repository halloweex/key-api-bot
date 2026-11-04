"""Step 14's S14-04: the way back is gone, and every lever that named it says so.

`scripts/chain_copy_back.py`, its handover (`core/chain_transfer.py`) and the
lever journal (`core/lever_journal.py`) were the one way to hand a latched
chain back to DuckDB. Under `KS_DUCKDB=off` none of it could run, and since
S14-00 nothing in a step-14 image routes by the latch anyway; the image from
before step 14 carries its own copy-back if it is ever wanted. So the pages,
findings, health description and soak details that named the script as the
cure name one line instead, `core.rollback_lever.ROLLBACK_LEVER`: investigate
with `pg_chain_invariants`, and roll back to `rollback-step14` only after the
markers are checked against the owner rows.

What is held here, each with the mutation it must kill:

- the three modules are gone and nothing imports them, nor the names that
  existed only for them;
- no text a person reads still names the copy-back (`chain_copy_back`, its
  `--handover`) — docstrings and comments may tell the history;
- every lever that named it names `ROLLBACK_LEVER` now, in Python and in the
  soak's SQL and shell, and fits the line it is printed on;
- no condition, REGISTRY entry or canary key went with the texts (rule 15:
  names are neither renamed nor reused);
- the page and the daily digest still render with the new line.
"""
from __future__ import annotations

import ast
import importlib
import re
from pathlib import Path

import pytest

from core.rollback_lever import ROLLBACK_LEVER, ROLLBACK_RUNBOOK
from tests.repo_tree import needs_checkout

ROOT = Path(__file__).resolve().parents[2]

DELETED_MODULES = ("core.chain_transfer", "core.lever_journal")
DELETED_FILES = ("core/chain_transfer.py", "core/lever_journal.py",
                 "scripts/chain_copy_back.py")

# What a person may no longer be told to run. `handover` alone is a word the
# soak uses for the flip's moment and stays; `--handover` is the script's flag.
GONE = re.compile(r"chain_copy_back|--handover|chain_transfer|lever_journal")

# The conditions whose lever this PR rewrote — REMEDIATION, the registry's
# `clears` and the canary's actions. Literal on purpose: a later PR that drops
# one of them must say so here, beside rule 15's literal.
REMEDIATION_REWRITTEN = ("chain_latch_disagrees",)
# `write_chain_flag_mismatch` was in both until step 14's S14-11 deleted the
# condition with its emitter, the flags (rule 8; `test_write_flags_are_not_read`
# holds that it is gone everywhere). `order_owner_row_without_marker`,
# `owner_row_without_marker`, `chain_owner_unregistered` (both lists) and
# `chain_shipper_overwrote` (the second) went in S14-12 with their emitters,
# the daily comparisons against DuckDB (rule 8); `chain_latch_disagrees` moved
# to the chain invariants with its lever (decision 12).
CLEARS_REWRITTEN = ("warehouse_way_back_refused",)
CANARY_REWRITTEN = ("warehouse_way_back_refused",)
# Rewritten too, to say what is true without the copy-back, with no lever of
# their own about a chain's ownership. `shadow_missing_in_duckdb` was the one
# until step 14's S14-10 deleted it with `compare_shadow`, its emitter
# (rule 15's literal in `test_rollback_condition_names.py` keeps the name).
TOUCHED_ONLY: tuple = ()
# `mirror_stood_down`'s `clears` said the chain handing its tables back to
# DuckDB clears it, and was rewritten without a lever (the S14-04 review); the
# condition went in S14-12 with the comparisons that filed it.
CLEARS_REWORDED: tuple = ()

# What GONE cannot see: the copy-back in prose — "copied back to DuckDB",
# "hands its tables back" — with no script named (the S14-04 review found two
# `clears` saying so). Allowed only negated in its own clause, as in
# "nothing in this build hands the tables back to DuckDB".
HANDED_BACK = re.compile(
    r"\bcop(?:y|ies|ied)(?:-| )back\b"
    r"|\b(?:cop(?:y|ies|ied)|hands?|handed|carr(?:y|ies|ied))\b(?: [\w'-]+){1,4}"
    r" back to DuckDB\b"
    r"|\bhand(?:s|ed)? (?:[\w'-]+ ){0,2}chains? back\b", re.I)
NEGATION = re.compile(r"\b(?:nothing|never|no)\b", re.I)

# The soak's lost-marker details: the file and how many times its detail
# names the lever (one per state that can lose a marker).
SOAK_DETAILS = {
    "15_i1_inventory_copy_stood_down.sql": 1,
    "23_b1_buyers_copies_stood_down.sql": 2,
    "30_m1_managers_copy_stood_down.sql": 2,
    "32_o1_orders_copies_stood_down.sql": 2,
}

# A line a page prints: the charter's one-line imperative
# (`test_alert_remediation.ARROW_LINE_MAX`, `test_canary`).
LINE_MAX = 150


def _py_files(*tops):
    for top in tops:
        yield from sorted((ROOT / top).rglob("*.py"))


def _docstring_nodes(tree) -> set:
    out = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef,
                             ast.AsyncFunctionDef)):
            body = node.body
            if (body and isinstance(body[0], ast.Expr)
                    and isinstance(body[0].value, ast.Constant)
                    and isinstance(body[0].value.value, str)):
                out.add(id(body[0].value))
    return out


def _texts(path: Path):
    """Every string a module can say — not its docstrings."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    docs = _docstring_nodes(tree)
    for node in ast.walk(tree):
        if (isinstance(node, ast.Constant) and isinstance(node.value, str)
                and id(node) not in docs):
            yield node.lineno, node.value


def _says_handed_back(text: str) -> list:
    """Every HANDED_BACK phrase in `text` not negated in its own clause."""
    return [m.group(0) for m in HANDED_BACK.finditer(text)
            if not NEGATION.search(re.split(r"[.;:]", text[:m.start()])[-1])]


def _uncommented(text: str, marker: str) -> str:
    return "\n".join(line.split(marker, 1)[0] if line.lstrip().startswith(marker)
                     else line for line in text.splitlines())


# ─── gone, and nobody asks for it ────────────────────────────────────────────


class TestTheWayBackIsGone:
    @pytest.mark.parametrize("path", DELETED_FILES)
    def test_the_file_is_gone(self, path):
        assert not (ROOT / path).exists(), path

    @pytest.mark.parametrize("module", DELETED_MODULES)
    def test_the_module_cannot_be_imported(self, module):
        with pytest.raises(ModuleNotFoundError):
            importlib.import_module(module)

    def test_nothing_imports_them(self):
        """Mutation: put `from core import chain_transfer` back anywhere,
        lazily inside a function included."""
        found = []
        for path in _py_files("core", "web", "bot", "scripts", "deploy", "tests"):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                names = []
                if isinstance(node, ast.ImportFrom) and node.module:
                    names = [node.module] + [f"{node.module}.{a.name}" for a in node.names]
                elif isinstance(node, ast.Import):
                    names = [a.name for a in node.names]
                if any(n in DELETED_MODULES for n in names):
                    found.append(f"{path.relative_to(ROOT)}:{node.lineno}")
        assert not found, found

    def test_the_names_that_existed_only_for_it_are_gone(self):
        """`land_pending` was the copy-back's first write, `land_in_duckdb`
        the `duckdb` gate's landing of a spool for it, the rest declarations
        only the copy-back read. Mutation: leave one behind."""
        from core import (
            mirror_reconciliation, pg_catalogue_write, pg_managers_write,
            pg_replication, pg_traffic_ledger_write, pg_weekly_ledger_write,
            report_ledger,
        )

        leftovers = [
            (module.__name__, name) for module, name in (
                (pg_weekly_ledger_write, "land_pending"),
                (pg_traffic_ledger_write, "land_pending"),
                (report_ledger, "land_in_duckdb"),
                (pg_catalogue_write, "CHAIN_RELEASED_KEYS"),
                (pg_managers_write, "CHAIN_COPY_BACK_OWES_FULL_REBUILD"),
                (pg_replication, "REPLICATED_SHAPES"),
                (mirror_reconciliation, "MIRRORED_LANDING_TABLES"),
                (mirror_reconciliation, "REPLICATED_TABLES"),
                (mirror_reconciliation, "ORDERS_SPEC"),
                (mirror_reconciliation, "ORDER_PRODUCTS_SPEC"),
            ) if hasattr(module, name)]
        assert not leftovers, leftovers

    # `test_a_spooled_week_still_counts_as_sent_without_the_landing` read the
    # gate's `duckdb` branch for its spool check after S14-04 took the
    # landing; S14-10 deleted the branch. The gate's one path counts a spooled
    # week as sent under every flag: `test_report_ledger.py`,
    # `TestASpoolOutlivesItsFlag`.


# ─── nothing a person reads names it ─────────────────────────────────────────


class TestNoTextNamesTheCopyBack:
    def test_no_python_text_names_it(self):
        """Every string `core/`, `web/`, `bot/`, `scripts/` and `deploy/` can
        say — a page, a finding, a log line, a 409, a health description.
        Docstrings may tell the history. Mutation: put back
        "else scripts/chain_copy_back.py" in any lever."""
        found = [f"{path.relative_to(ROOT)}:{line}: {text[:80]!r}"
                 for path in _py_files("core", "web", "bot", "scripts", "deploy")
                 for line, text in _texts(path) if GONE.search(text)]
        assert not found, found

    def test_no_soak_detail_or_note_names_it(self):
        """The SQL's details and the shell's notes, comments aside. P2 reads
        the lever journal's rows as history and names no script. Mutation:
        put back "Restore the marker, else scripts/chain_copy_back.py X"."""
        found = []
        for path in sorted((ROOT / "deploy").rglob("*.sql")):
            code = _uncommented(path.read_text(encoding="utf-8"), "--")
            found += [f"{path.name}: {m.group(0)}" for m in GONE.finditer(code)]
        for path in sorted((ROOT / "deploy").rglob("*.sh")):
            code = _uncommented(path.read_text(encoding="utf-8"), "#")
            found += [f"{path.name}: {m.group(0)}" for m in GONE.finditer(code)]
        assert not found, found

    def test_the_guard_sees_what_it_forbids(self, tmp_path):
        """A lever split over two literals is one string to the parser, and
        a docstring saying the same is history, not a lever."""
        path = tmp_path / "probe.py"
        path.write_text('"""scripts/chain_copy_back.py was the way back."""\n'
                        'X = ("Restore the marker, else "\n'
                        '     "scripts/chain_copy_back.py orders")\n', encoding="utf-8")
        assert [t for _, t in _texts(path) if GONE.search(t)] == [
            "Restore the marker, else scripts/chain_copy_back.py orders"]
        assert GONE.search("--handover") and not GONE.search("since the handover")

    def test_no_lever_says_a_chain_is_handed_back(self):
        """Every `clears` in the alert registry, every canary action and
        title, every REMEDIATION line: none says a chain is copied or handed
        back to DuckDB, which nothing in this build does — unless it says
        that nothing does. Mutation: put back "web restarts with the named
        write chains copied back to DuckDB" as `warehouse_way_back_refused`'s
        `clears`."""
        from bot import canary
        from core.alerting import REGISTRY
        from core.data_quality import REMEDIATION

        texts = ([(f"clears:{k}", v.clears) for k, v in REGISTRY.items()]
                 + [(f"canary:{k}", a) for k, a in canary._ACTIONS]
                 + [(f"title:{k}", a) for k, a in canary._CRITICAL_TITLES]
                 + [(f"remediation:{k}", a) for k, a in REMEDIATION])
        found = [(where, _says_handed_back(text)) for where, text in texts
                 if _says_handed_back(text or "")]
        assert not found, found

    @pytest.mark.parametrize("text,said", [
        # The texts the S14-04 review found, as they stood.
        ("web restarts with the named write chains copied back to DuckDB, or "
         "with KS_WRITE_WAREHOUSE=postgres and every precondition met", True),
        ("the chain hands its tables back to DuckDB, or its flag returns "
         "to duckdb before it ever latched", True),
        # And the base's `write_chain_flag_mismatch`, which GONE also caught.
        ("the variable is set back to postgres, or scripts/chain_copy_back.py "
         "hands the tables back to DuckDB", True),
        ("the variable is set back to postgres; nothing in this build hands "
         "the tables back to DuckDB.", False),
        ("nothing repairs the rows it replaced and nothing hands the chain "
         "back, so a human decides", False),
        ("the copy reads back equal", False),
    ])
    def test_the_prose_guard_sees_what_it_forbids(self, text, said):
        assert bool(_says_handed_back(text)) is said, text


# ─── every lever that named it names the one line ────────────────────────────


class TestEveryLeverNamesTheOneLine:
    def test_the_line_says_what_the_plan_says_and_fits(self):
        """Investigate first, the image last, and the check between them —
        for a lost marker the rollback is exactly the dangerous step."""
        assert "\n" not in ROLLBACK_LEVER and len(ROLLBACK_LEVER) <= 121
        assert ROLLBACK_LEVER.startswith("Investigate with pg_chain_invariants")
        assert ROLLBACK_LEVER.index("pg_chain_invariants") < ROLLBACK_LEVER.index(
            "rollback-step14") < ROLLBACK_LEVER.index("owner rows")
        assert "only after checking the markers" in ROLLBACK_LEVER

    @needs_checkout
    def test_the_runbook_it_names_exists(self):
        """Mutation: rename the CLAUDE.md heading, or the constant's."""
        heading = re.search(r'"(.+)"', ROLLBACK_RUNBOOK).group(1)
        claude = (ROOT / ".claude" / "CLAUDE.md").read_text(encoding="utf-8")
        assert f"\n### {heading}\n" in claude
        assert ROLLBACK_RUNBOOK.startswith(".claude/CLAUDE.md")

    @pytest.mark.parametrize("condition", REMEDIATION_REWRITTEN)
    def test_the_remediation_line(self, condition):
        from core.data_quality import remediation_for

        (line,) = remediation_for([condition])
        assert line.startswith(ROLLBACK_LEVER), line
        assert len(line) <= LINE_MAX and "\n" not in line

    @pytest.mark.parametrize("condition", CLEARS_REWRITTEN)
    def test_what_clears_it(self, condition):
        from core.alerting import REGISTRY

        assert ROLLBACK_LEVER in REGISTRY[condition].clears

    @pytest.mark.parametrize("key", CANARY_REWRITTEN)
    def test_the_canary_action(self, key):
        from bot import canary

        action = dict(canary._ACTIONS)[key]
        assert ROLLBACK_LEVER in action and len(action) <= LINE_MAX, action

    @pytest.mark.parametrize("name,count", sorted(SOAK_DETAILS.items()))
    def test_the_soak_detail(self, name, count):
        """The SQL cannot import the constant, so it spells it; this holds
        the spelling. Mutation: reword one of them."""
        sql = (ROOT / "deploy" / "stage4_soak" / name).read_text(encoding="utf-8")
        assert _uncommented(sql, "--").count(ROLLBACK_LEVER) == count, name

    def test_the_soak_script_notes(self):
        script = (ROOT / "deploy" / "stage4_soak.sh").read_text(encoding="utf-8")
        assert _uncommented(script, "#").count(ROLLBACK_LEVER) == 4

    def test_the_finding_that_names_it(self):
        """`chain_latch_disagrees` as the chain invariants build it (since
        step 14's S14-12; the comparison's two owner-rows-only findings went
        with it): the runbook, the marker's content, and no way back."""
        from core import pg_chain_invariants as inv

        (issue,) = inv.check_chain_invariants(inv.Facts(
            watched=("pg_orders_write",), latch=inv.Latch(lost=(inv.LostMarker(
                chain="pg_orders_write", claimed=4, tables=4,
                since="2026-10-15T08:04:08.775536+00:00"),))))
        assert issue.check_name == "chain_latch_disagrees"
        text = issue.description
        assert ROLLBACK_RUNBOOK in text and not GONE.search(text), text
        assert "latched_at = 2026-10-15T08:04:08.775536+00:00" in text


# ─── no condition went with the texts ────────────────────────────────────────


class TestNoConditionWentWithTheTexts:
    @pytest.mark.parametrize("condition", sorted(
        set(REMEDIATION_REWRITTEN) | set(CLEARS_REWRITTEN) | set(CANARY_REWRITTEN)
        | set(TOUCHED_ONLY) | set(CLEARS_REWORDED)))
    def test_still_registered_as_a_condition(self, condition):
        """Rule 15: a lever changed, never a name. Mutation: drop or rename
        the entry with its text."""
        from core.alerting import REGISTRY, Kind

        assert REGISTRY[condition].kind is Kind.CONDITION

    @pytest.mark.parametrize("key", CANARY_REWRITTEN)
    def test_still_a_canary_key(self, key):
        from bot import canary

        assert key in {k for k, _ in canary._ACTIONS}

    @pytest.mark.parametrize("condition", REMEDIATION_REWRITTEN + TOUCHED_ONLY)
    def test_still_a_remediation_entry_of_its_own(self, condition):
        from core.data_quality import REMEDIATION

        assert condition in {prefix for prefix, _ in REMEDIATION}


# ─── the page and the digest still render ────────────────────────────────────


class TestThePageAndTheDigestRender:
    def _issue(self, check_name, severity):
        from core.data_quality import IntegrityIssue

        return IntegrityIssue(check_name=check_name, table_name="pg_orders_write",
                              severity=severity, count=2,
                              description="described, never printed")

    def test_the_page_ends_on_the_one_line(self):
        from core.data_quality import Severity, format_alert_message

        msg = format_alert_message(
            "integrity", Severity.CRITICAL,
            [self._issue("chain_latch_disagrees", Severity.CRITICAL)], [])
        assert msg.splitlines()[-1] == f"→ {ROLLBACK_LEVER} (CLAUDE.md)", msg
        assert not GONE.search(msg)

    def test_the_digest_renders_the_findings(self):
        from core.data_quality import DigestSection, build_digest

        rows = [{"check_name": name, "table_name": "pg_orders_write",
                 "severity": "WARN", "count": 1, "sample_ids": [],
                 "description": "x"} for name in REMEDIATION_REWRITTEN]
        msg = build_digest([DigestSection(
            layer="integrity",
            run={"run_id": 1, "status": "WARN",
                 "started_at": "2026-10-25T07:30:00+03:00"},
            issues=rows, age_hours=2)])
        assert msg is not None and not GONE.search(msg), msg
        from core.data_quality import human_check_name

        for name in REMEDIATION_REWRITTEN:
            assert human_check_name(name) in msg, (name, msg)
