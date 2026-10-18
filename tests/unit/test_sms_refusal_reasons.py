"""Every SMS refusal names a reason the page can translate, and every name is used.

The page turns `reason` into `sms.refusal.<reason>` in its reader's language
(`core/sms_refusals.py`). A reason raised outside the vocabulary would have no
sentence in any dictionary — the frontend falls back to the English `detail`,
which is the failure this exists to remove — and a name nothing raises is a
sentence three dictionaries carry for no screen.

Walks the modules rather than a list of call sites: a refusal added next month
in a new route is a call of the same three constructors, and is found by the
same walk.
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

from core.sms_refusals import REFUSAL_REASONS, SmsCampaignRefused

ROOT = Path(__file__).resolve().parents[2]
MODULES = [
    ROOT / "web" / "routes" / "api" / "customers.py",
    ROOT / "core" / "repositories" / "customers.py",
]

# constructor → index of the positional argument that is the reason
REASON_ARG = {"SmsRefusal": 2, "SmsCampaignRefused": 1, "_refusal": 1}

# The one call allowed to pass a reason it did not write: the route's
# `_refused`, which answers with the reason a `SmsCampaignRefused` from the
# store already carries — and that constructor is walked here too.
FORWARDERS = {"_refused"}


def _calls(tree: ast.AST):
    """Every call, with the name of the function it sits in."""
    def visit(node, inside):
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                yield from visit(child, child.name)
            else:
                if isinstance(child, ast.Call):
                    yield child, inside
                yield from visit(child, inside)
    yield from visit(tree, None)


def _raised_reasons() -> dict[str, list[str]]:
    found: dict[str, list[str]] = {}
    for path in MODULES:
        for node, inside in _calls(ast.parse(path.read_text(encoding="utf-8"))):
            if inside in FORWARDERS:
                continue
            name = getattr(node.func, "id", None) or getattr(node.func, "attr", None)
            if name not in REASON_ARG:
                continue
            index = REASON_ARG[name]
            arg = node.args[index] if len(node.args) > index else None
            if arg is None:
                for kw in node.keywords:
                    if kw.arg == "reason":
                        arg = kw.value
            where = f"{path.relative_to(ROOT)}:{node.lineno}"
            assert isinstance(arg, ast.Constant) and isinstance(arg.value, str), (
                f"{where}: the reason must be a literal, so this walk can read it"
            )
            found.setdefault(arg.value, []).append(where)
    return found


def test_every_raised_reason_is_in_the_vocabulary():
    unknown = {r: w for r, w in _raised_reasons().items() if r not in REFUSAL_REASONS}
    assert not unknown, (
        f"{unknown}: add them to core/sms_refusals.py REFUSAL_REASONS, and their "
        f"sentences to all three web/frontend/src/locales/*.json as sms.refusal.<reason>"
    )


def test_every_name_in_the_vocabulary_is_raised():
    unused = sorted(set(REFUSAL_REASONS) - set(_raised_reasons()))
    assert not unused, f"{unused} are named but nothing refuses with them"


def test_an_unknown_reason_cannot_be_constructed():
    """The walk reads literals; this holds the constructors to the list too."""
    with pytest.raises(KeyError):
        SmsCampaignRefused("no", "a_reason_nobody_translated")


def test_a_refusal_is_still_the_value_error_it_was():
    """Every caller that caught a ValueError from the store still catches it,
    and reads the same sentence."""
    e = SmsCampaignRefused("campaign 'x' is not frozen", "not_frozen", campaign="x")

    assert isinstance(e, ValueError)
    assert str(e) == "campaign 'x' is not frozen"
    assert (e.reason, e.figures) == ("not_frozen", {"campaign": "x"})
