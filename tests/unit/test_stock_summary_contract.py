"""What `/api/stocks/summary` returns, what the type declares, what is drawn.

THE DEFECT THIS EXISTS FOR

Three lists that should be one drifted apart and nobody could see it, because
each is in a different language and no test compared them.

    the endpoint returned   12 summary fields + an `outOfStock` list
    the TS interface        10 of those fields, and never the two cost sums
    the component drew      9 of them, and never the list

So `costValue` and `reserveCostValue` were computed on every request and were
not even *declarable* on the client — TypeScript could not have told anyone,
because a field absent from an interface is simply a field nobody asks for.
`totalOffers` and `averageQuantity` were declared and unread. The `outOfStock`
list carried its own query and twenty rows to no reader at all. All of it was
found by grep during a port, in 2026-09, which is the wrong instrument and the
wrong moment.

WHY IT IS CHECKED THIS WAY

Comparing Python to TypeScript by parsing both sounds fragile, and would be if
it were guessing. It is not: the response is a dict literal at the end of one
method, and the interface is a flat block of `name: type` lines. Both are read
structurally — the Python through `ast`, so a key mentioned in a comment or a
docstring cannot satisfy it, and the TS by walking one brace-delimited block.
If either shape ever stops being that simple, this test fails loudly rather
than passing vacuously, and the guard tests below are what make that true.

WHAT IS DELIBERATELY NOT ASSERTED

That the component reads every field. A card can be commented out for a week
without the API being wrong, and a test that forbids it would be a test about
layout. The pair that must agree is the wire and its type; the component is
reported by the third test as information, not as a failure.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
REPOSITORY = REPO / "core" / "repositories" / "inventory.py"
TYPES = REPO / "web" / "frontend" / "src" / "types" / "api.ts"
COMPONENT = REPO / "web" / "frontend" / "src" / "components" / "StockSummaryChart.tsx"


def _returned_summary_keys() -> set[str]:
    """The keys of the `summary` dict `get_stock_summary` returns.

    From the AST, so prose naming a field cannot satisfy the comparison —
    [[feedback_assert_on_structure_not_prose]], which this repository has paid
    for more than once.
    """
    tree = ast.parse(REPOSITORY.read_text(encoding="utf-8"))
    method = next(
        n for n in ast.walk(tree)
        if isinstance(n, ast.AsyncFunctionDef) and n.name == "get_stock_summary"
    )
    returns = [n for n in ast.walk(method) if isinstance(n, ast.Return)]
    assert len(returns) == 1, f"expected one return, found {len(returns)}"
    payload = returns[0].value
    assert isinstance(payload, ast.Dict), "the response is no longer a dict literal"

    summary = next(
        (v for k, v in zip(payload.keys, payload.values)
         if isinstance(k, ast.Constant) and k.value == "summary"),
        None,
    )
    assert isinstance(summary, ast.Dict), "no `summary` dict literal in the response"
    return {k.value for k in summary.keys
            if isinstance(k, ast.Constant) and isinstance(k.value, str)}


def _returned_top_level_keys() -> set[str]:
    tree = ast.parse(REPOSITORY.read_text(encoding="utf-8"))
    method = next(
        n for n in ast.walk(tree)
        if isinstance(n, ast.AsyncFunctionDef) and n.name == "get_stock_summary"
    )
    payload = next(n for n in ast.walk(method) if isinstance(n, ast.Return)).value
    return {k.value for k in payload.keys
            if isinstance(k, ast.Constant) and isinstance(k.value, str)}


def _interface_block(name: str) -> str:
    src = TYPES.read_text(encoding="utf-8")
    start = src.index(f"export interface {name} {{")
    depth = 0
    for i in range(start, len(src)):
        if src[i] == "{":
            depth += 1
        elif src[i] == "}":
            depth -= 1
            if depth == 0:
                return src[start:i + 1]
    raise AssertionError(f"interface {name} is not brace-balanced")


_FIELD = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\??:", re.M)


def _declared_summary_fields() -> set[str]:
    """The fields of the nested `summary: { … }` object in the interface."""
    block = _interface_block("StockSummaryResponse")
    start = block.index("summary: {")
    depth = 0
    for i in range(start, len(block)):
        if block[i] == "{":
            depth += 1
        elif block[i] == "}":
            depth -= 1
            if depth == 0:
                inner = block[start + len("summary: {"):i]
                return set(_FIELD.findall(inner))
    raise AssertionError("the `summary` object is not brace-balanced")


def _declared_top_level_fields() -> set[str]:
    block = _interface_block("StockSummaryResponse")
    # Strip the nested object's *body* so its fields are not read as top-level
    # ones — cutting from the brace and not from the label, or `summary` itself
    # would vanish from the declared set and the comparison would report a
    # difference this parser had created.
    start = block.index("summary: {") + len("summary: ")
    depth = 0
    for i in range(start, len(block)):
        if block[i] == "{":
            depth += 1
        elif block[i] == "}":
            depth -= 1
            if depth == 0:
                outer = block[:start] + block[i + 1:]
                return set(_FIELD.findall(outer)) - {"StockSummaryResponse"}
    raise AssertionError("the `summary` object is not brace-balanced")


class TestTheGuardsOnTheGuard:
    """A parser that quietly matched nothing would make every test below pass.

    Each of these asserts the extraction found something recognisable, so a
    refactor that changes either shape breaks the check rather than blessing
    whatever it now reads.
    """

    def test_the_python_side_parses(self):
        keys = _returned_summary_keys()
        assert len(keys) >= 5, keys
        assert "inStockCount" in keys

    def test_the_typescript_side_parses(self):
        fields = _declared_summary_fields()
        assert len(fields) >= 5, fields
        assert "inStockCount" in fields

    def test_the_top_level_sides_parse(self):
        assert {"summary", "topByQuantity"} <= _returned_top_level_keys()
        assert {"summary", "topByQuantity"} <= _declared_top_level_fields()


class TestTheWireAndItsTypeAgree:
    def test_the_summary_fields_are_the_declared_ones(self):
        returned = _returned_summary_keys()
        declared = _declared_summary_fields()
        assert returned == declared, (
            f"only the server sends: {sorted(returned - declared)}; "
            f"only the client expects: {sorted(declared - returned)}"
        )

    def test_the_top_level_keys_are_the_declared_ones(self):
        returned = _returned_top_level_keys()
        declared = _declared_top_level_fields()
        assert returned == declared, (
            f"only the server sends: {sorted(returned - declared)}; "
            f"only the client expects: {sorted(declared - returned)}"
        )


class TestNothingIsComputedForNobody:
    """The reason the four fields survived so long: nothing connected the
    response to the page. This is that connection, and it is allowed to be
    imperfect — see the module docstring for what it deliberately does not
    police."""

    def test_every_summary_field_reaches_the_component(self):
        drawn = set(re.findall(r"data\.summary\.([A-Za-z0-9_]+)",
                               COMPONENT.read_text(encoding="utf-8")))
        assert drawn, "the component scan found nothing — it has been renamed"
        unread = _returned_summary_keys() - drawn
        assert not unread, (
            f"computed on every request and rendered nowhere: {sorted(unread)} — "
            f"either draw them or stop sending them"
        )

    def test_every_list_reaches_the_component(self):
        body = COMPONENT.read_text(encoding="utf-8")
        drawn = set(re.findall(r"data\.([A-Za-z0-9_]+)", body))
        unread = _returned_top_level_keys() - drawn - {"summary"}
        assert not unread, (
            f"lists queried and rendered nowhere: {sorted(unread)}"
        )
