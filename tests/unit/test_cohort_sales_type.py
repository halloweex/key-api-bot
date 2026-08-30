"""The cohort tab now means the same thing by "retail" as every other tab.

Five customer-analytics methods used to re-derive `sales_type` from
`manager_id` against the RETAIL_MANAGER_IDS constant, instead of reading the
column Silver materialises. That is charter rule 1's prohibition — a rule with
two homes — and this copy had gone stale in two separate ways:

* it required `source_id = 4` for a manager-less order, where Silver requires
  nothing at all;
* it read the *constant* list rather than `managers.is_retail`, which is the
  column a human edits when they classify a manager — so a classification made
  through `POST /api/managers/{id}/retail-status` never reached these five.

Measured on production Silver, non-return orders: **1,781 were retail to every
other tab and invisible here, and 175 were the other way round**, out of
45,709. The cohort tab was describing a different population than the revenue
tab beside it.

The column is also the only spelling that survives a third engine: a manager
list rendered into SQL has to be rendered three ways, a column does not.
"""
from __future__ import annotations

import ast
import inspect

import pytest

from core.repositories import customers as module

FIVE = (
    "get_cohort_retention",
    "get_enhanced_cohort_retention",
    "get_days_to_second_purchase",
    "get_cohort_ltv",
    "get_at_risk_customers",
)


def _statements(name: str) -> str:
    """One method's source with its `--` and `#` commentary removed.

    Parsed and stripped rather than grepped: every one of these methods now
    *explains* the old rule in a comment, and a grep for the constant would be
    satisfied by the explanation — [[feedback_assert_on_structure_not_prose]],
    which this repository has now paid for three times in one day.
    """
    src = inspect.getsource(module)
    tree = ast.parse(src)
    lines = src.splitlines()
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == name:
            body = lines[node.lineno - 1:node.end_lineno]
            return "\n".join(
                line.split("--")[0].split("#")[0] for line in body
            )
    raise AssertionError(f"{name} not found")


@pytest.mark.parametrize("name", FIVE)
def test_it_reads_the_column_silver_materialised(name):
    code = _statements(name)
    assert "o.sales_type" in code, f"{name} does not filter on the Silver column"


@pytest.mark.parametrize("name", FIVE)
def test_it_no_longer_re_derives_the_rule(name):
    code = _statements(name)
    for stale in ("RETAIL_MANAGER_IDS", "B2B_MANAGER_ID", "manager_id IN"):
        assert stale not in code, (
            f"{name} still carries a second home for the sales_type rule: {stale}"
        )


def test_the_constants_are_no_longer_imported_here():
    """They belong to the warehouse projection and to manager seeding, not to
    a read path. Leaving the import invites the next copy."""
    src = inspect.getsource(module)
    assert "RETAIL_MANAGER_IDS" not in src.split("\n\n")[0]
    tree = ast.parse(src)
    imported = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        for alias in node.names
    }
    assert "RETAIL_MANAGER_IDS" not in imported
    assert "B2B_MANAGER_ID" not in imported


@pytest.mark.parametrize("name", FIVE)
def test_all_still_means_no_filter(name):
    """`sales_type=all` spans every category, including `internal`, and must
    not quietly become a predicate."""
    code = _statements(name)
    assert 'if sales_type == "all"' in code or '"" if sales_type == "all"' in code
