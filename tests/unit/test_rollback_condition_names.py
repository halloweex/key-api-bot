"""The condition names the images before step 14 retire, frozen while one
of them is the rollback target (step 14's rule 15).

Every image before step 14 — the rollback target is `rollback-step14`, the
last image before step 14, pinned at S14-00's deploy, not 3.0.292 by number
— keeps sets of conditions it treats as "retired": a delivered page
under one of them holds its start. Step 13's switch waits for none to be
open (`retired_conditions_clear`), off's verdict likewise
(`retired_pages_clear`), and chain 3's flip (`landing_pages_clear`). So a page
this code delivers under such a name — a Postgres twin renamed to the DuckDB
check it replaced, a new check taking a free-looking name — would, on a
rollback, hold step 13 back (a full DuckDB rebuild), run web `on` (the file
opened, the week's hash broken) or hold chain 3 on DuckDB. The names are
therefore not reused and not renamed until the images before step 14 are
retired (Z3).

`ROLLBACK_RETIRED` is what those images compute, captured on 3.0.292's code
(`origin/main` 57fd7c22, which differs from it only in VERSION) and equal on
3.0.295's (`origin/main` d621618a, rechecked 2026-10-22) and on 3.0.298's
(`origin/main` 6ac33044, VERSION 3.0.299 by the post-deploy bump, rechecked
2026-10-25, with `ROLLBACK_RETIRED_GROUPS` and `ROLLBACK_MARKS`):

    warehouse_cutover.retired_conditions()   step 13's five checks + reconcile_gold
    duckdb_off.retired_conditions()          off's three pk checks + compare_shadow
    pg_orders_write.retired_conditions()     chain 3's ten landing checks

Since S14-00 this code still computes the first and the third, and the second
is deleted with `core/duckdb_off.py`; while a source stands, the literal must
equal it. From S14-14 on, none of these may be in REGISTRY or be emitted at
all — the DuckDB checks that emit them today go before that.
"""
from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

ROLLBACK_RETIRED = {
    "warehouse_cutover.retired_conditions": frozenset({
        "attribution_coverage_website", "gold_cell_values", "gold_missing_cells",
        "gold_orphan_cells", "goods_shipped_without_sale", "headline_vs_line_items",
        "silver_missing_rows", "silver_orphan_rows", "silver_row_values",
    }),
    "duckdb_off.retired_conditions": frozenset({
        "pk_uniqueness_buyers", "pk_uniqueness_categories", "pk_uniqueness_products",
        "shadow_duckdb_only_rows", "shadow_missing_in_duckdb", "shadow_pruned_rows",
        "shadow_row_values",
    }),
    "pg_orders_write.retired_conditions": frozenset({
        "fk_orphan_order_products_order_id", "not_null_orders_ordered_at",
        "not_null_orders_source_id", "not_null_orders_status_id",
        "orders_without_line_items", "pk_uniqueness_order_products",
        "pk_uniqueness_orders", "status_group_vs_return_list",
        "value_domain_orders_source_id", "value_domain_orders_status_id",
    }),
}
# A page under this group holds chain 3's flip before step 14 whatever its name.
ROLLBACK_RETIRED_GROUPS = frozenset({"dq:reconciliation"})
# The marks the images before step 14 set on delivered pages, and read as
# "retired".
ROLLBACK_MARKS = frozenset({"warehouse_comparison", "duckdb_history"})

ALL_RETIRED = frozenset().union(*ROLLBACK_RETIRED.values())


class TestTheLiteralIsTheRollbackImages:
    def test_step_13s_set_while_its_source_stands(self):
        from core import warehouse_cutover

        assert (warehouse_cutover.retired_conditions()
                == ROLLBACK_RETIRED["warehouse_cutover.retired_conditions"])
        assert warehouse_cutover.COMPARISON_MARK in ROLLBACK_MARKS

    def test_chain_3s_set_while_its_source_stands(self):
        from core import pg_orders_write

        assert (pg_orders_write.retired_conditions()
                == ROLLBACK_RETIRED["pg_orders_write.retired_conditions"])
        assert pg_orders_write.RECONCILIATION_GROUP in ROLLBACK_RETIRED_GROUPS

    def test_offs_set_is_what_its_two_sources_still_emit(self):
        """`core/duckdb_off.py` is gone (S14-00); its set was the scan's
        three stood-down pk checks and `compare_shadow`'s four conditions,
        and both are still emitted by the code that stays until S14-10 and
        S14-14."""
        from core.data_quality import DUCKDB_OFF_STOOD_DOWN

        off = ROLLBACK_RETIRED["duckdb_off.retired_conditions"]
        assert DUCKDB_OFF_STOOD_DOWN == {c for c in off if c.startswith("pk_uniqueness_")}
        shadow = {c for c in off if c.startswith("shadow_")}
        assert shadow <= _check_names_in(ROOT / "core" / "mirror_reconciliation.py")


def _check_names_in(path: Path) -> set:
    """Every literal handed to a `check_name=` keyword in `path`."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            for kw in node.keywords:
                if (kw.arg == "check_name" and isinstance(kw.value, ast.Constant)
                        and isinstance(kw.value.value, str)):
                    found.add(kw.value.value)
    return found


class TestNoCheckThatStaysTakesOne:
    def test_no_postgres_or_clickhouse_check_emits_a_retired_name(self):
        """The checks step 14 keeps — every `core/pg_*.py` and `core/ch_*.py`
        module — name none of their findings after a retired condition.
        Mutation: rename a twin to the DuckDB check it replaced
        (`pg_silver_missing_rows` → `silver_missing_rows`)."""
        offenders = {}
        for path in sorted((ROOT / "core").glob("*.py")):
            if path.name.startswith(("pg_", "ch_")):
                hits = _check_names_in(path) & ALL_RETIRED
                if hits:
                    offenders[path.name] = sorted(hits)
        assert offenders == {}

    def test_the_twins_conditions_are_their_own(self):
        """The conditions the twins report and hold under — the names their
        pages carry — are none of the retired."""
        from core import pg_warehouse_dq as twins

        names = {c for conditions in twins.GUARD_CONDITIONS.values() for c in conditions}
        names |= set(twins.UNWATCHED_NAMES.values())
        names |= {twins.WHOLE_UNWATCHED, twins.FLAG_INVALID}
        names |= {twin for _guard, _detail, twin in twins.LANDING_TWINS}
        assert not names & ALL_RETIRED

    def test_the_canary_pages_under_none(self):
        """The canary's own keys — S14-00 added `backup_evidence_stale` —
        are none of them either."""
        from bot import canary

        keys = {key for key, _action in canary._ACTIONS}
        assert not keys & ALL_RETIRED
        assert "backup_evidence_stale" in keys
