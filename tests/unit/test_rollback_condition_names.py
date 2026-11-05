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

S14-00 deleted the second with `core/duckdb_off.py`; the first and the third
were computed by this code until S14-14, and the literal was held equal to
them while they stood. The four `shadow_*` names went first: S14-10 deleted
`compare_shadow`, their one emitter, with their REGISTRY and REMEDIATION
entries. S14-14 deleted the DuckDB integrity scan, the emitter of all the
rest, with the two sets' sources, so from S14-14 on none of these is in
REGISTRY, has a REMEDIATION line or a human name, or is emitted at all —
and the literal keeps them.
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
    def test_the_sources_are_gone_and_the_mark_stays(self):
        """The two sets' sources went with the DuckDB scan in S14-14; the
        literal is what they computed. `COMPARISON_MARK` stays until S14-15,
        and is one of the marks the images before step 14 read as retired."""
        from core import pg_orders_write, warehouse_cutover

        assert not hasattr(warehouse_cutover, "retired_conditions")
        assert not hasattr(pg_orders_write, "retired_conditions")
        assert warehouse_cutover.COMPARISON_MARK in ROLLBACK_MARKS

    def test_the_retired_group_is_paged_by_nothing(self):
        """`dq:reconciliation` was the group of the 05:30 job's DuckDB arm
        (`pg_orders_write.RECONCILIATION_GROUP`), which stood down from chain
        3's flip; step 14's S14-12 deleted the arm, its layer and the name.
        Nothing delivers a page under the group: no watched layer is
        `reconciliation`, and no condition the REGISTRY holds names it.
        Mutation: give the layer back to `WATCHED_LAYERS`."""
        from core import pg_orders_write
        from core.alerting import REGISTRY
        from core.data_quality import WATCHED_LAYERS

        (group,) = ROLLBACK_RETIRED_GROUPS
        layer = group.split(":", 1)[1]
        assert layer not in WATCHED_LAYERS
        assert not [k for k in REGISTRY if k.endswith(f":{layer}")]
        assert not hasattr(pg_orders_write, "RECONCILIATION_GROUP")

    def test_no_retired_name_is_registered_or_emitted(self):
        """Rule 15 from S14-14 on: every name the images before step 14
        retire is out of REGISTRY, has no REMEDIATION line of its own and no
        human name, and no module in `core/`, `web/` or `bot/` hands it to a
        `check_name=`. Mutation: put back any DuckDB check, or one of their
        REGISTRY entries."""
        from core.alerting import REGISTRY
        from core.data_quality import HUMAN_CHECK_NAMES, REMEDIATION

        emitted = set().union(*(_check_names_in(path)
                                for tree in ("core", "web", "bot")
                                for path in sorted((ROOT / tree).rglob("*.py"))))
        assert not ALL_RETIRED & emitted, sorted(ALL_RETIRED & emitted)
        assert not ALL_RETIRED & set(REGISTRY), sorted(ALL_RETIRED & set(REGISTRY))
        assert not ALL_RETIRED & {prefix for prefix, _ in REMEDIATION}
        assert not ALL_RETIRED & set(HUMAN_CHECK_NAMES)

    def test_no_generated_family_can_build_one(self):
        """The scan named four families from (table, column) call sites —
        `pk_uniqueness_*`, `fk_orphan_*`, `not_null_*`, `value_domain_*` — and
        every retired name of theirs came from one. No REGISTRY key and no
        REMEDIATION prefix starts a family any more, so none can come back by
        a generated name. Mutation: register `pk_uniqueness_offers`."""
        from core.alerting import REGISTRY
        from core.data_quality import REMEDIATION

        families = ("pk_uniqueness_", "fk_orphan_", "not_null_", "value_domain_")
        assert not [k for k in REGISTRY if k.startswith(families)]
        assert not [p for p, _ in REMEDIATION if p.startswith(families)]

    def test_offs_set_had_the_scans_three_pk_checks_and_compare_shadows_four(self):
        """`core/duckdb_off.py`'s set: the scan's three stood-down pk checks
        (`DUCKDB_OFF_STOOD_DOWN`, deleted in S14-14) and `compare_shadow`'s
        four conditions (S14-10)."""
        off = ROLLBACK_RETIRED["duckdb_off.retired_conditions"]
        assert {c for c in off if c.startswith("pk_uniqueness_")} == {
            "pk_uniqueness_products", "pk_uniqueness_buyers", "pk_uniqueness_categories"}
        assert len({c for c in off if c.startswith("shadow_")}) == 4


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
        names.add(twins.WHOLE_UNWATCHED)
        names |= set(twins.LANDING_TWINS)
        assert not names & ALL_RETIRED

    def test_the_canary_pages_under_none(self):
        """The canary's own keys — S14-00 added `backup_evidence_stale` —
        are none of them either."""
        from bot import canary

        keys = {key for key, _action in canary._ACTIONS}
        assert not keys & ALL_RETIRED
        assert "backup_evidence_stale" in keys
