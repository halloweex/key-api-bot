"""A sale to the company's own legal entity is revenue nowhere (the owner's
decision of 2026-11-14), judged without a database.

The orders whose buyer is the company's second legal entity are goods moved
between our own two entities. They stay in Silver — the shipment is real —
and Silver's `is_active_source`, which every Gold measure in Postgres and
ClickHouse, the line level and the inventory views read, says "not revenue"
for them. The readers that read around that column (the goals' growth
history, the /expenses profit, the cohorts' membership, the subcategory
drill-down, the assistant's top products, the bot's live KeyCRM reports)
leave them out by the same constant. The Postgres half — Silver derived,
Gold in both grains, those readers against a real store, the two integrity
findings — is `tests/integration/test_own_entity_not_revenue.py`.

Each guard names the mutation that bites it.
"""
from __future__ import annotations

import ast
import asyncio
import re
from datetime import date
from pathlib import Path

import pytest

from core import duckdb_constants as constants
from core.duckdb_constants import (
    NOT_OWN_ENTITY_BUYER_IDS, OWN_ENTITY_BUYER_IDS, REVENUE_SOURCE_IDS, not_own_entity,
)
from core.sql_dialect import POSTGRES

ROOT = Path(__file__).resolve().parents[2]
OWN = OWN_ENTITY_BUYER_IDS[0]
CLAUSE = "(o.buyer_id IS NULL OR o.buyer_id NOT IN (17358))"


# ─── The constant and the predicate ─────────────────────────────────────────

class TestTheConstant:
    def test_it_names_the_entity_by_id_alone(self):
        assert isinstance(OWN_ENTITY_BUYER_IDS, tuple) and OWN_ENTITY_BUYER_IDS
        assert all(type(i) is int and i > 0 for i in OWN_ENTITY_BUYER_IDS)
        assert OWN_ENTITY_BUYER_IDS == (17358,)
        assert isinstance(NOT_OWN_ENTITY_BUYER_IDS, tuple)
        assert not set(OWN_ENTITY_BUYER_IDS) & set(NOT_OWN_ENTITY_BUYER_IDS)

    def test_the_id_has_one_home(self):
        """The id is written once, in `core/duckdb_constants.py`; everything
        else asks the constant. Mutation: a reader that spells `NOT IN
        (17358)` itself — the next card listed would miss it."""
        found = []
        for tree in ("core", "web", "bot", "scripts", "deploy", "migrations"):
            for path in sorted((ROOT / tree).rglob("*.py")):
                if str(OWN) in path.read_text(encoding="utf-8"):
                    found.append(str(path.relative_to(ROOT)))
        assert found == ["core/duckdb_constants.py"], found


class TestThePredicate:
    def test_it_is_null_safe(self):
        assert not_own_entity("s") == "(s.buyer_id IS NULL OR s.buyer_id NOT IN (17358))"
        assert not_own_entity("l", (5, 7)) == "(l.buyer_id IS NULL OR l.buyer_id NOT IN (5, 7))"

    def test_no_ids_is_true_never_an_empty_list(self):
        """`NOT IN ()` parses in no engine here."""
        assert not_own_entity("o", ()) == "TRUE"

    def test_it_reads_the_constant_at_call_time(self, monkeypatch):
        monkeypatch.setattr(constants, "OWN_ENTITY_BUYER_IDS", (11, 12))
        assert not_own_entity("o") == "(o.buyer_id IS NULL OR o.buyer_id NOT IN (11, 12))"

    def test_it_answers_true_or_false_never_null(self):
        """Executed, not read: Silver's column is NOT NULL and ClickHouse's a
        plain Bool. Mutation: drop `IS NULL OR` — the order with no buyer
        answers NULL and every rebuild would fail."""
        duckdb = pytest.importorskip("duckdb")
        con = duckdb.connect(":memory:")
        try:
            con.execute("CREATE TABLE o (buyer_id INTEGER)")
            con.execute("INSERT INTO o VALUES (NULL), (?), (5)", [OWN])
            got = con.execute(
                f"SELECT buyer_id, {not_own_entity('o')} FROM o ORDER BY buyer_id NULLS FIRST"
            ).fetchall()
        finally:
            con.close()
        assert got == [(None, True), (5, True), (OWN, False)]


# ─── Silver's rule, and Gold following it ───────────────────────────────────

def _is_active_source_expression(sql: str) -> str:
    head = sql.split(" AS is_active_source", 1)[0]
    return head.rsplit(" AS sales_type,", 1)[1].strip()


class TestTheSilverRule:
    def test_counts_as_revenue_is_a_revenue_source_and_not_ourselves(self):
        from core.warehouse_sql import silver_select_sql

        sql = silver_select_sql(POSTGRES)
        assert sql.count(" AS is_active_source") == 1
        sources = ", ".join(str(s) for s in REVENUE_SOURCE_IDS)
        assert _is_active_source_expression(sql) == (
            f"o.source_id IN ({sources})\n                AND {CLAUSE}")

    def test_with_no_entity_the_rule_is_what_it_was(self, monkeypatch):
        """With the list empty, the rendering is the one recorded before the
        decision, byte for byte. Mutation: render `AND TRUE` for an empty
        list, or move any other character of the projection."""
        from core.warehouse_sql import silver_select_sql
        from tests.warehouse_sql_57fd7c22 import RENDERINGS

        monkeypatch.setattr(constants, "OWN_ENTITY_BUYER_IDS", ())
        before = RENDERINGS["silver_select_sql(POSTGRES)"].replace(
            f"\n                AND {CLAUSE}", "")
        assert silver_select_sql(POSTGRES) == before
        assert "buyer_id NOT IN" not in before

    def test_every_rendering_that_derives_silver_carries_it(self):
        """The rebuild's statement and the row-values recompute say the rule
        in one text: a twin recomputing the old rule would call every
        own-entity row stale for ever."""
        from core import pg_silver, pg_warehouse_dq
        from core.warehouse_sql import silver_recompute_ctes

        for sql in (pg_silver.insert_sql(), silver_recompute_ctes(POSTGRES),
                    pg_warehouse_dq._row_values_sql()):
            assert sql.count(f"AND {CLAUSE} AS is_active_source") == 1

    def test_gold_reads_the_column_never_the_rule(self):
        """Every Gold measure, in Postgres and ClickHouse, follows
        `is_active_source`; none restates who the buyer is. Mutation: a
        measure that names `buyer_id NOT IN` — a second home for the rule."""
        from core import ch_silver, pg_gold
        from core.warehouse_sql import GOLD_MEASURES

        renderings = [*GOLD_MEASURES.values(), pg_gold.select_sql(), pg_gold.insert_sql(),
                      *ch_silver.derive_gold_sql()]
        for sql in renderings:
            assert str(OWN) not in sql and "NOT IN" not in sql, sql[:80]
        for expr in GOLD_MEASURES.values():
            assert "is_active_source" in expr

    def test_the_column_stays_in_both_copies_of_silver(self):
        from core import ch_silver, pg_silver

        assert "is_active_source" in pg_silver.SILVER_COLUMNS
        assert "is_active_source" in ch_silver.SILVER_COLUMNS
        assert re.search(r"is_active_source\s+Bool,", ch_silver._SILVER_DDL)


# ─── The readers that read around the column ────────────────────────────────

class TestTheGoalsHistory:
    """It reads around `is_active_source` on purpose (OQ-1: a retired
    source is still history), so the own entity is spelled out."""

    @pytest.mark.parametrize("sales_type", ["retail", "b2b", "internal", "exhibition", "all"])
    def test_every_sales_type_leaves_the_entity_out(self, sales_type):
        from core.repositories.goals import _silver_history_where

        clause, _ = _silver_history_where(sales_type)
        assert not_own_entity("s") in clause
        assert "is_active_source" not in clause


COHORT_BODIES = ("cohort_retention_select", "enhanced_cohort_retention_select",
                 "days_to_second_purchase_select", "cohort_ltv_select",
                 "at_risk_customers_select")


class TestTheCohorts:
    """Membership reads every source (O5), so it names the entity itself."""

    @pytest.mark.parametrize("name", COHORT_BODIES)
    def test_the_membership_leaves_the_entity_out(self, name):
        import core.sql_dialect as dialects
        from tests.unit.test_cohort_active_sources import _ctes

        sql = getattr(dialects, name)(dialects.CLICKHOUSE_ANALYTICS,
                                      sales_type_filter="", months_back=12)
        assert sql.count(f"AND {CLAUSE}") == 1, name
        (_first, body), *_rest = _ctes(sql).items()
        assert f"AND {CLAUSE}" in body, name

    @pytest.mark.parametrize("name", COHORT_BODIES)
    def test_the_entity_is_in_no_cohort(self, name):
        """Run on DuckDB with the fixture of the O5 test and one own-entity
        order beside it, `is_active_source` FALSE as Silver now derives it:
        every answer is the fixture's own. Mutation: drop the membership
        clause — the entity joins March's cohort, never retained, and every
        cohort size there reads 4."""
        pytest.importorskip("duckdb")
        from tests.unit.test_cohort_active_sources import EXPECTED, ORDERS, run_on_duckdb

        with_entity = ORDERS + [(301, OWN, "2026-03-15", 1, False, 9000)]
        assert run_on_duckdb(name, with_entity) == EXPECTED[name]


def _capture():
    seen: list = []

    async def run(sql, params=None, *args, **kwargs):
        seen.append(sql)
        return []
    return seen, run


class TestTheProfit:
    def test_the_profit_leaves_the_entity_out(self):
        """Its revenue and the costs joined to it. Mutation: drop the clause
        from `get_profit_analysis`'s WHERE."""
        from core.repositories.expenses import ExpensesMixin

        seen, run = _capture()

        class Probe(ExpensesMixin):
            _expenses_run = staticmethod(run)

        for sales_type in ("b2b", "all"):
            asyncio.run(Probe().get_profit_analysis(date(2026, 11, 1), date(2026, 11, 2),
                                                    sales_type=sales_type))
        assert len(seen) == 2
        for sql in seen:
            assert not_own_entity("s") in sql


class TestTheSubcategories:
    def test_the_drill_down_reads_the_column(self):
        from core.repositories.revenue import RevenueMixin

        seen, run = _capture()

        class Probe(RevenueMixin):
            _dashboard_run = staticmethod(run)

        asyncio.run(Probe().get_subcategory_breakdown(
            date(2026, 11, 1), date(2026, 11, 2), "Care", sales_type="b2b"))
        (sql,) = seen
        assert "l.is_active_source" in sql and "l.source_id IN (1, 2, 4)" in sql


class TestTheAssistant:
    def test_its_top_products_leave_the_entity_out(self, monkeypatch):
        from core import chat_tools

        seen, run = _capture()
        monkeypatch.setattr(chat_tools, "_chat_run", run)
        asyncio.run(chat_tools._get_top_products("yesterday", "revenue", 5))
        (sql,) = seen
        assert not_own_entity("l") in sql


# ─── The bot's live KeyCRM reports ──────────────────────────────────────────

class _KeyCRM:
    """One page of orders, and the params it was asked with."""

    def __init__(self, orders):
        self.orders, self.asked = orders, []

    def get_orders(self, params):
        self.asked.append(dict(params))
        return {"data": self.orders if params["page"] == 1 else []}


def _order(oid, *, buyer, total, returned=False, source=1):
    return {
        "id": oid, "source_id": source, "status_id": 19 if returned else 1,
        "status_group_id": 6 if returned else 4, "grand_total": total,
        "ordered_at": "2026-11-10T10:00:00+00:00",
        "buyer": None if buyer is None else {"id": buyer},
        "manager": {"id": 15},
        "products": [{"name": f"goods {oid}", "quantity": 1, "price_sold": total}],
    }


class TestTheBot:
    def test_its_reports_leave_the_entity_out(self, monkeypatch):
        """The summary, TOP-10, Excel and the milestone broadcast all read
        `aggregate_sales_data`. Mutation: drop the skip, or skip after the
        return branch — the entity's sale or its return reaches a report."""
        from bot import services

        monkeypatch.setattr(services.database, "cache_get", lambda key: None)
        monkeypatch.setattr(services.database, "cache_set", lambda *a, **k: None)
        monkeypatch.setattr(services.time, "sleep", lambda s: None)
        api = _KeyCRM([
            _order(1, buyer=OWN, total=5000.0),
            _order(2, buyer=OWN, total=300.0, returned=True),
            _order(3, buyer=5001, total=2000.0),
            _order(4, buyer=None, total=100.0),
            _order(5, buyer=5002, total=40.0, returned=True),
        ])
        sales, counts, total, revenue, returns = services.ReportService(
            api).aggregate_sales_data(date(2026, 11, 10))
        assert total == 2 and counts == {1: 2}
        assert revenue == {1: 2100.0}
        assert returns == {1: {"count": 1, "revenue": 40.0}}
        assert sales == {1: {"goods 3": 1, "goods 4": 1}}
        assert "buyer" in api.asked[0]["include"].split(",")

    def test_an_order_without_its_buyer_is_somebody_elses(self):
        from bot.services import is_own_entity
        from core.models import Order

        assert is_own_entity(Order.from_api(_order(1, buyer=OWN, total=1.0)))
        assert not is_own_entity(Order.from_api(_order(1, buyer=None, total=1.0)))
        assert not is_own_entity(Order.from_api(_order(1, buyer=OWN + 1, total=1.0)))


# ─── The findings ───────────────────────────────────────────────────────────

class TestTheFindings:
    def test_nothing_found_files_nothing(self):
        from core import pg_warehouse_dq as twins

        assert twins._own_entity_check(twins.OwnEntity()) == []

    def test_the_orders_kept_out_are_an_info_by_sales_type(self):
        """A number rather than a silence. Mutation: WARN (a digest summoned
        every morning by the decision working), or the kept-out money
        summed with a row still counted."""
        from core import pg_warehouse_dq as twins
        from core.data_quality import Severity

        facts = twins.OwnEntity(orders=(
            (11, "internal", 7000.0, 6500.0, False),
            (12, "b2b", 5000.0, 5000.0, False),
            (13, "b2b", 0.0, 900.0, False),
        ))
        (issue,) = twins._own_entity_check(facts)
        assert (issue.check_name, issue.severity, issue.count, issue.table_name) == (
            "pg_own_entity_orders", Severity.INFO, 3, "silver.orders")
        assert issue.sample_ids == (11, 12, 13)
        assert "3 order(s)" in issue.description
        assert "12,000.00 billed, 12,400.00 of goods" in issue.description
        assert ("b2b 2: 5,000.00 billed, 5,900.00 of goods; "
                "internal 1: 7,000.00 billed, 6,500.00 of goods") in issue.description
        assert "2026-11-14" in issue.description
        assert "still count" not in issue.description

    def test_a_row_stored_silver_still_counts_is_named_not_filed_again(self):
        """Between a deploy and its first rebuild. `pg_silver_row_values`
        judges those rows; this says how many and what they are worth."""
        from core import pg_warehouse_dq as twins

        facts = twins.OwnEntity(orders=(
            (11, "b2b", 100.0, 100.0, False), (12, "b2b", 250.0, 250.0, True)))
        issues = twins._own_entity_check(facts)
        assert [i.check_name for i in issues] == ["pg_own_entity_orders"]
        (issue,) = issues
        assert issue.count == 2
        assert "1 order(s)" in issue.description
        assert "1 more still count as revenue in stored Silver, worth 250.00" in issue.description

    def test_an_unlisted_card_like_the_entity_is_a_warn_by_id(self):
        """The money a second card would quietly make revenue. Ids and money
        only: the facts carry no name to put in a finding."""
        from core import pg_warehouse_dq as twins
        from core.data_quality import Severity

        facts = twins.OwnEntity(unlisted=((900001, 3, 1500.0), (900002, 0, 0.0)))
        (issue,) = twins._own_entity_check(facts)
        assert (issue.check_name, issue.severity, issue.count, issue.table_name) == (
            "pg_own_entity_unlisted", Severity.WARN, 2, "bronze.buyers")
        assert issue.sample_ids == (900001, 900002)
        assert "cards 900001, 900002" in issue.description
        assert "3 of their orders, 1,500.00, count as revenue" in issue.description
        assert {f.name: f.type for f in __import__("dataclasses").fields(twins.OwnEntity)} == {
            "orders": "Tuple[Tuple[int, str, float, float, bool], ...]",
            "unlisted": "Tuple[Tuple[int, int, float], ...]"}

    def test_a_blind_group_holds_both_and_says_so(self):
        from core import pg_warehouse_dq as twins
        from tests.unit.test_pg_warehouse_dq import _facts

        held: list = []
        issues = twins.check_pg_warehouse(
            _facts(own_entity=twins.Unwatched("timeout")), held_out=held)
        assert [i.check_name for i in issues] == ["pg_own_entity_unwatched"]
        assert sorted(held) == ["pg_own_entity_orders", "pg_own_entity_unlisted"]

    def test_the_group_is_read_before_the_recompute(self):
        from core import pg_warehouse_dq as twins

        assert twins.GROUPS.index("own_entity") < twins.GROUPS.index("silver_row_values")

    def test_the_names_are_new_and_registered(self):
        """Rule 15: no image before step 14 treats these as retired, and each
        is in the registry, has its own lever and a human name."""
        from core.alerting import REGISTRY
        from core.data_quality import HUMAN_CHECK_NAMES, REMEDIATION
        from tests.unit.test_rollback_condition_names import ALL_RETIRED

        names = {"pg_own_entity_orders", "pg_own_entity_unlisted", "pg_own_entity_unwatched"}
        assert not names & ALL_RETIRED
        assert names <= set(REGISTRY)
        assert names <= set(HUMAN_CHECK_NAMES)
        assert names <= {prefix for prefix, _ in REMEDIATION}
        lever = dict(REMEDIATION)["pg_own_entity_unlisted"]
        assert "OWN_ENTITY_BUYER_IDS" in lever and "NOT_OWN_ENTITY_BUYER_IDS" in lever


class TestTheReadBindsTheLists:
    def test_the_lists_and_the_fold_are_parameters(self, monkeypatch):
        """Read at call time and bound, never copied into the SQL: a card
        listed by a deploy is asked about by the next run. Mutation: bind
        only the entity's list where the cards' read wants both."""
        from unittest.mock import AsyncMock, MagicMock

        from core import pg_warehouse_dq as twins

        monkeypatch.setattr(constants, "OWN_ENTITY_BUYER_IDS", (7, 8))
        monkeypatch.setattr(constants, "NOT_OWN_ENTITY_BUYER_IDS", (9,))
        conn = MagicMock(fetch=AsyncMock(return_value=[]))
        assert asyncio.run(twins._read_own_entity(conn)) == twins.OwnEntity()
        (orders_sql, *orders_args), (alike_sql, *alike_args) = [
            c.args for c in conn.fetch.call_args_list]
        assert orders_sql == twins._OWN_ENTITY_ORDERS_SQL
        assert orders_args == [[7, 8], list(REVENUE_SOURCE_IDS)]
        assert alike_sql == twins._OWN_ENTITY_LOOKALIKES_SQL
        assert alike_args == [
            [7, 8, 9], twins.OWN_ENTITY_FOLD_UPPER + twins.OWN_ENTITY_FOLD_DROP,
            twins.OWN_ENTITY_FOLD_LOWER, twins.OWN_ENTITY_NAME_PATTERN]
        for sql in (orders_sql, alike_sql):
            assert "7" not in re.sub(r"\$\d", "", sql)


# ─── What a name like the entity's is ───────────────────────────────────────

# Spellings a buyer card is written with — the legal form, quotes of either
# kind, case, a dash, Russian renderings, and Latin letters typed where
# Cyrillic ones look the same (and the converse). The integration test runs
# the same lists through Postgres' `translate()` and `~`.
LOOKS_LIKE_THE_ENTITY = (
    "Коріан Сторі", "ТОВ «Коріан Сторі»", 'ТОВ "КОРІАН СТОРІ"', "тов коріан сторі",
    "ТзОВ «Коріан-Сторі»", "КОРІАН СТОРІ ТОВ", "Корiан Сторi", "КОРIАН СТОРI",
    "Кориан Стори", "Кореан Стори", "ООО «Кориан Стори»", "Коріан\u00a0Сторі",
    "Korean Story", "KOREAN STORY", "KoreanStory", "korean-story", "Korean_Story LLC",
    "TOV Korean Story", "Korian Story", "Korean Stori", "Kоrean Story",
    # Characters a pasted name carries unseen: a thin space, a narrow no-break
    # space, a soft hyphen, a zero-width space, a byte-order mark, an
    # ideographic space.
    "Коріан\u2009Сторі", "ТОВ\u202f«Коріан\u00adСторі»",
    "Korean\u200bStory", "\ufeffKorean\u3000Story",
)
NOT_THE_ENTITY = (
    "", "Korean Shop", "Story Korean", "Korea Story", "Корея", "Історія", "Сторінка",
    "Коріандр", "Korean", "Story", "Test Buyer", "Korean Beauty Story", "Kor Story",
)


class TestTheNameRule:
    def test_the_fold_table_is_whole(self):
        from core import pg_warehouse_dq as twins

        upper, lower, drop = (twins.OWN_ENTITY_FOLD_UPPER, twins.OWN_ENTITY_FOLD_LOWER,
                              twins.OWN_ENTITY_FOLD_DROP)
        assert len(upper) == len(lower) == len(set(upper))
        assert not set(drop) & set(upper) and not set(drop) & set(lower)
        assert all(u.lower() == l and u != l for u, l in zip(upper, lower))
        for ukrainian in "ҐЄІЇ":
            assert ukrainian in upper

    @pytest.mark.parametrize("name", LOOKS_LIKE_THE_ENTITY)
    def test_these_read_like_the_entity(self, name):
        from core.pg_warehouse_dq import own_entity_name_matches

        assert own_entity_name_matches(name), name

    @pytest.mark.parametrize("name", NOT_THE_ENTITY)
    def test_these_do_not(self, name):
        from core.pg_warehouse_dq import own_entity_name_matches

        assert not own_entity_name_matches(name), name

    def test_no_name_is_not_the_entity(self):
        from core.pg_warehouse_dq import own_entity_name_matches

        assert not own_entity_name_matches(None)

    def test_the_statement_folds_by_translate_alone(self):
        """Production's statement folds by `translate()` and matches by `~`,
        never by an operator that folds case itself. CI's store is
        initialised `C.UTF-8`, whose LOWER and `~*` fold Cyrillic, and
        production's `C` (docker-compose.yml), whose do not: a statement
        rewritten with them would pass every Postgres test in CI and match no
        upper-case Cyrillic card in production. Mutation: `lower(b.full_name)
        ~ $4`, `~*` or ILIKE."""
        from core.pg_warehouse_dq import _OWN_ENTITY_LOOKALIKES_SQL as sql

        assert sql.count("translate(") == 2
        for folding in ("lower(", "upper(", "initcap(", "~*", "ilike"):
            assert folding not in sql.lower(), folding

    def test_the_patterns_are_plain_enough_for_both_engines(self):
        """Bracket classes of literal letters, joined by `|`: the subset
        Python's `re` and Postgres' advanced regular expressions read the
        same way. Mutation: a range (`[а-я]`, by code point in one engine and
        by collation in the other), a class shorthand or a lookaround."""
        from core.pg_warehouse_dq import OWN_ENTITY_NAME_PATTERNS

        for pattern in OWN_ENTITY_NAME_PATTERNS:
            assert re.fullmatch(r"(\[[^\]\-\\^]+\]|[^\[\]\\().*+?{}|^$-])+", pattern), pattern


class TestTheReadersOfTheRule:
    def test_every_module_naming_the_constant_imports_it_from_its_home(self):
        """The list is asked of `core.duckdb_constants` by name, never copied
        into a second tuple. Mutation: `OWN_ENTITY_BUYER_IDS = (17358,)` in a
        reader."""
        for tree in ("core", "web", "bot", "scripts"):
            for path in sorted((ROOT / tree).rglob("*.py")):
                if path.name == "duckdb_constants.py":
                    continue
                module = ast.parse(path.read_text(encoding="utf-8"))
                for node in ast.walk(module):
                    targets = []
                    if isinstance(node, ast.Assign):
                        targets = node.targets
                    elif isinstance(node, ast.AnnAssign):
                        targets = [node.target]
                    for t in targets:
                        assert not (isinstance(t, ast.Name) and t.id in (
                            "OWN_ENTITY_BUYER_IDS", "NOT_OWN_ENTITY_BUYER_IDS")), path
