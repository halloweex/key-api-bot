"""The discount on an order and its lines, landed beside the order (revision 0038).

What is held here: the parse lands each figure KeyCRM serves, rounded as its
column rounds it, and never refuses an order over one; the write statements
name exactly the parse's columns; the archive of versions compares none of
them; the revision adds exactly them, with the parse's types, and its
downgrade keeps them; the backfill's plan writes only the discount and only
on an order in the version KeyCRM served; and the walk keeps its pace. The
server's half is `tests/integration/test_order_discounts_pg.py`.

Every payload here is synthetic. Each test names the mutation it kills.
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from core import pg_landing
from core import pg_orders_write as chain
from core.landing_rows import (
    LINE_DISCOUNT_COLUMNS, LINE_DISCOUNT_NUMERICS, ORDER_COLUMNS,
    ORDER_DISCOUNT_COLUMNS, ORDER_DISCOUNT_NUMERICS, ORDER_PRODUCT_COLUMNS,
    discount_data_json, landed_orders)
from tests import migration_replay as replay

T0 = datetime(2026, 11, 16, 10, 0, tzinfo=timezone.utc)
REV_ID = "0038_order_discounts"


def _payload(oid=7, *, updated=T0, status=1, lines=1, **order):
    """The page's example: 4085 at −10 % is 409 off and 3676 to pay; one line
    of two units, 980 → 591, so 389 off each."""
    p = {
        "id": oid, "source_id": 4, "status_id": status, "status_group_id": 1,
        "grand_total": 3676, "ordered_at": T0.isoformat(),
        "created_at": T0.isoformat(), "updated_at": updated.isoformat(),
        "buyer": {"id": 501}, "manager": None, "manager_comment": None,
        "promocode": "AUTUMN10",
        "products_total": 4085, "total_discount": 409, "discount_percent": 10,
        "discount_amount": 0, "discount_data": {"loyalty": {"discount": 5}},
        "products": [{"name": f"Item {i}", "quantity": 2, "price_sold": 591,
                      "price": 980, "total_discount": 389,
                      "discount_percent": "39.6939", "discount_amount": 0,
                      "loyalty_discount": 5, "individual_discount": 0,
                      "offer": {"product_id": 700 + i}} for i in range(lines)],
    }
    p.update(order)
    return p


def _one(payload):
    landed = landed_orders([payload])
    assert not landed.refused, landed.refused
    (row,) = landed.orders
    return row, landed.products


class TestTheParse:
    def test_the_header_lands_every_figure_rounded_to_its_column(self):
        """Kills: a figure dropped, read from the wrong key, or left unrounded
        — the backfill's plan compares the parse with the stored value by
        plain equality, and NUMERIC(12,2) stores 409 as 409.00."""
        row, _ = _one(_payload())
        assert row.products_total == Decimal("4085.00")
        assert row.total_discount == Decimal("409.00")
        assert row.discount_amount == Decimal("0.00")
        assert row.discount_percent == Decimal("10.0000")
        assert str(row.total_discount) == "409.00"
        assert json.loads(row.discount_data) == {"loyalty": {"discount": 5}}

    def test_a_line_keeps_its_discount_per_unit(self):
        """980 → 591 is 389 off EACH unit; the line's discount in money is
        `total_discount * quantity`. Kills: a parse that multiplied it, which
        every reader would then multiply again."""
        _, (line,) = _one(_payload())
        assert line.price == Decimal("980.00")
        assert line.total_discount == Decimal("389.0000")
        assert line.quantity == 2
        assert line.price - Decimal(str(line.price_sold)) == line.total_discount
        assert line.discount_percent == Decimal("39.6939")
        assert line.loyalty_discount == Decimal("5.0000")
        assert line.individual_discount == Decimal("0.0000")

    def test_text_numbers_read_as_numbers(self):
        row, _ = _one(_payload(products_total="4085.00", total_discount=" 409 "))
        assert row.products_total == Decimal("4085.00")
        assert row.total_discount == Decimal("409.00")

    def test_rounding_is_postgres_rounding(self):
        """Half away from zero, as NUMERIC rounds. Kills: banker's rounding,
        Decimal's default, which would store 0.12 where Postgres stores 0.13
        and make every such order read as owed forever."""
        row, _ = _one(_payload(total_discount="0.125", discount_percent="1.00005"))
        assert row.total_discount == Decimal("0.13")
        assert row.discount_percent == Decimal("1.0001")

    @pytest.mark.parametrize("value", [
        None, "", "abc", True, False, float("nan"), float("inf"), "Infinity",
        {"x": 1}, [1], 10 ** 12, "1e30",
    ])
    def test_a_figure_that_cannot_be_held_is_null_and_the_order_lands(self, value):
        """A discount is a fact beside the order. Kills: a parse that raised
        (the order refused, its revenue lost to a footnote) or that wrote a
        value its column refuses (the batch refused by Postgres)."""
        payload = _payload(total_discount=value, discount_percent=value)
        payload["products"][0]["loyalty_discount"] = value
        payload["products"][0]["individual_discount"] = value
        row, (line,) = _one(payload)
        assert row.total_discount is None and row.discount_percent is None
        assert line.loyalty_discount is None and line.individual_discount is None
        assert row.grand_total == 3676.0
        assert chain.order_with_lines_refusal(row, [line]) is None

    def test_a_payload_of_the_old_shape_lands_nulls(self):
        """A payload without the keys — every one the sync saw before."""
        payload = _payload()
        for key in ORDER_DISCOUNT_COLUMNS:
            payload.pop(key)
        for key in LINE_DISCOUNT_COLUMNS:
            payload["products"][0].pop(key)
        row, (line,) = _one(payload)
        assert all(getattr(row, c) is None for c in ORDER_DISCOUNT_COLUMNS)
        assert all(getattr(line, c) is None for c in LINE_DISCOUNT_COLUMNS)

    def test_the_positions_before_the_discount_did_not_move(self):
        """`pg_landing` reads `grand_total` as `row[4]` and `price_sold` as
        `row[5]`. Kills: a discount column inserted in front of them."""
        row, (line,) = _one(_payload())
        assert ORDER_COLUMNS[4] == "grand_total" and row[4] == 3676.0
        assert ORDER_PRODUCT_COLUMNS[5] == "price_sold" and line[5] == 591.0
        assert ORDER_COLUMNS[-len(ORDER_DISCOUNT_COLUMNS):] == ORDER_DISCOUNT_COLUMNS
        assert ORDER_PRODUCT_COLUMNS[-len(LINE_DISCOUNT_COLUMNS):] == LINE_DISCOUNT_COLUMNS


class TestDiscountData:
    @pytest.mark.parametrize("value", [None, [], {}, "", 0, "loyalty"])
    def test_nothing_recorded_is_null(self, value):
        assert discount_data_json(value) is None

    def test_one_object_is_always_one_text(self):
        assert (discount_data_json({"b": 1, "a": {"d": 2, "c": 3}})
                == discount_data_json({"a": {"c": 3, "d": 2}, "b": 1}))

    def test_cyrillic_stays_readable(self):
        assert "Знижка" in discount_data_json({"loyalty": {"name": "Знижка"}})

    def test_a_nul_is_dropped_because_jsonb_refuses_it(self):
        """Postgres' JSONB refuses `\\u0000`. Kills: the NUL kept, and every
        order carrying it refused by the server, batch by batch."""
        text = discount_data_json({"lo\x00yalty": {"name": "a\x00b"}})
        assert "\\u0000" not in text and json.loads(text) == {"loyalty": {"name": "ab"}}

    @pytest.mark.parametrize("value", [
        {"x": float("nan")}, {"x": "\ud800"}, {"x": object()},
    ])
    def test_what_jsonb_cannot_carry_is_null(self, value):
        assert discount_data_json(value) is None


class TestTheRefusalsStayComplete:
    def test_a_row_built_around_the_parse_is_still_refused(self):
        """The parse never produces these; a row built any other way must not
        reach the latch with one. Kills: the discount left out of
        `order_refusal`/`product_refusal`."""
        row, (line,) = _one(_payload())
        assert "total_discount" in chain.order_refusal(
            row._replace(total_discount=Decimal("1e12")))
        assert "discount_data" in chain.order_refusal(
            row._replace(discount_data="a\x00"))
        assert "loyalty_discount" in chain.product_refusal(
            line._replace(loyalty_discount=Decimal("1e9")))
        assert chain.order_refusal(row) is None and chain.product_refusal(line) is None


class TestTheWriteStatements:
    @staticmethod
    def _insert_columns(sql):
        head = re.search(r"\(([^()]*)\)\s*VALUES", sql, re.S).group(1)
        return tuple(c.strip() for c in head.split(","))

    @pytest.mark.parametrize("sql,columns", [
        (pg_landing._ORDER_UPSERT, ORDER_COLUMNS),
        (pg_landing._ORDER_PRODUCT_INSERT, ORDER_PRODUCT_COLUMNS),
    ])
    def test_each_statement_names_the_parse_s_columns_in_its_order(self, sql, columns):
        """The rows are handed over positionally. Kills: a column the parse
        gained and the statement did not, or the two orders apart."""
        assert self._insert_columns(sql) == columns
        values = re.search(r"VALUES\s*\((.*?)\)\s*ON CONFLICT", sql, re.S).group(1)
        assert re.findall(r"\$(\d+)", values) == [str(i) for i in range(1, len(columns) + 1)]

    @pytest.mark.parametrize("sql,columns", [
        (pg_landing._ORDER_UPSERT, ORDER_DISCOUNT_COLUMNS),
        (pg_landing._ORDER_PRODUCT_INSERT, LINE_DISCOUNT_COLUMNS),
    ])
    def test_the_discount_is_a_plain_overwrite(self, sql, columns):
        """KeyCRM's current figure is the truth. Kills: a COALESCE copied from
        `manager_comment`, which would keep a discount KeyCRM removed."""
        for column in columns:
            assert re.search(rf"\b{column}\s*=\s*EXCLUDED\.{column}\b", sql), column
        assert "COALESCE(EXCLUDED.manager_comment" in pg_landing._ORDER_UPSERT

    def test_discount_data_is_cast_to_jsonb(self):
        """asyncpg registers no JSONB codec here; text cast by the statement."""
        assert "$17::jsonb" in pg_landing._ORDER_UPSERT
        assert ORDER_COLUMNS[16] == "discount_data"


class TestTheArchiveComparesNoneOfIt:
    def test_no_discount_column_is_versioned(self):
        """The backfill writes these on ~50 000 stored orders. Kills: one of
        them added to `VERSIONED_COLUMNS` — a version per order, all dated the
        backfill's day, and `order_versions_flooding`."""
        from core.pg_order_versions import CAPTURE_SQL, VERSIONED_COLUMNS

        discount = set(ORDER_DISCOUNT_COLUMNS)
        assert not discount & set(VERSIONED_COLUMNS)
        for column in discount:
            assert column not in CAPTURE_SQL, column


class TestTheRevision:
    def _module(self):
        return replay.load(replay.VERSIONS / f"{REV_ID}.py")

    def test_it_follows_0037_and_is_in_the_chain_the_code_requires(self):
        """Kills: 0038 hung off anything but 0037 (the frozen month goals,
        which took the number first by the owner's decision of 2026-11-18),
        or dropped from the line the code requires — web would write columns
        a database without it does not have. It was the head until 0039 (the
        promotions calendar) descended from it; `test_pg_plumbing` owns the
        head pin."""
        from core import pg

        rev = self._module()
        assert rev.revision == REV_ID
        assert rev.down_revision == "0037_frozen_month_goals"
        assert REV_ID in [m.revision for m in replay.lineage(pg.REQUIRED_REVISION)]

    def _added(self):
        added = {}
        for sql in replay.statements(self._module(), "upgrade"):
            table = re.search(r"ALTER TABLE ([\w.]+)", sql)
            for name, kind in re.findall(
                    r"ADD COLUMN IF NOT EXISTS (\w+)\s+([A-Z]+(?:\(\d+,\d+\))?)", sql):
                added.setdefault(table.group(1), {})[name] = kind
        return added

    def test_it_adds_exactly_the_parse_s_columns_with_the_parse_s_types(self):
        """The parse rounds to the scale the column has; one home for the
        types, held here. Kills: a column missing, added twice, or a scale
        that rounds differently from the parse (every order then reads as
        owed by the backfill, for ever)."""
        added = self._added()
        expected_orders = {c: f"NUMERIC({p},{s})" for c, (p, s) in ORDER_DISCOUNT_NUMERICS.items()}
        expected_orders["discount_data"] = "JSONB"
        expected_lines = {c: f"NUMERIC({p},{s})" for c, (p, s) in LINE_DISCOUNT_NUMERICS.items()}
        assert added == {"bronze.orders": expected_orders,
                         "bronze.order_products": expected_lines}

    def test_every_added_column_is_nullable_without_a_default_and_idempotent(self):
        """NULL means "not landed"; a DEFAULT would claim a figure KeyCRM never
        gave, and NOT NULL would refuse every order of the old shape. Without
        IF NOT EXISTS the upgrade after the keep-the-columns downgrade fails."""
        sql = "\n".join(replay.statements(self._module(), "upgrade")).upper()
        assert "NOT NULL" not in sql and "DEFAULT" not in sql
        assert sql.count("ADD COLUMN IF NOT EXISTS") == 11
        assert sql.count("ADD COLUMN") == 11

    def test_every_column_says_what_it_holds(self):
        sql = replay.statements(self._module(), "upgrade")
        comments = [s for s in sql if s.startswith("COMMENT ON COLUMN")]
        assert len(comments) == 11
        line = next(s for s in comments if "order_products.total_discount" in s)
        assert "PER UNIT" in line

    def test_the_downgrade_keeps_the_columns(self):
        """An older image names its columns one by one and never sees these;
        dropping them would discard the backfill. Kills: a DROP COLUMN."""
        assert replay.statements(self._module(), "downgrade") == []

    def test_it_touches_nothing_but_the_two_order_tables(self):
        sql = re.sub(r"--[^\n]*", "", "\n".join(
            replay.statements(self._module(), "upgrade"))).upper()
        sql = re.sub(r"'(?:[^']|'')*'", "''", sql)   # the comments' prose
        for word in ("CREATE", "DROP", "INSERT", "UPDATE", "DELETE", "GRANT",
                     "TRUNCATE", "RENAME"):
            assert not re.search(rf"\b{word}\b", sql), word
        assert set(re.findall(r"BRONZE\.[A-Z_]+", sql)) == {
            "BRONZE.ORDERS", "BRONZE.ORDER_PRODUCTS"}


# ─── The backfill's plan ─────────────────────────────────────────────────────

def _stored(row, **changes):
    """What Postgres would hand back for a header: every column, as stored."""
    values = dict(zip(ORDER_COLUMNS, row))
    values.update(changes)
    return values


def _stored_lines(lines, **changes):
    out = []
    for line in lines:
        values = dict(zip(ORDER_PRODUCT_COLUMNS, line))
        values["price_sold"] = Decimal(str(values["price_sold"])).quantize(Decimal("0.01"))
        values.update(changes)
        out.append(values)
    return out


def _plan(payload, stored, stored_lines):
    row, lines = _one(payload)
    oid = int(row.id)
    return chain.discount_plan({oid: row}, {oid: lines},
                               {oid: stored} if stored is not None else {},
                               {oid: stored_lines} if stored_lines else {})


NULLS_ORDER = {c: None for c in ORDER_DISCOUNT_COLUMNS}
NULLS_LINE = {c: None for c in LINE_DISCOUNT_COLUMNS}


class TestTheBackfillPlan:
    def test_an_order_landed_before_0038_gets_header_and_lines(self):
        row, lines = _one(_payload())
        headers, with_lines, current, moved, absent, differ = _plan(
            _payload(), _stored(row, **NULLS_ORDER), _stored_lines(lines, **NULLS_LINE))
        assert len(headers) == 1 and with_lines == [7]
        assert (current, moved, absent, differ) == ([], [], [], [])
        written = dict(zip(ORDER_COLUMNS, headers[0]))
        assert written["total_discount"] == Decimal("409.00")

    def test_only_the_discount_moves_on_the_header(self):
        """The stored status, total and comment stay, even where KeyCRM's
        differ without `updated_at` moving. Kills: the backfill as a resync,
        moving revenue already reported."""
        row, lines = _one(_payload())
        stored = _stored(row, **NULLS_ORDER, status_id=12, grand_total=Decimal("3600.00"),
                         manager_comment="utm_source=ig", buyer_id=None)
        (header,), *_ = _plan(_payload(), stored, _stored_lines(lines, **NULLS_LINE))
        written = dict(zip(ORDER_COLUMNS, header))
        for column in ORDER_COLUMNS:
            if column in ORDER_DISCOUNT_COLUMNS:
                assert written[column] == getattr(row, column), column
            else:
                assert written[column] == stored[column], column

    def test_an_order_in_another_version_is_left_to_the_sync(self):
        """Kills: the guard that makes the CLI safe without the heavy-job lock
        — a sync tick that landed a newer version between fetch and write
        would be overwritten with the older figures."""
        row, lines = _one(_payload())
        for stamp in (T0 + timedelta(seconds=1), T0 - timedelta(hours=1), None):
            plan = _plan(_payload(), _stored(row, **NULLS_ORDER, updated_at=stamp),
                         _stored_lines(lines, **NULLS_LINE))
            assert plan[0] == [] and plan[3] == [7], stamp

    def test_the_same_instant_in_another_zone_is_the_same_version(self):
        row, lines = _one(_payload())
        kyiv = T0.astimezone(timezone(timedelta(hours=2)))
        plan = _plan(_payload(), _stored(row, **NULLS_ORDER, updated_at=kyiv),
                     _stored_lines(lines, **NULLS_LINE))
        assert len(plan[0]) == 1

    def test_an_order_postgres_does_not_hold_is_absent(self):
        plan = _plan(_payload(), None, [])
        assert plan[0] == [] and plan[4] == [7]

    @pytest.mark.parametrize("change", [
        {"quantity": 3}, {"price_sold": Decimal("590.00")}, {"product_id": 1},
        {"name": "Other"},
    ])
    def test_lines_that_are_not_keycrm_s_keep_theirs(self, change):
        """A stored line set that differs from KeyCRM's is the sync's or a
        repair's to correct; the backfill writes the header alone. Kills:
        line discounts written onto another line's id."""
        row, lines = _one(_payload())
        headers, with_lines, _, _, _, differ = _plan(
            _payload(), _stored(row, **NULLS_ORDER),
            _stored_lines(lines, **NULLS_LINE, **change))
        assert len(headers) == 1 and with_lines == [] and differ == [7]

    def test_a_line_missing_or_extra_differs(self):
        row, lines = _one(_payload(lines=2))
        stored = _stored(row, **NULLS_ORDER)
        assert _plan(_payload(lines=2), stored,
                     _stored_lines(lines[:1], **NULLS_LINE))[5] == [7]
        assert _plan(_payload(lines=1), stored,
                     _stored_lines(lines, **NULLS_LINE))[5] == [7]

    def test_a_second_run_writes_nothing(self):
        """What the first run wrote, read back, is current. Kills: a
        comparison that never settles — JSON key order (JSONB sorts its keys
        its own way), scale, a float against a Decimal."""
        row, lines = _one(_payload())
        jsonb_order = json.dumps({"loyalty": {"discount": 5}}, separators=(", ", ": "))
        plan = _plan(_payload(), _stored(row, discount_data=jsonb_order),
                     _stored_lines(lines))
        assert plan[0] == [] and plan[2] == [7]

    def test_only_the_lines_owed_still_rewrites_them(self):
        row, lines = _one(_payload())
        headers, with_lines, *_ = _plan(_payload(), _stored(row),
                                        _stored_lines(lines, **NULLS_LINE))
        assert len(headers) == 1 and with_lines == [7]


# ─── The writer, idle ────────────────────────────────────────────────────────

class _ReadOnlyConn:
    def __init__(self, heads, lines):
        self.heads, self.lines, self.sql = heads, lines, []

    async def fetch(self, sql, *args, **kw):
        self.sql.append(sql)
        return self.heads if "FROM bronze.orders" in sql else self.lines

    def transaction(self):  # pragma: no cover — reaching it is the failure
        raise AssertionError("an idle page opened a transaction")


class _Pool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self, timeout=None):
        from contextlib import asynccontextmanager

        conn = self.conn

        @asynccontextmanager
        async def held():
            yield conn

        return held()


@pytest.mark.asyncio
async def test_a_page_already_landed_takes_no_latch(monkeypatch):
    """A re-run over pages already landed writes nothing and latches nothing.
    Kills: the latch and the owner claim taken before the plan is read."""
    from unittest.mock import AsyncMock

    row, lines = _one(_payload())
    conn = _ReadOnlyConn([_stored(row)], _stored_lines(lines))
    monkeypatch.setattr(chain, "_pool", AsyncMock(return_value=_Pool(conn)))
    monkeypatch.setattr(chain, "_latch", lambda: pytest.fail("latched"))
    result = await chain.restore_discounts([_payload()])
    assert result.already_current == (7,) and result.headers_written == 0
    assert all("FOR UPDATE" not in s for s in conn.sql)


@pytest.mark.asyncio
async def test_a_page_of_refused_payloads_asks_postgres_nothing(monkeypatch):
    from unittest.mock import AsyncMock

    pool = AsyncMock(side_effect=AssertionError("asked Postgres"))
    monkeypatch.setattr(chain, "_pool", pool)
    result = await chain.restore_discounts([_payload(ordered_at=None),
                                            _payload(8, grand_total=None)])
    assert [oid for oid, _ in result.refused] == [8]


# ─── The walk ────────────────────────────────────────────────────────────────

class _Clock:
    def __init__(self):
        self.now = 0.0
        self.slept = []

    def __call__(self):
        return self.now

    async def sleep(self, seconds):
        self.slept.append(seconds)
        self.now += seconds


class _Client:
    def __init__(self, sizes, clock, *, fail_on=None):
        self.sizes, self.clock, self.fail_on = sizes, clock, fail_on
        self.asked = []

    async def get_orders(self, params):
        self.asked.append((self.clock.now, dict(params)))
        self.clock.now += 0.4          # the request itself
        page = params["page"]
        if page == self.fail_on:
            raise RuntimeError("KeyCRM is down")
        size = self.sizes[page - 1] if page <= len(self.sizes) else 0
        return {"data": [{"id": page * 100 + i} for i in range(size)],
                "last_page": len(self.sizes)}


def _restore():
    async def restore(data):
        return chain.DiscountRestore(headers_written=len(data),
                                     already_current=tuple(range(1)))
    return restore


@pytest.fixture
def backfill():
    from scripts import backfill_discounts

    return backfill_discounts


@pytest.mark.asyncio
async def test_the_walk_keeps_its_pace_and_stops_at_the_end(backfill):
    """At most one request per interval, so 40 a minute leave the sync its
    share of KeyCRM's 60. Kills: the sleep dropped, or measured after the
    request instead of between starts."""
    clock = _Clock()
    client = _Client([50, 50, 50, 7], clock)
    totals = await backfill.walk(client, start_page=1, pages=None, interval_s=1.5,
                                 restore=_restore(), sleep=clock.sleep, clock=clock)
    starts = [t for t, _ in client.asked]
    assert [p["page"] for _, p in client.asked] == [1, 2, 3, 4]
    assert all(b - a >= 1.5 for a, b in zip(starts, starts[1:]))
    assert totals["next_page"] is None and totals["orders"] == 157
    assert all(p["sort"] == "id" and p["include"] == "products.offer"
               and p["limit"] == 50 for _, p in client.asked)


@pytest.mark.asyncio
async def test_the_walk_resumes_and_stops_where_told(backfill):
    clock = _Clock()
    client = _Client([50] * 10, clock)
    totals = await backfill.walk(client, start_page=4, pages=2, interval_s=1.5,
                                 restore=_restore(), sleep=clock.sleep, clock=clock)
    assert [p["page"] for _, p in client.asked] == [4, 5]
    assert totals["next_page"] == 6


@pytest.mark.asyncio
async def test_an_error_names_the_page_to_resume_from(backfill, caplog):
    clock = _Clock()
    client = _Client([50] * 10, clock, fail_on=3)
    with pytest.raises(RuntimeError):
        await backfill.walk(client, start_page=1, pages=None, interval_s=1.5,
                            restore=_restore(), sleep=clock.sleep, clock=clock)
    assert "resume with --start-page 3" in caplog.text


def test_the_walk_logs_no_payload_value(backfill):
    """Ids and counts only: a page carries names and phones. Kills: a
    progress line that formats a payload or a row."""
    import inspect

    source = inspect.getsource(backfill.walk)
    assert "data[" not in source and "%r" not in source
    assert backfill._sample(range(25)).endswith("(+15)")


@pytest.mark.parametrize("argv", [["--interval", "0.5"], ["--start-page", "0"],
                                  ["--pages", "0"]])
def test_the_cli_refuses_a_pace_or_page_out_of_bounds(backfill, argv):
    import asyncio

    with pytest.raises(SystemExit):
        asyncio.run(backfill.main(argv))


def test_the_restore_result_names_ids_never_values():
    fields = chain.DiscountRestore.__dataclass_fields__
    assert set(fields) == {"headers_written", "lines_written", "already_current",
                           "moved", "absent", "lines_differ", "refused"}

