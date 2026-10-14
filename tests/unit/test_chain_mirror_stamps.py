"""E1 against a chain whose own writer stamps the mirror's watermark.

`chain_shipper_overwrote` read any `meta.mirror_state.last_ok_at` after a
chain's owner row as a shipment out of DuckDB over the chain's rows. Chain 6
stamps that watermark on every write by design (OD-15 (a)), so from 2026-10-10
every morning's `dq_mirror_landing` paged the catalogue chain's own hourly
writes CRITICAL — `bronze.products` every day, `bronze.categories` from the
first Sunday write — with "restore from the nightly dump" as the lever.
Production, read 2026-10-14: both watermarks were exactly a stamp in the
chain's own record (`writes:<table>`), and nothing else had written either
table since the latch.

What is pinned here, without Postgres (the real statements are in
`tests/integration/test_shipper_overwrote_catalogue.py`):

- which chains stamp the watermark is walked, not remembered, and each must
  say so (`CHAIN_STAMPS_MIRROR_STATE`); a chain that stamps and keeps no record
  of when is a named decision (chain 3);
- the reader hands on the record's `stamps` and never its `previous`, answers
  a record nobody can parse as unreadable instead of raising, and reads the
  watermark in the same statement as the record;
- the verdict: the chain's own recorded instant files nothing, any other
  instant after the latch is CRITICAL, an unreadable or unread record is
  CRITICAL saying it could not tell, and the statement's watermark is the one
  judged;
- the comparison reads the record once per run, only for a chain that has
  changed hands, and hands it to the verdict.
"""
from __future__ import annotations

import ast
import inspect
import pathlib
import re
import textwrap
from datetime import datetime, timedelta, timezone

import pytest

from core import chain_latch, pg_catalogue_write, pg_expenses_write, pg_orders_write
from core import write_chains
from core.chain_latch import MirrorStamp
from core.mirror_reconciliation import _chain_latch_findings, _read_mirror_stamps

CORE = pathlib.Path(__file__).resolve().parents[2] / "core"

PRODUCTS, CATEGORIES = pg_catalogue_write.CHAIN_TABLES
ORDERS, ORDER_PRODUCTS, EXPENSES, MISSES = pg_orders_write.CHAIN_TABLES
MANUAL_EXPENSES = "app.manual_expenses"


# ─── The walk: which chains stamp the watermark ─────────────────────────────
#
# A statement that moves `last_ok_at` — the success stamp E1 reads. A failure
# stamp (`pg_landing._record_failure`) and a backfill stamp name neither.
_STAMP = re.compile(
    r"\b(?:INSERT\s+INTO|UPDATE)\s+meta\.mirror_state\b.*?\blast_ok_at\b",
    re.IGNORECASE | re.DOTALL)


class _Mod:
    def __init__(self, name: str, source: str):
        self.name = name
        self.tree = ast.parse(textwrap.dedent(source))
        self.funcs, self.imports, self.aliases, self.consts = {}, {}, {}, {}
        for n in ast.walk(self.tree):
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
                self.funcs.setdefault(n.name, n)
            elif isinstance(n, ast.ImportFrom) and n.module:
                for a in n.names:
                    self.imports[a.asname or a.name] = (n.module, a.name)
            elif isinstance(n, ast.Import):
                for a in n.names:
                    self.aliases[a.asname or a.name] = a.name
        for n in self.tree.body:
            if isinstance(n, (ast.Assign, ast.AnnAssign)) and n.value is not None:
                text = self._render(n.value)
                for t in (n.targets if isinstance(n, ast.Assign) else [n.target]):
                    if isinstance(t, ast.Name) and text is not None:
                        self.consts[t.id] = text

    def _render(self, node):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.JoinedStr):
            return "".join(v.value if isinstance(v, ast.Constant) else "{?}"
                           for v in node.values)
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            left, right = self._render(node.left), self._render(node.right)
            return None if left is None or right is None else left + right
        if isinstance(node, ast.Name):
            return self.consts.get(node.id)
        return None


class _Tree:
    """Every module under `core/`, and which of their functions reach a
    statement that stamps `last_ok_at` — through their own literals, the
    constants they name, and every function or constant they name in this
    module or another, `from core import pg_landing` and
    `from core.pg_landing import X` alike, to a fixed point. Names, not calls:
    a statement handed on as a value is reached as surely as one executed, and
    a chain module that merely names a stamping helper is asked to declare
    rather than let through."""

    def __init__(self, sources: dict):
        self.mods = {name: _Mod(name, src) for name, src in sources.items()}
        self._memo: dict = {}

    @classmethod
    def on_disk(cls) -> "_Tree":
        return cls({"core." + p.relative_to(CORE).with_suffix("").as_posix()
                    .replace("/", "."): p.read_text(encoding="utf-8")
                    for p in sorted(CORE.rglob("*.py"))})

    def _module(self, m, alias):
        if alias in m.aliases:
            return self.mods.get(m.aliases[alias])
        if alias in m.imports:
            src, attr = m.imports[alias]
            return self.mods.get(f"{src}.{attr}")
        return None

    def _target(self, m, name):
        if name in m.funcs or name in m.consts:
            return m, name
        if name in m.imports:
            src, attr = m.imports[name]
            other = self.mods.get(src)
            if other is not None:
                return other, attr
        return None

    def stamps(self, m, name, stack=()) -> bool:
        key = (m.name, name)
        if key in self._memo:
            return self._memo[key]
        if key in stack:
            return False
        found = bool(name in m.consts and _STAMP.search(m.consts[name]))
        if not found and name in m.funcs:
            for n in ast.walk(m.funcs[name]):
                if isinstance(n, ast.Constant) and isinstance(n.value, str) \
                        and _STAMP.search(n.value):
                    found = True
                    break
                hit = None
                if isinstance(n, ast.Name):
                    hit = self._target(m, n.id)
                elif isinstance(n, ast.Attribute) and isinstance(n.value, ast.Name):
                    other = self._module(m, n.value.id)
                    hit = (other, n.attr) if other is not None else None
                if hit and (hit[0] is not m or hit[1] != name) \
                        and self.stamps(hit[0], hit[1], stack + (key,)):
                    found = True
                    break
        if not stack:
            self._memo[key] = found
        return found

    def stampers_in(self, module_name: str) -> set:
        m = self.mods[module_name]
        return {f for f in m.funcs if self.stamps(m, f)}


@pytest.fixture(scope="module")
def tree():
    return _Tree.on_disk()


def _walked(tree) -> set:
    return {write_chains.chain_name(c) for c in write_chains.WRITE_CHAINS
            if tree.stampers_in(c.__name__)}


def _declared() -> dict:
    return {write_chains.chain_name(c): tuple(c.CHAIN_STAMPS_MIRROR_STATE)
            for c in write_chains.WRITE_CHAINS
            if getattr(c, "CHAIN_STAMPS_MIRROR_STATE", ())}


class TestEveryChainThatStampsTheWatermarkSaysSo:
    def test_the_walk_and_the_declarations_agree(self, tree):
        """A chain whose writer stamps `last_ok_at` without declaring it is
        what paged chain 6's own writes for five mornings: E1 reads its
        writes as a shipment over them."""
        assert _walked(tree) == set(_declared()), (
            "a registered chain's writer stamps meta.mirror_state.last_ok_at "
            "and does not declare CHAIN_STAMPS_MIRROR_STATE (or declares it and "
            "stamps nothing) — read core.chain_latch.MirrorStamp")

    def test_the_walk_is_not_vacuous(self, tree):
        # Today's facts, so a walk that stopped seeing anything fails here.
        assert _walked(tree) == {"pg_catalogue_write", "pg_orders_write"}
        assert {"upsert_products", "upsert_categories"} <= tree.stampers_in(
            "core.pg_catalogue_write")
        assert "upsert_orders_with_expenses" in tree.stampers_in("core.pg_orders_write")
        # The two shapes chain 3 and chain 6 reach it by, and the one that
        # must not count.
        toy = _Tree({
            "core.landing": '''
                _OK = """INSERT INTO meta.mirror_state (table_name, last_ok_at)
                         VALUES ($1, now())"""
                OK_SQL = _OK
                _FAILED = "UPDATE meta.mirror_state SET failures_since_ok = 1"
                async def write_rows(conn):
                    await conn.execute(_OK)
                async def record_failure(conn):
                    await conn.execute(_FAILED)
            ''',
            "core.through_a_helper": '''
                from core import landing
                async def upsert(conn):
                    await landing.write_rows(conn)
            ''',
            "core.through_a_constant": '''
                async def _land(conn):
                    from core.landing import OK_SQL
                    await conn.execute(OK_SQL)
                async def upsert(conn):
                    await _land(conn)
            ''',
            "core.failures_only": '''
                from core import landing
                async def upsert(conn):
                    await landing.record_failure(conn)
            ''',
        })
        assert toy.stampers_in("core.through_a_helper") == {"upsert"}
        assert toy.stampers_in("core.through_a_constant") == {"_land", "upsert"}
        assert toy.stampers_in("core.failures_only") == set()

    def test_the_declared_tables_are_the_chains_own(self):
        for chain in write_chains.WRITE_CHAINS:
            declared = set(getattr(chain, "CHAIN_STAMPS_MIRROR_STATE", ()))
            assert declared <= set(chain.CHAIN_TABLES), write_chains.chain_name(chain)

    def test_stamping_without_a_record_is_a_named_decision(self):
        """The strongest form the walk allows: a chain that stamps the
        watermark and keeps no record of when is not judged by E1 on those
        tables at all, so each one is a decision written beside its
        declaration (chain 3's module says why), and a new one fails here
        until somebody makes it."""
        without = {name for name in _declared()
                   if not hasattr(_chain(name), "read_mirror_stamps")}
        assert without == {"pg_orders_write"}

    def test_only_a_declaring_chain_offers_its_record(self):
        offering = {write_chains.chain_name(c) for c in write_chains.WRITE_CHAINS
                    if hasattr(c, "read_mirror_stamps")}
        assert offering <= set(_declared())
        assert offering == {"pg_catalogue_write"}

    def test_the_misses_are_judged_as_before(self):
        """`record_backfill_misses` is a bare INSERT: the misses' watermark is
        the hourly copy's alone, so a stamp after the latch there is still a
        shipment."""
        assert MISSES not in pg_orders_write.CHAIN_STAMPS_MIRROR_STATE
        assert set(pg_catalogue_write.CHAIN_STAMPS_MIRROR_STATE) == {PRODUCTS, CATEGORIES}


def _chain(name):
    (chain,) = [c for c in write_chains.WRITE_CHAINS if write_chains.chain_name(c) == name]
    return chain


# ─── The reader ─────────────────────────────────────────────────────────────


class _Conn:
    def __init__(self, rows):
        self.rows, self.calls = rows, []

    async def fetch(self, sql, *args):
        self.calls.append((sql, args))
        return self.rows


def _row(table, last_ok_us, record):
    last_ok_at = (None if last_ok_us is None else
                  datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=last_ok_us))
    return {"table_name": table, "last_ok_at": last_ok_at,
            "last_ok_us": last_ok_us, "record": record}


T0 = 1_791_970_314_789_360          # bronze.products' watermark, production, 10-14


class TestTheReader:
    @pytest.mark.asyncio
    async def test_one_statement_for_the_watermark_and_the_record(self):
        conn = _Conn([])
        await pg_catalogue_write.read_mirror_stamps(conn)
        ((sql, args),) = conn.calls
        assert "meta.mirror_state" in sql and "meta.chain_watermarks" in sql
        assert args == (list(pg_catalogue_write.CHAIN_STAMPS_MIRROR_STATE),
                        pg_catalogue_write.RECORD_PREFIX)

    def test_the_watermark_is_put_in_the_records_own_arithmetic(self):
        """The record holds `STAMP_SQL` of `mirrored_at`; the watermark must be
        the same expression of `last_ok_at`, worked out by Postgres, or a
        rendering of a timestamp is what gets compared."""
        expected = pg_catalogue_write.STAMP_SQL.replace("mirrored_at", "m.last_ok_at")
        assert expected + " AS last_ok_us" in pg_catalogue_write._MIRROR_STAMPS_SQL

    @pytest.mark.asyncio
    async def test_it_hands_on_the_stamps(self):
        conn = _Conn([_row(PRODUCTS, T0, f'{{"stamps": [{T0 - 5}, {T0}], "previous": {T0 - 5}}}'),
                      _row(CATEGORIES, T0 - 9, None)])
        got = await pg_catalogue_write.read_mirror_stamps(conn)
        assert got[PRODUCTS] == MirrorStamp(
            last_ok_at=conn.rows[0]["last_ok_at"], last_ok_us=T0,
            stamps=frozenset({T0 - 5, T0}), unreadable=None)
        # No record: an empty one — no instant is the chain's.
        assert got[CATEGORIES].stamps == frozenset() and got[CATEGORIES].unreadable is None

    @pytest.mark.asyncio
    async def test_previous_is_never_taken_for_the_chains(self):
        """After a shipment and then a chain write, `previous` IS the
        shipment's instant. Put the watermark back on it and the verdict must
        still be CRITICAL — which it is only if `previous` was never handed
        on as one of the chain's."""
        record = f'{{"stamps": [{T0}], "previous": {T0 - 1000}}}'
        got = await pg_catalogue_write.read_mirror_stamps(
            _Conn([_row(PRODUCTS, T0 - 1000, record)]))
        assert T0 - 1000 not in got[PRODUCTS].stamps

        since = chain_latch.latch(pg_catalogue_write.CHAIN)
        owners = {t: since for t in pg_catalogue_write.CHAIN_TABLES}
        # The latch is now; the watermark must be after it to be judged.
        stamp = got[PRODUCTS]
        later = datetime.now(timezone.utc) + timedelta(hours=1)
        stamp = MirrorStamp(later, stamp.last_ok_us, stamp.stamps, stamp.unreadable)
        found = _overwrote(_chain_latch_findings(owners, {}, {PRODUCTS: stamp}))
        assert set(found) == {PRODUCTS}

    @pytest.mark.parametrize("record", [
        "nonsense", "[]", '{"stamps": "x"}', '{"stamps": [true]}',
        '{"stamps": [], "previous": 1.5}'])
    @pytest.mark.asyncio
    async def test_a_record_nobody_can_parse_is_answered_not_raised(self, record):
        got = await pg_catalogue_write.read_mirror_stamps(_Conn([_row(PRODUCTS, T0, record)]))
        assert got[PRODUCTS].unreadable
        assert got[PRODUCTS].stamps == frozenset()

    @pytest.mark.asyncio
    async def test_it_only_reads(self):
        """A chain module's public coroutine that takes a connection is held
        to writing nothing by the latch walk only if it reaches one through the
        pool; this one is handed it, so say it here."""
        source = inspect.getsource(pg_catalogue_write.read_mirror_stamps)
        called = {getattr(n.func, "attr", getattr(n.func, "id", ""))
                  for n in ast.walk(ast.parse(textwrap.dedent(source)))
                  if isinstance(n, ast.Call)}
        assert not called & {"execute", "executemany", "transaction", "claim", "_latch"}
        assert pg_catalogue_write._MIRROR_STAMPS_SQL.lstrip().upper().startswith("SELECT")


# ─── The verdict ────────────────────────────────────────────────────────────


def _overwrote(issues) -> dict:
    return {i.table_name: i for i in issues if i.check_name == "chain_shipper_overwrote"}


def _at(us: int) -> datetime:
    return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=us)


def _us(moment: datetime) -> int:
    return (moment - datetime(1970, 1, 1, tzinfo=timezone.utc)) // timedelta(microseconds=1)


@pytest.fixture
def catalogue():
    """Chain 6 latched now, both owner rows at the latch — production's
    shape since 2026-10-09 10:45. Returns the owners and a moment after it."""
    since = chain_latch.latch(pg_catalogue_write.CHAIN)
    owners = {t: since for t in pg_catalogue_write.CHAIN_TABLES}
    after = datetime.fromisoformat(since) + timedelta(hours=1)
    return owners, after


def _stamp(moment, stamps=(), unreadable=None) -> MirrorStamp:
    return MirrorStamp(last_ok_at=moment, last_ok_us=_us(moment),
                       stamps=frozenset(stamps), unreadable=unreadable)


class TestTheVerdict:
    def test_the_chains_own_writes_file_nothing(self, catalogue):
        """Production on 2026-10-14: both watermarks after the latch, each a
        stamp in its record. Five CRITICAL pages before this."""
        owners, after = catalogue
        week = after + timedelta(days=4)
        stamps = {PRODUCTS: _stamp(week, [_us(week)]),
                  CATEGORIES: _stamp(after, [_us(after)])}
        issues = _chain_latch_findings(owners, {}, stamps)
        assert [i for i in issues if i.check_name.startswith("chain_")] == []

    def test_an_instant_the_record_does_not_carry_is_critical(self, catalogue):
        owners, after = catalogue
        stamps = {PRODUCTS: _stamp(after, [_us(after) - 3_600_000_000]),
                  CATEGORIES: _stamp(after, [_us(after)])}
        found = _overwrote(_chain_latch_findings(owners, {}, stamps))
        assert set(found) == {PRODUCTS}
        issue = found[PRODUCTS]
        assert issue.severity.value == "CRITICAL"
        assert after.isoformat() in issue.description
        assert "does not carry" in issue.description
        assert "Do not re-run the shipper" in issue.description
        # Not the hourly copy: it never ships the catalogue.
        assert "hourly replication" not in issue.description

    def test_one_microsecond_off_is_not_the_chains(self, catalogue):
        owners, after = catalogue
        stamps = {PRODUCTS: _stamp(after, [_us(after) + 1]),
                  CATEGORIES: _stamp(after, [_us(after)])}
        assert set(_overwrote(_chain_latch_findings(owners, {}, stamps))) == {PRODUCTS}

    def test_an_empty_record_owns_no_instant(self, catalogue):
        """Latched through the products, no categories write yet, and then a
        categories stamp after the latch: the chain did not write it."""
        owners, after = catalogue
        stamps = {PRODUCTS: _stamp(after, [_us(after)]), CATEGORIES: _stamp(after)}
        assert set(_overwrote(_chain_latch_findings(owners, {}, stamps))) == {CATEGORIES}

    def test_a_watermark_from_before_the_latch_is_not_judged(self, catalogue):
        """Production's categories from the latch until the first Sunday
        write: last stamped by the mirror a week before, no record — nothing
        to say, per table."""
        owners, after = catalogue
        before = after - timedelta(days=7)
        stamps = {PRODUCTS: _stamp(after, [_us(after)]), CATEGORIES: _stamp(before)}
        assert _overwrote(_chain_latch_findings(owners, {}, stamps)) == {}

    @pytest.mark.parametrize("why", ["not JSON (JSONDecodeError)", "not an object"])
    def test_an_unreadable_record_is_loud(self, catalogue, why):
        owners, after = catalogue
        # Even with the watermark on what would have been a recorded instant:
        # a record nobody can parse says nothing, so nothing is the chain's.
        stamps = {PRODUCTS: _stamp(after, [_us(after)], unreadable=why),
                  CATEGORIES: _stamp(after, [_us(after)])}
        found = _overwrote(_chain_latch_findings(owners, {}, stamps))
        assert set(found) == {PRODUCTS}
        assert found[PRODUCTS].severity.value == "CRITICAL"
        assert "cannot be read" in found[PRODUCTS].description
        assert why in found[PRODUCTS].description

    def test_an_unreadable_record_before_the_latch_is_not_judged(self, catalogue):
        owners, after = catalogue
        before = after - timedelta(days=1)
        stamps = {PRODUCTS: _stamp(before, unreadable="not an object"),
                  CATEGORIES: _stamp(before)}
        assert _overwrote(_chain_latch_findings(owners, {}, stamps)) == {}

    def test_a_record_not_read_is_loud(self, catalogue):
        """The reader answers every declared table; if one is ever missing the
        verdict falls back to the earlier watermark and says it could not
        tell, rather than letting the table through."""
        owners, after = catalogue
        stamps = {CATEGORIES: _stamp(after, [_us(after)])}
        found = _overwrote(_chain_latch_findings(
            owners, {PRODUCTS: {"last_ok_at": after}}, stamps))
        assert set(found) == {PRODUCTS}
        assert "was not read" in found[PRODUCTS].description

    def test_the_statements_watermark_is_the_one_judged(self, catalogue):
        """`watermarks` is read at the top of the run; a chain write after it
        moves the record on. The record's statement carries its own
        `last_ok_at`, and that is the one paired with the record — both
        ways round."""
        owners, after = catalogue
        older, newer = after, after + timedelta(minutes=60)
        torn = {PRODUCTS: {"last_ok_at": older}, CATEGORIES: {"last_ok_at": older}}
        stamps = {PRODUCTS: _stamp(newer, [_us(newer)]),
                  CATEGORIES: _stamp(newer, [_us(newer)])}
        assert _overwrote(_chain_latch_findings(owners, torn, stamps)) == {}

        before = after - timedelta(days=1)
        stale = {PRODUCTS: {"last_ok_at": before}, CATEGORIES: {"last_ok_at": before}}
        stamps = {PRODUCTS: _stamp(newer, [_us(older)]),
                  CATEGORIES: _stamp(before)}
        assert set(_overwrote(_chain_latch_findings(owners, stale, stamps))) == {PRODUCTS}

    def test_a_chain_that_stamps_without_a_record_is_not_judged_there(self):
        """Chain 3 latched and writing: its three bronze watermarks move with
        every write, so after the latch is its normal state. The misses are
        not its stamp — the hourly copy's alone — and stay judged."""
        since = chain_latch.latch(pg_orders_write.CHAIN)
        owners = {t: since for t in pg_orders_write.CHAIN_TABLES}
        after = datetime.fromisoformat(since) + timedelta(hours=1)
        watermarks = {t: {"last_ok_at": after} for t in pg_orders_write.CHAIN_TABLES}
        issues = _chain_latch_findings(owners, watermarks, {})
        assert set(_overwrote(issues)) == {MISSES}
        assert not [i for i in issues if i.check_name == "chain_latch_disagrees"]

    def test_with_its_marker_lost_chain_3_is_still_paged(self):
        """What stands in for E1 on chain 3's tables: the marker gone, the
        owner rows standing — the only state in which a shipper reaches them."""
        since = datetime.now(timezone.utc).isoformat()
        owners = {t: since for t in pg_orders_write.CHAIN_TABLES}
        issues = _chain_latch_findings(owners, {}, {})
        (disagrees,) = [i for i in issues if i.check_name == "chain_latch_disagrees"]
        assert disagrees.table_name == pg_orders_write.CHAIN
        assert disagrees.severity.value == "CRITICAL"

    def test_a_chain_that_never_stamps_is_judged_as_before(self):
        """Chain 8: nothing but the hourly copy stamps `app.manual_expenses`,
        so after the latch it is a shipment, record or none."""
        since = chain_latch.latch(pg_expenses_write.CHAIN)
        owners = {MANUAL_EXPENSES: since}
        after = datetime.fromisoformat(since) + timedelta(minutes=5)
        found = _overwrote(_chain_latch_findings(
            owners, {MANUAL_EXPENSES: {"last_ok_at": after}}, {}))
        assert set(found) == {MANUAL_EXPENSES}
        assert "a shipment out of DuckDB" in found[MANUAL_EXPENSES].description
        assert "Do not re-run the shipper" in found[MANUAL_EXPENSES].description

        before = datetime.fromisoformat(since) - timedelta(minutes=5)
        assert _overwrote(_chain_latch_findings(
            owners, {MANUAL_EXPENSES: {"last_ok_at": before}}, {})) == {}


# ─── The caller ─────────────────────────────────────────────────────────────


class _Ctx:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, *exc):
        return False


class _Pool:
    def __init__(self):
        self.acquired = 0

    def acquire(self):
        self.acquired += 1
        return _Ctx(object())


@pytest.fixture
def reader(monkeypatch):
    calls = []

    async def read(conn):
        calls.append(conn)
        return {PRODUCTS: "p", CATEGORIES: "c"}

    monkeypatch.setattr(pg_catalogue_write, "read_mirror_stamps", read)
    return calls


class TestTheCallerReadsIt:
    @pytest.mark.asyncio
    async def test_not_for_a_chain_that_has_not_changed_hands(self, reader):
        pool = _Pool()
        assert await _read_mirror_stamps(pool, {}) == {}
        assert pool.acquired == 0 and reader == []

    @pytest.mark.asyncio
    async def test_on_an_owner_row(self, reader):
        pool = _Pool()
        got = await _read_mirror_stamps(pool, {CATEGORIES: "2026-10-09T10:45:13+00:00"})
        assert got == {PRODUCTS: "p", CATEGORIES: "c"}
        assert pool.acquired == 1 and len(reader) == 1

    @pytest.mark.asyncio
    async def test_on_the_marker_alone(self, reader):
        chain_latch.latch(pg_catalogue_write.CHAIN)
        pool = _Pool()
        assert await _read_mirror_stamps(pool, {}) == {PRODUCTS: "p", CATEGORIES: "c"}
        assert pool.acquired == 1

    def test_reconcile_operational_hands_the_read_to_the_verdict(self):
        """The verdict is pure; this pins that the comparison actually reads
        the record — with the owner rows it already holds — and hands it in.
        Without it every declared table falls to "was not read"."""
        from core.mirror_reconciliation import reconcile_operational

        tree = ast.parse(textwrap.dedent(inspect.getsource(reconcile_operational)))
        (call,) = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
                   and getattr(n.func, "id", "") == "_chain_latch_findings"]
        assert len(call.args) == 3
        third = call.args[2]
        assert isinstance(third, ast.Await) and isinstance(third.value, ast.Call)
        assert third.value.func.id == "_read_mirror_stamps"
        assert [a.id for a in third.value.args] == ["pool", "owners"]
        assert [a.id for a in call.args[:2]] == ["owners", "watermarks"]
