"""The SMS audience tests pass whatever the wall clock says during the run.

`tests/sms_audience_fixture.py` counts every date back from today in Kyiv,
because the query counts from `TODAY_IN_KYIV`, which it reads when it runs.
Until 2026-10-21 the fixture read its today once, at import — which is
collection, half an hour before CI reaches these tests. PR #305's run
39494883413 collected at 23:47 Kyiv and reached them at 00:11: every row was
seeded a day older than the query thought, buyer 12 ("exactly 92 days") was
at 93, and eight tests failed on a change that touched nothing here.

Two things are held here, with a clock the test sets:

* **A suite collected before midnight and run after it still finds the 92nd
  day.** The fixture is loaded with its clock at yesterday 23:59 Kyiv, then
  run against a real store, whose today cannot be set and is today — the
  same disagreement CI had, a day between collection and query.
* **Nothing in the fixture, or in a test module that imports it, is dated at
  import.** Each is loaded twice, with the clock two days apart, and every
  date reachable from its namespace — constants, parametrize tables, class
  attributes, defaults, closures, dataclass fields — must be the same both
  times. A date that moved was read off the clock at collection. The
  modules are found by what they import, not listed.
"""
from __future__ import annotations

import ast
import dataclasses
import datetime as datetime_module
import importlib
import importlib.util
import sys
import types
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

import tests
from core.duckdb_store import DuckDBStore
from core.repositories.customers import SmsAudienceFilters, SmsAudienceGroup

KYIV = ZoneInfo("Europe/Kyiv")
TESTS = Path(tests.__file__).parent
ROOT = TESTS.parent
FIXTURE = "tests.sms_audience_fixture"

# The run that failed was collected at 23:47 Kyiv and reached these tests at
# 00:11. The guard loads each module at that collection and two days later:
# 23:47 against 00:11 moves Kyiv's calendar and not UTC's or the laptop's, so
# a `date.today()` at import would pass it; two days apart moves every zone's.
COLLECTED = datetime(2026, 10, 20, 23, 47, tzinfo=KYIV)
LATER = COLLECTED + timedelta(days=2)


class _Clock:
    """What `datetime.now` and `date.today` answer inside a module loaded
    here: `moment`, or the real clock once it is None."""

    def __init__(self, moment: datetime | None):
        self.moment = moment

    def now(self, tz=None) -> datetime:
        if self.moment is None:
            return datetime.now(tz)
        if tz is None:
            return self.moment.astimezone().replace(tzinfo=None)
        return self.moment.astimezone(tz)


def _clocked(clock: _Clock):
    class ClockedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock.now(tz)

        @classmethod
        def today(cls):
            return clock.now()

    class ClockedDate(date):
        @classmethod
        def today(cls):
            return clock.now().date()

    return ClockedDatetime, ClockedDate


def _module_name(path: Path) -> str:
    return ".".join(path.relative_to(ROOT).with_suffix("").parts)


def _load(name: str, clock: _Clock, monkeypatch, fixture=None):
    """A fresh copy of module `name`, executed with `datetime` and `date`
    answering `clock`, and with `fixture` — a copy of the audience fixture
    loaded the same way — standing in for the real one while it runs.

    The real module is imported first, so everything it imports is already
    loaded under the real clock: only the copy ever sees this one."""
    importlib.import_module(name)
    spec = importlib.util.find_spec(name)
    copy = importlib.util.module_from_spec(spec)
    with monkeypatch.context() as m:
        clocked_datetime, clocked_date = _clocked(clock)
        m.setattr(datetime_module, "datetime", clocked_datetime)
        m.setattr(datetime_module, "date", clocked_date)
        if fixture is not None:
            m.setitem(sys.modules, FIXTURE, fixture)
            m.setattr(tests, FIXTURE.rsplit(".", 1)[1], fixture)
        # A class's module is looked up here while it is being built
        # (dataclasses do), so the copy is registered while it runs.
        m.setitem(sys.modules, name, copy)
        spec.loader.exec_module(copy)
    return copy


def _importers() -> list[str]:
    """Every module under tests/ that imports the audience fixture."""
    leaf = FIXTURE.rsplit(".", 1)[1]
    found = []
    for path in sorted(TESTS.rglob("*.py")):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.ImportFrom) and (
                node.module == FIXTURE
                or (node.module == "tests" and any(a.name == leaf for a in node.names))
            ) or isinstance(node, ast.Import) and any(
                a.name == FIXTURE for a in node.names
            ):
                found.append(_module_name(path))
                break
    return found


_SCALARS = (str, bytes, int, float, complex, bool, type(None))


def _dates(value, home: str, path: str, seen: set):
    """(path, date) for every date reachable from `value` without leaving
    module `home`: into containers, dataclass fields and pytest marks, and
    into the classes and functions `home` defines — their attributes, their
    parametrize tables, their defaults and closures. Not into other modules,
    and not into functions' bodies, which run when a test does."""
    if isinstance(value, date):
        yield path, value
        return
    if isinstance(value, _SCALARS) or isinstance(value, types.ModuleType):
        return
    if id(value) in seen:
        return
    seen.add(id(value))
    if isinstance(value, dict):
        for key, item in value.items():
            yield from _dates(key, home, f"{path}[key {key!r}]", seen)
            yield from _dates(item, home, f"{path}[{key!r}]", seen)
    elif isinstance(value, (list, tuple)):
        for i, item in enumerate(value):
            yield from _dates(item, home, f"{path}[{i}]", seen)
    elif isinstance(value, (set, frozenset)):
        for i, item in enumerate(sorted(value, key=repr)):
            yield from _dates(item, home, f"{path}{{{i}}}", seen)
    elif isinstance(value, (staticmethod, classmethod)):
        yield from _dates(value.__func__, home, path, seen)
    elif isinstance(value, type):
        if value.__module__ == home:
            for key, item in vars(value).items():
                yield from _dates(item, home, f"{path}.{key}", seen)
    elif isinstance(value, types.FunctionType):
        if value.__module__ == home:
            for i, mark in enumerate(getattr(value, "pytestmark", ())):
                yield from _dates(mark, home, f"{path}@mark{i}", seen)
            yield from _dates(value.__defaults__ or (), home,
                              f"{path}.__defaults__", seen)
            yield from _dates(value.__kwdefaults__ or {}, home,
                              f"{path}.__kwdefaults__", seen)
            for i, cell in enumerate(value.__closure__ or ()):
                try:
                    contents = cell.cell_contents
                except ValueError:          # a cell not yet filled
                    continue
                yield from _dates(contents, home, f"{path}.__closure__[{i}]", seen)
    elif dataclasses.is_dataclass(value):
        for field in dataclasses.fields(value):
            yield from _dates(getattr(value, field.name), home,
                              f"{path}.{field.name}", seen)


def _dated_at_import(module) -> dict[str, date]:
    seen: set = set()
    return {
        path: value
        for key, item in vars(module).items() if not key.startswith("__")
        for path, value in _dates(item, module.__name__, key, seen)
    }


def _moved(name: str, monkeypatch) -> dict[str, tuple[date, date]]:
    """The dates in module `name` that are not the same when it is loaded
    at COLLECTED and at LATER, with the fixture loaded at each too."""
    loads = []
    for moment in (COLLECTED, LATER):
        clock = _Clock(moment)
        fx = _load(FIXTURE, clock, monkeypatch)
        module = fx if name == FIXTURE else _load(name, clock, monkeypatch, fx)
        loads.append(_dated_at_import(module))
    before, after = loads
    return {path: (before.get(path), after.get(path))
            for path in before.keys() | after.keys()
            if before.get(path) != after.get(path)}


class TestTheGuardSeesWhereADateHides:
    """The comparison is only as good as the walk: every place a date can be
    built at import must show up as moved."""

    SOURCE = '''
import pytest
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from core.repositories.customers import SmsAudienceFilters, SmsAudienceGroup

TODAY = datetime.now(ZoneInfo("Europe/Kyiv")).date()
LOCAL = date.today()
NESTED = [("label", {"from": TODAY - timedelta(days=1)}, {TODAY})]
GROUP = SmsAudienceGroup(365, SmsAudienceFilters(period_orders_min=2,
                                                 period_from=TODAY))
FIXED = date(2026, 1, 1)


def _make():
    day = TODAY
    return lambda: day


CLOSED = _make()


def defaulted(day=TODAY):
    return day


class TestSomething:
    CASES = [("a", TODAY)]

    @pytest.mark.parametrize("day", [TODAY - timedelta(days=200)])
    def test_it(self, day):
        pass
'''

    def test_each_place_is_found(self, tmp_path, monkeypatch):
        (tmp_path / "dated_module.py").write_text(self.SOURCE, encoding="utf-8")
        monkeypatch.syspath_prepend(str(tmp_path))
        try:
            moved = _moved("dated_module", monkeypatch)
        finally:
            sys.modules.pop("dated_module", None)
        assert sorted(moved) == sorted([
            "TODAY", "LOCAL", "NESTED[0][1]['from']", "NESTED[0][2]{0}",
            "GROUP.filters.period_from", "CLOSED.__closure__[0]",
            "defaulted.__defaults__[0]", "TestSomething.CASES[0][1]",
            "TestSomething.test_it@mark0.args[1][0]",
        ])
        assert "FIXED" not in moved, "a date written out is not the clock's"
        assert moved["TODAY"] == (COLLECTED.date(), LATER.date())


class TestNothingIsDatedAtImport:
    def test_the_importers_are_found(self):
        importers = _importers()
        assert "tests.unit.test_sms_audience_groups" in importers
        assert "tests.integration.test_sms_audience_groups_two_engines" in importers

    @pytest.mark.parametrize("name", [FIXTURE, *_importers()])
    def test_no_date_moves_with_the_clock(self, name, monkeypatch):
        moved = _moved(name, monkeypatch)
        assert not moved, (
            f"{name} builds these at import, from the clock — a suite that "
            f"crosses midnight in Kyiv seeds or expects them a day off. Build "
            f"them when the test runs: {sorted(moved)}")


@pytest.mark.asyncio
async def test_a_suite_collected_before_midnight_still_finds_the_92nd_day(
    tmp_path, monkeypatch,
):
    """Collected yesterday at 23:59, run today: buyer 12 is exactly 92 days
    back from the store's today, and stays in the 92-day window."""
    yesterday = datetime.now(KYIV).date() - timedelta(days=1)
    clock = _Clock(datetime.combine(yesterday, time(23, 59), KYIV))
    fx = _load(FIXTURE, clock, monkeypatch)          # collection
    clock.moment = None                              # the run, past midnight
    store = DuckDBStore(db_path=tmp_path / "midnight.duckdb")
    await store.connect()
    try:
        async with store.connection() as conn:
            fx.seed_duckdb_conn(conn)

        async def ids(group):
            result = await store.get_sms_segments(
                groups=[group], grouping="single", holdout_pct=0,
                include_customers=True)
            return {c["buyerId"] for c in result["customers"]}

        last_92_days = await ids(SmsAudienceGroup(92, SmsAudienceFilters()))
        assert 12 in last_92_days and 13 not in last_92_days
        assert last_92_days == fx.B_ALONE
        assert fx.matched(92) == fx.B_MATCHED
        # The table pytest built at collection, run after it.
        for label, window, fields, terms, answer in fx.PERIOD_CASES:
            assert fx.recipients(fx.matched(window, **terms)) == answer, label
            got = await ids(SmsAudienceGroup(window, SmsAudienceFilters(**fields())))
            assert got == answer, label
    finally:
        await store.close()
