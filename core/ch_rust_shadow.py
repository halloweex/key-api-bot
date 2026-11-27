"""The Rust shadow of `ch_sync`, judged at night (the Rust plan, revision 4, К1).

The Rust worker (`rust/crates/ks-worker`) ships ClickHouse's copy beside
Python's, under its own names: `silver.orders_rust`, `gold.daily_revenue_rust`,
`history.order_versions_rust`, and the state rows `rust_shadow.clickhouse.*`
in `meta.mirror_state`. Nothing reads them. This module does, once a night:
the shadow against Postgres, the way the morning check judges the live copy,
and the shadow against the copy Python shipped, tolerance zero. Seven nights
in a row with nothing found is the condition for switching `ch_sync` to Rust.

It stands down unless `KS_WORKER_SHADOW` lists `ch_sync` — the variable the
worker reads to run the shadow at all, from the same `.env` — and when
`executor:ch_sync` says `rust`, because then the live copy is Rust's own and
there is nothing left to judge the shadow against.

WHY PYTHON JUDGES, NOT THE WORKER

The judge is the incumbent's own comparison code: `ch_silver.parse_tsv`,
`compare_gold_cells`, the archive's buckets. A worker judging itself could
share a bug between what it ships and how it reads it back, and the
comparison would agree with the copy. The worker needs no new right either:
it writes its shadow tables and its `rust_shadow.*` rows, and the journal,
the digest and the alert Gate stay Python's. When the checks move to Rust
(К4), this moves with them.

WHEN TWO COPIES ARE COMPARABLE

The copies are shipped by two processes at different minutes of the hour,
and Silver may be rebuilt between them. A copy is of the Silver it read, and
`rebuild_silver` stamps `meta.mirror_state['silver.orders']` in the same
transaction as the rows, with the transaction's start. So, read in one
snapshot with that stamp:

- a shadow equal to Postgres' Silver is right, whenever it was shipped;
- a shadow that differs, shipped more than `MARGIN` after the stamp, read the
  Silver Postgres holds now — the difference is the shadow's;
- a shadow that differs, shipped before the stamp or within `MARGIN` of it,
  may be of the previous Silver. Nothing can be said; the next attempt asks
  again.

`MARGIN` covers the seconds between a ship's read and its stamp, and between
a rebuild's stamp and its commit. The hourly heartbeat rebuilds Silver with
nothing changed, which moves the stamp and not the rows, so on a quiet night
the first rule decides.

The live copy is compared only when the same rule says it is of the same
Silver, and Gold only when each side derived it after its own Silver. The
archive is compared below a cut — `MARGIN` before the earlier of the two
ships — so a version committed between the two id scans is not a difference.

WHEN IT RUNS, AND WHEN A NIGHT IS JOURNALLED

At 01:20, 02:20, 03:20 and 04:20 Kyiv, when orders stand. The first attempt
that compared every part, or found a difference, journals the night as layer
`rust_shadow` and the rest of the night stands down. An attempt that could
not compare a part and found nothing waits for the next one; the last one
journals what it has, with `rust_shadow_unwatched` naming what it could not
compare. Every finding is WARN: the shadow serves nobody, so the 09:00 digest
is where it is read, and nothing here pages.
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

from core.ch_common import URL_ENV, configured, execute as _execute
from core.data_quality import IntegrityIssue, Severity

logger = logging.getLogger(__name__)

SHADOW_ENV = "KS_WORKER_SHADOW"
JOB = "ch_sync"
LAYER = "rust_shadow"
SWITCH_KEY = f"executor:{JOB}"
PREFIX = "rust_shadow."
SUFFIX = "_rust"

SILVER_LIVE = "silver.orders"
SILVER_SHADOW = SILVER_LIVE + SUFFIX
GOLD_LIVE = "gold.daily_revenue"
GOLD_SHADOW = GOLD_LIVE + SUFFIX

# The state rows: Postgres' two rebuilds, and the three ships of each side.
SILVER_STAMP = "silver.orders"
GOLD_STAMP = "gold.daily_revenue"
SILVER_SHIP = "clickhouse.silver_orders"
GOLD_SHIP = "clickhouse.gold_daily_revenue"
HISTORY_SHIP = "clickhouse.order_versions"
SHIPS = (SILVER_SHIP, GOLD_SHIP, HISTORY_SHIP)

MARGIN = timedelta(minutes=5)
# The worker ships hourly; a shadow row older than this is a worker not running.
STALE = timedelta(hours=3)
# The job's hours (Kyiv). The last one journals whatever it has.
ATTEMPT_HOURS = "1-4"
FINAL_HOUR = 4
KYIV = ZoneInfo("Europe/Kyiv")
MAX_SAMPLES = 10
BUCKET = 10_000


def shadow_configured() -> bool:
    """`KS_WORKER_SHADOW` lists `ch_sync`, as the worker reads it."""
    return any(job.strip() == JOB for job in os.getenv(SHADOW_ENV, "").split(","))


def history_tables() -> Tuple[str, str]:
    """(live, shadow): `ch_history.table()`, which `KS_CH_HISTORY_DB` moves."""
    from core.ch_history import table

    live = table()
    return live, live + SUFFIX


# ─── What one snapshot of Postgres says ─────────────────────────────────────

@dataclass(frozen=True)
class State:
    """One `meta.mirror_state` row; an absent row is all None."""
    last_ok_at: Optional[datetime] = None
    failures: int = 0
    last_error: Optional[str] = None


@dataclass
class Snapshot:
    now: datetime
    states: Dict[str, State]
    switch: Optional[str]
    silver: List[Tuple[Any, ...]]
    gold: List[Tuple[Any, ...]]
    # The archive's buckets below the cut, None when the shadow never shipped it.
    history_cut: Optional[datetime] = None
    history_buckets: Optional[Dict[int, int]] = None

    def state(self, name: str) -> State:
        return self.states.get(name, State())


def comparable(stamp: Optional[datetime], ship: Optional[datetime],
               margin: timedelta = MARGIN) -> bool:
    """Whether a copy shipped at `ship` read the Silver stamped `stamp`."""
    return stamp is not None and ship is not None and stamp + margin < ship


def history_cut(states: Mapping[str, State], margin: timedelta = MARGIN) -> Optional[datetime]:
    """`margin` before the shadow's last archive ship: every version captured
    before it had committed when the shadow scanned Postgres' ids."""
    shipped = states.get(PREFIX + HISTORY_SHIP, State()).last_ok_at
    return None if shipped is None else shipped - margin


def silver_rows(records: Sequence[Mapping[str, Any]]) -> List[Tuple[Any, ...]]:
    """Postgres' Silver as the copy holds it: `_fetch_pg_silver`'s rule, the
    timestamps in UTC to the second."""
    from core.ch_silver import _TYPES
    from core.pg_silver import SILVER_COLUMNS

    out = []
    for record in records:
        row = []
        for name in SILVER_COLUMNS:
            value = record[name]
            if _TYPES[name] == "ts" and value is not None:
                value = value.astimezone(timezone.utc).replace(microsecond=0)
            row.append(value)
        out.append(tuple(row))
    return out


async def read_snapshot(*, margin: timedelta = MARGIN) -> Snapshot:
    """The stamps, the switch, Silver, Gold and the archive's buckets, in one
    REPEATABLE READ snapshot: whatever commits meanwhile, they agree."""
    from core.pg import get_pool
    from core.pg_gold import GOLD_COLUMNS
    from core.pg_silver import SILVER_COLUMNS

    names = [SILVER_STAMP, GOLD_STAMP, *SHIPS, *(PREFIX + s for s in SHIPS)]
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction(isolation="repeatable_read", readonly=True):
            now = await conn.fetchval("SELECT now()")
            states = {
                r["table_name"]: State(r["last_ok_at"], int(r["failures_since_ok"] or 0),
                                       r["last_error"])
                for r in await conn.fetch(
                    "SELECT table_name, last_ok_at, failures_since_ok, last_error "
                    "FROM meta.mirror_state WHERE table_name = ANY($1::text[])", names)
            }
            switch = await conn.fetchval(
                "SELECT value FROM meta.chain_watermarks WHERE key = $1", SWITCH_KEY)
            silver = silver_rows(await conn.fetch(
                f"SELECT {', '.join(SILVER_COLUMNS)} FROM silver.orders"))
            gold = [tuple(r) for r in await conn.fetch(
                f"SELECT {', '.join(GOLD_COLUMNS)} FROM gold.daily_revenue")]
            cut = history_cut(states, margin)
            buckets = None
            if cut is not None:
                buckets = {int(r[0]): int(r[1]) for r in await conn.fetch(
                    f"SELECT id / {BUCKET} AS b, count(*) FROM app.order_versions "
                    f"WHERE captured_at < $1 GROUP BY 1", cut)}
    return Snapshot(now=now, states=states, switch=switch, silver=silver, gold=gold,
                    history_cut=cut, history_buckets=buckets)


# ─── The findings ───────────────────────────────────────────────────────────

def _issue(*, check_name: str, table: str, count: int, description: str,
           samples: Sequence[int] = ()) -> IntegrityIssue:
    return IntegrityIssue(check_name=check_name, table_name=table, severity=Severity.WARN,
                          count=max(int(count), 1), sample_ids=tuple(samples[:MAX_SAMPLES]),
                          description=description)


# The four that say the shadow differs. Only these end a night early. A
# difference from the live copy is keyed on the live table, one from Postgres
# on the shadow's: the journal keeps one finding per (check, table).
DIFFERS = ("rust_shadow_silver_differs", "rust_shadow_gold_differs",
           "rust_shadow_history_differs", "rust_shadow_live_differs")


def liveness(states: Mapping[str, State], now: datetime,
             stale: timedelta = STALE) -> List[IntegrityIssue]:
    """The shadow's own state rows: never shipped, behind, or failing."""
    behind, failing = [], []
    for ship in SHIPS:
        row = states.get(PREFIX + ship, State())
        if row.last_ok_at is None:
            behind.append(f"{PREFIX}{ship} records no successful ship")
        elif now - row.last_ok_at > stale:
            behind.append(f"{PREFIX}{ship} last shipped "
                          f"{(now - row.last_ok_at).total_seconds() / 3600:.1f} h ago")
        if row.failures:
            error = (row.last_error or "").strip().splitlines()
            failing.append(f"{PREFIX}{ship}: {row.failures} failure(s) since its last "
                           f"success, the last {error[0][:200] if error else 'unrecorded'}")
    issues = []
    if behind:
        issues.append(_issue(
            check_name="rust_shadow_stale", table=SILVER_SHADOW, count=len(behind),
            description="the Rust worker's shadow is not being shipped: " + "; ".join(behind)
            + ". The worker runs hourly when KS_WORKER_SHADOW lists ch_sync; its log "
              "says why it did not"))
    if failing:
        issues.append(_issue(
            check_name="rust_shadow_failing", table=SILVER_SHADOW, count=len(failing),
            description="the Rust worker's shadow ships are failing: " + "; ".join(failing)))
    return issues


def compare_rows(want: Mapping[Any, Tuple[Any, ...]], got: Mapping[Any, Tuple[Any, ...]],
                 ) -> Tuple[List[Any], List[Any], List[Any]]:
    """(lost, invented, changed) keys, each sorted."""
    lost = sorted(set(want) - set(got))
    invented = sorted(set(got) - set(want))
    changed = sorted(k for k in set(want) & set(got) if want[k] != got[k])
    return lost, invented, changed


def gold_finding(found: Sequence[IntegrityIssue], against: str) -> Optional[IntegrityIssue]:
    """`compare_gold_cells`' findings as one: the shadow's Gold differs."""
    if not found:
        return None
    count = sum(i.count for i in found)
    samples = list(dict.fromkeys(s for i in found for s in i.sample_ids))
    parts = "; ".join(f"{i.check_name.removeprefix('ch_engines_gold_')}: {i.count}"
                      for i in found)
    return _issue(
        check_name="rust_shadow_gold_differs", table=GOLD_SHADOW, count=count,
        description=f"the shadow's Gold differs from {against} ({parts}). "
        + " ".join(i.description for i in found)[:600], samples=samples)


@dataclass
class Verdict:
    issues: List[IntegrityIssue] = field(default_factory=list)
    # What could not be compared, and why: empty when every part was.
    unwatched: List[str] = field(default_factory=list)
    skipped: Optional[str] = None
    compared: List[str] = field(default_factory=list)

    @property
    def differs(self) -> bool:
        return any(i.check_name in DIFFERS for i in self.issues)


def judge_silver_and_gold(snap: Snapshot, shadow_silver: Sequence[Tuple[Any, ...]],
                          shadow_gold: Sequence[Tuple[Any, ...]], verdict: Verdict,
                          margin: timedelta = MARGIN) -> bool:
    """The shadow's Silver against Postgres', then its Gold. True when the
    shadow's Silver equals Postgres' — the condition for the live comparison."""
    from core.ch_silver import compare_gold_cells

    stamp = snap.state(SILVER_STAMP).last_ok_at
    ship = snap.state(PREFIX + SILVER_SHIP).last_ok_at
    lost, invented, changed = compare_rows({r[0]: r for r in snap.silver},
                                           {r[0]: r for r in shadow_silver})
    if lost or invented or changed:
        if not comparable(stamp, ship, margin):
            verdict.unwatched.append(
                f"Silver: rebuilt at {stamp}, the shadow last shipped at {ship}, and the "
                f"two differ — the shadow may be of the Silver before")
            verdict.unwatched.append("Gold: waits for a comparable Silver")
            return False
        verdict.compared.append("silver")
        verdict.issues.append(_issue(
            check_name="rust_shadow_silver_differs", table=SILVER_SHADOW,
            count=len(lost) + len(invented) + len(changed),
            description=f"the shadow's Silver differs from Postgres': {len(lost)} lost, "
            f"{len(invented)} invented, {len(changed)} changed, though it was shipped at "
            f"{ship}, after Silver was rebuilt at {stamp}. Tolerance zero",
            samples=(lost + invented + changed)))
        return False
    verdict.compared.append("silver")

    gold_ship = snap.state(PREFIX + GOLD_SHIP).last_ok_at
    gold_stamp = snap.state(GOLD_STAMP).last_ok_at
    if gold_ship is None or ship is None or gold_ship < ship:
        verdict.unwatched.append(
            f"Gold: the shadow's last Gold ({gold_ship}) is not derived from its last "
            f"Silver ({ship})")
    elif gold_stamp is None or stamp is None or gold_stamp < stamp:
        verdict.unwatched.append(
            f"Gold: Postgres' Gold ({gold_stamp}) is older than its Silver ({stamp})")
    else:
        verdict.compared.append("gold")
        finding = gold_finding(compare_gold_cells(snap.gold, shadow_gold), "Postgres'")
        if finding is not None:
            verdict.issues.append(finding)
    return True


def judge_history(snap: Snapshot, shadow_buckets: Mapping[int, int],
                  verdict: Verdict) -> None:
    """The shadow archive's buckets below the cut against Postgres'."""
    if snap.history_cut is None or snap.history_buckets is None:
        verdict.unwatched.append("archive: the shadow has never shipped it")
        return
    verdict.compared.append("archive")
    bad = sorted(b for b in set(shadow_buckets) | set(snap.history_buckets)
                 if shadow_buckets.get(b, 0) != snap.history_buckets.get(b, 0))
    if bad:
        held = sum(shadow_buckets.values())
        want = sum(snap.history_buckets.values())
        verdict.issues.append(_issue(
            check_name="rust_shadow_history_differs", table=history_tables()[1], count=len(bad),
            description=f"{len(bad)} id-bucket(s) of the shadow archive disagree with Postgres' below "
            f"{snap.history_cut}: the shadow holds {held} version(s) captured before it, "
            f"Postgres {want}", samples=bad))


# ─── ClickHouse ─────────────────────────────────────────────────────────────

def ts_literal(at: datetime) -> str:
    """`at` as ClickHouse's DateTime64(6, 'UTC')."""
    return (f"toDateTime64('{at.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')}'"
            f", 6, 'UTC')")


def parse_difference(text: str) -> Tuple[int, int, List[int]]:
    """(rows only in `a`, rows only in `b`, sorted sample keys) of one comparison."""
    only_a, only_b, samples = text.strip().split("\t")
    return int(only_a), int(only_b), [int(s) for s in json.loads(samples)]


def difference_sql(a: str, b: str, *, columns: Sequence[str], key: str,
                   where: str = "") -> str:
    """The two tables as multisets, compared in ClickHouse: every distinct row
    with how many more times `a` holds it than `b`. Not `EXCEPT`: ClickHouse
    24.8's `EXCEPT ALL` — and plain `EXCEPT`, whose default mode is ALL —
    answered nothing for two tables that differ in a column other than the
    first, where `EXCEPT DISTINCT` saw the row; and DISTINCT cannot see a
    duplicated row. GROUP BY puts NULL with NULL, which is how a copy should
    compare."""
    cols = ", ".join(columns)
    return (
        f"SELECT countIf(d > 0), countIf(d < 0), "
        f"arraySort(arrayDistinct(groupArray({2 * MAX_SAMPLES})(k))) FROM ("
        f"SELECT {key} AS k, sum(side) AS d FROM ("
        f"SELECT {cols}, 1 AS side FROM {a}{where} UNION ALL "
        f"SELECT {cols}, -1 AS side FROM {b}{where}"
        f") GROUP BY {cols} HAVING d != 0) FORMAT TabSeparated")


async def same_rows(a: str, b: str, *, columns: Sequence[str], key: str,
                    where: str = "") -> Tuple[int, int, List[int]]:
    """(rows only in `a`, rows only in `b`, sample keys): two zeros mean the
    two tables hold the same rows, each as many times. A changed row is one
    of each."""
    only_a, only_b, samples = parse_difference(await _execute(
        difference_sql(a, b, columns=columns, key=key, where=where)))
    return only_a, only_b, samples[:MAX_SAMPLES]


async def _live_comparison(snap: Snapshot, verdict: Verdict, margin: timedelta) -> None:
    """The shadow against Python's copy in ClickHouse, row for row, where the
    copies are of one Silver."""
    stamp = snap.state(SILVER_STAMP).last_ok_at
    python_ship = snap.state(SILVER_SHIP).last_ok_at
    if not comparable(stamp, python_ship, margin):
        verdict.unwatched.append(
            f"the live copy: Python last shipped Silver at {python_ship}, Silver was "
            f"rebuilt at {stamp} — compared against Postgres instead")
        return
    from core.pg_silver import SILVER_COLUMNS

    only_shadow, only_live, samples = await same_rows(
        SILVER_SHADOW, SILVER_LIVE, columns=SILVER_COLUMNS, key="id")
    verdict.compared.append("live silver")
    if only_shadow or only_live:
        verdict.issues.append(_issue(
            check_name="rust_shadow_live_differs", table=SILVER_LIVE,
            count=only_shadow + only_live,
            description=f"the live Silver copy and the shadow are of one Silver and differ: "
            f"{only_shadow} row(s) only in the shadow, {only_live} only in the live copy. "
            f"The shadow equals Postgres, so the live copy is the one to look at "
            f"(ch_silver_roundtrip judges it at 07:30)", samples=samples))
        return
    python_gold = snap.state(GOLD_SHIP).last_ok_at
    shadow_gold = snap.state(PREFIX + GOLD_SHIP).last_ok_at
    shadow_silver = snap.state(PREFIX + SILVER_SHIP).last_ok_at
    if (python_gold is None or python_ship is None or python_gold < python_ship
            or shadow_gold is None or shadow_silver is None or shadow_gold < shadow_silver):
        verdict.unwatched.append("the live Gold: a side's Gold is not derived from its Silver")
        return
    from core.pg_gold import GOLD_COLUMNS

    only_shadow, only_live, samples = await same_rows(
        GOLD_SHADOW, GOLD_LIVE, columns=GOLD_COLUMNS, key="toYYYYMMDD(date)")
    verdict.compared.append("live gold")
    if only_shadow or only_live:
        verdict.issues.append(_issue(
            check_name="rust_shadow_gold_differs", table=GOLD_LIVE,
            count=only_shadow + only_live,
            description=f"the shadow's Gold differs from the live Gold derived from the same Silver: "
            f"{only_shadow} cell(s) only in the shadow, {only_live} only in the live copy. "
            f"One engine, one statement — no cent is allowed here", samples=samples))


async def _live_history(snap: Snapshot, verdict: Verdict, margin: timedelta) -> None:
    live, shadow = history_tables()
    python_ship = snap.state(HISTORY_SHIP).last_ok_at
    rust_ship = snap.state(PREFIX + HISTORY_SHIP).last_ok_at
    if python_ship is None or rust_ship is None:
        verdict.unwatched.append("the live archive: a side has never shipped it")
        return
    cut = min(python_ship, rust_ship) - margin
    where = f" WHERE captured_at < {ts_literal(cut)}"
    from core.ch_history import COLUMNS as HISTORY_COLUMNS

    only_shadow, only_live, samples = await same_rows(
        shadow, live, columns=HISTORY_COLUMNS, key="id", where=where)
    verdict.compared.append("live archive")
    if only_shadow or only_live:
        verdict.issues.append(_issue(
            check_name="rust_shadow_history_differs", table=live,
            count=only_shadow + only_live,
            description=f"the shadow archive and the live one differ below {cut}: {only_shadow} "
            f"version(s) only in the shadow, {only_live} only in the live copy",
            samples=samples))


async def judge(*, margin: timedelta = MARGIN) -> Verdict:
    """Tonight's comparison, once. Raises only if Postgres cannot be read."""
    from core.ch_gold import parse_tsv as parse_gold
    from core.ch_silver import parse_tsv as parse_silver
    from core.pg_gold import GOLD_COLUMNS
    from core.pg_silver import SILVER_COLUMNS

    snap = await read_snapshot(margin=margin)
    if snap.switch == "rust":
        return Verdict(skipped=f"{SWITCH_KEY} is rust: the live copy is the worker's own")
    verdict = Verdict(issues=liveness(snap.states, snap.now))
    _, shadow_history = history_tables()
    try:
        shadow_silver = parse_silver(await _execute(
            f"SELECT {', '.join(SILVER_COLUMNS)} FROM {SILVER_SHADOW} FORMAT TabSeparated"))
        shadow_gold = parse_gold(await _execute(
            f"SELECT {', '.join(GOLD_COLUMNS)} FROM {GOLD_SHADOW} FORMAT TabSeparated"))
        shadow_buckets: Dict[int, int] = {}
        if snap.history_cut is not None:
            text = await _execute(
                f"SELECT intDiv(id, {BUCKET}) AS b, count() FROM {shadow_history} "
                f"WHERE captured_at < {ts_literal(snap.history_cut)} GROUP BY b "
                f"FORMAT TabSeparated")
            for line in text.split("\n"):
                if line:
                    b, n = line.split("\t")
                    shadow_buckets[int(b)] = int(n)
    except Exception as e:  # noqa: BLE001 — ClickHouse down is a wait, not a raise
        verdict.unwatched.append(f"the shadow could not be read: {type(e).__name__}: {e}")
        return verdict

    if judge_silver_and_gold(snap, shadow_silver, shadow_gold, verdict, margin):
        try:
            await _live_comparison(snap, verdict, margin)
        except Exception as e:  # noqa: BLE001
            verdict.unwatched.append(f"the live copy could not be compared: "
                                     f"{type(e).__name__}: {e}")
    judge_history(snap, shadow_buckets, verdict)
    try:
        await _live_history(snap, verdict, margin)
    except Exception as e:  # noqa: BLE001
        verdict.unwatched.append(f"the live archive could not be compared: "
                                 f"{type(e).__name__}: {e}")
    # The copies were read after the snapshot. A ship that landed in between
    # replaced a copy the snapshot's stamps do not describe, and a rebuild
    # committed in the same seconds would read as the copy's difference.
    moved = moved_ships(snap, await ship_stamps())
    if moved:
        return Verdict(issues=liveness(snap.states, snap.now), unwatched=[
            f"{', '.join(moved)} shipped while the copies were read: the next attempt "
            f"compares"])
    return verdict


def moved_ships(snap: Snapshot, stamps: Mapping[str, Optional[datetime]]) -> List[str]:
    """The ships whose stamp is not the snapshot's any more."""
    names = [*SHIPS, *(PREFIX + s for s in SHIPS)]
    return [n for n in names if stamps.get(n) != snap.state(n).last_ok_at]


async def ship_stamps() -> Dict[str, Optional[datetime]]:
    """Every side's ship stamps, now."""
    from core.pg import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT table_name, last_ok_at FROM meta.mirror_state "
            "WHERE table_name = ANY($1::text[])",
            [*SHIPS, *(PREFIX + s for s in SHIPS)])
    return {r["table_name"]: r["last_ok_at"] for r in rows}


# ─── The night ──────────────────────────────────────────────────────────────

# What a night must have compared to count: the shadow against Postgres. The
# live copy is compared when it can be; a night when Python's copy lagged is
# not the shadow's to fail.
REQUIRED = ("silver", "gold", "archive")


def unwatched_issue(reasons: Sequence[str]) -> IntegrityIssue:
    return _issue(
        check_name="rust_shadow_unwatched", table=SILVER_SHADOW, count=len(reasons),
        description="tonight's last attempt could not compare the whole shadow: " + "; ".join(reasons)
        + ". The night does not count towards the seven")


def should_journal(verdict: Verdict, *, final: bool) -> bool:
    """Every part compared, a difference found, or the night's last attempt."""
    return final or verdict.differs or all(p in verdict.compared for p in REQUIRED)


def kyiv_midnight(now: datetime) -> datetime:
    local = now.astimezone(KYIV)
    return datetime(local.year, local.month, local.day, tzinfo=KYIV).astimezone(timezone.utc)


async def judged_since(at: datetime) -> bool:
    """Whether a run of the layer was journalled since `at`."""
    from core.pg import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        last = await conn.fetchval(
            "SELECT max(started_at) FROM app.data_quality_runs WHERE layer = $1", LAYER)
    return last is not None and last >= at


async def run_nightly(*, now: Optional[datetime] = None, final: Optional[bool] = None,
                      margin: timedelta = MARGIN) -> Dict[str, Any]:
    """The job: judge the shadow and journal the night once. Never raises."""
    from core import dq_journal

    if not shadow_configured():
        return {"skipped": f"{SHADOW_ENV} does not list {JOB}"}
    if not configured():
        return {"skipped": f"{URL_ENV} is not set"}
    started = now or datetime.now(timezone.utc)
    if final is None:
        final = started.astimezone(KYIV).hour >= FINAL_HOUR
    try:
        if await judged_since(kyiv_midnight(started)):
            return {"skipped": "tonight's comparison is journalled"}
        verdict = await judge(margin=margin)
        if verdict.skipped:
            return {"skipped": verdict.skipped}
        if not should_journal(verdict, final=final):
            logger.info("Rust shadow: not comparable yet: %s", "; ".join(verdict.unwatched))
            return {"pending": verdict.unwatched, "compared": verdict.compared}
        issues = list(verdict.issues)
        missing = [p for p in REQUIRED if p not in verdict.compared]
        if missing:
            issues.append(unwatched_issue(verdict.unwatched or missing))
        error = None if verdict.compared else "compared nothing: " + "; ".join(verdict.unwatched)
        ended = datetime.now(timezone.utc)
        run_id = await dq_journal.journal_run(
            started_at=started, ended_at=ended, as_of=ended,
            window_start=ended.date(), window_end=ended.date(), layer=LAYER,
            issues=issues, discrepancies=[], error_message=error)
        result = {"run_id": run_id, "compared": verdict.compared,
                  "findings": [i.check_name for i in issues]}
        logger.info("Rust shadow judged: %s", result)
        return result
    except Exception as e:  # noqa: BLE001 — an optional comparison never fails the scheduler
        logger.exception("Rust shadow check failed")
        return {"error": f"{type(e).__name__}: {e}"}
