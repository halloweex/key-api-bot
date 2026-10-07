"""Chain 6: the catalogue — products and categories — written to Postgres.

`bronze.products` and `bronze.categories` land from KeyCRM payloads. Until
this chain DuckDB was where they landed: `DuckDBStore.upsert_products` and
`upsert_categories` wrote DuckDB, and the mirror (`core.pg_landing`) shipped
the same parsed rows to Postgres in the same call. Under
`KS_WRITE_CATALOGUE=postgres` the rows the shared parse produced are written
here instead and DuckDB's two tables freeze — they stay in `_init_schema`
(OD-11 (a)); nothing is dropped.

The chain map said this chain "deletes the DuckDB half". It does not: like
chain 6a it inverts the write, after `core.landing_rows` has parsed the
payload, so both stores would read a product the same way — brand from the
custom field, `min_price or price` — and there is one parse to keep right.

RETIRED IS TOLD FROM LOST BY ONE CLOCK (OD-15 (a))

Every write here is the WHOLE catalogue: every product KeyCRM serves, every
hour, and every category on the weekly full sync. Each write upserts every row
with `mirrored_at = now()` and, in the same transaction, stamps
`meta.mirror_state.last_ok_at = now()` with the mirror's own statement
(`pg_landing.WATERMARK_OK_SQL`). One transaction, one `now()`, so after a write:

- a row it carried has `mirrored_at = last_ok_at`, exactly;
- a row it did not carry has `mirrored_at < last_ok_at` — KeyCRM no longer
  serves it, and the writer never deletes, so it is RETIRED and still named;
- a row with `mirrored_at > last_ok_at` was written by something that is not a
  full write — round the chain.

No `last_seen` column and no migration: measured on a real Postgres 17.2 with
the mirror's own statements before this was written. `core.pg_chain_invariants`
judges these three facts four times a day, and "lost" — a row the last full
write carried that is gone — from `last_rows`, which is why the rows are
de-duplicated here before they are counted: the mirror's `last_rows` counts a
payload's repeated id twice.

THE CHAIN KEEPS A RECORD OF ITS OWN INSTANTS

"Later than `last_ok_at`" is evidence only until the next full write: that
write moves `last_ok_at` past a row written round the chain, and a row KeyCRM
does not serve — a stray insert, a retired product edited — is not in its
payload, so nothing re-stamps it. An hour later it read as RETIRED, "not a
defect", and the copy-back took it for the chain's own work, because "the
chain wrote it" was `mirrored_at` at or after the latch, which any write
after the latch satisfies (the chain-6 review reproduced both, and the
copy-back released the latch with the stray rows in DuckDB).

So every write keeps, in the same transaction, a record of the instants the
chain has written at: the row `writes:<table>` in `meta.chain_watermarks`
(`record_key`), JSON `{"stamps": [...], "previous": ...}` in whole
microseconds since the epoch (`STAMP_SQL` — an integer renders one way
whatever the session's DateStyle). `stamps` is this write's `now()` plus every
earlier stamp a row still carries; a stamp no row carries is dropped, so the
record is bounded by the number of distinct `mirrored_at` values in the table,
never by the number of writes. `previous` is `last_ok_at` as this write found
it — the full write before it — which is what the short-write check measures
against (`core.pg_chain_invariants`).

From the latch on, "the chain wrote this row" is `mirrored_at` being one of the
recorded stamps: the watch reads any other instant at or after the latch as
written round the chain, CRITICAL for as long as the row keeps it, however many
full writes pass; the copy-back counts only recorded rows as the chain's work
(`chain_transfer._rewritten_since_latch`). It clears when KeyCRM serves the
row again (the next full write re-stamps it) or when a human deletes it or
restores it, `mirrored_at` included, from DuckDB's frozen copy.

What it cannot see, named rather than claimed: a write that leaves
`mirrored_at` alone, or copies an instant the chain did write. On a row KeyCRM
serves the next full write overwrites it within the hour; on a retired one
nothing in Postgres dates it — a trigger or a per-row history would, and both
are a migration. The copy-back still refuses such an edit on a row retired
before the flip, where DuckDB's frozen copy disagrees.

The record row is locked FIRST, before any product: two full writes of one
table queue on it and never interleave, so neither loses the other's stamp,
and the rows are still locked in id order behind it. A record nobody can parse
stops the write (`WriteRecordUnreadable`) rather than forget which instants
were the chain's.

THE FULL-CATALOGUE CONTRACT

Because every write stamps `last_ok_at`, a write of PART of the catalogue
would make every row it left out read as retired. So the two writers here are
reached only from the two repository methods, and those only from the sync's
two full-catalogue sites; `tests/unit/test_catalogue_chain.py` walks the tree
for any other caller. A future "refresh one product" path needs a writer that
does not stamp the watermark.

FAILURE POLICY: THESE RAISE

Like every chain writer: under the flag this IS the write. What the caller
does with the raise is the caller's — the hourly products step records it and
retries in ten minutes (`SyncService._catalogue_step_postgres`), `full_sync`
records it and carries on with orders (DN-01's rule: a chain's fault stops that
chain and nothing else). A payload Postgres would refuse — a NULL name, a NUL
in a text column, a price outside `NUMERIC(12,2)` — is refused whole BEFORE the
latch (`CatalogueRefused`): a first write that the server refused after the
marker was on disk would latch the chain with no owner row behind it. Whole,
not row by row, because a row skipped would read as retired.

AND THE FIRST WRITE TAKES THE FLAG'S PLACE

Every write latches the chain (`core/chain_latch.py`, OD-19 (a)) inside
`pool.acquire()` — with a bound, chain 4's — and claims both owner rows inside
its transaction. From the first catalogue written here only
`scripts/chain_copy_back.py catalogue` moves the writes back to DuckDB.

A PRECONDITION THE FLAG ENFORCES

Nearly every reader on the dashboard joins the catalogue — directly, or
through `silver_order_lines` — and each can still read DuckDB, whose catalogue
stops changing the moment this chain writes Postgres. So `unmet_precondition()`
holds an unlatched chain on DuckDB until:

- chain 1 writes Postgres: DuckDB's `refresh_sku_inventory_status` joins
  DuckDB's products, and its result is what the hourly copy ships;
- `KS_READ_FALLBACK=off`: under `duckdb` a failed Postgres read is answered
  from DuckDB, here from a frozen catalogue;
- every read switch of the warehouse (`warehouse_cutover.WAREHOUSE_READERS`)
  is postgres — the list step 13 checks, kept complete by a walk of every
  `KS_READ_*` name in the tree, read through the one helper the switch reads
  (`readers_not_on_postgres`), so the two cannot disagree. Over-inclusive on
  purpose: a hand-picked subset of "catalogue readers" would be a second list
  with no walker.

The warning that says so is rate-limited (chain 4's reason): the incremental
tick asks for the products watermark every minute.
"""
from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Dict, FrozenSet, List, Optional, Sequence, Tuple

from core import chain_latch
from core.landing_rows import (
    CATEGORY_COLUMNS, PRODUCT_COLUMNS, CategoryRow, ProductRow,
)

logger = logging.getLogger(__name__)

WRITE_ENV = "KS_WRITE_CATALOGUE"

# What this chain is called in `core.write_chains`, in the local latch marker
# and in the `/api/health` block. One spelling, so a marker written by one
# release is still read by the next. `chain_copy_back.py catalogue` resolves it.
CHAIN = "pg_catalogue_write"

PRODUCTS = "bronze.products"
CATEGORIES = "bronze.categories"

# The tables this chain writes. `core.write_chains` reads this, and through it
# the mirror (`pg_landing._mirror`), the daily comparison (`reconcile_mirror`),
# the carry of retired rows and the copy-back. Each is a unit of its own
# (`pg_landing.unit_of`): one writer, one table, one transaction.
CHAIN_TABLES: Tuple[str, ...] = (PRODUCTS, CATEGORIES)

# What the two full-catalogue sync sites stamp after the rows land. The store's
# getter and setter route them through `core.write_chains.chain_for_sync_key`
# to `meta.chain_watermarks`.
#
# NOT `last_sync_meilisearch_pg` (OD-15): its value is `MAX(mirrored_at)` over
# orders, buyers and products, which this chain stamps exactly as the mirror
# did, so its meaning does not move here; it moves with whichever of chains 3
# and 6 lands last, and by the sequencing that is chain 3. Declared here, a
# catalogue rollback would carry and release a search-index cursor, and the
# flip would read it as absent and re-index everything.
# `tests/unit/test_catalogue_chain.py` keeps it off every chain that does not
# also own an order table.
CHAIN_SYNC_KEYS: Tuple[str, ...] = ("last_sync_products", "last_sync_categories")

# Not judged by `core.pg_chain_invariants`' 90 minutes. A stalled catalogue
# loses nothing that cannot be fetched again, and `_freshness_check` judges
# both entities at 48 h and 192 h; a second limit on the same stamp would say
# one stall twice under two names (chain 6a's reasoning).
CHAIN_WATERMARK_MAX_AGE_MIN: Optional[int] = None

# Until the first write under the flag stamps a key in
# `meta.chain_watermarks`, `_freshness_check` judges DuckDB's frozen stamp
# instead of calling the entity never synced — the categories move on Sunday
# only, so a mid-week flip would file "never synced" on every integrity run
# until then. Chain 6a's arrangement.
CHAIN_WATERMARK_INHERITS_DUCKDB = True

# A share of the previous full write's rows that the LAST write left out,
# above which it is not KeyCRM retiring goods: it is a catalogue that arrived
# short — `paginate` stops on the first short page, so an API hiccup hands the
# sync a silently truncated list (WARN `chain_catalogue_short_write`). One
# write against the one before, never everything ever retired against the
# table: the writer never deletes, so that share only rises, and once past
# this it warned after every complete write (the chain-6 review). Reasoned,
# not measured: KeyCRM retires a product or two between two hourly writes.
RETIRED_WARN_PCT = 5.0
# And at least this many rows: the category tree is ~28 rows, where two
# categories KeyCRM retired in one week are 7% and ordinary. A truncated page
# of the products is 50.
RETIRED_WARN_MIN_ROWS = 10

# How long an acquire may wait (chain 4's bound). The pool sets none, and the
# hourly step runs inside the incremental tick under the heavy-job lock.
ACQUIRE_TIMEOUT_S = 10

# Per statement, inside every writing transaction (chain 4's form, DN-05a).
STATEMENT_TIMEOUT = "30s"

# The record of the chain's own write instants (module docstring). The key
# lives beside the owner rows in `meta.chain_watermarks`; nothing reads that
# table except by an exact key or the `owner:` prefix, so a third family of
# keys is seen by nobody but the two readers here.
RECORD_PREFIX = "writes:"

# A row's `mirrored_at` as the record keeps it: whole microseconds since the
# epoch. `extract(epoch …)` is exact to the microsecond from PostgreSQL 14 on.
STAMP_SQL = "(extract(epoch FROM mirrored_at) * 1000000)::bigint"

# Released with the owner rows by `scripts/chain_copy_back.py catalogue`: the
# record describes writes DuckDB owns again, and a later flip starts a new one.
CHAIN_RELEASED_KEYS: Tuple[str, ...] = tuple(
    f"{RECORD_PREFIX}{table}" for table in CHAIN_TABLES)

_EMPTY_RECORD_TEXT = '{"stamps": [], "previous": null}'
# Created empty if absent, then locked: the first statement of a write after
# the owner rows (module docstring, last paragraph).
_RECORD_CREATE_SQL = (
    "INSERT INTO meta.chain_watermarks (key, value, updated_at) "
    "VALUES ($1, $2, now()) ON CONFLICT (key) DO NOTHING")
_RECORD_LOCK_SQL = (
    "SELECT value FROM meta.chain_watermarks WHERE key = $1 FOR UPDATE")
_PREVIOUS_OK_SQL = (
    "SELECT last_ok_at FROM meta.mirror_state WHERE table_name = $1")
# This write's instant, every earlier stamp a row still carries, and the full
# write before this one. `{table}` is the chain's own constant, never input.
_RECORD_WRITE_SQL = (
    "UPDATE meta.chain_watermarks SET updated_at = now(), value = "
    "jsonb_build_object("
    "'stamps', (SELECT COALESCE(jsonb_agg(s ORDER BY s), '[]'::jsonb) FROM ("
    "SELECT DISTINCT " + STAMP_SQL + " AS s FROM {table} "
    "WHERE mirrored_at = now() OR " + STAMP_SQL + " = ANY($2::bigint[])) kept), "
    "'previous', (extract(epoch FROM $3::timestamptz) * 1000000)::bigint"
    ")::text WHERE key = $1")

# How often the unmet-precondition warning may repeat for one reason.
UNMET_WARN_EVERY_S = 3600.0
_unmet_warned: Dict[str, float] = {}

_INT32 = (-2 ** 31, 2 ** 31 - 1)
# `price NUMERIC(12,2)` (revision 0002), DuckDB's `DECIMAL(12,2)` beside it.
PRICE_NUMERIC = (12, 2)


class CatalogueRefused(ValueError):
    """A catalogue Postgres would refuse, refused whole before the latch.
    Nothing was written in either store — DuckDB writes the catalogue in one
    transaction and would have refused it whole too."""


class WriteRecordUnreadable(ValueError):
    """`writes:<table>` holds something `parse_record` cannot read. The write
    stops, the watch is blind and the copy-back refuses — none of them may
    guess which instants were the chain's."""


@dataclass(frozen=True)
class WriteRecord:
    """The record of one table's chain writes (module docstring).

    `stamps` — the instants, in microseconds since the epoch, of every chain
    write some row still carries; `previous` — `last_ok_at` as the last write
    found it, None when no full write had stamped the table before it."""
    stamps: FrozenSet[int] = frozenset()
    previous: Optional[int] = None


def record_key(table: str) -> str:
    return f"{RECORD_PREFIX}{table}"


def parse_record(value: str) -> WriteRecord:
    """The record as stored. Raises `WriteRecordUnreadable` on anything but
    `{"stamps": [int, ...], "previous": int | null}` — a bool is not an int."""
    def whole(x) -> bool:
        return isinstance(x, int) and not isinstance(x, bool)

    try:
        doc = json.loads(value)
    except (TypeError, ValueError) as exc:
        raise WriteRecordUnreadable(f"not JSON ({type(exc).__name__})") from None
    if not isinstance(doc, dict):
        raise WriteRecordUnreadable("not an object")
    stamps, previous = doc.get("stamps"), doc.get("previous")
    if not isinstance(stamps, list) or not all(whole(s) for s in stamps):
        raise WriteRecordUnreadable("'stamps' is not a list of integers")
    if previous is not None and not whole(previous):
        raise WriteRecordUnreadable("'previous' is not an integer")
    return WriteRecord(stamps=frozenset(stamps), previous=previous)


def env_writes_postgres() -> bool:
    """What `KS_WRITE_CATALOGUE` alone says.

    An unrecognised value raises — `KS_BOT_STORE`'s rule. Since DN-01 that
    stops this chain and nothing else: the registry stands it down, the hourly
    products step records it, and `full_sync` contains it.
    """
    value = (os.getenv(WRITE_ENV) or "duckdb").strip().lower()
    if value not in {"duckdb", "postgres"}:
        raise RuntimeError(
            f"{WRITE_ENV}={value!r} is not understood; expected "
            f"'duckdb' or 'postgres'"
        )
    return value == "postgres"


def unmet_precondition() -> Optional[str]:
    """Why `KS_WRITE_CATALOGUE=postgres` must not move the writes yet, or None.

    Never raises and never asks Postgres: `/api/health` reads it through the
    registry, and must still answer with Postgres down. Names flags, never
    their values — that endpoint is public. A flag nobody can parse is unmet."""
    from core import pg_inventory_write, read_fallback, warehouse_cutover

    lagging: List[str] = []
    try:
        if not pg_inventory_write.writes_postgres():
            lagging.append(f"{pg_inventory_write.WRITE_ENV} is not postgres "
                           "(DuckDB's SKU status joins DuckDB's products)")
    except Exception as exc:  # noqa: BLE001 — carried out, not swallowed
        lagging.append(f"{pg_inventory_write.WRITE_ENV} is not understood "
                       f"({type(exc).__name__})")
    try:
        if not read_fallback.refusing():
            lagging.append(f"{read_fallback.ENV} is not off (a failed read is "
                           "answered from DuckDB's catalogue)")
    except Exception as exc:  # noqa: BLE001
        lagging.append(f"{read_fallback.ENV} could not be read ({type(exc).__name__})")
    try:
        readers = warehouse_cutover.readers_not_on_postgres(os.environ)
        if readers:
            lagging.append(", ".join(readers) + " not postgres")
    except Exception as exc:  # noqa: BLE001
        lagging.append(f"the warehouse readers could not be read ({type(exc).__name__})")
    if not lagging:
        return None
    return ("; ".join(lagging) + ", so a reader still shows DuckDB's catalogue, "
            "which stops changing the moment this chain writes Postgres; move "
            "every reader first")


def _warn_unmet(reason: str) -> None:
    now = time.monotonic()
    last = _unmet_warned.get(reason)
    if last is not None and now - last < UNMET_WARN_EVERY_S:
        return
    _unmet_warned[reason] = now
    logger.warning("%s=postgres, but the chain stays on DuckDB: %s", WRITE_ENV, reason)


def writes_postgres() -> bool:
    """Whether this chain writes Postgres — the one answer every caller reads.

    The latch outranks the flag (OD-19 (a)), and outranks a value nobody can
    read. Unlatched, the flag moves the writes only once `unmet_precondition()`
    holds. Raises on a flag nobody can read while unlatched; `mode()` is the
    question that never raises.
    """
    if chain_latch.latched(CHAIN):
        return True
    if not env_writes_postgres():
        return False
    unmet = unmet_precondition()
    if unmet:
        _warn_unmet(unmet)
        return False
    return True


def mode() -> Optional[str]:
    """Where this chain's writes go: "postgres", "duckdb", or None when
    `KS_WRITE_CATALOGUE` is not understood and no latch overrides it — the
    registry's answer (`write_chains.chain_modes`). Never raises; the sync
    asks this before it decides which path the catalogue takes."""
    from core import write_chains

    return write_chains.chain_modes()[CHAIN]["mode"]


def _latch() -> str:
    """Take the local half of the latch before this process writes Postgres.

    Called with a connection already acquired, after `require_revision()` has
    passed — `pg_expenses_write._latch` has the reason: the latch is permanent,
    and a write that never reaches Postgres must not spend the rollback.
    """
    return chain_latch.latch(CHAIN, WRITE_ENV)


async def _pool():
    from core.pg import get_pool, require_revision

    pool = await get_pool()
    await require_revision()
    return pool


def _int_refusal(column: str, value, *, nullable: bool) -> Optional[str]:
    if value is None:
        return None if nullable else f"{column} is NULL"
    if isinstance(value, bool) or not isinstance(value, int):
        return f"{column} is a {type(value).__name__}, not an integer"
    if not _INT32[0] <= value <= _INT32[1]:
        return f"{column} is outside INTEGER's range"
    return None


def _text_refusal(column: str, value, *, nullable: bool) -> Optional[str]:
    if value is None:
        return None if nullable else f"{column} is NULL"
    if not isinstance(value, str):
        return f"{column} is a {type(value).__name__}, not text"
    if "\x00" in value:
        return f"{column} carries a NUL character"
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        return f"{column} is not valid UTF-8"
    return None


def _refusal(table: str, rows: Sequence[tuple]) -> Optional[str]:
    """Why Postgres would refuse this catalogue, or None. Asked before the latch.

    Names the table, the id and the column, never the value. Every column of
    the shared parse's rows, by what revision 0002 declares."""
    from core.pg_numeric import refusal

    for row in rows:
        rid = row[0]
        why = _int_refusal("id", rid, nullable=False)
        if why is None:
            if table == PRODUCTS:
                _, name, category_id, brand, sku, price = row
                why = (_text_refusal("name", name, nullable=False)
                       or _int_refusal("category_id", category_id, nullable=True)
                       or _text_refusal("brand", brand, nullable=True)
                       or _text_refusal("sku", sku, nullable=True)
                       or refusal("price", price, *PRICE_NUMERIC, nullable=True))
            else:
                _, name, parent_id = row
                why = (_text_refusal("name", name, nullable=False)
                       or _int_refusal("parent_id", parent_id, nullable=True))
        if why:
            return f"{table} id {rid!r}: {why}"
    return None


def _dedupe(rows: Sequence[tuple]) -> List[tuple]:
    """One row per id — the last one given, which is what DuckDB's per-row
    `INSERT OR REPLACE` leaves of an id the payload repeats — sorted by id.

    De-duplicated so `last_rows` counts distinct rows (the "lost" invariant
    reads it), and so one statement never upserts one id twice. Sorted so two
    full writes that overlap take their row locks in one order and cannot
    deadlock."""
    latest: Dict[int, tuple] = {}
    for row in rows:
        latest[row[0]] = tuple(row)
    return [latest[k] for k in sorted(latest)]


async def _land(conn, table: str, sql: str, rows: List[tuple]) -> None:
    """One whole catalogue into `table`, on the writer's transaction: the
    record locked first, the rows, the watermark, then the record of this
    write's instant (module docstring)."""
    from core.pg_landing import WATERMARK_OK_SQL

    key = record_key(table)
    await conn.execute(_RECORD_CREATE_SQL, key, _EMPTY_RECORD_TEXT)
    record = parse_record(await conn.fetchval(_RECORD_LOCK_SQL, key))
    previous_ok_at = await conn.fetchval(_PREVIOUS_OK_SQL, table)
    await conn.executemany(sql, rows)
    await conn.execute(WATERMARK_OK_SQL, table, len(rows))
    await conn.execute(_RECORD_WRITE_SQL.format(table=table), key,
                       sorted(record.stamps), previous_ok_at)


async def upsert_products(rows: List[ProductRow]) -> int:
    """The whole product catalogue, from the rows `core.landing_rows` parsed.
    Raises. Returns how many distinct products were written.

    One transaction: the owner rows, every product and the watermark land
    together or none do, so `last_ok_at` never claims a catalogue that did not
    land, and every product it carried shares its instant (module docstring).
    """
    if not rows:
        # Nothing moved: no latch, and no watermark — an empty payload stamped
        # as a whole catalogue would make every product read as retired.
        return 0
    why = _refusal(PRODUCTS, rows)
    if why:
        raise CatalogueRefused(why)
    rows = _dedupe(rows)
    columns = PRODUCT_COLUMNS
    # Spelled here, the mirror's own shape (`pg_landing._UPSERT`): every row it
    # carries is re-stamped, changed or not, which is what makes
    # `mirrored_at = last_ok_at` the mark of a row the last write carried.
    sql = (
        f"INSERT INTO bronze.products ({', '.join(columns)}, mirrored_at) "
        f"VALUES ({', '.join(f'${i}' for i in range(1, len(columns) + 1))}, now()) "
        "ON CONFLICT (id) DO UPDATE SET "
        + ", ".join(f"{c} = EXCLUDED.{c}" for c in columns if c != "id")
        + ", mirrored_at = EXCLUDED.mirrored_at"
    )
    pool = await _pool()
    async with pool.acquire(timeout=ACQUIRE_TIMEOUT_S) as conn:
        # Inside the acquire, not before it: an acquire that ends without a
        # connection is a write that never reached Postgres (`_latch`).
        stamp = _latch()
        async with conn.transaction():
            await conn.execute(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")
            await chain_latch.claim(conn, CHAIN_TABLES, stamp)
            await _land(conn, PRODUCTS, sql, rows)
    return len(rows)


async def upsert_categories(rows: List[CategoryRow]) -> int:
    """The whole category tree, from the rows `core.landing_rows` parsed.
    Raises. `upsert_products`' transaction, for its reasons."""
    if not rows:
        return 0
    why = _refusal(CATEGORIES, rows)
    if why:
        raise CatalogueRefused(why)
    rows = _dedupe(rows)
    columns = CATEGORY_COLUMNS
    sql = (
        f"INSERT INTO bronze.categories ({', '.join(columns)}, mirrored_at) "
        f"VALUES ({', '.join(f'${i}' for i in range(1, len(columns) + 1))}, now()) "
        "ON CONFLICT (id) DO UPDATE SET "
        + ", ".join(f"{c} = EXCLUDED.{c}" for c in columns if c != "id")
        + ", mirrored_at = EXCLUDED.mirrored_at"
    )
    pool = await _pool()
    async with pool.acquire(timeout=ACQUIRE_TIMEOUT_S) as conn:
        stamp = _latch()
        async with conn.transaction():
            await conn.execute(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")
            await chain_latch.claim(conn, CHAIN_TABLES, stamp)
            await _land(conn, CATEGORIES, sql, rows)
    return len(rows)
