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
full writes pass; the copy-back counted only recorded rows as the chain's
work (`chain_transfer._rewritten_since_latch`), until S14-04 deleted it. It
clears when KeyCRM serves the row again (the next full write re-stamps it) or
when a human deletes it or restores it, `mirrored_at` included, from DuckDB's
frozen copy.

The watermark was read the same way, until step 14's S14-12 deleted the daily
comparison that read it: it took a `last_ok_at` after the owner row for a
shipment over the chain's rows (`chain_shipper_overwrote`), and every write
here stamps one; at a recorded instant it was this chain's own write, at any
other it was not (`read_mirror_stamps`, one statement for both). Until that was
asked, every write after the latch was paged CRITICAL as a shipment over
itself, every morning from 2026-10-10. It sees a stamp that is not the chain's only until
the next write moves the watermark again — within the hour for the products,
the week for the categories — and it is not the only thing that would: the
sync's mirror (`pg_landing._mirror`), the one other writer that stamps this
watermark, ships only while the chain is routed to DuckDB. A step-14 image
routes no chain there (S14-00), so its mirror never ships these tables; in an
image from before step 14, with owner rows in Postgres, that is the marker
lost, which `chain_latch_disagrees` and `owner_row_without_marker` say for as
long as it lasts.

What it cannot see, named rather than claimed: a write that leaves
`mirrored_at` alone, or copies an instant the chain did write. On a row KeyCRM
serves the next full write overwrites it within the hour; on a retired one
nothing in Postgres dates it — a trigger or a per-row history would, and both
are a migration. The copy-back refused such an edit on a row retired before
the flip, where DuckDB's frozen copy disagrees; an image from before step 14
still does, and S14-04 deleted this build's.

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
`scripts/chain_copy_back.py catalogue` moved the writes back to DuckDB
(deleted in step 14's S14-04; the way back since is an image from before
step 14, `core.rollback_lever`).

A PRECONDITION THE FLAG ENFORCED — UNTIL STEP 14

Until step 14's S14-00 an unlatched chain was held on DuckDB until the
lock-out revision was required, chain 1 wrote Postgres, `KS_READ_FALLBACK` was
`off` and every warehouse read switch was postgres — nearly every reader on
the dashboard joins the catalogue, and each could still read DuckDB, whose
catalogue stops changing the moment this chain writes Postgres. S14-00 made
the answers constants; S14-09 deleted them, with the routing they fed: the
hourly products step, `full_sync` and the store's two writers ask nothing.
Kept: `WRITE_ENV` as a name (revision 0035's comments name it, and an image
from before step 14 reads it) and the latch and its owner rows (decision 3:
that image routes by them). `env_writes_postgres` and `writes_postgres()`,
which the registry read until then, went in S14-11 with the flag.

THE CATALOGUE'S HISTORY: `app.catalogue_versions` (revision 0036)

Every products write overwrites a product's name, brand and category in place,
so a brand answer about last month moves the day somebody edits the product in
KeyCRM. After each products write, once its transaction has committed and its
connection is back in the pool, `_capture_versions` keeps the history the
analysts' warehouse asks for (design 3.1, 3.3): SCD2 by content diff, in a
transaction of its own (`analytics_captures.run_own_transaction`), so it never
touches the write it follows and a missed run only coarsens when a change
became known. The chain's own statements are unchanged
(`tests/unit/test_chain_writers_sql_frozen.py`); the capture is a second unit
of work after them.

- A version is a change, not an observation: a row is written only when the
  product's canonical row — name, brand, category_id, exactly as
  `bronze.products` holds them — hashes differently from its latest version,
  or its deletion state moved. The first run seeds one version per product.
  `known_from` is the capture's clock, read after its lock.
- Brand is stored as bronze holds it. `''` (an emptied KeyCRM custom field,
  `landing_rows.brand_of`) and NULL (no field) both mean "no brand"; the
  reader folds them with `NULLIF(brand, '')`, as `dim_product` does (design
  3.1). A flip between the two is a version because bronze changed: one
  redundant row, never a wrong answer, and the self-check can stay exact.
- A tombstone (`is_deleted`) says the product left the catalogue. The writer
  never deletes ("RETIRED IS TOLD FROM LOST BY ONE CLOCK" above), so a product
  KeyCRM retires stays in `bronze.products` with its old `mirrored_at`; only a
  human or a defect removes a row. So a tombstone is written for a product
  that LEFT BOTH OF THE LAST TWO full writes (`mirrored_at` before the chain
  record's `previous`), carrying its content as bronze still holds it — one
  short write never retires anything, because a truncated catalogue
  (`chain_catalogue_short_write`) is put right by the next one — and for a
  product whose row is gone from `bronze.products`, carrying its last
  version's content. A product served again gets a live version.
- The mass-delete guard: more than max(50, 1 %) of the products held live
  tombstoned in one run is refused whole (`CaptureRefused`, WARN
  `capture_self_check_failed:app.catalogue_versions`) and nothing is written.
  A wiped or half-restored `bronze.products` is the case it is for, and it
  clears itself once the rows are back. A genuine mass retirement in KeyCRM
  looks the same from here and holds the refusal until a person who has seen
  it in KeyCRM runs the capture once with `guard=False` (the docstring of
  `_capture_versions` has the line); until then no catalogue change is
  versioned, and none is lost either: the next accepted run diffs against
  the last version kept.
- A zero-tolerance self-check, after the write and in the same snapshot:
  every product's latest version holds what `bronze.products` holds, with
  the deletion state the rule above gives it, no live version names a
  product bronze lacks, and every latest `row_hash` is the hash of its own
  row — the last asked before the diff too, so a latest version edited by
  hand is refused rather than superseded. Any difference refuses the run
  whole. The diff itself compares columns, never the hash: the canonical
  row is not injective, and two contents that hash alike are two versions.
- Repeatable read, behind a lock on the history table taken before the first
  read: two captures queue instead of both writing one change twice, and a
  products write committing in the middle cannot make the self-check read
  another catalogue than the diff did.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Dict, FrozenSet, List, Optional, Sequence, Tuple

from core import chain_latch
from core.analytics_captures import CaptureRefused, run_own_transaction
from core.landing_rows import (
    CATEGORY_COLUMNS, PRODUCT_COLUMNS, CategoryRow, ProductRow,
)

logger = logging.getLogger(__name__)

WRITE_ENV = "KS_WRITE_CATALOGUE"

# What this chain is called in `core.write_chains`, in the local latch marker
# and in the `/api/health` block. One spelling, so a marker written by one
# release is still read by the next.
CHAIN = "pg_catalogue_write"

PRODUCTS = "bronze.products"
CATEGORIES = "bronze.categories"

# The tables this chain writes. `core.write_chains` reads this, and through it
# the mirror (`pg_landing._mirror`), the daily comparison (`reconcile_mirror`)
# and the carry of retired rows (and the copy-back, until step 14's S14-04
# deleted it). Each is a unit of its own (`pg_landing.unit_of`): one writer,
# one table, one transaction.
CHAIN_TABLES: Tuple[str, ...] = (PRODUCTS, CATEGORIES)

# What the two full-catalogue sync sites stamp after the rows land. The store's
# getter and setter route them through `core.write_chains.chain_for_sync_key`
# to `meta.chain_watermarks`.
#
# NOT `last_sync_meilisearch_pg` (OD-15): its value is `MAX(mirrored_at)` over
# orders, buyers and products, which this chain stamps exactly as the mirror
# did, so its meaning does not move here; it moves with whichever of chains 3
# and 6 lands last, and by the sequencing that is chain 3, which declares it
# (`core.pg_orders_write.CHAIN_SYNC_KEYS`). Declared here too, a catalogue
# rollback would carry and release a search-index cursor chain 3 still writes.
# `tests/unit/test_catalogue_chain.py` keeps it off every chain that does not
# also own an order table, and on chain 3.
CHAIN_SYNC_KEYS: Tuple[str, ...] = ("last_sync_products", "last_sync_categories")

# Not judged by `core.pg_chain_invariants`' 90 minutes. A stalled catalogue
# loses nothing that cannot be fetched again, and `_freshness_check` judges
# both entities at 48 h and 192 h; a second limit on the same stamp would say
# one stall twice under two names (chain 6a's reasoning).
CHAIN_WATERMARK_MAX_AGE_MIN: Optional[int] = None

# `CHAIN_WATERMARK_INHERITS_DUCKDB` stood here until step 14's S14-09: until
# the first write under the flag stamped a key in `meta.chain_watermarks`,
# `_freshness_check` judged DuckDB's frozen stamp instead of calling the entity
# never synced. Production holds both keys (H6, 2026-10-29), and an absent one
# costs nothing the stand-in would save: the hourly products step syncs at once
# when `last_sync_products` is absent, nothing reads `last_sync_categories`
# back, and the freshness check then says "never synced", which is true
# (`tests/unit/test_absent_chain_keys.py`).

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
# keys is seen by nobody but its readers: the watch and the daily comparison
# (`read_mirror_stamps`) — and the copy-back, until step 14's S14-04 deleted
# it.
RECORD_PREFIX = "writes:"

# A row's `mirrored_at` as the record keeps it: whole microseconds since the
# epoch. `extract(epoch …)` is exact to the microsecond from PostgreSQL 14 on.
STAMP_SQL = "(extract(epoch FROM mirrored_at) * 1000000)::bigint"

# This chain's own writer stamps `meta.mirror_state.last_ok_at` on both tables
# (`_land`, OD-15 (a)). The daily comparison read that watermark and this
# record together (`read_mirror_stamps`, `CHAIN_STAMPS_MIRROR_STATE`) to tell
# the chain's own writes from a shipment out of DuckDB over them
# (`chain_shipper_overwrote`); step 14's S14-12 deleted the comparison and both.

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

_INT32 = (-2 ** 31, 2 ** 31 - 1)
# `price NUMERIC(12,2)` (revision 0002), DuckDB's `DECIMAL(12,2)` beside it.
PRICE_NUMERIC = (12, 2)

# ─── The catalogue's history (revision 0036; module docstring, last section) ──

VERSIONS = "app.catalogue_versions"

# The mass-delete guard: more than max(50, 1 %) of the products the history
# holds live, tombstoned in one run, is refused whole (design 3.3).
MASS_DELETE_MIN = 50
MASS_DELETE_PCT = 1
# How many ids a refusal names.
CAPTURE_SAMPLE = 10


def _row_hash(alias: str) -> str:
    """The canonical row's 64-bit md5 prefix, as a signed BIGINT: name, brand
    and category_id as text, `\\N` for NULL, chr(31) between them (design
    3.4's conventions). Brand verbatim, so `''` and NULL hash apart."""
    return ("('x' || substr(md5("
            f"COALESCE({alias}.name, '\\N') || chr(31) || "
            f"COALESCE({alias}.brand, '\\N') || chr(31) || "
            f"COALESCE({alias}.category_id::text, '\\N')"
            "), 1, 16))::bit(64)::bigint")


# A product left out of both of the last two full writes: its `mirrored_at`
# is older than the write before the last (the record's `previous`, $1). No
# `previous` yet retires nothing. Read inside `bronze.products` alone, where
# `mirrored_at` is unambiguous.
_RETIRED_SQL = "COALESCE(" + STAMP_SQL + " < $1::bigint, false)"

_LATEST_SQL = """latest AS (
    SELECT DISTINCT ON (product_id)
           product_id, name, brand, category_id, row_hash, is_deleted
    FROM app.catalogue_versions
    ORDER BY product_id, known_from DESC, id DESC
)"""

# Repeatable read, and the lock before the first read: the snapshot is taken
# at that read, after the lock, so a second capture waits for this one and
# then sees its rows. Only this module writes the table; readers are not held.
_VERSIONS_ISOLATION_SQL = "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"
_VERSIONS_LOCK_SQL = "LOCK TABLE app.catalogue_versions IN SHARE ROW EXCLUSIVE MODE"
_VERSIONS_CLOCK_SQL = "SELECT clock_timestamp()"
_RECORD_READ_SQL = "SELECT value FROM meta.chain_watermarks WHERE key = $1"
_VERSIONS_LIVE_SQL = "WITH " + _LATEST_SQL + " SELECT count(*) FROM latest WHERE NOT is_deleted"
# Before the diff: a latest version whose hash is not its own row's was
# edited by hand (or written by a broken renderer). The diff compares
# columns, so it would version bronze's content over it as a change nobody
# made; refused instead, as the self-check refuses the same after a run.
_VERSIONS_MISHASHED_SQL = (
    "WITH " + _LATEST_SQL + " SELECT count(*) FROM latest l WHERE l.row_hash <> "
    + _row_hash("l"))

# Every version this run owes: a product bronze holds whose content or
# deletion state differs from its latest version (or that has none), and a
# product held live whose row bronze no longer has. `tombstoned` marks a live
# version turning into a tombstone — what the guard counts. Content is
# compared column by column, as the self-check compares it, never by
# `row_hash`: the canonical row is not injective (NULL and the text `\N`
# render alike, and a chr(31) inside a value moves the separators), so two
# contents that hash alike would write no version and the self-check would
# refuse every run after. The hash is stored and verified, not trusted.
_VERSIONS_DIFF_SQL = (
    "WITH " + _LATEST_SQL + """,
held AS (
    SELECT p.id AS product_id, p.name, p.brand, p.category_id,
           """ + _row_hash("p") + """ AS row_hash,
           """ + _RETIRED_SQL + """ AS is_deleted
    FROM bronze.products p
)
SELECT c.product_id, c.name, c.brand, c.category_id, c.row_hash, c.is_deleted,
       (c.is_deleted AND l.product_id IS NOT NULL AND NOT l.is_deleted) AS tombstoned
FROM held c
LEFT JOIN latest l ON l.product_id = c.product_id
WHERE l.product_id IS NULL OR l.is_deleted <> c.is_deleted
   OR (l.name, l.brand, l.category_id) IS DISTINCT FROM (c.name, c.brand, c.category_id)
UNION ALL
SELECT l.product_id, l.name, l.brand, l.category_id, l.row_hash, true, true
FROM latest l
WHERE NOT l.is_deleted
  AND NOT EXISTS (SELECT 1 FROM bronze.products p WHERE p.id = l.product_id)
ORDER BY 1""")

# Bounded by arithmetic: one seed row per product the first run finds, then a
# row per product whose name, brand or category moves, or that leaves or comes
# back — 1-2 k a year (the design's §3.3), well under 1 MB. Append-only:
# nothing deletes from it.
_VERSION_INSERT_SQL = """
INSERT INTO app.catalogue_versions
    (product_id, known_from, name, brand, category_id, row_hash, is_deleted)
VALUES ($1, $2, $3, $4, $5, $6, $7)
"""

# The self-check, column by column rather than by hash, so a fault in the
# hash cannot vouch for itself; and every latest hash against its own row.
_VERSIONS_SELF_CHECK_SQL = (
    "WITH " + _LATEST_SQL + """
SELECT
    (SELECT count(*) FROM bronze.products p
     LEFT JOIN latest l ON l.product_id = p.id
     WHERE l.product_id IS NULL
        OR l.name IS DISTINCT FROM p.name
        OR l.brand IS DISTINCT FROM p.brand
        OR l.category_id IS DISTINCT FROM p.category_id
        OR l.is_deleted <> """ + _RETIRED_SQL + """) AS differ,
    (SELECT count(*) FROM latest l
     WHERE NOT l.is_deleted
       AND NOT EXISTS (SELECT 1 FROM bronze.products p WHERE p.id = l.product_id)) AS orphaned,
    (SELECT count(*) FROM latest l
     WHERE l.row_hash <> """ + _row_hash("l") + """) AS mis_hashed""")


class CatalogueRefused(ValueError):
    """A catalogue Postgres would refuse, refused whole before the latch.
    Nothing was written in either store — DuckDB writes the catalogue in one
    transaction and would have refused it whole too."""


class WriteRecordUnreadable(ValueError):
    """`writes:<table>` holds something `parse_record` cannot read. The write
    stops and the watch is blind (and the copy-back refused, until step 14's
    S14-04 deleted it) — none of them may guess which instants were the
    chain's."""


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


# `env_writes_postgres` and `writes_postgres` stood here until step 14's
# S14-11. S14-09 left them for the registry alone — `error` and `mismatch` in
# `/api/health`, and the store's sync-key getter and setter — and S14-11
# deleted them with the flag and those readers: the chain writes Postgres, a
# constant. `WRITE_ENV` stays a name (revision 0035's comments and an image
# from before step 14 read it).


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


async def _capture_versions(conn, *, guard: bool = True) -> int:
    """The catalogue's history, one run (module docstring, last section).
    `run_own_transaction` hands it a connection inside a fresh transaction of
    its own and stamps it; returns how many versions it wrote. Raises
    `CaptureRefused` for the mass-delete guard and the self-check, which
    leaves nothing written.

    `guard=False` is the lever for a mass retirement a person has confirmed
    in KeyCRM, run once in the web container (`docker compose exec web`):

        python -c "import asyncio, functools; from core import analytics_captures as a, pg_catalogue_write as c; print(asyncio.run(a.run_own_transaction(c.VERSIONS, functools.partial(c._capture_versions, guard=False))))"
    """
    await conn.execute(_VERSIONS_ISOLATION_SQL)
    await conn.execute(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")
    await conn.execute(_VERSIONS_LOCK_SQL)
    known_from = await conn.fetchval(_VERSIONS_CLOCK_SQL)
    value = await conn.fetchval(_RECORD_READ_SQL, record_key(PRODUCTS))
    previous = parse_record(value).previous if value is not None else None

    mis_hashed = await conn.fetchval(_VERSIONS_MISHASHED_SQL)
    if mis_hashed:
        raise CaptureRefused(
            f"self-check: {mis_hashed} latest version(s) whose row_hash is not "
            "of their own row before the run — edited by hand, or a hash "
            "renderer changed; nothing written",
            mis_hashed_before=mis_hashed)
    live = await conn.fetchval(_VERSIONS_LIVE_SQL)
    owed = await conn.fetch(_VERSIONS_DIFF_SQL, previous)
    tombstoned = [r["product_id"] for r in owed if r["tombstoned"]]
    limit = max(MASS_DELETE_MIN, live * MASS_DELETE_PCT // 100)
    if guard and len(tombstoned) > limit:
        shown = ", ".join(str(i) for i in tombstoned[:CAPTURE_SAMPLE])
        raise CaptureRefused(
            f"mass-delete guard: {len(tombstoned)} of the {live} products held "
            f"live would be tombstoned in one run, over the {limit} allowed "
            f"(e.g. id {shown}); nothing written. A truncated or emptied "
            "bronze.products clears this once its rows are back; a retirement "
            "confirmed in KeyCRM is accepted with guard=False "
            "(pg_catalogue_write._capture_versions)",
            tombstoned=len(tombstoned), live=live, limit=limit,
            sample_ids=tuple(tombstoned[:CAPTURE_SAMPLE]))
    if owed:
        await conn.executemany(_VERSION_INSERT_SQL, [
            (r["product_id"], known_from, r["name"], r["brand"], r["category_id"],
             r["row_hash"], r["is_deleted"]) for r in owed])

    check = await conn.fetchrow(_VERSIONS_SELF_CHECK_SQL, previous)
    if check["differ"] or check["orphaned"] or check["mis_hashed"]:
        raise CaptureRefused(
            f"self-check: {check['differ']} product(s) whose latest version is "
            f"not what bronze.products holds, {check['orphaned']} live "
            "version(s) of a product bronze.products lacks, "
            f"{check['mis_hashed']} latest row_hash(es) not of their own row; "
            "nothing written",
            differ=check["differ"], orphaned=check["orphaned"],
            mis_hashed=check["mis_hashed"])
    if owed:
        logger.info("Catalogue history: %d version(s) written, %d of them "
                    "tombstones (%d newly)", len(owed),
                    sum(1 for r in owed if r["is_deleted"]), len(tombstoned))
    return len(owed)


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
    # The write has committed and its connection is back in the pool. The
    # history is a unit of work of its own, on a connection of its own: it
    # never raises, and its failure is a gap in the history, stamped and
    # digested (module docstring, last section).
    await run_own_transaction(VERSIONS, _capture_versions)
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
