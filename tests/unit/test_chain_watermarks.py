"""Chain 1's watermarks: one registry for the getter, the setter and the check.

The defect this pins (revision 0032): under `KS_WRITE_INVENTORY=postgres` the
setter wrote `last_sync_offers`/`last_sync_stocks` to Postgres, the getter read
them from DuckDB, where they had stopped moving, and the freshness check judged
DuckDB's frozen copy — so the sync would have run on every tick and the check
would have paged "offers stalled" two days after a switch that changed nothing.

These run without Postgres: the freshness check is handed its watermarks, which
is the point of its signature. The round trip against a real Postgres is in
`tests/integration/test_chain_watermarks.py`.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from core.data_quality import _freshness_check, check_internal_integrity

ENTITIES = ("orders", "products", "buyers", "offers", "stocks",
            "managers", "categories", "expense_types")


@pytest.fixture
def flags(monkeypatch):
    for env in ("KS_WRITE_INVENTORY", "KS_WRITE_EXPENSES", "KS_WRITE_GOALS"):
        monkeypatch.delenv(env, raising=False)
    return monkeypatch


@pytest.fixture
def frozen_duckdb():
    """DuckDB's `sync_metadata` as Postgres copied it (`app.sync_metadata`):
    every entity freshly synced except the chain's two, which stopped 60 h
    ago — the state DuckDB was in two and a half days after the switch. The
    threshold for both is 48 h. Since step 14's S14-14 the check reads only
    the copy; until then these were DuckDB's own rows, read under `on`."""
    now = datetime.now(timezone.utc)
    rows = {}
    for entity in ENTITIES:
        stamp = now - (timedelta(hours=60) if entity in ("offers", "stocks")
                       else timedelta(minutes=5))
        rows[f"last_sync_{entity}"] = stamp.isoformat()
    return rows, now


def _names(issues):
    return sorted(i.check_name for i in issues)


def _moved(now, **overrides):
    """Every watermark a chain has moved to Postgres, fresh — since step 14's
    S14-00 that is every chain's, a constant — with the ones a test is about
    set (or, as None, left out)."""
    from core.write_chains import sync_keys as stood_down_sync_keys

    fresh = (now - timedelta(minutes=20)).isoformat()
    out = {key: fresh for key in stood_down_sync_keys()}
    for key, value in overrides.items():
        if value is None:
            out.pop(key, None)
        else:
            out[key] = value
    return out


class TestTheFreshnessCheckFollowsTheWatermarks:
    def test_under_the_flag_the_frozen_copy_is_not_judged(
        self, flags, frozen_duckdb,
    ):
        rows, now = frozen_duckdb
        flags.setenv("KS_WRITE_INVENTORY", "postgres")
        issues = _freshness_check(now, chain_watermarks=_moved(now), frozen_rows=rows)
        assert issues == []

    def test_under_the_flag_a_stale_postgres_stamp_is_a_stall(
        self, flags, frozen_duckdb,
    ):
        """What makes the test above mean something: the values are read, not
        merely skipped. Only offers is stale here, and only offers is named."""
        rows, now = frozen_duckdb
        flags.setenv("KS_WRITE_INVENTORY", "postgres")
        issues = _freshness_check(now, frozen_rows=rows, chain_watermarks=_moved(
            now, last_sync_offers=(now - timedelta(hours=50)).isoformat()))
        assert _names(issues) == ["freshness_offers"]
        assert issues[0].count == 50

    def test_a_key_postgres_never_received_is_named_where_it_is_missing(
        self, flags, frozen_duckdb,
    ):
        """DuckDB's frozen copy must not stand in for a watermark Postgres lacks."""
        rows, now = frozen_duckdb
        flags.setenv("KS_WRITE_INVENTORY", "postgres")
        issues = _freshness_check(now, frozen_rows=rows, chain_watermarks=_moved(
            now, last_sync_stocks=None))
        assert _names(issues) == ["freshness_stocks"]
        assert "meta.chain_watermarks" in issues[0].description

    def test_not_even_a_fresh_frozen_copy_stands_in_for_chain_1(
        self, flags, frozen_duckdb,
    ):
        """The test above cannot tell "never synced" from a frozen stamp judged
        in its place: DuckDB's copy there is 60 h old, past the 48 h limit, so
        either reading files `freshness_stocks`. Here DuckDB's stamp is fresh,
        and chain 1 — which does not declare `CHAIN_WATERMARK_INHERITS_DUCKDB`
        (chain 6a does) — must still be told Postgres has no key."""
        rows, now = frozen_duckdb
        flags.setenv("KS_WRITE_INVENTORY", "postgres")
        rows["last_sync_stocks"] = (now - timedelta(minutes=5)).isoformat()
        issues = _freshness_check(now, frozen_rows=rows, chain_watermarks=_moved(
            now, last_sync_stocks=None))
        assert _names(issues) == ["freshness_stocks"]
        assert "never synced" in issues[0].description

    def test_called_without_them_it_says_it_is_not_watching(
        self, flags, frozen_duckdb,
    ):
        """Neither judging the frozen copy nor going quiet: one WARN that names
        the unwatched syncs, and nothing about them either way."""
        rows, now = frozen_duckdb
        flags.setenv("KS_WRITE_INVENTORY", "postgres")
        issues = _freshness_check(now, frozen_rows=rows)
        assert _names(issues) == ["sync_watermarks_unwatched"]
        assert "offers" in issues[0].description and "stocks" in issues[0].description

    def test_the_integrity_scan_hands_them_through(self, flags, frozen_duckdb):
        rows, now = frozen_duckdb
        flags.setenv("KS_WRITE_INVENTORY", "postgres")
        fresh = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
        handed = check_internal_integrity(frozen_sync_rows=rows, chain_watermarks={
            "last_sync_offers": fresh, "last_sync_stocks": fresh})
        not_handed = check_internal_integrity(frozen_sync_rows=rows)
        assert not {"freshness_offers", "freshness_stocks",
                    "sync_watermarks_unwatched"} & set(_names(handed))
        assert "sync_watermarks_unwatched" in _names(not_handed)
