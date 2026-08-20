"""Two holes on the same seam: who counts as retail, and what happens to
revenue that counts as neither.

`sales_type` decides which orders any page can show — every endpoint defaults
to `Query("retail")`. It is derived from `managers.is_retail`, which the
manager sync recomputed from a hardcoded list on every run, so nobody could
correct it. And the warehouse checksums sum *all* sales_types, so a value
outside the partition would balance them perfectly while appearing nowhere.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from core.duckdb_constants import KNOWN_SALES_TYPES, RETAIL_MANAGER_IDS
from core.duckdb_store import DuckDBStore


async def _make_store(tmp_path: Path) -> DuckDBStore:
    s = DuckDBStore(db_path=tmp_path / "test.duckdb")
    await s.connect()
    return s


def _api_manager(mid: int, name: str = "Someone", status: str = "active"):
    return {"id": mid, "name": name, "email": f"{mid}@example.com", "status": status}


# ─── P2-1: a classification that survives the next sync ─────────────────────

class TestManagerClassificationIsDurable:
    @pytest.mark.asyncio
    async def test_a_manual_classification_survives_a_manager_sync(self, tmp_path):
        """This is the whole point: KeyCRM cannot tell us who is wholesale."""
        store = await _make_store(tmp_path)
        try:
            unlisted = 34
            assert unlisted not in RETAIL_MANAGER_IDS

            await store.upsert_managers([_api_manager(unlisted)])
            await store.set_manager_retail_status(unlisted, True)

            # …the next sync arrives with the same manager.
            await store.upsert_managers([_api_manager(unlisted, name="Renamed")])

            managers = {m["id"]: m for m in await store.get_all_managers()}
            assert managers[unlisted]["is_retail"] is True, (
                "the sync overwrote a human's classification"
            )
            assert managers[unlisted]["name"] == "Renamed", "fields KeyCRM owns still update"
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_new_managers_are_seeded_from_the_constant(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            known_retail = RETAIL_MANAGER_IDS[0]
            await store.upsert_managers([
                _api_manager(known_retail), _api_manager(9999),
            ])
            managers = {m["id"]: m for m in await store.get_all_managers()}
            assert managers[known_retail]["is_retail"] is True
            assert managers[9999]["is_retail"] is False
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_demoting_a_manager_also_sticks(self, tmp_path):
        """The constant lists managers who have left; a correction runs both ways."""
        store = await _make_store(tmp_path)
        try:
            listed = RETAIL_MANAGER_IDS[0]
            await store.upsert_managers([_api_manager(listed)])
            await store.set_manager_retail_status(listed, False)
            await store.upsert_managers([_api_manager(listed)])

            managers = {m["id"]: m for m in await store.get_all_managers()}
            assert managers[listed]["is_retail"] is False
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_sync_does_not_reset_computed_stats(self, tmp_path):
        """order_count and the date range are computed here, not in KeyCRM."""
        store = await _make_store(tmp_path)
        try:
            await store.upsert_managers([_api_manager(7)])
            async with store.connection() as conn:
                conn.execute(
                    "UPDATE managers SET order_count = 412, "
                    "first_order_date = DATE '2025-01-05' WHERE id = 7"
                )

            await store.upsert_managers([_api_manager(7, name="Still Here")])

            managers = {m["id"]: m for m in await store.get_all_managers()}
            assert managers[7]["order_count"] == 412
            assert managers[7]["first_order_date"] == date(2025, 1, 5)
        finally:
            await store.close()


# ─── P2-2: the partition has to be exhaustive ───────────────────────────────

def _insert_order(conn, oid: int, manager_id, total: float, when: datetime):
    conn.execute(
        "INSERT INTO orders (id, source_id, status_id, grand_total, ordered_at, "
        "buyer_id, manager_id) VALUES (?, 1, 1, ?, ?, 1, ?)",
        [oid, total, when, manager_id],
    )


class TestPartitionAssertion:
    @pytest.mark.asyncio
    async def test_a_healthy_warehouse_reports_an_exhaustive_partition(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            when = datetime.now(timezone.utc) - timedelta(days=1)
            async with store.connection() as conn:
                _insert_order(conn, 1, None, 1000.0, when)       # retail (NULL manager)
                _insert_order(conn, 2, 15, 2000.0, when)         # b2b
                _insert_order(conn, 3, 34, 3000.0, when)         # internal

            alerts: list[tuple[str, str | None]] = []

            async def fake_alert(msg, key=None):
                alerts.append((msg, key))

            store._send_warehouse_alert = fake_alert
            res = await store.refresh_warehouse_layers(trigger="manual")

            assert res["status"] != "error"
            assert not any(k == "warehouse:sales_type_partition" for _, k in alerts)

            async with store.connection() as conn:
                by_type = dict(conn.execute(
                    "SELECT sales_type, SUM(revenue) FROM gold_daily_revenue "
                    "GROUP BY sales_type"
                ).fetchall())
            assert set(by_type) <= set(KNOWN_SALES_TYPES)
            assert sum(by_type.values()) == pytest.approx(6000.0)
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_revenue_outside_the_partition_alerts(self, tmp_path, monkeypatch):
        """A sales_type the code does not list balances every existing checksum.

        Simulated by narrowing the known set rather than by widening the CASE:
        the effect on the assertion is identical, and Gold is rebuilt from
        Silver inside the very call under test, so an injected value would not
        survive to be seen.
        """
        store = await _make_store(tmp_path)
        try:
            when = datetime.now(timezone.utc) - timedelta(days=1)
            async with store.connection() as conn:
                _insert_order(conn, 1, None, 1000.0, when)   # retail
                _insert_order(conn, 2, 34, 500.0, when)      # 'internal'

            alerts: list[tuple[str, str | None]] = []

            async def fake_alert(msg, key=None):
                alerts.append((msg, key))

            store._send_warehouse_alert = fake_alert
            monkeypatch.setattr(
                "core.duckdb_store.KNOWN_SALES_TYPES", ("retail", "b2b"),
            )
            res = await store.refresh_warehouse_layers(trigger="manual")

            # The existing checksums are blind to it: the totals still balance,
            # so validation passes and nothing is marked dirty.
            assert res["validation_passed"] is True

            partition = [(m, k) for m, k in alerts if k == "warehouse:sales_type_partition"]
            assert partition, "revenue outside the partition must be reported"
            assert "internal=₴500.00" in partition[0][0]
        finally:
            await store.close()


class TestSalesTypeSurfacesAgree:
    """Every layer that names a sales_type must know all of them.

    `exhibition` came within one deploy of shipping as a dashboard button the
    API answered with 400: the Silver CASE, the partition assertion and the
    frontend union were updated, and `validate_sales_type`'s hardcoded
    whitelist was not. Five hand-copied lists, four changed. These tests are
    cheap and they fail loudly on the fifth.
    """

    def test_validator_accepts_every_known_type(self):
        from core.validators import validate_sales_type

        for sales_type in KNOWN_SALES_TYPES:
            assert validate_sales_type(sales_type) == sales_type

    def test_validator_still_rejects_an_unknown_type(self):
        from core.exceptions import ValidationError
        from core.validators import validate_sales_type

        with pytest.raises(ValidationError):
            validate_sales_type("not_a_sales_type")

    def test_frontend_union_covers_every_known_type(self):
        """The TS union is a sixth copy; nothing but a test can hold it in step."""
        from pathlib import Path

        filters_ts = (
            Path(__file__).resolve().parents[2]
            / "web" / "frontend" / "src" / "types" / "filters.ts"
        )
        source = filters_ts.read_text(encoding="utf-8")
        union_line = next(
            line for line in source.splitlines()
            if line.startswith("export type SalesType")
        )
        for sales_type in KNOWN_SALES_TYPES:
            assert f"'{sales_type}'" in union_line, (
                f"{sales_type} is in KNOWN_SALES_TYPES but not in the "
                f"SalesType union in {filters_ts.name}"
            )

    def test_every_known_type_has_a_filter_label_in_every_language(self):
        """A button with no label renders blank rather than failing."""
        import json
        from pathlib import Path

        locales = (
            Path(__file__).resolve().parents[2]
            / "web" / "frontend" / "src" / "locales"
        )
        for lang in ("en", "uk", "ru"):
            keys = json.loads((locales / f"{lang}.json").read_text(encoding="utf-8"))
            for sales_type in KNOWN_SALES_TYPES:
                key = f"filter.{sales_type}"
                assert keys.get(key), f"{key} missing or empty in {lang}.json"


class TestClassificationReachesTheWarehouse:
    """A classification nobody rebuilds for changes nothing a reader can see.

    `sales_type` is materialised into silver_orders by one CASE at rebuild
    time. The route has always marked the warehouse dirty after calling this;
    the method never did, so any other caller left Silver stale and no test
    would have noticed.
    """

    @pytest.mark.asyncio
    async def test_setting_retail_status_marks_the_warehouse_dirty(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            await store.upsert_managers([_api_manager(4, "Someone")])
            # Clear whatever the sync left behind.
            await store.consume_warehouse_dirty()

            await store.set_manager_retail_status(4, False)

            is_dirty, changed_ids = await store.consume_warehouse_dirty()
            assert is_dirty is True, (
                "reclassifying a manager left the warehouse clean, so Silver "
                "keeps the old sales_type until something else triggers a rebuild"
            )
            assert changed_ids is None, (
                "the scope must be full: the classification applies to every "
                "order the manager ever took, not to a list of changed ids"
            )
        finally:
            await store.close()


class TestOrderStatusRefreshIsOffTheCompactionInstant:
    """The host cron stops the containers at 05:00:01–05:00:39 Kyiv on Sundays.

    A CronTrigger due at that instant does not exist when it is due — the
    scheduler comes back at 05:00:51 and sets the next fire a day out. That
    cost order_status_refresh every Sunday for six weeks running.
    """

    def test_it_is_not_due_in_the_forbidden_window(self):
        import re
        from pathlib import Path

        source = (
            Path(__file__).resolve().parents[2] / "core" / "scheduler.py"
        ).read_text(encoding="utf-8")

        block = source.split('job_id="order_status_refresh"', 1)[1][:600]
        match = re.search(r"CronTrigger\(hour=(\d+), minute=(\d+)\)", block)
        assert match, "order_status_refresh no longer registers a CronTrigger"

        hour, minute = int(match.group(1)), int(match.group(2))
        assert not (hour == 5 and minute <= 5), (
            f"order_status_refresh is due at {hour:02d}:{minute:02d} Kyiv, inside "
            "the window the weekly compaction has the containers stopped"
        )
        assert (hour, minute) != (5, 30), (
            "05:30 Kyiv already belongs to dq_reconciliation"
        )


class TestSilverHasOneDefinition:
    """Two copies of the Silver build existed and drifted apart in under a day.

    On 2026-08-20 the exhibition source was added to `duckdb_store.py` and not
    to the `rebuild-silver` endpoint's copy — by someone who knew about the
    duplicate and was checking for exactly this. Running that endpoint would
    have silently reverted the fix and dropped 178 orders back out of revenue.
    """

    def test_the_rebuild_endpoint_restates_nothing(self):
        from pathlib import Path

        source = (
            Path(__file__).resolve().parents[2]
            / "web" / "routes" / "api" / "admin.py"
        ).read_text(encoding="utf-8")

        for fragment, what in (
            ("WHEN o.manager_id", "the sales_type CASE"),
            # `AS is_active_source`, not the bare name: reading the column in
            # a WHERE clause is what it is for. Only defining it is a copy.
            ("AS is_active_source", "the revenue-source list"),
            ("AS source_name", "the source_name CASE"),
            ("is_new_customer = CASE", "the is_new_customer rule"),
            ("CREATE TABLE silver_orders", "the silver_orders DDL"),
        ):
            assert fragment not in source, (
                f"admin.py restates {what} instead of importing it from "
                "core/duckdb_store.py. That duplicate drifted once already."
            )

    def test_every_revenue_source_is_a_synced_source(self):
        from core.duckdb_constants import REVENUE_SOURCE_IDS, SYNCED_SOURCE_IDS

        assert set(REVENUE_SOURCE_IDS) <= set(SYNCED_SOURCE_IDS), (
            "revenue cannot come from a source we do not sync and reconcile"
        )

    def test_the_exhibition_branch_comes_before_the_manager_branches(self):
        """Source wins over manager, or a retail-listed manager reclaims it."""
        from core.duckdb_constants import EXHIBITION_SOURCE_ID
        from core.duckdb_store import silver_sales_type_case

        case = silver_sales_type_case()
        assert case.index(f"o.source_id = {EXHIBITION_SOURCE_ID}") < case.index("o.manager_id"), (
            "the exhibition was staffed by a manager on the retail list, so a "
            "manager-based branch placed first would pull it into retail"
        )
