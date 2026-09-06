"""The SMS tab's state on its way into Postgres.

Two things are checked here that reading the code cannot settle.

The **column contracts** are asserted against both ends — the live DuckDB
schema and the parsed 0013 DDL — because a copier's column tuple is the one
place where a typo produces a table that is present, populated, and quietly
missing a field.

The **freshness stamp** for `sms_campaign_members` is executed rather than
argued about. It is a correlated subquery standing in for a column the source
table does not have, and its whole job is to give the right answer after each
of the three write paths (freeze, send, delivery). If it is wrong, nothing
fails — the daily comparison simply starts forgiving rows it should report, or
reporting rows it should forgive, which is the kind of defect that is found
months later by someone wondering why a check never fires.
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from core.duckdb_store import DuckDBStore
from core.pg_sms import (
    CAMPAIGN_COLUMNS,
    DLR_COLUMNS,
    MEMBER_COLUMNS,
    MEMBER_STAMP,
    OPTOUT_COLUMNS,
    PRESET_COLUMNS,
    _coerce,
    _insert,
    read_dlr_appends,
    source_tables,
    read_full_replace,
    replicate_sms,
    sms_store_is_postgres,
)

REPO = Path(__file__).resolve().parents[2]
_MIGRATION = REPO / "migrations" / "versions" / "0013_sms_state.py"

UTC = timezone.utc


# ── fixtures ─────────────────────────────────────────────────────────────────


async def _store(tmp_path: Path) -> DuckDBStore:
    store = DuckDBStore(db_path=tmp_path / "sms.duckdb")
    await store.connect()
    return store


async def _seed(store) -> None:
    """One campaign of two members, plus a row in every other table."""
    async with store.connection() as conn:
        conn.execute(
            "INSERT INTO marketing_optouts "
            "(buyer_id, channel, phone, reason, source, opted_out_at) VALUES "
            "(9, 'sms', '380501234567', 'manual', 'admin', "
            " TIMESTAMPTZ '2026-08-01 10:00:00+00')"
        )
        conn.execute(
            "INSERT INTO sms_audience_presets "
            "(name, criteria, created_by, created_at, updated_at) VALUES "
            "('vip', '{\"tier\": \"VIP\"}', 42, "
            " TIMESTAMPTZ '2026-08-01 10:00:00+00', NULL)"
        )
        conn.execute(
            "INSERT INTO sms_campaigns "
            "(campaign, ltv_basis, sales_type, holdout_pct, criteria, "
            " exported_at) VALUES "
            "('aug', 'revenue', 'retail', 10, '{}', "
            " TIMESTAMPTZ '2026-08-10 09:00:00+00')"
        )
        for buyer, assignment in ((1, "target"), (2, "holdout")):
            conn.execute(
                "INSERT INTO sms_campaign_members "
                "(campaign, buyer_id, phone, tier, assignment, "
                " orders_at_export, revenue_ltv_at_export, "
                " margin_ltv_at_export, recency_at_export) VALUES "
                f"('aug', {buyer}, '38050000000{buyer}', 'VIP', "
                f"'{assignment}', 3, 1000.00, 400.00, 12)"
            )
        if "sms_dlr_events" in source_tables(conn):
            conn.execute(
                "INSERT INTO sms_dlr_events (event_id, message_id, first_seen_at) "
                "VALUES ('e1', 'm1', TIMESTAMPTZ '2026-08-11 09:00:00+00')"
            )


async def _has_dlr(store) -> bool:
    """Whether this checkout's DuckDB carries the delivery-report binding.

    It arrives with the TurboSMS signature fix. Tests that need it say so and
    skip rather than seeding it themselves: a fixture that creates a
    production table would keep passing after the real DDL had drifted away
    from it, which is the one thing these contract tests exist to catch.
    """
    async with store.connection() as conn:
        return "sms_dlr_events" in source_tables(conn)


def _migration_columns(qualified: str) -> list[str]:
    """Column names the 0013 DDL declares for one table, in order.

    Parsed, not grepped: a grep is satisfied by a comment that names the
    column, which is exactly how a copier ends up shipping a field the table
    does not have.
    """
    ddl = _MIGRATION.read_text(encoding="utf-8")
    body = ddl.split(f"CREATE TABLE {qualified} (", 1)[1]
    columns: list[str] = []
    for raw in body.splitlines():
        line = raw.split("--")[0].strip()
        if not line:
            continue
        if line.startswith(")") or line.startswith("PRIMARY KEY"):
            break
        match = re.match(
            r"^([a-z_]+)\s+(TEXT|INTEGER|NUMERIC|BIGINT|TIMESTAMPTZ|BOOLEAN)",
            line,
        )
        if match:
            columns.append(match.group(1))
    return columns


async def _duckdb_columns(store, table: str) -> set[str]:
    async with store.connection() as conn:
        rows = conn.execute(f"DESCRIBE {table}").fetchall()
    return {r[0] for r in rows}


# ── the contracts ────────────────────────────────────────────────────────────


_PAIRS = (
    ("app.marketing_optouts", "marketing_optouts", OPTOUT_COLUMNS),
    ("app.sms_audience_presets", "sms_audience_presets", PRESET_COLUMNS),
    ("app.sms_campaigns", "sms_campaigns", CAMPAIGN_COLUMNS),
    ("app.sms_campaign_members", "sms_campaign_members", MEMBER_COLUMNS),
    ("app.sms_dlr_events", "sms_dlr_events", DLR_COLUMNS),
)


class TestColumnContracts:
    @pytest.mark.parametrize("qualified,dk_table,columns", _PAIRS)
    @pytest.mark.asyncio
    async def test_duckdb_has_every_column_the_copier_reads(
        self, tmp_path, qualified, dk_table, columns,
    ):
        store = await _store(tmp_path)
        try:
            if not await _has_dlr(store) and dk_table == "sms_dlr_events":
                pytest.skip("the TurboSMS signature fix has not landed here yet")
            actual = await _duckdb_columns(store, dk_table)
            assert set(columns) <= actual, (
                f"{dk_table} is missing {set(columns) - actual}"
            )
        finally:
            await store.close()

    @pytest.mark.parametrize("qualified,dk_table,columns", _PAIRS)
    def test_migration_declares_every_column_the_copier_writes(
        self, qualified, dk_table, columns,
    ):
        declared = _migration_columns(qualified)
        assert set(columns) <= set(declared), (
            f"{qualified} is missing {set(columns) - set(declared)}"
        )

    def test_bookkeeping_is_not_shared(self):
        """Two correct copies differ on when each of them wrote the row.

        DuckDB stamps `synced_at`, Postgres `mirrored_at`. Shipping either
        would make the comparison report the copy's own clock as a defect.
        """
        for _qualified, _dk, columns in _PAIRS:
            assert "synced_at" not in columns
            assert "mirrored_at" not in columns


# ── the freshness stamp, executed ────────────────────────────────────────────


class TestMemberStamp:
    """The stamp must move after each of the three writes, or the daily
    comparison silently mis-forgives."""

    async def _stamp(self, store, buyer: int):
        async with store.connection() as conn:
            return conn.execute(
                f"SELECT {MEMBER_STAMP} FROM sms_campaign_members "
                "WHERE campaign = 'aug' AND buyer_id = ?",
                [buyer],
            ).fetchone()[0]

    @pytest.mark.asyncio
    async def test_a_frozen_roster_dates_from_the_export(self, tmp_path):
        store = await _store(tmp_path)
        try:
            await _seed(store)
            assert await self._stamp(store, 1) == datetime(
                2026, 8, 10, 9, 0, tzinfo=UTC
            )
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_send_moves_it_though_it_writes_no_timestamp(self, tmp_path):
        """`record_sms_send` sets `message_id` on the member and `sent_at` on
        the campaign. Only the second is a clock, and it is the one that keeps
        a just-sent roster inside the grace window."""
        store = await _store(tmp_path)
        try:
            await _seed(store)
            async with store.connection() as conn:
                conn.execute(
                    "UPDATE sms_campaign_members SET message_id = 'm1', "
                    "delivery_status = 'Accepted' WHERE buyer_id = 1"
                )
                conn.execute(
                    "UPDATE sms_campaigns SET sent_at = "
                    "TIMESTAMPTZ '2026-08-12 08:00:00+00' WHERE campaign = 'aug'"
                )
            assert await self._stamp(store, 1) == datetime(
                2026, 8, 12, 8, 0, tzinfo=UTC
            )
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_delivery_report_wins_over_the_campaign(self, tmp_path):
        store = await _store(tmp_path)
        try:
            await _seed(store)
            async with store.connection() as conn:
                conn.execute(
                    "UPDATE sms_campaigns SET sent_at = "
                    "TIMESTAMPTZ '2026-08-12 08:00:00+00' WHERE campaign = 'aug'"
                )
                conn.execute(
                    "UPDATE sms_campaign_members SET delivered = TRUE, "
                    "delivered_at = TIMESTAMPTZ '2026-08-12 08:05:00+00' "
                    "WHERE buyer_id = 1"
                )
            assert await self._stamp(store, 1) == datetime(
                2026, 8, 12, 8, 5, tzinfo=UTC
            )
            # The member nobody delivered to still dates from the send, not
            # from NULL — a NULL stamp is a row the grace window cannot judge.
            assert await self._stamp(store, 2) == datetime(
                2026, 8, 12, 8, 0, tzinfo=UTC
            )
        finally:
            await store.close()


# ── reading ──────────────────────────────────────────────────────────────────


class TestReads:
    @pytest.mark.asyncio
    async def test_full_replace_returns_every_table_in_declared_order(self, tmp_path):
        store = await _store(tmp_path)
        try:
            await _seed(store)
            async with store.connection() as conn:
                out = read_full_replace(conn)

            assert len(out["app.sms_campaign_members"]) == 2
            assert "bronze.offer_stocks" not in out, (
                "offer_stocks ships with the operational tables now — see "
                "TestNothingUnderTheGuardIsStillWritten"
            )
            # Column order is the contract's, not the table's.
            campaign = out["app.sms_campaigns"][0]
            assert campaign[CAMPAIGN_COLUMNS.index("ltv_basis")] == "revenue"
            assert campaign[CAMPAIGN_COLUMNS.index("holdout_pct")] == 10
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_dlr_watermark_reships_the_boundary_instant(self, tmp_path):
        """Several bindings can share one `first_seen_at`. A strict `>` drops
        every one that landed in the same instant as the watermark — and each
        dropped row is a security control missing its binding."""
        store = await _store(tmp_path)
        try:
            if not await _has_dlr(store):
                pytest.skip("the TurboSMS signature fix has not landed here yet")
            await _seed(store)
            instant = datetime(2026, 8, 11, 9, 0, tzinfo=UTC)
            async with store.connection() as conn:
                conn.execute(
                    "INSERT INTO sms_dlr_events (event_id, message_id, first_seen_at) "
                    "VALUES ('e2', 'm2', TIMESTAMPTZ '2026-08-11 09:00:00+00')"
                )
                shipped = read_dlr_appends(conn, instant)

            assert {r[0] for r in shipped} == {"e1", "e2"}
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_dlr_without_a_watermark_ships_everything(self, tmp_path):
        store = await _store(tmp_path)
        try:
            if not await _has_dlr(store):
                pytest.skip("the TurboSMS signature fix has not landed here yet")
            await _seed(store)
            async with store.connection() as conn:
                assert len(read_dlr_appends(conn, None)) == 1
        finally:
            await store.close()


class TestASourceTableThatIsNotThereYet:
    """The source schema is mid-flight, and this is what that costs.

    `sms_dlr_events` arrives with the TurboSMS signature fix. A copier that
    assumed it raises on every tick — found by the in-image gate on 2026-08-30,
    where the laptop passed only because the fix was sitting uncommitted in the
    working tree.
    """

    @pytest.mark.asyncio
    async def test_a_missing_table_is_skipped_not_raised(self, tmp_path):
        store = await _store(tmp_path)
        try:
            await _seed(store)
            async with store.connection() as conn:
                conn.execute("DROP TABLE marketing_optouts")
                out = read_full_replace(conn)

            assert "app.marketing_optouts" not in out
            # and everything else still came back
            assert len(out["app.sms_campaign_members"]) == 2
            assert out["app.sms_campaigns"]
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_the_copier_asks_rather_than_assumes(self, tmp_path):
        store = await _store(tmp_path)
        try:
            async with store.connection() as conn:
                present = source_tables(conn)
            # The four that are in committed code; `sms_dlr_events` may or
            # may not be.
            for table in ("marketing_optouts", "sms_audience_presets",
                          "sms_campaigns", "sms_campaign_members"):
                assert table in present
        finally:
            await store.close()


class TestCoercion:
    def test_a_delivered_flag_becomes_a_real_boolean(self):
        """asyncpg refuses an int where BOOLEAN is declared — revision 0009's
        `notifications_enabled` trap, in the column a campaign's lift is
        computed from."""
        row = _coerce(("delivered",), (1,))
        assert row == (True,)
        assert isinstance(row[0], bool)

    def test_not_reported_yet_stays_null(self):
        """NULL is the third state. Coercing it to False moves every pending
        recipient into the failed bucket."""
        assert _coerce(("delivered",), (None,)) == (None,)


# ── the guards ───────────────────────────────────────────────────────────────


class TestGuards:
    @pytest.mark.asyncio
    async def test_replication_stands_down_once_postgres_is_the_writer(
        self, tmp_path, monkeypatch,
    ):
        """`replicate_bot_state`'s guard, for its reason: a full replace out of
        DuckDB after the switch rolls back every opt-out recorded since it,
        once an hour, looking healthy in between."""
        monkeypatch.setenv("KS_SMS_STORE", "postgres")
        store = await _store(tmp_path)
        try:
            with patch("core.mirror_reconciliation.configured", return_value=True):
                out = await replicate_sms(store)
            assert "skipped" in out
            assert "no longer the writer" in out["skipped"]
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_without_a_dsn_it_does_nothing(self, tmp_path, monkeypatch):
        monkeypatch.delenv("KS_SMS_STORE", raising=False)
        store = await _store(tmp_path)
        try:
            with patch("core.mirror_reconciliation.configured", return_value=False):
                out = await replicate_sms(store)
            assert out == {"skipped": "KS_PG_DSN is not set"}
        finally:
            await store.close()

    def test_nothing_under_the_guard_is_still_written_by_duckdb(self):
        """The guard above turns off the copy for the *whole module*, on the
        premise that DuckDB has stopped being the writer. That premise is true
        of SMS state and false of landing, and the difference is visible in the
        schema: `app.*` is written by this application, `bronze.*` arrives from
        KeyCRM on every sync no matter which store the SMS tab reads.

        `bronze.offer_stocks` shipped here until 2026-09-06 and froze the
        moment the flag went on — 892 rows of `purchased_price`, which is the
        entire cost side of `ltv_basis=margin`, silently stuck at 08:00:53 with
        `reconcile_sms` standing down on the same flag so nothing would report
        it. It ships with `replicate_operational` now.
        """
        from core.pg_sms import DLR_TABLE, _FULL_REPLACE

        for pg_table, dk_table, _columns, _order in _FULL_REPLACE + (
            (DLR_TABLE, "sms_dlr_events", (), ""),
        ):
            assert pg_table.startswith("app."), (
                f"{pg_table} is not application state, so DuckDB does not stop "
                f"writing {dk_table} when KS_SMS_STORE=postgres — it will "
                f"freeze here. Ship it from core/pg_operational.py instead."
            )

    def test_offer_stocks_left_and_landed_somewhere_that_keeps_shipping(self):
        """The other half of the move: gone from here, present there. Asserted
        together so a revert of one side cannot look tidy."""
        from core.pg_operational import _FULL_REPLACE as OPERATIONAL
        from core.pg_sms import _FULL_REPLACE as SMS

        assert "offer_stocks" not in {dk for _pg, dk, _c, _o in SMS}
        assert "bronze.offer_stocks" in {pg for pg, _dk, _c, _o in OPERATIONAL}

    def test_a_typo_in_the_store_variable_raises(self, monkeypatch):
        """`KS_BOT_STORE`'s rule. Six thousand names and phone numbers are read
        from whichever store this names; a typo must stop the container rather
        than quietly select the copy that is about to go stale."""
        monkeypatch.setenv("KS_SMS_STORE", "postgress")
        with pytest.raises(ValueError, match="KS_SMS_STORE"):
            sms_store_is_postgres()

    @pytest.mark.parametrize("value,expected", (("duckdb", False), ("postgres", True)))
    def test_the_default_is_duckdb(self, monkeypatch, value, expected):
        monkeypatch.setenv("KS_SMS_STORE", value)
        assert sms_store_is_postgres() is expected

    def test_unset_means_duckdb(self, monkeypatch):
        monkeypatch.delenv("KS_SMS_STORE", raising=False)
        assert sms_store_is_postgres() is False


class TestStatements:
    def test_insert_numbers_its_parameters(self):
        assert _insert("app.x", ("a", "b")) == (
            "INSERT INTO app.x (a, b) VALUES ($1, $2)"
        )
