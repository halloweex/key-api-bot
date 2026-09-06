"""The goals tables are shared and keyed without sales_type.

`seasonal_indices`, `growth_metrics` and `weekly_patterns` have no sales_type
in their keys, so whoever stored last decided what every user's retail goal
was built from. Two things used to store: three plain GETs, open to any
viewer, and the Monday job's b2b pass, which overwrote the retail rows every
week. Neither may write now.
"""
import inspect
from datetime import datetime, timedelta, timezone

import pytest

from core.duckdb_store import DuckDBStore


class TestTheReadRoutesDoNotStore:
    def test_the_three_gets_ask_for_no_persistence(self):
        from web.routes.api import goals as routes

        for name in ("get_seasonality_data", "get_growth_data", "get_weekly_patterns"):
            src = inspect.getsource(getattr(routes, name))
            assert "persist=False" in src, name

    def test_recalculate_on_the_forecast_get_is_admin_only(self):
        from web.routes.api import goals as routes

        src = inspect.getsource(routes.get_goal_forecast)
        assert "if recalculate:" in src and "require_admin(request)" in src

    @pytest.mark.asyncio
    async def test_persist_false_computes_and_stores_nothing(self, tmp_path):
        store = DuckDBStore(db_path=tmp_path / "goals.duckdb")
        await store.connect()
        try:
            base = datetime(2024, 3, 1, 12, tzinfo=timezone.utc)
            async with store.connection() as conn:
                oid = 1
                for year_shift in (0, 1):
                    for day in range(25):
                        when = base.replace(year=base.year + year_shift) + timedelta(days=day)
                        conn.execute(
                            "INSERT INTO orders (id, source_id, status_id, grand_total, ordered_at, buyer_id) "
                            "VALUES (?, 1, 1, 1000.0, ?, 1)", [oid, when],
                        )
                        oid += 1
                conn.execute("DELETE FROM seasonal_indices")
                conn.execute("DELETE FROM weekly_patterns")
                conn.execute("DELETE FROM growth_metrics")

            computed = await store.calculate_seasonality_indices("all", persist=False)
            await store.calculate_yoy_growth("all", persist=False)
            patterns = await store.calculate_weekly_patterns("all", persist=False)
            async with store.connection() as conn:
                stored = [
                    conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                    for t in ("seasonal_indices", "growth_metrics", "weekly_patterns")
                ]
            assert patterns, "the read path still answers"
            assert stored == [0, 0, 0], "the read path must not store"

            if computed:
                await store.calculate_seasonality_indices("all")
                async with store.connection() as conn:
                    assert conn.execute("SELECT COUNT(*) FROM seasonal_indices").fetchone()[0] > 0
        finally:
            await store.close()


class TestTheMondayJobWritesRetailOnly:
    def test_no_b2b_pass(self):
        from core.scheduler import BackgroundScheduler

        src = inspect.getsource(BackgroundScheduler._run_seasonality_calc)
        assert 'calculate_seasonality_indices("b2b")' not in src
        assert 'calculate_yoy_growth("b2b")' not in src
        assert 'calculate_seasonality_indices("retail")' in src


class TestPredictionsAndModelFilesLandWhole:
    def test_store_predictions_is_one_transaction(self):
        from core.repositories.goals import GoalsMixin

        src = inspect.getsource(GoalsMixin.store_predictions)
        assert "BEGIN TRANSACTION" in src and "ROLLBACK" in src

    def test_json_files_are_replaced_not_truncated(self, tmp_path):
        from core.prediction_service import _write_json_atomically

        target = tmp_path / "params.json"
        target.write_text("{\"old\": true}")
        _write_json_atomically(target, {"new": 1})
        assert target.read_text().strip().startswith("{")
        assert "new" in target.read_text()
        assert [p.name for p in tmp_path.iterdir()] == ["params.json"], "no temp file left behind"

    def test_the_model_writer_uses_replace(self):
        from core.prediction_service import PredictionService

        src = inspect.getsource(PredictionService._save_model)
        assert "os.replace(tmp, MODEL_PATH)" in src
        assert "_write_json_atomically" in src
