"""The last batch of the race audit's LOW findings, each with the guard it got."""
import pytest



# `TestAHeaderOnlyRefreshNeverCreatesAnOrder` stood here, pinning the
# header-only refresh's deferral on DuckDB's order writer (with chain 3 routed
# back to DuckDB for it). Step 14's S14-09 deleted that writer; the same rule
# on chain 3's is `tests/integration/test_orders_chain_writer.py`
# (`test_a_header_only_refresh_never_creates_an_order`), and the decision
# itself `tests/unit/test_pg_orders_write.py` (`TestTheDecisionIsDuckDBs`).


# `TestPersistRunIsOneTransaction` stood here: DuckDB's `persist_run` opened
# one transaction around the run and its findings and rolled it back on a
# failed insert. Step 14's S14-10 deleted that writer; the same property of
# chain 9's, a refused finding leaving no run behind, is
# `tests/integration/test_dq_journal_pg.py` (`TestOneTransaction`).


# `TestATimedOutQueryLeavesTheConnectionBeforeTheLockDoes` and
# `TestBackupSweepsItsOwnOrphans` stood here: the DuckDB store's `_fetch_*`
# releasing its lock after a `QueryTimeoutError`, and its nightly backup
# removing a stale temp copy. Both went with the store's DuckDB half in step
# 14's S14-18.


class TestTheUtmBackfillFlagIsAlwaysReleased:
    @pytest.mark.asyncio
    async def test_a_failure_before_the_body_does_not_wedge_the_endpoint(self, monkeypatch):
        from web.routes.api import traffic

        async def _boom():
            raise RuntimeError("KeyCRM unavailable")

        # The client, the first thing the body awaits since step 14's S14-15
        # (the store it opened first went with the way back's note).
        monkeypatch.setattr("core.keycrm.get_async_client", _boom)
        traffic._backfill_status.update(running=True, result={"status": "started"})
        with pytest.raises(RuntimeError):
            await traffic._run_backfill(1)
        assert traffic._backfill_status["running"] is False


class TestTheRest:
    def test_the_bot_sync_client_serialises_its_callers(self, monkeypatch):
        """The bot shares one client between worker threads (a report handler
        and the nightly milestone job), each on an event loop of its own: the
        inner client's pool and the circuit breaker's lock belong to one loop,
        so two calls at once must reach KeyCRM one after the other. KeyCRM is
        faked at its transport and counts the requests it serves at once."""
        import threading
        import time

        from core.keycrm import SyncKeyCRMClient
        from tests.helpers.orders_sync import keycrm_serving_orders

        counting = threading.Lock()
        inside = most = 0

        def slowly(_request):
            nonlocal inside, most
            with counting:
                inside += 1
                most = max(most, inside)
            time.sleep(0.05)
            with counting:
                inside -= 1
            return None          # then answered as KeyCRM answers

        keycrm_serving_orders(monkeypatch, [], answer=slowly)
        client = SyncKeyCRMClient(api_key="test-only")
        answers, errors = [], []

        def call():
            try:
                answers.append(client.get_orders({"limit": 50}))
            except Exception as exc:  # noqa: BLE001 — reported by the assert below
                errors.append(exc)

        threads = [threading.Thread(target=call) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)

        assert errors == []
        assert answers == [{"data": []}, {"data": []}]
        assert most == 1

    def test_utm_parse_stamps_the_updated_at_it_parsed(self):
        """The parse's own, in Postgres since step 13 and alone since step 14's
        S14-15 deleted DuckDB's (`TrafficMixin._parse_utm_into_silver`)."""
        from core import pg_utm_parse

        assert "SELECT o.id, o.manager_comment, o.updated_at" in pg_utm_parse.INCREMENTAL_SELECT_SQL
        assert "COALESCE($14::timestamptz, now())" in pg_utm_parse.insert_sql()

    # `test_the_classification_endpoint_reports_its_replica` held the
    # retail-status route to answering the classification copy's result, until
    # step 14's S14-13 deleted the copy (`core.pg_replication`) and the key.
