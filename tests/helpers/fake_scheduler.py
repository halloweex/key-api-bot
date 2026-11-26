"""A BackgroundScheduler on a fake APScheduler, with the layers' last good runs
given.
"""
from core.scheduler import BackgroundScheduler


class _FakeJob:
    def __init__(self, job_id):
        self.id = job_id
        self.name = job_id
        self.func = lambda: None


class _FakeScheduler:
    def __init__(self, known=()):
        self.known = {j: _FakeJob(j) for j in known}
        self.added = {}

    def get_job(self, job_id):
        return self.known.get(job_id)

    def add_job(self, func, **kwargs):
        self.added[kwargs.get("id")] = kwargs


_NO_TABLE = object()


def _scheduler_with(
    monkeypatch, ages, known=("dq_reconciliation", "dq_integrity_check"),
    inventory_today=_NO_TABLE, writes_layers=("reconciliation_pg",),
):
    scheduler = BackgroundScheduler()
    scheduler._scheduler = _FakeScheduler(known)

    class _Conn:
        def execute(self, sql, *args):
            # The only raw SQL _schedule_catchup_runs issues is the probe for
            # today's inventory snapshot. The default is a schema without that
            # table, which must degrade quietly rather than disable the
            # data-quality catch-ups above it.
            if inventory_today is _NO_TABLE:
                raise RuntimeError("Catalog Error: inventory_sku_history does not exist")

            class _Result:
                def fetchone(_self):
                    return (1,) if inventory_today else None

            return _Result()

    class _Ctx:
        async def __aenter__(self):
            return _Conn()

        async def __aexit__(self, *exc):
            return False

    class _Store:
        def connection(self):
            return _Ctx()

    async def _get_store():
        return _Store()

    monkeypatch.setattr("core.store.get_store", _get_store)
    # Since step 14's S14-00 the journal's and the snapshot's chains (9 and 1)
    # write Postgres, a constant, so the catch-up reads both facts there: the
    # same ages, and today's snapshot — a Postgres fault reads as taken. Since
    # S14-10 the ages are read nowhere else (DuckDB's reader,
    # `data_quality.fetch_last_success_ages`, is gone).
    async def _ages_pg(*a, **k):
        return ages

    async def _snapshot_pg():
        if inventory_today is _NO_TABLE:
            return ()
        return (1,) if inventory_today else None

    monkeypatch.setattr("core.dq_journal.last_success_ages_pg", _ages_pg)
    monkeypatch.setattr("core.scheduler._inventory_snapshot_taken_today_pg", _snapshot_pg)
    # And DuckDB's arm of the reconciliation stands down (chain 3, a constant):
    # the Postgres arm's age decides, on a host that writes it. None leaves
    # the real answer, read off the environment, to a caller that sets it.
    if writes_layers is not None:
        monkeypatch.setattr("core.scheduler._writes_layer",
                            lambda layer: layer in writes_layers)
    return scheduler
