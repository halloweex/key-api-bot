"""silver.orders — the first table Postgres computes rather than receives.

Everything before it arrived from somewhere else. This one is the output of
`silver_select_sql(POSTGRES)`, so the property that matters is that its columns
are exactly the projection's columns: a table with a column the projection does
not produce, or missing one it does, fails at the first rebuild rather than
here — and the first rebuild is on production data.
"""
from __future__ import annotations

import importlib.util
import inspect
from pathlib import Path

from core import pg


REPO = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "_rev0006", REPO / "migrations" / "versions" / "0006_silver_orders.py"
)
_rev = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_rev)
DOWN = inspect.getsource(_rev.downgrade)



def _ancestry(revision):
    """A revision and everything it is built on, by walking `down_revision`."""
    chain = {}
    for path in (REPO / "migrations" / "versions").glob("[0-9]*.py"):
        spec = importlib.util.spec_from_file_location(path.stem, path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        chain[module.revision] = module.down_revision

    seen = []
    while revision is not None:
        seen.append(revision)
        revision = chain.get(revision)
    return seen


class TestTheChainIsIntact:
    def test_it_follows_the_classification(self):
        assert _rev.down_revision == "0005_manager_classification"
        assert _rev.revision == "0006_silver_orders"

    def test_the_code_requires_a_revision_that_includes_it(self):
        """It was the head until 0007 added Gold. What has to stay true is that
        the revision the application demands still carries this table — a chain
        that forked would satisfy a name check and not this one."""
        assert "0006_silver_orders" in _ancestry(pg.REQUIRED_REVISION)

    def test_it_can_be_undone(self):
        assert "DROP TABLE IF EXISTS silver.orders" in DOWN
