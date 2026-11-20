"""Facts for the Postgres warehouse twins in which every group reads clean, for
a test to break one of.
"""
from __future__ import annotations

from datetime import date, datetime, timezone

from core import pg_warehouse_dq as twins
from core.pg_warehouse_dq import (
    Attribution, DerivationJournal, Facts, LineItems, OrderLanding, RowValues,
    SilverArc, Watermark,
)


WM = Watermark(today=date(2026, 9, 17), in_flight=2, backfilled=True,
               landing_age_s=120, landing_failures=0)


ARC_CLEAN = SilverArc(0, 0, 0.0, None, (), 0, 0.0, ())


ATT_OK = Attribution(orders=100, tagged=30, base_orders=400, base_tagged=120)


LI_NONE = LineItems(0, 0.0, (), 0, 0.0, ())


REBUILT = datetime(2026, 9, 17, 6, 50, tzinfo=timezone.utc)


def _rv(**kw):
    base = dict(reported=0, covered=0, abandoned=0, in_flight=0, columns=(), sample=(),
                oldest_age_s=None, rebuilt_at=REBUILT, compared=48000, elapsed_ms=230)
    base.update(kw)
    return RowValues(**base)


RV_CLEAN = _rv()


JOURNAL_QUIET = DerivationJournal(since=REBUILT, runs=())


def _facts(**groups):
    base = dict(silver_arc=ARC_CLEAN, attribution=ATT_OK, line_items=LI_NONE,
                derivation_signal=JOURNAL_QUIET, order_landing=OrderLanding(),
                own_entity=twins.OwnEntity(), silver_row_values=RV_CLEAN)
    base.update(groups)
    return Facts(whole=None, watermark=WM, **base)
