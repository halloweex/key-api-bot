"""The 09:00 digest of 2026-11-14 and the morning after, as judged since the
owner asked why the data-quality digest kept arriving (2026-11-14).

Three findings sent it for nothing. `pg_headline_vs_line_items` was the only
standing WARN for a month, and every certificate redeemed or test shipped to
a b2b partner moved its count; `ga4_backfill_incomplete` fell by ~23 a night
while the backfill was safe; and `capture_stalled` named a capture whose
writer had not had its first turn. Each is INFO now while it says nothing a
person must act on, and each WARN state it can turn into has a name of its
own, so the morning it turns is news.

The findings come out of the real judges (`pg_warehouse_dq`'s groups and
`analytics_captures.judge`), not hand-written severities, and go through the
real `build_digest`.
"""
from __future__ import annotations

import dataclasses
from datetime import date, datetime, timedelta, timezone

from core import analytics_captures as cap
from core import ga4_capture as gc
from core import pg_warehouse_dq as twins
from core.data_quality import DigestSection, Severity, build_digest
from core.pg_warehouse_dq import Attribution, LineItems, OrderLanding, Watermark

# The integrity run the 09:00 digest read: 07:00 Kyiv, 05:00 UTC.
NOV_14 = datetime(2026, 11, 14, 5, 0, tzinfo=timezone.utc)
NOV_15 = NOV_14 + timedelta(days=1)
# The first integrity run after revision 0036 deployed (01:00 Kyiv on the 14th).
FIRST_RUN_AFTER_0036 = datetime(2026, 11, 13, 23, 0, tzinfo=timezone.utc)
VINTAGES = "app.revenue_prediction_vintages"

WM = Watermark(today=date(2026, 11, 14), in_flight=0, backfilled=True,
               landing_age_s=60, landing_failures=0)


def _rows(issues):
    """Persisted as `RUN_ISSUES_SQL` returns them: worst first, then by name."""
    rank = {"CRITICAL": 0, "WARN": 1, "INFO": 2}
    out = [{"check_name": i.check_name, "table_name": i.table_name,
            "severity": i.severity.value, "count": i.count} for i in issues]
    return sorted(out, key=lambda r: (rank[r["severity"]], r["check_name"]))


def _captures(now: datetime, kept_nights: int):
    """The captures' findings: every capture stamped but the vintages (no
    training since 0036), and GA4's backlog after `kept_nights` nights."""
    stamps = {t: cap.Stamp(now - timedelta(minutes=30), 0, None)
              for t in cap.CAPTURES if t != VINTAGES}
    today = gc.kyiv_today(now)
    backlog = gc.Backlog(
        tuple(gc.overdue_periods(today)[kept_nights * gc.PERIODS_PER_RUN:]),
        gc.first_night(now))
    return cap.judge(cap.Facts(now=now, stamps=stamps,
                               missing_since={VINTAGES: FIRST_RUN_AFTER_0036},
                               ga4_backlog=backlog))


def _headline(orders: int, retail: int, goods: int = 812):
    li = LineItems(orders, 1_023_472.67 + 2_000.0 * (orders - 461), (1,), goods, 900_000.0, (2,),
                   headline_by_type=(("b2b", 350, 809_553.0), ("exhibition", 5, 12_000.0),
                                     ("retail", retail, 201_919.67)))
    return twins._line_items_check(li)


def _source_domain(count: int):
    landing = OrderLanding(unknown_source=count, unknown_source_values=(6,),
                           unknown_source_sample=tuple(range(1, count + 1)))
    return [twins._landing_findings(landing)["pg_value_domain_orders_source_id"]]


def _attribution():
    # 22 of 150 website orders tagged: under the 15 % floor, 128 untagged.
    return twins._attribution_check(Attribution(150, 22, 600, 150), WM)


def _section(issues, previous):
    return DigestSection(
        layer="integrity",
        run={"started_at": NOV_14.isoformat(), "status": "WARN"},
        issues=_rows(issues), previous_issues=_rows(previous), age_hours=2.0)


def _nov_13_digest_run():
    """The run the 13 November digest reported on: before revision 0036,
    with the headline still a WARN."""
    headline = _headline(459, 104)
    return ([dataclasses.replace(headline[0], severity=Severity.WARN), headline[1]]
            + _source_domain(1))


def _nov_14_run():
    return (_headline(461, 106) + _captures(NOV_14, kept_nights=1)
            + _attribution() + _source_domain(2))


class TestTheMorningOfTheFourteenth:
    def test_the_findings_are_judged_as_the_owner_decided(self):
        by = {i.check_name: i for i in _nov_14_run()}
        assert {n: by[n].severity for n in by} == {
            "pg_headline_vs_line_items": Severity.INFO,
            "pg_goods_shipped_without_sale": Severity.INFO,
            f"capture_unstarted:{VINTAGES}": Severity.INFO,
            cap.GA4_BACKFILL: Severity.INFO,
            "pg_attribution_coverage_website": Severity.WARN,
            "pg_value_domain_orders_source_id": Severity.WARN,
        }
        assert by[cap.GA4_BACKFILL].count == 471
        assert "19 nights to spare" in by[cap.GA4_BACKFILL].description
        assert by["pg_attribution_coverage_website"].count == 128

    def test_it_is_sent_for_the_two_real_warns(self):
        message = build_digest([_section(_nov_14_run(), _nov_13_digest_run())],
                               last_sent_at=NOV_14 - timedelta(days=1), now=NOV_14)
        assert message is not None
        assert "(pg_attribution_coverage_website): 128 (new)" in message
        assert "(pg_value_domain_orders_source_id): 2 (+1)" in message
        # The INFO findings ride along as content.
        assert "(ga4_backfill_incomplete): 471 (new)" in message
        assert f"(capture_unstarted:{VINTAGES}): 1 (new)" in message
        assert "(pg_headline_vs_line_items): 461 (+2)" in message

    def test_without_them_the_infos_alone_send_nothing(self):
        """What the owner asked for: the three findings that sent the digest
        every morning say nothing a person must act on. Judged as they were
        before (WARN), the same morning would have been sent for them."""
        quiet = [i for i in _nov_14_run() if i.severity == Severity.INFO]
        assert build_digest([_section(quiet, _nov_13_digest_run())],
                            last_sent_at=NOV_14 - timedelta(days=1), now=NOV_14) is None
        noisy = [dataclasses.replace(i, severity=Severity.WARN,
                                     check_name=i.check_name.replace(
                                         cap.UNSTARTED, cap.STALLED))
                 if i.check_name != "pg_goods_shipped_without_sale" else i
                 for i in quiet]
        assert build_digest([_section(noisy, _nov_13_digest_run())],
                            last_sent_at=NOV_14 - timedelta(days=1), now=NOV_14) is not None


class TestTheMorningAfter:
    def test_only_the_info_counts_moved_and_nothing_is_sent(self):
        """The 15th: another certificate redeemed and another blogger
        shipment, 23 more GA4 periods kept, the vintages still inside their
        99 h; the two WARNs unchanged since the digest that said them."""
        after = (_headline(463, 108, goods=813) + _captures(NOV_15, kept_nights=2)
                 + _attribution() + _source_domain(2))
        by = {i.check_name: i for i in after}
        assert by[cap.GA4_BACKFILL].severity == Severity.INFO
        assert by[cap.GA4_BACKFILL].count != 471
        assert by[f"capture_unstarted:{VINTAGES}"].severity == Severity.INFO
        assert by["pg_headline_vs_line_items"].count == 463
        message = build_digest([_section(after, _nov_14_run())],
                               last_sent_at=NOV_14 + timedelta(hours=1), now=NOV_15)
        assert message is None

    def test_a_warn_that_moves_is_still_news(self):
        """The quiet is the INFOs', not the digest's: the attribution WARN
        moving by one sends it the same morning."""
        after = (_headline(463, 108) + _captures(NOV_15, kept_nights=2)
                 + twins._attribution_check(Attribution(151, 22, 600, 150), WM)
                 + _source_domain(2))
        message = build_digest([_section(after, _nov_14_run())],
                               last_sent_at=NOV_14 + timedelta(hours=1), now=NOV_15)
        assert message is not None and "129 (+1)" in message
