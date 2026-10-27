"""Organic on /traffic, split into the channels it is made of.

`organic` is one verdict over very different things. Measured on production
2026-10-19, 180 days of retail, ₴14.1M of it: ₴9.0M were orders a manager took
in Instagram Direct with nothing in the comment, ₴3.1M visits from the link in
the profile, ₴0.9M email — welcome and abandoned-cart letters more than the
newsletters — ₴0.7M Telegram, ₴0.3M Google's free listings, and a handful of
bloggers' stories and ChatGPT answers. One number on a card said none of that.

The rule is `core.utm_classify.organic_channel`, read from the stored tags at
query time. Every case below is a tag production actually carries; the
literal half is `tests/integration/test_traffic_two_engines.py`. Since step
14's S14-05 the tab reads Postgres alone, so the repository's Silver rows go
into Postgres where they used to go into DuckDB; those tests skip without
`KS_PG_DSN`.
"""
from __future__ import annotations

import os
from datetime import date, timedelta
from typing import Optional

import pytest
import pytest_asyncio

from core.duckdb_store import DuckDBStore
from tests.integration.pg_tab_seed import (
    needs_postgres, store_that_never_reaches_duckdb, tab_in_postgres,
)
from core.utm_classify import (
    ORGANIC_CHANNELS,
    classify_traffic,
    organic_channel,
    organic_detail,
)

DAY = date(2026, 8, 12)
WINDOW = (DAY - timedelta(days=1), DAY + timedelta(days=1))
# Inside the three days before WINDOW: `_comparison_window`'s previous period.
BEFORE = DAY - timedelta(days=3)


def _channel(utm: dict, *, source_id: int = 4) -> str:
    """The channel the tab gives a comment carrying `utm`, through the real
    classifier — so a test here cannot pass on a verdict the parser would
    never write."""
    traffic_type, platform = classify_traffic(utm)
    assert traffic_type == "organic", (utm, traffic_type)
    return organic_channel(
        source_id=source_id, tagged=True, platform=platform,
        utm_medium=utm.get("utm_medium"), utm_campaign=utm.get("utm_campaign"),
    )


class TestTheRule:

    def test_an_order_with_no_tag_is_direct_or_telegram_by_its_source(self):
        """Organic without a verdict is organic only through the readers'
        COALESCE: a manager's order on source 1 or 2. That is what Direct
        means, and it is two thirds of organic revenue."""
        for source_id, expected in ((1, "instagram_direct"), (2, "telegram")):
            assert organic_channel(
                source_id=source_id, tagged=False,
                platform="instagram" if source_id == 1 else "telegram",
                utm_medium=None, utm_campaign=None,
            ) == expected

    @pytest.mark.parametrize("utm,expected", [
        ({"utm_source": "ig", "utm_medium": "social",
          "utm_content": "link_in_bio"}, "instagram_site"),
        ({"utm_source": "instagram", "utm_medium": "social"}, "instagram_site"),
        ({"utm_source": "instagram"}, "instagram_site"),
        ({"utm_source": "instagram", "utm_medium": "stories_tanya_pads",
          "utm_campaign": "tanya10"}, "instagram_stories"),
        ({"utm_source": "instagram", "utm_medium": "stories"}, "instagram_stories"),
        ({"utm_source": "klaviyo", "utm_medium": "klaviyo",
          "utm_campaign": "[07/28/26] Campaign #12: [PR] Бʼюті-рулетка"},
         "email_campaigns"),
        ({"utm_source": "klaviyo", "utm_medium": "campaign"}, "email_campaigns"),
        ({"utm_source": "klaviyo", "utm_medium": "email",
          "utm_campaign": "Welcome Email 1a: Welcome & Gift Reveal (Vt8RBn)"},
         "email_flows"),
        ({"utm_source": "klaviyo", "utm_medium": "flow"}, "email_flows"),
        ({"utm_source": "rivo", "utm_medium": "email",
          "utm_campaign": "rivo-loyalty-points-earned-email"}, "email_flows"),
        ({"utm_source": "shopify_email", "utm_medium": "email",
          "utm_campaign": "Твій кошик чекає на тебе 🛍️_186454245708"}, "email_flows"),
        ({"utm_source": "klaviyo", "utm_campaign": "()"}, "email_flows"),
        ({"utm_source": "google", "utm_medium": "product_sync",
          "utm_campaign": "sag_organic"}, "google_organic"),
        ({"utm_source": "chatgpt.com"}, "ai"),
        ({"utm_source": "facebook", "utm_medium": "social"}, "facebook"),
        ({"utm_source": "tiktok", "utm_medium": "social"}, "tiktok"),
        ({"utm_source": "telegram", "utm_medium": "social"}, "telegram"),
        ({"utm_source": "wishpicks", "utm_medium": "referral"}, "other"),
    ])
    def test_every_tag_production_carries_lands_somewhere_named(self, utm, expected):
        assert _channel(utm) == expected

    def test_a_campaign_is_told_by_its_name_whatever_the_medium(self):
        """The dated "Campaign #N" name is Klaviyo's own for a newsletter, so
        it decides even where the medium says `email`."""
        assert _channel({"utm_source": "klaviyo", "utm_medium": "email",
                         "utm_campaign": "[09/04/26] Campaign #2: осінь"}) \
            == "email_campaigns"

    def test_the_medium_is_compared_without_case(self):
        assert _channel({"utm_source": "instagram",
                         "utm_medium": "Stories_Anna15"}) == "instagram_stories"

    def test_the_channels_are_the_ones_the_tab_knows(self):
        """Every channel the rule can return is in `ORGANIC_CHANNELS`, which
        the frontend translates; a new one has to be named there too."""
        returned = {
            organic_channel(source_id=s, tagged=t, platform=p,
                            utm_medium=m, utm_campaign=c)
            for s in (1, 2, 4) for t in (True, False)
            for p in ("instagram", "email", "google", "telegram", "ai",
                      "facebook", "tiktok", "other", "unattributed")
            for m in ("", "stories_x", "klaviyo", "email")
            for c in ("", "Campaign #1")
        }
        assert returned <= set(ORGANIC_CHANNELS)


class TestTheRowInsideAChannel:
    """GA4's shape: source / medium, then campaign and content."""

    def test_source_and_medium_are_a_place_and_read_lower_case(self):
        """`Instagram` and `instagram` are one source in GA4's report and
        one row here; the campaign is the marketer's own text and keeps its
        case, so they find the letter they named."""
        assert organic_detail(
            utm_source=" Instagram ", utm_medium="Stories_Tanya_Pads",
            utm_campaign="Tanya10", utm_content="",
        ) == ("instagram", "stories_tanya_pads", "Tanya10", "")

    def test_the_profile_link_is_told_by_its_content(self):
        """1 140 of 1 171 profile-link orders carry no campaign, only
        `utm_content=link_in_bio` — so content is part of the row."""
        assert organic_detail(
            utm_source="ig", utm_medium="social",
            utm_campaign=None, utm_content="link_in_bio",
        ) == ("ig", "social", "", "link_in_bio")

    def test_an_untagged_order_is_a_row_of_its_own(self):
        """Direct is made of orders with no tags at all, and saying so is
        the answer to "what is in Direct" — so the row exists, all empty."""
        assert organic_detail(utm_source=None, utm_medium=None,
                              utm_campaign=None, utm_content=None) == ("", "", "", "")


# ─── The repository, on Postgres (DuckDB until step 14's S14-05) ─────────────

async def _nothing(conn, pool):
    """Each test writes the Silver rows it is about."""


@pytest_asyncio.fixture
async def tab(tmp_path, monkeypatch):
    """`(store, pool)`: the tab's store, which can only answer from Postgres,
    and the pool the rows go in through."""
    async with tab_in_postgres(os.environ["KS_PG_DSN"], monkeypatch, _nothing) as pool:
        yield store_that_never_reaches_duckdb(tmp_path, monkeypatch, "/traffic"), pool


async def _order(pool, order_id: int, *, total: float = 100.0,
                 source_id: int = 4, day: date = DAY, buyer_id: Optional[int] = None,
                 new: bool = False, utm: Optional[dict] = None,
                 sales_type: str = "retail") -> None:
    """One Silver order and, when `utm` is given, the row the parser would
    have written for those tags — verdict included, through the real
    classifier."""
    buyer = order_id if buyer_id is None else buyer_id
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO silver.orders (
                id, source_id, status_id, grand_total, ordered_at, buyer_id,
                manager_id, order_date, is_return, sales_type, is_active_source,
                source_name, is_new_customer, buyer_first_order_date, promocode
            ) VALUES ($1, $2, 12, $3, NULL, $4, NULL, $5, FALSE, $6, TRUE, 'x', $7, $8, NULL)
            """,
            order_id, source_id, total, buyer, day, sales_type, new,
            day if new else day - timedelta(days=90),
        )
        if utm is not None:
            traffic_type, platform = classify_traffic(utm)
            await conn.execute(
                "INSERT INTO silver.order_utm (order_id, utm_source, utm_medium,"
                " utm_campaign, utm_content, traffic_type, platform, parsed_at)"
                " VALUES ($1, $2, $3, $4, $5, $6, $7, now())",
                order_id, utm.get("utm_source"), utm.get("utm_medium"),
                utm.get("utm_campaign"), utm.get("utm_content"),
                traffic_type, platform,
            )


async def _organic(store, **kwargs) -> dict:
    return await store.get_traffic_organic(*WINDOW, **kwargs)


def _by_channel(out: dict) -> dict:
    return {c["channel"]: c for c in out["channels"]}


BIO = {"utm_source": "ig", "utm_medium": "social", "utm_content": "link_in_bio"}
WELCOME = {"utm_source": "klaviyo", "utm_medium": "email",
           "utm_campaign": "Welcome Email 1a"}
NEWSLETTER = {"utm_source": "klaviyo", "utm_medium": "klaviyo",
              "utm_campaign": "[08/10/26] Campaign #4: Neogen"}
PAID = {"utm_source": "fbads", "utm_medium": "paid", "utm_campaign": "spring"}


@needs_postgres
class TestTheBreakdown:

    @pytest.mark.asyncio
    async def test_each_order_is_counted_in_its_channel_and_paid_is_not(self, tab):
        store, pool = tab
        await _order(pool, 1, total=3000, source_id=1)
        await _order(pool, 2, total=2000, source_id=1)
        await _order(pool, 3, total=500, source_id=2)
        await _order(pool, 4, total=1200, utm=BIO)
        await _order(pool, 5, total=800, utm=WELCOME)
        await _order(pool, 6, total=400, utm=NEWSLETTER)
        await _order(pool, 7, total=9999, utm=PAID)
        out = await _organic(store)

        channels = _by_channel(out)
        assert [c["channel"] for c in out["channels"]] == [
            "instagram_direct", "instagram_site", "email_flows", "telegram",
            "email_campaigns",
        ]
        assert channels["instagram_direct"]["orders"] == 2
        assert channels["instagram_direct"]["revenue"] == 5000.0
        assert channels["email_flows"]["revenue"] == 800.0
        assert out["totals"]["orders"] == 6
        assert out["totals"]["revenue"] == 7900.0

    @pytest.mark.asyncio
    async def test_the_total_is_the_organic_card(self, tab):
        """The breakdown is of the card above it: the same orders, so the
        same sums — or a reader adding the rows gets a number the card does
        not show."""
        store, pool = tab
        await _order(pool, 1, total=3000, source_id=1)
        await _order(pool, 2, total=500, source_id=2)
        await _order(pool, 3, total=1200, utm=BIO)
        await _order(pool, 4, total=777, source_id=4)  # unknown, no tag
        await _order(pool, 5, total=9999, utm=PAID)
        organic = await _organic(store, sales_type="retail")
        card = (await store.get_traffic_analytics(
            *WINDOW, sales_type="retail"))["summary"]["organic"]

        assert {k: organic["totals"][k] for k in ("orders", "revenue")} == card

    @pytest.mark.asyncio
    async def test_the_previous_period_is_the_same_length_just_before(self, tab):
        """A channel that sold last period and nothing this one is the row an
        analyst most needs, so it is listed, with zeros where it stopped."""
        store, pool = tab
        await _order(pool, 1, total=3000, source_id=1)
        await _order(pool, 2, total=1000, source_id=1, day=BEFORE)
        await _order(pool, 3, total=600, day=BEFORE, utm=NEWSLETTER)
        await _order(pool, 4, total=50, day=WINDOW[0] - timedelta(days=4),
                     utm=NEWSLETTER)  # a day before the previous window
        out = await _organic(store)

        assert out["previous_period"] == {
            "start": (WINDOW[0] - timedelta(days=3)).isoformat(),
            "end": (WINDOW[0] - timedelta(days=1)).isoformat(),
        }
        channels = _by_channel(out)
        assert channels["instagram_direct"]["previous"] == {
            "orders": 1, "revenue": 1000.0}
        assert channels["email_campaigns"]["orders"] == 0
        assert channels["email_campaigns"]["previous"] == {
            "orders": 1, "revenue": 600.0}
        assert out["previous_totals"] == {"orders": 2, "revenue": 1600.0}

    @pytest.mark.asyncio
    async def test_new_buyers_are_people_and_do_not_add_up(self, tab):
        """Buyer 50's first day carried a Direct order and a welcome-letter
        order: new on both rows, one person in the total. Buyer 51 is not
        new at all."""
        store, pool = tab
        await _order(pool, 1, source_id=1, buyer_id=50, new=True)
        await _order(pool, 2, buyer_id=50, new=True, utm=WELCOME)
        await _order(pool, 3, source_id=1, buyer_id=51, new=False)
        out = await _organic(store)

        channels = _by_channel(out)
        assert channels["instagram_direct"]["new_customers"] == 1
        assert channels["email_flows"]["new_customers"] == 1
        assert out["totals"]["new_customers"] == 1

    @pytest.mark.asyncio
    async def test_the_rows_inside_a_channel_are_ranked_and_capped(
        self, tab, monkeypatch,
    ):
        monkeypatch.setattr(DuckDBStore, "ORGANIC_DETAIL_LIMIT", 2)
        store, pool = tab
        for i, (name, total) in enumerate(
                [("Welcome", 100), ("Cart", 300), ("Winback", 200),
                 ("Cart", 50)], start=1):
            await _order(pool, i, total=total, utm={
                "utm_source": "klaviyo", "utm_medium": "email",
                "utm_campaign": name})
        await _order(pool, 9, total=999, source_id=1)
        out = await _organic(store)

        channels = _by_channel(out)

        def row(campaign, orders, revenue, source="klaviyo", medium="email"):
            return {"source": source, "medium": medium, "campaign": campaign,
                    "content": "", "orders": orders, "revenue": revenue}

        assert channels["email_flows"]["details"] == [
            row("Cart", 2, 350.0), row("Winback", 1, 200.0)]
        assert channels["email_flows"]["details_total"] == 3
        assert channels["instagram_direct"]["details"] == [
            row("", 1, 999.0, source="", medium="")]
        assert channels["instagram_direct"]["details_total"] == 1

    @pytest.mark.asyncio
    async def test_the_filters_are_the_tabs(self, tab):
        store, pool = tab
        await _order(pool, 1, total=3000, source_id=1)
        await _order(pool, 2, total=1200, utm=BIO)
        await _order(pool, 3, total=700, source_id=1, sales_type="b2b")
        retail_shopify = await _organic(store, sales_type="retail", source_id=4)
        everyone = await _organic(store, sales_type="all")

        assert [c["channel"] for c in retail_shopify["channels"]] == ["instagram_site"]
        assert everyone["totals"] == {"orders": 3, "revenue": 4900.0,
                                      "new_customers": 0}

    @pytest.mark.asyncio
    async def test_an_empty_window_is_an_empty_breakdown(self, tab):
        store, pool = tab
        out = await _organic(store)

        assert out["channels"] == []
        assert out["totals"] == {"orders": 0, "revenue": 0.0, "new_customers": 0}
