"""Tests for TrafficMixin UTM parsing and traffic classification.

Both methods are pure (no I/O). The parser tests are the contract for
which manager_comment formats are recognized — the canonical
"UTM: key: value; ..." block plus real-world deviations (query strings,
URLs, newline-separated pairs, missing prefix). The classifier tests
lock in rule priority: explicit UTM > funnel-stage campaign patterns >
cookies/pixels > nothing.
"""
from __future__ import annotations

import pytest

from core.repositories.traffic import TrafficMixin

parse = TrafficMixin._parse_utm_from_comment
classify = TrafficMixin._classify_traffic


# ─── Parser: canonical format ────────────────────────────────────────────────

class TestParseCanonical:
    def test_basic_block(self):
        result = parse("UTM: utm_source: fbads; utm_medium: cpc; utm_campaign: summer")
        assert result == {
            "utm_source": "fbads", "utm_medium": "cpc", "utm_campaign": "summer",
        }

    def test_text_before_block(self):
        result = parse("Клиент просил перезвонить\nUTM: utm_source: fbads; utm_medium: cpc")
        assert result["utm_source"] == "fbads"
        assert result["utm_medium"] == "cpc"

    def test_block_ends_at_next_capitalized_section(self):
        result = parse("UTM: utm_source: fbads; utm_medium: cpc\nDelivery: Nova Poshta #23")
        assert "delivery" not in result
        assert result["utm_medium"] == "cpc"

    def test_block_ends_at_cyrillic_section(self):
        result = parse("UTM: utm_source: fbads; utm_medium: cpc\nДоставка: НП")
        assert "доставка" not in result
        assert result["utm_medium"] == "cpc"

    def test_pixels_and_click_ids(self):
        result = parse("UTM: _fbp: fb.1.123.456; _fbc: fb.1.789; ttp: TTP123; fbclid: IwAR1")
        assert result["_fbp"] == "fb.1.123.456"
        assert result["_fbc"] == "fb.1.789"
        assert result["ttp"] == "TTP123"
        assert result["fbclid"] == "IwAR1"

    def test_value_with_pipes_is_kept_whole(self):
        result = parse("UTM: utm_campaign: TOF | ss | broad; utm_source: tiktok")
        assert result["utm_campaign"] == "TOF | ss | broad"

    def test_value_with_url_is_kept_whole(self):
        result = parse("UTM: utm_source: fbads; utm_content: https://fb.com/ad?id=1; utm_medium: cpc")
        assert result["utm_content"] == "https://fb.com/ad?id=1"
        assert result["utm_medium"] == "cpc"

    def test_no_utm_returns_empty(self):
        assert parse("просто комментарий про доставку") == {}
        assert parse("") == {}
        assert parse(None) == {}


# ─── Parser: real-world deviations ───────────────────────────────────────────

class TestParseDeviations:
    def test_lowercase_prefix(self):
        result = parse("utm: utm_source: fbads; utm_medium: cpc")
        assert result["utm_source"] == "fbads"

    def test_newline_separated_pairs(self):
        result = parse("UTM: utm_source: fbads\nutm_medium: cpc\nutm_campaign: test")
        assert result == {
            "utm_source": "fbads", "utm_medium": "cpc", "utm_campaign": "test",
        }

    def test_capitalized_utm_key_on_next_line(self):
        result = parse("UTM: utm_source: fbads;\nUtm_medium: cpc")
        assert result["utm_medium"] == "cpc"

    def test_comma_separated_pairs(self):
        result = parse("UTM: utm_source: fbads, utm_medium: cpc")
        assert result["utm_source"] == "fbads"
        assert result["utm_medium"] == "cpc"

    def test_query_string_inside_block(self):
        result = parse("UTM: utm_source=fbads&utm_medium=cpc&utm_campaign=x")
        assert result == {"utm_source": "fbads", "utm_medium": "cpc", "utm_campaign": "x"}

    def test_bare_query_string(self):
        result = parse("utm_source=fbads&utm_medium=cpc")
        assert result["utm_source"] == "fbads"

    def test_utm_inside_url(self):
        result = parse("Заказ с лендинга https://site.com/?utm_source=google&utm_medium=cpc&utm_campaign=23436234141")
        assert result["utm_source"] == "google"
        assert result["utm_campaign"] == "23436234141"

    def test_bare_pairs_without_prefix(self):
        result = parse("utm_source: fbads; utm_medium: cpc; utm_campaign: x")
        assert result["utm_campaign"] == "x"

    def test_crlf_line_endings(self):
        result = parse("UTM: utm_source: fbads; utm_medium: cpc\r\nDelivery: NP")
        assert result["utm_medium"] == "cpc"
        assert "delivery" not in result

    def test_explicit_block_wins_over_query_string(self):
        result = parse("UTM: utm_source: fbads\nhttps://x.com?utm_source=other")
        assert result["utm_source"] == "fbads"


# ─── Classifier: rule priority ───────────────────────────────────────────────

class TestClassify:
    def test_sales_manager_campaign(self):
        assert classify({"utm_campaign": "sales_manager_olga"}) == ("manager", "manager")

    def test_fbads_source(self):
        assert classify({"utm_source": "fbads_ua", "utm_medium": ""}) == ("paid_confirmed", "facebook")

    def test_facebook_paid_medium(self):
        assert classify({"utm_source": "facebook", "utm_medium": "cpc"}) == ("paid_confirmed", "facebook")

    def test_funnel_campaign_with_facebook_source_stays_facebook(self):
        # TOF/MOF/BOF naming is used by Facebook campaigns too — an explicit
        # facebook source must not be reattributed to TikTok.
        for campaign in ("mof_catalog_allpr_s_i", "18_06_TOF_40aged_1_creo"):
            assert classify({"utm_source": "facebook", "utm_medium": "", "utm_campaign": campaign}) \
                == ("paid_confirmed", "facebook")

    def test_funnel_campaign_without_source_is_tiktok(self):
        assert classify({"utm_source": "", "utm_medium": "", "utm_campaign": "TOF | ss | broad"}) \
            == ("paid_confirmed", "tiktok")

    def test_google_ads_numeric_campaign(self):
        assert classify({"utm_source": "google", "utm_medium": "cpc", "utm_campaign": "23436234141"}) \
            == ("paid_confirmed", "google")

    def test_google_shopping_organic(self):
        assert classify({"utm_source": "google", "utm_medium": "product_sync"}) == ("organic", "google")

    def test_email_klaviyo(self):
        assert classify({"utm_source": "klaviyo", "utm_medium": ""}) == ("organic", "email")

    def test_instagram_organic(self):
        assert classify({"utm_source": "ig", "utm_medium": ""}) == ("organic", "instagram")

    def test_ai_assistant(self):
        assert classify({"utm_source": "chatgpt.com", "utm_medium": ""}) == ("organic", "ai")

    def test_fbc_cookie_only_is_paid_likely(self):
        assert classify({"_fbc": "fb.1.123"}) == ("paid_likely", "facebook")

    def test_fbclid_only_is_paid_likely(self):
        assert classify({"fbclid": "IwAR1"}) == ("paid_likely", "facebook")

    def test_fbp_pixel_only(self):
        assert classify({"_fbp": "fb.1.123"}) == ("pixel_only", "facebook")

    def test_no_data_is_unknown(self):
        assert classify({}) == ("unknown", "other")

    def test_explicit_utm_beats_cookie(self):
        # Cookie persists 90 days; explicit organic UTM must win.
        assert classify({"utm_source": "ig", "utm_medium": "", "_fbc": "fb.1.123"}) \
            == ("organic", "instagram")


# ─── Parser + classifier integration ─────────────────────────────────────────

class TestEndToEnd:
    @pytest.mark.parametrize("comment,expected", [
        ("UTM: utm_source: fbads; utm_medium: cpc; utm_campaign: orgahue_3_celltox_3_kyiv",
         ("paid_confirmed", "facebook")),
        ("Лендинг https://site.com/?utm_source=google&utm_medium=cpc&utm_campaign=23436234141",
         ("paid_confirmed", "google")),
        ("UTM: utm_source: fbads\nutm_medium: cpc",
         ("paid_confirmed", "facebook")),
        ("UTM: fbclid: IwAR123", ("paid_likely", "facebook")),
        ("комментарий без меток", ("unknown", "other")),
    ])
    def test_comment_to_classification(self, comment, expected):
        assert classify(parse(comment)) == expected
