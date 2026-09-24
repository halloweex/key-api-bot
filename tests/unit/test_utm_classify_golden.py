"""The UTM parser moved out of the DuckDB mixin, and nothing it says moved (DN-15).

`_parse_utm_from_comment` and `_classify_traffic` left `TrafficMixin` for
`core/utm_classify.py`, verbatim, so the reclassify dry run and step 9's
Postgres parser can call them without a store. What proves "verbatim" is not
a diff of the source but the outputs: every tuple below was produced by the
methods as they stood on the mixin before the move (`8cf727a`), by running
them over these fixtures, and is frozen here as a literal. The new functions,
the mixin's names for them and the DuckDB parse that writes the rows must all
still produce exactly these.

The fixtures are not a sample, they are a sweep. Every `return`, every
`platform =`, every `if` and every operand of every `or` in the classifier was
mutated one at a time before these were frozen, and each of the 83 mutants
changed at least one tuple here; so did 23 of 25 mutations of the parser's
regexes and steps. The two that change nothing are equivalent — stripping a
block whose values are stripped anyway, and short-circuiting `None` to `''` —
and a golden cannot see an equivalent mutant by definition.
`test_every_verdict_the_classifier_can_return_has_a_fixture` keeps it a
sweep: a new branch with no fixture here fails it.

A RULE CHANGE IS NOT A REFACTOR

Changing what the parser says about a comment is a reclassify decision
(OD-06): stored verdicts keep the old answer until one is run. So a tuple
here changes in the same commit as the rule, deliberately, and the commit says
which stored verdicts it moves — `scripts/utm_reclassify_dryrun.py` counts
them.
"""
from __future__ import annotations

import ast
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from core import pg_order_utm
from core.repositories.traffic import TrafficMixin
from core.utm_classify import (
    UTM_VERDICT_COLUMNS,
    classify_traffic,
    parse_utm_from_comment,
    tab_platform,
    utm_columns,
)

REPO = Path(__file__).resolve().parents[2]

# ─── frozen: the outputs of the methods on the mixin at 8cf727a ──────────────
#
# (comment, what the parser returned, the row the parse wrote without order_id
# and parsed_at — UTM_VERDICT_COLUMNS in order)
PARSE_GOLDEN = [
    ('',
     {},
     (None, None, None, None, None, None, None, None, None, None, None, None)),
    ('просто комментарий про доставку',
     {},
     (None, None, None, None, None, None, None, None, None, None, None, None)),
    ('Передзвонити після 18:00',
     {},
     (None, None, None, None, None, None, None, None, None, None, None, None)),
    ('UTM: utm_source: fbads; utm_medium: cpc; utm_campaign: summer',
     {'utm_source': 'fbads', 'utm_medium': 'cpc', 'utm_campaign': 'summer'},
     ('fbads', 'cpc', 'summer', None, None, None, None, None, None, None, 'paid_confirmed', 'facebook')),
    ('Клиент просил перезвонить\nUTM: utm_source: fbads; utm_medium: cpc',
     {'utm_source': 'fbads', 'utm_medium': 'cpc'},
     ('fbads', 'cpc', None, None, None, None, None, None, None, None, 'paid_confirmed', 'facebook')),
    ('UTM: utm_source: fbads; utm_medium: cpc\nDelivery: Nova Poshta #23',
     {'utm_source': 'fbads', 'utm_medium': 'cpc'},
     ('fbads', 'cpc', None, None, None, None, None, None, None, None, 'paid_confirmed', 'facebook')),
    ('UTM: utm_source: fbads; utm_medium: cpc\nДоставка: НП',
     {'utm_source': 'fbads', 'utm_medium': 'cpc'},
     ('fbads', 'cpc', None, None, None, None, None, None, None, None, 'paid_confirmed', 'facebook')),
    ('UTM: utm_source: fbads; utm_medium: cpc\n\nутм: ще щось',
     {'utm_source': 'fbads', 'utm_medium': 'cpc'},
     ('fbads', 'cpc', None, None, None, None, None, None, None, None, 'paid_confirmed', 'facebook')),
    ('UTM: _fbp: fb.1.123.456; _fbc: fb.1.789; ttp: TTP123; fbclid: IwAR1',
     {'_fbp': 'fb.1.123.456', '_fbc': 'fb.1.789', 'ttp': 'TTP123', 'fbclid': 'IwAR1'},
     (None, None, None, None, None, None, 'fb.1.123.456', 'fb.1.789', 'TTP123', 'IwAR1', 'paid_likely', 'facebook')),
    ('UTM: utm_campaign: TOF | ss | broad; utm_source: tiktok',
     {'utm_campaign': 'TOF | ss | broad', 'utm_source': 'tiktok'},
     ('tiktok', None, 'TOF | ss | broad', None, None, None, None, None, None, None, 'paid_confirmed', 'tiktok')),
    ('UTM: utm_source: fbads; utm_content: https://fb.com/ad?id=1; utm_medium: cpc',
     {'utm_source': 'fbads', 'utm_content': 'https://fb.com/ad?id=1', 'utm_medium': 'cpc'},
     ('fbads', 'cpc', None, 'https://fb.com/ad?id=1', None, None, None, None, None, None, 'paid_confirmed', 'facebook')),
    ('UTM: utm_source: google; utm_medium: cpc; utm_campaign: 23436234141; utm_term: крем; utm_lang: uk',
     {'utm_source': 'google', 'utm_medium': 'cpc', 'utm_campaign': '23436234141', 'utm_term': 'крем', 'utm_lang': 'uk'},
     ('google', 'cpc', '23436234141', None, 'крем', 'uk', None, None, None, None, 'paid_confirmed', 'google')),
    ('UTM: utm_source: Facebook; utm_medium: Paid_Social; utm_campaign: Spring Sale.',
     {'utm_source': 'Facebook', 'utm_medium': 'Paid_Social', 'utm_campaign': 'Spring Sale'},
     ('Facebook', 'Paid_Social', 'Spring Sale', None, None, None, None, None, None, None, 'paid_confirmed', 'facebook')),
    ('UTM: utm_source: fbads,; utm_medium: cpc.',
     {'utm_source': 'fbads', 'utm_medium': 'cpc'},
     ('fbads', 'cpc', None, None, None, None, None, None, None, None, 'paid_confirmed', 'facebook')),
    ('UTM: utm_source: ; utm_medium: cpc',
     {'utm_medium': 'cpc'},
     (None, 'cpc', None, None, None, None, None, None, None, None, 'paid_likely', 'other')),
    ('UTM: utm_id: 12345; utm_source: ig; utm_medium: social; foo: bar',
     {'utm_id': '12345', 'utm_source': 'ig', 'utm_medium': 'social', 'foo': 'bar'},
     ('ig', 'social', None, None, None, None, None, None, None, None, 'organic', 'instagram')),
    ('UTM: utm_source: fbads; utm_source: tiktok',
     {'utm_source': 'fbads'},
     ('fbads', None, None, None, None, None, None, None, None, None, 'paid_confirmed', 'facebook')),
    ('utm: utm_source: fbads; utm_medium: cpc',
     {'utm_source': 'fbads', 'utm_medium': 'cpc'},
     ('fbads', 'cpc', None, None, None, None, None, None, None, None, 'paid_confirmed', 'facebook')),
    ('UTM: utm_source: fbads\nutm_medium: cpc\nutm_campaign: test',
     {'utm_source': 'fbads', 'utm_medium': 'cpc', 'utm_campaign': 'test'},
     ('fbads', 'cpc', 'test', None, None, None, None, None, None, None, 'paid_confirmed', 'facebook')),
    ('UTM: utm_source: fbads;\nUtm_medium: cpc',
     {'utm_source': 'fbads', 'utm_medium': 'cpc'},
     ('fbads', 'cpc', None, None, None, None, None, None, None, None, 'paid_confirmed', 'facebook')),
    ('UTM: utm_source: fbads, utm_medium: cpc',
     {'utm_source': 'fbads', 'utm_medium': 'cpc'},
     ('fbads', 'cpc', None, None, None, None, None, None, None, None, 'paid_confirmed', 'facebook')),
    ('UTM: utm_source=fbads&utm_medium=cpc&utm_campaign=x',
     {'utm_source': 'fbads', 'utm_medium': 'cpc', 'utm_campaign': 'x'},
     ('fbads', 'cpc', 'x', None, None, None, None, None, None, None, 'paid_confirmed', 'facebook')),
    ('utm_source=fbads&utm_medium=cpc',
     {'utm_source': 'fbads', 'utm_medium': 'cpc'},
     ('fbads', 'cpc', None, None, None, None, None, None, None, None, 'paid_confirmed', 'facebook')),
    ('Заказ с лендинга https://site.com/?utm_source=google&utm_medium=cpc&utm_campaign=23436234141',
     {'utm_source': 'google', 'utm_medium': 'cpc', 'utm_campaign': '23436234141'},
     ('google', 'cpc', '23436234141', None, None, None, None, None, None, None, 'paid_confirmed', 'google')),
    ('utm_source: fbads; utm_medium: cpc; utm_campaign: x',
     {'utm_source': 'fbads', 'utm_medium': 'cpc', 'utm_campaign': 'x'},
     ('fbads', 'cpc', 'x', None, None, None, None, None, None, None, 'paid_confirmed', 'facebook')),
    ('UTM: utm_source: fbads; utm_medium: cpc\r\nDelivery: NP',
     {'utm_source': 'fbads', 'utm_medium': 'cpc'},
     ('fbads', 'cpc', None, None, None, None, None, None, None, None, 'paid_confirmed', 'facebook')),
    ('UTM: utm_source: fbads\nhttps://x.com?utm_source=other',
     {'utm_source': 'fbads', 'https': '//x.com?utm_source=other'},
     ('fbads', None, None, None, None, None, None, None, None, None, 'paid_confirmed', 'facebook')),
    ('UTM: utm_source: fbads\n_fbp: fb.1.1\nTtp: abc\nНотатка: терміново',
     {'utm_source': 'fbads', '_fbp': 'fb.1.1', 'ttp': 'abc'},
     ('fbads', None, None, None, None, None, 'fb.1.1', None, 'abc', None, 'paid_confirmed', 'facebook')),
    ('UTM: utm_source: klaviyo; utm_medium: email; utm_campaign: welcome_flow',
     {'utm_source': 'klaviyo', 'utm_medium': 'email', 'utm_campaign': 'welcome_flow'},
     ('klaviyo', 'email', 'welcome_flow', None, None, None, None, None, None, None, 'organic', 'email')),
    ('https://koreanstory.com/products/x?fbclid=IwAR0abc&utm_source=ig&utm_medium=social',
     {'fbclid': 'IwAR0abc', 'utm_source': 'ig', 'utm_medium': 'social'},
     ('ig', 'social', None, None, None, None, None, None, None, 'IwAR0abc', 'organic', 'instagram')),
    ('https://koreanstory.com/?fbclid=IwAR0zzz',
     {'fbclid': 'IwAR0zzz'},
     (None, None, None, None, None, None, None, None, None, 'IwAR0zzz', 'paid_likely', 'facebook')),
    ('Оплата карткою. _fbp=fb.1.1700000000.123 ttp=TT-9',
     {'_fbp': 'fb.1.1700000000.123', 'ttp': 'TT-9'},
     (None, None, None, None, None, None, 'fb.1.1700000000.123', None, 'TT-9', None, 'pixel_only', 'unattributed')),
    ('_fbc: fb.1.1700.IwAR; коментар',
     {'_fbc': 'fb.1.1700.IwAR'},
     (None, None, None, None, None, None, None, 'fb.1.1700.IwAR', None, None, 'paid_likely', 'facebook')),
    ("ttp: 2abcDEF; Доставка кур'єром",
     {'ttp': '2abcDEF'},
     (None, None, None, None, None, None, None, None, '2abcDEF', None, 'pixel_only', 'unattributed')),
    ('_fbp: fb.1.1700000000.42',
     {'_fbp': 'fb.1.1700000000.42'},
     (None, None, None, None, None, None, 'fb.1.1700000000.42', None, None, None, 'pixel_only', 'unattributed')),
    ('UTM: _fbp: fb.1.9; ttp: xyz',
     {'_fbp': 'fb.1.9', 'ttp': 'xyz'},
     (None, None, None, None, None, None, 'fb.1.9', None, 'xyz', None, 'pixel_only', 'unattributed')),
    ('UTM: utm_campaign: sales_manager_olga; utm_source: manager',
     {'utm_campaign': 'sales_manager_olga', 'utm_source': 'manager'},
     ('manager', None, 'sales_manager_olga', None, None, None, None, None, None, None, 'manager', 'manager')),
    ('UTM: utm_source: facebook; utm_medium: ; utm_campaign: 18_06_TOF_40aged_1_creo',
     {'utm_source': 'facebook', 'utm_campaign': '18_06_TOF_40aged_1_creo'},
     ('facebook', None, '18_06_TOF_40aged_1_creo', None, None, None, None, None, None, None, 'paid_confirmed', 'facebook')),
    ('UTM: utm_source: ; utm_medium: ; utm_campaign: mof_catalog_allpr_s_i',
     {'utm_campaign': 'mof_catalog_allpr_s_i'},
     (None, None, 'mof_catalog_allpr_s_i', None, None, None, None, None, None, None, 'paid_confirmed', 'tiktok')),
    ('UTM: utm_source: tiktok; utm_medium: paid; utm_campaign: spring',
     {'utm_source': 'tiktok', 'utm_medium': 'paid', 'utm_campaign': 'spring'},
     ('tiktok', 'paid', 'spring', None, None, None, None, None, None, None, 'paid_confirmed', 'tiktok')),
    ('UTM: utm_source: tiktok; utm_medium: social',
     {'utm_source': 'tiktok', 'utm_medium': 'social'},
     ('tiktok', 'social', None, None, None, None, None, None, None, None, 'organic', 'tiktok')),
    ('UTM: utm_source: google; utm_medium: product_sync',
     {'utm_source': 'google', 'utm_medium': 'product_sync'},
     ('google', 'product_sync', None, None, None, None, None, None, None, None, 'organic', 'google')),
    ('UTM: utm_source: instagram; utm_medium: bio',
     {'utm_source': 'instagram', 'utm_medium': 'bio'},
     ('instagram', 'bio', None, None, None, None, None, None, None, None, 'organic', 'instagram')),
    ('UTM: utm_source: facebook; utm_medium: organic',
     {'utm_source': 'facebook', 'utm_medium': 'organic'},
     ('facebook', 'organic', None, None, None, None, None, None, None, None, 'organic', 'facebook')),
    ('UTM: utm_source: chatgpt.com',
     {'utm_source': 'chatgpt.com'},
     ('chatgpt.com', None, None, None, None, None, None, None, None, None, 'organic', 'ai')),
    ('UTM: utm_source: perplexity; utm_medium: paid_search',
     {'utm_source': 'perplexity', 'utm_medium': 'paid_search'},
     ('perplexity', 'paid_search', None, None, None, None, None, None, None, None, 'paid_confirmed', 'ai')),
    ('UTM: utm_source: qr; utm_medium: referral',
     {'utm_source': 'qr', 'utm_medium': 'referral'},
     ('qr', 'referral', None, None, None, None, None, None, None, None, 'organic', 'other')),
    ('UTM: utm_source: novaposhta; utm_medium: sms',
     {'utm_source': 'novaposhta', 'utm_medium': 'sms'},
     ('novaposhta', 'sms', None, None, None, None, None, None, None, None, 'unknown', 'other')),
    ('UTM: utm_source: fb_retarget; utm_medium: cpc_v2',
     {'utm_source': 'fb_retarget', 'utm_medium': 'cpc_v2'},
     ('fb_retarget', 'cpc_v2', None, None, None, None, None, None, None, None, 'paid_likely', 'facebook')),
    ('UTM: utm_source: tt; utm_medium: paid_video',
     {'utm_source': 'tt', 'utm_medium': 'paid_video'},
     ('tt', 'paid_video', None, None, None, None, None, None, None, None, 'paid_likely', 'tiktok')),
    ('UTM: utm_source: google_maps; utm_medium: organic',
     {'utm_source': 'google_maps', 'utm_medium': 'organic'},
     ('google_maps', 'organic', None, None, None, None, None, None, None, None, 'organic', 'google')),
    ('UTM: utm_source: insta_story; utm_medium: social',
     {'utm_source': 'insta_story', 'utm_medium': 'social'},
     ('insta_story', 'social', None, None, None, None, None, None, None, None, 'organic', 'instagram')),
    ('UTM: utm_source: telegram; utm_medium: post',
     {'utm_source': 'telegram', 'utm_medium': 'post'},
     ('telegram', 'post', None, None, None, None, None, None, None, None, 'unknown', 'telegram')),
    ('UTM: utm_medium: newsletter',
     {'utm_medium': 'newsletter'},
     (None, 'newsletter', None, None, None, None, None, None, None, None, 'unknown', 'other')),
    ('UTM: utm_source: salesfb; utm_medium: ',
     {'utm_source': 'salesfb'},
     ('salesfb', None, None, None, None, None, None, None, None, None, 'paid_confirmed', 'facebook')),
    ('UTM: utm_source: advantage_plus_catalog',
     {'utm_source': 'advantage_plus_catalog'},
     ('advantage_plus_catalog', None, None, None, None, None, None, None, None, None, 'paid_confirmed', 'facebook')),
    ('UTM: utm_source: bloger; utm_campaign: adventage_test',
     {'utm_source': 'bloger', 'utm_campaign': 'adventage_test'},
     ('bloger', None, 'adventage_test', None, None, None, None, None, None, None, 'paid_confirmed', 'facebook')),
    ('UTM: utm_source: partner; utm_content: facebook_ua_video',
     {'utm_source': 'partner', 'utm_content': 'facebook_ua_video'},
     ('partner', None, None, 'facebook_ua_video', None, None, None, None, None, None, 'paid_confirmed', 'facebook')),
    ('UTM: utm_source: x; utm_medium: y; utm_campaign: bof | dynamic',
     {'utm_source': 'x', 'utm_medium': 'y', 'utm_campaign': 'bof | dynamic'},
     ('x', 'y', 'bof | dynamic', None, None, None, None, None, None, None, 'paid_confirmed', 'tiktok')),
    ('UTM: utm_source: fbads\n\nutm_medium: cpc',
     {'utm_source': 'fbads'},
     ('fbads', None, None, None, None, None, None, None, None, None, 'paid_confirmed', 'facebook')),
    ('Utm: utm_source: ig; campaign_id: 55',
     {'utm_source': 'ig', 'campaign_id': '55'},
     ('ig', None, None, None, None, None, None, None, None, None, 'organic', 'instagram')),
    ('UTM: utm_source: fbads\rutm_medium: cpc',
     {'utm_source': 'fbads', 'utm_medium': 'cpc'},
     ('fbads', 'cpc', None, None, None, None, None, None, None, None, 'paid_confirmed', 'facebook')),
    ('Замовлення з сайту ?_fbc=fb.1.99',
     {'_fbc': 'fb.1.99'},
     (None, None, None, None, None, None, None, 'fb.1.99', None, None, 'paid_likely', 'facebook')),
    ('utm_source=fbads;utm_medium=cpc',
     {'utm_source': 'fbads', 'utm_medium': 'cpc'},
     ('fbads', 'cpc', None, None, None, None, None, None, None, None, 'paid_confirmed', 'facebook')),
    ('UTM: utm_source: fbads\nІнше: текст',
     {'utm_source': 'fbads'},
     ('fbads', None, None, None, None, None, None, None, None, None, 'paid_confirmed', 'facebook')),
]

# (utm_data, what the classifier returned)
CLASSIFY_GOLDEN = [
    ({}, ('unknown', 'unattributed')),
    ({'utm_campaign': 'sales_manager_olga'}, ('manager', 'manager')),
    ({'utm_campaign': 'Sales_Manager_ivan', 'utm_source': 'fbads'}, ('manager', 'manager')),
    ({'utm_source': 'klaviyo'}, ('organic', 'email')),
    ({'utm_source': 'email', 'utm_medium': 'cpc'}, ('organic', 'email')),
    ({'utm_medium': 'email'}, ('organic', 'email')),
    ({'utm_medium': 'Klaviyo'}, ('organic', 'email')),
    ({'utm_source': 'fbads_ua'}, ('paid_confirmed', 'facebook')),
    ({'utm_source': 'fb_ads_catalog'}, ('paid_confirmed', 'facebook')),
    ({'utm_source': 'fbsales'}, ('paid_confirmed', 'facebook')),
    ({'utm_source': 'salesfb'}, ('paid_confirmed', 'facebook')),
    ({'utm_medium': 'fbads_story'}, ('paid_confirmed', 'facebook')),
    ({'utm_medium': 'facebook_ads'}, ('paid_confirmed', 'facebook')),
    ({'utm_medium': 'fbsales'}, ('paid_confirmed', 'facebook')),
    ({'utm_medium': 'salescpcfb'}, ('paid_confirmed', 'facebook')),
    ({'utm_medium': 'cpcfb'}, ('paid_confirmed', 'facebook')),
    ({'utm_campaign': 'fbads_spring'}, ('paid_confirmed', 'facebook')),
    ({'utm_content': 'x_facebook_ua_y'}, ('paid_confirmed', 'facebook')),
    ({'utm_source': 'meta_advantage'}, ('paid_confirmed', 'facebook')),
    ({'utm_campaign': 'catalog advantage+'}, ('paid_confirmed', 'facebook')),
    ({'utm_source': 'adventage'}, ('paid_confirmed', 'facebook')),
    ({'utm_campaign': 'big_adventage'}, ('paid_confirmed', 'facebook')),
    ({'utm_source': 'facebook', 'utm_medium': 'paid'}, ('paid_confirmed', 'facebook')),
    ({'utm_source': 'fb', 'utm_medium': 'cpc'}, ('paid_confirmed', 'facebook')),
    ({'utm_source': 'facebook', 'utm_medium': 'paid_social'}, ('paid_confirmed', 'facebook')),
    ({'utm_source': 'fb', 'utm_medium': 'sales'}, ('paid_confirmed', 'facebook')),
    ({'utm_source': 'facebook', 'utm_medium': '', 'utm_campaign': '18_06_TOF_40aged_1_creo'}, ('paid_confirmed', 'facebook')),
    ({'utm_source': 'fbx', 'utm_campaign': 'mof'}, ('paid_confirmed', 'facebook')),
    ({'utm_source': '', 'utm_campaign': 'TOF | ss | broad'}, ('paid_confirmed', 'tiktok')),
    ({'utm_source': 'x', 'utm_campaign': 'promo | retarget'}, ('paid_confirmed', 'tiktok')),
    ({'utm_source': 'x', 'utm_campaign': 'promo | dynamic'}, ('paid_confirmed', 'tiktok')),
    ({'utm_source': 'x', 'utm_campaign': 'bof|feed'}, ('paid_confirmed', 'tiktok')),
    ({'utm_source': 'x', 'utm_campaign': 'tofu'}, ('unknown', 'other')),
    ({'utm_source': 'tiktok', 'utm_medium': 'paid'}, ('paid_confirmed', 'tiktok')),
    ({'utm_source': 'tiktok', 'utm_medium': 'cpc'}, ('paid_confirmed', 'tiktok')),
    ({'utm_source': 'google', 'utm_medium': 'cpc'}, ('paid_confirmed', 'google')),
    ({'utm_source': 'google', 'utm_medium': 'cpc_brand'}, ('paid_confirmed', 'google')),
    ({'utm_source': 'google', 'utm_medium': '', 'utm_campaign': '23436234141'}, ('paid_confirmed', 'google')),
    ({'utm_source': 'google', 'utm_medium': 'product_sync'}, ('organic', 'google')),
    ({'utm_source': 'ig'}, ('organic', 'instagram')),
    ({'utm_source': 'Instagram', 'utm_medium': 'paid'}, ('organic', 'instagram')),
    ({'utm_source': 'facebook', 'utm_medium': 'social'}, ('organic', 'facebook')),
    ({'utm_source': 'facebook', 'utm_medium': 'organic'}, ('organic', 'facebook')),
    ({'utm_source': 'tiktok', 'utm_medium': 'social'}, ('organic', 'tiktok')),
    ({'utm_source': 'tiktok', 'utm_medium': 'organic'}, ('organic', 'tiktok')),
    ({'utm_source': 'tiktok'}, ('organic', 'tiktok')),
    ({'utm_source': 'chatgpt.com'}, ('organic', 'ai')),
    ({'utm_source': 'openai', 'utm_medium': 'cpc'}, ('paid_confirmed', 'ai')),
    ({'utm_source': 'perplexity', 'utm_medium': 'paid_search'}, ('paid_confirmed', 'ai')),
    ({'utm_source': 'claude.ai', 'utm_medium': 'ppc'}, ('paid_confirmed', 'ai')),
    ({'utm_source': 'gemini', 'utm_medium': 'paid'}, ('paid_confirmed', 'ai')),
    ({'utm_source': 'copilot', 'utm_medium': 'referral'}, ('organic', 'ai')),
    ({'utm_source': 'meta.ai'}, ('organic', 'ai')),
    ({'utm_source': 'you.com'}, ('organic', 'ai')),
    ({'utm_source': 'facebook', 'utm_medium': 'cpm'}, ('unknown', 'facebook')),
    ({'utm_source': 'facebook_group', 'utm_medium': 'referral'}, ('organic', 'facebook')),
    ({'utm_source': 'fb-post', 'utm_medium': 'paid_boost'}, ('paid_likely', 'facebook')),
    ({'utm_source': 'tiktok', 'utm_medium': 'video'}, ('unknown', 'tiktok')),
    ({'utm_source': 'tt_shop', 'utm_medium': 'cpc_video'}, ('paid_likely', 'tiktok')),
    ({'utm_source': 'ttads', 'utm_medium': 'x'}, ('unknown', 'tiktok')),
    ({'utm_source': 'google-maps', 'utm_medium': 'organic'}, ('organic', 'google')),
    ({'utm_source': 'insta story', 'utm_medium': 'social'}, ('organic', 'instagram')),
    ({'utm_source': 'telegram', 'utm_medium': 'post'}, ('unknown', 'telegram')),
    ({'utm_source': 'qr', 'utm_medium': 'referral'}, ('organic', 'other')),
    ({'utm_source': 'novaposhta', 'utm_medium': 'sms'}, ('unknown', 'other')),
    ({'utm_source': 'rivo', 'utm_medium': 'ppc'}, ('paid_likely', 'other')),
    ({'utm_medium': 'paid'}, ('paid_likely', 'other')),
    ({'utm_medium': 'newsletter'}, ('unknown', 'other')),
    ({'utm_source': '', 'utm_medium': '', '_fbc': 'fb.1'}, ('paid_likely', 'facebook')),
    ({'fbclid': 'IwAR'}, ('paid_likely', 'facebook')),
    ({'_fbc': 'fb.1', '_fbp': 'fb.2', 'ttp': 't'}, ('paid_likely', 'facebook')),
    ({'_fbp': 'fb.1'}, ('pixel_only', 'unattributed')),
    ({'ttp': 'T'}, ('pixel_only', 'unattributed')),
    ({'_fbp': 'fb.1', 'ttp': 'T'}, ('pixel_only', 'unattributed')),
    ({'utm_term': 'крем', 'utm_lang': 'uk'}, ('unknown', 'unattributed')),
    ({'utm_source': 'facebook', '_fbp': 'fb.1'}, ('unknown', 'facebook')),
    ({'utm_source': 'ig', 'fbclid': 'IwAR'}, ('organic', 'instagram')),
]


def _ids(fixtures):
    return [f"{i:02d}" for i in range(len(fixtures))]


# ─── the new functions against the frozen outputs ─────────────────────────────

@pytest.mark.parametrize("comment, parsed, row", PARSE_GOLDEN, ids=_ids(PARSE_GOLDEN))
def test_parse_matches_the_golden(comment, parsed, row):
    assert parse_utm_from_comment(comment) == parsed


@pytest.mark.parametrize("comment, parsed, row", PARSE_GOLDEN, ids=_ids(PARSE_GOLDEN))
def test_the_row_matches_the_golden(comment, parsed, row):
    assert utm_columns(comment) == row


@pytest.mark.parametrize("utm_data, verdict", CLASSIFY_GOLDEN, ids=_ids(CLASSIFY_GOLDEN))
def test_classify_matches_the_golden(utm_data, verdict):
    assert classify_traffic(utm_data) == verdict


def test_no_comment_is_no_data_and_a_row_of_nulls():
    """`None` never reaches the parse through its own SELECT, but the dry run
    hands it an order with no comment, and it must read as nothing."""
    assert parse_utm_from_comment(None) == {}
    assert utm_columns(None) == (None,) * len(UTM_VERDICT_COLUMNS)


# ─── the old names agree ──────────────────────────────────────────────────────

@pytest.mark.parametrize("comment, parsed, row", PARSE_GOLDEN, ids=_ids(PARSE_GOLDEN))
def test_the_mixin_parses_as_it_did(comment, parsed, row):
    assert TrafficMixin._parse_utm_from_comment(comment) == parsed


@pytest.mark.parametrize("utm_data, verdict", CLASSIFY_GOLDEN, ids=_ids(CLASSIFY_GOLDEN))
def test_the_mixin_classifies_as_it_did(utm_data, verdict):
    assert TrafficMixin._classify_traffic(utm_data) == verdict


def test_the_mixin_holds_the_functions_not_copies():
    """Delegation by identity: a second copy of the classifier on the mixin is
    exactly the drift the move exists to prevent, and a golden over outputs
    would only notice it once the copies disagreed on a fixture."""
    assert TrafficMixin._parse_utm_from_comment is parse_utm_from_comment
    assert TrafficMixin._classify_traffic is classify_traffic


@pytest.mark.asyncio
async def test_the_duckdb_parse_writes_the_golden_rows(tmp_path):
    """The whole old path — the mixin's SELECT, its loop, its INSERT — against
    the frozen rows, so the loop's switch to `utm_columns` is covered by the
    same evidence as the functions."""
    from core.duckdb_store import DuckDBStore

    stamp = datetime(2026, 9, 7, 9, 0, tzinfo=timezone.utc)
    expected = {}
    store = DuckDBStore(db_path=tmp_path / "golden.duckdb")
    await store.connect()
    try:
        async with store.connection() as conn:
            for i, (comment, _parsed, row) in enumerate(PARSE_GOLDEN, start=1):
                updated = stamp + timedelta(minutes=i)
                conn.execute(
                    "INSERT INTO orders (id, source_id, status_id, grand_total, "
                    "ordered_at, updated_at, buyer_id, manager_id, manager_comment) "
                    "VALUES (?, 4, 1, 100, ?, ?, 10, NULL, ?)",
                    [i, updated, updated, comment],
                )
                if comment:
                    expected[i] = (*row, updated)
        parsed = await store.refresh_utm_silver_layer()
        async with store.connection() as conn:
            stored = conn.execute(
                "SELECT order_id, " + ", ".join(UTM_VERDICT_COLUMNS) + ", parsed_at "
                "FROM silver_order_utm ORDER BY order_id").fetchall()
    finally:
        await store.close()

    assert parsed == set(expected), "the empty comment must not be selected"
    got = {r[0]: (*r[1:-1], r[-1].astimezone(timezone.utc)) for r in stored}
    assert got == expected


# ─── the golden stays a sweep ─────────────────────────────────────────────────

def test_every_verdict_the_classifier_can_return_has_a_fixture():
    """Walks `classify_traffic` for every literal it can return and asks that
    the frozen outputs contain it. A branch added without a fixture fails
    here, before the golden quietly stops covering the classifier."""
    tree = ast.parse((REPO / "core" / "utm_classify.py").read_text(encoding="utf-8"))
    fn = next(
        n for n in ast.walk(tree)
        if isinstance(n, ast.FunctionDef) and n.name == "classify_traffic"
    )
    verdicts = {v for _, v in CLASSIFY_GOLDEN} | {
        (row[-2], row[-1]) for _, _, row in PARSE_GOLDEN if row[-1] is not None}
    types = {v[0] for v in verdicts}
    platforms = {v[1] for v in verdicts}

    literal_returns, typed_returns, assigned = set(), set(), set()
    for node in ast.walk(fn):
        if isinstance(node, ast.Return) and isinstance(node.value, ast.Tuple):
            first, second = node.value.elts
            if isinstance(second, ast.Constant):
                literal_returns.add((first.value, second.value))
            else:
                typed_returns.add(first.value)
        if (isinstance(node, ast.Assign)
                and any(isinstance(t, ast.Name) and t.id == "platform" for t in node.targets)
                and isinstance(node.value, ast.Constant)):
            assigned.add(node.value.value)

    assert literal_returns and typed_returns and assigned, "the walk found nothing"
    assert literal_returns - verdicts == set()
    assert typed_returns - types == set()
    assert assigned - platforms == set()


def test_the_row_is_the_shippers_row():
    """`silver.order_utm` in Postgres is `order_id`, these columns, `parsed_at`.
    The dry run reads the stored verdict by these names, so a column added to
    one and not the other would compare a verdict against the wrong field."""
    assert pg_order_utm.UTM_COLUMNS == ("order_id", *UTM_VERDICT_COLUMNS, "parsed_at")
    assert UTM_VERDICT_COLUMNS[-2:] == ("traffic_type", "platform")


# ─── the tab's names for a verdict ────────────────────────────────────────────

# Frozen like the verdicts above, for the same reason: which Google orders the
# tab calls ads is a statement about the past as much as the future, and the
# reclassify dry run's platform table is read in these names.
TAB_PLATFORM_GOLDEN = {
    ("google", "paid_confirmed"): "google_ads",
    ("google", "paid_likely"): "google_ads",
    ("google", "organic"): "google_organic",
    ("google", "pixel_only"): "google_organic",
    ("google", "manager"): "google_organic",
    ("google", "unknown"): "google_organic",
    ("facebook", "paid_confirmed"): "facebook",
    ("unattributed", "pixel_only"): "unattributed",
}


@pytest.mark.parametrize("verdict, shown", list(TAB_PLATFORM_GOLDEN.items()),
                         ids=[f"{p}-{t}" for p, t in TAB_PLATFORM_GOLDEN])
def test_the_tab_names_the_platform_as_it_did(verdict, shown):
    platform, traffic_type = verdict
    assert tab_platform(platform, traffic_type) == shown


def test_every_traffic_type_has_a_google_name_frozen():
    """A traffic type added to the classifier lands in one of the two Google
    slices by default; which one is a decision, so it has to be written here."""
    types = {v[0] for _, v in CLASSIFY_GOLDEN} | {
        row[-2] for _, _, row in PARSE_GOLDEN if row[-2] is not None}
    assert types - {t for p, t in TAB_PLATFORM_GOLDEN if p == "google"} == set()
