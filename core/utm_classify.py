"""The UTM parser and the traffic classifier: one comment in, one verdict out.

`manager_comment` is free text, and what a chart on /traffic calls an order —
paid Facebook, organic Instagram, a pixel with no channel — is decided here, by
regexes over that text and then a chain of rules. Nothing else in the system
computes it: `silver_order_utm` in DuckDB stores what this returned when it
last looked, `silver.order_utm` in Postgres is a copy of that, and revision
0018 explains why a SQL re-derivation would be a second classifier that drifts
from the first.

WHY IT IS A MODULE AND NOT TWO METHODS ON THE MIXIN

It lived as two static methods on `TrafficMixin` (`core/repositories/traffic.py`)
until DN-15, step 9a of the DuckDB exit. They were already pure — `re` and
nothing else, no store, no clock — so moving them costs nothing, and three
readers need them without a DuckDB store in hand:

- the mixin itself, which still parses in the warehouse tick and the four
  re-parse doors, and now delegates here;
- `scripts/utm_reclassify_dryrun.py`, which recomputes every stored verdict in
  memory to say what a reclassify would move before anybody runs one;
- the Postgres parse of step 9b (DN-18), which will read `bronze.orders`
  instead of DuckDB's `orders` and must reach the same verdict on the same
  comment.

Moved verbatim. `tests/unit/test_utm_classify_golden.py` holds the outputs the
methods produced before the move, frozen as literals, and fails on any change
to them — including a deliberate one. A rule change is a reclassify decision
(OD-06), not a refactor, so the golden is where it has to be argued: update
the frozen tuple in the same change, and say which stored verdicts it moves.
"""
from __future__ import annotations

import re
from typing import Any, Callable, Dict, Optional, Tuple


# A pair value runs until a semicolon, newline, or the next ", key:" pair
# (comma-separated lists), so one malformed separator can't swallow the
# remaining pairs into a single corrupted value.
_UTM_PAIR_RE = re.compile(r'(\w+):\s*((?:(?!,\s*\w+:)[^;\n\r])+)')
# Query-string style (utm_source=fbads&fbclid=... — raw or inside a URL)
_UTM_QS_RE = re.compile(r'\b(utm_\w+|fbclid|ttp|_fbp|_fbc)=([^&\s;]+)')
# "key: value" pairs without any "UTM:" prefix (tracking keys only)
_UTM_BARE_RE = re.compile(r'\b(utm_\w+|fbclid|ttp|_fbp|_fbc):\s*((?:(?!,\s*\w+:)[^;\n\r])+)')


def parse_utm_from_comment(comment: str) -> Dict[str, Any]:
    """Parse UTM data from manager_comment field.

    The canonical format is:
    UTM: utm_source: value; utm_medium: value; ...

    Also tolerates real-world deviations: lowercase "utm:", pairs
    separated by newlines/commas, query-string form (utm_source=x&...)
    including UTM parameters inside URLs, and pairs without the
    "UTM:" prefix. Extracts pixel IDs like _fbp, _fbc, ttp, fbclid.
    """
    if not comment:
        return {}

    utm_data: Dict[str, Any] = {}

    def take(key: str, value: str) -> None:
        key = key.strip().lower()
        value = value.strip().rstrip('.,')
        if value and key not in utm_data:
            utm_data[key] = value

    # 1) Explicit "UTM:" block (prefix case-insensitive via scoped (?i:),
    #    NOT a global IGNORECASE — that would make the [A-Z] terminator
    #    class match lowercase and cut the block at any newline. The block
    #    ends at a blank line or a line starting with a capital letter
    #    (next comment section), unless that line is itself a UTM key.
    utm_match = re.search(
        r'(?i:UTM):\s*(.+?)(?:\n\s*\n|\n(?!(?i:utm_|_fb|ttp|fbclid))[A-ZА-ЯІЇЄ]|$)',
        comment, re.DOTALL,
    )
    if utm_match:
        for key, value in _UTM_PAIR_RE.findall(utm_match.group(1).strip()):
            take(key, value)

    # 2) Query-string pairs anywhere in the comment (raw or inside URLs)
    for key, value in _UTM_QS_RE.findall(comment):
        take(key, value)

    # 3) Fallback: bare "utm_x: value" pairs without the "UTM:" prefix
    if not utm_data:
        for key, value in _UTM_BARE_RE.findall(comment):
            take(key, value)

    return utm_data

def classify_traffic(utm_data: Dict[str, Any]) -> Tuple[str, str]:
    """Classify traffic based on UTM data.

    Returns:
        Tuple of (traffic_type, platform)

    Traffic types:
        - paid_confirmed: Strong evidence of paid ad (explicit UTM + click tracking)
        - paid_likely: Medium confidence paid (cookie/fbclid only, no explicit UTM)
        - manager: Sales manager driven (campaign starts with sales_manager_)
        - organic: Explicit organic/social medium with no ad indicators
        - pixel_only: Only pixel present, no UTM parameters
        - unknown: No tracking data at all

    Priority: explicit UTM params > cookies/pixels (cookies persist 90 days
    and don't indicate current session intent).
    """
    source = (utm_data.get('utm_source') or '').lower()
    medium = (utm_data.get('utm_medium') or '').lower()
    campaign = (utm_data.get('utm_campaign') or '').lower()
    content = (utm_data.get('utm_content') or '').lower()

    has_fbc = '_fbc' in utm_data  # Facebook click cookie (persists 90 days)
    has_fbp = '_fbp' in utm_data  # Facebook browser pixel
    has_ttp = 'ttp' in utm_data   # TikTok pixel
    has_fbclid = 'fbclid' in utm_data

    # ─── Explicit UTM rules (highest priority) ──────────────────────────────

    # 1. Sales manager tag (campaign starts with sales_manager_)
    if campaign.startswith('sales_manager_'):
        return 'manager', 'manager'

    # 2. Email / Klaviyo (source or medium indicates email)
    if source in ['klaviyo', 'email'] or medium in ['email', 'klaviyo']:
        return 'organic', 'email'

    # 3. Facebook Ads - fbads/fb_ads/fbsales/Advantage+ patterns
    if (source.startswith('fbads') or source.startswith('fb_ads') or
            source in ('fbsales', 'salesfb') or
            medium.startswith('fbads') or medium.startswith('facebook_ads') or
            medium in ('fbsales', 'salescpcfb', 'cpcfb') or
            campaign.startswith('fbads') or 'facebook_ua' in content or
            'advantage' in source or 'advantage' in campaign or
            'adventage' in source or 'adventage' in campaign):
        return 'paid_confirmed', 'facebook'

    # 4. Facebook Ads - explicit UTM (source=facebook/fb, medium=paid/cpc/paid_social/sales)
    if source in ('facebook', 'fb') and medium in ('paid', 'cpc', 'paid_social', 'sales'):
        return 'paid_confirmed', 'facebook'

    # 5. Funnel-stage campaign patterns (TOF/MOF/BOF). Historically these
    # were TikTok-only, but Facebook campaigns use the same naming —
    # trust the source when it says facebook.
    if campaign and re.search(
        r'(?:^|[\s_|])(?:tof|mof|bof)(?:[\s_|]|$)|'
        r'\| ss \||\| retarget|\| dynamic',
        campaign
    ):
        if source.startswith('fb') or 'facebook' in source:
            return 'paid_confirmed', 'facebook'
        return 'paid_confirmed', 'tiktok'

    # 6. TikTok Ads - source + medium
    if source == 'tiktok' and medium in ['paid', 'cpc']:
        return 'paid_confirmed', 'tiktok'

    # 7. Google Ads (source=google, medium starts with cpc, or numeric campaign)
    if source == 'google' and (medium.startswith('cpc') or (campaign and campaign.isdigit())):
        return 'paid_confirmed', 'google'

    # 8. Google Shopping organic (source=google, medium=product_sync)
    if source == 'google' and medium == 'product_sync':
        return 'organic', 'google'

    # 9. Instagram organic (source=ig/instagram)
    if source in ['ig', 'instagram']:
        return 'organic', 'instagram'

    # 10. Facebook organic (source=facebook, medium=social/organic)
    if source == 'facebook' and medium in ['social', 'organic']:
        return 'organic', 'facebook'

    # 11. TikTok organic (source=tiktok, medium not paid/cpc)
    if source == 'tiktok' and medium in ['social', 'organic', '']:
        return 'organic', 'tiktok'

    # 12. AI assistants — only ChatGPT auto-tags utm_source today
    # (others arrive untagged via Referer and can't be detected here).
    # Substring match because chatgpt.com is one token after split on _/-/space.
    ai_prefixes = ('chatgpt', 'openai', 'perplexity', 'claude', 'gemini',
                   'copilot', 'meta.ai', 'you.com')
    if any(source.startswith(p) for p in ai_prefixes):
        if medium in ('cpc', 'paid', 'ppc') or medium.startswith('paid'):
            return 'paid_confirmed', 'ai'
        return 'organic', 'ai'

    # ─── Generic UTM fallback (has source/medium but no known pattern) ──────

    if source or medium:
        # Infer platform from source (token-based to avoid substring false positives)
        platform = 'other'
        source_tokens = set(re.split(r'[_\-\s]', source))
        if 'facebook' in source_tokens or source_tokens & {'fb', 'fbads', 'fbsales', 'salesfb'}:
            platform = 'facebook'
        elif 'tiktok' in source_tokens or source_tokens & {'tt', 'ttads'}:
            platform = 'tiktok'
        elif 'google' in source_tokens:
            platform = 'google'
        elif source_tokens & {'instagram', 'insta', 'ig'}:
            platform = 'instagram'
        elif source_tokens & {'telegram'}:
            platform = 'telegram'

        if medium in ('cpc', 'paid', 'ppc') or medium.startswith('cpc') or medium.startswith('paid'):
            return 'paid_likely', platform
        elif medium in ('social', 'organic', 'referral'):
            return 'organic', platform
        else:
            return 'unknown', platform

    # ─── Cookie/pixel fallback (no explicit UTM matched above) ──────────────

    # 12. _fbc only (no UTM) → paid_likely (cookie persists 90 days, not confirmed)
    if has_fbc:
        return 'paid_likely', 'facebook'

    # 13. fbclid only → paid_likely (URL click ID, no UTM params)
    if has_fbclid:
        return 'paid_likely', 'facebook'

    # 14. Pixel-only — passive tracking, and **no platform to name**.
    #
    # `_fbp` and `ttp` are our own first-party cookies: the Meta and
    # TikTok pixels set them on any page view, whatever brought the
    # visitor. Neither says where the order came from, so neither may
    # name a channel — this used to return facebook for `_fbp` and
    # tiktok for `ttp`, which made the answer depend on the order of two
    # `if`s rather than on anything the visitor did.
    #
    # It was not a rare corner: measured on production 2026-09-08, 1,969
    # of 2,210 pixel-only retail orders over 180 days (₴4.55M) carry
    # **both** cookies, so ₴4.55M sat in the Facebook slice of the
    # platform chart and would have sat in TikTok's had the two lines
    # been written the other way round. Which pixel fired is still
    # visible per order — `_build_evidence` puts both in the evidence
    # column, where a claim that weak belongs.
    #
    # `unattributed` rather than `other`, and the distinction is the whole
    # point: `other` is a source we were *told* and have not learned to
    # name (`qr`, `novaposhta`), which is a different kind of ignorance
    # from having been told nothing. Folding them together is what made
    # ₴1.36M read as "miscellaneous small channels" on the chart.
    if has_fbp or has_ttp:
        return 'pixel_only', 'unattributed'

    # 15. No tracking data at all
    return 'unknown', 'unattributed'


# The columns a parse writes for one order, in the order `utm_columns` returns
# them: everything in `silver.order_utm` except the key and the two stamps.
# `core.pg_order_utm.UTM_COLUMNS` is this with `order_id` in front and
# `parsed_at` behind, and a test holds the two together.
UTM_VERDICT_COLUMNS: Tuple[str, ...] = (
    "utm_source", "utm_medium", "utm_campaign", "utm_content",
    "utm_term", "utm_lang",
    "fbp", "fbc", "ttp", "fbclid",
    "traffic_type", "platform",
)


def utm_columns(
    comment: Optional[str],
    *,
    parse: Callable[[str], Dict[str, Any]] = parse_utm_from_comment,
    classify: Callable[[Dict[str, Any]], Tuple[str, str]] = classify_traffic,
) -> Tuple[Optional[str], ...]:
    """The row a parse writes for `comment`, as `UTM_VERDICT_COLUMNS`.

    One place for the shape, because two readers build it and must build the
    same one: the DuckDB parse writes it, and the reclassify dry run compares
    against it. The shape has one rule that is not obvious from the two
    functions above: a comment with no tracking data at all is written as
    NULLs, *not* as `classify_traffic({})`'s `('unknown', 'unattributed')`.
    The NULLs are what let the readers' `COALESCE` fall through to the
    source-based default, so a hand-taken Instagram order with a delivery note
    stays Instagram instead of becoming "unattributed" because somebody wrote
    in its comment field.

    `parse` and `classify` default to this module's functions. The mixin
    passes its own static methods, which are these same functions, so that a
    test patching the method on the store still reaches the parse it patched.
    """
    utm_data = parse(comment)
    if not utm_data:
        return (None,) * len(UTM_VERDICT_COLUMNS)
    traffic_type, platform = classify(utm_data)
    return (
        utm_data.get('utm_source'), utm_data.get('utm_medium'),
        utm_data.get('utm_campaign'), utm_data.get('utm_content'),
        utm_data.get('utm_term'), utm_data.get('utm_lang'),
        utm_data.get('_fbp'), utm_data.get('_fbc'),
        utm_data.get('ttp'), utm_data.get('fbclid'),
        traffic_type, platform,
    )


# How /traffic names a platform, which is not always the classifier's name
# for it. The classifier files every Google order under `google`; the tab
# splits them by the verdict's traffic type — numeric-campaign CPC against
# free listings and product_sync — because the two mean different things and
# summed under one slice they say neither. Here rather than on the mixin
# because the reclassify dry run must name platforms as the tab does and may
# not load a DuckDB store to find out; one function, so the tab, the weekly
# traffic report that reads it and the dry run cannot name Google two ways.
GOOGLE_PAID_TYPES: Tuple[str, ...] = ("paid_confirmed", "paid_likely")


def tab_platform(platform: str, traffic_type: str) -> str:
    """The platform /traffic shows for one verdict, `(platform, traffic_type)`."""
    if platform == "google":
        return "google_ads" if traffic_type in GOOGLE_PAID_TYPES else "google_organic"
    return platform


# The channels /traffic splits organic into, in the order the tab lists them
# when two carry the same revenue. "Organic" is one verdict and several
# different things: an order a manager took in Instagram Direct, a visit from
# the link in the profile, a blogger's story, a welcome letter, a free Google
# listing. Measured on production 2026-10-19, 180 days of retail: Direct is
# ₴9.0M of the ₴14.1M, the profile link ₴3.1M, email ₴0.9M (automated letters
# more than two thirds of it), Telegram ₴0.7M, Google's free listings ₴0.3M.
ORGANIC_CHANNELS: Tuple[str, ...] = (
    "instagram_direct", "instagram_site", "instagram_stories", "telegram",
    "email_campaigns", "email_flows", "google_organic", "ai",
    "facebook", "tiktok", "other",
)

# Klaviyo tags a campaign `utm_medium=klaviyo` and names it
# "[MM/DD/YY] Campaign #N: …"; its flows (welcome, abandoned cart, post
# purchase) are tagged `email`. In 2025 it said `campaign` and `flow` outright.
# Rivo's loyalty letters and Shopify Email's are automations too — Shopify
# Email named a campaign no differently from a flow, so its few 2025 campaigns
# read as automated letters, and nothing in the tag can tell them apart.
_EMAIL_CAMPAIGN_RE = re.compile(r'campaign\s*#', re.IGNORECASE)


def organic_channel(
    *,
    source_id: Optional[int],
    tagged: bool,
    platform: str,
    utm_medium: Optional[str],
    utm_campaign: Optional[str],
) -> str:
    """Which `ORGANIC_CHANNELS` entry one organic order belongs to.

    `tagged` is whether the parser gave the order a verdict at all. An order
    without one is organic only through the readers' COALESCE — source 1 or 2,
    taken by a manager in Instagram or Telegram with nothing in the comment —
    and that is the whole of what "Instagram Direct" means here.

    Read at query time from the stored tags, never stored, so changing this
    rule needs no reclassify: unlike `classify_traffic`, nothing materialises
    what it returns.
    """
    if not tagged:
        return "telegram" if source_id == 2 else "instagram_direct"
    medium = (utm_medium or "").strip().lower()
    if platform == "instagram":
        return "instagram_stories" if medium.startswith("stories") else "instagram_site"
    if platform == "email":
        if medium in ("klaviyo", "campaign") or _EMAIL_CAMPAIGN_RE.search(utm_campaign or ""):
            return "email_campaigns"
        return "email_flows"
    if platform == "google":
        return "google_organic"
    if platform in ("telegram", "ai", "facebook", "tiktok"):
        return platform
    return "other"


def organic_detail(
    *,
    utm_source: Optional[str],
    utm_medium: Optional[str],
    utm_campaign: Optional[str],
    utm_content: Optional[str],
) -> Tuple[str, str, str, str]:
    """The row one organic order is listed under inside its channel:
    `(source, medium, campaign, content)`, the shape of GA4's traffic
    acquisition report — "source / medium", then the campaign beneath it.

    Source and medium are lower-cased, because they name a place and GA4's
    readers know them that way (`ig / social`, `klaviyo / email`); campaign
    and content stay as written, so the marketer finds the letter or the
    blogger's code they typed. An order with no tags at all — Direct, a
    manager's Telegram order — is `('', '', '', '')`, which the tab names
    "no UTM tags" rather than hiding the row, since that row *is* what the
    channel is made of.
    """
    return (
        (utm_source or "").strip().lower(),
        (utm_medium or "").strip().lower(),
        (utm_campaign or "").strip(),
        (utm_content or "").strip(),
    )


# The first-channel keys that are neither an organic channel nor a paid
# platform. `pixel_only`: the buyer's first order also came to the website
# with no tags. `opencart` and `exhibition` are sources with no tags of their
# own (the 2024 website, the exhibition stand), which a first order can still
# sit on because "first" counts every source — Silver's rule for a new buyer.
# `unknown`: a first order no tag explains, mostly website orders from before
# the comment template carried UTM.
FIRST_CHANNEL_OTHER: Tuple[str, ...] = (
    "pixel_only", "manager", "opencart", "exhibition", "unknown",
)


def first_order_channel(
    *,
    source_id: Optional[int],
    tagged: bool,
    platform: str,
    traffic_type: str,
    utm_medium: Optional[str],
    utm_campaign: Optional[str],
) -> Tuple[str, str]:
    """Where a buyer first came from: `(group, channel)` for their first order.

    The group is the first order's own traffic type, folded the way the tab
    folds it — `organic` (the channel is `organic_channel`'s), `paid` (the
    channel is the tab's platform, so Google Ads stays apart from free
    listings), and `manager`, `pixel_only` or `unknown`, each its own channel.
    Read at query time from stored tags, like `organic_channel`, so it needs
    no reclassify.
    """
    if traffic_type == "organic":
        return "organic", organic_channel(
            source_id=source_id, tagged=tagged, platform=platform,
            utm_medium=utm_medium, utm_campaign=utm_campaign,
        )
    if traffic_type in GOOGLE_PAID_TYPES:
        return "paid", tab_platform(platform, traffic_type)
    if traffic_type in ("manager", "pixel_only"):
        return traffic_type, traffic_type
    if source_id == 3:
        return "unknown", "opencart"
    if source_id == 5:
        return "unknown", "exhibition"
    return "unknown", "unknown"
