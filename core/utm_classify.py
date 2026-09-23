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
