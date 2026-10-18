"""Why an SMS campaign was refused, named so the page can say it in its reader's language.

Every refusal on the campaign routes answers with an English `detail`, which
is what an API caller reads and what the log keeps. The page used to show that
sentence as it was, so the owner — who works in Ukrainian and Russian — was
told in English that the balance was short or that the people had already
been messaged today. So each refusal also carries `reason`, one of the names
below, and the figures behind it; the page turns `reason` into
`sms.refusal.<reason>` and fills in the figures. `detail` stays exactly what
it was, for everybody else.

The list is the vocabulary, not a description: `tests/unit/test_frontend_locales.py`
requires a sentence for every name in all three languages, and
`tests/unit/test_sms_refusal_reasons.py` requires every reason the routes and
the store raise to be one of them. A reason the page has no sentence for is
shown as `detail`, which is the right failure — never a raw key.
"""
from __future__ import annotations

from typing import Any, Dict

# name → what it means, for the reader of this file. Snake case, because these
# travel as JSON values beside `detail` and are matched by the page verbatim.
REFUSAL_REASONS: Dict[str, str] = {
    # The send — POST /customers/sms-campaigns/{campaign}/send
    "confirm_mismatch": "the name typed back is missing or is not the campaign's",
    "not_frozen": "no campaign of that name has been frozen",
    "already_sent": "the campaign was sent already; a second send messages everybody twice",
    "no_targets": "the frozen roster has nobody to send to",
    "recent_campaign": "the roster overlaps a campaign sent in the last day",
    "balance_unreadable": "the gateway's balance could not be read",
    "balance_short": "the gateway's balance does not cover the send",
    "gateway_refused": "the gateway refused the request; nothing left, the claim is released",
    "gateway_outcome_unknown": "the gateway's answer is unknown; the claim is kept",
    # The freeze — POST /customers/sms-campaigns
    "name_placeholder": "the campaign is named 'default', the preview's placeholder",
    "roster_truncated": "the audience is larger than the limit and would freeze truncated",
    "audience_empty": "the audience is empty; there is nothing to freeze",
    "already_frozen": "a campaign of that name is frozen already",
    "roster_sent": "the campaign was sent; its roster is the control group and stays",
    "claimed_while_refreezing": "the campaign was claimed for sending while being re-frozen",
    # A follow-up — the preview and the freeze with follow_up_of
    "follow_up_narrowed": "audience parameters were passed beside follow_up_of",
    "follow_up_holdout": "a follow-up was given a control group of its own",
    "follow_up_self": "a campaign was named as its own follow-up",
    "follow_up_root_unsent": "the campaign followed up has not been sent",
    "follow_up_of_follow_up": "the campaign followed up is itself a follow-up",
    "follow_up_sales_type": "the follow-up asks for another sales type than its root's",
}


class SmsCampaignRefused(ValueError):
    """A refusal about a campaign's state, with the name the page translates.

    A ValueError still, so every caller that caught one keeps catching it and
    `str(e)` is the sentence it always was; a route reads `reason` and
    `figures` to answer with them beside `detail`.
    """

    def __init__(self, message: str, reason: str, **figures: Any):
        if reason not in REFUSAL_REASONS:
            raise KeyError(f"unknown SMS refusal reason {reason!r}")
        super().__init__(message)
        self.reason = reason
        self.figures = figures
