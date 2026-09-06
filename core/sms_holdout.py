"""Which arm of a campaign a customer lands in.

One definition, in Python, because this is the one piece of the audience that
**must not** be computed by a database.

It used to be `hash(buyer_id::VARCHAR || '|' || campaign) % 100 < holdout_pct`
inside the DuckDB query. That has two problems, and only the second was
noticed while the store was single-engine:

1. `hash()` is DuckDB's own function. Its algorithm is not part of any
   documented contract, so a DuckDB upgrade may legitimately change it — and
   the withheld set is promised to be *stable across reruns of the same
   campaign*. A roster previewed before an upgrade and frozen after it would
   quietly measure a different control group than the one the operator saw.
2. PostgreSQL has no equivalent and cannot be given one. Any engine-side
   rendering would assign differently, so the same audience previewed in one
   store and frozen in the other would disagree about who was withheld — and a
   campaign whose control group is not the one it was measured against has no
   measurable lift at all, which is the entire point of holding anyone back.

So the split is computed here, from the same three values it always used, and
both engines return the same answer by construction rather than by agreement.

**This changes which people are withheld from future campaigns, once.** It
cannot change a past one: `freeze_sms_campaign` records the arm per member, so
every campaign already sent carries its own roster. And it does not change how
*many* are withheld, or the property the split exists for — the draw is still
deterministic per (customer, campaign), still re-drawn when the campaign name
changes so the same people are not always the ones withheld, and still uniform
across the base.
"""
from __future__ import annotations

import hashlib

TARGET = "target"
HOLDOUT = "holdout"

# The string that is hashed, kept identical to the expression this replaced so
# the shape of the draw is recognisably the same one.
_SEED = "{buyer_id}|{campaign}"


def holdout_bucket(buyer_id: int, campaign: str) -> int:
    """A stable 0–99 bucket for one customer in one campaign.

    SHA-1 rather than Python's `hash()`, which is salted per process: the same
    customer would land in a different arm on every restart, so a preview and
    the freeze that follows it could disagree within one sitting. Only the
    first eight bytes are used — the modulus is 100, and the bias from folding
    2^64 into 100 buckets is about one part in 10^17.
    """
    seed = _SEED.format(buyer_id=buyer_id, campaign=campaign)
    digest = hashlib.sha1(seed.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % 100


def assign_arm(buyer_id: int, campaign: str, holdout_pct: int) -> str:
    """`holdout` for the withheld share of a campaign, `target` for the rest.

    `holdout_pct` of 0 disables the control group, and every customer is a
    target — the explicit branch is here because "0 % withheld" is a real
    choice on the page, not a degenerate input.
    """
    if holdout_pct <= 0:
        return TARGET
    return HOLDOUT if holdout_bucket(buyer_id, campaign) < holdout_pct else TARGET
