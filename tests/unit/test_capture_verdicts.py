"""The channel-verdict log's pure half (revision 0036, `core/pg_utm_parse.py`).

Everything a server decides — the diff, the self-check, what commits, where
the capture runs, the kinds and the row hash — is in
`tests/integration/test_capture_verdicts_pg.py`. What is here:

- `utm_classify.RULE_VERSION` moves with the golden: each version is pinned to
  a hash of the frozen tuples it was written against;
- which stored verdicts today's rules would not make.
"""
from __future__ import annotations

import hashlib
import json
import re


from core import pg_utm_parse as parse
from core import utm_classify
from tests.unit import test_utm_classify_golden as golden

# ─── The rules' version, pinned to the golden ─────────────────────────────────
#
# Append-only: one entry per version, never edited. A rule change edits a frozen
# tuple in `test_utm_classify_golden.py` (OD-06 makes that the place it is
# argued), and then `test_the_version_names_the_golden` fails until
# `RULE_VERSION` names a new version and its hash is added here. Editing an
# existing entry instead would let two sets of rules write one `rule_version`
# into `app.order_channel_verdicts`, which is the thing the column exists to
# prevent. A fixture added for a rule that did not change moves the hash too;
# that is the one case for replacing the current entry, and its commit says so.
GOLDEN_SHA256 = {
    "r1": "59ed4e1c8b7ea919bea114f1b9ac382fc98c07b5081328486a56809ab280a843",
    # The Meta cookies' labels `fbp:`/`fbc:` (PR #380): PARSE_GOLDEN_FB_LABELS.
    "r2": "a1963a8b311dcc951791a4b86db055b049fcd092d3ec7f93e69a4a7b97b36f28",
}

# The keys the two lists r1 was hashed over had then, kept so that a golden
# holding only those two lists still hashes to r1's entry.
_GOLDEN_KEYS = {"PARSE_GOLDEN": "parse", "CLASSIFY_GOLDEN": "classify"}


def _frozen_verdicts() -> dict:
    """Every list of frozen verdicts in the golden module: each name that
    starts with `PARSE_GOLDEN` or `CLASSIFY_GOLDEN`.

    Walked, not named. r2 froze its answers in a list of its own,
    `PARSE_GOLDEN_FB_LABELS`, because `PARSE_GOLDEN` is replayed by position
    against DuckDB's recorded rows; a hash over the two lists named here
    before did not move for it, and would have let r2's rules log as r1."""
    return {
        _GOLDEN_KEYS.get(name, name.lower()): value
        for name, value in vars(golden).items()
        if name.startswith(("PARSE_GOLDEN", "CLASSIFY_GOLDEN"))
    }


def _golden_sha256() -> str:
    """The frozen verdicts — every (comment, parse, row) and (tags, verdict) —
    as canonical JSON: keys sorted, so a dict written in another order is the
    same golden."""
    text = json.dumps(_frozen_verdicts(),
                      ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class TestTheRuleVersion:
    def test_the_version_names_the_golden(self):
        current = _golden_sha256()
        assert GOLDEN_SHA256.get(utm_classify.RULE_VERSION) == current, (
            "the golden's verdicts moved: a rule change names a new "
            "utm_classify.RULE_VERSION and adds "
            f"{{'r<n+1>': '{current}'}} to GOLDEN_SHA256 in the same commit")

    def test_the_current_version_is_the_newest(self):
        """Kills: a rule change that went back to an old name, so that rows of
        two rule sets read as one."""
        assert all(re.fullmatch(r"r[1-9][0-9]*", v) for v in GOLDEN_SHA256)
        newest = max(GOLDEN_SHA256, key=lambda v: int(v[1:]))
        assert utm_classify.RULE_VERSION == newest

    def test_no_two_versions_share_a_golden(self):
        """Kills: a new name for rules that did not change, which splits one
        rule set's verdicts across two `rule_version`s."""
        assert len(set(GOLDEN_SHA256.values())) == len(GOLDEN_SHA256)

    def test_the_hash_reads_every_frozen_list(self):
        """Kills: a walk that finds nothing, or misses a list a rule change
        froze its answers in, so the version stays where it was."""
        assert {"parse", "classify", "parse_golden_fb_labels"} <= set(_frozen_verdicts())
        assert all(_frozen_verdicts().values())


class TestTheEarlierRules:
    """A `first` verdict the current rules do not make of its comment was
    decided under earlier ones (the seed's 3 769 of the 2026-08-31 dry run),
    and says `pre-r1`. Labelled `r1`, a reclassify's move of them would read
    as the next version's drift (the design's C6)."""

    def _record(self, oid, comment, verdict):
        return {"order_id": oid, "manager_comment": comment,
                **dict(zip(parse.VERDICT_COLUMNS, verdict))}

    def _today(self, comment):
        row = dict(zip(parse.UTM_COLUMNS[1:-1], parse.utm_columns(comment)))
        return tuple(row[c] for c in parse.VERDICT_COLUMNS)

    def test_what_the_rules_still_make_is_theirs_and_the_rest_is_earlier(self):
        pixel = "UTM: _fbp: fb.1.123.456"
        paid = "UTM: utm_source: fbads; utm_medium: cpc; utm_campaign: spring"
        old = ("pixel_only", "facebook", None, None, None)
        assert self._today(pixel) != old, "the r1 rule this case is about"
        records = [self._record(1, paid, self._today(paid)),
                   self._record(2, pixel, old),
                   self._record(3, pixel, self._today(pixel))]
        assert parse.not_these_rules(records) == [2]
