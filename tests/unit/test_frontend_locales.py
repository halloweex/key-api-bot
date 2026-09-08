"""The frontend's three dictionaries, against the vocabularies in code.

`access.preset.marketer` reached a production screen as a raw key: the preset
was added to `ACCESS_PRESETS` and its label to nobody. Nothing could have
caught it — the backend's i18n test covers `core/i18n.py`, and the key is
built by interpolation (``t(`access.preset.${key}`)``), so no scan for string
literals would have found it either.

What is checkable is the shape that produced it: a **vocabulary in Python**
rendered through an interpolated key. Every member of one needs a label in all
three languages, and this asserts exactly that rather than pretending to check
every key the frontend uses.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from core.permissions import ACCESS_PRESETS, Role, TAB_FEATURE_KEYS

LOCALES = Path(__file__).resolve().parents[2] / "web" / "frontend" / "src" / "locales"
LANGUAGES = ("en", "uk", "ru")


def _load(language: str) -> dict:
    return json.loads((LOCALES / f"{language}.json").read_text(encoding="utf-8"))


DICTIONARIES = {language: _load(language) for language in LANGUAGES}


def _keys_for(prefix: str, vocabulary) -> list[str]:
    return [f"{prefix}{item}" for item in vocabulary]


VOCABULARIES = {
    # `t(`access.tab.${tab}`)` in the checklist, the summary and the row.
    "access.tab.": list(TAB_FEATURE_KEYS),
    # `t(`access.preset.${preset.key}`)` on the preset buttons. This is the one
    # that shipped broken.
    "access.preset.": sorted(ACCESS_PRESETS),
    # `t(`profile.${user.role}`)` names the level beside the chips.
    "profile.": [role.value for role in Role],
}


@pytest.mark.parametrize("language", LANGUAGES)
@pytest.mark.parametrize("prefix,vocabulary", sorted(VOCABULARIES.items()))
def test_every_member_has_a_label(language, prefix, vocabulary):
    missing = [
        key for key in _keys_for(prefix, vocabulary)
        if key not in DICTIONARIES[language]
    ]
    assert not missing, (
        f"{language}.json is missing {missing} — these are built by "
        f"interpolation, so the key itself is what a user sees on screen"
    )


def test_the_three_dictionaries_carry_the_same_keys():
    """A key in one language and not another renders as the key for whoever
    chose the other one — the same failure, one language at a time."""
    reference = set(DICTIONARIES["en"])
    for language in ("uk", "ru"):
        against = set(DICTIONARIES[language])
        assert reference == against, {
            f"only in en": sorted(reference - against),
            f"only in {language}": sorted(against - reference),
        }


def test_no_label_is_left_as_its_own_key():
    """A placeholder like `"access.preset.marketer": "access.preset.marketer"`
    passes the check above and still shows a key on screen."""
    for language, dictionary in DICTIONARIES.items():
        echoes = [k for k, v in dictionary.items() if k == v]
        assert not echoes, f"{language}.json: {echoes}"
