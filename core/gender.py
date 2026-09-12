"""Infer a customer's gender from the name KeyCRM holds, or refuse.

Pure: a string in, a verdict out. No I/O, no store, no network, no dependency
outside the standard library. The name tables live in `core/gender_data.py`.

WHAT THE VERDICT IS

`gender` is "f", "m" or **None**, and None is a first-class answer rather than a
failure. About 0.7% of the base cannot be decided — a company, a marketplace
handle, a pasted phone number, a name whose two tokens are both unknown — and a
guess there is an error nobody can detect afterwards. The caller writes NULL.

Every verdict carries `method` and `confidence` so a consumer can be stricter
than this module: a campaign that spends money may use `certain`/`high` only,
while a dashboard tile can use everything. That is why the column is not a
boolean.

THE ORDER OF THE LAYERS IS THE DESIGN

  0. human override            certain   — handled by the caller, never here
  1. patronymic (3+ tokens)    certain   — deterministic in both languages
  2. given-name dictionary     high      — ~1 400 folded entries
  3. given-name morphology     high      — "-а/-я → female" plus exceptions
  4. surname gender marking    medium    — only where the surname is marked
  5. refuse                    None

Cheapest and most certain first. Measured on the whole base: layers 1-2 alone
decide ~93%, and every false male this module can produce comes from layer 3.

THE ASYMMETRY, WHICH IS THE WHOLE REASON THIS FILE IS NOT TEN LINES

The base is ~96% female, so "everyone is female" scores 96% accuracy and is
worthless. The only thing with value is finding the ~4% of men, and the only
error that matters is a woman labelled male. Two facts conspire to produce
exactly that error: Slavic surnames overwhelmingly end in a consonant, and so
do male given names. So whenever the resolver picks the wrong token as the
given name, the row becomes a false male.

Three defences, all of them measured rather than assumed:

  * **Role resolution never trusts position.** 77% of rows are "Name Surname"
    and 21% are "Surname Name Patronymic", so position decides nothing. The
    resolver uses surname SHAPE as negative evidence — a token ending -енко/-ук/
    -ський is a surname and is never offered to the gender rules.
  * **A male verdict from morphology alone is gated.** Layer 3 may return "m"
    only when the token is not surname-shaped. Refusing costs coverage; not
    refusing cost 4.24 pp of male precision when it was measured both ways.
  * **The surname must not contradict the given name.** Where the surname is
    gender-marked (-ова vs -ов) and disagrees with layer 3's verdict, the row is
    refused. Two independent signals disagreeing is not a tie to be broken.

THE PATRONYMIC TRAP HAS TWO LEVELS, AND THE SECOND IS THE EXPENSIVE ONE

Level one is documented everywhere: a bare `-ич` is a surname (Мицкевич-shape),
and treating it as a patronymic mislabels ~350 women as men on this base.
Level two is not, and it cost a prototype 239 false males: **a strict `-ович`
token on a TWO-token row is also a surname** (Данилевич-shape). A patronymic
only exists beside a given name and a surname, so the rule requires three
tokens. Removing that guard is the single most expensive one-line change
available in this file.

PRIVACY

Nothing here records a customer's name. `decided_from` names the ROLE the
verdict came from ("given", "patronymic", "surname"), never the token itself:
this value is disclosable on a subject access request, and this repository is
public and has been scrubbed of customer data once already.
"""
from __future__ import annotations

import re
import unicodedata
from typing import Iterable, NamedTuple, Optional

from core import gender_data as D

__all__ = ["Verdict", "classify", "fold", "tokenise", "RULES_VERSION"]

# Bumped whenever a table or a rule changes in a way that would move a stored
# verdict. The backfill re-derives every row whose stored version is behind, so
# forgetting to bump this is how a fix fails to reach the database.
RULES_VERSION = 1


class Verdict(NamedTuple):
    """One decision about one name."""
    gender: Optional[str]       # "f" | "m" | None
    method: str                 # which layer decided
    confidence: Optional[str]   # "certain" | "high" | "medium" | None
    decided_from: Optional[str] # role only, never the token


# ── normalisation ────────────────────────────────────────────────────────────
_ZERO_WIDTH = dict.fromkeys(map(ord, "​‌‍﻿­"), None)
_PUNCT = ".,;:!?()[]{}\"'`«»_/\\|+*#№@<>=~^$%&"
_CYRILLIC = re.compile(r"[Ѐ-ӿԀ-ԯ]")
_LATIN = re.compile(r"[A-Za-z]")
_DIGIT = re.compile(r"\d")

_FOLD = str.maketrans(D.FOLD_MAP)
_HOMOGLYPH = str.maketrans(D.HOMOGLYPH_MAP)


def fold(token: str) -> str:
    """Collapse the orthographic axis so one entry serves every spelling.

    Ірина/Ирина, Олена/Елена and Мар'яна/Марьяна all fold together. Doubled
    letters collapse too (Анна → Ана, Алла → Ала), which is what lets one entry
    match both the Ukrainian spelling and a customer's transliteration.
    """
    t = token.translate(_FOLD)
    out = []
    for ch in t:
        if out and out[-1] == ch:
            continue
        out.append(ch)
    return "".join(out)


def _norm(name: str) -> str:
    s = unicodedata.normalize("NFC", name or "")
    s = s.translate(_ZERO_WIDTH).replace(" ", " ")
    return re.sub(r"\s+", " ", s).strip()


def _clean(token: str) -> str:
    return token.strip(_PUNCT).strip("-–—").lower()


def tokenise(name: str) -> list:
    """The name as cleaned lowercase tokens. Hyphenated names stay one token."""
    return [c for c in (_clean(t) for t in _norm(name).split(" ")) if c]


def _is_latin(token: str) -> bool:
    return bool(_LATIN.search(token)) and not _CYRILLIC.search(token)


def _cyrillicise(token: str) -> str:
    """A Latin-typed Cyrillic name, mapped so the morphology rules can see it."""
    return token.translate(_HOMOGLYPH)


# ── tables, folded once at import ────────────────────────────────────────────
def _fold_all(block: str) -> frozenset:
    return frozenset(fold(w.lower()) for w in D._words(block))


_FEMALE = _fold_all(D.FEMALE_NAMES) | _fold_all(D.FEMALE_LATIN)
_MALE = _fold_all(D.MALE_NAMES) | _fold_all(D.MALE_LATIN)
_MALE_VOWEL = _fold_all(D.MALE_VOWEL_ENDING)
_MALE_O = _fold_all(D.MALE_O_ENDING)
_FEMALE_CONS = _fold_all(D.FEMALE_CONSONANT_ENDING)
_UNISEX = _fold_all(D.UNISEX_NAMES)
_ORG = frozenset(k for k in _fold_all(D.ORG_MARKERS) if len(k) >= D.MIN_MARKER_LEN)
_NON_PERSON = frozenset(
    k for k in _fold_all(D.NON_PERSON_MARKERS) if len(k) >= D.MIN_MARKER_LEN
)
# A sole trader is a person, so this prefix is STRIPPED and the name behind it
# is classified. It is not an organisation marker and must never refuse a row.
_SOLE_TRADER = _fold_all(D.SOLE_TRADER_MARKERS)
_SURNAMES = _fold_all(D.SURNAME_LEXICON)

# A name claimed by both lists is a data error, not a unisex name: unisex names
# are declared explicitly. Resolve it as unisex so it is refused rather than
# decided by whichever set was tested first.
_AMBIGUOUS = (_FEMALE & _MALE) | _UNISEX
_FEMALE = _FEMALE - _AMBIGUOUS
_MALE = (_MALE | _MALE_VOWEL | _MALE_O) - _AMBIGUOUS

_VOWEL_FEMALE_ENDINGS = ("а", "я")
_AMBIGUOUS_ENDINGS = ("о", "е", "є", "і", "и", "у", "ю", "ь")
_CONSONANTS = set("бвгґджзйклмнпрстфхцчшщ")


# ── layer 1: the patronymic ──────────────────────────────────────────────────
# Strict suffixes only. A bare -ич is a surname and must never appear here.
_PATRONYMIC_F = re.compile(r"(івна|ївна|овна|евна|євна|ична|ивна)$")
_PATRONYMIC_M = re.compile(r"(ович|йович|ьович|евич|євич)$")
_PATRONYMIC_F_LAT = re.compile(r"(ivna|yivna|ovna|evna|ichna)$")
_PATRONYMIC_M_LAT = re.compile(r"(ovych|ovich|yovych|evych|evich)$")


def _patronymic_gender(token: str) -> Optional[str]:
    if len(token) < 5:
        return None
    if _is_latin(token):
        if _PATRONYMIC_F_LAT.search(token):
            return "f"
        if _PATRONYMIC_M_LAT.search(token):
            return "m"
        return None
    if _PATRONYMIC_F.search(token):
        return "f"
    if _PATRONYMIC_M.search(token):
        return "m"
    return None


# ── surname evidence ─────────────────────────────────────────────────────────
def _looks_like_surname(token: str) -> bool:
    """Negative evidence about the given name, not a gender signal.

    A token this returns True for is never offered to the gender rules, which is
    what keeps a consonant-ending surname from being read as a male given name.

    A KNOWN GIVEN NAME IS NEVER SURNAME-SHAPED, whatever its ending. Half the
    commonest female names in this base end in -ина/-іна — Галина, Ірина,
    Христина, Василина, Марина — which is also a surname shape, and letting the
    suffix win knocked every one of them out of the race for the given-name
    role. The other token then took it, and because Ukrainian surnames are so
    often bare male given names, the row became a false male. That was 18 of the
    18 errors the first version made on the gold set.
    """
    key = fold(token)
    if key in _FEMALE or key in _MALE or key in _AMBIGUOUS:
        return False
    if key in _SURNAMES:
        return True
    if _is_latin(token):
        return token.endswith(D.SURNAME_SHAPES_LATIN)
    return token.endswith(D.SURNAME_SHAPES)


def _surname_gender(token: str) -> Optional[str]:
    """Gender from a MARKED surname form, or None.

    The male side is an allow-list. Ukrainian surnames on -енко/-ук/-ів/-ак are
    invariant, and a rule that read their consonant ending as male would call
    every such woman a man — which is most of the base.
    """
    if len(token) < 4:
        return None
    if token.endswith(D.SURNAME_INVARIANT):
        return None
    if token.endswith(D.SURNAME_FEMALE_MARKED):
        return "f"
    if token.endswith(D.SURNAME_MALE_MARKED):
        return "m"
    return None


# ── layer 3: morphology of the given name ────────────────────────────────────
def _given_morphology(token: str) -> tuple:
    """(gender, rule) from the shape of a given name, or (None, reason)."""
    key = fold(token)
    if key in _AMBIGUOUS:
        return None, "unisex"
    if key in _MALE_VOWEL:
        return "m", "exception_male_vowel"
    if key in _MALE_O:
        return "m", "exception_male_o"
    if key in _FEMALE_CONS:
        return "f", "exception_female_consonant"
    if not token:
        return None, "empty"
    last = token[-1]
    if last in _VOWEL_FEMALE_ENDINGS:
        return "f", "ending_vowel_a_ya"
    if last in _AMBIGUOUS_ENDINGS:
        return None, "ending_ambiguous"
    if last in _CONSONANTS:
        return "m", "ending_consonant"
    return None, "ending_unrecognised"


# ── role resolution ──────────────────────────────────────────────────────────
class _Roles(NamedTuple):
    given: Optional[str]
    surname: Optional[str]
    patronymic_gender: Optional[str]
    flags: tuple
    given_is_certain: bool   # the given token was recognised, not guessed


def _resolve_roles(tokens: list) -> _Roles:
    """Decide which token is the given name. Position is never consulted."""
    flags = []
    working = list(tokens)

    # 1. The patronymic, and ONLY on a row of three or more tokens. On a
    #    two-token row an -ович token is a surname, and reading it as a
    #    patronymic is what turns ~239 women into men.
    patronymic = None
    if len(working) >= 3:
        for i in range(len(working) - 1, -1, -1):
            g = _patronymic_gender(working[i])
            if g:
                patronymic = g
                working.pop(i)
                flags.append("patronymic_found")
                break

    # 2. Tokens that are not names at all.
    kept = []
    for t in working:
        key = fold(t)
        if key in _SOLE_TRADER:
            flags.append("sole_trader")
            continue
        if key in _ORG:
            flags.append("organisation")
            continue
        if key in _NON_PERSON:
            flags.append("non_person")
            continue
        if _DIGIT.search(t):
            flags.append("contains_digits")
            continue
        kept.append(t)
    working = kept

    if not working:
        return _Roles(None, None, patronymic, tuple(flags), False)

    # 3. The given name. Recognition first, shape second, and a guess last —
    #    and a guess is recorded as such so the gender rules can be stricter.
    def _known(t):
        k = fold(t)
        if k in _FEMALE or k in _MALE or k in _AMBIGUOUS:
            return True
        if _is_latin(t):
            k2 = fold(_cyrillicise(t))
            return k2 in _FEMALE or k2 in _MALE or k2 in _AMBIGUOUS
        return False

    known = [t for t in working if _known(t)]
    given = surname = None
    certain = False

    if len(known) == 1:
        given, certain = known[0], True
    elif len(known) > 1:
        # Two recognised names. The commonest shape by far is a female given
        # name beside a surname that is itself a bare male given name — Борис,
        # Клим, Богдан, Тихон, Хома, Кузьма, Матвій are all ordinary Ukrainian
        # surnames as well as men's names, while the reverse (a woman's name
        # serving as a surname) barely occurs. With a base that is ~96% female,
        # both the morphology and the prior point the same way, so the female
        # token takes the given-name role.
        genders = {t: _lookup(t) for t in known}
        females = [t for t, g in genders.items() if g == "f"]
        males = [t for t, g in genders.items() if g == "m"]
        if len(females) == 1 and males:
            given, certain = females[0], True
            flags.append("female_over_bare_surname")
        else:
            non_surname = [t for t in known if not _looks_like_surname(t)]
            if len(non_surname) == 1:
                given, certain = non_surname[0], True
            else:
                given, certain = known[0], False
                flags.append("multiple_known_names")
    else:
        # Nothing recognised. Surname shape is the only evidence left, and it is
        # negative: whatever does NOT look like a surname is the given name.
        candidates = [t for t in working if not _looks_like_surname(t)]
        if len(candidates) == 1:
            given, certain = candidates[0], False
            flags.append("given_by_elimination")
        elif len(working) == 1:
            given, certain = working[0], False
            flags.append("single_token")
        else:
            flags.append("role_unresolved")

    if given is not None:
        surname = next((t for t in working if t != given), None)
    return _Roles(given, surname, patronymic, tuple(flags), certain)


# ── the cascade ──────────────────────────────────────────────────────────────
def _lookup(token: str) -> Optional[str]:
    """The dictionary verdict for a token, trying its Latin rendering too."""
    key = fold(token)
    if key in _AMBIGUOUS:
        return None
    if key in _FEMALE:
        return "f"
    if key in _MALE:
        return "m"
    if _is_latin(token):
        key = fold(_cyrillicise(token))
        if key in _AMBIGUOUS:
            return None
        if key in _FEMALE:
            return "f"
        if key in _MALE:
            return "m"
    return None


def classify(full_name: str) -> Verdict:
    """Infer gender from a full name, or refuse with gender=None."""
    tokens = tokenise(full_name)
    if not tokens:
        return Verdict(None, "empty_name", None, None)

    roles = _resolve_roles(tokens)

    # A company or a non-person row is refused BEFORE any name rule runs, and
    # the order matters. "ТОВ Ромашка" drops its org marker and leaves a token
    # ending in -а, which the morphology rule would happily call a woman — so
    # the marker has to be terminal, not merely noted. What is left after the
    # marker is a company's name, not a person's.
    if "organisation" in roles.flags:
        return Verdict(None, "organisation", None, None)
    if "non_person" in roles.flags:
        return Verdict(None, "non_person", None, None)

    # L1 — the patronymic. Deterministic, and the only layer worth calling
    # certain: it is the rule the whole classifier is measured against.
    if roles.patronymic_gender:
        return Verdict(roles.patronymic_gender, "patronymic", "certain", "patronymic")

    given = roles.given
    if given is not None:
        # L2 — the dictionary.
        hit = _lookup(given)
        if hit:
            method = "dictionary_latin" if _is_latin(given) else "dictionary"
            return Verdict(hit, method, "high", "given")

        # L3 — morphology, with the male side gated.
        probe = given if _CYRILLIC.search(given) else _cyrillicise(given)
        sex, rule = _given_morphology(probe)

        if sex == "m":
            # A consonant ending is the shape of most surnames as well as most
            # male names, so a male verdict is only allowed when the token is
            # not surname-shaped. Measured: refusing here costs 0.54 pp of
            # coverage and buys 4.24 pp of male precision.
            if _looks_like_surname(given) or not _surname_agrees(roles, "m"):
                sex = None
        elif sex == "f" and rule == "ending_vowel_a_ya":
            # The female side is far safer, but a marked male surname beside it
            # is still a contradiction worth refusing rather than averaging.
            if not _surname_agrees(roles, "f"):
                sex = None

        if sex:
            confidence = "high" if roles.given_is_certain else "medium"
            return Verdict(sex, "given_morphology", confidence, "given")

    # L3b — the role is unresolved, but the answer may not depend on it.
    # When every token that carries a gender signal gives the SAME answer, which
    # token is the given name stops mattering: "Ковальська Петрова" is a woman
    # either way. Only a disagreement needs a role, and a disagreement is
    # refused rather than broken by a guess.
    if given is None and "role_unresolved" in roles.flags:
        votes = set()
        for t in tokens:
            probe = t if _CYRILLIC.search(t) else _cyrillicise(t)
            sex, _ = _given_morphology(probe)
            if sex is None:
                sex = _surname_gender(probe)
            if sex:
                votes.add(sex)
        if votes == {"f"}:
            # Female only, and the asymmetry is the whole reason. Measured both
            # ways on the gold set: emitting male here too buys 0.84 pp of
            # coverage and costs 3.0 pp of male precision (2 false males become
            # 6). On a base that is ~96% female, a wrong female is absorbed by
            # the prior; a wrong male is the error the column exists to avoid.
            return Verdict("f", "role_free_agreement", "medium", "agreement")

    # L4 — the surname, only where it is a marked form.
    if roles.surname:
        s = roles.surname if _CYRILLIC.search(roles.surname) else _cyrillicise(roles.surname)
        sex = _surname_gender(s)
        if sex:
            return Verdict(sex, "surname_marking", "medium", "surname")

    # L5 — refuse, and say which kind of refusal it is. Organisation and
    # non-person already returned above, before any name rule could run.
    if "role_unresolved" in roles.flags or "multiple_known_names" in roles.flags:
        return Verdict(None, "role_unresolved", None, None)
    return Verdict(None, "unknown", None, None)


def _surname_agrees(roles: _Roles, gender: str) -> bool:
    """False only when the surname is MARKED and marked the other way.

    An unmarked surname — every -енко, -ук, -ів in the base — agrees with
    everything, which is the correct answer for a form that does not inflect.
    """
    if not roles.surname:
        return True
    s = roles.surname if _CYRILLIC.search(roles.surname) else _cyrillicise(roles.surname)
    marked = _surname_gender(s)
    return marked is None or marked == gender


def classify_many(rows: Iterable) -> list:
    """[(buyer_id, full_name)] -> [(buyer_id, Verdict)], in the order given."""
    return [(buyer_id, classify(name)) for buyer_id, name in rows]
