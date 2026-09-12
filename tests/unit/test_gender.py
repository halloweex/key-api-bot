"""What `core/gender.py` must keep doing.

Two of these tests exist because the rule they pin was got WRONG first, on real
data, and each mistake cost the same thing: a woman labelled as a man. On a base
that is ~96% female that is the only error with consequences, so they are worth
naming rather than folding into a table of cases.

  * `test_patronymic_needs_three_tokens` — a strict -ович token on a TWO-token
    row is a surname (Данилевич-shape), not a patronymic. Without the guard a
    prototype turned 239 women into men on this base.
  * `test_known_given_name_beats_surname_shape` — half the commonest female
    names here end in -ина/-іна, which is also a surname ending. Letting the
    suffix win knocked them out of the given-name role, the other token took it,
    and since Ukrainian surnames are so often bare male given names the row
    became a false male. That was 18 of 18 errors in the first version.

Names used below are ordinary given names and surnames, not customers.
"""
import pytest

from core import gender_data as D
from core.gender import RULES_VERSION, Verdict, classify, fold, tokenise


# ── the two expensive defects ────────────────────────────────────────────────

def test_patronymic_needs_three_tokens():
    """A strict patronymic suffix on a two-token row is a surname."""
    # Three tokens: a real patronymic, and it decides.
    assert classify("Шевченко Олена Іванівна").gender == "f"
    assert classify("Шевченко Андрій Іванович").gender == "m"

    # Two tokens: -ович here is the surname. The row must NOT become male on
    # that basis; the given name is what decides it.
    v = classify("Олена Данилевич")
    assert v.gender == "f", "a surname on -ович made a woman male"
    assert v.method != "patronymic"


def test_bare_ich_is_never_a_patronymic():
    """Level one of the same trap: -ич alone is a Mickiewicz-shaped surname."""
    v = classify("Марія Мицкевич")
    assert v.gender == "f"
    assert v.method != "patronymic"


def test_known_given_name_beats_surname_shape():
    """-ина/-іна is both a surname ending and a very common female name ending."""
    for name in ("Галина", "Ірина", "Христина", "Василина", "Марина"):
        v = classify(f"{name} Коваленко")
        assert v.gender == "f", f"{name} lost the given-name role to its suffix"


def test_female_given_name_wins_over_a_bare_surname():
    """Борис, Клим, Богдан, Хома are ordinary surnames as well as men's names."""
    for surname in ("Борис", "Клим", "Богдан", "Тихон", "Хома", "Кузьма"):
        v = classify(f"{surname} Надія")
        assert v.gender == "f", f"the surname {surname} was read as the given name"


# ── the asymmetry: a male verdict costs more than a female one ───────────────

def test_surname_shaped_token_never_yields_male_by_morphology():
    """Most surnames end in a consonant, and so do most male given names."""
    v = classify("Ковальчук Тарасенко")
    assert v.gender != "m", "two surnames produced a man"


def test_a_marked_surname_that_contradicts_the_given_name_refuses():
    """Two independent signals disagreeing is not a tie to be broken."""
    v = classify("Женя Петрова")   # unisex given name, unambiguously female surname
    assert v.gender in (None, "f")


def test_role_free_agreement_is_female_only():
    """Where the role is unresolved, only the safe direction may be emitted."""
    seen = set()
    for name in ("Ковальська Петрова", "Іванова Сидорова"):
        v = classify(name)
        if v.method == "role_free_agreement":
            seen.add(v.gender)
    assert seen <= {"f"}, "the agreement layer emitted a male verdict"


# ── refusal is a first-class answer ──────────────────────────────────────────

@pytest.mark.parametrize("name", ["", "   ", "​"])
def test_an_empty_name_refuses(name):
    assert classify(name).gender is None


def test_organisations_and_non_persons_refuse_with_a_reason():
    assert classify("ТОВ Ромашка").method == "organisation"
    assert classify("test test").method == "non_person"
    for name in ("ТОВ Ромашка", "test test"):
        assert classify(name).gender is None


def test_a_sole_trader_is_a_person():
    """ФОП names a human being; only a legal entity has no gender.

    A first version refused these along with companies. It cost 72 real
    customers on this base and dropped male recall on the gold set from 98.4%
    to 95.3%, because a sole trader writes their full official name — patronymic
    included — far more often than anyone else.
    """
    assert classify("ФОП Шевченко Андрій Іванович").gender == "m"
    assert classify("ФОП Коваленко Олена Петрівна").gender == "f"
    assert classify("ТОВ Ромашка").gender is None


def test_a_marker_shorter_than_the_floor_never_fires():
    """Folding collapses doubled letters, so "ПП" becomes "п"."""
    v = classify("Олена П Коваленко")
    assert v.gender == "f", "a bare initial was read as a company"


def test_a_refusal_carries_no_confidence():
    v = classify("ТОВ Ромашка")
    assert v.gender is None and v.confidence is None


def test_unisex_names_are_refused_not_guessed():
    for name in ("Саша", "Женя", "Валя"):
        assert classify(name).gender is None, f"{name} was guessed"


# ── the ordinary path ────────────────────────────────────────────────────────

@pytest.mark.parametrize("name,expected", [
    ("Олена Коваленко", "f"),
    ("Ірина Шевченко", "f"),
    ("Андрій Бондаренко", "m"),
    ("Сергій Ткаченко", "m"),
    ("Дмитро Мельник", "m"),      # -о male, the rule's main exception class
    ("Микола Бойко", "m"),        # -а male, the other one
    ("Любов Кравчук", "f"),       # female name ending in a consonant
    ("Olena Kovalenko", "f"),     # Latin transliteration
    ("Andrii Bondarenko", "m"),
])
def test_known_shapes(name, expected):
    assert classify(name).gender == expected


def test_both_naming_conventions_reach_the_same_answer():
    """77% of rows are Name Surname and 21% Surname Name Patronymic."""
    assert classify("Олена Коваленко").gender == "f"
    assert classify("Коваленко Олена").gender == "f"


def test_orthographic_variants_fold_together():
    """Ukrainian and Russian spell one name several ways; a customer types a fourth."""
    assert fold("ірина") == fold("ирина")
    assert fold("олена") != fold("елена")  # genuinely different names, both listed
    assert fold("мар'яна") == fold("маряна") == fold("марʼяна")
    assert fold("анна") == fold("ана")     # doubled letters collapse


# ── the contract with the caller ─────────────────────────────────────────────

def test_a_verdict_never_carries_the_customers_name():
    """This value is disclosable on a subject access request, and the repo is public."""
    for name in ("Олена Коваленко", "ТОВ Ромашка", "Шевченко Андрій Іванович"):
        v = classify(name)
        blob = " ".join(str(x) for x in v if x is not None).lower()
        for token in tokenise(name):
            assert token not in blob, f"{token!r} leaked into the verdict"


def test_decided_from_names_a_role_not_a_token():
    assert classify("Шевченко Андрій Іванович").decided_from == "patronymic"
    assert classify("Олена Коваленко").decided_from == "given"


def test_gender_is_only_ever_f_m_or_none():
    for name in ("Олена", "Андрій", "ТОВ", "", "Ковальчук Тарасенко", "12345"):
        assert classify(name).gender in (None, "f", "m")


def test_confidence_ladder_is_closed():
    for name in ("Шевченко Олена Іванівна", "Олена Коваленко", "Ковальська Петрова", "ТОВ"):
        assert classify(name).confidence in (None, "certain", "high", "medium")


def test_classification_is_deterministic():
    name = "Коваленко Олена Петрівна"
    assert classify(name) == classify(name)


def test_a_verdict_is_a_verdict():
    assert isinstance(classify("Олена"), Verdict)


def test_rules_version_is_a_positive_int():
    """The backfill re-derives rows stamped with an older version."""
    assert isinstance(RULES_VERSION, int) and RULES_VERSION >= 1


# ── the tables themselves ────────────────────────────────────────────────────

def test_no_name_is_declared_both_female_and_male():
    """A collision is a data error; a genuinely unisex name goes in UNISEX_NAMES.

    Every male table counts, not just MALE_NAMES: the exception lists are folded
    into the same set at import, so a diminutive that collapses onto a female
    name collides just as hard. Тьома folds to 'тома' and met Тома (Тамара)
    that way, and only the wider check caught it.
    """
    female = {fold(w.lower()) for w in D._words(D.FEMALE_NAMES)}
    female |= {fold(w.lower()) for w in D._words(D.FEMALE_LATIN)}
    male = set()
    for block in (D.MALE_NAMES, D.MALE_LATIN, D.MALE_VOWEL_ENDING, D.MALE_O_ENDING):
        male |= {fold(w.lower()) for w in D._words(block)}
    unisex = {fold(w.lower()) for w in D._words(D.UNISEX_NAMES)}
    assert (female & male) <= unisex, sorted((female & male) - unisex)


def test_invariant_surname_endings_are_not_claimed_by_the_male_rule():
    """-енко/-ук/-ів do not inflect; a male rule over them fails on most women."""
    for ending in D.SURNAME_INVARIANT:
        assert ending not in D.SURNAME_MALE_MARKED, ending
        assert ending not in D.SURNAME_FEMALE_MARKED, ending


def test_every_table_is_non_empty():
    for block in (D.FEMALE_NAMES, D.MALE_NAMES, D.FEMALE_LATIN, D.MALE_LATIN,
                  D.MALE_VOWEL_ENDING, D.MALE_O_ENDING,
                  D.FEMALE_CONSONANT_ENDING, D.UNISEX_NAMES,
                  D.ORG_MARKERS, D.NON_PERSON_MARKERS, D.SURNAME_LEXICON):
        assert D._words(block), "a name table is empty"


class TestTheDerivationRidesTheReplicationTick:
    """Derived first, shipped second, in one tick.

    A copy that ran before the derivation would carry yesterday's verdict for a
    buyer DuckDB decided an hour ago, and the daily reconciliation would then
    report a difference the ordering created rather than a real drift. This is
    `pg_gold`'s arrangement with `pg_silver` and the test that pins it.
    """

    def test_the_runner_derives_before_it_replicates(self):
        import ast
        import inspect

        from core.scheduler import BackgroundScheduler

        src = inspect.getsource(BackgroundScheduler._run_replicate_operational)
        tree = ast.parse(src.lstrip() if src.startswith(" ") else src)
        calls = [
            n.func.id
            for n in ast.walk(tree)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
        ]
        assert "derive_gender" in calls, "the tick no longer derives gender"
        assert "replicate_operational" in calls
        assert calls.index("derive_gender") < calls.index("replicate_operational")

    def test_the_cli_and_the_tick_share_one_implementation(self):
        """Two callers, one home — charter rule 1."""
        import inspect

        import scripts.backfill_gender as cli
        from core import gender_backfill

        assert cli.derive_gender is gender_backfill.derive_gender
        # The CLI must not carry its own copy of the SQL.
        assert "INSERT INTO buyer_gender" not in inspect.getsource(cli)

    def test_a_human_override_is_never_re_derived(self):
        """Even under --all. The managers.is_retail lesson."""
        from core.gender_backfill import _PENDING_ALL, _PENDING_NEW

        for sql in (_PENDING_NEW, _PENDING_ALL):
            assert "override_by_human = FALSE" in sql

    def test_the_derivation_never_raises_into_the_tick(self):
        """It shares a runner with the copy of tables nothing can rebuild."""
        import asyncio

        from core.gender_backfill import derive_gender

        class Exploding:
            def connection(self):
                raise RuntimeError("store is gone")

        result = asyncio.run(derive_gender(Exploding()))
        assert result["error"] and "RuntimeError" in result["error"]
        assert result["written"] == 0
