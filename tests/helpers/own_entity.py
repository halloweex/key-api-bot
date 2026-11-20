"""Names that read like the company's own legal entity, and names that only
look so.
"""
from __future__ import annotations


# Spellings a buyer card is written with — the legal form, quotes of either
# kind, case, a dash, Russian renderings, and Latin letters typed where
# Cyrillic ones look the same (and the converse). The integration test runs
# the same lists through Postgres' `translate()` and `~`.
LOOKS_LIKE_THE_ENTITY = (
    "Коріан Сторі", "ТОВ «Коріан Сторі»", 'ТОВ "КОРІАН СТОРІ"', "тов коріан сторі",
    "ТзОВ «Коріан-Сторі»", "КОРІАН СТОРІ ТОВ", "Корiан Сторi", "КОРIАН СТОРI",
    "Кориан Стори", "Кореан Стори", "ООО «Кориан Стори»", "Коріан\u00a0Сторі",
    "Korean Story", "KOREAN STORY", "KoreanStory", "korean-story", "Korean_Story LLC",
    "TOV Korean Story", "Korian Story", "Korean Stori", "Kоrean Story",
    # Characters a pasted name carries unseen: a thin space, a narrow no-break
    # space, a soft hyphen, a zero-width space, a byte-order mark, an
    # ideographic space.
    "Коріан\u2009Сторі", "ТОВ\u202f«Коріан\u00adСторі»",
    "Korean\u200bStory", "\ufeffKorean\u3000Story",
)


NOT_THE_ENTITY = (
    "", "Korean Shop", "Story Korean", "Korea Story", "Корея", "Історія", "Сторінка",
    "Коріандр", "Korean", "Story", "Test Buyer", "Korean Beauty Story", "Kor Story",
)
