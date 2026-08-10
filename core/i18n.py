"""Three languages, one string table.

Small enough not to need gettext: no extraction step, no .po files to keep in
sync with the code, no compile stage in the Docker build. A dict of keys to
translations is inspectable, diffable, and testable — and a test can assert
that every key carries every language, which is the one guarantee a translation
system actually owes you.

A missing key or a missing language falls back to English rather than raising.
A report that arrives with one English line in it is a blemish; a report that
does not arrive because a translator forgot a key is a failure.

**Flags are deliberately absent from the language picker.** This is a Ukrainian
company, and putting a Russian flag in its internal tooling is not a neutral
act. Languages have names; the names are enough.
"""
from __future__ import annotations

from typing import Dict, Optional, Tuple

EN = "en"
UK = "uk"
RU = "ru"

LANGUAGES: Tuple[str, ...] = (EN, UK, RU)
DEFAULT_LANGUAGE = EN

LANGUAGE_NAMES: Dict[str, str] = {
    EN: "English",
    UK: "Українська",
    RU: "Русский",
}

# English groups thousands with a comma; Ukrainian and Russian use a space.
# Non-breaking, so "₴ 968 638" never wraps mid-number in a Telegram bubble.
_GROUP_SEPARATOR: Dict[str, str] = {EN: ",", UK: " ", RU: " "}


def normalize(language: Optional[str]) -> str:
    """The nearest supported language, or English.

    Accepts what Telegram hands over in `language_code` too — "uk-UA", "ru",
    "en-GB" — so a first-time user can be met in their own language before
    they have set anything.
    """
    if not language:
        return DEFAULT_LANGUAGE
    code = language.strip().lower().replace("_", "-").split("-")[0]
    return code if code in LANGUAGES else DEFAULT_LANGUAGE


def t(key: str, language: str = DEFAULT_LANGUAGE, **fmt) -> str:
    """The translation for `key`, formatted with `fmt`.

    Never raises on a missing key, a missing language, or a placeholder the
    translation does not use — the worst case is an untranslated or unformatted
    line, which still carries the number it was there to carry.
    """
    entry = _STRINGS.get(key)
    if entry is None:
        return key
    text = entry.get(normalize(language)) or entry.get(DEFAULT_LANGUAGE) or key
    if not fmt:
        return text
    try:
        return text.format(**fmt)
    except (KeyError, IndexError, ValueError):
        return text


def fmt_int(value: float, language: str = DEFAULT_LANGUAGE) -> str:
    """A whole number with the thousands separator the language expects."""
    return f"{value:,.0f}".replace(",", _GROUP_SEPARATOR.get(normalize(language), ","))


def fmt_money(value: float, language: str = DEFAULT_LANGUAGE) -> str:
    """Hryvnia, spaced away from the digits.

    DejaVu sets ₴ tight against a following digit and the two read as one
    glyph; the space is what keeps ₴968 from looking like a single character.
    """
    return f"₴ {fmt_int(value, language)}"


def fmt_window(start, end, language: str = DEFAULT_LANGUAGE) -> str:
    """The reported week as `18.05 – 24.05.2026`.

    Numeric on purpose. Month names in Ukrainian and Russian inflect — a date
    takes the genitive ("24 травня", not "травень") — so a table of month names
    is a table of wrong month names unless it carries a case per position. The
    numbers say the same thing in every language and cannot be declined wrong.
    """
    return f"{start.strftime('%d.%m')} – {end.strftime('%d.%m.%Y')}"


# ─── The table ──────────────────────────────────────────────────────────────
#
# Keys are namespaced by surface: `report.*` is the weekly message and its
# card, `settings.*` is the bot's settings screen.

_STRINGS: Dict[str, Dict[str, str]] = {
    # ── The weekly report ──
    "report.title": {
        EN: "Weekly report",
        UK: "Тижневий звіт",
        RU: "Недельный отчёт",
    },
    "report.revenue": {EN: "Revenue", UK: "Виручка", RU: "Выручка"},
    "report.orders": {EN: "Orders", UK: "Замовлення", RU: "Заказы"},
    "report.avg_check": {EN: "Avg check", UK: "Серед. чек", RU: "Сред. чек"},
    "report.flat": {EN: "≈ flat", UK: "≈ без змін", RU: "≈ без изменений"},
    "report.no_base": {EN: "—", UK: "—", RU: "—"},

    "report.vs_average": {
        EN: "vs {weeks}-week average",
        UK: "проти середнього за {weeks} тиж.",
        RU: "против среднего за {weeks} нед.",
    },
    "report.vs_last_year": {
        EN: "vs same week {year}",
        UK: "проти того ж тижня {year}",
        RU: "против той же недели {year}",
    },
    "report.normal_range": {
        EN: "Inside the normal range",
        UK: "У межах норми",
        RU: "В пределах нормы",
    },
    "report.unusually_high": {
        EN: "Unusually high",
        UK: "Незвично високо",
        RU: "Необычно высоко",
    },
    "report.unusually_low": {
        EN: "Unusually low",
        UK: "Незвично низько",
        RU: "Необычно низко",
    },
    "report.over_weeks": {
        EN: "over {weeks}w",
        UK: "за {weeks} тиж.",
        RU: "за {weeks} нед.",
    },

    "report.what_moved": {
        EN: "What moved",
        UK: "Що зрушило",
        RU: "Что сдвинулось",
    },
    "report.revenue_wow": {
        EN: "Revenue {delta} week on week",
        UK: "Виручка {delta} тиждень до тижня",
        RU: "Выручка {delta} неделя к неделе",
    },
    "report.lever_orders": {
        EN: " — order count, not basket size:",
        UK: " — кількість замовлень, а не чек:",
        RU: " — количество заказов, а не чек:",
    },
    "report.lever_basket": {
        EN: " — basket size, not order count:",
        UK: " — чек, а не кількість замовлень:",
        RU: " — чек, а не количество заказов:",
    },
    "report.effect_orders": {
        EN: "order count",
        UK: "кількість замовлень",
        RU: "количество заказов",
    },
    "report.effect_check": {
        EN: "avg check",
        UK: "середній чек",
        RU: "средний чек",
    },

    "report.new_orders": {
        EN: "Orders from new customers",
        UK: "Замовлення від нових клієнтів",
        RU: "Заказы от новых клиентов",
    },
    "report.repeat_orders": {
        EN: "Repeat orders",
        UK: "Повторні замовлення",
        RU: "Повторные заказы",
    },
    "report.new_share_drop": {
        EN: "New-customer orders are {share}% of that drop.",
        UK: "Замовлення нових клієнтів — {share}% цього падіння.",
        RU: "Заказы новых клиентов — {share}% этого падения.",
    },
    "report.new_share_gain": {
        EN: "New-customer orders are {share}% of that gain.",
        UK: "Замовлення нових клієнтів — {share}% цього приросту.",
        RU: "Заказы новых клиентов — {share}% этого прироста.",
    },

    "report.top_movers": {
        EN: "Top movers vs previous week",
        UK: "Найбільші зрушення проти минулого тижня",
        RU: "Наибольшие сдвиги против прошлой недели",
    },
    "report.movers_share": {
        EN: "These {count} are {share}% of the week's product-revenue move",
        UK: "Ці {count} — {share}% усього руху виручки за товарами",
        RU: "Эти {count} — {share}% всего движения выручки по товарам",
    },
    "report.open_dashboard": {
        EN: "Open the dashboard",
        UK: "Відкрити дашборд",
        RU: "Открыть дашборд",
    },

    # ── The card ──
    "report.vs_last_week": {
        EN: "vs last week",
        UK: "проти минулого тижня",
        RU: "против прошлой недели",
    },
    "card.orders": {EN: "ORDERS", UK: "ЗАМОВЛЕННЯ", RU: "ЗАКАЗЫ"},
    "card.avg_check": {EN: "AVG CHECK", UK: "СЕРЕД. ЧЕК", RU: "СРЕД. ЧЕК"},
    "card.new_repeat": {
        EN: "NEW / REPEAT",
        UK: "НОВІ / ПОВТОРНІ",
        RU: "НОВЫЕ / ПОВТОРНЫЕ",
    },

    # ── Settings ──
    "settings.title": {EN: "Settings", UK: "Налаштування", RU: "Настройки"},
    "settings.language": {EN: "Language", UK: "Мова", RU: "Язык"},
    "settings.timezone": {EN: "Timezone", UK: "Часовий пояс", RU: "Часовой пояс"},
    "settings.date_range": {
        EN: "Default range",
        UK: "Період за умовчанням",
        RU: "Период по умолчанию",
    },
    "settings.notifications": {
        EN: "Notifications",
        UK: "Сповіщення",
        RU: "Уведомления",
    },
    "settings.on": {EN: "On", UK: "Увімк.", RU: "Вкл."},
    "settings.off": {EN: "Off", UK: "Вимк.", RU: "Выкл."},
    "settings.back": {EN: "Back", UK: "Назад", RU: "Назад"},
    "settings.choose_language": {
        EN: "Choose a language",
        UK: "Оберіть мову",
        RU: "Выберите язык",
    },
    "settings.language_set": {
        EN: "Language set to {name}",
        UK: "Мову змінено на {name}",
        RU: "Язык изменён на {name}",
    },
    "settings.applies_to_report": {
        EN: "The weekly report will arrive in this language too.",
        UK: "Тижневий звіт теж надходитиме цією мовою.",
        RU: "Недельный отчёт тоже будет приходить на этом языке.",
    },
    "range.today": {EN: "Today", UK: "Сьогодні", RU: "Сегодня"},
    "range.week": {EN: "Week", UK: "Тиждень", RU: "Неделя"},
    "range.month": {EN: "Month", UK: "Місяць", RU: "Месяц"},
}
