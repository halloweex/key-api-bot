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
    "settings.enable": {EN: "✅ Enable", UK: "✅ Увімкнути", RU: "✅ Включить"},
    "settings.disable": {EN: "❌ Disable", UK: "❌ Вимкнути", RU: "❌ Выключить"},
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

    # ── Month abbreviations, for standalone labels only ──
    # Safe here and nowhere else: a month on its own button is nominative, which
    # is what these are. Put one after a day number and Ukrainian and Russian
    # want the genitive — which is why dates are rendered numerically instead.
    "month.1": {EN: "Jan", UK: "Січ", RU: "Янв"},
    "month.2": {EN: "Feb", UK: "Лют", RU: "Фев"},
    "month.3": {EN: "Mar", UK: "Бер", RU: "Мар"},
    "month.4": {EN: "Apr", UK: "Кві", RU: "Апр"},
    "month.5": {EN: "May", UK: "Тра", RU: "Май"},
    "month.6": {EN: "Jun", UK: "Чер", RU: "Июн"},
    "month.7": {EN: "Jul", UK: "Лип", RU: "Июл"},
    "month.8": {EN: "Aug", UK: "Сер", RU: "Авг"},
    "month.9": {EN: "Sep", UK: "Вер", RU: "Сен"},
    "month.10": {EN: "Oct", UK: "Жов", RU: "Окт"},
    "month.11": {EN: "Nov", UK: "Лис", RU: "Ноя"},
    "month.12": {EN: "Dec", UK: "Гру", RU: "Дек"},

    # ── Buttons ──
    # The first five are reply-keyboard labels. Telegram sends them back as
    # plain text, so `bot/main.py` matches them with a regex built from every
    # translation of the key — see `all_translations`. Change a label here and
    # the matcher follows; type one into a regex by hand and the button dies.
    "btn.report": {EN: "📊 Report", UK: "📊 Звіт", RU: "📊 Отчёт"},
    "btn.search": {EN: "🔍 Search", UK: "🔍 Пошук", RU: "🔍 Поиск"},
    "btn.dashboard": {EN: "📈 Dashboard", UK: "📈 Дашборд", RU: "📈 Дашборд"},
    "btn.settings": {EN: "⚙️ Settings", UK: "⚙️ Налаштування", RU: "⚙️ Настройки"},
    "btn.help": {EN: "ℹ️ Help", UK: "ℹ️ Довідка", RU: "ℹ️ Помощь"},

    "btn.generate_report": {
        EN: "📊 Generate Report",
        UK: "📊 Створити звіт",
        RU: "📊 Создать отчёт",
    },
    "btn.new_report": {EN: "📊 New Report", UK: "📊 Новий звіт", RU: "📊 Новый отчёт"},
    "btn.new_search": {EN: "🔍 New Search", UK: "🔍 Новий пошук", RU: "🔍 Новый поиск"},
    "btn.main_menu": {EN: "🏠 Main Menu", UK: "🏠 Головне меню", RU: "🏠 Главное меню"},
    "btn.back_to_main": {
        EN: "🔙 Back to Main Menu",
        UK: "🔙 До головного меню",
        RU: "🔙 В главное меню",
    },
    "btn.back": {EN: "🔙 Back", UK: "🔙 Назад", RU: "🔙 Назад"},
    "btn.cancel": {EN: "🔙 Cancel", UK: "🔙 Скасувати", RU: "🔙 Отменить"},
    "btn.try_again": {EN: "🔄 Try Again", UK: "🔄 Спробувати ще", RU: "🔄 Попробовать ещё"},
    "btn.excel_version": {
        EN: "📑 Excel Version", UK: "📑 Версія Excel", RU: "📑 Версия Excel",
    },
    "btn.summary_view": {
        EN: "📊 Summary View", UK: "📊 Зведення", RU: "📊 Сводка",
    },
    "btn.summary_report": {
        EN: "📊 Summary Report", UK: "📊 Зведений звіт", RU: "📊 Сводный отчёт",
    },
    "btn.other_sources": {
        EN: "🏆 Other Sources", UK: "🏆 Інші джерела", RU: "🏆 Другие источники",
    },
    "btn.all_sources": {
        EN: "🌐 All Sources", UK: "🌐 Усі джерела", RU: "🌐 Все источники",
    },
    "btn.order_id": {EN: "🔢 Order ID", UK: "🔢 Номер замовлення", RU: "🔢 Номер заказа"},
    "btn.phone": {EN: "📱 Phone", UK: "📱 Телефон", RU: "📱 Телефон"},
    "btn.email": {EN: "📧 Email", UK: "📧 Пошта", RU: "📧 Почта"},
    "btn.open_dashboard": {
        EN: "📈 Open Dashboard", UK: "📈 Відкрити дашборд", RU: "📈 Открыть дашборд",
    },
    "btn.request_access": {
        EN: "🔑 Request Access", UK: "🔑 Запросити доступ", RU: "🔑 Запросить доступ",
    },

    # ── Report types and date ranges ──
    "report_type.summary": {
        EN: "📊 Summary Report", UK: "📊 Зведений звіт", RU: "📊 Сводный отчёт",
    },
    "report_type.excel": {
        EN: "📑 Excel Report", UK: "📑 Звіт у Excel", RU: "📑 Отчёт в Excel",
    },
    "report_type.top10": {
        EN: "🏆 TOP-10 Products", UK: "🏆 ТОП-10 товарів", RU: "🏆 ТОП-10 товаров",
    },

    "daterange.today": {EN: "📅 Today", UK: "📅 Сьогодні", RU: "📅 Сегодня"},
    "daterange.yesterday": {EN: "📅 Yesterday", UK: "📅 Учора", RU: "📅 Вчера"},
    "daterange.thisweek": {EN: "📆 This Week", UK: "📆 Цей тиждень", RU: "📆 Эта неделя"},
    "daterange.thismonth": {EN: "📆 This Month", UK: "📆 Цей місяць", RU: "📆 Этот месяц"},
    "daterange.custom": {
        EN: "🗓️ Custom Date Range", UK: "🗓️ Свій період", RU: "🗓️ Свой период",
    },
    "source.all": {EN: "All Sources", UK: "Усі джерела", RU: "Все источники"},

    # ── Commands, as Telegram shows them in the menu ──
    "cmd.start": {EN: "👋 Start the bot", UK: "👋 Запустити бота", RU: "👋 Запустить бота"},
    "cmd.report": {
        EN: "📊 Generate a sales report",
        UK: "📊 Створити звіт з продажів",
        RU: "📊 Создать отчёт по продажам",
    },
    "cmd.search": {EN: "🔍 Search orders", UK: "🔍 Пошук замовлень", RU: "🔍 Поиск заказов"},
    "cmd.settings": {
        EN: "⚙️ User settings", UK: "⚙️ Налаштування", RU: "⚙️ Настройки",
    },
    "cmd.dashboard": {
        EN: "📈 Open sales dashboard",
        UK: "📈 Відкрити дашборд продажів",
        RU: "📈 Открыть дашборд продаж",
    },
    "cmd.help": {
        EN: "ℹ️ Show help information", UK: "ℹ️ Показати довідку", RU: "ℹ️ Показать справку",
    },
    "cmd.cancel": {
        EN: "🛑 Cancel current operation",
        UK: "🛑 Скасувати поточну дію",
        RU: "🛑 Отменить текущее действие",
    },

    # ── Messages ──
    "msg.welcome": {
        EN: "👋 <b>Welcome, {name}</b>!\n\nI am your <b>KoreanStory Sales "
            "Report</b> assistant. I can build detailed sales reports and "
            "analytics.\n\n🚀 <i>What would you like to do?</i>",
        UK: "👋 <b>Вітаю, {name}</b>!\n\nЯ помічник зі <b>звітів про продажі "
            "KoreanStory</b>. Можу зібрати докладні звіти й аналітику.\n\n"
            "🚀 <i>Що зробимо?</i>",
        RU: "👋 <b>Привет, {name}</b>!\n\nЯ помощник по <b>отчётам о продажах "
            "KoreanStory</b>. Могу собрать подробные отчёты и аналитику.\n\n"
            "🚀 <i>Что сделаем?</i>",
    },
    "msg.help": {
        EN: "<b>📊 KoreanStory Sales Report Bot 📊</b>\n\n"
            "<b>Commands:</b>\n"
            "• /report — generate a sales report\n"
            "• /search — search orders by ID, phone or email\n"
            "• /settings — language and preferences\n"
            "• /dashboard — open the web dashboard\n"
            "• /cancel — cancel the current operation\n"
            "• /help — show this message\n\n"
            "<b>Report types:</b>\n"
            "📊 <b>Summary</b> — sales overview by source\n"
            "📑 <b>Excel</b> — detailed spreadsheet\n"
            "🏆 <b>TOP-10</b> — best-selling products\n\n"
            "<i>Version {version}</i>",
        UK: "<b>📊 Бот звітів про продажі KoreanStory 📊</b>\n\n"
            "<b>Команди:</b>\n"
            "• /report — створити звіт з продажів\n"
            "• /search — знайти замовлення за номером, телефоном чи поштою\n"
            "• /settings — мова та налаштування\n"
            "• /dashboard — відкрити вебдашборд\n"
            "• /cancel — скасувати поточну дію\n"
            "• /help — показати це повідомлення\n\n"
            "<b>Типи звітів:</b>\n"
            "📊 <b>Зведення</b> — огляд продажів за джерелами\n"
            "📑 <b>Excel</b> — докладна таблиця\n"
            "🏆 <b>ТОП-10</b> — найпопулярніші товари\n\n"
            "<i>Версія {version}</i>",
        RU: "<b>📊 Бот отчётов о продажах KoreanStory 📊</b>\n\n"
            "<b>Команды:</b>\n"
            "• /report — создать отчёт по продажам\n"
            "• /search — найти заказ по номеру, телефону или почте\n"
            "• /settings — язык и настройки\n"
            "• /dashboard — открыть вебдашборд\n"
            "• /cancel — отменить текущее действие\n"
            "• /help — показать это сообщение\n\n"
            "<b>Типы отчётов:</b>\n"
            "📊 <b>Сводка</b> — обзор продаж по источникам\n"
            "📑 <b>Excel</b> — подробная таблица\n"
            "🏆 <b>ТОП-10</b> — самые продаваемые товары\n\n"
            "<i>Версия {version}</i>",
    },
    "msg.cancelled": {
        EN: "<b>🛑 Operation Cancelled</b>\n\nI have cancelled the current "
            "operation.\nWhat would you like to do next?",
        UK: "<b>🛑 Дію скасовано</b>\n\nПоточну дію скасовано.\nЩо робимо далі?",
        RU: "<b>🛑 Действие отменено</b>\n\nТекущее действие отменено.\n"
            "Что делаем дальше?",
    },
    "msg.generator_title": {
        EN: "📊 Sales Report Generator",
        UK: "📊 Конструктор звітів",
        RU: "📊 Конструктор отчётов",
    },
    "msg.step": {
        EN: "Step {step} of {total}: {name}",
        UK: "Крок {step} з {total}: {name}",
        RU: "Шаг {step} из {total}: {name}",
    },
    "step.report_type": {
        EN: "Select Report Type", UK: "Оберіть тип звіту", RU: "Выберите тип отчёта",
    },
    "step.date_range": {
        EN: "Select Date Range", UK: "Оберіть період", RU: "Выберите период",
    },
    "step.generating": {
        EN: "Generating Report", UK: "Формування звіту", RU: "Формирование отчёта",
    },
    "msg.choose_report_type": {
        EN: "Please choose the type of report you would like:",
        UK: "Оберіть тип звіту, який потрібен:",
        RU: "Выберите тип отчёта, который нужен:",
    },
    "msg.selected_report_type": {
        EN: "Selected report type: <b>{type}</b>",
        UK: "Обраний тип звіту: <b>{type}</b>",
        RU: "Выбранный тип отчёта: <b>{type}</b>",
    },
    "msg.choose_date_range": {
        EN: "Now select the date range for your report:",
        UK: "Тепер оберіть період для звіту:",
        RU: "Теперь выберите период для отчёта:",
    },
    "msg.report_label": {EN: "Report", UK: "Звіт", RU: "Отчёт"},
    "msg.source_label": {EN: "Source", UK: "Джерело", RU: "Источник"},
    "msg.date_range_label": {EN: "Date Range", UK: "Період", RU: "Период"},
    "msg.report_type_label": {EN: "Report type", UK: "Тип звіту", RU: "Тип отчёта"},
    "msg.please_wait": {
        EN: "⏳ <i>Please wait while I build your report…</i>",
        UK: "⏳ <i>Зачекайте, збираю звіт…</i>",
        RU: "⏳ <i>Подождите, собираю отчёт…</i>",
    },
    "msg.excel_preparing": {
        EN: "<b>📑 Preparing Excel Report</b>\n\n📅 <b>Period</b>: {range}\n\n"
            "⏳ <i>Creating your spreadsheet…</i>\nThis may take a moment "
            "depending on how much data there is.",
        UK: "<b>📑 Готую звіт у Excel</b>\n\n📅 <b>Період</b>: {range}\n\n"
            "⏳ <i>Створюю таблицю…</i>\nЦе може зайняти трохи часу — залежить "
            "від обсягу даних.",
        RU: "<b>📑 Готовлю отчёт в Excel</b>\n\n📅 <b>Период</b>: {range}\n\n"
            "⏳ <i>Создаю таблицу…</i>\nЭто может занять немного времени — "
            "зависит от объёма данных.",
    },
    "msg.excel_success": {
        EN: "<b>✅ Excel Report Ready</b>\n\n📅 <b>Period</b>: {range}\n\n"
            "📎 The file has been sent as a separate message.\n"
            "📊 <i>What would you like to do next?</i>",
        UK: "<b>✅ Звіт у Excel готовий</b>\n\n📅 <b>Період</b>: {range}\n\n"
            "📎 Файл надіслано окремим повідомленням.\n"
            "📊 <i>Що робимо далі?</i>",
        RU: "<b>✅ Отчёт в Excel готов</b>\n\n📅 <b>Период</b>: {range}\n\n"
            "📎 Файл отправлен отдельным сообщением.\n"
            "📊 <i>Что делаем дальше?</i>",
    },
    "msg.report_error": {
        EN: "<b>⚠️ Could Not Build the Report</b>\n\nSomething went wrong:\n"
            "<i>{error}</i>\n\nTry a different date range, or contact support "
            "if this keeps happening.",
        UK: "<b>⚠️ Не вдалося зібрати звіт</b>\n\nЩось пішло не так:\n"
            "<i>{error}</i>\n\nСпробуйте інший період або зверніться до "
            "підтримки, якщо це повторюється.",
        RU: "<b>⚠️ Не удалось собрать отчёт</b>\n\nЧто-то пошло не так:\n"
            "<i>{error}</i>\n\nПопробуйте другой период или обратитесь в "
            "поддержку, если это повторяется.",
    },
    "msg.excel_error": {
        EN: "<b>⚠️ Excel Report Failed</b>\n\n<i>{error}</i>\n\nTry again, or "
            "take a summary report instead?",
        UK: "<b>⚠️ Звіт у Excel не вдався</b>\n\n<i>{error}</i>\n\nСпробувати "
            "ще раз чи взяти зведений звіт?",
        RU: "<b>⚠️ Отчёт в Excel не удался</b>\n\n<i>{error}</i>\n\nПопробовать "
            "ещё раз или взять сводный отчёт?",
    },
    "msg.excel_error_plain": {
        EN: "<b>⚠️ Excel Report Failed</b>\n\nI could not build your "
            "spreadsheet.\n\nTry again, or take a summary report instead?",
        UK: "<b>⚠️ Звіт у Excel не вдався</b>\n\nНе вдалося створити таблицю."
            "\n\nСпробувати ще раз чи взяти зведений звіт?",
        RU: "<b>⚠️ Отчёт в Excel не удался</b>\n\nНе удалось создать таблицу."
            "\n\nПопробовать ещё раз или взять сводный отчёт?",
    },
    "msg.api_error": {
        EN: "❌ <b>API Error</b>\n\nCould not reach KeyCRM. Please try again "
            "later.",
        UK: "❌ <b>Помилка API</b>\n\nНе вдалося зв'язатися з KeyCRM. "
            "Спробуйте пізніше.",
        RU: "❌ <b>Ошибка API</b>\n\nНе удалось связаться с KeyCRM. "
            "Попробуйте позже.",
    },
    "msg.session_expired": {
        EN: "⚠️ This report has expired. Start a new one with /report",
        UK: "⚠️ Цей звіт застарів. Почніть новий через /report",
        RU: "⚠️ Этот отчёт устарел. Начните новый через /report",
    },
    "msg.report_ready": {
        EN: "✅ Report ready!", UK: "✅ Звіт готовий!", RU: "✅ Отчёт готов!",
    },
    "msg.custom_dates_title": {
        EN: "📊 Custom Date Range", UK: "📊 Свій період", RU: "📊 Свой период",
    },
    "msg.dashboard": {
        EN: "📈 <b>Sales Dashboard</b>\n\nInteractive charts and analytics:",
        UK: "📈 <b>Дашборд продажів</b>\n\nІнтерактивні графіки й аналітика:",
        RU: "📈 <b>Дашборд продаж</b>\n\nИнтерактивные графики и аналитика:",
    },
    "msg.truncated": {
        EN: "⚠️ <i>Message truncated — it was too long.</i>",
        UK: "⚠️ <i>Повідомлення скорочено — воно було задовге.</i>",
        RU: "⚠️ <i>Сообщение сокращено — оно было слишком длинным.</i>",
    },

    # ── Custom date picker ──
    "pick.start_year": {EN: "Select start year", UK: "Оберіть рік початку",
                        RU: "Выберите год начала"},
    "pick.start_month": {EN: "Select start month", UK: "Оберіть місяць початку",
                         RU: "Выберите месяц начала"},
    "pick.start_day": {EN: "Select start day", UK: "Оберіть день початку",
                       RU: "Выберите день начала"},
    "pick.end_year": {EN: "Select end year", UK: "Оберіть рік кінця",
                      RU: "Выберите год конца"},
    "pick.end_month": {EN: "Select end month", UK: "Оберіть місяць кінця",
                       RU: "Выберите месяц конца"},
    "pick.end_day": {EN: "Select end day", UK: "Оберіть день кінця",
                     RU: "Выберите день конца"},
    "pick.prompt_start_year": {
        EN: "Pick the first year of your range:",
        UK: "Оберіть перший рік періоду:",
        RU: "Выберите первый год периода:",
    },
    "pick.prompt_start_month": {
        EN: "Start year: <b>{year}</b>\nNow pick the month:",
        UK: "Рік початку: <b>{year}</b>\nТепер оберіть місяць:",
        RU: "Год начала: <b>{year}</b>\nТеперь выберите месяц:",
    },
    "pick.prompt_start_day": {
        EN: "Start: <b>{month} {year}</b>\nNow pick the day:",
        UK: "Початок: <b>{month} {year}</b>\nТепер оберіть день:",
        RU: "Начало: <b>{month} {year}</b>\nТеперь выберите день:",
    },
    "pick.prompt_end_year": {
        EN: "Start date: <b>{start}</b>\n\nNow pick the end year:",
        UK: "Дата початку: <b>{start}</b>\n\nТепер оберіть рік кінця:",
        RU: "Дата начала: <b>{start}</b>\n\nТеперь выберите год конца:",
    },
    "pick.prompt_end_month": {
        EN: "Start date: <b>{start}</b>\nEnd year: <b>{year}</b>\n\n"
            "Now pick the end month:",
        UK: "Дата початку: <b>{start}</b>\nРік кінця: <b>{year}</b>\n\n"
            "Тепер оберіть місяць кінця:",
        RU: "Дата начала: <b>{start}</b>\nГод конца: <b>{year}</b>\n\n"
            "Теперь выберите месяц конца:",
    },
    "pick.prompt_end_day": {
        EN: "Start date: <b>{start}</b>\nEnd so far: <b>{month} {year}</b>\n\n"
            "Now pick the end day:",
        UK: "Дата початку: <b>{start}</b>\nКінець наразі: <b>{month} {year}</b>"
            "\n\nТепер оберіть день кінця:",
        RU: "Дата начала: <b>{start}</b>\nКонец пока: <b>{month} {year}</b>\n\n"
            "Теперь выберите день конца:",
    },
    "pick.current_year_note": {
        EN: "(current year)", UK: "(поточний рік)", RU: "(текущий год)",
    },

    # ── TOP-10 ──
    "top10.title": {
        EN: "🏆 TOP-10 Products Report",
        UK: "🏆 Звіт ТОП-10 товарів",
        RU: "🏆 Отчёт ТОП-10 товаров",
    },
    "top10.choose_source": {
        EN: "<i>Select a source to see its TOP-10:</i>",
        UK: "<i>Оберіть джерело, щоб побачити ТОП-10:</i>",
        RU: "<i>Выберите источник, чтобы увидеть ТОП-10:</i>",
    },
    "top10.by_source": {
        EN: "🏆 TOP-10 PRODUCTS BY SOURCE",
        UK: "🏆 ТОП-10 ТОВАРІВ ЗА ДЖЕРЕЛАМИ",
        RU: "🏆 ТОП-10 ТОВАРОВ ПО ИСТОЧНИКАМ",
    },
    "top10.for_source": {
        EN: "🏆 TOP-10 PRODUCTS — {source}",
        UK: "🏆 ТОП-10 ТОВАРІВ — {source}",
        RU: "🏆 ТОП-10 ТОВАРОВ — {source}",
    },
    "top10.no_sales": {
        EN: "<i>no sales in this period</i>",
        UK: "<i>продажів за цей період немає</i>",
        RU: "<i>продаж за этот период нет</i>",
    },
    "top10.total_sold": {EN: "Total sold", UK: "Усього продано", RU: "Всего продано"},
    "msg.generating": {
        EN: "⏳ <i>Building the report…</i>",
        UK: "⏳ <i>Збираю звіт…</i>",
        RU: "⏳ <i>Собираю отчёт…</i>",
    },

    # ── Summary report ──
    "summary.title": {
        EN: "📊 SALES SUMMARY", UK: "📊 ЗВЕДЕННЯ ПРОДАЖІВ", RU: "📊 СВОДКА ПРОДАЖ",
    },
    "summary.total_orders": {
        EN: "Total orders", UK: "Усього замовлень", RU: "Всего заказов",
    },
    "summary.by_source": {
        EN: "📦 Products by source",
        UK: "📦 Товари за джерелами",
        RU: "📦 Товары по источникам",
    },
    "summary.products": {EN: "Products", UK: "Товарів", RU: "Товаров"},
    "summary.orders": {EN: "Orders", UK: "Замовлень", RU: "Заказов"},
    "summary.avg_check": {EN: "Avg check", UK: "Середній чек", RU: "Средний чек"},
    "summary.returns": {
        EN: "Returns/cancelled", UK: "Повернення/скасування", RU: "Возвраты/отмены",
    },
    "summary.generated_at": {
        EN: "Generated {time}", UK: "Сформовано {time}", RU: "Сформировано {time}",
    },
    "milestone.reached": {
        EN: "MILESTONE REACHED!", UK: "РУБІЖ ДОСЯГНУТО!", RU: "РУБЕЖ ДОСТИГНУТ!",
    },
    "milestone.revenue": {EN: "Revenue", UK: "Виручка", RU: "Выручка"},
    "milestone.daily_300k": {
        EN: "₴300K in a single day!",
        UK: "₴300 тис. за один день!",
        RU: "₴300 тыс. за один день!",
    },
    "milestone.weekly_1m": {
        EN: "₴1 MILLION in a week!",
        UK: "₴1 МІЛЬЙОН за тиждень!",
        RU: "₴1 МИЛЛИОН за неделю!",
    },
    "milestone.weekly_2m": {
        EN: "₴2 MILLION in a week!",
        UK: "₴2 МІЛЬЙОНИ за тиждень!",
        RU: "₴2 МИЛЛИОНА за неделю!",
    },
    "milestone.monthly_10m": {
        EN: "₴10 MILLION in a month!",
        UK: "₴10 МІЛЬЙОНІВ за місяць!",
        RU: "₴10 МИЛЛИОНОВ за месяц!",
    },

    # ── Search ──
    "search.title": {EN: "🔍 Order Search", UK: "🔍 Пошук замовлень", RU: "🔍 Поиск заказов"},
    "search.choose_type": {
        EN: "What do you want to search by?",
        UK: "За чим шукаємо?",
        RU: "По чему ищем?",
    },
    "search.prompt_id": {
        EN: "Send me the order number:",
        UK: "Надішліть номер замовлення:",
        RU: "Пришлите номер заказа:",
    },
    "search.prompt_phone": {
        EN: "Send me the phone number:",
        UK: "Надішліть номер телефону:",
        RU: "Пришлите номер телефона:",
    },
    "search.prompt_email": {
        EN: "Send me the email address:",
        UK: "Надішліть адресу пошти:",
        RU: "Пришлите адрес почты:",
    },
    "search.searching": {EN: "🔍 <i>Searching…</i>", UK: "🔍 <i>Шукаю…</i>",
                         RU: "🔍 <i>Ищу…</i>"},
    "search.nothing_found": {
        EN: "🔍 <b>Nothing found</b>\n\nNo orders match <code>{query}</code>.",
        UK: "🔍 <b>Нічого не знайдено</b>\n\nЖодне замовлення не збігається з "
            "<code>{query}</code>.",
        RU: "🔍 <b>Ничего не найдено</b>\n\nНи один заказ не совпадает с "
            "<code>{query}</code>.",
    },

    # ── Access control ──
    "access.required": {
        EN: "🔐 <b>Access Required</b>\n\nThis bot needs authorisation.\n"
            "Tap the button below to request it.",
        UK: "🔐 <b>Потрібен доступ</b>\n\nЦей бот потребує авторизації.\n"
            "Натисніть кнопку нижче, щоб надіслати запит.",
        RU: "🔐 <b>Нужен доступ</b>\n\nЭтот бот требует авторизации.\n"
            "Нажмите кнопку ниже, чтобы отправить запрос.",
    },
    "access.denied": {
        EN: "🔒 <b>Access Denied</b>\n\nYour request was declined.\n"
            "Please contact the administrator.",
        UK: "🔒 <b>Доступ відхилено</b>\n\nВаш запит відхилено.\n"
            "Зверніться до адміністратора.",
        RU: "🔒 <b>Доступ отклонён</b>\n\nВаш запрос отклонён.\n"
            "Обратитесь к администратору.",
    },
    "access.frozen": {
        EN: "🚫 <b>Account Frozen</b>\n\nYour account was frozen after several "
            "declined requests.\nPlease contact the administrator directly.",
        UK: "🚫 <b>Обліковий запис заморожено</b>\n\nЙого заморожено після "
            "кількох відхилених запитів.\nЗверніться до адміністратора напряму.",
        RU: "🚫 <b>Аккаунт заморожен</b>\n\nОн заморожен после нескольких "
            "отклонённых запросов.\nОбратитесь к администратору напрямую.",
    },
    "access.pending": {
        EN: "⏳ <b>Request Pending</b>\n\nYour request is being reviewed.\n"
            "Please wait for approval.",
        UK: "⏳ <b>Запит на розгляді</b>\n\nВаш запит розглядають.\n"
            "Зачекайте на схвалення.",
        RU: "⏳ <b>Запрос на рассмотрении</b>\n\nВаш запрос рассматривают.\n"
            "Дождитесь одобрения.",
    },
    "access.requested": {
        EN: "✅ <b>Request Sent</b>\n\nThe administrator has been notified.\n\n"
            "⏳ Please wait for approval.",
        UK: "✅ <b>Запит надіслано</b>\n\nАдміністратора сповіщено.\n\n"
            "⏳ Зачекайте на схвалення.",
        RU: "✅ <b>Запрос отправлен</b>\n\nАдминистратор уведомлён.\n\n"
            "⏳ Дождитесь одобрения.",
    },
    "access.granted": {
        EN: "🎉 <b>Access Granted</b>\n\nYour request was approved. "
            "You can use the bot now — send /start to begin.",
        UK: "🎉 <b>Доступ надано</b>\n\nВаш запит схвалено. Можна користуватися "
            "ботом — надішліть /start, щоб почати.",
        RU: "🎉 <b>Доступ выдан</b>\n\nВаш запрос одобрен. Можно пользоваться "
            "ботом — отправьте /start, чтобы начать.",
    },
    "access.request_again": {
        EN: "You can request access again:",
        UK: "Можна надіслати запит ще раз:",
        RU: "Можно отправить запрос ещё раз:",
    },
    "search.results_title": {
        EN: "🔍 <b>Search Results</b>", UK: "🔍 <b>Результати пошуку</b>",
        RU: "🔍 <b>Результаты поиска</b>",
    },
    "search.found": {
        EN: "Found {count} order(s) for <code>{query}</code>",
        UK: "Знайдено замовлень: {count} за запитом <code>{query}</code>",
        RU: "Найдено заказов: {count} по запросу <code>{query}</code>",
    },
    "search.more": {
        EN: "…and {count} more", UK: "…і ще {count}", RU: "…и ещё {count}",
    },
    "search.failed": {
        EN: "⚠️ Search failed: {error}", UK: "⚠️ Пошук не вдався: {error}",
        RU: "⚠️ Поиск не удался: {error}",
    },
    "search.order": {EN: "Order", UK: "Замовлення", RU: "Заказ"},
    "search.unknown": {EN: "Unknown", UK: "Невідомо", RU: "Неизвестно"},
    "search.by_type": {
        EN: "🔍 <b>Search by {type}</b>\n\nSend me the value:",
        UK: "🔍 <b>Пошук за: {type}</b>\n\nНадішліть значення:",
        RU: "🔍 <b>Поиск по: {type}</b>\n\nПришлите значение:",
    },
    "admin.only": {
        EN: "⛔ Admin access required", UK: "⛔ Потрібні права адміністратора",
        RU: "⛔ Нужны права администратора",
    },
    "admin.approved_users": {
        EN: "👥 <b>Approved Users</b>", UK: "👥 <b>Схвалені користувачі</b>",
        RU: "👥 <b>Одобренные пользователи</b>",
    },
    "admin.frozen_users": {
        EN: "🧊 <b>Frozen Users</b>", UK: "🧊 <b>Заморожені користувачі</b>",
        RU: "🧊 <b>Замороженные пользователи</b>",
    },
    "admin.no_users": {
        EN: "📋 <b>No users</b>\n\nNobody is approved or frozen.",
        UK: "📋 <b>Користувачів немає</b>\n\nНікого не схвалено й не заморожено.",
        RU: "📋 <b>Пользователей нет</b>\n\nНикто не одобрен и не заморожен.",
    },
    "admin.last_active": {EN: "Last active", UK: "Востаннє активний", RU: "Последняя активность"},
    "admin.frozen_at": {EN: "Frozen", UK: "Заморожено", RU: "Заморожен"},
    "admin.and_more": {
        EN: "…and {count} more", UK: "…і ще {count}", RU: "…и ещё {count}",
    },
    "admin.revoke": {EN: "🚫 Revoke {name}", UK: "🚫 Відкликати {name}", RU: "🚫 Отозвать {name}"},
    "admin.unfreeze": {EN: "🔓 Unfreeze {name}", UK: "🔓 Розморозити {name}", RU: "🔓 Разморозить {name}"},
    "admin.panel_closed": {
        EN: "✅ Admin panel closed.", UK: "✅ Панель закрито.", RU: "✅ Панель закрыта.",
    },
    "admin.new_request": {
        EN: "🔔 <b>New Access Request</b>", UK: "🔔 <b>Новий запит доступу</b>",
        RU: "🔔 <b>Новый запрос доступа</b>",
    },
    "admin.choose_action": {
        EN: "Choose an action:", UK: "Оберіть дію:", RU: "Выберите действие:",
    },
    "admin.user_approved": {EN: "User approved", UK: "Користувача схвалено", RU: "Пользователь одобрен"},
    "admin.user_denied": {EN: "User denied", UK: "Користувача відхилено", RU: "Пользователь отклонён"},
    "admin.user_frozen": {EN: "User frozen", UK: "Користувача заморожено", RU: "Пользователь заморожен"},
    "admin.user_unfrozen": {EN: "User unfrozen", UK: "Користувача розморожено", RU: "Пользователь разморожен"},
    "admin.user_not_found": {EN: "User not found", UK: "Користувача не знайдено", RU: "Пользователь не найден"},
    "admin.action_failed": {EN: "Action failed", UK: "Дія не вдалася", RU: "Действие не удалось"},
    "msg.request_failed": {
        EN: "⚠️ Could not send the request. Try /start again.",
        UK: "⚠️ Не вдалося надіслати запит. Спробуйте /start ще раз.",
        RU: "⚠️ Не удалось отправить запрос. Попробуйте /start ещё раз.",
    },
    "access.unfrozen": {
        EN: "🔓 <b>Account Unfrozen</b>\n\nAn administrator has unfrozen your "
            "account. You can request access again:",
        UK: "🔓 <b>Обліковий запис розморожено</b>\n\nАдміністратор розморозив "
            "ваш обліковий запис. Можна знову надіслати запит доступу:",
        RU: "🔓 <b>Аккаунт разморожен</b>\n\nАдминистратор разморозил ваш "
            "аккаунт. Можно снова отправить запрос доступа:",
    },
    "access.pending_alert": {
        EN: "Your request is pending", UK: "Ваш запит на розгляді",
        RU: "Ваш запрос на рассмотрении",
    },
    "access.frozen_alert": {
        EN: "Account frozen", UK: "Обліковий запис заморожено", RU: "Аккаунт заморожен",
    },
}


def all_translations(key: str) -> Tuple[str, ...]:
    """Every rendering of `key`, deduplicated, for building a matcher.

    Telegram sends a reply-keyboard tap back as the button's plain text, so the
    handler has to recognise every language's label. Deriving that set from the
    same table the buttons are drawn from is what keeps the two from drifting —
    a label typed by hand into a regex is a button that dies silently, which
    has happened here before over a single U+FE0F.
    """
    entry = _STRINGS.get(key)
    if not entry:
        return ()
    return tuple(dict.fromkeys(entry[lang] for lang in LANGUAGES if entry.get(lang)))
