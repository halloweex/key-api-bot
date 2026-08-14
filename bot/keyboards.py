"""
Keyboard builders for the Telegram bot.

This module contains all inline keyboard definitions to eliminate duplication
throughout the codebase.
"""
import calendar
from typing import List
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove
)
from core.i18n import DEFAULT_LANGUAGE, normalize, t


class ReplyKeyboards:
    """Factory class for creating reply keyboards (persistent bottom buttons)."""

    @staticmethod
    def main_menu(lang: str = DEFAULT_LANGUAGE) -> ReplyKeyboardMarkup:
        """Create persistent main menu keyboard at bottom of chat.

        Telegram sends these back as plain text, so `bot/main.py` matches them
        with a regex built from every translation of the same keys — see
        `core.i18n.all_translations`. Never hand-write one of these labels
        anywhere else.
        """
        keyboard = [
            [KeyboardButton(t("btn.report", lang)), KeyboardButton(t("btn.search", lang))],
            [KeyboardButton(t("btn.dashboard", lang)), KeyboardButton(t("btn.settings", lang))],
            [KeyboardButton(t("btn.help", lang))],
        ]
        return ReplyKeyboardMarkup(
            keyboard,
            resize_keyboard=True,
            one_time_keyboard=False
        )

    @staticmethod
    def remove() -> ReplyKeyboardRemove:
        """Remove reply keyboard."""
        return ReplyKeyboardRemove()


class Keyboards:
    """Factory class for creating inline keyboards."""

    @staticmethod
    def main_menu(lang: str = DEFAULT_LANGUAGE) -> InlineKeyboardMarkup:
        """Create main menu keyboard with Generate Report and Help buttons."""
        keyboard = [
            [InlineKeyboardButton(t("btn.generate_report", lang), callback_data="cmd_report")],
            [InlineKeyboardButton(t("btn.help", lang), callback_data="cmd_help")]
        ]
        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def help_menu(lang: str = DEFAULT_LANGUAGE) -> InlineKeyboardMarkup:
        """Create help menu keyboard."""
        keyboard = [
            [InlineKeyboardButton(t("btn.generate_report", lang), callback_data="cmd_report")],
            [InlineKeyboardButton(t("btn.back_to_main", lang), callback_data="cmd_start")]
        ]
        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def report_types(include_cancel: bool = True,
                     lang: str = DEFAULT_LANGUAGE) -> InlineKeyboardMarkup:
        """Create report type selection keyboard (Summary, Excel, TOP-10)."""
        keyboard = [
            [
                InlineKeyboardButton(t("report_type.summary", lang), callback_data="report_type_summary"),
                InlineKeyboardButton(t("report_type.excel", lang), callback_data="report_type_excel")
            ],
            [
                InlineKeyboardButton(t("report_type.top10", lang), callback_data="report_type_top10")
            ]
        ]

        if include_cancel:
            keyboard.append([InlineKeyboardButton(t("btn.cancel", lang), callback_data="go_back")])

        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def date_ranges(back_callback: str = "back_to_report_type",
                    lang: str = DEFAULT_LANGUAGE) -> InlineKeyboardMarkup:
        """Create date range selection keyboard."""
        keyboard = [
            [
                InlineKeyboardButton(t("daterange.today", lang), callback_data="range_today"),
                InlineKeyboardButton(t("daterange.yesterday", lang), callback_data="range_yesterday")
            ],
            [
                InlineKeyboardButton(t("daterange.thisweek", lang), callback_data="range_thisweek"),
                InlineKeyboardButton(t("daterange.thismonth", lang), callback_data="range_thismonth")
            ],
            [
                InlineKeyboardButton(t("daterange.custom", lang), callback_data="range_custom")
            ],
            [
                InlineKeyboardButton(t("btn.back", lang), callback_data=back_callback)
            ]
        ]
        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def top10_sources(lang: str = DEFAULT_LANGUAGE) -> InlineKeyboardMarkup:
        """Create TOP-10 source selection keyboard."""
        keyboard = [
            [
                InlineKeyboardButton("📸 Instagram", callback_data="top10_source_1"),
                InlineKeyboardButton("🛍️ Shopify", callback_data="top10_source_4")
            ],
            [
                InlineKeyboardButton("✈️ Telegram", callback_data="top10_source_2"),
                InlineKeyboardButton(t("btn.all_sources", lang), callback_data="top10_source_all")
            ],
            [
                InlineKeyboardButton(t("btn.back", lang), callback_data="back_to_report_type")
            ]
        ]
        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def year_picker(years: List[int], back_callback: str,
                    lang: str = DEFAULT_LANGUAGE) -> InlineKeyboardMarkup:
        """Create year picker keyboard with horizontal layout."""
        keyboard = []
        # Put all years in a single row (typically 2-3 years)
        year_row = [InlineKeyboardButton(str(year), callback_data=f"custom_start_year_{year}") for year in years]
        keyboard.append(year_row)
        keyboard.append([InlineKeyboardButton(t("btn.back", lang), callback_data=back_callback)])
        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def end_year_picker(years: List[int], back_callback: str,
                    lang: str = DEFAULT_LANGUAGE) -> InlineKeyboardMarkup:
        """Create end year picker keyboard with horizontal layout."""
        keyboard = []
        # Put all years in a single row (typically 2-3 years)
        year_row = [InlineKeyboardButton(str(year), callback_data=f"custom_end_year_{year}") for year in years]
        keyboard.append(year_row)
        keyboard.append([InlineKeyboardButton(t("btn.back", lang), callback_data=back_callback)])
        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def month_picker(back_callback: str, start_callback_prefix: str = "custom_start_month",
                     lang: str = DEFAULT_LANGUAGE) -> InlineKeyboardMarkup:
        """Create month picker keyboard with 3 months per row."""
        keyboard = []
        months = []

        for month in range(1, 13):
            month_name = t(f"month.{month}", lang)
            months.append(InlineKeyboardButton(month_name, callback_data=f"{start_callback_prefix}_{month}"))

            if len(months) == 3:  # 3 months per row
                keyboard.append(months)
                months = []

        if months:  # Add any remaining months
            keyboard.append(months)

        keyboard.append([InlineKeyboardButton(t("btn.back", lang), callback_data=back_callback)])
        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def month_picker_range(start_month: int, back_callback: str,
                           callback_prefix: str = "custom_end_month",
                           lang: str = DEFAULT_LANGUAGE) -> InlineKeyboardMarkup:
        """Create month picker keyboard for a specific range of months."""
        keyboard = []
        months = []

        for month in range(start_month, 13):
            month_name = t(f"month.{month}", lang)
            months.append(InlineKeyboardButton(month_name, callback_data=f"{callback_prefix}_{month}"))

            if len(months) == 3:  # 3 months per row
                keyboard.append(months)
                months = []

        if months:  # Add any remaining months
            keyboard.append(months)

        keyboard.append([InlineKeyboardButton(t("btn.back", lang), callback_data=back_callback)])
        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def day_picker(year: int, month: int, start_day: int, back_callback: str,
                   callback_prefix: str = "custom_start_day",
                   lang: str = DEFAULT_LANGUAGE) -> InlineKeyboardMarkup:
        """Create day picker keyboard with 7 days per row."""
        num_days = calendar.monthrange(year, month)[1]

        keyboard = []
        days_row = []

        for day in range(start_day, num_days + 1):
            days_row.append(InlineKeyboardButton(str(day), callback_data=f"{callback_prefix}_{day}"))

            if len(days_row) == 7 or day == num_days:
                keyboard.append(days_row)
                days_row = []

        keyboard.append([InlineKeyboardButton(t("btn.back", lang), callback_data=back_callback)])
        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def post_report_actions(include_excel: bool = True, include_summary: bool = True,
                            include_top10: bool = True,
                            lang: str = DEFAULT_LANGUAGE) -> InlineKeyboardMarkup:
        """Create post-report action buttons (New Report, Convert formats, Main Menu)."""
        keyboard = []

        # First row - format conversions
        format_row = []
        if include_excel:
            format_row.append(InlineKeyboardButton(t("btn.excel_version", lang), callback_data="convert_to_excel"))
        if include_summary:
            format_row.append(InlineKeyboardButton(t("btn.summary_view", lang), callback_data="convert_to_summary"))

        if format_row:
            # Split into rows of 2 if needed
            if len(format_row) == 2:
                keyboard.append(format_row)
            else:
                keyboard.append([format_row[0]])

        # TOP-10 row
        if include_top10:
            keyboard.append([InlineKeyboardButton(t("report_type.top10", lang), callback_data="convert_to_top10")])

        # Navigation row
        keyboard.append([
            InlineKeyboardButton(t("btn.new_report", lang), callback_data="cmd_report"),
            InlineKeyboardButton(t("btn.main_menu", lang), callback_data="cmd_start")
        ])

        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def top10_post_report(lang: str = DEFAULT_LANGUAGE) -> InlineKeyboardMarkup:
        """Create post TOP-10 report action buttons."""
        keyboard = [
            [
                InlineKeyboardButton(t("btn.new_report", lang), callback_data="cmd_report"),
                InlineKeyboardButton(t("btn.other_sources", lang), callback_data="change_top10_source")
            ],
            [InlineKeyboardButton(t("btn.main_menu", lang), callback_data="cmd_start")]
        ]
        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def top10_quick_source_picker(lang: str = DEFAULT_LANGUAGE) -> InlineKeyboardMarkup:
        """Create quick TOP-10 source picker (for changing sources)."""
        keyboard = [
            [
                InlineKeyboardButton("📸 Instagram", callback_data="quick_top10_1"),
                InlineKeyboardButton("🛍️ Shopify", callback_data="quick_top10_4")
            ],
            [
                InlineKeyboardButton("✈️ Telegram", callback_data="quick_top10_2"),
                InlineKeyboardButton(t("btn.all_sources", lang), callback_data="quick_top10_all")
            ],
            [
                InlineKeyboardButton(t("btn.main_menu", lang), callback_data="cmd_start")
            ]
        ]
        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def error_retry(lang: str = DEFAULT_LANGUAGE) -> InlineKeyboardMarkup:
        """Create error retry keyboard."""
        keyboard = [
            [InlineKeyboardButton(t("btn.try_again", lang), callback_data="cmd_report")],
            [InlineKeyboardButton(t("btn.main_menu", lang), callback_data="cmd_start")]
        ]
        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def cancel_operation(lang: str = DEFAULT_LANGUAGE) -> InlineKeyboardMarkup:
        """Create cancel operation keyboard."""
        keyboard = [
            [
                InlineKeyboardButton(t("btn.new_report", lang), callback_data="cmd_report"),
                InlineKeyboardButton(t("btn.help", lang), callback_data="cmd_help")
            ],
            [InlineKeyboardButton(t("btn.main_menu", lang), callback_data="cmd_start")]
        ]
        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def try_again_or_convert(lang: str = DEFAULT_LANGUAGE) -> InlineKeyboardMarkup:
        """Create try again or convert format keyboard (for Excel errors)."""
        keyboard = [
            [
                InlineKeyboardButton(t("btn.try_again", lang), callback_data="cmd_report"),
                InlineKeyboardButton(t("btn.summary_report", lang), callback_data="convert_to_summary")
            ],
            [InlineKeyboardButton(t("btn.main_menu", lang), callback_data="cmd_start")]
        ]
        return InlineKeyboardMarkup(keyboard)

    # ─── Search Keyboards ──────────────────────────────────────────────────

    @staticmethod
    def search_type(lang: str = DEFAULT_LANGUAGE) -> InlineKeyboardMarkup:
        """Create search type selection keyboard."""
        keyboard = [
            [
                InlineKeyboardButton(t("btn.order_id", lang), callback_data="search_type_id"),
                InlineKeyboardButton(t("btn.phone", lang), callback_data="search_type_phone")
            ],
            [
                InlineKeyboardButton(t("btn.email", lang), callback_data="search_type_email")
            ],
            [InlineKeyboardButton(t("btn.cancel", lang), callback_data="cmd_start")]
        ]
        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def search_results_actions(lang: str = DEFAULT_LANGUAGE) -> InlineKeyboardMarkup:
        """Create search results action buttons."""
        keyboard = [
            [InlineKeyboardButton(t("btn.new_search", lang), callback_data="cmd_search")],
            [InlineKeyboardButton(t("btn.main_menu", lang), callback_data="cmd_start")]
        ]
        return InlineKeyboardMarkup(keyboard)

    # ─── Settings Keyboards ────────────────────────────────────────────────

    @staticmethod
    def settings_menu(prefs: dict = None, lang: str = None) -> InlineKeyboardMarkup:
        """Create settings menu keyboard."""
        from core.i18n import LANGUAGE_NAMES

        prefs = prefs or {}
        lang = normalize(lang if lang is not None else prefs.get('language'))
        tz = prefs.get('timezone', 'Europe/Kyiv')
        date_range = prefs.get('default_date_range', 'week')
        notif = prefs.get('notifications_enabled', 1)

        # Format display values
        tz_display = tz.split('/')[-1] if '/' in tz else tz
        range_display = t(f"range.{date_range}", lang) if date_range in (
            'today', 'week', 'month') else date_range.title()
        notif_display = f"✅ {t('settings.on', lang)}" if notif else f"❌ {t('settings.off', lang)}"

        keyboard = [
            [InlineKeyboardButton(
                f"🌐 {t('settings.language', lang)}: {LANGUAGE_NAMES[lang]}",
                callback_data="settings_language")],
            [InlineKeyboardButton(f"🌍 {t('settings.timezone', lang)}: {tz_display}", callback_data="settings_timezone")],
            [InlineKeyboardButton(f"📅 {t('settings.date_range', lang)}: {range_display}", callback_data="settings_date_range")],
            [InlineKeyboardButton(f"🔔 {t('settings.notifications', lang)}: {notif_display}", callback_data="settings_notifications")],
            [InlineKeyboardButton(f"🔙 {t('settings.back', lang)}", callback_data="cmd_start")]
        ]
        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def settings_language(current: str = None) -> InlineKeyboardMarkup:
        """Create language selection keyboard.

        Named, not flagged. This is a Ukrainian company, and putting a Russian
        flag in its internal tooling is not a neutral act; a language has a
        name, and the name is enough.
        """
        from core.i18n import LANGUAGE_NAMES, LANGUAGES

        current = normalize(current)
        keyboard = [
            [InlineKeyboardButton(
                f"{'● ' if code == current else ''}{LANGUAGE_NAMES[code]}",
                callback_data=f"set_lang_{code}")]
            for code in LANGUAGES
        ]
        keyboard.append([InlineKeyboardButton(
            f"🔙 {t('settings.back', current)}", callback_data="settings_back")])
        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def settings_timezone(lang: str = DEFAULT_LANGUAGE) -> InlineKeyboardMarkup:
        """Create timezone selection keyboard."""
        keyboard = [
            [
                InlineKeyboardButton("🇺🇦 Kyiv", callback_data="set_tz_Europe/Kyiv"),
                InlineKeyboardButton("🇵🇱 Warsaw", callback_data="set_tz_Europe/Warsaw")
            ],
            [
                InlineKeyboardButton("🇩🇪 Berlin", callback_data="set_tz_Europe/Berlin"),
                InlineKeyboardButton("🇬🇧 London", callback_data="set_tz_Europe/London")
            ],
            [
                InlineKeyboardButton("🇺🇸 New York", callback_data="set_tz_America/New_York"),
                InlineKeyboardButton("🇺🇸 Los Angeles", callback_data="set_tz_America/Los_Angeles")
            ],
            [InlineKeyboardButton(t("btn.back", lang), callback_data="settings_back")]
        ]
        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def settings_date_range(lang: str = DEFAULT_LANGUAGE) -> InlineKeyboardMarkup:
        """Create default date range selection keyboard."""
        keyboard = [
            [
                InlineKeyboardButton(t("daterange.today", lang), callback_data="set_range_today"),
                InlineKeyboardButton(t("daterange.thisweek", lang), callback_data="set_range_week")
            ],
            [
                InlineKeyboardButton(t("daterange.thismonth", lang), callback_data="set_range_month")
            ],
            [InlineKeyboardButton(t("btn.back", lang), callback_data="settings_back")]
        ]
        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def settings_notifications(lang: str = DEFAULT_LANGUAGE) -> InlineKeyboardMarkup:
        """Create notifications toggle keyboard."""
        keyboard = [
            [
                InlineKeyboardButton(t("settings.enable", lang), callback_data="set_notif_1"),
                InlineKeyboardButton(t("settings.disable", lang), callback_data="set_notif_0")
            ],
            [InlineKeyboardButton(t("btn.back", lang), callback_data="settings_back")]
        ]
        return InlineKeyboardMarkup(keyboard)
