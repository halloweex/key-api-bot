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
    ReplyKeyboardRemove,
    WebAppInfo
)
from bot.config import REPORT_TYPES, DATE_RANGES, SOURCE_NAMES, DASHBOARD_URL


class ReplyKeyboards:
    """Factory class for creating reply keyboards (persistent bottom buttons)."""

    @staticmethod
    def main_menu() -> ReplyKeyboardMarkup:
        """Create persistent main menu keyboard at bottom of chat."""
        keyboard = [
            [KeyboardButton("📊 Report"), KeyboardButton("🔍 Search")],
            [
                KeyboardButton("📈 Dashboard", web_app=WebAppInfo(url=DASHBOARD_URL)),
                KeyboardButton("⚙️ Settings")
            ],
            [KeyboardButton("ℹ️ Help")],
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
    def main_menu() -> InlineKeyboardMarkup:
        """Create main menu keyboard with Generate Report and Help buttons."""
        keyboard = [
            [InlineKeyboardButton("📊 Generate Report", callback_data="cmd_report")],
            [InlineKeyboardButton("ℹ️ Help", callback_data="cmd_help")]
        ]
        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def help_menu() -> InlineKeyboardMarkup:
        """Create help menu keyboard."""
        keyboard = [
            [InlineKeyboardButton("📊 Generate Report", callback_data="cmd_report")],
            [InlineKeyboardButton("🔙 Back to Main Menu", callback_data="cmd_start")]
        ]
        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def report_types(include_cancel: bool = True) -> InlineKeyboardMarkup:
        """Create report type selection keyboard (Summary, Excel, TOP-10)."""
        keyboard = [
            [
                InlineKeyboardButton(REPORT_TYPES["summary"], callback_data="report_type_summary"),
                InlineKeyboardButton(REPORT_TYPES["excel"], callback_data="report_type_excel")
            ],
            [
                InlineKeyboardButton(REPORT_TYPES["top10"], callback_data="report_type_top10")
            ]
        ]

        if include_cancel:
            keyboard.append([InlineKeyboardButton("🔙 Cancel", callback_data="go_back")])

        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def date_ranges(back_callback: str = "back_to_report_type") -> InlineKeyboardMarkup:
        """Create date range selection keyboard."""
        keyboard = [
            [
                InlineKeyboardButton(DATE_RANGES["today"], callback_data="range_today"),
                InlineKeyboardButton(DATE_RANGES["yesterday"], callback_data="range_yesterday")
            ],
            [
                InlineKeyboardButton(DATE_RANGES["thisweek"], callback_data="range_thisweek"),
                InlineKeyboardButton(DATE_RANGES["thismonth"], callback_data="range_thismonth")
            ],
            [
                InlineKeyboardButton(DATE_RANGES["custom"], callback_data="range_custom")
            ],
            [
                InlineKeyboardButton("🔙 Back", callback_data=back_callback)
            ]
        ]
        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def top10_sources() -> InlineKeyboardMarkup:
        """Create TOP-10 source selection keyboard."""
        keyboard = [
            [
                InlineKeyboardButton("📸 Instagram", callback_data="top10_source_1"),
                InlineKeyboardButton("🛍️ Shopify", callback_data="top10_source_4")
            ],
            [
                InlineKeyboardButton("✈️ Telegram", callback_data="top10_source_2"),
                InlineKeyboardButton("🌐 All Sources", callback_data="top10_source_all")
            ],
            [
                InlineKeyboardButton("🔙 Back", callback_data="back_to_report_type")
            ]
        ]
        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def year_picker(years: List[int], back_callback: str) -> InlineKeyboardMarkup:
        """Create year picker keyboard with horizontal layout."""
        keyboard = []
        # Put all years in a single row (typically 2-3 years)
        year_row = [InlineKeyboardButton(str(year), callback_data=f"custom_start_year_{year}") for year in years]
        keyboard.append(year_row)
        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data=back_callback)])
        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def end_year_picker(years: List[int], back_callback: str) -> InlineKeyboardMarkup:
        """Create end year picker keyboard with horizontal layout."""
        keyboard = []
        # Put all years in a single row (typically 2-3 years)
        year_row = [InlineKeyboardButton(str(year), callback_data=f"custom_end_year_{year}") for year in years]
        keyboard.append(year_row)
        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data=back_callback)])
        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def month_picker(back_callback: str, start_callback_prefix: str = "custom_start_month") -> InlineKeyboardMarkup:
        """Create month picker keyboard with 3 months per row."""
        keyboard = []
        months = []

        for month in range(1, 13):
            month_name = calendar.month_abbr[month]
            months.append(InlineKeyboardButton(month_name, callback_data=f"{start_callback_prefix}_{month}"))

            if len(months) == 3:  # 3 months per row
                keyboard.append(months)
                months = []

        if months:  # Add any remaining months
            keyboard.append(months)

        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data=back_callback)])
        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def month_picker_range(start_month: int, back_callback: str, callback_prefix: str = "custom_end_month") -> InlineKeyboardMarkup:
        """Create month picker keyboard for a specific range of months."""
        keyboard = []
        months = []

        for month in range(start_month, 13):
            month_name = calendar.month_abbr[month]
            months.append(InlineKeyboardButton(month_name, callback_data=f"{callback_prefix}_{month}"))

            if len(months) == 3:  # 3 months per row
                keyboard.append(months)
                months = []

        if months:  # Add any remaining months
            keyboard.append(months)

        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data=back_callback)])
        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def day_picker(year: int, month: int, start_day: int, back_callback: str, callback_prefix: str = "custom_start_day") -> InlineKeyboardMarkup:
        """Create day picker keyboard with 7 days per row."""
        num_days = calendar.monthrange(year, month)[1]

        keyboard = []
        days_row = []

        for day in range(start_day, num_days + 1):
            days_row.append(InlineKeyboardButton(str(day), callback_data=f"{callback_prefix}_{day}"))

            if len(days_row) == 7 or day == num_days:
                keyboard.append(days_row)
                days_row = []

        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data=back_callback)])
        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def post_report_actions(include_excel: bool = True, include_summary: bool = True, include_top10: bool = True) -> InlineKeyboardMarkup:
        """Create post-report action buttons (New Report, Convert formats, Main Menu)."""
        keyboard = []

        # First row - format conversions
        format_row = []
        if include_excel:
            format_row.append(InlineKeyboardButton("📑 Excel Version", callback_data="convert_to_excel"))
        if include_summary:
            format_row.append(InlineKeyboardButton("📊 Summary View", callback_data="convert_to_summary"))

        if format_row:
            # Split into rows of 2 if needed
            if len(format_row) == 2:
                keyboard.append(format_row)
            else:
                keyboard.append([format_row[0]])

        # TOP-10 row
        if include_top10:
            keyboard.append([InlineKeyboardButton("🏆 TOP-10 Products", callback_data="convert_to_top10")])

        # Navigation row
        keyboard.append([
            InlineKeyboardButton("📊 New Report", callback_data="cmd_report"),
            InlineKeyboardButton("🏠 Main Menu", callback_data="cmd_start")
        ])

        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def top10_post_report() -> InlineKeyboardMarkup:
        """Create post TOP-10 report action buttons."""
        keyboard = [
            [
                InlineKeyboardButton("📊 New Report", callback_data="cmd_report"),
                InlineKeyboardButton("🏆 Other Sources", callback_data="change_top10_source")
            ],
            [InlineKeyboardButton("🏠 Main Menu", callback_data="cmd_start")]
        ]
        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def top10_quick_source_picker() -> InlineKeyboardMarkup:
        """Create quick TOP-10 source picker (for changing sources)."""
        keyboard = [
            [
                InlineKeyboardButton("📸 Instagram", callback_data="quick_top10_1"),
                InlineKeyboardButton("🛍️ Shopify", callback_data="quick_top10_4")
            ],
            [
                InlineKeyboardButton("✈️ Telegram", callback_data="quick_top10_2"),
                InlineKeyboardButton("🌐 All Sources", callback_data="quick_top10_all")
            ],
            [
                InlineKeyboardButton("🔙 Main Menu", callback_data="cmd_start")
            ]
        ]
        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def error_retry() -> InlineKeyboardMarkup:
        """Create error retry keyboard."""
        keyboard = [
            [InlineKeyboardButton("🔄 Try Again", callback_data="cmd_report")],
            [InlineKeyboardButton("🏠 Main Menu", callback_data="cmd_start")]
        ]
        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def cancel_operation() -> InlineKeyboardMarkup:
        """Create cancel operation keyboard."""
        keyboard = [
            [
                InlineKeyboardButton("📊 New Report", callback_data="cmd_report"),
                InlineKeyboardButton("ℹ️ Help", callback_data="cmd_help")
            ],
            [InlineKeyboardButton("🏠 Main Menu", callback_data="cmd_start")]
        ]
        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def try_again_or_convert() -> InlineKeyboardMarkup:
        """Create try again or convert format keyboard (for Excel errors)."""
        keyboard = [
            [
                InlineKeyboardButton("🔄 Try Again", callback_data="cmd_report"),
                InlineKeyboardButton("📊 Summary Report", callback_data="convert_to_summary")
            ],
            [InlineKeyboardButton("🏠 Main Menu", callback_data="cmd_start")]
        ]
        return InlineKeyboardMarkup(keyboard)

    # ─── Search Keyboards ──────────────────────────────────────────────────

    @staticmethod
    def search_type() -> InlineKeyboardMarkup:
        """Create search type selection keyboard."""
        keyboard = [
            [
                InlineKeyboardButton("🔢 Order ID", callback_data="search_type_id"),
                InlineKeyboardButton("📱 Phone", callback_data="search_type_phone")
            ],
            [
                InlineKeyboardButton("📧 Email", callback_data="search_type_email")
            ],
            [InlineKeyboardButton("🔙 Cancel", callback_data="cmd_start")]
        ]
        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def search_results_actions() -> InlineKeyboardMarkup:
        """Create search results action buttons."""
        keyboard = [
            [InlineKeyboardButton("🔍 New Search", callback_data="cmd_search")],
            [InlineKeyboardButton("🏠 Main Menu", callback_data="cmd_start")]
        ]
        return InlineKeyboardMarkup(keyboard)

    # ─── Settings Keyboards ────────────────────────────────────────────────

    @staticmethod
    def settings_menu(prefs: dict = None) -> InlineKeyboardMarkup:
        """Create settings menu keyboard."""
        prefs = prefs or {}
        tz = prefs.get('timezone', 'Europe/Kyiv')
        date_range = prefs.get('default_date_range', 'week')
        notif = prefs.get('notifications_enabled', 1)

        # Format display values
        tz_display = tz.split('/')[-1] if '/' in tz else tz
        range_display = {'today': 'Today', 'week': 'Week', 'month': 'Month'}.get(date_range, date_range.title())
        notif_display = "✅ On" if notif else "❌ Off"

        keyboard = [
            [InlineKeyboardButton(f"🌍 Timezone: {tz_display}", callback_data="settings_timezone")],
            [InlineKeyboardButton(f"📅 Default Range: {range_display}", callback_data="settings_date_range")],
            [InlineKeyboardButton(f"🔔 Notifications: {notif_display}", callback_data="settings_notifications")],
            [InlineKeyboardButton("🔙 Back", callback_data="cmd_start")]
        ]
        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def settings_timezone() -> InlineKeyboardMarkup:
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
            [InlineKeyboardButton("🔙 Back", callback_data="settings_back")]
        ]
        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def settings_date_range() -> InlineKeyboardMarkup:
        """Create default date range selection keyboard."""
        keyboard = [
            [
                InlineKeyboardButton("📅 Today", callback_data="set_range_today"),
                InlineKeyboardButton("📆 This Week", callback_data="set_range_week")
            ],
            [
                InlineKeyboardButton("📆 This Month", callback_data="set_range_month")
            ],
            [InlineKeyboardButton("🔙 Back", callback_data="settings_back")]
        ]
        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def settings_notifications() -> InlineKeyboardMarkup:
        """Create notifications toggle keyboard."""
        keyboard = [
            [
                InlineKeyboardButton("✅ Enable", callback_data="set_notif_1"),
                InlineKeyboardButton("❌ Disable", callback_data="set_notif_0")
            ],
            [InlineKeyboardButton("🔙 Back", callback_data="settings_back")]
        ]
        return InlineKeyboardMarkup(keyboard)
