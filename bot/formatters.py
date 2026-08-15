"""
Message formatters and text utilities for the Telegram bot.

Contains text formatting helpers, message templates, and report formatters.
"""
import logging
from datetime import date
from typing import Dict, List, Tuple
from bot.config import SOURCE_MAPPING, MEDALS, VERSION
from core.i18n import DEFAULT_LANGUAGE, fmt_int, fmt_money, t

logger = logging.getLogger(__name__)

# Telegram message length limit
TELEGRAM_MAX_MESSAGE_LENGTH = 4096
TELEGRAM_SAFE_MESSAGE_LENGTH = 3500  # Leave room for formatting


def truncate_message(message: str, max_length: int = TELEGRAM_SAFE_MESSAGE_LENGTH,
                     lang: str = DEFAULT_LANGUAGE) -> str:
    """
    Truncate message if it exceeds max length.

    Args:
        message: The message to truncate
        max_length: Maximum allowed length

    Returns:
        Truncated message with indicator if truncated
    """
    if len(message) <= max_length:
        return message

    logger.warning(f"Message truncated from {len(message)} to {max_length} characters")
    truncated = message[:max_length - 50]
    return truncated + "\n\n" + t("msg.truncated", lang)


def bold(text: str) -> str:
    """Make text bold in Telegram HTML format."""
    return f"<b>{text}</b>"


def italic(text: str) -> str:
    """Make text italic in Telegram HTML format."""
    return f"<i>{text}</i>"


def code(text: str) -> str:
    """Format text as code in Telegram HTML format."""
    return f"<code>{text}</code>"


def create_progress_indicator(current_step: int, total_steps: int) -> str:
    """Create a progress indicator for multi-step processes."""
    filled = "●" * current_step
    empty = "○" * (total_steps - current_step)
    return f"{filled}{empty}"


# ─── Message Templates ──────────────────────────────────────────────────────

class Messages:
    """Standard message templates, rendered in the reader's language."""

    @staticmethod
    def welcome(username: str, lang: str = DEFAULT_LANGUAGE) -> str:
        return t("msg.welcome", lang, name=username)

    @staticmethod
    def help_text(lang: str = DEFAULT_LANGUAGE) -> str:
        return t("msg.help", lang, version=VERSION)

    @staticmethod
    def cancel(lang: str = DEFAULT_LANGUAGE) -> str:
        return t("msg.cancelled", lang)

    @staticmethod
    def _header(step: int, total: int, step_key: str, lang: str) -> str:
        """The generator's title plus its progress line — every step shares it."""
        progress = create_progress_indicator(step, total)
        return (
            f"{bold(t('msg.generator_title', lang))}\n\n"
            f"{progress} {italic(t('msg.step', lang, step=step, total=total, name=t(step_key, lang)))}\n\n"
        )

    @staticmethod
    def report_selection(step: int = 1, total_steps: int = 3,
                         lang: str = DEFAULT_LANGUAGE) -> str:
        return (Messages._header(step, total_steps, "step.report_type", lang)
                + t("msg.choose_report_type", lang))

    @staticmethod
    def date_selection(report_type: str, step: int = 2, total_steps: int = 3,
                       lang: str = DEFAULT_LANGUAGE) -> str:
        return (
            Messages._header(step, total_steps, "step.date_range", lang)
            + t("msg.selected_report_type", lang,
                type=report_type_label(report_type, lang)) + "\n\n"
            + t("msg.choose_date_range", lang)
        )

    @staticmethod
    def top10_date_selection(source_name: str, step: int = 2, total_steps: int = 3,
                             lang: str = DEFAULT_LANGUAGE) -> str:
        return (
            Messages._header(step, total_steps, "step.date_range", lang)
            + f"{t('msg.report_label', lang)}: {bold(t('report_type.top10', lang))}\n"
            + f"{t('msg.source_label', lang)}: {bold(source_name)}\n\n"
            + t("msg.choose_date_range", lang)
        )

    @staticmethod
    def loading(report_type: str, start_date: date, end_date: date,
                step: int = 3, total_steps: int = 3,
                lang: str = DEFAULT_LANGUAGE) -> str:
        return (
            Messages._header(step, total_steps, "step.generating", lang)
            + f"{t('msg.report_type_label', lang)}: "
              f"{bold(report_type_label(report_type, lang))}\n"
            + f"{t('msg.date_range_label', lang)}: {bold(date_span(start_date, end_date))}\n\n"
            + t("msg.please_wait", lang)
        )

    @staticmethod
    def excel_preparing(start_date: date, end_date: date,
                        lang: str = DEFAULT_LANGUAGE) -> str:
        return t("msg.excel_preparing", lang, range=date_span(start_date, end_date))

    @staticmethod
    def excel_success(start_date: date, end_date: date,
                      lang: str = DEFAULT_LANGUAGE) -> str:
        return t("msg.excel_success", lang, range=date_span(start_date, end_date))

    @staticmethod
    def error(error_msg: str, lang: str = DEFAULT_LANGUAGE) -> str:
        return t("msg.report_error", lang, error=error_msg)

    @staticmethod
    def excel_error(error_msg: str = None, lang: str = DEFAULT_LANGUAGE) -> str:
        if error_msg:
            return t("msg.excel_error", lang, error=error_msg)
        return t("msg.excel_error_plain", lang)

    @staticmethod
    def custom_date_prompt(step_key: str, step_num: int, total_steps: int,
                           context: str = "", lang: str = DEFAULT_LANGUAGE) -> str:
        """`step_key` is an i18n key such as `pick.start_year`."""
        return (
            f"{bold(t('msg.custom_dates_title', lang))}\n\n"
            f"{italic(t('msg.step', lang, step=step_num, total=total_steps, name=t(step_key, lang)))}\n\n"
            f"{context}"
        )

    @staticmethod
    def top10_source_selection(lang: str = DEFAULT_LANGUAGE) -> str:
        return f"{bold(t('top10.title', lang))}\n\n{t('top10.choose_source', lang)}"

    @staticmethod
    def top10_change_source(start_date: date, end_date: date,
                            lang: str = DEFAULT_LANGUAGE) -> str:
        return (
            f"{bold(t('top10.title', lang))}\n\n"
            f"📅 {t('msg.date_range_label', lang)}: {date_span(start_date, end_date)}\n\n"
            f"{t('top10.choose_source', lang)}"
        )


def report_type_label(report_type: str, lang: str = DEFAULT_LANGUAGE) -> str:
    """"summary" → "📊 Зведений звіт". Unknown types pass through unchanged."""
    key = f"report_type.{report_type}"
    label = t(key, lang)
    return report_type if label == key else label


def date_range_label(range_key: str, lang: str = DEFAULT_LANGUAGE) -> str:
    key = f"daterange.{range_key}"
    label = t(key, lang)
    return range_key if label == key else label


def date_span(start_date: date, end_date: date) -> str:
    """`03.08.2026 – 09.08.2026`, in every language.

    Numeric because Ukrainian and Russian month names inflect — see
    `core.i18n.fmt_window` for the full reasoning.
    """
    return f"{start_date.strftime('%d.%m.%Y')} – {end_date.strftime('%d.%m.%Y')}"


# ─── Report Formatters ──────────────────────────────────────────────────────

class ReportFormatters:
    """Report formatting functions."""

    @staticmethod
    def format_summary(
        sales_by_source: Dict,
        order_counts: Dict,
        revenue_by_source: Dict,
        returns_by_source: Dict,
        total_orders: int,
        start_date: date,
        end_date: date,
        report_time: str,
        lang: str = DEFAULT_LANGUAGE,
    ) -> str:
        """Format summary sales report."""
        report = (
            f"{bold(t('summary.title', lang))}\n\n"
            f"📅 {bold(t('msg.date_range_label', lang))}: "
            f"{date_span(start_date, end_date)}\n"
            f"📈 {bold(t('summary.total_orders', lang))}: "
            f"{fmt_int(total_orders, lang)}\n\n"
            f"{bold(t('summary.by_source', lang))}\n"
        )

        # Sorted by total quantity, biggest source first.
        for src_id, products_dict in sorted(
            sales_by_source.items(), key=lambda x: sum(x[1].values()), reverse=True
        ):
            qty = sum(products_dict.values())
            name = SOURCE_MAPPING.get(int(src_id), src_id)
            order_count = order_counts.get(src_id, 0)
            revenue = revenue_by_source.get(src_id, 0)
            avg_check = revenue / order_count if order_count > 0 else 0

            report += f"\n{bold(name)}:\n"
            report += f"  • {t('summary.products', lang)}: {fmt_int(qty, lang)}\n"
            report += f"  • {t('summary.orders', lang)}: {fmt_int(order_count, lang)}\n"
            report += (f"  • {t('summary.avg_check', lang)}: "
                       f"{fmt_money(avg_check, lang)}\n")

            returns = returns_by_source.get(src_id, {"count": 0, "revenue": 0})
            if returns["count"] > 0:
                rate = (returns["count"] / order_count * 100) if order_count > 0 else 0
                report += (f"  • {t('summary.returns', lang)}: "
                           f"{returns['count']} ({rate:.1f}%)\n")

        report += f"\n📝 {italic(t('summary.generated_at', lang, time=report_time))}"
        return report

    @staticmethod
    def format_top10(
        top_products: List[Tuple[str, int, float]],
        source_name: str,
        emoji: str,
        total_quantity: int,
        report_time: str,
        lang: str = DEFAULT_LANGUAGE,
    ) -> str:
        """Format TOP-10 products report."""
        if total_quantity == 0:
            return (f"{emoji} {bold(source_name.upper())}: "
                    f"{t('top10.no_sales', lang)}")

        report = f"{emoji} {bold(source_name.upper())}\n"
        report += f"{'─' * 30}\n"
        report += (f"📦 {t('top10.total_sold', lang)}: "
                   f"{bold(fmt_int(total_quantity, lang))}\n\n")

        for i, (product_name, quantity, percentage) in enumerate(top_products, 1):
            display_name = (product_name[:57] + "…"
                            if len(product_name) > 60 else product_name)
            if i <= 3:
                report += (f"{MEDALS[i - 1]} {bold(str(quantity))} "
                           f"({percentage:.1f}%) — {display_name}\n\n")
            else:
                report += (f"{bold(f'{i}.')} {quantity} ({percentage:.1f}%) — "
                           f"{display_name}\n")
                if i < len(top_products):
                    report += "\n"

        report += f"\n{'─' * 30}\n"
        report += f"📝 {italic(report_time)}"
        return report

    @staticmethod
    def format_top10_header(title: str, start_date: date, end_date: date,
                            lang: str = DEFAULT_LANGUAGE) -> str:
        """Format TOP-10 report header."""
        return (
            f"{bold(title)}\n\n"
            f"📅 {bold(t('msg.date_range_label', lang))}: "
            f"{date_span(start_date, end_date)}\n\n"
            f"{t('msg.generating', lang)}"
        )


def get_period_type(start_date: date, end_date: date) -> str:
    """Determine period type: 'daily', 'weekly', 'monthly', or None."""
    days = (end_date - start_date).days + 1

    if days == 1:
        return "daily"
    elif days <= 7:
        return "weekly"
    elif days <= 31:
        return "monthly"
    return None


