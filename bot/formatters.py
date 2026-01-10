"""
Message formatters and text utilities for the Telegram bot.

Contains text formatting helpers, message templates, and report formatters.
"""
import logging
from datetime import date
from typing import Dict, List, Tuple
from bot.config import REPORT_TYPES, SOURCE_MAPPING, MEDALS, VERSION, REVENUE_MILESTONES

logger = logging.getLogger(__name__)

# Telegram message length limit
TELEGRAM_MAX_MESSAGE_LENGTH = 4096
TELEGRAM_SAFE_MESSAGE_LENGTH = 3500  # Leave room for formatting


def truncate_message(message: str, max_length: int = TELEGRAM_SAFE_MESSAGE_LENGTH) -> str:
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
    return truncated + "\n\n⚠️ <i>Message truncated due to length...</i>"


def split_message(message: str, max_length: int = TELEGRAM_SAFE_MESSAGE_LENGTH) -> List[str]:
    """
    Split a long message into multiple parts.

    Args:
        message: The message to split
        max_length: Maximum length per part

    Returns:
        List of message parts
    """
    if len(message) <= max_length:
        return [message]

    parts = []
    lines = message.split('\n')
    current_part = ""

    for line in lines:
        # If adding this line would exceed limit, start a new part
        if len(current_part) + len(line) + 1 > max_length:
            if current_part:
                parts.append(current_part.rstrip())
            current_part = line + '\n'
        else:
            current_part += line + '\n'

    # Add the last part
    if current_part:
        parts.append(current_part.rstrip())

    logger.info(f"Message split into {len(parts)} parts")
    return parts


# ─── Text Formatting Helpers ────────────────────────────────────────────────

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
    """Standard message templates."""

    @staticmethod
    def welcome(username: str) -> str:
        """Generate welcome message."""
        return (
            f"👋 {bold('Welcome, ' + username)}! \n\n"
            f"I'm your {bold('KoreanStory Sales Report')} assistant. I can help you generate detailed sales reports "
            f"and analytics.\n\n"
            f"🚀 {italic('What would you like to do?')}"
        )

    @staticmethod
    def help_text() -> str:
        """Generate help message."""
        return (
            f"{bold('📊 KoreanStory Sales Report Bot 📊')}\n\n"
            f"{bold('Available Commands:')}\n"
            f"• /report - Generate a sales report\n"
            f"• /search - Search orders by ID/name/phone\n"
            f"• /settings - Configure preferences\n"
            f"• /dashboard - Open web dashboard\n"
            f"• /cancel - Cancel current operation\n"
            f"• /help - Show this help message\n\n"
            f"{bold('Report Types:')}\n"
            f"📝 {bold('Summary')} - Sales overview by source\n"
            f"📊 {bold('Excel')} - Detailed Excel report\n"
            f"🏆 {bold('TOP-10')} - Best-selling products\n\n"
            f"{bold('Web Dashboard:')}\n"
            f"📈 http://108.130.86.30:8080\n\n"
            f"{italic(f'Version {VERSION}')}"
        )

    @staticmethod
    def cancel() -> str:
        """Generate cancellation message."""
        return (
            f"{bold('🛑 Operation Cancelled')}\n\n"
            f"I've cancelled the current operation as requested.\n"
            f"What would you like to do next?"
        )

    @staticmethod
    def report_selection(step: int = 1, total_steps: int = 3) -> str:
        """Generate report type selection message."""
        progress = create_progress_indicator(step, total_steps)
        return (
            f"{bold('📊 Sales Report Generator')}\n\n"
            f"{progress} {italic(f'Step {step} of {total_steps}: Select Report Type')}\n\n"
            f"Please choose the type of report you'd like to generate:"
        )

    @staticmethod
    def date_selection(report_type: str, step: int = 2, total_steps: int = 3) -> str:
        """Generate date range selection message."""
        progress = create_progress_indicator(step, total_steps)
        return (
            f"{bold('📊 Sales Report Generator')}\n\n"
            f"{progress} {italic(f'Step {step} of {total_steps}: Select Date Range')}\n\n"
            f"Selected report type: {bold(REPORT_TYPES.get(report_type, report_type))}\n\n"
            f"Now, please select the date range for your report:"
        )

    @staticmethod
    def top10_date_selection(source_name: str, step: int = 2, total_steps: int = 3) -> str:
        """Generate date selection message for TOP-10 report."""
        progress = create_progress_indicator(step, total_steps)
        return (
            f"{bold('📊 Sales Report Generator')}\n\n"
            f"{progress} {italic(f'Step {step} of {total_steps}: Select Date Range')}\n\n"
            f"Report: {bold('TOP-10 Products')}\n"
            f"Source: {bold(source_name)}\n\n"
            f"Now, please select the date range:"
        )

    @staticmethod
    def loading(report_type: str, start_date: date, end_date: date, step: int = 3, total_steps: int = 3) -> str:
        """Generate loading message."""
        progress = create_progress_indicator(step, total_steps)
        return (
            f"{bold('📊 Sales Report Generator')}\n\n"
            f"{progress} {italic(f'Step {step} of {total_steps}: Generating Report')}\n\n"
            f"Report type: {bold(REPORT_TYPES.get(report_type, report_type))}\n"
            f"Date range: {bold(start_date.strftime('%Y-%m-%d'))} to {bold(end_date.strftime('%Y-%m-%d'))}\n\n"
            f"⏳ {italic('Please wait while I generate your report...')}"
        )

    @staticmethod
    def excel_preparing(start_date: date, end_date: date) -> str:
        """Generate Excel file preparation message."""
        return (
            f"{bold('📑 Preparing Excel Report')}\n\n"
            f"📅 {bold('Date Range')}: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}\n\n"
            f"⏳ {italic('Creating your Excel file...')}\n"
            f"This may take a moment depending on the amount of data."
        )

    @staticmethod
    def excel_success(start_date: date, end_date: date) -> str:
        """Generate Excel success message."""
        return (
            f"{bold('✅ Excel Report Generated!')}\n\n"
            f"📅 {bold('Date Range')}: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}\n\n"
            f"📎 Your Excel file has been sent as a separate message.\n"
            f"📊 {italic('What would you like to do next?')}"
        )

    @staticmethod
    def error(error_msg: str) -> str:
        """Generate error message."""
        return (
            f"{bold('⚠️ Error Generating Report')}\n\n"
            f"I encountered a problem while generating your report:\n"
            f"{italic(error_msg)}\n\n"
            f"Please try again with a different date range or contact support if the issue persists."
        )

    @staticmethod
    def excel_error(error_msg: str = None) -> str:
        """Generate Excel error message."""
        if error_msg:
            return (
                f"{bold('⚠️ Excel Report Error')}\n\n"
                f"I encountered an issue while generating your Excel report:\n"
                f"{italic(error_msg)}\n\n"
                f"Would you like to try again or generate a summary report instead?"
            )
        else:
            return (
                f"{bold('⚠️ Excel Report Error')}\n\n"
                f"I was unable to generate your Excel report.\n\n"
                f"Would you like to try again or generate a summary report instead?"
            )

    @staticmethod
    def custom_date_prompt(step_name: str, step_num: int, total_steps: int, context: str = "") -> str:
        """Generate custom date selection prompt."""
        return (
            f"{bold('📊 Custom Date Selection')}\n\n"
            f"{italic(f'Step {step_num} of {total_steps}: {step_name}')}\n\n"
            f"{context}"
        )

    @staticmethod
    def top10_source_selection() -> str:
        """Generate TOP-10 source selection message."""
        return (
            f"{bold('🏆 TOP-10 Products Report')}\n\n"
            f"{italic('Select the source to view TOP-10 products:')}"
        )

    @staticmethod
    def top10_change_source(start_date: date, end_date: date) -> str:
        """Generate TOP-10 change source message."""
        return (
            f"{bold('🏆 TOP-10 Products Report')}\n\n"
            f"📅 Date: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}\n\n"
            f"{italic('Select a source to view TOP-10 products:')}"
        )


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
        report_time: str
    ) -> str:
        """Format summary sales report."""
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

        report = (
            f"{bold('📊 SALES SUMMARY REPORT')}\n\n"
            f"📅 {bold('Date Range')}: {start_date_str} to {end_date_str}\n"
            f"📈 {bold('Total Orders')}: {total_orders}\n\n"
            f"{bold('📦 TOTAL Products by Source')}\n"
        )

        # Add data for each source - sort by total quantity (sum of all products)
        for src_id, products_dict in sorted(sales_by_source.items(), key=lambda x: sum(x[1].values()), reverse=True):
            qty = sum(products_dict.values())
            name = SOURCE_MAPPING.get(int(src_id), src_id)
            order_count = order_counts.get(src_id, 0)
            revenue = revenue_by_source.get(src_id, 0)
            avg_check = revenue / order_count if order_count > 0 else 0

            report += f"\n{bold(name)}:\n"
            report += f"  • Products: {qty}\n"
            report += f"  • Orders: {order_count}\n"
            report += f"  • Avg Check: {avg_check:.2f} UAH\n"

            # Returns data
            returns = returns_by_source.get(src_id, {"count": 0, "revenue": 0})
            if returns["count"] > 0:
                return_rate = (returns["count"] / order_count * 100) if order_count > 0 else 0
                report += f"  • Returns/Canceled: {returns['count']} ({return_rate:.1f}%)\n"

        # Add footer
        report += f"\n📝 {italic(f'Report generated on {report_time}')}"

        return report

    @staticmethod
    def format_top10(
        top_products: List[Tuple[str, int, float]],
        source_name: str,
        emoji: str,
        total_quantity: int,
        report_time: str
    ) -> str:
        """Format TOP-10 products report."""
        if total_quantity == 0:
            return f"{emoji} {bold(source_name.upper())}: {italic('No sales in this period')}"

        report = f"{emoji} {bold(source_name.upper())}\n"
        report += f"{'─' * 30}\n"
        report += f"📦 Total Sold: {bold(str(total_quantity))}\n\n"

        for i, (product_name, quantity, percentage) in enumerate(top_products, 1):
            # Truncate long product names
            if len(product_name) > 60:
                display_name = product_name[:57] + "..."
            else:
                display_name = product_name

            if i <= 3:
                medal = MEDALS[i - 1]
                report += f"{medal} {bold(str(quantity))} ({percentage:.1f}%) - {display_name}\n\n"
            else:
                report += f"{bold(f'{i}.')} {quantity} ({percentage:.1f}%) - {display_name}\n"
                if i < len(top_products):
                    report += "\n"

        report += f"\n{'─' * 30}\n"
        report += f"📝 {italic(report_time)}"

        return report

    @staticmethod
    def format_top10_header(title: str, start_date: date, end_date: date) -> str:
        """Format TOP-10 report header."""
        return (
            f"{bold(title)}\n\n"
            f"📅 {bold('Date Range')}: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}\n\n"
            f"⏳ {italic('Generating report...')}"
        )


def get_period_type(start_date: date, end_date: date) -> str:
    """
    Determine period type based on date range.

    Returns: 'daily', 'weekly', 'monthly', or None
    """
    days = (end_date - start_date).days + 1

    if days == 1:
        return "daily"
    elif days <= 7:
        return "weekly"
    elif days <= 31:
        return "monthly"
    return None


def check_milestone(total_revenue: float, start_date: date, end_date: date) -> str | None:
    """
    Check if revenue hits a milestone and return congratulations message.

    Args:
        total_revenue: Total revenue for the period
        start_date: Start date of the report
        end_date: End date of the report

    Returns:
        Congratulations message if milestone hit, None otherwise
    """
    period_type = get_period_type(start_date, end_date)

    if not period_type or period_type not in REVENUE_MILESTONES:
        return None

    # Find the highest milestone reached
    highest_milestone = None
    for milestone in REVENUE_MILESTONES[period_type]:
        if total_revenue >= milestone["amount"]:
            highest_milestone = milestone

    if not highest_milestone:
        return None

    # Format amount for display
    amount = highest_milestone["amount"]
    if amount >= 1000000:
        amount_text = f"₴{amount / 1000000:.1f}M"
    else:
        amount_text = f"₴{amount / 1000:.0f}K"

    emoji = highest_milestone["emoji"]
    message = highest_milestone["message"]

    return (
        f"\n\n{'🎊' * 10}\n\n"
        f"{emoji} {bold('MILESTONE REACHED!')} {emoji}\n\n"
        f"🏆 {bold(message)}\n\n"
        f"💰 Revenue: {bold(amount_text)}\n\n"
        f"{'🎊' * 10}"
    )
