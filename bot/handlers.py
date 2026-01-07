"""
Telegram bot handlers organized by functionality.

All 25 handlers from telegram_bot.py, reorganized and using new
keyboards and formatters modules to eliminate duplication.
"""
import logging
import calendar
import pytz
from datetime import datetime, timedelta, date
from telegram import Update
from telegram.ext import ContextTypes, ConversationHandler

from bot.config import (
    ConversationState,
    REPORT_TYPES,
    SOURCE_NAMES,
    SOURCE_EMOJIS,
    DEFAULT_TIMEZONE,
    TELEGRAM_MANAGER_IDS
)
from bot.keyboards import Keyboards
from bot.formatters import Messages, ReportFormatters, create_progress_indicator
from bot.services import ReportService

# Logger
logger = logging.getLogger(__name__)

# Global user data storage (will be replaced with proper state management later)
user_data = {}

# Global service instance (will be injected properly in main.py)
report_service: ReportService = None

# ═══════════════════════════════════════════════════════════════════════════
# COMMAND HANDLERS
# ═══════════════════════════════════════════════════════════════════════════

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Send a welcome message when /start is issued."""
    user = update.effective_user
    welcome_message = Messages.welcome(user.first_name)

    try:
        await update.message.reply_text(
            welcome_message,
            reply_markup=Keyboards.main_menu(),
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"Error sending welcome message: {e}")
        await update.message.reply_text(
            f"Welcome, {user.first_name}! Use /report to generate a sales report or /help for assistance."
        )

    return ConversationHandler.END


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a helpful message when /help is issued."""
    try:
        await update.message.reply_text(
            Messages.help_text(),
            reply_markup=Keyboards.help_menu(),
            parse_mode="HTML"
        )
    except Exception:
        await update.message.reply_text(
            "📊 KeyCRM Sales Report Bot 📊\n\n"
            "Available Commands:\n"
            "/report - Generate a sales report\n"
            "/cancel - Cancel the current operation\n"
            "/help - Show this help message"
        )


async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancel and end the conversation."""
    user_id = update.effective_user.id
    if user_id in user_data:
        del user_data[user_id]

    await update.message.reply_text(
        Messages.cancel(),
        reply_markup=Keyboards.cancel_operation(),
        parse_mode="HTML"
    )

    return ConversationHandler.END


async def command_button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle command buttons from menus (cmd_report, cmd_help, cmd_start)."""
    query = update.callback_query
    await query.answer()

    command = query.data.split('_')[1]

    if command == "report":
        return await report_command_from_callback(update, context)
    elif command == "help":
        await query.edit_message_text(
            Messages.help_text(),
            reply_markup=Keyboards.help_menu(),
            parse_mode="HTML"
        )
        return ConversationHandler.END
    elif command == "start":
        user = update.effective_user
        await query.edit_message_text(
            Messages.welcome(user.first_name),
            reply_markup=Keyboards.main_menu(),
            parse_mode="HTML"
        )
        return ConversationHandler.END

    return ConversationHandler.END


# ═══════════════════════════════════════════════════════════════════════════
# REPORT FLOW HANDLERS
# ═══════════════════════════════════════════════════════════════════════════

async def report_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Start the report generation process."""
    await update.message.reply_text(
        Messages.report_selection(),
        reply_markup=Keyboards.report_types(),
        parse_mode="HTML"
    )
    return ConversationState.SELECTING_REPORT_TYPE


async def report_command_from_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Start the report generation process from a callback query."""
    query = update.callback_query

    await query.edit_message_text(
        Messages.report_selection(),
        reply_markup=Keyboards.report_types(),
        parse_mode="HTML"
    )
    return ConversationState.SELECTING_REPORT_TYPE


async def report_type_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle the report type selection."""
    query = update.callback_query
    await query.answer()

    if query.data == "go_back":
        await query.edit_message_text(
            f"Operation canceled. Use /report to start again or select an option below:",
            reply_markup=Keyboards.cancel_operation()
        )
        return ConversationHandler.END

    user_id = update.effective_user.id
    selected_type = query.data.split('_')[-1]

    # Handle TOP-10 report type
    if selected_type == "top10":
        if user_id not in user_data:
            user_data[user_id] = {"report_type": selected_type}
        else:
            user_data[user_id]["report_type"] = selected_type

        await query.edit_message_text(
            Messages.top10_source_selection(),
            reply_markup=Keyboards.top10_sources(),
            parse_mode="HTML"
        )
        return ConversationState.SELECTING_TOP10_SOURCE

    # Initialize user data
    if user_id not in user_data:
        user_data[user_id] = {"report_type": selected_type}
    else:
        user_data[user_id]["report_type"] = selected_type

    # Ask for date range
    await query.edit_message_text(
        Messages.date_selection(selected_type),
        reply_markup=Keyboards.date_ranges(),
        parse_mode="HTML"
    )

    return ConversationState.SELECTING_DATE_RANGE


async def prepare_generate_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Prepare to generate the report based on selected options."""
    query = update.callback_query
    user_id = update.effective_user.id

    # Validate session data exists
    if user_id not in user_data or "start_date" not in user_data[user_id]:
        await query.edit_message_text(
            "⚠️ Session expired. Please start a new report with /report",
            parse_mode="HTML"
        )
        return ConversationHandler.END

    start_date = user_data[user_id]["start_date"]
    end_date = user_data[user_id]["end_date"]
    report_type = user_data[user_id]["report_type"]

    # Show loading message
    await query.edit_message_text(
        Messages.loading(report_type, start_date, end_date),
        parse_mode="HTML"
    )

    # Generate the appropriate report
    if report_type == "excel":
        return await generate_excel_report(update, context)
    elif report_type == "top10":
        return await generate_top10_report(update, context)
    else:
        return await generate_summary_report(update, context)


# ═══════════════════════════════════════════════════════════════════════════
# DATE SELECTION HANDLERS
# ═══════════════════════════════════════════════════════════════════════════

async def date_range_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle the date range selection."""
    query = update.callback_query
    await query.answer()

    if query.data == "back_to_report_type":
        await query.edit_message_text(
            Messages.report_selection(),
            reply_markup=Keyboards.report_types(),
            parse_mode="HTML"
        )
        return ConversationState.SELECTING_REPORT_TYPE

    if query.data == "back_to_source_selection":
        await query.edit_message_text(
            Messages.top10_source_selection(),
            reply_markup=Keyboards.top10_sources(),
            parse_mode="HTML"
        )
        return ConversationState.SELECTING_TOP10_SOURCE

    user_id = update.effective_user.id
    selected_range = query.data.split('_')[1]

    today = date.today()

    # Process predefined date ranges
    if selected_range == "today":
        user_data[user_id]["start_date"] = today
        user_data[user_id]["end_date"] = today
        return await prepare_generate_report(update, context)

    elif selected_range == "yesterday":
        yesterday = today - timedelta(days=1)
        user_data[user_id]["start_date"] = yesterday
        user_data[user_id]["end_date"] = yesterday
        return await prepare_generate_report(update, context)

    elif selected_range == "thisweek":
        start_date = today - timedelta(days=today.weekday())
        user_data[user_id]["start_date"] = start_date
        user_data[user_id]["end_date"] = today
        return await prepare_generate_report(update, context)

    elif selected_range == "thismonth":
        start_date = date(today.year, today.month, 1)
        user_data[user_id]["start_date"] = start_date
        user_data[user_id]["end_date"] = today
        return await prepare_generate_report(update, context)

    elif selected_range == "custom":
        # Start custom date selection
        current_year = datetime.now().year
        years = list(range(current_year - 2, current_year + 1))

        message = Messages.custom_date_prompt(
            "Select START year",
            1, 6,
            f"Please select the start year for your custom date range:"
        )

        await query.edit_message_text(
            message,
            reply_markup=Keyboards.year_picker(years, "back_to_date_range"),
            parse_mode="HTML"
        )
        return ConversationState.SELECTING_CUSTOM_START_YEAR

    return ConversationHandler.END


async def back_to_date_range(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Go back to date range selection."""
    query = update.callback_query
    user_id = update.effective_user.id
    report_type = user_data[user_id].get("report_type", "summary")

    await query.edit_message_text(
        Messages.date_selection(report_type),
        reply_markup=Keyboards.date_ranges(),
        parse_mode="HTML"
    )

    return ConversationState.SELECTING_DATE_RANGE


# ═══════════════════════════════════════════════════════════════════════════
# CUSTOM DATE PICKER HANDLERS (6 handlers for year/month/day selection)
# ═══════════════════════════════════════════════════════════════════════════

async def custom_start_year_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle custom start year selection."""
    query = update.callback_query
    await query.answer()

    if query.data == "back_to_date_range":
        return await back_to_date_range(update, context)

    user_id = update.effective_user.id
    selected_year = int(query.data.split('_')[-1])

    if user_id not in user_data:
        user_data[user_id] = {}

    user_data[user_id]["custom_start_year"] = selected_year

    message = Messages.custom_date_prompt(
        "Select START month",
        2, 6,
        f"Selected start year: <b>{selected_year}</b>\nNow select the start month:"
    )

    await query.edit_message_text(
        message,
        reply_markup=Keyboards.month_picker("back_to_custom_start_year"),
        parse_mode="HTML"
    )

    return ConversationState.SELECTING_CUSTOM_START_MONTH


async def custom_start_month_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle custom start month selection."""
    query = update.callback_query
    await query.answer()

    if query.data == "back_to_custom_start_year":
        current_year = datetime.now().year
        years = list(range(current_year - 2, current_year + 1))

        message = Messages.custom_date_prompt(
            "Select START year",
            1, 6,
            "Please select the start year:"
        )

        await query.edit_message_text(
            message,
            reply_markup=Keyboards.year_picker(years, "back_to_date_range"),
            parse_mode="HTML"
        )
        return ConversationState.SELECTING_CUSTOM_START_YEAR

    user_id = update.effective_user.id
    selected_month = int(query.data.split('_')[-1])
    user_data[user_id]["custom_start_month"] = selected_month

    selected_year = user_data[user_id]["custom_start_year"]
    month_name = calendar.month_name[selected_month]

    message = Messages.custom_date_prompt(
        "Select START day",
        3, 6,
        f"Selected start: <b>{month_name} {selected_year}</b>\nNow select the start day:"
    )

    await query.edit_message_text(
        message,
        reply_markup=Keyboards.day_picker(selected_year, selected_month, 1, "back_to_custom_start_month"),
        parse_mode="HTML"
    )

    return ConversationState.SELECTING_CUSTOM_START_DAY


async def custom_start_day_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle custom start day selection."""
    query = update.callback_query
    await query.answer()

    if query.data == "back_to_custom_start_month":
        user_id = update.effective_user.id
        selected_year = user_data[user_id]["custom_start_year"]

        message = Messages.custom_date_prompt(
            "Select START month",
            2, 6,
            f"Selected start year: <b>{selected_year}</b>\nNow select the start month:"
        )

        await query.edit_message_text(
            message,
            reply_markup=Keyboards.month_picker("back_to_custom_start_year"),
            parse_mode="HTML"
        )
        return ConversationState.SELECTING_CUSTOM_START_MONTH

    user_id = update.effective_user.id
    selected_day = int(query.data.split('_')[-1])

    # Save complete start date
    selected_year = user_data[user_id]["custom_start_year"]
    selected_month = user_data[user_id]["custom_start_month"]
    user_data[user_id]["start_date"] = datetime(selected_year, selected_month, selected_day).date()

    # Move to end date selection
    current_year = datetime.now().year
    if selected_year == current_year:
        # Skip year selection, go to month
        user_data[user_id]["custom_end_year"] = current_year
        start_date = user_data[user_id]["start_date"]

        message = Messages.custom_date_prompt(
            "Select END month",
            4, 5,
            f"Start date: <b>{start_date.strftime('%Y-%m-%d')}</b>\n"
            f"End year: <b>{current_year}</b> (Current year)\n\n"
            f"Now select the end month:"
        )

        await query.edit_message_text(
            message,
            reply_markup=Keyboards.month_picker_range(start_date.month, "back_to_custom_start_day", "custom_end_month"),
            parse_mode="HTML"
        )
        return ConversationState.SELECTING_CUSTOM_END_MONTH
    else:
        # Show year selection for end date
        years = list(range(selected_year, current_year + 1))
        start_date = user_data[user_id]["start_date"]

        message = Messages.custom_date_prompt(
            "Select END year",
            4, 6,
            f"Start date: <b>{start_date.strftime('%Y-%m-%d')}</b>\n\nNow select the end year:"
        )

        await query.edit_message_text(
            message,
            reply_markup=Keyboards.end_year_picker(years, "back_to_custom_start_day"),
            parse_mode="HTML"
        )
        return ConversationState.SELECTING_CUSTOM_END_YEAR


async def custom_end_year_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle custom end year selection."""
    query = update.callback_query
    await query.answer()

    if query.data == "back_to_custom_start_day":
        user_id = update.effective_user.id
        selected_year = user_data[user_id]["custom_start_year"]
        selected_month = user_data[user_id]["custom_start_month"]
        month_name = calendar.month_name[selected_month]

        message = Messages.custom_date_prompt(
            "Select START day",
            3, 6,
            f"Selected start: <b>{month_name} {selected_year}</b>\nNow select the start day:"
        )

        await query.edit_message_text(
            message,
            reply_markup=Keyboards.day_picker(selected_year, selected_month, 1, "back_to_custom_start_month"),
            parse_mode="HTML"
        )
        return ConversationState.SELECTING_CUSTOM_START_DAY

    user_id = update.effective_user.id
    selected_year = int(query.data.split('_')[-1])
    user_data[user_id]["custom_end_year"] = selected_year

    start_date = user_data[user_id]["start_date"]
    start_month = 1
    if selected_year == start_date.year:
        start_month = start_date.month

    message = Messages.custom_date_prompt(
        "Select END month",
        5, 6,
        f"Start date: <b>{start_date.strftime('%Y-%m-%d')}</b>\n"
        f"End year: <b>{selected_year}</b>\n\n"
        f"Now select the end month:"
    )

    await query.edit_message_text(
        message,
        reply_markup=Keyboards.month_picker_range(start_month, "back_to_custom_end_year", "custom_end_month"),
        parse_mode="HTML"
    )

    return ConversationState.SELECTING_CUSTOM_END_MONTH


async def custom_end_month_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle custom end month selection."""
    query = update.callback_query
    await query.answer()

    if query.data == "back_to_custom_end_year":
        user_id = update.effective_user.id
        start_date = user_data[user_id]["start_date"]
        current_year = datetime.now().year
        years = list(range(start_date.year, current_year + 1))

        message = Messages.custom_date_prompt(
            "Select END year",
            4, 6,
            f"Start date: <b>{start_date.strftime('%Y-%m-%d')}</b>\n\nNow select the end year:"
        )

        await query.edit_message_text(
            message,
            reply_markup=Keyboards.end_year_picker(years, "back_to_custom_start_day"),
            parse_mode="HTML"
        )
        return ConversationState.SELECTING_CUSTOM_END_YEAR

    user_id = update.effective_user.id
    selected_month = int(query.data.split('_')[-1])
    user_data[user_id]["custom_end_month"] = selected_month

    selected_year = user_data[user_id]["custom_end_year"]
    start_date = user_data[user_id]["start_date"]
    start_day = 1
    if selected_year == start_date.year and selected_month == start_date.month:
        start_day = start_date.day

    month_name = calendar.month_name[selected_month]
    message = Messages.custom_date_prompt(
        "Select END day",
        6, 6,
        f"Start date: <b>{start_date.strftime('%Y-%m-%d')}</b>\n"
        f"End date so far: <b>{month_name} {selected_year}</b>\n\n"
        f"Now select the end day:"
    )

    await query.edit_message_text(
        message,
        reply_markup=Keyboards.day_picker(selected_year, selected_month, start_day, "back_to_custom_end_month", "custom_end_day"),
        parse_mode="HTML"
    )

    return ConversationState.SELECTING_CUSTOM_END_DAY


async def custom_end_day_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle custom end day selection."""
    query = update.callback_query
    await query.answer()

    if query.data == "back_to_custom_end_month":
        user_id = update.effective_user.id
        start_date = user_data[user_id]["start_date"]
        selected_year = user_data[user_id]["custom_end_year"]
        start_month = 1
        if selected_year == start_date.year:
            start_month = start_date.month

        message = Messages.custom_date_prompt(
            "Select END month",
            5, 6,
            f"Start date: <b>{start_date.strftime('%Y-%m-%d')}</b>\n"
            f"End year: <b>{selected_year}</b>\n\n"
            f"Now select the end month:"
        )

        await query.edit_message_text(
            message,
            reply_markup=Keyboards.month_picker_range(start_month, "back_to_custom_end_year", "custom_end_month"),
            parse_mode="HTML"
        )
        return ConversationState.SELECTING_CUSTOM_END_MONTH

    user_id = update.effective_user.id
    selected_day = int(query.data.split('_')[-1])

    # Save complete end date
    selected_year = user_data[user_id]["custom_end_year"]
    selected_month = user_data[user_id]["custom_end_month"]
    user_data[user_id]["end_date"] = datetime(selected_year, selected_month, selected_day).date()

    return await prepare_generate_report(update, context)


# ═══════════════════════════════════════════════════════════════════════════
# TOP-10 HANDLERS
# ═══════════════════════════════════════════════════════════════════════════

async def top10_source_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle TOP-10 source selection."""
    query = update.callback_query
    await query.answer()

    if query.data == "back_to_report_type":
        return await report_command_from_callback(update, context)

    user_id = update.effective_user.id
    source_selection = query.data.split('_')[-1]

    user_data[user_id]["top10_source"] = source_selection

    # Proceed to date range selection
    source_name = SOURCE_NAMES.get(source_selection, "Unknown")

    await query.edit_message_text(
        Messages.top10_date_selection(source_name),
        reply_markup=Keyboards.date_ranges("back_to_source_selection"),
        parse_mode="HTML"
    )
    return ConversationState.SELECTING_DATE_RANGE


async def change_top10_source(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Change TOP-10 source while keeping the same date range."""
    query = update.callback_query
    await query.answer()

    user_id = update.effective_user.id

    if user_id in user_data and "start_date" in user_data[user_id]:
        start_date = user_data[user_id]["start_date"]
        end_date = user_data[user_id]["end_date"]

        await query.edit_message_text(
            Messages.top10_change_source(start_date, end_date),
            reply_markup=Keyboards.top10_quick_source_picker(),
            parse_mode="HTML"
        )
    else:
        await query.edit_message_text("Session expired. Please start a new report.", parse_mode="HTML")

    return ConversationHandler.END


async def quick_top10_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Generate TOP-10 for selected source using existing date range."""
    query = update.callback_query
    await query.answer()

    user_id = update.effective_user.id
    source_selection = query.data.split('_')[-1]

    user_data[user_id]["top10_source"] = source_selection

    return await generate_top10_report(update, context)


async def generate_top10_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Generate TOP-10 products report for selected source(s)."""
    query = update.callback_query
    user_id = update.effective_user.id

    # Validate session data exists
    if user_id not in user_data or "start_date" not in user_data[user_id]:
        await query.edit_message_text(
            "⚠️ Session expired. Please start a new report with /report",
            parse_mode="HTML"
        )
        return ConversationHandler.END

    start_date = user_data[user_id]["start_date"]
    end_date = user_data[user_id]["end_date"]
    source_selection = user_data[user_id].get("top10_source", "all")

    try:
        now = datetime.now(pytz.timezone(DEFAULT_TIMEZONE))
        report_time = now.strftime("%Y-%m-%d %H:%M:%S")

        # Define sources
        all_sources = [
            (1, "Instagram", "📸"),
            (4, "Shopify", "🛍️"),
            (2, "Telegram", "✈️")
        ]

        # Determine which sources to process
        if source_selection == "all":
            sources_to_process = all_sources
            title = "🏆 TOP-10 PRODUCTS BY SOURCE"
        else:
            source_id = int(source_selection)
            sources_to_process = [s for s in all_sources if s[0] == source_id]
            source_name = sources_to_process[0][1] if sources_to_process else "Unknown"
            title = f"🏆 TOP-10 PRODUCTS - {source_name.upper()}"

        # Header message
        await query.edit_message_text(
            ReportFormatters.format_top10_header(title, start_date, end_date),
            parse_mode="HTML"
        )

        # Process each source
        for source_id, source_name, emoji in sources_to_process:
            top_products, total_quantity = report_service.calculate_top10_products(
                target_date=(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')),
                source_id=source_id,
                limit=10,
                tz_name=DEFAULT_TIMEZONE
            )

            report = ReportFormatters.format_top10(
                top_products,
                source_name,
                emoji,
                total_quantity,
                report_time
            )

            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=report,
                parse_mode="HTML"
            )

        # Final message
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="✅ Report generated successfully!",
            reply_markup=Keyboards.top10_post_report(),
            parse_mode="HTML"
        )

    except Exception as e:
        logger.error(f"Error generating TOP-10 report: {e}")
        await query.edit_message_text(f"⚠️ Error: {str(e)}", parse_mode="HTML")

    return ConversationHandler.END


# ═══════════════════════════════════════════════════════════════════════════
# REPORT GENERATION HANDLERS
# ═══════════════════════════════════════════════════════════════════════════

async def generate_summary_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Generate the summary sales report."""
    query = update.callback_query
    user_id = update.effective_user.id

    # Validate session data exists
    if user_id not in user_data or "start_date" not in user_data[user_id]:
        await query.edit_message_text(
            "⚠️ Session expired. Please start a new report with /report",
            parse_mode="HTML"
        )
        return ConversationHandler.END

    start_date = user_data[user_id]["start_date"]
    end_date = user_data[user_id]["end_date"]

    try:
        # Get sales data
        sales_by_source, order_counts, total_orders, revenue_by_source, returns_by_source = (
            report_service.aggregate_sales_data(
                target_date=(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')),
                tz_name=DEFAULT_TIMEZONE,
                telegram_manager_ids=TELEGRAM_MANAGER_IDS
            )
        )

        # Get report timestamp
        now = datetime.now(pytz.timezone(DEFAULT_TIMEZONE))
        report_time = now.strftime("%Y-%m-%d %H:%M:%S")

        # Format report
        report = ReportFormatters.format_summary(
            sales_by_source,
            order_counts,
            revenue_by_source,
            returns_by_source,
            total_orders,
            start_date,
            end_date,
            report_time
        )

        # Send report
        await query.edit_message_text(
            report,
            reply_markup=Keyboards.post_report_actions(include_summary=False),
            parse_mode="HTML"
        )

    except Exception as e:
        logger.error(f"Error generating report: {e}")
        await query.edit_message_text(
            Messages.error(str(e)),
            reply_markup=Keyboards.error_retry(),
            parse_mode="HTML"
        )

        if user_id in user_data:
            del user_data[user_id]

    return ConversationHandler.END


async def generate_excel_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Generate and send an Excel sales report."""
    query = update.callback_query
    user_id = update.effective_user.id

    # Validate session data exists
    if user_id not in user_data or "start_date" not in user_data[user_id]:
        await query.edit_message_text(
            "⚠️ Session expired. Please start a new report with /report",
            parse_mode="HTML"
        )
        return ConversationHandler.END

    start_date = user_data[user_id]["start_date"]
    end_date = user_data[user_id]["end_date"]

    # Get bot token from context
    bot_token = context.bot.token
    chat_id = update.effective_chat.id

    try:
        # Show preparing message
        await query.edit_message_text(
            Messages.excel_preparing(start_date, end_date),
            parse_mode="HTML"
        )

        # Generate and send Excel
        success = report_service.generate_excel_report(
            target_date=(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')),
            bot_token=bot_token,
            chat_id=chat_id,
            tz_name=DEFAULT_TIMEZONE,
            telegram_manager_ids=TELEGRAM_MANAGER_IDS
        )

        if success:
            await query.edit_message_text(
                Messages.excel_success(start_date, end_date),
                reply_markup=Keyboards.post_report_actions(include_excel=False),
                parse_mode="HTML"
            )
        else:
            await query.edit_message_text(
                Messages.excel_error(),
                reply_markup=Keyboards.try_again_or_convert(),
                parse_mode="HTML"
            )

    except Exception as e:
        logger.error(f"Error generating Excel report: {e}")
        await query.edit_message_text(
            Messages.excel_error(str(e)),
            reply_markup=Keyboards.try_again_or_convert(),
            parse_mode="HTML"
        )

        if user_id in user_data:
            del user_data[user_id]

    return ConversationHandler.END


# ═══════════════════════════════════════════════════════════════════════════
# FORMAT CONVERSION HANDLERS
# ═══════════════════════════════════════════════════════════════════════════

async def convert_report_format(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Convert between report formats (Summary/Excel/TOP-10) using existing date range."""
    query = update.callback_query
    await query.answer()

    conversion_type = query.data.split('_')[-1]
    user_id = update.effective_user.id

    # Check if we have previous date selection
    if user_id in user_data and "start_date" in user_data[user_id] and "end_date" in user_data[user_id]:
        user_data[user_id]["report_type"] = conversion_type

        # Generate the new format (prepare_generate_report will show loading message)
        return await prepare_generate_report(update, context)
    else:
        # No previous date range, need to start from scratch
        await query.edit_message_text(
            f"Please start a new report to generate a {conversion_type} report.",
            reply_markup=Keyboards.cancel_operation(),
            parse_mode="HTML"
        )
        return ConversationHandler.END


# ═══════════════════════════════════════════════════════════════════════════
# QUICK REPORT HANDLERS (for format conversion shortcuts)
# ═══════════════════════════════════════════════════════════════════════════

async def quick_report_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle quick report generation from format conversion."""
    query = update.callback_query
    await query.answer()

    # Parse callback data
    parts = query.data.split('_')
    report_type = parts[1]  # summary, excel, or top10
    date_range = parts[2]  # today, yesterday, thisweek, thismonth

    user_id = update.effective_user.id

    # Initialize user data
    if user_id not in user_data:
        user_data[user_id] = {}

    user_data[user_id]["report_type"] = report_type

    # Process date range
    today = date.today()

    if date_range == "today":
        user_data[user_id]["start_date"] = today
        user_data[user_id]["end_date"] = today
    elif date_range == "yesterday":
        yesterday = today - timedelta(days=1)
        user_data[user_id]["start_date"] = yesterday
        user_data[user_id]["end_date"] = yesterday
    elif date_range == "thisweek":
        start_date = today - timedelta(days=today.weekday())
        user_data[user_id]["start_date"] = start_date
        user_data[user_id]["end_date"] = today
    elif date_range == "thismonth":
        start_date = date(today.year, today.month, 1)
        user_data[user_id]["start_date"] = start_date
        user_data[user_id]["end_date"] = today

    # Generate report
    return await prepare_generate_report(update, context)
