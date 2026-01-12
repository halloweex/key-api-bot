"""
Telegram bot handlers organized by functionality.

All 25 handlers from telegram_bot.py, reorganized and using new
keyboards and formatters modules to eliminate duplication.
"""
import asyncio
import logging
import calendar
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from typing import Dict, Any, Optional
from telegram import Update
from telegram.ext import ContextTypes, ConversationHandler

from functools import wraps
from bot.config import (
    ConversationState,
    REPORT_TYPES,
    SOURCE_NAMES,
    SOURCE_EMOJIS,
    DEFAULT_TIMEZONE,
    TELEGRAM_MANAGER_IDS,
    TOP10_SOURCES,
    get_year_choices,
    is_admin,
    ADMIN_USER_IDS,
    DASHBOARD_URL,
    VERSION,
    REVENUE_MILESTONES
)
from bot import database
from bot.keyboards import Keyboards, ReplyKeyboards
from bot.formatters import Messages, ReportFormatters, create_progress_indicator, truncate_message, check_milestone
from bot.services import ReportService, KeyCRMAPIError, ReportGenerationError

# Logger
logger = logging.getLogger(__name__)

# Session timeout in minutes
SESSION_TIMEOUT_MINUTES = 30

# ═══════════════════════════════════════════════════════════════════════════
# AUTHORIZATION
# ═══════════════════════════════════════════════════════════════════════════

ACCESS_DENIED_MESSAGE = (
    "🔒 <b>Access Denied</b>\n\n"
    "Your access request was denied.\n"
    "Please contact the administrator."
)

ACCESS_FROZEN_MESSAGE = (
    "🚫 <b>Access Frozen</b>\n\n"
    "Your account has been frozen due to multiple denied requests.\n"
    "Please contact the administrator directly."
)

ACCESS_PENDING_MESSAGE = (
    "⏳ <b>Access Pending</b>\n\n"
    "Your access request is being reviewed.\n"
    "Please wait for admin approval."
)

REQUEST_ACCESS_MESSAGE = (
    "🔐 <b>Access Required</b>\n\n"
    "This bot requires authorization.\n"
    "Click the button below to request access."
)


def authorized(func):
    """Decorator to check if user is authorized."""
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user = update.effective_user
        if not user:
            return ConversationHandler.END

        # Admins always have access - auto-approve them in DB too
        if is_admin(user.id):
            # Ensure admin is in approved users table
            if not database.is_user_authorized(user.id):
                database.request_access(user.id, user.username, user.first_name, user.last_name)
                database.approve_user(user.id, user.id)
                logger.info(f"Auto-approved admin {user.id}")
            return await func(update, context, *args, **kwargs)

        # Check authorization status
        auth_status = database.get_user_auth_status(user.id)

        if not auth_status:
            # New user - show request access prompt
            logger.info(f"New user {user.id} (@{user.username}) - showing access request")

            keyboard = [[
                InlineKeyboardButton("🔑 Request Access", callback_data="auth_request_access")
            ]]

            if update.callback_query:
                await update.callback_query.answer()
                await update.callback_query.edit_message_text(
                    REQUEST_ACCESS_MESSAGE,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="HTML"
                )
            elif update.message:
                await update.message.reply_text(
                    REQUEST_ACCESS_MESSAGE,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="HTML"
                )
            return ConversationHandler.END

        if auth_status['status'] == database.STATUS_PENDING:
            logger.info(f"User {user.id} has pending request")
            if update.callback_query:
                await update.callback_query.answer("Your request is pending", show_alert=True)
            elif update.message:
                await update.message.reply_text(ACCESS_PENDING_MESSAGE, parse_mode="HTML")
            return ConversationHandler.END

        if auth_status['status'] == database.STATUS_FROZEN:
            logger.warning(f"Frozen user {user.id} attempted access")
            if update.callback_query:
                await update.callback_query.answer("Account frozen", show_alert=True)
            elif update.message:
                await update.message.reply_text(ACCESS_FROZEN_MESSAGE, parse_mode="HTML")
            return ConversationHandler.END

        if auth_status['status'] == database.STATUS_DENIED:
            logger.warning(f"Denied user {user.id} attempted access")

            keyboard = [[
                InlineKeyboardButton("🔄 Request Again", callback_data="auth_request_again")
            ]]

            if update.callback_query:
                await update.callback_query.answer()
                await update.callback_query.edit_message_text(
                    ACCESS_DENIED_MESSAGE + "\n\nYou can request access again:",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="HTML"
                )
            elif update.message:
                await update.message.reply_text(
                    ACCESS_DENIED_MESSAGE + "\n\nYou can request access again:",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="HTML"
                )
            return ConversationHandler.END

        # User is approved - update last activity
        database.update_last_activity(user.id)
        return await func(update, context, *args, **kwargs)
    return wrapper


# Need to import here to avoid circular imports
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

# Global user data storage with session management
import threading
_user_data: Dict[int, Dict[str, Any]] = {}
_user_data_lock = threading.RLock()

# Global service instance (injected in main.py)
report_service: Optional[ReportService] = None


def get_user_session(user_id: int) -> Optional[Dict[str, Any]]:
    """Get user session if it exists and is not expired."""
    with _user_data_lock:
        if user_id not in _user_data:
            return None

        session = _user_data[user_id]
        created_at = session.get("_created_at")

        if created_at:
            elapsed = datetime.now() - created_at
            if elapsed > timedelta(minutes=SESSION_TIMEOUT_MINUTES):
                # Session expired, clean it up
                logger.info(f"Session expired for user {user_id} after {elapsed}")
                del _user_data[user_id]
                return None

        # Return a copy to avoid race conditions
        return dict(session)


def create_user_session(user_id: int, initial_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Create a new user session with timestamp."""
    session = {"_created_at": datetime.now()}
    if initial_data:
        session.update(initial_data)
    with _user_data_lock:
        _user_data[user_id] = session
    return session


def update_user_session(user_id: int, data: Dict[str, Any]) -> Dict[str, Any]:
    """Update user session, creating if needed."""
    with _user_data_lock:
        if user_id not in _user_data:
            session = {"_created_at": datetime.now()}
            session.update(data)
            _user_data[user_id] = session
            return session

        _user_data[user_id].update(data)
        return dict(_user_data[user_id])


def cleanup_expired_sessions() -> int:
    """Remove all expired sessions. Returns count of removed sessions."""
    now = datetime.now()
    expired_users = []

    with _user_data_lock:
        for user_id, session in _user_data.items():
            created_at = session.get("_created_at")
            if created_at and (now - created_at) > timedelta(minutes=SESSION_TIMEOUT_MINUTES):
                expired_users.append(user_id)

        for user_id in expired_users:
            del _user_data[user_id]

    if expired_users:
        logger.info(f"Cleaned up {len(expired_users)} expired sessions")

    return len(expired_users)


# Backward compatibility alias
user_data = _user_data


def calculate_date_range(range_name: str) -> tuple[date, date]:
    """
    Calculate start and end dates for a given range name.

    Args:
        range_name: One of 'today', 'yesterday', 'thisweek', 'thismonth'

    Returns:
        Tuple of (start_date, end_date)
    """
    today = date.today()

    if range_name == "today":
        return today, today
    elif range_name == "yesterday":
        yesterday = today - timedelta(days=1)
        return yesterday, yesterday
    elif range_name == "thisweek":
        start = today - timedelta(days=today.weekday())
        return start, today
    elif range_name == "thismonth":
        start = date(today.year, today.month, 1)
        return start, today
    else:
        raise ValueError(f"Unknown date range: {range_name}")


# ═══════════════════════════════════════════════════════════════════════════
# COMMAND HANDLERS
# ═══════════════════════════════════════════════════════════════════════════

@authorized
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Send a welcome message when /start is issued."""
    user = update.effective_user
    welcome_message = Messages.welcome(user.first_name)

    try:
        # Send welcome with persistent reply keyboard
        await update.message.reply_text(
            welcome_message,
            reply_markup=ReplyKeyboards.main_menu(),
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"Error sending welcome message: {e}")
        await update.message.reply_text(
            f"Welcome, {user.first_name}! Use /report to generate a sales report or /help for assistance."
        )

    return ConversationHandler.END


@authorized
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


@authorized
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


@authorized
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

@authorized
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
        update_user_session(user_id, {"report_type": selected_type})

        await query.edit_message_text(
            Messages.top10_source_selection(),
            reply_markup=Keyboards.top10_sources(),
            parse_mode="HTML"
        )
        return ConversationState.SELECTING_TOP10_SOURCE

    # Initialize user data with session management
    update_user_session(user_id, {"report_type": selected_type})

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

    # Validate session data exists and is not expired
    session = get_user_session(user_id)
    if not session or "start_date" not in session:
        await query.edit_message_text(
            "⚠️ Session expired. Please start a new report with /report",
            parse_mode="HTML"
        )
        return ConversationHandler.END

    start_date = session["start_date"]
    end_date = session["end_date"]
    report_type = session["report_type"]

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

    # Process predefined date ranges
    if selected_range in ("today", "yesterday", "thisweek", "thismonth"):
        start_date, end_date = calculate_date_range(selected_range)
        user_data[user_id]["start_date"] = start_date
        user_data[user_id]["end_date"] = end_date
        return await prepare_generate_report(update, context)

    elif selected_range == "custom":
        # Start custom date selection
        message = Messages.custom_date_prompt(
            "Select START year",
            1, 6,
            f"Please select the start year for your custom date range:"
        )

        await query.edit_message_text(
            message,
            reply_markup=Keyboards.year_picker(get_year_choices(), "back_to_date_range"),
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
        message = Messages.custom_date_prompt(
            "Select START year",
            1, 6,
            "Please select the start year:"
        )

        await query.edit_message_text(
            message,
            reply_markup=Keyboards.year_picker(get_year_choices(), "back_to_date_range"),
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
    start_date = user_data[user_id]["start_date"]

    # Move to end date selection
    current_year = datetime.now().year
    if selected_year == current_year:
        # Skip year selection, go to month
        user_data[user_id]["custom_end_year"] = current_year

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
        # Show year selection for end date (from start year to current year)
        message = Messages.custom_date_prompt(
            "Select END year",
            4, 6,
            f"Start date: <b>{start_date.strftime('%Y-%m-%d')}</b>\n\nNow select the end year:"
        )

        await query.edit_message_text(
            message,
            reply_markup=Keyboards.end_year_picker(get_year_choices(selected_year), "back_to_custom_start_day"),
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

        message = Messages.custom_date_prompt(
            "Select END year",
            4, 6,
            f"Start date: <b>{start_date.strftime('%Y-%m-%d')}</b>\n\nNow select the end year:"
        )

        await query.edit_message_text(
            message,
            reply_markup=Keyboards.end_year_picker(get_year_choices(start_date.year), "back_to_custom_start_day"),
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

    # Update session with source selection
    session = update_user_session(user_id, {"top10_source": source_selection})

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

    # Check if session exists and has required data
    session = get_user_session(user_id)
    if not session or "start_date" not in session:
        await query.edit_message_text(
            "⚠️ Session expired. Please start a new report with /report",
            reply_markup=Keyboards.error_retry(),
            parse_mode="HTML"
        )
        return ConversationHandler.END

    session["top10_source"] = source_selection

    return await generate_top10_report(update, context)


async def generate_top10_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Generate TOP-10 products report for selected source(s)."""
    query = update.callback_query
    user_id = update.effective_user.id

    # Validate session data exists and is not expired
    session = get_user_session(user_id)
    if not session or "start_date" not in session:
        await query.edit_message_text(
            "⚠️ Session expired. Please start a new report with /report",
            parse_mode="HTML"
        )
        return ConversationHandler.END

    start_date = session["start_date"]
    end_date = session["end_date"]
    source_selection = session.get("top10_source", "all")

    try:
        now = datetime.now(ZoneInfo(DEFAULT_TIMEZONE))
        report_time = now.strftime("%Y-%m-%d %H:%M:%S")

        # Use sources from config
        all_sources = TOP10_SOURCES

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
            # Run in thread to avoid blocking event loop
            top_products, total_quantity = await asyncio.to_thread(
                report_service.calculate_top10_products,
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

            # Truncate if needed
            report = truncate_message(report)

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

    except KeyCRMAPIError as e:
        logger.error(f"KeyCRM API error in TOP-10: {e.message} - {e.error_details}")
        await query.edit_message_text(
            "❌ <b>API Error</b>\n\nFailed to connect to KeyCRM. Please try again later.",
            reply_markup=Keyboards.error_retry(),
            parse_mode="HTML"
        )

    except Exception as e:
        logger.error(f"Error generating TOP-10 report: {e}", exc_info=True)
        await query.edit_message_text(
            f"⚠️ Error generating report: {str(e)}",
            reply_markup=Keyboards.error_retry(),
            parse_mode="HTML"
        )

    return ConversationHandler.END


# ═══════════════════════════════════════════════════════════════════════════
# REPORT GENERATION HANDLERS
# ═══════════════════════════════════════════════════════════════════════════

async def generate_summary_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Generate the summary sales report."""
    query = update.callback_query
    user_id = update.effective_user.id

    # Validate session data exists and is not expired
    session = get_user_session(user_id)
    if not session or "start_date" not in session:
        await query.edit_message_text(
            "⚠️ Session expired. Please start a new report with /report",
            parse_mode="HTML"
        )
        return ConversationHandler.END

    start_date = session["start_date"]
    end_date = session["end_date"]

    try:
        # Get sales data (run in thread to avoid blocking event loop)
        sales_by_source, order_counts, total_orders, revenue_by_source, returns_by_source = (
            await asyncio.to_thread(
                report_service.aggregate_sales_data,
                target_date=(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')),
                tz_name=DEFAULT_TIMEZONE,
                telegram_manager_ids=TELEGRAM_MANAGER_IDS
            )
        )

        # Get report timestamp
        now = datetime.now(ZoneInfo(DEFAULT_TIMEZONE))
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

        # Truncate if needed to stay within Telegram limits
        report = truncate_message(report)

        # Send report
        await query.edit_message_text(
            report,
            reply_markup=Keyboards.post_report_actions(include_summary=False),
            parse_mode="HTML"
        )

    except KeyCRMAPIError as e:
        logger.error(f"KeyCRM API error: {e.message} - {e.error_details}")
        await query.edit_message_text(
            "❌ <b>API Error</b>\n\nFailed to connect to KeyCRM. Please try again later.",
            reply_markup=Keyboards.error_retry(),
            parse_mode="HTML"
        )

    except Exception as e:
        logger.error(f"Error generating report: {e}", exc_info=True)
        await query.edit_message_text(
            Messages.error(str(e)),
            reply_markup=Keyboards.error_retry(),
            parse_mode="HTML"
        )

    return ConversationHandler.END


async def generate_excel_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Generate and send an Excel sales report."""
    query = update.callback_query
    user_id = update.effective_user.id

    # Validate session data exists and is not expired
    session = get_user_session(user_id)
    if not session or "start_date" not in session:
        await query.edit_message_text(
            "⚠️ Session expired. Please start a new report with /report",
            parse_mode="HTML"
        )
        return ConversationHandler.END

    start_date = session["start_date"]
    end_date = session["end_date"]

    # Get bot token from context
    bot_token = context.bot.token
    chat_id = update.effective_chat.id

    try:
        # Show preparing message
        await query.edit_message_text(
            Messages.excel_preparing(start_date, end_date),
            parse_mode="HTML"
        )

        # Generate and send Excel (run in thread to avoid blocking event loop)
        success = await asyncio.to_thread(
            report_service.generate_excel_report,
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

    # Initialize user data and set report type
    update_user_session(user_id, {"report_type": report_type})

    # Calculate and set date range
    start_date, end_date = calculate_date_range(date_range)
    user_data[user_id]["start_date"] = start_date
    user_data[user_id]["end_date"] = end_date

    # Generate report
    return await prepare_generate_report(update, context)


# ═══════════════════════════════════════════════════════════════════════════
# REPLY KEYBOARD TEXT HANDLERS
# ═══════════════════════════════════════════════════════════════════════════

@authorized
async def reply_keyboard_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle '📊 Report' button from reply keyboard."""
    await update.message.reply_text(
        Messages.report_selection(),
        reply_markup=Keyboards.report_types(),
        parse_mode="HTML"
    )
    return ConversationState.SELECTING_REPORT_TYPE


@authorized
async def reply_keyboard_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle 'ℹ️ Help' button from reply keyboard."""
    await update.message.reply_text(
        Messages.help_text(),
        reply_markup=Keyboards.help_menu(),
        parse_mode="HTML"
    )


@authorized
async def dashboard_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send dashboard link when /dashboard is issued."""
    keyboard = [[InlineKeyboardButton("📈 Open Dashboard", url=DASHBOARD_URL)]]
    await update.message.reply_text(
        f"📈 <b>Sales Dashboard</b>\n\n"
        f"View interactive charts and analytics:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


@authorized
async def reply_keyboard_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle '📈 Dashboard' button from reply keyboard."""
    keyboard = [[InlineKeyboardButton("📈 Open Dashboard", url=DASHBOARD_URL)]]
    await update.message.reply_text(
        f"📈 <b>Sales Dashboard</b>\n\n"
        f"View interactive charts and analytics:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


# ═══════════════════════════════════════════════════════════════════════════
# AUTHORIZATION HANDLERS
# ═══════════════════════════════════════════════════════════════════════════

async def auth_request_access(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle access request from new user."""
    query = update.callback_query
    await query.answer()

    user = update.effective_user

    # Create access request
    is_new = database.request_access(
        user_id=user.id,
        username=user.username,
        first_name=user.first_name,
        last_name=user.last_name
    )

    if is_new:
        # Notify admins
        await notify_admins_new_request(context, user)

        await query.edit_message_text(
            "✅ <b>Access Requested!</b>\n\n"
            f"Your request has been sent to the administrator.\n\n"
            f"<b>Your details:</b>\n"
            f"• User ID: <code>{user.id}</code>\n"
            f"• Username: @{user.username or 'N/A'}\n"
            f"• Name: {user.first_name or ''} {user.last_name or ''}\n\n"
            "⏳ Please wait for approval.",
            parse_mode="HTML"
        )
    else:
        # Already has a request
        status = database.get_user_auth_status(user.id)
        if status['status'] == database.STATUS_PENDING:
            await query.edit_message_text(
                ACCESS_PENDING_MESSAGE,
                parse_mode="HTML"
            )
        elif status['status'] == database.STATUS_DENIED:
            await query.edit_message_text(
                ACCESS_DENIED_MESSAGE,
                parse_mode="HTML"
            )

    return ConversationHandler.END


async def notify_admins_new_request(context: ContextTypes.DEFAULT_TYPE, user) -> None:
    """Notify all admins about new access request."""
    if not ADMIN_USER_IDS:
        logger.warning("No admin IDs configured - cannot notify about access request")
        return

    message = (
        "🔔 <b>New Access Request</b>\n\n"
        f"<b>User ID:</b> <code>{user.id}</code>\n"
        f"<b>Username:</b> @{user.username or 'N/A'}\n"
        f"<b>Name:</b> {user.first_name or ''} {user.last_name or ''}\n\n"
        "Choose an action:"
    )

    keyboard = [
        [
            InlineKeyboardButton("✅ Approve", callback_data=f"auth_approve_{user.id}"),
            InlineKeyboardButton("❌ Deny", callback_data=f"auth_deny_{user.id}")
        ]
    ]

    for admin_id in ADMIN_USER_IDS:
        try:
            await context.bot.send_message(
                chat_id=admin_id,
                text=message,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="HTML"
            )
            logger.info(f"Notified admin {admin_id} about access request from {user.id}")
        except Exception as e:
            logger.error(f"Failed to notify admin {admin_id}: {e}")


async def auth_approve_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin approves user access."""
    query = update.callback_query
    admin = update.effective_user

    if not is_admin(admin.id):
        await query.answer("You are not authorized to approve users", show_alert=True)
        return

    # Extract user_id from callback data
    target_user_id = int(query.data.split('_')[-1])

    # Approve user
    success = database.approve_user(target_user_id, admin.id)

    if success:
        await query.answer("User approved!")

        # Update admin message
        user_info = database.get_user_auth_status(target_user_id)
        await query.edit_message_text(
            f"✅ <b>User Approved</b>\n\n"
            f"<b>User ID:</b> <code>{target_user_id}</code>\n"
            f"<b>Username:</b> @{user_info.get('username') or 'N/A'}\n"
            f"<b>Name:</b> {user_info.get('first_name') or ''} {user_info.get('last_name') or ''}\n\n"
            f"<i>Approved by you</i>",
            parse_mode="HTML"
        )

        # Notify the user
        try:
            await context.bot.send_message(
                chat_id=target_user_id,
                text=(
                    "🎉 <b>Access Granted!</b>\n\n"
                    "Your access request has been approved.\n"
                    "You can now use the bot.\n\n"
                    "Use /start to begin."
                ),
                parse_mode="HTML"
            )
        except Exception as e:
            logger.error(f"Failed to notify user {target_user_id} about approval: {e}")
    else:
        await query.answer("Failed to approve user", show_alert=True)


async def auth_deny_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin denies user access."""
    query = update.callback_query
    admin = update.effective_user

    if not is_admin(admin.id):
        await query.answer("You are not authorized to deny users", show_alert=True)
        return

    # Extract user_id from callback data
    target_user_id = int(query.data.split('_')[-1])

    # Get user info before denying
    user_info = database.get_user_auth_status(target_user_id)
    denial_count = (user_info.get('denial_count') or 0) + 1

    # Deny user
    success, is_frozen = database.deny_user(target_user_id, admin.id)

    if success:
        if is_frozen:
            await query.answer("User frozen!")

            # Update admin message
            await query.edit_message_text(
                f"🚫 <b>User Frozen</b>\n\n"
                f"<b>User ID:</b> <code>{target_user_id}</code>\n"
                f"<b>Username:</b> @{user_info.get('username') or 'N/A'}\n"
                f"<b>Name:</b> {user_info.get('first_name') or ''} {user_info.get('last_name') or ''}\n\n"
                f"<i>Denied {denial_count} times - account frozen</i>",
                parse_mode="HTML"
            )

            # Notify the user - no re-request option
            try:
                await context.bot.send_message(
                    chat_id=target_user_id,
                    text=ACCESS_FROZEN_MESSAGE,
                    parse_mode="HTML"
                )
            except Exception as e:
                logger.error(f"Failed to notify user {target_user_id} about freeze: {e}")
        else:
            await query.answer("User denied!")

            # Update admin message
            await query.edit_message_text(
                f"❌ <b>User Denied</b>\n\n"
                f"<b>User ID:</b> <code>{target_user_id}</code>\n"
                f"<b>Username:</b> @{user_info.get('username') or 'N/A'}\n"
                f"<b>Name:</b> {user_info.get('first_name') or ''} {user_info.get('last_name') or ''}\n\n"
                f"<i>Denied by you ({denial_count}/{database.MAX_DENIAL_COUNT})</i>",
                parse_mode="HTML"
            )

            # Notify the user with option to request again
            try:
                keyboard = [[
                    InlineKeyboardButton("🔄 Request Again", callback_data="auth_request_again")
                ]]
                await context.bot.send_message(
                    chat_id=target_user_id,
                    text=ACCESS_DENIED_MESSAGE + f"\n\n<i>({denial_count}/{database.MAX_DENIAL_COUNT} denials)</i>\n\nYou can request access again:",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="HTML"
                )
            except Exception as e:
                logger.error(f"Failed to notify user {target_user_id} about denial: {e}")
    else:
        await query.answer("Failed to deny user", show_alert=True)


async def auth_request_again(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle re-request access from denied user."""
    query = update.callback_query
    await query.answer()

    user = update.effective_user

    # Reset status to pending
    success, was_frozen = database.reset_user_to_pending(user.id)

    if was_frozen:
        await query.edit_message_text(
            ACCESS_FROZEN_MESSAGE,
            parse_mode="HTML"
        )
    elif success:
        # Notify admins
        await notify_admins_new_request(context, user)

        await query.edit_message_text(
            "✅ <b>Access Re-Requested!</b>\n\n"
            f"Your new request has been sent to the administrator.\n\n"
            "⏳ Please wait for approval.",
            parse_mode="HTML"
        )
    else:
        await query.edit_message_text(
            "⚠️ Failed to submit request. Please try /start again.",
            parse_mode="HTML"
        )

    return ConversationHandler.END


# ═══════════════════════════════════════════════════════════════════════════
# ADMIN USER MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════

async def admin_users_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show list of approved and frozen users for admin management."""
    user = update.effective_user

    if not is_admin(user.id):
        await update.message.reply_text("⛔ Admin access required.", parse_mode="HTML")
        return

    users = database.get_all_authorized_users()
    frozen_users = database.get_frozen_users()

    if not users and not frozen_users:
        await update.message.reply_text(
            "📋 <b>No users</b>\n\nNo approved or frozen users.",
            parse_mode="HTML"
        )
        return

    message = ""
    keyboard = []

    # Approved users
    if users:
        message += "👥 <b>Approved Users</b>\n\n"
        for u in users[:15]:
            username = f"@{u['username']}" if u.get('username') else "N/A"
            name = f"{u.get('first_name') or ''} {u.get('last_name') or ''}".strip() or "Unknown"
            last_active = u.get('last_activity') or u.get('reviewed_at') or "Never"

            message += f"• <code>{u['user_id']}</code> - {name} ({username})\n"
            message += f"  Last active: {last_active}\n\n"

            keyboard.append([
                InlineKeyboardButton(
                    f"🚫 Revoke {name[:15]}",
                    callback_data=f"admin_revoke_{u['user_id']}"
                )
            ])

        if len(users) > 15:
            message += f"<i>...and {len(users) - 15} more approved users</i>\n\n"

    # Frozen users
    if frozen_users:
        message += "🧊 <b>Frozen Users</b>\n\n"
        for u in frozen_users[:10]:
            username = f"@{u['username']}" if u.get('username') else "N/A"
            name = f"{u.get('first_name') or ''} {u.get('last_name') or ''}".strip() or "Unknown"
            frozen_at = u.get('reviewed_at') or "Unknown"

            message += f"• <code>{u['user_id']}</code> - {name} ({username})\n"
            message += f"  Frozen: {frozen_at}\n\n"

            keyboard.append([
                InlineKeyboardButton(
                    f"🔓 Unfreeze {name[:15]}",
                    callback_data=f"admin_unfreeze_{u['user_id']}"
                )
            ])

        if len(frozen_users) > 10:
            message += f"<i>...and {len(frozen_users) - 10} more frozen users</i>\n"

    keyboard.append([InlineKeyboardButton("🔙 Close", callback_data="admin_close")])

    await update.message.reply_text(
        message,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="HTML"
    )


async def admin_revoke_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin revokes user access."""
    query = update.callback_query
    admin = update.effective_user

    if not is_admin(admin.id):
        await query.answer("Admin access required", show_alert=True)
        return

    target_user_id = int(query.data.split('_')[-1])
    user_info = database.get_user_auth_status(target_user_id)

    if not user_info:
        await query.answer("User not found", show_alert=True)
        return

    # Revoke access
    success = database.revoke_user(target_user_id, admin.id)

    if success:
        await query.answer("Access revoked!")
        # Refresh the user list (silent revoke - no notification to user)
        await show_updated_user_list(query, admin.id)
    else:
        await query.answer("Failed to revoke access", show_alert=True)


async def show_updated_user_list(query, admin_id: int) -> None:
    """Show updated user list after revocation/unfreeze."""
    users = database.get_all_authorized_users()
    frozen_users = database.get_frozen_users()

    if not users and not frozen_users:
        await query.edit_message_text(
            "📋 <b>No users</b>\n\nNo approved or frozen users.",
            parse_mode="HTML"
        )
        return

    message = ""
    keyboard = []

    # Approved users
    if users:
        message += "👥 <b>Approved Users</b>\n\n"
        for u in users[:15]:
            username = f"@{u['username']}" if u.get('username') else "N/A"
            name = f"{u.get('first_name') or ''} {u.get('last_name') or ''}".strip() or "Unknown"
            last_active = u.get('last_activity') or u.get('reviewed_at') or "Never"

            message += f"• <code>{u['user_id']}</code> - {name} ({username})\n"
            message += f"  Last active: {last_active}\n\n"

            keyboard.append([
                InlineKeyboardButton(
                    f"🚫 Revoke {name[:15]}",
                    callback_data=f"admin_revoke_{u['user_id']}"
                )
            ])

        if len(users) > 15:
            message += f"<i>...and {len(users) - 15} more approved users</i>\n\n"

    # Frozen users
    if frozen_users:
        message += "🧊 <b>Frozen Users</b>\n\n"
        for u in frozen_users[:10]:
            username = f"@{u['username']}" if u.get('username') else "N/A"
            name = f"{u.get('first_name') or ''} {u.get('last_name') or ''}".strip() or "Unknown"
            frozen_at = u.get('reviewed_at') or "Unknown"

            message += f"• <code>{u['user_id']}</code> - {name} ({username})\n"
            message += f"  Frozen: {frozen_at}\n\n"

            keyboard.append([
                InlineKeyboardButton(
                    f"🔓 Unfreeze {name[:15]}",
                    callback_data=f"admin_unfreeze_{u['user_id']}"
                )
            ])

        if len(frozen_users) > 10:
            message += f"<i>...and {len(frozen_users) - 10} more frozen users</i>\n"

    keyboard.append([InlineKeyboardButton("🔙 Close", callback_data="admin_close")])

    await query.edit_message_text(
        message,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="HTML"
    )


async def admin_unfreeze_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin unfreezes a user."""
    query = update.callback_query
    admin = update.effective_user

    if not is_admin(admin.id):
        await query.answer("Admin access required", show_alert=True)
        return

    target_user_id = int(query.data.split('_')[-1])
    user_info = database.get_user_auth_status(target_user_id)

    if not user_info:
        await query.answer("User not found", show_alert=True)
        return

    # Unfreeze user
    success = database.unfreeze_user(target_user_id, admin.id)

    if success:
        await query.answer("User unfrozen!")

        # Notify the user
        try:
            keyboard = [[
                InlineKeyboardButton("🔑 Request Access", callback_data="auth_request_access")
            ]]
            await context.bot.send_message(
                chat_id=target_user_id,
                text=(
                    "🔓 <b>Account Unfrozen</b>\n\n"
                    "Your account has been unfrozen by an administrator.\n"
                    "You can now request access again:"
                ),
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="HTML"
            )
        except Exception as e:
            logger.error(f"Failed to notify user {target_user_id} about unfreeze: {e}")

        # Refresh the user list
        await show_updated_user_list(query, admin.id)
    else:
        await query.answer("Failed to unfreeze user", show_alert=True)


async def admin_close(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Close admin panel."""
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("✅ Admin panel closed.", parse_mode="HTML")


# ═══════════════════════════════════════════════════════════════════════════
# SEARCH HANDLERS
# ═══════════════════════════════════════════════════════════════════════════

@authorized
async def search_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle /search command - start order search."""
    await update.message.reply_text(
        "🔍 <b>Search Orders</b>\n\n"
        "How would you like to search?",
        reply_markup=Keyboards.search_type(),
        parse_mode="HTML"
    )
    return ConversationState.SEARCH_WAITING_QUERY


@authorized
async def search_command_from_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle search from callback."""
    query = update.callback_query
    await query.answer()

    await query.edit_message_text(
        "🔍 <b>Search Orders</b>\n\n"
        "How would you like to search?",
        reply_markup=Keyboards.search_type(),
        parse_mode="HTML"
    )
    return ConversationState.SEARCH_WAITING_QUERY


async def search_type_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle search type selection."""
    query = update.callback_query
    await query.answer()

    user_id = update.effective_user.id
    search_type = query.data.split('_')[-1]  # id, phone, or email

    update_user_session(user_id, {"search_type": search_type})

    type_labels = {
        "id": "Order ID",
        "phone": "Phone Number",
        "email": "Email"
    }

    await query.edit_message_text(
        f"🔍 <b>Search by {type_labels.get(search_type, search_type)}</b>\n\n"
        f"Please enter {type_labels.get(search_type, 'your query')}:",
        parse_mode="HTML"
    )
    return ConversationState.SEARCH_WAITING_QUERY


async def search_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle search query text input."""
    user_id = update.effective_user.id
    query_text = update.message.text.strip()

    session = get_user_session(user_id)
    search_type = session.get("search_type", "all") if session else "all"

    # Show searching message
    searching_msg = await update.message.reply_text(
        "🔍 Searching...",
        parse_mode="HTML"
    )

    try:
        # Perform search
        results = await asyncio.to_thread(
            report_service.api.search_orders,
            query_text,
            search_type,
            10
        )

        orders = results.get("data", [])

        if not orders:
            await searching_msg.edit_text(
                f"🔍 <b>No Results</b>\n\n"
                f"No orders found for: <code>{query_text}</code>",
                reply_markup=Keyboards.search_results_actions(),
                parse_mode="HTML"
            )
            return ConversationHandler.END

        # Format results
        message = f"🔍 <b>Search Results</b>\n"
        message += f"Found {len(orders)} order(s) for: <code>{query_text}</code>\n\n"

        for order in orders[:5]:  # Limit to 5 results
            order_id = order.get("id", "?")
            status = order.get("status_group", {}).get("name", "Unknown")
            buyer = order.get("buyer", {})
            buyer_name = buyer.get("full_name", "Unknown")
            buyer_phone = buyer.get("phone", "N/A")
            total = order.get("grand_total", 0)
            created = order.get("created_at", "")[:10]

            message += f"📦 <b>Order #{order_id}</b>\n"
            message += f"   👤 {buyer_name}\n"
            message += f"   📱 {buyer_phone}\n"
            message += f"   💰 {total} UAH\n"
            message += f"   📊 {status}\n"
            message += f"   📅 {created}\n\n"

        if len(orders) > 5:
            message += f"<i>...and {len(orders) - 5} more results</i>\n"

        await searching_msg.edit_text(
            message,
            reply_markup=Keyboards.search_results_actions(),
            parse_mode="HTML"
        )

    except Exception as e:
        logger.error(f"Search error: {e}", exc_info=True)
        await searching_msg.edit_text(
            f"⚠️ Search failed: {str(e)}",
            reply_markup=Keyboards.search_results_actions(),
            parse_mode="HTML"
        )

    return ConversationHandler.END


# ═══════════════════════════════════════════════════════════════════════════
# SETTINGS HANDLERS
# ═══════════════════════════════════════════════════════════════════════════

@authorized
async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle /settings command."""
    user_id = update.effective_user.id
    prefs = database.get_user_preferences(user_id) or {}

    await update.message.reply_text(
        "⚙️ <b>Settings</b>\n\n"
        "Configure your preferences:",
        reply_markup=Keyboards.settings_menu(prefs),
        parse_mode="HTML"
    )
    return ConversationState.SETTINGS_MENU


@authorized
async def settings_command_from_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle settings from callback."""
    query = update.callback_query
    await query.answer()

    user_id = update.effective_user.id
    prefs = database.get_user_preferences(user_id) or {}

    await query.edit_message_text(
        "⚙️ <b>Settings</b>\n\n"
        "Configure your preferences:",
        reply_markup=Keyboards.settings_menu(prefs),
        parse_mode="HTML"
    )
    return ConversationState.SETTINGS_MENU


async def settings_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle settings menu callbacks."""
    query = update.callback_query
    await query.answer()

    user_id = update.effective_user.id
    action = query.data

    if action == "settings_timezone":
        await query.edit_message_text(
            "🌍 <b>Select Timezone</b>\n\n"
            "Choose your timezone:",
            reply_markup=Keyboards.settings_timezone(),
            parse_mode="HTML"
        )
    elif action == "settings_date_range":
        await query.edit_message_text(
            "📅 <b>Default Date Range</b>\n\n"
            "Choose the default date range for reports:",
            reply_markup=Keyboards.settings_date_range(),
            parse_mode="HTML"
        )
    elif action == "settings_notifications":
        await query.edit_message_text(
            "🔔 <b>Notifications</b>\n\n"
            "Enable or disable notifications:",
            reply_markup=Keyboards.settings_notifications(),
            parse_mode="HTML"
        )
    elif action == "settings_back":
        prefs = database.get_user_preferences(user_id) or {}
        await query.edit_message_text(
            "⚙️ <b>Settings</b>\n\n"
            "Configure your preferences:",
            reply_markup=Keyboards.settings_menu(prefs),
            parse_mode="HTML"
        )
    elif action.startswith("set_tz_"):
        timezone = action.replace("set_tz_", "")
        database.update_user_preference(user_id, "timezone", timezone)
        prefs = database.get_user_preferences(user_id) or {}
        await query.edit_message_text(
            f"✅ Timezone set to <b>{timezone}</b>\n\n"
            "⚙️ <b>Settings</b>",
            reply_markup=Keyboards.settings_menu(prefs),
            parse_mode="HTML"
        )
    elif action.startswith("set_range_"):
        date_range = action.replace("set_range_", "")
        database.update_user_preference(user_id, "default_date_range", date_range)
        prefs = database.get_user_preferences(user_id) or {}
        range_label = {'today': 'Today', 'week': 'This Week', 'month': 'This Month'}.get(date_range, date_range)
        await query.edit_message_text(
            f"✅ Default range set to <b>{range_label}</b>\n\n"
            "⚙️ <b>Settings</b>",
            reply_markup=Keyboards.settings_menu(prefs),
            parse_mode="HTML"
        )
    elif action.startswith("set_notif_"):
        enabled = action.replace("set_notif_", "") == "1"
        database.update_user_preference(user_id, "notifications_enabled", 1 if enabled else 0)
        prefs = database.get_user_preferences(user_id) or {}
        status = "enabled" if enabled else "disabled"
        await query.edit_message_text(
            f"✅ Notifications <b>{status}</b>\n\n"
            "⚙️ <b>Settings</b>",
            reply_markup=Keyboards.settings_menu(prefs),
            parse_mode="HTML"
        )

    return ConversationState.SETTINGS_MENU


# ═══════════════════════════════════════════════════════════════════════════
# SCHEDULED MILESTONE CHECK JOB
# ═══════════════════════════════════════════════════════════════════════════

async def check_and_broadcast_milestones(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Scheduled job to check revenue milestones and broadcast to all users.
    Runs at end of day/week to check if milestones were hit.
    """
    from bot.formatters import bold

    logger.info("Running milestone check job...")

    now = datetime.now(ZoneInfo(DEFAULT_TIMEZONE))

    # Determine what periods to check based on current time
    periods_to_check = []

    # Always check daily (for today)
    today = now.date()
    periods_to_check.append({
        "type": "daily",
        "key": today.isoformat(),
        "start": today,
        "end": today,
        "label": f"Today ({today.strftime('%d.%m.%Y')})"
    })

    # Check weekly on Sunday (end of week)
    if now.weekday() == 6:  # Sunday
        week_start = today - timedelta(days=6)
        periods_to_check.append({
            "type": "weekly",
            "key": f"{week_start.isocalendar()[0]}-W{week_start.isocalendar()[1]:02d}",
            "start": week_start,
            "end": today,
            "label": f"This Week ({week_start.strftime('%d.%m')} - {today.strftime('%d.%m.%Y')})"
        })

    # Check monthly on last day of month
    last_day_of_month = calendar.monthrange(now.year, now.month)[1]
    if now.day == last_day_of_month:
        month_start = today.replace(day=1)
        periods_to_check.append({
            "type": "monthly",
            "key": f"{today.year}-{today.month:02d}",
            "start": month_start,
            "end": today,
            "label": f"This Month ({today.strftime('%B %Y')})"
        })

    # Get all authorized users
    authorized_users = database.get_all_authorized_users()
    if not authorized_users:
        logger.info("No authorized users to notify")
        return

    # Check each period
    for period in periods_to_check:
        period_type = period["type"]
        period_key = period["key"]
        milestones = REVENUE_MILESTONES.get(period_type, [])

        if not milestones:
            continue

        try:
            # Get revenue for this period
            sales_by_source, order_counts, total_orders, revenue_by_source, returns_by_source = (
                await asyncio.to_thread(
                    report_service.aggregate_sales_data,
                    target_date=(period["start"].strftime('%Y-%m-%d'), period["end"].strftime('%Y-%m-%d')),
                    tz_name=DEFAULT_TIMEZONE,
                    telegram_manager_ids=TELEGRAM_MANAGER_IDS
                )
            )

            total_revenue = sum(revenue_by_source.values())
            logger.info(f"Milestone check for {period_type} ({period_key}): ₴{total_revenue:,.0f}")

            # Find highest milestone reached
            highest_milestone = None
            for milestone in milestones:
                if total_revenue >= milestone["amount"]:
                    highest_milestone = milestone

            if not highest_milestone:
                continue

            # Check if already celebrated
            if database.is_milestone_celebrated(period_type, period_key, highest_milestone["amount"]):
                logger.info(f"Milestone {highest_milestone['amount']} already celebrated for {period_key}")
                continue

            # Mark as celebrated
            if not database.mark_milestone_celebrated(period_type, period_key, highest_milestone["amount"], total_revenue):
                continue

            # Format congratulations message
            amount = highest_milestone["amount"]
            if amount >= 1000000:
                amount_text = f"₴{amount / 1000000:.1f}M"
            else:
                amount_text = f"₴{amount / 1000:.0f}K"

            emoji = highest_milestone["emoji"]
            message = highest_milestone["message"]

            congrats_msg = (
                f"{'🎊' * 8}\n\n"
                f"{emoji} {bold('MILESTONE REACHED!')} {emoji}\n\n"
                f"🏆 {bold(message)}\n\n"
                f"📅 {period['label']}\n"
                f"💰 Revenue: {bold(f'₴{total_revenue:,.0f}')}\n"
                f"📦 Orders: {bold(str(total_orders))}\n\n"
                f"{'🎊' * 8}"
            )

            # Broadcast to all authorized users
            success_count = 0
            for user in authorized_users:
                try:
                    await context.bot.send_message(
                        chat_id=user["user_id"],
                        text=congrats_msg,
                        parse_mode="HTML"
                    )
                    success_count += 1
                except Exception as e:
                    logger.warning(f"Failed to send milestone to user {user['user_id']}: {e}")

            logger.info(f"Milestone {amount_text} broadcast to {success_count}/{len(authorized_users)} users")

        except Exception as e:
            logger.error(f"Error checking milestone for {period_type}: {e}", exc_info=True)
