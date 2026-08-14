"""
Telegram bot handlers organized by functionality.

All 25 handlers from telegram_bot.py, reorganized and using new
keyboards and formatters modules to eliminate duplication.
"""
import asyncio
import logging
import calendar
import threading
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from typing import Dict, Any, Optional, Tuple, List
from telegram import Update
from telegram.ext import ContextTypes, ConversationHandler

from functools import wraps
from core.filters import parse_period
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
from core.i18n import DEFAULT_LANGUAGE, LANGUAGE_NAMES, normalize, t
from bot.keyboards import Keyboards, ReplyKeyboards
from bot.formatters import Messages, ReportFormatters, truncate_message
from bot.services import ReportService, KeyCRMAPIError, ReportGenerationError

# Logger
logger = logging.getLogger(__name__)

# Session timeout in minutes
SESSION_TIMEOUT_MINUTES = 30

def _lang(update: Update) -> str:
    """The language this reader chose, for whichever kind of update this is.

    Called at the point of use rather than threaded through forty signatures.
    It is a primary-key read on a local SQLite table a few times per message —
    cheaper than the Telegram round trip it decorates, and it keeps every call
    site honest instead of relying on someone remembering to pass a parameter.
    """
    user = update.effective_user
    return database.get_user_language(user.id) if user else DEFAULT_LANGUAGE


# ═══════════════════════════════════════════════════════════════════════════
# AUTHORIZATION
# ═══════════════════════════════════════════════════════════════════════════

ACCESS_DENIED_MESSAGE = "access.denied"  # i18n key; see _msg() below

ACCESS_FROZEN_MESSAGE = "access.frozen"

ACCESS_PENDING_MESSAGE = "access.pending"

REQUEST_ACCESS_MESSAGE = "access.required"


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
                InlineKeyboardButton(t("btn.request_access", _lang(update)), callback_data="auth_request_access")
            ]]

            if update.callback_query:
                await update.callback_query.answer()
                await update.callback_query.edit_message_text(
                    t(REQUEST_ACCESS_MESSAGE, _lang(update)),
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="HTML"
                )
            elif update.message:
                await update.message.reply_text(
                    t(REQUEST_ACCESS_MESSAGE, _lang(update)),
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="HTML"
                )
            return ConversationHandler.END

        if auth_status['status'] == database.STATUS_PENDING:
            logger.info(f"User {user.id} has pending request")
            if update.callback_query:
                await update.callback_query.answer(t("access.pending_alert", _lang(update)), show_alert=True)
            elif update.message:
                await update.message.reply_text(t(ACCESS_PENDING_MESSAGE, _lang(update)), parse_mode="HTML")
            return ConversationHandler.END

        if auth_status['status'] == database.STATUS_FROZEN:
            logger.warning(f"Frozen user {user.id} attempted access")
            if update.callback_query:
                await update.callback_query.answer(t("access.frozen_alert", _lang(update)), show_alert=True)
            elif update.message:
                await update.message.reply_text(t(ACCESS_FROZEN_MESSAGE, _lang(update)), parse_mode="HTML")
            return ConversationHandler.END

        if auth_status['status'] == database.STATUS_DENIED:
            logger.warning(f"Denied user {user.id} attempted access")

            keyboard = [[
                InlineKeyboardButton("🔄 Request Again", callback_data="auth_request_again")
            ]]

            if update.callback_query:
                await update.callback_query.answer()
                await update.callback_query.edit_message_text(
                    t(ACCESS_DENIED_MESSAGE, _lang(update)) + "\n\n" + t("access.request_again", _lang(update)),
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="HTML"
                )
            elif update.message:
                await update.message.reply_text(
                    t(ACCESS_DENIED_MESSAGE, _lang(update)) + "\n\n" + t("access.request_again", _lang(update)),
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


def calculate_date_range(range_name: str) -> Tuple[date, date]:
    """
    Calculate start and end dates for a given range name.

    Args:
        range_name: One of 'today', 'yesterday', 'thisweek', 'thismonth'

    Returns:
        Tuple of (start_date, end_date)
    """
    return parse_period(range_name).as_tuple()


# ═══════════════════════════════════════════════════════════════════════════
# COMMAND HANDLERS
# ═══════════════════════════════════════════════════════════════════════════

@authorized
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Send a welcome message when /start is issued."""
    user = update.effective_user
    welcome_message = Messages.welcome(user.first_name, lang=_lang(update))

    try:
        # Send welcome with persistent reply keyboard
        await update.message.reply_text(
            welcome_message,
            reply_markup=ReplyKeyboards.main_menu(lang=_lang(update)),
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"Error sending welcome message: {e}")
        await update.message.reply_text(
            t("msg.welcome", _lang(update), name=user.first_name)
        )

    return ConversationHandler.END


@authorized
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a helpful message when /help is issued."""
    try:
        await update.message.reply_text(
            Messages.help_text(lang=_lang(update)),
            reply_markup=Keyboards.help_menu(lang=_lang(update)),
            parse_mode="HTML"
        )
    except Exception:
        await update.message.reply_text(
            t("msg.help", _lang(update), version=VERSION)
        )


@authorized
async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancel and end the conversation."""
    user_id = update.effective_user.id
    if user_id in user_data:
        del user_data[user_id]

    await update.message.reply_text(
        Messages.cancel(lang=_lang(update)),
        reply_markup=Keyboards.cancel_operation(lang=_lang(update)),
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
            Messages.help_text(lang=_lang(update)),
            reply_markup=Keyboards.help_menu(lang=_lang(update)),
            parse_mode="HTML"
        )
        return ConversationHandler.END
    elif command == "start":
        user = update.effective_user
        await query.edit_message_text(
            Messages.welcome(user.first_name, lang=_lang(update)),
            reply_markup=Keyboards.main_menu(lang=_lang(update)),
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
        Messages.report_selection(lang=_lang(update)),
        reply_markup=Keyboards.report_types(lang=_lang(update)),
        parse_mode="HTML"
    )
    return ConversationState.SELECTING_REPORT_TYPE


async def report_command_from_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Start the report generation process from a callback query."""
    query = update.callback_query

    await query.edit_message_text(
        Messages.report_selection(lang=_lang(update)),
        reply_markup=Keyboards.report_types(lang=_lang(update)),
        parse_mode="HTML"
    )
    return ConversationState.SELECTING_REPORT_TYPE


async def report_type_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle the report type selection."""
    query = update.callback_query
    await query.answer()

    if query.data == "go_back":
        await query.edit_message_text(
            t("msg.cancelled", _lang(update)),
            reply_markup=Keyboards.cancel_operation(lang=_lang(update))
        )
        return ConversationHandler.END

    user_id = update.effective_user.id
    selected_type = query.data.split('_')[-1]

    # Handle TOP-10 report type
    if selected_type == "top10":
        update_user_session(user_id, {"report_type": selected_type})

        await query.edit_message_text(
            Messages.top10_source_selection(lang=_lang(update)),
            reply_markup=Keyboards.top10_sources(lang=_lang(update)),
            parse_mode="HTML"
        )
        return ConversationState.SELECTING_TOP10_SOURCE

    # Initialize user data with session management
    update_user_session(user_id, {"report_type": selected_type})

    # Ask for date range
    await query.edit_message_text(
        Messages.date_selection(selected_type, lang=_lang(update)),
        reply_markup=Keyboards.date_ranges(lang=_lang(update)),
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
            t("msg.session_expired", _lang(update)),
            parse_mode="HTML"
        )
        return ConversationHandler.END

    start_date = session["start_date"]
    end_date = session["end_date"]
    report_type = session["report_type"]

    # Show loading message
    await query.edit_message_text(
        Messages.loading(report_type, start_date, end_date, lang=_lang(update)),
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
            Messages.report_selection(lang=_lang(update)),
            reply_markup=Keyboards.report_types(lang=_lang(update)),
            parse_mode="HTML"
        )
        return ConversationState.SELECTING_REPORT_TYPE

    if query.data == "back_to_source_selection":
        await query.edit_message_text(
            Messages.top10_source_selection(lang=_lang(update)),
            reply_markup=Keyboards.top10_sources(lang=_lang(update)),
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
            "pick.start_year", 1, 6,
            t("pick.prompt_start_year", _lang(update)),
            lang=_lang(update),
        )

        await query.edit_message_text(
            message,
            reply_markup=Keyboards.year_picker(get_year_choices(), "back_to_date_range", lang=_lang(update)),
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
        Messages.date_selection(report_type, lang=_lang(update)),
        reply_markup=Keyboards.date_ranges(lang=_lang(update)),
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
        "pick.start_month", 2, 6,
        t("pick.prompt_start_month", _lang(update), year=selected_year),
        lang=_lang(update),
    )

    await query.edit_message_text(
        message,
        reply_markup=Keyboards.month_picker("back_to_custom_start_year", lang=_lang(update)),
        parse_mode="HTML"
    )

    return ConversationState.SELECTING_CUSTOM_START_MONTH


async def custom_start_month_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle custom start month selection."""
    query = update.callback_query
    await query.answer()

    if query.data == "back_to_custom_start_year":
        message = Messages.custom_date_prompt(
            "pick.start_year", 1, 6,
            t("pick.prompt_start_year", _lang(update)),
            lang=_lang(update),
        )

        await query.edit_message_text(
            message,
            reply_markup=Keyboards.year_picker(get_year_choices(), "back_to_date_range", lang=_lang(update)),
            parse_mode="HTML"
        )
        return ConversationState.SELECTING_CUSTOM_START_YEAR

    user_id = update.effective_user.id
    selected_month = int(query.data.split('_')[-1])
    user_data[user_id]["custom_start_month"] = selected_month

    selected_year = user_data[user_id]["custom_start_year"]
    month_name = t(f"month.{selected_month}", _lang(update))

    message = Messages.custom_date_prompt(
        "pick.start_day", 3, 6,
        t("pick.prompt_start_day", _lang(update), month=month_name, year=selected_year),
        lang=_lang(update),
    )

    await query.edit_message_text(
        message,
        reply_markup=Keyboards.day_picker(selected_year, selected_month, 1, "back_to_custom_start_month", lang=_lang(update)),
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
            "pick.start_month", 2, 6,
            t("pick.prompt_start_month", _lang(update), year=selected_year),
            lang=_lang(update),
        )

        await query.edit_message_text(
            message,
            reply_markup=Keyboards.month_picker("back_to_custom_start_year", lang=_lang(update)),
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
            "pick.end_month", 4, 5,
            t("pick.prompt_end_month", _lang(update),
              start=start_date.strftime('%d.%m.%Y'),
              year=f"{current_year} {t('pick.current_year_note', _lang(update))}"),
            lang=_lang(update),
        )

        await query.edit_message_text(
            message,
            reply_markup=Keyboards.month_picker_range(start_date.month, "back_to_custom_start_day", "custom_end_month", lang=_lang(update)),
            parse_mode="HTML"
        )
        return ConversationState.SELECTING_CUSTOM_END_MONTH
    else:
        # Show year selection for end date (from start year to current year)
        message = Messages.custom_date_prompt(
            "pick.end_year", 4, 6,
            t("pick.prompt_end_year", _lang(update),
              start=start_date.strftime('%d.%m.%Y')),
            lang=_lang(update),
        )

        await query.edit_message_text(
            message,
            reply_markup=Keyboards.end_year_picker(get_year_choices(selected_year), "back_to_custom_start_day", lang=_lang(update)),
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
        month_name = t(f"month.{selected_month}", _lang(update))

        message = Messages.custom_date_prompt(
            "pick.start_day", 3, 6,
            t("pick.prompt_start_day", _lang(update), month=month_name, year=selected_year),
            lang=_lang(update),
        )

        await query.edit_message_text(
            message,
            reply_markup=Keyboards.day_picker(selected_year, selected_month, 1, "back_to_custom_start_month", lang=_lang(update)),
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
        "pick.end_month", 5, 6,
        t("pick.prompt_end_month", _lang(update),
          start=start_date.strftime('%d.%m.%Y'), year=selected_year),
        lang=_lang(update),
    )

    await query.edit_message_text(
        message,
        reply_markup=Keyboards.month_picker_range(start_month, "back_to_custom_end_year", "custom_end_month", lang=_lang(update)),
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
            "pick.end_year", 4, 6,
            t("pick.prompt_end_year", _lang(update),
              start=start_date.strftime('%d.%m.%Y')),
            lang=_lang(update),
        )

        await query.edit_message_text(
            message,
            reply_markup=Keyboards.end_year_picker(get_year_choices(start_date.year), "back_to_custom_start_day", lang=_lang(update)),
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

    month_name = t(f"month.{selected_month}", _lang(update))
    message = Messages.custom_date_prompt(
        "pick.end_day", 6, 6,
        t("pick.prompt_end_day", _lang(update),
          start=start_date.strftime('%d.%m.%Y'), month=month_name, year=selected_year),
        lang=_lang(update),
    )

    await query.edit_message_text(
        message,
        reply_markup=Keyboards.day_picker(selected_year, selected_month, start_day, "back_to_custom_end_month", "custom_end_day", lang=_lang(update)),
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
            "pick.end_month", 5, 6,
            t("pick.prompt_end_month", _lang(update),
              start=start_date.strftime('%d.%m.%Y'), year=selected_year),
            lang=_lang(update),
        )

        await query.edit_message_text(
            message,
            reply_markup=Keyboards.month_picker_range(start_month, "back_to_custom_end_year", "custom_end_month", lang=_lang(update)),
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
        Messages.top10_date_selection(source_name, lang=_lang(update)),
        reply_markup=Keyboards.date_ranges("back_to_source_selection", lang=_lang(update)),
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
            Messages.top10_change_source(start_date, end_date, lang=_lang(update)),
            reply_markup=Keyboards.top10_quick_source_picker(lang=_lang(update)),
            parse_mode="HTML"
        )
    else:
        await query.edit_message_text(t("msg.session_expired", _lang(update)), parse_mode="HTML")

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
            t("msg.session_expired", _lang(update)),
            reply_markup=Keyboards.error_retry(lang=_lang(update)),
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
            t("msg.session_expired", _lang(update)),
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
            text=t("msg.report_ready", _lang(update)),
            reply_markup=Keyboards.top10_post_report(lang=_lang(update)),
            parse_mode="HTML"
        )

    except KeyCRMAPIError as e:
        logger.error(f"KeyCRM API error in TOP-10: {e.message} - {e.error_details}")
        await query.edit_message_text(
            t("msg.api_error", _lang(update)),
            reply_markup=Keyboards.error_retry(lang=_lang(update)),
            parse_mode="HTML"
        )

    except Exception as e:
        logger.error(f"Error generating TOP-10 report: {e}", exc_info=True)
        await query.edit_message_text(
            f"⚠️ Error generating report: {str(e)}",
            reply_markup=Keyboards.error_retry(lang=_lang(update)),
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
            t("msg.session_expired", _lang(update)),
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
            reply_markup=Keyboards.post_report_actions(include_summary=False, lang=_lang(update)),
            parse_mode="HTML"
        )

    except KeyCRMAPIError as e:
        logger.error(f"KeyCRM API error: {e.message} - {e.error_details}")
        await query.edit_message_text(
            t("msg.api_error", _lang(update)),
            reply_markup=Keyboards.error_retry(lang=_lang(update)),
            parse_mode="HTML"
        )

    except Exception as e:
        logger.error(f"Error generating report: {e}", exc_info=True)
        await query.edit_message_text(
            Messages.error(str(e), lang=_lang(update)),
            reply_markup=Keyboards.error_retry(lang=_lang(update)),
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
            t("msg.session_expired", _lang(update)),
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
            Messages.excel_preparing(start_date, end_date, lang=_lang(update)),
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
                Messages.excel_success(start_date, end_date, lang=_lang(update)),
                reply_markup=Keyboards.post_report_actions(include_excel=False, lang=_lang(update)),
                parse_mode="HTML"
            )
        else:
            await query.edit_message_text(
                Messages.excel_error(lang=_lang(update)),
                reply_markup=Keyboards.try_again_or_convert(lang=_lang(update)),
                parse_mode="HTML"
            )

    except Exception as e:
        logger.error(f"Error generating Excel report: {e}")
        await query.edit_message_text(
            Messages.excel_error(str(e), lang=_lang(update)),
            reply_markup=Keyboards.try_again_or_convert(lang=_lang(update)),
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
            t("msg.session_expired", _lang(update)),
            reply_markup=Keyboards.cancel_operation(lang=_lang(update)),
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
        Messages.report_selection(lang=_lang(update)),
        reply_markup=Keyboards.report_types(lang=_lang(update)),
        parse_mode="HTML"
    )
    return ConversationState.SELECTING_REPORT_TYPE


@authorized
async def reply_keyboard_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle 'ℹ️ Help' button from reply keyboard."""
    await update.message.reply_text(
        Messages.help_text(lang=_lang(update)),
        reply_markup=Keyboards.help_menu(lang=_lang(update)),
        parse_mode="HTML"
    )


@authorized
async def dashboard_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send dashboard link when /dashboard is issued."""
    keyboard = [[InlineKeyboardButton(t("btn.open_dashboard", _lang(update)), url=DASHBOARD_URL)]]
    await update.message.reply_text(
        t("msg.dashboard", _lang(update)),
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


@authorized
async def reply_keyboard_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle '📈 Dashboard' button from reply keyboard."""
    return await dashboard_command(update, context)


@authorized
async def reply_keyboard_search(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle '🔍 Search' button from reply keyboard."""
    return await search_command(update, context)


@authorized
async def reply_keyboard_settings(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle '⚙️ Settings' button from reply keyboard."""
    return await settings_command(update, context)


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
            t("access.requested", _lang(update)),
            parse_mode="HTML"
        )
    else:
        # Already has a request
        status = database.get_user_auth_status(user.id)
        if status['status'] == database.STATUS_PENDING:
            await query.edit_message_text(
                t(ACCESS_PENDING_MESSAGE, _lang(update)),
                parse_mode="HTML"
            )
        elif status['status'] == database.STATUS_DENIED:
            await query.edit_message_text(
                t(ACCESS_DENIED_MESSAGE, _lang(update)),
                parse_mode="HTML"
            )

    return ConversationHandler.END


async def notify_admins_new_request(context: ContextTypes.DEFAULT_TYPE, user) -> None:
    """Notify all admins about new access request."""
    if not ADMIN_USER_IDS:
        logger.warning("No admin IDs configured - cannot notify about access request")
        return

    message = (
        t("admin.new_request", DEFAULT_LANGUAGE) + "\n\n"
        f"<code>{user.id}</code>\n"
        f"@{user.username or '—'}\n"
        f"{user.first_name or ''} {user.last_name or ''}\n\n"
        + t("admin.choose_action", DEFAULT_LANGUAGE)
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
        await query.answer(t("admin.only", _lang(update)), show_alert=True)
        return

    # Extract user_id from callback data
    target_user_id = int(query.data.split('_')[-1])

    # Approve user
    success = database.approve_user(target_user_id, admin.id)

    if success:
        await query.answer(t("admin.user_approved", _lang(update)))

        # Update admin message
        user_info = database.get_user_auth_status(target_user_id)
        await query.edit_message_text(
            f"✅ <b>{t('admin.user_approved', _lang(update))}</b>\n\n"
            f"<code>{target_user_id}</code>\n"
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
                    t("access.granted", database.get_user_language(target_user_id))
                ),
                parse_mode="HTML"
            )
        except Exception as e:
            logger.error(f"Failed to notify user {target_user_id} about approval: {e}")
    else:
        await query.answer(t("admin.action_failed", _lang(update)), show_alert=True)


async def auth_deny_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin denies user access."""
    query = update.callback_query
    admin = update.effective_user

    if not is_admin(admin.id):
        await query.answer(t("admin.only", _lang(update)), show_alert=True)
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
            await query.answer(t("admin.user_frozen", _lang(update)))

            # Update admin message
            await query.edit_message_text(
                f"🚫 <b>{t('admin.user_frozen', _lang(update))}</b>\n\n"
                f"<code>{target_user_id}</code>\n"
                f"<b>Username:</b> @{user_info.get('username') or 'N/A'}\n"
                f"<b>Name:</b> {user_info.get('first_name') or ''} {user_info.get('last_name') or ''}\n\n"
                f"<i>Denied {denial_count} times - account frozen</i>",
                parse_mode="HTML"
            )

            # Notify the user - no re-request option
            try:
                await context.bot.send_message(
                    chat_id=target_user_id,
                    text=t(ACCESS_FROZEN_MESSAGE, _lang(update)),
                    parse_mode="HTML"
                )
            except Exception as e:
                logger.error(f"Failed to notify user {target_user_id} about freeze: {e}")
        else:
            await query.answer(t("admin.user_denied", _lang(update)))

            # Update admin message
            await query.edit_message_text(
                f"❌ <b>{t('admin.user_denied', _lang(update))}</b>\n\n"
                f"<code>{target_user_id}</code>\n"
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
        await query.answer(t("admin.action_failed", _lang(update)), show_alert=True)


async def auth_request_again(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle re-request access from denied user."""
    query = update.callback_query
    await query.answer()

    user = update.effective_user

    # Reset status to pending
    success, was_frozen = database.reset_user_to_pending(user.id)

    if was_frozen:
        await query.edit_message_text(
            t(ACCESS_FROZEN_MESSAGE, _lang(update)),
            parse_mode="HTML"
        )
    elif success:
        # Notify admins
        await notify_admins_new_request(context, user)

        await query.edit_message_text(
            t("access.requested", _lang(update)),
            parse_mode="HTML"
        )
    else:
        await query.edit_message_text(
            t("msg.request_failed", _lang(update)),
            parse_mode="HTML"
        )

    return ConversationHandler.END


# ═══════════════════════════════════════════════════════════════════════════
# ADMIN USER MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════

def _build_user_list_ui(lang: str = DEFAULT_LANGUAGE) -> Tuple[str, List[List[InlineKeyboardButton]]]:
    """Build user list message and keyboard for admin management.

    Returns:
        Tuple of (message_text, keyboard_buttons) or (None, None) if no users.
    """
    users = database.get_all_authorized_users()
    frozen_users = database.get_frozen_users()

    if not users and not frozen_users:
        return None, None

    message = ""
    keyboard = []

    # Approved users
    if users:
        message += t("admin.approved_users", lang) + "\n\n"
        for u in users[:15]:
            username = f"@{u['username']}" if u.get('username') else "N/A"
            name = f"{u.get('first_name') or ''} {u.get('last_name') or ''}".strip() or "Unknown"
            last_active = u.get('last_activity') or u.get('reviewed_at') or "Never"

            message += f"• <code>{u['user_id']}</code> - {name} ({username})\n"
            message += f"  {t('admin.last_active', lang)}: {last_active}\n\n"

            keyboard.append([
                InlineKeyboardButton(
                    t("admin.revoke", lang, name=name[:15]),
                    callback_data=f"admin_revoke_{u['user_id']}"
                )
            ])

        if len(users) > 15:
            message += f"<i>{t('admin.and_more', lang, count=len(users) - 15)}</i>\n\n"

    # Frozen users
    if frozen_users:
        message += t("admin.frozen_users", lang) + "\n\n"
        for u in frozen_users[:10]:
            username = f"@{u['username']}" if u.get('username') else "N/A"
            name = f"{u.get('first_name') or ''} {u.get('last_name') or ''}".strip() or "Unknown"
            frozen_at = u.get('reviewed_at') or "Unknown"

            message += f"• <code>{u['user_id']}</code> - {name} ({username})\n"
            message += f"  {t('admin.frozen_at', lang)}: {frozen_at}\n\n"

            keyboard.append([
                InlineKeyboardButton(
                    t("admin.unfreeze", lang, name=name[:15]),
                    callback_data=f"admin_unfreeze_{u['user_id']}"
                )
            ])

        if len(frozen_users) > 10:
            message += f"<i>{t('admin.and_more', lang, count=len(frozen_users) - 10)}</i>\n"

    keyboard.append([InlineKeyboardButton("🔙 Close", callback_data="admin_close")])
    return message, keyboard


async def admin_users_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show list of approved and frozen users for admin management."""
    user = update.effective_user

    if not is_admin(user.id):
        await update.message.reply_text(t("admin.only", _lang(update)), parse_mode="HTML")
        return

    message, keyboard = _build_user_list_ui(_lang(update))

    if message is None:
        await update.message.reply_text(
            t("admin.no_users", _lang(update)),
            parse_mode="HTML"
        )
        return

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
        await query.answer(t("admin.only", _lang(update)), show_alert=True)
        return

    target_user_id = int(query.data.split('_')[-1])
    user_info = database.get_user_auth_status(target_user_id)

    if not user_info:
        await query.answer(t("admin.user_not_found", _lang(update)), show_alert=True)
        return

    # Revoke access
    success = database.revoke_user(target_user_id, admin.id)

    if success:
        await query.answer("Access revoked!")
        # Refresh the user list (silent revoke - no notification to user)
        await show_updated_user_list(query, admin.id)
    else:
        await query.answer(t("admin.action_failed", _lang(update)), show_alert=True)


async def show_updated_user_list(query, admin_id: int) -> None:
    """Show updated user list after revocation/unfreeze."""
    message, keyboard = _build_user_list_ui(_lang(update))

    if message is None:
        await query.edit_message_text(
            t("admin.no_users", _lang(update)),
            parse_mode="HTML"
        )
        return

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
        await query.answer(t("admin.only", _lang(update)), show_alert=True)
        return

    target_user_id = int(query.data.split('_')[-1])
    user_info = database.get_user_auth_status(target_user_id)

    if not user_info:
        await query.answer(t("admin.user_not_found", _lang(update)), show_alert=True)
        return

    # Unfreeze user
    success = database.unfreeze_user(target_user_id, admin.id)

    if success:
        await query.answer(t("admin.user_unfrozen", _lang(update)))

        # Notify the user
        try:
            target_lang = database.get_user_language(target_user_id)
            keyboard = [[
                InlineKeyboardButton(t("btn.request_access", target_lang),
                                     callback_data="auth_request_access")
            ]]
            await context.bot.send_message(
                chat_id=target_user_id,
                text=t("access.unfrozen", target_lang),
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="HTML"
            )
        except Exception as e:
            logger.error(f"Failed to notify user {target_user_id} about unfreeze: {e}")

        # Refresh the user list
        await show_updated_user_list(query, admin.id)
    else:
        await query.answer(t("admin.action_failed", _lang(update)), show_alert=True)


async def admin_close(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Close admin panel."""
    query = update.callback_query
    await query.answer()
    await query.edit_message_text(t("admin.panel_closed", _lang(update)), parse_mode="HTML")


# ═══════════════════════════════════════════════════════════════════════════
# SEARCH HANDLERS
# ═══════════════════════════════════════════════════════════════════════════

@authorized
async def search_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle /search command - start order search."""
    await update.message.reply_text(
        f"{t('search.title', _lang(update))}\n\n"
        f"{t('search.choose_type', _lang(update))}",
        reply_markup=Keyboards.search_type(lang=_lang(update)),
        parse_mode="HTML"
    )
    return ConversationState.SEARCH_WAITING_QUERY


@authorized
async def search_command_from_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle search from callback."""
    query = update.callback_query
    await query.answer()

    await query.edit_message_text(
        f"{t('search.title', _lang(update))}\n\n"
        f"{t('search.choose_type', _lang(update))}",
        reply_markup=Keyboards.search_type(lang=_lang(update)),
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

    lang = _lang(update)
    prompts = {"id": "search.prompt_id", "phone": "search.prompt_phone",
               "email": "search.prompt_email"}

    await query.edit_message_text(
        f"{t('search.title', lang)}\n\n"
        f"{t(prompts.get(search_type, 'search.choose_type'), lang)}",
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
        t("search.searching", _lang(update)),
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
                t("search.nothing_found", _lang(update), query=query_text),
                reply_markup=Keyboards.search_results_actions(lang=_lang(update)),
                parse_mode="HTML"
            )
            return ConversationHandler.END

        # Format results
        lang = _lang(update)
        message = f"{t('search.results_title', lang)}\n"
        message += t("search.found", lang, count=len(orders), query=query_text) + "\n\n"

        for order in orders[:5]:  # Limit to 5 results
            order_id = order.get("id", "?")
            status = order.get("status_group", {}).get("name", t("search.unknown", lang))
            buyer = order.get("buyer", {})
            buyer_name = buyer.get("full_name", t("search.unknown", lang))
            buyer_phone = buyer.get("phone", "N/A")
            total = order.get("grand_total", 0)
            created = order.get("created_at", "")[:10]

            message += f"📦 <b>{t('search.order', lang)} #{order_id}</b>\n"
            message += f"   👤 {buyer_name}\n"
            message += f"   📱 {buyer_phone}\n"
            message += f"   💰 {total} UAH\n"
            message += f"   📊 {status}\n"
            message += f"   📅 {created}\n\n"

        if len(orders) > 5:
            message += f"<i>{t('search.more', lang, count=len(orders) - 5)}</i>\n"

        await searching_msg.edit_text(
            message,
            reply_markup=Keyboards.search_results_actions(lang=_lang(update)),
            parse_mode="HTML"
        )

    except Exception as e:
        logger.error(f"Search error: {e}", exc_info=True)
        await searching_msg.edit_text(
            t("search.failed", _lang(update), error=str(e)),
            reply_markup=Keyboards.search_results_actions(lang=_lang(update)),
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
    lang = database.get_user_language(user_id)

    await update.message.reply_text(
        f"⚙️ <b>{t('settings.title', lang)}</b>",
        reply_markup=Keyboards.settings_menu(prefs, lang),
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
    lang = database.get_user_language(user_id)

    await query.edit_message_text(
        f"⚙️ <b>{t('settings.title', lang)}</b>",
        reply_markup=Keyboards.settings_menu(prefs, lang),
        parse_mode="HTML"
    )
    return ConversationState.SETTINGS_MENU


async def settings_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle settings menu callbacks."""
    query = update.callback_query
    await query.answer()

    user_id = update.effective_user.id
    action = query.data
    lang = database.get_user_language(user_id)

    if action == "settings_language":
        await query.edit_message_text(
            f"🌐 <b>{t('settings.choose_language', lang)}</b>\n\n"
            f"{t('settings.applies_to_report', lang)}",
            reply_markup=Keyboards.settings_language(lang),
            parse_mode="HTML"
        )
    elif action.startswith("set_lang_"):
        chosen = normalize(action.replace("set_lang_", ""))
        database.update_user_preference(user_id, "language", chosen)
        prefs = database.get_user_preferences(user_id) or {}
        # Everything below is rendered in the language just chosen, so the
        # confirmation is itself the proof that it took.
        await query.edit_message_text(
            f"✅ {t('settings.language_set', chosen, name=LANGUAGE_NAMES[chosen])}\n\n"
            f"⚙️ <b>{t('settings.title', chosen)}</b>",
            reply_markup=Keyboards.settings_menu(prefs, chosen, lang=_lang(update)),
            parse_mode="HTML"
        )
    elif action == "settings_timezone":
        await query.edit_message_text(
            f"🌍 <b>{t('settings.timezone', lang)}</b>",
            reply_markup=Keyboards.settings_timezone(lang=_lang(update)),
            parse_mode="HTML"
        )
    elif action == "settings_date_range":
        await query.edit_message_text(
            f"📅 <b>{t('settings.date_range', lang)}</b>",
            reply_markup=Keyboards.settings_date_range(lang=_lang(update)),
            parse_mode="HTML"
        )
    elif action == "settings_notifications":
        await query.edit_message_text(
            f"🔔 <b>{t('settings.notifications', lang)}</b>",
            reply_markup=Keyboards.settings_notifications(lang=_lang(update)),
            parse_mode="HTML"
        )
    elif action == "settings_back":
        prefs = database.get_user_preferences(user_id) or {}
        await query.edit_message_text(
            f"⚙️ <b>{t('settings.title', lang)}</b>",
            reply_markup=Keyboards.settings_menu(prefs, lang),
            parse_mode="HTML"
        )
    elif action.startswith("set_tz_"):
        timezone = action.replace("set_tz_", "")
        database.update_user_preference(user_id, "timezone", timezone)
        prefs = database.get_user_preferences(user_id) or {}
        await query.edit_message_text(
            f"✅ {t('settings.timezone', lang)}: <b>{timezone}</b>\n\n"
            f"⚙️ <b>{t('settings.title', lang)}</b>",
            reply_markup=Keyboards.settings_menu(prefs, lang),
            parse_mode="HTML"
        )
    elif action.startswith("set_range_"):
        date_range = action.replace("set_range_", "")
        database.update_user_preference(user_id, "default_date_range", date_range)
        prefs = database.get_user_preferences(user_id) or {}
        range_label = t(f"range.{date_range}", lang) if date_range in (
            'today', 'week', 'month') else date_range
        await query.edit_message_text(
            f"✅ {t('settings.date_range', lang)}: <b>{range_label}</b>\n\n"
            f"⚙️ <b>{t('settings.title', lang)}</b>",
            reply_markup=Keyboards.settings_menu(prefs, lang),
            parse_mode="HTML"
        )
    elif action.startswith("set_notif_"):
        enabled = action.replace("set_notif_", "") == "1"
        database.update_user_preference(user_id, "notifications_enabled", 1 if enabled else 0)
        prefs = database.get_user_preferences(user_id) or {}
        status = t('settings.on', lang) if enabled else t('settings.off', lang)
        await query.edit_message_text(
            f"✅ {t('settings.notifications', lang)}: <b>{status}</b>\n\n"
            f"⚙️ <b>{t('settings.title', lang)}</b>",
            reply_markup=Keyboards.settings_menu(prefs, lang),
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

            # Fetch last year's revenue for the equivalent period
            # Use ISO week alignment so we compare same weekdays
            # e.g. daily: same weekday in same ISO week last year
            #      weekly: same ISO week last year (Mon-Sun)
            #      monthly: same calendar month last year
            yoy_line = ""
            try:
                if period_type == "monthly":
                    ly_start = period["start"].replace(year=period["start"].year - 1)
                    # Use same day count as current period for fair comparison
                    ly_end = ly_start + (period["end"] - period["start"])
                else:
                    # ISO week-based: find the same ISO weekday/week last year
                    iso_year, iso_week, iso_dow = period["start"].isocalendar()
                    from datetime import date
                    # Same ISO week, same weekday, previous year
                    ly_start = date.fromisocalendar(iso_year - 1, iso_week, iso_dow)
                    ly_end = ly_start + (period["end"] - period["start"])
                _, _, ly_total_orders, ly_revenue_by_source, _ = (
                    await asyncio.to_thread(
                        report_service.aggregate_sales_data,
                        target_date=(ly_start.strftime('%Y-%m-%d'), ly_end.strftime('%Y-%m-%d')),
                        tz_name=DEFAULT_TIMEZONE,
                        telegram_manager_ids=TELEGRAM_MANAGER_IDS
                    )
                )
                ly_revenue = sum(ly_revenue_by_source.values())
                if ly_revenue > 0:
                    growth_pct = (total_revenue - ly_revenue) / ly_revenue * 100
                    sign = "+" if growth_pct >= 0 else ""
                    arrow = "📈" if growth_pct >= 0 else "📉"
                    yoy_line = (
                        f"\n{arrow} YoY: {bold(f'{sign}{growth_pct:.0f}%')}"
                        f" (₴{ly_revenue:,.0f} last year)"
                    )
            except (ValueError, Exception) as e:
                logger.warning(f"Failed to fetch YoY data for milestone: {e}")

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
                f"📦 Orders: {bold(str(total_orders))}{yoy_line}\n\n"
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
