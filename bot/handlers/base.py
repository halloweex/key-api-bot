"""
Base utilities for bot handlers.

Provides:
- Authorization decorator
- Session management
- Shared constants and imports
"""
import logging
import threading
from datetime import datetime, timedelta, date
from functools import wraps
from typing import Dict, Any, Optional, Tuple
from zoneinfo import ZoneInfo

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler

from bot import database
from bot.config import (
    ConversationState,
    DEFAULT_TIMEZONE,
    is_admin,
)
from bot.services import ReportService

logger = logging.getLogger(__name__)

# Session timeout in minutes
SESSION_TIMEOUT_MINUTES = 30

# ═══════════════════════════════════════════════════════════════════════════
# ACCESS CONTROL MESSAGES
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


# ═══════════════════════════════════════════════════════════════════════════
# SESSION MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════

# Global user data storage with session management
_user_data: Dict[int, Dict[str, Any]] = {}
_user_data_lock = threading.RLock()

# Global service instance (injected in main.py)
report_service: Optional[ReportService] = None


def set_report_service(service: ReportService) -> None:
    """Inject the report service instance."""
    global report_service
    report_service = service


def get_report_service() -> ReportService:
    """Get the report service instance."""
    if report_service is None:
        raise RuntimeError("Report service not initialized")
    return report_service


def get_user_session(user_id: int) -> Optional[Dict[str, Any]]:
    """
    Get user session data if not expired.

    Returns:
        User session dict or None if expired/missing
    """
    with _user_data_lock:
        session = _user_data.get(user_id)
        if session:
            last_activity = session.get('last_activity')
            if last_activity:
                elapsed = datetime.now() - last_activity
                if elapsed.total_seconds() > SESSION_TIMEOUT_MINUTES * 60:
                    del _user_data[user_id]
                    logger.debug(f"Session expired for user {user_id}")
                    return None
            session['last_activity'] = datetime.now()
        return session


def create_user_session(user_id: int, initial_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Create new user session with default data."""
    with _user_data_lock:
        _user_data[user_id] = {
            'last_activity': datetime.now(),
            **(initial_data or {})
        }
        return _user_data[user_id]


def update_user_session(user_id: int, data: Dict[str, Any]) -> Dict[str, Any]:
    """Update existing session or create new one."""
    with _user_data_lock:
        session = get_user_session(user_id)
        if session is None:
            session = create_user_session(user_id)
        session.update(data)
        session['last_activity'] = datetime.now()
        return session


def cleanup_expired_sessions() -> int:
    """Remove all expired sessions. Returns count of removed sessions."""
    removed = 0
    with _user_data_lock:
        expired_users = []
        for user_id, session in _user_data.items():
            last_activity = session.get('last_activity')
            if last_activity:
                elapsed = datetime.now() - last_activity
                if elapsed.total_seconds() > SESSION_TIMEOUT_MINUTES * 60:
                    expired_users.append(user_id)

        for user_id in expired_users:
            del _user_data[user_id]
            removed += 1

    if removed > 0:
        logger.info(f"Cleaned up {removed} expired sessions")
    return removed


# ═══════════════════════════════════════════════════════════════════════════
# AUTHORIZATION DECORATOR
# ═══════════════════════════════════════════════════════════════════════════

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


# ═══════════════════════════════════════════════════════════════════════════
# DATE UTILITIES
# ═══════════════════════════════════════════════════════════════════════════

def calculate_date_range(range_name: str) -> Tuple[date, date]:
    """Calculate date range from preset name."""
    tz = ZoneInfo(DEFAULT_TIMEZONE)
    today = datetime.now(tz).date()

    if range_name == "today":
        return today, today
    elif range_name == "yesterday":
        yesterday = today - timedelta(days=1)
        return yesterday, yesterday
    elif range_name == "week":
        start = today - timedelta(days=today.weekday())
        return start, today
    elif range_name == "month":
        start = today.replace(day=1)
        return start, today
    else:
        return today, today
