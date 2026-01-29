"""
Common bot handlers: start, help, cancel, dashboard.
"""
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler

from bot.config import DASHBOARD_URL
from bot.keyboards import Keyboards, ReplyKeyboards
from bot.formatters import Messages
from bot.handlers.base import authorized, get_user_session, _user_data, _user_data_lock

logger = logging.getLogger(__name__)


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
    with _user_data_lock:
        if user_id in _user_data:
            del _user_data[user_id]

    await update.message.reply_text(
        Messages.cancel(),
        reply_markup=Keyboards.cancel_operation(),
        parse_mode="HTML"
    )

    return ConversationHandler.END


@authorized
async def command_button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle command buttons from menus (cmd_report, cmd_help, cmd_start)."""
    # Import here to avoid circular imports
    from bot.handlers.reports import report_command_from_callback

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
async def reply_keyboard_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle 'Help' button from reply keyboard."""
    await update.message.reply_text(
        Messages.help_text(),
        reply_markup=Keyboards.help_menu(),
        parse_mode="HTML"
    )


@authorized
async def reply_keyboard_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle 'Dashboard' button from reply keyboard."""
    keyboard = [[InlineKeyboardButton("📈 Open Dashboard", url=DASHBOARD_URL)]]
    await update.message.reply_text(
        f"📈 <b>Sales Dashboard</b>\n\n"
        f"View interactive charts and analytics:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


@authorized
async def reply_keyboard_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle 'Report' button from reply keyboard."""
    from bot.handlers.reports import report_command
    return await report_command(update, context)


@authorized
async def reply_keyboard_search(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle 'Search' button from reply keyboard."""
    from bot.handlers.search import search_command
    return await search_command(update, context)


@authorized
async def reply_keyboard_settings(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle 'Settings' button from reply keyboard."""
    from bot.handlers.settings import settings_command
    return await settings_command(update, context)
