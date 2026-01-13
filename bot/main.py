"""
Main entry point for the refactored KeyCRM Telegram Bot.

This module wires together all components and starts the bot.
"""
import logging
from telegram import Update, BotCommand, MenuButtonWebApp, WebAppInfo
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    ConversationHandler,
    filters,
)

from bot.config import BOT_TOKEN, KEYCRM_API_KEY, ConversationState
from bot.api_client import KeyCRMClient
from bot.services import ReportService
from bot import handlers
from bot import database

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)


async def setup_command_menu(application: Application) -> None:
    """Set up the bot commands in the menu."""
    try:
        commands = [
            BotCommand("start", "👋 Start the bot"),
            BotCommand("report", "📊 Generate a sales report"),
            BotCommand("search", "🔍 Search orders"),
            BotCommand("settings", "⚙️ User settings"),
            BotCommand("dashboard", "📈 Open sales dashboard"),
            BotCommand("help", "ℹ️ Show help information"),
            BotCommand("cancel", "🛑 Cancel current operation")
        ]

        logger.info("Setting bot commands...")
        await application.bot.set_my_commands(commands)
        logger.info("Bot commands set successfully")

        # Set menu button to open web dashboard (requires HTTPS)
        try:
            from bot.config import DASHBOARD_URL
            logger.info("Setting menu button to open dashboard...")
            web_app = WebAppInfo(url=DASHBOARD_URL)
            menu_button = MenuButtonWebApp(text="📈 Dashboard", web_app=web_app)
            await application.bot.set_chat_menu_button(menu_button=menu_button)
            logger.info(f"Menu button set to open: {DASHBOARD_URL}")
        except Exception as menu_error:
            logger.warning(f"Could not set menu button: {menu_error} (not critical)")

    except Exception as e:
        logger.error(f"Error in setup_command_menu: {e}", exc_info=True)


def create_conversation_handler() -> ConversationHandler:
    """Create and configure the conversation handler with all states."""
    return ConversationHandler(
        entry_points=[
            CommandHandler("report", handlers.report_command),
            CommandHandler("search", handlers.search_command),
            CommandHandler("settings", handlers.settings_command),
            MessageHandler(filters.Regex(r"^📊 Report$"), handlers.reply_keyboard_report),
            CallbackQueryHandler(handlers.report_command_from_callback, pattern=r"^cmd_report$"),
            CallbackQueryHandler(handlers.search_command_from_callback, pattern=r"^cmd_search$"),
            CallbackQueryHandler(handlers.settings_command_from_callback, pattern=r"^cmd_settings$"),
            CallbackQueryHandler(handlers.command_button_handler, pattern=r"^cmd_"),
            CallbackQueryHandler(handlers.convert_report_format, pattern=r"^convert_to_"),
            CallbackQueryHandler(handlers.quick_report_callback, pattern=r"^quick_"),
            CallbackQueryHandler(handlers.change_top10_source, pattern=r"^change_top10_source$"),
            CallbackQueryHandler(handlers.quick_top10_callback, pattern=r"^quick_top10_")
        ],
        states={
            ConversationState.SELECTING_REPORT_TYPE: [
                CallbackQueryHandler(handlers.report_type_callback, pattern=r"^report_type_|^go_back")
            ],
            ConversationState.SELECTING_DATE_RANGE: [
                CallbackQueryHandler(handlers.date_range_callback, pattern=r"^range_|^back_to_report_type|^back_to_source_selection")
            ],
            ConversationState.SELECTING_CUSTOM_START_YEAR: [
                CallbackQueryHandler(handlers.custom_start_year_callback, pattern=r"^custom_start_year_|^back_to_date_range")
            ],
            ConversationState.SELECTING_CUSTOM_START_MONTH: [
                CallbackQueryHandler(handlers.custom_start_month_callback, pattern=r"^custom_start_month_|^back_to_custom_start_year")
            ],
            ConversationState.SELECTING_CUSTOM_START_DAY: [
                CallbackQueryHandler(handlers.custom_start_day_callback, pattern=r"^custom_start_day_|^back_to_custom_start_month")
            ],
            ConversationState.SELECTING_CUSTOM_END_YEAR: [
                CallbackQueryHandler(handlers.custom_end_year_callback, pattern=r"^custom_end_year_|^back_to_custom_start_day")
            ],
            ConversationState.SELECTING_CUSTOM_END_MONTH: [
                CallbackQueryHandler(handlers.custom_end_month_callback, pattern=r"^custom_end_month_|^back_to_custom_end_year")
            ],
            ConversationState.SELECTING_CUSTOM_END_DAY: [
                CallbackQueryHandler(handlers.custom_end_day_callback, pattern=r"^custom_end_day_|^back_to_custom_end_month")
            ],
            ConversationState.SELECTING_TOP10_SOURCE: [
                CallbackQueryHandler(handlers.top10_source_callback, pattern=r"^top10_source_|^back_to_report_type")
            ],
            ConversationState.SEARCH_WAITING_QUERY: [
                CallbackQueryHandler(handlers.search_type_callback, pattern=r"^search_type_"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handlers.search_query_handler)
            ],
            ConversationState.SETTINGS_MENU: [
                CallbackQueryHandler(handlers.settings_callback, pattern=r"^settings_|^set_")
            ],
        },
        fallbacks=[CommandHandler("cancel", handlers.cancel_command)],
        per_message=False,
    )


def main() -> None:
    """Start the bot."""
    # Validate configuration
    if not BOT_TOKEN:
        logger.error("No BOT_TOKEN found in environment variables")
        logger.error("Please set the BOT_TOKEN environment variable or add it to your .env file")
        return

    if not KEYCRM_API_KEY:
        logger.error("No KEYCRM_API_KEY found in environment variables")
        logger.error("Please set the KEYCRM_API_KEY environment variable or add it to your .env file")
        return

    logger.info("Starting KeyCRM Telegram bot (Refactored Version)...")

    # Initialize database
    logger.info("Initializing database...")
    database.init_database()

    logger.debug("API Key configured successfully")

    # Initialize services
    api_client = KeyCRMClient(KEYCRM_API_KEY)
    report_service = ReportService(api_client)

    # Inject service into handlers module
    handlers.report_service = report_service

    # Create the Application
    application = Application.builder().token(BOT_TOKEN).build()

    logger.info("Registering command handlers...")

    # Add conversation handler
    conv_handler = create_conversation_handler()
    application.add_handler(conv_handler)

    # Add basic command handlers
    application.add_handler(CommandHandler("start", handlers.start_command))
    application.add_handler(CommandHandler("help", handlers.help_command))
    application.add_handler(CommandHandler("cancel", handlers.cancel_command))
    application.add_handler(CommandHandler("dashboard", handlers.dashboard_command))

    # Add general callback query handler for unhandled callbacks
    application.add_handler(CallbackQueryHandler(handlers.command_button_handler, pattern=r"^cmd_"))

    # Add reply keyboard text handlers
    # Note: Dashboard button uses WebApp, opens directly without message
    application.add_handler(MessageHandler(filters.Regex(r"^ℹ️ Help$"), handlers.reply_keyboard_help))
    application.add_handler(MessageHandler(filters.Regex(r"^🔍 Search$"), handlers.reply_keyboard_search))
    application.add_handler(MessageHandler(filters.Regex(r"^⚙️ Settings$"), handlers.reply_keyboard_settings))

    # Add authorization handlers
    application.add_handler(CallbackQueryHandler(handlers.auth_request_access, pattern=r"^auth_request_access$"))
    application.add_handler(CallbackQueryHandler(handlers.auth_request_again, pattern=r"^auth_request_again$"))
    application.add_handler(CallbackQueryHandler(handlers.auth_approve_user, pattern=r"^auth_approve_\d+$"))
    application.add_handler(CallbackQueryHandler(handlers.auth_deny_user, pattern=r"^auth_deny_\d+$"))

    # Add admin user management
    application.add_handler(CommandHandler("users", handlers.admin_users_command))
    application.add_handler(CallbackQueryHandler(handlers.admin_revoke_user, pattern=r"^admin_revoke_\d+$"))
    application.add_handler(CallbackQueryHandler(handlers.admin_unfreeze_user, pattern=r"^admin_unfreeze_\d+$"))
    application.add_handler(CallbackQueryHandler(handlers.admin_close, pattern=r"^admin_close$"))

    # Add startup action to set up command menu
    async def set_commands(context):
        try:
            await setup_command_menu(application)
            logger.info("Command menu setup complete")
        except Exception as e:
            logger.error(f"Error setting up command menu: {e}", exc_info=True)

    # Periodic session cleanup job
    async def cleanup_sessions(context):
        count = handlers.cleanup_expired_sessions()
        if count > 0:
            logger.debug(f"Cleaned up {count} expired sessions")

    # Periodic database cleanup job
    async def cleanup_database(context):
        cache_count = database.cache_cleanup()
        history_count = database.cleanup_old_history(days=30)
        if cache_count > 0 or history_count > 0:
            logger.debug(f"DB cleanup: {cache_count} cache, {history_count} history")

    # Periodic inactive user revocation (runs daily)
    async def revoke_inactive_users(context):
        revoked_count = database.revoke_inactive_users(days=45)
        if revoked_count > 0:
            logger.info(f"Revoked {revoked_count} inactive users (45+ days)")

    # Set up command menu at startup
    application.job_queue.run_once(set_commands, 1)

    # Schedule milestone check job - runs at 23:30 Kyiv time daily
    from datetime import time as dt_time
    from zoneinfo import ZoneInfo
    milestone_time = dt_time(hour=23, minute=30, tzinfo=ZoneInfo("Europe/Kyiv"))
    application.job_queue.run_daily(
        handlers.check_and_broadcast_milestones,
        time=milestone_time,
        name="milestone_check"
    )
    logger.info(f"Milestone check scheduled daily at {milestone_time}")

    # Run session cleanup every 10 minutes
    application.job_queue.run_repeating(cleanup_sessions, interval=600, first=60)

    # Run database cleanup every hour
    application.job_queue.run_repeating(cleanup_database, interval=3600, first=120)

    # Run inactive user revocation daily (86400 seconds = 24 hours)
    application.job_queue.run_repeating(revoke_inactive_users, interval=86400, first=300)

    logger.info("Bot initialized. Starting polling...")

    # Start the bot
    try:
        application.run_polling(allowed_updates=Update.ALL_TYPES)
    except Exception as e:
        logger.error(f"Error starting bot: {e}", exc_info=True)


if __name__ == "__main__":
    main()
