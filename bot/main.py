"""
Main entry point for the refactored KeyCRM Telegram Bot.

This module wires together all components and starts the bot.
"""
import logging
from telegram import Update, BotCommand, MenuButtonCommands
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    ConversationHandler,
)

from bot.config import BOT_TOKEN, KEYCRM_API_KEY, ConversationState
from bot.api_client import KeyCRMClient
from bot.services import ReportService
from bot import handlers

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
            BotCommand("help", "ℹ️ Show help information"),
            BotCommand("report", "📊 Generate a sales report"),
            BotCommand("cancel", "🛑 Cancel current operation")
        ]

        print("Setting bot commands...")
        await application.bot.set_my_commands(commands)
        print("Bot commands set successfully")

        # Set menu button
        try:
            print("Setting menu button...")
            await application.bot.set_chat_menu_button(menu_button=MenuButtonCommands())
            print("Menu button set successfully")
        except Exception as menu_error:
            print(f"Warning: Could not set menu button: {menu_error}")
            print("This is not critical - the commands will still work.")

    except Exception as e:
        print(f"Error in setup_command_menu: {e}")


def create_conversation_handler() -> ConversationHandler:
    """Create and configure the conversation handler with all states."""
    return ConversationHandler(
        entry_points=[
            CommandHandler("report", handlers.report_command),
            CallbackQueryHandler(handlers.report_command_from_callback, pattern=r"^cmd_report$"),
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
                CallbackQueryHandler(handlers.date_range_callback, pattern=r"^range_|^back_to_report_type")
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
        },
        fallbacks=[CommandHandler("cancel", handlers.cancel_command)],
        per_message=False,
    )


def main() -> None:
    """Start the bot."""
    # Validate configuration
    if not BOT_TOKEN:
        print("Error: No BOT_TOKEN found in environment variables.")
        print("Please set the BOT_TOKEN environment variable or add it to your .env file.")
        return

    if not KEYCRM_API_KEY:
        print("Error: No KEYCRM_API_KEY found in environment variables.")
        print("Please set the KEYCRM_API_KEY environment variable or add it to your .env file.")
        return

    print(f"Starting KeyCRM Telegram bot (Refactored Version)...")
    print(f"API Key: {KEYCRM_API_KEY[:10] if KEYCRM_API_KEY else 'NOT FOUND'}...")

    # Initialize services
    api_client = KeyCRMClient(KEYCRM_API_KEY)
    report_service = ReportService(api_client)

    # Inject service into handlers module
    handlers.report_service = report_service

    # Create the Application
    application = Application.builder().token(BOT_TOKEN).build()

    print("Registering command handlers...")

    # Add conversation handler
    conv_handler = create_conversation_handler()
    application.add_handler(conv_handler)

    # Add basic command handlers
    application.add_handler(CommandHandler("start", handlers.start_command))
    application.add_handler(CommandHandler("help", handlers.help_command))
    application.add_handler(CommandHandler("cancel", handlers.cancel_command))

    # Add general callback query handler for unhandled callbacks
    application.add_handler(CallbackQueryHandler(handlers.command_button_handler, pattern=r"^cmd_"))

    # Add startup action to set up command menu
    async def set_commands(context):
        try:
            await setup_command_menu(application)
            print("Command menu setup complete.")
        except Exception as e:
            print(f"Error setting up command menu: {e}")

    # Set up command menu at startup
    application.job_queue.run_once(set_commands, 1)

    print("Bot initialized. Starting polling...")

    # Start the bot
    try:
        application.run_polling(allowed_updates=Update.ALL_TYPES)
    except Exception as e:
        print(f"Error starting bot: {e}")


if __name__ == "__main__":
    main()
