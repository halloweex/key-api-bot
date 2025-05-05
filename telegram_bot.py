import os
import logging
from datetime import datetime, timedelta, date
import pytz
import calendar
from collections import defaultdict

# Updated Telegram imports for python-telegram-bot v20+
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand, MenuButtonCommands
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
)

# Other imports remain the same
from dateutil.relativedelta import relativedelta
from keycrm_api import KeyCRMAPI, source_dct
from dotenv import load_dotenv

# ─── Config ─────────────────────────────────────────────────────────────────
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")

# Enable logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# States for the conversation
SELECTING_REPORT_TYPE = 0
SELECTING_DATE_RANGE = 1
SELECTING_CUSTOM_START_YEAR = 2
SELECTING_CUSTOM_START_MONTH = 3
SELECTING_CUSTOM_START_DAY = 4
SELECTING_CUSTOM_END_YEAR = 5
SELECTING_CUSTOM_END_MONTH = 6
SELECTING_CUSTOM_END_DAY = 7
GENERATING_REPORT = 8

# Initialize KeyCRM client
API_KEY = os.getenv("KEYCRM_API_KEY")
keycrm_client = KeyCRMAPI(API_KEY)

# Date range data storage
user_data = {}


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Send a message when the command /start is issued."""
    user = update.effective_user
    await update.message.reply_text(
        f"Hi {user.first_name}! I'm your KeyCRM sales report bot.\n\n"
        f"Use the menu or type /help to see all available commands."
    )
    return ConversationHandler.END


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a message when the command /help is issued."""
    help_text = (
        "<b>📊 KeyCRM Sales Report Bot 📊</b>\n\n"
        "<b>Available Commands:</b>\n"
        "/report - Generate a sales report\n"
        "/cancel - Cancel the current operation\n"
        "/help - Show this help message\n\n"
        "To generate a report, use the /report command and follow the prompts."
    )

    try:
        await update.message.reply_text(help_text, parse_mode="HTML")
    except Exception:
        # Fallback without formatting
        await update.message.reply_text(
            "📊 KeyCRM Sales Report Bot 📊\n\n"
            "Available Commands:\n"
            "/report - Generate a sales report\n"
            "/cancel - Cancel the current operation\n"
            "/help - Show this help message\n\n"
            "To generate a report, use the /report command and follow the prompts."
        )


async def report_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Start the report generation process by asking for the report type."""
    # Ask for report type first
    keyboard = [
        [
            InlineKeyboardButton("Summary Report", callback_data="report_type_summary"),
            InlineKeyboardButton("Excel Report", callback_data="report_type_excel")
        ],
        [
            InlineKeyboardButton("Go Back", callback_data="go_back")
        ]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Please select the report type:", reply_markup=reply_markup)

    return SELECTING_REPORT_TYPE


async def report_type_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle the report type selection."""
    query = update.callback_query
    await query.answer()

    if query.data == "go_back":
        await query.edit_message_text("Operation canceled. Use /report to start again.")
        return ConversationHandler.END

    user_id = update.effective_user.id
    selected_type = query.data.split('_')[-1]

    # Initialize the user data
    if user_id not in user_data:
        user_data[user_id] = {"report_type": selected_type}
    else:
        user_data[user_id]["report_type"] = selected_type

    # Now ask for date range
    keyboard = [
        [
            InlineKeyboardButton("Today", callback_data="range_today"),
            InlineKeyboardButton("Yesterday", callback_data="range_yesterday")
        ],
        [
            InlineKeyboardButton("This Week", callback_data="range_thisweek"),
            InlineKeyboardButton("This Month", callback_data="range_thismonth")
        ],
        [
            InlineKeyboardButton("Custom Date Range", callback_data="range_custom")
        ],
        [
            InlineKeyboardButton("Go Back", callback_data="back_to_report_type")
        ]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(
        f"Selected report type: {selected_type.capitalize()}\n\n"
        f"Now please select the date range:",
        reply_markup=reply_markup
    )

    return SELECTING_DATE_RANGE


async def date_range_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle the date range selection."""
    query = update.callback_query
    await query.answer()

    if query.data == "back_to_report_type":
        # Go back to report type selection
        keyboard = [
            [
                InlineKeyboardButton("Summary Report", callback_data="report_type_summary"),
                InlineKeyboardButton("Excel Report", callback_data="report_type_excel")
            ],
            [
                InlineKeyboardButton("Go Back", callback_data="go_back")
            ]
        ]

        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text("Please select the report type:", reply_markup=reply_markup)
        return SELECTING_REPORT_TYPE

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
        # Monday as the first day of the week
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
        # Start custom date selection flow
        # Get current year and previous 2 years
        current_year = datetime.now().year
        keyboard = [
            [InlineKeyboardButton(str(year), callback_data=f"custom_start_year_{year}")
             for year in range(current_year - 2, current_year + 1)]
        ]
        keyboard.append([InlineKeyboardButton("Go Back", callback_data="back_to_date_range")])

        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            f"Please select the START year for your custom date range:",
            reply_markup=reply_markup
        )
        return SELECTING_CUSTOM_START_YEAR

    # Fallback
    await query.edit_message_text("Invalid selection. Please use /report to start again.")
    return ConversationHandler.END


async def custom_start_year_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle the selection of the custom start year."""
    query = update.callback_query
    await query.answer()

    if query.data == "back_to_date_range":
        return await back_to_date_range(update, context)

    user_id = update.effective_user.id
    selected_year = int(query.data.split('_')[-1])

    if user_id not in user_data:
        user_data[user_id] = {}

    user_data[user_id]["custom_start_year"] = selected_year

    # Create month buttons (Jan-Dec)
    keyboard = []
    row = []
    for month in range(1, 13):
        month_name = calendar.month_abbr[month]
        row.append(InlineKeyboardButton(month_name, callback_data=f"custom_start_month_{month}"))
        if month % 4 == 0:
            keyboard.append(row)
            row = []

    keyboard.append([InlineKeyboardButton("Go Back", callback_data="back_to_custom_start_year")])

    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(
        f"Selected start year: {selected_year}\n"
        f"Now select the START month:",
        reply_markup=reply_markup
    )

    return SELECTING_CUSTOM_START_MONTH


async def custom_start_month_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle the selection of the custom start month."""
    query = update.callback_query
    await query.answer()

    if query.data == "back_to_custom_start_year":
        # Go back to year selection
        current_year = datetime.now().year
        keyboard = [
            [InlineKeyboardButton(str(year), callback_data=f"custom_start_year_{year}")
             for year in range(current_year - 2, current_year + 1)]
        ]
        keyboard.append([InlineKeyboardButton("Go Back", callback_data="back_to_date_range")])

        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            f"Please select the START year for your custom date range:",
            reply_markup=reply_markup
        )
        return SELECTING_CUSTOM_START_YEAR

    user_id = update.effective_user.id
    selected_month = int(query.data.split('_')[-1])
    user_data[user_id]["custom_start_month"] = selected_month

    # Get the number of days in the selected month and year
    selected_year = user_data[user_id]["custom_start_year"]
    num_days = calendar.monthrange(selected_year, selected_month)[1]

    # Create day buttons (1-31, depending on month)
    keyboard = []
    row = []
    for day in range(1, num_days + 1):
        row.append(InlineKeyboardButton(str(day), callback_data=f"custom_start_day_{day}"))
        if len(row) == 7 or day == num_days:
            keyboard.append(row)
            row = []

    keyboard.append([InlineKeyboardButton("Go Back", callback_data="back_to_custom_start_month")])

    reply_markup = InlineKeyboardMarkup(keyboard)
    month_name = calendar.month_name[selected_month]
    await query.edit_message_text(
        f"Selected start date: {month_name} {selected_year}\n"
        f"Now select the START day:",
        reply_markup=reply_markup
    )

    return SELECTING_CUSTOM_START_DAY


async def custom_start_day_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle the selection of the custom start day."""
    query = update.callback_query
    await query.answer()

    if query.data == "back_to_custom_start_month":
        # Go back to month selection
        user_id = update.effective_user.id
        selected_year = user_data[user_id]["custom_start_year"]

        keyboard = []
        row = []
        for month in range(1, 13):
            month_name = calendar.month_abbr[month]
            row.append(InlineKeyboardButton(month_name, callback_data=f"custom_start_month_{month}"))
            if month % 4 == 0:
                keyboard.append(row)
                row = []

        keyboard.append([InlineKeyboardButton("Go Back", callback_data="back_to_custom_start_year")])

        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            f"Selected start year: {selected_year}\n"
            f"Now select the START month:",
            reply_markup=reply_markup
        )
        return SELECTING_CUSTOM_START_MONTH

    user_id = update.effective_user.id
    selected_day = int(query.data.split('_')[-1])

    # Save the complete start date
    selected_year = user_data[user_id]["custom_start_year"]
    selected_month = user_data[user_id]["custom_start_month"]
    user_data[user_id]["start_date"] = datetime(selected_year, selected_month, selected_day).date()

    # Now move to selecting end year
    current_year = datetime.now().year
    keyboard = [
        [InlineKeyboardButton(str(year), callback_data=f"custom_end_year_{year}")
         for year in range(selected_year, current_year + 1)]
    ]
    keyboard.append([InlineKeyboardButton("Go Back", callback_data="back_to_custom_start_day")])

    reply_markup = InlineKeyboardMarkup(keyboard)
    start_date = user_data[user_id]["start_date"]
    await query.edit_message_text(
        f"Selected start date: {start_date.strftime('%Y-%m-%d')}\n"
        f"Now select the END year:",
        reply_markup=reply_markup
    )

    return SELECTING_CUSTOM_END_YEAR


async def custom_end_year_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle the selection of the custom end year."""
    query = update.callback_query
    await query.answer()

    if query.data == "back_to_custom_start_day":
        # Go back to day selection
        user_id = update.effective_user.id
        selected_year = user_data[user_id]["custom_start_year"]
        selected_month = user_data[user_id]["custom_start_month"]
        num_days = calendar.monthrange(selected_year, selected_month)[1]

        keyboard = []
        row = []
        for day in range(1, num_days + 1):
            row.append(InlineKeyboardButton(str(day), callback_data=f"custom_start_day_{day}"))
            if len(row) == 7 or day == num_days:
                keyboard.append(row)
                row = []

        keyboard.append([InlineKeyboardButton("Go Back", callback_data="back_to_custom_start_month")])

        reply_markup = InlineKeyboardMarkup(keyboard)
        month_name = calendar.month_name[selected_month]
        await query.edit_message_text(
            f"Selected start date: {month_name} {selected_year}\n"
            f"Now select the START day:",
            reply_markup=reply_markup
        )
        return SELECTING_CUSTOM_START_DAY

    user_id = update.effective_user.id
    selected_year = int(query.data.split('_')[-1])
    user_data[user_id]["custom_end_year"] = selected_year

    start_date = user_data[user_id]["start_date"]

    # Determine which months to show based on the selected year
    start_month = 1
    if selected_year == start_date.year:
        start_month = start_date.month

    # Create month buttons
    keyboard = []
    row = []
    for month in range(start_month, 13):
        month_name = calendar.month_abbr[month]
        row.append(InlineKeyboardButton(month_name, callback_data=f"custom_end_month_{month}"))
        if len(row) == 4:
            keyboard.append(row)
            row = []

    if row:  # Add any remaining buttons
        keyboard.append(row)

    keyboard.append([InlineKeyboardButton("Go Back", callback_data="back_to_custom_end_year")])

    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(
        f"Selected start date: {start_date.strftime('%Y-%m-%d')}\n"
        f"Selected end year: {selected_year}\n"
        f"Now select the END month:",
        reply_markup=reply_markup
    )

    return SELECTING_CUSTOM_END_MONTH


async def custom_end_month_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle the selection of the custom end month."""
    query = update.callback_query
    await query.answer()

    if query.data == "back_to_custom_end_year":
        # Go back to end year selection
        user_id = update.effective_user.id
        start_date = user_data[user_id]["start_date"]
        current_year = datetime.now().year

        keyboard = [
            [InlineKeyboardButton(str(year), callback_data=f"custom_end_year_{year}")
             for year in range(start_date.year, current_year + 1)]
        ]
        keyboard.append([InlineKeyboardButton("Go Back", callback_data="back_to_custom_start_day")])

        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            f"Selected start date: {start_date.strftime('%Y-%m-%d')}\n"
            f"Now select the END year:",
            reply_markup=reply_markup
        )
        return SELECTING_CUSTOM_END_YEAR

    user_id = update.effective_user.id
    selected_month = int(query.data.split('_')[-1])
    user_data[user_id]["custom_end_month"] = selected_month

    # Get the number of days in the selected month and year
    selected_year = user_data[user_id]["custom_end_year"]
    num_days = calendar.monthrange(selected_year, selected_month)[1]

    # Determine the start day based on selected dates
    start_date = user_data[user_id]["start_date"]
    start_day = 1
    if selected_year == start_date.year and selected_month == start_date.month:
        start_day = start_date.day

    # Create day buttons
    keyboard = []
    row = []
    for day in range(start_day, num_days + 1):
        row.append(InlineKeyboardButton(str(day), callback_data=f"custom_end_day_{day}"))
        if len(row) == 7 or day == num_days:
            keyboard.append(row)
            row = []

    keyboard.append([InlineKeyboardButton("Go Back", callback_data="back_to_custom_end_month")])

    reply_markup = InlineKeyboardMarkup(keyboard)
    month_name = calendar.month_name[selected_month]
    await query.edit_message_text(
        f"Selected start date: {start_date.strftime('%Y-%m-%d')}\n"
        f"Selected end date: {month_name} {selected_year}\n"
        f"Now select the END day:",
        reply_markup=reply_markup
    )

    return SELECTING_CUSTOM_END_DAY


async def custom_end_day_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle the selection of the custom end day."""
    query = update.callback_query
    await query.answer()

    if query.data == "back_to_custom_end_month":
        # Go back to end month selection
        user_id = update.effective_user.id
        start_date = user_data[user_id]["start_date"]
        selected_year = user_data[user_id]["custom_end_year"]

        # Determine which months to show
        start_month = 1
        if selected_year == start_date.year:
            start_month = start_date.month

        keyboard = []
        row = []
        for month in range(start_month, 13):
            month_name = calendar.month_abbr[month]
            row.append(InlineKeyboardButton(month_name, callback_data=f"custom_end_month_{month}"))
            if len(row) == 4:
                keyboard.append(row)
                row = []

        if row:  # Add any remaining buttons
            keyboard.append(row)

        keyboard.append([InlineKeyboardButton("Go Back", callback_data="back_to_custom_end_year")])

        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            f"Selected start date: {start_date.strftime('%Y-%m-%d')}\n"
            f"Selected end year: {selected_year}\n"
            f"Now select the END month:",
            reply_markup=reply_markup
        )
        return SELECTING_CUSTOM_END_MONTH

    user_id = update.effective_user.id
    selected_day = int(query.data.split('_')[-1])

    # Save the complete end date
    selected_year = user_data[user_id]["custom_end_year"]
    selected_month = user_data[user_id]["custom_end_month"]
    user_data[user_id]["end_date"] = datetime(selected_year, selected_month, selected_day).date()

    return await prepare_generate_report(update, context)


async def back_to_date_range(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Go back to date range selection."""
    query = update.callback_query

    # Get the report type from user data
    user_id = update.effective_user.id
    report_type = user_data[user_id].get("report_type", "summary")

    keyboard = [
        [
            InlineKeyboardButton("Today", callback_data="range_today"),
            InlineKeyboardButton("Yesterday", callback_data="range_yesterday")
        ],
        [
            InlineKeyboardButton("This Week", callback_data="range_thisweek"),
            InlineKeyboardButton("This Month", callback_data="range_thismonth")
        ],
        [
            InlineKeyboardButton("Custom Date Range", callback_data="range_custom")
        ],
        [
            InlineKeyboardButton("Go Back", callback_data="back_to_report_type")
        ]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(
        f"Selected report type: {report_type.capitalize()}\n\n"
        f"Now please select the date range:",
        reply_markup=reply_markup
    )

    return SELECTING_DATE_RANGE


async def prepare_generate_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Prepare to generate the report based on selected options."""
    query = update.callback_query
    user_id = update.effective_user.id

    start_date = user_data[user_id]["start_date"]
    end_date = user_data[user_id]["end_date"]
    report_type = user_data[user_id]["report_type"]

    await query.edit_message_text(
        f"Generating {report_type.capitalize()} report for date range:\n"
        f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}\n"
        f"Please wait..."
    )

    # Generate the appropriate report
    if report_type == "excel":
        return await generate_excel_report(update, context)
    else:
        return await generate_summary_report(update, context)


async def generate_summary_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Generate the summary sales report for the selected date range."""
    query = update.callback_query
    user_id = update.effective_user.id

    start_date = user_data[user_id]["start_date"]
    end_date = user_data[user_id]["end_date"]

    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    # Initialize counters
    total_sales_by_source = defaultdict(int)
    total_order_counts_by_source = defaultdict(int)
    total_orders_count = 0

    try:
        # Process each day in the range
        current = start_date
        while current <= end_date:
            date_str = current.strftime("%Y-%m-%d")

            # Get sales data for this day
            by_source, order_counts, day_total = keycrm_client.get_sales_by_product_and_source_for_date(
                target_date=date_str,
                tz_name="Etc/GMT-3",  # Adjust timezone as needed
                telegram_manager_ids=['19', '22', '4', '16']
            )

            # Accumulate sales
            for src_id, products in by_source.items():
                total_sales_by_source[src_id] += sum(products.values())

            # Accumulate order counts
            for src_id, cnt in order_counts.items():
                total_order_counts_by_source[src_id] += cnt

            total_orders_count += day_total
            current += timedelta(days=1)

        # Build final timestamp in GMT+3
        now = datetime.now(pytz.timezone("Etc/GMT-3"))
        report_time = now.strftime("%Y-%m-%d %H:%M:%S")

        # Format the report
        report = f"📅 Sales Summary for {start_date_str} to {end_date_str}\n"
        report += f"• Total orders: {total_orders_count}\n\n"

        report += "📦 Total quantity by source:\n"
        for src_id, qty in total_sales_by_source.items():
            name = source_dct.get(int(src_id), src_id)
            report += f"• {name}: {qty}\n"
        report += "\n"

        report += f"🛒 Order counts on {start_date_str} to {end_date_str}:\n"
        for src_id, cnt in total_order_counts_by_source.items():
            name = source_dct.get(int(src_id), src_id)
            report += f"• {name}: {cnt}\n"
        report += "\n"

        report += f"Report generated on {report_time}"

        # Send the report
        await query.edit_message_text(report)

    except Exception as e:
        logger.error(f"Error generating report: {e}")
        await query.edit_message_text(
            f"Sorry, there was an error generating the report:\n{str(e)}\n"
            f"Please try again with a different date range."
        )

    # Clear user data for this user
    if user_id in user_data:
        del user_data[user_id]

    return ConversationHandler.END


async def generate_excel_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Generate and send an Excel sales report for the selected date range."""
    query = update.callback_query
    user_id = update.effective_user.id

    start_date = user_data[user_id]["start_date"]
    end_date = user_data[user_id]["end_date"]

    # Get bot token from environment
    bot_token = BOT_TOKEN
    chat_id = update.effective_chat.id

    try:
        # Generate and send Excel report via the KeyCRM API function
        success = keycrm_client.send_sales_summary_excel_to_telegram(
            target_date=(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')),
            bot_token=bot_token,
            chat_id=chat_id,
            tz_name="Etc/GMT-3",  # Adjust timezone as needed
            exclude_status_id=None,
            telegram_manager_ids=['19', '22', '4', '16']
        )

        if success:
            await query.edit_message_text(
                f"Excel report for {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')} "
                f"has been generated and sent successfully."
            )
        else:
            await query.edit_message_text(
                f"There was an error generating the Excel report. Please try again."
            )

    except Exception as e:
        logger.error(f"Error generating Excel report: {e}")
        await query.edit_message_text(
            f"Sorry, there was an error generating the Excel report:\n{str(e)}\n"
            f"Please try again with a different date range."
        )

    # Clear user data for this user
    if user_id in user_data:
        del user_data[user_id]

    return ConversationHandler.END


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancel and end the conversation."""
    user_id = update.effective_user.id
    if user_id in user_data:
        del user_data[user_id]

    await update.message.reply_text("Report generation cancelled.")
    return ConversationHandler.END


async def setup_command_menu(application: Application) -> None:
    """Set up the bot commands in the menu."""
    try:
        commands = [
            BotCommand("start", "Start the bot"),
            BotCommand("help", "Show help information"),
            BotCommand("report", "Generate a sales report"),
            BotCommand("cancel", "Cancel current operation")
        ]

        print("Setting bot commands...")
        await application.bot.set_my_commands(commands)
        print("Bot commands set successfully")

        # Set menu button to display commands
        try:
            print("Setting menu button...")
            await application.bot.set_chat_menu_button(
                menu_button=MenuButtonCommands(type="commands")
            )
            print("Menu button set successfully")
        except Exception as menu_error:
            print(f"Warning: Could not set menu button: {menu_error}")
            print("This is not critical - the commands will still work.")

    except Exception as e:
        print(f"Error in setup_command_menu: {e}")


def main() -> None:
    """Start the bot."""
    # Make sure we have a valid token
    if not BOT_TOKEN:
        print("Error: No BOT_TOKEN found in environment variables.")
        print("Please set the BOT_TOKEN environment variable or add it to your .env file.")
        return

    print(f"Starting KeyCRM Telegram bot...")

    # Create the Application
    application = Application.builder().token(BOT_TOKEN).build()

    # Register handlers
    print("Registering command handlers...")

    # Add conversation handler for report generation
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("report", report_command)],
        states={
            SELECTING_REPORT_TYPE: [
                CallbackQueryHandler(report_type_callback, pattern=r"^report_type_|^go_back")
            ],
            SELECTING_DATE_RANGE: [
                CallbackQueryHandler(date_range_callback, pattern=r"^range_|^back_to_report_type")
            ],
            SELECTING_CUSTOM_START_YEAR: [
                CallbackQueryHandler(custom_start_year_callback, pattern=r"^custom_start_year_|^back_to_date_range")
            ],
            SELECTING_CUSTOM_START_MONTH: [
                CallbackQueryHandler(custom_start_month_callback,
                                     pattern=r"^custom_start_month_|^back_to_custom_start_year")
            ],
            SELECTING_CUSTOM_START_DAY: [
                CallbackQueryHandler(custom_start_day_callback,
                                     pattern=r"^custom_start_day_|^back_to_custom_start_month")
            ],
            SELECTING_CUSTOM_END_YEAR: [
                CallbackQueryHandler(custom_end_year_callback, pattern=r"^custom_end_year_|^back_to_custom_start_day")
            ],
            SELECTING_CUSTOM_END_MONTH: [
                CallbackQueryHandler(custom_end_month_callback, pattern=r"^custom_end_month_|^back_to_custom_end_year")
            ],
            SELECTING_CUSTOM_END_DAY: [
                CallbackQueryHandler(custom_end_day_callback, pattern=r"^custom_end_day_|^back_to_custom_end_month")
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        per_message=False,
    )

    application.add_handler(conv_handler)

    # Add basic command handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("cancel", cancel))

    # Add a startup action to set up the command menu
    async def set_commands(context):
        try:
            await setup_command_menu(application)
            print("Command menu setup complete.")
        except Exception as e:
            print(f"Error setting up command menu: {e}")

    # Set up the command menu at startup
    application.job_queue.run_once(set_commands, 1)

    print("Bot initialized. Starting polling...")

    # Start the Bot with error handling
    try:
        application.run_polling(allowed_updates=Update.ALL_TYPES)
    except Exception as e:
        print(f"Error starting bot: {e}")


if __name__ == "__main__":
    main()