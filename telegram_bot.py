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

# Other imports
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
SELECTING_START_YEAR, SELECTING_START_MONTH, SELECTING_START_DAY = range(3)
SELECTING_END_YEAR, SELECTING_END_MONTH, SELECTING_END_DAY = range(3, 6)
GENERATING_REPORT = 6

# Predefined date range options
SELECTING_RANGE_OPTION = 7

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
        "📊 *KeyCRM Sales Report Bot* 📊\n\n"
        "*Available Commands:*\n"
        "/report - Generate a custom sales report by selecting dates\n"
        "/quick\\_report - Choose from predefined date ranges\n"
        "/today - Get today's sales report\n"
        "/yesterday - Get yesterday's sales report\n"
        "/thisweek - Get this week's sales report\n"
        "/lastweek - Get last week's sales report\n"
        "/thismonth - Get this month's sales report\n"
        "/lastmonth - Get last month's sales report\n"
        "/cancel - Cancel the current operation\n"
        "/help - Show this help message\n\n"
        "To generate a report, click on any of the commands above or use the menu button."
    )

    # Try with MarkdownV2, which requires escaping special characters
    try:
        await update.message.reply_text(help_text, parse_mode="MarkdownV2")
    except Exception as e:
        # Fallback to HTML if Markdown fails
        html_help_text = (
            "<b>📊 KeyCRM Sales Report Bot 📊</b>\n\n"
            "<b>Available Commands:</b>\n"
            "/report - Generate a custom sales report by selecting dates\n"
            "/quick_report - Choose from predefined date ranges\n"
            "/today - Get today's sales report\n"
            "/yesterday - Get yesterday's sales report\n"
            "/thisweek - Get this week's sales report\n"
            "/lastweek - Get last week's sales report\n"
            "/thismonth - Get this month's sales report\n"
            "/lastmonth - Get last month's sales report\n"
            "/cancel - Cancel the current operation\n"
            "/help - Show this help message\n\n"
            "To generate a report, click on any of the commands above or use the menu button."
        )
        try:
            await update.message.reply_text(html_help_text, parse_mode="HTML")
        except Exception:
            # Last resort: Send without formatting
            await update.message.reply_text(
                "📊 KeyCRM Sales Report Bot 📊\n\n"
                "Available Commands:\n"
                "/report - Generate a custom sales report by selecting dates\n"
                "/quick_report - Choose from predefined date ranges\n"
                "/today - Get today's sales report\n"
                "/yesterday - Get yesterday's sales report\n"
                "/thisweek - Get this week's sales report\n"
                "/lastweek - Get last week's sales report\n"
                "/thismonth - Get this month's sales report\n"
                "/lastmonth - Get last month's sales report\n"
                "/cancel - Cancel the current operation\n"
                "/help - Show this help message\n\n"
                "To generate a report, click on any of the commands above or use the menu button."
            )

async def report_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Start the report generation process by asking for the start year."""
    # Initialize the user data
    user_id = update.effective_user.id
    user_data[user_id] = {"start_date": None, "end_date": None}

    # Get current year and previous 2 years
    current_year = datetime.now().year
    keyboard = [
        [InlineKeyboardButton(str(year), callback_data=f"start_year_{year}")
         for year in range(current_year - 2, current_year + 1)]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Please select the START year:", reply_markup=reply_markup)

    return SELECTING_START_YEAR


async def quick_report_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Provide quick access to predefined date ranges."""
    keyboard = [
        [
            InlineKeyboardButton("Today", callback_data="range_today"),
            InlineKeyboardButton("Yesterday", callback_data="range_yesterday")
        ],
        [
            InlineKeyboardButton("This Week", callback_data="range_thisweek"),
            InlineKeyboardButton("Last Week", callback_data="range_lastweek")
        ],
        [
            InlineKeyboardButton("This Month", callback_data="range_thismonth"),
            InlineKeyboardButton("Last Month", callback_data="range_lastmonth")
        ],
        [
            InlineKeyboardButton("Last 7 Days", callback_data="range_last7days"),
            InlineKeyboardButton("Last 30 Days", callback_data="range_last30days")
        ],
        [
            InlineKeyboardButton("Custom Range", callback_data="range_custom")
        ]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "Choose a date range for your sales report:",
        reply_markup=reply_markup
    )

    return SELECTING_RANGE_OPTION


async def range_selection_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle the selection of a predefined date range."""
    query = update.callback_query
    await query.answer()

    user_id = update.effective_user.id
    selected_range = query.data.split('_')[1]

    today = date.today()

    # Initialize user data
    if user_id not in user_data:
        user_data[user_id] = {}

    # Set date range based on selection
    if selected_range == "today":
        start_date = today
        end_date = today
    elif selected_range == "yesterday":
        start_date = today - timedelta(days=1)
        end_date = today - timedelta(days=1)
    elif selected_range == "thisweek":
        # Monday as the first day of the week
        start_date = today - timedelta(days=today.weekday())
        end_date = today
    elif selected_range == "lastweek":
        # Last week (Monday to Sunday)
        start_date = today - timedelta(days=today.weekday() + 7)
        end_date = today - timedelta(days=today.weekday() + 1)
    elif selected_range == "thismonth":
        start_date = date(today.year, today.month, 1)
        end_date = today
    elif selected_range == "lastmonth":
        # Last day of previous month
        if today.month == 1:
            last_month = date(today.year - 1, 12, 1)
        else:
            last_month = date(today.year, today.month - 1, 1)

        start_date = last_month
        # Last day of the month
        next_month = last_month.replace(day=28) + timedelta(days=4)
        end_date = next_month - timedelta(days=next_month.day)
    elif selected_range == "last7days":
        start_date = today - timedelta(days=6)
        end_date = today
    elif selected_range == "last30days":
        start_date = today - timedelta(days=29)
        end_date = today
    elif selected_range == "custom":
        # Redirect to the custom date selection flow
        return await report_command(update, context)
    else:
        await query.edit_message_text("Invalid range selection. Please try again.")
        return ConversationHandler.END

    # Store the selected dates
    user_data[user_id]["start_date"] = start_date
    user_data[user_id]["end_date"] = end_date

    # Show the selected date range
    await query.edit_message_text(
        f"Generating report for date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}\n"
        f"Please wait..."
    )

    # Generate the report
    return await generate_report(update, context)


# Shortcut commands for specific date ranges
async def today_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Generate a report for today."""
    user_id = update.effective_user.id
    today = date.today()

    if user_id not in user_data:
        user_data[user_id] = {}

    user_data[user_id]["start_date"] = today
    user_data[user_id]["end_date"] = today

    await update.message.reply_text(
        f"Generating report for today ({today.strftime('%Y-%m-%d')})\n"
        f"Please wait..."
    )

    # Create a dummy callback query for the generate_report function
    context.user_data["message"] = await update.message.reply_text("Processing...")
    return await generate_report_direct(update, context)


async def yesterday_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Generate a report for yesterday."""
    user_id = update.effective_user.id
    yesterday = date.today() - timedelta(days=1)

    if user_id not in user_data:
        user_data[user_id] = {}

    user_data[user_id]["start_date"] = yesterday
    user_data[user_id]["end_date"] = yesterday

    await update.message.reply_text(
        f"Generating report for yesterday ({yesterday.strftime('%Y-%m-%d')})\n"
        f"Please wait..."
    )

    context.user_data["message"] = await update.message.reply_text("Processing...")
    return await generate_report_direct(update, context)


async def thisweek_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Generate a report for this week."""
    user_id = update.effective_user.id
    today = date.today()
    start_date = today - timedelta(days=today.weekday())  # Monday

    if user_id not in user_data:
        user_data[user_id] = {}

    user_data[user_id]["start_date"] = start_date
    user_data[user_id]["end_date"] = today

    await update.message.reply_text(
        f"Generating report for this week ({start_date.strftime('%Y-%m-%d')} to {today.strftime('%Y-%m-%d')})\n"
        f"Please wait..."
    )

    context.user_data["message"] = await update.message.reply_text("Processing...")
    return await generate_report_direct(update, context)


async def lastweek_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Generate a report for last week."""
    user_id = update.effective_user.id
    today = date.today()
    start_date = today - timedelta(days=today.weekday() + 7)  # Monday of last week
    end_date = today - timedelta(days=today.weekday() + 1)  # Sunday of last week

    if user_id not in user_data:
        user_data[user_id] = {}

    user_data[user_id]["start_date"] = start_date
    user_data[user_id]["end_date"] = end_date

    await update.message.reply_text(
        f"Generating report for last week ({start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')})\n"
        f"Please wait..."
    )

    context.user_data["message"] = await update.message.reply_text("Processing...")
    return await generate_report_direct(update, context)


async def thismonth_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Generate a report for this month."""
    user_id = update.effective_user.id
    today = date.today()
    start_date = date(today.year, today.month, 1)

    if user_id not in user_data:
        user_data[user_id] = {}

    user_data[user_id]["start_date"] = start_date
    user_data[user_id]["end_date"] = today

    await update.message.reply_text(
        f"Generating report for this month ({start_date.strftime('%Y-%m-%d')} to {today.strftime('%Y-%m-%d')})\n"
        f"Please wait..."
    )

    context.user_data["message"] = await update.message.reply_text("Processing...")
    return await generate_report_direct(update, context)


async def lastmonth_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Generate a report for last month."""
    user_id = update.effective_user.id
    today = date.today()

    # Calculate first day of last month
    if today.month == 1:
        # January special case - go back to December
        first_day = date(today.year - 1, 12, 1)
    else:
        first_day = date(today.year, today.month - 1, 1)

    # Calculate last day of last month
    last_day = date(today.year, today.month, 1) - timedelta(days=1)

    if user_id not in user_data:
        user_data[user_id] = {}

    user_data[user_id]["start_date"] = first_day
    user_data[user_id]["end_date"] = last_day

    await update.message.reply_text(
        f"Generating report for last month ({first_day.strftime('%Y-%m-%d')} to {last_day.strftime('%Y-%m-%d')})\n"
        f"Please wait..."
    )

    context.user_data["message"] = await update.message.reply_text("Processing...")
    return await generate_report_direct(update, context)


async def start_year_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle the selection of the start year and ask for the start month."""
    query = update.callback_query
    await query.answer()

    user_id = update.effective_user.id
    selected_year = int(query.data.split('_')[-1])
    user_data[user_id]["start_year"] = selected_year

    # Create month buttons (Jan-Dec)
    keyboard = []
    row = []
    for month in range(1, 13):
        month_name = calendar.month_abbr[month]
        row.append(InlineKeyboardButton(month_name, callback_data=f"start_month_{month}"))
        if month % 4 == 0:
            keyboard.append(row)
            row = []

    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(f"Selected start year: {selected_year}\nNow select the START month:",
                                  reply_markup=reply_markup)

    return SELECTING_START_MONTH


async def start_month_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle the selection of the start month and ask for the start day."""
    query = update.callback_query
    await query.answer()

    user_id = update.effective_user.id
    selected_month = int(query.data.split('_')[-1])
    user_data[user_id]["start_month"] = selected_month

    # Get the number of days in the selected month and year
    selected_year = user_data[user_id]["start_year"]
    num_days = calendar.monthrange(selected_year, selected_month)[1]

    # Create day buttons (1-31, depending on month)
    keyboard = []
    row = []
    for day in range(1, num_days + 1):
        row.append(InlineKeyboardButton(str(day), callback_data=f"start_day_{day}"))
        if len(row) == 7 or day == num_days:
            keyboard.append(row)
            row = []

    reply_markup = InlineKeyboardMarkup(keyboard)
    month_name = calendar.month_name[selected_month]
    await query.edit_message_text(
        f"Selected start date: {month_name} {selected_year}\nNow select the START day:",
        reply_markup=reply_markup
    )

    return SELECTING_START_DAY


async def start_day_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle the selection of the start day and ask for the end year."""
    query = update.callback_query
    await query.answer()

    user_id = update.effective_user.id
    selected_day = int(query.data.split('_')[-1])

    # Save the complete start date
    selected_year = user_data[user_id]["start_year"]
    selected_month = user_data[user_id]["start_month"]
    user_data[user_id]["start_date"] = datetime(selected_year, selected_month, selected_day).date()

    # Move to selecting end date - first end year
    current_year = datetime.now().year
    keyboard = [
        [InlineKeyboardButton(str(year), callback_data=f"end_year_{year}")
         for year in range(selected_year, current_year + 1)]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)
    start_date = user_data[user_id]["start_date"]
    await query.edit_message_text(
        f"Selected start date: {start_date.strftime('%Y-%m-%d')}\n"
        f"Now select the END year:",
        reply_markup=reply_markup
    )

    return SELECTING_END_YEAR


async def end_year_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle the selection of the end year and ask for the end month."""
    query = update.callback_query
    await query.answer()

    user_id = update.effective_user.id
    selected_year = int(query.data.split('_')[-1])
    user_data[user_id]["end_year"] = selected_year

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
        row.append(InlineKeyboardButton(month_name, callback_data=f"end_month_{month}"))
        if month % 4 == 0 or month == 12:
            keyboard.append(row)
            row = []

    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(
        f"Selected start date: {start_date.strftime('%Y-%m-%d')}\n"
        f"Selected end year: {selected_year}\n"
        f"Now select the END month:",
        reply_markup=reply_markup
    )

    return SELECTING_END_MONTH


async def end_month_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle the selection of the end month and ask for the end day."""
    query = update.callback_query
    await query.answer()

    user_id = update.effective_user.id
    selected_month = int(query.data.split('_')[-1])
    user_data[user_id]["end_month"] = selected_month

    # Get the number of days in the selected month and year
    selected_year = user_data[user_id]["end_year"]
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
        row.append(InlineKeyboardButton(str(day), callback_data=f"end_day_{day}"))
        if len(row) == 7 or day == num_days:
            keyboard.append(row)
            row = []

    reply_markup = InlineKeyboardMarkup(keyboard)
    month_name = calendar.month_name[selected_month]
    await query.edit_message_text(
        f"Selected start date: {start_date.strftime('%Y-%m-%d')}\n"
        f"Selected end date: {month_name} {selected_year}\n"
        f"Now select the END day:",
        reply_markup=reply_markup
    )

    return SELECTING_END_DAY


async def end_day_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle the selection of the end day and generate the report."""
    query = update.callback_query
    await query.answer()

    user_id = update.effective_user.id
    selected_day = int(query.data.split('_')[-1])

    # Save the complete end date
    selected_year = user_data[user_id]["end_year"]
    selected_month = user_data[user_id]["end_month"]
    user_data[user_id]["end_date"] = datetime(selected_year, selected_month, selected_day).date()

    # Now we have both start and end dates, generate the report
    start_date = user_data[user_id]["start_date"]
    end_date = user_data[user_id]["end_date"]

    await query.edit_message_text(
        f"Generating report for date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}\n"
        f"Please wait..."
    )

    # Generate and send the report
    return await generate_report(update, context)


async def generate_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Generate the sales report for the selected date range."""
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

    # Show processing message
    await query.edit_message_text(f"Collecting data for {start_date_str} to {end_date_str}...\nThis may take a moment.")

    try:
        # Process each day in the range
        current = start_date
        while current <= end_date:
            date_str = current.strftime("%Y-%m-%d")

            # Get sales data for this day
            by_source, order_counts, day_total = keycrm_client.get_sales_by_product_and_source_for_date(
                target_date=date_str,
                tz_name="Etc/GMT-3"  # Adjust timezone as needed
            )

            # Accumulate sales
            for src_id, products in by_source.items():
                total_sales_by_source[src_id] += sum(products.values())

            # Accumulate order counts
            for src_id, cnt in order_counts.items():
                total_order_counts_by_source[src_id] += cnt

            total_orders_count += day_total
            current += timedelta(days=1)

        # Build final timestamp in GMT+4
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


async def generate_report_direct(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Generate the sales report for direct commands (no callback query)."""
    user_id = update.effective_user.id

    start_date = user_data[user_id]["start_date"]
    end_date = user_data[user_id]["end_date"]

    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    # Initialize counters
    total_sales_by_source = defaultdict(int)
    total_order_counts_by_source = defaultdict(int)
    total_orders_count = 0

    # Get the message to update
    message = context.user_data.get("message")
    if message:
        await message.edit_text(f"Collecting data for {start_date_str} to {end_date_str}...\nThis may take a moment.")

    try:
        # Process each day in the range
        current = start_date
        while current <= end_date:
            date_str = current.strftime("%Y-%m-%d")

            # Get sales data for this day
            by_source, order_counts, day_total = keycrm_client.get_sales_by_product_and_source_for_date(
                target_date=date_str,
                tz_name="Etc/GMT-3"  # Adjust timezone as needed
            )

            # Accumulate sales
            for src_id, products in by_source.items():
                total_sales_by_source[src_id] += sum(products.values())

            # Accumulate order counts
            for src_id, cnt in order_counts.items():
                total_order_counts_by_source[src_id] += cnt

            total_orders_count += day_total
            current += timedelta(days=1)

        # Build final timestamp in GMT+4
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
        if message:
            await message.edit_text(report)
        else:
            await update.message.reply_text(report)

    except Exception as e:
        logger.error(f"Error generating report: {e}")
        if message:
            await message.edit_text(
                f"Sorry, there was an error generating the report:\n{str(e)}\n"
                f"Please try again with a different date range."
            )
        else:
            await update.message.reply_text(
                f"Sorry, there was an error generating the report:\n{str(e)}\n"
                f"Please try again with a different date range."
            )

    # Clear user data for this user
    if user_id in user_data:
        del user_data[user_id]

    # Clear context user data
    if "message" in context.user_data:
        del context.user_data["message"]

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
            BotCommand("report", "Generate a custom sales report"),
            BotCommand("quick_report", "Choose from predefined date ranges"),
            BotCommand("today", "Get today's sales report"),
            BotCommand("yesterday", "Get yesterday's sales report"),
            BotCommand("thisweek", "Get this week's sales report"),
            BotCommand("lastweek", "Get last week's sales report"),
            BotCommand("thismonth", "Get this month's sales report"),
            BotCommand("lastmonth", "Get last month's sales report"),
            BotCommand("cancel", "Cancel current operation")
        ]

        print("Setting bot commands...")
        await application.bot.set_my_commands(commands)
        print("Bot commands set successfully")

        # Note: The MenuButtonCommands feature requires Telegram Bot API v6.0+
        # If your version of python-telegram-bot doesn't support this yet,
        # or if you get errors, you can comment out these lines
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

    # Add conversation handler for custom report generation
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("report", report_command)],
        states={
            SELECTING_START_YEAR: [CallbackQueryHandler(start_year_callback, pattern=r"^start_year_")],
            SELECTING_START_MONTH: [CallbackQueryHandler(start_month_callback, pattern=r"^start_month_")],
            SELECTING_START_DAY: [CallbackQueryHandler(start_day_callback, pattern=r"^start_day_")],
            SELECTING_END_YEAR: [CallbackQueryHandler(end_year_callback, pattern=r"^end_year_")],
            SELECTING_END_MONTH: [CallbackQueryHandler(end_month_callback, pattern=r"^end_month_")],
            SELECTING_END_DAY: [CallbackQueryHandler(end_day_callback, pattern=r"^end_day_")],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        per_message=False,
    )

    # Add conversation handler for quick report selection
    quick_report_handler = ConversationHandler(
        entry_points=[CommandHandler("quick_report", quick_report_command)],
        states={
            SELECTING_RANGE_OPTION: [CallbackQueryHandler(range_selection_callback, pattern=r"^range_")],
            SELECTING_START_YEAR: [CallbackQueryHandler(start_year_callback, pattern=r"^start_year_")],
            SELECTING_START_MONTH: [CallbackQueryHandler(start_month_callback, pattern=r"^start_month_")],
            SELECTING_START_DAY: [CallbackQueryHandler(start_day_callback, pattern=r"^start_day_")],
            SELECTING_END_YEAR: [CallbackQueryHandler(end_year_callback, pattern=r"^end_year_")],
            SELECTING_END_MONTH: [CallbackQueryHandler(end_month_callback, pattern=r"^end_month_")],
            SELECTING_END_DAY: [CallbackQueryHandler(end_day_callback, pattern=r"^end_day_")],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        per_message=False,
    )

    application.add_handler(conv_handler)
    application.add_handler(quick_report_handler)

    # Add command handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("today", today_command))
    application.add_handler(CommandHandler("yesterday", yesterday_command))
    application.add_handler(CommandHandler("thisweek", thisweek_command))
    application.add_handler(CommandHandler("lastweek", lastweek_command))
    application.add_handler(CommandHandler("thismonth", thismonth_command))
    application.add_handler(CommandHandler("lastmonth", lastmonth_command))

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