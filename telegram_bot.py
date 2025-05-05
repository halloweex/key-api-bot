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

# UI Constants
REPORT_TYPES = {
    "summary": "📊 Summary Report",
    "excel": "📑 Excel Report"
}

DATE_RANGES = {
    "today": "📅 Today",
    "yesterday": "📅 Yesterday",
    "thisweek": "📆 This Week",
    "thismonth": "📆 This Month",
    "custom": "🗓️ Custom Date Range"
}

# UI Styling Helper Functions
def bold(text):
    """Make text bold in Telegram"""
    return f"<b>{text}</b>"

def italic(text):
    """Make text italic in Telegram"""
    return f"<i>{text}</i>"

def code(text):
    """Format text as code in Telegram"""
    return f"<code>{text}</code>"

def create_progress_indicator(current_step, total_steps):
    """Create a progress indicator for multi-step processes"""
    filled = "●" * current_step
    empty = "○" * (total_steps - current_step)
    return f"{filled}{empty}"


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Send a welcome message when the command /start is issued."""
    user = update.effective_user

    welcome_message = (
        f"👋 {bold('Welcome, ' + user.first_name)}! \n\n"
        f"I'm your {bold('KeyCRM Sales Report')} assistant. I can help you generate detailed sales reports "
        f"from your KeyCRM data.\n\n"
        f"🚀 {italic('What would you like to do?')}"
    )

    # Create an attractive keyboard with main commands
    keyboard = [
        [InlineKeyboardButton("📊 Generate Report", callback_data="cmd_report")],
        [InlineKeyboardButton("ℹ️ Help", callback_data="cmd_help")]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)

    try:
        await update.message.reply_text(welcome_message, reply_markup=reply_markup, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Error sending welcome message: {e}")
        # Fallback without formatting
        await update.message.reply_text(
            f"Welcome, {user.first_name}! Use /report to generate a sales report or /help for assistance.")

    return ConversationHandler.END


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a helpful message when the command /help is issued."""
    help_text = (
        f"{bold('📊 KeyCRM Sales Report Bot 📊')}\n\n"
        f"{bold('Available Commands:')}\n"
        f"• /report - Generate a sales report\n"
        f"• /cancel - Cancel the current operation\n"
        f"• /help - Show this help message\n\n"
        f"{bold('How to use:')}\n"
        f"1️⃣ Start with /report command\n"
        f"2️⃣ Select report type (Summary or Excel)\n"
        f"3️⃣ Choose date range\n"
        f"4️⃣ View your report results\n\n"
        f"{italic('Need more assistance? Contact support at support@example.com')}"
    )

    # Add buttons for quick actions
    keyboard = [
        [InlineKeyboardButton("📊 Generate Report", callback_data="cmd_report")],
        [InlineKeyboardButton("🔙 Back to Main Menu", callback_data="cmd_start")]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)

    try:
        await update.message.reply_text(help_text, reply_markup=reply_markup, parse_mode="HTML")
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


async def command_button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle command buttons from the main menu."""
    query = update.callback_query
    await query.answer()

    command = query.data.split('_')[1]

    if command == "report":
        return await report_command_from_callback(update, context)
    elif command == "help":
        help_text = (
            f"{bold('📊 KeyCRM Sales Report Bot 📊')}\n\n"
            f"{bold('Available Commands:')}\n"
            f"• /report - Generate a sales report\n"
            f"• /cancel - Cancel the current operation\n"
            f"• /help - Show this help message\n\n"
            f"{bold('How to use:')}\n"
            f"1️⃣ Start with /report command\n"
            f"2️⃣ Select report type (Summary or Excel)\n"
            f"3️⃣ Choose date range\n"
            f"4️⃣ View your report results\n\n"
            f"{italic('Need more assistance? Contact support at support@example.com')}"
        )

        keyboard = [
            [InlineKeyboardButton("📊 Generate Report", callback_data="cmd_report")],
            [InlineKeyboardButton("🔙 Back to Main Menu", callback_data="cmd_start")]
        ]

        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(help_text, reply_markup=reply_markup, parse_mode="HTML")
        return ConversationHandler.END
    elif command == "start":
        user = update.effective_user

        welcome_message = (
            f"👋 {bold('Welcome, ' + user.first_name)}! \n\n"
            f"I'm your {bold('KeyCRM Sales Report')} assistant. I can help you generate detailed sales reports "
            f"from your KeyCRM data.\n\n"
            f"🚀 {italic('What would you like to do?')}"
        )

        keyboard = [
            [InlineKeyboardButton("📊 Generate Report", callback_data="cmd_report")],
            [InlineKeyboardButton("ℹ️ Help", callback_data="cmd_help")]
        ]

        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(welcome_message, reply_markup=reply_markup, parse_mode="HTML")
        return ConversationHandler.END

    return ConversationHandler.END


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancel and end the conversation."""
    user_id = update.effective_user.id
    if user_id in user_data:
        del user_data[user_id]

    # Create a more friendly cancellation message with options
    cancel_message = (
        f"{bold('🛑 Operation Cancelled')}\n\n"
        f"I've cancelled the current operation as requested.\n"
        f"What would you like to do next?"
    )

    # Add buttons for next actions
    keyboard = [
        [
            InlineKeyboardButton("📊 New Report", callback_data="cmd_report"),
            InlineKeyboardButton("ℹ️ Help", callback_data="cmd_help")
        ],
        [InlineKeyboardButton("🏠 Main Menu", callback_data="cmd_start")]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(cancel_message, reply_markup=reply_markup, parse_mode="HTML")

    return ConversationHandler.END


async def report_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Start the report generation process by asking for the report type."""
    progress = create_progress_indicator(1, 3)

    message = (
        f"{bold('📊 Sales Report Generator')}\n\n"
        f"{progress} {italic('Step 1 of 3: Select Report Type')}\n\n"
        f"Please choose the type of report you'd like to generate:"
    )

    # Create an attractive keyboard for report types
    keyboard = [
        [
            InlineKeyboardButton(REPORT_TYPES["summary"], callback_data="report_type_summary"),
            InlineKeyboardButton(REPORT_TYPES["excel"], callback_data="report_type_excel")
        ],
        [
            InlineKeyboardButton("🔙 Cancel", callback_data="go_back")
        ]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(message, reply_markup=reply_markup, parse_mode="HTML")

    return SELECTING_REPORT_TYPE


async def report_command_from_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Start the report generation process from a callback query."""
    query = update.callback_query

    progress = create_progress_indicator(1, 3)

    message = (
        f"{bold('📊 Sales Report Generator')}\n\n"
        f"{progress} {italic('Step 1 of 3: Select Report Type')}\n\n"
        f"Please choose the type of report you'd like to generate:"
    )

    # Create an attractive keyboard for report types
    keyboard = [
        [
            InlineKeyboardButton(REPORT_TYPES["summary"], callback_data="report_type_summary"),
            InlineKeyboardButton(REPORT_TYPES["excel"], callback_data="report_type_excel")
        ],
        [
            InlineKeyboardButton("🔙 Cancel", callback_data="go_back")
        ]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(message, reply_markup=reply_markup, parse_mode="HTML")

    return SELECTING_REPORT_TYPE


async def report_type_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle the report type selection."""
    query = update.callback_query
    await query.answer()

    if query.data == "go_back":
        await query.edit_message_text(
            f"Operation canceled. Use /report to start again or select an option below:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📊 Try Again", callback_data="cmd_report")],
                [InlineKeyboardButton("🏠 Main Menu", callback_data="cmd_start")]
            ])
        )
        return ConversationHandler.END

    user_id = update.effective_user.id
    selected_type = query.data.split('_')[-1]

    # Initialize the user data
    if user_id not in user_data:
        user_data[user_id] = {"report_type": selected_type}
    else:
        user_data[user_id]["report_type"] = selected_type

    # Create progress indicator
    progress = create_progress_indicator(2, 3)

    # Now ask for date range with an attractive UI
    keyboard = [
        [
            InlineKeyboardButton(DATE_RANGES["today"], callback_data="range_today"),
            InlineKeyboardButton(DATE_RANGES["yesterday"], callback_data="range_yesterday")
        ],
        [
            InlineKeyboardButton(DATE_RANGES["thisweek"], callback_data="range_thisweek"),
            InlineKeyboardButton(DATE_RANGES["thismonth"], callback_data="range_thismonth")
        ],
        [
            InlineKeyboardButton(DATE_RANGES["custom"], callback_data="range_custom")
        ],
        [
            InlineKeyboardButton("🔙 Back", callback_data="back_to_report_type")
        ]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)

    message = (
        f"{bold('📊 Sales Report Generator')}\n\n"
        f"{progress} {italic('Step 2 of 3: Select Date Range')}\n\n"
        f"Selected report type: {bold(REPORT_TYPES[selected_type])}\n\n"
        f"Now, please select the date range for your report:"
    )

    await query.edit_message_text(message, reply_markup=reply_markup, parse_mode="HTML")

    return SELECTING_DATE_RANGE


async def date_range_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle the date range selection."""
    query = update.callback_query
    await query.answer()

    if query.data == "back_to_report_type":
        # Go back to report type selection with updated UI
        progress = create_progress_indicator(1, 3)

        message = (
            f"{bold('📊 Sales Report Generator')}\n\n"
            f"{progress} {italic('Step 1 of 3: Select Report Type')}\n\n"
            f"Please choose the type of report you'd like to generate:"
        )

        keyboard = [
            [
                InlineKeyboardButton(REPORT_TYPES["summary"], callback_data="report_type_summary"),
                InlineKeyboardButton(REPORT_TYPES["excel"], callback_data="report_type_excel")
            ],
            [
                InlineKeyboardButton("🔙 Cancel", callback_data="go_back")
            ]
        ]

        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(message, reply_markup=reply_markup, parse_mode="HTML")
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
        # Start custom date selection flow with better UI
        # Get current year and previous 2 years
        current_year = datetime.now().year

        message = (
            f"{bold('📊 Custom Date Selection')}\n\n"
            f"{italic('Step 1 of 6: Select START year')}\n\n"
            f"Please select the {bold('START year')} for your custom date range:"
        )

        keyboard = []
        years = list(range(current_year - 2, current_year + 1))
        for year in years:
            keyboard.append([InlineKeyboardButton(str(year), callback_data=f"custom_start_year_{year}")])

        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="back_to_date_range")])

        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(message, reply_markup=reply_markup, parse_mode="HTML")
        return SELECTING_CUSTOM_START_YEAR

    # Fallback
    await query.edit_message_text(
        f"⚠️ {bold('Invalid selection')}\n\nPlease use /report to start again.",
        parse_mode="HTML"
    )
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

    # Create month buttons (Jan-Dec) with better UI
    keyboard = []
    months = []
    for month in range(1, 13):
        month_name = calendar.month_abbr[month]
        months.append(InlineKeyboardButton(month_name, callback_data=f"custom_start_month_{month}"))
        if len(months) == 3:  # 3 months per row for better layout
            keyboard.append(months)
            months = []

    if months:  # Add any remaining months
        keyboard.append(months)

    keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="back_to_custom_start_year")])

    reply_markup = InlineKeyboardMarkup(keyboard)

    message = (
        f"{bold('📊 Custom Date Selection')}\n\n"
        f"{italic('Step 2 of 6: Select START month')}\n\n"
        f"Selected start year: {bold(str(selected_year))}\n"
        f"Now select the {bold('START month')}:"
    )

    await query.edit_message_text(message, reply_markup=reply_markup, parse_mode="HTML")

    return SELECTING_CUSTOM_START_MONTH


async def custom_start_month_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle the selection of the custom start month."""
    query = update.callback_query
    await query.answer()

    if query.data == "back_to_custom_start_year":
        # Go back to year selection with improved UI
        current_year = datetime.now().year

        message = (
            f"{bold('📊 Custom Date Selection')}\n\n"
            f"{italic('Step 1 of 6: Select START year')}\n\n"
            f"Please select the {bold('START year')} for your custom date range:"
        )

        keyboard = []
        years = list(range(current_year - 2, current_year + 1))
        for year in years:
            keyboard.append([InlineKeyboardButton(str(year), callback_data=f"custom_start_year_{year}")])

        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="back_to_date_range")])

        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(message, reply_markup=reply_markup, parse_mode="HTML")
        return SELECTING_CUSTOM_START_YEAR

    user_id = update.effective_user.id
    selected_month = int(query.data.split('_')[-1])
    user_data[user_id]["custom_start_month"] = selected_month

    # Get the number of days in the selected month and year
    selected_year = user_data[user_id]["custom_start_year"]
    num_days = calendar.monthrange(selected_year, selected_month)[1]

    # Create day buttons with improved UI (maximum 7 days per row)
    keyboard = []
    days_row = []
    for day in range(1, num_days + 1):
        days_row.append(InlineKeyboardButton(str(day), callback_data=f"custom_start_day_{day}"))
        if len(days_row) == 7 or day == num_days:
            keyboard.append(days_row)
            days_row = []

    keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="back_to_custom_start_month")])

    reply_markup = InlineKeyboardMarkup(keyboard)
    month_name = calendar.month_name[selected_month]

    message = (
        f"{bold('📊 Custom Date Selection')}\n\n"
        f"{italic('Step 3 of 6: Select START day')}\n\n"
        f"Selected start: {bold(f'{month_name} {selected_year}')}\n"
        f"Now select the {bold('START day')}:"
    )

    await query.edit_message_text(message, reply_markup=reply_markup, parse_mode="HTML")

    return SELECTING_CUSTOM_START_DAY


async def custom_start_day_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle the selection of the custom start day."""
    query = update.callback_query
    await query.answer()

    if query.data == "back_to_custom_start_month":
        # Go back to month selection with better UI
        user_id = update.effective_user.id
        selected_year = user_data[user_id]["custom_start_year"]

        keyboard = []
        months = []
        for month in range(1, 13):
            month_name = calendar.month_abbr[month]
            months.append(InlineKeyboardButton(month_name, callback_data=f"custom_start_month_{month}"))
            if len(months) == 3:  # 3 months per row
                keyboard.append(months)
                months = []

        if months:  # Add any remaining months
            keyboard.append(months)

        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="back_to_custom_start_year")])

        reply_markup = InlineKeyboardMarkup(keyboard)

        message = (
            f"{bold('📊 Custom Date Selection')}\n\n"
            f"{italic('Step 2 of 6: Select START month')}\n\n"
            f"Selected start year: {bold(str(selected_year))}\n"
            f"Now select the {bold('START month')}:"
        )

        await query.edit_message_text(message, reply_markup=reply_markup, parse_mode="HTML")
        return SELECTING_CUSTOM_START_MONTH

    user_id = update.effective_user.id
    selected_day = int(query.data.split('_')[-1])

    # Save the complete start date
    selected_year = user_data[user_id]["custom_start_year"]
    selected_month = user_data[user_id]["custom_start_month"]
    user_data[user_id]["start_date"] = datetime(selected_year, selected_month, selected_day).date()

    # Check if we're selecting dates in the current year - if so, skip the year selection for end date
    current_year = datetime.now().year
    if selected_year == current_year:
        # Skip to month selection directly
        user_data[user_id]["custom_end_year"] = current_year

        # Determine which months to show based on the selected start date
        start_date = user_data[user_id]["start_date"]

        # Create month buttons with better UI
        keyboard = []
        months = []
        for month in range(start_date.month, 13):
            month_name = calendar.month_abbr[month]
            months.append(InlineKeyboardButton(month_name, callback_data=f"custom_end_month_{month}"))
            if len(months) == 3:  # 3 months per row
                keyboard.append(months)
                months = []

        if months:  # Add any remaining months
            keyboard.append(months)

        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="back_to_custom_start_day")])

        reply_markup = InlineKeyboardMarkup(keyboard)

        message = (
            f"{bold('📊 Custom Date Selection')}\n\n"
            f"{italic('Step 4 of 5: Select END month')}\n\n"
            f"Selected start date: {bold(start_date.strftime('%Y-%m-%d'))}\n"
            f"Selected end year: {bold(str(current_year))} (Current year)\n\n"
            f"Now select the {bold('END month')}:"
        )

        await query.edit_message_text(message, reply_markup=reply_markup, parse_mode="HTML")
        return SELECTING_CUSTOM_END_MONTH
    else:
        # Now move to selecting end year with improved UI
        current_year = datetime.now().year

        keyboard = []
        years = list(range(selected_year, current_year + 1))
        for year in years:
            keyboard.append([InlineKeyboardButton(str(year), callback_data=f"custom_end_year_{year}")])

        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="back_to_custom_start_day")])

        reply_markup = InlineKeyboardMarkup(keyboard)
        start_date = user_data[user_id]["start_date"]

        message = (
            f"{bold('📊 Custom Date Selection')}\n\n"
            f"{italic('Step 4 of 6: Select END year')}\n\n"
            f"Selected start date: {bold(start_date.strftime('%Y-%m-%d'))}\n\n"
            f"Now select the {bold('END year')}:"
        )

        await query.edit_message_text(message, reply_markup=reply_markup, parse_mode="HTML")
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

        # Create day buttons with improved UI (maximum 7 days per row)
        keyboard = []
        days_row = []
        for day in range(1, num_days + 1):
            days_row.append(InlineKeyboardButton(str(day), callback_data=f"custom_start_day_{day}"))
            if len(days_row) == 7 or day == num_days:
                keyboard.append(days_row)
                days_row = []

        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="back_to_custom_start_month")])

        reply_markup = InlineKeyboardMarkup(keyboard)
        month_name = calendar.month_name[selected_month]

        message = (
            f"{bold('📊 Custom Date Selection')}\n\n"
            f"{italic('Step 3 of 6: Select START day')}\n\n"
            f"Selected start: {bold(f'{month_name} {selected_year}')}\n"
            f"Now select the {bold('START day')}:"
        )

        await query.edit_message_text(message, reply_markup=reply_markup, parse_mode="HTML")
        return SELECTING_CUSTOM_START_DAY

    user_id = update.effective_user.id
    selected_year = int(query.data.split('_')[-1])
    user_data[user_id]["custom_end_year"] = selected_year

    start_date = user_data[user_id]["start_date"]

    # Determine which months to show based on the selected year
    start_month = 1
    if selected_year == start_date.year:
        start_month = start_date.month

    # Create month buttons with better UI
    keyboard = []
    months = []
    for month in range(start_month, 13):
        month_name = calendar.month_abbr[month]
        months.append(InlineKeyboardButton(month_name, callback_data=f"custom_end_month_{month}"))
        if len(months) == 3:  # 3 months per row
            keyboard.append(months)
            months = []

    if months:  # Add any remaining months
        keyboard.append(months)

    keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="back_to_custom_end_year")])

    reply_markup = InlineKeyboardMarkup(keyboard)

    message = (
        f"{bold('📊 Custom Date Selection')}\n\n"
        f"{italic('Step 5 of 6: Select END month')}\n\n"
        f"Selected start date: {bold(start_date.strftime('%Y-%m-%d'))}\n"
        f"Selected end year: {bold(str(selected_year))}\n\n"
        f"Now select the {bold('END month')}:"
    )

    await query.edit_message_text(message, reply_markup=reply_markup, parse_mode="HTML")

    return SELECTING_CUSTOM_END_MONTH


async def custom_end_month_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle the selection of the custom end month."""
    query = update.callback_query
    await query.answer()

    if query.data == "back_to_custom_end_year":
        # Go back to end year selection with improved UI
        user_id = update.effective_user.id
        start_date = user_data[user_id]["start_date"]
        current_year = datetime.now().year

        keyboard = []
        years = list(range(start_date.year, current_year + 1))
        for year in years:
            keyboard.append([InlineKeyboardButton(str(year), callback_data=f"custom_end_year_{year}")])

        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="back_to_custom_start_day")])

        reply_markup = InlineKeyboardMarkup(keyboard)

        message = (
            f"{bold('📊 Custom Date Selection')}\n\n"
            f"{italic('Step 4 of 6: Select END year')}\n\n"
            f"Selected start date: {bold(start_date.strftime('%Y-%m-%d'))}\n\n"
            f"Now select the {bold('END year')}:"
        )

        await query.edit_message_text(message, reply_markup=reply_markup, parse_mode="HTML")
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

    # Create day buttons with improved UI
    keyboard = []
    days_row = []
    for day in range(start_day, num_days + 1):
        days_row.append(InlineKeyboardButton(str(day), callback_data=f"custom_end_day_{day}"))
        if len(days_row) == 7 or day == num_days:
            keyboard.append(days_row)
            days_row = []

    keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="back_to_custom_end_month")])

    reply_markup = InlineKeyboardMarkup(keyboard)
    month_name = calendar.month_name[selected_month]

    message = (
        f"{bold('📊 Custom Date Selection')}\n\n"
        f"{italic('Step 6 of 6: Select END day')}\n\n"
        f"Selected start date: {bold(start_date.strftime('%Y-%m-%d'))}\n"
        f"Selected end date so far: {bold(f'{month_name} {selected_year}')}\n\n"
        f"Now select the {bold('END day')}:"
    )

    await query.edit_message_text(message, reply_markup=reply_markup, parse_mode="HTML")

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
        months = []
        for month in range(start_month, 13):
            month_name = calendar.month_abbr[month]
            months.append(InlineKeyboardButton(month_name, callback_data=f"custom_end_month_{month}"))
            if len(months) == 3:  # 3 months per row
                keyboard.append(months)
                months = []

        if months:  # Add any remaining months
            keyboard.append(months)

        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="back_to_custom_end_year")])

        reply_markup = InlineKeyboardMarkup(keyboard)

        message = (
            f"{bold('📊 Custom Date Selection')}\n\n"
            f"{italic('Step 5 of 6: Select END month')}\n\n"
            f"Selected start date: {bold(start_date.strftime('%Y-%m-%d'))}\n"
            f"Selected end year: {bold(str(selected_year))}\n\n"
            f"Now select the {bold('END month')}:"
        )

        await query.edit_message_text(message, reply_markup=reply_markup, parse_mode="HTML")
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

    # Create progress indicator
    progress = create_progress_indicator(2, 3)

    keyboard = [
        [
            InlineKeyboardButton(DATE_RANGES["today"], callback_data="range_today"),
            InlineKeyboardButton(DATE_RANGES["yesterday"], callback_data="range_yesterday")
        ],
        [
            InlineKeyboardButton(DATE_RANGES["thisweek"], callback_data="range_thisweek"),
            InlineKeyboardButton(DATE_RANGES["thismonth"], callback_data="range_thismonth")
        ],
        [
            InlineKeyboardButton(DATE_RANGES["custom"], callback_data="range_custom")
        ],
        [
            InlineKeyboardButton("🔙 Back", callback_data="back_to_report_type")
        ]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)

    message = (
        f"{bold('📊 Sales Report Generator')}\n\n"
        f"{progress} {italic('Step 2 of 3: Select Date Range')}\n\n"
        f"Selected report type: {bold(REPORT_TYPES[report_type])}\n\n"
        f"Now, please select the date range for your report:"
    )

    await query.edit_message_text(message, reply_markup=reply_markup, parse_mode="HTML")

    return SELECTING_DATE_RANGE


async def prepare_generate_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Prepare to generate the report based on selected options."""
    query = update.callback_query
    user_id = update.effective_user.id

    start_date = user_data[user_id]["start_date"]
    end_date = user_data[user_id]["end_date"]
    report_type = user_data[user_id]["report_type"]

    # Create progress indicator
    progress = create_progress_indicator(3, 3)

    # Show attractive loading message with animation
    loading_message = (
        f"{bold('📊 Sales Report Generator')}\n\n"
        f"{progress} {italic('Step 3 of 3: Generating Report')}\n\n"
        f"Report type: {bold(REPORT_TYPES[report_type])}\n"
        f"Date range: {bold(start_date.strftime('%Y-%m-%d'))} to {bold(end_date.strftime('%Y-%m-%d'))}\n\n"
        f"⏳ {italic('Please wait while I generate your report...')}"
    )

    await query.edit_message_text(loading_message, parse_mode="HTML")

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

        # Format the report with improved styling
        report = (
            f"{bold('📊 SALES SUMMARY REPORT')}\n\n"
            f"📅 {bold('Date Range')}: {start_date_str} to {end_date_str}\n"
            f"📈 {bold('Total Orders')}: {total_orders_count}\n\n"
            f"{bold('📦 TOTAL Products by Source')}\n"
        )

        # Add total quantity section with better formatting
        for src_id, qty in sorted(total_sales_by_source.items(), key=lambda x: x[1], reverse=True):
            name = source_dct.get(int(src_id), src_id)
            report += f"• {bold(name)}: {qty}\n"
        report += "\n"

        # Add order counts section with better formatting
        report += f"{bold('🛒 ORDER Numbers by Source')}\n"
        for src_id, cnt in sorted(total_order_counts_by_source.items(), key=lambda x: x[1], reverse=True):
            name = source_dct.get(int(src_id), src_id)
            report += f"• {bold(name)}: {cnt}\n"
        report += "\n"

        # Add footer with timestamp and action buttons
        report += f"📝 {italic(f'Report generated on {report_time}')}"

        # Create buttons for additional actions
        keyboard = [
            [
                InlineKeyboardButton("📊 New Report", callback_data="cmd_report"),
                InlineKeyboardButton("📑 Excel Version", callback_data="convert_to_excel")
            ],
            [InlineKeyboardButton("🏠 Main Menu", callback_data="cmd_start")]
        ]

        reply_markup = InlineKeyboardMarkup(keyboard)

        # Send the report with buttons
        await query.edit_message_text(report, reply_markup=reply_markup, parse_mode="HTML")

    except Exception as e:
        logger.error(f"Error generating report: {e}")

        # Create a more user-friendly error message
        error_message = (
            f"{bold('⚠️ Error Generating Report')}\n\n"
            f"I encountered a problem while generating your report:\n"
            f"{italic(str(e))}\n\n"
            f"Please try again with a different date range or contact support if the issue persists."
        )

        # Add retry buttons
        keyboard = [
            [InlineKeyboardButton("🔄 Try Again", callback_data="cmd_report")],
            [InlineKeyboardButton("🏠 Main Menu", callback_data="cmd_start")]
        ]

        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(error_message, reply_markup=reply_markup, parse_mode="HTML")

        # Only clear user data in case of error
        if user_id in user_data:
            del user_data[user_id]

    # DO NOT clear user data after successful report generation
    # This is the key change to preserve date range for format conversion

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
        # Update message to show file is being prepared
        preparing_message = (
            f"{bold('📑 Preparing Excel Report')}\n\n"
            f"📅 {bold('Date Range')}: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}\n\n"
            f"⏳ {italic('Creating your Excel file...')}\n"
            f"This may take a moment depending on the amount of data."
        )

        await query.edit_message_text(preparing_message, parse_mode="HTML")

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
            # Show success message with confirmation and options
            success_message = (
                f"{bold('✅ Excel Report Generated!')}\n\n"
                f"📅 {bold('Date Range')}: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}\n\n"
                f"📎 Your Excel file has been sent as a separate message.\n"
                f"📊 {italic('What would you like to do next?')}"
            )

            # Create buttons for additional actions
            keyboard = [
                [
                    InlineKeyboardButton("📊 New Report", callback_data="cmd_report"),
                    InlineKeyboardButton("📑 Summary View", callback_data="convert_to_summary")
                ],
                [InlineKeyboardButton("🏠 Main Menu", callback_data="cmd_start")]
            ]

            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(success_message, reply_markup=reply_markup, parse_mode="HTML")
        else:
            # Show error message with retry options
            error_message = (
                f"{bold('⚠️ Excel Report Error')}\n\n"
                f"I was unable to generate your Excel report.\n\n"
                f"Would you like to try again or generate a summary report instead?"
            )

            keyboard = [
                [
                    InlineKeyboardButton("🔄 Try Again", callback_data="cmd_report"),
                    InlineKeyboardButton("📊 Summary Report", callback_data="convert_to_summary")
                ],
                [InlineKeyboardButton("🏠 Main Menu", callback_data="cmd_start")]
            ]

            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(error_message, reply_markup=reply_markup, parse_mode="HTML")

    except Exception as e:
        logger.error(f"Error generating Excel report: {e}")

        # Create a more user-friendly error message
        error_message = (
            f"{bold('⚠️ Excel Report Error')}\n\n"
            f"I encountered an issue while generating your Excel report:\n"
            f"{italic(str(e))}\n\n"
            f"Would you like to try again or generate a summary report instead?"
        )

        # Add retry buttons
        keyboard = [
            [
                InlineKeyboardButton("🔄 Try Again", callback_data="cmd_report"),
                InlineKeyboardButton("📊 Summary Report", callback_data="convert_to_summary")
            ],
            [InlineKeyboardButton("🏠 Main Menu", callback_data="cmd_start")]
        ]

        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(error_message, reply_markup=reply_markup, parse_mode="HTML")

        # Only clear user data in case of error
        if user_id in user_data:
            del user_data[user_id]

    # DO NOT clear user data after successful report generation
    # This keeps the date range information for format conversion

    return ConversationHandler.END


async def convert_report_format(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Convert between report formats (Summary/Excel) without starting the whole process again."""
    query = update.callback_query
    await query.answer()

    conversion_type = query.data.split('_')[-1]
    user_id = update.effective_user.id

    # Check if we have previously selected dates for this user
    if user_id in user_data and "start_date" in user_data[user_id] and "end_date" in user_data[user_id]:
        # Use the existing date range
        user_data[user_id]["report_type"] = conversion_type

        # Show loading message for the conversion
        loading_message = (
            f"{bold('🔄 Converting Report Format')}\n\n"
            f"Converting to {bold(conversion_type.capitalize())} format using the same date range:\n"
            f"📅 {bold(user_data[user_id]['start_date'].strftime('%Y-%m-%d'))} to "
            f"{bold(user_data[user_id]['end_date'].strftime('%Y-%m-%d'))}\n\n"
            f"⏳ {italic('Please wait...')}"
        )

        await query.edit_message_text(loading_message, parse_mode="HTML")

        # Generate the new report format with the same date range
        return await prepare_generate_report(update, context)
    else:
        # If we don't have a previous date range, ask for a new one
        conversion_message = (
            f"{bold('🔄 Converting Report Format')}\n\n"
            f"Please select a date range for your {bold(conversion_type.capitalize())} report:"
        )

        # Create an attractive keyboard for date selection
        keyboard = [
            [
                InlineKeyboardButton(DATE_RANGES["today"], callback_data=f"quick_{conversion_type}_today"),
                InlineKeyboardButton(DATE_RANGES["yesterday"], callback_data=f"quick_{conversion_type}_yesterday")
            ],
            [
                InlineKeyboardButton(DATE_RANGES["thisweek"], callback_data=f"quick_{conversion_type}_thisweek"),
                InlineKeyboardButton(DATE_RANGES["thismonth"], callback_data=f"quick_{conversion_type}_thismonth")
            ],
            [
                InlineKeyboardButton(DATE_RANGES["custom"], callback_data="range_custom")
            ],
            [
                InlineKeyboardButton("🔙 Cancel", callback_data="cmd_start")
            ]
        ]

        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(conversion_message, reply_markup=reply_markup, parse_mode="HTML")

        # Create a new user data entry with the report type
        if user_id not in user_data:
            user_data[user_id] = {}
        user_data[user_id]["report_type"] = conversion_type

        return SELECTING_DATE_RANGE

async def quick_report_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle quick report generation from format conversion."""
    query = update.callback_query
    await query.answer()

    # Parse the callback data
    parts = query.data.split('_')
    report_type = parts[1]  # summary or excel
    date_range = parts[2]  # today, yesterday, thisweek, thismonth

    user_id = update.effective_user.id

    # Initialize the user data
    if user_id not in user_data:
        user_data[user_id] = {}

    # Set report type
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
        # Monday as the first day of the week
        start_date = today - timedelta(days=today.weekday())
        user_data[user_id]["start_date"] = start_date
        user_data[user_id]["end_date"] = today
    elif date_range == "thismonth":
        start_date = date(today.year, today.month, 1)
        user_data[user_id]["start_date"] = start_date
        user_data[user_id]["end_date"] = today

    # Generate the report
    return await prepare_generate_report(update, context)


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

        # Set menu button to display commands - fix for the type parameter error
        try:
            print("Setting menu button...")
            # Using the correct parameter format without 'type'
            await application.bot.set_chat_menu_button(menu_button=MenuButtonCommands())
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

    # Add conversation handler for report generation with improved flow
    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("report", report_command),
            CallbackQueryHandler(report_command_from_callback, pattern=r"^cmd_report$"),
            CallbackQueryHandler(command_button_handler, pattern=r"^cmd_"),
            CallbackQueryHandler(convert_report_format, pattern=r"^convert_to_"),
            CallbackQueryHandler(quick_report_callback, pattern=r"^quick_")
        ],
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

    # Add a general callback query handler for any unhandled callbacks
    application.add_handler(CallbackQueryHandler(command_button_handler, pattern=r"^cmd_"))

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
