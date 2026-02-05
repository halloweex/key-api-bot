"""
Bot handlers package.

Provides modular handler organization:
- base: Authorization, session management, utilities
- common: Start, help, cancel, dashboard
- reports: Report generation flow (in handlers_legacy for now)
- auth: User authorization flow (in handlers_legacy for now)
- admin: Admin user management (in handlers_legacy for now)
- search: Product/order search (in handlers_legacy for now)
- settings: Bot settings (in handlers_legacy for now)

For backward compatibility, this module re-exports all handlers
from the original handlers_legacy.py module.
"""

# Re-export everything from the legacy module for backward compatibility
# This allows gradual migration to the new modular structure
from bot.handlers_legacy import (
    # Authorization
    authorized,
    ACCESS_DENIED_MESSAGE,
    ACCESS_FROZEN_MESSAGE,
    ACCESS_PENDING_MESSAGE,
    REQUEST_ACCESS_MESSAGE,
    # Session management
    get_user_session,
    create_user_session,
    update_user_session,
    cleanup_expired_sessions,
    SESSION_TIMEOUT_MINUTES,
    # Date utilities
    calculate_date_range,
    # Report service
    report_service,
    # User data
    user_data,
    # Common handlers
    start_command,
    help_command,
    cancel_command,
    command_button_handler,
    # Report handlers
    report_command,
    report_command_from_callback,
    report_type_callback,
    prepare_generate_report,
    date_range_callback,
    back_to_date_range,
    custom_start_year_callback,
    custom_start_month_callback,
    custom_start_day_callback,
    custom_end_year_callback,
    custom_end_month_callback,
    custom_end_day_callback,
    # Top10 handlers
    top10_source_callback,
    change_top10_source,
    quick_top10_callback,
    generate_top10_report,
    # Report generation
    generate_summary_report,
    generate_excel_report,
    convert_report_format,
    quick_report_callback,
    # Reply keyboard handlers
    reply_keyboard_report,
    reply_keyboard_help,
    reply_keyboard_dashboard,
    reply_keyboard_search,
    reply_keyboard_settings,
    # Dashboard
    dashboard_command,
    # Auth handlers
    auth_request_access,
    notify_admins_new_request,
    auth_approve_user,
    auth_deny_user,
    auth_request_again,
    # Admin handlers
    admin_users_command,
    admin_revoke_user,
    show_updated_user_list,
    admin_unfreeze_user,
    admin_close,
    # Search handlers
    search_command,
    search_command_from_callback,
    search_type_callback,
    search_query_handler,
    # Settings handlers
    settings_command,
    settings_command_from_callback,
    settings_callback,
    # Milestone handlers
    check_and_broadcast_milestones,
)

__all__ = [
    # Authorization
    "authorized",
    "ACCESS_DENIED_MESSAGE",
    "ACCESS_FROZEN_MESSAGE",
    "ACCESS_PENDING_MESSAGE",
    "REQUEST_ACCESS_MESSAGE",
    # Session management
    "get_user_session",
    "create_user_session",
    "update_user_session",
    "cleanup_expired_sessions",
    "SESSION_TIMEOUT_MINUTES",
    # Date utilities
    "calculate_date_range",
    # Report service
    "report_service",
    # User data
    "user_data",
    # Common handlers
    "start_command",
    "help_command",
    "cancel_command",
    "command_button_handler",
    # Report handlers
    "report_command",
    "report_command_from_callback",
    "report_type_callback",
    "prepare_generate_report",
    "date_range_callback",
    "back_to_date_range",
    "custom_start_year_callback",
    "custom_start_month_callback",
    "custom_start_day_callback",
    "custom_end_year_callback",
    "custom_end_month_callback",
    "custom_end_day_callback",
    # Top10 handlers
    "top10_source_callback",
    "change_top10_source",
    "quick_top10_callback",
    "generate_top10_report",
    # Report generation
    "generate_summary_report",
    "generate_excel_report",
    "convert_report_format",
    "quick_report_callback",
    # Reply keyboard handlers
    "reply_keyboard_report",
    "reply_keyboard_help",
    "reply_keyboard_dashboard",
    "reply_keyboard_search",
    "reply_keyboard_settings",
    # Dashboard
    "dashboard_command",
    # Auth handlers
    "auth_request_access",
    "notify_admins_new_request",
    "auth_approve_user",
    "auth_deny_user",
    "auth_request_again",
    # Admin handlers
    "admin_users_command",
    "admin_revoke_user",
    "show_updated_user_list",
    "admin_unfreeze_user",
    "admin_close",
    # Search handlers
    "search_command",
    "search_command_from_callback",
    "search_type_callback",
    "search_query_handler",
    # Settings handlers
    "settings_command",
    "settings_command_from_callback",
    "settings_callback",
    # Milestone handlers
    "check_and_broadcast_milestones",
]
