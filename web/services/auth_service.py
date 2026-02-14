"""
Telegram Login authentication service.

Verifies Telegram Login Widget data and WebApp initData for user authorization.
"""
import hashlib
import hmac
import time
import json
import logging
from urllib.parse import parse_qs, unquote
from typing import Optional, Dict, Any

from bot.config import BOT_TOKEN
from bot.database import is_user_authorized, get_user_auth_status

logger = logging.getLogger(__name__)

# Auth data expires after 24 hours
AUTH_DATA_MAX_AGE = 86400

# WebApp data expires after 1 hour (more strict for auto-auth)
WEBAPP_DATA_MAX_AGE = 3600


def verify_webapp_auth(init_data: str) -> Dict[str, Any] | None:
    """
    Verify Telegram WebApp initData.

    The data is verified using HMAC-SHA256 with a special key derived from bot token.
    See: https://core.telegram.org/bots/webapps#validating-data-received-via-the-mini-app

    Args:
        init_data: The initData string from Telegram.WebApp.initData

    Returns:
        Dict with user data if valid, None if invalid
    """
    if not init_data:
        logger.warning("Empty initData received")
        return None

    try:
        # Parse the URL-encoded string
        parsed = parse_qs(init_data)

        # Get hash and remove from data for verification
        if 'hash' not in parsed:
            logger.warning("No hash in initData")
            return None

        received_hash = parsed['hash'][0]

        # Check auth_date expiration
        if 'auth_date' not in parsed:
            logger.warning("No auth_date in initData")
            return None

        auth_date = int(parsed['auth_date'][0])
        if time.time() - auth_date > WEBAPP_DATA_MAX_AGE:
            logger.warning(f"WebApp initData expired: {time.time() - auth_date}s old")
            return None

        # Build data-check-string (sorted, excluding hash)
        data_check_arr = []
        for key in sorted(parsed.keys()):
            if key != 'hash':
                # Values are lists, take first element
                value = parsed[key][0]
                data_check_arr.append(f"{key}={value}")
        data_check_string = '\n'.join(data_check_arr)

        # Create secret key: HMAC-SHA256(bot_token, "WebAppData")
        secret_key = hmac.new(
            b"WebAppData",
            BOT_TOKEN.encode(),
            hashlib.sha256
        ).digest()

        # Calculate HMAC-SHA256
        calculated_hash = hmac.new(
            secret_key,
            data_check_string.encode(),
            hashlib.sha256
        ).hexdigest()

        # Compare hashes
        if calculated_hash != received_hash:
            logger.warning("WebApp hash mismatch - invalid initData")
            return None

        # Parse user data from JSON
        if 'user' not in parsed:
            logger.warning("No user data in initData")
            return None

        user_data = json.loads(parsed['user'][0])
        user_data['auth_date'] = auth_date

        logger.info(f"WebApp auth verified for user {user_data.get('id')}")
        return user_data

    except (json.JSONDecodeError, ValueError, KeyError) as e:
        logger.warning(f"Error parsing WebApp initData: {e}")
        return None


def verify_telegram_auth(auth_data: Dict[str, Any]) -> bool:
    """
    Verify Telegram Login Widget authentication data.

    The data is verified using HMAC-SHA256 with the bot token.
    See: https://core.telegram.org/widgets/login#checking-authorization

    Args:
        auth_data: Dict containing id, first_name, username, photo_url, auth_date, hash

    Returns:
        True if data is valid and not expired
    """
    if not auth_data:
        return False

    # Check required fields
    if 'id' not in auth_data or 'hash' not in auth_data or 'auth_date' not in auth_data:
        logger.warning("Missing required fields in auth data")
        return False

    # Check if auth_date is not too old
    try:
        auth_date = int(auth_data['auth_date'])
        if time.time() - auth_date > AUTH_DATA_MAX_AGE:
            logger.warning(f"Auth data expired: {time.time() - auth_date}s old")
            return False
    except (ValueError, TypeError):
        logger.warning("Invalid auth_date format")
        return False

    # Extract the hash from data
    received_hash = auth_data.get('hash', '')

    # Build the data-check-string (sorted key=value pairs, excluding 'hash')
    data_check_arr = []
    for key in sorted(auth_data.keys()):
        if key != 'hash':
            data_check_arr.append(f"{key}={auth_data[key]}")
    data_check_string = '\n'.join(data_check_arr)

    # Create secret key: SHA256(bot_token)
    secret_key = hashlib.sha256(BOT_TOKEN.encode()).digest()

    # Calculate HMAC-SHA256
    calculated_hash = hmac.new(
        secret_key,
        data_check_string.encode(),
        hashlib.sha256
    ).hexdigest()

    # Compare hashes
    if calculated_hash != received_hash:
        logger.warning("Hash mismatch - invalid auth data")
        return False

    logger.info(f"Telegram auth verified for user {auth_data.get('id')}")
    return True


def check_user_access(user_id: int) -> Dict[str, Any]:
    """
    Check if a Telegram user has access to the dashboard.

    Args:
        user_id: Telegram user ID

    Returns:
        Dict with 'authorized' bool and 'status' string
    """
    from core.permissions import is_hardcoded_admin

    # Hardcoded admins always have access
    if is_hardcoded_admin(user_id):
        return {
            'authorized': True,
            'status': 'approved',
            'role': 'admin',
            'user_info': None
        }

    # Check if user is authorized in bot database (SQLite fallback)
    authorized = is_user_authorized(user_id)
    status_info = get_user_auth_status(user_id)

    status = status_info.get('status', 'unknown') if status_info else 'not_found'

    return {
        'authorized': authorized,
        'status': status,
        'user_info': status_info
    }


def create_session_data(auth_data: Dict[str, Any], role: str = "viewer") -> Dict[str, Any]:
    """
    Create session data from verified Telegram auth data.

    Args:
        auth_data: Verified Telegram auth data
        role: User role (admin, editor, viewer)

    Returns:
        Session data dict
    """
    return {
        'user_id': int(auth_data['id']),
        'first_name': auth_data.get('first_name', ''),
        'last_name': auth_data.get('last_name', ''),
        'username': auth_data.get('username', ''),
        'photo_url': auth_data.get('photo_url', ''),
        'auth_date': int(auth_data['auth_date']),
        'logged_in_at': int(time.time()),
        'role': role,
    }


async def get_user_role(user_id: int) -> str:
    """
    Get user role from DuckDB.

    Falls back to 'viewer' if user not found.
    Hardcoded admins always get 'admin' role.
    """
    from core.permissions import is_hardcoded_admin
    from core.duckdb_store import get_store

    # Hardcoded admin fallback
    if is_hardcoded_admin(user_id):
        return "admin"

    try:
        store = await get_store()
        user = await store.get_user(user_id)
        if user:
            return user.get("role", "viewer")
    except Exception as e:
        logger.warning(f"Error getting user role: {e}")

    return "viewer"


async def check_user_access_async(user_id: int, auth_data: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Check if a Telegram user has access to the dashboard (async version using DuckDB).

    Args:
        user_id: Telegram user ID
        auth_data: Optional auth data to create/update user record

    Returns:
        Dict with 'authorized' bool, 'status' string, and 'role'
    """
    from core.duckdb_store import get_store
    from core.permissions import is_hardcoded_admin

    try:
        store = await get_store()
        user = await store.get_user(user_id)

        if user:
            # Update last activity
            if auth_data:
                await store.update_user_activity(user_id)
            authorized = user.get("status") == "approved"
            return {
                "authorized": authorized,
                "status": user.get("status", "unknown"),
                "role": user.get("role", "viewer"),
                "user_info": user
            }

        # Hardcoded admin fallback - auto-create and approve
        if is_hardcoded_admin(user_id):
            # Create admin user record if we have auth data
            if auth_data:
                await store.create_user(
                    user_id=user_id,
                    username=auth_data.get('username'),
                    first_name=auth_data.get('first_name'),
                    last_name=auth_data.get('last_name'),
                    photo_url=auth_data.get('photo_url'),
                    role='admin',
                    status='approved',
                )
                logger.info(f"Auto-created admin user {user_id}")
            return {
                "authorized": True,
                "status": "approved",
                "role": "admin",
                "user_info": None
            }

        return {
            "authorized": False,
            "status": "not_found",
            "role": "viewer",
            "user_info": None
        }

    except Exception as e:
        logger.warning(f"Error checking user access: {e}")
        # Fallback to SQLite during migration
        return check_user_access(user_id)
