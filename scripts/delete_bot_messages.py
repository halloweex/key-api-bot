#!/usr/bin/env python3
"""
Delete bot messages from Telegram chats.

Usage:
    python scripts/delete_bot_messages.py                    # Delete last 100 messages from all admins
    python scripts/delete_bot_messages.py --count 50         # Delete last 50 messages
    python scripts/delete_bot_messages.py --chat 129462784   # Delete from specific chat only
    python scripts/delete_bot_messages.py --all              # Delete ALL bot messages (slow)
"""

import argparse
import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import httpx
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_USER_IDS = [int(x.strip()) for x in os.getenv("ADMIN_USER_IDS", "").split(",") if x.strip()]

API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"


async def get_current_message_id(client: httpx.AsyncClient, chat_id: int) -> int | None:
    """Send a test message to get current message ID, then delete it."""
    try:
        resp = await client.post(
            f"{API_URL}/sendMessage",
            json={"chat_id": chat_id, "text": "🔍"}
        )
        result = resp.json()
        if result.get("ok"):
            msg_id = result["result"]["message_id"]
            # Delete the test message
            await client.post(
                f"{API_URL}/deleteMessage",
                json={"chat_id": chat_id, "message_id": msg_id}
            )
            return msg_id
    except Exception as e:
        print(f"  Error getting message ID for chat {chat_id}: {e}")
    return None


async def delete_message(client: httpx.AsyncClient, chat_id: int, message_id: int) -> bool:
    """Try to delete a message."""
    try:
        resp = await client.post(
            f"{API_URL}/deleteMessage",
            json={"chat_id": chat_id, "message_id": message_id}
        )
        return resp.json().get("ok", False)
    except:
        return False


async def delete_messages_from_chat(
    client: httpx.AsyncClient,
    chat_id: int,
    count: int = 100,
    delete_all: bool = False
) -> int:
    """Delete recent bot messages from a chat."""
    current_id = await get_current_message_id(client, chat_id)
    if not current_id:
        print(f"  Cannot access chat {chat_id}")
        return 0

    end_id = 1 if delete_all else max(1, current_id - count)
    print(f"  Scanning messages {current_id - 1} to {end_id}...")

    deleted = 0
    batch_size = 20

    for batch_start in range(current_id - 1, end_id - 1, -batch_size):
        batch_end = max(end_id, batch_start - batch_size + 1)
        tasks = [
            delete_message(client, chat_id, msg_id)
            for msg_id in range(batch_start, batch_end - 1, -1)
        ]
        results = await asyncio.gather(*tasks)
        batch_deleted = sum(results)
        deleted += batch_deleted

        if batch_deleted > 0:
            print(f"  Deleted {batch_deleted} messages in range {batch_start}-{batch_end}")

        # Rate limiting
        await asyncio.sleep(0.5)

    return deleted


async def main():
    parser = argparse.ArgumentParser(description="Delete bot messages from Telegram")
    parser.add_argument("--count", type=int, default=100, help="Number of recent messages to scan (default: 100)")
    parser.add_argument("--chat", type=int, help="Specific chat ID to clean (default: all admins)")
    parser.add_argument("--all", action="store_true", help="Delete ALL messages (slow, use with caution)")
    args = parser.parse_args()

    if not BOT_TOKEN:
        print("Error: BOT_TOKEN not found in .env")
        sys.exit(1)

    # Verify bot
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(f"{API_URL}/getMe")
        bot = resp.json().get("result", {})
        print(f"Bot: @{bot.get('username')} ({bot.get('first_name')})")
        print()

        chat_ids = [args.chat] if args.chat else ADMIN_USER_IDS

        if not chat_ids:
            print("Error: No chat IDs to process. Set ADMIN_USER_IDS in .env or use --chat")
            sys.exit(1)

        total_deleted = 0

        for chat_id in chat_ids:
            print(f"Processing chat {chat_id}...")
            deleted = await delete_messages_from_chat(
                client,
                chat_id,
                count=args.count,
                delete_all=args.all
            )
            total_deleted += deleted
            print(f"  Done: {deleted} messages deleted")
            print()

        print(f"Total: {total_deleted} messages deleted")


if __name__ == "__main__":
    asyncio.run(main())
