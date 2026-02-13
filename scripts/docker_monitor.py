#!/usr/bin/env python3
"""
Docker container health monitor with Telegram alerts.

Monitors Docker events and sends Telegram notifications when:
- Container dies/crashes
- Container restarts
- Health check fails

Run as systemd service or: python scripts/docker_monitor.py
"""
import os
import sys
import json
import asyncio
import logging
from datetime import datetime
from typing import Optional

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
BOT_TOKEN = os.getenv('BOT_TOKEN')
ADMIN_USER_IDS = os.getenv('ADMIN_USER_IDS', '').split(',')
MONITORED_CONTAINERS = {'keycrm-web', 'keycrm-bot', 'keycrm-nginx', 'keycrm-meili'}

# Cooldown to prevent alert spam (seconds)
ALERT_COOLDOWN = 60
_last_alerts: dict[str, float] = {}


async def send_telegram_alert(message: str) -> None:
    """Send alert to all admin users via Telegram."""
    if not BOT_TOKEN or not ADMIN_USER_IDS[0]:
        logger.warning("BOT_TOKEN or ADMIN_USER_IDS not configured, skipping alert")
        return

    import httpx

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    async with httpx.AsyncClient() as client:
        for user_id in ADMIN_USER_IDS:
            if not user_id.strip():
                continue
            try:
                payload = {
                    "chat_id": user_id.strip(),
                    "text": message,
                    "parse_mode": "HTML",
                }
                resp = await client.post(url, json=payload)
                if resp.status_code != 200:
                    logger.error(f"Failed to send alert to {user_id}: {resp.text}")
                else:
                    logger.info(f"Alert sent to {user_id}")
            except Exception as e:
                logger.error(f"Error sending alert to {user_id}: {e}")


def should_alert(container_name: str) -> bool:
    """Check if we should send alert (cooldown check)."""
    now = datetime.now().timestamp()
    last_alert = _last_alerts.get(container_name, 0)

    if now - last_alert < ALERT_COOLDOWN:
        return False

    _last_alerts[container_name] = now
    return True


async def handle_event(event: dict) -> None:
    """Handle Docker event and send alert if needed."""
    status = event.get('status', '')
    actor = event.get('Actor', {})
    attributes = actor.get('Attributes', {})
    container_name = attributes.get('name', 'unknown')

    # Only monitor our containers
    if container_name not in MONITORED_CONTAINERS:
        return

    # Events we care about
    alert_events = {
        'die': '💀 Container crashed',
        'kill': '🔪 Container killed',
        'oom': '💥 Out of memory',
        'stop': '🛑 Container stopped',
        'restart': '🔄 Container restarting',
        'health_status: unhealthy': '🤒 Health check failed',
    }

    for event_type, emoji_msg in alert_events.items():
        if event_type in status:
            if not should_alert(f"{container_name}:{event_type}"):
                logger.info(f"Skipping alert for {container_name} (cooldown)")
                return

            exit_code = attributes.get('exitCode', 'N/A')
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            message = f"""
{emoji_msg}

<b>Container:</b> {container_name}
<b>Event:</b> {status}
<b>Exit Code:</b> {exit_code}
<b>Time:</b> {timestamp}

Check logs: <code>docker logs {container_name} --tail 50</code>
"""
            await send_telegram_alert(message.strip())
            break


async def monitor_docker_events() -> None:
    """Monitor Docker events using docker CLI."""
    import subprocess

    logger.info("Starting Docker event monitor...")
    logger.info(f"Monitoring containers: {MONITORED_CONTAINERS}")

    # Send startup notification
    await send_telegram_alert("🟢 Docker monitor started\n\nMonitoring container health...")

    # Use docker events command with JSON format
    cmd = [
        'docker', 'events',
        '--format', '{{json .}}',
        '--filter', 'type=container',
        '--filter', 'event=die',
        '--filter', 'event=kill',
        '--filter', 'event=oom',
        '--filter', 'event=stop',
        '--filter', 'event=restart',
        '--filter', 'event=health_status',
    ]

    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    logger.info("Connected to Docker events stream")

    while True:
        line = await process.stdout.readline()
        if not line:
            break

        try:
            event = json.loads(line.decode('utf-8'))
            await handle_event(event)
        except json.JSONDecodeError:
            continue
        except Exception as e:
            logger.error(f"Error handling event: {e}")


async def main() -> None:
    """Main entry point."""
    # Verify configuration
    if not BOT_TOKEN:
        logger.warning("BOT_TOKEN not set - alerts will be logged only")
    if not ADMIN_USER_IDS[0]:
        logger.warning("ADMIN_USER_IDS not set - alerts will be logged only")

    try:
        await monitor_docker_events()
    except KeyboardInterrupt:
        logger.info("Monitor stopped by user")
    except Exception as e:
        logger.error(f"Monitor error: {e}")
        # Try to send alert about monitor failure
        await send_telegram_alert(f"🔴 Docker monitor crashed!\n\nError: {e}")
        raise


if __name__ == '__main__':
    asyncio.run(main())
