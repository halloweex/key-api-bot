"""A heartbeat the bot writes, and the container's healthcheck reads.

The old check was `os.path.exists('/app/data/bot.db')`. It was weak from the
day it was written — a file existing says nothing about whether the bot is
polling Telegram — and on 2026-08-27 it became actively misleading: the bot
moved to Postgres, `bot.db` was frozen in place, and the check went on
reporting healthy against a file nothing writes.

WHAT A HONEST SIGNAL HAS TO COVER

Two things, and one file carries both:

* **The event loop ran.** The beat is a `job_queue` job, so it only happens if
  the loop that also answers Telegram is turning. A wedged loop stops the beat.
* **The store answered.** The beat does a real read through the port first and
  writes nothing if it raises, so a Postgres that has gone away goes stale
  within two intervals — which is the failure the switch introduced and the
  old check could never have seen.

WHY THE CHECK ITSELF ONLY LOOKS AT A FILE

Because a healthcheck runs in its own process, every thirty seconds, forever.
Under `KS_BOT_STORE=postgres` a check that queried the database would build a
connection pool and a loop thread each time and throw them away — 2,880 pools a
day to answer a question the bot can answer for free. So the bot does the work
and the check reads the result.

WHY `/tmp` AND NOT `/app/data`

`/app/data` is a bind mount shared with the web container: a beat there
survives the container that wrote it, so a dead bot could be reported alive by
its own ghost until the file aged out. `/tmp` is emptied with the container, so
a fresh one starts with no beat and is unhealthy until it produces its first —
which is exactly right, and what `start_period` is for.

IT DOES NOT RESTART ANYTHING

Compose does not act on a failing healthcheck — that is Swarm — and nothing
declares `depends_on: bot`. This is a signal for `docker ps` and for whoever is
watching, so it is tuned to be *informative* rather than safe to trip: a store
outage should show, not be smoothed over.
"""
from __future__ import annotations

import logging
import os
import time
from pathlib import Path

logger = logging.getLogger(__name__)

# Container-local on purpose — see the module docstring.
HEARTBEAT_PATH = Path(os.getenv("KS_BOT_HEARTBEAT", "/tmp/bot-heartbeat"))

# How often the bot beats.
BEAT_INTERVAL_SECONDS = 30

# How stale a beat may be before the container is unhealthy. Four intervals:
# enough that one slow query or one missed tick is not an incident, short
# enough that a wedged loop shows up inside two minutes.
MAX_AGE_SECONDS = 4 * BEAT_INTERVAL_SECONDS

# A user id no Telegram account has, so the probe reads an index and finds
# nothing. Cheaper than counting, and it exercises exactly the path every
# handler takes on its first line.
PROBE_USER_ID = 0


def probe_store() -> None:
    """One real read through the port. Raises if the store cannot answer."""
    from core.bot_store import get_bot_store

    get_bot_store().access.status(PROBE_USER_ID)


def write_beat(path: Path = None) -> None:
    """Record that the loop ran and the store answered.

    Written and then moved into place: a healthcheck reading a file mid-write
    would see an empty one and call it a bad beat. The rename is atomic on the
    same filesystem, which `/tmp` guarantees here.
    """
    path = path or HEARTBEAT_PATH
    temporary = path.with_suffix(".tmp")
    temporary.write_text(str(time.time()))
    temporary.replace(path)


def beat_age_seconds(path: Path = None, now: float = None) -> float:
    """How long ago the last beat was, or `inf` if there has never been one."""
    path = path or HEARTBEAT_PATH
    now = time.time() if now is None else now
    try:
        return now - float(path.read_text().strip())
    except (OSError, ValueError):
        return float("inf")


def is_healthy(path: Path = None, now: float = None) -> bool:
    return beat_age_seconds(path, now) <= MAX_AGE_SECONDS


async def beat_job(context) -> None:
    """The `job_queue` job. Never raises — a healthcheck is not worth taking
    the bot down for, and a beat that does not happen says so by itself."""
    try:
        probe_store()
        write_beat()
    except Exception as e:
        # DEBUG on purpose: the beat going stale is the signal, and a store
        # outage would otherwise write this line every thirty seconds into the
        # log somebody is trying to read the outage in.
        logger.debug("Heartbeat skipped: %s", e)


def main() -> int:
    """The healthcheck's entry point: `python -m bot.heartbeat`.

    A module rather than a `python -c` one-liner in the compose file, so the
    rule and the reason live next to each other and the check can be tested.
    """
    return 0 if is_healthy() else 1


if __name__ == "__main__":
    raise SystemExit(main())
