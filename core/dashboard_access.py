"""Which tabs one person may open — the statements, and the two ways to run them.

The rule these three statements carry lives in `core/permissions.py`
(`apply_feature_override`): **the tab set decides what is visible, the role
decides what may be done inside it**. This module is only about writing that
set down and reading it back.

WHY THE SQL IS HERE AND NOT IN THE REPOSITORY THAT USES IT

Two callers write this column and they are in different containers:

* the **web** container, through the admin page, on whichever store
  `KS_USER_STORE` names — `core/repositories/users.py` runs these against
  DuckDB or Postgres with the `{users}` / `{self}` holes filled in;
* the **bot**, at the moment an admin approves an access request, through
  `bot/store_postgres.py` and only ever against Postgres.

A grant written two ways would drift the first time one of them learned
something the other did not — that an approval must not demote an existing
admin, say. So the statements have one home and two runners.

WHY THE BOT CANNOT WRITE THIS ON EVERY ENGINE

DuckDB takes a single writer and the web container holds it, so a bot process
cannot open the analytics file at all — the same reason `core/bot_prefs.py`
exists. Under `KS_USER_STORE=duckdb` the bot therefore has no way to reach the
dashboard's user list, and `PostgresDashboardAccess.available()` says so rather
than pretending: the approval still grants bot access, and the tabs are set
from the admin page instead. That is a degradation the admin is told about, not
one they discover.
"""
from __future__ import annotations

from typing import Any, Dict, Optional, Sequence

# Every column the two writers need to read back. `role` and `status` are here
# because the bot renders them beside the checklist, and a keyboard drawn from
# a stale idea of somebody's role is how an admin ticks a box that does nothing.
ACCESS_COLUMNS = (
    "user_id", "username", "first_name", "last_name",
    "role", "status", "allowed_features",
)

SELECT_ACCESS = f"""
    SELECT {', '.join(ACCESS_COLUMNS)}
    FROM {{users}}
    WHERE user_id = ?
"""

# An approval, from the bot or from the admin page. Deliberately *not*
# `create_user` with a status update after it: two statements would leave a row
# approved with the previous tab set for however long the second one took, and
# the bot's approval is the moment somebody is told they have access.
#
# `role` is absent from the update list on purpose. An approval answers "may
# this person in", never "as what" — re-approving an admin whose request row
# came back to pending must not silently demote them to viewer. The names use
# `COALESCE` for `create_user`'s reason: Telegram stops sending a field it no
# longer has, and a repeat login must not blank a name.
GRANT_ACCESS = """
    INSERT INTO {users}
        (user_id, username, first_name, last_name,
         status, role, allowed_features, reviewed_at, reviewed_by, denial_count)
    VALUES (?, ?, ?, ?, 'approved', ?, ?, CURRENT_TIMESTAMP, ?, 0)
    ON CONFLICT (user_id) DO UPDATE SET
        username = COALESCE(excluded.username, {self}.username),
        first_name = COALESCE(excluded.first_name, {self}.first_name),
        last_name = COALESCE(excluded.last_name, {self}.last_name),
        status = 'approved',
        allowed_features = excluded.allowed_features,
        -- `excluded.reviewed_at`, not `CURRENT_TIMESTAMP`. DuckDB cannot
        -- resolve a bare function call on the right of a `DO UPDATE SET` — it
        -- tries to bind the name as a column and fails with "Table \"users\"
        -- does not have a column named CURRENT_TIMESTAMP". Found by running
        -- the statement, not by reading about it. The value is the same one
        -- the VALUES clause above computed, and `create_user` and
        -- `set_permission` in this repository already take timestamps out of
        -- `excluded` for the same reason.
        reviewed_at = excluded.reviewed_at,
        reviewed_by = excluded.reviewed_by,
        denial_count = 0
"""

# `RETURNING` rather than a rowcount: DuckDB and asyncpg report an UPDATE that
# matched nothing differently, and "there is no such user" is an answer the
# admin page turns into a 404.
SET_FEATURES = """
    UPDATE {users}
    SET allowed_features = ?, reviewed_at = CURRENT_TIMESTAMP, reviewed_by = ?
    WHERE user_id = ?
    RETURNING user_id
"""


def access_row(row: Optional[Sequence[Any]]) -> Optional[Dict[str, Any]]:
    """One `SELECT_ACCESS` row as a dict, with the tab set already parsed.

    `allowed_features` comes back as a list of keys, or `None` for "as the
    role" — the distinction the column exists to carry, so it is resolved here
    once instead of at each of the places that reads it.
    """
    if row is None:
        return None
    from core.permissions import parse_features

    data = dict(zip(ACCESS_COLUMNS, row))
    data["allowed_features"] = parse_features(data.get("allowed_features"))
    return data
