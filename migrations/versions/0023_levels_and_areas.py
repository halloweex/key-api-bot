"""Roles are levels; areas are tabs. Untangling the two.

Revision ID: 0023_levels_and_areas
Revises: 0022_role_permissions
Created: 2026-09-08

WHAT WAS WRONG

`marketer` was a *role*, and it was an **area wearing a level's clothes**: a
viewer's depth plus `sms` edit. So "a marketer who may only look at campaign
results" could not be said at all — the only marketer there could be was one
who may send. The owner put it plainly: viewer and editor are one category,
marketer is another, and a person can be marketer-viewer or marketer-editor.

THE TWO AXES

* **level** — viewer, editor, admin. How deep, and nothing else.
* **area** — `dashboard_users.allowed_features`. Which tabs, and nothing else.

`view` is now "the tab is in my set"; `edit` is that and a level of editor or
above. Which makes both sentences expressible with one pair each.

WHAT THIS MIGRATION HAS TO DO, AND WHY DATA AND NOT ONLY CODE

`seed_default_permissions` fills pairs the table does not carry; it never
rewrites one it does. So the 48 rows already stored still describe the old
world — `viewer/sms = false`, which would make "marketer viewer" a person who
holds the SMS tab and sees nothing on it. The rows are rewritten here.

Safe to rewrite because it was measured before it was written: on the
2026-09-07 snapshot every one of the 32 stored pairs equalled the code
defaults, so nobody has ever customised this matrix and nothing is being
overwritten but the defaults themselves.

THE ONE ACCOUNT

Exactly one row carried `marketer` (measured the same day: 2 admins, 1
marketer, 16 viewers). It becomes `editor` with the tab set the role used to
grant — everything a viewer saw plus SMS — which is the owner's choice of the
two on offer, and keeps every ability that account has today.
"""
from alembic import op

revision = "0023_levels_and_areas"
down_revision = "0022_role_permissions"
branch_labels = None
depends_on = None

# Everything a viewer could reach before per-user tabs existed, plus SMS.
# Spelled here rather than imported: a migration is a record of what was done
# on the day it ran, and `ACCESS_PRESETS` is free to change afterwards.
MARKETER_TABS = "dashboard,products,traffic,inventory,reports,marketing,sms"

# The tabs a level opens when nobody has set any — needed as data only for the
# editor row below, which gains `expenses` exactly as the old editor row had it.
LEVELS = {
    # level: (can_view, can_edit, can_delete, user_management)
    "admin": (True, True, True, True),
    "editor": (True, True, False, False),
    "viewer": (True, False, False, False),
}


def upgrade() -> None:
    # The account whose role was really an area.
    op.execute(
        f"""
        UPDATE app.dashboard_users
           SET role = 'editor',
               allowed_features = COALESCE(allowed_features, '{MARKETER_TABS}'),
               reviewed_at = now()
         WHERE role = 'marketer'
        """
    )

    # The matrix, rewritten as depth. One statement per level so the intent is
    # readable in the migration rather than assembled at runtime.
    op.execute("DELETE FROM app.role_permissions WHERE role = 'marketer'")
    for level, (view, edit, delete, user_management) in LEVELS.items():
        op.execute(
            f"""
            UPDATE app.role_permissions
               SET can_view = {str(view).lower()},
                   can_edit = {str(edit).lower()},
                   can_delete = {str(delete).lower()},
                   updated_at = now()
             WHERE role = '{level}' AND feature <> 'user_management'
            """
        )
        op.execute(
            f"""
            UPDATE app.role_permissions
               SET can_view = {str(user_management).lower()},
                   can_edit = {str(user_management).lower()},
                   can_delete = {str(user_management).lower()},
                   updated_at = now()
             WHERE role = '{level}' AND feature = 'user_management'
            """
        )


def downgrade() -> None:
    # Deliberately not reversible. Going back would have to invent which of the
    # editors had been a marketer, and a guess about who may spend money on SMS
    # is worse than a migration that only goes forward.
    raise NotImplementedError(
        "0023 is not reversible: it cannot know which editor used to be the "
        "marketer, and guessing that is guessing who may send SMS."
    )
