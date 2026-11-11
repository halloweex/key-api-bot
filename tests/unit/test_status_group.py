"""What counts as revenue comes from KeyCRM, not from our memory of KeyCRM.

Revenue excludes the lost/cancel group. We encoded that as a list of status
ids, and a list can only describe the statuses that existed when it was
written. Status 20 — «Прибув у відділення», KeyCRM group 4, a parcel that has
reached the branch — first appeared on 2026-07-09 and nothing noticed for a
month. It was counted as revenue, correctly, by luck rather than by rule: a
comment in the integrity module had it filed under "return/cancel family",
and two sessions spent effort on a ₴265,230.78 discrepancy that did not exist.

Verified against the live API on 2026-08-09, one real order per status:

    status  1  → group 1     status 15  → group 6
    status  9  → group 4     status 19  → group 6
    status 12  → group 5     status 21  → group 6
    status 20  → group 4     status 22  → group 6
                             status 23  → group 6

The members now carry KeyCRM's labels; the names this codebase used before
are preserved in `LEGACY_STATUS_NAMES`, because whether the CRM's labels are
right is a question about the business and it is not settled here.
"""

import pytest

from core.pg_warehouse_dq import KNOWN_STATUS_IDS
from core.models import (
    LEGACY_STATUS_NAMES,
    LOST_STATUS_GROUP_ID,
    Order,
    OrderStatus,
)
from tests.pg_orders_seed import needs_pg, pg_orders  # noqa: F401 — the fixture


# The mapping the API returned, as measured.
MEASURED_GROUPS = {1: 1, 9: 4, 12: 5, 15: 6, 19: 6, 20: 4, 21: 6, 22: 6, 23: 6}


def _api_order(oid: int, status_id: int, group_id=None, total="1000.00"):
    payload = {
        "id": oid,
        "source_id": 4,
        "status_id": status_id,
        "grand_total": total,
        "ordered_at": "2026-08-01T10:00:00+00:00",
        "created_at": "2026-08-01T10:00:00+00:00",
        "updated_at": "2026-08-01T10:00:00+00:00",
        "buyer": {"id": 10},
        "products": [],
    }
    if group_id is not None:
        payload["status_group_id"] = group_id
    return payload


class TestTheOldNamesAreKeptOnFile:
    """A label in a CRM is typed by a person and can be wrong.

    The members now carry KeyCRM's labels, but whether those labels are right
    is a question about the business, not about the data. If 19 has always
    meant a *return* here while KeyCRM files it as `canceled`, the old name
    was the truer one — so it is written down, not thrown away.
    """

    def test_every_previous_name_is_on_file(self):
        assert LEGACY_STATUS_NAMES == {
            15: "NOT_AVAILABLE",
            18: "DID_NOT_ARRANGE_PRICE",
            19: "RETURNED",
            21: "CANCELED",
            22: "REFUNDED",
            23: "REJECTED",
        }

    def test_the_record_covers_every_excluded_status(self):
        """A rename with no way back is what this guards against."""
        assert {int(s) for s in OrderStatus.return_statuses()} == set(LEGACY_STATUS_NAMES)

    def test_the_names_moved_but_the_set_did_not(self):
        """No figure depends on the vocabulary — only on this membership."""
        assert {int(s) for s in OrderStatus.return_statuses()} == {15, 18, 19, 21, 22, 23}

    def test_the_current_names_follow_keycrm(self):
        assert OrderStatus.CANCELED == 19
        assert OrderStatus.DELIVERY_FAILED == 21
        assert OrderStatus.RETURNED == 22
        assert OrderStatus.RETURNING == 23


class TestTheListMatchesTheSource:
    @pytest.mark.parametrize("status_id,group_id", sorted(MEASURED_GROUPS.items()))
    def test_our_verdict_equals_keycrms_for_every_live_status(self, status_id, group_id):
        by_list = status_id in {int(s) for s in OrderStatus.return_statuses()}
        by_group = group_id == LOST_STATUS_GROUP_ID
        assert by_list == by_group, f"status {status_id} disagrees with group {group_id}"

    def test_status_20_is_revenue(self):
        """«Прибув у відділення» — a parcel at the branch, not a cancellation."""
        assert 20 not in {int(s) for s in OrderStatus.return_statuses()}
        assert MEASURED_GROUPS[20] != LOST_STATUS_GROUP_ID

    @pytest.mark.parametrize("status_id", sorted(MEASURED_GROUPS))
    def test_every_live_status_is_known_to_the_domain_check(self, status_id):
        assert status_id in KNOWN_STATUS_IDS

    @pytest.mark.parametrize("status_id", [3, 4, 8, 10, 11, 18, 24])
    def test_the_statuses_that_were_missing_are_registered(self, status_id):
        """24 is «Зібрано для самовивозу» — an ordinary parcel awaiting pickup.
        It would have been reported as an unknown status."""
        assert status_id in KNOWN_STATUS_IDS

    def test_every_excluded_status_is_also_a_known_one(self):
        """18 was excluded from revenue and absent from the known set at once."""
        assert {int(s) for s in OrderStatus.return_statuses()} <= KNOWN_STATUS_IDS


class TestOrderPrefersTheGroup:
    def test_the_group_decides_when_present(self):
        order = Order.from_api(_api_order(1, status_id=12, group_id=6))
        assert order.status_group_id == 6
        assert order.is_return is True, "status 12 but KeyCRM says lost/cancel"

    def test_the_group_can_also_rescue_revenue(self):
        order = Order.from_api(_api_order(1, status_id=19, group_id=4))
        assert order.is_return is False

    def test_the_id_list_covers_a_payload_without_a_group(self):
        order = Order.from_api(_api_order(1, status_id=19))
        assert order.status_group_id is None
        assert order.is_return is True

    def test_a_nested_status_object_is_read_too(self):
        payload = _api_order(1, status_id=12)
        del payload["status_id"]
        payload["status"] = {"id": 19, "status_group_id": 6}
        order = Order.from_api(payload)
        assert order.status_id == 19
        assert order.status_group_id == 6


@needs_pg
class TestSilverUsesIt:
    """The status-group rule in the Silver body, on Postgres: seeded through
    chain 3's writer and derived by `pg_silver.rebuild_silver`. It ran on
    DuckDB's writer and DuckDB's Silver until step 14's S14-09 deleted the
    writer; the body is the one both engines rendered
    (`core.warehouse_sql.silver_select_sql`)."""

    @staticmethod
    async def _silver(pool, payloads):
        from core import pg_orders_write
        from core.pg_silver import rebuild_silver

        await pg_orders_write.upsert_orders_with_expenses(payloads)
        try:
            await rebuild_silver(pool)
            async with pool.acquire() as conn:
                return {r["id"]: r["is_return"] for r in await conn.fetch(
                    "SELECT id, is_return FROM silver.orders ORDER BY id")}
        finally:
            async with pool.acquire() as conn:
                await conn.execute("DELETE FROM silver.orders")

    @pytest.mark.asyncio
    async def test_the_group_overrides_the_id_list_in_silver(self, pg_orders):
        _store, pool = pg_orders
        # Same status id, opposite groups.
        rows = await self._silver(pool, [
            _api_order(1, status_id=12, group_id=5),   # revenue
            _api_order(2, status_id=12, group_id=6),   # lost/cancel
        ])
        assert rows[1] is False
        assert rows[2] is True

    @pytest.mark.asyncio
    async def test_rows_without_a_group_keep_the_old_verdict(self, pg_orders):
        _store, pool = pg_orders
        rows = await self._silver(pool, [
            _api_order(1, status_id=19),   # excluded by the list
            _api_order(2, status_id=20),   # revenue: not in the list
        ])
        assert rows[1] is True
        assert rows[2] is False


# `TestTheColumnArrivesOnAnExistingDatabase` stood here until step 14's
# S14-18: DuckDB's migration that added `status_group_id` to an `orders`
# table created before it (`CREATE TABLE IF NOT EXISTS` adds no column).
# DuckDB's schema and migrations went with the store; Postgres's column is
# revision 0003's.
