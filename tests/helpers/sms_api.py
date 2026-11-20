"""A fake store and an admin's headers for the SMS audience routes."""
import time

from core.permissions import ADMIN_USER_IDS
from web.routes.auth import create_session_data, SESSION_COOKIE, session_serializer


ADMIN_ID = sorted(ADMIN_USER_IDS)[0]


def _headers() -> dict:
    data = create_session_data(
        {
            "id": str(ADMIN_ID),
            "first_name": "Test",
            "username": "tester",
            "auth_date": str(int(time.time())),
        },
        role="admin",
    )
    return {"Cookie": f"{SESSION_COOKIE}={session_serializer.dumps(data)}"}


def _customer(buyer_id: int, assignment: str) -> dict:
    return {
        "buyerId": buyer_id, "fullName": f"Buyer {buyer_id}",
        "phone": f"3809{buyer_id:08d}", "city": "Kyiv", "tier": "ALL",
        "orders": 1, "ltv": 900.0, "avgOrderValue": 900.0,
        "revenueLtv": 900.0, "marginLtv": 500.0, "marginPct": 55.0,
        "costCoverage": 99.0, "recencyDays": 20,
        "lastOrderDate": "2026-08-01", "firstOrderDate": "2026-08-01",
        "lastOrderId": 5, "lastOrderTotal": 900.0, "lastOrderItemCount": 1,
        "lastOrderItems": "Product", "assignment": assignment,
    }


class _FakeStore:
    def __init__(self):
        self.calls = []
        self.freezes = []
        self.presets = []
        self.saved = []
        self.deleted = []
        self.customers = [_customer(1, "target"), _customer(2, "holdout")]
        self.truncated = False
        self.freeze_error = None
        self.save_error = None
        self.delete_result = True

    async def get_sms_segments(self, **kwargs):
        self.calls.append(kwargs)
        return {
            "campaign": kwargs.get("campaign"),
            "grouping": kwargs.get("grouping"),
            "criteria": {"grouping": kwargs.get("grouping"), "filters": {}},
            "segments": [{"tier": "ALL", "total": 2, "target": 1, "holdout": 1}],
            "totals": {"customers": 2, "target": 1, "holdout": 1},
            "funnel": [{"stage": "customers", "remaining": 9}],
            "customers": self.customers if kwargs.get("include_customers") else [],
            "truncated": self.truncated,
        }

    async def freeze_sms_campaign(self, **kwargs):
        if self.freeze_error:
            raise ValueError(self.freeze_error)
        self.freezes.append(kwargs)
        return {"campaign": kwargs["campaign"], "frozen": True}

    async def list_sms_audience_presets(self):
        return self.presets

    async def save_sms_audience_preset(self, name, criteria, created_by=None):
        if self.save_error:
            raise ValueError(self.save_error)
        self.saved.append((name, criteria, created_by))
        return {"name": name, "criteria": criteria, "builtin": False}

    async def delete_sms_audience_preset(self, name):
        self.deleted.append(name)
        return self.delete_result
