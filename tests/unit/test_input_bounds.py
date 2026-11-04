"""Bad input is answered 400 before it reaches the database, never 503.

An integer past the width of the column it is compared with, or a NUL in a
text parameter, used to reach asyncpg and fail there. The engine routers read
that failure as the store refusing, so the request was answered 503 naming the
page and the canary paged `read_refused`, for a request no page sends. The
Rust ports of wave 1 found it on `product_id`, `category_id`,
`expense_type_id` and a NUL in `category`.

Integers are held to their column's width where the route reads them
(`core.validators.validate_db_int`, `validate_category_id`); a NUL in an /api/
path or query string is refused before routing
(`web.middleware.RejectNulMiddleware`). The walk at the end keeps it closed:
every integer parameter of every /api/ route is bounded on both sides by
FastAPI or passed to a bounded validator, or is in EXEMPT with the reason it
never reaches SQL.
"""
from __future__ import annotations

import ast
import inspect
import textwrap
import typing

import pytest

from core.validators import INT32_MAX, INT64_MAX
from tests.routes_helper import iter_endpoints
from tests.unit.test_read_fallback_sites import admin_client  # noqa: F401 — fixture


# ─── Integers past their column ──────────────────────────────────────────────

# (method, url, field). Each value is one past the column the parameter meets:
# integer for categories, expense types, buyers, managers and manual expenses;
# bigint for product_id (compared with COALESCE(product_id, line_id)), offsets
# and Telegram user ids.
OUT_OF_RANGE = [
    ("GET", f"/api/summary?category_id={INT32_MAX + 1}", "category_id"),
    ("GET", f"/api/categories/{INT32_MAX + 1}/children", "category_id"),
    ("GET", f"/api/expenses/summary?expense_type_id={INT32_MAX + 1}", "expense_type_id"),
    ("GET", f"/api/expenses/summary?expense_type_id={-INT32_MAX - 2}", "expense_type_id"),
    ("DELETE", f"/api/expenses/{INT32_MAX + 1}", "expense_id"),
    ("GET", f"/api/products/intel/pairs?product_id={INT64_MAX + 1}", "product_id"),
    ("GET", f"/api/products/intel/pairs?product_id={-INT64_MAX - 2}", "product_id"),
    ("POST", f"/api/customers/sms-campaigns/optout?buyer_id={INT32_MAX + 1}", "buyer_id"),
    ("GET", f"/api/traffic/transactions?offset={INT64_MAX + 1}", "offset"),
    ("GET", f"/api/traffic/utm-campaigns?offset={INT64_MAX + 1}", "offset"),
    ("GET", f"/api/admin/users?offset={INT64_MAX + 1}", "offset"),
    ("GET", f"/api/admin/users/{INT64_MAX + 1}", "user_id"),
    ("PATCH", f"/api/admin/users/{INT64_MAX + 1}/role?role=viewer", "user_id"),
    ("PATCH", f"/api/admin/users/{INT64_MAX + 1}/status?status=approved", "user_id"),
    ("PATCH", f"/api/admin/users/{INT64_MAX + 1}/features", "user_id"),
    ("POST", f"/api/admin/access-requests/{INT64_MAX + 1}/approve", "user_id"),
    ("POST", f"/api/admin/access-requests/{INT64_MAX + 1}/deny", "user_id"),
    ("POST", f"/api/managers/{INT32_MAX + 1}/retail-status?is_retail=true", "manager_id"),
]


@pytest.mark.parametrize("method, url, field", OUT_OF_RANGE)
def test_an_integer_past_its_column_is_400(admin_client, method, url, field):
    """Answered by the validator, naming the field: the store is never asked,
    so nothing is refused and nothing pages."""
    response = admin_client.request(method, url)
    assert response.status_code == 400, response.text[:200]
    assert response.json()["detail"].startswith(f"{field}: "), response.text[:200]


# ─── NUL ─────────────────────────────────────────────────────────────────────

NUL_ERROR = {"error": "Validation Error",
             "detail": "query: Must not contain a NUL character"}


def test_a_nul_in_the_query_string_is_400(admin_client):
    response = admin_client.get("/api/summary?brand=a%00b")
    assert response.status_code == 400
    assert response.json() == NUL_ERROR


def test_a_nul_in_the_path_is_400(admin_client):
    response = admin_client.get("/api/customers/sms-campaigns/a%00b/results")
    assert response.status_code == 400
    assert response.json()["detail"] == "path: Must not contain a NUL character"


def test_a_nul_is_refused_before_the_session_is_asked():
    """Before routing, so before any dependency: no session needed for a 400."""
    from fastapi.testclient import TestClient

    from web.main import app

    response = TestClient(app).get("/api/summary?brand=%00")
    assert response.status_code == 400
    assert response.json() == NUL_ERROR


def test_a_page_is_not_the_middlewares_to_refuse(admin_client):
    """Only /api/: a page path answers as it did."""
    response = admin_client.get("/reports?x=%00", follow_redirects=False)
    assert response.status_code != 400


def test_a_request_without_a_nul_passes(admin_client):
    response = admin_client.get("/api/health")
    assert response.status_code == 200


def test_the_middleware_sits_inside_the_logging():
    """Added first, so it is innermost: the logging middleware still records
    the 400 with its correlation id."""
    from web.main import app
    from web.middleware import RejectNulMiddleware, RequestLoggingMiddleware

    classes = [m.cls for m in app.user_middleware]
    assert classes[-1] is RejectNulMiddleware
    assert classes.index(RequestLoggingMiddleware) < classes.index(RejectNulMiddleware)


# ─── The walk ────────────────────────────────────────────────────────────────

# Validators that bound an integer on both sides.
BOUNDED_VALIDATORS = frozenset({
    "validate_db_int", "validate_category_id", "validate_limit", "validate_source_id",
})

# Integer parameters that never reach SQL, and why. Each must still exist:
# an entry that names nothing is a place the next parameter could hide.
EXEMPT = {
    ("GET", "/api/tg-bot/activity", "days"):
        "checked against tg_bot_service.WINDOWS in the body; sent to ks-tg-bot over HTTP",
    ("POST", "/api/mirror/backfill/orders", "max_chunks"):
        "a loop bound in Python, never a statement parameter",
    ("POST", "/api/duckdb/resync", "days"):
        "the KeyCRM fetch window, computed in Python; never a statement parameter",
}


def _is_int(param) -> bool:
    annotation = getattr(param.field_info, "annotation", None)
    return annotation is int or int in typing.get_args(annotation)


def _fastapi_bounds(param) -> typing.Tuple[bool, bool]:
    lower = upper = False
    for item in getattr(param.field_info, "metadata", None) or ():
        if getattr(item, "ge", None) is not None or getattr(item, "gt", None) is not None:
            lower = True
        if getattr(item, "le", None) is not None or getattr(item, "lt", None) is not None:
            upper = True
    return lower, upper


def _validated_names(function, depth: int = 1) -> typing.Set[str]:
    """Names passed as the first argument to a bounded validator in the body,
    or handed to a function of the same module that validates the parameter
    they land in (`reports._parse_common_params`), one level down."""
    function = inspect.unwrap(function)
    source = textwrap.dedent(inspect.getsource(function))
    names = set()
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
        if name in BOUNDED_VALIDATORS:
            if node.args and isinstance(node.args[0], ast.Name):
                names.add(node.args[0].id)
            continue
        helper = function.__globals__.get(name) if isinstance(func, ast.Name) else None
        if depth and inspect.isfunction(helper):
            inner = _validated_names(helper, depth - 1)
            params = list(inspect.signature(helper).parameters)
            for position, arg in enumerate(node.args):
                if position < len(params) and params[position] in inner \
                        and isinstance(arg, ast.Name):
                    names.add(arg.id)
            for keyword in node.keywords:
                if keyword.arg in inner and isinstance(keyword.value, ast.Name):
                    names.add(keyword.value.id)
    return names


def _integer_parameters():
    from web.main import app

    for endpoint in iter_endpoints(app):
        if not endpoint.path.startswith("/api/"):
            continue
        dependant = endpoint.route.dependant
        validated = _validated_names(endpoint.route.endpoint)
        for param in list(dependant.query_params) + list(dependant.path_params):
            if _is_int(param):
                for method in sorted(endpoint.methods):
                    yield method, endpoint.path, param, validated


def test_every_integer_parameter_is_bounded_before_sql():
    unbounded = []
    for method, path, param, validated in _integer_parameters():
        lower, upper = _fastapi_bounds(param)
        if (lower and upper) or param.name in validated:
            continue
        if (method, path, param.name) not in EXEMPT:
            unbounded.append(f"{method} {path} {param.name}")
    assert not unbounded, (
        "integer parameters with no upper and lower bound before SQL — bound "
        "them with Query(ge=, le=) or pass them to core.validators.validate_db_int: "
        + ", ".join(unbounded))


def test_the_walk_sees_parameters_and_every_exemption_exists():
    seen = {(method, path, param.name) for method, path, param, _ in _integer_parameters()}
    assert len(seen) > 60, f"the walk found only {len(seen)} integer parameters"
    assert ("GET", "/api/products/intel/pairs", "product_id") in seen
    assert ("GET", "/api/admin/users/{user_id}", "user_id") in seen
    missing = sorted(key for key in EXEMPT if key not in seen)
    assert not missing, f"exemptions that name no parameter: {missing}"
