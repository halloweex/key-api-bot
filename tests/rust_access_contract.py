"""What the Rust pilot must know about access, read out of the running app.

`rust/contract/access.json` is written by this module and committed, and the
pilot embeds it when it is built. Nothing in it is typed by hand:

* the routes, in the order the app tries them, with each one's path pattern,
  methods and dependencies in the order FastAPI runs them — read from the
  effective route contexts FastAPI 0.141 dispatches on, because an included
  router's prefix and dependencies are not on its routes any more
  (`tests/routes_helper.py` says why);
* each route's rate limits, from slowapi's own registry;
* the permission matrix, the default tab sets, the session's parameters and
  the schema revision, from the modules that define them.

A dependency from `web.routes.auth` is a **gate** and is written with what it
checks — `require_permission`'s feature and action come out of its closure.
Any other dependency is **opaque**: whatever it does belongs to the endpoint,
and the pilot stops comparing at it.

`tests/unit/test_rust_access_contract.py` is to fail when this export and the
committed file disagree — a route added, a gate moved, a limit changed, a tab
renamed. Regenerate in the Python 3.14 image:

    python -m tests.rust_access_contract

The hardcoded admins go out as a SHA-256 of their sorted ids: the pilot is
given the ids at start and refuses to run unless they hash to this, so the
contract is not one more file that lists them.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Dict, List

CONTRACT_PATH = Path(__file__).resolve().parents[1] / "rust" / "contract" / "access.json"


class ContractError(RuntimeError):
    """The app holds something the pilot's contract cannot describe yet."""


def _name(call: Any) -> str:
    return f"{getattr(call, '__module__', '?')}.{getattr(call, '__qualname__', repr(call))}"


def _closure(call: Any) -> Dict[str, Any]:
    return dict(zip(call.__code__.co_freevars,
                    (cell.cell_contents for cell in call.__closure__ or ())))


def _dependency(call: Any) -> Dict[str, Any]:
    from web.routes import auth

    if call is auth.api_gate:
        return {"gate": "api_gate"}
    if call is auth.require_user:
        return {"gate": "require_user"}
    if call is auth.require_admin:
        return {"gate": "require_admin"}
    if getattr(call, "__module__", None) == auth.__name__:
        qualname = getattr(call, "__qualname__", "")
        if qualname == "require_permission.<locals>.check_permission":
            cells = _closure(call)
            return {"gate": "require_permission", "feature": cells["feature"],
                    "action": cells["action"]}
        if qualname == "require_any_permission.<locals>.check_any":
            cells = _closure(call)
            return {"gate": "require_any_permission", "features": list(cells["features"]),
                    "action": cells["action"]}
        raise ContractError(
            f"{qualname}: a dependency from web.routes.auth the contract does not "
            f"describe. Teach this module and the pilot's gate what it checks, "
            f"then regenerate."
        )
    return {"opaque": _name(call)}


def _dependencies(dependant: Any) -> List[Dict[str, Any]]:
    """In the order `solve_dependencies` calls them: a dependency's own
    dependencies first, and a cached one once."""
    out: List[Dict[str, Any]] = []
    seen = set()

    def walk(node: Any) -> None:
        for sub in node.dependencies:
            walk(sub)
            if sub.use_cache:
                if id(sub.call) in seen:
                    continue
                seen.add(id(sub.call))
            out.append(_dependency(sub.call))

    walk(dependant)
    return out


def _rate_limits(endpoint: Any) -> List[Dict[str, Any]]:
    from web.ratelimit import client_key, limiter

    name = f"{endpoint.__module__}.{endpoint.__name__}"
    if name in limiter._dynamic_route_limits:
        raise ContractError(f"{name}: a dynamic limit, which the pilot has no form for")
    out = []
    for lim in limiter._route_limits.get(name, []):
        unusual = {
            "key_func": lim.key_func is not client_key,
            # slowapi scopes a limit by `lim.scope or endpoint`.
            "scope": bool(lim.scope),
            "per_method": bool(lim.per_method),
            "methods": lim.methods is not None,
            "error_message": lim.error_message is not None,
            "exempt_when": lim.exempt_when is not None,
            "cost": lim.cost != 1,
        }
        if any(unusual.values()):
            raise ContractError(
                f"{name}: a limit with {[k for k, v in unusual.items() if v]}, "
                f"which the pilot does not implement"
            )
        out.append({"limit": str(lim.limit), "amount": lim.limit.amount,
                    "expiry_s": lim.limit.get_expiry(), "scope": name})
    return out


def _api_route(path: str, pattern: str, methods, dependant, body_field, endpoint) -> Dict[str, Any]:
    return {
        "kind": "api",
        "path": path,
        "pattern": pattern,
        "methods": sorted(methods),
        "endpoint": f"{endpoint.__module__}.{endpoint.__name__}",
        # FastAPI reads a body before it runs a single dependency.
        "body": body_field is not None,
        "dependencies": _dependencies(dependant),
        "rate_limits": _rate_limits(endpoint),
    }


def _starlette_route(route: Any) -> Dict[str, Any]:
    from starlette.routing import Mount, Route, WebSocketRoute

    if isinstance(route, Mount):
        return {"kind": "mount", "path": route.path, "pattern": route.path_regex.pattern}
    if isinstance(route, WebSocketRoute):
        return {"kind": "websocket", "path": route.path}
    if isinstance(route, Route):
        return {"kind": "route", "path": route.path, "pattern": route.path_regex.pattern,
                "methods": sorted(route.methods or [])}
    raise ContractError(f"{type(route).__name__} at {getattr(route, 'path', '?')}: no form for it")


def _routes(app: Any) -> List[Dict[str, Any]]:
    from fastapi import routing as fastapi_routing

    if app.router._low_priority_routes:
        raise ContractError("the app has low-priority routes, which the pilot does not order")
    out: List[Dict[str, Any]] = []
    for entry in app.router.routes:
        if isinstance(entry, fastapi_routing._IncludedRouter):
            if entry.effective_low_priority_routes():
                raise ContractError(f"{entry.include_context.prefix!r}: low-priority routes")
            for ctx in entry.effective_route_contexts():
                route = ctx.original_route
                if isinstance(route, fastapi_routing.APIRoute):
                    out.append(_api_route(ctx.path, ctx.path_regex.pattern, ctx.methods,
                                          ctx.dependant, ctx.body_field, route.endpoint))
                elif isinstance(route, fastapi_routing.APIWebSocketRoute):
                    out.append({"kind": "websocket", "path": ctx.starlette_route.path})
                else:
                    out.append(_starlette_route(ctx.starlette_route))
        elif isinstance(entry, fastapi_routing.APIRoute):
            out.append(_api_route(entry.path, entry.path_regex.pattern, entry.methods,
                                  entry.dependant, entry.body_field, entry.endpoint))
        elif isinstance(entry, fastapi_routing.APIWebSocketRoute):
            out.append({"kind": "websocket", "path": entry.path})
        else:
            out.append(_starlette_route(entry))
    return out


def _session() -> Dict[str, Any]:
    from web.routes import auth

    serializer = auth.session_serializer
    signer = serializer.make_signer()
    payload = serializer.serializer
    shape = {
        "serializer": type(serializer).__name__,
        "payload": f"{payload.__module__}.{getattr(payload, '__name__', type(payload).__name__)}",
        "signer": type(signer).__name__,
        "fallback_signers": len(serializer.fallback_signers),
        "secret_keys": len(serializer.secret_keys),
    }
    if shape != {"serializer": "URLSafeTimedSerializer", "payload": "itsdangerous._json._CompactJSON",
                 "signer": "TimestampSigner", "fallback_signers": 0, "secret_keys": 1}:
        raise ContractError(f"the session serializer changed shape: {shape}")
    return {
        "cookie": auth.SESSION_COOKIE,
        "max_age_s": auth.SESSION_MAX_AGE,
        "salt": serializer.salt.decode("utf-8"),
        "separator": signer.sep.decode("utf-8"),
        "key_derivation": signer.key_derivation,
        "key_digest": signer.digest_method().name,
        "mac_digest": signer.algorithm.digest_method().name,
    }


def _permissions() -> Dict[str, Any]:
    from core import permissions as p

    return {
        "features": [f.value for f in p.Feature],
        "tab_features": list(p.TAB_FEATURE_KEYS),
        "role_permissions": {
            str(role.value): {str(feature.value): sorted(a.value for a in actions)
                              for feature, actions in matrix.items()}
            for role, matrix in p.ROLE_PERMISSIONS.items()
        },
        "default_tabs": {role: (list(tabs) if tabs is not None else None)
                         for role, tabs in p.DEFAULT_TABS.items()},
        "hardcoded_admins_sha256": hashlib.sha256(
            ",".join(str(i) for i in sorted(p.ADMIN_USER_IDS)).encode()
        ).hexdigest(),
    }


def build() -> Dict[str, Any]:
    """The contract, from the app as it is now."""
    from core.pg import REQUIRED_REVISION
    from web.main import app
    from web.routes.auth import PUBLIC_API_PATHS

    return {
        "about": "Generated by tests/rust_access_contract.py from web.main.app; "
                 "regenerate, never edit.",
        "session": _session(),
        "required_revision": REQUIRED_REVISION,
        "public_api_paths": sorted(PUBLIC_API_PATHS),
        "redirect_slashes": app.router.redirect_slashes,
        **_permissions(),
        "routes": _routes(app),
    }


def render(contract: Dict[str, Any]) -> str:
    """One route a line, so a change to one route is a one-line diff."""
    head = {k: v for k, v in contract.items() if k != "routes"}
    text = json.dumps(head, indent=2, ensure_ascii=False)[:-2]
    lines = [json.dumps(r, ensure_ascii=False) for r in contract["routes"]]
    return text + ',\n  "routes": [\n    ' + ",\n    ".join(lines) + "\n  ]\n}\n"


def main() -> None:
    # The values only have to exist for `web.main` to import; none of them is
    # written into the contract.
    os.environ.setdefault("DASHBOARD_SECRET_KEY", "contract-export-not-a-secret")
    os.environ.setdefault("BOT_TOKEN", "1:contract-export-not-a-secret")
    CONTRACT_PATH.parent.mkdir(parents=True, exist_ok=True)
    contract = build()
    CONTRACT_PATH.write_text(render(contract), encoding="utf-8")
    print(f"{CONTRACT_PATH}: {len(contract['routes'])} routes")


if __name__ == "__main__":
    main()
