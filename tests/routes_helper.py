"""Finding a route on the app, across two FastAPI generations.

`app.routes` used to be flat: every router included into the application
appeared there with its full path, and five test modules walked that list to
assert an endpoint exists and carries `Depends(require_admin)`.

**It is not flat any more.** On the versions production actually runs —
starlette 1.6.0, fastapi 0.141.1 — an `include_router` leaves an
`_IncludedRouter` object in `app.routes` and the endpoints live inside it.
Measured on the production image: 13 top-level entries, of which 5 are
`_IncludedRouter`, and `/api/managers` appears at the top level **zero** times.

The routes serve perfectly well; it is the introspection that broke. So for
some time those authorization tests were passing on a developer's older
fastapi while being structurally unable to check anything on the version the
dashboard is served by. That is the failure mode this helper exists to end,
and it was found by CI on its second run.

Written as a generic walk over anything exposing `.routes` rather than against
`_IncludedRouter` by name: it is private API, this is the second time its shape
has changed, and a walk that follows the attribute keeps working through the
third.
"""
from __future__ import annotations

from typing import Any, Iterator, Optional


def iter_routes(app: Any) -> Iterator[Any]:
    """Every endpoint on the app, however deeply it is nested."""
    seen: set[int] = set()

    def walk(routes) -> Iterator[Any]:
        for route in routes or ():
            if id(route) in seen:
                continue
            seen.add(id(route))
            if getattr(route, "path", None) is not None:
                yield route
            # `Mount` and `_IncludedRouter` both carry their children here;
            # `Mount` additionally hides them one level down on `.app`.
            yield from walk(getattr(route, "routes", None))
            inner = getattr(route, "app", None)
            if inner is not None and inner is not app:
                yield from walk(getattr(inner, "routes", None))

    yield from walk(getattr(app, "routes", None))


def find_route(app: Any, path: str, method: Optional[str] = None) -> Optional[Any]:
    """The route registered at `path`, or None.

    Matches on the path as declared, so a parameterised route is looked up by
    its template — `/api/managers/{manager_id}/retail-status`, not a filled-in
    id.
    """
    for route in iter_routes(app):
        if getattr(route, "path", None) != path:
            continue
        if method is None:
            return route
        if method in (getattr(route, "methods", None) or set()):
            return route
    return None


def route_dependencies(route: Any) -> list:
    """The dependency callables attached to a route, flattened.

    FastAPI keeps them on `route.dependant.dependencies`, each of which has its
    own `call`. Sub-dependencies are followed, because `require_admin` may be
    attached at the `include_router` level and appear one level in.
    """
    out: list = []

    def walk(dependant) -> None:
        for dep in getattr(dependant, "dependencies", ()) or ():
            call = getattr(dep, "call", None)
            if call is not None:
                out.append(call)
            walk(dep)

    walk(getattr(route, "dependant", None))
    return out
