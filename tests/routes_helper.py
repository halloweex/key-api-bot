"""Finding a route on the app, across two FastAPI generations.

`app.routes` used to be flat: every router included into the application
appeared there with its full path and its inherited dependencies already merged
in. Five test modules walked that list to assert an endpoint exists and carries
`Depends(require_admin)`.

**Neither of those things is true any more.** On the versions production runs —
starlette 1.6.0, fastapi 0.141.1 — `include_router` leaves a
`fastapi.routing._IncludedRouter` in `app.routes`, and it holds:

    include_context.included_router   the APIRouter, whose routes carry
                                      UN-PREFIXED paths (`/login`, not
                                      `/auth/login`)
    include_context.prefix            the prefix to put back
    include_context.dependencies      the dependencies applied AT INCLUDE TIME

That third one is the trap. `require_admin` is attached to the admin router by
`include_router(admin_router, dependencies=[Depends(require_admin)])`, and
`api_gate` by the `/api` include. Under the old flattening both ended up on
each route's own `dependant`; now they sit one level up, and a test reading
only `route.dependant` finds neither. It would report every admin endpoint as
unprotected — or, walking the old way, report that none of them exist.

The routes serve perfectly well. It is the introspection that broke, and for
some time those authorization tests were passing on a developer's older fastapi
while being structurally unable to check anything on the version the dashboard
is served by. That is the failure this module exists to end.

Measured on the production image before this was written: 13 top-level entries,
5 `_IncludedRouter`, 141 paths in the OpenAPI schema, and `/api/managers`
present at the top level of `app.routes` **zero** times.

Written to handle both shapes, because the repository is developed against one
and deployed on the other until every machine catches up.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterator, Optional, Tuple


@dataclass(frozen=True)
class Endpoint:
    """One registered endpoint, with every dependency that applies to it."""
    path: str
    methods: frozenset
    route: Any
    dependencies: Tuple[Any, ...]     # callables, own and inherited


def _depends_calls(dependencies) -> Tuple[Any, ...]:
    """Callables out of a list of `Depends(...)` markers."""
    out = []
    for dep in dependencies or ():
        call = getattr(dep, "dependency", None) or getattr(dep, "call", None)
        if call is not None:
            out.append(call)
    return tuple(out)


def _dependant_calls(dependant) -> Tuple[Any, ...]:
    """Every callable in a resolved dependency tree, sub-dependencies included."""
    out = []

    def walk(node) -> None:
        for dep in getattr(node, "dependencies", ()) or ():
            call = getattr(dep, "call", None)
            if call is not None:
                out.append(call)
            walk(dep)

    walk(dependant)
    return tuple(out)


def iter_endpoints(app: Any) -> Iterator[Endpoint]:
    """Every endpoint on the app, with its full path and inherited dependencies."""

    def walk(routes, prefix: str, inherited: Tuple[Any, ...]) -> Iterator[Endpoint]:
        for route in routes or ():
            context = getattr(route, "include_context", None)
            if context is not None:
                inner = getattr(context, "included_router", None)
                yield from walk(
                    getattr(inner, "routes", None),
                    prefix + (getattr(context, "prefix", "") or ""),
                    inherited + _depends_calls(getattr(context, "dependencies", ())),
                )
                continue

            dependant = getattr(route, "dependant", None)
            if dependant is not None and getattr(route, "path", None) is not None:
                yield Endpoint(
                    path=prefix + route.path,
                    methods=frozenset(getattr(route, "methods", None) or ()),
                    route=route,
                    dependencies=inherited + _dependant_calls(dependant),
                )
                continue

            # A Mount, or the old flat shape where a sub-router still exposed
            # `.routes`. Static mounts fall here and yield nothing, having no
            # dependant.
            nested = getattr(route, "routes", None)
            if nested:
                yield from walk(nested, prefix + (getattr(route, "path", "") or ""), inherited)

    yield from walk(getattr(app, "routes", None), "", ())


def find_endpoint(app: Any, path: str, method: Optional[str] = None) -> Optional[Endpoint]:
    """The endpoint registered at `path`, or None.

    Matched on the path as declared, so a parameterised route is looked up by
    its template — `/api/managers/{manager_id}/retail-status`.
    """
    for endpoint in iter_endpoints(app):
        if endpoint.path != path:
            continue
        if method is None or method in endpoint.methods:
            return endpoint
    return None


# ─── The shapes the existing tests were written against ──────────────────────

def find_route(app: Any, path: str, method: Optional[str] = None):
    """The raw route object, for tests that only need to know it is registered."""
    found = find_endpoint(app, path, method)
    return found.route if found else None


def route_dependencies(app: Any, path: str, method: Optional[str] = None) -> Tuple[Any, ...]:
    """Every dependency callable applying to an endpoint, inherited included."""
    found = find_endpoint(app, path, method)
    return found.dependencies if found else ()
