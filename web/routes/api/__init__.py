"""
API routes split by domain.

Each sub-module defines its own APIRouter which is composed
into the top-level router exposed by this package.

Authentication: every sub-router requires a valid dashboard session
(``require_user``) EXCEPT the health router — ``/api/health`` is polled by
Docker, nginx and external uptime monitors without a session. Individual
sensitive endpoints inside the health router gate themselves.
"""
from fastapi import APIRouter, Depends

from web.routes.auth import require_user

from .health import router as health_router
from .admin import router as admin_router
from .analytics import router as analytics_router
from .customers import router as customers_router
from .goals import router as goals_router
from .inventory import router as inventory_router
from .expenses import router as expenses_router
from .traffic import router as traffic_router
from .users import router as users_router
from .reports import router as reports_router
from .products_intel import router as products_intel_router
from .margin import router as margin_router

router = APIRouter(tags=["api"])

# Public: health check (no session) — sensitive sub-paths gate themselves.
router.include_router(health_router)

# Everything else requires an authenticated, approved dashboard user.
_auth = [Depends(require_user)]
router.include_router(admin_router, dependencies=_auth)
router.include_router(analytics_router, dependencies=_auth)
router.include_router(customers_router, dependencies=_auth)
router.include_router(goals_router, dependencies=_auth)
router.include_router(inventory_router, dependencies=_auth)
router.include_router(expenses_router, dependencies=_auth)
router.include_router(traffic_router, dependencies=_auth)
router.include_router(users_router, dependencies=_auth)
router.include_router(reports_router, dependencies=_auth)
router.include_router(products_intel_router, dependencies=_auth)
router.include_router(margin_router, dependencies=_auth)
