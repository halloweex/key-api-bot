"""
API routes split by domain.

Each sub-module defines its own APIRouter which is composed
into the top-level router exposed by this package.
"""
from fastapi import APIRouter

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

router = APIRouter(tags=["api"])

router.include_router(health_router)
router.include_router(admin_router)
router.include_router(analytics_router)
router.include_router(customers_router)
router.include_router(goals_router)
router.include_router(inventory_router)
router.include_router(expenses_router)
router.include_router(traffic_router)
router.include_router(users_router)
router.include_router(reports_router)
router.include_router(products_intel_router)
