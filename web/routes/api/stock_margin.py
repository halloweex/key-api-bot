"""The margin of everything in stock — for admins only.

The block at the bottom of /margin (`StockMarginSection`). It names every
offer's purchase price, so the owner asked for it to be shown to admins and
nobody else (2026-11-26). It is gated on the admin **role** at its include in
`web/routes/api/__init__.py`, not on the `margin` permission: that one is a
tab an admin can tick for somebody who is not an admin, and ticking the tab
must not hand over the purchase price of every offer.

It is a router of its own rather than a second gate on `/margin/*`, whose
docstring says why: two gates on one route — one "admin", one "whoever holds
the tab" — is a rule with two homes. Here the path is distinct and the one
gate is the admin role.

A read the database refuses is a 503 naming `margin`, through the same
`_margin_run` every margin query uses.
"""
import logging

from fastapi import APIRouter, Request

from web.services import margin_service
from ._deps import limiter

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/stock-margin")
@limiter.limit("30/minute")
async def get_stock_margin(request: Request):
    """Every offer in stock: price, cost, margin, the last 90 days, and what to check."""
    return await margin_service.get_stock_margin()
