"""
Page routes for serving HTML templates.
"""
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from web.config import TEMPLATES_DIR, VERSION
from web.routes.auth import get_current_user, require_auth

router = APIRouter(tags=["pages"])
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Serve the main dashboard page (protected)."""
    # Check authentication
    redirect = require_auth(request)
    if redirect:
        return redirect

    # Get current user for display
    user = get_current_user(request)

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "version": VERSION,
        "user": user
    })
