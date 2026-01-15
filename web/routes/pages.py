"""
Page routes for serving HTML templates.
"""
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.templating import Jinja2Templates

from web.config import TEMPLATES_DIR, STATIC_V2_DIR, VERSION
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


@router.get("/v2", response_class=HTMLResponse)
async def dashboard_v2(request: Request):
    """Serve the React-based dashboard (v2) for safe testing."""
    # Check authentication
    redirect = require_auth(request)
    if redirect:
        return redirect

    # Serve the React build index.html
    index_path = STATIC_V2_DIR / "index.html"
    if index_path.exists():
        return FileResponse(index_path)

    # Fallback message if build doesn't exist
    return HTMLResponse(
        content="""
        <html>
        <head><title>KoreanStory Analytics v2</title></head>
        <body style="background: #1e293b; color: #94a3b8; font-family: sans-serif; padding: 40px;">
            <h1 style="color: white;">React Dashboard (v2) Not Built Yet</h1>
            <p>Run the following commands to build:</p>
            <pre style="background: #0f172a; padding: 20px; border-radius: 8px;">
cd web/frontend
npm run build
            </pre>
            <p>Or use the dev server:</p>
            <pre style="background: #0f172a; padding: 20px; border-radius: 8px;">
cd web/frontend
npm run dev
# Then visit http://localhost:5173
            </pre>
        </body>
        </html>
        """,
        status_code=200
    )
