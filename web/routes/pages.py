"""
Page routes for serving HTML templates and React SPA.
"""
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from web.config import TEMPLATES_DIR, STATIC_V2_DIR, VERSION
from web.routes.auth import get_current_user, require_auth

router = APIRouter(tags=["pages"])
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


# ─── V1 Dashboard (Jinja2 Templates) ──────────────────────────────────────────

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


# ─── V2 Dashboard (React SPA) ─────────────────────────────────────────────────

def _serve_react_app() -> HTMLResponse:
    """Serve the React SPA index.html."""
    index_path = STATIC_V2_DIR / "index.html"

    if index_path.exists():
        return HTMLResponse(
            content=index_path.read_text(),
            media_type="text/html"
        )

    # Fallback message if build doesn't exist
    return HTMLResponse(
        content="""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>KoreanStory Analytics v2</title>
            <style>
                body {
                    background: #1e293b;
                    color: #94a3b8;
                    font-family: system-ui, -apple-system, sans-serif;
                    padding: 40px;
                    max-width: 600px;
                    margin: 0 auto;
                }
                h1 { color: white; }
                pre {
                    background: #0f172a;
                    padding: 20px;
                    border-radius: 8px;
                    overflow-x: auto;
                }
                code { color: #38bdf8; }
            </style>
        </head>
        <body>
            <h1>React Dashboard (v2) Not Built</h1>
            <p>The frontend build was not found. Run these commands to build:</p>
            <pre><code>cd web/frontend
npm install
npm run build</code></pre>
            <p>Or use the dev server for local development:</p>
            <pre><code>cd web/frontend
npm run dev
# Visit http://localhost:5173</code></pre>
        </body>
        </html>
        """,
        status_code=200
    )


@router.get("/v2", response_class=HTMLResponse)
async def dashboard_v2(request: Request):
    """Serve the React-based dashboard (v2)."""
    # Check authentication
    redirect = require_auth(request)
    if redirect:
        return redirect

    return _serve_react_app()


@router.get("/v2/{path:path}", response_class=HTMLResponse)
async def dashboard_v2_spa(request: Request, path: str):
    """
    Handle all /v2/* routes for React SPA client-side routing.
    This ensures deep links and browser refresh work correctly.
    """
    # Check authentication
    redirect = require_auth(request)
    if redirect:
        return redirect

    # Check if path is a static asset (js, css, etc.)
    # Static assets are served by the mounted static files handler
    static_extensions = {'.js', '.css', '.json', '.ico', '.svg', '.png', '.jpg', '.woff', '.woff2'}
    if any(path.endswith(ext) for ext in static_extensions):
        # Let the static file handler deal with this
        # This shouldn't normally be reached as static files are mounted separately
        asset_path = STATIC_V2_DIR / path
        if asset_path.exists():
            return HTMLResponse(
                content=asset_path.read_bytes(),
                media_type=_get_media_type(path)
            )
        return HTMLResponse(content="Not found", status_code=404)

    # For all other routes, serve the SPA index.html
    # React Router will handle the routing client-side
    return _serve_react_app()


def _get_media_type(path: str) -> str:
    """Get MIME type based on file extension."""
    ext = Path(path).suffix.lower()
    media_types = {
        '.js': 'application/javascript',
        '.css': 'text/css',
        '.json': 'application/json',
        '.ico': 'image/x-icon',
        '.svg': 'image/svg+xml',
        '.png': 'image/png',
        '.jpg': 'image/jpeg',
        '.woff': 'font/woff',
        '.woff2': 'font/woff2',
    }
    return media_types.get(ext, 'application/octet-stream')
