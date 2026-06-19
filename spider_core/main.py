import logging
import os

import db_store
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from app_config import APP_VERSION, AUTH_HEADER, PUBLIC_API_PATHS, is_api_authorized, is_auth_required
from schemas import *  # noqa: F403
from utils import *  # noqa: F403

from spider_engine import DATA_DIR

class _QuietPollFilter(logging.Filter):
    """Suppress access-log noise from high-frequency polling endpoints (200 OK)."""

    _POLLING_SIGS = (
        "GET /api/tasks HTTP/",
        "GET /api/tasks/queue_status HTTP/",
        "GET /api/status HTTP/",
    )

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            msg = record.getMessage()
            if "\" 200" not in msg:
                return True
            for sig in self._POLLING_SIGS:
                if sig in msg:
                    return False
        except Exception:
            pass
        return True

@asynccontextmanager
async def _lifespan(app):
    logging.getLogger("uvicorn.access").addFilter(_QuietPollFilter())
    yield

app = FastAPI(lifespan=_lifespan)


@app.exception_handler(HTTPException)
async def custom_http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={"code": exc.status_code, "msg": exc.detail},
    )

# 挂载前端静态资源（拆分后的 css/ 与 js/ 目录）。
# 这些路径不以 /api/ 开头，因此与 "/"、"/favicon.png" 一样无需访问令牌。
_FRONTEND_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "frontend")
for _sub in ("css", "js"):
    _static_dir = os.path.join(_FRONTEND_DIR, _sub)
    if os.path.isdir(_static_dir):
        app.mount(f"/{_sub}", StaticFiles(directory=_static_dir), name=_sub)

db_store.configure(DATA_DIR)
db_store.import_existing_csvs(DATA_DIR)
db_store.recover_interrupted_tasks()

@app.middleware("http")
async def require_api_token(request: Request, call_next):
    path = request.url.path
    if path.startswith("/api/") and path not in PUBLIC_API_PATHS:
        if not is_api_authorized(request.headers.get(AUTH_HEADER)):
            return JSONResponse(
                status_code=401,
                content={"code": 401, "msg": "访问令牌缺失或无效"},
            )
    return await call_next(request)

@app.get("/")
def read_root():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    html_path = os.path.join(base_dir, "frontend", "index.html")
    if os.path.exists(html_path):
        with open(html_path, "r", encoding="utf-8") as f:
            return HTMLResponse(f.read())
    logging.error("Frontend page not found: %s", html_path)
    return HTMLResponse("<h1>Frontend page not found.</h1>", status_code=404)

@app.get("/favicon.png")
def get_favicon():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(base_dir, "frontend", "favicon.png")
    if os.path.exists(file_path):
        return FileResponse(file_path, media_type="image/png")
    return JSONResponse(status_code=404, content={"error": "Favicon file not found."})

# Router registration. Routers depend on schemas/services, not main.
from routers import tasks as _tasks_router      # noqa: E402
from routers import movies as _movies_router    # noqa: E402
from routers import magnets as _magnets_router  # noqa: E402
from routers import rankings as _rankings_router  # noqa: E402
from routers import settings as _settings_router  # noqa: E402
from routers import storage as _storage_router  # noqa: E402

app.include_router(_tasks_router.router)
app.include_router(_movies_router.router)
app.include_router(_magnets_router.router)
app.include_router(_rankings_router.router)
app.include_router(_settings_router.router)
app.include_router(_storage_router.router)


def start_server(host="127.0.0.1"):
    import uvicorn

    uvicorn.run(app, host=host, port=8000, access_log=False, log_level="info")

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000, access_log=False, log_level="info")
