import json
import logging
import os
import queue
import secrets
import threading
import uuid
from urllib.parse import parse_qs, quote, urlencode, urlparse, urlunparse

import db_store
import magnet_checker
from contextlib import asynccontextmanager

from bs4 import BeautifulSoup
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from spider_engine import DATA_DIR, STATUS_FILE, fetch_html, get_android_javdb_cookie, run_task
from storage_utils import (
    UnsafeFilenameError,
    atomic_write_json,
    get_safe_csv_path,
    make_csv_filename_from_label,
    normalize_csv_filename,
    read_json_file,
)


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

# 挂载前端静态资源（拆分后的 css/ 与 js/ 目录）。
# 这些路径不以 /api/ 开头，因此与 "/"、"/favicon.png" 一样无需访问令牌。
_FRONTEND_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "frontend")
for _sub in ("css", "js"):
    _static_dir = os.path.join(_FRONTEND_DIR, _sub)
    if os.path.isdir(_static_dir):
        app.mount(f"/{_sub}", StaticFiles(directory=_static_dir), name=_sub)


APP_VERSION = os.getenv("JAVDB_SPIDER_VERSION", "1.8.2")
AUTH_HEADER = "X-JavDB-Token"
PUBLIC_API_PATHS = {"/api/version"}
QUEUE_LOCK = threading.RLock()
QUEUE_THREAD = None

db_store.configure(DATA_DIR)
db_store.import_existing_csvs(DATA_DIR)
db_store.recover_interrupted_tasks()


class TaskConfig(BaseModel):
    start_url: str
    cookie: str = ""
    user_agent: str = ""
    filename: str = ""
    proxies: str = None
    crawl_mode: str = ""
    remember_cookie: bool = False


class RuntimeConfig(BaseModel):
    cookie: str = ""
    remember_cookie: bool = False
    user_agent: str = ""
    proxies: str = ""
    trackers: list[str] = []


class CookieConfig(BaseModel):
    cookie: str = ""


class ModeConfig(BaseModel):
    mode: str


class TagConfigRequest(BaseModel):
    url: str
    cookie: str = ""
    user_agent: str = ""
    proxies: str = None


class DeleteRequest(BaseModel):
    filenames: list[str]


class SelectMagnetRequest(BaseModel):
    magnet_id: int


def _env_truthy(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def is_auth_required() -> bool:
    return _env_truthy("JAVDB_AUTH_REQUIRED") or bool(os.getenv("JAVDB_AUTH_TOKEN", "").strip())


def is_api_authorized(provided_token: str | None) -> bool:
    expected_token = os.getenv("JAVDB_AUTH_TOKEN", "").strip()
    if not is_auth_required():
        return True
    if not expected_token or not provided_token:
        return False
    return secrets.compare_digest(provided_token, expected_token)


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


def get_safe_name(filename: str) -> str | None:
    try:
        _, safe_name = get_safe_csv_path(DATA_DIR, filename)
        return safe_name
    except UnsafeFilenameError:
        return None


def parse_tag_filter(tags: str = None):
    if not tags:
        return []
    values = [tag.strip() for tag in tags.split(",")]
    return [tag for tag in values if tag and tag.lower() != "all"]


def ensure_zh_locale(url: str) -> str:
    parsed = urlparse(url)
    params = parse_qs(parsed.query, keep_blank_values=True)
    params["locale"] = ["zh"]
    query = urlencode(params, doseq=True)
    return urlunparse(parsed._replace(query=query))


def build_proxy_dict(proxy):
    return {"http": proxy, "https": proxy} if proxy else None


def save_runtime_from_payload(config):
    current = db_store.get_runtime_config(include_cookie=True)
    cookie = getattr(config, "cookie", "")
    if not cookie:
        cookie = current.get("cookie", "")
    user_agent = getattr(config, "user_agent", "") or current.get("user_agent", "")
    proxies = getattr(config, "proxies", None)
    if proxies is None:
        proxies = current.get("proxies", "")
    remember_cookie = bool(getattr(config, "remember_cookie", current.get("remember_cookie", False)))
    db_store.save_runtime_config(
        cookie=cookie,
        remember_cookie=remember_cookie,
        user_agent=user_agent,
        proxies=proxies or "",
    )


def get_runtime_for_request():
    runtime = db_store.get_runtime_config(include_cookie=True)
    if not runtime.get("cookie"):
        android_cookie = get_android_javdb_cookie().strip()
        if android_cookie:
            db_store.save_runtime_config(
                cookie=android_cookie,
                remember_cookie=runtime["remember_cookie"],
                user_agent=runtime["user_agent"],
                proxies=runtime["proxies"],
            )
            runtime = db_store.get_runtime_config(include_cookie=True)
    return runtime


def runtime_headers(runtime):
    return {
        "User-Agent": runtime.get("user_agent") or "",
        "Cookie": runtime.get("cookie") or "",
    }


def infer_task_filename(start_url, requested_filename, soup):
    if requested_filename:
        return normalize_csv_filename(requested_filename, allow_empty=True)
    actor_name = ""
    if "/actors/" in start_url:
        actor_tag = soup.select_one(".actor-section-name")
        if actor_tag:
            actor_name = actor_tag.text.strip()
    if actor_name:
        return make_csv_filename_from_label(actor_name)
    import time

    return make_csv_filename_from_label(f"javdb_{time.strftime('%Y%m%d_%H%M%S')}")


def prepare_task_config(config: TaskConfig):
    if config.crawl_mode and config.crawl_mode not in {"incremental", "overwrite"}:
        return JSONResponse(status_code=400, content={"code": 400, "msg": "爬取模式非法"})
    try:
        requested_filename = normalize_csv_filename(config.filename, allow_empty=True)
    except UnsafeFilenameError as e:
        return JSONResponse(status_code=400, content={"code": 400, "msg": f"文件名非法: {str(e)}"})

    save_runtime_from_payload(config)
    runtime = get_runtime_for_request()
    if not runtime.get("cookie"):
        return JSONResponse(
            status_code=400,
            content={"code": 400, "msg": "Cookie 不能为空；Android 端请先在内置浏览器登录 JavDB"},
        )

    start_url = ensure_zh_locale(config.start_url)
    try:
        response = fetch_html(
            start_url,
            headers=runtime_headers(runtime),
            proxies=build_proxy_dict(runtime.get("proxies")),
        )
    except Exception as e:
        # 网络层异常（TLS 握手失败、超时、连接被拒、代理不可用等）。
        # 收口为友好响应，避免异常冒泡到 ASGI 层打出整页 traceback。
        return JSONResponse(
            status_code=502,
            content={"code": 502, "msg": f"入队预检查请求失败：{str(e)}"},
        )
    if response.status_code != 200:
        return JSONResponse(
            status_code=response.status_code if response.status_code >= 400 else 400,
            content={"code": response.status_code, "msg": f"入队预检查失败，状态码: {response.status_code}"},
        )

    soup = BeautifulSoup(response.text, "html.parser")
    try:
        final_filename = infer_task_filename(start_url, requested_filename, soup)
    except UnsafeFilenameError as e:
        return JSONResponse(status_code=400, content={"code": 400, "msg": f"文件名非法: {str(e)}"})

    exists = db_store.collection_exists(final_filename)
    if exists and not config.crawl_mode:
        return JSONResponse(
            status_code=409,
            content={
                "code": 409,
                "msg": f"发现已有数据库集合：{final_filename}，请选择增量或覆盖。",
                "needs_mode": True,
                "filename": final_filename,
            },
        )
    return {
        "start_url": start_url,
        "filename": final_filename,
        "crawl_mode": config.crawl_mode or "",
    }


def task_to_response(task, include_logs=False):
    if not task:
        return None
    incremental_movie_codes = task_incremental_movie_codes(task)
    data = {
        "task_id": task["task_id"],
        "start_url": task["start_url"],
        "filename": task.get("final_filename") or task.get("requested_filename") or "",
        "final_filename": task.get("final_filename") or task.get("requested_filename") or "",
        "collection_filename": task.get("collection_filename") or "",
        "crawl_mode": task.get("crawl_mode") or "",
        "state": task["state"],
        "progress": task.get("progress") or "0/0",
        "current": task.get("current") or "-",
        "added_count": task.get("added_count") or 0,
        "error_message": task.get("error_message") or "",
        "created_at": task.get("created_at") or 0,
        "updated_at": task.get("updated_at") or 0,
        "started_at": task.get("started_at") or 0,
        "finished_at": task.get("finished_at") or 0,
        "incremental_movie_count": len(incremental_movie_codes),
        "can_copy_incremental_magnets": (
            task.get("state") == "finished"
            and task.get("crawl_mode") == "incremental"
            and bool(incremental_movie_codes)
        ),
    }
    if include_logs:
        data["logs"] = db_store.get_task_logs(task["task_id"])
    return data


def task_incremental_movie_codes(task):
    try:
        checkpoint = json.loads(task.get("checkpoint_json") or "{}")
    except (TypeError, json.JSONDecodeError):
        return []
    codes = checkpoint.get("incremental_movie_codes", [])
    if not isinstance(codes, list):
        return []
    normalized = []
    seen = set()
    for code in codes:
        value = str(code or "").strip()
        if value and value not in seen:
            normalized.append(value)
            seen.add(value)
    return normalized


def write_status_mirror(task=None):
    if not task:
        empty_status = {
            "state": "idle",
            "progress": "0/0",
            "current": "-",
            "logs": ["等待任务启动..."],
        }
        atomic_write_json(STATUS_FILE, empty_status, indent=2)
        return
    data = task_to_response(task, include_logs=True)
    atomic_write_json(STATUS_FILE, data, indent=2)


def queue_worker():
    global QUEUE_THREAD
    try:
        while True:
            task = db_store.claim_next_pending_task()
            if not task:
                write_status_mirror(db_store.get_current_task())
                return
            write_status_mirror(task)
            try:
                run_task(task["task_id"])
            except Exception as e:
                db_store.update_task_status(
                    task["task_id"],
                    state="failed",
                    current="任务异常",
                    log_msg=f"任务执行异常: {str(e)}",
                    error_message=str(e),
                )
            current = db_store.get_task(task["task_id"])
            write_status_mirror(current)
            if current and current["state"] in {"paused", "waiting_cookie", "waiting_choice", "failed"}:
                return
    finally:
        with QUEUE_LOCK:
            QUEUE_THREAD = None


def ensure_queue_worker():
    global QUEUE_THREAD
    with QUEUE_LOCK:
        if QUEUE_THREAD and QUEUE_THREAD.is_alive():
            return
        QUEUE_THREAD = threading.Thread(target=queue_worker, daemon=True)
        QUEUE_THREAD.start()


def is_queue_running():
    return bool(QUEUE_THREAD and QUEUE_THREAD.is_alive())


def get_queue_status_data():
    tasks = db_store.list_tasks(limit=200)
    pending_count = sum(1 for task in tasks if task["state"] == "pending")
    counts = db_store.count_tasks_by_state()
    finished_count = sum(counts.get(state, 0) for state in db_store.FINISHED_TASK_STATES)
    active_count = sum(count for state, count in counts.items() if state not in db_store.FINISHED_TASK_STATES)
    current = db_store.get_active_task()
    if not current:
        for task in tasks:
            if task["state"] in {"waiting_cookie", "waiting_choice", "paused"}:
                current = task
                break
    blocking = bool(
        current
        and current["state"] in {
            "running",
            "pause_requested",
            "cancel_requested",
            "waiting_cookie",
            "waiting_choice",
            "paused",
        }
    )
    return {
        "queue_state": "running" if is_queue_running() else ("blocked" if blocking else "idle"),
        "pending_count": pending_count,
        "active_count": active_count,
        "finished_count": finished_count,
        "current_task_id": current["task_id"] if current else "",
        "can_start": pending_count > 0 and not is_queue_running() and not blocking,
    }


def resolve_task_cookie(cookie):
    cookie = (cookie or "").strip()
    if cookie:
        return cookie
    android_cookie = get_android_javdb_cookie().strip()
    if android_cookie:
        return android_cookie
    return ""


def create_task_from_config(config: TaskConfig):
    prepared = prepare_task_config(config)
    if isinstance(prepared, JSONResponse):
        return prepared
    task_id = db_store.create_task(
        prepared["start_url"],
        filename=prepared["filename"],
        crawl_mode=prepared["crawl_mode"],
    )
    return {"code": 200, "msg": "任务已加入队列", "task_id": task_id, "filename": prepared["filename"]}


@app.get("/")
def read_root():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    html_path = os.path.join(base_dir, "frontend", "index.html")
    if os.path.exists(html_path):
        with open(html_path, "r", encoding="utf-8") as f:
            return HTMLResponse(f.read())
    return HTMLResponse(f"<h1>找不到前端页面，系统当前寻找的绝对路径是: {html_path}</h1>")


@app.get("/favicon.png")
def get_favicon():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(base_dir, "frontend", "favicon.png")
    if os.path.exists(file_path):
        return FileResponse(file_path, media_type="image/png")
    return {"error": f"找不到图标文件: {file_path}"}


# ---------------------------------------------------------------------------
# 路由注册：业务端点已按职责拆分到 routers/ 包，磁力检测编排在 services/magnet_service。
# 必须在上方所有 helper / 模型 / 中间件定义之后导入：各 router 在导入时
# `from main import ...` 才能解析到这些符号。
# ---------------------------------------------------------------------------
from routers import tasks as _tasks_router      # noqa: E402
from routers import movies as _movies_router    # noqa: E402
from routers import magnets as _magnets_router  # noqa: E402
from routers import settings as _settings_router  # noqa: E402
from routers import storage as _storage_router  # noqa: E402

app.include_router(_tasks_router.router)
app.include_router(_movies_router.router)
app.include_router(_magnets_router.router)
app.include_router(_settings_router.router)
app.include_router(_storage_router.router)

# 兼容：既有测试与调用方以 main.<端点函数> 直接调用端点（绕过 HTTP）。
# 端点已迁至 routers，这里把它们重新导出到 main 命名空间，保持 main.xxx() 可用（零回归）。
from routers.tasks import (  # noqa: E402,F401
    cancel_task,
    cleanup_finished_tasks,
    create_task,
    delete_task,
    get_queue_status,
    get_task_detail,
    get_task_incremental_magnets,
    list_tasks,
    pause_task,
    refresh_task_cookie,
    resume_task_by_id,
    set_task_mode,
    start_queue,
    update_task_cookie,
)
from routers.movies import (  # noqa: E402,F401
    auto_select_magnets,
    create_collection_incremental_task,
    get_collection_movies,
    get_history,
    get_movie_magnets,
    select_movie_magnet,
)
from routers.magnets import (  # noqa: E402,F401
    cancel_magnet_check_job,
    check_all_magnets,
    check_collection_magnets,
    check_movie_magnets,
    get_current_magnet_check_job_route,
    get_magnet_check_job,
)
from routers.settings import (  # noqa: E402,F401
    clear_logs,
    get_runtime_config,
    get_status,
    get_tags,
    get_version,
    set_runtime_config,
)
from routers.storage import delete_history, download_csv, get_magnets  # noqa: E402,F401


def start_server(host="127.0.0.1"):
    import uvicorn

    uvicorn.run(app, host=host, port=8000, access_log=False, log_level="info")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000, access_log=False, log_level="info")
