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


APP_VERSION = os.getenv("JAVDB_SPIDER_VERSION", "1.6.0")
AUTH_HEADER = "X-JavDB-Token"
PUBLIC_API_PATHS = {"/api/version"}
QUEUE_LOCK = threading.RLock()
QUEUE_THREAD = None
MAGNET_CHECK_LOCK = threading.RLock()
MAGNET_CHECK_JOBS = {}
ACTIVE_MAGNET_CHECK_JOB_ID = None

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


class ResumeConfig(BaseModel):
    cookie: str = ""


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


def public_magnet_check_job(job):
    with job["lock"]:
        return {
            "job_id": job["job_id"],
            "scope": job["scope"],
            "target": job["target"],
            "total": job["total"],
            "completed": job["completed"],
            "active": job["active"],
            "weak": job["weak"],
            "dead": job["dead"],
            "failed": job["failed"],
            "running": job["running"],
            "cancelled": job["cancelled"],
            "done": job["done"],
            "message": job.get("message", ""),
        }


def create_magnet_check_job(scope, target, magnets):
    global ACTIVE_MAGNET_CHECK_JOB_ID
    with MAGNET_CHECK_LOCK:
        if ACTIVE_MAGNET_CHECK_JOB_ID:
            active = MAGNET_CHECK_JOBS.get(ACTIVE_MAGNET_CHECK_JOB_ID)
            if active and public_magnet_check_job(active)["running"]:
                return None, active
        job_id = str(uuid.uuid4())
        job = {
            "job_id": job_id,
            "scope": scope,
            "target": target,
            "total": len(magnets),
            "completed": 0,
            "active": 0,
            "weak": 0,
            "dead": 0,
            "failed": 0,
            "running": True,
            "cancelled": False,
            "done": False,
            "message": "",
            "cancel_event": threading.Event(),
            "lock": threading.RLock(),
        }
        MAGNET_CHECK_JOBS[job_id] = job
        ACTIVE_MAGNET_CHECK_JOB_ID = job_id
    thread = threading.Thread(
        target=run_magnet_check_job,
        args=(job_id, list(magnets), db_store.get_runtime_config(include_cookie=False).get("trackers", [])),
        daemon=True,
    )
    thread.start()
    return job, None


def failed_magnet_rows(magnets):
    return [magnet for magnet in magnets if magnet.get("check_error") and not magnet.get("check_status")]


def run_magnet_check_job(job_id, magnets, user_trackers):
    global ACTIVE_MAGNET_CHECK_JOB_ID
    job = MAGNET_CHECK_JOBS[job_id]
    work_queue = queue.Queue()
    for magnet in magnets:
        work_queue.put(magnet)

    def worker():
        while not job["cancel_event"].is_set():
            try:
                magnet = work_queue.get_nowait()
            except queue.Empty:
                return
            try:
                result = magnet_checker.check_magnet(magnet.get("link", ""), user_trackers)
                db_store.update_magnet_check_result(
                    magnet["id"],
                    result.get("check_status"),
                    result.get("seeders", 0),
                    result.get("leechers", 0),
                    result.get("check_error"),
                )
                key = result.get("check_status") or "failed"
                if key not in {"active", "weak", "dead"}:
                    key = "failed"
            except Exception as exc:
                db_store.update_magnet_check_result(magnet["id"], None, 0, 0, str(exc))
                key = "failed"
            finally:
                with job["lock"]:
                    job["completed"] += 1
                    job[key] += 1
                work_queue.task_done()

    workers = [
        threading.Thread(target=worker, daemon=True)
        for _ in range(min(magnet_checker.CONCURRENCY_LIMIT, max(1, len(magnets))))
    ]
    for worker_thread in workers:
        worker_thread.start()
    for worker_thread in workers:
        worker_thread.join()
    with job["lock"]:
        job["cancelled"] = job["cancel_event"].is_set()
        job["running"] = False
        job["done"] = not job["cancelled"]
        if job["cancelled"]:
            job["message"] = "检测已取消"
        else:
            job["message"] = "检测完成"
    with MAGNET_CHECK_LOCK:
        if ACTIVE_MAGNET_CHECK_JOB_ID == job_id:
            ACTIVE_MAGNET_CHECK_JOB_ID = None


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
    response = fetch_html(
        start_url,
        headers=runtime_headers(runtime),
        proxies=build_proxy_dict(runtime.get("proxies")),
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


def current_controllable_task():
    task = db_store.get_active_task()
    if task:
        return task
    for task in db_store.list_tasks(limit=50):
        if task["state"] in {"paused", "waiting_cookie", "waiting_choice", "pending"}:
            return task
    return db_store.get_current_task()


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


@app.get("/api/version")
def get_version():
    return {"version": APP_VERSION, "auth_required": is_auth_required()}


@app.get("/api/runtime_config")
def get_runtime_config():
    runtime = db_store.get_runtime_config(include_cookie=False)
    return {"code": 200, "data": runtime}


@app.post("/api/runtime_config")
def set_runtime_config(config: RuntimeConfig):
    db_store.save_runtime_config(
        cookie=config.cookie,
        remember_cookie=config.remember_cookie,
        user_agent=config.user_agent,
        proxies=config.proxies,
        trackers=config.trackers,
    )
    return {"code": 200, "msg": "运行配置已保存"}


@app.post("/api/start")
def start_task(config: TaskConfig):
    return create_task_from_config(config)


@app.post("/api/stop")
def stop_task():
    task = current_controllable_task()
    if not task:
        return {"code": 400, "msg": "当前没有可暂停的任务"}
    if db_store.request_task_pause(task["task_id"]):
        return {"code": 200, "msg": "暂停请求已发送", "task_id": task["task_id"]}
    return {"code": 400, "msg": "当前任务状态不支持暂停"}


@app.post("/api/resume")
def resume_task(config: ResumeConfig):
    task = current_controllable_task()
    if not task:
        return {"code": 400, "msg": "找不到可恢复的任务"}
    cookie = resolve_task_cookie(config.cookie)
    if cookie:
        runtime = db_store.get_runtime_config(include_cookie=False)
        db_store.save_runtime_config(
            cookie=cookie,
            remember_cookie=runtime["remember_cookie"],
            user_agent=runtime["user_agent"],
            proxies=runtime["proxies"],
        )
    if db_store.resume_task_to_pending(task["task_id"]):
        ensure_queue_worker()
        return {"code": 200, "msg": "任务已恢复到队列", "task_id": task["task_id"]}
    return {"code": 400, "msg": "当前任务状态不支持恢复"}


@app.post("/api/set_mode")
def set_mode(config: ModeConfig):
    if config.mode not in {"incremental", "overwrite"}:
        return {"code": 400, "msg": "爬取模式非法"}
    task = current_controllable_task()
    if not task:
        return {"code": 400, "msg": "找不到等待模式选择的任务"}
    if db_store.update_task_mode(task["task_id"], config.mode) and db_store.resume_task_to_pending(task["task_id"]):
        ensure_queue_worker()
        return {"code": 200, "msg": "已应用爬取模式", "task_id": task["task_id"]}
    return {"code": 400, "msg": "当前任务状态不支持设置模式"}


@app.get("/api/status")
def get_status():
    task = db_store.get_current_task()
    if not task:
        return {"state": "idle", "progress": "0/0", "current": "-", "logs": ["等待任务启动..."]}
    return task_to_response(task, include_logs=True)


@app.post("/api/tasks")
def create_task(config: TaskConfig):
    return create_task_from_config(config)


@app.get("/api/tasks")
def list_tasks():
    return {"code": 200, "data": [task_to_response(task) for task in db_store.list_tasks(limit=100)]}


@app.post("/api/tasks/cleanup")
def cleanup_finished_tasks():
    deleted = db_store.cleanup_finished_tasks()
    return {"code": 200, "msg": f"已清理 {deleted} 个已结束任务", "deleted": deleted}


@app.delete("/api/tasks/{task_id}")
def delete_task(task_id: str):
    db_store.request_task_cancel(task_id)
    if not db_store.delete_task(task_id):
        return JSONResponse(status_code=404, content={"code": 404, "msg": "找不到任务"})
    return {"code": 200, "msg": "任务已删除"}


@app.get("/api/tasks/queue_status")
def get_queue_status():
    return {"code": 200, "data": get_queue_status_data()}


@app.post("/api/tasks/start_queue")
def start_queue():
    status = get_queue_status_data()
    if not status["can_start"]:
        return {"code": 400, "msg": "当前队列状态不支持启动", "data": status}
    ensure_queue_worker()
    return {"code": 200, "msg": "任务队列已启动"}


@app.get("/api/tasks/{task_id}")
def get_task_detail(task_id: str):
    task = db_store.get_task(task_id)
    if not task:
        return JSONResponse(status_code=404, content={"code": 404, "msg": "找不到任务"})
    return {"code": 200, "data": task_to_response(task, include_logs=True)}


@app.get("/api/tasks/{task_id}/incremental_magnets")
def get_task_incremental_magnets(task_id: str):
    task = db_store.get_task(task_id)
    if not task:
        return JSONResponse(status_code=404, content={"code": 404, "msg": "找不到任务"})
    if task.get("crawl_mode") != "incremental":
        return JSONResponse(status_code=400, content={"code": 400, "msg": "该任务不是增量任务"})
    codes = task_incremental_movie_codes(task)
    if not codes:
        return {"code": 200, "data": [], "count": 0}
    filename = task.get("collection_filename") or task.get("final_filename") or task.get("requested_filename") or ""
    try:
        safe_name = normalize_csv_filename(filename)
    except UnsafeFilenameError:
        return JSONResponse(status_code=400, content={"code": 400, "msg": "任务文件名非法"})
    if not db_store.collection_exists(safe_name):
        return JSONResponse(status_code=404, content={"code": 404, "msg": "找不到该集合"})
    links = db_store.get_magnet_links_for_codes(safe_name, codes)
    return {"code": 200, "data": links, "count": len(links)}


@app.post("/api/tasks/{task_id}/pause")
def pause_task(task_id: str):
    if db_store.request_task_pause(task_id):
        return {"code": 200, "msg": "暂停请求已发送"}
    return {"code": 400, "msg": "任务不存在或状态不支持暂停"}


@app.post("/api/tasks/{task_id}/resume")
def resume_task_by_id(task_id: str):
    if db_store.resume_task_to_pending(task_id):
        ensure_queue_worker()
        return {"code": 200, "msg": "任务已恢复到队列"}
    return {"code": 400, "msg": "任务不存在或状态不支持恢复"}


@app.post("/api/tasks/{task_id}/cancel")
def cancel_task(task_id: str):
    if db_store.request_task_cancel(task_id):
        return {"code": 200, "msg": "取消请求已发送"}
    return {"code": 400, "msg": "任务不存在或状态不支持取消"}


@app.post("/api/tasks/{task_id}/cookie")
def update_task_cookie(task_id: str, config: CookieConfig):
    cookie = resolve_task_cookie(config.cookie)
    if not cookie:
        return {"code": 400, "msg": "无法获取有效 Cookie"}
    if not db_store.update_task_cookie(task_id, cookie):
        return {"code": 404, "msg": "找不到任务"}
    db_store.resume_task_to_pending(task_id)
    ensure_queue_worker()
    return {"code": 200, "msg": "Cookie 已更新"}


@app.post("/api/tasks/{task_id}/refresh_cookie")
def refresh_task_cookie(task_id: str):
    cookie = get_android_javdb_cookie().strip()
    if not cookie:
        return {"code": 400, "msg": "当前环境无法读取 Android Cookie，请手动粘贴 Cookie"}
    if not db_store.update_task_cookie(task_id, cookie):
        return {"code": 404, "msg": "找不到任务"}
    db_store.resume_task_to_pending(task_id)
    ensure_queue_worker()
    return {"code": 200, "msg": "已使用 Android 当前 Cookie 恢复任务"}


@app.post("/api/tasks/{task_id}/mode")
def set_task_mode(task_id: str, config: ModeConfig):
    if not db_store.update_task_mode(task_id, config.mode):
        return {"code": 400, "msg": "任务不存在或模式非法"}
    db_store.resume_task_to_pending(task_id)
    ensure_queue_worker()
    return {"code": 200, "msg": "已应用爬取模式"}


@app.get("/api/history")
def get_history():
    return {"code": 200, "data": db_store.get_history()}


@app.get("/api/collections/{name}/movies")
def get_collection_movies(name: str):
    try:
        safe_name = normalize_csv_filename(name)
    except UnsafeFilenameError:
        return JSONResponse(status_code=400, content={"code": 400, "msg": "文件名非法"})
    if not db_store.collection_exists(safe_name):
        return JSONResponse(status_code=404, content={"code": 404, "msg": "找不到该集合"})
    return {"code": 200, "data": db_store.get_collection_movies(safe_name)}


@app.post("/api/collections/{name}/incremental_task")
def create_collection_incremental_task(name: str):
    try:
        safe_name = normalize_csv_filename(name)
    except UnsafeFilenameError:
        return JSONResponse(status_code=400, content={"code": 400, "msg": "文件名非法"})
    if not db_store.collection_exists(safe_name):
        return JSONResponse(status_code=404, content={"code": 404, "msg": "找不到该集合"})
    source_url = db_store.get_collection_source_url(safe_name)
    if not source_url:
        return JSONResponse(status_code=400, content={"code": 400, "msg": "该集合缺少原始爬取 URL，无法快捷增量"})
    return create_task_from_config(
        TaskConfig(
            start_url=source_url,
            filename=safe_name,
            crawl_mode="incremental",
        )
    )


@app.get("/api/movies/{movie_id}/magnets")
def get_movie_magnets(movie_id: int):
    return {"code": 200, "data": db_store.get_movie_magnets(movie_id)}


@app.post("/api/movies/{movie_id}/select_magnet")
def select_movie_magnet(movie_id: int, req: SelectMagnetRequest):
    if not db_store.select_movie_magnet(movie_id, req.magnet_id):
        return JSONResponse(status_code=404, content={"code": 404, "msg": "找不到候选磁力"})
    return {"code": 200, "msg": "已更新选中磁力"}


@app.post("/api/movies/{movie_id}/check_magnets")
def check_movie_magnets(movie_id: int, failed_only: bool = False):
    magnets = db_store.get_movie_magnet_rows(movie_id)
    if not magnets:
        return JSONResponse(status_code=404, content={"code": 404, "msg": "找不到候选磁力"})
    if failed_only:
        magnets = failed_magnet_rows(magnets)
        if not magnets:
            return JSONResponse(status_code=404, content={"code": 404, "msg": "没有检测失败的磁力"})
    job, active = create_magnet_check_job("movie", str(movie_id), magnets)
    if active:
        return JSONResponse(
            status_code=409,
            content={"code": 409, "msg": "磁力检测任务正在运行", "data": public_magnet_check_job(active)},
        )
    return {"code": 200, "data": public_magnet_check_job(job)}


@app.post("/api/collections/{name}/check_magnets")
def check_collection_magnets(name: str, failed_only: bool = False):
    try:
        safe_name = normalize_csv_filename(name)
    except UnsafeFilenameError:
        return JSONResponse(status_code=400, content={"code": 400, "msg": "文件名非法"})
    if not db_store.collection_exists(safe_name):
        return JSONResponse(status_code=404, content={"code": 404, "msg": "找不到该集合"})
    magnets = []
    for movie_id in db_store.get_collection_movie_ids(safe_name):
        magnets.extend(db_store.get_movie_magnet_rows(movie_id))
    if not magnets:
        return JSONResponse(status_code=404, content={"code": 404, "msg": "该集合没有候选磁力"})
    if failed_only:
        magnets = failed_magnet_rows(magnets)
        if not magnets:
            return JSONResponse(status_code=404, content={"code": 404, "msg": "没有检测失败的磁力"})
    job, active = create_magnet_check_job("collection", safe_name, magnets)
    if active:
        return JSONResponse(
            status_code=409,
            content={"code": 409, "msg": "磁力检测任务正在运行", "data": public_magnet_check_job(active)},
        )
    return {"code": 200, "data": public_magnet_check_job(job)}


@app.post("/api/magnets/check_all")
def check_all_magnets(failed_only: bool = False):
    magnets = []
    for item in db_store.get_history():
        for movie_id in db_store.get_collection_movie_ids(item["name"]):
            magnets.extend(db_store.get_movie_magnet_rows(movie_id))
    if not magnets:
        return JSONResponse(status_code=404, content={"code": 404, "msg": "没有候选磁力"})
    if failed_only:
        magnets = failed_magnet_rows(magnets)
        if not magnets:
            return JSONResponse(status_code=404, content={"code": 404, "msg": "没有检测失败的磁力"})
    job, active = create_magnet_check_job("all", "all", magnets)
    if active:
        return JSONResponse(
            status_code=409,
            content={"code": 409, "msg": "磁力检测任务正在运行", "data": public_magnet_check_job(active)},
        )
    return {"code": 200, "data": public_magnet_check_job(job)}


@app.get("/api/magnet_check_jobs/current")
def get_current_magnet_check_job_route():
    with MAGNET_CHECK_LOCK:
        job = MAGNET_CHECK_JOBS.get(ACTIVE_MAGNET_CHECK_JOB_ID) if ACTIVE_MAGNET_CHECK_JOB_ID else None
    if not job:
        return {"code": 200, "data": None}
    data = public_magnet_check_job(job)
    return {"code": 200, "data": data if data["running"] else None}


@app.get("/api/magnet_check_jobs/{job_id}")
def get_magnet_check_job(job_id: str):
    job = MAGNET_CHECK_JOBS.get(job_id)
    if not job:
        return JSONResponse(status_code=404, content={"code": 404, "msg": "找不到检测任务"})
    return {"code": 200, "data": public_magnet_check_job(job)}


@app.post("/api/magnet_check_jobs/{job_id}/cancel")
def cancel_magnet_check_job(job_id: str):
    job = MAGNET_CHECK_JOBS.get(job_id)
    if not job:
        return JSONResponse(status_code=404, content={"code": 404, "msg": "找不到检测任务"})
    job["cancel_event"].set()
    with job["lock"]:
        job["cancelled"] = True
        job["message"] = "正在取消检测"
    return {"code": 200, "data": public_magnet_check_job(job)}


@app.post("/api/delete")
def delete_history(req: DeleteRequest):
    success_count = 0
    fail_count = 0
    fail_reasons = []

    active_file = None
    active_task = db_store.get_active_task()
    if active_task:
        active_file = get_safe_name(active_task.get("collection_filename") or active_task.get("final_filename"))

    deletable = []
    for filename in req.filenames:
        safe_name = get_safe_name(filename)
        if not safe_name:
            fail_count += 1
            fail_reasons.append(f"{filename}(不存在或非法)")
            continue
        if active_file == safe_name:
            fail_count += 1
            fail_reasons.append(f"{filename}(被占用)")
            continue
        deletable.append(safe_name)

    try:
        deleted, missing = db_store.delete_collections(deletable, DATA_DIR)
        success_count += len(deleted)
        fail_count += len(missing)
        fail_reasons.extend(f"{name}(不存在)" for name in missing)
    except OSError:
        fail_count += len(deletable)
        fail_reasons.append("系统占用")

    if fail_count == 0:
        return {"code": 200, "msg": "删除成功"}
    reason_str = ", ".join(fail_reasons[:3]) + ("..." if len(fail_reasons) > 3 else "")
    return {
        "code": 200 if success_count > 0 else 400,
        "msg": f"成功 {success_count} 个，失败 {fail_count} 个 [{reason_str}]",
    }


@app.post("/api/magnets/auto_select")
def auto_select_magnets(req: DeleteRequest):
    filenames = []
    for filename in req.filenames:
        safe_name = get_safe_name(filename)
        if not safe_name:
            return JSONResponse(status_code=400, content={"code": 400, "msg": "集合不存在或非法"})
        filenames.append(safe_name)
    updated = db_store.auto_select_collection_magnets(filenames)
    return {"code": 200, "msg": f"已按评分自动选择 {updated} 部影片的磁力", "data": {"updated": updated}}


@app.get("/api/download")
def download_csv(name: str = None, tags: str = None):
    if not name:
        return JSONResponse(status_code=400, content={"code": 400, "msg": "未指定文件名参数"})
    try:
        csv_bytes, safe_name = db_store.export_collection_to_csv_bytes(name, parse_tag_filter(tags))
    except UnsafeFilenameError:
        return JSONResponse(status_code=400, content={"code": 400, "msg": "文件名非法"})
    if csv_bytes is not None:
        quoted_name = quote(safe_name)
        return Response(
            content=csv_bytes,
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="download.csv"; filename*=UTF-8\'\'{quoted_name}'},
        )
    return JSONResponse(status_code=404, content={"code": 404, "msg": "找不到该文件"})


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


@app.get("/api/magnets")
def get_magnets(name: str = None, tags: str = None):
    if not name:
        return {"code": 400, "msg": "未指定文件名参数"}
    try:
        safe_name = normalize_csv_filename(name)
    except UnsafeFilenameError:
        return {"code": 400, "msg": "文件名非法"}
    if not db_store.collection_exists(safe_name):
        return {"code": 404, "msg": "找不到该文件"}
    try:
        return {"code": 200, "data": db_store.get_magnet_links(safe_name, parse_tag_filter(tags))}
    except Exception as e:
        return {"code": 500, "msg": f"读取数据出错: {str(e)}"}


@app.post("/api/clear_logs")
def clear_logs():
    active_task = db_store.get_active_task()
    if active_task:
        return {"code": 400, "msg": "任务运行中，请先暂停或取消后再清除记录"}
    write_status_mirror(None)
    return {"code": 200, "msg": "记录已清除"}


@app.post("/api/get_tags")
def get_tags(req: TagConfigRequest):
    try:
        base_url = ensure_zh_locale(req.url)
        runtime = get_runtime_for_request()
        headers = {
            "User-Agent": req.user_agent or runtime.get("user_agent") or "",
            "Cookie": req.cookie or runtime.get("cookie") or "",
        }
        proxy = req.proxies if req.proxies is not None else runtime.get("proxies")
        proxy_dict = build_proxy_dict(proxy)
        response = fetch_html(base_url, headers=headers, proxies=proxy_dict)
        if response.status_code != 200:
            return {"code": response.status_code, "msg": f"请求失败，状态码: {response.status_code}"}

        soup = BeautifulSoup(response.text, "html.parser")
        tags_div = soup.select_one(".actor-tags .content")
        if not tags_div:
            return {"code": 404, "msg": "未在页面中找到标签区域"}

        tags = []
        for a in tags_div.find_all("a", class_="tag"):
            name = a.text.strip()
            href = a.get("href", "")
            parsed_url = urlparse(href)
            params = parse_qs(parsed_url.query)
            if "t" in params:
                tag_value = params["t"][0]
                if tag_value:
                    tags.append({"name": name, "value": tag_value})

        return {"code": 200, "data": tags}
    except Exception as e:
        return {"code": 500, "msg": f"解析标签发生异常: {str(e)}"}


def start_server():
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000, access_log=False, log_level="info")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000, access_log=False, log_level="info")
