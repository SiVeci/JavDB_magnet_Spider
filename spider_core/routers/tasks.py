"""Task configuration, queue, and lifecycle API routes."""

from fastapi import APIRouter
from fastapi.responses import JSONResponse

import db_store
from dependencies import valid_collection
from schemas import CookieConfig, ModeConfig, TaskConfig
from services.cookie_validation_service import validate_runtime_cookie
from services.queue_service import ensure_queue_worker, get_queue_status_data
from services.task_service import (
    TaskConfigError,
    cookie_validation_message,
    create_task_from_config,
    resolve_task_cookie,
    task_incremental_movie_codes,
    task_to_response,
)
from spider_engine import get_android_javdb_cookie

router = APIRouter()


@router.post("/api/tasks")
def create_task(config: TaskConfig):
    try:
        return create_task_from_config(config)
    except TaskConfigError as exc:
        return JSONResponse(status_code=exc.status_code, content={"code": exc.status_code, "msg": exc.msg, **exc.extra})


@router.get("/api/tasks")
def list_tasks():
    return {"code": 200, "data": [task_to_response(task) for task in db_store.list_tasks(limit=100)]}


@router.post("/api/tasks/cleanup")
def cleanup_finished_tasks():
    deleted = db_store.cleanup_finished_tasks()
    return {"code": 200, "msg": f"Cleaned {deleted} finished tasks", "data": {"deleted": deleted}}


@router.delete("/api/tasks/{task_id}")
def delete_task(task_id: str):
    db_store.request_task_cancel(task_id)
    if not db_store.delete_task(task_id):
        return JSONResponse(status_code=404, content={"code": 404, "msg": "Task not found"})
    return {"code": 200, "msg": "Task deleted"}


@router.get("/api/tasks/queue_status")
def get_queue_status():
    return {"code": 200, "data": get_queue_status_data()}


@router.post("/api/tasks/start_queue")
def start_queue():
    status = get_queue_status_data()
    if not status["can_start"]:
        return JSONResponse(status_code=400, content={"code": 400, "msg": "Queue cannot be started now", "data": status})
    ensure_queue_worker()
    return {"code": 200, "msg": "Task queue started"}


@router.get("/api/tasks/{task_id}")
def get_task_detail(task_id: str):
    task = db_store.get_task(task_id)
    if not task:
        return JSONResponse(status_code=404, content={"code": 404, "msg": "Task not found"})
    return {"code": 200, "data": task_to_response(task, include_logs=True)}


@router.get("/api/tasks/{task_id}/incremental_magnets")
def get_task_incremental_magnets(task_id: str):
    task = db_store.get_task(task_id)
    if not task:
        return JSONResponse(status_code=404, content={"code": 404, "msg": "Task not found"})
    if task.get("crawl_mode") != "incremental":
        return JSONResponse(status_code=400, content={"code": 400, "msg": "Task is not incremental"})
    codes = task_incremental_movie_codes(task)
    if not codes:
        return {"code": 200, "data": [], "count": 0}
    filename = task.get("collection_filename") or task.get("final_filename") or task.get("requested_filename") or ""
    safe_name = valid_collection(filename)
    links = db_store.get_magnet_links_for_codes(safe_name, codes)
    return {"code": 200, "data": links, "count": len(links)}


@router.post("/api/tasks/{task_id}/pause")
def pause_task(task_id: str):
    if db_store.request_task_pause(task_id):
        return {"code": 200, "msg": "Pause requested"}
    return JSONResponse(status_code=400, content={"code": 400, "msg": "Task cannot be paused"})


@router.post("/api/tasks/{task_id}/resume")
def resume_task_by_id(task_id: str):
    if not db_store.get_task(task_id):
        return JSONResponse(status_code=400, content={"code": 400, "msg": "Task cannot be resumed"})
    result = validate_runtime_cookie(update_runtime=True)
    if not result.get("valid"):
        msg = cookie_validation_message(result)
        db_store.append_task_log(task_id, f"Cookie validation failed: {msg}")
        return JSONResponse(status_code=400, content={"code": 400, "msg": msg, "cookie_status": result.get("status")})
    if db_store.resume_task_to_pending(task_id):
        ensure_queue_worker()
        return {"code": 200, "msg": "Task resumed"}
    return JSONResponse(status_code=400, content={"code": 400, "msg": "Task cannot be resumed"})


@router.post("/api/tasks/{task_id}/cancel")
def cancel_task(task_id: str):
    if db_store.request_task_cancel(task_id):
        return {"code": 200, "msg": "Cancel requested"}
    return JSONResponse(status_code=400, content={"code": 400, "msg": "Task cannot be canceled"})


@router.post("/api/tasks/{task_id}/cookie")
def update_task_cookie(task_id: str, config: CookieConfig):
    cookie = resolve_task_cookie(config.cookie)
    if not cookie:
        return JSONResponse(status_code=400, content={"code": 400, "msg": "Unable to get a valid Cookie"})
    if not db_store.update_task_cookie(task_id, cookie, "manual"):
        return JSONResponse(status_code=404, content={"code": 404, "msg": "Task not found"})
    result = validate_runtime_cookie(update_runtime=True)
    if not result.get("valid"):
        msg = cookie_validation_message(result)
        db_store.append_task_log(task_id, f"Cookie validation failed: {msg}")
        return JSONResponse(status_code=400, content={"code": 400, "msg": msg, "cookie_status": result.get("status")})
    db_store.resume_task_to_pending(task_id)
    ensure_queue_worker()
    return {"code": 200, "msg": "Cookie updated"}


@router.post("/api/tasks/{task_id}/refresh_cookie")
def refresh_task_cookie(task_id: str):
    cookie = get_android_javdb_cookie().strip()
    if not cookie:
        return JSONResponse(status_code=400, content={"code": 400, "msg": "Android Cookie is unavailable. Paste Cookie manually."})
    if not db_store.update_task_cookie(task_id, cookie, "android_webview"):
        return JSONResponse(status_code=404, content={"code": 404, "msg": "Task not found"})
    result = validate_runtime_cookie(update_runtime=True)
    if not result.get("valid"):
        msg = cookie_validation_message(result)
        db_store.append_task_log(task_id, f"Android Cookie validation failed: {msg}")
        return JSONResponse(status_code=400, content={"code": 400, "msg": msg, "cookie_status": result.get("status")})
    db_store.resume_task_to_pending(task_id)
    ensure_queue_worker()
    return {"code": 200, "msg": "Android Cookie applied"}


@router.post("/api/tasks/{task_id}/mode")
def set_task_mode(task_id: str, config: ModeConfig):
    if not db_store.update_task_mode(task_id, config.mode):
        return JSONResponse(status_code=400, content={"code": 400, "msg": "Invalid task or crawl mode"})
    db_store.resume_task_to_pending(task_id)
    ensure_queue_worker()
    return {"code": 200, "msg": "Crawl mode applied"}
