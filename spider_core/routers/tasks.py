"""routers/tasks — 任务配置、队列与生命周期端点。

业务 helper / 模型仍在 main 中作为共享核心，这里通过 from-import 复用（运行期 main 已完整初始化）。
"""

from fastapi import APIRouter
from fastapi.responses import JSONResponse

import db_store
from spider_engine import get_android_javdb_cookie
from storage_utils import UnsafeFilenameError, normalize_csv_filename
from main import (
    CookieConfig,
    ModeConfig,
    TaskConfig,
    create_task_from_config,
    ensure_queue_worker,
    get_queue_status_data,
    resolve_task_cookie,
    task_incremental_movie_codes,
    task_to_response,
)

router = APIRouter()


@router.post("/api/tasks")
def create_task(config: TaskConfig):
    return create_task_from_config(config)


@router.get("/api/tasks")
def list_tasks():
    return {"code": 200, "data": [task_to_response(task) for task in db_store.list_tasks(limit=100)]}


@router.post("/api/tasks/cleanup")
def cleanup_finished_tasks():
    deleted = db_store.cleanup_finished_tasks()
    return {"code": 200, "msg": f"已清理 {deleted} 个已结束任务", "deleted": deleted}


@router.delete("/api/tasks/{task_id}")
def delete_task(task_id: str):
    db_store.request_task_cancel(task_id)
    if not db_store.delete_task(task_id):
        return JSONResponse(status_code=404, content={"code": 404, "msg": "找不到任务"})
    return {"code": 200, "msg": "任务已删除"}


@router.get("/api/tasks/queue_status")
def get_queue_status():
    return {"code": 200, "data": get_queue_status_data()}


@router.post("/api/tasks/start_queue")
def start_queue():
    status = get_queue_status_data()
    if not status["can_start"]:
        return JSONResponse(status_code=400, content={"code": 400, "msg": "当前队列状态不支持启动", "data": status})
    ensure_queue_worker()
    return {"code": 200, "msg": "任务队列已启动"}


@router.get("/api/tasks/{task_id}")
def get_task_detail(task_id: str):
    task = db_store.get_task(task_id)
    if not task:
        return JSONResponse(status_code=404, content={"code": 404, "msg": "找不到任务"})
    return {"code": 200, "data": task_to_response(task, include_logs=True)}


@router.get("/api/tasks/{task_id}/incremental_magnets")
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


@router.post("/api/tasks/{task_id}/pause")
def pause_task(task_id: str):
    if db_store.request_task_pause(task_id):
        return {"code": 200, "msg": "暂停请求已发送"}
    return JSONResponse(status_code=400, content={"code": 400, "msg": "任务不存在或状态不支持暂停"})


@router.post("/api/tasks/{task_id}/resume")
def resume_task_by_id(task_id: str):
    if db_store.resume_task_to_pending(task_id):
        ensure_queue_worker()
        return {"code": 200, "msg": "任务已恢复到队列"}
    return JSONResponse(status_code=400, content={"code": 400, "msg": "任务不存在或状态不支持恢复"})


@router.post("/api/tasks/{task_id}/cancel")
def cancel_task(task_id: str):
    if db_store.request_task_cancel(task_id):
        return {"code": 200, "msg": "取消请求已发送"}
    return JSONResponse(status_code=400, content={"code": 400, "msg": "任务不存在或状态不支持取消"})


@router.post("/api/tasks/{task_id}/cookie")
def update_task_cookie(task_id: str, config: CookieConfig):
    cookie = resolve_task_cookie(config.cookie)
    if not cookie:
        return JSONResponse(status_code=400, content={"code": 400, "msg": "无法获取有效 Cookie"})
    if not db_store.update_task_cookie(task_id, cookie):
        return JSONResponse(status_code=404, content={"code": 404, "msg": "找不到任务"})
    db_store.resume_task_to_pending(task_id)
    ensure_queue_worker()
    return {"code": 200, "msg": "Cookie 已更新"}


@router.post("/api/tasks/{task_id}/refresh_cookie")
def refresh_task_cookie(task_id: str):
    cookie = get_android_javdb_cookie().strip()
    if not cookie:
        return JSONResponse(status_code=400, content={"code": 400, "msg": "当前环境无法读取 Android Cookie，请手动粘贴 Cookie"})
    if not db_store.update_task_cookie(task_id, cookie):
        return JSONResponse(status_code=404, content={"code": 404, "msg": "找不到任务"})
    db_store.resume_task_to_pending(task_id)
    ensure_queue_worker()
    return {"code": 200, "msg": "已使用 Android 当前 Cookie 恢复任务"}


@router.post("/api/tasks/{task_id}/mode")
def set_task_mode(task_id: str, config: ModeConfig):
    if not db_store.update_task_mode(task_id, config.mode):
        return JSONResponse(status_code=400, content={"code": 400, "msg": "任务不存在或模式非法"})
    db_store.resume_task_to_pending(task_id)
    ensure_queue_worker()
    return {"code": 200, "msg": "已应用爬取模式"}
