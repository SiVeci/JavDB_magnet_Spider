"""routers/magnets — 磁力验活（检测任务启动、查询、取消）端点。

检测任务编排在 services.magnet_service（自包含全局态）。读取活动任务 id 时统一用
magnet_service.ACTIVE_MAGNET_CHECK_JOB_ID 属性访问，以获取实时值。
"""

from fastapi import APIRouter
from fastapi.responses import JSONResponse

import db_store
from storage_utils import UnsafeFilenameError, normalize_csv_filename
from services import magnet_service

router = APIRouter()


@router.post("/api/movies/{movie_id}/check_magnets")
def check_movie_magnets(movie_id: int, failed_only: bool = False):
    magnets = db_store.get_movie_magnets(movie_id)
    return magnet_service.start_magnet_check("movie", str(movie_id), magnets, "找不到候选磁力", failed_only)


@router.post("/api/collections/{name}/check_magnets")
def check_collection_magnets(name: str, failed_only: bool = False):
    try:
        safe_name = normalize_csv_filename(name)
    except UnsafeFilenameError:
        return JSONResponse(status_code=400, content={"code": 400, "msg": "文件名非法"})
    if not db_store.collection_exists(safe_name):
        return JSONResponse(status_code=404, content={"code": 404, "msg": "找不到该集合"})
    magnets = []
    for movie_id in db_store.get_collection_movie_ids(safe_name):
        magnets.extend(db_store.get_movie_magnets(movie_id))
    return magnet_service.start_magnet_check("collection", safe_name, magnets, "该集合没有候选磁力", failed_only)


@router.post("/api/magnets/check_all")
def check_all_magnets(failed_only: bool = False):
    magnets = []
    for item in db_store.get_history():
        for movie_id in db_store.get_collection_movie_ids(item["name"]):
            magnets.extend(db_store.get_movie_magnets(movie_id))
    return magnet_service.start_magnet_check("all", "all", magnets, "没有候选磁力", failed_only)


@router.get("/api/magnet_check_jobs/current")
def get_current_magnet_check_job_route():
    with magnet_service.MAGNET_CHECK_LOCK:
        active_id = magnet_service.ACTIVE_MAGNET_CHECK_JOB_ID
        job = magnet_service.MAGNET_CHECK_JOBS.get(active_id) if active_id else None
    if not job:
        return {"code": 200, "data": None}
    data = magnet_service.public_magnet_check_job(job)
    return {"code": 200, "data": data if data["running"] else None}


@router.get("/api/magnet_check_jobs/{job_id}")
def get_magnet_check_job(job_id: str):
    job = magnet_service.MAGNET_CHECK_JOBS.get(job_id)
    if not job:
        return JSONResponse(status_code=404, content={"code": 404, "msg": "找不到检测任务"})
    return {"code": 200, "data": magnet_service.public_magnet_check_job(job)}


@router.post("/api/magnet_check_jobs/{job_id}/cancel")
def cancel_magnet_check_job(job_id: str):
    job = magnet_service.MAGNET_CHECK_JOBS.get(job_id)
    if not job:
        return JSONResponse(status_code=404, content={"code": 404, "msg": "找不到检测任务"})
    job["cancel_event"].set()
    with job["lock"]:
        job["cancelled"] = True
        job["message"] = "正在取消检测"
    return {"code": 200, "data": magnet_service.public_magnet_check_job(job)}
