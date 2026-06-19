"""routers/magnets — 磁力验活（检测任务启动、查询、取消）端点。

检测任务编排在 services.magnet_service（自包含全局态）。读取活动任务 id 时统一用
magnet_service.ACTIVE_MAGNET_CHECK_JOB_ID 属性访问，以获取实时值。
"""

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

import db_store
from dependencies import valid_collection
from services import magnet_service

router = APIRouter()


def _magnet_check_error_response(error: magnet_service.MagnetCheckError):
    content = {"code": error.status_code, "msg": error.msg}
    if error.data:
        content["data"] = error.data
    return JSONResponse(status_code=error.status_code, content=content)


@router.post("/api/movies/{movie_id}/check_magnets")
def check_movie_magnets(movie_id: int, failed_only: bool = False):
    magnets = db_store.get_movie_magnets(movie_id)
    try:
        return magnet_service.start_magnet_check("movie", str(movie_id), magnets, "找不到候选磁力", failed_only)
    except magnet_service.MagnetCheckError as e:
        return _magnet_check_error_response(e)


@router.post("/api/collections/{name}/check_magnets")
def check_collection_magnets(safe_name: str = Depends(valid_collection), failed_only: bool = False):
    magnets = []
    for movie_id in db_store.get_collection_movie_ids(safe_name):
        magnets.extend(db_store.get_movie_magnets(movie_id))
    try:
        return magnet_service.start_magnet_check("collection", safe_name, magnets, "该集合没有候选磁力", failed_only)
    except magnet_service.MagnetCheckError as e:
        return _magnet_check_error_response(e)


@router.post("/api/magnets/check_all")
def check_all_magnets(failed_only: bool = False):
    magnets = []
    for item in db_store.get_history():
        for movie_id in db_store.get_collection_movie_ids(item["name"]):
            magnets.extend(db_store.get_movie_magnets(movie_id))
    try:
        return magnet_service.start_magnet_check("all", "all", magnets, "没有候选磁力", failed_only)
    except magnet_service.MagnetCheckError as e:
        return _magnet_check_error_response(e)


@router.get("/api/magnet_check_jobs/current")
def get_current_magnet_check_job_route():
    return {"code": 200, "data": magnet_service.get_current_job()}


@router.get("/api/magnet_check_jobs/{job_id}")
def get_magnet_check_job(job_id: str):
    job = magnet_service.MAGNET_CHECK_JOBS.get(job_id)
    if not job:
        return JSONResponse(status_code=404, content={"code": 404, "msg": "找不到检测任务"})
    return {"code": 200, "data": magnet_service.public_magnet_check_job(job)}


@router.post("/api/magnet_check_jobs/{job_id}/cancel")
def cancel_magnet_check_job(job_id: str):
    success, data = magnet_service.cancel_job(job_id)
    if not success:
        return JSONResponse(status_code=404, content={"code": 404, "msg": "找不到检测任务"})
    return {"code": 200, "data": data}
