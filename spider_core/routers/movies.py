"""routers/movies — 集合、影片、候选磁力选择与自动择优端点。"""

from fastapi import APIRouter
from fastapi.responses import JSONResponse

import db_store
from storage_utils import UnsafeFilenameError, normalize_csv_filename
from main import (
    DeleteRequest,
    SelectMagnetRequest,
    TaskConfig,
    create_task_from_config,
    get_safe_name,
)

router = APIRouter()


@router.get("/api/history")
def get_history():
    return {"code": 200, "data": db_store.get_history()}


@router.get("/api/collections/{name}/movies")
def get_collection_movies(name: str):
    try:
        safe_name = normalize_csv_filename(name)
    except UnsafeFilenameError:
        return JSONResponse(status_code=400, content={"code": 400, "msg": "文件名非法"})
    if not db_store.collection_exists(safe_name):
        return JSONResponse(status_code=404, content={"code": 404, "msg": "找不到该集合"})
    return {"code": 200, "data": db_store.get_collection_movies(safe_name)}


@router.post("/api/collections/{name}/incremental_task")
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


@router.get("/api/movies/{movie_id}/magnets")
def get_movie_magnets(movie_id: int):
    return {"code": 200, "data": db_store.get_movie_magnets(movie_id)}


@router.post("/api/movies/{movie_id}/select_magnet")
def select_movie_magnet(movie_id: int, req: SelectMagnetRequest):
    if not db_store.select_movie_magnet(movie_id, req.magnet_id):
        return JSONResponse(status_code=404, content={"code": 404, "msg": "找不到候选磁力"})
    return {"code": 200, "msg": "已更新选中磁力"}


@router.post("/api/magnets/auto_select")
def auto_select_magnets(req: DeleteRequest):
    filenames = []
    for filename in req.filenames:
        safe_name = get_safe_name(filename)
        if not safe_name:
            return JSONResponse(status_code=400, content={"code": 400, "msg": "集合不存在或非法"})
        filenames.append(safe_name)
    updated = db_store.auto_select_collection_magnets(filenames)
    return {"code": 200, "msg": f"已按评分自动选择 {updated} 部影片的磁力", "data": {"updated": updated}}
