"""排行榜集合端点：按分类/周期定位榜单快照并复用现有集合能力。"""

from urllib.parse import quote

from fastapi import APIRouter
from fastapi.responses import JSONResponse, Response

import db_store
from main import TaskConfig, create_task_from_config, get_safe_name, parse_tag_filter
from ranking_utils import COLLECTION_TYPE_RANKING, is_valid_ranking, ranking_url
from services import magnet_service
from spider_engine import DATA_DIR
from storage_utils import UnsafeFilenameError

router = APIRouter()


def _invalid_ranking_response():
    return JSONResponse(status_code=404, content={"code": 404, "msg": "排行榜不存在"})


def _ranking_filename(category, period):
    if not is_valid_ranking(category, period):
        return ""
    return db_store.get_ranking_collection_filename(category, period)


@router.get("/api/rankings/{category}/{period}/movies")
def get_ranking_movies(category: str, period: str):
    if not is_valid_ranking(category, period):
        return _invalid_ranking_response()
    data = db_store.get_ranking_movies(category, period)
    data["collection_filename"] = _ranking_filename(category, period)
    return {"code": 200, "data": data}


@router.post("/api/rankings/{category}/{period}/update")
def create_ranking_update_task(category: str, period: str):
    url = ranking_url(category, period)
    if not url:
        return _invalid_ranking_response()
    return create_task_from_config(
        TaskConfig(
            start_url=url,
            crawl_mode="overwrite",
            collection_type=COLLECTION_TYPE_RANKING,
            ranking_category=category,
            ranking_period=period,
        )
    )


@router.get("/api/rankings/{category}/{period}/magnets")
def get_ranking_magnets(category: str, period: str, tags: str = None, exclude_tags: str = None):
    if not is_valid_ranking(category, period):
        return _invalid_ranking_response()
    try:
        links = db_store.get_ranking_magnet_links(category, period, parse_tag_filter(tags), parse_tag_filter(exclude_tags))
    except Exception as e:
        return JSONResponse(status_code=500, content={"code": 500, "msg": f"读取数据出错: {str(e)}"})
    return {"code": 200, "data": links}


@router.get("/api/rankings/{category}/{period}/download")
def download_ranking_csv(category: str, period: str, tags: str = None, exclude_tags: str = None):
    if not is_valid_ranking(category, period):
        return _invalid_ranking_response()
    filename = _ranking_filename(category, period)
    if not filename:
        return JSONResponse(status_code=404, content={"code": 404, "msg": "找不到该榜单"})
    try:
        csv_bytes, safe_name = db_store.export_collection_to_csv_bytes(
            filename,
            parse_tag_filter(tags),
            parse_tag_filter(exclude_tags),
        )
    except UnsafeFilenameError:
        return JSONResponse(status_code=400, content={"code": 400, "msg": "文件名非法"})
    if csv_bytes is None:
        return JSONResponse(status_code=404, content={"code": 404, "msg": "找不到该榜单"})
    quoted_name = quote(safe_name)
    return Response(
        content=csv_bytes,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="download.csv"; filename*=UTF-8\'\'{quoted_name}'},
    )


@router.post("/api/rankings/{category}/{period}/clear")
def clear_ranking_collection(category: str, period: str):
    if not is_valid_ranking(category, period):
        return _invalid_ranking_response()
    filename = _ranking_filename(category, period)
    if not filename:
        return {"code": 200, "msg": "榜单已为空"}

    active_file = None
    active_task = db_store.get_active_task()
    if active_task:
        active_file = get_safe_name(active_task.get("collection_filename") or active_task.get("final_filename"))
    if active_file == filename:
        return JSONResponse(status_code=400, content={"code": 400, "msg": "榜单正在被任务占用"})

    deleted, missing = db_store.delete_collections([filename], DATA_DIR)
    if deleted:
        return {"code": 200, "msg": "清空成功"}
    return JSONResponse(status_code=404, content={"code": 404, "msg": f"找不到该榜单: {', '.join(missing)}"})


@router.post("/api/rankings/{category}/{period}/check_magnets")
def check_ranking_magnets(category: str, period: str, failed_only: bool = False):
    if not is_valid_ranking(category, period):
        return _invalid_ranking_response()
    magnets = []
    for movie_id in db_store.get_ranking_movie_ids(category, period):
        magnets.extend(db_store.get_movie_magnets(movie_id))
    return magnet_service.start_magnet_check(
        "ranking",
        f"{category}:{period}",
        magnets,
        "该榜单没有候选磁力",
        failed_only,
    )
