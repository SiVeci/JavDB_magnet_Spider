"""排行榜集合端点：按分类/周期定位榜单快照并复用现有集合能力。"""

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

import db_store
from dependencies import check_not_occupied_by_task, valid_ranking
from ranking_utils import (
    COLLECTION_TYPE_RANKING,
    TOP250_OPTIONS_CACHE_KEY,
    TOP250_SOURCE_URL,
    parse_top250_options,
    ranking_url,
)
from schemas import TaskConfig
from services import magnet_service
from services.task_service import TaskConfigError, create_task_from_config, get_runtime_for_request
from spider_engine import DATA_DIR
from spider_engine import fetch_html
from storage_utils import UnsafeFilenameError
from utils import build_proxy_dict, csv_download_response, parse_tag_filter, runtime_headers

router = APIRouter()


def _invalid_ranking_response():
    return JSONResponse(status_code=404, content={"code": 404, "msg": "排行榜不存在"})


def _magnet_check_error_response(error: magnet_service.MagnetCheckError):
    content = {"code": error.status_code, "msg": error.msg}
    if error.data:
        content["data"] = error.data
    return JSONResponse(status_code=error.status_code, content=content)


def _ranking_filename(category, period):
    return db_store.get_ranking_collection_filename(category, period)


def _merge_ranking_options(*option_groups):
    merged = []
    seen = set()
    for options in option_groups:
        for item in options or []:
            key = str((item or {}).get("key") or "").strip()
            label = str((item or {}).get("label") or "").strip()
            if not key or key in seen:
                continue
            merged.append({"key": key, "label": label or key})
            seen.add(key)
    return merged


def _top250_options_payload(options, cache=None, stale=False, msg="", error_type=""):
    return {
        "options": options,
        "source_url": (cache or {}).get("source_url") or TOP250_SOURCE_URL,
        "updated_at": (cache or {}).get("updated_at") or 0,
        "stale": stale,
        "msg": msg,
        "error_type": error_type,
    }


class Top250OptionError(Exception):
    def __init__(self, error_type, message):
        super().__init__(message)
        self.error_type = error_type
        self.message = message


def _classify_top250_exception(error):
    if isinstance(error, Top250OptionError):
        return error.error_type, error.message
    text = str(error)
    lowered = text.lower()
    if any(token in lowered for token in ["timeout", "timed out", "proxy", "dns", "connection", "connect", "tls", "ssl"]):
        return "network", f"网络或代理异常：{text}"
    return "unknown", text or "未知错误"


@router.get("/api/rankings/top250/options")
def get_top250_options(refresh: bool = False):
    cache = db_store.get_ranking_option_cache(TOP250_OPTIONS_CACHE_KEY)
    local_options = db_store.get_local_top250_options()
    cached_options = cache["options"] if cache else []
    fallback_options = _merge_ranking_options(cached_options, local_options)
    if not refresh:
        return {"code": 200, "data": _top250_options_payload(fallback_options, cache)}

    try:
        runtime = get_runtime_for_request()
        response = fetch_html(
            TOP250_SOURCE_URL,
            headers=runtime_headers(runtime),
            proxies=build_proxy_dict(runtime.get("proxies")),
        )
        if response.status_code in [401, 403, 503]:
            raise Top250OptionError("auth", f"状态码 {response.status_code}，Cookie 可能失效")
        if response.status_code != 200:
            raise Top250OptionError("unknown", f"状态码 {response.status_code}")
        remote_options = parse_top250_options(response.text)
        if not remote_options:
            raise Top250OptionError("parse", "未找到 TOP250 分类选项")
        db_store.save_ranking_option_cache(TOP250_OPTIONS_CACHE_KEY, remote_options, TOP250_SOURCE_URL)
        fresh_cache = db_store.get_ranking_option_cache(TOP250_OPTIONS_CACHE_KEY)
        return {
            "code": 200,
            "data": _top250_options_payload(
                _merge_ranking_options(remote_options, local_options),
                fresh_cache,
            ),
        }
    except Exception as e:
        error_type, message = _classify_top250_exception(e)
        if fallback_options:
            return {
                "code": 200,
                "data": _top250_options_payload(
                    fallback_options,
                    cache,
                    True,
                    f"刷新分类失败，已使用本地缓存: {message}",
                    error_type,
                ),
            }
        return JSONResponse(
            status_code=502,
            content={"code": 502, "msg": f"TOP250 分类加载失败: {message}", "error_type": error_type},
        )


@router.get("/api/rankings/{category}/{period}/movies")
def get_ranking_movies(category: str, period: str):
    category, period = valid_ranking(category, period)
    data = db_store.get_ranking_movies(category, period)
    data["collection_filename"] = _ranking_filename(category, period)
    return {"code": 200, "data": data}


@router.post("/api/rankings/{category}/{period}/update")
def create_ranking_update_task(category: str, period: str):
    url = ranking_url(category, period)
    if not url:
        return _invalid_ranking_response()
    try:
        return create_task_from_config(
            TaskConfig(
                start_url=url,
                crawl_mode="overwrite",
                collection_type=COLLECTION_TYPE_RANKING,
                ranking_category=category,
                ranking_period=period,
            )
        )
    except TaskConfigError as e:
        return JSONResponse(status_code=e.status_code, content={"code": e.status_code, "msg": e.msg, **e.extra})


@router.get("/api/rankings/{category}/{period}/magnets")
def get_ranking_magnets(category: str, period: str, tags: str = None, exclude_tags: str = None):
    category, period = valid_ranking(category, period)
    try:
        links = db_store.get_ranking_magnet_links(category, period, parse_tag_filter(tags), parse_tag_filter(exclude_tags))
    except Exception as e:
        return JSONResponse(status_code=500, content={"code": 500, "msg": f"读取数据出错: {str(e)}"})
    return {"code": 200, "data": links}


@router.get("/api/rankings/{category}/{period}/download")
def download_ranking_csv(category: str, period: str, tags: str = None, exclude_tags: str = None):
    category, period = valid_ranking(category, period)
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
    return csv_download_response(csv_bytes, safe_name)


@router.post("/api/rankings/{category}/{period}/clear")
def clear_ranking_collection(category: str, period: str):
    category, period = valid_ranking(category, period)
    filename = _ranking_filename(category, period)
    if not filename:
        return {"code": 200, "msg": "榜单已为空"}

    try:
        check_not_occupied_by_task(filename)
    except HTTPException:
        return JSONResponse(status_code=400, content={"code": 400, "msg": "榜单正在被任务占用"})

    deleted, missing = db_store.delete_collections([filename], DATA_DIR)
    if deleted:
        return {"code": 200, "msg": "清空成功"}
    return JSONResponse(status_code=404, content={"code": 404, "msg": f"找不到该榜单: {', '.join(missing)}"})


@router.post("/api/rankings/{category}/{period}/check_magnets")
def check_ranking_magnets(category: str, period: str, failed_only: bool = False):
    category, period = valid_ranking(category, period)
    magnets = []
    for movie_id in db_store.get_ranking_movie_ids(category, period):
        magnets.extend(db_store.get_movie_magnets(movie_id))
    try:
        return magnet_service.start_magnet_check(
            "ranking",
            f"{category}:{period}",
            magnets,
            "该榜单没有候选磁力",
            failed_only,
        )
    except magnet_service.MagnetCheckError as e:
        return _magnet_check_error_response(e)
