"""Task configuration, preparation, and serialization logic."""

import json
import time

import db_store
from bs4 import BeautifulSoup
from ranking_utils import COLLECTION_TYPE_ACTOR, parse_ranking_url
from schemas import TaskConfig
from spider_engine import fetch_html, get_android_javdb_cookie
from storage_utils import UnsafeFilenameError, make_csv_filename_from_label, normalize_csv_filename
from utils import build_proxy_dict, ensure_zh_locale, runtime_headers


class TaskConfigError(Exception):
    """Task configuration error with HTTP response metadata."""

    def __init__(self, status_code: int, msg: str, **extra):
        super().__init__(msg)
        self.status_code = status_code
        self.msg = msg
        self.extra = extra


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

    return make_csv_filename_from_label(f"javdb_{time.strftime('%Y%m%d_%H%M%S')}")

def prepare_task_config(config: TaskConfig):
    if config.crawl_mode and config.crawl_mode not in {"incremental", "overwrite"}:
        raise TaskConfigError(400, "爬取模式非法")
    try:
        requested_filename = normalize_csv_filename(config.filename, allow_empty=True)
    except UnsafeFilenameError as e:
        raise TaskConfigError(400, f"文件名非法: {str(e)}")

    save_runtime_from_payload(config)
    runtime = get_runtime_for_request()
    if not runtime.get("cookie"):
        raise TaskConfigError(400, "Cookie 不能为空，Android 端请先在内置浏览器登录 JavDB")

    start_url = ensure_zh_locale(config.start_url)
    ranking_meta = parse_ranking_url(start_url)
    try:
        response = fetch_html(
            start_url,
            headers=runtime_headers(runtime),
            proxies=build_proxy_dict(runtime.get("proxies")),
        )
    except Exception as e:
        # 缃戠粶灞傚紓甯革紙TLS 鎻℃墜澶辫触銆佽秴鏃躲€佽繛鎺ヨ鎷掋€佷唬鐞嗕笉鍙敤绛夛級銆?
        # 鏀跺彛涓哄弸濂藉搷搴旓紝閬垮厤寮傚父鍐掓场鍒?ASGI 灞傛墦鍑烘暣椤?traceback銆?
        raise TaskConfigError(502, f"入队预检查请求失败: {str(e)}")
    if response.status_code != 200:
        raise TaskConfigError(
            response.status_code if response.status_code >= 400 else 400,
            f"入队预检查失败，状态码: {response.status_code}",
        )

    soup = BeautifulSoup(response.text, "html.parser")
    try:
        if ranking_meta:
            final_filename = (
                db_store.get_ranking_collection_filename(
                    ranking_meta["ranking_category"],
                    ranking_meta["ranking_period"],
                )
                or ranking_meta["filename"]
            )
        else:
            final_filename = infer_task_filename(start_url, requested_filename, soup)
    except UnsafeFilenameError as e:
        raise TaskConfigError(400, f"文件名非法: {str(e)}")

    exists = db_store.collection_exists(final_filename)
    if exists and not config.crawl_mode:
        raise TaskConfigError(
            409,
            f"发现已有数据库集合：{final_filename}，请选择增量或覆盖。",
            needs_mode=True,
            filename=final_filename,
        )
    return {
        "start_url": start_url,
        "filename": final_filename,
        "crawl_mode": config.crawl_mode or "",
        "collection_type": ranking_meta["collection_type"] if ranking_meta else COLLECTION_TYPE_ACTOR,
        "ranking_category": ranking_meta["ranking_category"] if ranking_meta else "",
        "ranking_period": ranking_meta["ranking_period"] if ranking_meta else "",
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
        "collection_type": task.get("collection_type") or COLLECTION_TYPE_ACTOR,
        "ranking_category": task.get("ranking_category") or "",
        "ranking_period": task.get("ranking_period") or "",
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
    task_id = db_store.create_task(
        prepared["start_url"],
        filename=prepared["filename"],
        crawl_mode=prepared["crawl_mode"],
        collection_type=prepared["collection_type"],
        ranking_category=prepared["ranking_category"],
        ranking_period=prepared["ranking_period"],
    )
    return {"code": 200, "msg": "任务已加入队列", "data": {"task_id": task_id, "filename": prepared["filename"]}}

