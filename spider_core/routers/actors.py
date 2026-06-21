"""routers/actors — 收藏演员清单的获取、刷新与一键加入任务队列端点。

清单获取/刷新使用当前配置的 JavDB Cookie；首次进入页面不自动请求远端（前端只读缓存）。
加入任务复用既有 create_task_from_config（预检查、命名、增量/覆盖 409、队列）。
"""

from urllib.parse import parse_qs, urlencode, urlparse

from fastapi import APIRouter
from fastapi.responses import JSONResponse

import db_store
from schemas import ActorRefreshRequest, ActorTaskRequest, TaskConfig
from services.actor_collection_service import (
    ALL_CATEGORY,
    ActorFetchError,
    categories_meta,
    refresh_all,
    refresh_category,
)
from services.task_service import TaskConfigError, create_task_from_config

router = APIRouter()


def _actor_fetch_error_response(err: ActorFetchError):
    # auth（Cookie 缺失/失效）用 400，避免前端把 401 当作访问令牌失效而锁定应用。
    status = 400 if err.kind == "auth" else 502
    return JSONResponse(
        status_code=status,
        content={"code": status, "msg": err.msg, "error_kind": err.kind},
    )


def _selected_tags(tags):
    """归一化前端传入的标签：仅保留含 value 的 {name,value}，去重保序。"""
    normalized = []
    seen = set()
    for item in tags or []:
        if not isinstance(item, dict):
            continue
        value = str(item.get("value") or "").strip()
        if not value or value in seen:
            continue
        normalized.append({"name": str(item.get("name") or "").strip(), "value": value})
        seen.add(value)
    return normalized


def build_actor_crawl_url(actor_url, tag_values):
    """以演员基础 URL 为起点按系统标签规则拼接爬取 URL（对齐前端 prepareActorUrl/buildActorUrl）。"""
    parsed = urlparse(actor_url)
    base = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    params = parse_qs(parsed.query, keep_blank_values=True)
    params["locale"] = ["zh"]
    params.setdefault("sort_type", ["0"])
    params.pop("page", None)
    params.pop("t", None)
    if tag_values:
        params["t"] = [",".join(tag_values)]
    return f"{base}?{urlencode(params, doseq=True)}"


@router.get("/api/actors")
def list_actors():
    return {
        "code": 200,
        "data": {
            "categories": categories_meta(),
            "actors": db_store.list_collection_actors(),
        },
    }


@router.post("/api/actors/refresh")
def refresh_actors(req: ActorRefreshRequest):
    category = (req.category or ALL_CATEGORY).strip()
    try:
        if category == ALL_CATEGORY:
            failed = refresh_all()["failed"]
        else:
            refresh_category(category)
            failed = []
    except ActorFetchError as e:
        return _actor_fetch_error_response(e)
    return {
        "code": 200,
        "msg": "刷新完成",
        "data": {
            "categories": categories_meta(),
            "actors": db_store.list_collection_actors(),
            "failed": failed,
        },
    }


@router.post("/api/actors/add_task")
def add_actor_task(req: ActorTaskRequest):
    actor = db_store.get_collection_actor(req.actor_id)
    if not actor:
        return JSONResponse(status_code=404, content={"code": 404, "msg": "收藏演员不存在，请先刷新清单"})

    tags = _selected_tags(req.tags)
    crawl_url = build_actor_crawl_url(actor["actor_url"], [t["value"] for t in tags])

    # 增量爬取提示：按 {actor_id} 判断是否已有数据集合（PRD §9）。
    existing = db_store.get_actor_collection_filename_by_actor_id(req.actor_id)
    if existing and not req.crawl_mode:
        return JSONResponse(
            status_code=409,
            content={
                "code": 409,
                "msg": f"发现已有数据库集合：{existing}，请选择增量或覆盖。",
                "needs_mode": True,
                "filename": existing,
                "actor_id": req.actor_id,
            },
        )

    # 保持现有“记住 Cookie”设置不被入队流程改写。
    runtime_cfg = db_store.get_runtime_config(include_cookie=False)
    config = TaskConfig(
        start_url=crawl_url,
        filename=existing or "",
        crawl_mode=req.crawl_mode or "",
        remember_cookie=bool(runtime_cfg.get("remember_cookie")),
    )
    try:
        result = create_task_from_config(config)
    except TaskConfigError as e:
        content = {"code": e.status_code, "msg": e.msg, **e.extra}
        if e.extra.get("needs_mode"):
            content["actor_id"] = req.actor_id
        return JSONResponse(status_code=e.status_code, content=content)

    # 仅在添加成功后记录最后一次标签（PRD §8.7/§8.8）。
    db_store.set_actor_last_task_tags(req.actor_id, tags)
    data = dict(result.get("data") or {})
    data["actor_id"] = req.actor_id
    data["tags"] = tags
    return {"code": 200, "msg": result.get("msg", "任务已加入队列"), "data": data}
