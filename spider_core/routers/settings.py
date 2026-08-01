"""routers/settings — 版本、运行配置、任务状态、日志清理、标签抓取端点。"""

from urllib.parse import parse_qs, urlparse

from bs4 import BeautifulSoup
from fastapi import APIRouter
from fastapi.responses import JSONResponse

import db_store
from app_config import APP_VERSION, is_auth_required
from schemas import RuntimeConfig, TagConfigRequest
from services.cookie_validation_service import validate_runtime_cookie
from services.queue_service import write_status_mirror
from services.task_service import get_runtime_for_request, task_to_response
from spider_engine import fetch_html
from utils import build_proxy_dict, ensure_zh_locale

router = APIRouter()


@router.get("/api/version")
def get_version():
    return {"code": 200, "data": {"version": APP_VERSION, "auth_required": is_auth_required()}}


@router.get("/api/runtime_config")
def get_runtime_config():
    runtime = db_store.get_runtime_config(include_cookie=False)
    return {"code": 200, "data": runtime}


@router.post("/api/runtime_config")
def set_runtime_config(config: RuntimeConfig):
    try:
        db_store.save_runtime_config(
            cookie=config.cookie if config.cookie else None,
            remember_cookie=config.remember_cookie,
            user_agent=config.user_agent,
            proxies=config.proxies,
            trackers=config.trackers,
            cookie_source="manual" if config.cookie else None,
            cookie_status="unverified" if config.cookie else None,
            magnet_score_100_condition=config.magnet_score_100_condition,
            magnet_score_10_condition=config.magnet_score_10_condition,
            magnet_score_1_condition=config.magnet_score_1_condition,
        )
    except ValueError:
        return JSONResponse(
            status_code=400,
            content={"code": 400, "msg": "磁力评分条件必须从四个支持项中选择三个且不能重复"},
        )
    return {"code": 200, "msg": "运行配置已保存"}


@router.post("/api/auth/check_cookie")
def check_cookie():
    result = validate_runtime_cookie(update_runtime=True)
    return {"code": 200, "data": result, "msg": result["message"]}


@router.get("/api/status")
def get_status():
    task = db_store.get_current_task()
    if not task:
        return {"state": "idle", "progress": "0/0", "current": "-", "logs": ["等待任务启动..."]}
    return task_to_response(task, include_logs=True)


@router.post("/api/clear_logs")
def clear_logs():
    active_task = db_store.get_active_task()
    if active_task:
        return JSONResponse(status_code=400, content={"code": 400, "msg": "任务运行中，请先暂停或取消后再清除记录"})
    write_status_mirror(None)
    return {"code": 200, "msg": "记录已清除"}


@router.post("/api/get_tags")
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
        try:
            response = fetch_html(base_url, headers=headers, proxies=proxy_dict)
        except Exception as e:
            # 网络层异常（TLS 握手失败、超时、连接被拒、代理不可用等）。
            return JSONResponse(
                status_code=502,
                content={"code": 502, "msg": f"标签页请求失败：{str(e)}"},
            )
        if response.status_code != 200:
            return JSONResponse(
                status_code=response.status_code if response.status_code >= 400 else 400,
                content={"code": response.status_code, "msg": f"请求失败，状态码: {response.status_code}"},
            )

        soup = BeautifulSoup(response.text, "html.parser")
        tags_div = soup.select_one(".actor-tags .content")
        if not tags_div:
            return JSONResponse(status_code=404, content={"code": 404, "msg": "未在页面中找到标签区域"})

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
        return JSONResponse(status_code=500, content={"code": 500, "msg": f"解析标签发生异常: {str(e)}"})
