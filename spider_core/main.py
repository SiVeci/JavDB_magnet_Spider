from datetime import datetime
import csv
import os
import secrets
import threading
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from bs4 import BeautifulSoup
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from pydantic import BaseModel

from spider_engine import DATA_DIR, STATUS_FILE, STOP_EVENT, fetch_html, run_spider
from storage_utils import (
    UnsafeFilenameError,
    atomic_write_json,
    get_safe_csv_path,
    normalize_csv_filename,
    read_json_file,
)


app = FastAPI()

APP_VERSION = os.getenv("JAVDB_SPIDER_VERSION", "dev-local")
CONFIG_FILE = os.path.join(DATA_DIR, "task_config.json")
AUTH_HEADER = "X-JavDB-Token"
PUBLIC_API_PATHS = {"/api/version"}


class TaskConfig(BaseModel):
    start_url: str
    cookie: str
    user_agent: str
    filename: str = ""
    proxies: str = None


class ResumeConfig(BaseModel):
    cookie: str


class ModeConfig(BaseModel):
    mode: str


class TagConfigRequest(BaseModel):
    url: str
    cookie: str
    user_agent: str
    proxies: str = None


class DeleteRequest(BaseModel):
    filenames: list[str]


def _env_truthy(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def is_auth_required() -> bool:
    return _env_truthy("JAVDB_AUTH_REQUIRED") or bool(os.getenv("JAVDB_AUTH_TOKEN", "").strip())


def is_api_authorized(provided_token: str | None) -> bool:
    expected_token = os.getenv("JAVDB_AUTH_TOKEN", "").strip()
    if not is_auth_required():
        return True
    if not expected_token or not provided_token:
        return False
    return secrets.compare_digest(provided_token, expected_token)


@app.middleware("http")
async def require_api_token(request: Request, call_next):
    path = request.url.path
    if path.startswith("/api/") and path not in PUBLIC_API_PATHS:
        if not is_api_authorized(request.headers.get(AUTH_HEADER)):
            return JSONResponse(
                status_code=401,
                content={"code": 401, "msg": "访问令牌缺失或无效"},
            )
    return await call_next(request)


def get_safe_path(filename: str) -> str | None:
    try:
        target_path, _ = get_safe_csv_path(DATA_DIR, filename)
        return target_path
    except UnsafeFilenameError:
        return None


def get_safe_name(filename: str) -> str | None:
    try:
        _, safe_name = get_safe_csv_path(DATA_DIR, filename)
        return safe_name
    except UnsafeFilenameError:
        return None


def ensure_zh_locale(url: str) -> str:
    parsed = urlparse(url)
    params = parse_qs(parsed.query, keep_blank_values=True)
    params["locale"] = ["zh"]
    query = urlencode(params, doseq=True)
    return urlunparse(parsed._replace(query=query))


def start_spider_thread(*args):
    thread = threading.Thread(target=run_spider, args=args)
    thread.start()
    return thread


@app.get("/api/version")
def get_version():
    return {"version": APP_VERSION, "auth_required": is_auth_required()}


@app.post("/api/stop")
def stop_task():
    STOP_EVENT.set()
    return {"code": 200, "msg": "停止信号已发送，请等待当前请求完成后安全退出。"}


@app.post("/api/start")
def start_task(config: TaskConfig):
    try:
        status = read_json_file(STATUS_FILE, default={})
        if status and status.get("state") == "running":
            return {"code": 400, "msg": "当前已有任务正在运行，请等待完成后再提交。"}
    except Exception:
        pass

    try:
        target_filename = normalize_csv_filename(config.filename, allow_empty=True)
    except UnsafeFilenameError as e:
        return {"code": 400, "msg": f"文件名非法: {str(e)}"}

    task_data = config.dict()
    task_data["final_filename"] = target_filename
    task_data["crawl_mode"] = None
    atomic_write_json(CONFIG_FILE, task_data)

    initial_status = {
        "state": "running",
        "progress": "0/0",
        "current": "系统初始化",
        "logs": ["系统已分配资源，正在启动爬虫引擎..."],
    }
    if target_filename:
        initial_status["final_filename"] = target_filename
    atomic_write_json(STATUS_FILE, initial_status, indent=2)

    start_spider_thread(
        config.start_url,
        config.cookie,
        config.user_agent,
        target_filename,
        config.proxies,
        False,
        None,
    )
    return {"code": 200, "msg": "任务已启动！"}


@app.post("/api/resume")
def resume_task(r_config: ResumeConfig):
    if not os.path.exists(CONFIG_FILE):
        return {"code": 400, "msg": "找不到原始任务配置，无法恢复。"}

    old_config = read_json_file(CONFIG_FILE, default={})
    old_config["cookie"] = r_config.cookie
    atomic_write_json(CONFIG_FILE, old_config)

    resume_status = {
        "state": "running",
        "progress": "恢复中",
        "current": "系统唤醒",
        "logs": ["已接收新凭据，正在从断点处唤醒引擎..."],
    }
    atomic_write_json(STATUS_FILE, resume_status, indent=2)

    start_spider_thread(
        old_config["start_url"],
        old_config["cookie"],
        old_config["user_agent"],
        old_config.get("final_filename", ""),
        old_config.get("proxies"),
        True,
        old_config.get("crawl_mode"),
    )
    return {"code": 200, "msg": "任务已成功从断点处恢复运行。"}


@app.post("/api/set_mode")
def set_mode(m_config: ModeConfig):
    if m_config.mode not in {"incremental", "overwrite"}:
        return {"code": 400, "msg": "爬取模式非法。"}
    if not os.path.exists(CONFIG_FILE):
        return {"code": 400, "msg": "找不到原始任务配置，无法恢复。"}

    old_config = read_json_file(CONFIG_FILE, default={})
    final_filename = old_config.get("final_filename", "")
    try:
        status = read_json_file(STATUS_FILE, default={})
        if status.get("final_filename"):
            final_filename = status.get("final_filename")
    except Exception:
        pass

    try:
        final_filename = normalize_csv_filename(final_filename)
    except UnsafeFilenameError as e:
        return {"code": 400, "msg": f"文件名非法: {str(e)}"}

    old_config["final_filename"] = final_filename
    old_config["crawl_mode"] = m_config.mode
    atomic_write_json(CONFIG_FILE, old_config)

    mode_name = "快速增量" if m_config.mode == "incremental" else "覆盖重爬"
    resume_status = {
        "state": "running",
        "progress": "恢复中",
        "current": "模式确认",
        "logs": [f"已选择模式: {mode_name}，正在继续任务..."],
        "final_filename": final_filename,
    }
    atomic_write_json(STATUS_FILE, resume_status, indent=2)

    start_spider_thread(
        old_config["start_url"],
        old_config["cookie"],
        old_config["user_agent"],
        final_filename,
        old_config.get("proxies"),
        True,
        m_config.mode,
    )
    return {"code": 200, "msg": "已应用爬取模式。"}


@app.get("/api/status")
def get_status():
    if not os.path.exists(STATUS_FILE):
        return {"state": "idle", "progress": "0/0", "current": "-", "logs": ["等待任务启动..."]}
    try:
        return read_json_file(STATUS_FILE, default={})
    except Exception:
        return {"state": "syncing", "progress": "IO同步", "current": "-", "logs": ["磁盘 IO 同步中，请稍候..."]}


FILE_COUNT_CACHE = {}


@app.get("/api/history")
def get_history():
    files_info = []
    if os.path.exists(DATA_DIR):
        for filename in os.listdir(DATA_DIR):
            if not filename.endswith(".csv"):
                continue
            path = get_safe_path(filename)
            if not path:
                continue
            stats = os.stat(path)
            mtime = stats.st_mtime
            ctime = stats.st_ctime
            if filename in FILE_COUNT_CACHE and FILE_COUNT_CACHE[filename]["mtime"] == mtime:
                count = FILE_COUNT_CACHE[filename]["count"]
            else:
                try:
                    with open(path, "r", encoding="utf-8-sig") as csv_f:
                        count = max(0, sum(1 for _ in csv_f) - 1)
                    FILE_COUNT_CACHE[filename] = {"mtime": mtime, "count": count}
                except Exception:
                    count = 0

            files_info.append(
                {
                    "name": filename,
                    "count": count,
                    "time": datetime.fromtimestamp(ctime).strftime("%Y-%m-%d %H:%M:%S"),
                    "timestamp": ctime,
                }
            )
    files_info.sort(key=lambda x: x["timestamp"], reverse=True)
    return {"code": 200, "data": files_info}


@app.post("/api/delete")
def delete_history(req: DeleteRequest):
    success_count = 0
    fail_count = 0
    fail_reasons = []

    active_file = None
    try:
        status = read_json_file(STATUS_FILE, default={})
        if status.get("state") == "running":
            active_file = get_safe_name(status.get("final_filename"))
    except Exception:
        pass

    for filename in req.filenames:
        target_path = get_safe_path(filename)
        safe_name = get_safe_name(filename)
        if not target_path or not os.path.exists(target_path):
            fail_count += 1
            fail_reasons.append(f"{filename}(不存在或非法)")
            continue

        if active_file == safe_name:
            fail_count += 1
            fail_reasons.append(f"{filename}(被占用)")
            continue

        try:
            os.remove(target_path)
            FILE_COUNT_CACHE.pop(safe_name, None)
            success_count += 1
        except OSError:
            fail_count += 1
            fail_reasons.append(f"{filename}(系统占用)")

    if fail_count == 0:
        return {"code": 200, "msg": "删除成功"}

    reason_str = ", ".join(fail_reasons[:3]) + ("..." if len(fail_reasons) > 3 else "")
    return {
        "code": 200 if success_count > 0 else 400,
        "msg": f"成功 {success_count} 个，失败 {fail_count} 个 [{reason_str}]",
    }


@app.get("/api/download")
def download_csv(name: str = None):
    if not name:
        return JSONResponse(status_code=400, content={"code": 400, "msg": "未指定文件名参数"})

    file_path = get_safe_path(name)
    safe_name = get_safe_name(name)
    if not file_path:
        return JSONResponse(status_code=400, content={"code": 400, "msg": "文件名非法"})
    if os.path.exists(file_path):
        return FileResponse(file_path, media_type="text/csv", filename=safe_name)
    return JSONResponse(status_code=404, content={"code": 404, "msg": "找不到该文件"})


@app.get("/")
def read_root():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    html_path = os.path.join(base_dir, "frontend", "index.html")
    if os.path.exists(html_path):
        with open(html_path, "r", encoding="utf-8") as f:
            return HTMLResponse(f.read())
    return HTMLResponse(f"<h1>找不到前端页面，系统当前寻找的绝对路径是: {html_path}</h1>")


@app.get("/favicon.png")
def get_favicon():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(base_dir, "frontend", "favicon.png")
    if os.path.exists(file_path):
        return FileResponse(file_path, media_type="image/png")
    return {"error": f"找不到图标文件: {file_path}"}


@app.get("/api/magnets")
def get_magnets(name: str = None):
    if not name:
        return {"code": 400, "msg": "未指定文件名参数"}

    file_path = get_safe_path(name)
    if not file_path:
        return {"code": 400, "msg": "文件名非法"}
    if not os.path.exists(file_path):
        return {"code": 404, "msg": "找不到该文件"}

    magnets = []
    try:
        with open(file_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if "磁力链接" in row and row["磁力链接"]:
                    magnets.append(row["磁力链接"])
        return {"code": 200, "data": magnets}
    except Exception as e:
        return {"code": 500, "msg": f"读取文件出错: {str(e)}"}


@app.post("/api/clear_logs")
def clear_logs():
    try:
        status = read_json_file(STATUS_FILE, default={})
        if status.get("state") == "running":
            return {"code": 400, "msg": "任务运行中，请先停止后再清除记录。"}
    except Exception:
        pass

    empty_status = {
        "state": "idle",
        "progress": "0/0",
        "current": "-",
        "logs": ["记录已安全清除。"],
    }
    atomic_write_json(STATUS_FILE, empty_status, indent=2)
    return {"code": 200, "msg": "记录已成功清除。"}


@app.post("/api/get_tags")
def get_tags(req: TagConfigRequest):
    try:
        base_url = ensure_zh_locale(req.url)
        headers = {"User-Agent": req.user_agent, "Cookie": req.cookie}
        proxy_dict = {"http": req.proxies, "https": req.proxies} if req.proxies else None
        response = fetch_html(base_url, headers=headers, proxies=proxy_dict)
        if response.status_code != 200:
            return {"code": response.status_code, "msg": f"请求失败，状态码: {response.status_code}"}

        soup = BeautifulSoup(response.text, "html.parser")
        tags_div = soup.select_one(".actor-tags .content")
        if not tags_div:
            return {"code": 404, "msg": "未在页面中找到标签区域。"}

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
        return {"code": 500, "msg": f"解析标签发生异常: {str(e)}"}


def start_server():
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
