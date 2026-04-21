from fastapi import FastAPI
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel
from datetime import datetime
import threading
import json
import os
import csv
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs

# 【关键修改】去掉了 curl_cffi，新增导入了我们自己写的 fetch_html
from spider_engine import run_spider, DATA_DIR, STATUS_FILE, STOP_EVENT, fetch_html

app = FastAPI()

# 定义配置文件路径
CONFIG_FILE = os.path.join(DATA_DIR, 'task_config.json')

class TaskConfig(BaseModel):
    start_url: str
    cookie: str
    user_agent: str
    filename: str = ""
    proxies: str = None

class ResumeConfig(BaseModel):
    cookie: str

class TagConfigRequest(BaseModel):
    url: str
    cookie: str
    user_agent: str

@app.post("/api/stop")
def stop_task():
    STOP_EVENT.set()
    return {"code": 200, "msg": "停止信号已发送，请等待当前单条抓取完成后安全退出。"}

@app.post("/api/start")
def start_task(config: TaskConfig):
    if os.path.exists(STATUS_FILE):
        try:
            with open(STATUS_FILE, 'r', encoding='utf-8') as f:
                status = json.load(f)
                if status.get("state") == "running":
                    return {"code": 400, "msg": "当前已有任务正在运行，请等待完成后再提交！"}
        except:
            pass

    target_filename = config.filename.strip()
    if not target_filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        target_filename = f"javdb_{timestamp}.csv"
    else:
        if not target_filename.lower().endswith(".csv"):
            target_filename += ".csv"

    task_data = config.dict()
    task_data['final_filename'] = target_filename
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(task_data, f, ensure_ascii=False)

    initial_status = {
        "state": "running",
        "progress": "0/0",
        "current": "系统初始化",
        "logs": ["系统已分配资源，正在启动爬虫引擎..."]
    }
    with open(STATUS_FILE, 'w', encoding='utf-8') as f:
        json.dump(initial_status, f, ensure_ascii=False, indent=2)

    thread = threading.Thread(
        target=run_spider,
        args=(config.start_url, config.cookie, config.user_agent, target_filename, config.proxies, False)
    )
    thread.start()
    return {"code": 200, "msg": f"任务已启动，文件将保存为: {target_filename}"}

@app.post("/api/resume")
def resume_task(r_config: ResumeConfig):
    if not os.path.exists(CONFIG_FILE):
        return {"code": 400, "msg": "找不到原始任务配置，无法恢复！"}

    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        old_config = json.load(f)

    old_config['cookie'] = r_config.cookie

    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(old_config, f, ensure_ascii=False)

    resume_status = {
        "state": "running",
        "progress": "恢复中",
        "current": "系统唤醒",
        "logs": ["已接收新凭据，正在从断点处唤醒引擎..."]
    }
    with open(STATUS_FILE, 'w', encoding='utf-8') as f:
        json.dump(resume_status, f, ensure_ascii=False, indent=2)

    thread = threading.Thread(
        target=run_spider,
        args=(old_config['start_url'], old_config['cookie'], old_config['user_agent'], old_config['final_filename'], old_config['proxies'], True)
    )
    thread.start()
    return {"code": 200, "msg": "任务已成功从断点处恢复运行"}

@app.get("/api/status")
def get_status():
    if not os.path.exists(STATUS_FILE):
        return {"state": "idle", "progress": "0/0", "current": "-", "logs": ["等待任务启动..."]}
    try:
        with open(STATUS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except:
        return {"state": "syncing", "progress": "IO同步", "current": "-", "logs": ["磁盘 IO 同步中，请稍候..."]}

@app.get("/api/download")
def download_csv(name: str = None):
    if not name: return {"error": "未指定文件名参数"}
    file_path = os.path.join(DATA_DIR, name)
    if os.path.exists(file_path):
        return FileResponse(file_path, media_type="text/csv", filename=name)
    return {"error": "找不到该文件"}

@app.get("/")
def read_root():
    # 获取当前 main.py 所在的绝对目录
    base_dir = os.path.dirname(os.path.abspath(__file__))
    # 动态拼接出 frontend 文件夹的绝对路径
    html_path = os.path.join(base_dir, "frontend", "index.html")

    if os.path.exists(html_path):
        with open(html_path, 'r', encoding='utf-8') as f:
            return HTMLResponse(f.read())
    # 顺便把真实的寻址路径打印出来，方便万一报错时排查
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
    if not name: return {"code": 400, "msg": "未指定文件名参数"}
    file_path = os.path.join(DATA_DIR, name)
    if not os.path.exists(file_path): return {"code": 404, "msg": "找不到该文件"}
    magnets = []
    try:
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if '磁力链接' in row and row['磁力链接']:
                    magnets.append(row['磁力链接'])
        return {"code": 200, "data": magnets}
    except Exception as e:
        return {"code": 500, "msg": f"读取文件出错: {str(e)}"}

@app.post("/api/clear_logs")
def clear_logs():
    if os.path.exists(STATUS_FILE):
        try:
            with open(STATUS_FILE, 'r', encoding='utf-8') as f:
                status = json.load(f)
                if status.get("state") == "running":
                    return {"code": 400, "msg": "任务运行中，请先停止后再清除记录！"}
        except:
            pass

    empty_status = {
        "state": "idle",
        "progress": "0/0",
        "current": "-",
        "logs": ["记录已安全清除。"]
    }
    with open(STATUS_FILE, 'w', encoding='utf-8') as f:
        json.dump(empty_status, f, ensure_ascii=False, indent=2)
    return {"code": 200, "msg": "记录已成功清除"}

@app.post("/api/get_tags")
def get_tags(req: TagConfigRequest):
    try:
        base_url = req.url.split('?')[0]
        headers = {"User-Agent": req.user_agent, "Cookie": req.cookie}

        # 【关键修改】不再使用 requests，直接调用封装好的环境自适应网关
        response = fetch_html(base_url, headers=headers)
        if response.status_code != 200:
            return {"code": response.status_code, "msg": f"请求失败，状态码: {response.status_code}"}

        soup = BeautifulSoup(response.text, 'html.parser')
        tags_div = soup.select_one('.actor-tags .content')

        if not tags_div:
            return {"code": 404, "msg": "未在页面中找到标签区域。"}

        tags = []
        for a in tags_div.find_all('a', class_='tag'):
            name = a.text.strip()
            href = a.get('href', '')
            parsed_url = urlparse(href)
            params = parse_qs(parsed_url.query)
            if 't' in params:
                tag_value = params['t'][0]
                if tag_value: tags.append({"name": name, "value": tag_value})

        return {"code": 200, "data": tags}
    except Exception as e:
        return {"code": 500, "msg": f"解析标签发生异常: {str(e)}"}

# ================= 安卓端启动入口 =================
def start_server():
    """供 Android 端调用的启动入口"""
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)