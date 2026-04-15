from fastapi import FastAPI
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel
from datetime import datetime
import threading
import json
import os
from spider_engine import run_spider, DATA_DIR, STATUS_FILE, STOP_EVENT
import csv

# 从我们的爬虫引擎导入需要的函数和常量
from spider_engine import run_spider, DATA_DIR, STATUS_FILE

app = FastAPI()

# 定义配置文件路径，用于断点续传时恢复基础配置
CONFIG_FILE = os.path.join(DATA_DIR, 'task_config.json')

# 定义接收前端全新任务数据的结构体
class TaskConfig(BaseModel):
    start_url: str
    cookie: str
    user_agent: str
    filename: str = ""  # 用户自定义文件名
    proxies: str = None # 代理设置

# 定义接收前端续传任务数据的结构体
class ResumeConfig(BaseModel):
    cookie: str

# ================= API 路由 =================
@app.post("/api/stop")
def stop_task():
    """强行终止任务"""
    STOP_EVENT.set()
    return {"code": 200, "msg": "停止信号已发送，请等待当前单条抓取完成后安全退出。"}

@app.post("/api/start")
def start_task(config: TaskConfig):
    """接收前端全新配置，启动后台爬虫线程"""
    
    # 1. 检查是否已经在运行
    if os.path.exists(STATUS_FILE):
        try:
            with open(STATUS_FILE, 'r', encoding='utf-8') as f:
                status = json.load(f)
                if status.get("state") == "running":
                    return {"code": 400, "msg": "当前已有任务正在运行，请等待完成后再提交！"}
        except:
            pass
            
    # 2. 处理文件名逻辑 (留空则使用时间戳)
    target_filename = config.filename.strip()
    if not target_filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        target_filename = f"javdb_{timestamp}.csv"
    else:
        if not target_filename.lower().endswith(".csv"):
            target_filename += ".csv"

    # 3. 将配置存入本地 JSON，为后续可能的断点续传做准备
    task_data = config.dict()
    task_data['final_filename'] = target_filename # 记录最终确定的文件名
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(task_data, f, ensure_ascii=False)
        
    # 4. 启动独立线程运行爬虫，不阻塞 API
    thread = threading.Thread(
        target=run_spider, 
        args=(
            config.start_url, 
            config.cookie, 
            config.user_agent, 
            target_filename, 
            config.proxies, 
            False # False 表示全新启动，非续传
        )
    )
    thread.start()
    return {"code": 200, "msg": f"任务已启动，文件将保存为: {target_filename}"}

@app.post("/api/resume")
def resume_task(r_config: ResumeConfig):
    """接收新 Cookie，唤醒因拦截而挂起的任务"""
    
    if not os.path.exists(CONFIG_FILE):
        return {"code": 400, "msg": "找不到原始任务配置，无法恢复！"}
        
    # 读取原始配置
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        old_config = json.load(f)
        
    # 替换为新传入的 Cookie，其他配置（如 URL/UA/文件名/代理）保持不变
    old_config['cookie'] = r_config.cookie
    
    # 将更新后的配置重新写回文件
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(old_config, f, ensure_ascii=False)
        
    # 重新触发引擎，开启续传
    thread = threading.Thread(
        target=run_spider, 
        args=(
            old_config['start_url'], 
            old_config['cookie'], 
            old_config['user_agent'], 
            old_config['final_filename'], 
            old_config['proxies'], 
            True # True 表示这是断点续传
        )
    )
    thread.start()
    return {"code": 200, "msg": "任务已成功从断点处恢复运行"}

@app.get("/api/status")
def get_status():
    """供前端定时轮询获取最新进度"""
    if not os.path.exists(STATUS_FILE):
        return {"state": "idle", "progress": "0/0", "current": "-", "logs": ["等待任务启动..."]}
        
    try:
        with open(STATUS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except:
        return {"state": "error", "progress": "-", "current": "-", "logs": ["状态文件读取异常"]}

@app.get("/api/download")
def download_csv(name: str = None):
    """提供最终的 CSV 文件下载"""
    if not name:
        return {"error": "未指定文件名参数"}
        
    file_path = os.path.join(DATA_DIR, name)
    if os.path.exists(file_path):
        return FileResponse(file_path, media_type="text/csv", filename=name)
    return {"error": "找不到该文件"}

# ================= 静态页面托管 =================

@app.get("/")
def read_root():
    """将前端的 HTML 返回给根路径"""
    html_path = os.path.join("frontend", "index.html")
    if os.path.exists(html_path):
        with open(html_path, 'r', encoding='utf-8') as f:
            return HTMLResponse(f.read())
    return HTMLResponse("<h1>找不到 frontend/index.html 文件，请检查目录结构。</h1>")

@app.get("/api/magnets")
def get_magnets(name: str = None):
    """提取 CSV 中的纯磁力链接，供前端一键复制"""
    if not name:
        return {"code": 400, "msg": "未指定文件名参数"}
        
    file_path = os.path.join(DATA_DIR, name)
    if not os.path.exists(file_path):
        return {"code": 404, "msg": "找不到该文件"}
        
    magnets = []
    try:
        # 打开生成的 CSV 文件，提取磁力链接列
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
    """清除运行记录和日志，保护隐私"""
    # 检查当前是否在运行，运行中禁止清除
    if os.path.exists(STATUS_FILE):
        try:
            with open(STATUS_FILE, 'r', encoding='utf-8') as f:
                status = json.load(f)
                if status.get("state") == "running":
                    return {"code": 400, "msg": "任务运行中，请先停止后再清除记录！"}
        except:
            pass

    # 重置状态文件，但不触碰 task_config.json (保留 Cookie)
    empty_status = {
        "state": "idle",
        "progress": "0/0",
        "current": "-",
        "logs": ["记录已安全清除。"]
    }
    with open(STATUS_FILE, 'w', encoding='utf-8') as f:
        json.dump(empty_status, f, ensure_ascii=False, indent=2)
    
    return {"code": 200, "msg": "记录已成功清除"}