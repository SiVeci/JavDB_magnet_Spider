from curl_cffi import requests
from bs4 import BeautifulSoup
import urllib.parse
import re
import csv
import time
import os
import json
import threading
STOP_EVENT = threading.Event()
DATA_DIR = 'data'
os.makedirs(DATA_DIR, exist_ok=True)

STATUS_FILE = os.path.join(DATA_DIR, 'status.json')
OUTPUT_CSV = os.path.join(DATA_DIR, 'final_magnets.csv')
CHECKPOINT_FILE = os.path.join(DATA_DIR, 'checkpoint.json') # 新增：断点记忆文件

def update_status(state="idle", progress="", current="", log_msg=None, clear_log=False, final_filename=None):
    """
    状态更新中心：将当前进度和日志写入 JSON 文件，供前端网页读取
    新增 final_filename 参数，用于在任务完成时告诉前端去下载哪个文件
    """
    status_data = {"state": state, "progress": progress, "current": current, "logs": []}
    
    # 如果传入了文件名，记录到状态中
    if final_filename:
        status_data["final_filename"] = final_filename

    # 尝试读取旧状态，保留之前的日志和之前存入的文件名
    if not clear_log and os.path.exists(STATUS_FILE):
        try:
            with open(STATUS_FILE, 'r', encoding='utf-8') as f:
                old_data = json.load(f)
                status_data["logs"] = old_data.get("logs", [])[-20:] # 最多保留最后20条
                # 如果当前调用没有传文件名，但之前存过，就继承下来
                if not final_filename and "final_filename" in old_data:
                    status_data["final_filename"] = old_data["final_filename"]
        except:
            pass
            
    if log_msg:
        # 在日志前面加个时间戳
        time_str = time.strftime("%H:%M:%S", time.localtime())
        status_data["logs"].append(f"[{time_str}] {log_msg}")

    with open(STATUS_FILE, 'w', encoding='utf-8') as f:
        json.dump(status_data, f, ensure_ascii=False, indent=2)

def save_checkpoint(data):
    """保存当前的断点数据"""
    with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)

def load_checkpoint():
    """读取断点数据"""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return None

def parse_size(size_str):
    if not size_str: return 0.0
    match = re.search(r'([\d\.]+)\s*(GB|MB|KB)', size_str.upper())
    if match:
        val = float(match.group(1))
        if match.group(2) == 'GB': return val * 1024
        if match.group(2) == 'MB': return val
        if match.group(2) == 'KB': return val / 1024
    return 0.0

def evaluate_magnet(item_soup):
    magnet_a = item_soup.select_one('a[href^="magnet:"]')
    if not magnet_a: return None
    
    name_elem = item_soup.select_one('.name')
    name = name_elem.text.strip().lower() if name_elem else ''
    tags = [t.text.strip() for t in item_soup.select('.tags .tag')]
    date_elem = item_soup.select_one('.date .time')
    date_str = date_elem.text.strip() if date_elem else '1970-01-01'
    size_str = item_soup.select_one('.meta').text.strip() if item_soup.select_one('.meta') else ''
    
    has_sub = '-c' in name or 'chs' in name or '字幕' in tags
    has_uncensored = '-u' in name or 'uncensored' in name
    has_uc = '-uc' in name
    has_hd = '高清' in tags
    
    if has_uc or (has_sub and has_uncensored): rank = 5
    elif has_sub: rank = 4
    elif has_uncensored: rank = 3
    elif has_hd: rank = 2
    else: rank = 1
        
    return {
        'link': magnet_a.get('href'),
        'name': name_elem.text.strip() if name_elem else 'Unknown',
        'rank': rank, 'date': date_str, 'size_mb': parse_size(size_str)
    }

def run_spider(start_url, cookie, user_agent, output_filename, proxies_config=None, is_resume=False):
    # 任务开始，重置停止信号
    STOP_EVENT.clear()
    headers = {
        'User-Agent': user_agent,
        'Cookie': cookie,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Referer': 'https://javdb.com/'
    }
    proxies = {'http': proxies_config, 'https': proxies_config} if proxies_config else None
    final_csv_path = os.path.join(DATA_DIR, output_filename)

    # 初始化运行变量
    phase = 1
    current_url = start_url
    page = 1
    movie_links = []
    start_index = 0

    if is_resume:
        chk = load_checkpoint()
        if chk:
            phase = chk.get('phase', 1)
            movie_links = chk.get('movie_links', [])
            if phase == 1:
                current_url = chk.get('current_url')
                page = chk.get('page', 1)
            elif phase == 2:
                start_index = chk.get('current_index', 0)
        update_status("running", f"恢复中...", "续传启动", "成功接收新 Cookie，正在从断点恢复任务...")
    else:
        # 全新启动，清理旧的断点文件
        if os.path.exists(CHECKPOINT_FILE): os.remove(CHECKPOINT_FILE)
        update_status("running", "0/0", "初始化", "任务全新启动，开始拉取目录...", clear_log=True)
    
    # === 阶段一：获取清单 ===
    if phase == 1:
        while current_url:
            # 【核心检查点】检查是否收到了停止信号
            if STOP_EVENT.is_set():
                save_checkpoint({"phase": 1, "current_url": current_url, "page": page, "movie_links": movie_links})
                update_status("stopped", f"第 {page} 页", "手动终止", "🛑 接收到停止指令，清单抓取已强行终止。")
                return
            update_status("running", f"第 {page} 页", "拉取目录", f"正在抓取列表页: {current_url}")
            try:
                res = requests.get(current_url, headers=headers, proxies=proxies, impersonate="edge101", timeout=15)
                
                # 【核心拦截检测】
                if res.status_code in [403, 401, 503]:
                    save_checkpoint({"phase": 1, "current_url": current_url, "page": page, "movie_links": movie_links})
                    update_status("paused_need_cookie", f"第 {page} 页", "拦截挂起", f"⚠️ 列表页被拦截(状态码{res.status_code})。任务已挂起，请补充新 Cookie！")
                    return
                
                soup = BeautifulSoup(res.text, 'html.parser')
                for item in soup.select('div.movie-list a.box'):
                    full_url = urllib.parse.urljoin('https://javdb.com', item.get('href'))
                    raw_title = item.get('title', '')
                    
                    #优先使用精准的 DOM 节点提取番号
                    uid_strong = item.select_one('div.video-title strong')
                    if uid_strong:
                        code = uid_strong.text.strip()
                    else:
                        # 仅当页面结构变化找不到 strong 标签时，才使用正则作为备用方案
                        code_match = re.search(r'[A-Za-z0-9\-]+', raw_title)
                        code = code_match.group(0) if code_match else "未知番号"

                    if not any(d['url'] == full_url for d in movie_links):
                        movie_links.append({'code': code, 'url': full_url, 'title': raw_title})
                
                next_btn = soup.select_one('nav.pagination a.pagination-next')
                if next_btn and next_btn.get('href'):
                    current_url = urllib.parse.urljoin('https://javdb.com', next_btn.get('href'))
                    page += 1
                    time.sleep(1.5)
                else:
                    break
            except Exception as e:
                update_status("error", "异常", "代码报错", f"目录页请求异常: {str(e)}")
                return

        # 阶段一完成，自动保存为阶段二的断点起始
        phase = 2
        start_index = 0
        save_checkpoint({"phase": 2, "movie_links": movie_links, "current_index": 0})

    total_movies = len(movie_links)
    if total_movies == 0:
        update_status("error", "0/0", "完成但无数据", "未找到任何影片，请检查 URL 是否正确。")
        return

    if not is_resume or phase == 1:
        update_status("running", f"0/{total_movies}", "准备就绪", f"目录拉取完毕，共 {total_movies} 部影片，开始深度提取...")

    # === 阶段二：提取磁力 ===
    if phase == 2:
        fieldnames = ['影片番号', '原始标题', '影片链接', '最佳资源文件名', '磁力链接', '优先级得分', '日期', '文件大小(MB)']
        
        # 续传模式如果是从中途开始，则以追加('a')模式打开；否则覆盖('w')
        mode = 'a' if (is_resume and start_index > 0) else 'w'
        with open(final_csv_path, mode, encoding='utf-8-sig', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if mode == 'w':
                writer.writeheader()
            
            for i in range(start_index, total_movies):
                # 【核心检查点】检查是否收到了停止信号
                if STOP_EVENT.is_set():
                    save_checkpoint({"phase": 2, "movie_links": movie_links, "current_index": i})
                    update_status("stopped", f"{i+1}/{total_movies}", "手动终止", "🛑 接收到停止指令，磁力抓取已强行终止，进度已保存。")
                    return
                movie = movie_links[i]
                progress_str = f"{i+1}/{total_movies}"
                update_status("running", progress_str, movie['code'], f"正在解析详情页...")
                
                try:
                    res = requests.get(movie['url'], headers=headers, proxies=proxies, impersonate="edge101", timeout=15)
                    
                    # 【核心拦截检测】
                    if res.status_code in [403, 401, 503]:
                        save_checkpoint({"phase": 2, "movie_links": movie_links, "current_index": i})
                        update_status("paused_need_cookie", progress_str, movie['code'], f"⚠️ 详情页被拦截(状态码{res.status_code})。任务已挂起，进度安全保存！")
                        return

                    soup = BeautifulSoup(res.text, 'html.parser')
                    magnets_content = soup.find(id='magnets-content')
                    
                    valid_magnets = []
                    if magnets_content:
                        for item in magnets_content.select('.item'):
                            mag_data = evaluate_magnet(item)
                            if mag_data: valid_magnets.append(mag_data)
                    
                    if valid_magnets:
                        valid_magnets.sort(key=lambda x: (x['rank'], x['date'], x['size_mb']), reverse=True)
                        best = valid_magnets[0]
                        
                        writer.writerow({
                            '影片番号': movie['code'], '原始标题': movie['title'], '影片链接': movie['url'],
                            '最佳资源文件名': best['name'], '磁力链接': best['link'], '优先级得分': best['rank'],
                            '日期': best['date'], '文件大小(MB)': round(best['size_mb'], 2)
                        })
                        update_status("running", progress_str, movie['code'], f"成功: 获取到最高级资源 (Rank {best['rank']}, {round(best['size_mb'],2)}MB)")
                    else:
                        update_status("running", progress_str, movie['code'], f"跳过: 此页面无有效磁力链。")
                        
                except Exception as e:
                    update_status("error", progress_str, movie['code'], f"提取失败: {str(e)}")
                    
                time.sleep(2)
    update_status("finished", f"{total_movies}/{total_movies}", "全部完成", "🎉 爬取任务圆满结束，文件已保存！", final_filename=output_filename)