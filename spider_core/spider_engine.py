from bs4 import BeautifulSoup
import urllib.parse
import re
import csv
import time
import os
import json
import threading
from storage_utils import (
    UnsafeFilenameError,
    atomic_write_json,
    get_safe_csv_path,
    make_csv_filename_from_label,
    normalize_csv_filename,
    read_json_file,
)

# ======= 1. 环境嗅探与路径配置 =======
try:
    from java import jclass
    IS_ANDROID = True
    # 获取安卓 App 的专属内部沙盒路径 (绝对安全可写)
    context = jclass("com.chaquo.python.Python").getInstance().getPlatform().getApplication()
    BASE_DIR = str(context.getFilesDir().getAbsolutePath())
except ImportError:
    IS_ANDROID = False
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 所有的文件都会保存在这个安全的 data 文件夹里
DATA_DIR = os.path.join(BASE_DIR, 'data')
os.makedirs(DATA_DIR, exist_ok=True)

STOP_EVENT = threading.Event()
STATUS_FILE = os.path.join(DATA_DIR, 'status.json')
OUTPUT_CSV = os.path.join(DATA_DIR, 'final_magnets.csv')
CHECKPOINT_FILE = os.path.join(DATA_DIR, 'checkpoint.json')

# ======= 2. HTML 提取底层网关抽象 =======
class MockResponse:
    """为了让安卓端的返回结果也能像 requests 一样调用 .text 和 .status_code"""
    def __init__(self, text, status_code):
        self.text = text
        self.status_code = status_code

def fetch_html(url, headers=None, proxies=None):
    """统一的源码获取接口：根据运行环境自动切换爬虫内核"""
    if IS_ANDROID:
        # 走安卓端原生 WebView 获取源码 (Java 层的桥接类)
        WebViewBridge = jclass("com.javdb_spider.app.WebViewBridge")
        html_content = WebViewBridge.getHtmlBlocking(url)

        # 简单模拟状态码：如果 WebView 返回的 HTML 包含 CF 盾的特征词，模拟返回 403 触发救援
        if html_content and ("Engine Timeout" in html_content or "Engine Error" in html_content):
            return MockResponse(html_content, 503)
        if not html_content or "Just a moment..." in html_content or "Cloudflare" in html_content:
            return MockResponse(html_content, 403)
        return MockResponse(html_content, 200)
    else:
        # 走 PC 端的 curl_cffi (必须放在局部导入，防止安卓端报错)
        from curl_cffi import requests
        return requests.get(url, headers=headers, proxies=proxies, impersonate="edge101", timeout=15)


# ======= 3. 核心业务逻辑 =======

def update_status(state="idle", progress="", current="", log_msg=None, clear_log=False, final_filename=None, added_count=None):
    status_data = {"state": state, "progress": progress, "current": current, "logs": []}
    if final_filename:
        status_data["final_filename"] = normalize_csv_filename(final_filename)
    if added_count is not None:
        status_data["added_count"] = added_count

    if not clear_log and os.path.exists(STATUS_FILE):
        try:
            old_data = read_json_file(STATUS_FILE, default={})
            status_data["logs"] = old_data.get("logs", [])[-20:]
            if not final_filename and "final_filename" in old_data:
                status_data["final_filename"] = normalize_csv_filename(old_data["final_filename"])
        except:
            pass

    if log_msg:
        time_str = time.strftime("%H:%M:%S", time.localtime())
        status_data["logs"].append(f"[{time_str}] {log_msg}")

    atomic_write_json(STATUS_FILE, status_data, indent=2)

def save_checkpoint(data):
    atomic_write_json(CHECKPOINT_FILE, data)

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        return read_json_file(CHECKPOINT_FILE)
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

    has_uncensored = bool(re.search(r'\b(uc|uncensored|u)\b', name))
    has_sub = bool(re.search(r'\b(c|chs)\b', name)) or ('字幕' in tags)
    has_hd = ('高清' in tags) or bool(re.search(r'\b(1080p|4k|2160p)\b', name))

    rank = 0
    if has_uncensored: rank += 100
    if has_hd:         rank += 10
    if has_sub:        rank += 1

    return {
        'link': magnet_a.get('href'),
        'name': name_elem.text.strip() if name_elem else 'Unknown',
        'rank': rank, 'date': date_str, 'size_mb': parse_size(size_str)
    }

def run_spider(start_url, cookie, user_agent, output_filename, proxies_config=None, is_resume=False, crawl_mode=None):
    STOP_EVENT.clear()
    try:
        output_filename = normalize_csv_filename(output_filename, allow_empty=True)
    except UnsafeFilenameError as e:
        update_status("error", "参数错误", "文件名非法", f"输出文件名非法: {str(e)}")
        return

    headers = {
        'User-Agent': user_agent,
        'Cookie': cookie,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Referer': 'https://javdb.com/'
    }
    proxies = {'http': proxies_config, 'https': proxies_config} if proxies_config else None

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
        if os.path.exists(CHECKPOINT_FILE): os.remove(CHECKPOINT_FILE)
        update_status("running", "0/0", "初始化", "任务全新启动，开始拉取目录...", clear_log=True)

    # === 阶段一：获取清单 ===
    if phase == 1:
        while current_url:
            if STOP_EVENT.is_set():
                save_checkpoint({"phase": 1, "current_url": current_url, "page": page, "movie_links": movie_links})
                update_status("stopped", f"第 {page} 页", "手动终止", "🛑 接收到停止指令，清单抓取已强行终止。")
                return
            update_status("running", f"第 {page} 页", "拉取目录", f"正在抓取列表页: {current_url}")
            try:
                # 【修改点】调用抽象的 fetch_html 替代 requests.get
                res = fetch_html(current_url, headers=headers, proxies=proxies)

                if res.status_code in [403, 401, 503]:
                    save_checkpoint({"phase": 1, "current_url": current_url, "page": page, "movie_links": movie_links})
                    update_status("paused_need_cookie", f"第 {page} 页", "拦截挂起", f"⚠️ 列表页被拦截(状态码{res.status_code})。任务已挂起，请补充新 Cookie！")
                    return

                soup = BeautifulSoup(res.text, 'html.parser')
                for item in soup.select('div.movie-list a.box'):
                    full_url = urllib.parse.urljoin('https://javdb.com', item.get('href'))
                    raw_title = item.get('title', '')

                    uid_strong = item.select_one('div.video-title strong')
                    if uid_strong:
                        code = uid_strong.text.strip()
                    else:
                        code_match = re.search(r'[A-Za-z0-9\-]+', raw_title)
                        code = code_match.group(0) if code_match else "未知番号"

                    if not any(d['url'] == full_url for d in movie_links):
                        movie_links.append({'code': code, 'url': full_url, 'title': raw_title})

                next_btn = soup.select_one('nav.pagination a.pagination-next')
                next_url = urllib.parse.urljoin('https://javdb.com', next_btn.get('href')) if (next_btn and next_btn.get('href')) else None

                # ======== 新增：动态命名与模式选择 ========
                if not output_filename:
                    actor_name = ""
                    if "/actors/" in current_url:
                        actor_tag = soup.select_one('.actor-section-name')
                        if actor_tag:
                            actor_name = actor_tag.text.strip()
                    
                    if actor_name:
                        output_filename = make_csv_filename_from_label(actor_name)
                    else:
                        timestamp = time.strftime("%Y%m%d_%H%M%S", time.localtime())
                        output_filename = make_csv_filename_from_label(f"javdb_{timestamp}")
                    
                    update_status("running", f"第 {page} 页", "生成文件名", f"已自动命名为: {output_filename}", final_filename=output_filename)

                final_csv_path, output_filename = get_safe_csv_path(DATA_DIR, output_filename)
                
                if page == 1 and os.path.exists(final_csv_path) and not crawl_mode:
                    save_checkpoint({"phase": 1, "current_url": next_url, "page": page + 1 if next_url else page, "movie_links": movie_links})
                    update_status("paused_need_choice", f"第 {page} 页", "等待选择", f"发现历史记录：【{output_filename}】，请选择爬取模式。", final_filename=output_filename)
                    return
                # ========================================

                current_url = next_url
                if current_url:
                    page += 1
                    time.sleep(1.5)
            except Exception as e:
                update_status("error", "异常", "代码报错", f"目录页请求异常: {str(e)}")
                return

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
        if not output_filename:
            timestamp = time.strftime("%Y%m%d_%H%M%S", time.localtime())
            output_filename = make_csv_filename_from_label(f"javdb_{timestamp}")
        final_csv_path, output_filename = get_safe_csv_path(DATA_DIR, output_filename)
        fieldnames = ['影片番号', '原始标题', '影片链接', '最佳资源文件名', '磁力链接', '优先级得分', '日期', '文件大小(MB)']

        # === 增量模式读取已有番号 ===
        existing_codes = set()
        if crawl_mode == 'incremental' and os.path.exists(final_csv_path):
            try:
                with open(final_csv_path, 'r', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        if '影片番号' in row and row['影片番号']:
                            existing_codes.add(row['影片番号'])
            except Exception: pass

        new_added_count = 0
        mode = 'a' if (is_resume and start_index > 0) or crawl_mode == 'incremental' else 'w'
        with open(final_csv_path, mode, encoding='utf-8-sig', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if mode == 'w':
                writer.writeheader()

            for i in range(start_index, total_movies):
                if STOP_EVENT.is_set():
                    save_checkpoint({"phase": 2, "movie_links": movie_links, "current_index": i})
                    update_status("stopped", f"{i+1}/{total_movies}", "手动终止", "🛑 接收到停止指令，磁力抓取已强行终止，进度已保存。")
                    return
                movie = movie_links[i]
                progress_str = f"{i+1}/{total_movies}"
                
                # 增量跳过判断
                if crawl_mode == 'incremental' and movie['code'] in existing_codes:
                    update_status("running", progress_str, movie['code'], f"⏭️ 跳过: 本地记录已存在")
                    time.sleep(0.1) # 稍微延迟让UI有时间刷新
                    continue

                update_status("running", progress_str, movie['code'], f"正在解析详情页...")

                try:
                    # 【修改点】调用抽象的 fetch_html 替代 requests.get
                    res = fetch_html(movie['url'], headers=headers, proxies=proxies)

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
                        new_added_count += 1
                        update_status("running", progress_str, movie['code'], f"成功: 获取到最高级资源 (Rank {best['rank']}, {round(best['size_mb'],2)}MB)")
                    else:
                        update_status("running", progress_str, movie['code'], f"跳过: 此页面无有效磁力链。")

                except Exception as e:
                    update_status("error", progress_str, movie['code'], f"提取失败: {str(e)}")

                time.sleep(2)
    update_status("finished", f"{total_movies}/{total_movies}", "全部完成", "🎉 爬取任务圆满结束，文件已保存！", final_filename=output_filename, added_count=new_added_count)
