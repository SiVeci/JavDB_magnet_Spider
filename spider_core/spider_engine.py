from bs4 import BeautifulSoup
import urllib.parse
import re
import time
import os
import json
import threading
import db_store
from ranking_utils import COLLECTION_TYPE_ACTOR, COLLECTION_TYPE_RANKING, ranking_filename
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
db_store.configure(DATA_DIR)

STOP_EVENT = threading.Event()
TASK_CONTEXT = threading.local()
STATUS_FILE = os.path.join(DATA_DIR, 'status.json')
CHECKPOINT_FILE = os.path.join(DATA_DIR, 'checkpoint.json')
HTTP_TIMEOUT_SECONDS = 15
HTTP_MAX_RETRIES = 2
HTTP_RETRY_BACKOFF_SECONDS = 1.0
PAGE_DELAY_SECONDS = 1.5
DETAIL_DELAY_SECONDS = 0.1
RETRY_DELAY_SECONDS = 2.0
DEFAULT_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Referer': 'https://javdb.com/',
}
LOGIN_MARKERS = (
    "/login",
    "sign in",
    "log in",
    "login",
    "password",
)
BLOCK_MARKERS = (
    "just a moment",
    "access denied",
    "captcha",
    "cf-browser-verification",
    "turnstile",
    "cf-challenge",
    "cf-chl-widget",
    "challenge-form",
)

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
        # 网络层瞬时异常(连接重置/超时等)做有限次指数退避重试；403/503 等正常响应不在此处理
        last_exc = None
        for attempt in range(HTTP_MAX_RETRIES + 1):
            try:
                return requests.get(url, headers=headers, proxies=proxies, impersonate="chrome", timeout=HTTP_TIMEOUT_SECONDS)
            except Exception as exc:
                last_exc = exc
                if attempt < HTTP_MAX_RETRIES:
                    time.sleep(HTTP_RETRY_BACKOFF_SECONDS * (2 ** attempt))
        raise last_exc


# ======= 3. 核心业务逻辑 =======

def get_current_task_id():
    return getattr(TASK_CONTEXT, "task_id", None)


def map_task_state(state):
    return {
        "stopped": "paused",
        "paused_need_cookie": "waiting_cookie",
        "paused_need_choice": "waiting_choice",
        "error": "failed",
    }.get(state, state)


def get_android_javdb_cookie():
    if not IS_ANDROID:
        return ""
    try:
        WebViewBridge = jclass("com.javdb_spider.app.WebViewBridge")
        return str(WebViewBridge.getJavdbCookie() or "")
    except Exception:
        return ""


def update_status(state="idle", progress="", current="", log_msg=None, clear_log=False, final_filename=None, added_count=None):
    task_id = get_current_task_id()
    if task_id:
        db_store.update_task_status(
            task_id,
            state=map_task_state(state),
            progress=progress,
            current=current,
            log_msg=log_msg,
            final_filename=final_filename,
            added_count=added_count,
            error_message=log_msg if state in {"error", "paused_need_cookie"} else None,
        )

    status_data = {"state": state, "progress": progress, "current": current, "logs": []}
    if task_id:
        status_data["task_id"] = task_id
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
        except (json.JSONDecodeError, KeyError, OSError):
            pass

    if log_msg:
        time_str = time.strftime("%H:%M:%S", time.localtime())
        status_data["logs"].append(f"[{time_str}] {log_msg}")

    atomic_write_json(STATUS_FILE, status_data, indent=2)
    if task_id:
        try:
            from services.queue_service import broadcast_update
            broadcast_update(db_store.get_task(task_id))
        except Exception:
            pass

def save_checkpoint(data):
    task_id = get_current_task_id()
    if task_id:
        db_store.save_task_checkpoint(task_id, data)
    atomic_write_json(CHECKPOINT_FILE, data)

def load_checkpoint():
    task_id = get_current_task_id()
    if task_id:
        data = db_store.load_task_checkpoint(task_id)
        if data:
            return data
    if os.path.exists(CHECKPOINT_FILE):
        return read_json_file(CHECKPOINT_FILE)
    return None


def get_control_request():
    task_id = get_current_task_id()
    if not task_id:
        return "cancel" if STOP_EVENT.is_set() else None
    task = db_store.get_task(task_id)
    if not task:
        return "cancel"
    if task["state"] == "pause_requested":
        return "pause"
    if task["state"] == "cancel_requested":
        return "cancel"
    return None


def pause_or_cancel_task(progress, current, checkpoint, pause_log, cancel_log):
    request = get_control_request()
    if not request:
        return False
    save_checkpoint(checkpoint)
    if request == "cancel":
        update_status("canceled", progress, current, cancel_log)
    else:
        update_status("stopped", progress, current, pause_log)
    return True


def _response_text(response):
    return str(getattr(response, "text", "") or "")


def _is_login_response(response):
    url = str(getattr(response, "url", "") or "").lower()
    text = _response_text(response)
    lowered = text.lower()
    if "/login" in url:
        return True
    if any(marker in lowered for marker in LOGIN_MARKERS):
        soup = BeautifulSoup(text, "html.parser")
        return bool(soup.select_one('form[action*="login"], a[href*="/login"], input[type="password"]'))
    return False


def _is_blocked_response(response):
    lowered = _response_text(response).lower()
    return any(marker in lowered for marker in BLOCK_MARKERS)


def classify_runtime_fetch_issue(response=None, error=None, stage_label="请求"):
    if error is not None:
        return {
            "cookie_status": "network_error",
            "message": f"{stage_label}遇到网络或代理错误：{str(error)}",
        }
    if response is None:
        return None
    status_code = getattr(response, "status_code", 0) or 0
    if status_code == 401 or _is_login_response(response):
        return {
            "cookie_status": "expired",
            "message": f"{stage_label}显示登录态已失效，请重新获取 Cookie。",
        }
    if status_code in {403, 429} or (status_code == 503 and _is_blocked_response(response)):
        return {
            "cookie_status": "blocked",
            "message": f"{stage_label}被访问限制拦截（状态码 {status_code}），请重新登录或稍后重试。",
        }
    if status_code >= 500:
        return {
            "cookie_status": "network_error",
            "message": f"{stage_label}遇到服务、网络或代理错误（状态码 {status_code}），请检查网络或代理。",
        }
    return None


def pause_for_cookie_recovery(progress, current, checkpoint, issue):
    message = issue["message"]
    cookie_status = issue["cookie_status"]
    save_checkpoint(checkpoint)
    db_store.update_cookie_validation_status(cookie_status, time.time(), message)
    task_id = get_current_task_id()
    if task_id:
        db_store.increment_task_cookie_failure_count(task_id)
    update_status("paused_need_cookie", progress, current, message)

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


def parse_movie_tags(soup):
    for block in soup.select('.movie-panel-info .panel-block'):
        label = block.select_one('strong')
        if not label:
            continue
        label_text = label.get_text(strip=True).rstrip(':：')
        if label_text not in {'类别', '類別'}:
            continue
        tags = []
        seen = set()
        for link in block.select('.value a'):
            tag = link.get_text(strip=True)
            if tag and tag not in seen:
                tags.append(tag)
                seen.add(tag)
        return tags
    return []

def run_spider(
    start_url,
    cookie,
    user_agent,
    output_filename,
    proxies_config=None,
    is_resume=False,
    crawl_mode=None,
    task_id=None,
    collection_type=COLLECTION_TYPE_ACTOR,
    ranking_category="",
    ranking_period="",
):
    TASK_CONTEXT.task_id = task_id
    if not task_id:
        STOP_EVENT.clear()
    try:
        output_filename = normalize_csv_filename(output_filename, allow_empty=True)
    except UnsafeFilenameError as e:
        update_status("error", "参数错误", "文件名非法", f"输出文件名非法: {str(e)}")
        return

    headers = {
        'User-Agent': user_agent,
        'Cookie': cookie,
        **DEFAULT_HEADERS,
    }
    proxies = {'http': proxies_config, 'https': proxies_config} if proxies_config else None

    phase = 1
    current_url = start_url
    page = 1
    movie_links = []
    start_index = 0
    incremental_movie_codes = []

    if is_resume:
        chk = load_checkpoint()
        if chk:
            phase = chk.get('phase', 1)
            movie_links = chk.get('movie_links', [])
            incremental_movie_codes = chk.get('incremental_movie_codes', [])
            if not isinstance(incremental_movie_codes, list):
                incremental_movie_codes = []
            if phase == 1:
                current_url = chk.get('current_url')
                page = chk.get('page', 1)
            elif phase == 2:
                start_index = chk.get('current_index', 0)
        update_status("running", "恢复中...", "续传启动", "成功接收新 Cookie，正在从断点恢复任务...")
    else:
        if task_id:
            db_store.clear_task_checkpoint(task_id)
        elif os.path.exists(CHECKPOINT_FILE):
            os.remove(CHECKPOINT_FILE)
        update_status("running", "0/0", "初始化", "任务全新启动，开始拉取目录...", clear_log=True)

    # === 阶段一：获取清单 ===
    if phase == 1:
        while current_url:
            if pause_or_cancel_task(
                f"第 {page} 页",
                "手动暂停",
                {"phase": 1, "current_url": current_url, "page": page, "movie_links": movie_links},
                "收到暂停指令，目录抓取已保存断点。",
                "收到取消指令，目录抓取已终止。",
            ):
                return
            update_status("running", f"第 {page} 页", "拉取目录", f"正在抓取列表页: {current_url}")
            res = None
            try:
                # 【修改点】调用抽象的 fetch_html 替代 requests.get
                res = fetch_html(current_url, headers=headers, proxies=proxies)

                issue = classify_runtime_fetch_issue(res, stage_label="列表页请求")
                if issue:
                    pause_for_cookie_recovery(
                        f"第 {page} 页",
                        "等待登录态恢复",
                        {"phase": 1, "current_url": current_url, "page": page, "movie_links": movie_links},
                        issue,
                    )
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
                    if collection_type == COLLECTION_TYPE_RANKING:
                        output_filename = ranking_filename(ranking_category, ranking_period)
                    else:
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

                if page == 1 and (db_store.collection_exists(output_filename) or os.path.exists(final_csv_path)) and not crawl_mode:
                    save_checkpoint({"phase": 1, "current_url": next_url, "page": page + 1 if next_url else page, "movie_links": movie_links})
                    update_status("paused_need_choice", f"第 {page} 页", "等待选择", f"发现历史记录：{output_filename}，请选择爬取模式。", final_filename=output_filename)
                    return
                # ========================================

                current_url = next_url
                if current_url:
                    page += 1
                    time.sleep(PAGE_DELAY_SECONDS)
            except Exception as e:
                if res is None:
                    issue = classify_runtime_fetch_issue(error=e, stage_label="目录页请求")
                    pause_for_cookie_recovery(
                        f"第 {page} 页",
                        "网络或代理错误",
                        {"phase": 1, "current_url": current_url, "page": page, "movie_links": movie_links},
                        issue,
                    )
                else:
                    update_status("error", "异常", "代码报错", f"目录页请求异常: {str(e)}")
                return

        phase = 2
        start_index = 0
        save_checkpoint({"phase": 2, "movie_links": movie_links, "current_index": 0, "incremental_movie_codes": incremental_movie_codes})

    total_movies = len(movie_links)
    if total_movies == 0:
        update_status("error", "0/0", "完成但无数据", "未找到任何影片，请检查 URL 是否正确。")
        return

    if not is_resume or phase == 1:
        update_status("running", f"0/{total_movies}", "准备就绪", f"目录拉取完成，共 {total_movies} 部影片，开始深度提取...")

    # === 阶段二：提取磁力 ===
    if phase == 2:
        if not output_filename:
            if collection_type == COLLECTION_TYPE_RANKING:
                output_filename = ranking_filename(ranking_category, ranking_period)
            else:
                timestamp = time.strftime("%Y%m%d_%H%M%S", time.localtime())
                output_filename = make_csv_filename_from_label(f"javdb_{timestamp}")
        final_csv_path, output_filename = get_safe_csv_path(DATA_DIR, output_filename)
        if crawl_mode == 'overwrite' and start_index == 0:
            db_store.clear_collection(output_filename)
        db_store.ensure_collection(output_filename, start_url, collection_type, ranking_category, ranking_period)

        existing_codes = db_store.get_existing_codes(output_filename) if crawl_mode == 'incremental' else set()
        new_added_count = len(incremental_movie_codes) if crawl_mode == 'incremental' else 0

        for i in range(start_index, total_movies):
                movie = movie_links[i]
                if pause_or_cancel_task(
                    f"{i+1}/{total_movies}",
                    movie.get("code", "手动暂停"),
                    {"phase": 2, "movie_links": movie_links, "current_index": i, "incremental_movie_codes": incremental_movie_codes},
                    "收到暂停指令，磁力抓取已保存断点。",
                    "收到取消指令，磁力抓取已终止。",
                ):
                    return
                progress_str = f"{i+1}/{total_movies}"

                # 增量跳过判断
                if crawl_mode == 'incremental' and movie['code'] in existing_codes:
                    update_status("running", progress_str, movie['code'], "跳过: 本地记录已存在")
                    time.sleep(DETAIL_DELAY_SECONDS) # 稍微延迟让 UI 有时间刷新
                    continue

                update_status("running", progress_str, movie['code'], "正在解析详情页...")

                res = None
                try:
                    # 【修改点】调用抽象的 fetch_html 替代 requests.get
                    res = fetch_html(movie['url'], headers=headers, proxies=proxies)

                    issue = classify_runtime_fetch_issue(res, stage_label="详情页请求")
                    if issue:
                        pause_for_cookie_recovery(
                            progress_str,
                            movie['code'],
                            {"phase": 2, "movie_links": movie_links, "current_index": i, "incremental_movie_codes": incremental_movie_codes},
                            issue,
                        )
                        return

                    soup = BeautifulSoup(res.text, 'html.parser')
                    movie["tags"] = parse_movie_tags(soup)
                    magnets_content = soup.find(id='magnets-content')

                    valid_magnets = []
                    if magnets_content:
                        for item in magnets_content.select('.item'):
                            mag_data = evaluate_magnet(item)
                            if mag_data: valid_magnets.append(mag_data)

                    if valid_magnets:
                        valid_magnets.sort(key=lambda x: (x['rank'], x['date'], x['size_mb']), reverse=True)
                        best = valid_magnets[0]

                        db_store.save_movie_result(output_filename, movie, best, valid_magnets)
                        new_added_count += 1
                        if crawl_mode == 'incremental' and movie.get('code') and movie['code'] not in incremental_movie_codes:
                            incremental_movie_codes.append(movie['code'])
                            save_checkpoint({"phase": 2, "movie_links": movie_links, "current_index": i + 1, "incremental_movie_codes": incremental_movie_codes})
                        update_status("running", progress_str, movie['code'], f"成功: 获取到最高级资源 (Rank {best['rank']}, {round(best['size_mb'],2)}MB)")
                    else:
                        update_status("running", progress_str, movie['code'], "跳过: 此页面无有效磁力链。")

                except Exception as e:
                    if res is None:
                        issue = classify_runtime_fetch_issue(error=e, stage_label="详情页请求")
                        pause_for_cookie_recovery(
                            progress_str,
                            movie['code'],
                            {"phase": 2, "movie_links": movie_links, "current_index": i, "incremental_movie_codes": incremental_movie_codes},
                            issue,
                        )
                    else:
                        update_status("error", progress_str, movie['code'], f"提取失败: {str(e)}")
                    return

                time.sleep(RETRY_DELAY_SECONDS)
    if crawl_mode == 'incremental':
        save_checkpoint({"phase": "finished", "movie_links": movie_links, "current_index": total_movies, "incremental_movie_codes": incremental_movie_codes})
    update_status("finished", f"{total_movies}/{total_movies}", "全部完成", "爬取任务已完成，文件已保存。", final_filename=output_filename, added_count=new_added_count)


def run_task(task_id):
    task = db_store.get_task(task_id)
    if not task:
        return
    TASK_CONTEXT.task_id = task_id
    try:
        checkpoint = db_store.load_task_checkpoint(task_id)
        runtime = db_store.get_runtime_config(include_cookie=True)
        run_spider(
            task["start_url"],
            runtime.get("cookie", ""),
            runtime.get("user_agent", ""),
            task.get("final_filename") or task.get("requested_filename") or "",
            runtime.get("proxies") or None,
            bool(checkpoint),
            task.get("crawl_mode") or None,
            task_id=task_id,
            collection_type=task.get("collection_type") or COLLECTION_TYPE_ACTOR,
            ranking_category=task.get("ranking_category") or "",
            ranking_period=task.get("ranking_period") or "",
        )
    finally:
        TASK_CONTEXT.task_id = None
