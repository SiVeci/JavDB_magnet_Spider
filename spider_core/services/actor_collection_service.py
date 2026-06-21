"""收藏演员清单的抓取、解析、分页与刷新服务。

职责（PRD §3-§6, §12）：
- 5 个具体分类各自抓取对应 collection_actors URL；"全部" 由清单合并生成（actor_id 去重）。
- 演员卡片解析：名称取自 <strong>，actor_id 取自 /actors/{id}，忽略头像，缺名视为异常跳过。
- 分页：解析 ul.pagination-list 找最大页，逐页抓取并保留 g/t 参数，解码 &amp;。
- 未登录 / 被拦截不解析为空清单，抛 ActorFetchError(kind='auth')，由路由提示检查 Cookie。
- 刷新失败的分类不写入快照（不静默覆盖为空）。
"""

import html
import logging
import re
import time
from urllib.parse import parse_qs, urlparse

from bs4 import BeautifulSoup

import db_store
from db_store import _extract_actor_id
from services.task_service import get_runtime_for_request
from spider_engine import fetch_html
from utils import build_proxy_dict, ensure_zh_locale, runtime_headers

log = logging.getLogger(__name__)

COLLECTION_ACTORS_BASE = "https://javdb.com/users/collection_actors"
ALL_CATEGORY = "all"
PAGE_DELAY_SECONDS = 1.0

# 具体分类登记表（PRD §3）。key 同时作为 collection_actors.category 存储值。
ACTOR_CATEGORIES = [
    {"key": "g0t0", "label": "有码女优", "g": "0", "t": "0"},
    {"key": "g1t0", "label": "有码男优", "g": "1", "t": "0"},
    {"key": "g0t1", "label": "无码演员", "g": "0", "t": "1"},
    {"key": "g0t2", "label": "欧美女优", "g": "0", "t": "2"},
    {"key": "g1t2", "label": "欧美男优", "g": "1", "t": "2"},
]
_CATEGORY_BY_KEY = {cat["key"]: cat for cat in ACTOR_CATEGORIES}


class ActorFetchError(Exception):
    """收藏演员抓取错误。kind ∈ {auth, network, parse, status}。"""

    def __init__(self, kind, msg):
        super().__init__(msg)
        self.kind = kind
        self.msg = msg


def categories_meta():
    """前端用分类元数据：全部 + 5 个具体分类。"""
    return [{"key": ALL_CATEGORY, "label": "全部"}] + [
        {"key": cat["key"], "label": cat["label"]} for cat in ACTOR_CATEGORIES
    ]


def _category_by_key(key):
    return _CATEGORY_BY_KEY.get((key or "").strip())


def _category_url(cat, page=1):
    url = f"{COLLECTION_ACTORS_BASE}?g={cat['g']}&t={cat['t']}"
    if page > 1:
        url += f"&page={page}"
    return url


def _clean_text(text):
    return re.sub(r"\s+", " ", html.unescape(text or "")).strip()


def _looks_logged_out(soup):
    """未登录页面通常带登录表单或登录入口链接（PRD §4.7）。"""
    if soup.select_one('form[action*="sign_in"], form[action*="login"]'):
        return True
    return bool(soup.select_one('a[href*="/login"], a[href*="/users/sign_in"]'))


def parse_actor_cards(soup):
    """从收藏演员页解析卡片，返回 [{actor_id, actor_name, actor_url}]，按 actor_id 去重。

    仅在收藏演员卡片容器 `.actor-box` 内解析，每张卡片只取其中的演员主链接
    （`<a href="/actors/{id}">`，含头像与 <strong> 名称）。这样可排除：
    - 容器外的分类导航链接（/actors/censored、/actors/uncensored、/actors/western 等）；
    - 卡片内的删除按钮 `<a href="/actors/{id}/uncollect">`（卡片中的第二个 <a>）。
    只有「确为收藏卡片却缺少 <strong> 名称」时才视为解析异常并记录（PRD §5.6）。
    """
    actors = []
    seen = set()
    for box in soup.select(".actor-box"):
        anchor = box.select_one('a[href^="/actors/"]')  # 卡片内第一个即演员主链接
        if anchor is None:
            continue
        href = anchor.get("href") or ""
        actor_id = _extract_actor_id(href)
        if not actor_id or actor_id in seen:
            continue
        strong = anchor.select_one("strong")
        if strong is None:
            # 卡片缺少 <strong> 名称：视为解析异常，跳过且不使用 title 兜底（PRD §5.6）。
            log.warning("收藏演员卡片缺少 <strong> 名称，已跳过: %s", href)
            continue
        name = _clean_text(strong.get_text())
        if not name:
            log.warning("收藏演员卡片名称为空，已跳过: %s", href)
            continue
        seen.add(actor_id)
        actors.append(
            {
                "actor_id": actor_id,
                "actor_name": name,
                "actor_url": f"https://javdb.com/actors/{actor_id}",
            }
        )
    return actors


def _parse_max_page(soup):
    """解析 ul.pagination-list 找最大页码；无分页区域按单页处理（PRD §6）。"""
    max_page = 1
    for link in soup.select("ul.pagination-list a.pagination-link"):
        page = None
        text = link.get_text(strip=True)
        if text.isdigit():
            page = int(text)
        else:
            href = html.unescape(link.get("href") or "")
            params = parse_qs(urlparse(href).query)
            if params.get("page"):
                try:
                    page = int(params["page"][0])
                except (ValueError, TypeError):
                    page = None
        if page and page > max_page:
            max_page = page
    return max_page


def _fetch(url, headers, proxies):
    try:
        resp = fetch_html(url, headers=headers, proxies=proxies)
    except Exception as e:  # 网络层异常（TLS/超时/代理不可用等）
        raise ActorFetchError("network", f"收藏演员页请求失败：{str(e)}")
    if resp.status_code in (401, 403, 503):
        raise ActorFetchError(
            "auth",
            f"收藏演员页被拦截或未登录（状态码 {resp.status_code}），请检查 Cookie。",
        )
    if resp.status_code != 200:
        raise ActorFetchError("status", f"收藏演员页请求失败，状态码：{resp.status_code}")
    return resp


def fetch_category(cat, runtime):
    """抓取单个具体分类的全部演员（含分页），返回去重后的演员列表。"""
    headers = runtime_headers(runtime)
    proxies = build_proxy_dict(runtime.get("proxies"))

    first_resp = _fetch(ensure_zh_locale(_category_url(cat, 1)), headers, proxies)
    soup = BeautifulSoup(first_resp.text, "html.parser")
    cards = parse_actor_cards(soup)
    if not cards and _looks_logged_out(soup):
        raise ActorFetchError("auth", "未检测到登录状态，请检查 Cookie。")

    seen = {card["actor_id"] for card in cards}
    actors = list(cards)
    max_page = _parse_max_page(soup)
    for page in range(2, max_page + 1):
        time.sleep(PAGE_DELAY_SECONDS)
        resp = _fetch(ensure_zh_locale(_category_url(cat, page)), headers, proxies)
        page_soup = BeautifulSoup(resp.text, "html.parser")
        for card in parse_actor_cards(page_soup):
            if card["actor_id"] not in seen:
                seen.add(card["actor_id"])
                actors.append(card)
    return actors


def refresh_category(key):
    """刷新单个具体分类并覆盖写入快照，返回演员数量。"""
    cat = _category_by_key(key)
    if not cat:
        raise ActorFetchError("status", f"未知收藏分类：{key}")
    runtime = get_runtime_for_request()
    if not runtime.get("cookie"):
        raise ActorFetchError("auth", "Cookie 不能为空，请先在设置中配置 JavDB Cookie。")
    actors = fetch_category(cat, runtime)
    db_store.replace_category_snapshot(cat["key"], _category_url(cat, 1), actors)
    return len(actors)


def refresh_all():
    """刷新全部 5 个具体分类。逐个尝试，失败分类不写空快照，返回成功与失败明细（PRD §12.3）。"""
    runtime = get_runtime_for_request()
    if not runtime.get("cookie"):
        raise ActorFetchError("auth", "Cookie 不能为空，请先在设置中配置 JavDB Cookie。")
    refreshed = []
    failed = []
    for cat in ACTOR_CATEGORIES:
        try:
            actors = fetch_category(cat, runtime)
        except ActorFetchError as e:
            failed.append({"category": cat["key"], "label": cat["label"], "kind": e.kind, "msg": e.msg})
            continue
        db_store.replace_category_snapshot(cat["key"], _category_url(cat, 1), actors)
        refreshed.append({"category": cat["key"], "label": cat["label"], "count": len(actors)})
    return {"refreshed": refreshed, "failed": failed}
