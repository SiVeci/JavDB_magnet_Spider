import re
from urllib.parse import parse_qs, urlencode, urlparse


COLLECTION_TYPE_ACTOR = "actor"
COLLECTION_TYPE_RANKING = "ranking"

RANKING_CATEGORIES = {
    "censored": {"label": "有码", "path": "/rankings/movies", "query": {"t": "censored"}},
    "uncensored": {"label": "无码", "path": "/rankings/movies", "query": {"t": "uncensored"}},
    "western": {"label": "欧美", "path": "/rankings/movies", "query": {"t": "western"}},
    "fc2": {"label": "FC2", "path": "/rankings/movies", "query": {"t": "fc2"}},
    "playback": {"label": "热播", "path": "/rankings/playback", "query": {}},
    "top250": {"label": "TOP250", "path": "/rankings/top", "query": {}},
}
RANKING_PERIODS = {
    "daily": "日榜",
    "weekly": "周榜",
    "monthly": "月榜",
}
TOP250_CATEGORY = "top250"
TOP250_ALL_KEY = "all"
TOP250_OPTIONS_CACHE_KEY = "top250"
TOP250_SOURCE_URL = "https://javdb.com/rankings/top"
_TOP250_KEY_RE = re.compile(r"^[A-Za-z0-9_-]{1,32}$")


def ranking_filename(category, period):
    return f"ranking_{category}_{period}.csv"


def ranking_url(category, period):
    if not is_valid_ranking(category, period):
        return ""
    meta = RANKING_CATEGORIES[category]
    params = dict(meta["query"])
    if category == TOP250_CATEGORY:
        if period != TOP250_ALL_KEY:
            params["t"] = period
    else:
        params = {"p": period, **params}
    query = urlencode(params)
    return f"https://javdb.com{meta['path']}{'?' + query if query else ''}"


def parse_ranking_url(url):
    parsed = urlparse((url or "").strip())
    path = parsed.path.rstrip("/")
    params = parse_qs(parsed.query, keep_blank_values=True)
    category = ""
    period = (params.get("p") or [""])[0]
    if path == "/rankings/movies":
        category = (params.get("t") or [""])[0]
    elif path == "/rankings/playback":
        category = "playback"
    elif path == "/rankings/top":
        category = TOP250_CATEGORY
        period = top250_option_key((params.get("t") or [""])[0])
    else:
        return None
    if not is_valid_ranking(category, period):
        return None
    return {
        "collection_type": COLLECTION_TYPE_RANKING,
        "ranking_category": category,
        "ranking_period": period,
        "filename": ranking_filename(category, period),
        "url": ranking_url(category, period),
    }


def is_valid_ranking(category, period):
    if category not in RANKING_CATEGORIES:
        return False
    if category == TOP250_CATEGORY:
        return is_valid_top250_option_key(period)
    return period in RANKING_PERIODS


def top250_option_key(value):
    value = (value or "").strip()
    return value or TOP250_ALL_KEY


def is_valid_top250_option_key(value):
    value = (value or "").strip()
    return value == TOP250_ALL_KEY or bool(_TOP250_KEY_RE.match(value))


def top250_option_label(key):
    if key == TOP250_ALL_KEY:
        return "全部"
    labels = {"0": "有码", "1": "无码", "2": "欧美", "3": "FC2"}
    if key in labels:
        return labels[key]
    if re.match(r"^y\d{4}$", key or ""):
        return key[1:]
    return key or TOP250_ALL_KEY


def parse_top250_options(html):
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html or "", "html.parser")
    select = soup.select_one('select[name="t"]')
    if not select:
        return []
    options = []
    seen = set()
    for option in select.select("option"):
        key = top250_option_key(option.get("value") or "")
        if not is_valid_top250_option_key(key) or key in seen:
            continue
        label = option.get_text(strip=True) or top250_option_label(key)
        options.append({"key": key, "label": label})
        seen.add(key)
    return options
