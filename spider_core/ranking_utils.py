from urllib.parse import parse_qs, urlparse


COLLECTION_TYPE_ACTOR = "actor"
COLLECTION_TYPE_RANKING = "ranking"

RANKING_CATEGORIES = {
    "censored": "有码",
    "uncensored": "无码",
    "western": "欧美",
    "fc2": "FC2",
}
RANKING_PERIODS = {
    "daily": "日榜",
    "weekly": "周榜",
    "monthly": "月榜",
}


def ranking_filename(category, period):
    return f"ranking_{category}_{period}.csv"


def ranking_url(category, period):
    if category not in RANKING_CATEGORIES or period not in RANKING_PERIODS:
        return ""
    return f"https://javdb.com/rankings/movies?p={period}&t={category}"


def parse_ranking_url(url):
    parsed = urlparse((url or "").strip())
    path = parsed.path.rstrip("/")
    if path != "/rankings/movies":
        return None
    params = parse_qs(parsed.query, keep_blank_values=True)
    period = (params.get("p") or [""])[0]
    category = (params.get("t") or [""])[0]
    if category not in RANKING_CATEGORIES or period not in RANKING_PERIODS:
        return None
    return {
        "collection_type": COLLECTION_TYPE_RANKING,
        "ranking_category": category,
        "ranking_period": period,
        "filename": ranking_filename(category, period),
        "url": ranking_url(category, period),
    }


def is_valid_ranking(category, period):
    return category in RANKING_CATEGORIES and period in RANKING_PERIODS
