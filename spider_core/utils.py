"""Pure utility helpers shared by routers and services."""

from urllib.parse import parse_qs, quote, urlencode, urlparse, urlunparse

from fastapi.responses import Response

from spider_engine import DATA_DIR
from storage_utils import UnsafeFilenameError, get_safe_csv_path


def get_safe_name(filename: str) -> str | None:
    try:
        _, safe_name = get_safe_csv_path(DATA_DIR, filename)
        return safe_name
    except UnsafeFilenameError:
        return None

def parse_tag_filter(tags: str = None):
    if not tags:
        return []
    values = [tag.strip() for tag in tags.split(",")]
    return [tag for tag in values if tag and tag.lower() != "all"]

def ensure_zh_locale(url: str) -> str:
    parsed = urlparse(url)
    params = parse_qs(parsed.query, keep_blank_values=True)
    params["locale"] = ["zh"]
    query = urlencode(params, doseq=True)
    return urlunparse(parsed._replace(query=query))

def build_proxy_dict(proxy):
    return {"http": proxy, "https": proxy} if proxy else None

def runtime_headers(runtime):
    return {
        "User-Agent": runtime.get("user_agent") or "",
        "Cookie": runtime.get("cookie") or "",
    }

def csv_download_response(csv_bytes: bytes, filename: str) -> Response:
    """Build a CSV file download response."""
    quoted_name = quote(filename)
    return Response(
        content=csv_bytes,
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="download.csv"; filename*=UTF-8\'\'{quoted_name}'
        },
    )
