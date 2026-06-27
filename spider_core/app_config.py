"""Application configuration and API authentication helpers."""

import os
import secrets

APP_VERSION = os.getenv("JAVDB_SPIDER_VERSION", "2.1.0")
AUTH_HEADER = "X-JavDB-Token"
PUBLIC_API_PATHS = {"/api/version"}
# 自行鉴权路径：这些端点不走 header 中间件校验，因为它们在端点内部用其他方式鉴权。
# /api/events 是 SSE，浏览器 EventSource 无法设置自定义请求头，只能用 ?token= query 校验，
# 故必须放行中间件（否则没有 X-JavDB-Token 头会被中间件直接 401，SSE 永远连不上）。
SELF_AUTH_API_PATHS = {"/api/events"}


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
