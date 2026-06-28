"""Cookie lifecycle validation owned by the main crawler service."""

import time
from urllib.parse import urlparse

from bs4 import BeautifulSoup

import db_store
from app_config import COOKIE_CHECK_URL
from spider_engine import IS_ANDROID, fetch_html
from utils import build_proxy_dict, ensure_zh_locale, runtime_headers

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


def _result(valid, status, message, validated_at=None):
    return {
        "valid": bool(valid),
        "status": status,
        "message": message,
        "validated_at": validated_at if validated_at is not None else time.time(),
    }


def _is_login_response(response, text):
    url = str(getattr(response, "url", "") or "").lower()
    if "/login" in url:
        return True
    lowered = (text or "").lower()
    if any(marker in lowered for marker in LOGIN_MARKERS):
        soup = BeautifulSoup(text or "", "html.parser")
        if soup.select_one('form[action*="login"], a[href*="/login"], input[type="password"]'):
            return True
    return False


def _is_blocked_response(response, text):
    if response.status_code in {403, 429, 503}:
        return True
    lowered = (text or "").lower()
    return any(marker in lowered for marker in BLOCK_MARKERS)


def _validation_url():
    parsed = urlparse(COOKIE_CHECK_URL or "")
    if parsed.scheme and parsed.netloc:
        return ensure_zh_locale(COOKIE_CHECK_URL)
    return ensure_zh_locale("https://javdb.com/users/want_watch_videos")


def validate_runtime_cookie(update_runtime=True):
    runtime = db_store.get_runtime_config(include_cookie=True)
    cookie = (runtime.get("cookie") or "").strip()
    user_agent = (runtime.get("user_agent") or "").strip()
    if not cookie:
        result = _result(False, "missing", "Cookie missing. Paste a Cookie or capture one from Auth Browser.")
        if update_runtime:
            db_store.update_cookie_validation_status(result["status"], result["validated_at"], result["message"])
        return result
    # 安卓端走 WebView 内核抓取：上行请求由 WebView 自带 cookie jar / UA 完成，
    # header 里的 Cookie 与 User-Agent 实际不参与请求，故不强制要求配对 UA。
    # 非安卓端（curl_cffi）依赖 header UA，缺失则判为无效。
    if not user_agent and not IS_ANDROID:
        result = _result(False, "invalid", "Cookie is missing its paired User-Agent. Save both again.")
        if update_runtime:
            db_store.update_cookie_validation_status(result["status"], result["validated_at"], result["message"])
        return result

    try:
        response = fetch_html(
            _validation_url(),
            headers=runtime_headers(runtime),
            proxies=build_proxy_dict(runtime.get("proxies")),
        )
    except Exception as exc:
        result = _result(False, "network_error", f"Cookie validation request failed: {str(exc)}")
        if update_runtime:
            db_store.update_cookie_validation_status(result["status"], result["validated_at"], result["message"])
        return result

    text = response.text or ""
    if _is_blocked_response(response, text):
        result = _result(False, "blocked", f"Cookie validation was blocked (status {response.status_code}).")
    elif response.status_code in {301, 302, 303, 307, 308}:
        result = _result(False, "expired", "Cookie validation was redirected to login.")
    elif response.status_code in {401, 404} or _is_login_response(response, text):
        result = _result(False, "expired", "Cookie cannot access the authenticated page.")
    elif response.status_code >= 500:
        result = _result(False, "network_error", f"Cookie validation server error (status {response.status_code}).")
    elif response.status_code != 200:
        result = _result(False, "invalid", f"Cookie validation failed (status {response.status_code}).")
    else:
        result = _result(True, "valid", "Cookie valid.")

    if update_runtime:
        last_error = "" if result["valid"] else result["message"]
        db_store.update_cookie_validation_status(result["status"], result["validated_at"], last_error)
    return result


def ensure_cookie_valid_for_task():
    runtime = db_store.get_runtime_config(include_cookie=True)
    status = runtime.get("cookie_status") or ("unverified" if runtime.get("cookie") else "missing")
    if not runtime.get("cookie"):
        result = validate_runtime_cookie(update_runtime=True)
    elif status == "valid":
        return _result(True, "valid", "Cookie valid.", runtime.get("cookie_validated_at") or time.time())
    else:
        result = validate_runtime_cookie(update_runtime=True)
    if result["valid"]:
        return result
    return result
