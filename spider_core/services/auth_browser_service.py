"""HTTP client for the optional Auth Browser Service."""

import json
import urllib.error
import urllib.parse
import urllib.request

import db_store
from app_config import (
    AUTH_BROWSER_SERVICE_URL,
    AUTH_BROWSER_SHARED_TOKEN,
    AUTH_BROWSER_TIMEOUT_SECONDS,
)
from services.cookie_validation_service import validate_runtime_cookie


class AuthBrowserError(Exception):
    def __init__(self, message: str, status_code: int = 502):
        super().__init__(message)
        self.status_code = status_code


def _configured_base_url() -> str:
    if not AUTH_BROWSER_SERVICE_URL:
        raise AuthBrowserError("Auth Browser Service 未配置，请设置 AUTH_BROWSER_SERVICE_URL", 400)
    return AUTH_BROWSER_SERVICE_URL


def _request(method: str, path: str, payload: dict | None = None) -> dict:
    base_url = _configured_base_url()
    url = urllib.parse.urljoin(base_url + "/", path.lstrip("/"))
    body = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    if AUTH_BROWSER_SHARED_TOKEN:
        headers["X-Auth-Browser-Token"] = AUTH_BROWSER_SHARED_TOKEN
    request = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=AUTH_BROWSER_TIMEOUT_SECONDS) as response:
            raw = response.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        try:
            raw = exc.read().decode("utf-8")
            data = json.loads(raw) if raw else {}
            detail = data.get("detail") or data.get("msg") or str(exc)
            if isinstance(detail, dict):
                detail = detail.get("message") or detail.get("code") or str(detail)
        except Exception:
            detail = str(exc)
        raise AuthBrowserError(f"Auth Browser Service 请求失败：{detail}", exc.code)
    except urllib.error.URLError as exc:
        raise AuthBrowserError(f"无法连接 Auth Browser Service：{exc.reason}")
    except TimeoutError:
        raise AuthBrowserError("Auth Browser Service 请求超时")
    except json.JSONDecodeError:
        raise AuthBrowserError("Auth Browser Service 返回了无效 JSON")


def start_session() -> dict:
    return _request("POST", "/sessions/start")


def check_connection() -> dict:
    return _request("GET", "/health")


def get_session_status(session_id: str) -> dict:
    if not session_id:
        raise AuthBrowserError("session_id 不能为空", 400)
    return _request("GET", f"/sessions/{urllib.parse.quote(session_id)}/status")


def capture_session(session_id: str, remember_cookie: bool = True) -> dict:
    if not session_id:
        raise AuthBrowserError("session_id 不能为空", 400)
    data = _request("POST", f"/sessions/{urllib.parse.quote(session_id)}/capture")
    cookie = str(data.get("cookie") or "").strip()
    user_agent = str(data.get("user_agent") or "").strip()
    if not cookie:
        raise AuthBrowserError("Auth Browser Service 未返回 Cookie", 502)
    db_store.save_runtime_config(
        cookie=cookie,
        remember_cookie=remember_cookie,
        user_agent=user_agent or None,
        cookie_source="auth_browser",
        cookie_status="unverified",
    )
    validation = validate_runtime_cookie(update_runtime=True)
    return {
        "session_id": data.get("session_id") or session_id,
        "status": data.get("status") or "captured",
        "has_cookie": True,
        "user_agent": user_agent,
        "cookie_validation": validation,
    }


def close_session(session_id: str) -> dict:
    if not session_id:
        raise AuthBrowserError("session_id 不能为空", 400)
    return _request("POST", f"/sessions/{urllib.parse.quote(session_id)}/close")
