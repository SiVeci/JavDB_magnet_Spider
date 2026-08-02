"""JavDB 账号密码直登服务（curl_cffi 实现，无需浏览器）。

登录链路：
  1. start_session — 用 curl_cffi(impersonate=chrome) GET /login，拿到 CSRF token、
     会话 cookie 与验证码字段，并把验证码图(/rucaptcha)取回为 base64 交给前端显示。
  2. submit_login — 用同一 curl session POST /user_sessions 提交账号/密码/验证码，
     成功后从 session 中导出 javdb.com 的 Cookie 写入 runtime_config。

JavDB 登录仅有自带的 rucaptcha 图片验证码，无 Cloudflare 交互式挑战；curl_cffi 的
Chrome 指纹可透明通过被动盾，因此整条流程不需要 Playwright/VNC。会话与登录请求都从
本进程的网络出口发出，cf_clearance/IP 绑定天然一致。
"""

import base64
import time
import uuid
from urllib.parse import urljoin

from bs4 import BeautifulSoup

import db_store
from javdb_url import ensure_zh_locale
from services.cookie_validation_service import validate_runtime_cookie
from utils import build_proxy_dict

LOGIN_URL = ensure_zh_locale("https://javdb.com/login")
LOGIN_POST_PATH = "/user_sessions"
CAPTCHA_PATH = "/rucaptcha/"
BASE_URL = "https://javdb.com"
SESSION_TTL_SECONDS = 600
HTTP_TIMEOUT_SECONDS = 25

# session_id -> {curl_session, token, created_at, expires_at, status}
_sessions: dict[str, dict] = {}


class AuthBrowserError(Exception):
    def __init__(self, message: str, status_code: int = 502):
        super().__init__(message)
        self.status_code = status_code


def _now() -> float:
    return time.time()


def _new_curl_session():
    # 局部导入：与 spider_engine 一致，避免安卓端导入 curl_cffi 报错。
    from curl_cffi import requests

    runtime = db_store.get_runtime_config(include_cookie=False)
    proxies = build_proxy_dict(runtime.get("proxies") or "")
    session = requests.Session(impersonate="chrome", timeout=HTTP_TIMEOUT_SECONDS)
    if proxies:
        session.proxies = proxies
    return session


def _cleanup_expired():
    now = _now()
    for sid in list(_sessions.keys()):
        if _sessions[sid].get("expires_at", 0) <= now:
            _sessions.pop(sid, None)


def _active_session_id() -> str:
    _cleanup_expired()
    return next(iter(_sessions), "")


def check_connection() -> dict:
    """健康检查：直登无外部服务依赖，确认 curl_cffi 可用即可。"""
    try:
        from curl_cffi import requests  # noqa: F401
    except ImportError:
        raise AuthBrowserError("curl_cffi 不可用，无法执行账号登录", 500)
    _cleanup_expired()
    return {
        "status": "ok",
        "mode": "direct_login",
        "active_session_id": _active_session_id(),
        "session_ttl_seconds": SESSION_TTL_SECONDS,
    }


def _parse_login_form(html: str) -> dict:
    soup = BeautifulSoup(html or "", "html.parser")
    form = soup.find("form", attrs={"action": LOGIN_POST_PATH})
    if form is None:
        raise AuthBrowserError("未找到 JavDB 登录表单，站点结构可能已变更", 502)
    token_input = form.find("input", attrs={"name": "authenticity_token"})
    token = token_input.get("value") if token_input else ""
    if not token:
        raise AuthBrowserError("未取得登录 CSRF token，请重试", 502)
    has_captcha = form.find("input", attrs={"name": "_rucaptcha"}) is not None
    return {"token": token, "has_captcha": has_captcha}


def _fetch_captcha(session) -> str:
    """取验证码图并编码为 data URL；失败不致命，返回空串。"""
    try:
        resp = session.get(urljoin(BASE_URL, CAPTCHA_PATH) + f"?t={int(_now() * 1000)}")
    except Exception:
        return ""
    if resp.status_code != 200 or not resp.content:
        return ""
    content_type = resp.headers.get("content-type", "image/gif")
    encoded = base64.b64encode(resp.content).decode("ascii")
    return f"data:{content_type};base64,{encoded}"


def start_session() -> dict:
    """开始一次登录：GET 登录页，返回验证码图供前端填写。"""
    _cleanup_expired()
    if _active_session_id():
        raise AuthBrowserError("已有进行中的登录会话，请先完成或关闭。", 409)

    session = _new_curl_session()
    try:
        resp = session.get(LOGIN_URL)
    except Exception as exc:
        raise AuthBrowserError(f"无法打开 JavDB 登录页：{exc}")
    if resp.status_code != 200:
        raise AuthBrowserError(f"打开登录页失败（HTTP {resp.status_code}）", 502)

    form = _parse_login_form(resp.text)
    captcha_image = _fetch_captcha(session) if form["has_captcha"] else ""

    session_id = uuid.uuid4().hex
    now = _now()
    _sessions[session_id] = {
        "curl_session": session,
        "token": form["token"],
        "created_at": now,
        "expires_at": now + SESSION_TTL_SECONDS,
        "status": "waiting_login",
    }
    return {
        "session_id": session_id,
        "status": "waiting_login",
        "login_url": LOGIN_URL,
        "needs_captcha": form["has_captcha"],
        "captcha_image": captcha_image,
        "created_at": now,
        "expires_at": now + SESSION_TTL_SECONDS,
    }


def refresh_captcha(session_id: str) -> dict:
    """用同一会话刷新验证码图（用户点图换一张时调用）。"""
    _cleanup_expired()
    session = _sessions.get(session_id)
    if not session:
        raise AuthBrowserError("登录会话不存在或已过期，请重新开始。", 404)
    captcha_image = _fetch_captcha(session["curl_session"])
    return {"session_id": session_id, "captcha_image": captcha_image}


def get_session_status(session_id: str) -> dict:
    _cleanup_expired()
    session = _sessions.get(session_id)
    if not session:
        raise AuthBrowserError("登录会话不存在或已过期。", 404)
    return {
        "session_id": session_id,
        "status": session["status"],
        "created_at": session["created_at"],
        "expires_at": session["expires_at"],
    }


def _extract_cookie_header(session) -> str:
    jar = session.cookies
    pairs = []
    try:
        items = jar.items()
    except Exception:
        items = dict(jar).items()
    for name, value in items:
        pairs.append(f"{name}={value}")
    return "; ".join(pairs)


def submit_login(session_id: str, email: str, password: str, captcha: str, remember_cookie: bool = True) -> dict:
    """提交账号密码 + 验证码，成功则保存 Cookie 到 runtime_config。"""
    _cleanup_expired()
    session = _sessions.get(session_id)
    if not session:
        raise AuthBrowserError("登录会话不存在或已过期，请重新开始。", 404)
    if not email or not password:
        raise AuthBrowserError("请填写账号和密码。", 400)

    curl_session = session["curl_session"]
    payload = {
        "authenticity_token": session["token"],
        "email": email,
        "password": password,
        "_rucaptcha": captcha or "",
        "remember": "1" if remember_cookie else "0",
        "commit": "Sign in",
    }
    try:
        resp = curl_session.post(
            urljoin(BASE_URL, LOGIN_POST_PATH),
            data=payload,
            allow_redirects=True,
        )
    except Exception as exc:
        raise AuthBrowserError(f"登录请求失败：{exc}")

    # 登录成功后 JavDB 会 302 跳离 /login；仍停留在登录表单说明凭据/验证码有误。
    final_url = str(getattr(resp, "url", "") or "")
    soup = BeautifulSoup(resp.text or "", "html.parser")
    still_on_login = soup.find("form", attrs={"action": LOGIN_POST_PATH}) is not None

    if still_on_login or "/login" in final_url:
        # 同一会话刷新验证码，便于用户直接重试。
        captcha_image = _fetch_captcha(curl_session)
        new_form = _parse_login_form(resp.text) if still_on_login else None
        if new_form:
            session["token"] = new_form["token"]
        message = _login_error_message(soup)
        raise AuthBrowserError(message or "登录失败，请检查账号、密码或验证码。", 422)

    cookie_header = _extract_cookie_header(curl_session)
    if "_jdb_session" not in cookie_header:
        raise AuthBrowserError("登录后未获取到会话 Cookie，请重试。", 502)

    user_agent = curl_session.headers.get("User-Agent") if hasattr(curl_session, "headers") else ""
    db_store.save_runtime_config(
        cookie=cookie_header,
        remember_cookie=remember_cookie,
        user_agent=user_agent or None,
        cookie_source="auth_browser",
        cookie_status="unverified",
    )
    validation = validate_runtime_cookie(update_runtime=True)

    session["status"] = "captured"
    _sessions.pop(session_id, None)
    return {
        "session_id": session_id,
        "status": "captured",
        "has_cookie": True,
        "user_agent": user_agent or "",
        "cookie_validation": validation,
    }


def _login_error_message(soup) -> str:
    for selector in (".message-body", ".notification", ".help.is-danger", "div.message"):
        el = soup.select_one(selector)
        if el:
            text = el.get_text(" ", strip=True)
            if text:
                return text[:200]
    return ""


def close_session(session_id: str) -> dict:
    session = _sessions.pop(session_id, None)
    if session:
        session["status"] = "closed"
    return {"session_id": session_id, "status": "closed"}
