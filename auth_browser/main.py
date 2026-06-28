"""Standalone Auth Browser Service MVP.

The main crawler talks to this service only through HTTP. This process may run
on the same host, in a container, or on a remote machine.
"""

import os
import secrets
import time
import uuid

from fastapi import FastAPI, Header, HTTPException

JAVDB_LOGIN_URL = os.getenv("AUTH_BROWSER_LOGIN_URL", "https://javdb.com/login")
PUBLIC_BASE_URL = os.getenv("AUTH_BROWSER_PUBLIC_BASE_URL", "").strip().rstrip("/")
SHARED_TOKEN = os.getenv("AUTH_BROWSER_SHARED_TOKEN", "").strip()
HEADLESS = os.getenv("AUTH_BROWSER_HEADLESS", "").strip().lower() in {"1", "true", "yes", "on"}
CHROMIUM_EXECUTABLE_PATH = os.getenv("AUTH_BROWSER_CHROMIUM_EXECUTABLE_PATH", "").strip() or None
SESSION_TTL_SECONDS = int(os.getenv("AUTH_BROWSER_SESSION_TTL_SECONDS", "900"))
PROFILE_DIR = os.getenv(
    "AUTH_BROWSER_PROFILE_DIR",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "profile"),
).strip()
STORAGE_STATE_PATH = os.path.join(PROFILE_DIR, "storage_state.json")

app = FastAPI(title="JavDB Auth Browser Service")
sessions: dict[str, dict] = {}


async def _require_token(x_auth_browser_token: str | None = Header(default=None)):
    if SHARED_TOKEN and not secrets.compare_digest(x_auth_browser_token or "", SHARED_TOKEN):
        raise HTTPException(status_code=401, detail="Auth Browser Service token 无效")


def _viewer_url(session_id: str) -> str:
    if PUBLIC_BASE_URL:
        return f"{PUBLIC_BASE_URL}/sessions/{session_id}/viewer"
    return ""


def _response(session_id: str) -> dict:
    session = sessions[session_id]
    return {
        "session_id": session_id,
        "status": session["status"],
        "login_url": JAVDB_LOGIN_URL,
        "viewer_url": session.get("viewer_url") or _viewer_url(session_id),
        "created_at": session.get("created_at", 0),
        "expires_at": session.get("expires_at", 0),
        "error": session.get("error", ""),
        "error_code": session.get("error_code", ""),
    }


async def _ensure_playwright():
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        raise HTTPException(status_code=500, detail="缺少 playwright，请安装 auth_browser/requirements.txt")
    return await async_playwright().start()


def _now() -> float:
    return time.time()


def _is_active_session(session: dict) -> bool:
    return session.get("status") not in {"closed", "failed", "captured", "expired", "capture_failed"}


async def _close_session_resources(session: dict):
    for key in ("context", "browser"):
        obj = session.get(key)
        if obj is not None:
            await obj.close()
            session[key] = None
    pw = session.get("playwright")
    if pw is not None:
        await pw.stop()
        session["playwright"] = None


async def _expire_session(session_id: str):
    session = sessions.get(session_id)
    if not session or not _is_active_session(session):
        return
    await _close_session_resources(session)
    session["status"] = "expired"
    session["error"] = "登录会话已过期，请重新打开登录页。"
    session["error_code"] = "session_expired"


async def _cleanup_expired_sessions():
    now = _now()
    for session_id, session in list(sessions.items()):
        if _is_active_session(session) and session.get("expires_at", 0) <= now:
            await _expire_session(session_id)


def _active_session_id() -> str:
    for session_id, session in sessions.items():
        if _is_active_session(session):
            return session_id
    return ""


def _error(status_code: int, message: str, code: str):
    raise HTTPException(status_code=status_code, detail={"message": message, "code": code})


@app.get("/health")
async def health(x_auth_browser_token: str | None = Header(default=None)):
    await _require_token(x_auth_browser_token)
    await _cleanup_expired_sessions()
    return {
        "status": "ok",
        "active_session_id": _active_session_id(),
        "session_ttl_seconds": SESSION_TTL_SECONDS,
        "profile_dir_configured": bool(PROFILE_DIR),
        "storage_state_exists": os.path.exists(STORAGE_STATE_PATH),
    }


@app.post("/sessions/start")
async def start_session(x_auth_browser_token: str | None = Header(default=None)):
    await _require_token(x_auth_browser_token)
    await _cleanup_expired_sessions()
    active_session_id = _active_session_id()
    if active_session_id:
        _error(409, "已有活跃登录会话，请先完成或关闭当前会话。", "active_session_exists")
    session_id = uuid.uuid4().hex
    now = _now()
    sessions[session_id] = {
        "status": "starting",
        "viewer_url": _viewer_url(session_id),
        "created_at": now,
        "expires_at": now + SESSION_TTL_SECONDS,
        "error": "",
        "error_code": "",
    }
    try:
        os.makedirs(PROFILE_DIR, exist_ok=True)
        pw = await _ensure_playwright()
        browser = await pw.chromium.launch(headless=HEADLESS, executable_path=CHROMIUM_EXECUTABLE_PATH)
        context_kwargs = {}
        if os.path.exists(STORAGE_STATE_PATH):
            context_kwargs["storage_state"] = STORAGE_STATE_PATH
        context = await browser.new_context(**context_kwargs)
        page = await context.new_page()
        await page.goto(JAVDB_LOGIN_URL)
        sessions[session_id].update({
            "status": "ready_to_capture" if HEADLESS else "waiting_login",
            "playwright": pw,
            "browser": browser,
            "context": context,
            "page": page,
        })
        return _response(session_id)
    except Exception as exc:
        sessions[session_id]["status"] = "failed"
        sessions[session_id]["error"] = str(exc)
        sessions[session_id]["error_code"] = "browser_start_failed"
        _error(500, f"启动授权浏览器失败：{exc}", "browser_start_failed")


@app.get("/sessions/{session_id}/status")
async def get_status(session_id: str, x_auth_browser_token: str | None = Header(default=None)):
    await _require_token(x_auth_browser_token)
    await _cleanup_expired_sessions()
    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="session 不存在")
    data = _response(session_id)
    if sessions[session_id].get("error"):
        data["error"] = sessions[session_id]["error"]
    return data


@app.get("/sessions/{session_id}/viewer")
async def get_viewer(session_id: str):
    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="session 不存在")
    if sessions[session_id].get("status") == "expired":
        raise HTTPException(status_code=410, detail="session 已过期")
    return {
        "session_id": session_id,
        "status": sessions[session_id]["status"],
        "message": "MVP 当前不内置远程浏览器画面。桌面模式会打开本机浏览器；Headless 模式请配置外部远程浏览器入口。",
        "login_url": JAVDB_LOGIN_URL,
    }


@app.post("/sessions/{session_id}/capture")
async def capture_session(session_id: str, x_auth_browser_token: str | None = Header(default=None)):
    await _require_token(x_auth_browser_token)
    await _cleanup_expired_sessions()
    session = sessions.get(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="session 不存在")
    if session.get("status") in {"closed", "failed", "expired", "captured", "capture_failed"}:
        _error(400, "session 当前状态不可捕获", "session_not_capturable")
    context = session.get("context")
    page = session.get("page")
    if context is None or page is None:
        session["status"] = "capture_failed"
        session["error"] = "浏览器上下文不可用"
        session["error_code"] = "context_missing"
        _error(500, "浏览器上下文不可用", "context_missing")
    cookies = await context.cookies("https://javdb.com")
    javdb_cookies = [c for c in cookies if "javdb.com" in c.get("domain", "")]
    cookie_header = "; ".join(f"{c['name']}={c['value']}" for c in javdb_cookies)
    if not cookie_header:
        session["status"] = "capture_failed"
        session["error"] = "未捕获到 JavDB Cookie，请确认已完成登录。"
        session["error_code"] = "cookie_missing"
        _error(400, "未捕获到 JavDB Cookie，请确认已完成登录。", "cookie_missing")
    user_agent = await page.evaluate("() => navigator.userAgent")
    await context.storage_state(path=STORAGE_STATE_PATH)
    await _close_session_resources(session)
    session["status"] = "captured"
    return {
        "session_id": session_id,
        "status": "captured",
        "cookie": cookie_header,
        "user_agent": user_agent,
    }


@app.post("/sessions/{session_id}/close")
async def close_session(session_id: str, x_auth_browser_token: str | None = Header(default=None)):
    await _require_token(x_auth_browser_token)
    session = sessions.get(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="session 不存在")
    await _close_session_resources(session)
    session["status"] = "closed"
    return _response(session_id)
