"""账号密码直登端点（curl_cffi，无需浏览器）。供 WebUI 登录弹窗调用。"""

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from schemas import AuthLoginRequest, AuthSessionRequest
from services import auth_browser_service
from services.auth_browser_service import AuthBrowserError

router = APIRouter()


def _error_response(exc: AuthBrowserError):
    return JSONResponse(status_code=exc.status_code, content={"code": exc.status_code, "msg": str(exc)})


@router.post("/api/auth/browser/start")
def start_auth_browser():
    """开始登录会话：返回验证码图与会话 id。"""
    try:
        return {"code": 200, "data": auth_browser_service.start_session()}
    except AuthBrowserError as exc:
        return _error_response(exc)


@router.get("/api/auth/browser/health")
def check_auth_browser_health():
    try:
        return {"code": 200, "data": auth_browser_service.check_connection()}
    except AuthBrowserError as exc:
        return _error_response(exc)


@router.get("/api/auth/browser/status")
def get_auth_browser_status(session_id: str):
    try:
        return {"code": 200, "data": auth_browser_service.get_session_status(session_id)}
    except AuthBrowserError as exc:
        return _error_response(exc)


@router.post("/api/auth/browser/captcha")
def refresh_auth_captcha(req: AuthSessionRequest):
    """刷新验证码图（用户点图换一张）。"""
    try:
        return {"code": 200, "data": auth_browser_service.refresh_captcha(req.session_id)}
    except AuthBrowserError as exc:
        return _error_response(exc)


@router.post("/api/auth/browser/login")
def submit_auth_login(req: AuthLoginRequest):
    """提交账号/密码/验证码，成功后保存 Cookie。"""
    try:
        data = auth_browser_service.submit_login(
            req.session_id, req.email, req.password, req.captcha, req.remember_cookie
        )
        return {"code": 200, "msg": "登录成功，Cookie 已保存", "data": data}
    except AuthBrowserError as exc:
        return _error_response(exc)


@router.post("/api/auth/browser/close")
def close_auth_browser(req: AuthSessionRequest):
    try:
        return {"code": 200, "data": auth_browser_service.close_session(req.session_id)}
    except AuthBrowserError as exc:
        return _error_response(exc)
