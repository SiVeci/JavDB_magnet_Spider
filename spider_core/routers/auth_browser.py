"""Routes that proxy Auth Browser Service operations for the WebUI."""

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from schemas import AuthBrowserSessionRequest
from services import auth_browser_service
from services.auth_browser_service import AuthBrowserError

router = APIRouter()


def _error_response(exc: AuthBrowserError):
    return JSONResponse(status_code=exc.status_code, content={"code": exc.status_code, "msg": str(exc)})


@router.post("/api/auth/browser/start")
def start_auth_browser():
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


@router.post("/api/auth/browser/capture")
def capture_auth_browser(req: AuthBrowserSessionRequest):
    try:
        data = auth_browser_service.capture_session(req.session_id, req.remember_cookie)
        return {"code": 200, "msg": "Cookie 已捕获并保存", "data": data}
    except AuthBrowserError as exc:
        return _error_response(exc)


@router.post("/api/auth/browser/close")
def close_auth_browser(req: AuthBrowserSessionRequest):
    try:
        return {"code": 200, "data": auth_browser_service.close_session(req.session_id)}
    except AuthBrowserError as exc:
        return _error_response(exc)
