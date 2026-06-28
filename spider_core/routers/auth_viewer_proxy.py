"""Reverse proxy for the auth-browser noVNC viewer.

将 /auth-viewer/* 反向代理到 auth-browser 容器的 noVNC（websockify）服务，使主程序
单一端口即可对外提供远程浏览器登录画面：

- HTTP：转发 noVNC 静态资源（vnc.html / app / core / vendor 等）。
- WebSocket：在浏览器与 websockify 之间双向桥接 VNC 二进制流。

访问控制：WebSocket（真正的操控通道）仅在存在活跃登录会话时放行；静态资源本身无害。
路径不以 /api/ 开头，故不经 RequireApiTokenMiddleware；这是必要的——noVNC 的 WebSocket
无法携带自定义鉴权头。公网部署时应另加 VNC 密码或路径 token。
"""

import asyncio
import logging

import httpx
import websockets
from fastapi import APIRouter, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import Response
from starlette.websockets import WebSocketState

from app_config import AUTH_BROWSER_VIEWER_INTERNAL_URL
from services import auth_browser_service
from services.auth_browser_service import AuthBrowserError

logger = logging.getLogger("auth_viewer_proxy")
router = APIRouter()

_PREFIX = "/auth-viewer"
# 逐跳头：不应透传给客户端。
_HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade", "content-encoding", "content-length",
}


def _has_active_session() -> bool:
    try:
        health = auth_browser_service.check_connection()
    except AuthBrowserError:
        return False
    return bool(health.get("active_session_id"))


@router.get(_PREFIX + "/{path:path}")
async def proxy_viewer_http(path: str, request: Request):
    upstream = f"{AUTH_BROWSER_VIEWER_INTERNAL_URL}/{path}"
    query = request.url.query
    if query:
        upstream = f"{upstream}?{query}"
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(upstream)
    except httpx.HTTPError as exc:
        logger.warning("auth-viewer HTTP 代理失败 %s: %s", upstream, exc)
        return Response(content=b"auth viewer upstream unavailable", status_code=502)
    headers = {k: v for k, v in resp.headers.items() if k.lower() not in _HOP_BY_HOP}
    return Response(content=resp.content, status_code=resp.status_code, headers=headers)


@router.websocket(_PREFIX + "/websockify")
async def proxy_viewer_ws(client_ws: WebSocket):
    # 仅在存在活跃登录会话时放行（无会话则无浏览器可操控）。
    if not _has_active_session():
        await client_ws.close(code=1008)
        return

    requested = client_ws.scope.get("subprotocols") or []
    subprotocol = "binary" if "binary" in requested else (requested[0] if requested else None)
    await client_ws.accept(subprotocol=subprotocol)

    upstream_url = AUTH_BROWSER_VIEWER_INTERNAL_URL.replace("http://", "ws://", 1).replace("https://", "wss://", 1)
    upstream_url = f"{upstream_url}/websockify"
    upstream_subprotocols = [subprotocol] if subprotocol else None

    try:
        async with websockets.connect(
            upstream_url,
            subprotocols=upstream_subprotocols,
            max_size=None,
            open_timeout=15,
        ) as upstream_ws:
            await _pump_both(client_ws, upstream_ws)
    except (OSError, websockets.WebSocketException, asyncio.TimeoutError) as exc:
        logger.warning("auth-viewer WS 上游连接失败 %s: %s", upstream_url, exc)
        if client_ws.application_state == WebSocketState.CONNECTED:
            await client_ws.close(code=1011)


async def _pump_both(client_ws: WebSocket, upstream_ws):
    async def client_to_upstream():
        try:
            while True:
                message = await client_ws.receive()
                if message.get("type") == "websocket.disconnect":
                    break
                data = message.get("bytes")
                if data is None and message.get("text") is not None:
                    data = message["text"]
                if data is not None:
                    await upstream_ws.send(data)
        except (WebSocketDisconnect, websockets.WebSocketException, RuntimeError):
            pass

    async def upstream_to_client():
        try:
            async for data in upstream_ws:
                if isinstance(data, bytes):
                    await client_ws.send_bytes(data)
                else:
                    await client_ws.send_text(data)
        except (websockets.WebSocketException, RuntimeError):
            pass

    task_a = asyncio.create_task(client_to_upstream())
    task_b = asyncio.create_task(upstream_to_client())
    done, pending = await asyncio.wait({task_a, task_b}, return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        task.cancel()
    try:
        await upstream_ws.close()
    except Exception:
        pass
    if client_ws.application_state == WebSocketState.CONNECTED:
        try:
            await client_ws.close()
        except Exception:
            pass
