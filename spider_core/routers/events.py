"""routers/events — SSE 实时事件流端点。

EventSource 不支持自定义请求头，token 通过 URL query 参数 ?token=xxx 传递。
鉴权逻辑单独实现，不经过 require_api_token 中间件（中间件检查 header，此处检查 query）。
"""

import asyncio
import json

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from app_config import is_api_authorized, is_auth_required
from services import event_bus
from services.queue_service import get_queue_status_data
from services.task_service import task_to_response
import db_store

router = APIRouter()


async def _event_generator(request: Request, queue: asyncio.Queue, sub_id: str):
    """SSE 生成器：先发一次全量快照，再持续推送变更事件。"""
    try:
        # 初始全量快照
        tasks = db_store.list_tasks(limit=100)
        queue_status = get_queue_status_data()
        current = db_store.get_current_task()
        status_data = task_to_response(current, include_logs=True) if current else {
            "state": "idle", "progress": "0/0", "current": "-", "logs": ["等待任务启动..."]
        }
        snapshot = {
            "type": "snapshot",
            "tasks": [task_to_response(t) for t in tasks],
            "queue": queue_status,
            "logs": status_data.get("logs", []),
            "collectionsChanged": False,
        }
        yield f"data: {json.dumps(snapshot, ensure_ascii=False)}\n\n"

        # 持续推送变更
        while True:
            if await request.is_disconnected():
                break
            try:
                event = await asyncio.wait_for(queue.get(), timeout=25.0)
                yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
            except asyncio.TimeoutError:
                # 发送心跳保持连接
                yield ": ping\n\n"
    finally:
        event_bus.unsubscribe(sub_id)


@router.get("/api/events")
async def sse_events(request: Request, token: str = ""):
    """SSE 实时事件流。客户端通过 ?token=xxx 传递访问令牌（无鉴权要求时可省略）。"""
    if is_auth_required() and not is_api_authorized(token or None):
        from fastapi.responses import JSONResponse
        return JSONResponse(
            status_code=401,
            content={"code": 401, "msg": "访问令牌缺失或无效"},
        )

    sub_id, queue = event_bus.subscribe()
    return StreamingResponse(
        _event_generator(request, queue, sub_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )
