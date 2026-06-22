"""SSE 事件总线：维护订阅者队列，由 queue_service 在状态变更时广播。"""

import asyncio
import threading
from typing import Dict

# 订阅者：{subscriber_id: asyncio.Queue}
_subscribers: Dict[str, asyncio.Queue] = {}
_lock = threading.Lock()
_id_counter = 0


def subscribe() -> tuple[str, asyncio.Queue]:
    """注册一个新订阅者，返回 (sub_id, queue)。"""
    global _id_counter
    with _lock:
        _id_counter += 1
        sub_id = str(_id_counter)
        q: asyncio.Queue = asyncio.Queue(maxsize=32)
        _subscribers[sub_id] = q
        return sub_id, q


def unsubscribe(sub_id: str) -> None:
    with _lock:
        _subscribers.pop(sub_id, None)


def broadcast(event: dict) -> None:
    """从同步线程（queue_worker）广播事件到所有订阅者。"""
    with _lock:
        subs = list(_subscribers.values())
    for q in subs:
        try:
            q.put_nowait(event)
        except asyncio.QueueFull:
            # 消费者跟不上时丢弃最旧的事件，再放新的
            try:
                q.get_nowait()
                q.put_nowait(event)
            except Exception:
                pass
