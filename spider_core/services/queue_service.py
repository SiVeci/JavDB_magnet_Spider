"""Task queue thread management and status reporting."""

import threading

import db_store
from services.task_service import task_to_response
from spider_engine import STATUS_FILE, run_task
from storage_utils import atomic_write_json

QUEUE_LOCK = threading.RLock()
QUEUE_THREAD = None


def write_status_mirror(task=None):
    if not task:
        empty_status = {
            "state": "idle",
            "progress": "0/0",
            "current": "-",
            "logs": ["等待任务启动..."],
        }
        atomic_write_json(STATUS_FILE, empty_status, indent=2)
        return
    data = task_to_response(task, include_logs=True)
    atomic_write_json(STATUS_FILE, data, indent=2)

def queue_worker():
    global QUEUE_THREAD
    try:
        while True:
            task = db_store.claim_next_pending_task()
            if not task:
                write_status_mirror(db_store.get_current_task())
                return
            write_status_mirror(task)
            try:
                run_task(task["task_id"])
            except Exception as e:
                db_store.update_task_status(
                    task["task_id"],
                    state="failed",
                    current="任务异常",
                    log_msg=f"任务执行异常: {str(e)}",
                    error_message=str(e),
                )
            current = db_store.get_task(task["task_id"])
            write_status_mirror(current)
            if current and current["state"] in {"paused", "waiting_cookie", "waiting_choice", "failed"}:
                return
    finally:
        with QUEUE_LOCK:
            QUEUE_THREAD = None

def ensure_queue_worker():
    global QUEUE_THREAD
    with QUEUE_LOCK:
        if QUEUE_THREAD and QUEUE_THREAD.is_alive():
            return
        QUEUE_THREAD = threading.Thread(target=queue_worker, daemon=True)
        QUEUE_THREAD.start()

def is_queue_running():
    return bool(QUEUE_THREAD and QUEUE_THREAD.is_alive())

def get_queue_status_data():
    tasks = db_store.list_tasks(limit=200)
    pending_count = sum(1 for task in tasks if task["state"] == "pending")
    counts = db_store.count_tasks_by_state()
    finished_count = sum(counts.get(state, 0) for state in db_store.FINISHED_TASK_STATES)
    active_count = sum(count for state, count in counts.items() if state not in db_store.FINISHED_TASK_STATES)
    current = db_store.get_active_task()
    if not current:
        for task in tasks:
            if task["state"] in {"waiting_cookie", "waiting_choice", "paused"}:
                current = task
                break
    blocking = bool(
        current
        and current["state"] in {
            "running",
            "pause_requested",
            "cancel_requested",
            "waiting_cookie",
            "waiting_choice",
            "paused",
        }
    )
    return {
        "queue_state": "running" if is_queue_running() else ("blocked" if blocking else "idle"),
        "pending_count": pending_count,
        "active_count": active_count,
        "finished_count": finished_count,
        "current_task_id": current["task_id"] if current else "",
        "can_start": pending_count > 0 and not is_queue_running() and not blocking,
    }
