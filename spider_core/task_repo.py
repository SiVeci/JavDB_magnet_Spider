"""task_repo — 任务（tasks / task_logs）的 CRUD 与状态机。

底层 connect / _now / _row_to_task / _normalize_state 复用 db_store（from-import）。
跨职责调用运行配置（settings_repo 的函数）时使用 db_store.xxx，运行时经 db_store 门面解析。
"""

import time
import uuid

import db_store
from db_store import connect, _now, _row_to_task, _normalize_state
from storage_utils import normalize_csv_filename

__all__ = [
    "create_task",
    "get_task",
    "list_tasks",
    "count_tasks_by_state",
    "cleanup_finished_tasks",
    "delete_task",
    "get_task_logs",
    "append_task_log",
    "update_task",
    "update_task_status",
    "save_task_checkpoint",
    "load_task_checkpoint",
    "clear_task_checkpoint",
    "claim_next_pending_task",
    "has_active_task",
    "get_active_task",
    "get_current_task",
    "recover_interrupted_tasks",
    "request_task_pause",
    "resume_task_to_pending",
    "request_task_cancel",
    "update_task_cookie",
    "update_task_mode",
]


def create_task(start_url, cookie="", user_agent="", filename="", proxies=None, crawl_mode=""):
    now = _now()
    task_id = uuid.uuid4().hex
    requested_filename = normalize_csv_filename(filename, allow_empty=True)
    if crawl_mode and crawl_mode not in {"incremental", "overwrite"}:
        raise ValueError(f"Invalid crawl mode: {crawl_mode}")
    with connect() as conn:
        conn.execute(
            """
            INSERT INTO tasks(
                task_id, start_url, requested_filename, final_filename,
                collection_filename, crawl_mode, state, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?)
            """,
            (
                task_id,
                start_url,
                requested_filename,
                requested_filename,
                requested_filename,
                crawl_mode or "",
                now,
                now,
            ),
        )
    append_task_log(task_id, "任务已加入队列")
    return task_id


def get_task(task_id):
    with connect() as conn:
        row = conn.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,)).fetchone()
    return _row_to_task(row)


def list_tasks(limit=50):
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT * FROM tasks
            ORDER BY
                CASE WHEN state IN ('finished', 'canceled', 'failed') THEN 1 ELSE 0 END,
                created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [_row_to_task(row) for row in rows]


def count_tasks_by_state():
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT state, COUNT(*) AS count
            FROM tasks
            GROUP BY state
            """
        ).fetchall()
    return {row["state"]: row["count"] for row in rows}


def cleanup_finished_tasks():
    with connect() as conn:
        cursor = conn.execute(
            """
            DELETE FROM tasks
            WHERE state IN ('finished', 'canceled', 'failed')
            """
        )
        return cursor.rowcount


def delete_task(task_id):
    with connect() as conn:
        row = conn.execute("SELECT task_id FROM tasks WHERE task_id = ?", (task_id,)).fetchone()
        if not row:
            return False
        conn.execute("DELETE FROM task_logs WHERE task_id = ?", (task_id,))
        checkpoint_table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'task_checkpoints'"
        ).fetchone()
        if checkpoint_table:
            conn.execute("DELETE FROM task_checkpoints WHERE task_id = ?", (task_id,))
        conn.execute("DELETE FROM tasks WHERE task_id = ?", (task_id,))
        return True


def get_task_logs(task_id, limit=80):
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT message, created_at FROM task_logs
            WHERE task_id = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (task_id, limit),
        ).fetchall()
    return [row["message"] for row in reversed(rows)]


def append_task_log(task_id, message):
    now = _now()
    time_str = time.strftime("%H:%M:%S", time.localtime(now))
    with connect() as conn:
        conn.execute(
            "INSERT INTO task_logs(task_id, message, created_at) VALUES (?, ?, ?)",
            (task_id, f"[{time_str}] {message}", now),
        )
        conn.execute("UPDATE tasks SET updated_at = ? WHERE task_id = ?", (now, task_id))


def update_task(task_id, **fields):
    if not fields:
        return
    if "state" in fields:
        fields["state"] = _normalize_state(fields["state"])
    fields["updated_at"] = _now()
    columns = ", ".join(f"{key} = ?" for key in fields)
    values = list(fields.values()) + [task_id]
    with connect() as conn:
        conn.execute(f"UPDATE tasks SET {columns} WHERE task_id = ?", values)


def update_task_status(
    task_id,
    state=None,
    progress=None,
    current=None,
    log_msg=None,
    final_filename=None,
    added_count=None,
    error_message=None,
):
    updates = {}
    if state:
        updates["state"] = state
        if state == "running":
            updates["started_at"] = _now()
        if state in {"finished", "failed", "canceled"}:
            updates["finished_at"] = _now()
    if progress is not None:
        updates["progress"] = progress
    if current is not None:
        updates["current"] = current
    if final_filename:
        safe_name = normalize_csv_filename(final_filename)
        updates["final_filename"] = safe_name
        updates["collection_filename"] = safe_name
    if added_count is not None:
        updates["added_count"] = int(added_count)
    if error_message is not None:
        updates["error_message"] = error_message
    if updates:
        update_task(task_id, **updates)
    if log_msg:
        append_task_log(task_id, log_msg)


def save_task_checkpoint(task_id, data):
    import json

    update_task(task_id, checkpoint_json=json.dumps(data, ensure_ascii=False))


def load_task_checkpoint(task_id):
    import json

    task = get_task(task_id)
    if not task or not task.get("checkpoint_json"):
        return None
    try:
        return json.loads(task["checkpoint_json"])
    except json.JSONDecodeError:
        return None


def clear_task_checkpoint(task_id):
    update_task(task_id, checkpoint_json="")


def claim_next_pending_task():
    now = _now()
    with connect() as conn:
        row = conn.execute(
            """
            SELECT task_id FROM tasks
            WHERE state = 'pending'
            ORDER BY created_at ASC
            LIMIT 1
            """
        ).fetchone()
        if not row:
            return None
        task_id = row["task_id"]
        conn.execute(
            """
            UPDATE tasks
            SET state = 'running', started_at = ?, updated_at = ?
            WHERE task_id = ? AND state = 'pending'
            """,
            (now, now, task_id),
        )
        changed = conn.total_changes
    if changed:
        append_task_log(task_id, "任务开始运行")
        return get_task(task_id)
    return None


def has_active_task():
    with connect() as conn:
        row = conn.execute(
            "SELECT 1 FROM tasks WHERE state IN ('running', 'pause_requested', 'cancel_requested') LIMIT 1"
        ).fetchone()
    return row is not None


def get_active_task():
    with connect() as conn:
        row = conn.execute(
            """
            SELECT * FROM tasks
            WHERE state IN ('running', 'pause_requested', 'cancel_requested')
            ORDER BY updated_at DESC
            LIMIT 1
            """
        ).fetchone()
    return _row_to_task(row)


def get_current_task():
    with connect() as conn:
        row = conn.execute(
            """
            SELECT * FROM tasks
            ORDER BY
                CASE state
                    WHEN 'running' THEN 0
                    WHEN 'pause_requested' THEN 1
                    WHEN 'cancel_requested' THEN 2
                    WHEN 'waiting_cookie' THEN 3
                    WHEN 'waiting_choice' THEN 4
                    WHEN 'paused' THEN 5
                    WHEN 'pending' THEN 6
                    ELSE 7
                END,
                updated_at DESC
            LIMIT 1
            """
        ).fetchone()
    return _row_to_task(row)


def recover_interrupted_tasks():
    now = _now()
    with connect() as conn:
        conn.execute(
            """
            UPDATE tasks
            SET state = 'paused', updated_at = ?
            WHERE state IN ('running', 'pause_requested')
            """,
            (now,),
        )
        conn.execute(
            """
            UPDATE tasks
            SET state = 'canceled', finished_at = ?, updated_at = ?
            WHERE state = 'cancel_requested'
            """,
            (now, now),
        )


def request_task_pause(task_id):
    task = get_task(task_id)
    if not task:
        return False
    if task["state"] == "running":
        update_task_status(task_id, state="pause_requested", log_msg="收到暂停请求")
        return True
    if task["state"] in {"pending", "paused", "waiting_cookie", "waiting_choice"}:
        update_task_status(task_id, state="paused", log_msg="任务已暂停")
        return True
    return False


def resume_task_to_pending(task_id):
    task = get_task(task_id)
    if not task or task["state"] not in {"paused", "waiting_cookie", "waiting_choice"}:
        return False
    update_task_status(task_id, state="pending", log_msg="任务已恢复到队列")
    return True


def request_task_cancel(task_id):
    task = get_task(task_id)
    if not task:
        return False
    if task["state"] == "running":
        update_task_status(task_id, state="cancel_requested", log_msg="收到取消请求")
        return True
    if task["state"] in {"pending", "paused", "waiting_cookie", "waiting_choice", "pause_requested"}:
        update_task_status(task_id, state="canceled", log_msg="任务已取消")
        return True
    return False


def update_task_cookie(task_id, cookie):
    if not get_task(task_id):
        return False
    current = db_store.get_runtime_config(include_cookie=False)
    db_store.save_runtime_config(
        cookie=cookie or "",
        remember_cookie=current["remember_cookie"],
        user_agent=current["user_agent"],
        proxies=current["proxies"],
    )
    append_task_log(task_id, "Cookie 已更新")
    return True


def update_task_mode(task_id, mode):
    if mode not in {"incremental", "overwrite"}:
        return False
    task = get_task(task_id)
    if not task:
        return False
    update_task(task_id, crawl_mode=mode)
    append_task_log(task_id, f"已选择爬取模式: {mode}")
    return True
