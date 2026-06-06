import csv
import io
import os
import sqlite3
import time
import uuid

from storage_utils import UnsafeFilenameError, get_safe_csv_path, normalize_csv_filename


DB_FILENAME = "spider_data.db"
TASK_STATES = {
    "pending",
    "running",
    "pause_requested",
    "paused",
    "waiting_cookie",
    "waiting_choice",
    "cancel_requested",
    "canceled",
    "finished",
    "failed",
}
FINISHED_TASK_STATES = {"finished", "canceled", "failed"}
CSV_FIELDNAMES = [
    "影片番号",
    "原始标题",
    "影片链接",
    "最佳资源文件名",
    "磁力链接",
    "优先级得分",
    "日期",
    "文件大小(MB)",
]

_DATA_DIR = None
_DB_PATH = None
_SESSION_COOKIE = ""


def configure(data_dir):
    global _DATA_DIR, _DB_PATH, _SESSION_COOKIE
    _DATA_DIR = os.path.abspath(data_dir)
    os.makedirs(_DATA_DIR, exist_ok=True)
    _DB_PATH = os.path.join(_DATA_DIR, DB_FILENAME)
    _SESSION_COOKIE = ""
    init_database()
    return _DB_PATH


def get_db_path():
    if not _DB_PATH:
        raise RuntimeError("Database is not configured")
    return _DB_PATH


def connect():
    conn = sqlite3.connect(get_db_path(), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def init_database():
    with connect() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS collections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT NOT NULL UNIQUE,
                source_url TEXT DEFAULT '',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS movies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                collection_id INTEGER NOT NULL,
                code TEXT NOT NULL,
                title TEXT DEFAULT '',
                url TEXT DEFAULT '',
                best_magnet_name TEXT DEFAULT '',
                best_magnet_link TEXT DEFAULT '',
                priority_score INTEGER DEFAULT 0,
                magnet_date TEXT DEFAULT '',
                size_mb REAL DEFAULT 0,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                FOREIGN KEY(collection_id) REFERENCES collections(id) ON DELETE CASCADE,
                UNIQUE(collection_id, code)
            );

            CREATE TABLE IF NOT EXISTS magnets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                movie_id INTEGER NOT NULL,
                name TEXT DEFAULT '',
                link TEXT NOT NULL,
                priority_score INTEGER DEFAULT 0,
                magnet_date TEXT DEFAULT '',
                size_mb REAL DEFAULT 0,
                is_selected INTEGER DEFAULT 0,
                position INTEGER DEFAULT 0,
                created_at REAL NOT NULL,
                FOREIGN KEY(movie_id) REFERENCES movies(id) ON DELETE CASCADE,
                UNIQUE(movie_id, link)
            );

            CREATE INDEX IF NOT EXISTS idx_movies_collection_code
                ON movies(collection_id, code);
            CREATE INDEX IF NOT EXISTS idx_magnets_movie_selected
                ON magnets(movie_id, is_selected);

            CREATE TABLE IF NOT EXISTS tasks (
                task_id TEXT PRIMARY KEY,
                start_url TEXT NOT NULL,
                requested_filename TEXT DEFAULT '',
                final_filename TEXT DEFAULT '',
                collection_filename TEXT DEFAULT '',
                crawl_mode TEXT DEFAULT '',
                state TEXT NOT NULL,
                progress TEXT DEFAULT '0/0',
                current TEXT DEFAULT '-',
                checkpoint_json TEXT DEFAULT '',
                error_message TEXT DEFAULT '',
                added_count INTEGER DEFAULT 0,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                started_at REAL DEFAULT 0,
                finished_at REAL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS task_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT NOT NULL,
                message TEXT NOT NULL,
                created_at REAL NOT NULL,
                FOREIGN KEY(task_id) REFERENCES tasks(task_id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_tasks_state_created
                ON tasks(state, created_at);
            CREATE INDEX IF NOT EXISTS idx_task_logs_task_created
                ON task_logs(task_id, created_at);

            CREATE TABLE IF NOT EXISTS runtime_config (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                cookie TEXT DEFAULT '',
                remember_cookie INTEGER DEFAULT 0,
                user_agent TEXT DEFAULT '',
                proxies TEXT DEFAULT '',
                updated_at REAL NOT NULL
            );
            """
        )
    _migrate_task_runtime_columns()


def _migrate_task_runtime_columns():
    with connect() as conn:
        columns = [row["name"] for row in conn.execute("PRAGMA table_info(tasks)").fetchall()]
        if not {"cookie", "user_agent", "proxies"}.intersection(columns):
            return
        now = _now()
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS tasks_new (
                task_id TEXT PRIMARY KEY,
                start_url TEXT NOT NULL,
                requested_filename TEXT DEFAULT '',
                final_filename TEXT DEFAULT '',
                collection_filename TEXT DEFAULT '',
                crawl_mode TEXT DEFAULT '',
                state TEXT NOT NULL,
                progress TEXT DEFAULT '0/0',
                current TEXT DEFAULT '-',
                checkpoint_json TEXT DEFAULT '',
                error_message TEXT DEFAULT '',
                added_count INTEGER DEFAULT 0,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                started_at REAL DEFAULT 0,
                finished_at REAL DEFAULT 0
            );
            INSERT OR IGNORE INTO tasks_new(
                task_id, start_url, requested_filename, final_filename, collection_filename,
                crawl_mode, state, progress, current, checkpoint_json, error_message,
                added_count, created_at, updated_at, started_at, finished_at
            )
            SELECT
                task_id, start_url, requested_filename, final_filename, collection_filename,
                crawl_mode, state, progress, current, checkpoint_json, error_message,
                added_count, created_at, updated_at, started_at, finished_at
            FROM tasks;
            DROP TABLE tasks;
            ALTER TABLE tasks_new RENAME TO tasks;
            CREATE INDEX IF NOT EXISTS idx_tasks_state_created
                ON tasks(state, created_at);
            """
        )
        conn.execute(
            "INSERT OR IGNORE INTO runtime_config(id, updated_at) VALUES (1, ?)",
            (now,),
        )
        conn.execute("PRAGMA foreign_keys = ON")


def _now():
    return time.time()


def _to_float(value, default=0.0):
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value, default=0):
    return int(_to_float(value, default))


def _normalize_state(state):
    if state not in TASK_STATES:
        raise ValueError(f"Invalid task state: {state}")
    return state


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


def _row_to_task(row):
    if not row:
        return None
    return dict(row)


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


def save_runtime_config(cookie=None, remember_cookie=False, user_agent=None, proxies=None):
    global _SESSION_COOKIE
    now = _now()
    with connect() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO runtime_config(id, updated_at) VALUES (1, ?)",
            (now,),
        )
        current = conn.execute("SELECT * FROM runtime_config WHERE id = 1").fetchone()
        if cookie is not None:
            _SESSION_COOKIE = cookie or ""
        elif not _SESSION_COOKIE and current["remember_cookie"]:
            _SESSION_COOKIE = current["cookie"] or ""
        db_cookie = _SESSION_COOKIE if remember_cookie else ""
        conn.execute(
            """
            UPDATE runtime_config
            SET cookie = ?, remember_cookie = ?, user_agent = ?, proxies = ?, updated_at = ?
            WHERE id = 1
            """,
            (
                db_cookie,
                1 if remember_cookie else 0,
                current["user_agent"] if user_agent is None else user_agent or "",
                current["proxies"] if proxies is None else proxies or "",
                now,
            ),
        )


def get_runtime_config(include_cookie=True):
    with connect() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO runtime_config(id, updated_at) VALUES (1, ?)",
            (_now(),),
        )
        row = conn.execute("SELECT * FROM runtime_config WHERE id = 1").fetchone()
    remember_cookie = bool(row["remember_cookie"])
    cookie = row["cookie"] if remember_cookie else _SESSION_COOKIE
    data = {
        "remember_cookie": remember_cookie,
        "has_cookie": bool(cookie),
        "user_agent": row["user_agent"] or "",
        "proxies": row["proxies"] or "",
        "updated_at": row["updated_at"] or 0,
    }
    if include_cookie:
        data["cookie"] = cookie or ""
    return data


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
    current = get_runtime_config(include_cookie=False)
    save_runtime_config(
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


def _collection_id(conn, filename, source_url=""):
    safe_name = normalize_csv_filename(filename)
    now = _now()
    conn.execute(
        """
        INSERT INTO collections(filename, source_url, created_at, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(filename) DO UPDATE SET
            source_url = CASE
                WHEN excluded.source_url != '' THEN excluded.source_url
                ELSE collections.source_url
            END,
            updated_at = excluded.updated_at
        """,
        (safe_name, source_url or "", now, now),
    )
    row = conn.execute("SELECT id FROM collections WHERE filename = ?", (safe_name,)).fetchone()
    return row["id"]


def ensure_collection(filename, source_url=""):
    with connect() as conn:
        return _collection_id(conn, filename, source_url)


def collection_exists(filename):
    safe_name = normalize_csv_filename(filename)
    with connect() as conn:
        row = conn.execute("SELECT 1 FROM collections WHERE filename = ?", (safe_name,)).fetchone()
        return row is not None


def clear_collection(filename):
    safe_name = normalize_csv_filename(filename)
    with connect() as conn:
        row = conn.execute("SELECT id FROM collections WHERE filename = ?", (safe_name,)).fetchone()
        if not row:
            return
        conn.execute("DELETE FROM movies WHERE collection_id = ?", (row["id"],))
        conn.execute(
            "UPDATE collections SET updated_at = ? WHERE id = ?",
            (_now(), row["id"]),
        )


def get_existing_codes(filename):
    safe_name = normalize_csv_filename(filename)
    with connect() as conn:
        row = conn.execute("SELECT id FROM collections WHERE filename = ?", (safe_name,)).fetchone()
        if not row:
            return set()
        rows = conn.execute(
            "SELECT code FROM movies WHERE collection_id = ?",
            (row["id"],),
        ).fetchall()
    return {r["code"] for r in rows if r["code"]}


def save_movie_result(filename, movie, best_magnet, candidates):
    safe_name = normalize_csv_filename(filename)
    now = _now()
    with connect() as conn:
        collection_id = _collection_id(conn, safe_name)
        conn.execute(
            """
            INSERT INTO movies(
                collection_id, code, title, url, best_magnet_name, best_magnet_link,
                priority_score, magnet_date, size_mb, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(collection_id, code) DO UPDATE SET
                title = excluded.title,
                url = excluded.url,
                best_magnet_name = excluded.best_magnet_name,
                best_magnet_link = excluded.best_magnet_link,
                priority_score = excluded.priority_score,
                magnet_date = excluded.magnet_date,
                size_mb = excluded.size_mb,
                updated_at = excluded.updated_at
            """,
            (
                collection_id,
                movie.get("code", ""),
                movie.get("title", ""),
                movie.get("url", ""),
                best_magnet.get("name", ""),
                best_magnet.get("link", ""),
                _to_int(best_magnet.get("rank", 0)),
                best_magnet.get("date", ""),
                _to_float(best_magnet.get("size_mb", 0)),
                now,
                now,
            ),
        )
        movie_id = conn.execute(
            "SELECT id FROM movies WHERE collection_id = ? AND code = ?",
            (collection_id, movie.get("code", "")),
        ).fetchone()["id"]
        conn.execute("DELETE FROM magnets WHERE movie_id = ?", (movie_id,))
        for index, magnet in enumerate(candidates):
            conn.execute(
                """
                INSERT INTO magnets(
                    movie_id, name, link, priority_score, magnet_date, size_mb,
                    is_selected, position, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    movie_id,
                    magnet.get("name", ""),
                    magnet.get("link", ""),
                    _to_int(magnet.get("rank", 0)),
                    magnet.get("date", ""),
                    _to_float(magnet.get("size_mb", 0)),
                    1 if magnet.get("link") == best_magnet.get("link") else 0,
                    index,
                    now,
                ),
            )
        conn.execute(
            "UPDATE collections SET updated_at = ? WHERE id = ?",
            (now, collection_id),
        )


def get_history():
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT c.filename, c.created_at, c.updated_at, COUNT(m.id) AS count
            FROM collections c
            LEFT JOIN movies m ON m.collection_id = c.id
            GROUP BY c.id
            ORDER BY c.updated_at DESC
            """
        ).fetchall()
    return [
        {
            "name": row["filename"],
            "count": row["count"],
            "time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(row["created_at"])),
            "timestamp": row["updated_at"],
        }
        for row in rows
    ]


def get_collection_movies(filename):
    safe_name = normalize_csv_filename(filename)
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT m.id, m.code, m.title, m.url, m.best_magnet_name, m.best_magnet_link,
                   m.priority_score, m.magnet_date, m.size_mb, COUNT(mg.id) AS candidate_count
            FROM movies m
            JOIN collections c ON c.id = m.collection_id
            LEFT JOIN magnets mg ON mg.movie_id = m.id
            WHERE c.filename = ?
            GROUP BY m.id
            ORDER BY m.id
            """,
            (safe_name,),
        ).fetchall()
    return [dict(row) for row in rows]


def get_movie_magnets(movie_id):
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT id, movie_id, name, link, priority_score, magnet_date, size_mb,
                   is_selected, position, created_at
            FROM magnets
            WHERE movie_id = ?
            ORDER BY position ASC, id ASC
            """,
            (movie_id,),
        ).fetchall()
    return [dict(row) for row in rows]


def select_movie_magnet(movie_id, magnet_id):
    now = _now()
    with connect() as conn:
        magnet = conn.execute(
            "SELECT * FROM magnets WHERE id = ? AND movie_id = ?",
            (magnet_id, movie_id),
        ).fetchone()
        if not magnet:
            return False
        conn.execute("UPDATE magnets SET is_selected = 0 WHERE movie_id = ?", (movie_id,))
        conn.execute("UPDATE magnets SET is_selected = 1 WHERE id = ?", (magnet_id,))
        conn.execute(
            """
            UPDATE movies
            SET best_magnet_name = ?, best_magnet_link = ?, priority_score = ?,
                magnet_date = ?, size_mb = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                magnet["name"],
                magnet["link"],
                magnet["priority_score"],
                magnet["magnet_date"],
                magnet["size_mb"],
                now,
                movie_id,
            ),
        )
        row = conn.execute("SELECT collection_id FROM movies WHERE id = ?", (movie_id,)).fetchone()
        if row:
            conn.execute("UPDATE collections SET updated_at = ? WHERE id = ?", (now, row["collection_id"]))
    return True


def _export_rows(conn, filename):
    safe_name = normalize_csv_filename(filename)
    rows = conn.execute(
        """
        SELECT m.code, m.title, m.url, m.best_magnet_name, m.best_magnet_link,
               m.priority_score, m.magnet_date, m.size_mb
        FROM movies m
        JOIN collections c ON c.id = m.collection_id
        WHERE c.filename = ?
        ORDER BY m.id
        """,
        (safe_name,),
    ).fetchall()
    return [
        {
            "影片番号": row["code"],
            "原始标题": row["title"],
            "影片链接": row["url"],
            "最佳资源文件名": row["best_magnet_name"],
            "磁力链接": row["best_magnet_link"],
            "优先级得分": row["priority_score"],
            "日期": row["magnet_date"],
            "文件大小(MB)": row["size_mb"],
        }
        for row in rows
    ]


def export_collection_to_csv_bytes(filename):
    safe_name = normalize_csv_filename(filename)
    with connect() as conn:
        if not conn.execute("SELECT 1 FROM collections WHERE filename = ?", (safe_name,)).fetchone():
            return None, safe_name
        rows = _export_rows(conn, safe_name)

    buffer = io.StringIO(newline="")
    buffer.write("\ufeff")
    writer = csv.DictWriter(buffer, fieldnames=CSV_FIELDNAMES)
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue().encode("utf-8"), safe_name


def get_magnet_links(filename):
    safe_name = normalize_csv_filename(filename)
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT m.best_magnet_link
            FROM movies m
            JOIN collections c ON c.id = m.collection_id
            WHERE c.filename = ? AND m.best_magnet_link != ''
            ORDER BY m.id
            """,
            (safe_name,),
        ).fetchall()
    return [row["best_magnet_link"] for row in rows]


def delete_collections(filenames, data_dir):
    deleted = []
    missing = []
    with connect() as conn:
        for filename in filenames:
            safe_name = normalize_csv_filename(filename)
            result = conn.execute("DELETE FROM collections WHERE filename = ?", (safe_name,))
            if result.rowcount:
                deleted.append(safe_name)
            else:
                missing.append(safe_name)

    for safe_name in deleted:
        try:
            path, _ = get_safe_csv_path(data_dir, safe_name)
            if os.path.exists(path):
                os.remove(path)
        except (OSError, UnsafeFilenameError):
            pass
    return deleted, missing


def import_csv_file(path, filename):
    safe_name = normalize_csv_filename(filename)
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        ensure_collection(safe_name)
        return 0

    imported = 0
    with connect() as conn:
        collection_id = _collection_id(conn, safe_name)
        existing = {
            row["code"]
            for row in conn.execute(
                "SELECT code FROM movies WHERE collection_id = ?",
                (collection_id,),
            ).fetchall()
        }
        now = _now()
        for row in rows:
            code = (row.get("影片番号") or "").strip()
            if not code or code in existing:
                continue
            best = {
                "name": row.get("最佳资源文件名", ""),
                "link": row.get("磁力链接", ""),
                "rank": row.get("优先级得分", 0) or 0,
                "date": row.get("日期", ""),
                "size_mb": row.get("文件大小(MB)", 0) or 0,
            }
            movie = {
                "code": code,
                "title": row.get("原始标题", ""),
                "url": row.get("影片链接", ""),
            }
            conn.execute(
                """
                INSERT INTO movies(
                    collection_id, code, title, url, best_magnet_name, best_magnet_link,
                    priority_score, magnet_date, size_mb, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    collection_id,
                    movie["code"],
                    movie["title"],
                    movie["url"],
                    best["name"],
                    best["link"],
                    _to_int(best["rank"]),
                    best["date"],
                    _to_float(best["size_mb"]),
                    now,
                    now,
                ),
            )
            movie_id = conn.execute(
                "SELECT id FROM movies WHERE collection_id = ? AND code = ?",
                (collection_id, movie["code"]),
            ).fetchone()["id"]
            if best["link"]:
                conn.execute(
                    """
                    INSERT INTO magnets(
                        movie_id, name, link, priority_score, magnet_date, size_mb,
                        is_selected, position, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, 1, 0, ?)
                    """,
                    (
                        movie_id,
                        best["name"],
                        best["link"],
                        _to_int(best["rank"]),
                        best["date"],
                        _to_float(best["size_mb"]),
                        now,
                    ),
                )
            existing.add(code)
            imported += 1
        conn.execute(
            "UPDATE collections SET updated_at = ? WHERE id = ?",
            (now, collection_id),
        )
    return imported


def import_existing_csvs(data_dir):
    imported = 0
    if not os.path.exists(data_dir):
        return imported
    for filename in os.listdir(data_dir):
        if not filename.lower().endswith(".csv"):
            continue
        try:
            path, safe_name = get_safe_csv_path(data_dir, filename)
            imported += import_csv_file(path, safe_name)
            os.remove(path)
        except (OSError, UnsafeFilenameError, csv.Error):
            continue
    return imported
