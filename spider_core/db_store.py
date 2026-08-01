import csv
import io
import json
import os
import re
import sqlite3
import time
import uuid

from ranking_utils import (
    COLLECTION_TYPE_ACTOR,
    COLLECTION_TYPE_RANKING,
    TOP250_CATEGORY,
    is_valid_ranking,
    parse_ranking_url,
    top250_option_label,
)
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
                collection_type TEXT DEFAULT 'actor',
                ranking_category TEXT DEFAULT '',
                ranking_period TEXT DEFAULT '',
                actor_id TEXT DEFAULT '',
                tags_json TEXT DEFAULT '[]',
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
                tags_json TEXT DEFAULT '[]',
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
                has_uncensored INTEGER,
                has_hd INTEGER,
                has_subtitle INTEGER,
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
                collection_type TEXT DEFAULT 'actor',
                ranking_category TEXT DEFAULT '',
                ranking_period TEXT DEFAULT '',
                state TEXT NOT NULL,
                progress TEXT DEFAULT '0/0',
                current TEXT DEFAULT '-',
                checkpoint_json TEXT DEFAULT '',
                error_message TEXT DEFAULT '',
                task_cookie_failure_count INTEGER DEFAULT 0,
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
                tracker_list_json TEXT DEFAULT '[]',
                magnet_score_100_condition TEXT DEFAULT 'uncensored',
                magnet_score_10_condition TEXT DEFAULT 'hd',
                magnet_score_1_condition TEXT DEFAULT 'subtitle',
                updated_at REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS ranking_option_cache (
                cache_key TEXT PRIMARY KEY,
                options_json TEXT NOT NULL,
                source_url TEXT DEFAULT '',
                updated_at REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS collection_actors (
                actor_id TEXT PRIMARY KEY,
                actor_name TEXT NOT NULL DEFAULT '',
                actor_url TEXT NOT NULL DEFAULT '',
                category TEXT NOT NULL DEFAULT '',
                source_category_url TEXT NOT NULL DEFAULT '',
                last_task_tags TEXT NOT NULL DEFAULT '[]',
                refreshed_at REAL NOT NULL DEFAULT 0,
                created_at REAL NOT NULL DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS idx_collection_actors_category
                ON collection_actors(category, actor_name);
            """
        )
    _migrate_task_runtime_columns()
    _migrate_collection_type_columns()
    _migrate_tag_columns()
    _migrate_magnet_check_columns()
    _migrate_magnet_condition_columns()
    _migrate_runtime_tracker_column()
    _migrate_cookie_lifecycle_columns()
    _migrate_runtime_magnet_score_columns()
    _migrate_task_cookie_failure_column()
    _migrate_actor_id_column()


def _ensure_column(conn, table, column, definition):
    columns = [row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    if column not in columns:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        return True
    return False


def _migrate_tag_columns():
    with connect() as conn:
        _ensure_column(conn, "collections", "tags_json", "TEXT DEFAULT '[]'")
        _ensure_column(conn, "movies", "tags_json", "TEXT DEFAULT '[]'")


def _migrate_magnet_check_columns():
    with connect() as conn:
        added_base_score = _ensure_column(conn, "magnets", "base_priority_score", "INTEGER DEFAULT 0")
        _ensure_column(conn, "magnets", "check_status", "TEXT DEFAULT NULL")
        _ensure_column(conn, "magnets", "seeders", "INTEGER DEFAULT 0")
        _ensure_column(conn, "magnets", "leechers", "INTEGER DEFAULT 0")
        _ensure_column(conn, "magnets", "checked_at", "REAL DEFAULT NULL")
        _ensure_column(conn, "magnets", "check_error", "TEXT DEFAULT NULL")
        if added_base_score:
            conn.execute("UPDATE magnets SET base_priority_score = priority_score")


def _migrate_magnet_condition_columns():
    with connect() as conn:
        _ensure_column(conn, "magnets", "has_uncensored", "INTEGER")
        _ensure_column(conn, "magnets", "has_hd", "INTEGER")
        _ensure_column(conn, "magnets", "has_subtitle", "INTEGER")


def _migrate_runtime_tracker_column():
    with connect() as conn:
        _ensure_column(conn, "runtime_config", "tracker_list_json", "TEXT DEFAULT '[]'")


def _migrate_cookie_lifecycle_columns():
    with connect() as conn:
        _ensure_column(conn, "runtime_config", "cookie_source", "TEXT DEFAULT 'unknown'")
        _ensure_column(conn, "runtime_config", "cookie_captured_at", "REAL DEFAULT 0")
        _ensure_column(conn, "runtime_config", "cookie_validated_at", "REAL DEFAULT 0")
        _ensure_column(conn, "runtime_config", "cookie_status", "TEXT DEFAULT 'missing'")
        _ensure_column(conn, "runtime_config", "cookie_last_error", "TEXT DEFAULT ''")


def _migrate_runtime_magnet_score_columns():
    with connect() as conn:
        _ensure_column(conn, "runtime_config", "magnet_score_100_condition", "TEXT DEFAULT 'uncensored'")
        _ensure_column(conn, "runtime_config", "magnet_score_10_condition", "TEXT DEFAULT 'hd'")
        _ensure_column(conn, "runtime_config", "magnet_score_1_condition", "TEXT DEFAULT 'subtitle'")


def _migrate_task_cookie_failure_column():
    with connect() as conn:
        _ensure_column(conn, "tasks", "task_cookie_failure_count", "INTEGER DEFAULT 0")


def _extract_actor_id(url):
    """从 /actors/{actor_id} 形式的 URL 中解析演员唯一标识，找不到返回空串。"""
    match = re.search(r"/actors/([^/?#]+)", url or "")
    return match.group(1) if match else ""


def _migrate_actor_id_column():
    with connect() as conn:
        _ensure_column(conn, "collections", "actor_id", "TEXT DEFAULT ''")
        rows = conn.execute(
            """
            SELECT id, source_url FROM collections
            WHERE collection_type = 'actor'
              AND IFNULL(actor_id, '') = ''
              AND IFNULL(source_url, '') != ''
            """
        ).fetchall()
        now = _now()
        for row in rows:
            actor_id = _extract_actor_id(row["source_url"])
            if actor_id:
                conn.execute(
                    "UPDATE collections SET actor_id = ?, updated_at = ? WHERE id = ?",
                    (actor_id, now, row["id"]),
                )


def _migrate_collection_type_columns():
    with connect() as conn:
        _ensure_column(conn, "collections", "collection_type", "TEXT DEFAULT 'actor'")
        _ensure_column(conn, "collections", "ranking_category", "TEXT DEFAULT ''")
        _ensure_column(conn, "collections", "ranking_period", "TEXT DEFAULT ''")
        _ensure_column(conn, "tasks", "collection_type", "TEXT DEFAULT 'actor'")
        _ensure_column(conn, "tasks", "ranking_category", "TEXT DEFAULT ''")
        _ensure_column(conn, "tasks", "ranking_period", "TEXT DEFAULT ''")

        now = _now()
        rows = conn.execute("SELECT id, source_url FROM collections WHERE source_url != ''").fetchall()
        for row in rows:
            ranking = parse_ranking_url(row["source_url"])
            if not ranking:
                continue
            conn.execute(
                """
                UPDATE collections
                SET collection_type = ?, ranking_category = ?, ranking_period = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    COLLECTION_TYPE_RANKING,
                    ranking["ranking_category"],
                    ranking["ranking_period"],
                    now,
                    row["id"],
                ),
            )

        rows = conn.execute("SELECT task_id, start_url FROM tasks WHERE start_url != ''").fetchall()
        for row in rows:
            ranking = parse_ranking_url(row["start_url"])
            if not ranking:
                continue
            conn.execute(
                """
                UPDATE tasks
                SET collection_type = ?, ranking_category = ?, ranking_period = ?, updated_at = ?
                WHERE task_id = ?
                """,
                (
                    COLLECTION_TYPE_RANKING,
                    ranking["ranking_category"],
                    ranking["ranking_period"],
                    now,
                    row["task_id"],
                ),
            )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_collections_type_ranking
                ON collections(collection_type, ranking_category, ranking_period, updated_at)
            """
        )


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
                task_cookie_failure_count INTEGER DEFAULT 0,
                added_count INTEGER DEFAULT 0,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                started_at REAL DEFAULT 0,
                finished_at REAL DEFAULT 0
            );
            INSERT OR IGNORE INTO tasks_new(
                task_id, start_url, requested_filename, final_filename, collection_filename,
                crawl_mode, state, progress, current, checkpoint_json, error_message,
                task_cookie_failure_count, added_count, created_at, updated_at, started_at, finished_at
            )
            SELECT
                task_id, start_url, requested_filename, final_filename, collection_filename,
                crawl_mode, state, progress, current, checkpoint_json, error_message,
                0, added_count, created_at, updated_at, started_at, finished_at
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


def _normalize_string_list(values):
    """去重并去空白的字符串列表（标签、Tracker 通用）。"""
    if not values:
        return []
    normalized = []
    seen = set()
    for item in values:
        value = str(item or "").strip()
        if value and value not in seen:
            normalized.append(value)
            seen.add(value)
    return normalized


def _string_list_to_json(values):
    """Serialize a normalized string list as JSON."""
    return json.dumps(_normalize_string_list(values), ensure_ascii=False)


def _string_list_from_json(value):
    """Deserialize a JSON string into a normalized string list."""
    if not value:
        return []
    try:
        data = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return []
    return _normalize_string_list(data if isinstance(data, list) else [])


_tags_to_json = _string_list_to_json
_tags_from_json = _string_list_from_json


def _normalize_ranking_options(options):
    normalized = []
    seen = set()
    for item in options or []:
        key = str((item or {}).get("key") or "").strip()
        label = str((item or {}).get("label") or "").strip()
        if not key or key in seen:
            continue
        normalized.append({"key": key, "label": label or key})
        seen.add(key)
    return normalized


def get_ranking_option_cache(cache_key):
    key = str(cache_key or "").strip()
    if not key:
        return None
    with connect() as conn:
        row = conn.execute(
            """
            SELECT cache_key, options_json, source_url, updated_at
            FROM ranking_option_cache
            WHERE cache_key = ?
            """,
            (key,),
        ).fetchone()
    if not row:
        return None
    try:
        options = json.loads(row["options_json"])
    except (TypeError, json.JSONDecodeError):
        options = []
    return {
        "cache_key": row["cache_key"],
        "options": _normalize_ranking_options(options),
        "source_url": row["source_url"] or "",
        "updated_at": row["updated_at"] or 0,
    }


def save_ranking_option_cache(cache_key, options, source_url=""):
    key = str(cache_key or "").strip()
    if not key:
        return
    normalized = _normalize_ranking_options(options)
    now = _now()
    with connect() as conn:
        conn.execute(
            """
            INSERT INTO ranking_option_cache(cache_key, options_json, source_url, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(cache_key) DO UPDATE SET
                options_json = excluded.options_json,
                source_url = excluded.source_url,
                updated_at = excluded.updated_at
            """,
            (key, json.dumps(normalized, ensure_ascii=False), source_url or "", now),
        )


def get_local_top250_options():
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT ranking_period, MAX(updated_at) AS updated_at
            FROM collections
            WHERE collection_type = ?
              AND ranking_category = ?
              AND ranking_period != ''
            GROUP BY ranking_period
            ORDER BY updated_at DESC
            """,
            (COLLECTION_TYPE_RANKING, TOP250_CATEGORY),
        ).fetchall()
    return [
        {"key": row["ranking_period"], "label": top250_option_label(row["ranking_period"])}
        for row in rows
        if row["ranking_period"]
    ]


_trackers_to_json = _string_list_to_json
_trackers_from_json = _string_list_from_json


def _matches_tags(row_tags_json, required_tags, exclude_tags=None):
    row_tags = set(_tags_from_json(row_tags_json))

    if exclude_tags:
        excluded = set(_normalize_string_list(exclude_tags))
        if row_tags.intersection(excluded):
            return False

    required = set(_normalize_string_list(required_tags))
    if not required:
        return True
    return required.issubset(row_tags)


def _rebuild_collection_tags(conn, collection_id, now=None):
    union = []
    seen = set()
    rows = conn.execute(
        "SELECT tags_json FROM movies WHERE collection_id = ? ORDER BY id",
        (collection_id,),
    ).fetchall()
    for row in rows:
        for tag in _tags_from_json(row["tags_json"]):
            if tag not in seen:
                union.append(tag)
                seen.add(tag)
    conn.execute(
        "UPDATE collections SET tags_json = ?, updated_at = ? WHERE id = ?",
        (_tags_to_json(union), now or _now(), collection_id),
    )
    return union


def _row_to_task(row):
    if not row:
        return None
    return dict(row)


def _collection_id(
    conn,
    filename,
    source_url="",
    collection_type=COLLECTION_TYPE_ACTOR,
    ranking_category="",
    ranking_period="",
    actor_id="",
):
    safe_name = normalize_csv_filename(filename)
    now = _now()
    collection_type = (collection_type or COLLECTION_TYPE_ACTOR).strip()
    ranking_category = (ranking_category or "").strip()
    ranking_period = (ranking_period or "").strip()
    actor_id = (actor_id or "").strip()
    ranking = parse_ranking_url(source_url)
    if ranking:
        collection_type = COLLECTION_TYPE_RANKING
        ranking_category = ranking["ranking_category"]
        ranking_period = ranking["ranking_period"]
    existing = conn.execute(
        "SELECT collection_type, ranking_category, ranking_period FROM collections WHERE filename = ?",
        (safe_name,),
    ).fetchone()
    if (
        existing
        and collection_type == COLLECTION_TYPE_ACTOR
        and not source_url
        and existing["collection_type"] == COLLECTION_TYPE_RANKING
    ):
        collection_type = COLLECTION_TYPE_RANKING
        ranking_category = existing["ranking_category"] or ""
        ranking_period = existing["ranking_period"] or ""
    if collection_type != COLLECTION_TYPE_RANKING or not is_valid_ranking(ranking_category, ranking_period):
        collection_type = COLLECTION_TYPE_ACTOR
        ranking_category = ""
        ranking_period = ""
    # 仅演员类集合记录 actor_id；榜单集合强制留空（PRD §10.6）。
    if collection_type == COLLECTION_TYPE_ACTOR:
        if not actor_id:
            actor_id = _extract_actor_id(source_url)
    else:
        actor_id = ""
    conn.execute(
        """
        INSERT INTO collections(
            filename, source_url, collection_type, ranking_category, ranking_period,
            actor_id, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(filename) DO UPDATE SET
            source_url = CASE
                WHEN excluded.source_url != '' THEN excluded.source_url
                ELSE collections.source_url
            END,
            collection_type = excluded.collection_type,
            ranking_category = excluded.ranking_category,
            ranking_period = excluded.ranking_period,
            actor_id = CASE
                WHEN excluded.actor_id != '' THEN excluded.actor_id
                ELSE collections.actor_id
            END,
            updated_at = excluded.updated_at
        """,
        (safe_name, source_url or "", collection_type, ranking_category, ranking_period, actor_id, now, now),
    )
    row = conn.execute("SELECT id FROM collections WHERE filename = ?", (safe_name,)).fetchone()
    return row["id"]


def _movie_magnet_health(movie):
    candidate_count = int(movie.get("candidate_count") or 0)
    active_count = int(movie.get("active_count") or 0)
    weak_count = int(movie.get("weak_count") or 0)
    dead_count = int(movie.get("dead_count") or 0)
    failed_count = int(movie.get("failed_count") or 0)
    checked_count = int(movie.get("checked_count") or 0)
    if not candidate_count or not checked_count:
        return None
    if active_count > 0:
        return "active"
    if weak_count > 0:
        return "weak"
    if dead_count > 0:
        return "dead"
    if failed_count >= candidate_count:
        return "failed"
    return None


def _score_from_check(base_score, check_status, check_error):
    score = _to_int(base_score)
    if check_status == "dead" or check_error:
        return score - 200
    return score


def _reselect_movie_magnet(conn, movie_id, now=None):
    now = now or _now()
    rows = conn.execute(
        """
        SELECT *
        FROM magnets
        WHERE movie_id = ?
        ORDER BY position ASC, id ASC
        """,
        (movie_id,),
    ).fetchall()
    if not rows:
        return False
    viable = [row for row in rows if row["check_status"] in {"active", "weak"}]
    candidates = viable if viable else rows
    selected = sorted(
        candidates,
        key=lambda row: (-_to_int(row["priority_score"]), _to_int(row["position"]), _to_int(row["id"])),
    )[0]
    conn.execute("UPDATE magnets SET is_selected = 0 WHERE movie_id = ?", (movie_id,))
    conn.execute("UPDATE magnets SET is_selected = 1 WHERE id = ?", (selected["id"],))
    conn.execute(
        """
        UPDATE movies
        SET best_magnet_name = ?, best_magnet_link = ?, priority_score = ?,
            magnet_date = ?, size_mb = ?, updated_at = ?
        WHERE id = ?
        """,
        (
            selected["name"],
            selected["link"],
            selected["priority_score"],
            selected["magnet_date"],
            selected["size_mb"],
            now,
            movie_id,
        ),
    )
    row = conn.execute("SELECT collection_id FROM movies WHERE id = ?", (movie_id,)).fetchone()
    if row:
        conn.execute("UPDATE collections SET updated_at = ? WHERE id = ?", (now, row["collection_id"]))
    return True


def _export_rows(conn, filename, required_tags=None, exclude_tags=None):
    safe_name = normalize_csv_filename(filename)
    rows = conn.execute(
        """
        SELECT m.code, m.title, m.url, m.best_magnet_name, m.best_magnet_link,
               m.priority_score, m.magnet_date, m.size_mb, m.tags_json
        FROM movies m
        JOIN collections c ON c.id = m.collection_id
        WHERE c.filename = ?
        ORDER BY m.id
        """,
        (safe_name,),
    ).fetchall()
    rows = [row for row in rows if _matches_tags(row["tags_json"], required_tags, exclude_tags)]
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


# ---------------------------------------------------------------------------
# 门面 re-export：公共函数实现已按职责拆分到各 repo / service 模块。
# 这里统一重新导出，保持 db_store.xxx() 的历史调用接口完全不变（零回归）。
# 必须置于文件末尾：此时上方的底层与私有 helper 均已定义，
# 各 repo 在导入时 `from db_store import ...` 可正确解析。
# ---------------------------------------------------------------------------
from export_service import export_collection_to_csv_bytes  # noqa: E402,F401
from movie_repo import (  # noqa: E402,F401
    auto_select_collection_magnets,
    clear_collection,
    collection_exists,
    delete_collections,
    ensure_collection,
    get_collection_movie_ids,
    get_collection_movies,
    get_collection_source_url,
    get_actor_collection_filename_by_actor_id,
    get_existing_codes,
    get_history,
    get_magnet_links,
    get_magnet_links_for_codes,
    get_movie_magnets,
    get_ranking_collection_filename,
    get_ranking_magnet_links,
    get_ranking_movie_ids,
    get_ranking_movies,
    import_csv_file,
    import_existing_csvs,
    resolve_ranking_collection_filename,
    save_movie_result,
    select_movie_magnet,
    update_magnet_check_result,
)
from actor_collection_repo import (  # noqa: E402,F401
    get_collection_actor,
    list_collection_actors,
    replace_category_snapshot,
    set_actor_last_task_tags,
)
from settings_repo import get_runtime_config, save_runtime_config, update_cookie_validation_status  # noqa: E402,F401
from task_repo import (  # noqa: E402,F401
    append_task_log,
    claim_next_pending_task,
    cleanup_finished_tasks,
    clear_task_checkpoint,
    count_tasks_by_state,
    create_task,
    delete_task,
    get_active_task,
    get_current_task,
    get_task,
    get_task_logs,
    has_active_task,
    increment_task_cookie_failure_count,
    list_tasks,
    load_task_checkpoint,
    recover_interrupted_tasks,
    request_task_cancel,
    request_task_pause,
    resume_task_to_pending,
    save_task_checkpoint,
    update_task,
    update_task_cookie,
    update_task_mode,
    update_task_status,
    reset_task_cookie_failure_count,
)
