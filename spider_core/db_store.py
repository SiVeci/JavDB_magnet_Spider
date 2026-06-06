import csv
import io
import os
import sqlite3
import time

from storage_utils import UnsafeFilenameError, get_safe_csv_path, normalize_csv_filename


DB_FILENAME = "spider_data.db"
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


def configure(data_dir):
    global _DATA_DIR, _DB_PATH
    _DATA_DIR = os.path.abspath(data_dir)
    os.makedirs(_DATA_DIR, exist_ok=True)
    _DB_PATH = os.path.join(_DATA_DIR, DB_FILENAME)
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
            """
        )


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
