"""movie_repo — 集合 / 影片 / 磁力的 CRUD、查询、磁力选择与 CSV 导入。

底层连接与私有 helper（_collection_id / _rebuild_collection_tags / _movie_magnet_health /
_score_from_check / _reselect_movie_magnet / _matches_tags / 类型转换等）复用 db_store（from-import）。
"""

import csv
import os
import time

from db_store import (
    connect,
    _now,
    _to_int,
    _to_float,
    _normalize_string_list,
    _tags_to_json,
    _tags_from_json,
    _matches_tags,
    _rebuild_collection_tags,
    _collection_id,
    _movie_magnet_health,
    _score_from_check,
    _reselect_movie_magnet,
)
from storage_utils import UnsafeFilenameError, get_safe_csv_path, normalize_csv_filename

__all__ = [
    "ensure_collection",
    "collection_exists",
    "get_collection_source_url",
    "clear_collection",
    "get_existing_codes",
    "save_movie_result",
    "get_history",
    "get_collection_movies",
    "get_movie_magnets",
    "select_movie_magnet",
    "get_collection_movie_ids",
    "auto_select_collection_magnets",
    "update_magnet_check_result",
    "get_magnet_links",
    "get_magnet_links_for_codes",
    "delete_collections",
    "import_csv_file",
    "import_existing_csvs",
]


def ensure_collection(filename, source_url=""):
    with connect() as conn:
        return _collection_id(conn, filename, source_url)


def collection_exists(filename):
    safe_name = normalize_csv_filename(filename)
    with connect() as conn:
        row = conn.execute("SELECT 1 FROM collections WHERE filename = ?", (safe_name,)).fetchone()
        return row is not None


def get_collection_source_url(filename):
    safe_name = normalize_csv_filename(filename)
    with connect() as conn:
        row = conn.execute(
            "SELECT source_url FROM collections WHERE filename = ?",
            (safe_name,),
        ).fetchone()
    return (row["source_url"] or "").strip() if row else ""


def clear_collection(filename):
    safe_name = normalize_csv_filename(filename)
    with connect() as conn:
        row = conn.execute("SELECT id FROM collections WHERE filename = ?", (safe_name,)).fetchone()
        if not row:
            return
        conn.execute("DELETE FROM movies WHERE collection_id = ?", (row["id"],))
        conn.execute(
            "UPDATE collections SET tags_json = '[]', updated_at = ? WHERE id = ?",
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
    movie_tags = _normalize_string_list(movie.get("tags", []))
    with connect() as conn:
        collection_id = _collection_id(conn, safe_name)
        conn.execute(
            """
            INSERT INTO movies(
                collection_id, code, title, url, best_magnet_name, best_magnet_link,
                priority_score, magnet_date, size_mb, tags_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(collection_id, code) DO UPDATE SET
                title = excluded.title,
                url = excluded.url,
                best_magnet_name = excluded.best_magnet_name,
                best_magnet_link = excluded.best_magnet_link,
                priority_score = excluded.priority_score,
                magnet_date = excluded.magnet_date,
                size_mb = excluded.size_mb,
                tags_json = excluded.tags_json,
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
                _tags_to_json(movie_tags),
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
                    movie_id, name, link, base_priority_score, priority_score, magnet_date, size_mb,
                    is_selected, position, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    movie_id,
                    magnet.get("name", ""),
                    magnet.get("link", ""),
                    _to_int(magnet.get("rank", 0)),
                    _to_int(magnet.get("rank", 0)),
                    magnet.get("date", ""),
                    _to_float(magnet.get("size_mb", 0)),
                    1 if magnet.get("link") == best_magnet.get("link") else 0,
                    index,
                    now,
                ),
            )
        _rebuild_collection_tags(conn, collection_id, now)


def get_history():
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT c.filename, c.source_url, c.tags_json, c.created_at, c.updated_at, COUNT(m.id) AS count
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
            "tags": _tags_from_json(row["tags_json"]),
            "time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(row["created_at"])),
            "timestamp": row["updated_at"],
            "has_source_url": bool((row["source_url"] or "").strip()),
        }
        for row in rows
    ]


def get_collection_movies(filename):
    safe_name = normalize_csv_filename(filename)
    with connect() as conn:
        collection = conn.execute(
            "SELECT tags_json FROM collections WHERE filename = ?",
            (safe_name,),
        ).fetchone()
        rows = conn.execute(
            """
            SELECT m.id, m.code, m.title, m.url, m.best_magnet_name, m.best_magnet_link,
                   m.priority_score, m.magnet_date, m.size_mb, m.tags_json,
                   COUNT(mg.id) AS candidate_count,
                   SUM(CASE WHEN mg.check_status = 'active' THEN 1 ELSE 0 END) AS active_count,
                   SUM(CASE WHEN mg.check_status = 'weak' THEN 1 ELSE 0 END) AS weak_count,
                   SUM(CASE WHEN mg.check_status = 'dead' THEN 1 ELSE 0 END) AS dead_count,
                   SUM(CASE WHEN mg.check_error IS NOT NULL AND mg.check_error != '' AND mg.check_status IS NULL THEN 1 ELSE 0 END) AS failed_count,
                   SUM(CASE WHEN mg.checked_at IS NOT NULL OR (mg.check_error IS NOT NULL AND mg.check_error != '') THEN 1 ELSE 0 END) AS checked_count
            FROM movies m
            JOIN collections c ON c.id = m.collection_id
            LEFT JOIN magnets mg ON mg.movie_id = m.id
            WHERE c.filename = ?
            GROUP BY m.id
            ORDER BY m.id
            """,
            (safe_name,),
        ).fetchall()
    movies = []
    for row in rows:
        item = dict(row)
        item["tags"] = _tags_from_json(item.pop("tags_json", ""))
        item["magnet_health"] = _movie_magnet_health(item)
        movies.append(item)
    return {
        "movies": movies,
        "available_tags": _tags_from_json(collection["tags_json"] if collection else ""),
        "total_count": len(movies),
    }


def get_movie_magnets(movie_id):
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT id, movie_id, name, link, base_priority_score, priority_score,
                   magnet_date, size_mb, is_selected, position, created_at,
                   check_status, seeders, leechers, checked_at, check_error
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


def get_collection_movie_ids(filename):
    safe_name = normalize_csv_filename(filename)
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT m.id
            FROM movies m
            JOIN collections c ON c.id = m.collection_id
            WHERE c.filename = ?
            ORDER BY m.id
            """,
            (safe_name,),
        ).fetchall()
    return [row["id"] for row in rows]


def auto_select_collection_magnets(filenames):
    safe_names = [normalize_csv_filename(filename) for filename in filenames]
    now = _now()
    updated = 0
    with connect() as conn:
        for filename in safe_names:
            rows = conn.execute(
                """
                SELECT m.id
                FROM movies m
                JOIN collections c ON c.id = m.collection_id
                WHERE c.filename = ?
                ORDER BY m.id
                """,
                (filename,),
            ).fetchall()
            for row in rows:
                if _reselect_movie_magnet(conn, row["id"], now):
                    updated += 1
    return updated


def update_magnet_check_result(magnet_id, check_status, seeders=0, leechers=0, check_error=None):
    if check_status not in {"active", "weak", "dead", None}:
        raise ValueError(f"Invalid magnet check status: {check_status}")
    now = _now()
    error = (check_error or "").strip()
    with connect() as conn:
        row = conn.execute("SELECT * FROM magnets WHERE id = ?", (magnet_id,)).fetchone()
        if not row:
            return False
        priority_score = _score_from_check(row["base_priority_score"], check_status, error)
        conn.execute(
            """
            UPDATE magnets
            SET check_status = ?, seeders = ?, leechers = ?, checked_at = ?,
                check_error = ?, priority_score = ?
            WHERE id = ?
            """,
            (
                check_status,
                max(0, _to_int(seeders)),
                max(0, _to_int(leechers)),
                now,
                error or None,
                priority_score,
                magnet_id,
            ),
        )
        _reselect_movie_magnet(conn, row["movie_id"], now)
    return True


def get_magnet_links(filename, required_tags=None, exclude_tags=None):
    safe_name = normalize_csv_filename(filename)
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT m.best_magnet_link, m.tags_json
            FROM movies m
            JOIN collections c ON c.id = m.collection_id
            WHERE c.filename = ? AND m.best_magnet_link != ''
            ORDER BY m.id
            """,
            (safe_name,),
        ).fetchall()
    return [row["best_magnet_link"] for row in rows if _matches_tags(row["tags_json"], required_tags, exclude_tags)]


def get_magnet_links_for_codes(filename, codes):
    safe_name = normalize_csv_filename(filename)
    ordered_codes = []
    seen = set()
    for code in codes or []:
        value = str(code or "").strip()
        if value and value not in seen:
            ordered_codes.append(value)
            seen.add(value)
    if not ordered_codes:
        return []
    placeholders = ",".join("?" for _ in ordered_codes)
    with connect() as conn:
        rows = conn.execute(
            f"""
            SELECT m.code, m.best_magnet_link
            FROM movies m
            JOIN collections c ON c.id = m.collection_id
            WHERE c.filename = ?
              AND m.code IN ({placeholders})
              AND m.best_magnet_link != ''
            """,
            [safe_name] + ordered_codes,
        ).fetchall()
    links_by_code = {row["code"]: row["best_magnet_link"] for row in rows}
    return [links_by_code[code] for code in ordered_codes if links_by_code.get(code)]


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
                    priority_score, magnet_date, size_mb, tags_json, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '[]', ?, ?)
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
                        movie_id, name, link, base_priority_score, priority_score, magnet_date, size_mb,
                        is_selected, position, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, 1, 0, ?)
                    """,
                    (
                        movie_id,
                        best["name"],
                        best["link"],
                        _to_int(best["rank"]),
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
        _rebuild_collection_tags(conn, collection_id, now)
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
