"""settings_repo — 运行配置（Cookie / UA / 代理 / Tracker）的读写。

底层连接与会话 Cookie 状态仍由 db_store 持有：
- connect / _now / _trackers_* 通过 from-import 复用（其 __globals__ 始终是 db_store）。
- _SESSION_COOKIE 为跨调用的会话态，唯一真源在 db_store；configure() 负责重置它，
  因此这里统一通过 db_store._SESSION_COOKIE 读写，避免拆分后出现状态副本。
"""

import logging

import db_store
from db_store import connect, _now, _trackers_to_json, _trackers_from_json
from magnet_scoring import DEFAULT_SCORE_CONDITIONS, validate_score_conditions

COOKIE_SOURCES = {"manual", "android_webview", "auth_browser", "unknown"}
COOKIE_STATUSES = {"missing", "unverified", "valid", "invalid", "expired", "network_error", "blocked"}

__all__ = [
    "save_runtime_config",
    "get_runtime_config",
    "update_cookie_validation_status",
    "COOKIE_SOURCES",
    "COOKIE_STATUSES",
]


def _normalize_cookie_source(source):
    source = (source or "unknown").strip()
    return source if source in COOKIE_SOURCES else "unknown"


def _normalize_cookie_status(status):
    status = (status or "unverified").strip()
    return status if status in COOKIE_STATUSES else "unverified"


def save_runtime_config(
    cookie=None,
    remember_cookie=False,
    user_agent=None,
    proxies=None,
    trackers=None,
    cookie_source=None,
    cookie_status=None,
    cookie_captured_at=None,
    cookie_validated_at=None,
    cookie_last_error=None,
    magnet_score_100_condition=None,
    magnet_score_10_condition=None,
    magnet_score_1_condition=None,
):
    now = _now()
    with connect() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO runtime_config(id, updated_at) VALUES (1, ?)",
            (now,),
        )
        current = conn.execute("SELECT * FROM runtime_config WHERE id = 1").fetchone()
        score_keys = tuple(DEFAULT_SCORE_CONDITIONS)
        stored_score_conditions = {key: current[key] for key in score_keys}
        provided_score_conditions = {
            key: value
            for key, value in {
                "magnet_score_100_condition": magnet_score_100_condition,
                "magnet_score_10_condition": magnet_score_10_condition,
                "magnet_score_1_condition": magnet_score_1_condition,
            }.items()
            if value is not None
        }
        if provided_score_conditions:
            next_score_conditions = dict(stored_score_conditions)
            next_score_conditions.update(provided_score_conditions)
            normalized_score_conditions = validate_score_conditions(next_score_conditions)
        else:
            normalized_score_conditions = stored_score_conditions
        if cookie is not None:
            db_store._SESSION_COOKIE = cookie or ""
        elif not db_store._SESSION_COOKIE and current["remember_cookie"]:
            db_store._SESSION_COOKIE = current["cookie"] or ""
        db_cookie = db_store._SESSION_COOKIE if remember_cookie else ""
        next_source = current["cookie_source"] or "unknown"
        next_captured_at = current["cookie_captured_at"] or 0
        next_status = current["cookie_status"] or "missing"
        next_validated_at = current["cookie_validated_at"] or 0
        next_last_error = current["cookie_last_error"] or ""
        if cookie is not None:
            if db_store._SESSION_COOKIE:
                next_source = _normalize_cookie_source(cookie_source)
                next_captured_at = cookie_captured_at if cookie_captured_at is not None else now
                next_status = _normalize_cookie_status(cookie_status or "unverified")
                next_validated_at = cookie_validated_at if cookie_validated_at is not None else 0
                next_last_error = "" if cookie_last_error is None else str(cookie_last_error or "")
            else:
                next_source = _normalize_cookie_source(cookie_source)
                next_captured_at = 0
                next_status = "missing"
                next_validated_at = 0
                next_last_error = ""
        else:
            if cookie_source is not None:
                next_source = _normalize_cookie_source(cookie_source)
            if cookie_status is not None:
                next_status = _normalize_cookie_status(cookie_status)
            if cookie_captured_at is not None:
                next_captured_at = cookie_captured_at
            if cookie_validated_at is not None:
                next_validated_at = cookie_validated_at
            if cookie_last_error is not None:
                next_last_error = str(cookie_last_error or "")
        conn.execute(
            """
            UPDATE runtime_config
            SET cookie = ?, remember_cookie = ?, user_agent = ?, proxies = ?,
                tracker_list_json = ?, cookie_source = ?, cookie_captured_at = ?,
                cookie_validated_at = ?, cookie_status = ?, cookie_last_error = ?,
                magnet_score_100_condition = ?, magnet_score_10_condition = ?,
                magnet_score_1_condition = ?,
                updated_at = ?
            WHERE id = 1
            """,
            (
                db_cookie,
                1 if remember_cookie else 0,
                current["user_agent"] if user_agent is None else user_agent or "",
                current["proxies"] if proxies is None else proxies or "",
                current["tracker_list_json"] if trackers is None else _trackers_to_json(trackers),
                next_source,
                next_captured_at,
                next_validated_at,
                next_status,
                next_last_error,
                normalized_score_conditions["magnet_score_100_condition"],
                normalized_score_conditions["magnet_score_10_condition"],
                normalized_score_conditions["magnet_score_1_condition"],
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
    cookie = row["cookie"] if remember_cookie else db_store._SESSION_COOKIE
    stored_score_conditions = {
        key: row[key] for key in DEFAULT_SCORE_CONDITIONS
    }
    try:
        normalized_score_conditions = validate_score_conditions(stored_score_conditions)
    except ValueError:
        logging.warning("runtime_config 中的磁力评分条件无效，读取时回退默认映射")
        normalized_score_conditions = dict(DEFAULT_SCORE_CONDITIONS)
    data = {
        "remember_cookie": remember_cookie,
        "has_cookie": bool(cookie),
        "user_agent": row["user_agent"] or "",
        "proxies": row["proxies"] or "",
        "trackers": _trackers_from_json(row["tracker_list_json"]),
        "updated_at": row["updated_at"] or 0,
        "cookie_source": row["cookie_source"] or "unknown",
        "cookie_captured_at": row["cookie_captured_at"] or 0,
        "cookie_validated_at": row["cookie_validated_at"] or 0,
        "cookie_status": row["cookie_status"] or ("unverified" if cookie else "missing"),
        "cookie_last_error": row["cookie_last_error"] or "",
        **normalized_score_conditions,
    }
    if include_cookie:
        data["cookie"] = cookie or ""
    return data


def update_cookie_validation_status(status, validated_at=None, last_error=""):
    now = _now()
    with connect() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO runtime_config(id, updated_at) VALUES (1, ?)",
            (now,),
        )
        conn.execute(
            """
            UPDATE runtime_config
            SET cookie_status = ?, cookie_validated_at = ?, cookie_last_error = ?, updated_at = ?
            WHERE id = 1
            """,
            (
                _normalize_cookie_status(status),
                validated_at if validated_at is not None else now,
                str(last_error or ""),
                now,
            ),
        )
