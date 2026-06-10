"""settings_repo — 运行配置（Cookie / UA / 代理 / Tracker）的读写。

底层连接与会话 Cookie 状态仍由 db_store 持有：
- connect / _now / _trackers_* 通过 from-import 复用（其 __globals__ 始终是 db_store）。
- _SESSION_COOKIE 为跨调用的会话态，唯一真源在 db_store；configure() 负责重置它，
  因此这里统一通过 db_store._SESSION_COOKIE 读写，避免拆分后出现状态副本。
"""

import db_store
from db_store import connect, _now, _trackers_to_json, _trackers_from_json

__all__ = ["save_runtime_config", "get_runtime_config"]


def save_runtime_config(cookie=None, remember_cookie=False, user_agent=None, proxies=None, trackers=None):
    now = _now()
    with connect() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO runtime_config(id, updated_at) VALUES (1, ?)",
            (now,),
        )
        current = conn.execute("SELECT * FROM runtime_config WHERE id = 1").fetchone()
        if cookie is not None:
            db_store._SESSION_COOKIE = cookie or ""
        elif not db_store._SESSION_COOKIE and current["remember_cookie"]:
            db_store._SESSION_COOKIE = current["cookie"] or ""
        db_cookie = db_store._SESSION_COOKIE if remember_cookie else ""
        conn.execute(
            """
            UPDATE runtime_config
            SET cookie = ?, remember_cookie = ?, user_agent = ?, proxies = ?,
                tracker_list_json = ?, updated_at = ?
            WHERE id = 1
            """,
            (
                db_cookie,
                1 if remember_cookie else 0,
                current["user_agent"] if user_agent is None else user_agent or "",
                current["proxies"] if proxies is None else proxies or "",
                current["tracker_list_json"] if trackers is None else _trackers_to_json(trackers),
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
    data = {
        "remember_cookie": remember_cookie,
        "has_cookie": bool(cookie),
        "user_agent": row["user_agent"] or "",
        "proxies": row["proxies"] or "",
        "trackers": _trackers_from_json(row["tracker_list_json"]),
        "updated_at": row["updated_at"] or 0,
    }
    if include_cookie:
        data["cookie"] = cookie or ""
    return data
