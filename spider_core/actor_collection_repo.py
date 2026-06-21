"""actor_collection_repo — 收藏演员清单快照（collection_actors 表）的 CRUD。

底层 connect / _now / 字符串列表 JSON 互转复用 db_store（from-import）。
快照语义见 PRD §3 / §4 / §11：
- 按 {actor_id} 唯一区分演员，跨分类去重，一个 actor_id 只归属一个具体分类（首见为准）。
- 单分类刷新覆盖该分类行；演员行的本地操作记录（last_task_tags）在 actor_id 仍存在时保留。
- 不保存头像、Cookie、令牌等敏感数据。
"""

import json

from db_store import connect, _now
from ranking_utils import COLLECTION_TYPE_ACTOR

__all__ = [
    "list_collection_actors",
    "replace_category_snapshot",
    "set_actor_last_task_tags",
    "get_collection_actor",
]


def _tags_from_json(value):
    """收藏演员标签为 [{name,value}] 对象数组；非法数据回退为空列表。"""
    if not value:
        return []
    try:
        data = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return []
    if not isinstance(data, list):
        return []
    tags = []
    for item in data:
        if isinstance(item, dict) and item.get("value"):
            tags.append({"name": str(item.get("name") or ""), "value": str(item.get("value"))})
    return tags


def _tags_to_json(tags):
    normalized = []
    seen = set()
    for item in tags or []:
        if not isinstance(item, dict):
            continue
        value = str(item.get("value") or "").strip()
        if not value or value in seen:
            continue
        normalized.append({"name": str(item.get("name") or "").strip(), "value": value})
        seen.add(value)
    return json.dumps(normalized, ensure_ascii=False)


def _row_to_actor(row):
    return {
        "actor_id": row["actor_id"],
        "actor_name": row["actor_name"] or "",
        "actor_url": row["actor_url"] or "",
        "category": row["category"] or "",
        "source_category_url": row["source_category_url"] or "",
        "last_task_tags": _tags_from_json(row["last_task_tags"]),
        "refreshed_at": row["refreshed_at"] or 0,
        "has_collection": bool(row["collection_filename"]),
        "collection_filename": row["collection_filename"] or "",
    }


def list_collection_actors():
    """返回全部收藏演员，并按 {actor_id} 关联演员类数据集合判断是否已入库（PRD §7.4/§10.4）。"""
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT a.actor_id, a.actor_name, a.actor_url, a.category,
                   a.source_category_url, a.last_task_tags, a.refreshed_at,
                   c.filename AS collection_filename
            FROM collection_actors a
            LEFT JOIN collections c
                ON c.collection_type = ?
               AND c.actor_id = a.actor_id
            ORDER BY a.category, a.actor_name
            """,
            (COLLECTION_TYPE_ACTOR,),
        ).fetchall()
    return [_row_to_actor(row) for row in rows]


def replace_category_snapshot(category, source_url, actors):
    """以最新远端结果覆盖写入某个具体分类的收藏演员快照（PRD §4.3）。

    - 先删除该 category 的旧行，再逐个 upsert。
    - 冲突（actor_id 已存在于其它分类）时不改写 category/last_task_tags/created_at，
      保持“首见分类为准”与本地标签记录（PRD §3/§4.6）。
    调用方需保证传入的是“刷新成功”的完整快照；失败分类不应进入此函数（PRD §12.2）。
    """
    category = (category or "").strip()
    now = _now()
    items = []
    seen = set()
    for actor in actors or []:
        actor_id = str(actor.get("actor_id") or "").strip()
        if not actor_id or actor_id in seen:
            continue
        seen.add(actor_id)
        items.append(actor)
    new_ids = [str(a.get("actor_id")).strip() for a in items]
    with connect() as conn:
        # 仅删除“本分类下已取消收藏”的演员，保留仍存在演员行的 last_task_tags（PRD §4.6/§11）。
        if new_ids:
            placeholders = ",".join("?" for _ in new_ids)
            conn.execute(
                f"DELETE FROM collection_actors WHERE category = ? AND actor_id NOT IN ({placeholders})",
                (category, *new_ids),
            )
        else:
            conn.execute("DELETE FROM collection_actors WHERE category = ?", (category,))
        for actor in items:
            actor_id = str(actor.get("actor_id")).strip()
            conn.execute(
                """
                INSERT INTO collection_actors(
                    actor_id, actor_name, actor_url, category, source_category_url,
                    last_task_tags, refreshed_at, created_at
                )
                VALUES (?, ?, ?, ?, ?, '[]', ?, ?)
                ON CONFLICT(actor_id) DO UPDATE SET
                    actor_name = excluded.actor_name,
                    actor_url = excluded.actor_url,
                    refreshed_at = excluded.refreshed_at
                """,
                (
                    actor_id,
                    str(actor.get("actor_name") or "").strip(),
                    str(actor.get("actor_url") or "").strip(),
                    category,
                    source_url or "",
                    now,
                    now,
                ),
            )


def set_actor_last_task_tags(actor_id, tags):
    """记录演员行最后一次添加任务所选标签（仅在添加成功后调用，PRD §8.7/§8.8）。"""
    actor_id = str(actor_id or "").strip()
    if not actor_id:
        return False
    with connect() as conn:
        cursor = conn.execute(
            "UPDATE collection_actors SET last_task_tags = ? WHERE actor_id = ?",
            (_tags_to_json(tags), actor_id),
        )
        return cursor.rowcount > 0


def get_collection_actor(actor_id):
    actor_id = str(actor_id or "").strip()
    if not actor_id:
        return None
    with connect() as conn:
        row = conn.execute(
            """
            SELECT a.actor_id, a.actor_name, a.actor_url, a.category,
                   a.source_category_url, a.last_task_tags, a.refreshed_at,
                   c.filename AS collection_filename
            FROM collection_actors a
            LEFT JOIN collections c
                ON c.collection_type = ?
               AND c.actor_id = a.actor_id
            WHERE a.actor_id = ?
            """,
            (COLLECTION_TYPE_ACTOR, actor_id),
        ).fetchone()
    return _row_to_actor(row) if row else None
