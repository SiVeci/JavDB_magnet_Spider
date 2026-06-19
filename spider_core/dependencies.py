"""Reusable FastAPI dependencies and request guards."""

from fastapi import HTTPException

import db_store
from ranking_utils import is_valid_ranking
from storage_utils import UnsafeFilenameError, normalize_csv_filename


def valid_collection(name: str) -> str:
    """Validate a collection path parameter and return its safe filename."""
    try:
        safe_name = normalize_csv_filename(name)
    except UnsafeFilenameError:
        raise HTTPException(status_code=400, detail="文件名非法")
    if not db_store.collection_exists(safe_name):
        raise HTTPException(status_code=404, detail="找不到该集合")
    return safe_name


def valid_ranking(category: str, period: str) -> tuple[str, str]:
    """Validate a ranking route pair and return it unchanged."""
    if not is_valid_ranking(category, period):
        raise HTTPException(status_code=404, detail="排行榜不存在")
    return category, period


def get_active_task_filename() -> str | None:
    """Return the collection filename occupied by the active task, if any."""
    active_task = db_store.get_active_task()
    if not active_task:
        return None
    from utils import get_safe_name

    return get_safe_name(active_task.get("collection_filename") or active_task.get("final_filename"))


def check_not_occupied_by_task(filename: str) -> None:
    """Raise when an active task is currently using filename."""
    if get_active_task_filename() == filename:
        raise HTTPException(status_code=400, detail="该集合正在被任务占用")
