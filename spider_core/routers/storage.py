"""routers/storage — 集合删除、CSV 下载、磁力链接导出端点。"""

from urllib.parse import quote

from fastapi import APIRouter
from fastapi.responses import JSONResponse, Response

import db_store
from spider_engine import DATA_DIR
from storage_utils import UnsafeFilenameError, normalize_csv_filename
from main import DeleteRequest, get_safe_name, parse_tag_filter

router = APIRouter()


@router.post("/api/delete")
def delete_history(req: DeleteRequest):
    success_count = 0
    fail_count = 0
    fail_reasons = []

    active_file = None
    active_task = db_store.get_active_task()
    if active_task:
        active_file = get_safe_name(active_task.get("collection_filename") or active_task.get("final_filename"))

    deletable = []
    for filename in req.filenames:
        safe_name = get_safe_name(filename)
        if not safe_name:
            fail_count += 1
            fail_reasons.append(f"{filename}(不存在或非法)")
            continue
        if active_file == safe_name:
            fail_count += 1
            fail_reasons.append(f"{filename}(被占用)")
            continue
        deletable.append(safe_name)

    try:
        deleted, missing = db_store.delete_collections(deletable, DATA_DIR)
        success_count += len(deleted)
        fail_count += len(missing)
        fail_reasons.extend(f"{name}(不存在)" for name in missing)
    except OSError:
        fail_count += len(deletable)
        fail_reasons.append("系统占用")

    if fail_count == 0:
        return {"code": 200, "msg": "删除成功"}
    reason_str = ", ".join(fail_reasons[:3]) + ("..." if len(fail_reasons) > 3 else "")
    return {
        "code": 200 if success_count > 0 else 400,
        "msg": f"成功 {success_count} 个，失败 {fail_count} 个 [{reason_str}]",
    }


@router.get("/api/download")
def download_csv(name: str = None, tags: str = None, exclude_tags: str = None):
    if not name:
        return JSONResponse(status_code=400, content={"code": 400, "msg": "未指定文件名参数"})
    try:
        csv_bytes, safe_name = db_store.export_collection_to_csv_bytes(name, parse_tag_filter(tags), parse_tag_filter(exclude_tags))
    except UnsafeFilenameError:
        return JSONResponse(status_code=400, content={"code": 400, "msg": "文件名非法"})
    if csv_bytes is not None:
        quoted_name = quote(safe_name)
        return Response(
            content=csv_bytes,
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="download.csv"; filename*=UTF-8\'\'{quoted_name}'},
        )
    return JSONResponse(status_code=404, content={"code": 404, "msg": "找不到该文件"})


@router.get("/api/magnets")
def get_magnets(name: str = None, tags: str = None, exclude_tags: str = None):
    if not name:
        return JSONResponse(status_code=400, content={"code": 400, "msg": "未指定文件名参数"})
    try:
        safe_name = normalize_csv_filename(name)
    except UnsafeFilenameError:
        return JSONResponse(status_code=400, content={"code": 400, "msg": "文件名非法"})
    if not db_store.collection_exists(safe_name):
        return JSONResponse(status_code=404, content={"code": 404, "msg": "找不到该文件"})
    try:
        return {"code": 200, "data": db_store.get_magnet_links(safe_name, parse_tag_filter(tags), parse_tag_filter(exclude_tags))}
    except Exception as e:
        return JSONResponse(status_code=500, content={"code": 500, "msg": f"读取数据出错: {str(e)}"})
