"""export_service — 集合数据导出（CSV 字节流）。

行选取与标签过滤复用 db_store 的私有 helper _export_rows；字段顺序复用 CSV_FIELDNAMES。
"""

import csv
import io

from db_store import connect, _export_rows, CSV_FIELDNAMES
from storage_utils import normalize_csv_filename

__all__ = ["export_collection_to_csv_bytes"]

_BOM = "﻿"


def export_collection_to_csv_bytes(filename, required_tags=None, exclude_tags=None):
    safe_name = normalize_csv_filename(filename)
    with connect() as conn:
        if not conn.execute("SELECT 1 FROM collections WHERE filename = ?", (safe_name,)).fetchone():
            return None, safe_name
        rows = _export_rows(conn, safe_name, required_tags, exclude_tags)

    buffer = io.StringIO(newline="")
    buffer.write(_BOM)
    writer = csv.DictWriter(buffer, fieldnames=CSV_FIELDNAMES)
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue().encode("utf-8"), safe_name
