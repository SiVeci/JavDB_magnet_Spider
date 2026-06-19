import json
import logging
import os
import re
import tempfile
import threading


JSON_FILE_LOCK = threading.RLock()

_CONTROL_CHARS_RE = re.compile(r"[\x00-\x1f\x7f]")
_WINDOWS_RESERVED_NAMES = {
    "CON", "PRN", "AUX", "NUL",
    "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
    "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
}


class UnsafeFilenameError(ValueError):
    pass


def normalize_csv_filename(filename, allow_empty=False):
    value = (filename or "").strip()
    if not value:
        if allow_empty:
            return ""
        raise UnsafeFilenameError("文件名不能为空")

    if _CONTROL_CHARS_RE.search(value):
        raise UnsafeFilenameError("文件名包含非法控制字符")

    if value != os.path.basename(value):
        raise UnsafeFilenameError("文件名不能包含路径")

    if "/" in value or "\\" in value:
        raise UnsafeFilenameError("文件名不能包含路径分隔符")

    if value in {".", ".."} or value.startswith("."):
        raise UnsafeFilenameError("文件名不能是隐藏文件或相对路径")

    stem, ext = os.path.splitext(value)
    if ext and ext.lower() != ".csv":
        raise UnsafeFilenameError("文件名后缀必须是 .csv")
    if not ext:
        value += ".csv"
        stem = value[:-4]

    stem = stem.rstrip(" .")
    if not stem:
        raise UnsafeFilenameError("文件名不能为空")

    if stem.upper() in _WINDOWS_RESERVED_NAMES:
        raise UnsafeFilenameError("文件名不能使用系统保留名称")

    return f"{stem}.csv"


def make_csv_filename_from_label(label, fallback_prefix="javdb"):
    value = (label or "").strip()
    value = _CONTROL_CHARS_RE.sub("_", value)
    value = value.replace("/", "_").replace("\\", "_")
    value = value.strip(" .")
    if not value:
        value = fallback_prefix
    if os.path.splitext(value)[0].upper() in _WINDOWS_RESERVED_NAMES:
        value = f"{fallback_prefix}_{value}"
    if not value.lower().endswith(".csv"):
        value = f"{value}.csv"
    return normalize_csv_filename(value)


def get_safe_csv_path(data_dir, filename):
    safe_name = normalize_csv_filename(filename)
    base_dir = os.path.abspath(data_dir)
    target_path = os.path.abspath(os.path.join(base_dir, safe_name))
    if os.path.commonpath([base_dir, target_path]) != base_dir:
        raise UnsafeFilenameError("文件路径越界")
    return target_path, safe_name


def read_json_file(path, default=None):
    with JSON_FILE_LOCK:
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            return default
        except (json.JSONDecodeError, ValueError):
            logging.warning("JSON parse failed: %s", path)
            return default


def atomic_write_json(path, data, indent=None):
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    with JSON_FILE_LOCK:
        fd, tmp_path = tempfile.mkstemp(prefix=".tmp-", suffix=".json", dir=directory)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=indent)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, path)
        except Exception:
            try:
                os.remove(tmp_path)
            except OSError:
                pass
            raise
