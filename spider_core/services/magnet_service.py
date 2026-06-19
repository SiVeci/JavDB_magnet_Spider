"""magnet_service — 磁力验活任务的并发编排（自包含全局态）。

MAGNET_CHECK_JOBS / ACTIVE_MAGNET_CHECK_JOB_ID 等全局态与操作它们的函数一起置于本模块，
模块内 global 语句正常工作；外部（路由）需读取 ACTIVE_MAGNET_CHECK_JOB_ID 时请用
`magnet_service.ACTIVE_MAGNET_CHECK_JOB_ID` 属性访问以获取实时值（勿 from-import 取快照）。
"""

import queue
import threading
import uuid

import db_store
import magnet_checker

MAGNET_CHECK_LOCK = threading.RLock()
MAGNET_CHECK_JOBS = {}
ACTIVE_MAGNET_CHECK_JOB_ID = None
MAX_FINISHED_JOBS = 20


class MagnetCheckError(Exception):
    """Magnet check job cannot be started."""

    def __init__(self, status_code: int, msg: str, data=None):
        super().__init__(msg)
        self.status_code = status_code
        self.msg = msg
        self.data = data


def public_magnet_check_job(job):
    with job["lock"]:
        return {
            "job_id": job["job_id"],
            "scope": job["scope"],
            "target": job["target"],
            "total": job["total"],
            "completed": job["completed"],
            "active": job["active"],
            "weak": job["weak"],
            "dead": job["dead"],
            "failed": job["failed"],
            "running": job["running"],
            "cancelled": job["cancelled"],
            "done": job["done"],
            "message": job.get("message", ""),
        }


def create_magnet_check_job(scope, target, magnets):
    global ACTIVE_MAGNET_CHECK_JOB_ID
    with MAGNET_CHECK_LOCK:
        if ACTIVE_MAGNET_CHECK_JOB_ID:
            active = MAGNET_CHECK_JOBS.get(ACTIVE_MAGNET_CHECK_JOB_ID)
            if active and public_magnet_check_job(active)["running"]:
                return None, active
        job_id = str(uuid.uuid4())
        job = {
            "job_id": job_id,
            "scope": scope,
            "target": target,
            "total": len(magnets),
            "completed": 0,
            "active": 0,
            "weak": 0,
            "dead": 0,
            "failed": 0,
            "running": True,
            "cancelled": False,
            "done": False,
            "message": "",
            "cancel_event": threading.Event(),
            "lock": threading.RLock(),
        }
        MAGNET_CHECK_JOBS[job_id] = job
        ACTIVE_MAGNET_CHECK_JOB_ID = job_id
    thread = threading.Thread(
        target=run_magnet_check_job,
        args=(job_id, list(magnets), db_store.get_runtime_config(include_cookie=False).get("trackers", [])),
        daemon=True,
    )
    thread.start()
    return job, None


def get_current_job():
    with MAGNET_CHECK_LOCK:
        active_id = ACTIVE_MAGNET_CHECK_JOB_ID
        job = MAGNET_CHECK_JOBS.get(active_id) if active_id else None
    if not job:
        return None
    data = public_magnet_check_job(job)
    return data if data["running"] else None


def cancel_job(job_id):
    job = MAGNET_CHECK_JOBS.get(job_id)
    if not job:
        return False, None
    job["cancel_event"].set()
    with job["lock"]:
        job["cancelled"] = True
        job["message"] = "正在取消检测"
    return True, public_magnet_check_job(job)


def failed_magnet_rows(magnets):
    return [magnet for magnet in magnets if magnet.get("check_error") and not magnet.get("check_status")]


def start_magnet_check(scope, target, magnets, empty_msg, failed_only=False):
    """统一磁力检测启动逻辑：空集校验 + failed_only 过滤 + 创建 job + 并发冲突处理。"""
    if not magnets:
        raise MagnetCheckError(404, empty_msg)
    if failed_only:
        magnets = failed_magnet_rows(magnets)
        if not magnets:
            raise MagnetCheckError(404, "没有检测失败的磁力")
    job, active = create_magnet_check_job(scope, target, magnets)
    if active:
        raise MagnetCheckError(409, "磁力检测任务正在运行", public_magnet_check_job(active))
    return {"code": 200, "data": public_magnet_check_job(job)}


def run_magnet_check_job(job_id, magnets, user_trackers):
    global ACTIVE_MAGNET_CHECK_JOB_ID
    job = MAGNET_CHECK_JOBS[job_id]
    work_queue = queue.Queue()
    for magnet in magnets:
        work_queue.put(magnet)

    def worker():
        while not job["cancel_event"].is_set():
            try:
                magnet = work_queue.get_nowait()
            except queue.Empty:
                return
            try:
                result = magnet_checker.check_magnet(magnet.get("link", ""), user_trackers)
                db_store.update_magnet_check_result(
                    magnet["id"],
                    result.get("check_status"),
                    result.get("seeders", 0),
                    result.get("leechers", 0),
                    result.get("check_error"),
                )
                key = result.get("check_status") or "failed"
                if key not in {"active", "weak", "dead"}:
                    key = "failed"
            except Exception as exc:
                db_store.update_magnet_check_result(magnet["id"], None, 0, 0, str(exc))
                key = "failed"
            finally:
                with job["lock"]:
                    job["completed"] += 1
                    job[key] += 1
                work_queue.task_done()

    workers = [
        threading.Thread(target=worker, daemon=True)
        for _ in range(min(magnet_checker.CONCURRENCY_LIMIT, max(1, len(magnets))))
    ]
    for worker_thread in workers:
        worker_thread.start()
    for worker_thread in workers:
        worker_thread.join()
    with job["lock"]:
        job["cancelled"] = job["cancel_event"].is_set()
        job["running"] = False
        job["done"] = not job["cancelled"]
        if job["cancelled"]:
            job["message"] = "检测已取消"
        else:
            job["message"] = "检测完成"
    with MAGNET_CHECK_LOCK:
        if ACTIVE_MAGNET_CHECK_JOB_ID == job_id:
            ACTIVE_MAGNET_CHECK_JOB_ID = None
        finished_ids = [
            jid for jid, item in MAGNET_CHECK_JOBS.items()
            if not item["running"] and jid != job_id
        ]
        excess = len(finished_ids) - MAX_FINISHED_JOBS
        if excess > 0:
            for jid in finished_ids[:excess]:
                del MAGNET_CHECK_JOBS[jid]
