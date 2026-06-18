# P0 — Bug / 正确性修复计划

> **目标：** 修复影响运行正确性的 Bug 和安全隐患，无架构变更。
> **预计工作量：** 4–6 小时
> **验证命令：** `python -m unittest discover -s spider_core/tests`

---

## 任务清单

### 0.1 修复 `storage.py` 删除失败时 HTTP 状态码与 body 不匹配

**问题：** `POST /api/delete` 在全部删除失败时，返回 HTTP 200 但 body 中 `"code": 400`。
前端或任何 HTTP 客户端只看 HTTP 状态码，会误判为成功。

**文件：** `spider_core/routers/storage.py`，第 49–55 行

**当前代码：**
```python
if fail_count == 0:
    return {"code": 200, "msg": "删除成功"}
reason_str = ", ".join(fail_reasons[:3]) + ("..." if len(fail_reasons) > 3 else "")
return {
    "code": 200 if success_count > 0 else 400,
    "msg": f"成功 {success_count} 个，失败 {fail_count} 个 [{reason_str}]",
}
```

**修复方案：** 全部失败时使用 `JSONResponse(status_code=400, ...)`，部分成功时保持 HTTP 200（因为确实有部分操作完成了）。

```python
if fail_count == 0:
    return {"code": 200, "msg": "删除成功"}
reason_str = ", ".join(fail_reasons[:3]) + ("..." if len(fail_reasons) > 3 else "")
msg = f"成功 {success_count} 个，失败 {fail_count} 个 [{reason_str}]"
if success_count > 0:
    return {"code": 200, "msg": msg}
return JSONResponse(status_code=400, content={"code": 400, "msg": msg})
```

**验证：**
- 手动测试：发送 `POST /api/delete` 请求，`filenames` 包含一个不存在的文件名，检查 HTTP 状态码是否为 400。
- 补充单元测试：在 `test_api_endpoints.py` 中新增 `test_delete_all_fail_returns_400` 用例。

---

### 0.2 修复 `magnets.py` 路由直接操作 service 内部状态

**问题：** `cancel_magnet_check_job` 路由在 `magnets.py:70-73` 直接操作 `job["cancel_event"].set()` 和修改 `job["cancelled"]`、`job["message"]`，使用的是 `job["lock"]` 而非全局 `MAGNET_CHECK_LOCK`。这违反了封装原则，且存在与 `create_magnet_check_job` 的竞态条件风险。

同样，`get_current_magnet_check_job_route`（第 48-50 行）直接访问 `MAGNET_CHECK_LOCK`、`ACTIVE_MAGNET_CHECK_JOB_ID`、`MAGNET_CHECK_JOBS` 等模块内部全局变量。

**文件：**
- `spider_core/routers/magnets.py`，第 46–74 行
- `spider_core/services/magnet_service.py`

**修复方案：**

**步骤 1：在 `magnet_service.py` 中新增两个封装函数：**

```python
def get_current_job():
    """返回当前活跃检测任务的公开快照，无活跃任务返回 None。"""
    with MAGNET_CHECK_LOCK:
        active_id = ACTIVE_MAGNET_CHECK_JOB_ID
        job = MAGNET_CHECK_JOBS.get(active_id) if active_id else None
    if not job:
        return None
    data = public_magnet_check_job(job)
    return data if data["running"] else None


def cancel_job(job_id):
    """取消指定检测任务。返回 (success, data_or_none)。
    success=False 表示 job_id 不存在。
    """
    job = MAGNET_CHECK_JOBS.get(job_id)
    if not job:
        return False, None
    job["cancel_event"].set()
    with job["lock"]:
        job["cancelled"] = True
        job["message"] = "正在取消检测"
    return True, public_magnet_check_job(job)
```

**步骤 2：简化 `magnets.py` 路由：**

```python
@router.get("/api/magnet_check_jobs/current")
def get_current_magnet_check_job_route():
    data = magnet_service.get_current_job()
    return {"code": 200, "data": data}


@router.post("/api/magnet_check_jobs/{job_id}/cancel")
def cancel_magnet_check_job(job_id: str):
    success, data = magnet_service.cancel_job(job_id)
    if not success:
        return JSONResponse(status_code=404, content={"code": 404, "msg": "找不到检测任务"})
    return {"code": 200, "data": data}
```

**验证：**
- 运行现有测试 `test_v14_db_store.py` 中的磁力检测相关用例。
- 手动测试：启动一个检测任务，立即取消，检查返回是否正确。

---

### 0.3 修复 `MAGNET_CHECK_JOBS` 无限增长的内存泄漏

**问题：** `magnet_service.py` 中 `MAGNET_CHECK_JOBS` 是一个 `dict`，每次创建检测任务就追加一个条目，已完成的任务永远不会被移除。在长时间运行的实例（特别是 Docker 部署）中，这是一个内存泄漏。

**文件：** `spider_core/services/magnet_service.py`

**修复方案：** 在 `run_magnet_check_job` 完成时保留最近 N 个任务记录，清理更早的已完成任务。

在 `magnet_service.py` 顶部新增常量：

```python
MAX_FINISHED_JOBS = 20  # 保留最近的已完成任务记录数
```

在 `run_magnet_check_job` 函数末尾（第 148-150 行之后），添加清理逻辑：

```python
    with MAGNET_CHECK_LOCK:
        if ACTIVE_MAGNET_CHECK_JOB_ID == job_id:
            ACTIVE_MAGNET_CHECK_JOB_ID = None
        # 清理过旧的已完成任务，保留最近 MAX_FINISHED_JOBS 个
        finished_ids = [
            jid for jid, j in MAGNET_CHECK_JOBS.items()
            if not j["running"] and jid != job_id
        ]
        excess = len(finished_ids) - MAX_FINISHED_JOBS
        if excess > 0:
            for jid in finished_ids[:excess]:
                del MAGNET_CHECK_JOBS[jid]
```

**注意事项：**
- 不能删除当前正在运行的任务（通过 `not j["running"]` 过滤）。
- 不能删除刚完成的当前任务（通过 `jid != job_id` 排除）。
- `finished_ids` 的遍历顺序依赖 Python 3.7+ dict 保持插入顺序的保证（项目目标 3.12，符合）。

**验证：**
- 在 `test_v14_db_store.py` 中新增测试：连续创建 25 个检测任务并等待完成，验证 `len(MAGNET_CHECK_JOBS)` 不超过 `MAX_FINISHED_JOBS + 1`。

---

### 0.4 锁定 `requirements.txt` 依赖版本

**问题：** 当前 `requirements.txt` 中 6 个依赖全部无版本约束，导致构建不可复现。`curl_cffi` 在不同大版本间有破坏性 API 变更。

**文件：** `requirements.txt`

**修复方案：** 在当前开发环境中执行 `pip freeze` 获取精确版本，然后锁定到兼容范围。

**步骤 1：** 获取当前安装版本：
```bash
pip freeze | grep -iE "fastapi|uvicorn|pydantic|curl.cffi|beautifulsoup4|bencodepy"
```

**步骤 2：** 根据输出锁定版本（示例，以实际输出为准）：
```
fastapi>=0.95.2,<1.0.0
uvicorn>=0.22.0,<1.0.0
pydantic>=1.10.0,<2.0.0
curl_cffi>=0.5.9,<1.0.0
beautifulsoup4>=4.12.0,<5.0.0
bencodepy>=0.9.5,<1.0.0
```

**注意：** `pydantic<2.0.0` 是必要的，因为当前代码使用 Pydantic v1 API。如果升级到 Pydantic v2 需要配合 P1 架构重构一起做。

**步骤 3：** 同步检查 `app/build.gradle` 中 Chaquopy 的 pip 版本是否一致（第 93-97 行），确保 PC/Docker 和 Android 使用相同版本范围。

**验证：**
- `pip install -r requirements.txt` 在干净虚拟环境中成功。
- `python -m unittest discover -s spider_core/tests` 全部通过。

---

### 0.5 修复 `main.py` HTML 错误响应泄露文件系统路径

**问题：** `main.py:483` 在找不到前端页面时，将绝对文件系统路径直接嵌入 HTML 响应返回给客户端，存在信息泄露风险。

**文件：** `spider_core/main.py`，第 476–483 行

**当前代码：**
```python
@app.get("/")
def read_root():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    html_path = os.path.join(base_dir, "frontend", "index.html")
    if os.path.exists(html_path):
        with open(html_path, "r", encoding="utf-8") as f:
            return HTMLResponse(f.read())
    return HTMLResponse(f"<h1>找不到前端页面，系统当前寻找的绝对路径是: {html_path}</h1>")
```

**修复方案：** 移除路径信息，仅记录日志：

```python
@app.get("/")
def read_root():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    html_path = os.path.join(base_dir, "frontend", "index.html")
    if os.path.exists(html_path):
        with open(html_path, "r", encoding="utf-8") as f:
            return HTMLResponse(f.read())
    logging.error("前端页面未找到: %s", html_path)
    return HTMLResponse("<h1>前端页面未找到，请检查部署</h1>", status_code=404)
```

同理检查第 492 行 `get_favicon` 的错误响应：
```python
return {"error": f"找不到图标文件: {file_path}"}
```
改为：
```python
return JSONResponse(status_code=404, content={"error": "图标文件未找到"})
```

**验证：** 静态审查 + 手动访问不存在的前端文件路径，确认响应中不包含服务器路径。

---

### 0.6 修复 `task_repo.py` 中的死代码和冗余操作

**问题 A：** `delete_task`（`task_repo.py:141`）手动删除 `task_logs` 表记录，但 schema 中已定义 `FOREIGN KEY ... ON DELETE CASCADE`，这个 DELETE 是冗余的。

**问题 B：** `delete_task`（`task_repo.py:146`）检查并删除 `task_checkpoints` 表，但该表在 schema 中不存在（`db_store.py` 的 `init_database` 中没有创建此表）。

**文件：** `spider_core/task_repo.py`

**修复方案：**

查找 `delete_task` 函数，移除冗余的 `task_logs` DELETE 语句和 `task_checkpoints` 相关代码。保留核心的 `DELETE FROM tasks WHERE task_id = ?`。

修改前需确认：
1. 确认 `tasks` 表的外键约束确实启用了 `ON DELETE CASCADE`（检查 `db_store.py` 的 `init_database` 中 `task_logs` 表定义）。
2. 确认 `task_checkpoints` 表确实不存在于 schema 中。

**验证：**
- `python -m unittest spider_core/tests/test_v15_tasks.py` — 任务删除相关测试通过。
- 检查删除任务后 `task_logs` 表中对应记录是否被级联删除。

---

## 执行顺序

建议按以下顺序执行，每完成一项跑一次测试：

1. **0.4** — 锁定依赖版本（最高优先，确保后续修改在稳定环境中）
2. **0.1** — 修复 HTTP 状态码 Bug（最简单的代码修改）
3. **0.5** — 修复路径信息泄露（简单修改）
4. **0.6** — 清理死代码（简单修改）
5. **0.2** — 封装 magnet_service 操作（需要改两个文件）
6. **0.3** — 修复内存泄漏（需要新增清理逻辑和测试）

## 回归测试

全部修改完成后，执行完整测试套件：
```bash
python -m unittest discover -s spider_core/tests
```

确认所有测试通过后，提交一个 commit：
```
Fix: P0 正确性修复 — HTTP 状态码、内存泄漏、路径泄露、依赖锁定
```
