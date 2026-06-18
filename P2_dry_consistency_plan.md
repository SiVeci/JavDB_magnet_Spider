# P2 — DRY / 一致性优化计划

> **目标：** 消除后端和前端的重复代码模式，统一 API 响应格式，提取可复用组件。
> **前置条件：** P1 全部完成并通过测试。
> **预计工作量：** 10–14 小时（建议分 5 个独立 commit 提交）
> **验证命令：**
> - 后端：`python -m unittest discover -s spider_core/tests`
> - 前端：浏览器手动验证所有页面功能

---

## 任务清单

### 2.1 提取集合名校验为 FastAPI 依赖注入

**问题：** 以下 5 行代码模式在 5 个路由文件中重复出现：

```python
try:
    safe_name = normalize_csv_filename(name)
except UnsafeFilenameError:
    return JSONResponse(status_code=400, content={"code": 400, "msg": "文件名非法"})
if not db_store.collection_exists(safe_name):
    return JSONResponse(status_code=404, content={"code": 404, "msg": "找不到该集合"})
```

**出现位置：**
- `routers/movies.py:26-31`（`get_collection_movies`）
- `routers/movies.py:37-42`（`create_collection_incremental_task`）
- `routers/magnets.py:25-30`（`check_collection_magnets`）
- `routers/storage.py:80-85`（`get_magnets`）
- `routers/tasks.py:84-89`（`get_task_incremental_magnets` — 仅校验部分）

**修复方案：** 创建 FastAPI 依赖函数。

**文件：** `spider_core/dependencies.py`（新建）

```python
"""dependencies.py — FastAPI 可复用依赖注入函数。"""

from fastapi import HTTPException

import db_store
from storage_utils import UnsafeFilenameError, normalize_csv_filename
from ranking_utils import is_valid_ranking


def valid_collection(name: str) -> str:
    """校验集合名称合法且存在，返回安全文件名。用作路径参数依赖。"""
    try:
        safe_name = normalize_csv_filename(name)
    except UnsafeFilenameError:
        raise HTTPException(status_code=400, detail="文件名非法")
    if not db_store.collection_exists(safe_name):
        raise HTTPException(status_code=404, detail="找不到该集合")
    return safe_name


def valid_ranking(category: str, period: str) -> tuple[str, str]:
    """校验排行榜分类和周期有效，返回 (category, period) 元组。"""
    if not is_valid_ranking(category, period):
        raise HTTPException(status_code=404, detail="排行榜不存在")
    return category, period
```

**注意：** `HTTPException` 的 `detail` 字段替代了手动构造的 `JSONResponse`。FastAPI 会自动将其转为 `{"detail": "..."}` 格式。如需保持 `{"code": N, "msg": "..."}` 的现有格式，需要添加自定义异常处理器：

```python
# 在 main.py 中注册
@app.exception_handler(HTTPException)
async def custom_http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={"code": exc.status_code, "msg": exc.detail},
    )
```

**路由改造示例（`routers/movies.py`）：**

```python
from fastapi import APIRouter, Depends
from dependencies import valid_collection

@router.get("/api/collections/{name}/movies")
def get_collection_movies(safe_name: str = Depends(valid_collection)):
    return {"code": 200, "data": db_store.get_collection_movies(safe_name)}
```

**需要修改的路由函数：**

| 文件 | 函数 | 当前行数 | 改造后减少行数 |
|------|------|---------|-------------|
| `movies.py` | `get_collection_movies` | 26-32 | -5 |
| `movies.py` | `create_collection_incremental_task` | 37-42 | -5 |
| `magnets.py` | `check_collection_magnets` | 25-30 | -5 |
| `storage.py` | `get_magnets` | 80-85 | -5 |
| `rankings.py` | 6 个函数使用 `is_valid_ranking` | 各处 | 各-2 |

**验证：**
- 全量后端测试通过。
- 手动测试：请求不存在的集合名 → 404；请求非法文件名（含 `../`）→ 400。

---

### 2.2 提取 CSV 下载响应构建器

**问题：** CSV 下载的 Response 构造在两处完全相同。

**出现位置：**
- `routers/storage.py:67-72`（`download_csv`）
- `routers/rankings.py:190-195`（`download_ranking_csv`）

**重复代码：**
```python
quoted_name = quote(safe_name)
return Response(
    content=csv_bytes,
    media_type="text/csv; charset=utf-8",
    headers={"Content-Disposition": f'attachment; filename="download.csv"; filename*=UTF-8\'\'{quoted_name}'},
)
```

**修复方案：** 在 P1 创建的 `utils.py` 中添加函数：

```python
from urllib.parse import quote
from fastapi.responses import Response

def csv_download_response(csv_bytes: bytes, filename: str) -> Response:
    """构造 CSV 文件下载响应。"""
    quoted_name = quote(filename)
    return Response(
        content=csv_bytes,
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="download.csv"; filename*=UTF-8\'\'{quoted_name}'
        },
    )
```

**修改步骤：**
1. 在 `utils.py` 中添加 `csv_download_response` 函数。
2. 修改 `storage.py:66-72`：
   ```python
   if csv_bytes is not None:
       return csv_download_response(csv_bytes, safe_name)
   ```
3. 修改 `rankings.py:188-195`：
   ```python
   if csv_bytes is None:
       return JSONResponse(status_code=404, ...)
   return csv_download_response(csv_bytes, safe_name)
   ```

**验证：** 手动测试两个下载端点，确认文件名、编码、Content-Disposition 正常。

---

### 2.3 提取活跃任务冲突检查

**问题：** 删除集合前检查是否有活跃任务占用该文件名的逻辑在两处重复。

**出现位置：**
- `routers/storage.py:22-26`（`delete_history`）
- `routers/rankings.py:206-211`（`clear_ranking_collection`）

**重复代码模式：**
```python
active_file = None
active_task = db_store.get_active_task()
if active_task:
    active_file = get_safe_name(active_task.get("collection_filename") or active_task.get("final_filename"))
if active_file == filename:
    return JSONResponse(status_code=400, ...)
```

**修复方案：** 在 `dependencies.py` 中添加：

```python
def get_active_task_filename() -> str | None:
    """返回当前活跃任务占用的集合文件名，无活跃任务返回 None。"""
    active_task = db_store.get_active_task()
    if not active_task:
        return None
    from utils import get_safe_name
    return get_safe_name(
        active_task.get("collection_filename") or active_task.get("final_filename")
    )


def check_not_occupied_by_task(filename: str) -> None:
    """检查指定文件名未被活跃任务占用，否则抛出 HTTPException 400。"""
    active_file = get_active_task_filename()
    if active_file and active_file == filename:
        raise HTTPException(status_code=400, detail="该集合正在被任务占用")
```

**修改步骤：**
1. 在 `dependencies.py` 中添加上述函数。
2. 简化 `storage.py:delete_history`：在循环中调用 `check_not_occupied_by_task(safe_name)`。
3. 简化 `rankings.py:clear_ranking_collection`：调用 `check_not_occupied_by_task(filename)`。

**验证：** 手动测试：在任务运行时尝试删除对应集合 → 400。

---

### 2.4 统一 API 响应格式

**问题：** 当前 API 响应格式不统一，有 5 种不同的模式。

| 端点 | 当前格式 | 问题 |
|------|---------|------|
| 大多数端点 | `{"code": 200, "data": ...}` | 正确 |
| `GET /api/version` | `{"version": ..., "auth_required": ...}` | 缺少 `code` 字段 |
| `GET /api/status` | `{"state": ..., "progress": ...}` | 缺少 `code` 字段 |
| `POST /api/tasks/cleanup` | `{"code": 200, "msg": ..., "deleted": N}` | `deleted` 应在 `data` 内 |
| `POST /api/tasks` 成功 | `{"code": 200, "msg": ..., "task_id": ..., "filename": ...}` | 额外字段应在 `data` 内 |

**修复方案：** 逐个修正为统一格式 `{"code": N, "data": {...}}` 或 `{"code": N, "msg": "..."}`。

**步骤 1 — `GET /api/version`（`settings.py:27-28`）：**

```python
# 之前
return {"version": APP_VERSION, "auth_required": is_auth_required()}

# 之后
return {"code": 200, "data": {"version": APP_VERSION, "auth_required": is_auth_required()}}
```

**前端配套修改（`app.js` 中 `fetchVersion` 函数）：**
当前前端直接读取 `data.version` 和 `data.auth_required`，需要改为 `data.data.version` 和 `data.data.auth_required`，或保持向后兼容两种格式。

**建议：** 前端做兼容处理：
```javascript
const payload = res.data || res;  // 兼容新旧格式
```

**步骤 2 — `GET /api/status`（`settings.py:50-54`）：**

此端点被前端和 Android `status.json` 轮询共同使用，且 Android `SpiderService.updateNotificationFromJson` 直接读取 `state` 字段。
**建议不改此端点**，保持向后兼容（Android 端改动成本高）。在文档中标注为遗留格式。

**步骤 3 — `POST /api/tasks/cleanup`（`tasks.py:38-40`）：**

```python
# 之前
return {"code": 200, "msg": f"已清理 {deleted} 个已结束任务", "deleted": deleted}

# 之后
return {"code": 200, "msg": f"已清理 {deleted} 个已结束任务", "data": {"deleted": deleted}}
```

**前端配套修改（`tasks.js`）：** 检查是否有读取 `res.deleted` 的代码，改为 `res.data.deleted`。

**步骤 4 — `POST /api/tasks` 成功（`services/task_service.py` 中的 `create_task_from_config`）：**

```python
# 之前
return {"code": 200, "msg": "任务已加入队列", "task_id": task_id, "filename": prepared["filename"]}

# 之后
return {"code": 200, "msg": "任务已加入队列", "data": {"task_id": task_id, "filename": prepared["filename"]}}
```

**前端配套修改（`tasks.js`）：** 检查是否有读取 `res.task_id` 或 `res.filename` 的代码。

**验证：**
- 全量后端测试通过（需同步更新测试中的断言）。
- 前端全流程手动测试。

---

### 2.5 合并 `db_store.py` 中结构相同的 JSON 序列化函数

**问题：** `_tags_to_json`/`_tags_from_json` 与 `_trackers_to_json`/`_trackers_from_json` 实现完全相同。

**当前代码（`db_store.py`）：**

```python
# 第 364-376 行
def _tags_to_json(tags):
    return json.dumps(_normalize_string_list(tags), ensure_ascii=False)

def _tags_from_json(value):
    if not value:
        return []
    try:
        data = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return []
    return _normalize_string_list(data if isinstance(data, list) else [])

# 第 459-470 行
def _trackers_to_json(trackers):
    return json.dumps(_normalize_string_list(trackers), ensure_ascii=False)

def _trackers_from_json(value):
    if not value:
        return []
    try:
        data = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return []
    return _normalize_string_list(data if isinstance(data, list) else [])
```

**修复方案：** 提取为通用函数，保留别名以兼容调用方：

```python
def _string_list_to_json(values):
    """将字符串列表序列化为 JSON（去重去空白）。"""
    return json.dumps(_normalize_string_list(values), ensure_ascii=False)


def _string_list_from_json(value):
    """从 JSON 字符串反序列化为字符串列表。"""
    if not value:
        return []
    try:
        data = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return []
    return _normalize_string_list(data if isinstance(data, list) else [])


# 语义别名 — 保持现有调用方无需修改
_tags_to_json = _string_list_to_json
_tags_from_json = _string_list_from_json
_trackers_to_json = _string_list_to_json
_trackers_from_json = _string_list_from_json
```

**验证：** 全量测试通过。

---

### 2.6 提取集合级查询 helper（消除 5 处 JOIN 重复）

**问题：** `movies JOIN collections WHERE filename = ?` 模式在 `movie_repo.py` 中出现 5 次。

**出现位置：**
- `get_collection_movies`
- `get_collection_movie_ids`
- `auto_select_collection_magnets`
- `get_magnet_links`
- `get_magnet_links_for_codes`

**修复方案：** 在 `movie_repo.py` 中添加内部 helper：

```python
def _collection_movie_query(conn, filename, select_columns, extra_joins="", extra_where="", order_by="m.id", params=None):
    """对指定集合执行 movies JOIN collections 查询的通用 helper。"""
    safe_name = normalize_csv_filename(filename)
    sql = f"""
        SELECT {select_columns}
        FROM movies m
        JOIN collections c ON c.id = m.collection_id
        {extra_joins}
        WHERE c.filename = ?
        {extra_where}
        ORDER BY {order_by}
    """
    all_params = [safe_name] + (list(params) if params else [])
    return conn.execute(sql, all_params).fetchall()
```

**使用示例：**

```python
# 之前（get_collection_movie_ids）
rows = conn.execute("""
    SELECT m.id FROM movies m
    JOIN collections c ON c.id = m.collection_id
    WHERE c.filename = ?
    ORDER BY m.id
""", (safe_name,)).fetchall()

# 之后
rows = _collection_movie_query(conn, filename, "m.id")
```

**验证：** 全量测试通过。

---

### 2.7 前端：合并重复的筛选器渲染函数

**问题：** 集合和排行榜的筛选器渲染函数几乎完全相同，仅 onclick handler 名称不同。

**重复对（movies.js 拆分后分布在 movies.js 和 ranking.js 中）：**

| 集合版本 | 排行榜版本 | 差异 |
|---------|----------|------|
| `renderCollectionFilter(movies, key)` | `renderRankingFilter(movies, key)` | onclick handler 名称 |
| `renderTagOption(tag, key)` | `renderRankingTagOption(tag, key)` | onchange handler 名称 |
| `renderExcludeOption(tag, key)` | `renderRankingExcludeOption(tag, key)` | onchange handler 名称 |
| `renderMovies(filteredMovies, key)` | `renderRankingMovies(filteredMovies, key)` | onclick handler 名称 |
| `toggleCollectionTag(tag, key)` | `toggleRankingTag(tag, key)` | filter store 不同 |
| `toggleExcludeTag(tag, key)` | `toggleRankingExcludeTag(tag, key)` | filter store 不同 |
| `clearExcludeTags(key)` | `clearRankingExcludeTags(key)` | filter store 不同 |
| `downloadCsv(name)` | `downloadRankingCsv(cat, period)` | URL 不同 |
| `copyMagnets(name)` | `copyRankingMagnets(cat, period)` | URL 不同 |

**修复方案：参数化**

**步骤 1 — 筛选器渲染（预计消除 ~80 行）：**

```javascript
/**
 * 通用筛选器下拉框渲染。
 * @param {Object} config
 * @param {Array} config.movies - 电影列表
 * @param {string} config.filterKey - 筛选器键
 * @param {Function} config.getSelectedTags - 获取当前选中标签
 * @param {Function} config.getExcludeTags - 获取当前排除标签
 * @param {string} config.toggleTagFn - 切换标签的全局函数名
 * @param {string} config.toggleExcludeFn - 切换排除标签的全局函数名
 * @param {string} config.clearExcludeFn - 清空排除标签的全局函数名
 */
function renderFilterDropdowns(config) {
    // 提取 movies 中的所有唯一标签
    // 渲染标签筛选下拉框 + 排除筛选下拉框
    // 使用 config 中的函数名作为 onclick/onchange handler
}
```

**步骤 2 — 标签操作函数统一（预计消除 ~40 行）：**

```javascript
function _toggleTag(filterStore, tag, key, rerenderFn) {
    const tags = filterStore[key] || new Set();
    if (tags.has(tag)) tags.delete(tag); else tags.add(tag);
    filterStore[key] = tags;
    rerenderFn(key);
}

// 集合版本
function toggleCollectionTag(tag, key) {
    _toggleTag(collectionTagFilters, tag, key, renderCollectionBody);
}
// 排行榜版本
function toggleRankingTag(tag, key) {
    _toggleTag(rankingTagFilters, tag, key, renderRankingMovieListPage);
}
```

**步骤 3 — 下载/复制统一（预计消除 ~50 行）：**

利用 P1 中新增的 `apiDownloadBlob`：

```javascript
// 统一下载函数
function downloadCollectionCsv(name) {
    const query = buildTagFilterQuery(collectionTagFilters, collectionExcludeFilters, selectedTagsForFilterKey(name));
    apiDownloadBlob(`/api/download?name=${encodeURIComponent(name)}${query}`, name)
        .catch(err => showToast(err.message, 'error'));
}
function downloadRankingCsv(cat, period) {
    const key = `${cat}:${period}`;
    const query = buildTagFilterQuery(rankingTagFilters, rankingExcludeFilters, key);
    apiDownloadBlob(`/api/rankings/${cat}/${period}/download${query}`, `${cat}_${period}.csv`)
        .catch(err => showToast(err.message, 'error'));
}

// 统一复制函数
async function copyMagnetsFromUrl(url) {
    try {
        const res = await apiFetchJson(url);
        const links = (res.data || []).map(m => m.magnet_link).filter(Boolean);
        if (!links.length) return showToast('没有可复制的磁力链接', 'warning');
        copyText(links.join('\n'));
        showToast(`已复制 ${links.length} 条磁力链接`);
    } catch (err) {
        showToast(err.message, 'error');
    }
}
```

**验证：** 浏览器中测试集合和排行榜的筛选、下载、复制功能。

---

### 2.8 前端：统一磁力检测按钮渲染

**问题：** `movies.js`（拆分后在 `ranking.js`）中的 `renderRankingMagnetCheckButton` 是 `magnets.js` 中通用实现的完整复制品。

**修复方案：** 删除 `ranking.js` 中的 `renderRankingMagnetCheckButton`，改为复用 `magnets.js` 的 `renderMagnetCheckButton`。

需要确认 `renderMagnetCheckButton` 的参数是否已经支持 ranking scope。根据分析，`MAGNET_CHECK_SCOPE` 已包含 `ranking` 定义，所以应该可以直接使用。

**步骤：**
1. 在 `ranking.js` 的排行榜电影列表页中，将 `renderRankingMagnetCheckButton(cat, period)` 调用替换为 `renderMagnetCheckButton('ranking', \`${cat}:${period}\`)` 或等效调用。
2. 删除 `renderRankingMagnetCheckButton` 函数定义。
3. 确保 `magnets.js` 的 `MAGNET_CHECK_SCOPE` 正确配置了 ranking 的启动和取消端点。

**验证：** 在排行榜页面测试磁力检测按钮的启动、轮询、取消流程。

---

### 2.9 前端：消除任务操作函数重复

**问题：** `tasks.js` 中 `pauseTask`、`cancelTask`、`deleteTaskById`、`resumeTaskById`、`refreshCookie`、`setTaskModeById` 等函数遵循相同模式：

```javascript
async function xxxTask(taskId) {
    const res = await apiFetch('/api/tasks/' + taskId + '/xxx', { method: 'POST' }).then(r => r.json());
    if (res.code !== 200) return showToast(res.msg, 'error');
    showToast(res.msg || '操作成功');
    refreshMonitor();
}
```

**修复方案：** 提取通用函数：

```javascript
async function taskAction(taskId, action, method = 'POST', body = null) {
    try {
        const res = await apiPost(`/api/tasks/${taskId}/${action}`, body);
        showToast(res.msg || '操作成功');
        refreshMonitor();
    } catch (err) {
        showToast(err.message || '操作失败', 'error');
    }
}

function pauseTask(taskId) { taskAction(taskId, 'pause'); }
function cancelTask(taskId) { taskAction(taskId, 'cancel'); }
function resumeTaskById(taskId) { taskAction(taskId, 'resume'); }
```

**需要特殊处理的函数：**
- `deleteTaskById` — 使用 `DELETE` 方法，且 URL 不同（`/api/tasks/${taskId}` 无 action 后缀）。
- `setTaskModeById` — 需要 body（`{mode: ...}`）。
- `refreshCookie` — URL 是 `refresh_cookie`。

这些可以通过参数灵活处理。

**验证：** 手动测试所有任务操作按钮。

---

### 2.10 提取测试共享基类

**问题：** 4 个测试文件有几乎相同的 `setUp`/`tearDown` 样板代码。

**重复模式（出现在 `test_api_endpoints.py`、`test_rankings.py`、`test_v14_db_store.py::DbBackedApiTest`、`test_v16_queue_runtime.py::TaskEnqueueTest`）：**

```python
def setUp(self):
    self.tmpdir = tempfile.TemporaryDirectory()
    db_store.configure(self.tmpdir.name)
    import main
    db_store.configure(self.tmpdir.name)
    self.main = main
    self.old_data_dir = main.DATA_DIR
    main.DATA_DIR = self.tmpdir.name
```

另外 `MockResponse` 类在 `test_rankings.py` 和 `test_v16_queue_runtime.py` 中重复定义。

**修复方案：** 创建共享测试工具模块。

**新建文件：** `spider_core/tests/test_helpers.py`

```python
"""test_helpers.py — 测试共享基础设施。"""

import tempfile
import unittest

import db_store


class MockResponse:
    """模拟 HTTP 响应对象。"""
    def __init__(self, text="", status_code=200):
        self.text = text
        self.status_code = status_code


class IntegrationTestBase(unittest.TestCase):
    """需要完整 main + db_store 初始化的集成测试基类。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        db_store.configure(self.tmpdir.name)
        import main
        db_store.configure(self.tmpdir.name)
        self.main = main
        self.old_data_dir = main.DATA_DIR
        main.DATA_DIR = self.tmpdir.name

    def tearDown(self):
        import main
        main.DATA_DIR = self.old_data_dir
        self.tmpdir.cleanup()


class DbTestBase(unittest.TestCase):
    """仅需 db_store 初始化的数据库测试基类。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        db_store.configure(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()
```

**修改步骤：**
1. 创建 `spider_core/tests/test_helpers.py`。
2. 在 4 个测试文件中，将重复的 `setUp`/`tearDown` 替换为继承 `IntegrationTestBase` 或 `DbTestBase`。
3. 将 `MockResponse` 的重复定义替换为 `from test_helpers import MockResponse`。
4. 移除所有测试文件开头的 `sys.path.insert` 样板（改为在 `test_helpers.py` 中统一处理，或使用 `conftest.py`）。

**验证：** `python -m unittest discover -s spider_core/tests` 全量通过。

---

### 2.11 清理死代码

**汇总所有已确认的死代码：**

| 位置 | 内容 | 原因 |
|------|------|------|
| `state.js:33` | `let runtimeConfigCollapsed = true;` | 从未读取 |
| `settings.js` | `toggleRuntimePanel()` 函数 | 从未被调用 |
| `settings.js` | `renderRuntimePanelState()` 函数体 | 仅移除 hidden class，面板始终可见 |
| `task_repo.py:141` | `DELETE FROM task_logs WHERE task_id = ?` | `ON DELETE CASCADE` 已处理（P0.6 已修复） |
| `task_repo.py:146` | `task_checkpoints` 表检查和删除 | 表不存在（P0.6 已修复） |
| `SpiderService.java:107` | `TYPE_PHONE` 分支 | `minSdk 26` 永不执行 |
| 项目根目录 | `src/components/` 空目录 | 无内容的脚手架 |
| 项目根目录 | `diff_output.txt` | 调试产物 |

**修改步骤：**
1. 在 `state.js` 中删除 `runtimeConfigCollapsed` 声明。
2. 在 `settings.js` 中删除 `toggleRuntimePanel()` 函数。
3. 简化 `renderRuntimePanelState()` 为空函数或删除（需确认无调用方）。
4. 在 `SpiderService.java:initStealthWebView` 中删除 `else` 分支中的 `TYPE_PHONE` 代码。
5. 删除 `src/components/` 空目录。
6. 删除 `diff_output.txt`（如果不在 `.gitignore` 中）。

**验证：** 全量测试通过 + 浏览器无 JS 报错。

---

## 执行顺序与提交策略

| Commit | 包含任务 | 说明 |
|--------|---------|------|
| **Commit 1** | 2.1 + 2.2 + 2.3 | 后端：依赖注入 + 共享 helpers |
| **Commit 2** | 2.4 | 后端+前端：统一响应格式（需同步修改前后端） |
| **Commit 3** | 2.5 + 2.6 | 后端：db_store 内部 DRY |
| **Commit 4** | 2.7 + 2.8 + 2.9 | 前端：合并重复组件和函数 |
| **Commit 5** | 2.10 + 2.11 | 测试基类 + 死代码清理 |

每个 Commit 后运行：
```bash
python -m unittest discover -s spider_core/tests
```

Commit 2 和 Commit 4 额外需要浏览器手动全流程验证。
