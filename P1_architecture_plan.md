# P1 — 架构重构计划

> **目标：** 解决循环依赖、上帝模块、职责混乱等结构性问题，不改变外部 API 行为。
> **前置条件：** P0 全部完成并通过测试。
> **预计工作量：** 12–18 小时（建议分 4 个独立 commit 提交）
> **验证命令：** `python -m unittest discover -s spider_core/tests`

---

## 总体策略

核心思路是**自底向上**打破循环依赖链：

```
当前依赖（有环）:
  main.py ←→ routers/* （循环）
  db_store ←→ *_repo   （star-import 门面）

目标依赖（无环）:
  schemas.py          ← 纯数据定义，无依赖
  utils.py            ← 纯工具函数，无依赖
  db_store.py         ← schema + 连接 + helpers（不再 star-import）
  *_repo.py           → db_store（单向）
  services/*          → *_repo + db_store（单向）
  routers/*           → services + schemas（单向，不再 import main）
  main.py             → routers + services + schemas（单向，入口组装）
```

---

## 任务清单

### 1.1 提取 Pydantic 模型到 `schemas.py`

**目的：** 打破 `routers/* → main` 循环依赖的第一步。模型是纯数据定义，不依赖任何业务模块。

**新建文件：** `spider_core/schemas.py`

**从 `main.py` 提取的内容（第 82–124 行）：**

```python
"""schemas.py — Pydantic 请求/响应模型。"""

from pydantic import BaseModel
from ranking_utils import COLLECTION_TYPE_ACTOR


class TaskConfig(BaseModel):
    start_url: str
    cookie: str = ""
    user_agent: str = ""
    filename: str = ""
    proxies: str = None
    crawl_mode: str = ""
    collection_type: str = COLLECTION_TYPE_ACTOR
    ranking_category: str = ""
    ranking_period: str = ""
    remember_cookie: bool = False


class RuntimeConfig(BaseModel):
    cookie: str = ""
    remember_cookie: bool = False
    user_agent: str = ""
    proxies: str = ""
    trackers: list[str] = []


class CookieConfig(BaseModel):
    cookie: str = ""


class ModeConfig(BaseModel):
    mode: str


class TagConfigRequest(BaseModel):
    url: str
    cookie: str = ""
    user_agent: str = ""
    proxies: str = None


class DeleteRequest(BaseModel):
    filenames: list[str]


class SelectMagnetRequest(BaseModel):
    magnet_id: int


class AutoSelectRequest(BaseModel):
    """自动选择磁力请求（语义明确，替代 DeleteRequest 的误用）。"""
    filenames: list[str]
```

**修改步骤：**

1. 创建 `spider_core/schemas.py`，内容如上。
2. 在 `main.py` 中删除第 82–124 行的类定义，改为 `from schemas import *`。
3. 在所有 router 文件中，将 `from main import TaskConfig, ...` 改为 `from schemas import TaskConfig, ...`。
4. 在 `movies.py` 中将 `auto_select_magnets` 的参数类型从 `DeleteRequest` 改为 `AutoSelectRequest`。

**需要修改的文件清单：**

| 文件 | 修改内容 |
|------|---------|
| `main.py` | 删除 6 个 class 定义，添加 `from schemas import *` |
| `routers/tasks.py:12-22` | `from main import CookieConfig, ModeConfig, TaskConfig` → `from schemas import ...` |
| `routers/movies.py:8-14` | `from main import DeleteRequest, SelectMagnetRequest, TaskConfig` → `from schemas import ...` |
| `routers/settings.py:11-21` | `from main import RuntimeConfig, TagConfigRequest` → `from schemas import ...` |
| `routers/storage.py:11` | `from main import DeleteRequest` → `from schemas import DeleteRequest` |
| `routers/rankings.py:9-18` | `from main import TaskConfig` → `from schemas import TaskConfig` |

**验证：** 全量测试通过 + 手动验证 `POST /api/tasks` 和 `POST /api/magnets/auto_select` 端点正常。

---

### 1.2 提取工具函数到 `utils.py`

**目的：** 将与业务无关的纯工具函数从 `main.py` 中移出，进一步减少路由对 `main` 的依赖。

**新建文件：** `spider_core/utils.py`

**从 `main.py` 提取的函数：**

| 函数 | 当前位置 | 说明 |
|------|---------|------|
| `get_safe_name(filename)` | `main.py:155-160` | 安全文件名解析 |
| `parse_tag_filter(tags)` | `main.py:163-167` | 标签逗号分隔解析 |
| `ensure_zh_locale(url)` | `main.py:170-175` | URL 强制中文 locale |
| `build_proxy_dict(proxy)` | `main.py:178-179` | 代理字典构造 |
| `runtime_headers(runtime)` | `main.py:215-219` | 构造请求头 |

```python
"""utils.py — 与业务模块无关的纯工具函数。"""

from urllib.parse import parse_qs, urlencode, urlparse, urlunparse
from storage_utils import UnsafeFilenameError, get_safe_csv_path
from spider_engine import DATA_DIR


def get_safe_name(filename: str) -> str | None:
    try:
        _, safe_name = get_safe_csv_path(DATA_DIR, filename)
        return safe_name
    except UnsafeFilenameError:
        return None


def parse_tag_filter(tags: str = None) -> list[str]:
    if not tags:
        return []
    values = [tag.strip() for tag in tags.split(",")]
    return [tag for tag in values if tag and tag.lower() != "all"]


def ensure_zh_locale(url: str) -> str:
    parsed = urlparse(url)
    params = parse_qs(parsed.query, keep_blank_values=True)
    params["locale"] = ["zh"]
    query = urlencode(params, doseq=True)
    return urlunparse(parsed._replace(query=query))


def build_proxy_dict(proxy: str | None) -> dict | None:
    return {"http": proxy, "https": proxy} if proxy else None


def runtime_headers(runtime: dict) -> dict:
    return {
        "User-Agent": runtime.get("user_agent") or "",
        "Cookie": runtime.get("cookie") or "",
    }
```

**修改步骤：**

1. 创建 `spider_core/utils.py`，内容如上。
2. 在 `main.py` 中删除这 5 个函数定义，改为 `from utils import *`。
3. 在所有引用这些函数的文件中修改 import 来源：

| 文件 | 当前 import | 改为 |
|------|-----------|------|
| `routers/storage.py:11` | `from main import ..., get_safe_name, parse_tag_filter` | `from utils import get_safe_name, parse_tag_filter` |
| `routers/movies.py:8-14` | `from main import ..., get_safe_name` | `from utils import get_safe_name` |
| `routers/rankings.py:9-18` | `from main import ..., build_proxy_dict, get_safe_name, parse_tag_filter, runtime_headers` | `from utils import ...` |
| `routers/settings.py:11-21` | `from main import ..., build_proxy_dict, ensure_zh_locale, runtime_headers` | `from utils import ...` |
| `routers/tasks.py:12-22` | 无直接引用这些函数 | 无需改 |

**验证：** 全量测试通过。

---

### 1.3 创建 `services/task_service.py` — 任务业务逻辑

**目的：** 将 `main.py` 中的任务相关业务逻辑移入专门的 service 模块，使 `main.py` 回归为纯粹的应用入口。

**新建文件：** `spider_core/services/task_service.py`

**从 `main.py` 提取的函数和对应行号：**

| 函数 | 行号 | 说明 |
|------|------|------|
| `save_runtime_from_payload(config)` | 182-197 | 从请求合并并保存运行配置 |
| `get_runtime_for_request()` | 200-212 | 获取运行配置（含 Android cookie 回退） |
| `infer_task_filename(start_url, requested_filename, soup)` | 222-234 | 推断任务文件名 |
| `prepare_task_config(config)` | 237-307 | 预检查并构造任务配置 |
| `task_to_response(task, include_logs)` | 310-342 | 任务序列化为 API 响应 |
| `task_incremental_movie_codes(task)` | 345-360 | 提取增量爬取电影代码 |
| `create_task_from_config(config)` | 461-473 | 创建任务入口 |
| `resolve_task_cookie(cookie)` | 451-458 | 解析有效 cookie |

**关键变更：** `prepare_task_config` 和 `create_task_from_config` 当前返回 `JSONResponse`（失败时）或 `dict`（成功时）。重构后改为：

- 成功时返回 `dict`（不变）。
- 失败时抛出自定义异常 `TaskConfigError`，由路由层捕获并转为 HTTP 响应。

```python
"""services/task_service.py — 任务配置、准备、序列化的业务逻辑。"""

class TaskConfigError(Exception):
    """任务配置错误，包含 HTTP 状态码和消息。"""
    def __init__(self, status_code: int, msg: str, **extra):
        super().__init__(msg)
        self.status_code = status_code
        self.msg = msg
        self.extra = extra  # needs_mode, filename 等附加字段
```

**路由层调用方式变更示例（`routers/tasks.py`）：**

```python
# 之前
from main import create_task_from_config

def create_task(config: TaskConfig):
    return create_task_from_config(config)

# 之后
from services.task_service import create_task_from_config, TaskConfigError

def create_task(config: TaskConfig):
    try:
        return create_task_from_config(config)
    except TaskConfigError as e:
        return JSONResponse(status_code=e.status_code, content={"code": e.status_code, "msg": e.msg, **e.extra})
```

**修改步骤：**

1. 创建 `spider_core/services/task_service.py`，将上述 8 个函数移入。
2. 将 `prepare_task_config` 中所有 `return JSONResponse(...)` 改为 `raise TaskConfigError(...)`。
3. 将 `create_task_from_config` 中的 `isinstance(prepared, JSONResponse)` 检查改为直接调用（异常自动传播）。
4. 修改所有引用这些函数的文件：

| 文件 | 改动 |
|------|------|
| `routers/tasks.py:12-22` | `from main import create_task_from_config, ...` → `from services.task_service import ...` |
| `routers/movies.py:8-14` | `from main import create_task_from_config` → `from services.task_service import ...` |
| `routers/rankings.py:9-18` | `from main import create_task_from_config, fetch_html, get_runtime_for_request` → `from services.task_service import ...` |
| `routers/settings.py:11-21` | `from main import task_to_response, write_status_mirror, get_runtime_for_request` → `from services.task_service import ...` |

5. 在 `main.py` 中删除这些函数，改为 `from services.task_service import *`（保持向后兼容）。

**验证：** 全量测试通过 + 手动测试 `POST /api/tasks` 在各种错误场景下返回正确的 HTTP 状态码和 body。

---

### 1.4 创建 `services/queue_service.py` — 队列管理

**目的：** 封装队列线程的全局状态和编排逻辑。

**新建文件：** `spider_core/services/queue_service.py`

**从 `main.py` 提取的内容：**

| 函数/变量 | 行号 | 说明 |
|----------|------|------|
| `QUEUE_LOCK` | 74 | 队列锁 |
| `QUEUE_THREAD` | 75 | 队列线程引用 |
| `queue_worker()` | 377-402 | 队列主循环 |
| `ensure_queue_worker()` | 405-411 | 启动队列线程 |
| `is_queue_running()` | 414-415 | 查询队列状态 |
| `get_queue_status_data()` | 418-448 | 组装队列状态数据 |
| `write_status_mirror(task)` | 363-374 | 写状态镜像文件 |

```python
"""services/queue_service.py — 任务队列的线程管理与状态查询。"""

import threading
import db_store
from spider_engine import STATUS_FILE, run_task
from storage_utils import atomic_write_json
from services.task_service import task_to_response

QUEUE_LOCK = threading.RLock()
QUEUE_THREAD = None

# ... 移入上述函数
```

**修改步骤：**

1. 创建 `spider_core/services/queue_service.py`。
2. 在 `main.py` 中删除队列相关代码，改为 `from services.queue_service import *`。
3. 修改引用：

| 文件 | 改动 |
|------|------|
| `routers/tasks.py` | `from main import ensure_queue_worker, get_queue_status_data` → `from services.queue_service import ...` |
| `routers/settings.py` | `from main import write_status_mirror` → `from services.queue_service import ...` |

**验证：** 全量测试通过 + 手动测试 `POST /api/tasks/start_queue` 和 `GET /api/tasks/queue_status`。

---

### 1.5 让 `magnet_service` 返回领域对象而非 `JSONResponse`

**目的：** 与 1.3 的 `TaskConfigError` 策略一致，service 层不应耦合 FastAPI。

**文件：** `spider_core/services/magnet_service.py`

**当前问题：** `start_magnet_check`（第 81-95 行）返回 `JSONResponse` 对象。

**修改方案：**

定义自定义异常和结果类型：

```python
class MagnetCheckError(Exception):
    """磁力检测无法启动。"""
    def __init__(self, status_code: int, msg: str, data=None):
        super().__init__(msg)
        self.status_code = status_code
        self.msg = msg
        self.data = data


def start_magnet_check(scope, target, magnets, empty_msg, failed_only=False):
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
```

**路由层修改（`routers/magnets.py`）：**

```python
from services.magnet_service import start_magnet_check, MagnetCheckError

@router.post("/api/movies/{movie_id}/check_magnets")
def check_movie_magnets(movie_id: int, failed_only: bool = False):
    magnets = db_store.get_movie_magnets(movie_id)
    try:
        return start_magnet_check("movie", str(movie_id), magnets, "找不到候选磁力", failed_only)
    except MagnetCheckError as e:
        content = {"code": e.status_code, "msg": e.msg}
        if e.data:
            content["data"] = e.data
        return JSONResponse(status_code=e.status_code, content=content)
```

移除 `magnet_service.py` 中的 `from fastapi.responses import JSONResponse`。

**验证：** 运行 `test_v14_db_store.py` 中磁力检测相关测试 + 手动测试。

---

### 1.6 清理 `main.py` 末尾的重导出块

**目的：** 在 1.1–1.5 完成后，路由不再 `from main import` 任何业务符号，重导出块可以安全移除。

**文件：** `spider_core/main.py`，第 514–564 行

**修改步骤：**

1. 使用 `grep -r "from main import\|import main" spider_core/` 确认没有剩余的 `from main import` 引用（测试文件除外）。
2. 逐步删除重导出块中的导入组：
   - 先删除 `from routers.tasks import ...`（第 516-531 行）
   - 再删除 `from routers.movies import ...`（第 532-539 行）
   - 继续删除其余所有组
3. 对于**测试文件**中 `main.xxx()` 的调用，修改为直接引用目标模块（如 `from services.task_service import create_task_from_config`）。

**注意：** 保留 `from schemas import *` 和 `from utils import *`（如果测试依赖 `main.TaskConfig` 等）。或者更彻底地修改测试 import。

**最终 `main.py` 目标结构（约 80–100 行）：**

```python
# 1. 标准库 / 三方库 import
# 2. from schemas import * （向后兼容）
# 3. from utils import *   （向后兼容）
# 4. FastAPI app 创建 + 静态文件挂载
# 5. APP_VERSION, AUTH_HEADER 常量
# 6. db_store.configure() + 初始化
# 7. 认证中间件
# 8. GET / 和 GET /favicon.png
# 9. 路由注册 app.include_router(...)
# 10. start_server()
```

**验证：** 全量测试通过。

---

### 1.7 重构 `db_store.py` 的 star-import 门面

**目的：** 消除 `db_store.py` 末尾的 `from xxx import *`，改为显式导出。

**文件：** `spider_core/db_store.py`，第 675-678 行

**当前代码：**
```python
from task_repo import *
from settings_repo import *
from movie_repo import *
from export_service import *
```

**修改方案 A（保守 — 保持 `db_store.xxx()` 调用接口不变）：**

将 star-import 改为显式列出每个公开符号：

```python
from task_repo import (
    create_task, get_task, list_tasks, update_task, delete_task,
    update_task_status, append_task_log, get_task_logs,
    request_task_pause, resume_task_to_pending, request_task_cancel,
    claim_next_pending_task, get_active_task, get_current_task,
    recover_interrupted_tasks, cleanup_finished_tasks,
    count_tasks_by_state, save_task_checkpoint, load_task_checkpoint,
    update_task_cookie, update_task_mode,
)
from settings_repo import save_runtime_config, get_runtime_config
from movie_repo import (
    save_movie_result, get_collection_movies, get_collection_movie_ids,
    get_movie_magnets, select_movie_magnet, update_magnet_check_result,
    get_magnet_links, get_magnet_links_for_codes, collection_exists,
    get_history, get_existing_codes, clear_collection,
    auto_select_collection_magnets, delete_collections,
    import_existing_csvs, get_collection_source_url,
    get_ranking_movies, get_ranking_movie_ids,
    get_ranking_magnet_links, get_ranking_collection_filename,
)
from export_service import export_collection_to_csv_bytes
```

**修改方案 B（激进 — 消除门面，直接引用 repo）：**

所有调用方从 `db_store.xxx()` 改为 `task_repo.xxx()`、`movie_repo.xxx()` 等。这涉及大量文件改动，建议在 P2 阶段渐进实施。

**推荐：** 先执行方案 A（P1 范围），在 P2 中渐进迁移调用方到方案 B。

**验证：** 全量测试通过。

---

### 1.8 前端：拆分 `movies.js`（1518 行）

**目的：** 将过于庞大的 `movies.js` 按职责拆分为独立模块。

**拆分方案：**

#### 新建文件 1：`js/routing.js`（~180 行）

从 `movies.js` 提取的函数：
- `databaseRouteParts()`、`databaseRouteInfo()`、`currentDatabaseMovieId()`
- 所有 `*Hash()` 函数（`collectionHash`、`movieHash`、`rankingCategoryHash`、`rankingPeriodHash`、`rankingMovieHash`、`rankingMagnetHash`）
- `setDatabaseHash()`、`setRankingHash()`
- `renderDatabaseBreadcrumb()`
- `renderDatabaseRoute()` — 主路由分发器

#### 新建文件 2：`js/ranking.js`（~400 行）

从 `movies.js` 提取的函数：
- `RANKING_CATEGORIES`、`RANKING_PERIODS` 常量
- `top250OptionCache` 状态
- `rankingCategoryMeta()`、`rankingPeriodMeta()`
- `loadTop250Options()`
- `renderRankingCategoryPage()`、`renderRankingPeriodPage()`
- `renderRankingMovieListPage()`、`renderRankingMagnetListPage()`
- `renderRankingFilter()`、`renderRankingTagOption()`、`renderRankingExcludeOption()`
- `renderRankingMovies()`
- `renderRankingMagnetCheckButton()`（P2 中将改为复用 magnets.js 的通用实现）
- `copyRankingMagnets()`、`downloadRankingCsv()`
- `updateRankingList()`、`clearRankingList()`
- 所有排行榜标签筛选函数：`toggleRankingTag()`、`toggleRankingExcludeTag()`、`clearRankingExcludeTags()`

#### 新建文件 3：`js/magnet-table.js`（~150 行）

从 `movies.js` 提取的函数：
- `loadMagnets()`、`renderMagnetTable()`、`renderMagnetRow()`
- `refreshMagnetRows()`、`magnetRowSignature()`
- `syncSelectedMagnetToMovie()`、`syncSelectedMagnetToRankingMovie()`
- `selectMagnet()`、`updateMovieSelectedName()`

**修改步骤：**

1. 创建 3 个新 JS 文件。
2. 从 `movies.js` 中剪切相关函数到各新文件（注意保持全局函数形式，因为 onclick 依赖全局作用域）。
3. 在 `index.html` 中按正确顺序添加 `<script>` 标签（依赖顺序：`state.js` → `utils.js` → `meta.js` → `api.js` → `routing.js` → `magnet-table.js` → `ranking.js` → `movies.js` → `magnets.js` → `tasks.js` → `settings.js` → `app.js`）。
4. 为新文件添加 `?v=xxx` 缓存标识。

**验证：** 在浏览器中手动测试所有数据库页面导航流程：
- 集合列表 → 选择集合 → 电影列表 → 磁力详情
- 排行榜分类 → 周期 → 电影列表 → 磁力详情
- 标签筛选、复制磁力、下载 CSV
- 磁力检测启动/取消

---

### 1.9 前端：扩充 `api.js`

**目的：** 将散落在各 JS 文件中的 ~40 处 `.then(r => r.json())` 和错误处理逻辑集中到 `api.js`。

**新增函数：**

```javascript
/**
 * 发起 API 请求并解析 JSON 响应。
 * 自动处理 401 和非 JSON 响应错误。
 */
async function apiFetchJson(url, options = {}) {
    const response = await apiFetch(url, options);
    if (!response.ok) {
        let body;
        try { body = await response.json(); } catch { body = {}; }
        const msg = body.msg || body.message || `请求失败 (${response.status})`;
        throw Object.assign(new Error(msg), { status: response.status, body });
    }
    return response.json();
}

/**
 * POST JSON 请求的便捷封装。
 */
async function apiPost(url, body = null) {
    const options = { method: 'POST' };
    if (body !== null && body !== undefined) {
        options.headers = { 'Content-Type': 'application/json' };
        options.body = JSON.stringify(body);
    }
    return apiFetchJson(url, options);
}

/**
 * 下载 blob 并触发浏览器保存。
 */
async function apiDownloadBlob(url, filename) {
    const response = await apiFetch(url);
    if (!response.ok) {
        throw new Error(`下载失败 (${response.status})`);
    }
    const blob = await response.blob();
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    URL.revokeObjectURL(a.href);
    a.remove();
}
```

**迁移策略（渐进）：**

不需要一次性替换所有调用。建议：
1. 先在 `api.js` 中添加这 3 个函数。
2. 在新创建的 `ranking.js`、`magnet-table.js`、`routing.js` 中直接使用新 API。
3. 在 P2 阶段将 `tasks.js`、`movies.js`（剩余部分）、`magnets.js`、`settings.js` 中的旧模式逐步替换。

**验证：** 浏览器中所有 API 调用正常，网络面板无异常请求。

---

## 执行顺序与提交策略

建议分 4 个独立 commit，每个 commit 单独可验证：

| Commit | 包含任务 | 说明 |
|--------|---------|------|
| **Commit 1** | 1.1 + 1.2 | 提取 schemas.py + utils.py，修改所有 import |
| **Commit 2** | 1.3 + 1.4 + 1.5 | 创建 task_service + queue_service，重构 magnet_service |
| **Commit 3** | 1.6 + 1.7 | 清理 main.py 重导出 + 显式化 db_store 门面 |
| **Commit 4** | 1.8 + 1.9 | 前端 movies.js 拆分 + api.js 扩充 |

每个 Commit 后运行：
```bash
python -m unittest discover -s spider_core/tests
```

Commit 4 额外需要浏览器手动验证。

---

## 风险与回退策略

| 风险 | 缓解措施 |
|------|---------|
| 循环 import 在重构中间状态出现 | 每个 commit 保持 `from main import *` 兼容层，确保中间状态可运行 |
| 测试中 `import main` 的模块缓存问题 | 修改测试直接引用新模块，或在 setUp 中 reload |
| 前端拆分后 `onclick` 找不到全局函数 | 确保所有新文件在使用前加载（script 标签顺序） |
| Android Chaquopy 无法找到新模块 | 验证 Chaquopy `pip` 不需要额外配置（它直接读 spider_core/ 下的 .py 文件） |
