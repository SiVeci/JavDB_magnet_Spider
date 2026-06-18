# P3 — 规范化 / 长期改善计划

> **目标：** 提升代码规范性、补全测试覆盖、加固 CI/CD 和 Android 端质量。
> **前置条件：** P2 全部完成并通过测试。
> **预计工作量：** 20–30 小时（建议按子模块分多个 PR 渐进实施）
> **验证命令：**
> - 后端：`python -m unittest discover -s spider_core/tests`
> - Android：`./gradlew test`
> - Docker：`docker build -t javdb-spider .`

---

## 一、后端类型注解与文档（预计 6–8 小时）

### 3.1 为核心模块添加类型注解

**目标：** 让 IDE 和静态分析工具（mypy/pyright）能够捕获类型错误。

**优先级：** 按调用频率和复杂度排序。

#### 第一批（高价值 — repo 层公开函数）

**文件：`task_repo.py`**

为所有公开函数添加参数和返回类型注解。示例：

```python
def create_task(
    start_url: str,
    filename: str = "",
    crawl_mode: str = "",
    collection_type: str = "actor",
    ranking_category: str = "",
    ranking_period: str = "",
) -> str:
    """创建爬取任务并返回 task_id。"""
    ...

def get_task(task_id: str) -> dict | None:
    """根据 task_id 获取任务详情，不存在返回 None。"""
    ...

def list_tasks(limit: int = 100) -> list[dict]:
    """按创建时间倒序列出最近的任务。"""
    ...

def update_task_status(
    task_id: str,
    state: str = "",
    progress: str = "",
    current: str = "",
    log_msg: str = "",
    error_message: str = "",
    **extra_fields,
) -> None:
    ...

def request_task_pause(task_id: str) -> bool: ...
def resume_task_to_pending(task_id: str) -> bool: ...
def request_task_cancel(task_id: str) -> bool: ...
def claim_next_pending_task() -> dict | None: ...
def delete_task(task_id: str) -> bool: ...
```

**文件：`movie_repo.py`**

```python
def save_movie_result(
    filename: str,
    code: str,
    title: str,
    url: str,
    magnets: list[dict],
    source_url: str = "",
    tags: list[str] | None = None,
    collection_type: str = "actor",
    ranking_category: str = "",
    ranking_period: str = "",
) -> None: ...

def get_collection_movies(filename: str) -> dict: ...
def get_collection_movie_ids(filename: str) -> list[int]: ...
def collection_exists(filename: str) -> bool: ...
def get_history() -> list[dict]: ...
def get_magnet_links(filename: str, tags: list[str] | None = None, exclude_tags: list[str] | None = None) -> list[dict]: ...
def select_movie_magnet(movie_id: int, magnet_id: int) -> bool: ...
def delete_collections(filenames: list[str], data_dir: str) -> tuple[list[str], list[str]]: ...
```

**文件：`settings_repo.py`**

```python
def save_runtime_config(
    cookie: str | None = None,
    remember_cookie: bool = False,
    user_agent: str | None = None,
    proxies: str | None = None,
    trackers: list[str] | None = None,
) -> None: ...

def get_runtime_config(include_cookie: bool = True) -> dict: ...
```

**文件：`magnet_checker.py`**

```python
def check_magnet(magnet_link: str, user_trackers: list[str] | None = None) -> dict: ...
def extract_info_hash(magnet_link: str) -> str: ...
def classify_result(status: str | None, seeders: int, check_error: str | None) -> str: ...
def get_trackers_for_magnet(magnet_link: str, user_trackers: list[str] | None = None) -> list[str]: ...
```

#### 第二批（中等价值 — db_store 内部 helpers）

```python
def _now() -> float: ...
def _to_float(value: object, default: float = 0.0) -> float: ...
def _to_int(value: object, default: int = 0) -> int: ...
def _normalize_string_list(values: list | None) -> list[str]: ...
def _string_list_to_json(values: list | None) -> str: ...
def _string_list_from_json(value: str | None) -> list[str]: ...
def _collection_id(conn: sqlite3.Connection, filename: str, ...) -> int: ...
def _matches_tags(row_tags_json: str, required_tags: list[str] | None, exclude_tags: list[str] | None = None) -> bool: ...
```

#### 第三批（service 层 — P1 新建的文件）

P1 中新创建的 `services/task_service.py`、`services/queue_service.py` 应在创建时就包含完整类型注解。

**验证：**
- 可选：`pip install pyright && pyright spider_core/` 检查类型一致性。
- 全量测试通过。

---

### 3.2 为关键函数添加 docstring

**规范：** 使用 Google 风格 docstring。对所有公开函数添加一行摘要，对复杂函数添加 Args/Returns/Raises 说明。

**优先文件：**

| 文件 | 公开函数数 | 当前有 docstring 的 |
|------|----------|-------------------|
| `task_repo.py` | 20 | 0 |
| `movie_repo.py` | 16 | 0 |
| `db_store.py` | 8（不含 re-export） | 1（`_normalize_string_list`） |
| `magnet_checker.py` | 8 | 0 |
| `spider_engine.py` | 5 | 0 |
| `services/magnet_service.py` | 6 | 1（`start_magnet_check`） |

**示例：**

```python
def claim_next_pending_task() -> dict | None:
    """原子性地认领队列中第一个 pending 状态的任务。

    将任务状态从 pending 更改为 running，并记录启动时间。
    如果没有 pending 任务，返回 None。

    Returns:
        任务字典（dict）或 None。

    Note:
        当前实现在高并发下存在 TOCTOU 竞态（SELECT + UPDATE 非原子），
        但单 worker 架构下不会触发。
    """
```

**验证：** 静态审查。

---

## 二、测试覆盖补全（预计 8–12 小时）

### 3.3 补充爬虫编排核心测试

**当前状态：** `spider_engine.py` 的核心爬取循环 `run_spider` / `run_task` 完全没有测试覆盖。这是项目最关键的功能。

**新建文件：** `spider_core/tests/test_crawl_orchestration.py`

**测试场景设计：**

```python
class CrawlOrchestrationTest(DbTestBase):
    """测试 run_task 的编排逻辑（mock 网络层）。"""

    @patch('spider_engine.fetch_html')
    def test_single_page_actor_crawl(self, mock_fetch):
        """单页演员列表 → 详情页 → 保存结果。"""
        # 准备：构造列表页 HTML（含 1 个电影链接）+ 详情页 HTML（含磁力）
        # 执行：run_task(task_id)
        # 验证：任务状态 finished，集合中有 1 部电影，磁力正确保存

    @patch('spider_engine.fetch_html')
    def test_multi_page_pagination(self, mock_fetch):
        """多页列表 → 正确翻页 → 所有电影保存。"""
        # 准备：构造含 "下一页" 链接的列表页 HTML
        # 验证：fetch_html 被调用了正确的页数次

    @patch('spider_engine.fetch_html')
    def test_incremental_mode_skips_existing(self, mock_fetch):
        """增量模式跳过已存在的电影代码。"""
        # 准备：预先保存一些电影到集合
        # 执行：以 incremental 模式运行
        # 验证：只处理新增电影

    @patch('spider_engine.fetch_html')
    def test_overwrite_mode_clears_existing(self, mock_fetch):
        """覆盖模式清除已有数据。"""

    @patch('spider_engine.fetch_html')
    def test_pause_request_stops_crawl(self, mock_fetch):
        """运行中请求暂停 → 爬取在当前电影完成后停止。"""
        # 在 mock 中调用 request_task_pause
        # 验证：任务状态变为 paused

    @patch('spider_engine.fetch_html')
    def test_cancel_request_stops_crawl(self, mock_fetch):
        """运行中请求取消 → 爬取停止并标记 canceled。"""

    @patch('spider_engine.fetch_html')
    def test_network_error_during_detail_page(self, mock_fetch):
        """详情页网络错误 → 记录错误但继续处理其他电影。"""

    @patch('spider_engine.fetch_html')
    def test_checkpoint_save_and_resume(self, mock_fetch):
        """爬取中保存 checkpoint → 恢复后从 checkpoint 继续。"""
```

**注意事项：**
- 需要构造真实的 JavDB HTML 片段作为 mock 返回值（可从现有页面中提取最小化版本）。
- Mock `fetch_html` 而非 `curl_cffi.requests.get`，以保持测试与 HTTP 库解耦。
- 使用 `TemporaryDirectory` 隔离。

**验证：** 新测试全部通过。

---

### 3.4 补充 API 写操作端点测试

**当前状态：** `test_api_endpoints.py` 仅覆盖读操作端点。以下写操作端点缺少测试：

| 端点 | 操作 | 建议测试 |
|------|------|---------|
| `DELETE /api/tasks/{task_id}` | 删除任务 | 正常删除、不存在返回 404 |
| `POST /api/delete` | 批量删除集合 | 全部成功、部分失败、全部失败（P0 修复后应返回 400） |
| `POST /api/tasks/{task_id}/pause` | 暂停任务 | 运行中暂停成功、非法状态返回 400 |
| `POST /api/tasks/{task_id}/resume` | 恢复任务 | 暂停后恢复成功、非法状态返回 400 |
| `POST /api/tasks/{task_id}/cancel` | 取消任务 | 运行中取消成功 |
| `POST /api/tasks/{task_id}/mode` | 更改模式 | 正常更改、非法模式返回 400 |
| `POST /api/tasks/cleanup` | 清理已完成任务 | 有/无已完成任务 |
| `POST /api/magnets/auto_select` | 自动选择磁力 | 正常选择、空集合 |

**新建文件：** `spider_core/tests/test_write_endpoints.py`

**验证：** 新测试全部通过。

---

### 3.5 补充队列生命周期测试

**当前状态：** 队列线程的启动/停止/异常恢复完全没有测试。

**新增到：** `spider_core/tests/test_v16_queue_runtime.py` 或新建 `test_queue_lifecycle.py`

```python
class QueueLifecycleTest(IntegrationTestBase):

    @patch('spider_engine.run_task')
    def test_queue_processes_tasks_in_order(self, mock_run):
        """队列按 FIFO 顺序处理任务。"""

    @patch('spider_engine.run_task', side_effect=Exception("boom"))
    def test_queue_continues_after_task_failure(self, mock_run):
        """单个任务失败不影响后续任务执行。"""

    @patch('spider_engine.run_task')
    def test_queue_stops_on_pause(self, mock_run):
        """任务暂停后队列线程退出。"""

    def test_ensure_queue_worker_idempotent(self):
        """多次调用 ensure_queue_worker 不会创建多个线程。"""

    @patch('spider_engine.run_task')
    def test_queue_auto_stops_when_empty(self, mock_run):
        """队列处理完所有任务后自动停止。"""
```

**验证：** 新测试全部通过。

---

### 3.6 消除重复测试覆盖

**问题：** `parse_movie_tags` 在 `test_movie_tags.py` 和 `test_spider_engine.py::ParseMovieTagsTest` 中重复测试。

**修复方案：**
- 保留 `test_movie_tags.py`（更专注，命名更清晰）。
- 删除 `test_spider_engine.py` 中的 `ParseMovieTagsTest` 类。
- 在 `test_movie_tags.py` 中合并两边独有的测试场景（如果有）。

**问题：** `test_udp_response_parser_shape`（`test_magnet_checker.py:119-124`）不测试任何生产代码。

**修复方案：** 删除该测试用例，或改写为真正测试 `magnet_checker` 内部 UDP 响应解析逻辑的用例。

**验证：** 全量测试通过，测试数量减少但覆盖度不降。

---

## 三、CI/CD 加固（预计 3–4 小时）

### 3.7 在 CI 工作流中添加测试步骤

**问题：** 两个 CI 工作流（`android-build.yml` 和 `docker-publish.yml`）都不运行测试就直接构建/发布。

**修改文件：** `.github/workflows/android-build.yml`

在 `Build APK` 步骤之前添加：

```yaml
    - name: Run Python tests
      run: |
        pip install -r requirements.txt
        python -m unittest discover -s spider_core/tests
```

**修改文件：** `.github/workflows/docker-publish.yml`

在 `Build and push` 步骤之前添加：

```yaml
    - name: Run Python tests
      run: |
        pip install -r requirements.txt
        python -m unittest discover -s spider_core/tests
```

**验证：** 手动触发工作流，确认测试步骤执行且通过后才继续构建。

---

### 3.8 统一版本注入逻辑

**问题：** 两个工作流使用不同的 `sed` 模式注入版本号：
- `android-build.yml:29`：匹配 `JAVDB_SPIDER_VERSION", "[^"]*"`
- `docker-publish.yml:27`：匹配 `"dev-local"`

如果 `main.py` 中的默认值不是 `"dev-local"`（当前是 `"1.9.1"`），Docker 工作流的 sed 会静默失败。

**修复方案：** 统一两个工作流的版本注入模式，使用相同的 sed 命令：

```yaml
# 在两个工作流中统一使用
- name: Set version
  run: |
    sed -i 's/JAVDB_SPIDER_VERSION\s*=\s*os\.getenv("JAVDB_SPIDER_VERSION",\s*"[^"]*")/JAVDB_SPIDER_VERSION = os.getenv("JAVDB_SPIDER_VERSION", "${{ github.event.inputs.version }}")/' spider_core/main.py
```

或更简单的方案 — 通过环境变量注入（不修改源码）：

```yaml
# Dockerfile 中已有 ENV JAVDB_SPIDER_VERSION，在 CI 中覆盖即可
- name: Build and push
  uses: docker/build-push-action@v5
  with:
    build-args: |
      JAVDB_SPIDER_VERSION=${{ github.event.inputs.version }}
```

对应修改 `Dockerfile`：
```dockerfile
ARG JAVDB_SPIDER_VERSION=dev-local
ENV JAVDB_SPIDER_VERSION=${JAVDB_SPIDER_VERSION}
```

**验证：** 手动触发两个工作流，检查构建产物中的版本号正确。

---

### 3.9 Docker 镜像安全加固

**问题：** 容器以 root 用户运行；无 `.dockerignore`；无漏洞扫描。

**修改文件：** `Dockerfile`

添加非 root 用户：
```dockerfile
RUN useradd -r -m -s /bin/false appuser
# ... (安装依赖后)
RUN chown -R appuser:appuser /app/spider_core/data
USER appuser
```

**新建文件：** `.dockerignore`

```
.git
.github
.gradle
.idea
__pycache__
*.pyc
app/
src/
gradle/
*.jks
*.md
spider_core/tests/
spider_core/data/
diff_output.txt
local.properties
```

**可选 — 添加漏洞扫描步骤到 `docker-publish.yml`：**

```yaml
    - name: Scan for vulnerabilities
      uses: aquasecurity/trivy-action@master
      with:
        image-ref: ${{ steps.meta.outputs.tags }}
        format: 'table'
        exit-code: '1'
        severity: 'CRITICAL,HIGH'
```

**验证：**
- `docker build -t javdb-spider .` 成功。
- `docker run javdb-spider whoami` 输出非 root 用户。
- 确认镜像中不包含测试文件：`docker run javdb-spider ls tests/` 应失败。

---

## 四、Android 端规范化（预计 4–6 小时）

### 3.10 字符串资源化

**问题：** 约 20+ 处中文字符串硬编码在 Java 代码中。

**修改文件：**
- `app/src/main/res/values/strings.xml` — 添加所有字符串资源
- `app/src/main/java/com/javdb_spider/app/MainActivity.java` — 替换硬编码字符串
- `app/src/main/java/com/javdb_spider/app/SpiderService.java` — 替换硬编码字符串

**提取清单（按文件）：**

**MainActivity.java：**

| 行号 | 当前硬编码 | 建议资源名 |
|------|----------|-----------|
| 109 | `"Cookie 已自动接管"` | `@string/cookie_auto_captured` |
| 120 | `"✅ 链接已复制..."` | `@string/link_copied` |
| 121 | `"⚠️ 当前页面..."` | `@string/page_not_javdb` |
| 138 | `"未找到可用的浏览器"` | `@string/no_browser_found` |
| 181 | `"尚无 WLAN 地址"` | `@string/no_wlan_address` |
| 189 | `"正在获取 Cookie..."` | `@string/getting_cookie` |
| 206 | `"请在系统设置中..."` | `@string/overlay_permission_required` |
| 225 | `"爬取引擎已启动..."` | `@string/engine_started_browser` |
| 238 | `"爬取引擎已启动..."` | `@string/engine_started_lan` |
| 243 | `"LAN 地址已复制"` | `@string/lan_address_copied` |
| 246 | `"WLAN 不可用..."` | `@string/wlan_unavailable` |
| 274 | `"这不是当前引擎的首页..."` | `@string/not_engine_home` |

**SpiderService.java：**

| 行号 | 当前硬编码 | 建议资源名 |
|------|----------|-----------|
| 67 | `"JavDB 磁力引擎"` | `@string/notification_channel_name` |
| 213 | `"正在启动引擎..."` | `@string/engine_starting` |
| 225 | `"引擎正在初始化..."` | `@string/engine_initializing` |
| 各处 | 通知内容文本 | `@string/notification_*` |

**验证：** `./gradlew assembleDebug` 编译通过（注意环境限制不实际执行，通过静态检查确认）。

---

### 3.11 颜色资源化与暗色模式准备

**问题：** 所有颜色硬编码在 Java 和 XML 中，无暗色模式支持。

**修改文件：**
- `app/src/main/res/values/colors.xml` — 添加语义颜色
- `app/src/main/res/values-night/colors.xml` — 添加暗色模式颜色
- `app/src/main/res/layout/activity_main.xml` — 替换硬编码颜色

**步骤 1：** 在 `colors.xml` 中定义语义颜色：

```xml
<resources>
    <!-- 现有颜色保持不变 -->

    <!-- 语义颜色 -->
    <color name="background_primary">#F3F4F6</color>
    <color name="text_primary">#1F2937</color>
    <color name="text_secondary">#6B7280</color>
    <color name="divider">#E5E7EB</color>
    <color name="accent_primary">#4F46E5</color>
    <color name="accent_success">#10B981</color>
    <color name="card_background">#FFFFFF</color>
    <color name="text_hint">#9CA3AF</color>
    <color name="surface_secondary">#EEF2FF</color>
    <color name="text_dark">#374151</color>
</resources>
```

**步骤 2：** 创建 `values-night/colors.xml`（暗色对应）：

```xml
<resources>
    <color name="background_primary">#111827</color>
    <color name="text_primary">#F9FAFB</color>
    <color name="text_secondary">#9CA3AF</color>
    <color name="divider">#374151</color>
    <color name="accent_primary">#818CF8</color>
    <color name="accent_success">#34D399</color>
    <color name="card_background">#1F2937</color>
    <color name="text_hint">#6B7280</color>
    <color name="surface_secondary">#1E1B4B</color>
    <color name="text_dark">#E5E7EB</color>
</resources>
```

**步骤 3：** 在 `activity_main.xml` 中将 `android:background="#F3F4F6"` 替换为 `android:background="@color/background_primary"`，其他颜色类似。

**步骤 4：** 在 `MainActivity.java` 中将 `Color.parseColor("#F3F4F6")` 替换为 `ContextCompat.getColor(this, R.color.background_primary)`。

**验证：** 静态审查 XML 合法性 + 确认所有硬编码颜色已替换。

---

### 3.12 Android 安全加固

**问题 A：** `fetchHtml` 无 URL scheme 校验。

**文件：** `SpiderService.java` 的 `fetchHtml` 方法

**修复：** 在 `loadUrl` 之前添加 scheme 白名单检查：

```java
private void fetchHtml(String url, FetchCallback callback) {
    // URL scheme 白名单检查
    if (url == null || (!url.startsWith("http://") && !url.startsWith("https://"))) {
        callback.onResult(null);
        return;
    }
    // ... 现有逻辑
}
```

**问题 B：** `WAKE_LOCK` 权限声明但从未使用。

**文件：** `AndroidManifest.xml`

**修复：** 移除 `<uses-permission android:name="android.permission.WAKE_LOCK" />`，或者在 `SpiderService` 中正确获取和释放 WakeLock（如果确实需要后台保活）。

**建议：** 如果用户反馈息屏后服务不稳定，则添加 WakeLock 获取逻辑；否则移除权限声明。

**问题 C：** Release 构建未启用混淆。

**文件：** `app/build.gradle`

**修复：**
```groovy
buildTypes {
    release {
        minifyEnabled true
        shrinkResources true
        proguardFiles getDefaultProguardFile('proguard-android-optimize.txt'), 'proguard-rules.pro'
        // ... 签名配置
    }
}
```

同时需要在 `proguard-rules.pro` 中添加规则，保留 Chaquopy 和 WebView 相关类：

```proguard
# 保留 Chaquopy Python 桥接
-keep class com.chaquo.python.** { *; }

# 保留 WebViewBridge（被 Python 通过反射调用）
-keep class com.javdb_spider.app.WebViewBridge { *; }

# 保留 JavaScript 接口
-keepclassmembers class * {
    @android.webkit.JavascriptInterface <methods>;
}
```

**验证：** 静态审查 + 确认 proguard 规则覆盖关键类。

---

### 3.13 清理 Android 死代码

**问题：** `SpiderService.java:107` 的 `TYPE_PHONE` 分支在 `minSdk 26` 下永远不会执行。

**文件：** `SpiderService.java` 的 `initStealthWebView` 方法

**当前代码（伪）：**
```java
if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
    // TYPE_APPLICATION_OVERLAY
} else {
    // TYPE_PHONE  ← 死代码
}
```

**修复：** 删除 `else` 分支，只保留 `TYPE_APPLICATION_OVERLAY` 逻辑，移除 `if` 判断（`minSdk 26 = API 26 = O`）。

**验证：** 静态审查。

---

### 3.14 添加通知 PendingIntent 和停止按钮

**问题：** 通知无点击动作，也无停止引擎的机制。

**文件：** `SpiderService.java`

**修复 A — 通知点击打开 MainActivity：**

```java
private Notification buildNotification(String text) {
    Intent intent = new Intent(this, MainActivity.class);
    intent.setFlags(Intent.FLAG_ACTIVITY_SINGLE_TOP);
    PendingIntent pendingIntent = PendingIntent.getActivity(
        this, 0, intent, PendingIntent.FLAG_IMMUTABLE
    );

    return new NotificationCompat.Builder(this, CHANNEL_ID)
        .setContentTitle("JavDB 磁力引擎")
        .setContentText(text)
        .setSmallIcon(R.mipmap.ic_launcher)
        .setContentIntent(pendingIntent)  // 新增
        .setOngoing(true)
        .build();
}
```

**修复 B — 添加停止按钮（通知 Action）：**

```java
// 在 Service 中注册 BroadcastReceiver 处理停止动作
Intent stopIntent = new Intent("com.javdb_spider.STOP_SERVICE");
PendingIntent stopPendingIntent = PendingIntent.getBroadcast(
    this, 0, stopIntent, PendingIntent.FLAG_IMMUTABLE
);

builder.addAction(R.drawable.ic_launcher_foreground, "停止引擎", stopPendingIntent);
```

在 `onCreate` 中注册 receiver：
```java
registerReceiver(new BroadcastReceiver() {
    @Override
    public void onReceive(Context context, Intent intent) {
        stopSelf();
    }
}, new IntentFilter("com.javdb_spider.STOP_SERVICE"));
```

**验证：** 静态审查（需手动在设备上测试通知行为）。

---

## 五、后端杂项改进（预计 2–3 小时）

### 3.15 将 `spider_engine.py` 中的硬编码常量提取为模块级命名常量

**当前硬编码：**

| 位置 | 值 | 建议常量名 |
|------|---|-----------|
| `spider_engine.py:353` | `time.sleep(1.5)` | `PAGE_DELAY_SECONDS = 1.5` |
| `spider_engine.py:401` | `time.sleep(0.1)` | `DETAIL_DELAY_SECONDS = 0.1` |
| `spider_engine.py:441` | `time.sleep(2)` | `RETRY_DELAY_SECONDS = 2.0` |
| `spider_engine.py:63` | `timeout=15` | `HTTP_TIMEOUT_SECONDS = 15` |
| `spider_engine.py:248-253` | HTTP 请求头 | `DEFAULT_HEADERS = {...}` |
| `magnet_checker.py:199` | `"JavDB-Magnet-Spider/1.0"` | 使用 `APP_VERSION` 或独立常量 |

**修复：** 在各文件顶部定义命名常量，替换所有硬编码值。

**验证：** 全量测试通过。

---

### 3.16 修复 `spider_engine.py` 中 `import time` 在函数内部

**文件：** `spider_core/main.py`（P1 重构后在 `services/task_service.py`），第 232 行

**当前：** `infer_task_filename` 函数体内 `import time`。

**修复：** 将 `import time` 移至文件顶部。

**验证：** 无功能变化，静态审查即可。

---

### 3.17 改善 `read_json_file` 的错误处理

**文件：** `spider_core/storage_utils.py`

**问题：** `read_json_file` 只捕获 `FileNotFoundError`，`json.JSONDecodeError` 会作为未处理异常传播。

**修复：**

```python
def read_json_file(filepath, default=None):
    """读取 JSON 文件，文件不存在或格式错误时返回 default。"""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default
    except (json.JSONDecodeError, ValueError):
        logging.warning("JSON 解析失败: %s", filepath)
        return default
```

**验证：** 补充一个测试：写入非法 JSON 内容 → `read_json_file` 返回 default 值。

---

## 执行顺序建议

P3 的任务相对独立，可以按模块并行推进：

| 阶段 | 任务 | 可并行 |
|------|------|--------|
| **阶段 A** | 3.7 + 3.8 + 3.9（CI/CD 加固） | 独立，可先行 |
| **阶段 B** | 3.1 + 3.2（类型注解 + docstring） | 独立，可与 A 并行 |
| **阶段 C** | 3.3 + 3.4 + 3.5 + 3.6（测试补全） | 依赖 A 中的 CI 测试步骤 |
| **阶段 D** | 3.10 + 3.11 + 3.12 + 3.13 + 3.14（Android 规范化） | 独立，可与 B/C 并行 |
| **阶段 E** | 3.15 + 3.16 + 3.17（后端杂项） | 独立，随时可做 |

**建议 commit 粒度：** 每个编号任务一个 commit，便于 code review 和回退。

---

## 长期展望（超出 P3 范围，记录备忘）

以下事项超出当前优化范围，但值得在未来版本中考虑：

1. **前端框架迁移：** 从全局作用域 vanilla JS 迁移到 ES modules 或轻量框架（Preact/Alpine.js），解决全局污染和 HTML-in-JS 问题。
2. **Pydantic v2 升级：** 配合 FastAPI 升级，获得显著的性能提升和更好的类型支持。
3. **数据库连接池：** 引入 `aiosqlite` 或连接池，减少每次请求的 PRAGMA 开销。
4. **任务状态机形式化：** 用显式状态转换表替代 ad-hoc if 链，防止非法状态转换。
5. **WebSocket 实时推送：** 替代前端轮询，降低网络开销。
6. **Python-Java 桥接安全加固：** URL scheme 白名单 + 并发 fetch 队列。
7. **国际化（i18n）：** 后端 CSV 表头和错误消息的多语言支持。
