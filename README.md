<div align="center">
  <img src="spider_core/web/public/favicon.png" alt="JavDB Magnet Spider Logo" width="180">
  <h1>JavDB Magnet Spider</h1>
  <p>
    <img src="https://img.shields.io/badge/Platform-Android%20%7C%20Docker%20%7C%20PC-brightgreen" alt="Platform">
    <img src="https://img.shields.io/badge/Python-3.12-blue" alt="Python Version">
    <img src="https://img.shields.io/badge/Framework-FastAPI%20%7C%20Chaquopy-orange" alt="Framework">
    <img src="https://img.shields.io/badge/License-MIT-lightgrey" alt="License">
  </p>
</div>

---

## 核心架构概述

本项目为跨平台架构的 JavDB 自动化数据采集引擎，兼容 Docker、PC 桌面端及 Android 移动端运行环境。核心业务流实现了从端侧人机验证（CF WAF）穿透、守护进程模式下的静默采集，至基于 WebUI 的远程任务调度与状态同步的完整生命周期管理，支持指定目标路由的批量数据抓取与持久化。

## 核心特性

* **任务调度机制（Task Queue）**：
  * **核心对象/组件/路由（`spider_engine.py` / `/api/v1/events`）**：在后端引擎中引入基于内存的状态机与任务队列。支持批量下发采集指令，系统按序执行后台调度。前端控制面板通过 Server-Sent Events (SSE) 事件总线获取毫秒级实时进度与日志推送，并暴露 API 路由实现对运行中任务的原子级暂停、恢复与终止操作。

* **授权浏览器与登录态生命周期管理（Auth Browser & Cookie Lifecycle）**：
  * **核心对象/组件/路由（`auth_browser_service.py` / `/api/v1/auth_browser`）**：引入独立的 Auth Browser 服务组件。在 PC 或 Docker 环境下，用户可直接从 WebUI 唤起远程浏览器进行可视化登录，系统自动捕获并接管 Cookie。**主程序内建 VNC 与 WebSocket 流量反向代理**，无需暴露额外端口即可安全穿透。内置 Cookie 健康度主动校验机制，当爬虫遭遇反爬拦截或 Cookie 失效时，引擎自动将当前任务挂起至 `waiting_cookie` 状态，智能引导用户重新授权后无缝恢复执行，确保长时运行的连续性。

* **排行榜解析引擎（Ranking Parser）**：
  * **核心对象/组件/路由（`ranking_utils.py` / `/api/v1/rankings`）**：通过底层工具模块解析日/周/月常规榜单、热播流及支持多维度查询（年份/分类）的 TOP250 榜单数据。底层打通排行榜视图与爬虫任务队列，支持通过一键操作将榜单全量转化为采集任务。同时在前端页面耦合磁力可用性校验模块与多维标签交并集过滤模块。

* **端侧运行容器（Android Native Engine）**：
  * **核心对象/组件/路由（`Chaquopy` / `WebViewBridge.java`）**：通过 Chaquopy 将 FastAPI 后端全量打包至 Android 运行时。集成原生 WebViewBridge 组件，在客户端层面实现 WAF 质询拦截。在拦截通过后，系统自动同步会话 Cookie 至 Python 引擎，实现端侧鉴权状态向采集引擎的无缝移交。

* **数据同步与容灾策略（Incremental & Resume）**：
  * **核心对象/组件/路由（`db_store.py` / `spider_data.db`）**：支持基于增量策略的页面遍历机制。引擎启动前依赖本地数据库（SQLite）执行哈希/主键碰撞检测，跳过已持久化的数据实体。采集生命周期内，通过定时事务记录游标位置，在遇到异常退出时提供现场保护与恢复机制，确保数据集的幂等性与一致性。

* **持久化与数据视图（Storage & View）**：
  * **核心对象/组件/路由（`export_service.py` / `routers/storage.py`）**：底层依赖 SQLite 提供本地存储服务。内嵌基于 Web 的数据管理面板，支持数据集合（Collections）与实体（Movies）的结构化展示。支持查看实体的全量候选磁力资源，允许手动指定生效资源的优先级。系统集成了 CSV 序列化导出服务，以及单任务磁力链路的聚合复制模块。

* **收藏演员管理系统（Actor Collection Management）**：
  * **核心对象/组件/路由（`actor_collection_repo.py` / `/api/v1/actors`）**：集成了全新的收藏演员数据看板与数据库结构。支持分类展示个人收藏的演员动态，提供从远端一键刷新快照能力。深度融合任务调度系统，支持在列表页直接展开历史标签特征，实现无缝聚合下发抓取任务，大幅提高专项数据追踪效率。

* **资源评估策略（Magnet Priority Strategy）**：
  * **核心对象/组件/路由（`services/magnet_service.py`）**：内置多维度的启发式评分函数（如：无码特征加权、高清特征加权、内嵌字幕加权），在爬取阶段为候选磁力链接动态计算权值。默认选取最高评分链路作为实体首选关联数据，同时持久化存储完整候选集以支持降级与人工复核。

* **链路可用性校验（Magnet Health Checker）**：
  * **核心对象/组件/路由（`magnet_checker.py`）**：通过并发发起 HTTP/UDP 请求连接 BT Tracker，嗅探目标磁力的 Seeders（做种者）与 Leechers（下载者）节点分布。分析结果划分为活跃（Active）、衰退（Weak）与失效（Dead）三个阈值级别，并回调更新资源权重评分。支持实体级、集合级与全局级别的探测范围控制，以及针对失败任务的重试补偿机制。

* **链路自动降级与切换（Auto-Selection Mechanism）**：
  * **核心对象/组件/路由（`/api/v1/storage/magnets/auto-select`）**：基于可用性校验返回的状态回调，系统支持批量计算并应用候选磁力的优先级变更。有效与衰退状态链路的基准评分维持不变，失效或不可达的链路将被执行惩罚扣分（基础评分递减），以此实现资源的自动化汰换与首选链路重置。

* **Tracker 路由配置（Tracker Configuration）**：
  * **核心对象/组件/路由（`settings_repo.py` / `routers/settings.py`）**：在全局配置模块中开放自定义 Tracker 地址簿的注入接口。系统在发起网络探测前，会动态聚合链路内嵌（dn 字段）Tracker、用户注入的自定义 Tracker 列表以及底层封装的默认公共 Tracker，以提高嗅探机制的可用率与网络覆盖度。

* **特征标签解析引擎（Tag Extraction & Filtering）**：
  * **核心对象/组件/路由（`web/src/composables/useMovieTags.ts`）**：在 HTML 解析阶段提取实体的业务标签（类别、特征等）并持久化至关联表。前端层级实现了基于交、并、差集的复杂布尔查询逻辑，可作用于常规集合视图与排行榜视图。支持在过滤结果集上原子级触发数据序列化与导出任务。

* **自适应控制面板与设计系统（Adaptive WebUI & Design System）**：
  * **核心对象/组件/路由（`spider_core/web/src` / `Tailwind CSS` / `Vue 3`）**：基于 Vue 3 + Vite 构建的现代化响应式前端控制台。

* **流量混淆与防护穿透（WAF Bypass）**：
  * **核心对象/组件/路由（`curl_cffi` / `spider_engine.py`）**：底层 HTTP Client 集成 curl_cffi 库。通过复刻主流浏览器的 TLS Client Hello 指纹与 JA3 特征，规避并稳定穿透基于四层与七层协议分析的 Web 应用防火墙（如 Cloudflare），提供更稳健的网络连通性。

* **应用层安全策略（Application Security）**：
  * **核心对象/组件/路由（`main.py` / `JAVDB_AUTH_TOKEN`）**：在后端路由中间件引入基于环境变量令牌（Token）的 Bearer 鉴权拦截。在涉及文件系统交互的操作（如导出路径构造）中，严格限制基准工作目录的读写边界，从而防御针对特定平台（PC/Docker）的目录穿越（Path Traversal）攻击漏洞。

* **局域网代理服务（LAN Access）**：
  * **核心对象/组件/路由（`SpiderService.java` / `0.0.0.0:8000`）**：Android 平台的后台守护进程支持将内嵌的 FastAPI 监听端口强制绑定至本地局域网接口，允许同网段（Wi-Fi）内的其他终端（桌面设备、平板终端）发起基于 HTTP 协议的控制信令，实现无头（Headless）化终端调度管理。

---

## 部署与初始化环境

### 方案一：Android 端部署与运行
1. **分发包安装**：自 [Releases] 渠道获取最新构建的 APK 产物并完成客户端安装。
2. **三阶段运行流程**：
   * **阶段 1：人机质询接管**：触发 `1. 手动登录过盾`。调用系统 `WebViewBridge` 容器发起鉴权会话，在完成 CF 防护校验与用户认证后，导航至目标采集入口并提取 URI 路由。关闭 WebView 容器时，后台进程将自动捕获并持久化当前会话的 Cookie 池。
   * **阶段 2：引擎守护进程拉起**：触发 `2. 启动爬虫引擎`。申请设备通知（Notification）及悬浮窗（System Alert Window）权限，用以维持后台 `SpiderService` 实例的常驻状态。
   * **阶段 3：WebUI 控制台挂载**：触发 `3. 打开 WebUI`。通过前端浏览器加载本地闭环监听端口 `127.0.0.1:8000`，发起路由请求并将采集指令下发至后端引擎。

### 方案二：Docker 容器化部署
推荐使用 Docker Compose 以一键启动核心引擎与独立的 Auth Browser 鉴权浏览器服务：
```bash
# 获取 docker-compose.yml 与 .env.example
cp .env.example .env
# 在 .env 中配置 JAVDB_AUTH_TOKEN 等变量
docker-compose up -d
```
*监听网关：`http://NAS_IP:8090`*

若不使用 Docker Compose，也可通过 `docker run` 手动运行并连接两个容器：
```bash
# 1. 创建内部通信网络
docker network create javdb-network

# 2. 启动 Auth Browser 鉴权服务 (开启 VNC 模式)
docker run -d \
  --name=javdb-auth-browser \
  --network=javdb-network \
  -e AUTH_BROWSER_VNC=1 \
  -v auth_browser_profile:/app/profile \
  --restart=unless-stopped \
  ghcr.io/siveci/javdb_auth_browser:latest

# 3. 启动爬虫核心引擎（链接到 Auth Browser 的 API 与 VNC 服务）
docker run -d \
  --name=javdb-spider \
  --network=javdb-network \
  -p 8090:8000 \
  -e JAVDB_AUTH_TOKEN=注入访问鉴权令牌 \
  -e AUTH_BROWSER_SERVICE_URL=http://javdb-auth-browser:8090 \
  -e AUTH_BROWSER_VIEWER_INTERNAL_URL=http://javdb-auth-browser:6080 \
  -v /你的路径/appdata/javdb_spider/data:/app/data \
  --restart=unless-stopped \
  ghcr.io/siveci/javdb_spider:latest
```
*监听网关：`http://NAS_IP:8090`*

*(注：如果不需要 Auth Browser，可省略前两步及网络相关参数，单体启动 javdb-spider。)*

**部署参数解析：**
* **API 鉴权注入**：Docker 镜像默认激活后端接口的强制访问鉴权拦截。需通过 `-e JAVDB_AUTH_TOKEN` 环境变量注入认证令牌（Token），前端控制面板发起初始数据请求时将校验该凭证。
* **存储卷挂载持久化**：需映射宿主机目录至容器内部路径 `/app/data`，确保底层核心 SQLite 实例（`spider_data.db`）与运行时配置文件的持久化存储与容器生命周期完全解耦。

### 方案三：PC 端原生 Python 运行时部署
```bash
# 1. 初始化依赖环境
pip install -r requirements.txt

# 2. 挂载至核心工作区并启动服务进程
# (可选) 注入 JAVDB_AUTH_TOKEN 环境变量以激活 API 中间件拦截保护机制
cd spider_core
python -m uvicorn main:app --host 0.0.0.0 --port 8000 --no-access-log
```

*注：推荐在非故障诊断模式下开启 `--no-access-log` 以过滤静态资源及心跳产生的状态日志，降低标准输出（STDOUT）系统开销。*

### 前端开发与构建（Vue3 + Vite）

```bash
cd spider_core/web

# 开发模式（热更新）
npm install
npm run dev      # Vite dev server，代理 /api/ 到 127.0.0.1:8000

# 生产构建（输出到 spider_core/frontend_dist/，由 FastAPI 托管）
npm run build    # vue-tsc 类型检查 + vite build
```

> 注意：为了保持仓库纯净，构建产物 `frontend_dist/` 已不再入库。对于 PC 源码部署，首次运行前需执行 `npm run build` 生成前端静态资源。
> 对于 Docker 与 Android 产物，CI/CD 流水线（`.github/workflows/`）会在构建时自动执行前端编译打包环节，您可以直接下载发行版使用。

---

## 核心操作与链路调用指南

### 1. 采集链路参数配置
* **入口 URI 定位**：在目标前端视图引擎筛选业务标签（如“单体”、“高清”），提取浏览器地址栏中的完整 URI 参数表作为引擎基础抓取路由（支持由 WebUI 基于本地资源库渲染出动态过滤检索条件）。
  * **Android 运行环境**：强制要求在第一阶段调用的系统内置 WebView 容器中捕获目标 URI，并在后端直连的 WebUI 实例中完成提交。严禁将链接跨越外部第三方浏览器应用中转，以此规避安全会话标识（Cookie）校验失效引发的连接阻断风险。
* **请求头注入（Cookie & User-Agent）**：
  * **Android 运行环境**：系统引擎底层自动挂钩拦截并同步当前 WebView 的活动会话池。
  * **PC / Docker 运行环境**：在 WebUI 设置页或任务面板点击 **“打开登录页获取 Cookie”**，系统将弹出Auth Browser 授权窗口，登录后自动捕获并同步 Cookie 与 User-Agent（同时仍保留手动粘贴输入框作为兜底）。
* **前端会话控制**：由 Web 控制台注入或 Auth Browser 获取的 Cookie 凭据默认映射于前端内存临时生命周期。执行会话保持参数勾选后，当前凭据将经过序列化流程后写入本地数据库及浏览器沙盒缓存以实现持久化。

### 2. 链路探测与资源降级演练
伴随着数据采集批次的终结，用户态系统可通过内置的协议嗅探器校验磁力关联资源的存活态势：

#### 探测作用域路由控制
* **实体级嗅探（Entity Level）**：在当前数据实体容器下展开候选磁力堆栈，依赖单实体探测层 API (`/api/v1/magnets/check`) 触发 P2P 健康校验。
* **集合级嗅探（Collection Level）**：针对指定的集合模型执行子节点遍历，将关联列表投递至探测队列以执行高并发监测。
* **全局域嗅探（Global Level）**：借助 DAO 层（`db_store.py`）针对全量库候选磁力数据集进行扫描匹配并抛送至探测器。
* **差量容错处理（Delta Retry）**：系统支持将上一轮标记状态为 `失败`（Error）的非稳态链接集合作为脏节点剥离，重新调度并补发探测请求。

#### 协议状态量化定义与调度回调
* 🟢 **活跃节点（Active）**：系统拦截到做种终端（Seeders）反馈状态，资源宣告为高优可用。
* 🟡 **衰退节点（Weak）**：当前未命中做种响应，但观测到其他下行请求（Leechers）网络广播，可用态势降级为受限。
* 🔴 **失效节点（Dead）**：P2P 寻址广播未收到来自做种端与下载端的双重反馈，宣告资源无法连通。
* ⚪ **挂起状态（Pending）**：相关子模块未下发调度事件前的默认常驻状态。
* ❌ **异常状态（Error）**：探测底层引发的网络超时层或解析协议层未处理异常（Exception）。

#### 资源优先级自动清洗算法
依靠数据控制层入口（面板 ★ 图标按钮）触发自动降级回调，链路管理服务层（`magnet_service`）将针对嗅探分析报告执行链路优先级权重的批处理洗牌操作：
* **稳态链路（Active / Weak）**：锁定资源评分阈值，承接原始启发算法授予的评分分配不作削减。
* **离线链路（Dead / Error）**：系统注入全局强惩罚因子（基础评分骤降机制，默认执行 -200 扣分）。依赖评分衰减排序策略实现链路强制落选，并自动将目标对象重新锚定为次优级可用节点资源。

#### 复合型 Tracker 请求源融合管理
配置映射中心（Config DAO）提供面向外部数据源的 Tracker 注入配置点。当嗅探模块（`magnet_checker.py`）构造测试请求包时，底层自动实现三源网络地址的动态聚合：
1. 提取磁力链路（URI）自身的内联数据流（dn 与 tr 键值）。
2. 装载持久化实例（`settings_repo.py`）维护的用户显式注入的数据字典。
3. 加入框架内预设埋点的公共网关保底链路组。

### 3. 移动端进程防杀与保活模型 (Android)
为了对抗 Android 系统针对底层 `SpiderService` 脱机服务的资源深度回收调度策略（OOM-Killer & Doze-Mode），需要实施平台特权级干预：
* **抢占系统级弹窗权限（System Alert Window）**：申请悬浮窗口（Overlay）权限作为挂载系统视图层的前置门槛，此配置极大拉高目标应用在 Activity Manager Service (AMS) 内存评级机制内的优先级评分。
* **电池白名单隔离（Battery Optimization Bypass）**：进入设备底层设定 -> 应用程序详情 -> 电量管控视图区域，调整耗电控制准则为完全无限制状态（Unrestricted），拦截待机睡眠模式对网络及 I/O 请求的强行中断。
* **强制内存锁定挂载（Task Lock）**：在系统级任务总控（Recents Tasks View）列表操作面板对宿主包进行硬锁定标记，阻止物理内存在资源紧张时的强制淘汰机制。

---

## 核心架构图谱

```text
├── auth_browser/               # Auth Browser Service (Headless Browser for Login/Cookie Capture)
├── app/                        # Android Client (Java Native / UI / WebKit)
│   ├── src/
│   │   ├── main/
│   │   │   ├── AndroidManifest.xml     # Android App Manifest
│   │   │   ├── java/com/javdb_spider/app/
│   │   │   │   ├── MainActivity.java   # Android App Entrypoint & Permissions
│   │   │   │   ├── SpiderService.java  # Foreground Daemon Service for Chaquopy
│   │   │   │   └── WebViewBridge.java  # WebView Hook for CF WAF Bypass & Cookie Sync
│   │   │   └── res/                    # Android Resources (Layouts, Icons, Strings)
│   │   ├── androidTest/               # Android Instrumentation Tests
│   │   └── test/                       # Android Unit Tests
├── spider_core/                # Backend Core Engine (FastAPI Runtime)
│   ├── main.py                 # ASGI Entrypoint & Middleware (CORS / Auth Interceptor)
│   ├── app_config.py           # Centralized Application Configuration & Constants
│   ├── dependencies.py         # FastAPI Dependency Injection & Request Scoping
│   ├── schemas.py              # Pydantic Data Models & Type Definitions
│   ├── utils.py                # Core Utilities & Helper Functions
│   ├── spider_engine.py        # Task Orchestration & Concurrent Scraping Engine
│   ├── ranking_utils.py        # HTML/JSON Parsing & Ranking Data Binding
│   ├── magnet_checker.py       # P2P Tracker Probing Module (UDP/HTTP Client)
│   ├── movie_repo.py           # Movie Entity DAO
│   ├── task_repo.py            # Task Queue & State Machine DAO
│   ├── settings_repo.py        # Config Serialization & Caching DAO
│   ├── db_store.py             # SQLite Connection Pool & Transaction Manager
│   ├── export_service.py       # Entity-to-CSV Serialization Service
│   ├── storage_utils.py        # Storage Path & Security Utilities
│   ├── routers/                # API Routing Layer
│   │   ├── auth_browser.py     # Auth Browser Service Interaction Routes
│   │   ├── magnets.py          # Magnet Health & Priority Routes
│   │   ├── movies.py           # Movie Entity & Tag Filtering Routes
│   │   ├── rankings.py         # Ranking & Top250 Routes
│   │   ├── settings.py         # System Configuration Routes
│   │   ├── storage.py          # Storage Management & CSV Export Routes
│   │   └── tasks.py            # Spider Task Queue Control Routes
│   ├── services/               # Business Logic Layer
│   │   ├── auth_browser_service.py # Auth Browser Client Integration
│   │   ├── cookie_validation_service.py # Cookie Health Check & Validation
│   │   ├── magnet_service.py   # Magnet Selection & Scoring Algorithms
│   │   ├── queue_service.py    # Background Queue & Thread Management
│   │   └── task_service.py     # Task Preparation & Payload Serialization
│   ├── web/                    # Vue3+Vite 前端源码（重写后）
│   │   ├── src/                # Vue SFC 源码
│   │   │   ├── views/          # 页面级组件（Tasks/Database/Actors/Settings）
│   │   │   ├── stores/         # Pinia 状态（auth/tasks/settings/actors/database）
│   │   │   ├── composables/    # 复用逻辑（useToast/useTheme/useClipboard）
│   │   │   ├── api/            # apiFetch 封装
│   │   │   ├── router/         # Vue Router（hash 模式）
│   │   │   └── types/          # 对照后端 schemas.py 的 TS 类型
│   │   ├── package.json
│   │   └── vite.config.ts      # build.outDir → ../frontend_dist
│   ├── frontend_dist/          # Vite 构建产物（在 CI/CD 或本地手动构建，不再入库）
│   │   ├── index.html          # 单页入口（FastAPI 托管）
│   │   └── assets/             # JS/CSS（FastAPI 挂载 /assets）
│   ├── tests/                  # Python Unit Test Suite
│   │   ├── test_api_endpoints.py
│   │   ├── test_magnet_checker.py
│   │   ├── test_movie_tags.py
│   │   ├── test_rankings.py
│   │   ├── test_spider_engine.py
│   │   ├── test_v13_security.py
│   │   ├── test_v14_db_store.py
│   │   ├── test_v15_tasks.py
│   │   └── test_v16_queue_runtime.py
│   └── data/                   # Local Persistent Data Volume
│       ├── spider_data.db      # SQLite Database File
│       ├── checkpoint.json     # Scraping State Snapshot
│       └── status.json         # Runtime Status Metrics
├── docker-compose.yml          # Container Orchestration with Auth Browser
├── Dockerfile                  # OCI Container Build Script
├── build.gradle                # Root Gradle Build Configuration
├── gradle/                     # Gradle Wrapper & Version Catalog
│   └── libs.versions.toml      # Dependency Version Catalog
└── requirements.txt            # Python Package Dependencies
```

---

## 系统边界约束与依赖项

| 部署基线 | 兼容性边界范围 |
| :--- | :--- |
| **Android 容器层** | 系统基础级 API Level 26+ (Android 8.0+，核心推荐 API 30+)；强制匹配 `arm64-v8a` 处理器指令集。 |
| **OCI 容器体系 (Docker)** | 原生构建镜像文件兼容 `linux/amd64` 以及基于 ARM 架构的 `linux/arm64` 执行端。 |
| **Python 后端运行时** | 运行时依赖版本要求 `>= 3.12.x`（以适配后端对高阶类型提示泛型化特性支持及核心模块库解析环境）。 |

---

## 合规限制与免责声告

1. **研发边界控制限制**：此软件仓库所涉项目的唯一确立主旨，在于论证 Python 数据层抓取及 Chaquopy Android 原生环境混合桥接的深层架构开发体系。开发者严禁在违反所在地网络合规政策的前提下，将其用于具备主观恶意的违法性信息探测业务节点上。
2. **抓取并发与链路阻断控制**：发起集群式并发数据挖掘等行为时，责任主体应当基于目标源服务器挂载的 `robots.txt` 控制指引执行轮询缓冲设置。须规避以高强度连接池风暴触发对服务端点的分布式拒绝服务式（DoS）冲刷瘫痪。
3. **零隐私探针（Telemetry-Free）审计宣告**：系统整体架构建立于纯粹的本地脱机离线运转沙箱内，不留存或内嵌外部状态回送及数据上报钩子。凡关涉授权凭据层（Cookie）与平台拦截特征码的处理，均在应用客户端（Client）单向对目标集群点对点广播传递。工程维护者不对越权利用及 Cookie Leakage 问题提供关联担保。
