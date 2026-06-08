<div align="center">
  <img src="spider_core/frontend/logo.png" alt="JavDB Magnet Spider Logo" width="180">
  <h1>JavDB Magnet Spider</h1>
  <p>
    <img src="https://img.shields.io/badge/Platform-Android%20%7C%20Docker%20%7C%20PC-brightgreen" alt="Platform">
    <img src="https://img.shields.io/badge/Python-3.12-blue" alt="Python Version">
    <img src="https://img.shields.io/badge/Framework-FastAPI%20%7C%20Chaquopy-orange" alt="Framework">
    <img src="https://img.shields.io/badge/License-MIT-lightgrey" alt="License">
  </p>
</div>

---


## 项目简介

跨平台的自动化JavDB爬虫工具，支持在Docker、PC、Android 移动端运行。本项目实现了在手机上**手动过盾、后台静默爬取、WebUI 远程控制**的闭环体验，能批量抓取指定目录。

## 核心特性

- **多任务队列**：支持批量下发爬取任务，系统自动进行后台排队与调度。Web 控制台提供实时进度监控，并支持对运行任务进行安全暂停、恢复与取消。
- **Android支持**：基于 Chaquopy 框架在手机端运行完整后端。集成原生 WebView 机制进行验证拦截与 Cookie 自动同步，支持通过端侧无缝接管并恢复需要身份验证的任务。
- **智能预检与断点续传**：任务启动前自动执行页面预检与本地数据冲突检测；爬取过程提供精准的断点记录与救援机制，确保增量去重与覆盖操作的数据一致性。
- **数据库化管理**：底层采用 SQLite 实现可靠的数据持久化。内置可视化数据管理中心，支持浏览抓取集合、查看影片的全部候选磁力，并允许手动切换单部影片的最终生效链接，动态关联 CSV 导出与一键复制功能。
- **磁力优选策略**：内置智能权重算法（无码+100 > 高清+10 > 字幕+1），自动为每部影片筛选最佳磁力资源，同时透明保留所有候选磁力记录以便进行二次筛查调优。
- **磁力健康检测**：通过 BT Tracker 实时检测磁力链接的可用性，获取 seeders/leechers 数量，支持 HTTP/UDP 协议。检测结果分为有效（active）、弱（weak）、无效（dead）三级，并自动影响磁力优选评分。支持单部影片、集合、全局三种检测范围，以及仅重新检测失败项的选项。
- **智能自动选择**：根据检测结果和评分算法，一键为选定集合或全部集合自动选择最佳可用磁力。检测结果会动态调整磁力优先级评分，确保优先选择可用资源。
- **Tracker 配置**：支持自定义 Tracker 列表以提高检测覆盖率。可在全局配置中添加自定义 Tracker 地址，系统会自动合并磁力链接内置的 Tracker 和默认公共 Tracker。
- **标签解析与动态过滤**：爬取时自动提取并保存每部影片的“类别/标签”信息。WebUI 支持基于标签对抓取结果进行多条件组合筛选，实现按需定制 CSV 导出与一键复制。
- **过盾机制**：采用 `curl_cffi` 模拟 TLS 指纹，稳定穿透 Cloudflare 等网络防护。
- **系统安全**：严格规范文件路径校验以防止目录穿透，在 PC 及 Docker 端默认启用 `JAVDB_AUTH_TOKEN` 接口鉴权保护。

---

## 快速开始

### 方案一：Android 手机端
1. **下载安装**：前往 [Releases] 下载最新的 APK 文件。
2. **三步走启动**：
   - **第一步**：点击 `1. 手动登录过盾`。在弹出的网页中完成 JavDB 登录并过掉 CF 验证，找到需要爬取的目标页面后点击 **「复制当前链接」**，最后点击关闭（Cookie 将自动接管）。
   - **第二步**：点击 `2. 启动爬虫引擎`。按提示授予**通知权限**与**悬浮窗权限**。
   - **第三步**：点击 `3. 打开 WebUI`。系统将跳转至浏览器访问 `127.0.0.1:8000`，即可开始配置爬取任务。

### 方案二：Docker 部署
```bash
docker run -d \
  --name=javdb-spider \
  -p 8090:8000 \
  -e JAVDB_AUTH_TOKEN=请替换为你的访问令牌 \
  -v /你的路径/appdata/javdb_spider/data:/app/data \
  --restart=unless-stopped \
  ghcr.io/你的用户名/javdb_spider:latest
```
*访问地址：`http://NAS_IP:8090`*

**部署说明：**
- **鉴权保护**：Docker 镜像默认启用 API 访问保护，请务必设置 `JAVDB_AUTH_TOKEN`。WebUI 首次访问时会提示输入此令牌进行验证。
- **数据持久化**：挂载 `/app/data` 目录后，系统运行所需的 SQLite 数据库（`spider_data.db`）将与数据一同安全持久化。

### 方案三：本地 Python 运行
```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 进入核心目录并启动
# (可选) 设置 JAVDB_AUTH_TOKEN 环境变量启用 API 鉴权保护
cd spider_core
python -m uvicorn main:app --host 0.0.0.0 --port 8000 --no-access-log
```

`--no-access-log` 会保留启动与错误日志，但不打印高频轮询接口的 `GET /api/... 200 OK`；排查请求时可临时去掉。

---

## 使用指南

### 1. 爬取逻辑配置
- **起始页面 URL**：进入老师主页，筛选好想要抓取的 Tag（如“单体”、“高清”），复制浏览器地址栏链接（支持在WebUI中动态获取过滤标签）。
  - **Android 端**：建议直接在“手动登录过盾”的内置浏览器中浏览，并点击「复制当前链接」，然后直接在 WebUI 中粘贴，全程无需切换外部浏览器以防 Cookie 失效。
- **Cookie & UA**：
  - **Android 端**：自动接管，无需手动填写（注意不要留空，随便填，避免触发前台检测）。
  - **PC/Docker 端**：按 `F5` 刷新页面，在开发者工具 `Network` 选项卡的第一个请求头中获取。
- **WebUI 安全与状态**：前端页面输入的 Cookie 默认仅在当前会话中生效，勾选“记住 Cookie”后才会写入浏览器 `localStorage` 进行长期保存。

### 2. 磁力检测与优选
爬取完成后，可以对磁力链接进行健康检测，确保选择的磁力资源可用：

#### 检测操作
- **单部影片检测**：展开影片的候选磁力列表，点击检测按钮（check）检测该影片的所有候选磁力
- **集合检测**：在集合列表中点击检测按钮，检测该集合下所有影片的候选磁力
- **全局检测**：在数据库管理区域点击全局检测按钮，检测所有集合的候选磁力
- **仅检测失败项**：点击检测按钮旁的下拉箭头，选择"check failed"仅重新检测之前检测失败的磁力

#### 检测状态说明
- 🟢 **有效（active）**：有 seeders，资源可用
- 🟡 **弱（weak）**：无 seeders 但有 leechers，资源可能不稳定
- 🔴 **无效（dead）**：无 seeders 和 leechers，资源不可用
- ⚪ **未检测**：尚未进行检测
- ❌ **检测失败**：检测过程中出现错误

#### 自动选择磁力
点击数据库管理区域的 ★ 按钮，系统会根据检测结果和评分算法，自动为选定集合或全部集合选择最佳可用磁力。检测结果会动态调整磁力优先级评分：
- 有效/弱状态的磁力保持原始评分
- 无效/检测失败的磁力评分降低 200 分

#### Tracker 配置
在全局配置的"Tracker 列表"中，可以添加自定义 Tracker 地址（一行一个），系统会自动合并：
1. 磁力链接内置的 Tracker
2. 用户自定义的 Tracker
3. 默认公共 Tracker

### 3. 电池优化 (Android)
为了防止安卓系统在息屏后杀掉爬虫进程，请进行以下设置：
- **开启悬浮窗权限**（核心要求）。
- 在"系统设置" -> "应用管理"中，将本 App 的电池策略设为**无限制**。
- 在多任务界面为 App **加锁**。

---

## 项目结构
```text
├── app/                # Android 原生 Java 代码与资源
├── spider_core/        # 核心 Python 逻辑 (三端共享)
│   ├── frontend/       # WebUI 静态资源
│   ├── main.py         # FastAPI 路由入口
│   ├── spider_engine.py # 爬虫核心引擎
│   ├── magnet_checker.py # 磁力链接健康检测模块
│   └── db_store.py     # 数据库操作与存储层
├── Dockerfile          # Docker 构建脚本
├── build.gradle        # 安卓构建配置
└── requirements.txt    # Python 依赖清单
```

---

## 系统要求

| 维度 | 要求 |
| :--- | :--- |
| **Android** | Android 8.0+ (推荐 11+), arm64-v8a 架构 |
| **Docker** | 支持 linux/amd64, linux/arm64 (支持树莓派) |
| **Python** | 推荐 3.12.x |

---

## 免责声明
1. **仅供学习**：本项目仅用于 Python 爬虫技术研究与 Chaquopy 框架实践，请勿用于非法用途。
2. **合规使用**：请遵守目标网站的 `robots.txt` 协议，合理控制爬取频率。
3. **隐私安全**：本项目为开源软件，不收集任何用户 Cookie 信息。用户需妥善保管个人凭据。
