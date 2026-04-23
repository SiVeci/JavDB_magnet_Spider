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

## 🌟 项目简介

跨平台的自动化JavDB爬虫工具，支持在Docker、PC、Android 移动端运行。本项目实现了在手机上**手动过盾、后台静默爬取、WebUI 远程控制**的闭环体验，能批量抓取指定目录，并根据自定义规则（中文字幕、无码、高清等）自动挑选出质量最高的磁力链接。

## ✨ 核心特性

- **Android 原生支持**：基于 Chaquopy 框架，在手机内运行完整的 FastAPI + Uvicorn 后端。
- **Android 端**：利用原生 WebView 手动登录，Cookie 自动无缝同步。
- **PC/Docker 端**：采用 `curl_cffi` 模拟 TLS 指纹，稳定穿透防护。
- **Web 控制台**：适配移动＆桌面屏幕尺寸，支持实时日志查看与任务管理。
- **自动选择磁力**：按 `中字无码 > 无码 > 中字 > 高清` 优先级自动筛选。
- **断点续传系统**：支持任务挂起与救援，Cookie 失效后补充即可接力。
- **Docker 化部署**

---

## 🚀 快速开始

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
  -v /你的路径/appdata/javdb_spider/data:/app/data \
  --restart=unless-stopped \
  ghcr.io/你的用户名/javdb_spider:latest
```
*访问地址：`http://NAS_IP:8090`*

### 方案三：本地 Python 运行
```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 进入核心目录并启动
cd spider_core
python -m uvicorn main:app --host 0.0.0.0 --port 8000
```

---

## 🛠️ 使用指南

### 1. 爬取逻辑配置
- **起始页面 URL**：进入老师主页，筛选好想要抓取的 Tag（如“单体”、“高清”），复制浏览器地址栏链接（支持在WebUI中动态获取过滤标签）。
  - **Android 端**：建议直接在“手动登录过盾”的内置浏览器中浏览，并点击「复制当前链接」，然后直接在 WebUI 中粘贴，全程无需切换外部浏览器以防 Cookie 失效。
- **Cookie & UA**：
  - **Android 端**：自动接管，无需手动填写（注意不要留空，随便填，避免触发前台检测）。
  - **PC/Docker 端**：按 `F5` 刷新页面，在开发者工具 `Network` 选项卡的第一个请求头中获取。

### 2. 电池优化 (Android)
为了防止安卓系统在息屏后杀掉爬虫进程，请进行以下设置：
- **开启悬浮窗权限**（核心要求）。
- 在“系统设置” -> “应用管理”中，将本 App 的电池策略设为**无限制**。
- 在多任务界面为 App **加锁**。

---

## 📁 项目结构 (Monorepo)
```text
├── app/                # Android 原生 Java 代码与资源
├── spider_core/        # 核心 Python 逻辑 (三端共享)
│   ├── frontend/       # WebUI 静态资源
│   ├── main.py         # FastAPI 路由入口
│   └── spider_engine.py # 爬虫核心引擎
├── Dockerfile          # Docker 构建脚本
├── build.gradle        # 安卓构建配置
└── requirements.txt    # Python 依赖清单
```

---

## ⚙️ 系统要求

| 维度 | 要求 |
| :--- | :--- |
| **Android** | Android 8.0+ (推荐 11+), arm64-v8a 架构 |
| **Docker** | 支持 linux/amd64, linux/arm64 (支持树莓派) |
| **Python** | 推荐 3.12.x |

---

## ⚠️ 免责声明
1. **仅供学习**：本项目仅用于 Python 爬虫技术研究与 Chaquopy 框架实践，请勿用于非法用途。
2. **合规使用**：请遵守目标网站的 `robots.txt` 协议，合理控制爬取频率。
3. **隐私安全**：本项目为开源软件，不收集任何用户 Cookie 信息。用户需妥善保管个人凭据。
