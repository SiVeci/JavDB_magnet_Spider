# Auth Browser Service MVP

这是独立授权浏览器服务。主程序只通过 HTTP API 调用它，不关心它运行在源码进程、Docker、NAS、桌面机还是远程机器。

## API

- `POST /sessions/start`：启动授权浏览器并在该浏览器内打开 `https://javdb.com/login`
- `GET /health`：检查服务、鉴权、活跃会话和 profile 状态
- `GET /sessions/{session_id}/status`：查询会话状态
- `POST /sessions/{session_id}/capture`：读取当前 JavDB Cookie 和 User-Agent
- `POST /sessions/{session_id}/close`：关闭浏览器会话

如果配置了 `AUTH_BROWSER_SHARED_TOKEN`，请求需要带 `X-Auth-Browser-Token`。

## 源码运行

```bash
cd auth_browser
pip install -r requirements.txt
python -m playwright install chromium
python -m uvicorn main:app --host 127.0.0.1 --port 8090
```

主程序配置：

```bash
set AUTH_BROWSER_SERVICE_URL=http://127.0.0.1:8090
cd spider_core
python -m uvicorn main:app --host 0.0.0.0 --port 8000 --no-access-log
```

## Docker / 远程登录画面（VNC 投屏）

支持三种运行形态：

1. **桌面模式**（默认，未设 `AUTH_BROWSER_HEADLESS`/`AUTH_BROWSER_VNC`）：登录窗口由本服务在本机弹出，适合源码运行调试。
2. **Headless 模式**（`AUTH_BROWSER_HEADLESS=1`）：无头启动并捕获 Cookie，但不提供可视画面，无法人工登录。
3. **VNC 投屏模式**（`AUTH_BROWSER_VNC=1`，compose 默认）：容器内用虚拟显示（Xvfb）跑有头 Chromium，经 x11vnc + noVNC 把画面投成网页。主程序把 `/auth-viewer/*` 反代到本服务 6080 端口，其他设备访问主程序 WebUI 即可在网页里看到并操作 javdb 登录框，登录后获取 Cookie。此模式下 `viewer_url` 返回相对路径 `/auth-viewer/vnc.html?...`，由前端拼接当前页面 origin。

常用环境变量：

- `AUTH_BROWSER_VNC=1`：启用 VNC 投屏模式（容器内有头 Chromium + noVNC，端口 6080）
- `AUTH_BROWSER_HEADLESS=1`：以 Headless 模式启动 Chromium（VNC 模式会覆盖此项强制有头）
- `AUTH_BROWSER_VNC_VIEWER_PATH=...`：VNC 模式下 viewer_url 返回的 noVNC 相对入口，默认无需修改
- `AUTH_BROWSER_PUBLIC_BASE_URL=http://host:8090`：非 VNC 模式下返回给主程序和 WebUI 的远程 viewer 入口基址；未配置时 `viewer_url` 为空
- `AUTH_BROWSER_SHARED_TOKEN=...`：主程序和 Auth Browser Service 之间的共享令牌
- `AUTH_BROWSER_LOGIN_URL=https://javdb.com/login`：登录页，默认无需修改
- `AUTH_BROWSER_SESSION_TTL_SECONDS=900`：登录会话有效期
- `AUTH_BROWSER_PROFILE_DIR=./profile`：保存 Playwright storage state 的目录

安全提示：VNC 投屏模式下 `/auth-viewer` 默认无 VNC 密码，仅靠内网隔离；公网暴露前请加 VNC 密码或在反代层加鉴权。

Cookie 只通过 capture API 返回给主程序，服务日志不要输出完整 Cookie。

## Phase 3 行为

- 同一时间只允许一个活跃登录会话；如已有会话，新的 `/sessions/start` 会返回 409。
- 会话超过有效期会标记为 `expired` 并关闭浏览器资源。
- 捕获成功后保存 `storage_state.json`，下次启动会尝试复用仍有效的登录态。
- 捕获成功后关闭本次浏览器资源，登录入口不再无限复用。
- 启动失败、上下文丢失、未捕获到 Cookie 等异常会返回结构化 `message` 和 `code`。

远程或公网部署时必须配置 `AUTH_BROWSER_SHARED_TOKEN`，并且不要把 `AUTH_BROWSER_PROFILE_DIR` 放到任何公开 Web 目录。
