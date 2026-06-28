#!/usr/bin/env bash
# Auth Browser Service 启动脚本。
#
# AUTH_BROWSER_VNC 开启时，先拉起虚拟显示 + VNC + noVNC 投屏链路，再启动 API；
# 关闭时仅启动 API（保留桌面机/源码运行的原有行为）。
set -euo pipefail

VNC_ENABLED="$(printf '%s' "${AUTH_BROWSER_VNC:-}" | tr '[:upper:]' '[:lower:]')"
DISPLAY_NUM="${AUTH_BROWSER_DISPLAY:-:99}"
NOVNC_PORT="${AUTH_BROWSER_NOVNC_PORT:-6080}"
SCREEN_GEOMETRY="${AUTH_BROWSER_SCREEN_GEOMETRY:-1280x800x24}"
NOVNC_WEB_DIR="${AUTH_BROWSER_NOVNC_WEB_DIR:-/usr/share/novnc}"

if [ "$VNC_ENABLED" = "1" ] || [ "$VNC_ENABLED" = "true" ] || [ "$VNC_ENABLED" = "yes" ] || [ "$VNC_ENABLED" = "on" ]; then
  echo "[start] VNC viewer 链路已启用 (display=$DISPLAY_NUM, novnc_port=$NOVNC_PORT)"

  # 虚拟显示：Chromium 有头模式渲染到此。
  Xvfb "$DISPLAY_NUM" -screen 0 "$SCREEN_GEOMETRY" -nolisten tcp &
  export DISPLAY="$DISPLAY_NUM"

  # 等待 Xvfb 就绪。
  for _ in $(seq 1 50); do
    if xdpyinfo -display "$DISPLAY_NUM" >/dev/null 2>&1; then break; fi
    sleep 0.1
  done

  # 把虚拟显示导出为 VNC（仅监听本地回环，由 websockify 桥接对外）。
  # -nopw：不设 VNC 密码，访问控制依赖主程序反代所在的内网隔离。
  x11vnc -display "$DISPLAY_NUM" -forever -shared -nopw -localhost -rfbport 5900 -quiet &

  # noVNC：托管静态页并把 VNC 流桥接为 WebSocket。
  websockify --web "$NOVNC_WEB_DIR" "$NOVNC_PORT" localhost:5900 &
fi

# Playwright 在 VNC 模式下需要 DISPLAY；main.py 会据 AUTH_BROWSER_VNC 决定有头/无头。
exec python -m uvicorn main:app --host 0.0.0.0 --port 8090
