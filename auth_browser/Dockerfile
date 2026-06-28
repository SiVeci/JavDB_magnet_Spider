FROM mcr.microsoft.com/playwright/python:v1.60.0-noble

WORKDIR /app
ENV PYTHONUNBUFFERED=1

# VNC 投屏链路所需组件：
#   xvfb       —— 无头宿主上的虚拟 X 显示（Chromium 有头模式渲染到此）
#   x11vnc     —— 把虚拟显示导出为 VNC
#   novnc      —— 浏览器端 VNC 客户端（静态页 vnc.html）
#   websockify —— 将 VNC TCP 流桥接为 WebSocket，并托管 noVNC 静态资源
RUN apt-get update && apt-get install -y --no-install-recommends \
    xvfb \
    x11vnc \
    x11-utils \
    novnc \
    websockify \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py .
COPY start.sh /app/start.sh
RUN chmod +x /app/start.sh

EXPOSE 8090 6080
CMD ["/app/start.sh"]
