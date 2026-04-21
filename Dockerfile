# 1. 使用 Ubuntu 24.04，它原生自带 Python 3.12
FROM ubuntu:24.04

# 2. 避免安装过程中的交互弹窗
ENV DEBIAN_FRONTEND=noninteractive

# 3. 安装 Python 3.12 及系统库
# Ubuntu 24.04 下包名就是 python3 (即 3.12)
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    ca-certificates \
    libnss3 \
    libnspr4 \
    libcurl4 \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# 4. 建立 python 指向
RUN ln -s /usr/bin/python3 /usr/bin/python

# 5. 设置工作目录
WORKDIR /app

# 6. 复制依赖并安装
COPY requirements.txt .

# 💡 注意：Ubuntu 24.04 引入了 PEP 668，需要加 --break-system-packages 才能在容器内全局安装
RUN python -m pip install --upgrade pip --break-system-packages
RUN python -m pip install --no-cache-dir "curl_cffi>=0.7.0b4" --break-system-packages
RUN python -m pip install --no-cache-dir -r requirements.txt --break-system-packages

# 7. 复制核心代码
COPY spider_core/ .

# 8. 预创建数据目录
RUN mkdir -p /app/data

# 9. 暴露端口
EXPOSE 8000

# 10. 启动命令
CMD ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
