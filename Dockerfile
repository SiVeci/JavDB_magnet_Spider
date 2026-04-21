# 1. 继续使用 Ubuntu 24.04
FROM ubuntu:24.04

# 2. 避免安装过程中的交互弹窗
ENV DEBIAN_FRONTEND=noninteractive

# 3. 安装 Python 3 和核心系统库
# 这里补全了 python3-pip，它在 Ubuntu 24.04 中非常关键
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    ca-certificates \
    libnss3 \
    libnspr4 \
    libcurl4 \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# 4. 建立 python 快捷指向
RUN ln -s /usr/bin/python3 /usr/bin/python

# 5. 设置工作目录
WORKDIR /app

# 6. 复制依赖清单
COPY requirements.txt .

# 7. 【核心改动】直接安装依赖，跳过自升级 pip
# 我们直接使用 pip3 命令，并加上 --break-system-packages 参数
RUN pip3 install --no-cache-dir --break-system-packages "curl_cffi>=0.7.0b4"
RUN pip3 install --no-cache-dir --break-system-packages -r requirements.txt

# 8. 复制核心代码 (注意：复制 spider_core 内的内容到 /app)
COPY spider_core/ .

# 9. 预创建数据目录
RUN mkdir -p /app/data

# 10. 暴露端口
EXPOSE 8000

# 11. 启动命令
CMD ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
