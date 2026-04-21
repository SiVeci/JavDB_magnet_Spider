# 1. 使用 Ubuntu 22.04 作为底座，它的二进制兼容性是最好的
FROM ubuntu:22.04

# 2. 避免安装过程中的交互弹窗
ENV DEBIAN_FRONTEND=noninteractive

# 3. 安装 Python 3.12 以及 curl_cffi 必需的系统库
# 这里补全了 libnss3, libnspr4, ca-certificates 等所有“全家桶”
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.12 \
    python3-pip \
    python3.12-dev \
    ca-certificates \
    libnss3 \
    libnspr4 \
    libcurl4 \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# 4. 建立 python 指向
RUN ln -s /usr/bin/python3.12 /usr/bin/python

# 5. 设置工作目录
WORKDIR /app

# 6. 复制依赖并安装
# 注意：加了 --no-cache-dir 确保不使用错误的缓存
COPY requirements.txt .
RUN python -m pip install --upgrade pip
RUN python -m pip install --no-cache-dir "curl_cffi>=0.7.0b4"
RUN python -m pip install --no-cache-dir -r requirements.txt

# 7. 复制核心代码
COPY spider_core/ .

# 8. 预创建数据目录
RUN mkdir -p /app/data

# 9. 暴露端口
EXPOSE 8000

# 10. 启动命令
CMD ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
