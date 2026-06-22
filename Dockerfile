FROM python:3.12-slim
WORKDIR /app
ARG JAVDB_SPIDER_VERSION=dev-local
ENV PYTHONUNBUFFERED=1 \
    TZ=Asia/Shanghai \
    JAVDB_AUTH_REQUIRED=1 \
    JAVDB_SPIDER_VERSION=${JAVDB_SPIDER_VERSION}
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libnss3 \
    libcurl4 \
    curl \
    gosu \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY spider_core/ .
COPY entrypoint.sh /entrypoint.sh
RUN useradd -r -m -s /usr/sbin/nologin appuser \
    && mkdir -p /app/data \
    && chown -R appuser:appuser /app \
    && chmod +x /entrypoint.sh
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://127.0.0.1:8000/api/version || exit 1
EXPOSE 8000
# 以 root 进入 entrypoint：修正挂载目录属主后用 gosu 降权到 appuser，
# 因此这里不写 USER appuser（降权由 entrypoint 完成）。
ENTRYPOINT ["/entrypoint.sh"]
CMD ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
