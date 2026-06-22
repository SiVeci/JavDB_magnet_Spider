#!/bin/sh
set -e

# 容器以 root 启动本脚本：先把挂载进来的 /app/data 归属修正给 appuser，
# 解决 bind mount 后宿主目录属主覆盖镜像 chown、导致非 root 进程无法写库
# （SQLite WAL 需在该目录创建 .db-wal/.db-shm）的问题。
# 然后用 gosu 降权到 appuser 运行真正的服务进程，保留非 root 运行的安全性。
chown -R appuser:appuser /app/data 2>/dev/null || true

exec gosu appuser "$@"
