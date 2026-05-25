#!/bin/sh
# 修复 bind mount 后 conf/log 目录权限，再以 appuser 启动应用
set -e

fix_dir() {
    dir="$1"
    if [ -d "$dir" ]; then
        chown -R appuser:appuser "$dir" 2>/dev/null || true
        chmod -R u+rwX,g+rwX,o+rX "$dir" 2>/dev/null || true
        find "$dir" -type f -exec chmod 644 {} \; 2>/dev/null || true
    fi
}

mkdir -p /app/conf /app/log
fix_dir /app/conf
fix_dir /app/log
chown -R appuser:appuser /app/log 2>/dev/null || true

exec gosu appuser "$@"
