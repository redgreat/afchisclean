FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=Asia/Shanghai

RUN apt-get update \
    && apt-get install -y --no-install-recommends gosu \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r appuser && useradd -r -g appuser appuser

WORKDIR /app

ENV PYTHONPATH=/app:/app/src

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# 镜像内保留配置模板（宿主机 config.yml 不打包进镜像，见 .dockerignore）
COPY conf/config.yml.template /app/etc/config.yml.template

RUN mkdir -p /app/log /app/conf /app/etc \
    && chmod +x /app/scripts/docker-entrypoint.sh \
    && chown -R appuser:appuser /app

# entrypoint 以 root 修正挂载卷权限后，再降权为 appuser
ENTRYPOINT ["/app/scripts/docker-entrypoint.sh"]
CMD ["python", "src/job_scheduler.py"]