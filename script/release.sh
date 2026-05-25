#!/bin/bash

git tag v0.0.1
git push origin v0.0.1

cd /opt/afchisclean
docker stop afchisclean
docker rm afchisclean
docker rmi registry.cn-hangzhou.aliyuncs.com/redgreat/afchisclean
docker pull registry.cn-hangzhou.aliyuncs.com/redgreat/afchisclean:latest
docker-compose up -d
docker logs afchisclean


cd /opt/afchisclean
docker stop afchisclean
docker rm afchisclean
docker rmi registry.cn-hangzhou.aliyuncs.com/redgreat/afchisclean:main
docker pull registry.cn-hangzhou.aliyuncs.com/redgreat/afchisclean:main
docker-compose up -d
docker logs afchisclean


# 手动执行
docker exec afchisclean python src/job_scheduler.py --run-now
