#!/bin/bash
set -eo pipefail
APP_NAME=$1
ENV=$2
UPDATE_CACHE=""
echo master_deploy.sh . >>.dockerignore
echo buildenv.sh . >>.dockerignore
echo awsconfiguration.sh . >>.dockerignore
# docker-compose -f docker/docker-compose.yml build $APP_NAME
# docker create --name app $APP_NAME:latest
#sed -i s/@env@/$ENV/g docker/Dockerfile
docker build --no-cache=true -f docker/Dockerfile \
   -t $APP_NAME:latest .
