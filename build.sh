#!/bin/bash
set -eo pipefail
APP_NAME=$1
ENV=$2
UPDATE_CACHE=""
echo master_deploy.sh . >>.dockerignore
echo buildenv.sh . >>.dockerignore
echo awsconfiguration.sh . >>.dockerignore
docker build --no-cache=true -t $APP_NAME:latest .
