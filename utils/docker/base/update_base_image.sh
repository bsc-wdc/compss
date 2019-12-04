#!/bin/bash -e

dh_username=$1
dh_password=$2


BASE_VERSION=$(date -u +%y%m%d-%H%M%S)

docker build -t compss/base:${BASE_VERSION} .

docker login -u "${dh_username}" -p "${dh_password}"
docker push compss/base:${BASE_VERSION}


docker tag compss/base:${BASE_VERSION} compss/base:latest
docker push compss/base:latest
