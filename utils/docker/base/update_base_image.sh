#!/bin/bash -e

dh_username=$1
dh_password=$2

base_name=base18
BASE_VERSION=$(date -u +%y%m%d-%H%M%S)

docker build -t compss/${base_name}:${BASE_VERSION} .

docker login -u "${dh_username}" -p "${dh_password}"
docker push compss/${base_name}:${BASE_VERSION}


docker tag compss/${base_name}:${BASE_VERSION} compss/${base_name}:latest
docker push compss/${base_name}:latest
