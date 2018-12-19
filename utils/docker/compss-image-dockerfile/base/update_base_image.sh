#!/bin/bash -e

dh_username=$1
dh_password=$2

docker build -t compss/base:latest .

docker login -u ${dh_username} -p ${dh_password}
docker push compss/base:latest
