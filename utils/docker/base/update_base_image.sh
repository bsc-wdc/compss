#!/bin/bash -e

dh_username=$1
dh_password=$2

#base_name=base18
base_name=base22
base_images="rt bindings all tutorial ci"
BASE_VERSION=$(date -u +%y%m%d-%H%M%S)
#docker login -u "${dh_username}" -p "${dh_password}"
for img in $base_images; do
   docker build --target base_${img} -t compss/${base_name}_${img}:${BASE_VERSION} .
   docker push compss/${base_name}_${img}:${BASE_VERSION}

   docker tag compss/${base_name}_${img}:${BASE_VERSION} compss/${base_name}_${img}:latest
   docker push compss/${base_name}_${img}:latest
done
