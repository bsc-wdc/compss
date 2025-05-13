#!/bin/bash -e
push=$1

dh_username=$2
dh_password=$3

#base_name=base18
base_name=base22
base_images="rt bindings all tutorial ci"
platforms="linux/amd64,linux/arm64"
builder="docker-multiarch"
BASE_VERSION=$(date -u +%y%m%d-%H%M%S)
if [[ "$push" == "true" ]]; then
   docker login -u "${dh_username}" -p "${dh_password}"
   flag="--push"
else
   flag=""
fi

for img in $base_images; do
   docker buildx build --builder ${builder} --target base_${img} --platform ${platforms} ${flag} -t compss/${base_name}_${img}:${BASE_VERSION} -t compss/${base_name}_${img}:latest .
   docker push compss/${base_name}_${img}:${BASE_VERSION}

   docker tag compss/${base_name}_${img}:${BASE_VERSION} compss/${base_name}_${img}:latest
   docker push compss/${base_name}_${img}:latest
done
