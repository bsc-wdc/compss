#!/bin/bash -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
version=$1
push=$2
dh_username=$3
dh_password=$4
default_images="minimal pycompss compss compss-tutorial"
platforms="linux/amd64,linux/arm64"
builder="docker-multiarch"
# move to root folder
cd ${SCRIPT_DIR}/../..

echo "Version: $version"
echo "Push: $push"

if [[ "$push" == "true" ]]; then
   docker login -u "${dh_username}" -p "${dh_password}"
   flag="--push"
else
   flag=""
fi
echo "Building images ..."
first=true
for img in $default_images; do
   echo "Building image ${img}"
   if $first; then
	  #docker build --no-cache --target ${img} -t compss/${img}:${version} .
	  docker buildx build --no-cache --builder ${builder} --target ${img} --platform ${platforms} ${flag} -t compss/${img}:${version} .
	  first=false
   fi
   #docker build --target ${img} -t compss/${img}:${version} .
   #if $push; then
   #	docker push compss/${img}:${version}
   #fi
   docker buildx build --builder ${builder} --target ${img} --platform ${platforms} ${flag} -t compss/${img}:${version} .
done
