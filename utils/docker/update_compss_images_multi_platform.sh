#!/bin/bash -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
version=$1
push=$2
dh_username=$3
dh_password=$4
default_images="minimal pycompss compss compss-tutorial"
platforms="linux/amd64,linux/arm64"
builder="docker-multiarch"

# Move to root folder
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
    # Minimal image will be built completeley from scratch with --no-cache
    docker buildx build --no-cache --builder ${builder} --target ${img} --platform ${platforms} ${flag} -t compss/${img}:${version} . > out_${img}.txt 2>&1 | tee out_${img}.txt
    first=false
  else
    docker buildx build --builder ${builder} --target ${img} --platform ${platforms} ${flag} -t compss/${img}:${version} . > out_${img}.txt 2>&1 | tee out_${img}.txt
  fi
done
