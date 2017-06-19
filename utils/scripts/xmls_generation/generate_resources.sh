#!/bin/bash

  usage() {
    echo "Usage: "
    echo "    $0 <resourcesFile> <remoteUser> List<workerName>"
    echo " "
  }

  write_header() {
    local file=$1
    cat > $file << EOT
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<ResourcesList>
EOT
  }

  write_worker() {
    local user=$1
    local workerName=$2
    local file=$3

    ${scriptDir}/addWorker_resources.sh "${user}" "$workerName" "$file"
    if [ $? -ne 0 ]; then
      echo "[GENERATE_RESOURCES] [ERROR] Cannot add $workerName to $file"
      exit 1
    fi
  }

  write_footer() {
    local file=$1
    cat >> $file << EOT
</ResourcesList>
EOT

  }

  ###########################################################
  # MAIN
  ###########################################################

  # Arguments
  # echo "[GENERATE_RESOURCES] [DEBUG] Parsing arguments"
  if [ $# -lt 2 ]; then
    echo "[GENERATE_RESOURCES] [ERROR] Invalid number of arguments"
    usage
    exit 1
  fi
  resourcesFile="$1"
  remoteUser="$2"
  shift 2
  workers="$@"

  # Global variable
  scriptDir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

  # Opening file
  echo "[GENERATE_RESOURCES] [DEBUG] Adding header"
  write_header "${resourcesFile}"

  # Appending workers
  for worker in ${workers}; do
    echo "[GENERATE_RESOURCES] [DEBUG] Adding worker ${worker}"
    write_worker "${remoteUser}" "${worker}" "${resourcesFile}"
  done

  # Closing file
  echo "[GENERATE_RESOURCES] [DEBUG] Adding footer"
  write_footer "${resourcesFile}"

  # DONE
  echo "[GENERATE_RESOURCES] [INFO] DONE"
  exit 0
 
