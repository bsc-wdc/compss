#!/bin/bash

  usage() {
    echo "Usage: "
    echo "    $0 <projectFile> <remoteUser> List<workerName>"
    echo " "
  }

  write_header() {
    local file=$1
    cat > "$file" << EOT
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Project>
EOT
  }

  write_master() {
    local file=$1
    cat >> "$file" << EOT
    <MasterNode />
EOT
  }

  write_worker() {
    local user=$1
    local workerName=$2
    local file=$3

    "${SCRIPT_DIR}"/addWorker_project.sh "$user" "$workerName" "$file"
    local ev=$?
    if [ $ev -ne 0 ]; then
      echo "[GENERATE_PROJECT] [ERROR] Cannot add $workerName to $file"
      exit 1
    fi
  }

  write_footer() {
    local file=$1
    cat >> "$file" << EOT
</Project>
EOT

  }

  ###########################################################
  # MAIN
  ###########################################################

  # Arguments
  #echo "[GENERATE_PROJECT] [DEBUG] Parsing arguments"
  if [ $# -lt 2 ]; then
    echo "[GENERATE_PROJECT] [ERROR] Invalid number of arguments"
    usage
    exit 1
  fi
  projectFile="$1"
  remoteUser="$2"
  shift 2
  workers="$@"

  # Global variable
  SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

  # Opening file
  echo "[GENERATE_PROJECT] [DEBUG] Adding header"
  write_header "${projectFile}"
  echo "[GENERATE_PROJECT] [DEBUG] Adding Master Node"
  write_master "${projectFile}"

  # Appending workers
  for worker in ${workers}; do
    echo "[GENERATE_PROJECT] [DEBUG] Adding worker ${worker}"
    write_worker "${remoteUser}" "${worker}" "${projectFile}"
  done

  # Closing file
  echo "[GENERATE_PROJECT] [DEBUG] Adding footer"
  write_footer "${projectFile}"

  # DONE
  echo "[GENERATE_PROJECT] [INFO] DONE"
  exit 0
 
