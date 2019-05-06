#!/bin/bash

  # worker_commons.sh loads some variables
  # shellcheck disable=SC2154

  #-------------------------------------
  # Define script variables and exports
  #-------------------------------------
  if [ -z "${COMPSS_HOME}" ]; then
    SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    export COMPSS_HOME=${SCRIPT_DIR}/../../../../../
  else
    SCRIPT_DIR="${COMPSS_HOME}/Runtime/scripts/system/adaptors/gat"
  fi

  # shellcheck source=./worker_commons.sh
  # shellcheck disable=SC1091
  source "${SCRIPT_DIR}"/worker_commons.sh
 
  #-------------------------------------
  # Remove obsolete files
  #-------------------------------------
  rmfilesNum=${11}
  obsoletes=${*:12:${rmfilesNum}}

  if [ "${rmfilesNum}" -gt 0 ]; then
    echo "[WORKER.SH] Removing $rmfilesNum obsolete files"
    # shellcheck disable=SC2086
    rm -f ${obsoletes}
  fi

  #-------------------------------------
  # Determine Language-dependent script
  #-------------------------------------
  # Loads variables
  get_host_parameters "$@"

  echo "[WORKER.SH] Starting GAT Worker"
  if [ "${debug}" == "true" ]; then
    echo "[WORKER.SH]         - Node name                          = ${nodeName}"
    echo "[WORKER.SH]         - Installation Directory             = ${installDir}"
    echo "[WORKER.SH]         - Application path                   = ${appDir}"
    echo "[WORKER.SH]         - LibPath                            = ${libPath}"
    echo "[WORKER.SH]         - Working Directory                  = ${workingDir}"
    echo "[WORKER.SH]         - Storage Configuration              = ${storageConf}"
    if [ "${tracing}" == "true" ]; then
      echo "[WORKER.SH]         - Tracing                            = enabled" 
    else
      echo "[WORKER.SH]         - Tracing                            = disabled" 
    fi
    echo "[WORKER.SH]         - Streaming Backend                  = ${streaming}"
    echo "[WORKER.SH]         - Streaming Master                   = ${streamingMasterName}"
    echo "[WORKER.SH]         - Streaming Port                     = ${streamingPort}"
  fi

  implType=${invocation[0]}
  if [ "${implType}" == "METHOD" ]; then
    lang=${invocation[1]}
  else
    echo "[WORKER.SH] Non-native task detected. Switching to JAVA invoker."
    lang="java"
  fi

  #-------------------------------------
  # Determine Language-dependent script
  #-------------------------------------
  cd "$workingDir" || exit 1
  echo "[WORKER.SH] Starting language $lang script"
  "${SCRIPT_DIR}"/worker_$lang.sh "$@"
  endCode=$?
  echo " "
  echo "[WORKER.SH] EndStatus = $endCode"
  cd "$workingDir" || exit 1


  #-------------------------------------
  # Exit
  #-------------------------------------
  if [ $endCode -eq 0 ]; then
    exit 0
  else
    echo 1>&2 "Task execution failed"
    exit 7
  fi

