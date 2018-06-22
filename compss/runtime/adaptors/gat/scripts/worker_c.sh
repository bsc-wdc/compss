#!/bin/bash

  # Get worker common functions
  SCRIPT_DIR=$(dirname "$0")
  #shellcheck source=./worker_commons.sh
  source "${SCRIPT_DIR}"/worker_commons.sh

  # Pre-execution
  get_parameters "$@"
  set_env

  # Execution
  taskTracing=false # Only available with NIO
  taskId=0 # Not used with GAT

  #Removing  AppDir and Installation dir from params
  param0=`echo ${params} | awk '{print \$1}'`
  params=`echo ${params} | awk '{\$1=\$2=\$3="";print \$0}'`
  params=${params:3}
  params="$param0 $params"

  echo "[WORKER_C.SH] EXEC CMD: ${app_dir}/worker/worker_c $taskTracing $taskId $params"
  "${app_dir}"/worker/worker_c $taskTracing $taskId $params
  ev=$?

  # Exit
  if [ $ev -eq 0 ]; then
    exit 0
  else
    echo 1>&2 "Task execution failed"
    exit 7
  fi

