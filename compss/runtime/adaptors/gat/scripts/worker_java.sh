#!/bin/bash

  # Get worker common functions
  SCRIPT_DIR=$(dirname "$0")
  # shellcheck source=./worker_commons.sh
  source "${SCRIPT_DIR}"/worker_commons.sh

  # Pre-execution
  get_parameters "$@"
  set_env

  # Execution: launch the JVM to run the task
  java \
    -Xms128m -Xmx2048m \
    -classpath "$CLASSPATH" \
    es.bsc.compss.gat.worker.GATWorker "$taskSandboxWorkingDir" $params
  ev=$?
 
  # Exit  
  if [ $ev -eq 0 ]; then
    exit 0
  else
    echo 1>&2 "Task execution failed"
    exit 7
  fi

