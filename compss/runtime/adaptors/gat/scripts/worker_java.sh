#!/bin/bash

  # Get worker common functions
  SCRIPT_DIR=$(dirname "$0")
  # shellcheck source=./worker_commons.sh
  source "${SCRIPT_DIR}"/worker_commons.sh

  #-------------------------------------
  # Retrieve host configuration
  #-------------------------------------
  get_host_parameters "$@"

  implType=${invocation[0]}
  lang=${invocation[1]}
  case "${implType}" in
    "METHOD")
      cp=${invocation[2]}
      echo "[WORKER_JAVA.SH]    - classpath                          = $cp"
      className=${invocation[3]}
      echo "[WORKER_JAVA.SH]    - class name                         = $className"
      methodName=${invocation[4]}
      echo "[WORKER_JAVA.SH]    - method name                        = $methodName"
      implDescription=( "${implType}" "${className}" "${methodName}" )
      specificSandbox=false;
      arguments=(${invocation[@]:5})
      ;;
    *)
      echo "Parsing non-native";
      ;;
  esac

  echo "ARGUMENTS  = ${arguments[*]}"
  get_invocation_params ${arguments[@]}

  # Pre-execution
  set_env
  
  # Execution: launch the JVM to run the task
  java \
    -Xms128m -Xmx2048m \
    -classpath "$CLASSPATH" \
    es.bsc.compss.gat.worker.GATWorker ${hostFlags[@]} ${implDescription[@]} ${invocationParams[@]}
  ev=$?
 echo "Exit value=$ev"
  # Exit  
  if [ $ev -eq 0 ]; then
    exit 0
  else
    echo 1>&2 "Task execution failed"
    exit 7
  fi

