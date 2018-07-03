#!/bin/bash

  ######################
  # MAIN PROGRAM
  ######################

  # Load common setup functions --------------------------------------
  if [ -z "${COMPSS_HOME}" ]; then
    SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    export COMPSS_HOME=${SCRIPT_DIR}/../../../../../
  else
    SCRIPT_DIR="${COMPSS_HOME}/Runtime/scripts/system/adaptors/nio"
  fi
  # shellcheck source=setup.sh
  source ${SCRIPT_DIR}/setup.sh

  # Load parameters --------------------------------------------------
  load_parameters $@

  # Trap to clean environment
  trap clean_env EXIT

  # Normal start -----------------------------------------------------
  # Setup
  setup_environment
  setup_extrae
  setup_jvm

  # Launch the Worker JVM
  pre_launch  

  reprogram_fpga

  if [ "$debug" == "true" ]; then
      export COMPSS_BINDINGS_DEBUG=1
      export NX_ARGS="--summary"
      echo "Calling NIOWorker"
      echo "Cmd: $cmd ${paramsToCOMPSsWorker}"
  fi

  $cmd ${paramsToCOMPSsWorker} 1>$workingDir/log/worker_${hostName}.out 2> $workingDir/log/worker_${hostName}.err
  exitValue=$?

  post_launch

  # Exit with the worker status (last command)
  if [ "$debug" == "true" ]; then
    echo "Exit NIOWorker"
  fi
  exit $exitValue

