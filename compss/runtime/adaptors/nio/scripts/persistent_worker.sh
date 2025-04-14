#!/bin/bash

  # shellcheck disable=SC2154
  # Because many variables are sourced from common setup.sh


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
  # shellcheck disable=SC1091
  source "${SCRIPT_DIR}"/setup.sh

  # Load parameters --------------------------------------------------
  load_parameters "$@"

  # BLAUNCH start ----------------------------------------------------
  # Check that the current machine has not already awaken any WORKER in PORT and for app UUID
  worker_class="es.bsc.compss.nio.worker.NIOWorker"
  pid=$(ps -elfa | grep ${worker_class} | grep "${appUuid}" | grep "${hostName}" | grep "${worker_port}" | grep -v grep | awk '{ print $4 }')
  if [ "$pid" != "" ]; then
    if [ "$debug" == "true" ]; then
       echo "Worker already awaken. Nothing to do"
    fi
    echo "$pid"
    exit 0
  fi

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
      export NANOS6=debug

      echo "[persistent_worker.sh] Calling NIOWorker of host ${hostName}"
      echo "Calling NIOWorker"
      echo "Cmd: $cmd ${paramsToCOMPSsWorker}"
  fi

  # Prepare binding log files
  # TODO: avoid to create always these log files. Create and transfer only when needed.
  mkdir -p "${logDir}"
  touch "${logDir}/binding_worker.out"
  touch "${logDir}/binding_worker.err"

  # shellcheck disable=SC2086

  SETSID="setsid"

  export LD_PRELOAD=${LD_PRELOAD}:${AFTER_EXTRAE_LD_PRELOAD}

  "${SETSID}" $cmd ${paramsToCOMPSsWorker} 1> "${logDir}/worker_${hostName}.out" 2> "${logDir}/worker_${hostName}.err" < /dev/null | echo "$!" &
  exitValue=$?
  if [ "$exitValue" != "0" ]; then
    echo "[WARNING][persistent_worker_starter.sh] Failed to start worker ${hostName}. Exit value: ${exitValue}"
  fi

  post_launch

  # Exit
  if [ $exitValue -eq 0 ]; then
	exit 0
  else
	echo 1>&2 "[persistent_worker.sh] Worker could not be initalized"
	exit 7
  fi
