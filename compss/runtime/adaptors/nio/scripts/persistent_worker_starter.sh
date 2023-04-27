#!/bin/bash

  # shellcheck disable=SC2154
  # Because many variables are sourced from common setup.sh


  ######################
  # MAIN PROGRAM
  ######################

  echo "EAR log: Worker  starting----------------" >> $HOME/ear_compss_logs

  echo "LD_PRELOAD_LD_PRELOAD INICIAL"
  echo "LD_PRELOAD: ${LD_PRELOAD}"
  echo "LD_PRELOAD_LD_PRELOAD INICIAL"


  # Load common setup functions --------------------------------------
  if [ -z "${COMPSS_HOME}" ]; then
    SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    export COMPSS_HOME=${SCRIPT_DIR}/../../../../../
  else
    SCRIPT_DIR="${COMPSS_HOME}/Runtime/scripts/system/adaptors/nio"
  fi
  # shellcheck source=setup.sh
  # shellcheck disable=SC1091

  echo "EAR log: loading setup" >> $HOME/ear_compss_logs
  source "${SCRIPT_DIR}"/setup.sh
  echo "EAR log: setup loaded" >> $HOME/ear_compss_logs

  # Load parameters --------------------------------------------------

  echo "EAR log: loading parameters $@ " >> $HOME/ear_compss_logs
  load_parameters "$@"

  # Trap to clean environment

  echo "EAR log: before trap " >> $HOME/ear_compss_logs
  trap clean_env EXIT

  # Normal start -----------------------------------------------------
  # Setup

  echo "EAR log: Starting normal start "  >> $HOME/ear_compss_logs
  setup_environment
  echo "EAR log: setup_extrae "  >> $HOME/ear_compss_logs
  setup_extrae
  echo "EAR log: setup_jvm "  >> $HOME/ear_compss_logs
  setup_jvm

  # Launch the Worker JVM

  echo "EAR log: pre_launch "  >> $HOME/ear_compss_logs
  pre_launch

  echo "EAR log: reprogram_fpga "  >> $HOME/ear_compss_logs
  reprogram_fpga

  if [ "$debug" == "true" ]; then
      export COMPSS_BINDINGS_DEBUG=1
      export NX_ARGS="--summary"
      export NANOS6=debug

      echo "[persistent_worker_starter.sh] Calling NIOWorker of host ${hostName}"
      echo "Calling NIOWorker"
      echo "Cmd: $cmd ${paramsToCOMPSsWorker}"
  fi

  if [ -n "$cusGPU" ] && [ "$cusGPU" -gt 0 ]; then
    echo "Computing units GPU is greater than zero, Nanos6 scheduler set to hierarchical"
    export NANOS6_SCHEDULER=hierarchical
  fi

  # Prepare binding log files
  # TODO: avoid to create always these log files. Create and transfer only when needed.
  mkdir -p "${logDir}"
  touch "${logDir}/binding_worker.out"
  touch "${logDir}/binding_worker.err"

  # export LD_PRELOAD=${AFTER_EXTRAE_LD_PRELOAD}

  $cmd ${paramsToCOMPSsWorker} 1>"${logDir}/worker_${hostName}.out" 2>"${logDir}/worker_${hostName}.err"

  exitValue=$?
  if [ "$exitValue" != "0" ]; then
    echo "[WARNING][persistent_worker_starter.sh] Failed to start worker ${hostName}. Exit value: ${exitValue}"
  fi

  post_launch

  # Exit with the worker status (last command)
  if [ "$debug" == "true" ]; then
    echo "[persistent_worker_starter.sh] Exit NIOWorker of host ${hostName} with exit value ${exitValue}"
  fi

  echo "LD_PRELOAD_LD_PRELOAD FINAL"
  echo "LD_PRELOAD: ${LD_PRELOAD}"
  echo "LD_PRELOAD_LD_PRELOAD FINAL"

  exit $exitValue

