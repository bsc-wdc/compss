#!/bin/bash

  ######################
  # MAIN PROGRAM
  ######################

  # Load common setup functions --------------------------------------
  scriptDir=$(dirname $0)
  source ${scriptDir}/setup.sh

  # Load parameters --------------------------------------------------
  load_parameters $*
  # Shift the 4 paramters consumed only by .sh
  shift ${paramsToShift}

  # Normal start -----------------------------------------------------
  # Setup
  setup_environment
  setup_jvm

  # Launch the Worker JVM
  pre_launch  

  if [ "$debug" == "true" ]; then
      echo "Calling NIOWorker"
      echo "Cmd: $cmd $*"
  fi
  
  $cmd $* 1>$workingDir/log/worker_${hostName}.out 2> $workingDir/log/worker_${hostName}.err
  exitValue=$?

  post_launch

  # Exit with the worker status (last command)
  if [ "$debug" == "true" ]; then
    echo "Exit NIOWorker"
  fi
  exit $exitValue

