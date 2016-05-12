#!/bin/bash

  # Get worker common functions
  scriptDir=$(dirname $0)
  source ${scriptDir}/worker_commons.sh

  # Pre-execution
  get_parameters $@
  set_env

  # Execution
  shift $shiftSizeForTaskParams # Keep only method params

  # Launch the JVM to run the task
  java -Xms128m -Xmx2048m -classpath $CLASSPATH integratedtoolkit.gat.worker.GATWorker $debug $app $method $has_target $nparams $params
 
  # Exit  
  if [ $? -eq 0 ]; then
        exit 0
  else
        echo 1>&2 "Task execution failed"
        exit 7
  fi

