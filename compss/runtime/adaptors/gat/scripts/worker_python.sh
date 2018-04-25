#!/bin/bash

  # Get worker common functions
  scriptDir=$(dirname $0)
  source ${scriptDir}/worker_commons.sh

  # Pre-execution
  get_parameters $@
  set_env

  # Execution
  taskTracing=false # Only available with NIO
  taskId=0 # Not used with GAT

  # Python version folder
  subfolder=$( $pythonInterpreter -c "import sys; print(sys.version_info[:][0])" )

  # Include subfolder in pycompss home and set pythonpath related env
  export PYCOMPSS_HOME=${PYCOMPSS_HOME}/$subfolder
  export PYTHONPATH=$app_dir:$PYCOMPSS_HOME:$pythonpath:$PYTHONPATH

  echo "[WORKER_PYTHON.SH] EXEC CMD: $pythonInterpreter $PYCOMPSS_HOME/pycompss/worker/worker.py $taskTracing $taskId $params"
  $pythonInterpreter ${PYCOMPSS_HOME}/pycompss/worker/worker.py $taskTracing $taskId $params

  # Exit
  if [ $? -eq 0 ]; then
    exit 0
  else
    echo 1>&2 "Task execution failed"
    exit 7
  fi
