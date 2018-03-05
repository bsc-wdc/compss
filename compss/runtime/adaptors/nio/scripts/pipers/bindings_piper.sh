#!/bin/bash

  ########################################
  # SCRIPT HELPER FUNCTIONS
  ########################################
  get_args() {
    # Get number of threads
    numThreads=$1
    shift 1
    # Get Data pipes
    dataCMDpipe=$1
    dataRESULTpipe=$2
    shift 2
    
    # Get CMD pipes
    CMDpipes=()
    numPipesCMD=$1
    shift 1
    local i=0
    while [ $i -lt "$numPipesCMD" ]; do
      local arg_pos=$((i+1))
      CMDpipes[$i]=${!arg_pos}
      i=$((i+1))
    done
    shift "${numPipesCMD}"

    # Get RESULT pipes
    RESULTpipes=()
    numPipesRESULT=$1
    shift 1
    i=0
    while [ $i -lt "$numPipesRESULT" ]; do
      local arg_pos=$((i+1))
      RESULTpipes[$i]=${!arg_pos}
      i=$((i+1))
    done
    shift "${numPipesRESULT}"

    # Get binding
    binding=$1
    shift 1

    # Get tracing if python
    if [ "$binding" == "PYTHON" ]; then
        tracing=$1
        shift 1
        if [ "$tracing" == "true" ]; then
            configPath="${SCRIPT_DIR}/../../../../../configuration/xml/tracing"
            escapedConfigPath=$(echo "$configPath" | sed 's_/_\\/_g')
            baseConfigFile="${configPath}/extrae_python_worker.xml"
            workerConfigFile="$(pwd)/extrae_python_worker.xml"

            echo $(sed s/{{PATH}}/"${escapedConfigPath}"/g <<< $(cat "${baseConfigFile}")) > "${workerConfigFile}"

            export PYTHONPATH=${SCRIPT_DIR}/../../../../../../Dependencies/extrae/libexec/:${SCRIPT_DIR}/../../../../../../Dependencies/extrae/lib/:${PYTHONPATH}
            export EXTRAE_CONFIG_FILE=${workerConfigFile}
        fi
    fi

    # Get binding additional executable and arguments
    bindingExecutable=$1
    shift 1
    bindingArgs=$@
  }

  clean_env() {
    echo "[BINDINGS PIPER] Cleaning environment"

    if [ -n "$bindingPID" ]; then
      # Send SIGTERM to specific binding process

      # Ignore kill output because if it doesn't exist it means that
      # the subprocess has correctly finished
      kill -15 "$bindingPID" &> /dev/null
    fi
    # remove data pipes
    rm -f "${dataCMDpipe}" "${dataRESULTpipe}"
    
    # remove job pipes
    for i in "${CMDpipes[@]}"; do
      rm -f $i
    done

    for i in "${RESULTpipes[@]}"; do
      rm -f $i
    done
  }

  ########################################
  # MAIN
  ########################################

  # Actual script dir
  SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

  # Arguments
  get_args $@

  # Create TRAP to ensure pipes are deleted if something goes wrong
  trap clean_env EXIT

  # Log
  echo "[BINDINGS PIPER] NumThreads: $numThreads"
  
  # Clean and Create data pipes
  echo "[BINDINGS PIPER] Data CMD Pipe: $dataCMDpipe"
  rm -f "$dataCMDpipe"
  mkfifo "$dataCMDpipe"
  
  echo "[BINDINGS PIPER] Data RESULT Pipe: $dataRESULTpipe"
  rm -f "$dataRESULTpipe"
  mkfifo "$dataRESULTpipe"

  # Clean and Create CMD Pipes
  for i in "${CMDpipes[@]}"; do
    echo "[BINDINGS PIPER] CMD Pipe: $i"
    rm -f $i
    mkfifo $i
  done

  # Clean and Create RESULT Pipes
  for i in "${RESULTpipes[@]}"; do
    echo "[BINDINGS PIPER] RESULT Pipe: $i"
    rm -f $i
    mkfifo $i
  done

  # Perform specific biding call (THE CALL IS BLOCKING)
  echo "[BINDINGS PIPER] Initializing specific binding for $binding"
  echo "[BINDINGS PIPER] Making call: $bindingExecutable $bindingArgs"

  eval $bindingExecutable $bindingArgs &
  bindingPID=$!

  # Wait for binding executable completion
  wait $bindingPID
  exitValue=$?

  # If process has failed, force exit on pipes
  if [ $exitValue -ne 0 ]; then
    for i in "${RESULTpipes[@]}"; do
      if [ -f "$i" ]; then
        echo "errorTask" >> $i
      fi
    done
  fi
  
  # Clean environment
  # Cleaned on TRAP

  # Exit message
  echo "[BINDINGS PIPER] Finished with status $exitValue"
  exit $exitValue

