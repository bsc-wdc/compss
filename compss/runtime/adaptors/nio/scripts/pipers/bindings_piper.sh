#!/bin/bash

  ########################################
  # SCRIPT HELPER FUNCTIONS
  ########################################
  get_args() {
    # Get number of threads
    numThreads=$1
    shift 1
    # Get Data pipes
    controlCMDpipe=$1
    controlRESULTpipe=$2
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

    # Get tracing and virtual environment if python
    if [ "$binding" == "PYTHON" ]; then
        # Get virtual environment
        virtualEnvironment=$1
        propagateVirtualEnvironment=$2
        shift 2
        # Get tracing
        tracing=$1
        shift 1
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
    rm -f "${controlCMDpipe}" "${controlRESULTpipe}"

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
  echo "[BINDINGS PIPER] Control CMD Pipe: $controlCMDpipe"
  rm -f "$controlCMDpipe"
  mkfifo "$controlCMDpipe"

  echo "[BINDINGS PIPER] Control RESULT Pipe: $controlRESULTpipe"
  rm -f "$controlRESULTpipe"
  mkfifo "$controlRESULTpipe"

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

  # Activating virtual environment if needed
  if [ "$virtualEnvironment" != "null" ] && [ "$propagateVirtualEnvironment" == "true" ]; then
    echo "[BINDINGS PIPER] Activating virtual environment $virtualEnvironment"
    source ${virtualEnvironment}/bin/activate
  fi

  # Export tracing
  if [ "$tracing" == "true" ]; then
    configPath="${SCRIPT_DIR}/../../../../../configuration/xml/tracing"
    escapedConfigPath=$(echo "$configPath" | sed 's_/_\\/_g')
    baseConfigFile="${configPath}/extrae_python_worker.xml"
    workerConfigFile="$(pwd)/extrae_python_worker.xml"

    echo $(sed s/{{PATH}}/"${escapedConfigPath}"/g <<< $(cat "${baseConfigFile}")) > "${workerConfigFile}"

    export PYTHONPATH=${SCRIPT_DIR}/../../../../../../Dependencies/extrae/libexec/:${SCRIPT_DIR}/../../../../../../Dependencies/extrae/lib/:${PYTHONPATH}
    export EXTRAE_CONFIG_FILE=${workerConfigFile}
  fi

  # Perform specific biding call (THE CALL IS BLOCKING)
  echo "[BINDINGS PIPER] Initializing specific binding for $binding"
  echo "[BINDINGS PIPER] Making call: $bindingExecutable $bindingArgs"

  eval $bindingExecutable $bindingArgs &
  bindingPID=$!

  stop_received=false
  while [ "${stop_received}" = false ]; do
    read line < "$controlCMDpipe"
    echo "Obtained line ${line}"
    command=$(echo "$line" | tr " " "\t" | awk '{ print $1 }')
    case "${command}" in
      "PING")
        echo "PONG" > "${controlRESULTpipe}"
        ;;
      "NEW_PIPE")
        pipe_name=$(echo "$line" | tr " " "\t" | awk '{ print $2 }')
        echo "[BINDINGS PIPER] Creating new pipe pair ${pipe_name}"
        rm -f "${pipe_name}.outbound"
        rm -f "${pipe_name}.inbound"
        mkfifo "${pipe_name}.inbound"
        mkfifo "${pipe_name}.outbound"
        echo "CREATED_PIPE ${pipe_name}" > "${controlRESULTpipe}"
        ;;
      "QUIT")
        echo "We should stop the piper"
        stop_received=true
        ;;
      *)
        echo "UNKNOWN COMMAND"
    esac
    echo "[BINDINGS PIPER] Next Command"
  done

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

  # Deactivate virtual environment if needed
  if [ "$virtualEnvironment" != "null" ] && [ "$propagateVirtualEnvironment" == "true" ]; then
    deactivate  # this function is contained in activate script sourced previously
  fi

  # Clean environment
  # Cleaned on TRAP

  # Exit message
  echo "[BINDINGS PIPER] Finished with status $exitValue"
  exit $exitValue
