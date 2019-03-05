#!/bin/bash

  ########################################
  # SCRIPT HELPER FUNCTIONS
  ########################################
  get_args() {
    # Get Control pipes
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

    tracing=${1}
    shift 1
    
    # Get binding
    binding=$1
    shift 1

    # Get tracing and virtual environment if python
    if [ "$binding" == "PYTHON" ]; then
        # Get virtual environment
        virtualEnvironment=$1
        propagateVirtualEnvironment=$2
        shift 2
    fi

  }

  create_pipe() {
    rm -f "$1"
    mkfifo "$1"
  }

  ########################################
  # MAIN
  ########################################
  echo "[BINDINGS PIPER] STARTING BINDINGS PIPER"
  # Actual script dir
  SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

  # Arguments
  get_args $@

  # Clean and Create control pipes
  echo "[BINDINGS PIPER] Control CMD Pipe: $controlCMDpipe"
  create_pipe "$controlCMDpipe"
  
  echo "[BINDINGS PIPER] Control RESULT Pipe: $controlRESULTpipe"
  create_pipe "$controlRESULTpipe"

  # Clean and Create CMD Pipes
  for i in "${CMDpipes[@]}"; do
    echo "[BINDINGS PIPER] CMD Pipe: $i"
    create_pipe $i
  done

  # Clean and Create RESULT Pipes
  for i in "${RESULTpipes[@]}"; do
    echo "[BINDINGS PIPER] RESULT Pipe: $i"
    create_pipe $i
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

  stop_received=false
  while [ "${stop_received}" = false ]; do
    read line 
    command=$(echo "$line" | tr " " "\t" | awk '{ print $1 }')
    echo "[BINDINGS PIPER] READ COMMAND --${line}--"
    case "${command}" in
      "PING")
        echo "PONG" >> "${controlRESULTpipe}"
        ;;

      "CREATE_CHANNEL")
        reply="CHANNEL_CREATED"
        for pipe_name in $(echo "${line}" | cut -d' ' -f2- | tr " " "\t"); do
          create_pipe "${pipe_name}"
          reply="${reply} ${pipe_name}"
        done
        echo "${reply}" >> "${controlRESULTpipe}"
        ;;

      "START_WORKER")
        workerCMDpipe=$(echo "$line" | tr " " "\t" | awk '{ print $2 }')
        create_pipe ${workerCMDpipe}
        workerRESULTpipe=$(echo "$line" | tr " " "\t" | awk '{ print $3 }')
        create_pipe ${workerRESULTpipe}

        workerCMD=$(echo ${line} | cut -d' ' -f4-) 
        eval ${workerCMD} &
        bindingPID=$!
        echo "WORKER_STARTED ${bindingPID}" >> "${controlRESULTpipe}"
        ;;

      "GET_ALIVE")
        reply="ALIVE_REPLY"
        if [ "${line}" != "GET_ALIVE" ]; then
          pids=$(echo ${line} | cut -d' ' -f2-)
          alive_processes=$(ps h -o pid,stat ${pids} | awk '$2 != "Z" {print $1}')
          for alive_process in ${alive_processes}; do
            reply="${reply} ${alive_process}"
          done
        fi
        echo "${reply}" >> "${controlRESULTpipe}"
        ;;

      "QUIT")
        stop_received=true
        ;;
      *)
        echo "[BINDINGS PIPER] UNKNOWN COMMAND ${line}"
    esac
  done <"${controlCMDpipe}" 3>"${controlCMDpipe}"

  echo "QUIT" >> "${controlRESULTpipe}"
  
  # Exit message
  echo "[BINDINGS PIPER] Finished"
  exit 0
