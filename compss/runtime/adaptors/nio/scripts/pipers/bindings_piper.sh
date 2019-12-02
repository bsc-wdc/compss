#!/bin/bash

########################################
# SCRIPT CONSTANTS
########################################

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    

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
        # Get virtual environment parameters
        virtualEnvironment=$1
        propagateVirtualEnvironment=$2
        # Get Information for the mpi worker
        mpiWorker=$3
        numThreads=$4
        pythonInterpreter=$5
        shift 5
    fi
}

create_pipe() {
    rm -f "$1"
    mkfifo "$1"
}

export_tracing() {
    if [ "$tracing" -gt "0" ]; then
        echo "Initializing python tracing with extrae..."
        configPath="${SCRIPT_DIR}/../../../../../configuration/xml/tracing"
        escapedConfigPath=$(echo "$configPath" | sed 's_/_\\/_g')
        baseConfigFile="${configPath}/extrae_python_worker.xml"
        workerConfigFile="$(pwd)/extrae_python_worker.xml"

        sed "s/{{PATH}}/${escapedConfigPath}/g" "${baseConfigFile}" > "${workerConfigFile}"

        if [ "$mpiWorker" == "true" ]; then
            # Exporting variables for MPI Python worker
            dependencies_path="${SCRIPT_DIR}/../../../../../../Dependencies"
            libmpitrace="lib/libmpitrace.so"
            if [ -f "${dependencies_path}/extrae/${libmpitrace}" ]; then
                # If the normal installation contains libmpitrace.so
                export EXTRAE_HOME=${dependencies_path}/extrae
            elif [ -f "${dependencies_path}/extrae-openmpi/${libmpitrace}" ]; then
                # If openmpi extrae installation exists
                export EXTRAE_HOME=${dependencies_path}/extrae-openmpi
            elif [ -f "${dependencies_path}/extrae-impi/${libmpitrace}" ]; then
                # If impi extrae installation exists
                export EXTRAE_HOME=${dependencies_path}/extrae-impi
            else
                # Tracing with extrae with mpi worker requires extrae to be compiled with mpi
                echo "ERROR: Extrae is not compiled with mpi support."
                echo "QUIT" >> "${controlRESULTpipe}"
                exit 1
            fi
            
            # The LATER_* variables are used in the workerCMD built from the JAVA side (mpirun -X var=LATER_var)
            unset EXTRAE_SKIP_AUTO_LIBRARY_INITIALIZE
            export LATER_MPI_EXTRAE_CONFIG_FILE=${workerConfigFile}
            export LATER_MPI_LD_PRELOAD="${EXTRAE_HOME}/${libmpitrace}"
            export LATER_MPI_PYTHONPATH=${EXTRAE_HOME}/libexec/:${EXTRAE_HOME}/lib/:${PYTHONPATH}
        else
            # Exporting variables for multi-processing Python worker
            unset EXTRAE_SKIP_AUTO_LIBRARY_INITIALIZE
            export EXTRAE_CONFIG_FILE=${workerConfigFile}
            export PYTHONPATH=${SCRIPT_DIR}/../../../../../../Dependencies/extrae/libexec/:${SCRIPT_DIR}/../../../../../../Dependencies/extrae/lib/:${PYTHONPATH}
        fi
    
    elif [ "$tracing" -lt "-1" ]; then
        # exporting variables required by any other tracing option (map/ddt/scorep)
        if [ "$binding" == "PYTHON" ]; then
            export ALLINEA_MPI_INIT=MPI_Init_thread
            export ALLINEA_MPI_INIT_PENDING=1
        else
            # Only tracing with extrae is supported for other bindings
            echo "ERROR: Unsupported tracing mode for this binding."
            echo "QUIT" >> "${controlRESULTpipe}"
            exit 1
        fi
    fi
}

process_pipe_commands() {
    stop_received=false

    # shellcheck disable=SC2094 # to block pipe read
    while [ "${stop_received}" = false ]; do
        read cmd_tag line
        echo "[BINDINGS PIPER] READ COMMAND --${cmd_tag}--"
        case "${cmd_tag}" in
            "PING")
                echo "PONG" >> "${controlRESULTpipe}"
                ;;

            "CREATE_CHANNEL")
                reply="CHANNEL_CREATED"
                for pipe_name in $(echo "${line}" | tr " " "\\t"); do
                    create_pipe "${pipe_name}"
                    reply="${reply} ${pipe_name}"
                done
                echo "${reply}" >> "${controlRESULTpipe}"
                ;;

            "START_WORKER")
                workerCMDpipe=$(echo "$line" | tr " " "\\t" | awk '{ print $1 }')
                create_pipe "${workerCMDpipe}"
                workerRESULTpipe=$(echo "$line" | tr " " "\\t" | awk '{ print $2 }')
                create_pipe "${workerRESULTpipe}"

                # Touch the binding_worker.out and err files
                mkdir -p "${worker_log_dir}"
                touch "${worker_log_dir}/binding_worker.out"
                touch "${worker_log_dir}/binding_worker.err"
                
                # Build workerCMD
                workerCMD=$(echo "${line}" | cut -d' ' -f3-)
                
                if [ "$binding" == "PYTHON" ] && [ "$mpiWorker" == "true" ]; then
                    # delimiter example: "python -u"
                    delimiter="${pythonInterpreter} -u"
                    bindingExecutable="${workerCMD%%"$delimiter"*}"
                    bindingArgs=${workerCMD#*"$delimiter"}
        
                    # Get full path of the binary
                    pythonInterpreter=$(which "${pythonInterpreter}")
        
                    if [ "$tracing" -eq "-1" ]; then 
                        # scorep
                        echo "[BINDINGS PIPER] Making preload call in folder $(pwd)"
                        TRACE_SCRIPTS_PATH=${SCRIPT_DIR}/../../../../../scripts/system/trace
                        python_version=$( ${pythonInterpreter} -c "import sys; print(sys.version_info[:][0])" )
                        # shellcheck disable=SC1091
                        # shellcheck source=./scorep.sh
                        source "${TRACE_SCRIPTS_PATH}/scorep.sh" "$python_version"
                        # Set path to the application - this even better because the submission is independent of the current working directory
                        app_path=$(pwd)
                        app_name=piper_worker.py
                        rm -rf "${app_path}"/.scorep_preload
                        ld_preload=$(scorep-preload-init --value-only --user --nocompiler --mpp=mpi --thread=pthread --io=runtime:posix "${app_path}/${app_name}")
                        bindingArgs=" -x LD_PRELOAD=$ld_preload ${pythonInterpreter} -u -m scorep $bindingArgs"
                        echo "[BINDINGS PIPER] ScoreP setup done"
                    elif [ "$tracing" -eq "-2" ]; then 
                        # arm-map
                        echo "[BINDINGS PIPER] Setting up arm-map tracing in folder $(pwd)"
                        TRACE_SCRIPTS_PATH=${SCRIPT_DIR}/../../../../../scripts/system/trace
                        # shellcheck disable=SC1091
                        # shellcheck source=./arm-forge.sh
                        source "${TRACE_SCRIPTS_PATH}"/arm-forge.sh
                        # Set path to the application - this even better because the submission is independent of the current working directory
                        bindingExecutable="map"
                        bindingArgs=" --profile -o $(pwd)/$(hostname).map --mpi=generic -n ${numThreads} ${pythonInterpreter} $bindingArgs"
                        echo "[BINDINGS PIPER] Arm setup for MAP done"
                    elif [ "$tracing" -eq "-3" ]; then 
                        # arm-ddt
                        echo "[BINDINGS PIPER] Setting up arm-ddt tracing in folder $(pwd)"
                        TRACE_SCRIPTS_PATH=${SCRIPT_DIR}/../../../../../scripts/system/trace
                        # shellcheck disable=SC1091
                        # shellcheck source=./arm-forge.sh
                        source "${TRACE_SCRIPTS_PATH}"/arm-forge.sh
                        # Set path to the application - this even better because the submission is independent of the current working directory
                        bindingExecutable="ddt"
                        bindingArgs=" --connect --mpi=generic -n ${numThreads} ${pythonInterpreter} $bindingArgs"
                        echo "[BINDINGS PIPER] Arm setup for DDT done"
                    else
                        # Extrae
                        bindingExecutable="${workerCMD%%"$delimiter"*}$delimiter"
                    fi
                    # Build worker command
                    workerCMD="${bindingExecutable} ${bindingArgs}"
                fi
                
                # Add support for coverage-run command
                if [ "$binding" == "PYTHON" ]; then
                    delimiter="${pythonInterpreter} -u"
                    pythonInterpreter=$(which "${pythonInterpreter}")
                    if [[ "${pythonInterpreter}" = coverage* ]]; then
                        newInterpreter=$(echo "${pythonInterpreter}" | tr "#" " " )
                        newDelimiter=$(echo "${delimiter}")
                        echo "[BINDINGS PIPER] Changing Interpreter: ${newDelimiter} to ${newInterpreter}"
                        workerCMD=$(echo "${workerCMD}" | sed "s+${newDelimiter}+${newInterpreter}+")
                    fi
                fi

                # INVOKE WORKER
                echo "[BINDINGS PIPER] Executing command: ${workerCMD}"
                # shellcheck disable=SC2086
                eval ${workerCMD} </dev/null 3>/dev/null &
                bindingPID=$!
    
                # Disable EXTRAE automatic library initialisation (just in case)
                if [ "$tracing" -gt "0" ]; then
                  export EXTRAE_SKIP_AUTO_LIBRARY_INITIALIZE=1
                fi

                # Worker started notification
                echo "WORKER_STARTED ${bindingPID}" >> "${controlRESULTpipe}"
                ;;

            "GET_ALIVE")
                reply="ALIVE_REPLY"
                if [ "${line}" != "GET_ALIVE" ]; then
                    pids="${line}"
                    # shellcheck disable=SC2086
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
                ;;
        esac
    done <"${controlCMDpipe}" 3>"${controlCMDpipe}"
}


########################################
# MAIN
########################################

main() {
    echo "[BINDINGS PIPER] STARTING BINDINGS PIPER"

    # Arguments
    # shellcheck disable=SC2068
    get_args $@

    # Clean and Create control pipes
    echo "[BINDINGS PIPER] Control CMD Pipe: $controlCMDpipe"
    create_pipe "$controlCMDpipe"

    echo "[BINDINGS PIPER] Control RESULT Pipe: $controlRESULTpipe"
    create_pipe "$controlRESULTpipe"

    # Take control pipe path for creating the binding log path
    tmpFiles_dir=$(dirname "${controlCMDpipe}")
    worker_log_dir="${tmpFiles_dir}/log"

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
        # shellcheck source=./activate
        # shellcheck disable=SC1091
        source "${virtualEnvironment}/bin/activate"
    fi

    # If MPI worker override SLURM variable to allow process 0 + N workers:
    # NOTE: The MPI worker only works in local and in SCs with SLURM
    if [ "$mpiWorker" == "true" ]; then
        export SLURM_TASKS_PER_NODE=${numThreads}
        # Force to spawn the MPI processes in the same node
        export SLURM_NODELIST=${SLURM_STEP_NODELIST}
    fi
    
    # Export tracing
    export_tracing
  
    # Process pipe commands
    process_pipe_commands
  
    # Quit
    echo "QUIT" >> "${controlRESULTpipe}"
  
    # Exit message
    echo "[BINDINGS PIPER] Finished"
    exit 0
}


########################################
# ENTRY POINT
########################################

main "$@"
