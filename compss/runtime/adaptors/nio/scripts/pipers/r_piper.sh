#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

  ########################################
  # SCRIPT HELPER FUNCTIONS
  ########################################
  get_args() {
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

    controlCMDpipe=${1}
    controlRESULTpipe=${2}
    shift 2
  }

  pipe_processor() {
    local cmdPipe=$1
    local resultPipe=$2
    local executorId=$3

    echo "[R EXECUTOR] Launching R_executor for $cmdPipe to $resultPipe with id: ${executorId}"
    Rscript $SCRIPT_DIR/executor.R $cmdPipe $resultPipe $executorId

    echo "${QUIT_TAG}" > "${resultPipe}"
    echo "[R EXECUTOR] Pipe processor on $cmdPipe finished"
  }

  export_vars(){
    envars=$(echo "$1" | tr ";" "\\n")
    for var in $envars; do
       norm_var=$(echo "$var" | tr "#" " ")
       # shellcheck disable=SC2163
       export "${norm_var}"
    done
  }

  execute_task() {
    local tid=$1
    local sandBox=$2
    local jobOut=$3
    local jobErr=$4
    shift 4

    # Log the task initialization
    echo "[R EXECUTOR] Execute task $tid"
    echo "[R EXECUTOR]   - CMD: $* 1>> $jobOut 2>> $jobErr"

    echo "[R EXECUTOR] Execute task $tid" >> "$jobOut"
    echo "[R EXECUTOR]   - CMD: $* 1>> $jobOut 2>> $jobErr" >> "$jobOut"

    export_vars "$1"
    shift 1

    # Real task execution
    # shellcheck disable=SC2068
    $@ 1>> "$jobOut" 2>> "$jobErr"
    local exitValue=$?

    # Log the task end
    echo "[R EXECUTOR] Finished Task $tid with exitStatus ${exitValue}"
    echo "[R EXECUTOR] Finished Task $tid with exitStatus ${exitValue}" >> "$jobOut"

    # Return the result to the Runtime
    echo "${END_TASK_TAG} ${tid} ${exitValue}" >> "$resultPipe"
  }

  clean_procs() {
    # Send forced kill to subprocesses
    i=0
    while [ $i -lt ${#pipe_pids[@]} ]; do
      pid=${pipe_pids[$i]}
      if [ ${pid} -gt 0 ]; then
      	kill -9 "$pid"
      fi
      i=$((i+1))
    done
  }

  function get_executor_index() {
    executor_index=-1
    for i in "${!CMDpipes[@]}"; do
      if [[ "${CMDpipes[$i]}" = "${1}" ]]; then
          executor_index=${i}
      fi
    done
  }

  ########################################
  # MAIN
  ########################################

  # Script variables
  QUIT_TAG="QUIT"
  EXECUTE_TASK_TAG="EXECUTE_TASK"
  END_TASK_TAG="END_TASK"

  # Arguments
  get_args "$@"

  # Check if tracing
  tracing=${EXTRAE_CONFIG_FILE-""}

  # Launch one process per CMDPipe
  pipe_pids=()
  i=0
  while [ $i -lt "${numPipesCMD}" ]; do
    pipe_processor ${CMDpipes[$i]} ${RESULTpipes[$i]} $i &
    pipe_pids[$i]=$!
    i=$((i+1))
  done

  # Trap if error occurs (bindings_piper sends SIGTERM -15)
  trap clean_procs SIGTERM

  stop_received=false
  while [ "${stop_received}" = false ]; do
    read line
    command=$(echo "$line" | tr " " "\t" | awk '{ print $1 }')
    case "${command}" in
      "QUERY_EXECUTOR_ID")
        in_pipe=$(echo "${line}" | tr " " "\t" | awk '{ print $2 }')
        out_pipe=$(echo "${line}" | tr " " "\t" | awk '{ print $3 }')
        get_executor_index "${in_pipe}"
        pipe_pid=${pipe_pids[${executor_index}]}
        echo "REPLY_EXECUTOR_ID ${out_pipe} ${in_pipe} ${pipe_pid}" >> "${controlRESULTpipe}"
        ;;
      "REMOVE_EXECUTOR")
        in_pipe=$(echo "${line}" | tr " " "\t" | awk '{ print $2 }')
        out_pipe=$(echo "${line}" | tr " " "\t" | awk '{ print $3 }')
        get_executor_index "${in_pipe}"
        pipe_pid=${pipe_pids[${executor_index}]}
        if [ "${pipe_pid}" -gt 0 ]; then
          kill -9 ${pipe_pid} >/dev/null 2>/dev/null
          #if [ "$tracing" != "" ] && [ "${executor_index}" == "0" ]; then
          #  echo "Emitting sync event for executor: ${executor_index}"
          #  env | grep EXTRAE
          #  $EXTRAE_HOME/bin/extrae-cmd emit 0 8000666 $(date +%s)
          #  $EXTRAE_HOME/bin/extrae-cmd emit 0 8000666 0
          #fi
          wait ${pipe_pid}
          pipe_pids[${executor_index}]=-1
        fi
        echo "REMOVED_EXECUTOR ${out_pipe} ${in_pipe}" >> "${controlRESULTpipe}"
        ;;
      "QUIT")
          stop_received=true
          ;;
      *)
        echo "[R WORKER] UNKNOWN COMMAND ${line}"
    esac
  done <"${controlCMDpipe}" 3>"${controlCMDpipe}"

  # Wait for sub-processes to perform execution
  errorStatus=0
  i=0
  while [ $i -lt ${#pipe_pids[@]} ]; do
     pid=${pipe_pids[$i]}
     if [ ${pid} -gt 0 ]; then
          wait $pid || errorStatus=$((errorStatus+1))
     fi
     i=$((i+1))
  done

  # $EXTRAE_HOME/bin/extrae-cmd emit 0 8000666 $(date +%s)
  # $EXTRAE_HOME/bin/extrae-cmd emit 0 8000666 0

  # Exit message
  if [ $errorStatus -ne 0 ]; then
      echo "[R PIPER] Sub proccess failed"
      exit 1
  else
      echo "[R PIPER] Finished"
      exit 0
  fi
