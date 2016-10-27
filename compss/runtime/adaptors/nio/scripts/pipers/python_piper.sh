#!/bin/bash

  ########################################
  # SCRIPT HELPER FUNCTIONS
  ########################################
  get_args() {
    # Get debug and tracing
    debug=$1
    tracing=$2
    shift 2

    # Get num Threads
    numThreads=$1
    shift 1

    # Get CMD pipes
    CMDpipes=()
    local i=0
    while [ $i -lt $numThreads ]; do
      local arg_pos=$((i+1))
      CMDpipes[$i]=${!arg_pos}
      i=$((i+1))
    done
    shift ${numThreads}

    # Get RESULT pipes
    RESULTpipes=()
    i=0
    while [ $i -lt $numThreads ]; do
      local arg_pos=$((i+1))
      RESULTpipes[$i]=${!arg_pos}
      i=$((i+1))
    done
    shift ${numThreads}
  }

  pipe_processor() {
    local cmdPipe=$1
    local resultPipe=$2

    echo "[PYTHON PIPER] Ready to receive commands for $cmdPipe to $resultPipe"
    while true; do
      if read line <$cmdPipe; then
        echo "[PYTHON PIPER] Processing $line"
        local command=($line)
        local tag=${command[0]}
        if [ "$tag" == "${EXECUTE_TASK_TAG}" ]; then
          local taskId=${command[1]}
          local toCall=${command[@]:2}
          execute_task $taskId $toCall
        elif [ "$tag" == "${QUIT_TAG}" ]; then
          break
        else
          echo "[PYTHON PIPER] Unrecognised tag $tag. Skipping"
        fi
      fi
    done

    echo "[PYTHON PIPER] Pipe processor on $cmdPipe finished"
  }

  execute_task() {
    local tid=$1
    local jobOut=$2
    local jobErr=$3
    shift 3

    # Log the task initialization
    echo "[PYTHON PIPER] Execute task $tid"
    echo "[PYTHON PIPER]   - CMD: $@ 1>> $jobOut 2>> $jobErr"

    echo "[PYTHON PIPER] Execute task $tid" >> $jobOut
    echo "[PYTHON PIPER]   - CMD: $@ 1>> $jobOut 2>> $jobErr" >> $jobOut

    # Tracing pre-handler
    echo "Tracing: $tracing" >> $jobOut
    if [ "$tracing" == "true" ]; then
      baseConfigFile=$(dirname $0)/../../../../../configuration/xml/tracing/extrae_task.xml
      taskConfigFile=$(pwd)/task${tid}.xml
      echo "baseC: ${baseConfigFile}" >> $jobOut
      echo "taskC: ${taskConfigFile}" >> $jobOut
      echo $(sed s/{{NAME}}/task$tid/g <<< $(cat ${baseConfigFile})) > ${taskConfigFile}
      export EXTRAE_CONFIG_FILE=${taskConfigFile}
    fi

    tmp=$(mktemp -d --tmpdir=.)
    wd=$(pwd)
    cd ${tmp}
    # Real task execution
    $@ 1>> $jobOut 2>> $jobErr
    local exitValue=$?
    cd ${wd}

    # Log the task end
    echo "[PYTHON PIPER] Finished Task $tid with exitStatus ${exitValue}"
    echo "[PYTHON PIPER] Finished Task $tid with exitStatus ${exitValue}" >> $jobOut

    # Return the result to the Runtime
    echo "${END_TASK_TAG} ${tid} ${exitValue}" >> $resultPipe
  }

  clean_procs() {
    # Send forced kill to subprocesses
    i=0
    while [ $i -lt ${#pipe_pids[@]} ]; do
      pid=${pipe_pids[$i]}
      kill -9 $pid
      i=$((i+1))
    done
  }


  ########################################
  # MAIN
  ########################################

  # Script variables
  QUIT_TAG="quit"
  EXECUTE_TASK_TAG="task"
  END_TASK_TAG="endTask"

  # Arguments
  get_args $@

  # Launch one process per CMDPipe
  pipe_pids=()
  i=0
  while [ $i -lt ${numThreads} ]; do
    pipe_processor ${CMDpipes[$i]} ${RESULTpipes[$i]} &
    pipe_pids[$i]=$!
    i=$((i+1))
  done
  
  # Wait for sub-processes to perform execution
  errorStatus=0
  i=0
  while [ $i -lt ${#pipe_pids[@]} ]; do
     pid=${pipe_pids[$i]}
     wait $pid || let "errorStatus+=1"
     i=$((i+1))
  done

  # Trap if error occurs (bindings_piper sends SIGTERM -15)
  trap clean_procs SIGTERM

  # Exit message
  if [ $errorStatus -ne 0 ]; then
      echo "[PYTHON PIPER] Sub proccess failed"
      exit 1
  else 
      echo "[PYTHON PIPER] Finished"
      exit 0
  fi

