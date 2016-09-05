#!/bin/bash

  ########################################
  # SCRIPT HELPER FUNCTIONS
  ########################################
  get_args() {
    # Get CMD pipes
    CMDpipes=()
    numPipesCMD=$1
    shift 1
    local i=0
    while [ $i -lt $numPipesCMD ]; do
      local arg_pos=$((i+1))
      CMDpipes[$i]=${!arg_pos}
      i=$((i+1))
    done
    shift ${numPipesCMD}

    # Get RESULT pipes
    RESULTpipes=()
    numPipesRESULT=$1
    shift 1
    i=0
    while [ $i -lt $numPipesRESULT ]; do
      local arg_pos=$((i+1))
      RESULTpipes[$i]=${!arg_pos}
      i=$((i+1))
    done
    shift ${numPipesRESULT}
  }

  pipe_processor() {
    local cmdPipe=$1
    local resultPipe=$2

    echo "[C PIPER] Ready to receive commands for $cmdPipe to $resultPipe"
    while true; do
      if read line <$cmdPipe; then
        echo "[C PIPER] Processing $line"
        local command=($line)
        local tag=${command[0]}
        if [ "$tag" == "${EXECUTE_TASK_TAG}" ]; then
          local taskId=${command[1]}
          local toCall=${command[@]:2}
          execute_task $taskId $toCall
        elif [ "$tag" == "${QUIT_TAG}" ]; then
          break
        else
          echo "[C PIPER] Unrecognised tag $tag. Skipping"
        fi
      fi
    done

    echo "[C PIPER] Pipe processor on $cmdPipe finished"
  }

  execute_task() {
    local tid=$1
    local jobOut=$2
    local jobErr=$3
    shift 3

    # Log the task initialization
    echo "[C PIPER] Execute task $tid"
    echo "[C PIPER]   - CMD: $@ 1>> $jobOut 2>> $jobErr"

    echo "[C PIPER] Execute task $tid" >> $jobOut
    echo "[C PIPER]   - CMD: $@ 1>> $jobOut 2>> $jobErr" >> $jobOut

    # Real task execution
    eval $@ 1>> $jobOut 2>> $jobErr
    local exitValue=$?

    # Log the task end
    echo "[C PIPER] Finished Task $tid with exitStatus ${exitValue}"
    echo "[C PIPER] Finished Task $tid with exitStatus ${exitValue}" >> $jobOut

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
  while [ $i -lt ${numPipesCMD} ]; do
    pipe_processor ${CMDpipes[$i]} ${RESULTpipes[$i]} &
    pipe_pids[$i]=$!
    i=$((i+1))
  done

  # Trap if error occurs (bindings_piper sends SIGTERM -15)
  trap clean_procs SIGTERM
  
  # Wait for sub-processes to perform execution
  errorStatus=0
  i=0
  while [ $i -lt ${#pipe_pids[@]} ]; do
     pid=${pipe_pids[$i]}
     wait $pid || let "errorStatus+=1"
     i=$((i+1))
  done

  # Exit message
  if [ $errorStatus -ne 0 ]; then
      echo "[C PIPER] Sub proccess failed"
      exit 1
  else 
      echo "[C PIPER] Finished"
      exit 0
  fi

